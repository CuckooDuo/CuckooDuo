/*
 * Class declaraiton and definition for RACE
 *
 */

#include "murmur3.h"
#include <cstring>
#include <random>
#include <set>
#include <cstdio>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <stack>
#include <unordered_map>
#include <mutex>
#include "../rdma/rdma_client.h"
#pragma once

/* entry parameters */
#ifndef _ENTRY_H_
#define _ENTRY_H_
#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
#endif

/* Define RACE class in namespace RACE */
namespace RACE {

/* table parameters */
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8                 // item(cell) number in a bucket
#endif
#define STASH_SIZE_RACE 64        // size of stash

struct Bucket{
    /* These should be in remote for real implement */
    /* These are in remote */
    //char key[N][KEY_LEN];   //keys are put in here
    //char val[N][VAL_LEN];   //values are put here
    /* This could be in local or in remote */
    bool full[N];           //whether the cell is full or not
};

struct Table
{
    Bucket *bucket;
    uint32_t *cell;        // occupied cell number of each bucket
};

class RACETable{
public:
    int bucket_number;
    int group_number;
    Table table;
    std::unordered_map<std::string, std::string> stash;  //overflow stash

    uint32_t seed_hash_to_fp;
    uint32_t seed_hash_to_bucket[2];

    int stash_num;  // the number of entries in stash

    mutex stash_mut;    // mutex for stash visition
    mutex *bucket_mut;  // mutex for bucket visition

    int thread_num; // the number of threads we use

private:
    /* function to lock and unlock buckets */
	void inline bucket_lock(int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[bucket_idx].lock();
        }
    }
    void inline bucket_unlock(int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[bucket_idx].unlock();
        }
    }
    void inline bucket_lock(const std::set<int> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].lock();
        }
    }
    void inline bucket_unlock(const std::set<int> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].unlock();
        }
    }

    /* function to lock and unlock stash */
    void inline stash_lock() {
        if (thread_num > 1) {
            stash_mut.lock();
        }
    }
    void inline stash_unlock() {
        if (thread_num > 1) {
            stash_mut.unlock();
        }
    }

    void initialize_hash_functions(){
        std::set<int> seeds;
        srand((unsigned)time(NULL));

        uint32_t seed = rand()%MAX_PRIME32;
        seed_hash_to_fp = seed;
        seeds.insert(seed);
        
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_bucket[0] = seed;
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_bucket[1] = seed;
    }

    // return index for group 1.
    int hash1(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket[0]) % group_number;
    }

    // return index for group 2.
    int hash2(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket[1]) % group_number;
    }

    // insert an full entry to stash
    bool insert_to_stash(const Entry& entry) {
		stash_lock();
        if(stash_num >= STASH_SIZE_RACE) {
			stash_unlock();
            return false;
		}
        std::string skey(entry.key, KEY_LEN);
        std::string sval(entry.val, VAL_LEN);
        stash.emplace(skey, sval);
        stash_num++;
		stash_unlock();
        return true;
    }

    // RDMA write, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID);
        memcpy(&write_buf[tid][0], &entry, KV_LEN);
        mod_remote(1, tid);
    }
    // RDMA read, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID);
        get_remote(1, tid);
        memcpy(&entry, &read_buf[tid][0], KV_LEN);
    }
    // RDMA read, read sz*b_num entries from vec(table,bucket)
    inline void RDMA_read_bucket(Entry (*entry)[2*N], int *bucketID, int sz, int b_num, int tid){
        for (int i = 0; i < sz; ++i)
            ra[tid][i] = get_offset_table(0, bucketID[i], 0);
        get_bucket(b_num, sz, tid);
        memcpy(entry, bucket_buf[tid].buf, N*2*KV_LEN*sz);
    }

    /* Old version, check in the table in local
     * given a key, check whether table has key in group g, 
     * return cell number if in main bucket, 
     * cell number plus depth if in overflow bucket,
     * -1 if false */
    /*int check_in_table(int g, char *key) {
        //calculate bucket number
        int idx1 = (g%2==0) ? (g/2*3) : ((g+1)/2*3-1);
        int idx2 = (g%2==0) ? (idx1+1) : (idx1-1);
        for(int i = 0; i < table.cell[idx1]; i++) {
            if(memcmp(key, table.bucket[idx1].key[i], KEY_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        for(int i = 0; i < table.cell[idx2]; i++) {
            if(memcmp(key, table.bucket[idx2].key[i], KEY_LEN*sizeof(char)) == 0) {
				return i + N;
            }
        }
        return -1;
    }*/
    
    /* New version, check in the buckets read from the remote
     * given a key, check whether table has key in group g, 
     * return cell number if in main bucket, 
     * cell number plus depth if in overflow bucket,
     * -1 if false */
    int check_in_table(int g, char *key, Entry *entry) {
        //calculate bucket number
        int idx1 = (g%2==0) ? (g/2*3) : ((g+1)/2*3-1);
        int idx2 = (g%2==0) ? (idx1+1) : (idx1-1);

        if (idx1 < idx2) {
            for(int i = 0; i < table.cell[idx1]; i++) {
                if(memcmp(key, entry[i].key, KEY_LEN*sizeof(char)) == 0) {
                    return i;
                }
            }
            for(int i = 0; i < table.cell[idx2]; i++) {
                if(memcmp(key, entry[i+N].key, KEY_LEN*sizeof(char)) == 0) {
		    		return i + N;
                }
            }
        }
        else {
            for(int i = 0; i < table.cell[idx2]; i++) {
                if(memcmp(key, entry[i].key, KEY_LEN*sizeof(char)) == 0) {
                    return i + N;
                }
            }
            for(int i = 0; i < table.cell[idx1]; i++) {
                if(memcmp(key, entry[i+N].key, KEY_LEN*sizeof(char)) == 0) {
	        		return i;
                }
            }
        }
        return -1;
    }

public:
    /* Insert an entry */
    bool insert(Entry& entry, int tid) {
        //calculate group and bucket
        int h1 = hash1(entry.key);
        int h2 = hash2(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx);

        /* RDMA read: table, bucket h1_idx1, h1_idx2, h2_idx1, h2_idx2*/
        /* NOTICE: this is only for simulation*/
        /* RDMA read: table, bucket h1_idx1, h1_idx2, h2_idx1, h2_idx2*/
        /* NOTICE: this is only for simulation*/
        Entry tmp_bucket[2][2*N];
        int b_id[2] = {min(h1_idx1, h1_idx2), min(h2_idx1, h2_idx2)};
        RDMA_read_bucket(tmp_bucket, b_id, 2, 2, tid);

        //check collision
        if(check_in_table(h1, entry.key, tmp_bucket[0]) != -1 || check_in_table(h2, entry.key, tmp_bucket[1]) != -1) { 
            //collision happen
            bucket_unlock(mut_idx);

            if(insert_to_stash(entry))
                return true;
            return false;
        }

        //if h1 has more empty cell, insert key into group 1
        if((table.cell[h1_idx1]+table.cell[h1_idx2] < 2*N) && (table.cell[h1_idx1]+table.cell[h1_idx2] <= table.cell[h2_idx1]+table.cell[h2_idx2])) {
            //try to insert into main bucket first
            if(table.cell[h1_idx1] < N) {
                //memcpy(table.bucket[h1_idx1].key[table.cell[h1_idx1]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx1, cell table.cell[h1_idx1]*/
                RDMA_write(0, h1_idx1, table.cell[h1_idx1], entry, tid);
                
                table.cell[h1_idx1]++;
                if(table.cell[h1_idx1] == N);

				bucket_unlock(mut_idx);
                return true;
            }
            //then the overflow bucket
            if(table.cell[h1_idx2] < N) {
                //memcpy(table.bucket[h1_idx2].key[table.cell[h1_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx2, cell table.cell[h1_idx2]*/
                RDMA_write(0, h1_idx2, table.cell[h1_idx2], entry,tid);
                
                table.cell[h1_idx2]++;
                if(table.cell[h1_idx2] == N);

				bucket_unlock(mut_idx);
                return true;
            }
            //error
			bucket_unlock(mut_idx);
            return false;
        }
        //if h2 has empty cell, insert key into group 2
        else if(table.cell[h2_idx1]+table.cell[h2_idx2] < 2*N) {
            //try to insert into main bucket first
            if(table.cell[h2_idx1] < N) {
                //memcpy(table.bucket[h2_idx1].key[table.cell[h2_idx1]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx1, cell table.cell[h2_idx1]*/
                RDMA_write(0, h2_idx1, table.cell[h2_idx1], entry, tid);

                table.cell[h2_idx1]++;
                if(table.cell[h2_idx1] == N);

				bucket_unlock(mut_idx);
                return true;
            }
            //then the overflow bucket
            if(table.cell[h2_idx2] < N) {
                //memcpy(table.bucket[h2_idx2].key[table.cell[h2_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx2, cell table.cell[h2_idx2]*/
                RDMA_write(0, h2_idx2, table.cell[h2_idx2], entry, tid);

                table.cell[h2_idx2]++;
                if(table.cell[h2_idx2] == N);

				bucket_unlock(mut_idx);
                return true;
            }
            //error
			bucket_unlock(mut_idx);
            return false;
        }

        bucket_unlock(mut_idx);

        //both group full, insert into stash
        if(insert_to_stash(entry))
            return true;
        return false;
    }

    /* Query key and put result in val */
    bool query(char *key, char *val, int tid) {
        //query h1
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1,h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx);

        Entry tmp_bucket[2][2*N];
        int b_id[2] = {min(h1_idx1, h1_idx2), min(h2_idx1, h2_idx2)};
        // read two buckets in one time twice
        RDMA_read_bucket(tmp_bucket, b_id, 2, 2, tid);

        int cell;
        cell = check_in_table(h1, key, tmp_bucket[0]);
        if(cell != -1) {
            if(cell < N) {
				/*bucket_unlock(mut_idx);

                if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            else {
				/*bucket_unlock(mut_idx);

                if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            bucket_unlock(mut_idx);
            return true;
        }

        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
            if(cell < N) {
				/*bucket_unlock(mut_idx);

                if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            else {
				/*bucket_unlock(mut_idx);

                if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            bucket_unlock(mut_idx);
            return true;
        }
		bucket_unlock(mut_idx);

        // query in stash
        std::string skey(key, KEY_LEN);
		stash_lock();
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            std::string sval = entry->second;
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);
			stash_unlock();
            return true;
        }
		stash_unlock();

        //miss
        return false;
    }

    /* Delete an entry given a key */
    bool deletion(char* key, int tid) {
        //query h1
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1,h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx);

        Entry tmp_bucket[2][2*N];
        int b_id[2] = {min(h1_idx1, h1_idx2), min(h2_idx1, h2_idx2)};
        RDMA_read_bucket(tmp_bucket, b_id, 2, 2, tid);

        int cell;
        cell = check_in_table(h1, key, tmp_bucket[0]);
        if(cell != -1) {
            if(cell < N) {
                //memset(table.bucket[h1_idx1].key[cell], 0, KEY_LEN);
                table.bucket[h1_idx1].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h1_idx1, cell cell*/
                RDMA_write(0, h1_idx1, cell, Entry({{0},{0}}), tid);
            }
            else {
                //memset(table.bucket[h1_idx2].key[cell-N], 0, KEY_LEN);
                table.bucket[h1_idx2].full[cell-N] = 0;
                /* RDMA write: 0 to table, bucket h1_idx2, cell cell-N*/
                RDMA_write(0, h1_idx2, cell-N, Entry({{0},{0}}), tid);
            }
            bucket_unlock(mut_idx);
            return true;
        }

        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
            if(cell < N) {
                //memset(table.bucket[h2_idx1].key[cell], 0, KEY_LEN);
                table.bucket[h2_idx1].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h2_idx1, cell cell*/
                RDMA_write(0, h2_idx1, cell, Entry({{0},{0}}), tid);
            }
            else {
                //memset(table.bucket[h2_idx2].key[cell-N], 0, KEY_LEN);
                table.bucket[h2_idx2].full[cell-N] = 0;
                /* RDMA write: 0 to table, bucket h2_idx2, cell cell-N*/
                RDMA_write(0, h2_idx2, cell-N, Entry({{0},{0}}), tid);
            }
            bucket_unlock(mut_idx);
            return true;
        }
		bucket_unlock(mut_idx);

        //query in stash, if find then delete
        std::string skey(key, KEY_LEN);
		stash_lock();
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            stash.erase(entry);
            stash_num--;
			stash_unlock();
            return true;
        }
		stash_unlock();

        //miss
        return false;
    }

    /* Update an entry */
    bool update(Entry& entry, int tid) {

        //query h1
        int h1 = hash1(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(entry.key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1,h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx);

        Entry tmp_bucket[2][2*N];
        int b_id[2] = {min(h1_idx1, h1_idx2), min(h2_idx1, h2_idx2)};
        RDMA_read_bucket(tmp_bucket, b_id, 2, 2, tid);

        int cell;
        cell = check_in_table(h1, entry.key, tmp_bucket[0]);
        if(cell != -1) {
            if(cell < N) {
                //memcpy(table.bucket[h1_idx1].val[cell], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h1_idx1, cell cell*/
                RDMA_write(0, h1_idx1, cell, entry, tid);
            }
            else {
                //memcpy(table.bucket[h1_idx2].val[cell-N], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h1_idx2, cell cell-N*/
                RDMA_write(0, h1_idx2, cell-N, entry, tid);
            }
            bucket_unlock(mut_idx);
            return true;
        }

        cell = check_in_table(h2, entry.key, tmp_bucket[1]);
        if(cell != -1) {
            if(cell < N) {
                //memcpy(table.bucket[h2_idx1].val[cell], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h2_idx1, cell cell*/
                RDMA_write(0, h2_idx1, cell, entry, tid);
            }
            else {
                //memcpy(table.bucket[h2_idx2].val[cell-N], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h2_idx2, cell cell-N*/
                RDMA_write(0, h2_idx2, cell-N, entry, tid);
            }
            bucket_unlock(mut_idx);
            return true;
        }

		bucket_unlock(mut_idx);

        // query in stash
        stash_lock();
        std::string skey(entry.key, KEY_LEN);
        auto it = stash.find(skey);
        if(it != stash.end()) {
            std::string sval = entry.val;
            it->second = sval;
            stash_unlock();
            return true;
        }
        stash_unlock();

        //miss
        return false;
    }

    RACETable(int cell_number, int thread_num) {
        this->thread_num = thread_num;
        this->bucket_number = cell_number/N;
        this->group_number = this->bucket_number/3*2;
        this->table.bucket = new Bucket[this->bucket_number];
        memset(table.bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table.cell = new uint32_t[this->bucket_number];
        memset(table.cell, 0, this->bucket_number*sizeof(uint32_t));

        initialize_hash_functions();

        stash_num = 0;

		this->bucket_mut = new mutex[this->bucket_number];
    }

    ~RACETable() {
        delete [] table.cell;
        delete [] table.bucket;
		delete [] bucket_mut;
        stash.clear();
    }
};

}
