/*
 * Class declaraiton and definition for TEA
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

/* Define TEA class in namespace TEA */
namespace TEA {

/* table parameters */
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8                 // item(cell) number in a bucket
#endif
#define STASH_SIZE_TEA 8192      // size of stash

struct Bucket{
    /* These should be in remote for real implement */
    /* These are in remote */
    //char key[N][KEY_LEN];   //fingerprints are put in here
    //char val[N][VAL_LEN];   //values are put here
    /* This could be in local or in remote */
    bool full[N];           //whether the cell is full or not
};

struct Table
{
    Bucket *bucket;
    uint32_t *cell;        // occupied cell number of each bucket
};

class TEATable{
public:
    int bucket_number;
    int max_kick_number;
    Table table;
    std::unordered_map<std::string, std::string> stash;  //overflow stash

    uint32_t seed_hash_to_fp;
    uint32_t seed_hash_to_bucket;
    uint32_t seed_hash_to_alt;

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
        seed_hash_to_bucket = seed;
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_alt = seed;
    }

    // return index for table 1.
    int hash1(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket) % bucket_number;
    }


    // insert an full entry to stash
    bool insert_to_stash(const Entry& entry) {
		stash_lock();
        if(stash_num >= STASH_SIZE_TEA) {
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

    /* Old version, check in table in the local
     * given a key, check whether table has key in bucket b
     * return cell number if true, -1 if false */
    /*int check_in_table(int b, const char *key) {
        for(int i = 0; i < table.cell[b]; i++) {
            if(memcmp(key, table.bucket[b].key[i], KEY_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
    }*/
    /* New version, check in the buckets read from the remote
     * given a key, check whether table has key in bucket b
     * return cell number if true, -1 if false */
    int check_in_table(int b, char *key, Entry *entry) {
        for(int i = 0; i < table.cell[b]; i++) {
            if(memcmp(key, entry[i].key, KEY_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
    }

public:
    /* Insert an entry */
    bool insert(const Entry& entry, int tid) {
        int tries = 0;
        Entry tmp_entry;
        memcpy(tmp_entry.key, entry.key, KEY_LEN*sizeof(char));
        memcpy(tmp_entry.val, entry.val, VAL_LEN*sizeof(char));
        Entry victim;

        // do cuckoo kick
        while(tries < max_kick_number)
        {
            int h1 = hash1(tmp_entry.key);
            int h2 = (h1 + 1) % bucket_number; //next bucket is the alternate bucket in TEA.

            std::set<int> mut_idx({h1,h2});
            bucket_lock(mut_idx);

            /* RDMA read: table, bucket h1, h2*/
            /* NOTICE: this is only for simulation*/
            // If h1 is next to h2, read once, otherwise twice
            Entry tmp_buffer[2][2*N];
            Entry *tmp_bucket[2];
            if (h1 < h2) {
                RDMA_read_bucket(tmp_buffer, &h1, 1, 2, tid);
                tmp_bucket[0] = tmp_buffer[0];
                tmp_bucket[1] = tmp_buffer[0]+N;
            }
            else {
                int h[2] = {h1,h2};
                RDMA_read_bucket(tmp_buffer, h, 2, 1, tid);
                tmp_bucket[0] = tmp_buffer[0];
                tmp_bucket[1] = tmp_buffer[1];
            }

            if(check_in_table(h1, tmp_entry.key, tmp_bucket[0]) != -1 || check_in_table(h2, tmp_entry.key, tmp_bucket[1]) != -1) { //collision happen				
                bucket_unlock(mut_idx);
                if(insert_to_stash(tmp_entry)) {					
                    return true;
				}
                return false;
            }

            if(table.cell[h1] < N) {  //if h1 has empty cell, insert key into h1
                for(int i = 0; i < N; i++ ) {
                    if(!table.bucket[h1].full[i]) {
                        //memcpy(table.bucket[h1].key[i], tmp_entry.key, KEY_LEN*sizeof(char));
                        //memcpy(table.bucket[h1].val[i], tmp_entry.val, VAL_LEN*sizeof(char));
                        /* RDMA write: tmp_entry to table, bucket h1, cell i*/
                        RDMA_write(0, h1, i, tmp_entry, tid);

                        table.bucket[h1].full[i] = true;
                        break;
                    }
                }
                table.cell[h1]++;
                if(table.cell[h1] == N);  //fullbucket_num++;
                if(tries > 0) {
                    /*kick_num++;
                    kick_success_num++;
                    kick_stat[tries]++;*/
                }
                bucket_unlock(mut_idx);
                return true;
            }
            if(table.cell[h2] < N) {  //if h2 has empty cell, insert key into h2
                for(int i = 0; i < N; i++ ) {
                    if(!table.bucket[h2].full[i]) {
                        //memcpy(table.bucket[h2].key[i], tmp_entry.key, KEY_LEN*sizeof(char));
                        //memcpy(table.bucket[h2].val[i], tmp_entry.val, VAL_LEN*sizeof(char));
                        /* RDMA write: tmp_entry to table, bucket h2, cell i*/
                        RDMA_write(0, h2, i, tmp_entry, tid);

                        table.bucket[h2].full[i] = true;
                        break;
                    }
                }
                table.cell[h2]++;
                if(table.cell[h2] == N);  //fullbucket_num++;
                if(tries > 0) {
                    /*kick_num++;
                    kick_success_num++;
                    kick_stat[tries]++;*/
                }
                bucket_unlock(mut_idx);
                return true;
            }
            //choose a victim
            //int tmptable = (rand() % 2 == 0)?h1:h2;
            int tmptable = rand() % 2;
            int tmpcell = rand() % N;
            //victim 
            //memcpy(victim.key, table.bucket[tmptable].key[tmpcell], KEY_LEN*sizeof(char));
            //memcpy(victim.val, table.bucket[tmptable].val[tmpcell], VAL_LEN*sizeof(char));
            memcpy(victim.key, tmp_bucket[tmptable][tmpcell].key, KEY_LEN*sizeof(char));
            memcpy(victim.val, tmp_bucket[tmptable][tmpcell].val, VAL_LEN*sizeof(char));
            //insert tmp_entry
            //memcpy(table.bucket[tmptable].key[tmpcell], tmp_entry.key, KEY_LEN*sizeof(char));
            //memcpy(table.bucket[tmptable].val[tmpcell], tmp_entry.val, VAL_LEN*sizeof(char));
            /* RDMA write: tmp_entry to table, bucket tmptable, cell tmpcell*/
            tmptable = (tmptable == 0)?h1:h2;
            RDMA_write(0, tmptable, tmpcell, tmp_entry, tid);

            //tmp_entry = victim
            memcpy(tmp_entry.key, victim.key, KEY_LEN*sizeof(char));
            memcpy(tmp_entry.val, victim.val, VAL_LEN*sizeof(char));
        
            tries++;
            bucket_unlock(mut_idx);
        }
        //max kick reached
		
        if(insert_to_stash(tmp_entry))
            return true;
        return false;
    }

    /* Query key and put result in val */
    bool query(char *key, char *val, int tid) {
        //query in table
        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;
        int cell;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx);

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_buffer[2][2*N];
        Entry *tmp_bucket[2];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_buffer, &h1, 1, 2, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[0]+N;
        }
        else {
            int h[2] = {h1,h2};
            RDMA_read_bucket(tmp_buffer, h, 2, 1, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[1];
        }

        //query h1
        cell = check_in_table(h1, key, tmp_bucket[0]);
        if(cell != -1) {
			/*bucket_unlock(h1);

            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                //memcpy(val, &read_buf[0].val, PTR_LEN);
                return true;
            }*/
            bucket_unlock(mut_idx);
            return true;
        }

        //query h2
        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
			/*bucket_unlock(h2);

            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0) {
                //memcpy(val, &read_buf[0].val, PTR_LEN);
                return true;
            }*/
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
            stash_unlock();
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);
            return true;
        }
		stash_unlock();

        //miss
        return false;
    }

    /* Delete an entry given a key */
    bool deletion(char* key, int tid) {
        //query in table
        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx);

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_buffer[2][2*N];
        Entry *tmp_bucket[2];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_buffer, &h1, 1, 2, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[0]+N;
        }
        else {
            int h[2] = {h1,h2};
            RDMA_read_bucket(tmp_buffer, h, 2, 1, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[1];
        }
        
        int cell;
        cell = check_in_table(h1, key, tmp_bucket[0]);
        if(cell != -1) {
            //memset(table.bucket[h1].key[cell], 0, KEY_LEN);
            table.bucket[h1].full[cell] = 0;
            /* RDMA write: 0 to table, bucket h1, cell cell*/
            RDMA_write(0, h1, cell, Entry({{0},{0}}), tid);

			bucket_unlock(mut_idx);
            return true;
        }

        //query in table
        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
            //memset(table.bucket[h2].key[cell], 0, KEY_LEN);
            table.bucket[h2].full[cell] = 0;
            /* RDMA write: 0 to table, bucket h2, cell cell*/
            RDMA_write(0, h2, cell, Entry({{0},{0}}), tid);

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
        int h1 = hash1(entry.key);
        int h2 = (h1 + 1) % bucket_number;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx);

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_buffer[2][2*N];
        Entry *tmp_bucket[2];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_buffer, &h1, 1, 2, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[0]+N;
        }
        else {
            int h[2] = {h1,h2};
            RDMA_read_bucket(tmp_buffer, h, 2, 1, tid);
            tmp_bucket[0] = tmp_buffer[0];
            tmp_bucket[1] = tmp_buffer[1];
        }

        //query in table
        int cell;
        cell = check_in_table(h1, entry.key, tmp_bucket[0]);
        if(cell != -1) {
            //memcpy(table.bucket[h1].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h1, cell cell*/
            RDMA_write(0, h1, cell, entry, tid);
            
			bucket_unlock(mut_idx);
            return true;
        }

        cell = check_in_table(h2, entry.key, tmp_bucket[1]);
        if(cell != -1) {
            //memcpy(table.bucket[h2].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h2, cell cell*/
            RDMA_write(0, h2, cell, entry, tid);
            
			bucket_unlock(mut_idx);
            return true;
        }
		bucket_unlock(mut_idx);

        // query in stash
        std::string skey(entry.key, KEY_LEN);
		stash_lock();
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

    TEATable(int cell_number, int max_kick_num, int thread_num) {
        this->thread_num = thread_num;
        this->bucket_number = cell_number/N;
        this->max_kick_number = max_kick_num;
        this->table.bucket = new Bucket[this->bucket_number];
        memset(table.bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table.cell = new uint32_t[this->bucket_number];
        memset(table.cell, 0, this->bucket_number*sizeof(uint32_t));

        initialize_hash_functions();

        stash_num = 0;

		this->bucket_mut = new mutex[this->bucket_number];
    }

    ~TEATable() {
        delete [] table.cell;
        delete [] table.bucket;
		delete [] bucket_mut;
        stash.clear();
    }
};

}
