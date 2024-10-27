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
#include <shared_mutex>
#include "../rdma/rdma_client_largeKV.h"
#include "SimpleCache.h"
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

#define RW_LOCK_RACE

/* Define RACE class in namespace RACE */
namespace RACE {

/* table parameters */
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8                 // item(cell) number in a bucket
#endif
#define STASH_SIZE_RACE 64        // size of stash

// if not equal to 0, using cache
uint32_t TOTAL_MEMORY_BYTE_USING_CACHE = 0; // 65'000'000

struct Bucket{
    /* These should be in remote for real implement */
    /* These are in remote */
    char key[N][KEY_LEN];   //keys are put in here
    char val[N][VAL_LEN];   //values are put here
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

    #ifndef RW_LOCK_RACE
	mutex stash_mut;        // mutex for stash visition
    mutex *bucket_mut;      // mutex for bucket visition
    #endif

    #ifdef RW_LOCK_RACE
    shared_mutex stash_mut;
    shared_mutex *bucket_mut;
    #endif

    int thread_num; // the number of threads we use
    
    int viscnt = 0; // for memory access test

    SimpleLRUCache::Cache *cache;

private:
    /* function to lock and unlock buckets */
	void inline bucket_lock(int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            bucket_mut[bucket_idx].lock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            if(write_flag) bucket_mut[bucket_idx].lock();
            else bucket_mut[bucket_idx].lock_shared();
        }
        #endif
    }
    void inline bucket_unlock(int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            bucket_mut[bucket_idx].unlock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            if(write_flag) bucket_mut[bucket_idx].unlock();
            else bucket_mut[bucket_idx].unlock_shared();
        }
        #endif
    }
    void inline bucket_lock(const std::set<int> &mut_idx, bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].lock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            for (const auto &i : mut_idx) {
                if(write_flag) bucket_mut[i].lock();
                else bucket_mut[i].lock_shared();
            }
        }
        #endif
    }
    void inline bucket_unlock(const std::set<int> &mut_idx, bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].unlock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            for (const auto &i : mut_idx) {
                if(write_flag) bucket_mut[i].unlock();
                else bucket_mut[i].unlock_shared();
            }
        }
        #endif
    }

    /* function to lock and unlock stash */
    void inline stash_lock(bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            stash_mut.lock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            if(write_flag) stash_mut.lock();
            else stash_mut.lock_shared();
        }
        #endif
    }
    void inline stash_unlock(bool write_flag) {
        #ifndef RW_LOCK_RACE
        if (thread_num > 1) {
            stash_mut.unlock();
        }
        #endif

        #ifdef RW_LOCK_RACE
        if (thread_num > 1) {
            if(write_flag) stash_mut.unlock();
            else stash_mut.unlock_shared();
        }
        #endif
    }

    void initialize_hash_functions(){
        std::set<int> seeds;
        //srand((unsigned)time(NULL));
        srand(20241009);

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
		stash_lock(true);
        if(stash_num >= STASH_SIZE_RACE) {
			stash_unlock(true);
            return false;
		}
        std::string skey(entry.key, KEY_LEN);
        std::string sval(entry.val, VAL_LEN);
        stash.emplace(skey, sval);
        stash_num++;
		stash_unlock(true);
        return true;
    }

    // RDMA write, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry, int tid){
        //ra[tid][0] = get_offset_table(tableID, bucketID, cellID);
        ra[tid][0] = get_offset_table(0, 0, tid);
        //memcpy(&write_buf[tid][0], &entry, KV_LEN);
        memset(&write_buf[tid][0], 0, RDMA_KV_LEN);
        memcpy(&table.bucket[bucketID].key[cellID], &entry.key, KEY_LEN);
        memcpy(&table.bucket[bucketID].val[cellID], &entry.val, VAL_LEN);
        mod_remote(1, tid);
        ++viscnt;
    }
    // RDMA read, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID, int tid){
        //ra[tid][0] = get_offset_table(tableID, bucketID, cellID);
        ra[tid][0] = get_offset_table(0, 0, tid);
        get_remote(1, tid);
        ++viscnt;
        //memcpy(&entry, &read_buf[tid][0], KV_LEN);
        memset(&read_buf[tid][0], 0, RDMA_KV_LEN);
        memcpy(&entry.key, &table.bucket[bucketID].key[cellID], KEY_LEN);
        memcpy(&entry.val, &table.bucket[bucketID].val[cellID], VAL_LEN);
    }
    // RDMA read, read sz*b_num entries from vec(table,bucket)
    inline void RDMA_read_bucket(Entry (*entry)[2*N], int *bucketID, int sz, int b_num, int tid){
        for (int i = 0; i < sz; ++i)
            //ra[tid][i] = get_offset_table(0, bucketID[i], 0);
            ra[tid][i] = get_offset_table(0, i*32+tid*2, 0);
        get_bucket(b_num, sz, tid);
        ++viscnt;
        //memcpy(entry, bucket_buf[tid].buf, N*2*KV_LEN*sz);
        memset(bucket_buf[tid].buf, 0, N*2*RDMA_KV_LEN*sz);
        for (int i = 0; i < sz; ++i) {
            for (int j = 0; j < N; ++j) {
                memcpy(&entry[i][j].key, &table.bucket[bucketID[i]].key[j], KEY_LEN);
                memcpy(&entry[i][j].val, &table.bucket[bucketID[i]].val[j], VAL_LEN);
                if (b_num < 2) continue;
                memcpy(&entry[i][j+N].key, &table.bucket[bucketID[i]+1].key[j], KEY_LEN);
                memcpy(&entry[i][j+N].val, &table.bucket[bucketID[i]+1].val[j], KEY_LEN);
            }
        }
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
        int vislast = viscnt;

        //calculate group and bucket
        int h1 = hash1(entry.key);
        int h2 = hash2(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx, true);

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
            bucket_unlock(mut_idx, true);

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

				bucket_unlock(mut_idx, true);

                return true;
            }
            //then the overflow bucket
            if(table.cell[h1_idx2] < N) {
                //memcpy(table.bucket[h1_idx2].key[table.cell[h1_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx2, cell table.cell[h1_idx2]*/
                RDMA_write(0, h1_idx2, table.cell[h1_idx2], entry,tid);
                
                table.cell[h1_idx2]++;
                if(table.cell[h1_idx2] == N);

				bucket_unlock(mut_idx, true);

                return true;
            }
            //error
			bucket_unlock(mut_idx, true);
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

				bucket_unlock(mut_idx, true);
                return true;
            }
            //then the overflow bucket
            if(table.cell[h2_idx2] < N) {
                //memcpy(table.bucket[h2_idx2].key[table.cell[h2_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx2, cell table.cell[h2_idx2]*/
                RDMA_write(0, h2_idx2, table.cell[h2_idx2], entry, tid);

                table.cell[h2_idx2]++;
                if(table.cell[h2_idx2] == N);

				bucket_unlock(mut_idx, true);
                return true;
            }
            //error
			bucket_unlock(mut_idx, true);
            return false;
        }
        // if can't insert to bucket2, try to insert bucket1 again
        //else if(table.cell[h1_idx1]+table.cell[h1_idx2] < 2*N) {
        //    //try to insert into main bucket first
        //    if(table.cell[h1_idx1] < N) {
        //        //memcpy(table.bucket[h1_idx1].key[table.cell[h1_idx1]], entry.key, KEY_LEN*sizeof(char));
        //        /* RDMA write: Entry to table, bucket h1_idx1, cell table.cell[h1_idx1]*/
        //        RDMA_write(0, h1_idx1, table.cell[h1_idx1], entry, tid);
        //        
        //        table.cell[h1_idx1]++;
        //        if(table.cell[h1_idx1] == N);
        //
		//		bucket_unlock(mut_idx, true);
        //        return true;
        //    }
        //    //then the overflow bucket
        //    if(table.cell[h1_idx2] < N) {
        //        //memcpy(table.bucket[h1_idx2].key[table.cell[h1_idx2]], entry.key, KEY_LEN*sizeof(char));
        //        /* RDMA write: Entry to table, bucket h1_idx2, cell table.cell[h1_idx2]*/
        //        RDMA_write(0, h1_idx2, table.cell[h1_idx2], entry,tid);
        //        
        //        table.cell[h1_idx2]++;
        //        if(table.cell[h1_idx2] == N);
        //
		//		bucket_unlock(mut_idx, true);
        //        return true;
        //    }
        //    //error
		//	bucket_unlock(mut_idx, true);
        //    return false;
        //}

        bucket_unlock(mut_idx, true);

        //both group full, insert into stash
        if(insert_to_stash(entry))
            return true;
        return false;
    }

    /* Query key and put result in val */
    bool query(char *key, char *val, int tid) {
        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            if (cache->query(key, val)) return true;
        }
        
        //query h1
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx, false);

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
            bucket_unlock(mut_idx, false);

            if (TOTAL_MEMORY_BYTE_USING_CACHE)
                cache->insert(key, val);

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
            bucket_unlock(mut_idx, false);

            if (TOTAL_MEMORY_BYTE_USING_CACHE)
                cache->insert(key, val);

            return true;
        }
		bucket_unlock(mut_idx, false);

        // query in stash
        std::string skey(key, KEY_LEN);
		stash_lock(false);
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            std::string sval = entry->second;
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);
			stash_unlock(false);

            if (TOTAL_MEMORY_BYTE_USING_CACHE)
                cache->insert(key, val);

            return true;
        }
		stash_unlock(false);

        //miss
        return false;
    }

    /* Delete an entry given a key */
    bool deletion(char* key, int tid, bool test_flag=false) {
        //query h1
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx, true);

        Entry tmp_bucket[2][2*N];
        int b_id[2] = {min(h1_idx1, h1_idx2), min(h2_idx1, h2_idx2)};
        RDMA_read_bucket(tmp_bucket, b_id, 2, 2, tid);

        int cell;
        cell = check_in_table(h1, key, tmp_bucket[0]);
        if(cell != -1) {
            if(cell < N) {
                if (!test_flag) {
                    //memset(table.bucket[h1_idx1].key[cell], 0, KEY_LEN);
                    table.bucket[h1_idx1].full[cell] = 0;
                    /* RDMA write: 0 to table, bucket h1_idx1, cell cell*/
                    RDMA_write(0, h1_idx1, cell, Entry({{0},{0}}), tid);
                }
                else {
                    RDMA_write(0, h1_idx1, cell, tmp_bucket[0][cell], tid);
                }
            }
            else {
                if (!test_flag) {
                    //memset(table.bucket[h1_idx2].key[cell-N], 0, KEY_LEN);
                    table.bucket[h1_idx2].full[cell-N] = 0;
                    /* RDMA write: 0 to table, bucket h1_idx2, cell cell-N*/
                    RDMA_write(0, h1_idx2, cell-N, Entry({{0},{0}}), tid);
                }
                else {
                    RDMA_write(0, h1_idx2, cell-N, tmp_bucket[0][cell], tid);
                }
            }
            bucket_unlock(mut_idx, true);
            return true;
        }

        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
            if(cell < N) {
                if (!test_flag) {
                    //memset(table.bucket[h2_idx1].key[cell], 0, KEY_LEN);
                    table.bucket[h2_idx1].full[cell] = 0;
                    /* RDMA write: 0 to table, bucket h2_idx1, cell cell*/
                    RDMA_write(0, h2_idx1, cell, Entry({{0},{0}}), tid);
                }
                else {
                    RDMA_write(0, h2_idx1, cell, tmp_bucket[1][cell], tid);
                }
            }
            else {
                if (!test_flag) {
                    //memset(table.bucket[h2_idx2].key[cell-N], 0, KEY_LEN);
                    table.bucket[h2_idx2].full[cell-N] = 0;
                    /* RDMA write: 0 to table, bucket h2_idx2, cell cell-N*/
                    RDMA_write(0, h2_idx2, cell-N, Entry({{0},{0}}), tid);
                }
                else {
                    RDMA_write(0, h2_idx2, cell-N, tmp_bucket[1][cell], tid);
                }
            }
            bucket_unlock(mut_idx, true);
            return true;
        }
		bucket_unlock(mut_idx, true);

        //query in stash, if find then delete
        std::string skey(key, KEY_LEN);
		stash_lock(true);
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            if (!test_flag) {
            stash.erase(entry);
            stash_num--;
            }
			stash_unlock(true);
            return true;
        }
		stash_unlock(true);

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

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx, true);

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
            bucket_unlock(mut_idx, true);
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
            bucket_unlock(mut_idx, true);
            return true;
        }

		bucket_unlock(mut_idx, true);

        // query in stash
        stash_lock(true);
        std::string skey(entry.key, KEY_LEN);
        auto it = stash.find(skey);
        if(it != stash.end()) {
            std::string sval = entry.val;
            it->second = sval;
            stash_unlock(true);
            return true;
        }
        stash_unlock(true);

        //miss
        return false;
    }

    bool checkInsert(Entry& entry, int tid) {
        //query h1
        int h1 = hash1(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        //query h2
        int h2 = hash2(entry.key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        std::set<int> mut_idx({h1_idx1,h1_idx2,h2_idx1,h2_idx2});
        bucket_lock(mut_idx, true);

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
            bucket_unlock(mut_idx, true);
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
            bucket_unlock(mut_idx, true);
            return true;
        }

        // query in stash
        stash_lock(true);
        std::string skey(entry.key, KEY_LEN);
        auto it = stash.find(skey);
        if(it != stash.end()) {
            std::string sval = entry.val;
            it->second = sval;
            stash_unlock(true);
            return true;
        }
        stash_unlock(true);

        // if not found, insert

        //if h1 has more empty cell, insert key into group 1
        if((table.cell[h1_idx1]+table.cell[h1_idx2] < 2*N) && (table.cell[h1_idx1]+table.cell[h1_idx2] <= table.cell[h2_idx1]+table.cell[h2_idx2])) {
            //try to insert into main bucket first
            if(table.cell[h1_idx1] < N) {
                //memcpy(table.bucket[h1_idx1].key[table.cell[h1_idx1]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx1, cell table.cell[h1_idx1]*/
                RDMA_write(0, h1_idx1, table.cell[h1_idx1], entry, tid);
                
                table.cell[h1_idx1]++;
                if(table.cell[h1_idx1] == N);

				bucket_unlock(mut_idx, true);
                return true;
            }
            //then the overflow bucket
            if(table.cell[h1_idx2] < N) {
                //memcpy(table.bucket[h1_idx2].key[table.cell[h1_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx2, cell table.cell[h1_idx2]*/
                RDMA_write(0, h1_idx2, table.cell[h1_idx2], entry,tid);
                
                table.cell[h1_idx2]++;
                if(table.cell[h1_idx2] == N);

				bucket_unlock(mut_idx, true);
                return true;
            }
            //error
			bucket_unlock(mut_idx, true);
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

				bucket_unlock(mut_idx, true);
                return true;
            }
            //then the overflow bucket
            if(table.cell[h2_idx2] < N) {
                //memcpy(table.bucket[h2_idx2].key[table.cell[h2_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx2, cell table.cell[h2_idx2]*/
                RDMA_write(0, h2_idx2, table.cell[h2_idx2], entry, tid);

                table.cell[h2_idx2]++;
                if(table.cell[h2_idx2] == N);

				bucket_unlock(mut_idx, true);
                return true;
            }
            //error
			bucket_unlock(mut_idx, true);
            return false;
        }

        bucket_unlock(mut_idx, true);

        //both group full, insert into stash
        if(insert_to_stash(entry))
            return true;
        return false;
    }

    /* Expand table to n times with sending message in TCP with fd sock */
    /* Expand2, the basic expand method that RACE used */
    /* Return <copy time, clean time> as the result */
    std::pair<long long, long long> expand2(int n, int sock) {
        timespec time1, time2;
        long long resns;
        double copy_time, clean_time;

        clock_gettime(CLOCK_MONOTONIC, &time1);
        //sleep(1);
        // send an extension message to server
        if (send_msg("expand2", sock) <= 0) {
            //std::cout << "TCP send error" << std::endl;
            return make_pair(-1.0, -1.0); 
        }

        int old_bucket_number = bucket_number;
        bucket_number = bucket_number*2;
        group_number = bucket_number/3*2;

        // allocate resources in local
        Bucket *tmp_bucket = new Bucket[bucket_number];
        memcpy(tmp_bucket, table.bucket, old_bucket_number*sizeof(Bucket));
        //memcpy(tmp_bucket+old_bucket_number, table.bucket, old_bucket_number*sizeof(Bucket));
        memset(tmp_bucket+old_bucket_number, 0, old_bucket_number*sizeof(Bucket));

        delete [] table.bucket;
        table.bucket = tmp_bucket;

        uint32_t *tmp_cell = new uint32_t[bucket_number];
        memcpy(tmp_cell, table.cell, old_bucket_number*sizeof(uint32_t));
        //memcpy(tmp_cell+old_bucket_number, table.cell, old_bucket_number*sizeof(uint8_t));
        memset(tmp_cell+old_bucket_number, 0, old_bucket_number*sizeof(uint32_t));

        delete [] table.cell;
        table.cell = tmp_cell;

        #ifndef RW_LOCK_RACE
        mutex *tmp_mut = new mutex[bucket_number];
        delete [] bucket_mut;
        bucket_mut = tmp_mut;
        #endif

        #ifdef RW_LOCK_RACE
        shared_mutex *tmp_mut = new shared_mutex[bucket_number];
        delete [] bucket_mut;
        bucket_mut = tmp_mut;
        #endif
        // allocate remote resources
        expand_remote(n);

        clock_gettime(CLOCK_MONOTONIC, &time2);
        resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
        copy_time = (resns/1000);

        std::cout << "Copy finished" << std::endl;

        int read_cnt = 0;
        clock_gettime(CLOCK_MONOTONIC, &time1);

        // read 5 buckets one time, and clean remote info
        for (int j = 0; j < old_bucket_number; j += 5) {
            Entry (*tmp_entry)[2][N];

            ra[0][0] = get_offset_table(0, j, 0);
            read_cnt += get_clean_info();
            //read_cnt += 5;
            tmp_entry = (Entry (*)[2][N])clean_buf;
            //memset(clean_buf[1], 0, sizeof(Entry)*N*5);

            // check if new hash(key) == old hash(key) 
            for (int k = 0; k < 5; ++k) {
                int i = j+k;

                int h1, h2;
                int h1_idx1, h1_idx2;
                int h2_idx1, h2_idx2;

                int l = 0;
                while (l < table.cell[i]) {
                    h1 = hash1(tmp_entry[0][k][l].key);
                    h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
                    h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);
                    
                    h2 = hash2(tmp_entry[0][k][l].key);
                    h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
                    h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

                    if (i == h1_idx1 || i == h1_idx2 || i == h2_idx1 || i == h2_idx2) {
                        l++;
                    }
                    else {
                        memcpy(&tmp_entry[1][k][table.cell[i+old_bucket_number]], &tmp_entry[0][k][l], sizeof(Entry));
                        if (l != table.cell[i]-1) {
                            memcpy(&tmp_entry[0][k][l], &tmp_entry[0][k][l+1], sizeof(Entry)*(table.cell[i]-1-l));
                        }
                        else {
                            //memset(&tmp_entry[0][k][l], 0, sizeof(Entry));
                            table.bucket[i].full[l] = false;
                        }

                        table.bucket[i+old_bucket_number].full[table.cell[i+old_bucket_number]] = true;
                        table.cell[i]--;
                        table.cell[i+old_bucket_number]++;
                    }
                }
                //memset(&tmp_entry[0][k][table.cell[i]], 0, sizeof(Entry)*(N-table.cell[i]));
            }
                
            ra[0][0] = get_offset_table(0, j, 0);
            ra[0][1] = get_offset_table(0, j+old_bucket_number, 0);
            mod_clean_info();
        }

        clock_gettime(CLOCK_MONOTONIC, &time2);

        resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
        clean_time = (resns/1000);
        
        std::cout << "Clean finished" << std::endl;
        /*std::cout << "Read: " << read_cnt << " times" << std::endl;
        std::cout << "Buckets: " << read_cnt*5 << std::endl;
        std::cout << "Cells: " << read_cnt*5*N << std::endl;*/

        return std::make_pair(copy_time, clean_time);
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

        #ifndef RW_LOCK_RACE
		this->bucket_mut = new mutex[this->bucket_number];
        #endif

        #ifdef RW_LOCK_RACE
        this->bucket_mut = new shared_mutex[this->bucket_number];
        #endif

        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            int cache_memory = TOTAL_MEMORY_BYTE_USING_CACHE - 0;
            cache = new SimpleLRUCache::Cache(cache_memory);
        }
    }

    ~RACETable() {
        delete [] table.cell;
        delete [] table.bucket;
		delete [] bucket_mut;
        stash.clear();
    }
};

}