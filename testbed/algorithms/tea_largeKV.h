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

#define RW_LOCK_TEA

/* Define TEA class in namespace TEA */
namespace TEA {

/* table parameters */
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8                 // item(cell) number in a bucket
#endif
#define STASH_SIZE_TEA 60000      // size of stash

// if not equal to 0, using cache
uint32_t TOTAL_MEMORY_BYTE_USING_CACHE = 0; // 65'000'000

struct Bucket{
    /* These should be in remote for real implement */
    /* These are in remote */
    char key[N][KEY_LEN];   //fingerprints are put in here
    char val[N][VAL_LEN];   //values are put here
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

	#ifndef RW_LOCK_TEA
	mutex stash_mut;        // mutex for stash visition
    mutex *bucket_mut;      // mutex for bucket visition
    #endif

    #ifdef RW_LOCK_TEA
    shared_mutex stash_mut;
    shared_mutex *bucket_mut;
    #endif

    int thread_num; // the number of threads we use

    int viscnt = 0; // for memory access test

    SimpleLRUCache::Cache *cache;

private:
	/* function to lock and unlock buckets */
	void inline bucket_lock(int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            bucket_mut[bucket_idx].lock();
        }
        #endif

        #ifdef RW_LOCK_TEA
        if (thread_num > 1) {
            if(write_flag) bucket_mut[bucket_idx].lock();
            else bucket_mut[bucket_idx].lock_shared();
        }
        #endif
    }
    void inline bucket_unlock(int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            bucket_mut[bucket_idx].unlock();
        }
        #endif

        #ifdef RW_LOCK_TEA
        if (thread_num > 1) {
            if(write_flag) bucket_mut[bucket_idx].unlock();
            else bucket_mut[bucket_idx].unlock_shared();
        }
        #endif
    }
    void inline bucket_lock(const std::set<int> &mut_idx, bool write_flag) {
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].lock();
        }
        #endif

        #ifdef RW_LOCK_TEA
        if (thread_num > 1) {
            for (const auto &i : mut_idx) {
                if(write_flag) bucket_mut[i].lock();
                else bucket_mut[i].lock_shared();
            }
        }
        #endif
    }
    void inline bucket_unlock(const std::set<int> &mut_idx, bool write_flag) {
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].unlock();
        }
        #endif

        #ifdef RW_LOCK_TEA
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
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            stash_mut.lock();
        }
        #endif

        #ifdef RW_LOCK_TEA
        if (thread_num > 1) {
            if(write_flag) stash_mut.lock();
            else stash_mut.lock_shared();
        }
        #endif
    }
    void inline stash_unlock(bool write_flag) {
        #ifndef RW_LOCK_TEA
        if (thread_num > 1) {
            stash_mut.unlock();
        }
        #endif

        #ifdef RW_LOCK_TEA
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
		stash_lock(true);
        if(stash_num >= STASH_SIZE_TEA) {
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
        int vislast = viscnt;

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
            bucket_lock(mut_idx, true);

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
                bucket_unlock(mut_idx, true);

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
                bucket_unlock(mut_idx, true);
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
                bucket_unlock(mut_idx, true);
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
            bucket_unlock(mut_idx, true);
        }
        //max kick reached
		
        if(insert_to_stash(tmp_entry))
            return true;
        return false;
    }

    /* Query key and put result in val */
    bool query(char *key, char *val, int tid) {
        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            if (cache->query(key, val)) return true;
        }

        //query in table
        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;
        int cell;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx, false);

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
            bucket_unlock(mut_idx, false);

            if (TOTAL_MEMORY_BYTE_USING_CACHE)
                cache->insert(key, val);

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
            stash_unlock(false);
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);

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
        //query in table
        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx, true);

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
            if (!test_flag) {
                //memset(table.bucket[h1].key[cell], 0, KEY_LEN);
                table.bucket[h1].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h1, cell cell*/
                RDMA_write(0, h1, cell, Entry({{0},{0}}), tid);
            }
            else {
                RDMA_write(0, h1, cell, tmp_bucket[0][cell], tid);
            }

			bucket_unlock(mut_idx, true);
            return true;
        }

        //query in table
        cell = check_in_table(h2, key, tmp_bucket[1]);
        if(cell != -1) {
            if (!test_flag) {
                //memset(table.bucket[h2].key[cell], 0, KEY_LEN);
                table.bucket[h2].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h2, cell cell*/
                RDMA_write(0, h2, cell, Entry({{0},{0}}), tid);
            }
            else {
                RDMA_write(0, h2, cell, tmp_bucket[1][cell], tid);
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
        int h1 = hash1(entry.key);
        int h2 = (h1 + 1) % bucket_number;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx, true);

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
            
			bucket_unlock(mut_idx, true);
            return true;
        }

        cell = check_in_table(h2, entry.key, tmp_bucket[1]);
        if(cell != -1) {
            //memcpy(table.bucket[h2].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h2, cell cell*/
            RDMA_write(0, h2, cell, entry, tid);
            
			bucket_unlock(mut_idx, true);
            return true;
        }
		bucket_unlock(mut_idx, true);

        // query in stash
        std::string skey(entry.key, KEY_LEN);
		stash_lock(true);
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
        int h1 = hash1(entry.key);
        int h2 = (h1 + 1) % bucket_number;

        std::set<int> mut_idx({h1,h2});
        bucket_lock(mut_idx, true);

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
            
			bucket_unlock(mut_idx, true);
            return true;
        }

        cell = check_in_table(h2, entry.key, tmp_bucket[1]);
        if(cell != -1) {
            //memcpy(table.bucket[h2].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h2, cell cell*/
            RDMA_write(0, h2, cell, entry, tid);
            
			bucket_unlock(mut_idx, true);
            return true;
        }
		

        // query in stash
        std::string skey(entry.key, KEY_LEN);
		stash_lock(true);
        auto it = stash.find(skey);
        if(it != stash.end()) {
            std::string sval = entry.val;
            it->second = sval;
			stash_unlock(true);
            return true;
        }
		stash_unlock(true);

        // if not found, insert
        int tries = 0;
        Entry tmp_entry;
        memcpy(tmp_entry.key, entry.key, KEY_LEN*sizeof(char));
        memcpy(tmp_entry.val, entry.val, VAL_LEN*sizeof(char));
        Entry victim;

        // do cuckoo kick
        while(tries < max_kick_number)
        {
            if (tries > 0) {
                h1 = hash1(tmp_entry.key);
                h2 = (h1 + 1) % bucket_number; //next bucket is the alternate bucket in TEA.
            }

            std::set<int> mut_idx_insert({h1,h2});
            if (tries > 0)
                bucket_lock(mut_idx_insert, true);

            /* RDMA read: table, bucket h1, h2*/
            /* NOTICE: this is only for simulation*/
            // If h1 is next to h2, read once, otherwise twice
            if (tries > 0) {
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
                bucket_unlock(mut_idx_insert, true);
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
                bucket_unlock(mut_idx_insert, true);
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
            bucket_unlock(mut_idx_insert, true);
        }
        //max kick reached
		
        if(insert_to_stash(tmp_entry))
            return true;
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

		#ifndef RW_LOCK_TEA
		this->bucket_mut = new mutex[this->bucket_number];
        #endif

        #ifdef RW_LOCK_TEA
        this->bucket_mut = new shared_mutex[this->bucket_number];
        #endif

        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            int cache_memory = TOTAL_MEMORY_BYTE_USING_CACHE - 0;
            cache = new SimpleLRUCache::Cache(cache_memory);
        }
    }

    ~TEATable() {
        delete [] table.cell;
        delete [] table.bucket;
		delete [] bucket_mut;
        stash.clear();
    }
};

}