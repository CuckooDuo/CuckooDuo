/*
 * Class declaraiton and definition for CuckooDuo with siglen change experiment
 * 
 */

#include "murmur3.h"
#include <iostream>
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
#include <queue>
#include <bitset>
#include <mutex>
#include <shared_mutex>
#pragma once

#include "../rdma/rdma_client.h"
#include "SimpleCache.h"

#include <functional>

/* entry parameters */
#ifndef _ENTRY_H_
#define _ENTRY_H_
#define KEY_LEN 8
#define VAL_LEN 8
#define KV_LEN (KEY_LEN + VAL_LEN)

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
#endif

#define RW_LOCK_CK

/* Define CuckooDuo class in namespace CK */
namespace CK {

template <typename T>
inline void hash_combine(std::size_t &seed, const T &val) {
    seed ^= std::hash<T>()(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}
// auxiliary generic functions to create a hash value using a seed
template <typename T> inline void hash_val(std::size_t &seed, const T &val) {
    hash_combine(seed, val);
}
template <typename T, typename... Types>
inline void hash_val(std::size_t &seed, const T &val, const Types &... args) {
    hash_combine(seed, val);
    hash_val(seed, args...);
}

template <typename... Types>
inline std::size_t hash_val(const Types &... args) {
    std::size_t seed = 0;
    hash_val(seed, args...);
    return seed;
}

// if not equal to 0, using cache
uint32_t TOTAL_MEMORY_BYTE_USING_CACHE = 0; // 65'000'000

//#define SIG_BIT 16
uint32_t SIG_BIT = 16;
//static_assert((SIG_BIT > 0 && SIG_BIT <= 32), "SIG_BIT must be (0, 32]");
#define SIG_LEN 4 //((SIG_BIT - 1) / 8 + 1)
#define SIG_TYPE uint32_t
#define DEFAULT_INVALID_SIG 0xffffffff
#define SIG_MASK (SIG_TYPE)((((SIG_TYPE)1) << CK::SIG_BIT) - 1)

#define KEY_TYPE uint64_t
#define VAL_TYPE uint64_t
//#define SIG_TYPE uint16_t
#define BOOL_ARR uint8_t
#define indexType uint32_t

inline KEY_TYPE turnInKV(const char *KV){
    /*KEY_TYPE InKV = 0;
    int byte = KEY_LEN;
    while(--byte >= 0) {
        InKV = InKV << 8;
        InKV += (unsigned char)KV[byte];
    }*/
    //KEY_TYPE InKV = *(KEY_TYPE*) KV;
    return *(KEY_TYPE*) KV;
}

inline char *turnOutKV(const KEY_TYPE& KV){
    /*char *OutKV = new char[KEY_LEN];
    int byte = -1;
    while(++byte < KEY_LEN) {
        OutKV[byte] = KV & 0b11111111;
        KV = KV >> 8;
    }*/
    return (char *)(&KV);
}

struct CK_Entry{
    KEY_TYPE key;
    VAL_TYPE val;
}emptyEntry;
typedef std::pair<int, std::pair<CK_Entry, CK_Entry> > rebuildInfo;

CK_Entry inline turnInEntry(const Entry& OutEntry){
    CK_Entry result = (CK_Entry){
        turnInKV(OutEntry.key),
        turnInKV(OutEntry.val)
    };
    return result;
}

// 3000'0000 < 2^25 = 1 + 21 + 3
// tbbbbbbbbbbbbbbbbbbbbbccc
#define getTableID(x) ((((indexType)(x)) >> 24) & 1)
#define getBucketID(x) ((((indexType)(x)) >> 3) & 0x1fffff)
#define getCellID(x) (((indexType)(x)) & 7)
#define getIndexType(x, y, z) (((((x) << 21) | (y)) << 3) | (z))
#define emptyIndex (((indexType)0xfffffffe))
#define indexTypeNotFound (((indexType)0xffffffff))
#define maxIndexType ((indexType)0x01ffffff)

/* table parameters */
#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N MY_BUCKET_SIZE                // item(cell) number in a bucket (TABLE1)
#endif
#define M (MY_BUCKET_SIZE*3/4)          // item(cell) number in a bucket using hash1 (N-M for hash2) (TABLE1)
#define L MY_BUCKET_SIZE                // item(cell) number in a bucket (TABLE2)
#define maxNL MY_BUCKET_SIZE            // max(N, L)
//#define SIG_LEN 2                       // sig(fingerprint) length: 16 bit

int test_stash_size = 100;
#define STASH_SIZE_CK test_stash_size       // size of stash
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2
#define rebuildError -1
#define rebuildOK 1
#define rebuildNeedkick 0
#define rebuildNeedTwokick 2

// following params are fixed:
// static_assert(MY_BUCKET_SIZE == 8, "MY_BUCKET_SIZE must be 8");
// static_assert(KEY_LEN == 8, "MY_BUCKET_SIZE must be 8");
// static_assert(VAL_LEN == 8, "MY_BUCKET_SIZE must be 8");
// static_assert(SIG_LEN == 2, "SIG_LEN must be 2");
// end fixed params

/* bucket definition */
struct Bucket{
    //KEY_TYPE key[maxNL];
    SIG_TYPE sig[maxNL];        //fingerprints are put in here
    BOOL_ARR full;              //whether the cell is full or not
    BOOL_ARR fixed;
};

#define isFull(x, y) ((((BOOL_ARR)(x)) >> (y)) & 1)
#define setFull(x, y, z) x = ((x) & (0xff ^ (1 << (y)))) | (((BOOL_ARR)(z)) << (y))
#define isFixed(x, y) ((((BOOL_ARR)(x)) >> (y)) & 1)
#define setFixed(x, y, z) x = ((x) & (0xff ^ (1 << (y)))) | (((BOOL_ARR)(z)) << (y))
#define isFullBoolArray(x) ((x) == 0xff)

struct Table
{
    Bucket *bucket;
    uint8_t *cell;        // occupied cell number of each bucket
};

class CuckooHashTable{
public:
    int bucket_number;
    int max_kick_number;
    Table table[2];
    std::unordered_map<KEY_TYPE, VAL_TYPE> stash;  //overflow cam

    uint32_t seed_hash_to_fp, seed_hash_to_fp2;
    uint32_t seed_hash_to_bucket;
    uint32_t seed_hash_to_alt;

    int stash_num;  // the number of entries in stash
    int move_num, max_move_num, sum_move_num; 
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; 
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; 

    #ifndef RW_LOCK_CK
	mutex stash_mut;        // mutex for stash visition
    mutex *bucket_mut[2];   // mutex for bucket visition
    #endif

    #ifdef RW_LOCK_CK
    shared_mutex stash_mut;
    shared_mutex *bucket_mut[2];
    #endif

    SimpleLRUCache::Cache *cache;

    int thread_num;     // the number of threads we use
    int connect_num;    // the number of threads we have

    int viscnt = 0; // for memory access test

    int elecnt = 0; // for test

private:
    /* function to lock and unlock buckets */
    bool inline bucket_lock(int table_idx, int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            bucket_mut[table_idx][bucket_idx].lock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            if (write_flag) bucket_mut[table_idx][bucket_idx].lock();
            else bucket_mut[table_idx][bucket_idx].lock_shared();
        }
        #endif
        return true;
    }
    bool inline bucket_unlock(int table_idx, int bucket_idx, bool write_flag) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            bucket_mut[table_idx][bucket_idx].unlock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            if (write_flag) bucket_mut[table_idx][bucket_idx].unlock();
            else bucket_mut[table_idx][bucket_idx].unlock_shared();
        }
        #endif
        return true;
    }
    bool inline bucket_lock(const std::set<pair<int,int>> &mut_idx, bool back_flag) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                if (back_flag) {
                    if (!bucket_mut[i.first][i.second].try_lock()) {
                        // if we can't lock each mutex, we release all of them
                        for (const auto &j: mut_idx) {
                            if (i == j) return false;
                            bucket_mut[j.first][j.second].unlock();
                        }
                    }
                }
                else
                    bucket_mut[i.first][i.second].lock();
            }
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                if (back_flag) {
                    if (!bucket_mut[i.first][i.second].try_lock()) {
                        // if we can't lock each mutex, we release all of them
                        for (const auto &j: mut_idx) {
                            if (i == j) return false;
                            bucket_mut[j.first][j.second].unlock();
                        }
                    }
                }
                else
                    bucket_mut[i.first][i.second].lock();
            }
        }
        #endif
        return true;
    }
    bool inline bucket_unlock(const std::set<pair<int,int>> &mut_idx) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                bucket_mut[i.first][i.second].unlock();
            }
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                bucket_mut[i.first][i.second].unlock();
            }
        }
        #endif
        return true;
    }

    /* function to lock and unlock stash */
    bool inline stash_lock(bool write_flag) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            stash_mut.lock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            if(write_flag) stash_mut.lock();
            else stash_mut.lock_shared();
        }
        #endif
        return true;
    }
    bool inline stash_unlock(bool write_flag) {
        #ifndef RW_LOCK_CK
        if (thread_num > 1) {
            stash_mut.unlock();
        }
        #endif

        #ifdef RW_LOCK_CK
        if (thread_num > 1) {
            if(write_flag) stash_mut.unlock();
            else stash_mut.unlock_shared();
        }
        #endif
        return true;
    }

    void initialize_hash_functions(){
        std::set<int> seeds;
        //srand((unsigned)time(NULL));
        //srand(20241009);
        srand(100);

        uint32_t seed = rand()%MAX_PRIME32;
        seed_hash_to_fp = seed;
        seeds.insert(seed);
        
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_fp2 = seed;
        seeds.insert(seed);
        
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_bucket = seed;
        seeds.insert(seed);

        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_alt = seed;
        seeds.insert(seed);
    }

    SIG_TYPE calculate_fp(KEY_TYPE key, int fphash) {
        //auto OutKey = turnOutKV(key);
        //uint32_t fp = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp);
        //if (fphash == 1) return fp & (65535);
        //else return fp >> 16;

        auto OutKey = turnOutKV(key);
        SIG_TYPE fp = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp + (fphash == 1 ? 1 : 2));
        return fp;
        //return fp & SIG_MASK;
    }

    void calculate_two_fp(KEY_TYPE key, SIG_TYPE &fp1, SIG_TYPE &fp2) {
        //auto OutKey = turnOutKV(key);
        //uint32_t fp = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp);
        //fp1 = fp & (65535);
        //fp2 = fp >> 16;

        auto OutKey = turnOutKV(key);
        //fp1 = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp + 1) & SIG_MASK;
        //fp2 = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp + 2) & SIG_MASK;
        fp1 = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp + 1);
        fp2 = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp + 2);
    }

    // return bucket index for table 1.
    int hash1(KEY_TYPE key) {
        auto OutKey = turnOutKV(key);
        int result = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_bucket) % bucket_number;
        return result;
    }

    // return alternate index for h_now, use h_now mod+/- hash(fp).
    int hash_alt(int h_now, int table_now, SIG_TYPE fp) {
        if(table_now == TABLE1)
            return (h_now + (int(fp % bucket_number))) % bucket_number;
        else if(table_now == TABLE2) {
            int h_alt = (h_now - (int(fp % bucket_number))) % bucket_number; // copy wrong
            if(h_alt < 0) h_alt += bucket_number;
            return h_alt;
        }
        return -1;
    }

    // insert an full entry to stash
    bool insert_to_stash(const CK_Entry& entry) {
		stash_lock(true);
        if(stash_num >= STASH_SIZE_CK) {
			stash_unlock(true);
            return false;
		}
        stash.emplace(entry.key, entry.val);
        stash_num++;
		stash_unlock(true);
        return true;
    }

    // given a sig, check whether table[TABLE1] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table1(int b, SIG_TYPE sig, SIG_TYPE sig2) {
        for(int i = M; i < N; i++) {
            if (isFull(table[TABLE1].bucket[b].full, i) 
            && (sig2 == table[TABLE1].bucket[b].sig[i]))
                return i;
        }
        for(int i = 0; i < M; i++) {
            if (isFull(table[TABLE1].bucket[b].full, i) 
            && (sig == table[TABLE1].bucket[b].sig[i]))
                return i;
        }
        return -1;
    }

    // given a sig, check whether table[TABLE2] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table2(int b, SIG_TYPE sig) {
        for(int i = 0; i < L; i++) {
            if (isFull(table[TABLE2].bucket[b].full, i) 
            && (sig == table[TABLE2].bucket[b].sig[i]))
                return i;
        }
        return -1;
    }

    // CPU write, write sig at (table,bucket,cell) in local index
    inline void CPU_write(int tableID, int bucketID, int cellID, CK_Entry entry, 
    SIG_TYPE sig = DEFAULT_INVALID_SIG, SIG_TYPE sig2 = DEFAULT_INVALID_SIG){
        if (sig2 != DEFAULT_INVALID_SIG){
            table[tableID].bucket[bucketID].sig[cellID] = (tableID == TABLE2 || cellID < M) ? sig : sig2;
        } else if (sig != DEFAULT_INVALID_SIG){
            table[tableID].bucket[bucketID].sig[cellID] = sig;
        } else {
            table[tableID].bucket[bucketID].sig[cellID] = 
                calculate_fp(entry.key, (tableID == TABLE2 || cellID < M) ? 1 : 2);
        }
    }

    // RDMA write, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, const Entry& entry, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        ++viscnt;
        if (&entry != &write_buf[tid][0]) {
            memcpy(&write_buf[tid][0], &entry, KV_LEN);
            //*(uint64_t*)write_buf[tid][0].key = *(uint64_t*)entry.key;
        }
        mod_remote(1, tid);
    }
    // RDMA read, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        ++viscnt;
        get_remote(1, tid);
        if (&entry != &read_buf[tid][0]) {
            memcpy(&entry, &read_buf[tid][0], KV_LEN);
            //*(uint64_t*)entry.key = *(uint64_t*)read_buf[tid][0].key;
        }
    }
    // RDMA read, read entries from (table,bucket)
    inline void RDMA_read_bucket(Entry *entry, int tableID, int bucketID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, 0, 1);
        ++viscnt;
        get_bucket(1, 1, tid);
        memcpy(entry, bucket_buf[tid][0], N*KV_LEN);
    }

    // RDMA write, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, const CK_Entry& entry, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        ++viscnt;
        //memcpy(&write_buf[tid][0], &entry, KV_LEN);
        *(CK_Entry*)&write_buf[tid][0] = entry;
        mod_remote(1, tid);
    }
    // RDMA read, read entry from (table,bucket,cell)
    inline void RDMA_read(CK_Entry &entry, int tableID, int bucketID, int cellID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        ++viscnt;
        get_remote(1, tid);
        //memcpy(&entry, &read_buf[tid][0], KV_LEN);
        entry = *(CK_Entry*)&read_buf[tid][0];
    }
    // RDMA read, read entries from (table,bucket)
    inline void RDMA_read_bucket(CK_Entry *entry, int tableID, int bucketID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, 0, 1);
        ++viscnt;
        get_bucket(1, 1, tid);
        memcpy(entry, bucket_buf[tid][0], N*KV_LEN);
    }

    // rebuild bucket with new entry, return (rebuildError, 0) if need insert to cache, 
    // return (rebuildNeedkick, kickEntry) if need kick, return (rebuildOK, 0) if done
    rebuildInfo rebuild(const CK_Entry& entry, int balanceFlag, int tid){
        SIG_TYPE fp, fp2;
        calculate_two_fp(entry.key, fp, fp2);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);

        // lock buckets for rebuild
        std::set<pair<int,int>> mut_idx({std::make_pair(TABLE1,h[TABLE1]),
                                         std::make_pair(TABLE2,h[TABLE2])});
        bucket_lock(mut_idx, false);
        
        int hash1coll = check_in_table2(h[TABLE2], fp);
        Bucket *current_bucket = &table[TABLE1].bucket[h[TABLE1]];
        CK_Entry insertEntry[5];
        // SIG_TYPE insertFp[5];  // 202409 check here insertFp
        int insertCnt = 0, fixedCnt = 0, hash1Cnt = 0;
        for (int i=0; i<6; i++){
            if (isFixed(current_bucket->fixed, i)) fixedCnt++;
            if (isFull(current_bucket->full, i)) hash1Cnt++;
        }
        if (fixedCnt >= 2) fixedCnt -= 2;

        // for record allentry in a bucket
        CK_Entry allEntry[8];

        if (hash1coll != -1){
            // remove from table2, put it in 0~5, put entry in 6~7
            // remove from table2
            RDMA_read(insertEntry[insertCnt], TABLE2, h[TABLE2], hash1coll, tid);
            // insertFp[insertCnt] = table[TABLE2].bucket[h[TABLE2]].sig[hash1coll];
            ++insertCnt;
            setFull(table[TABLE2].bucket[h[TABLE2]].full, hash1coll, 0);
            table[TABLE2].cell[h[TABLE2]]--;
            // replace if 6~7 not empty
            // check entry hash2coll with 0~5
            int entryHash2CollFlag = 0;
            /* read all */
            RDMA_read_bucket(allEntry, TABLE1, h[TABLE1], tid);
            for (int j=0; j<6; j++){
                if (!isFull(current_bucket->full, j)) continue;
                SIG_TYPE fpJ = calculate_fp(allEntry[j].key, 2);
                if (fpJ == fp2){
                    entryHash2CollFlag = 1;
                    j = 8;
                }
            }
            if (entryHash2CollFlag){
                if (isFull(current_bucket->full, 6+fixedCnt)){
                    insertEntry[insertCnt] = allEntry[6+fixedCnt];
                    // insertFp[insertCnt] = calculate_fp(insertEntry[insertCnt].key, 1);
                    ++insertCnt;
                } else {
                    setFull(current_bucket->full, 6+fixedCnt, 1);
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // coll -> 6~7, entry -> coll -> 0~5
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, insertEntry[0], tid);
                CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, insertEntry[0]);
                // current_bucket->sig[6+fixedCnt] = calculate_fp(insertEntry[0].key, 2);
                insertEntry[0] = entry;
                table[TABLE2].bucket[h[TABLE2]].sig[hash1coll] = fp;
            } else {
                if (isFull(current_bucket->full, 6+fixedCnt)){
                    insertEntry[insertCnt] = allEntry[6+fixedCnt];
                    // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                    ++insertCnt;
                } else {
                    setFull(current_bucket->full, 6+fixedCnt, 1);
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // entry -> 6~7
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
                // current_bucket->sig[6+fixedCnt] = fp2;
            }
            // hash1coll -> 0~5
            if (hash1Cnt < 6){
                for (int i=0; i<6; i++){
                    if (isFull(current_bucket->full, i)) continue;
                    ++hash1Cnt;
                    setFull(current_bucket->full, i, 1);
                    table[TABLE1].cell[h[TABLE1]]++;
                    setFixed(current_bucket->fixed, i, 1);
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                    CPU_write(TABLE1, h[TABLE1], i, insertEntry[0], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll]);
                    // current_bucket->sig[i] = table[TABLE2].bucket[h[TABLE2]].sig[hash1coll];
                    break;
                }
            } else {
                for (int i=0; i<6; i++){
                    if (isFixed(current_bucket->fixed, i)) continue; // copy wrong
                    // kick
                    insertEntry[insertCnt] = allEntry[i];
                    ++insertCnt;
                    setFixed(current_bucket->fixed, i, 1);
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                    CPU_write(TABLE1, h[TABLE1], i, insertEntry[0], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll]);
                    // current_bucket->sig[i] = table[TABLE2].bucket[h[TABLE2]].sig[hash1coll];
                    break;
                }
            }
            // kick
            if (insertCnt > 2) {
                bucket_unlock(mut_idx);
                return std::make_pair(rebuildNeedTwokick, std::make_pair(insertEntry[1], insertEntry[2]));
            }
            if (insertCnt > 1) {
                bucket_unlock(mut_idx);
                return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[1], emptyEntry));
            }
            bucket_unlock(mut_idx);
            return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
        } else {
            int table1coll = check_in_table1(h[TABLE1], fp, fp2);
            if (table1coll >= 6){
                /* read all */
                RDMA_read_bucket(allEntry, TABLE1, h[TABLE1], tid);

                if (table1coll == 6 && fixedCnt == 1){
                    // a-b hash1coll, b-entry hash2coll, swap a b and put entry in 0~5
                    // swap a b
                    int entryAID = 0;
                    CK_Entry entryA, entryB;
                    // SIG_TYPE fpA, fpB;
                    while (entryAID < 6 && (!isFull(current_bucket->fixed, entryAID))) ++entryAID;
                    //memcpy(&entryA, &allEntry[entryAID], sizeof(Entry));
                    //memcpy(&entryB, &allEntry[6], sizeof(Entry));
                    //*(uint64_t*)entryA.key = *(uint64_t*)allEntry[entryAID].key;
                    //*(uint64_t*)entryB.key = *(uint64_t*)allEntry[6].key;

                    memcpy(&write_buf[tid][0], &allEntry[6], KV_LEN);
                    memcpy(&write_buf[tid][1], &allEntry[entryAID], KV_LEN);
                    //*(uint64_t*)write_buf[tid][0].key = *(uint64_t*)allEntry[6].key;
                    //*(uint64_t*)write_buf[tid][1].key = *(uint64_t*)allEntry[entryAID].key;

                    //calculate_two_fp(*(KEY_TYPE*)write_buf[tid][1].key, fpInt, fpA);
                    //calculate_two_fp(*(KEY_TYPE*)write_buf[tid][0].key, fpB, fpInt);

                    ra[tid][0] = get_offset_table(TABLE1, h[TABLE1], entryAID, 1);
                    ra[tid][1] = get_offset_table(TABLE1, h[TABLE1], 6, 1);
                    mod_remote(2, tid);
                    CPU_write(TABLE1, h[TABLE1], entryAID, allEntry[6]);
                    CPU_write(TABLE1, h[TABLE1], 6, allEntry[entryAID]);
                    viscnt += 2;

                    // fp_to_sig(current_bucket->sig[entryAID], fpB);
                    // fp_to_sig(current_bucket->sig[6], fpA);

                    // entry -> 0~5
                    if (hash1Cnt < 6){
                        for (int i=0; i<6; i++){
                            if (isFull(current_bucket->full, i)) continue;
                            ++hash1Cnt;
                            setFull(current_bucket->full, i, 1);
                            table[TABLE1].cell[h[TABLE1]]++;
                            setFixed(current_bucket->fixed, i, 1);
                            RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                            CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                            // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                            break;
                        }
                    } else {
                        for (int i=0; i<6; i++){
                            if (isFixed(current_bucket->fixed, i)) continue;
                            // kick
                            insertEntry[insertCnt] = allEntry[i];
                            ++insertCnt;
                            setFixed(current_bucket->fixed, i, 1);
                            RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                            CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                            // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                            break;
                        }
                    }
                    // kick
                    if (insertCnt > 0) {
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    }
                    bucket_unlock(mut_idx);
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
                // remove from hash2, put two entry in 0~5
                // remove from hash2
                insertEntry[insertCnt] = allEntry[table1coll];
                // insertFp[insertCnt] = calculate_fp(insertEntry[insertCnt].key, 1);
                // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                ++insertCnt;
                setFull(current_bucket->full, table1coll, 0);
                table[TABLE1].cell[h[TABLE1]]--;
                
                // entry -> 0~5
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        if (isFull(current_bucket->full, i)) continue;
                        ++hash1Cnt;
                        setFull(current_bucket->full, i, 1);
                        table[TABLE1].cell[h[TABLE1]]++;
                        setFixed(current_bucket->fixed, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (isFixed(current_bucket->fixed, i)) continue;
                        // kick
                        insertEntry[insertCnt] = allEntry[i];
                        ++insertCnt;
                        setFixed(current_bucket->fixed, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                        break;
                    }
                }
                // hash2coll -> 0~5
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        if (isFull(current_bucket->full, i)) continue;
                        setFull(current_bucket->full, i, 1);
                        table[TABLE1].cell[h[TABLE1]]++;
                        setFixed(current_bucket->fixed, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                        CPU_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        // fp_to_sig(current_bucket->sig[i], insertFp[0]);
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (isFixed(current_bucket->fixed, i)) continue;
                        // kick
                        insertEntry[insertCnt] = allEntry[i];
                        ++insertCnt;
                        setFixed(current_bucket->fixed, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                        CPU_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        // fp_to_sig(current_bucket->sig[i], insertFp[0]);
                        break;
                    }
                }
                // kick
                if (insertCnt > 2) {
                    bucket_unlock(mut_idx);
                    return std::make_pair(rebuildNeedTwokick, std::make_pair(insertEntry[1], insertEntry[2]));
                }
                if (insertCnt > 1) {
                    bucket_unlock(mut_idx);
                    return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[1], emptyEntry));
                }
                bucket_unlock(mut_idx);
                return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
            } else if (table1coll >= 0){
                /* read all */
                RDMA_read_bucket(allEntry, TABLE1, h[TABLE1], tid);

                // fix coll, put entry in 6~7
                // check entry hash2coll with 0~5
                int entryHash2CollFlag = 0;
                for (int j=0; j<6; j++){
                    if (!isFull(current_bucket->full, j)) continue;
                    SIG_TYPE fpJ = calculate_fp(allEntry[j].key, 2);
                    if (fpJ == fp2){
                        entryHash2CollFlag = 1;
                        j = 8;
                    }
                }
                if (entryHash2CollFlag){
                    /// swap, entry in 0~5, coll in 6~7
                    CK_Entry collEntry;
                    collEntry = allEntry[table1coll];
                    // SIG_TYPE collEntryFp2 = calculate_fp(collEntry.key, 2);
                    // fix coll, entry -> 0~5
                    setFixed(current_bucket->fixed, table1coll, 1);
                    RDMA_write(TABLE1, h[TABLE1], table1coll, entry, tid);
                    CPU_write(TABLE1, h[TABLE1], table1coll, entry, fp);
                    // fp_to_sig(current_bucket->sig[table1coll], fp);
                    // replace if 6~7 not empty
                    if (isFull(current_bucket->full, 6+fixedCnt)){
                        insertEntry[insertCnt] = allEntry[6+fixedCnt];
                        // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                        ++insertCnt;
                    } else {
                        setFull(current_bucket->full, 6+fixedCnt, 1);
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // coll -> 6~7
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, collEntry, tid);
                    CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, collEntry);
                    // fp_to_sig(current_bucket->sig[6+fixedCnt], collEntryFp2);
                    // kick
                    if (insertCnt > 0) {
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    }
                    bucket_unlock(mut_idx);
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                } else {
                    // fix coll
                    setFixed(current_bucket->fixed, table1coll, 1);
                    // replace if 6~7 not empty
                    if (isFull(current_bucket->full, 6+fixedCnt)){
                        insertEntry[insertCnt] = allEntry[6+fixedCnt];
                        // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                        ++insertCnt;
                    } else {
                        setFull(current_bucket->full, 6+fixedCnt, 1);
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // entry -> 6~7
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                    CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
                    // memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
                    // kick
                    if (insertCnt > 0) {
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    }
                    bucket_unlock(mut_idx);
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
            } else {
                // for read allentry check
                /* read all */
                if (isFull(table[TABLE1].bucket[h[TABLE1]].full, 6) || isFull(table[TABLE1].bucket[h[TABLE1]].full, 7))
                    RDMA_read_bucket(allEntry, TABLE1, h[TABLE1], tid);

                for (int i=6; i<8; i++){
                    if (!isFull(table[TABLE1].bucket[h[TABLE1]].full, i)) continue;
                    CK_Entry collEntry;
                    SIG_TYPE collfp1, collfp2;
                    collEntry = allEntry[i];
                    calculate_two_fp(collEntry.key, collfp1, collfp2);

                    if (collfp1 == fp){
                        if (i == 6 && fixedCnt == 1){
                            // 3 hash1 coll, put entry in 7
                            // replace if 7 not empty
                            if (isFull(current_bucket->full, 6+fixedCnt)){
                                insertEntry[insertCnt] = allEntry[6+fixedCnt];
                                ++insertCnt;
                            } else {
                                setFull(current_bucket->full, 6+fixedCnt, 1);
                                table[TABLE1].cell[h[TABLE1]]++;
                            }
                            // entry -> 7
                            RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                            CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
                            // memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
                        } else {
                            // put entry in 0~5, move coll 6/7 if need
                            // replace if 6/7 not empty
                            if (i == 7 && fixedCnt == 0){
                                // need move
                                // replace 6
                                if (!isFull(current_bucket->full, 6)){
                                    // empty, just move from 7 to 6
                                    CK_Entry tmpEntry;
                                    tmpEntry = allEntry[7];
                                    RDMA_write(TABLE1, h[TABLE1], 6, tmpEntry, tid);
                                    CPU_write(TABLE1, h[TABLE1], 6, tmpEntry, current_bucket->sig[7]);
                                    setFull(current_bucket->full, 6, 1);
                                    setFull(current_bucket->full, 7, 0);
                                    // memcpy(current_bucket->sig[6], current_bucket->sig[7], SIG_LEN*sizeof(char));
                                } else {
                                    // full, swap 6 and 7
                                    //Entry Entry6, Entry7;
                                    SIG_TYPE sig6 = current_bucket->sig[6], sig7 = current_bucket->sig[7];
                                    memcpy(&write_buf[tid][0], &allEntry[7], KV_LEN);
                                    memcpy(&write_buf[tid][1], &allEntry[6], KV_LEN);
                                    //*(uint64_t*)write_buf[tid][0].key = *(uint64_t*)allEntry[7].key;
                                    //*(uint64_t*)write_buf[tid][1].key = *(uint64_t*)allEntry[6].key;
                                    ra[tid][0] = get_offset_table(TABLE1, h[TABLE1], 6, 1);
                                    ra[tid][1] = get_offset_table(TABLE1, h[TABLE1], 7, 1);
                                    mod_remote(2, tid);

                                    CPU_write(TABLE1, h[TABLE1], 6, allEntry[7], sig7);
                                    CPU_write(TABLE1, h[TABLE1], 7, allEntry[6], sig6);
                                    viscnt += 2;

                                }
                            }
                            // entry -> 0~5
                            if (hash1Cnt < 6){
                                for (int i=0; i<6; i++){
                                    if (isFull(current_bucket->full, i)) continue;
                                    ++hash1Cnt;
                                    setFull(current_bucket->full, i, 1);
                                    table[TABLE1].cell[h[TABLE1]]++;
                                    setFixed(current_bucket->fixed, i, 1);
                                    RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                                    CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                                    // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                                    break;
                                }
                            } else {
                                for (int i=0; i<6; i++){
                                    if (isFixed(current_bucket->fixed, i)) continue;
                                    // kick
                                    insertEntry[insertCnt] = allEntry[i];
                                    ++insertCnt;
                                    setFixed(current_bucket->fixed, i, 1);
                                    RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                                    CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                                    // memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                                    break;
                                }
                            }
                        }
                        // kick
                        if (insertCnt > 0) {
                            bucket_unlock(mut_idx);
                            return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                        }
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                // no any coll
                if (balanceFlag && (0 < (L-(int)table[TABLE2].cell[h[TABLE2]]))
                && ((N-(int)table[TABLE1].cell[h[TABLE1]]) <= (L-(int)table[TABLE2].cell[h[TABLE2]]))){
                    for (int i=0; i<8; i++){
                        if (isFull(table[TABLE2].bucket[h[TABLE2]].full, i)) continue;
                        table[TABLE2].cell[h[TABLE2]]++;
                        setFull(table[TABLE2].bucket[h[TABLE2]].full, i, 1);
                        RDMA_write(TABLE2, h[TABLE2], i, entry, tid);
                        CPU_write(TABLE2, h[TABLE2], i, entry, fp);
                        // memcpy(table[TABLE2].bucket[h[TABLE2]].sig[i], sig, SIG_LEN*sizeof(char));
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                } else {
                    // swap follow two parts
                    for (int i=0; i<6; i++){
                        if (isFull(table[TABLE1].bucket[h[TABLE1]].full, i)) continue;
                        table[TABLE1].cell[h[TABLE1]]++;
                        setFull(table[TABLE1].bucket[h[TABLE1]].full, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        // memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig, SIG_LEN*sizeof(char));
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                    for (int i=6; i<8; i++){
                        if (isFull(table[TABLE1].bucket[h[TABLE1]].full, i)) continue;
                        // should check hash2coll with 0~5, if insert 6~7
                        /* read all */
                        //CK_Entry allEntry[8];
                        //RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
                        for (int j=0; j<6; j++){
                            if (!isFull(current_bucket->full, j)) continue;
                            SIG_TYPE fpJ = calculate_fp(allEntry[j].key, 2);
                            if (fpJ == fp2){
                                i = 8;
                                j = 8;
                            }
                        }
                        if (i >= 8) break;
                        table[TABLE1].cell[h[TABLE1]]++;
                        setFull(table[TABLE1].bucket[h[TABLE1]].full, i, 1);
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp2);
                        // memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig2, SIG_LEN*sizeof(char));
                        bucket_unlock(mut_idx);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                    //for (int i=0; i<6; i++){
                    //    if (isFull(table[TABLE1].bucket[h[TABLE1]].full, i)) continue;
                    //    table[TABLE1].cell[h[TABLE1]]++;
                    //    setFull(table[TABLE1].bucket[h[TABLE1]].full, i, 1);
                    //    RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                    //    CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                    //    // memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig, SIG_LEN*sizeof(char));
                    //    bucket_unlock(mut_idx);
                    //    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    //}
                }
                bucket_unlock(mut_idx);
                return std::make_pair(rebuildNeedkick, std::make_pair(entry, emptyEntry));
            }
        }
    }
    
    std::vector<indexType*> visit;
    std::vector<int*> visitStep;
    std::vector<std::deque<indexType>> bfsQueue, dirtyList;
    /* Spread bfs with from(current cell) */
    void bfsSpread(indexType from, int tid){
        int tableID = getTableID(from);
        int bucketID = getBucketID(from);
        int cellID = getCellID(from);
        SIG_TYPE sig = table[tableID].bucket[bucketID].sig[cellID];
        int altBucketID = hash_alt(bucketID, tableID, sig);

        int vid = connect_num-1-tid;

        if (visitStep[vid][from] >= 3) return;
        // in queue elements 8^3 -> 8^2
        if (visitStep[vid][from] == 2 && isFullBoolArray(
            table[tableID^1].bucket[altBucketID].full | 
            table[tableID^1].bucket[altBucketID].fixed)) return;

        for (int i=0; i<((tableID^1) == TABLE1 ? M : L); i++)
        if (!isFixed(table[tableID^1].bucket[altBucketID].fixed, i)){
            indexType target = getIndexType(tableID^1, altBucketID, i);
            if (visit[vid][target] != indexTypeNotFound) continue;
            visit[vid][target] = from;
            visitStep[vid][target] = visitStep[vid][from] + 1;
            if (!isFull(table[tableID^1].bucket[altBucketID].full, i)){
                bfsQueue[vid].push_front(target);
                dirtyList[vid].push_front(target);
                continue;
            }
            if (visitStep[vid][from] == 2) continue;
            bfsQueue[vid].push_back(target);
            dirtyList[vid].push_back(target);
        }
    }

    /* Clean unused info in bfs */
    void cleanVisit(int tid){
        int vid = connect_num-1-tid;
        while (!dirtyList[vid].empty()){
            indexType item = dirtyList[vid].front();
            visit[vid][item] = indexTypeNotFound;
            dirtyList[vid].pop_front();
        }
    }

    /* Do cuckoo kicks in bfs */
    bool bfs(CK_Entry entry, int tid){
        cleanVisit(tid);
        int vid = connect_num-1-tid;

        SIG_TYPE fp = calculate_fp(entry.key, 1);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);
        Bucket *current_bucket1 = &table[TABLE1].bucket[h[TABLE1]];
        Bucket *current_bucket2 = &table[TABLE2].bucket[h[TABLE2]];
        // modify for faster bfs, local is better
        // maybe need to assert bucket element arranged as full cells first
        while (!bfsQueue[vid].empty()) bfsQueue[vid].pop_front();
        int collID = check_in_table2(h[TABLE2], fp);
        if (collID == -1){
            for (int i=0; i<L; i++){
                indexType target = getIndexType(TABLE2, h[TABLE2], i);
                visit[vid][target] = emptyIndex;
                visitStep[vid][target] = 1;
                bfsQueue[vid].push_back(target);
                dirtyList[vid].push_back(target);
            }
        } else {
            //std::cout << "unexpected error: hash1 collision" << std::endl;
            return false;
        }
        for (int i=0; i<M; i++) if (!isFixed(current_bucket1->fixed, i)){
            indexType target = getIndexType(TABLE1, h[TABLE1], i);
            visit[vid][target] = emptyIndex;
            visitStep[vid][target] = 1;
            bfsQueue[vid].push_back(target);
            dirtyList[vid].push_back(target);
        }

        while (!bfsQueue[vid].empty()){// (kick_num < max_kick_number){  //kick
            // kick_num++;
            indexType from = bfsQueue[vid].front();
            bfsQueue[vid].pop_front();

            if (visitStep[vid][from] > max_kick_number) break;
            int tableID = getTableID(from);
            int bucketID = getBucketID(from);
            int cellID = getCellID(from);

            Bucket *BFSbucket = &table[tableID].bucket[bucketID];
            if (!isFull(BFSbucket->full, cellID)){
                // find an empty cell, kick
                indexType indexList[10];
                CK_Entry entryList[10];
                SIG_TYPE sigList[10], sig2List[10];
                int listCnt = 0;

                // lock buckets will be visited in this path
                std::set<pair<int,int>> mut_idx;
                mut_idx.insert(std::make_pair(tableID, bucketID));
                while (1){
                    indexList[listCnt++] = from;
                    mut_idx.insert(std::make_pair(getTableID(from), getBucketID(from)));

                    from = visit[vid][from];
                    if (from == emptyIndex) break;
                }
                // if we can't lock every locks at once, we try another way
                if (!bucket_lock(mut_idx, true)) continue;
                // read entries in path to entryList
                for (int i=0; i<listCnt; i++){
                    if (i+1<listCnt){
                        int tmpTableID = getTableID(indexList[i+1]);
                        int tmpBucketID = getBucketID(indexList[i+1]);
                        int tmpCellID = getCellID(indexList[i+1]);
                        ra[tid][i] = get_offset_table(
                            tmpTableID,
                            tmpBucketID, 
                            tmpCellID, 1);
                        ++viscnt;
                        sigList[i] = table[tmpTableID].bucket[tmpBucketID].sig[tmpCellID];
                    }
                    else{
                        entryList[i] = entry;
                        sigList[i] = fp;
                    }
                }
                get_remote(listCnt-1, tid);
                memcpy(&entryList, &read_buf[tid], KV_LEN*(listCnt-1));

                // check hash2 collision
                int hash2collisionFlag = 0;
                for (int i=0; i<listCnt; i++){
                    // kick to table2 won't cause hash2 collision
                    if (getTableID(indexList[i]) == TABLE2) continue;
                    int tmpTableID = getTableID(indexList[i]);
                    int tmpBucketID = getBucketID(indexList[i]);
                    int tmpCellID = getCellID(indexList[i]);
                    SIG_TYPE fpi2 = calculate_fp(entryList[i].key, 2);
                    sig2List[i] = fpi2;                    
                    for (int j=M; j<N; j++)
                        if (isFull(table[TABLE1].bucket[tmpBucketID].full, j)
                        && (fpi2 == table[TABLE1].bucket[tmpBucketID].sig[j]))
                        hash2collisionFlag = 1;
                    if (hash2collisionFlag) break;
                }
                if (hash2collisionFlag){
                    // planA: insert to cache (86% load ratio, cache explode)
                    // kick_num = max_kick_number + 999;
                    // break;

                    // planB: find another augment path (multi times of RDMA read-write)
                    // continueCounter++;
                    // continue;

                    // planC: small adjustment in TABLE1 bucket, insert to cache if fail
                    // need to modify : RDMA read/write in 1 turn
                    // /*

                    // for doorbell write
                    int wr_idx = 0;

                    for (int i=listCnt-1; i>=0; i--){
                        // kick to table2 won't cause hash2 collision
                        int tmpTableID = getTableID(indexList[i]);
                        int tmpBucketID = getBucketID(indexList[i]);
                        int tmpCellID = getCellID(indexList[i]);
                        Bucket *current_bucket = &table[tmpTableID].bucket[tmpBucketID];
                        if (tmpTableID == TABLE1){
                            int currentBucketCollision = 0;
                            SIG_TYPE fpi2 = sig2List[i];
                            for (int j=M; j<N; j++)
                                if (isFull(current_bucket->full, j)
                                && (fpi2 == current_bucket->sig[j]))
                                currentBucketCollision = 1;
                            if (currentBucketCollision){
                                // clear kicked cell
                                if (isFull(current_bucket->full, tmpCellID)){
                                    setFull(current_bucket->full, tmpCellID, 0);
                                    table[TABLE1].cell[tmpBucketID]--;
                                }
            
                                std::set<pair<int,int>> mut_idx_r({std::make_pair(TABLE1, h[TABLE1]),
                                                                   std::make_pair(TABLE2, h[TABLE2])});
                                // unlock old locks, and lock for build
                                bucket_unlock(mut_idx);
                                //bucket_lock(mut_idx_r, false);
                                // rebuild without balance flag
                                rebuildInfo info = rebuild(entryList[i], 0, tid);
                                //bucket_unlock(mut_idx_r);

                                if (info.first == rebuildOK){
                                    // ajustment success
                                } else {
                                    
                                    if (info.first != rebuildNeedkick){
                                        printf("unexpected: info.first != rebuildNeedkick:%d\n", info.first);
                                        //cleanVisit(tid);
                                        return false;
                                    }
                                    // need to modify : insert 2 -> 1
                                    if(!insert_to_stash(info.second.first)){
                                        //cleanVisit(tid);
                                        return false;
                                    }
                                    if (i == 0) {
                                        //cleanVisit(tid);
                                        return true;
                                    } else {
                                        if(!insert_to_stash(entryList[i-1])){
                                            //cleanVisit(tid);
                                            return false;
                                        }
                                        //cleanVisit(tid);
                                        return true;
                                    }
                                }
                            } else {
                                // write entries in path to new locations
                                CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);
                                memcpy(&write_buf[tid][wr_idx], &entryList[i], KV_LEN);
								ra[tid][wr_idx] = get_offset_table(tmpTableID, 
                                                              tmpBucketID, 
                                                              tmpCellID, 1);
                                ++wr_idx;
                                ++viscnt;
                            }
                        } else {
                            // write entries in path to new locations
                            CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);
                            memcpy(&write_buf[tid][wr_idx], &entryList[i], KV_LEN);
							ra[tid][wr_idx] = get_offset_table(tmpTableID, 
                                                          tmpBucketID, 
                                                          tmpCellID, 1);
                            ++wr_idx;
                            ++viscnt;
                        }
                    }
                    mod_remote(wr_idx, tid);

                    if (!isFull(BFSbucket->full, cellID)){
                        setFull(BFSbucket->full, cellID, 1);
                        table[tableID].cell[bucketID] += 1;
                    }
                    bucket_unlock(mut_idx);

                    //cleanVisit(tid);
                    return true;
                }
                // no hash2 collision
                for (int i=0; i<listCnt; i++){
                    // write entries in path to new locations
                    int tmpTableID = getTableID(indexList[i]);
                    int tmpBucketID = getBucketID(indexList[i]);
                    int tmpCellID = getCellID(indexList[i]);
                    CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);

                    Bucket *current_bucket = &table[tmpTableID].bucket[tmpBucketID];

                    memcpy(&write_buf[tid][i], &entryList[i], KV_LEN);
                    ra[tid][i] = get_offset_table(tmpTableID, 
                                                  tmpBucketID, 
                                                  tmpCellID, 1);
                    ++viscnt;
                }
                mod_remote(listCnt, tid);

                if (!isFull(BFSbucket->full, cellID)){
                    setFull(BFSbucket->full, cellID, 1);
                    table[tableID].cell[bucketID] += 1;
                } else printf("unexpected full bucket cell\n");
                //cleanVisit(tid);

                bucket_unlock(mut_idx);
                return true;
            }
            // if (dirtyList[vid].size() < 50) 
            // spread visit path from current situation
            bfsSpread(from, tid);
        }
        //cleanVisit(tid);
        // insert entry to stash if we can't find location in tables
        if(insert_to_stash(entry))
            return true;
        return false;
    }
public:
    /* Insert an entry */
    bool insert(const Entry& OutEntry, int tid) {
        int vislast = viscnt;

        CK_Entry entry = turnInEntry(OutEntry);
        /*SIG_TYPE fp, fp2;
        calculate_two_fp(entry.key, fp, fp2);
        int h1 = hash1(entry.key);
        int h2 = hash_alt(h1, TABLE1, fp);*/

        // try rebuild with balance flag
        rebuildInfo info = rebuild(entry, 1, tid);

        if (info.first == rebuildOK){
            ++elecnt;
            return true;
        }
        if (info.first == rebuildError){
            if(insert_to_stash(entry)) {
                ++elecnt;
                return true;
            }
            return false;
        }

		// do cuckoo kick with bfs
        bool insertOK = bfs(info.second.first, tid);
        if (!insertOK || info.first == rebuildNeedkick){
            ++elecnt;
            return insertOK;
        }
        insertOK = bfs(info.second.second, tid);
        ++elecnt;
        return insertOK;
    }

    /* Query key and put result in val */
    bool query(const char *key, char *val, int tid) {
        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            if (cache->query(key, val)) return true;
        }
        
        KEY_TYPE InKey = turnInKV(key);

        //query in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InKey, fp, fp2);
        int h1 = hash1(InKey);
        int cell;

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2, false);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            CK_Entry readEntry;
            RDMA_read(readEntry, TABLE2, h2, cell, tid);

			bucket_unlock(TABLE2, h2, false);
            if (InKey == readEntry.key) {
                
                if (TOTAL_MEMORY_BYTE_USING_CACHE)
                    cache->insert(key, val);
                
                return true;
            }
            else if (fp == calculate_fp(readEntry.key, 1)
                && fp2 == calculate_fp(readEntry.key, 2)) {
                
                if (TOTAL_MEMORY_BYTE_USING_CACHE)
                    cache->insert(key, val);
                
                return true;
            }
            else return false;
            //return true;
        }
        bucket_unlock(TABLE2, h2, false);

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1, false);
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            CK_Entry readEntry;
            RDMA_read(readEntry, TABLE1, h1, cell, tid);

			bucket_unlock(TABLE1, h1, false);
            if (InKey == readEntry.key) {
                
                if (TOTAL_MEMORY_BYTE_USING_CACHE)
                    cache->insert(key, val);
                
                return true;
            }
            else if (fp == calculate_fp(readEntry.key, 1)
                && fp2 == calculate_fp(readEntry.key, 2)) {
                
                if (TOTAL_MEMORY_BYTE_USING_CACHE)
                    cache->insert(key, val);
                
                return true;
            }
            else return false;
        }
        bucket_unlock(TABLE1, h1, false);        

        // query in stash
		stash_lock(false);
        auto entry = stash.find(InKey);
        if(entry != stash.end()) {
            if(val != NULL) {
                VAL_TYPE InVal = entry->second;
                auto OutVal = turnOutKV(InVal);
                memcpy(val, OutVal, VAL_LEN);
            }
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
    bool deletion(const char* key, int tid) {
        KEY_TYPE InKey = turnInKV(key);

        //query in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InKey, fp, fp2);
        int h1 = hash1(InKey);
        int cell;

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2, true);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            RDMA_read(read_buf[tid][0], TABLE2, h2, cell, tid);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE2, h2, true);
                return false;
            }

            table[TABLE2].bucket[h2].sig[cell] = 0;
            setFull(table[TABLE1].bucket[h1].full, cell, 0);

			bucket_unlock(TABLE2, h2, true);
            --elecnt;
            return true;
        }
		bucket_unlock(TABLE2, h2, true);

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1, true);
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE1, h1, cell, tid);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE1, h1, true);
                return false;
            }

            table[TABLE1].bucket[h1].sig[cell] = 0;
            setFull(table[TABLE1].bucket[h1].full, cell, 0);
			
			bucket_unlock(TABLE1, h1, true);
            --elecnt;
			return true;
        }
		bucket_unlock(TABLE1, h1, true);

        // query in stash, if find then delete
        stash_lock(true);
        auto entry = stash.find(InKey);
        if(entry != stash.end()) {
            stash.erase(entry);
            stash_num--;
			stash_unlock(true);
            --elecnt;
            return true;
        }
		stash_unlock(true);

        //miss
        return false;
    }

    /* Update an entry */
    bool update(const Entry& OutEntry, int tid) {
        CK_Entry InEntry = turnInEntry(OutEntry);

        //query in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InEntry.key, fp, fp2);
        int h1 = hash1(InEntry.key);
        int cell;

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2, true);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE2, h2, cell, tid);

            if (memcmp(OutEntry.key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE2, h2, true);
                return false;
            }
            RDMA_write(TABLE2, h2, cell, InEntry, tid);

			bucket_unlock(TABLE2, h2, true);
            return true;
        }
		bucket_unlock(TABLE2, h2, true);

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1, true);
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE1, h1, cell, tid);
            
            if (memcmp(OutEntry.key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE1, h1, true);
                return false;
            }
            RDMA_write(TABLE1, h1, cell, InEntry, tid);

			bucket_unlock(TABLE1, h1, true);
            return true;
        }
		bucket_unlock(TABLE1, h1, true);

        // query in stash
		stash_lock(true);
        auto it = stash.find(InEntry.key);
        if(it != stash.end()) {
            it->second = InEntry.val;
			stash_unlock(true);
            return true;
        }
		stash_unlock(true);

        //miss
        return false;
    }

    bool checkInsert(const Entry& entry, int tid) {
        if (update(entry, tid))
            return true;
        return insert(entry, tid);
    }

    /* Expand table to n times with sending message in TCP with fd sock */
    /* Return <copy time, clean time> as the result */
    std::pair<long long, long long> expand(int n, int sock) {
        timespec time1, time2;
        long long resns;
        double copy_time, clean_time;

        clock_gettime(CLOCK_MONOTONIC, &time1);
        //sleep(1);
        // send an extension message to server
        if (send_msg("expand", sock) <= 0) {
            //std::cout << "TCP send error" << std::endl;
            return make_pair(-1.0, -1.0); 
        }

        int old_bucket_number = bucket_number;
        bucket_number = bucket_number*2;

        // allocate resources in local
        for (int i = TABLE1; i <= TABLE2; ++i) {
            Bucket *tmp_bucket = new Bucket[bucket_number];
            memcpy(tmp_bucket, table[i].bucket, old_bucket_number*sizeof(Bucket));
            memcpy(tmp_bucket+old_bucket_number, table[i].bucket, old_bucket_number*sizeof(Bucket));
            //memset(tmp_bucket, 0, bucket_number*sizeof(Bucket));

            delete [] table[i].bucket;
            table[i].bucket = tmp_bucket;

            uint8_t *tmp_cell = new uint8_t[bucket_number];
            memcpy(tmp_cell, table[i].cell, old_bucket_number*sizeof(uint8_t));
            memcpy(tmp_cell+old_bucket_number, table[i].cell, old_bucket_number*sizeof(uint8_t));
            //memset(tmp_cell, 0, bucket_number*sizeof(uint32_t));

            delete [] table[i].cell;
            table[i].cell = tmp_cell;

            #ifndef RW_LOCK_CK
            mutex *tmp_mut = new mutex[bucket_number];
            delete [] bucket_mut[i];
            bucket_mut[i] = tmp_mut;
            #endif

            #ifdef RW_LOCK_CK
            shared_mutex *tmp_mut = new shared_mutex[bucket_number];
            delete [] bucket_mut[i];
            bucket_mut[i] = tmp_mut;
            #endif
        }
        // allocate remote resources
        expand_remote(n);

        clock_gettime(CLOCK_MONOTONIC, &time2);
        resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
        copy_time = (resns/1000);

        std::cout << "Copy finished" << std::endl;

        int read_cnt = 0;
        clock_gettime(CLOCK_MONOTONIC, &time1);

        // read 5 buckets one time, and clean local info
        for (int i = TABLE1; i <= TABLE2; ++i) {
            for (int j = 0; j < old_bucket_number; j += 5) {
                CK_Entry (*tmp_entry)[N];

                ra[0][0] = get_offset_table(i, j, 0, 1);
                read_cnt += get_clean_info();
                //read_cnt += 5;
                tmp_entry = (CK_Entry (*)[N])clean_buf[0];

                // check if new hash(key) == old hash(key) 
                for (int k = 0; k < 5; ++k) {
                    int cell = 0;
                    int h1, h2;
                    int fp;

                    if (i == TABLE1) {
                        for (int l = 0; l < N; ++l) {
                            //if (!isFull(table[TABLE1].bucket[j+k].full, l)) continue;

                            h1 = hash1(tmp_entry[k][l].key);
                            if (h1 == j+k) {
                                table[TABLE1].bucket[j+k+old_bucket_number].sig[l] = 0;
                                setFull(table[TABLE1].bucket[j+k+old_bucket_number].full, l, 0);
                                setFixed(table[TABLE1].bucket[j+k+old_bucket_number].fixed, l, 0);
                                cell++;
                            }
                            else {
                                table[TABLE1].bucket[j+k].sig[l] = 0;
                                setFull(table[TABLE1].bucket[j+k].full, l, 0);
                                setFixed(table[TABLE1].bucket[j+k].fixed, l, 0);
                            }
                        }
                    }
                    else {
                        for (int l = 0; l < L; ++l) {
                            //if (!isFull(table[TABLE2].bucket[j+k].full, l)) continue;

                            h1 = hash1(tmp_entry[k][l].key);
                            fp = calculate_fp(tmp_entry[k][l].key, 1);
                            h2 = hash_alt(h1, TABLE1, fp);
                            if (h2 == j+k) {
                                table[TABLE2].bucket[j+k+old_bucket_number].sig[l] = 0;
                                setFull(table[TABLE2].bucket[j+k+old_bucket_number].full, l, 0);
                                setFixed(table[TABLE2].bucket[j+k+old_bucket_number].fixed, l, 0);
                                cell++;
                            }
                            else {
                                table[TABLE2].bucket[j+k].sig[l] = 0;
                                setFull(table[TABLE2].bucket[j+k].full, l, 0);
                                setFixed(table[TABLE2].bucket[j+k].fixed, l, 0);
                            }
                        }
                    }

                    table[i].cell[j+k] = cell;
                    if (table[i].cell[j+k+old_bucket_number] >= cell)
                        table[i].cell[j+k+old_bucket_number] -= cell;
                    else
                        table[i].cell[j+k+old_bucket_number] = 0;
                }
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &time2);

        resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
        clean_time = (resns/1000);
        
        std::cout << "Update finished" << std::endl;
        /*std::cout << "Read: " << read_cnt << " times" << std::endl;
        std::cout << "Buckets: " << read_cnt*5 << std::endl;
        std::cout << "Cells: " << read_cnt*5*N << std::endl;*/

        return std::make_pair(copy_time, clean_time);
    }

    /* Expand table to n times with sending message in TCP with fd sock */
    /* Expand like RACE */
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

        // allocate resources in local
        for (int i = TABLE1; i <= TABLE2; ++i) {
            Bucket *tmp_bucket = new Bucket[bucket_number];
            memcpy(tmp_bucket, table[i].bucket, old_bucket_number*sizeof(Bucket));
            memcpy(tmp_bucket+old_bucket_number, table[i].bucket, old_bucket_number*sizeof(Bucket));
            //memset(tmp_bucket, 0, bucket_number*sizeof(Bucket));

            delete [] table[i].bucket;
            table[i].bucket = tmp_bucket;

            uint8_t *tmp_cell = new uint8_t[bucket_number];
            memcpy(tmp_cell, table[i].cell, old_bucket_number*sizeof(uint8_t));
            memcpy(tmp_cell+old_bucket_number, table[i].cell, old_bucket_number*sizeof(uint8_t));
            //memset(tmp_cell, 0, bucket_number*sizeof(uint32_t));

            delete [] table[i].cell;
            table[i].cell = tmp_cell;

            #ifndef RW_LOCK_CK
            mutex *tmp_mut = new mutex[bucket_number];
            delete [] bucket_mut[i];
            bucket_mut[i] = tmp_mut;
            #endif

            #ifdef RW_LOCK_CK
            shared_mutex *tmp_mut = new shared_mutex[bucket_number];
            delete [] bucket_mut[i];
            bucket_mut[i] = tmp_mut;
            #endif
        }
        // allocate remote resources
        expand_remote(n);

        clock_gettime(CLOCK_MONOTONIC, &time2);
        resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
        copy_time = (resns/1000);

        std::cout << "Copy finished" << std::endl;

        int read_cnt = 0;
        clock_gettime(CLOCK_MONOTONIC, &time1);

        // read 5 buckets one time, and clean local info
        for (int i = TABLE1; i <= TABLE2; ++i) {
            for (int j = 0; j < old_bucket_number; j += 5) {
                CK_Entry (*tmp_entry)[2][N];

                ra[0][0] = get_offset_table(i, j, 0, 1);
                read_cnt += get_clean_info();
                //read_cnt += 5;
                tmp_entry = (CK_Entry (*)[2][N])clean_buf;
                //memset(clean_buf[1], 0, sizeof(CK_Entry)*N*5);
                memcpy(clean_buf[1], clean_buf[0], sizeof(CK_Entry)*N*5);

                // check if new hash(key) == old hash(key) 
                for (int k = 0; k < 5; ++k) {
                    int cell = 0;
                    int h1, h2;
                    int fp;

                    if (i == TABLE1) {
                        for (int l = 0; l < N; ++l) {
                            h1 = hash1(tmp_entry[0][k][l].key);
                            if (h1 == j+k) {
                                table[TABLE1].bucket[j+k+old_bucket_number].sig[l] = 0;
                                setFull(table[TABLE1].bucket[j+k+old_bucket_number].full, l, 0);
                                setFixed(table[TABLE1].bucket[j+k+old_bucket_number].fixed, l, 0);   
                                ++cell;                            
                            }
                            else {
                                table[TABLE1].bucket[j+k].sig[l] = 0;
                                setFull(table[TABLE1].bucket[j+k].full, l, 0);
                                setFixed(table[TABLE1].bucket[j+k].fixed, l, 0);

                                //tmp_entry[1][k][l] = tmp_entry[0][k][l];
                                //tmp_entry[0][k][l].key = 0;
                            }
                        }
                    }
                    else {
                        for (int l = 0; l < L; ++l) {                            
                            h1 = hash1(tmp_entry[0][k][l].key);
                            fp = calculate_fp(tmp_entry[0][k][l].key, 1);
                            h2 = hash_alt(h1, TABLE1, fp);
                            if (h2 == j+k) {
                                table[TABLE2].bucket[j+k+old_bucket_number].sig[l] = 0;
                                setFull(table[TABLE2].bucket[j+k+old_bucket_number].full, l, 0);
                                setFixed(table[TABLE2].bucket[j+k+old_bucket_number].fixed, l, 0);
                                ++cell;
                            }
                            else {
                                table[TABLE2].bucket[j+k].sig[l] = 0;
                                setFull(table[TABLE2].bucket[j+k].full, l, 0);
                                setFixed(table[TABLE2].bucket[j+k].fixed, l, 0);

                                //tmp_entry[1][k][l] = tmp_entry[0][k][l];
                                //tmp_entry[0][k][l].key = 0;
                            }
                        }
                    }

                    table[i].cell[j+k] = cell;
                    if (table[i].cell[j+k+old_bucket_number] >= cell)
                        table[i].cell[j+k+old_bucket_number] -= cell;
                    else
                        table[i].cell[j+k+old_bucket_number] = 0;
                }
                
                ra[0][0] = get_offset_table(i, j, 0, 1);
                ra[0][1] = get_offset_table(i, j+old_bucket_number, 0, 1);
                mod_clean_info();
            }
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

    CuckooHashTable(int cell_number, int max_kick_num, int thread_num, int connect_num) {
        this->bucket_number = cell_number/(N+L);
        this->max_kick_number = max_kick_num;

        this->table[TABLE1].bucket = new Bucket[this->bucket_number];
        memset(table[TABLE1].bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table[TABLE2].bucket = new Bucket[this->bucket_number];
        memset(table[TABLE2].bucket, 0, this->bucket_number*sizeof(Bucket));

        this->table[TABLE1].cell = new uint8_t[this->bucket_number];
        memset(table[TABLE1].cell, 0, this->bucket_number*sizeof(uint8_t));
        this->table[TABLE2].cell = new uint8_t[this->bucket_number];
        memset(table[TABLE2].cell, 0, this->bucket_number*sizeof(uint8_t));

        this->thread_num = thread_num;
        this->connect_num = connect_num;

        for (int tid = 0; tid < thread_num; ++tid) {
            this->visit.push_back({});
            this->visit[tid] = new indexType[maxIndexType + 5];
            for (int i = 0; i < maxIndexType; ++i)
                this->visit[tid][i] = indexTypeNotFound;
        }

        for (int tid = 0; tid < thread_num; ++tid) {
            this->visitStep.push_back({});
            this->visitStep[tid] = new int[maxIndexType + 5];
        }

        initialize_hash_functions();

        stash_num = 0;

        for (int tid = 0; tid < thread_num; ++tid) {
            this->bfsQueue.push_back({});
            this->dirtyList.push_back({});
        }

        #ifndef RW_LOCK_CK
		this->bucket_mut[TABLE1] = new mutex[this->bucket_number];
		this->bucket_mut[TABLE2] = new mutex[this->bucket_number];
        #endif

        #ifdef RW_LOCK_CK
        this->bucket_mut[TABLE1] = new shared_mutex[this->bucket_number];
		this->bucket_mut[TABLE2] = new shared_mutex[this->bucket_number];
        #endif

        if (TOTAL_MEMORY_BYTE_USING_CACHE) {
            int cache_memory = TOTAL_MEMORY_BYTE_USING_CACHE - SIG_LEN * 2 * MY_BUCKET_SIZE * this->bucket_number;
            cache = new SimpleLRUCache::Cache(cache_memory);
        }
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;

		delete [] bucket_mut[TABLE1];
		delete [] bucket_mut[TABLE2];
    }
};

}