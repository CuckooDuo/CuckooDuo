/*
 * Class declaraiton and definition for CuckooDuo
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
#pragma once

#include "../rdma/rdma_client.h"

#include <functional>

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

struct pair_hash {
    template <class T1, class T2, class T3>
    std::size_t operator()(const std::pair<std::pair<T1, T2>, T3> &p) const {
        return hash_val(p.first.first, p.first.second, p.second);
    }
};

/* some tools */
Entry emptyEntry;
typedef std::pair<int, std::pair<Entry, Entry> > rebuildInfo;
typedef std::pair<std::pair<int, int>, int> indexType;
#define emptyIndex std::make_pair(std::make_pair(-1, -1), -1)
#define indexTypeNotFound std::make_pair(std::make_pair(-2, -2), -2)

/* table parameters */
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8                 // item(cell) number in a bucket (TABLE1)
#endif
#define M 6                 // item(cell) number in a bucket using hash1 (N-M for hash2) (TABLE1)
#define L 8                 // item(cell) number in a bucket (TABLE2)
#define maxNL 8            // max(N, L)
#define SIG_LEN 2           // sig(fingerprint) length: 16 bit
#define STASH_SIZE_CK 64        // size of stash
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2
#define rebuildError -1
#define rebuildOK 1
#define rebuildNeedkick 0
#define rebuildNeedTwokick 2

/* bucket definition */
struct Bucket{
    /* These are in remote */
    //char key[maxNL][KEY_LEN];
    //char val[maxNL][KEY_LEN];
    /* Others in local */
    char sig[maxNL][SIG_LEN];   //fingerprints are put in here
    bool full[maxNL];           //whether the cell is full or not
    bool fixed[maxNL];
};

struct Table
{
    Bucket *bucket;
    uint32_t *cell;        // occupied cell number of each bucket
};

class CuckooHashTable{
public:
    int bucket_number;
    int max_kick_number;
    Table table[2];
    std::unordered_map<std::string, std::string> stash;  //overflow stash

    struct kick_params //params for kicks
    {
        int table;
        int bucket;
        int cell;
        int alt_bucket;
    } tmp_kickparams;
    std::stack<kick_params> tmp_stack; // kick stack
    std::queue<std::stack<kick_params> > kick_queue; //kick queue

    uint32_t seed_hash_to_fp, seed_hash_to_fp2;
    uint32_t seed_hash_to_bucket;
    uint32_t seed_hash_to_alt;

    int stash_num;  // the number of entries in stash

	mutex stash_mut;        // mutex for stash visition
    mutex* bucket_mut[2];   // mutex for bucket visition

    int thread_num;     // the number of threads we use
    int connect_num;    // the number of threads we have

private:
    /* function to lock and unlock buckets */
    bool inline bucket_lock(int table_idx, int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[table_idx][bucket_idx].lock();
        }
        return true;
    }
    bool inline bucket_unlock(int table_idx, int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[table_idx][bucket_idx].unlock();
        }
        return true;
    }
    bool inline bucket_lock(const std::set<pair<int,int>> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                if (!bucket_mut[i.first][i.second].try_lock()) {
                    // if we can't lock each mutex, we release all of them
                    for (const auto &j: mut_idx) {
                        if (i == j) return false;
                        bucket_mut[j.first][j.second].unlock();
                    }
                }
            }
        }
        return true;
    }
    bool inline bucket_unlock(const std::set<pair<int,int>> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i: mut_idx) {
                bucket_mut[i.first][i.second].unlock();
            }
        }
        return true;
    }

    /* function to lock and unlock stash */
    bool inline stash_lock() {
        if (thread_num > 1) {
            stash_mut.lock();
        }
        return true;
    }
    bool inline stash_unlock() {
        if (thread_num > 1) {
            stash_mut.unlock();
        }
        return true;
    }

    void initialize_hash_functions(){
        std::set<int> seeds;
        // srand((unsigned)time(NULL));
        srand(578982);

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

    int calculate_fp(const char *key, int fphash = 1) {
        // return MurmurHash3_x86_32(key, KEY_LEN, fphash == 1 ? seed_hash_to_fp : seed_hash_to_fp2) % (1 << (SIG_LEN * 8));
        uint32_t fp = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);// % (1 << (SIG_LEN * 16));
        if (fphash == 1) return fp & (65535);
        else return fp >> 16;
    }

    void calculate_two_fp(const char *key, int &fp1, int &fp2) {
        uint32_t fp = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);// % (1 << (SIG_LEN * 16));
        fp1 = fp & (65535);
        fp2 = fp >> 16;
    }

    // return hash of sig(fp), used for alternate index.
    int hash_fp(const char *sig) {
        return MurmurHash3_x86_32(sig, SIG_LEN, seed_hash_to_alt) % bucket_number;
    }

    // return index for table 1.
    int hash1(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket) % bucket_number;
    }

    // return alternate index for h_now, use h_now mod+/- hash(fp).
    int hash_alt(int h_now, int table_now, const char *fp) {
        int hfp;//hash_fp(fp);
        sig_to_fp(hfp, fp);
        // hfp = hfp % bucket_number;
        if(table_now == TABLE1) {
            return (h_now + hfp) % bucket_number;
        }
        else if(table_now == TABLE2) {
            int h_alt = (h_now - hfp) % bucket_number;
            if(h_alt < 0) h_alt += bucket_number;
            return h_alt;
        }
        printf("wrong parameter: table not 1 or 2\n");
        return -1;
    }
    int hash_alt(int h_now, int table_now, int hfp) {
        // hfp = hfp % bucket_number;
        if(table_now == TABLE1) {
            return (h_now + hfp) % bucket_number;
        }
        else if(table_now == TABLE2) {
            int h_alt = (h_now - hfp) % bucket_number;
            if(h_alt < 0) h_alt += bucket_number;
            return h_alt;
        }
        printf("wrong parameter: table not 1 or 2\n");
        return -1;
    }

    // return char *sig for int fp.
    void fp_to_sig(char *sig, int fp) {
        int byte = -1;
        while(++byte < SIG_LEN) {
            sig[byte] = fp & 0b11111111;
            fp = fp >> 8;
        }
    }

    // return int fp for char *sig.
    void sig_to_fp(int &fp, const char *sig) {
        fp = 0;
        int byte = SIG_LEN;
        while(--byte >= 0) {
            fp = fp << 8;
            fp += (unsigned char)sig[byte];
        }
    }

    // insert an full entry to stash
    bool insert_to_stash(const Entry& entry) {
		stash_lock();
        if(stash_num >= STASH_SIZE_CK) {
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

    // given a sig, check whether table[TABLE1] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table1(int b, char *sig, char *sig2) {
        for(int i = M; i < N; i++) {
            if (table[TABLE1].bucket[b].full[i])
            if(memcmp(sig2, table[TABLE1].bucket[b].sig[i], SIG_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        for(int i = 0; i < M; i++) {
            if (table[TABLE1].bucket[b].full[i])
            if(memcmp(sig, table[TABLE1].bucket[b].sig[i], SIG_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
    }

    // given a sig, check whether table[TABLE2] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table2(int b, char *sig) {
        for(int i = 0; i < L; i++) {
            if (table[TABLE2].bucket[b].full[i])
            if(memcmp(sig, table[TABLE2].bucket[b].sig[i], SIG_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
    }

    // clear the queue and the stack
    void kick_clear() {
        while(!kick_queue.empty()) {
            kick_queue.pop();
        }
        while(!tmp_stack.empty()) {
            tmp_stack.pop();
        }
    }

    // RDMA write, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, const Entry& entry, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        if (&entry != &write_buf[tid][0])
            memcpy(&write_buf[tid][0], &entry, KV_LEN);
        mod_remote(1, tid);
    }
    // RDMA read, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, cellID, 1);
        get_remote(1, tid);
        if (&entry != &read_buf[tid][0])
            memcpy(&entry, &read_buf[tid][0], KV_LEN);
    }
    // RDMA read, read entries from (table,bucket)
    inline void RDMA_read_bucket(Entry *entry, int tableID, int bucketID, int tid){
        ra[tid][0] = get_offset_table(tableID, bucketID, 0, 1);
        get_bucket(1, 1, tid);
        memcpy(entry, bucket_buf[tid][0], N*KV_LEN);
    }

    // rebuild bucket with new entry, return (rebuildError, 0) if need insert to cache, 
    // return (rebuildNeedkick, kickEntry) if need kick, return (rebuildOK, 0) if done
    rebuildInfo rebuild(const Entry& entry, int balanceFlag, int tid){
        // int fp = calculate_fp(entry.key, 1);
        // int fp2 = calculate_fp(entry.key, 2);
        int fp, fp2;
        calculate_two_fp(entry.key, fp, fp2);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);
        
        int hash1coll = check_in_table2(h[TABLE2], sig);
        int table1coll;
        Bucket *current_bucket = &table[TABLE1].bucket[h[TABLE1]];

        Entry insertEntry[5], kickEntry[5];
        int insertFp[5], kickFp[5];
        int insertCnt = 0, kickCnt = 0, fpInt = 0;
        int target = 0;// 1 for hash1, 2 for hash2, 3 for any, 4: fixed if hash1
        int fixedCnt = 0;
        int hash1Cnt = 0;
        for (int i=0; i<6; i++){
            if (current_bucket->fixed[i]) fixedCnt++;
            if (current_bucket->full[i]) hash1Cnt++;
        }
        if (fixedCnt >= 2) fixedCnt -= 2;

        /* read all */
        Entry allEntry[8];
        RDMA_read_bucket(allEntry, TABLE1, h[TABLE1], tid);

        if (hash1coll != -1){
            // remove from table2, put it in 0~5, put entry in 6~7
            // remove from table2
            RDMA_read(insertEntry[insertCnt], TABLE2, h[TABLE2], hash1coll, tid);
            
            ++insertCnt;
            table[TABLE2].bucket[h[TABLE2]].full[hash1coll] = 0;
            table[TABLE2].cell[h[TABLE2]]--;
            // replace if 6~7 not empty
            // check entry hash2coll with 0~5
            int entryHash2CollFlag = 0;

            for (int j=0; j<6; j++){
                if (!current_bucket->full[j]) continue;
                int fpJ;
                calculate_two_fp(allEntry[j].key, fpInt, fpJ);
                if (fpJ == fp2){
                    entryHash2CollFlag = 1;
                    j = 8;
                }
            }
            /* read one by one */
            // for (int j=0; j<6; j++){
            //     if (!current_bucket->full[j]) continue;
            //     Entry entryJ;
            //     int fpJ;
            //     RDMA_read(entryJ, TABLE1, h[TABLE1], j);
            //     calculate_two_fp(entryJ.key, fpInt, fpJ);
            //     if (fpJ == fp2){
            //         entryHash2CollFlag = 1;
            //         j = 8;
            //     }
            // }
            if (entryHash2CollFlag){
                if (current_bucket->full[6+fixedCnt]){
                    memcpy(&insertEntry[insertCnt], &allEntry[6+fixedCnt], sizeof(Entry));
                    ++insertCnt;
                } else {
                    current_bucket->full[6+fixedCnt] = 1;
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // coll -> 6~7, entry -> coll -> 0~5
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, insertEntry[0], tid);
                int collFp;
                calculate_two_fp(insertEntry[0].key, fpInt, collFp);
                fp_to_sig(current_bucket->sig[6+fixedCnt], collFp);
                insertEntry[0] = entry;
                memcpy(table[TABLE2].bucket[h[TABLE2]].sig[hash1coll], sig, SIG_LEN*sizeof(char));
            } else {
                if (current_bucket->full[6+fixedCnt]){
                    memcpy(&insertEntry[insertCnt], &allEntry[6+fixedCnt], sizeof(Entry));
                    ++insertCnt;
                } else {
                    current_bucket->full[6+fixedCnt] = 1;
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // entry -> 6~7
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
            }
            // hash1coll -> 0~5
            if (hash1Cnt < 6){
                for (int i=0; i<6; i++){
                    if (current_bucket->full[i]) continue;
                    ++hash1Cnt;
                    current_bucket->full[i] = 1;
                    table[TABLE1].cell[h[TABLE1]]++;
                    current_bucket->fixed[i] = 1;
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                    memcpy(current_bucket->sig[i], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll], SIG_LEN*sizeof(char));
                    break;
                }
            } else {
                for (int i=0; i<6; i++){
                    if (current_bucket->fixed[i]) continue;
                    // kick
                    memcpy(&insertEntry[insertCnt], &allEntry[i], sizeof(Entry));
                    ++insertCnt;
                    current_bucket->fixed[i] = 1;
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                    memcpy(current_bucket->sig[i], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll], SIG_LEN*sizeof(char));
                    break;
                }
            }
            // kick
            if (insertCnt > 2)
                return std::make_pair(rebuildNeedTwokick, std::make_pair(insertEntry[1], insertEntry[2]));
            if (insertCnt > 1)
                return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[1], emptyEntry));
            return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
        } else {
            table1coll = check_in_table1(h[TABLE1], sig, sig2);
            if (table1coll >= 6){
                if (table1coll == 6 && fixedCnt == 1){
                    // a-b hash1coll, b-entry hash2coll, swap a b and put entry in 0~5
                    // swap a b
                    int entryAID = 0;
                    Entry entryA, entryB;
                    int fpA, fpB;
                    while (entryAID < 6 && current_bucket->fixed[entryAID] != 1) ++entryAID;
                    memcpy(&entryA, &allEntry[entryAID], sizeof(Entry));
                    memcpy(&entryB, &allEntry[6], sizeof(Entry));

                    calculate_two_fp(entryA.key, fpInt, fpA);
                    calculate_two_fp(entryB.key, fpB, fpInt);

                    memcpy(&write_buf[0], &entryB, sizeof(Entry));
                    memcpy(&write_buf[1], &entryA, sizeof(Entry));
                    ra[tid][0] = get_offset_table(TABLE1, h[TABLE1], entryAID, 1);
                    ra[tid][1] = get_offset_table(TABLE1, h[TABLE1], 6, 1);
                    mod_remote(2, tid);

                    fp_to_sig(current_bucket->sig[entryAID], fpB);
                    fp_to_sig(current_bucket->sig[6], fpA);
                    // entry -> 0~5
                    if (hash1Cnt < 6){
                        for (int i=0; i<6; i++){
                            if (current_bucket->full[i]) continue;
                            ++hash1Cnt;
                            current_bucket->full[i] = 1;
                            table[TABLE1].cell[h[TABLE1]]++;
                            current_bucket->fixed[i] = 1;
                            RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                            memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                            break;
                        }
                    } else {
                        for (int i=0; i<6; i++){
                            if (current_bucket->fixed[i]) continue;
                            // kick
                            memcpy(&insertEntry[insertCnt], &allEntry[i], sizeof(Entry));

                            ++insertCnt;
                            current_bucket->fixed[i] = 1;
                            RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                            memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                            break;
                        }
                    }
                    // kick
                    if (insertCnt > 0)
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
                // remove from hash2, put two entry in 0~5
                // remove from hash2
                memcpy(&insertEntry[insertCnt], &allEntry[table1coll], sizeof(Entry));

                calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                ++insertCnt;
                current_bucket->full[table1coll] = 0;
                table[TABLE1].cell[h[TABLE1]]--;
                // entry -> 0~5
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        if (current_bucket->full[i]) continue;
                        ++hash1Cnt;
                        current_bucket->full[i] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (current_bucket->fixed[i]) continue;
                        // kick
                        memcpy(&insertEntry[insertCnt], &allEntry[i], sizeof(Entry));

                        ++insertCnt;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                        break;
                    }
                }
                // hash2coll -> 0~5
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        if (current_bucket->full[i]) continue;
                        current_bucket->full[i] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                        fp_to_sig(current_bucket->sig[i], insertFp[0]);
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (current_bucket->fixed[i]) continue;
                        // kick
                        memcpy(&insertEntry[insertCnt], &allEntry[i], sizeof(Entry));

                        ++insertCnt;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0], tid);
                        fp_to_sig(current_bucket->sig[i], insertFp[0]);
                        break;
                    }
                }
                // kick
                if (insertCnt > 2)
                    return std::make_pair(rebuildNeedTwokick, std::make_pair(insertEntry[1], insertEntry[2]));
                if (insertCnt > 1)
                    return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[1], emptyEntry));
                return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
            } else if (table1coll >= 0){
                // fix coll, put entry in 6~7
                // check entry hash2coll with 0~5
                int entryHash2CollFlag = 0;
                for (int j=0; j<6; j++){
                    if (!current_bucket->full[j]) continue;
                    int fpJ;
                    calculate_two_fp(allEntry[j].key, fpInt, fpJ);
                    if (fpJ == fp2){
                        entryHash2CollFlag = 1;
                        j = 8;
                    }
                }
                /* read one by one */
                // for (int j=0; j<6; j++){
                //     if (!current_bucket->full[j]) continue;
                //     Entry entryJ;
                //     int fpJ;
                //     RDMA_read(entryJ, TABLE1, h[TABLE1], j);
                //     calculate_two_fp(entryJ.key, fpInt, fpJ);
                //     if (fpJ == fp2){
                //         entryHash2CollFlag = 1;
                //         j = 8;
                //     }
                // }
                if (entryHash2CollFlag){
                    // swap, entry in 0~5, coll in 6~7
                    Entry collEntry;
                    int collEntryFp2;
                    memcpy(&insertEntry[insertCnt], &allEntry[table1coll], sizeof(Entry));

                    calculate_two_fp(collEntry.key, fpInt, collEntryFp2);
                    // fix coll, entry -> 0~5
                    current_bucket->fixed[table1coll] = 1;
                    RDMA_write(TABLE1, h[TABLE1], table1coll, entry, tid);
                    fp_to_sig(current_bucket->sig[table1coll], fp);
                    // replace if 6~7 not empty
                    if (current_bucket->full[6+fixedCnt]){
                        memcpy(&insertEntry[insertCnt], &allEntry[6+fixedCnt], sizeof(Entry));
                        ++insertCnt;
                    } else {
                        current_bucket->full[6+fixedCnt] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // coll -> 6~7
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, collEntry, tid);
                    fp_to_sig(current_bucket->sig[6+fixedCnt], collEntryFp2);
                    // kick
                    if (insertCnt > 0)
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                } else {
                    // fix coll
                    current_bucket->fixed[table1coll] = 1;
                    // replace if 6~7 not empty
                    if (current_bucket->full[6+fixedCnt]){
                        memcpy(&insertEntry[insertCnt], &allEntry[6+fixedCnt], sizeof(Entry));
                        // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                        ++insertCnt;
                    } else {
                        current_bucket->full[6+fixedCnt] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // entry -> 6~7
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                    memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
                    // kick
                    if (insertCnt > 0)
                        return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
            } else {
                for (int i=6; i<8; i++){
                    if (!table[TABLE1].bucket[h[TABLE1]].full[i]) continue;
                    Entry collEntry;
                    int collfp1, collfp2;
                    memcpy(&collEntry, &allEntry[i], sizeof(Entry));

                    calculate_two_fp(collEntry.key, collfp1, collfp2);
                    if (collfp1 == fp){
                        if (i == 6 && fixedCnt == 1){
                            // 3 hash1 coll, put entry in 7
                            // replace if 7 not empty
                            if (current_bucket->full[6+fixedCnt]){
                                memcpy(&insertEntry[insertCnt], &allEntry[6+fixedCnt], sizeof(Entry));

                                ++insertCnt;
                            } else {
                                current_bucket->full[6+fixedCnt] = 1;
                                table[TABLE1].cell[h[TABLE1]]++;
                            }
                            // entry -> 7
                            RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, tid);
                            memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
                        } else {
                            // put entry in 0~5, move coll 6/7 if need
                            // replace if 6/7 not empty
                            if (i == 7 && fixedCnt == 0){
                                // need move
                                // replace 6
                                if (!current_bucket->full[6]){
                                    // empty, just move from 7 to 6
                                    Entry tmp_entry;
                                    memcpy(&tmp_entry, &allEntry[7], sizeof(Entry));

                                    RDMA_write(TABLE1, h[TABLE1], 6, tmp_entry, tid);
                                    current_bucket->full[6] = 1;
                                    current_bucket->full[7] = 0;
                                    memcpy(current_bucket->sig[6], current_bucket->sig[7], SIG_LEN*sizeof(char));
                                } else {
                                    // full, swap 6 and 7
                                    Entry Entry6, Entry7;
                                    char sig6[SIG_LEN], sig7[SIG_LEN];
                                    memcpy(&Entry6, &allEntry[6], sizeof(Entry));
                                    memcpy(&Entry7, &allEntry[7], sizeof(Entry));

                                    memcpy(&write_buf[0], &Entry7, sizeof(Entry));
                                    memcpy(&write_buf[1], &Entry6, sizeof(Entry));
                                    ra[tid][0] = get_offset_table(TABLE1, h[TABLE1], 6, 1);
                                    ra[tid][1] = get_offset_table(TABLE1, h[TABLE1], 7, 1);
                                    mod_remote(2, tid);

                                    memcpy(sig6, current_bucket->sig[6], SIG_LEN*sizeof(char));
                                    memcpy(sig7, current_bucket->sig[7], SIG_LEN*sizeof(char));
                                    memcpy(current_bucket->sig[6], sig7, SIG_LEN*sizeof(char));
                                    memcpy(current_bucket->sig[7], sig6, SIG_LEN*sizeof(char));
                                }
                            }
                            // entry -> 0~5
                            if (hash1Cnt < 6){
                                for (int i=0; i<6; i++){
                                    if (current_bucket->full[i]) continue;
                                    ++hash1Cnt;
                                    current_bucket->full[i] = 1;
                                    table[TABLE1].cell[h[TABLE1]]++;
                                    current_bucket->fixed[i] = 1;
                                    RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                                    memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                                    break;
                                }
                            } else {
                                for (int i=0; i<6; i++){
                                    if (current_bucket->fixed[i]) continue;
                                    // kick
                                    memcpy(&insertEntry[insertCnt], &allEntry[i], sizeof(Entry));

                                    ++insertCnt;
                                    current_bucket->fixed[i] = 1;
                                    memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                                    break;
                                }
                            }
                        }
                        // kick
                        if (insertCnt > 0)
                            return std::make_pair(rebuildNeedkick, std::make_pair(insertEntry[0], emptyEntry));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                // no any coll
                if (balanceFlag && (0 < (L-(int)table[TABLE2].cell[h[TABLE2]]))
                && ((N-(int)table[TABLE1].cell[h[TABLE1]]) <= (L-(int)table[TABLE2].cell[h[TABLE2]]))){
                    for (int i=0; i<8; i++){
                        if (table[TABLE2].bucket[h[TABLE2]].full[i]) continue;
                        table[TABLE2].cell[h[TABLE2]]++;
                        table[TABLE2].bucket[h[TABLE2]].full[i] = 1;
                        RDMA_write(TABLE2, h[TABLE2], i, entry, tid);
                        memcpy(table[TABLE2].bucket[h[TABLE2]].sig[i], sig, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                } else {
                    for (int i=6; i<8; i++){
                        if (table[TABLE1].bucket[h[TABLE1]].full[i]) continue;
                        // should check hash2coll with 0~5, if insert 6~7
                        for (int j=0; j<6; j++){
                            if (!current_bucket->full[j]) continue;
                            int fpJ;
                            calculate_two_fp(allEntry[j].key, fpInt, fpJ);
                            if (fpJ == fp2){
                                i = 8;
                                j = 8;
                            }
                        }
                        /* read one by one */
                        // for (int j=0; j<6; j++){
                        //     if (!current_bucket->full[j]) continue;
                        //     Entry entryJ;
                        //     int fpJ;
                        //     RDMA_read(entryJ, TABLE1, h[TABLE1], j);
                        //     calculate_two_fp(entryJ.key, fpInt, fpJ);
                        //     if (fpJ == fp2){
                        //         i = 8;
                        //         j = 8;
                        //     }
                        // }
                        if (i >= 8) break;
                        table[TABLE1].cell[h[TABLE1]]++;
                        table[TABLE1].bucket[h[TABLE1]].full[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig2, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                    for (int i=0; i<6; i++){
                        if (table[TABLE1].bucket[h[TABLE1]].full[i]) continue;
                        table[TABLE1].cell[h[TABLE1]]++;
                        table[TABLE1].bucket[h[TABLE1]].full[i] = 1;
                        RDMA_write(TABLE1, h[TABLE1], i, entry, tid);
                        memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                return std::make_pair(rebuildNeedkick, std::make_pair(entry, emptyEntry));
            }
        }
    }
    
    // std::unordered_map<indexType, indexType, pair_hash> visit[tid];
    // std::unordered_map<indexType, int, pair_hash> visitStep[tid];
    std::vector<indexType ***> visit;    // record cell will check
    std::vector<int ***> visitStep;      // record the number of steps of this visition
    std::vector<std::queue<indexType>> bfsQueue, dirtyList;    // queue for cells will check or checked
    /* Spread bfs with from(current cell) */
    void bfsSpread(indexType from, int tid){
        int tableID = from.first.first;
        int bucketID = from.first.second;
        int cellID = from.second;
        char *sig = table[tableID].bucket[bucketID].sig[cellID];
        int altBucketID = hash_alt(bucketID, tableID, sig);

        int vid = connect_num-1-tid;
        for (int i=0; i<((tableID^1) == TABLE1 ? M : L); i++) if (!table[tableID^1].bucket[altBucketID].fixed[i]){
            indexType target = std::make_pair(std::make_pair(tableID^1, altBucketID), i);
            if (visit[vid][target.first.first][target.first.second][target.second] != indexTypeNotFound) continue;
            visit[vid][target.first.first][target.first.second][target.second] = from;
            visitStep[vid][target.first.first][target.first.second][target.second] = visitStep[vid][from.first.first][from.first.second][from.second] + 1;
            bfsQueue[vid].push(target);
            dirtyList[vid].push(target);
        }
    }

    /* Clean unused info in bfs */
    void cleanVisit(int tid){
        int vid = connect_num-1-tid;
        while (!dirtyList[vid].empty()){
            indexType item = dirtyList[vid].front();
            visit[vid][item.first.first][item.first.second][item.second] = indexTypeNotFound;
            dirtyList[vid].pop();
        }
    }

    /* Do cuckoo kicks in bfs */
    bool bfs(Entry entry, int tid){
        int vid = connect_num-1-tid;

        int fp = calculate_fp(entry.key, 1);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);

        Bucket *current_bucket1 = &table[TABLE1].bucket[h[TABLE1]];
        Bucket *current_bucket2 = &table[TABLE2].bucket[h[TABLE2]];
        // modify for faster bfs, local is better
        // maybe need to assert bucket element arranged as full cells first
        while (!bfsQueue[vid].empty()) bfsQueue[vid].pop();
        int collID = check_in_table2(h[TABLE2], sig);
        if (collID == -1){
            for (int i=0; i<L; i++){
                indexType target = std::make_pair(std::make_pair(TABLE2, h[TABLE2]), i);
                visit[vid][target.first.first][target.first.second][target.second] = emptyIndex;
                visitStep[vid][target.first.first][target.first.second][target.second] = 1;
                bfsQueue[vid].push(target);
                dirtyList[vid].push(target);
            }
        } else {
            //std::cout << "unexpected error: hash1 collision" << std::endl;
        }
        for (int i=0; i<M; i++) if (!current_bucket1->fixed[i]){
            indexType target = std::make_pair(std::make_pair(TABLE1, h[TABLE1]), i);
            visit[vid][target.first.first][target.first.second][target.second] = emptyIndex;
            visitStep[vid][target.first.first][target.first.second][target.second] = 1;
            bfsQueue[vid].push(target);
            dirtyList[vid].push(target);
        }

        while (!bfsQueue[vid].empty()){// (kick_num < max_kick_number){  //kick
            // kick_num++;
            indexType from = bfsQueue[vid].front();
            bfsQueue[vid].pop();
            if (from.first.first != 0 && from.first.first != 1) return false;
            if (visitStep[vid][from.first.first][from.first.second][from.second] > max_kick_number) break;
            int tableID = from.first.first;
            int bucketID = from.first.second;
            int cellID = from.second;

            Bucket *BFSbucket = &table[tableID].bucket[bucketID];
            if (!BFSbucket->full[cellID]){
                // find an empty cell, kick
                indexType indexList[10];
                Entry entryList[10];
                char sigList[10][SIG_LEN];
                int listCnt = 0;

                // lock buckets will be visited in this path
                std::set<pair<int,int>> mut_idx;
                mut_idx.insert(std::make_pair(tableID, bucketID));
                while (1){
                    indexList[listCnt++] = from;
                    mut_idx.insert(std::make_pair(from.first.first, from.first.second));

                    from = visit[vid][from.first.first][from.first.second][from.second];
                    if (from == emptyIndex) break;
                }
                // if we can't lock every locks at once, we try another way
                if (!bucket_lock(mut_idx)) continue;
                // read entries in path to entryList
                for (int i=0; i<listCnt; i++){
                    if (i+1<listCnt){
                        Bucket *current_bucket = &table[indexList[i+1].first.first].bucket[indexList[i+1].first.second];
                        /*RDMA_read(entryList[i], 
                                  indexList[i+1].first.first, 
                                  indexList[i+1].first.second, 
                                  indexList[i+1].second);*/
                        ra[tid][i] = get_offset_table(
                            indexList[i+1].first.first,
                            indexList[i+1].first.second, 
                            indexList[i+1].second, 1);

                        memcpy(sigList[i], current_bucket->sig[indexList[i+1].second], SIG_LEN*sizeof(char));
                    }
                    else{
                        entryList[i] = entry;
                        memcpy(sigList[i], sig, SIG_LEN*sizeof(char));
                    }
                }
                get_remote(listCnt-1, tid);
                memcpy(&entryList, &read_buf[tid], KV_LEN*(listCnt-1));

                // check hash2 collision
                int hash2collisionFlag = 0;
                for (int i=0; i<listCnt; i++){
                    // kick to table2 won't cause hash2 collision
                    if (indexList[i].first.first == TABLE2) continue;
                    int fp2 = calculate_fp(entryList[i].key, 2);
                    char sig2[SIG_LEN];
                    fp_to_sig(sig2, fp2);
                    for (int j=M; j<N; j++)
                        if (table[TABLE1].bucket[indexList[i].first.second].full[j]
                        && memcmp(sig2, table[TABLE1].bucket[indexList[i].first.second].sig[j], SIG_LEN*sizeof(char)) == 0)
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
                    for (int i=listCnt-1; i>=0; i--){
                        // kick to table2 won't cause hash2 collision
                        Bucket *current_bucket = &table[indexList[i].first.first].bucket[indexList[i].first.second];
                        if (indexList[i].first.first == TABLE1){
                            int currentBucketCollision = 0;
                            int fp2 = calculate_fp(entryList[i].key, 2);
                            char sig2[SIG_LEN];
                            fp_to_sig(sig2, fp2);
                            for (int j=M; j<N; j++)
                                if (current_bucket->full[j]
                                && memcmp(sig2, current_bucket->sig[j], SIG_LEN*sizeof(char)) == 0)
                                currentBucketCollision = 1;
                            if (currentBucketCollision){
                                // clear kicked cell
                                if (current_bucket->full[indexList[i].second]){
                                    current_bucket->full[indexList[i].second] = 0;
                                    table[TABLE1].cell[indexList[i].first.second]--;
                                }

                                int fp, fp2;
                                calculate_two_fp(entryList[i].key, fp, fp2);
                                int h[2];
                                h[TABLE1] = hash1(entryList[i].key);
                                char sig[SIG_LEN], sig2[SIG_LEN];
                                fp_to_sig(sig, fp);
                                fp_to_sig(sig2, fp2);
                                h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);
            
                                std::set<pair<int,int>> mut_idx_r({std::make_pair(TABLE1, h[TABLE1]),
                                                                   std::make_pair(TABLE2, h[TABLE2])});
                                // unlock old locks, and lock for build
                                bucket_unlock(mut_idx);
                                bucket_lock(mut_idx_r);
                                // rebuild without balance flag
                                rebuildInfo info = rebuild(entryList[i], 0, tid);
                                bucket_unlock(mut_idx_r);

                                if (info.first == rebuildOK){
                                    // ajustment success
                                } else {
                                    
                                    if (info.first != rebuildNeedkick){
                                        //printf("unexpected: info.first != rebuildNeedkick:%d\n", info.first);
                                        cleanVisit(tid);
                                        return false;
                                    }
                                    // need to modify : insert 2 -> 1
                                    if(!insert_to_stash(info.second.first)){
                                        cleanVisit(tid);
                                        return false;
                                    }
                                    if (i == 0) {
                                        cleanVisit(tid);
                                        return true;
                                    } else {
                                        if(!insert_to_stash(entryList[i-1])){
                                            cleanVisit(tid);
                                            return false;
                                        }
                                        cleanVisit(tid);
                                        return true;
                                    }
                                }
                            } else {
                                // write entries in path to new locations
                                memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                                /*RDMA_write(indexList[i].first.first, 
                                           indexList[i].first.second, 
                                           indexList[i].second, 
                                           entryList[i]);*/
                                memcpy(&write_buf[tid][i], &entryList[i], KV_LEN);
								ra[tid][i] = get_offset_table(indexList[i].first.first, 
                                                         indexList[i].first.second, 
                                                         indexList[i].second, 1);

                            }
                        } else {
                            // write entries in path to new locations
                            memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                            /*RDMA_write(indexList[i].first.first, 
                                       indexList[i].first.second, 
                                       indexList[i].second, 
                                       entryList[i]);*/
                            memcpy(&write_buf[tid][i], &entryList[i], KV_LEN);
							ra[tid][i] = get_offset_table(indexList[i].first.first, 
                                                     indexList[i].first.second, 
                                                     indexList[i].second, 1);

                        }
                    }
                    mod_remote(listCnt, tid);

                    if (!BFSbucket->full[cellID]){
                        BFSbucket->full[cellID] = 1;
                        table[tableID].cell[bucketID] += 1;
                    }
                    bucket_unlock(mut_idx);

                    cleanVisit(tid);
                    return true;
                }
                // no hash2 collision
                for (int i=0; i<listCnt; i++){
                    // write entries in path to new locations
                    Bucket *current_bucket = &table[indexList[i].first.first].bucket[indexList[i].first.second];
                    memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                    /*RDMA_write(indexList[i].first.first, 
                               indexList[i].first.second, 
                               indexList[i].second, 
                               entryList[i],
							   tid);*/
                    memcpy(&write_buf[tid][i], &entryList[i], KV_LEN);
					ra[tid][i] = get_offset_table(indexList[i].first.first, 
                                                  indexList[i].first.second, 
                                                  indexList[i].second, 1);
                }
                mod_remote(listCnt, tid);

                if (!BFSbucket->full[cellID]){
                    BFSbucket->full[cellID] = 1;
                    table[tableID].cell[bucketID] += 1;
                } else /*printf("unexpected full bucket cell\n")*/;
                cleanVisit(tid);

                bucket_unlock(mut_idx);
                return true;
            }
            // if (dirtyList[vid].size() < 50) 
            // spread visit path from current situation
            bfsSpread(from, tid);
        }
        cleanVisit(tid);
        // insert entry to stash if we can't find location in tables
        if(insert_to_stash(entry))
            return true;
        return false;
    }
public:
    /* Insert an entry */
    bool insert(const Entry& entry, int tid) {
        int fp, fp2;
        calculate_two_fp(entry.key, fp, fp2);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);

        // lock buckets for rebuild
        std::set<pair<int,int>> mut_idx({std::make_pair(TABLE1,h[TABLE1]),
                                         std::make_pair(TABLE2,h[TABLE2])});
        bucket_lock(mut_idx);
        // try rebuild with balance flag
        rebuildInfo info = rebuild(entry, 1, tid);

        bucket_unlock(mut_idx);

        if (info.first == rebuildOK){
            return true;
        }
        if (info.first == rebuildError){
            if(insert_to_stash(entry))
                return true;
            return false;
        }

		// do cuckoo kick with bfs
        bool insertOK = bfs(info.second.first, tid);
        if (!insertOK || info.first == rebuildNeedkick){
            return insertOK;
        }
        insertOK = bfs(info.second.second, tid);
        return insertOK;
    }

    /* Query key and put result in val */
    bool query(const char *key, char *val, int tid) {

        //query in tables
        // int fp = calculate_fp(key);
        // int fp2 = calculate_fp(key, 2);
        int fp, fp2;
        calculate_two_fp(key, fp, fp2);
        int h1 = hash1(key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1);
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE1, h1, cell, tid);

			bucket_unlock(TABLE1, h1);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0)
                return true;
			
            else return false;
        }
        bucket_unlock(TABLE1, h1);

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE2, h2, cell, tid);

			bucket_unlock(TABLE2, h2);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) == 0)
                return true;

            else return false;
        }
        bucket_unlock(TABLE2, h2);

        // query in stash
        std::string skey(key, KEY_LEN);
		stash_lock();
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            if(val != NULL) {
                std::string sval = entry->second;
                char* pval = const_cast<char*>(sval.c_str());
                memcpy(val, pval, VAL_LEN);
            }
			stash_unlock();
            return true;
        }
		stash_unlock();

        //miss
        return false;
    }

    /* Delete an entry given a key */
    bool deletion(const char* key, int tid) {

        //query in tables
        int fp = calculate_fp(key);
        int fp2 = calculate_fp(key, 2);
        int h1 = hash1(key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1);
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE1, h1, cell, tid);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE1, h1);
                return false;
            }

            memset(table[TABLE1].bucket[h1].sig[cell], 0, SIG_LEN);
            table[TABLE1].bucket[h1].full[cell] = 0;
			
			bucket_unlock(TABLE1, h1);
			return true;
        }
		bucket_unlock(TABLE1, h1);

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            RDMA_read(read_buf[tid][0], TABLE2, h2, cell, tid);
            if (memcmp(key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE2, h2);
                return false;
            }

            memset(table[TABLE2].bucket[h2].sig[cell], 0, SIG_LEN);
            table[TABLE2].bucket[h2].full[cell] = 0;

			bucket_unlock(TABLE2, h2);
            return true;
        }
		bucket_unlock(TABLE2, h2);

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
    bool update(const Entry& entry, int tid) {

        //query in tables
        int fp = calculate_fp(entry.key);
        int fp2 = calculate_fp(entry.key, 2);
        int h1 = hash1(entry.key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE1]
		bucket_lock(TABLE1, h1);
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE1, h1, cell, tid);
            
            if (memcmp(entry.key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE1, h1);
                return false;
            }
            RDMA_write(TABLE1, h1, cell, entry, tid);

			bucket_unlock(TABLE1, h1);
            return true;
        }
		bucket_unlock(TABLE1, h1);

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
		bucket_lock(TABLE2, h2);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            RDMA_read(read_buf[tid][0], TABLE2, h2, cell, tid);

            if (memcmp(entry.key, read_buf[tid][0].key, KEY_LEN) != 0) {
                bucket_unlock(TABLE2, h2);
                return false;
            }
            RDMA_write(TABLE2, h2, cell, entry, tid);

			bucket_unlock(TABLE2, h2);
            return true;
        }
		bucket_unlock(TABLE2, h2);

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

            uint32_t *tmp_cell = new uint32_t[bucket_number];
            memcpy(tmp_cell, table[i].cell, old_bucket_number*sizeof(uint32_t));
            memcpy(tmp_cell+old_bucket_number, table[i].cell, old_bucket_number*sizeof(uint32_t));
            //memset(tmp_cell, 0, bucket_number*sizeof(uint32_t));

            delete [] table[i].cell;
            table[i].cell = tmp_cell;

            mutex *tmp_mut = new mutex[bucket_number];
            delete [] bucket_mut[i];
            bucket_mut[i] = tmp_mut;

            for (int t = 0; t < thread_num; ++t) {
                indexType **tmp_visit = new indexType*[bucket_number];
                int **tmp_visitStep = new int*[bucket_number];

                for (int j=0; j<bucket_number; j++){
                    tmp_visit[j] = new indexType[maxNL];
                    for (int k = 0; k < maxNL; ++k)
                        tmp_visit[j][k] = indexTypeNotFound;
                    tmp_visitStep[j] = new int[maxNL];
                    memset(tmp_visitStep[j], 0, maxNL*sizeof(int));
                }

                for (int j=0; j<old_bucket_number; j++) {
                    delete [] visit[t][i][j];
                    delete [] visitStep[t][i][j];
                }
                delete [] visit[t][i];
                delete [] visitStep[t][i];

                visit[t][i] = tmp_visit;
                visitStep[t][i] = tmp_visitStep;
            }
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
                Entry (*tmp_entry)[N];

                ra[0][0] = get_offset_table(i, j, 0, 1);
                read_cnt += get_clean_info();
                //read_cnt += 5;
                tmp_entry = clean_buf;

                // check if new hash(key) == old hash(key) 
                for (int k = 0; k < 5; ++k) {
                    int cell = 0;
                    int h1, h2;
                    int fp;

                    if (i == TABLE1) {
                        for (int l = 0; l < N; ++l) {
                            h1 = hash1(tmp_entry[k][l].key);
                            if (h1 == j+k) {
                                memset(table[TABLE1].bucket[j+k+old_bucket_number].sig[l], 0, SIG_LEN);
                                table[TABLE1].bucket[j+k+old_bucket_number].full[l] = false;
                                table[TABLE1].bucket[j+k+old_bucket_number].fixed[l] = false;
                                cell++;
                            }
                            else {
                                memset(table[TABLE1].bucket[j+k].sig[l], 0, SIG_LEN);
                                table[TABLE1].bucket[j+k].full[l] = false;
                                table[TABLE1].bucket[j+k].fixed[l] = false;
                            }
                        }
                    }
                    else {
                        for (int l = 0; l < L; ++l) {
                            h1 = hash1(tmp_entry[k][l].key);
                            fp = calculate_fp(tmp_entry[k][l].key);
                            h2 = hash_alt(h1, TABLE1, fp);
                            if (h2 == j+k) {
                                memset(table[TABLE2].bucket[j+k+old_bucket_number].sig[l], 0, SIG_LEN);
                                table[TABLE2].bucket[j+k+old_bucket_number].full[l] = false;
                                table[TABLE2].bucket[j+k+old_bucket_number].fixed[l] = false;
                                cell++;
                            }
                            else {
                                memset(table[TABLE2].bucket[j+k].sig[l], 0, SIG_LEN);
                                table[TABLE2].bucket[j+k].full[l] = false;
                                table[TABLE2].bucket[j+k].fixed[l] = false;
                            }
                        }
                    }

                    table[i].cell[j+k] = cell;
                    table[i].cell[j+k+old_bucket_number] -= cell;
                }
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
        this->table[TABLE1].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE1].cell, 0, this->bucket_number*sizeof(uint32_t));
        this->table[TABLE2].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE2].cell, 0, this->bucket_number*sizeof(uint32_t));
        
        this->thread_num = thread_num;
        this->connect_num = connect_num;
        for (int tid = 0; tid < thread_num; ++tid) {
            this->visit.push_back({});
            this->visit[tid] = new indexType**[2];
            for (int i=0; i<2; i++){
                this->visit[tid][i] = new indexType*[this->bucket_number];
                for (int j=0; j<this->bucket_number; j++){
                    this->visit[tid][i][j] = new indexType[maxNL];
                    memset(this->visit[tid][i][j], 0, maxNL*sizeof(indexType));
                }
            }
        }

        for (int tid = 0; tid < thread_num; ++tid) {
            this->visitStep.push_back({});
            this->visitStep[tid] = new int**[2];
            for (int i=0; i<2; i++){
                this->visitStep[tid][i] = new int*[this->bucket_number];
                for (int j=0; j<this->bucket_number; j++){
                    this->visitStep[tid][i][j] = new int[maxNL];
                    memset(this->visitStep[tid][i][j], 0, maxNL*sizeof(int));
                }
            }
        }

        initialize_hash_functions();

        stash_num = 0;

        for (int tid = 0; tid < thread_num; ++tid)
        for (int i=0; i<2; i++) for (int j=0; j<this->bucket_number; j++) for (int k=0; k<maxNL; k++)
            visit[tid][i][j][k] = indexTypeNotFound;

        for (int tid = 0; tid < thread_num; ++tid) {
            this->bfsQueue.push_back({});
            this->dirtyList.push_back({});
        }

		this->bucket_mut[TABLE1] = new mutex[this->bucket_number];
		this->bucket_mut[TABLE2] = new mutex[this->bucket_number];
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;

		delete [] bucket_mut[TABLE1];
		delete [] bucket_mut[TABLE2];

        for (int tid = 0; tid < thread_num; ++tid) {
            for (int i=0; i<2; i++){
                for (int j=0; j<this->bucket_number; j++){
                    delete [] visit[tid][i][j];
                }
                delete [] visit[tid][i];
            }
            delete [] visit[tid];
        }

        for (int tid = 0; tid < thread_num; ++tid) {
            for (int i=0; i<2; i++){
                for (int j=0; j<this->bucket_number; j++){
                    delete [] visitStep[tid][i][j];
                }
                delete [] visitStep[tid][i];
            }
            delete [] visitStep[tid];
        }
    }
};

}