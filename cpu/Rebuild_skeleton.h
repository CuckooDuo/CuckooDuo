/*
    this version fix many params to insert faster

    1. CPU_write repeatedly calculates the sig many times
    2. define a struct { key val sig1 sig2 fix}
    3. for (i) if is full continue: change to i = next(not full cell) maybe faster
    4. visitStep can be uint4_t
    5. insert to head of Queue if find empty cell, so that bfs stop earlier?
    6. cell use int8 fast or slow?
    7. RDMA read access memory times in rebuild not optimized
    8. calculate_fp can be faster if use (char *)key_type_value instead of while do convert

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
// #include <queue>
#include <deque>
#include <bitset>
#include <assert.h>
#pragma once

#include <functional>

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

// #define DEBUG_FLAG

#define KEY_TYPE uint64_t
#define VAL_TYPE uint64_t
#define SIG_TYPE uint16_t
#define BOOL_ARR uint8_t
#define indexType uint32_t

#define DEFAULT_INVALID_SIG 0xffff

#define KEY_LEN 8
#define VAL_LEN 8

struct inputEntry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

KEY_TYPE turnInKV(const char *KV){
    KEY_TYPE InKV = 0;
    int byte = KEY_LEN;
    while(--byte >= 0) {
        InKV = InKV << 8;
        InKV += (unsigned char)KV[byte];
    }
    return InKV;
}

char *turnOutKV(KEY_TYPE KV){
    char *OutKV = new char[KEY_LEN];
    int byte = -1;
    while(++byte < KEY_LEN) {
        OutKV[byte] = KV & 0b11111111;
        KV = KV >> 8;
    }
    return OutKV;
}

struct Entry{
    KEY_TYPE key;
    VAL_TYPE val;
}emptyEntry;
typedef std::pair<int, std::pair<Entry, Entry> > rebuildInfo;

Entry turnInEntry(inputEntry OutEntry){
    Entry result = (Entry){
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

Entry RDMA_Simulation[maxIndexType + 5];

/* table parameters */
#define N MY_BUCKET_SIZE                // item(cell) number in a bucket (TABLE1)
#define M (MY_BUCKET_SIZE*3/4)          // item(cell) number in a bucket using hash1 (N-M for hash2) (TABLE1)
#define L MY_BUCKET_SIZE                // item(cell) number in a bucket (TABLE2)
#define maxNL MY_BUCKET_SIZE            // max(N, L)
#define SIG_LEN 2                       // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 100                   // size of TCAM
#define TABLE1 0                        // index of table1
#define TABLE2 1                        // index of table2
#define rebuildError -1
#define rebuildOK 1
#define rebuildNeedkick 0
#define rebuildNeedTwokick 2


// following params are fixed:
static_assert(MY_BUCKET_SIZE == 8, "MY_BUCKET_SIZE must be 8");
static_assert(KEY_LEN == 8, "MY_BUCKET_SIZE must be 8");
static_assert(VAL_LEN == 8, "MY_BUCKET_SIZE must be 8");
static_assert(SIG_LEN == 2, "SIG_LEN must be 2");
// end fixed params

struct Bucket{
    KEY_TYPE key[maxNL];
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
    std::unordered_map<KEY_TYPE, VAL_TYPE> TCAM;  //overflow cam

    uint32_t seed_hash_to_fp, seed_hash_to_fp2;
    uint32_t seed_hash_to_bucket;
    uint32_t seed_hash_to_alt;

    //test data
#ifdef DEBUG_FLAG
    int collision_num;
    int continueCounter, adjustmentCounter;
    int sumPathLength, bfsTimes, sumBFSqueueLength;
#endif
    int tcam_num;
    int move_num, max_move_num, sum_move_num; 
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; 
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; 

private:
    void initialize_hash_functions(){
        std::set<int> seeds;
        // srand((unsigned)time(NULL));
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
        auto OutKey = turnOutKV(key);
        uint32_t fp = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp);
        delete [] OutKey;
        if (fphash == 1) return fp & (65535);
        else return fp >> 16;
    }

    void calculate_two_fp(KEY_TYPE key, SIG_TYPE &fp1, SIG_TYPE &fp2) {
        auto OutKey = turnOutKV(key);
        uint32_t fp = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_fp);
        delete [] OutKey;
        fp1 = fp & (65535);
        fp2 = fp >> 16;
    }

    // return bucket index for table 1.
    int hash1(KEY_TYPE key) {
        auto OutKey = turnOutKV(key);
        int result = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_bucket) % bucket_number;
        delete [] OutKey;
        return result;
    }

    // return alternate index for h_now, use h_now mod+/- hash(fp).
    int hash_alt(int h_now, int table_now, SIG_TYPE fp) {
    #ifdef DEBUG_FLAG
        assert(h_now >= 0 && h_now < bucket_number);
    #endif
        if(table_now == TABLE1)
            return (h_now + fp) % bucket_number;
        else if(table_now == TABLE2) {
            int h_alt = (h_now - fp) % bucket_number;
            if(h_alt < 0) h_alt += bucket_number;
            return h_alt;
        }
    #ifdef DEBUG_FLAG
        printf("wrong parameter: table not 1 or 2\n");
        assert(false);
        return -1;
    #endif
    }

    // insert an full entry to TCAM
    bool insert_to_cam(const Entry& entry) {
        if(tcam_num > TCAM_SIZE)
            return false;
        TCAM.emplace(entry.key, entry.val);
        tcam_num++;
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

    // simulate RDMA, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry){
        move_num ++;
        RDMA_Simulation[getIndexType(tableID, bucketID, cellID)] = entry;
    }
    inline void CPU_write(int tableID, int bucketID, int cellID, Entry entry, 
    SIG_TYPE sig = DEFAULT_INVALID_SIG, SIG_TYPE sig2 = DEFAULT_INVALID_SIG){
        table[tableID].bucket[bucketID].key[cellID] = entry.key;
        if (sig2 != DEFAULT_INVALID_SIG){
            table[tableID].bucket[bucketID].sig[cellID] = (tableID == TABLE2 || cellID < M) ? sig : sig2;
        } else if (sig != DEFAULT_INVALID_SIG){
            table[tableID].bucket[bucketID].sig[cellID] = sig;
        } else {
            table[tableID].bucket[bucketID].sig[cellID] = 
                calculate_fp(entry.key, (tableID == TABLE2 || cellID < M) ? 1 : 2);
        }
    }

    // simulate RDMA, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID){
        RDMA_read_num ++;
        RDMA_read_num2 ++;
        entry = RDMA_Simulation[getIndexType(tableID, bucketID, cellID)];
    }
    inline void RDMA_read_bucket(Entry *entry, int tableID, int bucketID){
        RDMA_read_num ++;
        RDMA_read_num2 += maxNL;
        for (int i = 0; i < maxNL; i++)
            entry[i] = RDMA_Simulation[getIndexType(tableID, bucketID, i)];
    }

    // rebuild bucket with new entry, return (rebuildError, 0) if need insert to cache, 
    // return (rebuildNeedkick, kickEntry) if need kick, return (rebuildOK, 0) if done
    rebuildInfo rebuild(const Entry& entry, int balanceFlag = 1){
        if (hash1coll != -1){
            RDMA_read(insertEntry[insertCnt], TABLE2, h[TABLE2], hash1coll);
            RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
            if (entryHash2CollFlag){
                if (isFull(current_bucket->full, 6+fixedCnt))
                    RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], 6+fixedCnt);
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, insertEntry[0]);
                CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, insertEntry[0]);
            } else {
                if (isFull(current_bucket->full, 6+fixedCnt))
                    RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], 6+fixedCnt);
                RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry);
                CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
            }
            if (hash1Cnt < 6){
                for (int i=0; i<6; i++){
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                    CPU_write(TABLE1, h[TABLE1], i, insertEntry[0], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll]);
                    break;
                }
            } else {
                for (int i=0; i<6; i++){
                    RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], i);
                    RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                    CPU_write(TABLE1, h[TABLE1], i, insertEntry[0], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll]);
                    break;
                }
            }
            return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
        } else {
            if (table1coll >= 6){
                if (table1coll == 6 && fixedCnt == 1){
                    RDMA_read(entryA, TABLE1, h[TABLE1], entryAID);
                    RDMA_read(entryB, TABLE1, h[TABLE1], 6);
                    RDMA_write(TABLE1, h[TABLE1], entryAID, entryB);
                    CPU_write(TABLE1, h[TABLE1], entryAID, entryB);
                    RDMA_write(TABLE1, h[TABLE1], 6, entryA);
                    CPU_write(TABLE1, h[TABLE1], 6, entryA);
                    if (hash1Cnt < 6){
                        for (int i=0; i<6; i++){
                            RDMA_write(TABLE1, h[TABLE1], i, entry);
                            CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                            break;
                        }
                    } else {
                        for (int i=0; i<6; i++){
                            RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], i);
                            RDMA_write(TABLE1, h[TABLE1], i, entry);
                            CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                            break;
                        }
                    }
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
                RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], table1coll);
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        RDMA_write(TABLE1, h[TABLE1], i, entry);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], i);
                        RDMA_write(TABLE1, h[TABLE1], i, entry);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        break;
                    }
                }
                if (hash1Cnt < 6){
                    for (int i=0; i<6; i++){
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        CPU_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], i);
                        RDMA_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        CPU_write(TABLE1, h[TABLE1], i, insertEntry[0]);
                        break;
                    }
                }
                return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
            } else if (table1coll >= 0){
                RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
                if (entryHash2CollFlag){
                    RDMA_read(collEntry, TABLE1, h[TABLE1], table1coll);
                    RDMA_write(TABLE1, h[TABLE1], table1coll, entry);
                    CPU_write(TABLE1, h[TABLE1], table1coll, entry, fp);
                    if (isFull(current_bucket->full, 6+fixedCnt))
                        RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], 6+fixedCnt);
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, collEntry);
                    CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, collEntry);
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                } else {
                    if (isFull(current_bucket->full, 6+fixedCnt))
                        RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], 6+fixedCnt);
                    RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry);
                    CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
                    return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                }
            } else {
                for (int i=6; i<8; i++){
                    RDMA_read(collEntry, TABLE1, h[TABLE1], i);
                    calculate_two_fp(collEntry.key, collfp1, collfp2);
                    if (collfp1 == fp){
                        if (i == 6 && fixedCnt == 1){
                            if (isFull(current_bucket->full, 6+fixedCnt))
                                RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], 6+fixedCnt);
                            RDMA_write(TABLE1, h[TABLE1], 6+fixedCnt, entry);
                            CPU_write(TABLE1, h[TABLE1], 6+fixedCnt, entry, fp2);
                        } else {
                            if (i == 7 && fixedCnt == 0){
                                if (!isFull(current_bucket->full, 6)){
                                    RDMA_read(tmpEntry, TABLE1, h[TABLE1], 7);
                                    RDMA_write(TABLE1, h[TABLE1], 6, tmpEntry);
                                    CPU_write(TABLE1, h[TABLE1], 6, tmpEntry, current_bucket->sig[7]);
                                } else {
                                    RDMA_read(Entry6, TABLE1, h[TABLE1], 6);
                                    RDMA_read(Entry7, TABLE1, h[TABLE1], 7);
                                    RDMA_write(TABLE1, h[TABLE1], 6, Entry7);
                                    CPU_write(TABLE1, h[TABLE1], 6, Entry7, sig7);
                                    RDMA_write(TABLE1, h[TABLE1], 7, Entry6);
                                    CPU_write(TABLE1, h[TABLE1], 7, Entry6, sig6);
                                }
                            }
                            if (hash1Cnt < 6){
                                for (int i=0; i<6; i++){
                                    RDMA_write(TABLE1, h[TABLE1], i, entry);
                                    CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                                    break;
                                }
                            } else {
                                for (int i=0; i<6; i++){
                                    RDMA_read(insertEntry[insertCnt], TABLE1, h[TABLE1], i);
                                    RDMA_write(TABLE1, h[TABLE1], i, entry);
                                    CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                                    break;
                                }
                            }
                        }
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                if (balanceFlag && (0 < (L-(int)table[TABLE2].cell[h[TABLE2]]))
                && ((N-(int)table[TABLE1].cell[h[TABLE1]]) <= (L-(int)table[TABLE2].cell[h[TABLE2]]))){
                    for (int i=0; i<8; i++){
                        RDMA_write(TABLE2, h[TABLE2], i, entry);
                        CPU_write(TABLE2, h[TABLE2], i, entry, fp);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                } else {
                    // 20240929 swap follow two parts
                    for (int i=0; i<6; i++){
                        RDMA_write(TABLE1, h[TABLE1], i, entry);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                    for (int i=6; i<8; i++){
                        RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
                        RDMA_write(TABLE1, h[TABLE1], i, entry);
                        CPU_write(TABLE1, h[TABLE1], i, entry, fp2);
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                return std::make_pair(rebuildNeedkick, std::make_pair(entry, emptyEntry));
            }
        }
    }

    indexType* visit;
    int *visitStep;
    std::deque<indexType> bfsQueue, dirtyList;
    // once find a possible augment path, clean all other queue elements
    // if from layer 2, no need to expand to not empty layer 3
    void bfsSpread(indexType from){
        int tableID = getTableID(from);
        int bucketID = getBucketID(from);
        int cellID = getCellID(from);
        SIG_TYPE sig = table[tableID].bucket[bucketID].sig[cellID];
        int altBucketID = hash_alt(bucketID, tableID, sig);
        
        if (visitStep[from] >= 3) return;
        // in queue elements 8^3 -> 8^2
        if (visitStep[from] == 2 && isFullBoolArray(
            table[tableID^1].bucket[altBucketID].full | 
            table[tableID^1].bucket[altBucketID].fixed)) return;

        for (int i=0; i<((tableID^1) == TABLE1 ? M : L); i++)
        if (!isFixed(table[tableID^1].bucket[altBucketID].fixed, i)){
            indexType target = getIndexType(tableID^1, altBucketID, i);
            if (visit[target] != indexTypeNotFound) continue;
            visit[target] = from;
            visitStep[target] = visitStep[from] + 1;
            if (!isFull(table[tableID^1].bucket[altBucketID].full, i)){
                bfsQueue.push_front(target);
                dirtyList.push_front(target);
                continue;
            }
            if (visitStep[from] == 2) continue;
            bfsQueue.push_back(target);
            dirtyList.push_back(target);
        }
    }

    void cleanVisit(){
        while (!dirtyList.empty()){
            indexType item = dirtyList.front();
            visit[item] = indexTypeNotFound;
            dirtyList.pop_front();
        }
    }

    bool bfs(Entry entry){
        SIG_TYPE fp = calculate_fp(entry.key, 1);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, fp);
        Bucket *current_bucket1 = &table[TABLE1].bucket[h[TABLE1]];
        Bucket *current_bucket2 = &table[TABLE2].bucket[h[TABLE2]];
        // modify for faster bfs, local is better
        // maybe need to assert bucket element arranged as full cells first
        while (!bfsQueue.empty()) bfsQueue.pop_front();
        int collID = check_in_table2(h[TABLE2], fp);
        if (collID == -1){
            for (int i=0; i<L; i++){
                indexType target = getIndexType(TABLE2, h[TABLE2], i);
                visit[target] = emptyIndex;
                visitStep[target] = 1;
                bfsQueue.push_back(target);
                dirtyList.push_back(target);
            }
        } else {
            std::cout << "unexpected error: hash1 collision" << std::endl;
        }
        for (int i=0; i<M; i++) if (!isFixed(current_bucket1->fixed, i)){
            indexType target = getIndexType(TABLE1, h[TABLE1], i);
            visit[target] = emptyIndex;
            visitStep[target] = 1;
            bfsQueue.push_back(target);
            dirtyList.push_back(target);
        }
        while (!bfsQueue.empty()){
            indexType from = bfsQueue.front();
            bfsQueue.pop_front();
        #ifdef DEBUG_FLAG
            assert(getTableID(from) == 0 || getTableID(from) == 1);
        #endif
            if (visitStep[from] > max_kick_number) break;
            int tableID = getTableID(from);
            int bucketID = getBucketID(from);
            int cellID = getCellID(from);
            Bucket *BFSbucket = &table[tableID].bucket[bucketID];
            if (!isFull(BFSbucket->full, cellID)){
                // find an empty cell, kick
                indexType indexList[10];
                Entry entryList[10];
                SIG_TYPE sigList[10], sig2List[10];
                int listCnt = 0;
                while (1){
                    indexList[listCnt++] = from;
                    from = visit[from];
                    if (from == emptyIndex) break;
                }
            #ifdef DEBUG_FLAG
                sumPathLength += listCnt;
                sumBFSqueueLength += dirtyList.size();
                bfsTimes ++;
            #endif
                for (int i=0; i<listCnt; i++){
                    if (i+1<listCnt){
                        int tmpTableID = getTableID(indexList[i+1]);
                        int tmpBucketID = getBucketID(indexList[i+1]);
                        int tmpCellID = getCellID(indexList[i+1]);
                        RDMA_read(entryList[i], tmpTableID, tmpBucketID, tmpCellID);
                        sigList[i] = table[tmpTableID].bucket[tmpBucketID].sig[tmpCellID];
                    }
                    else{
                        entryList[i] = entry;
                        sigList[i] = fp;
                    }
                }
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
                #ifdef DEBUG_FLAG
                    adjustmentCounter++;
                #endif
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
                                // rebuild
                                rebuildInfo info = rebuild(entryList[i], 0);
                                if (info.first == rebuildOK){
                                    // ajustment success
                                } else {
                                    if (info.first != rebuildNeedkick){
                                        printf("unexpected: info.first != rebuildNeedkick:%d\n", info.first);
                                        cleanVisit();
                                        return false;
                                    }
                                    // need to modify : insert 2 -> 1
                                    if(!insert_to_cam(info.second.first)){
                                        cleanVisit();
                                        return false;
                                    }
                                    if (i == 0) {
                                        cleanVisit();
                                        return true;
                                    } else {
                                        if(!insert_to_cam(entryList[i-1])){
                                            cleanVisit();
                                            return false;
                                        }
                                        cleanVisit();
                                        return true;
                                    }
                                }
                            } else {
                                RDMA_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i]);
                                CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);
                            }
                        } else {
                            RDMA_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i]);
                            CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);
                        }
                    }
                    if (!isFull(BFSbucket->full, cellID)){
                        setFull(BFSbucket->full, cellID, 1);
                        table[tableID].cell[bucketID] += 1;
                    }
                    cleanVisit();
                    return true;
                    // */
                }
                // no hash2 collision
                for (int i=0; i<listCnt; i++){
                    int tmpTableID = getTableID(indexList[i]);
                    int tmpBucketID = getBucketID(indexList[i]);
                    int tmpCellID = getCellID(indexList[i]);
                    RDMA_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i]);
                    CPU_write(tmpTableID, tmpBucketID, tmpCellID, entryList[i], sigList[i]);
                }
                if (!isFull(BFSbucket->full, cellID)){
                    setFull(BFSbucket->full, cellID, 1);
                    table[tableID].cell[bucketID] += 1;
                } else printf("unexpected full bucket cell\n");
                cleanVisit();
                return true;
            }
            bfsSpread(from);
        }
        cleanVisit();
        if(insert_to_cam(entry))
            return true;
        return false;
    }

public:
    void update_indicators(){
        max_move_num = std::max(max_move_num, move_num);
        sum_move_num += move_num;
        max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
        sum_RDMA_read_num += RDMA_read_num;
        max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
        sum_RDMA_read_num2 += RDMA_read_num2;
    }

    bool insert(const inputEntry& OutEntry) {
        Entry entry = turnInEntry(OutEntry);
        move_num = 0;
        RDMA_read_num = 0;
        RDMA_read_num2 = 0;
        rebuildInfo info = rebuild(entry);
        if (info.first == rebuildOK){
            update_indicators();
            return true;
        }
        if (info.first == rebuildError){
            update_indicators();
        #ifdef DEBUG_FLAG
            collision_num++;
        #endif
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }

        bool insertOK = bfs(info.second.first);
        if (!insertOK || info.first == rebuildNeedkick){
            update_indicators();
            return insertOK;
        }
        insertOK = bfs(info.second.second);
        update_indicators();
        return insertOK;
    }

    //query result put in val
    bool query(const char *key, char *val = NULL) {
        // query in TCAM
        KEY_TYPE InKey = turnInKV(key);
        auto entry = TCAM.find(InKey);
        if(entry != TCAM.end()) {
            if(val != NULL) {
                VAL_TYPE InVal = entry->second;
                auto OutVal = turnOutKV(InVal);
                memcpy(val, OutVal, VAL_LEN);
                delete [] OutVal;
            }
            return true;
        }

        //query in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InKey, fp, fp2);
        int h1 = hash1(InKey);
        int cell;

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            Entry readEntry;
            RDMA_read(readEntry, TABLE2, h2, cell);
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            auto tmpVal = turnOutKV(readEntry.val);
            memcpy(val, tmpVal, 8);
            delete [] tmpVal;
            if (InKey == readEntry.key)
                return true;
            else if (fp == calculate_fp(readEntry.key, 1)
                && fp2 == calculate_fp(readEntry.key, 2)) return true;
            else return false;
        }

        //query in table[TABLE1]
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            Entry readEntry;
            RDMA_read(readEntry, TABLE1, h1, cell);
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            auto tmpVal = turnOutKV(readEntry.val);
            memcpy(val, tmpVal, 8);
            delete [] tmpVal;
            if (InKey == readEntry.key)
                return true;
            else if (fp == calculate_fp(readEntry.key, 1)
                && fp2 == calculate_fp(readEntry.key, 2)) return true;
            else return false;
        }

        //miss
        return false;
    }

    //update result put in val
    bool update(const char *key, char *val = NULL) {
        // update in TCAM
        KEY_TYPE InKey = turnInKV(key);
        auto entry = TCAM.find(InKey);
        if(entry != TCAM.end()) {
            if(val != NULL) {
                VAL_TYPE InVal = turnInKV(val);
                TCAM.emplace(InKey, InVal);
            }
            return true;
        }

        //update in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InKey, fp, fp2);
        int h1 = hash1(InKey);
        int cell;

        //update in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            // if (memcmp(key, RDMAmap[TABLE2][h2][cell].key, KEY_LEN*sizeof(char)) != 0)
            //     return false;
            // memcpy(RDMAmap[TABLE2][h2][cell].val, val, VAL_LEN*sizeof(char));
            return true;
        }

        //update in table[TABLE1]
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            // if (memcmp(key, RDMAmap[TABLE1][h1][cell].key, KEY_LEN*sizeof(char)) != 0)
            //     return false;
            // memcpy(RDMAmap[TABLE1][h1][cell].val, val, VAL_LEN*sizeof(char));
            return true;
        }

        //miss
        return false;
    }

    //delete an entry given a key
    bool deletion(const char* key) {
        //query in TCAM, if find then delete
        KEY_TYPE InKey = turnInKV(key);
        auto entry = TCAM.find(InKey);
        if(entry != TCAM.end()) {
            TCAM.erase(entry);
            tcam_num--;
            return true;
        }

        //update in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InKey, fp, fp2);
        int h1 = hash1(InKey);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            table[TABLE1].bucket[h1].sig[cell] = 0;
            setFull(table[TABLE1].bucket[h1].full, cell, 0);
            /* RDMA write: 0 to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            table[TABLE2].bucket[h2].sig[cell] = 0;
            setFull(table[TABLE2].bucket[h2].full, cell, 0);
            /* RDMA write: 0 to table[TABLE2], bucket h2, cell cell*/
            return true;
        }

        //miss
        return false;
    }

    //update an entry
    bool update(const inputEntry& OutEntry) {
        // query in TCAM
        Entry InEntry = turnInEntry(OutEntry);
        auto it = TCAM.find(InEntry.key);
        if(it != TCAM.end()) {
            it->second = InEntry.val;
            return true;
        }

        //query in tables
        SIG_TYPE fp, fp2;
        calculate_two_fp(InEntry.key, fp, fp2);
        int h1 = hash1(InEntry.key);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, fp, fp2);
        if(cell != -1) {
            /* RDMA write: entry.val to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, fp);
        if(cell != -1) {
            /* RDMA write: entry.val to table[TABLE2], bucket h2, cell cell*/
            return true;
        }

        //miss
        return false;
    }

    CuckooHashTable(int cell_number, int max_kick_num) {
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
        
        this->visit = new indexType[maxIndexType + 5];
        this->visitStep = new int[maxIndexType + 5];
        for (int i = 0; i <= maxIndexType; i++)
            this->visit[i] = indexTypeNotFound;

        initialize_hash_functions();

        tcam_num = 0;
        move_num = max_move_num = sum_move_num = 0;
        RDMA_read_num = max_RDMA_read_num = sum_RDMA_read_num = 0;
        RDMA_read_num2 = max_RDMA_read_num2 = sum_RDMA_read_num2 = 0;
    #ifdef DEBUG_FLAG
        collision_num = 0;
        continueCounter = 0;
        adjustmentCounter = 0;
        sumPathLength = 0;
        bfsTimes = 0;
        sumBFSqueueLength = 0;
    #endif
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;
        delete [] visit;
        delete [] visitStep;
    }
};

}