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
#include <queue>
#include <bitset>
#pragma once

#include <functional>
// from boost (functional/hash):
// see http://www. boost . org/doc/libs/1_ 35_ 0/doc/html/hash/combine . htmL template


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


/* entry parameters */
#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
Entry emptyEntry;
// using namespace std;
typedef std::pair<int, std::pair<Entry, Entry> > rebuildInfo;
typedef std::pair<std::pair<int, int>, int> indexType;
#define emptyIndex std::make_pair(std::make_pair(-1, -1), -1)
#define indexTypeNotFound std::make_pair(std::make_pair(-2, -2), -2)

/* table parameters */
#define N MY_BUCKET_SIZE                 // item(cell) number in a bucket (TABLE1)
#define M (MY_BUCKET_SIZE*3/4)           // item(cell) number in a bucket using hash1 (N-M for hash2) (TABLE1)
#define L MY_BUCKET_SIZE                 // item(cell) number in a bucket (TABLE2)
#define maxNL MY_BUCKET_SIZE             // max(N, L)
#define SIG_LEN MY_SIGLEN           // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 64       // size of TCAM
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2
#define rebuildError -1
#define rebuildOK 1
#define rebuildNeedkick 0
#define rebuildNeedTwokick 2

struct Bucket{
    char key[maxNL][KEY_LEN];
    char val[maxNL][VAL_LEN];
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
    std::unordered_map<std::string, std::string> TCAM;  //overflow cam

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

    //test data
    int kick_num; 
    int kick_success_num;
    int collision_num;
    int fullbucket_num;
    int tcam_num;
    int move_num, max_move_num, sum_move_num; 
    int adjust_num, sum_adjust_num; 
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; 
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; 
    uint32_t *kick_stat;
    int guessSuccess, guessFail;
    int continueCounter, adjustmentCounter;
    int sumPathLength, bfsTimes, sumBFSqueueLength;

private:
    void initialize_hash_functions(){
        std::set<int> seeds;
        srand((unsigned)time(NULL));

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
        uint32_t result = MurmurHash3_x86_32(key, KEY_LEN, fphash == 1 ? seed_hash_to_fp : seed_hash_to_fp2);
        if (SIG_LEN < 4) result = result % (1 << (SIG_LEN * 8));
        return result;
        // uint32_t fp = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);// % (1 << (SIG_LEN * 16));
        // if (fphash == 1) return fp & (65535);
        // else return fp >> 16;
    }

    void calculate_two_fp(const char *key, int &fp1, int &fp2) {
        uint32_t result1 = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);
        uint32_t result2 = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp2);
        if (SIG_LEN < 4) result1 = result1 % (1 << (SIG_LEN * 8));
        if (SIG_LEN < 4) result2 = result1 % (1 << (SIG_LEN * 8));
        fp1 = result1;
        fp2 = result2;
        // uint32_t fp = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);// % (1 << (SIG_LEN * 16));
        // fp1 = fp & (65535);
        // fp2 = fp >> 16;
        // fp1 = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp);
        // fp2 = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp2);
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
        hfp = hfp % bucket_number;
        if(table_now == TABLE1) {
            return (((h_now + hfp) % bucket_number) + bucket_number) % bucket_number;
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
        hfp = hfp % bucket_number;
        if(table_now == TABLE1) {
            return (((h_now + hfp) % bucket_number) + bucket_number) % bucket_number;
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

    // insert an full entry to TCAM
    bool insert_to_cam(const Entry& entry) {
        if(tcam_num > TCAM_SIZE)
            return false;
        std::string skey(entry.key, KEY_LEN);
        std::string sval(entry.val, VAL_LEN);
        TCAM.emplace(skey, sval);
        tcam_num++;
        return true;
    }

    // given a sig, check whether table[TABLE1] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table1(int b, char *sig, char *sig2) {
        // for(int i = 0; i < table[TABLE1].cell[b]; i++) {
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
        // for(int i = 0; i < table[TABLE2].cell[b]; i++) {
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

    // std::unordered_map<indexType, Entry, pair_hash> RDMAmap;  // simulate RDMA
    // Entry ***RDMAmap;

    // simulate RDMA, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry, int adjustFlag = 1){
        move_num ++;
        adjust_num += adjustFlag;
        // RDMAmap[std::make_pair(std::make_pair(table, bucket), cell)] = entry;
        memcpy(table[tableID].bucket[bucketID].key[cellID], entry.key, KEY_LEN*sizeof(char));
        memcpy(table[tableID].bucket[bucketID].val[cellID], entry.val, VAL_LEN*sizeof(char));
    }
    inline void RDMA_write(Bucket *current_bucket, int cellID, Entry entry, int adjustFlag = 1){
        move_num ++;
        adjust_num += adjustFlag;
        // RDMAmap[std::make_pair(std::make_pair(table, bucket), cell)] = entry;
        memcpy(current_bucket->key[cellID], entry.key, KEY_LEN*sizeof(char));
        memcpy(current_bucket->val[cellID], entry.val, VAL_LEN*sizeof(char));
    }

    // simulate RDMA, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID){
        RDMA_read_num ++;
        RDMA_read_num2 ++;
        memcpy(entry.key, table[tableID].bucket[bucketID].key[cellID], KEY_LEN*sizeof(char));
        memcpy(entry.val, table[tableID].bucket[bucketID].val[cellID], VAL_LEN*sizeof(char));
    }
    inline void RDMA_read(Entry &entry, Bucket *current_bucket, int cellID){
        RDMA_read_num ++;
        RDMA_read_num2 ++;
        memcpy(entry.key, current_bucket->key[cellID], KEY_LEN*sizeof(char));
        memcpy(entry.val, current_bucket->val[cellID], VAL_LEN*sizeof(char));
    }
    inline void RDMA_read_bucket(Entry *entry, int tableID, int bucketID){
        RDMA_read_num ++;
        RDMA_read_num2 += N;
        for (int i=0; i<N; i++){
            memcpy(entry[i].key, table[tableID].bucket[bucketID].key[i], KEY_LEN*sizeof(char));
            memcpy(entry[i].val, table[tableID].bucket[bucketID].val[i], VAL_LEN*sizeof(char));
        }
    }

    // rebuild bucket with new entry, return (rebuildError, 0) if need insert to cache, 
    // return (rebuildNeedkick, kickEntry) if need kick, return (rebuildOK, 0) if done
    rebuildInfo rebuild(const Entry& entry, int balanceFlag = 1){
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
        if (hash1coll != -1){
            // remove from table2, put it in 0~5, put entry in 6~7
            // remove from table2
            RDMA_read(insertEntry[insertCnt], TABLE2, h[TABLE2], hash1coll);
            // table[TABLE2].bucket[h[TABLE2]].sig[hash1coll];
            ++insertCnt;
            table[TABLE2].bucket[h[TABLE2]].full[hash1coll] = 0;
            table[TABLE2].cell[h[TABLE2]]--;
            // replace if 6~7 not empty
            // check entry hash2coll with 0~5
            int entryHash2CollFlag = 0;
            /* read all */
            Entry allEntry[8];
            RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
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
            //     RDMA_read(entryJ, current_bucket, j);
            //     calculate_two_fp(entryJ.key, fpInt, fpJ);
            //     if (fpJ == fp2){
            //         entryHash2CollFlag = 1;
            //         j = 8;
            //     }
            // }
            if (entryHash2CollFlag){
                if (current_bucket->full[6+fixedCnt]){
                    RDMA_read(insertEntry[insertCnt], current_bucket, 6+fixedCnt);
                    // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                    ++insertCnt;
                } else {
                    current_bucket->full[6+fixedCnt] = 1;
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // coll -> 6~7, entry -> coll -> 0~5
                RDMA_write(current_bucket, 6+fixedCnt, insertEntry[0]);
                int collFp;
                calculate_two_fp(insertEntry[0].key, fpInt, collFp);
                fp_to_sig(current_bucket->sig[6+fixedCnt], collFp);
                insertEntry[0] = entry;
                memcpy(table[TABLE2].bucket[h[TABLE2]].sig[hash1coll], sig, SIG_LEN*sizeof(char));
            } else {
                if (current_bucket->full[6+fixedCnt]){
                    RDMA_read(insertEntry[insertCnt], current_bucket, 6+fixedCnt);
                    // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                    ++insertCnt;
                } else {
                    current_bucket->full[6+fixedCnt] = 1;
                    table[TABLE1].cell[h[TABLE1]]++;
                }
                // entry -> 6~7
                RDMA_write(current_bucket, 6+fixedCnt, entry);
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
                    RDMA_write(current_bucket, i, insertEntry[0]);
                    memcpy(current_bucket->sig[i], table[TABLE2].bucket[h[TABLE2]].sig[hash1coll], SIG_LEN*sizeof(char));
                    break;
                }
            } else {
                for (int i=0; i<6; i++){
                    if (current_bucket->fixed[i]) continue;
                    // kick
                    RDMA_read(insertEntry[insertCnt], current_bucket, i);
                    ++insertCnt;
                    current_bucket->fixed[i] = 1;
                    RDMA_write(current_bucket, i, insertEntry[0]);
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
                    RDMA_read(entryA, current_bucket, entryAID);
                    RDMA_read(entryB, current_bucket, 6);
                    calculate_two_fp(entryA.key, fpInt, fpA);
                    calculate_two_fp(entryB.key, fpB, fpInt);
                    RDMA_write(current_bucket, entryAID, entryB);
                    RDMA_write(current_bucket, 6, entryA);
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
                            RDMA_write(current_bucket, i, entry);
                            memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                            break;
                        }
                    } else {
                        for (int i=0; i<6; i++){
                            if (current_bucket->fixed[i]) continue;
                            // kick
                            RDMA_read(insertEntry[insertCnt], current_bucket, i);
                            ++insertCnt;
                            current_bucket->fixed[i] = 1;
                            RDMA_write(current_bucket, i, entry);
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
                RDMA_read(insertEntry[insertCnt], current_bucket, table1coll);
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
                        RDMA_write(current_bucket, i, entry);
                        memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (current_bucket->fixed[i]) continue;
                        // kick
                        RDMA_read(insertEntry[insertCnt], current_bucket, i);
                        ++insertCnt;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(current_bucket, i, entry);
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
                        RDMA_write(current_bucket, i, insertEntry[0]);
                        fp_to_sig(current_bucket->sig[i], insertFp[0]);
                        break;
                    }
                } else {
                    for (int i=0; i<6; i++){
                        if (current_bucket->fixed[i]) continue;
                        // kick
                        RDMA_read(insertEntry[insertCnt], current_bucket, i);
                        ++insertCnt;
                        current_bucket->fixed[i] = 1;
                        RDMA_write(current_bucket, i, insertEntry[0]);
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
                /* read all */
                Entry allEntry[8];
                RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
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
                //     RDMA_read(entryJ, current_bucket, j);
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
                    RDMA_read(collEntry, current_bucket, table1coll);
                    calculate_two_fp(collEntry.key, fpInt, collEntryFp2);
                    // fix coll, entry -> 0~5
                    current_bucket->fixed[table1coll] = 1;
                    RDMA_write(current_bucket, table1coll, entry);
                    fp_to_sig(current_bucket->sig[table1coll], fp);
                    // replace if 6~7 not empty
                    if (current_bucket->full[6+fixedCnt]){
                        RDMA_read(insertEntry[insertCnt], current_bucket, 6+fixedCnt);
                        // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                        ++insertCnt;
                    } else {
                        current_bucket->full[6+fixedCnt] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // coll -> 6~7
                    RDMA_write(current_bucket, 6+fixedCnt, collEntry);
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
                        RDMA_read(insertEntry[insertCnt], current_bucket, 6+fixedCnt);
                        // calculate_two_fp(insertEntry[insertCnt].key, insertFp[insertCnt], fpInt);
                        ++insertCnt;
                    } else {
                        current_bucket->full[6+fixedCnt] = 1;
                        table[TABLE1].cell[h[TABLE1]]++;
                    }
                    // entry -> 6~7
                    RDMA_write(current_bucket, 6+fixedCnt, entry);
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
                    RDMA_read(collEntry, current_bucket, i);
                    calculate_two_fp(collEntry.key, collfp1, collfp2);
                    if (collfp1 == fp){
                        if (i == 6 && fixedCnt == 1){
                            // 3 hash1 coll, put entry in 7
                            // replace if 7 not empty
                            if (current_bucket->full[6+fixedCnt]){
                                RDMA_read(insertEntry[insertCnt], current_bucket, 6+fixedCnt);
                                ++insertCnt;
                            } else {
                                current_bucket->full[6+fixedCnt] = 1;
                                table[TABLE1].cell[h[TABLE1]]++;
                            }
                            // entry -> 7
                            RDMA_write(current_bucket, 6+fixedCnt, entry);
                            memcpy(current_bucket->sig[6+fixedCnt], sig2, SIG_LEN*sizeof(char));
                        } else {
                            // put entry in 0~5, move coll 6/7 if need
                            // replace if 6/7 not empty
                            if (i == 7 && fixedCnt == 0){
                                // need move
                                // replace 6
                                if (!current_bucket->full[6]){
                                    // empty, just move from 7 to 6
                                    Entry tmpEntry;
                                    RDMA_read(tmpEntry, current_bucket, 7);
                                    RDMA_write(current_bucket, 6, tmpEntry);
                                    current_bucket->full[6] = 1;
                                    current_bucket->full[7] = 0;
                                    memcpy(current_bucket->sig[6], current_bucket->sig[7], SIG_LEN*sizeof(char));
                                } else {
                                    // full, swap 6 and 7
                                    Entry Entry6, Entry7;
                                    char sig6[SIG_LEN], sig7[SIG_LEN];
                                    RDMA_read(Entry6, current_bucket, 6);
                                    RDMA_read(Entry7, current_bucket, 7);
                                    RDMA_write(current_bucket, 6, Entry7);
                                    RDMA_write(current_bucket, 7, Entry6);
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
                                    RDMA_write(current_bucket, i, entry);
                                    memcpy(current_bucket->sig[i], sig, SIG_LEN*sizeof(char));
                                    break;
                                }
                            } else {
                                for (int i=0; i<6; i++){
                                    if (current_bucket->fixed[i]) continue;
                                    // kick
                                    RDMA_read(insertEntry[insertCnt], current_bucket, i);
                                    ++insertCnt;
                                    current_bucket->fixed[i] = 1;
                                    RDMA_write(current_bucket, i, entry);
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
                        RDMA_write(&table[TABLE2].bucket[h[TABLE2]], i, entry);
                        memcpy(table[TABLE2].bucket[h[TABLE2]].sig[i], sig, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                } else {
                    for (int i=6; i<8; i++){
                        if (table[TABLE1].bucket[h[TABLE1]].full[i]) continue;
                        // should check hash2coll with 0~5, if insert 6~7
                        /* read all */
                        Entry allEntry[8];
                        RDMA_read_bucket(allEntry, TABLE1, h[TABLE1]);
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
                        //     RDMA_read(entryJ, current_bucket, j);
                        //     calculate_two_fp(entryJ.key, fpInt, fpJ);
                        //     if (fpJ == fp2){
                        //         i = 8;
                        //         j = 8;
                        //     }
                        // }
                        if (i >= 8) break;
                        table[TABLE1].cell[h[TABLE1]]++;
                        table[TABLE1].bucket[h[TABLE1]].full[i] = 1;
                        RDMA_write(current_bucket, i, entry);
                        memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig2, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                    for (int i=0; i<6; i++){
                        if (table[TABLE1].bucket[h[TABLE1]].full[i]) continue;
                        table[TABLE1].cell[h[TABLE1]]++;
                        table[TABLE1].bucket[h[TABLE1]].full[i] = 1;
                        RDMA_write(current_bucket, i, entry);
                        memcpy(table[TABLE1].bucket[h[TABLE1]].sig[i], sig, SIG_LEN*sizeof(char));
                        return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
                    }
                }
                return std::make_pair(rebuildNeedkick, std::make_pair(entry, emptyEntry));
            }
        }
    }
    // std::unordered_map<indexType, indexType, pair_hash> visit;
    // std::unordered_map<indexType, int, pair_hash> visitStep;
    indexType ***visit;
    int ***visitStep;
    std::queue<indexType> bfsQueue, dirtyList;
    void bfsSpread(indexType from){
        int tableID = from.first.first;
        int bucketID = from.first.second;
        int cellID = from.second;
        char *sig = table[tableID].bucket[bucketID].sig[cellID];
        int altBucketID = hash_alt(bucketID, tableID, sig);

        for (int i=0; i<((tableID^1) == TABLE1 ? M : L); i++) if (!table[tableID^1].bucket[altBucketID].fixed[i]){
            indexType target = std::make_pair(std::make_pair(tableID^1, altBucketID), i);
            if (visit[target.first.first][target.first.second][target.second] != indexTypeNotFound) continue;
            visit[target.first.first][target.first.second][target.second] = from;
            visitStep[target.first.first][target.first.second][target.second] = visitStep[from.first.first][from.first.second][from.second] + 1;
            bfsQueue.push(target);
            dirtyList.push(target);
        }
    }

    void cleanVisit(){
        while (!dirtyList.empty()){
            indexType item = dirtyList.front();
            visit[item.first.first][item.first.second][item.second] = indexTypeNotFound;
            // visitStep[item.first.first][item.first.second][item.second] = 0;
            dirtyList.pop();
        }
    }

    bool bfs(Entry entry){
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
        while (!bfsQueue.empty()) bfsQueue.pop();
        int collID = check_in_table2(h[TABLE2], sig);
        if (collID == -1){
            for (int i=0; i<L; i++){
                indexType target = std::make_pair(std::make_pair(TABLE2, h[TABLE2]), i);
                visit[target.first.first][target.first.second][target.second] = emptyIndex;
                visitStep[target.first.first][target.first.second][target.second] = 1;
                bfsQueue.push(target);
                dirtyList.push(target);
            }
        } else {
            // std::cout << "unexpected error: hash1 collision" << std::endl;
        }
        for (int i=0; i<M; i++) if (!current_bucket1->fixed[i]){
            indexType target = std::make_pair(std::make_pair(TABLE1, h[TABLE1]), i);
            visit[target.first.first][target.first.second][target.second] = emptyIndex;
            visitStep[target.first.first][target.first.second][target.second] = 1;
            bfsQueue.push(target);
            dirtyList.push(target);
        }
        while (!bfsQueue.empty()){// (kick_num < max_kick_number){  //kick
            // kick_num++;
            indexType from = bfsQueue.front();
            bfsQueue.pop();
            if (from.first.first != 0 && from.first.first != 1) return false;
            if (visitStep[from.first.first][from.first.second][from.second] > max_kick_number) break;
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
                while (1){
                    indexList[listCnt++] = from;
                    from = visit[from.first.first][from.first.second][from.second];
                    if (from == emptyIndex) break;
                }
                sumPathLength += listCnt;
                sumBFSqueueLength += dirtyList.size();
                bfsTimes ++;
                for (int i=0; i<listCnt; i++){
                    if (i+1<listCnt){
                        Bucket *current_bucket = &table[indexList[i+1].first.first].bucket[indexList[i+1].first.second];
                        RDMA_read(entryList[i], current_bucket, indexList[i+1].second);
                        memcpy(sigList[i], current_bucket->sig[indexList[i+1].second], SIG_LEN*sizeof(char));
                    }
                    else{
                        entryList[i] = entry;
                        memcpy(sigList[i], sig, SIG_LEN*sizeof(char));
                    }
                }
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
                    adjustmentCounter++;
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
                                memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                                RDMA_write(current_bucket, indexList[i].second, entryList[i], 0);
                            }
                        } else {
                            memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                            RDMA_write(current_bucket, indexList[i].second, entryList[i], 0);
                        }
                    }
                    if (!BFSbucket->full[cellID]){
                        BFSbucket->full[cellID] = 1;
                        table[tableID].cell[bucketID] += 1;
                    }
                    cleanVisit();
                    return true;
                    // */
                }
                // no hash2 collision
                for (int i=0; i<listCnt; i++){
                    Bucket *current_bucket = &table[indexList[i].first.first].bucket[indexList[i].first.second];
                    memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                    RDMA_write(current_bucket, indexList[i].second, entryList[i], 0);
                }
                if (!BFSbucket->full[cellID]){
                    BFSbucket->full[cellID] = 1;
                    table[tableID].cell[bucketID] += 1;
                } else printf("unexpected full bucket cell\n");
                cleanVisit();
                return true;
            }
            // if (dirtyList.size() < 50) 
                bfsSpread(from);
        }
        cleanVisit();
        if(insert_to_cam(entry))
            return true;
        return false;
    }
public:
    bool insert(const Entry& entry) {
        move_num = 0;
        adjust_num = 0;
        RDMA_read_num = 0;
        RDMA_read_num2 = 0;
        rebuildInfo info = rebuild(entry);
        if (info.first == rebuildOK){
            adjust_num--;
            max_move_num = std::max(max_move_num, move_num);
            sum_move_num += move_num;
            sum_adjust_num += adjust_num;
            max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
            sum_RDMA_read_num += RDMA_read_num;
            max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
            sum_RDMA_read_num2 += RDMA_read_num2;
            return true;
        }
        if (info.first == rebuildError){
            max_move_num = std::max(max_move_num, move_num);
            sum_move_num += move_num;
            sum_adjust_num += adjust_num;
            max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
            sum_RDMA_read_num += RDMA_read_num;
            max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
            sum_RDMA_read_num2 += RDMA_read_num2;
            collision_num++;
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }

        bool insertOK = bfs(info.second.first);
        if (!insertOK || info.first == rebuildNeedkick){
            max_move_num = std::max(max_move_num, move_num);
            sum_move_num += move_num;
            sum_adjust_num += adjust_num;
            max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
            sum_RDMA_read_num += RDMA_read_num;
            max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
            sum_RDMA_read_num2 += RDMA_read_num2;
            return insertOK;
        }
        insertOK = bfs(info.second.second);
        max_move_num = std::max(max_move_num, move_num);
        sum_move_num += move_num;
        sum_adjust_num += adjust_num;
        max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
        sum_RDMA_read_num += RDMA_read_num;
        max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
        sum_RDMA_read_num2 += RDMA_read_num2;
        return insertOK;
    }

    //query result put in val
    bool query(const char *key, char *val = NULL) {
        // query in TCAM
        std::string skey(key, KEY_LEN);
        auto entry = TCAM.find(skey);
        if(entry != TCAM.end()) {
            if(val != NULL) {
                std::string sval = entry->second;
                char* pval = const_cast<char*>(sval.c_str());
                memcpy(val, pval, VAL_LEN);
            }
            return true;
        }

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

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            Entry readEntry;
            RDMA_read(readEntry, TABLE2, h2, cell);
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            if (memcmp(key, readEntry.key, KEY_LEN*sizeof(char)) == 0)
                return true;
            else if (fp == calculate_fp(readEntry.key)
                && fp2 == calculate_fp(readEntry.key, 2)) return true;
            else return false;
        }

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            Entry readEntry;
            RDMA_read(readEntry, TABLE1, h1, cell);
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            if (memcmp(key, readEntry.key, KEY_LEN*sizeof(char)) == 0)
                return true;
            else if (fp == calculate_fp(readEntry.key)
                && fp2 == calculate_fp(readEntry.key, 2)) return true;
            else return false;
        }

        //miss
        return false;
    }

    //update result put in val
    bool update(const char *key, char *val = NULL) {
        // update in TCAM
        std::string skey(key, KEY_LEN);
        auto entry = TCAM.find(skey);
        if(entry != TCAM.end()) {
            if(val != NULL) {
                std::string sval(val, VAL_LEN);
                TCAM.emplace(skey, sval);
            }
            return true;
        }

        //query in tables
        int fp = calculate_fp(key);
        int fp2 = calculate_fp(key, 2);
        int h1 = hash1(key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            // if (memcmp(key, RDMAmap[TABLE2][h2][cell].key, KEY_LEN*sizeof(char)) != 0)
            //     return false;
            // memcpy(RDMAmap[TABLE2][h2][cell].val, val, VAL_LEN*sizeof(char));
            return true;
        }

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig, sig2);
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
        std::string skey(key, KEY_LEN);
        auto entry = TCAM.find(skey);
        if(entry != TCAM.end()) {
            TCAM.erase(entry);
            tcam_num--;
            return true;
        }

        //query in tables
        int fp = calculate_fp(key);
        int fp2 = calculate_fp(key, 2);
        int h1 = hash1(key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            memset(table[TABLE1].bucket[h1].sig[cell], 0, SIG_LEN);
            table[TABLE1].bucket[h1].full[cell] = 0;
            /* RDMA write: 0 to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            memset(table[TABLE2].bucket[h2].sig[cell], 0, SIG_LEN);
            table[TABLE2].bucket[h2].full[cell] = 0;
            /* RDMA write: 0 to table[TABLE2], bucket h2, cell cell*/
            return true;
        }

        //miss
        return false;
    }

    //update an entry
    bool update(const Entry& entry) {
        // query in TCAM
        std::string skey(entry.key, KEY_LEN);
        auto it = TCAM.find(skey);
        if(it != TCAM.end()) {
            std::string sval = entry.val;
            it->second = sval;
            return true;
        }

        //query in tables
        int fp = calculate_fp(entry.key);
        int fp2 = calculate_fp(entry.key, 2);
        int h1 = hash1(entry.key);
        char sig[SIG_LEN], sig2[SIG_LEN];
        fp_to_sig(sig, fp);
        fp_to_sig(sig2, fp2);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            /* RDMA write: entry.val to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, fp);
        cell = check_in_table2(h2, sig);
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
        this->table[TABLE1].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE1].cell, 0, this->bucket_number*sizeof(uint32_t));
        this->table[TABLE2].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE2].cell, 0, this->bucket_number*sizeof(uint32_t));
        
        // this->RDMAmap = new Entry[2][this->bucket_number][N];
        // memset(RDMAmap, 0, (long long)2*this->bucket_number*N*sizeof(Entry));
        // this->RDMAmap = new Entry**[2];
        // for (int i=0; i<2; i++){
        //     this->RDMAmap[i] = new Entry*[this->bucket_number];
        //     for (int j=0; j<this->bucket_number; j++){
        //         this->RDMAmap[i][j] = new Entry[maxNL];
        //         memset(this->RDMAmap[i][j], 0, maxNL*sizeof(Entry));
        //     }
        // }
        this->visit = new indexType**[2];
        for (int i=0; i<2; i++){
            this->visit[i] = new indexType*[this->bucket_number];
            for (int j=0; j<this->bucket_number; j++){
                this->visit[i][j] = new indexType[maxNL];
                memset(this->visit[i][j], 0, maxNL*sizeof(indexType));
            }
        }
        this->visitStep = new int**[2];
        for (int i=0; i<2; i++){
            this->visitStep[i] = new int*[this->bucket_number];
            for (int j=0; j<this->bucket_number; j++){
                this->visitStep[i][j] = new int[maxNL];
                memset(this->visitStep[i][j], 0, maxNL*sizeof(int));
            }
        }

        initialize_hash_functions();

        collision_num = 0;
        kick_num = 0;
        fullbucket_num = 0;
        tcam_num = 0;
        kick_success_num = 0;
        move_num = max_move_num = sum_move_num = 0;
        adjust_num = sum_adjust_num = 0;
        RDMA_read_num = max_RDMA_read_num = sum_RDMA_read_num = 0;
        RDMA_read_num2 = max_RDMA_read_num2 = sum_RDMA_read_num2 = 0;
        this->kick_stat = new uint32_t[max_kick_num+1];
        memset(kick_stat, 0, (max_kick_num+1)*sizeof(uint32_t));
        guessSuccess = 0;
        guessFail = 0;
        continueCounter = 0;
        adjustmentCounter = 0;
        sumPathLength = 0;
        bfsTimes = 0;
        sumBFSqueueLength = 0;
        for (int i=0; i<2; i++) for (int j=0; j<this->bucket_number; j++) for (int k=0; k<maxNL; k++)
            visit[i][j][k] = indexTypeNotFound;
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;
    }
};

}