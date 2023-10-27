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
#define N 8 // item(cell) number in a bucket (TABLE1)
#define M 6 // item(cell) number in a bucket using hash1 (N-M for hash2) (TABLE1)
#define L 8 // item(cell) number in a bucket (TABLE2)
#define maxNL 8 // max(N, L)
#define SIG_LEN 2 // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 100      // size of TCAM
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2
#define rebuildError -1
#define rebuildOK 1
#define rebuildNeedkick 0
#define rebuildNeedTwokick 2

struct Bucket{
    char key[maxNL][KEY_LEN];
    char val[maxNL][KEY_LEN];
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
    int kick_num; //插入一个key时，如果需要kick则加1，kick时再需要kick不会加1
    int kick_success_num;
    int collision_num;
    int fullbucket_num;
    int tcam_num;
    int sum_move_num; //数据搬移量
    uint32_t *kick_stat;

private:
    void initialize_hash_functions(){
        std::set<int> seeds;
        // srand((unsigned)time(NULL));
        srand(0);

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
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry){
        // RDMAmap[std::make_pair(std::make_pair(table, bucket), cell)] = entry;
        memcpy(table[tableID].bucket[bucketID].key[cellID], entry.key, KEY_LEN*sizeof(char));
        memcpy(table[tableID].bucket[bucketID].val[cellID], entry.val, VAL_LEN*sizeof(char));
    }
    inline void RDMA_write(Bucket *current_bucket, int cellID, Entry entry){
        // RDMAmap[std::make_pair(std::make_pair(table, bucket), cell)] = entry;
        memcpy(current_bucket->key[cellID], entry.key, KEY_LEN*sizeof(char));
        memcpy(current_bucket->val[cellID], entry.val, VAL_LEN*sizeof(char));
    }

    // simulate RDMA, read entry from (table,bucket,cell)
    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID){
        memcpy(entry.key, table[tableID].bucket[bucketID].key[cellID], KEY_LEN*sizeof(char));
        memcpy(entry.val, table[tableID].bucket[bucketID].val[cellID], VAL_LEN*sizeof(char));
    }
    inline void RDMA_read(Entry &entry, Bucket *current_bucket, int cellID){
        memcpy(entry.key, current_bucket->key[cellID], KEY_LEN*sizeof(char));
        memcpy(entry.val, current_bucket->val[cellID], VAL_LEN*sizeof(char));
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

        Entry cellEntry[N+2];
        int cellfp1[N+2];
        int cellfp2[N+2];
        int cellcnt = 0;
        Bucket *current_bucket = &table[TABLE1].bucket[h[TABLE1]];
        for (int i=0; i<N; i++){
            if (current_bucket->full[i]){
                // cellEntry[cellcnt] = RDMA_read(TABLE1, h[TABLE1], i);
                RDMA_read(cellEntry[cellcnt], current_bucket, i);
                // cellfp1[cellcnt] = calculate_fp(cellEntry[cellcnt].key, 1);
                // cellfp2[cellcnt] = calculate_fp(cellEntry[cellcnt].key, 2);
                calculate_two_fp(cellEntry[cellcnt].key, cellfp1[cellcnt], cellfp2[cellcnt]);
                cellcnt++;
            }
        }

        // balance insertion
        int noHash1CollisionFlag = balanceFlag;
        for (int i=0; i<cellcnt; i++){
            if (cellfp1[i] == fp) noHash1CollisionFlag = 0;
        }
        if (noHash1CollisionFlag && (hash1coll == -1) && (0 < (L-(int)table[TABLE2].cell[h[TABLE2]]))//){
        && ((N-(int)table[TABLE1].cell[h[TABLE1]]) <= (L-(int)table[TABLE2].cell[h[TABLE2]]))){
            Bucket *current_bucket = &table[TABLE2].bucket[h[TABLE2]];
            int targetCell = 0;
            while (current_bucket->full[targetCell]) ++targetCell;
            memcpy(current_bucket->sig[targetCell], sig, SIG_LEN*sizeof(char));
            current_bucket->full[targetCell] = 1;
            RDMA_write(current_bucket, targetCell, entry);
            table[TABLE2].cell[h[TABLE2]]++;
            return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
        }
        
        // int hash1coll = check_in_table2(h[TABLE2], sig);
        if (hash1coll != -1){
            // cellEntry[cellcnt] = RDMA_read(TABLE2, h[TABLE2], hash1coll);
            RDMA_read(cellEntry[cellcnt], TABLE2, h[TABLE2], hash1coll);
            // cellfp1[cellcnt] = calculate_fp(cellEntry[cellcnt].key, 1);
            // cellfp2[cellcnt] = calculate_fp(cellEntry[cellcnt].key, 2);
            calculate_two_fp(cellEntry[cellcnt].key, cellfp1[cellcnt], cellfp2[cellcnt]);
            cellcnt++;
        }

        cellEntry[cellcnt] = entry;
        cellfp1[cellcnt] = fp;
        cellfp2[cellcnt] = fp2;
        cellcnt++;

        int fp1group[N+2], fp1gpcnt = 0;
        int fp2group[N+2], fp2gpcnt = 0;
        for (int i=0; i<cellcnt; i++){
            fp1group[i] = fp2group[i] = -1;
            for (int j=0; j<i; j++){
                if (cellfp1[j] == cellfp1[i]){
                    if (fp1group[j] == -1)
                        fp1group[j] = ++fp1gpcnt;
                    fp1group[i] = fp1group[j];
                }
                if (cellfp2[j] == cellfp2[i]){
                    if (fp2group[j] == -1)
                        fp2group[j] = ++fp2gpcnt;
                    fp2group[i] = fp2group[j];
                }
            }
        }

        int newcell[N+2], h1cnt = 0, h2cnt = 0, kickid = -1, kickid2 = -1;
        int id[N+2], pos[N+2], group_evict[N+2];
        memset(id, -1, sizeof(id));
        memset(pos, -1, sizeof(pos));
        memset(group_evict, -1, sizeof(group_evict));

        for (int i=1; i<=fp1gpcnt; i++){
            bool useh1 = 0;
            for (int j=0; j<cellcnt; j++) if (fp1group[j] == i){
                if (fp2group[j] != -1){
                    useh1 = 1;
                    id[h1cnt] = j;
                    pos[j] = h1cnt;
                    h1cnt++;
                    group_evict[i] = -2;
                } else {
                    if (M + h2cnt >= N){// hash2 cells full, try evict
                        bool find_evict = 0;
                        for (int k=1; k<i; k++) if (group_evict[k] != -2){
                            find_evict = 1;
                            int evict_j = group_evict[k];
                            int evict_pos = pos[evict_j];
                            id[evict_pos] = j;
                            pos[j] = evict_pos;
                            id[h1cnt] = evict_j;
                            pos[evict_j] = h1cnt;
                            h1cnt++;
                            group_evict[k] = -2;
                            if (group_evict[i] == -1)
                                group_evict[i] = j;
                            break;
                        }
                        if (!find_evict){// hash2 cells exhausted
                            useh1 = 1;
                            id[h1cnt] = j;
                            pos[j] = h1cnt;
                            h1cnt++;
                            group_evict[i] = -2;
                        }
                    } else {
                        id[M+h2cnt] = j;
                        pos[j] = M+h2cnt;
                        h2cnt++;
                        if (group_evict[i] == -1)
                            group_evict[i] = j;
                    }
                }
                
            }
        }

        if (hash1coll != -1){
            table[TABLE2].cell[h[TABLE2]]--;
            table[TABLE2].bucket[h[TABLE2]].full[hash1coll] = 0;
            table[TABLE2].bucket[h[TABLE2]].fixed[hash1coll] = 0;
        }

        for (int i=0; i<cellcnt; i++) if (pos[i] == -1){
            char newsig[SIG_LEN];
            fp_to_sig(newsig, cellfp1[i]);
            if (fp2group[i] == -1 && M + h2cnt < N){
                // insert to hash2
                id[M+h2cnt] = i;
                pos[i] = M+h2cnt;
                h2cnt++;
            } else if (h1cnt < M){
                // insert to hash1
                id[h1cnt] = i;
                pos[i] = h1cnt;
                h1cnt++;
            } else {
                // kick
                if (kickid != -1){
                    kickid2 = i;
                } else kickid = i;
            }
        }
        for (int i=0; i<N; i++){
            if (i < h1cnt){
                char cur_sig[SIG_LEN];
                fp_to_sig(cur_sig, cellfp1[id[i]]);
                current_bucket->full[i] = 1;
                memcpy(current_bucket->sig[i], cur_sig, SIG_LEN*sizeof(char));
                RDMA_write(current_bucket, i, cellEntry[id[i]]);
            } else if (i >= M && i < M+h2cnt){
                char cur_sig[SIG_LEN];
                fp_to_sig(cur_sig, cellfp2[id[i]]);
                current_bucket->full[i] = 1;
                memcpy(current_bucket->sig[i], cur_sig, SIG_LEN*sizeof(char));
                RDMA_write(current_bucket, i, cellEntry[id[i]]);
            } else current_bucket->full[i] = 0;
        }
        memset(current_bucket->fixed, 0, sizeof(current_bucket->fixed));
        for (int i=0; i<cellcnt; i++){
            if (pos[i] != -1 && fp1group[i] != -1)
                current_bucket->fixed[pos[i]] = 1;
        }
        table[TABLE1].cell[h[TABLE1]] = h1cnt + h2cnt;
        if (kickid == -1) return std::make_pair(rebuildOK, std::make_pair(emptyEntry, emptyEntry));
        if (kickid2 == -1) return std::make_pair(rebuildNeedkick, std::make_pair(cellEntry[kickid], emptyEntry));
        return std::make_pair(rebuildNeedTwokick, std::make_pair(cellEntry[kickid], cellEntry[kickid2]));
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
                for (int i=0; i<listCnt; i++){
                    if (i+1<listCnt){
                        Bucket *current_bucket = &table[indexList[i+1].first.first].bucket[indexList[i+1].first.second];
                        // entryList[i] = RDMA_read(
                        //     indexList[i+1].first.first, 
                        //     indexList[i+1].first.second, 
                        //     indexList[i+1].second
                        // );
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
                                RDMA_write(current_bucket, indexList[i].second, entryList[i]);
                            }
                        } else {
                            memcpy(current_bucket->sig[indexList[i].second], sigList[i], SIG_LEN*sizeof(char));
                            RDMA_write(current_bucket, indexList[i].second, entryList[i]);
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
                    RDMA_write(current_bucket, indexList[i].second, entryList[i]);
                }
                BFSbucket->full[cellID] = 1;
                table[tableID].cell[bucketID] += 1;
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
        rebuildInfo info = rebuild(entry);
        if (info.first == rebuildOK){
            return true;
        }
        if (info.first == rebuildError){
            collision_num++;
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }

        bool insertOK = bfs(info.second.first);
        if (!insertOK || info.first == rebuildNeedkick) return insertOK;
        insertOK = bfs(info.second.second);
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
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
            // if (memcmp(key, RDMAmap[TABLE2][h2][cell].key, KEY_LEN*sizeof(char)) == 0)
                return true;
            // else if (fp == calculate_fp(RDMAmap[TABLE2][h2][cell].key)
            //     && fp2 == calculate_fp(RDMAmap[TABLE2][h2][cell].key, 2)) return true;
            // else return false;
        }

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig, sig2);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            // if (memcmp(key, RDMAmap[TABLE1][h1][cell].key, KEY_LEN*sizeof(char)) == 0)
                return true;
            // else if (fp == calculate_fp(RDMAmap[TABLE1][h1][cell].key)
            //     && fp2 == calculate_fp(RDMAmap[TABLE1][h1][cell].key, 2)) return true;
            // else return false;
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
        sum_move_num = 0;
        this->kick_stat = new uint32_t[max_kick_num+1];
        memset(kick_stat, 0, (max_kick_num+1)*sizeof(uint32_t));
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