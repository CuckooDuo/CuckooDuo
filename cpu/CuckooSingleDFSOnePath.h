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

namespace SCK {
/* entry parameters */
#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
Entry emptyEntry;
// Entry RDMAmap[2][1876000][8];

/* table parameters */
#define N MY_BUCKET_SIZE    // item(cell) number in a bucket
#define SIG_LEN 2           // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 64      // size of TCAM
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2

struct Bucket{
    char key[N][KEY_LEN];
    char val[N][VAL_LEN];
    char sig[N][SIG_LEN];   //fingerprints are put in here
    bool full[N];           //whether the cell is full or not
};

struct Table
{
    Bucket *bucket;
    uint32_t *cell;        // occupied cell number of each bucket
};

class CuckooHashTable{
public:

    int move_num, max_move_num, sum_move_num; 
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; 
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; 

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

    uint32_t seed_hash_to_fp;
    uint32_t seed_hash_to_bucket;
    uint32_t seed_hash_to_alt;

    //test data
    int kick_num;
    int kick_success_num;
    int collision_num;
    int fullbucket_num;
    int tcam_num;
    uint32_t *kick_stat;

    // std::unordered_map<indexType, Entry, pair_hash> RDMAmap;  // simulate RDMA
    // Entry ***RDMAmap;
    // Entry RDMAmap[2][1876000][8];

    // simulate RDMA, write entry at (table,bucket,cell)
    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry){
        move_num ++;
        // RDMAmap[std::make_pair(std::make_pair(table, bucket), cell)] = entry;
        memcpy(table[tableID].bucket[bucketID].key[cellID], entry.key, KEY_LEN*sizeof(char));
        memcpy(table[tableID].bucket[bucketID].val[cellID], entry.val, VAL_LEN*sizeof(char));
    }
    inline void RDMA_write(Bucket *current_bucket, int cellID, Entry entry){
        move_num ++;
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
        seed_hash_to_bucket = seed;
        seed = rand()%MAX_PRIME32;
        while(seeds.find(seed) != seeds.end())
            seed = rand()%MAX_PRIME32;
        seed_hash_to_alt = seed;
    }

    int calculate_fp(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_fp) % (1 << (SIG_LEN * 8));
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
        int hfp = hash_fp(fp);
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
    int check_in_table1(int b, char *sig) {
        for(int i = 0; i < table[TABLE1].cell[b]; i++) {
            if(memcmp(sig, table[TABLE1].bucket[b].sig[i], SIG_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
    }

    // given a sig, check whether table[TABLE2] has sig in bucket b, return cell number if true, -1 if false
    int check_in_table2(int b, char *sig) {
        for(int i = 0; i < table[TABLE2].cell[b]; i++) {
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
    
    bool dfs(int tableID, int bucketID, int cellID, int depth = 0){
        if (depth > max_kick_number) return false;
        if (!table[tableID].bucket[bucketID].full[cellID]){
            // kick
            table[tableID].bucket[bucketID].full[cellID] = 1;
            table[tableID].cell[bucketID]++;
            return true;
        }
        int nextTableID = 1 - tableID;
        int nextBucketID = hash_alt(bucketID, tableID, table[tableID].bucket[bucketID].sig[cellID]);
        int i = rand()%N;
        if (dfs(nextTableID, nextBucketID, i, depth+1)){
            // kick
            Entry entry;
            RDMA_read(entry, tableID, bucketID, cellID);
            RDMA_write(nextTableID, nextBucketID, i, entry);
            memcpy(table[nextTableID].bucket[nextBucketID].sig[i],
                table[tableID].bucket[bucketID].sig[cellID],
                SIG_LEN*sizeof(char));
            return true;
        }
        return false;
    }
public:
    bool insert(const Entry& entry) {
        max_move_num = std::max(max_move_num, move_num);
        sum_move_num += move_num;
        max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
        sum_RDMA_read_num += RDMA_read_num;
        max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
        sum_RDMA_read_num2 += RDMA_read_num2;
        move_num = 0;
        RDMA_read_num = 0;
        RDMA_read_num2 = 0;
        int fp = calculate_fp(entry.key);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, sig);

        if(check_in_table1(h[TABLE1], sig) != -1 || check_in_table2(h[TABLE2], sig) != -1) { //collision happen
            // collision_num++;
            tcam_num--;
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }
        int randomTableID = rand()%2;
        int randomCellID = rand()%N;
        if (dfs(randomTableID, h[randomTableID], randomCellID)){
            // kick
            RDMA_write(randomTableID, h[randomTableID], randomCellID, entry);
            memcpy(table[randomTableID].bucket[h[randomTableID]].sig[randomCellID],
                sig,
                SIG_LEN*sizeof(char));
            return true;
        }
        
        collision_num++;
        if(insert_to_cam(entry)) 
            return true;
        return false;
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
        int fp = calculate_fp(key);
        int h1 = hash1(key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig);
        if(cell != -1) {
            /* RDMA read: table[TABLE1], bucket h1, cell cell to val*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, sig);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            /* RDMA read: table[TABLE2], bucket h2, cell cell to val*/
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
        int h1 = hash1(key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig);
        if(cell != -1) {
            memset(table[TABLE1].bucket[h1].sig[cell], 0, SIG_LEN);
            table[TABLE1].bucket[h1].full[cell] = 0;
            /* RDMA write: 0 to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, sig);
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
        int h1 = hash1(entry.key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        int cell;

        //query in table[TABLE1]
        cell = check_in_table1(h1, sig);
        if(cell != -1) {
            /* RDMA write: entry.val to table[TABLE1], bucket h1, cell cell*/
            return true;
        }

        //query in table[TABLE2]
        int h2 = hash_alt(h1, TABLE1, sig);
        cell = check_in_table2(h2, sig);
        if(cell != -1) {
            /* RDMA write: entry.val to table[TABLE2], bucket h2, cell cell*/
            return true;
        }

        //miss
        return false;
    }

    CuckooHashTable(int cell_number, int max_kick_num) {
        this->bucket_number = cell_number/N/2;
        this->max_kick_number = max_kick_num;
        this->table[TABLE1].bucket = new Bucket[this->bucket_number];
        memset(table[TABLE1].bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table[TABLE2].bucket = new Bucket[this->bucket_number];
        memset(table[TABLE2].bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table[TABLE1].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE1].cell, 0, this->bucket_number*sizeof(uint32_t));
        this->table[TABLE2].cell = new uint32_t[this->bucket_number];
        memset(table[TABLE2].cell, 0, this->bucket_number*sizeof(uint32_t));

        initialize_hash_functions();

        // this->RDMAmap = new Entry[2][this->bucket_number][N];
        // memset(RDMAmap, 0, (long long)2*this->bucket_number*N*sizeof(Entry));
        // this->RDMAmap = new Entry**[2];
        // for (int i=0; i<2; i++){
        //     this->RDMAmap[i] = new Entry*[this->bucket_number];
        //     for (int j=0; j<this->bucket_number; j++){
        //         this->RDMAmap[i][j] = new Entry[N];
        //         memset(this->RDMAmap[i][j], 0, N*sizeof(Entry));
        //     }
        // }
        // memset(RDMAmap, 0, sizeof(RDMAmap));

        collision_num = 0;
        kick_num = 0;
        fullbucket_num = 0;
        tcam_num = 0;
        kick_success_num = 0;
        move_num = 0;
        this->kick_stat = new uint32_t[max_kick_num+1];
        memset(kick_stat, 0, (max_kick_num+1)*sizeof(uint32_t));

        move_num = max_move_num = sum_move_num = 0;
        RDMA_read_num = max_RDMA_read_num = sum_RDMA_read_num = 0;
        RDMA_read_num2 = max_RDMA_read_num2 = sum_RDMA_read_num2 = 0;
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;
    }
};

}