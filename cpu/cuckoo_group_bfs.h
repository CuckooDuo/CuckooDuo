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

/* entry parameters */
#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

/* table parameters */
#define N 8                 // item(cell) number in a bucket
#define L 8
#define SIG_LEN 2           // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 100      // size of TCAM
#define TABLE1 0            // index of table1
#define TABLE2 1            // index of table2

struct Bucket{
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
public:
    bool insert(const Entry& entry) {
        int fp = calculate_fp(entry.key);
        int h[2];
        h[TABLE1] = hash1(entry.key);
        char sig[SIG_LEN];
        fp_to_sig(sig, fp);
        h[TABLE2] = hash_alt(h[TABLE1], TABLE1, sig);

        if(check_in_table1(h[TABLE1], sig) != -1 || check_in_table2(h[TABLE2], sig) != -1) { //collision happen
            collision_num++;
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }

        if(table[TABLE1].cell[h[TABLE1]] < N && table[TABLE1].cell[h[TABLE1]] <= table[TABLE2].cell[h[TABLE2]]) {  //if h1 has more empty cell, insert sig into table 1
            memcpy(table[TABLE1].bucket[h[TABLE1]].sig[table[TABLE1].cell[h[TABLE1]]], sig, SIG_LEN*sizeof(char));
            /* RDMA write: Entry to table[TABLE1], bucket h1, cell table[TABLE1].cell[h1]*/
            table[TABLE1].bucket[h[TABLE1]].full[table[TABLE1].cell[h[TABLE1]]] = true;
            table[TABLE1].cell[h[TABLE1]]++;
            
            if(table[TABLE1].cell[h[TABLE1]] == N)  fullbucket_num++;
            return true;
        }
        else if(table[TABLE2].cell[h[TABLE2]] < N) {  //insert sig into table 2
            memcpy(table[TABLE2].bucket[h[TABLE2]].sig[table[TABLE2].cell[h[TABLE2]]], sig, SIG_LEN*sizeof(char));
            /* RDMA write: Entry to table[TABLE2], bucket h2, cell i*/
            table[TABLE2].bucket[h[TABLE2]].full[table[TABLE2].cell[h[TABLE2]]] = true;
            table[TABLE2].cell[h[TABLE2]]++;
            
            if(table[TABLE2].cell[h[TABLE2]] == N)  fullbucket_num++;
            return true;
        }
        else if(max_kick_number > 0) {  //kick
            kick_num++;
            //bfs
            int alt_idx;

            //first kick
            for(int i = TABLE1; i <= TABLE2; i++) { // two tables, alt table is table[1-i]
                for(int j = 0; j < N; j++) { // traverse cells: now at table[i].bucket[h[i]].sig[j]
                    alt_idx = hash_alt(h[i], i, table[i].bucket[h[i]].sig[j]);
                    
                    if(table[1-i].cell[alt_idx] < N) { //alt bucket has empty cell, look for it
                        for(int k = 0; k < N; k++) {
                            if(!table[1-i].bucket[alt_idx].full[k]) { //find that empty cell
                                //kick
                                /* move table */
                                memcpy(table[1-i].bucket[alt_idx].sig[k], table[i].bucket[h[i]].sig[j], SIG_LEN*sizeof(char));
                                memcpy(table[i].bucket[h[i]].sig[j], sig, SIG_LEN*sizeof(char));
                                /* RDMA move: twice */

                                //table status
                                table[1-i].bucket[alt_idx].full[k] = true;
                                table[1-i].cell[alt_idx]++;
                                kick_clear();

                                sum_move_num += 1;
                                return true;
                            }
                        }
                    }
                    else { //no empty cell, push into queue
                        tmp_kickparams.table = i;
                        tmp_kickparams.bucket = h[i];
                        tmp_kickparams.cell = j;
                        tmp_kickparams.alt_bucket = alt_idx;
                        tmp_stack.push(tmp_kickparams);
                        kick_queue.push(tmp_stack);
                        tmp_stack.pop(); //remain stack empty
                    }
                } // cell traverse over
            }

            if(max_kick_number <= 1) { //max kick number reached
                kick_clear();
                //insert to cam
                if(insert_to_cam(entry))
                    return true;
                return false;
            }

            //bfs kicks
            while(!kick_queue.empty()) {
                //pop queue front
                tmp_stack = kick_queue.front();

                //max kick number reached
                if(tmp_stack.size() >= max_kick_number) {
                    kick_clear();
                    //insert to cam
                    if(insert_to_cam(entry))
                        return true;
                    return false;
                }
                // get the alt bucket
                kick_queue.pop();
                tmp_kickparams = tmp_stack.top();
                int table_now = 1 - tmp_kickparams.table;
                int idx_now = tmp_kickparams.alt_bucket;

                int tmp_stack_size = tmp_stack.size();

                // traverse cells: now at table[table_now].bucket[idx_now].sig[j]
                for(int j = 0; j < N; j++)  {
                    alt_idx = hash_alt(idx_now, table_now, table[table_now].bucket[idx_now].sig[j]);
                    if(table[1-table_now].cell[alt_idx] < N) {
                        for(int k = 0; k < N; k++) { //look for the empty cell
                            if(!table[1-table_now].bucket[alt_idx].full[k]) { //find that empty cell
                                //kick

                                /* move table */
                                //alt(now) to alt of alt(1-now)
                                memcpy(table[1-table_now].bucket[alt_idx].sig[k], table[table_now].bucket[idx_now].sig[j], SIG_LEN*sizeof(char));
                                /* RDMA move: now to 1-now */

                                kick_params dst, src;
                                src = tmp_kickparams;
                                //move: stack top(src) to alt(now)
                                memcpy(table[table_now].bucket[idx_now].sig[j], table[src.table].bucket[src.bucket].sig[src.cell], SIG_LEN*sizeof(char));
                                /* RDMA move: src to now */

                                //move in stack
                                while(!tmp_stack.empty()) {
                                    dst = src;
                                    src = tmp_stack.top();
                                    tmp_stack.pop();
                                    /* move table */
                                    memcpy(table[dst.table].bucket[dst.bucket].sig[dst.cell], table[src.table].bucket[src.bucket].sig[src.cell], SIG_LEN*sizeof(char));
                                    /* RDMA move: src to dst */
                                }

                                /* the last move: move the entry into src(the actual dst, last params in the stack.)*/
                                memcpy(table[src.table].bucket[src.bucket].sig[src.cell], sig, SIG_LEN*sizeof(char));
                                /* RDMA move: Entry to src */

                                //table status
                                table[1-table_now].bucket[alt_idx].full[k] = true;
                                table[1-table_now].cell[alt_idx]++;
                                kick_clear(); //clear the queue and the stack

                                sum_move_num += tmp_stack_size+1;
                                //printf("--------------------------------------sum_move_num--------------------------------------: %d \n", tmp_stack_size+1);
                                return true;
                            }
                        }
                    }
                    else { //no empty cell, push into queue
                        tmp_kickparams.table = table_now;
                        tmp_kickparams.bucket = idx_now;
                        tmp_kickparams.cell = j;
                        tmp_kickparams.alt_bucket = alt_idx;
                        tmp_stack.push(tmp_kickparams);
                        kick_queue.push(tmp_stack);
                    }
                }
            }
        }

        kick_clear();
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

        collision_num = 0;
        kick_num = 0;
        fullbucket_num = 0;
        tcam_num = 0;
        kick_success_num = 0;
        sum_move_num = 0;
        this->kick_stat = new uint32_t[max_kick_num+1];
        memset(kick_stat, 0, (max_kick_num+1)*sizeof(uint32_t));
    }

    ~CuckooHashTable() {
        delete [] table[TABLE1].cell;
        delete [] table[TABLE1].bucket;
        delete [] table[TABLE2].cell;
        delete [] table[TABLE2].bucket;
    }
};