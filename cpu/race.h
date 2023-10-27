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
// #include "../rdma/rdma_client.h"
#pragma once

/* entry parameters */
#define KEY_LEN 8
#define VAL_LEN 8

namespace RACE {

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

/* table parameters */
#define N MY_BUCKET_SIZE    // item(cell) number in a bucket
#define SIG_LEN 2           // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 64        // size of TCAM

struct Bucket{
    char key[N][KEY_LEN];   //keys are put in here
    char val[N][VAL_LEN];   //values are put here
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
    std::unordered_map<std::string, std::string> TCAM;  //overflow cam

    uint32_t seed_hash_to_fp;
    uint32_t seed_hash_to_bucket[2];

    //test data
    int collision_num;
    int fullbucket_num;
    int tcam_num;
    
    int move_num, max_move_num, sum_move_num; 
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; 
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; 

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

    // insert an full entry to TCAM
    bool insert_to_cam(const Entry& entry) {
        if(tcam_num >= TCAM_SIZE)
            return false;
        std::string skey(entry.key, KEY_LEN);
        std::string sval(entry.val, VAL_LEN);
        TCAM.emplace(skey, sval);
        tcam_num++;
        return true;
    }

    inline void RDMA_write(int tableID, int bucketID, int cellID, Entry entry){
        //memcpy(table[tableID].bucket[bucketID].key[cellID], entry.key, KEY_LEN*sizeof(char));
        //memcpy(table[tableID].bucket[bucketID].val[cellID], entry.val, VAL_LEN*sizeof(char));
        move_num++;
        // ra[0] = get_offset_table(tableID, bucketID, cellID);
        // memcpy(&write_buf[0], &entry, KV_LEN);
        // mod_remote(1);
    }

    inline void RDMA_read(Entry &entry, int tableID, int bucketID, int cellID){
        //memcpy(entry.key, table[tableID].bucket[bucketID].key[cellID], KEY_LEN*sizeof(char));
        //memcpy(entry.val, table[tableID].bucket[bucketID].val[cellID], VAL_LEN*sizeof(char));
        RDMA_read_num++;
        RDMA_read_num2++;
        // ra[0] = get_offset_table(tableID, bucketID, cellID);
        // get_remote(1);
        // memcpy(&entry, &read_buf[0], KV_LEN);
    }

    inline void RDMA_read_bucket(Entry *entry, int tableID, int bucketID, int b_num){
        //for (int i=0; i<8; i++){
        //    memcpy(entry[i].key, table[tableID].bucket[bucketID].key[i], KEY_LEN*sizeof(char));
        //    memcpy(entry[i].val, table[tableID].bucket[bucketID].val[i], VAL_LEN*sizeof(char));
        //}
        RDMA_read_num++;
        RDMA_read_num2+=b_num*N;
        // ra[0] = get_offset_table(tableID, bucketID, 0);
        // get_bucket(b_num);
        // memcpy(entry, bucket_buf, N*b_num*KV_LEN);
    }

    /* given a key, check whether table has key in group g, 
     * return cell number if in main bucket, 
     * cell number plus depth if in overflow bucket,
     * -1 if false */
    int check_in_table(int g, char *key) {
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
    }

public:
    bool insert(Entry& entry) {
        max_move_num = std::max(max_move_num, move_num);
        sum_move_num += move_num;
        max_RDMA_read_num = std::max(max_RDMA_read_num, RDMA_read_num);
        sum_RDMA_read_num += RDMA_read_num;
        max_RDMA_read_num2 = std::max(max_RDMA_read_num2, RDMA_read_num2);
        sum_RDMA_read_num2 += RDMA_read_num2;
        move_num = 0;
        RDMA_read_num = 0;
        RDMA_read_num2 = 0;
        //calculate group and bucket
        int h1 = hash1(entry.key);
        int h2 = hash2(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        //check collision
        if(check_in_table(h1, entry.key) != -1 || check_in_table(h2, entry.key) != -1) { 
            //collision happen
            collision_num++;
            if(insert_to_cam(entry)) 
                return true;
            return false;
        }

        /* RDMA read: table, bucket h1_idx1, h1_idx2, h2_idx1, h2_idx2*/
        /* NOTICE: this is only for simulation*/
        Entry tmp_entry1[2*N];
        Entry tmp_entry2[2*N];
        // Two buckets need read once
        RDMA_read_bucket(tmp_entry1, 0, std::min(h1_idx1, h1_idx2), 2);
        // So we need read twice totally here
        RDMA_read_bucket(tmp_entry2, 0, std::min(h2_idx1, h2_idx1), 2);

        //if h1 has more empty cell, insert key into group 1
        if((table.cell[h1_idx1]+table.cell[h1_idx2] < 2*N) && (table.cell[h1_idx1]+table.cell[h1_idx2] <= table.cell[h2_idx1]+table.cell[h2_idx2])) {
            //try to insert into main bucket first
            if(table.cell[h1_idx1] < N) {
                memcpy(table.bucket[h1_idx1].key[table.cell[h1_idx1]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx1, cell table.cell[h1_idx1]*/
                RDMA_write(0, h1_idx1, table.cell[h1_idx1], entry);
                
                table.cell[h1_idx1]++;
                if(table.cell[h1_idx1] == N)  fullbucket_num++;
                return true;
            }
            //then the overflow bucket
            if(table.cell[h1_idx2] < N) {
                memcpy(table.bucket[h1_idx2].key[table.cell[h1_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h1_idx2, cell table.cell[h1_idx2]*/
                RDMA_write(0, h1_idx2, table.cell[h1_idx2], entry);
                
                table.cell[h1_idx2]++;
                if(table.cell[h1_idx2] == N)  fullbucket_num++;
                return true;
            }
            //error
            return false;
        }
        //if h2 has empty cell, insert key into group 2
        else if(table.cell[h2_idx1]+table.cell[h2_idx2] < 2*N) {
            //try to insert into main bucket first
            if(table.cell[h2_idx1] < N) {
                memcpy(table.bucket[h2_idx1].key[table.cell[h2_idx1]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx1, cell table.cell[h2_idx1]*/
                RDMA_write(0, h2_idx1, table.cell[h2_idx1], entry);

                table.cell[h2_idx1]++;
                if(table.cell[h2_idx1] == N)  fullbucket_num++;
                return true;
            }
            //then the overflow bucket
            if(table.cell[h2_idx2] < N) {
                memcpy(table.bucket[h2_idx2].key[table.cell[h2_idx2]], entry.key, KEY_LEN*sizeof(char));
                /* RDMA write: Entry to table, bucket h2_idx2, cell table.cell[h2_idx2]*/
                RDMA_write(0, h2_idx2, table.cell[h2_idx2], entry);

                table.cell[h2_idx2]++;
                if(table.cell[h2_idx2] == N)  fullbucket_num++;
                return true;
            }
            //error
            return false;
        }

        //both group full, insert into TCAM
        if(insert_to_cam(entry)) {
            return true;
        }
        return false;
    }

    //query result put in val
    bool query(char *key, char *val = NULL) {

        Entry tmp_entry1[2*N];
        Entry tmp_entry2[2*N];

        //query h1
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        // Two buckets need read once
        RDMA_read_bucket(tmp_entry1, 0, std::min(h1_idx1, h1_idx2), 2);

        int cell;
        cell = check_in_table(h1, key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h1_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h1_idx1, cell);

                /*if (memcmp(key, table.bucket[h1_idx1].key[cell], KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            else {
                /* RDMA read: table, bucket h1_idx2, cell cell-N to val*/
                //RDMA_read(tmp_entry, 0, h1_idx2, cell-N);

                /*if (memcmp(key, table.bucket[h1_idx1].key[cell-N], KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            return true;
        }

        //query h2
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        // Two buckets need read once
        RDMA_read_bucket(tmp_entry2, 0, std::min(h2_idx1, h2_idx1), 2);

        cell = check_in_table(h2, key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h2_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h2_idx1, cell);

                /*if (memcmp(key, table.bucket[h2_idx1].key[cell], KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            else {
                /* RDMA read: table, bucket h2_idx2, cell cell-N to val*/
                //RDMA_read(tmp_entry, 0, h2_idx2, cell-N);

                /*if (memcmp(key, table.bucket[h2_idx1].key[cell-N], KEY_LEN) == 0) {
                    //memcpy(val, &read_buf[0].val, PTR_LEN);
                    return true;
                }*/
            }
            return true;
        }

        // query in TCAM
        std::string skey(key, KEY_LEN);
        auto entry = TCAM.find(skey);
        if(entry != TCAM.end()) {
            std::string sval = entry->second;
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);
            return true;
        }

        //miss
        return false;
    }

    //delete an entry given a key
    bool deletion(char* key) {

        Entry tmp_entry1[2*N];
        Entry tmp_entry2[2*N];

        //query in table
        int h1 = hash1(key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        // Two buckets need read once
        RDMA_read_bucket(tmp_entry1, 0, std::min(h1_idx1, h1_idx2), 2);

        int cell;
        cell = check_in_table(h1, key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h1_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h1_idx1, cell);

                memset(table.bucket[h1_idx1].key[cell], 0, KEY_LEN);
                table.bucket[h1_idx1].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h1_idx1, cell cell*/
                RDMA_write(0, h1_idx1, cell, Entry({{0},{0}}));
            }
            else {
                /* RDMA read: table, bucket h1_idx2, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h1_idx2, cell);

                memset(table.bucket[h1_idx2].key[cell-N], 0, KEY_LEN);
                table.bucket[h1_idx2].full[cell-N] = 0;
                /* RDMA write: 0 to table, bucket h1_idx2, cell cell-N*/
                RDMA_write(0, h1_idx2, cell-N, Entry({{0},{0}}));
            }
            return true;
        }

        //query in table
        int h2 = hash2(key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);
        
        // Two buckets need read once
        RDMA_read_bucket(tmp_entry2, 0, std::min(h2_idx1, h2_idx1), 2);

        cell = check_in_table(h2, key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h2_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h2_idx1, cell);

                memset(table.bucket[h2_idx1].key[cell], 0, KEY_LEN);
                table.bucket[h2_idx1].full[cell] = 0;
                /* RDMA write: 0 to table, bucket h2_idx1, cell cell*/
                RDMA_write(0, h2_idx1, cell, Entry({{0},{0}}));
            }
            else {
                /* RDMA read: table, bucket h2_idx2, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h2_idx2, cell);

                memset(table.bucket[h2_idx2].key[cell-N], 0, KEY_LEN);
                table.bucket[h2_idx2].full[cell-N] = 0;
                /* RDMA write: 0 to table, bucket h2_idx2, cell cell-N*/
                RDMA_write(0, h2_idx2, cell-N, Entry({{0},{0}}));
            }
            return true;
        }

        //query in TCAM, if find then delete
        std::string skey(key, KEY_LEN);
        auto entry = TCAM.find(skey);
        if(entry != TCAM.end()) {
            TCAM.erase(entry);
            tcam_num--;
            return true;
        }

        //miss
        return false;
    }

    //update an entry
    bool update(Entry& entry) {

        Entry tmp_entry1[2*N];
        Entry tmp_entry2[2*N];

        //query in table
        int h1 = hash1(entry.key);
        int h1_idx1 = (h1%2==0) ? (h1/2*3) : ((h1+1)/2*3-1);
        int h1_idx2 = (h1%2==0) ? (h1_idx1+1) : (h1_idx1-1);

        // Two buckets need read once
        RDMA_read_bucket(tmp_entry1, 0, std::min(h1_idx1, h1_idx2), 2);

        int cell;
        cell = check_in_table(h1, entry.key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h1_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h1_idx1, cell);

                memcpy(table.bucket[h1_idx1].val[cell], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h1_idx1, cell cell*/
                RDMA_write(0, h1_idx1, cell, entry);
            }
            else {
                /* RDMA read: table, bucket h1_idx2, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h1_idx2, cell);

                memcpy(table.bucket[h1_idx2].val[cell-N], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h1_idx2, cell cell-N*/
                RDMA_write(0, h1_idx2, cell-N, entry);
            }
            return true;
        }

        //query in table
        int h2 = hash2(entry.key);
        int h2_idx1 = (h2%2==0) ? (h2/2*3) : ((h2+1)/2*3-1);
        int h2_idx2 = (h2%2==0) ? (h2_idx1+1) : (h2_idx1-1);

        // Two buckets need read once
        RDMA_read_bucket(tmp_entry2, 0, std::min(h2_idx1, h2_idx1), 2);

        cell = check_in_table(h2, entry.key);
        if(cell != -1) {
            if(cell < N) {
                /* RDMA read: table, bucket h2_idx1, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h2_idx1, cell);

                memcpy(table.bucket[h2_idx1].val[cell], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h2_idx1, cell cell*/
                RDMA_write(0, h2_idx1, cell, entry);
            }
            else {
                /* RDMA read: table, bucket h2_idx2, cell cell to val*/
                //RDMA_read(tmp_entry, 0, h2_idx2, cell);

                memcpy(table.bucket[h2_idx2].val[cell-N], entry.val, VAL_LEN);
                /* RDMA write: entry.val to table, bucket h2_idx2, cell cell-N*/
                RDMA_write(0, h2_idx2, cell-N, entry);
            }
            return true;
        }

        // query in TCAM
        std::string skey(entry.key, KEY_LEN);
        auto it = TCAM.find(skey);
        if(it != TCAM.end()) {
            std::string sval = entry.val;
            it->second = sval;
            return true;
        }

        //miss
        return false;
    }

    RACETable(int cell_number) {
        this->bucket_number = cell_number/N;
        this->group_number = this->bucket_number/3*2;
        this->table.bucket = new Bucket[this->bucket_number];
        memset(table.bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table.cell = new uint32_t[this->bucket_number];
        memset(table.cell, 0, this->bucket_number*sizeof(uint32_t));

        initialize_hash_functions();

        collision_num = 0;
        fullbucket_num = 0;
        tcam_num = 0;
        
        move_num = max_move_num = sum_move_num = 0;
        RDMA_read_num = max_RDMA_read_num = sum_RDMA_read_num = 0;
        RDMA_read_num2 = max_RDMA_read_num2 = sum_RDMA_read_num2 = 0;
    }

    ~RACETable() {
        delete [] table.cell;
        delete [] table.bucket;
        TCAM.clear();
    }
};
    
}