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
namespace TEA {

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

/* table parameters */
#define N 8                 // item(cell) number in a bucket
#define SIG_LEN 2           // sig(fingerprint) length: 16 bit
#define TCAM_SIZE 4096       // size of TCAM

struct Bucket{
    char key[N][KEY_LEN];   //fingerprints are put in here
    char val[N][VAL_LEN];   //values are put here
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
    std::unordered_map<std::string, std::string> TCAM;  //overflow cam

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

    int move_num, max_move_num, sum_move_num; //数据搬移量
    int RDMA_read_num, max_RDMA_read_num, sum_RDMA_read_num; //片外访存次数
    int RDMA_read_num2, max_RDMA_read_num2, sum_RDMA_read_num2; //片外访存数据总量

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

    // return index for table 1.
    int hash1(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket) % bucket_number;
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
        RDMA_read_num2 += b_num*N;
        // ra[0] = get_offset_table(tableID, bucketID, 0);
        // get_bucket(b_num);
        // memcpy(entry, bucket_buf, b_num*N*KV_LEN);
    }

    // given a key, check whether table has key in bucket b, return cell number if true, -1 if false
    int check_in_table(int b, const char *key) {
        for(int i = 0; i < table.cell[b]; i++) {
            if(memcmp(key, table.bucket[b].key[i], KEY_LEN*sizeof(char)) == 0) {
                return i;
            }
        }
        return -1;
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
        int tries = 0;
        Entry tmp_entry;
        memcpy(tmp_entry.key, entry.key, KEY_LEN*sizeof(char));
        memcpy(tmp_entry.val, entry.val, VAL_LEN*sizeof(char));
        Entry victim;
        while(tries < max_kick_number)
        {
            int h1 = hash1(tmp_entry.key);
            int h2 = (h1 + 1) % bucket_number; //next bucket is the alternate bucket in TEA.

            if(check_in_table(h1, tmp_entry.key) != -1 || check_in_table(h2, tmp_entry.key) != -1) { //collision happen
                collision_num++;
                if(insert_to_cam(tmp_entry)) 
                    return true;
                return false;
            }

            /* RDMA read: table, bucket h1, h2*/
            /* NOTICE: this is only for simulation*/
            // If h1 is next to h2, read once, otherwise twice
            Entry tmp_entry1[2*N];
            if (h1 < h2) {
                RDMA_read_bucket(tmp_entry1, 0, h1, 2);
            }
            else {
                RDMA_read_bucket(tmp_entry1, 0, h1, 1);
                RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
            }

            if(table.cell[h1] < N) {  //if h1 has empty cell, insert key into h1
                for(int i = 0; i < N; i++ ) {
                    if(!table.bucket[h1].full[i]) {
                        memcpy(table.bucket[h1].key[i], tmp_entry.key, KEY_LEN*sizeof(char));
                        memcpy(table.bucket[h1].val[i], tmp_entry.val, VAL_LEN*sizeof(char));
                        /* RDMA write: tmp_entry to table, bucket h1, cell i*/
                        RDMA_write(0, h1, i, tmp_entry);

                        table.bucket[h1].full[i] = true;
                        break;
                    }
                }
                table.cell[h1]++;
                if(table.cell[h1] == N)  fullbucket_num++;
                if(tries > 0) {
                    kick_num++;
                    kick_success_num++;
                    kick_stat[tries]++;
                }
                return true;
            }
            if(table.cell[h2] < N) {  //if h2 has empty cell, insert key into h2
                for(int i = 0; i < N; i++ ) {
                    if(!table.bucket[h2].full[i]) {
                        memcpy(table.bucket[h2].key[i], tmp_entry.key, KEY_LEN*sizeof(char));
                        memcpy(table.bucket[h2].val[i], tmp_entry.val, VAL_LEN*sizeof(char));
                        /* RDMA write: tmp_entry to table, bucket h2, cell i*/
                        RDMA_write(0, h2, i, tmp_entry);

                        table.bucket[h2].full[i] = true;
                        break;
                    }
                }
                table.cell[h2]++;
                if(table.cell[h2] == N)  fullbucket_num++;
                if(tries > 0) {
                    kick_num++;
                    kick_success_num++;
                    kick_stat[tries]++;
                }
                return true;
            }
            //choose a victim
            int tmptable = (rand() % 2 == 0)?h1:h2;
            int tmpcell = rand() % N;
            //victim 
            memcpy(victim.key, table.bucket[tmptable].key[tmpcell], KEY_LEN*sizeof(char));
            memcpy(victim.val, table.bucket[tmptable].val[tmpcell], VAL_LEN*sizeof(char));
            /* RDMA read: table, bucket tmptable, cell tmpcell*/
            //RDMA_read(read_buf[0], 0, tmptable, tmpcell);

            //insert tmp_entry
            memcpy(table.bucket[tmptable].key[tmpcell], tmp_entry.key, KEY_LEN*sizeof(char));
            memcpy(table.bucket[tmptable].val[tmpcell], tmp_entry.val, VAL_LEN*sizeof(char));
            /* RDMA write: tmp_entry to table, bucket tmptable, cell tmpcell*/
            RDMA_write(0, tmptable, tmpcell, tmp_entry);


            //tmp_entry = victim
            memcpy(tmp_entry.key, victim.key, KEY_LEN*sizeof(char));
            memcpy(tmp_entry.val, victim.val, VAL_LEN*sizeof(char));
        
            tries++;
        }
        //max kick reached
        kick_num++;
        if(insert_to_cam(tmp_entry)) {
            return true;
        }
        return false;
    }

    //query result put in val
    bool query(char *key, char *val = NULL) {

        //query in table
        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;
        int cell;

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_entry1[2*N];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_entry1, 0, h1, 2);
        }
        else {
            RDMA_read_bucket(tmp_entry1, 0, h1, 1);
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }

        //query h1
        cell = check_in_table(h1, key);
        if(cell != -1) {
            /* RDMA read: table, bucket h1, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h1, cell);

            /*if (memcmp(key, read_buf[0].key, KEY_LEN) == 0) {
                //memcpy(val, &read_buf[0].val, PTR_LEN);
                return true;
            }*/
            return true;
        }

        if (h1 > h2) {
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }
        //query h2
        cell = check_in_table(h2, key);
        if(cell != -1) {
            /* RDMA read: table, bucket h2, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h2, cell);

            /*if (memcmp(key, read_buf[0].key, KEY_LEN) == 0) {
                //memcpy(val, &read_buf[0].val, PTR_LEN);
                return true;
            }*/
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

        int h1 = hash1(key);
        int h2 = (h1 + 1) % bucket_number;

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_entry1[2*N];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_entry1, 0, h1, 2);
        }
        else {
            RDMA_read_bucket(tmp_entry1, 0, h1, 1);
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }

        //query in table
        int cell;
        cell = check_in_table(h1, key);
        if(cell != -1) {
            /* RDMA read: table, bucket h1, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h1, cell);

            memset(table.bucket[h1].key[cell], 0, KEY_LEN);
            table.bucket[h1].full[cell] = 0;
            /* RDMA write: 0 to table, bucket h1, cell cell*/
            RDMA_write(0, h1, cell, Entry({{0},{0}}));

            return true;
        }

        if (h1 > h2) {
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }
        //query in table
        cell = check_in_table(h2, key);
        if(cell != -1) {
            /* RDMA read: table, bucket h2, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h2, cell);

            memset(table.bucket[h2].key[cell], 0, KEY_LEN);
            table.bucket[h2].full[cell] = 0;
            /* RDMA write: 0 to table, bucket h2, cell cell*/
            RDMA_write(0, h2, cell, Entry({{0},{0}}));

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

        int h1 = hash1(entry.key);
        int h2 = (h1 + 1) % bucket_number;

        // If h1 is next to h2, read once, otherwise twice
        Entry tmp_entry1[2*N];
        if (h1 < h2) {
            RDMA_read_bucket(tmp_entry1, 0, h1, 2);
        }
        else {
            RDMA_read_bucket(tmp_entry1, 0, h1, 1);
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }

        //query in table
        int cell;
        cell = check_in_table(h1, entry.key);
        if(cell != -1) {
            /* RDMA read: table, bucket h1, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h1, cell);

            memcpy(table.bucket[h1].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h1, cell cell*/
            RDMA_write(0, h1, cell, entry);
            
            return true;
        }

        if (h1 > h2) {
            RDMA_read_bucket(tmp_entry1+N, 0, h2, 1);
        }
        //query in table
        cell = check_in_table(h2, entry.key);
        if(cell != -1) {
            /* RDMA read: table, bucket h1, cell cell to val*/
            //RDMA_read(tmp_entry, 0, h2, cell);

            memcpy(table.bucket[h2].val[cell], entry.val, VAL_LEN);
            /* RDMA write: entry.val to table, bucket h2, cell cell*/
            RDMA_write(0, h1, cell, entry);
            
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

    TEATable(int cell_number, int max_kick_num) {
        this->bucket_number = cell_number/N;
        this->max_kick_number = max_kick_num;
        this->table.bucket = new Bucket[this->bucket_number];
        memset(table.bucket, 0, this->bucket_number*sizeof(Bucket));
        this->table.cell = new uint32_t[this->bucket_number];
        memset(table.cell, 0, this->bucket_number*sizeof(uint32_t));

        initialize_hash_functions();

        collision_num = 0;
        kick_num = 0;
        fullbucket_num = 0;
        tcam_num = 0;
        kick_success_num = 0;
        this->kick_stat = new uint32_t[max_kick_num+1];
        memset(kick_stat, 0, (max_kick_num+1)*sizeof(uint32_t));
        
        move_num = max_move_num = sum_move_num = 0;
        RDMA_read_num = max_RDMA_read_num = sum_RDMA_read_num = 0;
        RDMA_read_num2 = max_RDMA_read_num2 = sum_RDMA_read_num2 = 0;
    }

    ~TEATable() {
        delete [] table.cell;
        delete [] table.bucket;
        TCAM.clear();
    }
};

}