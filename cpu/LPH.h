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
#include "SmoothExpansionFramework.h"
#pragma once

/* entry parameters */
#define KEY_LEN 8
#define VAL_LEN 8

namespace LPH {

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};

struct Item{
    char key[KEY_LEN];   //keys are put in here
    char val[VAL_LEN];   //values are put here
    bool full;           //whether the cell is full or not
};

class LinearProbingHash{
public:
    const float kResizingThreshold = 0.95; // tablesize small will cause recursive expand error
    const int maxProb = 64;

    Item *dict;

    int item_number, total_insert;

    uint32_t seed_hash_to_fp;
    uint32_t seed_hash_to_bucket[2];

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
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket[0]) % item_number;
    }

    // return index for group 2.
    int hash2(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket[1]) % item_number;
    }

public:
    bool insert(Entry& entry) {
        if (total_insert >= kResizingThreshold * item_number) return false;
        int h1 = hash1(entry.key);
        
        int i = 0;
        while (i < maxProb){
            int index = (h1 + i) % item_number;
            if (dict[index].full == false){
                memcpy(dict[index].key, entry.key, sizeof(entry.key));
                memcpy(dict[index].val, entry.val, sizeof(entry.val));
                dict[index].full = true;
                total_insert++;
                return true;
            }
            i++;
        }
        return false;
    }

    LinearProbingHash(int cell_number) {
        item_number = cell_number;
        total_insert = 0;
        dict = new Item[item_number];
        for (int i = 0; i < item_number; i++){
            dict[i].full = false;
        }

        initialize_hash_functions();
    }

    ~LinearProbingHash() {
        delete [] dict;
    }
};
    
}



// lph_dict = Dictionary dict([]() -> BaseSegment* {
//     return new LPHSegment();
// });
class LPHSegment: public SEF::BaseSegment{
public:
SEF::Entry all_inserted_entry_list[MAX_ITEMS_PER_SEGMENT];
int total_insert_counter;
LPH::LinearProbingHash *lph;

    LPHSegment(){
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        lph = new LPH::LinearProbingHash(MAX_ITEMS_PER_SEGMENT);
    }
    ~LPHSegment(){
        delete lph;
    }

    bool insert(const SEF::Entry& entry) override {
        LPH::Entry _entry;
        memcpy(_entry.key, entry.key, sizeof(entry.key));
        memcpy(_entry.val, entry.val, sizeof(entry.val));
        all_inserted_entry_list[total_insert_counter++] = entry;
        if (total_insert_counter >= MAX_ITEMS_PER_SEGMENT) return false;
        bool result = lph->insert(_entry);
        return result;
    }

    int dump(SEF::Entry *entry) override {
        memcpy(entry, all_inserted_entry_list, sizeof(all_inserted_entry_list));
        return total_insert_counter;
    }

    void clean() override {
        memset(all_inserted_entry_list, 0, sizeof(all_inserted_entry_list));
        total_insert_counter = 0;
        delete lph;
        lph = new LPH::LinearProbingHash(MAX_ITEMS_PER_SEGMENT);
    }

    int memory() override {
        return MAX_ITEMS_PER_SEGMENT * memory_per_item();
    }

    int memory_per_item() override {
        return 64 + 32;  // key 64,  pointer 32
    }

    int current_items() override {
        return total_insert_counter;
    }
};