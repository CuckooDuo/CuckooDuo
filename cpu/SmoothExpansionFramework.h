/*
    Smooth expansion framework
*/

#pragma once
#include <iostream>
#include <cmath>
#include <cstring>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <assert.h>
#include "murmur3.h"
#include <functional>
#include <memory>

// config

#ifdef MAX_ITEMS_PER_SEGMENT

#if MAX_ITEMS_PER_SEGMENT == 300
#define LARGE_ENOUGH_DICTIONARY_SIZE 300000
#define MAX_ITEMS_PER_SEGMENT_LOG 9

#elif MAX_ITEMS_PER_SEGMENT == 3000
#define LARGE_ENOUGH_DICTIONARY_SIZE 35000
#define MAX_ITEMS_PER_SEGMENT_LOG 12

#elif MAX_ITEMS_PER_SEGMENT == 30000
#define LARGE_ENOUGH_DICTIONARY_SIZE 2500
#define MAX_ITEMS_PER_SEGMENT_LOG 15
#endif

#else


// #define LARGE_ENOUGH_DICTIONARY_SIZE 2500
// #define MAX_ITEMS_PER_SEGMENT 30000
// #define MAX_ITEMS_PER_SEGMENT_LOG 15
// #define LARGE_ENOUGH_DICTIONARY_SIZE 35000
// #define MAX_ITEMS_PER_SEGMENT 3000
// #define MAX_ITEMS_PER_SEGMENT_LOG 12
#define LARGE_ENOUGH_DICTIONARY_SIZE 300000
#define MAX_ITEMS_PER_SEGMENT 300
#define MAX_ITEMS_PER_SEGMENT_LOG 9

#endif

#define MAX_INSERT_DEPTH 5

namespace SEF{

#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};


class BaseSegment{
public:
    BaseSegment(){}
    virtual ~BaseSegment(){}

    virtual bool insert(const Entry& entry) {
        printf("Call BaseSegment Error\n");
        return false;
    }

    virtual int dump(Entry *entry) {
        printf("Call BaseSegment Error\n");
        return -1;
    }

    virtual void clean() {
        printf("Call BaseSegment Error\n");
    }

    virtual int memory(){
        printf("Call BaseSegment Error\n");
        return -1;
    }

    virtual int memory_per_item(){
        printf("Call BaseSegment Error\n");
        return -1;
    }

    virtual int current_items(){
        printf("Call BaseSegment Error\n");
        return -1;
    }
};

class Dictionary{
public:
    std::function<BaseSegment*()> segment_factory_function;
    BaseSegment *segment_list[LARGE_ENOUGH_DICTIONARY_SIZE];
    int depth[LARGE_ENOUGH_DICTIONARY_SIZE];
    int global_depth;
    int seed_hash_to_segment;

    Entry dump_entry_list[MAX_ITEMS_PER_SEGMENT];
    Entry *dump_entry_list2[MAX_INSERT_DEPTH];
    bool dump_entry_list_in_use_flag[MAX_INSERT_DEPTH];
    int insert_depth;

    Dictionary(std::function<BaseSegment*()> factory)
        : segment_factory_function(factory)
    {
    // Dictionary dict([]() -> BaseSegment* {
    //     return new DerivedSegment();
    // });
        seed_hash_to_segment = 13;
        segment_list[0] = segment_factory_function();
        memset(depth, 0, sizeof(depth));
        global_depth = 0;
        for (int i = 0; i < MAX_INSERT_DEPTH; i++)
            dump_entry_list_in_use_flag[i] = false;
    }

    ~Dictionary(){
        for (int i = 0; i < LARGE_ENOUGH_DICTIONARY_SIZE; i++)
            if (segment_list[i] != nullptr){
                delete segment_list[i];
            }
    }

    int32_t hash(const char *key) {
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_segment);
    }

    int getDictIndex(const Entry& entry){
        int32_t hash_value = hash(entry.key);
        int32_t hash_global = hash_value & ((1 << global_depth) - 1);
        int local_depth = depth[hash_global];
        int32_t hash_local = hash_global & ((1 << local_depth) - 1);
        return hash_local;
    }

    int expand_new_segment(int old_index, int current_i = -1){
        std::cout << "current global depth = " << global_depth;
        if (current_i != -1) std::cout << "\tcurrent i = " << current_i;
        std::cout << std::endl;
        if (depth[old_index] == global_depth){
            // increase global_depth
            memcpy(depth + (1 << global_depth), depth, sizeof(int) * (1 << global_depth));
            global_depth++;
        }
        int new_index = old_index + (1 << depth[old_index]);
        depth[old_index]++;
        depth[new_index]++;
        segment_list[new_index] = segment_factory_function();
        return new_index;
    }

    bool insert(const Entry& entry, int current_i = -1) {
        int dictionary_index = getDictIndex(entry);
        BaseSegment *target_segment = segment_list[dictionary_index];
        bool result = target_segment->insert(entry);
        if (result) return true;
        // expand, two ways, choose way 2
        // 1. copy and clean
        // 2. dump and reinsert
        int insert_depth = 0;
        while (insert_depth < MAX_INSERT_DEPTH && dump_entry_list_in_use_flag[insert_depth])
            insert_depth++;
        assert(insert_depth < MAX_INSERT_DEPTH);
        dump_entry_list_in_use_flag[insert_depth] = true;
        if (insert_depth == 0){
            int total_entry = target_segment->dump(dump_entry_list);
            target_segment->clean();
            int second_dictionary_index = expand_new_segment(dictionary_index, current_i);
            for (int i = 0; i < total_entry; i++) insert(dump_entry_list[i]);
        } else {
            dump_entry_list2[insert_depth] = new Entry[MAX_ITEMS_PER_SEGMENT];
            int total_entry = target_segment->dump(dump_entry_list2[insert_depth]);
            target_segment->clean();
            int second_dictionary_index = expand_new_segment(dictionary_index, current_i);
            for (int i = 0; i < total_entry; i++) insert(dump_entry_list2[insert_depth][i]);
            delete [] dump_entry_list2[insert_depth];
        }
        dump_entry_list_in_use_flag[insert_depth] = false;
        return true;
    }

    int memory(){
        // ignore Dictionary memory
        int total_memory = 0;
        for (int i = 0; i < (1 << global_depth); i++){
            if (segment_list[i] != nullptr){
                total_memory += segment_list[i]->memory();
            }
        }
        return total_memory;
    }

    double load_factor(){
        int max_possible_items = 0;
        int current_items = 0;
        for (int i = 0; i < (1 << global_depth); i++){
            if (segment_list[i] != nullptr){
                max_possible_items += MAX_ITEMS_PER_SEGMENT;
                current_items += segment_list[i]->current_items();
            }
        }
        return (double) current_items / max_possible_items;
    }

    double bit_per_key(){
        double lf = load_factor();
        int mem_per_item = segment_list[0]->memory_per_item();
        return mem_per_item / lf;
    }
};

}