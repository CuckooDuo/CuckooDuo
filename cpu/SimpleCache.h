/*
    A simple LRU cache
*/

#pragma once
#include "murmur3.h"
#include <cstring>
#include <random>
#include <set>
#include <cstdio>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define KEY_LEN 8
#define VAL_LEN 8
#define KEY_TYPE uint64_t
#define VAL_TYPE uint64_t
#define CACHE_BUCKET_SIZE 8

namespace SimpleLRUCache {

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

void turnOutKV(KEY_TYPE KV, char *OutKV){
    int byte = -1;
    while(++byte < KEY_LEN) {
        OutKV[byte] = KV & 0b11111111;
        KV = KV >> 8;
    }
}

struct Bucket{
    KEY_TYPE key[CACHE_BUCKET_SIZE];
    VAL_TYPE val[CACHE_BUCKET_SIZE];
    uint32_t lru[CACHE_BUCKET_SIZE];
};

// #define MAX_LRU 0xff
// #define HALF_MAX_LRU 0x80

class Cache{
public:
    int bucket_number;
    Bucket *buckets;
    uint32_t seed_hash_to_bucket;

    Cache(int memory_in_bytes){
        // int sizeofBucket = sizeof(Bucket);
        int sizeofBucket = 64 * CACHE_BUCKET_SIZE + 64 * CACHE_BUCKET_SIZE;
        bucket_number = memory_in_bytes / sizeofBucket;
        buckets = new Bucket[bucket_number];
        memset(buckets, 0, sizeof(Bucket) * bucket_number);

        srand(222);
        seed_hash_to_bucket = rand()%MAX_PRIME32;
    }

    uint32_t hash_bucket_id(KEY_TYPE key){
        auto OutKey = turnOutKV(key);
        uint32_t result = MurmurHash3_x86_32(OutKey, KEY_LEN, seed_hash_to_bucket) % bucket_number;
        delete [] OutKey;
        return result;
    }

    uint32_t hash_bucket_id(const char *key){
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_bucket) % bucket_number;
    }

    bool query(const char *key, char *val){
        KEY_TYPE InKey = turnInKV(key);
        uint32_t bucket_id = hash_bucket_id(key);
        uint32_t maxLRU = 0;
        for (int i = 0; i < CACHE_BUCKET_SIZE; i++){
            maxLRU = std::max(maxLRU, buckets[bucket_id].lru[i]);
        }
        for (int i = 0; i < CACHE_BUCKET_SIZE; i++){
            if (InKey == buckets[bucket_id].key[i]){
                turnOutKV(buckets[bucket_id].val[i], val);
                buckets[bucket_id].lru[i] = maxLRU + 1;
                return true;
            }
        }
        return false;
    }

    void insert(const char *key, const char *val){
        KEY_TYPE InKey = turnInKV(key);
        VAL_TYPE InVal = turnInKV(val);
        uint32_t bucket_id = hash_bucket_id(key);
        uint32_t maxLRU = 0, minLRU = 0xffffffff, minCellID = CACHE_BUCKET_SIZE + 5;
        for (int i = 0; i < CACHE_BUCKET_SIZE; i++){
            if (buckets[bucket_id].lru[i] < minLRU){
                minLRU = buckets[bucket_id].lru[i];
                minCellID = i;
            }
            maxLRU = std::max(maxLRU, buckets[bucket_id].lru[i]);
        }
        buckets[bucket_id].key[minCellID] = InKey;
        buckets[bucket_id].val[minCellID] = InVal;
        buckets[bucket_id].lru[minCellID] = maxLRU + 1;
    }

    void clear(){
        memset(buckets, 0, sizeof(Bucket) * bucket_number);
    }
};

}