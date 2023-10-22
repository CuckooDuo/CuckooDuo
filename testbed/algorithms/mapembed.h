/*
 * Class declaraiton and definition for MapEmbed
 * 
 */

#ifndef MapEmbed_H
#define MapEmbed_H

//This the implementation of MapEmbed Hashing in C++.
//#include "immintrin.h"
#include "murmur3.h"
#include <cstring>
#include <random>
#include <set>
#include <cstdio>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <mutex>
#include <unordered_map>

#include "../rdma/rdma_client.h"

// begin kv pair
#ifndef _ENTRY_H_
#define _ENTRY_H_
#define KEY_LEN 8
#define VAL_LEN 8

struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
#endif

/* Define MapEmbed class in namespace ME */
namespace ME {

#define KV_NUM (30000000)
#define STASH_SIZE_ME 64        // size of stash

Entry kvPairs[KV_NUM+10];

inline void random_string(char* str, int len){
    for(int i = 0; i < len; ++i)
        str[i] = '!' + rand()%93;
    str[len-1] = 0;
}

void create_random_kvs(Entry *kvs, int num){
    srand((uint32_t)time(0));
    for(int i = 0; i < num; ++i){
        random_string(kvs[i].key, KEY_LEN);
        random_string(kvs[i].val, VAL_LEN);
    }
}

void create_random_kvs_keyint(Entry *kvs, int num){
    for(uint32_t i = 0; i < num; ++i){
        *(int*)kvs[i].key = i;
        *(int*)kvs[i].val = i;
    }
}

// end kv pair


int cnt = 0; // debug use

#define MAX_LAYER 12
#define MAX_HASH 32                // max number of hash functions in each layer
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8
#endif

struct Bucket{
    /* These should be in remote for real implement */
    /* We just do a simulation, so they're both in local and remote */
    char key[N][KEY_LEN];
    char val[N][VAL_LEN];
    /* These could be in local or in remote */
    int layer[N];           // item's layer
    int k[N];               // item's cell index
    int used;               // used slots
    bool check;
    char pad[27];
};

class MapEmbed{
public:
    int bucket_number;

    int cell_layer;
    int cell_number[MAX_LAYER];     // cell number of each layer
    
    int cell_bit;        // bit number of each cell
    int cell_hash;       // number of hash functions in each layer, 2^(cell's bit number)
    int cell_full;       // 2^(cell's bit number)-1

    int cell_offset[MAX_LAYER];     // used to calculate the unique ID of a cell, e.g. cell[l][k]'s ID is k+cell_offset[l]
    std::unordered_map<std::string, std::string> stash;  //overflow stash
    int stash_num = 0;

#ifdef USING_SIMD
    __attribute__((aligned(32))) Bucket *bucket;
#else
    Bucket *bucket;
#endif
    uint32_t **cell;
    
    uint32_t seed_hash_to_cell[MAX_LAYER];
    uint32_t seed_hash_to_bucket[MAX_HASH];
    uint32_t seed_hash_to_guide;

// below are print status
public:
    int bucket_items;

    mutex stash_mut;    // mutex for stash visition
	mutex *bucket_mut;  // mutex for bucket visition
	mutex **cell_mut;   // mutex for cell visition

    int thread_num;

private:
    /* function to lock and unlock buckets */
	void inline bucket_lock(int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[bucket_idx].lock();
        }
    }
    void inline bucket_unlock(int bucket_idx) {
        if (thread_num > 1) {
            bucket_mut[bucket_idx].unlock();
        }
    }
    void inline bucket_lock(const std::set<int> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].lock();
        }
    }
    void inline bucket_unlock(const std::set<int> &mut_idx) {
        if (thread_num > 1) {
            for (const auto &i : mut_idx)
                bucket_mut[i].unlock();
        }
    }

    /* function to lock and unlock stash */
    void inline stash_lock() {
        if (thread_num > 1) {
            stash_mut.lock();
        }
    }
    void inline stash_unlock() {
        if (thread_num > 1) {
            stash_mut.unlock();
        }
    }

    /* function to lock and unlock cell */
    void inline cell_lock(int layer, int k) {
        if (thread_num > 1) {
            cell_mut[layer][k].lock();
        }
    }
    void inline cell_unlock(int layer, int k) {
        if (thread_num > 1) {
            cell_mut[layer][k].unlock();
        }
    }

    void initialize_hash_functions(){
        std::set<int> seeds;
        uint32_t seed = rand()%MAX_PRIME32;
        seed_hash_to_guide = seed;
        seeds.insert(seed);
        for(int i = 0; i < cell_layer; ++i){
            seed = rand()%MAX_PRIME32;
            while(seeds.find(seed) != seeds.end())
                seed = rand()%MAX_PRIME32;
            seed_hash_to_cell[i] = seed;
            seeds.insert(seed);
        }
        for(int i = 0; i < MAX_HASH; ++i){
            seed = rand()%MAX_PRIME32;
            while(seeds.find(seed) != seeds.end())
                seed = rand()%MAX_PRIME32;
            seed_hash_to_bucket[i] = seed;
            seeds.insert(seed);
        }
    }


    int calculate_cell(const char* key, int layer){
        // cost: calculate 1 hash
        return MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_cell[layer]) % cell_number[layer];
    }

    int calculate_guide(const char* key, int cell_value){
        // cost: calculate 1 hash
        uint32_t tmp = MurmurHash3_x86_32(key, KEY_LEN, seed_hash_to_guide);
        int ret = ((tmp & (cell_hash - 1)) + cell_value) % cell_hash;
        return ret;
    }

    int calculate_bucket(const char* key, int layer, int k){
        // cost: calculate 2 hash, plus visit cell[layer][k]
        int guide = calculate_guide(key, cell[layer][k]);
        int cell_ID = k + cell_offset[layer];
        int page_size = bucket_number / cell_hash;
        int ret = MurmurHash3_x86_32((const char*)&cell_ID, sizeof(int), seed_hash_to_bucket[guide]) % page_size + guide * page_size;
        return ret;
    }

    int calculate_bucket_from_cell(int layer, int guide, int cell_ID){
        // cost: calculate 1 hash
        int page_size = bucket_number / cell_hash;
        int ret = MurmurHash3_x86_32((const char*)&cell_ID, sizeof(int), seed_hash_to_bucket[guide]) % page_size + guide * page_size;
        return ret;
    }

    /* Calculate a sequence of bucketIDs for cell[layer][k] */
    inline void calculate_for_RDMA(int layer, int k, int *bucketID) {
        int cell_ID = k + cell_offset[layer];
        int page_size = bucket_number / cell_hash;
        int j = 1;

        for (int i = 0; i < cell_hash; ++i) {
            int b_ID = MurmurHash3_x86_32((const char*)&cell_ID, sizeof(int), seed_hash_to_bucket[i]) % page_size + i * page_size;
            if (bucketID[i] != b_ID)
                bucketID[j++] = b_ID;
        }
    }
    /* RDMA write, write a sequence of buckets to remote */
    inline void ME_RDMA_write(int *bucketID, int sz, const Entry (*entry)[N], int tid){
        for (int i = 0; i < sz; ++i)
            ra[tid][i] = get_offset_table(0, bucketID[i], 0);
        if (entry)
            memcpy(ME_write_buf[tid].buf, entry, KV_LEN*N*sz);
        else
            memset(ME_write_buf[tid].buf, 0, KV_LEN*N*sz);
        mod_ME(sz, tid);
    }
    /* RDMA read, read a sequence of buckets from remote */
    inline void ME_RDMA_read(int *bucketID, int sz, Entry (*entry)[N], int tid){
        for (int i = 0; i < sz; ++i)
            ra[tid][i] = get_offset_table(0, bucketID[i], 0);
        get_ME(sz, tid);
        memcpy(entry, ME_read_buf[tid].buf, KV_LEN*N*sz);
    }

    // insert an full entry to stash
    bool insert_to_stash(const Entry& entry) {
		stash_lock();
        if(stash_num >= STASH_SIZE_ME) {
			stash_unlock();
            return false;
		}
        std::string skey(entry.key, KEY_LEN);
        std::string sval(entry.val, VAL_LEN);
        stash.emplace(skey, sval);
        stash_num++;
		stash_unlock();
        return true;
    }

    bool insert_kv_to_bucket(const Entry& kv, int d, int layer, int k){
        if(bucket[d].check == false)
            check_bucket(d);
        int used = bucket[d].used;
        if(used >= N)
            // bucket full
            return false;
        memcpy(bucket[d].key[used], kv.key, (KEY_LEN)*sizeof(char));
        memcpy(bucket[d].val[used], kv.val, (VAL_LEN)*sizeof(char));
        bucket[d].layer[used] = layer;
        bucket[d].k[used] = k;
        bucket[d].used++;
        return true;
    }
    
    // the returned bitmap size restrict MAX_HASH <= 32
    inline int collect_kv(int k, int layer, Entry *kvs, int *cell_bucket, int *bucket_empty, int &size){   
        int ret = 0;
        // return bitmap, 1: full 0: not full
        int cell_ID = k + cell_offset[layer];
        for(int guide = 0; guide < cell_hash; ++guide){
            int d = calculate_bucket_from_cell(layer, guide, cell_ID);
            cell_bucket[guide] = d;
            for(int j = 0; j < bucket[d].used; ++j){
                if(bucket[d].layer[j] == layer && bucket[d].k[j] == k){
                    memcpy(kvs[size].key, bucket[d].key[j], (KEY_LEN)*sizeof(char));
                    memcpy(kvs[size].val, bucket[d].val[j], (VAL_LEN)*sizeof(char));
                    size++;
                    int used = bucket[d].used;
                    if(j == used - 1)
                        bucket[d].used--;
                    else{
                        memcpy(bucket[d].key[j], bucket[d].key[used-1], (KEY_LEN)*sizeof(char));
                        memcpy(bucket[d].val[j], bucket[d].val[used-1], (VAL_LEN)*sizeof(char));
                        bucket[d].layer[j] = bucket[d].layer[used-1];
                        bucket[d].k[j] = bucket[d].k[used-1];
                        bucket[d].used--;
                        j--;
                    }
                }
            }
            if(bucket[d].used >= N)
                ret |= (1 << guide);
            bucket_empty[guide] = N - bucket[d].used;
        }
        return ret;
    }

    inline bool check_kvs(int now_cell, int *bucket_empty, int *kvs_guide_bucket_count){
        int hash_number = cell_hash;
        for(int i = 0; i < hash_number; ++i){
            int now_guide = (i + hash_number - now_cell) % hash_number;
            if(kvs_guide_bucket_count[now_guide] > bucket_empty[i])
                return false;
        }
        return true;
    }

    inline void dispense_kvs(const Entry *kvs, int size, int k, int layer, int *cell_bucket, int *kvs_now_guide){
        for(int i = 0; i < size; ++i){
            int guide = kvs_now_guide[i];
            int d = cell_bucket[guide];
            insert_kv_to_bucket(kvs[i], d, layer, k);
        }
    }

    void calculate_kvs_initial_guide(const Entry *kvs, int size, int *kvs_initial_guide, int *kvs_guide_bucket_count){
        for(int i = 0; i < size; ++i){
            int guide = calculate_guide(kvs[i].key, 0);
            kvs_initial_guide[i] = guide;
            kvs_guide_bucket_count[guide]++;
        }
    }

    void calculate_kvs_now_guide(int *kvs_initial_guide, int *kvs_now_guide, int size, int now_cell){
        for(int i = 0; i < size; ++i)
            kvs_now_guide[i] = (kvs_initial_guide[i] + now_cell) % cell_hash;
    }

    int calculate_kvs_initial_bitmap(int layer, int size, int *kvs_initial_guide){
        int ret = 0;
        for(int i = 0; i < size; ++i){
            int guide = kvs_initial_guide[i];
            ret |= (1 << guide);
        }
        return ret;
    }

    inline int shift_kvs_bitmap(int kvs_initial_bitmap, int now_cell){
        int hash_number = cell_hash;
        int tmp_mask = ((1 << now_cell) - 1);
        int full_mask = ((1 << hash_number) - 1);
        int tmp = (((kvs_initial_bitmap << now_cell) & (tmp_mask << hash_number)) >> hash_number);
        int ret = ((kvs_initial_bitmap << now_cell) | tmp) & full_mask;
        return ret;
    }

    bool query_key_in_bucket(const char* key, int d, char* val = NULL){
        if(bucket[d].check == false)
            check_bucket(d);
        
#ifdef USING_SIMD
        const __m256i item = _mm256_set1_epi32(*(int*)key);
        __m256i *keys_p = (__m256i *)(bucket[d].key);
        int matched = 0;

        __m256i a_comp = _mm256_cmpeq_epi32(item, keys_p[0]);
        matched = _mm256_movemask_ps((__m256)a_comp);
        

        if(matched != 0){
            int matched_lowbit = matched & (-matched);
            int matched_index = _mm_tzcnt_32((uint32_t)matched_lowbit);
            if(matched_index < bucket[d].used){
                if(val != NULL){
                    memcpy(val, bucket[d].val[matched_index], VAL_LEN*sizeof(char));
                }
                return true;
            }
            return false;
        }
        return false;

#else        
       int used = bucket[d].used;
       for(int i = 0; i < used; ++i){
           if(memcmp(bucket[d].key[i], key, KEY_LEN*sizeof(char)) == 0){
               if(val != NULL)
                   memcpy(val, bucket[d].val[i], VAL_LEN*sizeof(char));
               return true;
           }
       }
       return false;
#endif
    }

    int query_bucket(const char *key){
        for(int layer = 0; layer < cell_layer; ++layer){
            int k = calculate_cell(key, layer);
            if(cell[layer][k] == cell_full)
                continue;
            int d = calculate_bucket(key, layer, k);
            return d;
        }
        return -1;
    }

    void check_bucket(int d){
        int used = bucket[d].used;
        for(int i = 0; i < used; ++i){
            int dd = query_bucket(bucket[d].key[i]);
            if(dd != d){
                used--;
                memcpy(bucket[d].key[i], bucket[d].key[used], KEY_LEN);
                memcpy(bucket[d].val[i], bucket[d].val[used], VAL_LEN);
                bucket[d].layer[i] = bucket[d].layer[used];
                bucket[d].k[i] = bucket[d].k[used];
                i--;
            }
        }
        bucket[d].used = used;
        bucket[d].check = true;
    }

public:
    MapEmbed(int layer_, int bucket_number_, int* cell_number_, int cell_bit_, int thread_num_){
        thread_num = thread_num_;
        cell_layer = layer_;
        bucket_number = bucket_number_;
        bucket = new Bucket[bucket_number];
        for(int i = 0; i < bucket_number; ++i){
            bucket[i].used = false;
            bucket[i].check = true;
        }
        
        for(int i = 0; i < cell_layer; ++i)
            cell_number[i] = cell_number_[i];
        cell_bit = cell_bit_;
        cell_hash = pow(2, cell_bit);
        cell_full = cell_hash - 1;

        cell = new uint32_t* [cell_layer];
        for(int i = 0; i < cell_layer; ++i){
            cell[i] = new uint32_t[cell_number[i] + 10];
            memset(cell[i], 0, (cell_number[i]+10)*sizeof(uint32_t));
        }
        cell_offset[0] = 0;
        for(int i = 1; i < cell_layer; ++i)
            cell_offset[i] = cell_offset[i-1] + cell_number[i-1];
        initialize_hash_functions();

        bucket_items = 0;

		bucket_mut = new mutex[bucket_number];

		cell_mut = new mutex*[cell_layer];
		for (int i = 0; i < cell_layer; ++i)
			cell_mut[i] = new mutex[cell_number[i] + 10];
    }
    
    ~MapEmbed(){
        for(int i = 0; i < cell_layer; ++i) {
            delete [] cell[i];
			delete [] cell_mut[i];
		}
        delete [] cell;
        delete [] bucket;

		delete [] bucket_mut;
		delete [] cell_mut;
    }

    // One layer read 1/16 times, write once
    bool insert(const Entry& kv, int start_layer, int tid){
        // try to insert 'kv' from 'start_layer'
        if(start_layer >= cell_layer){ 
            cnt++; //debug
            return false;
        }
        for(int layer = start_layer; layer < cell_layer; ++layer){
            int k = calculate_cell(kv.key, layer);
            // k: cell index

			cell_lock(layer, k);
            if(cell[layer][k] == cell_full) {
				cell_unlock(layer, k);
                continue;
			}
            int d = calculate_bucket(kv.key, layer, k);
            // d: bucket index

            int bucketID[cell_hash] = {d};
            calculate_for_RDMA(layer, k, bucketID);

            std::set<int> mut_idx;
			//mut_idx.insert(d);

			bucket_lock(d);

            // for simulate, read a bucket
            Entry tmp_entry[cell_hash][N];
            ME_RDMA_read(&d, 1, &tmp_entry[0], tid);
            if(insert_kv_to_bucket(kv, d, layer, k)) {
                // for simulate, write a bucket
                ME_RDMA_write(&d, 1, &tmp_entry[0], tid);

				bucket_unlock(d);
				cell_unlock(layer, k);
                return true;
            }

            bucket_unlock(d);

            for (int i = 0; i < cell_hash; ++i)
				mut_idx.insert(bucketID[i]);
            bucket_lock(mut_idx);
            // for simulate, read the rest of buckets
            ME_RDMA_read(bucketID, cell_hash-1, &tmp_entry[1], tid);		

            // directly insert failed, collect items and adjust cell state
            Entry kvs[MAX_HASH*N+5];
            int cell_bucket[MAX_HASH+5] = {};
            int bucket_empty[MAX_HASH+5] = {};

            int size = 0;
            memcpy(kvs[size].key, kv.key, (KEY_LEN)*sizeof(char));
            memcpy(kvs[size].val, kv.val, (VAL_LEN)*sizeof(char));
            size++;
            int cell_bitmap = collect_kv(k, layer, kvs, cell_bucket, bucket_empty, size);
            // full: 1, not full: 0

            int kvs_initial_guide[MAX_HASH*N+5] = {};
            int kvs_now_guide[MAX_HASH*N+5] = {};
            int kvs_guide_bucket_count[MAX_HASH+5] = {};
            calculate_kvs_initial_guide(kvs, size, kvs_initial_guide, kvs_guide_bucket_count);
            int kvs_initial_bitmap = calculate_kvs_initial_bitmap(layer, size, kvs_initial_guide);

            int now_cell = cell[layer][k];

            while(true){
                now_cell++;
                if(now_cell == cell_full){
                    cell[layer][k] = now_cell;
                    bool ret = true;

					bucket_unlock(mut_idx);
					cell_unlock(layer, k);

                    for(int i = 0; i < size; ++i)
                        ret &= insert(kvs[i], layer+1, tid);
                    return ret;
                }
                int kvs_bitmap = shift_kvs_bitmap(kvs_initial_bitmap, now_cell);
                if((kvs_bitmap & cell_bitmap) != 0)
                    continue;
                if(check_kvs(now_cell, bucket_empty, kvs_guide_bucket_count)){
                    cell[layer][k] = now_cell;
                    calculate_kvs_now_guide(kvs_initial_guide, kvs_now_guide, size, now_cell);
                    dispense_kvs(kvs, size, k, layer, cell_bucket, kvs_now_guide);

                    // for simulate, write all buckets
                    ME_RDMA_write(bucketID, cell_hash, tmp_entry, tid);
					
					bucket_unlock(mut_idx);
					cell_unlock(layer, k);

                    return true;
                }
            }
        }

        //both group full, insert into stash
        if(insert_to_stash(kv))
            return true;
        return false;
    }

    /* Query key and put result in val */
    bool query(const char *key, char* val, int tid){
        for(int layer = 0; layer < cell_layer; ++layer){
            int k = calculate_cell(key, layer);

			cell_lock(layer, k);
            if(cell[layer][k] == cell_full) {
				cell_unlock(layer, k);
                continue;
			}
            int d = calculate_bucket(key, layer, k);
            
			bucket_lock(d);
            // for simulate, read a bucket
            Entry tmp_entry[N];
            ME_RDMA_read(&d, 1, &tmp_entry, tid);
			bucket_unlock(d);

            if (query_key_in_bucket(key, d, NULL)) {
				bucket_unlock(d);
				cell_unlock(layer, k);
				return true;
			}

			bucket_unlock(d);
			cell_unlock(layer, k);
        }

        // query in stash
        std::string skey(key, KEY_LEN);
		stash_lock();
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            std::string sval = entry->second;
            char* pval = const_cast<char*>(sval.c_str());
            if(val != NULL) memcpy(val, pval, VAL_LEN);
			stash_unlock();
            return true;
        }
		stash_unlock();

        return false;
    }

    /* Delete an entry given a key */
	bool deletion(const char *key, int tid){
        for(int layer = 0; layer < cell_layer; ++layer){
            int k = calculate_cell(key, layer);

			cell_lock(layer, k);
            if(cell[layer][k] == cell_full) {
				cell_unlock(layer, k);
                continue;
			}
            int d = calculate_bucket(key, layer, k);

			bucket_lock(d);
            // for simulate, read a bucket
            Entry tmp_entry[N];
            ME_RDMA_read(&d, 1, &tmp_entry, tid);
            if (!query_key_in_bucket(key, d, NULL)) {
				bucket_unlock(d);
				cell_unlock(layer, k);
				return false;
			}
            // for simulate, write a bucket
            ME_RDMA_write(&d, 1, NULL, tid);
			bucket_unlock(d);

			cell_unlock(layer, k);
			return true;
        }

        //query in stash, if find then delete
        std::string skey(key, KEY_LEN);
		stash_lock();
        auto entry = stash.find(skey);
        if(entry != stash.end()) {
            stash.erase(entry);
            stash_num--;
			stash_unlock();
            return true;
        }
		stash_unlock();

        return false;
    }

    /* Update an entry */
	bool update(const Entry& entry, int tid){
        for(int layer = 0; layer < cell_layer; ++layer){
            int k = calculate_cell(entry.key, layer);

			cell_lock(layer, k);
            if(cell[layer][k] == cell_full) {
				cell_unlock(layer, k);
                continue;
			}
            int d = calculate_bucket(entry.key, layer, k);

			bucket_lock(d);
            // for simulate, read a bucket
            Entry tmp_entry[N];
            ME_RDMA_read(&d, 1, &tmp_entry, tid);
            if (!query_key_in_bucket(entry.key, d, NULL)) {
				bucket_unlock(d);
				cell_unlock(layer, k);
				return false;
			}
            // for simulate, write a bucket
            ME_RDMA_write(&d, 1, &tmp_entry, tid);
			bucket_unlock(d);

			cell_unlock(layer, k);
			return true;
        }

        // query in stash
        stash_lock();
        std::string skey(entry.key, KEY_LEN);
        auto it = stash.find(skey);
        if(it != stash.end()) {
            std::string sval = entry.val;
            it->second = sval;
            stash_unlock();
            return true;
        }
        stash_unlock();
        
        return false;
    }

    void extend(bool lazy = true){  //note! initial bucket number must be multiple of `cell_hash'
        Bucket* bucket_new = new Bucket[bucket_number*2];
        int page_size = bucket_number / cell_hash;
        for(int i = bucket_number - 1; i >= 0; --i){
            int p = i / page_size;
            int q = i % page_size;
            // copy i to 2*p*page_size+q and 2*p*page_size+page_size+q
            int j1 = 2*p*page_size + q;
            int j2 = 2*p*page_size + page_size + q;
            memcpy(bucket_new + j1, bucket + i, sizeof(Bucket));
            memcpy(bucket_new + j2, bucket + i, sizeof(Bucket));
            bucket_new[j1].check = bucket_new[j2].check = false;
        }
        delete [] bucket;
        bucket = bucket_new;
        bucket_number *= 2;

        if(lazy == false){
            for(int i = 0; i < bucket_number; ++i)
                if(bucket[i].check == 0)
                    check_bucket(i);
        }
    }

// below are print status functions
public:
    int calculate_bucket_items(){
        bucket_items = 0;
        for(int i = 0; i < bucket_number; ++i){
            if(bucket[i].check == 0)
                check_bucket(i);
            bucket_items += bucket[i].used;
        }
        return bucket_items;
    }

    double bit_per_item(){
        calculate_bucket_items();
        int cell_bit_sum = 0;
        for(int i = 0; i < cell_layer; ++i)
            cell_bit_sum += cell_number[i] * cell_bit;
        return (double)cell_bit_sum / bucket_items;
    }

    double load_factor(){
        calculate_bucket_items();
        int bucket_slots = bucket_number * N;
        return (double)bucket_items / bucket_slots;
    }
};

}

#endif
