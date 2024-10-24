#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw
#include <chrono>
#include <assert.h>
#include <random>
#include <algorithm>

#define TOTAL_MEMORY_BYTE_USING_CACHE (65 * 1024 * 1024)
// #define INVALID_QUERY_PERCENT 30
#define INVALID_QUERY_PERCENT_RANGE_L 0
#define INVALID_QUERY_PERCENT_RANGE_R 100

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif

#include "CuckooDuoWithCache.h"
#include "MapEmbed.h"
#include "tea.h"
#include "race.h"
#include "CuckooSingle.h"

using namespace std;

const string inputFilePath = "../zw_dataset/zhouwei_dataset/load.txt";
const string queryFilePath = "../zw_dataset/zhouwei_dataset/run_18M_zipf.txt";
const string testResultDir = "output.csv";
#define TEST_SLOTS 3000'0000
#define QUERY_NUMBER 30'0000
CK::inputEntry entry[TEST_SLOTS];
CK::inputEntry queryEntry[QUERY_NUMBER];

#define EPSILON 0.000001

//---------------------------------------------------------CSV---------------------------------------------------------
class my_tuple
{
public:
    double load_factor;
    int sum_move_num;
    int table_size;
    my_tuple(double a = 0, int b = 0, int c = 0)
        : load_factor(a), sum_move_num(b), table_size(c) {}
};
bool isBigger(double a, double b, double tolerance=1e-7) {
    return (a - tolerance) > b;
}

double roundDownToPrecision(double value, double precision) {
    return std::floor(value / precision) * precision;
}

double loadFactor(const CK::CuckooHashTable& cuckoo, const int& bucket_num, const int& test_num)
{
    int table_num = 0;
    for(int i = 0; i < bucket_num; i++) {
        table_num += cuckoo.table[TABLE1].cell[i];
        table_num += cuckoo.table[TABLE2].cell[i];
    }
    double load_factor = double(table_num) / double(test_num);
    return load_factor;
}

enum command {
    INSERT = 1,
    UPDATE = 2,
    READ = 3,
    DELETE = 4
};
map<string, command> str_to_cmd = { {"INSERT",INSERT}, {"UPDATE",UPDATE}, {"DELETE",DELETE}, {"READ",READ} };
vector< tuple<command, uint64_t> > full_command;
unordered_map<uint64_t, int> valid_key_map;

void read_ycsb_load(string load_path, bool load_query = false)
{
    std::ifstream inputFile(load_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::string line;
    int count = 0;

    while (std::getline(inputFile, line)) {
        if (count % 1000000 == 0) printf("count : %d\n", count);
        // 查找包含 "usertable" 的行
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // 从 "user" 后面提取数字
            size_t userStart = found + strlen("usertable user"); // "user" 后面的位置
            size_t userEnd = line.find(" ", userStart); // 空格后面的位置

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);
                    if (load_query)
                        *(uint64_t*)queryEntry[count++].key = userID;
                    else {
                        // key = val
                        *(uint64_t*)entry[count].val = userID;
                        *(uint64_t*)entry[count++].key = userID;
                        // printf("userID: %lu\n", userID);
                        valid_key_map[userID] = 1;
                    }

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

if (load_query)
    cout<<"load: "<<count<<" query keys"<<endl;
else
    cout<<"load: "<<count<<" keys"<<endl;
    
    inputFile.close();
}

#define TEST_YCSB true

void preprocessing()
{
    if (TEST_YCSB)
    {
        printf("start ycsb load\n");
        string load_path = inputFilePath;
        read_ycsb_load(load_path);
        load_path = queryFilePath;
        read_ycsb_load(load_path, true);
    }
    else
    {
        for(uint32_t i = 0; i < TEST_SLOTS; ++i)
        {
            *(uint64_t*)entry[i].key = i;
            *(int*)entry[i].val = i;
        }
    }
}
double LF[5], total_time[5][110], avrMove[5][110], maxMove[5][110], avrRead[5][110], maxRead[5][110], avrReadCell[5][110], maxReadCell[5][110];
int totalRound[5][110];
int LFstepSum = 100;

// sketch ready to query
CK::CuckooHashTable *cuckoo;
ME::MapEmbed *mapembed;
RACE::RACETable *race;
TEA::TEATable *tea;
SCK::CuckooHashTable *cuckooSingle;

void test_query(int query_number = TEST_SLOTS){
    int i, fails;
    cout << "test query begin" << endl;
/******************************* create Cuckoo ********************************/
    cuckoo->clear_counters();
    // cout << totalRound[0][0] << " " << avrRead[0][0] << " " << cuckoo->RDMA_read_num << endl;
    auto start = std::chrono::high_resolution_clock::now();
    for(i = 0; i < query_number; i++){
        auto tmpVal = new char[8];
        bool result = cuckoo->query(queryEntry[i].key, tmpVal);
        delete [] tmpVal;
    }
    auto end = std::chrono::high_resolution_clock::now();
    chrono::duration<double> diff = end - start;
    cout << "Time taken by Cuckoo Duo Query : " << diff.count() << " seconds" << endl;
    totalRound[0][0]++;
    avrRead[0][0] += (double) cuckoo->RDMA_read_num / query_number;
    avrReadCell[0][0] += (double) cuckoo->RDMA_read_num2 / query_number;
    // cout << totalRound[0][0] << " " << avrRead[0][0] << " " << cuckoo->RDMA_read_num << endl;
    // cout << "Cuckoo Done" << endl;
/******************************* create MapEmbed ********************************/
    mapembed->clear_counters();
    start = std::chrono::high_resolution_clock::now();
    for(i = 0; i < query_number; i++){
        auto tmpVal = new char[8];
        bool result = mapembed->query(queryEntry[i].key, tmpVal);
        delete [] tmpVal;
    }
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    cout << "Time taken by MapEmbed Query : " << diff.count() << " seconds" << endl;
    totalRound[1][0]++;
    avrRead[1][0] += (double) mapembed->RDMA_read_num / query_number;
    avrReadCell[1][0] += (double) mapembed->RDMA_read_num2 / query_number;
    // cout << "MapEmbed Done" << endl;
// /******************************* create RACE ********************************/
    race->clear_counters();
    start = std::chrono::high_resolution_clock::now();
    for(i = 0; i < query_number; i++){
        auto tmpVal = new char[8];
        bool result = race->query(queryEntry[i].key, tmpVal);
        delete [] tmpVal;
    }
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    cout << "Time taken by RACE Query : " << diff.count() << " seconds" << endl;
    totalRound[2][0]++;
    avrRead[2][0] += (double) race->RDMA_read_num / query_number;
    avrReadCell[2][0] += (double) race->RDMA_read_num2 / query_number;
    // cout << "RACE Done" << endl;
// /******************************* create TEA ********************************/
    tea->clear_counters();
    start = std::chrono::high_resolution_clock::now();
    for(i = 0; i < query_number; i++){
        auto tmpVal = new char[8];
        bool result = tea->query(queryEntry[i].key, tmpVal);
        delete [] tmpVal;
    }
    end = std::chrono::high_resolution_clock::now();
    diff = end - start;
    cout << "Time taken by TEA Query : " << diff.count() << " seconds" << endl;
    totalRound[3][0]++;
    avrRead[3][0] += (double) tea->RDMA_read_num / query_number;
    avrReadCell[3][0] += (double) tea->RDMA_read_num2 / query_number;
    // cout << "TEA Done" << endl;
/******************************* create Cuckoo single hash ********************************/
    // cuckooSingle->clear_counters();
    // start = std::chrono::high_resolution_clock::now();
    // for(i = 0; i < query_number; i++){
    //     auto tmpVal = new char[8];
    //     bool result = cuckooSingle->query(queryEntry[i].key, tmpVal);
    //     delete [] tmpVal;
    // }
    // end = std::chrono::high_resolution_clock::now();
    // diff = end - start;
    // cout << "Time taken by Cuckoo Single Query : " << diff.count() << " seconds" << endl;
    // totalRound[4][0]++;
    // avrRead[4][0] += (double) cuckooSingle->RDMA_read_num / query_number;
    // avrReadCell[4][0] += (double) cuckooSingle->RDMA_read_num2 / query_number;
    // // cout << "Cuckoo Single Done" << endl;
}

void test_insert(int insert_number = TEST_SLOTS){
    int insertFailFlag = 0;
    int i, fails;
    int LFstep[110];
    for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
    cout << "test begin" << endl;
/******************************* create Cuckoo ********************************/
    insertFailFlag = 0;
    int cuckoo_max_kick_num = 3;
    // CK::CuckooHashTable cuckoo(insert_number, cuckoo_max_kick_num);
    cuckoo = new CK::CuckooHashTable(insert_number, cuckoo_max_kick_num);
    
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            if(cuckoo->insert(entry[i]) == false){
                insertFailFlag = 1;
                break;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by Cuckoo Duo Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
    }
    LF[0] += (double)i/insert_number;
    cout << "Cuckoo Done" << endl;
    // return;
/******************************* create MapEmbed ********************************/
    fails = 0;
    insertFailFlag = 0;
    int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = 30000000 / 8; //500000;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;
    
    // ME::MapEmbed mapembed(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit);
    mapembed = new ME::MapEmbed(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit);
    
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            ME::KV_entry insertEntry;
            memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
            memcpy(insertEntry.value, entry[i].val, 8*sizeof(char));
            if(mapembed->insert(insertEntry) == false)
                if(++fails >= 8){
                    insertFailFlag = 1;
                    break;
                }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by MapEmbed Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
    }
    LF[1] += mapembed->load_factor();//(double)i/insert_number;
    cout << "MapEmbed Done" << endl;
// /******************************* create RACE ********************************/
    fails = 0;
    insertFailFlag = 0;
    race = new RACE::RACETable(insert_number);
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            RACE::Entry insertEntry;
            memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
            memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
            if(race->insert(insertEntry) == false)
                if(++fails >= 8){
                    insertFailFlag = 1;
                    break;
                }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by RACE Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
    }
    LF[2] += (double)i/insert_number;
    cout << "RACE Done" << endl;
    // return;
// /******************************* create TEA ********************************/
    fails = 0;
    insertFailFlag = 0;
    int tea_max_kick_num = 1000;
    tea = new TEA::TEATable(insert_number, tea_max_kick_num);
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            TEA::Entry insertEntry;
            memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
            memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
            if(tea->insert(insertEntry) == false)
                if(++fails >= 8){
                    insertFailFlag = 1;
                    break;
                }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by TEA Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
    }
    LF[3] += (double)i/insert_number;
    cout << "TEA Done" << endl;
/******************************* create Cuckoo single hash ********************************/
    // insertFailFlag = 0;
    // // int cuckoo_max_kick_num = 3;
    // cuckooSingle = new SCK::CuckooHashTable(insert_number, cuckoo_max_kick_num);
    // for(int step = 0; step < LFstepSum; step++){
    //     auto start = std::chrono::high_resolution_clock::now();
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         SCK::Entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
    //         if(cuckooSingle->insert(insertEntry) == false){
    //             insertFailFlag = 1;
    //             break;
    //         }
    //     }
    //     auto end = std::chrono::high_resolution_clock::now();
    //     chrono::duration<double> diff = end - start;
    //     cout << "Time taken by Cuckoo Single Step " << step << " : " << diff.count() << " seconds" << endl;
    //     total_time[0][step] = diff.count();
    //     if (insertFailFlag) break;
    // }
    // LF[4] += (double)i/insert_number;
    // cout << "Cuckoo Single Done" << endl;
}

void make_query_invalid(int percent, int query_number = QUERY_NUMBER){
    std::vector<int> permutation(query_number);
    for (int i = 0; i < query_number; ++i) {
        permutation[i] = i;
    }
    std::mt19937 gen(123);
    std::shuffle(permutation.begin(), permutation.end(), gen);
    std::uniform_int_distribution<uint64_t> dis(0, std::numeric_limits<uint64_t>::max());
    // for (int i = 0; i < 10; i++) cout << permutation[i] << " ";
    // cout << endl;
    for (int i = 0; i < (double)query_number * percent / 100; i++){
        uint64_t invalid_id;
        while (true){
            invalid_id = dis(gen);
            auto it = valid_key_map.find(invalid_id);
            if (it == valid_key_map.end()) break;
        }
        *(uint64_t*)queryEntry[permutation[i]].key = invalid_id;
    }
}

int main(int argc, char *argv[])
{
    preprocessing();

    test_insert(TEST_SLOTS);
    for (int i=0; i<4; i++) cout << LF[i] << ",\t"; cout << endl;
    std::ofstream access_times_file("time_vs_InvalidQueryRatio.csv");
    std::ofstream access_items_file("item_vs_InvalidQueryRatio.csv");
    access_times_file << "IQR,cuckoo,MapEmbed,RACE,TEA" << endl;
    access_items_file << "IQR,cuckoo,MapEmbed,RACE,TEA" << endl;
#define TEST_ROUNDS 5
    for (int percent = INVALID_QUERY_PERCENT_RANGE_L; percent <= INVALID_QUERY_PERCENT_RANGE_R; percent += 10){
        make_query_invalid(percent);
        memset(totalRound, 0, sizeof(totalRound));
        memset(avrMove, 0, sizeof(avrMove));
        memset(maxMove, 0, sizeof(maxMove));
        memset(avrRead, 0, sizeof(avrRead));
        memset(maxRead, 0, sizeof(maxRead));
        memset(avrReadCell, 0, sizeof(avrReadCell));
        memset(maxReadCell, 0, sizeof(maxReadCell));
        for (int t = 0; t < TEST_ROUNDS; t++){
            cuckoo->cache->clear();
            mapembed->cache->clear();
            race->cache->clear();
            tea->cache->clear();
            // cuckooSingle->cache->clear();
            test_query(QUERY_NUMBER);
        }
        access_times_file << percent << ",\t";
        for (int i=0; i<4; i++) access_times_file << avrRead[i][0] / totalRound[i][0] << ",\t";
        access_times_file << endl;

        access_items_file << percent << ",\t";
        for (int i=0; i<4; i++) access_items_file << avrReadCell[i][0] / totalRound[i][0] << ",\t";
        access_items_file << endl;
    }
    cout << endl;
    return 0;
}