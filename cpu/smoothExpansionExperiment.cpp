#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw
#include <chrono>
#include <assert.h>

// #define TOTAL_MEMORY_BYTE_USING_CACHE (64 * 1024 * 1024)

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif

#include "RaceDict.h"
#include "CuckooDuoSIGLEN.h"
#include "race.h"
#include "CuckooSingle.h"

using namespace std;

const string inputFilePath = "load.txt";
const string testResultDir = "output.csv";
#define TEST_SLOTS 3000'0000
CK::inputEntry entry[TEST_SLOTS];

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

void read_ycsb_load(string load_path)
{
    std::ifstream inputFile(load_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::string line;
    int count = 0;

    while (std::getline(inputFile, line)) {
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
                    *(uint64_t*)entry[count++].key = userID;

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }

    cout<<"load: "<<count<<" keys"<<endl;
    
    inputFile.close();
}

#define TEST_YCSB false

void preprocessing()
{
    if (TEST_YCSB)
    {
        string load_path = inputFilePath;
        read_ycsb_load(load_path);
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
double LF[5][110], total_time[5][110];
int totalRound[5][110];
int LFstepSum = 100;

void test_all(int insert_number = TEST_SLOTS){
    int insertFailFlag = 0;
    int i;
    int LFstep[110];
    for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
    cout << "test begin" << endl;
/******************************* create Cuckoo ********************************/
    insertFailFlag = 0;
    SEF::Dictionary cuckoo_dict([]() -> SEF::BaseSegment* {
        return new CuckooDuoSegment();
    });
    
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            SEF::Entry tmp_entry;
            memcpy(tmp_entry.key, entry[i].key, sizeof(tmp_entry.key));
            memcpy(tmp_entry.val, entry[i].val, sizeof(tmp_entry.val));
            if(cuckoo_dict.insert(tmp_entry, i) == false){
                insertFailFlag = 1;
                break;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by Cuckoo Duo Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        totalRound[0][step]++;
        LF[0][step] += cuckoo_dict.load_factor();
    }
    cout << "Cuckoo Done" << endl;
// /******************************* create RACE ********************************/
    insertFailFlag = 0;
    SEF::Dictionary race_dict([]() -> SEF::BaseSegment* {
        return new RaceSegment();
    });

    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            SEF::Entry tmp_entry;
            memcpy(tmp_entry.key, entry[i].key, sizeof(tmp_entry.key));
            memcpy(tmp_entry.val, entry[i].val, sizeof(tmp_entry.val));
            if(race_dict.insert(tmp_entry, i) == false){
                insertFailFlag = 1;
                break;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by RACE Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
        totalRound[1][step]++;
        LF[1][step] += race_dict.load_factor();
    }
    cout << "RACE Done" << endl;
    return;
/******************************* create Cuckoo single hash ********************************/
    // insertFailFlag = 0;
    // // int cuckoo_max_kick_num = 3;
    // SCK::CuckooHashTable cuckooSingle(insert_number, cuckoo_max_kick_num);
    // for(int step = 0; step < LFstepSum; step++){
    //     auto start = std::chrono::high_resolution_clock::now();
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         SCK::Entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
    //         if(cuckooSingle.insert(insertEntry) == false){
    //             insertFailFlag = 1;
    //             break;
    //         }
    //     }
    //     auto end = std::chrono::high_resolution_clock::now();
    //     chrono::duration<double> diff = end - start;
    //     cout << "Time taken by Cuckoo Single Step " << step << " : " << diff.count() << " seconds" << endl;
    //     total_time[0][step] = diff.count();
    //     if (insertFailFlag) break;
    //     cuckooSingle.sum_RDMA_read_num += cuckooSingle.sum_move_num;
    //     cuckooSingle.sum_RDMA_read_num2 += cuckooSingle.sum_move_num;
    //     totalRound[4][step]++;
    // }
    // LF[4] += (double)i/insert_number;
    // cout << "Cuckoo Single Done" << endl;
}

int main(int argc, char *argv[])
{
    //choose how to create entry: random or 1 to 1M
    //create_random_kvs(entry, TEST_SLOTS);
    /*
    for(int i = 0; i < TEST_SLOTS; i++) {
        for(int j = 0; j < KEY_LEN; j++) {
            entry[i].key[j] = (i >> (8 * (KEY_LEN - j - 1))) & 0b11111111;
            entry[i].val[j] = 0;
        }
    }
    */
    preprocessing();
    memset(totalRound, 0, sizeof(totalRound));

    double ourslf = 0.95, racelf = 0.94;
    double init = 300, oursmem = init, racemem = init;
    for (int i = 0; i < 100; i++){
        double current_items = ((double)i + 1) / 100 * TEST_SLOTS;
        while (current_items > oursmem * ourslf){
            oursmem *= 2;
        }
        while (current_items > racemem * racelf){
            racemem *= 2;
        }
        LF[0][i] = current_items / oursmem;
        LF[1][i] = current_items / racemem;
    }
    for (int i=0; i<1; i++){
        for (int j = 0; j < 100; j++) 
            cout << LF[i][j] << ",\t";
        cout << endl;
    }
    return 0;

    int test_round = 1;
    for (int i = 1; i<=test_round; i++)
    {
        test_all(TEST_SLOTS);
    }
    for (int i=0; i<2; i++)
        for (int j = 0; j < 100; j++) LF[i][j] = LF[i][j] / totalRound[i][j];
    for (int i=0; i<2; i++){
        for (int j = 0; j < 100; j++) 
            cout << LF[i][j] << ",\t";
        cout << endl;
    }
    cout << endl;
    return 0;
}