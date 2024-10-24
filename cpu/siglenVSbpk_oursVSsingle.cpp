#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw
#include <chrono>
#include <assert.h>

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif

#define SIG_BIT_RANGE_L 10
#define SIG_BIT_RANGE_R 32
#ifndef SIG_BIT
static_assert(false);
#endif
static_assert((SIG_BIT >= SIG_BIT_RANGE_L && SIG_BIT <= SIG_BIT_RANGE_R), "SIG_BIT must be [8, 20]");

#include "CuckooDuoSIGLEN.h"
#include "CuckooSingleSigbit.h"

using namespace std;

const string inputFilePath = "load.txt";
const string testResultDir = "output.csv";
#define TEST_SLOTS 3000'0000
CK::inputEntry entry[TEST_SLOTS * 2];

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
double LF[5], FPR, total_time[5][110], bpk[5];
int totalRound[5][110];
int LFstepSum = 100;

void test_all(int insert_number = TEST_SLOTS){
    int insertFailFlag = 0;
    int i, fails;
    int LFstep[110];
    for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
/******************************* create Cuckoo ********************************/
    insertFailFlag = 0;
    int cuckoo_max_kick_num = 3;
    CK::CuckooHashTable cuckoo(insert_number, cuckoo_max_kick_num);
    
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            if(cuckoo.insert(entry[i]) == false){
                insertFailFlag = 1;
                break;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by Cuckoo Duo Step " << step << " : " << diff.count() << " seconds" << endl;
        // cout << cuckoo.sumPathLength << " " << cuckoo.sumBFSqueueLength << " " << cuckoo.bfsTimes << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
        cuckoo.sum_RDMA_read_num += cuckoo.sum_move_num;
        cuckoo.sum_RDMA_read_num2 += cuckoo.sum_move_num;
        totalRound[0][step]++;
    }
    int valid_i = 0;
    for (int j = 0; j < i; j++){
        auto tmpVal = new char[8];
        bool result = cuckoo.query(entry[j].key, tmpVal);
        if (result){
            valid_i++;
            if (memcmp(entry[j].val, tmpVal, 8) != 0)
                valid_i--;
        }
        delete [] tmpVal;
    }
    LF[0] += (double)valid_i/insert_number;
    bpk[0] += (double)cuckoo.full_bpk() / LF[0];
/******************************* create Cuckoo single hash ********************************/
    insertFailFlag = 0;
    // int cuckoo_max_kick_num = 3;
    SCK::CuckooHashTable cuckooSingle(insert_number, cuckoo_max_kick_num);
    for(int step = 0; step < LFstepSum; step++){
        auto start = std::chrono::high_resolution_clock::now();
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            SCK::Entry insertEntry;
            memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
            memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
            if(cuckooSingle.insert(insertEntry) == false){
                insertFailFlag = 1;
                break;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        chrono::duration<double> diff = end - start;
        cout << "Time taken by Cuckoo Single Step " << step << " : " << diff.count() << " seconds" << endl;
        total_time[4][step] = diff.count();
        if (insertFailFlag) break;
        cuckooSingle.sum_RDMA_read_num += cuckooSingle.sum_move_num;
        cuckooSingle.sum_RDMA_read_num2 += cuckooSingle.sum_move_num;
        totalRound[4][step]++;
    }
    LF[4] += (double)i/insert_number;
    bpk[4] += (double)cuckooSingle.full_bpk() / LF[4];
    cout << "Cuckoo Single Done" << endl;
    return;
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

    int test_round = 1;
    for (int i = 1; i<=test_round; i++) test_all(TEST_SLOTS);
    for (int i=0; i<5; i++) LF[i] = LF[i] / test_round;
    cout << SIG_BIT << ",\t" << LF[0] << ",\t" << LF[4] << endl;

#if SIG_BIT == SIG_BIT_RANGE_L
    std::ofstream lfFile("LF_vs_SIGBIT_(single,ours).csv");
    lfFile << "SIGBIT,\tours,\tsingle" << endl;
#else
    std::ofstream lfFile("LF_vs_SIGBIT_(single,ours).csv", std::ios::app);
#endif
    lfFile << SIG_BIT << ",\t" << LF[0] << ",\t" << LF[4] << endl;
    lfFile.close();

    cout << SIG_BIT << ",\t" << bpk[0] << ",\t" << bpk[4] << endl;
    
#if SIG_BIT == SIG_BIT_RANGE_L
    std::ofstream fprFile("BPK_vs_SIGBIT_(single,ours).csv");
    fprFile << "SIGBIT,\tours,\tsingle" << endl;
#else
    std::ofstream fprFile("BPK_vs_SIGBIT_(single,ours).csv", std::ios::app);
#endif
    fprFile << SIG_BIT << ",\t" << bpk[0] << ",\t" << bpk[4] << endl;
    fprFile.close();
    return 0;
}