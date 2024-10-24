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

#define SIG_BIT_RANGE_L 8
#define SIG_BIT_RANGE_R 20
#ifndef SIG_BIT
static_assert(false);
#endif
static_assert((SIG_BIT >= SIG_BIT_RANGE_L && SIG_BIT <= SIG_BIT_RANGE_R), "SIG_BIT must be [8, 20]");

#include "CuckooDuoSIGLEN.h"
#include "MapEmbed.h"
#include "tea.h"
#include "race.h"
#include "CuckooSingle.h"

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
double LF[5], FPR, total_time[5][110], avrMove[5][110], maxMove[5][110], avrRead[5][110], maxRead[5][110], avrReadCell[5][110], maxReadCell[5][110];
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
        // cout << "Time taken by Cuckoo Duo Step " << step << " : " << diff.count() << " seconds" << endl;
        // cout << cuckoo.sumPathLength << " " << cuckoo.sumBFSqueueLength << " " << cuckoo.bfsTimes << endl;
        total_time[0][step] = diff.count();
        if (insertFailFlag) break;
        cuckoo.sum_RDMA_read_num += cuckoo.sum_move_num;
        cuckoo.sum_RDMA_read_num2 += cuckoo.sum_move_num;
        totalRound[0][step]++;
        avrMove[0][step] += (double) cuckoo.sum_move_num / (LFstep[step+1] - LFstep[step]);
        maxMove[0][step] += (double) cuckoo.max_move_num;
        avrRead[0][step] += (double) cuckoo.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
        maxRead[0][step] += (double) cuckoo.max_RDMA_read_num;
        avrReadCell[0][step] += (double) cuckoo.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
        maxReadCell[0][step] += (double) cuckoo.max_RDMA_read_num2;
        cuckoo.sum_move_num = 0;
        cuckoo.max_move_num = 0;
        cuckoo.sum_RDMA_read_num = 0;
        cuckoo.max_RDMA_read_num = 0;
        cuckoo.sum_RDMA_read_num2 = 0;
        cuckoo.max_RDMA_read_num2 = 0;
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
    cuckoo.sum_move_num = 0;
    cuckoo.max_move_num = 0;
    cuckoo.sum_RDMA_read_num = 0;
    cuckoo.max_RDMA_read_num = 0;
    cuckoo.sum_RDMA_read_num2 = 0;
    cuckoo.max_RDMA_read_num2 = 0;
    LF[0] += (double)valid_i/insert_number;
    return;
}

void test_false_positive(int insert_number = TEST_SLOTS){
    int insertFailFlag = 0;
    int i, fails;
    int LFstep[110];
    for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
/******************************* create Cuckoo ********************************/
    insertFailFlag = 0;
    int cuckoo_max_kick_num = 3;
    CK::CuckooHashTable cuckoo(insert_number, cuckoo_max_kick_num);
    
    for(int step = 0; step < LFstepSum; step++){
        if (step > 70) break;
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            if(cuckoo.insert(entry[i]) == false){
                insertFailFlag = 1;
                break;
            }
        }
        if (insertFailFlag) break;
    }
    for(uint32_t i = insert_number; i < insert_number * 2; ++i)
    {
        *(uint64_t*)entry[i].key = i;
        *(int*)entry[i].val = i;
    }
    int FPC = 0;
    for (int i = insert_number; i < insert_number * 2; i++){
        bool result = cuckoo.filter_query(entry[i].key);
        if (result) FPC++;
    }
    FPR += (double)FPC/insert_number;
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
    memset(avrMove, 0, sizeof(avrMove));
    memset(maxMove, 0, sizeof(maxMove));
    memset(avrRead, 0, sizeof(avrRead));
    memset(maxRead, 0, sizeof(maxRead));
    memset(avrReadCell, 0, sizeof(avrReadCell));
    memset(maxReadCell, 0, sizeof(maxReadCell));

    int test_round = 1;
    for (int i = 1; i<=test_round; i++) test_all(TEST_SLOTS);
    for (int i=0; i<5; i++) LF[i] = LF[i] / test_round;
    cout << SIG_BIT << ",\t" << LF[0] << endl;

#if SIG_BIT == SIG_BIT_RANGE_L
    std::ofstream lfFile("LF_vs_SIGBIT.csv");
    lfFile << "SIGBIT,\tLF" << endl;
#else
    std::ofstream lfFile("LF_vs_SIGBIT.csv", std::ios::app);
#endif
    lfFile << SIG_BIT << ",\t" << LF[0] << endl;
    lfFile.close();

    FPR = 0;
    for (int i = 1; i<=test_round; i++) test_false_positive(TEST_SLOTS);
    FPR /= test_round;
    cout << SIG_BIT << ",\t" << FPR << endl;
    
#if SIG_BIT == SIG_BIT_RANGE_L
    std::ofstream fprFile("FPR_vs_SIGBIT.csv");
    fprFile << "SIGBIT,\tFPR" << endl;
#else
    std::ofstream fprFile("FPR_vs_SIGBIT.csv", std::ios::app);
#endif
    fprFile << SIG_BIT << ",\t" << FPR << endl;
    fprFile.close();
    return 0;
}