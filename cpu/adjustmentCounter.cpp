#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw
#include <chrono>
#include <assert.h>

#define DEBUG_FLAG
// #define SIG_BIT 10
#define SIG_BIT_RANGE_L 10
#define SIG_BIT_RANGE_R 16
static_assert((SIG_BIT >= SIG_BIT_RANGE_L && SIG_BIT <= SIG_BIT_RANGE_R), "SIG_BIT must be [10, 16]");

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif

#include "CuckooDuoSIGLEN.h"

using namespace std;

const string inputFilePath = "load.txt";
const string testResultDir = "output.csv";
#define TEST_SLOTS 3000'0000
CK::inputEntry entry[TEST_SLOTS];
std::ofstream outputfile;

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
double LF[5], total_time[5][110], avrMove[5][110], maxMove[5][110], avrRead[5][110], maxRead[5][110], avrReadCell[5][110], maxReadCell[5][110];
int totalRound[5][110];
int LFstepSum = 100;

void test_all(int insert_number = TEST_SLOTS){
    int insertFailFlag = 0;
    int i, fails;
    int LFstep[110];
    for (i = 0; i <= LFstepSum; ++i) LFstep[i] = (int)((double)i * insert_number / LFstepSum);
    cout << "test begin" << endl;
/******************************* create Cuckoo ********************************/
    insertFailFlag = 0;
    int cuckoo_max_kick_num = 3;
    CK::CuckooHashTable cuckoo(insert_number, cuckoo_max_kick_num);
    
    for(int step = 0; step < LFstepSum; step++){
        // if (step > 90) break;
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
        cout << cuckoo.adjustmentCounter << endl;
        outputfile << ",\t" << cuckoo.adjustmentCounter;
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
        cuckoo.clear_counters();
    }
    LF[0] += (double)i/insert_number;
    cout << "Cuckoo Done" << endl;
    return;
}

int main(int argc, char *argv[])
{
    preprocessing();
    memset(totalRound, 0, sizeof(totalRound));
    memset(avrMove, 0, sizeof(avrMove));
    memset(maxMove, 0, sizeof(maxMove));
    memset(avrRead, 0, sizeof(avrRead));
    memset(maxRead, 0, sizeof(maxRead));
    memset(avrReadCell, 0, sizeof(avrReadCell));
    memset(maxReadCell, 0, sizeof(maxReadCell));


#if SIG_BIT == SIG_BIT_RANGE_L
    outputfile.open("AdjustmentCounterResult.csv");
    outputfile << "SIGBIT";
    for (int i = 0; i < 100; i++) outputfile << ",\t" << i+1;
    outputfile << endl;
#else
    outputfile.open("AdjustmentCounterResult.csv", std::ios::app);
#endif
    outputfile << SIG_BIT;
    test_all(TEST_SLOTS);
    for (int i=0; i<5; i++) cout << LF[i] << ",\t";
    cout << endl;
    outputfile << endl;
    outputfile.close();
    return 0;
}