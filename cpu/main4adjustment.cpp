#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif
#ifndef MY_SIGLEN
#define MY_SIGLEN 2
#endif

#include "CuckooDuoExtra.h"
#include "CuckooSingleDFSOnePath.h"

using namespace std;

const string inputFilePath = "load.txt";
const string testResultDir = "output.csv";
#define TEST_SLOTS 3000'0000
CK::Entry entry[TEST_SLOTS];

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
double LF[5], avrAdjust[5][110], avrMove[5][110], maxMove[5][110], avrRead[5][110], maxRead[5][110], avrReadCell[5][110], maxReadCell[5][110];
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
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            if(cuckoo.insert(entry[i]) == false){
                insertFailFlag = 1;
                break;
            }
        }
        if (insertFailFlag) break;
        cuckoo.sum_RDMA_read_num += cuckoo.sum_move_num;
        cuckoo.sum_RDMA_read_num2 += cuckoo.sum_move_num;
        totalRound[0][step]++;
        avrAdjust[0][step] += (double) cuckoo.sum_adjust_num / (LFstep[step+1] - LFstep[step]);
        avrMove[0][step] += (double) cuckoo.sum_move_num / (LFstep[step+1] - LFstep[step]);
        maxMove[0][step] += (double) cuckoo.max_move_num;
        avrRead[0][step] += (double) cuckoo.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
        maxRead[0][step] += (double) cuckoo.max_RDMA_read_num;
        avrReadCell[0][step] += (double) cuckoo.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
        maxReadCell[0][step] += (double) cuckoo.max_RDMA_read_num2;
        cuckoo.sum_adjust_num = 0;
        cuckoo.sum_move_num = 0;
        cuckoo.max_move_num = 0;
        cuckoo.sum_RDMA_read_num = 0;
        cuckoo.max_RDMA_read_num = 0;
        cuckoo.sum_RDMA_read_num2 = 0;
        cuckoo.max_RDMA_read_num2 = 0;
        cout << "step:" << step << "  adjust:" << avrAdjust[0][step] << endl;
    }
    LF[0] += (double)i/insert_number;
/******************************* create MapEmbed ********************************/
    // fails = 0;
    // insertFailFlag = 0;
    // int MapEmbed_layer = 3;
    // int MapEmbed_bucket_number = 30000000 / 8; //500000;
    // int MapEmbed_cell_number[3];
    // MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    // MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    // MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    // int MapEmbed_cell_bit = 4;
    
    // ME::MapEmbed mapembed(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit);
    
    // for(int step = 0; step < LFstepSum; step++){
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         ME::KV_entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.value, entry[i].val, 8*sizeof(char));
    //         if(mapembed.insert(insertEntry) == false)
    //             if(++fails >= 8){
    //                 insertFailFlag = 1;
    //                 break;
    //             }
    //     }
    //     if (insertFailFlag) break;
    //     mapembed.sum_RDMA_read_num += mapembed.sum_move_num;
    //     mapembed.sum_RDMA_read_num2 += mapembed.sum_move_num;
    //     totalRound[1][step]++;
    //     avrMove[1][step] += (double) mapembed.sum_move_num / (LFstep[step+1] - LFstep[step]);
    //     maxMove[1][step] += (double) mapembed.max_move_num;
    //     avrRead[1][step] += (double) mapembed.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
    //     maxRead[1][step] += (double) mapembed.max_RDMA_read_num;
    //     avrReadCell[1][step] += (double) mapembed.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
    //     maxReadCell[1][step] += (double) mapembed.max_RDMA_read_num2;
    //     mapembed.sum_move_num = 0;
    //     mapembed.max_move_num = 0;
    //     mapembed.sum_RDMA_read_num = 0;
    //     mapembed.max_RDMA_read_num = 0;
    //     mapembed.sum_RDMA_read_num2 = 0;
    //     mapembed.max_RDMA_read_num2 = 0;
    // }
    // LF[1] += mapembed.load_factor();//(double)i/insert_number;
// /******************************* create RACE ********************************/
    // fails = 0;
    // insertFailFlag = 0;
    // RACE::RACETable race(insert_number);
    // for(int step = 0; step < LFstepSum; step++){
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         RACE::Entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
    //         if(race.insert(insertEntry) == false)
    //             if(++fails >= 8){
    //                 insertFailFlag = 1;
    //                 break;
    //             }
    //     }
    //     if (insertFailFlag) break;
    //     race.sum_RDMA_read_num += race.sum_move_num;
    //     race.sum_RDMA_read_num2 += race.sum_move_num;
    //     totalRound[2][step]++;
    //     avrMove[2][step] += (double) race.sum_move_num / (LFstep[step+1] - LFstep[step]);
    //     maxMove[2][step] += (double) race.max_move_num;
    //     avrRead[2][step] += (double) race.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
    //     maxRead[2][step] += (double) race.max_RDMA_read_num;
    //     avrReadCell[2][step] += (double) race.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
    //     maxReadCell[2][step] += (double) race.max_RDMA_read_num2;
    //     race.sum_move_num = 0;
    //     race.max_move_num = 0;
    //     race.sum_RDMA_read_num = 0;
    //     race.max_RDMA_read_num = 0;
    //     race.sum_RDMA_read_num2 = 0;
    //     race.max_RDMA_read_num2 = 0;
    // }
    // LF[2] += (double)i/insert_number;
// /******************************* create TEA ********************************/
    // fails = 0;
    // insertFailFlag = 0;
    // int tea_max_kick_num = 1000;
    // TEA::TEATable tea(insert_number, tea_max_kick_num);
    // for(int step = 0; step < LFstepSum; step++){
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         TEA::Entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
    //         if(tea.insert(insertEntry) == false)
    //             if(++fails >= 8){
    //                 insertFailFlag = 1;
    //                 break;
    //             }
    //     }
    //     if (insertFailFlag) break;
    //     tea.sum_RDMA_read_num += tea.sum_move_num;
    //     tea.sum_RDMA_read_num2 += tea.sum_move_num;
    //     totalRound[3][step]++;
    //     avrMove[3][step] += (double) tea.sum_move_num / (LFstep[step+1] - LFstep[step]);
    //     maxMove[3][step] += (double) tea.max_move_num;
    //     avrRead[3][step] += (double) tea.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
    //     maxRead[3][step] += (double) tea.max_RDMA_read_num;
    //     avrReadCell[3][step] += (double) tea.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
    //     maxReadCell[3][step] += (double) tea.max_RDMA_read_num2;
    //     tea.sum_move_num = 0;
    //     tea.max_move_num = 0;
    //     tea.sum_RDMA_read_num = 0;
    //     tea.max_RDMA_read_num = 0;
    //     tea.sum_RDMA_read_num2 = 0;
    //     tea.max_RDMA_read_num2 = 0;
    // }
    // LF[3] += (double)i/insert_number;
/******************************* create Cuckoo single hash ********************************/
    // insertFailFlag = 0;
    // // int cuckoo_max_kick_num = 3;
    // cuckoo_max_kick_num = 50;
    // SCK::CuckooHashTable cuckooSingle(insert_number, cuckoo_max_kick_num);
    // for(int step = 0; step < LFstepSum; step++){
    //     cout << "step:" << step << "  cache: " << cuckooSingle.collision_num << endl;
    //     for(i = LFstep[step]; i < LFstep[step+1]; ++i){
    //         SCK::Entry insertEntry;
    //         memcpy(insertEntry.key, entry[i].key, 8*sizeof(char));
    //         memcpy(insertEntry.val, entry[i].val, 8*sizeof(char));
    //         if(cuckooSingle.insert(insertEntry) == false){
    //             insertFailFlag = 1;
    //             break;
    //         }
    //     }
    //     if (insertFailFlag) break;
    //     cuckooSingle.sum_RDMA_read_num += cuckooSingle.sum_move_num;
    //     cuckooSingle.sum_RDMA_read_num2 += cuckooSingle.sum_move_num;
    //     totalRound[4][step]++;
    //     avrMove[4][step] += (double) cuckooSingle.sum_move_num / (LFstep[step+1] - LFstep[step]);
    //     maxMove[4][step] += (double) cuckooSingle.max_move_num;
    //     avrRead[4][step] += (double) cuckooSingle.sum_RDMA_read_num / (LFstep[step+1] - LFstep[step]);
    //     maxRead[4][step] += (double) cuckooSingle.max_RDMA_read_num;
    //     avrReadCell[4][step] += (double) cuckooSingle.sum_RDMA_read_num2 / (LFstep[step+1] - LFstep[step]);
    //     maxReadCell[4][step] += (double) cuckooSingle.max_RDMA_read_num2;
    //     cuckooSingle.sum_move_num = 0;
    //     cuckooSingle.max_move_num = 0;
    //     cuckooSingle.sum_RDMA_read_num = 0;
    //     cuckooSingle.max_RDMA_read_num = 0;
    //     cuckooSingle.sum_RDMA_read_num2 = 0;
    //     cuckooSingle.max_RDMA_read_num2 = 0;
    // }
    // LF[4] += (double)i/insert_number;
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
    memset(avrAdjust, 0, sizeof(avrAdjust));
    memset(avrMove, 0, sizeof(avrMove));
    memset(maxMove, 0, sizeof(maxMove));
    memset(avrRead, 0, sizeof(avrRead));
    memset(maxRead, 0, sizeof(maxRead));
    memset(avrReadCell, 0, sizeof(avrReadCell));
    memset(maxReadCell, 0, sizeof(maxReadCell));

    int test_round = 6;
    for (int i = 1; i<=test_round; i++)
    {
        cout << "iteration: " << i << endl;
        // normal_test(TEST_SLOTS/6 * i);
        test_all(TEST_SLOTS);
    }
    // cout<<"SIG_LEN: "<<(SIG_LEN*8)<<endl;
    // cout << "cuckoo,\tMapEmbed,\tRACE,\tTEA,\tcuckoo_SingleHash\n";
    for (int i=0; i<5; i++) LF[i] = LF[i] / test_round;
    for (int i=0; i<5; i++) cout << LF[i] << ",\t";
    cout << endl;
    if(!strcmp(argv[argc - 1], "average_moved_items")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                // outputFile << std::max(avrMove[j][i] / test_round, (double)1) << ",";
                if (totalRound[j][i] > 0) outputFile << avrMove[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "worst_moved_items")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                // outputFile << std::max(maxMove[j][i] / test_round, (double)1) << ",";
                if (totalRound[j][i] > 0) outputFile << maxMove[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "average_accessed_items")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                if (totalRound[j][i] > 0) outputFile << avrRead[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "worst_accessed_items")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                if (totalRound[j][i] > 0) outputFile << maxRead[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "average_memory_accesses")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                if (totalRound[j][i] > 0) outputFile << avrReadCell[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "worst_memory_accesses")){
        std::ofstream outputFile(testResultDir);
        outputFile << "load_factor,cuckoo,MapEmbed,RACE,TEA,cuckoo_SingleHash" << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<5; j++){
                if (totalRound[j][i] > 0) outputFile << maxReadCell[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    if(!strcmp(argv[argc - 1], "average_adjustments")){
        std::ofstream outputFile(testResultDir);
        // outputFile << "load_factor,cuckoo," << endl;
        for (int i=0; i<LFstepSum; i++){
            outputFile << (i+1) << ",";
            for (int j=0; j<1; j++){
                if (totalRound[j][i] > 0) outputFile << avrAdjust[j][i] / totalRound[j][i];
                outputFile << ",";
            }
            outputFile << endl;
        }
        outputFile.close();
    }
    return 0;
}