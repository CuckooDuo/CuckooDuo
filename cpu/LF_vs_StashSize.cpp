#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw
#include <chrono>

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif

#include "CuckooDuoStashSize.h"

using namespace std;

const string inputFilePath = "load.txt";
#define TEST_SLOTS 3000'0000
CK::inputEntry entry[TEST_SLOTS];

#define EPSILON 0.000001

//---------------------------------------------------------CSV---------------------------------------------------------
class my_tuple
{
public:
    int stash_size;
    double load_factor;
    my_tuple(int a = 0, double b = 0)
        : stash_size(a), load_factor(b) {}
};
vector<my_tuple> csv_data;

void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "stash_size,load_factor\n";
	for (const auto& item : csv_data) {
		file << item.stash_size << ","
			 << item.load_factor << "\n";
	}
}

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
        // find the line with usertable
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {
            // get number follow the user
            size_t userStart = found + strlen("usertable user");
            size_t userEnd = line.find(" ", userStart);

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
    CK::swap_flag = true;
}

int LFstepSum = 100;

double test_all(int insert_number = TEST_SLOTS){
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
        for(i = LFstep[step]; i < LFstep[step+1]; ++i){
            if(cuckoo.insert(entry[i]) == false){
                insertFailFlag = 1;
                break;
            }
        }
        if (insertFailFlag) break;
    }

    double LF = (double)i/insert_number;
    cout << "Cuckoo Done" << endl;
    cout << "Stash size: " << CK::TCAM_SIZE << endl;
    cout << "LF: " << LF << endl;

    return LF;

}

int main(int argc, char *argv[])
{
    preprocessing();

    // stash_size = 0/1/2/4/8/16/32/64/128
    CK::stash_size = 0;
    csv_data.push_back(my_tuple(CK::stash_size, test_all(TEST_SLOTS)));

    for (int i = 0; i < 8; ++i) {
        CK::stash_size = 1<<i;
        csv_data.push_back(my_tuple(CK::stash_size, test_all(TEST_SLOTS)));
    }
    
    writeCSV("lf_vs_stash_sz.csv");
    
    return 0;
}