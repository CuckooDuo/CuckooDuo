#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>

#ifndef MY_BUCKET_SIZE
#define MY_BUCKET_SIZE 8
#endif
#ifndef MY_FP_LEN
#define MY_FP_LEN 16
#endif

#include "CuckooDuo.h"

using namespace std;

//#define TEST_SLOTS (1 << 20)
#define TEST_SLOTS 3000'0000
Entry entry[TEST_SLOTS];

#define EPSILON 0.000001
class my_tuple
{
public:
    double load_factor;
    int move_num;
    my_tuple(double a, int b) {load_factor=a; move_num=b;}
};

bool isBigger(double a, double b, double tolerance=1e-7) {
    return (a - tolerance) > b;
}

double roundDownToPrecision(double value, double precision) {
    return std::floor(value / precision) * precision;
}

double loadFactor(const CuckooHashTable& cuckoo, const int& bucket_num, const int& test_num)
{
    int table_num = 0;
    for(int i = 0; i < bucket_num; i++) {
        table_num += cuckoo.table[TABLE1].cell[i];
        table_num += cuckoo.table[TABLE2].cell[i];
    }
    double load_factor = double(table_num) / double(test_num);
    return load_factor;
}

int main() 
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

    // ifstream fin;
    // fin.open("a.txt");
    // for(uint32_t i = 0; i < TEST_SLOTS; ++i)
    // {
    //     uint64_t j;
    //     fin >> j;
    //     *(uint64_t*)entry[i].key = j;
    //     *(uint64_t*)entry[i].val = j;
    // }

    for(uint32_t i = 0; i < TEST_SLOTS; ++i)
    {
        *(int*)entry[i].key = i;
        *(int*)entry[i].val = i;
    }

    int test_num = TEST_SLOTS;
    int bucket_num = test_num/(N+N);
    int insert_success_count = 0;
    int query_success_count = 0;
    int table_num = 0;

    //cuckoo version
    // cout << "cuckoo filter version: bfs" << endl;
    // cout << "input your max kick number: \n";
    int max_kick_num = 3;
    // cin >> max_kick_num;
    // printf("max kick num: %d \n",max_kick_num);
    // cout << "\033[96m" << "entry num: " << TEST_SLOTS << "\033[39m" << endl;

    
    clock_t timer, timer0, timer1;

    int test_times = 1; //run test 5 times
    int test_run = 0;
    double load_factor[test_times+1];
    double distribution[test_times+1][max_kick_num+1];
    while(test_run++ < test_times)
    {
        // test run
        CuckooHashTable cuckoo(test_num, max_kick_num);
    
        insert_success_count = 0;
        query_success_count = 0;

        std::vector<my_tuple> last_time;
        last_time.emplace_back( my_tuple(0,0) );
        std::vector<my_tuple> savedData;
        int success = 0;
        int i=0;

        timer0 = timer = clock();
        int last_i = 0;

        // ofstream dataFile("5_1.csv");

        for(i = 0; i < test_num; i++) 
        {
            if(cuckoo.insert(entry[i])) 
            {
                insert_success_count++;
                if(cuckoo.query(entry[i].key)) 
                {
                    query_success_count++;
                } else {
                    // printf("query error: i = %d \n",i);
                    break;
                }
                // if (i%150'0000 == 0) printf("i = %d \n",i);
        
                if (i%10000 != 0) continue;
                double now = loadFactor(cuckoo, bucket_num, test_num);
                double last = roundDownToPrecision(last_time.back().load_factor, 0.01);
                if (isBigger(now - last, 0.01))
                {
                    timer1 = clock();
                    long long delay = (double)(timer1 - timer0) / CLOCKS_PER_SEC * 1000;
                    long long delayAll = (double)(timer1 - timer) / CLOCKS_PER_SEC * 1000;
                    double speed = (double)(i - last_i) / delay / 1000;
                    double speedAll = (double)(i) / delayAll / 1000;

                    success++;
                    last_time.emplace_back( my_tuple(now, cuckoo.move_num) );
                    timer0 = timer1;
                    last_i = i;
                }
            }
            else break;
        }
        cout << cuckoo.tcam_coll_num << "," 
            << (double)2*TEST_SLOTS*(MY_BUCKET_SIZE+1)*(MY_BUCKET_SIZE-1)/3/(1<<16) << ",\n";
    }
    //end one run
    return 0;
}
