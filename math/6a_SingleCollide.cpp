#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include "CuckooSingle.h"

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
    cout << "cuckoo filter version: bfs" << endl;
    cout << "input your max kick number: \n";
    int max_kick_num = 3;
    // cin >> max_kick_num;
    printf("max kick num: %d \n",max_kick_num);
    cout << "\033[96m" << "entry num: " << TEST_SLOTS << "\033[39m" << endl;

    
    clock_t timer, timer0, timer1;

    int test_times = 1;
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

        ofstream dataFile("6a.csv");
        dataFile << "load_factor,Experimental,Theoretical\n";

        for(i = 0; i < test_num; i++) 
        {
            if(cuckoo.insert(entry[i])) 
            {
                insert_success_count++;
                if(cuckoo.query(entry[i].key)) 
                {
                    query_success_count++;
                } else {
                    printf("query error: i = %d \n",i);
                    break;
                }
                if (i%150'0000 == 0) printf("i = %d \n",i);
        
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

                    printf("i=%d, now=%f, last=%f, cache=%d, cacheColl=%d, cacheBFS=%d, time=%lldms\n"//, avrLen=%f, avrBFSelab=%f, bfsTimes=%d\n"
                        ,i,now,last,cuckoo.tcam_num,cuckoo.tcam_coll_num,cuckoo.tcam_bfs_num, delay);

                    dataFile << int(now*100) << "," << cuckoo.tcam_coll_num << "," << (double)TEST_SLOTS*8*int(now*100)*int(now*100)/(1<<16)/100/100 << ",\n";

                    success++;
                    last_time.emplace_back( my_tuple(now, cuckoo.move_num) );
                    timer0 = timer1;
                    last_i = i;
                }
            }
            else break;
        }
        printf("i = %d \n",i);
        printf("success = %d \n",success);
    
        cout << endl;
    
        //test outcome
        cout << "bucket_num: "<<bucket_num<<endl;
        cout << "max kick number: " << max_kick_num << endl;
        cout << "total count: inserted " << insert_success_count << " entries." << endl;
        cout << "tried kick " << cuckoo.kick_num << " times." << endl;
        cout << "kick success " << cuckoo.kick_success_num << " times." << endl;
        cout << "fingerprint collision number: " << cuckoo.collision_num << endl;
        cout << "entries in TCAM: " << cuckoo.tcam_num << endl;
        cout << "insert success rate: " << double(insert_success_count) / double(test_num) << endl;
        cout << "\033[96m" << "move sig: " << cuckoo.move_num << "\033[39m" << endl;
        cout << endl;
        cout << "query success number: " << query_success_count << endl;
        cout << "query success rate: " << double(query_success_count) / double(insert_success_count) << endl;
        query_success_count = 0;
        for (int i=0; i<insert_success_count; i++){
            if (cuckoo.query(entry[i].key)){
                query_success_count++;
            } else {
                printf("wrong query: %d\n", i);
            }
        }
        cout << "query success number: " << query_success_count << endl;
        cout << "query success rate: " << double(query_success_count) / double(insert_success_count) << endl;
    
        cout << endl;
    
        //kick stats
        cout << "the distribution of kicks is listed as follows:" << endl;
        for(int i = 1; i < max_kick_num + 1; i++) {
            if(cuckoo.kick_stat[i] > 0) {
                distribution[test_run][i] = double(cuckoo.kick_stat[i]) / double(cuckoo.kick_success_num);
                cout << "kick " << i << " times, appears " << cuckoo.kick_stat[i] << " times, takes up " << distribution[test_run][i] << endl;
            }
        }
        cout << endl;
    
        //examine load factor
        table_num = 0;
        for(int i = 0; i < bucket_num; i++) {
            table_num += cuckoo.table[TABLE1].cell[i];
            table_num += cuckoo.table[TABLE2].cell[i];
        }
        //cout << "entries in table: " << table_num << endl;
        load_factor[test_run] = double(table_num) / double(test_num);
        cout << "\033[96m" << "load factor: " << load_factor[test_run] << "\033[39m" << endl;
    }
    //end one run
    return 0;
}
