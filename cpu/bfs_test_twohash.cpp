#include <iostream>
#include <cmath>
#include <fstream>
#include <bitset>
#include <ctime>
#include <map>
#include <iomanip> // Include <iomanip> for setw


//#include "cuckoo_group_dfs.h"
//#include "cuckoo_group_dfs_random.h"
//#include "cuckoo_group_bfs_twohash_move_less.h"
//#include "cuckoo_group_bfs.h"
//#include "cuckoo_group_bfs_twohash.h"

using namespace std;

//#define TEST_SLOTS (1 << 20)
#define TEST_SLOTS 3000'0000
Entry entry[TEST_SLOTS];

#define EPSILON 0.000001
//#define run_6e

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

// Function to write data to a CSV file
void writeCSV(const std::string& filename, const std::vector<my_tuple>& data) 
{
    std::ofstream file(filename, std::ios::app); 
    if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

    // Initialize maximum column widths with column headers' lengths
    size_t maxLoadFactorWidth = strlen("load_factor");
    size_t maxMoveNumWidth = strlen("sum_move_num");
    size_t maxTableSizeWidth = strlen("table_size");

    // Calculate maximum widths for each column
    for (const auto& item : data) {
        maxLoadFactorWidth = std::max(maxLoadFactorWidth, std::to_string(item.load_factor).length());
        maxMoveNumWidth = std::max(maxMoveNumWidth, std::to_string(item.sum_move_num).length());
        maxTableSizeWidth = std::max(maxTableSizeWidth, std::to_string(item.table_size).length());
    }

    // Write column headers
    file << std::left << std::setw(maxLoadFactorWidth) << "load_factor" << ",";
    file << std::left << std::setw(maxMoveNumWidth) << "sum_move_num" << ",";
    file << std::left << std::setw(maxTableSizeWidth) << "table_size" << "\n";

    // Write data rows with proper column widths
    for (const auto& item : data) {
        file << std::left << std::setw(maxLoadFactorWidth) << item.load_factor << ",";
        file << std::left << std::setw(maxMoveNumWidth) << item.sum_move_num << ",";
        file << std::left << std::setw(maxTableSizeWidth) << item.table_size << "\n";
    }

    file.close();
}
//---------------------------------------------------------CSV---------------------------------------------------------

bool isBigger(double a, double b, double tolerance=1e-7) {
    return (a - tolerance) > b;
}

double roundDownToPrecision(double value, double precision) {
    return std::floor(value / precision) * precision;
}

double loadFactor(const int& inserted, const int& cell_num)
{
    double load_factor = double(inserted) / double(cell_num);
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

void read_ycsb_run(string run_path) //unfinish
{
    std::ifstream inputFile(run_path);

    if (!inputFile.is_open()) {
        std::cerr << "can't open file" << std::endl;
        return;
    }

    std::vector<uint64_t> userIDs; // 存储用户ID的数组
    std::vector<std::string> prefixes; // 存储前缀的数组

    std::string line;
    while (std::getline(inputFile, line)) {
        // 查找包含 "usertable" 的行
        size_t found = line.find("usertable user");
        if (found != std::string::npos) {

            // 提取 "usertable" 前面的部分
            std::string prefix = line.substr(0, found-1);

            // 从 "user" 后面提取数字
            size_t userStart = found + strlen("usertable user"); // "user" 后面的位置
            size_t userEnd = line.find(" ", userStart); // 空格后面的位置

            if (userEnd != std::string::npos) {
                std::string userIDStr = line.substr(userStart, userEnd - userStart);
                try {
                    uint64_t userID = std::stoull(userIDStr);

                    full_command.emplace_back(tuple<command, uint64_t> (str_to_cmd[prefix], userID));

                } catch (const std::invalid_argument& e) {
                    std::cerr << "invalid id: " << userIDStr << std::endl;
                } catch (const std::out_of_range& e) {
                    std::cerr << "id out of range: " << userIDStr << std::endl;
                }
            }
        }
    }
    /*
    for (int i=0; i<full_command.size(); i++)
    {
        cout<<get<0>(full_command[i])<<" "<<get<1>(full_command[i])<<endl;
    }
    */
    cout<<"run: "<<full_command.size()<<" keys"<<endl;
    inputFile.close();
}

#define TEST_YCSB true

void preprocessing()
{
    if (TEST_YCSB)
    {
        string load_path = "/root/hongyisen/CuckooPerfectHash-main/ycsb-0.17.0/load_a.txt";
        string run_path = "/root/hongyisen/CuckooPerfectHash-main/ycsb-0.17.0/run_a.txt";
        read_ycsb_load(load_path);
        //read_ycsb_run(run_path);
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
void normal_test(int MAX_CELL_NUM = TEST_SLOTS)
{
    cout<<"test_num: "<<MAX_CELL_NUM<<endl;

    int test_num = MAX_CELL_NUM;
    int bucket_num = test_num/(N+L);
    int insert_success_count = 0;
    int query_success_count = 0;
    int update_success_count = 0;
    int table_num = 0;

    //cuckoo version
    cout << "cuckoo filter version: bfs" << endl;
    cout << "input your max kick number: \n";
    int max_kick_num = 3;
    //cin >> max_kick_num;
    printf("max kick num: %d \n",max_kick_num);
    cout << "\033[96m" << "entry num: " << test_num << "\033[39m" << endl;

    
    clock_t timer, timer0, timer1;

    int test_times = 1; //run test 5 times
    int test_run = 0;
    double load_factor[test_times+1];
    double distribution[test_times+1][max_kick_num+1];
    while(test_run++ < test_times)
    {
        // test run
        CuckooHashTable cuckoo(test_num, max_kick_num);

        // INSERT
        insert_success_count = 0;
        query_success_count = 0;

        std::vector<my_tuple> last_time;
        //my_tuple(load_factor, sum_move_num, table_size)
        last_time.emplace_back( my_tuple(0.0, 0, test_num) );
        int success = 0;
        int i=0;

        timer0 = timer = clock();
        int last_i = 0;
        int old_cache = 0;
        for(i = 0; i < test_num; i++)
        {
            // if (i == 25049446){
            //     printf("error in insert");
            // }

            if(cuckoo.insert(entry[i])) 
            {
                // for (int j=0; j<=i; j++){
                //     // if (i == 3850 && j == 3850){
                //     //     printf("error in insert");
                //     // }
                //     if (cuckoo.query(entry[j].key)) continue;
                //     else {
                //         printf("query %d error!", j);
                //     }
                // }
                insert_success_count++;
                if(cuckoo.query(entry[i].key)) 
                {
                    query_success_count++;
                } else {
                    printf("query error: i = %d \n",i);
                    break;
                }
                //if (i%150'0000 == 0) printf("i = %d \n",i);
                //if (i%10000 != 0) continue;
                double now = loadFactor(insert_success_count, test_num);
                double last = roundDownToPrecision(last_time.back().load_factor, 0.01);

                //if (i%100==0) printf("i=%d\n",i);

                if (insert_success_count % (test_num/100) == 0)
//                if (cuckoo.tcam_num > old_cache)
                {
                    old_cache = cuckoo.tcam_num;
                    timer1 = clock();
                    long long delay = (double)(timer1 - timer0) / CLOCKS_PER_SEC * 1000;
                    long long delayAll = (double)(timer1 - timer) / CLOCKS_PER_SEC * 1000;
                    double speed = (double)(i - last_i) / delay / 1000;
                    double speedAll = (double)(i) / delayAll / 1000;
                    printf("inserted=%d, now=%f, last=%f, cache=%d, time=%lldms, mops=%f, Alltime=%lldms, Allmops=%f, \n"
                        ,insert_success_count,now,last,cuckoo.tcam_num, delay, speed, delayAll, speedAll);
                    // printf("i=%d, now=%f, last=%f, cache=%d, guess=%d/%d=%f\n",i,now,last,cuckoo.tcam_num,
                    //     cuckoo.guessSuccess,(cuckoo.guessSuccess+cuckoo.guessFail),
                    //     (double)cuckoo.guessSuccess/(cuckoo.guessSuccess+cuckoo.guessFail));
                    success++;
                    last_time.emplace_back( my_tuple(now, cuckoo.sum_move_num, test_num) );
                    timer0 = timer1;
                    last_i = i;
                }
            }
            else break;
        }
        printf("i = %d \n",i);
        last_time.emplace_back( my_tuple(loadFactor(insert_success_count, test_num), cuckoo.sum_move_num, test_num) );
        printf("success = %d \n",success);
        writeCSV("cuckoo.csv", last_time);
        cout << endl;
    
        /*
        cout << "hash alt seed:" << cuckoo.seed_hash_to_alt << endl;
        cout << "hash bucket seed:" << cuckoo.seed_hash_to_bucket << endl;
        cout << "hash fp seed:" << cuckoo.seed_hash_to_fp << endl;
        */
    
        //test outcome
        cout << "bucket_num: "<<bucket_num<<endl;
        cout << "max kick number: " << max_kick_num << endl;
        cout << "total count: inserted " << insert_success_count << " entries." << endl;
        cout << "tried kick " << cuckoo.kick_num << " times." << endl;
        cout << "kick success " << cuckoo.kick_success_num << " times." << endl;
        cout << "fingerprint collision number: " << cuckoo.collision_num << endl;
        cout << "entries in TCAM: " << cuckoo.tcam_num << endl;
        cout << "insert success rate: " << double(insert_success_count) / double(test_num) << endl;
        cout << "\033[96m" << "move sig: " << cuckoo.sum_move_num << "\033[39m" << endl;
        cout << endl;
        cout << "query success number: " << query_success_count << endl;
        cout << "query success rate: " << double(query_success_count) / double(insert_success_count) << endl;


        // QUERY
        query_success_count = 0;
        for (int i=0; i<insert_success_count; i++){
            if (cuckoo.query(entry[i].key)){
                query_success_count++;
            }
            else
            {
                cout<<i<<endl;
                break;
                //cuckoo.insert(entry[i], true);
                //cuckoo.query(entry[i].key,NULL,true);
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
        load_factor[test_run] = loadFactor(insert_success_count, test_num);
        cout << "\033[96m" << "load factor: " << load_factor[test_run] << "\033[39m" << endl;

        return;

        // RUN
        /*
        cout<<endl<<"---------------start run---------------"<<endl;
        insert_success_count = 0;
        query_success_count = 0;
        update_success_count = 0;
        for(i = 0; i < test_num; i++) 
        {
            command temp_command = get<0>(full_command[i]);
            Entry temp_entry;
            *(uint64_t*)temp_entry.key = get<1>(full_command[i]);
            *(int*)temp_entry.val = i;

            bool test_success = true;

            switch(temp_command)
            {
            case INSERT:
                if(cuckoo.insert(temp_entry)) 
                {
                    insert_success_count++;
                    if(cuckoo.query(temp_entry.key) == false) 
                    {
                        //printf("insert error: i = %d \n",i);
                        test_success = false;
                    }
                }
                else
                {
                    //printf("insert error: i = %d \n",i);
                    test_success = false;
                }
                break;

            case READ:
                if (cuckoo.query(temp_entry.key))
                {
                    query_success_count++;
                }
                else 
                {
                    //printf("query error: i = %d \n",i);
                    test_success = false;
                }
                break;

            case UPDATE:
                int temp_val = 0;
                if (cuckoo.query(temp_entry.key, (char*)&temp_val) == false)
                {
                    //printf("update error, at query: i = %d \n",i);
                    test_success = false;
                }
                else
                {
                    //cout << "before update: "<<temp_val<<"i="<<i<<endl;                    
                    temp_val += 1;
                    if (cuckoo.update(temp_entry.key, (char*)&temp_val))
                    {
                        update_success_count++;
                        //cout << "after update: "<<temp_val<<"i="<<i<<endl;
                    }
                    else
                    {
                        //printf("update error: i = %d \n",i);
                        test_success = false;
                    }
                }
                break;
            }

            //if (test_success == false) break;
        }
        
        int total_success = insert_success_count + query_success_count + update_success_count;
        printf("insert = %d, query = %d, update = %d, sum = %d\n", 
            insert_success_count, query_success_count, update_success_count, total_success);
        */
    }
    //end one run
    return;
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
    string load_path = "/root/hongyisen/CuckooPerfectHash-main/ycsb-0.17.0/load1.txt";
    string run_path = "/root/hongyisen/CuckooPerfectHash-main/ycsb-0.17.0/run1.txt";
    preprocessing();
    std::ofstream file("cuckoo.csv");
    
#ifdef run_6e
    normal_test(TEST_SLOTS);
#else
    for (int i = 1; i<=6; i++)
    {
        normal_test(TEST_SLOTS/6 * i);
    }
#endif
    cout<<"SIG_LEN: "<<(SIG_LEN*8)<<endl;
    

/*
    //average stats
    cout << endl;
    double sum_load_factor = 0;
    double sum_distribution[max_kick_num+1];
    memset(sum_distribution, 0, sizeof(sum_distribution));
    for(int i = 1; i <= test_times; i++) {
        for(int j = 1; j <= max_kick_num; j++) {
            sum_distribution[j] += distribution[i][j];
        }
        sum_load_factor += load_factor[i];
    }
    double avg = sum_load_factor / test_times;
    cout << "average load factor: " << avg << endl;
    for(int i = 1; i <= max_kick_num; i++) {
        cout << "average distribution of kick " << i << " times : " << sum_distribution[i] / test_times << endl;
    }
*/
    return 0;
}