/* 
 * A test program for query latency with cache under the same fast memory
 * Query with cache (Figure 11(c))
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo.h"
#include "../ycsb_header/ycsb_mapembed.h"
#include "../ycsb_header/ycsb_race.h"
#include "../ycsb_header/ycsb_tea.h"

#define CACHE_BYTES (65 * 1024 * 1024)

/* Struct of result data */
class csv_toulpe {
public:
	double not_inserted;
	double ck_latency;
	double me_latency;
	double race_latency;
	double tea_latency;
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "notInsert,CuckooDuo,MapEmbed,RACE,TEA\n";
	for (const auto& item : csv_data) {
		file << item.not_inserted << ","
			 << item.ck_latency << ","
			 << item.me_latency << ","
			 << item.race_latency << ","
			 << item.tea_latency << "\n";
	}
}

void test_ck(int cell_number) {
	CK::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;

	int test_thread_num = 1;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int load_num = cell_number;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "CuckooDuo" << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	int success_cnt = CK::MultiThreadAction(&tb, load_num, 1, command::INSERT);
	cout << "LF: " << success_cnt/(double)load_num << endl;

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		tb.cache->clear();
		if (i > 0) {
			for (int j = 0; j < check_num; ++j) {
				if (j % 10 == i-1) {
					*(uint64_t*)entry[cell_number+j].key = *(uint64_t*)entry[cell_number-check_num+j].key;
				}
			}
		}

		cout << "querying..." << endl;
		int check_start = load_num;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << CK::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		query_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
    	cout << "query latency: " << query_latency << endl;

		csv_data[i].ck_latency = query_latency;
	}
}

void test_me(int cell_number) {
	ME::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;

	int test_thread_num = 1;

	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, test_thread_num);

	int load_num = cell_number;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "MapEmbed" << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	int success_cnt = ME::MultiThreadAction(&tb, load_num, 1, command::INSERT);
	cout << "LF: " << success_cnt/(double)load_num << endl;

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		tb.cache->clear();
		if (i > 0) {
			for (int j = 0; j < check_num; ++j) {
				if (j % 10 == i-1) {
					*(uint64_t*)entry[cell_number+j].key = *(uint64_t*)entry[cell_number-check_num+j].key;
				}
			}
		}

		cout << "querying..." << endl;
		int check_start = load_num;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << ME::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		query_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
    	cout << "query latency: " << query_latency << endl;

		csv_data[i].me_latency = query_latency;
	}
}

void test_race(int cell_number) {
	RACE::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;

	int test_thread_num = 1;
	RACE::RACETable tb(cell_number, test_thread_num);

	int load_num = cell_number;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "RACE" << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	int success_cnt = RACE::MultiThreadAction(&tb, load_num, 1, command::INSERT);
	cout << "LF: " << success_cnt/(double)load_num << endl;

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		tb.cache->clear();
		if (i > 0) {
			for (int j = 0; j < check_num; ++j) {
				if (j % 10 == i-1) {
					*(uint64_t*)entry[cell_number+j].key = *(uint64_t*)entry[cell_number-check_num+j].key;
				}
			}
		}

		cout << "querying..." << endl;
		int check_start = load_num;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << RACE::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		query_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
    	cout << "query latency: " << query_latency << endl;

		csv_data[i].race_latency = query_latency;
	}
}

void test_tea(int cell_number) {
	TEA::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;

	int test_thread_num = 1;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	int load_num = cell_number;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "TEA" << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	int success_cnt = TEA::MultiThreadAction(&tb, load_num, 1, command::INSERT);
	cout << "LF: " << success_cnt/(double)load_num << endl;

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		tb.cache->clear();
		if (i > 0) {
			for (int j = 0; j < check_num; ++j) {
				if (j % 10 == i-1) {
					*(uint64_t*)entry[cell_number+j].key = *(uint64_t*)entry[cell_number-check_num+j].key;
				}
			}
		}

		cout << "querying..." << endl;
		int check_start = load_num;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << TEA::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		query_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
    	cout << "query latency: " << query_latency << endl;

		csv_data[i].tea_latency = query_latency;
	}
}

int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+100000, entry.end());

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "query_cache.csv";
	for (int i = 0; i <= 10; ++i) {
		csv_data.push_back({});
		csv_data[i].not_inserted = i*0.1;
	}

	/* Test latency with single client with different non-existent keys
	 * Non-existent keys changes from 0% to 100%
	 */
	string run_path = "ycsb_files_cache/run_18M_zipf.txt";
	read_ycsb_run(run_path);

	entry.insert(entry.end(), fc_entry.begin(), fc_entry.end());
	test_ck(cell_number);
	send_msg("zero", sock);
	entry.erase(entry.begin()+cell_number, entry.end());
	
	entry.insert(entry.end(), fc_entry.begin(), fc_entry.end());
	test_me(cell_number);
	send_msg("zero", sock);
	entry.erase(entry.begin()+cell_number, entry.end());
	
	entry.insert(entry.end(), fc_entry.begin(), fc_entry.end());
	test_race(cell_number);
	send_msg("zero", sock);
	entry.erase(entry.begin()+cell_number, entry.end());
	
	entry.insert(entry.end(), fc_entry.begin(), fc_entry.end());
	test_tea(cell_number);
	send_msg("zero", sock);

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}