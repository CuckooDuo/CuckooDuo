/* 
 * A test program for query latency with non-existent keys
 * Query with non-existent keys (Figure 10(f))
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo.h"
#include "../ycsb_header/ycsb_mapembed.h"
#include "../ycsb_header/ycsb_race.h"
#include "../ycsb_header/ycsb_tea.h"

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
	int test_thread_num = 1;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int load_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "CuckooDuo" << endl;

/******************************** load data **********************************/
	cout << "loading..." << endl;

	int check_num = cell_number/100;
    load_num -= check_num;
	CK::MultiThreadAction(&tb, load_num, 1, command::INSERT);

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		cout << "querying..." << endl;
		int check_start = load_num - (check_num/10*(10-i));

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
	int test_thread_num = 1;

	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, test_thread_num);

	int load_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "MapEmbed" << endl;

/******************************** load data **********************************/
	cout << "loading..." << endl;

	int check_num = cell_number/100;
    load_num -= check_num;
	ME::MultiThreadAction(&tb, load_num, 1, command::INSERT);

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		cout << "querying..." << endl;
		int check_start = load_num - (check_num/10*(10-i));

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
	int test_thread_num = 1;
	RACE::RACETable tb(cell_number, test_thread_num);

	int load_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "RACE" << endl;

/******************************** load data **********************************/
	cout << "loading..." << endl;

	int check_num = cell_number/100;
    load_num -= check_num;
	RACE::MultiThreadAction(&tb, load_num, 1, command::INSERT);

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		cout << "querying..." << endl;
		int check_start = load_num - (check_num/10*(10-i));

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
	int test_thread_num = 1;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	int load_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;

	cout << endl << "TEA" << endl;

/******************************** load data **********************************/
	cout << "loading..." << endl;

	int check_num = cell_number/100;
    load_num -= check_num;
	TEA::MultiThreadAction(&tb, load_num, 1, command::INSERT);

/******************************* query data *********************************/
    for (int i = 0; i <= 10; ++i) {
		cout << "querying..." << endl;
		int check_start = load_num - (check_num/10*(10-i));

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

	string csv_path = "partial_query.csv";
	for (int i = 0; i <= 10; ++i) {
		csv_data.push_back({});
		csv_data[i].not_inserted = i*0.1;
	}

	/* Test latency with single client with different non-existent keys
	 * Non-existent keys changes from 0% to 100%
	 */
	test_ck(cell_number);
	send_msg("zero", sock);
	test_me(cell_number);
	send_msg("zero", sock);
	test_race(cell_number);
	send_msg("zero", sock);
	test_tea(cell_number);
	send_msg("zero", sock);

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}