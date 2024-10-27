/* 
 * A test program for latency at different load factor with large KV length
 * Insert(Figure 8(a) in Supplementary Materials)
 * Lookup(Figure 8(b) in Supplementary Materials)
 * Update(Figure 8(c) in Supplementary Materials)
 * Delete(Figure 8(d) in Supplementary Materials)
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo_largeKV.h"
#include "../ycsb_header/ycsb_mapembed_largeKV.h"
#include "../ycsb_header/ycsb_race_largeKV.h"
#include "../ycsb_header/ycsb_tea_largeKV.h"

/* Struct of result data */
class csv_toulpe {
public:
	double test_load_factor;
	double ck_latency;
	double me_latency;
	double race_latency;
	double tea_latency;
};
vector<csv_toulpe> csv_data[4];

/* Write result to csv file */
void writeCSV(const string& filename, int i) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "load_factor,CuckooDuo,MapEmbed,RACE,TEA\n";
	for (const auto& item : csv_data[i]) {
		file << item.test_load_factor << ","
			 << item.ck_latency << ","
			 << item.me_latency << ","
			 << item.race_latency << ","
			 << item.tea_latency << "\n";
	}
}

void test_ck(int cell_number) {
	int test_thread_num = 1;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	for (int i = 0; i < 9; ++i) {

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_latency = 0;
	double query_latency = 0;
	double update_latency = 0;
	double delete_latency = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	CK::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

	tb.viscnt = 0;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num) << endl;;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency = (double)(resns/1000) / check_num; 

	cout << "memory access per insert: " << (double)tb.viscnt/check_num << endl;
/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 1, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_latency = (double)(resns/1000) / check_num; 
/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 1, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_latency = (double)(resns/1000) / check_num; 
/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << CK::MultiThreadAction(&tb, check_num, 1, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
	cout << "insert latency: " << insert_latency << endl;
    cout << "query latency: " << query_latency << endl;
    cout << "update latency: " << update_latency << endl;
    cout << "delete latency: " << delete_latency << endl;

	csv_data[0][i].ck_latency = insert_latency;
	csv_data[1][i].ck_latency = query_latency;
	csv_data[2][i].ck_latency = update_latency;
	csv_data[3][i].ck_latency = delete_latency;

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

	for (int i = 0; i < 9; ++i) {

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_latency = 0;
	double query_latency = 0;
	double update_latency = 0;
	double delete_latency = 0;

	cout << endl << "MapEmbed" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	ME::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);
	
    cout << ME::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency = (double)(resns/1000) / check_num; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << ME::MultiThreadAction(&tb, check_num, 1, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_latency = (double)(resns/1000) / check_num; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << ME::MultiThreadAction(&tb, check_num, 1, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_latency = (double)(resns/1000) / check_num; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << ME::MultiThreadAction(&tb, check_num, 1, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
	cout << "insert latency: " << insert_latency << endl;
    cout << "query latency: " << query_latency << endl;
    cout << "update latency: " << update_latency << endl;
    cout << "delete latency: " << delete_latency << endl;

	csv_data[0][i].me_latency = insert_latency;
	csv_data[1][i].me_latency = query_latency;
	csv_data[2][i].me_latency = update_latency;
	csv_data[3][i].me_latency = delete_latency;

	}
}

void test_race(int cell_number) {
	int test_thread_num = 1;
	RACE::RACETable tb(cell_number, test_thread_num);

	for (int i = 0; i < 9; ++i) {

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_latency = 0;
	double query_latency = 0;
	double update_latency = 0;
	double delete_latency = 0;

	cout << endl << "RACE" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	RACE::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency = (double)(resns/1000) / check_num; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 1, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_latency = (double)(resns/1000) / check_num; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 1, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_latency = (double)(resns/1000) / check_num; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << RACE::MultiThreadAction(&tb, check_num, 1, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
	cout << "insert latency: " << insert_latency << endl;
    cout << "query latency: " << query_latency << endl;
    cout << "update latency: " << update_latency << endl;
    cout << "delete latency: " << delete_latency << endl;

	csv_data[0][i].race_latency = insert_latency;
	csv_data[1][i].race_latency = query_latency;
	csv_data[2][i].race_latency = update_latency;
	csv_data[3][i].race_latency = delete_latency;

	}
}

void test_tea(int cell_number) {	
	int test_thread_num = 1;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	for (int i = 0; i < 9; ++i) {
	
	if (i > 6) {
		csv_data[0][i].tea_latency = NAN;
		csv_data[1][i].tea_latency = NAN;
		csv_data[2][i].tea_latency = NAN;
		csv_data[3][i].tea_latency = NAN;
		continue;
	}

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_latency = 0;
	double query_latency = 0;
	double update_latency = 0;
	double delete_latency = 0;

	cout << endl << "TEA" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	int success_cnt;
	TEA::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency = (double)(resns/1000) / success_cnt; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 1, command::READ, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_latency = (double)(resns/1000) / success_cnt; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 1, command::UPDATE, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_latency = (double)(resns/1000) / success_cnt; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 1, command::DELETE, test_num);
   	cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_latency = (double)(resns/1000) / success_cnt; 

/******************************* print results *********************************/
	cout << "insert latency: " << insert_latency << endl;
    cout << "query latency: " << query_latency << endl;
    cout << "update latency: " << update_latency << endl;
    cout << "delete latency: " << delete_latency << endl;

	csv_data[0][i].tea_latency = insert_latency;
	csv_data[1][i].tea_latency = query_latency;
	csv_data[2][i].tea_latency = update_latency;
	csv_data[3][i].tea_latency = delete_latency;

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

	string csv_path[4] = {
		"largeKV_2048B_10G_latency_insert.csv",
		"largeKV_2048B_10G_latency_query.csv",
		"largeKV_2048B_10G_latency_update.csv",
		"largeKV_2048B_10G_latency_delete.csv"
	};

	for (int i  = 0; i < 9; ++i) {
		for (int j = 0; j < 4; ++j) {
			csv_data[j].push_back({});
			csv_data[j][i].test_load_factor = 0.1*(i+1);
		}
	}

	/* Test latency with single client with different load factor
	 * Load factor changes from 0.1 to 0.9
	 */
	//for (int i = 0; i < 9; i += 1) {
		test_ck(cell_number);
		send_msg("zero", sock);
		test_me(cell_number);
		send_msg("zero", sock);
		test_race(cell_number);
		send_msg("zero", sock);
		test_tea(cell_number);
		send_msg("zero", sock);
	//}

	for (int i = 0; i < 4; ++i)
		writeCSV(csv_path[i], i);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}