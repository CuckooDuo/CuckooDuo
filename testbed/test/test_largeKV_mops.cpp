/* 
 * A test program for throughput at different load factor with large KV length
 * Insert(Figure 8(e) in Supplementary Materials)
 * Lookup(Figure 8(f) in Supplementary Materials)
 * Update(Figure 8(g) in Supplementary Materials)
 * Delete(Figure 8(h) in Supplementary Materials)
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
	double ck_mops;
	double me_mops;
	double race_mops;
	double tea_mops;
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
			 << item.ck_mops << ","
			 << item.me_mops << ","
			 << item.race_mops << ","
			 << item.tea_mops << "\n";
	}
}

void test_ck(int cell_number) {
	int test_thread_num = 16;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	for (int i = 0; i < 9; ++i) {

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	CK::MultiThreadAction(&tb, test_num-last_num, 16, command::INSERT, last_num);

	tb.viscnt = 0;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num) << endl;;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 

	cout << "memory access per insert: " << (double)tb.viscnt/check_num << endl;
/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 16, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * check_num / resns; 
/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << CK::MultiThreadAction(&tb, check_num, 16, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * check_num / resns; 
/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << CK::MultiThreadAction(&tb, check_num, 16, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;
    cout << "query mops: " << query_mops << endl;
    cout << "update mops: " << update_mops << endl;
    cout << "delete mops: " << delete_mops << endl;

	csv_data[0][i].ck_mops = insert_mops;
	csv_data[1][i].ck_mops = query_mops;
	csv_data[2][i].ck_mops = update_mops;
	csv_data[3][i].ck_mops = delete_mops;

	}
}

void test_me(int cell_number) {
	int test_thread_num = 16;

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

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "MapEmbed" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	ME::MultiThreadAction(&tb, test_num-last_num, 16, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);
	int success_cnt = ME::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * success_cnt / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << ME::MultiThreadAction(&tb, check_num, 16, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * check_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << ME::MultiThreadAction(&tb, check_num, 16, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * check_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << ME::MultiThreadAction(&tb, check_num, 16, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;
    cout << "query mops: " << query_mops << endl;
    cout << "update mops: " << update_mops << endl;
    cout << "delete mops: " << delete_mops << endl;

	csv_data[0][i].me_mops = insert_mops;
	csv_data[1][i].me_mops = query_mops;
	csv_data[2][i].me_mops = update_mops;
	csv_data[3][i].me_mops = delete_mops;

	}
}

void test_race(int cell_number) {
	int test_thread_num = 16;
	RACE::RACETable tb(cell_number, test_thread_num);

	for (int i = 0; i < 9; ++i) {

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "RACE" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	RACE::MultiThreadAction(&tb, test_num-last_num, 16, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 16, command::READ, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * check_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    cout << RACE::MultiThreadAction(&tb, check_num, 16, command::UPDATE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * check_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	cout << RACE::MultiThreadAction(&tb, check_num, 16, command::DELETE, test_num) << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;
    cout << "query mops: " << query_mops << endl;
    cout << "update mops: " << update_mops << endl;
    cout << "delete mops: " << delete_mops << endl;

	csv_data[0][i].race_mops = insert_mops;
	csv_data[1][i].race_mops = query_mops;
	csv_data[2][i].race_mops = update_mops;
	csv_data[3][i].race_mops = delete_mops;

	}
}

void test_tea(int cell_number) {	
	int test_thread_num = 16;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	for (int i = 0; i < 9; ++i) {
	
	if (i > 6) {
		csv_data[0][i].tea_mops = NAN;
		csv_data[1][i].tea_mops = NAN;
		csv_data[2][i].tea_mops = NAN;
		csv_data[3][i].tea_mops = NAN;
		continue;
	}

	double test_load_factor = 0.1*(i+1);
	int test_num = cell_number/10*(i+1);
	int last_num = cell_number/10*i;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "TEA" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	int success_cnt;
	TEA::MultiThreadAction(&tb, test_num-last_num, 16, command::INSERT, last_num);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * success_cnt / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 16, command::READ, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * check_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 16, command::UPDATE, test_num);
    cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * check_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = TEA::MultiThreadAction(&tb, check_num, 16, command::DELETE, test_num);
   	cout << success_cnt << endl;

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;
    cout << "query mops: " << query_mops << endl;
    cout << "update mops: " << update_mops << endl;
    cout << "delete mops: " << delete_mops << endl;

	csv_data[0][i].tea_mops = insert_mops;
	csv_data[1][i].tea_mops = query_mops;
	csv_data[2][i].tea_mops = update_mops;
	csv_data[3][i].tea_mops = delete_mops;

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
		"largeKV_2048B_10G_mops_insert.csv",
		"largeKV_2048B_10G_mops_query.csv",
		"largeKV_2048B_10G_mops_update.csv",
		"largeKV_2048B_10G_mops_delete.csv"
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