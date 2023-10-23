/* 
 * A test program for throughput with different thread number
 * Insert(Figure 9(a))
 * Lookup(Figure 9(b))
 * Update(Figure 9(c))
 * Delete(Figure 9(d))
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
	int test_thread_num;
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

	file << "threads,CuckooDuo,MapEmbed,RACE,TEA\n";
	for (const auto& item : csv_data[i]) {
		file << item.test_thread_num << ","
			 << item.ck_mops << ","
			 << item.me_mops << ","
			 << item.race_mops << ","
			 << item.tea_mops << "\n";
	}
}

void test_ck(int i, int cell_number) {
	int test_thread_num = 1<<i;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int test_num = cell_number/10*9;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "Threads num: " << test_thread_num << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, test_num, test_thread_num, command::INSERT);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * test_num / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, test_num, test_thread_num, command::READ);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * test_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, test_num, test_thread_num, command::UPDATE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * test_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	CK::MultiThreadAction(&tb, test_num, test_thread_num, command::DELETE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * test_num / resns; 

/******************************* print results *********************************/
	cout << "insert Mops: " << insert_mops << endl;
    cout << "query Mops: " << query_mops << endl;
    cout << "update Mops: " << update_mops << endl;
    cout << "delete Mops: " << delete_mops << endl;

	for (int j = 0; j < 4; ++j) {
		csv_data[j].push_back({});
		csv_data[j][i].test_thread_num = test_thread_num;
	}
	csv_data[0][i].ck_mops = insert_mops;
	csv_data[1][i].ck_mops = query_mops;
	csv_data[2][i].ck_mops = update_mops;
	csv_data[3][i].ck_mops = delete_mops;
}

void test_me(int i, int cell_number) {
	int test_thread_num = 1<<i;
	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, test_thread_num);

	int test_num = cell_number/10*9;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "MapEmbed" << endl;
	cout << "Threads num: " << test_thread_num << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    ME::MultiThreadAction(&tb, test_num, test_thread_num, command::INSERT);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * test_num / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    ME::MultiThreadAction(&tb, test_num, test_thread_num, command::READ);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * test_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    ME::MultiThreadAction(&tb, test_num, test_thread_num, command::UPDATE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * test_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	ME::MultiThreadAction(&tb, test_num, test_thread_num, command::DELETE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * test_num / resns; 

/******************************* print results *********************************/
	cout << "insert Mops: " << insert_mops << endl;
    cout << "query Mops: " << query_mops << endl;
    cout << "update Mops: " << update_mops << endl;
    cout << "delete Mops: " << delete_mops << endl;

	csv_data[0][i].me_mops = insert_mops;
	csv_data[1][i].me_mops = query_mops;
	csv_data[2][i].me_mops = update_mops;
	csv_data[3][i].me_mops = delete_mops;
}

void test_race(int i, int cell_number) {
	int test_thread_num = 1<<i;
	RACE::RACETable tb(cell_number, test_thread_num);

	int test_num = cell_number/10*9;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "RACE" << endl;
	cout << "Threads num: " << test_thread_num << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    RACE::MultiThreadAction(&tb, test_num, test_thread_num, command::INSERT);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * test_num / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    RACE::MultiThreadAction(&tb, test_num, test_thread_num, command::READ);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * test_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    RACE::MultiThreadAction(&tb, test_num, test_thread_num, command::UPDATE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * test_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	RACE::MultiThreadAction(&tb, test_num, test_thread_num, command::DELETE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * test_num / resns; 

/******************************* print results *********************************/
	cout << "insert Mops: " << insert_mops << endl;
    cout << "query Mops: " << query_mops << endl;
    cout << "update Mops: " << update_mops << endl;
    cout << "delete Mops: " << delete_mops << endl;

	csv_data[0][i].race_mops = insert_mops;
	csv_data[1][i].race_mops = query_mops;
	csv_data[2][i].race_mops = update_mops;
	csv_data[3][i].race_mops = delete_mops;
}

void test_tea(int i, int cell_number) {
	int test_thread_num = 1<<i;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	int test_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;
	double query_mops = 0;
	double update_mops = 0;
	double delete_mops = 0;

	cout << endl << "TEA" << endl;
	cout << "Threads num: " << test_thread_num << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    TEA::MultiThreadAction(&tb, test_num, test_thread_num, command::INSERT);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * test_num / resns; 

/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    TEA::MultiThreadAction(&tb, test_num, test_thread_num, command::READ);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * test_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    TEA::MultiThreadAction(&tb, test_num, test_thread_num, command::UPDATE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * test_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	TEA::MultiThreadAction(&tb, test_num, test_thread_num, command::DELETE);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * test_num / resns; 

/******************************* print results *********************************/
	cout << "insert Mops: " << insert_mops << endl;
    cout << "query Mops: " << query_mops << endl;
    cout << "update Mops: " << update_mops << endl;
    cout << "delete Mops: " << delete_mops << endl;

	csv_data[0][i].tea_mops = insert_mops;
	csv_data[1][i].tea_mops = query_mops;
	csv_data[2][i].tea_mops = update_mops;
	csv_data[3][i].tea_mops = delete_mops;
}

int main(int argc, char **argv) {
	string load_path = "load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	entry.erase(entry.begin()+1000000, entry.end());

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path[4] = {
		"multi_insert.csv",
		"multi_query.csv",
		"multi_update.csv",
		"multi_delete.csv"
	};

	/* Test throughput with different number of threads
	 * Thread number changes from 1 to 16
	 */
	for (int i = 0; i < 5; ++i) {
		test_ck(i, cell_number);
		send_msg("zero", sock);
		test_me(i, cell_number);
		send_msg("zero", sock);
		test_race(i, cell_number);
		send_msg("zero", sock);
		test_tea(i, cell_number);
		send_msg("zero", sock);
	}

	for (int i = 0; i < 4; ++i)
		writeCSV(csv_path[i], i);

	send_msg("over", sock);
	close(sock);

	rdma_client_close();

	return 0;
}
