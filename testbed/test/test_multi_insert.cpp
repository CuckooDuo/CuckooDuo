/* 
 * A test program for insert throughput with 16 threads at different load factors
 * Insert throughput (Figure 10(g))
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
	double test_load_factor;
	double ck_mops;
	double me_mops;
	double race_mops;
	double tea_mops;
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "load_factor,CuckooDuo,MapEmbed,RACE,TEA\n";
	for (const auto& item : csv_data) {
		file << item.test_load_factor << ","
			 << item.ck_mops << ","
			 << item.me_mops << ","
			 << item.race_mops << ","
			 << item.tea_mops << "\n";
	}
}

void test_ck(int i, int cell_number) {
	double test_load_factor = 0.1*(i+1);
	int test_thread_num = 16;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int test_num = cell_number/10*(i+1);

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	CK::MultiThreadAction(&tb, test_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;

	csv_data.push_back({});
	csv_data[i].test_load_factor = test_load_factor;
	
	csv_data[i].ck_mops = insert_mops;
}

void test_me(int i, int cell_number) {
	double test_load_factor = 0.1*(i+1);
	int test_thread_num = 16;

	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, test_thread_num);

	int test_num = cell_number/10*(i+1);

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;

	cout << endl << "MapEmbed" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	ME::MultiThreadAction(&tb, test_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);
	
    ME::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;

	csv_data[i].me_mops = insert_mops;
}

void test_race(int i, int cell_number) {
	double test_load_factor = 0.1*(i+1);
	int test_thread_num = 16;
	RACE::RACETable tb(cell_number, test_thread_num);

	int test_num = cell_number/10*(i+1);

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;

	cout << endl << "RACE" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	RACE::MultiThreadAction(&tb, test_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    RACE::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns;  

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;

	csv_data[i].race_mops = insert_mops;
}

void test_tea(int i, int cell_number) {
	if (i > 6) {
		csv_data[i].tea_mops = NAN;
		return;
	}

	double test_load_factor = 0.1*(i+1);
	int test_thread_num = 16;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	int test_num = cell_number/10*(i+1);

	timespec time1, time2;
    long long resns = 0;

	double insert_mops = 0;

	cout << endl << "TEA" << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	int check_num = cell_number/100;
	test_num -= check_num;
	TEA::MultiThreadAction(&tb, test_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    TEA::MultiThreadAction(&tb, check_num, 16, command::INSERT, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert mops: " << insert_mops << endl;
	
	csv_data[i].tea_mops = insert_mops;
}

int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+10000000, entry.end());

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "multi_insert_lf.csv";
		
	/* Test throughput with 16 clients with different load factors
	 * Load factor changes from 0.1 to 0.9
	 */
	for (int i = 0; i < 9; i += 1) {
		test_ck(i, cell_number);
		send_msg("zero", sock);
		test_me(i, cell_number);
		send_msg("zero", sock);
		test_race(i, cell_number);
		send_msg("zero", sock);
		test_tea(i, cell_number);
		send_msg("zero", sock);
	}

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}