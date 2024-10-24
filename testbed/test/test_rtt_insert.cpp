/* 
 * A test program for insert rtts at different load factor
 * Insert rtts in (Figure 9(b))
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
	double ck_rtt;
	double me_rtt;
	double race_rtt;
	double tea_rtt;
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
			 << item.ck_rtt << ","
			 << item.me_rtt << ","
			 << item.race_rtt << ","
			 << item.tea_rtt << "\n";
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

		double insert_rtt = 0;

		cout << endl << "CuckooDuo" << endl;
		cout << "Load factor at: " << test_load_factor << endl;

	/******************************** insert data **********************************/
		cout << "inserting..." << endl;

		int check_num = cell_number/100;
		test_num -= check_num;
		CK::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

		tb.viscnt = 0;
		clock_gettime(CLOCK_MONOTONIC, &time1);

	    CK::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num);

	    clock_gettime(CLOCK_MONOTONIC, &time2); 

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		insert_rtt = tb.viscnt/(double)check_num;
	/******************************* print results *********************************/
		cout << "insert rtt: " << insert_rtt << endl;

		csv_data[i].test_load_factor = test_load_factor;
		csv_data[i].ck_rtt = insert_rtt;
	}

	CK::MultiThreadAction(&tb, cell_number/10, 1, command::INSERT, cell_number/10*9);
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

		double insert_rtt = 0;

		cout << endl << "MapEmbed" << endl;
		cout << "Load factor at: " << test_load_factor << endl;

	/******************************** insert data **********************************/
		cout << "inserting..." << endl;

		int check_num = cell_number/100;
		test_num -= check_num;
		ME::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

		tb.viscnt = 0;
		clock_gettime(CLOCK_MONOTONIC, &time1);

	    int success_cnt = ME::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num);

	    clock_gettime(CLOCK_MONOTONIC, &time2); 

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		insert_rtt = tb.viscnt/(double)success_cnt; 

	/******************************* print results *********************************/
		cout << "insert rtt: " << insert_rtt << endl;
		csv_data[i].me_rtt = tb.vismax;
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

		double insert_rtt = 0;

		cout << endl << "RACE" << endl;
		cout << "Load factor at: " << test_load_factor << endl;

	/******************************** insert data **********************************/
		cout << "inserting..." << endl;

		int check_num = cell_number/100;
		test_num -= check_num;
		RACE::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

		tb.viscnt = 0;
		clock_gettime(CLOCK_MONOTONIC, &time1);

	    RACE::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num);

	    clock_gettime(CLOCK_MONOTONIC, &time2); 

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		insert_rtt = tb.viscnt/(double)check_num;

	/******************************* print results *********************************/
		cout << "insert rtt: " << insert_rtt << endl;
		csv_data[i].race_rtt = tb.vismax;
	}
	
	RACE::MultiThreadAction(&tb, cell_number/10, 1, command::INSERT, cell_number/10*9);
}

void test_tea(int cell_number) {
	int test_thread_num = 1;
	TEA::TEATable tb(cell_number, 1000, test_thread_num);

	for (int i = 0; i < 9; ++i) {
		if (i > 6) {
			csv_data[i].tea_rtt = NAN;
			continue;
		}

		double test_load_factor = 0.1*(i+1);

		int test_num = cell_number/10*(i+1);
		int last_num = cell_number/10*i;

		timespec time1, time2;
	    long long resns = 0;

		double insert_rtt = 0;

		cout << endl << "TEA" << endl;
		cout << "Load factor at: " << test_load_factor << endl;

	/******************************** insert data **********************************/
		cout << "inserting..." << endl;

		int check_num = cell_number/100;
		test_num -= check_num;
		TEA::MultiThreadAction(&tb, test_num-last_num, 1, command::INSERT, last_num);

		tb.viscnt = 0;
		clock_gettime(CLOCK_MONOTONIC, &time1);

	    int success_cnt = TEA::MultiThreadAction(&tb, check_num, 1, command::INSERT, test_num);

	    clock_gettime(CLOCK_MONOTONIC, &time2); 

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		insert_rtt = tb.viscnt/(double)success_cnt; 

	/******************************* print results *********************************/
		cout << "insert rtt: " << insert_rtt << endl;
		csv_data[i].tea_rtt = tb.vismax;
	}
}

int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+10000000, entry.end());
	/*for (int i = 0; i < 30000000; ++i) {
		entry.push_back({});
		*(uint64_t*)entry[i].key = i;
	}*/

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "insert_rtt.csv";

	/* Test rtts with single client with different load factor
	 * Load factor changes from 0.1 to 0.9
	 */
	for (int i = 0; i < 9; ++i)
		csv_data.push_back({});

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
