/* 
 * A test program for latency at different load factors on large-scale workloads
 * Latency (Figure 6(a) in Supplementary Materials)
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo_large.h"

/* Struct of result data */
class csv_toulpe {
public:
	double test_load_factor;
	double insert_latency;
	double query_latency;
	double update_latency;
	double delete_latency;
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "load_factor,insert,query,update,delete\n";
	for (const auto& item : csv_data) {
		file << item.test_load_factor << ","
			 << item.insert_latency << ","
			 << item.query_latency << ","
			 << item.update_latency << ","
			 << item.delete_latency << "\n";
	}
}

void test_ck(CK::CuckooHashTable &tb, int cell_number, int i) {
	double test_load_factor = 0.1*(i+1);
	
	int test_num = cell_number/10*(i+1);
	int check_num = cell_number/100;
	test_num -= check_num;
	int tmp = 2;
	int last_num = max(cell_number/10*(i-tmp)-check_num, 0);

	timespec time1, time2;
    long long resns = 0;

	double insert_latency = 0;
	double query_latency = 0;
	double update_latency = 0;
	double delete_latency = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "last num: " << last_num << endl;
	cout << "test num: " << test_num << endl;
	cout << "check num: " << check_num << endl;
	cout << "Load factor at: " << test_load_factor << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

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

    CK::MultiThreadAction(&tb, check_num, 1, command::UPDATE, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_latency = (double)(resns/1000) / check_num; 
/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	CK::MultiThreadAction(&tb, check_num, 1, command::DELETE, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
	cout << "insert latency: " << insert_latency << endl;
    cout << "query latency: " << query_latency << endl;
    cout << "update latency: " << update_latency << endl;
    cout << "delete latency: " << delete_latency << endl;

	csv_data.push_back({});
	int j = csv_data.size()-1;
	csv_data[j].test_load_factor = test_load_factor;
	csv_data[j].insert_latency = insert_latency;
	csv_data[j].query_latency = query_latency;
	csv_data[j].update_latency = update_latency;
	csv_data[j].delete_latency = delete_latency;
}


int main(int argc, char **argv) {
	/*string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);*/
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+100000, entry.end());
	for (int i = 0; i < 1'000'000'000; ++i) {
		entry.push_back({});
		*(uint64_t*)entry[i].key = i;
	}

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "large_latency.csv";

	int test_thread_num = 1;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	/* Test latency with single client with different load factor
	 * Load factor changes from 0.1 to 0.9
	 */
	for (int i = 2; i < 9; i += 3) {
		test_ck(tb, cell_number, i);
	}

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}