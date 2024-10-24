/* 
 * A test program for throughput at different load factors on large-scale workloads
 * Throughput (Figure 6(b) in Supplementary Materials)
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo_large.h"

/* Struct of result data */
class csv_toulpe {
public:
	int test_thread_num;
	double insert_mops;
	double query_mops;
	double update_mops;
	double delete_mops;
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
		file << item.test_thread_num << ","
			 << item.insert_mops << ","
			 << item.query_mops << ","
			 << item.update_mops << ","
			 << item.delete_mops << "\n";
	}
}

void test_ck(int i, uint64_t cell_number) {
	int test_thread_num = 1<<i;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	uint64_t test_num = cell_number/10*7;

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

	int check_num = cell_number/100;
	test_num -= check_num;
	CK::MultiThreadAction(&tb, test_num, test_thread_num, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, check_num, test_thread_num, command::INSERT, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2); 

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_mops = (double)1000.0 * check_num / resns; 


/******************************* query data *********************************/
    cout << "querying..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, check_num, test_thread_num, command::READ, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_mops = (double)1000.0 * check_num / resns; 

/******************************* update data *********************************/
    cout << "updating..." << endl;
	clock_gettime(CLOCK_MONOTONIC, &time1);

    CK::MultiThreadAction(&tb, check_num, test_thread_num, command::UPDATE, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	update_mops = (double)1000.0 * check_num / resns; 

/******************************* delete data *********************************/
    cout << "deleting..." << endl;
    clock_gettime(CLOCK_MONOTONIC, &time1);

   	CK::MultiThreadAction(&tb, check_num, test_thread_num, command::DELETE, test_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	delete_mops = (double)1000.0 * check_num / resns; 

/******************************* print results *********************************/
	cout << "insert Mops: " << insert_mops << endl;
    cout << "query Mops: " << query_mops << endl;
    cout << "update Mops: " << update_mops << endl;
    cout << "delete Mops: " << delete_mops << endl;

	csv_data.push_back({});
	int j = csv_data.size()-1;

	csv_data[j].test_thread_num = test_thread_num;
	csv_data[j].insert_mops = insert_mops;
	csv_data[j].query_mops = query_mops;
	csv_data[j].update_mops = update_mops;
	csv_data[j].delete_mops = delete_mops;
}

int main(int argc, char **argv) {
	//string load_path = "ycsb_files/load.txt";

	//read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+10000, entry.end());
 
	for (int i = 0; i < 1'000'000'000; ++i) {
		entry.push_back({});
		*(uint64_t*)entry[i].key = i;
	}

	uint64_t cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "large_mops.csv";

	/* Test throughput with different number of threads
	 * Thread number changes from 1 to 16
	 */
	for (int i = 2; i < 5; i += 2) {
		test_ck(i, cell_number);
		send_msg("zero", sock);
	}

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);

	rdma_client_close();

	return 0;
}