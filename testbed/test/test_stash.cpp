/* 
 * A test program for query latency with different stash sizes
 * Query with different stash sizes (Figure 7(k))
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo.h"

/* Struct of result data */
class csv_toulpe {
public:
	int stash_size;
	double load_factor;
	double positive_latency;
	double negative_latency;
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "stash_size,load_factor,positive,negative\n";
	for (const auto& item : csv_data) {
		file << item.stash_size << ","
			 << item.load_factor << ","
			 << item.positive_latency << ","
			 << item.negative_latency << "\n";
	}
}

void test_ck(int cell_number) {
	int test_thread_num = 1;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int load_num = cell_number;

	timespec time1, time2;
    long long resns = 0;

	double positive_latency;
	double negative_latency;

	int check_num = cell_number/100;
	int success_cnt = 0;

/******************************** insert data **********************************/
    for (int i = 0; i <= 9; ++i) {
		cout << endl << "CuckooDuo" << endl;

		cout << "inserting..." << endl;

		CK::test_stash_size = csv_data[i].stash_size;

		load_num =  cell_number - success_cnt;
		success_cnt += CK::MultiThreadAction(&tb, load_num, 1, command::INSERT, success_cnt);
		cout << "LF: " << success_cnt/(double)cell_number << endl;

/******************************* query data *********************************/

		cout << "querying..." << endl;
		int check_start = success_cnt-check_num;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << CK::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		positive_latency = (double)(resns/1000) / check_num; 

/******************************* query data *********************************/

		cout << "querying..." << endl;
		check_start = success_cnt;

		clock_gettime(CLOCK_MONOTONIC, &time1);

    	cout << CK::MultiThreadAction(&tb, check_num, 1, command::READ, check_start) << endl;

    	clock_gettime(CLOCK_MONOTONIC, &time2);

		resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
		negative_latency = (double)(resns/1000) / check_num; 

/******************************* print results *********************************/
    	cout << "positive query latency: " << positive_latency << endl;
		cout << "negative query latency: " << negative_latency << endl;
		cout << "stash_size: " << tb.stash.size() << endl;

		csv_data[i].load_factor = success_cnt/(double)cell_number;
		csv_data[i].positive_latency = positive_latency;
		csv_data[i].negative_latency = negative_latency;
	}
}

int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+100000, entry.end());
	/*for (int i = 0; i < 30'000'000; ++i) {
		entry.push_back({});
		*(uint64_t*)entry[i].key = i;
		*(uint64_t*)entry[i].val = i;
	}*/

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "test_stash.csv";
	for (int i = 0; i <= 9; ++i) {
		csv_data.push_back({});
		csv_data[i].stash_size = ((i-1)<0)?0:(1<<(i-1));
	}

	/* Test latency with single client with different load factor
	 * Load factor changes from 0.1 to 0.9
	 */
	test_ck(cell_number);

	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}
