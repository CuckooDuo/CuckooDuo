/* 
 * A test program for latency at different fingerprint length
 * Lookup (Figure 8(c))
 * Insert (Figure 8(d))
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo_siglen.h"

/* Struct of result data */
class csv_toulpe {
public:
	int sig_bit;
	double query_latency;
	double insert_latency_0;
	double insert_latency_1;
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "SIG_BIT,query,insert_0-70,insert_70\n";
	for (const auto& item : csv_data) {
		file << item.sig_bit << ","
			 << item.query_latency << ","
			 << item.insert_latency_0 << ","
			 << item.insert_latency_1 << "\n";
	}
}

void test_ck(int cell_number, int i) {
	int test_thread_num = 1;
	CK::SIG_BIT = csv_data[i].sig_bit;
	CK::CuckooHashTable tb(cell_number, 3, test_thread_num, connect_num);

	int load_num = cell_number/10*7;

	timespec time1, time2;
    long long resns = 0;

	double query_latency = 0;
	double insert_latency_0 = 0;
	double insert_latency_1 = 0;

	cout << endl << "CuckooDuo" << endl;
	cout << "SIG_BIT: " << CK::SIG_BIT << endl;

/******************************** load data **********************************/
	cout << "loading..." << endl;

	int check_num = cell_number/100;
    load_num -= check_num;
	int success_cnt;

	clock_gettime(CLOCK_MONOTONIC, &time1);

	success_cnt = CK::MultiThreadAction(&tb, load_num, 1, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency_0 = (double)(resns/1000) / load_num; 
	cout << success_cnt << endl;

/******************************** insert data **********************************/
	cout << "inserting..." << endl;

	clock_gettime(CLOCK_MONOTONIC, &time1);

    success_cnt = CK::MultiThreadAction(&tb, check_num, 1, command::INSERT, load_num);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	insert_latency_1 = (double)(resns/1000) / check_num; 
	cout << success_cnt << endl;

	load_num += check_num;

/******************************* query data *********************************/
	cout << "querying..." << endl;
	check_num = cell_number/5;
	int check_start = load_num - (check_num/10);

	clock_gettime(CLOCK_MONOTONIC, &time1);

    success_cnt = CK::MultiThreadAction(&tb, check_num, 1, command::READ, check_start);

    clock_gettime(CLOCK_MONOTONIC, &time2);

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	query_latency = (double)(resns/1000) / check_num; 
	cout << success_cnt << endl;

/******************************* print results *********************************/
	insert_latency_0 = (insert_latency_0*69+insert_latency_1)/70;

	cout << "stash size: " << tb.stash_num << endl;
	cout << "query latency: " << query_latency << endl;
	cout << "insert latency 0~70: " << insert_latency_0 << endl;
	cout << "insert latency 70: " << insert_latency_1 << endl;

	csv_data[i].query_latency += query_latency;
	csv_data[i].insert_latency_0 += insert_latency_0;
	csv_data[i].insert_latency_1 += insert_latency_1;

}


int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);
	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+100000, entry.end());
	/*for (int i = 0; i < 30'000'000; ++i) {
		entry.push_back({});
		*(uint64_t*)entry[i].key = i+1;
		*(uint64_t*)entry[i].val = i+1;
	}*/

	int cell_number = entry.size();
	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	int sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	string csv_path = "bpk_latency.csv";
	/* Test latency with single client with different siglens
	 * Siglen changes from 10 to 20
	 */
	for (int i = 0; i <= 10; ++i) {
		csv_data.push_back({});
	}
	for (int i = 0; i <= 10; ++i) {
		csv_data[i].sig_bit = 10+i;

		csv_data[i].insert_latency_0 = 0;
		csv_data[i].insert_latency_1 = 0;
		csv_data[i].query_latency = 0;

		test_ck(cell_number, i);
		send_msg("zero", sock);
	}


	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}