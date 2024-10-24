/* 
 * A test program for YCSB-A workloads (50% Lookup & 50% Update)
 * Zipf-0.99, Zipf-0.90, Uniform, Latest
 * (Upper part of Table Ⅲ)
 */
#include <iostream>
#include <cmath>

#include "../ycsb_header/ycsb_cuckooduo.h"
#include "../ycsb_header/ycsb_mapembed.h"
#include "../ycsb_header/ycsb_race.h"
#include "../ycsb_header/ycsb_tea.h"

#define CACHE_BYTES (65 * 1024 * 1024)

/* Struct of result data */
class csv_toulpe {
public:
	string distribution;
	double ck_latency[2];
	double me_latency[2];
	double race_latency[2];
	double tea_latency[2];
};
vector<csv_toulpe> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "Distribution,"
		 << "CK0,ME0,RACE0,TEA0,"
		 << "CK1,ME1,RACE1,TEA1"
		 << "\n";
	for (const auto& item : csv_data) {
		file << item.distribution << ","
			 << item.ck_latency[0] << ","
			 << item.me_latency[0] << ","
			 << item.race_latency[0] << ","
			 << item.tea_latency[0] << ","
			 << item.ck_latency[1] << ","
			 << item.me_latency[1] << ","
			 << item.race_latency[1] << ","
			 << item.tea_latency[1] << "\n";
	}
}

double test_ck(int cell_number, bool cacheFlag, int i) {
	if (cacheFlag)
		CK::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;
	else
		CK::TOTAL_MEMORY_BYTE_USING_CACHE = 0;

	CK::CuckooHashTable tb(cell_number, 3, 1, connect_num);
	cout << "CuckooDuo" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;

	clock_gettime(CLOCK_MONOTONIC, &time1);
	CK::MultiThreadAction(&tb, load_num, 1, command::INSERT);
	clock_gettime(CLOCK_MONOTONIC, &time2); 
	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);
	cout << (double)(resns/1000)/load_num << endl;

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = CK::MultiThreadRun(&tb, test_num, 1);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double latency = (double)(resns/1000) / test_num;
	cout << "Latency: " << latency << endl << endl;

	return latency;
}

double test_me(int cell_number, bool cacheFlag, int i) {
	if (cacheFlag)
		ME::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;
	else
		ME::TOTAL_MEMORY_BYTE_USING_CACHE = 0;

	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, 1);
	cout << "MapEmbed" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = ME::MultiThreadAction(&tb, load_num, 1, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = ME::MultiThreadRun(&tb, test_num, 1);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double latency = (double)(resns/1000) / test_num;
	cout << "Latency: " << latency << endl << endl;

	return latency;

}

double test_race(int cell_number, bool cacheFlag, int i) {
	if (cacheFlag)
		RACE::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;
	else
		RACE::TOTAL_MEMORY_BYTE_USING_CACHE = 0;

	RACE::RACETable tb(cell_number, 1);
	cout << "RACE" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = RACE::MultiThreadAction(&tb, load_num, 1, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = RACE::MultiThreadRun(&tb, test_num, 1);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double latency = (double)(resns/1000) / test_num;
	cout << "Latency: " << latency << endl << endl;

	return latency;
}

double test_tea(int cell_number, bool cacheFlag, int i) {
	if (cacheFlag)
		TEA::TOTAL_MEMORY_BYTE_USING_CACHE = CACHE_BYTES;
	else
		TEA::TOTAL_MEMORY_BYTE_USING_CACHE = 0;

	TEA::TEATable tb(cell_number, 1000, 1);
	cout << "TEA" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = TEA::MultiThreadAction(&tb, load_num, 1, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = TEA::MultiThreadRun(&tb, test_num, 1);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double latency = (double)(resns/1000) / test_num;
	cout << "Latency: " << latency << endl << endl;

	return latency;
}

int main(int argc, char **argv) {
	int cell_number, sock;

	string load_path = "ycsb_files_cache/load_workloada.txt";
	// string load_path = "ycsb_files/load.txt";

	string run_path[4] = {
		"ycsb_files_cache/run_workloada_uniform.txt",
		"ycsb_files_cache/run_workloada_latest.txt",
		"ycsb_files_cache/run_workloada_ycsb_zipf.txt",
		"ycsb_files_cache/run_workloada_our_zipf090.txt"
	};

	string csv_path = "test_workloada.csv";

	read_ycsb_load(load_path);
	// if using the full load.txt, can run like below
	//entry.erase(entry.begin()+entry.size()/2, entry.end());
	cell_number = entry.size()*2;

	if (rdma_client_init(argc, argv, cell_number) < 0)
		return 0;
	sleep(1);
	sock = set_tcp_client();
	if (sock <= 0)
		return 0;

	for (int i = 0; i < 4; ++i)
		csv_data.push_back({});
	csv_data[0].distribution = "uniform";
	csv_data[1].distribution = "latest";
	csv_data[2].distribution = "zipfian099";
	csv_data[3].distribution = "zipfian090";

	for (int i = 0; i < 4; ++i) {
	
		if (i == 3) {
			for (int j = 0; j < entry.size(); ++j) {
				*(uint64_t*)entry[j].key = j+1;
				*(uint64_t*)entry[j].val = j+1;
			}
		}		

		read_ycsb_run(run_path[i]);

		// 0 for no cache, 1 for cache
		cout << endl << csv_data[i].distribution << endl;
		csv_data[i].ck_latency[0] = test_ck(cell_number, false, i);
		send_msg("zero", sock);
		csv_data[i].ck_latency[1] = test_ck(cell_number, true, i);
		send_msg("zero", sock);
		csv_data[i].me_latency[0] = test_me(cell_number, false, i);
		send_msg("zero", sock);
		csv_data[i].me_latency[1] = test_me(cell_number, true, i);
		send_msg("zero", sock);
		csv_data[i].race_latency[0] = test_race(cell_number, false, i);
		send_msg("zero", sock);
		csv_data[i].race_latency[1] = test_race(cell_number, true, i);
		send_msg("zero", sock);
		csv_data[i].tea_latency[0] = test_tea(cell_number, false, i);
		send_msg("zero", sock);
		csv_data[i].tea_latency[1] = test_tea(cell_number, true, i);
		send_msg("zero", sock);

		//entry.clear();
		fc_cmd.clear();
		fc_entry.clear();
	}


	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);
	rdma_client_close();

	return 0;
}