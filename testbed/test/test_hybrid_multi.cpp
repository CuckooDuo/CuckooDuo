/* 
 * A test program for throughput of hybrid workloads
 * Throughput (Figure 5(b) in Supplementary Materials)
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
	string ratio;
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

	file << "Insertion/Qurey,CuckooDuo,MapEmbed,RACE,TEA\n";
	for (const auto& item : csv_data) {
		file << item.ratio << ","
			 << item.ck_mops << ","
			 << item.me_mops << ","
			 << item.race_mops << ","
			 << item.tea_mops << "\n";
	}
}

double test_ck(int cell_number) {
	CK::CuckooHashTable tb(cell_number, 3, 16, connect_num);
	cout << "CuckooDuo" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = CK::MultiThreadAction(&tb, load_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = CK::MultiThreadRun(&tb, test_num, 16);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double mops = (double)1000.0 * test_num / resns;
	cout << "Mops: " << mops << endl << endl;

	return mops;
}

double test_me(int cell_number) {
	int MapEmbed_layer = 3;
    int MapEmbed_bucket_number = cell_number / 8;
    int MapEmbed_cell_number[3];
    MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
    MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
    MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
    int MapEmbed_cell_bit = 4;

    ME::MapEmbed tb(MapEmbed_layer, MapEmbed_bucket_number, MapEmbed_cell_number, MapEmbed_cell_bit, 16);
	cout << "MapEmbed" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = ME::MultiThreadAction(&tb, load_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = ME::MultiThreadRun(&tb, test_num, 16);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double mops = (double)1000.0 * test_num / resns;
	cout << "Mops: " << mops << endl << endl;

	return mops;

}

double test_race(int cell_number) {
	RACE::RACETable tb(cell_number, 16);
	cout << "RACE" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = RACE::MultiThreadAction(&tb, load_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = RACE::MultiThreadRun(&tb, test_num, 16);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double mops = (double)1000.0 * test_num / resns;
	cout << "Mops: " << mops << endl << endl;

	return mops;
}

double test_tea(int cell_number) {
	TEA::TEATable tb(cell_number, 1000, 16);
	cout << "TEA" << endl;

	timespec time1, time2;
    long long resns = 0;
	int load_num = entry.size();
	int test_num = fc_entry.size();

	/* We load partial entries at first */
	cout << "loading..." << endl;
	int success_cnt = TEA::MultiThreadAction(&tb, load_num, 16, command::INSERT);

	clock_gettime(CLOCK_MONOTONIC, &time1);

	int success_num = TEA::MultiThreadRun(&tb, test_num, 16);

	clock_gettime(CLOCK_MONOTONIC, &time2); 
	cout << success_num << endl;

	resns = (long long)(time2.tv_sec - time1.tv_sec) * 1000000000LL + (time2.tv_nsec - time1.tv_nsec);

	double mops = (double)1000.0 * test_num / resns;
	cout << "Mops: " << mops << endl << endl;

	return mops;
}

int main(int argc, char **argv) {
	int cell_number, sock;

	string load_path = "ycsb_files/load.txt";

	string run_path;
	string run_prefix = "ycsb_files/run_";
	string run_suffix = ".txt";

	string csv_path = "hybrid_mops.csv";

	/* Test throughput with 16 threads on different hybrid wordloads
	 * Insertion/Query changes from 90/10 to 10/90
	 */
	read_ycsb_load(load_path);
	// load factor: 60%
	entry.erase(entry.begin()+18000000, entry.end());

	for (int i = 10; i < 100; i += 10) {
		cout << endl << "Query/Insertion Ratio: " << i << "/" << 100-i << endl;

		run_path = run_prefix+to_string(i)+run_suffix;
		read_ycsb_run(run_path);

		if (i == 10) {
			cell_number = entry.size()/6*10;
			if (rdma_client_init(argc, argv, cell_number) < 0)
				return 0;
			sleep(1);
			sock = set_tcp_client();
			if (sock <= 0)
				return 0;
		}

		csv_data.push_back({});
		int j = i/10-1;
		csv_data[j].ratio = to_string(i)+"/"+to_string(100-i);
		csv_data[j].ck_mops = test_ck(cell_number);
		send_msg("zero", sock);
		csv_data[j].me_mops = test_me(cell_number);
		send_msg("zero", sock);
		csv_data[j].race_mops = test_race(cell_number);
		send_msg("zero", sock);
		csv_data[j].tea_mops = test_tea(cell_number);
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