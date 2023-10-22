/* 
 * A test program for expansion(Figure 12(b))
 */
#include "../ycsb_header/ycsb_cuckooduo.h"

#include <iostream>

using namespace std;

/* Struct of result data */
class csv_touple {
public:
	string size;
	double t1;
	double t2;

	csv_touple(string s, double t1_, double t2_) {
		size = s;
		t1 = t1_;
		t2 = t2_;
	}
};
vector<csv_touple> csv_data;

/* Write result to csv file */
void writeCSV(const string& filename) {
	ofstream file(filename);
	if (!file) {
        std::cerr << "Error opening file: " << filename << std::endl;
        return;
    }

	file << "Size,Copy,Copy&Erase\n";
	for (const auto& item : csv_data) {
		file << item.size << ","
			 << item.t1 << ","
			 << item.t2 << "\n";
	}
}

int main(int argc, char **argv) {
	string load_path = "ycsb_files/load.txt";

	read_ycsb_load(load_path);

	/* You can use this to get a minor partial dataset */
	//entry.erase(entry.begin()+6400000, entry.end());

	int cell_number = entry.size()/8;
	int max_kick_num = 3;
	int thread_num = 1;
	int connect_num = 1;
	rdma_client_init(argc, argv, cell_number);
	sleep(1);

	CK::CuckooHashTable tb(cell_number, max_kick_num, thread_num, connect_num);

	int sock = set_tcp_client();
	if (sock <= 0) {
		return 0;
	}

	/* Insert 1/8 with load factor at 0.9 at first */
	int success_cnt = 0;
	success_cnt = CK::MultiThreadAction(&tb, cell_number/10*9, thread_num, command::INSERT);
	entry.erase(entry.begin(), entry.begin()+cell_number/10*9);
	cout << "1/8 inserted" << endl;
	cout << "load_factor at: " << double(success_cnt)/cell_number << endl;

	pair<long long, long long> t;
	for (int i = 0; i < 4; ++i) {
		/* Expand to double once, from 1/8 to 2 */
		t = tb.expand(2, sock);
		cout << t.first << " " << t.second << endl;
		char info[32];
		sprintf(info, "%.2lfM to %.2lfM", ((double)(cell_number<<i))/1000000, ((double)((cell_number<<(i+1)))/1000000));
		cout << info << endl;
		csv_data.push_back(csv_touple(
			string(info),
			double(t.first)/1e6,
			double(t.first+t.second)/1e6
		));
		
		/* Insert with load factor at 0.9 after expansion */
		if (i < 3) {
			cout << (1<<(i+1))-(1<<i) << endl;
			success_cnt += CK::MultiThreadAction(&tb, cell_number*(1<<i)/10*9, thread_num, command::INSERT);
			entry.erase(entry.begin(), entry.begin()+cell_number*(1<<i)/10*9);
			cout << "1/" << (8>>(i+1)) << " inserted" << endl;
			cout << "load_factor at: " << double(success_cnt)/(cell_number*(1<<(i+1))) << endl;
		}

		cout << endl;
	}

	string csv_path = "test_expand.csv";
	writeCSV(csv_path);

	send_msg("over", sock);
	close(sock);

	rdma_client_close();
	return 0;
}