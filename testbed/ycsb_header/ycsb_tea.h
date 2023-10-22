/*
 * Functions to work with YCSB datasets on TEA
 */
#include <future>

#include "../algorithms/tea.h"
#include "ycsb_read.h"

namespace TEA {
/* Excute commands with entries in type defined by cmd with thread tid */
int inline ExecuteCmd(TEA::TEATable *tb, int num_ops, command cmd, int tid) {
	int oks = 0;
	int offset = num_ops*tid;
	
	tid = connect_num-1-tid;

	switch (cmd)
	{
	case INSERT:
		for (int i = 0; i < num_ops; ++i) {
			if (tb->insert(entry[offset+i], tid))
				oks += 1;
			else
				break;
		}
		break;
	case READ:
		for (int i = 0; i < num_ops; ++i)
			oks += tb->query(entry[offset+i].key, NULL, tid);
		break;
	case UPDATE:
		for (int i = 0; i < num_ops; ++i)
			oks += tb->update(entry[offset+i], tid);
		break;
	case DELETE:
		for (int i = 0; i < num_ops; ++i)
			oks += tb->deletion(entry[offset+i].key, tid);
		break;
	default:
		break;
	}

	return oks;
}

/* Excute commands with entries in type defined by cmd on multiple threads */
int inline MultiThreadAction(TEA::TEATable *tb, int total_ops, int num_threads, command cmd) {
	int ret = -1;

	vector<future<int>> actual_ops;

	int part_ops = total_ops/num_threads;

	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(async(launch::async, ExecuteCmd,
									  tb, part_ops, cmd, i));

	}

	cout << actual_ops.size() << " " << num_threads << endl;

	if ((int)actual_ops.size() != num_threads) {
		ret = -1;
		return ret;
	}

	int sum = 0;
	for (auto &n : actual_ops) {
		if (n.valid()) {
			sum += n.get();
		}
		else {
			ret = -1;
			return ret;
		}
	}

	ret = sum;

	return ret;
}

/* Seperate types and entries of commands paired in sequence */
struct full_cmd_t {
	vector<command> *cmd;
	vector<Entry> *entry;

	full_cmd_t(vector<command> *c, vector<Entry> *e) {
		cmd = c;
		entry = e;
	}
};

/* Excute full commands with thread tid */
int inline ExecuteFullCmd(TEA::TEATable *tb, int num_ops, full_cmd_t *fc, int tid) {
	int oks = 0;
	int offset = num_ops*tid;
	
	tid = connect_num-1-tid;

	bool insert_success = true;

	for (int i = 0; i < num_ops; ++i) {
		switch((*(fc->cmd))[offset+i]) {
			case INSERT:
				if (!insert_success)
					break;
				insert_success = tb->insert((*(fc->entry))[offset+i], tid);
				oks += insert_success;
				//oks += tb->insert((*(fc->entry))[offset+i], tid);
				break;
			case READ:
				oks += tb->query((*(fc->entry))[offset+i].key, NULL, tid);
				break;
			default:
				break;
		}
	}

	return oks;
}

/* Excute full commands on multiple threads */
int inline MultiThreadRun(TEA::TEATable *tb, full_cmd_t *fc, int num_threads) {
	int ret = -1;

	vector<future<int>> actual_ops;

	cout << "running..." << endl;
	
	int total_ops = full_command.size(); // The count of YCSB commands
	int part_ops = total_ops/num_threads;

	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(async(launch::async, ExecuteFullCmd,
									  tb, part_ops, fc, i));

	}

	cout << actual_ops.size() << " " << num_threads << endl;

	if ((int)actual_ops.size() != num_threads) {
		ret = -1;
		return ret;
	}

	int sum = 0;
	for (auto &n : actual_ops) {
		if (n.valid()) {
			sum += n.get();
		}
		else {
			ret = -1;
			return ret;
		}
	}

	ret = sum;

	return ret;
}

}