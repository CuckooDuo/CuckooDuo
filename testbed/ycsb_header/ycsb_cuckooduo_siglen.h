/*
 * Functions to work with YCSB datasets on CuckooDuo
 */
#include <future>

#include "../algorithms/cuckooduo_siglen.h"
#include "ycsb_read.h"

namespace CK {
/* Excute commands with entries in type defined by cmd with thread tid */
int inline ExecuteCmd(CK::CuckooHashTable *tb, int num_ops, command cmd, int tid, int start_loc = 0) {
	int oks = 0;
	int offset = num_ops*tid+start_loc;
	
	tid = connect_num-1-tid;

	switch (cmd)
	{
	case QINSERT:
		for (int i = 0; i < num_ops; ++i)
			oks += tb->checkInsert(entry[offset+i], tid);
		break;
	case INSERT:
		for (int i = 0; i < num_ops; ++i) {
			if (tb->insert(entry[offset+i], tid))
				oks += 1;
			else
				break;
			/*if (oks % 100000 == 0)
				printf("%d: %d\n", connect_num-1-tid, oks);*/
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
int inline MultiThreadAction(CK::CuckooHashTable *tb, int total_ops, int num_threads, command cmd, int start_loc = 0) {
	int ret = -1;

	vector<future<int>> actual_ops;	

	int part_ops = total_ops/num_threads;

	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(async(launch::async, ExecuteCmd,
									  tb, part_ops, cmd, i, start_loc));

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

/* Excute full commands with thread tid */
int inline ExecuteFullCmd(CK::CuckooHashTable *tb, int num_ops, int tid) {
	int oks = 0;
	int offset = num_ops*tid;
	
	tid = connect_num-1-tid;

	bool insert_success = true;

	for (int i = 0; i < num_ops; ++i) {
		switch(fc_cmd[offset+i]) {
			case QINSERT:
				oks += tb->checkInsert(fc_entry[offset+i], tid);
				break;
			case INSERT:
				if (!insert_success)
					break;
				insert_success = tb->insert(fc_entry[offset+i], tid);
				oks += insert_success;
				break;
			case READ:
				oks += tb->query(fc_entry[offset+i].key, NULL, tid);
				break;
			case UPDATE:
				oks += tb->update(fc_entry[offset+i], tid);
				break;
			case DELETE:
				oks += tb->deletion(fc_entry[offset+i].key, tid);
				break;
			default:
				break;
		}
	}

	return oks;
}

/* Excute full commands on multiple threads */
int inline MultiThreadRun(CK::CuckooHashTable *tb, int total_ops, int num_threads) {
	int ret = -1;

	vector<future<int>> actual_ops;

	cout << "running..." << endl;

	int part_ops = total_ops/num_threads;

	for (int i = 0; i < num_threads; ++i) {
		actual_ops.emplace_back(async(launch::async, ExecuteFullCmd,
									  tb, part_ops, i));

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