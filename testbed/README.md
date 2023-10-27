# CuckooDuo in RDMA-based Testbed

## Overview

We implement and test CuckooDuo in a RDMA-based testbed with 2 servers and 1 switch. Each server is with one 24-core Intel(R) Xeon(R) Silver 4116 @ 2.10GHz CPU, 32GB DRAM, and one 100Gbps Mellanox ConnectX-5 IB RNIC. Each RNIC is connected to a Tofino programmable switch through 100Gbps cables, which is also equipped with Mellanox ConnectX-5 IB RNICs to support RDMA protocol. We use one server for emulating local fast memory, on which we build the hashing index. Another server is used for emulating remote slow memory, on which we build the KV table. As remote memory usually only has minimal computational power, which cannot handle frequent insert/update/delete/lookup operations. To emulate the weak compute power, we only use the CPU in remote server for registering memory to RNICs during initialization and performing memory copy operation during expansion. We do not use the CPU on remote server for any calculation. 

We also implement the other three algorithms in our testbed: MapEmbed, RACE, and TEA.

## File structures

### algorithms: 
* `murmur3.h`: MurmurHash
* `CuckooDuo.h`: Implementation of CuckooDuo algorithm with RDMA
* `mapembed.h`: Implementation of MapEmbed algorithm with RDMA
* `race.h`: Implementation of RACE algorithm with RDMA
* `tea.h`: Implementation of TEA algorithm with RDMA

### rdma: 
* `rdma_common.h/cpp`: The common RDMA routines used in the server/client program
* `rdma_client.h`: RDMA client side code for CuckooDuo
* `rdma_server.h`: RDMA server side code for CuckooDuo
* `rdma_server.cpp`: An implement of RDMA server on remote node

### test:
* `test_latency.cpp`: Test program for latency at different load factor
* `test_multi.cpp`: Test program for throughput with different thread number
* `test_hybrid_single.cpp`: Test program for latency on hybrid workloads with single thread
* `test_hybrid_multi.cpp`: Test program for throughput on hybrid workloads with multiple thread
* `test_expand.cpp`: Test program for expansion

### ycsb_header: 
* `ycsb_read.h`: Definions and tools for work with YCSB
* `ycsb_cuckooduo.h`: Functions to work with YCSB datasets on CuckooDuo
* `ycsb_mapembed.h`: Functions to work with YCSB datasets on MapEmbed
* `ycsb_race.h`: Functions to work with YCSB datasets on RACE
* `ycsb_tea.h`: Functions to work with YCSB datasets on TEA

### others:
* `CMakeLists.txt`: Helper to build programs with cmake tools

## Datasets

Datasets used in RDMA experiments can be find or downloaded as described in `workload/`. After getting data files, modify the corresponding configuration as `load_path` or `run_path` in `test_xx.cpp` in `test/`.

## How to run

### Build

First, get the datasets like described above. Modify `load_path` and `run_path` in `test_xx.cpp` to specify which dataset to be used.

Then, compile the executable programs for test with the following commands, which requiring `makefile` and `cmake`.

```bash
cmake .
make clean
make
```

### Parametes settings
We will introduce some basic parameter settings. You can adjust them by modifying the  files yourself.

* Gerneral parameters
```C
int cell_number = sizeof(workload);
int bucket_depth = 8;
```

* CuckooDuo
```C
int max_kick_num = 3;
int max_stash_size = 64;
```

* MapEmbed
```C
int MapEmbed_layer = 3;
int MapEmbed_cell_number[3];
MapEmbed_cell_number[0] = MapEmbed_bucket_number * 9 / 2;
MapEmbed_cell_number[1] = MapEmbed_bucket_number * 3 / 2;
MapEmbed_cell_number[2] = MapEmbed_bucket_number / 2;
int MapEmbed_cell_bit = 4;
max_stash_size = 64;
```

* RACE
```C
int max_stash_size = 64;
```

* TEA
```C
int max_kick_num = 1000;
int max_stash_size = 8192;
```

### Test cases

Below we show some examples of running tests. And `server@` means to execute commands on remote server, `client@` means on local client similarly.  

The results will stored in csv files described by `csv_path` in corresponding cpp files.

You can also get more details of each test in corresponding cpp files, and modify test by yourself.

* Comparison of average latency with prior art (Figure 8(a~d))
```bash
server@: ./bin/rdma_server -a remote_IP

client@: ./bin/test_latency -a remote_IP
```

*  Comparison of average throughput with prior art. (Figure 9(a~d))
```bash
server@: ./bin/rdma_server -a remote_IP -n 16

client@: ./bin/test_multi -a remote_IP -n 16
```

* Comparison of speed on hybrid workloads (Figure 11(a))
```bash
server@: ./bin/rdma_server -a remote_IP

client@: ./bin/test_hybrid_single -a remote_IP
```

* Comparison of speed on hybrid workloads (Figure 11(b))
```bash
server@: ./bin/rdma_server -a remote_IP -n 16

client@: ./bin/test_hybrid_multi -a remote_IP -n 16
```

*  Performance of dynamic expansion (Figure 12(b))
```bash
server@: ./bin/rdma_server -a remote_IP

client@: ./bin/test_expand -a remote_IP
```
