# YCSB Workloads

## Overview

We use YCSB [[1]](#md-ref-1) workloads to conduct experiments. We use the default Zipfian request distribution (𝜃 = 0.99) for all workloads. For most experiments in our paper, we first load a YCSB workload with 30M insert requests into the KV table, and then perform lookup/update/delete operations on the inserted items. For the experiments on hybrid workloads (Figure 11), we first load 3M items into the KV table, and then test the performance of all algorithms under nine YCSB workloads with different insert/update ratio (from 1:9 to 9:1). 

All the YCSB workloads used in our paper can be found in the following [link](???). 

In this folder, we also provide 19 small sample workloads(1 for insert requests, other 18 are paired for hybrid workloads) with suffix `-64K`, which contains 64K requests. These small workloads can be directly used to run the codes in this project by modfying file path in cpp files. 

## References

<span id="md-ref-1"></span>
[1] Brian F Cooper, Adam Silberstein, Erwin Tam, Raghu Ramakrishnan, and Russell Sears. Benchmarking cloud serving systems with ycsb. In Proceedings of the 1st ACM symposium on Cloud computing, pages 143–154, 2010.
