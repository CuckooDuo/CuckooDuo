# Extendible RDMA-based Remote Memory KV Store with Dynamic Perfect Hashing Index

This repository contains all related code of our paper "Extendible RDMA-based Remote Memory KV Store with Dynamic Perfect Hashing Index". 

**Our codes can reproduce all experimental results (figures) in our paper.**

## Introduction

Perfect hashing is a special hashing function that maps each item to a unique location without collision, which enables the creation of a KV store with small and constant lookup time. Recent dynamic perfect hashing attains high load factor by increasing associativity, which impacts bandwidth and throughput. This paper proposes a novel dynamic perfect hashing index without sacrificing associativity, and uses it to devise an RDMA-based remote memory KV store called CuckooDuo. CuckooDuo simultaneously achieves high load factor, fast speed, minimal bandwidth, and efficient expansion without item movement. We theoretically analyze the properties of CuckooDuo, and implement it in an RDMA-network based testbed. The results show CuckooDuo achieves 1.9∼17.6× smaller insertion latency and 9.0∼18.5× smaller insertion bandwidth than prior works.

## About this repository

* `cpu` contains codes of CuckooDuo and the related algorithms implemented on CPU platforms. 

* `math` contains the codes for validating the theoretical results in our paper, where we compare the theoretical bounds/theorems with our experimental results.

* `testbed` contains codes of CuckooDuo and the related algorithms implemented in our testbed (built with two servers and one programmalbe swtich connected through RDMA networks).

* `workload` contains several small sample workloads, which can be directly used to run the codes in this project. We also upload all workloads used in our paper to Google Drive and provide [link](https://drive.google.com/file/d/1ZUKmtoi40vPkr0qxi1syRrciAjZJrYDu/view?usp=sharing) to download them.

* `CuckooDuo_Supplementary.pdf` contains the supplementary materials of CuckooDuo.

* More details can be found in the folders.
