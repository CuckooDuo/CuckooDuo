# CuckooDuo: Extensible Dynamic Perfect Hashing for Remote Memory Key-Value Store

This repository contains all related code of our paper "CuckooDuo: Extensible Dynamic Perfect Hashing for Remote Memory Key-Value Store". 

## Introduction

Perfect hashing is a special hash function that ensures a collision-free mapping of 𝑛 keys into 𝑚 consecutive natural numbers, which enables the creation of a perfect hashing table where each unique key occupies a distinct slot. Thanks to its small and constant lookup time, perfect hashing can be a powerful tool for remote memory data storage. SOTA dynamic perfect hashing schemes attain high load factor by sacrificing associativity. This paper proposes CuckooDuo, an extensible dynamic perfect hashing framework with the key design of two interlinked cuckoo hash tables. CuckooDuo has the associativity of exact one, and it achieves high load factor, fast speed, and minimal communication bandwidth simultaneously, as well as supports efficient expansion without subtable rebuilding. We theoretically analyze the probability of insertion failure in CuckooDuo. We fully implement CuckooDuo in a testbed built with two servers and one switch interconnected via RDMA networks. Extensive experimental results show that CuckooDuo achieves up to 125× smaller communication overhead and 2.3× faster speed than prior art. All related codes are open-sourced anonymously.

## About this repository

* `cpu` contains codes of CuckooDuo and the related algorithms implemented on CPU platforms. 

* `testbed` contains codes of CuckooDuo and the related algorithms implemented in our testbed (built with two servers and one programmalbe swtich connected through RDMA networks).

* `workload` contains 10 small sample workloads with 64K requests, and the method to download workloads with 30M requests like used in experiments in our paper.

* More details can be found in the folders.
