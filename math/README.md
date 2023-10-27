# CuckooDuo Math

## Overview

This folder contains the codes for validating the theoretical results in our paper, where we compare the theoretical bounds/theorems with our experimental results. 

## File structures

* `murmur3.h`: MurmurHash (hash function used in this project)
* `CuckooDuo.h`: Implementation of CuckooDuo algorithm
* `CuckooSingle.h`: Implementation of single fingerprint cuckoo
* `CuckooDuo_BFSfailure.h`: Implementation of CuckooDuo algorithm for BFSfailure counting
* `5a_SingleCollide.cpp`: Testing and comparing CuckooSingle's numbers of collide items 
* `5b_5c_CollideItems.cpp`: Testing and comparing CuckooDuo's numbers of collide items
* `5d_BFSfailure.cpp`: Testing and comparing CuckooDuo's numbers of BFS failure items
* `5a_SingleCollide.py`: Python script for generating csv data for Fig.5a
* `5b_CollideItems.py`: Python script for generating csv data for Fig.5b
* `5c_CollideItems.py`: Python script for generating csv data for Fig.5c
* `5d_BFSfailure.py`: Python script for generating csv data for Fig.5d


## Datasets

Datasets used in CPU experiments can be find or downloaded as described in `workload/`. After downloading, modify the corresponding configuration in `main.cpp` line 17.

## How to run

### Test cases

Below we show some examples of running tests. Test results csv files will be saved in the current directory.

By running the following commands, you can automatically reproduce the results in our paper. 

* Collided Items of CuckooSingle(f=16) vs. Load Factor (Figure 5(a))

  ```bash
  python 5a_SingleCollide.py
  ```


* Collided Items of CuckooDuo(f=8) vs. Bucket Size (Figure 5(b))

  ```bash
  python 5b_CollideItems.py
  ```


* Collided Items of CuckooDuo(f=16) vs. Bucket Size (Figure 5(c))

  ```bash
  python 5c_CollideItems.py
  ```


* BFS Failed Items of CuckooSingle(L=1) vs. Load Factor (Figure 5(d))

  ```bash
  python 5d_BFSfailure.py
  ```
