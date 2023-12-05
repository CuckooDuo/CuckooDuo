# CuckooDuo Math

## Overview

This folder contains the codes for validating the theoretical results in our paper, where we compare the theoretical bounds/theorems with our experimental results. 

## File structures

* `murmur3.h`: MurmurHash (hash function used in this project)
* `CuckooDuo.h`: Implementation of CuckooDuo algorithm
* `CuckooSingle.h`: Implementation of single fingerprint cuckoo
* `CuckooDuo_BFSfailure.h`: Implementation of CuckooDuo algorithm for BFSfailure counting
* `4a_SingleCollide.cpp`: Testing and comparing CuckooSingle's numbers of collide items 
* `4b_4c_CollideItems.cpp`: Testing and comparing CuckooDuo's numbers of collide items
* `4d_BFSfailure.cpp`: Testing and comparing CuckooDuo's numbers of BFS failure items
* `4a_SingleCollide.py`: Python script for generating csv data for Fig.4a
* `4b_CollideItems.py`: Python script for generating csv data for Fig.4b
* `4c_CollideItems.py`: Python script for generating csv data for Fig.4c
* `4d_BFSfailure.py`: Python script for generating csv data for Fig.4d


## Datasets

Datasets used in CPU experiments can be found or downloaded as described in `workload/`. After downloading, modify the corresponding configuration in `main.cpp` line 17.

## How to run

### Test cases

Below we show some examples of running tests. Test results csv files will be saved in the current directory.

By running the following commands, you can automatically reproduce the results in our paper. 

* Collided Items of CuckooSingle(f=16) vs. Load Factor (Figure 4(a))

  ```bash
  python 4a_SingleCollide.py
  ```


* Collided Items of CuckooDuo(f=8) vs. Bucket Size (Figure 4(b))

  ```bash
  python 4b_CollideItems.py
  ```


* Collided Items of CuckooDuo(f=16) vs. Bucket Size (Figure 4(c))

  ```bash
  python 4c_CollideItems.py
  ```


* BFS Failed Items of CuckooSingle(L=1) vs. Load Factor (Figure 4(d))

  ```bash
  python 4d_BFSfailure.py
  ```
