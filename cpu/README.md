#  CuckooDuo on CPU

## Overview


This folder contains the codes of CuckooDuo and the related algorithms implemented with C++ on CPU platforms. We compare CuckooDuo with three other algorithms: MapEmbed, RACE, and TEA. 


## File structures

* `murmur3.h`: MurmurHash (hash function used in this project)
* `CuckooDuo.h`: Implementation of CuckooDuo algorithm
* `CuckooSingle.h`: Implementation of single fingerprint cuckoo
* `MapEmbed.h`: Implementation of MapEmbed algorithm
* `race.h`: Implementation of RACE algorithm
* `tea.h`: Implementation of TEA algorithm
* `main.cpp`: Testing and comparing CuckooDuo and other algorithms
* `bucket_sizeVSload_factor.py`: Testing and comparing load factor


## Datasets

Datasets used in CPU experiments can be find or downloaded as described in `workload/`. After downloading, modify the corresponding configuration in `main.cpp` line 17.

## How to run

### Build

First, get the datasets like described above. Modify `inputFilePath` in `main.cpp` line 17 to specify which dataset to be used.

Then, compile the main.cpp with the following command. 

```bash
g++ main.cpp -o main
```


### Test cases

Below we show some examples of running tests. The directory where test results are stored is specified by "testResultDir" line 18 in `main.cpp`, line 4 in `bucket_sizeVSload_factor.py`. 
 
By running the following commands, you can automatically reproduce the results in our paper. 


* Load Factor of all algorithms vs. Bucket Size (Figure 7(a))

  ```bash
  python bucket_sizeVSload_factor.py
  ```


* Average number of memory accesses vs. Load factor (Figure 7(b))

  ```bash
  ./main average_memory_accesses
  ```


* Worst number of memory accesses vs. Load factor

  ```bash
  ./main worst_memory_accesses
  ```


* Average number of moved items vs. Load factor (Figure 7(c))

  ```bash
  ./main average_moved_items
  ```


* Worst number of moved items vs. Load factor

  ```bash
  ./main worst_moved_items
  ```


* Average number of accessed items vs. Load factor (Figure 7(d))

  ```bash
  ./main average_accessed_items
  ```


* Worst number of accessed items vs. Load factor

  ```bash
  ./main worst_accessed_items
  ```

* Load factor(bucket size) vs. Table Size (Figure 6(a))

  ```bash
  python experiment.py -fig 6a
  ```

* Load factor(f) vs. Table Size (Figure 6(b))

  ```bash
  python experiment.py -fig 6b
  ```

* False Positive Rate vs. Bucket Size (Figure 6(c))

  ```bash
  python experiment.py -fig 6c
  ```

* Load factor of BFS, DFS, Random vs. Table Size (Figure 6(d))

  ```bash
  python experiment.py -fig 6d
  ```

* Moved Items of BFS, DFS, Random vs. Table Size (Figure 6(e))

  ```bash
  python experiment.py -fig 6e
  ```
  
* items in stash of Basic, Dual-FP vs. Inserted Items (Figure 6(f))

  ```bash
  python get_6f.py.
  ```
  
* Load factor of Basic, Dual-FP vs. Table Size (Figure 6(g))

  ```bash
  python experiment.py -fig 6g
  ```
