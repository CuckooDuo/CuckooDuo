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

Datasets used in CPU experiments can be found or downloaded as described in `workload/`. After downloading, modify the corresponding configuration in `main.cpp` line 17.

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


* Load Factor of all algorithms vs. Bucket Size (Figure 9(a))

  ```bash
  python bucket_sizeVSload_factor.py
  ```


* Average number of memory accesses vs. Load factor (Figure 3(a) in Supplementary Materials)

  ```bash
  ./main average_memory_accesses
  ```


* Worst number of memory accesses vs. Load factor

  ```bash
  ./main worst_memory_accesses
  ```


* Average number of moved items vs. Load factor (Figure 3(b) in Supplementary Materials)

  ```bash
  ./main average_moved_items
  ```


* Worst number of moved items vs. Load factor

  ```bash
  ./main worst_moved_items
  ```


* Average number of accessed items vs. Load factor (Figure 9(c))

  ```bash
  ./main average_accessed_items
  ```


* Worst number of accessed items vs. Load factor

  ```bash
  ./main worst_accessed_items
  ```

* Load factor(bucket size) vs. Table Size (Figure 7(a))

  ```bash
  python experiment.py -fig 5a
  ```

* Load factor(f) vs. Table Size

  ```bash
  python experiment.py -fig 5b
  ```

* False Positive Rate vs. Bucket Size

  ```bash
  python experiment.py -fig 5c
  ```

* Load factor of BFS(ours), DFS(cuckoo) vs. Table Size (Figure 7(b))

  ```bash
  python extra_experiment.py -fig 5d
  ```

* Moved Items of BFS(ours), DFS(cuckoo) vs. Load factor (Figure 7(c))

  ```bash
  python extra_experiment.py -fig 5e
  ```
  
* Load factor(L) vs. Bucket Size (Figure 7(d))

  ```bash
  python extra_experiment.py -fig 5f
  ```
  
* items in stash of Basic, Dual-FP vs. Inserted Items (Figure 7(g))

  ```bash
  python get_5g.py
  ```
  
* Load factor of Basic, Dual-FP vs. Table Size (Figure 7(h))

  ```bash
  python experiment.py -fig 5h
  ```
  
* FP adjust rate(f) vs. Load factor

  ```bash
  python extra_experiment.py -fig 5i
  ```

### Experiments added during the revision of the paper

1. **Memory Access and Elements Metrics with Cache**

   To run the experiment that evaluates memory access count and the number of memory elements accessed under different proportions of illegal query operations with caching, execute the following commands:

   ```bash
   g++ cacheExperiment.cpp -o main
   ./main
   ```

2. **Adjustment Frequency vs. Fingerprint Length and Load Factor**

   To obtain data on how the number of adjustments in CuckooDuo (ours) changes with the fingerprint length and load factor, use the following script:

   ```bash
   ./AdjustmentInBucketvsLFvsSIGLEN.sh
   ```

3. **Load Factor and False Positive Rate vs. Fingerprint Length**

   This script generates experimental data on load factor and false positive rate in CuckooDuo (ours) as the fingerprint length varies:

   ```bash
   ./LFvsSIGLEN_FPRvsSIGLEN.sh
   ```

4. **BPK and Load Factor: CuckooDuo vs Megakv by Fingerprint Length**

   To compare BPK and load factor for CuckooDuo (ours) and Megakv across different fingerprint lengths, run:

   ```bash
   ./bpk_lf_siglen_ours_vs_single.sh
   ```

5. **BPK Comparison: CuckooDuo vs CCEH by Insertion Volume**

   For a comparison of BPK between CuckooDuo (ours) and CCEH as the insertion volume changes, compile and run the following program:

   ```bash
   g++ ours_vs_cceh_bpk.cpp -o main -D SIG_BIT=16
   ./main
   ```
