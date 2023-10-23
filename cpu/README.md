# CuckooDuo on CPU

## Overview

We implement and test CuckooDuo on a CPU platform. We compare CuckooDuo with three other algorithms: MapEmbed, RACE, and TEA.

## File structures

* `murmur3.h`: MurmurHash
* `CuckooDuo.h`: Implementation of CuckooDuo algorithm
* `CuckooSingle.h`: Implementation of single fingerprint cuckoo
* `MapEmbed.h`: Implementation of MapEmbed algorithm
* `race.h`: Implementation of RACE algorithm
* `tea.h`: Implementation of TEA algorithm
* `main.cpp`: Testing and comparing CuckooDuo and other algorithms

## Datasets

Datasets used in CPU experiments can be find or downloaded as described in `workload/`. After getting data files, modify the corresponding configuration in `main.cpp` line 17.

## How to run

### Build

First, download the datasets from the above links. Modify `inputFilePath` in `main.cpp` line 17 to specify which dataset to be used.

Then, compile the main.cpp with the following command. 

```bash
g++ main.cpp -o main
```


### Test cases

Below we show some examples of running tests. 

The directory where test results are stored is specified by "testResultDir" line 18 in `main.cpp`.


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
