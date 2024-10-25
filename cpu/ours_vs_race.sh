#!/bin/bash

for i in 300 3000 30000; do
  g++ ours_vs_cceh_bpk.cpp -o main -O2 -D SIG_BIT=16 -D MAX_ITEMS_PER_SEGMENT=$i
  ./main
done