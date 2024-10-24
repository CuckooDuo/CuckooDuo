#!/bin/bash

for i in 10 12 14 16; do
  g++ adjustmentCounter.cpp -o main -O2 -D SIG_BIT=$i
  ./main
done