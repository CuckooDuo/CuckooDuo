#!/bin/bash

for i in $(seq 10 32); do
  g++ siglenVSbpk_oursVSsingle.cpp -o main -O2 -D SIG_BIT=$i
  ./main
done