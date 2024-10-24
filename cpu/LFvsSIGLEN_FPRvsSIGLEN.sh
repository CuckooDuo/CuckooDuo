#!/bin/bash

for i in $(seq 8 20); do
  g++ differentSIGLEN.cpp -o main -O2 -D SIG_BIT=$i
  ./main
done