#!/bin/bash
g++ local_latency.cpp -o main -O2 -Ofast -ffast-math -funroll-loops
./main