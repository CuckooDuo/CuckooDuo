#!/bin/bash
g++ LF_vs_StashSize.cpp -o main -O2 -Ofast -ffast-math -funroll-loops
./main