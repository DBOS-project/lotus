#!/bin/bash

rm -rf CMakeFiles/ CMakeCache.txt 
cmake .
make -j 4
