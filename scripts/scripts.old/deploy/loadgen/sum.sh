#!/bin/sh

grep "operations per second" results/* | awk '{print $5}' | awk -Fops/s '{print $1}' | awk '{s+=$1} END {print s}'
