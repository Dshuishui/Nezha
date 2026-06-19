#!/bin/bash

# Small value (64B) YCSB workloads — mirrors run_ycsb.sh with vsize=64
# Run with server started using -inlineThreshold 0 (baseline) or -inlineThreshold 512 (AVP)

log_file="results_small_64b.txt"

> $log_file

workloads=(
  "C go run ./A/mixLoad.go -cnums 1 -dnums 100000 -vsize 64 -wratio 0 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "B go run ./A/mixLoad.go -cnums 1 -dnums 100000 -vsize 64 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "D go run ./D/D.go -cnums 1 -dnums 100000 -vsize 64 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "E go run ./E/mixLoad_scan.go -cnums 1 -dnums 100000 -scansize 100 -vsize 64 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "A go run ./A/mixLoad.go -cnums 1 -dnums 100000 -vsize 64 -wratio 0.5 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "F go run ./F/RMW.go -cnums 1 -dnums 100000 -vsize 64 -wratio 0.5 -servers 192.168.1.240:3088,192.168.1.241:3088"
)

for wl in "${workloads[@]}"; do
  workload_name=$(echo $wl | cut -d' ' -f1)
  command=$(echo $wl | cut -d' ' -f2-)

  echo "Running workload $workload_name"
  echo "----- Output of Workload $workload_name -----" >> $log_file
  $command >> $log_file 2>&1
  echo "" >> $log_file
done

echo "All workloads completed. Results are in $log_file"
