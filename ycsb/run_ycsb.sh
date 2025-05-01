#!/bin/bash

# 定义日志文件
log_file="results.txt"

# 清空日志文件或创建新文件
> $log_file

# 定义工作负载及其对应的命令
workloads=(
  "C go run ./ycsb/A/mixLoad.go -cnums 1 -dnums 100000 -vsize 4000 -wratio 0 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "B go run ./ycsb/A/mixLoad.go -cnums 1 -dnums 100000 -vsize 4000 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "D go run ./ycsb/D/D.go -cnums 1 -dnums 100000 -vsize 4000 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "E go run ./ycsb/E/mixLoad_scan.go -cnums 1 -dnums 100000 -scansize 100 -vsize 4000 -wratio 0.05 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "A go run ./ycsb/A/mixLoad.go -cnums 1 -dnums 100000 -vsize 4000 -wratio 0.5 -servers 192.168.1.240:3088,192.168.1.241:3088"
  "F go run ./ycsb/F/RMW.go -cnums 1 -dnums 100000 -vsize 4000 -wratio 0.5 -servers 192.168.1.240:3088,192.168.1.241:3088"
)

# 遍历每个工作负载
for wl in "${workloads[@]}"; do
  # 分割工作负载名称和命令
  workload_name=$(echo $wl | cut -d' ' -f1)
  command=$(echo $wl | cut -d' ' -f2-)
  
  # 运行命令并将输出（包括标准错误）追加到日志文件
  echo "Running workload $workload_name"
  echo "----- Output of Workload $workload_name -----" >> $log_file
  $command >> $log_file 2>&1
  echo "" >> $log_file  # 添加额外的换行符以分隔不同命令的输出
done

# 脚本执行完成
echo "All workloads completed. Results are in $log_file"