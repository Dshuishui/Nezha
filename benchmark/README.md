# benchmark 工具

本目录下每个子目录是一个独立的 `package main` 客户端。当前实验脚本（`scripts/`）在用的：

| 工具 | 用途 |
|---|---|
| `randwrite_goroutine` | 主表 PUT 吞吐（并发 goroutine 写入） |
| `zipf_read` | GET：Zipf 分布随机读，多轮 |
| `scan_pro` | SCAN：范围查询，可配 keyspace / gap / 轮数 |
| `scanverify` | 写入 + 逐条校验 GET/SCAN（正确性） |
| `readonly` | 只读校验，故障切换/恢复后用 |
| `countkeys` | 统计库内 key 数 |
| `testRocksDB` | RocksDB 单机基准 |

其余（`cdf` `cdf_v2` `randread` `randread_pro` `randwrite` `randwrite_pro` `scan` `scan_pro_paginated`
`seqwrite` `small_value` `testRocksDB-kvs` `testRocksDB_v2`）是早期实验用的，暂时保留（用户 2026-09-04 决定）。

## 早期手写的调用示例（原 `client.txt`，服务器地址已过时，仅作参数参考）

```
go run ./benchmark/randwrite/randwrite.go -cnums 300 -dnums 40000 -vsize 16000 -servers 192.168.1.62:3088,192.168.1.104:3088

go run ./benchmark/seqwrite/seqwrite.go -cnums 300 -dnums 100000 -vsize 64000 -servers 192.168.1.62:3088,192.168.1.104:3088

go run ./benchmark/randread/randread.go -cnums 400 -dnums 15000 -servers 192.168.1.62:3088,192.168.1.100:3088,192.168.1.104:3088

go run ./benchmark/scan/scan.go -cnums 100 -dnums 2500 -servers 192.168.1.104:3088,192.168.1.100:3088

go run . -address 192.168.1.104:3088 -internalAddress 192.168.1.104:30881 -peers 192.168.1.104:30881,192.168.1.100:30881 -gap 40000

===================================================
serverF : ~/Gitee/FlexSync/kvstore/FlexSync$ go run . -address 192.168.1.104:3088 -internalAddress 192.168.1.104:30881 -peers 192.168.1.104:30881,192.168.1.105:30881 -gap 40000
serverL : ~/Gitee/FlexSync$ go run ./kvstore/LevelDB/LevelDB.go -address 192.168.1.104:3088 -internalAddress 192.168.1.104:30881 -peers 192.168.1.104:30881,192.168.1.105:30881 -gap 40000

write : ~/Gitee/FlexSync$ go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go -cnums 100 -dnums 39062 -vsize 256000 -servers 192.168.1.104:3088,192.168.1.105:3088
read  : ~/Gitee/FlexSync$ go run ./benchmark/zipf_read/zipf_read.go -cnums 100 -dnums 10000 -servers 192.168.1.104:3088,192.168.1.105:3088
scan  : ~/Gitee/FlexSync$ go run ./benchmark/scan_pro/scan_pro.go -cnums 1 -dnums 4 -servers 192.168.1.104:3088,192.168.1.105:3088

YCSB-ABCD: ~/Gitee/FlexSync$ go run ./ycsb/A/mixLoad.go -cnums 100 -dnums 100000 -vsize 64000 -wratio 0.5 -servers 192.168.1.104:3088,192.168.1.105:3088
YCSB-F  : ~/Gitee/FlexSync$ go run ./ycsb/F/RMW.go -cnums 100 -dnums 1000000 -vsize 4000 -wratio 0.5 -servers 192.168.1.104:3088,192.168.1.105:3088
YCSB-E  : ~/Gitee/FlexSync$ go run ./ycsb/E/mixLoad_scan.go -cnums 1 -dnums 4 -scansize 100 -vsize 64000 -wratio 0.05 -servers 192.168.1.104:3088,192.168.1.105:3088
```
