# Nezha: A Key-Value Separated Distributed Store with Optimized Raft Integration

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go 1.24+](https://img.shields.io/badge/go-1.24+-blue.svg)](https://golang.org/dl/)
[![RocksDB](https://img.shields.io/badge/RocksDB-8.x-green.svg)](https://rocksdb.org/)
[![ICDE 2026](https://img.shields.io/badge/ICDE-2026-orange.svg)](https://doi.org/10.1109/ICDE65706.2026.00187)
[![arXiv](https://img.shields.io/badge/arXiv-2603.09122-b31b1b.svg)](https://arxiv.org/abs/2603.09122)

**High-Performance Distributed Key-Value Storage System with Key-Value Separation Optimized Raft Consensus Protocol**

Nezha is an innovative distributed key-value storage system that deeply integrates key-value separation technology with the Raft consensus protocol, significantly reducing redundant persistence operations while providing scalable throughput and strong consistency guarantees. By redesigning the persistence strategy and introducing a tiered garbage collection mechanism, the system dramatically improves read and write performance while maintaining Raft's safety properties.

---

## Quick Start

**Docker (no installation required):**

```bash
docker run -d --name nezha --network host \
  -v nezha-data:/app/data dyucong/nezha:latest \
  -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 -peers 127.0.0.1:30881
```

**From source (Ubuntu 20.04+):**

```bash
bash scripts/setup-env.sh && source ~/.bashrc
./scripts/run-node.sh
```

---

## Key Features

- **KVS-Raft Protocol**: Innovative integration of key-value separation into the Raft consensus protocol
- **Optimized Persistence Strategy**: Reduces value write operations from at least 3 times to just 1 time
- **Raft-Aware Garbage Collection**: Adaptive GC framework that balances read and write performance
- **Three-Phase Request Processing**: Ensures correct request handling during GC operations
- **Strong Consistency Guarantee**: Maintains Raft's safety properties and linearizability
- **High Performance**: Average throughput improvements of **460.2% (PUT)**, **12.5% (GET)**, **72.6% (SCAN)** over standard Raft+RocksDB

---

## System Architecture

Nezha adopts a three-layer architectural design with deep optimization of consensus and storage layers:

### 1. Application Layer
- Provides standard key-value storage interfaces including Put, Get, Scan
- Compatible with existing system APIs
- Supports multiple access patterns

### 2. Consensus Layer (KVS-Raft)
- Implements Raft protocol integrated with key-value separation
- Values are stored directly in Raft logs with unified persistence
- State machine stores only lightweight offsets for enhanced performance

### 3. Storage Layer
- Three-module storage management: Active Storage, New Storage, Final Compacted Storage
- Raft-aware garbage collection mechanism with dynamic storage space optimization
- Hash index + sequential storage optimization for both point and range query performance

---

## Deployment Methods

Nezha provides two deployment options:

- **[Method 1: Source Code Compilation](#method-1-source-code-compilation)** — Recommended for development and research
- **[Method 2: Docker Container](#method-2-docker-container)** — Recommended for quick deployment and testing

---

## Method 1: Source Code Compilation

### Quick Setup (Recommended)

For Ubuntu 20.04+, use the one-command setup script:

```bash
bash scripts/setup-env.sh
source ~/.bashrc
```

This installs Go 1.24, RocksDB, and all dependencies automatically (~1 minute).

Then start the node:

```bash
./scripts/run-node.sh
```

---

### Manual Setup

#### 1. Go Environment (1.24+)

```bash
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
```

#### 2. System Dependencies

```bash
sudo apt-get update
sudo apt-get install -y gcc g++ make git \
    librocksdb-dev \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev
```

#### 3. Configure CGO Environment Variables

```bash
echo 'export CGO_CFLAGS="-I/usr/include"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L/usr/lib/x86_64-linux-gnu -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/lib/x86_64-linux-gnu' >> ~/.bashrc
source ~/.bashrc
```

#### 4. Download Project Dependencies

```bash
cd Nezha
go mod download
```

### Build

```bash
go build -o nezha ./kvstore/FlexSync/
```

### Run

#### Single Node

```bash
./nezha \
  -address 127.0.0.1:3088 \
  -internalAddress 127.0.0.1:30881 \
  -peers 127.0.0.1:30881 \
  -data ./data
```

Or using `go run` directly (no build step required):

```bash
go run ./kvstore/FlexSync/ \
  -address 127.0.0.1:3088 \
  -internalAddress 127.0.0.1:30881 \
  -peers 127.0.0.1:30881 \
  -data ./data
```

#### Multi-Node Cluster (3 nodes on separate machines)

```bash
# Node 1 (run on machine with IP1)
./nezha -address IP1:3088 -internalAddress IP1:30881 \
        -peers IP1:30881,IP2:30881,IP3:30881 -data ./data

# Node 2 (run on machine with IP2)
./nezha -address IP2:3088 -internalAddress IP2:30881 \
        -peers IP1:30881,IP2:30881,IP3:30881 -data ./data

# Node 3 (run on machine with IP3)
./nezha -address IP3:3088 -internalAddress IP3:30881 \
        -peers IP1:30881,IP2:30881,IP3:30881 -data ./data
```

### Parameter Reference

| Parameter | Description | Required |
|-----------|-------------|----------|
| `-address` | Client-facing address and port | Yes |
| `-internalAddress` | Raft internal communication address and port | Yes |
| `-peers` | Comma-separated Raft addresses of all cluster nodes | Yes |
| `-data` | Data storage directory (default: `.`) | No |

### Data Directory Structure

After startup, Nezha creates the following layout under the specified `-data` directory:

```
<data>/
└── data/
    ├── dbfile/
    │   └── keyIndex/       # LevelDB key-to-offset index
    └── valuelog/
        └── RaftState.log   # Unified Raft log + value store
```

---

## Method 2: Docker Container

No compilation required. Pull the pre-built image from [Docker Hub](https://hub.docker.com/r/dyucong/nezha) and run immediately.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Mac / Windows) or [Docker Engine](https://docs.docker.com/engine/install/) 20.10+ (Linux)
- x86_64 (amd64) architecture

### Run (Single Command)

```bash
docker run -d \
  --name nezha \
  --network host \
  -v nezha-data:/app/data \
  dyucong/nezha:latest \
  -address 127.0.0.1:3088 \
  -internalAddress 127.0.0.1:30881 \
  -peers 127.0.0.1:30881
```

> **Mac / Windows users:** `--network host` is Linux-only. Use port mapping instead:
> ```bash
> docker run -d \
>   --name nezha \
>   -p 3088:3088 -p 30881:30881 \
>   -v nezha-data:/app/data \
>   dyucong/nezha:latest \
>   -address 0.0.0.0:3088 \
>   -internalAddress 0.0.0.0:30881 \
>   -peers 127.0.0.1:30881
> ```

### Using Docker Compose

```bash
cd docker
docker-compose up -d
```

### Management

```bash
docker logs nezha -f      # Follow logs
docker stop nezha         # Stop
docker start nezha        # Start again
docker rm -v nezha        # Remove container and data
```

Or use the management script:

```bash
cd docker
./manage.sh start    # Start node
./manage.sh stop     # Stop node
./manage.sh restart  # Restart node
./manage.sh logs     # Follow logs
./manage.sh status   # View status
./manage.sh clean    # Remove all containers, volumes, and image
```

### Build from Source (Optional)

If you want to build the image yourself instead of using the pre-built one:

```bash
cd docker
./manage.sh build   # ~2 min (uses apt librocksdb, no source compilation)
```

---

## Performance Testing

### Experimental Environment

Benchmarks were conducted on a 3-node cluster, each node equipped with:

| Component | Specification |
|-----------|---------------|
| CPU | Intel Xeon E5-2603 v3 (12 cores, 2.4 GHz) |
| Memory | 64 GB DRAM |
| Storage | 2 TB SSD |
| OS | Ubuntu 20.04.4 LTS |
| Network | 10 Gigabit Ethernet |

Dataset: 100 GB, key size 10 B, value size 1 KB–256 KB, Zipf access distribution.

### PUT (Random Write)

```bash
go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
  -cnums 100 -dnums 39062 -vsize 256000 \
  -servers 127.0.0.1:3088
```

### GET (Zipf Distribution Read)

```bash
go run ./benchmark/zipf_read/zipf_read.go \
  -cnums 100 -dnums 10000 \
  -servers 127.0.0.1:3088
```

### SCAN (Range Scan)

```bash
go run ./benchmark/scan_pro/scan_pro.go \
  -cnums 1 -dnums 4 \
  -servers 127.0.0.1:3088
```

### Benchmark Parameter Reference

| Parameter | Description | Example |
|-----------|-------------|---------|
| `-cnums` | Number of concurrent clients | `100` |
| `-dnums` | Number of operations | `39062` |
| `-vsize` | Value size in bytes | `256000` |
| `-servers` | Comma-separated server addresses | `127.0.0.1:3088` |

---

## Project Structure

```
Nezha/
├── go.mod / go.sum            # Go module dependencies
│
├── scripts/                   # Deployment helper scripts
│   ├── setup-env.sh           # One-command environment setup
│   └── run-node.sh            # One-command node startup
│
├── docker/                    # Docker deployment files
│   ├── Dockerfile             # Container image definition (Ubuntu 24.04)
│   ├── docker-compose.yml     # Single-node orchestration
│   └── manage.sh              # Node management script
│
├── .github/workflows/         # CI/CD
│   └── docker-publish.yml     # Auto build and push to Docker Hub
│
├── kvstore/                   # Storage service implementations
│   ├── FlexSync/              # KVS-Raft core (main entry point)
│   │   ├── FlexSync.go        # gRPC server and Raft integration
│   │   ├── GC.go              # Garbage collection strategy
│   │   ├── GC_opt.go          # Optimized GC implementation
│   │   ├── AnotherGC.go       # Multi-round GC implementation
│   │   ├── AnotherGC_opt.go   # Optimized multi-round GC
│   │   └── filePool.go        # File handle pool
│   ├── LevelDB/               # LevelDB storage engine adapter
│   └── GC/                    # GC strategy experiments
│
├── raft/                      # Raft consensus protocol
│   ├── raft.go                # Core implementation (election, log replication)
│   ├── persister.go           # State persistence
│   └── common.go              # Shared data structures
│
├── rpc/                       # gRPC protocol definitions
│   ├── kvrpc/                 # Client-server RPC (Put/Get/Scan)
│   └── raftrpc/               # Raft inter-node RPC (RequestVote/AppendEntries)
│
├── benchmark/                 # Performance benchmark suite
│   ├── randwrite_goroutine/   # Concurrent random write
│   ├── zipf_read/             # Zipf-distribution read
│   ├── scan_pro/              # Range scan
│   └── ...                    # Additional benchmark workloads
│
├── ycsb/                      # YCSB standard benchmarks
│   ├── A/, D/, E/, F/         # YCSB workload implementations
│   └── run_ycsb.sh            # YCSB execution script
│
├── pool/                      # gRPC connection pool
└── util/                      # Utility functions
```

---

## Troubleshooting

### CGO Linking Error

```bash
# Verify environment variables are set
echo $CGO_CFLAGS
echo $CGO_LDFLAGS
echo $LD_LIBRARY_PATH

# Verify RocksDB is installed
ls /usr/lib/x86_64-linux-gnu/librocksdb*
```

### Port Already in Use

```bash
# Check what is using the ports
ss -tulpn | grep -E "3088|30881"

# Kill occupying process if needed
sudo kill $(lsof -t -i:3088)
```

### Node Fails to Start

```bash
# Check firewall
sudo ufw status
sudo ufw allow 3088/tcp
sudo ufw allow 30881/tcp
```

---

## Citation

This work was published at the **42nd IEEE International Conference on Data Engineering (ICDE 2026)**, Montreal, QC, Canada, May 4–8, 2026. If you use Nezha in your research, please cite:

> Yangyang Wang, Yucong Dong, Ziqian Cheng, and Zichen Xu. "Nezha: A Key-Value Separated Distributed Store with Optimized Raft Integration." In *Proceedings of the 42nd IEEE International Conference on Data Engineering (ICDE 2026)*, pp. 2503–2516. IEEE, 2026. [https://doi.org/10.1109/ICDE65706.2026.00187](https://doi.org/10.1109/ICDE65706.2026.00187)

```bibtex
@inproceedings{wang2026nezha,
  author    = {Yangyang Wang and Yucong Dong and Ziqian Cheng and Zichen Xu},
  title     = {Nezha: A Key-Value Separated Distributed Store with Optimized Raft Integration},
  booktitle = {Proceedings of the 42nd {IEEE} International Conference on Data Engineering ({ICDE} 2026)},
  address   = {Montreal, QC, Canada},
  pages     = {2503--2516},
  publisher = {IEEE},
  year      = {2026},
  doi       = {10.1109/ICDE65706.2026.00187}
}
```

- **IEEE Xplore**: https://ieeexplore.ieee.org/document/11629449
- **arXiv preprint**: https://arxiv.org/abs/2603.09122

---

## Contact

- **Issues**: [GitHub Issues](https://github.com/Dshuishui/Nezha/issues)
- **Email**: Contact project maintainers through GitHub
