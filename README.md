# Nezha: A Key-Value Separated Distributed Store with Optimized Raft Integration

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go 1.22+](https://img.shields.io/badge/go-1.22+-blue.svg)](https://golang.org/dl/)
[![RocksDB](https://img.shields.io/badge/RocksDB-5.18.fb-green.svg)](https://rocksdb.org/)

**High-Performance Distributed Key-Value Storage System with Key-Value Separation Optimized Raft Consensus Protocol**

Nezha is an innovative distributed key-value storage system that deeply integrates key-value separation technology with the Raft consensus protocol, significantly reducing redundant persistence operations while providing scalable throughput and strong consistency guarantees. By redesigning the persistence strategy and introducing a tiered garbage collection mechanism, the system dramatically improves read and write performance while maintaining Raft's safety properties.

---

## Key Features

- **KVS-Raft Protocol**: Innovative integration of key-value separation into the Raft consensus protocol
- **Optimized Persistence Strategy**: Reduces value write operations from at least 3 times to just 1 time
- **Raft-Aware Garbage Collection**: Adaptive GC framework that balances read and write performance
- **Three-Phase Request Processing**: Ensures correct request handling during GC operations
- **Strong Consistency Guarantee**: Maintains Raft's safety properties and linearizability
- **High Performance Improvement**: Average throughput improvements of 445.8% (PUT), 12.5% (GET), 72.6% (SCAN)

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

### Prerequisites

#### 1. Go Environment (1.22+)

```bash
# Download Go 1.22
wget https://go.dev/dl/go1.22.0.linux-amd64.tar.gz

# Install
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.22.0.linux-amd64.tar.gz

# Add to PATH
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

# Verify
go version
```

#### 2. System Dependencies

```bash
sudo apt-get update
sudo apt-get install -y gcc g++ make git \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev
```

#### 3. RocksDB 5.18.fb (Source Compilation)

```bash
# Clone RocksDB
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout v5.18.3

# Compile shared library (takes 5–15 minutes)
make shared_lib -j$(nproc)

# Install
sudo make install-shared INSTALL_PATH=/usr/local

# Update library cache
sudo ldconfig

cd ..
```

> **Note:** If compilation fails with `'uint64_t' does not name a type` on Ubuntu 20.04+, run the included fix script:
> ```bash
> ./fix-rocksdb.sh
> ```
> Then retry `make shared_lib`.

#### 4. Configure CGO Environment Variables

```bash
# Add to ~/.bashrc (replace /path/to/rocksdb with your actual path)
echo 'export CGO_CFLAGS="-I/path/to/rocksdb/include"' >> ~/.bashrc
echo 'export CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"' >> ~/.bashrc
echo 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib' >> ~/.bashrc
source ~/.bashrc
```

#### 5. Download Project Dependencies

```bash
cd Nezha
go mod download
```

### Build

```bash
# Build the server binary
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

For quick deployment without compiling from source.

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- x86_64 (amd64) architecture

### Step 1: Obtain Required Files

The Docker image requires a pre-compiled `nezha` binary and RocksDB shared libraries. These must be placed in the `docker/` directory before building:

```
docker/
├── nezha                  # compiled binary
├── librocksdb.so.5.18     # RocksDB shared library
└── libgflags.so.2         # gflags shared library
```

To obtain these files, compile from source (Method 1) and copy:

```bash
# After building via Method 1
cp nezha docker/
cp /usr/local/lib/librocksdb.so.5.18 docker/
cp /usr/local/lib/libgflags.so.2 docker/
```

### Step 2: Build Docker Image

```bash
cd docker
./build.sh
```

### Step 3: Run

#### Using Docker Compose (Recommended)

```bash
cd docker
./manage.sh start
```

#### Using Docker CLI Directly

```bash
docker run -d \
  --name nezha-node1 \
  --network host \
  -v nezha-data:/app/data \
  nezha-multigc:latest \
  -address 127.0.0.1:3088 \
  -internalAddress 127.0.0.1:30881 \
  -peers 127.0.0.1:30881
```

#### Management Commands

```bash
./manage.sh start    # Start node
./manage.sh stop     # Stop node
./manage.sh restart  # Restart node
./manage.sh logs     # View logs
./manage.sh status   # View status
./manage.sh clean    # Remove all containers and volumes
```

---

## Performance Testing

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
├── fix-rocksdb.sh             # RocksDB compilation fix script
├── setup-go.sh                # Go environment setup script
│
├── docker/                    # Docker deployment files
│   ├── Dockerfile.ubuntu24    # Container image definition (Ubuntu 24.04)
│   ├── docker-compose.yml     # Single-node orchestration
│   ├── manage.sh              # Node management script
│   ├── build.sh               # Image build script
│   └── README.md              # Docker deployment guide
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

### RocksDB Compilation: Missing `<cstdint>` Headers

**Symptom:** `error: 'uint64_t' does not name a type`

**Cause:** RocksDB 5.18.fb predates modern GCC requirements for explicit `<cstdint>` inclusion.

**Fix:**
```bash
./fix-rocksdb.sh
```

### CGO Linking Error

```bash
# Verify environment variables are set
echo $CGO_CFLAGS
echo $CGO_LDFLAGS
echo $LD_LIBRARY_PATH

# Verify RocksDB is installed
ls /usr/local/lib/librocksdb*
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

## Contact

- **Issues**: [GitHub Issues](https://github.com/Dshuishui/Nezha/issues)
- **Email**: Contact project maintainers through GitHub
