# Nezha: A Key-Value Separated Distributed Store with Optimized Raft Integration

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Go 1.19+](https://img.shields.io/badge/go-1.19+-blue.svg)](https://golang.org/dl/)
[![RocksDB](https://img.shields.io/badge/RocksDB-5.18.fb-green.svg)](https://rocksdb.org/)

**High-Performance Distributed Key-Value Storage System with Key-Value Separation Optimized Raft Consensus Protocol**

Nezha is an innovative distributed key-value storage system that deeply integrates key-value separation technology with the Raft consensus protocol, significantly reducing redundant persistence operations while providing scalable throughput and strong consistency guarantees. By redesigning the persistence strategy and introducing a tiered garbage collection mechanism, the system dramatically improves read and write performance while maintaining Raft's safety properties.

---

## 🎯 Key Features

- **KVS-Raft Protocol**: Innovative integration of key-value separation into the Raft consensus protocol
- **Optimized Persistence Strategy**: Reduces value write operations from at least 3 times to just 1 time
- **Raft-Aware Garbage Collection**: Adaptive GC framework that balances read and write performance
- **Three-Phase Request Processing**: Ensures correct request handling during GC operations
- **Strong Consistency Guarantee**: Maintains Raft's safety properties and linearizability
- **High Performance Improvement**: Average throughput improvements of 445.8% (PUT), 12.5% (GET), 72.6% (SCAN)

---

## 🔧 System Architecture

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

## 🚀 Deployment Methods

Nezha provides three deployment options to accommodate different environments and requirements:

### 🔧 **Method 1: Source Code Compilation (Recommended for Development)**
Complete control with custom compilation for optimal performance and debugging capabilities.

### 📦 **Method 2: Pre-compiled Binary Deployment**
Quick deployment using pre-built binaries for production environments.  
👉 **[Download from GitHub Release V1.0.0-multiGC](https://github.com/Dshuishui/Nezha/releases/tag/V1.0.0-multiGC)**

### 🐳 **Method 3: Docker Container Deployment**
Containerized deployment with full orchestration support for modern cloud environments.  
👉 **[View Docker Documentation](https://github.com/Dshuishui/Nezha/tree/multiGC/docker)**

---

## 🔧 Method 1: Source Code Compilation Deployment

### Prerequisites

#### 1. Go Environment (1.19+)
```bash
# Install Go 1.19 or higher
wget https://golang.org/dl/go1.19.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.19.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```

#### 2. RocksDB C++ Library (Source Compilation)
```bash
# Install system dependencies
sudo apt-get update
sudo apt-get install gcc g++ libsnappy-dev zlib1g-dev libbz2-dev \
                     liblz4-dev libzstd-dev libgflags-dev

# Install gflags
git clone https://github.com/gflags/gflags.git
cd gflags
git checkout v2.0
./configure && make && sudo make install

# Configure basic environment variables
cd ..
vim ~/.bashrc

# Add the following content to .bashrc
export CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:/usr/local/include
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib
export LIBRARY_PATH=${LIBRARY_PATH}:/usr/local/lib

# Reload environment variables
source ~/.bashrc

# Download and compile RocksDB
git clone https://github.com/facebook/rocksdb.git
cd rocksdb
git checkout 5.18.fb

# Compile shared library (time-consuming, please be patient)
make shared_lib

# Install to system directory
sudo INSTALL_PATH=/usr/local make install-shared

# Configure RocksDB environment variables
cd ..
vim ~/.bashrc

# Add the following content to .bashrc (replace /path/to/rocksdb with actual path)
export CGO_CFLAGS="-I/path/to/rocksdb/include"
export CGO_LDFLAGS="-L/path/to/rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"

# Reload environment variables
source ~/.bashrc
```

#### 3. Go Dependencies
```bash
# Automatically download gRPC, Protocol Buffers and other dependencies
go mod tidy
```

### Build Project

```bash
# Clone project
git clone https://github.com/Dshuishui/Nezha.git
cd Nezha

# Download dependencies
go mod download

# Build project (optional)
go build -o nezha .
```

### Deployment and Startup

#### Standard Three-Node Cluster Deployment

**Startup Command Format**
```bash
go run . -address <CLIENT_IP>:<CLIENT_PORT> \
         -internalAddress <RAFT_IP>:<RAFT_PORT> \
         -peers <PEER1_RAFT_ADDR>,<PEER2_RAFT_ADDR>,<PEER3_RAFT_ADDR> \
         -data <DATA_DIRECTORY>
```

**Specific Startup Examples**

**Using Default Data Directory (current directory)**
```bash
# Node 1 (execute on first machine)
go run . -address IP1:3088 -internalAddress IP1:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881

# Node 2 (execute on second machine)  
go run . -address IP2:3088 -internalAddress IP2:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881

# Node 3 (execute on third machine)
go run . -address IP3:3088 -internalAddress IP3:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881
```

**Using Custom Data Directory**
```bash
# Node 1 (execute on first machine)
go run . -address IP1:3088 -internalAddress IP1:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881 \
         -data "/var/lib/nezha/node1"

# Node 2 (execute on second machine)
go run . -address IP2:3088 -internalAddress IP2:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881 \
         -data "/var/lib/nezha/node2"

# Node 3 (execute on third machine)
go run . -address IP3:3088 -internalAddress IP3:30881 \
         -peers IP1:30881,IP2:30881,IP3:30881 \
         -data "/var/lib/nezha/node3"
```

**Development Environment Example**
```bash
# Using relative paths for development
go run . -address localhost:3088 -internalAddress localhost:30881 \
         -peers localhost:30881,localhost:30882,localhost:30883 \
         -data "./data/node1"
```

**Parameter Description**
| Parameter | Description | Example | Required |
|-----------|-------------|---------|----------|
| `address` | Client access address and port | `192.168.1.10:3088` | Yes |
| `internalAddress` | Raft internal communication address and port | `192.168.1.10:30881` | Yes |
| `peers` | Raft address list of all nodes in the cluster | `IP1:30881,IP2:30881,IP3:30881` | Yes |
| `data` | Data storage directory path | `/var/lib/nezha` or `./data` | No (default: `.`) |

**Data Directory Structure**

When you specify a data directory, Nezha will create the following structure:
```
<data_directory>/
└── data/
    ├── dbfile/
    │   ├── keyIndex/           # LevelDB key-index mapping
    │   ├── newKeyIndex_*/      # GC temporary index files
    │   └── ...
    └── valuelog/
        ├── RaftState.log       # Main Raft state log
        ├── RaftState_sorted_*  # GC sorted log files
        ├── newRaftState_*      # GC temporary log files
        └── ...
```

**Storage Considerations**
- Ensure sufficient disk space (files can grow to 40GB+ before garbage collection)
- Use fast storage (SSD recommended) for better performance
- Consider backup strategies for the data directory
- Each node should have its own separate data directory to avoid conflicts

---

## 📦 Method 2: Pre-compiled Binary Deployment

For users who prefer quick deployment without compilation complexity:

### Quick Start with Pre-built Binaries

1. **Download Release Package**
   ```bash
   # Download the latest release
   wget https://github.com/Dshuishui/Nezha/releases/download/V1.0.0-multiGC/nezha-multiGC-linux-x86_64.tar.gz
   
   # Extract files
   tar -xzf nezha-multiGC-linux-x86_64.tar.gz
   cd nezha-multiGC-linux-x86_64
   ```

2. **Deploy and Run**
   ```bash
   # Make binary executable
   chmod +x nezha
   
   # Start cluster nodes (same commands as Method 1, but use ./nezha instead of go run .)
   ./nezha -address IP1:3088 -internalAddress IP1:30881 \
           -peers IP1:30881,IP2:30881,IP3:30881 \
           -data "/var/lib/nezha/node1"
   ```

### Features
- ✅ **No compilation required** - Ready to run out of the box
- ✅ **All dependencies included** - RocksDB and gflags libraries bundled
- ✅ **Production optimized** - Built with performance optimizations
- ✅ **Cross-platform support** - Available for multiple architectures

👉 **[View complete pre-compiled deployment guide and download latest release](https://github.com/Dshuishui/Nezha/releases/tag/V1.0.0-multiGC)**

---

## 🐳 Method 3: Docker Container Deployment

For modern containerized environments and cloud-native deployments:

### Quick Start with Docker

1. **Single Command Cluster Setup**
   ```bash
   # Clone repository for Docker files
   git clone https://github.com/Dshuishui/Nezha.git
   cd Nezha/docker
   
   # One-command cluster deployment
   ./manage.sh start
   ```

2. **Access Your Cluster**
   ```bash
   # Check cluster status
   ./manage.sh status
   
   # View logs
   ./manage.sh logs
   ```

### Features
- ✅ **One-click deployment** - Full cluster setup with single command
- ✅ **Docker Compose support** - Multi-node orchestration included
- ✅ **Development friendly** - Perfect for local testing and development
- ✅ **Production ready** - Configurable for production environments
- ✅ **Resource management** - Built-in resource limits and monitoring

### Container Architecture
- **Three-node cluster simulation** on single host
- **Automated networking** with Docker Compose
- **Persistent volumes** for data storage
- **Health checks** and monitoring included

👉 **[View complete Docker deployment guide and configuration options](https://github.com/Dshuishui/Nezha/tree/multiGC/docker)**

---

## 🧪 Performance Testing

The system provides comprehensive benchmark tools for thorough performance evaluation of PUT, GET, and SCAN operations.

### Test Commands

**1. PUT Performance Test (Random Write)**
```bash
go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
       -cnums 100 -dnums 39062 -vsize 256000 \
       -servers IP1:3088,IP2:3088,IP3:3088
```

**2. GET Performance Test (Zipf Distribution Read)**
```bash
go run ./benchmark/zipf_read/zipf_read.go \
       -cnums 100 -dnums 10000 \
       -servers IP1:3088,IP2:3088,IP3:3088
```

**3. SCAN Performance Test (Range Scan)**
```bash
go run ./benchmark/scan_pro/scan_pro.go \
       -cnums 1 -dnums 4 \
       -servers IP1:3088,IP2:3088,IP3:3088
```

### Test Parameter Description

| Parameter | Description | Example Value | Applicable Tests |
|-----------|-------------|---------------|------------------|
| `-cnums` | Number of concurrent clients | `100` | All tests |
| `-dnums` | Number of data operations | `39062` | All tests |
| `-vsize` | Value size (bytes) | `256000` | PUT test only |
| `-servers` | Server address list | `IP1:3088,IP2:3088,IP3:3088` | All tests |

### Test Result Description
- **Throughput**: Operations completed per second (ops/sec)
- **Latency**: Average response time per operation (ms)

---

## 📁 Project Structure

```
Nezha/
├── main.go                     # Main program entry
├── go.mod & go.sum            # Go module dependency management
├── client.txt                 # Client test command examples
├── setup-go.sh               # Go environment setup script
│
├── docker/                    # 🐳 Docker deployment files
│   ├── README.md              # Docker deployment guide
│   ├── Dockerfile.ubuntu24    # Container image definition
│   ├── docker-compose.yml     # Multi-node orchestration
│   ├── manage.sh              # Cluster management script
│   └── build.sh               # Image build script
│
├── kvstore/                   # Core storage service layer
│   ├── FlexSync/              # KVS-Raft core implementation
│   │   ├── FlexSync.go        # gRPC server and Raft interaction logic
│   │   ├── GC.go              # Garbage collection strategy implementation
│   │   ├── AnotherGC.go       # Multi-round GC implementation
│   │   └── filePool.go        # File handle pool management
│   ├── LevelDB/               # LevelDB storage engine
│   │   └── LevelDB.go         # LevelDB adapter implementation
│   └── GC/                    # Multiple GC strategy experiments
│
├── raft/                      # Raft consensus protocol layer
│   ├── raft.go               # Raft core implementation (election, log replication)
│   ├── persister.go          # State persistence management
│   └── common.go             # Common data structures and constants
│
├── rpc/                       # gRPC communication protocol definitions
│   ├── kvrpc/                # Client-server RPC interfaces
│   │   └── *.proto           # Put/Get/Scan operation interfaces
│   └── raftrpc/              # Raft inter-node communication interfaces
│       └── *.proto           # RequestVote/AppendEntries etc.
│
├── benchmark/                 # Performance benchmark test suite
│   ├── randwrite/            # Random write tests
│   ├── randread/             # Random read tests
│   ├── seqwrite/             # Sequential write tests
│   ├── scan/                 # Scan operation tests
│   ├── zipf_read/            # Zipf distribution read tests
│   └── cdf/                  # Latency distribution statistics
│
├── ycsb/                      # YCSB standard benchmark tests
│   ├── A/, D/, E/, F/        # Different YCSB workloads
│   └── run_ycsb.sh           # YCSB test execution script
│
├── pool/                      # Connection pool implementation
└── util/                      # Utility function library
```

---

## 📚 Technical Highlights

### Innovative Design
- **Key-Value Separation Optimization**: Separates value storage from key indexing to reduce write amplification
- **Raft Log Reuse**: Stores values directly in Raft logs to avoid redundant persistence
- **Three-Phase Processing**: Pre-GC, During-GC, Post-GC phases ensure system availability

### Performance Advantages
- **Reduced I/O Overhead**: Decreases write operations from at least 3 times to 1 time
- **Enhanced Read Performance**: Hybrid optimization with hash index + sequential storage
- **Intelligent Garbage Collection**: Raft-aware GC mechanism with dynamic performance balancing

---

## 🔧 Troubleshooting

### Common Issues

**1. RocksDB Compilation Failure**
```bash
# Check if dependencies are completely installed
sudo apt-get install build-essential

# Confirm gflags version
cd gflags && git checkout v2.0
```

**2. CGO Linking Error**
```bash
# Check if environment variables are correctly set
echo $CGO_CFLAGS
echo $CGO_LDFLAGS

# Reset paths
export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb"
```

**3. Cluster Startup Failure**
```bash
# Check if ports are occupied
netstat -tulpn | grep :3088
netstat -tulpn | grep :30881

# Check firewall settings
sudo ufw status
```

**4. RocksDB Compilation Error with Missing cstdint Headers**

**Problem**: When compiling RocksDB 5.18.fb on newer systems (Ubuntu 20.04+ with GCC 9+), you may encounter compilation errors like:
```
error: 'uint64_t' does not name a type
note: 'uint64_t' is defined in header '<cstdint>'; did you forget to '#include <cstdint>'?
```

**Cause**: RocksDB 5.18.fb was released before modern C++ standards became widespread. Newer GCC compilers (GCC 9+) require explicit inclusion of `<cstdint>` header for standard integer types (`uint64_t`, `uint32_t`, `uint16_t`, `uint8_t`), but older RocksDB versions don't include this header in all necessary files.

**Solution**: Use the provided fix script to automatically add missing headers:
```bash
# Navigate to RocksDB source directory
cd Nezha

# Run the fix script from project root
./fix-rocksdb.sh
```

**Manual Fix (if script doesn't work)**:
```bash
# Add missing header to specific files as errors occur
sed -i '1i#include <cstdint>' path/to/problematic/file.h

# Example for common files that typically need fixing:
sed -i '1i#include <cstdint>' db/compaction_iteration_stats.h
sed -i '1i#include <cstdint>' table/data_block_hash_index.h
sed -i '1i#include <cstdint>' table/block_based_table_reader.h
sed -i '1i#include <cstdint>' table/block_based_table_builder.h
```

---

## 📞 Contact

For questions, suggestions, or collaboration opportunities, please contact us through:

- 📧 **Submit Issues**: [GitHub Issues](https://github.com/Dshuishui/Nezha/issues)
- ✉️ **Email Contact**: Contact project maintainers through GitHub
- 💬 **Technical Discussion**: Welcome to participate in project discussions and contributions

---

**Contributing to the distributed systems community 🚀**