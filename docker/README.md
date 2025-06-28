# Nezha Distributed Key-Value Store - Docker Deployment Guide

![Nezha Logo](https://img.shields.io/badge/Nezha-Distributed%20KV%20Store-blue)
![Docker](https://img.shields.io/badge/Docker-Supported-blue)
![Raft](https://img.shields.io/badge/Consensus-Raft-green)

Nezha is a high-performance distributed key-value storage system that implements optimized Raft integration with key-value separation technology, achieving exceptional performance under strong consistency guarantees.

## 📁 Directory Structure

```
docker/
├── Dockerfile.ubuntu24      # Docker image definition (Ubuntu 24.04)
├── build.sh                 # Image build script
├── manage.sh                # Cluster management script
├── docker-compose.yml       # Cluster orchestration file
├── README.md                # This documentation
├── nezha                    # Compiled binary *
├── librocksdb.so.5.18       # RocksDB library file *
└── libgflags.so.2           # gflags library file *
```

**Note:** Files marked with `*` are large library files (approximately 135MB total) and are not included directly in the Git repository. These files can be obtained from the `nezha-multiGC-linux-x86_64.tar.gz` archive in [GitHub Release V1.0.0-multiGC](https://github.com/Dshuishui/Nezha/releases/tag/V1.0.0-multiGC).

## 🚀 Quick Start

### Prerequisites

- Docker Engine 20.0+
- Docker Compose 2.0+
- At least 4GB available memory
- x86_64 architecture

### 1. Build Image

```bash
cd docker
./build.sh
```

The build process includes:
- Creating image based on Ubuntu 24.04
- Installing runtime dependencies
- Copying Nezha binary and library files
- Configuring container environment

### 2. Start Cluster

#### Using Docker Compose (Recommended)

```bash
# One-click start of three-node cluster
./manage.sh start

# Check startup status
./manage.sh status
```

**Note:** This deployment method simulates a three-node cluster on a single host, suitable for development, testing, and demonstration environments.

#### Using Native Docker Commands

```bash
# Start three-node cluster (manual method)
./manage.sh start-manual

# Check cluster status
docker ps | grep nezha
```

### 3. Verify Deployment

```bash
# View all node logs
./manage.sh logs

# View specific node
./manage.sh logs nezha-multigc-node1

# Check cluster status
docker ps | grep nezha
```

## 📋 Management Commands

### Image Management
```bash
# Build image
./manage.sh build

# Test image
./manage.sh test
```

### Cluster Operations
```bash
# Start three-node cluster (Docker Compose)
./manage.sh start

# Start three-node cluster (manual mode)
./manage.sh start-manual

# Stop cluster
./manage.sh stop

# Restart cluster
./manage.sh restart

# Check cluster status
./manage.sh status

# Scale cluster (if supported)
./manage.sh scale 5
```

### Log Management
```bash
# View all node logs
./manage.sh logs

# View specific node logs
./manage.sh logs nezha-multigc-node1
./manage.sh logs nezha-multigc-node2
./manage.sh logs nezha-multigc-node3

# Follow logs in real-time
./manage.sh logs -f nezha-multigc-node1
```

### Testing and Debugging
```bash
# Start single node test
./manage.sh single

# Complete environment cleanup
./manage.sh clean

```

## 🐳 Docker Compose Configuration

### Cluster Architecture

The `docker-compose.yml` defines a three-node Nezha cluster running on a single host:

```yaml
# Simplified configuration example
version: '3.8'
services:
  nezha-node1:
    image: nezha-multigc:latest
    container_name: nezha-multigc-node1
    ports:
      - "3088:3088"
      - "30881:30881"
    environment:
      - NODE_ID=1
    volumes:
      - nezha-data1:/app/data
    networks:
      - nezha-network

  nezha-node2:
    image: nezha-multigc:latest
    container_name: nezha-multigc-node2
    ports:
      - "3089:3088"
      - "30882:30882"
    environment:
      - NODE_ID=2
    volumes:
      - nezha-data2:/app/data
    networks:
      - nezha-network

  nezha-node3:
    image: nezha-multigc:latest
    container_name: nezha-multigc-node3
    ports:
      - "3090:3088"
      - "30883:30883"
    environment:
      - NODE_ID=3
    volumes:
      - nezha-data3:/app/data
    networks:
      - nezha-network

volumes:
  nezha-data1:
  nezha-data2:
  nezha-data3:

networks:
  nezha-network:
    driver: bridge
```

### Cluster Features

- **Single-Host Simulation**: Running three container instances on one host
- **Network Isolation**: Using dedicated Docker network
- **Data Persistence**: Independent data volumes for each node
- **Port Mapping**: Configuration avoiding port conflicts
- **Service Discovery**: Automatic service discovery between containers

## 🔧 Manual Deployment

### Single Container Startup

```bash
# Start single node
docker run -d --name nezha-node1 \
  -p 3088:3088 -p 30881:30881 \
  -v nezha-data:/app/data \
  nezha-multigc:latest \
  -address 0.0.0.0:3088 -internalAddress 0.0.0.0:30881 -peers 127.0.0.1:30881
```

### Three-Node Cluster Manual Deployment

```bash
# Create network
docker network create nezha-network

# Start node 1
docker run -d --name nezha-node1 \
  --network nezha-network \
  -p 3088:3088 -p 30881:30881 \
  -v nezha-data1:/app/data \
  nezha-multigc:latest \
  -address 0.0.0.0:3088 -internalAddress 0.0.0.0:30881 \
  -peers nezha-node1:30881,nezha-node2:30882,nezha-node3:30883

# Start node 2
docker run -d --name nezha-node2 \
  --network nezha-network \
  -p 3089:3088 -p 30882:30882 \
  -v nezha-data2:/app/data \
  nezha-multigc:latest \
  -address 0.0.0.0:3088 -internalAddress 0.0.0.0:30882 \
  -peers nezha-node1:30881,nezha-node2:30882,nezha-node3:30883

# Start node 3
docker run -d --name nezha-node3 \
  --network nezha-network \
  -p 3090:3088 -p 30883:30883 \
  -v nezha-data3:/app/data \
  nezha-multigc:latest \
  -address 0.0.0.0:3088 -internalAddress 0.0.0.0:30883 \
  -peers nezha-node1:30881,nezha-node2:30882,nezha-node3:30883
```

## 📊 Network Configuration

### Port Mapping
| Node | Client Port | Raft Port | Description |
|------|-------------|-----------|-------------|
| Node 1 | 3088 | 30881 | Leader node |
| Node 2 | 3089 | 30882 | Follower node |
| Node 3 | 3090 | 30883 | Follower node |

### Service Discovery
- **Client Connection**: `localhost:3088-3090`
- **Internal Communication**: Docker network automatic discovery

- **Load Balancing**: Clients can connect to any node

### Cluster Communication
```bash
# Inter-node communication test
docker exec nezha-multigc-node1 ping nezha-multigc-node2
docker exec nezha-multigc-node1 telnet nezha-multigc-node2 30882

# Check Raft status
docker exec nezha-multigc-node1 curl http://localhost:3088/status
```

## 💾 Data Persistence

### Data Volumes
- `nezha-multigc-data1`: Node 1 data storage
- `nezha-multigc-data2`: Node 2 data storage
- `nezha-multigc-data3`: Node 3 data storage

### Data Directory Structure
```
/app/data/
├── raft_log/          # Raft log files
├── rocksdb/           # RocksDB data
├── value_log/         # Value log files
└── gc_temp/           # GC temporary files
```

### Backup and Recovery
```bash
# Backup data
docker run --rm -v nezha-multigc-data1:/data -v $(pwd):/backup \
  ubuntu:24.04 tar czf /backup/nezha-backup.tar.gz -C /data .

# Restore data
docker run --rm -v nezha-multigc-data1:/data -v $(pwd):/backup \
  ubuntu:24.04 tar xzf /backup/nezha-backup.tar.gz -C /data
```

## 🚨 Troubleshooting

### Common Issues

#### 1. Build Failure
```bash
# Check necessary files
ls -la nezha librocksdb.so.5.18 libgflags.so.2

# Check file permissions
chmod +x nezha
chmod 644 lib*.so*

# Rebuild
./manage.sh clean
./manage.sh build
```

#### 2. Container Startup Failure
```bash
# View detailed errors
docker logs nezha-multigc-node1

# Check port occupation
netstat -tulpn | grep -E "3088|30881"

# Check disk space
df -h

# Manual startup debugging
docker run -it --rm nezha-multigc:latest /bin/bash
```

#### 3. Cluster Communication Issues
```bash
# Check network
docker network ls
docker network inspect nezha-multigc-network

# Check container network
docker inspect nezha-multigc-node1 | grep -A 10 "NetworkSettings"

# Test network connectivity
docker exec nezha-multigc-node1 ping nezha-multigc-node2
docker exec nezha-multigc-node1 telnet nezha-multigc-node2 30882
```

#### 4. Docker Compose Issues
```bash
# Check compose configuration
docker-compose config

# View service status
docker-compose ps

# Recreate services
docker-compose down
docker-compose up -d

# View service logs
docker-compose logs nezha-node1
```

#### 5. Performance Issues
```bash
# Check resource limits
docker inspect nezha-multigc-node1 | grep -A 5 "Memory\|Cpu"

# Adjust resource limits
docker update --memory 4g --cpus 2 nezha-multigc-node1

# Check disk I/O
docker exec nezha-multigc-node1 iostat -x 1
```
