# Nezha Docker Deployment Guide

This guide covers building and running Nezha using Docker on a single host.

---

## Directory Structure

```
deploy/docker/
├── Dockerfile.ubuntu24      # Image definition (Ubuntu 24.04)
├── docker-compose.yml       # Single-node orchestration
├── manage.sh                # Management script
├── build.sh                 # Image build script
├── README.md                # This guide
├── nezha                    # Compiled binary (required, not in repo)
├── librocksdb.so.5.18       # RocksDB shared library (required, not in repo)
└── libgflags.so.2           # gflags shared library (required, not in repo)
```

The three files marked "not in repo" must be provided before building the image. See [Step 1](#step-1-prepare-required-files) below.

---

## Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- x86_64 (amd64) architecture
- At least 2GB available memory

---

## Step 1: Prepare Required Files

The image bundles the Nezha binary and its RocksDB runtime dependencies. These must be compiled from source and copied into the `deploy/docker/` directory.

Follow [Method 1 in the main README](../README.md#method-1-source-code-compilation) to compile, then:

```bash
# From the project root after compiling
cp nezha deploy/docker/
cp /usr/local/lib/librocksdb.so.5.18 deploy/docker/
cp /usr/local/lib/libgflags.so.2 deploy/docker/
```

Verify:
```bash
ls -lh deploy/docker/nezha deploy/docker/librocksdb.so.5.18 deploy/docker/libgflags.so.2
```

---

## Step 2: Build the Image

```bash
cd deploy/docker
./build.sh
```

Verify the image was built:
```bash
docker images nezha-multigc:latest
```

---

## Step 3: Run

### Using Docker Compose (Recommended)

```bash
cd deploy/docker
./manage.sh start
```

This starts a single Nezha node using host networking. The node listens on:
- **Client port**: `127.0.0.1:3088`
- **Raft port**: `127.0.0.1:30881`

### Using Docker CLI

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

---

## Management Commands

```bash
./manage.sh build    # Build image
./manage.sh start    # Start node
./manage.sh stop     # Stop node
./manage.sh restart  # Restart node
./manage.sh logs     # Follow logs
./manage.sh status   # Show status and port bindings
./manage.sh clean    # Remove containers, volumes, and image
./manage.sh test     # Smoke-test image (prints usage)
```

---

## Networking

The deployment uses Docker **host networking** (`network_mode: "host"`). This means the container shares the host's network stack directly — no port mapping required, and the node is reachable at the host's IP address.

This is the recommended mode for single-host deployments. For multi-machine clusters, run one container per machine and set `-peers` to the actual IP addresses of all nodes.

---

## Data Persistence

Container data is stored in a named Docker volume:

| Volume | Container path | Description |
|--------|---------------|-------------|
| `nezha-multigc-data1` | `/app/data` | All node data (index + value log) |

The actual on-disk layout inside `/app/data`:
```
/app/data/
└── data/
    ├── dbfile/
    │   └── keyIndex/       # LevelDB key-to-offset index
    └── valuelog/
        └── RaftState.log   # Raft log + value store
```

### Backup

```bash
docker run --rm \
  -v nezha-multigc-data1:/data \
  -v $(pwd):/backup \
  ubuntu:24.04 \
  tar czf /backup/nezha-backup.tar.gz -C /data .
```

### Restore

```bash
docker run --rm \
  -v nezha-multigc-data1:/data \
  -v $(pwd):/backup \
  ubuntu:24.04 \
  tar xzf /backup/nezha-backup.tar.gz -C /data
```

---

## Troubleshooting

### Build fails: missing files

```bash
# Check all three required files exist
ls -lh deploy/docker/nezha deploy/docker/librocksdb.so.5.18 deploy/docker/libgflags.so.2
chmod +x deploy/docker/nezha
```

### Container exits immediately

```bash
# View logs for error details
docker logs nezha-multigc-node1

# Smoke-test the binary inside the container
docker run --rm nezha-multigc:latest -h
```

### Port already in use

```bash
ss -tulpn | grep -E "3088|30881"
```

### Removing all data and starting fresh

```bash
cd deploy/docker
./manage.sh clean
./manage.sh start
```
