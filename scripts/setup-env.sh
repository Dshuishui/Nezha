#!/bin/bash
set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GO_VERSION="1.22.0"
ROCKSDB_VERSION="v8.11.3"
ROCKSDB_DIR="$HOME/rocksdb"

info "=== Nezha Environment Setup ==="
info "Project: $PROJECT_DIR"
echo ""

# ── Step 1: System dependencies ────────────────────────────────────────────────
info "Step 1/5: Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y gcc g++ cmake make git wget \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev
info "System dependencies installed."
echo ""

# ── Step 2: Go ─────────────────────────────────────────────────────────────────
info "Step 2/5: Installing Go $GO_VERSION..."
if command -v go &>/dev/null && [[ "$(go version)" == *"go$GO_VERSION"* ]]; then
    info "Go $GO_VERSION already installed, skipping."
else
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf /tmp/go.tar.gz
    rm /tmp/go.tar.gz

    if ! grep -q '/usr/local/go/bin' ~/.bashrc; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    fi
    export PATH=$PATH:/usr/local/go/bin
    info "Go $(go version) installed."
fi
echo ""

# ── Step 3: RocksDB ────────────────────────────────────────────────────────────
info "Step 3/5: Building RocksDB $ROCKSDB_VERSION..."
if [ -f /usr/local/lib/librocksdb.so ]; then
    info "RocksDB already installed at /usr/local/lib, skipping."
else
    if [ ! -d "$ROCKSDB_DIR" ]; then
        info "Cloning RocksDB $ROCKSDB_VERSION..."
        git clone --depth 1 --branch "$ROCKSDB_VERSION" \
            https://github.com/facebook/rocksdb.git "$ROCKSDB_DIR"
    else
        info "RocksDB source already present at $ROCKSDB_DIR"
    fi

    cd "$ROCKSDB_DIR"
    mkdir -p build && cd build

    info "Configuring RocksDB with CMake..."
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DROCKSDB_BUILD_SHARED=ON \
        -DWITH_SNAPPY=ON \
        -DWITH_LZ4=ON \
        -DWITH_ZSTD=ON \
        -DWITH_ZLIB=ON \
        -DWITH_BZ2=ON \
        -DWITH_GFLAGS=ON \
        -DFAIL_ON_WARNINGS=OFF \
        -DWITH_TESTS=OFF \
        -DWITH_TOOLS=OFF \
        -DWITH_BENCHMARK_TOOLS=OFF

    info "Compiling RocksDB (this takes 5–10 minutes)..."
    make -j$(nproc)

    info "Installing RocksDB..."
    sudo make install
    sudo ldconfig

    cd "$PROJECT_DIR"
    info "RocksDB $ROCKSDB_VERSION installed."
fi
echo ""

# ── Step 4: CGO environment variables ──────────────────────────────────────────
info "Step 4/5: Configuring CGO environment variables..."

add_to_bashrc() {
    local line="$1"
    if ! grep -qF "$line" ~/.bashrc; then
        echo "$line" >> ~/.bashrc
    fi
}

add_to_bashrc 'export CGO_CFLAGS="-I/usr/local/include"'
add_to_bashrc 'export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"'
add_to_bashrc 'export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib'

export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

info "CGO environment configured."
echo ""

# ── Step 5: Go module dependencies ─────────────────────────────────────────────
info "Step 5/5: Downloading Go module dependencies..."
cd "$PROJECT_DIR"
go mod download
info "Dependencies downloaded."
echo ""

info "=== Setup complete! ==="
echo ""
echo "To start the node, run:"
echo "  source ~/.bashrc"
echo "  ./scripts/run-node.sh"
