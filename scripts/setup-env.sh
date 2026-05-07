#!/bin/bash
set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
GO_VERSION="1.24.0"

info "=== Nezha Environment Setup ==="
info "Project: $PROJECT_DIR"
echo ""

# ── Step 1: System dependencies ────────────────────────────────────────────────
info "Step 1/4: Installing system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y gcc g++ make git wget \
    librocksdb-dev \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev
info "System dependencies installed."
echo ""

# ── Step 2: Go ─────────────────────────────────────────────────────────────────
info "Step 2/4: Installing Go $GO_VERSION..."
if command -v go &>/dev/null && [[ "$(go version 2>/dev/null)" == *"go$GO_VERSION"* ]]; then
    info "Go $GO_VERSION already installed, skipping."
else
    wget -q "https://golang.google.cn/dl/go${GO_VERSION}.linux-amd64.tar.gz" -O /tmp/go.tar.gz
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

# ── Step 3: CGO environment variables ──────────────────────────────────────────
info "Step 3/4: Configuring CGO environment variables..."

add_to_bashrc() {
    local line="$1"
    if ! grep -qF "$line" ~/.bashrc; then
        echo "$line" >> ~/.bashrc
    fi
}

ROCKSDB_LIB_DIR=""
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    if [ -f "$d/librocksdb.so" ]; then
        ROCKSDB_LIB_DIR="$d"
        break
    fi
done

[ -z "$ROCKSDB_LIB_DIR" ] && error "librocksdb.so not found after installation."

add_to_bashrc 'export CGO_CFLAGS="-I/usr/include"'
add_to_bashrc "export CGO_LDFLAGS=\"-L${ROCKSDB_LIB_DIR} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd\""
add_to_bashrc "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:${ROCKSDB_LIB_DIR}"

export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L${ROCKSDB_LIB_DIR} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${ROCKSDB_LIB_DIR}

info "CGO environment configured (RocksDB at ${ROCKSDB_LIB_DIR})."
echo ""

# ── Step 4: Go module dependencies ─────────────────────────────────────────────
info "Step 4/4: Downloading Go module dependencies..."
cd "$PROJECT_DIR"
go mod download
info "Dependencies downloaded."
echo ""

info "=== Setup complete! ==="
echo ""
echo "To start the node, run:"
echo "  source ~/.bashrc"
echo "  ./scripts/run-node.sh"
