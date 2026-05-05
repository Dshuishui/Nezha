#!/bin/bash
set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# ── Configuration (edit these if needed) ───────────────────────────────────────
ADDRESS="${ADDRESS:-127.0.0.1:3088}"
INTERNAL_ADDRESS="${INTERNAL_ADDRESS:-127.0.0.1:30881}"
PEERS="${PEERS:-127.0.0.1:30881}"
DATA_DIR="${DATA_DIR:-$PROJECT_DIR/data}"

# ── Source environment ──────────────────────────────────────────────────────────
export PATH=$PATH:/usr/local/go/bin
export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

# ── Checks ─────────────────────────────────────────────────────────────────────
if ! command -v go &>/dev/null; then
    echo "Go not found. Run scripts/setup-env.sh first."
    exit 1
fi

if [ ! -f /usr/local/lib/librocksdb.so ]; then
    echo "RocksDB not found. Run scripts/setup-env.sh first."
    exit 1
fi

# ── Start ──────────────────────────────────────────────────────────────────────
info "=== Starting Nezha Node ==="
info "Client address  : $ADDRESS"
info "Raft address    : $INTERNAL_ADDRESS"
info "Peers           : $PEERS"
info "Data directory  : $DATA_DIR"
echo ""
warn "Press Ctrl+C to stop."
echo ""

cd "$PROJECT_DIR"
exec go run ./kvstore/FlexSync/ \
    -address "$ADDRESS" \
    -internalAddress "$INTERNAL_ADDRESS" \
    -peers "$PEERS" \
    -data "$DATA_DIR"
