#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Nezha Source Deployment Test ==="
echo ""

# ── Step 1: setup-env.sh ───────────────────────────────────────────────────────
info "Running setup-env.sh..."
bash "$SCRIPT_DIR/setup-env.sh" || fail "setup-env.sh failed"
source ~/.bashrc
pass "Environment setup complete"

# ── Step 2: Go available ───────────────────────────────────────────────────────
if command -v go &>/dev/null; then
    pass "Go available: $(go version)"
else
    fail "Go not found after setup"
fi

# ── Step 3: RocksDB available ─────────────────────────────────────────────────
if ldconfig -p | grep -q librocksdb; then
    pass "RocksDB library found"
else
    fail "librocksdb not found"
fi

# ── Step 4: Build binary ───────────────────────────────────────────────────────
info "Building nezha binary..."
cd "$PROJECT_DIR"
go build -o /tmp/nezha-test ./cmd/nezha/ || fail "Build failed"
pass "Build successful"

# ── Step 5: Start node ────────────────────────────────────────────────────────
info "Starting node..."
DATA_DIR="$(mktemp -d)"
/tmp/nezha-test \
    -address 127.0.0.1:3088 \
    -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 \
    -data "$DATA_DIR" &
NODE_PID=$!

info "Waiting for node to start (10s)..."
sleep 10

if kill -0 "$NODE_PID" 2>/dev/null; then
    pass "Node is running (pid=$NODE_PID)"
else
    fail "Node exited unexpectedly"
fi

if ss -tulpn | grep -q ":3088"; then
    pass "Port 3088 is listening"
else
    kill "$NODE_PID" 2>/dev/null || true
    fail "Port 3088 not listening"
fi

# ── Step 6: Benchmark ─────────────────────────────────────────────────────────
info "Running write benchmark (100 ops)..."
go run ./cmd/bench/randwrite_goroutine/ \
    -cnums 5 -dnums 100 -vsize 1024 \
    -servers 127.0.0.1:3088 && pass "Benchmark completed" || fail "Benchmark failed"

# ── Cleanup ────────────────────────────────────────────────────────────────────
info "Stopping node..."
kill "$NODE_PID" 2>/dev/null || true
rm -rf "$DATA_DIR" /tmp/nezha-test

echo ""
echo -e "${GREEN}=== Source deployment test PASSED ===${NC}"
