#!/bin/bash
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

echo "=== Nezha Docker Deployment Test ==="
echo ""

# ── Cleanup ────────────────────────────────────────────────────────────────────
info "Cleaning up any existing nezha container..."
docker stop nezha 2>/dev/null && docker rm nezha 2>/dev/null || docker rm nezha 2>/dev/null || true

# ── Pull latest image ──────────────────────────────────────────────────────────
info "Pulling latest image..."
docker pull dyucong/nezha:latest || fail "Failed to pull image"
pass "Image pulled"

# ── Start container ────────────────────────────────────────────────────────────
info "Starting container..."
docker run -d \
  --name nezha \
  --network host \
  -v nezha-data:/app/data \
  dyucong/nezha:latest \
  -address 127.0.0.1:3088 \
  -internalAddress 127.0.0.1:30881 \
  -peers 127.0.0.1:30881
pass "Container started"

# ── Wait for node to become ready ─────────────────────────────────────────────
info "Waiting for node to start (10s)..."
sleep 10

# ── Check container is still running ──────────────────────────────────────────
if docker ps --filter "name=nezha" --filter "status=running" | grep -q nezha; then
    pass "Container is running"
else
    echo "--- Container logs ---"
    docker logs nezha
    fail "Container exited unexpectedly"
fi

# ── Check port is listening ───────────────────────────────────────────────────
if ss -tulpn | grep -q ":3088"; then
    pass "Port 3088 is listening"
else
    fail "Port 3088 not listening"
fi

# ── Run benchmark ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"

info "Running write benchmark (100 ops)..."
cd "$PROJECT_DIR"
go run ./cmd/bench/randwrite_goroutine/ \
    -cnums 5 -dnums 100 -vsize 1024 \
    -servers 127.0.0.1:3088 && pass "Benchmark completed" || fail "Benchmark failed"

# ── Cleanup ────────────────────────────────────────────────────────────────────
info "Stopping container..."
docker stop nezha && docker rm nezha
docker volume rm nezha-data 2>/dev/null || true

echo ""
echo -e "${GREEN}=== Docker deployment test PASSED ===${NC}"
