# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake wget ca-certificates \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev \
    golang-1.22-go \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=$PATH:/usr/lib/go-1.22/bin

# RocksDB 8.11.3: wget tarball is more reliable than git clone
RUN wget -q https://github.com/facebook/rocksdb/archive/refs/tags/v8.11.3.tar.gz \
        -O /tmp/rocksdb.tar.gz \
    && tar -xzf /tmp/rocksdb.tar.gz -C /tmp \
    && cmake -S /tmp/rocksdb-8.11.3 -B /tmp/rocksdb-build \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -DROCKSDB_BUILD_SHARED=ON \
        -DWITH_SNAPPY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON \
        -DWITH_ZLIB=ON -DWITH_BZ2=ON -DWITH_GFLAGS=ON \
        -DFAIL_ON_WARNINGS=OFF \
        -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
    && cmake --build /tmp/rocksdb-build -j$(nproc) \
    && cmake --install /tmp/rocksdb-build \
    && rm -rf /tmp/rocksdb*

# Download Go modules first for better layer caching
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=https://goproxy.cn,direct go mod download

# Build Nezha binary
COPY . .
RUN CGO_CFLAGS="-I/usr/local/include" \
    CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
    go build -o /nezha ./kvstore/FlexSync/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy1v5 zlib1g libbz2-1.0 liblz4-1 libzstd1 \
    libstdc++6 libgflags2.2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/librocksdb.so.8* /usr/local/lib/
RUN ldconfig

COPY --from=builder /nezha /usr/local/bin/nezha

RUN groupadd -g 1001 nezha \
    && useradd -r -u 1001 -g nezha nezha \
    && mkdir -p /app/data \
    && chown nezha:nezha /app/data

USER nezha
WORKDIR /app

EXPOSE 3088 30881

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD pgrep nezha > /dev/null || exit 1

ENTRYPOINT ["/usr/local/bin/nezha", "-data", "/app/data"]
