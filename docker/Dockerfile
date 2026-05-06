# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive
ENV PATH=$PATH:/usr/local/go/bin

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ cmake make git wget ca-certificates \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev \
    && rm -rf /var/lib/apt/lists/*

# Go 1.22
RUN wget -q https://go.dev/dl/go1.22.0.linux-amd64.tar.gz -O /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz

# RocksDB v8.11.3
RUN git clone --depth 1 --branch v8.11.3 \
        https://github.com/facebook/rocksdb.git /tmp/rocksdb \
    && cmake -S /tmp/rocksdb -B /tmp/rocksdb/build \
        -DCMAKE_BUILD_TYPE=Release \
        -DROCKSDB_BUILD_SHARED=ON \
        -DWITH_SNAPPY=ON -DWITH_LZ4=ON -DWITH_ZSTD=ON \
        -DWITH_ZLIB=ON -DWITH_BZ2=ON -DWITH_GFLAGS=ON \
        -DFAIL_ON_WARNINGS=OFF \
        -DWITH_TESTS=OFF -DWITH_TOOLS=OFF -DWITH_BENCHMARK_TOOLS=OFF \
    && cmake --build /tmp/rocksdb/build -j$(nproc) \
    && cmake --install /tmp/rocksdb/build \
    && rm -rf /tmp/rocksdb

# Build Nezha binary
WORKDIR /src
COPY . .
RUN CGO_CFLAGS="-I/usr/local/include" \
    CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
    go build -o /nezha ./kvstore/FlexSync/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsnappy1v5 zlib1g libbz2-1.0 liblz4-1 libzstd1 \
    libstdc++6 libc6 libgcc-s1 libgflags2.2 \
    ca-certificates \
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
