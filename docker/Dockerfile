# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git ca-certificates \
    golang-1.22-go \
    librocksdb-dev \
    libsnappy-dev zlib1g-dev libbz2-dev \
    liblz4-dev libzstd-dev libgflags-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=$PATH:/usr/lib/go-1.22/bin

# Download Go modules first for better layer caching
WORKDIR /src
COPY go.mod go.sum ./
RUN GOPROXY=https://goproxy.cn,direct go mod download

# Build Nezha binary
COPY . .
RUN CGO_CFLAGS="-I/usr/include" \
    CGO_LDFLAGS="-L/usr/lib/x86_64-linux-gnu -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd" \
    go build -o /nezha ./kvstore/FlexSync/

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    librocksdb8.9 \
    libsnappy1v5 zlib1g libbz2-1.0 liblz4-1 libzstd1 \
    libstdc++6 libgflags2.2 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

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
