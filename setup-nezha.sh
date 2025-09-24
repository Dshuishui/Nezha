#!/bin/bash

#######################################################
# Nezha 项目自动化部署脚本
# 适用于全新的 Ubuntu 20.04/22.04 云服务器
# RocksDB: 8.11.3
# grocksdb: v1.9.3
#######################################################

set -e  # 遇到错误立即退出

echo "======================================================"
echo "Nezha 项目环境自动化部署脚本"
echo "======================================================"

# 配置变量
GO_VERSION="1.21.6"
ROCKSDB_VERSION="8.11.3"
GROCKSDB_VERSION="v1.9.3"
PROJECT_REPO="https://github.com/Dshuishui/Nezha.git"
PROJECT_DIR="$HOME/Nezha"

# 颜色输出
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# ========== 步骤 1: 更新系统 ==========
echo ""
echo "步骤 1/7: 更新系统包..."
sudo apt update && sudo apt upgrade -y
log_success "系统更新完成"

# ========== 步骤 2: 安装基础依赖 ==========
echo ""
echo "步骤 2/7: 安装基础依赖和编译工具..."
sudo apt install -y \
    build-essential \
    git \
    wget \
    curl \
    cmake \
    gcc \
    g++ \
    make \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    pkg-config

log_success "基础依赖安装完成"

# ========== 步骤 3: 安装 Go ==========
echo ""
echo "步骤 3/7: 安装 Go ${GO_VERSION}..."

if command -v go &> /dev/null; then
    CURRENT_GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
    echo "检测到已安装 Go ${CURRENT_GO_VERSION}"
    if [[ "$CURRENT_GO_VERSION" == "$GO_VERSION" ]]; then
        log_success "Go 版本正确，跳过安装"
    else
        echo "版本不匹配，重新安装..."
        sudo rm -rf /usr/local/go
    fi
fi

if ! command -v go &> /dev/null || [[ "$CURRENT_GO_VERSION" != "$GO_VERSION" ]]; then
    cd /tmp
    wget https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz
    sudo tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz
    rm go${GO_VERSION}.linux-amd64.tar.gz
    
    # 配置 Go 环境变量
    if ! grep -q "/usr/local/go/bin" ~/.bashrc; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
        echo 'export GOPATH=$HOME/go' >> ~/.bashrc
        echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bashrc
    fi
    
    export PATH=$PATH:/usr/local/go/bin
    export GOPATH=$HOME/go
    
    log_success "Go ${GO_VERSION} 安装完成"
fi

go version

# ========== 步骤 4: 安装 RocksDB ==========
echo ""
echo "步骤 4/7: 安装 RocksDB ${ROCKSDB_VERSION}..."

# 卸载旧版本
sudo rm -rf /usr/local/include/rocksdb /usr/local/lib/librocksdb*
sudo rm -rf /usr/include/rocksdb /usr/lib/x86_64-linux-gnu/librocksdb*
sudo ldconfig

# 清理可能存在的临时文件
rm -rf /tmp/rocksdb-${ROCKSDB_VERSION}* /tmp/v${ROCKSDB_VERSION}.tar.gz*

# 安装 RocksDB 8.11.3
cd /tmp
echo "下载 RocksDB ${ROCKSDB_VERSION}..."
wget -O rocksdb-${ROCKSDB_VERSION}.tar.gz https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz
tar -xzf rocksdb-${ROCKSDB_VERSION}.tar.gz

echo "编译 RocksDB ${ROCKSDB_VERSION}..."
cd rocksdb-${ROCKSDB_VERSION}

# 清理可能存在的 build 目录
rm -rf build
mkdir build
cd build

echo "当前目录: $(pwd)"
echo "运行 CMake 配置..."

cmake -DCMAKE_BUILD_TYPE=Release \
      -DPORTABLE=ON \
      -DWITH_SNAPPY=ON \
      -DWITH_LZ4=ON \
      -DWITH_ZLIB=ON \
      -DWITH_ZSTD=ON \
      -DWITH_GFLAGS=OFF \
      -DWITH_BENCHMARK_TOOLS=OFF \
      -DWITH_CORE_TOOLS=OFF \
      -DWITH_TOOLS=OFF \
      -DFAIL_ON_WARNINGS=OFF \
      ..

echo "开始编译..."
make -j$(nproc)

echo "安装 RocksDB..."
sudo make install
sudo ldconfig

# 修正：检查实际的安装位置
ROCKSDB_INSTALLED=false
if [ -f "/usr/local/lib/librocksdb.so" ]; then
    ROCKSDB_LIB_DIR="/usr/local/lib"
    ROCKSDB_INCLUDE_DIR="/usr/local/include"
    ROCKSDB_INSTALLED=true
elif [ -f "/usr/lib/x86_64-linux-gnu/librocksdb.so" ]; then
    ROCKSDB_LIB_DIR="/usr/lib/x86_64-linux-gnu"
    ROCKSDB_INCLUDE_DIR="/usr/include"
    ROCKSDB_INSTALLED=true
fi

if [ "$ROCKSDB_INSTALLED" = true ]; then
    log_success "RocksDB ${ROCKSDB_VERSION} 安装完成"
    echo "库目录: ${ROCKSDB_LIB_DIR}"
    echo "头文件目录: ${ROCKSDB_INCLUDE_DIR}"
    echo "安装的库文件:"
    ls -la ${ROCKSDB_LIB_DIR}/librocksdb*
else
    log_error "RocksDB 安装失败"
    exit 1
fi

# 清理临时文件
cd /tmp
rm -rf rocksdb-${ROCKSDB_VERSION}* v${ROCKSDB_VERSION}.tar.gz*

# ========== 步骤 5: 设置环境变量 ==========
echo ""
echo "步骤 5/7: 配置环境变量..."

# 移除旧的环境变量
sed -i '/# RocksDB 环境变量/,+4d' ~/.bashrc 2>/dev/null || true

# 添加新的环境变量（使用实际的安装路径）
cat >> ~/.bashrc << EOF

# RocksDB 环境变量
export CGO_CFLAGS="-I${ROCKSDB_INCLUDE_DIR}"
export CGO_LDFLAGS="-L${ROCKSDB_LIB_DIR} -lrocksdb"
export LD_LIBRARY_PATH=${ROCKSDB_LIB_DIR}:\$LD_LIBRARY_PATH
export CGO_ENABLED=1
EOF

log_success "环境变量已更新到 ~/.bashrc"

# 立即应用环境变量
export CGO_CFLAGS="-I${ROCKSDB_INCLUDE_DIR}"
export CGO_LDFLAGS="-L${ROCKSDB_LIB_DIR} -lrocksdb"
export LD_LIBRARY_PATH=${ROCKSDB_LIB_DIR}:$LD_LIBRARY_PATH
export CGO_ENABLED=1

echo "当前环境变量:"
echo "  CGO_CFLAGS: $CGO_CFLAGS"
echo "  CGO_LDFLAGS: $CGO_LDFLAGS"
echo "  LD_LIBRARY_PATH: $LD_LIBRARY_PATH"

# ========== 步骤 6: 克隆项目 ==========
echo ""
echo "步骤 6/7: 克隆 Nezha 项目..."

if [ -d "$PROJECT_DIR" ]; then
    echo "项目目录已存在，更新代码..."
    cd "$PROJECT_DIR"
    git pull
else
    git clone "$PROJECT_REPO" "$PROJECT_DIR"
    cd "$PROJECT_DIR"
fi

log_success "项目代码已就绪"

# ========== 步骤 7: 安装 Go 依赖并编译 ==========
echo ""
echo "步骤 7/7: 安装 Go 依赖并编译项目..."

# 更新项目依赖
cd "$PROJECT_DIR/kvstore/FlexSync"
go get github.com/linxGnu/grocksdb@${GROCKSDB_VERSION}
go mod tidy
go clean -cache

echo "开始编译项目..."
go build -o nezha .

if [ -f "./nezha" ]; then
    log_success "项目编译成功！可执行文件: $(pwd)/nezha"
else
    log_error "项目编译失败"
    # 显示详细错误信息
    echo "尝试显示编译错误..."
    go build -v -o nezha . || true
    exit 1
fi

# ========== 完成 ==========
echo ""
echo "======================================================"
echo "环境部署完成！"
echo "======================================================"
echo ""
echo "环境信息："
echo "  - Go 版本: $(go version | awk '{print $3}')"
echo "  - RocksDB 版本: ${ROCKSDB_VERSION}"
echo "  - RocksDB 库目录: ${ROCKSDB_LIB_DIR}"
echo "  - RocksDB 头文件目录: ${ROCKSDB_INCLUDE_DIR}"
echo "  - grocksdb 版本: ${GROCKSDB_VERSION}"
echo "  - 项目目录: ${PROJECT_DIR}"
echo "  - 可执行文件: ${PROJECT_DIR}/kvstore/FlexSync/nezha"
echo ""
echo "运行项目："
echo "  cd ${PROJECT_DIR}/kvstore/FlexSync"
echo "  ./nezha -address <地址:端口> -internalAddress <内部地址:端口> -peers <节点列表> -gap <间隔>"
echo ""
echo "示例命令："
echo "  ./nezha -address 192.168.0.43:3088 -internalAddress 192.168.0.43:30881 -peers 192.168.0.43:30881 -gap 40000"
echo ""
echo "使环境变量生效："
echo "  source ~/.bashrc"
echo ""
echo "======================================================"