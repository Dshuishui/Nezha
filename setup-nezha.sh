#!/bin/bash

#######################################################
# Nezha 项目自动化部署脚本
# 适用于全新的 Ubuntu 20.04/22.04 云服务器
# RocksDB: 8.11.3
# grocksdb: v1.6.22
#######################################################

set -e  # 遇到错误立即退出

echo "======================================================"
echo "Nezha 项目环境自动化部署脚本"
echo "======================================================"

# 配置变量
GO_VERSION="1.21.6"
ROCKSDB_VERSION="8.11.3"
GROCKSDB_VERSION="v1.6.22"
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

# 检查是否已安装
if [ -f "/usr/local/lib/librocksdb.so" ]; then
    echo "检测到已安装 RocksDB，先卸载..."
    sudo rm -rf /usr/local/include/rocksdb
    sudo rm -f /usr/local/lib/librocksdb*
    sudo rm -f /usr/local/lib/pkgconfig/rocksdb.pc
    sudo ldconfig
fi

cd /tmp
wget https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz
tar -xzf v${ROCKSDB_VERSION}.tar.gz
cd rocksdb-${ROCKSDB_VERSION}

# 使用 CMake 编译
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release \
      -DPORTABLE=ON \
      -DWITH_SNAPPY=ON \
      -DWITH_LZ4=ON \
      -DWITH_ZLIB=ON \
      -DWITH_ZSTD=ON \
      ..

make -j$(nproc)
sudo make install
sudo ldconfig

# 验证安装
if [ -f "/usr/local/lib/librocksdb.so" ]; then
    log_success "RocksDB ${ROCKSDB_VERSION} 安装完成"
else
    log_error "RocksDB 安装失败"
    exit 1
fi

# 清理临时文件
cd /tmp
rm -rf rocksdb-${ROCKSDB_VERSION} v${ROCKSDB_VERSION}.tar.gz

# ========== 步骤 5: 设置环境变量 ==========
echo ""
echo "步骤 5/7: 配置环境变量..."

if ! grep -q "RocksDB 环境变量" ~/.bashrc; then
    cat >> ~/.bashrc << 'EOF'

# RocksDB 环境变量
export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb"
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export CGO_ENABLED=1
EOF
    log_success "环境变量已添加到 ~/.bashrc"
else
    log_success "环境变量已存在"
fi

# 立即应用环境变量
export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb"
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
export CGO_ENABLED=1

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

# 设置 grocksdb 版本
go get github.com/linxGnu/grocksdb@${GROCKSDB_VERSION}
go mod tidy

# 清理缓存
go clean -cache -modcache

# 编译项目
cd kvstore/FlexSync
go build -o nezha .

if [ -f "./nezha" ]; then
    log_success "项目编译成功！可执行文件: $(pwd)/nezha"
else
    log_error "项目编译失败"
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