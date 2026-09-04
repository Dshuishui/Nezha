#!/bin/bash

echo "=== Nezha multiGC Docker 构建脚本 ==="

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "当前工作目录: $(pwd)"

# 检查必要文件
echo "检查必要文件..."

if [ ! -f "nezha" ]; then
    echo "❌ 错误: nezha 可执行文件不存在"
    echo "请确保已将编译好的 nezha 复制到 deploy/docker/ 目录"
    exit 1
fi

if [ ! -f "librocksdb.so.5.18" ]; then
    echo "❌ 错误: librocksdb.so.5.18 库文件不存在"
    echo "请确保已将 RocksDB 库文件复制到 deploy/docker/ 目录"
    exit 1
fi

if [ ! -f "libgflags.so.2" ]; then
    echo "❌ 错误: libgflags.so.2 库文件不存在"
    echo "请确保已将 gflags 库文件复制到 deploy/docker/ 目录"
    exit 1
fi

if [ ! -f "Dockerfile.ubuntu24" ]; then
    echo "❌ 错误: Dockerfile.ubuntu24 不存在"
    exit 1
fi

echo "✓ 所有必要文件检查通过"

# 显示文件信息
echo ""
echo "构建文件列表:"
ls -lh nezha librocksdb.so.5.18 libgflags.so.2 Dockerfile.ubuntu24

echo ""
echo "开始构建 Docker 镜像..."

# 构建 Ubuntu 24.04 版本
docker build -f Dockerfile.ubuntu24 -t nezha-multigc:latest .

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Docker 镜像构建成功！"
    echo ""
    echo "镜像信息:"
    docker images nezha-multigc:latest
    
    # 显示镜像大小
    IMAGE_SIZE=$(docker images nezha-multigc:latest --format "table {{.Size}}" | tail -n +2)
    echo "镜像大小: $IMAGE_SIZE"
    
    echo ""
    echo "📋 使用方法:"
    echo ""
    echo "1. 测试镜像:"
    echo "   docker run --rm nezha-multigc:latest -h"
    echo ""
    echo "2. 单节点启动:"
    echo "   docker run -d --name nezha-node1 \\"
    echo "     -p 3088:3088 -p 30881:30881 \\"
    echo "     -v nezha-data:/app/data \\"
    echo "     nezha-multigc:latest \\"
    echo "     -address 0.0.0.0:3088 -internalAddress 0.0.0.0:30881 -peers 127.0.0.1:30881"
    echo ""
    echo "3. 使用 Docker Compose 启动集群:"
    echo "   docker-compose -f docker-compose.yml up -d"
    echo ""
    echo "4. 管理集群:"
    echo "   ./manage.sh start    # 启动集群"
    echo "   ./manage.sh stop     # 停止集群"
    echo "   ./manage.sh status   # 查看状态"
    echo "   ./manage.sh logs     # 查看日志"
    echo ""
else
    echo "❌ Docker 镜像构建失败"
    exit 1
fi
