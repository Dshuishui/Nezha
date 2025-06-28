cat > manage.sh << 'EOF'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 默认使用主机网络模式
COMPOSE_FILE="docker-compose.yml"

case "$1" in
    "build")
        echo "构建 Docker 镜像..."
        ./build.sh
        ;;
    "start")
        echo "启动 Nezha multiGC 集群（主机网络模式）..."
        docker-compose up -d
        echo ""
        echo "集群启动中，请稍等..."
        sleep 10
        echo ""
        echo "集群状态:"
        docker-compose ps
        echo ""
        echo "端口分配:"
        echo "- 节点1: 客户端 127.0.0.1:3088, Raft 127.0.0.1:30881"
        echo "- 节点2: 客户端 127.0.0.1:3089, Raft 127.0.0.1:30882"  
        echo "- 节点3: 客户端 127.0.0.1:3090, Raft 127.0.0.1:30883"
        ;;
    "start-fixed-ip")
        echo "启动 Nezha multiGC 集群（固定IP模式）..."
        docker-compose -f docker-compose-fixed-ip.yml up -d
        echo ""
        echo "集群启动中，请稍等..."
        sleep 10
        echo ""
        echo "集群状态:"
        docker-compose -f docker-compose-fixed-ip.yml ps
        ;;
    "stop")
        echo "停止 Nezha multiGC 集群..."
        docker-compose down
        docker-compose -f docker-compose-fixed-ip.yml down 2>/dev/null || true
        ;;
    "restart")
        echo "重启 Nezha multiGC 集群..."
        docker-compose down
        sleep 2
        docker-compose up -d
        echo ""
        echo "集群重启完成，状态:"
        docker-compose ps
        ;;
    "logs")
        echo "查看 Nezha multiGC 集群日志..."
        if [ -n "$2" ]; then
            echo "查看 $2 节点日志:"
            docker-compose logs -f "$2"
        else
            echo "查看所有节点日志:"
            docker-compose logs -f
        fi
        ;;
    "status")
        echo "Nezha multiGC 集群状态:"
        docker-compose ps
        echo ""
        echo "镜像信息:"
        docker images nezha-multigc:latest
        echo ""
        echo "主机端口监听:"
        netstat -tulpn | grep -E "3088|3089|3090|30881|30882|30883" || echo "无相关端口监听"
        ;;
    "clean")
        echo "清理 Nezha multiGC 集群和数据..."
        docker-compose down -v
        docker-compose -f docker-compose-fixed-ip.yml down -v 2>/dev/null || true
        docker rmi nezha-multigc:latest 2>/dev/null || true
        echo "清理完成"
        ;;
    "test")
        echo "测试 Nezha multiGC 镜像..."
        docker run --rm nezha-multigc:latest -h
        ;;
    "single")
        echo "启动单节点 Nezha multiGC（修复版）..."
        docker run -d \
          --name nezha-multigc-single \
          --network host \
          -v nezha-single-data:/app/data \
          nezha-multigc:latest \
          -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 -peers 127.0.0.1:30881 \
          -data /app/data
        
        echo ""
        echo "单节点启动完成:"
        echo "- 客户端访问: http://127.0.0.1:3088"
        echo "- 查看日志: docker logs nezha-multigc-single"
        echo "- 停止节点: docker stop nezha-multigc-single && docker rm nezha-multigc-single"
        ;;
    *)
        echo "Nezha multiGC Docker 管理脚本"
        echo ""
        echo "用法: $0 {build|start|start-fixed-ip|stop|restart|logs|status|clean|test|single}"
        echo ""
        echo "命令说明:"
        echo "  build        - 构建 Docker 镜像"
        echo "  start        - 启动三节点集群（主机网络）"
        echo "  start-fixed-ip - 启动三节点集群（固定IP）"
        echo "  stop         - 停止集群"
        echo "  restart      - 重启集群"
        echo "  logs         - 查看日志"
        echo "  status       - 查看集群状态"
        echo "  clean        - 清理所有"
        echo "  test         - 测试镜像"
        echo "  single       - 启动单节点测试"
        echo ""
        echo "示例:"
        echo "  $0 start             # 启动主机网络集群"
        echo "  $0 start-fixed-ip    # 启动固定IP集群"
        echo "  $0 single            # 单节点测试"
        exit 1
        ;;
esac
EOF
