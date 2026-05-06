#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

case "$1" in
    "build")
        echo "Building Nezha Docker image (first build takes 10-20 min for RocksDB compilation)..."
        PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
        docker build --network=host \
            -f "$SCRIPT_DIR/Dockerfile" \
            -t dshuishui/nezha:latest \
            -t dshuishui/nezha:multiGC \
            "$PROJECT_DIR"
        ;;
    "push")
        echo "Pushing image to Docker Hub..."
        docker push dshuishui/nezha:latest
        docker push dshuishui/nezha:multiGC
        echo "Done. Users can now run: docker pull dshuishui/nezha:latest"
        ;;
    "start")
        echo "Starting Nezha node..."
        docker-compose up -d
        echo ""
        sleep 5
        echo "Node status:"
        docker-compose ps
        echo ""
        echo "Access: client port 127.0.0.1:3088"
        ;;
    "stop")
        echo "Stopping Nezha node..."
        docker-compose down
        ;;
    "restart")
        echo "Restarting Nezha node..."
        docker-compose down
        sleep 2
        docker-compose up -d
        echo ""
        docker-compose ps
        ;;
    "logs")
        if [ -n "$2" ]; then
            docker-compose logs -f "$2"
        else
            docker-compose logs -f
        fi
        ;;
    "status")
        echo "Nezha node status:"
        docker-compose ps
        echo ""
        echo "Image:"
        docker images nezha-multigc:latest
        echo ""
        echo "Listening ports:"
        netstat -tulpn 2>/dev/null | grep -E "3088|30881" || ss -tulpn | grep -E "3088|30881" || echo "No ports found"
        ;;
    "clean")
        echo "Cleaning Nezha node and data..."
        docker-compose down -v
        docker rmi nezha-multigc:latest 2>/dev/null || true
        echo "Done"
        ;;
    "test")
        echo "Testing Nezha image..."
        docker run --rm nezha-multigc:latest -h
        ;;
    *)
        echo "Nezha Docker Management Script"
        echo ""
        echo "Usage: $0 {build|push|start|stop|restart|logs|status|clean|test}"
        echo ""
        echo "Commands:"
        echo "  build    - Build Docker image from source (10-20 min first time)"
        echo "  push     - Push image to Docker Hub (requires docker login)"
        echo "  start    - Start node"
        echo "  stop     - Stop node"
        echo "  restart  - Restart node"
        echo "  logs     - View logs"
        echo "  status   - View status"
        echo "  clean    - Remove containers, volumes, and image"
        echo "  test     - Test image"
        echo ""
        echo "Examples:"
        echo "  $0 build && $0 push   # Build and publish to Docker Hub"
        echo "  $0 start              # Start node (pulls image if not built locally)"
        echo "  $0 logs               # Follow logs"
        exit 1
        ;;
esac
