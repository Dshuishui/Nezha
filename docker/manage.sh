#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

case "$1" in
    "build")
        echo "Building Nezha Docker image..."
        ./build.sh
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
        echo "Usage: $0 {build|start|stop|restart|logs|status|clean|test}"
        echo ""
        echo "Commands:"
        echo "  build    - Build Docker image"
        echo "  start    - Start node"
        echo "  stop     - Stop node"
        echo "  restart  - Restart node"
        echo "  logs     - View logs"
        echo "  status   - View status"
        echo "  clean    - Remove containers, volumes, and image"
        echo "  test     - Test image"
        echo ""
        echo "Examples:"
        echo "  $0 build    # Build image first"
        echo "  $0 start    # Start node"
        echo "  $0 logs     # Follow logs"
        exit 1
        ;;
esac
