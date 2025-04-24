# #!/bin/bash

# # 简化版Go环境配置脚本(带选项)
# # 作者：Claude
# # 日期：2025-04-12

# # 颜色定义
# GREEN='\033[0;32m'
# BLUE='\033[0;34m'
# RED='\033[0;31m'
# YELLOW='\033[1;33m'
# NC='\033[0m'

# # 默认配置
# GO_VERSION="1.19.5"
# INSTALL_DIR="$HOME/local"

# # 打印帮助信息
# print_usage() {
#     echo -e "${BLUE}Go语言环境自动配置脚本${NC}"
#     echo ""
#     echo "用法: $0 [选项]"
#     echo ""
#     echo "选项:"
#     echo "  -h, --help              显示此帮助信息"
#     echo "  -v, --version <版本>    指定Go版本 (默认: $GO_VERSION)"
#     echo "  -d, --dir <目录>        指定安装目录 (默认: $INSTALL_DIR)"
#     echo ""
#     echo "示例:"
#     echo "  $0                      # 使用默认配置安装"
#     echo "  $0 -v 1.20.2            # 安装Go 1.20.2版本"
#     echo "  $0 -d ~/goenv           # 指定安装目录为~/goenv"
#     echo ""
# }

# # 解析命令行参数
# parse_arguments() {
#     while [[ $# -gt 0 ]]; do
#         case "$1" in
#             -h|--help)
#                 print_usage
#                 exit 0
#                 ;;
#             -v|--version)
#                 GO_VERSION="$2"
#                 shift 2
#                 ;;
#             -d|--dir)
#                 INSTALL_DIR="$2"
#                 shift 2
#                 ;;
#             *)
#                 echo -e "${RED}错误: 未知选项 $1${NC}" >&2
#                 print_usage
#                 exit 1
#                 ;;
#         esac
#     done
# }

# # 主函数
# main() {
#     # 解析命令行参数
#     parse_arguments "$@"

#     echo -e "${BLUE}开始安装Go环境...${NC}"

#     # 1. 创建安装目录
#     echo -e "1. 创建安装目录..."
#     mkdir -p "$INSTALL_DIR"
#     cd "$INSTALL_DIR"

#     # 2. 获取当前路径
#     CURRENT_PATH=$(pwd)
#     echo -e "   当前路径: ${GREEN}$CURRENT_PATH${NC}"

#     # 3. 下载并解压Go
#     echo -e "3. 下载并安装Go ${GO_VERSION}..."
#     GO_DOWNLOAD_URL="https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz"
#     wget -c "$GO_DOWNLOAD_URL" -O - | tar -xz -C "$INSTALL_DIR"

#     # 4. 检查Go是否安装成功
#     if [ -x "$INSTALL_DIR/go/bin/go" ]; then
#         GO_VERSION_INSTALLED=$("$INSTALL_DIR/go/bin/go" version)
#         echo -e "   Go安装成功: ${GREEN}$GO_VERSION_INSTALLED${NC}"
#     else
#         echo -e "${RED}Go安装失败，请检查下载链接和权限${NC}"
#         exit 1
#     fi

#     # 5. 创建gowork目录
#     echo -e "5. 创建GOPATH目录结构..."
#     mkdir -p "$INSTALL_DIR/gowork/src" "$INSTALL_DIR/gowork/bin" "$INSTALL_DIR/gowork/pkg"

#     # 6. 添加环境变量到.bashrc
#     echo -e "6. 添加环境变量到.bashrc..."

#     # 检查是否已经添加过环境变量
#     if grep -q "export GOROOT=$INSTALL_DIR/go" ~/.bashrc; then
#         echo -e "   Go环境变量已存在于.bashrc文件中"
#     else
#         # 添加环境变量
#         echo "" >> ~/.bashrc
#         echo "# Go环境变量配置" >> ~/.bashrc
#         echo "export GOROOT=$INSTALL_DIR/go" >> ~/.bashrc
#         echo "export GOPATH=$INSTALL_DIR/gowork" >> ~/.bashrc
#         echo "export PATH=\$PATH:\$GOROOT/bin:\$GOPATH/bin" >> ~/.bashrc
#         echo "export GOPROXY=https://goproxy.io,direct" >> ~/.bashrc
        
#         echo -e "   ${GREEN}环境变量已添加到.bashrc${NC}"
#     fi

#     # 设置当前会话的环境变量
#     export GOROOT="$INSTALL_DIR/go"
#     export GOPATH="$INSTALL_DIR/gowork"
#     export PATH="$PATH:$GOROOT/bin:$GOPATH/bin"
#     export GOPROXY="https://goproxy.io,direct"

#     echo -e "${GREEN}Go环境配置完成!${NC}"
#     echo -e "请运行 ${BLUE}source ~/.bashrc${NC} 使环境变量立即生效"
#     echo -e "然后运行 ${BLUE}go version${NC} 测试Go是否安装成功"
# }

# # 执行主函数
# main "$@"
#!/bin/bash

# 兼容多平台的Go环境配置脚本
# 支持: Ubuntu, Debian, CentOS, RHEL, Fedora
# 作者：Claude
# 日期：2025-04-12

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 默认配置
GO_VERSION="1.19.5"
INSTALL_DIR="$HOME/local"

# 打印帮助信息
print_usage() {
    echo -e "${BLUE}Go语言环境自动配置脚本${NC}"
    echo ""
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -h, --help              显示此帮助信息"
    echo "  -v, --version <版本>    指定Go版本 (默认: $GO_VERSION)"
    echo "  -d, --dir <目录>        指定安装目录 (默认: $INSTALL_DIR)"
    echo ""
    echo "示例:"
    echo "  $0                      # 使用默认配置安装"
    echo "  $0 -v 1.20.2            # 安装Go 1.20.2版本"
    echo "  $0 -d ~/goenv           # 指定安装目录为~/goenv"
    echo ""
}

# 解析命令行参数
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -h|--help)
                print_usage
                exit 0
                ;;
            -v|--version)
                GO_VERSION="$2"
                shift 2
                ;;
            -d|--dir)
                INSTALL_DIR="$2"
                shift 2
                ;;
            *)
                echo -e "${RED}错误: 未知选项 $1${NC}" >&2
                print_usage
                exit 1
                ;;
        esac
    done
}

# 检测Linux发行版
detect_os() {
    # 检测是CentOS还是Ubuntu系列
    if [ -f /etc/redhat-release ]; then
        OS_TYPE="centos"
        echo -e "检测到 ${YELLOW}CentOS/RHEL${NC} 系统"
    elif [ -f /etc/debian_version ]; then
        OS_TYPE="debian"
        echo -e "检测到 ${YELLOW}Debian/Ubuntu${NC} 系统"
    else
        OS_TYPE="unknown"
        echo -e "${YELLOW}未能确定操作系统类型，将使用通用配置${NC}"
    fi
}

# 安装依赖
install_dependencies() {
    echo -e "安装必要依赖..."
    
    if [ "$OS_TYPE" = "centos" ]; then
        # CentOS/RHEL需要的依赖
        if ! command -v wget &> /dev/null; then
            echo -e "安装wget..."
            sudo yum install -y wget
        fi
    elif [ "$OS_TYPE" = "debian" ]; then
        # Debian/Ubuntu需要的依赖
        if ! command -v wget &> /dev/null; then
            echo -e "安装wget..."
            sudo apt-get update
            sudo apt-get install -y wget
        fi
    fi
}

# 设置shell配置文件
setup_shell_config() {
    # 确定要修改的配置文件
    if [ -f "$HOME/.bashrc" ]; then
        SHELL_CONFIG="$HOME/.bashrc"
    elif [ -f "$HOME/.bash_profile" ]; then
        SHELL_CONFIG="$HOME/.bash_profile"
    elif [ -f "$HOME/.profile" ]; then
        SHELL_CONFIG="$HOME/.profile"
    else
        # 如果都不存在，默认使用.bashrc
        SHELL_CONFIG="$HOME/.bashrc"
        touch "$SHELL_CONFIG"
    fi
    
    echo -e "将使用 ${GREEN}$SHELL_CONFIG${NC} 作为配置文件"
    return 0
}

# 主函数
main() {
    # 解析命令行参数
    parse_arguments "$@"
    
    echo -e "${BLUE}开始安装Go环境...${NC}"
    
    # 检测系统类型
    detect_os
    
    # 安装依赖
    install_dependencies
    
    # 设置shell配置文件
    setup_shell_config

    # 1. 创建安装目录
    echo -e "1. 创建安装目录..."
    mkdir -p "$INSTALL_DIR"
    cd "$INSTALL_DIR"

    # 2. 获取当前路径
    CURRENT_PATH=$(pwd)
    echo -e "   当前路径: ${GREEN}$CURRENT_PATH${NC}"

    # 3. 下载并解压Go
    echo -e "3. 下载并安装Go ${GO_VERSION}..."
    GO_DOWNLOAD_URL="https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz"
    
    if [ -d "$INSTALL_DIR/go" ]; then
        echo -e "${YELLOW}检测到已存在Go安装，将先删除...${NC}"
        rm -rf "$INSTALL_DIR/go"
    fi
    
    echo -e "   正在下载Go ${GO_VERSION}..."
    wget -c "$GO_DOWNLOAD_URL" -O - | tar -xz -C "$INSTALL_DIR"

    # 4. 检查Go是否安装成功
    if [ -x "$INSTALL_DIR/go/bin/go" ]; then
        GO_VERSION_INSTALLED=$("$INSTALL_DIR/go/bin/go" version)
        echo -e "   Go安装成功: ${GREEN}$GO_VERSION_INSTALLED${NC}"
    else
        echo -e "${RED}Go安装失败，请检查下载链接和权限${NC}"
        exit 1
    fi

    # 5. 创建gowork目录
    echo -e "5. 创建GOPATH目录结构..."
    mkdir -p "$INSTALL_DIR/gowork/src" "$INSTALL_DIR/gowork/bin" "$INSTALL_DIR/gowork/pkg"

    # 6. 添加环境变量到shell配置文件
    echo -e "6. 添加环境变量到${SHELL_CONFIG}..."

    # 检查是否已经添加过环境变量
    if grep -q "export GOROOT=$INSTALL_DIR/go" "$SHELL_CONFIG"; then
        echo -e "   Go环境变量已存在于配置文件中"
        
        # 询问是否要更新
        read -p "   是否要更新环境变量? [y/N] " -n 1 -r UPDATE_ENV
        echo
        if [[ $UPDATE_ENV =~ ^[Yy]$ ]]; then
            # 删除旧的环境变量设置
            sed -i '/# Go环境变量配置/,+4d' "$SHELL_CONFIG"
            echo -e "   ${GREEN}已删除旧的环境变量配置${NC}"
        else
            echo -e "   ${YELLOW}保留现有环境变量配置${NC}"
        fi
    fi
    
    # 如果不存在或者用户选择更新，则添加环境变量
    if ! grep -q "export GOROOT=$INSTALL_DIR/go" "$SHELL_CONFIG" || [[ $UPDATE_ENV =~ ^[Yy]$ ]]; then
        # 添加环境变量
        echo "" >> "$SHELL_CONFIG"
        echo "# Go环境变量配置" >> "$SHELL_CONFIG"
        echo "export GOROOT=$INSTALL_DIR/go" >> "$SHELL_CONFIG"
        echo "export GOPATH=$INSTALL_DIR/gowork" >> "$SHELL_CONFIG"
        echo "export PATH=\$PATH:\$GOROOT/bin:\$GOPATH/bin" >> "$SHELL_CONFIG"
        echo "export GOPROXY=https://goproxy.io,direct" >> "$SHELL_CONFIG"
        
        echo -e "   ${GREEN}环境变量已添加到${SHELL_CONFIG}${NC}"
    fi

    # 设置当前会话的环境变量
    export GOROOT="$INSTALL_DIR/go"
    export GOPATH="$INSTALL_DIR/gowork"
    export PATH="$PATH:$GOROOT/bin:$GOPATH/bin"
    export GOPROXY="https://goproxy.io,direct"

    echo -e "${GREEN}Go环境配置完成!${NC}"
    echo -e "请运行 ${BLUE}source $SHELL_CONFIG${NC} 使环境变量立即生效"
    echo -e "然后运行 ${BLUE}go version${NC} 测试Go是否安装成功"
}

# 执行主函数
main "$@"