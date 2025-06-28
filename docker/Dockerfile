# Nezha multiGC 版本 Dockerfile
FROM alpine:3.18

# 配置国内镜像源
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# 安装运行时依赖
RUN apk add --no-cache \
    snappy \
    zlib \
    bzip2 \
    lz4 \
    zstd \
    libstdc++ \
    libgcc \
    ca-certificates

# 复制编译好的 nezha 程序
COPY nezha /usr/local/bin/nezha

# 复制依赖库文件
COPY librocksdb.so.5.18 /usr/local/lib/
COPY libgflags.so.2 /usr/local/lib/

# 创建符号链接
RUN ln -sf /usr/local/lib/librocksdb.so.5.18 /usr/local/lib/librocksdb.so.5 && \
    ln -sf /usr/local/lib/librocksdb.so.5 /usr/local/lib/librocksdb.so

# 创建非 root 用户
RUN addgroup -g 1001 nezha && \
    adduser -D -u 1001 -G nezha nezha

# 设置权限
RUN chmod +x /usr/local/bin/nezha

# 更新库缓存
RUN ldconfig /usr/local/lib || true

# 创建数据目录（multiGC 版本的新特性）
RUN mkdir -p /app/data && chown nezha:nezha /app/data

# 切换到非 root 用户
USER nezha
WORKDIR /app

# 暴露端口
EXPOSE 3088 30881

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD pgrep nezha > /dev/null || exit 1

# 设置启动命令（默认使用 /app/data 作为数据目录）
ENTRYPOINT ["/usr/local/bin/nezha", "-data", "/app/data"]
