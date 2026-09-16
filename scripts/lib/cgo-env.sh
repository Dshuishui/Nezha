#!/bin/bash
# cgo 环境（RocksDB 的头与库）。source 进来后调用 setup_cgo_env，不单独执行。
#
# 为什么要抽成一份：七个单机脚本各自写死了同一段探测——只搜
# /usr/lib/x86_64-linux-gnu、/usr/local/lib、/usr/lib 三处，并把 CGO_CFLAGS 定成
# -I/usr/include。那是照 winbox 写的，在三台实验机上会以两种方式失败：
#
#   node55        三个系统路径里**都没有** librocksdb.so（它在 $HOME/local/lib），
#                 脚本直接 "librocksdb.so 未找到" 退出。这种至少是响的。
#   tikv240/241   /usr/local/lib 里**有**一个更老的 librocksdb.so，于是探测"成功"，
#                 但对应的头文件不在 /usr/include，编译时报
#                   could not determine what C.rocksdb_backup_engine_open_opts refers to
#                 **这一种更坏：不是找不到，而是找到了错的。** 报错出现在 cgo 阶段，
#                 读起来像代码问题，而不是环境问题。
#
# 每台机器自己的 ~/env.sh 才是那台机器的权威配置（三台实验机都有，内容各不相同：
# 240/241/55 的库都在 $HOME/local/lib 与 $HOME/local/lib64，GOMODCACHE、GOPROXY、
# GOSUMDB 也都在那里设）。所以次序是：**先用 ~/env.sh，它没给才回落到探测。**
#
# 顺带解决另一个坑：不 source ~/env.sh 就拿不到那台机器的 GOMODCACHE 与 GOPROXY，
# 于是 go build 会把所有模块重新下一遍（2026-09-16 实测，snapshot-crash.sh 卡在
# "go: downloading ..." 上）。
#
# 用法：
#   . "$(dirname "$0")/../lib/cgo-env.sh"
#   setup_cgo_env || exit 1      # 或者配上脚本自己的 die/fail
setup_cgo_env() {
    export PATH=$PATH:/usr/local/go/bin
    # shellcheck source=/dev/null
    [ -f "$HOME/env.sh" ] && . "$HOME/env.sh"
    if [ -n "${CGO_LDFLAGS:-}" ]; then
        echo "[cgo-env] 取自 ~/env.sh"
        return 0
    fi
    local d
    for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
        [ -f "$d/librocksdb.so" ] && { CGO_ROCKSDB_LIB=$d; break; }
    done
    if [ -z "${CGO_ROCKSDB_LIB:-}" ]; then
        echo "[cgo-env] librocksdb.so 未找到，且 ~/env.sh 也没提供 cgo 配置" >&2
        return 1
    fi
    export CGO_CFLAGS="-I/usr/include"
    export CGO_LDFLAGS="-L$CGO_ROCKSDB_LIB -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
    export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$CGO_ROCKSDB_LIB
    echo "[cgo-env] 由探测得到：$CGO_ROCKSDB_LIB"
    return 0
}
