#!/bin/bash
# bench 脚本的公共采样与解析工具。
#
# 抽出来的直接原因：三个 bench 脚本各自复制了一份 RSS 读取逻辑，
# 并且都栽在同一个坑上——
#
#     rss(){ ps -o rss= -p $PID 2>/dev/null | tr -d ' ' || echo 0; }
#
# 管道的退出码取自最后一个命令 tr，而 tr 永远成功，所以 `|| echo 0` 永不执行。
# 进程一旦消失，函数返回空串，$(( $(rss) / 1024 )) 就展开成 $(( / 1024 ))：
#
#     line 95: / 1024 : syntax error (error token is "/ 1024 ")
#     line 107: FINAL: unbound variable
#
# 于是整轮实验数据在最后一步全部丢失——节点确实跑完了，结果却没落盘。
# 2026-08-30 那轮 64KB 块尺寸对比就是这样废掉的。

# rss_kb <pid> —— 返回进程 RSS（KB）。进程不存在时返回 0，绝不返回空串。
rss_kb() {
    local v
    v=$(ps -o rss= -p "${1:-0}" 2>/dev/null | tr -d ' ')
    echo "${v:-0}"
}

# rss_mb <pid> —— 同上，单位 MB。
rss_mb() {
    echo $(( $(rss_kb "${1:-0}") / 1024 ))
}

# start_rss_sampler <pid> <输出文件> —— 后台按秒采样，进程退出即自行结束。
# 打印采样器自己的 PID 供调用方 kill。只监控活着的目标：目标一消失循环就退出，
# 不会留下无意义的采样进程。
start_rss_sampler() {
    local pid="$1" out="$2" interval="${3:-3}"
    ( while kill -0 "$pid" 2>/dev/null; do rss_kb "$pid"; sleep "$interval"; done ) > "$out" &
    echo $!
}

# peak_mb <采样文件> —— 峰值 RSS（MB）。
# 采样文件为空或无有效数字时返回非零退出码，由调用方决定如何处理；
# 旧版在这种情况下静默输出 0，把"没采到数据"伪装成"内存占用为零"。
peak_mb() {
    local f="$1"
    [ -s "$f" ] || return 1
    awk '{ if ($1 ~ /^[0-9]+$/ && $1 > m) { m = $1; seen = 1 } }
         END { if (!seen) exit 1; print int(m / 1024) }' "$f"
}

# count_matches <模式> <文件> —— grep -c 的安全版本。
# grep -c 无匹配时输出 0 但退出码为 1，写成 `$(grep -c ... || echo 0)` 会让
# 命令替换收到两行 "0"，变量变成 "0\n0" 并污染后续输出（旧版 RESULT 行后面
# 那个孤零零的 0 就是这么来的）。
count_matches() {
    local n
    n=$(grep -c "$1" "$2" 2>/dev/null) || n=0
    echo "${n:-0}"
}

# ---- benchmark 输出解析 ----
#
# 三个 benchmark 的输出格式各不相同：
#   randwrite : "elapse:..., throughput:0.1666MB/S, avg latency:14.32ms, ..."
#   zipf_read : "... Elapse: 2.68s, Throughput: 0.3319 MB/S, ..., Average Latency: 3.81ms"
#   scan_pro  : "Test N: elapse:..., throught:0.44MB/S, avg latency:..., ..."
# 注意 scan_pro 的 "throught" 是上游的拼写错误，不是笔误。
# 因此解析必须忽略大小写、容忍冒号后的空格，并同时匹配 throughput/throught。

# duration_ms —— 把 Go 的 duration 字符串换算成毫秒。
# 必须换算，不能直接取数字：同一列里既有 "avg latency:15.22256ms" 也有
# "avg latency:1.629651758s"，还有 "elapse:219.187µs" 和 "1h15m48.837991792s"。
# 旧解析用 [0-9.]+ 抓到数字就当毫秒写进 *_lat_ms 列，
# 于是 1.629651758 秒被记成 1.63 毫秒——差了三个数量级。
duration_ms() {
    awk -v s="$1" '''BEGIN {
        gsub(/µ/, "u", s)
        total = 0; found = 0
        while (match(s, /[0-9]+(\.[0-9]+)?(ns|us|ms|h|m|s)/)) {
            tok = substr(s, RSTART, RLENGTH)
            s = substr(s, RSTART + RLENGTH)
            if (tok ~ /ns$/)      { unit = "ns"; mult = 0.000001 }
            else if (tok ~ /us$/) { unit = "us"; mult = 0.001 }
            else if (tok ~ /ms$/) { unit = "ms"; mult = 1 }
            else if (tok ~ /h$/)  { unit = "h";  mult = 3600000 }
            else if (tok ~ /m$/)  { unit = "m";  mult = 60000 }
            else                  { unit = "s";  mult = 1000 }
            total += substr(tok, 1, length(tok) - length(unit)) * mult
            found = 1
        }
        if (!found) exit 1
        printf "%.4f", total
    }'''
}

# ---- 单轮结果 ----
# randwrite 只跑一轮，用这两个。
bench_latency() {
    local raw
    raw=$(grep -oiE "latency: *[0-9.]+(ns|us|µs|ms|h|m|s)" <<<"$1" | tail -1 | sed 's/.*: *//')
    [ -n "$raw" ] || return 1
    duration_ms "$raw"
}
bench_throughput() { grep -oiE "throughp?u?t?h?t?: *[0-9.]+ *MB/S" <<<"$1" | tail -1 | grep -oE "[0-9.]+"; }

# ---- 多轮汇总 ----
# zipf_read 和 scan_pro 都跑 numTests=100 轮，并在结尾自己打印 100 轮的平均：
#   scan_pro : "Average throughput over 100 tests: 10.2112MB/S"
#              "Average latency over 100 tests: 47.800721ms"
#   zipf_read: "100 次测试的平均吞吐量: 0.5286 MB/S"
#              "100 次测试的总平均延迟: 2.395777ms"
# 旧脚本用 `grep elapse | tail -1` 取的是**第 100 轮那一次**的结果，把现成的
# 100 轮平均丢掉了。scan_pro 每轮只扫 dnums 次（默认 4），方差极大——
# 冒烟测试里 100 轮有 61 轮 goodPut 为 0，取哪一轮纯看运气。
bench_avg_throughput() {
    local v
    v=$(sed -n 's/.*[Tt]ests: *\([0-9.]*\) *MB\/S.*/\1/p' <<<"$1" | tail -1)
    [ -n "$v" ] || v=$(sed -n 's/.*平均吞吐量: *\([0-9.]*\) *MB\/S.*/\1/p' <<<"$1" | tail -1)
    [ -n "$v" ] || return 1
    echo "$v"
}
bench_avg_latency() {
    local raw
    raw=$(sed -n 's/.*[Ll]atency over [0-9]* tests: *\([0-9.a-zµ]*\).*/\1/p' <<<"$1" | tail -1)
    [ -n "$raw" ] || raw=$(sed -n 's/.*总平均延迟: *\([0-9.a-zµ]*\).*/\1/p' <<<"$1" | tail -1)
    [ -n "$raw" ] || return 1
    duration_ms "$raw"
}

# bench_round_stats <完整输出> —— 打印 "总轮数 空轮数"。
# 空轮（goodPut 0）在数据量小于扫描起点范围时是正常现象，不是故障；
# 但空轮占比过高说明这一轮实验的 SCAN 数字没有意义。
bench_round_stats() {
    local total empty
    total=$(grep -cE "^(Test|测试) [0-9]+ *[:-]" <<<"$1") || total=0
    empty=$(grep -cE "goodPut:? *0([,[:space:]]|$)" <<<"$1") || empty=0
    echo "$total $empty"
}

# bench_invalid_reason <名称> <输出文本> —— 判断一轮 benchmark 是否产出了无效数据。
# 有问题时在 stdout 打印原因并返回 0；数据可用则返回 1。
#
# 覆盖三种此前会被静默放过的坏数据：
#   1. 输出为空——benchmark 崩溃了。旧版写成 `X=$(cmd | grep ... | tail -1) || X="(失败)"`，
#      而管道退出码取自 tail，永远为 0，所以 `|| X="(失败)"` 从不生效。
#   2. GoodPut 为 0——一条都没读到，延迟和吞吐都无意义。
#   3. 吞吐为 NaN——scan_pro 在 GoodPut=0 时输出 "throught:NaNMB/S"，
#      而解析用的 [0-9.]+ 匹配不到 NaN，CSV 里只会留下一个空字段。
bench_invalid_reason() {
    local name="$1" text="$2"
    if [ -z "${text// /}" ]; then
        echo "$name 无输出——benchmark 未产出结果行（进程可能崩溃）"; return 0
    fi
    if grep -qiE "goodput:? *0([,[:space:]]|$)" <<<"$text"; then
        echo "$name 的 GoodPut 为 0——一条都没读到，本轮数据无效"; return 0
    fi
    if grep -qi "nan" <<<"$text"; then
        echo "$name 的吞吐为 NaN——分母为零，本轮数据无效"; return 0
    fi
    return 1
}
