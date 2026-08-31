#!/bin/bash
# scripts/lib/bench-common.sh 的回归测试。不依赖 RocksDB/Go，本地可跑：
#   bash scripts/lib/bench-common_test.sh
#
# 用例取自 2026-08-30 真实废掉的那轮实验的输出。
set -u
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/bench-common.sh"

PASS=0; FAIL=0; SKIP=0
ok(){ echo "  ok   - $1"; PASS=$((PASS+1)); }
no(){ echo "  FAIL - $1"; echo "         $2"; FAIL=$((FAIL+1)); }
skip(){ echo "  skip - $1 ($2)"; SKIP=$((SKIP+1)); }
check(){ [ "$2" = "$3" ] && ok "$1" || no "$1" "期望 [$3] 实际 [$2]"; }

echo "== rss_kb / rss_mb：进程不存在时必须返回 0 而不是空串 =="
DEAD_PID=999999
check "rss_kb 对已消失的进程返回 0" "$(rss_kb $DEAD_PID)" "0"
check "rss_kb 无参数也返回 0"       "$(rss_kb)"           "0"
check "rss_mb 对已消失的进程返回 0" "$(rss_mb $DEAD_PID)" "0"
# 这是当初炸掉整轮实验的那一行的等价写法
if out=$( (set -u; echo $(( $(rss_kb $DEAD_PID) / 1024 ))) 2>&1 ); then
    check "算术展开不再语法错误" "$out" "0"
else
    no "算术展开不再语法错误" "$out"
fi
# 沙箱环境下 ps 可能不可执行；这时上面几条（返回 0 而非空串）仍然有效且是关键路径。
if ps -o rss= -p $$ >/dev/null 2>&1; then
    check "rss_kb 对自身进程返回正数" "$([ "$(rss_kb $$)" -gt 0 ] && echo yes)" "yes"
else
    skip "rss_kb 对自身进程返回正数" "本机 ps 不可用"
fi

echo "== peak_mb：采样为空必须失败，而不是伪装成 0 MB =="
T=$(mktemp -d "${TMPDIR:-/tmp}/benchcommon.XXXXXX") || { echo "无法创建临时目录"; exit 1; }
trap 'rm -rf "$T"' EXIT
: > "$T/empty.txt"
peak_mb "$T/empty.txt" >/dev/null 2>&1 && no "空采样文件应返回非零" "返回了成功" || ok "空采样文件返回非零"
printf '\n\n' > "$T/blank.txt"
peak_mb "$T/blank.txt" >/dev/null 2>&1 && no "全空行应返回非零" "返回了成功" || ok "全空行返回非零"
printf '1024\n3145728\n2048\n' > "$T/good.txt"
check "peak_mb 取最大值并换算 MB" "$(peak_mb "$T/good.txt")" "3072"

echo "== count_matches：grep -c 无匹配时不得产生两行 0 =="
printf 'hello\n' > "$T/log.txt"
check "无匹配返回单个 0"   "$(count_matches '垃圾回收完成' "$T/log.txt")" "0"
check "文件不存在返回 0"   "$(count_matches 'x' "$T/nope.txt")"           "0"
printf '垃圾回收完成\n垃圾回收完成\n' >> "$T/log.txt"
check "有匹配返回正确计数" "$(count_matches '垃圾回收完成' "$T/log.txt")" "2"

echo "== 输出解析：三个 benchmark 格式各异 =="
PUT='  elapse:3m9.5s, throughput:0.1688MB/S, avg latency:15.22256ms, total 500000, goodPut 499983, value 64'
GET='  测试 100 - Elapse: 1.41s, Throughput: 0.6839 MB/S, Total: 20000, GoodPut: 14856, Average Latency: 1.851807ms'
SCAN='  Test 100: elapse:6.51s, throught:10.2112MB/S, avg latency:1.629651758s, total 4, goodPut 1040042'
check "randwrite 延迟（毫秒）" "$(bench_latency "$PUT")" "15.2226"
check "randwrite 吞吐" "$(bench_throughput "$PUT")" "0.1688"
check "zipf_read 延迟（毫秒）" "$(bench_latency "$GET")" "1.8518"
check "zipf_read 吞吐" "$(bench_throughput "$GET")" "0.6839"
# 1.629651758s，旧解析会写成 1.63 并当作毫秒——差三个数量级
check "scan_pro 延迟（秒→毫秒）" "$(bench_latency "$SCAN")" "1629.6518"
check "scan_pro 吞吐（注意上游拼写 throught）" "$(bench_throughput "$SCAN")" "10.2112"

echo "== bench_invalid_reason：三种此前被静默放过的坏数据 =="
bench_invalid_reason PUT "$PUT" >/dev/null && no "健康输出不该被判无效" "被判无效" || ok "健康输出判为有效"
bench_invalid_reason GET "$GET" >/dev/null && no "健康输出不该被判无效" "被判无效" || ok "健康 GET 判为有效"
bench_invalid_reason SCAN "$SCAN" >/dev/null && no "健康输出不该被判无效" "被判无效" || ok "健康 SCAN 判为有效"

# 真实坏数据：benchmark 崩溃，一行都没输出
bench_invalid_reason GET "" >/dev/null && ok "空输出判为无效" || no "空输出判为无效" "被放过了"
# 真实坏数据（2026-08-30 那轮 baseline）
BAD_GET='  测试 100 - Elapse: 113.676819ms, Throughput: 0.0000 MB/S, Total: 20000, GoodPut: 0, Value: 0, Client: 20'
BAD_SCAN='  Test 100: elapse:219.187µs, throught:NaNMB/S, avg latency:0s, total 4, goodPut 0, client 1'
bench_invalid_reason GET "$BAD_GET" >/dev/null && ok "GoodPut: 0 判为无效（冒号式）" || no "GoodPut: 0 判为无效" "被放过了"
bench_invalid_reason SCAN "$BAD_SCAN" >/dev/null && ok "goodPut 0 判为无效（空格式）" || no "goodPut 0 判为无效" "被放过了"
bench_invalid_reason SCAN 'throught:NaNMB/S, goodPut 5' >/dev/null && ok "NaN 吞吐判为无效" || no "NaN 吞吐判为无效" "被放过了"
# goodPut 大数不能被 "0" 误伤
bench_invalid_reason PUT 'goodPut 1040042, client 1' >/dev/null && no "goodPut 1040042 不该判无效" "被误判" || ok "goodPut 非零不误判"

echo "== duration_ms：三个 benchmark 混用 µs/ms/s，必须归一到毫秒 =="
check "毫秒原样"       "$(duration_ms 15.22256ms)"          "15.2226"
check "秒换算成毫秒"   "$(duration_ms 1.629651758s)"        "1629.6518"
check "微秒换算"       "$(duration_ms 219.187µs)"           "0.2192"
check "零值"           "$(duration_ms 0s)"                  "0.0000"
check "分秒复合"       "$(duration_ms 3m9.552304222s)"      "189552.3042"
check "时分秒复合"     "$(duration_ms 1h15m48.837991792s)"  "4548837.9918"
duration_ms "无数字" >/dev/null 2>&1 && no "无时长字符串应失败" "返回了成功" || ok "无时长字符串返回非零"
# 这是旧解析最要命的一处：SCAN 延迟 1.63 秒被当成 1.63 毫秒写进 scan_lat_ms 列
check "秒不再被当成毫秒" "$(bench_latency 'avg latency:1.629651758s')" "1629.6518"

echo "== 多轮汇总：取 benchmark 自己算的 100 轮平均，而不是第 100 轮 =="
SCAN_FULL='Test 99: elapse:2ms, throught:NaNMB/S, avg latency:0s, total 4, goodPut 0, client 1
Test 100: elapse:6.51s, throught:10.2112MB/S, avg latency:1.629651758s, total 4, goodPut 1040042
Average throughput over 100 tests: 8.4321MB/S
Average latency over 100 tests: 47.800721ms'
GET_FULL='测试 99 - Elapse: 1.51s, Throughput: 0.5331 MB/S, Total: 20000, GoodPut: 12467, Average Latency: 2.367014ms
测试 100 - Elapse: 1.53s, Throughput: 0.5270 MB/S, Total: 20000, GoodPut: 12500, Average Latency: 2.402897ms
100 次测试的平均吞吐量: 0.5286 MB/S
100 次测试的总平均延迟: 2.395777ms'
check "scan_pro 100 轮平均吞吐（非末轮 10.2112）" "$(bench_avg_throughput "$SCAN_FULL")" "8.4321"
check "scan_pro 100 轮平均延迟"                   "$(bench_avg_latency "$SCAN_FULL")"    "47.8007"
check "zipf_read 100 轮平均吞吐（非末轮 0.5270）" "$(bench_avg_throughput "$GET_FULL")"  "0.5286"
check "zipf_read 100 轮平均延迟"                  "$(bench_avg_latency "$GET_FULL")"     "2.3958"
bench_avg_throughput "没有汇总行" >/dev/null 2>&1 && no "无汇总行应失败" "返回了成功" || ok "无汇总行返回非零"

echo "== bench_round_stats：统计空轮，判断这轮 SCAN 有没有意义 =="
check "统计 scan 轮数与空轮" "$(bench_round_stats "$SCAN_FULL")" "2 1"
check "统计 get 轮数与空轮"  "$(bench_round_stats "$GET_FULL")"  "2 0"
check "无轮次行返回 0 0"     "$(bench_round_stats "随便一句话")"  "0 0"

echo ""
# 变量必须用 ${} 包起来：后面紧跟全角逗号时，裸写 $PASS 会被当成变量名的一部分。
echo "通过 ${PASS}，跳过 ${SKIP}，失败 ${FAIL}"
[ "${FAIL}" -eq 0 ]
