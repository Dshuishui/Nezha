#!/usr/bin/env python3
"""数一个数据目录里到底丢了多少 key。

GC 的正确性底线是"搬运不丢数据"，但读路径把"这一处没有"当成常态（数据分散在有序文件
与新旧 valuelog 中，一次读并发查这几处），所以少搬一条不会报错，只会在某次 GET 上变成
一个 NOKEY。命中率里 0.5% 的缺口，可能只是**一个热点 key** 丢了——Zipf 下排名 15 的
key 就占 0.46% 的请求量。

这里直接从盘上数：把有序文件（改造后是各分区）与当前 valuelog 里出现过的 key 全部收集
起来，与"本应写入的 0..N-1"做差集。定长 entry（20B 头 + 定长 padded key + 定长 value）
才能按固定跨步解析，所以只适用于单一 value 尺寸的负载。

**不适用于开了 -inlinePlacement 且 value 小于内联阈值的负载。** 那种配置下小值被内联写进
RocksDB（apply.go 的内联分支在最前面且不看 FileVersion），GC 期间在途的旧日志条目会内联落进
新库：值自包含、读得到，但它在旧 valuelog 里的那份随文件被删，而搬运遍历的是旧**库**、看不到
它。于是这个 key 在所有 valuelog 文件里都不存在，却完全可读——按本工具的口径会被误报成丢失。
实测 nezha-avp 在 64B/256B 上被误报几十条，而 GET 命中率是 1.0000、ops 一条不差。
传入 inline_threshold 后，value 小于它就直接判定"不适用"，不再给出会被误读的数字。

用法: lost-keys.py <数据目录> <写入条数> <value字节数> [inline_threshold]
"""
import glob
import os
import sys

KEY_LEN = 10
HEADER = 20


def keys_in(path, stride):
    with open(path, "rb") as f:
        data = f.read()
    if len(data) % stride:
        print(f"  警告: {os.path.basename(path)} 长度 {len(data)} 不是 {stride} 的整数倍，"
              f"余 {len(data) % stride} 字节——可能不是定长负载，结果不可信")
    out = set()
    for i in range(len(data) // stride):
        out.add(int(data[i * stride + HEADER:i * stride + HEADER + KEY_LEN]))
    return out


def main():
    if len(sys.argv) not in (4, 5):
        sys.exit(__doc__)
    d, n, vsize = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    inline_threshold = int(sys.argv[4]) if len(sys.argv) == 5 else 0
    if inline_threshold and vsize < inline_threshold:
        print(f"不适用：value {vsize}B 小于内联阈值 {inline_threshold}B，小值不在 valuelog 里，"
              f"本工具数不准。这种配置请用 cmd/bench/readonly 或 scanverify 逐条校验。")
        return
    stride = HEADER + KEY_LEN + vsize
    vlog = os.path.join(d, "data", "valuelog")

    present = set()
    files = sorted(glob.glob(os.path.join(vlog, "*")))
    if not files:
        sys.exit(f"{vlog} 下没有文件")
    for p in files:
        if os.path.isdir(p):
            continue
        got = keys_in(p, stride)
        present |= got
        print(f"  {os.path.basename(p):40s} {len(got):>9d} 个 key")

    lost = sorted(set(range(n)) - present)
    print(f"写入 {n}，盘上 distinct {len(present)}，丢失 {len(lost)}")
    if lost:
        # Zipf(s=1.01) 下 rank k 的请求占比约 1/(k*(ln N + 0.5772))，据此估计
        # 这些丢失会让命中率掉多少——冷 key 丢了几乎看不出来，热 key 丢一个就很显眼
        import math
        h = math.log(n) + 0.5772
        share = sum(1.0 / ((k + 1) * h) for k in lost)
        print(f"最小的 20 个: {lost[:20]}")
        print(f"按 Zipf(s=1.01) 估计的请求缺失比例: {share:.4%}")


if __name__ == "__main__":
    main()
