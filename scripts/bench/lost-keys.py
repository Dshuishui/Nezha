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
import re
import sys

HEADER = 20


def _key_len():
    """从 Go 源码里读客户端的定长 key 宽度，而不是写死。

    宽度曾经是存储层的 `KeyLength`（先 10 后 24）。存储层现在原样存 key，编码搬到了
    客户端（internal/client 的 DefaultKeyPadWidth），所以盘上记录里的 key 宽度等于
    **客户端补齐到的宽度**。

    写死过一次，改了宽度之后本工具解析出的是记录中间的字节，`int()` 直接抛异常，
    crash-recovery.sh 拿到空串、把"丢了 ? 条"当成失败报了出来——工具坏了，看起来像
    被测系统坏了。
    """
    env = os.environ.get("KEY_LEN")
    if env:
        return int(env)
    src = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "..", "..", "internal", "client", "client.go")
    try:
        with open(src, encoding="utf-8") as f:
            m = re.search(r"DefaultKeyPadWidth\s*=\s*(\d+)", f.read())
        if m:
            return int(m.group(1))
    except OSError:
        pass
    sys.exit(f"读不到 {src} 里的 DefaultKeyPadWidth，请用 KEY_LEN=<位数> 覆盖")

KEY_LEN = _key_len()


def keys_in(path, stride):
    """返回 (解析出的 key 集合, 解析不了的记录数)。

    **按每条记录的头往前走，不按固定跨步。** 记录的编码是（internal/raft/logwriter.go）：
        [0:4] index  [4:8] term  [8:12] votedFor  [12:16] keySize  [16:20] valueSize
        然后是 keySize 字节的 key、valueSize 字节的 value
    长度写在头里，所以变长记录与定长记录都能走。

    早先是按 `stride = 20 + KEY_LEN + vsize` 盲目跨步的，只有在**每条记录都等长**时才对。
    2026-09-18 在 nezha-nogc 上炸了：未经 GC 的原始日志开头有一条 leader 任期开始时写的
    NoOp（keySize=0、valueSize=0，只占 20 字节的头），它把后面每一条都错位 20 字节，
    于是 446 万条**全部**解析不了，工具报"盘上丢了 4462025 条"——
    一个健康的系统被报成丢了全部数据。
    以前照不到是因为历史跑法都是 nezha（开 GC），而 GC 产物里没有 NoOp，
    定长跨步恰好对得上。

    单条解析失败不能让整个工具崩掉：一个被篡改或截断的文件恰恰是最需要这个计数的时候。
    此前这里直接 `int(...)`，遇到清零的 key 字节抛 ValueError，整个脚本带着 traceback
    退出，crash-recovery.sh 拿到空串把它报成"盘上丢了 ? 条"——**唯一一个全量覆盖的闸门
    在有损坏时反而不出数**。
    """
    with open(path, "rb") as f:
        data = f.read()
    out, broken, noop, pos, n = set(), 0, 0, 0, len(data)
    while pos + HEADER <= n:
        key_size = int.from_bytes(data[pos + 12:pos + 16], "little")
        val_size = int.from_bytes(data[pos + 16:pos + 20], "little")
        end = pos + HEADER + key_size + val_size
        if end > n:
            # 最后一条被截断：文件写到一半崩过。这正是要报出来的情况。
            print(f"  警告: {os.path.basename(path)} 末尾有半条记录"
                  f"（还差 {end - n} 字节）——写到一半崩过")
            broken += 1
            break
        if key_size == 0:
            noop += 1          # leader 任期开始的空指令，不是用户数据
        else:
            raw = data[pos + HEADER:pos + HEADER + key_size]
            try:
                out.add(int(raw))
            except ValueError:
                broken += 1
        pos = end
    if noop:
        print(f"  {os.path.basename(path)}: 跳过 {noop} 条 NoOp（任期开始的空指令，不是用户数据）")
    return out, broken


def main():
    if len(sys.argv) not in (4, 5):
        sys.exit(__doc__)
    d, n, vsize = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    inline_threshold = int(sys.argv[4]) if len(sys.argv) == 5 else 0
    if inline_threshold and vsize < inline_threshold:
        print(f"不适用：value {vsize}B 小于内联阈值 {inline_threshold}B，小值不在 valuelog 里，"
              f"本工具数不准。这种配置请用 cmd/bench/readonly 或 scanverify 逐条校验。")
        # **调用方要能把"不适用"与"没跑成"分开，所以再给一行机器可读的判定。**
        # 原先只有上面那句中文散文，而驱动是按 `丢失 <数字>` 取值的，取不到就记 NA；
        # NA 在 LOST_KEYS=fail 下判失败——于是 nezha-avp 的每一格都被判死，
        # 而它其实是**工具看过配置之后明确回答"这里没法查"**，跟超时、崩溃完全不是一回事。
        # 2026-09-18 的四系统冒烟就停在这里（前三个系统全部跑完，第四个一格没出）。
        # 散文会改、会翻译、会被 tail 截断，判定行不会，所以判定走这一行。
        print("LOST_KEYS_VERDICT=not_applicable")
        return
    stride = HEADER + KEY_LEN + vsize
    vlog = os.path.join(d, "data", "valuelog")

    present = set()
    broken_total = 0
    files = sorted(glob.glob(os.path.join(vlog, "*")))
    if not files:
        sys.exit(f"{vlog} 下没有文件")
    for p in files:
        if os.path.isdir(p):
            continue  # index/ 子目录：稀疏索引的旁挂文件，不是数据记录
        if p.endswith(".idx"):
            continue
        got, broken = keys_in(p, stride)
        present |= got
        broken_total += broken
        note = f"  ({broken} 条解析不了)" if broken else ""
        print(f"  {os.path.basename(p):40s} {len(got):>9d} 个 key{note}")

    lost = sorted(set(range(n)) - present)
    if broken_total:
        print(f"**{broken_total} 条记录解析不了**——盘上有损坏，下面的丢失数只是下界")
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
