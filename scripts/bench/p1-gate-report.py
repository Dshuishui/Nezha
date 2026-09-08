#!/usr/bin/env python3
"""判 P1 门槛：GET/SCAN 的 p50/p99 相对改进前退化不超过 5%。

取每格若干轮的**中位数**再比，不取均值：延迟分布是长尾的，一轮偶发的抖动会把均值拉走，
而门槛问的是"典型情况下有没有变慢"。

同时报出改进前那几轮之间的散布（max/min − 1）。这一列比结论本身更重要：如果同一份代码
自己跑三轮就能差 6%，那么一个 6% 的"退化"什么也说明不了。实测归档里 GET p99 的轮间散布
就有 5.4%，与门槛同量级。

用法: p1-gate-report.py <before.csv> <after.csv>
"""
import csv
import sys
from collections import defaultdict

THRESHOLD = 0.05
OPS = ["GET", "SCAN"]
METRICS = [("p50_ms", "p50"), ("p99_ms", "p99")]


def load(path):
    rows = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            if r["op"] not in OPS:
                continue
            rows[(r["system"], int(r["vsize"]), r["op"])].append(r)
    return rows


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def spread(xs):
    """轮间散布：最大与最小相差多少。同一份代码自己的噪声底线。"""
    xs = [x for x in xs if x is not None]
    if len(xs) < 2 or min(xs) <= 0:
        return None
    return max(xs) / min(xs) - 1


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    before, after = load(sys.argv[1]), load(sys.argv[2])

    keys = sorted(set(before) & set(after), key=lambda k: (k[2], k[0], k[1]))
    if not keys:
        sys.exit("两份 CSV 没有可对照的格子")

    print(f"{'op':5s} {'system':11s} {'v':>5s} {'metric':6s} "
          f"{'before':>9s} {'after':>9s} {'delta':>8s} {'噪声':>8s}  判定")
    print("-" * 78)

    worst = None
    failures = []
    for k in keys:
        sysname, vs, op = k
        for col, name in METRICS:
            b = [float(r[col]) for r in before[k] if r[col] not in ("NA", "")]
            a = [float(r[col]) for r in after[k] if r[col] not in ("NA", "")]
            mb, ma = median(b), median(a)
            if mb is None or ma is None or mb <= 0:
                print(f"{op:5s} {sysname:11s} {vs:5d} {name:6s} {'NA':>9s} {'NA':>9s}")
                continue
            delta = ma / mb - 1
            noise = spread(b)
            # 退化超过门槛，但若改进前自己的轮间散布就盖过了这个差值，则判为噪声
            if delta > THRESHOLD:
                verdict = "噪声内" if noise is not None and noise >= delta else "*** 超门槛 ***"
                if verdict != "噪声内":
                    failures.append((op, sysname, vs, name, delta))
            else:
                verdict = "OK"
            if worst is None or delta > worst[0]:
                worst = (delta, op, sysname, vs, name)
            ns = f"±{noise:6.1%}" if noise is not None else "     NA"
            print(f"{op:5s} {sysname:11s} {vs:5d} {name:6s} "
                  f"{mb:9.3f} {ma:9.3f} {delta:+8.1%} {ns:>8s}  {verdict}")

    print("-" * 78)
    if worst:
        d, op, s, v, m = worst
        print(f"最差一格: {op} {s} v={v} {m} {d:+.1%}（门槛 +{THRESHOLD:.0%}）")
    if failures:
        print(f"结论: 不通过——{len(failures)} 格退化超过门槛且大于改进前自身的轮间散布")
        print("      先调大 partitionTargetMB 再测；仍不达标则本设计作废")
        sys.exit(1)
    print("结论: 通过——没有一格的退化同时超过门槛且大于噪声")


if __name__ == "__main__":
    main()
