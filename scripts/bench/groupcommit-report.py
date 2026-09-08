#!/usr/bin/env python3
"""攒批窗口扫描的判读表。

要回答的问题只有两个：

1. **上一轮 200μs 那档的反常延迟是噪声吗？** 判据是它与相邻窗口的差，跟同一档自身的
   轮间散布比。散布盖过差值就是噪声，选窗口时不必理会。
2. **窗口开到多大就不再有收益？** 批大小随窗口上升总会饱和；饱和之后再加窗口只是白等，
   单条延迟净增。取"批大小接近饱和且延迟仍在下降"的那一档。

用法: groupcommit-report.py <csv>
"""
import csv
import sys
from collections import defaultdict


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def spread(xs):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2 or min(xs) <= 0:
        return None
    return max(xs) / min(xs) - 1


def num(r, k):
    v = r.get(k, "NA")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    rows = defaultdict(list)
    for r in csv.DictReader(open(sys.argv[1])):
        rows[int(r["window_us"])].append(r)
    if not rows:
        sys.exit("CSV 里没有数据")

    print(f"{'窗口us':>7s} {'轮数':>4s} {'p50中位':>9s} {'轮间散布':>9s} "
          f"{'p99中位':>9s} {'吞吐中位':>11s} {'批大小':>8s} {'省下fsync':>10s}")
    print("-" * 76)

    table = {}
    for w in sorted(rows):
        rs = rows[w]
        p50s = [x for x in (num(r, "p50_ms") for r in rs) if x is not None]
        p99s = [x for x in (num(r, "p99_ms") for r in rs) if x is not None]
        tps = [x for x in (num(r, "ops_per_s") for r in rs) if x is not None]
        batch = [x for x in (num(r, "avg_batch") for r in rs) if x is not None]
        saved = [x for x in (num(r, "fsync_saved") for r in rs) if x is not None]
        m50, sp = median(p50s), spread(p50s)
        table[w] = (m50, sp, median(p99s), median(tps), median(batch))
        print(f"{w:7d} {len(rs):4d} {m50 if m50 is None else f'{m50:9.3f}':>9s} "
              f"{'       NA' if sp is None else f'{sp:8.1%}':>9s} "
              f"{median(p99s) or 0:9.3f} {median(tps) or 0:11.1f} "
              f"{median(batch) or 0:8.2f} {median(saved) or 0:10.0f}")

    print("-" * 76)

    ws = sorted(table)
    # 问题 1：每一档与两侧相邻档比，凸起是否盖过自身噪声
    print("\n[反常档判读] 与相邻窗口比，凸起是否大于该档自身的轮间散布：")
    flagged = False
    for i, w in enumerate(ws):
        m50, sp, _, _, _ = table[w]
        if m50 is None:
            continue
        nb = [table[ws[j]][0] for j in (i - 1, i + 1) if 0 <= j < len(ws) and table[ws[j]][0]]
        if len(nb) < 2:
            continue
        bump = m50 / max(nb) - 1
        if bump <= 0:
            continue
        verdict = "噪声" if sp is not None and sp >= bump else "*** 真实凸起 ***"
        if verdict != "噪声":
            flagged = True
        print(f"  {w:5d}us: 比两侧最高的一档还高 {bump:+.1%}，自身散布 ±{sp:.1%}  → {verdict}"
              if sp is not None else f"  {w:5d}us: 高 {bump:+.1%}，无散布数据")
    if not flagged:
        print("  没有一档的凸起超过自身噪声——上一轮的非单调是单点抖动，选窗口时不必理会")

    # 问题 2：批大小饱和点
    print("\n[饱和点] 批大小相对上一档的增幅：")
    prev = None
    knee = None
    for w in ws:
        b = table[w][4]
        if b is None:
            continue
        if prev is not None and prev > 0:
            gain = b / prev - 1
            print(f"  {w:5d}us: 批大小 {b:.2f}（+{gain:.0%}）")
            if knee is None and gain < 0.15:
                knee = w
        else:
            print(f"  {w:5d}us: 批大小 {b:.2f}")
        prev = b
    if knee:
        print(f"  → 批大小在 {knee}us 之后基本不再增长，再加窗口只是让单条多等")

    best = min((w for w in ws if table[w][0] is not None), key=lambda w: table[w][0], default=None)
    if best is not None:
        print(f"\n[建议] p50 最低的是 {best}us（{table[best][0]:.3f}ms，批大小 {table[best][4]:.2f}）。"
              f"\n       若它与相邻档的差落在噪声里，优先取批大小已饱和、窗口更小的那一档——"
              f"\n       窗口是纯粹的等待，能小则小。")


if __name__ == "__main__":
    main()
