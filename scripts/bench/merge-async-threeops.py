#!/usr/bin/env python3
"""把异步档的 PUT 与读测两份 CSV 合成 plot-paper4.py 认识的那一种。

为什么要有这个脚本：上一版的 `merged-leader-three-ops.csv` 是手工拼的，于是
**没人能从文件本身看出哪一列是实测、哪一列是借来的** —— 它的 GET/SCAN 其实来自
B3（多数派），只有 `label` 里那句 `aq-leader-PUT+B3-READ` 记着这件事，而图上什么
都看不出来。拼接必须留痕，否则下一个人（包括三天后的自己）会把它当成一整轮实测。

所以这里做两件事：
  1. `label` 写清每一行的来源（`quorum` 列也逐行带上）
  2. GET 有多遍重复时**取中位数**，并把遍数记进 `extra`

用法：
    merge-async-threeops.py --put putonly-aq-leader.csv \\
                            --read readops-ro-async.csv \\
                            --out merged.csv
"""
import argparse
import csv
import statistics
import sys

# plot-paper4.py 认的列，顺序不能动。
OUT_COLS = [
    "commit", "label", "phase", "topo", "system", "syncwal", "gcgb", "vsize",
    "entries", "total_mb", "op", "n", "mean_ms", "p50_ms", "p90_ms", "p95_ms",
    "p99_ms", "p999_ms", "min_ms", "max_ms", "ops", "bytes", "elapsed_s",
    "ops_per_s", "mb_per_s", "extra", "gc_max", "lost_keys", "rss_peak_mb",
    "fd_peak", "lag_spread",
]

NA = "NA"


def num(s):
    """空串与 NA 一律读成 None，**不要读成 0**。

    写 0 等于声称"测过了、就是零"，而实际是没拿到数。这个区别在图上看不出来，
    在结论里却是"没有优势"与"没有数据"的差别。
    """
    if s is None:
        return None
    s = s.strip()
    if s == "" or s.upper() == "NA":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def fmt(v):
    if v is None:
        return NA
    return f"{v:.4f}".rstrip("0").rstrip(".") if isinstance(v, float) else str(v)


def rows_of(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def merge(put_path, read_path, label_prefix):
    out = []
    quorums = set()

    # ---- PUT ----
    # putonly 的表里没有 p90/p95/p999/min/bytes，一律写 NA。
    for r in rows_of(put_path):
        quorums.add(r.get("quorum", "?"))
        out.append({
            "commit": r["commit"],
            "label": f"{label_prefix}-PUT",
            "phase": "B", "topo": "three",
            "system": r["system"],
            "syncwal": "0", "gcgb": "0.3",
            "vsize": r["vsize"], "entries": r["entries"], "total_mb": r["total_mb"],
            "op": "PUT", "n": r["ops"],
            "mean_ms": r.get("mean_ms", NA), "p50_ms": r.get("p50_ms", NA),
            "p90_ms": NA, "p95_ms": NA,
            "p99_ms": r.get("p99_ms", NA), "p999_ms": NA, "min_ms": NA,
            "max_ms": r.get("max_ms", NA),
            "ops": r["ops"], "bytes": NA,
            "elapsed_s": r["elapsed_s"],
            "ops_per_s": r["ops_per_s"], "mb_per_s": r["mb_per_s"],
            "extra": f"quorum={r.get('quorum','?')}",
            "gc_max": r.get("gc_rounds", NA),
            "lost_keys": NA, "rss_peak_mb": NA, "fd_peak": NA, "lag_spread": NA,
        })

    # ---- GET / SCAN ----
    # 按 (system, vsize, op) 收齐所有遍数，再取中位数。
    # **中位数而不是平均**：一格里偶尔有一遍被别的东西干扰（后台压实、发快照），
    # 平均会把它带进结果，中位数不会。遍数少于 3 时中位数等于中间那个，也仍然可用。
    buckets = {}
    for r in rows_of(read_path):
        quorums.add(r.get("quorum", "?"))
        key = (r["system"], int(r["vsize"]), r["op"])
        buckets.setdefault(key, []).append(r)

    for (system, vsize, op), rs in sorted(buckets.items(), key=lambda kv: (kv[0][2], kv[0][0], kv[0][1])):
        def med(col):
            vals = [num(x.get(col)) for x in rs]
            vals = [v for v in vals if v is not None]
            return statistics.median(vals) if vals else None

        # 代表行取中位吞吐那一遍，好让 ops/bytes/elapsed 自相一致
        # （逐列各取中位数会拼出一个**任何一遍都没出现过**的组合）。
        ranked = [x for x in rs if num(x.get("ops_per_s")) is not None]
        rep = sorted(ranked, key=lambda x: num(x["ops_per_s"]))[len(ranked) // 2] if ranked else rs[0]

        snaps = [num(x.get("snapshots")) for x in rs]
        snaps = [s for s in snaps if s is not None]
        extra = f"reps={len(rs)}"
        if snaps:
            extra += f";snap_during_read={int(max(snaps))}"
        hr = rep.get("hitrate", NA)
        if op == "GET" and hr not in (NA, "", None):
            extra += f";hitrate={hr}"

        out.append({
            "commit": rep["commit"],
            "label": f"{label_prefix}-{op}",
            "phase": "B", "topo": "three",
            "system": system,
            "syncwal": "0", "gcgb": "0.3",
            "vsize": str(vsize), "entries": rep["entries"], "total_mb": rep["total_mb"],
            "op": op, "n": rep.get("ops", NA),
            "mean_ms": fmt(med("mean_ms")), "p50_ms": fmt(med("p50_ms")),
            "p90_ms": fmt(med("p90_ms")), "p95_ms": fmt(med("p95_ms")),
            "p99_ms": fmt(med("p99_ms")), "p999_ms": fmt(med("p999_ms")),
            "min_ms": NA, "max_ms": fmt(med("max_ms")),
            "ops": rep.get("ops", NA), "bytes": rep.get("bytes", NA),
            "elapsed_s": rep.get("elapsed_s", NA),
            "ops_per_s": fmt(med("ops_per_s")), "mb_per_s": fmt(med("mb_per_s")),
            "extra": extra,
            "gc_max": rep.get("gc_rounds", NA),
            "lost_keys": NA, "rss_peak_mb": NA, "fd_peak": NA, "lag_spread": NA,
        })

    return out, quorums


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--put", required=True, help="putonly-*.csv（PUT 那一半）")
    ap.add_argument("--read", required=True, help="readops-*.csv（GET/SCAN 那一半）")
    ap.add_argument("--out", required=True)
    ap.add_argument("--label", default="aq-leader", help="写进 label 列的前缀")
    a = ap.parse_args()

    rows, quorums = merge(a.put, a.read, a.label)

    # **两半的提交条件必须一致。**混起来画一张图，图上没有任何迹象，
    # 而"只等 leader"与"等多数派"是完全不同的两个系统。
    quorums.discard("?")
    if len(quorums) > 1:
        sys.exit(f"两份输入的 quorum 不一致：{sorted(quorums)}——混起来画图没有意义，先确认口径")

    with open(a.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    ops = {}
    for r in rows:
        ops[r["op"]] = ops.get(r["op"], 0) + 1
    print(f"写出 {a.out}：{len(rows)} 行  " + "  ".join(f"{k}={v}" for k, v in sorted(ops.items())))
    print(f"提交条件 = {sorted(quorums) or ['未标注']}")
    missing = [f"{r['system']}/{r['vsize']}/{r['op']}" for r in rows if r["ops_per_s"] in (NA, "")]
    if missing:
        print(f"**{len(missing)} 格没有吞吐数据**（记 NA，不是 0）: " + ", ".join(missing[:8]))


if __name__ == "__main__":
    main()
