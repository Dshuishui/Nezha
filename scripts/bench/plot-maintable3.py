#!/usr/bin/env python3
"""三节点跑的出图：延迟分位数、时间序列、跨节点落后。

与 plot-maintable.py 的分工：那个画的是**单节点四系统主表**（论文的主结果）。
这个画的是 maintable3.sh 的产物，回答的是另一类问题——"多节点下有没有出问题"：
内存有没有随规模涨、fd 有没有泄漏、GC 的节奏、三个副本有没有掉队。
所以它的主角是**时间序列**，而不是柱状对比。

三张图，各自对着一件事：

  latency   每个操作的 p50/p90/p95/p99。SCAN 与 GET 差三个数量级，所以分子图、对数轴。
            **SCAN 的 p999 不画**：250 次扫描时 p999 就是最大值那一个样本，
            画出来会被当成一个分位数读。这一条写在图注里。
  series    每格一张，四个面板共用时间轴：RSS / fd 数 / GC 轮数 / 空间放大。
            三个节点三条线。这是"有没有随规模无界增长"唯一看得出来的形式——
            一个峰值标量说不了它是平的还是在爬。
  lag       每个节点的压缩点与当时最快那台的差，随时间。判"某台副本掉队"用它，
            不用活动日志的字节数：某台刚把 valuelog 切成有序文件时字节数会瞬间
            变短，健康的运行里也会大幅分叉（见 maintable3.sh 的 lag_check）。

用法:
    plot-maintable3.py latency <maintable3-X.csv> [输出目录]
    plot-maintable3.py series  <mt3-X 归档目录>   [输出目录]
    plot-maintable3.py lag     <mt3-X 归档目录>   [输出目录]
    plot-maintable3.py ab <修复前的 mt3-X 目录> <修复后的> [输出目录]   混合阶段的 A/B

matplotlib 只在 winbox 上装着（3.6.3），实验机与 Mac 都没有，所以出图在 winbox 上做。
"""
import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

# 灰度可读：颜色 + 线型双重编码。论文常被黑白打印，只靠颜色区分等于没区分。
NODE_STYLE = {
    "node0": ("#1f4e79", "-", "o"),
    "node1": ("#b45f06", "--", "s"),
    "node2": ("#38761d", ":", "^"),
}
OPS = ["PUT", "GET", "SCAN", "MIXPUT", "MIXGET", "MIXSCAN"]
PCTS = ["p50_ms", "p90_ms", "p95_ms", "p99_ms"]
PCT_LABEL = {"p50_ms": "p50", "p90_ms": "p90", "p95_ms": "p95", "p99_ms": "p99"}


def save(fig, outdir, name):
    """PDF（矢量，供 LaTeX）与 PNG（供预览）各存一份，与 plot-maintable.py 一致。"""
    os.makedirs(outdir, exist_ok=True)
    for ext in ("pdf", "png"):
        path = os.path.join(outdir, f"{name}.{ext}")
        fig.savefig(path, bbox_inches="tight", dpi=150)
        print(path)
    plt.close(fig)


def read_main(path):
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def fnum(row, key):
    """CSV 里取不到的字段写的是 NA，不是空——空字段在表里看起来只是"这列没测"。"""
    v = row.get(key, "NA")
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# latency
# ---------------------------------------------------------------------------
def plot_latency(csvpath, outdir):
    rows = read_main(csvpath)
    # 按 (op, vsize) 收。同一格可能被跑过多次（重跑、A/B），取最后一行——
    # 不取平均：两次跑之间可能换了 commit，平均掉就把差异抹平了。
    cell = {}
    vsizes = []
    for r in rows:
        op, vs = r["op"], r["vsize"]
        if op not in OPS:
            continue
        cell[(op, vs)] = r
        if vs not in vsizes:
            vsizes.append(vs)
    vsizes.sort(key=int)
    present = [op for op in OPS if any((op, vs) in cell for vs in vsizes)]
    if not present:
        sys.exit(f"{csvpath} 里没有可画的操作行")

    fig, axes = plt.subplots(1, len(present), figsize=(3.4 * len(present), 3.2))
    if len(present) == 1:
        axes = [axes]
    width = 0.8 / max(len(vsizes), 1)
    for ax, op in zip(axes, present):
        for i, vs in enumerate(vsizes):
            r = cell.get((op, vs))
            if not r:
                continue
            xs = [j + (i - (len(vsizes) - 1) / 2) * width for j in range(len(PCTS))]
            ys = [fnum(r, k) or 0.0 for k in PCTS]
            ax.bar(xs, ys, width=width, label=f"{vs}B",
                   color=["#1f4e79", "#b45f06", "#38761d"][i % 3],
                   hatch=["", "//", "xx"][i % 3], edgecolor="white", linewidth=0.5)
        ax.set_xticks(range(len(PCTS)))
        ax.set_xticklabels([PCT_LABEL[k] for k in PCTS])
        ax.set_yscale("log")
        ax.set_title(op)
        ax.grid(axis="y", alpha=0.3, which="both")
        if ax is axes[0]:
            ax.set_ylabel("Latency (ms)")
    axes[0].legend(title="value size", fontsize=8)
    fig.suptitle("Three-node latency percentiles (log scale)", y=1.02)
    save(fig, outdir, "latency")


# ---------------------------------------------------------------------------
# 采样时间序列
# ---------------------------------------------------------------------------
SAMPLE_COLS = ["ts", "rss_kb", "fds", "threads", "gc", "base_index", "term",
               "log_bytes", "data_bytes", "disk_avail_kb", "pinned", "truncates",
               "budget", "stuck", "snap_sent", "snap_made", "snap_installed",
               "elections", "won", "slow_appends", "lock_stalls"]


def read_samples(celldir):
    """读一格里三个节点的 sample CSV。回 {node: {列名: [值]}}。"""
    out = {}
    for node in ("node0", "node1", "node2"):
        path = os.path.join(celldir, f"sample-{node}.csv")
        if not os.path.exists(path):
            continue
        cols = defaultdict(list)
        with open(path, newline="") as f:
            for r in csv.DictReader(f):
                for k in SAMPLE_COLS:
                    v = r.get(k)
                    try:
                        cols[k].append(float(v))
                    except (TypeError, ValueError):
                        cols[k].append(float("nan"))
        if cols["ts"]:
            out[node] = cols
    return out


def cells_of(archive):
    """归档目录下每个子目录是一格（B-64B、C-64B……）。"""
    return sorted(d for d in os.listdir(archive)
                  if os.path.isdir(os.path.join(archive, d)))


def plot_series(archive, outdir):
    for cellname in cells_of(archive):
        celldir = os.path.join(archive, cellname)
        data = read_samples(celldir)
        if not data:
            continue
        t0 = min(c["ts"][0] for c in data.values())
        panels = [
            ("rss_kb", lambda v: v / 1024.0, "RSS (MB)"),
            ("fds", lambda v: v, "Open fds"),
            ("gc", lambda v: v, "GC rounds"),
            # 画的是**绝对占用**而不是放大率：放大率的分母（逻辑字节）历史口径是
            # "补齐宽度 10 + value"，而那个口径本身有争议（见 maintable.sh 里 LOGICAL
            # 那段）。绝对占用没有口径问题，而"有没有随轮数无界增长"看绝对值就够了。
            ("data_bytes", lambda v: v / 1048576.0, "Data dir (MB)"),
        ]
        fig, axes = plt.subplots(len(panels), 1, figsize=(7.2, 2.0 * len(panels)),
                                 sharex=True)
        for ax, (key, conv, ylabel) in zip(axes, panels):
            for node, cols in sorted(data.items()):
                color, ls, _ = NODE_STYLE[node]
                xs = [(t - t0) / 60.0 for t in cols["ts"]]
                ys = [conv(v) for v in cols[key]]
                # GC 轮数是阶跃量，用 step 画；连线会让"某一刻跳了一轮"看成斜坡。
                if key == "gc":
                    ax.step(xs, ys, where="post", color=color, ls=ls, lw=1.4, label=node)
                else:
                    ax.plot(xs, ys, color=color, ls=ls, lw=1.2, label=node)
            ax.set_ylabel(ylabel, fontsize=9)
            ax.grid(alpha=0.3)
        axes[0].legend(fontsize=8, ncol=3)
        axes[-1].set_xlabel("Minutes since cell start")
        fig.suptitle(f"{cellname}: memory, fds, GC rounds, disk", y=0.995)
        save(fig, outdir, f"series-{cellname}")


# ---------------------------------------------------------------------------
# 跨节点落后
# ---------------------------------------------------------------------------
def plot_lag(archive, outdir):
    for cellname in cells_of(archive):
        celldir = os.path.join(archive, cellname)
        data = read_samples(celldir)
        if len(data) < 2:
            continue
        # 三个节点的采样时刻不会重合：三个独立的循环，各自的相位不同。
        # 第一版按**秒**取整再要求三台都有，于是基本对不上——A-64B 那一格直接被跳过
        # （A-256B 侥幸有几秒重合，所以问题只在一半的图上显形，更难发现）。
        #
        # 改成按窗口分桶，桶宽取采样间隔的两倍，桶内取每个节点**最近的一次实测值**。
        # 这不是插值：报出去的每个数都是真采到的，只是把"同一时间段"定义得比一秒宽。
        # 仍然要求三台在这个桶里都有值——缺一台就不画那个点，不拿上一轮的值顶替。
        ts_all = sorted(t for cols in data.values() for t in cols["ts"])
        if len(ts_all) < 2:
            continue
        # 采样间隔从数据自己推，不写死：驱动的 SAMPLE_IV 在不同跑法里是 15 或 30。
        gaps = sorted(b - a for a, b in zip(ts_all, ts_all[1:]) if b > a)
        step = gaps[len(gaps) // 2] * 2 if gaps else 30.0
        step = max(step, 2.0)
        buckets = defaultdict(dict)
        for node, cols in data.items():
            for t, b in zip(cols["ts"], cols["base_index"]):
                k = int(t // step)
                prev = buckets[k].get(node)
                if prev is None or t >= prev[0]:
                    buckets[k][node] = (t, b)
        common = sorted(k for k, d in buckets.items() if len(d) == len(data))
        if not common:
            print(f"{cellname}: no window holds a sample from all {len(data)} nodes, skipped")
            continue
        by_ts = {k: {n: v[1] for n, v in buckets[k].items()} for k in common}
        t0 = common[0] * step
        fig, ax = plt.subplots(figsize=(7.2, 3.0))
        for node in sorted(data):
            color, ls, _ = NODE_STYLE[node]
            xs = [(k * step - t0) / 60.0 for k in common]
            ys = [max(by_ts[k].values()) - by_ts[k][node] for k in common]
            ax.plot(xs, ys, color=color, ls=ls, lw=1.2, label=node)
        ax.set_xlabel("Minutes since cell start")
        ax.set_ylabel("Entries behind the leading node\n(compaction point)")
        ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else f"{v:.0f}"))
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
        ax.set_title(f"{cellname}: replica lag")
        save(fig, outdir, f"lag-{cellname}")


# ---------------------------------------------------------------------------
# 混合阶段的 A/B
# ---------------------------------------------------------------------------
LAT_KEYS = ("mean", "p50", "p90", "p95", "p99", "p999", "min", "max")


def parse_latency_line(path):
    """从 bench 工具的输出里取那行 [LATENCY]，回 {p50: 0.71, max: 2430.024, ...}。

    为什么直接读 `mixed-put.out` 而不读汇总 CSV：`MIXPUT` 这一列是 2026-09-18 才加进
    驱动的，在那之前跑的几轮**只有混合 GET 进了表**，混合 PUT 的数字只在这个文件里。
    而混合 PUT 恰恰是那个发现的关键量。直接读输出文件，新旧几轮都能画。
    """
    if not os.path.exists(path):
        return None
    line = None
    with open(path, errors="replace") as f:
        for ln in f:
            if ln.startswith("[LATENCY]"):
                line = ln.strip()
    if not line:
        return None
    out = {}
    for tok in line.split():
        if "=" not in tok:
            continue
        k, v = tok.split("=", 1)
        if k in LAT_KEYS:
            out[k] = float(v.rstrip("ms"))
    return out or None


def ab_cells(archive, op):
    """归档目录下每格的混合输出。回 {value档: {分位数: 值}}。
    格名形如 `B-64B` / `A-256B`，取中间那个数字当档位。"""
    out = {}
    for cellname in cells_of(archive):
        vs = cellname.split("-")[-1].rstrip("B")
        lat = parse_latency_line(os.path.join(archive, cellname, f"mixed-{op}.out"))
        if lat:
            out[vs] = lat
    return out


def plot_ab(before_dir, after_dir, outdir):
    """扫描是否还按住写入路径：混合阶段 PUT 的 p50 与 max 前后对比。

    两个面板而不是双轴：p50 是亚毫秒，修复前的 max 是 2.4 秒，同轴会把 p50 压成零。
    **要看的就是 max**——p50 前后不变才说明这不是"把延迟挪到别处"，
    所以两个面板必须一起看，只画 max 那一半是会误导人的。
    """
    rows_b = ab_cells(before_dir, "put")
    rows_a = ab_cells(after_dir, "put")
    vsizes = sorted(set(rows_b) & set(rows_a), key=int)
    if not vsizes:
        sys.exit("两个 CSV 没有共同的 value 档")
    fig, axes = plt.subplots(1, 2, figsize=(8.0, 3.2))
    for ax, key, title in ((axes[0], "p50", "p50 (should not move)"),
                           (axes[1], "max", "max (the one to read)")):
        xs = range(len(vsizes))
        b = [rows_b[v].get(key, 0.0) for v in vsizes]
        a = [rows_a[v].get(key, 0.0) for v in vsizes]
        ax.bar([x - 0.2 for x in xs], b, width=0.4, label="before",
               color="#b45f06", hatch="//", edgecolor="white")
        ax.bar([x + 0.2 for x in xs], a, width=0.4, label="after",
               color="#1f4e79", edgecolor="white")
        ax.set_xticks(list(xs))
        ax.set_xticklabels([f"{v}B" for v in vsizes])
        ax.set_ylabel("PUT latency (ms)")
        ax.set_title(title)
        ax.grid(axis="y", alpha=0.3)
    axes[0].legend(fontsize=8)
    fig.suptitle("Does a range scan still stall the write path? (mixed phase)", y=1.02)
    save(fig, outdir, "ab-scan-lock")


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    mode = sys.argv[1]
    if mode == "latency":
        plot_latency(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ".")
    elif mode == "series":
        plot_series(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ".")
    elif mode == "lag":
        plot_lag(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else ".")
    elif mode == "ab":
        if len(sys.argv) < 4:
            sys.exit("ab 需要两个归档目录（修复前、修复后）")
        plot_ab(sys.argv[2], sys.argv[3], sys.argv[4] if len(sys.argv) > 4 else ".")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
