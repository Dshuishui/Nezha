#!/usr/bin/env python3
"""主表对比图：四个系统 × PUT/GET/SCAN × 三档 value。

出图口径按同类工作（WiscKey / HashKV / DiffKV / Titan / Scavenger）的通行做法：

- 每个操作单独一个子图。三项的量级差两到三个数量级（SCAN 约 1e2，GET 约 1e5），
  挤在一张图里小的那组会被压成一条线。
- 柱状分组：横轴是 value 大小，同一档内四根柱是四个系统。要比较的是"同一条件下
  哪个系统更好"，分组柱最直观。
- 误差棒取三轮的最小/最大值，不是标准差。三个样本的标准差没有统计意义，
  而极差如实地说明"重复跑能差多少"。
- 灰度可读：颜色 + 填充纹理双重编码。论文常被黑白打印，只靠颜色区分等于没区分。
- 输出 PDF（矢量，供 LaTeX 直接 \\includegraphics）与 PNG（供预览）。

用法:
    plot-maintable.py <data.csv> [输出目录] [--metric throughput|p50|p99]
"""
import csv
import statistics as st
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import FuncFormatter

# 四个系统只差存储侧开关，顺序按"改动逐步累加"排，读者从左到右就能看出每一步的贡献。
SYSTEMS = ["baseline", "nezha-nogc", "nezha", "nezha-avp"]
LABELS = {
    "baseline": "Baseline",
    "nezha-nogc": "Nezha-NoGC",
    "nezha": "Nezha",
    "nezha-avp": "Nezha-AVP",
}
# 冷灰 → 暖色，最后一个（我们的完整方案）用最醒目的一个；纹理保证黑白可读。
COLORS = ["#4a5568", "#7f9eb2", "#e0a458", "#c1666b"]
HATCHES = ["", "///", "...", "xxx"]
OPS = ["PUT", "GET", "SCAN"]
VSIZES = [64, 256, 1024]

METRIC = {
    # 两种吞吐口径都保留：ops/s 是请求速率，MB/s 是数据速率。
    # 同样的 ops/s 在 1024B 档搬运的数据是 64B 档的 16 倍，只看 ops/s 会低估大 value；
    # 反过来只看 MB/s 又会掩盖小 value 下的请求处理能力。论文里通常两者并列。
    "throughput": ("ops_per_s", "Throughput (ops/s)", "higher is better"),
    "mbps": ("mb_per_s", "Throughput (MB/s)", "higher is better"),
    "p50": ("p50_ms", "Median latency (ms)", "lower is better"),
    "p99": ("p99_ms", "99th-percentile latency (ms)", "lower is better"),
}


def load(path):
    rows = defaultdict(list)
    with open(path) as f:
        for r in csv.DictReader(f):
            rows[(r["system"], r["op"], int(r["vsize"]))].append(r)
    return rows


def stat(rows, key, col):
    """返回 (中位数, 最小, 最大, 全部样本)。样本要原样带出来画到图上。"""
    rs = rows.get(key, [])
    vals = [float(r[col]) for r in rs if r[col] not in ("NA", "")]
    if not vals:
        return None
    return st.median(vals), min(vals), max(vals), vals


def plot(data, out_dir, metric, logy=False, norm=False):
    col, ylabel, direction = METRIC[metric]
    if norm:
        # 归一化到 baseline：每格除以该 (操作, value 档) 下 baseline 的中位数。
        # 好处是把"绝对值差一个数量级"的三个面板放到同一把尺子上，读者一眼看到
        # 的是倍数而不是刻度；代价是丢掉绝对性能，所以它是绝对值图的补充而非替代。
        ylabel = f"Normalized {ylabel.split(' (')[0].lower()}"
        direction = "baseline = 1.0; " + ("higher is better" if metric in ("throughput", "mbps") else "lower is better")
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif"],
        "font.size": 9,
        "axes.linewidth": 0.8,
        "axes.labelsize": 10,
        "xtick.labelsize": 9,
        "ytick.labelsize": 8,
        "legend.fontsize": 9,
        "legend.frameon": False,
        # 纹理调细调浅：默认的粗黑斜线会把细误差棒淹掉，而纹理只是给黑白打印用的
        # 冗余编码，不该抢主体。
        "hatch.linewidth": 0.55,
        "hatch.color": "#33333366",
        # 论文图不留多余边距，交给 LaTeX 排版
        "figure.constrained_layout.use": True,
    })

    # 上方留出图例的高度，否则图例会压住子图标题
    fig, axes = plt.subplots(1, 3, figsize=(9.8, 3.1))

    def thousands(x, _):
        """10 万写成 100k：论文图的纵轴不该占掉半个子图的宽度。"""
        if x >= 1000:
            v = x / 1000
            return f"{v:.0f}k" if v == int(v) else f"{v:.1f}k"
        return f"{x:.0f}" if x == int(x) else f"{x:g}"

    width = 0.2

    for ax, op in zip(axes, OPS):
        xs = range(len(VSIZES))
        for si, sysname in enumerate(SYSTEMS):
            meds, los, his, samples = [], [], [], []
            for v in VSIZES:
                s = stat(data, (sysname, op, v), col)
                if s is None:
                    meds.append(0); los.append(0); his.append(0); samples.append([])
                    continue
                m, lo, hi, vals = s
                if norm:
                    b = stat(data, ("baseline", op, v), col)
                    if b is None or b[0] == 0:
                        meds.append(0); los.append(0); his.append(0); samples.append([])
                        continue
                    d = b[0]
                    m, lo, hi = m / d, lo / d, hi / d
                    vals = [x / d for x in vals]
                meds.append(m); los.append(m - lo); his.append(hi - m); samples.append(vals)
            pos = [x + (si - 1.5) * width for x in xs]
            ax.bar(pos, meds, width, yerr=[los, his],
                   color=COLORS[si], hatch=HATCHES[si], edgecolor="black",
                   linewidth=0.6, zorder=2,
                   error_kw=dict(lw=1.1, capsize=3.2, capthick=1.1,
                                 ecolor="#111111", zorder=4),
                   label=LABELS[sysname])
            # 三轮的原始值直接画上去。轮间方差不到 2%，误差棒本身几乎看不见；
            # 把样本点画出来既说明 n=3，也让读者自己判断离散程度——
            # 这比放大误差棒诚实（放大会让"很稳"看起来像"有波动"）。
            for p, vals in zip(pos, samples):
                if len(vals) < 2:
                    continue
                spread = width * 0.16
                offs = [(k - (len(vals) - 1) / 2) * spread for k in range(len(vals))]
                ax.plot([p + o for o in offs], vals, linestyle="none", marker="o",
                        markersize=2.6, markerfacecolor="white",
                        markeredgecolor="#111111", markeredgewidth=0.6, zorder=5)

        ax.set_xticks(list(xs))
        ax.set_xticklabels([f"{v}B" for v in VSIZES])
        ax.set_xlabel("Value size")
        ax.set_title(op, fontsize=10, pad=4)
        ax.grid(axis="y", linewidth=0.4, alpha=0.35)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
        ax.margins(x=0.06)
        if norm:
            # baseline 的参考线要压在柱子下面，否则会在柱面上划一道
            ax.axhline(1.0, color="#111111", linewidth=0.8, linestyle=(0, (4, 3)), zorder=1)
        if metric == "throughput" and not logy:
            ax.yaxis.set_major_formatter(FuncFormatter(thousands))
        if logy:
            # MB/s 跨 value 档相差一个数量级以上（每次操作搬的数据量差 16 倍），
            # 线性轴下小 value 那组被压扁，而要比的恰恰是同一档内四个系统的差别。
            ax.set_yscale("log")
            ax.grid(axis="y", which="both", linewidth=0.4, alpha=0.3)

    axes[0].set_ylabel(f"{ylabel}\n({direction})")

    handles = [Patch(facecolor=COLORS[i], hatch=HATCHES[i], edgecolor="black",
                     linewidth=0.6, label=LABELS[s]) for i, s in enumerate(SYSTEMS)]
    fig.legend(handles=handles, loc="upper center", ncol=4,
               bbox_to_anchor=(0.5, 1.10), columnspacing=1.6, handlelength=1.6)

    suffix = ("-norm" if norm else "") + ("-log" if logy else "")
    for ext in ("pdf", "png"):
        p = f"{out_dir}/maintable-{metric}{suffix}.{ext}"
        fig.savefig(p, dpi=200, bbox_inches="tight")
        print(p)
    plt.close(fig)


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    metrics = [a.split("=", 1)[1] for a in sys.argv[1:] if a.startswith("--metric=")]
    if not args:
        sys.exit(__doc__)
    csv_path = args[0]
    out_dir = args[1] if len(args) > 1 else "."
    data = load(csv_path)
    for m in (metrics or ["throughput", "mbps", "p50", "p99"]):
        plot(data, out_dir, m)
    if not metrics:
        plot(data, out_dir, "mbps", logy=True)
        plot(data, out_dir, "throughput", norm=True)
        plot(data, out_dir, "p99", norm=True)


if __name__ == "__main__":
    main()
