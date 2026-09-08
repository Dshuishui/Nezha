#!/usr/bin/env python3
"""放大率图：四个系统 × 三档 value × 覆盖比例。

横轴取覆盖比例（垃圾量），因为要看的正是"回收代价随垃圾量怎么变"——
当前 GC 全量重写，代价与垃圾量无关，画出来应该是平的；按垃圾量回收的设计
应该随垃圾量上升。这条"平线 vs 斜线"的对比是 GC 改造最直接的论据。

风格与 plot-maintable.py 一致：serif、颜色+纹理双编码、PDF+PNG。

用法: plot-amplification.py <data.csv> [输出目录]
"""
import csv
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

SYSTEMS = ["baseline", "nezha-nogc", "nezha", "nezha-avp"]
LABELS = {"baseline": "Baseline", "nezha-nogc": "Nezha-NoGC",
          "nezha": "Nezha", "nezha-avp": "Nezha-AVP"}
COLORS = ["#4a5568", "#7f9eb2", "#e0a458", "#c1666b"]
HATCHES = ["", "///", "...", "xxx"]
VSIZES = [64, 256, 1024]
OVERWRITES = [25, 50, 100]

METRIC = {
    "write-amp": ("write_amp_total", "Write amplification", "lower is better"),
    "space-amp": ("space_amp", "Space amplification", "lower is better"),
}


def load(path):
    d = {}
    for r in csv.DictReader(open(path)):
        d[(r["system"], int(r["vsize"]), int(r["overwrite_pct"]))] = r
    return d


def plot(data, out_dir, metric):
    col, ylabel, direction = METRIC[metric]
    plt.rcParams.update({
        "font.family": "serif",
        "font.serif": ["Times New Roman", "DejaVu Serif"],
        "font.size": 9, "axes.linewidth": 0.8, "axes.labelsize": 10,
        "xtick.labelsize": 9, "ytick.labelsize": 8,
        "legend.fontsize": 9, "legend.frameon": False,
        "hatch.linewidth": 0.55, "hatch.color": "#33333366",
        "figure.constrained_layout.use": True,
    })
    fig, axes = plt.subplots(1, 3, figsize=(9.8, 3.1))
    width = 0.2

    for ax, vs in zip(axes, VSIZES):
        xs = range(len(OVERWRITES))
        for si, sysname in enumerate(SYSTEMS):
            vals = []
            for o in OVERWRITES:
                r = data.get((sysname, vs, o))
                v = r[col] if r else "NA"
                vals.append(float(v) if v not in ("NA", "") else 0.0)
            pos = [x + (si - 1.5) * width for x in xs]
            ax.bar(pos, vals, width, color=COLORS[si], hatch=HATCHES[si],
                   edgecolor="black", linewidth=0.6, zorder=2, label=LABELS[sysname])

        # 1.0 是"写下去多少就落盘多少"的理想下界，给读者一把尺子
        ax.axhline(1.0, color="#111111", linewidth=0.8, linestyle=(0, (4, 3)), zorder=1)
        ax.set_xticks(list(xs))
        ax.set_xticklabels([f"{o}%" for o in OVERWRITES])
        ax.set_xlabel("Overwritten fraction")
        ax.set_title(f"value = {vs}B", fontsize=10, pad=4)
        ax.grid(axis="y", linewidth=0.4, alpha=0.35)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
        ax.margins(x=0.06)

    axes[0].set_ylabel(f"{ylabel}\n({direction})")
    handles = [Patch(facecolor=COLORS[i], hatch=HATCHES[i], edgecolor="black",
                     linewidth=0.6, label=LABELS[s]) for i, s in enumerate(SYSTEMS)]
    fig.legend(handles=handles, loc="upper center", ncol=4,
               bbox_to_anchor=(0.5, 1.10), columnspacing=1.6, handlelength=1.6)

    for ext in ("pdf", "png"):
        p = f"{out_dir}/amplification-{metric}.{ext}"
        fig.savefig(p, dpi=200, bbox_inches="tight")
        print(p)
    plt.close(fig)


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    data = load(sys.argv[1])
    out_dir = sys.argv[2] if len(sys.argv) > 2 else "."
    for m in METRIC:
        plot(data, out_dir, m)


if __name__ == "__main__":
    main()
