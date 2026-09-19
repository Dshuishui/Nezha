#!/usr/bin/env python3
"""四系统对比的论文级出图（10GB 三节点主结果）。

与 plot-maintable3.py 的分工：那个是**跑中诊断**用的（时间序列、落差、分位数总览），
这个只做一件事——把四系统 × 三操作的结论排成可以直接进论文的图。

排版按顶会要求定的，每一条都有理由：
  矢量 PDF      LaTeX 里 \\includegraphics 不会糊；PNG 只作预览
  色盲安全配色  Okabe-Ito 调色板，红绿色盲下四个系统仍然分得开
  填充纹理      颜色**之外**再加一层编码，论文常被黑白打印
  统一顺序      四个系统一律按"改动逐步累加"排，读者从左到右看出每一步的贡献
  无图表垃圾    去掉上/右边框、网格只留 y 轴且压到很淡、不用 3D/阴影/渐变
  对数轴        SCAN 与 GET 差三个数量级，同一张图里只能用对数轴
  数值标注      柱子上直接标值，读者不用拿尺子量
  相对倍率      次要信息用"× 基线"的形式标在柱内，避免再开一张图

用法:
    plot-paper4.py <maintable3-X.csv> <输出目录>
"""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ---------------------------------------------------------------------------
# 统一视觉规范
# ---------------------------------------------------------------------------
# 系统顺序 = 改动逐步累加。与驱动的 SYSTEMS 默认值一致，不按字母序。
SYS = ["baseline", "nezha-nogc", "nezha", "nezha-avp"]
# 论文里用的名字：代码里的标识符不适合直接印出来。
LABEL = {
    "baseline": "Original",
    "nezha-nogc": "Nezha-NoGC",
    "nezha": "Nezha",
    "nezha-avp": "Nezha-AVP",
}
# Okabe-Ito：公认的色盲安全调色板。灰色给基线，让三个改进版在视觉上成组。
COLOR = {
    "baseline": "#999999",
    "nezha-nogc": "#0072B2",
    "nezha": "#D55E00",
    "nezha-avp": "#009E73",
}
# 纹理是第二层编码。黑白打印时颜色全塌成灰阶，只能靠它区分。
HATCH = {"baseline": "", "nezha-nogc": "///", "nezha": "xxx", "nezha-avp": "..."}

plt.rcParams.update({
    "font.family": "serif",
    "font.serif": ["DejaVu Serif", "Times New Roman", "serif"],
    "font.size": 9,
    "axes.labelsize": 9,
    "axes.titlesize": 9.5,
    "xtick.labelsize": 8.5,
    "ytick.labelsize": 8.5,
    "legend.fontsize": 8,
    "legend.frameon": False,
    "axes.spines.top": False,      # 去掉上/右边框：Tufte 的 data-ink 原则
    "axes.spines.right": False,
    "axes.linewidth": 0.7,
    "grid.linewidth": 0.4,
    "grid.alpha": 0.25,
    "pdf.fonttype": 42,            # TrueType 而非 Type3：多数会议明确要求
    "ps.fonttype": 42,
})


def save(fig, outdir, name):
    os.makedirs(outdir, exist_ok=True)
    for ext in ("pdf", "png"):
        p = os.path.join(outdir, f"{name}.{ext}")
        fig.savefig(p, bbox_inches="tight", dpi=300)
        print(p)
    plt.close(fig)


def load(csvpath):
    """(system, op, vsize) -> row。同一格跑多次取最后一行，理由同 plot-maintable3。"""
    cell = {}
    with open(csvpath, newline="") as f:
        for r in csv.DictReader(f):
            cell[(r["system"], r["op"], r["vsize"])] = r
    return cell


def num(cell, sy, op, vs, field):
    r = cell.get((sy, op, vs))
    if not r:
        return None
    # **扫描吞吐必须由 p50 反算，不能读 ops_per_s。** 驱动把 ops_per_s 写成一位小数，
    # 而 40 次扫描跑 1750 秒是 0.023 scans/s——落盘时被舍成 "0.0"，照它画出来是
    # 一排"0.00"的柱子，读者会以为扫描吞吐是零。这不是测量错误，是 CSV 的精度不够；
    # 1000/p50 用的是同一次测量里精度足够的那一列，语义也更明确（每秒多少次扫描）。
    if field == "scans_per_s":
        p50 = r.get("p50_ms")
        try:
            v = float(p50)
            return 1000.0 / v if v > 0 else None
        except (TypeError, ValueError):
            return None
    try:
        return float(r[field])
    except (TypeError, ValueError, KeyError):
        return None


def grouped(ax, cell, op, field, vsizes, scale=1.0, fmt="{:.2f}"):
    """一个面板：x 轴是 value 档，每组四根柱子是四个系统。返回是否画出了东西。"""
    width = 0.8 / len(SYS)
    drew = False
    for i, sy in enumerate(SYS):
        xs, ys = [], []
        for j, vs in enumerate(vsizes):
            v = num(cell, sy, op, vs, field)
            # 缺的格子**留空，不画 0**：0 在延迟轴上意味着最好，
            # 会把"这一格没跑"读成"这个系统赢了"。
            if v is None:
                continue
            xs.append(j + (i - (len(SYS) - 1) / 2) * width)
            ys.append(v * scale)
        if not xs:
            continue
        drew = True
        bars = ax.bar(xs, ys, width=width * 0.92, label=LABEL[sy],
                      color=COLOR[sy], hatch=HATCH[sy],
                      edgecolor="white", linewidth=0.6, zorder=3)
        for b, y in zip(bars, ys):
            # **标注竖排。** 一组四根柱子挨得很近，横排的数字在数值接近时会直接连成
            # "1.711.691.691.70" 一团——读者分不出哪个数配哪根柱子。竖排彻底消掉这类碰撞，
            # 代价只是多占一点纵向空间（由 headroom 补）。
            ax.annotate(fmt.format(y), (b.get_x() + b.get_width() / 2, y),
                        textcoords="offset points", xytext=(0, 3),
                        ha="center", va="bottom", fontsize=6.4,
                        rotation=90, zorder=4)
        # **不在柱内标"× 基线"。** 第一版标了，四根柱子挨得近、矮柱又放不下，
        # 于是相邻两个标注糊成 "0.72×0.75×"，矮柱上还溢出到柱子外面——
        # 典型的图表垃圾：想多说一件事，结果把本来说清楚的那件也弄脏了。
        # 倍率放到图注和 meta.txt 里讲，图上只留绝对值。
    ax.set_xticks(range(len(vsizes)))
    ax.set_xticklabels([f"{v} B" for v in vsizes])
    ax.set_xlabel("value size")
    ax.grid(axis="y", zorder=0)
    ax.set_axisbelow(True)
    return drew


def toplegend(fig, ax, ncol=4, y=1.10):
    """图例统一放到**图外**顶部。

    第一版把图例放在某个面板内（loc="upper left" 之类），结果在 p99 那张图上直接
    压住了 Original 与 Nezha-NoGC 两根柱子，连数值标注一起遮掉——
    图例本该帮读者认柱子，反而把柱子藏了。放到图外就不存在这个问题。
    """
    h, l = ax.get_legend_handles_labels()
    fig.legend(h, l, ncol=ncol, loc="upper center", bbox_to_anchor=(0.5, y),
               handlelength=1.4, columnspacing=1.6, borderaxespad=0)


def headroom(ax, factor=1.30):
    """给柱顶的数值标注留出空间，否则标注会贴到边框上。"""
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, hi * factor)


# ---------------------------------------------------------------------------
# 统一版式：**左吞吐、右时延**
# ---------------------------------------------------------------------------
# 吞吐一律用 MB/s 而不是 ops/s：三个操作的单次数据量差了三个数量级
# （一次 GET 取 1 个 value，一次 SCAN 取上千万个），ops/s 放在一起没法横向比，
# MB/s 才是同一个量纲。
#
# mb_per_s 这一列用的是十进制 MB（10^6），不是 MiB——与 bytes/elapsed_s 反算差 4.86%，
# 全表一致，所以直接取用，只需在图注里写清单位口径。
# 它有 4 位小数，不存在 ops_per_s 那种被舍成 0 的问题（见 num() 里的说明）。


def pair(cell, vsizes, outdir, op, name, title,
         lat_field="p50_ms", lat_scale=1.0, lat_unit="latency (ms)",
         lat_fmt="{:.2f}", lat_title="Latency, p50"):
    """一张图两个面板：左吞吐（MB/s），右时延。"""
    fig, axes = plt.subplots(1, 2, figsize=(5.6, 2.7))
    grouped(axes[0], cell, op, "mb_per_s", vsizes, fmt="{:.1f}")
    axes[0].set_ylabel("throughput (MB/s)")
    axes[0].set_title("Throughput")
    headroom(axes[0])

    grouped(axes[1], cell, op, lat_field, vsizes, scale=lat_scale, fmt=lat_fmt)
    axes[1].set_ylabel(lat_unit)
    axes[1].set_title(lat_title)
    headroom(axes[1])

    fig.tight_layout()
    # 顺序：总标题最上，图例紧随其下，都在图外。
    # 第一版 suptitle 用 y=1.02、图例用 y=1.10，于是图例跑到了标题**上面**，
    # 两者还挨着——读起来像图例是标题的一部分。
    fig.suptitle(title, y=1.17, fontsize=9.5)
    toplegend(fig, axes[0], y=1.05)
    save(fig, outdir, name)


def fig_scan(cell, vsizes, outdir):
    """扫描：KV 分离让它变慢，GC 不但补回来还比不做分离的基线更快。"""
    pair(cell, vsizes, outdir, "SCAN", "fig1-scan",
         "Range scan (1 GB per scan, 40 scans per cell)",
         lat_scale=1 / 1000.0, lat_unit="scan latency (s)", lat_fmt="{:.1f}")


def fig_put(cell, vsizes, outdir):
    """写：p50 被 Raft 共识锁死（四家差 3%），所以时延这一侧只看 p99。"""
    pair(cell, vsizes, outdir, "PUT", "fig2-put",
         "Write  (p50 is consensus-bound and identical across systems; the tail is not)",
         lat_field="p99_ms", lat_unit="write latency (ms)",
         lat_fmt="{:.2f}", lat_title="Latency, p99")


def fig_get(cell, vsizes, outdir):
    """点读：AVP 把 Nezha 在小值上的读代价补回一部分（具体比例见 meta.txt）。"""
    pair(cell, vsizes, outdir, "GET", "fig3-get",
         "Point read",
         lat_unit="point-read latency (ms)", lat_fmt="{:.3f}")


def fig_throughput(cell, vsizes, outdir):
    """三个操作的吞吐总览，统一 MB/s。"""
    fig, axes = plt.subplots(1, 3, figsize=(7.4, 2.6))
    for ax, (op, title) in zip(axes, [("PUT", "Write"),
                                      ("GET", "Point read"),
                                      ("SCAN", "Range scan")]):
        grouped(ax, cell, op, "mb_per_s", vsizes, fmt="{:.1f}")
        ax.set_ylabel("throughput (MB/s)" if ax is axes[0] else "")
        ax.set_title(title)
        headroom(ax)
    fig.tight_layout()
    toplegend(fig, axes[0])
    save(fig, outdir, "fig5-throughput")


# ---------------------------------------------------------------------------
# 图 4：空间 —— 13.5 倍的日志回收
# ---------------------------------------------------------------------------
def fig_space(outdir, space):
    """space: {system: (raft_log_bytes, data_dir_bytes)}，来自各格最后一个采样点。"""
    fig, ax = plt.subplots(figsize=(3.6, 2.7))
    xs = range(len(SYS))
    vals = [space[s][0] / 1024 ** 3 for s in SYS if s in space]
    names = [s for s in SYS if s in space]
    bars = ax.bar(range(len(names)), vals, width=0.62,
                  color=[COLOR[s] for s in names],
                  hatch=[HATCH[s] for s in names],
                  edgecolor="white", linewidth=0.6, zorder=3)
    base = space[SYS[0]][0] / 1024 ** 3 if SYS[0] in space else None
    for b, v, s in zip(bars, vals, names):
        ax.annotate(f"{v:.2f} GB", (b.get_x() + b.get_width() / 2, v),
                    textcoords="offset points", xytext=(0, 2.5),
                    ha="center", fontsize=7, zorder=4)
        # 只在真的小一截时才标。"1.0× smaller" 是句废话——Nezha-NoGC 与 Original
        # 的日志一样大（它不回收），标出来反而像在暗示它更省。
        if base and v > 0 and s != SYS[0] and base / v > 1.1:
            ax.annotate(f"{base / v:.1f}× smaller",
                        (b.get_x() + b.get_width() / 2, v),
                        textcoords="offset points", xytext=(0, 13),
                        ha="center", fontsize=6.4, color=COLOR[s], zorder=4)
    ax.set_xticks(range(len(names)))
    ax.set_xticklabels([LABEL[s] for s in names], rotation=18, ha="right")
    ax.set_ylabel("Raft log / value log on disk (GB)")
    ax.set_title("Log space after the run  (10 GB written, 64 B)")
    ax.grid(axis="y", zorder=0)
    ax.set_axisbelow(True)
    headroom(ax, 1.35)
    fig.tight_layout()
    save(fig, outdir, "fig4-log-space")


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    csvpath, outdir = sys.argv[1], sys.argv[2]
    cell = load(csvpath)
    vsizes = sorted({vs for (_, _, vs) in cell}, key=int)
    missing = [s for s in SYS if not any(k[0] == s for k in cell)]
    if missing:
        print(f"注意: CSV 里缺这些系统: {', '.join(missing)}", file=sys.stderr)

    fig_scan(cell, vsizes, outdir)
    fig_put(cell, vsizes, outdir)
    fig_get(cell, vsizes, outdir)
    fig_throughput(cell, vsizes, outdir)

    # 空间数据不在主 CSV 里，由调用方通过环境变量给（见归档脚本）。
    sp = os.environ.get("SPACE_BYTES", "")
    if sp:
        space = {}
        for item in sp.split(";"):
            if not item.strip():
                continue
            name, logb, datab = item.split(":")
            space[name] = (int(logb), int(datab))
        if space:
            fig_space(outdir, space)


if __name__ == "__main__":
    main()
