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
    plot-paper4.py <maintable3-X.csv> [更多.csv ...] <输出目录>

多个 CSV 会按 (system, op, vsize) 合并成一张表——论文那六个 value 档是分两轮跑出来的
（64B/256B 一轮，1KB/4KB/16KB/256KB 一轮），合并后横坐标才是完整的六档。
后给的文件覆盖先给的同名格子。
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


def load(*csvpaths):
    """(system, op, vsize) -> row。同一格跑多次取最后一行，理由同 plot-maintable3。

    多个文件依次读入，后来的覆盖先前的同名格子。**不同轮次的 CSV 能不能合并，
    取决于口径是否一致**——这里合并的两轮都是 10GiB / 三节点 / 每次扫描 1.07GB /
    GET 200 万次，只有 value 档不同，所以可以。口径不同的两轮合进一张图会骗人，
    合并前必须自己核对，脚本查不了这件事。
    """
    cell = {}
    for csvpath in csvpaths:
        with open(csvpath, newline="") as f:
            for r in csv.DictReader(f):
                cell[(r["system"], r["op"], r["vsize"])] = r
    return cell


def vlabel(vs):
    """横坐标上的 value 档名。六档跨 64B~256KB，一律写成 "1024 B" 又长又难认。
    数字与单位之间沿用文件里原有的窄空格，不然六档里两档的排版跟另外四档不一样。"""
    v = int(vs)
    if v >= 1024 and v % 1024 == 0:
        return f"{v // 1024} KB"
    return f"{v} B"


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


def grouped(ax, cell, op, field, vsizes, scale=1.0, fmt="{:.2f}",
            annotate=None, logy=None):
    """一个面板：x 轴是 value 档，每组四根柱子是四个系统。返回是否画出了东西。

    annotate 与 logy 默认按数据自己定，两条理由见下。
    """
    # **柱顶数值标注只在档位少时才放得下。** 两档时一个面板 8 根柱子，标得下；
    # 六档时是 24 根，在 2.4 英寸宽的面板里每根只有 0.1 英寸，竖排的数字会互相压住
    # ——又是"想多说一件事，结果把本来说清楚的那件弄脏了"。六档时去掉标注，
    # 具体数值放进同目录的对照表。
    if annotate is None:
        annotate = len(vsizes) <= 3
    vals_all = []
    for sy in SYS:
        for vs in vsizes:
            v = num(cell, sy, op, vs, field)
            if v is not None and v * scale > 0:
                vals_all.append(v * scale)
    # **跨度超过 30 倍就得上对数轴。** 六档合并之后 GET 吞吐从 64B 的 7.85 MB/s
    # 到 256KB 的 3135 MB/s，差 400 倍：线性轴上前四档会被压成贴着零线的一条毛边，
    # 读者只看得出"大 value 更快"这件早就知道的事，四个系统的差异全看不见。
    if logy is None:
        logy = bool(vals_all) and max(vals_all) / min(vals_all) > 30
    if logy:
        ax.set_yscale("log")
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
            if not annotate:
                break
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
    ax.set_xticklabels([vlabel(v) for v in vsizes],
                       rotation=30 if len(vsizes) > 3 else 0,
                       ha="right" if len(vsizes) > 3 else "center")
    ax.set_xlabel("value size")
    ax.grid(axis="y", zorder=0)
    ax.set_axisbelow(True)
    if logy and vals_all:
        # 对数轴画不出 0，柱底要显式给下界，否则 matplotlib 自己挑一个很小的数，
        # 把所有柱子拉成差不多长，柱高的**比例**就读不出来了。取最小值的一半。
        ax.set_ylim(bottom=min(vals_all) / 2)
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
    """给柱顶的数值标注留出空间，否则标注会贴到边框上。

    对数轴上乘一个常数就是往上挪固定的视觉距离，正好是要的效果，所以两种轴共用。
    """
    lo, hi = ax.get_ylim()
    ax.set_ylim(lo, hi * factor)


# leader 网卡 1000 Mb/s = 125 MB/s（十进制）出向，而每次 PUT 要把 value 复制给
# **两个** follower，于是应用层写吞吐上限约 62.5 MB/s。
# 这条线不是装饰：2026-09-19 实测 leader 出向在 256KB 档占 92.8% 线速、1KB 档只占
# 65.7%，说明大 value 的写吞吐差异是被这条链路压小的。不标的话审稿人会问
# "为什么大 value 上四个系统的写吞吐挤在一起"，而诚实的答案就是这条线。
NIC_CEILING_MBPS = 62.5


def put_ceiling(ax):
    """在写吞吐面板上画出链路上限。**够不着就不画。**

    第一版无条件画。64B/256B 那张图的最大值只有 14.5 MB/s，62.5 的线远在画布之外，
    于是 matplotlib 把纵轴一直拉到 62.5，`bbox_inches="tight"` 再把这一大片空白
    原样保留——整张图变成一片空白加底下一条柱子，柱间差异完全看不出来。
    一条"帮读者理解"的参考线，把它要解释的那张图毁掉了：与图内图例压住柱子、
    柱内标倍率糊成一团是同一类毛病。
    所以先看这个面板的量级：上限比画面顶还高出一倍以上，说明这一档离链路瓶颈还很远，
    画它没有信息量，不画。
    """
    top = ax.get_ylim()[1]
    if NIC_CEILING_MBPS > 2.2 * top:
        return False
    ax.axhline(NIC_CEILING_MBPS, color="#444444", linestyle=(0, (4, 3)),
               linewidth=0.8, zorder=5)
    # **标签靠左，不靠右。** 靠右在异步复制档那张图上正好压在 1KB/4KB 的高柱上
    # ——那一档的柱子越过了这条线，右侧没有空位。左端是 64B，柱子最矮，
    # 两种模式的图都有余量。
    ax.annotate("1 GbE ceiling (2x replication)", (0.015, NIC_CEILING_MBPS),
                xycoords=("axes fraction", "data"),
                textcoords="offset points", xytext=(0, 2.5),
                ha="left", va="bottom", fontsize=6.2, color="#444444", zorder=5)
    # axhline 会让自动缩放把纵轴再抬一截；显式钉回去，留一点点放标注的余量。
    ax.set_ylim(ax.get_ylim()[0], max(top, NIC_CEILING_MBPS * 1.12))
    return True


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
    # 六档比两档多三倍的柱子，宽度不跟着长，柱子会细到看不出纹理。
    width_in = 5.6 if len(vsizes) <= 3 else 7.6
    fig, axes = plt.subplots(1, 2, figsize=(width_in, 2.9))
    grouped(axes[0], cell, op, "mb_per_s", vsizes, fmt="{:.1f}")
    axes[0].set_ylabel("throughput (MB/s)")
    axes[0].set_title("Throughput")
    headroom(axes[0])
    # 写吞吐这一侧标出链路上限，理由见 NIC_CEILING_MBPS 那段。
    if op == "PUT":
        put_ceiling(axes[0])

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
    width_in = 7.4 if len(vsizes) <= 3 else 10.2
    fig, axes = plt.subplots(1, 3, figsize=(width_in, 2.8))
    for ax, (op, title) in zip(axes, [("PUT", "Write"),
                                      ("GET", "Point read"),
                                      ("SCAN", "Range scan")]):
        grouped(ax, cell, op, "mb_per_s", vsizes, fmt="{:.1f}")
        ax.set_ylabel("throughput (MB/s)" if ax is axes[0] else "")
        ax.set_title(title)
        headroom(ax)
        if op == "PUT":
            put_ceiling(ax)
    fig.tight_layout()
    toplegend(fig, axes[0])
    save(fig, outdir, "fig5-throughput")


def fig_relative(cell, vsizes, outdir):
    """三个操作的吞吐，**归一化到 Original**。

    为什么要有这一张：六档合并之后绝对吞吐跨 400 倍，fig5 只能上对数轴，
    而对数轴把四个系统之间 10~40% 的差异压成了几乎看不见的高度差——
    那恰恰是这张图存在的理由。归一化之后纵轴回到线性的 0.5~1.5，
    对比一眼可见，代价是看不到绝对量级（绝对值在 fig5 与 figure-values.txt 里）。
    两张图各答一个问题，不是同一张图的两个版本。
    """
    ref = SYS[0]
    width_in = 7.4 if len(vsizes) <= 3 else 10.2
    fig, axes = plt.subplots(1, 3, figsize=(width_in, 2.8))
    width = 0.8 / len(SYS)
    for ax, (op, title) in zip(axes, [("PUT", "Write"),
                                      ("GET", "Point read"),
                                      ("SCAN", "Range scan")]):
        for i, sy in enumerate(SYS):
            xs, ys = [], []
            for j, vs in enumerate(vsizes):
                v = num(cell, sy, op, vs, "mb_per_s")
                b = num(cell, ref, op, vs, "mb_per_s")
                # 基准缺了就整组都画不了——**不能拿别的档的基准凑**，
                # 那样画出来的倍率不属于任何一次测量。
                if v is None or not b:
                    continue
                xs.append(j + (i - (len(SYS) - 1) / 2) * width)
                ys.append(v / b)
            if not xs:
                continue
            ax.bar(xs, ys, width=width * 0.92, label=LABEL[sy],
                   color=COLOR[sy], hatch=HATCH[sy],
                   edgecolor="white", linewidth=0.6, zorder=3)
        ax.axhline(1.0, color="#444444", linewidth=0.8, zorder=4)
        ax.set_xticks(range(len(vsizes)))
        ax.set_xticklabels([vlabel(v) for v in vsizes],
                           rotation=30 if len(vsizes) > 3 else 0,
                           ha="right" if len(vsizes) > 3 else "center")
        ax.set_xlabel("value size")
        ax.set_ylabel(f"throughput / {LABEL[ref]}" if ax is axes[0] else "")
        ax.set_title(title)
        ax.grid(axis="y", zorder=0)
        ax.set_axisbelow(True)
    fig.tight_layout()
    toplegend(fig, axes[0])
    save(fig, outdir, "fig6-relative")


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


def dump_table(cell, vsizes, outdir):
    """把图上画的每个数写成一张文本表。

    六档时图上不再标数（24 根柱子标不下），**数值就必须另有去处**——
    没有这张表的话，"去掉标注"等于把数据丢了，而图本身看不出这件事。
    """
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, "figure-values.txt")
    with open(path, "w") as f:
        for op, field, unit in [("PUT", "mb_per_s", "MB/s"),
                                ("PUT", "p99_ms", "ms (p99)"),
                                ("GET", "mb_per_s", "MB/s"),
                                ("GET", "p50_ms", "ms (p50)"),
                                ("SCAN", "mb_per_s", "MB/s"),
                                ("SCAN", "p50_ms", "ms (p50)")]:
            f.write(f"\n{op}  {field}  [{unit}]\n")
            # 表头要空出系统名那一列的宽度，否则每个档名都偏左 12 个字符，
            # 读者会把 64B 的数对到 256B 的表头上。
            f.write("  " + " " * 12
                    + "".join(f"{vlabel(v):>12}" for v in vsizes) + "\n")
            for sy in SYS:
                row = "".join(
                    f"{num(cell, sy, op, v, field):>12.2f}"
                    if num(cell, sy, op, v, field) is not None else f"{'-':>12}"
                    for v in vsizes)
                f.write(f"  {LABEL[sy]:<12}{row}\n")
    print(path)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    *csvpaths, outdir = sys.argv[1:]
    cell = load(*csvpaths)
    vsizes = sorted({vs for (_, _, vs) in cell}, key=int)
    missing = [s for s in SYS if not any(k[0] == s for k in cell)]
    if missing:
        print(f"注意: CSV 里缺这些系统: {', '.join(missing)}", file=sys.stderr)

    fig_scan(cell, vsizes, outdir)
    fig_put(cell, vsizes, outdir)
    fig_get(cell, vsizes, outdir)
    fig_throughput(cell, vsizes, outdir)
    fig_relative(cell, vsizes, outdir)
    dump_table(cell, vsizes, outdir)

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
