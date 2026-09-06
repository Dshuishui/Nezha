package kvstore

// 可视化页面。内联在二进制里、不引用任何外部资源——实验机器通常没有出站网络，
// 从 CDN 取 JS 只会得到一个空白页。
const vizPage = `<!doctype html>
<meta charset="utf-8">
<title>AVP Placement</title>
<style>
  :root {
    --bg: #fbfaf7; --fg: #1c1a17; --muted: #7a736a; --line: #e3ded5;
    --inline: #2f6f4f; --external: #b8792f; --accent: #8c3b2e;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #171614; --fg: #ece8e1; --muted: #9a938a; --line: #2f2c28;
      --inline: #6fbf95; --external: #e0a763; --accent: #d97b68;
    }
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; padding: 32px; background: var(--bg); color: var(--fg);
    font: 14px/1.6 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  h1 { font-size: 19px; font-weight: 600; margin: 0 0 4px; letter-spacing: -.01em; }
  .sub { color: var(--muted); font-size: 13px; margin-bottom: 28px; }
  .sub b { color: var(--fg); font-weight: 600; }
  .cards { display: flex; flex-wrap: wrap; gap: 28px 40px; margin-bottom: 32px; }
  .card .k { color: var(--muted); font-size: 12px; letter-spacing: .04em; text-transform: uppercase; }
  .card .v { font-size: 26px; font-weight: 600; font-variant-numeric: tabular-nums; }
  .card .v small { font-size: 14px; font-weight: 400; color: var(--muted); }
  table { border-collapse: collapse; width: 100%; max-width: 860px; }
  th, td { text-align: right; padding: 7px 12px; border-bottom: 1px solid var(--line); font-variant-numeric: tabular-nums; }
  th:first-child, td:first-child { text-align: left; }
  th { color: var(--muted); font-weight: 500; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
  tr.thresholdRow td { border-bottom: 2px solid var(--accent); }
  .bar { display: flex; height: 9px; border-radius: 5px; overflow: hidden; background: var(--line); min-width: 150px; }
  .bar i { display: block; }
  .bar .in { background: var(--inline); }
  .bar .ex { background: var(--external); }
  .legend { display: flex; gap: 20px; color: var(--muted); font-size: 12px; margin: 14px 0 6px; }
  .legend i { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 6px; vertical-align: middle; }
  .note { color: var(--muted); font-size: 12px; margin-top: 18px; max-width: 620px; }
</style>
<h1>AVP value placement</h1>
<div class="sub">system <b id="sys">—</b> · inlineThreshold <b id="thr">—</b> · 每秒刷新</div>

<div class="cards">
  <div class="card"><div class="k">内联命中率</div><div class="v"><span id="hr">—</span><small>%</small></div></div>
  <div class="card"><div class="k">有效命中率</div><div class="v"><span id="ehr">—</span><small>%</small></div></div>
  <div class="card"><div class="k">读请求</div><div class="v" id="reads">—</div></div>
  <div class="card"><div class="k">未命中每次解析</div><div class="v"><span id="eps">—</span><small> 条</small></div></div>
</div>

<div class="legend">
  <span><i style="background:var(--inline)"></i>内联进存储引擎</span>
  <span><i style="background:var(--external)"></i>留在 valuelog</span>
  <span><i style="background:var(--accent)"></i>阈值位置</span>
</div>
<table>
  <thead><tr><th>value 大小</th><th>内联</th><th>valuelog</th><th style="width:40%">占比</th></tr></thead>
  <tbody id="rows"></tbody>
</table>

<p class="note">
  未命中每次解析的条数反映稀疏索引的块内扫描量：它是块大小与 entry 大小的函数，
  与命中率无关。命中率会随读的键空间漂移，有效命中率剔除了本就不存在的 key。
</p>

<script>
const fmt = n => n.toLocaleString();
async function tick() {
  let d;
  try { d = await (await fetch('/api/stats')).json(); } catch (e) { return; }
  sys.textContent = d.system;
  thr.textContent = d.inlineThreshold + 'B';
  hr.textContent = d.hitRate.toFixed(1);
  ehr.textContent = d.effHitRate.toFixed(1);
  reads.textContent = fmt(d.reads);
  eps.textContent = d.entriesPerScan.toFixed(1);

  rows.innerHTML = d.buckets.map(b => {
    const t = b.inlined + b.external;
    const pi = t ? b.inlined / t * 100 : 0;
    // 阈值落在哪一格：该格的上界刚好越过 inlineThreshold
    const m = b.label.match(/(\d+)B$/);
    const isThr = m && +m[1] === d.inlineThreshold;
    return '<tr class="' + (isThr ? 'thresholdRow' : '') + '">' +
      '<td>' + b.label + '</td>' +
      '<td>' + fmt(b.inlined) + '</td>' +
      '<td>' + fmt(b.external) + '</td>' +
      '<td><span class="bar">' +
        '<i class="in" style="width:' + pi + '%"></i>' +
        '<i class="ex" style="width:' + (100 - pi) + '%"></i>' +
      '</span></td></tr>';
  }).join('');
}
tick(); setInterval(tick, 1000);
</script>
`
