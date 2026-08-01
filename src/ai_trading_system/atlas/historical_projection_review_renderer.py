# ruff: noqa: E501
"""Static renderer for the Atlas historical projection owner-review pack."""

from __future__ import annotations

from collections.abc import Mapping
from html import escape

from .historical_projection_review import HistoricalProjectionReviewPack

_COUNT_LABELS = {
    "sources": "来源",
    "nodes": "流程节点",
    "edges": "连接关系",
    "results": "结果卡",
    "attributions": "归因记录",
}


def render_historical_projection_review_html(
    review_pack: HistoricalProjectionReviewPack,
) -> str:
    """Render a self-contained, accessible HTML review page."""

    count_cards = "".join(
        _render_count_card(
            label=_COUNT_LABELS[name],
            current=review_pack.current_counts[name],
            candidate=review_pack.candidate_counts[name],
        )
        for name in _COUNT_LABELS
    )
    historical_nodes = "".join(
        (
            '<div class="history-node" data-node-id="{node}">'
            '<span class="node-state limited">待审阅</span><strong>{title}</strong>'
            "<small>{kind}</small></div>"
        ).format(
            node=escape(str(item["candidate_node_id"])),
            title=escape(str(item["title"])),
            kind=escape(str(item["candidate_node_kind"])),
        )
        for item in review_pack.records
    )
    record_cards = "".join(_render_record(item) for item in review_pack.records)
    page = review_pack.canonical_page_receipt
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Atlas 历史投影审阅包 V1</title>
  <style>
    :root {{
      color-scheme: light;
      --ink:#172033; --muted:#5b6475; --line:#d8deea; --surface:#ffffff;
      --canvas:#f4f7fb; --navy:#17345f; --blue:#2f6feb; --blue-soft:#eaf2ff;
      --amber:#a76306; --amber-soft:#fff3d7; --green:#1b7f52; --green-soft:#e4f6ec;
      --red:#b23b3b; --red-soft:#fdeaea; --purple:#7357c7; --purple-soft:#f0ebff;
      --shadow:0 16px 38px rgba(35,52,85,.09); --radius:18px;
    }}
    * {{ box-sizing:border-box; }}
    html {{ scroll-behavior:smooth; }}
    body {{ margin:0; background:var(--canvas); color:var(--ink); font-family:Inter,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; line-height:1.6; }}
    a {{ color:var(--blue); }}
    code {{ font-family:"Cascadia Code",Consolas,monospace; overflow-wrap:anywhere; }}
    .page {{ width:min(1180px,calc(100% - 32px)); margin:0 auto; padding:28px 0 64px; }}
    .hero {{ position:relative; overflow:hidden; padding:34px; border-radius:24px; color:#fff; background:linear-gradient(135deg,#142a4c,#2356a0 68%,#5575d8); box-shadow:var(--shadow); }}
    .hero::after {{ content:""; position:absolute; width:280px; height:280px; border-radius:50%; background:rgba(255,255,255,.09); right:-70px; top:-120px; }}
    .eyebrow {{ margin:0 0 10px; letter-spacing:.12em; font-size:.76rem; font-weight:800; text-transform:uppercase; opacity:.86; }}
    h1 {{ margin:0; max-width:800px; font-size:clamp(2rem,5vw,3.45rem); line-height:1.12; }}
    .hero-copy {{ max-width:820px; margin:18px 0 0; font-size:1.05rem; color:#e8efff; }}
    .boundary {{ display:flex; gap:10px; align-items:flex-start; margin-top:24px; padding:14px 16px; border:1px solid rgba(255,255,255,.28); border-radius:14px; background:rgba(9,23,49,.3); }}
    .boundary strong {{ white-space:nowrap; color:#ffe7a6; }}
    .meta-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; margin-top:22px; }}
    .meta {{ padding:12px 14px; border-radius:12px; background:rgba(255,255,255,.11); }}
    .meta small {{ display:block; color:#cbd9f8; }}
    .meta code {{ display:block; margin-top:3px; font-size:.8rem; }}
    section {{ margin-top:26px; padding:28px; border:1px solid var(--line); border-radius:var(--radius); background:var(--surface); box-shadow:0 8px 24px rgba(35,52,85,.05); }}
    .section-head {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-end; margin-bottom:20px; }}
    .section-head h2 {{ margin:0; font-size:1.45rem; }}
    .section-head p {{ margin:0; max-width:720px; color:var(--muted); }}
    .count-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:12px; }}
    .count-card {{ padding:16px; border:1px solid var(--line); border-radius:14px; background:#fbfcff; }}
    .count-card small {{ color:var(--muted); font-weight:700; }}
    .count-line {{ display:flex; gap:9px; align-items:baseline; margin-top:8px; }}
    .count-line .count-current {{ font-size:1.65rem; font-weight:800; color:var(--navy); }}
    .count-line .arrow {{ color:#97a0b2; }}
    .count-line .candidate {{ font-size:1.65rem; font-weight:800; color:var(--purple); }}
    .delta {{ display:block; margin-top:2px; color:var(--purple); font-size:.78rem; font-weight:800; }}
    .flow-shell {{ padding:18px; border:1px solid var(--line); border-radius:16px; background:linear-gradient(#fbfdff,#f7f9fd); overflow:hidden; }}
    .flow-label {{ margin:0 0 10px; color:var(--muted); font-size:.82rem; font-weight:800; letter-spacing:.06em; text-transform:uppercase; }}
    .main-flow {{ display:grid; grid-template-columns:repeat(7,minmax(0,1fr)); gap:16px; align-items:stretch; }}
    .flow-node {{ position:relative; min-height:82px; display:flex; flex-direction:column; justify-content:center; padding:12px; border-radius:13px; border:1px solid #cbd7eb; background:#fff; text-align:center; }}
    .flow-node:not(:last-child)::after {{ content:"→"; position:absolute; right:-14px; top:30%; color:#8090a8; font-weight:900; }}
    .flow-node strong {{ font-size:.88rem; }}
    .flow-node small {{ color:var(--muted); }}
    .flow-node.current {{ border:2px solid var(--blue); background:var(--blue-soft); box-shadow:0 0 0 4px rgba(47,111,235,.08); }}
    .you-are-here {{ display:inline-block; align-self:center; margin-bottom:5px; padding:2px 8px; border-radius:99px; color:#fff; background:var(--blue); font-size:.7rem; font-weight:800; }}
    .branch-connector {{ width:2px; height:30px; margin:0 auto; background:repeating-linear-gradient(to bottom,var(--purple),var(--purple) 5px,transparent 5px,transparent 9px); }}
    .history-lane {{ padding:18px; border:1px dashed #aa96e5; border-radius:15px; background:var(--purple-soft); }}
    .history-head {{ display:flex; justify-content:space-between; gap:16px; align-items:center; margin-bottom:14px; }}
    .history-head strong {{ color:#4d348e; }}
    .review-chip {{ padding:5px 10px; border-radius:99px; background:#fff; color:#6246aa; font-size:.76rem; font-weight:800; }}
    .history-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; }}
    .history-node {{ min-height:112px; padding:12px; border-radius:12px; background:#fff; border:1px solid #cdbff2; }}
    .history-node strong,.history-node small {{ display:block; }}
    .history-node strong {{ margin-top:8px; font-size:.86rem; }}
    .history-node small {{ margin-top:4px; color:var(--muted); }}
    .node-state {{ display:inline-block; padding:2px 7px; border-radius:99px; font-size:.7rem; font-weight:800; }}
    .limited {{ color:var(--amber); background:var(--amber-soft); }}
    .legend {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:16px; }}
    .legend span {{ padding:5px 9px; border:1px solid var(--line); border-radius:99px; background:#fff; color:var(--muted); font-size:.76rem; }}
    .cards {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:16px; }}
    .record {{ display:flex; flex-direction:column; min-width:0; border:1px solid var(--line); border-radius:16px; overflow:hidden; background:#fff; }}
    .record-head {{ padding:18px; background:#f8faff; border-bottom:1px solid var(--line); }}
    .record-kicker {{ display:flex; justify-content:space-between; gap:10px; align-items:center; }}
    .role {{ color:var(--blue); font-size:.74rem; font-weight:900; letter-spacing:.06em; }}
    .status-pill {{ padding:4px 9px; border-radius:99px; color:var(--amber); background:var(--amber-soft); font-size:.74rem; font-weight:900; }}
    .record h3 {{ margin:9px 0 5px; font-size:1.15rem; }}
    .record-head p {{ margin:0; color:var(--muted); }}
    .record-body {{ display:grid; gap:14px; padding:18px; }}
    .status-map {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; }}
    .status-box {{ padding:10px; border-radius:10px; background:#f5f7fb; }}
    .status-box small,.detail small {{ display:block; color:var(--muted); font-size:.72rem; }}
    .status-box code {{ display:block; margin-top:3px; font-size:.73rem; font-weight:800; }}
    .status-box.proposed {{ background:var(--green-soft); color:var(--green); }}
    .status-box.display {{ background:var(--amber-soft); color:var(--amber); }}
    .rationale {{ margin:0; padding:12px; border-left:3px solid var(--amber); background:#fffaf0; color:#65481c; }}
    .detail-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:10px; }}
    .detail {{ padding:10px; border:1px solid var(--line); border-radius:10px; min-width:0; }}
    .detail code {{ display:block; margin-top:3px; font-size:.75rem; }}
    .window-list,.limits {{ margin:5px 0 0; padding-left:18px; color:var(--muted); }}
    .window-list li,.limits li {{ margin:4px 0; }}
    .ids {{ padding:11px; border-radius:10px; background:#172033; color:#dce6ff; }}
    .ids small {{ color:#aebedc; }}
    .ids code {{ display:block; margin-top:3px; font-size:.72rem; }}
    .wide {{ grid-column:1/-1; }}
    .checklist {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; padding:0; margin:0; list-style:none; }}
    .checklist li {{ position:relative; padding:13px 13px 13px 42px; border:1px solid var(--line); border-radius:12px; background:#fbfcff; }}
    .checklist li::before {{ content:"✓"; position:absolute; left:14px; top:12px; width:20px; height:20px; display:grid; place-items:center; border:2px solid #8795ac; border-radius:5px; color:transparent; }}
    .receipt {{ display:grid; grid-template-columns:1fr auto; gap:14px; padding:15px; border:1px solid var(--line); border-radius:12px; background:#f8faff; }}
    .receipt p {{ margin:0; }}
    .receipt .hash {{ max-width:520px; text-align:right; font-size:.77rem; }}
    .safety {{ margin-top:16px; padding:15px; border-radius:12px; background:var(--red-soft); color:#702c2c; }}
    footer {{ padding:24px 4px 0; color:var(--muted); font-size:.82rem; text-align:center; }}
    @media (max-width:920px) {{
      .count-grid {{ grid-template-columns:repeat(3,minmax(0,1fr)); }}
      .main-flow {{ grid-template-columns:repeat(4,minmax(0,1fr)); }}
      .flow-node::after {{ display:none; }}
      .history-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .meta-grid {{ grid-template-columns:1fr; }}
    }}
    @media (max-width:680px) {{
      .page {{ width:min(100% - 20px,1180px); padding-top:10px; }}
      .hero,section {{ padding:20px; border-radius:16px; }}
      .section-head,.history-head,.receipt {{ display:block; }}
      .section-head p {{ margin-top:7px; }}
      .count-grid,.main-flow,.history-grid,.cards,.checklist {{ grid-template-columns:1fr; }}
      .status-map,.detail-grid {{ grid-template-columns:1fr; }}
      .receipt .hash {{ margin-top:8px; text-align:left; }}
    }}
  </style>
</head>
<body>
<main class="page">
  <header class="hero">
    <p class="eyebrow">Atlas · Historical Projection Review Pack V1</p>
    <h1>历史研究应该放在哪里，先让你一眼看懂</h1>
    <p class="hero-copy">这份独立页面把五份历史研究材料映射到一条隔离的候选支线，供你审阅位置、状态和限制语言。它不会改变当前策略研究结果页。</p>
    <div class="boundary"><strong>重要边界</strong><span>这是 review-only preview。当前 Atlas snapshot、结果卡和已验收 cited-query 页面均未投影、未重建、未修改。</span></div>
    <div class="meta-grid">
      <div class="meta"><small>Review pack</small><code>{escape(review_pack.review_pack_id)}</code></div>
      <div class="meta"><small>Current snapshot</small><code>{escape(review_pack.current_snapshot_id)}</code></div>
      <div class="meta"><small>Evidence exact commit</small><code>{escape(review_pack.evidence_exact_commit)}</code></div>
    </div>
  </header>

  <section aria-labelledby="counts-title">
    <div class="section-head"><div><h2 id="counts-title">如果后续批准，结构会怎样变化</h2><p>来源数量不变，只把已经注册的五份历史来源组织成候选节点、结果卡与中性 provenance。</p></div></div>
    <div class="count-grid">{count_cards}</div>
  </section>

  <section aria-labelledby="flow-title">
    <div class="section-head"><div><h2 id="flow-title">整个策略系统中的位置</h2><p>蓝色节点是当前实际关注位置；紫色虚线区只是待审阅历史支线。卡片顺序是阅读顺序，不代表因果或研究时序。</p></div></div>
    <div class="flow-shell">
      <p class="flow-label">当前主线</p>
      <div class="main-flow" role="list" aria-label="当前策略系统主线">
        <div class="flow-node" role="listitem"><strong>数据输入</strong><small>market / macro</small></div>
        <div class="flow-node" role="listitem"><strong>数据质量</strong><small>validate-data</small></div>
        <div class="flow-node" role="listitem"><strong>特征与标签</strong><small>research inputs</small></div>
        <div class="flow-node" role="listitem"><strong>候选策略</strong><small>hypothesis</small></div>
        <div class="flow-node" role="listitem"><strong>回测与门禁</strong><small>evidence</small></div>
        <div class="flow-node" role="listitem"><strong>结果与归因</strong><small>ledger</small></div>
        <div class="flow-node current" role="listitem"><span class="you-are-here">当前关注</span><strong>Citation-first 页面</strong><small>current canonical</small></div>
      </div>
      <div class="branch-connector" aria-hidden="true"></div>
      <div class="history-lane" aria-label="待审阅历史投影支线">
        <div class="history-head"><strong>候选：历史权重研究支线</strong><span class="review-chip">未投影 · review only</span></div>
        <div class="history-grid">{historical_nodes}</div>
      </div>
      <div class="legend" aria-label="状态图例">
        <span>蓝色：当前页面关注点</span><span>紫色：候选历史支线</span><span>黄色：读者展示 LIMITED</span><span>原始状态 ≠ 当前投资评级</span>
      </div>
    </div>
  </section>

  <section aria-labelledby="records-title">
    <div class="section-head"><div><h2 id="records-title">五份历史材料逐项看</h2><p>每张卡都把来源原始状态、候选 Atlas raw status 和普通读者 display status 分开，并保留 DQ、窗口和限制。</p></div></div>
    <div class="cards">{record_cards}</div>
  </section>

  <section aria-labelledby="review-title">
    <div class="section-head"><div><h2 id="review-title">请按这六点验收</h2><p>通过本页只意味着审阅包可用；是否真正写入 canonical Atlas，需要下一份独立 Owner 决策。</p></div></div>
    <ul class="checklist">
      <li>历史支线在整套策略系统中的位置是否清晰？</li>
      <li>五张卡的阅读顺序和信息密度是否合适？</li>
      <li>original / proposed raw / display 三层状态是否避免误读？</li>
      <li>六节点、六条 CONTAINS、五结果、五条中性归因是否合理？</li>
      <li>历史窗口、DQ 缺口和 limitations 是否足够醒目？</li>
      <li>是否值得另立 canonical projection 任务？</li>
    </ul>
    <div class="receipt">
      <p><strong>已验收 canonical 页面 identity</strong><br><code>{escape(str(page["repository_path"]))}</code><br>{page["size_bytes"]} bytes · 本任务构建前后保持不变</p>
      <code class="hash">SHA-256<br>{escape(str(page["sha256"]))}</code>
    </div>
    <div class="safety"><strong>未执行：</strong>node projection、result projection、page projection、investment conclusion、production effect、broker action。</div>
  </section>
  <footer>Primary research default：2021-02-22 · historical 2022-12-01 只保留为旧窗口语义 · attribution direction：NEUTRAL</footer>
</main>
</body>
</html>
"""


def _render_count_card(*, label: str, current: int, candidate: int) -> str:
    delta = candidate - current
    return (
        '<div class="count-card"><small>{label}</small><div class="count-line">'
        '<span class="count-current">{current}</span><span class="arrow">→</span>'
        '<span class="candidate">{candidate}</span></div>'
        '<span class="delta">{delta}</span></div>'
    ).format(
        label=escape(label),
        current=current,
        candidate=candidate,
        delta=f"+{delta}" if delta > 0 else "不变",
    )


def _render_record(item: Mapping[str, object]) -> str:
    windows = _mapping_sequence(item.get("windows"))
    window_items = "".join(f"<li>{_render_window(window)}</li>" for window in windows)
    if not window_items:
        window_items = "<li>未提供窗口记录</li>"
    limitations = _text_sequence(item.get("limitations"))
    limit_items = "".join(f"<li>{escape(value)}</li>" for value in limitations)
    dq = item.get("data_quality")
    dq_detail = escape(str(item["data_quality_label"]))
    if isinstance(dq, Mapping):
        dq_detail += (
            f" · errors={escape(str(dq.get('error_count')))}"
            f" · warnings={escape(str(dq.get('warning_count')))}"
        )
    return f"""
<article class="record" data-source-ref="{escape(str(item["source_ref_id"]))}" data-display-status="{escape(str(item["proposed_display_status"]))}">
  <div class="record-head">
    <div class="record-kicker"><span class="role">{escape(str(item["role_code"]))}</span><span class="status-pill">DISPLAY · LIMITED</span></div>
    <h3>{escape(str(item["title"]))}</h3><p>{escape(str(item["reader_summary"]))}</p>
  </div>
  <div class="record-body">
    <div class="status-map" aria-label="三层状态映射">
      <div class="status-box"><small>来源 original raw</small><code>{escape(str(item["original_raw_status"]))}</code></div>
      <div class="status-box proposed"><small>候选 Atlas raw</small><code>{escape(str(item["proposed_raw_status"]))}</code></div>
      <div class="status-box display"><small>读者 display</small><code>{escape(str(item["proposed_display_status"]))}</code></div>
    </div>
    <p class="rationale"><strong>为什么这样映射：</strong>{escape(str(item["mapping_rationale"]))}</p>
    <div class="detail-grid">
      <div class="detail"><small>数据质量</small><strong>{dq_detail}</strong></div>
      <div class="detail"><small>来源时间</small><strong>{escape(str(item["as_of"]))}</strong></div>
      <div class="detail wide"><small>requested / evaluated 窗口</small><ul class="window-list">{window_items}</ul></div>
      <div class="detail wide"><small>来源 key result</small><code>{escape(str(item["key_result"]))}</code></div>
      <div class="detail wide"><small>关键限制</small><ul class="limits">{limit_items}</ul></div>
    </div>
    <div class="ids"><small>候选 stable IDs</small><code>node · {escape(str(item["candidate_node_id"]))}</code><code>result · {escape(str(item["candidate_result_id"]))}</code><code>attribution · {escape(str(item["candidate_attribution_id"]))} · NEUTRAL</code><code>source · {escape(str(item["source_ref_id"]))}</code></div>
  </div>
</article>"""


def _render_window(window: Mapping[str, object]) -> str:
    requested = _range(window.get("requested_start"), window.get("requested_end"))
    evaluated = _range(window.get("evaluated_start"), window.get("evaluated_end"))
    return (
        f"<code>{escape(str(window.get('window_id')))}</code> · "
        f"requested {escape(requested)} · evaluated {escape(evaluated)}"
    )


def _range(start: object, end: object) -> str:
    if start is None and end is None:
        return "未提供"
    return f"{start or '未提供'} → {end or '未提供'}"


def _mapping_sequence(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _text_sequence(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(str(item) for item in value)


__all__ = ["render_historical_projection_review_html"]
