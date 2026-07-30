from __future__ import annotations

# HTML/CSS source lines remain readable as one semantic declaration.
# ruff: noqa: E501
from dataclasses import dataclass
from html import escape
from pathlib import Path

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.strategy_research_explorer import (
    AssertionKind,
    AttributionDirection,
    ResearchAttribution,
    ResearchNodeKind,
    ResearchPathNode,
    ResearchResultCard,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic

from .snapshot_builder import AtlasExplorerBundle
from .validation import validate_atlas_bundle, validation_json_bytes

_ASSERTION_LABELS = {
    AssertionKind.DATA_FACT: "数据事实",
    AssertionKind.RULE_JUDGMENT: "规则判断",
    AssertionKind.MODEL_RESULT: "模型结果",
    AssertionKind.RESEARCHER_INTERPRETATION: "研究者解释",
    AssertionKind.OWNER_DECISION: "Owner 决策",
}
_STATUS_LABELS = {
    CanonicalStatus.NOT_DUE: "尚未到期",
    CanonicalStatus.DUE: "待处理",
    CanonicalStatus.RUNNING: "进行中",
    CanonicalStatus.PASS: "已通过",
    CanonicalStatus.LIMITED: "证据有限",
    CanonicalStatus.SKIPPED: "已跳过",
    CanonicalStatus.BLOCKED: "已阻断",
    CanonicalStatus.FAILED: "未通过",
}
_DIRECTION_LABELS = {
    AttributionDirection.SUPPORTS: "支持",
    AttributionDirection.CONTRADICTS: "反对 / 阻断",
    AttributionDirection.MIXED: "影响混合",
    AttributionDirection.NEUTRAL: "中性",
    AttributionDirection.UNKNOWN: "未知",
}


@dataclass(frozen=True)
class AtlasRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


def render_atlas_html(bundle: AtlasExplorerBundle) -> str:
    validation = validate_atlas_bundle(bundle)
    if validation.status != "PASS":
        raise ValueError("ATLAS_RENDER_VALIDATION_FAILED:" + ",".join(validation.errors))
    snapshot = bundle.snapshot
    attribution_map: dict[str, list[ResearchAttribution]] = {}
    for item in snapshot.attributions:
        attribution_map.setdefault(item.result_id, []).append(item)
    source_map = {item.source_ref_id: item for item in snapshot.sources}
    campaigns = tuple(
        item for item in snapshot.nodes if item.node_kind is ResearchNodeKind.CAMPAIGN
    )
    attention_count = sum(
        item.display_status
        in (
            CanonicalStatus.LIMITED,
            CanonicalStatus.BLOCKED,
            CanonicalStatus.NOT_DUE,
            CanonicalStatus.DUE,
        )
        for item in snapshot.results
    )
    campaign_cards = "\n".join(_render_campaign(item) for item in campaigns)
    node_cards = "\n".join(
        _render_node(index, node) for index, node in enumerate(snapshot.nodes, start=1)
    )
    edge_rows = "\n".join(
        "<tr>"
        f"<td>{escape(item.from_node_id)}</td>"
        f'<td><span class="edge-kind">{escape(item.edge_kind.value)}</span>'
        f"<br>{escape(item.label)}</td>"
        f"<td>{escape(item.to_node_id)}</td>"
        "</tr>"
        for item in snapshot.edges
    )
    result_cards = "\n".join(
        _render_result(
            item,
            attribution_map.get(item.result_id, []),
            source_map,
        )
        for item in snapshot.results
    )
    source_rows = "\n".join(
        "<tr>"
        f"<td><strong>{escape(item.source_ref_id)}</strong>"
        f'<br><span class="muted">{escape(item.artifact_identity)}</span></td>'
        f"<td><code>{escape(item.source_path)}</code></td>"
        f"<td><code>{escape(item.content_sha256[:12])}…</code>"
        f'<br><span class="muted">commit {escape(item.exact_commit[:10])}…</span></td>'
        f"<td>{_yes_no(item.research_context_complete)}</td>"
        f"<td>{_yes_no(item.data_quality_ready)}</td>"
        f"<td>{escape(item.limitation or '无额外限制')}</td>"
        "</tr>"
        for item in snapshot.sources
    )
    glossary_cards = "\n".join(
        '<div class="glossary-card">'
        f"<dt>{escape(item.term)}</dt>"
        f"<dd>{escape(item.plain_language)}</dd>"
        "</div>"
        for item in bundle.glossary
    )
    assertion_legend = "\n".join(
        f'<li><span class="assertion assertion-{kind.value.lower()}">{escape(label)}</span></li>'
        for kind, label in _ASSERTION_LABELS.items()
    )
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light">
  <title>{escape(snapshot.title)}</title>
  <style>
    :root {{
      --ink: #14213d;
      --muted: #60708f;
      --paper: #f4f7fb;
      --surface: #ffffff;
      --line: #d9e1ee;
      --navy: #173b67;
      --blue: #2d6cdf;
      --teal: #087f72;
      --amber: #a35b00;
      --red: #b42318;
      --purple: #6f42c1;
      --shadow: 0 10px 28px rgba(20, 33, 61, .08);
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      color: var(--ink);
      background: var(--paper);
      font-family: Inter, "Noto Sans SC", "Microsoft YaHei", system-ui, sans-serif;
      line-height: 1.6;
    }}
    a {{ color: var(--blue); }}
    a:focus-visible {{ outline: 3px solid #ffbf47; outline-offset: 3px; }}
    code {{ font-family: "Cascadia Code", Consolas, monospace; font-size: .84em; }}
    .skip-link {{
      position: absolute; left: -999px; top: 1rem; z-index: 10;
      background: #fff; padding: .65rem 1rem; border-radius: .5rem;
    }}
    .skip-link:focus {{ left: 1rem; }}
    .hero {{
      color: white;
      background:
        radial-gradient(circle at 88% 12%, rgba(61, 190, 174, .28), transparent 28rem),
        linear-gradient(125deg, #102a4c 0%, #173b67 62%, #205e78 100%);
      padding: 3.8rem max(1.25rem, calc((100vw - 1180px) / 2));
    }}
    .eyebrow {{ margin: 0 0 .65rem; color: #8de2d7; font-weight: 750; letter-spacing: .08em; }}
    h1 {{ margin: 0; max-width: 880px; font-size: clamp(2rem, 5vw, 4rem); line-height: 1.12; }}
    .hero-copy {{ max-width: 820px; color: #dce9f7; font-size: 1.08rem; }}
    .notice {{
      max-width: 880px; margin-top: 1.35rem; padding: .85rem 1rem;
      border: 1px solid rgba(255,255,255,.28); border-radius: .8rem;
      background: rgba(8, 25, 48, .36);
    }}
    .metrics {{
      display: grid; grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: .8rem; margin-top: 2rem; max-width: 900px;
    }}
    .metric {{ padding: .8rem 1rem; border-left: 3px solid #6ad6c8; background: rgba(255,255,255,.08); }}
    .metric strong {{ display: block; font-size: 1.5rem; }}
    .metric span {{ color: #dce9f7; font-size: .88rem; }}
    .layout {{
      display: grid; grid-template-columns: 230px minmax(0, 1fr);
      gap: 2rem; max-width: 1240px; margin: 0 auto; padding: 2rem 1.25rem 4rem;
    }}
    nav {{
      position: sticky; top: 1rem; align-self: start;
      border: 1px solid var(--line); border-radius: .9rem; background: var(--surface);
      box-shadow: var(--shadow); padding: 1rem;
    }}
    nav strong {{ display: block; margin-bottom: .4rem; }}
    nav a {{ display: block; padding: .45rem .5rem; border-radius: .4rem; text-decoration: none; }}
    nav a:hover {{ background: #edf4ff; }}
    main {{ min-width: 0; }}
    section {{ scroll-margin-top: 1rem; margin-bottom: 2.3rem; }}
    h2 {{ margin: 0 0 .5rem; font-size: 1.65rem; }}
    .section-intro {{ color: var(--muted); margin-top: 0; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: .5rem; padding: 0; list-style: none; }}
    .assertion, .status {{
      display: inline-flex; align-items: center; width: fit-content;
      border-radius: 999px; padding: .18rem .6rem; font-size: .77rem; font-weight: 750;
    }}
    .assertion-data_fact {{ color: #075f55; background: #ddf5f0; }}
    .assertion-rule_judgment {{ color: #744100; background: #fff0d2; }}
    .assertion-model_result {{ color: #174a92; background: #e3edff; }}
    .assertion-researcher_interpretation {{ color: #5b3791; background: #efe7fb; }}
    .assertion-owner_decision {{ color: #8c2f5e; background: #f9e3ef; }}
    .status-pass {{ color: #096b5d; background: #ddf5f0; }}
    .status-blocked, .status-failed {{ color: var(--red); background: #fee4e2; }}
    .status-not_due, .status-due {{ color: #744100; background: #fff0d2; }}
    .status-limited, .status-skipped {{ color: #5b3791; background: #efe7fb; }}
    .status-running {{ color: #174a92; background: #e3edff; }}
    .path {{
      position: relative; display: grid; gap: .85rem; margin-top: 1.2rem;
    }}
    .campaign-grid {{
      display: grid; grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 1rem; margin-top: 1.2rem;
    }}
    .campaign-card {{
      border: 1px solid var(--line); border-top: 4px solid var(--blue);
      border-radius: .85rem; background: var(--surface); box-shadow: var(--shadow);
      padding: 1rem 1.1rem;
    }}
    .campaign-card h3 {{ margin: 0 0 .35rem; font-size: 1.06rem; }}
    .campaign-card p {{ margin: .35rem 0 .8rem; color: #475673; }}
    .campaign-meta {{ display: flex; flex-wrap: wrap; gap: .45rem; align-items: center; }}
    .path::before {{
      content: ""; position: absolute; left: 1.27rem; top: 1.5rem; bottom: 1.5rem;
      width: 2px; background: #b9c8dc;
    }}
    .node {{
      position: relative; display: grid; grid-template-columns: 2.55rem 1fr;
      gap: .9rem; padding: 1rem; border: 1px solid var(--line);
      border-radius: .85rem; background: var(--surface); box-shadow: var(--shadow);
    }}
    .node-index {{
      z-index: 1; display: grid; place-items: center; width: 2.55rem; height: 2.55rem;
      border-radius: 50%; color: white; background: var(--navy); font-weight: 800;
    }}
    .node h3, .result-card h3 {{ margin: 0 0 .3rem; font-size: 1.05rem; }}
    .node p {{ margin: .35rem 0; }}
    .node-meta {{ display: flex; flex-wrap: wrap; gap: .45rem; align-items: center; }}
    .result-grid {{ display: grid; gap: 1rem; }}
    .result-card {{
      border: 1px solid var(--line); border-radius: .9rem;
      background: var(--surface); box-shadow: var(--shadow); overflow: hidden;
    }}
    .result-head {{ padding: 1rem 1.1rem; border-bottom: 1px solid var(--line); }}
    .result-body {{ padding: 1rem 1.1rem; }}
    .result-statuses {{ display: flex; flex-wrap: wrap; gap: .6rem; margin-top: .6rem; }}
    .raw-status {{ color: var(--muted); font-size: .83rem; }}
    .limitations {{ margin-bottom: 0; color: #5a3b11; }}
    .attribution {{
      margin-top: .85rem; padding: .8rem .9rem; border-left: 4px solid var(--purple);
      background: #f8f5fc; border-radius: .4rem;
    }}
    .attribution strong {{ margin-right: .4rem; }}
    .source-pills {{ display: flex; flex-wrap: wrap; gap: .35rem; margin-top: .7rem; }}
    .source-pill {{ padding: .15rem .45rem; border-radius: .35rem; background: #eef2f7; font-size: .78rem; }}
    .table-wrap {{
      overflow-x: auto; border: 1px solid var(--line); border-radius: .8rem; background: white;
    }}
    table {{ width: 100%; border-collapse: collapse; font-size: .9rem; }}
    th, td {{ padding: .75rem; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: #3d4e6e; background: #f0f4f9; white-space: nowrap; }}
    tr:last-child td {{ border-bottom: 0; }}
    .edge-kind {{ color: var(--muted); font-size: .75rem; font-weight: 750; }}
    .muted {{ color: var(--muted); }}
    .glossary {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: .8rem; }}
    .glossary-card {{ border: 1px solid var(--line); border-radius: .7rem; background: white; padding: .85rem; }}
    .glossary-card dt {{ font-weight: 800; }}
    .glossary-card dd {{ margin: .25rem 0 0; color: #475673; }}
    footer {{ color: var(--muted); border-top: 1px solid var(--line); padding-top: 1rem; font-size: .85rem; }}
    @media (max-width: 850px) {{
      .metrics {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .layout {{ grid-template-columns: 1fr; }}
      nav {{ position: static; }}
      nav a {{ display: inline-block; }}
      .campaign-grid {{ grid-template-columns: 1fr; }}
      .glossary {{ grid-template-columns: 1fr; }}
    }}
    @media print {{
      body {{ background: white; }}
      nav, .skip-link {{ display: none; }}
      .layout {{ display: block; padding: 1rem; }}
      .node, .result-card {{ box-shadow: none; break-inside: avoid; }}
    }}
  </style>
</head>
<body>
  <a class="skip-link" href="#main">跳到主要内容</a>
  <header class="hero">
    <p class="eyebrow">ATLAS · STRATEGY RESEARCH EXPLORER</p>
    <h1>{escape(snapshot.title)}</h1>
    <p class="hero-copy">把复杂的策略研究拆成一条可追溯主线：研究了什么、看到了什么、为什么停止，以及下一步何时才合法。</p>
    <p class="notice"><strong>阅读边界：</strong>{escape(bundle.reader_notice)}</p>
    <div class="metrics" aria-label="页面摘要">
      <div class="metric"><strong>{len(campaigns)}</strong><span>代表性研究主线</span></div>
      <div class="metric"><strong>{len(snapshot.nodes)}</strong><span>研究路径节点</span></div>
      <div class="metric"><strong>{len(snapshot.results)}</strong><span>实际结果卡</span></div>
      <div class="metric"><strong>{attention_count}</strong><span>受限 / 阻断 / 待处理</span></div>
    </div>
  </header>
  <div class="layout">
    <nav aria-label="页内导航">
      <strong>快速定位</strong>
      <a href="#how-to-read">如何阅读</a>
      <a href="#overview">研究全景</a>
      <a href="#mainline">研究主线</a>
      <a href="#results">结果与归因</a>
      <a href="#relations">关系表</a>
      <a href="#sources">证据来源</a>
      <a href="#glossary">术语表</a>
    </nav>
    <main id="main">
      <section id="how-to-read">
        <h2>先分清“谁在说话”</h2>
        <p class="section-intro">相同一句话如果来源不同，可信边界也不同。Atlas 不把解释写成事实，也不把验证 PASS 写成策略 PASS。</p>
        <ul class="legend">{assertion_legend}</ul>
      </section>
      <section id="overview">
        <h2>研究全景</h2>
        <p class="section-intro">先看四条代表性主线的当前状态，再进入下方逐节点证据链。V1.1 不声称覆盖全部历史研究。</p>
        <div class="campaign-grid">{campaign_cards}</div>
      </section>
      <section id="mainline">
        <h2>研究主线</h2>
        <p class="section-intro">按治理顺序阅读；每一步都显示原始状态和信息类型。</p>
        <div class="path">{node_cards}</div>
      </section>
      <section id="results">
        <h2>实际结果与归因</h2>
        <p class="section-intro">“结果”说明发生了什么；“归因”说明哪些 evidence 支持、反对或限制该结果。</p>
        <div class="result-grid">{result_cards}</div>
      </section>
      <section id="relations">
        <h2>路径关系</h2>
        <p class="section-intro">这是可审计的有向关系，不是相关性或因果权重。</p>
        <div class="table-wrap">
          <table>
            <thead><tr><th>上游节点</th><th>关系</th><th>下游节点</th></tr></thead>
            <tbody>{edge_rows}</tbody>
          </table>
        </div>
      </section>
      <section id="sources">
        <h2>证据来源与限制</h2>
        <p class="section-intro">每条来源绑定 exact commit 与内容 SHA-256。DQ/context 未就绪时，不能展示为 investment-facing PASS。</p>
        <div class="table-wrap">
          <table>
            <thead><tr><th>来源</th><th>路径</th><th>身份</th><th>研究上下文</th><th>DQ</th><th>限制</th></tr></thead>
            <tbody>{source_rows}</tbody>
          </table>
        </div>
      </section>
      <section id="glossary">
        <h2>金融研究术语表</h2>
        <p class="section-intro">用通俗语言解释页面中的关键术语。</p>
        <dl class="glossary">{glossary_cards}</dl>
      </section>
      <footer>
        <p>Primary research start：<code>{escape(bundle.primary_research_start)}</code></p>
        <p>Snapshot：<code>{escape(snapshot.snapshot_id)}</code> · Registry：<code>{escape(bundle.registry_id)}</code> · 生成时间：<code>{escape(snapshot.generated_at.isoformat())}</code></p>
        <p>production_effect=<code>none</code> · broker_action=<code>none</code> · manual_review_only=<code>true</code></p>
      </footer>
    </main>
  </div>
</body>
</html>
"""


def write_atlas_artifacts(
    bundle: AtlasExplorerBundle, output_directory: Path
) -> tuple[AtlasRenderedArtifact, ...]:
    validation = validate_atlas_bundle(bundle)
    if validation.status != "PASS":
        raise ValueError("ATLAS_ARTIFACT_VALIDATION_FAILED:" + ",".join(validation.errors))
    output_directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "index.html": render_atlas_html(bundle).encode("utf-8"),
        "snapshot.json": bundle.snapshot.canonical_json_bytes(),
        "validation.json": validation_json_bytes(validation),
    }
    artifacts: list[AtlasRenderedArtifact] = []
    for name, payload in payloads.items():
        target = output_directory / name
        write_result = write_bytes_atomic(target, payload)
        artifacts.append(
            AtlasRenderedArtifact(
                path=write_result.path.as_posix(),
                sha256=write_result.sha256,
                size_bytes=write_result.size_bytes,
            )
        )
    return tuple(artifacts)


def _render_node(index: int, node: ResearchPathNode) -> str:
    return (
        '<article class="node">'
        f'<div class="node-index" aria-hidden="true">{index}</div>'
        "<div>"
        f"<h3>{escape(node.title)}</h3>"
        f"<p>{escape(node.summary)}</p>"
        '<div class="node-meta">'
        f"{_status_badge(node.raw_status)}"
        f"{_assertion_badge(node.assertion_kind)}"
        f'<span class="muted">node <code>{escape(node.node_id)}</code></span>'
        "</div></div></article>"
    )


def _render_campaign(node: ResearchPathNode) -> str:
    return (
        '<article class="campaign-card">'
        f"<h3>{escape(node.title)}</h3>"
        f"<p>{escape(node.summary)}</p>"
        '<div class="campaign-meta">'
        f"{_status_badge(node.raw_status)}"
        f"{_assertion_badge(node.assertion_kind)}"
        "</div></article>"
    )


def _render_result(
    result: ResearchResultCard,
    attributions: list[ResearchAttribution],
    source_map: dict[str, object],
) -> str:
    limitation_items = "".join(f"<li>{escape(item)}</li>" for item in result.limitations)
    attribution_items = "".join(
        '<div class="attribution">'
        f"<strong>{escape(_DIRECTION_LABELS[item.direction])}</strong>"
        f"{_assertion_badge(item.assertion_kind)}"
        f"<div>{escape(item.explanation)}</div>"
        "</div>"
        for item in sorted(attributions, key=lambda item: item.attribution_id)
    )
    source_pills = "".join(
        f'<span class="source-pill">{escape(source_ref_id)}</span>'
        for source_ref_id in result.source_ref_ids
        if source_ref_id in source_map
    )
    return (
        '<article class="result-card">'
        '<div class="result-head">'
        f"<h3>{escape(result.title)}</h3>"
        f"{_assertion_badge(result.assertion_kind)}"
        '<div class="result-statuses">'
        f"{_status_badge(result.display_status)}"
        f'<span class="raw-status">raw_status={escape(result.raw_status.value)}</span>'
        "</div></div>"
        '<div class="result-body">'
        f"<p>{escape(result.reader_summary)}</p>"
        f'<ul class="limitations">{limitation_items}</ul>'
        f"{attribution_items}"
        f'<div class="source-pills" aria-label="绑定来源">{source_pills}</div>'
        "</div></article>"
    )


def _status_badge(status: CanonicalStatus) -> str:
    return (
        f'<span class="status status-{status.value.lower()}">'
        f"{escape(_STATUS_LABELS[status])}</span>"
    )


def _assertion_badge(kind: AssertionKind) -> str:
    return (
        f'<span class="assertion assertion-{kind.value.lower()}">'
        f"{escape(_ASSERTION_LABELS[kind])}</span>"
    )


def _yes_no(value: bool) -> str:
    return "已就绪" if value else "未就绪"


__all__ = [
    "AtlasRenderedArtifact",
    "render_atlas_html",
    "write_atlas_artifacts",
]
