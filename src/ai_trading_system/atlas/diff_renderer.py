from __future__ import annotations

# HTML/CSS source lines remain readable as one semantic declaration.
# ruff: noqa: E501
import json
from dataclasses import dataclass
from html import escape
from pathlib import Path

from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffSignificance,
    ExplorerEntityChange,
)
from ai_trading_system.platform.artifacts import write_bytes_atomic

from .diff_validation import (
    diff_validation_json_bytes,
    validate_snapshot_diff_bundle,
)
from .snapshot_diff import AtlasSnapshotDiffBundle

_ENTITY_LABELS = {
    ExplorerDiffEntityKind.SOURCE: "证据来源",
    ExplorerDiffEntityKind.NODE: "研究节点",
    ExplorerDiffEntityKind.EDGE: "路径关系",
    ExplorerDiffEntityKind.RESULT: "结果卡",
    ExplorerDiffEntityKind.ATTRIBUTION: "归因说明",
}
_CHANGE_LABELS = {
    ExplorerDiffChangeKind.ADDED: "新增",
    ExplorerDiffChangeKind.REMOVED: "移除",
    ExplorerDiffChangeKind.CHANGED: "内容变化",
}
_SIGNIFICANCE_LABELS = {
    ExplorerDiffSignificance.SEMANTIC: "研究含义变化",
    ExplorerDiffSignificance.LINEAGE_ONLY: "仅证据时点变化",
    ExplorerDiffSignificance.STRUCTURAL: "结构变化",
}


@dataclass(frozen=True)
class AtlasDiffRenderedArtifact:
    path: str
    sha256: str
    size_bytes: int


def _entity_payload_maps(
    bundle: AtlasSnapshotDiffBundle,
) -> tuple[
    dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
    dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
]:
    specs = (
        (ExplorerDiffEntityKind.SOURCE, "sources", "source_ref_id"),
        (ExplorerDiffEntityKind.NODE, "nodes", "node_id"),
        (ExplorerDiffEntityKind.EDGE, "edges", "edge_id"),
        (ExplorerDiffEntityKind.RESULT, "results", "result_id"),
        (ExplorerDiffEntityKind.ATTRIBUTION, "attributions", "attribution_id"),
    )

    def build(snapshot: object) -> dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]]:
        mapped: dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]] = {}
        for kind, collection_name, id_field in specs:
            for item in getattr(snapshot, collection_name):
                payload = item.to_dict()
                mapped[(kind, str(payload[id_field]))] = payload
        return mapped

    return build(bundle.before), build(bundle.after)


def _entity_title(
    change: ExplorerEntityChange,
    before_map: dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
    after_map: dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
) -> str:
    payload = after_map.get((change.entity_kind, change.entity_id)) or before_map.get(
        (change.entity_kind, change.entity_id),
        {},
    )
    for field in ("title", "label", "artifact_identity", "source_path", "explanation"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            return value
    return change.entity_id


def _pretty_json(raw: str) -> str:
    return json.dumps(
        json.loads(raw),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )


def _render_field_changes(change: ExplorerEntityChange) -> str:
    if not change.field_changes:
        return (
            '<p class="plain-note">该稳定 ID 只出现在一侧，因此记录为结构新增或移除；'
            "系统不会猜测它是否只是改名。</p>"
        )
    rows = "".join(
        "<tr>"
        f"<th><code>{escape(item.field_name)}</code></th>"
        f"<td><pre>{escape(_pretty_json(item.before_json))}</pre></td>"
        f"<td><pre>{escape(_pretty_json(item.after_json))}</pre></td>"
        "</tr>"
        for item in change.field_changes
    )
    return (
        '<div class="table-wrap"><table class="field-table">'
        "<thead><tr><th>字段</th><th>之前</th><th>之后</th></tr></thead>"
        f"<tbody>{rows}</tbody></table></div>"
    )


def _render_change_card(
    change: ExplorerEntityChange,
    before_map: dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
    after_map: dict[tuple[ExplorerDiffEntityKind, str], dict[str, object]],
) -> str:
    title = _entity_title(change, before_map, after_map)
    return (
        '<article class="change-card">'
        '<div class="change-head">'
        "<div>"
        f'<p class="entity-kind">{escape(_ENTITY_LABELS[change.entity_kind])}</p>'
        f"<h3>{escape(title)}</h3>"
        f'<p class="entity-id">稳定 ID：<code>{escape(change.entity_id)}</code></p>'
        "</div>"
        '<div class="badges">'
        f'<span class="badge badge-{change.change_kind.value.lower()}">{escape(_CHANGE_LABELS[change.change_kind])}</span>'
        f'<span class="badge significance-{change.significance.value.lower()}">{escape(_SIGNIFICANCE_LABELS[change.significance])}</span>'
        "</div></div>"
        f"{_render_field_changes(change)}"
        '<p class="hashes">'
        f"before <code>{escape(change.before_sha256 or 'none')}</code><br>"
        f"after <code>{escape(change.after_sha256 or 'none')}</code>"
        "</p>"
        "</article>"
    )


def render_snapshot_diff_html(bundle: AtlasSnapshotDiffBundle) -> str:
    validation = validate_snapshot_diff_bundle(bundle)
    if validation.status != "PASS":
        raise ValueError("ATLAS_DIFF_RENDER_VALIDATION_FAILED:" + ",".join(validation.errors))
    diff = bundle.diff
    before_map, after_map = _entity_payload_maps(bundle)
    summary_rows = "".join(
        "<tr>"
        f"<th>{escape(_ENTITY_LABELS[item.entity_kind])}</th>"
        f"<td>{item.before_count}</td>"
        f"<td>{item.after_count}</td>"
        f"<td>{item.added_count}</td>"
        f"<td>{item.removed_count}</td>"
        f"<td>{item.changed_count}</td>"
        f"<td>{item.unchanged_count}</td>"
        "</tr>"
        for item in diff.entity_summaries
    )
    semantic_cards = "".join(
        _render_change_card(item, before_map, after_map)
        for item in diff.changes
        if item.significance is ExplorerDiffSignificance.SEMANTIC
    )
    structural_cards = "".join(
        _render_change_card(item, before_map, after_map)
        for item in diff.changes
        if item.significance is ExplorerDiffSignificance.STRUCTURAL
    )
    lineage_cards = "".join(
        _render_change_card(item, before_map, after_map)
        for item in diff.changes
        if item.significance is ExplorerDiffSignificance.LINEAGE_ONLY
    )
    before_input, after_input = bundle.input_receipt.inputs
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Atlas 研究变化一览</title>
  <style>
    :root {{ --ink:#172238; --muted:#647189; --line:#dbe2ec; --paper:#f6f8fb; --panel:#fff; --blue:#2856a3; --green:#1c7255; --amber:#936316; --red:#a63a4b; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font:16px/1.65 system-ui,-apple-system,"Segoe UI",sans-serif; }}
    header {{ color:white; background:linear-gradient(135deg,#13233f,#2856a3); padding:3rem max(1.25rem,calc((100vw - 1120px)/2)); }}
    header h1 {{ margin:.2rem 0 .6rem; font-size:clamp(2rem,5vw,3.7rem); line-height:1.08; }}
    .eyebrow {{ margin:0; font-size:.78rem; letter-spacing:.14em; font-weight:800; }}
    .lead {{ max-width:760px; margin:.7rem 0 0; color:#dce8ff; font-size:1.08rem; }}
    main {{ width:min(1120px,calc(100% - 2rem)); margin:0 auto; padding:2rem 0 4rem; }}
    section {{ margin:0 0 2rem; }}
    h2 {{ margin:0 0 .35rem; font-size:1.55rem; }}
    .intro {{ margin:.2rem 0 1rem; color:var(--muted); max-width:850px; }}
    .metrics {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.8rem; margin-top:1.5rem; }}
    .metric {{ padding:1rem; border:1px solid #ffffff33; border-radius:.85rem; background:#ffffff12; }}
    .metric strong {{ display:block; font-size:1.8rem; }}
    .metric span {{ color:#dce8ff; font-size:.86rem; }}
    .notice {{ padding:1rem 1.15rem; border-left:5px solid var(--blue); border-radius:.5rem; background:#edf3ff; }}
    .table-wrap {{ overflow:auto; border:1px solid var(--line); border-radius:.8rem; background:white; }}
    table {{ width:100%; border-collapse:collapse; }}
    th,td {{ padding:.7rem .8rem; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
    thead th {{ background:#edf1f7; font-size:.82rem; }}
    .change-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:1rem; }}
    .change-card {{ min-width:0; border:1px solid var(--line); border-radius:.9rem; background:var(--panel); box-shadow:0 8px 24px #12213a0a; overflow:hidden; }}
    .change-head {{ display:flex; justify-content:space-between; gap:1rem; padding:1rem; border-bottom:1px solid var(--line); }}
    .change-head h3 {{ margin:.1rem 0; font-size:1.05rem; }}
    .entity-kind,.entity-id {{ margin:0; color:var(--muted); font-size:.78rem; }}
    .badges {{ display:flex; flex-wrap:wrap; justify-content:flex-end; align-content:flex-start; gap:.35rem; }}
    .badge {{ white-space:nowrap; padding:.2rem .52rem; border-radius:999px; font-size:.72rem; font-weight:800; }}
    .badge-added {{ color:var(--green); background:#e4f5ee; }}
    .badge-removed {{ color:var(--red); background:#fae8eb; }}
    .badge-changed {{ color:var(--blue); background:#e7efff; }}
    .significance-semantic {{ color:#6c378d; background:#f3e9fa; }}
    .significance-lineage_only {{ color:#536078; background:#eef1f5; }}
    .significance-structural {{ color:var(--amber); background:#fff1d8; }}
    .field-table th {{ width:20%; }}
    pre {{ margin:0; max-width:31rem; overflow-wrap:anywhere; white-space:pre-wrap; font:12px/1.45 ui-monospace,SFMono-Regular,Consolas,monospace; }}
    .plain-note,.hashes {{ margin:.8rem 1rem; color:var(--muted); font-size:.84rem; overflow-wrap:anywhere; }}
    details {{ border:1px solid var(--line); border-radius:.8rem; background:#eef1f5; }}
    summary {{ cursor:pointer; padding:1rem; font-weight:800; }}
    details .change-grid {{ padding:0 1rem 1rem; }}
    code {{ overflow-wrap:anywhere; }}
    footer {{ margin-top:2.5rem; padding-top:1rem; border-top:1px solid var(--line); color:var(--muted); font-size:.82rem; }}
    @media (max-width:820px) {{ .metrics,.change-grid {{ grid-template-columns:1fr; }} .change-head {{ display:block; }} .badges {{ justify-content:flex-start; margin-top:.6rem; }} }}
    @media print {{ body {{ background:white; }} .change-card {{ break-inside:avoid; box-shadow:none; }} details {{ background:white; }} }}
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">ATLAS · CROSS-SNAPSHOT DIFF</p>
    <h1>研究变化一览</h1>
    <p class="lead">比较两次研究快照，回答“新增了什么、删掉了什么、哪些研究含义改变、哪些只是证据时点更新”。这不是策略好坏结论，也不触发任何交易。</p>
    <div class="metrics" aria-label="变化摘要">
      <div class="metric"><strong>{diff.total_change_count}</strong><span>全部变化</span></div>
      <div class="metric"><strong>{diff.semantic_count}</strong><span>研究含义变化</span></div>
      <div class="metric"><strong>{diff.structural_count}</strong><span>结构新增 / 移除</span></div>
      <div class="metric"><strong>{diff.lineage_only_count}</strong><span>仅证据时点变化</span></div>
    </div>
  </header>
  <main>
    <section>
      <h2>先读这一条规则</h2>
      <p class="notice"><strong>只按稳定 ID 比较。</strong>同一个 ID 内容不同，记为“内容变化”；只在一侧出现，记为“新增”或“移除”。系统不做 rename inference，也不使用 fuzzy matching 猜测两个对象是否相同。</p>
    </section>
    <section>
      <h2>五类对象的变化</h2>
      <p class="intro">“之前 / 之后”是对象数量；“未变化”仍被统计，但不会制造变化卡片。</p>
      <div class="table-wrap"><table>
        <thead><tr><th>对象</th><th>之前</th><th>之后</th><th>新增</th><th>移除</th><th>内容变化</th><th>未变化</th></tr></thead>
        <tbody>{summary_rows}</tbody>
      </table></div>
    </section>
    <section>
      <h2>研究含义变化</h2>
      <p class="intro">这些字段会改变读者对研究内容、结果或归因的理解，需要优先复核。</p>
      <div class="change-grid">{semantic_cards or '<p class="plain-note">没有研究含义变化。</p>'}</div>
    </section>
    <section>
      <h2>结构新增与移除</h2>
      <p class="intro">稳定 ID 只在一个快照中出现。为避免误判，改名也会显示成一条移除加一条新增。</p>
      <div class="change-grid">{structural_cards or '<p class="plain-note">没有结构变化。</p>'}</div>
    </section>
    <section>
      <h2>仅证据时点变化</h2>
      <p class="intro">这类变化只涉及 exact_commit、as_of、known_at 或 available_at，内容身份保持不变；仍完整保留以便审计。</p>
      <details open>
        <summary>{diff.lineage_only_count} 条 lineage-only 变化（可折叠）</summary>
        <div class="change-grid">{lineage_cards or '<p class="plain-note">没有 lineage-only 变化。</p>'}</div>
      </details>
    </section>
    <footer>
      <p>Before：<code>{escape(before_input.source_path)}</code> · snapshot <code>{escape(before_input.snapshot_id)}</code> · SHA-256 <code>{escape(before_input.file_sha256)}</code> · {before_input.size_bytes} bytes</p>
      <p>After：<code>{escape(after_input.source_path)}</code> · snapshot <code>{escape(after_input.snapshot_id)}</code> · SHA-256 <code>{escape(after_input.file_sha256)}</code> · {after_input.size_bytes} bytes</p>
      <p>Diff：<code>{escape(diff.diff_id)}</code> · Receipt：<code>{escape(bundle.input_receipt.receipt_id)}</code> · recorded_at <code>{escape(bundle.input_receipt.recorded_at.isoformat())}</code></p>
      <p>manual_review_only=<code>true</code> · production_effect=<code>none</code> · broker_action=<code>none</code></p>
    </footer>
  </main>
</body>
</html>
"""


def write_snapshot_diff_artifacts(
    bundle: AtlasSnapshotDiffBundle,
    output_directory: Path,
) -> tuple[AtlasDiffRenderedArtifact, ...]:
    validation = validate_snapshot_diff_bundle(bundle)
    if validation.status != "PASS":
        raise ValueError("ATLAS_DIFF_ARTIFACT_VALIDATION_FAILED:" + ",".join(validation.errors))
    output_directory.mkdir(parents=True, exist_ok=True)
    payloads = {
        "index.html": render_snapshot_diff_html(bundle).encode("utf-8"),
        "diff.json": bundle.diff.canonical_json_bytes(),
        "validation.json": diff_validation_json_bytes(validation),
        "input_receipt.json": bundle.input_receipt.canonical_json_bytes(),
    }
    artifacts: list[AtlasDiffRenderedArtifact] = []
    for name, payload in payloads.items():
        result = write_bytes_atomic(output_directory / name, payload)
        artifacts.append(
            AtlasDiffRenderedArtifact(
                path=result.path.as_posix(),
                sha256=result.sha256,
                size_bytes=result.size_bytes,
            )
        )
    return tuple(artifacts)


__all__ = [
    "AtlasDiffRenderedArtifact",
    "render_snapshot_diff_html",
    "write_snapshot_diff_artifacts",
]
