from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html import escape
from pathlib import Path
from typing import Any, cast

TraceRecord = dict[str, Any]


@dataclass(frozen=True)
class EvidenceDashboardReport:
    as_of: date
    generated_at: datetime
    daily_report_path: Path
    trace_bundle_path: Path
    decision_snapshot_path: Path
    belief_state_path: Path | None
    trace_bundle: TraceRecord
    decision_snapshot: TraceRecord
    belief_state: TraceRecord | None
    conclusion_card: Mapping[str, str]
    change_conditions: tuple[str, ...]
    warnings: tuple[str, ...]
    production_effect: str = "none"

    @property
    def status(self) -> str:
        return "PASS_WITH_LIMITATIONS" if self.warnings else "PASS"


def default_evidence_dashboard_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"evidence_dashboard_{as_of.isoformat()}.html"


def build_evidence_dashboard_report(
    *,
    as_of: date,
    daily_report_path: Path,
    trace_bundle_path: Path,
    decision_snapshot_path: Path,
    belief_state_path: Path | None = None,
) -> EvidenceDashboardReport:
    daily_text = _read_required_text(daily_report_path)
    trace_bundle = _read_required_json(trace_bundle_path)
    decision_snapshot = _read_required_json(decision_snapshot_path)

    warnings: list[str] = []
    _validate_artifact_dates(
        as_of=as_of,
        trace_bundle=trace_bundle,
        decision_snapshot=decision_snapshot,
        warnings=warnings,
    )

    resolved_belief_state_path = _resolve_belief_state_path(
        explicit_path=belief_state_path,
        decision_snapshot=decision_snapshot,
    )
    belief_state = _read_optional_belief_state(resolved_belief_state_path, warnings)
    conclusion_card = _extract_conclusion_card(daily_text, warnings)
    change_conditions = _extract_change_conditions(daily_text, warnings)

    return EvidenceDashboardReport(
        as_of=as_of,
        generated_at=datetime.now(tz=UTC),
        daily_report_path=daily_report_path,
        trace_bundle_path=trace_bundle_path,
        decision_snapshot_path=decision_snapshot_path,
        belief_state_path=resolved_belief_state_path,
        trace_bundle=trace_bundle,
        decision_snapshot=decision_snapshot,
        belief_state=belief_state,
        conclusion_card=conclusion_card,
        change_conditions=change_conditions,
        warnings=tuple(warnings),
    )


def write_evidence_dashboard(
    report: EvidenceDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_evidence_dashboard(report), encoding="utf-8")
    return output_path


def render_evidence_dashboard(report: EvidenceDashboardReport) -> str:
    title = f"AI 产业链证据下钻 Dashboard {report.as_of.isoformat()}"
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="zh-Hans">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{_text(title)}</title>",
            f"<style>{_CSS}</style>",
            "</head>",
            "<body>",
            _render_header(report, title),
            "<main>",
            _render_summary_grid(report),
            _render_logic_chain(report),
            _render_reader_modes(report),
            _render_claim_evidence_map(report),
            _render_artifact_section(report),
            _render_warnings(report),
            "</main>",
            _render_footer(report),
            "</body>",
            "</html>",
            "",
        ]
    )


def _render_header(report: EvidenceDashboardReport, title: str) -> str:
    return "\n".join(
        [
            "<header>",
            "<p class=\"eyebrow\">UI-001 evidence-first dashboard</p>",
            f"<h1>{_text(title)}</h1>",
            (
                "<p class=\"subtle\">只读解释页，连接日报结论、论证链、"
                "trace evidence 和实际输入数据；Markdown 日报和 trace bundle "
                "仍是审计源。</p>"
            ),
            '<div class="header-meta">',
            _metric("状态", report.status),
            _metric("评估日期", report.as_of.isoformat()),
            _metric("生成时间", report.generated_at.isoformat()),
            _metric("production_effect", report.production_effect),
            "</div>",
            "</header>",
        ]
    )


def _render_summary_grid(report: EvidenceDashboardReport) -> str:
    snapshot = report.decision_snapshot
    primary_claim = _primary_claim(report)
    quality_status = _quality_status(report)
    position = _final_position_summary(snapshot)
    confidence = _confidence_summary(snapshot)
    action = report.conclusion_card.get("执行动作", "未从日报结论卡提取")
    market_regime = _market_regime_summary(report.trace_bundle)
    largest_limit = report.conclusion_card.get("最大限制", _triggered_gate_summary(snapshot))
    rows = [
        _summary_item("主结论", _record_text(primary_claim, "statement", "未找到核心 claim")),
        _summary_item("执行动作", action),
        _summary_item("最终仓位", position),
        _summary_item("判断置信度", confidence),
        _summary_item("数据质量", quality_status),
        _summary_item("市场阶段", market_regime),
        _summary_item("最大限制", largest_limit),
    ]
    return "\n".join(
        [
            '<section aria-labelledby="summary-title">',
            '<div class="section-head">',
            '<h2 id="summary-title">结论总览</h2>',
            "<p>先看当前结论，再沿着 evidence 和输入数据下钻。</p>",
            "</div>",
            '<div class="summary-grid">',
            *rows,
            "</div>",
            "</section>",
        ]
    )


def _render_logic_chain(report: EvidenceDashboardReport) -> str:
    primary_claim = _primary_claim(report)
    evidence_rows = _linked_evidence(report, primary_claim)
    dataset_rows = _linked_datasets(report, primary_claim)
    quality_rows = _linked_quality(report, primary_claim)
    evidence_summary = _joined_record_values(evidence_rows, "summary", "未找到关联 evidence")
    dataset_summary = _joined_record_values(dataset_rows, "label", "未找到关联 dataset")
    quality_summary = _joined_quality_status(quality_rows)
    gate_summary = _triggered_gate_summary(report.decision_snapshot)
    change_summary = "；".join(report.change_conditions) or "未从日报提取改变判断条件。"
    return "\n".join(
        [
            '<section aria-labelledby="logic-title">',
            '<div class="section-head">',
            '<h2 id="logic-title">论证链</h2>',
            "<p>固定按结论、证据、输入、质量、限制和触发条件展开。</p>",
            "</div>",
            '<ol class="logic-chain">',
            _logic_item("结论", _record_text(primary_claim, "statement", "未找到核心 claim")),
            _logic_item("证据", evidence_summary),
            _logic_item("输入数据", dataset_summary),
            _logic_item("质量门禁", quality_summary),
            _logic_item("限制/闸门", gate_summary),
            _logic_item("可改变判断条件", change_summary),
            "</ol>",
            "</section>",
        ]
    )


def _render_reader_modes(report: EvidenceDashboardReport) -> str:
    return "\n".join(
        [
            '<section aria-labelledby="mode-title">',
            '<div class="section-head">',
            '<h2 id="mode-title">读者模式</h2>',
            "<p>同一组输入，按不同复核深度分层查看。</p>",
            "</div>",
            '<div class="mode-tabs">',
            (
                '<input type="radio" id="mode-quick" name="reader-mode" '
                'checked aria-controls="quick-panel">'
            ),
            '<input type="radio" id="mode-review" name="reader-mode" '
            'aria-controls="review-panel">',
            '<input type="radio" id="mode-audit" name="reader-mode" '
            'aria-controls="audit-panel">',
            '<div class="tab-labels" role="tablist" aria-label="读者模式">',
            '<label for="mode-quick" role="tab">快速读者</label>',
            '<label for="mode-review" role="tab">投资复核者</label>',
            '<label for="mode-audit" role="tab">系统审计者</label>',
            "</div>",
            '<div class="panels">',
            _render_quick_panel(report),
            _render_review_panel(report),
            _render_audit_panel(report),
            "</div>",
            "</div>",
            "</section>",
        ]
    )


def _render_quick_panel(report: EvidenceDashboardReport) -> str:
    conclusion_rows = [
        ("状态标签", report.conclusion_card.get("状态标签", "")),
        ("执行动作", report.conclusion_card.get("执行动作", "")),
        ("主结论", _record_text(_primary_claim(report), "statement", "")),
        (
            "最大限制",
            report.conclusion_card.get(
                "最大限制",
                _triggered_gate_summary(report.decision_snapshot),
            ),
        ),
        ("改变判断条件", "；".join(report.change_conditions)),
    ]
    return "\n".join(
        [
            '<section id="quick-panel" class="panel quick-panel">',
            "<h3>快速读者</h3>",
            _key_value_table(conclusion_rows),
            "</section>",
        ]
    )


def _render_review_panel(report: EvidenceDashboardReport) -> str:
    return "\n".join(
        [
            '<section id="review-panel" class="panel review-panel">',
            "<h3>投资复核者</h3>",
            '<div class="split">',
            '<div>',
            "<h4>评分组件</h4>",
            _component_table(report.decision_snapshot),
            "</div>",
            '<div>',
            "<h4>仓位闸门</h4>",
            _gate_table(report.decision_snapshot),
            "</div>",
            "</div>",
            "<h4>Thesis / Risk / Valuation</h4>",
            _state_table(report),
            "</section>",
        ]
    )


def _render_audit_panel(report: EvidenceDashboardReport) -> str:
    primary_claim = _primary_claim(report)
    command = (
        f"aits trace lookup --bundle-path {report.trace_bundle_path} "
        f"--id {_record_text(primary_claim, 'claim_id', '')}"
    )
    return "\n".join(
        [
            '<section id="audit-panel" class="panel audit-panel">',
            "<h3>系统审计者</h3>",
            "<p>从 claim 反查 evidence、dataset、quality 和 run manifest。</p>",
            f"<pre>{_text(command)}</pre>",
            _claim_table(report),
            "</section>",
        ]
    )


def _render_claim_evidence_map(report: EvidenceDashboardReport) -> str:
    return "\n".join(
        [
            '<section aria-labelledby="claim-map-title">',
            '<div class="section-head">',
            '<h2 id="claim-map-title">Claim 到输入映射</h2>',
            "<p>每条核心结论都显示它引用的证据、数据集和质量门禁。</p>",
            "</div>",
            _claim_table(report),
            "</section>",
        ]
    )


def _render_artifact_section(report: EvidenceDashboardReport) -> str:
    return "\n".join(
        [
            '<section aria-labelledby="artifact-title">',
            '<div class="section-head">',
            '<h2 id="artifact-title">输入与审计 Artifact</h2>',
            "<p>这些路径是 dashboard 使用的实际输入，不是推断口径。</p>",
            "</div>",
            _key_value_table(
                [
                    ("日报 Markdown", str(report.daily_report_path)),
                    ("Evidence bundle", str(report.trace_bundle_path)),
                    ("Decision snapshot", str(report.decision_snapshot_path)),
                    (
                        "Belief state",
                        (
                            "未接入"
                            if report.belief_state_path is None
                            else str(report.belief_state_path)
                        ),
                    ),
                ]
            ),
            "<h3>Dataset refs</h3>",
            _dataset_table(report),
            "<h3>Quality refs</h3>",
            _quality_table(report),
            "</section>",
        ]
    )


def _render_warnings(report: EvidenceDashboardReport) -> str:
    if not report.warnings:
        return ""
    return "\n".join(
        [
            '<section aria-labelledby="warning-title">',
            '<div class="section-head warning">',
            '<h2 id="warning-title">限制说明</h2>',
            "<p>Dashboard 已生成，但以下输入限制需要复核。</p>",
            "</div>",
            "<ul>",
            *[f"<li>{_text(warning)}</li>" for warning in report.warnings],
            "</ul>",
            "</section>",
        ]
    )


def _render_footer(report: EvidenceDashboardReport) -> str:
    return "\n".join(
        [
            "<footer>",
            (
                "Dashboard 为只读解释层；正式审计仍以 "
                f"{_code(report.daily_report_path)} 和 {_code(report.trace_bundle_path)} 为准。"
            ),
            "</footer>",
        ]
    )


def _claim_table(report: EvidenceDashboardReport) -> str:
    claims = _records(report.trace_bundle, "claims")
    if not claims:
        return "<p>未找到 claim refs。</p>"
    rows = [
        (
            _record_text(claim, "claim_id", ""),
            _record_text(claim, "statement", ""),
            _inline_values(claim.get("evidence_ids")),
            _inline_values(claim.get("dataset_ids")),
            _inline_values(claim.get("quality_ids")),
        )
        for claim in claims
    ]
    return _table(
        ("Claim", "结论", "Evidence", "Dataset", "Quality"),
        rows,
    )


def _dataset_table(report: EvidenceDashboardReport) -> str:
    datasets = _records(report.trace_bundle, "dataset_refs")
    if not datasets:
        return "<p>未找到 dataset refs。</p>"
    rows = []
    for dataset in datasets:
        rows.append(
            (
                _record_text(dataset, "dataset_id", ""),
                _record_text(dataset, "label", ""),
                _record_text(dataset, "dataset_type", ""),
                _record_text(dataset, "path", ""),
                _record_text(dataset, "row_count", ""),
                _record_text(dataset, "checksum_sha256", ""),
                _record_text(dataset, "provider", ""),
            )
        )
    return _table(
        ("Dataset", "名称", "类型", "路径", "行数", "Checksum", "Provider"),
        rows,
    )


def _quality_table(report: EvidenceDashboardReport) -> str:
    qualities = _records(report.trace_bundle, "quality_refs")
    if not qualities:
        return "<p>未找到 quality refs。</p>"
    rows = [
        (
            _record_text(quality, "quality_id", ""),
            _record_text(quality, "label", ""),
            _record_text(quality, "status", ""),
            _record_text(quality, "report_path", ""),
            _record_text(quality, "error_count", ""),
            _record_text(quality, "warning_count", ""),
        )
        for quality in qualities
    ]
    return _table(("Quality", "名称", "状态", "报告", "错误", "警告"), rows)


def _component_table(snapshot: TraceRecord) -> str:
    scores = _mapping(snapshot.get("scores"))
    components = _list_mappings(scores.get("components"))
    if not components:
        return "<p>decision snapshot 未提供组件评分。</p>"
    rows = [
        (
            _record_text(component, "component", ""),
            _record_text(component, "score", ""),
            _record_text(component, "weight", ""),
            _record_text(component, "source_type", ""),
            _record_text(component, "coverage", ""),
            _record_text(component, "confidence", ""),
            _record_text(component, "reason", ""),
        )
        for component in components
    ]
    return _table(("模块", "分数", "权重", "来源", "覆盖率", "置信度", "说明"), rows)


def _gate_table(snapshot: TraceRecord) -> str:
    gates = _list_mappings(_mapping(snapshot.get("positions")).get("position_gates"))
    if not gates:
        return "<p>decision snapshot 未提供仓位 gate。</p>"
    rows = [
        (
            _record_text(gate, "gate_id", ""),
            _record_text(gate, "label", ""),
            _record_text(gate, "source", ""),
            _format_percent(gate.get("max_position")),
            "是" if gate.get("triggered") is True else "否",
            _record_text(gate, "reason", ""),
        )
        for gate in gates
    ]
    return _table(("Gate", "名称", "来源", "上限", "触发", "说明"), rows)


def _state_table(report: EvidenceDashboardReport) -> str:
    belief = report.belief_state or {}
    rows = [
        ("Thesis", _state_summary(belief, report.decision_snapshot, "thesis_state")),
        ("Risk", _state_summary(belief, report.decision_snapshot, "risk_state")),
        ("Valuation", _state_summary(belief, report.decision_snapshot, "valuation_state")),
        (
            "仓位边界",
            _position_boundary_summary(report.decision_snapshot, belief),
        ),
    ]
    return _key_value_table(rows)


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header = "".join(f"<th>{_text(item)}</th>" for item in headers)
    body_rows = [
        "<tr>" + "".join(f"<td>{_text(cell)}</td>" for cell in row) + "</tr>"
        for row in rows
    ]
    return "\n".join(
        [
            '<div class="table-wrap">',
            "<table>",
            f"<thead><tr>{header}</tr></thead>",
            "<tbody>",
            *body_rows,
            "</tbody>",
            "</table>",
            "</div>",
        ]
    )


def _key_value_table(rows: Sequence[tuple[str, str]]) -> str:
    return _table(("项目", "内容"), rows)


def _summary_item(label: str, value: str) -> str:
    return "\n".join(
        [
            '<div class="summary-item">',
            f"<span>{_text(label)}</span>",
            f"<strong>{_text(value)}</strong>",
            "</div>",
        ]
    )


def _logic_item(label: str, value: str) -> str:
    return f"<li><strong>{_text(label)}</strong><span>{_text(value)}</span></li>"


def _metric(label: str, value: str) -> str:
    return f"<span><strong>{_text(label)}:</strong> {_text(value)}</span>"


def _read_required_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"required dashboard input does not exist: {path}")
    return path.read_text(encoding="utf-8")


def _read_required_json(path: Path) -> TraceRecord:
    if not path.exists():
        raise FileNotFoundError(f"required dashboard input does not exist: {path}")
    return _read_json_object(path)


def _read_json_object(path: Path) -> TraceRecord:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON input for dashboard: {path}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"dashboard JSON input must be an object: {path}")
    return cast(TraceRecord, value)


def _resolve_belief_state_path(
    *,
    explicit_path: Path | None,
    decision_snapshot: TraceRecord,
) -> Path | None:
    if explicit_path is not None:
        return explicit_path
    ref = _mapping(decision_snapshot.get("belief_state_ref"))
    path_text = ref.get("path")
    return Path(str(path_text)) if path_text else None


def _read_optional_belief_state(
    belief_state_path: Path | None,
    warnings: list[str],
) -> TraceRecord | None:
    if belief_state_path is None:
        warnings.append("decision snapshot 未提供 belief_state_ref；认知状态下钻不可用。")
        return None
    if not belief_state_path.exists():
        warnings.append(f"belief_state 不存在：{belief_state_path}")
        return None
    return _read_json_object(belief_state_path)


def _validate_artifact_dates(
    *,
    as_of: date,
    trace_bundle: TraceRecord,
    decision_snapshot: TraceRecord,
    warnings: list[str],
) -> None:
    signal_date = decision_snapshot.get("signal_date")
    if signal_date is not None and str(signal_date) != as_of.isoformat():
        warnings.append(
            f"decision snapshot signal_date={signal_date} 与 as_of={as_of.isoformat()} 不一致。"
        )
    report_id = str(trace_bundle.get("report_id", ""))
    if report_id and as_of.isoformat() not in report_id:
        warnings.append(f"trace bundle report_id={report_id} 未包含 as_of={as_of.isoformat()}。")


def _extract_conclusion_card(
    daily_text: str,
    warnings: list[str],
) -> Mapping[str, str]:
    section = _markdown_section(daily_text, "## 今日结论卡")
    if not section:
        warnings.append("日报缺少“今日结论卡”章节；快速读者摘要会降级。")
        return {}
    rows: dict[str, str] = {}
    for line in section.splitlines():
        if not line.startswith("|") or "---" in line:
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) != 2 or cells[0] == "项目":
            continue
        rows[cells[0]] = cells[1]
    largest_limit = _markdown_subsection(section, "### 最大限制")
    if largest_limit:
        bullets = _bullet_lines(largest_limit)
        if bullets:
            rows["最大限制"] = bullets[0]
    return rows


def _extract_change_conditions(
    daily_text: str,
    warnings: list[str],
) -> tuple[str, ...]:
    section = _markdown_subsection(daily_text, "### 什么情况会改变判断")
    if not section:
        warnings.append("日报缺少“什么情况会改变判断”小节；触发条件下钻会降级。")
        return ()
    return tuple(_bullet_lines(section))


def _markdown_section(text: str, heading: str) -> str:
    lines = text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.strip() == heading:
            start = index + 1
            break
    if start is None:
        return ""
    end = len(lines)
    for index in range(start, len(lines)):
        if lines[index].startswith("## "):
            end = index
            break
    return "\n".join(lines[start:end]).strip()


def _markdown_subsection(text: str, heading: str) -> str:
    lines = text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if line.strip() == heading:
            start = index + 1
            break
    if start is None:
        return ""
    end = len(lines)
    for index in range(start, len(lines)):
        line = lines[index]
        if line.startswith("## ") or line.startswith("### "):
            end = index
            break
    return "\n".join(lines[start:end]).strip()


def _bullet_lines(markdown: str) -> list[str]:
    return [line[2:].strip() for line in markdown.splitlines() if line.startswith("- ")]


def _primary_claim(report: EvidenceDashboardReport) -> TraceRecord:
    claims = _records(report.trace_bundle, "claims")
    for claim in claims:
        if str(claim.get("claim_id", "")).endswith(":overall_position"):
            return claim
    return claims[0] if claims else {}


def _linked_evidence(report: EvidenceDashboardReport, claim: TraceRecord) -> list[TraceRecord]:
    ids = _string_set(claim.get("evidence_ids"))
    return [
        evidence
        for evidence in _records(report.trace_bundle, "evidence_cards")
        if str(evidence.get("evidence_id")) in ids
    ]


def _linked_datasets(report: EvidenceDashboardReport, claim: TraceRecord) -> list[TraceRecord]:
    ids = _string_set(claim.get("dataset_ids"))
    return [
        dataset
        for dataset in _records(report.trace_bundle, "dataset_refs")
        if str(dataset.get("dataset_id")) in ids
    ]


def _linked_quality(report: EvidenceDashboardReport, claim: TraceRecord) -> list[TraceRecord]:
    ids = _string_set(claim.get("quality_ids"))
    return [
        quality
        for quality in _records(report.trace_bundle, "quality_refs")
        if str(quality.get("quality_id")) in ids
    ]


def _records(container: TraceRecord, key: str) -> list[TraceRecord]:
    return [cast(TraceRecord, item) for item in _list_mappings(container.get(key))]


def _list_mappings(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _string_set(value: object) -> set[str]:
    if not isinstance(value, list | tuple):
        return set()
    return {str(item) for item in value}


def _record_text(record: Mapping[str, Any], key: str, default: str) -> str:
    value = record.get(key)
    if value is None:
        return default
    if isinstance(value, float):
        return f"{value:.3g}"
    return str(value)


def _inline_values(value: object) -> str:
    if not isinstance(value, list | tuple):
        return "" if value is None else str(value)
    return "\n".join(str(item) for item in value)


def _joined_record_values(
    records: list[TraceRecord],
    key: str,
    default: str,
) -> str:
    values = [_record_text(record, key, "") for record in records]
    values = [value for value in values if value]
    return "；".join(values) if values else default


def _joined_quality_status(records: list[TraceRecord]) -> str:
    values = [
        (
            f"{_record_text(record, 'label', _record_text(record, 'quality_id', 'quality'))}: "
            f"{_record_text(record, 'status', 'UNKNOWN')}"
        )
        for record in records
    ]
    return "；".join(values) if values else "未找到关联 quality refs"


def _quality_status(report: EvidenceDashboardReport) -> str:
    quality_refs = _records(report.trace_bundle, "quality_refs")
    if quality_refs:
        return _joined_quality_status(quality_refs)
    quality = _mapping(report.decision_snapshot.get("quality"))
    return _record_text(quality, "market_data_status", "UNKNOWN")


def _market_regime_summary(trace_bundle: TraceRecord) -> str:
    run_manifest = _mapping(trace_bundle.get("run_manifest"))
    regime = _mapping(run_manifest.get("market_regime"))
    regime_id = _record_text(regime, "regime_id", "unknown")
    start_date = _record_text(regime, "start_date", "")
    return regime_id if not start_date else f"{regime_id}，start {start_date}"


def _confidence_summary(snapshot: TraceRecord) -> str:
    scores = _mapping(snapshot.get("scores"))
    score = _record_text(scores, "confidence_score", "")
    level = _record_text(scores, "confidence_level", "")
    if not score and not level:
        return "未在 decision snapshot 中找到置信度"
    return f"{score}（{level}）" if score and level else score or level


def _final_position_summary(snapshot: TraceRecord) -> str:
    positions = _mapping(snapshot.get("positions"))
    return _format_band_record(positions.get("final_risk_asset_ai_band"))


def _position_boundary_summary(snapshot: TraceRecord, belief: Mapping[str, Any]) -> str:
    positions = _mapping(snapshot.get("positions"))
    final_band = _format_band_record(positions.get("final_risk_asset_ai_band"))
    total_band = _format_band_record(positions.get("final_total_risk_asset_band"))
    belief_boundary = _mapping(belief.get("position_boundary"))
    belief_limitations = _record_text(belief_boundary, "summary", "")
    parts = [f"最终 AI {final_band}", f"总风险资产 {total_band}"]
    if belief_limitations:
        parts.append(belief_limitations)
    return "；".join(parts)


def _format_band_record(value: object) -> str:
    band = _mapping(value)
    min_text = _format_percent(band.get("min_position"))
    max_text = _format_percent(band.get("max_position"))
    label = _record_text(band, "label", "")
    if min_text and max_text:
        return f"{min_text}-{max_text}" + (f"（{label}）" if label else "")
    return "未提供"


def _format_percent(value: object) -> str:
    if isinstance(value, int | float):
        return f"{value:.0%}"
    return "" if value is None else str(value)


def _triggered_gate_summary(snapshot: TraceRecord) -> str:
    gates = _list_mappings(_mapping(snapshot.get("positions")).get("position_gates"))
    triggered = [gate for gate in gates if gate.get("triggered") is True]
    if not triggered:
        return "未触发额外仓位闸门。"
    return "；".join(
        (
            f"{_record_text(gate, 'label', _record_text(gate, 'gate_id', 'gate'))}"
            f" 上限 {_format_percent(gate.get('max_position'))}: "
            f"{_record_text(gate, 'reason', '')}"
        )
        for gate in triggered
    )


def _state_summary(
    belief: Mapping[str, Any],
    snapshot: TraceRecord,
    key: str,
) -> str:
    state = _mapping(belief.get(key))
    if summary := state.get("summary"):
        return str(summary)
    if key == "risk_state":
        risk_state = _mapping(snapshot.get("risk_event_state"))
        return _record_text(risk_state, "status", "未接入")
    if key == "valuation_state":
        valuation_state = _mapping(snapshot.get("valuation_state"))
        return _record_text(valuation_state, "status", "未接入")
    if key == "thesis_state":
        reviews = _list_mappings(snapshot.get("manual_review"))
        thesis_reviews = [
            item for item in reviews if "thesis" in str(item.get("name", "")).lower()
        ]
        return _record_text(thesis_reviews[0], "summary", "未接入") if thesis_reviews else "未接入"
    return "未接入"


def _code(value: object) -> str:
    return f"<code>{_text(str(value))}</code>"


def _text(value: object) -> str:
    return escape(str(value), quote=True)


_CSS = """
:root {
  color-scheme: light;
  --bg: #f7f9fb;
  --surface: #ffffff;
  --ink: #111827;
  --muted: #5b6472;
  --line: #d7dee8;
  --accent: #0f766e;
  --accent-soft: #e6f3f1;
  --warn: #b45309;
}
* {
  box-sizing: border-box;
}
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: Arial, "Microsoft YaHei", sans-serif;
  line-height: 1.55;
}
header,
main,
footer {
  width: min(1180px, calc(100% - 32px));
  margin: 0 auto;
}
header {
  padding: 28px 0 18px;
  border-bottom: 1px solid var(--line);
}
h1,
h2,
h3,
h4,
p {
  margin-top: 0;
}
h1 {
  margin-bottom: 8px;
  font-size: 28px;
  letter-spacing: 0;
}
h2 {
  font-size: 20px;
  margin-bottom: 6px;
  letter-spacing: 0;
}
h3 {
  font-size: 17px;
  margin-bottom: 12px;
  letter-spacing: 0;
}
h4 {
  font-size: 15px;
  margin: 18px 0 8px;
  letter-spacing: 0;
}
main {
  display: flex;
  flex-direction: column;
  gap: 24px;
  padding: 24px 0 34px;
}
section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 18px;
}
.section-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 14px;
  border-bottom: 1px solid var(--line);
  margin-bottom: 14px;
  padding-bottom: 10px;
}
.section-head p,
.subtle,
.eyebrow,
footer {
  color: var(--muted);
}
.eyebrow {
  margin-bottom: 4px;
  font-size: 12px;
  text-transform: uppercase;
}
.header-meta,
.summary-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
  gap: 10px;
}
.header-meta span,
.summary-item {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  padding: 10px 12px;
}
.summary-item span {
  display: block;
  color: var(--muted);
  font-size: 12px;
}
.summary-item strong {
  display: block;
  overflow-wrap: anywhere;
  font-size: 15px;
}
.logic-chain {
  margin: 0;
  padding-left: 24px;
}
.logic-chain li {
  margin-bottom: 10px;
}
.logic-chain strong {
  display: block;
  color: var(--accent);
}
.logic-chain span {
  overflow-wrap: anywhere;
}
.mode-tabs > input {
  position: absolute;
  opacity: 0;
  pointer-events: none;
}
.tab-labels {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 14px;
}
.tab-labels label {
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 8px 12px;
  cursor: pointer;
  color: var(--muted);
}
#mode-quick:checked ~ .tab-labels label[for="mode-quick"],
#mode-review:checked ~ .tab-labels label[for="mode-review"],
#mode-audit:checked ~ .tab-labels label[for="mode-audit"] {
  background: var(--accent-soft);
  border-color: var(--accent);
  color: var(--accent);
}
.panel {
  display: none;
  border-style: solid;
  background: #fbfcfd;
}
#mode-quick:checked ~ .panels #quick-panel,
#mode-review:checked ~ .panels #review-panel,
#mode-audit:checked ~ .panels #audit-panel {
  display: block;
}
.split {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 16px;
}
.table-wrap {
  overflow-x: auto;
}
table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
th,
td {
  border-bottom: 1px solid var(--line);
  padding: 8px 9px;
  text-align: left;
  vertical-align: top;
  overflow-wrap: anywhere;
  word-break: break-word;
}
th {
  color: var(--muted);
  font-size: 12px;
  font-weight: 700;
}
pre,
code {
  font-family: Consolas, "Courier New", monospace;
}
pre {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  background: #f1f5f9;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 12px;
}
.warning h2 {
  color: var(--warn);
}
footer {
  padding: 0 0 28px;
  font-size: 13px;
  overflow-wrap: anywhere;
}
@media (max-width: 760px) {
  header,
  main,
  footer {
    width: min(100% - 20px, 1180px);
  }
  section {
    padding: 14px;
  }
  .section-head {
    display: block;
  }
}
"""
