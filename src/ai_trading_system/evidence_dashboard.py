from __future__ import annotations

import csv
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
    main_conclusion: str | None
    core_reasons: tuple[str, ...]
    main_invalidator: str | None
    next_checks: tuple[str, ...]
    period_change_summary: str | None
    warnings: tuple[str, ...]
    alerts_report_path: Path | None = None
    scores_daily_path: Path | None = None
    alert_summary: TraceRecord | None = None
    history_points: tuple[TraceRecord, ...] = ()
    production_effect: str = "none"

    @property
    def status(self) -> str:
        return "PASS_WITH_LIMITATIONS" if self.warnings else "PASS"


def default_evidence_dashboard_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"evidence_dashboard_{as_of.isoformat()}.html"


def default_evidence_dashboard_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"evidence_dashboard_{as_of.isoformat()}.json"


def build_evidence_dashboard_report(
    *,
    as_of: date,
    daily_report_path: Path,
    trace_bundle_path: Path,
    decision_snapshot_path: Path,
    belief_state_path: Path | None = None,
    alerts_report_path: Path | None = None,
    scores_daily_path: Path | None = None,
    history_limit: int = 20,
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
    main_conclusion = _first_bullet(_markdown_subsection(daily_text, "### 一句话主结论"))
    core_reasons = tuple(_bullet_lines(_markdown_subsection(daily_text, "### 三个核心原因")))
    main_invalidator = _first_bullet(_markdown_subsection(daily_text, "### Main Invalidator"))
    next_checks = tuple(_bullet_lines(_markdown_subsection(daily_text, "### Next Checks")))
    period_change_summary = _extract_period_change_summary(daily_text)
    alert_summary = _read_optional_alert_summary(alerts_report_path, warnings)
    history_points = _read_optional_history_points(
        scores_daily_path,
        as_of=as_of,
        history_limit=history_limit,
        warnings=warnings,
    )

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
        main_conclusion=main_conclusion,
        core_reasons=core_reasons,
        main_invalidator=main_invalidator,
        next_checks=next_checks,
        period_change_summary=period_change_summary,
        warnings=tuple(warnings),
        alerts_report_path=alerts_report_path,
        scores_daily_path=scores_daily_path,
        alert_summary=alert_summary,
        history_points=history_points,
    )


def write_evidence_dashboard(
    report: EvidenceDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_evidence_dashboard(report), encoding="utf-8")
    return output_path


def write_evidence_dashboard_json(
    report: EvidenceDashboardReport,
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(build_evidence_dashboard_payload(report), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
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
            _render_decision_card(report),
            _render_summary_grid(report),
            _render_alert_summary(report),
            _render_history_trend(report),
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


def build_evidence_dashboard_payload(report: EvidenceDashboardReport) -> TraceRecord:
    return {
        "as_of": report.as_of.isoformat(),
        "generated_at": report.generated_at.isoformat(),
        "status": report.status,
        "production_effect": report.production_effect,
        "decision": _decision_payload(report),
        "top_supporting_evidence": list(_top_supporting_evidence(report)),
        "top_invalidators": list(_top_invalidators(report)),
        "next_checks": list(report.next_checks),
        "alerts": report.alert_summary or {},
        "history": list(report.history_points),
        "artifacts": {
            "daily_report_path": str(report.daily_report_path),
            "trace_bundle_path": str(report.trace_bundle_path),
            "decision_snapshot_path": str(report.decision_snapshot_path),
            "belief_state_path": None
            if report.belief_state_path is None
            else str(report.belief_state_path),
            "alerts_report_path": None
            if report.alerts_report_path is None
            else str(report.alerts_report_path),
            "scores_daily_path": None
            if report.scores_daily_path is None
            else str(report.scores_daily_path),
        },
        "warnings": list(report.warnings),
    }


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


def _render_decision_card(report: EvidenceDashboardReport) -> str:
    decision = _decision_payload(report)
    support_items = _top_supporting_evidence(report)
    invalidators = _top_invalidators(report)
    next_checks = report.next_checks or ("日报未提供 Next Checks。",)
    return "\n".join(
        [
            '<section class="decision-section" aria-labelledby="decision-title">',
            '<div class="section-head">',
            '<h2 id="decision-title">今日决策视图</h2>',
            "<p>只读展示已生成结论，不重新计算评分或触发交易。</p>",
            "</div>",
            '<div class="decision-grid">',
            _decision_metric("执行动作", str(decision["action"])),
            _decision_metric("最终 AI 仓位", str(decision["final_risk_asset_ai_position"])),
            _decision_metric("总风险资产预算", str(decision["total_risk_asset_budget"])),
            _decision_metric("判断置信度", str(decision["confidence"])),
            _decision_metric("Data Gate", str(decision["data_gate"])),
            _decision_metric("最大限制", str(decision["largest_constraint"])),
            _decision_metric("变化摘要", str(decision["change_vs_previous"])),
            _decision_metric("市场阶段", str(decision["market_regime"])),
            "</div>",
            '<div class="decision-columns">',
            _compact_list("Top Supporting Evidence", support_items),
            _compact_list("Top Invalidators / Risks", invalidators),
            _compact_list("Next Checks", next_checks),
            "</div>",
            "</section>",
        ]
    )


def _render_alert_summary(report: EvidenceDashboardReport) -> str:
    if report.alert_summary is None:
        message = (
            "未接入 alerts 报告；dashboard 仍可查看结论和 trace，"
            "但告警聚合需要先生成 alerts_YYYY-MM-DD.md。"
        )
        return "\n".join(
            [
                '<section aria-labelledby="alert-summary-title">',
                '<div class="section-head warning">',
                '<h2 id="alert-summary-title">告警聚合</h2>',
                "<p>可选输入缺失或不可解析时降级显示。</p>",
                "</div>",
                f"<p>{_text(message)}</p>",
                "</section>",
            ]
        )
    severity = _mapping(report.alert_summary.get("severity_counts"))
    top_alerts = _list_mappings(report.alert_summary.get("top_alerts"))
    rows = [
        ("状态", _record_text(report.alert_summary, "status", "UNKNOWN")),
        ("活跃告警", _record_text(report.alert_summary, "active_count", "0")),
        ("data/system", _record_text(report.alert_summary, "data_system_count", "0")),
        ("investment/risk", _record_text(report.alert_summary, "investment_risk_count", "0")),
        (
            "严重度",
            "；".join(
                [
                    f"critical {severity.get('critical', 0)}",
                    f"high {severity.get('high', 0)}",
                    f"warning {severity.get('warning', 0)}",
                ]
            ),
        ),
        ("报告路径", _record_text(report.alert_summary, "path", "")),
    ]
    return "\n".join(
        [
            '<section aria-labelledby="alert-summary-title">',
            '<div class="section-head">',
            '<h2 id="alert-summary-title">告警聚合</h2>',
            "<p>告警只做复核提示，production_effect=none。</p>",
            "</div>",
            _key_value_table(rows),
            "<h3>Top Alerts</h3>",
            _alert_table(top_alerts),
            "</section>",
        ]
    )


def _render_history_trend(report: EvidenceDashboardReport) -> str:
    if not report.history_points:
        message = (
            "未接入 scores_daily.csv 历史趋势；dashboard 仍可查看当日结论，"
            "但不能展示近 20 个交易日的评分和仓位变化。"
        )
        return "\n".join(
            [
                '<section aria-labelledby="history-title">',
                '<div class="section-head warning">',
                '<h2 id="history-title">历史趋势</h2>',
                "<p>可选历史输入缺失时降级显示。</p>",
                "</div>",
                f"<p>{_text(message)}</p>",
                "</section>",
            ]
        )
    score_values = [
        _record_text(point, "overall_score", "")
        for point in report.history_points
        if _record_text(point, "overall_score", "")
    ]
    return "\n".join(
        [
            '<section aria-labelledby="history-title">',
            '<div class="section-head">',
            '<h2 id="history-title">近 20 个交易日趋势</h2>',
            "<p>只读读取 scores_daily.csv 的 overall 行，不重算结论。</p>",
            "</div>",
            _key_value_table(
                [
                    ("样本数量", str(len(report.history_points))),
                    ("Score sparkline", _sparkline(score_values)),
                    (
                        "历史输入",
                        "未接入"
                        if report.scores_daily_path is None
                        else str(report.scores_daily_path),
                    ),
                ]
            ),
            _history_table(report.history_points),
            "</section>",
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


def _decision_metric(label: str, value: str) -> str:
    return "\n".join(
        [
            '<div class="decision-metric">',
            f"<span>{_text(label)}</span>",
            f"<strong>{_text(value or '未提供')}</strong>",
            "</div>",
        ]
    )


def _compact_list(title: str, items: Sequence[str]) -> str:
    values = [item for item in items if item]
    if not values:
        values = ["未提供"]
    return "\n".join(
        [
            '<div class="compact-list">',
            f"<h3>{_text(title)}</h3>",
            "<ol>",
            *[f"<li>{_text(item)}</li>" for item in values[:5]],
            "</ol>",
            "</div>",
        ]
    )


def _alert_table(alerts: Sequence[Mapping[str, Any]]) -> str:
    if not alerts:
        return "<p>未触发告警。</p>"
    rows = [
        (
            _record_text(alert, "severity", ""),
            _record_text(alert, "category", ""),
            _record_text(alert, "source", ""),
            _record_text(alert, "title", ""),
            _record_text(alert, "trigger_condition", ""),
        )
        for alert in alerts
    ]
    return _table(("等级", "类别", "来源", "标题", "触发条件"), rows)


def _history_table(points: Sequence[Mapping[str, Any]]) -> str:
    rows = [
        (
            _record_text(point, "as_of", ""),
            _record_text(point, "overall_score", ""),
            _record_text(point, "confidence", ""),
            _record_text(point, "final_risk_asset_ai_position", ""),
            _record_text(point, "total_risk_asset_budget", ""),
            _record_text(point, "triggered_gate_count", ""),
            _record_text(point, "data_quality_note", ""),
        )
        for point in points
    ]
    return _table(
        ("日期", "总分", "置信度", "最终 AI 仓位", "总风险预算", "Gate 数", "质量/限制"),
        rows,
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


def _read_optional_alert_summary(
    alerts_report_path: Path | None,
    warnings: list[str],
) -> TraceRecord | None:
    if alerts_report_path is None:
        return None
    if not alerts_report_path.exists():
        warnings.append(f"alerts 报告不存在：{alerts_report_path}")
        return None
    text = alerts_report_path.read_text(encoding="utf-8")
    severity_rows = _parse_markdown_table(_markdown_section(text, "## 严重度摘要"))
    severity_counts: dict[str, int] = {"critical": 0, "high": 0, "warning": 0}
    for row in severity_rows:
        level = row.get("等级", "")
        if level in severity_counts:
            severity_counts[level] = _parse_int(row.get("数量"))
    alert_rows = _parse_markdown_table(_markdown_section(text, "## 告警明细"))
    top_alerts = [
        {
            "severity": row.get("等级", ""),
            "category": row.get("类别", ""),
            "source": row.get("来源", ""),
            "title": row.get("标题", ""),
            "trigger_condition": row.get("触发条件", ""),
            "clear_condition": row.get("解除条件", ""),
            "refs": row.get("引用", ""),
        }
        for row in alert_rows[:5]
    ]
    return {
        "path": str(alerts_report_path),
        "status": _metadata_value(text, "状态") or "UNKNOWN",
        "active_count": _parse_int(_metadata_value(text, "活跃告警数")),
        "data_system_count": _parse_int(_metadata_value(text, "data/system")),
        "investment_risk_count": _parse_int(_metadata_value(text, "investment/risk")),
        "severity_counts": severity_counts,
        "top_alerts": top_alerts,
        "production_effect": "none",
    }


def _read_optional_history_points(
    scores_daily_path: Path | None,
    *,
    as_of: date,
    history_limit: int,
    warnings: list[str],
) -> tuple[TraceRecord, ...]:
    if scores_daily_path is None:
        return ()
    if not scores_daily_path.exists():
        warnings.append(f"scores_daily.csv 不存在：{scores_daily_path}")
        return ()
    points: list[TraceRecord] = []
    with scores_daily_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("component") != "overall":
                continue
            row_date = _parse_iso_date(row.get("as_of"))
            if row_date is None or row_date > as_of:
                continue
            points.append(_history_point_from_score_row(row))
    return tuple(points[-history_limit:])


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


def _extract_period_change_summary(daily_text: str) -> str | None:
    section = _markdown_section(daily_text, "## 变化原因树")
    for line in _bullet_lines(section):
        if line.startswith("本期仓位变化："):
            return line.removeprefix("本期仓位变化：").strip()
    return None


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


def _first_bullet(markdown: str) -> str | None:
    bullets = _bullet_lines(markdown)
    return bullets[0] if bullets else None


def _metadata_value(markdown: str, label: str) -> str | None:
    prefix = f"- {label}："
    for line in markdown.splitlines():
        if line.startswith(prefix):
            return line.removeprefix(prefix).strip()
    return None


def _parse_markdown_table(markdown: str) -> list[dict[str, str]]:
    table_lines = [
        line
        for line in markdown.splitlines()
        if line.strip().startswith("|") and "---" not in line
    ]
    if len(table_lines) < 2:
        return []
    headers = _split_markdown_table_row(table_lines[0])
    rows: list[dict[str, str]] = []
    for line in table_lines[1:]:
        cells = _split_markdown_table_row(line)
        if len(cells) != len(headers):
            continue
        rows.append(dict(zip(headers, cells, strict=True)))
    return rows


def _split_markdown_table_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|"):
        return []
    content = stripped[1:-1] if stripped.endswith("|") else stripped[1:]
    cells: list[str] = []
    current: list[str] = []
    escaped = False
    for char in content:
        if char == "|" and not escaped:
            cells.append("".join(current).strip().replace("\\|", "|"))
            current = []
            escaped = False
            continue
        current.append(char)
        if escaped:
            escaped = False
        else:
            escaped = char == "\\"
    cells.append("".join(current).strip().replace("\\|", "|"))
    return cells


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


def _decision_payload(report: EvidenceDashboardReport) -> TraceRecord:
    return {
        "date": report.as_of.isoformat(),
        "action": report.conclusion_card.get("执行动作", "未从日报结论卡提取"),
        "main_conclusion": report.main_conclusion
        or _record_text(_primary_claim(report), "statement", "未找到核心 claim"),
        "final_risk_asset_ai_position": _final_position_summary(report.decision_snapshot),
        "total_risk_asset_budget": report.conclusion_card.get(
            "总风险资产预算",
            _total_risk_asset_budget_summary(report.decision_snapshot),
        ),
        "confidence": _confidence_summary(report.decision_snapshot),
        "data_gate": report.conclusion_card.get("Data Gate", _quality_status(report)),
        "largest_constraint": report.conclusion_card.get(
            "最大限制",
            _triggered_gate_summary(report.decision_snapshot),
        ),
        "change_vs_previous": report.period_change_summary or "未从日报提取上期对比。",
        "market_regime": _market_regime_summary(report.trace_bundle),
        "production_effect": report.production_effect,
    }


def _top_supporting_evidence(report: EvidenceDashboardReport) -> tuple[str, ...]:
    if report.core_reasons:
        return report.core_reasons[:3]
    primary_claim = _primary_claim(report)
    evidence_rows = _linked_evidence(report, primary_claim)
    values = [
        _record_text(record, "summary", "")
        for record in evidence_rows
        if _record_text(record, "summary", "")
    ]
    return tuple(values[:3])


def _top_invalidators(report: EvidenceDashboardReport) -> tuple[str, ...]:
    values: list[str] = []
    if report.main_invalidator:
        values.append(report.main_invalidator)
    values.extend(_triggered_constraint_summaries(report.decision_snapshot)[:2])
    if report.alert_summary is not None:
        for alert in _list_mappings(report.alert_summary.get("top_alerts")):
            severity = _record_text(alert, "severity", "")
            title = _record_text(alert, "title", "")
            if severity in {"critical", "high"} and title:
                values.append(f"{severity}: {title}")
            if len(values) >= 3:
                break
    return tuple(values[:3]) if values else ("未从日报或告警中提取主要 invalidator。",)


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


def _total_risk_asset_budget_summary(snapshot: TraceRecord) -> str:
    positions = _mapping(snapshot.get("positions"))
    return _format_band_record(positions.get("final_total_risk_asset_band"))


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


def _history_point_from_score_row(row: Mapping[str, str]) -> TraceRecord:
    triggered_gates = str(row.get("triggered_position_gates") or "").strip()
    return {
        "as_of": str(row.get("as_of") or ""),
        "overall_score": _format_number_text(row.get("score")),
        "confidence": _format_confidence_text(
            row.get("confidence"),
            row.get("confidence_level"),
        ),
        "final_risk_asset_ai_position": _format_csv_band(
            row.get("final_risk_asset_ai_min"),
            row.get("final_risk_asset_ai_max"),
        ),
        "total_risk_asset_budget": _format_csv_band(
            row.get("final_total_risk_asset_min"),
            row.get("final_total_risk_asset_max"),
        ),
        "triggered_position_gates": triggered_gates or "无",
        "triggered_gate_count": _triggered_gate_count(triggered_gates),
        "data_quality_note": str(row.get("confidence_reasons") or ""),
    }


def _format_csv_band(min_value: object, max_value: object) -> str:
    min_text = _format_csv_percent(min_value)
    max_text = _format_csv_percent(max_value)
    if min_text and max_text:
        return f"{min_text}-{max_text}"
    return "未提供"


def _format_csv_percent(value: object) -> str:
    number = _parse_float(value)
    return "" if number is None else f"{number:.0%}"


def _format_number_text(value: object) -> str:
    number = _parse_float(value)
    return "" if number is None else f"{number:.1f}"


def _format_confidence_text(value: object, level: object) -> str:
    score = _format_number_text(value)
    level_text = "" if level is None else str(level)
    if score and level_text:
        return f"{score}（{level_text}）"
    return score or level_text or "未提供"


def _parse_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value))
    except ValueError:
        return None


def _parse_int(value: object) -> int:
    if value is None:
        return 0
    try:
        return int(str(value).strip())
    except ValueError:
        return 0


def _parse_iso_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None


def _triggered_gate_count(value: str) -> int:
    if not value or value == "无":
        return 0
    normalized = value.replace(",", "、").replace("，", "、").replace(";", "、")
    return len([item for item in normalized.split("、") if item.strip()])


def _sparkline(values: Sequence[str]) -> str:
    numbers = [number for value in values if (number := _parse_float(value)) is not None]
    if not numbers:
        return "未提供"
    if len(set(numbers)) == 1:
        return "━" * len(numbers)
    ticks = "▁▂▃▄▅▆▇█"
    low = min(numbers)
    high = max(numbers)
    span = high - low
    chars = [ticks[round((number - low) / span * (len(ticks) - 1))] for number in numbers]
    return "".join(chars)


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


def _triggered_constraint_summaries(snapshot: TraceRecord) -> tuple[str, ...]:
    gates = _list_mappings(_mapping(snapshot.get("positions")).get("position_gates"))
    triggered = [
        gate
        for gate in gates
        if gate.get("triggered") is True and str(gate.get("gate_id")) != "score_model"
    ]
    return tuple(
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
.decision-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(230px, 1fr));
  gap: 10px;
  margin-bottom: 16px;
}
.decision-columns {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 14px;
}
.header-meta span,
.summary-item,
.decision-metric,
.compact-list {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: var(--surface);
  padding: 10px 12px;
}
.decision-metric span,
.summary-item span {
  display: block;
  color: var(--muted);
  font-size: 12px;
}
.decision-metric strong,
.summary-item strong {
  display: block;
  overflow-wrap: anywhere;
  font-size: 15px;
}
.compact-list h3 {
  margin-bottom: 8px;
}
.compact-list ol {
  margin: 0;
  padding-left: 20px;
}
.compact-list li {
  margin-bottom: 6px;
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
