from __future__ import annotations

import html
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "reader_brief"
PRODUCTION_EFFECT = "none"
QUALITY_REPORT_TYPE = "reader_brief_quality"


def default_reader_brief_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_{as_of.isoformat()}.json"


def default_reader_brief_html_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_{as_of.isoformat()}.html"


def default_reader_brief_quality_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_quality_{as_of.isoformat()}.json"


def default_reader_brief_quality_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"reader_brief_quality_{as_of.isoformat()}.md"


def build_reader_brief_payload(
    *,
    as_of: date,
    reports_dir: Path,
    decision_snapshot_path: Path,
    calculation_explainers_path: Path | None = None,
    daily_decision_summary_path: Path | None = None,
    evidence_dashboard_json_path: Path | None = None,
    daily_task_dashboard_json_path: Path | None = None,
    daily_report_path: Path | None = None,
    trace_bundle_path: Path | None = None,
    score_change_attribution_path: Path | None = None,
    research_governance_summary_path: Path | None = None,
    report_index_path: Path | None = None,
    documentation_contract_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
) -> dict[str, Any]:
    snapshot = _read_required_json(decision_snapshot_path, "decision_snapshot")
    calculation_explainers = _read_optional_json(calculation_explainers_path)
    daily_decision_summary = _read_optional_json(daily_decision_summary_path)
    evidence_dashboard = _read_optional_json(evidence_dashboard_json_path)
    daily_task_dashboard = _read_optional_json(daily_task_dashboard_json_path)
    score_change_attribution = _read_optional_json(score_change_attribution_path)
    research_governance_summary = _read_optional_json(research_governance_summary_path)
    report_index = _read_optional_json(report_index_path)
    documentation_contract = _read_optional_json(documentation_contract_path)
    warnings = _input_warnings(
        {
            "calculation_explainers": calculation_explainers_path,
            "daily_decision_summary": daily_decision_summary_path,
            "evidence_dashboard": evidence_dashboard_json_path,
            "daily_task_dashboard": daily_task_dashboard_json_path,
            "daily_report": daily_report_path,
            "trace_bundle": trace_bundle_path,
            "score_change_attribution": score_change_attribution_path,
            "research_governance_summary": research_governance_summary_path,
            "report_index": report_index_path,
            "documentation_contract": documentation_contract_path,
        }
    )
    source_inputs = {
        "decision_snapshot": _source_input("decision_snapshot", decision_snapshot_path, True),
        "calculation_explainers": _source_input(
            "calculation_explainers",
            calculation_explainers_path,
            calculation_explainers_path is not None and calculation_explainers_path.exists(),
        ),
        "daily_decision_summary": _source_input(
            "daily_decision_summary",
            daily_decision_summary_path,
            daily_decision_summary_path is not None and daily_decision_summary_path.exists(),
        ),
        "evidence_dashboard": _source_input(
            "evidence_dashboard",
            evidence_dashboard_json_path,
            evidence_dashboard_json_path is not None and evidence_dashboard_json_path.exists(),
        ),
        "daily_task_dashboard": _source_input(
            "daily_task_dashboard",
            daily_task_dashboard_json_path,
            daily_task_dashboard_json_path is not None and daily_task_dashboard_json_path.exists(),
        ),
        "daily_report": _source_input(
            "daily_report",
            daily_report_path,
            daily_report_path is not None and daily_report_path.exists(),
        ),
        "trace_bundle": _source_input(
            "trace_bundle",
            trace_bundle_path,
            trace_bundle_path is not None and trace_bundle_path.exists(),
        ),
        "score_change_attribution": _source_input(
            "score_change_attribution",
            score_change_attribution_path,
            score_change_attribution_path is not None and score_change_attribution_path.exists(),
        ),
        "research_governance_summary": _source_input(
            "research_governance_summary",
            research_governance_summary_path,
            research_governance_summary_path is not None
            and research_governance_summary_path.exists(),
        ),
        "report_index": _source_input(
            "report_index",
            report_index_path,
            report_index_path is not None and report_index_path.exists(),
        ),
        "documentation_contract": _source_input(
            "documentation_contract",
            documentation_contract_path,
            documentation_contract_path is not None and documentation_contract_path.exists(),
        ),
    }

    run_context = _run_context(
        as_of=as_of,
        snapshot=snapshot,
        daily_decision_summary=daily_decision_summary,
        daily_task_dashboard=daily_task_dashboard,
    )
    executive_decision = _executive_decision(
        snapshot=snapshot,
        daily_decision_summary=daily_decision_summary,
        evidence_dashboard=evidence_dashboard,
        calculation_explainers=calculation_explainers,
    )
    score_change_summary = _score_change_attribution_summary(score_change_attribution)
    report_index_summary = _report_index_summary(report_index)
    governance_summary = _backtest_shadow_governance(
        daily_decision_summary=daily_decision_summary,
        daily_task_dashboard=daily_task_dashboard,
        research_governance_summary=research_governance_summary,
    )
    manual_review_queue = _manual_review_queue(
        snapshot=snapshot,
        daily_decision_summary=daily_decision_summary,
        report_index=report_index,
        research_governance_summary=research_governance_summary,
        documentation_contract=documentation_contract,
    )
    component_explainability = _component_score_explainability(
        snapshot=snapshot,
        calculation_explainers=calculation_explainers,
    )
    gate_ladder = _binding_gate_ladder(
        snapshot=snapshot,
        calculation_explainers=calculation_explainers,
    )
    task_cadence_calendar = _task_cadence_calendar(
        report_index,
        report_registry_path=report_registry_path,
    )
    missing_artifact_impact = _missing_artifact_impact(
        source_inputs=source_inputs,
        report_index_summary=report_index_summary,
        task_cadence_calendar=task_cadence_calendar,
    )
    contribution_summary = _contribution_summary(
        component_explainability=component_explainability,
        gate_ladder=gate_ladder,
        decision=executive_decision,
    )
    report_navigation = _report_navigation(
        reports_dir=reports_dir,
        source_inputs=source_inputs,
        report_index=report_index,
        missing_artifact_impact=missing_artifact_impact,
    )
    report_navigation_groups = _report_navigation_groups(report_navigation)
    narrative_summary = _narrative_executive_summary(
        run_context=run_context,
        decision=executive_decision,
        score_changes=score_change_summary,
        contribution_summary=contribution_summary,
        manual_review_queue=manual_review_queue,
        missing_artifact_impact=missing_artifact_impact,
    )
    quality_status = _reader_brief_status(
        warnings=warnings,
        missing_artifact_impact=missing_artifact_impact,
        decision=executive_decision,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": quality_status,
        "production_effect": PRODUCTION_EFFECT,
        "reader_entry_role": "daily_reading_home",
        "source_inputs": source_inputs,
        "warnings": warnings,
        "run_context": run_context,
        "narrative_executive_summary": narrative_summary,
        "executive_decision": executive_decision,
        "market_situation_snapshot": _market_situation_snapshot(
            evidence_dashboard=evidence_dashboard,
            snapshot=snapshot,
        ),
        "score_to_position_funnel": _score_to_position_funnel(
            snapshot=snapshot,
            calculation_explainers=calculation_explainers,
            source_inputs=source_inputs,
        ),
        "score_change_attribution_summary": score_change_summary,
        "report_index_summary": report_index_summary,
        "missing_limited_artifact_impact": missing_artifact_impact,
        "task_cadence_calendar": task_cadence_calendar,
        "documentation_contract_summary": _documentation_contract_summary(
            documentation_contract,
        ),
        "contribution_summary": contribution_summary,
        "component_score_explainability": component_explainability,
        "binding_gate_ladder": gate_ladder,
        "data_quality_pit_safety": _data_quality_pit_safety(
            snapshot=snapshot,
            daily_decision_summary=daily_decision_summary,
            report_index_summary=report_index_summary,
        ),
        "backtest_shadow_governance": governance_summary,
        "manual_review_queue": manual_review_queue,
        "executive_summary": _executive_summary(
            run_context=run_context,
            decision=executive_decision,
            score_changes=score_change_summary,
            report_index_summary=report_index_summary,
            governance_summary=governance_summary,
            manual_review_queue=manual_review_queue,
        ),
        "appendix_links": _appendix_links(reports_dir, source_inputs),
        "report_navigation": report_navigation,
        "report_navigation_groups": report_navigation_groups,
    }
    return payload


def write_reader_brief_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_reader_brief_html(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_reader_brief_html(payload), encoding="utf-8")
    return output_path


def build_reader_brief_quality_payload(
    *,
    reader_brief_payload: Mapping[str, Any],
    reader_brief_json_path: Path | None = None,
    reader_brief_html_path: Path | None = None,
) -> dict[str, Any]:
    missing_impact = _mapping(reader_brief_payload.get("missing_limited_artifact_impact"))
    manual_queue = _mapping(reader_brief_payload.get("manual_review_queue"))
    checks = [
        _quality_check(
            "narrative_executive_summary",
            bool(_mapping(reader_brief_payload.get("narrative_executive_summary"))),
            "首屏 narrative summary 存在。",
        ),
        _quality_check(
            "missing_artifact_impact",
            bool(_records(missing_impact.get("items"))) or missing_impact.get("status") == "OK",
            "缺失/受限 artifact 影响层存在。",
        ),
        _quality_check(
            "manual_review_groups",
            bool(_records(manual_queue.get("groups"))),
            "Manual Review Queue 已按 severity 分组。",
        ),
        _quality_check(
            "contribution_summary",
            bool(_mapping(reader_brief_payload.get("contribution_summary"))),
            "Component contribution summary 存在。",
        ),
        _quality_check(
            "market_minimum_panel",
            bool(
                _mapping(reader_brief_payload.get("market_situation_snapshot")).get(
                    "market_price_panel_status"
                )
            ),
            "Market Situation 披露 price panel 状态。",
        ),
        _quality_check(
            "grouped_report_navigation",
            bool(
                _records(
                    _mapping(reader_brief_payload.get("report_navigation_groups")).get("groups")
                )
            ),
            "Report Navigation 已按目的分组。",
        ),
        _quality_check(
            "production_effect_none",
            _text(reader_brief_payload.get("production_effect")) == PRODUCTION_EFFECT,
            "Reader Brief production_effect=none。",
        ),
    ]
    failed_checks = [check for check in checks if check["status"] == "FAIL"]
    blocking = _int(missing_impact.get("blocking_count"))
    important = _int(missing_impact.get("important_count"))
    source_status = _text(reader_brief_payload.get("status"), "UNKNOWN")
    if failed_checks or source_status == "FAILED":
        quality_status = "FAILED"
    elif blocking or important or source_status == "LIMITED_READER_CONTEXT":
        quality_status = "LIMITED_READER_CONTEXT"
    elif source_status == "PASS_WITH_WARNINGS":
        quality_status = "PASS_WITH_WARNINGS"
    else:
        quality_status = "OK"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": QUALITY_REPORT_TYPE,
        "as_of": _text(reader_brief_payload.get("as_of"), "UNKNOWN"),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": quality_status,
        "source_reader_brief_status": source_status,
        "production_effect": PRODUCTION_EFFECT,
        "source_artifacts": {
            "reader_brief_json": (
                "" if reader_brief_json_path is None else str(reader_brief_json_path)
            ),
            "reader_brief_html": (
                "" if reader_brief_html_path is None else str(reader_brief_html_path)
            ),
        },
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(failed_checks),
            "blocking_artifact_count": blocking,
            "important_artifact_count": important,
            "manual_review_count": len(_records(manual_queue.get("items"))),
        },
        "checks": checks,
        "missing_limited_artifact_impact": missing_impact,
        "manual_review_queue_groups": _records(manual_queue.get("groups")),
        "methodology": {
            "mode": "read_existing_reader_brief_only",
            "does_not_run_upstream_commands": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_reader_brief_quality_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_reader_brief_quality_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_reader_brief_quality_markdown(payload), encoding="utf-8")
    return output_path


def render_reader_brief_quality_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Reader Brief Quality {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('status'), 'UNKNOWN')}",
        f"- Reader Brief 状态：{_text(payload.get('source_reader_brief_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checks：{summary.get('check_count')}，failed：{summary.get('failed_check_count')}",
        (
            f"- blocking artifact：{summary.get('blocking_artifact_count')}，"
            f"important artifact：{summary.get('important_artifact_count')}"
        ),
        "",
        "## Checks",
        "",
        "|check_id|status|message|",
        "|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_text(check.get('check_id'))}|{_text(check.get('status'))}|"
            f"{_text(check.get('message'))}|"
        )
    lines.extend(
        [
            "",
            "## Missing / Limited Artifact Impact",
            "",
            "|artifact_id|status|impact_level|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for item in _records(_mapping(payload.get("missing_limited_artifact_impact")).get("items")):
        lines.append(
            f"|{_text(item.get('artifact_id'))}|{_text(item.get('status'))}|"
            f"{_text(item.get('impact_level'))}|{_text(item.get('recommended_action'))}|"
        )
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "本质量报告只读取既有 Reader Brief JSON，不运行上游 scoring、backtest、"
            "shadow、SEC PIT、weight 或 docs 任务。",
            "",
        ]
    )
    return "\n".join(lines)


def render_reader_brief_html(payload: Mapping[str, Any]) -> str:
    as_of = _text(payload.get("as_of"), "UNKNOWN")
    status = _text(payload.get("status"), "UNKNOWN")
    run_context = _mapping(payload.get("run_context"))
    narrative_summary = _mapping(payload.get("narrative_executive_summary"))
    executive_summary = _mapping(payload.get("executive_summary"))
    decision = _mapping(payload.get("executive_decision"))
    market = _mapping(payload.get("market_situation_snapshot"))
    funnel = _records(_mapping(payload.get("score_to_position_funnel")).get("steps"))
    score_changes = _mapping(payload.get("score_change_attribution_summary"))
    report_index = _mapping(payload.get("report_index_summary"))
    missing_impact = _mapping(payload.get("missing_limited_artifact_impact"))
    cadence_calendar = _mapping(payload.get("task_cadence_calendar"))
    documentation_contract = _mapping(payload.get("documentation_contract_summary"))
    contribution_summary = _mapping(payload.get("contribution_summary"))
    components = _records(_mapping(payload.get("component_score_explainability")).get("components"))
    gates = _records(_mapping(payload.get("binding_gate_ladder")).get("gates"))
    quality = _mapping(payload.get("data_quality_pit_safety"))
    governance = _mapping(payload.get("backtest_shadow_governance"))
    manual_review = _mapping(payload.get("manual_review_queue"))
    manual_queue = _records(manual_review.get("items"))
    navigation = _records(payload.get("report_navigation"))
    navigation_groups = _mapping(payload.get("report_navigation_groups"))
    appendix = _records(payload.get("appendix_links"))

    html_parts = [
        "<!doctype html>",
        '<html lang="zh-CN">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>Reader Brief {html.escape(as_of)}</title>",
        f"<style>{_css()}</style>",
        "</head>",
        "<body>",
        "<main>",
        f"<header><p>Reader Brief</p><h1>{html.escape(as_of)}</h1>"
        f"<span>Status: {html.escape(status)}</span></header>",
        _section(
            "Executive Summary",
            _narrative_summary_html(narrative_summary)
            + _definition_table(
                [
                    ("today_conclusion", narrative_summary.get("today_conclusion")),
                    ("why_this_conclusion", narrative_summary.get("why_this_conclusion")),
                    ("binding_constraint", narrative_summary.get("binding_constraint")),
                    ("manual_review_summary", narrative_summary.get("manual_review_summary")),
                    (
                        "production_effect_statement",
                        narrative_summary.get("production_effect_statement"),
                    ),
                ]
            )
            + _definition_table(
                [
                    ("market_regime", executive_summary.get("market_regime_summary")),
                    ("top_model_conclusion", executive_summary.get("top_model_conclusion")),
                    ("major_score_change", executive_summary.get("major_score_change")),
                    ("report_freshness", executive_summary.get("report_freshness")),
                    ("governance_status", executive_summary.get("governance_status")),
                    ("manual_review_count", executive_summary.get("manual_review_count")),
                    ("production_effect", executive_summary.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Run Context",
            _definition_table(
                [
                    ("run_id", run_context.get("run_id")),
                    ("market_regime", run_context.get("market_regime")),
                    ("generated_at", payload.get("generated_at")),
                    ("production_effect", payload.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Core Decision",
            _definition_table(
                [
                    ("执行动作", decision.get("action")),
                    ("最终 AI 仓位", decision.get("final_risk_asset_ai_position")),
                    ("总风险资产预算", decision.get("total_risk_asset_budget")),
                    ("判断置信度", decision.get("confidence")),
                    ("Data Gate", decision.get("data_gate")),
                    ("最大限制", decision.get("binding_gate_label")),
                    ("人工复核", decision.get("manual_review_required")),
                    (
                        "交易边界",
                        (
                            "不是实盘交易指令"
                            if decision.get("not_trade_instruction") is True
                            else decision.get("not_trade_instruction")
                        ),
                    ),
                ]
            ),
        ),
        _section("Market Situation", _definition_table(list(market.items()))),
        _section("Score & Decision Funnel", _funnel_details(funnel)),
        _section(
            "Score Change Attribution",
            _definition_table(
                [
                    ("availability", score_changes.get("availability")),
                    ("status", score_changes.get("status")),
                    ("comparison_window", score_changes.get("comparison_window")),
                    ("overall_score_delta", score_changes.get("overall_score_delta")),
                    ("final_position_max_delta", score_changes.get("final_position_max_delta")),
                ]
            )
            + _records_table(_records(score_changes.get("drivers"))),
        ),
        _section(
            "Report Index Freshness",
            _definition_table(
                [
                    ("availability", report_index.get("availability")),
                    ("status", report_index.get("status")),
                    ("report_count", report_index.get("report_count")),
                    ("missing_count", report_index.get("missing_count")),
                    ("stale_count", report_index.get("stale_count")),
                    ("required_missing_count", report_index.get("required_missing_count")),
                ]
            )
            + _records_table(_records(report_index.get("problem_reports"))),
        ),
        _section(
            "Missing / Limited Artifact Impact",
            _definition_table(
                [
                    ("status", missing_impact.get("status")),
                    ("blocking_count", missing_impact.get("blocking_count")),
                    ("important_count", missing_impact.get("important_count")),
                    ("production_effect", missing_impact.get("production_effect")),
                ]
            )
            + _artifact_impact_table(_records(missing_impact.get("items"))),
        ),
        _section(
            "Task Cadence Calendar",
            _definition_table(
                [
                    ("availability", cadence_calendar.get("availability")),
                    ("status", cadence_calendar.get("status")),
                    ("source", cadence_calendar.get("source")),
                    ("production_effect", cadence_calendar.get("production_effect")),
                ]
            )
            + _cadence_calendar_tables(cadence_calendar),
        ),
        _section(
            "Contribution Summary",
            _definition_table(
                [
                    (
                        "top_positive_contributors",
                        contribution_summary.get("top_positive_contributors"),
                    ),
                    (
                        "top_negative_or_zero_contributors",
                        contribution_summary.get("top_negative_or_zero_contributors"),
                    ),
                    (
                        "largest_weighted_contribution",
                        contribution_summary.get("largest_weighted_contribution"),
                    ),
                    ("largest_drag", contribution_summary.get("largest_drag")),
                    (
                        "binding_gate_vs_score_explanation",
                        contribution_summary.get("binding_gate_vs_score_explanation"),
                    ),
                    ("production_effect", contribution_summary.get("production_effect")),
                ]
            ),
        ),
        _section("Component Explainability", _records_table(components)),
        _section("Binding Gate Ladder", _records_table(gates)),
        _section("Data Quality & PIT Safety", _definition_table(list(quality.items()))),
        _section("Backtest / Shadow / Governance", _definition_table(list(governance.items()))),
        _section("Documentation Contract", _definition_table(list(documentation_contract.items()))),
        _section("Manual Review Queue", _manual_review_groups_html(manual_review, manual_queue)),
        _section("Report Navigation", _navigation_groups_html(navigation_groups, navigation)),
        _section("Appendix Links", _records_table(appendix)),
        "</main>",
        "</body>",
        "</html>",
    ]
    return "\n".join(html_parts)


def _run_context(
    *,
    as_of: date,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    daily_task_dashboard: Mapping[str, Any],
) -> dict[str, Any]:
    market_regime = _mapping(snapshot.get("market_regime"))
    summary_run_id = _text(daily_decision_summary.get("run_id"))
    task_summary = _mapping(daily_task_dashboard.get("summary"))
    return {
        "as_of": as_of.isoformat(),
        "run_id": summary_run_id or _text(daily_task_dashboard.get("run_id"), "UNKNOWN"),
        "market_regime": _text(market_regime.get("regime_id"), "ai_after_chatgpt"),
        "market_regime_start": _text(market_regime.get("start_date"), "2022-12-01"),
        "visibility_cutoff": _text(
            daily_task_dashboard.get("visibility_cutoff"),
            _text(task_summary.get("visibility_cutoff"), "UNKNOWN"),
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _executive_decision(
    *,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    evidence_dashboard: Mapping[str, Any],
    calculation_explainers: Mapping[str, Any],
) -> dict[str, Any]:
    positions = _mapping(snapshot.get("positions"))
    scores = _mapping(snapshot.get("scores"))
    final_band = _mapping(positions.get("final_risk_asset_ai_band"))
    total_band = _mapping(positions.get("final_total_risk_asset_band"))
    investment = _mapping(daily_decision_summary.get("investment_conclusion"))
    data_gate = _mapping(daily_decision_summary.get("data_gate"))
    evidence_decision = _mapping(evidence_dashboard.get("decision"))
    binding_gate = _binding_gate_from_calculation(
        calculation_explainers
    ) or _binding_gate_from_snapshot(snapshot)
    manual_items = _records(snapshot.get("manual_review"))
    manual_required = any(_text(item.get("status")) not in {"", "PASS"} for item in manual_items)
    return {
        "action": _text(
            investment.get("action_bias"),
            _text(evidence_decision.get("action"), "UNKNOWN"),
        ),
        "final_risk_asset_ai_position": _text(
            investment.get("position_band"),
            _format_band(final_band),
        ),
        "total_risk_asset_budget": _text(
            evidence_decision.get("total_risk_asset_budget"),
            _format_band(total_band),
        ),
        "confidence": _text(
            investment.get("confidence"),
            _confidence_summary(scores),
        ),
        "data_gate": _text(data_gate.get("status"), _quality_status(snapshot)),
        "binding_gate_id": _text(binding_gate.get("gate_id")) if binding_gate else "UNKNOWN",
        "binding_gate_label": _text(binding_gate.get("label")) if binding_gate else "UNKNOWN",
        "binding_gate_reason": _text(binding_gate.get("reason")) if binding_gate else "",
        "manual_review_required": manual_required,
        "not_trade_instruction": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _market_situation_snapshot(
    *,
    evidence_dashboard: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(evidence_dashboard.get("decision"))
    dashboard_quality = _mapping(evidence_dashboard.get("quality"))
    quality = _mapping(snapshot.get("quality"))
    benchmark_proxy = _text(decision.get("benchmark_proxy")) or _text(
        decision.get("benchmark_direction"),
        "MISSING",
    )
    ai_sector_proxy = _text(decision.get("ai_sector_proxy"), "MISSING")
    risk_proxy = _text(decision.get("risk_proxy"), "MISSING")
    liquidity_proxy = _text(decision.get("liquidity_proxy"), "MISSING")
    price_panel_status = (
        "AVAILABLE"
        if any(value != "MISSING" for value in (benchmark_proxy, ai_sector_proxy, risk_proxy))
        else "MISSING_PRICE_PANEL"
    )
    return {
        "availability": "LIMITED",
        "risk_regime_label": _text(decision.get("market_regime"), "not_available"),
        "benchmark_proxy": benchmark_proxy,
        "ai_sector_proxy": ai_sector_proxy,
        "risk_proxy": risk_proxy,
        "liquidity_proxy": liquidity_proxy,
        "market_price_panel_status": price_panel_status,
        "market_data_status": _text(
            dashboard_quality.get("market_data_status"),
            _text(quality.get("market_data_status"), "UNKNOWN"),
        ),
        "feature_status": _text(quality.get("feature_status"), "UNKNOWN"),
        "recommended_action": (
            "generate_market_panel_in_future_task"
            if price_panel_status == "MISSING_PRICE_PANEL"
            else "review_price_panel_sources"
        ),
        "limitation": (
            "当前 Reader Brief 只读现有 daily artifacts。若 market_price_panel_status="
            "MISSING_PRICE_PANEL，则今日不披露 benchmark/AI sector/risk proxy 实际涨跌，"
            "不应把 Market Situation 解读为完整市场复盘。"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _score_to_position_funnel(
    *,
    snapshot: Mapping[str, Any],
    calculation_explainers: Mapping[str, Any],
    source_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _mapping(calculation_explainers.get("metrics"))
    positions = _mapping(snapshot.get("positions"))
    scores = _mapping(snapshot.get("scores"))
    steps = [
        _funnel_step(
            "component_score",
            metrics,
            _text(len(_records(scores.get("components")))),
            "decision_snapshot.scores.components",
            source_inputs,
        ),
        _funnel_step(
            "overall_score",
            metrics,
            _text(scores.get("overall_score")),
            "scores",
            source_inputs,
        ),
        _funnel_step(
            "model_position_band",
            metrics,
            _format_band(_mapping(positions.get("model_risk_asset_ai_band"))),
            "decision_snapshot.positions",
            source_inputs,
        ),
        _funnel_step(
            "confidence_adjusted_position",
            metrics,
            _format_band(_mapping(positions.get("confidence_adjusted_risk_asset_ai_band"))),
            "decision_snapshot.positions",
            source_inputs,
        ),
        _funnel_step(
            "position_gate",
            metrics,
            _text(len(_records(positions.get("position_gates")))),
            "decision_snapshot.positions.position_gates",
            source_inputs,
        ),
        _funnel_step(
            "final_position_band",
            metrics,
            _format_band(_mapping(positions.get("final_risk_asset_ai_band"))),
            "decision_snapshot.positions.final_risk_asset_ai_band",
            source_inputs,
        ),
    ]
    return {"status": "AVAILABLE", "steps": steps}


def _component_score_explainability(
    *,
    snapshot: Mapping[str, Any],
    calculation_explainers: Mapping[str, Any],
) -> dict[str, Any]:
    components = _records(_mapping(snapshot.get("scores")).get("components"))
    contribution_rows = _records(
        _mapping(_mapping(calculation_explainers.get("metrics")).get("overall_score")).get(
            "input_values"
        )
    )
    contribution_by_component = {
        _text(item.get("component")): item for item in contribution_rows if item.get("component")
    }
    rows: list[dict[str, Any]] = []
    for component in components:
        component_id = _text(component.get("component"))
        contribution = contribution_by_component.get(component_id, {})
        rows.append(
            {
                "component": component_id,
                "score": component.get("score"),
                "effective_weight": contribution.get("effective_weight"),
                "contribution_to_overall_score": contribution.get("contribution_to_overall_score"),
                "coverage": component.get("coverage"),
                "confidence": component.get("confidence"),
                "source_type": component.get("source_type"),
                "reason": component.get("reason"),
            }
        )
    return {"status": "AVAILABLE" if rows else "MISSING", "components": rows}


def _contribution_summary(
    *,
    component_explainability: Mapping[str, Any],
    gate_ladder: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    components = _records(component_explainability.get("components"))
    scored = [
        (
            _text(component.get("component"), "UNKNOWN"),
            _float_or_none(component.get("contribution_to_overall_score")),
            _float_or_none(component.get("score")),
        )
        for component in components
    ]
    positive = sorted(
        [item for item in scored if item[1] is not None and item[1] > 0],
        key=lambda item: item[1] or 0,
        reverse=True,
    )
    negative_or_zero = sorted(
        [
            item
            for item in scored
            if (item[1] is not None and item[1] <= 0) or (item[2] is not None and item[2] <= 0)
        ],
        key=lambda item: item[1] if item[1] is not None else -1,
    )
    if not negative_or_zero:
        low_scores = sorted(
            [item for item in scored if item[2] is not None],
            key=lambda item: item[2] or 0,
        )
        negative_or_zero = low_scores[:2]
    largest_positive = positive[0] if positive else None
    largest_drag = negative_or_zero[0] if negative_or_zero else None
    binding_gate = _text(gate_ladder.get("binding_gate_id"), _text(decision.get("binding_gate_id")))
    final_position = _text(decision.get("final_risk_asset_ai_position"), "UNKNOWN")
    return {
        "status": "AVAILABLE" if components else "MISSING",
        "top_positive_contributors": [item[0] for item in positive[:3]],
        "top_negative_or_zero_contributors": [item[0] for item in negative_or_zero[:3]],
        "largest_weighted_contribution": (
            f"{largest_positive[0]}={largest_positive[1]:.2f}"
            if largest_positive and largest_positive[1] is not None
            else "MISSING"
        ),
        "largest_drag": (
            f"{largest_drag[0]}={largest_drag[1]:.2f}"
            if largest_drag and largest_drag[1] is not None
            else (_text(largest_drag[0]) if largest_drag else "MISSING")
        ),
        "binding_gate_vs_score_explanation": (
            f"最终仓位 {final_position} 由 binding gate={binding_gate} 约束；"
            "Reader Brief 不把综合分数自动解读为可执行仓位。"
            if binding_gate and binding_gate != "UNKNOWN"
            else (
                "当前缺少 binding gate 解释，需打开 calculation_explainers / "
                "decision snapshot 审计。"
            )
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _score_change_attribution_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "comparison_window": "MISSING",
            "overall_score_delta": "MISSING",
            "final_position_max_delta": "MISSING",
            "drivers": [],
        }
    window = _mapping(payload.get("comparison_window"))
    overall = _mapping(payload.get("overall_score_delta"))
    position = _mapping(payload.get("position_attribution"))
    top_changes = _mapping(payload.get("top_changes"))
    drivers: list[dict[str, Any]] = []
    for bucket in (
        "positive_contribution_drivers",
        "negative_contribution_drivers",
        "weight_changes",
        "coverage_changes",
    ):
        for item in _records(top_changes.get(bucket)):
            component = _text(item.get("component"), "UNKNOWN")
            value = next((value for key, value in item.items() if key != "component"), "")
            drivers.append({"bucket": bucket, "driver": component, "value": value})
    for item in _records(top_changes.get("gate_changes")):
        drivers.append(
            {
                "bucket": "gate_changes",
                "driver": _text(item.get("gate_id"), "UNKNOWN"),
                "value": _text(item.get("change_flags")),
            }
        )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "comparison_window": (
            f"{_text(window.get('previous_signal_date'), 'UNKNOWN')} -> "
            f"{_text(window.get('current_signal_date'), _text(payload.get('as_of'), 'UNKNOWN'))}"
        ),
        "overall_score_delta": overall.get("delta"),
        "final_position_max_delta": position.get("final_max_delta"),
        "drivers": drivers,
    }


def _report_index_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "report_count": 0,
            "missing_count": 0,
            "stale_count": 0,
            "required_missing_count": 0,
            "production_effect": PRODUCTION_EFFECT,
            "problem_reports": [],
            "limitation": "report_index artifact missing; Reader Brief 不补造 freshness 结论。",
        }
    summary = _mapping(payload.get("summary"))
    problem_reports = [
        {
            "report_id": _text(report.get("report_id")),
            "title": _text(report.get("title")),
            "freshness_status": _text(report.get("freshness_status"), "UNKNOWN"),
            "owner_action": _text(report.get("owner_action")),
        }
        for report in _records(payload.get("reports"))
        if _text(report.get("freshness_status")) in {"MISSING", "STALE"}
    ]
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "report_count": _int(summary.get("report_count")),
        "missing_count": _int(summary.get("missing_count")),
        "stale_count": _int(summary.get("stale_count")),
        "required_missing_count": _int(summary.get("required_missing_count")),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "problem_reports": problem_reports[:8],
        "limitation": "Reader Brief 只展示 report_index 的 freshness 摘要和 stale/missing 报告。",
    }


_ARTIFACT_IMPACT_POLICY: dict[str, tuple[str, str, str, str]] = {
    "decision_snapshot": (
        "BLOCKING",
        "缺少 decision snapshot 时 Reader Brief 无法形成当日核心结论。",
        "阻断今日 Reader Brief 结论。",
        "重新生成或提供当日 decision snapshot。",
    ),
    "daily_decision_summary": (
        "IMPORTANT",
        "缺少 daily decision summary 会降低首屏结论和 data gate 可读性。",
        "不直接改写 snapshot 结论，但会降低读者对 action/data gate 的信心。",
        "运行或补齐 daily task dashboard 生成的 daily_decision_summary。",
    ),
    "calculation_explainers": (
        "IMPORTANT",
        "缺少 calculation explainers 会让关键数字缺少公式、输入和 PIT 解释。",
        "不重算 score，但限制计算审计能力。",
        "运行 aits reports calculation-explainers。",
    ),
    "evidence_dashboard": (
        "IMPORTANT",
        "缺少 evidence dashboard 会削弱结论证据下钻。",
        "不覆盖最终决策，但减少证据链可见性。",
        "运行 aits reports dashboard 或打开 trace bundle。",
    ),
    "daily_task_dashboard": (
        "IMPORTANT",
        "缺少 daily task dashboard 会限制 backtest/shadow/SEC PIT/weight 状态聚合。",
        "不改变 production_effect，但可能遗漏治理 warning。",
        "运行 aits reports daily-tasks。",
    ),
    "daily_report": (
        "IMPORTANT",
        "缺少 Markdown 日报会减少面向人的完整叙事和风险注释。",
        "不改变 snapshot 决策，但减少解释上下文。",
        "运行 aits score-daily 或打开 canonical run bundle。",
    ),
    "trace_bundle": (
        "IMPORTANT",
        "缺少 trace bundle 会削弱 source/evidence audit trail。",
        "不重算结论，但降低可追溯性。",
        "重新生成 daily score trace bundle。",
    ),
    "score_change_attribution": (
        "IMPORTANT",
        "缺少 score change attribution 时读者难以判断今天相对上一期为何变化。",
        "不改变今日分数，但限制变化原因解释。",
        "运行 aits reports score-change-attribution。",
    ),
    "research_governance_summary": (
        "IMPORTANT",
        "缺少 research governance summary 会分散 backtest/shadow/weight/SEC PIT 状态。",
        "不允许把 observe-only 结果当作 production，但会降低治理可读性。",
        "运行 aits reports research-governance-summary。",
    ),
    "report_index": (
        "OPTIONAL",
        "缺少 report index 时 freshness 摘要来自 registry fallback，缺少真实 last-run 状态。",
        "不改变今日决策，但限制报告新鲜度判断。",
        "运行 aits reports index。",
    ),
    "documentation_contract": (
        "OPTIONAL",
        "缺少 documentation contract 时无法证明 registry 与 artifact catalog 完全同步。",
        "不改变今日投资结论，但留下文档治理缺口。",
        "运行 aits docs report-contract。",
    ),
}


def _missing_artifact_impact(
    *,
    source_inputs: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
    task_cadence_calendar: Mapping[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for artifact_id, source in source_inputs.items():
        record = _mapping(source)
        status = _text(record.get("availability"), "UNKNOWN")
        if status == "AVAILABLE":
            continue
        impact, reader_impact, decision_impact, action = _ARTIFACT_IMPACT_POLICY.get(
            artifact_id,
            (
                "INFO",
                "该 artifact 缺失，但当前 Reader Brief 未配置专门影响说明。",
                "不直接改变今日决策。",
                "确认该 artifact 是否仍应进入 Reader Brief。",
            ),
        )
        items.append(
            {
                "artifact_id": artifact_id,
                "short_name": _short_path(record.get("path")),
                "status": status,
                "impact_level": impact,
                "reader_impact": reader_impact,
                "decision_impact": decision_impact,
                "recommended_action": action,
                "production_effect": PRODUCTION_EFFECT,
                "full_path": _text(record.get("path")),
            }
        )
    if _text(task_cadence_calendar.get("source")) == "registry_fallback":
        items.append(
            {
                "artifact_id": "task_cadence_calendar",
                "short_name": "config/report_registry.yaml",
                "status": "REGISTRY_FALLBACK",
                "impact_level": "IMPORTANT",
                "reader_impact": (
                    "缺少 runtime report_index，cadence calendar 只能展示 registry 预期项。"
                ),
                "decision_impact": (
                    "不改变今日决策，但 next expected run / last run 不是运行时事实。"
                ),
                "recommended_action": "运行 aits reports index 后重新生成 Reader Brief。",
                "production_effect": PRODUCTION_EFFECT,
                "full_path": _text(task_cadence_calendar.get("registry_path")),
            }
        )
    if _int(report_index_summary.get("required_missing_count")):
        items.append(
            {
                "artifact_id": "required_daily_reading_reports",
                "short_name": "report_index required reports",
                "status": "REQUIRED_MISSING",
                "impact_level": "BLOCKING",
                "reader_impact": "一个或多个 daily reading 必需报告缺失。",
                "decision_impact": "Reader Brief 结论应视为受限，需先补齐必需报告。",
                "recommended_action": (
                    "打开 Report Index Freshness 并补跑 required_for_daily_reading 报告。"
                ),
                "production_effect": PRODUCTION_EFFECT,
                "full_path": "",
            }
        )
    blocking = len([item for item in items if item["impact_level"] == "BLOCKING"])
    important = len([item for item in items if item["impact_level"] == "IMPORTANT"])
    return {
        "status": "OK" if not items else "IMPACT_REVIEW_REQUIRED",
        "blocking_count": blocking,
        "important_count": important,
        "optional_count": len([item for item in items if item["impact_level"] == "OPTIONAL"]),
        "info_count": len([item for item in items if item["impact_level"] == "INFO"]),
        "production_effect": PRODUCTION_EFFECT,
        "items": items,
    }


def _binding_gate_ladder(
    *,
    snapshot: Mapping[str, Any],
    calculation_explainers: Mapping[str, Any],
) -> dict[str, Any]:
    binding_gate = _binding_gate_from_calculation(
        calculation_explainers
    ) or _binding_gate_from_snapshot(snapshot)
    binding_id = _text(binding_gate.get("gate_id")) if binding_gate else ""
    rows = []
    for gate in _records(_mapping(snapshot.get("positions")).get("position_gates")):
        rows.append(
            {
                "gate_id": _text(gate.get("gate_id")),
                "label": _text(gate.get("label")),
                "cap": _format_percent(gate.get("max_position")),
                "triggered": bool(gate.get("triggered")),
                "binding": _text(gate.get("gate_id")) == binding_id,
                "source": _text(gate.get("source")),
                "reason": _text(gate.get("reason")),
                "release_condition": "参见 calculation_explainers / 后续 gate policy registry。",
            }
        )
    return {
        "status": "AVAILABLE" if rows else "MISSING",
        "binding_gate_id": binding_id,
        "gates": rows,
    }


def _data_quality_pit_safety(
    *,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
) -> dict[str, Any]:
    quality = _mapping(snapshot.get("quality"))
    data_gate = _mapping(daily_decision_summary.get("data_gate"))
    return {
        "data_gate_status": _text(data_gate.get("status"), _quality_status(snapshot)),
        "market_data_status": _text(quality.get("market_data_status"), "UNKNOWN"),
        "market_data_error_count": _text(quality.get("market_data_error_count"), "UNKNOWN"),
        "market_data_warning_count": _text(quality.get("market_data_warning_count"), "UNKNOWN"),
        "feature_status": _text(quality.get("feature_status"), "UNKNOWN"),
        "sec_feature_status": _text(quality.get("sec_feature_status"), "UNKNOWN"),
        "blocking_reasons": _texts(data_gate.get("blocking_reasons")),
        "stale_report_count": report_index_summary.get("stale_count"),
        "missing_report_count": report_index_summary.get("missing_count"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _backtest_shadow_governance(
    *,
    daily_decision_summary: Mapping[str, Any],
    daily_task_dashboard: Mapping[str, Any],
    research_governance_summary: Mapping[str, Any],
) -> dict[str, Any]:
    if research_governance_summary:
        summary = _mapping(research_governance_summary.get("summary"))
        groups = _mapping(summary.get("groups"))
        return {
            "availability": "AVAILABLE",
            "source": "research_governance_summary",
            "status": _text(research_governance_summary.get("status"), "UNKNOWN"),
            "card_count": summary.get("card_count"),
            "missing_count": summary.get("missing_count"),
            "warning_count": summary.get("warning_count"),
            "manual_review_required_count": summary.get("manual_review_required_count"),
            "shadow_observe_count": groups.get("Shadow observe-only"),
            "candidate_research_count": groups.get("Candidate / research-only"),
            "blocked_count": groups.get("Blocked / insufficient data"),
            "production_effect": PRODUCTION_EFFECT,
            "limitation": "详细卡片见 research_governance_summary artifact。",
        }
    parameter_governance = _mapping(daily_decision_summary.get("parameter_governance"))
    feedback_review = _mapping(daily_decision_summary.get("feedback_review"))
    key_conclusions = _records(daily_task_dashboard.get("key_conclusions"))
    shadow_conclusions = [
        _text(item.get("primary"))
        for item in key_conclusions
        if "shadow" in _text(item.get("area")).lower()
        or "shadow" in _text(item.get("title")).lower()
    ]
    return {
        "availability": "AVAILABLE" if parameter_governance or feedback_review else "LIMITED",
        "parameter_governance_status": _text(parameter_governance.get("status"), "MISSING"),
        "promotion_status": _text(parameter_governance.get("promotion_status"), "MISSING"),
        "feedback_review_status": _text(feedback_review.get("status"), "MISSING"),
        "shadow_summary": "; ".join(item for item in shadow_conclusions if item) or "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": "完整 research governance summary 将由 REPORT-051 统一生成。",
    }


def _manual_review_queue(
    *,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    report_index: Mapping[str, Any],
    research_governance_summary: Mapping[str, Any],
    documentation_contract: Mapping[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    for item in _records(snapshot.get("manual_review")):
        status = _text(item.get("status"), "UNKNOWN")
        if status != "PASS":
            items.append(
                {
                    "action_id": _text(item.get("name"), "manual_review"),
                    "severity": "warning" if "WARNING" in status else "info",
                    "category": "manual_review",
                    "reason": _text(item.get("summary"), status),
                    "source_artifact": _text(item.get("source_path"), "decision_snapshot"),
                    "production_impact": "may_limit_conclusion_use",
                }
            )
    data_gate = _mapping(daily_decision_summary.get("data_gate"))
    for reason in _texts(data_gate.get("blocking_reasons")):
        items.append(
            {
                "action_id": "data_gate_review",
                "severity": "critical",
                "category": "data_quality",
                "reason": reason,
                "source_artifact": "daily_decision_summary",
                "production_impact": "blocks_or_limits_reader_brief",
            }
        )
    for report in _records(report_index.get("reports")):
        freshness = _text(report.get("freshness_status"))
        if freshness in {"MISSING", "STALE"}:
            items.append(
                {
                    "action_id": f"report_freshness:{_text(report.get('report_id'))}",
                    "severity": (
                        "critical" if report.get("required_for_daily_reading") else "warning"
                    ),
                    "category": "report_freshness",
                    "reason": f"{_text(report.get('title'), 'report')} freshness={freshness}",
                    "source_artifact": _text(report.get("latest_artifact_path"), "report_index"),
                    "production_impact": "reader_visibility_or_required_report_gap",
                }
            )
    research_summary = _mapping(research_governance_summary.get("summary"))
    manual_count = _int(research_summary.get("manual_review_required_count"))
    if manual_count:
        items.append(
            {
                "action_id": "research_governance_manual_review",
                "severity": "warning",
                "category": "research_governance",
                "reason": f"{manual_count} research/shadow/governance cards require manual review.",
                "source_artifact": "research_governance_summary",
                "production_impact": "manual_review_only",
            }
        )
    for issue in _records(documentation_contract.get("issues")):
        severity = _text(issue.get("severity"), "WARNING")
        items.append(
            {
                "action_id": f"documentation_contract:{_text(issue.get('report_id'))}",
                "severity": "critical" if severity == "ERROR" else "warning",
                "category": "documentation_contract",
                "reason": f"{_text(issue.get('code'))}: {_text(issue.get('message'))}",
                "source_artifact": "documentation_contract",
                "production_impact": "documentation_governance_gap",
            }
        )
    enriched = [_manual_review_item(item) for item in items]
    return {
        "status": "EMPTY" if not enriched else "ACTION_REQUIRED",
        "items": enriched,
        "groups": _manual_review_groups(enriched),
        "production_effect": PRODUCTION_EFFECT,
    }


def _manual_review_item(item: Mapping[str, Any]) -> dict[str, Any]:
    category = _text(item.get("category"), "manual_review")
    reason = _text(item.get("reason"), "UNKNOWN")
    source_artifact = _text(item.get("source_artifact"), "UNKNOWN")
    action_by_category = {
        "data_quality": (
            "打开 data quality / daily decision summary，确认 blocking reason 是否影响今日结论。"
        ),
        "report_freshness": (
            "打开 report index 或对应 report，补齐缺失/过期 artifact 后重跑 Reader Brief。"
        ),
        "research_governance": (
            "打开 research governance summary，确认 observe-only warning 是否需要人工处置。"
        ),
        "documentation_contract": (
            "打开 documentation contract，修复 registry / artifact catalog 契约缺口。"
        ),
        "manual_review": (
            "打开 decision snapshot manual_review 来源，确认 warning 是否影响结论使用等级。"
        ),
    }
    decision_impact_by_category = {
        "data_quality": "可能限制或阻断今日 Reader Brief 结论使用。",
        "report_freshness": "可能让读者缺少必要上下文，但不直接重算 score。",
        "research_governance": (
            "影响 observe-only / research-only 状态解释，不得视作 production promotion。"
        ),
        "documentation_contract": "影响文档治理可信度，不直接改变投资结论。",
        "manual_review": "可能降低某一分项或数据来源的解释置信度。",
    }
    return {
        **dict(item),
        "recommended_next_action": action_by_category.get(
            category,
            f"复核 {category}：{reason}",
        ),
        "decision_impact": decision_impact_by_category.get(
            category,
            "需人工判断是否影响今日结论使用。",
        ),
        "source_artifact": _short_path(source_artifact),
        "source_artifact_full_path": source_artifact,
        "production_effect": PRODUCTION_EFFECT,
    }


def _manual_review_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    labels = [
        ("critical", "Critical / Must Review Today"),
        ("warning", "Warning / Review Before Acting"),
        ("info", "Info / No Immediate Action"),
    ]
    return [
        {
            "severity": severity,
            "label": label,
            "count": len([item for item in items if _text(item.get("severity")) == severity]),
            "items": [item for item in items if _text(item.get("severity")) == severity],
        }
        for severity, label in labels
    ]


def _appendix_links(reports_dir: Path, source_inputs: Mapping[str, Any]) -> list[dict[str, Any]]:
    links: list[dict[str, Any]] = []
    for source_id, record in source_inputs.items():
        path_text = _text(_mapping(record).get("path"))
        if not path_text:
            continue
        path = Path(path_text)
        links.append(
            {
                "artifact_id": source_id,
                "short_name": _short_path(path_text),
                "path": path_text,
                "full_path": path_text,
                "href": _relative_href(path, reports_dir),
                "exists": bool(_mapping(record).get("exists")),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return links


def _executive_summary(
    *,
    run_context: Mapping[str, Any],
    decision: Mapping[str, Any],
    score_changes: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
    governance_summary: Mapping[str, Any],
    manual_review_queue: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "market_regime_summary": (
            f"{_text(run_context.get('market_regime'), 'UNKNOWN')} "
            f"since {_text(run_context.get('market_regime_start'), 'UNKNOWN')}"
        ),
        "top_model_conclusion": (
            f"{_text(decision.get('action'), 'UNKNOWN')} / "
            f"{_text(decision.get('final_risk_asset_ai_position'), 'UNKNOWN')}"
        ),
        "major_score_change": (
            f"overall_delta={_text(score_changes.get('overall_score_delta'), 'MISSING')}; "
            f"position_max_delta={_text(score_changes.get('final_position_max_delta'), 'MISSING')}"
        ),
        "report_freshness": (
            f"missing={_text(report_index_summary.get('missing_count'), '0')}; "
            f"stale={_text(report_index_summary.get('stale_count'), '0')}; "
            f"required_missing={_text(report_index_summary.get('required_missing_count'), '0')}"
        ),
        "governance_status": _text(governance_summary.get("status"), "UNKNOWN"),
        "manual_review_count": len(_records(manual_review_queue.get("items"))),
        "production_effect": PRODUCTION_EFFECT,
    }


def _narrative_executive_summary(
    *,
    run_context: Mapping[str, Any],
    decision: Mapping[str, Any],
    score_changes: Mapping[str, Any],
    contribution_summary: Mapping[str, Any],
    manual_review_queue: Mapping[str, Any],
    missing_artifact_impact: Mapping[str, Any],
) -> dict[str, Any]:
    action = _text(decision.get("action"), "UNKNOWN")
    position = _text(decision.get("final_risk_asset_ai_position"), "UNKNOWN")
    binding = _text(decision.get("binding_gate_label"), "UNKNOWN")
    positives = _texts(contribution_summary.get("top_positive_contributors"))
    negatives = _texts(contribution_summary.get("top_negative_or_zero_contributors"))
    critical_count = sum(
        _int(group.get("count"))
        for group in _records(manual_review_queue.get("groups"))
        if _text(group.get("severity")) == "critical"
    )
    manual_count = len(_records(manual_review_queue.get("items")))
    important_missing = _int(missing_artifact_impact.get("important_count"))
    blocking_missing = _int(missing_artifact_impact.get("blocking_count"))
    return {
        "today_conclusion": (
            f"今日系统结论为 {action}，最终 AI 风险资产仓位为 {position}。"
            f"当前适用市场 regime 为 {_text(run_context.get('market_regime'), 'UNKNOWN')}。"
        ),
        "why_this_conclusion": (
            "主要正向贡献来自 "
            + (", ".join(positives) if positives else "MISSING")
            + "；主要拖累或零贡献来自 "
            + (", ".join(negatives) if negatives else "MISSING")
            + (
                "；score change overall_delta="
                f"{_text(score_changes.get('overall_score_delta'), 'MISSING')}。"
            )
        ),
        "main_positive_drivers": positives,
        "main_negative_drivers": negatives,
        "binding_constraint": (
            f"最终仓位受 {binding} 约束。{_text(decision.get('binding_gate_reason'))}"
        ),
        "manual_review_summary": (
            f"当前有 {manual_count} 个复核项，其中 critical={critical_count}；"
            f"缺失/受限 artifact 中 blocking={blocking_missing}, important={important_missing}。"
        ),
        "production_effect_statement": (
            "Reader Brief 为只读阅读入口，production_effect=none；"
            "不运行 scoring/backtest/shadow/SEC PIT/weight，也不生成交易指令。"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief_status(
    *,
    warnings: list[str],
    missing_artifact_impact: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    if _text(decision.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT:
        return "FAILED"
    if _int(missing_artifact_impact.get("blocking_count")):
        return "LIMITED_READER_CONTEXT"
    if _int(missing_artifact_impact.get("important_count")):
        return "LIMITED_READER_CONTEXT"
    if warnings:
        return "PASS_WITH_WARNINGS"
    return "OK"


def _documentation_contract_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "error_count": 0,
            "warning_count": 0,
            "production_effect": PRODUCTION_EFFECT,
            "limitation": (
                "documentation_contract artifact missing; " "Reader Brief 不补造文档治理结论。"
            ),
        }
    summary = _mapping(payload.get("summary"))
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "report_count": summary.get("report_count"),
        "error_count": summary.get("error_count"),
        "warning_count": summary.get("warning_count"),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": "Documentation contract 只读检查 registry 与 artifact catalog 覆盖。",
    }


def _task_cadence_calendar(
    payload: Mapping[str, Any],
    *,
    report_registry_path: Path,
) -> dict[str, Any]:
    if not payload:
        try:
            registry = load_report_registry(report_registry_path)
        except (FileNotFoundError, ValueError) as exc:
            return {
                "availability": "MISSING",
                "status": "MISSING",
                "source": "missing_report_index_and_registry_unavailable",
                "registry_error": str(exc),
                "production_effect": PRODUCTION_EFFECT,
                "groups": [],
            }
        groups: dict[str, list[dict[str, Any]]] = {}
        for report in _records(registry.get("reports")):
            if not report.get("include_in_reader_brief"):
                continue
            group_key = _cadence_group(report)
            expected = _texts(report.get("artifact_globs"))
            groups.setdefault(group_key, []).append(
                {
                    "report_id": _text(report.get("report_id")),
                    "title": _text(report.get("title")),
                    "cadence": _normalize_cadence(_text(report.get("cadence"), "ad_hoc")),
                    "latest_status": "UNKNOWN",
                    "last_run": "MISSING_RUNTIME_INDEX",
                    "next_expected_run": "按 registry freshness_sla_days 复核",
                    "expected_artifact": expected[0] if expected else "MISSING",
                    "artifact_path": expected[0] if expected else "MISSING",
                    "reader_role": _text(report.get("audience"), "UNKNOWN"),
                    "owner": _text(report.get("owner"), "UNKNOWN"),
                    "review_need": _text(report.get("owner_action"), "UNKNOWN"),
                    "next_action": _text(report.get("owner_action"), "UNKNOWN"),
                    "source": "registry_fallback",
                    "production_effect": _text(report.get("production_effect"), PRODUCTION_EFFECT),
                }
            )
        ordered = [
            {"cadence": cadence, "reports": groups[cadence]}
            for cadence in (
                "daily",
                "weekly",
                "bi_weekly",
                "monthly",
                "ad_hoc_research",
                "governance",
            )
            if cadence in groups
        ]
        return {
            "availability": "REGISTRY_FALLBACK",
            "status": "LIMITED_READER_CONTEXT",
            "source": "registry_fallback",
            "registry_path": str(report_registry_path),
            "production_effect": PRODUCTION_EFFECT,
            "groups": ordered,
        }
    groups: dict[str, list[dict[str, Any]]] = {}
    for report in _records(payload.get("reports")):
        cadence = _cadence_group(report)
        path_text = _text(report.get("latest_artifact_path"), "MISSING")
        groups.setdefault(cadence, []).append(
            {
                "report_id": _text(report.get("report_id")),
                "title": _text(report.get("title")),
                "last_run": _text(report.get("artifact_date"), "MISSING"),
                "next_expected_run": "按 report_index freshness_sla_days 复核",
                "latest_status": _text(report.get("freshness_status"), "UNKNOWN"),
                "status": _text(report.get("freshness_status"), "UNKNOWN"),
                "artifact_path": _short_path(path_text),
                "full_path": "" if path_text == "MISSING" else path_text,
                "owner": _text(report.get("owner"), "UNKNOWN"),
                "review_need": _text(report.get("owner_action"), "UNKNOWN"),
                "next_action": _text(report.get("owner_action"), "UNKNOWN"),
                "production_effect": _text(report.get("production_effect"), PRODUCTION_EFFECT),
            }
        )
    ordered = [
        {"cadence": cadence, "reports": groups[cadence]}
        for cadence in (
            "daily",
            "weekly",
            "bi_weekly",
            "monthly",
            "ad_hoc_research",
            "governance",
        )
        if cadence in groups
    ]
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "source": "report_index",
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "groups": ordered,
    }


def _report_navigation(
    *,
    reports_dir: Path,
    source_inputs: Mapping[str, Any],
    report_index: Mapping[str, Any],
    missing_artifact_impact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    links = _appendix_links(reports_dir, source_inputs)
    impact_by_id = {
        _text(item.get("artifact_id")): _text(item.get("impact_level"), "INFO")
        for item in _records(missing_artifact_impact.get("items"))
    }
    for link in links:
        artifact_id = _text(link.get("artifact_id"))
        link["title"] = artifact_id
        link["status"] = "AVAILABLE" if link.get("exists") else "MISSING"
        link["freshness_status"] = link["status"]
        link["purpose"] = _navigation_purpose(artifact_id, link["status"])
        link["why_open_this"] = _navigation_reason(artifact_id, link["status"])
        link["impact_level"] = impact_by_id.get(artifact_id, "INFO")
    for report in _records(report_index.get("reports")):
        path_text = _text(report.get("latest_artifact_path"))
        if not path_text:
            continue
        path = Path(path_text)
        links.append(
            {
                "artifact_id": _text(report.get("report_id")),
                "title": _text(report.get("title")),
                "short_name": _short_path(path_text),
                "path": path_text,
                "full_path": path_text,
                "href": _relative_href(path, reports_dir),
                "exists": bool(report.get("exists")),
                "status": _text(report.get("artifact_status"), "UNKNOWN"),
                "freshness_status": _text(report.get("freshness_status"), "UNKNOWN"),
                "production_effect": _text(report.get("production_effect"), PRODUCTION_EFFECT),
                "purpose": _navigation_purpose(
                    _text(report.get("report_id")),
                    _text(report.get("freshness_status"), "UNKNOWN"),
                ),
                "why_open_this": _navigation_reason(
                    _text(report.get("report_id")),
                    _text(report.get("freshness_status"), "UNKNOWN"),
                ),
                "impact_level": impact_by_id.get(_text(report.get("report_id")), "INFO"),
            }
        )
    links.append(
        {
            "artifact_id": "artifact_catalog",
            "title": "Artifact Catalog",
            "short_name": "artifact_catalog.md",
            "path": "docs/artifact_catalog.md",
            "full_path": "docs/artifact_catalog.md",
            "href": "../../docs/artifact_catalog.md",
            "exists": True,
            "status": "DOCUMENTATION",
            "freshness_status": "DOCUMENTATION",
            "production_effect": PRODUCTION_EFFECT,
            "purpose": "Governance / documentation",
            "why_open_this": "查看产物边界、schema/status 术语和 common misread。",
            "impact_level": "INFO",
        }
    )
    return links


def _report_navigation_groups(navigation: list[dict[str, Any]]) -> dict[str, Any]:
    purposes = [
        "Core decision artifacts",
        "Detailed evidence",
        "Governance / documentation",
        "Missing but expected",
    ]
    groups = []
    for purpose in purposes:
        items = [item for item in navigation if _text(item.get("purpose")) == purpose]
        groups.append({"purpose": purpose, "count": len(items), "items": items})
    return {
        "status": "AVAILABLE",
        "production_effect": PRODUCTION_EFFECT,
        "groups": groups,
    }


def _navigation_purpose(artifact_id: str, status: str) -> str:
    if status in {"MISSING", "STALE", "REQUIRED_MISSING"}:
        return "Missing but expected"
    if artifact_id in {
        "decision_snapshot",
        "daily_decision_summary",
        "daily_report",
        "reader_brief",
    }:
        return "Core decision artifacts"
    if artifact_id in {
        "evidence_dashboard",
        "calculation_explainers",
        "trace_bundle",
        "score_change_attribution",
        "daily_task_dashboard",
    }:
        return "Detailed evidence"
    return "Governance / documentation"


def _navigation_reason(artifact_id: str, status: str) -> str:
    if status in {"MISSING", "STALE", "REQUIRED_MISSING"}:
        return "确认缺失或过期是否影响今日阅读上下文。"
    reasons = {
        "decision_snapshot": "审计最终 score、gate、position 和 manual review 原始字段。",
        "daily_decision_summary": "查看面向 daily task 的核心决策摘要和 data gate。",
        "daily_report": "阅读全文叙事和风险注释。",
        "evidence_dashboard": "下钻核心证据、trace 和 source artifacts。",
        "calculation_explainers": "查看关键数字公式、输入、PIT policy 和 common misread。",
        "trace_bundle": "审计 claim、dataset 和 report trace。",
        "score_change_attribution": "查看今天相对上一期为何变化。",
        "research_governance_summary": "确认 backtest/shadow/SEC PIT/weight 是否仍 observe-only。",
        "report_index": "检查报告 freshness、missing/stale 和 owner action。",
        "documentation_contract": "检查 registry 与 artifact catalog 契约覆盖。",
    }
    return reasons.get(artifact_id, "打开该 artifact 获取详细证据或治理上下文。")


def _normalize_cadence(value: str) -> str:
    normalized = value.lower().replace("-", "_").replace(" ", "_")
    if normalized in {"biweekly", "bi_weekly"}:
        return "bi_weekly"
    if normalized in {"ad_hoc", "adhoc", "research", "ad_hoc_research"}:
        return "ad_hoc_research"
    if normalized in {"daily", "weekly", "monthly"}:
        return normalized
    return "ad_hoc_research"


def _cadence_group(report: Mapping[str, Any]) -> str:
    group = _text(report.get("group")).lower().replace("-", "_").replace(" ", "_")
    if group in {"governance", "docs", "documentation"}:
        return "governance"
    return _normalize_cadence(_text(report.get("cadence"), "ad_hoc"))


def _relative_href(path: Path, reports_dir: Path) -> str:
    try:
        return str(path.relative_to(reports_dir))
    except ValueError:
        return str(path)


def _source_freshness(
    source_artifacts: list[dict[str, Any]],
    source_inputs: Mapping[str, Any],
) -> str:
    statuses = [
        _text(item.get("freshness_status"))
        or _text(item.get("availability"))
        or _text(item.get("status"))
        for item in source_artifacts
        if isinstance(item, Mapping)
    ]
    if statuses:
        return ", ".join(status for status in statuses if status) or "UNKNOWN"
    snapshot = _mapping(source_inputs.get("decision_snapshot"))
    return _text(snapshot.get("availability"), "UNKNOWN")


def _funnel_step(
    metric_id: str,
    metrics: Mapping[str, Any],
    fallback_value: str,
    source_field: str,
    source_inputs: Mapping[str, Any],
) -> dict[str, Any]:
    metric = _mapping(metrics.get(metric_id))
    source_artifacts = _records(metric.get("source_artifacts"))
    return {
        "metric_id": metric_id,
        "label": _text(metric.get("audience_label"), metric_id),
        "current_value": _text(metric.get("value"), fallback_value),
        "formula": _text(metric.get("formula"), "MISSING_EXPLAINER"),
        "source_field": source_field,
        "source_artifacts": source_artifacts
        or [
            _mapping(source_inputs.get("decision_snapshot")),
        ],
        "source_freshness": _source_freshness(source_artifacts, source_inputs),
        "pit_policy": _text(metric.get("pit_policy"), "UNKNOWN"),
        "common_misread": _text(metric.get("common_misread"), "MISSING"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _binding_gate_from_calculation(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    metrics = _mapping(payload.get("metrics"))
    final_max = _mapping(metrics.get("final_position_max"))
    binding = _mapping(_mapping(final_max.get("input_values")).get("binding_gate"))
    return binding or None


def _binding_gate_from_snapshot(snapshot: Mapping[str, Any]) -> dict[str, Any] | None:
    positions = _mapping(snapshot.get("positions"))
    final_max = _float_or_none(
        _mapping(positions.get("final_risk_asset_ai_band")).get("max_position")
    )
    if final_max is None:
        return None
    for gate in _records(positions.get("position_gates")):
        cap = _float_or_none(gate.get("max_position"))
        if cap is not None and abs(cap - final_max) < 1e-9:
            return gate
    return None


def _input_warnings(paths: Mapping[str, Path | None]) -> list[str]:
    warnings: list[str] = []
    for source_id, path in paths.items():
        if path is None:
            warnings.append(f"{source_id}_not_configured")
        elif not path.exists():
            warnings.append(f"{source_id}_missing:{path}")
    return warnings


def _source_input(source_id: str, path: Path | None, exists: bool) -> dict[str, Any]:
    path_text = "" if path is None else str(path)
    return {
        "id": source_id,
        "path": path_text,
        "short_name": _short_path(path_text),
        "full_path": path_text,
        "exists": exists,
        "availability": "AVAILABLE" if exists else "MISSING",
        "production_effect": PRODUCTION_EFFECT,
    }


def _read_required_json(path: Path, label: str) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{label} must be a JSON object: {path}")
    return raw


def _read_optional_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _quality_status(snapshot: Mapping[str, Any]) -> str:
    return _text(_mapping(snapshot.get("quality")).get("market_data_status"), "UNKNOWN")


def _confidence_summary(scores: Mapping[str, Any]) -> str:
    score = _text(scores.get("confidence_score"))
    level = _text(scores.get("confidence_level"))
    return " ".join(item for item in (score, level) if item) or "UNKNOWN"


def _format_band(raw: Mapping[str, Any]) -> str:
    min_position = _format_percent(raw.get("min_position"))
    max_position = _format_percent(raw.get("max_position"))
    if min_position == "UNKNOWN" or max_position == "UNKNOWN":
        return "UNKNOWN"
    label = _text(raw.get("label"))
    return f"{min_position}-{max_position}" + (f" ({label})" if label else "")


def _format_percent(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "UNKNOWN"
    return f"{number:.0%}"


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: object) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item not in {None, ""}]


def _text(value: object, default: str = "") -> str:
    if value is None or value == "":
        return default
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _short_path(value: object) -> str:
    text = _text(value)
    if not text:
        return ""
    return Path(text).name or text


def _quality_check(check_id: str, passed: bool, message: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "message": message,
        "production_effect": PRODUCTION_EFFECT,
    }


def _section(title: str, body: str) -> str:
    return f"<section><h2>{html.escape(title)}</h2>{body}</section>"


def _narrative_summary_html(summary: Mapping[str, Any]) -> str:
    if not summary:
        return '<p class="narrative">Narrative summary missing.</p>'
    today = html.escape(_text(summary.get("today_conclusion"), "UNKNOWN"))
    why = html.escape(_text(summary.get("why_this_conclusion"), "UNKNOWN"))
    review = html.escape(_text(summary.get("manual_review_summary"), "UNKNOWN"))
    return (
        '<div class="narrative">'
        f"<p><strong>今日结论：</strong>{today}</p>"
        f"<p><strong>为什么：</strong>{why}</p>"
        f"<p><strong>需要复核：</strong>{review}</p>"
        "</div>"
    )


def _artifact_impact_table(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>未发现缺失或受限 artifact。</p>"
    rows = []
    for record in records:
        details = ""
        full_path = _text(record.get("full_path"))
        if full_path:
            details = (
                "<details><summary>audit</summary>"
                + _definition_table(
                    [
                        ("full_path", full_path),
                        ("artifact_id", record.get("artifact_id")),
                        ("production_effect", record.get("production_effect")),
                    ]
                )
                + "</details>"
            )
        rows.append(
            "<tr>"
            f"<td>{html.escape(_text(record.get('artifact_id')))}</td>"
            f"<td>{html.escape(_text(record.get('short_name'), 'MISSING'))}</td>"
            f"<td>{html.escape(_text(record.get('status'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_text(record.get('impact_level'), 'INFO'))}</td>"
            f"<td>{html.escape(_text(record.get('reader_impact'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_text(record.get('decision_impact'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_text(record.get('recommended_action'), 'UNKNOWN'))}{details}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>artifact_id</th><th>short_name</th><th>status</th>"
        "<th>impact_level</th><th>reader_impact</th><th>decision_impact</th>"
        "<th>recommended_action</th></tr></thead><tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def _manual_review_groups_html(
    manual_review: Mapping[str, Any],
    fallback_items: list[dict[str, Any]],
) -> str:
    groups = _records(manual_review.get("groups"))
    if not groups:
        return _records_table(fallback_items)
    parts: list[str] = []
    for group in groups:
        label = html.escape(_text(group.get("label"), "UNKNOWN"))
        parts.append(f"<h3>{label}</h3>")
        parts.append(_manual_review_table(_records(group.get("items"))))
    return "\n".join(parts)


def _manual_review_table(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无。</p>"
    rows = []
    for record in records:
        audit = _definition_table(
            [
                ("source_artifact_full_path", record.get("source_artifact_full_path")),
                ("production_effect", record.get("production_effect")),
            ]
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(_text(record.get('action_id')))}</td>"
            f"<td>{html.escape(_text(record.get('category')))}</td>"
            f"<td>{html.escape(_text(record.get('reason')))}</td>"
            f"<td>{html.escape(_text(record.get('recommended_next_action')))}</td>"
            f"<td>{html.escape(_text(record.get('decision_impact')))}</td>"
            f"<td>{html.escape(_text(record.get('source_artifact')))}"
            f"<details><summary>audit</summary>{audit}</details></td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>action_id</th><th>category</th><th>reason</th>"
        "<th>recommended_next_action</th><th>decision_impact</th><th>source</th>"
        "</tr></thead><tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def _navigation_groups_html(
    navigation_groups: Mapping[str, Any],
    fallback_navigation: list[dict[str, Any]],
) -> str:
    groups = _records(navigation_groups.get("groups"))
    if not groups:
        return _records_table(fallback_navigation)
    parts: list[str] = []
    for group in groups:
        label = html.escape(_text(group.get("purpose"), "UNKNOWN"))
        parts.append(f"<h3>{label}</h3>")
        parts.append(_navigation_table(_records(group.get("items"))))
    return "\n".join(parts)


def _navigation_table(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无。</p>"
    rows = []
    for record in records:
        audit = _definition_table(
            [
                ("full_path", record.get("full_path")),
                ("href", record.get("href")),
                ("artifact_id", record.get("artifact_id")),
                ("exists", record.get("exists")),
                ("production_effect", record.get("production_effect")),
            ]
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(_text(record.get('artifact_id')))}</td>"
            f"<td>{html.escape(_text(record.get('short_name'), _text(record.get('title'))))}</td>"
            f"<td>{html.escape(_text(record.get('status'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_text(record.get('freshness_status'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_text(record.get('production_effect'), PRODUCTION_EFFECT))}</td>"
            f"<td>{html.escape(_text(record.get('why_open_this'), 'UNKNOWN'))}"
            f"<details><summary>audit</summary>{audit}</details></td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>artifact_id</th><th>short_name</th><th>status</th>"
        "<th>freshness_status</th><th>production_effect</th><th>why_open_this</th>"
        "</tr></thead><tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def _definition_table(rows: list[tuple[object, object]]) -> str:
    table_rows = [
        "<tr><th>{}</th><td>{}</td></tr>".format(
            html.escape(_text(label)),
            html.escape(_text(value, "UNKNOWN")),
        )
        for label, value in rows
    ]
    return "<table><tbody>" + "\n".join(table_rows) + "</tbody></table>"


def _records_table(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无可用记录。</p>"
    columns = list(dict.fromkeys(key for record in records for key in record))
    header = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body_rows = [
        "<tr>"
        + "".join(f"<td>{html.escape(_text(record.get(column), ''))}</td>" for column in columns)
        + "</tr>"
        for record in records
    ]
    return (
        "<table><thead><tr>"
        + header
        + "</tr></thead><tbody>"
        + "\n".join(body_rows)
        + "</tbody></table>"
    )


def _funnel_details(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无可用 funnel 记录。</p>"
    parts: list[str] = ['<div class="funnel-list">']
    for record in records:
        label = html.escape(_text(record.get("label"), _text(record.get("metric_id"))))
        value = html.escape(_text(record.get("current_value"), "UNKNOWN"))
        detail_rows = [
            ("metric_id", record.get("metric_id")),
            ("formula", record.get("formula")),
            ("source_field", record.get("source_field")),
            ("source_freshness", record.get("source_freshness")),
            ("pit_policy", record.get("pit_policy")),
            ("common_misread", record.get("common_misread")),
            ("production_effect", record.get("production_effect")),
            ("source_artifacts", record.get("source_artifacts")),
        ]
        parts.append(
            '<details class="funnel-step">'
            f"<summary><strong>{label}</strong><span>{value}</span></summary>"
            + _definition_table(detail_rows)
            + "</details>"
        )
    parts.append("</div>")
    return "\n".join(parts)


def _cadence_calendar_tables(calendar: Mapping[str, Any]) -> str:
    groups = _records(calendar.get("groups"))
    if not groups:
        return "<p>无 cadence 记录。</p>"
    parts: list[str] = []
    for group in groups:
        cadence = html.escape(_text(group.get("cadence"), "unknown"))
        parts.append(f"<h3>{cadence}</h3>")
        parts.append(_cadence_report_table(_records(group.get("reports"))))
    return "\n".join(parts)


def _cadence_report_table(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无 cadence 记录。</p>"
    rows = []
    for record in records:
        status = _text(
            record.get("latest_status"),
            _text(record.get("status"), "UNKNOWN"),
        )
        next_action = _text(
            record.get("next_action"),
            _text(record.get("review_need"), "UNKNOWN"),
        )
        audit = _definition_table(
            [
                ("full_path", record.get("full_path")),
                ("expected_artifact", record.get("expected_artifact")),
                ("source", record.get("source")),
                ("production_effect", record.get("production_effect")),
            ]
        )
        artifact = _text(record.get("artifact_path"), _text(record.get("expected_artifact")))
        rows.append(
            "<tr>"
            f"<td>{html.escape(_text(record.get('report_id')))}</td>"
            f"<td>{html.escape(_text(record.get('cadence'), ''))}</td>"
            f"<td>{html.escape(_text(record.get('last_run'), 'MISSING'))}</td>"
            f"<td>{html.escape(_text(record.get('next_expected_run'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(_short_path(artifact) or artifact)}</td>"
            f"<td>{html.escape(status)}</td>"
            f"<td>{html.escape(_text(record.get('owner'), 'UNKNOWN'))}</td>"
            f"<td>{html.escape(next_action)}"
            f"<details><summary>audit</summary>{audit}</details></td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>report_id</th><th>cadence</th><th>last_run</th>"
        "<th>next_expected_run</th><th>artifact</th><th>status</th><th>owner</th>"
        "<th>next_action</th></tr></thead><tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def _css() -> str:
    return """
:root {
  color-scheme: light;
  font-family: Inter, "Segoe UI", Arial, sans-serif;
  color: #1b1f23;
  background: #f6f7f9;
}
body {
  margin: 0;
}
main {
  max-width: 1180px;
  margin: 0 auto;
  padding: 28px;
}
header {
  margin-bottom: 18px;
}
header p {
  margin: 0 0 6px;
  color: #586069;
  font-size: 13px;
}
h1 {
  margin: 0 0 8px;
  font-size: 30px;
  letter-spacing: 0;
}
h2 {
  margin: 0 0 12px;
  font-size: 18px;
  letter-spacing: 0;
}
section {
  background: #ffffff;
  border: 1px solid #d9dee7;
  border-radius: 6px;
  margin: 14px 0;
  padding: 16px;
}
details.funnel-step {
  border-top: 1px solid #e7ebf0;
  padding: 10px 0;
}
details.funnel-step summary {
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  gap: 16px;
  list-style-position: inside;
}
h3 {
  margin: 14px 0 6px;
  font-size: 15px;
  letter-spacing: 0;
}
table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
th,
td {
  border-top: 1px solid #e7ebf0;
  padding: 8px;
  text-align: left;
  vertical-align: top;
  overflow-wrap: anywhere;
  font-size: 13px;
}
th {
  color: #4d5968;
  width: 22%;
}
thead th {
  background: #f0f3f7;
  color: #29313d;
}
"""
