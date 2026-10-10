from __future__ import annotations

import html
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import PRIMARY_RESEARCH_START
from ai_trading_system.platform.reporting.reader_brief_native import (
    project_data_quality_pit_safety,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.trading_engine.signal_snapshots import (
    load_signal_snapshot_payload,
    signal_snapshot_summary,
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
    market_panel_path: Path | None = None,
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
    market_panel = _read_optional_json(market_panel_path)
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
            "market_panel": market_panel_path,
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
        "market_panel": _source_input(
            "market_panel",
            market_panel_path,
            market_panel_path is not None and market_panel_path.exists(),
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
    market_situation = _market_situation_snapshot(
        evidence_dashboard=evidence_dashboard,
        snapshot=snapshot,
        market_panel=market_panel,
    )
    report_index_summary = _report_index_summary(report_index)
    report_index_waiver_inventory = _report_index_waiver_inventory_summary(report_index)
    reader_brief_consistency = _reader_brief_consistency_summary(report_index)
    production_boundary_static_scan = _production_boundary_static_scan_summary(report_index)
    owner_review_template_v2 = _owner_review_template_v2_summary(report_index)
    owner_decision_audit_log = _owner_decision_audit_log_summary(report_index)
    research_monthly_review_pack = _research_monthly_review_pack_summary(report_index)
    paper_shadow_promotion_board = _paper_shadow_promotion_board_summary(report_index)
    candidate_rejection_postmortem = _candidate_rejection_postmortem_summary(report_index)
    decision_snapshot_lifecycle_policy = _decision_snapshot_lifecycle_policy_summary(
        report_index
    )
    extended_shadow_observation_clock = _extended_shadow_observation_clock_summary(report_index)
    extended_shadow_protocol = _extended_shadow_protocol_summary(report_index)
    research_roadmap_dashboard = _research_roadmap_dashboard_summary(report_index)
    research_governance_end_to_end_pack = _research_governance_end_to_end_pack_summary(
        report_index
    )
    research_governance_recovery_pack = _research_governance_recovery_pack_summary(
        report_index
    )
    decision_stage_governance_snapshot = _decision_stage_governance_snapshot_summary(
        report_index
    )
    return_to_research_governance_snapshot = (
        _return_to_research_governance_snapshot_summary(report_index)
    )
    next_research_cycle_snapshot = _next_research_cycle_snapshot_summary(report_index)
    next_candidate_backfill = _next_candidate_backfill_summary(report_index)
    next_candidate_stress_cost_benchmark = (
        _next_candidate_stress_cost_benchmark_summary(report_index)
    )
    next_candidate_vs_returned_comparison = (
        _next_candidate_vs_returned_comparison_summary(report_index)
    )
    next_candidate_signal_window_sensitivity = (
        _next_candidate_signal_window_sensitivity_summary(report_index)
    )
    next_candidate_research_gate = _next_candidate_research_gate_summary(report_index)
    next_candidate_owner_research_review_packet = (
        _next_candidate_owner_research_review_packet_summary(report_index)
    )
    executable_binding_contract = _executable_binding_contract_summary(report_index)
    executable_signal_binding = _executable_signal_binding_summary(report_index)
    executable_research_weight_binding = _executable_research_weight_binding_summary(
        report_index
    )
    executable_binding_safety_audit = _executable_binding_safety_audit_summary(
        report_index
    )
    research_safety_boundary_audit = _research_safety_boundary_audit_summary(report_index)
    artifact_lineage_graph = _artifact_lineage_graph_summary(report_index)
    task_register_consistency = _task_register_consistency_summary(report_index)
    report_quality_gate = _report_quality_gate_summary(report_index)
    pit_source_manifest = _pit_source_manifest_summary(report_index)
    data_refresh_audit = _data_refresh_audit_summary(report_index)
    data_source_fallback_policy = _data_source_fallback_policy_summary(report_index)
    cache_catalog = _cache_catalog_summary(report_index)
    governance_summary = _backtest_shadow_governance(
        daily_decision_summary=daily_decision_summary,
        daily_task_dashboard=daily_task_dashboard,
        research_governance_summary=research_governance_summary,
    )
    parameter_shadow_review = _parameter_shadow_review(as_of)
    etf_operations_health = _etf_operations_health_summary(report_index)
    tail_risk_fallback_status = _tail_risk_daily_reading_safety_summary()
    portfolio_control_research = _portfolio_control_research_summary()
    portfolio_control_forward_aging = _portfolio_control_forward_aging_summary()
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
        governance_summary=governance_summary,
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
        market_situation=market_situation,
        score_changes=score_change_summary,
        contribution_summary=contribution_summary,
        governance_summary=governance_summary,
        manual_review_queue=manual_review_queue,
        missing_artifact_impact=missing_artifact_impact,
    )
    quality_status = _reader_brief_status(
        warnings=warnings,
        missing_artifact_impact=missing_artifact_impact,
        decision=executive_decision,
    )
    data_quality_pit_safety = project_data_quality_pit_safety(
        as_of=as_of,
        snapshot=snapshot,
        daily_decision_summary=daily_decision_summary,
        report_index_summary=report_index_summary,
    )
    status_panel = _status_panel(
        build_status=quality_status,
        decision=executive_decision,
        governance_summary=governance_summary,
        manual_review_queue=manual_review_queue,
        missing_artifact_impact=missing_artifact_impact,
        report_index_summary=report_index_summary,
        data_quality_pit_safety=data_quality_pit_safety,
    )
    action_checklist = _action_checklist(
        decision=executive_decision,
        status_panel=status_panel,
        governance_summary=governance_summary,
        manual_review_queue=manual_review_queue,
        data_quality_pit_safety=data_quality_pit_safety,
    )
    score_change_narrative = _score_change_narrative(
        score_changes=score_change_summary,
        contribution_summary=contribution_summary,
        decision=executive_decision,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": quality_status,
        "status_panel": status_panel,
        "production_effect": PRODUCTION_EFFECT,
        "reader_entry_role": "daily_reading_home",
        "source_inputs": source_inputs,
        "warnings": warnings,
        "run_context": run_context,
        "narrative_executive_summary": narrative_summary,
        "action_checklist": action_checklist,
        "executive_decision": executive_decision,
        "market_situation_snapshot": market_situation,
        "score_to_position_funnel": _score_to_position_funnel(
            snapshot=snapshot,
            calculation_explainers=calculation_explainers,
            source_inputs=source_inputs,
        ),
        "score_change_attribution_summary": score_change_summary,
        "score_change_narrative": score_change_narrative,
        "report_index_summary": report_index_summary,
        "report_index_waiver_inventory": report_index_waiver_inventory,
        "reader_brief_consistency": reader_brief_consistency,
        "production_boundary_static_scan": production_boundary_static_scan,
        "owner_review_template_v2": owner_review_template_v2,
        "owner_decision_audit_log": owner_decision_audit_log,
        "research_monthly_review_pack": research_monthly_review_pack,
        "paper_shadow_promotion_board": paper_shadow_promotion_board,
        "candidate_rejection_postmortem": candidate_rejection_postmortem,
        "decision_snapshot_lifecycle_policy": decision_snapshot_lifecycle_policy,
        "extended_shadow_observation_clock": extended_shadow_observation_clock,
        "extended_shadow_protocol": extended_shadow_protocol,
        "research_roadmap_dashboard": research_roadmap_dashboard,
        "research_governance_end_to_end_pack": research_governance_end_to_end_pack,
        "research_governance_recovery_pack": research_governance_recovery_pack,
        "decision_stage_governance_snapshot": decision_stage_governance_snapshot,
        "return_to_research_governance_snapshot": return_to_research_governance_snapshot,
        "next_research_cycle_snapshot": next_research_cycle_snapshot,
        "next_candidate_backfill": next_candidate_backfill,
        "next_candidate_stress_cost_benchmark": next_candidate_stress_cost_benchmark,
        "next_candidate_vs_returned_comparison": next_candidate_vs_returned_comparison,
        "next_candidate_signal_window_sensitivity": (
            next_candidate_signal_window_sensitivity
        ),
        "next_candidate_research_gate": next_candidate_research_gate,
        "next_candidate_owner_research_review_packet": (
            next_candidate_owner_research_review_packet
        ),
        "executable_binding_contract": executable_binding_contract,
        "executable_signal_binding": executable_signal_binding,
        "executable_research_weight_binding": executable_research_weight_binding,
        "executable_binding_safety_audit": executable_binding_safety_audit,
        "research_safety_boundary_audit": research_safety_boundary_audit,
        "artifact_lineage_graph": artifact_lineage_graph,
        "task_register_consistency": task_register_consistency,
        "report_quality_gate": report_quality_gate,
        "missing_limited_artifact_impact": missing_artifact_impact,
        "task_cadence_calendar": task_cadence_calendar,
        "documentation_contract_summary": _documentation_contract_summary(
            documentation_contract,
        ),
        "contribution_summary": contribution_summary,
        "component_score_explainability": component_explainability,
        "binding_gate_ladder": gate_ladder,
        "data_quality_pit_safety": data_quality_pit_safety,
        "pit_source_manifest": pit_source_manifest,
        "data_refresh_audit": data_refresh_audit,
        "data_source_fallback_policy": data_source_fallback_policy,
        "cache_catalog": cache_catalog,
        "backtest_shadow_governance": governance_summary,
        "parameter_shadow_review": parameter_shadow_review,
        "etf_operations_health": etf_operations_health,
        "tail_risk_fallback_status": tail_risk_fallback_status,
        "portfolio_control_research": portfolio_control_research,
        "portfolio_control_forward_aging": portfolio_control_forward_aging,
        "manual_review_queue": manual_review_queue,
        "executive_summary": _executive_summary(
            run_context=run_context,
            decision=executive_decision,
            market_situation=market_situation,
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
    status_panel = _mapping(reader_brief_payload.get("status_panel"))
    executive_decision = _mapping(reader_brief_payload.get("executive_decision"))
    action_checklist = _records(reader_brief_payload.get("action_checklist"))
    checks = [
        _quality_check(
            "narrative_executive_summary",
            bool(_mapping(reader_brief_payload.get("narrative_executive_summary"))),
            "首屏 narrative summary 存在。",
        ),
        _quality_check(
            "status_panel",
            all(
                _text(status_panel.get(key))
                for key in (
                    "build_status",
                    "decision_usability",
                    "research_promotion_status",
                )
            ),
            "首屏拆分 Build / Decision Usability / Research Promotion 状态。",
        ),
        _quality_check(
            "action_checklist",
            bool(action_checklist),
            "首屏 Action Checklist 存在。",
        ),
        _quality_check(
            "next_action_clear",
            bool(action_checklist) or bool(_text(executive_decision.get("recommended_action"))),
            "Reader Brief 披露明确下一步。",
        ),
        _quality_check(
            "safety_boundary",
            _text(reader_brief_payload.get("production_effect")) == PRODUCTION_EFFECT
            and executive_decision.get("not_trade_instruction") is True,
            "Reader Brief 披露 production_effect=none 和非交易指令边界。",
        ),
        _quality_check(
            "decision_state_clear",
            any(
                _text(status_panel.get(key))
                for key in (
                    "build_status",
                    "decision_usability",
                    "research_promotion_status",
                )
            )
            or any(
                _text(executive_decision.get(key))
                for key in ("action", "decision", "status")
            ),
            "Reader Brief 披露清晰 decision/build/research 状态。",
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
            "top_review_items",
            bool(_records(manual_queue.get("top_items")))
            or not bool(_records(manual_queue.get("items"))),
            "Manual Review Queue 已收敛 Top 3 复核项。",
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
    status_panel = _mapping(payload.get("status_panel"))
    run_context = _mapping(payload.get("run_context"))
    narrative_summary = _mapping(payload.get("narrative_executive_summary"))
    action_checklist = _records(payload.get("action_checklist"))
    executive_summary = _mapping(payload.get("executive_summary"))
    decision = _mapping(payload.get("executive_decision"))
    market = _mapping(payload.get("market_situation_snapshot"))
    funnel = _records(_mapping(payload.get("score_to_position_funnel")).get("steps"))
    score_changes = _mapping(payload.get("score_change_attribution_summary"))
    score_change_narrative = _mapping(payload.get("score_change_narrative"))
    report_index = _mapping(payload.get("report_index_summary"))
    report_index_waiver_inventory = _mapping(payload.get("report_index_waiver_inventory"))
    reader_brief_consistency = _mapping(payload.get("reader_brief_consistency"))
    production_boundary_static_scan = _mapping(
        payload.get("production_boundary_static_scan")
    )
    owner_review_template_v2 = _mapping(payload.get("owner_review_template_v2"))
    owner_decision_audit_log = _mapping(payload.get("owner_decision_audit_log"))
    research_monthly_review_pack = _mapping(payload.get("research_monthly_review_pack"))
    paper_shadow_promotion_board = _mapping(payload.get("paper_shadow_promotion_board"))
    candidate_rejection_postmortem = _mapping(payload.get("candidate_rejection_postmortem"))
    decision_snapshot_lifecycle_policy = _mapping(
        payload.get("decision_snapshot_lifecycle_policy")
    )
    extended_shadow_observation_clock = _mapping(
        payload.get("extended_shadow_observation_clock")
    )
    extended_shadow_protocol = _mapping(payload.get("extended_shadow_protocol"))
    research_roadmap_dashboard = _mapping(payload.get("research_roadmap_dashboard"))
    research_governance_end_to_end_pack = _mapping(
        payload.get("research_governance_end_to_end_pack")
    )
    research_governance_recovery_pack = _mapping(
        payload.get("research_governance_recovery_pack")
    )
    decision_stage_governance_snapshot = _mapping(
        payload.get("decision_stage_governance_snapshot")
    )
    return_to_research_governance_snapshot = _mapping(
        payload.get("return_to_research_governance_snapshot")
    )
    next_research_cycle_snapshot = _mapping(payload.get("next_research_cycle_snapshot"))
    next_candidate_backfill = _mapping(payload.get("next_candidate_backfill"))
    next_candidate_stress_cost_benchmark = _mapping(
        payload.get("next_candidate_stress_cost_benchmark")
    )
    next_candidate_vs_returned_comparison = _mapping(
        payload.get("next_candidate_vs_returned_comparison")
    )
    next_candidate_signal_window_sensitivity = _mapping(
        payload.get("next_candidate_signal_window_sensitivity")
    )
    next_candidate_research_gate = _mapping(payload.get("next_candidate_research_gate"))
    next_candidate_owner_research_review_packet = _mapping(
        payload.get("next_candidate_owner_research_review_packet")
    )
    executable_binding_contract = _mapping(payload.get("executable_binding_contract"))
    executable_signal_binding = _mapping(payload.get("executable_signal_binding"))
    executable_research_weight_binding = _mapping(
        payload.get("executable_research_weight_binding")
    )
    executable_binding_safety_audit = _mapping(
        payload.get("executable_binding_safety_audit")
    )
    research_safety_boundary_audit = _mapping(
        payload.get("research_safety_boundary_audit")
    )
    artifact_lineage_graph = _mapping(payload.get("artifact_lineage_graph"))
    task_register_consistency = _mapping(payload.get("task_register_consistency"))
    report_quality_gate = _mapping(payload.get("report_quality_gate"))
    missing_impact = _mapping(payload.get("missing_limited_artifact_impact"))
    cadence_calendar = _mapping(payload.get("task_cadence_calendar"))
    documentation_contract = _mapping(payload.get("documentation_contract_summary"))
    contribution_summary = _mapping(payload.get("contribution_summary"))
    components = _records(_mapping(payload.get("component_score_explainability")).get("components"))
    gates = _records(_mapping(payload.get("binding_gate_ladder")).get("gates"))
    quality = _mapping(payload.get("data_quality_pit_safety"))
    pit_source_manifest = _mapping(payload.get("pit_source_manifest"))
    data_refresh_audit = _mapping(payload.get("data_refresh_audit"))
    data_source_fallback_policy = _mapping(payload.get("data_source_fallback_policy"))
    cache_catalog = _mapping(payload.get("cache_catalog"))
    governance = _mapping(payload.get("backtest_shadow_governance"))
    parameter_shadow = _mapping(payload.get("parameter_shadow_review"))
    etf_operations_health = _mapping(payload.get("etf_operations_health"))
    tail_risk_fallback_status = _mapping(payload.get("tail_risk_fallback_status"))
    portfolio_control_research = _mapping(payload.get("portfolio_control_research"))
    portfolio_control_forward_aging = _mapping(payload.get("portfolio_control_forward_aging"))
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
        f"{_status_panel_header(status_panel, status)}</header>",
        _section(
            "Executive Summary",
            _status_panel_html(status_panel)
            + _action_checklist_html(action_checklist)
            + _top_summary_cards(
                decision=decision,
                market=market,
                manual_review=manual_review,
                governance=governance,
                status_panel=status_panel,
                payload_status=status,
                production_effect=_text(payload.get("production_effect"), PRODUCTION_EFFECT),
            )
            + _narrative_summary_html(narrative_summary)
            + _definition_table(
                [
                    ("today_conclusion", narrative_summary.get("today_conclusion")),
                    ("today_market_movement", narrative_summary.get("today_market_movement")),
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
                    ("market_movement", executive_summary.get("market_movement")),
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
        _section(
            "Market Situation",
            _market_proxy_cards(market)
            + _definition_table(
                [
                    ("availability", market.get("availability")),
                    ("market_price_panel_status", market.get("market_price_panel_status")),
                    ("market_movement", market.get("market_movement_sentence")),
                    ("benchmark_proxy", market.get("benchmark_proxy")),
                    ("ai_sector_proxy", market.get("ai_sector_proxy")),
                    ("risk_proxy", market.get("risk_proxy")),
                    ("liquidity_proxy", market.get("liquidity_proxy")),
                    ("market_data_status", market.get("market_data_status")),
                    ("feature_status", market.get("feature_status")),
                    ("recommended_action", market.get("recommended_action")),
                    ("production_effect", market.get("production_effect")),
                    ("limitation", market.get("limitation")),
                ]
            )
            + _records_table(_records(market.get("proxy_rows"))),
        ),
        _section(
            "Score & Decision Funnel",
            _funnel_flow(funnel, decision) + _funnel_details(funnel),
        ),
        _section(
            "Score Change Attribution",
            _score_change_narrative_html(score_change_narrative)
            + _definition_table(
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
            "Tail-Risk Fallback Status",
            _definition_table(
                [
                    ("research_status", tail_risk_fallback_status.get("research_status")),
                    ("promotion_allowed", tail_risk_fallback_status.get("promotion_allowed")),
                    (
                        "paper_shadow_allowed",
                        tail_risk_fallback_status.get("paper_shadow_allowed"),
                    ),
                    ("production_allowed", tail_risk_fallback_status.get("production_allowed")),
                    ("broker_action", tail_risk_fallback_status.get("broker_action")),
                    (
                        "latest_master_review_status",
                        tail_risk_fallback_status.get("latest_master_review_status"),
                    ),
                    (
                        "current_blocker_count",
                        tail_risk_fallback_status.get("current_blocker_count"),
                    ),
                    ("source_artifact", tail_risk_fallback_status.get("source_artifact")),
                    ("production_effect", tail_risk_fallback_status.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Portfolio Control Research",
            _definition_table(
                [
                    ("status", portfolio_control_research.get("status")),
                    (
                        "top_simple_baseline_candidate",
                        portfolio_control_research.get("top_simple_baseline_candidate"),
                    ),
                    (
                        "research_only_target_weights",
                        portfolio_control_research.get("research_only_target_weights"),
                    ),
                    ("promotion_allowed", portfolio_control_research.get("promotion_allowed")),
                    (
                        "paper_shadow_allowed",
                        portfolio_control_research.get("paper_shadow_allowed"),
                    ),
                    ("production_allowed", portfolio_control_research.get("production_allowed")),
                    ("broker_action", portfolio_control_research.get("broker_action")),
                    ("major_blocker_count", portfolio_control_research.get("major_blocker_count")),
                    ("source_artifact", portfolio_control_research.get("source_artifact")),
                    ("production_effect", portfolio_control_research.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Portfolio Control Forward Aging",
            _definition_table(
                [
                    ("status", portfolio_control_forward_aging.get("status")),
                    (
                        "primary_candidate",
                        portfolio_control_forward_aging.get("primary_candidate"),
                    ),
                    (
                        "challenger_candidate",
                        portfolio_control_forward_aging.get("challenger_candidate"),
                    ),
                    (
                        "latest_observation_date",
                        portfolio_control_forward_aging.get("latest_observation_date"),
                    ),
                    (
                        "matured_20d_count",
                        portfolio_control_forward_aging.get("matured_20d_count"),
                    ),
                    (
                        "matured_60d_count",
                        portfolio_control_forward_aging.get("matured_60d_count"),
                    ),
                    (
                        "matured_120d_count",
                        portfolio_control_forward_aging.get("matured_120d_count"),
                    ),
                    (
                        "paper_shadow_allowed",
                        portfolio_control_forward_aging.get("paper_shadow_allowed"),
                    ),
                    (
                        "production_allowed",
                        portfolio_control_forward_aging.get("production_allowed"),
                    ),
                    ("broker_action", portfolio_control_forward_aging.get("broker_action")),
                    ("source_artifact", portfolio_control_forward_aging.get("source_artifact")),
                ]
            ),
        ),
        _section(
            "Report Index Waiver Inventory",
            _definition_table(
                [
                    ("availability", report_index_waiver_inventory.get("availability")),
                    ("status", report_index_waiver_inventory.get("status")),
                    (
                        "inventory_status",
                        report_index_waiver_inventory.get("inventory_status"),
                    ),
                    (
                        "configured_waivers",
                        report_index_waiver_inventory.get("configured_waiver_count"),
                    ),
                    ("active_waivers", report_index_waiver_inventory.get("active_waiver_count")),
                    (
                        "expired_waivers",
                        report_index_waiver_inventory.get("expired_waiver_count"),
                    ),
                    (
                        "expiring_soon",
                        report_index_waiver_inventory.get("expiring_soon_waiver_count"),
                    ),
                    (
                        "missing_registry_entries",
                        report_index_waiver_inventory.get("missing_registry_entry_count"),
                    ),
                    ("next_action", report_index_waiver_inventory.get("next_action")),
                    ("detail_report", report_index_waiver_inventory.get("detail_report")),
                    ("production_effect", report_index_waiver_inventory.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Reader Brief Consistency Pack",
            _definition_table(
                [
                    ("availability", reader_brief_consistency.get("availability")),
                    ("status", reader_brief_consistency.get("status")),
                    ("consistency_status", reader_brief_consistency.get("consistency_status")),
                    (
                        "checked_reports",
                        reader_brief_consistency.get("checked_report_count"),
                    ),
                    (
                        "full_coverage_reports",
                        reader_brief_consistency.get("full_coverage_report_count"),
                    ),
                    (
                        "missing_sections",
                        reader_brief_consistency.get("missing_section_count"),
                    ),
                    (
                        "unclear_decisions",
                        reader_brief_consistency.get("unclear_decision_count"),
                    ),
                    ("next_action", reader_brief_consistency.get("next_action")),
                    ("detail_report", reader_brief_consistency.get("detail_report")),
                    ("production_effect", reader_brief_consistency.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Production Boundary Static Scan",
            _definition_table(
                [
                    ("availability", production_boundary_static_scan.get("availability")),
                    ("status", production_boundary_static_scan.get("status")),
                    (
                        "scan_status",
                        production_boundary_static_scan.get("scan_status"),
                    ),
                    (
                        "scanned_files",
                        production_boundary_static_scan.get("scanned_file_count"),
                    ),
                    (
                        "blocking_findings",
                        production_boundary_static_scan.get("blocking_finding_count"),
                    ),
                    (
                        "warning_findings",
                        production_boundary_static_scan.get("warning_finding_count"),
                    ),
                    (
                        "allowed_matches",
                        production_boundary_static_scan.get("allowed_match_count"),
                    ),
                    (
                        "static_scan_input",
                        production_boundary_static_scan.get("static_scan_input"),
                    ),
                    ("next_action", production_boundary_static_scan.get("next_action")),
                    ("detail_report", production_boundary_static_scan.get("detail_report")),
                    ("production_effect", production_boundary_static_scan.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Owner Review Template V2",
            _definition_table(
                [
                    ("availability", owner_review_template_v2.get("availability")),
                    ("status", owner_review_template_v2.get("status")),
                    (
                        "template_status",
                        owner_review_template_v2.get("template_status"),
                    ),
                    (
                        "validation_status",
                        owner_review_template_v2.get("validation_status"),
                    ),
                    (
                        "required_fields",
                        owner_review_template_v2.get("required_field_count"),
                    ),
                    (
                        "owner_actions",
                        owner_review_template_v2.get("owner_action_count"),
                    ),
                    (
                        "review_record_supported",
                        owner_review_template_v2.get("optional_record_validation_supported"),
                    ),
                    ("next_action", owner_review_template_v2.get("next_action")),
                    ("detail_report", owner_review_template_v2.get("detail_report")),
                    ("production_effect", owner_review_template_v2.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Owner Decision Audit Log",
            _definition_table(
                [
                    ("availability", owner_decision_audit_log.get("availability")),
                    ("status", owner_decision_audit_log.get("status")),
                    (
                        "audit_log_status",
                        owner_decision_audit_log.get("audit_log_status"),
                    ),
                    (
                        "validation_status",
                        owner_decision_audit_log.get("validation_status"),
                    ),
                    ("records", owner_decision_audit_log.get("record_count")),
                    (
                        "latest_decision_id",
                        owner_decision_audit_log.get("latest_decision_id"),
                    ),
                    (
                        "latest_owner_action",
                        owner_decision_audit_log.get("latest_owner_action"),
                    ),
                    (
                        "latest_safety_status",
                        owner_decision_audit_log.get("latest_safety_status"),
                    ),
                    (
                        "monthly_review_pack_input",
                        owner_decision_audit_log.get("monthly_review_pack_input"),
                    ),
                    (
                        "promotion_board_input",
                        owner_decision_audit_log.get("promotion_board_input"),
                    ),
                    ("next_action", owner_decision_audit_log.get("next_action")),
                    ("detail_report", owner_decision_audit_log.get("detail_report")),
                    ("production_effect", owner_decision_audit_log.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Research Monthly Review Pack",
            _definition_table(
                [
                    ("availability", research_monthly_review_pack.get("availability")),
                    ("status", research_monthly_review_pack.get("status")),
                    (
                        "monthly_review_status",
                        research_monthly_review_pack.get("monthly_review_status"),
                    ),
                    (
                        "validation_status",
                        research_monthly_review_pack.get("validation_status"),
                    ),
                    (
                        "active_candidates",
                        research_monthly_review_pack.get("active_candidate_count"),
                    ),
                    (
                        "paper_shadow_candidates",
                        research_monthly_review_pack.get("paper_shadow_candidate_count"),
                    ),
                    (
                        "needs_evidence_candidates",
                        research_monthly_review_pack.get("needs_evidence_candidate_count"),
                    ),
                    (
                        "major_blockers",
                        research_monthly_review_pack.get("major_blocker_count"),
                    ),
                    (
                        "major_warnings",
                        research_monthly_review_pack.get("major_warning_count"),
                    ),
                    (
                        "safety_audit_status",
                        research_monthly_review_pack.get("safety_audit_status"),
                    ),
                    (
                        "data_governance_status",
                        research_monthly_review_pack.get("data_governance_status"),
                    ),
                    (
                        "owner_decision_status",
                        research_monthly_review_pack.get("owner_decision_status"),
                    ),
                    ("next_action", research_monthly_review_pack.get("next_action")),
                    ("detail_report", research_monthly_review_pack.get("detail_report")),
                    (
                        "production_effect",
                        research_monthly_review_pack.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Paper Shadow Promotion Board",
            _definition_table(
                [
                    ("availability", paper_shadow_promotion_board.get("availability")),
                    ("status", paper_shadow_promotion_board.get("status")),
                    ("board_decision", paper_shadow_promotion_board.get("board_decision")),
                    (
                        "validation_status",
                        paper_shadow_promotion_board.get("validation_status"),
                    ),
                    ("candidate_id", paper_shadow_promotion_board.get("candidate_id")),
                    (
                        "evidence_checks",
                        paper_shadow_promotion_board.get("evidence_check_count"),
                    ),
                    (
                        "passed_evidence",
                        paper_shadow_promotion_board.get("passed_evidence_count"),
                    ),
                    (
                        "blocked_evidence",
                        paper_shadow_promotion_board.get("blocked_evidence_count"),
                    ),
                    (
                        "warning_evidence",
                        paper_shadow_promotion_board.get("warning_evidence_count"),
                    ),
                    ("safety_status", paper_shadow_promotion_board.get("safety_status")),
                    (
                        "readiness_status",
                        paper_shadow_promotion_board.get("readiness_status"),
                    ),
                    (
                        "owner_decision_status",
                        paper_shadow_promotion_board.get("owner_decision_status"),
                    ),
                    ("next_action", paper_shadow_promotion_board.get("next_action")),
                    ("detail_report", paper_shadow_promotion_board.get("detail_report")),
                    (
                        "production_effect",
                        paper_shadow_promotion_board.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Candidate Rejection Postmortem Template",
            _definition_table(
                [
                    ("availability", candidate_rejection_postmortem.get("availability")),
                    ("status", candidate_rejection_postmortem.get("status")),
                    (
                        "template_status",
                        candidate_rejection_postmortem.get("template_status"),
                    ),
                    (
                        "validation_status",
                        candidate_rejection_postmortem.get("validation_status"),
                    ),
                    (
                        "postmortem_record_provided",
                        candidate_rejection_postmortem.get("postmortem_record_provided"),
                    ),
                    (
                        "filled_postmortem_status",
                        candidate_rejection_postmortem.get("filled_postmortem_status"),
                    ),
                    ("candidate_id", candidate_rejection_postmortem.get("candidate_id")),
                    (
                        "required_sections",
                        candidate_rejection_postmortem.get("required_section_count"),
                    ),
                    (
                        "failed_evidence_gates",
                        candidate_rejection_postmortem.get("failed_evidence_gate_count"),
                    ),
                    (
                        "failed_stress_scenarios",
                        candidate_rejection_postmortem.get("failed_stress_scenario_count"),
                    ),
                    (
                        "data_quality_issues",
                        candidate_rejection_postmortem.get("data_quality_issue_count"),
                    ),
                    (
                        "safety_boundary_issues",
                        candidate_rejection_postmortem.get("safety_boundary_issue_count"),
                    ),
                    ("can_revisit", candidate_rejection_postmortem.get("can_revisit")),
                    ("next_action", candidate_rejection_postmortem.get("next_action")),
                    ("detail_report", candidate_rejection_postmortem.get("detail_report")),
                    (
                        "production_effect",
                        candidate_rejection_postmortem.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Decision Snapshot Lifecycle Policy",
            _definition_table(
                [
                    (
                        "availability",
                        decision_snapshot_lifecycle_policy.get("availability"),
                    ),
                    ("status", decision_snapshot_lifecycle_policy.get("status")),
                    (
                        "snapshot_lifecycle_status",
                        decision_snapshot_lifecycle_policy.get(
                            "snapshot_lifecycle_status"
                        ),
                    ),
                    (
                        "validation_status",
                        decision_snapshot_lifecycle_policy.get("validation_status"),
                    ),
                    (
                        "target_as_of",
                        decision_snapshot_lifecycle_policy.get("target_as_of"),
                    ),
                    (
                        "context_mode",
                        decision_snapshot_lifecycle_policy.get("context_mode"),
                    ),
                    (
                        "snapshot_exists",
                        decision_snapshot_lifecycle_policy.get("snapshot_exists"),
                    ),
                    (
                        "snapshot_signal_date",
                        decision_snapshot_lifecycle_policy.get("snapshot_signal_date"),
                    ),
                    (
                        "latest_available_snapshot_date",
                        decision_snapshot_lifecycle_policy.get(
                            "latest_available_snapshot_date"
                        ),
                    ),
                    (
                        "blocking_impact",
                        decision_snapshot_lifecycle_policy.get("blocking_impact"),
                    ),
                    (
                        "next_action",
                        decision_snapshot_lifecycle_policy.get("next_action"),
                    ),
                    (
                        "detail_report",
                        decision_snapshot_lifecycle_policy.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        decision_snapshot_lifecycle_policy.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Extended Shadow Observation Clock",
            _definition_table(
                [
                    ("availability", extended_shadow_observation_clock.get("availability")),
                    ("status", extended_shadow_observation_clock.get("status")),
                    (
                        "observation_clock_status",
                        extended_shadow_observation_clock.get("observation_clock_status"),
                    ),
                    (
                        "validation_status",
                        extended_shadow_observation_clock.get("validation_status"),
                    ),
                    ("candidate_id", extended_shadow_observation_clock.get("candidate_id")),
                    (
                        "observation_start_date",
                        extended_shadow_observation_clock.get("observation_start_date"),
                    ),
                    ("current_count", extended_shadow_observation_clock.get("current_count")),
                    ("required_count", extended_shadow_observation_clock.get("required_count")),
                    (
                        "missing_day_count",
                        extended_shadow_observation_clock.get("missing_day_count"),
                    ),
                    (
                        "invalid_day_count",
                        extended_shadow_observation_clock.get("invalid_day_count"),
                    ),
                    ("next_action", extended_shadow_observation_clock.get("next_action")),
                    ("detail_report", extended_shadow_observation_clock.get("detail_report")),
                    (
                        "production_effect",
                        extended_shadow_observation_clock.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Extended Shadow Protocol",
            _definition_table(
                [
                    ("availability", extended_shadow_protocol.get("availability")),
                    ("status", extended_shadow_protocol.get("status")),
                    (
                        "eligibility_status",
                        extended_shadow_protocol.get("eligibility_status"),
                    ),
                    (
                        "validation_status",
                        extended_shadow_protocol.get("validation_status"),
                    ),
                    ("candidate_id", extended_shadow_protocol.get("candidate_id")),
                    (
                        "observed_trading_days",
                        extended_shadow_protocol.get("observed_trading_days"),
                    ),
                    (
                        "minimum_observation_trading_days",
                        extended_shadow_protocol.get("minimum_observation_trading_days"),
                    ),
                    ("checks", extended_shadow_protocol.get("check_count")),
                    ("blocked_checks", extended_shadow_protocol.get("blocked_check_count")),
                    ("warning_checks", extended_shadow_protocol.get("warning_check_count")),
                    ("safety_status", extended_shadow_protocol.get("safety_status")),
                    ("readiness_status", extended_shadow_protocol.get("readiness_status")),
                    (
                        "owner_decision_status",
                        extended_shadow_protocol.get("owner_decision_status"),
                    ),
                    ("lineage_status", extended_shadow_protocol.get("lineage_status")),
                    ("next_action", extended_shadow_protocol.get("next_action")),
                    ("detail_report", extended_shadow_protocol.get("detail_report")),
                    ("production_effect", extended_shadow_protocol.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Research Roadmap Dashboard",
            _definition_table(
                [
                    ("availability", research_roadmap_dashboard.get("availability")),
                    ("status", research_roadmap_dashboard.get("status")),
                    (
                        "dashboard_status",
                        research_roadmap_dashboard.get("dashboard_status"),
                    ),
                    (
                        "validation_status",
                        research_roadmap_dashboard.get("validation_status"),
                    ),
                    (
                        "active_tasks",
                        research_roadmap_dashboard.get("active_task_count"),
                    ),
                    (
                        "completed_tasks",
                        research_roadmap_dashboard.get("completed_task_count"),
                    ),
                    (
                        "open_blockers",
                        research_roadmap_dashboard.get("open_blocker_count"),
                    ),
                    (
                        "stale_artifacts",
                        research_roadmap_dashboard.get("stale_artifact_count"),
                    ),
                    (
                        "missing_artifacts",
                        research_roadmap_dashboard.get("missing_artifact_count"),
                    ),
                    (
                        "active_candidates",
                        research_roadmap_dashboard.get("active_candidate_count"),
                    ),
                    (
                        "paper_shadow_status",
                        research_roadmap_dashboard.get("paper_shadow_status"),
                    ),
                    (
                        "data_governance_status",
                        research_roadmap_dashboard.get("data_governance_status"),
                    ),
                    ("safety_status", research_roadmap_dashboard.get("safety_status")),
                    ("lineage_status", research_roadmap_dashboard.get("lineage_status")),
                    ("top_next_task", research_roadmap_dashboard.get("top_next_task")),
                    ("next_action", research_roadmap_dashboard.get("next_action")),
                    ("detail_report", research_roadmap_dashboard.get("detail_report")),
                    (
                        "production_effect",
                        research_roadmap_dashboard.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Research Governance End-to-End Pack",
            _definition_table(
                [
                    (
                        "availability",
                        research_governance_end_to_end_pack.get("availability"),
                    ),
                    ("status", research_governance_end_to_end_pack.get("status")),
                    (
                        "overall_governance_status",
                        research_governance_end_to_end_pack.get(
                            "overall_governance_status"
                        ),
                    ),
                    (
                        "validation_status",
                        research_governance_end_to_end_pack.get("validation_status"),
                    ),
                    (
                        "source_reports",
                        research_governance_end_to_end_pack.get("source_report_count"),
                    ),
                    (
                        "available_sources",
                        research_governance_end_to_end_pack.get(
                            "available_source_count"
                        ),
                    ),
                    (
                        "blockers",
                        research_governance_end_to_end_pack.get("blocking_item_count"),
                    ),
                    (
                        "warnings",
                        research_governance_end_to_end_pack.get("warning_item_count"),
                    ),
                    (
                        "manual_review_items",
                        research_governance_end_to_end_pack.get(
                            "manual_review_item_count"
                        ),
                    ),
                    (
                        "top_blocker",
                        research_governance_end_to_end_pack.get("top_blocker"),
                    ),
                    ("next_action", research_governance_end_to_end_pack.get("next_action")),
                    (
                        "detail_report",
                        research_governance_end_to_end_pack.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        research_governance_end_to_end_pack.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Research Governance Recovery Pack",
            _definition_table(
                [
                    (
                        "availability",
                        research_governance_recovery_pack.get("availability"),
                    ),
                    ("status", research_governance_recovery_pack.get("status")),
                    (
                        "recovery_governance_status",
                        research_governance_recovery_pack.get(
                            "recovery_governance_status"
                        ),
                    ),
                    (
                        "validation_status",
                        research_governance_recovery_pack.get("validation_status"),
                    ),
                    (
                        "source_reports",
                        research_governance_recovery_pack.get("source_report_count"),
                    ),
                    (
                        "available_sources",
                        research_governance_recovery_pack.get(
                            "available_source_count"
                        ),
                    ),
                    (
                        "remaining_blockers",
                        research_governance_recovery_pack.get("remaining_blocker_count"),
                    ),
                    (
                        "remaining_warnings",
                        research_governance_recovery_pack.get("remaining_warning_count"),
                    ),
                    (
                        "manual_review_items",
                        research_governance_recovery_pack.get(
                            "manual_review_item_count"
                        ),
                    ),
                    (
                        "normal_paper_shadow_may_resume",
                        research_governance_recovery_pack.get(
                            "normal_paper_shadow_may_resume"
                        ),
                    ),
                    (
                        "extended_shadow_remains_forbidden",
                        research_governance_recovery_pack.get(
                            "extended_shadow_remains_forbidden"
                        ),
                    ),
                    (
                        "live_trading_remains_forbidden",
                        research_governance_recovery_pack.get(
                            "live_trading_remains_forbidden"
                        ),
                    ),
                    (
                        "top_blocker",
                        research_governance_recovery_pack.get("top_remaining_blocker"),
                    ),
                    ("next_action", research_governance_recovery_pack.get("next_action")),
                    (
                        "detail_report",
                        research_governance_recovery_pack.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        research_governance_recovery_pack.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Decision-Stage Governance Snapshot",
            _definition_table(
                [
                    (
                        "availability",
                        decision_stage_governance_snapshot.get("availability"),
                    ),
                    ("status", decision_stage_governance_snapshot.get("status")),
                    (
                        "snapshot_status",
                        decision_stage_governance_snapshot.get("snapshot_status"),
                    ),
                    (
                        "blockers",
                        decision_stage_governance_snapshot.get("blocker_count"),
                    ),
                    (
                        "warnings",
                        decision_stage_governance_snapshot.get("warning_count"),
                    ),
                    (
                        "recommended_owner_action",
                        decision_stage_governance_snapshot.get(
                            "recommended_owner_action"
                        ),
                    ),
                    (
                        "normal_shadow_may_resume",
                        decision_stage_governance_snapshot.get(
                            "normal_shadow_may_resume"
                        ),
                    ),
                    (
                        "extended_shadow_remains_forbidden",
                        decision_stage_governance_snapshot.get(
                            "extended_shadow_remains_forbidden"
                        ),
                    ),
                    (
                        "live_trading_remains_forbidden",
                        decision_stage_governance_snapshot.get(
                            "live_trading_remains_forbidden"
                        ),
                    ),
                    (
                        "owner_decision_append_allowed",
                        decision_stage_governance_snapshot.get(
                            "owner_decision_append_allowed"
                        ),
                    ),
                    (
                        "dry_run_written",
                        decision_stage_governance_snapshot.get("dry_run_written"),
                    ),
                    (
                        "next_action",
                        decision_stage_governance_snapshot.get("next_action"),
                    ),
                    (
                        "detail_report",
                        decision_stage_governance_snapshot.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        decision_stage_governance_snapshot.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Return-To-Research Governance Snapshot",
            _definition_table(
                [
                    (
                        "availability",
                        return_to_research_governance_snapshot.get("availability"),
                    ),
                    ("status", return_to_research_governance_snapshot.get("status")),
                    (
                        "return_to_research_status",
                        return_to_research_governance_snapshot.get(
                            "return_to_research_status"
                        ),
                    ),
                    (
                        "candidate_status",
                        return_to_research_governance_snapshot.get("candidate_status"),
                    ),
                    (
                        "owner_decision_id",
                        return_to_research_governance_snapshot.get("owner_decision_id"),
                    ),
                    (
                        "normal_paper_shadow_active",
                        return_to_research_governance_snapshot.get(
                            "normal_paper_shadow_active"
                        ),
                    ),
                    (
                        "extended_shadow_allowed",
                        return_to_research_governance_snapshot.get(
                            "extended_shadow_allowed"
                        ),
                    ),
                    (
                        "live_trading_allowed",
                        return_to_research_governance_snapshot.get(
                            "live_trading_allowed"
                        ),
                    ),
                    (
                        "candidate_rejected",
                        return_to_research_governance_snapshot.get(
                            "candidate_rejected"
                        ),
                    ),
                    (
                        "hypothesis_count",
                        return_to_research_governance_snapshot.get("hypothesis_count"),
                    ),
                    (
                        "next_candidate_id",
                        return_to_research_governance_snapshot.get(
                            "next_candidate_id"
                        ),
                    ),
                    (
                        "validation_status",
                        return_to_research_governance_snapshot.get("validation_status"),
                    ),
                    (
                        "next_action",
                        return_to_research_governance_snapshot.get("next_action"),
                    ),
                    (
                        "detail_report",
                        return_to_research_governance_snapshot.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        return_to_research_governance_snapshot.get(
                            "production_effect"
                        ),
                    ),
                ]
            ),
        ),
        _section(
            "Next Research Cycle Snapshot",
            _definition_table(
                [
                    (
                        "availability",
                        next_research_cycle_snapshot.get("availability"),
                    ),
                    ("status", next_research_cycle_snapshot.get("status")),
                    (
                        "research_cycle_snapshot_status",
                        next_research_cycle_snapshot.get(
                            "research_cycle_snapshot_status"
                        ),
                    ),
                    (
                        "research_gate_decision",
                        next_research_cycle_snapshot.get("research_gate_decision"),
                    ),
                    (
                        "candidate_id",
                        next_research_cycle_snapshot.get("candidate_id"),
                    ),
                    (
                        "market_regime",
                        next_research_cycle_snapshot.get("market_regime"),
                    ),
                    (
                        "requested_date_range",
                        next_research_cycle_snapshot.get("requested_date_range"),
                    ),
                    (
                        "owner_packet_ready",
                        next_research_cycle_snapshot.get("owner_packet_ready"),
                    ),
                    (
                        "paper_shadow_activation_allowed",
                        next_research_cycle_snapshot.get(
                            "paper_shadow_activation_allowed"
                        ),
                    ),
                    (
                        "extended_shadow_allowed",
                        next_research_cycle_snapshot.get("extended_shadow_allowed"),
                    ),
                    (
                        "live_trading_allowed",
                        next_research_cycle_snapshot.get("live_trading_allowed"),
                    ),
                    (
                        "official_target_weights_generated",
                        next_research_cycle_snapshot.get(
                            "official_target_weights_generated"
                        ),
                    ),
                    (
                        "broker_order_allowed",
                        next_research_cycle_snapshot.get("broker_order_allowed"),
                    ),
                    (
                        "validation_status",
                        next_research_cycle_snapshot.get("validation_status"),
                    ),
                    ("next_action", next_research_cycle_snapshot.get("next_action")),
                    ("detail_report", next_research_cycle_snapshot.get("detail_report")),
                    (
                        "production_effect",
                        next_research_cycle_snapshot.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Next Candidate Backfill",
            _definition_table(
                [
                    ("availability", next_candidate_backfill.get("availability")),
                    ("status", next_candidate_backfill.get("status")),
                    (
                        "candidate_backfill_status",
                        next_candidate_backfill.get("candidate_backfill_status"),
                    ),
                    ("candidate_id", next_candidate_backfill.get("candidate_id")),
                    ("backfill_metric_mode", next_candidate_backfill.get("backfill_metric_mode")),
                    (
                        "real_metrics_generated",
                        next_candidate_backfill.get("real_metrics_generated"),
                    ),
                    (
                        "aggregate_return_proxy",
                        next_candidate_backfill.get("aggregate_return_proxy"),
                    ),
                    (
                        "aggregate_drawdown_proxy",
                        next_candidate_backfill.get("aggregate_drawdown_proxy"),
                    ),
                    ("turnover_proxy", next_candidate_backfill.get("turnover_proxy")),
                    ("rotation_count", next_candidate_backfill.get("rotation_count")),
                    (
                        "false_risk_off_count",
                        next_candidate_backfill.get("false_risk_off_count"),
                    ),
                    (
                        "signal_completeness",
                        next_candidate_backfill.get("signal_completeness"),
                    ),
                    ("missing_data_count", next_candidate_backfill.get("missing_data_count")),
                    ("partial_reason_count", next_candidate_backfill.get("partial_reason_count")),
                    ("safety_audit_status", next_candidate_backfill.get("safety_audit_status")),
                    ("validation_status", next_candidate_backfill.get("validation_status")),
                    ("next_action", next_candidate_backfill.get("next_action")),
                    ("detail_report", next_candidate_backfill.get("detail_report")),
                    ("production_effect", next_candidate_backfill.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Next Candidate Stress Cost Benchmark",
            _definition_table(
                [
                    (
                        "availability",
                        next_candidate_stress_cost_benchmark.get("availability"),
                    ),
                    (
                        "stress_status",
                        next_candidate_stress_cost_benchmark.get("stress_status"),
                    ),
                    (
                        "cost_benchmark_status",
                        next_candidate_stress_cost_benchmark.get(
                            "cost_benchmark_status"
                        ),
                    ),
                    (
                        "cost_survival_status",
                        next_candidate_stress_cost_benchmark.get(
                            "cost_survival_status"
                        ),
                    ),
                    (
                        "benchmark_relative_status",
                        next_candidate_stress_cost_benchmark.get(
                            "benchmark_relative_status"
                        ),
                    ),
                    (
                        "source_backfill_status",
                        next_candidate_stress_cost_benchmark.get(
                            "source_backfill_status"
                        ),
                    ),
                    (
                        "stress_validation_status",
                        next_candidate_stress_cost_benchmark.get(
                            "stress_validation_status"
                        ),
                    ),
                    (
                        "cost_validation_status",
                        next_candidate_stress_cost_benchmark.get(
                            "cost_validation_status"
                        ),
                    ),
                    (
                        "major_blocker_count",
                        next_candidate_stress_cost_benchmark.get("major_blocker_count"),
                    ),
                    (
                        "major_warning_count",
                        next_candidate_stress_cost_benchmark.get("major_warning_count"),
                    ),
                    (
                        "next_action",
                        next_candidate_stress_cost_benchmark.get("next_action"),
                    ),
                    (
                        "stress_detail_report",
                        next_candidate_stress_cost_benchmark.get("stress_detail_report"),
                    ),
                    (
                        "cost_detail_report",
                        next_candidate_stress_cost_benchmark.get("cost_detail_report"),
                    ),
                    (
                        "production_effect",
                        next_candidate_stress_cost_benchmark.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Next Candidate Vs Returned Comparison",
            _definition_table(
                [
                    (
                        "availability",
                        next_candidate_vs_returned_comparison.get("availability"),
                    ),
                    ("status", next_candidate_vs_returned_comparison.get("status")),
                    (
                        "comparison_result",
                        next_candidate_vs_returned_comparison.get("comparison_result"),
                    ),
                    (
                        "real_metrics_available",
                        next_candidate_vs_returned_comparison.get(
                            "real_metrics_available"
                        ),
                    ),
                    (
                        "source_backfill_status",
                        next_candidate_vs_returned_comparison.get(
                            "source_backfill_status"
                        ),
                    ),
                    (
                        "stress_result",
                        next_candidate_vs_returned_comparison.get("stress_result"),
                    ),
                    (
                        "benchmark_relative_status",
                        next_candidate_vs_returned_comparison.get(
                            "benchmark_relative_status"
                        ),
                    ),
                    (
                        "repeated_failure_mode_count",
                        next_candidate_vs_returned_comparison.get(
                            "repeated_failure_mode_count"
                        ),
                    ),
                    (
                        "improved_metric_count",
                        next_candidate_vs_returned_comparison.get(
                            "improved_metric_count"
                        ),
                    ),
                    (
                        "mixed_metric_count",
                        next_candidate_vs_returned_comparison.get("mixed_metric_count"),
                    ),
                    (
                        "no_improvement_count",
                        next_candidate_vs_returned_comparison.get(
                            "no_improvement_count"
                        ),
                    ),
                    (
                        "validation_status",
                        next_candidate_vs_returned_comparison.get("validation_status"),
                    ),
                    (
                        "next_action",
                        next_candidate_vs_returned_comparison.get("next_action"),
                    ),
                    (
                        "detail_report",
                        next_candidate_vs_returned_comparison.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        next_candidate_vs_returned_comparison.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Next Candidate Signal Window Sensitivity",
            _definition_table(
                [
                    (
                        "availability",
                        next_candidate_signal_window_sensitivity.get("availability"),
                    ),
                    (
                        "signal_status",
                        next_candidate_signal_window_sensitivity.get("signal_status"),
                    ),
                    (
                        "window_status",
                        next_candidate_signal_window_sensitivity.get("window_status"),
                    ),
                    (
                        "signal_validation_status",
                        next_candidate_signal_window_sensitivity.get(
                            "signal_validation_status"
                        ),
                    ),
                    (
                        "window_validation_status",
                        next_candidate_signal_window_sensitivity.get(
                            "window_validation_status"
                        ),
                    ),
                    (
                        "source_signal_binding_status",
                        next_candidate_signal_window_sensitivity.get(
                            "source_signal_binding_status"
                        ),
                    ),
                    (
                        "source_backfill_status",
                        next_candidate_signal_window_sensitivity.get(
                            "source_backfill_status"
                        ),
                    ),
                    (
                        "signal_blocking_check_count",
                        next_candidate_signal_window_sensitivity.get(
                            "signal_blocking_check_count"
                        ),
                    ),
                    (
                        "window_weak_split_count",
                        next_candidate_signal_window_sensitivity.get(
                            "window_weak_split_count"
                        ),
                    ),
                    (
                        "window_partial_static_proxy_split_count",
                        next_candidate_signal_window_sensitivity.get(
                            "window_partial_static_proxy_split_count"
                        ),
                    ),
                    (
                        "overfit_risk",
                        next_candidate_signal_window_sensitivity.get("overfit_risk"),
                    ),
                    (
                        "next_action",
                        next_candidate_signal_window_sensitivity.get("next_action"),
                    ),
                    (
                        "signal_detail_report",
                        next_candidate_signal_window_sensitivity.get(
                            "signal_detail_report"
                        ),
                    ),
                    (
                        "window_detail_report",
                        next_candidate_signal_window_sensitivity.get(
                            "window_detail_report"
                        ),
                    ),
                    (
                        "production_effect",
                        next_candidate_signal_window_sensitivity.get(
                            "production_effect"
                        ),
                    ),
                ]
            ),
        ),
        _section(
            "Next Candidate Research Gate",
            _definition_table(
                [
                    ("availability", next_candidate_research_gate.get("availability")),
                    ("status", next_candidate_research_gate.get("status")),
                    (
                        "research_gate_decision",
                        next_candidate_research_gate.get("research_gate_decision"),
                    ),
                    (
                        "validation_status",
                        next_candidate_research_gate.get("validation_status"),
                    ),
                    (
                        "safety_audit_status",
                        next_candidate_research_gate.get("safety_audit_status"),
                    ),
                    (
                        "source_backfill_status",
                        next_candidate_research_gate.get("source_backfill_status"),
                    ),
                    (
                        "signal_robustness_status",
                        next_candidate_research_gate.get("signal_robustness_status"),
                    ),
                    (
                        "window_sensitivity_status",
                        next_candidate_research_gate.get("window_sensitivity_status"),
                    ),
                    ("blocker_count", next_candidate_research_gate.get("blocker_count")),
                    (
                        "paper_shadow_activation_allowed",
                        next_candidate_research_gate.get(
                            "paper_shadow_activation_allowed"
                        ),
                    ),
                    (
                        "official_target_weights_generated",
                        next_candidate_research_gate.get(
                            "official_target_weights_generated"
                        ),
                    ),
                    (
                        "broker_order_allowed",
                        next_candidate_research_gate.get("broker_order_allowed"),
                    ),
                    ("next_action", next_candidate_research_gate.get("next_action")),
                    ("detail_report", next_candidate_research_gate.get("detail_report")),
                    (
                        "production_effect",
                        next_candidate_research_gate.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Next Candidate Owner Research Review Packet",
            _definition_table(
                [
                    (
                        "availability",
                        next_candidate_owner_research_review_packet.get("availability"),
                    ),
                    ("status", next_candidate_owner_research_review_packet.get("status")),
                    (
                        "validation_status",
                        next_candidate_owner_research_review_packet.get(
                            "validation_status"
                        ),
                    ),
                    (
                        "source_research_gate_decision",
                        next_candidate_owner_research_review_packet.get(
                            "source_research_gate_decision"
                        ),
                    ),
                    (
                        "source_gate_blocker_count",
                        next_candidate_owner_research_review_packet.get(
                            "source_gate_blocker_count"
                        ),
                    ),
                    (
                        "option_count",
                        next_candidate_owner_research_review_packet.get("option_count"),
                    ),
                    (
                        "owner_decision_appended",
                        next_candidate_owner_research_review_packet.get(
                            "owner_decision_appended"
                        ),
                    ),
                    (
                        "paper_shadow_activation_allowed",
                        next_candidate_owner_research_review_packet.get(
                            "paper_shadow_activation_allowed"
                        ),
                    ),
                    (
                        "official_target_weights_generated",
                        next_candidate_owner_research_review_packet.get(
                            "official_target_weights_generated"
                        ),
                    ),
                    (
                        "broker_order_allowed",
                        next_candidate_owner_research_review_packet.get(
                            "broker_order_allowed"
                        ),
                    ),
                    (
                        "next_action",
                        next_candidate_owner_research_review_packet.get("next_action"),
                    ),
                    (
                        "detail_report",
                        next_candidate_owner_research_review_packet.get("detail_report"),
                    ),
                    (
                        "production_effect",
                        next_candidate_owner_research_review_packet.get(
                            "production_effect"
                        ),
                    ),
                ]
            ),
        ),
        _section(
            "Executable Binding Contract",
            _definition_table(
                [
                    (
                        "availability",
                        executable_binding_contract.get("availability"),
                    ),
                    ("status", executable_binding_contract.get("status")),
                    (
                        "contract_status",
                        executable_binding_contract.get("contract_status"),
                    ),
                    (
                        "candidate_id",
                        executable_binding_contract.get("candidate_id"),
                    ),
                    (
                        "binding_version",
                        executable_binding_contract.get("binding_version"),
                    ),
                    (
                        "input_schema_id",
                        executable_binding_contract.get("input_schema_id"),
                    ),
                    (
                        "output_schema_id",
                        executable_binding_contract.get("output_schema_id"),
                    ),
                    (
                        "research_only",
                        executable_binding_contract.get("research_only"),
                    ),
                    (
                        "manual_review_only",
                        executable_binding_contract.get("manual_review_only"),
                    ),
                    (
                        "official_target_weights",
                        executable_binding_contract.get("official_target_weights"),
                    ),
                    (
                        "strategy_behavior_implemented",
                        executable_binding_contract.get("strategy_behavior_implemented"),
                    ),
                    (
                        "signal_binding_implemented",
                        executable_binding_contract.get("signal_binding_implemented"),
                    ),
                    (
                        "weight_binding_implemented",
                        executable_binding_contract.get("weight_binding_implemented"),
                    ),
                    (
                        "validation_status",
                        executable_binding_contract.get("validation_status"),
                    ),
                    ("next_action", executable_binding_contract.get("next_action")),
                    (
                        "detail_report",
                        executable_binding_contract.get("detail_report"),
                    ),
                    (
                        "validation_detail_report",
                        executable_binding_contract.get("validation_detail_report"),
                    ),
                    (
                        "production_effect",
                        executable_binding_contract.get("production_effect"),
                    ),
                    ("broker_effect", executable_binding_contract.get("broker_effect")),
                    ("order_effect", executable_binding_contract.get("order_effect")),
                ]
            ),
        ),
        _section(
            "Executable Signal Binding",
            _definition_table(
                [
                    ("availability", executable_signal_binding.get("availability")),
                    ("status", executable_signal_binding.get("status")),
                    (
                        "signal_binding_status",
                        executable_signal_binding.get("signal_binding_status"),
                    ),
                    ("candidate_id", executable_signal_binding.get("candidate_id")),
                    (
                        "binding_version",
                        executable_signal_binding.get("binding_version"),
                    ),
                    (
                        "latest_signal_date",
                        executable_signal_binding.get("latest_signal_date"),
                    ),
                    (
                        "signal_row_count",
                        executable_signal_binding.get("signal_row_count"),
                    ),
                    ("signal_score", executable_signal_binding.get("signal_score")),
                    ("regime_state", executable_signal_binding.get("regime_state")),
                    ("risk_state", executable_signal_binding.get("risk_state")),
                    ("rotation_state", executable_signal_binding.get("rotation_state")),
                    ("confidence", executable_signal_binding.get("confidence")),
                    ("uncertainty", executable_signal_binding.get("uncertainty")),
                    (
                        "blocking_reason",
                        executable_signal_binding.get("blocking_reason"),
                    ),
                    (
                        "validation_status",
                        executable_signal_binding.get("validation_status"),
                    ),
                    (
                        "official_target_weights",
                        executable_signal_binding.get("official_target_weights"),
                    ),
                    (
                        "hypothetical_research_weight_produced",
                        executable_signal_binding.get(
                            "hypothetical_research_weight_produced"
                        ),
                    ),
                    (
                        "backfill_metrics_produced",
                        executable_signal_binding.get("backfill_metrics_produced"),
                    ),
                    ("next_action", executable_signal_binding.get("next_action")),
                    ("detail_report", executable_signal_binding.get("detail_report")),
                    (
                        "validation_detail_report",
                        executable_signal_binding.get("validation_detail_report"),
                    ),
                    (
                        "production_effect",
                        executable_signal_binding.get("production_effect"),
                    ),
                    ("broker_effect", executable_signal_binding.get("broker_effect")),
                    ("order_effect", executable_signal_binding.get("order_effect")),
                ]
            ),
        ),
        _section(
            "Executable Research Weight Binding",
            _definition_table(
                [
                    (
                        "availability",
                        executable_research_weight_binding.get("availability"),
                    ),
                    ("status", executable_research_weight_binding.get("status")),
                    (
                        "research_weight_binding_status",
                        executable_research_weight_binding.get(
                            "research_weight_binding_status"
                        ),
                    ),
                    (
                        "candidate_id",
                        executable_research_weight_binding.get("candidate_id"),
                    ),
                    (
                        "binding_version",
                        executable_research_weight_binding.get("binding_version"),
                    ),
                    (
                        "latest_signal_date",
                        executable_research_weight_binding.get("latest_signal_date"),
                    ),
                    (
                        "weight_row_count",
                        executable_research_weight_binding.get("weight_row_count"),
                    ),
                    (
                        "risk_state",
                        executable_research_weight_binding.get("risk_state"),
                    ),
                    (
                        "rotation_state",
                        executable_research_weight_binding.get("rotation_state"),
                    ),
                    (
                        "turnover_proxy",
                        executable_research_weight_binding.get("turnover_proxy"),
                    ),
                    (
                        "constraint_hit_count",
                        executable_research_weight_binding.get("constraint_hit_count"),
                    ),
                    (
                        "blocking_reason",
                        executable_research_weight_binding.get("blocking_reason"),
                    ),
                    (
                        "validation_status",
                        executable_research_weight_binding.get("validation_status"),
                    ),
                    (
                        "official_target_weights",
                        executable_research_weight_binding.get("official_target_weights"),
                    ),
                    (
                        "broker_order_produced",
                        executable_research_weight_binding.get("broker_order_produced"),
                    ),
                    (
                        "backfill_metrics_produced",
                        executable_research_weight_binding.get(
                            "backfill_metrics_produced"
                        ),
                    ),
                    (
                        "next_action",
                        executable_research_weight_binding.get("next_action"),
                    ),
                    (
                        "detail_report",
                        executable_research_weight_binding.get("detail_report"),
                    ),
                    (
                        "validation_detail_report",
                        executable_research_weight_binding.get(
                            "validation_detail_report"
                        ),
                    ),
                    (
                        "production_effect",
                        executable_research_weight_binding.get("production_effect"),
                    ),
                    (
                        "broker_effect",
                        executable_research_weight_binding.get("broker_effect"),
                    ),
                    (
                        "order_effect",
                        executable_research_weight_binding.get("order_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Executable Binding Safety Audit",
            _definition_table(
                [
                    ("availability", executable_binding_safety_audit.get("availability")),
                    ("status", executable_binding_safety_audit.get("status")),
                    (
                        "safety_audit_status",
                        executable_binding_safety_audit.get("safety_audit_status"),
                    ),
                    (
                        "candidate_id",
                        executable_binding_safety_audit.get("candidate_id"),
                    ),
                    (
                        "acceptable_warning",
                        executable_binding_safety_audit.get("acceptable_warning"),
                    ),
                    (
                        "artifact_check_count",
                        executable_binding_safety_audit.get("artifact_check_count"),
                    ),
                    (
                        "failed_artifact_check_count",
                        executable_binding_safety_audit.get(
                            "failed_artifact_check_count"
                        ),
                    ),
                    (
                        "static_scan_finding_count",
                        executable_binding_safety_audit.get(
                            "static_scan_finding_count"
                        ),
                    ),
                    (
                        "blocking_static_finding_count",
                        executable_binding_safety_audit.get(
                            "blocking_static_finding_count"
                        ),
                    ),
                    (
                        "warning_static_finding_count",
                        executable_binding_safety_audit.get(
                            "warning_static_finding_count"
                        ),
                    ),
                    (
                        "validation_status",
                        executable_binding_safety_audit.get("validation_status"),
                    ),
                    ("next_action", executable_binding_safety_audit.get("next_action")),
                    (
                        "detail_report",
                        executable_binding_safety_audit.get("detail_report"),
                    ),
                    (
                        "validation_detail_report",
                        executable_binding_safety_audit.get(
                            "validation_detail_report"
                        ),
                    ),
                    (
                        "production_effect",
                        executable_binding_safety_audit.get("production_effect"),
                    ),
                    ("broker_effect", executable_binding_safety_audit.get("broker_effect")),
                    ("order_effect", executable_binding_safety_audit.get("order_effect")),
                ]
            ),
        ),
        _section(
            "Research Safety Boundary Audit",
            _definition_table(
                [
                    ("availability", research_safety_boundary_audit.get("availability")),
                    ("status", research_safety_boundary_audit.get("status")),
                    (
                        "safety_status",
                        research_safety_boundary_audit.get("safety_status"),
                    ),
                    (
                        "task_checks",
                        research_safety_boundary_audit.get("task_check_count"),
                    ),
                    (
                        "artifact_checks",
                        research_safety_boundary_audit.get("artifact_check_count"),
                    ),
                    (
                        "unsafe_signals",
                        research_safety_boundary_audit.get("unsafe_signal_count"),
                    ),
                    (
                        "missing_metadata",
                        research_safety_boundary_audit.get("missing_metadata_count"),
                    ),
                    (
                        "shadow_readiness_input",
                        research_safety_boundary_audit.get(
                            "shadow_continuation_readiness_input"
                        ),
                    ),
                    (
                        "promotion_board_input",
                        research_safety_boundary_audit.get("future_promotion_board_input"),
                    ),
                    ("next_action", research_safety_boundary_audit.get("next_action")),
                    ("detail_report", research_safety_boundary_audit.get("detail_report")),
                    ("production_effect", research_safety_boundary_audit.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Artifact Lineage Graph",
            _definition_table(
                [
                    ("availability", artifact_lineage_graph.get("availability")),
                    ("status", artifact_lineage_graph.get("status")),
                    ("lineage_status", artifact_lineage_graph.get("lineage_status")),
                    (
                        "available_required_families",
                        artifact_lineage_graph.get("available_required_family_count"),
                    ),
                    (
                        "required_families",
                        artifact_lineage_graph.get("required_family_count"),
                    ),
                    (
                        "passing_required_edges",
                        artifact_lineage_graph.get("passing_required_edge_count"),
                    ),
                    ("required_edges", artifact_lineage_graph.get("required_edge_count")),
                    ("blocking_issues", artifact_lineage_graph.get("blocking_issue_count")),
                    ("warning_issues", artifact_lineage_graph.get("warning_issue_count")),
                    ("next_action", artifact_lineage_graph.get("next_action")),
                    ("detail_report", artifact_lineage_graph.get("detail_report")),
                    ("production_effect", artifact_lineage_graph.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Task Register Consistency",
            _definition_table(
                [
                    ("availability", task_register_consistency.get("availability")),
                    ("status", task_register_consistency.get("status")),
                    (
                        "consistency_status",
                        task_register_consistency.get("consistency_status"),
                    ),
                    (
                        "validation_status",
                        task_register_consistency.get("validation_status"),
                    ),
                    ("active_tasks", task_register_consistency.get("active_task_count")),
                    (
                        "completed_tasks",
                        task_register_consistency.get("completed_task_count"),
                    ),
                    ("checks", task_register_consistency.get("check_count")),
                    ("failed_checks", task_register_consistency.get("failed_check_count")),
                    (
                        "blocking_issues",
                        task_register_consistency.get("blocking_issue_count"),
                    ),
                    (
                        "warning_issues",
                        task_register_consistency.get("warning_issue_count"),
                    ),
                    ("next_action", task_register_consistency.get("next_action")),
                    ("detail_report", task_register_consistency.get("detail_report")),
                    (
                        "validation_report",
                        task_register_consistency.get("validation_detail_report"),
                    ),
                    (
                        "production_effect",
                        task_register_consistency.get("production_effect"),
                    ),
                ]
            ),
        ),
        _section(
            "Report Quality Gate",
            _definition_table(
                [
                    ("availability", report_quality_gate.get("availability")),
                    ("status", report_quality_gate.get("status")),
                    ("report_quality_status", report_quality_gate.get("report_quality_status")),
                    ("checked_report_count", report_quality_gate.get("checked_report_count")),
                    ("missing_sections", report_quality_gate.get("missing_section_count")),
                    ("blocking_issues", report_quality_gate.get("blocking_quality_issue_count")),
                    ("warning_issues", report_quality_gate.get("warning_quality_issue_count")),
                    ("next_action", report_quality_gate.get("next_action")),
                    ("detail_report", report_quality_gate.get("detail_report")),
                    ("production_effect", report_quality_gate.get("production_effect")),
                ]
            ),
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
            + _artifact_impact_summary_html(_records(missing_impact.get("impact_summary")))
            + _artifact_impact_sections(_records(missing_impact.get("items"))),
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
            "Operations Health",
            _definition_table(
                [
                    ("availability", etf_operations_health.get("availability")),
                    ("status", etf_operations_health.get("status")),
                    ("summary", etf_operations_health.get("summary_sentence")),
                    ("cadence", etf_operations_health.get("cadence")),
                    ("pipeline_status", etf_operations_health.get("pipeline_status")),
                    (
                        "blocking_failures",
                        etf_operations_health.get("blocking_failure_count"),
                    ),
                    ("warnings", etf_operations_health.get("warning_count")),
                    ("stale_artifacts", etf_operations_health.get("stale_artifacts")),
                    ("missing_artifacts", etf_operations_health.get("missing_artifacts")),
                    (
                        "next_owner_review",
                        etf_operations_health.get("next_owner_review"),
                    ),
                    ("safety_status", etf_operations_health.get("safety_status")),
                    ("detailed_report", etf_operations_health.get("detail_report")),
                    ("production_effect", etf_operations_health.get("production_effect")),
                    ("broker_action", etf_operations_health.get("broker_action")),
                ]
            ),
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
        _section("Binding Gate Ladder", _gate_ladder_html(gates)),
        _section("Data Quality & PIT Safety", _definition_table(list(quality.items()))),
        _section(
            "Data Refresh Audit",
            _definition_table(
                [
                    ("availability", data_refresh_audit.get("availability")),
                    ("status", data_refresh_audit.get("status")),
                    ("validation_status", data_refresh_audit.get("validation_status")),
                    ("audit_record_count", data_refresh_audit.get("audit_record_count")),
                    ("failed_record_count", data_refresh_audit.get("failed_record_count")),
                    ("skipped_record_count", data_refresh_audit.get("skipped_record_count")),
                    (
                        "skipped_market_closed_count",
                        data_refresh_audit.get("skipped_market_closed_count"),
                    ),
                    (
                        "skipped_no_new_data_count",
                        data_refresh_audit.get("skipped_no_new_data_count"),
                    ),
                    ("warning_count", data_refresh_audit.get("warning_count")),
                    ("error_count", data_refresh_audit.get("error_count")),
                    ("next_action", data_refresh_audit.get("next_action")),
                    ("safety_status", data_refresh_audit.get("safety_status")),
                    ("report_path", data_refresh_audit.get("report_path")),
                    ("production_effect", data_refresh_audit.get("production_effect")),
                    ("limitation", data_refresh_audit.get("limitation")),
                ]
            ),
        ),
        _section(
            "Data Source Fallback Policy",
            _definition_table(
                [
                    ("availability", data_source_fallback_policy.get("availability")),
                    ("status", data_source_fallback_policy.get("status")),
                    (
                        "validation_status",
                        data_source_fallback_policy.get("validation_status"),
                    ),
                    ("fallback_status", data_source_fallback_policy.get("fallback_status")),
                    (
                        "fallback_used_count",
                        data_source_fallback_policy.get("fallback_used_count"),
                    ),
                    (
                        "fallback_used_sources",
                        data_source_fallback_policy.get("fallback_used_sources"),
                    ),
                    (
                        "blocking_data_types",
                        data_source_fallback_policy.get("blocking_data_types"),
                    ),
                    ("next_action", data_source_fallback_policy.get("next_action")),
                    ("safety_status", data_source_fallback_policy.get("safety_status")),
                    ("report_path", data_source_fallback_policy.get("report_path")),
                    (
                        "production_effect",
                        data_source_fallback_policy.get("production_effect"),
                    ),
                    ("limitation", data_source_fallback_policy.get("limitation")),
                ]
            ),
        ),
        _section(
            "Cache Catalog",
            _definition_table(
                [
                    ("availability", cache_catalog.get("availability")),
                    ("status", cache_catalog.get("status")),
                    ("validation_status", cache_catalog.get("validation_status")),
                    (
                        "cache_integrity_status",
                        cache_catalog.get("cache_integrity_status"),
                    ),
                    ("entry_count", cache_catalog.get("entry_count")),
                    (
                        "required_entry_count",
                        cache_catalog.get("required_entry_count"),
                    ),
                    (
                        "missing_required_count",
                        cache_catalog.get("missing_required_count"),
                    ),
                    (
                        "checksum_mismatch_count",
                        cache_catalog.get("checksum_mismatch_count"),
                    ),
                    (
                        "blocking_entry_ids",
                        cache_catalog.get("blocking_entry_ids"),
                    ),
                    ("refresh_audit_id", cache_catalog.get("refresh_audit_id")),
                    ("validated_at", cache_catalog.get("validated_at")),
                    ("next_action", cache_catalog.get("next_action")),
                    ("report_path", cache_catalog.get("report_path")),
                    ("production_effect", cache_catalog.get("production_effect")),
                    ("limitation", cache_catalog.get("limitation")),
                ]
            ),
        ),
        _section(
            "PIT Source Manifest",
            _definition_table(
                [
                    ("availability", pit_source_manifest.get("availability")),
                    ("status", pit_source_manifest.get("status")),
                    ("validation_status", pit_source_manifest.get("validation_status")),
                    ("source_count", pit_source_manifest.get("source_count")),
                    ("STRONG_PIT", pit_source_manifest.get("strong_pit_count")),
                    ("APPROX_PIT", pit_source_manifest.get("approx_pit_count")),
                    ("NON_PIT", pit_source_manifest.get("non_pit_count")),
                    ("UNKNOWN", pit_source_manifest.get("unknown_count")),
                    (
                        "non_strong_source_count",
                        pit_source_manifest.get("non_strong_source_count"),
                    ),
                    (
                        "non_strong_source_ids",
                        pit_source_manifest.get("non_strong_source_ids"),
                    ),
                    ("policy_version", pit_source_manifest.get("policy_version")),
                    ("safety_status", pit_source_manifest.get("safety_status")),
                    ("report_path", pit_source_manifest.get("report_path")),
                    ("production_effect", pit_source_manifest.get("production_effect")),
                    ("limitation", pit_source_manifest.get("limitation")),
                ]
            ),
        ),
        _section(
            "Parameter Shadow Review",
            _definition_table(
                [
                    ("availability", parameter_shadow.get("availability")),
                    ("status", parameter_shadow.get("status")),
                    ("backtest_mode", parameter_shadow.get("backtest_mode")),
                    ("promotion_eligibility", parameter_shadow.get("promotion_eligibility")),
                    ("signal_snapshot_status", parameter_shadow.get("signal_snapshot_status")),
                    ("real_signals_count", parameter_shadow.get("real_signals_count")),
                    ("fallback_signals_count", parameter_shadow.get("fallback_signals_count")),
                    ("missing_signals_count", parameter_shadow.get("missing_signals_count")),
                    ("data_quality_status", parameter_shadow.get("data_quality_status")),
                    ("data_quality_summary", parameter_shadow.get("data_quality_summary")),
                    ("promotion_status", parameter_shadow.get("promotion_status")),
                    ("baseline_version", parameter_shadow.get("baseline_version")),
                    ("candidate_version", parameter_shadow.get("candidate_version")),
                    ("annualized_return_delta", parameter_shadow.get("annualized_return_delta")),
                    ("max_drawdown_delta", parameter_shadow.get("max_drawdown_delta")),
                    ("sharpe_ratio_delta", parameter_shadow.get("sharpe_ratio_delta")),
                    ("turnover_delta", parameter_shadow.get("turnover_delta")),
                    ("signal_ablation_status", parameter_shadow.get("signal_ablation_status")),
                    (
                        "signal_ablation_summary",
                        parameter_shadow.get("signal_ablation_summary"),
                    ),
                    (
                        "signal_ablation_promotion_credit_signals",
                        parameter_shadow.get("signal_ablation_promotion_credit_signals"),
                    ),
                    (
                        "signal_ablation_negative_signals",
                        parameter_shadow.get("signal_ablation_negative_signals"),
                    ),
                    (
                        "signal_ablation_no_promotion_credit_reason",
                        parameter_shadow.get("signal_ablation_no_promotion_credit_reason"),
                    ),
                    (
                        "signal_ablation_implementation_warnings",
                        parameter_shadow.get("signal_ablation_implementation_warnings"),
                    ),
                    (
                        "signal_calibration_status",
                        parameter_shadow.get("signal_calibration_status"),
                    ),
                    (
                        "signal_calibration_summary",
                        parameter_shadow.get("signal_calibration_summary"),
                    ),
                    (
                        "signal_calibration_best_profile",
                        parameter_shadow.get("signal_calibration_best_profile"),
                    ),
                    (
                        "signal_calibration_profiles_tested",
                        parameter_shadow.get("signal_calibration_profiles_tested"),
                    ),
                    (
                        "signal_calibration_positive_signal_count",
                        parameter_shadow.get("signal_calibration_positive_signal_count"),
                    ),
                    (
                        "signal_calibration_promotion_credit_signal_count",
                        parameter_shadow.get("signal_calibration_promotion_credit_signal_count"),
                    ),
                    (
                        "signal_calibration_neutral_warning",
                        parameter_shadow.get("signal_calibration_neutral_warning"),
                    ),
                    (
                        "signal_calibration_correlation_warning",
                        parameter_shadow.get("signal_calibration_correlation_warning"),
                    ),
                    (
                        "portfolio_sensitivity_status",
                        parameter_shadow.get("portfolio_sensitivity_status"),
                    ),
                    (
                        "portfolio_sensitivity_summary",
                        parameter_shadow.get("portfolio_sensitivity_summary"),
                    ),
                    (
                        "portfolio_sensitivity_best_profile",
                        parameter_shadow.get("portfolio_sensitivity_best_profile"),
                    ),
                    (
                        "portfolio_sensitivity_primary_bottleneck",
                        parameter_shadow.get("portfolio_sensitivity_primary_bottleneck"),
                    ),
                    (
                        "portfolio_is_too_insensitive",
                        parameter_shadow.get("portfolio_is_too_insensitive"),
                    ),
                    (
                        "portfolio_candidates_status",
                        parameter_shadow.get("portfolio_candidates_status"),
                    ),
                    (
                        "portfolio_candidates_summary",
                        parameter_shadow.get("portfolio_candidates_summary"),
                    ),
                    (
                        "portfolio_candidates_best_profile",
                        parameter_shadow.get("portfolio_candidates_best_profile"),
                    ),
                    (
                        "portfolio_candidates_profiles_tested",
                        parameter_shadow.get("portfolio_candidates_profiles_tested"),
                    ),
                    (
                        "portfolio_candidates_guardrail_status",
                        parameter_shadow.get("portfolio_candidates_guardrail_status"),
                    ),
                    (
                        "portfolio_candidates_promotion_eligibility",
                        parameter_shadow.get("portfolio_candidates_promotion_eligibility"),
                    ),
                    (
                        "portfolio_candidate_review_status",
                        parameter_shadow.get("portfolio_candidate_review_status"),
                    ),
                    (
                        "portfolio_candidate_review_summary",
                        parameter_shadow.get("portfolio_candidate_review_summary"),
                    ),
                    (
                        "portfolio_candidate_review_profile",
                        parameter_shadow.get("portfolio_candidate_review_profile"),
                    ),
                    (
                        "portfolio_candidate_review_next_step",
                        parameter_shadow.get("portfolio_candidate_review_next_step"),
                    ),
                    (
                        "market_data_freshness_status",
                        parameter_shadow.get("market_data_freshness_status"),
                    ),
                    (
                        "market_data_freshness_summary",
                        parameter_shadow.get("market_data_freshness_summary"),
                    ),
                    (
                        "market_data_freshness_tracking_date",
                        parameter_shadow.get("market_data_freshness_tracking_date"),
                    ),
                    (
                        "market_data_freshness_effective_data_date",
                        parameter_shadow.get("market_data_freshness_effective_data_date"),
                    ),
                    (
                        "market_data_tracking_readiness",
                        parameter_shadow.get("market_data_tracking_readiness"),
                    ),
                    (
                        "market_data_refresh_status",
                        parameter_shadow.get("market_data_refresh_status"),
                    ),
                    (
                        "market_data_refresh_summary",
                        parameter_shadow.get("market_data_refresh_summary"),
                    ),
                    (
                        "market_data_refresh_target_date",
                        parameter_shadow.get("market_data_refresh_target_date"),
                    ),
                    (
                        "portfolio_candidate_tracking_status",
                        parameter_shadow.get("portfolio_candidate_tracking_status"),
                    ),
                    (
                        "portfolio_candidate_tracking_summary",
                        parameter_shadow.get("portfolio_candidate_tracking_summary"),
                    ),
                    (
                        "portfolio_candidate_tracking_effective_data_date",
                        parameter_shadow.get("portfolio_candidate_tracking_effective_data_date"),
                    ),
                    (
                        "portfolio_candidate_tracking_excess_return",
                        parameter_shadow.get("portfolio_candidate_tracking_excess_return"),
                    ),
                    (
                        "portfolio_tracking_review_recommendation",
                        parameter_shadow.get("portfolio_tracking_review_recommendation"),
                    ),
                    (
                        "portfolio_tracking_review_summary",
                        parameter_shadow.get("portfolio_tracking_review_summary"),
                    ),
                    (
                        "portfolio_tracking_review_tracking_days",
                        parameter_shadow.get("portfolio_tracking_review_tracking_days"),
                    ),
                    (
                        "portfolio_tracking_review_stage",
                        parameter_shadow.get("portfolio_tracking_review_stage"),
                    ),
                    (
                        "portfolio_tracking_review_days_until_short_review",
                        parameter_shadow.get("portfolio_tracking_review_days_until_short_review"),
                    ),
                    (
                        "portfolio_tracking_review_days_until_extended_review",
                        parameter_shadow.get(
                            "portfolio_tracking_review_days_until_extended_review"
                        ),
                    ),
                    (
                        "portfolio_tracking_review_excess_return",
                        parameter_shadow.get("portfolio_tracking_review_excess_return"),
                    ),
                    ("weight_tuning_status", parameter_shadow.get("weight_tuning_status")),
                    ("weight_tuning_summary", parameter_shadow.get("weight_tuning_summary")),
                    (
                        "weight_tuning_candidate_status",
                        parameter_shadow.get("weight_tuning_candidate_status"),
                    ),
                    (
                        "weight_tuning_candidates_evaluated",
                        parameter_shadow.get("weight_tuning_candidates_evaluated"),
                    ),
                    (
                        "weight_tuning_guardrail_status",
                        parameter_shadow.get("weight_tuning_guardrail_status"),
                    ),
                    (
                        "weight_tuning_non_worse_walk_forward_ratio",
                        parameter_shadow.get("weight_tuning_non_worse_walk_forward_ratio"),
                    ),
                    (
                        "weight_tuning_failure_status",
                        parameter_shadow.get("weight_tuning_failure_status"),
                    ),
                    (
                        "weight_tuning_failure_summary",
                        parameter_shadow.get("weight_tuning_failure_summary"),
                    ),
                    (
                        "weight_tuning_failure_root_cause",
                        parameter_shadow.get("weight_tuning_failure_root_cause"),
                    ),
                    (
                        "weight_tuning_failure_top_reason",
                        parameter_shadow.get("weight_tuning_failure_top_reason"),
                    ),
                    (
                        "weight_tuning_failure_next_action",
                        parameter_shadow.get("weight_tuning_failure_next_action"),
                    ),
                    (
                        "weight_stability_status",
                        parameter_shadow.get("weight_stability_status"),
                    ),
                    (
                        "weight_stability_summary",
                        parameter_shadow.get("weight_stability_summary"),
                    ),
                    (
                        "weight_stability_candidate_status",
                        parameter_shadow.get("weight_stability_candidate_status"),
                    ),
                    (
                        "weight_stability_candidates_generated",
                        parameter_shadow.get("weight_stability_candidates_generated"),
                    ),
                    (
                        "weight_stability_rejected_by_stability",
                        parameter_shadow.get("weight_stability_rejected_by_stability"),
                    ),
                    (
                        "weight_stability_rejected_by_turnover_prefilter",
                        parameter_shadow.get("weight_stability_rejected_by_turnover_prefilter"),
                    ),
                    (
                        "weight_stability_turnover_failures_reduced",
                        parameter_shadow.get("weight_stability_turnover_failures_reduced"),
                    ),
                    (
                        "weight_stability_readiness_status",
                        parameter_shadow.get("weight_stability_readiness_status"),
                    ),
                    (
                        "weight_stability_readiness_summary",
                        parameter_shadow.get("weight_stability_readiness_summary"),
                    ),
                    (
                        "weight_stability_readiness_can_run",
                        parameter_shadow.get("weight_stability_readiness_can_run"),
                    ),
                    (
                        "weight_stability_readiness_blocking_checks",
                        parameter_shadow.get("weight_stability_readiness_blocking_checks"),
                    ),
                    (
                        "weight_stability_readiness_next_action",
                        parameter_shadow.get("weight_stability_readiness_next_action"),
                    ),
                    (
                        "portfolio_turnover_attribution_status",
                        parameter_shadow.get("portfolio_turnover_attribution_status"),
                    ),
                    (
                        "portfolio_turnover_attribution_summary",
                        parameter_shadow.get("portfolio_turnover_attribution_summary"),
                    ),
                    (
                        "portfolio_turnover_attribution_root_cause",
                        parameter_shadow.get("portfolio_turnover_attribution_root_cause"),
                    ),
                    (
                        "portfolio_turnover_top_assets",
                        parameter_shadow.get("portfolio_turnover_top_assets"),
                    ),
                    (
                        "portfolio_turnover_next_action",
                        parameter_shadow.get("portfolio_turnover_next_action"),
                    ),
                    ("manual_review_required", parameter_shadow.get("manual_review_required")),
                    ("risk", parameter_shadow.get("risk")),
                    ("diagnostic_report", parameter_shadow.get("diagnostic_report")),
                ]
            ),
        ),
        _section("Documentation Contract", _definition_table(list(documentation_contract.items()))),
        _section(
            "Manual Review Queue",
            _top_review_items_html(manual_review)
            + _manual_review_impact_groups_html(manual_review)
            + _manual_review_groups_html(manual_review, manual_queue),
        ),
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
        "market_regime": _text(
            market_regime.get("regime_id"), "unified_primary_2021"
        ),
        "market_regime_start": _text(market_regime.get("start_date"), PRIMARY_RESEARCH_START),
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
    action = _text(
        investment.get("action_bias"),
        _text(evidence_decision.get("action"), "UNKNOWN"),
    )
    manual_required = any(_text(item.get("status")) not in {"", "PASS"} for item in manual_items)
    action_lower = action.lower()
    manual_required = manual_required or "manual" in action_lower or "人工复核" in action
    return {
        "action": action,
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
    market_panel: Mapping[str, Any],
) -> dict[str, Any]:
    decision = _mapping(evidence_dashboard.get("decision"))
    dashboard_quality = _mapping(evidence_dashboard.get("quality"))
    quality = _mapping(snapshot.get("quality"))
    if market_panel:
        panel_summary = _mapping(market_panel.get("summary"))
        panel_quality = _mapping(market_panel.get("data_quality"))
        proxies = _records(market_panel.get("proxies"))
        panel_status = _text(market_panel.get("status"), "UNKNOWN")
        return {
            "availability": (
                "LIMITED" if panel_status == "MISSING_MARKET_PRICE_DATA" else "AVAILABLE"
            ),
            "risk_regime_label": _text(decision.get("market_regime"), "not_available"),
            "benchmark_proxy": _role_proxy_summary(proxies, "benchmark_proxy"),
            "ai_sector_proxy": _role_proxy_summary(proxies, "ai_sector_proxy"),
            "risk_proxy": _role_proxy_summary(proxies, "risk_proxy"),
            "liquidity_proxy": _role_proxy_summary(proxies, "liquidity_proxy"),
            "market_price_panel_status": (
                "MISSING_MARKET_PRICE_DATA"
                if panel_status == "MISSING_MARKET_PRICE_DATA"
                else "AVAILABLE"
            ),
            "market_panel_status": panel_status,
            "market_movement_sentence": _text(
                panel_summary.get("market_movement_sentence"),
                "市场面板未提供 movement summary。",
            ),
            "market_data_status": _text(
                panel_quality.get("status"),
                _text(
                    dashboard_quality.get("market_data_status"),
                    _text(quality.get("market_data_status"), "UNKNOWN"),
                ),
            ),
            "feature_status": _text(quality.get("feature_status"), "UNKNOWN"),
            "proxy_rows": _reader_market_proxy_rows(proxies),
            "recommended_action": (
                "review_missing_market_data"
                if panel_status == "MISSING_MARKET_PRICE_DATA"
                else "review_market_panel_sources"
            ),
            "limitation": (
                "Market panel 只读缓存价格/利率 artifact；缺失或 PARTIAL_HISTORY 的 proxy "
                "不得被解读为完整市场复盘。"
            ),
            "source_artifact": _text(
                _mapping(_mapping(market_panel.get("source_artifacts")).get("prices_daily")).get(
                    "path"
                )
            ),
            "production_effect": PRODUCTION_EFFECT,
        }
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
    binding_gate = _binding_gate_from_snapshot(snapshot)
    steps = [
        _funnel_step(
            "component_score",
            metrics,
            f"{len(_records(scores.get('components')))} components",
            "decision_snapshot.scores.components",
            source_inputs,
            display_value=f"{len(_records(scores.get('components')))} components",
        ),
        _funnel_step(
            "overall_score",
            metrics,
            _format_number(scores.get("overall_score"), digits=1),
            "scores",
            source_inputs,
            display_value=_format_number(scores.get("overall_score"), digits=1),
        ),
        _funnel_step(
            "model_position_band",
            metrics,
            _format_band(_mapping(positions.get("model_risk_asset_ai_band"))),
            "decision_snapshot.positions",
            source_inputs,
            display_value=_format_band(_mapping(positions.get("model_risk_asset_ai_band"))),
        ),
        _funnel_step(
            "confidence_adjusted_position",
            metrics,
            _format_band(_mapping(positions.get("confidence_adjusted_risk_asset_ai_band"))),
            "decision_snapshot.positions",
            source_inputs,
            display_value=_format_band(
                _mapping(positions.get("confidence_adjusted_risk_asset_ai_band"))
            ),
        ),
        _funnel_step(
            "portfolio_limit",
            metrics,
            _portfolio_limit_value(positions),
            "decision_snapshot.positions.final_total_risk_asset_band",
            source_inputs,
            display_value=_portfolio_limit_value(positions),
        ),
        _funnel_step(
            "position_gate",
            metrics,
            _binding_gate_value(binding_gate),
            "decision_snapshot.positions.position_gates",
            source_inputs,
            display_value=_binding_gate_value(binding_gate),
        ),
        _funnel_step(
            "final_position_band",
            metrics,
            _format_band(_mapping(positions.get("final_risk_asset_ai_band"))),
            "decision_snapshot.positions.final_risk_asset_ai_band",
            source_inputs,
            display_value=_format_band(_mapping(positions.get("final_risk_asset_ai_band"))),
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


def _role_proxy_summary(proxies: list[dict[str, Any]], role: str) -> str:
    records = [row for row in proxies if _text(row.get("role")) == role]
    if not records:
        return "MISSING"
    parts = []
    for row in records:
        change = _format_market_change(row.get("return_1d"), row.get("change_mode"))
        status = _text(row.get("data_status"), "UNKNOWN")
        if status == "MISSING_MARKET_PRICE_DATA":
            parts.append(f"{_text(row.get('symbol'), 'UNKNOWN')}:MISSING")
        else:
            parts.append(f"{_text(row.get('symbol'), 'UNKNOWN')} 1D={change}")
    return "; ".join(parts)


def _reader_market_proxy_rows(proxies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "symbol": _text(row.get("symbol")),
            "role": _text(row.get("role")),
            "last_price": row.get("last_price"),
            "return_1d": _format_market_change(row.get("return_1d"), row.get("change_mode")),
            "return_5d": _format_market_change(row.get("return_5d"), row.get("change_mode")),
            "return_20d": _format_market_change(row.get("return_20d"), row.get("change_mode")),
            "trend_label": _text(row.get("trend_label"), "UNKNOWN"),
            "risk_interpretation": _text(row.get("risk_interpretation"), "UNKNOWN"),
            "data_status": _text(row.get("data_status"), "UNKNOWN"),
            "source_artifact": _short_path(_text(row.get("source_artifact"))),
            "production_effect": _text(row.get("production_effect"), PRODUCTION_EFFECT),
        }
        for row in proxies
    ]


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


def _report_index_waiver_inventory_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_report_index_waiver_inventory_summary(
            "report_index artifact missing; Reader Brief cannot discover waiver inventory."
        )
    report_path = _report_index_artifact_path(report_index, "report_index_waiver_inventory")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_report_index_waiver_inventory_summary(
            "report_index_waiver_inventory artifact missing from report index latest pointer."
        )
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("inventory_status"), _text(payload.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "inventory_status": status,
        "configured_waiver_count": _int(summary.get("configured_waiver_count")),
        "expanded_waiver_count": _int(summary.get("expanded_waiver_count")),
        "active_waiver_count": _int(summary.get("active_waiver_count")),
        "expired_waiver_count": _int(summary.get("expired_waiver_count")),
        "expiring_soon_waiver_count": _int(summary.get("expiring_soon_waiver_count")),
        "missing_registry_entry_count": _int(summary.get("missing_registry_entry_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"waiver_inventory={status}; "
            f"active={_int(summary.get('active_waiver_count'))}; "
            f"expired={_int(summary.get('expired_waiver_count'))}; "
            f"expiring_soon={_int(summary.get('expiring_soon_waiver_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest waiver inventory artifact from report index; "
            "it does not renew, remove, or apply waivers."
        ),
    }


def _missing_report_index_waiver_inventory_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "inventory_status": "MISSING",
        "configured_waiver_count": 0,
        "expanded_waiver_count": 0,
        "active_waiver_count": 0,
        "expired_waiver_count": 0,
        "expiring_soon_waiver_count": 0,
        "missing_registry_entry_count": 0,
        "blocking_issue_count": 0,
        "warning_issue_count": 0,
        "next_action": "run_aits_reports_waiver_inventory_then_validate",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "report_index_waiver_inventory artifact missing; run reports waiver-inventory."
        ),
        "limitation": reason,
    }


def _reader_brief_consistency_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_reader_brief_consistency_summary(
            "report_index artifact missing; Reader Brief cannot discover consistency pack."
        )
    report_path = _report_index_artifact_path(report_index, "reader_brief_consistency_pack")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_reader_brief_consistency_summary(
            "reader_brief_consistency_pack artifact missing from report index latest pointer."
        )
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("consistency_status"), _text(payload.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "consistency_status": status,
        "checked_report_count": _int(summary.get("checked_report_count")),
        "available_report_count": _int(summary.get("available_report_count")),
        "full_coverage_report_count": _int(summary.get("full_coverage_report_count")),
        "missing_section_count": _int(summary.get("missing_section_count")),
        "unclear_decision_count": _int(summary.get("unclear_decision_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"reader_brief_consistency={status}; "
            f"missing_sections={_int(summary.get('missing_section_count'))}; "
            f"unclear_decisions={_int(summary.get('unclear_decision_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest consistency pack artifact from report index; "
            "it does not rewrite report templates or run upstream commands."
        ),
    }


def _missing_reader_brief_consistency_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "consistency_status": "MISSING",
        "checked_report_count": 0,
        "available_report_count": 0,
        "full_coverage_report_count": 0,
        "missing_section_count": 0,
        "unclear_decision_count": 0,
        "blocking_issue_count": 0,
        "warning_issue_count": 0,
        "next_action": "run_aits_reports_reader_brief_consistency_then_validate",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "reader_brief_consistency_pack artifact missing; run reports "
            "reader-brief-consistency."
        ),
        "limitation": reason,
    }


def _production_boundary_static_scan_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_production_boundary_static_scan_summary(
            "report_index artifact missing; Reader Brief cannot discover static scan."
        )
    report_path = _report_index_artifact_path(report_index, "production_boundary_static_scan")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_production_boundary_static_scan_summary(
            "production_boundary_static_scan artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "production_boundary_static_scan_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("scan_status"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "scan_status": status,
        "validation_status": validation_status,
        "scanned_file_count": _int(summary.get("scanned_file_count")),
        "finding_count": _int(summary.get("finding_count")),
        "blocking_finding_count": _int(summary.get("blocking_finding_count")),
        "warning_finding_count": _int(summary.get("warning_finding_count")),
        "allowed_match_count": _int(summary.get("allowed_match_count")),
        "static_scan_input": _text(summary.get("static_scan_input"), "UNKNOWN"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"production_boundary_static_scan={status}; "
            f"blocking={_int(summary.get('blocking_finding_count'))}; "
            f"warnings={_int(summary.get('warning_finding_count'))}; "
            f"allowed={_int(summary.get('allowed_match_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest static scan artifacts from report index; "
            "it does not edit source, config, or docs."
        ),
    }


def _missing_production_boundary_static_scan_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "scan_status": "MISSING",
        "validation_status": "MISSING",
        "scanned_file_count": 0,
        "finding_count": 0,
        "blocking_finding_count": 0,
        "warning_finding_count": 0,
        "allowed_match_count": 0,
        "static_scan_input": "MISSING",
        "next_action": "run_aits_reports_production_boundary_static_scan_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "production_boundary_static_scan artifact missing; run reports "
            "production-boundary-static-scan."
        ),
        "limitation": reason,
    }


def _owner_review_template_v2_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_owner_review_template_v2_summary(
            "report_index artifact missing; Reader Brief cannot discover owner review template."
        )
    report_path = _report_index_artifact_path(report_index, "owner_review_template_v2")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_owner_review_template_v2_summary(
            "owner_review_template_v2 artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "owner_review_template_v2_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    template_status = _text(payload.get("template_status"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": template_status,
        "template_status": template_status,
        "validation_status": validation_status,
        "required_field_count": _int(summary.get("required_field_count")),
        "owner_action_count": _int(summary.get("owner_action_count")),
        "optional_record_validation_supported": (
            summary.get("optional_record_validation_supported") is True
        ),
        "owner_decision_logged": summary.get("owner_decision_logged") is True,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"owner_review_template_v2={template_status}; "
            f"validation={validation_status}; "
            f"fields={_int(summary.get('required_field_count'))}; "
            f"owner_actions={_int(summary.get('owner_action_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest owner review template artifacts from report index; "
            "it does not create or append owner decisions."
        ),
    }


def _missing_owner_review_template_v2_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "template_status": "MISSING",
        "validation_status": "MISSING",
        "required_field_count": 0,
        "owner_action_count": 0,
        "optional_record_validation_supported": False,
        "owner_decision_logged": False,
        "next_action": "run_aits_reports_owner_review_template_v2_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "owner_review_template_v2 artifact missing; run reports owner-review-template-v2."
        ),
        "limitation": reason,
    }


def _owner_decision_audit_log_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_owner_decision_audit_log_summary(
            "report_index artifact missing; Reader Brief cannot discover owner decision audit log."
        )
    report_path = _report_index_artifact_path(report_index, "owner_decision_audit_log")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_owner_decision_audit_log_summary(
            "owner_decision_audit_log artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "owner_decision_audit_log_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("audit_log_status"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "audit_log_status": status,
        "validation_status": validation_status,
        "record_count": _int(summary.get("included_record_count")),
        "raw_record_count": _int(summary.get("record_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "duplicate_decision_id_count": _int(summary.get("duplicate_decision_id_count")),
        "latest_decision_id": _text(summary.get("latest_decision_id")),
        "latest_candidate_id": _text(summary.get("latest_candidate_id")),
        "latest_owner_action": _text(summary.get("latest_owner_action")),
        "latest_safety_status": _text(summary.get("latest_safety_status")),
        "monthly_review_pack_input": _text(
            summary.get("monthly_review_pack_input"),
            "UNKNOWN",
        ),
        "promotion_board_input": _text(summary.get("promotion_board_input"), "UNKNOWN"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"owner_decision_audit_log={status}; "
            f"validation={validation_status}; "
            f"records={_int(summary.get('included_record_count'))}; "
            f"latest={_text(summary.get('latest_decision_id'), 'none')}."
        ),
        "limitation": (
            "Reader Brief only reads the latest owner decision audit log artifacts from "
            "report index; it does not append decisions or mutate strategy outputs."
        ),
    }


def _missing_owner_decision_audit_log_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "audit_log_status": "MISSING",
        "validation_status": "MISSING",
        "record_count": 0,
        "raw_record_count": 0,
        "blocking_issue_count": 0,
        "duplicate_decision_id_count": 0,
        "latest_decision_id": "",
        "latest_candidate_id": "",
        "latest_owner_action": "",
        "latest_safety_status": "",
        "monthly_review_pack_input": "MISSING",
        "promotion_board_input": "MISSING",
        "next_action": "run_aits_reports_owner_decision_audit_log_report_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "owner_decision_audit_log artifact missing; run reports "
            "owner-decision-audit-log report."
        ),
        "limitation": reason,
    }


def _research_monthly_review_pack_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_research_monthly_review_pack_summary(
            "report_index artifact missing; Reader Brief cannot discover monthly review pack."
        )
    report_path = _report_index_artifact_path(report_index, "research_monthly_review_pack")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_research_monthly_review_pack_summary(
            "research_monthly_review_pack artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "research_monthly_review_pack_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(
        payload.get("monthly_review_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "monthly_review_status": status,
        "validation_status": validation_status,
        "source_family_count": _int(summary.get("source_family_count")),
        "active_candidate_count": _int(summary.get("active_candidate_count")),
        "rejected_candidate_count": _int(summary.get("rejected_candidate_count")),
        "paper_shadow_candidate_count": _int(summary.get("paper_shadow_candidate_count")),
        "needs_evidence_candidate_count": _int(
            summary.get("needs_evidence_candidate_count")
        ),
        "major_blocker_count": _int(summary.get("major_blocker_count")),
        "major_warning_count": _int(summary.get("major_warning_count")),
        "safety_audit_status": _text(summary.get("safety_audit_status"), "UNKNOWN"),
        "data_governance_status": _text(
            summary.get("data_governance_status"),
            "UNKNOWN",
        ),
        "owner_decision_status": _text(summary.get("owner_decision_status"), "UNKNOWN"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_monthly_review_pack={status}; "
            f"validation={validation_status}; "
            f"active={_int(summary.get('active_candidate_count'))}; "
            f"needs_evidence={_int(summary.get('needs_evidence_candidate_count'))}; "
            f"blockers={_int(summary.get('major_blocker_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest monthly review pack artifacts from "
            "report index; it does not run upstream source reports or approve promotion."
        ),
    }


def _missing_research_monthly_review_pack_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "monthly_review_status": "MISSING",
        "validation_status": "MISSING",
        "source_family_count": 0,
        "active_candidate_count": 0,
        "rejected_candidate_count": 0,
        "paper_shadow_candidate_count": 0,
        "needs_evidence_candidate_count": 0,
        "major_blocker_count": 0,
        "major_warning_count": 0,
        "safety_audit_status": "MISSING",
        "data_governance_status": "MISSING",
        "owner_decision_status": "MISSING",
        "next_action": "run_aits_reports_research_monthly_review_pack_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "research_monthly_review_pack artifact missing; run reports "
            "research-monthly-review-pack."
        ),
        "limitation": reason,
    }


def _paper_shadow_promotion_board_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_paper_shadow_promotion_board_summary(
            "report_index artifact missing; Reader Brief cannot discover promotion board."
        )
    report_path = _report_index_artifact_path(report_index, "paper_shadow_promotion_board")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_paper_shadow_promotion_board_summary(
            "paper_shadow_promotion_board artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "paper_shadow_promotion_board_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    decision = _text(payload.get("board_decision"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": decision,
        "board_decision": decision,
        "validation_status": validation_status,
        "candidate_id": _text(summary.get("candidate_id"), _text(payload.get("candidate_id"))),
        "evidence_check_count": _int(summary.get("evidence_check_count")),
        "passed_evidence_count": _int(summary.get("passed_evidence_count")),
        "blocked_evidence_count": _int(summary.get("blocked_evidence_count")),
        "warning_evidence_count": _int(summary.get("warning_evidence_count")),
        "safety_status": _text(summary.get("safety_status"), "UNKNOWN"),
        "readiness_status": _text(summary.get("readiness_status"), "UNKNOWN"),
        "owner_decision_status": _text(summary.get("owner_decision_status"), "UNKNOWN"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"paper_shadow_promotion_board={decision}; "
            f"validation={validation_status}; "
            f"blocked={_int(summary.get('blocked_evidence_count'))}; "
            f"warnings={_int(summary.get('warning_evidence_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest paper-shadow promotion board artifacts "
            "from report index; it does not promote candidates or mutate shadow state."
        ),
    }


def _missing_paper_shadow_promotion_board_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "board_decision": "MISSING",
        "validation_status": "MISSING",
        "candidate_id": "",
        "evidence_check_count": 0,
        "passed_evidence_count": 0,
        "blocked_evidence_count": 0,
        "warning_evidence_count": 0,
        "safety_status": "MISSING",
        "readiness_status": "MISSING",
        "owner_decision_status": "MISSING",
        "next_action": "run_aits_reports_paper_shadow_promotion_board_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "paper_shadow_promotion_board artifact missing; run reports "
            "paper-shadow-promotion-board."
        ),
        "limitation": reason,
    }


def _candidate_rejection_postmortem_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_candidate_rejection_postmortem_summary(
            "report_index artifact missing; Reader Brief cannot discover postmortem template."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "candidate_rejection_postmortem_template",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_candidate_rejection_postmortem_summary(
            "candidate_rejection_postmortem_template artifact missing from report "
            "index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "candidate_rejection_postmortem_template_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    template_status = _text(
        payload.get("template_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": template_status,
        "template_status": template_status,
        "validation_status": validation_status,
        "postmortem_record_provided": summary.get("postmortem_record_provided") is True,
        "filled_postmortem_status": _text(summary.get("filled_postmortem_status"), "UNKNOWN"),
        "candidate_id": _text(summary.get("candidate_id")),
        "required_section_count": _int(summary.get("required_section_count")),
        "failed_evidence_gate_count": _int(summary.get("failed_evidence_gate_count")),
        "failed_stress_scenario_count": _int(summary.get("failed_stress_scenario_count")),
        "data_quality_issue_count": _int(summary.get("data_quality_issue_count")),
        "safety_boundary_issue_count": _int(summary.get("safety_boundary_issue_count")),
        "lessons_learned_count": _int(summary.get("lessons_learned_count")),
        "can_revisit": _text(summary.get("can_revisit"), "UNSPECIFIED"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"candidate_rejection_postmortem_template={template_status}; "
            f"validation={validation_status}; "
            f"filled={summary.get('postmortem_record_provided') is True}; "
            f"sections={_int(summary.get('required_section_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest postmortem template artifacts; it does not "
            "reject candidates or mutate candidate state."
        ),
    }


def _missing_candidate_rejection_postmortem_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "template_status": "MISSING",
        "validation_status": "MISSING",
        "postmortem_record_provided": False,
        "filled_postmortem_status": "MISSING",
        "candidate_id": "",
        "required_section_count": 0,
        "failed_evidence_gate_count": 0,
        "failed_stress_scenario_count": 0,
        "data_quality_issue_count": 0,
        "safety_boundary_issue_count": 0,
        "lessons_learned_count": 0,
        "can_revisit": "UNSPECIFIED",
        "next_action": "run_aits_reports_candidate_rejection_postmortem_template_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "candidate_rejection_postmortem_template artifact missing; run reports "
            "candidate-rejection-postmortem-template."
        ),
        "limitation": reason,
    }


def _decision_snapshot_lifecycle_policy_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_decision_snapshot_lifecycle_policy_summary(
            "report_index artifact missing; Reader Brief cannot discover lifecycle policy."
        )
    report_path = _report_index_artifact_path(report_index, "decision_snapshot_lifecycle_policy")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_decision_snapshot_lifecycle_policy_summary(
            "decision_snapshot_lifecycle_policy artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "decision_snapshot_lifecycle_policy_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(
        payload.get("snapshot_lifecycle_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "snapshot_lifecycle_status": status,
        "validation_status": validation_status,
        "target_as_of": _text(summary.get("target_as_of"), _text(payload.get("as_of"))),
        "context_mode": _text(summary.get("context_mode"), "UNKNOWN"),
        "snapshot_path": _text(summary.get("snapshot_path")),
        "snapshot_exists": bool(summary.get("snapshot_exists")),
        "snapshot_signal_date": _text(summary.get("snapshot_signal_date")),
        "latest_available_snapshot_date": _text(
            summary.get("latest_available_snapshot_date")
        ),
        "market_session_status": _text(summary.get("market_session_status"), "UNKNOWN"),
        "blocking_impact": _text(summary.get("blocking_impact"), "UNKNOWN"),
        "invalid_reason_count": _int(summary.get("invalid_reason_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"decision_snapshot_lifecycle_policy={status}; "
            f"validation={validation_status}; "
            f"snapshot_exists={bool(summary.get('snapshot_exists'))}; "
            f"latest={_text(summary.get('latest_available_snapshot_date'), 'none')}."
        ),
        "limitation": (
            "Reader Brief only reads latest lifecycle policy artifacts; it does not "
            "run score-daily or fabricate missing decision snapshots."
        ),
    }


def _missing_decision_snapshot_lifecycle_policy_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "snapshot_lifecycle_status": "MISSING",
        "validation_status": "MISSING",
        "target_as_of": "",
        "context_mode": "MISSING",
        "snapshot_path": "",
        "snapshot_exists": False,
        "snapshot_signal_date": "",
        "latest_available_snapshot_date": "",
        "market_session_status": "MISSING",
        "blocking_impact": "UNKNOWN",
        "invalid_reason_count": 0,
        "next_action": "run_aits_reports_decision_snapshot_lifecycle_policy_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "decision_snapshot_lifecycle_policy artifact missing; run reports "
            "decision-snapshot-lifecycle-policy."
        ),
        "limitation": reason,
    }


def _extended_shadow_observation_clock_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_extended_shadow_observation_clock_summary(
            "report_index artifact missing; Reader Brief cannot discover observation clock."
        )
    report_path = _report_index_artifact_path(report_index, "extended_shadow_observation_clock")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_extended_shadow_observation_clock_summary(
            "extended_shadow_observation_clock artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "extended_shadow_observation_clock_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(
        payload.get("observation_clock_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "observation_clock_status": status,
        "validation_status": validation_status,
        "candidate_id": _text(summary.get("candidate_id"), _text(payload.get("candidate_id"))),
        "observation_start_date": _text(summary.get("observation_start_date")),
        "current_count": _int(summary.get("current_count")),
        "required_count": _int(summary.get("required_count")),
        "missing_day_count": _int(summary.get("missing_day_count")),
        "invalid_day_count": _int(summary.get("invalid_day_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"extended_shadow_observation_clock={status}; "
            f"validation={validation_status}; "
            f"current={_int(summary.get('current_count'))}/"
            f"{_int(summary.get('required_count'))}; "
            f"invalid={_int(summary.get('invalid_day_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest observation clock artifacts; it does not "
            "run paper-shadow observation or fabricate missing days."
        ),
    }


def _missing_extended_shadow_observation_clock_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "observation_clock_status": "MISSING",
        "validation_status": "MISSING",
        "candidate_id": "",
        "observation_start_date": "",
        "current_count": 0,
        "required_count": 0,
        "missing_day_count": 0,
        "invalid_day_count": 0,
        "next_action": "run_aits_reports_extended_shadow_observation_clock_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "extended_shadow_observation_clock artifact missing; run reports "
            "extended-shadow-observation-clock."
        ),
        "limitation": reason,
    }


def _extended_shadow_protocol_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_extended_shadow_protocol_summary(
            "report_index artifact missing; Reader Brief cannot discover extended shadow protocol."
        )
    report_path = _report_index_artifact_path(report_index, "extended_shadow_protocol")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_extended_shadow_protocol_summary(
            "extended_shadow_protocol artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "extended_shadow_protocol_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    eligibility_status = _text(
        payload.get("eligibility_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": eligibility_status,
        "eligibility_status": eligibility_status,
        "validation_status": validation_status,
        "candidate_id": _text(summary.get("candidate_id"), _text(payload.get("candidate_id"))),
        "observed_trading_days": _int(summary.get("observed_trading_days")),
        "minimum_observation_trading_days": _int(
            summary.get("minimum_observation_trading_days")
        ),
        "check_count": _int(summary.get("check_count")),
        "passed_check_count": _int(summary.get("passed_check_count")),
        "blocked_check_count": _int(summary.get("blocked_check_count")),
        "warning_check_count": _int(summary.get("warning_check_count")),
        "safety_status": _text(summary.get("safety_status"), "UNKNOWN"),
        "readiness_status": _text(summary.get("readiness_status"), "UNKNOWN"),
        "owner_decision_status": _text(summary.get("owner_decision_status"), "UNKNOWN"),
        "lineage_status": _text(summary.get("lineage_status"), "UNKNOWN"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"extended_shadow_protocol={eligibility_status}; "
            f"validation={validation_status}; "
            f"blocked={_int(summary.get('blocked_check_count'))}; "
            f"warnings={_int(summary.get('warning_check_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest extended shadow protocol artifacts; "
            "it does not extend shadow or mutate candidate state."
        ),
    }


def _missing_extended_shadow_protocol_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "eligibility_status": "MISSING",
        "validation_status": "MISSING",
        "candidate_id": "",
        "observed_trading_days": 0,
        "minimum_observation_trading_days": 0,
        "check_count": 0,
        "passed_check_count": 0,
        "blocked_check_count": 0,
        "warning_check_count": 0,
        "safety_status": "MISSING",
        "readiness_status": "MISSING",
        "owner_decision_status": "MISSING",
        "lineage_status": "MISSING",
        "next_action": "run_aits_reports_extended_shadow_protocol_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "extended_shadow_protocol artifact missing; run reports "
            "extended-shadow-protocol."
        ),
        "limitation": reason,
    }


def _research_roadmap_dashboard_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_research_roadmap_dashboard_summary(
            "report_index artifact missing; Reader Brief cannot discover roadmap dashboard."
        )
    report_path = _report_index_artifact_path(report_index, "research_roadmap_dashboard")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_research_roadmap_dashboard_summary(
            "research_roadmap_dashboard artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "research_roadmap_dashboard_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    dashboard_status = _text(
        payload.get("dashboard_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": dashboard_status,
        "dashboard_status": dashboard_status,
        "validation_status": validation_status,
        "active_task_count": _int(summary.get("active_task_count")),
        "completed_task_count": _int(summary.get("completed_task_count")),
        "open_blocker_count": _int(summary.get("open_blocker_count")),
        "stale_artifact_count": _int(summary.get("stale_artifact_count")),
        "missing_artifact_count": _int(summary.get("missing_artifact_count")),
        "active_candidate_count": _int(summary.get("active_candidate_count")),
        "paper_shadow_status": _text(summary.get("paper_shadow_status"), "UNKNOWN"),
        "data_governance_status": _text(
            summary.get("data_governance_status"),
            "UNKNOWN",
        ),
        "safety_status": _text(summary.get("safety_status"), "UNKNOWN"),
        "lineage_status": _text(summary.get("lineage_status"), "UNKNOWN"),
        "top_next_task": _text(summary.get("top_next_task"), "none"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_roadmap_dashboard={dashboard_status}; "
            f"validation={validation_status}; "
            f"blockers={_int(summary.get('open_blocker_count'))}; "
            f"stale={_int(summary.get('stale_artifact_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest roadmap dashboard artifacts; it does not "
            "modify task, candidate, paper-shadow, or production state."
        ),
    }


def _missing_research_roadmap_dashboard_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "dashboard_status": "MISSING",
        "validation_status": "MISSING",
        "active_task_count": 0,
        "completed_task_count": 0,
        "open_blocker_count": 0,
        "stale_artifact_count": 0,
        "missing_artifact_count": 0,
        "active_candidate_count": 0,
        "paper_shadow_status": "MISSING",
        "data_governance_status": "MISSING",
        "safety_status": "MISSING",
        "lineage_status": "MISSING",
        "top_next_task": "none",
        "next_action": "run_aits_reports_research_roadmap_dashboard_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "research_roadmap_dashboard artifact missing; run reports "
            "research-roadmap-dashboard."
        ),
        "limitation": reason,
    }


def _research_governance_end_to_end_pack_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_research_governance_end_to_end_pack_summary(
            "report_index artifact missing; Reader Brief cannot discover governance pack."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "research_governance_end_to_end_pack",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_research_governance_end_to_end_pack_summary(
            "research_governance_end_to_end_pack artifact missing from report index "
            "latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "research_governance_end_to_end_pack_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    governance_status = _text(
        payload.get("overall_governance_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": governance_status,
        "overall_governance_status": governance_status,
        "validation_status": validation_status,
        "source_report_count": _int(summary.get("source_report_count")),
        "available_source_count": _int(summary.get("available_source_count")),
        "validation_pass_count": _int(summary.get("validation_pass_count")),
        "validation_warning_count": _int(summary.get("validation_warning_count")),
        "validation_fail_count": _int(summary.get("validation_fail_count")),
        "blocking_item_count": _int(summary.get("blocking_item_count")),
        "warning_item_count": _int(summary.get("warning_item_count")),
        "manual_review_item_count": _int(summary.get("manual_review_item_count")),
        "top_blocker": _text(summary.get("top_blocker"), "none"),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_governance_end_to_end_pack={governance_status}; "
            f"validation={validation_status}; "
            f"blockers={_int(summary.get('blocking_item_count'))}; "
            f"warnings={_int(summary.get('warning_item_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest governance end-to-end pack artifacts; "
            "it does not run upstream reports or mutate state."
        ),
    }


def _missing_research_governance_end_to_end_pack_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "overall_governance_status": "MISSING",
        "validation_status": "MISSING",
        "source_report_count": 0,
        "available_source_count": 0,
        "validation_pass_count": 0,
        "validation_warning_count": 0,
        "validation_fail_count": 0,
        "blocking_item_count": 0,
        "warning_item_count": 0,
        "manual_review_item_count": 0,
        "top_blocker": "none",
        "next_action": "run_aits_reports_research_governance_end_to_end_pack_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "research_governance_end_to_end_pack artifact missing; run reports "
            "research-governance-end-to-end-pack."
        ),
        "limitation": reason,
    }


def _research_governance_recovery_pack_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_research_governance_recovery_pack_summary(
            "report_index artifact missing; Reader Brief cannot discover recovery pack."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "research_governance_recovery_pack",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_research_governance_recovery_pack_summary(
            "research_governance_recovery_pack artifact missing from report index "
            "latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "research_governance_recovery_pack_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    recovery_status = _text(
        payload.get("recovery_governance_status"),
        _text(payload.get("status"), "UNKNOWN"),
    )
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": recovery_status,
        "recovery_governance_status": recovery_status,
        "validation_status": validation_status,
        "source_report_count": _int(summary.get("source_report_count")),
        "available_source_count": _int(summary.get("available_source_count")),
        "validation_pass_count": _int(summary.get("validation_pass_count")),
        "validation_warning_count": _int(summary.get("validation_warning_count")),
        "remaining_blocker_count": _int(summary.get("remaining_blocker_count")),
        "remaining_warning_count": _int(summary.get("remaining_warning_count")),
        "manual_review_item_count": _int(summary.get("manual_review_item_count")),
        "top_remaining_blocker": _text(summary.get("top_remaining_blocker"), "none"),
        "normal_paper_shadow_may_resume": bool(
            summary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            summary.get("live_trading_remains_forbidden")
        ),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_governance_recovery_pack={recovery_status}; "
            f"validation={validation_status}; "
            f"blockers={_int(summary.get('remaining_blocker_count'))}; "
            f"normal_shadow={bool(summary.get('normal_paper_shadow_may_resume'))}; "
            f"live_forbidden={bool(summary.get('live_trading_remains_forbidden'))}."
        ),
        "limitation": (
            "Reader Brief only reads latest recovery governance pack artifacts; "
            "it does not run recovery commands or approve live trading."
        ),
    }


def _missing_research_governance_recovery_pack_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "recovery_governance_status": "MISSING",
        "validation_status": "MISSING",
        "source_report_count": 0,
        "available_source_count": 0,
        "validation_pass_count": 0,
        "validation_warning_count": 0,
        "remaining_blocker_count": 0,
        "remaining_warning_count": 0,
        "manual_review_item_count": 0,
        "top_remaining_blocker": "none",
        "normal_paper_shadow_may_resume": False,
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "next_action": "run_aits_reports_research_governance_recovery_pack_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "research_governance_recovery_pack artifact missing; run reports "
            "research-governance-recovery-pack."
        ),
        "limitation": reason,
    }


def _decision_stage_governance_snapshot_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_decision_stage_governance_snapshot_summary(
            "report_index artifact missing; Reader Brief cannot discover decision-stage snapshot."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "governance_status_snapshot_after_decision_review",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_decision_stage_governance_snapshot_summary(
            "governance_status_snapshot_after_decision_review artifact missing "
            "from report index latest pointer."
        )
    summary = _mapping(payload.get("summary"))
    snapshot_status = _text(payload.get("status"), _text(summary.get("snapshot_status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": snapshot_status,
        "snapshot_status": snapshot_status,
        "blocker_count": _int(summary.get("blocker_count")),
        "warning_count": _int(summary.get("warning_count")),
        "recommended_owner_action": _text(
            summary.get("recommended_owner_action"),
            "MISSING",
        ),
        "normal_shadow_may_resume": bool(summary.get("normal_shadow_may_resume")),
        "extended_shadow_remains_forbidden": bool(
            summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            summary.get("live_trading_remains_forbidden")
        ),
        "owner_decision_append_allowed": bool(
            summary.get("owner_decision_append_allowed")
        ),
        "dry_run_written": bool(summary.get("dry_run_written")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"decision_stage_snapshot={snapshot_status}; "
            f"blockers={_int(summary.get('blocker_count'))}; "
            f"recommended_owner_action={_text(summary.get('recommended_owner_action'))}; "
            f"normal_shadow={bool(summary.get('normal_shadow_may_resume'))}; "
            f"live_forbidden={bool(summary.get('live_trading_remains_forbidden'))}."
        ),
        "limitation": (
            "Reader Brief only reads the generated decision-stage snapshot; "
            "it does not append owner decisions or start observation clocks."
        ),
    }


def _missing_decision_stage_governance_snapshot_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "snapshot_status": "MISSING",
        "blocker_count": 0,
        "warning_count": 0,
        "recommended_owner_action": "MISSING",
        "normal_shadow_may_resume": False,
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "owner_decision_append_allowed": False,
        "dry_run_written": False,
        "next_action": "run_aits_reports_decision_stage_review_after_recovery_pack",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "governance_status_snapshot_after_decision_review artifact missing; "
            "run reports decision-stage-review."
        ),
        "limitation": reason,
    }


def _return_to_research_governance_snapshot_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_return_to_research_governance_snapshot_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "return-to-research snapshot."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "return_to_research_governance_snapshot",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_return_to_research_governance_snapshot_summary(
            "return_to_research_governance_snapshot artifact missing from report "
            "index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "return_to_research_governance_snapshot_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(validation_payload.get("status"), "MISSING")
    status = _text(payload.get("status"), _text(summary.get("return_to_research_status")))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "return_to_research_status": _text(
            summary.get("return_to_research_status"),
            status,
        ),
        "candidate_status": _text(summary.get("candidate_status"), "MISSING"),
        "owner_decision_id": _text(summary.get("owner_decision_id"), "MISSING"),
        "owner_action": _text(summary.get("owner_action"), "MISSING"),
        "normal_paper_shadow_active": bool(summary.get("normal_paper_shadow_active")),
        "extended_shadow_allowed": bool(summary.get("extended_shadow_allowed")),
        "live_trading_allowed": bool(summary.get("live_trading_allowed")),
        "candidate_rejected": bool(summary.get("candidate_rejected")),
        "hypothesis_count": _int(summary.get("hypothesis_count")),
        "next_candidate_id": _text(summary.get("next_candidate_id"), "MISSING"),
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"return_to_research={status}; "
            f"candidate_status={_text(summary.get('candidate_status'))}; "
            f"normal_active={bool(summary.get('normal_paper_shadow_active'))}; "
            f"live_allowed={bool(summary.get('live_trading_allowed'))}."
        ),
        "limitation": (
            "Reader Brief only reads the generated return-to-research snapshot; "
            "it does not append owner decisions, activate paper-shadow, or create orders."
        ),
    }


def _missing_return_to_research_governance_snapshot_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "return_to_research_status": "MISSING",
        "candidate_status": "MISSING",
        "owner_decision_id": "MISSING",
        "owner_action": "MISSING",
        "normal_paper_shadow_active": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "candidate_rejected": False,
        "hypothesis_count": 0,
        "next_candidate_id": "MISSING",
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_return_to_research_reset",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "return_to_research_governance_snapshot artifact missing; "
            "run reports return-to-research-reset."
        ),
        "limitation": reason,
    }


def _next_research_cycle_snapshot_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_research_cycle_snapshot_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next research-cycle snapshot."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "next_candidate_research_cycle_snapshot",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_next_research_cycle_snapshot_summary(
            "next_candidate_research_cycle_snapshot artifact missing from "
            "report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_research_cycle_snapshot_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(
        payload.get("status"),
        _text(summary.get("research_cycle_snapshot_status"), "UNKNOWN"),
    )
    research_gate_decision = _text(summary.get("research_gate_decision"), "MISSING")
    return {
        "availability": "AVAILABLE",
        "status": status,
        "research_cycle_snapshot_status": _text(
            summary.get("research_cycle_snapshot_status"),
            status,
        ),
        "research_gate_decision": research_gate_decision,
        "candidate_id": _text(summary.get("candidate_id"), "MISSING"),
        "market_regime": _text(summary.get("market_regime"), "MISSING"),
        "requested_date_range": _text(summary.get("requested_date_range"), "MISSING"),
        "artifact_count": _int(summary.get("artifact_count")),
        "owner_packet_ready": bool(summary.get("owner_packet_ready")),
        "paper_shadow_activation_allowed": bool(
            summary.get("paper_shadow_activation_allowed")
        ),
        "extended_shadow_allowed": bool(summary.get("extended_shadow_allowed")),
        "live_trading_allowed": bool(summary.get("live_trading_allowed")),
        "official_target_weights_generated": bool(
            summary.get("official_target_weights_generated")
        ),
        "broker_order_allowed": bool(summary.get("broker_order_allowed")),
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"next_research_cycle={status}; "
            f"research_gate={research_gate_decision}; "
            f"paper_shadow_allowed={bool(summary.get('paper_shadow_activation_allowed'))}; "
            f"live_allowed={bool(summary.get('live_trading_allowed'))}."
        ),
        "limitation": (
            "Reader Brief only reads the generated next research-cycle snapshot; "
            "it does not create paper-shadow candidates, approve live trading, "
            "write official target weights, or create orders."
        ),
    }


def _missing_next_research_cycle_snapshot_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "research_cycle_snapshot_status": "MISSING",
        "research_gate_decision": "MISSING",
        "candidate_id": "MISSING",
        "market_regime": "MISSING",
        "requested_date_range": "MISSING",
        "artifact_count": 0,
        "owner_packet_ready": False,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_research_cycle_snapshot",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_research_cycle_snapshot artifact missing; "
            "run reports next-candidate-research-cycle-snapshot."
        ),
        "limitation": reason,
    }


def _next_candidate_backfill_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_backfill_summary(
            "report_index artifact missing; Reader Brief cannot discover next candidate backfill."
        )
    report_path = _report_index_artifact_path(report_index, "next_candidate_backfill")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_next_candidate_backfill_summary(
            "next_candidate_backfill artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_backfill_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("candidate_backfill_status")))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "candidate_backfill_status": _text(summary.get("candidate_backfill_status"), status),
        "candidate_id": _text(summary.get("candidate_id"), "MISSING"),
        "backfill_metric_mode": _text(summary.get("backfill_metric_mode"), "MISSING"),
        "real_metrics_generated": summary.get("real_metrics_generated") is True,
        "aggregate_return_proxy": summary.get("aggregate_return_proxy"),
        "aggregate_drawdown_proxy": summary.get("aggregate_drawdown_proxy"),
        "turnover_proxy": summary.get("turnover_proxy"),
        "rotation_count": summary.get("rotation_count"),
        "false_risk_off_count": summary.get("false_risk_off_count"),
        "constraint_hit_count": summary.get("constraint_hit_count"),
        "signal_completeness": _text(summary.get("signal_completeness"), "MISSING"),
        "signal_completeness_ratio": summary.get("signal_completeness_ratio"),
        "missing_data_count": _int(summary.get("missing_data_count")),
        "partial_reason_count": _int(summary.get("partial_reason_count")),
        "safety_audit_status": _text(summary.get("safety_audit_status"), "MISSING"),
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"next_candidate_backfill={status}; validation={validation_status}; "
            f"metric_mode={_text(summary.get('backfill_metric_mode'), 'MISSING')}; "
            f"missing_data={_int(summary.get('missing_data_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the next candidate backfill artifact; it "
            "does not create paper-shadow candidates, approve live trading, "
            "write official weights, or create orders."
        ),
    }


def _missing_next_candidate_backfill_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "candidate_backfill_status": "MISSING",
        "candidate_id": "MISSING",
        "backfill_metric_mode": "MISSING",
        "real_metrics_generated": False,
        "aggregate_return_proxy": None,
        "aggregate_drawdown_proxy": None,
        "turnover_proxy": None,
        "rotation_count": None,
        "false_risk_off_count": None,
        "constraint_hit_count": None,
        "signal_completeness": "MISSING",
        "signal_completeness_ratio": None,
        "missing_data_count": 0,
        "partial_reason_count": 0,
        "safety_audit_status": "MISSING",
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_backfill",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_backfill artifact missing; run reports "
            "next-candidate-backfill."
        ),
        "limitation": reason,
    }


def _next_candidate_stress_cost_benchmark_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_stress_cost_benchmark_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next candidate stress/cost/benchmark review."
        )
    stress_path = _report_index_artifact_path(
        report_index,
        "next_candidate_stress_review",
    )
    cost_path = _report_index_artifact_path(
        report_index,
        "next_candidate_cost_benchmark_review",
    )
    stress_payload = _read_optional_json(stress_path)
    cost_payload = _read_optional_json(cost_path)
    if not stress_payload and not cost_payload:
        return _missing_next_candidate_stress_cost_benchmark_summary(
            "next_candidate_stress_review and next_candidate_cost_benchmark_review "
            "artifacts are missing from report index latest pointers."
        )
    stress_validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_stress_review_validation",
    )
    cost_validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_cost_benchmark_review_validation",
    )
    stress_validation_payload = _read_optional_json(stress_validation_path)
    cost_validation_payload = _read_optional_json(cost_validation_path)
    stress_summary = _mapping(stress_payload.get("summary"))
    cost_summary = _mapping(cost_payload.get("summary"))
    stress_status = _text(
        stress_payload.get("status"),
        _text(stress_summary.get("stress_result"), "MISSING"),
    )
    cost_status = _text(
        cost_payload.get("status"),
        _text(cost_summary.get("cost_benchmark_status"), "MISSING"),
    )
    stress_validation_status = _text(
        stress_validation_payload.get("status"),
        _text(
            _mapping(stress_validation_payload.get("summary")).get("validation_status"),
            "MISSING",
        ),
    )
    cost_validation_status = _text(
        cost_validation_payload.get("status"),
        _text(
            _mapping(cost_validation_payload.get("summary")).get("validation_status"),
            "MISSING",
        ),
    )
    availability = "AVAILABLE" if stress_payload and cost_payload else "PARTIAL"
    source_backfill_status = _text(
        cost_summary.get("source_backfill_status"),
        _text(stress_summary.get("source_backfill_status"), "MISSING"),
    )
    blocker_count = _int(stress_summary.get("major_blocker_count")) + _int(
        cost_summary.get("major_blocker_count")
    )
    warning_count = _int(stress_summary.get("major_warning_count")) + _int(
        cost_summary.get("major_warning_count")
    )
    return {
        "availability": availability,
        "stress_status": stress_status,
        "cost_benchmark_status": cost_status,
        "cost_survival_status": _text(
            cost_summary.get("cost_survival_status"),
            "MISSING",
        ),
        "benchmark_relative_status": _text(
            cost_summary.get("benchmark_relative_status"),
            "MISSING",
        ),
        "source_backfill_status": source_backfill_status,
        "stress_validation_status": stress_validation_status,
        "cost_validation_status": cost_validation_status,
        "major_blocker_count": blocker_count,
        "major_warning_count": warning_count,
        "next_action": _text(
            cost_payload.get("next_action"),
            _text(stress_payload.get("next_action"), "MISSING"),
        ),
        "stress_detail_report": "" if stress_path is None else str(stress_path),
        "cost_detail_report": "" if cost_path is None else str(cost_path),
        "stress_validation_detail_report": ""
        if stress_validation_path is None
        else str(stress_validation_path),
        "cost_validation_detail_report": ""
        if cost_validation_path is None
        else str(cost_validation_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"stress={stress_status}; cost_benchmark={cost_status}; "
            f"backfill={source_backfill_status}; blockers={blocker_count}; "
            f"warnings={warning_count}."
        ),
        "limitation": (
            "Reader Brief only reads generated TRADING-465 research artifacts; it "
            "does not optimize the candidate, create paper-shadow, write official "
            "weights, or create broker/order artifacts."
        ),
    }


def _missing_next_candidate_stress_cost_benchmark_summary(
    reason: str,
) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "stress_status": "MISSING",
        "cost_benchmark_status": "MISSING",
        "cost_survival_status": "MISSING",
        "benchmark_relative_status": "MISSING",
        "source_backfill_status": "MISSING",
        "stress_validation_status": "MISSING",
        "cost_validation_status": "MISSING",
        "major_blocker_count": 0,
        "major_warning_count": 0,
        "next_action": "run_aits_reports_next_candidate_stress_and_cost_benchmark",
        "stress_detail_report": "",
        "cost_detail_report": "",
        "stress_validation_detail_report": "",
        "cost_validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_stress_review / next_candidate_cost_benchmark_review "
            "artifacts missing; run TRADING-465 reports."
        ),
        "limitation": reason,
    }


def _next_candidate_vs_returned_comparison_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_vs_returned_comparison_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next candidate vs returned comparison."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "next_candidate_vs_returned_candidate_comparison",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_next_candidate_vs_returned_comparison_summary(
            "next_candidate_vs_returned_candidate_comparison artifact missing "
            "from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_vs_returned_candidate_comparison_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("comparison_result")))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "comparison_result": _text(summary.get("comparison_result"), status),
        "previous_candidate_id": _text(summary.get("previous_candidate_id"), "MISSING"),
        "new_candidate_id": _text(summary.get("new_candidate_id"), "MISSING"),
        "real_metrics_available": summary.get("real_metrics_available") is True,
        "source_backfill_status": _text(
            summary.get("source_backfill_status"),
            "MISSING",
        ),
        "stress_result": _text(summary.get("stress_result"), "MISSING"),
        "cost_survival_status": _text(summary.get("cost_survival_status"), "MISSING"),
        "benchmark_relative_status": _text(
            summary.get("benchmark_relative_status"),
            "MISSING",
        ),
        "repeated_failure_mode_count": _int(
            summary.get("repeated_failure_mode_count")
        ),
        "improved_metric_count": _int(summary.get("improved_metric_count")),
        "mixed_metric_count": _int(summary.get("mixed_metric_count")),
        "no_improvement_count": _int(summary.get("no_improvement_count")),
        "governance_blockers": _text(summary.get("governance_blockers"), "MISSING"),
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": ""
        if validation_path is None
        else str(validation_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"vs_returned={status}; repeated_failures="
            f"{_int(summary.get('repeated_failure_mode_count'))}; "
            f"benchmark={_text(summary.get('benchmark_relative_status'), 'MISSING')}."
        ),
        "limitation": (
            "Reader Brief only reads the generated TRADING-466 comparison; it "
            "does not activate paper-shadow, write official weights, append owner "
            "decisions, or create broker/order artifacts."
        ),
    }


def _missing_next_candidate_vs_returned_comparison_summary(
    reason: str,
) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "comparison_result": "MISSING",
        "previous_candidate_id": "MISSING",
        "new_candidate_id": "MISSING",
        "real_metrics_available": False,
        "source_backfill_status": "MISSING",
        "stress_result": "MISSING",
        "cost_survival_status": "MISSING",
        "benchmark_relative_status": "MISSING",
        "repeated_failure_mode_count": 0,
        "improved_metric_count": 0,
        "mixed_metric_count": 0,
        "no_improvement_count": 0,
        "governance_blockers": "MISSING",
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_vs_returned_comparison",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_vs_returned_candidate_comparison artifact missing; "
            "run TRADING-466 comparison."
        ),
        "limitation": reason,
    }


def _next_candidate_signal_window_sensitivity_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_signal_window_sensitivity_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next candidate signal/window sensitivity reviews."
        )
    signal_path = _report_index_artifact_path(
        report_index,
        "next_candidate_signal_robustness_review",
    )
    window_path = _report_index_artifact_path(
        report_index,
        "next_candidate_overfit_window_sensitivity",
    )
    signal_payload = _read_optional_json(signal_path)
    window_payload = _read_optional_json(window_path)
    if not signal_payload and not window_payload:
        return _missing_next_candidate_signal_window_sensitivity_summary(
            "next_candidate_signal_robustness_review and "
            "next_candidate_overfit_window_sensitivity artifacts are missing "
            "from report index latest pointers."
        )
    signal_validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_signal_robustness_review_validation",
    )
    window_validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_overfit_window_sensitivity_validation",
    )
    signal_validation_payload = _read_optional_json(signal_validation_path)
    window_validation_payload = _read_optional_json(window_validation_path)
    signal_summary = _mapping(signal_payload.get("summary"))
    window_summary = _mapping(window_payload.get("summary"))
    signal_status = _text(
        signal_payload.get("status"),
        _text(signal_summary.get("signal_robustness_status"), "MISSING"),
    )
    window_status = _text(
        window_payload.get("status"),
        _text(window_summary.get("window_sensitivity_status"), "MISSING"),
    )
    signal_validation_status = _text(
        signal_validation_payload.get("status"),
        _text(
            _mapping(signal_validation_payload.get("summary")).get(
                "validation_status"
            ),
            "MISSING",
        ),
    )
    window_validation_status = _text(
        window_validation_payload.get("status"),
        _text(
            _mapping(window_validation_payload.get("summary")).get(
                "validation_status"
            ),
            "MISSING",
        ),
    )
    availability = "AVAILABLE" if signal_payload and window_payload else "PARTIAL"
    return {
        "availability": availability,
        "signal_status": signal_status,
        "window_status": window_status,
        "signal_validation_status": signal_validation_status,
        "window_validation_status": window_validation_status,
        "source_signal_binding_status": _text(
            signal_summary.get("source_signal_binding_status"),
            "MISSING",
        ),
        "source_backfill_status": _text(
            window_summary.get("source_backfill_status"),
            "MISSING",
        ),
        "backfill_signal_completeness": _text(
            signal_summary.get("backfill_signal_completeness"),
            "MISSING",
        ),
        "signal_blocking_check_count": _int(
            signal_summary.get("blocking_check_count")
        ),
        "signal_warning_check_count": _int(signal_summary.get("warning_check_count")),
        "window_weak_split_count": _int(window_summary.get("weak_split_count")),
        "window_partial_static_proxy_split_count": _int(
            window_summary.get("partial_static_proxy_split_count")
        ),
        "window_unavailable_split_count": _int(
            window_summary.get("unavailable_split_count")
        ),
        "overfit_risk": _text(window_summary.get("overfit_risk"), "MISSING"),
        "next_action": _text(
            window_payload.get("next_action"),
            _text(signal_payload.get("next_action"), "MISSING"),
        ),
        "signal_detail_report": "" if signal_path is None else str(signal_path),
        "window_detail_report": "" if window_path is None else str(window_path),
        "signal_validation_detail_report": ""
        if signal_validation_path is None
        else str(signal_validation_path),
        "window_validation_detail_report": ""
        if window_validation_path is None
        else str(window_validation_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"signal={signal_status}; window={window_status}; "
            f"overfit_risk={_text(window_summary.get('overfit_risk'), 'MISSING')}; "
            f"signal_blocks={_int(signal_summary.get('blocking_check_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads generated TRADING-467 research artifacts; it "
            "does not activate paper-shadow, write official weights, append owner "
            "decisions, or create broker/order artifacts."
        ),
    }


def _missing_next_candidate_signal_window_sensitivity_summary(
    reason: str,
) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "signal_status": "MISSING",
        "window_status": "MISSING",
        "signal_validation_status": "MISSING",
        "window_validation_status": "MISSING",
        "source_signal_binding_status": "MISSING",
        "source_backfill_status": "MISSING",
        "backfill_signal_completeness": "MISSING",
        "signal_blocking_check_count": 0,
        "signal_warning_check_count": 0,
        "window_weak_split_count": 0,
        "window_partial_static_proxy_split_count": 0,
        "window_unavailable_split_count": 0,
        "overfit_risk": "MISSING",
        "next_action": "run_aits_reports_next_candidate_signal_and_window_sensitivity",
        "signal_detail_report": "",
        "window_detail_report": "",
        "signal_validation_detail_report": "",
        "window_validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_signal_robustness_review / "
            "next_candidate_overfit_window_sensitivity artifacts missing; run "
            "TRADING-467 reports."
        ),
        "limitation": reason,
    }


def _next_candidate_research_gate_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_research_gate_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next candidate research gate."
        )
    report_path = _report_index_artifact_path(report_index, "next_candidate_research_gate")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_next_candidate_research_gate_summary(
            "next_candidate_research_gate artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_research_gate_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("research_gate_decision")))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "research_gate_decision": _text(summary.get("research_gate_decision"), status),
        "validation_status": validation_status,
        "safety_audit_status": _text(summary.get("safety_audit_status"), "MISSING"),
        "source_backfill_status": _text(summary.get("source_backfill_status"), "MISSING"),
        "stress_result": _text(summary.get("stress_result"), "MISSING"),
        "cost_benchmark_status": _text(summary.get("cost_benchmark_status"), "MISSING"),
        "vs_returned_status": _text(summary.get("vs_returned_status"), "MISSING"),
        "signal_robustness_status": _text(
            summary.get("signal_robustness_status"),
            "MISSING",
        ),
        "window_sensitivity_status": _text(
            summary.get("window_sensitivity_status"),
            "MISSING",
        ),
        "blocker_count": _int(summary.get("blocker_count")),
        "strongest_positive_evidence_count": _int(
            summary.get("strongest_positive_evidence_count")
        ),
        "strongest_negative_evidence_count": _int(
            summary.get("strongest_negative_evidence_count")
        ),
        "paper_shadow_activation_allowed": bool(
            summary.get("paper_shadow_activation_allowed")
        ),
        "official_target_weights_generated": bool(
            summary.get("official_target_weights_generated")
        ),
        "broker_order_allowed": bool(summary.get("broker_order_allowed")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": ""
        if validation_path is None
        else str(validation_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"research_gate={status}; blockers={_int(summary.get('blocker_count'))}; "
            f"paper_shadow_allowed={bool(summary.get('paper_shadow_activation_allowed'))}."
        ),
        "limitation": (
            "Reader Brief only reads the generated TRADING-468 research gate; it "
            "does not activate paper-shadow, write official weights, append owner "
            "decisions, or create broker/order artifacts."
        ),
    }


def _missing_next_candidate_research_gate_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "research_gate_decision": "MISSING",
        "validation_status": "MISSING",
        "safety_audit_status": "MISSING",
        "source_backfill_status": "MISSING",
        "stress_result": "MISSING",
        "cost_benchmark_status": "MISSING",
        "vs_returned_status": "MISSING",
        "signal_robustness_status": "MISSING",
        "window_sensitivity_status": "MISSING",
        "blocker_count": 0,
        "strongest_positive_evidence_count": 0,
        "strongest_negative_evidence_count": 0,
        "paper_shadow_activation_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "next_action": "run_aits_reports_next_candidate_research_gate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_research_gate artifact missing; run TRADING-468 gate."
        ),
        "limitation": reason,
    }


def _next_candidate_owner_research_review_packet_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_next_candidate_owner_research_review_packet_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "next candidate owner research review packet."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "next_candidate_owner_research_review_packet",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_next_candidate_owner_research_review_packet_summary(
            "next_candidate_owner_research_review_packet artifact missing from "
            "report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_owner_research_review_packet_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "MISSING"),
        "validation_status": validation_status,
        "source_research_gate_decision": _text(
            summary.get("source_research_gate_decision"),
            "MISSING",
        ),
        "source_gate_blocker_count": _int(summary.get("source_gate_blocker_count")),
        "source_gate_required_next_action": _text(
            summary.get("source_gate_required_next_action"),
            "MISSING",
        ),
        "option_count": _int(summary.get("option_count")),
        "owner_option_ids": _texts(summary.get("owner_option_ids")),
        "owner_decision_appended": bool(summary.get("owner_decision_appended")),
        "paper_shadow_activation_allowed": bool(
            summary.get("paper_shadow_activation_allowed")
        ),
        "extended_shadow_allowed": bool(summary.get("extended_shadow_allowed")),
        "live_trading_allowed": bool(summary.get("live_trading_allowed")),
        "official_target_weights_generated": bool(
            summary.get("official_target_weights_generated")
        ),
        "broker_order_allowed": bool(summary.get("broker_order_allowed")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": ""
        if validation_path is None
        else str(validation_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"owner_packet={_text(payload.get('status'), 'MISSING')}; "
            f"options={_int(summary.get('option_count'))}; "
            f"owner_decision_appended={bool(summary.get('owner_decision_appended'))}."
        ),
        "limitation": (
            "Reader Brief only reads the generated TRADING-469 owner packet; it "
            "does not append owner decisions, activate paper-shadow, write official "
            "weights, or create broker/order artifacts."
        ),
    }


def _missing_next_candidate_owner_research_review_packet_summary(
    reason: str,
) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_research_gate_decision": "MISSING",
        "source_gate_blocker_count": 0,
        "source_gate_required_next_action": "MISSING",
        "option_count": 0,
        "owner_option_ids": [],
        "owner_decision_appended": False,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "next_action": "run_aits_reports_next_candidate_owner_research_review_packet",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_owner_research_review_packet artifact missing; run "
            "TRADING-469 packet."
        ),
        "limitation": reason,
    }


def _executable_binding_contract_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_executable_binding_contract_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "executable binding contract."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "next_candidate_executable_binding_contract",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_executable_binding_contract_summary(
            "next_candidate_executable_binding_contract artifact missing from "
            "report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_executable_binding_contract_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    contract = _mapping(payload.get("binding_contract"))
    input_schema = _mapping(contract.get("input_schema"))
    output_schema = _mapping(contract.get("output_schema"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("contract_status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "contract_status": _text(summary.get("contract_status"), status),
        "candidate_id": _text(contract.get("candidate_id"), "MISSING"),
        "binding_version": _text(contract.get("binding_version"), "MISSING"),
        "input_schema_id": _text(input_schema.get("schema_id"), "MISSING"),
        "output_schema_id": _text(output_schema.get("schema_id"), "MISSING"),
        "research_only": contract.get("research_only") is True,
        "manual_review_only": contract.get("manual_review_only") is True,
        "official_target_weights": contract.get("official_target_weights") is True,
        "strategy_behavior_implemented": contract.get("strategy_behavior_implemented") is True,
        "signal_binding_implemented": contract.get("signal_binding_implemented") is True,
        "weight_binding_implemented": contract.get("weight_binding_implemented") is True,
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "broker_effect": _text(payload.get("broker_effect"), PRODUCTION_EFFECT),
        "order_effect": _text(payload.get("order_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"executable_binding_contract={status}; validation={validation_status}; "
            f"signal_binding_implemented={contract.get('signal_binding_implemented') is True}; "
            f"official_target_weights={contract.get('official_target_weights') is True}."
        ),
        "limitation": (
            "Reader Brief only reads the executable binding contract artifact; "
            "it does not compute signals, produce hypothetical weights, create "
            "paper-shadow candidates, approve live trading, or create orders."
        ),
    }


def _missing_executable_binding_contract_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "contract_status": "MISSING",
        "candidate_id": "MISSING",
        "binding_version": "MISSING",
        "input_schema_id": "MISSING",
        "output_schema_id": "MISSING",
        "research_only": False,
        "manual_review_only": False,
        "official_target_weights": False,
        "strategy_behavior_implemented": False,
        "signal_binding_implemented": False,
        "weight_binding_implemented": False,
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_executable_binding_contract",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": PRODUCTION_EFFECT,
        "order_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_executable_binding_contract artifact missing; "
            "run reports next-candidate-executable-binding-contract."
        ),
        "limitation": reason,
    }


def _executable_signal_binding_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_executable_signal_binding_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "executable signal binding."
        )
    report_path = _report_index_artifact_path(report_index, "next_candidate_signal_binding")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_executable_signal_binding_summary(
            "next_candidate_signal_binding artifact missing from report index "
            "latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_signal_binding_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    signal_binding = _mapping(payload.get("signal_binding"))
    signal_state = _mapping(payload.get("signal_state"))
    confidence_uncertainty = _mapping(signal_state.get("confidence_uncertainty"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("signal_binding_status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "signal_binding_status": _text(summary.get("signal_binding_status"), status),
        "candidate_id": _text(summary.get("candidate_id"), "MISSING"),
        "binding_version": _text(signal_binding.get("binding_version"), "MISSING"),
        "latest_signal_date": _text(summary.get("latest_signal_date"), "MISSING"),
        "signal_row_count": _int(summary.get("signal_row_count")),
        "signal_score": signal_state.get("signal_score"),
        "regime_state": _text(signal_state.get("regime_state"), "MISSING"),
        "risk_state": _text(signal_state.get("risk_state"), "MISSING"),
        "rotation_state": _text(signal_state.get("rotation_state"), "MISSING"),
        "confidence": _text(confidence_uncertainty.get("confidence"), "MISSING"),
        "uncertainty": _text(confidence_uncertainty.get("uncertainty"), "MISSING"),
        "blocking_reason": _text(summary.get("blocking_reason"), "none"),
        "warning_reason": _text(summary.get("warning_reason"), "none"),
        "research_only": signal_binding.get("research_only") is True,
        "manual_review_only": signal_binding.get("manual_review_only") is True,
        "official_target_weights": signal_binding.get("official_target_weights") is True,
        "hypothetical_research_weight_produced": (
            signal_binding.get("hypothetical_research_weight_produced") is True
        ),
        "backfill_metrics_produced": signal_binding.get("backfill_metrics_produced") is True,
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "broker_effect": _text(payload.get("broker_effect"), PRODUCTION_EFFECT),
        "order_effect": _text(payload.get("order_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"executable_signal_binding={status}; validation={validation_status}; "
            f"rows={_int(summary.get('signal_row_count'))}; "
            "official_target_weights="
            f"{signal_binding.get('official_target_weights') is True}; "
            "hypothetical_weights="
            f"{signal_binding.get('hypothetical_research_weight_produced') is True}."
        ),
        "limitation": (
            "Reader Brief only reads the signal binding artifact; it does not "
            "produce weights, run backfill, create paper-shadow candidates, "
            "approve live trading, or create orders."
        ),
    }


def _missing_executable_signal_binding_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "signal_binding_status": "MISSING",
        "candidate_id": "MISSING",
        "binding_version": "MISSING",
        "latest_signal_date": "MISSING",
        "signal_row_count": 0,
        "signal_score": None,
        "regime_state": "MISSING",
        "risk_state": "MISSING",
        "rotation_state": "MISSING",
        "confidence": "MISSING",
        "uncertainty": "MISSING",
        "blocking_reason": "MISSING",
        "warning_reason": "MISSING",
        "research_only": False,
        "manual_review_only": False,
        "official_target_weights": False,
        "hypothetical_research_weight_produced": False,
        "backfill_metrics_produced": False,
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_signal_binding",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": PRODUCTION_EFFECT,
        "order_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_signal_binding artifact missing; run reports "
            "next-candidate-signal-binding."
        ),
        "limitation": reason,
    }


def _executable_research_weight_binding_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_executable_research_weight_binding_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "research weight binding."
        )
    report_path = _report_index_artifact_path(
        report_index,
        "next_candidate_research_weight_binding",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_executable_research_weight_binding_summary(
            "next_candidate_research_weight_binding artifact missing from "
            "report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "next_candidate_research_weight_binding_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    binding_meta = _mapping(payload.get("research_weight_binding"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(
        payload.get("status"),
        _text(summary.get("research_weight_binding_status"), "UNKNOWN"),
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "research_weight_binding_status": _text(
            summary.get("research_weight_binding_status"),
            status,
        ),
        "candidate_id": _text(summary.get("candidate_id"), "MISSING"),
        "binding_version": _text(binding_meta.get("binding_version"), "MISSING"),
        "latest_signal_date": _text(summary.get("latest_signal_date"), "MISSING"),
        "weight_row_count": _int(summary.get("weight_row_count")),
        "risk_state": _text(summary.get("risk_state"), "MISSING"),
        "rotation_state": _text(summary.get("rotation_state"), "MISSING"),
        "turnover_proxy": summary.get("turnover_proxy"),
        "constraint_hit_count": _int(summary.get("constraint_hit_count")),
        "blocking_reason": _text(summary.get("blocking_reason"), "none"),
        "warning_reason": _text(summary.get("warning_reason"), "none"),
        "research_only": binding_meta.get("research_only") is True,
        "manual_review_only": binding_meta.get("manual_review_only") is True,
        "official_target_weights": binding_meta.get("official_target_weights") is True,
        "broker_order_produced": binding_meta.get("broker_order_produced") is True,
        "backfill_metrics_produced": binding_meta.get("backfill_metrics_produced") is True,
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "broker_effect": _text(payload.get("broker_effect"), PRODUCTION_EFFECT),
        "order_effect": _text(payload.get("order_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_weight_binding={status}; validation={validation_status}; "
            f"rows={_int(summary.get('weight_row_count'))}; "
            f"official_target_weights={binding_meta.get('official_target_weights') is True}; "
            f"broker_order={binding_meta.get('broker_order_produced') is True}."
        ),
        "limitation": (
            "Reader Brief only reads the research weight binding artifact; it "
            "does not run backfill, activate paper-shadow, approve live trading, "
            "create official target weights, or create orders."
        ),
    }


def _missing_executable_research_weight_binding_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "research_weight_binding_status": "MISSING",
        "candidate_id": "MISSING",
        "binding_version": "MISSING",
        "latest_signal_date": "MISSING",
        "weight_row_count": 0,
        "risk_state": "MISSING",
        "rotation_state": "MISSING",
        "turnover_proxy": None,
        "constraint_hit_count": 0,
        "blocking_reason": "MISSING",
        "warning_reason": "MISSING",
        "research_only": False,
        "manual_review_only": False,
        "official_target_weights": False,
        "broker_order_produced": False,
        "backfill_metrics_produced": False,
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_next_candidate_research_weight_binding",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": PRODUCTION_EFFECT,
        "order_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "next_candidate_research_weight_binding artifact missing; run "
            "reports next-candidate-research-weight-binding."
        ),
        "limitation": reason,
    }


def _executable_binding_safety_audit_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_executable_binding_safety_audit_summary(
            "report_index artifact missing; Reader Brief cannot discover "
            "executable binding safety audit."
        )
    report_path = _report_index_artifact_path(report_index, "executable_binding_safety_audit")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_executable_binding_safety_audit_summary(
            "executable_binding_safety_audit artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "executable_binding_safety_audit_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    validation_status = _text(
        validation_payload.get("status"),
        _text(_mapping(validation_payload.get("summary")).get("validation_status"), "MISSING"),
    )
    status = _text(payload.get("status"), _text(summary.get("safety_audit_status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "safety_audit_status": _text(summary.get("safety_audit_status"), status),
        "candidate_id": _text(summary.get("candidate_id"), "MISSING"),
        "acceptable_warning": summary.get("acceptable_warning") is True,
        "artifact_check_count": _int(summary.get("artifact_check_count")),
        "failed_artifact_check_count": _int(summary.get("failed_artifact_check_count")),
        "warning_artifact_check_count": _int(summary.get("warning_artifact_check_count")),
        "static_scan_path_count": _int(summary.get("static_scan_path_count")),
        "static_scan_finding_count": _int(summary.get("static_scan_finding_count")),
        "blocking_static_finding_count": _int(
            summary.get("blocking_static_finding_count")
        ),
        "warning_static_finding_count": _int(summary.get("warning_static_finding_count")),
        "signal_binding_research_only": summary.get("signal_binding_research_only") is True,
        "weight_binding_hypothetical_only": (
            summary.get("weight_binding_hypothetical_only") is True
        ),
        "official_target_weights": summary.get("official_target_weights") is True,
        "paper_shadow_activation": summary.get("paper_shadow_activation") is True,
        "owner_decision_append": summary.get("owner_decision_append") is True,
        "validation_status": validation_status,
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "broker_effect": _text(payload.get("broker_effect"), PRODUCTION_EFFECT),
        "order_effect": _text(payload.get("order_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"executable_binding_safety_audit={status}; validation={validation_status}; "
            f"artifact_failures={_int(summary.get('failed_artifact_check_count'))}; "
            f"static_blockers={_int(summary.get('blocking_static_finding_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the executable binding safety audit; it does not "
            "run backfill, activate paper-shadow, approve live trading, create "
            "official target weights, or create orders."
        ),
    }


def _missing_executable_binding_safety_audit_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "safety_audit_status": "MISSING",
        "candidate_id": "MISSING",
        "acceptable_warning": False,
        "artifact_check_count": 0,
        "failed_artifact_check_count": 0,
        "warning_artifact_check_count": 0,
        "static_scan_path_count": 0,
        "static_scan_finding_count": 0,
        "blocking_static_finding_count": 0,
        "warning_static_finding_count": 0,
        "signal_binding_research_only": False,
        "weight_binding_hypothetical_only": False,
        "official_target_weights": False,
        "paper_shadow_activation": False,
        "owner_decision_append": False,
        "validation_status": "MISSING",
        "next_action": "run_aits_reports_executable_binding_safety_audit",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": PRODUCTION_EFFECT,
        "order_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "executable_binding_safety_audit artifact missing; run reports "
            "executable-binding-safety-audit."
        ),
        "limitation": reason,
    }


def _research_safety_boundary_audit_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_research_safety_boundary_audit_summary(
            "report_index artifact missing; Reader Brief cannot discover safety boundary audit."
        )
    report_path = _report_index_artifact_path(report_index, "research_safety_boundary_audit")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_research_safety_boundary_audit_summary(
            "research_safety_boundary_audit artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "research_safety_boundary_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("safety_status"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "safety_status": status,
        "validation_status": validation_status,
        "task_check_count": _int(summary.get("task_check_count")),
        "artifact_check_count": _int(summary.get("artifact_check_count")),
        "checked_artifact_count": _int(summary.get("checked_artifact_count")),
        "missing_metadata_count": _int(summary.get("missing_metadata_count")),
        "unsafe_signal_count": _int(summary.get("unsafe_signal_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
        "shadow_continuation_readiness_input": _text(
            summary.get("shadow_continuation_readiness_input"),
            "UNKNOWN",
        ),
        "future_promotion_board_input": _text(
            summary.get("future_promotion_board_input"),
            "UNKNOWN",
        ),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"research_safety_boundary={status}; "
            f"unsafe_signals={_int(summary.get('unsafe_signal_count'))}; "
            f"missing_metadata={_int(summary.get('missing_metadata_count'))}; "
            f"shadow_input={_text(summary.get('shadow_continuation_readiness_input'), 'UNKNOWN')}."
        ),
        "limitation": (
            "Reader Brief only reads the latest safety boundary audit artifacts from "
            "report index; it does not run validation or approve promotion."
        ),
    }


def _missing_research_safety_boundary_audit_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "safety_status": "MISSING",
        "validation_status": "MISSING",
        "task_check_count": 0,
        "artifact_check_count": 0,
        "checked_artifact_count": 0,
        "missing_metadata_count": 0,
        "unsafe_signal_count": 0,
        "blocking_issue_count": 0,
        "warning_issue_count": 0,
        "shadow_continuation_readiness_input": "MISSING",
        "future_promotion_board_input": "MISSING",
        "next_action": "run_aits_reports_research_safety_boundary_audit_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "research_safety_boundary_audit artifact missing; run reports "
            "research-safety-boundary-audit."
        ),
        "limitation": reason,
    }


def _artifact_lineage_graph_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_artifact_lineage_graph_summary(
            "report_index artifact missing; Reader Brief cannot discover artifact_lineage_graph."
        )
    report_path = _report_index_artifact_path(report_index, "artifact_lineage_graph")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_artifact_lineage_graph_summary(
            "artifact_lineage_graph artifact missing from report index latest pointer."
        )
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("lineage_status"), _text(payload.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "lineage_status": status,
        "node_count": _int(summary.get("node_count")),
        "available_node_count": _int(summary.get("available_node_count")),
        "required_family_count": _int(summary.get("required_family_count")),
        "available_required_family_count": _int(
            summary.get("available_required_family_count")
        ),
        "required_edge_count": _int(summary.get("required_edge_count")),
        "passing_required_edge_count": _int(summary.get("passing_required_edge_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"artifact_lineage_status={status}; "
            f"families={_int(summary.get('available_required_family_count'))}/"
            f"{_int(summary.get('required_family_count'))}; "
            f"edges={_int(summary.get('passing_required_edge_count'))}/"
            f"{_int(summary.get('required_edge_count'))}; "
            f"blocking={_int(summary.get('blocking_issue_count'))}; "
            f"warnings={_int(summary.get('warning_issue_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest artifact_lineage_graph artifact from "
            "report index; it does not run lineage generation or validation."
        ),
    }


def _missing_artifact_lineage_graph_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "lineage_status": "MISSING",
        "node_count": 0,
        "available_node_count": 0,
        "required_family_count": 0,
        "available_required_family_count": 0,
        "required_edge_count": 0,
        "passing_required_edge_count": 0,
        "blocking_issue_count": 0,
        "warning_issue_count": 0,
        "next_action": "run_aits_reports_artifact_lineage_then_validate_artifact_lineage",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "artifact_lineage_graph artifact missing; run reports artifact-lineage."
        ),
        "limitation": reason,
    }


def _task_register_consistency_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_task_register_consistency_summary(
            "report_index artifact missing; Reader Brief cannot discover task_register_consistency."
        )
    report_path = _report_index_artifact_path(report_index, "task_register_consistency")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_task_register_consistency_summary(
            "task_register_consistency artifact missing from report index latest pointer."
        )
    validation_path = _report_index_artifact_path(
        report_index,
        "task_register_consistency_validation",
    )
    validation_payload = _read_optional_json(validation_path)
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("consistency_status"), _text(payload.get("status"), "UNKNOWN"))
    validation_status = _text(
        _mapping(validation_payload).get("validation_status"),
        "MISSING",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "consistency_status": status,
        "validation_status": validation_status,
        "active_task_count": _int(summary.get("active_task_count")),
        "completed_task_count": _int(summary.get("completed_task_count")),
        "check_count": _int(summary.get("check_count")),
        "failed_check_count": _int(summary.get("failed_check_count")),
        "blocking_issue_count": _int(summary.get("blocking_issue_count")),
        "warning_issue_count": _int(summary.get("warning_issue_count")),
        "explicit_docs_link_count": _int(summary.get("explicit_docs_link_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "validation_detail_report": "" if validation_path is None else str(validation_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"task_register_consistency={status}; "
            f"active={_int(summary.get('active_task_count'))}; "
            f"completed={_int(summary.get('completed_task_count'))}; "
            f"blocking={_int(summary.get('blocking_issue_count'))}; "
            f"warnings={_int(summary.get('warning_issue_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest task_register_consistency artifact from "
            "report index; it does not run checks or edit task registers."
        ),
    }


def _missing_task_register_consistency_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "consistency_status": "MISSING",
        "validation_status": "MISSING",
        "active_task_count": 0,
        "completed_task_count": 0,
        "check_count": 0,
        "failed_check_count": 0,
        "blocking_issue_count": 0,
        "warning_issue_count": 0,
        "explicit_docs_link_count": 0,
        "next_action": "run_aits_reports_task_register_consistency_then_validate",
        "detail_report": "",
        "validation_detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "task_register_consistency artifact missing; run reports "
            "task-register-consistency."
        ),
        "limitation": reason,
    }


def _report_quality_gate_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_report_quality_gate_summary(
            "report_index artifact missing; Reader Brief cannot discover report_quality_gate."
        )
    report_path = _report_index_artifact_path(report_index, "report_quality_gate")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_report_quality_gate_summary(
            "report_quality_gate artifact missing from report index latest pointer."
        )
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("report_quality_status"), _text(payload.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "report_quality_status": status,
        "checked_report_count": _int(summary.get("checked_report_count")),
        "checked_reader_brief_count": _int(summary.get("checked_reader_brief_count")),
        "missing_section_count": _int(summary.get("missing_section_count")),
        "blocking_quality_issue_count": _int(summary.get("blocking_quality_issue_count")),
        "warning_quality_issue_count": _int(summary.get("warning_quality_issue_count")),
        "next_action": _text(payload.get("next_action"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"report_quality_status={status}; "
            f"missing_sections={_int(summary.get('missing_section_count'))}; "
            f"blocking={_int(summary.get('blocking_quality_issue_count'))}; "
            f"warnings={_int(summary.get('warning_quality_issue_count'))}."
        ),
        "limitation": (
            "Reader Brief only reads the latest report_quality_gate artifact from report index; "
            "it does not run the quality gate."
        ),
    }


def _missing_report_quality_gate_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "report_quality_status": "MISSING",
        "checked_report_count": 0,
        "checked_reader_brief_count": 0,
        "missing_section_count": 0,
        "blocking_quality_issue_count": 0,
        "warning_quality_issue_count": 0,
        "next_action": "run_aits_reports_quality_gate_after_reader_brief_generation",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": "report_quality_gate artifact missing; run reports quality-gate.",
        "limitation": reason,
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
    "market_panel": (
        "IMPORTANT",
        "缺少 market panel 时读者无法看到 benchmark、AI sector、risk 和 liquidity 代理实际涨跌。",
        "不改变今日 score，但限制 Market Situation 的可读性。",
        "运行 aits reports market-panel。",
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
    governance_summary: Mapping[str, Any],
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
        "impact_summary": _artifact_impact_summary(
            items=items,
            report_index_summary=report_index_summary,
            governance_summary=governance_summary,
        ),
        "production_effect": PRODUCTION_EFFECT,
        "items": items,
    }


def _artifact_impact_summary(
    *,
    items: list[dict[str, Any]],
    report_index_summary: Mapping[str, Any],
    governance_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    required_missing = _int(report_index_summary.get("required_missing_count"))
    blocking = len([item for item in items if item.get("impact_level") == "BLOCKING"])
    reader_missing = _int(report_index_summary.get("missing_count"))
    reader_stale = _int(report_index_summary.get("stale_count"))
    important = len([item for item in items if item.get("impact_level") == "IMPORTANT"])
    promotion_missing = _int(governance_summary.get("missing_count"))
    promotion_status = _research_promotion_status(governance_summary)
    return [
        {
            "chain": "今日评分链路",
            "status": "PASS" if required_missing == 0 and blocking == 0 else "BLOCKED",
            "missing_count": required_missing,
            "interpretation": (
                "required_missing=0，不阻断 daily score。"
                if required_missing == 0 and blocking == 0
                else "存在 required/blocking artifact 缺口，今日结论使用受限。"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "chain": "阅读上下文",
            "status": (
                "OK" if reader_missing == 0 and reader_stale == 0 and important == 0 else "LIMITED"
            ),
            "missing_count": reader_missing,
            "stale_count": reader_stale,
            "important_count": important,
            "interpretation": (
                "影响读者下钻和上下文完整性，不等于自动重算 score。"
                if reader_missing or reader_stale or important
                else "未发现重要阅读上下文缺口。"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        {
            "chain": "研究/权重晋升链路",
            "status": promotion_status,
            "missing_count": promotion_missing,
            "interpretation": (
                "promotion 被缺失 artifact 阻断；不影响今日 score 产物，但不能晋升权重。"
                if promotion_status == "BLOCKED_BY_MISSING_ARTIFACTS"
                else "按 research governance summary 的 promotion_status 处理。"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
    ]


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


def _pit_source_manifest_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_pit_source_manifest_summary(
            "report_index_missing; Reader Brief 不补造 source-level PIT 结论。"
        )
    report_path = _report_index_artifact_path(report_index, "pit_source_manifest")
    if report_path is None:
        return _missing_pit_source_manifest_summary(
            "pit_source_manifest artifact missing from report_index."
        )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_pit_source_manifest_summary(
            f"pit_source_manifest JSON unreadable: {report_path}"
        )
    summary = _mapping(payload.get("summary"))
    policy = _mapping(payload.get("policy"))
    safety = _mapping(payload.get("safety_boundary"))
    non_strong = _texts(summary.get("non_strong_source_ids"))
    safety_status = (
        "PASS"
        if _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("read_only") is True
        and safety.get("broker_action_allowed") is False
        and safety.get("trading_action_allowed") is False
        else "REVIEW_REQUIRED"
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "manifest_id": _text(payload.get("manifest_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "source_count": summary.get("source_count"),
        "strong_pit_count": summary.get("strong_pit_count"),
        "approx_pit_count": summary.get("approx_pit_count"),
        "non_pit_count": summary.get("non_pit_count"),
        "unknown_count": summary.get("unknown_count"),
        "non_strong_source_count": summary.get("non_strong_source_count"),
        "non_strong_source_ids": ", ".join(non_strong) if non_strong else "none",
        "policy_version": _text(policy.get("policy_version"), "UNKNOWN"),
        "safety_status": safety_status,
        "report_path": str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Source-level governance only; grade counts do not promote any source "
            "to production-grade backtest evidence."
        ),
    }


def _missing_pit_source_manifest_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "manifest_id": "MISSING",
        "as_of": "UNKNOWN",
        "source_count": 0,
        "strong_pit_count": 0,
        "approx_pit_count": 0,
        "non_pit_count": 0,
        "unknown_count": 0,
        "non_strong_source_count": 0,
        "non_strong_source_ids": "MISSING",
        "policy_version": "UNKNOWN",
        "safety_status": "MISSING",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
    }


def _data_refresh_audit_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_data_refresh_audit_summary(
            "report_index_missing; Reader Brief 不补造 refresh/validation audit 结论。"
        )
    report_path = _report_index_artifact_path(report_index, "data_refresh_audit")
    if report_path is None:
        return _missing_data_refresh_audit_summary(
            "data_refresh_audit artifact missing from report_index."
        )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_data_refresh_audit_summary(
            f"data_refresh_audit JSON unreadable: {report_path}"
        )
    summary = _mapping(payload.get("summary"))
    safety = _mapping(payload.get("safety_boundary"))
    safety_status = (
        "PASS"
        if _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("read_only") is True
        and safety.get("data_refresh_allowed") is False
        and safety.get("data_downloaded_by_audit") is False
        and safety.get("broker_action_allowed") is False
        and safety.get("trading_action_allowed") is False
        else "REVIEW_REQUIRED"
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "audit_id": _text(payload.get("audit_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "audit_record_count": summary.get("audit_record_count", 0),
        "failed_record_count": summary.get("failed_record_count", 0),
        "skipped_record_count": summary.get("skipped_record_count", 0),
        "skipped_market_closed_count": summary.get("skipped_market_closed_count", 0),
        "skipped_no_new_data_count": summary.get("skipped_no_new_data_count", 0),
        "warning_count": summary.get("warning_count", 0),
        "error_count": summary.get("error_count", 0),
        "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        "safety_status": safety_status,
        "report_path": str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Refresh/validation governance only; audit status records evidence "
            "for paper-shadow review but does not run refreshes or approve "
            "production use."
        ),
    }


def _missing_data_refresh_audit_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "audit_id": "MISSING",
        "as_of": "UNKNOWN",
        "audit_record_count": 0,
        "failed_record_count": 0,
        "skipped_record_count": 0,
        "skipped_market_closed_count": 0,
        "skipped_no_new_data_count": 0,
        "warning_count": 0,
        "error_count": 0,
        "next_action": "generate_data_refresh_audit",
        "safety_status": "MISSING",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
    }


def _data_source_fallback_policy_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_data_source_fallback_policy_summary(
            "report_index_missing; Reader Brief 不补造 fallback policy 结论。"
        )
    report_path = _report_index_artifact_path(report_index, "data_source_fallback_policy")
    if report_path is None:
        return _missing_data_source_fallback_policy_summary(
            "data_source_fallback_policy artifact missing from report_index."
        )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_data_source_fallback_policy_summary(
            f"data_source_fallback_policy JSON unreadable: {report_path}"
        )
    summary = _mapping(payload.get("summary"))
    safety = _mapping(payload.get("safety_boundary"))
    safety_status = (
        "PASS"
        if _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("read_only") is True
        and safety.get("data_refresh_allowed") is False
        and safety.get("cache_mutation_allowed") is False
        and safety.get("broker_action_allowed") is False
        and safety.get("order_ticket_allowed") is False
        else "REVIEW_REQUIRED"
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "report_id": _text(payload.get("report_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "fallback_status": _text(summary.get("fallback_status"), "UNKNOWN"),
        "source_group_count": summary.get("source_group_count", 0),
        "fallback_used_count": summary.get("fallback_used_count", 0),
        "fallback_unavailable_count": summary.get("fallback_unavailable_count", 0),
        "blocked_no_valid_source_count": summary.get("blocked_no_valid_source_count", 0),
        "blocking_source_count": summary.get("blocking_source_count", 0),
        "fallback_used_sources": ", ".join(_texts(summary.get("fallback_used_sources")))
        or "none",
        "blocking_data_types": ", ".join(_texts(summary.get("blocking_data_types")))
        or "none",
        "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        "safety_status": safety_status,
        "report_path": str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Fallback policy is governance-only; fallback data cannot silently "
            "replace primary data or approve production use."
        ),
    }


def _missing_data_source_fallback_policy_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "report_id": "MISSING",
        "as_of": "UNKNOWN",
        "fallback_status": "MISSING",
        "source_group_count": 0,
        "fallback_used_count": 0,
        "fallback_unavailable_count": 0,
        "blocked_no_valid_source_count": 0,
        "blocking_source_count": 0,
        "fallback_used_sources": "none",
        "blocking_data_types": "none",
        "next_action": "generate_data_source_fallback_policy_report",
        "safety_status": "MISSING",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
    }


def _cache_catalog_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_cache_catalog_summary(
            "report_index_missing; Reader Brief 不补造 cache catalog 结论。"
        )
    report_path = _report_index_artifact_path(report_index, "cache_catalog")
    if report_path is None:
        return _missing_cache_catalog_summary(
            "cache_catalog artifact missing from report_index."
        )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_cache_catalog_summary(
            f"cache_catalog JSON unreadable: {report_path}"
        )
    summary = _mapping(payload.get("summary"))
    safety = _mapping(payload.get("safety_boundary"))
    blocking_ids = _texts(summary.get("blocking_entry_ids"))
    safety_status = (
        "PASS"
        if _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("read_only") is True
        and safety.get("data_refresh_allowed") is False
        and safety.get("cache_mutation_allowed") is False
        and safety.get("cache_repair_allowed") is False
        and safety.get("broker_action_allowed") is False
        and safety.get("order_ticket_allowed") is False
        else "REVIEW_REQUIRED"
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "validation_status": _text(
            payload.get("validation_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "cache_integrity_status": _text(
            payload.get("cache_integrity_status"),
            _text(summary.get("cache_integrity_status"), "UNKNOWN"),
        ),
        "catalog_id": _text(payload.get("catalog_id"), "UNKNOWN"),
        "as_of": _text(payload.get("as_of"), "UNKNOWN"),
        "entry_count": summary.get("entry_count", 0),
        "required_entry_count": summary.get("required_entry_count", 0),
        "missing_required_count": summary.get("missing_required_count", 0),
        "missing_optional_count": summary.get("missing_optional_count", 0),
        "checksum_mismatch_count": summary.get("checksum_mismatch_count", 0),
        "checksum_changed_without_refresh_count": summary.get(
            "checksum_changed_without_refresh_count",
            0,
        ),
        "blocking_entry_count": summary.get("blocking_entry_count", 0),
        "blocking_entry_ids": ", ".join(blocking_ids) if blocking_ids else "none",
        "refresh_audit_id": _text(summary.get("refresh_audit_id"), "UNKNOWN"),
        "validated_at": _text(summary.get("validated_at"), "UNKNOWN"),
        "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        "safety_status": safety_status,
        "report_path": str(report_path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "limitation": (
            "Cache catalog is governance-only; it records observed cache metadata "
            "and does not refresh, repair or approve production use."
        ),
    }


def _missing_cache_catalog_summary(reason: str) -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "cache_integrity_status": "MISSING",
        "catalog_id": "MISSING",
        "as_of": "UNKNOWN",
        "entry_count": 0,
        "required_entry_count": 0,
        "missing_required_count": 0,
        "missing_optional_count": 0,
        "checksum_mismatch_count": 0,
        "checksum_changed_without_refresh_count": 0,
        "blocking_entry_count": 0,
        "blocking_entry_ids": "none",
        "refresh_audit_id": "MISSING",
        "validated_at": "MISSING",
        "next_action": "generate_cache_catalog",
        "safety_status": "MISSING",
        "report_path": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": reason,
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
        backtest = _mapping(research_governance_summary.get("backtest"))
        weight_iteration = _mapping(research_governance_summary.get("weight_iteration"))
        shadow_observe = _mapping(research_governance_summary.get("shadow_observe"))
        sec_pit = _mapping(research_governance_summary.get("sec_pit"))
        documentation = _mapping(research_governance_summary.get("documentation"))
        return {
            "availability": "AVAILABLE",
            "source": "research_governance_summary",
            "status": _text(
                research_governance_summary.get("governance_status"),
                _text(research_governance_summary.get("status"), "UNKNOWN"),
            ),
            "research_readiness": _text(
                research_governance_summary.get("research_readiness"),
                "UNKNOWN",
            ),
            "promotion_status": _text(
                research_governance_summary.get("promotion_status"),
                _text(weight_iteration.get("promotion_status"), "UNKNOWN"),
            ),
            "manual_review_required": bool(
                research_governance_summary.get("manual_review_required")
            ),
            "summary_text": _text(research_governance_summary.get("summary_text")),
            "card_count": summary.get("card_count"),
            "missing_count": summary.get("missing_count"),
            "warning_count": summary.get("warning_count"),
            "manual_review_required_count": summary.get("manual_review_required_count"),
            "shadow_observe_count": groups.get("Shadow observe-only"),
            "candidate_research_count": groups.get("Candidate / research-only"),
            "blocked_count": groups.get("Blocked / insufficient data"),
            "backtest_status": _text(backtest.get("backtest_status"), "UNKNOWN"),
            "robustness_status": _text(backtest.get("robustness_status"), "UNKNOWN"),
            "shadow_monitor_status": _text(
                shadow_observe.get("shadow_monitor_status"),
                "UNKNOWN",
            ),
            "sec_pit_shadow_observe_status": _text(
                sec_pit.get("sec_pit_shadow_observe_status"),
                "UNKNOWN",
            ),
            "documentation_contract_status": _text(
                documentation.get("documentation_contract_status"),
                "UNKNOWN",
            ),
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






def _etf_operations_health_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_operations_health_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_operations_health_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_operations_health_summary()

    source_artifacts = _records(report.get("source_artifacts"))
    failures = _records(report.get("failures"))
    warnings = _records(report.get("warnings"))
    run_metadata = _mapping(report.get("run_metadata"))
    freshness_summary = _mapping(report.get("artifact_freshness_summary"))
    freshness_counts = _mapping(freshness_summary.get("freshness_summary"))
    dependency_status = _mapping(report.get("dependency_status"))
    owner_review = _mapping(report.get("owner_review_checklist"))
    safety_banner = _mapping(report.get("safety_banner"))

    stale_artifacts = [
        item for item in source_artifacts if _text(item.get("freshness_status")).lower() == "stale"
    ]
    missing_artifacts = [
        item
        for item in source_artifacts
        if _text(item.get("freshness_status")).lower() == "missing"
    ]
    blocking_failure_count = len(failures) or _int(run_metadata.get("blocking_failure_count"))
    warning_count = len(warnings) or _int(run_metadata.get("warning_count"))
    stale_artifact_count = len(stale_artifacts) or _int(freshness_counts.get("stale"))
    missing_artifact_count = len(missing_artifacts) or _int(freshness_counts.get("missing"))
    cadence = _text(report.get("cadence"), "UNKNOWN")
    status = _text(report.get("status"), _text(report.get("source_dry_run_status"), "UNKNOWN"))
    safety_status = _etf_operations_health_safety_status(report)
    next_owner_review = _etf_operations_health_owner_review(owner_review)

    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"Operations Health: cadence={cadence}; status={status}; "
            f"blocking_failures={blocking_failure_count}; warnings={warning_count}; "
            f"stale_artifacts={stale_artifact_count}; "
            f"missing_artifacts={missing_artifact_count}; safety={safety_status}."
        ),
        "cadence": cadence,
        "pipeline_status": f"{cadence}:{status}",
        "blocking_failure_count": blocking_failure_count,
        "warning_count": warning_count,
        "stale_artifact_count": stale_artifact_count,
        "missing_artifact_count": missing_artifact_count,
        "stale_artifacts": _etf_operations_health_artifact_list(stale_artifacts),
        "missing_artifacts": _etf_operations_health_artifact_list(missing_artifacts),
        "blocking_artifacts": _texts(dependency_status.get("blocking_artifacts")),
        "warning_artifacts": _texts(dependency_status.get("warning_artifacts")),
        "next_owner_review": next_owner_review,
        "owner_checklist_status": _text(owner_review.get("checklist_status"), "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety_banner.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety_banner.get("broker_action"), "none"),
        "manual_review_required": safety_banner.get("manual_review_required") is True,
        "commands_executed": report.get("commands_executed") is True,
        "production_state_mutated": report.get("production_state_mutated") is True,
    }


def _missing_etf_operations_health_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Operations Health: no latest operations health report found.",
        "cadence": "MISSING",
        "pipeline_status": "MISSING",
        "blocking_failure_count": 0,
        "warning_count": 1,
        "stale_artifact_count": 0,
        "missing_artifact_count": 1,
        "stale_artifacts": "none",
        "missing_artifacts": "etf_operations_health_report",
        "blocking_artifacts": [],
        "warning_artifacts": ["etf_operations_health_report"],
        "next_owner_review": "MISSING",
        "owner_checklist_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "Operations health artifact is missing; Reader Brief does not run etf ops report CLI."
        ),
    }


def _etf_operations_health_artifact_list(records: list[dict[str, Any]]) -> str:
    if not records:
        return "none"
    labels: list[str] = []
    for record in records[:5]:
        labels.append(
            f"{_text(record.get('artifact_id'), 'UNKNOWN')} "
            f"({_text(record.get('source_step'), 'UNKNOWN')}; "
            f"freshness={_text(record.get('freshness_status'), 'UNKNOWN')}; "
            f"dependency={_text(record.get('dependency_status'), 'UNKNOWN')})"
        )
    if len(records) > 5:
        labels.append(f"+{len(records) - 5} more")
    return "; ".join(labels)


def _etf_operations_health_owner_review(payload: Mapping[str, Any]) -> str:
    if not payload:
        return "MISSING"
    step_id = _text(payload.get("checklist_step_id"), "MISSING")
    status = _text(payload.get("checklist_status"), "MISSING")
    signoff_required = payload.get("signoff_required")
    return f"{step_id}:{status}; signoff_required={signoff_required is True}"


def _etf_operations_health_safety_status(payload: Mapping[str, Any]) -> str:
    safety_banner = _mapping(payload.get("safety_banner"))
    safe = (
        safety_banner.get("observe_only") is True
        and safety_banner.get("candidate_only") is True
        and _text(safety_banner.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety_banner.get("broker_action") == "none"
        and safety_banner.get("manual_review_required") is True
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "commands_executed=false; production_state_mutated=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )














































def _parameter_shadow_review(as_of: date) -> dict[str, Any]:
    ablation_summary = _signal_ablation_review_summary(as_of)
    calibration_summary = _signal_calibration_review_summary(as_of)
    sensitivity_summary = _portfolio_sensitivity_review_summary(as_of)
    candidates_summary = _portfolio_candidates_review_summary(as_of)
    candidate_review_summary = _portfolio_candidate_review_summary(as_of)
    market_freshness_summary = _market_data_freshness_review_summary(as_of)
    market_refresh_summary = _market_data_refresh_review_summary(as_of)
    candidate_tracking_summary = _portfolio_candidate_tracking_summary(as_of)
    tracking_review_summary = _portfolio_tracking_review_summary(as_of)
    weight_tuning_summary = _weight_tuning_review_summary(as_of)
    weight_tuning_failure_summary = _weight_tuning_failure_review_summary(as_of)
    weight_stability_summary = _weight_stability_review_summary(as_of)
    weight_stability_readiness_summary = _weight_stability_readiness_review_summary(as_of)
    turnover_attribution_summary = _portfolio_turnover_attribution_review_summary(as_of)
    path = (
        PROJECT_ROOT
        / "artifacts"
        / "shadow_backtest"
        / as_of.isoformat()
        / "shadow_backtest_summary.json"
    )
    payload = _read_optional_json(path)
    if not payload:
        diagnostic_path = _default_backtest_input_diagnostic_path(as_of)
        diagnostic_payload = _read_optional_json(diagnostic_path)
        diagnostic_summary = _mapping(diagnostic_payload.get("summary"))
        data_quality_status = _text(diagnostic_summary.get("overall_status"), "MISSING")
        backtest_mode = _text(diagnostic_summary.get("backtest_mode"), "MISSING")
        snapshot_summary = _signal_snapshot_review_summary(as_of, diagnostic_payload)
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "backtest_mode": backtest_mode,
            "promotion_eligibility": _parameter_shadow_promotion_eligibility(backtest_mode),
            "signal_snapshot_status": snapshot_summary.get("status", "MISSING"),
            "real_signals_count": snapshot_summary.get("real_signal_count", 0),
            "fallback_signals_count": snapshot_summary.get("fallback_signal_count", 0),
            "missing_signals_count": snapshot_summary.get("missing_signal_count", 0),
            "data_quality_status": data_quality_status,
            "data_quality_summary": _parameter_shadow_data_quality_sentence(
                data_quality_status=data_quality_status,
                promotion_status="UNKNOWN",
                diagnostic_summary=diagnostic_summary,
            ),
            "promotion_status": "UNKNOWN",
            "baseline_version": "UNKNOWN",
            "candidate_version": "UNKNOWN",
            "annualized_return_delta": "NA",
            "max_drawdown_delta": "NA",
            "sharpe_ratio_delta": "NA",
            "turnover_delta": "NA",
            "signal_ablation_status": ablation_summary.get("status", "MISSING"),
            "signal_ablation_summary": ablation_summary.get("summary_sentence", ""),
            "signal_ablation_promotion_credit_signals": ablation_summary.get(
                "promotion_credit_signals",
                [],
            ),
            "signal_ablation_negative_signals": ablation_summary.get("negative_signals", []),
            "signal_ablation_no_promotion_credit_reason": ablation_summary.get(
                "no_promotion_credit_reason",
                "",
            ),
            "signal_ablation_implementation_warnings": ablation_summary.get(
                "implementation_warnings",
                [],
            ),
            "signal_calibration_status": calibration_summary.get("status", "MISSING"),
            "signal_calibration_summary": calibration_summary.get("summary_sentence", ""),
            "signal_calibration_best_profile": calibration_summary.get("best_profile", ""),
            "signal_calibration_profiles_tested": calibration_summary.get(
                "profiles_tested",
                0,
            ),
            "signal_calibration_positive_signal_count": calibration_summary.get(
                "positive_signal_count",
                0,
            ),
            "signal_calibration_promotion_credit_signal_count": calibration_summary.get(
                "promotion_credit_signal_count",
                0,
            ),
            "signal_calibration_neutral_warning": calibration_summary.get(
                "neutral_warning",
                "",
            ),
            "signal_calibration_correlation_warning": calibration_summary.get(
                "correlation_warning",
                "",
            ),
            "portfolio_sensitivity_status": sensitivity_summary.get("status", "MISSING"),
            "portfolio_sensitivity_summary": sensitivity_summary.get("summary_sentence", ""),
            "portfolio_sensitivity_best_profile": sensitivity_summary.get("best_profile", ""),
            "portfolio_sensitivity_primary_bottleneck": sensitivity_summary.get(
                "primary_bottleneck",
                "",
            ),
            "portfolio_sensitivity_data_registry": sensitivity_summary.get(
                "data_registry_consistency",
                "MISSING",
            ),
            "portfolio_is_too_insensitive": sensitivity_summary.get(
                "portfolio_is_too_insensitive",
                False,
            ),
            "portfolio_candidates_status": candidates_summary.get("status", "MISSING"),
            "portfolio_candidates_summary": candidates_summary.get("summary_sentence", ""),
            "portfolio_candidates_best_profile": candidates_summary.get("best_profile", ""),
            "portfolio_candidates_profiles_tested": candidates_summary.get(
                "profiles_tested",
                0,
            ),
            "portfolio_candidates_guardrail_status": candidates_summary.get(
                "guardrail_status",
                "MISSING",
            ),
            "portfolio_candidates_promotion_eligibility": candidates_summary.get(
                "candidate_promotion_eligibility",
                False,
            ),
            "portfolio_candidate_review_status": candidate_review_summary.get(
                "status",
                "MISSING",
            ),
            "portfolio_candidate_review_summary": candidate_review_summary.get(
                "summary_sentence",
                "",
            ),
            "portfolio_candidate_review_profile": candidate_review_summary.get(
                "candidate_profile",
                "",
            ),
            "portfolio_candidate_review_reviewer": candidate_review_summary.get(
                "reviewer",
                "",
            ),
            "portfolio_candidate_review_next_step": candidate_review_summary.get(
                "allowed_next_step",
                "",
            ),
            "market_data_freshness_status": market_freshness_summary.get(
                "status",
                "MISSING",
            ),
            "market_data_freshness_summary": market_freshness_summary.get(
                "summary_sentence",
                "",
            ),
            "market_data_freshness_tracking_date": market_freshness_summary.get(
                "tracking_date",
                "",
            ),
            "market_data_freshness_effective_data_date": market_freshness_summary.get(
                "effective_data_date",
                "",
            ),
            "market_data_tracking_readiness": market_freshness_summary.get(
                "tracking_readiness",
                "unknown",
            ),
            "market_data_refresh_status": market_refresh_summary.get("status", "MISSING"),
            "market_data_refresh_summary": market_refresh_summary.get(
                "summary_sentence",
                "",
            ),
            "market_data_refresh_target_date": market_refresh_summary.get(
                "target_date",
                "",
            ),
            "portfolio_candidate_tracking_status": candidate_tracking_summary.get(
                "tracking_status",
                "MISSING",
            ),
            "portfolio_candidate_tracking_summary": candidate_tracking_summary.get(
                "summary_sentence",
                "",
            ),
            "portfolio_candidate_tracking_effective_data_date": (
                candidate_tracking_summary.get("effective_data_date", "")
            ),
            "portfolio_candidate_tracking_excess_return": candidate_tracking_summary.get(
                "excess_return",
                "",
            ),
            "portfolio_tracking_review_recommendation": tracking_review_summary.get(
                "recommendation",
                "MISSING",
            ),
            "portfolio_tracking_review_summary": tracking_review_summary.get(
                "summary_sentence",
                "",
            ),
            "portfolio_tracking_review_tracking_days": tracking_review_summary.get(
                "tracking_days",
                0,
            ),
            "portfolio_tracking_review_stage": tracking_review_summary.get(
                "stage",
                "MISSING",
            ),
            "portfolio_tracking_review_days_until_short_review": tracking_review_summary.get(
                "days_until_short_review",
                "",
            ),
            "portfolio_tracking_review_days_until_extended_review": tracking_review_summary.get(
                "days_until_extended_review",
                "",
            ),
            "portfolio_tracking_review_excess_return": tracking_review_summary.get(
                "excess_return",
                "",
            ),
            "weight_tuning_status": weight_tuning_summary.get("status", "MISSING"),
            "weight_tuning_summary": weight_tuning_summary.get("summary_sentence", ""),
            "weight_tuning_candidate_status": weight_tuning_summary.get(
                "candidate_status",
                "MISSING",
            ),
            "weight_tuning_candidates_evaluated": weight_tuning_summary.get(
                "candidates_evaluated",
                0,
            ),
            "weight_tuning_guardrail_status": weight_tuning_summary.get(
                "guardrail_status",
                "MISSING",
            ),
            "weight_tuning_non_worse_walk_forward_ratio": weight_tuning_summary.get(
                "non_worse_walk_forward_ratio",
                "",
            ),
            "weight_tuning_failure_status": weight_tuning_failure_summary.get(
                "status",
                "MISSING",
            ),
            "weight_tuning_failure_summary": weight_tuning_failure_summary.get(
                "summary_sentence",
                "",
            ),
            "weight_tuning_failure_root_cause": weight_tuning_failure_summary.get(
                "root_cause_category",
                "MISSING",
            ),
            "weight_tuning_failure_top_reason": weight_tuning_failure_summary.get(
                "top_failure_reason",
                "",
            ),
            "weight_tuning_failure_next_action": weight_tuning_failure_summary.get(
                "recommended_next_action",
                "",
            ),
            "weight_stability_status": weight_stability_summary.get("status", "MISSING"),
            "weight_stability_summary": weight_stability_summary.get("summary_sentence", ""),
            "weight_stability_candidate_status": weight_stability_summary.get(
                "candidate_status",
                "MISSING",
            ),
            "weight_stability_candidates_generated": weight_stability_summary.get(
                "candidates_generated",
                0,
            ),
            "weight_stability_rejected_by_stability": weight_stability_summary.get(
                "rejected_by_stability",
                0,
            ),
            "weight_stability_rejected_by_turnover_prefilter": weight_stability_summary.get(
                "rejected_by_turnover_prefilter",
                0,
            ),
            "weight_stability_turnover_failures_reduced": weight_stability_summary.get(
                "turnover_failures_reduced",
                False,
            ),
            "weight_stability_readiness_status": weight_stability_readiness_summary.get(
                "status",
                "MISSING",
            ),
            "weight_stability_readiness_summary": (
                weight_stability_readiness_summary.get("summary_sentence", "")
            ),
            "weight_stability_readiness_can_run": weight_stability_readiness_summary.get(
                "can_run",
                False,
            ),
            "weight_stability_readiness_blocking_checks": (
                weight_stability_readiness_summary.get("blocking_checks", [])
            ),
            "weight_stability_readiness_next_action": weight_stability_readiness_summary.get(
                "next_action",
                "",
            ),
            "portfolio_turnover_attribution_status": turnover_attribution_summary.get(
                "status",
                "MISSING",
            ),
            "portfolio_turnover_attribution_summary": turnover_attribution_summary.get(
                "summary_sentence",
                "",
            ),
            "portfolio_turnover_attribution_root_cause": turnover_attribution_summary.get(
                "root_cause_category",
                "MISSING",
            ),
            "portfolio_turnover_top_assets": turnover_attribution_summary.get(
                "top_turnover_assets",
                "",
            ),
            "portfolio_turnover_next_action": turnover_attribution_summary.get(
                "recommended_next_action",
                "",
            ),
            "manual_review_required": True,
            "risk": "Shadow parameter backtest artifact missing; Reader Brief does not run it.",
            "diagnostic_report": str(diagnostic_path) if diagnostic_path.exists() else "",
            "production_effect": PRODUCTION_EFFECT,
        }
    metadata = _mapping(payload.get("metadata"))
    comparison = _mapping(payload.get("relative_comparison"))
    decision = _mapping(payload.get("promotion_decision"))
    data_quality = _mapping(payload.get("data_quality"))
    status = _text(metadata.get("status"), "UNKNOWN")
    promotion_status = _text(decision.get("status"), "UNKNOWN")
    diagnostic_path_text = _text(data_quality.get("diagnostic_report"))
    diagnostic_payload = (
        _read_optional_json(Path(diagnostic_path_text)) if diagnostic_path_text else {}
    )
    diagnostic_summary = _mapping(diagnostic_payload.get("summary"))
    snapshot_summary = _signal_snapshot_review_summary(as_of, diagnostic_payload)
    data_quality_status = _text(data_quality.get("status"), "UNKNOWN")
    backtest_mode = _text(
        metadata.get("backtest_mode")
        or data_quality.get("backtest_mode")
        or diagnostic_summary.get("backtest_mode"),
        "UNKNOWN",
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "backtest_mode": backtest_mode,
        "promotion_eligibility": _parameter_shadow_promotion_eligibility(backtest_mode),
        "signal_snapshot_status": snapshot_summary.get("status", "MISSING"),
        "real_signals_count": snapshot_summary.get("real_signal_count", 0),
        "fallback_signals_count": snapshot_summary.get("fallback_signal_count", 0),
        "missing_signals_count": snapshot_summary.get("missing_signal_count", 0),
        "data_quality_status": data_quality_status,
        "data_quality_summary": _parameter_shadow_data_quality_sentence(
            data_quality_status=data_quality_status,
            promotion_status=promotion_status,
            diagnostic_summary=diagnostic_summary,
        ),
        "promotion_status": promotion_status,
        "baseline_version": _text(metadata.get("baseline_parameter_version"), "UNKNOWN"),
        "candidate_version": _text(metadata.get("candidate_parameter_version"), "UNKNOWN"),
        "annualized_return_delta": _format_number(
            comparison.get("annualized_return_delta"),
            digits=4,
        ),
        "max_drawdown_delta": _format_number(comparison.get("max_drawdown_delta"), digits=4),
        "sharpe_ratio_delta": _format_number(comparison.get("sharpe_ratio_delta"), digits=4),
        "turnover_delta": _format_number(comparison.get("turnover_delta"), digits=4),
        "signal_ablation_status": ablation_summary.get("status", "MISSING"),
        "signal_ablation_summary": ablation_summary.get("summary_sentence", ""),
        "signal_ablation_promotion_credit_signals": ablation_summary.get(
            "promotion_credit_signals",
            [],
        ),
        "signal_ablation_negative_signals": ablation_summary.get("negative_signals", []),
        "signal_ablation_no_promotion_credit_reason": ablation_summary.get(
            "no_promotion_credit_reason",
            "",
        ),
        "signal_ablation_implementation_warnings": ablation_summary.get(
            "implementation_warnings",
            [],
        ),
        "signal_calibration_status": calibration_summary.get("status", "MISSING"),
        "signal_calibration_summary": calibration_summary.get("summary_sentence", ""),
        "signal_calibration_best_profile": calibration_summary.get("best_profile", ""),
        "signal_calibration_profiles_tested": calibration_summary.get("profiles_tested", 0),
        "signal_calibration_positive_signal_count": calibration_summary.get(
            "positive_signal_count",
            0,
        ),
        "signal_calibration_promotion_credit_signal_count": calibration_summary.get(
            "promotion_credit_signal_count",
            0,
        ),
        "signal_calibration_neutral_warning": calibration_summary.get("neutral_warning", ""),
        "signal_calibration_correlation_warning": calibration_summary.get(
            "correlation_warning",
            "",
        ),
        "portfolio_sensitivity_status": sensitivity_summary.get("status", "MISSING"),
        "portfolio_sensitivity_summary": sensitivity_summary.get("summary_sentence", ""),
        "portfolio_sensitivity_best_profile": sensitivity_summary.get("best_profile", ""),
        "portfolio_sensitivity_primary_bottleneck": sensitivity_summary.get(
            "primary_bottleneck",
            "",
        ),
        "portfolio_sensitivity_data_registry": sensitivity_summary.get(
            "data_registry_consistency",
            "MISSING",
        ),
        "portfolio_is_too_insensitive": sensitivity_summary.get(
            "portfolio_is_too_insensitive",
            False,
        ),
        "portfolio_candidates_status": candidates_summary.get("status", "MISSING"),
        "portfolio_candidates_summary": candidates_summary.get("summary_sentence", ""),
        "portfolio_candidates_best_profile": candidates_summary.get("best_profile", ""),
        "portfolio_candidates_profiles_tested": candidates_summary.get("profiles_tested", 0),
        "portfolio_candidates_guardrail_status": candidates_summary.get(
            "guardrail_status",
            "MISSING",
        ),
        "portfolio_candidates_promotion_eligibility": candidates_summary.get(
            "candidate_promotion_eligibility",
            False,
        ),
        "portfolio_candidate_review_status": candidate_review_summary.get(
            "status",
            "MISSING",
        ),
        "portfolio_candidate_review_summary": candidate_review_summary.get(
            "summary_sentence",
            "",
        ),
        "portfolio_candidate_review_profile": candidate_review_summary.get(
            "candidate_profile",
            "",
        ),
        "portfolio_candidate_review_reviewer": candidate_review_summary.get("reviewer", ""),
        "portfolio_candidate_review_next_step": candidate_review_summary.get(
            "allowed_next_step",
            "",
        ),
        "market_data_freshness_status": market_freshness_summary.get("status", "MISSING"),
        "market_data_freshness_summary": market_freshness_summary.get("summary_sentence", ""),
        "market_data_freshness_tracking_date": market_freshness_summary.get(
            "tracking_date",
            "",
        ),
        "market_data_freshness_effective_data_date": market_freshness_summary.get(
            "effective_data_date",
            "",
        ),
        "market_data_tracking_readiness": market_freshness_summary.get(
            "tracking_readiness",
            "unknown",
        ),
        "market_data_refresh_status": market_refresh_summary.get("status", "MISSING"),
        "market_data_refresh_summary": market_refresh_summary.get("summary_sentence", ""),
        "market_data_refresh_target_date": market_refresh_summary.get("target_date", ""),
        "portfolio_candidate_tracking_status": candidate_tracking_summary.get(
            "tracking_status",
            "MISSING",
        ),
        "portfolio_candidate_tracking_summary": candidate_tracking_summary.get(
            "summary_sentence",
            "",
        ),
        "portfolio_candidate_tracking_effective_data_date": candidate_tracking_summary.get(
            "effective_data_date",
            "",
        ),
        "portfolio_candidate_tracking_excess_return": candidate_tracking_summary.get(
            "excess_return",
            "",
        ),
        "portfolio_tracking_review_recommendation": tracking_review_summary.get(
            "recommendation",
            "MISSING",
        ),
        "portfolio_tracking_review_summary": tracking_review_summary.get(
            "summary_sentence",
            "",
        ),
        "portfolio_tracking_review_tracking_days": tracking_review_summary.get(
            "tracking_days",
            0,
        ),
        "portfolio_tracking_review_stage": tracking_review_summary.get(
            "stage",
            "MISSING",
        ),
        "portfolio_tracking_review_days_until_short_review": tracking_review_summary.get(
            "days_until_short_review",
            "",
        ),
        "portfolio_tracking_review_days_until_extended_review": tracking_review_summary.get(
            "days_until_extended_review",
            "",
        ),
        "portfolio_tracking_review_excess_return": tracking_review_summary.get(
            "excess_return",
            "",
        ),
        "weight_tuning_status": weight_tuning_summary.get("status", "MISSING"),
        "weight_tuning_summary": weight_tuning_summary.get("summary_sentence", ""),
        "weight_tuning_candidate_status": weight_tuning_summary.get(
            "candidate_status",
            "MISSING",
        ),
        "weight_tuning_candidates_evaluated": weight_tuning_summary.get(
            "candidates_evaluated",
            0,
        ),
        "weight_tuning_guardrail_status": weight_tuning_summary.get(
            "guardrail_status",
            "MISSING",
        ),
        "weight_tuning_non_worse_walk_forward_ratio": weight_tuning_summary.get(
            "non_worse_walk_forward_ratio",
            "",
        ),
        "weight_tuning_failure_status": weight_tuning_failure_summary.get(
            "status",
            "MISSING",
        ),
        "weight_tuning_failure_summary": weight_tuning_failure_summary.get(
            "summary_sentence",
            "",
        ),
        "weight_tuning_failure_root_cause": weight_tuning_failure_summary.get(
            "root_cause_category",
            "MISSING",
        ),
        "weight_tuning_failure_top_reason": weight_tuning_failure_summary.get(
            "top_failure_reason",
            "",
        ),
        "weight_tuning_failure_next_action": weight_tuning_failure_summary.get(
            "recommended_next_action",
            "",
        ),
        "weight_stability_status": weight_stability_summary.get("status", "MISSING"),
        "weight_stability_summary": weight_stability_summary.get("summary_sentence", ""),
        "weight_stability_candidate_status": weight_stability_summary.get(
            "candidate_status",
            "MISSING",
        ),
        "weight_stability_candidates_generated": weight_stability_summary.get(
            "candidates_generated",
            0,
        ),
        "weight_stability_rejected_by_stability": weight_stability_summary.get(
            "rejected_by_stability",
            0,
        ),
        "weight_stability_rejected_by_turnover_prefilter": weight_stability_summary.get(
            "rejected_by_turnover_prefilter",
            0,
        ),
        "weight_stability_turnover_failures_reduced": weight_stability_summary.get(
            "turnover_failures_reduced",
            False,
        ),
        "weight_stability_readiness_status": weight_stability_readiness_summary.get(
            "status",
            "MISSING",
        ),
        "weight_stability_readiness_summary": weight_stability_readiness_summary.get(
            "summary_sentence",
            "",
        ),
        "weight_stability_readiness_can_run": weight_stability_readiness_summary.get(
            "can_run",
            False,
        ),
        "weight_stability_readiness_blocking_checks": weight_stability_readiness_summary.get(
            "blocking_checks",
            [],
        ),
        "weight_stability_readiness_next_action": weight_stability_readiness_summary.get(
            "next_action",
            "",
        ),
        "portfolio_turnover_attribution_status": turnover_attribution_summary.get(
            "status",
            "MISSING",
        ),
        "portfolio_turnover_attribution_summary": turnover_attribution_summary.get(
            "summary_sentence",
            "",
        ),
        "portfolio_turnover_attribution_root_cause": turnover_attribution_summary.get(
            "root_cause_category",
            "MISSING",
        ),
        "portfolio_turnover_top_assets": turnover_attribution_summary.get(
            "top_turnover_assets",
            "",
        ),
        "portfolio_turnover_next_action": turnover_attribution_summary.get(
            "recommended_next_action",
            "",
        ),
        "manual_review_required": metadata.get("manual_review_required") is True,
        "risk": _text(decision.get("reason"), "Open shadow backtest report before review."),
        "diagnostic_report": diagnostic_path_text,
        "source_artifact": str(path),
        "production_effect": PRODUCTION_EFFECT,
    }


def _default_backtest_input_diagnostic_path(as_of: date) -> Path:
    return (
        PROJECT_ROOT
        / "artifacts"
        / "data_quality"
        / as_of.isoformat()
        / "backtest_input_diagnostics.json"
    )


def _signal_snapshot_review_summary(
    as_of: date,
    diagnostic_payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks = _mapping(diagnostic_payload.get("checks"))
    signal_snapshots = _mapping(checks.get("signal_snapshots"))
    snapshot_files = [
        Path(path)
        for path in _texts(signal_snapshots.get("snapshot_files"))
        if path.endswith("signal_snapshot.json")
    ]
    default_path = (
        PROJECT_ROOT / "artifacts" / "signal_snapshots" / as_of.isoformat() / "signal_snapshot.json"
    )
    path = next((candidate for candidate in snapshot_files if candidate.exists()), default_path)
    payload = load_signal_snapshot_payload(path)
    if payload:
        return signal_snapshot_summary(payload)
    return {
        "status": _text(signal_snapshots.get("status"), "MISSING"),
        "real_signal_count": len(_texts(signal_snapshots.get("real_signals"))),
        "fallback_signal_count": len(_texts(signal_snapshots.get("neutral_fallback_signals"))),
        "missing_signal_count": len(_texts(signal_snapshots.get("missing_signals"))),
    }


def _signal_ablation_review_summary(as_of: date) -> dict[str, Any]:
    path = (
        PROJECT_ROOT
        / "artifacts"
        / "signal_ablation"
        / as_of.isoformat()
        / "signal_ablation_summary.json"
    )
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "promotion_credit_signals": [],
            "negative_signals": [],
            "no_promotion_credit_reason": "",
            "implementation_warnings": [],
            "summary_sentence": (
                "Signal ablation summary is missing; Reader Brief does not run ablation."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    diagnostics = _mapping(payload.get("diagnostics"))
    promotion_credit = _texts(summary.get("promotion_credit_signals"))
    negative = _texts(summary.get("negative_signals"))
    fallback = _texts(summary.get("fallback_signals"))
    implementation_warnings = _texts(diagnostics.get("implementation_warnings"))
    no_credit_reason = _text(summary.get("no_promotion_credit_reason"))
    status = _text(metadata.get("status"), "UNKNOWN")
    if implementation_warnings:
        sentence = (
            "Signal ablation detected an implementation warning: "
            f"{implementation_warnings[0]}. The result should not be used for parameter "
            "review until fixed."
        )
    elif negative:
        sentence = (
            "Signal ablation detected potential negative contribution from "
            f"{_format_english_list(negative)}. This should be reviewed before expanding "
            "promotion eligibility."
        )
    else:
        sentence = (
            f"Signal ablation remains {status}. "
            f"Promotion-credit-eligible signals: "
            f"{_format_english_list(promotion_credit) or 'none'}. "
            f"{no_credit_reason or 'Real signal contribution remains below promotion credit.'} "
            f"Fallback signals: {_format_english_list(fallback) or 'none'}; "
            "candidate promotion remains disabled."
        )
    return {
        "status": status,
        "source_artifact": str(path),
        "promotion_credit_signals": promotion_credit,
        "negative_signals": negative,
        "fallback_signals": fallback,
        "no_promotion_credit_reason": no_credit_reason,
        "implementation_warnings": implementation_warnings,
        "all_real_signals_used_in_score": diagnostics.get(
            "all_real_signals_used_in_score",
            False,
        ),
        "summary_sentence": sentence,
    }


def _tail_risk_daily_reading_safety_summary() -> dict[str, Any]:
    path = (
        PROJECT_ROOT
        / "outputs"
        / "research_strategies"
        / "value_surface_review"
        / "tail_risk_daily_reading_safety_summary.json"
    )
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "research_status": "MISSING",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "current_blockers": [],
            "current_blocker_count": 0,
            "latest_master_review_status": "MISSING",
            "source_artifact": str(path),
            "production_effect": PRODUCTION_EFFECT,
            "summary_sentence": (
                "Tail-risk fallback safety summary is missing; Reader Brief does not run "
                "tail-risk governance CLIs."
            ),
        }
    status = _mapping(payload.get("tail_risk_fallback_status"))
    blockers = _records(status.get("current_blockers"))
    research_status = _text(status.get("research_status"), "UNKNOWN")
    return {
        "status": _text(payload.get("status"), "UNKNOWN"),
        "research_status": research_status,
        "promotion_allowed": bool(status.get("promotion_allowed")),
        "paper_shadow_allowed": bool(status.get("paper_shadow_allowed")),
        "production_allowed": bool(status.get("production_allowed")),
        "broker_action": _text(payload.get("broker_action"), "none"),
        "current_blockers": blockers,
        "current_blocker_count": len(blockers),
        "latest_master_review_status": _text(status.get("latest_master_review_status"), "UNKNOWN"),
        "source_artifact": str(path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"Tail-risk fallback research_status={research_status}; "
            "promotion/paper-shadow/production remain disabled; broker_action=none."
        ),
    }


def _portfolio_control_research_summary() -> dict[str, Any]:
    path = (
        PROJECT_ROOT
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_portfolio_control_safety_summary.json"
    )
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "top_simple_baseline_candidate": "MISSING",
            "research_only_target_weights": {},
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "major_blockers": [],
            "major_blocker_count": 0,
            "source_artifact": str(path),
            "production_effect": PRODUCTION_EFFECT,
            "summary_sentence": (
                "Portfolio control research summary is missing; Reader Brief does not run "
                "simple-baseline research CLIs."
            ),
        }
    status = _mapping(payload.get("portfolio_control_research_status"))
    blockers = _records(status.get("major_blockers"))
    candidate = _text(status.get("top_simple_baseline_candidate"), "UNKNOWN")
    return {
        "status": _text(payload.get("status"), "UNKNOWN"),
        "top_simple_baseline_candidate": candidate,
        "research_only_target_weights": _mapping(
            status.get("current_research_only_target_weights")
        ),
        "promotion_allowed": bool(status.get("promotion_allowed")),
        "paper_shadow_allowed": bool(status.get("paper_shadow_allowed")),
        "production_allowed": bool(status.get("production_allowed")),
        "broker_action": _text(status.get("broker_action"), "none"),
        "major_blockers": blockers,
        "major_blocker_count": len(blockers),
        "source_artifact": str(path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"Portfolio control research candidate={candidate}; research-only / observe-only; "
            "promotion/paper-shadow/production remain disabled; broker_action=none."
        ),
    }


def _portfolio_control_forward_aging_summary() -> dict[str, Any]:
    path = (
        PROJECT_ROOT
        / "outputs"
        / "research_strategies"
        / "simple_baselines"
        / "daily_reader_forward_aging_summary.json"
    )
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "primary_candidate": "MISSING",
            "challenger_candidate": "MISSING",
            "latest_observation_date": "MISSING",
            "matured_20d_count": 0,
            "matured_60d_count": 0,
            "matured_120d_count": 0,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "source_artifact": str(path),
            "production_effect": PRODUCTION_EFFECT,
            "summary_sentence": (
                "Portfolio control forward-aging summary is missing; Reader Brief does not "
                "run simple-baseline research CLIs."
            ),
        }
    status = _mapping(payload.get("portfolio_control_forward_aging"))
    primary = _text(status.get("primary_candidate"), "UNKNOWN")
    return {
        "status": _text(payload.get("status"), "UNKNOWN"),
        "primary_candidate": primary,
        "challenger_candidate": _text(status.get("challenger_candidate"), "UNKNOWN"),
        "latest_observation_date": _text(status.get("latest_observation_date"), "UNKNOWN"),
        "matured_20d_count": _int(status.get("matured_20d_count")),
        "matured_60d_count": _int(status.get("matured_60d_count")),
        "matured_120d_count": _int(status.get("matured_120d_count")),
        "paper_shadow_allowed": bool(status.get("paper_shadow_allowed")),
        "production_allowed": bool(status.get("production_allowed")),
        "broker_action": _text(status.get("broker_action"), "none"),
        "source_artifact": str(path),
        "production_effect": _text(payload.get("production_effect"), PRODUCTION_EFFECT),
        "summary_sentence": (
            f"Portfolio control forward aging primary={primary}; no paper-shadow, "
            "production, or broker action is allowed."
        ),
    }


def _signal_calibration_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_signal_calibration_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "best_profile": "",
            "profiles_tested": 0,
            "positive_signal_count": 0,
            "promotion_credit_signal_count": 0,
            "neutral_warning": "",
            "correlation_warning": "",
            "summary_sentence": (
                "Signal calibration summary is missing; Reader Brief does not run calibration."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "best_profile": "",
            "profiles_tested": 0,
            "positive_signal_count": 0,
            "promotion_credit_signal_count": 0,
            "neutral_warning": "",
            "correlation_warning": "",
            "summary_sentence": (
                "Signal calibration summary is unreadable; Reader Brief does not run calibration."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    ranking = _mapping(payload.get("ranking"))
    profiles = _records(payload.get("profiles"))
    best_profile = _text(ranking.get("best_profile"))
    best = next(
        (item for item in profiles if _text(item.get("profile_name")) == best_profile),
        {},
    )
    ablation = _mapping(best.get("ablation"))
    distribution = _mapping(best.get("signal_distribution"))
    correlation = _mapping(best.get("signal_correlation"))
    neutral_warnings = [
        _text(item.get("warning"))
        for item in distribution.values()
        if isinstance(item, dict) and _text(item.get("warning"))
    ]
    correlation_warning = _text(correlation.get("warning"))
    positive_count = _int(ablation.get("positive_signals"))
    promotion_credit_count = _int(ablation.get("promotion_credit_signals"))
    status = _text(metadata.get("status"), "UNKNOWN")
    if positive_count > 0:
        sentence = (
            "Signal calibration tested multiple trend and sector profiles. "
            f"Best profile `{best_profile}` produced {positive_count} positive real-signal "
            "contribution(s), but signal quality remains LIMITED and candidate promotion "
            "remains disabled."
        )
    elif neutral_warnings or correlation_warning:
        sentence = (
            "Signal calibration tested multiple trend and sector profiles. "
            f"Best profile `{best_profile}` still shows neutral compression or signal "
            "correlation risk; candidate promotion remains disabled."
        )
    else:
        sentence = (
            "Signal calibration did not find a profile with material contribution above "
            "threshold. Trend/sector formulas may need stronger feature design or portfolio "
            "sensitivity diagnostics."
        )
    return {
        "status": status,
        "source_artifact": str(path),
        "best_profile": best_profile,
        "profiles_tested": len(profiles),
        "positive_signal_count": positive_count,
        "promotion_credit_signal_count": promotion_credit_count,
        "neutral_warning": neutral_warnings[0] if neutral_warnings else "",
        "correlation_warning": correlation_warning,
        "summary_sentence": sentence,
    }


def _portfolio_sensitivity_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_sensitivity_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "best_profile": "",
            "primary_bottleneck": "",
            "portfolio_is_too_insensitive": False,
            "summary_sentence": (
                "Portfolio sensitivity summary is missing; Reader Brief does not run "
                "sensitivity diagnostics."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "best_profile": "",
            "primary_bottleneck": "",
            "portfolio_is_too_insensitive": False,
            "summary_sentence": (
                "Portfolio sensitivity summary is unreadable; Reader Brief does not run "
                "sensitivity diagnostics."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    ranking = _mapping(payload.get("ranking"))
    diagnosis = _mapping(payload.get("diagnosis"))
    promotion = _mapping(payload.get("promotion_impact"))
    data_gate = _mapping(payload.get("data_gate"))
    status = _text(metadata.get("status"), "UNKNOWN")
    best_profile = _text(ranking.get("best_profile"))
    primary = _text(diagnosis.get("primary_bottleneck"), "UNKNOWN")
    too_insensitive = diagnosis.get("portfolio_is_too_insensitive") is True
    can_promote = promotion.get("can_support_candidate_promotion") is True
    data_registry_status = _text(data_gate.get("data_registry_consistency"), "UNKNOWN")
    reconcile = _price_cache_reconcile_review_summary(as_of)
    if data_gate.get("status") == "FAILED":
        reason = _text(
            data_gate.get("reason"),
            (
                "repaired price history exists but validate-data does not currently resolve it "
                "as the primary price source"
            ),
        )
        sentence = (
            "Portfolio sensitivity remains blocked by a data registry inconsistency: "
            f"{reason}. A cache/manifest reconciliation is required before sensitivity "
            "results can be interpreted."
        )
        if reconcile.get("status") == "FAILED":
            sentence = _text(reconcile.get("summary_sentence"), sentence)
    elif too_insensitive:
        sentence = (
            "Portfolio sensitivity diagnostics suggest the current portfolio construction "
            f"is too insensitive to signal changes. Best profile `{best_profile}` points to "
            f"`{primary}`, but signal quality remains LIMITED, so candidate promotion remains "
            "disabled."
        )
    elif primary and primary != "none":
        sentence = (
            "Portfolio sensitivity diagnostics found a possible transmission bottleneck at "
            f"`{primary}`. The result is advisory only and candidate promotion remains disabled."
        )
    else:
        sentence = (
            "Portfolio sensitivity diagnostics did not find a major score-to-weight "
            "transmission bottleneck. The remaining issue is likely signal quality rather "
            "than portfolio construction."
        )
    if data_gate.get("status") == "OK" and data_registry_status in {"OK", "LIMITED"}:
        sentence = "Portfolio sensitivity data registry is consistent. " + sentence
        if reconcile.get("status") in {"OK", "LIMITED"}:
            sentence = _text(reconcile.get("summary_sentence"), "") + " " + sentence
    if can_promote:
        sentence += " Safety warning: sensitivity artifact unexpectedly supports promotion."
    return {
        "status": status,
        "source_artifact": str(path),
        "best_profile": best_profile,
        "primary_bottleneck": primary,
        "data_registry_consistency": data_registry_status,
        "portfolio_is_too_insensitive": too_insensitive,
        "summary_sentence": sentence,
    }


def _portfolio_candidates_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_candidates_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "best_profile": "",
            "profiles_tested": 0,
            "guardrail_status": "MISSING",
            "candidate_promotion_eligibility": False,
            "summary_sentence": (
                "Portfolio candidate evaluation is missing; Reader Brief does not run "
                "candidate profile evaluation."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "best_profile": "",
            "profiles_tested": 0,
            "guardrail_status": "MISSING",
            "candidate_promotion_eligibility": False,
            "summary_sentence": (
                "Portfolio candidate evaluation is unreadable; Reader Brief does not run "
                "candidate profile evaluation."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    ranking = _mapping(payload.get("ranking"))
    baseline = _mapping(payload.get("baseline"))
    promotion = _mapping(payload.get("promotion_impact"))
    profiles = _records(payload.get("profiles"))
    best_profile = _text(ranking.get("best_profile"))
    best = next(
        (item for item in profiles if _text(item.get("profile_name")) == best_profile),
        {},
    )
    best_guardrail = _mapping(best.get("risk_guardrails"))
    best_transmission = _mapping(best.get("signal_transmission"))
    baseline_transmission = _mapping(baseline.get("signal_transmission"))
    transmission_delta = (
        _float_or_none(best_transmission.get("target_to_actual_weight_effectiveness")) or 0.0
    ) - (_float_or_none(baseline_transmission.get("target_to_actual_weight_effectiveness")) or 0.0)
    turnover_impact = _float_or_none(best_guardrail.get("turnover_relative_increase")) or 0.0
    status = _text(metadata.get("status"), "UNKNOWN")
    guardrail_status = _text(best_guardrail.get("guardrail_status"), "UNKNOWN")
    can_promote = promotion.get("can_support_candidate_promotion") is True
    if best_profile and best_profile != _text(baseline.get("profile_name")):
        if guardrail_status == "PASS":
            sentence = (
                "Portfolio candidate evaluation found a candidate profile with improved "
                f"signal-to-weight transmission. Best profile `{best_profile}` has "
                f"turnover impact {_format_number(turnover_impact, digits=4)} and guardrail "
                f"status `{guardrail_status}`, but recommendation remains advisory only."
            )
        else:
            sentence = (
                "Portfolio candidate evaluation found transmission changes, but the best "
                f"profile `{best_profile}` has guardrail status `{guardrail_status}`. "
                "Manual review is required and candidate promotion remains disabled."
            )
    else:
        sentence = (
            "Portfolio candidate evaluation did not find a safe improvement profile. "
            "Lower thresholds or stronger mappings may increase responsiveness, but turnover, "
            "drawdown, or guardrail checks must dominate single-period return."
        )
    if transmission_delta > 0.0 and guardrail_status == "PASS":
        sentence = (
            "Portfolio candidate evaluation found that a moderately more responsive "
            "construction can improve signal-to-weight transmission without breaching "
            "drawdown or turnover guardrails. " + sentence
        )
    if can_promote:
        sentence += " Safety warning: candidate artifact unexpectedly supports promotion."
    return {
        "status": status,
        "source_artifact": str(path),
        "best_profile": best_profile,
        "profiles_tested": len(profiles),
        "guardrail_status": guardrail_status,
        "candidate_promotion_eligibility": can_promote,
        "summary_sentence": sentence,
    }


def _portfolio_candidate_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_candidate_review_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "candidate_profile": "",
            "reviewer": "",
            "allowed_next_step": "",
            "summary_sentence": (
                "Portfolio candidate review is missing; Reader Brief does not create "
                "manual review decisions."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "candidate_profile": "",
            "reviewer": "",
            "allowed_next_step": "",
            "summary_sentence": (
                "Portfolio candidate review is unreadable; Reader Brief does not create "
                "manual review decisions."
            ),
        }
    decision = _mapping(payload.get("decision"))
    candidate = _mapping(payload.get("candidate"))
    evidence = _mapping(payload.get("evidence_summary"))
    status = _text(decision.get("status"), "UNKNOWN")
    profile = _text(candidate.get("profile_name"))
    reviewer = _text(decision.get("reviewer"))
    next_step = _text(decision.get("allowed_next_step"))
    signal_quality = _text(evidence.get("signal_snapshot_status"), "UNKNOWN")
    if status == "pending_review":
        sentence = (
            f"Portfolio candidate review is pending for `{profile}`. The candidate remains "
            f"advisory only because signal quality is `{signal_quality}` and production "
            "parameters are unchanged."
        )
    elif status == "watch":
        sentence = (
            "The portfolio candidate is under watch after manual review. It will continue "
            "to be tracked in shadow mode, but production promotion remains disabled."
        )
    elif status == "approved_for_shadow_candidate":
        sentence = (
            "The portfolio candidate has been manually approved for shadow tracking only. "
            "Production promotion remains disabled until signal quality improves and a "
            "separate promotion gate is passed."
        )
    elif status == "needs_more_data":
        sentence = (
            "Portfolio candidate review requires more data before shadow tracking approval. "
            "Production parameters remain unchanged."
        )
    elif status == "rejected":
        sentence = (
            "Portfolio candidate review rejected the recommended profile. Production "
            "parameters remain unchanged."
        )
    else:
        sentence = (
            "Portfolio candidate review status is unavailable; production parameters remain "
            "unchanged."
        )
    return {
        "status": status,
        "source_artifact": str(path),
        "candidate_profile": profile,
        "reviewer": reviewer,
        "allowed_next_step": next_step,
        "summary_sentence": sentence,
    }


def _portfolio_candidate_tracking_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_candidate_tracking_path(as_of)
    if path is None:
        return {
            "tracking_status": "MISSING",
            "candidate_profile": "",
            "effective_data_date": "",
            "excess_return": "",
            "summary_sentence": (
                "Portfolio candidate tracking is missing; Reader Brief does not start "
                "shadow tracking."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "tracking_status": "MISSING",
            "candidate_profile": "",
            "effective_data_date": "",
            "excess_return": "",
            "summary_sentence": (
                "Portfolio candidate tracking is unreadable; production parameters remain "
                "unchanged."
            ),
        }
    candidate = _mapping(payload.get("candidate"))
    date_resolution = _mapping(payload.get("date_resolution"))
    metrics = _mapping(payload.get("tracking_metrics"))
    candidate_metrics = _mapping(metrics.get("candidate"))
    tracking_status = _text(candidate.get("tracking_status"), "UNKNOWN")
    profile = _text(candidate.get("profile_name"))
    effective_data_date = _text(date_resolution.get("effective_data_date"))
    tracking_date = _text(date_resolution.get("tracking_date"))
    excess_return = candidate_metrics.get("excess_return_vs_baseline", "")
    if tracking_status == "active_tracking":
        sentence = (
            f"Portfolio candidate `{profile}` is actively tracked in shadow mode. "
            "Candidate performance is compared with the current baseline while "
            "production parameters remain unchanged."
        )
    elif tracking_status == "degraded_tracking":
        sentence = (
            f"Portfolio candidate `{profile}` is under shadow tracking, but latest "
            f"tracking is degraded because effective data remains on {effective_data_date} "
            f"while the run date is {tracking_date}. Tracking is advisory only."
        )
    elif tracking_status == "tracking_blocked":
        sentence = (
            f"Portfolio candidate `{profile}` tracking is blocked; production parameters "
            "remain unchanged and promotion remains disabled."
        )
    else:
        sentence = (
            f"Portfolio candidate `{profile}` tracking status is `{tracking_status}`; "
            "production parameters remain unchanged."
        )
    return {
        "tracking_status": tracking_status,
        "candidate_profile": profile,
        "effective_data_date": effective_data_date,
        "excess_return": excess_return,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _portfolio_tracking_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_tracking_review_path(as_of)
    if path is None:
        return {
            "recommendation": "MISSING",
            "tracking_days": 0,
            "excess_return": "",
            "summary_sentence": (
                "Portfolio tracking review is missing; Reader Brief does not run "
                "candidate performance review."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "recommendation": "MISSING",
            "tracking_days": 0,
            "excess_return": "",
            "summary_sentence": (
                "Portfolio tracking review is unreadable; production parameters remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    tracking_window = _mapping(payload.get("tracking_window"))
    recommendation = _mapping(payload.get("recommendation"))
    performance = _mapping(payload.get("performance_review"))
    relative = _mapping(performance.get("relative_performance"))
    status = _text(metadata.get("status"), "UNKNOWN")
    rec_status = _text(recommendation.get("status"), "UNKNOWN")
    profile = _text(candidate.get("profile_name"))
    tracking_days = tracking_window.get("tracking_days", candidate.get("tracking_days", 0))
    stage = _text(tracking_window.get("stage"), "UNKNOWN")
    min_short = tracking_window.get("min_days_for_short_review", 5)
    min_extended = tracking_window.get("min_days_for_extended_review", 20)
    days_until_short = tracking_window.get("days_until_short_review", "")
    days_until_extended = tracking_window.get("days_until_extended_review", "")
    excess_return = relative.get("excess_return", "")
    if rec_status == "needs_more_data":
        sentence = (
            "Portfolio tracking review remains in needs-more-data status. "
            f"Only {tracking_days} tracking {_day_label(tracking_days)} "
            f"{_is_are(tracking_days)} available for the `{profile}` candidate; "
            f"at least {min_short} valid tracking days are required before a "
            "short-window conclusion can be formed. "
            f"Days until short-window review: {days_until_short}."
        )
    elif rec_status == "eligible_for_extended_review":
        sentence = (
            "Portfolio tracking review has reached the extended review window. The "
            "candidate may be considered for extended manual review, but production "
            "promotion remains disabled unless a separate promotion gate is passed."
        )
    elif rec_status == "continue_tracking":
        if stage == "short_window_review":
            sentence = (
                "Portfolio tracking review has entered short-window review. Candidate "
                "performance can now be compared against baseline, but extended review "
                f"still requires {min_extended} valid tracking days; "
                f"{days_until_extended} day(s) remain."
            )
        else:
            sentence = (
                f"Portfolio tracking review recommends continuing shadow tracking for "
                f"`{profile}`. Data readiness and guardrails are acceptable, but "
                "production promotion remains disabled."
            )
    elif rec_status == "retire_candidate":
        sentence = (
            f"Portfolio tracking review recommends retiring `{profile}` because it is weaker "
            "than baseline or breached guardrails. Production parameters remain unchanged."
        )
    elif rec_status == "pause_tracking":
        sentence = (
            f"Portfolio tracking review recommends pausing `{profile}` tracking until the "
            "blocking data, freshness, or guardrail issue is resolved."
        )
    else:
        sentence = (
            f"Portfolio tracking review status is `{status}` with recommendation "
            f"`{rec_status}`. Production parameters remain unchanged."
        )
    return {
        "status": status,
        "recommendation": rec_status,
        "tracking_days": tracking_days,
        "stage": stage,
        "days_until_short_review": days_until_short,
        "days_until_extended_review": days_until_extended,
        "excess_return": excess_return,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _weight_tuning_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_weight_tuning_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "candidate_status": "MISSING",
            "candidates_evaluated": 0,
            "guardrail_status": "MISSING",
            "non_worse_walk_forward_ratio": "",
            "summary_sentence": (
                "Restricted backtest weight tuning is missing; Reader Brief does not run "
                "signal weight tuning."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "candidate_status": "MISSING",
            "candidates_evaluated": 0,
            "guardrail_status": "MISSING",
            "non_worse_walk_forward_ratio": "",
            "summary_sentence": (
                "Restricted backtest weight tuning is unreadable; production parameters "
                "remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    search = _mapping(payload.get("search"))
    recommended = _mapping(payload.get("recommended_candidate"))
    guardrails = _mapping(recommended.get("guardrails"))
    relative = _mapping(recommended.get("relative_metrics"))
    status = _text(metadata.get("status"), "UNKNOWN")
    candidate_status = _text(recommended.get("status"), "UNKNOWN")
    candidates_evaluated = search.get("candidates_evaluated", 0)
    guardrail_status = _text(guardrails.get("status"), "UNKNOWN")
    non_worse_ratio = relative.get("non_worse_walk_forward_ratio", "")
    if candidate_status in {"watch", "shadow_candidate_only"}:
        sentence = (
            "Restricted backtest weight tuning produced a shadow-only candidate that "
            "improves selected walk-forward metrics versus the current baseline. The "
            "candidate remains advisory because signal quality is LIMITED and fallback "
            "signals are not eligible for production tuning."
        )
    elif status == "NO_CANDIDATE" or candidate_status == "rejected":
        sentence = (
            "Restricted backtest weight tuning did not find a candidate that passed "
            "guardrails. The current baseline remains the reference configuration."
        )
    elif status == "INSUFFICIENT_DATA" or candidate_status in {
        "needs_more_data",
        "insufficient_data",
    }:
        sentence = (
            "Restricted backtest weight tuning could not run because data readiness or "
            "signal snapshot requirements were not met."
        )
    else:
        sentence = (
            f"Restricted backtest weight tuning status is `{status}` with candidate "
            f"`{candidate_status}`. Production parameters remain unchanged."
        )
    return {
        "status": status,
        "candidate_status": candidate_status,
        "candidates_evaluated": candidates_evaluated,
        "guardrail_status": guardrail_status,
        "non_worse_walk_forward_ratio": non_worse_ratio,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _weight_tuning_failure_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_weight_tuning_failure_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "root_cause_category": "MISSING",
            "top_failure_reason": "",
            "recommended_next_action": "",
            "summary_sentence": (
                "Weight tuning failure attribution is missing; Reader Brief cannot explain "
                "the latest NO_CANDIDATE result."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "root_cause_category": "MISSING",
            "top_failure_reason": "",
            "recommended_next_action": "",
            "summary_sentence": (
                "Weight tuning failure attribution is unreadable; production parameters "
                "remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    root_cause = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    ranking = _records(payload.get("failure_ranking"))
    category = _text(root_cause.get("category"), "mixed")
    top_reason = _text(ranking[0].get("reason"), "") if ranking else ""
    action = _text(next_action.get("action"), "")
    if metadata.get("status") == "BLOCKED":
        sentence = (
            "Weight tuning returned NO_CANDIDATE, but failure attribution is blocked "
            "because required tuning artifacts are missing."
        )
    elif category == "portfolio_turnover_too_high":
        sentence = (
            "Restricted weight tuning found near-miss candidates, but most failed "
            "turnover guardrails. The next step is to review portfolio construction "
            "turnover attribution."
        )
    elif category == "drawdown_control_insufficient":
        sentence = (
            "Restricted weight tuning improved some metrics but failed drawdown "
            "guardrails. Risk and valuation signals should be improved before another "
            "tuning attempt."
        )
    elif category == "no_alpha_detected":
        sentence = (
            "Restricted weight tuning did not find evidence that current signals improve "
            "risk-adjusted returns versus baseline."
        )
    elif category == "walk_forward_unstable":
        sentence = (
            "Restricted weight tuning did not produce a valid shadow candidate because "
            "candidate improvements were unstable across walk-forward windows."
        )
    elif category == "search_space_too_narrow":
        sentence = (
            "Restricted weight tuning did not have enough valid candidates after "
            "constraints; search space expansion should be reviewed without lowering "
            "guardrails automatically."
        )
    else:
        sentence = (
            "Restricted weight tuning did not produce a valid shadow candidate. Failure "
            f"attribution indicates the main blocker is {category}; production parameters "
            "remain unchanged."
        )
    return {
        "status": _text(metadata.get("status"), "UNKNOWN"),
        "root_cause_category": category,
        "top_failure_reason": top_reason,
        "recommended_next_action": action,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _weight_stability_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_weight_stability_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "candidate_status": "MISSING",
            "candidates_generated": 0,
            "rejected_by_stability": 0,
            "rejected_by_turnover_prefilter": 0,
            "turnover_failures_reduced": False,
            "summary_sentence": (
                "Weight search stability artifact is missing; Reader Brief does not run "
                "stable weight tuning."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "candidate_status": "MISSING",
            "candidates_generated": 0,
            "rejected_by_stability": 0,
            "rejected_by_turnover_prefilter": 0,
            "turnover_failures_reduced": False,
            "summary_sentence": (
                "Weight search stability artifact is unreadable; production parameters "
                "remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    search = _mapping(payload.get("search_summary"))
    recommended = _mapping(payload.get("recommended_candidate"))
    comparison = _mapping(payload.get("comparison_to_trading_059"))
    status = _text(metadata.get("status"), "UNKNOWN")
    candidate_status = _text(recommended.get("status"), "UNKNOWN")
    if status in {"INSUFFICIENT_DATA", "FAILED"}:
        sentence = (
            "Stable weight tuning could not run because data readiness or signal snapshot "
            "requirements were not met."
        )
    elif candidate_status in {"watch", "shadow_candidate_only"}:
        sentence = (
            "Stable weight tuning found a shadow-only candidate after adding L1 distance "
            "and turnover-aware constraints. Production promotion remains disabled "
            "because signal quality is LIMITED."
        )
    elif candidate_status == "no_candidate":
        sentence = (
            "Stable weight tuning reduced aggressive candidates but still did not find a "
            "guardrail-passing weight candidate. This suggests current real signals may "
            "not provide enough stable improvement over baseline."
        )
    else:
        sentence = (
            f"Stable weight tuning status is `{status}` with candidate "
            f"`{candidate_status}`. Production parameters remain unchanged."
        )
    return {
        "status": status,
        "candidate_status": candidate_status,
        "candidates_generated": search.get("candidates_generated", 0),
        "rejected_by_stability": search.get("candidates_rejected_by_stability", 0),
        "rejected_by_turnover_prefilter": search.get(
            "candidates_rejected_by_turnover_prefilter",
            0,
        ),
        "turnover_failures_reduced": comparison.get("turnover_failures_reduced", False),
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _weight_stability_readiness_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_weight_stability_readiness_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "can_run": False,
            "blocking_checks": [],
            "next_action": "aits parameters diagnose-weight-stability-inputs --latest",
            "summary_sentence": (
                "Stable weight tuning readiness artifact is missing; Reader Brief does "
                "not run readiness diagnostics."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "can_run": False,
            "blocking_checks": [],
            "next_action": "inspect readiness JSON",
            "summary_sentence": (
                "Stable weight tuning readiness artifact is unreadable; production "
                "parameters remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    eligibility = _mapping(payload.get("stable_tuning_eligibility"))
    recovery_plan = _records(payload.get("recovery_plan"))
    status = _text(metadata.get("status") or eligibility.get("status"), "UNKNOWN")
    can_run = eligibility.get("can_run") is True
    blocking_checks = [
        _text(item, "") for item in eligibility.get("blocking_checks", []) if _text(item, "")
    ]
    next_action = ""
    if recovery_plan:
        first_step = recovery_plan[0]
        next_action = _text(first_step.get("command") or first_step.get("action"), "")
    summary_sentence = _text(payload.get("reader_brief"), "")
    if not summary_sentence:
        if can_run:
            summary_sentence = (
                "Stable weight tuning input readiness is restored; the next run can "
                "enter candidate backtesting while promotion remains disabled."
            )
        else:
            summary_sentence = (
                "Stable weight tuning remains blocked before backtest; inspect readiness "
                "blockers before interpreting TRADING-061."
            )
    return {
        "status": status,
        "can_run": can_run,
        "blocking_checks": blocking_checks,
        "next_action": next_action,
        "source_artifact": str(path),
        "summary_sentence": summary_sentence,
    }


def _portfolio_turnover_attribution_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_portfolio_turnover_attribution_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "root_cause_category": "MISSING",
            "top_turnover_assets": "",
            "recommended_next_action": "",
            "summary_sentence": (
                "Portfolio turnover attribution is missing; Reader Brief cannot yet explain "
                "which turnover or cost-drag driver blocked weight candidates."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "root_cause_category": "MISSING",
            "top_turnover_assets": "",
            "recommended_next_action": "",
            "summary_sentence": (
                "Portfolio turnover attribution is unreadable; production parameters and "
                "turnover guardrails remain unchanged."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    summary = _mapping(payload.get("summary"))
    root_cause = _mapping(payload.get("root_cause"))
    next_action = _mapping(payload.get("recommended_next_action"))
    candidate_summary = _mapping(payload.get("candidate_turnover_summary"))
    category = _text(root_cause.get("category"), "mixed")
    top_assets = [
        _text(item.get("symbol"), "")
        for item in _records(payload.get("asset_turnover_contribution"))[:3]
        if item.get("symbol")
    ]
    action = _text(next_action.get("action"), "")
    if metadata.get("status") == "BLOCKED":
        sentence = (
            "Portfolio turnover attribution is blocked because required TRADING-059/059A "
            "artifacts are missing."
        )
    elif category == "rebalance_threshold_too_low":
        sentence = (
            "Weight tuning failed mainly because candidate portfolios generated excessive "
            "turnover under the current rebalance threshold. Test turnover-control overlays "
            "before expanding the weight search space."
        )
    elif category == "score_volatility_too_high":
        sentence = (
            "Weight tuning failed mainly because candidate weights amplified score volatility "
            "and caused frequent rebalances."
        )
    elif category == "weight_search_too_aggressive":
        sentence = (
            "Weight tuning failed mainly because candidate weights moved too far from the "
            "baseline and increased turnover pressure."
        )
    elif category == "asset_rotation_too_frequent":
        assets = ", ".join(top_assets)
        sentence = (
            "Weight tuning failed mainly because turnover is concentrated in a small set of "
            f"assets: {assets}."
        )
    elif category == "cost_model_too_punitive":
        sentence = (
            "Weight tuning failed mainly due to cost drag. Cost assumptions should be "
            "reviewed before changing signal weights or portfolio profiles."
        )
    elif category == "insufficient_details":
        sentence = (
            "Weight tuning turnover attribution is limited because candidate turnover "
            "details are insufficient."
        )
    else:
        sentence = (
            "Weight tuning turnover attribution found mixed turnover and cost drivers; "
            "production parameters remain unchanged."
        )
    return {
        "status": _text(metadata.get("status"), "UNKNOWN"),
        "root_cause_category": category,
        "top_failure_reason": _text(summary.get("top_failure_reason"), ""),
        "top_turnover_assets": ", ".join(top_assets),
        "failed_candidate_count": candidate_summary.get("total_failed_by_turnover", 0),
        "avg_cost_drag_delta": candidate_summary.get("avg_cost_drag_delta", 0.0),
        "recommended_next_action": action,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _market_data_freshness_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_market_data_freshness_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "tracking_date": "",
            "effective_data_date": "",
            "tracking_readiness": "unknown",
            "summary_sentence": (
                "Market data freshness summary is missing; Reader Brief does not run "
                "freshness checks."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "tracking_date": "",
            "effective_data_date": "",
            "tracking_readiness": "unknown",
            "summary_sentence": (
                "Market data freshness summary is unreadable; shadow tracking readiness "
                "requires manual review."
            ),
        }
    freshness = _mapping(payload.get("freshness"))
    data_dates = _mapping(payload.get("data_dates"))
    readiness = _mapping(payload.get("tracking_readiness"))
    status = _text(freshness.get("status"), _text(_mapping(payload.get("metadata")).get("status")))
    tracking_date = _text(data_dates.get("tracking_date"))
    effective_data_date = _text(data_dates.get("effective_data_date"))
    tracking_readiness = _text(readiness.get("readiness"), "unknown")
    if status == "OK":
        sentence = (
            "Market data freshness is OK. Shadow candidate tracking uses current "
            "effective data and remains advisory only."
        )
    elif status == "ACCEPTABLE_LAG":
        sentence = (
            f"Market data freshness is ACCEPTABLE_LAG: tracking date is {tracking_date} "
            f"while effective data remains {effective_data_date}. Shadow candidate "
            "tracking can continue in degraded mode, but production promotion remains "
            "disabled."
        )
    elif status == "NON_TRADING_DAY":
        sentence = (
            "Market data freshness is NON_TRADING_DAY. Shadow candidate tracking can use "
            "the latest previous trading day data and remains advisory only."
        )
    elif status == "STALE":
        sentence = (
            "Market data freshness is STALE. Shadow candidate tracking is blocked until "
            "market data cache and manifest are refreshed."
        )
    else:
        sentence = (
            f"Market data freshness is {status}. Shadow candidate tracking readiness is "
            f"{tracking_readiness}; production promotion remains disabled."
        )
    return {
        "status": status,
        "tracking_date": tracking_date,
        "effective_data_date": effective_data_date,
        "tracking_readiness": tracking_readiness,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _market_data_refresh_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_market_data_refresh_path(as_of)
    if path is None:
        return {
            "status": "MISSING",
            "target_date": "",
            "summary_sentence": (
                "Market data refresh summary is missing; Reader Brief does not run "
                "refresh or recovery actions."
            ),
        }
    payload = _read_optional_json(path)
    if not payload:
        return {
            "status": "MISSING",
            "target_date": "",
            "summary_sentence": (
                "Market data refresh summary is unreadable; recovery status requires manual review."
            ),
        }
    metadata = _mapping(payload.get("metadata"))
    actions = _mapping(payload.get("actions"))
    before = _mapping(payload.get("before"))
    after = _mapping(payload.get("after"))
    status = _text(metadata.get("status"), "UNKNOWN")
    target_date = _text(actions.get("target_date"))
    if status == "OK":
        sentence = (
            f"Market data refresh recovered freshness for {target_date}. Required "
            "assets were refreshed and the candidate tracking workflow is active again. "
            "Production promotion remains disabled."
        )
    elif status == "SOURCE_DELAYED":
        sentence = (
            "Market data refresh could not recover freshness because the data source "
            "has not provided the latest daily bars. Shadow candidate tracking remains "
            "blocked until data is available."
        )
    elif status == "NOT_NEEDED":
        sentence = "Market data refresh is NOT_NEEDED because freshness does not require recovery."
    else:
        sentence = (
            f"Market data refresh is {status}: before freshness was "
            f"{_text(before.get('freshness_status'), 'UNKNOWN')} and after freshness is "
            f"{_text(after.get('freshness_status'), 'UNKNOWN')}. Production promotion "
            "remains disabled."
        )
    return {
        "status": status,
        "target_date": target_date,
        "source_artifact": str(path),
        "summary_sentence": sentence,
    }


def _price_cache_reconcile_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_price_cache_reconcile_path(as_of)
    if path is None:
        return {"status": "MISSING", "summary_sentence": ""}
    payload = _read_optional_json(path)
    if not payload:
        return {"status": "MISSING", "summary_sentence": ""}
    metadata = _mapping(payload.get("metadata"))
    actions = _mapping(payload.get("actions"))
    after = _mapping(payload.get("after"))
    registered = _texts(actions.get("registered_repaired_artifacts"))
    status = _text(metadata.get("status"), "UNKNOWN")
    if status in {"OK", "LIMITED"}:
        return {
            "status": status,
            "summary_sentence": (
                "Price cache reconciliation resolved the manifest/cache asset view for "
                f"{', '.join(registered) or 'repaired assets'}; latest_resolution="
                f"{_text(after.get('latest_resolution'), 'UNKNOWN')}."
            ),
        }
    if status == "FAILED":
        return {
            "status": status,
            "summary_sentence": (
                "Price cache reconciliation remains blocked. Repaired price histories exist "
                "only if a validated cache artifact can be recovered and the manifest can be "
                "refreshed without lowering the data quality gate."
            ),
        }
    return {"status": status, "summary_sentence": ""}


def _latest_price_cache_reconcile_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_quality"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/price_cache_reconcile_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _report_index_artifact_path(payload: Mapping[str, Any], report_id: str) -> Path | None:
    for report in _records(payload.get("reports")):
        if _text(report.get("report_id")) != report_id:
            continue
        raw_path = _text(report.get("latest_artifact_path"))
        if not raw_path or raw_path == "MISSING":
            return None
        path = Path(raw_path)
        if path.suffix != ".json":
            json_sibling = path.with_suffix(".json")
            if json_sibling.exists():
                return json_sibling
        if path.exists():
            return path
    return None


def _first_existing_path(*paths: Path | None) -> Path | None:
    for path in paths:
        if path is not None and path.exists():
            return path
    return None


def _latest_signal_calibration_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "signal_calibration"
    exact = root / as_of.isoformat() / "signal_calibration_summary.json"
    if exact.exists():
        return exact
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/signal_calibration_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _latest_portfolio_sensitivity_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_sensitivity"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_sensitivity_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_candidates_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidates"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidates_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _latest_portfolio_candidate_review_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_reviews"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_review_decision.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_candidate_tracking_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_candidate_tracking"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_tracking_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_portfolio_tracking_review_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_tracking_reviews"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_tracking_review_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_weight_tuning_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "weight_tuning"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_tuning_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_weight_tuning_failure_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "weight_tuning_failure"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_tuning_failure_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_weight_stability_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "weight_stability"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_stability_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_weight_stability_readiness_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "weight_stability_readiness"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/weight_stability_readiness_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if candidates:
        return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]
    latest_candidates = sorted(root.glob("*/weight_stability_readiness_summary.json"))
    if not latest_candidates:
        return None
    return max(latest_candidates, key=lambda path: path.stat().st_mtime)


def _latest_portfolio_turnover_attribution_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "portfolio_turnover_attribution"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_turnover_attribution_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_market_data_freshness_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_freshness"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_freshness_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _latest_market_data_refresh_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "artifacts" / "data_refresh"
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/market_data_refresh_summary.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def _parameter_shadow_data_quality_sentence(
    *,
    data_quality_status: str,
    promotion_status: str,
    diagnostic_summary: Mapping[str, Any],
) -> str:
    normalized_status = data_quality_status.upper()
    backtest_mode = _text(diagnostic_summary.get("backtest_mode"))
    can_run_shadow = diagnostic_summary.get("can_run_shadow_backtest") is True
    can_promote = diagnostic_summary.get("can_promote_candidate") is True
    if backtest_mode == "full_signal_backtest_limited" and can_run_shadow and not can_promote:
        return (
            "Signal snapshot is available in limited mode. The system now runs "
            "full-signal-limited shadow backtests using price-derived trend and sector "
            "signals, while remaining limited or fallback signals keep candidate promotion "
            "disabled until signal quality reaches OK."
        )
    if backtest_mode == "full_signal_backtest" and can_run_shadow and can_promote:
        return (
            "Signal snapshot quality is OK. Shadow backtest can evaluate candidate "
            "parameters under full-signal mode, while production promotion still requires "
            "manual review."
        )
    if backtest_mode == "price_only_shadow_backtest" and can_run_shadow and not can_promote:
        return (
            "Shadow parameter review can now run in price-only mode because required price "
            "history is available. However, signal snapshot is missing, so candidate "
            "promotion is rejected until full signal inputs are available."
        )
    if normalized_status in {"FAILED", "FAIL", "INSUFFICIENT_DATA"}:
        reasons = diagnostic_summary.get("blocking_reasons")
        reason_items = (
            [str(reason) for reason in reasons if str(reason)] if isinstance(reasons, list) else []
        )
        missing_assets = _missing_price_assets_from_reasons(reason_items)
        if missing_assets:
            return (
                "Shadow parameter review remains blocked because required price history is "
                f"missing for {_format_english_list(missing_assets)}."
            )
        if isinstance(reasons, list) and reasons:
            return "Shadow parameter review remains blocked because " + "; ".join(
                str(reason) for reason in reasons if str(reason)
            )
        return (
            "Shadow parameter review remains rejected because the backtest input data quality "
            "gate failed. Missing or stale input data must be repaired before candidate "
            "parameters can be evaluated for promotion."
        )
    if normalized_status in {"OK", "PASS"}:
        return (
            "Shadow parameter review completed with valid backtest inputs. Candidate promotion "
            f"status is currently {promotion_status.upper()}."
        )
    blocking_errors = _text(diagnostic_summary.get("blocking_errors"), "0")
    return (
        "Shadow parameter review is limited by backtest input data quality warnings. "
        f"blocking_errors={blocking_errors}; promotion remains disabled until input quality is OK."
    )


def _parameter_shadow_promotion_eligibility(backtest_mode: str) -> str:
    if backtest_mode == "price_only_shadow_backtest":
        return "Disabled"
    if backtest_mode == "full_signal_backtest_limited":
        return "Watch-only"
    if backtest_mode == "full_signal_backtest":
        return "Candidate allowed"
    if backtest_mode == "blocked":
        return "Blocked by data quality"
    return "Unknown"


def _missing_price_assets_from_reasons(reasons: list[str]) -> list[str]:
    assets: list[str] = []
    for reason in reasons:
        if "price history" not in reason.lower():
            continue
        _, _, tail = reason.partition(" for ")
        if not tail:
            continue
        for token in tail.replace(" and ", ", ").split(","):
            symbol = token.strip().strip(".;")
            if symbol and symbol not in assets:
                assets.append(symbol)
    return assets


def _format_english_list(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"


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
    governance_review_items = _records(research_governance_summary.get("manual_review_queue"))
    if governance_review_items:
        for review_item in governance_review_items:
            items.append(
                {
                    "action_id": _text(review_item.get("item_id"), "research_governance_review"),
                    "severity": _text(review_item.get("severity"), "warning"),
                    "category": _text(review_item.get("category"), "research_governance"),
                    "reason": _text(review_item.get("reason"), "research governance review"),
                    "source_artifact": _text(
                        review_item.get("source_artifact_full_path"),
                        _text(review_item.get("source_artifact"), "research_governance_summary"),
                    ),
                    "recommended_next_action": _text(
                        review_item.get("recommended_next_action"),
                    ),
                    "decision_impact": _text(review_item.get("decision_impact")),
                    "production_impact": "manual_review_only",
                }
            )
    else:
        research_summary = _mapping(research_governance_summary.get("summary"))
        manual_count = _int(research_summary.get("manual_review_required_count"))
        if manual_count:
            items.append(
                {
                    "action_id": "research_governance_manual_review",
                    "severity": "warning",
                    "category": "research_governance",
                    "reason": (
                        f"{manual_count} research/shadow/governance cards require manual review."
                    ),
                    "source_artifact": "research_governance_summary",
                    "production_impact": "manual_review_only",
                }
            )
    governance_status = _text(research_governance_summary.get("governance_status"))
    promotion_status = _text(research_governance_summary.get("promotion_status"))
    if governance_status or promotion_status:
        items.append(
            {
                "action_id": "research_governance_status_review",
                "severity": "warning" if promotion_status != "PROMOTABLE" else "info",
                "category": "research_governance",
                "reason": (
                    f"research governance status={governance_status or 'UNKNOWN'}; "
                    f"promotion_status={promotion_status or 'UNKNOWN'}"
                ),
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
        "top_items": _top_manual_review_items(enriched),
        "impact_groups": _manual_review_impact_groups(enriched),
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
        "weight_iteration": (
            "打开 weight candidate / promotion gate 产物，确认是否仅阻断研究晋升。"
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
        "weight_iteration": "影响研究/权重晋升，不直接改变今日 score。",
        "documentation_contract": "影响文档治理可信度，不直接改变投资结论。",
        "manual_review": "可能降低某一分项或数据来源的解释置信度。",
    }
    provided_action = _text(item.get("recommended_next_action"))
    provided_decision_impact = _text(item.get("decision_impact"))
    return {
        **dict(item),
        "impact_type": _manual_review_impact_type(item),
        "impact_label": _manual_review_impact_label(_manual_review_impact_type(item)),
        "recommended_next_action": provided_action
        or action_by_category.get(
            category,
            f"复核 {category}：{reason}",
        ),
        "decision_impact": provided_decision_impact
        or decision_impact_by_category.get(
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


def _manual_review_impact_type(item: Mapping[str, Any]) -> str:
    category = _text(item.get("category")).lower()
    action_id = _text(item.get("action_id")).lower()
    reason = _text(item.get("reason")).lower()
    combined = f"{category} {action_id} {reason}"
    if category in {"data_quality", "manual_review"}:
        return "today_decision"
    if category == "report_freshness" and any(
        token in combined
        for token in ("data_quality", "daily_score", "daily_decision", "market_panel")
    ):
        return "today_decision"
    if any(token in combined for token in ("sec", "fmp", "valuation", "fundamental")):
        return "today_decision"
    if any(
        token in combined
        for token in (
            "promotion",
            "weight",
            "research_governance",
            "shadow",
            "backtest",
            "parameter",
        )
    ):
        return "research_promotion"
    return "audit_observe"


def _manual_review_impact_label(impact_type: str) -> str:
    return {
        "today_decision": "影响今日结论",
        "research_promotion": "影响研究晋升",
        "audit_observe": "仅审计/观察",
    }.get(impact_type, "仅审计/观察")


def _manual_review_impact_groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = [
        ("today_decision", "影响今日结论"),
        ("research_promotion", "影响研究晋升"),
        ("audit_observe", "仅审计/观察"),
    ]
    return [
        {
            "impact_type": impact_type,
            "label": label,
            "count": len([item for item in items if _text(item.get("impact_type")) == impact_type]),
            "items": [item for item in items if _text(item.get("impact_type")) == impact_type],
        }
        for impact_type, label in order
    ]


def _top_manual_review_items(
    items: list[dict[str, Any]], *, limit: int = 3
) -> list[dict[str, Any]]:
    severity_rank = {"critical": 0, "warning": 1, "info": 2}
    impact_rank = {"today_decision": 0, "research_promotion": 1, "audit_observe": 2}
    return sorted(
        items,
        key=lambda item: (
            severity_rank.get(_text(item.get("severity")), 9),
            impact_rank.get(_text(item.get("impact_type")), 9),
            _text(item.get("action_id")),
        ),
    )[:limit]


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
    market_situation: Mapping[str, Any],
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
        "market_movement": _text(
            market_situation.get("market_movement_sentence"),
            "MISSING",
        ),
        "major_score_change": (
            "overall_delta="
            f"{_format_signed_number(score_changes.get('overall_score_delta'), digits=2)}; "
            "position_max_delta="
            f"{_format_signed_number(score_changes.get('final_position_max_delta'), digits=2)}"
        ),
        "report_freshness": (
            f"missing={_text(report_index_summary.get('missing_count'), '0')}; "
            f"stale={_text(report_index_summary.get('stale_count'), '0')}; "
            f"required_missing={_text(report_index_summary.get('required_missing_count'), '0')}"
        ),
        "governance_status": _text(governance_summary.get("status"), "UNKNOWN"),
        "research_governance": (
            f"research governance status = {_text(governance_summary.get('status'), 'UNKNOWN')}; "
            f"promotion_status = {_text(governance_summary.get('promotion_status'), 'UNKNOWN')}"
        ),
        "manual_review_count": len(_records(manual_review_queue.get("items"))),
        "production_effect": PRODUCTION_EFFECT,
    }


def _narrative_executive_summary(
    *,
    run_context: Mapping[str, Any],
    decision: Mapping[str, Any],
    market_situation: Mapping[str, Any],
    score_changes: Mapping[str, Any],
    contribution_summary: Mapping[str, Any],
    governance_summary: Mapping[str, Any],
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
    impact_by_chain = {
        _text(item.get("chain")): item
        for item in _records(missing_artifact_impact.get("impact_summary"))
    }
    daily_chain = _mapping(impact_by_chain.get("今日评分链路"))
    reader_chain = _mapping(impact_by_chain.get("阅读上下文"))
    promotion_chain = _mapping(impact_by_chain.get("研究/权重晋升链路"))
    score_delta = _float_or_none(score_changes.get("overall_score_delta"))
    score_delta_text = (
        _format_signed_number(score_delta, digits=2)
        if score_delta is not None
        else _text(score_changes.get("overall_score_delta"), "MISSING")
    )
    return {
        "today_conclusion": (
            f"今日系统结论为 {action}，最终 AI 风险资产仓位为 {position}。"
            f"当前适用市场 regime 为 {_text(run_context.get('market_regime'), 'UNKNOWN')}。"
        ),
        "today_market_movement": _text(
            market_situation.get("market_movement_sentence"),
            "市场面板缺失，不能描述今日 benchmark/AI sector/risk/liquidity 变化。",
        ),
        "why_this_conclusion": (
            "主要正向贡献来自 "
            + (", ".join(positives) if positives else "MISSING")
            + "；主要拖累或零贡献来自 "
            + (", ".join(negatives) if negatives else "MISSING")
            + (f"；score change overall_delta={score_delta_text}。")
        ),
        "main_positive_drivers": positives,
        "main_negative_drivers": negatives,
        "binding_constraint": (
            f"最终仓位受 {binding} 约束。{_text(decision.get('binding_gate_reason'))}"
        ),
        "manual_review_summary": (
            f"当前有 {manual_count} 个复核项，其中 critical={critical_count}；"
            f"缺失/受限 artifact 中 blocking={blocking_missing}, important={important_missing}。"
            f"今日评分链路={_text(daily_chain.get('status'), 'UNKNOWN')}；"
            f"阅读上下文={_text(reader_chain.get('status'), 'UNKNOWN')}；"
            f"研究/权重晋升链路={_text(promotion_chain.get('status'), 'UNKNOWN')}。"
        ),
        "research_governance_summary": (
            f"research governance status = {_text(governance_summary.get('status'), 'UNKNOWN')}; "
            f"promotion_status = {_text(governance_summary.get('promotion_status'), 'UNKNOWN')}。"
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


def _status_panel(
    *,
    build_status: str,
    decision: Mapping[str, Any],
    governance_summary: Mapping[str, Any],
    manual_review_queue: Mapping[str, Any],
    missing_artifact_impact: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
    data_quality_pit_safety: Mapping[str, Any],
) -> dict[str, Any]:
    decision_status = _decision_usability_status(
        decision=decision,
        manual_review_queue=manual_review_queue,
        missing_artifact_impact=missing_artifact_impact,
        report_index_summary=report_index_summary,
        data_quality_pit_safety=data_quality_pit_safety,
    )
    promotion_status = _research_promotion_status(governance_summary)
    return {
        "build_status": build_status,
        "decision_usability": decision_status,
        "research_promotion_status": promotion_status,
        "raw_reader_brief_status": build_status,
        "raw_promotion_status": _text(governance_summary.get("promotion_status"), "UNKNOWN"),
        "build_status_explanation": (
            "Reader Brief artifact 已生成；该状态只说明简报构建成功，不等于今日结论可直接行动。"
            if build_status in {"OK", "PASS", "PASS_WITH_WARNINGS", "LIMITED_READER_CONTEXT"}
            else "Reader Brief 构建或输入校验存在失败，需要先修复。"
        ),
        "decision_usability_explanation": _decision_usability_explanation(decision_status),
        "research_promotion_explanation": _promotion_status_explanation(
            promotion_status,
            governance_summary,
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _decision_usability_status(
    *,
    decision: Mapping[str, Any],
    manual_review_queue: Mapping[str, Any],
    missing_artifact_impact: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
    data_quality_pit_safety: Mapping[str, Any],
) -> str:
    data_gate_status = _leading_status(data_quality_pit_safety.get("data_gate_status")).upper()
    if data_gate_status in {"FAIL", "FAILED", "BLOCKED_BY_DATA_QUALITY"}:
        return "LIMITED_CONTEXT"
    if _int(missing_artifact_impact.get("blocking_count")) or _int(
        report_index_summary.get("required_missing_count")
    ):
        return "LIMITED_CONTEXT"
    manual_items = _records(manual_review_queue.get("items"))
    critical_count = _manual_review_severity_count(manual_review_queue, "critical")
    if (
        bool(decision.get("manual_review_required"))
        or critical_count > 0
        or _action_requests_manual_review(decision.get("action"))
    ):
        return "MANUAL_REVIEW_REQUIRED"
    if manual_items or data_gate_status in {"PASS_WITH_WARNINGS", "PASS_WITH_LIMITATIONS"}:
        return "REVIEW_WITH_LIMITATIONS"
    if _int(missing_artifact_impact.get("important_count")):
        return "REVIEW_WITH_LIMITATIONS"
    return "READY_FOR_READING"


def _decision_usability_explanation(status: str) -> str:
    return {
        "READY_FOR_READING": "今日结论可阅读；仍然不是交易指令。",
        "REVIEW_WITH_LIMITATIONS": (
            "今日结论可阅读，但存在 warning 或重要上下文缺口，行动前需复核。"
        ),
        "MANUAL_REVIEW_REQUIRED": "今日结论需要人工复核后才可进入投资讨论或执行前判断。",
        "LIMITED_CONTEXT": "今日结论上下文受限；存在必需报告缺失、data gate failure 或阻断项。",
    }.get(status, "今日结论使用等级未知，需打开审计区确认。")


def _research_promotion_status(governance_summary: Mapping[str, Any]) -> str:
    raw_status = _text(governance_summary.get("promotion_status"), "UNKNOWN")
    missing_count = _int(governance_summary.get("missing_count"))
    if raw_status == "BLOCKED_BY_MISSING_ARTIFACTS":
        return raw_status
    if missing_count and raw_status != "PROMOTABLE":
        return "BLOCKED_BY_MISSING_ARTIFACTS"
    return raw_status


def _promotion_status_explanation(status: str, governance_summary: Mapping[str, Any]) -> str:
    missing_count = _int(governance_summary.get("missing_count"))
    if status == "BLOCKED_BY_MISSING_ARTIFACTS":
        return (
            f"研究/权重晋升被缺失 artifact 阻断；当前 missing_count={missing_count}，"
            "不影响已生成的今日 score，但不能推进 weight promotion。"
        )
    if status == "PROMOTABLE":
        return "研究晋升状态为 PROMOTABLE；仍需遵守人工审批和 production-effect 边界。"
    if status == "NOT_PROMOTABLE":
        return "研究晋升当前不可晋级；不得写入 production weights 或 active shadow weights。"
    return "研究晋升状态未知或受限；需打开 research governance summary。"


def _action_checklist(
    *,
    decision: Mapping[str, Any],
    status_panel: Mapping[str, Any],
    governance_summary: Mapping[str, Any],
    manual_review_queue: Mapping[str, Any],
    data_quality_pit_safety: Mapping[str, Any],
) -> list[dict[str, Any]]:
    position = _text(decision.get("final_risk_asset_ai_position"), "UNKNOWN")
    binding_gate = _text(decision.get("binding_gate_label"), "UNKNOWN")
    data_gate_status = _text(data_quality_pit_safety.get("data_gate_status"), "UNKNOWN")
    promotion_status = _text(
        status_panel.get("research_promotion_status"),
        _research_promotion_status(governance_summary),
    )
    items = [
        _checklist_item(
            1,
            (
                f"不新增 AI 风险资产仓位；除非人工确认 {binding_gate} gate 可解除。"
                if binding_gate != "UNKNOWN"
                else "不新增 AI 风险资产仓位；先确认当前最大约束。"
            ),
            "Decision Usability 不是 READY_FOR_READING 时，首页结论只能进入复核流程。",
            "today_decision",
            _text(status_panel.get("decision_usability"), "UNKNOWN"),
        ),
        _checklist_item(
            2,
            f"保持并复核现有 AI 仓位上限 {position}。",
            "最终仓位来自 score、confidence 和 gate 后的受限结果。",
            "today_decision",
            _text(decision.get("data_gate"), data_gate_status),
        ),
    ]
    top_reviews = _records(manual_review_queue.get("top_items"))
    if top_reviews:
        top_sources = ", ".join(_text(item.get("action_id")) for item in top_reviews[:2])
        items.append(
            _checklist_item(
                3,
                f"优先处理 Top Review Items Today：{top_sources}。",
                "这些复核项优先级高于完整 23 项队列的逐项阅读。",
                "today_decision",
                "ACTION_REQUIRED",
            )
        )
    else:
        items.append(
            _checklist_item(
                3,
                "确认 Data Quality / SEC / FMP warning 区是否为空。",
                "PIT 与数据源 warning 直接影响读者对今日结论的信任等级。",
                "today_decision",
                data_gate_status,
            )
        )
    if promotion_status != "PROMOTABLE":
        items.append(
            _checklist_item(
                4,
                "不进行 weight promotion。",
                (
                    f"promotion_status={promotion_status}；"
                    "缺失或受限研究 artifact 只允许进入人工治理复核。"
                ),
                "research_promotion",
                promotion_status,
            )
        )
    items.append(
        _checklist_item(
            len(items) + 1,
            "确认本报告只读，不触发 broker/trading action。",
            (
                "production_effect=none；Reader Brief 不写 production weights "
                "或 active shadow weights。"
            ),
            "audit_observe",
            PRODUCTION_EFFECT,
        )
    )
    return items


def _checklist_item(
    priority: int,
    action: str,
    rationale: str,
    impact_type: str,
    status: str,
) -> dict[str, Any]:
    return {
        "priority": priority,
        "action": action,
        "rationale": rationale,
        "impact_type": impact_type,
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
    }


def _score_change_narrative(
    *,
    score_changes: Mapping[str, Any],
    contribution_summary: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    delta = _float_or_none(score_changes.get("overall_score_delta"))
    position_delta = _float_or_none(score_changes.get("final_position_max_delta"))
    if delta is None:
        return {
            "status": "INSUFFICIENT_DATA",
            "summary": "缺少上一期对比，不能描述今天相对昨天/上一交易日发生了什么。",
            "position_interpretation": "仓位变化原因需打开 decision snapshot 和 gate ladder 审计。",
            "production_effect": PRODUCTION_EFFECT,
        }
    positive = _texts(contribution_summary.get("top_positive_contributors"))
    negative = _texts(contribution_summary.get("top_negative_or_zero_contributors"))
    direction = "上升" if delta > 0 else "下降" if delta < 0 else "基本持平"
    driver_text = (
        f"主要正向来自 {', '.join(positive[:2])}"
        if positive and delta >= 0
        else f"主要拖累来自 {', '.join(negative[:2])}"
        if negative
        else "主要驱动未充分披露"
    )
    binding = _text(decision.get("binding_gate_label"), "UNKNOWN")
    if position_delta is None:
        position_sentence = "最终仓位变化缺少上一期可比字段。"
    elif abs(position_delta) < 1e-9:
        position_sentence = f"仓位没有提升，主要因为 {binding} 仍是最终约束。"
    else:
        position_sentence = (
            f"最终仓位上限变化 {_format_signed_number(position_delta, digits=2)}；"
            f"仍需结合 {binding} gate 判断。"
        )
    return {
        "status": _text(score_changes.get("status"), "AVAILABLE"),
        "summary": (
            f"今日 score {_format_signed_number(delta, digits=2)}，相对上一期{direction}；"
            f"{driver_text}。"
        ),
        "position_interpretation": position_sentence,
        "production_effect": PRODUCTION_EFFECT,
    }


def _manual_review_severity_count(manual_review_queue: Mapping[str, Any], severity: str) -> int:
    for group in _records(manual_review_queue.get("groups")):
        if _text(group.get("severity")) == severity:
            return _int(group.get("count"))
    return 0


def _action_requests_manual_review(value: object) -> bool:
    text = _text(value).lower()
    return "manual" in text or "人工复核" in text


def _documentation_contract_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "error_count": 0,
            "warning_count": 0,
            "production_effect": PRODUCTION_EFFECT,
            "limitation": (
                "documentation_contract artifact missing; Reader Brief 不补造文档治理结论。"
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
                    "cadence": _reader_cadence_label(report),
                    "latest_status": "UNKNOWN",
                    "last_run": "MISSING_RUNTIME_INDEX",
                    "next_expected_run": _reader_next_expected_run(
                        report,
                        fallback="按 registry freshness_sla_days 复核",
                    ),
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
                "cadence": _reader_cadence_label(report),
                "last_run": _text(report.get("artifact_date"), "MISSING"),
                "next_expected_run": _reader_next_expected_run(
                    report,
                    fallback="按 report_index freshness_sla_days 复核",
                ),
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
        link["navigation_source"] = "reader_brief_runtime_input"
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
                "navigation_source": "report_index_runtime",
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
            "navigation_source": "documentation_static",
        }
    )
    return _dedupe_navigation_links(links)


def _report_navigation_groups(navigation: list[dict[str, Any]]) -> dict[str, Any]:
    purposes = [
        "Core decision artifacts",
        "Detailed evidence",
        "Governance / documentation",
        "Missing but expected",
    ]
    groups = []
    for purpose in purposes:
        items = sorted(
            [item for item in navigation if _text(item.get("purpose")) == purpose],
            key=_navigation_sort_key,
        )
        groups.append({"purpose": purpose, "count": len(items), "items": items})
    return {
        "status": "AVAILABLE",
        "production_effect": PRODUCTION_EFFECT,
        "groups": groups,
    }


def _dedupe_navigation_links(links: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for link in links:
        artifact_id = _text(link.get("artifact_id"))
        purpose = _text(link.get("purpose"), _navigation_purpose(artifact_id, "UNKNOWN"))
        key = (purpose, artifact_id)
        current = merged.get(key)
        if current is None:
            record = dict(link)
            record["navigation_sources"] = [_navigation_source_record(link)]
            merged[key] = record
            continue
        merged[key] = _merge_navigation_link(current, link)
    return sorted(merged.values(), key=_navigation_sort_key)


def _merge_navigation_link(
    current: Mapping[str, Any], incoming: Mapping[str, Any]
) -> dict[str, Any]:
    merged = dict(current)
    incoming_source = _navigation_source_record(incoming)
    merged["navigation_sources"] = _dedupe_navigation_sources(
        [*_records(merged.get("navigation_sources")), incoming_source]
    )
    merged["status"] = _more_specific_status(
        _text(merged.get("status")),
        _text(incoming.get("status")),
    )
    merged["freshness_status"] = _more_specific_status(
        _text(merged.get("freshness_status")),
        _text(incoming.get("freshness_status")),
    )
    if _prefer_navigation_record(merged, incoming):
        for key in (
            "title",
            "short_name",
            "path",
            "full_path",
            "href",
            "exists",
            "production_effect",
            "why_open_this",
            "navigation_source",
        ):
            if _text(incoming.get(key)) or key == "exists":
                merged[key] = incoming.get(key)
    merged["impact_level"] = _higher_impact_level(
        _text(merged.get("impact_level"), "INFO"),
        _text(incoming.get("impact_level"), "INFO"),
    )
    return merged


def _navigation_source_record(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source": _text(record.get("navigation_source"), "unknown"),
        "status": _text(record.get("status"), "UNKNOWN"),
        "freshness_status": _text(record.get("freshness_status"), "UNKNOWN"),
        "path": _text(record.get("full_path"), _text(record.get("path"))),
    }


def _dedupe_navigation_sources(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for record in records:
        key = (_text(record.get("source")), _text(record.get("path")))
        by_key[key] = record
    return sorted(
        by_key.values(),
        key=lambda item: (_text(item.get("source")), _text(item.get("path"))),
    )


def _prefer_navigation_record(current: Mapping[str, Any], incoming: Mapping[str, Any]) -> bool:
    current_rank = _navigation_source_rank(_text(current.get("navigation_source")))
    incoming_rank = _navigation_source_rank(_text(incoming.get("navigation_source")))
    if incoming_rank != current_rank:
        return incoming_rank > current_rank
    if bool(incoming.get("exists")) != bool(current.get("exists")):
        return bool(incoming.get("exists"))
    return _status_specificity(_text(incoming.get("status"))) > _status_specificity(
        _text(current.get("status"))
    )


def _navigation_source_rank(source: str) -> int:
    return {
        "documentation_static": 1,
        "reader_brief_runtime_input": 2,
        "report_index_runtime": 3,
    }.get(source, 0)


def _more_specific_status(left: str, right: str) -> str:
    return right if _status_specificity(right) > _status_specificity(left) else left


def _status_specificity(status: str) -> int:
    normalized = status.upper()
    if not normalized or normalized == "UNKNOWN":
        return 0
    if normalized in {"AVAILABLE", "DOCUMENTATION"}:
        return 1
    if normalized == "FRESH":
        return 2
    if normalized in {"PASS", "OK"}:
        return 2
    if normalized in {"LIMITED", "PASS_WITH_WARNINGS", "PASS_WITH_LIMITATIONS"}:
        return 3
    if normalized in {"MISSING", "STALE", "REQUIRED_MISSING", "FAILED", "FAIL"}:
        return 4
    return 2


def _higher_impact_level(left: str, right: str) -> str:
    ranks = {"INFO": 0, "OPTIONAL": 1, "IMPORTANT": 2, "BLOCKING": 3}
    return right if ranks.get(right, 0) > ranks.get(left, 0) else left


def _navigation_sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
    order = {
        "decision_snapshot": 10,
        "daily_decision_summary": 20,
        "daily_report": 30,
        "reader_brief": 40,
        "market_panel": 100,
        "score_change_attribution": 110,
        "evidence_dashboard": 120,
        "daily_task_dashboard": 130,
        "calculation_explainers": 140,
        "trace_bundle": 150,
        "research_governance_summary": 200,
        "report_index": 210,
        "report_index_waiver_inventory": 211,
        "report_index_waiver_inventory_validation": 212,
        "reader_brief_consistency_pack": 213,
        "reader_brief_consistency_validation": 214,
        "production_boundary_static_scan": 215,
        "production_boundary_static_scan_validation": 216,
        "owner_review_template_v2": 217,
        "owner_review_template_v2_validation": 218,
        "owner_decision_audit_log": 219,
        "owner_decision_audit_log_validation": 220,
        "research_monthly_review_pack": 221,
        "research_monthly_review_pack_validation": 222,
        "research_safety_boundary_audit": 221,
        "research_safety_boundary_validation": 222,
        "decision_snapshot_lifecycle_policy": 223,
        "decision_snapshot_lifecycle_policy_validation": 224,
        "documentation_contract": 225,
        "task_register_consistency": 226,
        "task_register_consistency_validation": 227,
        "artifact_lineage_graph": 228,
        "artifact_lineage_validation": 229,
        "research_roadmap_dashboard": 230,
        "research_roadmap_dashboard_validation": 231,
        "research_governance_end_to_end_pack": 232,
        "research_governance_end_to_end_pack_validation": 233,
        "research_governance_recovery_pack": 234,
        "research_governance_recovery_pack_validation": 235,
        "eight_blocker_decision_review": 236,
        "eight_blocker_decision_review_validation": 237,
        "normal_shadow_gate_gap_analysis": 238,
        "promotion_blocker_after_metrics_review": 239,
        "candidate_research_return_assessment": 240,
        "owner_decision_options_packet": 241,
        "owner_decision_dry_run": 242,
        "observation_clock_readiness_plan": 243,
        "post_decision_rerun_plan": 244,
        "report_quality_warning_drilldown": 245,
        "governance_status_snapshot_after_decision_review": 246,
        "owner_return_to_research_decision_record": 247,
        "candidate_return_to_research_transition_pack": 248,
        "candidate_failure_mode_attribution": 249,
        "reusable_evidence_extraction": 250,
        "return_to_research_hypothesis_backlog": 251,
        "next_candidate_spec_draft": 252,
        "research_backfill_plan_for_next_candidate": 253,
        "archived_candidate_status_update": 254,
        "research_cycle_reset_pack": 255,
        "return_to_research_governance_snapshot": 256,
        "return_to_research_governance_snapshot_validation": 257,
        "next_research_cycle_intake": 258,
        "next_candidate_spec_frozen": 259,
        "next_candidate_backfill": 260,
        "next_candidate_backfill_validation": 261,
        "next_candidate_stress_review": 262,
        "next_candidate_stress_review_validation": 263,
        "next_candidate_cost_benchmark_review": 264,
        "next_candidate_cost_benchmark_review_validation": 265,
        "next_candidate_vs_returned_candidate_comparison": 266,
        "next_candidate_vs_returned_candidate_comparison_validation": 267,
        "next_candidate_signal_robustness_review": 268,
        "next_candidate_signal_robustness_review_validation": 269,
        "next_candidate_overfit_window_sensitivity": 270,
        "next_candidate_overfit_window_sensitivity_validation": 271,
        "next_candidate_research_gate": 272,
        "next_candidate_research_gate_validation": 273,
        "next_candidate_owner_research_review_packet": 274,
        "next_candidate_owner_research_review_packet_validation": 275,
        "next_candidate_research_cycle_snapshot": 276,
        "next_candidate_research_cycle_snapshot_validation": 277,
        "executable_research_evidence_gap_ledger": 278,
        "executable_research_evidence_gap_ledger_validation": 279,
        "backfill_partial_root_cause_repair_plan": 280,
        "backfill_partial_root_cause_repair_plan_validation": 281,
        "signal_robustness_blocker_drilldown": 282,
        "signal_robustness_blocker_drilldown_validation": 283,
        "window_fragility_attribution": 284,
        "window_fragility_attribution_validation": 285,
        "stress_weakness_attribution": 286,
        "stress_weakness_attribution_validation": 287,
        "cost_benchmark_weakness_attribution": 288,
        "cost_benchmark_weakness_attribution_validation": 289,
        "candidate_redesign_hypothesis_v2": 290,
        "candidate_redesign_hypothesis_v2_validation": 291,
        "candidate_v2_spec_freeze": 292,
        "candidate_v2_spec_freeze_validation": 293,
        "candidate_v2_executable_binding_update": 294,
        "candidate_v2_executable_binding_update_validation": 295,
        "candidate_v2_mini_backfill": 296,
        "candidate_v2_mini_backfill_validation": 297,
        "candidate_v2_mini_gate": 298,
        "candidate_v2_mini_gate_validation": 299,
        "candidate_v2_full_backfill_if_approved": 300,
        "candidate_v2_full_backfill_if_approved_validation": 301,
        "candidate_v2_research_gate": 302,
        "candidate_v2_research_gate_validation": 303,
        "candidate_v2_owner_research_review_packet": 304,
        "candidate_v2_owner_research_review_packet_validation": 305,
        "candidate_v2_research_cycle_snapshot": 306,
        "candidate_v2_research_cycle_snapshot_validation": 307,
        "next_candidate_executable_binding_contract": 308,
        "next_candidate_executable_binding_contract_validation": 309,
        "next_candidate_signal_binding": 310,
        "next_candidate_signal_binding_validation": 311,
        "next_candidate_research_weight_binding": 312,
        "next_candidate_research_weight_binding_validation": 313,
        "executable_binding_safety_audit": 314,
        "executable_binding_safety_audit_validation": 315,
        "report_quality_gate": 316,
        "reader_brief_quality": 317,
        "artifact_catalog": 318,
    }
    artifact_id = _text(item.get("artifact_id"))
    return (order.get(artifact_id, 999), artifact_id)


def _navigation_purpose(artifact_id: str, status: str) -> str:
    if status.upper() in {"MISSING", "STALE", "REQUIRED_MISSING"}:
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
        "market_panel",
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
        "market_panel": "查看 benchmark、AI sector、risk 和 liquidity 代理实际涨跌。",
        "research_governance_summary": "确认 backtest/shadow/SEC PIT/weight 是否仍 observe-only。",
        "report_index": "检查报告 freshness、missing/stale 和 owner action。",
        "report_index_waiver_inventory": "检查 report index waivers 是否有人负责且未过期。",
        "report_index_waiver_inventory_validation": (
            "确认 expired waiver 和 missing registry reference 是否 fail-closed。"
        ),
        "reader_brief_consistency_pack": (
            "检查 Reader Brief-facing reports 是否使用统一 section 和 decision language。"
        ),
        "reader_brief_consistency_validation": (
            "确认 Reader Brief consistency pack 的 core section gate 是否通过。"
        ),
        "production_boundary_static_scan": (
            "检查 source/config/docs 是否出现 production-facing broker/order/secret-like terms。"
        ),
        "production_boundary_static_scan_validation": (
            "确认 production boundary static scan 是否 fail-closed 通过。"
        ),
        "owner_review_template_v2": (
            "查看 owner review v2 required fields、action enum 和 safety boundary。"
        ),
        "owner_review_template_v2_validation": (
            "确认 owner review template v2 contract 和可选 filled review 校验是否通过。"
        ),
        "owner_decision_audit_log": (
            "查看 append-only owner decision 记录、latest decision 和下游治理输入状态。"
        ),
        "owner_decision_audit_log_validation": (
            "确认 owner decision audit log schema、唯一 decision id 和安全边界是否通过。"
        ),
        "research_monthly_review_pack": (
            "查看月度 candidate research、paper-shadow、data governance 和 owner decision 汇总。"
        ),
        "research_monthly_review_pack_validation": (
            "确认月度 review pack 的 source coverage、Reader Brief section 和安全边界是否通过。"
        ),
        "paper_shadow_promotion_board": (
            "查看 paper-shadow promotion board decision、required evidence checklist 和 blocker。"
        ),
        "paper_shadow_promotion_board_validation": (
            "确认 promotion board schema、required evidence、Reader Brief section "
            "和安全边界是否通过。"
        ),
        "candidate_rejection_postmortem_template": (
            "查看 candidate rejection postmortem template 和 optional filled record 校验摘要。"
        ),
        "candidate_rejection_postmortem_template_validation": (
            "确认 rejection postmortem required sections、filled record 和安全边界是否通过。"
        ),
        "decision_snapshot_lifecycle_policy": (
            "查看 decision snapshot 是否按 as-of 到期、存在、缺失阻断或 latest-context 受限。"
        ),
        "decision_snapshot_lifecycle_policy_validation": (
            "确认 lifecycle policy 状态枚举、snapshot/date alignment 和不补造边界是否通过。"
        ),
        "extended_shadow_observation_clock": (
            "查看 extended-shadow observation clock 的 current/required count "
            "和 missing/invalid days。"
        ),
        "extended_shadow_observation_clock_validation": (
            "确认 observation clock count policy、Reader Brief section 和安全边界是否通过。"
        ),
        "extended_shadow_protocol": (
            "查看 extended shadow eligibility、minimum observation period 和 blocker。"
        ),
        "extended_shadow_protocol_validation": (
            "确认 extended shadow protocol schema、required evidence 和安全边界是否通过。"
        ),
        "research_roadmap_dashboard": (
            "查看 active/completed tasks、blockers、stale artifacts 和 latest governance states。"
        ),
        "research_roadmap_dashboard_validation": (
            "确认 research roadmap dashboard sections、Reader Brief fields 和只读边界是否通过。"
        ),
        "research_governance_end_to_end_pack": (
            "查看 task/register、waiver、Reader Brief、safety、owner、paper-shadow、roadmap "
            "和 lineage 的 end-to-end governance status。"
        ),
        "research_governance_end_to_end_pack_validation": (
            "确认 end-to-end governance pack source coverage、Reader Brief fields 和只读边界。"
        ),
        "research_governance_recovery_pack": (
            "查看 recovery governance status、remaining blockers/warnings、owner action "
            "和 normal/extended/live trading 边界。"
        ),
        "research_governance_recovery_pack_validation": (
            "确认 recovery governance pack source coverage、Reader Brief fields "
            "和 live trading forbidden 边界。"
        ),
        "eight_blocker_decision_review": (
            "查看 exact-eight remaining blockers、source fields 和 owner/code/data 分类。"
        ),
        "eight_blocker_decision_review_validation": (
            "确认 eight-blocker review 是否正好列出 8 个 blocker 且保持只读边界。"
        ),
        "normal_shadow_gate_gap_analysis": (
            "检查 normal paper-shadow resumption gate 仍被哪些 source/safety/owner 条件阻塞。"
        ),
        "promotion_blocker_after_metrics_review": (
            "查看成本、基准、owner、readiness 和 safety blockers 是否仍阻止 promotion。"
        ),
        "candidate_research_return_assessment": (
            "查看候选是否应继续 hold、return_to_research、reject 或等待 normal-shadow review。"
        ),
        "owner_decision_options_packet": (
            "查看 owner decision options；该 packet 不会 append owner audit log。"
        ),
        "owner_decision_dry_run": (
            "检查 proposed owner decision record 是否可验证且 real_entry_written=false。"
        ),
        "observation_clock_readiness_plan": (
            "确认 normal observation clock 何时才能开始；当前不会启动或递增 clock。"
        ),
        "post_decision_rerun_plan": (
            "查看 explicit owner decision 后应重跑哪些 governance reports。"
        ),
        "report_quality_warning_drilldown": (
            "下钻 report quality warnings 和 template alias 修复边界。"
        ),
        "governance_status_snapshot_after_decision_review": (
            "查看 TRADING-429~438 之后的 blocked/hold 边界和 recommended owner action。"
        ),
        "owner_return_to_research_decision_record": (
            "查看 TRADING-439 append-only owner return_to_research decision 和 audit validation。"
        ),
        "candidate_return_to_research_transition_pack": (
            "查看 candidate 如何从 normal paper-shadow resumption path 返回 research backlog。"
        ),
        "candidate_failure_mode_attribution": (
            "查看 cost、benchmark、readiness、owner 和 observation failure mode 排名。"
        ),
        "reusable_evidence_extraction": (
            "查看哪些 failed-candidate evidence 可复用、哪些已 invalidated 或 stale。"
        ),
        "return_to_research_hypothesis_backlog": (
            "查看 return-to-research 后的 P0/P1/P2 research hypotheses。"
        ),
        "next_candidate_spec_draft": (
            "查看下一轮 research-only candidate spec；该 spec 不激活 paper-shadow。"
        ),
        "research_backfill_plan_for_next_candidate": (
            "查看下一候选 backfill windows、metrics 和 pass/fail/needs-more-evidence 规则。"
        ),
        "archived_candidate_status_update": (
            "确认当前 candidate 是 RETURNED_TO_RESEARCH，而不是 rejected 或 live eligible。"
        ),
        "research_cycle_reset_pack": (
            "查看 failed recovery cycle 的关闭和下一 research cycle handoff。"
        ),
        "return_to_research_governance_snapshot": (
            "查看最终 return-to-research 状态、owner decision、candidate status 和安全边界。"
        ),
        "return_to_research_governance_snapshot_validation": (
            "确认 final return-to-research snapshot 的 no shadow/live/weights/broker 边界。"
        ),
        "next_research_cycle_intake": (
            "查看 return-to-research 后进入下一轮 research-only cycle 的 intake 证据。"
        ),
        "next_candidate_spec_frozen": (
            "查看 frozen next-candidate research spec；该 spec 不激活 paper-shadow。"
        ),
        "next_candidate_backfill": (
            "查看下一候选 binding-backed research-only backfill、真实 proxy metrics "
            "和 partial/blocking reasons。"
        ),
        "next_candidate_backfill_validation": (
            "确认 next-candidate backfill taxonomy、metric rows、data quality "
            "和 no official weights/broker 边界。"
        ),
        "next_candidate_stress_review": (
            "查看 stress/drawdown/flip evidence 对下一候选的 research-only 复核。"
        ),
        "next_candidate_stress_review_validation": (
            "确认 stress review 使用真实 backfill metrics、taxonomy 和安全边界。"
        ),
        "next_candidate_cost_benchmark_review": (
            "查看 cost survival 和 benchmark-relative blockers 是否已被新候选解决。"
        ),
        "next_candidate_cost_benchmark_review_validation": (
            "确认 cost/benchmark review 使用真实 backfill metrics 且保持 research-only。"
        ),
        "next_candidate_vs_returned_candidate_comparison": (
            "比较下一候选与 returned candidate；缺新指标时不得声明改善。"
        ),
        "next_candidate_vs_returned_candidate_comparison_validation": (
            "确认 vs-returned comparison 披露 repeated failure modes 和安全边界。"
        ),
        "next_candidate_signal_robustness_review": (
            "查看 executable signal binding 对 missing/partial/stale/schema/coverage "
            "输入是否 fail-closed。"
        ),
        "next_candidate_signal_robustness_review_validation": (
            "确认 signal robustness 使用 TRADING-467 taxonomy、源 binding 状态和安全边界。"
        ),
        "next_candidate_overfit_window_sensitivity": (
            "检查 executable backfill window metrics 是否显示 fragile/overfit 风险。"
        ),
        "next_candidate_overfit_window_sensitivity_validation": (
            "确认 window sensitivity 有真实 split metrics、fragility disclosure 和安全边界。"
        ),
        "next_candidate_research_gate": (
            "查看 TRADING-468 四态 research gate 决策、blockers 和 no paper-shadow 边界。"
        ),
        "next_candidate_research_gate_validation": (
            "确认 research gate source statuses、strongest evidence、blockers 和安全边界。"
        ),
        "next_candidate_owner_research_review_packet": (
            "查看 TRADING-469 owner options；该 packet 不 append owner decision。"
        ),
        "next_candidate_owner_research_review_packet_validation": (
            "确认 owner packet 含 continue/revise/return/reject/hold options 和安全边界。"
        ),
        "next_candidate_research_cycle_snapshot": (
            "查看 TRADING-470 executable research-cycle final snapshot "
            "和 no shadow/live/weights/broker 边界。"
        ),
        "next_candidate_research_cycle_snapshot_validation": (
            "确认 executable research-cycle snapshot source coverage 和安全边界。"
        ),
        "next_candidate_executable_binding_contract": (
            "查看 frozen next candidate 的 executable research-only binding contract；"
            "该 contract 不实现 strategy behavior。"
        ),
        "next_candidate_executable_binding_contract_validation": (
            "确认 executable binding contract 的 schema、required output types "
            "和 no shadow/live/weights/broker 边界。"
        ),
        "next_candidate_signal_binding": (
            "查看 frozen next candidate 的 research-only executable signal state；"
            "该 artifact 不生成 weights、backfill metrics、paper-shadow 或 broker/order。"
        ),
        "next_candidate_signal_binding_validation": (
            "确认 signal binding 的 validated-input、research-only metadata "
            "和 no official weights/broker/order 边界。"
        ),
        "next_candidate_research_weight_binding": (
            "查看 frozen next candidate 的 hypothetical research-only weights；"
            "该 artifact 不是 official target weights 或 broker/order。"
        ),
        "next_candidate_research_weight_binding_validation": (
            "确认 research weight binding 的 required fields、research_only metadata "
            "和 no official weights/broker/order 边界。"
        ),
        "executable_binding_safety_audit": (
            "检查 executable signal/weight binding 是否仍无 official weights、"
            "paper-shadow、broker/order、owner append 或 production mutation。"
        ),
        "executable_binding_safety_audit_validation": (
            "确认 executable binding safety audit 非 BLOCKED，且 static scan 无阻塞。"
        ),
        "research_safety_boundary_audit": (
            "检查 research artifacts 和 task scope 是否保持 no broker / no order / no production。"
        ),
        "research_safety_boundary_validation": (
            "确认 research safety boundary audit 是否 fail-closed 通过。"
        ),
        "task_register_consistency": (
            "检查 active/completed task register、docs link 和 registry 一致性。"
        ),
        "task_register_consistency_validation": (
            "确认 task register consistency report 是否 fail-closed 通过。"
        ),
        "artifact_lineage_graph": "检查 candidate research / paper-shadow artifact 依赖链。",
        "artifact_lineage_validation": (
            "确认 artifact lineage required families 和 edges 是否通过。"
        ),
        "report_quality_gate": "检查报告和 Reader Brief 是否披露基础可读 section。",
        "documentation_contract": "检查 registry 与 artifact catalog 契约覆盖。",
    }
    return reasons.get(artifact_id, "打开该 artifact 获取详细证据或治理上下文。")


_READER_CADENCE_OVERRIDES: dict[str, tuple[str, str, str]] = {
    "daily_score": ("daily", "daily", "下一个完整 U.S. equity trading day。"),
    "daily_decision_summary": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "reader_brief": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "next_candidate_executable_binding_contract": (
        "manual",
        "manual research cycle",
        "TRADING-460 contract 生成后、TRADING-461 signal binding 前校验。",
    ),
    "next_candidate_executable_binding_contract_validation": (
        "manual",
        "manual research cycle",
        "Executable binding contract 生成后立即校验 schema 和安全边界。",
    ),
    "next_candidate_signal_binding": (
        "manual",
        "manual research cycle",
        "TRADING-461 在 contract validation 和 validate-data 通过后生成。",
    ),
    "next_candidate_signal_binding_validation": (
        "manual",
        "manual research cycle",
        "Signal binding artifact 生成后立即校验 research-only 安全边界。",
    ),
    "next_candidate_research_weight_binding": (
        "manual",
        "manual research cycle",
        "TRADING-462 在 signal binding validation 和 validate-data 通过后生成。",
    ),
    "next_candidate_research_weight_binding_validation": (
        "manual",
        "manual research cycle",
        "Research weight binding artifact 生成后立即校验 research-only 安全边界。",
    ),
    "executable_binding_safety_audit": (
        "manual",
        "manual research cycle",
        "TRADING-463 在 executable binding backfill 前运行。",
    ),
    "executable_binding_safety_audit_validation": (
        "manual",
        "manual research cycle",
        "Safety audit artifact 生成后立即校验；BLOCKED 不得进入 backfill。",
    ),
    "next_candidate_backfill": (
        "manual",
        "manual research cycle",
        "TRADING-464 在 safety audit pass/acceptable warning 后运行 binding-backed backfill。",
    ),
    "next_candidate_backfill_validation": (
        "manual",
        "manual research cycle",
        "Backfill artifact 生成后立即校验 metric rows、taxonomy 和安全边界。",
    ),
    "next_candidate_stress_review": (
        "manual",
        "manual research cycle",
        "TRADING-465 在 binding-backed backfill complete/partial 后重新评估 stress scenarios。",
    ),
    "next_candidate_stress_review_validation": (
        "manual",
        "manual research cycle",
        "Stress review artifact 生成后立即校验 taxonomy、Reader Brief 和安全边界。",
    ),
    "next_candidate_cost_benchmark_review": (
        "manual",
        "manual research cycle",
        "TRADING-465 在真实 backfill metrics 可用后重新评估 cost/benchmark。",
    ),
    "next_candidate_cost_benchmark_review_validation": (
        "manual",
        "manual research cycle",
        "Cost/benchmark artifact 生成后立即校验 taxonomy、Reader Brief 和安全边界。",
    ),
    "next_candidate_vs_returned_candidate_comparison": (
        "manual",
        "manual research cycle",
        "TRADING-466 在 real backfill/stress/cost/benchmark metrics 后运行。",
    ),
    "next_candidate_vs_returned_candidate_comparison_validation": (
        "manual",
        "manual research cycle",
        "Vs-returned comparison 生成后立即校验 repeated failure disclosure。",
    ),
    "next_candidate_signal_robustness_review": (
        "manual",
        "manual research cycle",
        "TRADING-467 在 executable signal binding 和 backfill metrics 后运行。",
    ),
    "next_candidate_signal_robustness_review_validation": (
        "manual",
        "manual research cycle",
        "Signal robustness artifact 生成后立即校验 fail-closed disclosure。",
    ),
    "next_candidate_overfit_window_sensitivity": (
        "manual",
        "manual research cycle",
        "TRADING-467 在 binding-backed backfill metrics 后评估 window stability。",
    ),
    "next_candidate_overfit_window_sensitivity_validation": (
        "manual",
        "manual research cycle",
        "Window sensitivity artifact 生成后立即校验 split metrics 和安全边界。",
    ),
    "next_candidate_research_gate": (
        "manual",
        "manual research cycle",
        "TRADING-468 在 executable binding 和 real metrics review 完成后运行。",
    ),
    "next_candidate_research_gate_validation": (
        "manual",
        "manual research cycle",
        "Research gate artifact 生成后立即校验四态 taxonomy、evidence 和安全边界。",
    ),
    "next_candidate_owner_research_review_packet": (
        "manual",
        "manual research cycle",
        "TRADING-469 在 research gate rerun 后准备 owner options packet。",
    ),
    "next_candidate_owner_research_review_packet_validation": (
        "manual",
        "manual research cycle",
        "Owner packet artifact 生成后立即校验 option set、no-append 和安全边界。",
    ),
    "next_candidate_research_cycle_snapshot": (
        "manual",
        "manual research cycle",
        "TRADING-470 在 owner packet 后生成 executable research-cycle final snapshot。",
    ),
    "next_candidate_research_cycle_snapshot_validation": (
        "manual",
        "manual research cycle",
        "Executable research-cycle snapshot 生成后立即校验 source coverage 和安全边界。",
    ),
    "executable_research_evidence_gap_ledger": (
        "manual",
        "manual research cycle",
        "TRADING-471 在 executable research-cycle snapshot 后生成 non-aggregated gap ledger。",
    ),
    "executable_research_evidence_gap_ledger_validation": (
        "manual",
        "manual research cycle",
        "Evidence gap ledger 生成后立即校验 source coverage、gap 分类和安全边界。",
    ),
    "backfill_partial_root_cause_repair_plan": (
        "manual",
        "manual research cycle",
        "TRADING-472 在 evidence gap ledger 后解释 partial backfill 根因和修复性。",
    ),
    "backfill_partial_root_cause_repair_plan_validation": (
        "manual",
        "manual research cycle",
        "Backfill repair plan 生成后立即校验窗口覆盖、repairability 和安全边界。",
    ),
    "signal_robustness_blocker_drilldown": (
        "manual",
        "manual research cycle",
        "TRADING-473 在 backfill repair plan 后下钻 signal robustness blockers。",
    ),
    "signal_robustness_blocker_drilldown_validation": (
        "manual",
        "manual research cycle",
        "Signal drilldown 生成后立即校验 blocker 字段、repairability 和安全边界。",
    ),
    "window_fragility_attribution": (
        "manual",
        "manual research cycle",
        "TRADING-474 在 signal drilldown 后归因 window fragility 和 overfit/under-observed。",
    ),
    "window_fragility_attribution_validation": (
        "manual",
        "manual research cycle",
        "Window attribution 生成后立即校验 split 覆盖、failure modes 和安全边界。",
    ),
    "stress_weakness_attribution": (
        "manual",
        "manual research cycle",
        "TRADING-475 在 window attribution 后归因 stress weakness 和 redesign/reject 判断。",
    ),
    "stress_weakness_attribution_validation": (
        "manual",
        "manual research cycle",
        "Stress attribution 生成后立即校验 scenario 覆盖、root causes 和安全边界。",
    ),
    "cost_benchmark_weakness_attribution": (
        "manual",
        "manual research cycle",
        "TRADING-476 在 stress attribution 后归因 cost/benchmark weakness "
        "和 redesign repairability。",
    ),
    "cost_benchmark_weakness_attribution_validation": (
        "manual",
        "manual research cycle",
        "Cost/benchmark attribution 生成后立即校验 cost scenarios、baselines 和安全边界。",
    ),
    "candidate_redesign_hypothesis_v2": (
        "manual",
        "manual research cycle",
        "TRADING-477 在 cost/benchmark attribution 后生成 v2 redesign hypotheses。",
    ),
    "candidate_redesign_hypothesis_v2_validation": (
        "manual",
        "manual research cycle",
        "Candidate redesign hypotheses 生成后立即校验 target 覆盖、priority 和安全边界。",
    ),
    "candidate_v2_spec_freeze": (
        "manual",
        "manual research cycle",
        "TRADING-478 在 redesign hypotheses 后冻结 research-only v2 spec。",
    ),
    "candidate_v2_spec_freeze_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 spec freeze 后立即校验 spec fields、differences 和安全边界。",
    ),
    "candidate_v2_executable_binding_update": (
        "manual",
        "manual research cycle",
        "TRADING-479 在 frozen v2 spec 后生成 research-only v2 signal/weight binding。",
    ),
    "candidate_v2_executable_binding_update_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 binding update 后立即校验 signal/weight rows 和安全边界。",
    ),
    "candidate_v2_mini_backfill": (
        "manual",
        "manual research cycle",
        "TRADING-480 在 v2 binding safety pass 或 acceptable warning 后运行 "
        "compact mini backfill。",
    ),
    "candidate_v2_mini_backfill_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 mini backfill 后立即校验 representative windows、metrics 和安全边界。",
    ),
    "candidate_v2_mini_gate": (
        "manual",
        "manual research cycle",
        "TRADING-481 在 mini backfill 后决定是否允许 full backfill。",
    ),
    "candidate_v2_mini_gate_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 mini gate 后立即校验 hard stop、evidence rows 和安全边界。",
    ),
    "candidate_v2_full_backfill_if_approved": (
        "manual",
        "manual research cycle",
        "TRADING-482 只在 mini gate proceed 时进入 full backfill，否则输出 blocked。",
    ),
    "candidate_v2_full_backfill_if_approved_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 full backfill artifact 后立即校验 gate stop 和安全边界。",
    ),
    "candidate_v2_research_gate": (
        "manual",
        "manual research cycle",
        "TRADING-483 在 full-backfill artifact 后输出 v2 research gate。",
    ),
    "candidate_v2_research_gate_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 research gate 后立即校验 decision、evidence 和安全边界。",
    ),
    "candidate_v2_owner_research_review_packet": (
        "manual",
        "manual research cycle",
        "TRADING-484 在 v2 research gate 后准备 owner research options；不写 owner decision。",
    ),
    "candidate_v2_owner_research_review_packet_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 owner packet 后立即校验 options、Reader Brief 和安全边界。",
    ),
    "candidate_v2_research_cycle_snapshot": (
        "manual",
        "manual research cycle",
        "TRADING-485 收集 candidate v2 research-only cycle artifacts 并输出 final snapshot。",
    ),
    "candidate_v2_research_cycle_snapshot_validation": (
        "manual",
        "manual research cycle",
        "Candidate v2 research-cycle snapshot 后立即校验 source coverage 和安全边界。",
    ),
    "task_register_consistency": (
        "daily",
        "daily / manual governance",
        "Governance pack 或 task register 变更后运行。",
    ),
    "task_register_consistency_validation": (
        "daily",
        "daily / manual governance",
        "Task register consistency report 生成后立即校验。",
    ),
    "artifact_lineage_graph": (
        "daily",
        "daily",
        "Report index 前生成 candidate research artifact dependency chain。",
    ),
    "artifact_lineage_validation": (
        "daily",
        "daily",
        "Report index 前 fail-closed 校验 lineage required families / edges。",
    ),
    "report_quality_gate": (
        "daily",
        "daily",
        "Reader Brief 生成后校验 report / Reader Brief section。",
    ),
    "reader_brief_quality": ("daily", "daily", "Reader Brief 生成后立即校验。"),
    "market_panel": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "score_change_attribution": ("daily", "daily", "随 daily-run 每个交易日对比上一信号日。"),
    "report_index": ("daily", "daily", "随 daily-run 每个交易日扫描 latest artifacts。"),
    "report_index_waiver_inventory": (
        "daily",
        "daily / governance",
        "Report index 前或 governance pack 前确认 waiver 未过期。",
    ),
    "report_index_waiver_inventory_validation": (
        "daily",
        "daily / governance",
        "Waiver inventory 生成后立即校验 expired waiver。",
    ),
    "reader_brief_consistency_pack": (
        "daily",
        "daily / governance",
        "Reader Brief 生成和 report index 刷新后检查 section consistency。",
    ),
    "reader_brief_consistency_validation": (
        "daily",
        "daily / governance",
        "Reader Brief consistency pack 生成后立即校验 core section contract。",
    ),
    "production_boundary_static_scan": (
        "daily",
        "daily / governance",
        "Governance pack 或 source/config/docs safety-sensitive 变更后运行。",
    ),
    "production_boundary_static_scan_validation": (
        "daily",
        "daily / governance",
        "Production boundary static scan 生成后立即校验 blocking findings。",
    ),
    "owner_review_template_v2": (
        "daily",
        "daily / manual governance",
        "Owner review template 或 manual review policy 变更后运行。",
    ),
    "owner_review_template_v2_validation": (
        "daily",
        "daily / manual governance",
        "Owner review template v2 生成后立即校验 contract。",
    ),
    "owner_decision_audit_log": (
        "daily",
        "manual / monthly governance",
        "Owner decision append 后、monthly review 或 promotion board 前生成 report。",
    ),
    "owner_decision_audit_log_validation": (
        "daily",
        "manual / monthly governance",
        "Owner decision audit log report 生成后立即校验 append-only boundary。",
    ),
    "research_monthly_review_pack": (
        "monthly",
        "monthly / manual governance",
        "Owner 月度 research review、promotion board 准备或重要 source artifact 更新后生成。",
    ),
    "research_monthly_review_pack_validation": (
        "monthly",
        "monthly / manual governance",
        "Monthly review pack 生成后立即校验 source coverage 和 safety boundary。",
    ),
    "paper_shadow_promotion_board": (
        "monthly",
        "manual promotion board",
        "Monthly review pack、owner decision audit log、safety audit 和 lineage 更新后生成。",
    ),
    "paper_shadow_promotion_board_validation": (
        "monthly",
        "manual promotion board",
        "Paper-shadow promotion board 生成后立即校验 evidence checklist 和安全边界。",
    ),
    "candidate_rejection_postmortem_template": (
        "monthly",
        "manual rejection review",
        (
            "Promotion board 或 owner decision 输出 reject / return 后，或 template "
            "contract 变更后生成。"
        ),
    ),
    "candidate_rejection_postmortem_template_validation": (
        "monthly",
        "manual rejection review",
        "Candidate rejection postmortem template 或 filled record 生成后立即校验。",
    ),
    "decision_snapshot_lifecycle_policy": (
        "daily",
        "daily reader context / governance",
        "Daily scoring window、Reader Brief 或 recovery governance pack "
        "需要判断 snapshot 缺失行为时生成。",
    ),
    "decision_snapshot_lifecycle_policy_validation": (
        "daily",
        "daily reader context / governance",
        "Decision snapshot lifecycle policy 生成后立即校验状态、日期和不补造边界。",
    ),
    "extended_shadow_observation_clock": (
        "monthly",
        "manual extended shadow review",
        "Paper-shadow weekly review、promotion board 或 extended shadow protocol 准备后生成。",
    ),
    "extended_shadow_observation_clock_validation": (
        "monthly",
        "manual extended shadow review",
        "Extended-shadow observation clock 生成后立即校验 count policy 和安全边界。",
    ),
    "extended_shadow_protocol": (
        "monthly",
        "manual extended shadow review",
        "Promotion board、observation clock、owner decision、safety audit 和 lineage 更新后生成。",
    ),
    "extended_shadow_protocol_validation": (
        "monthly",
        "manual extended shadow review",
        "Extended shadow protocol 生成后立即校验 evidence checklist 和安全边界。",
    ),
    "research_roadmap_dashboard": (
        "monthly",
        "manual roadmap review",
        "Task register、report index 或 governance report 更新后生成。",
    ),
    "research_roadmap_dashboard_validation": (
        "monthly",
        "manual roadmap review",
        "Research roadmap dashboard 生成后立即校验 sections 和只读安全边界。",
    ),
    "research_governance_end_to_end_pack": (
        "monthly",
        "manual governance pack",
        "TRADING-362 到 TRADING-382 governance artifacts 更新后生成。",
    ),
    "research_governance_end_to_end_pack_validation": (
        "monthly",
        "manual governance pack",
        "Research governance end-to-end pack 生成后立即校验 source coverage 和安全边界。",
    ),
    "research_governance_recovery_pack": (
        "manual",
        "manual recovery governance pack",
        "TRADING-385 到 TRADING-399 recovery artifacts 更新后生成。",
    ),
    "research_governance_recovery_pack_validation": (
        "manual",
        "manual recovery governance pack",
        "Research governance recovery pack 生成后立即校验 source coverage 和 live boundary。",
    ),
    "eight_blocker_decision_review": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 前置或同步生成。",
    ),
    "eight_blocker_decision_review_validation": (
        "manual",
        "manual decision-stage governance",
        "Eight-blocker decision review 生成后立即校验 exact-eight 和安全边界。",
    ),
    "normal_shadow_gate_gap_analysis": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成。",
    ),
    "promotion_blocker_after_metrics_review": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成。",
    ),
    "candidate_research_return_assessment": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成。",
    ),
    "owner_decision_options_packet": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成；不 append owner log。",
    ),
    "owner_decision_dry_run": (
        "manual",
        "manual decision-stage governance",
        "Owner decision option review 时 dry-run；real_entry_written 必须为 false。",
    ),
    "observation_clock_readiness_plan": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成；不启动 observation clock。",
    ),
    "post_decision_rerun_plan": (
        "manual",
        "manual decision-stage governance",
        "Explicit owner decision 后按匹配分支执行；本报告只列计划。",
    ),
    "report_quality_warning_drilldown": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 生成时同步生成 warning drilldown。",
    ),
    "governance_status_snapshot_after_decision_review": (
        "manual",
        "manual decision-stage governance",
        "TRADING-429~438 decision-stage review 结束后生成最终 blocked/hold snapshot。",
    ),
    "research_safety_boundary_audit": (
        "daily",
        "daily / governance",
        "Report index 刷新后检查 research safety boundary 和 future promotion 输入。",
    ),
    "research_safety_boundary_validation": (
        "daily",
        "daily / governance",
        "Research safety boundary audit 生成后立即校验 unsafe positive signals。",
    ),
    "research_governance_summary": (
        "daily",
        "daily / weekly review",
        "daily 汇总；weekly 复核 governance 队列。",
    ),
    "backtest_daily": (
        "weekly",
        "weekly or scoring policy change",
        "每周或 scoring policy change 后运行。",
    ),
    "backtest_robustness": (
        "weekly",
        "weekly or scoring policy change",
        "每周或 scoring policy change 后运行。",
    ),
    "parameter_governance": ("weekly", "weekly", "每周复核参数候选和 owner input。"),
    "weight_candidate_evaluation": (
        "bi_weekly",
        "biweekly",
        "每两周评估候选权重；缺样本时记录 INSUFFICIENT_DATA。",
    ),
    "weight_promotion_gate": (
        "bi_weekly",
        "biweekly after candidate evaluation",
        "candidate evaluation 完成后每两周运行；缺 artifact 时 promotion blocked。",
    ),
    "documentation_contract": (
        "governance",
        "weekly / registry change",
        "每周或 registry/artifact catalog 变更后运行。",
    ),
    "artifact_catalog_consistency": (
        "governance",
        "monthly / artifact contract change",
        "monthly governance 或 artifact contract 变更后复核。",
    ),
}


def _reader_cadence_override(report: Mapping[str, Any]) -> tuple[str, str, str] | None:
    report_id = _text(report.get("report_id"))
    return _READER_CADENCE_OVERRIDES.get(report_id)


def _reader_cadence_label(report: Mapping[str, Any]) -> str:
    override = _reader_cadence_override(report)
    if override:
        return override[1]
    return _normalize_cadence(_text(report.get("cadence"), "ad_hoc"))


def _reader_next_expected_run(report: Mapping[str, Any], *, fallback: str) -> str:
    override = _reader_cadence_override(report)
    if override:
        return override[2]
    return fallback


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
    override = _reader_cadence_override(report)
    if override:
        return override[0]
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
    *,
    display_value: str | None = None,
) -> dict[str, Any]:
    metric = _mapping(metrics.get(metric_id))
    source_artifacts = _records(metric.get("source_artifacts"))
    raw_value = metric.get("value")
    return {
        "metric_id": metric_id,
        "label": _text(metric.get("audience_label"), metric_id),
        "current_value": (
            display_value if display_value is not None else _text(raw_value, fallback_value)
        ),
        "audit_value": fallback_value if raw_value is None or raw_value == "" else raw_value,
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


def _binding_gate_value(gate: Mapping[str, Any] | None) -> str:
    if not gate:
        return "UNKNOWN"
    label = _text(gate.get("label"), _text(gate.get("gate_id"), "gate"))
    cap = _format_percent(gate.get("max_position"))
    return f"{label} -> {cap}" if cap != "UNKNOWN" else label


def _portfolio_limit_value(positions: Mapping[str, Any]) -> str:
    for gate in _records(positions.get("position_gates")):
        gate_id = _text(gate.get("gate_id")).lower()
        if gate_id in {"portfolio_limit", "portfolio_limits", "portfolio_risk_budget"}:
            cap = _format_percent(gate.get("max_position"))
            return f"≤{cap}" if cap != "UNKNOWN" else _text(gate.get("label"), "UNKNOWN")
    total_band = _format_band(_mapping(positions.get("final_total_risk_asset_band")))
    return total_band if total_band != "UNKNOWN" else "not separately disclosed"


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


def _read_optional_jsonl(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


def _read_optional_yaml_mapping(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError:
        return {}
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


def _format_number(value: object, *, digits: int = 2) -> str:
    number = _float_or_none(value)
    if number is None:
        return _text(value, "UNKNOWN")
    return f"{number:.{digits}f}"


def _format_signed_number(value: object, *, digits: int = 2) -> str:
    number = _float_or_none(value)
    if number is None:
        return _text(value, "UNKNOWN")
    return f"{number:+.{digits}f}"


def _format_percent(value: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "UNKNOWN"
    return f"{number:.0%}"


def _format_market_change(value: object, change_mode: object) -> str:
    number = _float_or_none(value)
    if number is None:
        return "UNKNOWN"
    if change_mode == "difference":
        return f"{number:+.4f}pp"
    return f"{number:+.2%}"


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


def _path_or_fallback(value: object, fallback: Path | None) -> Path | None:
    text = str(value).strip() if value is not None else ""
    return Path(text) if text else fallback


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


def _day_label(value: object) -> str:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = 0
    return "day" if count == 1 else "days"


def _is_are(value: object) -> str:
    try:
        count = int(value)
    except (TypeError, ValueError):
        count = 0
    return "is" if count == 1 else "are"


_BADGE_VALUES = {
    "ACTION_REQUIRED",
    "AVAILABLE",
    "BLOCKED",
    "BLOCKING",
    "BLOCKED_BY_DATA_QUALITY",
    "BLOCKED_BY_MANUAL_REVIEW",
    "BLOCKED_BY_MISSING_ARTIFACTS",
    "CRITICAL",
    "DOCUMENTATION",
    "FAILED",
    "FAIL",
    "FALSE",
    "FRESH",
    "INFO",
    "IMPORTANT",
    "LIMITED",
    "LIMITED_CONTEXT",
    "LIMITED_READER_CONTEXT",
    "MANUAL_REVIEW_REQUIRED",
    "MISSING",
    "MISSING_MARKET_PRICE_DATA",
    "NOT_PROMOTABLE",
    "OK",
    "OPTIONAL",
    "PASS",
    "PASS_WITH_LIMITATIONS",
    "PASS_WITH_WARNINGS",
    "PROMOTABLE",
    "REGISTRY_FALLBACK",
    "REQUIRED_MISSING",
    "READY_FOR_READING",
    "REVIEW_WITH_LIMITATIONS",
    "STALE",
    "TRUE",
    "WARNING",
}


def _value_html(label: object, value: object, *, default: str = "UNKNOWN") -> str:
    label_text = _text(label).lower()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        digits = 1 if "score" in label_text and "delta" not in label_text else 2
        return html.escape(_format_number(value, digits=digits))
    text = _text(value, default)
    if label_text == "production_effect":
        return _status_badge(text)
    status_label = label_text.endswith("status") or label_text in {
        "availability",
        "impact_level",
        "freshness_status",
        "triggered",
    }
    if (status_label and _is_badge_value(text)) or _is_badge_value(text):
        return _status_badge(text)
    return html.escape(text)


def _status_badge(value: object) -> str:
    text = _text(value, "UNKNOWN")
    normalized = text.upper()
    if normalized == "NONE":
        label = "production_effect=none"
        class_name = "production-none"
    elif normalized == "BINDING GATE":
        label = "binding gate"
        class_name = "binding-gate"
    elif not _is_badge_value(text):
        label = text
        class_name = "custom"
    else:
        label = text
        class_name = _css_token(normalized)
    return (
        f'<span class="status-badge status-{html.escape(class_name)}">{html.escape(label)}</span>'
    )


def _is_badge_value(value: str) -> bool:
    normalized = value.upper()
    return normalized in _BADGE_VALUES or normalized == "NONE"


def _leading_status(value: object) -> str:
    text = _text(value, "UNKNOWN").strip()
    for separator in ("；", ";", "，", ",", " "):
        if separator in text:
            text = text.split(separator, maxsplit=1)[0]
            break
    return text or "UNKNOWN"


def _css_token(value: str) -> str:
    chars = [char.lower() if char.isalnum() else "-" for char in value]
    token = "".join(chars).strip("-")
    while "--" in token:
        token = token.replace("--", "-")
    return token or "unknown"


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
    governance = html.escape(_text(summary.get("research_governance_summary"), "UNKNOWN"))
    return (
        '<div class="narrative">'
        f"<p><strong>今日结论：</strong>{today}</p>"
        f"<p><strong>为什么：</strong>{why}</p>"
        f"<p><strong>研究治理：</strong>{governance}</p>"
        f"<p><strong>需要复核：</strong>{review}</p>"
        "</div>"
    )


def _top_summary_cards(
    *,
    decision: Mapping[str, Any],
    market: Mapping[str, Any],
    manual_review: Mapping[str, Any],
    governance: Mapping[str, Any],
    status_panel: Mapping[str, Any],
    payload_status: str,
    production_effect: str,
) -> str:
    manual_items = _records(manual_review.get("items"))
    critical_count = sum(
        _int(group.get("count"))
        for group in _records(manual_review.get("groups"))
        if _text(group.get("severity")) == "critical"
    )
    cards = [
        {
            "label": "Final Action",
            "value": _text(decision.get("action"), "UNKNOWN"),
            "detail": (
                "decision_usability="
                f"{_text(status_panel.get('decision_usability'), payload_status)}"
            ),
            "badge": _text(status_panel.get("decision_usability"), payload_status),
            "class": "summary-card--decision",
        },
        {
            "label": "Final AI Position",
            "value": _text(decision.get("final_risk_asset_ai_position"), "UNKNOWN"),
            "detail": _text(decision.get("total_risk_asset_budget"), "UNKNOWN"),
            "badge": _leading_status(decision.get("data_gate")),
            "class": "summary-card--position",
        },
        {
            "label": "Binding Gate",
            "value": _text(decision.get("binding_gate_label"), "UNKNOWN"),
            "detail": _text(decision.get("binding_gate_reason"), "打开 gate ladder 查看约束来源。"),
            "badge": "binding gate",
            "class": "summary-card--binding",
        },
        {
            "label": "Market Movement",
            "value": _text(market.get("market_movement_sentence"), "MISSING"),
            "detail": _text(market.get("market_price_panel_status"), "UNKNOWN"),
            "badge": _text(market.get("market_price_panel_status"), "UNKNOWN"),
            "class": "summary-card--market",
        },
        {
            "label": "Manual Review",
            "value": _text(len(manual_items)),
            "detail": f"critical={critical_count}",
            "badge": "ACTION_REQUIRED" if manual_items else "OK",
            "class": "summary-card--review",
        },
        {
            "label": "Production Effect",
            "value": f"production_effect={production_effect}",
            "detail": (
                f"研究晋升：{_text(status_panel.get('research_promotion_status'), 'UNKNOWN')}"
            ),
            "badge": production_effect,
            "extra_badge": _text(
                status_panel.get("research_promotion_status"),
                _text(governance.get("promotion_status"), "UNKNOWN"),
            ),
            "class": "summary-card--safety",
        },
    ]
    rendered = []
    for card in cards:
        badges = _status_badge(_text(card.get("badge"), "UNKNOWN"))
        extra_badge = _text(card.get("extra_badge"))
        if extra_badge:
            badges += _status_badge(extra_badge)
        rendered.append(
            '<article class="summary-card {}">'.format(html.escape(_text(card.get("class"))))
            + f"<div>{html.escape(_text(card.get('label')))}</div>"
            + f"<strong>{html.escape(_text(card.get('value'), 'UNKNOWN'))}</strong>"
            + f"<p>{html.escape(_text(card.get('detail'), 'UNKNOWN'))}</p>"
            + f'<div class="badge-row">{badges}</div>'
            + "</article>"
        )
    return '<div class="summary-card-grid">' + "\n".join(rendered) + "</div>"


def _status_panel_header(status_panel: Mapping[str, Any], fallback_status: str) -> str:
    build = _text(status_panel.get("build_status"), fallback_status)
    usability = _text(status_panel.get("decision_usability"), "UNKNOWN")
    promotion = _text(status_panel.get("research_promotion_status"), "UNKNOWN")
    return (
        '<div class="status-strip">'
        f"<span>Reader Brief Build Status: {_status_badge(build)}</span>"
        f"<span>Decision Usability: {_status_badge(usability)}</span>"
        f"<span>Research Promotion Status: {_status_badge(promotion)}</span>"
        "</div>"
    )


def _status_panel_html(status_panel: Mapping[str, Any]) -> str:
    if not status_panel:
        return ""
    cards = [
        (
            "Reader Brief Build Status",
            status_panel.get("build_status"),
            status_panel.get("build_status_explanation"),
        ),
        (
            "Decision Usability",
            status_panel.get("decision_usability"),
            status_panel.get("decision_usability_explanation"),
        ),
        (
            "Research Promotion Status",
            status_panel.get("research_promotion_status"),
            status_panel.get("research_promotion_explanation"),
        ),
    ]
    return (
        '<div class="status-panel">'
        + "\n".join(
            '<article class="status-panel-card">'
            f"<div>{html.escape(label)}</div>"
            f"<strong>{_status_badge(value)}</strong>"
            f"<p>{html.escape(_text(detail, 'UNKNOWN'))}</p>"
            "</article>"
            for label, value, detail in cards
        )
        + "</div>"
    )


def _action_checklist_html(items: list[dict[str, Any]]) -> str:
    if not items:
        return ""
    rows = []
    for item in items:
        rows.append(
            "<li>"
            f"<span>{html.escape(_text(item.get('priority')))}</span>"
            "<div>"
            f"<strong>{html.escape(_text(item.get('action'), 'UNKNOWN'))}</strong>"
            f"<p>{html.escape(_text(item.get('rationale'), 'UNKNOWN'))}</p>"
            f"{_status_badge(_text(item.get('status'), 'UNKNOWN'))}"
            "</div>"
            "</li>"
        )
    return '<h3>今日建议动作</h3><ol class="action-checklist">' + "\n".join(rows) + "</ol>"


def _score_change_narrative_html(summary: Mapping[str, Any]) -> str:
    if not summary:
        return ""
    return (
        '<div class="narrative callout">'
        f"<p>{html.escape(_text(summary.get('summary'), 'UNKNOWN'))}</p>"
        f"<p>{html.escape(_text(summary.get('position_interpretation'), 'UNKNOWN'))}</p>"
        "</div>"
    )


def _artifact_impact_summary_html(records: list[dict[str, Any]]) -> str:
    if not records:
        return ""
    cards = []
    for record in records:
        cards.append(
            '<article class="impact-summary-card">'
            f"<div>{html.escape(_text(record.get('chain'), 'UNKNOWN'))}</div>"
            f"<strong>{_status_badge(_text(record.get('status'), 'UNKNOWN'))}</strong>"
            f"<p>{html.escape(_text(record.get('interpretation'), 'UNKNOWN'))}</p>"
            f"<small>missing={html.escape(_text(record.get('missing_count'), '0'))}</small>"
            "</article>"
        )
    return '<div class="impact-summary-grid">' + "\n".join(cards) + "</div>"


def _top_review_items_html(manual_review: Mapping[str, Any]) -> str:
    top_items = _records(manual_review.get("top_items"))
    if not top_items:
        return "<h3>Top 3 Review Items Today</h3><p>无优先复核项。</p>"
    return "<h3>Top 3 Review Items Today</h3>" + _manual_review_table(top_items)


def _manual_review_impact_groups_html(manual_review: Mapping[str, Any]) -> str:
    groups = _records(manual_review.get("impact_groups"))
    if not groups:
        return ""
    parts = ["<h3>按影响类型收敛</h3>"]
    for group in groups:
        label = html.escape(_text(group.get("label"), "UNKNOWN"))
        impact_type = html.escape(_css_token(_text(group.get("impact_type"), "audit_observe")))
        parts.append(
            f'<div class="review-impact-group review-impact-{impact_type}">'
            f"<h4>{label} ({html.escape(_text(group.get('count'), '0'))})</h4>"
            + _manual_review_table(_records(group.get("items")))
            + "</div>"
        )
    return "\n".join(parts)


def _market_proxy_cards(market: Mapping[str, Any]) -> str:
    rows = _records(market.get("proxy_rows"))
    rows_by_symbol = {_normalize_market_symbol(row.get("symbol")): row for row in rows}
    cards = []
    for symbol in ("SPY", "QQQ", "SMH", "SOXX", "VIX", "DGS10"):
        row = rows_by_symbol.get(symbol, {})
        status = _text(row.get("data_status"), "MISSING")
        cards.append(
            '<article class="market-card">'
            f"<div>{html.escape(symbol)}</div>"
            f"<strong>{html.escape(_text(row.get('last_price'), 'MISSING'))}</strong>"
            '<dl class="market-metrics">'
            f"<dt>1D</dt><dd>{html.escape(_text(row.get('return_1d'), 'MISSING'))}</dd>"
            f"<dt>5D</dt><dd>{html.escape(_text(row.get('return_5d'), 'MISSING'))}</dd>"
            f"<dt>20D</dt><dd>{html.escape(_text(row.get('return_20d'), 'MISSING'))}</dd>"
            "</dl>"
            + _status_badge(status)
            + f"<p>{html.escape(_text(row.get('risk_interpretation'), '未提供该 proxy。'))}</p>"
            + "</article>"
        )
    return '<div class="market-card-grid">' + "\n".join(cards) + "</div>"


def _normalize_market_symbol(value: object) -> str:
    text = _text(value).upper()
    return text[1:] if text.startswith("^") else text


def _funnel_flow(records: list[dict[str, Any]], decision: Mapping[str, Any]) -> str:
    by_metric = {_text(record.get("metric_id")): record for record in records}
    sequence = [
        ("overall_score", "综合评分"),
        ("model_position_band", "评分映射仓位"),
        ("confidence_adjusted_position", "置信度调整"),
        ("portfolio_limit", "组合上限"),
        ("position_gate", "估值/最严闸门"),
        ("final_position_band", "最终仓位"),
    ]
    nodes = []
    for metric_id, label in sequence:
        record = by_metric.get(metric_id, {})
        is_binding = metric_id == "position_gate"
        node_classes = "funnel-node" + (" binding" if is_binding else "")
        value = _text(record.get("current_value"), "MISSING")
        detail = (
            _text(decision.get("binding_gate_label"), "UNKNOWN")
            if is_binding
            else _text(record.get("source_field"), metric_id)
        )
        badge = _status_badge("binding gate") if is_binding else ""
        nodes.append(
            f'<div class="{node_classes}">'
            f"<span>{html.escape(label)}</span>"
            f"<strong>{html.escape(value)}</strong>"
            f"<small>{html.escape(detail)}</small>"
            f"{badge}"
            "</div>"
        )
    return '<div class="funnel-flow">' + "\n".join(nodes) + "</div>"


def _gate_ladder_html(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>无可用 gate 记录。</p>"
    rows = []
    for record in records:
        is_binding = bool(record.get("binding"))
        row_class = ' class="binding-row"' if is_binding else ""
        state_badge = (
            _status_badge("binding gate")
            if is_binding
            else _status_badge(_text(record.get("triggered"), "UNKNOWN"))
        )
        rows.append(
            f"<tr{row_class}>"
            f"<td>{html.escape(_text(record.get('gate_id')))}</td>"
            f"<td>{html.escape(_text(record.get('label')))}</td>"
            f"<td>{html.escape(_text(record.get('cap'), 'UNKNOWN'))}</td>"
            f"<td>{state_badge}</td>"
            f"<td>{html.escape(_text(record.get('source')))}</td>"
            f"<td>{html.escape(_text(record.get('reason')))}</td>"
            f"<td>{html.escape(_text(record.get('release_condition')))}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr><th>gate_id</th><th>label</th><th>cap</th>"
        "<th>state</th><th>source</th><th>reason</th><th>release_condition</th>"
        "</tr></thead><tbody>" + "\n".join(rows) + "</tbody></table>"
    )


def _artifact_impact_sections(records: list[dict[str, Any]]) -> str:
    if not records:
        return "<p>未发现缺失或受限 artifact。</p>"
    parts: list[str] = []
    for level in ("BLOCKING", "IMPORTANT", "OPTIONAL", "INFO"):
        subset = [record for record in records if _text(record.get("impact_level")) == level]
        if not subset:
            continue
        parts.append(
            f'<div class="impact-group impact-{_css_token(level)}">'
            f"<h3>{_status_badge(level)} {html.escape(level.title())}</h3>"
            + _artifact_impact_table(subset)
            + "</div>"
        )
    return "\n".join(parts)


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
            f"<td>{_status_badge(_text(record.get('status'), 'UNKNOWN'))}</td>"
            f"<td>{_status_badge(_text(record.get('impact_level'), 'INFO'))}</td>"
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
        severity = _text(group.get("severity"), "info")
        parts.append(
            f'<div class="review-group review-{html.escape(_css_token(severity))}">'
            f"<h3>{_status_badge(severity)} {label}</h3>"
        )
        parts.append(_manual_review_table(_records(group.get("items"))))
        parts.append("</div>")
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
            '<td><strong class="recommended-action">'
            f"{html.escape(_text(record.get('recommended_next_action')))}</strong></td>"
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
                ("navigation_sources", record.get("navigation_sources")),
            ]
        )
        rows.append(
            "<tr>"
            f"<td>{html.escape(_text(record.get('artifact_id')))}</td>"
            f"<td>{html.escape(_text(record.get('short_name'), _text(record.get('title'))))}</td>"
            f"<td>{_status_badge(_text(record.get('status'), 'UNKNOWN'))}</td>"
            f"<td>{_status_badge(_text(record.get('freshness_status'), 'UNKNOWN'))}</td>"
            f"<td>{_status_badge(_text(record.get('production_effect'), PRODUCTION_EFFECT))}</td>"
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
        f"<tr><th>{html.escape(_text(label))}</th><td>{_value_html(label, value)}</td></tr>"
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
        + "".join(
            f"<td>{_value_html(column, record.get(column), default='')}</td>" for column in columns
        )
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
            ("audit_value", record.get("audit_value")),
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
            f"<td>{_status_badge(status)}</td>"
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
header .status-strip {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 10px;
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
  margin: 24px 0;
  padding: 0;
}
.summary-card-grid,
.market-card-grid,
.status-panel,
.impact-summary-grid,
.funnel-flow {
  display: grid;
  gap: 10px;
  margin: 0 0 14px;
}
.summary-card-grid {
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
}
.status-panel,
.impact-summary-grid {
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
}
.market-card-grid {
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
}
.summary-card,
.market-card,
.status-panel-card,
.impact-summary-card,
.funnel-node {
  background: #ffffff;
  border: 1px solid #d9dee7;
  border-radius: 6px;
  padding: 12px;
  min-width: 0;
}
.summary-card div,
.market-card div,
.status-panel-card div,
.impact-summary-card div,
.funnel-node span {
  color: #586069;
  font-size: 12px;
  font-weight: 700;
  text-transform: uppercase;
}
.summary-card strong,
.market-card strong,
.status-panel-card strong,
.impact-summary-card strong,
.funnel-node strong {
  display: block;
  margin: 6px 0;
  color: #1b1f23;
  font-size: 20px;
  line-height: 1.2;
  overflow-wrap: anywhere;
}
.summary-card p,
.market-card p,
.status-panel-card p,
.impact-summary-card p,
.funnel-node small {
  color: #4d5968;
  display: block;
  font-size: 12px;
  line-height: 1.35;
  margin: 0 0 8px;
  overflow-wrap: anywhere;
}
.summary-card--binding,
.funnel-node.binding,
tr.binding-row {
  border-color: #b7791f;
  box-shadow: inset 3px 0 0 #b7791f;
}
.badge-row {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}
.action-checklist {
  background: #ffffff;
  border: 1px solid #d9dee7;
  border-radius: 6px;
  list-style: none;
  margin: 0 0 14px;
  padding: 0;
}
.action-checklist li {
  display: grid;
  grid-template-columns: 36px minmax(0, 1fr);
  gap: 10px;
  padding: 10px 12px;
  border-top: 1px solid #e7ebf0;
}
.action-checklist li:first-child {
  border-top: 0;
}
.action-checklist li > span {
  align-items: center;
  background: #edf4ff;
  border-radius: 999px;
  color: #25476a;
  display: flex;
  font-weight: 700;
  height: 28px;
  justify-content: center;
  width: 28px;
}
.action-checklist strong {
  display: block;
  margin-bottom: 4px;
}
.action-checklist p {
  color: #4d5968;
  font-size: 12px;
  margin: 0 0 6px;
}
.status-badge {
  background: #eef2f7;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  color: #29313d;
  display: inline-block;
  font-size: 11px;
  font-weight: 700;
  line-height: 1;
  margin: 1px 4px 1px 0;
  padding: 4px 7px;
  vertical-align: middle;
  white-space: nowrap;
}
.status-ok,
.status-pass,
.status-available,
.status-fresh,
.status-production-none {
  background: #e8f5ee;
  border-color: #9fd3b4;
  color: #0f5132;
}
.status-pass-with-warnings,
.status-pass-with-limitations,
.status-limited,
.status-limited-reader-context,
.status-registry-fallback,
.status-warning,
.status-important,
.status-review-with-limitations,
.status-true,
.status-binding-gate {
  background: #fff4db;
  border-color: #e3b45d;
  color: #7a4f01;
}
.status-missing,
.status-stale,
.status-required-missing,
.status-blocked,
.status-blocking,
.status-failed,
.status-fail,
.status-critical,
.status-manual-review-required,
.status-blocked-by-missing-artifacts,
.status-blocked-by-manual-review,
.status-blocked-by-data-quality {
  background: #fdecec;
  border-color: #efaaa7;
  color: #842029;
}
.status-info,
.status-optional,
.status-documentation,
.status-not-promotable,
.status-ready-for-reading,
.status-false {
  background: #edf4ff;
  border-color: #b7cbed;
  color: #25476a;
}
.market-metrics {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 4px 8px;
  margin: 8px 0;
}
.market-metrics dt {
  color: #586069;
  font-size: 11px;
  font-weight: 700;
}
.market-metrics dd {
  font-size: 13px;
  margin: 0;
}
.funnel-flow {
  align-items: stretch;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
}
.funnel-node {
  position: relative;
}
.review-group,
.impact-group,
.review-impact-group {
  border-left: 3px solid #d9dee7;
  margin: 12px 0;
  padding-left: 12px;
}
.review-critical,
.review-impact-today-decision,
.impact-blocking {
  border-left-color: #c2413d;
}
.review-warning,
.review-impact-research-promotion,
.impact-important {
  border-left-color: #b7791f;
}
.review-info,
.review-impact-audit-observe,
.impact-info,
.impact-optional {
  border-left-color: #4676b6;
}
.recommended-action {
  color: #1b1f23;
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
  background: #ffffff;
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
