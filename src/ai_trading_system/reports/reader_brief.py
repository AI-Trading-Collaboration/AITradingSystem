from __future__ import annotations

import html
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
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
    pit_source_manifest = _pit_source_manifest_summary(report_index)
    governance_summary = _backtest_shadow_governance(
        daily_decision_summary=daily_decision_summary,
        daily_task_dashboard=daily_task_dashboard,
        research_governance_summary=research_governance_summary,
    )
    parameter_shadow_review = _parameter_shadow_review(as_of)
    etf_backtest_summary = _etf_backtest_review_summary(as_of)
    etf_calibration_experiments = _etf_calibration_experiment_summary(report_index)
    etf_forward_simulation = _etf_forward_simulation_summary(report_index)
    etf_ai_confirmation = _etf_ai_confirmation_summary(report_index)
    etf_ai_attribution = _etf_ai_attribution_summary(report_index)
    etf_satellite_replacement = _etf_satellite_replacement_summary(report_index)
    etf_satellite_attribution = _etf_satellite_attribution_summary(report_index)
    etf_weekly_review = _etf_weekly_review_summary(report_index)
    etf_decision_journal = _etf_decision_journal_summary(report_index)
    etf_parameter_review = _etf_parameter_review_summary(report_index)
    etf_weight_calibration = _etf_weight_calibration_summary(report_index)
    etf_initial_weight_candidates = _etf_initial_weight_candidate_summary(report_index)
    etf_weight_calibration_profiling = _etf_weight_calibration_profiling_summary(
        report_index,
    )
    etf_operations_health = _etf_operations_health_summary(report_index)
    etf_data_quality_governance = _etf_data_quality_governance_summary(report_index)
    etf_strategy_evidence = _etf_strategy_evidence_summary(report_index)
    etf_baseline_review = _etf_baseline_review_summary(report_index)
    etf_shadow_candidate_review = _etf_shadow_candidate_review_summary(report_index)
    etf_trend_calibration = _etf_trend_calibration_summary(report_index)
    etf_dynamic_allocation = _etf_dynamic_allocation_summary(report_index)
    etf_dynamic_calibration = _etf_dynamic_calibration_summary(report_index)
    etf_dynamic_robustness = _etf_dynamic_robustness_summary(report_index)
    etf_dynamic_shadow = _etf_dynamic_shadow_summary(report_index)
    etf_dynamic_rescue = _etf_dynamic_rescue_summary(report_index)
    etf_dynamic_v2_review = _etf_dynamic_v2_review_summary(report_index)
    etf_dynamic_v3_rescue = _etf_dynamic_v3_rescue_summary(report_index)
    etf_dynamic_v3_real_evaluation = _etf_dynamic_v3_real_evaluation_summary(report_index)
    etf_dynamic_v3_failure_attribution = _etf_dynamic_v3_failure_attribution_summary(report_index)
    etf_dynamic_v3_parameter_research = _etf_dynamic_v3_parameter_research_summary(report_index)
    etf_dynamic_v3_sim_review = _etf_dynamic_v3_sim_review_summary(report_index)
    etf_dynamic_v3_manual_execution_review = _etf_dynamic_v3_manual_execution_review_summary(
        report_index
    )
    etf_dynamic_v3_real_snapshot_review = _etf_dynamic_v3_real_snapshot_review_summary(report_index)
    etf_dynamic_v3_system_target = _etf_dynamic_v3_system_target_summary(report_index)
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
    data_quality_pit_safety = _data_quality_pit_safety(
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
        "backtest_shadow_governance": governance_summary,
        "parameter_shadow_review": parameter_shadow_review,
        "etf_backtest_summary": etf_backtest_summary,
        "etf_calibration_experiments": etf_calibration_experiments,
        "etf_forward_simulation": etf_forward_simulation,
        "etf_ai_confirmation": etf_ai_confirmation,
        "etf_ai_attribution": etf_ai_attribution,
        "etf_satellite_replacement": etf_satellite_replacement,
        "etf_satellite_attribution": etf_satellite_attribution,
        "etf_weekly_review": etf_weekly_review,
        "etf_decision_journal": etf_decision_journal,
        "etf_parameter_review": etf_parameter_review,
        "etf_weight_calibration": etf_weight_calibration,
        "etf_initial_weight_candidates": etf_initial_weight_candidates,
        "etf_weight_calibration_profiling": etf_weight_calibration_profiling,
        "etf_operations_health": etf_operations_health,
        "etf_data_quality_governance": etf_data_quality_governance,
        "etf_strategy_evidence": etf_strategy_evidence,
        "etf_baseline_review": etf_baseline_review,
        "etf_shadow_candidate_review": etf_shadow_candidate_review,
        "etf_trend_calibration": etf_trend_calibration,
        "etf_dynamic_allocation": etf_dynamic_allocation,
        "etf_dynamic_calibration": etf_dynamic_calibration,
        "etf_dynamic_robustness": etf_dynamic_robustness,
        "etf_dynamic_shadow": etf_dynamic_shadow,
        "etf_dynamic_rescue": etf_dynamic_rescue,
        "etf_dynamic_v2_review": etf_dynamic_v2_review,
        "etf_dynamic_v3_rescue": etf_dynamic_v3_rescue,
        "etf_dynamic_v3_real_evaluation": etf_dynamic_v3_real_evaluation,
        "etf_dynamic_v3_failure_attribution": etf_dynamic_v3_failure_attribution,
        "etf_dynamic_v3_parameter_research": etf_dynamic_v3_parameter_research,
        "etf_dynamic_v3_sim_review": etf_dynamic_v3_sim_review,
        "etf_dynamic_v3_manual_execution_review": etf_dynamic_v3_manual_execution_review,
        "etf_dynamic_v3_real_snapshot_review": etf_dynamic_v3_real_snapshot_review,
        "etf_dynamic_v3_system_target": etf_dynamic_v3_system_target,
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
    missing_impact = _mapping(payload.get("missing_limited_artifact_impact"))
    cadence_calendar = _mapping(payload.get("task_cadence_calendar"))
    documentation_contract = _mapping(payload.get("documentation_contract_summary"))
    contribution_summary = _mapping(payload.get("contribution_summary"))
    components = _records(_mapping(payload.get("component_score_explainability")).get("components"))
    gates = _records(_mapping(payload.get("binding_gate_ladder")).get("gates"))
    quality = _mapping(payload.get("data_quality_pit_safety"))
    pit_source_manifest = _mapping(payload.get("pit_source_manifest"))
    governance = _mapping(payload.get("backtest_shadow_governance"))
    parameter_shadow = _mapping(payload.get("parameter_shadow_review"))
    etf_backtest = _mapping(payload.get("etf_backtest_summary"))
    etf_calibration = _mapping(payload.get("etf_calibration_experiments"))
    etf_forward = _mapping(payload.get("etf_forward_simulation"))
    etf_ai_confirmation = _mapping(payload.get("etf_ai_confirmation"))
    etf_ai_attribution = _mapping(payload.get("etf_ai_attribution"))
    etf_satellite = _mapping(payload.get("etf_satellite_replacement"))
    etf_satellite_attribution = _mapping(payload.get("etf_satellite_attribution"))
    etf_weekly_review = _mapping(payload.get("etf_weekly_review"))
    etf_decision_journal = _mapping(payload.get("etf_decision_journal"))
    etf_parameter_review = _mapping(payload.get("etf_parameter_review"))
    etf_weight_calibration = _mapping(payload.get("etf_weight_calibration"))
    etf_initial_weight_candidates = _mapping(payload.get("etf_initial_weight_candidates"))
    etf_weight_calibration_profiling = _mapping(payload.get("etf_weight_calibration_profiling"))
    etf_operations_health = _mapping(payload.get("etf_operations_health"))
    etf_data_quality_governance = _mapping(payload.get("etf_data_quality_governance"))
    etf_strategy_evidence = _mapping(payload.get("etf_strategy_evidence"))
    etf_baseline_review = _mapping(payload.get("etf_baseline_review"))
    etf_shadow_candidate_review = _mapping(payload.get("etf_shadow_candidate_review"))
    etf_trend_calibration = _mapping(payload.get("etf_trend_calibration"))
    etf_dynamic_allocation = _mapping(payload.get("etf_dynamic_allocation"))
    etf_dynamic_calibration = _mapping(payload.get("etf_dynamic_calibration"))
    etf_dynamic_robustness = _mapping(payload.get("etf_dynamic_robustness"))
    etf_dynamic_shadow = _mapping(payload.get("etf_dynamic_shadow"))
    etf_dynamic_rescue = _mapping(payload.get("etf_dynamic_rescue"))
    etf_dynamic_v2_review = _mapping(payload.get("etf_dynamic_v2_review"))
    etf_dynamic_v3_rescue = _mapping(payload.get("etf_dynamic_v3_rescue"))
    etf_dynamic_v3_real_evaluation = _mapping(payload.get("etf_dynamic_v3_real_evaluation"))
    etf_dynamic_v3_failure_attribution = _mapping(payload.get("etf_dynamic_v3_failure_attribution"))
    etf_dynamic_v3_parameter_research = _mapping(payload.get("etf_dynamic_v3_parameter_research"))
    etf_dynamic_v3_sim_review = _mapping(payload.get("etf_dynamic_v3_sim_review"))
    etf_dynamic_v3_manual_execution_review = _mapping(
        payload.get("etf_dynamic_v3_manual_execution_review")
    )
    etf_dynamic_v3_real_snapshot_review = _mapping(
        payload.get("etf_dynamic_v3_real_snapshot_review")
    )
    etf_dynamic_v3_system_target = _mapping(payload.get("etf_dynamic_v3_system_target"))
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
            "ETF Data Quality",
            _definition_table(
                [
                    ("availability", etf_data_quality_governance.get("availability")),
                    ("status", etf_data_quality_governance.get("status")),
                    ("summary", etf_data_quality_governance.get("summary_sentence")),
                    (
                        "blocking_failures",
                        etf_data_quality_governance.get("blocking_failure_count"),
                    ),
                    ("warnings", etf_data_quality_governance.get("warning_count")),
                    (
                        "price_freshness",
                        etf_data_quality_governance.get("price_freshness_status"),
                    ),
                    (
                        "missing_bars",
                        etf_data_quality_governance.get("missing_bars_status"),
                    ),
                    (
                        "return_outliers",
                        etf_data_quality_governance.get("return_outliers_status"),
                    ),
                    ("config_drift", etf_data_quality_governance.get("config_drift_status")),
                    (
                        "evidence_completeness",
                        etf_data_quality_governance.get("evidence_completeness_status"),
                    ),
                    (
                        "gate_freshness",
                        etf_data_quality_governance.get("gate_freshness_status"),
                    ),
                    (
                        "report_staleness",
                        etf_data_quality_governance.get("report_staleness_status"),
                    ),
                    (
                        "reader_brief_links",
                        etf_data_quality_governance.get("reader_brief_link_status"),
                    ),
                    ("detailed_report", etf_data_quality_governance.get("detail_report")),
                    ("safety_status", etf_data_quality_governance.get("safety_status")),
                    ("production_effect", etf_data_quality_governance.get("production_effect")),
                    ("broker_action", etf_data_quality_governance.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Strategy Evidence Dashboard",
            _definition_table(
                [
                    ("availability", etf_strategy_evidence.get("availability")),
                    ("overall_status", etf_strategy_evidence.get("overall_status")),
                    ("summary", etf_strategy_evidence.get("summary_sentence")),
                    ("strongest_evidence", etf_strategy_evidence.get("strongest_evidence")),
                    ("weakest_evidence", etf_strategy_evidence.get("weakest_evidence")),
                    ("blocking_issues", etf_strategy_evidence.get("blocking_issues")),
                    (
                        "manual_review_priorities",
                        etf_strategy_evidence.get("manual_review_priority_count"),
                    ),
                    ("data_quality_status", etf_strategy_evidence.get("data_quality_status")),
                    ("detailed_dashboard", etf_strategy_evidence.get("detail_report")),
                    ("safety_status", etf_strategy_evidence.get("safety_status")),
                    ("production_effect", etf_strategy_evidence.get("production_effect")),
                    ("broker_action", etf_strategy_evidence.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Baseline Candidate Review",
            _definition_table(
                [
                    ("availability", etf_baseline_review.get("availability")),
                    ("status", etf_baseline_review.get("status")),
                    ("summary", etf_baseline_review.get("summary_sentence")),
                    ("eligible_candidates", etf_baseline_review.get("eligible_count")),
                    ("needs_more_data", etf_baseline_review.get("needs_more_data_count")),
                    ("blocked_candidates", etf_baseline_review.get("blocked_count")),
                    ("latest_review_package", etf_baseline_review.get("latest_review_package")),
                    ("latest_owner_decision", etf_baseline_review.get("latest_owner_decision")),
                    ("proposal_drafts", etf_baseline_review.get("proposal_draft_count")),
                    ("latest_outcome", etf_baseline_review.get("latest_outcome_status")),
                    ("detailed_review_package", etf_baseline_review.get("detail_report")),
                    ("safety_status", etf_baseline_review.get("safety_status")),
                    ("production_effect", etf_baseline_review.get("production_effect")),
                    ("broker_action", etf_baseline_review.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Shadow Candidate Review",
            _definition_table(
                [
                    ("availability", etf_shadow_candidate_review.get("availability")),
                    ("status", etf_shadow_candidate_review.get("status")),
                    ("summary", etf_shadow_candidate_review.get("summary_sentence")),
                    ("top_candidate", etf_shadow_candidate_review.get("top_candidate")),
                    (
                        "pending_review_candidates",
                        etf_shadow_candidate_review.get("pending_review_count"),
                    ),
                    (
                        "approved_enrollments",
                        etf_shadow_candidate_review.get("approved_enrollment_count"),
                    ),
                    (
                        "latest_owner_decision",
                        etf_shadow_candidate_review.get("latest_owner_decision"),
                    ),
                    ("latest_enrollment", etf_shadow_candidate_review.get("latest_enrollment")),
                    ("detailed_review_package", etf_shadow_candidate_review.get("detail_report")),
                    ("safety_status", etf_shadow_candidate_review.get("safety_status")),
                    ("production_effect", etf_shadow_candidate_review.get("production_effect")),
                    ("broker_action", etf_shadow_candidate_review.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Trend Signal Calibration",
            _definition_table(
                [
                    ("availability", etf_trend_calibration.get("availability")),
                    ("status", etf_trend_calibration.get("status")),
                    ("summary", etf_trend_calibration.get("summary_sentence")),
                    ("top_config", etf_trend_calibration.get("top_config")),
                    ("evidence_status", etf_trend_calibration.get("evidence_status")),
                    ("redundancy_risk", etf_trend_calibration.get("redundancy_risk")),
                    ("regime_stability", etf_trend_calibration.get("regime_stability")),
                    ("data_quality_status", etf_trend_calibration.get("data_quality_status")),
                    ("detailed_report", etf_trend_calibration.get("detail_report")),
                    ("safety_status", etf_trend_calibration.get("safety_status")),
                    ("production_effect", etf_trend_calibration.get("production_effect")),
                    ("broker_action", etf_trend_calibration.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Dynamic Allocation Candidate",
            _definition_table(
                [
                    ("availability", etf_dynamic_allocation.get("availability")),
                    ("status", etf_dynamic_allocation.get("status")),
                    ("summary", etf_dynamic_allocation.get("summary_sentence")),
                    ("policy_id", etf_dynamic_allocation.get("policy_id")),
                    ("selected_regime", etf_dynamic_allocation.get("selected_regime")),
                    ("rebalance_decision", etf_dynamic_allocation.get("rebalance_decision")),
                    (
                        "candidate_target_weights",
                        etf_dynamic_allocation.get("candidate_target_weights"),
                    ),
                    ("constraint_count", etf_dynamic_allocation.get("constraint_count")),
                    ("data_quality_status", etf_dynamic_allocation.get("data_quality_status")),
                    ("detailed_report", etf_dynamic_allocation.get("detail_report")),
                    ("safety_status", etf_dynamic_allocation.get("safety_status")),
                    ("production_effect", etf_dynamic_allocation.get("production_effect")),
                    ("broker_action", etf_dynamic_allocation.get("broker_action")),
                    (
                        "official_target_weights_mutated",
                        etf_dynamic_allocation.get("official_target_weights_mutated"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Calibration Batch",
            _definition_table(
                [
                    ("availability", etf_dynamic_calibration.get("availability")),
                    ("status", etf_dynamic_calibration.get("status")),
                    ("summary", etf_dynamic_calibration.get("summary_sentence")),
                    ("pack_id", etf_dynamic_calibration.get("pack_id")),
                    ("top_candidate", etf_dynamic_calibration.get("top_candidate")),
                    ("top_ranking_score", etf_dynamic_calibration.get("top_ranking_score")),
                    ("candidate_pack_count", etf_dynamic_calibration.get("candidate_pack_count")),
                    ("cache_hit_rate", etf_dynamic_calibration.get("cache_hit_rate")),
                    ("data_quality_status", etf_dynamic_calibration.get("data_quality_status")),
                    (
                        "full_robustness_backtest_required",
                        etf_dynamic_calibration.get("full_robustness_backtest_required"),
                    ),
                    ("detailed_report", etf_dynamic_calibration.get("detail_report")),
                    ("safety_status", etf_dynamic_calibration.get("safety_status")),
                    ("production_effect", etf_dynamic_calibration.get("production_effect")),
                    ("broker_action", etf_dynamic_calibration.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Dynamic Robustness Review",
            _definition_table(
                [
                    ("availability", etf_dynamic_robustness.get("availability")),
                    ("status", etf_dynamic_robustness.get("status")),
                    ("summary", etf_dynamic_robustness.get("summary_sentence")),
                    ("candidate", etf_dynamic_robustness.get("candidate")),
                    ("dynamic_total_return", etf_dynamic_robustness.get("dynamic_total_return")),
                    ("dynamic_cagr", etf_dynamic_robustness.get("dynamic_cagr")),
                    ("dynamic_max_drawdown", etf_dynamic_robustness.get("dynamic_max_drawdown")),
                    (
                        "excess_vs_static_base",
                        etf_dynamic_robustness.get("excess_vs_static_base"),
                    ),
                    (
                        "false_risk_off_count",
                        etf_dynamic_robustness.get("false_risk_off_count"),
                    ),
                    (
                        "false_risk_on_count",
                        etf_dynamic_robustness.get("false_risk_on_count"),
                    ),
                    ("overfit_status", etf_dynamic_robustness.get("overfit_status")),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_robustness.get("shadow_enrollment_allowed"),
                    ),
                    (
                        "data_quality_status",
                        etf_dynamic_robustness.get("data_quality_status"),
                    ),
                    ("detailed_report", etf_dynamic_robustness.get("detail_report")),
                    ("safety_status", etf_dynamic_robustness.get("safety_status")),
                    ("production_effect", etf_dynamic_robustness.get("production_effect")),
                    ("broker_action", etf_dynamic_robustness.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Dynamic Shadow Review",
            _definition_table(
                [
                    ("availability", etf_dynamic_shadow.get("availability")),
                    ("status", etf_dynamic_shadow.get("status")),
                    ("summary", etf_dynamic_shadow.get("summary_sentence")),
                    ("top_candidate", etf_dynamic_shadow.get("top_candidate")),
                    (
                        "ready_after_owner_approval_count",
                        etf_dynamic_shadow.get("ready_after_owner_approval_count"),
                    ),
                    ("blocked_count", etf_dynamic_shadow.get("blocked_count")),
                    ("latest_owner_decision", etf_dynamic_shadow.get("latest_owner_decision")),
                    ("latest_enrollment", etf_dynamic_shadow.get("latest_enrollment")),
                    ("active_candidate_count", etf_dynamic_shadow.get("active_candidate_count")),
                    ("watch_count", etf_dynamic_shadow.get("watch_count")),
                    (
                        "reject_pending_review_count",
                        etf_dynamic_shadow.get("reject_pending_review_count"),
                    ),
                    ("tracking_status", etf_dynamic_shadow.get("tracking_status")),
                    ("package_report", etf_dynamic_shadow.get("package_report")),
                    ("weekly_review", etf_dynamic_shadow.get("weekly_review")),
                    ("safety_status", etf_dynamic_shadow.get("safety_status")),
                    ("production_effect", etf_dynamic_shadow.get("production_effect")),
                    ("broker_action", etf_dynamic_shadow.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Dynamic Strategy Rescue",
            _definition_table(
                [
                    ("availability", etf_dynamic_rescue.get("availability")),
                    ("status", etf_dynamic_rescue.get("status")),
                    ("summary", etf_dynamic_rescue.get("summary_sentence")),
                    ("failed_v0_1_status", etf_dynamic_rescue.get("failed_v0_1_status")),
                    ("main_failures", etf_dynamic_rescue.get("main_failures")),
                    ("best_rescue_candidate", etf_dynamic_rescue.get("best_rescue_candidate")),
                    ("best_status", etf_dynamic_rescue.get("best_status")),
                    (
                        "false_risk_off_reduction",
                        etf_dynamic_rescue.get("false_risk_off_reduction"),
                    ),
                    ("turnover_reduction", etf_dynamic_rescue.get("turnover_reduction")),
                    ("remaining_blockers", etf_dynamic_rescue.get("remaining_blockers")),
                    ("detailed_report", etf_dynamic_rescue.get("detail_report")),
                    ("safety_status", etf_dynamic_rescue.get("safety_status")),
                    ("production_effect", etf_dynamic_rescue.get("production_effect")),
                    ("broker_action", etf_dynamic_rescue.get("broker_action")),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_rescue.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic v0.2 Review",
            _definition_table(
                [
                    ("availability", etf_dynamic_v2_review.get("availability")),
                    ("status", etf_dynamic_v2_review.get("status")),
                    ("review_status", etf_dynamic_v2_review.get("review_status")),
                    ("summary", etf_dynamic_v2_review.get("summary_sentence")),
                    ("candidate", etf_dynamic_v2_review.get("candidate")),
                    ("improvements", etf_dynamic_v2_review.get("improvements")),
                    ("blockers", etf_dynamic_v2_review.get("blockers")),
                    ("next_action", etf_dynamic_v2_review.get("next_action")),
                    ("detailed_package", etf_dynamic_v2_review.get("package_report")),
                    ("safety_status", etf_dynamic_v2_review.get("safety_status")),
                    ("production_effect", etf_dynamic_v2_review.get("production_effect")),
                    ("broker_action", etf_dynamic_v2_review.get("broker_action")),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v2_review.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic v0.3 Rescue",
            _definition_table(
                [
                    ("availability", etf_dynamic_v3_rescue.get("availability")),
                    ("status", etf_dynamic_v3_rescue.get("status")),
                    ("review_status", etf_dynamic_v3_rescue.get("review_status")),
                    ("summary", etf_dynamic_v3_rescue.get("summary_sentence")),
                    ("base_candidate", etf_dynamic_v3_rescue.get("base_candidate")),
                    ("best_candidate", etf_dynamic_v3_rescue.get("best_candidate")),
                    ("best_candidate_status", etf_dynamic_v3_rescue.get("best_candidate_status")),
                    ("constraint_status", etf_dynamic_v3_rescue.get("constraint_status")),
                    ("drawdown_status", etf_dynamic_v3_rescue.get("drawdown_status")),
                    ("remaining_blockers", etf_dynamic_v3_rescue.get("remaining_blockers")),
                    ("detailed_report", etf_dynamic_v3_rescue.get("detail_report")),
                    ("safety_status", etf_dynamic_v3_rescue.get("safety_status")),
                    ("production_effect", etf_dynamic_v3_rescue.get("production_effect")),
                    ("broker_action", etf_dynamic_v3_rescue.get("broker_action")),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v3_rescue.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic v0.3 Real Evaluation",
            _definition_table(
                [
                    ("availability", etf_dynamic_v3_real_evaluation.get("availability")),
                    ("status", etf_dynamic_v3_real_evaluation.get("status")),
                    (
                        "promotion_gate_decision",
                        etf_dynamic_v3_real_evaluation.get("promotion_gate_decision"),
                    ),
                    ("summary", etf_dynamic_v3_real_evaluation.get("summary_sentence")),
                    ("best_candidate", etf_dynamic_v3_real_evaluation.get("best_candidate")),
                    (
                        "constraint_hit_reduction_vs_v0_4",
                        etf_dynamic_v3_real_evaluation.get("constraint_hit_reduction_vs_v0_4"),
                    ),
                    (
                        "false_risk_off_delta_vs_v0_4",
                        etf_dynamic_v3_real_evaluation.get("false_risk_off_delta_vs_v0_4"),
                    ),
                    (
                        "drawdown_preservation",
                        etf_dynamic_v3_real_evaluation.get("drawdown_preservation"),
                    ),
                    ("static_gap", etf_dynamic_v3_real_evaluation.get("static_gap")),
                    ("overfit_status", etf_dynamic_v3_real_evaluation.get("overfit_status")),
                    ("detailed_report", etf_dynamic_v3_real_evaluation.get("detail_report")),
                    ("safety_status", etf_dynamic_v3_real_evaluation.get("safety_status")),
                    (
                        "production_effect",
                        etf_dynamic_v3_real_evaluation.get("production_effect"),
                    ),
                    ("broker_action", etf_dynamic_v3_real_evaluation.get("broker_action")),
                    (
                        "automatic_candidate_promotion",
                        etf_dynamic_v3_real_evaluation.get("automatic_candidate_promotion"),
                    ),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v3_real_evaluation.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic v0.3 Failure Attribution",
            _definition_table(
                [
                    (
                        "availability",
                        etf_dynamic_v3_failure_attribution.get("availability"),
                    ),
                    ("status", etf_dynamic_v3_failure_attribution.get("status")),
                    (
                        "summary",
                        etf_dynamic_v3_failure_attribution.get("summary_sentence"),
                    ),
                    (
                        "v0_3_rejection_primary_reason",
                        etf_dynamic_v3_failure_attribution.get("v0_3_rejection_primary_reason"),
                    ),
                    (
                        "v0_4_promotion_review",
                        etf_dynamic_v3_failure_attribution.get("v0_4_promotion_review"),
                    ),
                    (
                        "v0_5_design_recommendation",
                        etf_dynamic_v3_failure_attribution.get("v0_5_design_recommendation"),
                    ),
                    (
                        "constraint_hit_reduction_vs_v0_4",
                        etf_dynamic_v3_failure_attribution.get("constraint_hit_reduction_vs_v0_4"),
                    ),
                    (
                        "v0_3_constraint_hit_rate",
                        etf_dynamic_v3_failure_attribution.get("v0_3_constraint_hit_rate"),
                    ),
                    (
                        "v0_4_constraint_hit_rate",
                        etf_dynamic_v3_failure_attribution.get("v0_4_constraint_hit_rate"),
                    ),
                    (
                        "detailed_report",
                        etf_dynamic_v3_failure_attribution.get("detail_report"),
                    ),
                    (
                        "safety_status",
                        etf_dynamic_v3_failure_attribution.get("safety_status"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_failure_attribution.get("production_effect"),
                    ),
                    (
                        "broker_action",
                        etf_dynamic_v3_failure_attribution.get("broker_action"),
                    ),
                    (
                        "automatic_candidate_promotion",
                        etf_dynamic_v3_failure_attribution.get("automatic_candidate_promotion"),
                    ),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v3_failure_attribution.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Parameter Sweep",
            _definition_table(
                [
                    (
                        "availability",
                        etf_dynamic_v3_parameter_research.get("availability"),
                    ),
                    ("status", etf_dynamic_v3_parameter_research.get("status")),
                    (
                        "evaluator_mode",
                        etf_dynamic_v3_parameter_research.get("evaluator_mode"),
                    ),
                    (
                        "metrics_source",
                        etf_dynamic_v3_parameter_research.get("metrics_source"),
                    ),
                    (
                        "not_for_investment_decision",
                        etf_dynamic_v3_parameter_research.get("not_for_investment_decision"),
                    ),
                    (
                        "summary",
                        etf_dynamic_v3_parameter_research.get("summary_sentence"),
                    ),
                    (
                        "candidate_count",
                        etf_dynamic_v3_parameter_research.get("candidate_count"),
                    ),
                    (
                        "top_candidate",
                        etf_dynamic_v3_parameter_research.get("top_candidate"),
                    ),
                    ("top_gate", etf_dynamic_v3_parameter_research.get("top_gate")),
                    ("top_score", etf_dynamic_v3_parameter_research.get("top_score")),
                    (
                        "common_reject_reasons",
                        etf_dynamic_v3_parameter_research.get("common_reject_reasons"),
                    ),
                    (
                        "promotion_status",
                        etf_dynamic_v3_parameter_research.get("promotion_status"),
                    ),
                    (
                        "backtest_window_status",
                        etf_dynamic_v3_parameter_research.get("backtest_window_status"),
                    ),
                    (
                        "weight_path_status",
                        etf_dynamic_v3_parameter_research.get("weight_path_status"),
                    ),
                    (
                        "candidate_attribution_status",
                        etf_dynamic_v3_parameter_research.get("candidate_attribution_status"),
                    ),
                    (
                        "data_provenance_status",
                        etf_dynamic_v3_parameter_research.get("data_provenance_status"),
                    ),
                    (
                        "download_manifest_status",
                        etf_dynamic_v3_parameter_research.get("download_manifest_status"),
                    ),
                    (
                        "promotion_blocking_flags",
                        etf_dynamic_v3_parameter_research.get("promotion_blocking_flags"),
                    ),
                    (
                        "shadow_monitor_status",
                        etf_dynamic_v3_parameter_research.get("shadow_monitor_status"),
                    ),
                    (
                        "shadow_observe_only_count",
                        etf_dynamic_v3_parameter_research.get("shadow_observe_only_count"),
                    ),
                    (
                        "shadow_promotion_ready_count",
                        etf_dynamic_v3_parameter_research.get("shadow_promotion_ready_count"),
                    ),
                    (
                        "shadow_live_drift_review_required_count",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_live_drift_review_required_count"
                        ),
                    ),
                    (
                        "candidate_evidence_status",
                        etf_dynamic_v3_parameter_research.get("candidate_evidence_status"),
                    ),
                    (
                        "candidate_evidence_usable_count",
                        etf_dynamic_v3_parameter_research.get("candidate_evidence_usable_count"),
                    ),
                    (
                        "candidate_evidence_complete_count",
                        etf_dynamic_v3_parameter_research.get("candidate_evidence_complete_count"),
                    ),
                    (
                        "candidate_evidence_top_blockers",
                        etf_dynamic_v3_parameter_research.get("candidate_evidence_top_blockers"),
                    ),
                    (
                        "observe_pool_status",
                        etf_dynamic_v3_parameter_research.get("observe_pool_status"),
                    ),
                    (
                        "observe_candidate_count",
                        etf_dynamic_v3_parameter_research.get("observe_candidate_count"),
                    ),
                    (
                        "observe_pool_manual_review_required_count",
                        etf_dynamic_v3_parameter_research.get(
                            "observe_pool_manual_review_required_count"
                        ),
                    ),
                    (
                        "shadow_registry_sync_status",
                        etf_dynamic_v3_parameter_research.get("shadow_registry_sync_status"),
                    ),
                    (
                        "overnight_readiness",
                        etf_dynamic_v3_parameter_research.get("overnight_readiness"),
                    ),
                    (
                        "overnight_blocking_reasons",
                        etf_dynamic_v3_parameter_research.get("overnight_blocking_reasons"),
                    ),
                    (
                        "research_decision_recommendation",
                        etf_dynamic_v3_parameter_research.get("research_decision_recommendation"),
                    ),
                    (
                        "research_decision_priority",
                        etf_dynamic_v3_parameter_research.get("research_decision_priority"),
                    ),
                    (
                        "research_decision_next_task",
                        etf_dynamic_v3_parameter_research.get("research_decision_next_task"),
                    ),
                    (
                        "evidence_diagnosis_status",
                        etf_dynamic_v3_parameter_research.get("evidence_diagnosis_status"),
                    ),
                    (
                        "evidence_diagnosis_usable_candidates",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_diagnosis_usable_candidates"
                        ),
                    ),
                    (
                        "evidence_diagnosis_hard_blocked_candidates",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_diagnosis_hard_blocked_candidates"
                        ),
                    ),
                    (
                        "evidence_diagnosis_soft_blocked_candidates",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_diagnosis_soft_blocked_candidates"
                        ),
                    ),
                    (
                        "gate_impact_best_scenario",
                        etf_dynamic_v3_parameter_research.get("gate_impact_best_scenario"),
                    ),
                    (
                        "gate_impact_best_observe_candidates",
                        etf_dynamic_v3_parameter_research.get(
                            "gate_impact_best_observe_candidates"
                        ),
                    ),
                    (
                        "gate_policy_version",
                        etf_dynamic_v3_parameter_research.get("gate_policy_version"),
                    ),
                    (
                        "gate_policy_observe_only_candidates",
                        etf_dynamic_v3_parameter_research.get(
                            "gate_policy_observe_only_candidates"
                        ),
                    ),
                    (
                        "candidate_recovery_status",
                        etf_dynamic_v3_parameter_research.get("candidate_recovery_status"),
                    ),
                    (
                        "recovered_candidate_count",
                        etf_dynamic_v3_parameter_research.get("recovered_candidate_count"),
                    ),
                    (
                        "research_decision_update_go_no_go",
                        etf_dynamic_v3_parameter_research.get("research_decision_update_go_no_go"),
                    ),
                    (
                        "research_decision_update_recommended_action",
                        etf_dynamic_v3_parameter_research.get(
                            "research_decision_update_recommended_action"
                        ),
                    ),
                    (
                        "research_decision_update_required_owner_approval",
                        etf_dynamic_v3_parameter_research.get(
                            "research_decision_update_required_owner_approval"
                        ),
                    ),
                    (
                        "research_decision_update_usable_candidates_before",
                        etf_dynamic_v3_parameter_research.get(
                            "research_decision_update_usable_candidates_before"
                        ),
                    ),
                    (
                        "research_decision_update_usable_candidates_after",
                        etf_dynamic_v3_parameter_research.get(
                            "research_decision_update_usable_candidates_after"
                        ),
                    ),
                    (
                        "research_decision_update_warnings",
                        etf_dynamic_v3_parameter_research.get("research_decision_update_warnings"),
                    ),
                    (
                        "research_decision_update_next_task",
                        etf_dynamic_v3_parameter_research.get("research_decision_update_next_task"),
                    ),
                    (
                        "shortlist_status",
                        etf_dynamic_v3_parameter_research.get("shortlist_status"),
                    ),
                    (
                        "shortlist_count",
                        etf_dynamic_v3_parameter_research.get("shortlist_count"),
                    ),
                    (
                        "candidate_cluster_count",
                        etf_dynamic_v3_parameter_research.get("candidate_cluster_count"),
                    ),
                    (
                        "candidate_cluster_representative_count",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_cluster_representative_count"
                        ),
                    ),
                    (
                        "candidate_cluster_weight_path_similarity_status",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_cluster_weight_path_similarity_status"
                        ),
                    ),
                    (
                        "shadow_shortlist_candidate_count",
                        etf_dynamic_v3_parameter_research.get("shadow_shortlist_candidate_count"),
                    ),
                    (
                        "shadow_shortlist_monitoring_ready",
                        etf_dynamic_v3_parameter_research.get("shadow_shortlist_monitoring_ready"),
                    ),
                    (
                        "position_advisory_status",
                        etf_dynamic_v3_parameter_research.get("position_advisory_status"),
                    ),
                    (
                        "position_advisory_consensus_status",
                        etf_dynamic_v3_parameter_research.get("position_advisory_consensus_status"),
                    ),
                    (
                        "position_advisory_recommended_action",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_recommended_action"
                        ),
                    ),
                    (
                        "position_advisory_owner_approval_required",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_owner_approval_required"
                        ),
                    ),
                    (
                        "position_advisory_broker_action_allowed",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_broker_action_allowed"
                        ),
                    ),
                    (
                        "shadow_observation_readiness",
                        etf_dynamic_v3_parameter_research.get("shadow_observation_readiness"),
                    ),
                    (
                        "position_advisory_readiness",
                        etf_dynamic_v3_parameter_research.get("position_advisory_readiness"),
                    ),
                    (
                        "production_readiness",
                        etf_dynamic_v3_parameter_research.get("production_readiness"),
                    ),
                    (
                        "position_review_recommended_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "position_review_recommended_next_action"
                        ),
                    ),
                    (
                        "shadow_monitor_run_active_count",
                        etf_dynamic_v3_parameter_research.get("shadow_monitor_run_active_count"),
                    ),
                    (
                        "shadow_monitor_run_recommendation",
                        etf_dynamic_v3_parameter_research.get("shadow_monitor_run_recommendation"),
                    ),
                    (
                        "portfolio_snapshot_status",
                        etf_dynamic_v3_parameter_research.get("portfolio_snapshot_status"),
                    ),
                    (
                        "position_advisory_daily_mode",
                        etf_dynamic_v3_parameter_research.get("position_advisory_daily_mode"),
                    ),
                    (
                        "position_advisory_daily_consensus_status",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_daily_consensus_status"
                        ),
                    ),
                    (
                        "position_advisory_daily_recommended_action",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_daily_recommended_action"
                        ),
                    ),
                    (
                        "position_advisory_daily_broker_action_allowed",
                        etf_dynamic_v3_parameter_research.get(
                            "position_advisory_daily_broker_action_allowed"
                        ),
                    ),
                    (
                        "consensus_drift_disagreement_status",
                        etf_dynamic_v3_parameter_research.get(
                            "consensus_drift_disagreement_status"
                        ),
                    ),
                    (
                        "consensus_drift_advisory_implication",
                        etf_dynamic_v3_parameter_research.get(
                            "consensus_drift_advisory_implication"
                        ),
                    ),
                    (
                        "owner_review_decision",
                        etf_dynamic_v3_parameter_research.get("owner_review_decision"),
                    ),
                    (
                        "owner_review_broker_action_taken",
                        etf_dynamic_v3_parameter_research.get("owner_review_broker_action_taken"),
                    ),
                    (
                        "sweep_leaderboard",
                        etf_dynamic_v3_parameter_research.get("sweep_leaderboard"),
                    ),
                    (
                        "promotion_manifest",
                        etf_dynamic_v3_parameter_research.get("promotion_manifest"),
                    ),
                    (
                        "evidence_summary",
                        etf_dynamic_v3_parameter_research.get("evidence_summary"),
                    ),
                    (
                        "shadow_monitor_report",
                        etf_dynamic_v3_parameter_research.get("shadow_monitor_report"),
                    ),
                    (
                        "candidate_evidence_summary",
                        etf_dynamic_v3_parameter_research.get("candidate_evidence_summary"),
                    ),
                    (
                        "observe_pool",
                        etf_dynamic_v3_parameter_research.get("observe_pool"),
                    ),
                    (
                        "overnight_readiness_report",
                        etf_dynamic_v3_parameter_research.get("overnight_readiness_report"),
                    ),
                    (
                        "research_decision",
                        etf_dynamic_v3_parameter_research.get("research_decision"),
                    ),
                    (
                        "evidence_diagnosis",
                        etf_dynamic_v3_parameter_research.get("evidence_diagnosis"),
                    ),
                    (
                        "gate_impact",
                        etf_dynamic_v3_parameter_research.get("gate_impact"),
                    ),
                    (
                        "gate_policy",
                        etf_dynamic_v3_parameter_research.get("gate_policy"),
                    ),
                    (
                        "candidate_recovery",
                        etf_dynamic_v3_parameter_research.get("candidate_recovery"),
                    ),
                    (
                        "research_decision_update",
                        etf_dynamic_v3_parameter_research.get("research_decision_update"),
                    ),
                    (
                        "shortlist",
                        etf_dynamic_v3_parameter_research.get("shortlist"),
                    ),
                    (
                        "candidate_cluster",
                        etf_dynamic_v3_parameter_research.get("candidate_cluster"),
                    ),
                    (
                        "shadow_shortlist",
                        etf_dynamic_v3_parameter_research.get("shadow_shortlist"),
                    ),
                    (
                        "position_advisory",
                        etf_dynamic_v3_parameter_research.get("position_advisory"),
                    ),
                    (
                        "position_review",
                        etf_dynamic_v3_parameter_research.get("position_review"),
                    ),
                    (
                        "safety_status",
                        etf_dynamic_v3_parameter_research.get("safety_status"),
                    ),
                    (
                        "production_candidate_generated",
                        etf_dynamic_v3_parameter_research.get("production_candidate_generated"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Filtered Candidate Readiness",
            _definition_table(
                [
                    (
                        "filtered_candidate_evidence",
                        etf_dynamic_v3_parameter_research.get("filtered_candidate_evidence_id"),
                    ),
                    (
                        "evidence_status",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_candidate_evidence_status"
                        ),
                    ),
                    (
                        "primary_improvements",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_candidate_primary_improvements"
                        ),
                    ),
                    (
                        "primary_weaknesses",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_candidate_primary_weaknesses"
                        ),
                    ),
                    (
                        "median_regime_filter_spec",
                        etf_dynamic_v3_parameter_research.get("median_regime_filter_spec_id"),
                    ),
                    (
                        "contract_status",
                        etf_dynamic_v3_parameter_research.get(
                            "median_regime_filter_contract_status"
                        ),
                    ),
                    (
                        "formalization_complexity",
                        etf_dynamic_v3_parameter_research.get(
                            "median_regime_filter_complexity"
                        ),
                    ),
                    (
                        "stress_backfill",
                        etf_dynamic_v3_parameter_research.get("filtered_candidate_stress_id"),
                    ),
                    (
                        "stress_status",
                        etf_dynamic_v3_parameter_research.get("filtered_candidate_stress_status"),
                    ),
                    (
                        "stress_improved_count",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_candidate_stress_improved_count"
                        ),
                    ),
                    (
                        "drawdown_mismatch_reduction",
                        etf_dynamic_v3_parameter_research.get("drawdown_mismatch_reduction_id"),
                    ),
                    (
                        "drawdown_mismatch_status",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_mismatch_reduction_status"
                        ),
                    ),
                    (
                        "drawdown_mismatch_reduction_pct",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_mismatch_reduction_pct"
                        ),
                    ),
                    (
                        "flip_rotation_reduction",
                        etf_dynamic_v3_parameter_research.get("flip_rotation_reduction_id"),
                    ),
                    (
                        "flip_reduction_status",
                        etf_dynamic_v3_parameter_research.get("flip_reduction_status"),
                    ),
                    (
                        "rotation_reduction_status",
                        etf_dynamic_v3_parameter_research.get("rotation_reduction_status"),
                    ),
                    (
                        "filtered_candidate_ab_review",
                        etf_dynamic_v3_parameter_research.get("filtered_candidate_ab_review_id"),
                    ),
                    (
                        "filtered_candidate_ab_status",
                        etf_dynamic_v3_parameter_research.get("filtered_candidate_ab_status"),
                    ),
                    (
                        "filtered_candidate_ab_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_candidate_ab_next_action"
                        ),
                    ),
                    (
                        "signal_gate_confirmation",
                        etf_dynamic_v3_parameter_research.get("signal_gate_confirmation_id"),
                    ),
                    (
                        "confirmation_target_count",
                        etf_dynamic_v3_parameter_research.get(
                            "signal_gate_confirmation_target_count"
                        ),
                    ),
                    (
                        "confirmation_auto_apply",
                        etf_dynamic_v3_parameter_research.get(
                            "signal_gate_confirmation_auto_apply"
                        ),
                    ),
                    (
                        "formalization_readiness",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_formalization_readiness_id"
                        ),
                    ),
                    (
                        "formalization_decision",
                        etf_dynamic_v3_parameter_research.get("filtered_formalization_decision"),
                    ),
                    (
                        "formalization_confidence",
                        etf_dynamic_v3_parameter_research.get("filtered_formalization_confidence"),
                    ),
                    (
                        "can_write_official_target_weights",
                        etf_dynamic_v3_parameter_research.get(
                            "filtered_formalization_can_write_official_target_weights"
                        ),
                    ),
                    (
                        "owner_filtered_candidate_review",
                        etf_dynamic_v3_parameter_research.get(
                            "owner_filtered_candidate_review_id"
                        ),
                    ),
                    (
                        "owner_filtered_candidate_action",
                        etf_dynamic_v3_parameter_research.get("owner_filtered_candidate_action"),
                    ),
                    (
                        "owner_filtered_candidate_readiness",
                        etf_dynamic_v3_parameter_research.get(
                            "owner_filtered_candidate_readiness_decision"
                        ),
                    ),
                    (
                        "filtered_next_decision",
                        etf_dynamic_v3_parameter_research.get("filtered_next_decision"),
                    ),
                    (
                        "formal_research_method_contract",
                        etf_dynamic_v3_parameter_research.get(
                            "formal_research_method_contract_id"
                        ),
                    ),
                    (
                        "formal_research_method_status",
                        etf_dynamic_v3_parameter_research.get(
                            "formal_research_method_status"
                        ),
                    ),
                    (
                        "formal_research_method_promotion_state",
                        etf_dynamic_v3_parameter_research.get(
                            "formal_research_method_promotion_state"
                        ),
                    ),
                    (
                        "paper_shadow_eligibility",
                        etf_dynamic_v3_parameter_research.get(
                            "formal_research_method_paper_shadow_eligibility"
                        ),
                    ),
                    (
                        "contract_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "formal_research_method_validation_status"
                        ),
                    ),
                    (
                        "promotion_threshold_calibration",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_calibration_id"
                        ),
                    ),
                    (
                        "promotion_threshold_policy",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_policy_id"
                        ),
                    ),
                    (
                        "promotion_threshold_interpretation",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_current_interpretation"
                        ),
                    ),
                    (
                        "promotion_threshold_stress_required",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_stress_required"
                        ),
                    ),
                    (
                        "promotion_threshold_confirmation_minimum",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_confirmation_minimum"
                        ),
                    ),
                    (
                        "promotion_threshold_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "promotion_threshold_validation_status"
                        ),
                    ),
                    (
                        "paper_shadow_protocol",
                        etf_dynamic_v3_parameter_research.get("paper_shadow_protocol_id"),
                    ),
                    (
                        "paper_shadow_protocol_status",
                        etf_dynamic_v3_parameter_research.get("paper_shadow_protocol_status"),
                    ),
                    (
                        "paper_shadow_protocol_eligibility",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_protocol_eligibility_status"
                        ),
                    ),
                    (
                        "paper_shadow_min_observation_days",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_protocol_min_observation_days"
                        ),
                    ),
                    (
                        "paper_shadow_protocol_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_protocol_validation_status"
                        ),
                    ),
                    (
                        "paper_shadow_daily_observation",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_observation_id"
                        ),
                    ),
                    (
                        "paper_shadow_daily_candidate",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_candidate"
                        ),
                    ),
                    (
                        "paper_shadow_daily_date",
                        etf_dynamic_v3_parameter_research.get("paper_shadow_daily_date"),
                    ),
                    (
                        "paper_shadow_daily_status",
                        etf_dynamic_v3_parameter_research.get("paper_shadow_daily_status"),
                    ),
                    (
                        "paper_shadow_daily_signal",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_signal_output"
                        ),
                    ),
                    (
                        "paper_shadow_daily_risk_state",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_risk_state"
                        ),
                    ),
                    (
                        "paper_shadow_daily_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_next_action"
                        ),
                    ),
                    (
                        "paper_shadow_daily_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_daily_validation_status"
                        ),
                    ),
                    (
                        "paper_shadow_drift_monitor",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_monitor_id"
                        ),
                    ),
                    (
                        "paper_shadow_drift_observation",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_observation_id"
                        ),
                    ),
                    (
                        "paper_shadow_drift_severity",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_severity"
                        ),
                    ),
                    (
                        "paper_shadow_drift_blocking_count",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_blocking_count"
                        ),
                    ),
                    (
                        "paper_shadow_drift_warning_count",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_warning_count"
                        ),
                    ),
                    (
                        "paper_shadow_drift_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_next_action"
                        ),
                    ),
                    (
                        "paper_shadow_drift_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_drift_validation_status"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_review",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_review_id"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_candidate",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_candidate"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_window",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_window"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_decision",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_decision"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_coverage_classification",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_coverage_classification"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_coverage_status",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_coverage_status"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_coverage_safe_for_continuation",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_coverage_safe_for_continuation"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_missing_inputs",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_missing_inputs"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_drift_trend",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_drift_trend"
                        ),
                    ),
                    (
                        "paper_shadow_weekly_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "paper_shadow_weekly_validation_status"
                        ),
                    ),
                    (
                        "candidate_decision_ledger",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_ledger_id"
                        ),
                    ),
                    (
                        "candidate_decision_candidate",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_candidate"
                        ),
                    ),
                    (
                        "candidate_decision_final_decision",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_final_decision"
                        ),
                    ),
                    (
                        "candidate_decision_owner_action",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_owner_action"
                        ),
                    ),
                    (
                        "candidate_decision_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_next_action"
                        ),
                    ),
                    (
                        "candidate_decision_ledger_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "candidate_decision_ledger_validation_status"
                        ),
                    ),
                    (
                        "evidence_staleness_monitor",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_staleness_monitor_id"
                        ),
                    ),
                    (
                        "evidence_freshness_status",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_freshness_status"
                        ),
                    ),
                    (
                        "evidence_coverage_status",
                        etf_dynamic_v3_parameter_research.get("evidence_coverage_status"),
                    ),
                    (
                        "evidence_weekly_review_coverage_classification",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_weekly_review_coverage_classification"
                        ),
                    ),
                    (
                        "evidence_weekly_review_coverage_safe_for_continuation",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_weekly_review_coverage_safe_for_continuation"
                        ),
                    ),
                    (
                        "evidence_requested_as_of",
                        etf_dynamic_v3_parameter_research.get("evidence_requested_as_of"),
                    ),
                    (
                        "evidence_freshness_reference_date",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_freshness_reference_date"
                        ),
                    ),
                    (
                        "evidence_latest_complete_market_date",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_latest_complete_market_date"
                        ),
                    ),
                    (
                        "evidence_market_calendar_status",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_market_calendar_status"
                        ),
                    ),
                    (
                        "evidence_stale_artifacts",
                        etf_dynamic_v3_parameter_research.get("evidence_stale_artifacts"),
                    ),
                    (
                        "evidence_blocking_artifacts",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_blocking_artifacts"
                        ),
                    ),
                    (
                        "evidence_missing_artifacts",
                        etf_dynamic_v3_parameter_research.get("evidence_missing_artifacts"),
                    ),
                    (
                        "evidence_next_refresh_action",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_next_refresh_action"
                        ),
                    ),
                    (
                        "evidence_safe_to_continue_shadow",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_safe_to_continue_shadow"
                        ),
                    ),
                    (
                        "evidence_safety_boundary_status",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_safety_boundary_status"
                        ),
                    ),
                    (
                        "evidence_staleness_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "evidence_staleness_validation_status"
                        ),
                    ),
                    (
                        "shadow_continuation_readiness_id",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_readiness_id"
                        ),
                    ),
                    (
                        "shadow_continuation_readiness",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_readiness"
                        ),
                    ),
                    (
                        "shadow_continuation_safe_to_continue_shadow",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_safe_to_continue_shadow"
                        ),
                    ),
                    (
                        "shadow_continuation_missing_artifacts",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_missing_artifacts"
                        ),
                    ),
                    (
                        "shadow_continuation_blocking_artifacts",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_blocking_artifacts"
                        ),
                    ),
                    (
                        "shadow_continuation_stale_artifacts",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_stale_artifacts"
                        ),
                    ),
                    (
                        "shadow_continuation_coverage_status",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_coverage_status"
                        ),
                    ),
                    (
                        "shadow_continuation_manual_review_required",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_manual_review_required"
                        ),
                    ),
                    (
                        "shadow_continuation_next_required_action",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_next_required_action"
                        ),
                    ),
                    (
                        "shadow_continuation_data_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_data_validation_status"
                        ),
                    ),
                    (
                        "shadow_continuation_safety_boundary_status",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_safety_boundary_status"
                        ),
                    ),
                    (
                        "shadow_continuation_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "shadow_continuation_validation_status"
                        ),
                    ),
                    (
                        "stress_scenario_library",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_library_run_id"
                        ),
                    ),
                    (
                        "stress_scenario_count",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_count"
                        ),
                    ),
                    (
                        "stress_scenario_required_present",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_required_present"
                        ),
                    ),
                    (
                        "stress_scenario_candidate_validation_use",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_candidate_validation_use"
                        ),
                    ),
                    (
                        "stress_scenario_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_next_action"
                        ),
                    ),
                    (
                        "stress_scenario_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "stress_scenario_validation_status"
                        ),
                    ),
                    (
                        "drawdown_event_casebook",
                        etf_dynamic_v3_parameter_research.get("drawdown_casebook_run_id"),
                    ),
                    (
                        "drawdown_casebook_event_count",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_casebook_event_count"
                        ),
                    ),
                    (
                        "drawdown_casebook_worst_event",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_casebook_worst_event"
                        ),
                    ),
                    (
                        "drawdown_casebook_regime_coverage",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_casebook_regime_coverage"
                        ),
                    ),
                    (
                        "drawdown_casebook_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_casebook_next_action"
                        ),
                    ),
                    (
                        "drawdown_casebook_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "drawdown_casebook_validation_status"
                        ),
                    ),
                    (
                        "flip_rotation_event_casebook",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_casebook_run_id"
                        ),
                    ),
                    (
                        "flip_rotation_casebook_event_count",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_casebook_event_count"
                        ),
                    ),
                    (
                        "flip_rotation_useful_count",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_useful_count"
                        ),
                    ),
                    (
                        "flip_rotation_false_positive_count",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_false_positive_count"
                        ),
                    ),
                    (
                        "flip_rotation_dominant_trigger",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_dominant_trigger"
                        ),
                    ),
                    (
                        "flip_rotation_next_action",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_next_action"
                        ),
                    ),
                    (
                        "flip_rotation_casebook_validation_status",
                        etf_dynamic_v3_parameter_research.get(
                            "flip_rotation_casebook_validation_status"
                        ),
                    ),
                    (
                        "filtered_next_action",
                        etf_dynamic_v3_parameter_research.get("filtered_next_action"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_parameter_research.get("production_effect"),
                    ),
                    (
                        "automatic_candidate_promotion",
                        etf_dynamic_v3_parameter_research.get("automatic_candidate_promotion"),
                    ),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v3_parameter_research.get("shadow_enrollment_allowed"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Historical Replay Performance",
            _definition_table(
                [
                    (
                        "replay_inventory",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_id"),
                    ),
                    (
                        "replay_inventory_status",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_status"),
                    ),
                    (
                        "replay_inventory_events",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_total_events"),
                    ),
                    (
                        "pit_safe_count",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_pit_safe_count"),
                    ),
                    (
                        "pit_warning_count",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_pit_warning_count"),
                    ),
                    (
                        "pit_unsafe_count",
                        etf_dynamic_v3_parameter_research.get("replay_inventory_pit_unsafe_count"),
                    ),
                    (
                        "historical_replay",
                        etf_dynamic_v3_parameter_research.get("historical_replay_id"),
                    ),
                    (
                        "historical_replay_status",
                        etf_dynamic_v3_parameter_research.get("historical_replay_status"),
                    ),
                    (
                        "historical_replay_event_count",
                        etf_dynamic_v3_parameter_research.get("historical_replay_event_count"),
                    ),
                    (
                        "historical_replay_skipped_count",
                        etf_dynamic_v3_parameter_research.get("historical_replay_skipped_count"),
                    ),
                    (
                        "generated_variants",
                        etf_dynamic_v3_parameter_research.get(
                            "historical_replay_generated_variants"
                        ),
                    ),
                    (
                        "backfilled_outcome",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome_id"),
                    ),
                    (
                        "backfilled_outcome_status",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome_status"),
                    ),
                    (
                        "data_quality_status",
                        etf_dynamic_v3_parameter_research.get(
                            "backfilled_outcome_data_quality_status"
                        ),
                    ),
                    (
                        "available_count",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome_available_count"),
                    ),
                    (
                        "pending_count",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome_pending_count"),
                    ),
                    (
                        "insufficient_data_count",
                        etf_dynamic_v3_parameter_research.get(
                            "backfilled_outcome_insufficient_data_count"
                        ),
                    ),
                    (
                        "best_variant",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome_best_variant"),
                    ),
                    (
                        "limited_adjustment_vs_no_trade_5d",
                        etf_dynamic_v3_parameter_research.get("limited_adjustment_vs_no_trade_5d"),
                    ),
                    (
                        "historical_paper_sim",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_id"),
                    ),
                    (
                        "historical_paper_sim_status",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_status"),
                    ),
                    (
                        "historical_paper_sim_variant",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_variant"),
                    ),
                    (
                        "historical_paper_sim_total_return",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_total_return"),
                    ),
                    (
                        "historical_paper_sim_max_drawdown",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_max_drawdown"),
                    ),
                    (
                        "historical_paper_sim_turnover",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim_turnover"),
                    ),
                    (
                        "historical_paper_sim_relative_to_no_trade",
                        etf_dynamic_v3_parameter_research.get(
                            "historical_paper_sim_relative_to_no_trade"
                        ),
                    ),
                    (
                        "replay_performance_review",
                        etf_dynamic_v3_parameter_research.get("replay_performance_review_id"),
                    ),
                    (
                        "replay_performance_review_status",
                        etf_dynamic_v3_parameter_research.get("replay_performance_review_status"),
                    ),
                    (
                        "review_best_variant",
                        etf_dynamic_v3_parameter_research.get("replay_performance_best_variant"),
                    ),
                    (
                        "review_available_outcome_count",
                        etf_dynamic_v3_parameter_research.get(
                            "replay_performance_available_outcome_count"
                        ),
                    ),
                    (
                        "review_limited_adjustment_vs_no_trade",
                        etf_dynamic_v3_parameter_research.get(
                            "replay_performance_limited_adjustment_vs_no_trade"
                        ),
                    ),
                    (
                        "calibration_recommendation",
                        etf_dynamic_v3_parameter_research.get("replay_calibration_recommendation"),
                    ),
                    (
                        "calibration_priority",
                        etf_dynamic_v3_parameter_research.get("replay_calibration_priority"),
                    ),
                    (
                        "requires_owner_approval",
                        etf_dynamic_v3_parameter_research.get(
                            "replay_calibration_requires_owner_approval"
                        ),
                    ),
                    ("next_action", etf_dynamic_v3_parameter_research.get("replay_next_action")),
                    (
                        "outcome_update_review_status",
                        etf_dynamic_v3_parameter_research.get("outcome_update_review_status"),
                    ),
                    (
                        "outcome_update_ready_count",
                        etf_dynamic_v3_parameter_research.get("outcome_update_review_ready_count"),
                    ),
                    (
                        "outcome_update_review_future_data_used",
                        etf_dynamic_v3_parameter_research.get(
                            "outcome_update_review_future_data_used"
                        ),
                    ),
                    (
                        "outcome_update_status",
                        etf_dynamic_v3_parameter_research.get("outcome_update_status"),
                    ),
                    (
                        "outcome_update_updated_count",
                        etf_dynamic_v3_parameter_research.get("outcome_update_updated_count"),
                    ),
                    (
                        "outcome_update_skipped_count",
                        etf_dynamic_v3_parameter_research.get("outcome_update_skipped_count"),
                    ),
                    (
                        "outcome_update_forward_available_before_after",
                        (
                            f"{etf_dynamic_v3_parameter_research.get('outcome_update_forward_available_before')}"
                            " / "
                            f"{etf_dynamic_v3_parameter_research.get('outcome_update_forward_available_after')}"
                        ),
                    ),
                    (
                        "rolling_refresh_material_change",
                        etf_dynamic_v3_parameter_research.get(
                            "rolling_evidence_refresh_material_change"
                        ),
                    ),
                    (
                        "rolling_limited_vs_notrade_count_before_after",
                        (
                            f"{etf_dynamic_v3_parameter_research.get('rolling_limited_vs_notrade_count_before')}"
                            " / "
                            f"{etf_dynamic_v3_parameter_research.get('rolling_limited_vs_notrade_count_after')}"
                        ),
                    ),
                    (
                        "rolling_consensus_risk_before_after",
                        (
                            f"{etf_dynamic_v3_parameter_research.get('rolling_consensus_risk_before')}"
                            " / "
                            f"{etf_dynamic_v3_parameter_research.get('rolling_consensus_risk_after')}"
                        ),
                    ),
                    (
                        "evidence_trend_status",
                        etf_dynamic_v3_parameter_research.get("evidence_trend_status"),
                    ),
                    (
                        "evidence_trend_confidence_change",
                        etf_dynamic_v3_parameter_research.get("evidence_trend_confidence_change"),
                    ),
                    (
                        "evidence_trend_next_action",
                        etf_dynamic_v3_parameter_research.get("evidence_trend_next_action"),
                    ),
                    (
                        "forward_outcome_decision_action",
                        etf_dynamic_v3_parameter_research.get("forward_outcome_decision_action"),
                    ),
                    (
                        "forward_rule_calibration_readiness",
                        etf_dynamic_v3_parameter_research.get("forward_rule_calibration_readiness"),
                    ),
                    (
                        "forward_next_due_scan_date",
                        etf_dynamic_v3_parameter_research.get("forward_next_due_scan_date"),
                    ),
                    (
                        "historical_replay_broker_action_present",
                        etf_dynamic_v3_parameter_research.get(
                            "historical_replay_broker_action_present"
                        ),
                    ),
                    (
                        "safety_status",
                        etf_dynamic_v3_parameter_research.get("safety_status"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_parameter_research.get("production_effect"),
                    ),
                    ("broker_action", etf_dynamic_v3_parameter_research.get("broker_action")),
                    (
                        "production_candidate_generated",
                        etf_dynamic_v3_parameter_research.get("production_candidate_generated"),
                    ),
                    (
                        "automatic_candidate_promotion",
                        etf_dynamic_v3_parameter_research.get("automatic_candidate_promotion"),
                    ),
                    (
                        "shadow_enrollment_allowed",
                        etf_dynamic_v3_parameter_research.get("shadow_enrollment_allowed"),
                    ),
                    (
                        "replay_inventory_path",
                        etf_dynamic_v3_parameter_research.get("replay_inventory"),
                    ),
                    (
                        "historical_replay_path",
                        etf_dynamic_v3_parameter_research.get("historical_replay"),
                    ),
                    (
                        "backfilled_outcome_path",
                        etf_dynamic_v3_parameter_research.get("backfilled_outcome"),
                    ),
                    (
                        "historical_paper_sim_path",
                        etf_dynamic_v3_parameter_research.get("historical_paper_sim"),
                    ),
                    (
                        "replay_performance_review_path",
                        etf_dynamic_v3_parameter_research.get("replay_performance_review"),
                    ),
                    (
                        "outcome_update_review_path",
                        etf_dynamic_v3_parameter_research.get("outcome_update_review"),
                    ),
                    (
                        "outcome_update_path",
                        etf_dynamic_v3_parameter_research.get("outcome_update"),
                    ),
                    (
                        "rolling_evidence_refresh_path",
                        etf_dynamic_v3_parameter_research.get("rolling_evidence_refresh"),
                    ),
                    (
                        "evidence_trend_path",
                        etf_dynamic_v3_parameter_research.get("evidence_trend"),
                    ),
                    (
                        "forward_outcome_decision_path",
                        etf_dynamic_v3_parameter_research.get("forward_outcome_decision"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Manual Execution Review",
            _definition_table(
                [
                    (
                        "availability",
                        etf_dynamic_v3_manual_execution_review.get("availability"),
                    ),
                    ("status", etf_dynamic_v3_manual_execution_review.get("status")),
                    (
                        "summary",
                        etf_dynamic_v3_manual_execution_review.get("summary_sentence"),
                    ),
                    (
                        "manual_review_id",
                        etf_dynamic_v3_manual_execution_review.get("manual_review_id"),
                    ),
                    (
                        "snapshot_status",
                        etf_dynamic_v3_manual_execution_review.get("snapshot_status"),
                    ),
                    (
                        "exposure_status",
                        etf_dynamic_v3_manual_execution_review.get("exposure_status"),
                    ),
                    (
                        "drift_status",
                        etf_dynamic_v3_manual_execution_review.get("drift_status"),
                    ),
                    (
                        "candidate_agreement_status",
                        etf_dynamic_v3_manual_execution_review.get("candidate_agreement_status"),
                    ),
                    (
                        "guardrail_status",
                        etf_dynamic_v3_manual_execution_review.get("guardrail_status"),
                    ),
                    (
                        "recommended_action",
                        etf_dynamic_v3_manual_execution_review.get("recommended_action"),
                    ),
                    (
                        "owner_approval_required",
                        etf_dynamic_v3_manual_execution_review.get("owner_approval_required"),
                    ),
                    (
                        "broker_action_allowed",
                        etf_dynamic_v3_manual_execution_review.get("broker_action_allowed"),
                    ),
                    (
                        "broker_action_taken",
                        etf_dynamic_v3_manual_execution_review.get("broker_action_taken"),
                    ),
                    (
                        "order_ticket_generated",
                        etf_dynamic_v3_manual_execution_review.get("order_ticket_generated"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_manual_execution_review.get("production_effect"),
                    ),
                    (
                        "safety_status",
                        etf_dynamic_v3_manual_execution_review.get("safety_status"),
                    ),
                    (
                        "manual_execution_review_path",
                        etf_dynamic_v3_manual_execution_review.get("manual_execution_review_path"),
                    ),
                    (
                        "guardrail_path",
                        etf_dynamic_v3_manual_execution_review.get("guardrail_path"),
                    ),
                    (
                        "drift_path",
                        etf_dynamic_v3_manual_execution_review.get("drift_path"),
                    ),
                    (
                        "exposure_path",
                        etf_dynamic_v3_manual_execution_review.get("exposure_path"),
                    ),
                    (
                        "snapshot_path",
                        etf_dynamic_v3_manual_execution_review.get("snapshot_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Real Snapshot Advisory Review",
            _definition_table(
                [
                    ("availability", etf_dynamic_v3_real_snapshot_review.get("availability")),
                    ("status", etf_dynamic_v3_real_snapshot_review.get("status")),
                    ("summary", etf_dynamic_v3_real_snapshot_review.get("summary_sentence")),
                    (
                        "weekly_real_review_id",
                        etf_dynamic_v3_real_snapshot_review.get("weekly_real_review_id"),
                    ),
                    (
                        "snapshot_status",
                        etf_dynamic_v3_real_snapshot_review.get("snapshot_status"),
                    ),
                    (
                        "recommended_action",
                        etf_dynamic_v3_real_snapshot_review.get("recommended_action"),
                    ),
                    (
                        "owner_decision",
                        etf_dynamic_v3_real_snapshot_review.get("owner_decision"),
                    ),
                    (
                        "paper_action_taken",
                        etf_dynamic_v3_real_snapshot_review.get("paper_action_taken"),
                    ),
                    (
                        "broker_action_taken",
                        etf_dynamic_v3_real_snapshot_review.get("broker_action_taken"),
                    ),
                    (
                        "order_ticket_generated",
                        etf_dynamic_v3_real_snapshot_review.get("order_ticket_generated"),
                    ),
                    ("next_action", etf_dynamic_v3_real_snapshot_review.get("next_action")),
                    (
                        "production_effect",
                        etf_dynamic_v3_real_snapshot_review.get("production_effect"),
                    ),
                    (
                        "safety_status",
                        etf_dynamic_v3_real_snapshot_review.get("safety_status"),
                    ),
                    (
                        "weekly_real_snapshot_review_path",
                        etf_dynamic_v3_real_snapshot_review.get("weekly_real_snapshot_review_path"),
                    ),
                    (
                        "dry_run_path",
                        etf_dynamic_v3_real_snapshot_review.get("dry_run_path"),
                    ),
                    (
                        "owner_review_path",
                        etf_dynamic_v3_real_snapshot_review.get("owner_review_path"),
                    ),
                    (
                        "paper_action_path",
                        etf_dynamic_v3_real_snapshot_review.get("paper_action_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue System Target Portfolio",
            _definition_table(
                [
                    ("availability", etf_dynamic_v3_system_target.get("availability")),
                    ("status", etf_dynamic_v3_system_target.get("status")),
                    ("summary", etf_dynamic_v3_system_target.get("summary_sentence")),
                    ("target_id", etf_dynamic_v3_system_target.get("target_id")),
                    ("paper_shadow_id", etf_dynamic_v3_system_target.get("paper_shadow_id")),
                    ("rebalance_id", etf_dynamic_v3_system_target.get("rebalance_id")),
                    ("performance_id", etf_dynamic_v3_system_target.get("performance_id")),
                    ("review_id", etf_dynamic_v3_system_target.get("review_id")),
                    (
                        "recommended_research_method",
                        etf_dynamic_v3_system_target.get("recommended_research_method"),
                    ),
                    ("decision_status", etf_dynamic_v3_system_target.get("decision_status")),
                    (
                        "selection_review_id",
                        etf_dynamic_v3_system_target.get("selection_review_id"),
                    ),
                    ("hardening_id", etf_dynamic_v3_system_target.get("hardening_id")),
                    (
                        "hardening_decision",
                        etf_dynamic_v3_system_target.get("hardening_decision"),
                    ),
                    (
                        "hardening_confidence",
                        etf_dynamic_v3_system_target.get("hardening_decision_confidence"),
                    ),
                    (
                        "hardening_blockers",
                        etf_dynamic_v3_system_target.get("hardening_blocking_issues"),
                    ),
                    (
                        "refined_proposal_id",
                        etf_dynamic_v3_system_target.get("refined_proposal_id"),
                    ),
                    (
                        "refined_recommended_next_step",
                        etf_dynamic_v3_system_target.get("refined_recommended_next_step"),
                    ),
                    (
                        "refined_proposed_next_methods",
                        etf_dynamic_v3_system_target.get("refined_proposed_next_methods"),
                    ),
                    (
                        "refined_confidence",
                        etf_dynamic_v3_system_target.get("refined_confidence"),
                    ),
                    ("risk_capped_id", etf_dynamic_v3_system_target.get("risk_capped_id")),
                    (
                        "risk_capped_backfill_id",
                        etf_dynamic_v3_system_target.get("risk_capped_backfill_id"),
                    ),
                    (
                        "risk_capped_comparison_id",
                        etf_dynamic_v3_system_target.get("risk_capped_comparison_id"),
                    ),
                    (
                        "risk_capped_review_id",
                        etf_dynamic_v3_system_target.get("risk_capped_review_id"),
                    ),
                    (
                        "risk_capped_decision",
                        etf_dynamic_v3_system_target.get("risk_capped_decision"),
                    ),
                    (
                        "risk_capped_confidence",
                        etf_dynamic_v3_system_target.get("risk_capped_confidence"),
                    ),
                    (
                        "risk_capped_improvements_vs_limited",
                        etf_dynamic_v3_system_target.get("risk_capped_improvements_vs_limited"),
                    ),
                    (
                        "risk_capped_return_delta_vs_limited",
                        etf_dynamic_v3_system_target.get("risk_capped_return_delta_vs_limited"),
                    ),
                    (
                        "risk_capped_drawdown_delta_vs_limited",
                        etf_dynamic_v3_system_target.get("risk_capped_drawdown_delta_vs_limited"),
                    ),
                    (
                        "risk_capped_semiconductor_exposure_delta",
                        etf_dynamic_v3_system_target.get(
                            "risk_capped_semiconductor_exposure_delta"
                        ),
                    ),
                    (
                        "risk_capped_rolling_consistency_delta",
                        etf_dynamic_v3_system_target.get("risk_capped_rolling_consistency_delta"),
                    ),
                    (
                        "risk_capped_generated_cap_event_count",
                        etf_dynamic_v3_system_target.get("risk_capped_generated_cap_event_count"),
                    ),
                    (
                        "risk_capped_backfill_cap_event_count",
                        etf_dynamic_v3_system_target.get("risk_capped_backfill_cap_event_count"),
                    ),
                    (
                        "risk_capped_reallocated_to_cash",
                        etf_dynamic_v3_system_target.get("risk_capped_reallocated_to_cash"),
                    ),
                    (
                        "risk_capped_requires_forward_confirmation",
                        etf_dynamic_v3_system_target.get(
                            "risk_capped_requires_forward_confirmation"
                        ),
                    ),
                    ("smoothed_id", etf_dynamic_v3_system_target.get("smoothed_id")),
                    (
                        "smoothed_backfill_id",
                        etf_dynamic_v3_system_target.get("smoothed_backfill_id"),
                    ),
                    (
                        "smoothed_comparison_id",
                        etf_dynamic_v3_system_target.get("smoothed_comparison_id"),
                    ),
                    (
                        "smoothed_review_id",
                        etf_dynamic_v3_system_target.get("smoothed_review_id"),
                    ),
                    (
                        "smoothed_decision",
                        etf_dynamic_v3_system_target.get("smoothed_decision"),
                    ),
                    (
                        "smoothed_recommended_method",
                        etf_dynamic_v3_system_target.get("smoothed_recommended_method"),
                    ),
                    (
                        "smoothed_confidence",
                        etf_dynamic_v3_system_target.get("smoothed_confidence"),
                    ),
                    (
                        "smoothed_improvements_vs_limited",
                        etf_dynamic_v3_system_target.get("smoothed_improvements_vs_limited"),
                    ),
                    (
                        "smoothed_return_delta_vs_limited",
                        etf_dynamic_v3_system_target.get("smoothed_return_delta_vs_limited"),
                    ),
                    (
                        "smoothed_drawdown_delta_vs_limited",
                        etf_dynamic_v3_system_target.get("smoothed_drawdown_delta_vs_limited"),
                    ),
                    (
                        "smoothed_turnover_delta_vs_limited",
                        etf_dynamic_v3_system_target.get("smoothed_turnover_delta_vs_limited"),
                    ),
                    (
                        "smoothed_rolling_consistency_delta",
                        etf_dynamic_v3_system_target.get("smoothed_rolling_consistency_delta"),
                    ),
                    (
                        "smoothed_lag_risk",
                        etf_dynamic_v3_system_target.get("smoothed_lag_risk"),
                    ),
                    (
                        "smoothed_requires_forward_confirmation",
                        etf_dynamic_v3_system_target.get("smoothed_requires_forward_confirmation"),
                    ),
                    (
                        "smoothed_watch_pack_id",
                        etf_dynamic_v3_system_target.get("smoothed_watch_pack_id"),
                    ),
                    (
                        "smoothed_watch_current_decision",
                        etf_dynamic_v3_system_target.get("smoothed_watch_current_decision"),
                    ),
                    (
                        "smoothed_watch_benefit_lag_tradeoff",
                        etf_dynamic_v3_system_target.get("smoothed_watch_benefit_lag_tradeoff"),
                    ),
                    (
                        "smoothed_watch_sideways_validation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_watch_sideways_validation_status"
                        ),
                    ),
                    (
                        "smoothed_watch_recovery_lag_status",
                        etf_dynamic_v3_system_target.get("smoothed_watch_recovery_lag_status"),
                    ),
                    (
                        "smoothed_watch_forward_confirmation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_watch_forward_confirmation_status"
                        ),
                    ),
                    (
                        "smoothed_watch_recommended_action",
                        etf_dynamic_v3_system_target.get("smoothed_watch_recommended_action"),
                    ),
                    (
                        "smoothed_owner_update_id",
                        etf_dynamic_v3_system_target.get("smoothed_owner_update_id"),
                    ),
                    (
                        "smoothed_owner_readiness_decision",
                        etf_dynamic_v3_system_target.get("smoothed_owner_readiness_decision"),
                    ),
                    (
                        "smoothed_owner_recommended_action",
                        etf_dynamic_v3_system_target.get("smoothed_owner_recommended_action"),
                    ),
                    (
                        "smoothed_owner_forward_confirmation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_owner_forward_confirmation_status"
                        ),
                    ),
                    (
                        "smoothed_promotion_review_id",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_review_id"),
                    ),
                    (
                        "primary_research_candidate_gate_decision",
                        etf_dynamic_v3_system_target.get(
                            "primary_research_candidate_gate_decision"
                        ),
                    ),
                    (
                        "smoothed_forward_binding_id",
                        etf_dynamic_v3_system_target.get("smoothed_forward_binding_id"),
                    ),
                    (
                        "paper_shadow_primary_switch_auto_switch",
                        etf_dynamic_v3_system_target.get("paper_shadow_primary_switch_auto_switch"),
                    ),
                    (
                        "smoothed_owner_promotion_decision",
                        etf_dynamic_v3_system_target.get("smoothed_owner_promotion_decision"),
                    ),
                    (
                        "smoothed_forward_progress",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_progress_forward_events"
                        ),
                    ),
                    (
                        "smoothed_weekly_recommendation",
                        etf_dynamic_v3_system_target.get("smoothed_weekly_recommendation"),
                    ),
                    (
                        "smoothed_switch_recheck_decision",
                        etf_dynamic_v3_system_target.get("smoothed_switch_recheck_decision"),
                    ),
                    (
                        "smoothed_owner_renewal_action",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_owner_renewal_recommended_action"
                        ),
                    ),
                    (
                        "smoothed_sample_bootstrap",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_weekly_run_recommendation"
                        ),
                    ),
                    (
                        "experiment_triage_id",
                        etf_dynamic_v3_system_target.get("experiment_triage_id"),
                    ),
                    (
                        "experiment_batch_id",
                        etf_dynamic_v3_system_target.get("experiment_batch_id"),
                    ),
                    (
                        "experiment_matrix_id",
                        etf_dynamic_v3_system_target.get("experiment_matrix_id"),
                    ),
                    (
                        "experiment_top_variant",
                        etf_dynamic_v3_system_target.get("experiment_top_variant"),
                    ),
                    (
                        "experiment_promote_count",
                        etf_dynamic_v3_system_target.get("experiment_promote_count"),
                    ),
                    (
                        "experiment_keep_testing_count",
                        etf_dynamic_v3_system_target.get("experiment_keep_testing_count"),
                    ),
                    (
                        "experiment_reject_count",
                        etf_dynamic_v3_system_target.get("experiment_reject_count"),
                    ),
                    (
                        "experiment_top_promoted_variants",
                        etf_dynamic_v3_system_target.get("experiment_top_promoted_variants"),
                    ),
                    (
                        "top_variant_interpretation_id",
                        etf_dynamic_v3_system_target.get("top_variant_interpretation_id"),
                    ),
                    (
                        "best_experiment_variant",
                        etf_dynamic_v3_system_target.get("best_experiment_variant"),
                    ),
                    (
                        "top_variant_solved_failure_modes",
                        etf_dynamic_v3_system_target.get("top_variant_solved_failure_modes"),
                    ),
                    (
                        "top_variant_expected_costs",
                        etf_dynamic_v3_system_target.get("top_variant_expected_costs"),
                    ),
                    (
                        "method_promotion_plan_id",
                        etf_dynamic_v3_system_target.get("method_promotion_plan_id"),
                    ),
                    (
                        "proposed_method_names",
                        etf_dynamic_v3_system_target.get("proposed_method_names"),
                    ),
                    (
                        "promotion_implementation_scope",
                        etf_dynamic_v3_system_target.get("promotion_implementation_scope"),
                    ),
                    (
                        "promotion_next_action",
                        etf_dynamic_v3_system_target.get("promotion_next_action"),
                    ),
                    (
                        "secondary_research_methods",
                        etf_dynamic_v3_system_target.get("secondary_research_methods"),
                    ),
                    (
                        "reference_only_methods",
                        etf_dynamic_v3_system_target.get("reference_only_methods"),
                    ),
                    (
                        "data_quality_status",
                        etf_dynamic_v3_system_target.get("data_quality_status"),
                    ),
                    ("tracked_methods", etf_dynamic_v3_system_target.get("tracked_methods")),
                    (
                        "best_return_method",
                        etf_dynamic_v3_system_target.get("best_return_method"),
                    ),
                    (
                        "best_risk_adjusted_method",
                        etf_dynamic_v3_system_target.get("best_risk_adjusted_method"),
                    ),
                    (
                        "broker_action_allowed",
                        etf_dynamic_v3_system_target.get("broker_action_allowed"),
                    ),
                    (
                        "broker_action_taken",
                        etf_dynamic_v3_system_target.get("broker_action_taken"),
                    ),
                    (
                        "not_official_target_weights",
                        etf_dynamic_v3_system_target.get("not_official_target_weights"),
                    ),
                    ("production_effect", etf_dynamic_v3_system_target.get("production_effect")),
                    ("safety_status", etf_dynamic_v3_system_target.get("safety_status")),
                    ("next_action", etf_dynamic_v3_system_target.get("next_action")),
                    (
                        "system_target_review_path",
                        etf_dynamic_v3_system_target.get("system_target_review_path"),
                    ),
                    ("model_target_path", etf_dynamic_v3_system_target.get("model_target_path")),
                    ("paper_shadow_path", etf_dynamic_v3_system_target.get("paper_shadow_path")),
                    (
                        "model_rebalance_path",
                        etf_dynamic_v3_system_target.get("model_rebalance_path"),
                    ),
                    (
                        "paper_shadow_performance_path",
                        etf_dynamic_v3_system_target.get("paper_shadow_performance_path"),
                    ),
                    ("hardening_path", etf_dynamic_v3_system_target.get("hardening_path")),
                    (
                        "refined_proposal_path",
                        etf_dynamic_v3_system_target.get("refined_proposal_path"),
                    ),
                    ("risk_capped_path", etf_dynamic_v3_system_target.get("risk_capped_path")),
                    (
                        "risk_capped_backfill_path",
                        etf_dynamic_v3_system_target.get("risk_capped_backfill_path"),
                    ),
                    (
                        "risk_capped_comparison_path",
                        etf_dynamic_v3_system_target.get("risk_capped_comparison_path"),
                    ),
                    (
                        "risk_capped_review_path",
                        etf_dynamic_v3_system_target.get("risk_capped_review_path"),
                    ),
                    ("smoothed_path", etf_dynamic_v3_system_target.get("smoothed_path")),
                    (
                        "smoothed_backfill_path",
                        etf_dynamic_v3_system_target.get("smoothed_backfill_path"),
                    ),
                    (
                        "smoothed_comparison_path",
                        etf_dynamic_v3_system_target.get("smoothed_comparison_path"),
                    ),
                    (
                        "smoothed_review_path",
                        etf_dynamic_v3_system_target.get("smoothed_review_path"),
                    ),
                    (
                        "smoothed_watch_pack_path",
                        etf_dynamic_v3_system_target.get("smoothed_watch_pack_path"),
                    ),
                    (
                        "smoothed_owner_update_path",
                        etf_dynamic_v3_system_target.get("smoothed_owner_update_path"),
                    ),
                    (
                        "smoothed_promotion_review_path",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_review_path"),
                    ),
                    (
                        "primary_research_candidate_gate_path",
                        etf_dynamic_v3_system_target.get("primary_research_candidate_gate_path"),
                    ),
                    (
                        "smoothed_forward_binding_path",
                        etf_dynamic_v3_system_target.get("smoothed_forward_binding_path"),
                    ),
                    (
                        "paper_shadow_primary_switch_path",
                        etf_dynamic_v3_system_target.get("paper_shadow_primary_switch_path"),
                    ),
                    (
                        "smoothed_owner_promotion_path",
                        etf_dynamic_v3_system_target.get("smoothed_owner_promotion_path"),
                    ),
                    (
                        "experiment_triage_path",
                        etf_dynamic_v3_system_target.get("experiment_triage_path"),
                    ),
                    (
                        "top_variant_interpretation_path",
                        etf_dynamic_v3_system_target.get("top_variant_interpretation_path"),
                    ),
                    (
                        "method_promotion_plan_path",
                        etf_dynamic_v3_system_target.get("method_promotion_plan_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Method Watch",
            _definition_table(
                [
                    (
                        "watch_pack_id",
                        etf_dynamic_v3_system_target.get("smoothed_watch_pack_id"),
                    ),
                    (
                        "candidate_method",
                        etf_dynamic_v3_system_target.get("smoothed_recommended_method"),
                    ),
                    (
                        "current_decision",
                        etf_dynamic_v3_system_target.get("smoothed_watch_current_decision"),
                    ),
                    (
                        "benefit_lag_tradeoff",
                        etf_dynamic_v3_system_target.get("smoothed_watch_benefit_lag_tradeoff"),
                    ),
                    (
                        "sideways_validation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_watch_sideways_validation_status"
                        ),
                    ),
                    (
                        "recovery_lag_status",
                        etf_dynamic_v3_system_target.get("smoothed_watch_recovery_lag_status"),
                    ),
                    (
                        "forward_confirmation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_watch_forward_confirmation_status"
                        ),
                    ),
                    (
                        "recommended_action",
                        etf_dynamic_v3_system_target.get("smoothed_watch_recommended_action"),
                    ),
                    (
                        "broker_action_allowed",
                        etf_dynamic_v3_system_target.get("broker_action_allowed"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "watch_pack_path",
                        etf_dynamic_v3_system_target.get("smoothed_watch_pack_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Owner Review",
            _definition_table(
                [
                    (
                        "owner_update_id",
                        etf_dynamic_v3_system_target.get("smoothed_owner_update_id"),
                    ),
                    (
                        "readiness_decision",
                        etf_dynamic_v3_system_target.get("smoothed_owner_readiness_decision"),
                    ),
                    (
                        "recommended_owner_action",
                        etf_dynamic_v3_system_target.get("smoothed_owner_recommended_action"),
                    ),
                    (
                        "forward_confirmation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_owner_forward_confirmation_status"
                        ),
                    ),
                    (
                        "broker_action_allowed",
                        etf_dynamic_v3_system_target.get("broker_action_allowed"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "owner_update_path",
                        etf_dynamic_v3_system_target.get("smoothed_owner_update_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Promotion Decision",
            _definition_table(
                [
                    (
                        "promotion_review_id",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_review_id"),
                    ),
                    (
                        "readiness_decision",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_readiness_decision"),
                    ),
                    (
                        "decision_confidence",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_decision_confidence"),
                    ),
                    (
                        "can_enter_owner_review",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_promotion_can_enter_owner_review"
                        ),
                    ),
                    (
                        "supporting_evidence",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_supporting_evidence"),
                    ),
                    (
                        "blocking_issues",
                        etf_dynamic_v3_system_target.get("smoothed_promotion_blocking_issues"),
                    ),
                    (
                        "gate_decision",
                        etf_dynamic_v3_system_target.get(
                            "primary_research_candidate_gate_decision"
                        ),
                    ),
                    (
                        "owner_approval_required",
                        etf_dynamic_v3_system_target.get(
                            "primary_research_candidate_owner_approval_required"
                        ),
                    ),
                    (
                        "paper_shadow_update_allowed",
                        etf_dynamic_v3_system_target.get(
                            "primary_research_candidate_update_allowed"
                        ),
                    ),
                    (
                        "bound_targets",
                        etf_dynamic_v3_system_target.get("smoothed_forward_binding_bound_targets"),
                    ),
                    (
                        "watch_only_targets",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_binding_watch_only_targets"
                        ),
                    ),
                    (
                        "proposed_primary_research_candidate",
                        etf_dynamic_v3_system_target.get(
                            "paper_shadow_primary_switch_proposed_candidate"
                        ),
                    ),
                    (
                        "auto_switch",
                        etf_dynamic_v3_system_target.get("paper_shadow_primary_switch_auto_switch"),
                    ),
                    (
                        "rollback_method",
                        etf_dynamic_v3_system_target.get(
                            "paper_shadow_primary_switch_rollback_method"
                        ),
                    ),
                    (
                        "owner_decision",
                        etf_dynamic_v3_system_target.get("smoothed_owner_promotion_decision"),
                    ),
                    (
                        "paper_shadow_primary_candidate_change_allowed",
                        etf_dynamic_v3_system_target.get("smoothed_owner_promotion_change_allowed"),
                    ),
                    (
                        "broker_action_allowed",
                        etf_dynamic_v3_system_target.get("broker_action_allowed"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "owner_promotion_path",
                        etf_dynamic_v3_system_target.get("smoothed_owner_promotion_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Owner Renewal",
            _definition_table(
                [
                    (
                        "forward_progress_id",
                        etf_dynamic_v3_system_target.get("smoothed_forward_progress_id"),
                    ),
                    (
                        "forward_events",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_progress_forward_events"
                        ),
                    ),
                    (
                        "sideways_events",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_progress_sideways_events"
                        ),
                    ),
                    (
                        "recovery_events",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_progress_recovery_events"
                        ),
                    ),
                    (
                        "progress_statuses",
                        etf_dynamic_v3_system_target.get("smoothed_forward_progress_statuses"),
                    ),
                    (
                        "weekly_dashboard_id",
                        etf_dynamic_v3_system_target.get("smoothed_weekly_dashboard_id"),
                    ),
                    (
                        "forward_confirmation_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_weekly_forward_confirmation_status"
                        ),
                    ),
                    (
                        "ready_for_switch_recheck",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_weekly_ready_for_switch_recheck"
                        ),
                    ),
                    (
                        "weekly_recommendation",
                        etf_dynamic_v3_system_target.get("smoothed_weekly_recommendation"),
                    ),
                    (
                        "event_monitor_id",
                        etf_dynamic_v3_system_target.get("smoothed_event_monitor_id"),
                    ),
                    (
                        "event_sideways_available_required",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_event_sideways_available_required"
                        ),
                    ),
                    (
                        "event_recovery_available_required",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_event_recovery_available_required"
                        ),
                    ),
                    (
                        "recovery_lag_status",
                        etf_dynamic_v3_system_target.get("smoothed_event_recovery_lag_status"),
                    ),
                    (
                        "lag_warning_count",
                        etf_dynamic_v3_system_target.get("smoothed_event_lag_warning_count"),
                    ),
                    (
                        "switch_readiness_id",
                        etf_dynamic_v3_system_target.get("smoothed_switch_readiness_id"),
                    ),
                    (
                        "recheck_decision",
                        etf_dynamic_v3_system_target.get("smoothed_switch_recheck_decision"),
                    ),
                    (
                        "criteria_not_met",
                        etf_dynamic_v3_system_target.get("smoothed_switch_criteria_not_met"),
                    ),
                    (
                        "can_execute_switch",
                        etf_dynamic_v3_system_target.get("smoothed_switch_can_execute_switch"),
                    ),
                    (
                        "owner_renewal_id",
                        etf_dynamic_v3_system_target.get("smoothed_owner_renewal_id"),
                    ),
                    (
                        "previous_owner_decision",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_owner_renewal_previous_decision"
                        ),
                    ),
                    (
                        "owner_renewal_recheck_decision",
                        etf_dynamic_v3_system_target.get("smoothed_owner_renewal_recheck_decision"),
                    ),
                    (
                        "recommended_owner_action",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_owner_renewal_recommended_action"
                        ),
                    ),
                    (
                        "owner_options",
                        etf_dynamic_v3_system_target.get("smoothed_owner_renewal_options"),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "owner_renewal_path",
                        etf_dynamic_v3_system_target.get("smoothed_owner_renewal_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Forward Sample Bootstrap",
            _definition_table(
                [
                    (
                        "daily_emission_id",
                        etf_dynamic_v3_system_target.get("smoothed_daily_emission_id"),
                    ),
                    (
                        "daily_emission_as_of",
                        etf_dynamic_v3_system_target.get("smoothed_daily_emission_as_of"),
                    ),
                    (
                        "daily_event_status",
                        etf_dynamic_v3_system_target.get("smoothed_daily_emission_event_status"),
                    ),
                    (
                        "daily_data_quality",
                        etf_dynamic_v3_system_target.get("smoothed_daily_emission_data_quality"),
                    ),
                    (
                        "emitted_event_count",
                        etf_dynamic_v3_system_target.get("smoothed_daily_emission_event_count"),
                    ),
                    (
                        "outcome_due_id",
                        etf_dynamic_v3_system_target.get("smoothed_outcome_due_id"),
                    ),
                    (
                        "due_windows",
                        etf_dynamic_v3_system_target.get("smoothed_outcome_due_windows"),
                    ),
                    (
                        "update_ready_count",
                        etf_dynamic_v3_system_target.get("smoothed_outcome_due_update_ready_count"),
                    ),
                    (
                        "blocked_future_as_of",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_outcome_due_blocked_future_as_of"
                        ),
                    ),
                    (
                        "outcome_update_id",
                        etf_dynamic_v3_system_target.get("smoothed_outcome_update_id"),
                    ),
                    (
                        "updated_skipped_windows",
                        etf_dynamic_v3_system_target.get("smoothed_outcome_update_updated_skipped"),
                    ),
                    (
                        "available_forward_events_after_update",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_outcome_update_available_forward_events"
                        ),
                    ),
                    (
                        "classification_id",
                        etf_dynamic_v3_system_target.get("smoothed_forward_classification_id"),
                    ),
                    (
                        "sideways_events",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_classification_sideways_events"
                        ),
                    ),
                    (
                        "recovery_events",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_classification_recovery_events"
                        ),
                    ),
                    (
                        "lag_warnings",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_classification_lag_warnings"
                        ),
                    ),
                    (
                        "weekly_run_id",
                        etf_dynamic_v3_system_target.get("smoothed_forward_weekly_run_id"),
                    ),
                    (
                        "weekly_recommendation",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_weekly_run_recommendation"
                        ),
                    ),
                    (
                        "weekly_can_execute_switch",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_forward_weekly_run_can_execute_switch"
                        ),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "weekly_run_path",
                        etf_dynamic_v3_system_target.get("smoothed_forward_weekly_run_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Freshness Bootstrap",
            _definition_table(
                [
                    (
                        "data_preflight_id",
                        etf_dynamic_v3_system_target.get("smoothed_data_preflight_id"),
                    ),
                    (
                        "freshness_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_data_preflight_freshness_status"
                        ),
                    ),
                    (
                        "latest_valid_as_of",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_data_preflight_latest_valid_as_of"
                        ),
                    ),
                    (
                        "validate_data_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_data_preflight_validate_data_status"
                        ),
                    ),
                    (
                        "blocking_errors",
                        etf_dynamic_v3_system_target.get("smoothed_data_preflight_blocking_errors"),
                    ),
                    (
                        "latest_available_fallback_commands",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_data_preflight_latest_available_fallback_commands"
                        ),
                    ),
                    (
                        "latest_emission_id",
                        etf_dynamic_v3_system_target.get("smoothed_latest_emission_id"),
                    ),
                    (
                        "latest_emission_resolved_as_of",
                        etf_dynamic_v3_system_target.get("smoothed_latest_emission_resolved_as_of"),
                    ),
                    (
                        "latest_emission_event_status",
                        etf_dynamic_v3_system_target.get("smoothed_latest_emission_event_status"),
                    ),
                    (
                        "outcome_update_allowed",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_latest_emission_outcome_update_allowed"
                        ),
                    ),
                    (
                        "blocked_explain_id",
                        etf_dynamic_v3_system_target.get("smoothed_blocked_explain_id"),
                    ),
                    (
                        "blocked_commands",
                        etf_dynamic_v3_system_target.get("smoothed_blocked_explain_commands"),
                    ),
                    (
                        "refresh_plan_id",
                        etf_dynamic_v3_system_target.get("smoothed_refresh_plan_id"),
                    ),
                    (
                        "required_sources",
                        etf_dynamic_v3_system_target.get("smoothed_refresh_plan_required_sources"),
                    ),
                    (
                        "rerun_allowed_now",
                        etf_dynamic_v3_system_target.get("smoothed_refresh_plan_rerun_allowed_now"),
                    ),
                    (
                        "bootstrap_retry_id",
                        etf_dynamic_v3_system_target.get("smoothed_bootstrap_retry_id"),
                    ),
                    (
                        "bootstrap_retry_status",
                        etf_dynamic_v3_system_target.get("smoothed_bootstrap_retry_status"),
                    ),
                    (
                        "retry_preflight_status",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_bootstrap_retry_preflight_status"
                        ),
                    ),
                    (
                        "retry_updated_windows",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_bootstrap_retry_updated_windows"
                        ),
                    ),
                    (
                        "retry_can_execute_switch",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_bootstrap_retry_can_execute_switch"
                        ),
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "bootstrap_retry_path",
                        etf_dynamic_v3_system_target.get("smoothed_bootstrap_retry_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Smoothed Data Readiness",
            _definition_table(
                [
                    (
                        "source_refresh_id",
                        etf_dynamic_v3_system_target.get("smoothed_source_refresh_id"),
                    ),
                    (
                        "source_refresh_status",
                        etf_dynamic_v3_system_target.get("smoothed_source_refresh_status"),
                    ),
                    (
                        "ready_source_count",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_source_refresh_ready_source_count"
                        ),
                    ),
                    (
                        "external_refresh_executed",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_source_refresh_external_refresh_executed"
                        ),
                    ),
                    (
                        "post_refresh_id",
                        etf_dynamic_v3_system_target.get("smoothed_post_refresh_id"),
                    ),
                    (
                        "post_refresh_retry_decision",
                        etf_dynamic_v3_system_target.get("smoothed_post_refresh_retry_decision"),
                    ),
                    (
                        "post_refresh_blocking_errors",
                        etf_dynamic_v3_system_target.get("smoothed_post_refresh_blocking_errors"),
                    ),
                    (
                        "retry_resume_id",
                        etf_dynamic_v3_system_target.get("smoothed_retry_resume_id"),
                    ),
                    (
                        "retry_resume_status",
                        etf_dynamic_v3_system_target.get("smoothed_retry_resume_status"),
                    ),
                    (
                        "retry_resume_updated_windows",
                        etf_dynamic_v3_system_target.get("smoothed_retry_resume_updated_windows"),
                    ),
                    (
                        "sample_growth_status",
                        etf_dynamic_v3_system_target.get("smoothed_sample_growth_status"),
                    ),
                    (
                        "sample_growth_delta",
                        "forward="
                        f"{etf_dynamic_v3_system_target.get('smoothed_sample_growth_forward_delta')},"
                        "sideways="
                        f"{etf_dynamic_v3_system_target.get('smoothed_sample_growth_sideways_delta')},"
                        "recovery="
                        f"{etf_dynamic_v3_system_target.get('smoothed_sample_growth_recovery_delta')}",
                    ),
                    (
                        "readiness_id",
                        etf_dynamic_v3_system_target.get("smoothed_data_readiness_id"),
                    ),
                    (
                        "current_status",
                        etf_dynamic_v3_system_target.get("smoothed_data_readiness_current_status"),
                    ),
                    (
                        "recommended_owner_action",
                        etf_dynamic_v3_system_target.get(
                            "smoothed_data_readiness_recommended_owner_action"
                        ),
                    ),
                    (
                        "source_statuses",
                        etf_dynamic_v3_system_target.get("smoothed_data_readiness_source_statuses"),
                    ),
                    (
                        "readiness_progress",
                        "forward="
                        f"{etf_dynamic_v3_system_target.get('smoothed_data_readiness_forward_progress')},"
                        "sideways="
                        f"{etf_dynamic_v3_system_target.get('smoothed_data_readiness_sideways_progress')},"
                        "recovery="
                        f"{etf_dynamic_v3_system_target.get('smoothed_data_readiness_recovery_progress')}",
                    ),
                    (
                        "production_effect",
                        etf_dynamic_v3_system_target.get("production_effect"),
                    ),
                    (
                        "readiness_path",
                        etf_dynamic_v3_system_target.get("smoothed_data_readiness_path"),
                    ),
                ]
            ),
        ),
        _section(
            "Dynamic Rescue Simulation Advisory Review",
            _definition_table(
                [
                    ("availability", etf_dynamic_v3_sim_review.get("availability")),
                    ("status", etf_dynamic_v3_sim_review.get("status")),
                    ("summary", etf_dynamic_v3_sim_review.get("summary_sentence")),
                    ("interpretation_id", etf_dynamic_v3_sim_review.get("interpretation_id")),
                    ("risk_return_id", etf_dynamic_v3_sim_review.get("risk_return_id")),
                    (
                        "defensive_validation_id",
                        etf_dynamic_v3_sim_review.get("defensive_validation_id"),
                    ),
                    (
                        "proposal_review_id",
                        etf_dynamic_v3_sim_review.get("proposal_review_id"),
                    ),
                    (
                        "confirmation_plan_id",
                        etf_dynamic_v3_sim_review.get("confirmation_plan_id"),
                    ),
                    ("variant_roles", etf_dynamic_v3_sim_review.get("variant_roles")),
                    ("key_findings", etf_dynamic_v3_sim_review.get("key_findings")),
                    (
                        "risk_return_statuses",
                        etf_dynamic_v3_sim_review.get("risk_return_statuses"),
                    ),
                    (
                        "defensive_status",
                        etf_dynamic_v3_sim_review.get("defensive_status"),
                    ),
                    (
                        "proposal_decisions",
                        etf_dynamic_v3_sim_review.get("proposal_decisions"),
                    ),
                    (
                        "confirmation_targets",
                        etf_dynamic_v3_sim_review.get("confirmation_targets"),
                    ),
                    (
                        "calibration_ready_conditions",
                        etf_dynamic_v3_sim_review.get("calibration_ready_conditions"),
                    ),
                    (
                        "failure_conditions",
                        etf_dynamic_v3_sim_review.get("failure_conditions"),
                    ),
                    (
                        "confirmation_registry_id",
                        etf_dynamic_v3_sim_review.get("confirmation_registry_id"),
                    ),
                    (
                        "confirmation_progress_id",
                        etf_dynamic_v3_sim_review.get("confirmation_progress_id"),
                    ),
                    (
                        "confirmation_evaluation_id",
                        etf_dynamic_v3_sim_review.get("confirmation_evaluation_id"),
                    ),
                    (
                        "rule_review_cycle_id",
                        etf_dynamic_v3_sim_review.get("rule_review_cycle_id"),
                    ),
                    (
                        "rule_review_cycle_recommendation",
                        etf_dynamic_v3_sim_review.get("rule_review_cycle_recommendation"),
                    ),
                    (
                        "weekly_cycle_id",
                        etf_dynamic_v3_sim_review.get("confirmation_weekly_cycle_id"),
                    ),
                    (
                        "weekly_due_updated_windows",
                        (
                            f"{etf_dynamic_v3_sim_review.get('confirmation_weekly_due_windows')}"
                            " / "
                            f"{etf_dynamic_v3_sim_review.get('confirmation_weekly_updated_windows')}"
                        ),
                    ),
                    (
                        "weekly_rule_review_recommendation",
                        etf_dynamic_v3_sim_review.get(
                            "confirmation_weekly_rule_review_recommendation"
                        ),
                    ),
                    (
                        "pressure_regime_tag_id",
                        etf_dynamic_v3_sim_review.get("pressure_regime_tag_id"),
                    ),
                    (
                        "pressure_counts",
                        (
                            "tech_drawdown="
                            f"{etf_dynamic_v3_sim_review.get('pressure_tech_drawdown_count')}; "
                            "risk_off="
                            f"{etf_dynamic_v3_sim_review.get('pressure_risk_off_count')}; "
                            "semiconductor_pullback="
                            f"{etf_dynamic_v3_sim_review.get('pressure_semiconductor_pullback_count')}"
                        ),
                    ),
                    (
                        "defensive_validation_relevant_outcomes",
                        etf_dynamic_v3_sim_review.get(
                            "pressure_defensive_validation_relevant_outcomes"
                        ),
                    ),
                    (
                        "pressure_diagnosis",
                        (
                            f"id={etf_dynamic_v3_sim_review.get('pressure_diagnosis_id')}; "
                            f"reason={etf_dynamic_v3_sim_review.get('pressure_diagnosis_reason')}; "
                            "near_miss="
                            f"{etf_dynamic_v3_sim_review.get('pressure_near_miss_window_count')}"
                        ),
                    ),
                    (
                        "pressure_backfill",
                        (
                            f"id={etf_dynamic_v3_sim_review.get('pressure_backfill_id')}; "
                            "total="
                            f"{etf_dynamic_v3_sim_review.get('pressure_backfill_total')}; "
                            "forward="
                            f"{etf_dynamic_v3_sim_review.get('pressure_backfill_forward_count')}; "
                            "replay="
                            f"{etf_dynamic_v3_sim_review.get('pressure_backfill_replay_count')}; "
                            "simulation="
                            + "{}; ".format(
                                etf_dynamic_v3_sim_review.get("pressure_backfill_simulation_count")
                            )
                            + (
                                "relevant="
                                f"{etf_dynamic_v3_sim_review.get('pressure_backfill_relevant_count')}"
                            )
                        ),
                    ),
                    (
                        "defensive_pressure_compare",
                        (
                            "id={}; ".format(
                                etf_dynamic_v3_sim_review.get("defensive_pressure_comparison_id")
                            )
                            + "status={}; ".format(
                                etf_dynamic_v3_sim_review.get("defensive_pressure_status")
                            )
                            + (
                                "rule_approval="
                                f"{etf_dynamic_v3_sim_review.get('defensive_pressure_can_support_rule_approval')}"
                            )
                        ),
                    ),
                    (
                        "defensive_rule_review",
                        (
                            f"id={etf_dynamic_v3_sim_review.get('defensive_rule_review_id')}; "
                            "recommended={}; ".format(
                                etf_dynamic_v3_sim_review.get("defensive_rule_recommended_status")
                            )
                            + (
                                "approval="
                                f"{etf_dynamic_v3_sim_review.get('defensive_rule_approval_allowed')}"
                            )
                        ),
                    ),
                    (
                        "weekly_ops_decision_update",
                        (
                            f"id={etf_dynamic_v3_sim_review.get('weekly_ops_decision_update_id')}; "
                            "recommendation={}; ".format(
                                etf_dynamic_v3_sim_review.get("weekly_ops_recommendation")
                            )
                            + (
                                "policy_change_allowed={}; ".format(
                                    etf_dynamic_v3_sim_review.get(
                                        "weekly_ops_policy_change_allowed"
                                    )
                                )
                                + (
                                    "broker_action_allowed="
                                    f"{etf_dynamic_v3_sim_review.get('weekly_ops_broker_action_allowed')}"
                                )
                            )
                        ),
                    ),
                    (
                        "defensive_hypothesis_deep_dive",
                        (
                            "id={}; supporting={}; contradicting={}; rule_approval={}".format(
                                etf_dynamic_v3_sim_review.get("defensive_hypothesis_deep_dive_id"),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_hypothesis_supporting_count"
                                ),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_hypothesis_contradicting_count"
                                ),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_hypothesis_can_support_rule_approval"
                                ),
                            )
                        ),
                    ),
                    (
                        "defensive_label_review",
                        (
                            "id={}; status={}; recommended={}; auto_rename={}".format(
                                etf_dynamic_v3_sim_review.get("defensive_label_review_id"),
                                etf_dynamic_v3_sim_review.get("defensive_label_status"),
                                etf_dynamic_v3_sim_review.get("defensive_recommended_label"),
                                etf_dynamic_v3_sim_review.get("defensive_label_auto_rename"),
                            )
                        ),
                    ),
                    (
                        "defensive_failure_study",
                        (
                            "id={}; cases={}; top_pattern={}".format(
                                etf_dynamic_v3_sim_review.get("defensive_failure_study_id"),
                                etf_dynamic_v3_sim_review.get("defensive_failure_case_count"),
                                etf_dynamic_v3_sim_review.get("defensive_failure_top_pattern"),
                            )
                        ),
                    ),
                    (
                        "defensive_research_note",
                        (
                            "id={}; status={}; forward_support={}".format(
                                etf_dynamic_v3_sim_review.get("defensive_research_note_id"),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_hypothesis_current_status"
                                ),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_hypothesis_forward_support"
                                ),
                            )
                        ),
                    ),
                    (
                        "defensive_owner_pack",
                        (
                            "id={}; continue_tracking_recommended={}".format(
                                etf_dynamic_v3_sim_review.get("defensive_owner_pack_id"),
                                etf_dynamic_v3_sim_review.get(
                                    "defensive_owner_continue_tracking_recommended"
                                ),
                            )
                        ),
                    ),
                    (
                        "forward_pressure_capture",
                        (
                            "id={}; event_triggers={}".format(
                                etf_dynamic_v3_sim_review.get("forward_pressure_capture_plan_id"),
                                etf_dynamic_v3_sim_review.get(
                                    "forward_pressure_event_trigger_count"
                                ),
                            )
                        ),
                    ),
                    (
                        "pressure_trigger_capture",
                        (
                            "trigger_id={}; trigger_status={}; capture_required={}; "
                            "capture_id={}; capture_status={}; manual_force={}".format(
                                etf_dynamic_v3_sim_review.get("pressure_trigger_id"),
                                etf_dynamic_v3_sim_review.get("pressure_trigger_status"),
                                etf_dynamic_v3_sim_review.get("pressure_capture_required"),
                                etf_dynamic_v3_sim_review.get("pressure_capture_id"),
                                etf_dynamic_v3_sim_review.get("pressure_capture_status"),
                                etf_dynamic_v3_sim_review.get("pressure_capture_manual_force"),
                            )
                        ),
                    ),
                    (
                        "pressure_sample_ledger",
                        (
                            "id={}; forward={}; simulation={}; progress={}".format(
                                etf_dynamic_v3_sim_review.get("pressure_sample_ledger_id"),
                                etf_dynamic_v3_sim_review.get("pressure_forward_samples"),
                                etf_dynamic_v3_sim_review.get("pressure_simulation_samples"),
                                etf_dynamic_v3_sim_review.get("pressure_progress_to_requirement"),
                            )
                        ),
                    ),
                    (
                        "weekly_defensive_evidence",
                        (
                            "id={}; rule_status={}; recommendation={}; new_simulation={}".format(
                                etf_dynamic_v3_sim_review.get("weekly_defensive_id"),
                                etf_dynamic_v3_sim_review.get("weekly_defensive_rule_status"),
                                etf_dynamic_v3_sim_review.get("weekly_defensive_recommendation"),
                                etf_dynamic_v3_sim_review.get(
                                    "weekly_defensive_new_simulation_samples"
                                ),
                            )
                        ),
                    ),
                    (
                        "confirmation_dashboard_id",
                        etf_dynamic_v3_sim_review.get("confirmation_dashboard_id"),
                    ),
                    (
                        "dashboard_ready_for_evaluation",
                        etf_dynamic_v3_sim_review.get("dashboard_ready_for_evaluation"),
                    ),
                    (
                        "limited_adjustment_progress",
                        etf_dynamic_v3_sim_review.get("dashboard_limited_adjustment_progress"),
                    ),
                    (
                        "defensive_pressure_progress",
                        etf_dynamic_v3_sim_review.get("dashboard_defensive_pressure_progress"),
                    ),
                    (
                        "consensus_target_status",
                        etf_dynamic_v3_sim_review.get("dashboard_consensus_target_status"),
                    ),
                    (
                        "rule_review_queue_id",
                        etf_dynamic_v3_sim_review.get("rule_review_queue_id"),
                    ),
                    (
                        "rule_review_queue_counts",
                        (
                            "pending="
                            f"{etf_dynamic_v3_sim_review.get('rule_review_queue_pending_count')}; "
                            "ready="
                            f"{etf_dynamic_v3_sim_review.get('rule_review_queue_ready_count')}; "
                            "not_ready="
                            f"{etf_dynamic_v3_sim_review.get('rule_review_queue_not_ready_count')}"
                        ),
                    ),
                    (
                        "ready_for_evaluation_count",
                        etf_dynamic_v3_sim_review.get("confirmation_ready_for_evaluation_count"),
                    ),
                    (
                        "insufficient_events_count",
                        etf_dynamic_v3_sim_review.get("confirmation_insufficient_events_count"),
                    ),
                    (
                        "evaluation_counts",
                        (
                            "success="
                            f"{etf_dynamic_v3_sim_review.get('confirmation_success_count')}; "
                            "failure="
                            f"{etf_dynamic_v3_sim_review.get('confirmation_failure_count')}; "
                            "not_ready="
                            f"{etf_dynamic_v3_sim_review.get('confirmation_not_ready_count')}"
                        ),
                    ),
                    (
                        "rule_review_policy_change_allowed",
                        etf_dynamic_v3_sim_review.get("rule_review_policy_change_allowed"),
                    ),
                    (
                        "rule_owner_decision_id",
                        etf_dynamic_v3_sim_review.get("rule_owner_decision_id"),
                    ),
                    (
                        "rule_owner_decision",
                        etf_dynamic_v3_sim_review.get("rule_owner_decision"),
                    ),
                    ("auto_apply", etf_dynamic_v3_sim_review.get("auto_apply")),
                    (
                        "owner_approval_required",
                        etf_dynamic_v3_sim_review.get("owner_approval_required"),
                    ),
                    (
                        "position_advisory_config_mutated",
                        etf_dynamic_v3_sim_review.get("position_advisory_config_mutated"),
                    ),
                    ("report_label", etf_dynamic_v3_sim_review.get("report_label")),
                    ("outcome_mode", etf_dynamic_v3_sim_review.get("outcome_mode")),
                    (
                        "pit_safety_status",
                        etf_dynamic_v3_sim_review.get("pit_safety_status"),
                    ),
                    ("safety_status", etf_dynamic_v3_sim_review.get("safety_status")),
                    (
                        "production_effect",
                        etf_dynamic_v3_sim_review.get("production_effect"),
                    ),
                    ("broker_action", etf_dynamic_v3_sim_review.get("broker_action")),
                    (
                        "sim_interpretation_path",
                        etf_dynamic_v3_sim_review.get("sim_interpretation_path"),
                    ),
                    (
                        "sim_risk_return_path",
                        etf_dynamic_v3_sim_review.get("sim_risk_return_path"),
                    ),
                    (
                        "sim_defensive_validation_path",
                        etf_dynamic_v3_sim_review.get("sim_defensive_validation_path"),
                    ),
                    (
                        "advisory_proposal_review_path",
                        etf_dynamic_v3_sim_review.get("advisory_proposal_review_path"),
                    ),
                    (
                        "forward_confirmation_plan_path",
                        etf_dynamic_v3_sim_review.get("forward_confirmation_plan_path"),
                    ),
                    (
                        "confirmation_registry_path",
                        etf_dynamic_v3_sim_review.get("confirmation_registry_path"),
                    ),
                    (
                        "confirmation_progress_path",
                        etf_dynamic_v3_sim_review.get("confirmation_progress_path"),
                    ),
                    (
                        "confirmation_evaluation_path",
                        etf_dynamic_v3_sim_review.get("confirmation_evaluation_path"),
                    ),
                    (
                        "rule_review_cycle_path",
                        etf_dynamic_v3_sim_review.get("rule_review_cycle_path"),
                    ),
                    (
                        "confirmation_weekly_path",
                        etf_dynamic_v3_sim_review.get("confirmation_weekly_path"),
                    ),
                    (
                        "pressure_regime_tag_path",
                        etf_dynamic_v3_sim_review.get("pressure_regime_tag_path"),
                    ),
                    (
                        "confirmation_dashboard_path",
                        etf_dynamic_v3_sim_review.get("confirmation_dashboard_path"),
                    ),
                    (
                        "rule_review_queue_path",
                        etf_dynamic_v3_sim_review.get("rule_review_queue_path"),
                    ),
                    (
                        "pressure_tag_diagnosis_path",
                        etf_dynamic_v3_sim_review.get("pressure_tag_diagnosis_path"),
                    ),
                    (
                        "pressure_outcome_backfill_path",
                        etf_dynamic_v3_sim_review.get("pressure_outcome_backfill_path"),
                    ),
                    (
                        "defensive_pressure_compare_path",
                        etf_dynamic_v3_sim_review.get("defensive_pressure_compare_path"),
                    ),
                    (
                        "defensive_rule_review_path",
                        etf_dynamic_v3_sim_review.get("defensive_rule_review_path"),
                    ),
                    (
                        "weekly_ops_decision_update_path",
                        etf_dynamic_v3_sim_review.get("weekly_ops_decision_update_path"),
                    ),
                    (
                        "defensive_hypothesis_deep_dive_path",
                        etf_dynamic_v3_sim_review.get("defensive_hypothesis_deep_dive_path"),
                    ),
                    (
                        "defensive_label_review_path",
                        etf_dynamic_v3_sim_review.get("defensive_label_review_path"),
                    ),
                    (
                        "defensive_failure_study_path",
                        etf_dynamic_v3_sim_review.get("defensive_failure_study_path"),
                    ),
                    (
                        "defensive_research_note_path",
                        etf_dynamic_v3_sim_review.get("defensive_research_note_path"),
                    ),
                    (
                        "defensive_owner_pack_path",
                        etf_dynamic_v3_sim_review.get("defensive_owner_pack_path"),
                    ),
                    (
                        "forward_pressure_capture_path",
                        etf_dynamic_v3_sim_review.get("forward_pressure_capture_path"),
                    ),
                    (
                        "pressure_trigger_path",
                        etf_dynamic_v3_sim_review.get("pressure_trigger_path"),
                    ),
                    (
                        "pressure_capture_path",
                        etf_dynamic_v3_sim_review.get("pressure_capture_path"),
                    ),
                    (
                        "pressure_sample_ledger_path",
                        etf_dynamic_v3_sim_review.get("pressure_sample_ledger_path"),
                    ),
                    (
                        "weekly_defensive_evidence_path",
                        etf_dynamic_v3_sim_review.get("weekly_defensive_evidence_path"),
                    ),
                    (
                        "rule_owner_decision_path",
                        etf_dynamic_v3_sim_review.get("rule_owner_decision_path"),
                    ),
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
        _section("Backtest / Shadow / Governance", _definition_table(list(governance.items()))),
        _section(
            "ETF Backtest Summary",
            _definition_table(
                [
                    ("availability", etf_backtest.get("availability")),
                    ("status", etf_backtest.get("status")),
                    ("summary", etf_backtest.get("summary_sentence")),
                    ("run_id", etf_backtest.get("run_id")),
                    ("start_date", etf_backtest.get("start_date")),
                    ("end_date", etf_backtest.get("end_date")),
                    ("trading_days", etf_backtest.get("trading_days")),
                    ("primary_benchmark_id", etf_backtest.get("primary_benchmark_id")),
                    ("total_return", etf_backtest.get("total_return")),
                    ("CAGR", etf_backtest.get("CAGR")),
                    ("max_drawdown", etf_backtest.get("max_drawdown")),
                    ("Sharpe", etf_backtest.get("Sharpe")),
                    ("benchmark_excess_return", etf_backtest.get("benchmark_excess_return")),
                    (
                        "benchmark_drawdown_reduction",
                        etf_backtest.get("benchmark_drawdown_reduction"),
                    ),
                    ("monthly_rows", etf_backtest.get("monthly_rows")),
                    ("null_reason_count", etf_backtest.get("null_reason_count")),
                    ("data_quality_status", etf_backtest.get("data_quality_status")),
                    ("production_effect", etf_backtest.get("production_effect")),
                    ("source_artifact", etf_backtest.get("source_artifact")),
                ]
            ),
        ),
        _section(
            "Weekly Portfolio Review",
            _definition_table(
                [
                    ("availability", etf_weekly_review.get("availability")),
                    ("status", etf_weekly_review.get("status")),
                    ("summary", etf_weekly_review.get("summary_sentence")),
                    (
                        "active_shadow_candidates",
                        etf_weekly_review.get("active_shadow_candidates"),
                    ),
                    (
                        "candidates_requiring_review",
                        etf_weekly_review.get("candidates_requiring_review"),
                    ),
                    ("AI confirmation", etf_weekly_review.get("ai_confirmation")),
                    ("satellite replacement", etf_weekly_review.get("satellite_replacement")),
                    ("critical_warnings", etf_weekly_review.get("critical_warnings")),
                    (
                        "manual_review_actions",
                        etf_weekly_review.get("manual_review_actions"),
                    ),
                    ("safety_status", etf_weekly_review.get("safety_status")),
                    ("detailed_weekly_review", etf_weekly_review.get("detail_report")),
                    ("production_effect", etf_weekly_review.get("production_effect")),
                ]
            ),
        ),
        _section(
            "Portfolio Decision Journal",
            _definition_table(
                [
                    ("availability", etf_decision_journal.get("availability")),
                    ("status", etf_decision_journal.get("status")),
                    ("summary", etf_decision_journal.get("summary_sentence")),
                    ("entry_count", etf_decision_journal.get("entry_count")),
                    ("removed_entry_count", etf_decision_journal.get("removed_entry_count")),
                    ("follow_up_task_count", etf_decision_journal.get("follow_up_task_count")),
                    ("decision_status_counts", etf_decision_journal.get("decision_status_counts")),
                    ("average_confidence", etf_decision_journal.get("average_confidence")),
                    ("latest_journal_report", etf_decision_journal.get("detail_report")),
                    ("safety_status", etf_decision_journal.get("safety_status")),
                    ("production_effect", etf_decision_journal.get("production_effect")),
                ]
            ),
        ),
        _section(
            "ETF Parameter Review",
            _definition_table(
                [
                    ("availability", etf_parameter_review.get("availability")),
                    ("status", etf_parameter_review.get("status")),
                    ("summary", etf_parameter_review.get("summary_sentence")),
                    ("candidates_reviewed", etf_parameter_review.get("candidate_count")),
                    (
                        "eligible_for_manual_review",
                        etf_parameter_review.get("eligible_for_manual_review_count"),
                    ),
                    ("continue_shadow", etf_parameter_review.get("continue_shadow_count")),
                    ("rejected_proposals", etf_parameter_review.get("rejected_count")),
                    ("needs_more_data", etf_parameter_review.get("needs_more_data_count")),
                    ("blocked_proposals", etf_parameter_review.get("blocked_count")),
                    ("main_reason", etf_parameter_review.get("main_reason")),
                    ("safety_status", etf_parameter_review.get("safety_status")),
                    ("detailed_report", etf_parameter_review.get("detail_report")),
                    ("production_effect", etf_parameter_review.get("production_effect")),
                    ("broker_action", etf_parameter_review.get("broker_action")),
                ]
            ),
        ),
        _section(
            "ETF Weight Calibration",
            _definition_table(
                [
                    ("availability", etf_weight_calibration.get("availability")),
                    ("status", etf_weight_calibration.get("status")),
                    ("summary", etf_weight_calibration.get("summary_sentence")),
                    ("search_pack", etf_weight_calibration.get("search_pack")),
                    (
                        "top_historical_candidate",
                        etf_weight_calibration.get("top_historical_candidate"),
                    ),
                    (
                        "forward_evidence_status",
                        etf_weight_calibration.get("forward_evidence_status"),
                    ),
                    ("overfit_risk", etf_weight_calibration.get("overfit_risk")),
                    ("candidate_status", etf_weight_calibration.get("candidate_status")),
                    (
                        "manual_review_proposals",
                        etf_weight_calibration.get("manual_review_proposals"),
                    ),
                    ("safety_status", etf_weight_calibration.get("safety_status")),
                    ("detailed_report", etf_weight_calibration.get("detail_report")),
                    ("production_effect", etf_weight_calibration.get("production_effect")),
                    ("broker_action", etf_weight_calibration.get("broker_action")),
                ]
            ),
        ),
        _section(
            "ETF Initial Weight Candidates",
            _definition_table(
                [
                    ("availability", etf_initial_weight_candidates.get("availability")),
                    ("status", etf_initial_weight_candidates.get("status")),
                    ("summary", etf_initial_weight_candidates.get("summary_sentence")),
                    (
                        "latest_search_preset",
                        etf_initial_weight_candidates.get("latest_search_preset"),
                    ),
                    ("top_candidate", etf_initial_weight_candidates.get("top_candidate")),
                    ("suggested_action", etf_initial_weight_candidates.get("suggested_action")),
                    ("overfit_risk", etf_initial_weight_candidates.get("overfit_risk")),
                    ("best_robustness", etf_initial_weight_candidates.get("best_robustness")),
                    (
                        "blocked_candidates",
                        etf_initial_weight_candidates.get("blocked_candidate_count"),
                    ),
                    ("safety_status", etf_initial_weight_candidates.get("safety_status")),
                    ("detailed_report", etf_initial_weight_candidates.get("detail_report")),
                    ("production_effect", etf_initial_weight_candidates.get("production_effect")),
                    ("broker_action", etf_initial_weight_candidates.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Weight Calibration Profiling",
            _definition_table(
                [
                    ("availability", etf_weight_calibration_profiling.get("availability")),
                    ("status", etf_weight_calibration_profiling.get("status")),
                    ("profile_mode", etf_weight_calibration_profiling.get("profile_mode")),
                    (
                        "total_runtime_seconds",
                        etf_weight_calibration_profiling.get("total_runtime_seconds"),
                    ),
                    ("slowest_step", etf_weight_calibration_profiling.get("slowest_step")),
                    (
                        "cache_hit_rate",
                        etf_weight_calibration_profiling.get("cache_hit_rate"),
                    ),
                    (
                        "recommendation",
                        etf_weight_calibration_profiling.get("next_step_recommendation"),
                    ),
                    ("safety_status", etf_weight_calibration_profiling.get("safety_status")),
                    ("detailed_report", etf_weight_calibration_profiling.get("detail_report")),
                    (
                        "production_effect",
                        etf_weight_calibration_profiling.get("production_effect"),
                    ),
                    ("broker_action", etf_weight_calibration_profiling.get("broker_action")),
                ]
            ),
        ),
        _section(
            "ETF Calibration Experiments",
            _definition_table(
                [
                    ("availability", etf_calibration.get("availability")),
                    ("status", etf_calibration.get("status")),
                    ("latest_experiment_pack", etf_calibration.get("latest_experiment_pack")),
                    ("top_candidate", etf_calibration.get("top_candidate")),
                    ("rejected_count", etf_calibration.get("rejected_count")),
                    ("active_shadow_candidates", etf_calibration.get("active_shadow_candidates")),
                    ("weekly_review_action", etf_calibration.get("weekly_review_action")),
                    ("safety_status", etf_calibration.get("safety_status")),
                    ("detail_report", etf_calibration.get("detail_report")),
                    ("production_effect", etf_calibration.get("production_effect")),
                ]
            ),
        ),
        _section(
            "ETF Forward Simulation",
            _definition_table(
                [
                    ("availability", etf_forward.get("availability")),
                    ("status", etf_forward.get("status")),
                    ("summary", etf_forward.get("summary_sentence")),
                    ("active_shadow_candidates", etf_forward.get("active_shadow_candidates")),
                    ("best_candidate_since_enrollment", etf_forward.get("best_candidate")),
                    ("weakest_candidate_since_enrollment", etf_forward.get("weakest_candidate")),
                    ("needs_more_data_count", etf_forward.get("needs_more_data_count")),
                    ("watch_count", etf_forward.get("watch_count")),
                    ("reject_pending_review_count", etf_forward.get("reject_pending_review_count")),
                    ("watchlist_attention_count", etf_forward.get("watchlist_attention_count")),
                    ("safety_status", etf_forward.get("safety_status")),
                    ("decision_input_usage", etf_forward.get("decision_input_usage")),
                    ("detail_report", etf_forward.get("detail_report")),
                    ("production_effect", etf_forward.get("production_effect")),
                ]
            ),
        ),
        _section(
            "AI Confirmation",
            _definition_table(
                [
                    ("availability", etf_ai_confirmation.get("availability")),
                    ("status", etf_ai_confirmation.get("status")),
                    ("AIConfirmationScore", etf_ai_confirmation.get("AIConfirmationScore")),
                    ("score_band", etf_ai_confirmation.get("score_band")),
                    (
                        "semiconductor_breadth",
                        etf_ai_confirmation.get("semiconductor_breadth"),
                    ),
                    ("mega_cap_ai_score", etf_ai_confirmation.get("mega_cap_ai_score")),
                    (
                        "ai_relative_strength",
                        etf_ai_confirmation.get("ai_relative_strength"),
                    ),
                    ("event_risk", etf_ai_confirmation.get("event_risk")),
                    ("interpretation", etf_ai_confirmation.get("interpretation")),
                    ("safety_status", etf_ai_confirmation.get("safety_status")),
                    ("detail_report", etf_ai_confirmation.get("detail_report")),
                    ("production_effect", etf_ai_confirmation.get("production_effect")),
                    ("broker_action", etf_ai_confirmation.get("broker_action")),
                ]
            ),
        ),
        _section(
            "AI Attribution Review",
            _definition_table(
                [
                    ("availability", etf_ai_attribution.get("availability")),
                    ("status", etf_ai_attribution.get("status")),
                    ("overall_status", etf_ai_attribution.get("overall_status")),
                    ("best_evidence", etf_ai_attribution.get("best_evidence")),
                    ("weak_evidence", etf_ai_attribution.get("weak_evidence")),
                    ("redundancy_status", etf_ai_attribution.get("redundancy_status")),
                    ("manual_review", etf_ai_attribution.get("manual_review")),
                    ("safety_status", etf_ai_attribution.get("safety_status")),
                    ("detailed_report", etf_ai_attribution.get("detail_report")),
                    ("production_effect", etf_ai_attribution.get("production_effect")),
                    ("broker_action", etf_ai_attribution.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Satellite Replacement",
            _definition_table(
                [
                    ("availability", etf_satellite.get("availability")),
                    ("status", etf_satellite.get("status")),
                    ("summary", etf_satellite.get("summary_sentence")),
                    ("eligible_stocks", etf_satellite.get("eligible_stocks")),
                    ("watchlist", etf_satellite.get("watchlist")),
                    ("fallback_to_etf", etf_satellite.get("fallback_to_etf")),
                    (
                        "proposed_candidate_replacement",
                        etf_satellite.get("proposed_candidate_replacement"),
                    ),
                    ("main_reason", etf_satellite.get("main_reason")),
                    ("main_blocker", etf_satellite.get("main_blocker")),
                    ("safety_status", etf_satellite.get("safety_status")),
                    ("detail_report", etf_satellite.get("detail_report")),
                    ("production_effect", etf_satellite.get("production_effect")),
                    ("broker_action", etf_satellite.get("broker_action")),
                ]
            ),
        ),
        _section(
            "Satellite Attribution Review",
            _definition_table(
                [
                    ("availability", etf_satellite_attribution.get("availability")),
                    ("status", etf_satellite_attribution.get("status")),
                    ("overall_status", etf_satellite_attribution.get("overall_status")),
                    ("eligible_evidence", etf_satellite_attribution.get("eligible_evidence")),
                    ("fallback_evidence", etf_satellite_attribution.get("fallback_evidence")),
                    ("role_evidence", etf_satellite_attribution.get("role_evidence")),
                    ("risk_note", etf_satellite_attribution.get("risk_note")),
                    ("weak_evidence", etf_satellite_attribution.get("weak_evidence")),
                    ("manual_review", etf_satellite_attribution.get("manual_review")),
                    ("safety_status", etf_satellite_attribution.get("safety_status")),
                    ("detailed_report", etf_satellite_attribution.get("detail_report")),
                    ("production_effect", etf_satellite_attribution.get("production_effect")),
                    ("broker_action", etf_satellite_attribution.get("broker_action")),
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


def _data_quality_pit_safety(
    *,
    as_of: date,
    snapshot: Mapping[str, Any],
    daily_decision_summary: Mapping[str, Any],
    report_index_summary: Mapping[str, Any],
) -> dict[str, Any]:
    quality = _mapping(snapshot.get("quality"))
    data_gate = _mapping(daily_decision_summary.get("data_gate"))
    data_gate_status = _text(data_gate.get("status"), _quality_status(snapshot))
    signal_date = _text(snapshot.get("signal_date"), as_of.isoformat())
    future_data_status = (
        "PASS"
        if _leading_status(data_gate_status).upper() in {"PASS", "PASS_WITH_WARNINGS"}
        else "REVIEW_REQUIRED"
    )
    return {
        "as_of_date": signal_date,
        "decision_snapshot_id": _text(snapshot.get("snapshot_id"), "UNKNOWN"),
        "data_gate_status": data_gate_status,
        "market_data_status": _text(quality.get("market_data_status"), "UNKNOWN"),
        "market_data_latest_date": _text(
            quality.get("market_data_latest_date"),
            _text(quality.get("latest_market_data_date"), "UNKNOWN_IN_SNAPSHOT"),
        ),
        "market_data_error_count": _text(quality.get("market_data_error_count"), "UNKNOWN"),
        "market_data_warning_count": _text(quality.get("market_data_warning_count"), "UNKNOWN"),
        "feature_status": _text(quality.get("feature_status"), "UNKNOWN"),
        "sec_feature_status": _text(quality.get("sec_feature_status"), "UNKNOWN"),
        "sec_data_latest_filing": _text(
            quality.get("sec_data_latest_filing"),
            _text(quality.get("latest_sec_filing"), "UNKNOWN_IN_SNAPSHOT"),
        ),
        "fmp_valuation_snapshot_timestamp": _text(
            quality.get("fmp_valuation_snapshot_timestamp"),
            _text(quality.get("latest_fmp_valuation_timestamp"), "UNKNOWN_IN_SNAPSHOT"),
        ),
        "future_data_check": future_data_status,
        "carried_forward_fields": _texts(quality.get("carried_forward_fields")),
        "stale_fields": _texts(quality.get("stale_fields")),
        "blocking_reasons": _texts(data_gate.get("blocking_reasons")),
        "stale_report_count": report_index_summary.get("stale_count"),
        "missing_report_count": report_index_summary.get("missing_count"),
        "pit_visibility_note": (
            "UNKNOWN_IN_SNAPSHOT 表示该源的可见时间未在当前 decision snapshot 明确披露；"
            "不得据此补造 PIT 结论。"
        ),
        "production_effect": PRODUCTION_EFFECT,
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


def _etf_backtest_review_summary(as_of: date) -> dict[str, Any]:
    path = _latest_etf_backtest_summary_path(as_of)
    if path is None:
        return {
            "availability": "MISSING",
            "status": "MISSING",
            "summary_sentence": (
                "ETF backtest summary is missing; Reader Brief does not run ETF backtest."
            ),
            "run_id": "",
            "start_date": "",
            "end_date": "",
            "trading_days": 0,
            "primary_benchmark_id": "",
            "total_return": "UNKNOWN",
            "CAGR": "UNKNOWN",
            "max_drawdown": "UNKNOWN",
            "Sharpe": "UNKNOWN",
            "benchmark_excess_return": "UNKNOWN",
            "benchmark_drawdown_reduction": "UNKNOWN",
            "monthly_rows": 0,
            "null_reason_count": 0,
            "data_quality_status": "MISSING",
            "production_effect": PRODUCTION_EFFECT,
            "source_artifact": "",
        }
    payload = _read_optional_json(path)
    metrics = _mapping(payload.get("standardized_metrics"))
    monthly_returns = _records(payload.get("monthly_returns"))
    null_reasons = _mapping(metrics.get("metric_null_reasons"))
    status = "AVAILABLE" if metrics else "LIMITED"
    total_return = _format_number(metrics.get("total_return"), digits=4)
    benchmark_excess = _format_number(metrics.get("benchmark_excess_return"), digits=4)
    summary_sentence = (
        f"ETF backtest standardized metrics are {status}: total_return={total_return}, "
        f"benchmark_excess_return={benchmark_excess}, monthly_rows={len(monthly_returns)}; "
        "production_effect=none."
    )
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": summary_sentence,
        "run_id": path.parent.name,
        "start_date": _text(metrics.get("start_date"), _text(payload.get("first_signal_date"))),
        "end_date": _text(metrics.get("end_date"), _text(payload.get("last_signal_date"))),
        "trading_days": metrics.get("trading_days", payload.get("row_count", 0)),
        "primary_benchmark_id": _text(metrics.get("primary_benchmark_id")),
        "total_return": total_return,
        "CAGR": _format_number(metrics.get("CAGR"), digits=4),
        "max_drawdown": _format_number(metrics.get("max_drawdown"), digits=4),
        "Sharpe": _format_number(metrics.get("Sharpe"), digits=4),
        "benchmark_excess_return": benchmark_excess,
        "benchmark_drawdown_reduction": _format_number(
            metrics.get("benchmark_drawdown_reduction"),
            digits=4,
        ),
        "monthly_rows": len(monthly_returns),
        "null_reason_count": len(null_reasons),
        "data_quality_status": _text(payload.get("data_quality_status"), "UNKNOWN"),
        "production_effect": PRODUCTION_EFFECT,
        "source_artifact": str(path),
    }


def _etf_weekly_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_weekly_review_summary()
    report_path = _report_index_artifact_path(report_index, "etf_weekly_review")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_weekly_review_summary()
    sections = _mapping(report.get("sections"))
    shadow = _mapping(sections.get("shadow_candidate_review"))
    ai = _mapping(sections.get("ai_confirmation_review"))
    satellite = _mapping(sections.get("satellite_replacement_review"))
    risk = _mapping(sections.get("risk_watchlist_constraints"))
    shadow_summary = _mapping(shadow.get("summary"))
    severity = _mapping(risk.get("severity_counts"))
    status = _text(report.get("status"), "AVAILABLE")
    manual_count = len(_records(report.get("manual_review_actions")))
    active_count = int(shadow_summary.get("active_candidate_count") or 0)
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"Weekly Portfolio Review: status={status}; "
            f"active_shadow_candidates={active_count}; "
            f"manual_review_actions={manual_count}."
        ),
        "active_shadow_candidates": active_count,
        "candidates_requiring_review": int(
            shadow_summary.get("candidate_requiring_review_count") or 0
        ),
        "ai_confirmation": _text(ai.get("section_status"), "MISSING"),
        "satellite_replacement": _text(satellite.get("section_status"), "MISSING"),
        "critical_warnings": int(severity.get("critical") or 0),
        "manual_review_actions": manual_count,
        "safety_status": _etf_weekly_review_safety_status(report),
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
    }


def _missing_etf_weekly_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Weekly Portfolio Review: no latest weekly review report found.",
        "active_shadow_candidates": 0,
        "candidates_requiring_review": 0,
        "ai_confirmation": "MISSING",
        "satellite_replacement": "MISSING",
        "critical_warnings": 0,
        "manual_review_actions": 0,
        "safety_status": "MISSING",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "limitation": (
            "ETF weekly review artifact is missing; Reader Brief does not run weekly review."
        ),
    }


def _etf_weekly_review_safety_status(payload: Mapping[str, Any]) -> str:
    safe = (
        payload.get("observe_only") is True
        and payload.get("candidate_only") is True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") == "none"
        and payload.get("manual_review_required") is True
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_decision_journal_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_decision_journal_summary()
    report_path = _report_index_artifact_path(report_index, "etf_decision_journal_report")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_decision_journal_summary()
    metadata = _mapping(report.get("review_metadata"))
    summary = _mapping(report.get("human_decision_summary"))
    status_counts = _mapping(summary.get("decision_status_counts"))
    entry_count = int(metadata.get("entry_count") or summary.get("entry_count") or 0)
    follow_up_count = int(summary.get("follow_up_task_count") or 0)
    safety_status = _etf_decision_journal_safety_status(report)
    return {
        "availability": "AVAILABLE",
        "status": "AVAILABLE" if entry_count else "NO_ACTIVE_DECISIONS",
        "summary_sentence": (
            f"Portfolio Decision Journal: active_entries={entry_count}; "
            f"follow_up_tasks={follow_up_count}; safety={safety_status}."
        ),
        "entry_count": entry_count,
        "removed_entry_count": int(metadata.get("removed_entry_count") or 0),
        "follow_up_task_count": follow_up_count,
        "decision_status_counts": dict(status_counts),
        "average_confidence": summary.get("average_confidence"),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
    }


def _missing_etf_decision_journal_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Portfolio Decision Journal: no latest journal report found.",
        "entry_count": 0,
        "removed_entry_count": 0,
        "follow_up_task_count": 0,
        "decision_status_counts": {},
        "average_confidence": None,
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "limitation": (
            "ETF decision journal artifact is missing; Reader Brief does not run journal CLI."
        ),
    }


def _etf_decision_journal_safety_status(payload: Mapping[str, Any]) -> str:
    safe = (
        payload.get("observe_only") is True
        and payload.get("candidate_only") is True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") == "none"
        and payload.get("manual_review_required") is True
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_parameter_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_parameter_review_summary()
    report_path = _report_index_artifact_path(report_index, "etf_parameter_review_report")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_parameter_review_summary()
    summary = _mapping(report.get("summary"))
    scorecard = _mapping(report.get("proposal_scorecard"))
    status_counts = _mapping(scorecard.get("status_counts"))
    status = _text(report.get("status"), "UNKNOWN")
    candidate_count = _int(summary.get("candidate_count"))
    eligible_count = _int(summary.get("eligible_for_manual_review_count"))
    continue_shadow_count = _int(summary.get("continue_shadow_count"))
    rejected_count = _int(summary.get("rejected_count"))
    needs_more_data_count = _int(summary.get("needs_more_data_count"))
    blocked_count = _int(summary.get("blocked_count"))
    if not any(
        (
            eligible_count,
            continue_shadow_count,
            rejected_count,
            needs_more_data_count,
            blocked_count,
        )
    ):
        eligible_count = _int(status_counts.get("eligible_for_manual_review"))
        continue_shadow_count = _int(status_counts.get("continue_shadow"))
        rejected_count = _int(status_counts.get("rejected"))
        needs_more_data_count = _int(status_counts.get("needs_more_data"))
        blocked_count = _int(status_counts.get("blocked"))
    main_reason = _text(summary.get("main_reason"), _text(report.get("reason"), "none"))
    safety_status = _etf_parameter_review_safety_status(report)
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"ETF Parameter Review: status={status}; "
            f"candidates_reviewed={candidate_count}; "
            f"eligible_for_manual_review={eligible_count}; "
            f"continue_shadow={continue_shadow_count}; rejected={rejected_count}; "
            f"safety={safety_status}."
        ),
        "candidate_count": candidate_count,
        "eligible_for_manual_review_count": eligible_count,
        "continue_shadow_count": continue_shadow_count,
        "rejected_count": rejected_count,
        "needs_more_data_count": needs_more_data_count,
        "blocked_count": blocked_count,
        "main_reason": main_reason,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _missing_etf_parameter_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "ETF Parameter Review: no latest parameter review report found.",
        "candidate_count": 0,
        "eligible_for_manual_review_count": 0,
        "continue_shadow_count": 0,
        "rejected_count": 0,
        "needs_more_data_count": 0,
        "blocked_count": 0,
        "main_reason": "PARAMETER_REVIEW_REPORT_MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "limitation": (
            "ETF parameter review artifact is missing; Reader Brief does not run "
            "parameter-review CLI."
        ),
    }


def _etf_parameter_review_safety_status(payload: Mapping[str, Any]) -> str:
    safe = (
        payload.get("observe_only") is True
        and payload.get("candidate_only") is True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") == "none"
        and payload.get("manual_review_required") is True
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_weight_calibration_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_weight_calibration_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_weight_dual_track_calibration_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_weight_calibration_summary()
    summary = _mapping(report.get("summary"))
    search = _mapping(report.get("search_configuration"))
    proposals = _mapping(report.get("proposal_scorecard"))
    proposal_counts = _mapping(proposals.get("proposal_type_counts"))
    top_candidates = _records(report.get("top_historical_candidates"))
    top_candidate = top_candidates[0] if top_candidates else {}
    safety_status = _etf_weight_calibration_safety_status(report)
    top_candidate_id = _text(
        summary.get("top_candidate_id"),
        _text(top_candidate.get("weight_set_id"), "MISSING"),
    )
    forward_status = _text(
        summary.get("dominant_forward_evidence_status"),
        _text(_mapping(report.get("forward_evidence_comparison")).get("status"), "MISSING"),
    )
    overfit_risk = _text(summary.get("highest_overfit_risk_band"), "MISSING")
    manual_review_count = _int(summary.get("manual_review_proposal_count"))
    if manual_review_count == 0:
        manual_review_count = _int(proposal_counts.get("propose_manual_baseline_review"))
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"ETF Weight Calibration: search_pack={_text(search.get('search_id'), 'MISSING')}; "
            f"top_candidate={top_candidate_id}; forward={forward_status}; "
            f"overfit_risk={overfit_risk}; safety={safety_status}."
        ),
        "search_pack": _text(search.get("search_id"), "MISSING"),
        "top_historical_candidate": top_candidate_id,
        "forward_evidence_status": forward_status,
        "overfit_risk": overfit_risk,
        "candidate_status": _text(top_candidate.get("status"), _text(report.get("status"))),
        "manual_review_proposals": manual_review_count,
        "proposal_type_counts": dict(proposal_counts),
        "candidate_count": _int(summary.get("candidate_count")),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _missing_etf_weight_calibration_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "ETF Weight Calibration: no latest dual-track report found.",
        "search_pack": "MISSING",
        "top_historical_candidate": "MISSING",
        "forward_evidence_status": "MISSING",
        "overfit_risk": "MISSING",
        "candidate_status": "MISSING",
        "manual_review_proposals": 0,
        "proposal_type_counts": {},
        "candidate_count": 0,
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "limitation": (
            "ETF weight calibration artifact is missing; Reader Brief does not run "
            "weight-calibration CLI."
        ),
    }


def _etf_initial_weight_candidate_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_initial_weight_candidate_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_initial_weight_recommendation_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_initial_weight_candidate_summary()
    data_range = _mapping(report.get("data_range_and_preset"))
    preset = _mapping(data_range.get("historical_range_preset"))
    top_candidates = _records(report.get("top_n_candidates"))
    top_candidate = top_candidates[0] if top_candidates else {}
    shadow = _mapping(report.get("shadow_enrollment_recommendations"))
    regime = _mapping(report.get("regime_robustness"))
    best_robustness = _best_initial_weight_robustness_candidate(
        _records(regime.get("candidate_summary"))
    )
    safety_status = _etf_weight_calibration_safety_status(report)
    top_candidate_id = _text(top_candidate.get("weight_set_id"), "MISSING")
    suggested_action = _text(shadow.get("suggested_action"), "MISSING")
    overfit_risk = _text(top_candidate.get("overfit_risk"), "MISSING")
    blocked_count = _int(shadow.get("blocked_candidate_count"))
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"ETF Initial Weight Candidates: preset={_text(preset.get('preset_id'), 'MISSING')}; "
            f"top={top_candidate_id}; action={suggested_action}; "
            f"overfit={overfit_risk}; blocked={blocked_count}; safety={safety_status}."
        ),
        "latest_search_preset": _text(preset.get("preset_id"), "MISSING"),
        "top_candidate": top_candidate_id,
        "suggested_action": suggested_action,
        "overfit_risk": overfit_risk,
        "best_robustness": best_robustness,
        "blocked_candidate_count": blocked_count,
        "recommended_weight_set_ids": list(shadow.get("recommended_weight_set_ids") or []),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _missing_etf_initial_weight_candidate_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "ETF Initial Weight Candidates: no latest recommendation report found."
        ),
        "latest_search_preset": "MISSING",
        "top_candidate": "MISSING",
        "suggested_action": "MISSING",
        "overfit_risk": "MISSING",
        "best_robustness": "MISSING",
        "blocked_candidate_count": 0,
        "recommended_weight_set_ids": [],
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "limitation": (
            "ETF initial weight recommendation artifact is missing; Reader Brief does not "
            "run weight-calibration recommendation CLI."
        ),
    }


def _etf_weight_calibration_profiling_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_weight_calibration_profiling_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_weight_calibration_profiling_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_weight_calibration_profiling_summary()
    step_timing = _mapping(report.get("step_timing"))
    slowest_steps = _records(step_timing.get("slowest_steps"))
    slowest_step = slowest_steps[0] if slowest_steps else {}
    cache_hit_rate = _profiling_cache_hit_rate(report)
    safety_status = _etf_weight_calibration_safety_status(report)
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), "AVAILABLE"),
        "profile_mode": _text(report.get("profile_mode"), "UNKNOWN"),
        "total_runtime_seconds": report.get("total_runtime_seconds"),
        "slowest_step": _text(slowest_step.get("step_id"), "MISSING"),
        "slowest_step_seconds": slowest_step.get("duration_seconds"),
        "cache_hit_rate": cache_hit_rate,
        "next_step_recommendation": _text(
            report.get("next_step_recommendation"),
            "profile_cold_run_before_numerical_optimization",
        ),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": (
            "Weight Calibration Profiling: "
            f"mode={_text(report.get('profile_mode'), 'UNKNOWN')}; "
            f"runtime={report.get('total_runtime_seconds')}; "
            f"slowest={_text(slowest_step.get('step_id'), 'MISSING')}; "
            f"cache_hit_rate={cache_hit_rate}; safety={safety_status}."
        ),
    }


def _missing_etf_weight_calibration_profiling_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "profile_mode": "MISSING",
        "total_runtime_seconds": None,
        "slowest_step": "MISSING",
        "slowest_step_seconds": None,
        "cache_hit_rate": "MISSING",
        "next_step_recommendation": "profile_cold_run_before_numerical_optimization",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": (
            "Weight Calibration Profiling: no latest profiling report found; "
            "Reader Brief does not run diagnostics or cProfile."
        ),
    }


def _profiling_cache_hit_rate(report: Mapping[str, Any]) -> float | str:
    cache_layers = _records(_mapping(report.get("cache_timing_breakdown")).get("cache_layers"))
    hits = sum(_int(layer.get("hit_count")) for layer in cache_layers)
    misses = sum(_int(layer.get("miss_count")) for layer in cache_layers)
    reads = hits + misses
    if reads <= 0:
        return "MISSING"
    return round(hits / reads, 6)


def _best_initial_weight_robustness_candidate(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "MISSING"
    selected = min(
        rows,
        key=lambda row: (
            _int(row.get("warning_count")),
            _int(row.get("missing_regime_count")),
            -(_float_or_none(row.get("worst_max_drawdown")) or -1.0),
            _text(row.get("weight_set_id")),
        ),
    )
    return _text(selected.get("weight_set_id"), "MISSING")


def _etf_weight_calibration_safety_status(payload: Mapping[str, Any]) -> str:
    safe = (
        payload.get("observe_only") is True
        and payload.get("candidate_only") is True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") == "none"
        and payload.get("manual_review_required") is True
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


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


def _etf_data_quality_governance_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_data_quality_governance_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_data_quality_governance_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_data_quality_governance_summary()

    status = _text(report.get("status"), "UNKNOWN")
    blocking_failures = _records(report.get("blocking_failures"))
    warnings = _records(report.get("warnings"))
    safety_status = _etf_data_quality_governance_safety_status(report)
    section_status = {
        "price_freshness_status": _section_blocking_status(report.get("price_freshness")),
        "missing_bars_status": _section_blocking_status(report.get("missing_bars")),
        "return_outliers_status": _section_blocking_status(report.get("return_outliers")),
        "config_drift_status": _section_blocking_status(
            report.get("config_hash_model_version_drift")
        ),
        "evidence_completeness_status": _section_blocking_status(
            report.get("evidence_completeness")
        ),
        "gate_freshness_status": _section_blocking_status(report.get("validation_gate_freshness")),
        "report_staleness_status": _section_blocking_status(report.get("report_staleness")),
        "reader_brief_link_status": _section_blocking_status(report.get("reader_brief_links")),
    }
    safety_banner = _mapping(report.get("safety_banner"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"ETF Data Quality: status={status}; "
            f"blocking_failures={len(blocking_failures)}; warnings={len(warnings)}; "
            f"price_freshness={section_status['price_freshness_status']}; "
            f"missing_bars={section_status['missing_bars_status']}; "
            f"reader_brief_links={section_status['reader_brief_link_status']}; "
            f"safety={safety_status}."
        ),
        "blocking_failure_count": len(blocking_failures),
        "warning_count": len(warnings),
        "blocking_failures": [
            _text(item.get("finding_id"), "UNKNOWN") for item in blocking_failures[:5]
        ],
        "warning_findings": [_text(item.get("finding_id"), "UNKNOWN") for item in warnings[:5]],
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety_banner.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety_banner.get("broker_action"), "none"),
        "manual_review_required": safety_banner.get("manual_review_required") is True,
        "commands_executed": report.get("commands_executed") is True,
        "production_state_mutated": report.get("production_state_mutated") is True,
        **section_status,
    }


def _missing_etf_data_quality_governance_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "ETF Data Quality: no latest governance report found.",
        "blocking_failure_count": 0,
        "warning_count": 1,
        "blocking_failures": [],
        "warning_findings": ["etf_data_quality_governance_report"],
        "detail_report": "",
        "price_freshness_status": "MISSING",
        "missing_bars_status": "MISSING",
        "return_outliers_status": "MISSING",
        "config_drift_status": "MISSING",
        "evidence_completeness_status": "MISSING",
        "gate_freshness_status": "MISSING",
        "report_staleness_status": "MISSING",
        "reader_brief_link_status": "MISSING",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "ETF data quality governance artifact is missing; Reader Brief does not run "
            "etf data-quality report CLI."
        ),
    }


def _section_blocking_status(section: Any) -> str:
    summary = _mapping(_mapping(section).get("summary"))
    blocking_count = _int(summary.get("blocking_count"))
    warning_count = _int(summary.get("warning_count"))
    if blocking_count > 0:
        return "BLOCKED"
    if warning_count > 0:
        return "WARNING"
    if summary:
        return "PASS"
    return "MISSING"


def _etf_data_quality_governance_safety_status(payload: Mapping[str, Any]) -> str:
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


def _etf_strategy_evidence_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_strategy_evidence_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_strategy_evidence_dashboard",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_strategy_evidence_summary()

    cards = _records(report.get("evidence_cards"))
    priorities = _records(report.get("manual_review_priorities"))
    conflicts = _records(report.get("conflicts"))
    data_quality_overlay = _mapping(report.get("data_quality_overlay"))
    overall_status = _text(report.get("overall_status"), "UNKNOWN")
    strongest = _strategy_evidence_card_list(cards, strongest=True)
    weakest = _strategy_evidence_card_list(cards, strongest=False)
    blocking_cards = [
        _text(card.get("category"), "UNKNOWN")
        for card in cards
        if _text(card.get("status")) in {"blocked", "invalid", "stale"}
    ]
    safety_status = _etf_strategy_evidence_safety_status(report)
    safety = _mapping(report.get("safety"))
    return {
        "availability": "AVAILABLE",
        "overall_status": overall_status,
        "status": overall_status,
        "summary_sentence": (
            f"Strategy Evidence Dashboard: overall_status={overall_status}; "
            f"strongest={strongest}; weakest={weakest}; "
            f"blocking_issues={len(blocking_cards)}; "
            f"manual_review_priorities={len(priorities)}; "
            f"data_quality={_text(data_quality_overlay.get('status'), 'unknown')}; "
            f"safety={safety_status}."
        ),
        "strongest_evidence": strongest,
        "weakest_evidence": weakest,
        "blocking_issues": "none" if not blocking_cards else ", ".join(blocking_cards[:5]),
        "manual_review_priority_count": len(priorities),
        "conflict_count": len(conflicts),
        "data_quality_status": _text(data_quality_overlay.get("status"), "unknown"),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "commands_executed": report.get("commands_executed") is True,
        "production_state_mutated": report.get("production_state_mutated") is True,
    }


def _missing_etf_strategy_evidence_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "overall_status": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Strategy Evidence Dashboard: no latest dashboard report found.",
        "strongest_evidence": "MISSING",
        "weakest_evidence": "MISSING",
        "blocking_issues": "MISSING",
        "manual_review_priority_count": 0,
        "conflict_count": 0,
        "data_quality_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "Strategy evidence dashboard artifact is missing; Reader Brief does not run "
            "etf evidence-dashboard report CLI."
        ),
    }


def _strategy_evidence_card_list(cards: list[dict[str, Any]], *, strongest: bool) -> str:
    if not cards:
        return "none"
    ordered = sorted(
        cards,
        key=lambda card: (
            (
                -_strategy_evidence_status_rank(_text(card.get("status")))
                if strongest
                else _strategy_evidence_status_rank(_text(card.get("status")))
            ),
            _text(card.get("category")),
        ),
    )
    labels = [
        f"{_text(card.get('category'), 'UNKNOWN')}={_text(card.get('status'), 'UNKNOWN')}"
        for card in ordered[:3]
    ]
    return ", ".join(labels)


def _strategy_evidence_status_rank(status: str) -> int:
    return {
        "strong_support": 7,
        "supportive": 6,
        "mixed": 5,
        "needs_more_data": 4,
        "weak": 3,
        "stale": 2,
        "blocked": 1,
        "invalid": 0,
    }.get(status, 0)


def _etf_strategy_evidence_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
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


def _etf_baseline_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_baseline_review_summary()
    package_path = _report_index_artifact_path(report_index, "etf_baseline_review_package")
    package = _read_optional_json(package_path)
    if not package:
        return _missing_etf_baseline_review_summary()
    decision_path = _report_index_artifact_path(report_index, "etf_baseline_review_decision")
    proposal_path = _report_index_artifact_path(
        report_index,
        "etf_baseline_change_proposal_draft",
    )
    outcome_path = _report_index_artifact_path(report_index, "etf_baseline_review_outcome")
    decision = _read_optional_json(decision_path)
    proposal = _read_optional_json(proposal_path)
    outcome = _read_optional_json(outcome_path)
    review_summary = _mapping(package.get("review_summary"))
    eligibility = _mapping(package.get("eligibility"))
    eligible_count = _int(review_summary.get("eligible_count"))
    needs_more_data_count = _int(review_summary.get("needs_more_data_count"))
    blocked_count = _int(review_summary.get("blocked_count"))
    proposal_count = 1 if proposal else _int(review_summary.get("proposal_draft_count"))
    latest_decision = _text(decision.get("owner_decision"), "MISSING" if not decision else "")
    latest_outcome = _text(outcome.get("latest_review_status"), "MISSING" if not outcome else "")
    safety_status = _etf_baseline_review_safety_status(package)
    safety = _mapping(package.get("safety"))
    status = _text(eligibility.get("eligibility_status"), _text(package.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"Baseline Candidate Review: eligible={eligible_count}; "
            f"needs_more_data={needs_more_data_count}; blocked={blocked_count}; "
            f"latest_package={_text(package.get('candidate_id'), 'UNKNOWN')}; "
            f"latest_decision={latest_decision}; proposal_drafts={proposal_count}; "
            f"safety={safety_status}."
        ),
        "eligible_count": eligible_count,
        "needs_more_data_count": needs_more_data_count,
        "blocked_count": blocked_count,
        "latest_review_package": _text(package.get("candidate_id"), "UNKNOWN"),
        "latest_owner_decision": latest_decision,
        "proposal_draft_count": proposal_count,
        "latest_outcome_status": latest_outcome,
        "detail_report": "" if package_path is None else str(package_path),
        "decision_report": "" if decision_path is None else str(decision_path),
        "proposal_report": "" if proposal_path is None else str(proposal_path),
        "outcome_report": "" if outcome_path is None else str(outcome_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "commands_executed": package.get("commands_executed") is True,
        "production_state_mutated": package.get("production_state_mutated") is True,
    }


def _missing_etf_baseline_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Baseline Candidate Review: no latest review package found.",
        "eligible_count": 0,
        "needs_more_data_count": 0,
        "blocked_count": 0,
        "latest_review_package": "MISSING",
        "latest_owner_decision": "MISSING",
        "proposal_draft_count": 0,
        "latest_outcome_status": "MISSING",
        "detail_report": "",
        "decision_report": "",
        "proposal_report": "",
        "outcome_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "Baseline review package artifact is missing; Reader Brief does not run "
            "etf baseline-review package CLI."
        ),
    }


def _etf_baseline_review_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
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


def _etf_shadow_candidate_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_shadow_candidate_review_summary()
    package_path = _report_index_artifact_path(
        report_index,
        "etf_shadow_candidate_review_package",
    )
    package = _read_optional_json(package_path)
    if not package:
        return _missing_etf_shadow_candidate_review_summary()
    approval_path = _report_index_artifact_path(
        report_index,
        "etf_shadow_candidate_owner_approval",
    )
    enrollment_path = _report_index_artifact_path(
        report_index,
        "etf_shadow_candidate_enrollment",
    )
    approval = _read_optional_json(approval_path)
    enrollment = _read_optional_json(enrollment_path)
    summary = _mapping(package.get("review_summary"))
    top_candidates = _records(package.get("top_review_candidates"))
    top_candidate = _text(
        summary.get("top_candidate"),
        _text(top_candidates[0].get("shape_id"), "MISSING") if top_candidates else "MISSING",
    )
    approved_count = 1 if enrollment else _int(summary.get("approved_enrollment_count"))
    latest_decision = _text(
        approval.get("owner_decision"),
        "MISSING" if not approval else "",
    )
    latest_enrollment = _text(
        enrollment.get("shadow_candidate_id"),
        "MISSING" if not enrollment else "",
    )
    safety_status = _etf_shadow_candidate_review_safety_status(package)
    safety = _mapping(package.get("safety"))
    status = _text(summary.get("owner_approval_status"), _text(package.get("status"), "UNKNOWN"))
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"Shadow Candidate Review: top={top_candidate}; "
            f"pending_review={_int(summary.get('pending_review_count'))}; "
            f"approved_enrollments={approved_count}; "
            f"latest_decision={latest_decision}; "
            f"latest_enrollment={latest_enrollment}; safety={safety_status}."
        ),
        "top_candidate": top_candidate,
        "pending_review_count": _int(summary.get("pending_review_count")),
        "blocked_count": _int(summary.get("blocked_count")),
        "approved_enrollment_count": approved_count,
        "latest_owner_decision": latest_decision,
        "latest_enrollment": latest_enrollment,
        "detail_report": "" if package_path is None else str(package_path),
        "approval_report": "" if approval_path is None else str(approval_path),
        "enrollment_report": "" if enrollment_path is None else str(enrollment_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "commands_executed": package.get("commands_executed") is True,
        "production_state_mutated": package.get("production_state_mutated") is True,
    }


def _missing_etf_shadow_candidate_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Shadow Candidate Review: no latest review package found.",
        "top_candidate": "MISSING",
        "pending_review_count": 0,
        "blocked_count": 0,
        "approved_enrollment_count": 0,
        "latest_owner_decision": "MISSING",
        "latest_enrollment": "MISSING",
        "detail_report": "",
        "approval_report": "",
        "enrollment_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "Shadow candidate review package artifact is missing; Reader Brief does not run "
            "etf shadow-review package CLI."
        ),
    }


def _etf_shadow_candidate_review_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
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


def _etf_trend_calibration_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_trend_calibration_summary()
    report_path = _report_index_artifact_path(report_index, "etf_trend_calibration_report")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_trend_calibration_summary()
    summary = _mapping(payload.get("summary"))
    coverage = _mapping(payload.get("dataset_coverage"))
    safety = _mapping(payload.get("safety"))
    safety_status = _etf_trend_calibration_safety_status(payload)
    top_config = _text(summary.get("top_config"), "MISSING")
    evidence_status = _text(summary.get("evidence_status"), _text(payload.get("status"), "UNKNOWN"))
    redundancy_risk = _text(summary.get("redundancy_risk"), "unknown")
    regime_stability = _text(summary.get("regime_stability"), "unknown")
    data_quality_status = _text(
        summary.get("data_quality_status"),
        _text(coverage.get("data_quality_status"), "UNKNOWN"),
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"Trend Signal Calibration: top={top_config}; "
            f"evidence={evidence_status}; redundancy={redundancy_risk}; "
            f"regime_stability={regime_stability}; data_quality={data_quality_status}; "
            f"safety={safety_status}."
        ),
        "top_config": top_config,
        "evidence_status": evidence_status,
        "redundancy_risk": redundancy_risk,
        "regime_stability": regime_stability,
        "data_quality_status": data_quality_status,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "evaluation_only": payload.get("evaluation_only") is True,
        "commands_executed": payload.get("commands_executed") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
    }


def _missing_etf_trend_calibration_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Trend Signal Calibration: no latest calibration report found.",
        "top_config": "MISSING",
        "evidence_status": "MISSING",
        "redundancy_risk": "MISSING",
        "regime_stability": "MISSING",
        "data_quality_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "evaluation_only": True,
        "commands_executed": False,
        "production_state_mutated": False,
        "limitation": (
            "Trend calibration report artifact is missing; Reader Brief does not run "
            "etf trend-calibration run CLI."
        ),
    }


def _etf_trend_calibration_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("evaluation_only") is True
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; evaluation_only=true; "
        "commands_executed=false; production_state_mutated=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_allocation_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_allocation_summary()
    report_path = _report_index_artifact_path(report_index, "etf_dynamic_allocation_report")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_allocation_summary()
    summary = _mapping(payload.get("summary"))
    safety = _mapping(payload.get("safety"))
    safety_status = _etf_dynamic_allocation_safety_status(payload)
    weights = _mapping(summary.get("candidate_target_weights"))
    selected_regime = _text(summary.get("selected_regime"), "MISSING")
    rebalance_decision = _text(summary.get("rebalance_decision"), "MISSING")
    policy_id = _text(summary.get("policy_id"), "MISSING")
    data_quality_status = _text(summary.get("data_quality_status"), "UNKNOWN")
    constraint_count = summary.get("constraint_count", 0)
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"Dynamic Allocation Candidate: policy={policy_id}; "
            f"regime={selected_regime}; rebalance={rebalance_decision}; "
            f"constraints={constraint_count}; data_quality={data_quality_status}; "
            f"safety={safety_status}."
        ),
        "policy_id": policy_id,
        "selected_regime": selected_regime,
        "rebalance_decision": rebalance_decision,
        "candidate_target_weights": (
            ", ".join(f"{key}={float(value):.2%}" for key, value in weights.items())
            if weights
            else "MISSING"
        ),
        "constraint_count": constraint_count,
        "data_quality_status": data_quality_status,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
    }


def _missing_etf_dynamic_allocation_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Dynamic Allocation Candidate: no latest allocation report found.",
        "policy_id": "MISSING",
        "selected_regime": "MISSING",
        "rebalance_decision": "MISSING",
        "candidate_target_weights": "MISSING",
        "constraint_count": 0,
        "data_quality_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "limitation": (
            "Dynamic allocation report artifact is missing; Reader Brief does not run "
            "etf dynamic-allocation decide CLI."
        ),
    }


def _etf_dynamic_allocation_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_calibration_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_calibration_summary()
    report_path = _report_index_artifact_path(report_index, "etf_dynamic_calibration_report")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_calibration_summary()
    summary = _mapping(payload.get("summary"))
    cache = _mapping(payload.get("cache_summary"))
    safety = _mapping(payload.get("safety"))
    safety_status = _etf_dynamic_calibration_safety_status(payload)
    pack_id = _text(payload.get("requested_pack_id"), "MISSING")
    top_candidate = _text(summary.get("top_dynamic_candidate_pack_id"), "MISSING")
    top_score = summary.get("top_ranking_score", "UNKNOWN")
    candidate_count = payload.get("candidate_pack_count", 0)
    data_quality_status = _text(summary.get("data_quality_status"), "UNKNOWN")
    full_backtest_required = summary.get("full_robustness_backtest_required") is True
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"Dynamic Calibration Batch: pack={pack_id}; top={top_candidate}; "
            f"score={top_score}; candidates={candidate_count}; "
            f"data_quality={data_quality_status}; safety={safety_status}."
        ),
        "pack_id": pack_id,
        "top_candidate": top_candidate,
        "top_ranking_score": top_score,
        "candidate_pack_count": candidate_count,
        "cache_hit_rate": cache.get("cache_hit_rate", "UNKNOWN"),
        "cache_write_count": cache.get("cache_write_count", "UNKNOWN"),
        "data_quality_status": data_quality_status,
        "full_robustness_backtest_required": full_backtest_required,
        "calibration_proxy": payload.get("calibration_proxy", True),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
    }


def _missing_etf_dynamic_calibration_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Dynamic Calibration Batch: no latest batch report found.",
        "pack_id": "MISSING",
        "top_candidate": "MISSING",
        "top_ranking_score": "MISSING",
        "candidate_pack_count": 0,
        "cache_hit_rate": "MISSING",
        "cache_write_count": "MISSING",
        "data_quality_status": "MISSING",
        "full_robustness_backtest_required": True,
        "calibration_proxy": True,
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "limitation": (
            "Dynamic calibration report artifact is missing; Reader Brief does not run "
            "etf dynamic-calibration run CLI."
        ),
    }


def _etf_dynamic_calibration_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_robustness_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_robustness_summary()
    report_path = _report_index_artifact_path(report_index, "etf_dynamic_robustness_report")
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_robustness_summary()
    summary = _mapping(payload.get("summary"))
    safety = _mapping(payload.get("safety"))
    safety_status = _etf_dynamic_robustness_safety_status(payload)
    candidate = _text(summary.get("dynamic_candidate_id"), "MISSING")
    data_quality_status = _text(summary.get("data_quality_status"), "UNKNOWN")
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            f"Dynamic Robustness Review: candidate={candidate}; "
            f"excess_vs_static={summary.get('excess_vs_static_base')}; "
            f"false_off={summary.get('false_risk_off_count')}; "
            f"false_on={summary.get('false_risk_on_count')}; "
            f"overfit={summary.get('overfit_status')}; "
            f"data_quality={data_quality_status}; safety={safety_status}."
        ),
        "candidate": candidate,
        "dynamic_total_return": summary.get("dynamic_total_return"),
        "dynamic_cagr": summary.get("dynamic_cagr"),
        "dynamic_max_drawdown": summary.get("dynamic_max_drawdown"),
        "excess_vs_static_base": summary.get("excess_vs_static_base"),
        "false_risk_off_count": summary.get("false_risk_off_count"),
        "false_risk_on_count": summary.get("false_risk_on_count"),
        "overfit_status": summary.get("overfit_status"),
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "data_quality_status": data_quality_status,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": _text(safety.get("production_effect"), PRODUCTION_EFFECT),
        "broker_action": _text(safety.get("broker_action"), "none"),
        "manual_review_required": safety.get("manual_review_required") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_robustness_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Dynamic Robustness Review: no latest robustness report found.",
        "candidate": "MISSING",
        "dynamic_total_return": "MISSING",
        "dynamic_cagr": "MISSING",
        "dynamic_max_drawdown": "MISSING",
        "excess_vs_static_base": "MISSING",
        "false_risk_off_count": "MISSING",
        "false_risk_on_count": "MISSING",
        "overfit_status": "MISSING",
        "shadow_enrollment_allowed": False,
        "data_quality_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic robustness report artifact is missing; Reader Brief does not run "
            "etf dynamic-robustness report CLI."
        ),
    }


def _etf_dynamic_robustness_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_rescue_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_rescue_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_rescue_evaluation_report",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_rescue_summary()
    failed = _mapping(payload.get("failed_v0_1_summary"))
    improvement = _mapping(payload.get("improvement_summary"))
    best = _mapping(payload.get("best_rescue_candidate"))
    blockers = _records(payload.get("remaining_blockers"))
    safety_status = _etf_dynamic_rescue_safety_status(payload)
    main_failures = failed.get("reason_codes", [])
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic Strategy Rescue: "
            f"failed_v0_1={failed.get('status', 'MISSING')}; "
            f"best={improvement.get('best_candidate', 'MISSING')}; "
            f"best_status={improvement.get('best_status', 'MISSING')}; "
            f"safety={safety_status}."
        ),
        "failed_v0_1_status": failed.get("status", "MISSING"),
        "main_failures": ", ".join(str(item) for item in main_failures),
        "best_rescue_candidate": improvement.get("best_candidate", "MISSING"),
        "best_status": improvement.get("best_status", "MISSING"),
        "false_risk_off_reduction": best.get("false_risk_off_reduction", "MISSING"),
        "turnover_reduction": best.get("turnover_reduction", "MISSING"),
        "remaining_blockers": ", ".join(str(item.get("blocker_id")) for item in blockers),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "automatic_enrollment_allowed": payload.get("automatic_enrollment_allowed") is True,
        "owner_approval_executed": payload.get("owner_approval_executed") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_rescue_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Dynamic Strategy Rescue: no latest rescue report found.",
        "failed_v0_1_status": "MISSING",
        "main_failures": "MISSING",
        "best_rescue_candidate": "MISSING",
        "best_status": "MISSING",
        "false_risk_off_reduction": "MISSING",
        "turnover_reduction": "MISSING",
        "remaining_blockers": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic rescue report artifact is missing; Reader Brief does not run "
            "etf dynamic-rescue run."
        ),
    }


def _etf_dynamic_rescue_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
        and payload.get("automatic_enrollment_allowed") is False
        and payload.get("owner_approval_executed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "automatic_enrollment_allowed=false; owner_approval_executed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v2_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v2_review_summary()
    package_path = _report_index_artifact_path(report_index, "etf_dynamic_v2_review_package")
    payload = _read_optional_json(package_path)
    if not payload:
        return _missing_etf_dynamic_v2_review_summary()
    evidence = _mapping(payload.get("candidate_evidence"))
    gate = _mapping(payload.get("shadow_review_eligibility_gate"))
    safety_status = _etf_dynamic_v2_review_safety_status(payload)
    positive = _texts(gate.get("positive_reason_codes"))
    blockers = _texts(gate.get("blocking_reason_codes"))
    actions = _texts(payload.get("recommended_next_actions"))
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "review_status": _text(payload.get("review_status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic v0.2 Review: "
            f"candidate={evidence.get('candidate_id', 'MISSING')}; "
            f"status={payload.get('review_status', 'UNKNOWN')}/"
            f"{payload.get('status', 'UNKNOWN')}; "
            f"blockers={', '.join(blockers) or 'none'}; safety={safety_status}."
        ),
        "candidate": evidence.get("candidate_id", "MISSING"),
        "rescue_policy_id": evidence.get("rescue_policy_id", "MISSING"),
        "improvements": ", ".join(positive),
        "blockers": ", ".join(blockers),
        "next_action": actions[0] if actions else "MISSING",
        "package_report": "" if package_path is None else str(package_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "automatic_enrollment_allowed": payload.get("automatic_enrollment_allowed") is True,
        "owner_approval_executed": payload.get("owner_approval_executed") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_v2_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "review_status": "MISSING",
        "summary_sentence": "Dynamic v0.2 Review: no latest review package found.",
        "candidate": "MISSING",
        "rescue_policy_id": "MISSING",
        "improvements": "MISSING",
        "blockers": "MISSING",
        "next_action": "MISSING",
        "package_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic v0.2 review package artifact is missing; Reader Brief does not run "
            "etf dynamic-v2-review package."
        ),
    }


def _etf_dynamic_v2_review_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
        and payload.get("automatic_enrollment_allowed") is False
        and payload.get("owner_approval_executed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "automatic_enrollment_allowed=false; owner_approval_executed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v3_rescue_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_rescue_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_v3_rescue_evaluation_report",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_v3_rescue_summary()
    best = _mapping(payload.get("best_candidate"))
    blocker_summary = _mapping(payload.get("v0_4_blocker_summary"))
    safety_status = _etf_dynamic_v3_rescue_safety_status(payload)
    remaining = _records(payload.get("remaining_blockers"))
    constraint_status = "improved" if best.get("constraint_fixed") is True else "still_blocked"
    drawdown_status = "improved" if best.get("drawdown_fixed") is True else "still_blocked"
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "review_status": _text(payload.get("review_status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic v0.3 Rescue: "
            f"base={blocker_summary.get('candidate', 'MISSING')}; "
            f"best={best.get('policy_id', 'MISSING')}; "
            f"constraint={constraint_status}; drawdown={drawdown_status}; "
            f"safety={safety_status}."
        ),
        "base_candidate": blocker_summary.get("candidate", "MISSING"),
        "best_candidate": best.get("policy_id", "MISSING"),
        "best_candidate_status": best.get("candidate_status", "MISSING"),
        "constraint_status": constraint_status,
        "constraint_hit_delta_vs_v0_4": best.get("constraint_hit_delta_vs_v0_4", "MISSING"),
        "drawdown_status": drawdown_status,
        "drawdown_preservation": best.get("drawdown_preservation", "MISSING"),
        "remaining_blockers": ", ".join(str(item.get("blocker_id")) for item in remaining),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "automatic_enrollment_allowed": payload.get("automatic_enrollment_allowed") is True,
        "owner_approval_executed": payload.get("owner_approval_executed") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_v3_rescue_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "review_status": "MISSING",
        "summary_sentence": "Dynamic v0.3 Rescue: no latest rescue report found.",
        "base_candidate": "MISSING",
        "best_candidate": "MISSING",
        "best_candidate_status": "MISSING",
        "constraint_status": "MISSING",
        "constraint_hit_delta_vs_v0_4": "MISSING",
        "drawdown_status": "MISSING",
        "drawdown_preservation": "MISSING",
        "remaining_blockers": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic v0.3 rescue report artifact is missing; Reader Brief does not run "
            "etf dynamic-v3-rescue run."
        ),
    }


def _etf_dynamic_v3_rescue_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
        and payload.get("automatic_enrollment_allowed") is False
        and payload.get("owner_approval_executed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "automatic_enrollment_allowed=false; owner_approval_executed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v3_real_evaluation_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_real_evaluation_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_v3_real_evaluation_report",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_v3_real_evaluation_summary()
    summary = _mapping(payload.get("summary"))
    best = _mapping(payload.get("best_candidate"))
    gate = _mapping(payload.get("promotion_gate"))
    safety_status = _etf_dynamic_v3_real_evaluation_safety_status(payload)
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "promotion_gate_decision": _text(
            payload.get("promotion_gate_decision"),
            "UNKNOWN",
        ),
        "summary_sentence": (
            "Dynamic v0.3 Real Evaluation: "
            f"gate={payload.get('promotion_gate_decision', 'UNKNOWN')}; "
            f"best={best.get('policy_id', summary.get('best_v0_3_candidate', 'MISSING'))}; "
            f"constraint_reduction={summary.get('constraint_hit_reduction_vs_v0_4')}; "
            f"static_gap={_format_percent(summary.get('dynamic_vs_static_gap'))}; "
            "manual review only, production_effect=none."
        ),
        "best_candidate": best.get("policy_id", summary.get("best_v0_3_candidate", "MISSING")),
        "constraint_hit_reduction_vs_v0_4": summary.get(
            "constraint_hit_reduction_vs_v0_4",
            "MISSING",
        ),
        "false_risk_off_delta_vs_v0_4": summary.get(
            "false_risk_off_delta_vs_v0_4",
            "MISSING",
        ),
        "drawdown_preservation": summary.get(
            "max_drawdown_degradation_vs_v0_4",
            "MISSING",
        ),
        "turnover": summary.get("turnover", "MISSING"),
        "static_gap": summary.get("dynamic_vs_static_gap", "MISSING"),
        "static_gap_delta_vs_v0_4": summary.get("static_gap_delta_vs_v0_4", "MISSING"),
        "overfit_status": summary.get("overfit_status", "MISSING"),
        "blockers": ", ".join(str(item) for item in _texts(gate.get("blocker_ids"))),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "automatic_enrollment_allowed": payload.get("automatic_enrollment_allowed") is True,
        "owner_approval_executed": payload.get("owner_approval_executed") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_v3_real_evaluation_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "promotion_gate_decision": "MISSING",
        "summary_sentence": (
            "Dynamic v0.3 Real Evaluation: no latest real evaluation report found."
        ),
        "best_candidate": "MISSING",
        "constraint_hit_reduction_vs_v0_4": "MISSING",
        "false_risk_off_delta_vs_v0_4": "MISSING",
        "drawdown_preservation": "MISSING",
        "turnover": "MISSING",
        "static_gap": "MISSING",
        "static_gap_delta_vs_v0_4": "MISSING",
        "overfit_status": "MISSING",
        "blockers": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic v0.3 real evaluation report artifact is missing; Reader Brief "
            "does not run etf dynamic-v3-rescue real-evaluate."
        ),
    }


def _etf_dynamic_v3_real_evaluation_safety_status(payload: Mapping[str, Any]) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and safety.get("shadow_enrollment_allowed") is False
        and safety.get("automatic_enrollment_allowed") is False
        and safety.get("owner_approval_executed") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
        and payload.get("automatic_enrollment_allowed") is False
        and payload.get("owner_approval_executed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "automatic_enrollment_allowed=false; owner_approval_executed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v3_failure_attribution_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_failure_attribution_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_v3_failure_attribution_report",
    )
    payload = _read_optional_json(report_path)
    if not payload:
        return _missing_etf_dynamic_v3_failure_attribution_summary()
    summary = _mapping(payload.get("summary"))
    safety_status = _etf_dynamic_v3_failure_attribution_safety_status(payload)
    return {
        "availability": "AVAILABLE",
        "status": _text(payload.get("status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic v0.3 Failure Attribution: "
            f"reject_reason={summary.get('v0_3_rejection_primary_reason', 'UNKNOWN')}; "
            f"v0_4_review={summary.get('v0_4_promotion_review', 'UNKNOWN')}; "
            f"v0_5={summary.get('v0_5_design_recommendation', 'UNKNOWN')}; "
            "manual review only, production_effect=none."
        ),
        "v0_3_rejection_primary_reason": summary.get(
            "v0_3_rejection_primary_reason",
            "MISSING",
        ),
        "v0_4_promotion_review": summary.get("v0_4_promotion_review", "MISSING"),
        "v0_5_design_recommendation": summary.get(
            "v0_5_design_recommendation",
            "MISSING",
        ),
        "constraint_hit_reduction_vs_v0_4": summary.get(
            "constraint_hit_reduction_vs_v0_4",
            "MISSING",
        ),
        "v0_3_constraint_hit_rate": summary.get("v0_3_constraint_hit_rate", "MISSING"),
        "v0_4_constraint_hit_rate": summary.get("v0_4_constraint_hit_rate", "MISSING"),
        "v0_3_constraint_hit_rate_pct": _format_percent(summary.get("v0_3_constraint_hit_rate")),
        "v0_4_constraint_hit_rate_pct": _format_percent(summary.get("v0_4_constraint_hit_rate")),
        "v0_3_overfit_status": summary.get("v0_3_overfit_status", "MISSING"),
        "v0_4_overfit_status": summary.get("v0_4_overfit_status", "MISSING"),
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": payload.get("shadow_enrollment_allowed") is True,
        "automatic_enrollment_allowed": payload.get("automatic_enrollment_allowed") is True,
        "owner_approval_executed": payload.get("owner_approval_executed") is True,
        "official_target_weights_mutated": payload.get("official_target_weights_mutated") is True,
        "baseline_config_mutated": payload.get("baseline_config_mutated") is True,
        "production_state_mutated": payload.get("production_state_mutated") is True,
        "automatic_candidate_promotion": payload.get("automatic_candidate_promotion") is True,
        "auto_enrollment_without_owner_approval": (
            payload.get("auto_enrollment_without_owner_approval") is True
        ),
    }


def _missing_etf_dynamic_v3_failure_attribution_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Dynamic v0.3 Failure Attribution: no latest attribution report found."
        ),
        "v0_3_rejection_primary_reason": "MISSING",
        "v0_4_promotion_review": "MISSING",
        "v0_5_design_recommendation": "MISSING",
        "constraint_hit_reduction_vs_v0_4": "MISSING",
        "v0_3_constraint_hit_rate": "MISSING",
        "v0_4_constraint_hit_rate": "MISSING",
        "v0_3_constraint_hit_rate_pct": "MISSING",
        "v0_4_constraint_hit_rate_pct": "MISSING",
        "v0_3_overfit_status": "MISSING",
        "v0_4_overfit_status": "MISSING",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic v0.3 failure attribution artifact is missing; Reader Brief "
            "does not run etf dynamic-v3-rescue failure-attribution."
        ),
    }


def _etf_dynamic_v3_failure_attribution_safety_status(
    payload: Mapping[str, Any],
) -> str:
    safety = _mapping(payload.get("safety"))
    safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and safety.get("shadow_enrollment_allowed") is False
        and safety.get("automatic_enrollment_allowed") is False
        and safety.get("owner_approval_executed") is False
        and payload.get("commands_executed") is False
        and payload.get("production_state_mutated") is False
        and payload.get("baseline_config_mutated") is False
        and payload.get("official_target_weights_mutated") is False
        and payload.get("automatic_candidate_promotion") is False
        and payload.get("auto_enrollment_without_owner_approval") is False
        and payload.get("shadow_enrollment_allowed") is False
        and payload.get("automatic_enrollment_allowed") is False
        and payload.get("owner_approval_executed") is False
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; shadow_enrollment_allowed=false; "
        "automatic_enrollment_allowed=false; owner_approval_executed=false; "
        "commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v3_sim_review_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_sim_review_summary()
    interpretation_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_sim_interpretation"),
        "sim_interpretation_manifest.json",
    )
    risk_return_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_sim_risk_return"),
        "risk_return_manifest.json",
    )
    defensive_validation_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_sim_defensive_validation"),
        "defensive_validation_manifest.json",
    )
    proposal_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_advisory_proposal_review"),
        "proposal_review_manifest.json",
    )
    confirmation_plan_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_forward_confirmation_plan"),
        "confirmation_plan_manifest.json",
    )
    confirmation_registry_path = _dynamic_v3_confirmation_registry_manifest_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_confirmation_registry"),
    )
    confirmation_progress_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_confirmation_progress"),
        "confirmation_progress_manifest.json",
    )
    confirmation_evaluation_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_confirmation_evaluation"),
        "confirmation_evaluation_manifest.json",
    )
    rule_review_cycle_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_rule_review_cycle"),
        "rule_review_cycle_manifest.json",
    )
    confirmation_weekly_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_confirmation_cycle_weekly"),
        "weekly_cycle_manifest.json",
    )
    pressure_regime_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_regime_tag"),
        "pressure_regime_manifest.json",
    )
    confirmation_dashboard_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_confirmation_dashboard"),
        "confirmation_dashboard_manifest.json",
    )
    rule_review_queue_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_rule_review_queue"),
        "rule_review_queue_manifest.json",
    )
    pressure_tag_diagnosis_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_tag_diagnosis"),
        "pressure_tag_diagnosis_manifest.json",
    )
    pressure_outcome_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_outcome_backfill"),
        "pressure_backfill_manifest.json",
    )
    defensive_pressure_compare_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_pressure_compare"),
        "defensive_pressure_compare_manifest.json",
    )
    defensive_rule_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_rule_review"),
        "defensive_rule_review_manifest.json",
    )
    weekly_ops_decision_update_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_weekly_ops_decision_update"),
        "weekly_ops_decision_update_manifest.json",
    )
    defensive_hypothesis_deep_dive_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_defensive_hypothesis_deep_dive",
        ),
        "deep_dive_manifest.json",
    )
    defensive_label_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_label_review"),
        "label_review_manifest.json",
    )
    defensive_failure_study_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_failure_study"),
        "failure_study_manifest.json",
    )
    defensive_research_note_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_research_note"),
        "defensive_research_note_manifest.json",
    )
    defensive_owner_pack_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_defensive_owner_pack"),
        "defensive_owner_pack_manifest.json",
    )
    forward_pressure_capture_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_forward_pressure_capture"),
        "capture_plan_manifest.json",
    )
    pressure_trigger_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_trigger"),
        "pressure_trigger_manifest.json",
    )
    pressure_capture_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_capture"),
        "pressure_capture_manifest.json",
    )
    pressure_sample_ledger_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_pressure_sample_ledger"),
        "pressure_sample_ledger_manifest.json",
    )
    weekly_defensive_evidence_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_weekly_defensive_evidence"),
        "weekly_defensive_manifest.json",
    )
    rule_owner_decision_path = _dynamic_v3_rule_owner_decision_journal_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_rule_owner_decision",
        )
    )
    interpretation = _read_optional_json(interpretation_path)
    interpretation_matrix = _read_optional_json(
        interpretation_path.parent / "variant_interpretation_matrix.json"
        if interpretation_path is not None
        else None
    )
    key_findings = _read_optional_json(
        interpretation_path.parent / "key_findings.json"
        if interpretation_path is not None
        else None
    )
    risk_return = _read_optional_json(risk_return_path)
    risk_summary = _read_optional_json(
        risk_return_path.parent / "risk_adjusted_summary.json"
        if risk_return_path is not None
        else None
    )
    defensive_validation = _read_optional_json(defensive_validation_path)
    defensive_summary = _read_optional_json(
        defensive_validation_path.parent / "defensive_validation_summary.json"
        if defensive_validation_path is not None
        else None
    )
    proposal_review = _read_optional_json(proposal_review_path)
    decision_matrix = _read_optional_json(
        proposal_review_path.parent / "proposal_decision_matrix.json"
        if proposal_review_path is not None
        else None
    )
    confirmation_plan = _read_optional_json(confirmation_plan_path)
    confirmation_targets = _read_optional_json(
        confirmation_plan_path.parent / "confirmation_targets.json"
        if confirmation_plan_path is not None
        else None
    )
    trigger_conditions = _read_optional_json(
        confirmation_plan_path.parent / "trigger_conditions.json"
        if confirmation_plan_path is not None
        else None
    )
    failure_conditions = _read_optional_json(
        confirmation_plan_path.parent / "failure_conditions.json"
        if confirmation_plan_path is not None
        else None
    )
    confirmation_registry = _read_optional_json(confirmation_registry_path)
    confirmation_progress = _read_optional_json(confirmation_progress_path)
    progress_summary = _read_optional_json(
        confirmation_progress_path.parent / "target_progress_summary.json"
        if confirmation_progress_path is not None
        else None
    )
    confirmation_evaluation = _read_optional_json(confirmation_evaluation_path)
    evaluation_summary = _read_optional_json(
        confirmation_evaluation_path.parent / "confirmation_evaluation_summary.json"
        if confirmation_evaluation_path is not None
        else None
    )
    rule_review_cycle = _read_optional_json(rule_review_cycle_path)
    rule_review_matrix = _read_optional_json(
        rule_review_cycle_path.parent / "rule_review_decision_matrix.json"
        if rule_review_cycle_path is not None
        else None
    )
    confirmation_weekly = _read_optional_json(confirmation_weekly_path)
    confirmation_weekly_summary = _read_optional_json(
        confirmation_weekly_path.parent / "weekly_cycle_summary.json"
        if confirmation_weekly_path is not None
        else None
    )
    pressure_regime = _read_optional_json(pressure_regime_path)
    pressure_regime_summary = _read_optional_json(
        pressure_regime_path.parent / "pressure_regime_summary.json"
        if pressure_regime_path is not None
        else None
    )
    confirmation_dashboard = _read_optional_json(confirmation_dashboard_path)
    confirmation_dashboard_summary = _read_optional_json(
        confirmation_dashboard_path.parent / "confirmation_dashboard_summary.json"
        if confirmation_dashboard_path is not None
        else None
    )
    confirmation_dashboard_targets = _read_optional_json(
        confirmation_dashboard_path.parent / "target_status_table.json"
        if confirmation_dashboard_path is not None
        else None
    )
    rule_review_queue = _read_optional_json(rule_review_queue_path)
    rule_review_queue_summary = _read_optional_json(
        rule_review_queue_path.parent / "queue_summary.json"
        if rule_review_queue_path is not None
        else None
    )
    pressure_tag_diagnosis = _read_optional_json(pressure_tag_diagnosis_path)
    pressure_tag_diagnosis_distribution = _read_optional_json(
        pressure_tag_diagnosis_path.parent / "threshold_hit_distribution.json"
        if pressure_tag_diagnosis_path is not None
        else None
    )
    pressure_tag_diagnosis_mapping = _read_optional_json(
        pressure_tag_diagnosis_path.parent / "outcome_mapping_diagnostics.json"
        if pressure_tag_diagnosis_path is not None
        else None
    )
    pressure_outcome_backfill = _read_optional_json(pressure_outcome_backfill_path)
    pressure_outcome_backfill_summary = _read_optional_json(
        pressure_outcome_backfill_path.parent / "pressure_source_summary.json"
        if pressure_outcome_backfill_path is not None
        else None
    )
    defensive_pressure_compare = _read_optional_json(defensive_pressure_compare_path)
    defensive_pressure_summary = _read_optional_json(
        defensive_pressure_compare_path.parent / "defensive_pressure_summary.json"
        if defensive_pressure_compare_path is not None
        else None
    )
    defensive_rule_review = _read_optional_json(defensive_rule_review_path)
    defensive_rule_matrix = _read_optional_json(
        defensive_rule_review_path.parent / "defensive_rule_decision_matrix.json"
        if defensive_rule_review_path is not None
        else None
    )
    weekly_ops_decision_update = _read_optional_json(weekly_ops_decision_update_path)
    weekly_ops_decision_matrix = _read_optional_json(
        weekly_ops_decision_update_path.parent / "updated_weekly_decision_matrix.json"
        if weekly_ops_decision_update_path is not None
        else None
    )
    weekly_ops_next_actions = _read_optional_json(
        weekly_ops_decision_update_path.parent / "weekly_ops_next_actions.json"
        if weekly_ops_decision_update_path is not None
        else None
    )
    defensive_hypothesis_deep_dive = _read_optional_json(defensive_hypothesis_deep_dive_path)
    defensive_hypothesis_regime_matrix = _read_optional_json(
        defensive_hypothesis_deep_dive_path.parent / "regime_effect_matrix.json"
        if defensive_hypothesis_deep_dive_path is not None
        else None
    )
    defensive_label_review = _read_optional_json(defensive_label_review_path)
    defensive_label_matrix = _read_optional_json(
        defensive_label_review_path.parent / "label_decision_matrix.json"
        if defensive_label_review_path is not None
        else None
    )
    defensive_failure_study = _read_optional_json(defensive_failure_study_path)
    defensive_failure_summary = _read_optional_json(
        defensive_failure_study_path.parent / "failure_pattern_summary.json"
        if defensive_failure_study_path is not None
        else None
    )
    defensive_research_note = _read_optional_json(defensive_research_note_path)
    defensive_hypothesis_summary = _read_optional_json(
        defensive_research_note_path.parent / "defensive_hypothesis_summary.json"
        if defensive_research_note_path is not None
        else None
    )
    defensive_owner_pack = _read_optional_json(defensive_owner_pack_path)
    defensive_owner_options = _read_optional_json(
        defensive_owner_pack_path.parent / "owner_decision_options.json"
        if defensive_owner_pack_path is not None
        else None
    )
    forward_pressure_capture = _read_optional_json(forward_pressure_capture_path)
    forward_pressure_event_plan = _read_optional_json(
        forward_pressure_capture_path.parent / "event_driven_trigger_plan.json"
        if forward_pressure_capture_path is not None
        else None
    )
    pressure_trigger = _read_optional_json(pressure_trigger_path)
    pressure_trigger_metrics = _read_optional_json(
        pressure_trigger_path.parent / "trigger_metrics.json"
        if pressure_trigger_path is not None
        else None
    )
    pressure_trigger_actions = _read_optional_json(
        pressure_trigger_path.parent / "triggered_actions.json"
        if pressure_trigger_path is not None
        else None
    )
    pressure_capture = _read_optional_json(pressure_capture_path)
    pressure_capture_steps = _read_optional_json(
        pressure_capture_path.parent / "pressure_capture_steps.json"
        if pressure_capture_path is not None
        else None
    )
    pressure_sample_ledger = _read_optional_json(pressure_sample_ledger_path)
    pressure_sample_summary = _read_optional_json(
        pressure_sample_ledger_path.parent / "pressure_sample_summary.json"
        if pressure_sample_ledger_path is not None
        else None
    )
    weekly_defensive_evidence = _read_optional_json(weekly_defensive_evidence_path)
    weekly_defensive_summary = _read_optional_json(
        weekly_defensive_evidence_path.parent / "weekly_defensive_summary.json"
        if weekly_defensive_evidence_path is not None
        else None
    )
    owner_decisions = _read_optional_jsonl(rule_owner_decision_path)
    latest_owner_decision = owner_decisions[-1] if owner_decisions else {}
    source_payloads = tuple(
        _mapping(payload)
        for payload in (
            interpretation,
            interpretation_matrix,
            key_findings,
            risk_return,
            risk_summary,
            defensive_validation,
            defensive_summary,
            proposal_review,
            decision_matrix,
            confirmation_plan,
            confirmation_targets,
            trigger_conditions,
            failure_conditions,
            confirmation_registry,
            confirmation_progress,
            progress_summary,
            confirmation_evaluation,
            evaluation_summary,
            rule_review_cycle,
            rule_review_matrix,
            confirmation_weekly,
            confirmation_weekly_summary,
            pressure_regime,
            pressure_regime_summary,
            confirmation_dashboard,
            confirmation_dashboard_summary,
            confirmation_dashboard_targets,
            rule_review_queue,
            rule_review_queue_summary,
            pressure_tag_diagnosis,
            pressure_tag_diagnosis_distribution,
            pressure_tag_diagnosis_mapping,
            pressure_outcome_backfill,
            pressure_outcome_backfill_summary,
            defensive_pressure_compare,
            defensive_pressure_summary,
            defensive_rule_review,
            defensive_rule_matrix,
            weekly_ops_decision_update,
            weekly_ops_decision_matrix,
            weekly_ops_next_actions,
            defensive_hypothesis_deep_dive,
            defensive_hypothesis_regime_matrix,
            defensive_label_review,
            defensive_label_matrix,
            defensive_failure_study,
            defensive_failure_summary,
            defensive_research_note,
            defensive_hypothesis_summary,
            defensive_owner_pack,
            defensive_owner_options,
            forward_pressure_capture,
            forward_pressure_event_plan,
            pressure_trigger,
            pressure_trigger_metrics,
            pressure_trigger_actions,
            pressure_capture,
            pressure_capture_steps,
            pressure_sample_ledger,
            pressure_sample_summary,
            weekly_defensive_evidence,
            weekly_defensive_summary,
            latest_owner_decision,
        )
    )
    if not any(source_payloads):
        return _missing_etf_dynamic_v3_sim_review_summary()
    required_manifests = (
        interpretation,
        risk_return,
        defensive_validation,
        proposal_review,
        confirmation_plan,
    )
    availability = "AVAILABLE" if all(required_manifests) else "PARTIAL"
    variant_roles = ", ".join(
        f"{row.get('variant')}={row.get('role')}"
        for row in _records(_mapping(interpretation_matrix).get("variants"))
    )
    finding_ids = ", ".join(
        _text(row.get("finding_id")) for row in _records(_mapping(key_findings).get("findings"))
    )
    risk_rows = _records(_mapping(risk_summary).get("summary"))
    risk_statuses = ", ".join(
        f"{row.get('variant')}={row.get('risk_return_status')}" for row in risk_rows
    )
    proposal_rows = _records(_mapping(decision_matrix).get("proposals"))
    proposal_decisions = ", ".join(
        f"{row.get('proposal_id')}={row.get('decision')}" for row in proposal_rows
    )
    target_rows = _records(_mapping(confirmation_targets).get("targets"))
    target_ids = ", ".join(_text(row.get("target_id")) for row in target_rows)
    ready_conditions = ", ".join(
        _text(row.get("condition"))
        for row in _records(_mapping(trigger_conditions).get("calibration_ready_conditions"))
    )
    failure_condition_list = ", ".join(
        f"{row.get('target')}={row.get('condition')}"
        for row in _records(_mapping(failure_conditions).get("failure_conditions"))
    )
    dashboard_targets = {
        _text(row.get("target_id")): row
        for row in _records(_mapping(confirmation_dashboard_targets).get("targets"))
    }
    limited_target = _mapping(dashboard_targets.get("limited_adjustment_vs_no_trade"))
    defensive_target = _mapping(dashboard_targets.get("defensive_limited_adjustment_drawdown"))
    consensus_target = _mapping(dashboard_targets.get("consensus_target_risk"))
    pressure_samples = _mapping(_mapping(pressure_regime_summary).get("pressure_samples"))
    pressure_diagnosis_summary = _mapping(_mapping(pressure_tag_diagnosis).get("diagnosis_summary"))
    pressure_near_counts = _mapping(
        _mapping(pressure_tag_diagnosis_distribution).get("near_miss_counts")
    )
    pressure_backfill_by_source = _mapping(
        _mapping(pressure_outcome_backfill_summary).get("by_source_mode")
    )
    weekly_next_action_names = ", ".join(
        _text(row.get("action"))
        for row in _records(_mapping(weekly_ops_next_actions).get("next_actions"))
    )
    failure_patterns = _records(_mapping(defensive_failure_summary).get("patterns"))
    top_failure_pattern = next(
        (_text(row.get("pattern")) for row in failure_patterns if _int(row.get("count")) > 0),
        "MISSING",
    )
    owner_recommended_decisions = {
        _text(row.get("decision"))
        for row in _records(_mapping(defensive_owner_options).get("options"))
        if row.get("recommended") is True
    }
    pressure_trigger_status = _text(
        _mapping(pressure_trigger_metrics).get("trigger_status"),
        "MISSING",
    )
    pressure_capture_required = (
        _mapping(pressure_trigger_actions).get("event_driven_capture_required") is True
    )
    auto_apply = any(
        payload.get("auto_apply") is True or payload.get("auto_policy_apply") is True
        for payload in source_payloads
    ) or any(row.get("auto_apply") is True for row in proposal_rows)
    owner_approval_required = (
        True
        if not proposal_rows
        else any(row.get("owner_approval_required") is True for row in proposal_rows)
    )
    position_advisory_config_mutated = (
        _mapping(decision_matrix).get("position_advisory_config_mutated") is True
    )
    safety_ok = all(_etf_dynamic_v3_sim_review_payload_safe(payload) for payload in source_payloads)
    safety_ok = safety_ok and not auto_apply and not position_advisory_config_mutated
    status = _text(
        _mapping(confirmation_plan).get("status")
        or _mapping(proposal_review).get("status")
        or _mapping(defensive_validation).get("status")
        or _mapping(risk_return).get("status")
        or _mapping(interpretation).get("status")
        or _mapping(weekly_defensive_evidence).get("status")
        or _mapping(pressure_sample_ledger).get("status")
        or _mapping(pressure_capture).get("status")
        or _mapping(pressure_trigger).get("status")
        or _mapping(defensive_research_note).get("status")
        or _mapping(defensive_hypothesis_deep_dive).get("status"),
        "MISSING",
    )
    defensive_status = _text(
        _mapping(defensive_summary).get("defensive_limited_adjustment_status"),
        "MISSING",
    )
    report_label = _text(
        _mapping(confirmation_plan).get("report_label")
        or _mapping(proposal_review).get("report_label")
        or _mapping(interpretation).get("report_label"),
        "MISSING",
    )
    outcome_mode = _text(
        _mapping(confirmation_plan).get("outcome_mode")
        or _mapping(proposal_review).get("outcome_mode")
        or _mapping(interpretation).get("outcome_mode"),
        "MISSING",
    )
    pit_safety_status = _text(
        _mapping(confirmation_plan).get("pit_safety_status")
        or _mapping(proposal_review).get("pit_safety_status")
        or _mapping(interpretation).get("pit_safety_status"),
        "MISSING",
    )
    return {
        "availability": availability,
        "status": status,
        "summary_sentence": (
            "Dynamic Rescue Simulation Advisory Review: "
            "interpretation="
            f"{_text(_mapping(interpretation).get('interpretation_id'), 'MISSING')}; "
            f"risk_return={_text(_mapping(risk_return).get('risk_return_id'), 'MISSING')}; "
            "defensive_validation="
            f"{_text(_mapping(defensive_validation).get('defensive_validation_id'), 'MISSING')}; "
            "proposal_review="
            f"{_text(_mapping(proposal_review).get('proposal_review_id'), 'MISSING')}; "
            "confirmation_plan="
            f"{_text(_mapping(confirmation_plan).get('confirmation_plan_id'), 'MISSING')}; "
            "rule_review_cycle="
            f"{_text(_mapping(rule_review_cycle).get('cycle_id'), 'MISSING')}; "
            "weekly_cycle="
            f"{_text(_mapping(confirmation_weekly).get('weekly_cycle_id'), 'MISSING')}; "
            "defensive_hypothesis="
            f"{_text(_mapping(defensive_hypothesis_summary).get('current_status'), 'MISSING')}; "
            f"pressure_trigger={pressure_trigger_status}; "
            "weekly_defensive="
            f"{_text(_mapping(weekly_defensive_summary).get('defensive_rule_status'), 'MISSING')}; "
            f"defensive_status={defensive_status}; auto_apply={auto_apply}; "
            "production_effect=none."
        ),
        "interpretation_id": _text(_mapping(interpretation).get("interpretation_id"), "MISSING"),
        "risk_return_id": _text(_mapping(risk_return).get("risk_return_id"), "MISSING"),
        "defensive_validation_id": _text(
            _mapping(defensive_validation).get("defensive_validation_id"),
            "MISSING",
        ),
        "proposal_review_id": _text(
            _mapping(proposal_review).get("proposal_review_id"),
            "MISSING",
        ),
        "confirmation_plan_id": _text(
            _mapping(confirmation_plan).get("confirmation_plan_id"),
            "MISSING",
        ),
        "outcome_id": _text(_mapping(interpretation).get("outcome_id"), "MISSING"),
        "calibration_id": _text(_mapping(interpretation).get("calibration_id"), "MISSING"),
        "bridge_id": _text(_mapping(confirmation_plan).get("bridge_id"), "MISSING"),
        "variant_roles": variant_roles or "MISSING",
        "key_findings": finding_ids or "MISSING",
        "risk_return_statuses": risk_statuses or "MISSING",
        "defensive_status": defensive_status,
        "defensive_recommendation": _text(
            _mapping(defensive_summary).get("recommendation"),
            "MISSING",
        ),
        "proposal_decisions": proposal_decisions or "MISSING",
        "confirmation_targets": target_ids or "MISSING",
        "calibration_ready_conditions": ready_conditions or "MISSING",
        "failure_conditions": failure_condition_list or "MISSING",
        "confirmation_registry_id": _text(
            _mapping(confirmation_registry).get("registry_id"),
            "MISSING",
        ),
        "confirmation_progress_id": _text(
            _mapping(confirmation_progress).get("progress_id"),
            "MISSING",
        ),
        "confirmation_evaluation_id": _text(
            _mapping(confirmation_evaluation).get("evaluation_id"),
            "MISSING",
        ),
        "rule_review_cycle_id": _text(
            _mapping(rule_review_cycle).get("cycle_id"),
            "MISSING",
        ),
        "rule_review_cycle_recommendation": _text(
            _mapping(rule_review_cycle).get("cycle_recommendation"),
            "MISSING",
        ),
        "confirmation_weekly_cycle_id": _text(
            _mapping(confirmation_weekly).get("weekly_cycle_id"),
            "MISSING",
        ),
        "confirmation_weekly_due_windows": _int(
            _mapping(confirmation_weekly_summary).get("due_windows")
        ),
        "confirmation_weekly_updated_windows": _int(
            _mapping(confirmation_weekly_summary).get("updated_windows")
        ),
        "confirmation_weekly_rule_review_recommendation": _text(
            _mapping(confirmation_weekly_summary).get("rule_review_recommendation"),
            "MISSING",
        ),
        "pressure_regime_tag_id": _text(
            _mapping(pressure_regime).get("tag_id"),
            "MISSING",
        ),
        "pressure_tech_drawdown_count": _int(pressure_samples.get("tech_drawdown")),
        "pressure_risk_off_count": _int(pressure_samples.get("risk_off")),
        "pressure_semiconductor_pullback_count": _int(
            pressure_samples.get("semiconductor_pullback")
        ),
        "pressure_defensive_validation_relevant_outcomes": _int(
            _mapping(pressure_regime_summary).get("defensive_validation_relevant_outcomes")
        ),
        "pressure_diagnosis_id": _text(
            _mapping(pressure_tag_diagnosis).get("diagnosis_id"),
            "MISSING",
        ),
        "pressure_diagnosis_reason": _text(
            pressure_diagnosis_summary.get("primary_reason"),
            "MISSING",
        ),
        "pressure_near_miss_window_count": sum(
            _int(value) for value in pressure_near_counts.values()
        ),
        "pressure_mapping_failure_count": len(
            _records(_mapping(pressure_tag_diagnosis_mapping).get("mapping_failures"))
        ),
        "pressure_backfill_id": _text(
            _mapping(pressure_outcome_backfill).get("pressure_backfill_id"),
            "MISSING",
        ),
        "pressure_backfill_total": _int(
            _mapping(pressure_outcome_backfill_summary).get("total_pressure_outcomes")
        ),
        "pressure_backfill_forward_count": _int(pressure_backfill_by_source.get("FORWARD_OUTCOME")),
        "pressure_backfill_replay_count": _int(
            pressure_backfill_by_source.get("HISTORICAL_REPLAY")
        ),
        "pressure_backfill_simulation_count": _int(
            pressure_backfill_by_source.get("BACKTEST_SIMULATION")
        ),
        "pressure_backfill_relevant_count": _int(
            _mapping(pressure_outcome_backfill_summary).get("defensive_validation_relevant_count")
        ),
        "defensive_pressure_comparison_id": _text(
            _mapping(defensive_pressure_compare).get("comparison_id"),
            "MISSING",
        ),
        "defensive_pressure_status": _text(
            _mapping(defensive_pressure_summary).get("defensive_status"),
            "MISSING",
        ),
        "defensive_pressure_can_support_rule_approval": (
            _mapping(defensive_pressure_summary).get("can_support_rule_approval") is True
        ),
        "defensive_rule_review_id": _text(
            _mapping(defensive_rule_review).get("review_id"),
            "MISSING",
        ),
        "defensive_rule_recommended_status": _text(
            _mapping(defensive_rule_matrix).get("recommended_status"),
            "MISSING",
        ),
        "defensive_rule_approval_allowed": (
            _mapping(defensive_rule_matrix).get("rule_approval_allowed") is True
        ),
        "weekly_ops_decision_update_id": _text(
            _mapping(weekly_ops_decision_update).get("decision_update_id"),
            "MISSING",
        ),
        "weekly_ops_recommendation": _text(
            _mapping(weekly_ops_decision_matrix).get("weekly_recommendation"),
            "MISSING",
        ),
        "weekly_ops_policy_change_allowed": (
            _mapping(weekly_ops_decision_matrix).get("policy_change_allowed") is True
        ),
        "weekly_ops_broker_action_allowed": (
            _mapping(weekly_ops_decision_matrix).get("broker_action_allowed") is True
        ),
        "weekly_ops_next_actions": weekly_next_action_names or "MISSING",
        "defensive_hypothesis_deep_dive_id": _text(
            _mapping(defensive_hypothesis_deep_dive).get("deep_dive_id"),
            "MISSING",
        ),
        "defensive_hypothesis_supporting_count": _int(
            _mapping(defensive_hypothesis_deep_dive).get("supporting_case_count")
        ),
        "defensive_hypothesis_contradicting_count": _int(
            _mapping(defensive_hypothesis_deep_dive).get("contradicting_case_count")
        ),
        "defensive_hypothesis_can_support_rule_approval": (
            _mapping(defensive_hypothesis_deep_dive).get("can_support_rule_approval") is True
        ),
        "defensive_label_review_id": _text(
            _mapping(defensive_label_review).get("label_review_id"),
            "MISSING",
        ),
        "defensive_label_status": _text(
            _mapping(defensive_label_matrix).get("label_status"),
            "MISSING",
        ),
        "defensive_recommended_label": _text(
            _mapping(defensive_label_matrix).get("recommended_label"),
            "MISSING",
        ),
        "defensive_label_auto_rename": (
            _mapping(defensive_label_matrix).get("auto_rename") is True
        ),
        "defensive_failure_study_id": _text(
            _mapping(defensive_failure_study).get("failure_study_id"),
            "MISSING",
        ),
        "defensive_failure_case_count": _int(
            _mapping(defensive_failure_study).get("failure_case_count")
        ),
        "defensive_failure_top_pattern": top_failure_pattern,
        "defensive_research_note_id": _text(
            _mapping(defensive_research_note).get("note_id"),
            "MISSING",
        ),
        "defensive_hypothesis_current_status": _text(
            _mapping(defensive_hypothesis_summary).get("current_status"),
            "MISSING",
        ),
        "defensive_hypothesis_forward_support": _text(
            _mapping(defensive_hypothesis_summary).get("forward_support"),
            "MISSING",
        ),
        "defensive_owner_pack_id": _text(
            _mapping(defensive_owner_pack).get("pack_id"),
            "MISSING",
        ),
        "defensive_owner_continue_tracking_recommended": (
            "continue_tracking" in owner_recommended_decisions
        ),
        "forward_pressure_capture_plan_id": _text(
            _mapping(forward_pressure_capture).get("capture_plan_id"),
            "MISSING",
        ),
        "forward_pressure_event_trigger_count": len(
            _mapping(forward_pressure_event_plan).get("triggers", {})
        ),
        "pressure_trigger_id": _text(
            _mapping(pressure_trigger).get("trigger_id"),
            "MISSING",
        ),
        "pressure_trigger_status": pressure_trigger_status,
        "pressure_capture_required": pressure_capture_required,
        "pressure_capture_id": _text(
            _mapping(pressure_capture).get("capture_id"),
            "MISSING",
        ),
        "pressure_capture_status": _text(
            _mapping(pressure_capture).get("status"),
            "MISSING",
        ),
        "pressure_capture_manual_force": (
            _mapping(pressure_capture_steps).get("manual_force") is True
        ),
        "pressure_sample_ledger_id": _text(
            _mapping(pressure_sample_ledger).get("ledger_id"),
            "MISSING",
        ),
        "pressure_forward_samples": _int(_mapping(pressure_sample_summary).get("forward_samples")),
        "pressure_simulation_samples": _int(
            _mapping(pressure_sample_summary).get("simulation_samples")
        ),
        "pressure_progress_to_requirement": _mapping(pressure_sample_summary).get(
            "progress_to_requirement",
            "MISSING",
        ),
        "weekly_defensive_id": _text(
            _mapping(weekly_defensive_evidence).get("weekly_defensive_id"),
            "MISSING",
        ),
        "weekly_defensive_rule_status": _text(
            _mapping(weekly_defensive_summary).get("defensive_rule_status"),
            "MISSING",
        ),
        "weekly_defensive_recommendation": _text(
            _mapping(weekly_defensive_summary).get("weekly_recommendation"),
            "MISSING",
        ),
        "weekly_defensive_new_simulation_samples": _int(
            _mapping(weekly_defensive_summary).get("new_simulation_pressure_samples")
        ),
        "confirmation_dashboard_id": _text(
            _mapping(confirmation_dashboard).get("dashboard_id"),
            "MISSING",
        ),
        "dashboard_ready_for_evaluation": _int(
            _mapping(confirmation_dashboard_summary).get("ready_for_evaluation")
        ),
        "dashboard_limited_adjustment_progress": limited_target.get("progress_pct", "MISSING"),
        "dashboard_defensive_pressure_progress": defensive_target.get("progress_pct", "MISSING"),
        "dashboard_consensus_target_status": _text(
            consensus_target.get("status"),
            "MISSING",
        ),
        "rule_review_queue_id": _text(
            _mapping(rule_review_queue).get("queue_id"),
            "MISSING",
        ),
        "rule_review_queue_pending_count": _int(
            _mapping(rule_review_queue_summary).get("pending_count")
        ),
        "rule_review_queue_ready_count": _int(
            _mapping(rule_review_queue_summary).get("ready_for_owner_review_count")
        ),
        "rule_review_queue_not_ready_count": _int(
            _mapping(rule_review_queue_summary).get("not_ready_count")
        ),
        "confirmation_ready_for_evaluation_count": _int(
            _mapping(progress_summary).get("ready_for_evaluation_count")
        ),
        "confirmation_insufficient_events_count": _int(
            _mapping(progress_summary).get("insufficient_events_count")
        ),
        "confirmation_success_count": _int(_mapping(evaluation_summary).get("success_count")),
        "confirmation_failure_count": _int(_mapping(evaluation_summary).get("failure_count")),
        "confirmation_not_ready_count": _int(_mapping(evaluation_summary).get("not_ready_count")),
        "rule_review_policy_change_allowed": (
            _mapping(rule_review_matrix).get("policy_change_allowed") is True
        ),
        "rule_owner_decision_id": _text(
            _mapping(latest_owner_decision).get("decision_id"),
            "MISSING",
        ),
        "rule_owner_decision": _text(
            _mapping(latest_owner_decision).get("owner_decision"),
            "MISSING",
        ),
        "auto_apply": auto_apply,
        "owner_approval_required": owner_approval_required,
        "position_advisory_config_mutated": position_advisory_config_mutated,
        "report_label": report_label,
        "outcome_mode": outcome_mode,
        "pit_safety_status": pit_safety_status,
        "safety_status": (
            "BACKTEST_SIMULATION_NOT_PIT; production_effect=none; broker_action=none; "
            "auto_policy_apply=false; position_advisory_config_mutated=false"
            if safety_ok
            else "SAFETY_REVIEW_REQUIRED"
        ),
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "sim_interpretation_path": _text(interpretation_path),
        "sim_risk_return_path": _text(risk_return_path),
        "sim_defensive_validation_path": _text(defensive_validation_path),
        "advisory_proposal_review_path": _text(proposal_review_path),
        "forward_confirmation_plan_path": _text(confirmation_plan_path),
        "confirmation_registry_path": _text(confirmation_registry_path),
        "confirmation_progress_path": _text(confirmation_progress_path),
        "confirmation_evaluation_path": _text(confirmation_evaluation_path),
        "rule_review_cycle_path": _text(rule_review_cycle_path),
        "confirmation_weekly_path": _text(confirmation_weekly_path),
        "pressure_regime_tag_path": _text(pressure_regime_path),
        "confirmation_dashboard_path": _text(confirmation_dashboard_path),
        "rule_review_queue_path": _text(rule_review_queue_path),
        "pressure_tag_diagnosis_path": _text(pressure_tag_diagnosis_path),
        "pressure_outcome_backfill_path": _text(pressure_outcome_backfill_path),
        "defensive_pressure_compare_path": _text(defensive_pressure_compare_path),
        "defensive_rule_review_path": _text(defensive_rule_review_path),
        "weekly_ops_decision_update_path": _text(weekly_ops_decision_update_path),
        "defensive_hypothesis_deep_dive_path": _text(defensive_hypothesis_deep_dive_path),
        "defensive_label_review_path": _text(defensive_label_review_path),
        "defensive_failure_study_path": _text(defensive_failure_study_path),
        "defensive_research_note_path": _text(defensive_research_note_path),
        "defensive_owner_pack_path": _text(defensive_owner_pack_path),
        "forward_pressure_capture_path": _text(forward_pressure_capture_path),
        "pressure_trigger_path": _text(pressure_trigger_path),
        "pressure_capture_path": _text(pressure_capture_path),
        "pressure_sample_ledger_path": _text(pressure_sample_ledger_path),
        "weekly_defensive_evidence_path": _text(weekly_defensive_evidence_path),
        "rule_owner_decision_path": _text(rule_owner_decision_path),
        "detail_report": _text(
            weekly_defensive_evidence_path
            or pressure_sample_ledger_path
            or pressure_capture_path
            or pressure_trigger_path
            or defensive_research_note_path
            or defensive_hypothesis_deep_dive_path
            or forward_pressure_capture_path
            or confirmation_dashboard_path
            or confirmation_weekly_path
            or rule_review_queue_path
            or pressure_regime_path
            or rule_review_cycle_path
            or confirmation_evaluation_path
            or confirmation_progress_path
            or confirmation_plan_path
            or proposal_review_path
            or interpretation_path
        ),
    }


def _missing_etf_dynamic_v3_sim_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Dynamic Rescue Simulation Advisory Review: no latest interpretation, "
            "risk-return, defensive validation, proposal review, or confirmation plan found."
        ),
        "interpretation_id": "MISSING",
        "risk_return_id": "MISSING",
        "defensive_validation_id": "MISSING",
        "proposal_review_id": "MISSING",
        "confirmation_plan_id": "MISSING",
        "outcome_id": "MISSING",
        "calibration_id": "MISSING",
        "bridge_id": "MISSING",
        "variant_roles": "MISSING",
        "key_findings": "MISSING",
        "risk_return_statuses": "MISSING",
        "defensive_status": "MISSING",
        "defensive_recommendation": "MISSING",
        "proposal_decisions": "MISSING",
        "confirmation_targets": "MISSING",
        "calibration_ready_conditions": "MISSING",
        "failure_conditions": "MISSING",
        "confirmation_registry_id": "MISSING",
        "confirmation_progress_id": "MISSING",
        "confirmation_evaluation_id": "MISSING",
        "rule_review_cycle_id": "MISSING",
        "rule_review_cycle_recommendation": "MISSING",
        "confirmation_weekly_cycle_id": "MISSING",
        "confirmation_weekly_due_windows": 0,
        "confirmation_weekly_updated_windows": 0,
        "confirmation_weekly_rule_review_recommendation": "MISSING",
        "pressure_regime_tag_id": "MISSING",
        "pressure_tech_drawdown_count": 0,
        "pressure_risk_off_count": 0,
        "pressure_semiconductor_pullback_count": 0,
        "pressure_defensive_validation_relevant_outcomes": 0,
        "pressure_diagnosis_id": "MISSING",
        "pressure_diagnosis_reason": "MISSING",
        "pressure_near_miss_window_count": 0,
        "pressure_mapping_failure_count": 0,
        "pressure_backfill_id": "MISSING",
        "pressure_backfill_total": 0,
        "pressure_backfill_forward_count": 0,
        "pressure_backfill_replay_count": 0,
        "pressure_backfill_simulation_count": 0,
        "pressure_backfill_relevant_count": 0,
        "defensive_pressure_comparison_id": "MISSING",
        "defensive_pressure_status": "MISSING",
        "defensive_pressure_can_support_rule_approval": False,
        "defensive_rule_review_id": "MISSING",
        "defensive_rule_recommended_status": "MISSING",
        "defensive_rule_approval_allowed": False,
        "weekly_ops_decision_update_id": "MISSING",
        "weekly_ops_recommendation": "MISSING",
        "weekly_ops_policy_change_allowed": False,
        "weekly_ops_broker_action_allowed": False,
        "weekly_ops_next_actions": "MISSING",
        "defensive_hypothesis_deep_dive_id": "MISSING",
        "defensive_hypothesis_supporting_count": 0,
        "defensive_hypothesis_contradicting_count": 0,
        "defensive_hypothesis_can_support_rule_approval": False,
        "defensive_label_review_id": "MISSING",
        "defensive_label_status": "MISSING",
        "defensive_recommended_label": "MISSING",
        "defensive_label_auto_rename": False,
        "defensive_failure_study_id": "MISSING",
        "defensive_failure_case_count": 0,
        "defensive_failure_top_pattern": "MISSING",
        "defensive_research_note_id": "MISSING",
        "defensive_hypothesis_current_status": "MISSING",
        "defensive_hypothesis_forward_support": "MISSING",
        "defensive_owner_pack_id": "MISSING",
        "defensive_owner_continue_tracking_recommended": False,
        "forward_pressure_capture_plan_id": "MISSING",
        "forward_pressure_event_trigger_count": 0,
        "pressure_trigger_id": "MISSING",
        "pressure_trigger_status": "MISSING",
        "pressure_capture_required": False,
        "pressure_capture_id": "MISSING",
        "pressure_capture_status": "MISSING",
        "pressure_capture_manual_force": False,
        "pressure_sample_ledger_id": "MISSING",
        "pressure_forward_samples": 0,
        "pressure_simulation_samples": 0,
        "pressure_progress_to_requirement": "MISSING",
        "weekly_defensive_id": "MISSING",
        "weekly_defensive_rule_status": "MISSING",
        "weekly_defensive_recommendation": "MISSING",
        "weekly_defensive_new_simulation_samples": 0,
        "confirmation_dashboard_id": "MISSING",
        "dashboard_ready_for_evaluation": 0,
        "dashboard_limited_adjustment_progress": "MISSING",
        "dashboard_defensive_pressure_progress": "MISSING",
        "dashboard_consensus_target_status": "MISSING",
        "rule_review_queue_id": "MISSING",
        "rule_review_queue_pending_count": 0,
        "rule_review_queue_ready_count": 0,
        "rule_review_queue_not_ready_count": 0,
        "confirmation_ready_for_evaluation_count": 0,
        "confirmation_insufficient_events_count": 0,
        "confirmation_success_count": 0,
        "confirmation_failure_count": 0,
        "confirmation_not_ready_count": 0,
        "rule_review_policy_change_allowed": False,
        "rule_owner_decision_id": "MISSING",
        "rule_owner_decision": "MISSING",
        "auto_apply": False,
        "owner_approval_required": True,
        "position_advisory_config_mutated": False,
        "report_label": "MISSING",
        "outcome_mode": "MISSING",
        "pit_safety_status": "MISSING",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "sim_interpretation_path": "",
        "sim_risk_return_path": "",
        "sim_defensive_validation_path": "",
        "advisory_proposal_review_path": "",
        "forward_confirmation_plan_path": "",
        "confirmation_registry_path": "",
        "confirmation_progress_path": "",
        "confirmation_evaluation_path": "",
        "rule_review_cycle_path": "",
        "confirmation_weekly_path": "",
        "pressure_regime_tag_path": "",
        "confirmation_dashboard_path": "",
        "rule_review_queue_path": "",
        "pressure_tag_diagnosis_path": "",
        "pressure_outcome_backfill_path": "",
        "defensive_pressure_compare_path": "",
        "defensive_rule_review_path": "",
        "weekly_ops_decision_update_path": "",
        "defensive_hypothesis_deep_dive_path": "",
        "defensive_label_review_path": "",
        "defensive_failure_study_path": "",
        "defensive_research_note_path": "",
        "defensive_owner_pack_path": "",
        "forward_pressure_capture_path": "",
        "pressure_trigger_path": "",
        "pressure_capture_path": "",
        "pressure_sample_ledger_path": "",
        "weekly_defensive_evidence_path": "",
        "rule_owner_decision_path": "",
        "detail_report": "",
        "limitation": (
            "Reader Brief does not run simulation interpretation or proposal review CLIs; "
            "it only reads latest report registry artifacts."
        ),
    }


def _etf_dynamic_v3_sim_review_payload_safe(payload: Mapping[str, Any]) -> bool:
    if not payload:
        return True
    return (
        _etf_dynamic_v3_extra_payload_safe(payload)
        and payload.get("broker_action_allowed") is not True
        and payload.get("broker_action_taken") is not True
        and payload.get("auto_policy_apply") is not True
        and payload.get("auto_apply") is not True
        and payload.get("position_advisory_config_mutated") is not True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
    )


def _etf_dynamic_v3_manual_execution_review_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_manual_execution_review_summary()

    snapshot_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_manual_portfolio_snapshot",
        ),
        "manual_portfolio_manifest.json",
    )
    exposure_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_portfolio_exposure",
        ),
        "portfolio_exposure_manifest.json",
    )
    drift_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_position_drift",
        ),
        "position_drift_manifest.json",
    )
    guardrail_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_execution_guardrails",
        ),
        "guardrail_manifest.json",
    )
    review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_manual_execution_review",
        ),
        "manual_execution_review_manifest.json",
    )

    review_manifest = _read_optional_json(review_path)
    if not review_manifest:
        return _missing_etf_dynamic_v3_manual_execution_review_summary()

    snapshot_manifest = _read_optional_json(snapshot_path)
    exposure_manifest = _read_optional_json(exposure_path)
    drift_manifest = _read_optional_json(drift_path)
    guardrail_manifest = _read_optional_json(guardrail_path)
    exposure_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(exposure_path, "exposure_summary.json")
    )
    drift_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(drift_path, "consensus_drift_summary.json")
    )
    guardrail_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(guardrail_path, "guardrail_summary.json")
    )
    decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(review_path, "manual_execution_decision.json")
    )

    recommended_action = _text(
        decision.get("recommended_action"),
        _text(review_manifest.get("recommended_action"), "MISSING"),
    )
    manual_review_id = _text(review_manifest.get("manual_review_id"), "MISSING")
    snapshot_status = _text(snapshot_manifest.get("status"), "MISSING")
    exposure_status = _text(exposure_manifest.get("status"), "MISSING")
    drift_status = _text(
        drift_summary.get("drift_status"),
        _text(drift_manifest.get("status"), "MISSING"),
    )
    candidate_agreement = _text(
        drift_summary.get("candidate_agreement_status"),
        "MISSING",
    )
    guardrail_status = _text(guardrail_manifest.get("status"), "MISSING")
    owner_approval_required = (
        decision.get("owner_approval_required")
        if "owner_approval_required" in decision
        else review_manifest.get("owner_approval_required")
    )
    broker_action_allowed = (
        decision.get("broker_action_allowed")
        if "broker_action_allowed" in decision
        else review_manifest.get("broker_action_allowed")
    )
    broker_action_taken = (
        decision.get("broker_action_taken")
        if "broker_action_taken" in decision
        else review_manifest.get("broker_action_taken")
    )
    order_ticket_generated = (
        decision.get("order_ticket_generated")
        if "order_ticket_generated" in decision
        else review_manifest.get("order_ticket_generated")
    )
    production_effect = _text(
        decision.get("production_effect"),
        _text(review_manifest.get("production_effect"), PRODUCTION_EFFECT),
    )
    safety_status = _etf_dynamic_v3_manual_execution_review_safety_status(
        snapshot_manifest,
        exposure_manifest,
        exposure_summary,
        drift_manifest,
        drift_summary,
        guardrail_manifest,
        guardrail_summary,
        review_manifest,
        decision,
    )

    return {
        "availability": "AVAILABLE",
        "status": _text(review_manifest.get("status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic Rescue Manual Execution Review: "
            f"review={manual_review_id}; snapshot={snapshot_status}; "
            f"exposure={exposure_status}; drift={drift_status}; "
            f"agreement={candidate_agreement}; guardrail={guardrail_status}; "
            f"action={recommended_action}; "
            f"broker_action_allowed={str(broker_action_allowed).lower()}; "
            f"order_ticket_generated={str(order_ticket_generated).lower()}; "
            f"production_effect={production_effect}."
        ),
        "manual_review_id": manual_review_id,
        "snapshot_id": _text(review_manifest.get("snapshot_id"), "MISSING"),
        "exposure_id": _text(review_manifest.get("exposure_id"), "MISSING"),
        "drift_id": _text(review_manifest.get("drift_id"), "MISSING"),
        "guardrail_id": _text(review_manifest.get("guardrail_id"), "MISSING"),
        "snapshot_status": snapshot_status,
        "exposure_status": exposure_status,
        "drift_status": drift_status,
        "candidate_agreement_status": candidate_agreement,
        "total_abs_drift_to_consensus": drift_summary.get(
            "total_abs_drift_to_consensus",
            "MISSING",
        ),
        "guardrail_status": guardrail_status,
        "recommended_action": recommended_action,
        "capped_count": guardrail_summary.get("capped_count", 0),
        "blocked_count": guardrail_summary.get("blocked_count", 0),
        "owner_approval_required": owner_approval_required is True,
        "broker_action_allowed": broker_action_allowed is True,
        "broker_action_taken": broker_action_taken is True,
        "order_ticket_generated": order_ticket_generated is True,
        "production_effect": production_effect,
        "safety_status": safety_status,
        "max_single_symbol": _text(exposure_summary.get("max_single_symbol"), "MISSING"),
        "tech_weight": exposure_summary.get("tech_weight", "MISSING"),
        "semiconductor_weight": exposure_summary.get("semiconductor_weight", "MISSING"),
        "defensive_weight": exposure_summary.get("defensive_weight", "MISSING"),
        "manual_execution_review_path": "" if review_path is None else str(review_path),
        "guardrail_path": "" if guardrail_path is None else str(guardrail_path),
        "drift_path": "" if drift_path is None else str(drift_path),
        "exposure_path": "" if exposure_path is None else str(exposure_path),
        "snapshot_path": "" if snapshot_path is None else str(snapshot_path),
        "manual_review_required": review_manifest.get("manual_review_required") is True,
        "broker_action": _text(review_manifest.get("broker_action"), "none"),
    }


def _missing_etf_dynamic_v3_manual_execution_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Dynamic Rescue Manual Execution Review: no latest manual execution review pack found."
        ),
        "manual_review_id": "MISSING",
        "snapshot_id": "MISSING",
        "exposure_id": "MISSING",
        "drift_id": "MISSING",
        "guardrail_id": "MISSING",
        "snapshot_status": "MISSING",
        "exposure_status": "MISSING",
        "drift_status": "MISSING",
        "candidate_agreement_status": "MISSING",
        "total_abs_drift_to_consensus": "MISSING",
        "guardrail_status": "MISSING",
        "recommended_action": "MISSING",
        "capped_count": 0,
        "blocked_count": 0,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        "safety_status": "MISSING",
        "max_single_symbol": "MISSING",
        "tech_weight": "MISSING",
        "semiconductor_weight": "MISSING",
        "defensive_weight": "MISSING",
        "manual_execution_review_path": "",
        "guardrail_path": "",
        "drift_path": "",
        "exposure_path": "",
        "snapshot_path": "",
        "manual_review_required": True,
        "broker_action": "none",
        "limitation": (
            "Reader Brief only reads latest manual snapshot, exposure, drift, "
            "guardrail, and review artifacts; it does not run execution CLIs."
        ),
    }


def _etf_dynamic_v3_manual_execution_review_safety_status(
    *payloads: Mapping[str, Any],
) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    unsafe = any(
        payload.get("broker_action_allowed") is True
        or payload.get("broker_action_taken") is True
        or payload.get("order_ticket_generated") is True
        or payload.get("production_state_mutated") is True
        or payload.get("baseline_config_mutated") is True
        or payload.get("official_target_weights_mutated") is True
        or payload.get("automatic_candidate_promotion") is True
        or payload.get("auto_enrollment_without_owner_approval") is True
        or payload.get("owner_approval_executed") is True
        or _text(payload.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT
        for payload in material
    )
    review_payloads = [
        payload
        for payload in material
        if "owner_approval_required" in payload or "order_ticket_generated" in payload
    ]
    owner_review_required = all(
        payload.get("owner_approval_required") is True for payload in review_payloads
    )
    if not unsafe and owner_review_required:
        return (
            "observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action_allowed=false; broker_action_taken=false; "
            "order_ticket_generated=false; owner_approval_required=true"
        )
    return "SAFETY_REVIEW_REQUIRED"


def _etf_dynamic_v3_real_snapshot_review_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_real_snapshot_review_summary()
    weekly_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_weekly_real_snapshot_review",
        ),
        "weekly_real_snapshot_review_manifest.json",
    )
    weekly_manifest = _read_optional_json(weekly_path)
    if not weekly_manifest:
        return _missing_etf_dynamic_v3_real_snapshot_review_summary()
    weekly_summary_path = _dynamic_v3_sibling_artifact_path(
        weekly_path,
        "weekly_real_snapshot_summary.json",
    )
    owner_decision_summary_path = _dynamic_v3_sibling_artifact_path(
        weekly_path,
        "weekly_owner_decision_summary.json",
    )
    dry_run_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_real_snapshot_dry_run"),
        "real_snapshot_dry_run_manifest.json",
    )
    owner_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_real_execution_owner_review",
        ),
        "real_execution_owner_review_manifest.json",
    )
    paper_action_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_real_snapshot_paper_action",
        ),
        "real_snapshot_paper_action_manifest.json",
    )
    weekly_summary = _read_optional_json(weekly_summary_path)
    owner_decision_summary = _read_optional_json(owner_decision_summary_path)
    dry_run_manifest = _read_optional_json(dry_run_path)
    owner_review_manifest = _read_optional_json(owner_review_path)
    paper_action_manifest = _read_optional_json(paper_action_path)
    source_summary = weekly_summary or weekly_manifest
    weekly_real_review_id = _text(
        source_summary.get("weekly_real_review_id"),
        "MISSING",
    )
    snapshot_status = _text(source_summary.get("snapshot_status"), "MISSING")
    recommended_action = _text(source_summary.get("recommended_action"), "MISSING")
    owner_decision = _text(source_summary.get("owner_decision"), "pending")
    paper_action_taken = source_summary.get("paper_action_taken") is True
    broker_action_taken = source_summary.get("broker_action_taken") is True
    order_ticket_generated = source_summary.get("order_ticket_generated") is True
    next_action = _text(source_summary.get("next_action"), "MISSING")
    production_effect = _text(source_summary.get("production_effect"), PRODUCTION_EFFECT)
    safety_status = _etf_dynamic_v3_real_snapshot_safety_status(
        weekly_manifest,
        weekly_summary or {},
        owner_decision_summary or {},
        dry_run_manifest or {},
        owner_review_manifest or {},
        paper_action_manifest or {},
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(weekly_manifest.get("status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic Rescue Real Snapshot Advisory Review: "
            f"weekly={weekly_real_review_id}; snapshot={snapshot_status}; "
            f"recommended_action={recommended_action}; owner_decision={owner_decision}; "
            f"paper_action_taken={str(paper_action_taken).lower()}; "
            f"broker_action_taken={str(broker_action_taken).lower()}; "
            f"order_ticket_generated={str(order_ticket_generated).lower()}; "
            f"next_action={next_action}."
        ),
        "weekly_real_review_id": weekly_real_review_id,
        "week_ending": _text(source_summary.get("week_ending"), "MISSING"),
        "latest_snapshot_id": _text(source_summary.get("latest_snapshot_id"), "MISSING"),
        "latest_dry_run_id": _text(source_summary.get("latest_dry_run_id"), "MISSING"),
        "latest_owner_review_id": _text(
            source_summary.get("latest_owner_review_id"),
            "MISSING",
        ),
        "latest_paper_action_id": _text(
            source_summary.get("latest_paper_action_id"),
            "MISSING",
        ),
        "snapshot_status": snapshot_status,
        "recommended_action": recommended_action,
        "owner_decision": owner_decision,
        "paper_action_taken": paper_action_taken,
        "broker_action_taken": broker_action_taken,
        "order_ticket_generated": order_ticket_generated,
        "next_action": next_action,
        "production_effect": production_effect,
        "safety_status": safety_status,
        "pending_reviews": (owner_decision_summary or {}).get("pending_reviews", 0),
        "monitor_count": (owner_decision_summary or {}).get("monitor_count", 0),
        "no_trade_count": (owner_decision_summary or {}).get("no_trade_count", 0),
        "paper_adjustment_review_only_count": (owner_decision_summary or {}).get(
            "paper_adjustment_review_only_count",
            0,
        ),
        "weekly_real_snapshot_review_path": "" if weekly_path is None else str(weekly_path),
        "dry_run_path": "" if dry_run_path is None else str(dry_run_path),
        "owner_review_path": "" if owner_review_path is None else str(owner_review_path),
        "paper_action_path": "" if paper_action_path is None else str(paper_action_path),
        "broker_action": _text(weekly_manifest.get("broker_action"), "none"),
    }


def _missing_etf_dynamic_v3_real_snapshot_review_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Dynamic Rescue Real Snapshot Advisory Review: no latest weekly real "
            "snapshot review found."
        ),
        "weekly_real_review_id": "MISSING",
        "week_ending": "MISSING",
        "latest_snapshot_id": "MISSING",
        "latest_dry_run_id": "MISSING",
        "latest_owner_review_id": "MISSING",
        "latest_paper_action_id": "MISSING",
        "snapshot_status": "MISSING",
        "recommended_action": "MISSING",
        "owner_decision": "pending",
        "paper_action_taken": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "next_action": "update_snapshot",
        "production_effect": PRODUCTION_EFFECT,
        "safety_status": "MISSING",
        "pending_reviews": 0,
        "monitor_count": 0,
        "no_trade_count": 0,
        "paper_adjustment_review_only_count": 0,
        "weekly_real_snapshot_review_path": "",
        "dry_run_path": "",
        "owner_review_path": "",
        "paper_action_path": "",
        "broker_action": "none",
    }


def _etf_dynamic_v3_real_snapshot_safety_status(
    *payloads: Mapping[str, Any],
) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    unsafe = any(
        payload.get("broker_action_allowed") is True
        or payload.get("broker_action_taken") is True
        or payload.get("order_ticket_generated") is True
        or payload.get("production_state_mutated") is True
        or payload.get("baseline_config_mutated") is True
        or payload.get("official_target_weights_mutated") is True
        or payload.get("automatic_candidate_promotion") is True
        or payload.get("auto_enrollment_without_owner_approval") is True
        or payload.get("owner_approval_executed") is True
        or _text(payload.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT
        for payload in material
    )
    if not unsafe:
        return (
            "observe_only=true; candidate_only=true; production_effect=none; "
            "broker_action_allowed=false; broker_action_taken=false; "
            "order_ticket_generated=false"
        )
    return "SAFETY_REVIEW_REQUIRED"


def _etf_dynamic_v3_system_target_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_system_target_summary()

    target_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_model_target"),
        "model_target_manifest.json",
    )
    paper_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_paper_shadow"),
        "paper_shadow_manifest.json",
    )
    rebalance_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_model_rebalance"),
        "model_rebalance_manifest.json",
    )
    performance_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_paper_shadow_performance"),
        "paper_shadow_performance_manifest.json",
    )
    review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_system_target_review"),
        "system_target_review_manifest.json",
    )
    selection_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_system_target_selection_review",
        ),
        "system_target_selection_manifest.json",
    )
    hardening_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_research_method_hardening",
        ),
        "research_method_hardening_manifest.json",
    )
    refined_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_refined_method_proposal",
        ),
        "refined_method_proposal_manifest.json",
    )
    risk_capped_limited_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_risk_capped_limited",
        ),
        "risk_capped_limited_manifest.json",
    )
    risk_capped_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_risk_capped_backfill",
        ),
        "risk_capped_backfill_manifest.json",
    )
    risk_capped_comparison_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_risk_capped_comparison",
        ),
        "risk_capped_comparison_manifest.json",
    )
    risk_capped_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_risk_capped_review",
        ),
        "risk_capped_review_manifest.json",
    )
    smoothed_limited_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_limited",
        ),
        "smoothed_limited_manifest.json",
    )
    smoothed_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_backfill",
        ),
        "smoothed_backfill_manifest.json",
    )
    smoothed_comparison_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_comparison",
        ),
        "smoothed_comparison_manifest.json",
    )
    smoothed_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_review",
        ),
        "smoothed_review_manifest.json",
    )
    smoothed_watch_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_watch_pack",
        ),
        "smoothed_watch_manifest.json",
    )
    smoothed_owner_update_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_owner_review_update",
        ),
        "smoothed_owner_update_manifest.json",
    )
    smoothed_promotion_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_promotion_review",
        ),
        "smoothed_promotion_review_manifest.json",
    )
    primary_research_gate_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_primary_research_candidate_gate",
        ),
        "primary_research_candidate_gate_manifest.json",
    )
    smoothed_forward_binding_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_forward_binding",
        ),
        "smoothed_forward_binding_manifest.json",
    )
    paper_shadow_primary_switch_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_shadow_primary_switch",
        ),
        "paper_shadow_primary_switch_manifest.json",
    )
    smoothed_owner_promotion_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_owner_promotion",
        ),
        "smoothed_owner_promotion_manifest.json",
    )
    smoothed_forward_progress_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_forward_progress",
        ),
        "smoothed_forward_progress_manifest.json",
    )
    smoothed_weekly_dashboard_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_weekly_dashboard",
        ),
        "smoothed_weekly_dashboard_manifest.json",
    )
    smoothed_event_monitor_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_event_monitor",
        ),
        "smoothed_event_monitor_manifest.json",
    )
    smoothed_switch_readiness_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_switch_readiness",
        ),
        "smoothed_switch_readiness_manifest.json",
    )
    smoothed_owner_renewal_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_owner_renewal",
        ),
        "smoothed_owner_renewal_manifest.json",
    )
    smoothed_daily_emission_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_daily_emission",
        ),
        "smoothed_daily_emission_manifest.json",
    )
    smoothed_outcome_due_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_outcome_due",
        ),
        "smoothed_outcome_due_manifest.json",
    )
    smoothed_outcome_update_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_outcome_update",
        ),
        "smoothed_outcome_update_manifest.json",
    )
    smoothed_forward_classification_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_forward_classification",
        ),
        "smoothed_forward_classification_manifest.json",
    )
    smoothed_forward_weekly_run_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_forward_weekly_run",
        ),
        "smoothed_forward_weekly_run_manifest.json",
    )
    smoothed_data_preflight_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_data_preflight",
        ),
        "smoothed_data_preflight_manifest.json",
    )
    smoothed_latest_emission_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_latest_emission",
        ),
        "smoothed_latest_emission_manifest.json",
    )
    smoothed_blocked_explain_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_blocked_explain",
        ),
        "smoothed_blocked_explain_manifest.json",
    )
    smoothed_refresh_plan_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_refresh_plan",
        ),
        "smoothed_refresh_plan_manifest.json",
    )
    smoothed_bootstrap_retry_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_bootstrap_retry",
        ),
        "smoothed_bootstrap_retry_manifest.json",
    )
    smoothed_source_refresh_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_source_refresh",
        ),
        "smoothed_source_refresh_manifest.json",
    )
    smoothed_post_refresh_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_post_refresh_validation",
        ),
        "smoothed_post_refresh_manifest.json",
    )
    smoothed_retry_resume_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_retry_resume",
        ),
        "smoothed_retry_resume_manifest.json",
    )
    smoothed_sample_growth_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_sample_growth",
        ),
        "smoothed_sample_growth_manifest.json",
    )
    smoothed_data_readiness_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_smoothed_data_readiness",
        ),
        "smoothed_data_readiness_manifest.json",
    )
    experiment_triage_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_experiment_triage",
        ),
        "triage_manifest.json",
    )
    top_variant_interpretation_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_top_variant_interpretation",
        ),
        "top_variant_interpretation_manifest.json",
    )
    method_promotion_plan_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_method_promotion_plan",
        ),
        "method_promotion_manifest.json",
    )
    no_promotion_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_no_promotion_review",
        ),
        "no_promotion_review_manifest.json",
    )
    near_miss_candidates_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_near_miss_candidates",
        ),
        "near_miss_manifest.json",
    )
    cash_buffer_attribution_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_cash_buffer_attribution",
        ),
        "cash_buffer_attribution_manifest.json",
    )
    search_coverage_gap_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_search_coverage_gap",
        ),
        "search_coverage_gap_manifest.json",
    )
    targeted_search_v3_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_targeted_search_v3",
        ),
        "targeted_search_v3_manifest.json",
    )
    targeted_v3_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_targeted_v3_backfill",
        ),
        "targeted_v3_backfill_manifest.json",
    )
    near_miss_ab_comparison_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_near_miss_ab_comparison",
        ),
        "near_miss_ab_manifest.json",
    )
    promotion_threshold_sensitivity_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_promotion_threshold_sensitivity",
        ),
        "threshold_sensitivity_manifest.json",
    )
    candidate_promotion_v2_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_candidate_promotion_v2",
        ),
        "candidate_promotion_v2_manifest.json",
    )
    next_formal_or_search_plan_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_next_formal_or_search_plan",
        ),
        "next_formal_or_search_manifest.json",
    )
    review_manifest = _read_optional_json(review_path)
    risk_capped_review_manifest = _read_optional_json(risk_capped_review_path)
    smoothed_review_manifest = _read_optional_json(smoothed_review_path)
    smoothed_watch_manifest = _read_optional_json(smoothed_watch_path)
    smoothed_owner_update_manifest = _read_optional_json(smoothed_owner_update_path)
    smoothed_promotion_review_manifest = _read_optional_json(smoothed_promotion_review_path)
    primary_research_gate_manifest = _read_optional_json(primary_research_gate_path)
    smoothed_forward_binding_manifest = _read_optional_json(smoothed_forward_binding_path)
    paper_shadow_primary_switch_manifest = _read_optional_json(paper_shadow_primary_switch_path)
    smoothed_owner_promotion_manifest = _read_optional_json(smoothed_owner_promotion_path)
    smoothed_forward_progress_manifest = _read_optional_json(smoothed_forward_progress_path)
    smoothed_weekly_dashboard_manifest = _read_optional_json(smoothed_weekly_dashboard_path)
    smoothed_event_monitor_manifest = _read_optional_json(smoothed_event_monitor_path)
    smoothed_switch_readiness_manifest = _read_optional_json(smoothed_switch_readiness_path)
    smoothed_owner_renewal_manifest = _read_optional_json(smoothed_owner_renewal_path)
    smoothed_daily_emission_manifest = _read_optional_json(smoothed_daily_emission_path)
    smoothed_outcome_due_manifest = _read_optional_json(smoothed_outcome_due_path)
    smoothed_outcome_update_manifest = _read_optional_json(smoothed_outcome_update_path)
    smoothed_forward_classification_manifest = _read_optional_json(
        smoothed_forward_classification_path
    )
    smoothed_forward_weekly_run_manifest = _read_optional_json(smoothed_forward_weekly_run_path)
    smoothed_data_preflight_manifest = _read_optional_json(smoothed_data_preflight_path)
    smoothed_latest_emission_manifest = _read_optional_json(smoothed_latest_emission_path)
    smoothed_blocked_explain_manifest = _read_optional_json(smoothed_blocked_explain_path)
    smoothed_refresh_plan_manifest = _read_optional_json(smoothed_refresh_plan_path)
    smoothed_bootstrap_retry_manifest = _read_optional_json(smoothed_bootstrap_retry_path)
    smoothed_source_refresh_manifest = _read_optional_json(smoothed_source_refresh_path)
    smoothed_post_refresh_manifest = _read_optional_json(smoothed_post_refresh_path)
    smoothed_retry_resume_manifest = _read_optional_json(smoothed_retry_resume_path)
    smoothed_sample_growth_manifest = _read_optional_json(smoothed_sample_growth_path)
    smoothed_data_readiness_manifest = _read_optional_json(smoothed_data_readiness_path)
    experiment_triage_manifest = _read_optional_json(experiment_triage_path)
    top_variant_interpretation_manifest = _read_optional_json(top_variant_interpretation_path)
    method_promotion_plan_manifest = _read_optional_json(method_promotion_plan_path)
    no_promotion_review_manifest = _read_optional_json(no_promotion_review_path)
    near_miss_candidates_manifest = _read_optional_json(near_miss_candidates_path)
    cash_buffer_attribution_manifest = _read_optional_json(cash_buffer_attribution_path)
    search_coverage_gap_manifest = _read_optional_json(search_coverage_gap_path)
    targeted_search_v3_manifest = _read_optional_json(targeted_search_v3_path)
    targeted_v3_backfill_manifest = _read_optional_json(targeted_v3_backfill_path)
    near_miss_ab_comparison_manifest = _read_optional_json(near_miss_ab_comparison_path)
    promotion_threshold_sensitivity_manifest = _read_optional_json(
        promotion_threshold_sensitivity_path
    )
    candidate_promotion_v2_manifest = _read_optional_json(candidate_promotion_v2_path)
    next_formal_or_search_plan_manifest = _read_optional_json(next_formal_or_search_plan_path)
    if (
        not review_manifest
        and not risk_capped_review_manifest
        and not smoothed_review_manifest
        and not smoothed_watch_manifest
        and not smoothed_owner_update_manifest
        and not smoothed_promotion_review_manifest
        and not primary_research_gate_manifest
        and not smoothed_forward_binding_manifest
        and not paper_shadow_primary_switch_manifest
        and not smoothed_owner_promotion_manifest
        and not smoothed_forward_progress_manifest
        and not smoothed_weekly_dashboard_manifest
        and not smoothed_event_monitor_manifest
        and not smoothed_switch_readiness_manifest
        and not smoothed_owner_renewal_manifest
        and not smoothed_daily_emission_manifest
        and not smoothed_outcome_due_manifest
        and not smoothed_outcome_update_manifest
        and not smoothed_forward_classification_manifest
        and not smoothed_forward_weekly_run_manifest
        and not smoothed_data_preflight_manifest
        and not smoothed_latest_emission_manifest
        and not smoothed_blocked_explain_manifest
        and not smoothed_refresh_plan_manifest
        and not smoothed_bootstrap_retry_manifest
        and not smoothed_source_refresh_manifest
        and not smoothed_post_refresh_manifest
        and not smoothed_retry_resume_manifest
        and not smoothed_sample_growth_manifest
        and not smoothed_data_readiness_manifest
        and not experiment_triage_manifest
        and not top_variant_interpretation_manifest
        and not method_promotion_plan_manifest
        and not no_promotion_review_manifest
        and not near_miss_candidates_manifest
        and not cash_buffer_attribution_manifest
        and not search_coverage_gap_manifest
        and not targeted_search_v3_manifest
        and not targeted_v3_backfill_manifest
        and not near_miss_ab_comparison_manifest
        and not promotion_threshold_sensitivity_manifest
        and not candidate_promotion_v2_manifest
        and not next_formal_or_search_plan_manifest
    ):
        return _missing_etf_dynamic_v3_system_target_summary()

    target_manifest = _read_optional_json(target_path)
    paper_manifest = _read_optional_json(paper_path)
    rebalance_manifest = _read_optional_json(rebalance_path)
    performance_manifest = _read_optional_json(performance_path)
    decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(review_path, "system_target_decision.json")
    )
    selection_manifest = _read_optional_json(selection_path)
    selection_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(selection_path, "selection_decision.json")
    )
    selection_scorecard = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(selection_path, "target_method_scorecard.json")
    )
    hardening_manifest = _read_optional_json(hardening_path)
    hardening_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(hardening_path, "hardening_decision.json")
    )
    refined_manifest = _read_optional_json(refined_path)
    refined_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(refined_path, "refined_method_decision.json")
    )
    refined_methods = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(refined_path, "proposed_next_methods.json")
    )
    risk_capped_limited_manifest = _read_optional_json(risk_capped_limited_path)
    risk_capped_cap_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(risk_capped_limited_path, "cap_reason_summary.json")
    )
    risk_capped_backfill_manifest = _read_optional_json(risk_capped_backfill_path)
    risk_capped_backfill_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            risk_capped_backfill_path,
            "risk_capped_backfill_summary.json",
        )
    )
    risk_capped_comparison_manifest = _read_optional_json(risk_capped_comparison_path)
    risk_capped_metrics = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            risk_capped_comparison_path,
            "risk_capped_vs_limited_metrics.json",
        )
    )
    risk_capped_rolling = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            risk_capped_comparison_path,
            "risk_capped_rolling_comparison.json",
        )
    )
    risk_capped_stability = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            risk_capped_comparison_path,
            "risk_capped_stability_comparison.json",
        )
    )
    risk_capped_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            risk_capped_review_path,
            "risk_capped_decision.json",
        )
    )
    smoothed_limited_manifest = _read_optional_json(smoothed_limited_path)
    smoothed_jump_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_limited_path,
            "weight_jump_reduction_summary.json",
        )
    )
    smoothed_backfill_manifest = _read_optional_json(smoothed_backfill_path)
    smoothed_backfill_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_backfill_path,
            "smoothed_backfill_summary.json",
        )
    )
    smoothed_comparison_manifest = _read_optional_json(smoothed_comparison_path)
    smoothed_metrics = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_comparison_path,
            "smoothed_vs_limited_metrics.json",
        )
    )
    smoothed_rolling = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_comparison_path,
            "smoothed_rolling_comparison.json",
        )
    )
    smoothed_stability = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_comparison_path,
            "smoothed_stability_comparison.json",
        )
    )
    smoothed_lag_cost = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_comparison_path,
            "smoothing_lag_cost_analysis.json",
        )
    )
    smoothed_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_review_path,
            "smoothed_decision.json",
        )
    )
    smoothed_watch_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_watch_path,
            "smoothed_watch_summary.json",
        )
    )
    smoothed_owner_options = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_owner_update_path,
            "smoothed_owner_decision_options.json",
        )
    )
    smoothed_promotion_evidence = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_promotion_review_path,
            "promotion_evidence_summary.json",
        )
    )
    smoothed_promotion_blocking = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_promotion_review_path,
            "promotion_blocking_issues.json",
        )
    )
    primary_research_gate_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            primary_research_gate_path,
            "gate_decision.json",
        )
    )
    primary_research_gate_criteria = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            primary_research_gate_path,
            "gate_criteria_results.json",
        )
    )
    smoothed_bound_targets = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_binding_path,
            "bound_confirmation_targets.json",
        )
    )
    smoothed_forward_requirements = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_binding_path,
            "forward_progress_requirements.json",
        )
    )
    paper_shadow_switch_plan = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            paper_shadow_primary_switch_path,
            "primary_switch_plan.json",
        )
    )
    paper_shadow_switch_safety = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            paper_shadow_primary_switch_path,
            "primary_switch_safety_checks.json",
        )
    )
    smoothed_owner_promotion_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_owner_promotion_path,
            "owner_promotion_decision.json",
        )
    )
    smoothed_forward_progress_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_progress_path,
            "smoothed_forward_progress_summary.json",
        )
    )
    smoothed_target_progress = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_progress_path,
            "smoothed_target_progress.jsonl",
        )
    )
    smoothed_dashboard_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_weekly_dashboard_path,
            "smoothed_dashboard_summary.json",
        )
    )
    smoothed_target_status_table = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_weekly_dashboard_path,
            "smoothed_target_status_table.json",
        )
    )
    smoothed_event_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_event_monitor_path,
            "event_accumulation_summary.json",
        )
    )
    smoothed_switch_readiness_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_switch_readiness_path,
            "switch_readiness_decision.json",
        )
    )
    smoothed_switch_readiness_criteria = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_switch_readiness_path,
            "switch_readiness_criteria.json",
        )
    )
    smoothed_owner_renewal_options = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_owner_renewal_path,
            "owner_renewal_options.json",
        )
    )
    smoothed_daily_events = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(
            smoothed_daily_emission_path,
            "smoothed_forward_events.jsonl",
        )
    )
    smoothed_emission_data_quality = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_daily_emission_path,
            "smoothed_emission_data_quality.json",
        )
    )
    smoothed_outcome_due_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_outcome_due_path,
            "due_summary.json",
        )
    )
    smoothed_outcome_update_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_outcome_update_path,
            "smoothed_outcome_delta_summary.json",
        )
    )
    smoothed_forward_classification_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_classification_path,
            "classification_summary.json",
        )
    )
    smoothed_forward_weekly_run_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_forward_weekly_run_path,
            "weekly_run_summary.json",
        )
    )
    smoothed_data_freshness_snapshot = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_data_preflight_path,
            "data_freshness_snapshot.json",
        )
    )
    smoothed_runnable_command_matrix = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_data_preflight_path,
            "runnable_command_matrix.json",
        )
    )
    smoothed_latest_emission_resolution = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_latest_emission_path,
            "latest_emission_resolution.json",
        )
    )
    smoothed_latest_emission_links = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_latest_emission_path,
            "latest_emission_artifact_links.json",
        )
    )
    smoothed_blocked_command_explanations = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_blocked_explain_path,
            "blocked_command_explanations.json",
        )
    )
    smoothed_source_refresh_requirements = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_refresh_plan_path,
            "source_refresh_requirements.json",
        )
    )
    smoothed_rerun_command_plan = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_refresh_plan_path,
            "rerun_command_plan.json",
        )
    )
    smoothed_retry_preflight_result = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_bootstrap_retry_path,
            "retry_preflight_result.json",
        )
    )
    smoothed_retry_steps = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_bootstrap_retry_path,
            "retry_steps.json",
        )
    )
    smoothed_retry_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_bootstrap_retry_path,
            "retry_summary.json",
        )
    )
    smoothed_source_refresh_results = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_source_refresh_path,
            "source_refresh_results.json",
        )
    )
    smoothed_post_refresh_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_post_refresh_path,
            "post_refresh_decision.json",
        )
    )
    smoothed_post_refresh_preflight = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_post_refresh_path,
            "post_refresh_preflight_result.json",
        )
    )
    smoothed_retry_resume_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_retry_resume_path,
            "resume_summary.json",
        )
    )
    smoothed_retry_resume_precondition = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_retry_resume_path,
            "resume_precondition_check.json",
        )
    )
    smoothed_sample_growth_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_sample_growth_path,
            "sample_growth_summary.json",
        )
    )
    smoothed_data_readiness_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            smoothed_data_readiness_path,
            "owner_data_readiness_summary.json",
        )
    )
    experiment_triage_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(experiment_triage_path, "triage_summary.json")
    )
    experiment_scorecard = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(experiment_triage_path, "variant_scorecard.jsonl")
    )
    promotion_candidates = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(
            experiment_triage_path,
            "promotion_candidates.jsonl",
        )
    )
    top_variant_explanations = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(
            top_variant_interpretation_path,
            "top_variant_explanations.jsonl",
        )
    )
    promoted_method_specs = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            method_promotion_plan_path,
            "promoted_method_specs.json",
        )
    )
    no_promotion_reason_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            no_promotion_review_path,
            "no_promotion_reason_summary.json",
        )
    )
    near_miss_candidates = _read_optional_jsonl(
        _dynamic_v3_sibling_artifact_path(
            near_miss_candidates_path,
            "near_miss_candidates.jsonl",
        )
    )
    near_miss_family_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            near_miss_candidates_path,
            "near_miss_family_summary.json",
        )
    )
    cash_buffer_effect_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            cash_buffer_attribution_path,
            "cash_buffer_effect_summary.json",
        )
    )
    cash_buffer_failure_reason = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            cash_buffer_attribution_path,
            "cash_buffer_failure_reason.json",
        )
    )
    targeted_v3_recommendations = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            search_coverage_gap_path,
            "targeted_v3_recommendations.json",
        )
    )
    v3_family_coverage = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            targeted_search_v3_path,
            "v3_family_coverage.json",
        )
    )
    v3_backfill_progress = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            targeted_v3_backfill_path,
            "v3_backfill_progress.json",
        )
    )
    ab_winner_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            near_miss_ab_comparison_path,
            "ab_winner_summary.json",
        )
    )
    threshold_candidate_impact = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            promotion_threshold_sensitivity_path,
            "threshold_candidate_impact.json",
        )
    )
    promotion_v2_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            candidate_promotion_v2_path,
            "promotion_v2_decision.json",
        )
    )
    next_plan_decision = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            next_formal_or_search_plan_path,
            "next_plan_decision.json",
        )
    )
    paper_state = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(paper_path, "paper_shadow_state.json")
    )
    rebalance_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            rebalance_path,
            "rebalance_turnover_summary.json",
        )
    )
    performance_summary = _read_optional_json(
        _dynamic_v3_sibling_artifact_path(
            performance_path,
            "method_performance_summary.json",
        )
    )
    method_rows = _records(performance_summary.get("methods"))
    limited_row = _first_method_row(method_rows, "limited_adjustment")
    consensus_row = _first_method_row(method_rows, "consensus_target")
    tracked_methods = _texts(paper_state.get("tracked_methods")) or [
        _text(row.get("target_method"))
        for row in _records(paper_state.get("method_states"))
        if row.get("target_method")
    ]
    target_id = _text(
        review_manifest.get("target_id"), _text(target_manifest.get("target_id"), "MISSING")
    )
    paper_shadow_id = _text(
        review_manifest.get("paper_shadow_id"),
        _text(paper_manifest.get("paper_shadow_id"), "MISSING"),
    )
    performance_id = _text(
        review_manifest.get("performance_id"),
        _text(performance_manifest.get("performance_id"), "MISSING"),
    )
    review_id = _text(review_manifest.get("review_id"), "MISSING")
    recommended = _text(
        selection_decision.get("recommended_research_method"),
        _text(
            decision.get("recommended_research_method"),
            _text(review_manifest.get("recommended_research_method"), "MISSING"),
        ),
    )
    decision_status = _text(
        selection_decision.get("decision_status"),
        _text(
            decision.get("decision_status"),
            _text(review_manifest.get("decision_status"), "MISSING"),
        ),
    )
    production_effect = _text(review_manifest.get("production_effect"), PRODUCTION_EFFECT)
    safety_status = _etf_dynamic_v3_system_target_safety_status(
        target_manifest,
        paper_manifest,
        paper_state,
        rebalance_manifest,
        rebalance_summary,
        performance_manifest,
        performance_summary,
        review_manifest,
        decision,
        selection_manifest,
        selection_decision,
        selection_scorecard,
        hardening_manifest,
        hardening_decision,
        refined_manifest,
        refined_decision,
        refined_methods,
        risk_capped_limited_manifest,
        risk_capped_cap_summary,
        risk_capped_backfill_manifest,
        risk_capped_backfill_summary,
        risk_capped_comparison_manifest,
        risk_capped_metrics,
        risk_capped_rolling,
        risk_capped_stability,
        risk_capped_review_manifest,
        risk_capped_decision,
        smoothed_limited_manifest,
        smoothed_jump_summary,
        smoothed_backfill_manifest,
        smoothed_backfill_summary,
        smoothed_comparison_manifest,
        smoothed_metrics,
        smoothed_rolling,
        smoothed_stability,
        smoothed_lag_cost,
        smoothed_review_manifest,
        smoothed_decision,
        smoothed_watch_manifest,
        smoothed_watch_summary,
        smoothed_owner_update_manifest,
        smoothed_owner_options,
        smoothed_promotion_review_manifest,
        smoothed_promotion_evidence,
        smoothed_promotion_blocking,
        primary_research_gate_manifest,
        primary_research_gate_decision,
        primary_research_gate_criteria,
        smoothed_forward_binding_manifest,
        smoothed_bound_targets,
        smoothed_forward_requirements,
        paper_shadow_primary_switch_manifest,
        paper_shadow_switch_plan,
        paper_shadow_switch_safety,
        smoothed_owner_promotion_manifest,
        smoothed_owner_promotion_decision,
        smoothed_forward_progress_manifest,
        smoothed_forward_progress_summary,
        *smoothed_target_progress,
        smoothed_weekly_dashboard_manifest,
        smoothed_dashboard_summary,
        smoothed_target_status_table,
        smoothed_event_monitor_manifest,
        smoothed_event_summary,
        smoothed_switch_readiness_manifest,
        smoothed_switch_readiness_decision,
        smoothed_switch_readiness_criteria,
        smoothed_owner_renewal_manifest,
        smoothed_owner_renewal_options,
        smoothed_daily_emission_manifest,
        *smoothed_daily_events,
        smoothed_emission_data_quality,
        smoothed_outcome_due_manifest,
        smoothed_outcome_due_summary,
        smoothed_outcome_update_manifest,
        smoothed_outcome_update_summary,
        smoothed_forward_classification_manifest,
        smoothed_forward_classification_summary,
        smoothed_forward_weekly_run_manifest,
        smoothed_forward_weekly_run_summary,
        smoothed_data_preflight_manifest,
        smoothed_data_freshness_snapshot,
        smoothed_runnable_command_matrix,
        smoothed_latest_emission_manifest,
        smoothed_latest_emission_resolution,
        smoothed_latest_emission_links,
        smoothed_blocked_explain_manifest,
        smoothed_blocked_command_explanations,
        smoothed_refresh_plan_manifest,
        smoothed_source_refresh_requirements,
        smoothed_rerun_command_plan,
        smoothed_bootstrap_retry_manifest,
        smoothed_retry_preflight_result,
        smoothed_retry_steps,
        smoothed_retry_summary,
        smoothed_source_refresh_manifest,
        smoothed_source_refresh_results,
        smoothed_post_refresh_manifest,
        smoothed_post_refresh_decision,
        smoothed_post_refresh_preflight,
        smoothed_retry_resume_manifest,
        smoothed_retry_resume_precondition,
        smoothed_retry_resume_summary,
        smoothed_sample_growth_manifest,
        smoothed_sample_growth_summary,
        smoothed_data_readiness_manifest,
        smoothed_data_readiness_summary,
        experiment_triage_manifest,
        experiment_triage_summary,
        *experiment_scorecard,
        *promotion_candidates,
        top_variant_interpretation_manifest,
        *top_variant_explanations,
        method_promotion_plan_manifest,
        promoted_method_specs,
        no_promotion_review_manifest,
        no_promotion_reason_summary,
        near_miss_candidates_manifest,
        *near_miss_candidates,
        near_miss_family_summary,
        cash_buffer_attribution_manifest,
        cash_buffer_effect_summary,
        cash_buffer_failure_reason,
        search_coverage_gap_manifest,
        targeted_v3_recommendations,
        targeted_search_v3_manifest,
        v3_family_coverage,
        targeted_v3_backfill_manifest,
        v3_backfill_progress,
        near_miss_ab_comparison_manifest,
        ab_winner_summary,
        promotion_threshold_sensitivity_manifest,
        threshold_candidate_impact,
        candidate_promotion_v2_manifest,
        promotion_v2_decision,
        next_formal_or_search_plan_manifest,
        next_plan_decision,
    )
    hardening_decision_label = _text(hardening_decision.get("hardening_decision"), "MISSING")
    refined_next_step = _text(refined_decision.get("recommended_next_step"), "MISSING")
    refined_method_names = ",".join(
        _texts([row.get("method") for row in _records(refined_methods.get("methods"))])
    )
    risk_capped_values = _mapping(risk_capped_metrics.get("metrics"))
    risk_capped_improvements = _mapping(risk_capped_decision.get("improvements_vs_limited"))
    risk_capped_improvement_summary = ",".join(
        f"{key}={value}" for key, value in sorted(risk_capped_improvements.items())
    )
    risk_capped_decision_label = _text(risk_capped_decision.get("decision"), "MISSING")
    smoothed_comparisons = _records(smoothed_metrics.get("comparisons"))
    smoothed_primary = next(
        (
            row
            for row in smoothed_comparisons
            if row.get("method_a") == "smooth_weights_3d_limited_adjustment"
            and row.get("method_b") == "limited_adjustment"
        ),
        {},
    )
    smoothed_improvements = _mapping(smoothed_decision.get("improvements_vs_limited"))
    smoothed_improvement_summary = ",".join(
        f"{key}={value}" for key, value in sorted(smoothed_improvements.items())
    )
    smoothed_decision_label = _text(smoothed_decision.get("decision"), "MISSING")
    smoothed_rolling_primary = next(
        (
            row
            for row in _records(smoothed_rolling.get("methods"))
            if row.get("target_method") == "smooth_weights_3d_limited_adjustment"
        ),
        {},
    )
    smoothed_stability_primary = next(
        (
            row
            for row in _records(smoothed_stability.get("methods"))
            if row.get("target_method") == "smooth_weights_3d_limited_adjustment"
        ),
        {},
    )
    smoothed_lag_primary = next(
        (
            row
            for row in _records(smoothed_lag_cost.get("methods"))
            if row.get("target_method") == "smooth_weights_3d_limited_adjustment"
        ),
        {},
    )
    experiment_top_variant = _text(experiment_triage_summary.get("top_variant"), "MISSING")
    top_promoted_variants = ",".join(
        _texts([row.get("variant_id") for row in promotion_candidates])
    )
    best_explanation = _mapping(top_variant_explanations[0] if top_variant_explanations else {})
    promoted_methods = _records(promoted_method_specs.get("methods"))
    proposed_method_names = ",".join(
        _texts([row.get("proposed_method_name") for row in promoted_methods])
    )
    smoothed_readiness_not_met = ",".join(
        _texts(
            [
                row.get("criterion")
                for row in _records(smoothed_switch_readiness_criteria.get("criteria"))
                if row.get("status") != "PASS"
            ]
        )
    )
    smoothed_owner_renewal_option_names = ",".join(
        _texts(
            [
                row.get("decision")
                for row in _records(smoothed_owner_renewal_options.get("owner_options"))
            ]
        )
    )
    smoothed_event_sideways = _mapping(smoothed_event_summary.get("sideways_events"))
    smoothed_event_recovery = _mapping(smoothed_event_summary.get("recovery_events"))
    smoothed_daily_event = _mapping(smoothed_daily_events[0] if smoothed_daily_events else {})
    smoothed_bootstrap_due_window_counts = (
        f"{smoothed_outcome_due_summary.get('due_windows', 'MISSING')}/"
        f"{smoothed_outcome_due_summary.get('total_windows_scanned', 'MISSING')}"
    )
    smoothed_bootstrap_update_counts = (
        f"{smoothed_outcome_update_summary.get('updated_count', 'MISSING')}/"
        f"{smoothed_outcome_update_summary.get('skipped_count', 'MISSING')}"
    )
    smoothed_bootstrap_recommendation = _text(
        smoothed_forward_weekly_run_summary.get("weekly_recommendation"),
        "MISSING",
    )
    smoothed_preflight_blocking_errors = ",".join(
        _texts(smoothed_data_freshness_snapshot.get("blocking_errors"))
    )
    smoothed_preflight_runnable_latest_count = len(
        [
            row
            for row in _records(smoothed_runnable_command_matrix.get("commands"))
            if row.get("status") == "RUNNABLE_WITH_LATEST_AVAILABLE"
        ]
    )
    smoothed_blocked_commands = _records(
        smoothed_blocked_command_explanations.get("blocked_commands")
    )
    smoothed_blocked_command_names = ",".join(
        _texts([row.get("command") for row in smoothed_blocked_commands])
    )
    smoothed_source_requirements = _records(
        smoothed_source_refresh_requirements.get("source_requirements")
    )
    smoothed_refresh_required_sources = ",".join(
        _texts(
            [
                row.get("source")
                for row in smoothed_source_requirements
                if row.get("required") is True and row.get("status") != "FRESH"
            ]
        )
    )
    smoothed_refresh_stale_source_count = len(
        [row for row in smoothed_source_requirements if row.get("status") != "FRESH"]
    )
    smoothed_retry_step_statuses = ",".join(
        _texts(
            [
                f"{row.get('step')}={row.get('status')}"
                for row in _records(smoothed_retry_steps.get("steps"))
            ]
        )
    )
    smoothed_source_refresh_rows = _records(smoothed_source_refresh_results.get("sources"))
    smoothed_source_refresh_ready_count = sum(
        1 for row in smoothed_source_refresh_rows if row.get("freshness_after_refresh") == "READY"
    )
    smoothed_source_refresh_failed_sources = ",".join(
        _texts(
            [
                row.get("source")
                for row in smoothed_source_refresh_rows
                if row.get("status") == "FAILED"
            ]
        )
    )
    smoothed_post_refresh_blocking_errors = ",".join(
        _texts(smoothed_post_refresh_preflight.get("blocking_errors"))
    )
    smoothed_sample_growth_progress = _mapping(smoothed_sample_growth_summary.get("progress"))
    smoothed_sample_growth_delta = _mapping(smoothed_sample_growth_summary.get("delta"))
    smoothed_data_readiness_sources = _mapping(
        smoothed_data_readiness_summary.get("sources_status")
    )
    smoothed_data_readiness_source_statuses = ",".join(
        f"{key}={value}" for key, value in sorted(smoothed_data_readiness_sources.items())
    )
    primary_status = _text(
        review_manifest.get("status"),
        _text(
            method_promotion_plan_manifest.get("status"),
            _text(experiment_triage_manifest.get("status"), "UNKNOWN"),
        ),
    )
    return {
        "availability": "AVAILABLE",
        "status": primary_status,
        "summary_sentence": (
            "Dynamic Rescue System Target Portfolio: "
            f"target={target_id}; paper_shadow={paper_shadow_id}; "
            f"performance={performance_id}; recommended={recommended}; "
            f"decision={decision_status}; "
            f"hardening={hardening_decision_label}; "
            f"refined_next_step={refined_next_step}; "
            f"risk_capped={risk_capped_decision_label}; "
            f"smoothed={smoothed_decision_label}; "
            f"smoothed_watch={_text(smoothed_watch_summary.get('recommended_action'), 'MISSING')}; "
            "smoothed_owner="
            f"{_text(smoothed_owner_options.get('recommended_owner_action'), 'MISSING')}; "
            "smoothed_promotion="
            f"{_text(smoothed_promotion_evidence.get('readiness_decision'), 'MISSING')}; "
            "primary_gate="
            f"{_text(primary_research_gate_decision.get('gate_decision'), 'MISSING')}; "
            "owner_promotion="
            f"{_text(smoothed_owner_promotion_decision.get('owner_decision'), 'MISSING')}; "
            "forward_progress="
            f"{_text(smoothed_forward_progress_summary.get('summary_recommendation'), 'MISSING')}; "
            "weekly_dashboard="
            f"{_text(smoothed_dashboard_summary.get('weekly_recommendation'), 'MISSING')}; "
            "switch_readiness="
            f"{_text(smoothed_switch_readiness_decision.get('recheck_decision'), 'MISSING')}; "
            "owner_renewal="
            f"{_text(smoothed_owner_renewal_options.get('recommended_owner_action'), 'MISSING')}; "
            "sample_bootstrap="
            f"{smoothed_bootstrap_recommendation}; "
            "data_readiness="
            f"{_text(smoothed_data_readiness_summary.get('current_status'), 'MISSING')}; "
            f"experiment_top={experiment_top_variant}; "
            f"promotion_next={_text(promoted_method_specs.get('next_action'), 'MISSING')}; "
            f"data_quality={_text(performance_summary.get('data_quality_status'), 'MISSING')}; "
            "broker_action_allowed="
            f"{str(review_manifest.get('broker_action_allowed') is True).lower()}; "
            f"production_effect={production_effect}."
        ),
        "target_id": target_id,
        "paper_shadow_id": paper_shadow_id,
        "rebalance_id": _text(rebalance_summary.get("rebalance_id"), "MISSING"),
        "performance_id": performance_id,
        "review_id": review_id,
        "generated_methods": ",".join(_texts(target_manifest.get("generated_methods"))),
        "tracked_methods": ",".join(tracked_methods),
        "recommended_research_method": recommended,
        "decision_status": decision_status,
        "selection_review_id": _text(
            selection_manifest.get("selection_review_id"),
            "MISSING",
        ),
        "secondary_research_methods": ",".join(
            _texts(selection_decision.get("secondary_research_methods"))
        ),
        "reference_only_methods": ",".join(
            _texts(selection_decision.get("reference_only_methods"))
        ),
        "selection_review_path": "" if selection_path is None else str(selection_path),
        "hardening_id": _text(hardening_manifest.get("hardening_id"), "MISSING"),
        "hardening_decision": hardening_decision_label,
        "hardening_decision_confidence": _text(
            hardening_decision.get("decision_confidence"),
            "MISSING",
        ),
        "hardening_blocking_issues": ",".join(_texts(hardening_decision.get("blocking_issues"))),
        "hardening_path": "" if hardening_path is None else str(hardening_path),
        "refined_proposal_id": _text(refined_manifest.get("proposal_id"), "MISSING"),
        "refined_recommended_next_step": refined_next_step,
        "refined_confidence": _text(refined_decision.get("confidence"), "MISSING"),
        "refined_proposed_next_methods": refined_method_names,
        "refined_proposal_path": "" if refined_path is None else str(refined_path),
        "risk_capped_id": _text(risk_capped_limited_manifest.get("risk_capped_id"), "MISSING"),
        "risk_capped_backfill_id": _text(
            risk_capped_backfill_manifest.get("risk_capped_backfill_id"),
            "MISSING",
        ),
        "risk_capped_comparison_id": _text(
            risk_capped_comparison_manifest.get("comparison_id"),
            "MISSING",
        ),
        "risk_capped_review_id": _text(
            risk_capped_review_manifest.get("review_id"),
            "MISSING",
        ),
        "risk_capped_decision": risk_capped_decision_label,
        "risk_capped_confidence": _text(
            risk_capped_decision.get("decision_confidence"),
            "MISSING",
        ),
        "risk_capped_improvements_vs_limited": risk_capped_improvement_summary,
        "risk_capped_requires_forward_confirmation": risk_capped_decision.get(
            "requires_forward_confirmation"
        )
        is True,
        "risk_capped_return_delta_vs_limited": risk_capped_values.get(
            "total_return_delta",
            "MISSING",
        ),
        "risk_capped_drawdown_delta_vs_limited": risk_capped_values.get(
            "max_drawdown_delta",
            "MISSING",
        ),
        "risk_capped_semiconductor_exposure_delta": risk_capped_values.get(
            "avg_semiconductor_weight_delta",
            "MISSING",
        ),
        "risk_capped_rolling_consistency_delta": _text(
            risk_capped_rolling.get("stability_delta"),
            "MISSING",
        ),
        "risk_capped_stability_conclusion": _text(
            risk_capped_stability.get("stability_conclusion"),
            "MISSING",
        ),
        "risk_capped_generated_cap_event_count": risk_capped_cap_summary.get(
            "total_cap_events",
            "MISSING",
        ),
        "risk_capped_backfill_cap_event_count": risk_capped_backfill_summary.get(
            "cap_event_count",
            "MISSING",
        ),
        "risk_capped_reallocated_to_cash": risk_capped_cap_summary.get(
            "total_reallocated_to_cash",
            "MISSING",
        ),
        "risk_capped_path": (
            "" if risk_capped_limited_path is None else str(risk_capped_limited_path)
        ),
        "risk_capped_backfill_path": (
            "" if risk_capped_backfill_path is None else str(risk_capped_backfill_path)
        ),
        "risk_capped_comparison_path": (
            "" if risk_capped_comparison_path is None else str(risk_capped_comparison_path)
        ),
        "risk_capped_review_path": (
            "" if risk_capped_review_path is None else str(risk_capped_review_path)
        ),
        "smoothed_id": _text(smoothed_limited_manifest.get("smoothed_id"), "MISSING"),
        "smoothed_backfill_id": _text(
            smoothed_backfill_manifest.get("smoothed_backfill_id"),
            "MISSING",
        ),
        "smoothed_comparison_id": _text(
            smoothed_comparison_manifest.get("comparison_id"),
            "MISSING",
        ),
        "smoothed_review_id": _text(
            smoothed_review_manifest.get("review_id"),
            "MISSING",
        ),
        "smoothed_decision": smoothed_decision_label,
        "smoothed_recommended_method": _text(
            smoothed_decision.get("recommended_method"),
            "MISSING",
        ),
        "smoothed_secondary_method": _text(
            smoothed_decision.get("secondary_method"),
            "MISSING",
        ),
        "smoothed_confidence": _text(
            smoothed_decision.get("decision_confidence"),
            "MISSING",
        ),
        "smoothed_improvements_vs_limited": smoothed_improvement_summary,
        "smoothed_lag_risk": _text(smoothed_decision.get("lag_risk"), "MISSING"),
        "smoothed_requires_forward_confirmation": smoothed_decision.get(
            "requires_forward_confirmation"
        )
        is True,
        "smoothed_return_delta_vs_limited": smoothed_primary.get(
            "total_return_delta",
            "MISSING",
        ),
        "smoothed_drawdown_delta_vs_limited": smoothed_primary.get(
            "max_drawdown_delta",
            "MISSING",
        ),
        "smoothed_turnover_delta_vs_limited": smoothed_primary.get(
            "turnover_delta",
            "MISSING",
        ),
        "smoothed_rolling_consistency_delta": _text(
            smoothed_rolling_primary.get("rolling_consistency_delta"),
            "MISSING",
        ),
        "smoothed_stability_conclusion": _text(
            smoothed_stability_primary.get("stability_conclusion"),
            "MISSING",
        ),
        "smoothed_lag_cost_status": _text(
            smoothed_lag_primary.get("lag_cost_status"),
            "MISSING",
        ),
        "smoothed_smoothing_event_count": smoothed_backfill_summary.get(
            "smoothing_event_count",
            "MISSING",
        ),
        "smoothed_lag_event_count": smoothed_backfill_summary.get(
            "lag_event_count",
            "MISSING",
        ),
        "smoothed_watch_pack_id": _text(
            smoothed_watch_manifest.get("watch_pack_id"),
            "MISSING",
        ),
        "smoothed_watch_current_decision": _text(
            smoothed_watch_summary.get("current_decision"),
            "MISSING",
        ),
        "smoothed_watch_recommended_action": _text(
            smoothed_watch_summary.get("recommended_action"),
            "MISSING",
        ),
        "smoothed_watch_forward_confirmation_status": _text(
            smoothed_watch_summary.get("forward_confirmation_status"),
            "MISSING",
        ),
        "smoothed_watch_benefit_lag_tradeoff": _text(
            smoothed_watch_summary.get("benefit_lag_tradeoff"),
            "MISSING",
        ),
        "smoothed_watch_sideways_validation_status": _text(
            smoothed_watch_summary.get("sideways_validation_status"),
            "MISSING",
        ),
        "smoothed_watch_recovery_lag_status": _text(
            smoothed_watch_summary.get("recovery_lag_status"),
            "MISSING",
        ),
        "smoothed_owner_update_id": _text(
            smoothed_owner_update_manifest.get("owner_update_id"),
            "MISSING",
        ),
        "smoothed_owner_readiness_decision": _text(
            smoothed_owner_options.get("readiness_decision"),
            "MISSING",
        ),
        "smoothed_owner_recommended_action": _text(
            smoothed_owner_options.get("recommended_owner_action"),
            "MISSING",
        ),
        "smoothed_owner_forward_confirmation_status": _text(
            smoothed_owner_options.get("forward_confirmation_status"),
            "MISSING",
        ),
        "smoothed_promotion_review_id": _text(
            smoothed_promotion_review_manifest.get("promotion_review_id"),
            "MISSING",
        ),
        "smoothed_promotion_readiness_decision": _text(
            smoothed_promotion_evidence.get("readiness_decision"),
            "MISSING",
        ),
        "smoothed_promotion_decision_confidence": _text(
            smoothed_promotion_evidence.get("decision_confidence"),
            "MISSING",
        ),
        "smoothed_promotion_can_enter_owner_review": (
            smoothed_promotion_blocking.get("can_enter_owner_review") is True
        ),
        "smoothed_promotion_supporting_evidence": ",".join(
            _texts(
                [
                    row.get("evidence_id")
                    for row in _records(smoothed_promotion_evidence.get("supporting_evidence"))
                ]
            )
        ),
        "smoothed_promotion_blocking_issues": ",".join(
            _texts(
                [
                    row.get("issue")
                    for row in _records(smoothed_promotion_blocking.get("blocking_issues"))
                ]
            )
        ),
        "primary_research_candidate_gate_id": _text(
            primary_research_gate_manifest.get("gate_id"),
            "MISSING",
        ),
        "primary_research_candidate_gate_decision": _text(
            primary_research_gate_decision.get("gate_decision"),
            "MISSING",
        ),
        "primary_research_candidate_gate_confidence": _text(
            primary_research_gate_decision.get("decision_confidence"),
            "MISSING",
        ),
        "primary_research_candidate_owner_approval_required": (
            primary_research_gate_decision.get("owner_approval_required") is True
        ),
        "primary_research_candidate_update_allowed": _text(
            primary_research_gate_decision.get("can_update_paper_shadow_primary_candidate"),
            "MISSING",
        ),
        "smoothed_forward_binding_id": _text(
            smoothed_forward_binding_manifest.get("binding_id"),
            "MISSING",
        ),
        "smoothed_forward_binding_bound_targets": ",".join(
            _texts(
                [row.get("target_id") for row in _records(smoothed_bound_targets.get("targets"))]
            )
        ),
        "smoothed_forward_binding_watch_only_targets": ",".join(
            _texts(
                [
                    row.get("target_id")
                    for row in _records(smoothed_bound_targets.get("targets"))
                    if row.get("status") == "WATCH_ONLY"
                ]
            )
        ),
        "smoothed_forward_binding_rule_review_ready_when": ",".join(
            _texts(smoothed_forward_requirements.get("rule_review_ready_when"))
        ),
        "paper_shadow_primary_switch_plan_id": _text(
            paper_shadow_primary_switch_manifest.get("switch_plan_id"),
            "MISSING",
        ),
        "paper_shadow_primary_switch_proposed_candidate": _text(
            paper_shadow_switch_plan.get("proposed_primary_research_candidate"),
            "MISSING",
        ),
        "paper_shadow_primary_switch_auto_switch": (
            paper_shadow_switch_plan.get("auto_switch") is True
        ),
        "paper_shadow_primary_switch_rollback_method": _text(
            paper_shadow_switch_plan.get("rollback_method"),
            "MISSING",
        ),
        "paper_shadow_primary_switch_safety_status": _text(
            paper_shadow_switch_safety.get("status"),
            "MISSING",
        ),
        "smoothed_owner_promotion_decision_id": _text(
            smoothed_owner_promotion_manifest.get("decision_id"),
            "MISSING",
        ),
        "smoothed_owner_promotion_decision": _text(
            smoothed_owner_promotion_decision.get("owner_decision"),
            "MISSING",
        ),
        "smoothed_owner_promotion_recommended_action": _text(
            smoothed_owner_promotion_decision.get("recommended_owner_action"),
            "MISSING",
        ),
        "smoothed_owner_promotion_change_allowed": (
            smoothed_owner_promotion_decision.get("paper_shadow_primary_candidate_change_allowed")
            is True
        ),
        "smoothed_forward_progress_id": _text(
            smoothed_forward_progress_manifest.get("progress_id"),
            "MISSING",
        ),
        "smoothed_forward_progress_forward_events": (
            f"{smoothed_forward_progress_summary.get('available_forward_events_total', 'MISSING')}/"
            f"{smoothed_forward_progress_summary.get('required_forward_events_total', 'MISSING')}"
        ),
        "smoothed_forward_progress_sideways_events": (
            f"{smoothed_forward_progress_summary.get('available_sideways_events', 'MISSING')}/"
            f"{smoothed_forward_progress_summary.get('required_sideways_events', 'MISSING')}"
        ),
        "smoothed_forward_progress_recovery_events": (
            f"{smoothed_forward_progress_summary.get('available_recovery_events', 'MISSING')}/"
            f"{smoothed_forward_progress_summary.get('required_recovery_events', 'MISSING')}"
        ),
        "smoothed_forward_progress_statuses": ",".join(
            _texts(
                [
                    f"{row.get('target_id')}={row.get('progress_status')}"
                    for row in smoothed_target_progress
                ]
            )
        ),
        "smoothed_forward_progress_recommendation": _text(
            smoothed_forward_progress_summary.get("summary_recommendation"),
            "MISSING",
        ),
        "smoothed_weekly_dashboard_id": _text(
            smoothed_weekly_dashboard_manifest.get("dashboard_id"),
            "MISSING",
        ),
        "smoothed_weekly_forward_confirmation_status": _text(
            smoothed_dashboard_summary.get("forward_confirmation_status"),
            "MISSING",
        ),
        "smoothed_weekly_ready_for_switch_recheck": (
            smoothed_dashboard_summary.get("ready_for_switch_recheck") is True
        ),
        "smoothed_weekly_recommendation": _text(
            smoothed_dashboard_summary.get("weekly_recommendation"),
            "MISSING",
        ),
        "smoothed_weekly_target_statuses": ",".join(
            _texts(
                [
                    f"{row.get('target_id')}={row.get('status')}"
                    for row in _records(smoothed_target_status_table.get("targets"))
                ]
            )
        ),
        "smoothed_event_monitor_id": _text(
            smoothed_event_monitor_manifest.get("monitor_id"),
            "MISSING",
        ),
        "smoothed_event_sideways_available_required": (
            f"{smoothed_event_sideways.get('available', 'MISSING')}/"
            f"{smoothed_event_sideways.get('required', 'MISSING')}"
        ),
        "smoothed_event_recovery_available_required": (
            f"{smoothed_event_recovery.get('available', 'MISSING')}/"
            f"{smoothed_event_recovery.get('required', 'MISSING')}"
        ),
        "smoothed_event_recovery_lag_status": _text(
            smoothed_event_summary.get("recovery_lag_status"),
            "MISSING",
        ),
        "smoothed_event_lag_warning_count": smoothed_event_summary.get(
            "lag_warning_count",
            "MISSING",
        ),
        "smoothed_switch_readiness_id": _text(
            smoothed_switch_readiness_manifest.get("recheck_id"),
            "MISSING",
        ),
        "smoothed_switch_recheck_decision": _text(
            smoothed_switch_readiness_decision.get("recheck_decision"),
            "MISSING",
        ),
        "smoothed_switch_criteria_not_met": smoothed_readiness_not_met,
        "smoothed_switch_can_execute_switch": (
            smoothed_switch_readiness_decision.get("can_execute_switch") is True
        ),
        "smoothed_owner_renewal_id": _text(
            smoothed_owner_renewal_manifest.get("renewal_id"),
            "MISSING",
        ),
        "smoothed_owner_renewal_previous_decision": _text(
            smoothed_owner_renewal_options.get("previous_owner_decision"),
            "MISSING",
        ),
        "smoothed_owner_renewal_recheck_decision": _text(
            smoothed_owner_renewal_options.get("current_recheck_decision"),
            "MISSING",
        ),
        "smoothed_owner_renewal_recommended_action": _text(
            smoothed_owner_renewal_options.get("recommended_owner_action"),
            "MISSING",
        ),
        "smoothed_owner_renewal_options": smoothed_owner_renewal_option_names,
        "smoothed_daily_emission_id": _text(
            smoothed_daily_emission_manifest.get("emission_id"),
            "MISSING",
        ),
        "smoothed_daily_emission_as_of": _text(
            smoothed_daily_emission_manifest.get("as_of"),
            _text(smoothed_daily_event.get("as_of"), "MISSING"),
        ),
        "smoothed_daily_emission_event_status": _text(
            smoothed_daily_emission_manifest.get("event_status"),
            _text(smoothed_daily_event.get("event_status"), "MISSING"),
        ),
        "smoothed_daily_emission_data_quality": _text(
            smoothed_daily_emission_manifest.get("data_quality"),
            _text(smoothed_emission_data_quality.get("data_quality"), "MISSING"),
        ),
        "smoothed_daily_emission_event_count": smoothed_daily_emission_manifest.get(
            "emitted_event_count",
            "MISSING",
        ),
        "smoothed_outcome_due_id": _text(
            smoothed_outcome_due_manifest.get("due_id"),
            _text(smoothed_outcome_due_summary.get("due_id"), "MISSING"),
        ),
        "smoothed_outcome_due_windows": smoothed_bootstrap_due_window_counts,
        "smoothed_outcome_due_update_ready_count": smoothed_outcome_due_summary.get(
            "update_ready_count",
            "MISSING",
        ),
        "smoothed_outcome_due_blocked_future_as_of": smoothed_outcome_due_summary.get(
            "blocked_future_as_of",
            "MISSING",
        ),
        "smoothed_outcome_update_id": _text(
            smoothed_outcome_update_manifest.get("update_id"),
            _text(smoothed_outcome_update_summary.get("update_id"), "MISSING"),
        ),
        "smoothed_outcome_update_updated_skipped": smoothed_bootstrap_update_counts,
        "smoothed_outcome_update_available_forward_events": (
            smoothed_outcome_update_summary.get(
                "available_forward_events_after_update",
                "MISSING",
            )
        ),
        "smoothed_forward_classification_id": _text(
            smoothed_forward_classification_manifest.get("classification_id"),
            _text(smoothed_forward_classification_summary.get("classification_id"), "MISSING"),
        ),
        "smoothed_forward_classification_events": smoothed_forward_classification_summary.get(
            "events_classified",
            "MISSING",
        ),
        "smoothed_forward_classification_sideways_events": (
            smoothed_forward_classification_summary.get(
                "sideways_events_available",
                "MISSING",
            )
        ),
        "smoothed_forward_classification_recovery_events": (
            smoothed_forward_classification_summary.get(
                "recovery_events_available",
                "MISSING",
            )
        ),
        "smoothed_forward_classification_lag_warnings": (
            smoothed_forward_classification_summary.get(
                "lag_warning_count",
                "MISSING",
            )
        ),
        "smoothed_forward_weekly_run_id": _text(
            smoothed_forward_weekly_run_manifest.get("weekly_run_id"),
            _text(smoothed_forward_weekly_run_summary.get("weekly_run_id"), "MISSING"),
        ),
        "smoothed_forward_weekly_run_emitted_events": (
            smoothed_forward_weekly_run_summary.get("emitted_events", "MISSING")
        ),
        "smoothed_forward_weekly_run_updated_windows": (
            smoothed_forward_weekly_run_summary.get("updated_windows", "MISSING")
        ),
        "smoothed_forward_weekly_run_forward_progress": (
            f"{smoothed_forward_weekly_run_summary.get('available_forward_events', 'MISSING')}/"
            f"{smoothed_forward_weekly_run_summary.get('required_forward_events', 'MISSING')}"
        ),
        "smoothed_forward_weekly_run_sideways_progress": (
            f"{smoothed_forward_weekly_run_summary.get('available_sideways_events', 'MISSING')}/"
            f"{smoothed_forward_weekly_run_summary.get('required_sideways_events', 'MISSING')}"
        ),
        "smoothed_forward_weekly_run_recovery_progress": (
            f"{smoothed_forward_weekly_run_summary.get('available_recovery_events', 'MISSING')}/"
            f"{smoothed_forward_weekly_run_summary.get('required_recovery_events', 'MISSING')}"
        ),
        "smoothed_forward_weekly_run_can_execute_switch": (
            smoothed_forward_weekly_run_summary.get("can_execute_switch") is True
        ),
        "smoothed_forward_weekly_run_recommendation": _text(
            smoothed_forward_weekly_run_summary.get("weekly_recommendation"),
            "MISSING",
        ),
        "smoothed_data_preflight_id": _text(
            smoothed_data_preflight_manifest.get("preflight_id"),
            "MISSING",
        ),
        "smoothed_data_preflight_requested_as_of": _text(
            smoothed_data_preflight_manifest.get("requested_as_of"),
            _text(smoothed_data_freshness_snapshot.get("requested_as_of"), "MISSING"),
        ),
        "smoothed_data_preflight_freshness_status": _text(
            smoothed_data_preflight_manifest.get("freshness_status"),
            _text(smoothed_data_freshness_snapshot.get("freshness_status"), "MISSING"),
        ),
        "smoothed_data_preflight_latest_valid_as_of": _text(
            smoothed_data_preflight_manifest.get("latest_valid_as_of"),
            _text(smoothed_data_freshness_snapshot.get("latest_valid_as_of"), "MISSING"),
        ),
        "smoothed_data_preflight_validate_data_status": _text(
            smoothed_data_preflight_manifest.get("validate_data_status"),
            _text(smoothed_data_freshness_snapshot.get("validate_data_status"), "MISSING"),
        ),
        "smoothed_data_preflight_blocking_errors": smoothed_preflight_blocking_errors,
        "smoothed_data_preflight_latest_available_fallback_commands": (
            smoothed_preflight_runnable_latest_count
        ),
        "smoothed_latest_emission_id": _text(
            smoothed_latest_emission_manifest.get("latest_emission_id"),
            "MISSING",
        ),
        "smoothed_latest_emission_resolved_as_of": _text(
            smoothed_latest_emission_manifest.get("resolved_as_of"),
            _text(smoothed_latest_emission_resolution.get("resolved_as_of"), "MISSING"),
        ),
        "smoothed_latest_emission_event_status": _text(
            smoothed_latest_emission_links.get("event_status"),
            "MISSING",
        ),
        "smoothed_latest_emission_emitted_event_count": smoothed_latest_emission_links.get(
            "emitted_event_count",
            "MISSING",
        ),
        "smoothed_latest_emission_outcome_update_allowed": (
            smoothed_latest_emission_resolution.get("outcome_update_allowed")
        ),
        "smoothed_latest_emission_due_scan_allowed": (
            smoothed_latest_emission_resolution.get("due_scan_allowed")
        ),
        "smoothed_latest_emission_future_data_used": (
            smoothed_latest_emission_resolution.get("future_data_used")
        ),
        "smoothed_blocked_explain_id": _text(
            smoothed_blocked_explain_manifest.get("explain_id"),
            "MISSING",
        ),
        "smoothed_blocked_explain_command_count": smoothed_blocked_explain_manifest.get(
            "blocked_command_count",
            "MISSING",
        ),
        "smoothed_blocked_explain_commands": smoothed_blocked_command_names,
        "smoothed_refresh_plan_id": _text(
            smoothed_refresh_plan_manifest.get("refresh_plan_id"),
            "MISSING",
        ),
        "smoothed_refresh_plan_required_sources": smoothed_refresh_required_sources,
        "smoothed_refresh_plan_stale_source_count": smoothed_refresh_stale_source_count,
        "smoothed_refresh_plan_all_required_sources_fresh": (
            smoothed_source_refresh_requirements.get("all_required_sources_fresh")
        ),
        "smoothed_refresh_plan_rerun_allowed_now": smoothed_rerun_command_plan.get(
            "rerun_allowed_now"
        ),
        "smoothed_refresh_plan_external_refresh_executed": (
            smoothed_rerun_command_plan.get("external_refresh_executed")
        ),
        "smoothed_bootstrap_retry_id": _text(
            smoothed_bootstrap_retry_manifest.get("retry_id"),
            "MISSING",
        ),
        "smoothed_bootstrap_retry_status": _text(
            smoothed_bootstrap_retry_manifest.get("retry_status"),
            _text(smoothed_retry_summary.get("retry_status"), "MISSING"),
        ),
        "smoothed_bootstrap_retry_preflight_status": _text(
            smoothed_retry_preflight_result.get("preflight_status"),
            "MISSING",
        ),
        "smoothed_bootstrap_retry_updated_windows": smoothed_retry_summary.get(
            "updated_windows",
            "MISSING",
        ),
        "smoothed_bootstrap_retry_emitted_events": smoothed_retry_summary.get(
            "emitted_events",
            "MISSING",
        ),
        "smoothed_bootstrap_retry_can_execute_switch": (
            smoothed_retry_summary.get("can_execute_switch") is True
        ),
        "smoothed_bootstrap_retry_step_statuses": smoothed_retry_step_statuses,
        "smoothed_source_refresh_id": _text(
            smoothed_source_refresh_manifest.get("refresh_execution_id"),
            "MISSING",
        ),
        "smoothed_source_refresh_status": _text(
            smoothed_source_refresh_manifest.get("refresh_status"),
            _text(smoothed_source_refresh_results.get("refresh_status"), "MISSING"),
        ),
        "smoothed_source_refresh_ready_source_count": smoothed_source_refresh_ready_count,
        "smoothed_source_refresh_failed_sources": smoothed_source_refresh_failed_sources,
        "smoothed_source_refresh_external_refresh_executed": (
            smoothed_source_refresh_results.get("external_refresh_executed")
        ),
        "smoothed_post_refresh_id": _text(
            smoothed_post_refresh_manifest.get("post_refresh_id"),
            "MISSING",
        ),
        "smoothed_post_refresh_validate_data_status": _text(
            smoothed_post_refresh_manifest.get("validate_data_status"),
            "MISSING",
        ),
        "smoothed_post_refresh_freshness_status": _text(
            smoothed_post_refresh_manifest.get("freshness_status"),
            _text(smoothed_post_refresh_preflight.get("freshness_status"), "MISSING"),
        ),
        "smoothed_post_refresh_retry_decision": _text(
            smoothed_post_refresh_manifest.get("retry_decision"),
            _text(smoothed_post_refresh_decision.get("retry_decision"), "MISSING"),
        ),
        "smoothed_post_refresh_blocking_errors": smoothed_post_refresh_blocking_errors,
        "smoothed_retry_resume_id": _text(
            smoothed_retry_resume_manifest.get("resume_id"),
            "MISSING",
        ),
        "smoothed_retry_resume_status": _text(
            smoothed_retry_resume_manifest.get("resume_status"),
            _text(smoothed_retry_resume_summary.get("resume_status"), "MISSING"),
        ),
        "smoothed_retry_resume_can_resume": (
            smoothed_retry_resume_precondition.get("can_resume") is True
        ),
        "smoothed_retry_resume_updated_windows": smoothed_retry_resume_summary.get(
            "updated_windows",
            "MISSING",
        ),
        "smoothed_retry_resume_can_execute_switch": (
            smoothed_retry_resume_summary.get("can_execute_switch") is True
        ),
        "smoothed_sample_growth_id": _text(
            smoothed_sample_growth_manifest.get("growth_id"),
            "MISSING",
        ),
        "smoothed_sample_growth_status": _text(
            smoothed_sample_growth_manifest.get("growth_status"),
            _text(smoothed_sample_growth_summary.get("growth_status"), "MISSING"),
        ),
        "smoothed_sample_growth_forward_delta": smoothed_sample_growth_delta.get(
            "forward_events",
            "MISSING",
        ),
        "smoothed_sample_growth_sideways_delta": smoothed_sample_growth_delta.get(
            "sideways_events",
            "MISSING",
        ),
        "smoothed_sample_growth_recovery_delta": smoothed_sample_growth_delta.get(
            "recovery_events",
            "MISSING",
        ),
        "smoothed_sample_growth_forward_progress": smoothed_sample_growth_progress.get(
            "forward",
            "MISSING",
        ),
        "smoothed_sample_growth_sideways_progress": smoothed_sample_growth_progress.get(
            "sideways",
            "MISSING",
        ),
        "smoothed_sample_growth_recovery_progress": smoothed_sample_growth_progress.get(
            "recovery",
            "MISSING",
        ),
        "smoothed_data_readiness_id": _text(
            smoothed_data_readiness_manifest.get("readiness_id"),
            "MISSING",
        ),
        "smoothed_data_readiness_current_status": _text(
            smoothed_data_readiness_manifest.get("current_status"),
            _text(smoothed_data_readiness_summary.get("current_status"), "MISSING"),
        ),
        "smoothed_data_readiness_recommended_owner_action": _text(
            smoothed_data_readiness_manifest.get("recommended_owner_action"),
            _text(
                smoothed_data_readiness_summary.get("recommended_owner_action"),
                "MISSING",
            ),
        ),
        "smoothed_data_readiness_source_statuses": smoothed_data_readiness_source_statuses,
        "smoothed_data_readiness_retry_status": _text(
            smoothed_data_readiness_summary.get("retry_status"),
            "MISSING",
        ),
        "smoothed_data_readiness_sample_growth_status": _text(
            smoothed_data_readiness_summary.get("sample_growth_status"),
            "MISSING",
        ),
        "smoothed_data_readiness_forward_progress": _text(
            smoothed_data_readiness_summary.get("forward_progress"),
            "MISSING",
        ),
        "smoothed_data_readiness_sideways_progress": _text(
            smoothed_data_readiness_summary.get("sideways_progress"),
            "MISSING",
        ),
        "smoothed_data_readiness_recovery_progress": _text(
            smoothed_data_readiness_summary.get("recovery_progress"),
            "MISSING",
        ),
        "smoothed_path": "" if smoothed_limited_path is None else str(smoothed_limited_path),
        "smoothed_backfill_path": (
            "" if smoothed_backfill_path is None else str(smoothed_backfill_path)
        ),
        "smoothed_comparison_path": (
            "" if smoothed_comparison_path is None else str(smoothed_comparison_path)
        ),
        "smoothed_review_path": "" if smoothed_review_path is None else str(smoothed_review_path),
        "smoothed_watch_pack_path": "" if smoothed_watch_path is None else str(smoothed_watch_path),
        "smoothed_owner_update_path": (
            "" if smoothed_owner_update_path is None else str(smoothed_owner_update_path)
        ),
        "smoothed_promotion_review_path": (
            "" if smoothed_promotion_review_path is None else str(smoothed_promotion_review_path)
        ),
        "primary_research_candidate_gate_path": (
            "" if primary_research_gate_path is None else str(primary_research_gate_path)
        ),
        "smoothed_forward_binding_path": (
            "" if smoothed_forward_binding_path is None else str(smoothed_forward_binding_path)
        ),
        "paper_shadow_primary_switch_path": (
            ""
            if paper_shadow_primary_switch_path is None
            else str(paper_shadow_primary_switch_path)
        ),
        "smoothed_owner_promotion_path": (
            "" if smoothed_owner_promotion_path is None else str(smoothed_owner_promotion_path)
        ),
        "smoothed_forward_progress_path": (
            "" if smoothed_forward_progress_path is None else str(smoothed_forward_progress_path)
        ),
        "smoothed_weekly_dashboard_path": (
            "" if smoothed_weekly_dashboard_path is None else str(smoothed_weekly_dashboard_path)
        ),
        "smoothed_event_monitor_path": (
            "" if smoothed_event_monitor_path is None else str(smoothed_event_monitor_path)
        ),
        "smoothed_switch_readiness_path": (
            "" if smoothed_switch_readiness_path is None else str(smoothed_switch_readiness_path)
        ),
        "smoothed_owner_renewal_path": (
            "" if smoothed_owner_renewal_path is None else str(smoothed_owner_renewal_path)
        ),
        "smoothed_daily_emission_path": (
            "" if smoothed_daily_emission_path is None else str(smoothed_daily_emission_path)
        ),
        "smoothed_outcome_due_path": (
            "" if smoothed_outcome_due_path is None else str(smoothed_outcome_due_path)
        ),
        "smoothed_outcome_update_path": (
            "" if smoothed_outcome_update_path is None else str(smoothed_outcome_update_path)
        ),
        "smoothed_forward_classification_path": (
            ""
            if smoothed_forward_classification_path is None
            else str(smoothed_forward_classification_path)
        ),
        "smoothed_forward_weekly_run_path": (
            ""
            if smoothed_forward_weekly_run_path is None
            else str(smoothed_forward_weekly_run_path)
        ),
        "smoothed_data_preflight_path": (
            "" if smoothed_data_preflight_path is None else str(smoothed_data_preflight_path)
        ),
        "smoothed_latest_emission_path": (
            "" if smoothed_latest_emission_path is None else str(smoothed_latest_emission_path)
        ),
        "smoothed_blocked_explain_path": (
            "" if smoothed_blocked_explain_path is None else str(smoothed_blocked_explain_path)
        ),
        "smoothed_refresh_plan_path": (
            "" if smoothed_refresh_plan_path is None else str(smoothed_refresh_plan_path)
        ),
        "smoothed_bootstrap_retry_path": (
            "" if smoothed_bootstrap_retry_path is None else str(smoothed_bootstrap_retry_path)
        ),
        "smoothed_source_refresh_path": (
            "" if smoothed_source_refresh_path is None else str(smoothed_source_refresh_path)
        ),
        "smoothed_post_refresh_path": (
            "" if smoothed_post_refresh_path is None else str(smoothed_post_refresh_path)
        ),
        "smoothed_retry_resume_path": (
            "" if smoothed_retry_resume_path is None else str(smoothed_retry_resume_path)
        ),
        "smoothed_sample_growth_path": (
            "" if smoothed_sample_growth_path is None else str(smoothed_sample_growth_path)
        ),
        "smoothed_data_readiness_path": (
            "" if smoothed_data_readiness_path is None else str(smoothed_data_readiness_path)
        ),
        "experiment_triage_id": _text(
            experiment_triage_manifest.get("triage_id"),
            "MISSING",
        ),
        "experiment_batch_id": _text(
            experiment_triage_manifest.get("batch_id"),
            "MISSING",
        ),
        "experiment_matrix_id": _text(
            experiment_triage_manifest.get("matrix_id"),
            "MISSING",
        ),
        "experiment_top_variant": experiment_top_variant,
        "experiment_promote_count": experiment_triage_summary.get("promote_count", "MISSING"),
        "experiment_keep_testing_count": experiment_triage_summary.get(
            "keep_testing_count",
            "MISSING",
        ),
        "experiment_reject_count": experiment_triage_summary.get("reject_count", "MISSING"),
        "experiment_top_promoted_variants": top_promoted_variants,
        "top_variant_interpretation_id": _text(
            top_variant_interpretation_manifest.get("interpretation_id"),
            "MISSING",
        ),
        "best_experiment_variant": _text(
            top_variant_interpretation_manifest.get("recommended_variant"),
            experiment_top_variant,
        ),
        "top_variant_solved_failure_modes": ",".join(_texts(best_explanation.get("why_it_helped"))),
        "top_variant_expected_costs": ",".join(_texts(best_explanation.get("what_it_costs"))),
        "method_promotion_plan_id": _text(
            method_promotion_plan_manifest.get("promotion_plan_id"),
            "MISSING",
        ),
        "proposed_method_names": proposed_method_names,
        "promotion_implementation_scope": _text(
            method_promotion_plan_manifest.get("implementation_scope"),
            "MISSING",
        ),
        "promotion_next_action": _text(promoted_method_specs.get("next_action"), "MISSING"),
        "experiment_triage_path": (
            "" if experiment_triage_path is None else str(experiment_triage_path)
        ),
        "top_variant_interpretation_path": (
            "" if top_variant_interpretation_path is None else str(top_variant_interpretation_path)
        ),
        "method_promotion_plan_path": (
            "" if method_promotion_plan_path is None else str(method_promotion_plan_path)
        ),
        "data_quality_status": _text(performance_summary.get("data_quality_status"), "MISSING"),
        "best_return_method": _text(performance_summary.get("best_return_method"), "MISSING"),
        "best_drawdown_method": _text(performance_summary.get("best_drawdown_method"), "MISSING"),
        "best_risk_adjusted_method": _text(
            performance_summary.get("best_risk_adjusted_method"),
            "MISSING",
        ),
        "limited_adjustment_vs_static": limited_row.get(
            "relative_to_static_baseline",
            "MISSING",
        ),
        "consensus_target_vs_no_trade": consensus_row.get(
            "relative_to_no_trade",
            "MISSING",
        ),
        "total_turnover": rebalance_summary.get("total_turnover", "MISSING"),
        "skipped_methods": ",".join(_texts(rebalance_summary.get("skipped_methods"))),
        "broker_action_allowed": review_manifest.get("broker_action_allowed") is True,
        "broker_action_taken": review_manifest.get("broker_action_taken") is True,
        "not_official_target_weights": review_manifest.get("not_official_target_weights") is True,
        "research_target_only": review_manifest.get("research_target_only") is True,
        "paper_shadow_only": review_manifest.get("paper_shadow_only") is True,
        "production_effect": production_effect,
        "safety_status": safety_status,
        "next_action": _text(
            selection_decision.get("next_action"),
            _text(decision.get("next_action"), "continue_paper_shadow_observation"),
        ),
        "system_target_review_path": "" if review_path is None else str(review_path),
        "model_target_path": "" if target_path is None else str(target_path),
        "paper_shadow_path": "" if paper_path is None else str(paper_path),
        "model_rebalance_path": "" if rebalance_path is None else str(rebalance_path),
        "paper_shadow_performance_path": (
            "" if performance_path is None else str(performance_path)
        ),
        "broker_action": _text(review_manifest.get("broker_action"), "none"),
        "limitation": (
            "Reader Brief only reads latest system target artifacts; it does not "
            "generate target weights, run paper shadow performance, mutate official "
            "target weights, or trigger broker action."
        ),
    }


def _missing_etf_dynamic_v3_system_target_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Dynamic Rescue System Target Portfolio: no latest system target review pack found."
        ),
        "target_id": "MISSING",
        "paper_shadow_id": "MISSING",
        "rebalance_id": "MISSING",
        "performance_id": "MISSING",
        "review_id": "MISSING",
        "generated_methods": "",
        "tracked_methods": "",
        "recommended_research_method": "MISSING",
        "decision_status": "MISSING",
        "selection_review_id": "MISSING",
        "secondary_research_methods": "",
        "reference_only_methods": "",
        "selection_review_path": "",
        "hardening_id": "MISSING",
        "hardening_decision": "MISSING",
        "hardening_decision_confidence": "MISSING",
        "hardening_blocking_issues": "",
        "hardening_path": "",
        "refined_proposal_id": "MISSING",
        "refined_recommended_next_step": "MISSING",
        "refined_confidence": "MISSING",
        "refined_proposed_next_methods": "",
        "refined_proposal_path": "",
        "risk_capped_id": "MISSING",
        "risk_capped_backfill_id": "MISSING",
        "risk_capped_comparison_id": "MISSING",
        "risk_capped_review_id": "MISSING",
        "risk_capped_decision": "MISSING",
        "risk_capped_confidence": "MISSING",
        "risk_capped_improvements_vs_limited": "",
        "risk_capped_requires_forward_confirmation": False,
        "risk_capped_return_delta_vs_limited": "MISSING",
        "risk_capped_drawdown_delta_vs_limited": "MISSING",
        "risk_capped_semiconductor_exposure_delta": "MISSING",
        "risk_capped_rolling_consistency_delta": "MISSING",
        "risk_capped_stability_conclusion": "MISSING",
        "risk_capped_generated_cap_event_count": "MISSING",
        "risk_capped_backfill_cap_event_count": "MISSING",
        "risk_capped_reallocated_to_cash": "MISSING",
        "risk_capped_path": "",
        "risk_capped_backfill_path": "",
        "risk_capped_comparison_path": "",
        "risk_capped_review_path": "",
        "smoothed_id": "MISSING",
        "smoothed_backfill_id": "MISSING",
        "smoothed_comparison_id": "MISSING",
        "smoothed_review_id": "MISSING",
        "smoothed_decision": "MISSING",
        "smoothed_recommended_method": "MISSING",
        "smoothed_secondary_method": "MISSING",
        "smoothed_confidence": "MISSING",
        "smoothed_improvements_vs_limited": "",
        "smoothed_lag_risk": "MISSING",
        "smoothed_requires_forward_confirmation": False,
        "smoothed_return_delta_vs_limited": "MISSING",
        "smoothed_drawdown_delta_vs_limited": "MISSING",
        "smoothed_turnover_delta_vs_limited": "MISSING",
        "smoothed_rolling_consistency_delta": "MISSING",
        "smoothed_stability_conclusion": "MISSING",
        "smoothed_lag_cost_status": "MISSING",
        "smoothed_smoothing_event_count": "MISSING",
        "smoothed_lag_event_count": "MISSING",
        "smoothed_watch_pack_id": "MISSING",
        "smoothed_watch_current_decision": "MISSING",
        "smoothed_watch_recommended_action": "MISSING",
        "smoothed_watch_forward_confirmation_status": "MISSING",
        "smoothed_watch_benefit_lag_tradeoff": "MISSING",
        "smoothed_watch_sideways_validation_status": "MISSING",
        "smoothed_watch_recovery_lag_status": "MISSING",
        "smoothed_owner_update_id": "MISSING",
        "smoothed_owner_readiness_decision": "MISSING",
        "smoothed_owner_recommended_action": "MISSING",
        "smoothed_owner_forward_confirmation_status": "MISSING",
        "smoothed_promotion_review_id": "MISSING",
        "smoothed_promotion_readiness_decision": "MISSING",
        "smoothed_promotion_decision_confidence": "MISSING",
        "smoothed_promotion_can_enter_owner_review": False,
        "smoothed_promotion_supporting_evidence": "",
        "smoothed_promotion_blocking_issues": "",
        "primary_research_candidate_gate_id": "MISSING",
        "primary_research_candidate_gate_decision": "MISSING",
        "primary_research_candidate_gate_confidence": "MISSING",
        "primary_research_candidate_owner_approval_required": False,
        "primary_research_candidate_update_allowed": "MISSING",
        "smoothed_forward_binding_id": "MISSING",
        "smoothed_forward_binding_bound_targets": "",
        "smoothed_forward_binding_watch_only_targets": "",
        "smoothed_forward_binding_rule_review_ready_when": "",
        "paper_shadow_primary_switch_plan_id": "MISSING",
        "paper_shadow_primary_switch_proposed_candidate": "MISSING",
        "paper_shadow_primary_switch_auto_switch": False,
        "paper_shadow_primary_switch_rollback_method": "MISSING",
        "paper_shadow_primary_switch_safety_status": "MISSING",
        "smoothed_owner_promotion_decision_id": "MISSING",
        "smoothed_owner_promotion_decision": "MISSING",
        "smoothed_owner_promotion_recommended_action": "MISSING",
        "smoothed_owner_promotion_change_allowed": False,
        "smoothed_forward_progress_id": "MISSING",
        "smoothed_forward_progress_forward_events": "MISSING/MISSING",
        "smoothed_forward_progress_sideways_events": "MISSING/MISSING",
        "smoothed_forward_progress_recovery_events": "MISSING/MISSING",
        "smoothed_forward_progress_statuses": "",
        "smoothed_forward_progress_recommendation": "MISSING",
        "smoothed_weekly_dashboard_id": "MISSING",
        "smoothed_weekly_forward_confirmation_status": "MISSING",
        "smoothed_weekly_ready_for_switch_recheck": False,
        "smoothed_weekly_recommendation": "MISSING",
        "smoothed_weekly_target_statuses": "",
        "smoothed_event_monitor_id": "MISSING",
        "smoothed_event_sideways_available_required": "MISSING/MISSING",
        "smoothed_event_recovery_available_required": "MISSING/MISSING",
        "smoothed_event_recovery_lag_status": "MISSING",
        "smoothed_event_lag_warning_count": "MISSING",
        "smoothed_switch_readiness_id": "MISSING",
        "smoothed_switch_recheck_decision": "MISSING",
        "smoothed_switch_criteria_not_met": "",
        "smoothed_switch_can_execute_switch": False,
        "smoothed_owner_renewal_id": "MISSING",
        "smoothed_owner_renewal_previous_decision": "MISSING",
        "smoothed_owner_renewal_recheck_decision": "MISSING",
        "smoothed_owner_renewal_recommended_action": "MISSING",
        "smoothed_owner_renewal_options": "",
        "smoothed_daily_emission_id": "MISSING",
        "smoothed_daily_emission_as_of": "MISSING",
        "smoothed_daily_emission_event_status": "MISSING",
        "smoothed_daily_emission_data_quality": "MISSING",
        "smoothed_daily_emission_event_count": "MISSING",
        "smoothed_outcome_due_id": "MISSING",
        "smoothed_outcome_due_windows": "MISSING/MISSING",
        "smoothed_outcome_due_update_ready_count": "MISSING",
        "smoothed_outcome_due_blocked_future_as_of": "MISSING",
        "smoothed_outcome_update_id": "MISSING",
        "smoothed_outcome_update_updated_skipped": "MISSING/MISSING",
        "smoothed_outcome_update_available_forward_events": "MISSING",
        "smoothed_forward_classification_id": "MISSING",
        "smoothed_forward_classification_events": "MISSING",
        "smoothed_forward_classification_sideways_events": "MISSING",
        "smoothed_forward_classification_recovery_events": "MISSING",
        "smoothed_forward_classification_lag_warnings": "MISSING",
        "smoothed_forward_weekly_run_id": "MISSING",
        "smoothed_forward_weekly_run_emitted_events": "MISSING",
        "smoothed_forward_weekly_run_updated_windows": "MISSING",
        "smoothed_forward_weekly_run_forward_progress": "MISSING/MISSING",
        "smoothed_forward_weekly_run_sideways_progress": "MISSING/MISSING",
        "smoothed_forward_weekly_run_recovery_progress": "MISSING/MISSING",
        "smoothed_forward_weekly_run_can_execute_switch": False,
        "smoothed_forward_weekly_run_recommendation": "MISSING",
        "smoothed_data_preflight_id": "MISSING",
        "smoothed_data_preflight_requested_as_of": "MISSING",
        "smoothed_data_preflight_freshness_status": "MISSING",
        "smoothed_data_preflight_latest_valid_as_of": "MISSING",
        "smoothed_data_preflight_validate_data_status": "MISSING",
        "smoothed_data_preflight_blocking_errors": "",
        "smoothed_data_preflight_latest_available_fallback_commands": "MISSING",
        "smoothed_latest_emission_id": "MISSING",
        "smoothed_latest_emission_resolved_as_of": "MISSING",
        "smoothed_latest_emission_event_status": "MISSING",
        "smoothed_latest_emission_emitted_event_count": "MISSING",
        "smoothed_latest_emission_outcome_update_allowed": "MISSING",
        "smoothed_latest_emission_due_scan_allowed": "MISSING",
        "smoothed_latest_emission_future_data_used": "MISSING",
        "smoothed_blocked_explain_id": "MISSING",
        "smoothed_blocked_explain_command_count": "MISSING",
        "smoothed_blocked_explain_commands": "",
        "smoothed_refresh_plan_id": "MISSING",
        "smoothed_refresh_plan_required_sources": "",
        "smoothed_refresh_plan_stale_source_count": "MISSING",
        "smoothed_refresh_plan_all_required_sources_fresh": "MISSING",
        "smoothed_refresh_plan_rerun_allowed_now": "MISSING",
        "smoothed_refresh_plan_external_refresh_executed": "MISSING",
        "smoothed_bootstrap_retry_id": "MISSING",
        "smoothed_bootstrap_retry_status": "MISSING",
        "smoothed_bootstrap_retry_preflight_status": "MISSING",
        "smoothed_bootstrap_retry_updated_windows": "MISSING",
        "smoothed_bootstrap_retry_emitted_events": "MISSING",
        "smoothed_bootstrap_retry_can_execute_switch": False,
        "smoothed_bootstrap_retry_step_statuses": "",
        "smoothed_source_refresh_id": "MISSING",
        "smoothed_source_refresh_status": "MISSING",
        "smoothed_source_refresh_ready_source_count": "MISSING",
        "smoothed_source_refresh_failed_sources": "",
        "smoothed_source_refresh_external_refresh_executed": "MISSING",
        "smoothed_post_refresh_id": "MISSING",
        "smoothed_post_refresh_validate_data_status": "MISSING",
        "smoothed_post_refresh_freshness_status": "MISSING",
        "smoothed_post_refresh_retry_decision": "MISSING",
        "smoothed_post_refresh_blocking_errors": "",
        "smoothed_retry_resume_id": "MISSING",
        "smoothed_retry_resume_status": "MISSING",
        "smoothed_retry_resume_can_resume": False,
        "smoothed_retry_resume_updated_windows": "MISSING",
        "smoothed_retry_resume_can_execute_switch": False,
        "smoothed_sample_growth_id": "MISSING",
        "smoothed_sample_growth_status": "MISSING",
        "smoothed_sample_growth_forward_delta": "MISSING",
        "smoothed_sample_growth_sideways_delta": "MISSING",
        "smoothed_sample_growth_recovery_delta": "MISSING",
        "smoothed_sample_growth_forward_progress": "MISSING",
        "smoothed_sample_growth_sideways_progress": "MISSING",
        "smoothed_sample_growth_recovery_progress": "MISSING",
        "smoothed_data_readiness_id": "MISSING",
        "smoothed_data_readiness_current_status": "MISSING",
        "smoothed_data_readiness_recommended_owner_action": "MISSING",
        "smoothed_data_readiness_source_statuses": "",
        "smoothed_data_readiness_retry_status": "MISSING",
        "smoothed_data_readiness_sample_growth_status": "MISSING",
        "smoothed_data_readiness_forward_progress": "MISSING",
        "smoothed_data_readiness_sideways_progress": "MISSING",
        "smoothed_data_readiness_recovery_progress": "MISSING",
        "smoothed_path": "",
        "smoothed_backfill_path": "",
        "smoothed_comparison_path": "",
        "smoothed_review_path": "",
        "smoothed_watch_pack_path": "",
        "smoothed_owner_update_path": "",
        "smoothed_promotion_review_path": "",
        "primary_research_candidate_gate_path": "",
        "smoothed_forward_binding_path": "",
        "paper_shadow_primary_switch_path": "",
        "smoothed_owner_promotion_path": "",
        "smoothed_forward_progress_path": "",
        "smoothed_weekly_dashboard_path": "",
        "smoothed_event_monitor_path": "",
        "smoothed_switch_readiness_path": "",
        "smoothed_owner_renewal_path": "",
        "smoothed_daily_emission_path": "",
        "smoothed_outcome_due_path": "",
        "smoothed_outcome_update_path": "",
        "smoothed_forward_classification_path": "",
        "smoothed_forward_weekly_run_path": "",
        "smoothed_data_preflight_path": "",
        "smoothed_latest_emission_path": "",
        "smoothed_blocked_explain_path": "",
        "smoothed_refresh_plan_path": "",
        "smoothed_bootstrap_retry_path": "",
        "smoothed_source_refresh_path": "",
        "smoothed_post_refresh_path": "",
        "smoothed_retry_resume_path": "",
        "smoothed_sample_growth_path": "",
        "smoothed_data_readiness_path": "",
        "experiment_triage_id": "MISSING",
        "experiment_batch_id": "MISSING",
        "experiment_matrix_id": "MISSING",
        "experiment_top_variant": "MISSING",
        "experiment_promote_count": "MISSING",
        "experiment_keep_testing_count": "MISSING",
        "experiment_reject_count": "MISSING",
        "experiment_top_promoted_variants": "",
        "top_variant_interpretation_id": "MISSING",
        "best_experiment_variant": "MISSING",
        "top_variant_solved_failure_modes": "",
        "top_variant_expected_costs": "",
        "method_promotion_plan_id": "MISSING",
        "proposed_method_names": "",
        "promotion_implementation_scope": "MISSING",
        "promotion_next_action": "MISSING",
        "experiment_triage_path": "",
        "top_variant_interpretation_path": "",
        "method_promotion_plan_path": "",
        "data_quality_status": "MISSING",
        "best_return_method": "MISSING",
        "best_drawdown_method": "MISSING",
        "best_risk_adjusted_method": "MISSING",
        "limited_adjustment_vs_static": "MISSING",
        "consensus_target_vs_no_trade": "MISSING",
        "total_turnover": "MISSING",
        "skipped_methods": "",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "not_official_target_weights": True,
        "research_target_only": True,
        "paper_shadow_only": True,
        "production_effect": PRODUCTION_EFFECT,
        "safety_status": "MISSING",
        "next_action": "generate_system_target_review_pack",
        "system_target_review_path": "",
        "model_target_path": "",
        "paper_shadow_path": "",
        "model_rebalance_path": "",
        "paper_shadow_performance_path": "",
        "broker_action": "none",
        "limitation": (
            "Reader Brief only reads latest system target artifacts; it cannot "
            "backfill missing review evidence."
        ),
    }


def _etf_dynamic_v3_system_target_safety_status(
    *payloads: Mapping[str, Any],
) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    unsafe = any(
        payload.get("broker_action_allowed") is True
        or payload.get("broker_action_taken") is True
        or payload.get("order_ticket_generated") is True
        or payload.get("production_state_mutated") is True
        or payload.get("baseline_config_mutated") is True
        or payload.get("official_target_weights_mutated") is True
        or payload.get("production_candidate_generated") is True
        or payload.get("automatic_candidate_promotion") is True
        or payload.get("auto_apply") is True
        or _text(payload.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT
        for payload in material
    )
    if not unsafe:
        return (
            "research_target_only=true; paper_shadow_only=true; "
            "not_official_target_weights=true; production_effect=none; "
            "broker_action_allowed=false; broker_action_taken=false; "
            "order_ticket_generated=false"
        )
    return "SAFETY_REVIEW_REQUIRED"


def _first_method_row(rows: list[dict[str, Any]], method: str) -> dict[str, Any]:
    for row in rows:
        if _text(row.get("target_method")) == method:
            return row
    return {}


def _etf_dynamic_v3_parameter_research_summary(
    report_index: Mapping[str, Any],
) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_v3_parameter_research_summary()
    leaderboard_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_v3_parameter_sweep_leaderboard",
    )
    indexed_promotion_path = _report_index_artifact_path(
        report_index,
        "etf_dynamic_v3_promotion_pack",
    )
    shadow_monitor_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shadow_monitor_report",
        ),
        "shadow_monitor_manifest.json",
    )
    candidate_evidence_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_evidence_summary",
        ),
        "evidence_summary_manifest.json",
    )
    observe_pool_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_observe_pool",
        ),
        "observe_pool_manifest.json",
    )
    overnight_readiness_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_overnight_readiness",
        ),
        "overnight_readiness_manifest.json",
    )
    research_decision_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_research_decision",
        ),
        "research_decision_manifest.json",
    )
    evidence_diagnosis_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_evidence_diagnosis",
        ),
        "diagnosis_manifest.json",
    )
    gate_impact_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_gate_impact",
        ),
        "gate_impact_manifest.json",
    )
    gate_policy_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_gate_policy",
        ),
        "gate_policy_manifest.json",
    )
    candidate_recovery_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_candidate_recovery",
        ),
        "recovery_manifest.json",
    )
    research_decision_update_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_research_decision_update",
        ),
        "decision_update_manifest.json",
    )
    shortlist_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shortlist",
        ),
        "shortlist_manifest.json",
    )
    candidate_cluster_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_candidate_cluster",
        ),
        "cluster_manifest.json",
    )
    shadow_shortlist_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shadow_shortlist",
        ),
        "shadow_shortlist_manifest.json",
    )
    position_advisory_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_position_advisory",
        ),
        "position_advisory_manifest.json",
    )
    position_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_position_review",
        ),
        "position_review_manifest.json",
    )
    shadow_monitor_run_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shadow_monitor_run",
        ),
        "shadow_monitor_manifest.json",
    )
    portfolio_snapshot_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_portfolio_snapshot",
        ),
        "snapshot_manifest.json",
    )
    position_advisory_daily_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_position_advisory_daily",
        ),
        "daily_advisory_manifest.json",
    )
    consensus_drift_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_consensus_drift",
        ),
        "consensus_drift_manifest.json",
    )
    owner_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_owner_review",
        ),
        "latest_owner_review.json",
    )
    paper_portfolio_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_portfolio",
        ),
        "paper_portfolio_manifest.json",
    )
    advisory_outcome_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_advisory_outcome",
        ),
        "advisory_outcome_manifest.json",
    )
    owner_attribution_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_owner_attribution",
        ),
        "owner_attribution_manifest.json",
    )
    shadow_aging_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shadow_aging",
        ),
        "shadow_aging_manifest.json",
    )
    weekly_advisory_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_weekly_advisory_review",
        ),
        "weekly_review_manifest.json",
    )
    replay_inventory_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_replay_inventory",
        ),
        "replay_inventory_manifest.json",
    )
    historical_replay_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_historical_replay",
        ),
        "historical_replay_manifest.json",
    )
    backfilled_outcome_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_backfilled_outcome",
        ),
        "backfill_manifest.json",
    )
    historical_paper_sim_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_historical_paper_sim",
        ),
        "historical_paper_sim_manifest.json",
    )
    replay_performance_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_replay_performance_review",
        ),
        "replay_performance_manifest.json",
    )
    replay_forward_bridge_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_replay_forward_bridge",
        ),
        "bridge_manifest.json",
    )
    outcome_dashboard_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_outcome_dashboard",
        ),
        "outcome_dashboard_manifest.json",
    )
    outcome_update_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_outcome_update_review",
        ),
        "outcome_update_review_manifest.json",
    )
    outcome_update_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_outcome_update",
        ),
        "outcome_update_manifest.json",
    )
    rolling_evidence_refresh_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_rolling_evidence_refresh",
        ),
        "rolling_refresh_manifest.json",
    )
    evidence_trend_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_evidence_trend",
        ),
        "evidence_trend_manifest.json",
    )
    forward_outcome_decision_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_forward_outcome_decision",
        ),
        "forward_decision_manifest.json",
    )
    no_promotion_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_no_promotion_review"),
        "no_promotion_review_manifest.json",
    )
    near_miss_candidates_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_near_miss_candidates"),
        "near_miss_manifest.json",
    )
    cash_buffer_attribution_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_cash_buffer_attribution"),
        "cash_buffer_attribution_manifest.json",
    )
    search_coverage_gap_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_search_coverage_gap"),
        "search_coverage_gap_manifest.json",
    )
    targeted_search_v3_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_targeted_search_v3"),
        "targeted_search_v3_manifest.json",
    )
    targeted_v3_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_targeted_v3_backfill"),
        "targeted_v3_backfill_manifest.json",
    )
    near_miss_ab_comparison_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_near_miss_ab_comparison"),
        "near_miss_ab_manifest.json",
    )
    promotion_threshold_sensitivity_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_promotion_threshold_sensitivity",
        ),
        "threshold_sensitivity_manifest.json",
    )
    candidate_promotion_v2_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_candidate_promotion_v2"),
        "candidate_promotion_v2_manifest.json",
    )
    next_formal_or_search_plan_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_next_formal_or_search_plan",
        ),
        "next_formal_or_search_manifest.json",
    )
    signal_failure_taxonomy_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_signal_failure_taxonomy"),
        "signal_failure_taxonomy_manifest.json",
    )
    candidate_signal_ledger_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_candidate_signal_ledger"),
        "candidate_signal_ledger_manifest.json",
    )
    signal_churn_root_cause_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_signal_churn_root_cause"),
        "signal_churn_root_cause_manifest.json",
    )
    regime_mismatch_attribution_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_regime_mismatch_attribution"),
        "regime_mismatch_manifest.json",
    )
    candidate_quality_filter_design_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_candidate_quality_filter_design",
        ),
        "candidate_quality_filter_manifest.json",
    )
    filtered_candidate_backfill_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_filtered_candidate_backfill"),
        "filtered_candidate_backfill_manifest.json",
    )
    filtered_vs_original_comparison_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_filtered_vs_original_comparison",
        ),
        "filtered_vs_original_manifest.json",
    )
    signal_gate_experiment_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_signal_gate_experiment"),
        "signal_gate_experiment_manifest.json",
    )
    filtered_candidate_promotion_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_filtered_candidate_promotion_review",
        ),
        "filtered_promotion_manifest.json",
    )
    owner_signal_roadmap_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_owner_signal_roadmap"),
        "owner_signal_roadmap_manifest.json",
    )
    filtered_candidate_evidence_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_filtered_candidate_evidence"),
        "filtered_candidate_evidence_manifest.json",
    )
    median_regime_filter_spec_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_median_regime_filter_spec"),
        "median_regime_filter_spec_manifest.json",
    )
    filtered_candidate_stress_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_filtered_candidate_stress_backfill",
        ),
        "filtered_candidate_stress_manifest.json",
    )
    drawdown_mismatch_reduction_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_drawdown_mismatch_reduction"),
        "drawdown_mismatch_reduction_manifest.json",
    )
    flip_rotation_reduction_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_flip_rotation_reduction"),
        "flip_rotation_reduction_manifest.json",
    )
    filtered_candidate_ab_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_filtered_candidate_ab_review"),
        "filtered_candidate_ab_manifest.json",
    )
    signal_gate_confirmation_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_signal_gate_confirmation"),
        "signal_gate_confirmation_manifest.json",
    )
    filtered_formalization_readiness_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_filtered_formalization_readiness",
        ),
        "filtered_formalization_manifest.json",
    )
    owner_filtered_candidate_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_owner_filtered_candidate_review",
        ),
        "owner_filtered_candidate_manifest.json",
    )
    filtered_next_decision_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(report_index, "etf_dynamic_v3_filtered_next_decision"),
        "filtered_next_decision_manifest.json",
    )
    formal_research_method_contract_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_formal_research_method_contract",
        ),
        "formal_research_method_contract_manifest.json",
    )
    promotion_threshold_calibration_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_promotion_gate_threshold_calibration",
        ),
        "promotion_gate_threshold_calibration_manifest.json",
    )
    paper_shadow_protocol_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_shadow_protocol",
        ),
        "paper_shadow_protocol_manifest.json",
    )
    paper_shadow_daily_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_shadow_daily",
        ),
        "paper_shadow_daily_manifest.json",
    )
    paper_shadow_drift_monitor_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_shadow_drift_monitor",
        ),
        "paper_shadow_drift_manifest.json",
    )
    paper_shadow_weekly_review_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_paper_shadow_weekly_review",
        ),
        "paper_shadow_weekly_manifest.json",
    )
    candidate_decision_ledger_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_candidate_decision_ledger",
        ),
        "candidate_decision_ledger_manifest.json",
    )
    evidence_staleness_monitor_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_evidence_staleness_monitor",
        ),
        "evidence_staleness_manifest.json",
    )
    shadow_continuation_readiness_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_shadow_continuation_readiness",
        ),
        "shadow_continuation_readiness_manifest.json",
    )
    stress_scenario_library_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_stress_scenario_library",
        ),
        "stress_scenario_manifest.json",
    )
    drawdown_event_casebook_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_drawdown_event_casebook",
        ),
        "drawdown_casebook_manifest.json",
    )
    flip_rotation_event_casebook_path = _dynamic_v3_sibling_artifact_path(
        _report_index_artifact_path(
            report_index,
            "etf_dynamic_v3_flip_rotation_event_casebook",
        ),
        "flip_rotation_casebook_manifest.json",
    )
    leaderboard = _read_optional_json(leaderboard_path)
    promotion_path = _promotion_pack_manifest_path(indexed_promotion_path)
    evidence_path = (
        indexed_promotion_path
        if indexed_promotion_path is not None
        and indexed_promotion_path.name == "evidence_summary.json"
        else (
            promotion_path.parent / "evidence_summary.json" if promotion_path is not None else None
        )
    )
    promotion = _read_optional_json(promotion_path)
    evidence = _read_optional_json(evidence_path)
    shadow_monitor = _read_optional_json(shadow_monitor_path)
    candidate_evidence = _read_optional_json(candidate_evidence_path)
    observe_pool = _read_optional_json(observe_pool_path)
    overnight_readiness = _read_optional_json(overnight_readiness_path)
    research_decision = _read_optional_json(research_decision_path)
    research_next_action = _read_optional_json(
        _research_decision_recommendation_path(research_decision_path)
    )
    evidence_diagnosis = _read_optional_json(evidence_diagnosis_path)
    gate_impact = _read_optional_json(gate_impact_path)
    gate_policy = _read_optional_json(gate_policy_path)
    candidate_recovery = _read_optional_json(candidate_recovery_path)
    research_decision_update = _read_optional_json(research_decision_update_path)
    research_decision_update_next_action = _read_optional_json(
        _research_decision_recommendation_path(research_decision_update_path)
    )
    go_no_go_matrix = _read_optional_json(
        _research_decision_update_go_no_go_path(research_decision_update_path)
    )
    shortlist = _read_optional_json(shortlist_path)
    candidate_cluster = _read_optional_json(candidate_cluster_path)
    shadow_shortlist = _read_optional_json(shadow_shortlist_path)
    position_advisory = _read_optional_json(position_advisory_path)
    position_advisory_actions = _read_optional_json(
        position_advisory_path.parent / "advisory_actions.json"
        if position_advisory_path is not None
        else None
    )
    position_review = _read_optional_json(position_review_path)
    position_review_decision = _read_optional_json(
        position_review_path.parent / "go_no_go_decision.json"
        if position_review_path is not None
        else None
    )
    shadow_monitor_run = _read_optional_json(shadow_monitor_run_path)
    shadow_monitor_run_summary = _read_optional_json(
        shadow_monitor_run_path.parent / "shadow_monitor_summary.json"
        if shadow_monitor_run_path is not None
        else None
    )
    portfolio_snapshot = _read_optional_json(portfolio_snapshot_path)
    position_advisory_daily = _read_optional_json(position_advisory_daily_path)
    position_advisory_daily_actions = _read_optional_json(
        position_advisory_daily_path.parent / "daily_advisory_actions.json"
        if position_advisory_daily_path is not None
        else None
    )
    consensus_drift = _read_optional_json(consensus_drift_path)
    consensus_drift_summary = _read_optional_json(
        consensus_drift_path.parent / "consensus_drift_summary.json"
        if consensus_drift_path is not None
        else None
    )
    owner_review = _read_optional_json(owner_review_path)
    paper_portfolio = _read_optional_json(paper_portfolio_path)
    paper_portfolio_state = _read_optional_json(
        paper_portfolio_path.parent / "paper_portfolio_state.json"
        if paper_portfolio_path is not None
        else None
    )
    advisory_outcome = _read_optional_json(advisory_outcome_path)
    owner_attribution = _read_optional_json(owner_attribution_path)
    owner_attribution_summary = _read_optional_json(
        owner_attribution_path.parent / "owner_decision_summary.json"
        if owner_attribution_path is not None
        else None
    )
    shadow_aging = _read_optional_json(shadow_aging_path)
    shadow_aging_summary = _read_optional_json(
        shadow_aging_path.parent / "promotion_clock_v2_summary.json"
        if shadow_aging_path is not None
        else None
    )
    weekly_advisory_review = _read_optional_json(weekly_advisory_review_path)
    weekly_owner_summary = _read_optional_json(
        weekly_advisory_review_path.parent / "weekly_owner_decision_summary.json"
        if weekly_advisory_review_path is not None
        else None
    )
    replay_inventory = _read_optional_json(replay_inventory_path)
    replay_inventory_coverage = _read_optional_json(
        replay_inventory_path.parent / "replay_coverage_summary.json"
        if replay_inventory_path is not None
        else None
    )
    historical_replay = _read_optional_json(historical_replay_path)
    replay_action_summary = _read_optional_json(
        historical_replay_path.parent / "replay_action_summary.json"
        if historical_replay_path is not None
        else None
    )
    backfilled_outcome = _read_optional_json(backfilled_outcome_path)
    variant_performance = _read_optional_json(
        backfilled_outcome_path.parent / "variant_performance_summary.json"
        if backfilled_outcome_path is not None
        else None
    )
    historical_paper_sim = _read_optional_json(historical_paper_sim_path)
    simulated_performance = _read_optional_json(
        historical_paper_sim_path.parent / "simulated_performance_summary.json"
        if historical_paper_sim_path is not None
        else None
    )
    replay_performance_review = _read_optional_json(replay_performance_review_path)
    replay_calibration = _read_optional_json(
        replay_performance_review_path.parent / "calibration_recommendations.json"
        if replay_performance_review_path is not None
        else None
    )
    replay_forward_bridge = _read_optional_json(replay_forward_bridge_path)
    replay_forward_focus = _read_optional_json(
        replay_forward_bridge_path.parent / "forward_tracking_focus.json"
        if replay_forward_bridge_path is not None
        else None
    )
    outcome_dashboard = _read_optional_json(outcome_dashboard_path)
    outcome_availability_matrix = _read_optional_json(
        outcome_dashboard_path.parent / "outcome_availability_matrix.json"
        if outcome_dashboard_path is not None
        else None
    )
    pending_reason_dashboard = _read_optional_json(
        outcome_dashboard_path.parent / "pending_reason_dashboard.json"
        if outcome_dashboard_path is not None
        else None
    )
    outcome_update_review = _read_optional_json(outcome_update_review_path)
    outcome_update_safety = _read_optional_json(
        outcome_update_review_path.parent / "update_safety_checks.json"
        if outcome_update_review_path is not None
        else None
    )
    outcome_update_impact = _read_optional_json(
        outcome_update_review_path.parent / "update_impact_preview.json"
        if outcome_update_review_path is not None
        else None
    )
    outcome_update = _read_optional_json(outcome_update_path)
    outcome_status_delta = _read_optional_json(
        outcome_update_path.parent / "outcome_status_delta.json"
        if outcome_update_path is not None
        else None
    )
    rolling_evidence_refresh = _read_optional_json(rolling_evidence_refresh_path)
    rolling_evidence_delta = _read_optional_json(
        rolling_evidence_refresh_path.parent / "evidence_delta_summary.json"
        if rolling_evidence_refresh_path is not None
        else None
    )
    refreshed_artifacts = _read_optional_json(
        rolling_evidence_refresh_path.parent / "refreshed_artifacts.json"
        if rolling_evidence_refresh_path is not None
        else None
    )
    evidence_trend = _read_optional_json(evidence_trend_path)
    confidence_trend_summary = _read_optional_json(
        evidence_trend_path.parent / "confidence_trend_summary.json"
        if evidence_trend_path is not None
        else None
    )
    forward_outcome_decision = _read_optional_json(forward_outcome_decision_path)
    forward_go_no_go_matrix = _read_optional_json(
        forward_outcome_decision_path.parent / "forward_go_no_go_matrix.json"
        if forward_outcome_decision_path is not None
        else None
    )
    forward_next_actions = _read_optional_json(
        forward_outcome_decision_path.parent / "forward_next_actions.json"
        if forward_outcome_decision_path is not None
        else None
    )
    no_promotion_review_manifest = _read_optional_json(no_promotion_review_path)
    no_promotion_reason_summary = _read_optional_json(
        no_promotion_review_path.parent / "no_promotion_reason_summary.json"
        if no_promotion_review_path is not None
        else None
    )
    near_miss_candidates_manifest = _read_optional_json(near_miss_candidates_path)
    near_miss_candidates = _read_optional_jsonl(
        near_miss_candidates_path.parent / "near_miss_candidates.jsonl"
        if near_miss_candidates_path is not None
        else None
    )
    near_miss_family_summary = _read_optional_json(
        near_miss_candidates_path.parent / "near_miss_family_summary.json"
        if near_miss_candidates_path is not None
        else None
    )
    cash_buffer_attribution_manifest = _read_optional_json(cash_buffer_attribution_path)
    cash_buffer_effect_summary = _read_optional_json(
        cash_buffer_attribution_path.parent / "cash_buffer_effect_summary.json"
        if cash_buffer_attribution_path is not None
        else None
    )
    cash_buffer_failure_reason = _read_optional_json(
        cash_buffer_attribution_path.parent / "cash_buffer_failure_reason.json"
        if cash_buffer_attribution_path is not None
        else None
    )
    search_coverage_gap_manifest = _read_optional_json(search_coverage_gap_path)
    targeted_v3_recommendations = _read_optional_json(
        search_coverage_gap_path.parent / "targeted_v3_recommendations.json"
        if search_coverage_gap_path is not None
        else None
    )
    targeted_search_v3_manifest = _read_optional_json(targeted_search_v3_path)
    v3_family_coverage = _read_optional_json(
        targeted_search_v3_path.parent / "v3_family_coverage.json"
        if targeted_search_v3_path is not None
        else None
    )
    targeted_v3_backfill_manifest = _read_optional_json(targeted_v3_backfill_path)
    v3_backfill_progress = _read_optional_json(
        targeted_v3_backfill_path.parent / "v3_backfill_progress.json"
        if targeted_v3_backfill_path is not None
        else None
    )
    near_miss_ab_comparison_manifest = _read_optional_json(near_miss_ab_comparison_path)
    ab_winner_summary = _read_optional_json(
        near_miss_ab_comparison_path.parent / "ab_winner_summary.json"
        if near_miss_ab_comparison_path is not None
        else None
    )
    promotion_threshold_sensitivity_manifest = _read_optional_json(
        promotion_threshold_sensitivity_path
    )
    threshold_candidate_impact = _read_optional_json(
        promotion_threshold_sensitivity_path.parent / "threshold_candidate_impact.json"
        if promotion_threshold_sensitivity_path is not None
        else None
    )
    candidate_promotion_v2_manifest = _read_optional_json(candidate_promotion_v2_path)
    promotion_v2_decision = _read_optional_json(
        candidate_promotion_v2_path.parent / "promotion_v2_decision.json"
        if candidate_promotion_v2_path is not None
        else None
    )
    next_formal_or_search_plan_manifest = _read_optional_json(next_formal_or_search_plan_path)
    next_plan_decision = _read_optional_json(
        next_formal_or_search_plan_path.parent / "next_plan_decision.json"
        if next_formal_or_search_plan_path is not None
        else None
    )
    signal_failure_taxonomy_manifest = _read_optional_json(signal_failure_taxonomy_path)
    signal_failure_mode_catalog = _read_optional_json(
        signal_failure_taxonomy_path.parent / "signal_failure_mode_catalog.json"
        if signal_failure_taxonomy_path is not None
        else None
    )
    candidate_signal_ledger_manifest = _read_optional_json(candidate_signal_ledger_path)
    candidate_signal_summary = _read_optional_json(
        candidate_signal_ledger_path.parent / "candidate_signal_summary.json"
        if candidate_signal_ledger_path is not None
        else None
    )
    signal_churn_root_cause_manifest = _read_optional_json(signal_churn_root_cause_path)
    churn_root_cause_summary = _read_optional_json(
        signal_churn_root_cause_path.parent / "churn_root_cause_summary.json"
        if signal_churn_root_cause_path is not None
        else None
    )
    regime_mismatch_attribution_manifest = _read_optional_json(regime_mismatch_attribution_path)
    regime_mismatch_summary = _read_optional_json(
        regime_mismatch_attribution_path.parent / "regime_mismatch_summary.json"
        if regime_mismatch_attribution_path is not None
        else None
    )
    candidate_quality_filter_design_manifest = _read_optional_json(
        candidate_quality_filter_design_path
    )
    proposed_quality_filters = _read_optional_json(
        candidate_quality_filter_design_path.parent / "proposed_quality_filters.json"
        if candidate_quality_filter_design_path is not None
        else None
    )
    filtered_candidate_backfill_manifest = _read_optional_json(filtered_candidate_backfill_path)
    filtered_vs_original_comparison_manifest = _read_optional_json(
        filtered_vs_original_comparison_path
    )
    filtered_improvement_summary = _read_optional_json(
        filtered_vs_original_comparison_path.parent / "filtered_improvement_summary.json"
        if filtered_vs_original_comparison_path is not None
        else None
    )
    signal_gate_experiment_manifest = _read_optional_json(signal_gate_experiment_path)
    signal_gate_experiment_summary = _read_optional_json(
        signal_gate_experiment_path.parent / "signal_gate_experiment_summary.json"
        if signal_gate_experiment_path is not None
        else None
    )
    filtered_candidate_promotion_review_manifest = _read_optional_json(
        filtered_candidate_promotion_review_path
    )
    filtered_promotion_decision = _read_optional_json(
        filtered_candidate_promotion_review_path.parent / "filtered_promotion_decision.json"
        if filtered_candidate_promotion_review_path is not None
        else None
    )
    owner_signal_roadmap_manifest = _read_optional_json(owner_signal_roadmap_path)
    owner_signal_roadmap_summary = _read_optional_json(
        owner_signal_roadmap_path.parent / "owner_signal_roadmap_summary.json"
        if owner_signal_roadmap_path is not None
        else None
    )
    filtered_candidate_evidence_manifest = _read_optional_json(filtered_candidate_evidence_path)
    filtered_candidate_evidence_summary = _read_optional_json(
        filtered_candidate_evidence_path.parent / "filtered_candidate_evidence_summary.json"
        if filtered_candidate_evidence_path is not None
        else None
    )
    median_regime_filter_spec_manifest = _read_optional_json(median_regime_filter_spec_path)
    median_regime_filter_contract = _read_optional_json(
        median_regime_filter_spec_path.parent / "median_regime_filter_contract.json"
        if median_regime_filter_spec_path is not None
        else None
    )
    filtered_candidate_stress_manifest = _read_optional_json(filtered_candidate_stress_path)
    filtered_candidate_stress_summary = _read_optional_json(
        filtered_candidate_stress_path.parent / "filtered_candidate_stress_summary.json"
        if filtered_candidate_stress_path is not None
        else None
    )
    drawdown_mismatch_reduction_manifest = _read_optional_json(drawdown_mismatch_reduction_path)
    mismatch_reduction_summary = _read_optional_json(
        drawdown_mismatch_reduction_path.parent / "mismatch_reduction_summary.json"
        if drawdown_mismatch_reduction_path is not None
        else None
    )
    flip_rotation_reduction_manifest = _read_optional_json(flip_rotation_reduction_path)
    flip_rotation_reduction_summary = _read_optional_json(
        flip_rotation_reduction_path.parent / "flip_rotation_reduction_summary.json"
        if flip_rotation_reduction_path is not None
        else None
    )
    filtered_candidate_ab_manifest = _read_optional_json(filtered_candidate_ab_review_path)
    filtered_candidate_ab_summary = _read_optional_json(
        filtered_candidate_ab_review_path.parent / "ab_summary.json"
        if filtered_candidate_ab_review_path is not None
        else None
    )
    signal_gate_confirmation_manifest = _read_optional_json(signal_gate_confirmation_path)
    signal_gate_confirmation_targets = _read_optional_json(
        signal_gate_confirmation_path.parent / "signal_gate_confirmation_targets.json"
        if signal_gate_confirmation_path is not None
        else None
    )
    filtered_formalization_manifest = _read_optional_json(filtered_formalization_readiness_path)
    formalization_readiness_decision = _read_optional_json(
        filtered_formalization_readiness_path.parent / "formalization_readiness_decision.json"
        if filtered_formalization_readiness_path is not None
        else None
    )
    owner_filtered_candidate_manifest = _read_optional_json(owner_filtered_candidate_review_path)
    owner_filtered_candidate_summary = _read_optional_json(
        owner_filtered_candidate_review_path.parent / "owner_filtered_candidate_summary.json"
        if owner_filtered_candidate_review_path is not None
        else None
    )
    filtered_next_decision_manifest = _read_optional_json(filtered_next_decision_path)
    filtered_next_decision = _read_optional_json(
        filtered_next_decision_path.parent / "filtered_next_decision.json"
        if filtered_next_decision_path is not None
        else None
    )
    formal_research_method_contract_manifest = _read_optional_json(
        formal_research_method_contract_path
    )
    formal_research_method_contract = _read_optional_json(
        formal_research_method_contract_path.parent / "formal_research_method_contract.json"
        if formal_research_method_contract_path is not None
        else None
    )
    formal_research_method_decision = _read_optional_json(
        formal_research_method_contract_path.parent / "formal_research_method_decision.json"
        if formal_research_method_contract_path is not None
        else None
    )
    formal_research_method_validation = _read_optional_json(
        formal_research_method_contract_path.parent
        / "formal_research_method_contract_validation.json"
        if formal_research_method_contract_path is not None
        else None
    )
    promotion_threshold_manifest = _read_optional_json(
        promotion_threshold_calibration_path
    )
    promotion_threshold_report = _read_optional_json(
        promotion_threshold_calibration_path.parent
        / "promotion_gate_threshold_calibration_report.json"
        if promotion_threshold_calibration_path is not None
        else None
    )
    promotion_threshold_validation = _read_optional_json(
        promotion_threshold_calibration_path.parent
        / "promotion_gate_threshold_validation.json"
        if promotion_threshold_calibration_path is not None
        else None
    )
    paper_shadow_protocol_manifest = _read_optional_json(paper_shadow_protocol_path)
    paper_shadow_protocol = _read_optional_json(
        paper_shadow_protocol_path.parent / "paper_shadow_protocol.json"
        if paper_shadow_protocol_path is not None
        else None
    )
    paper_shadow_protocol_validation = _read_optional_json(
        paper_shadow_protocol_path.parent / "paper_shadow_protocol_validation.json"
        if paper_shadow_protocol_path is not None
        else None
    )
    paper_shadow_daily_manifest = _read_optional_json(paper_shadow_daily_path)
    paper_shadow_daily_observation = _read_optional_json(
        paper_shadow_daily_path.parent / "paper_shadow_daily_observation.json"
        if paper_shadow_daily_path is not None
        else None
    )
    paper_shadow_daily_validation = _read_optional_json(
        paper_shadow_daily_path.parent / "paper_shadow_daily_validation.json"
        if paper_shadow_daily_path is not None
        else None
    )
    paper_shadow_drift_manifest = _read_optional_json(paper_shadow_drift_monitor_path)
    paper_shadow_drift_report = _read_optional_json(
        paper_shadow_drift_monitor_path.parent / "paper_shadow_drift_report.json"
        if paper_shadow_drift_monitor_path is not None
        else None
    )
    paper_shadow_drift_validation = _read_optional_json(
        paper_shadow_drift_monitor_path.parent / "paper_shadow_drift_validation.json"
        if paper_shadow_drift_monitor_path is not None
        else None
    )
    paper_shadow_weekly_manifest = _read_optional_json(paper_shadow_weekly_review_path)
    paper_shadow_weekly_review = _read_optional_json(
        paper_shadow_weekly_review_path.parent / "paper_shadow_weekly_review.json"
        if paper_shadow_weekly_review_path is not None
        else None
    )
    paper_shadow_weekly_validation = _read_optional_json(
        paper_shadow_weekly_review_path.parent / "paper_shadow_weekly_validation.json"
        if paper_shadow_weekly_review_path is not None
        else None
    )
    candidate_decision_ledger_manifest = _read_optional_json(candidate_decision_ledger_path)
    candidate_decision_record = _read_optional_json(
        candidate_decision_ledger_path.parent / "candidate_decision_record.json"
        if candidate_decision_ledger_path is not None
        else None
    )
    candidate_decision_ledger_validation = _read_optional_json(
        candidate_decision_ledger_path.parent / "candidate_decision_ledger_validation.json"
        if candidate_decision_ledger_path is not None
        else None
    )
    evidence_staleness_manifest = _read_optional_json(evidence_staleness_monitor_path)
    evidence_staleness_report = _read_optional_json(
        evidence_staleness_monitor_path.parent / "evidence_staleness_report.json"
        if evidence_staleness_monitor_path is not None
        else None
    )
    evidence_staleness_validation = _read_optional_json(
        evidence_staleness_monitor_path.parent / "evidence_staleness_validation.json"
        if evidence_staleness_monitor_path is not None
        else None
    )
    shadow_continuation_manifest = _read_optional_json(shadow_continuation_readiness_path)
    shadow_continuation_report = _read_optional_json(
        shadow_continuation_readiness_path.parent
        / "shadow_continuation_readiness_report.json"
        if shadow_continuation_readiness_path is not None
        else None
    )
    shadow_continuation_validation = _read_optional_json(
        shadow_continuation_readiness_path.parent
        / "shadow_continuation_readiness_validation.json"
        if shadow_continuation_readiness_path is not None
        else None
    )
    stress_scenario_manifest = _read_optional_json(stress_scenario_library_path)
    stress_scenario_library = _read_optional_json(
        stress_scenario_library_path.parent / "stress_scenario_library.json"
        if stress_scenario_library_path is not None
        else None
    )
    stress_scenario_validation = _read_optional_json(
        stress_scenario_library_path.parent / "stress_scenario_validation.json"
        if stress_scenario_library_path is not None
        else None
    )
    drawdown_casebook_manifest = _read_optional_json(drawdown_event_casebook_path)
    drawdown_event_casebook = _read_optional_json(
        drawdown_event_casebook_path.parent / "drawdown_event_casebook.json"
        if drawdown_event_casebook_path is not None
        else None
    )
    drawdown_casebook_validation = _read_optional_json(
        drawdown_event_casebook_path.parent / "drawdown_event_casebook_validation.json"
        if drawdown_event_casebook_path is not None
        else None
    )
    flip_rotation_casebook_manifest = _read_optional_json(flip_rotation_event_casebook_path)
    flip_rotation_event_casebook = _read_optional_json(
        flip_rotation_event_casebook_path.parent / "flip_rotation_event_casebook.json"
        if flip_rotation_event_casebook_path is not None
        else None
    )
    flip_rotation_casebook_validation = _read_optional_json(
        flip_rotation_event_casebook_path.parent
        / "flip_rotation_event_casebook_validation.json"
        if flip_rotation_event_casebook_path is not None
        else None
    )
    filtered_candidate_readiness_payloads = (
        filtered_candidate_evidence_manifest,
        filtered_candidate_evidence_summary,
        median_regime_filter_spec_manifest,
        median_regime_filter_contract,
        filtered_candidate_stress_manifest,
        filtered_candidate_stress_summary,
        drawdown_mismatch_reduction_manifest,
        mismatch_reduction_summary,
        flip_rotation_reduction_manifest,
        flip_rotation_reduction_summary,
        filtered_candidate_ab_manifest,
        filtered_candidate_ab_summary,
        signal_gate_confirmation_manifest,
        signal_gate_confirmation_targets,
        filtered_formalization_manifest,
        formalization_readiness_decision,
        owner_filtered_candidate_manifest,
        owner_filtered_candidate_summary,
        filtered_next_decision_manifest,
        filtered_next_decision,
        formal_research_method_contract_manifest,
        formal_research_method_contract,
        formal_research_method_decision,
        formal_research_method_validation,
        promotion_threshold_manifest,
        promotion_threshold_report,
        promotion_threshold_validation,
        paper_shadow_protocol_manifest,
        paper_shadow_protocol,
        paper_shadow_protocol_validation,
        paper_shadow_daily_manifest,
        paper_shadow_daily_observation,
        paper_shadow_daily_validation,
        paper_shadow_drift_manifest,
        paper_shadow_drift_report,
        paper_shadow_drift_validation,
        paper_shadow_weekly_manifest,
        paper_shadow_weekly_review,
        paper_shadow_weekly_validation,
        candidate_decision_ledger_manifest,
        candidate_decision_record,
        candidate_decision_ledger_validation,
        evidence_staleness_manifest,
        evidence_staleness_report,
        evidence_staleness_validation,
        shadow_continuation_manifest,
        shadow_continuation_report,
        shadow_continuation_validation,
        stress_scenario_manifest,
        stress_scenario_library,
        stress_scenario_validation,
        drawdown_casebook_manifest,
        drawdown_event_casebook,
        drawdown_casebook_validation,
        flip_rotation_casebook_manifest,
        flip_rotation_event_casebook,
        flip_rotation_casebook_validation,
    )
    outcome_loop_payloads = (
        outcome_update_review,
        outcome_update_safety,
        outcome_update_impact,
        outcome_update,
        outcome_status_delta,
        rolling_evidence_refresh,
        rolling_evidence_delta,
        refreshed_artifacts,
        evidence_trend,
        confidence_trend_summary,
        forward_outcome_decision,
        forward_go_no_go_matrix,
        forward_next_actions,
    )
    replay_recommendations = _records(replay_calibration.get("recommendations"))
    replay_recommendation = replay_recommendations[0] if replay_recommendations else {}
    if not leaderboard:
        replay_payloads = (
            replay_inventory,
            replay_inventory_coverage,
            historical_replay,
            replay_action_summary,
            backfilled_outcome,
            variant_performance,
            historical_paper_sim,
            simulated_performance,
            replay_performance_review,
            replay_calibration,
            replay_forward_bridge,
            replay_forward_focus,
            outcome_dashboard,
            outcome_availability_matrix,
            pending_reason_dashboard,
            *outcome_loop_payloads,
        )
        if any(replay_payloads):
            return _etf_dynamic_v3_parameter_research_replay_only_summary(
                replay_inventory_path=replay_inventory_path,
                replay_inventory=replay_inventory,
                replay_inventory_coverage=replay_inventory_coverage,
                historical_replay_path=historical_replay_path,
                historical_replay=historical_replay,
                replay_action_summary=replay_action_summary,
                backfilled_outcome_path=backfilled_outcome_path,
                backfilled_outcome=backfilled_outcome,
                variant_performance=variant_performance,
                historical_paper_sim_path=historical_paper_sim_path,
                historical_paper_sim=historical_paper_sim,
                simulated_performance=simulated_performance,
                replay_performance_review_path=replay_performance_review_path,
                replay_performance_review=replay_performance_review,
                replay_calibration=replay_calibration,
                replay_recommendation=replay_recommendation,
                replay_forward_bridge_path=replay_forward_bridge_path,
                replay_forward_bridge=replay_forward_bridge,
                replay_forward_focus=replay_forward_focus,
                outcome_dashboard_path=outcome_dashboard_path,
                outcome_dashboard=outcome_dashboard,
                outcome_availability_matrix=outcome_availability_matrix,
                pending_reason_dashboard=pending_reason_dashboard,
                outcome_update_review_path=outcome_update_review_path,
                outcome_update_review=outcome_update_review,
                outcome_update_safety=outcome_update_safety,
                outcome_update_impact=outcome_update_impact,
                outcome_update_path=outcome_update_path,
                outcome_update=outcome_update,
                outcome_status_delta=outcome_status_delta,
                rolling_evidence_refresh_path=rolling_evidence_refresh_path,
                rolling_evidence_refresh=rolling_evidence_refresh,
                rolling_evidence_delta=rolling_evidence_delta,
                refreshed_artifacts=refreshed_artifacts,
                evidence_trend_path=evidence_trend_path,
                evidence_trend=evidence_trend,
                confidence_trend_summary=confidence_trend_summary,
                forward_outcome_decision_path=forward_outcome_decision_path,
                forward_outcome_decision=forward_outcome_decision,
                forward_go_no_go_matrix=forward_go_no_go_matrix,
                forward_next_actions=forward_next_actions,
            )
        return _missing_etf_dynamic_v3_parameter_research_summary()
    top = _records(leaderboard.get("top_eligible_candidates"))
    first = top[0] if top else {}
    common_reasons = _records(leaderboard.get("most_common_reject_reasons"))[:5]
    shadow_summary = _mapping(_mapping(shadow_monitor).get("summary"))
    promotion_status = _text(_mapping(promotion).get("status"), "MISSING")
    backtest_window_status = _text(evidence.get("backtest_window_status"), "MISSING")
    weight_path_status = _text(evidence.get("weight_path_status"), "MISSING")
    candidate_attribution_status = _text(
        evidence.get("candidate_attribution_status"),
        "MISSING",
    )
    provenance_status = _text(evidence.get("provenance_status"), "MISSING")
    download_manifest_status = _text(
        evidence.get("download_manifest_status"),
        "MISSING",
    )
    promotion_blocking_flags = (
        ", ".join(_texts(evidence.get("promotion_blocking_flags"))) or "MISSING"
    )
    signal_filter_payloads = (
        signal_failure_taxonomy_manifest,
        signal_failure_mode_catalog,
        candidate_signal_ledger_manifest,
        candidate_signal_summary,
        signal_churn_root_cause_manifest,
        churn_root_cause_summary,
        regime_mismatch_attribution_manifest,
        regime_mismatch_summary,
        candidate_quality_filter_design_manifest,
        proposed_quality_filters,
        filtered_candidate_backfill_manifest,
        filtered_vs_original_comparison_manifest,
        filtered_improvement_summary,
        signal_gate_experiment_manifest,
        signal_gate_experiment_summary,
        filtered_candidate_promotion_review_manifest,
        filtered_promotion_decision,
        owner_signal_roadmap_manifest,
        owner_signal_roadmap_summary,
    )
    safety_status = _etf_dynamic_v3_parameter_research_safety_status(
        leaderboard,
        promotion,
        evidence_diagnosis,
        gate_impact,
        gate_policy,
        candidate_recovery,
        research_decision_update,
        shortlist,
        candidate_cluster,
        shadow_shortlist,
        position_advisory,
        position_advisory_actions,
        position_review,
        position_review_decision,
        shadow_monitor_run,
        shadow_monitor_run_summary,
        portfolio_snapshot,
        position_advisory_daily,
        position_advisory_daily_actions,
        consensus_drift,
        consensus_drift_summary,
        owner_review,
        paper_portfolio,
        paper_portfolio_state,
        advisory_outcome,
        owner_attribution,
        owner_attribution_summary,
        shadow_aging,
        shadow_aging_summary,
        weekly_advisory_review,
        weekly_owner_summary,
        replay_inventory,
        replay_inventory_coverage,
        historical_replay,
        replay_action_summary,
        backfilled_outcome,
        variant_performance,
        historical_paper_sim,
        simulated_performance,
        replay_performance_review,
        replay_calibration,
        replay_forward_bridge,
        replay_forward_focus,
        outcome_dashboard,
        outcome_availability_matrix,
        pending_reason_dashboard,
        *outcome_loop_payloads,
        *signal_filter_payloads,
        *filtered_candidate_readiness_payloads,
    )
    top_candidate = _text(first.get("candidate_id"), "MISSING")
    evaluator_mode = _text(leaderboard.get("evaluator_mode"), "UNKNOWN")
    not_for_investment = leaderboard.get("not_for_investment_decision") is True
    outcome_delta_before = _mapping(outcome_status_delta.get("before"))
    outcome_delta_after = _mapping(outcome_status_delta.get("after"))
    rolling_delta_before = _mapping(rolling_evidence_delta.get("before"))
    rolling_delta_after = _mapping(rolling_evidence_delta.get("after"))
    forward_actions = _records(forward_next_actions.get("next_actions"))
    forward_next_due_scan_date = "MISSING"
    for action in forward_actions:
        if action.get("action") == "run_next_due_scan":
            forward_next_due_scan_date = _text(action.get("target_date"), "MISSING")
            break
    no_promotion_reasons = _records(no_promotion_reason_summary.get("primary_reasons"))
    no_promotion_top_reason = (
        _text(no_promotion_reasons[0].get("reason"), "MISSING")
        if no_promotion_reasons
        else "MISSING"
    )
    candidate_promotion_v2_decision = _text(
        promotion_v2_decision.get("decision"),
        "MISSING",
    )
    next_formal_or_search_plan_decision = _text(
        next_plan_decision.get("decision"),
        "MISSING",
    )
    signal_gate_confirmation_target_count = len(
        _records(signal_gate_confirmation_targets.get("targets"))
    )
    paper_shadow_daily_review = _mapping(
        paper_shadow_daily_observation.get("daily_review")
    )
    paper_shadow_weekly_summary = _mapping(paper_shadow_weekly_review.get("summary"))
    paper_shadow_weekly_window = (
        f"{paper_shadow_weekly_review.get('week_start')}.."
        f"{paper_shadow_weekly_review.get('week_end')}"
        if paper_shadow_weekly_review.get("week_start")
        and paper_shadow_weekly_review.get("week_end")
        else "MISSING"
    )
    paper_shadow_weekly_drift_trend = _mapping(
        paper_shadow_weekly_summary.get("drift_severity_trend")
    )
    paper_shadow_weekly_drift_trend_text = (
        f"max={paper_shadow_weekly_drift_trend.get('max_severity')}; "
        f"sequence={','.join(_texts(paper_shadow_weekly_drift_trend.get('sequence')))}"
        if paper_shadow_weekly_drift_trend
        else "MISSING"
    )
    paper_shadow_weekly_missing_inputs = (
        ", ".join(_texts(paper_shadow_weekly_summary.get("missing_input_artifacts")))
        or "none"
    )
    signal_filter_payloads = (
        signal_failure_taxonomy_manifest,
        signal_failure_mode_catalog,
        candidate_signal_ledger_manifest,
        candidate_signal_summary,
        signal_churn_root_cause_manifest,
        churn_root_cause_summary,
        regime_mismatch_attribution_manifest,
        regime_mismatch_summary,
        candidate_quality_filter_design_manifest,
        proposed_quality_filters,
        filtered_candidate_backfill_manifest,
        filtered_vs_original_comparison_manifest,
        filtered_improvement_summary,
        signal_gate_experiment_manifest,
        signal_gate_experiment_summary,
        filtered_candidate_promotion_review_manifest,
        filtered_promotion_decision,
        owner_signal_roadmap_manifest,
        owner_signal_roadmap_summary,
    )
    no_promotion_v3_payloads = (
        no_promotion_review_manifest,
        no_promotion_reason_summary,
        near_miss_candidates_manifest,
        *near_miss_candidates,
        near_miss_family_summary,
        cash_buffer_attribution_manifest,
        cash_buffer_effect_summary,
        cash_buffer_failure_reason,
        search_coverage_gap_manifest,
        targeted_v3_recommendations,
        targeted_search_v3_manifest,
        v3_family_coverage,
        targeted_v3_backfill_manifest,
        v3_backfill_progress,
        near_miss_ab_comparison_manifest,
        ab_winner_summary,
        promotion_threshold_sensitivity_manifest,
        threshold_candidate_impact,
        candidate_promotion_v2_manifest,
        promotion_v2_decision,
        next_formal_or_search_plan_manifest,
        next_plan_decision,
        *signal_filter_payloads,
        *filtered_candidate_readiness_payloads,
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(leaderboard.get("status"), "UNKNOWN"),
        "summary_sentence": (
            "Dynamic Rescue Parameter Sweep: "
            f"top={top_candidate}; candidates={leaderboard.get('candidate_count', 0)}; "
            f"evaluator={evaluator_mode}; "
            f"promotion_status={promotion_status}; "
            f"window={backtest_window_status}; "
            f"weight_path={weight_path_status}; "
            f"provenance={provenance_status}; "
            f"observe_pool={observe_pool.get('observe_candidate_count', 'MISSING')}; "
            f"research_decision={research_decision.get('recommendation', 'MISSING')}; "
            f"go_no_go={research_decision_update.get('go_no_go', 'MISSING')}; "
            f"recovered={candidate_recovery.get('recovered_candidate_count', 'MISSING')}; "
            f"shortlist={shortlist.get('shortlist_count', 'MISSING')}; "
            f"shadow_shortlist={shadow_shortlist.get('shadow_candidate_count', 'MISSING')}; "
            f"advisory={position_advisory.get('position_advisory_status', 'MISSING')}; "
            f"production_readiness={position_review.get('production_readiness', 'MISSING')}; "
            f"daily_monitor={shadow_monitor_run_summary.get('summary_recommendation', 'MISSING')}; "
            f"daily_advisory={position_advisory_daily.get('recommended_action', 'MISSING')}; "
            f"consensus_drift={consensus_drift_summary.get('disagreement_status', 'MISSING')}; "
            f"owner_decision={owner_review.get('owner_decision', 'MISSING')}; "
            f"paper_portfolio={paper_portfolio_state.get('state_status', 'MISSING')}; "
            f"advisory_outcome={advisory_outcome.get('status', 'MISSING')}; "
            f"owner_attribution={owner_attribution.get('status', 'MISSING')}; "
            f"shadow_aging_eligible="
            f"{shadow_aging_summary.get('eligible_for_review_count', 'MISSING')}; "
            f"weekly_advisory="
            f"{weekly_advisory_review.get('weekly_recommendation', 'MISSING')}; "
            f"historical_replay={replay_performance_review.get('status', 'MISSING')}; "
            f"best_replay_variant={variant_performance.get('best_variant', 'MISSING')}; "
            f"replay_forward_bridge={replay_forward_bridge.get('status', 'MISSING')}; "
            f"outcome_update={outcome_update.get('status', 'MISSING')}; "
            f"rolling_refresh={rolling_evidence_refresh.get('status', 'MISSING')}; "
            f"evidence_trend={confidence_trend_summary.get('trend_status', 'MISSING')}; "
            f"forward_decision="
            f"{forward_go_no_go_matrix.get('recommended_action', 'MISSING')}; "
            f"no_promotion_reason={no_promotion_top_reason}; "
            f"near_miss_count={near_miss_candidates_manifest.get('candidate_count', 'MISSING')}; "
            f"targeted_v3_variants={targeted_search_v3_manifest.get('variant_count', 'MISSING')}; "
            f"targeted_v3_data_quality="
            f"{targeted_v3_backfill_manifest.get('data_quality_status', 'MISSING')}; "
            f"ab_best={ab_winner_summary.get('best_v3_variant', 'MISSING')}; "
            f"threshold_relaxed_only="
            f"{threshold_candidate_impact.get('relaxed_only_count', 'MISSING')}; "
            f"candidate_promotion_v2={candidate_promotion_v2_decision}; "
            f"next_formal_or_search_plan={next_formal_or_search_plan_decision}; "
            f"signal_taxonomy_modes="
            f"{signal_failure_taxonomy_manifest.get('failure_mode_count', 'MISSING')}; "
            f"signal_ledger_dominant="
            f"{candidate_signal_summary.get('dominant_failure_mode', 'MISSING')}; "
            f"churn_root_cause="
            f"{churn_root_cause_summary.get('dominant_root_cause', 'MISSING')}; "
            f"regime_mismatch="
            f"{regime_mismatch_summary.get('dominant_mismatch_type', 'MISSING')}; "
            f"quality_filter_count="
            f"{len(_records(proposed_quality_filters.get('filters')))}; "
            f"filtered_backfill="
            f"{filtered_candidate_backfill_manifest.get('status', 'MISSING')}; "
            f"filtered_comparison="
            f"{filtered_improvement_summary.get('recommendation', 'MISSING')}; "
            f"signal_gate="
            f"{signal_gate_experiment_summary.get('recommended_next_action', 'MISSING')}; "
            f"filtered_promotion="
            f"{filtered_promotion_decision.get('decision', 'MISSING')}; "
            f"owner_signal_action="
            f"{owner_signal_roadmap_summary.get('recommended_owner_action', 'MISSING')}; "
            f"filtered_candidate_evidence="
            f"{filtered_candidate_evidence_summary.get('evidence_status', 'MISSING')}; "
            f"median_regime_contract="
            f"{median_regime_filter_contract.get('contract_status', 'MISSING')}; "
            f"filtered_stress="
            f"{filtered_candidate_stress_summary.get('stress_robustness_status', 'MISSING')}; "
            f"drawdown_mismatch_reduction="
            f"{mismatch_reduction_summary.get('drawdown_mismatch_reduction_status', 'MISSING')}; "
            f"flip_reduction="
            f"{flip_rotation_reduction_summary.get('flip_reduction_status', 'MISSING')}; "
            f"filtered_ab="
            f"{filtered_candidate_ab_summary.get('overall_ab_status', 'MISSING')}; "
            f"confirmation_targets={signal_gate_confirmation_target_count}; "
            f"formalization="
            f"{formalization_readiness_decision.get('decision', 'MISSING')}; "
            f"owner_filtered_action="
            f"{owner_filtered_candidate_summary.get('recommended_owner_action', 'MISSING')}; "
            f"filtered_next="
            f"{filtered_next_decision.get('decision', 'MISSING')}; "
            f"formal_research_contract="
            f"{formal_research_method_decision.get('promotion_state', 'MISSING')}; "
            f"paper_shadow_daily="
            f"{paper_shadow_daily_observation.get('observation_status', 'MISSING')}; "
            f"paper_shadow_drift="
            f"{paper_shadow_drift_report.get('drift_severity', 'MISSING')}; "
            f"paper_shadow_weekly="
            f"{paper_shadow_weekly_review.get('weekly_decision', 'MISSING')}; "
            "hard gate precedes soft score and production_candidate is manual-only."
        ),
        "evaluator_mode": evaluator_mode,
        "evaluator_version": leaderboard.get("evaluator_version", "MISSING"),
        "metrics_source": leaderboard.get("metrics_source", "MISSING"),
        "not_for_investment_decision": not_for_investment,
        "data_quality": leaderboard.get("data_quality", {}),
        "candidate_count": leaderboard.get("candidate_count", 0),
        "top_candidate": top_candidate,
        "top_gate": first.get("gate", "MISSING"),
        "top_score": first.get("score", "MISSING"),
        "common_reject_reasons": ", ".join(
            f"{row.get('reason')}:{row.get('count')}" for row in common_reasons
        ),
        "recommended_next_actions": ", ".join(_texts(leaderboard.get("recommended_next_actions"))),
        "promotion_status": promotion_status,
        "backtest_window_status": backtest_window_status,
        "weight_path_status": weight_path_status,
        "candidate_attribution_status": candidate_attribution_status,
        "data_provenance_status": provenance_status,
        "download_manifest_status": download_manifest_status,
        "promotion_blocking_flags": promotion_blocking_flags,
        "shadow_monitor_status": _text(_mapping(shadow_monitor).get("status"), "MISSING"),
        "shadow_observe_only_count": shadow_summary.get("observe_only_candidate_count", 0),
        "shadow_promotion_ready_count": shadow_summary.get("promotion_review_ready_count", 0),
        "shadow_live_drift_review_required_count": shadow_summary.get(
            "live_drift_review_required_count",
            0,
        ),
        "candidate_evidence_status": _text(candidate_evidence.get("status"), "MISSING"),
        "candidate_evidence_usable_count": candidate_evidence.get(
            "usable_for_research_count",
            0,
        ),
        "candidate_evidence_complete_count": candidate_evidence.get(
            "complete_evidence_count",
            0,
        ),
        "candidate_evidence_top_blockers": ", ".join(
            f"{row.get('reason')}:{row.get('count')}"
            for row in _records(candidate_evidence.get("top_blocking_reasons"))[:5]
        )
        or "MISSING",
        "observe_pool_status": _text(observe_pool.get("status"), "MISSING"),
        "observe_candidate_count": observe_pool.get("observe_candidate_count", 0),
        "observe_pool_manual_review_required_count": observe_pool.get(
            "manual_review_required_count",
            0,
        ),
        "shadow_registry_sync_status": _text(
            observe_pool.get("shadow_registry_sync_status"),
            "MISSING",
        ),
        "overnight_readiness": _text(
            overnight_readiness.get("overnight_readiness"),
            "MISSING",
        ),
        "overnight_blocking_reasons": ", ".join(_texts(overnight_readiness.get("blocking_reasons")))
        or "none",
        "research_decision_recommendation": _text(
            research_decision.get("recommendation"),
            "MISSING",
        ),
        "research_decision_priority": _text(research_decision.get("priority"), "MISSING"),
        "research_decision_next_task": _text(
            research_next_action.get("suggested_codex_task"),
            "MISSING",
        ),
        "evidence_diagnosis_status": _text(evidence_diagnosis.get("status"), "MISSING"),
        "evidence_diagnosis_usable_candidates": evidence_diagnosis.get(
            "usable_candidates",
            0,
        ),
        "evidence_diagnosis_hard_blocked_candidates": evidence_diagnosis.get(
            "hard_blocked_candidates",
            0,
        ),
        "evidence_diagnosis_soft_blocked_candidates": evidence_diagnosis.get(
            "soft_blocked_candidates",
            0,
        ),
        "gate_impact_status": _text(gate_impact.get("status"), "MISSING"),
        "gate_impact_best_scenario": _text(gate_impact.get("best_scenario"), "MISSING"),
        "gate_impact_best_observe_candidates": gate_impact.get("best_observe_candidates", 0),
        "gate_policy_status": _text(gate_policy.get("status"), "MISSING"),
        "gate_policy_version": _text(gate_policy.get("policy_version"), "MISSING"),
        "gate_policy_observe_only_candidates": gate_policy.get("observe_only_candidates", 0),
        "gate_policy_manual_review_required_candidates": gate_policy.get(
            "manual_review_required_candidates",
            0,
        ),
        "candidate_recovery_status": _text(candidate_recovery.get("status"), "MISSING"),
        "recovered_candidate_count": candidate_recovery.get("recovered_candidate_count", 0),
        "candidate_recovery_manual_review_required_count": candidate_recovery.get(
            "manual_review_required_count",
            0,
        ),
        "research_decision_update_status": _text(
            research_decision_update.get("status"),
            "MISSING",
        ),
        "research_decision_update_go_no_go": _text(
            research_decision_update.get("go_no_go") or go_no_go_matrix.get("go_no_go"),
            "MISSING",
        ),
        "research_decision_update_recommended_action": _text(
            research_decision_update.get("recommended_action")
            or go_no_go_matrix.get("recommended_action"),
            "MISSING",
        ),
        "research_decision_update_required_owner_approval": (
            go_no_go_matrix.get("required_owner_approval")
            if "required_owner_approval" in go_no_go_matrix
            else True
        ),
        "research_decision_update_usable_candidates_before": go_no_go_matrix.get(
            "usable_candidates_before",
            0,
        ),
        "research_decision_update_usable_candidates_after": go_no_go_matrix.get(
            "usable_candidates_after",
            0,
        ),
        "research_decision_update_warnings": ", ".join(_texts(go_no_go_matrix.get("warnings")))
        or "MISSING",
        "research_decision_update_next_task": _text(
            research_decision_update_next_action.get("suggested_codex_task"),
            "MISSING",
        ),
        "shortlist_status": _text(shortlist.get("status"), "MISSING"),
        "shortlist_count": shortlist.get("shortlist_count", 0),
        "shortlist_manual_review_required_count": shortlist.get(
            "manual_review_required_count",
            0,
        ),
        "candidate_cluster_status": _text(candidate_cluster.get("status"), "MISSING"),
        "candidate_cluster_count": candidate_cluster.get("cluster_count", 0),
        "candidate_cluster_representative_count": candidate_cluster.get(
            "representative_count",
            0,
        ),
        "candidate_cluster_weight_path_similarity_status": _text(
            candidate_cluster.get("weight_path_similarity_status"),
            "MISSING",
        ),
        "shadow_shortlist_status": _text(shadow_shortlist.get("status"), "MISSING"),
        "shadow_shortlist_candidate_count": shadow_shortlist.get("shadow_candidate_count", 0),
        "shadow_shortlist_monitoring_ready": shadow_shortlist.get(
            "shadow_monitoring_ready",
            False,
        ),
        "position_advisory_status": _text(
            position_advisory.get("position_advisory_status"),
            "MISSING",
        ),
        "position_advisory_consensus_status": _text(
            position_advisory.get("consensus_target_weight_status"),
            "MISSING",
        ),
        "position_advisory_recommended_action": _text(
            position_advisory.get("recommended_action")
            or position_advisory_actions.get("recommended_action"),
            "MISSING",
        ),
        "position_advisory_owner_approval_required": (
            position_advisory.get("owner_approval_required")
            if "owner_approval_required" in position_advisory
            else True
        ),
        "position_advisory_broker_action_allowed": (
            position_advisory.get("broker_action_allowed")
            if "broker_action_allowed" in position_advisory
            else False
        ),
        "position_review_status": _text(position_review.get("status"), "MISSING"),
        "shadow_observation_readiness": _text(
            position_review.get("shadow_observation_readiness")
            or position_review_decision.get("shadow_observation_readiness"),
            "MISSING",
        ),
        "position_advisory_readiness": _text(
            position_review.get("position_advisory_readiness")
            or position_review_decision.get("position_advisory_readiness"),
            "MISSING",
        ),
        "production_readiness": _text(
            position_review.get("production_readiness")
            or position_review_decision.get("production_readiness"),
            "MISSING",
        ),
        "position_review_recommended_next_action": _text(
            position_review.get("recommended_next_action")
            or position_review_decision.get("recommended_next_action"),
            "MISSING",
        ),
        "shadow_monitor_run_status": _text(shadow_monitor_run.get("status"), "MISSING"),
        "shadow_monitor_run_active_count": shadow_monitor_run_summary.get("active_count", 0),
        "shadow_monitor_run_recommendation": _text(
            shadow_monitor_run_summary.get("summary_recommendation"),
            "MISSING",
        ),
        "shadow_monitor_run_broker_action_allowed": (
            shadow_monitor_run.get("broker_action_allowed")
            if "broker_action_allowed" in shadow_monitor_run
            else False
        ),
        "portfolio_snapshot_status": _text(portfolio_snapshot.get("status"), "MISSING"),
        "portfolio_snapshot_manual_review_required": (
            portfolio_snapshot.get("manual_review_required")
            if "manual_review_required" in portfolio_snapshot
            else True
        ),
        "portfolio_snapshot_broker_imported": portfolio_snapshot.get(
            "broker_imported",
            False,
        ),
        "position_advisory_daily_status": _text(
            position_advisory_daily.get("status"),
            "MISSING",
        ),
        "position_advisory_daily_mode": _text(
            position_advisory_daily.get("mode") or position_advisory_daily_actions.get("mode"),
            "MISSING",
        ),
        "position_advisory_daily_consensus_status": _text(
            position_advisory_daily.get("consensus_status")
            or position_advisory_daily_actions.get("consensus_status"),
            "MISSING",
        ),
        "position_advisory_daily_recommended_action": _text(
            position_advisory_daily.get("recommended_action")
            or position_advisory_daily_actions.get("recommended_action"),
            "MISSING",
        ),
        "position_advisory_daily_broker_action_allowed": (
            position_advisory_daily.get("broker_action_allowed")
            if "broker_action_allowed" in position_advisory_daily
            else False
        ),
        "consensus_drift_status": _text(consensus_drift.get("status"), "MISSING"),
        "consensus_drift_disagreement_status": _text(
            consensus_drift_summary.get("disagreement_status"),
            "MISSING",
        ),
        "consensus_drift_advisory_implication": _text(
            consensus_drift_summary.get("position_advisory_implication"),
            "MISSING",
        ),
        "owner_review_id": _text(owner_review.get("review_id"), "MISSING"),
        "owner_review_decision": _text(owner_review.get("owner_decision"), "MISSING"),
        "owner_review_broker_action_taken": (
            owner_review.get("broker_action_taken")
            if "broker_action_taken" in owner_review
            else False
        ),
        "paper_portfolio_id": _text(
            paper_portfolio_state.get("paper_portfolio_id")
            or paper_portfolio.get("paper_portfolio_id"),
            "MISSING",
        ),
        "paper_portfolio_status": _text(
            paper_portfolio_state.get("state_status") or paper_portfolio.get("status"),
            "MISSING",
        ),
        "paper_portfolio_broker_action_taken": (
            paper_portfolio_state.get("broker_action_taken")
            if "broker_action_taken" in paper_portfolio_state
            else False
        ),
        "advisory_outcome_id": _text(advisory_outcome.get("outcome_id"), "MISSING"),
        "advisory_outcome_status": _text(advisory_outcome.get("status"), "MISSING"),
        "advisory_outcome_data_quality_status": _text(
            advisory_outcome.get("data_quality_status"),
            "MISSING",
        ),
        "owner_attribution_id": _text(owner_attribution.get("attribution_id"), "MISSING"),
        "owner_attribution_status": _text(owner_attribution.get("status"), "MISSING"),
        "owner_attribution_total_reviews": owner_attribution_summary.get("total_reviews", 0),
        "shadow_aging_id": _text(shadow_aging.get("aging_id"), "MISSING"),
        "shadow_aging_status": _text(shadow_aging.get("status"), "MISSING"),
        "shadow_aging_eligible_for_review_count": shadow_aging_summary.get(
            "eligible_for_review_count",
            0,
        ),
        "shadow_aging_downgrade_recommended_count": shadow_aging_summary.get(
            "downgrade_recommended_count",
            0,
        ),
        "weekly_advisory_review_id": _text(
            weekly_advisory_review.get("weekly_review_id"),
            "MISSING",
        ),
        "weekly_advisory_recommendation": _text(
            weekly_advisory_review.get("weekly_recommendation"),
            "MISSING",
        ),
        "weekly_advisory_next_actions": ", ".join(
            _texts(weekly_advisory_review.get("next_actions"))
        ),
        "replay_inventory_id": _text(replay_inventory.get("inventory_id"), "MISSING"),
        "replay_inventory_status": _text(replay_inventory.get("status"), "MISSING"),
        "replay_inventory_total_events": replay_inventory.get("total_replay_events", 0),
        "replay_inventory_pit_safe_count": replay_inventory.get("pit_safe_count", 0),
        "replay_inventory_pit_warning_count": replay_inventory.get("pit_warning_count", 0),
        "replay_inventory_pit_unsafe_count": replay_inventory.get("pit_unsafe_count", 0),
        "replay_inventory_eligible_count": replay_inventory_coverage.get("eligible_count", 0),
        "historical_replay_id": _text(historical_replay.get("replay_id"), "MISSING"),
        "historical_replay_status": _text(historical_replay.get("status"), "MISSING"),
        "historical_replay_event_count": historical_replay.get("replay_event_count", 0),
        "historical_replay_skipped_count": historical_replay.get("skipped_count", 0),
        "historical_replay_generated_variants": ", ".join(
            _texts(historical_replay.get("generated_variants"))
        ),
        "historical_replay_broker_action_present": replay_action_summary.get(
            "broker_action_present",
            False,
        ),
        "backfilled_outcome_id": _text(backfilled_outcome.get("backfill_id"), "MISSING"),
        "backfilled_outcome_status": _text(backfilled_outcome.get("status"), "MISSING"),
        "backfilled_outcome_data_quality_status": _text(
            backfilled_outcome.get("data_quality_status"),
            "MISSING",
        ),
        "backfilled_outcome_available_count": backfilled_outcome.get("available_count", 0),
        "backfilled_outcome_pending_count": backfilled_outcome.get("pending_count", 0),
        "backfilled_outcome_insufficient_data_count": backfilled_outcome.get(
            "insufficient_data_count",
            0,
        ),
        "backfilled_outcome_best_variant": _text(
            variant_performance.get("best_variant") or backfilled_outcome.get("best_variant"),
            "MISSING",
        ),
        "limited_adjustment_vs_no_trade_5d": variant_performance.get(
            "limited_adjustment_vs_no_trade_5d",
            0.0,
        ),
        "historical_paper_sim_id": _text(historical_paper_sim.get("sim_id"), "MISSING"),
        "historical_paper_sim_status": _text(historical_paper_sim.get("status"), "MISSING"),
        "historical_paper_sim_variant": _text(simulated_performance.get("variant"), "MISSING"),
        "historical_paper_sim_total_return": simulated_performance.get("total_return", 0.0),
        "historical_paper_sim_max_drawdown": simulated_performance.get("max_drawdown", 0.0),
        "historical_paper_sim_turnover": simulated_performance.get("turnover", 0.0),
        "historical_paper_sim_relative_to_no_trade": simulated_performance.get(
            "relative_to_no_trade",
            0.0,
        ),
        "replay_performance_review_id": _text(
            replay_performance_review.get("review_id"),
            "MISSING",
        ),
        "replay_performance_review_status": _text(
            replay_performance_review.get("status"),
            "MISSING",
        ),
        "replay_performance_best_variant": _text(
            replay_performance_review.get("best_variant"),
            "MISSING",
        ),
        "replay_performance_available_outcome_count": replay_performance_review.get(
            "available_outcome_count",
            0,
        ),
        "replay_performance_limited_adjustment_vs_no_trade": replay_performance_review.get(
            "limited_adjustment_vs_no_trade",
            0.0,
        ),
        "replay_calibration_recommendation": _text(
            replay_recommendation.get("type"),
            "MISSING",
        ),
        "replay_forward_bridge_status": _text(
            replay_forward_bridge.get("status"),
            "MISSING",
        ),
        "replay_forward_focus": _text(
            (_records(replay_forward_focus.get("focus_items")) or [{}])[0].get("item"),
            "MISSING",
        ),
        "replay_forward_next_action": _text(
            replay_forward_bridge.get("next_action"),
            "MISSING",
        ),
        "outcome_dashboard_id": _text(outcome_dashboard.get("dashboard_id"), "MISSING"),
        "outcome_dashboard_status": _text(outcome_dashboard.get("status"), "MISSING"),
        "outcome_dashboard_available_count": outcome_dashboard.get("available_count", 0),
        "outcome_dashboard_pending_count": outcome_dashboard.get("pending_count", 0),
        "outcome_dashboard_insufficient_count": outcome_dashboard.get(
            "insufficient_data_count",
            0,
        ),
        "outcome_dashboard_top_pending_reason": _text(
            (_records(pending_reason_dashboard.get("top_pending_reasons")) or [{}])[0].get(
                "reason"
            ),
            "MISSING",
        ),
        "outcome_dashboard_next_action": _text(
            pending_reason_dashboard.get("next_action"),
            "MISSING",
        ),
        "outcome_update_review_id": _text(
            outcome_update_review.get("update_review_id"),
            "MISSING",
        ),
        "outcome_update_review_status": _text(
            outcome_update_review.get("status"),
            "MISSING",
        ),
        "outcome_update_review_ready_count": outcome_update_review.get(
            "ready_to_update_count",
            0,
        ),
        "outcome_update_review_blocked_count": outcome_update_review.get(
            "blocked_count",
            0,
        ),
        "outcome_update_review_future_data_used": outcome_update_review.get(
            "future_data_used_in_decision",
            False,
        ),
        "outcome_update_review_expected_available_delta": outcome_update_impact.get(
            "expected_forward_available_delta",
            0,
        ),
        "outcome_update_id": _text(outcome_update.get("outcome_update_id"), "MISSING"),
        "outcome_update_status": _text(outcome_update.get("status"), "MISSING"),
        "outcome_update_updated_count": outcome_update.get("updated_count", 0),
        "outcome_update_skipped_count": outcome_update.get("skipped_count", 0),
        "outcome_update_forward_available_before": outcome_delta_before.get(
            "forward_available",
            0,
        ),
        "outcome_update_forward_available_after": outcome_delta_after.get(
            "forward_available",
            0,
        ),
        "outcome_update_forward_pending_before": outcome_delta_before.get(
            "forward_pending",
            0,
        ),
        "outcome_update_forward_pending_after": outcome_delta_after.get(
            "forward_pending",
            0,
        ),
        "rolling_evidence_refresh_id": _text(
            rolling_evidence_refresh.get("refresh_id"),
            "MISSING",
        ),
        "rolling_evidence_refresh_status": _text(
            rolling_evidence_refresh.get("status"),
            "MISSING",
        ),
        "rolling_evidence_refresh_material_change": rolling_evidence_refresh.get(
            "material_change",
            False,
        ),
        "rolling_limited_vs_notrade_count_before": rolling_delta_before.get(
            "limited_vs_notrade_available_count",
            0,
        ),
        "rolling_limited_vs_notrade_count_after": rolling_delta_after.get(
            "limited_vs_notrade_available_count",
            0,
        ),
        "rolling_consensus_risk_before": _text(
            rolling_delta_before.get("consensus_target_risk"),
            "MISSING",
        ),
        "rolling_consensus_risk_after": _text(
            rolling_delta_after.get("consensus_target_risk"),
            "MISSING",
        ),
        "rolling_weekly_advisory_review_id": _text(
            refreshed_artifacts.get("weekly_advisory_review_id"),
            "MISSING",
        ),
        "evidence_trend_id": _text(evidence_trend.get("trend_id"), "MISSING"),
        "evidence_trend_status": _text(
            confidence_trend_summary.get("trend_status") or evidence_trend.get("trend_status"),
            "MISSING",
        ),
        "evidence_trend_confidence_change": _text(
            confidence_trend_summary.get("confidence_change"),
            "MISSING",
        ),
        "evidence_trend_next_action": _text(
            confidence_trend_summary.get("next_action"),
            "MISSING",
        ),
        "forward_outcome_decision_id": _text(
            forward_outcome_decision.get("decision_id"),
            "MISSING",
        ),
        "forward_outcome_decision_action": _text(
            forward_go_no_go_matrix.get("recommended_action")
            or forward_outcome_decision.get("recommended_action"),
            "MISSING",
        ),
        "forward_rule_calibration_readiness": _text(
            forward_go_no_go_matrix.get("rule_calibration_readiness")
            or forward_outcome_decision.get("rule_calibration_readiness"),
            "MISSING",
        ),
        "forward_next_due_scan_date": forward_next_due_scan_date,
        "no_promotion_review_id": _text(
            no_promotion_review_manifest.get("review_id"),
            "MISSING",
        ),
        "no_promotion_review_status": _text(
            no_promotion_review_manifest.get("status"),
            "MISSING",
        ),
        "no_promotion_top_reason": no_promotion_top_reason,
        "no_promotion_gate_assessment": _text(
            no_promotion_reason_summary.get("gate_assessment"),
            "MISSING",
        ),
        "near_miss_id": _text(near_miss_candidates_manifest.get("near_miss_id"), "MISSING"),
        "near_miss_candidate_count": near_miss_candidates_manifest.get("candidate_count", 0),
        "near_miss_focus_families": ", ".join(
            _texts(near_miss_family_summary.get("recommended_focus_families"))
        )
        or "MISSING",
        "cash_buffer_attribution_id": _text(
            cash_buffer_attribution_manifest.get("attribution_id"),
            "MISSING",
        ),
        "cash_buffer_primary_failure": _text(
            cash_buffer_failure_reason.get("primary_failure_reason"),
            "MISSING",
        ),
        "cash_buffer_interpretation": _text(
            cash_buffer_effect_summary.get("overall_interpretation"),
            "MISSING",
        ),
        "search_coverage_gap_id": _text(
            search_coverage_gap_manifest.get("coverage_gap_id"),
            "MISSING",
        ),
        "targeted_v3_recommended_focus": ", ".join(
            _texts(targeted_v3_recommendations.get("recommended_focus"))
        )
        or "MISSING",
        "targeted_search_v3_id": _text(
            targeted_search_v3_manifest.get("v3_matrix_id"),
            "MISSING",
        ),
        "targeted_search_v3_variant_count": targeted_search_v3_manifest.get(
            "variant_count",
            0,
        ),
        "targeted_v3_backfill_id": _text(
            targeted_v3_backfill_manifest.get("v3_backfill_id"),
            "MISSING",
        ),
        "targeted_v3_backfill_status": _text(
            targeted_v3_backfill_manifest.get("status"),
            "MISSING",
        ),
        "targeted_v3_data_quality_status": _text(
            targeted_v3_backfill_manifest.get("data_quality_status"),
            "MISSING",
        ),
        "near_miss_ab_id": _text(near_miss_ab_comparison_manifest.get("ab_id"), "MISSING"),
        "near_miss_ab_best_v3_variant": _text(
            ab_winner_summary.get("best_v3_variant"),
            "MISSING",
        ),
        "promotion_threshold_sensitivity_id": _text(
            promotion_threshold_sensitivity_manifest.get("sensitivity_id"),
            "MISSING",
        ),
        "promotion_threshold_relaxed_only_count": threshold_candidate_impact.get(
            "relaxed_only_count",
            0,
        ),
        "candidate_promotion_v2_id": _text(
            candidate_promotion_v2_manifest.get("promotion_v2_id"),
            "MISSING",
        ),
        "candidate_promotion_v2_decision": candidate_promotion_v2_decision,
        "candidate_promotion_v2_promoted_count": promotion_v2_decision.get(
            "promoted_count",
            0,
        ),
        "next_formal_or_search_plan_id": _text(
            next_formal_or_search_plan_manifest.get("plan_id"),
            "MISSING",
        ),
        "next_formal_or_search_plan_decision": next_formal_or_search_plan_decision,
        "next_formal_or_search_plan_action": _text(
            next_plan_decision.get("recommended_next_action"),
            "MISSING",
        ),
        "signal_failure_taxonomy_id": _text(
            signal_failure_taxonomy_manifest.get("taxonomy_id"),
            "MISSING",
        ),
        "signal_failure_mode_count": signal_failure_taxonomy_manifest.get(
            "failure_mode_count",
            0,
        ),
        "candidate_signal_ledger_id": _text(
            candidate_signal_ledger_manifest.get("ledger_id"),
            "MISSING",
        ),
        "candidate_signal_dominant_failure": _text(
            candidate_signal_summary.get("dominant_failure_mode"),
            "MISSING",
        ),
        "candidate_signal_unstable_method_count": candidate_signal_summary.get(
            "unstable_method_count",
            0,
        ),
        "signal_churn_root_cause_id": _text(
            signal_churn_root_cause_manifest.get("root_cause_id"),
            "MISSING",
        ),
        "signal_churn_dominant_root_cause": _text(
            churn_root_cause_summary.get("dominant_root_cause"),
            "MISSING",
        ),
        "regime_mismatch_attribution_id": _text(
            regime_mismatch_attribution_manifest.get("mismatch_id"),
            "MISSING",
        ),
        "regime_mismatch_dominant_type": _text(
            regime_mismatch_summary.get("dominant_mismatch_type"),
            "MISSING",
        ),
        "candidate_quality_filter_design_id": _text(
            candidate_quality_filter_design_manifest.get("filter_design_id"),
            "MISSING",
        ),
        "candidate_quality_filter_count": len(_records(proposed_quality_filters.get("filters"))),
        "filtered_candidate_backfill_id": _text(
            filtered_candidate_backfill_manifest.get("filtered_backfill_id"),
            "MISSING",
        ),
        "filtered_candidate_data_quality_status": _text(
            filtered_candidate_backfill_manifest.get("data_quality_status"),
            "MISSING",
        ),
        "filtered_vs_original_comparison_id": _text(
            filtered_vs_original_comparison_manifest.get("comparison_id"),
            "MISSING",
        ),
        "filtered_vs_original_recommendation": _text(
            filtered_improvement_summary.get("recommendation"),
            "MISSING",
        ),
        "signal_gate_experiment_id": _text(
            signal_gate_experiment_manifest.get("signal_gate_experiment_id"),
            "MISSING",
        ),
        "signal_gate_recommended_next_action": _text(
            signal_gate_experiment_summary.get("recommended_next_action"),
            "MISSING",
        ),
        "filtered_candidate_promotion_review_id": _text(
            filtered_candidate_promotion_review_manifest.get("filtered_review_id"),
            "MISSING",
        ),
        "filtered_candidate_promotion_decision": _text(
            filtered_promotion_decision.get("decision"),
            "MISSING",
        ),
        "owner_signal_roadmap_id": _text(
            owner_signal_roadmap_manifest.get("owner_signal_roadmap_id"),
            "MISSING",
        ),
        "owner_signal_recommended_action": _text(
            owner_signal_roadmap_summary.get("recommended_owner_action"),
            "MISSING",
        ),
        "filtered_candidate_evidence_id": _text(
            filtered_candidate_evidence_manifest.get("evidence_id"),
            "MISSING",
        ),
        "filtered_candidate_evidence_status": _text(
            filtered_candidate_evidence_summary.get("evidence_status"),
            "MISSING",
        ),
        "filtered_candidate_primary_improvements": ", ".join(
            _texts(filtered_candidate_evidence_summary.get("primary_improvements"))
        )
        or "MISSING",
        "filtered_candidate_primary_weaknesses": ", ".join(
            _texts(filtered_candidate_evidence_summary.get("primary_weaknesses"))
        )
        or "MISSING",
        "median_regime_filter_spec_id": _text(
            median_regime_filter_spec_manifest.get("spec_id"),
            "MISSING",
        ),
        "median_regime_filter_contract_status": _text(
            median_regime_filter_contract.get("contract_status"),
            "MISSING",
        ),
        "median_regime_filter_complexity": _text(
            median_regime_filter_contract.get("formalization_complexity"),
            "MISSING",
        ),
        "filtered_candidate_stress_id": _text(
            filtered_candidate_stress_manifest.get("stress_backfill_id"),
            "MISSING",
        ),
        "filtered_candidate_stress_status": _text(
            filtered_candidate_stress_summary.get("stress_robustness_status"),
            "MISSING",
        ),
        "filtered_candidate_stress_improved_count": filtered_candidate_stress_summary.get(
            "improved_count",
            0,
        ),
        "drawdown_mismatch_reduction_id": _text(
            drawdown_mismatch_reduction_manifest.get("reduction_id"),
            "MISSING",
        ),
        "drawdown_mismatch_reduction_status": _text(
            mismatch_reduction_summary.get("drawdown_mismatch_reduction_status"),
            "MISSING",
        ),
        "drawdown_mismatch_reduction_pct": mismatch_reduction_summary.get(
            "reduction_pct",
            0.0,
        ),
        "flip_rotation_reduction_id": _text(
            flip_rotation_reduction_manifest.get("flip_reduction_id"),
            "MISSING",
        ),
        "flip_reduction_status": _text(
            flip_rotation_reduction_summary.get("flip_reduction_status"),
            "MISSING",
        ),
        "rotation_reduction_status": _text(
            flip_rotation_reduction_summary.get("rotation_reduction_status"),
            "MISSING",
        ),
        "filtered_candidate_ab_review_id": _text(
            filtered_candidate_ab_manifest.get("ab_review_id"),
            "MISSING",
        ),
        "filtered_candidate_ab_status": _text(
            filtered_candidate_ab_summary.get("overall_ab_status"),
            "MISSING",
        ),
        "filtered_candidate_ab_next_action": _text(
            filtered_candidate_ab_summary.get("recommended_next_action"),
            "MISSING",
        ),
        "signal_gate_confirmation_id": _text(
            signal_gate_confirmation_manifest.get("confirmation_id"),
            "MISSING",
        ),
        "signal_gate_confirmation_target_count": signal_gate_confirmation_target_count,
        "signal_gate_confirmation_auto_apply": (
            signal_gate_confirmation_targets.get("auto_apply")
            if "auto_apply" in signal_gate_confirmation_targets
            else False
        ),
        "filtered_formalization_readiness_id": _text(
            filtered_formalization_manifest.get("readiness_id"),
            "MISSING",
        ),
        "filtered_formalization_decision": _text(
            formalization_readiness_decision.get("decision"),
            "MISSING",
        ),
        "filtered_formalization_confidence": _text(
            formalization_readiness_decision.get("confidence"),
            "MISSING",
        ),
        "filtered_formalization_can_write_official_target_weights": (
            formalization_readiness_decision.get("can_write_official_target_weights")
            if "can_write_official_target_weights" in formalization_readiness_decision
            else False
        ),
        "owner_filtered_candidate_review_id": _text(
            owner_filtered_candidate_manifest.get("owner_review_id"),
            "MISSING",
        ),
        "owner_filtered_candidate_action": _text(
            owner_filtered_candidate_summary.get("recommended_owner_action"),
            "MISSING",
        ),
        "owner_filtered_candidate_readiness_decision": _text(
            owner_filtered_candidate_summary.get("readiness_decision"),
            "MISSING",
        ),
        "filtered_next_decision_id": _text(
            filtered_next_decision_manifest.get("decision_id"),
            "MISSING",
        ),
        "filtered_next_decision": _text(filtered_next_decision.get("decision"), "MISSING"),
        "filtered_next_action": _text(filtered_next_decision.get("next_action"), "MISSING"),
        "formal_research_method_contract_id": _text(
            formal_research_method_contract_manifest.get("contract_id"),
            "MISSING",
        ),
        "formal_research_method_status": _text(
            formal_research_method_decision.get("formal_research_method_status"),
            "MISSING",
        ),
        "formal_research_method_promotion_state": _text(
            formal_research_method_decision.get("promotion_state"),
            "MISSING",
        ),
        "formal_research_method_paper_shadow_eligibility": _text(
            formal_research_method_decision.get("paper_shadow_eligibility"),
            "MISSING",
        ),
        "formal_research_method_safety_boundary_status": _text(
            formal_research_method_contract.get("safety_boundary_status")
            or formal_research_method_decision.get("safety_boundary_status"),
            "MISSING",
        ),
        "formal_research_method_validation_status": _text(
            formal_research_method_validation.get("status"),
            "MISSING",
        ),
        "promotion_threshold_calibration_id": _text(
            promotion_threshold_manifest.get("calibration_id"),
            "MISSING",
        ),
        "promotion_threshold_policy_id": _text(
            promotion_threshold_report.get("policy_id"),
            "MISSING",
        ),
        "promotion_threshold_policy_version": _text(
            promotion_threshold_report.get("policy_version"),
            "MISSING",
        ),
        "promotion_threshold_status": _text(
            promotion_threshold_report.get("status"),
            "MISSING",
        ),
        "promotion_threshold_current_interpretation": _text(
            promotion_threshold_report.get("current_threshold_interpretation"),
            "MISSING",
        ),
        "promotion_threshold_stress_required": _text(
            promotion_threshold_report.get("stress_required"),
            "MISSING",
        ),
        "promotion_threshold_confirmation_minimum": promotion_threshold_report.get(
            "confirmation_target_minimum",
            "MISSING",
        ),
        "promotion_threshold_validation_status": _text(
            promotion_threshold_validation.get("status"),
            "MISSING",
        ),
        "promotion_threshold_next_action": _text(
            promotion_threshold_report.get("next_required_action"),
            "MISSING",
        ),
        "paper_shadow_protocol_id": _text(
            paper_shadow_protocol_manifest.get("protocol_id"),
            "MISSING",
        ),
        "paper_shadow_protocol_status": _text(
            paper_shadow_protocol.get("protocol_status"),
            "MISSING",
        ),
        "paper_shadow_protocol_eligibility_status": _text(
            paper_shadow_protocol.get("eligibility_status"),
            "MISSING",
        ),
        "paper_shadow_protocol_min_observation_days": _mapping(
            paper_shadow_protocol.get("required_observation_period")
        ).get("minimum_trading_days", "MISSING"),
        "paper_shadow_protocol_validation_status": _text(
            paper_shadow_protocol_validation.get("status"),
            "MISSING",
        ),
        "paper_shadow_protocol_next_action": _text(
            paper_shadow_protocol.get("next_required_action"),
            "MISSING",
        ),
        "paper_shadow_daily_observation_id": _text(
            paper_shadow_daily_manifest.get("observation_id"),
            "MISSING",
        ),
        "paper_shadow_daily_candidate": _text(
            paper_shadow_daily_observation.get("candidate"),
            "MISSING",
        ),
        "paper_shadow_daily_date": _text(
            paper_shadow_daily_observation.get("observation_date"),
            "MISSING",
        ),
        "paper_shadow_daily_status": _text(
            paper_shadow_daily_observation.get("observation_status"),
            "MISSING",
        ),
        "paper_shadow_daily_signal_output": _text(
            paper_shadow_daily_review.get("signal_output"),
            "MISSING",
        ),
        "paper_shadow_daily_risk_state": _text(
            paper_shadow_daily_review.get("risk_off_risk_on_state"),
            "MISSING",
        ),
        "paper_shadow_daily_next_action": _text(
            paper_shadow_daily_observation.get("next_required_action"),
            "MISSING",
        ),
        "paper_shadow_daily_validation_status": _text(
            paper_shadow_daily_validation.get("status"),
            "MISSING",
        ),
        "paper_shadow_drift_monitor_id": _text(
            paper_shadow_drift_manifest.get("monitor_id"),
            "MISSING",
        ),
        "paper_shadow_drift_candidate": _text(
            paper_shadow_drift_report.get("candidate"),
            "MISSING",
        ),
        "paper_shadow_drift_observation_id": _text(
            paper_shadow_drift_report.get("observation_id"),
            "MISSING",
        ),
        "paper_shadow_drift_severity": _text(
            paper_shadow_drift_report.get("drift_severity"),
            "MISSING",
        ),
        "paper_shadow_drift_blocking_count": paper_shadow_drift_report.get(
            "blocking_count",
            "MISSING",
        ),
        "paper_shadow_drift_warning_count": paper_shadow_drift_report.get(
            "warning_count",
            "MISSING",
        ),
        "paper_shadow_drift_next_action": _text(
            paper_shadow_drift_report.get("next_action"),
            "MISSING",
        ),
        "paper_shadow_drift_validation_status": _text(
            paper_shadow_drift_validation.get("status"),
            "MISSING",
        ),
        "paper_shadow_weekly_review_id": _text(
            paper_shadow_weekly_manifest.get("weekly_review_id"),
            "MISSING",
        ),
        "paper_shadow_weekly_candidate": _text(
            paper_shadow_weekly_review.get("candidate"),
            "MISSING",
        ),
        "paper_shadow_weekly_window": paper_shadow_weekly_window,
        "paper_shadow_weekly_decision": _text(
            paper_shadow_weekly_review.get("weekly_decision"),
            "MISSING",
        ),
        "paper_shadow_weekly_coverage_classification": _text(
            paper_shadow_weekly_review.get("coverage_classification")
            or paper_shadow_weekly_summary.get("coverage_classification"),
            "MISSING",
        ),
        "paper_shadow_weekly_coverage_status": _text(
            paper_shadow_weekly_review.get("coverage_status")
            or paper_shadow_weekly_summary.get("coverage_status"),
            "MISSING",
        ),
        "paper_shadow_weekly_coverage_safe_for_continuation": (
            paper_shadow_weekly_review.get(
                "coverage_safe_for_continuation",
                paper_shadow_weekly_summary.get(
                    "coverage_safe_for_continuation",
                    "MISSING",
                ),
            )
        ),
        "paper_shadow_weekly_missing_inputs": paper_shadow_weekly_missing_inputs,
        "paper_shadow_weekly_drift_trend": paper_shadow_weekly_drift_trend_text,
        "paper_shadow_weekly_validation_status": _text(
            paper_shadow_weekly_validation.get("status"),
            "MISSING",
        ),
        "candidate_decision_ledger_id": _text(
            candidate_decision_ledger_manifest.get("ledger_run_id"),
            "MISSING",
        ),
        "candidate_decision_record_id": _text(
            candidate_decision_record.get("record_id"),
            "MISSING",
        ),
        "candidate_decision_candidate": _text(
            candidate_decision_record.get("candidate"),
            "MISSING",
        ),
        "candidate_decision_evidence_status": _text(
            candidate_decision_record.get("evidence_status"),
            "MISSING",
        ),
        "candidate_decision_stress_result": _text(
            candidate_decision_record.get("stress_result"),
            "MISSING",
        ),
        "candidate_decision_mismatch_result": _text(
            candidate_decision_record.get("mismatch_result"),
            "MISSING",
        ),
        "candidate_decision_rotation_result": _text(
            candidate_decision_record.get("rotation_result"),
            "MISSING",
        ),
        "candidate_decision_ab_result": _text(
            candidate_decision_record.get("ab_result"),
            "MISSING",
        ),
        "candidate_decision_confirmation_count": candidate_decision_record.get(
            "confirmation_count",
            "MISSING",
        ),
        "candidate_decision_owner_action": _text(
            candidate_decision_record.get("owner_action"),
            "MISSING",
        ),
        "candidate_decision_final_decision": _text(
            candidate_decision_record.get("final_decision"),
            "MISSING",
        ),
        "candidate_decision_next_action": _text(
            candidate_decision_record.get("next_required_action"),
            "MISSING",
        ),
        "candidate_decision_ledger_validation_status": _text(
            candidate_decision_ledger_validation.get("status"),
            "MISSING",
        ),
        "evidence_staleness_monitor_id": _text(
            evidence_staleness_manifest.get("monitor_id"),
            "MISSING",
        ),
        "evidence_freshness_status": _text(
            evidence_staleness_report.get("evidence_freshness_status"),
            "MISSING",
        ),
        "evidence_coverage_status": _text(
            evidence_staleness_report.get("coverage_status"),
            "MISSING",
        ),
        "evidence_weekly_review_coverage_classification": _text(
            evidence_staleness_report.get("weekly_review_coverage_classification"),
            "MISSING",
        ),
        "evidence_weekly_review_coverage_safe_for_continuation": (
            evidence_staleness_report.get(
                "weekly_review_coverage_safe_for_continuation",
                "MISSING",
            )
        ),
        "evidence_requested_as_of": _text(
            evidence_staleness_report.get("requested_as_of")
            or evidence_staleness_report.get("as_of"),
            "MISSING",
        ),
        "evidence_freshness_reference_date": _text(
            evidence_staleness_report.get("freshness_reference_date"),
            "MISSING",
        ),
        "evidence_latest_complete_market_date": _text(
            evidence_staleness_report.get("latest_complete_market_date"),
            "MISSING",
        ),
        "evidence_market_calendar_status": _text(
            evidence_staleness_report.get("market_calendar_status"),
            "MISSING",
        ),
        "evidence_stale_artifacts": (
            ", ".join(_texts(evidence_staleness_report.get("stale_artifacts")))
            or "none"
        ),
        "evidence_blocking_artifacts": (
            ", ".join(_texts(evidence_staleness_report.get("blocking_artifacts")))
            or "none"
        ),
        "evidence_missing_artifacts": (
            ", ".join(_texts(evidence_staleness_report.get("missing_artifacts")))
            or "none"
        ),
        "evidence_next_refresh_action": _text(
            evidence_staleness_report.get("next_refresh_action"),
            "MISSING",
        ),
        "evidence_safe_to_continue_shadow": evidence_staleness_report.get(
            "safe_to_continue_shadow",
            "MISSING",
        ),
        "evidence_safety_boundary_status": _text(
            evidence_staleness_report.get("safety_boundary_status"),
            "MISSING",
        ),
        "evidence_staleness_validation_status": _text(
            evidence_staleness_validation.get("status"),
            "MISSING",
        ),
        "shadow_continuation_readiness_id": _text(
            shadow_continuation_manifest.get("readiness_id"),
            "MISSING",
        ),
        "shadow_continuation_readiness": _text(
            shadow_continuation_report.get("shadow_continuation_readiness"),
            "MISSING",
        ),
        "shadow_continuation_safe_to_continue_shadow": shadow_continuation_report.get(
            "safe_to_continue_shadow",
            "MISSING",
        ),
        "shadow_continuation_missing_artifacts": (
            ", ".join(_texts(shadow_continuation_report.get("missing_artifacts")))
            or "none"
        ),
        "shadow_continuation_blocking_artifacts": (
            ", ".join(_texts(shadow_continuation_report.get("blocking_artifacts")))
            or "none"
        ),
        "shadow_continuation_stale_artifacts": (
            ", ".join(_texts(shadow_continuation_report.get("stale_artifacts")))
            or "none"
        ),
        "shadow_continuation_coverage_status": _text(
            shadow_continuation_report.get("coverage_status"),
            "MISSING",
        ),
        "shadow_continuation_manual_review_required": shadow_continuation_report.get(
            "manual_review_required",
            "MISSING",
        ),
        "shadow_continuation_next_required_action": _text(
            shadow_continuation_report.get("next_required_action"),
            "MISSING",
        ),
        "shadow_continuation_data_validation_status": _text(
            shadow_continuation_report.get("data_validation_status"),
            "MISSING",
        ),
        "shadow_continuation_safety_boundary_status": _text(
            shadow_continuation_report.get("safety_boundary_status"),
            "MISSING",
        ),
        "shadow_continuation_validation_status": _text(
            shadow_continuation_validation.get("status"),
            "MISSING",
        ),
        "stress_scenario_library_run_id": _text(
            stress_scenario_manifest.get("library_run_id"),
            "MISSING",
        ),
        "stress_scenario_library_id": _text(
            stress_scenario_library.get("stress_scenario_library_id"),
            "MISSING",
        ),
        "stress_scenario_count": stress_scenario_library.get(
            "scenario_count",
            "MISSING",
        ),
        "stress_scenario_required_present": stress_scenario_library.get(
            "required_scenarios_present",
            "MISSING",
        ),
        "stress_scenario_candidate_validation_use": _text(
            stress_scenario_library.get("candidate_validation_use"),
            "MISSING",
        ),
        "stress_scenario_next_action": _text(
            stress_scenario_library.get("next_validation_action"),
            "MISSING",
        ),
        "stress_scenario_validation_status": _text(
            stress_scenario_validation.get("status"),
            "MISSING",
        ),
        "drawdown_casebook_run_id": _text(
            drawdown_casebook_manifest.get("casebook_run_id"),
            "MISSING",
        ),
        "drawdown_casebook_id": _text(
            drawdown_event_casebook.get("drawdown_casebook_id"),
            "MISSING",
        ),
        "drawdown_casebook_event_count": drawdown_event_casebook.get(
            "event_count",
            "MISSING",
        ),
        "drawdown_casebook_worst_event": _text(
            drawdown_event_casebook.get("worst_event"),
            "MISSING",
        ),
        "drawdown_casebook_regime_coverage": (
            ", ".join(_texts(drawdown_event_casebook.get("regime_coverage")))
            or "MISSING"
        ),
        "drawdown_casebook_next_action": _text(
            drawdown_event_casebook.get("next_review_action"),
            "MISSING",
        ),
        "drawdown_casebook_validation_status": _text(
            drawdown_casebook_validation.get("status"),
            "MISSING",
        ),
        "flip_rotation_casebook_run_id": _text(
            flip_rotation_casebook_manifest.get("casebook_run_id"),
            "MISSING",
        ),
        "flip_rotation_casebook_id": _text(
            flip_rotation_event_casebook.get("flip_rotation_casebook_id"),
            "MISSING",
        ),
        "flip_rotation_casebook_event_count": flip_rotation_event_casebook.get(
            "event_count",
            "MISSING",
        ),
        "flip_rotation_useful_count": flip_rotation_event_casebook.get(
            "useful_flip_count",
            "MISSING",
        ),
        "flip_rotation_false_positive_count": flip_rotation_event_casebook.get(
            "false_positive_count",
            "MISSING",
        ),
        "flip_rotation_dominant_trigger": _text(
            flip_rotation_event_casebook.get("dominant_trigger_signal"),
            "MISSING",
        ),
        "flip_rotation_next_action": _text(
            flip_rotation_event_casebook.get("next_review_action"),
            "MISSING",
        ),
        "flip_rotation_casebook_validation_status": _text(
            flip_rotation_casebook_validation.get("status"),
            "MISSING",
        ),
        "replay_calibration_priority": _text(replay_recommendation.get("priority"), "MISSING"),
        "replay_calibration_requires_owner_approval": (
            replay_recommendation.get("requires_owner_approval")
            if "requires_owner_approval" in replay_recommendation
            else True
        ),
        "replay_next_action": _text(
            replay_forward_bridge.get("next_action")
            or replay_performance_review.get("next_action")
            or replay_recommendation.get("type"),
            "MISSING",
        ),
        "sweep_leaderboard": "" if leaderboard_path is None else str(leaderboard_path),
        "promotion_manifest": "" if promotion_path is None else str(promotion_path),
        "evidence_summary": "" if evidence_path is None else str(evidence_path),
        "shadow_monitor_report": ("" if shadow_monitor_path is None else str(shadow_monitor_path)),
        "candidate_evidence_summary": (
            "" if candidate_evidence_path is None else str(candidate_evidence_path)
        ),
        "observe_pool": "" if observe_pool_path is None else str(observe_pool_path),
        "overnight_readiness_report": (
            "" if overnight_readiness_path is None else str(overnight_readiness_path)
        ),
        "research_decision": (
            "" if research_decision_path is None else str(research_decision_path)
        ),
        "evidence_diagnosis": (
            "" if evidence_diagnosis_path is None else str(evidence_diagnosis_path)
        ),
        "gate_impact": "" if gate_impact_path is None else str(gate_impact_path),
        "gate_policy": "" if gate_policy_path is None else str(gate_policy_path),
        "candidate_recovery": (
            "" if candidate_recovery_path is None else str(candidate_recovery_path)
        ),
        "research_decision_update": (
            "" if research_decision_update_path is None else str(research_decision_update_path)
        ),
        "shortlist": "" if shortlist_path is None else str(shortlist_path),
        "candidate_cluster": (
            "" if candidate_cluster_path is None else str(candidate_cluster_path)
        ),
        "shadow_shortlist": "" if shadow_shortlist_path is None else str(shadow_shortlist_path),
        "position_advisory": (
            "" if position_advisory_path is None else str(position_advisory_path)
        ),
        "position_review": "" if position_review_path is None else str(position_review_path),
        "shadow_monitor_run": (
            "" if shadow_monitor_run_path is None else str(shadow_monitor_run_path)
        ),
        "portfolio_snapshot": (
            "" if portfolio_snapshot_path is None else str(portfolio_snapshot_path)
        ),
        "position_advisory_daily": (
            "" if position_advisory_daily_path is None else str(position_advisory_daily_path)
        ),
        "consensus_drift": "" if consensus_drift_path is None else str(consensus_drift_path),
        "owner_review": "" if owner_review_path is None else str(owner_review_path),
        "paper_portfolio": "" if paper_portfolio_path is None else str(paper_portfolio_path),
        "advisory_outcome": "" if advisory_outcome_path is None else str(advisory_outcome_path),
        "owner_attribution": (
            "" if owner_attribution_path is None else str(owner_attribution_path)
        ),
        "shadow_aging": "" if shadow_aging_path is None else str(shadow_aging_path),
        "weekly_advisory_review": (
            "" if weekly_advisory_review_path is None else str(weekly_advisory_review_path)
        ),
        "replay_inventory": "" if replay_inventory_path is None else str(replay_inventory_path),
        "historical_replay": "" if historical_replay_path is None else str(historical_replay_path),
        "backfilled_outcome": (
            "" if backfilled_outcome_path is None else str(backfilled_outcome_path)
        ),
        "historical_paper_sim": (
            "" if historical_paper_sim_path is None else str(historical_paper_sim_path)
        ),
        "replay_performance_review": (
            "" if replay_performance_review_path is None else str(replay_performance_review_path)
        ),
        "replay_forward_bridge": (
            "" if replay_forward_bridge_path is None else str(replay_forward_bridge_path)
        ),
        "outcome_dashboard": (
            "" if outcome_dashboard_path is None else str(outcome_dashboard_path)
        ),
        "outcome_update_review": (
            "" if outcome_update_review_path is None else str(outcome_update_review_path)
        ),
        "outcome_update": "" if outcome_update_path is None else str(outcome_update_path),
        "rolling_evidence_refresh": (
            "" if rolling_evidence_refresh_path is None else str(rolling_evidence_refresh_path)
        ),
        "evidence_trend": "" if evidence_trend_path is None else str(evidence_trend_path),
        "forward_outcome_decision": (
            "" if forward_outcome_decision_path is None else str(forward_outcome_decision_path)
        ),
        "no_promotion_review": (
            "" if no_promotion_review_path is None else str(no_promotion_review_path)
        ),
        "near_miss_candidates": (
            "" if near_miss_candidates_path is None else str(near_miss_candidates_path)
        ),
        "cash_buffer_attribution": (
            "" if cash_buffer_attribution_path is None else str(cash_buffer_attribution_path)
        ),
        "search_coverage_gap": (
            "" if search_coverage_gap_path is None else str(search_coverage_gap_path)
        ),
        "targeted_search_v3": (
            "" if targeted_search_v3_path is None else str(targeted_search_v3_path)
        ),
        "targeted_v3_backfill": (
            "" if targeted_v3_backfill_path is None else str(targeted_v3_backfill_path)
        ),
        "near_miss_ab_comparison": (
            "" if near_miss_ab_comparison_path is None else str(near_miss_ab_comparison_path)
        ),
        "promotion_threshold_sensitivity": (
            ""
            if promotion_threshold_sensitivity_path is None
            else str(promotion_threshold_sensitivity_path)
        ),
        "candidate_promotion_v2": (
            "" if candidate_promotion_v2_path is None else str(candidate_promotion_v2_path)
        ),
        "next_formal_or_search_plan": (
            "" if next_formal_or_search_plan_path is None else str(next_formal_or_search_plan_path)
        ),
        "signal_failure_taxonomy": (
            "" if signal_failure_taxonomy_path is None else str(signal_failure_taxonomy_path)
        ),
        "candidate_signal_ledger": (
            "" if candidate_signal_ledger_path is None else str(candidate_signal_ledger_path)
        ),
        "signal_churn_root_cause": (
            "" if signal_churn_root_cause_path is None else str(signal_churn_root_cause_path)
        ),
        "regime_mismatch_attribution": (
            ""
            if regime_mismatch_attribution_path is None
            else str(regime_mismatch_attribution_path)
        ),
        "candidate_quality_filter_design": (
            ""
            if candidate_quality_filter_design_path is None
            else str(candidate_quality_filter_design_path)
        ),
        "filtered_candidate_backfill": (
            ""
            if filtered_candidate_backfill_path is None
            else str(filtered_candidate_backfill_path)
        ),
        "filtered_vs_original_comparison": (
            ""
            if filtered_vs_original_comparison_path is None
            else str(filtered_vs_original_comparison_path)
        ),
        "signal_gate_experiment": (
            "" if signal_gate_experiment_path is None else str(signal_gate_experiment_path)
        ),
        "filtered_candidate_promotion_review": (
            ""
            if filtered_candidate_promotion_review_path is None
            else str(filtered_candidate_promotion_review_path)
        ),
        "owner_signal_roadmap": (
            "" if owner_signal_roadmap_path is None else str(owner_signal_roadmap_path)
        ),
        "formal_research_method_contract": (
            ""
            if formal_research_method_contract_path is None
            else str(formal_research_method_contract_path)
        ),
        "promotion_gate_threshold_calibration": (
            ""
            if promotion_threshold_calibration_path is None
            else str(promotion_threshold_calibration_path)
        ),
        "paper_shadow_protocol": (
            "" if paper_shadow_protocol_path is None else str(paper_shadow_protocol_path)
        ),
        "paper_shadow_daily": (
            "" if paper_shadow_daily_path is None else str(paper_shadow_daily_path)
        ),
        "paper_shadow_drift_monitor": (
            ""
            if paper_shadow_drift_monitor_path is None
            else str(paper_shadow_drift_monitor_path)
        ),
        "paper_shadow_weekly_review": (
            ""
            if paper_shadow_weekly_review_path is None
            else str(paper_shadow_weekly_review_path)
        ),
        "candidate_decision_ledger": (
            ""
            if candidate_decision_ledger_path is None
            else str(candidate_decision_ledger_path)
        ),
        "evidence_staleness_monitor": (
            ""
            if evidence_staleness_monitor_path is None
            else str(evidence_staleness_monitor_path)
        ),
        "shadow_continuation_readiness_report": (
            ""
            if shadow_continuation_readiness_path is None
            else str(shadow_continuation_readiness_path)
        ),
        "stress_scenario_library": (
            ""
            if stress_scenario_library_path is None
            else str(stress_scenario_library_path)
        ),
        "drawdown_event_casebook": (
            ""
            if drawdown_event_casebook_path is None
            else str(drawdown_event_casebook_path)
        ),
        "flip_rotation_event_casebook": (
            ""
            if flip_rotation_event_casebook_path is None
            else str(flip_rotation_event_casebook_path)
        ),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "production_candidate_generated": (
            leaderboard.get("production_candidate_generated") is True
            or _mapping(promotion).get("production_candidate_generated") is True
            or _any_payload_flag_true(
                (
                    evidence_diagnosis,
                    gate_impact,
                    gate_policy,
                    candidate_recovery,
                    research_decision_update,
                    shortlist,
                    candidate_cluster,
                    shadow_shortlist,
                    position_advisory,
                    position_advisory_actions,
                    position_review,
                    position_review_decision,
                    shadow_monitor_run,
                    shadow_monitor_run_summary,
                    portfolio_snapshot,
                    position_advisory_daily,
                    position_advisory_daily_actions,
                    consensus_drift,
                    consensus_drift_summary,
                    owner_review,
                    paper_portfolio,
                    paper_portfolio_state,
                    advisory_outcome,
                    owner_attribution,
                    owner_attribution_summary,
                    shadow_aging,
                    shadow_aging_summary,
                    weekly_advisory_review,
                    weekly_owner_summary,
                    replay_inventory,
                    replay_inventory_coverage,
                    historical_replay,
                    replay_action_summary,
                    backfilled_outcome,
                    variant_performance,
                    historical_paper_sim,
                    simulated_performance,
                    replay_performance_review,
                    replay_calibration,
                    replay_forward_bridge,
                    replay_forward_focus,
                    outcome_dashboard,
                    outcome_availability_matrix,
                    pending_reason_dashboard,
                    *outcome_loop_payloads,
                    *no_promotion_v3_payloads,
                ),
                "production_candidate_generated",
            )
        ),
        "automatic_candidate_promotion": (
            leaderboard.get("automatic_candidate_promotion") is True
            or _mapping(promotion).get("automatic_candidate_promotion") is True
            or _any_payload_flag_true(
                (
                    evidence_diagnosis,
                    gate_impact,
                    gate_policy,
                    candidate_recovery,
                    research_decision_update,
                    shortlist,
                    candidate_cluster,
                    shadow_shortlist,
                    position_advisory,
                    position_advisory_actions,
                    position_review,
                    position_review_decision,
                    shadow_monitor_run,
                    shadow_monitor_run_summary,
                    portfolio_snapshot,
                    position_advisory_daily,
                    position_advisory_daily_actions,
                    consensus_drift,
                    consensus_drift_summary,
                    owner_review,
                    paper_portfolio,
                    paper_portfolio_state,
                    advisory_outcome,
                    owner_attribution,
                    owner_attribution_summary,
                    shadow_aging,
                    shadow_aging_summary,
                    weekly_advisory_review,
                    weekly_owner_summary,
                    replay_inventory,
                    replay_inventory_coverage,
                    historical_replay,
                    replay_action_summary,
                    backfilled_outcome,
                    variant_performance,
                    historical_paper_sim,
                    simulated_performance,
                    replay_performance_review,
                    replay_calibration,
                    replay_forward_bridge,
                    replay_forward_focus,
                    outcome_dashboard,
                    outcome_availability_matrix,
                    pending_reason_dashboard,
                    *outcome_loop_payloads,
                    *no_promotion_v3_payloads,
                ),
                "automatic_candidate_promotion",
            )
        ),
        "shadow_enrollment_allowed": (
            leaderboard.get("shadow_enrollment_allowed") is True
            or _mapping(promotion).get("shadow_enrollment_allowed") is True
            or _any_payload_flag_true(
                (
                    evidence_diagnosis,
                    gate_impact,
                    gate_policy,
                    candidate_recovery,
                    research_decision_update,
                    shortlist,
                    candidate_cluster,
                    shadow_shortlist,
                    position_advisory,
                    position_advisory_actions,
                    position_review,
                    position_review_decision,
                    shadow_monitor_run,
                    shadow_monitor_run_summary,
                    portfolio_snapshot,
                    position_advisory_daily,
                    position_advisory_daily_actions,
                    consensus_drift,
                    consensus_drift_summary,
                    owner_review,
                    paper_portfolio,
                    paper_portfolio_state,
                    advisory_outcome,
                    owner_attribution,
                    owner_attribution_summary,
                    shadow_aging,
                    shadow_aging_summary,
                    weekly_advisory_review,
                    weekly_owner_summary,
                    replay_inventory,
                    replay_inventory_coverage,
                    historical_replay,
                    replay_action_summary,
                    backfilled_outcome,
                    variant_performance,
                    historical_paper_sim,
                    simulated_performance,
                    replay_performance_review,
                    replay_calibration,
                    replay_forward_bridge,
                    replay_forward_focus,
                    outcome_dashboard,
                    outcome_availability_matrix,
                    pending_reason_dashboard,
                    *outcome_loop_payloads,
                    *no_promotion_v3_payloads,
                ),
                "shadow_enrollment_allowed",
            )
        ),
    }


def _etf_dynamic_v3_parameter_research_replay_only_summary(
    *,
    replay_inventory_path: Path | None,
    replay_inventory: Mapping[str, Any],
    replay_inventory_coverage: Mapping[str, Any],
    historical_replay_path: Path | None,
    historical_replay: Mapping[str, Any],
    replay_action_summary: Mapping[str, Any],
    backfilled_outcome_path: Path | None,
    backfilled_outcome: Mapping[str, Any],
    variant_performance: Mapping[str, Any],
    historical_paper_sim_path: Path | None,
    historical_paper_sim: Mapping[str, Any],
    simulated_performance: Mapping[str, Any],
    replay_performance_review_path: Path | None,
    replay_performance_review: Mapping[str, Any],
    replay_calibration: Mapping[str, Any],
    replay_recommendation: Mapping[str, Any],
    replay_forward_bridge_path: Path | None,
    replay_forward_bridge: Mapping[str, Any],
    replay_forward_focus: Mapping[str, Any],
    outcome_dashboard_path: Path | None,
    outcome_dashboard: Mapping[str, Any],
    outcome_availability_matrix: Mapping[str, Any],
    pending_reason_dashboard: Mapping[str, Any],
    outcome_update_review_path: Path | None,
    outcome_update_review: Mapping[str, Any],
    outcome_update_safety: Mapping[str, Any],
    outcome_update_impact: Mapping[str, Any],
    outcome_update_path: Path | None,
    outcome_update: Mapping[str, Any],
    outcome_status_delta: Mapping[str, Any],
    rolling_evidence_refresh_path: Path | None,
    rolling_evidence_refresh: Mapping[str, Any],
    rolling_evidence_delta: Mapping[str, Any],
    refreshed_artifacts: Mapping[str, Any],
    evidence_trend_path: Path | None,
    evidence_trend: Mapping[str, Any],
    confidence_trend_summary: Mapping[str, Any],
    forward_outcome_decision_path: Path | None,
    forward_outcome_decision: Mapping[str, Any],
    forward_go_no_go_matrix: Mapping[str, Any],
    forward_next_actions: Mapping[str, Any],
) -> dict[str, Any]:
    replay_payloads = (
        replay_inventory,
        replay_inventory_coverage,
        historical_replay,
        replay_action_summary,
        backfilled_outcome,
        variant_performance,
        historical_paper_sim,
        simulated_performance,
        replay_performance_review,
        replay_calibration,
        replay_forward_bridge,
        replay_forward_focus,
        outcome_dashboard,
        outcome_availability_matrix,
        pending_reason_dashboard,
        outcome_update_review,
        outcome_update_safety,
        outcome_update_impact,
        outcome_update,
        outcome_status_delta,
        rolling_evidence_refresh,
        rolling_evidence_delta,
        refreshed_artifacts,
        evidence_trend,
        confidence_trend_summary,
        forward_outcome_decision,
        forward_go_no_go_matrix,
        forward_next_actions,
    )
    status = _text(
        outcome_dashboard.get("status")
        or replay_performance_review.get("status")
        or backfilled_outcome.get("status")
        or historical_replay.get("status")
        or replay_inventory.get("status"),
        "UNKNOWN",
    )
    best_variant = _text(
        replay_performance_review.get("best_variant")
        or variant_performance.get("best_variant")
        or backfilled_outcome.get("best_variant"),
        "MISSING",
    )
    next_action = _text(
        replay_forward_bridge.get("next_action")
        or pending_reason_dashboard.get("next_action")
        or replay_performance_review.get("next_action")
        or replay_recommendation.get("type"),
        "MISSING",
    )
    forward_focus_items = _records(replay_forward_focus.get("focus_items"))
    top_pending = (_records(pending_reason_dashboard.get("top_pending_reasons")) or [{}])[0]
    outcome_delta_before = _mapping(outcome_status_delta.get("before"))
    outcome_delta_after = _mapping(outcome_status_delta.get("after"))
    rolling_delta_before = _mapping(rolling_evidence_delta.get("before"))
    rolling_delta_after = _mapping(rolling_evidence_delta.get("after"))
    forward_actions = _records(forward_next_actions.get("next_actions"))
    forward_next_due_scan_date = "MISSING"
    for action in forward_actions:
        if action.get("action") == "run_next_due_scan":
            forward_next_due_scan_date = _text(action.get("target_date"), "MISSING")
            break
    safety_status = _etf_dynamic_v3_parameter_research_safety_status(
        _etf_dynamic_v3_parameter_research_safe_placeholder(),
        *replay_payloads,
    )
    summary = _missing_etf_dynamic_v3_parameter_research_summary()
    summary.update(
        {
            "availability": "PARTIAL",
            "status": status,
            "summary_sentence": (
                "Dynamic Rescue Historical Replay Performance: "
                f"inventory={replay_inventory.get('status', 'MISSING')}; "
                f"replay={historical_replay.get('status', 'MISSING')}; "
                f"backfill={backfilled_outcome.get('status', 'MISSING')}; "
                f"paper_sim={historical_paper_sim.get('status', 'MISSING')}; "
                f"review={status}; "
                f"replay_forward_bridge={replay_forward_bridge.get('status', 'MISSING')}; "
                f"outcome_dashboard={outcome_dashboard.get('status', 'MISSING')}; "
                f"outcome_update={outcome_update.get('status', 'MISSING')}; "
                f"rolling_refresh={rolling_evidence_refresh.get('status', 'MISSING')}; "
                f"evidence_trend={confidence_trend_summary.get('trend_status', 'MISSING')}; "
                f"forward_decision="
                f"{forward_go_no_go_matrix.get('recommended_action', 'MISSING')}; "
                f"best_replay_variant={best_variant}; "
                f"next_action={next_action}; "
                "parameter_sweep_leaderboard=MISSING; manual-only replay evidence "
                "is available."
            ),
            "not_for_investment_decision": True,
            "data_quality": {
                "status": _text(
                    backfilled_outcome.get("data_quality_status"),
                    "MISSING",
                ),
            },
            "replay_inventory_id": _text(replay_inventory.get("inventory_id"), "MISSING"),
            "replay_inventory_status": _text(replay_inventory.get("status"), "MISSING"),
            "replay_inventory_total_events": replay_inventory.get("total_replay_events", 0),
            "replay_inventory_pit_safe_count": replay_inventory.get("pit_safe_count", 0),
            "replay_inventory_pit_warning_count": replay_inventory.get(
                "pit_warning_count",
                0,
            ),
            "replay_inventory_pit_unsafe_count": replay_inventory.get(
                "pit_unsafe_count",
                0,
            ),
            "replay_inventory_eligible_count": replay_inventory_coverage.get(
                "eligible_count",
                0,
            ),
            "historical_replay_id": _text(historical_replay.get("replay_id"), "MISSING"),
            "historical_replay_status": _text(historical_replay.get("status"), "MISSING"),
            "historical_replay_event_count": historical_replay.get("replay_event_count", 0),
            "historical_replay_skipped_count": historical_replay.get("skipped_count", 0),
            "historical_replay_generated_variants": ", ".join(
                _texts(historical_replay.get("generated_variants"))
            ),
            "historical_replay_broker_action_present": replay_action_summary.get(
                "broker_action_present",
                False,
            ),
            "backfilled_outcome_id": _text(backfilled_outcome.get("backfill_id"), "MISSING"),
            "backfilled_outcome_status": _text(backfilled_outcome.get("status"), "MISSING"),
            "backfilled_outcome_data_quality_status": _text(
                backfilled_outcome.get("data_quality_status"),
                "MISSING",
            ),
            "backfilled_outcome_available_count": backfilled_outcome.get(
                "available_count",
                0,
            ),
            "backfilled_outcome_pending_count": backfilled_outcome.get("pending_count", 0),
            "backfilled_outcome_insufficient_data_count": backfilled_outcome.get(
                "insufficient_data_count",
                0,
            ),
            "backfilled_outcome_best_variant": best_variant,
            "limited_adjustment_vs_no_trade_5d": variant_performance.get(
                "limited_adjustment_vs_no_trade_5d",
                0.0,
            ),
            "historical_paper_sim_id": _text(historical_paper_sim.get("sim_id"), "MISSING"),
            "historical_paper_sim_status": _text(
                historical_paper_sim.get("status"),
                "MISSING",
            ),
            "historical_paper_sim_variant": _text(
                simulated_performance.get("variant"),
                "MISSING",
            ),
            "historical_paper_sim_total_return": simulated_performance.get(
                "total_return",
                0.0,
            ),
            "historical_paper_sim_max_drawdown": simulated_performance.get(
                "max_drawdown",
                0.0,
            ),
            "historical_paper_sim_turnover": simulated_performance.get("turnover", 0.0),
            "historical_paper_sim_relative_to_no_trade": simulated_performance.get(
                "relative_to_no_trade",
                0.0,
            ),
            "replay_performance_review_id": _text(
                replay_performance_review.get("review_id"),
                "MISSING",
            ),
            "replay_performance_review_status": _text(
                replay_performance_review.get("status"),
                "MISSING",
            ),
            "replay_performance_best_variant": best_variant,
            "replay_performance_available_outcome_count": replay_performance_review.get(
                "available_outcome_count",
                0,
            ),
            "replay_performance_limited_adjustment_vs_no_trade": (
                replay_performance_review.get("limited_adjustment_vs_no_trade", 0.0)
            ),
            "replay_calibration_recommendation": _text(
                replay_recommendation.get("type"),
                "MISSING",
            ),
            "replay_forward_bridge_status": _text(
                replay_forward_bridge.get("status"),
                "MISSING",
            ),
            "replay_forward_focus": _text(
                (forward_focus_items or [{}])[0].get("item"),
                "MISSING",
            ),
            "replay_forward_next_action": _text(
                replay_forward_bridge.get("next_action"),
                "MISSING",
            ),
            "outcome_dashboard_id": _text(outcome_dashboard.get("dashboard_id"), "MISSING"),
            "outcome_dashboard_status": _text(outcome_dashboard.get("status"), "MISSING"),
            "outcome_dashboard_available_count": outcome_dashboard.get("available_count", 0),
            "outcome_dashboard_pending_count": outcome_dashboard.get("pending_count", 0),
            "outcome_dashboard_insufficient_count": outcome_dashboard.get(
                "insufficient_data_count",
                0,
            ),
            "outcome_dashboard_top_pending_reason": _text(
                top_pending.get("reason"),
                "MISSING",
            ),
            "outcome_dashboard_next_action": _text(
                pending_reason_dashboard.get("next_action"),
                "MISSING",
            ),
            "outcome_update_review_id": _text(
                outcome_update_review.get("update_review_id"),
                "MISSING",
            ),
            "outcome_update_review_status": _text(
                outcome_update_review.get("status"),
                "MISSING",
            ),
            "outcome_update_review_ready_count": outcome_update_review.get(
                "ready_to_update_count",
                0,
            ),
            "outcome_update_review_blocked_count": outcome_update_review.get(
                "blocked_count",
                0,
            ),
            "outcome_update_review_future_data_used": outcome_update_review.get(
                "future_data_used_in_decision",
                False,
            ),
            "outcome_update_review_expected_available_delta": outcome_update_impact.get(
                "expected_forward_available_delta",
                0,
            ),
            "outcome_update_id": _text(outcome_update.get("outcome_update_id"), "MISSING"),
            "outcome_update_status": _text(outcome_update.get("status"), "MISSING"),
            "outcome_update_updated_count": outcome_update.get("updated_count", 0),
            "outcome_update_skipped_count": outcome_update.get("skipped_count", 0),
            "outcome_update_forward_available_before": outcome_delta_before.get(
                "forward_available",
                0,
            ),
            "outcome_update_forward_available_after": outcome_delta_after.get(
                "forward_available",
                0,
            ),
            "outcome_update_forward_pending_before": outcome_delta_before.get(
                "forward_pending",
                0,
            ),
            "outcome_update_forward_pending_after": outcome_delta_after.get(
                "forward_pending",
                0,
            ),
            "rolling_evidence_refresh_id": _text(
                rolling_evidence_refresh.get("refresh_id"),
                "MISSING",
            ),
            "rolling_evidence_refresh_status": _text(
                rolling_evidence_refresh.get("status"),
                "MISSING",
            ),
            "rolling_evidence_refresh_material_change": rolling_evidence_refresh.get(
                "material_change",
                False,
            ),
            "rolling_limited_vs_notrade_count_before": rolling_delta_before.get(
                "limited_vs_notrade_available_count",
                0,
            ),
            "rolling_limited_vs_notrade_count_after": rolling_delta_after.get(
                "limited_vs_notrade_available_count",
                0,
            ),
            "rolling_consensus_risk_before": _text(
                rolling_delta_before.get("consensus_target_risk"),
                "MISSING",
            ),
            "rolling_consensus_risk_after": _text(
                rolling_delta_after.get("consensus_target_risk"),
                "MISSING",
            ),
            "rolling_weekly_advisory_review_id": _text(
                refreshed_artifacts.get("weekly_advisory_review_id"),
                "MISSING",
            ),
            "evidence_trend_id": _text(evidence_trend.get("trend_id"), "MISSING"),
            "evidence_trend_status": _text(
                confidence_trend_summary.get("trend_status") or evidence_trend.get("trend_status"),
                "MISSING",
            ),
            "evidence_trend_confidence_change": _text(
                confidence_trend_summary.get("confidence_change"),
                "MISSING",
            ),
            "evidence_trend_next_action": _text(
                confidence_trend_summary.get("next_action"),
                "MISSING",
            ),
            "forward_outcome_decision_id": _text(
                forward_outcome_decision.get("decision_id"),
                "MISSING",
            ),
            "forward_outcome_decision_action": _text(
                forward_go_no_go_matrix.get("recommended_action")
                or forward_outcome_decision.get("recommended_action"),
                "MISSING",
            ),
            "forward_rule_calibration_readiness": _text(
                forward_go_no_go_matrix.get("rule_calibration_readiness")
                or forward_outcome_decision.get("rule_calibration_readiness"),
                "MISSING",
            ),
            "forward_next_due_scan_date": forward_next_due_scan_date,
            "replay_calibration_priority": _text(
                replay_recommendation.get("priority"),
                "MISSING",
            ),
            "replay_calibration_requires_owner_approval": (
                replay_recommendation.get("requires_owner_approval")
                if "requires_owner_approval" in replay_recommendation
                else True
            ),
            "replay_next_action": next_action,
            "replay_inventory": (
                "" if replay_inventory_path is None else str(replay_inventory_path)
            ),
            "historical_replay": (
                "" if historical_replay_path is None else str(historical_replay_path)
            ),
            "backfilled_outcome": (
                "" if backfilled_outcome_path is None else str(backfilled_outcome_path)
            ),
            "historical_paper_sim": (
                "" if historical_paper_sim_path is None else str(historical_paper_sim_path)
            ),
            "replay_performance_review": (
                ""
                if replay_performance_review_path is None
                else str(replay_performance_review_path)
            ),
            "replay_forward_bridge": (
                "" if replay_forward_bridge_path is None else str(replay_forward_bridge_path)
            ),
            "outcome_dashboard": (
                "" if outcome_dashboard_path is None else str(outcome_dashboard_path)
            ),
            "outcome_update_review": (
                "" if outcome_update_review_path is None else str(outcome_update_review_path)
            ),
            "outcome_update": "" if outcome_update_path is None else str(outcome_update_path),
            "rolling_evidence_refresh": (
                "" if rolling_evidence_refresh_path is None else str(rolling_evidence_refresh_path)
            ),
            "evidence_trend": "" if evidence_trend_path is None else str(evidence_trend_path),
            "forward_outcome_decision": (
                "" if forward_outcome_decision_path is None else str(forward_outcome_decision_path)
            ),
            "safety_status": safety_status,
            "production_candidate_generated": _any_payload_flag_true(
                replay_payloads,
                "production_candidate_generated",
            ),
            "automatic_candidate_promotion": _any_payload_flag_true(
                replay_payloads,
                "automatic_candidate_promotion",
            ),
            "shadow_enrollment_allowed": _any_payload_flag_true(
                replay_payloads,
                "shadow_enrollment_allowed",
            ),
            "limitation": (
                "Dynamic rescue parameter sweep artifact is missing; historical replay "
                "evidence is reported as a partial, manual-review-only input."
            ),
        }
    )
    return summary


def _etf_dynamic_v3_parameter_research_safe_placeholder() -> dict[str, Any]:
    safety = {
        "observe_only": True,
        "candidate_only": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "production_state_mutated": False,
        "baseline_config_mutated": False,
        "official_target_weights_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "shadow_enrollment_allowed": False,
        "automatic_enrollment_allowed": False,
        "owner_approval_executed": False,
        "production_candidate_generated": False,
    }
    return {
        "safety": safety,
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "shadow_enrollment_allowed": False,
    }


def _missing_etf_dynamic_v3_parameter_research_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": ("Dynamic Rescue Parameter Sweep: no latest sweep leaderboard found."),
        "evaluator_mode": "MISSING",
        "evaluator_version": "MISSING",
        "metrics_source": "MISSING",
        "not_for_investment_decision": False,
        "data_quality": {},
        "candidate_count": 0,
        "top_candidate": "MISSING",
        "top_gate": "MISSING",
        "top_score": "MISSING",
        "common_reject_reasons": "MISSING",
        "recommended_next_actions": "MISSING",
        "promotion_status": "MISSING",
        "backtest_window_status": "MISSING",
        "weight_path_status": "MISSING",
        "candidate_attribution_status": "MISSING",
        "data_provenance_status": "MISSING",
        "download_manifest_status": "MISSING",
        "promotion_blocking_flags": "MISSING",
        "shadow_monitor_status": "MISSING",
        "shadow_observe_only_count": 0,
        "shadow_promotion_ready_count": 0,
        "shadow_live_drift_review_required_count": 0,
        "candidate_evidence_status": "MISSING",
        "candidate_evidence_usable_count": 0,
        "candidate_evidence_complete_count": 0,
        "candidate_evidence_top_blockers": "MISSING",
        "observe_pool_status": "MISSING",
        "observe_candidate_count": 0,
        "observe_pool_manual_review_required_count": 0,
        "shadow_registry_sync_status": "MISSING",
        "overnight_readiness": "MISSING",
        "overnight_blocking_reasons": "none",
        "research_decision_recommendation": "MISSING",
        "research_decision_priority": "MISSING",
        "research_decision_next_task": "MISSING",
        "evidence_diagnosis_status": "MISSING",
        "evidence_diagnosis_usable_candidates": 0,
        "evidence_diagnosis_hard_blocked_candidates": 0,
        "evidence_diagnosis_soft_blocked_candidates": 0,
        "gate_impact_status": "MISSING",
        "gate_impact_best_scenario": "MISSING",
        "gate_impact_best_observe_candidates": 0,
        "gate_policy_status": "MISSING",
        "gate_policy_version": "MISSING",
        "gate_policy_observe_only_candidates": 0,
        "gate_policy_manual_review_required_candidates": 0,
        "candidate_recovery_status": "MISSING",
        "recovered_candidate_count": 0,
        "candidate_recovery_manual_review_required_count": 0,
        "research_decision_update_status": "MISSING",
        "research_decision_update_go_no_go": "MISSING",
        "research_decision_update_recommended_action": "MISSING",
        "research_decision_update_required_owner_approval": True,
        "research_decision_update_usable_candidates_before": 0,
        "research_decision_update_usable_candidates_after": 0,
        "research_decision_update_warnings": "MISSING",
        "research_decision_update_next_task": "MISSING",
        "shortlist_status": "MISSING",
        "shortlist_count": 0,
        "shortlist_manual_review_required_count": 0,
        "candidate_cluster_status": "MISSING",
        "candidate_cluster_count": 0,
        "candidate_cluster_representative_count": 0,
        "candidate_cluster_weight_path_similarity_status": "MISSING",
        "shadow_shortlist_status": "MISSING",
        "shadow_shortlist_candidate_count": 0,
        "shadow_shortlist_monitoring_ready": False,
        "position_advisory_status": "MISSING",
        "position_advisory_consensus_status": "MISSING",
        "position_advisory_recommended_action": "MISSING",
        "position_advisory_owner_approval_required": True,
        "position_advisory_broker_action_allowed": False,
        "position_review_status": "MISSING",
        "shadow_observation_readiness": "MISSING",
        "position_advisory_readiness": "MISSING",
        "production_readiness": "MISSING",
        "position_review_recommended_next_action": "MISSING",
        "shadow_monitor_run_status": "MISSING",
        "shadow_monitor_run_active_count": 0,
        "shadow_monitor_run_recommendation": "MISSING",
        "shadow_monitor_run_broker_action_allowed": False,
        "portfolio_snapshot_status": "MISSING",
        "portfolio_snapshot_manual_review_required": True,
        "portfolio_snapshot_broker_imported": False,
        "position_advisory_daily_status": "MISSING",
        "position_advisory_daily_mode": "MISSING",
        "position_advisory_daily_consensus_status": "MISSING",
        "position_advisory_daily_recommended_action": "MISSING",
        "position_advisory_daily_broker_action_allowed": False,
        "consensus_drift_status": "MISSING",
        "consensus_drift_disagreement_status": "MISSING",
        "consensus_drift_advisory_implication": "MISSING",
        "owner_review_id": "MISSING",
        "owner_review_decision": "MISSING",
        "owner_review_broker_action_taken": False,
        "paper_portfolio_id": "MISSING",
        "paper_portfolio_status": "MISSING",
        "paper_portfolio_broker_action_taken": False,
        "advisory_outcome_id": "MISSING",
        "advisory_outcome_status": "MISSING",
        "advisory_outcome_data_quality_status": "MISSING",
        "owner_attribution_id": "MISSING",
        "owner_attribution_status": "MISSING",
        "owner_attribution_total_reviews": 0,
        "shadow_aging_id": "MISSING",
        "shadow_aging_status": "MISSING",
        "shadow_aging_eligible_for_review_count": 0,
        "shadow_aging_downgrade_recommended_count": 0,
        "weekly_advisory_review_id": "MISSING",
        "weekly_advisory_recommendation": "MISSING",
        "weekly_advisory_next_actions": "",
        "replay_inventory_id": "MISSING",
        "replay_inventory_status": "MISSING",
        "replay_inventory_total_events": 0,
        "replay_inventory_pit_safe_count": 0,
        "replay_inventory_pit_warning_count": 0,
        "replay_inventory_pit_unsafe_count": 0,
        "replay_inventory_eligible_count": 0,
        "historical_replay_id": "MISSING",
        "historical_replay_status": "MISSING",
        "historical_replay_event_count": 0,
        "historical_replay_skipped_count": 0,
        "historical_replay_generated_variants": "",
        "historical_replay_broker_action_present": False,
        "backfilled_outcome_id": "MISSING",
        "backfilled_outcome_status": "MISSING",
        "backfilled_outcome_data_quality_status": "MISSING",
        "backfilled_outcome_available_count": 0,
        "backfilled_outcome_pending_count": 0,
        "backfilled_outcome_insufficient_data_count": 0,
        "backfilled_outcome_best_variant": "MISSING",
        "limited_adjustment_vs_no_trade_5d": 0.0,
        "historical_paper_sim_id": "MISSING",
        "historical_paper_sim_status": "MISSING",
        "historical_paper_sim_variant": "MISSING",
        "historical_paper_sim_total_return": 0.0,
        "historical_paper_sim_max_drawdown": 0.0,
        "historical_paper_sim_turnover": 0.0,
        "historical_paper_sim_relative_to_no_trade": 0.0,
        "replay_performance_review_id": "MISSING",
        "replay_performance_review_status": "MISSING",
        "replay_performance_best_variant": "MISSING",
        "replay_performance_available_outcome_count": 0,
        "replay_performance_limited_adjustment_vs_no_trade": 0.0,
        "replay_calibration_recommendation": "MISSING",
        "replay_calibration_priority": "MISSING",
        "replay_calibration_requires_owner_approval": True,
        "replay_forward_bridge_status": "MISSING",
        "replay_forward_focus": "MISSING",
        "replay_forward_next_action": "MISSING",
        "outcome_dashboard_id": "MISSING",
        "outcome_dashboard_status": "MISSING",
        "outcome_dashboard_available_count": 0,
        "outcome_dashboard_pending_count": 0,
        "outcome_dashboard_insufficient_count": 0,
        "outcome_dashboard_top_pending_reason": "MISSING",
        "outcome_dashboard_next_action": "MISSING",
        "outcome_update_review_id": "MISSING",
        "outcome_update_review_status": "MISSING",
        "outcome_update_review_ready_count": 0,
        "outcome_update_review_blocked_count": 0,
        "outcome_update_review_future_data_used": False,
        "outcome_update_review_expected_available_delta": 0,
        "outcome_update_id": "MISSING",
        "outcome_update_status": "MISSING",
        "outcome_update_updated_count": 0,
        "outcome_update_skipped_count": 0,
        "outcome_update_forward_available_before": 0,
        "outcome_update_forward_available_after": 0,
        "outcome_update_forward_pending_before": 0,
        "outcome_update_forward_pending_after": 0,
        "rolling_evidence_refresh_id": "MISSING",
        "rolling_evidence_refresh_status": "MISSING",
        "rolling_evidence_refresh_material_change": False,
        "rolling_limited_vs_notrade_count_before": 0,
        "rolling_limited_vs_notrade_count_after": 0,
        "rolling_consensus_risk_before": "MISSING",
        "rolling_consensus_risk_after": "MISSING",
        "rolling_weekly_advisory_review_id": "MISSING",
        "evidence_trend_id": "MISSING",
        "evidence_trend_status": "MISSING",
        "evidence_trend_confidence_change": "MISSING",
        "evidence_trend_next_action": "MISSING",
        "forward_outcome_decision_id": "MISSING",
        "forward_outcome_decision_action": "MISSING",
        "forward_rule_calibration_readiness": "MISSING",
        "forward_next_due_scan_date": "MISSING",
        "filtered_candidate_evidence_id": "MISSING",
        "filtered_candidate_evidence_status": "MISSING",
        "filtered_candidate_primary_improvements": "MISSING",
        "filtered_candidate_primary_weaknesses": "MISSING",
        "median_regime_filter_spec_id": "MISSING",
        "median_regime_filter_contract_status": "MISSING",
        "median_regime_filter_complexity": "MISSING",
        "filtered_candidate_stress_id": "MISSING",
        "filtered_candidate_stress_status": "MISSING",
        "filtered_candidate_stress_improved_count": 0,
        "drawdown_mismatch_reduction_id": "MISSING",
        "drawdown_mismatch_reduction_status": "MISSING",
        "drawdown_mismatch_reduction_pct": 0.0,
        "flip_rotation_reduction_id": "MISSING",
        "flip_reduction_status": "MISSING",
        "rotation_reduction_status": "MISSING",
        "filtered_candidate_ab_review_id": "MISSING",
        "filtered_candidate_ab_status": "MISSING",
        "filtered_candidate_ab_next_action": "MISSING",
        "signal_gate_confirmation_id": "MISSING",
        "signal_gate_confirmation_target_count": 0,
        "signal_gate_confirmation_auto_apply": False,
        "filtered_formalization_readiness_id": "MISSING",
        "filtered_formalization_decision": "MISSING",
        "filtered_formalization_confidence": "MISSING",
        "filtered_formalization_can_write_official_target_weights": False,
        "owner_filtered_candidate_review_id": "MISSING",
        "owner_filtered_candidate_action": "MISSING",
        "owner_filtered_candidate_readiness_decision": "MISSING",
        "filtered_next_decision_id": "MISSING",
        "filtered_next_decision": "MISSING",
        "filtered_next_action": "MISSING",
        "formal_research_method_contract_id": "MISSING",
        "formal_research_method_status": "MISSING",
        "formal_research_method_promotion_state": "MISSING",
        "formal_research_method_paper_shadow_eligibility": "MISSING",
        "formal_research_method_safety_boundary_status": "MISSING",
        "formal_research_method_validation_status": "MISSING",
        "promotion_threshold_calibration_id": "MISSING",
        "promotion_threshold_policy_id": "MISSING",
        "promotion_threshold_policy_version": "MISSING",
        "promotion_threshold_status": "MISSING",
        "promotion_threshold_current_interpretation": "MISSING",
        "promotion_threshold_stress_required": "MISSING",
        "promotion_threshold_confirmation_minimum": "MISSING",
        "promotion_threshold_validation_status": "MISSING",
        "promotion_threshold_next_action": "MISSING",
        "paper_shadow_protocol_id": "MISSING",
        "paper_shadow_protocol_status": "MISSING",
        "paper_shadow_protocol_eligibility_status": "MISSING",
        "paper_shadow_protocol_min_observation_days": "MISSING",
        "paper_shadow_protocol_validation_status": "MISSING",
        "paper_shadow_protocol_next_action": "MISSING",
        "paper_shadow_daily_observation_id": "MISSING",
        "paper_shadow_daily_candidate": "MISSING",
        "paper_shadow_daily_date": "MISSING",
        "paper_shadow_daily_status": "MISSING",
        "paper_shadow_daily_signal_output": "MISSING",
        "paper_shadow_daily_risk_state": "MISSING",
        "paper_shadow_daily_next_action": "MISSING",
        "paper_shadow_daily_validation_status": "MISSING",
        "paper_shadow_drift_monitor_id": "MISSING",
        "paper_shadow_drift_candidate": "MISSING",
        "paper_shadow_drift_observation_id": "MISSING",
        "paper_shadow_drift_severity": "MISSING",
        "paper_shadow_drift_blocking_count": "MISSING",
        "paper_shadow_drift_warning_count": "MISSING",
        "paper_shadow_drift_next_action": "MISSING",
        "paper_shadow_drift_validation_status": "MISSING",
        "paper_shadow_weekly_review_id": "MISSING",
        "paper_shadow_weekly_candidate": "MISSING",
        "paper_shadow_weekly_window": "MISSING",
        "paper_shadow_weekly_decision": "MISSING",
        "paper_shadow_weekly_coverage_classification": "MISSING",
        "paper_shadow_weekly_coverage_status": "MISSING",
        "paper_shadow_weekly_coverage_safe_for_continuation": "MISSING",
        "paper_shadow_weekly_missing_inputs": "MISSING",
        "paper_shadow_weekly_drift_trend": "MISSING",
        "paper_shadow_weekly_validation_status": "MISSING",
        "candidate_decision_ledger_id": "MISSING",
        "candidate_decision_record_id": "MISSING",
        "candidate_decision_candidate": "MISSING",
        "candidate_decision_evidence_status": "MISSING",
        "candidate_decision_stress_result": "MISSING",
        "candidate_decision_mismatch_result": "MISSING",
        "candidate_decision_rotation_result": "MISSING",
        "candidate_decision_ab_result": "MISSING",
        "candidate_decision_confirmation_count": "MISSING",
        "candidate_decision_owner_action": "MISSING",
        "candidate_decision_final_decision": "MISSING",
        "candidate_decision_next_action": "MISSING",
        "candidate_decision_ledger_validation_status": "MISSING",
        "evidence_staleness_monitor_id": "MISSING",
        "evidence_freshness_status": "MISSING",
        "evidence_coverage_status": "MISSING",
        "evidence_weekly_review_coverage_classification": "MISSING",
        "evidence_weekly_review_coverage_safe_for_continuation": "MISSING",
        "evidence_requested_as_of": "MISSING",
        "evidence_freshness_reference_date": "MISSING",
        "evidence_latest_complete_market_date": "MISSING",
        "evidence_market_calendar_status": "MISSING",
        "evidence_stale_artifacts": "MISSING",
        "evidence_blocking_artifacts": "MISSING",
        "evidence_missing_artifacts": "MISSING",
        "evidence_next_refresh_action": "MISSING",
        "evidence_safe_to_continue_shadow": "MISSING",
        "evidence_safety_boundary_status": "MISSING",
        "evidence_staleness_validation_status": "MISSING",
        "shadow_continuation_readiness_id": "MISSING",
        "shadow_continuation_readiness": "MISSING",
        "shadow_continuation_safe_to_continue_shadow": "MISSING",
        "shadow_continuation_missing_artifacts": "MISSING",
        "shadow_continuation_blocking_artifacts": "MISSING",
        "shadow_continuation_stale_artifacts": "MISSING",
        "shadow_continuation_coverage_status": "MISSING",
        "shadow_continuation_manual_review_required": "MISSING",
        "shadow_continuation_next_required_action": "MISSING",
        "shadow_continuation_data_validation_status": "MISSING",
        "shadow_continuation_safety_boundary_status": "MISSING",
        "shadow_continuation_validation_status": "MISSING",
        "stress_scenario_library_run_id": "MISSING",
        "stress_scenario_library_id": "MISSING",
        "stress_scenario_count": "MISSING",
        "stress_scenario_required_present": "MISSING",
        "stress_scenario_candidate_validation_use": "MISSING",
        "stress_scenario_next_action": "MISSING",
        "stress_scenario_validation_status": "MISSING",
        "drawdown_casebook_run_id": "MISSING",
        "drawdown_casebook_id": "MISSING",
        "drawdown_casebook_event_count": "MISSING",
        "drawdown_casebook_worst_event": "MISSING",
        "drawdown_casebook_regime_coverage": "MISSING",
        "drawdown_casebook_next_action": "MISSING",
        "drawdown_casebook_validation_status": "MISSING",
        "flip_rotation_casebook_run_id": "MISSING",
        "flip_rotation_casebook_id": "MISSING",
        "flip_rotation_casebook_event_count": "MISSING",
        "flip_rotation_useful_count": "MISSING",
        "flip_rotation_false_positive_count": "MISSING",
        "flip_rotation_dominant_trigger": "MISSING",
        "flip_rotation_next_action": "MISSING",
        "flip_rotation_casebook_validation_status": "MISSING",
        "replay_next_action": "MISSING",
        "sweep_leaderboard": "",
        "promotion_manifest": "",
        "evidence_summary": "",
        "shadow_monitor_report": "",
        "candidate_evidence_summary": "",
        "observe_pool": "",
        "overnight_readiness_report": "",
        "research_decision": "",
        "evidence_diagnosis": "",
        "gate_impact": "",
        "gate_policy": "",
        "candidate_recovery": "",
        "research_decision_update": "",
        "shortlist": "",
        "candidate_cluster": "",
        "shadow_shortlist": "",
        "position_advisory": "",
        "position_review": "",
        "shadow_monitor_run": "",
        "portfolio_snapshot": "",
        "position_advisory_daily": "",
        "consensus_drift": "",
        "owner_review": "",
        "paper_portfolio": "",
        "advisory_outcome": "",
        "owner_attribution": "",
        "shadow_aging": "",
        "weekly_advisory_review": "",
        "replay_inventory": "",
        "historical_replay": "",
        "backfilled_outcome": "",
        "historical_paper_sim": "",
        "replay_performance_review": "",
        "replay_forward_bridge": "",
        "outcome_dashboard": "",
        "outcome_update_review": "",
        "outcome_update": "",
        "rolling_evidence_refresh": "",
        "evidence_trend": "",
        "forward_outcome_decision": "",
        "promotion_gate_threshold_calibration": "",
        "paper_shadow_protocol": "",
        "paper_shadow_daily": "",
        "paper_shadow_drift_monitor": "",
        "paper_shadow_weekly_review": "",
        "candidate_decision_ledger": "",
        "evidence_staleness_monitor": "",
        "shadow_continuation_readiness_report": "",
        "stress_scenario_library": "",
        "drawdown_event_casebook": "",
        "flip_rotation_event_casebook": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "shadow_enrollment_allowed": False,
        "limitation": (
            "Dynamic rescue parameter sweep artifact is missing; Reader Brief does not "
            "run etf dynamic-v3-rescue sweep or promotion commands."
        ),
    }


def _promotion_pack_manifest_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.name == "promotion_manifest.json":
        return path
    sibling = path.parent / "promotion_manifest.json"
    if sibling.exists():
        return sibling
    return path


def _dynamic_v3_sibling_artifact_path(path: Path | None, artifact_name: str) -> Path | None:
    if path is None:
        return None
    if path.name == artifact_name:
        return path
    sibling = path.parent / artifact_name
    if sibling.exists():
        return sibling
    return path


def _dynamic_v3_confirmation_registry_manifest_path(path: Path | None) -> Path | None:
    sibling = _dynamic_v3_sibling_artifact_path(path, "confirmation_registry_manifest.json")
    if sibling is None:
        return None
    if sibling.name == "confirmation_registry_manifest.json":
        return sibling
    if path is None or path.suffix.lower() not in {".yaml", ".yml"}:
        return None
    registry_id = _text(_read_optional_yaml_mapping(path).get("registry_id"))
    if not registry_id:
        return None
    for root in _dynamic_v3_confirmation_registry_search_roots(path):
        for relative in (
            Path("reports")
            / "etf_portfolio"
            / "dynamic_v3_rescue"
            / "forward_confirmation_registry",
            Path("forward_confirmation_registry"),
        ):
            candidate = root / relative / registry_id / "confirmation_registry_manifest.json"
            if candidate.exists():
                return candidate
    return None


def _dynamic_v3_confirmation_registry_search_roots(path: Path) -> tuple[Path, ...]:
    roots = [PROJECT_ROOT]
    if path.parent.name == "registry":
        roots.append(path.parent.parent)
    if path.parent.name == "etf_portfolio" and path.parent.parent.name == "registry":
        roots.append(path.parent.parent.parent)
    return tuple(dict.fromkeys(roots))


def _dynamic_v3_rule_owner_decision_journal_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.name == "rule_owner_decision_journal.jsonl":
        return path
    sibling = path.parent / "rule_owner_decision_journal.jsonl"
    if sibling.exists():
        return sibling
    return path if path.suffix.lower() == ".jsonl" else None


def _research_decision_recommendation_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.name == "next_action_recommendations.json":
        return path
    sibling = path.parent / "next_action_recommendations.json"
    return sibling if sibling.exists() else None


def _research_decision_update_go_no_go_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    if path.name == "go_no_go_matrix.json":
        return path
    sibling = path.parent / "go_no_go_matrix.json"
    return sibling if sibling.exists() else None


def _etf_dynamic_v3_parameter_research_safety_status(
    payload: Mapping[str, Any],
    *extra_payloads: Mapping[str, Any],
) -> str:
    safety = _mapping(payload.get("safety"))
    primary_safe = (
        safety.get("observe_only") is True
        and safety.get("candidate_only") is True
        and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and safety.get("broker_action") == "none"
        and safety.get("manual_review_required") is True
        and safety.get("production_state_mutated") is False
        and safety.get("baseline_config_mutated") is False
        and safety.get("official_target_weights_mutated") is False
        and safety.get("automatic_candidate_promotion") is False
        and safety.get("auto_enrollment_without_owner_approval") is False
        and safety.get("shadow_enrollment_allowed") is False
        and safety.get("automatic_enrollment_allowed") is False
        and safety.get("owner_approval_executed") is False
        and safety.get("production_candidate_generated") is False
        and payload.get("production_candidate_generated") is False
    )
    safe = primary_safe and all(
        _etf_dynamic_v3_extra_payload_safe(extra_payload) for extra_payload in extra_payloads
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "production_candidate_generated=false; automatic_candidate_promotion=false; "
        "shadow_enrollment_allowed=false; owner_approval_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_dynamic_v3_extra_payload_safe(payload: Mapping[str, Any]) -> bool:
    if not payload:
        return True
    safety = _mapping(payload.get("safety"))
    source = safety if safety else payload
    return (
        _text(source.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and _text(source.get("broker_action"), "none") == "none"
        and source.get("production_candidate_generated") is not True
        and payload.get("production_candidate_generated") is not True
        and source.get("automatic_candidate_promotion") is not True
        and payload.get("automatic_candidate_promotion") is not True
        and source.get("shadow_enrollment_allowed") is not True
        and payload.get("shadow_enrollment_allowed") is not True
        and source.get("owner_approval_executed") is not True
        and payload.get("owner_approval_executed") is not True
        and source.get("production_state_mutated") is not True
        and payload.get("production_state_mutated") is not True
        and source.get("baseline_config_mutated") is not True
        and payload.get("baseline_config_mutated") is not True
        and source.get("official_target_weights_mutated") is not True
        and payload.get("official_target_weights_mutated") is not True
    )


def _any_payload_flag_true(payloads: tuple[Mapping[str, Any], ...], flag: str) -> bool:
    return any(_mapping(payload).get(flag) is True for payload in payloads)


def _etf_dynamic_shadow_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_dynamic_shadow_summary()
    package_path = _report_index_artifact_path(report_index, "etf_dynamic_shadow_review_package")
    approval_path = _report_index_artifact_path(report_index, "etf_dynamic_shadow_owner_approval")
    enrollment_path = _report_index_artifact_path(report_index, "etf_dynamic_shadow_enrollment")
    update_path = _report_index_artifact_path(report_index, "etf_dynamic_shadow_forward_update")
    weekly_path = _report_index_artifact_path(report_index, "etf_dynamic_shadow_weekly_review")
    package = _read_optional_json(package_path)
    approval = _read_optional_json(approval_path)
    enrollment = _read_optional_json(enrollment_path)
    update = _read_optional_json(update_path)
    weekly = _read_optional_json(weekly_path)
    payloads = [payload for payload in (package, approval, enrollment, update, weekly) if payload]
    if not payloads:
        return _missing_etf_dynamic_shadow_summary()
    package_summary = _mapping(package.get("review_summary"))
    update_summary = _mapping(update.get("summary"))
    weekly_summary = _mapping(weekly.get("summary"))
    candidate = _text(
        package_summary.get("top_candidate") or enrollment.get("candidate_id"),
        "MISSING",
    )
    status = _text(
        weekly.get("status") or update.get("status") or package_summary.get("status"),
        "MISSING",
    )
    safety_status = _etf_dynamic_shadow_safety_status(payloads)
    return {
        "availability": "AVAILABLE",
        "status": status,
        "summary_sentence": (
            f"Dynamic Shadow Review: candidate={candidate}; status={status}; "
            f"owner_decision={approval.get('owner_decision', 'MISSING')}; "
            f"active={update.get('active_candidate_count', 'MISSING')}; "
            f"watch={weekly_summary.get('watch_count', 'MISSING')}; "
            f"safety={safety_status}."
        ),
        "top_candidate": candidate,
        "ready_after_owner_approval_count": package_summary.get(
            "ready_after_owner_approval_count",
            "MISSING",
        ),
        "blocked_count": package_summary.get("blocked_count", "MISSING"),
        "latest_owner_decision": approval.get("owner_decision", "MISSING"),
        "latest_enrollment": enrollment.get("enrollment_id", "MISSING"),
        "active_candidate_count": update.get(
            "active_candidate_count",
            update_summary.get("active_candidate_count", "MISSING"),
        ),
        "watch_count": weekly_summary.get("watch_count", "MISSING"),
        "reject_pending_review_count": weekly_summary.get(
            "reject_pending_review_count",
            "MISSING",
        ),
        "tracking_status": enrollment.get("tracking_status", "MISSING"),
        "package_report": "" if package_path is None else str(package_path),
        "weekly_review": "" if weekly_path is None else str(weekly_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "official_target_weights_mutated": any(
            payload.get("official_target_weights_mutated") is True for payload in payloads
        ),
        "baseline_config_mutated": any(
            payload.get("baseline_config_mutated") is True for payload in payloads
        ),
        "production_state_mutated": any(
            payload.get("production_state_mutated") is True for payload in payloads
        ),
        "automatic_candidate_promotion": any(
            payload.get("automatic_candidate_promotion") is True for payload in payloads
        ),
        "auto_enrollment_without_owner_approval": any(
            payload.get("auto_enrollment_without_owner_approval") is True for payload in payloads
        ),
    }


def _missing_etf_dynamic_shadow_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": "Dynamic Shadow Review: no latest dynamic shadow artifacts found.",
        "top_candidate": "MISSING",
        "ready_after_owner_approval_count": "MISSING",
        "blocked_count": "MISSING",
        "latest_owner_decision": "MISSING",
        "latest_enrollment": "MISSING",
        "active_candidate_count": "MISSING",
        "watch_count": "MISSING",
        "reject_pending_review_count": "MISSING",
        "tracking_status": "MISSING",
        "package_report": "",
        "weekly_review": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "production_state_mutated": False,
        "automatic_candidate_promotion": False,
        "auto_enrollment_without_owner_approval": False,
        "limitation": (
            "Dynamic shadow artifacts are missing; Reader Brief does not run "
            "etf dynamic-shadow package/update/weekly-review CLI."
        ),
    }


def _etf_dynamic_shadow_safety_status(payloads: list[Mapping[str, Any]]) -> str:
    safe = True
    for payload in payloads:
        safety = _mapping(payload.get("safety"))
        safe = safe and (
            safety.get("observe_only") is True
            and safety.get("candidate_only") is True
            and _text(safety.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
            and safety.get("broker_action") == "none"
            and safety.get("manual_review_required") is True
            and safety.get("production_state_mutated") is False
            and safety.get("baseline_config_mutated") is False
            and safety.get("official_target_weights_mutated") is False
            and safety.get("automatic_candidate_promotion") is False
            and safety.get("auto_enrollment_without_owner_approval") is False
            and payload.get("commands_executed") is False
            and payload.get("production_state_mutated") is False
            and payload.get("baseline_config_mutated") is False
            and payload.get("official_target_weights_mutated") is False
            and payload.get("automatic_candidate_promotion") is False
            and payload.get("auto_enrollment_without_owner_approval") is False
        )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true; "
        "official_target_weights_mutated=false; baseline_config_mutated=false; "
        "production_state_mutated=false; automatic_candidate_promotion=false; "
        "auto_enrollment_without_owner_approval=false; commands_executed=false"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_calibration_experiment_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_calibration_summary()
    selection_path = _report_index_artifact_path(
        report_index,
        "etf_experiment_candidate_selection",
    )
    comparison_path = _report_index_artifact_path(
        report_index,
        "etf_experiment_comparison",
    )
    shadow_path = _report_index_artifact_path(
        report_index,
        "etf_shadow_candidates",
    )
    weekly_path = _report_index_artifact_path(
        report_index,
        "etf_experiment_weekly_review",
    )
    selection = _read_optional_json(selection_path)
    comparison = _read_optional_json(comparison_path)
    shadow = _read_optional_json(shadow_path)
    weekly = _read_optional_json(weekly_path)
    if not any((selection, comparison, shadow, weekly)):
        return _missing_etf_calibration_summary()
    selection_summary = _mapping(selection.get("selection_summary"))
    weekly_summary = _mapping(weekly.get("summary"))
    top_candidate = _top_etf_experiment_candidate(selection)
    latest_pack = (
        _text(_mapping(selection.get("run_metadata")).get("pack_id"))
        or _text(_mapping(comparison.get("run_metadata")).get("pack_id"))
        or "MISSING"
    )
    safety_status = _etf_calibration_safety_status(selection, comparison, shadow, weekly)
    detail_report = _first_existing_path(weekly_path, selection_path, comparison_path)
    return {
        "availability": "AVAILABLE",
        "status": (
            _text(weekly_summary.get("status"))
            or _text(selection_summary.get("status"))
            or _text(comparison.get("ranking_policy_status"), "AVAILABLE")
        ),
        "latest_experiment_pack": latest_pack,
        "top_candidate": top_candidate,
        "rejected_count": int(selection_summary.get("rejected_count") or 0)
        + int(selection_summary.get("blocked_count") or 0),
        "active_shadow_candidates": int(shadow.get("candidate_count") or 0),
        "weekly_review_action": _weekly_review_action_summary(weekly),
        "safety_status": safety_status,
        "detail_report": "" if detail_report is None else str(detail_report),
        "candidate_selection_report": "" if selection_path is None else str(selection_path),
        "comparison_report": "" if comparison_path is None else str(comparison_path),
        "shadow_registry": "" if shadow_path is None else str(shadow_path),
        "weekly_review_report": "" if weekly_path is None else str(weekly_path),
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"ETF calibration pack={latest_pack}; top_candidate={top_candidate}; "
            f"active_shadow_candidates={int(shadow.get('candidate_count') or 0)}; "
            f"safety={safety_status}."
        ),
    }


def _top_etf_experiment_candidate(selection: Mapping[str, Any]) -> str:
    candidates = _records(selection.get("candidates"))
    if not candidates:
        return "MISSING"
    eligible = [
        candidate
        for candidate in candidates
        if _text(candidate.get("selection_status")) == "eligible_for_shadow"
    ]
    candidate = (eligible or candidates)[0]
    return _text(candidate.get("candidate_id")) or _text(candidate.get("experiment_id"), "MISSING")


def _missing_etf_calibration_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "latest_experiment_pack": "MISSING",
        "top_candidate": "MISSING",
        "rejected_count": 0,
        "active_shadow_candidates": 0,
        "weekly_review_action": "MISSING",
        "safety_status": "MISSING",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "limitation": (
            "ETF calibration experiment artifacts are missing; Reader Brief does not "
            "run experiments."
        ),
    }


def _weekly_review_action_summary(weekly: Mapping[str, Any]) -> str:
    reviews = _records(weekly.get("candidate_reviews"))
    if not reviews:
        return "MISSING"
    return ", ".join(sorted({_text(item.get("recommended_action"), "UNKNOWN") for item in reviews}))


def _etf_calibration_safety_status(*payloads: Mapping[str, Any]) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    safe = all(
        payload.get("observe_only") in (None, True)
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") in (None, "none")
        and payload.get("production_promotion_allowed") in (None, False)
        for payload in material
    )
    return (
        "observe_only=true; production_effect=none; broker_action=none"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_forward_simulation_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_forward_simulation_summary()
    dashboard_path = _report_index_artifact_path(report_index, "etf_forward_dashboard")
    watchlist_path = _report_index_artifact_path(report_index, "etf_forward_watchlist")
    dashboard = _read_optional_json(dashboard_path)
    watchlist = _read_optional_json(watchlist_path)
    if not dashboard:
        return _missing_etf_forward_simulation_summary()
    rows = _records(dashboard.get("candidate_summary_table"))
    status_summary = _mapping(dashboard.get("status_summary"))
    safety_status = _etf_forward_safety_status(dashboard, watchlist)
    detail_report = _first_existing_path(dashboard_path, watchlist_path)
    watchlist_summary = _mapping(watchlist.get("summary"))
    if not rows:
        return {
            "availability": "AVAILABLE",
            "status": _text(dashboard.get("status"), "NO_ACTIVE_SHADOW_CANDIDATES"),
            "active_shadow_candidates": 0,
            "best_candidate": "MISSING",
            "weakest_candidate": "MISSING",
            "needs_more_data_count": 0,
            "watch_count": 0,
            "reject_pending_review_count": 0,
            "watchlist_attention_count": int(watchlist_summary.get("item_count") or 0),
            "safety_status": safety_status,
            "detail_report": "" if detail_report is None else str(detail_report),
            "dashboard_report": "" if dashboard_path is None else str(dashboard_path),
            "watchlist_report": "" if watchlist_path is None else str(watchlist_path),
            "decision_input_usage": "none; forward metrics are evaluation-only",
            "production_effect": PRODUCTION_EFFECT,
            "summary_sentence": (
                "ETF Forward Simulation: no active shadow candidates. "
                "Run experiment enrollment first."
            ),
        }
    best = _best_forward_candidate(rows)
    weakest = _weakest_forward_candidate(rows)
    return {
        "availability": "AVAILABLE",
        "status": _text(dashboard.get("status"), "AVAILABLE"),
        "active_shadow_candidates": int(status_summary.get("active_candidate_count") or len(rows)),
        "best_candidate": best,
        "weakest_candidate": weakest,
        "needs_more_data_count": int(status_summary.get("needs_more_data_count") or 0),
        "watch_count": int(status_summary.get("watch_count") or 0),
        "reject_pending_review_count": int(status_summary.get("reject_pending_review_count") or 0),
        "watchlist_attention_count": int(watchlist_summary.get("item_count") or 0),
        "safety_status": safety_status,
        "detail_report": "" if detail_report is None else str(detail_report),
        "dashboard_report": "" if dashboard_path is None else str(dashboard_path),
        "watchlist_report": "" if watchlist_path is None else str(watchlist_path),
        "decision_input_usage": "none; forward metrics are evaluation-only",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            f"ETF Forward Simulation: active_shadow_candidates={len(rows)}; "
            f"best_candidate={best}; weakest_candidate={weakest}; safety={safety_status}."
        ),
    }


def _missing_etf_forward_simulation_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "active_shadow_candidates": 0,
        "best_candidate": "MISSING",
        "weakest_candidate": "MISSING",
        "needs_more_data_count": 0,
        "watch_count": 0,
        "reject_pending_review_count": 0,
        "watchlist_attention_count": 0,
        "safety_status": "MISSING",
        "detail_report": "",
        "decision_input_usage": "none; Reader Brief does not run forward update",
        "production_effect": PRODUCTION_EFFECT,
        "summary_sentence": (
            "ETF Forward Simulation: no active shadow candidates. Run experiment enrollment first."
        ),
        "limitation": (
            "ETF forward dashboard artifact is missing; Reader Brief does not run "
            "forward simulation."
        ),
    }


def _etf_forward_safety_status(*payloads: Mapping[str, Any]) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    safe = all(
        payload.get("observe_only") in (None, True)
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") in (None, "none")
        and payload.get("manual_review_required") in (None, True)
        and payload.get("production_promotion_allowed") in (None, False)
        for payload in material
    )
    return (
        "observe_only=true; production_effect=none; broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_ai_confirmation_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_ai_confirmation_summary()
    report_path = _report_index_artifact_path(report_index, "etf_ai_confirmation_report")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_ai_confirmation_summary()
    score = _mapping(report.get("AIConfirmationScore"))
    components = _mapping(report.get("component_scores"))
    event_risk = _mapping(report.get("event_risk_overlay"))
    action_hint = _text(score.get("action_hint"), "insufficient_data")
    coverage = _float_or_none(score.get("data_coverage_ratio"))
    safety_status = _etf_ai_confirmation_safety_status(report, score)
    insufficient = action_hint == "insufficient_data"
    interpretation = (
        "AI Confirmation: insufficient data coverage. No overlay recommendation."
        if insufficient
        else (
            "AI confirmation supports current QQQ / SMH candidate exposure, "
            "but no production weights are changed."
        )
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), _text(score.get("score_band"), "AVAILABLE")),
        "AIConfirmationScore": _format_number(score.get("score_value"), digits=2),
        "score_band": _text(score.get("score_band"), "MISSING"),
        "action_hint": action_hint,
        "semiconductor_breadth": _format_number(
            components.get("semiconductor_breadth"),
            digits=2,
        ),
        "mega_cap_ai_score": _format_number(components.get("mega_cap_ai"), digits=2),
        "ai_relative_strength": _format_number(
            components.get("ai_relative_strength"),
            digits=2,
        ),
        "event_risk": _text(event_risk.get("risk_band"), "MISSING"),
        "event_risk_score": _format_number(event_risk.get("event_risk_score"), digits=2),
        "data_coverage_ratio": _format_number(coverage, digits=2),
        "interpretation": interpretation,
        "safety_status": safety_status,
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "summary_sentence": (
            "AI Confirmation: insufficient data coverage. No overlay recommendation."
            if insufficient
            else (
                f"AIConfirmationScore={_format_number(score.get('score_value'), digits=2)} "
                f"/ {_text(score.get('score_band'), 'MISSING')}; event_risk="
                f"{_text(event_risk.get('risk_band'), 'MISSING')}; "
                "production_effect=none."
            )
        ),
    }


def _missing_etf_ai_confirmation_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "AIConfirmationScore": "MISSING",
        "score_band": "MISSING",
        "action_hint": "insufficient_data",
        "semiconductor_breadth": "MISSING",
        "mega_cap_ai_score": "MISSING",
        "ai_relative_strength": "MISSING",
        "event_risk": "MISSING",
        "event_risk_score": "MISSING",
        "data_coverage_ratio": "MISSING",
        "interpretation": (
            "AI Confirmation: insufficient data coverage. No overlay recommendation."
        ),
        "safety_status": "MISSING",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "summary_sentence": (
            "AI Confirmation: insufficient data coverage. No overlay recommendation."
        ),
        "limitation": (
            "AI confirmation report artifact is missing; Reader Brief does not run "
            "AI confirmation scoring."
        ),
    }


def _etf_ai_confirmation_safety_status(*payloads: Mapping[str, Any]) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    safe = all(
        payload.get("observe_only") in (None, True)
        and payload.get("candidate_only") in (None, True)
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") in (None, "none")
        and payload.get("manual_review_required") in (None, True)
        for payload in material
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _etf_ai_attribution_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_ai_attribution_summary()
    report_path = _report_index_artifact_path(report_index, "etf_ai_attribution_report")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_ai_attribution_summary()
    scorecard = _mapping(report.get("evidence_scorecard"))
    dimensions = _mapping(scorecard.get("dimension_scores"))
    redundancy = _mapping(report.get("redundancy_diagnostics"))
    safety_status = _etf_ai_attribution_safety_status(report, scorecard)
    best_evidence = _ai_attribution_best_evidence(dimensions)
    weak_evidence = _ai_attribution_weak_evidence(dimensions)
    overall_status = _text(scorecard.get("overall_status"), _text(report.get("status"), "UNKNOWN"))
    redundancy_status = _text(redundancy.get("redundancy_band"), "unknown")
    manual_review = _text(
        scorecard.get("manual_review_recommendation"),
        "继续 observe-only；不得自动提高 AI overlay 权重。",
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), overall_status),
        "overall_status": overall_status,
        "best_evidence": best_evidence,
        "weak_evidence": weak_evidence,
        "redundancy_status": redundancy_status,
        "manual_review": manual_review,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": (
            f"AI Attribution Review: status={overall_status}; best={best_evidence}; "
            f"weak={weak_evidence}; redundancy={redundancy_status}; safety={safety_status}."
        ),
    }


def _missing_etf_ai_attribution_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "overall_status": "MISSING",
        "best_evidence": "MISSING",
        "weak_evidence": "AI attribution report artifact is missing",
        "redundancy_status": "MISSING",
        "manual_review": "继续 observe-only；Reader Brief 不运行 AI attribution CLI。",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": "AI Attribution Review: no latest attribution report found.",
        "limitation": (
            "AI attribution report artifact is missing; Reader Brief does not run AI attribution."
        ),
    }


def _etf_ai_attribution_safety_status(*payloads: Mapping[str, Any]) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    safe = all(
        payload.get("observe_only") in (None, True)
        and payload.get("candidate_only") in (None, True)
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") in (None, "none")
        and payload.get("manual_review_required") in (None, True)
        for payload in material
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _ai_attribution_best_evidence(dimensions: Mapping[str, Any]) -> str:
    if not dimensions:
        return "MISSING"
    candidates = [
        ("forward_return_evidence", dimensions.get("forward_return_evidence")),
        ("semiconductor_relative_evidence", dimensions.get("semiconductor_relative_evidence")),
        ("mega_cap_growth_evidence", dimensions.get("mega_cap_growth_evidence")),
        ("event_risk_evidence", dimensions.get("event_risk_evidence")),
        ("regime_stability_evidence", dimensions.get("regime_stability_evidence")),
    ]
    scored = [(name, _float_or_none(value)) for name, value in candidates]
    scored = [(name, value) for name, value in scored if value is not None]
    if not scored:
        return "MISSING"
    name, value = max(scored, key=lambda item: item[1])
    return f"{name}={_format_number(value, digits=2)}"


def _ai_attribution_weak_evidence(dimensions: Mapping[str, Any]) -> str:
    if not dimensions:
        return "MISSING"
    candidates = [
        ("forward_return_evidence", dimensions.get("forward_return_evidence")),
        ("semiconductor_relative_evidence", dimensions.get("semiconductor_relative_evidence")),
        ("mega_cap_growth_evidence", dimensions.get("mega_cap_growth_evidence")),
        ("event_risk_evidence", dimensions.get("event_risk_evidence")),
        ("regime_stability_evidence", dimensions.get("regime_stability_evidence")),
        ("sample_quality", dimensions.get("sample_quality")),
        ("data_coverage", dimensions.get("data_coverage")),
    ]
    scored = [(name, _float_or_none(value)) for name, value in candidates]
    scored = [(name, value) for name, value in scored if value is not None]
    if not scored:
        return "MISSING"
    name, value = min(scored, key=lambda item: item[1])
    return f"{name}={_format_number(value, digits=2)}"


def _etf_satellite_attribution_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_satellite_attribution_summary()
    report_path = _report_index_artifact_path(
        report_index,
        "etf_satellite_attribution_report",
    )
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_satellite_attribution_summary()
    scorecard = _mapping(report.get("evidence_scorecard"))
    dimensions = _mapping(scorecard.get("dimension_scores"))
    fallback = _mapping(report.get("fallback_attribution"))
    risk = _mapping(report.get("risk_attribution"))
    role = _mapping(report.get("role_group_attribution"))
    safety_status = _etf_satellite_safety_status(report, scorecard)
    overall_status = _text(scorecard.get("overall_status"), _text(report.get("status"), "UNKNOWN"))
    eligible_evidence = _text(
        dimensions.get("eligible_outperformance_evidence"),
        "unknown",
    )
    fallback_evidence = (
        f"{_text(dimensions.get('fallback_protection_evidence'), 'unknown')}; "
        f"saved_loss_rate={_format_number(fallback.get('fallback_saved_loss_rate'), digits=2)}; "
        f"missed_gain_rate={_format_number(fallback.get('fallback_missed_gain_rate'), digits=2)}"
    )
    role_evidence = (
        f"best_role={_text(role.get('best_role'), 'unknown')}; "
        f"worst_role={_text(role.get('worst_role'), 'unknown')}; "
        f"status={_text(dimensions.get('role_group_evidence'), 'unknown')}"
    )
    risk_note = (
        f"risk_adjusted_alpha={_format_number(risk.get('risk_adjusted_alpha'), digits=4)}; "
        "eligible_drawdown_added="
        f"{_format_number(risk.get('drawdown_added_by_eligible_replacement'), digits=4)}"
    )
    weak_evidence = _satellite_attribution_weak_evidence(dimensions)
    manual_review = _text(
        scorecard.get("manual_review_recommendation"),
        "继续 observe-only；不得自动提高 satellite replacement 权重。",
    )
    return {
        "availability": "AVAILABLE",
        "status": _text(report.get("status"), overall_status),
        "overall_status": overall_status,
        "eligible_evidence": eligible_evidence,
        "fallback_evidence": fallback_evidence,
        "role_evidence": role_evidence,
        "risk_note": risk_note,
        "weak_evidence": weak_evidence,
        "manual_review": manual_review,
        "detail_report": "" if report_path is None else str(report_path),
        "safety_status": safety_status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": (
            f"Satellite Attribution Review: status={overall_status}; "
            f"eligible={eligible_evidence}; fallback={fallback_evidence}; "
            f"risk={risk_note}; safety={safety_status}."
        ),
    }


def _missing_etf_satellite_attribution_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "overall_status": "MISSING",
        "eligible_evidence": "MISSING",
        "fallback_evidence": "MISSING",
        "role_evidence": "MISSING",
        "risk_note": "satellite attribution report artifact is missing",
        "weak_evidence": "satellite attribution report artifact is missing",
        "manual_review": "继续 observe-only；Reader Brief 不运行 satellite attribution CLI。",
        "detail_report": "",
        "safety_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "manual_review_required": True,
        "summary_sentence": "Satellite Attribution Review: no latest attribution report found.",
        "limitation": (
            "Satellite attribution report artifact is missing; Reader Brief does not run "
            "satellite attribution."
        ),
    }


def _satellite_attribution_weak_evidence(dimensions: Mapping[str, Any]) -> str:
    if not dimensions:
        return "MISSING"
    priority = [
        ("sample_quality", dimensions.get("sample_quality")),
        ("data_coverage", dimensions.get("data_coverage")),
        ("eligible_outperformance_evidence", dimensions.get("eligible_outperformance_evidence")),
        ("fallback_protection_evidence", dimensions.get("fallback_protection_evidence")),
        ("score_ranking_evidence", dimensions.get("score_ranking_evidence")),
        ("risk_adjusted_evidence", dimensions.get("risk_adjusted_evidence")),
        ("AI_interaction_evidence", dimensions.get("AI_interaction_evidence")),
    ]
    weak_values = {"insufficient", "negative", "weak", "unknown", "FAIL"}
    for name, value in priority:
        text = _text(value)
        if text in weak_values:
            return f"{name}={text}"
    return "none"


def _etf_satellite_replacement_summary(report_index: Mapping[str, Any]) -> dict[str, Any]:
    if not report_index:
        return _missing_etf_satellite_replacement_summary()
    report_path = _report_index_artifact_path(report_index, "etf_satellite_replacement_report")
    report = _read_optional_json(report_path)
    if not report:
        return _missing_etf_satellite_replacement_summary()
    plan = _mapping(report.get("replacement_plan"))
    allocations = _records(plan.get("satellite_allocations"))
    eligibility = _records(report.get("replacement_eligibility"))
    eligible = _texts(report.get("eligible_stocks"))
    watchlist = _texts(report.get("watchlist"))
    fallback = _texts(report.get("fallback_to_etf_stocks"))
    proposed = _satellite_replacement_delta_text(plan)
    main_reason = _satellite_main_reason(report, allocations)
    main_blocker = _satellite_main_blocker(eligibility)
    safety_status = _etf_satellite_safety_status(report, plan)
    no_eligible = not eligible
    summary_sentence = (
        "Satellite Replacement: no eligible stock replacement. Default ETF exposure "
        "remains preferred."
        if no_eligible
        else (
            f"Satellite Replacement: eligible stocks {_format_english_list(eligible)}; "
            f"candidate-only replacement {proposed}; production_effect=none."
        )
    )
    return {
        "availability": "AVAILABLE",
        "status": "NO_ELIGIBLE" if no_eligible else "CANDIDATE_REPLACEMENT_AVAILABLE",
        "summary_sentence": summary_sentence,
        "eligible_stocks": _format_english_list(eligible) or "none",
        "watchlist": _format_english_list(watchlist) or "none",
        "fallback_to_etf": _format_english_list(fallback) or "none",
        "proposed_candidate_replacement": proposed,
        "main_reason": main_reason,
        "main_blocker": main_blocker,
        "safety_status": safety_status,
        "detail_report": "" if report_path is None else str(report_path),
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
    }


def _missing_etf_satellite_replacement_summary() -> dict[str, Any]:
    return {
        "availability": "MISSING",
        "status": "MISSING",
        "summary_sentence": (
            "Satellite Replacement: no eligible stock replacement. Default ETF exposure "
            "remains preferred."
        ),
        "eligible_stocks": "none",
        "watchlist": "none",
        "fallback_to_etf": "unknown",
        "proposed_candidate_replacement": "none",
        "main_reason": "satellite replacement report artifact is missing",
        "main_blocker": "SATELLITE_REPORT_MISSING",
        "safety_status": "MISSING",
        "detail_report": "",
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": "none",
        "limitation": (
            "Satellite replacement report artifact is missing; Reader Brief does not "
            "run satellite scoring."
        ),
    }


def _etf_satellite_safety_status(*payloads: Mapping[str, Any]) -> str:
    material = [payload for payload in payloads if payload]
    if not material:
        return "MISSING"
    safe = all(
        payload.get("observe_only") in (None, True)
        and payload.get("candidate_only") in (None, True)
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        and payload.get("broker_action") in (None, "none")
        and payload.get("manual_review_required") in (None, True)
        for payload in material
    )
    return (
        "observe_only=true; candidate_only=true; production_effect=none; "
        "broker_action=none; manual_review_required=true"
        if safe
        else "SAFETY_REVIEW_REQUIRED"
    )


def _satellite_replacement_delta_text(plan: Mapping[str, Any]) -> str:
    allocations = _records(plan.get("satellite_allocations"))
    if not allocations:
        return "none"
    pieces: list[str] = []
    replaced = _mapping(plan.get("replaced_etf"))
    for etf, weight in replaced.items():
        parsed = _float_or_none(weight)
        if parsed is not None and parsed > 0:
            pieces.append(f"{etf} -{_format_percent(parsed)}")
    for allocation in allocations:
        pieces.append(
            f"{_text(allocation.get('ticker'))} +{_format_percent(allocation.get('allocation'))}"
        )
    return ", ".join(pieces) if pieces else "none"


def _satellite_main_reason(
    report: Mapping[str, Any],
    allocations: list[dict[str, Any]],
) -> str:
    if allocations:
        return (
            "eligible stocks passed relative-strength, trend, risk, AI confirmation, "
            "and replacement-cap checks"
        )
    drivers = _texts(report.get("top_positive_drivers"))
    return drivers[0] if drivers else "no eligible replacement driver"


def _satellite_main_blocker(eligibility: list[dict[str, Any]]) -> str:
    blockers: list[str] = []
    for row in eligibility:
        blockers.extend(_texts(row.get("blockers")))
    if not blockers:
        return "none"
    counts = {blocker: blockers.count(blocker) for blocker in set(blockers)}
    return max(sorted(counts), key=lambda item: counts[item])


def _best_forward_candidate(rows: list[dict[str, Any]]) -> str:
    available = [
        row for row in rows if _float_or_none(row.get("excess_return_vs_baseline")) is not None
    ]
    if not available:
        return "MISSING"
    row = max(
        available,
        key=lambda item: _float_or_none(item.get("excess_return_vs_baseline")) or 0.0,
    )
    return (
        f"{_text(row.get('candidate_id'), 'MISSING')} "
        f"({_format_percent(row.get('excess_return_vs_baseline'))} vs baseline)"
    )


def _weakest_forward_candidate(rows: list[dict[str, Any]]) -> str:
    available = [
        row for row in rows if _float_or_none(row.get("excess_return_vs_baseline")) is not None
    ]
    if not available:
        return "MISSING"
    row = min(
        available,
        key=lambda item: _float_or_none(item.get("excess_return_vs_baseline")) or 0.0,
    )
    return (
        f"{_text(row.get('candidate_id'), 'MISSING')} "
        f"({_format_percent(row.get('excess_return_vs_baseline'))} vs baseline)"
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


def _latest_etf_backtest_summary_path(as_of: date) -> Path | None:
    root = PROJECT_ROOT / "reports" / "etf_portfolio" / "backtests"
    if not root.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/summary.json"):
        run_date = _etf_backtest_run_date(path.parent.name)
        if path.is_file() and run_date is not None and run_date <= as_of:
            candidates.append((run_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


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


def _etf_backtest_run_date(run_id: str) -> date | None:
    prefix = "etf-backtest-"
    if not run_id.startswith(prefix):
        return None
    stamp = run_id.removeprefix(prefix)
    try:
        return date.fromisoformat(f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}")
    except ValueError:
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
        "documentation_contract": 220,
        "reader_brief_quality": 230,
        "artifact_catalog": 240,
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
        "documentation_contract": "检查 registry 与 artifact catalog 契约覆盖。",
    }
    return reasons.get(artifact_id, "打开该 artifact 获取详细证据或治理上下文。")


_READER_CADENCE_OVERRIDES: dict[str, tuple[str, str, str]] = {
    "daily_score": ("daily", "daily", "下一个完整 U.S. equity trading day。"),
    "daily_decision_summary": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "reader_brief": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "reader_brief_quality": ("daily", "daily", "Reader Brief 生成后立即校验。"),
    "market_panel": ("daily", "daily", "随 daily-run 每个交易日生成。"),
    "score_change_attribution": ("daily", "daily", "随 daily-run 每个交易日对比上一信号日。"),
    "report_index": ("daily", "daily", "随 daily-run 每个交易日扫描 latest artifacts。"),
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
