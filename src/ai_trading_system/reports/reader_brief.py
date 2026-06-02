from __future__ import annotations

import html
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

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
    etf_weekly_review = _etf_weekly_review_summary(report_index)
    etf_decision_journal = _etf_decision_journal_summary(report_index)
    etf_parameter_review = _etf_parameter_review_summary(report_index)
    etf_weight_calibration = _etf_weight_calibration_summary(report_index)
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
        "backtest_shadow_governance": governance_summary,
        "parameter_shadow_review": parameter_shadow_review,
        "etf_backtest_summary": etf_backtest_summary,
        "etf_calibration_experiments": etf_calibration_experiments,
        "etf_forward_simulation": etf_forward_simulation,
        "etf_ai_confirmation": etf_ai_confirmation,
        "etf_ai_attribution": etf_ai_attribution,
        "etf_satellite_replacement": etf_satellite_replacement,
        "etf_weekly_review": etf_weekly_review,
        "etf_decision_journal": etf_decision_journal,
        "etf_parameter_review": etf_parameter_review,
        "etf_weight_calibration": etf_weight_calibration,
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
    governance = _mapping(payload.get("backtest_shadow_governance"))
    parameter_shadow = _mapping(payload.get("parameter_shadow_review"))
    etf_backtest = _mapping(payload.get("etf_backtest_summary"))
    etf_calibration = _mapping(payload.get("etf_calibration_experiments"))
    etf_forward = _mapping(payload.get("etf_forward_simulation"))
    etf_ai_confirmation = _mapping(payload.get("etf_ai_confirmation"))
    etf_ai_attribution = _mapping(payload.get("etf_ai_attribution"))
    etf_satellite = _mapping(payload.get("etf_satellite_replacement"))
    etf_weekly_review = _mapping(payload.get("etf_weekly_review"))
    etf_decision_journal = _mapping(payload.get("etf_decision_journal"))
    etf_parameter_review = _mapping(payload.get("etf_parameter_review"))
    etf_weight_calibration = _mapping(payload.get("etf_weight_calibration"))
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
                        parameter_shadow.get(
                            "signal_calibration_promotion_credit_signal_count"
                        ),
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
                        parameter_shadow.get(
                            "portfolio_candidate_tracking_effective_data_date"
                        ),
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
                        parameter_shadow.get(
                            "portfolio_tracking_review_days_until_short_review"
                        ),
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
                        parameter_shadow.get(
                            "weight_stability_rejected_by_turnover_prefilter"
                        ),
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
    return ", ".join(
        sorted({_text(item.get("recommended_action"), "UNKNOWN") for item in reviews})
    )


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
        "active_shadow_candidates": int(
            status_summary.get("active_candidate_count") or len(rows)
        ),
        "best_candidate": best,
        "weakest_candidate": weakest,
        "needs_more_data_count": int(status_summary.get("needs_more_data_count") or 0),
        "watch_count": int(status_summary.get("watch_count") or 0),
        "reject_pending_review_count": int(
            status_summary.get("reject_pending_review_count") or 0
        ),
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
            "ETF Forward Simulation: no active shadow candidates. "
            "Run experiment enrollment first."
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
        "observe_only=true; production_effect=none; broker_action=none; "
        "manual_review_required=true"
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
            "AI attribution report artifact is missing; Reader Brief does not run "
            "AI attribution."
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
            f"{_text(allocation.get('ticker'))} +"
            f"{_format_percent(allocation.get('allocation'))}"
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
        row
        for row in rows
        if _float_or_none(row.get("excess_return_vs_baseline")) is not None
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
        row
        for row in rows
        if _float_or_none(row.get("excess_return_vs_baseline")) is not None
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
        PROJECT_ROOT
        / "artifacts"
        / "signal_snapshots"
        / as_of.isoformat()
        / "signal_snapshot.json"
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
        (_float_or_none(best_transmission.get("target_to_actual_weight_effectiveness")) or 0.0)
        - (
            _float_or_none(
                baseline_transmission.get("target_to_actual_weight_effectiveness")
            )
            or 0.0
        )
    )
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
                "Portfolio tracking review is unreadable; production parameters remain "
                "unchanged."
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
        _text(item, "")
        for item in eligibility.get("blocking_checks", [])
        if _text(item, "")
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
                "Market data refresh summary is unreadable; recovery status requires "
                "manual review."
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
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


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
            [str(reason) for reason in reasons if str(reason)]
            if isinstance(reasons, list)
            else []
        )
        missing_assets = _missing_price_assets_from_reasons(reason_items)
        if missing_assets:
            return (
                "Shadow parameter review remains blocked because required price history is "
                f"missing for {_format_english_list(missing_assets)}."
            )
        if isinstance(reasons, list) and reasons:
            return (
                "Shadow parameter review remains blocked because "
                + "; ".join(str(reason) for reason in reasons if str(reason))
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
        else f"主要拖累来自 {', '.join(negative[:2])}" if negative else "主要驱动未充分披露"
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
        f'<span class="status-badge status-{html.escape(class_name)}">'
        f"{html.escape(label)}</span>"
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
                "研究晋升：" f"{_text(status_panel.get('research_promotion_status'), 'UNKNOWN')}"
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
