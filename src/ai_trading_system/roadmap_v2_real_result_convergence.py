from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.controlled_growth_component_research import (
    DEFAULT_AI_REGIME_BACKTEST_START,
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    _benchmark_specs,
    _load_config,
    _metric_rows,
    run_beta_adjusted_growth_edge_contract,
    run_controlled_growth_component_registry_v2_review,
    run_drawdown_guarded_growth_component_search,
    run_equal_risk_and_growth_dual_track_roadmap,
    run_growth_component_beta_exposure_attribution,
    run_growth_component_cost_turnover_sensitivity,
    run_growth_component_owner_decision_pack,
    run_growth_component_period_drawdown_validation,
    run_growth_component_readiness_gate,
    run_layer2_growth_component_restart_contract,
    run_low_turnover_controlled_growth_search,
    run_research_roadmap_v2_master_review,
    run_volatility_targeted_growth_component_search,
)
from ai_trading_system.data_foundation import utc_now_iso, write_foundation_artifact_pair
from ai_trading_system.research_roadmap_stabilization import (
    run_equal_risk_first_maturity_monitor,
    run_equal_risk_forward_aging_scheduler_integration,
    run_equal_risk_forward_aging_scoreboard_safety_gate,
    run_equal_risk_observation_continuity_check,
    run_equal_risk_reader_brief_live_summary,
)
from ai_trading_system.simple_baseline_portfolio_control import (
    DEFAULT_MARKETSTACK_PRICES_PATH,
    DEFAULT_PRICES_PATH,
    DEFAULT_RATES_PATH,
    DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
)

DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / "roadmap"
)
DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "dual_track_owner_decision_pack.md"
)
DEFAULT_ROADMAP_V2_REAL_RESULT_MASTER_REVIEW_DOC_PATH = (
    PROJECT_ROOT / "docs" / "research" / "roadmap_v2_real_result_master_review.md"
)

SAFETY_BOUNDARY: dict[str, Any] = {
    "production_effect": "none",
    "broker_action": "none",
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "manual_review_required": True,
    "research_only": True,
    "observe_only": True,
}

AI_REGIME_SUMMARY: dict[str, str] = {
    "market_regime": "ai_after_chatgpt",
    "anchor_event": "ChatGPT public launch",
    "anchor_date": "2022-11-30",
    "default_backtest_start": DEFAULT_AI_REGIME_BACKTEST_START.isoformat(),
}


def run_equal_risk_growth_v2_real_cli_suite(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    growth_owner_docs_path: Path = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    growth_roadmap_docs_path: Path = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    as_of_date: date | None = None,
    decision_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    source_runs = _real_cli_source_runs(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        roadmap_output_root=output_root,
        growth_owner_docs_path=growth_owner_docs_path,
        growth_roadmap_docs_path=growth_roadmap_docs_path,
        as_of_date=as_of_date,
        decision_date=decision_date,
        start_date=start_date,
        end_date=end_date,
    )
    rows: list[dict[str, Any]] = []
    source_payloads: dict[str, dict[str, Any]] = {}
    for report_id, command, builder in source_runs:
        payload = builder()
        source_payloads[report_id] = payload
        rows.append(_source_run_row(report_id, command, payload))

    blocked = [row["report_id"] for row in rows if _blocked_status(row["status"])]
    warning_rows = [
        row["report_id"]
        for row in rows
        if not _blocked_status(row["status"])
        and (
            row["warnings"]
            or row["blockers"]
            or _warning_status(row["status"])
        )
    ]
    if blocked:
        status = "EQUAL_RISK_GROWTH_V2_REAL_RUN_BLOCKED"
    elif warning_rows:
        status = "EQUAL_RISK_GROWTH_V2_REAL_RUN_WARN"
    else:
        status = "EQUAL_RISK_GROWTH_V2_REAL_RUN_PASS"

    payload = _payload(
        report_type="equal_risk_growth_v2_real_cli_suite_summary",
        title="Equal-Risk Growth V2 Real CLI Suite Summary",
        status=status,
        summary={
            "source_command_count": len(rows),
            "blocked_source_count": len(blocked),
            "warning_source_count": len(warning_rows),
            "top_growth_candidate": _first_present(
                row.get("top_growth_candidate") for row in rows
            ),
            "equal_risk_forward_aging_status": _latest_status(
                rows, prefix="equal_risk"
            ),
            "growth_component_status": _latest_status(rows, contains="growth"),
            **_safety_summary(),
        },
        required_command_count=len(source_runs),
        real_run_results=rows,
        source_artifacts={
            report_id: payload.get("artifact_paths", {})
            for report_id, payload in source_payloads.items()
        },
        warnings=warning_rows,
        blockers=blocked,
        report_registry_entry=_report_registry_entry(
            "equal_risk_growth_v2_real_cli_suite_summary",
            "Equal-Risk Growth V2 Real CLI Suite Summary",
            "aits research strategies equal-risk-growth-v2-real-cli-suite",
            "outputs/research_strategies/roadmap/"
            "equal_risk_growth_v2_real_cli_suite_summary",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_equal_risk_forward_aging_live_health_summary(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    continuity = run_equal_risk_observation_continuity_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
    )
    maturity = run_equal_risk_first_maturity_monitor(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
    )
    scoreboard = run_equal_risk_forward_aging_scoreboard_safety_gate(
        config_path=config_path,
        output_root=output_root,
    )
    reader = run_equal_risk_reader_brief_live_summary(
        config_path=config_path,
        output_root=output_root,
    )
    continuity_summary = _mapping(continuity.get("summary"))
    matured_counts = _matured_counts(maturity, scoreboard)
    warnings = _dedupe_text(
        [
            *_text_list(continuity.get("warnings")),
            *_text_list(maturity.get("warnings")),
            *_text_list(scoreboard.get("warning_reasons")),
            *_text_list(reader.get("warnings")),
        ]
    )
    blockers = _dedupe_text(
        [
            *_text_list(continuity.get("blockers")),
            *_text_list(maturity.get("blockers")),
            *_text_list(scoreboard.get("blockers")),
            *_text_list(reader.get("blockers")),
        ]
    )
    if _blocked_status(str(continuity.get("status"))) or _blocked_status(str(reader.get("status"))):
        blockers.append("source_health_check_blocked")
    blockers = _dedupe_text(blockers)
    if blockers:
        status = "EQUAL_RISK_FORWARD_AGING_BLOCKED"
    elif warnings or _warning_status(str(continuity.get("status"))):
        status = "EQUAL_RISK_FORWARD_AGING_WARN"
    else:
        status = "EQUAL_RISK_FORWARD_AGING_HEALTHY"
    payload = _payload(
        report_type="equal_risk_forward_aging_live_health_summary",
        title="Equal-Risk Forward-Aging Live Health Summary",
        status=status,
        summary={
            "latest_observation_date": continuity.get("latest_observation_date"),
            "observation_count": _int(
                continuity_summary.get("actual_observation_date_count")
            ),
            "missing_observation_count": _int(
                continuity_summary.get("missing_observation_date_count")
            ),
            "duplicate_observation_count": _int(
                continuity_summary.get("duplicate_observation_date_count")
            ),
            "scoreboard_status": scoreboard.get("scoreboard_status"),
            "reader_brief_status": reader.get("status"),
            **matured_counts,
            **_safety_summary(),
        },
        latest_observation_date=continuity.get("latest_observation_date"),
        observation_count=_int(continuity_summary.get("actual_observation_date_count")),
        missing_observation_count=_int(
            continuity_summary.get("missing_observation_date_count")
        ),
        duplicate_observation_count=_int(
            continuity_summary.get("duplicate_observation_date_count")
        ),
        invalid_artifact_count=_int(continuity.get("invalid_artifact_count")),
        invalid_artifact_replacement_count=_int(
            continuity.get("previous_invalid_artifact_replacement_count")
        ),
        duplicate_guard_status=(
            "PASS"
            if not _int(continuity_summary.get("duplicate_observation_date_count"))
            else "BLOCKED"
        ),
        maturity_updater_status=maturity.get("status"),
        scoreboard_status=scoreboard.get("scoreboard_status"),
        reader_brief_status=reader.get("status"),
        health_warnings=warnings,
        health_blockers=blockers,
        source_statuses={
            "equal_risk_observation_continuity_check": continuity.get("status"),
            "equal_risk_first_maturity_monitor": maturity.get("status"),
            "equal_risk_forward_aging_scoreboard_safety_gate": scoreboard.get("status"),
            "equal_risk_reader_brief_live_summary": reader.get("status"),
        },
        source_artifacts=_source_artifacts(
            {
                "equal_risk_observation_continuity_check": continuity,
                "equal_risk_first_maturity_monitor": maturity,
                "equal_risk_forward_aging_scoreboard_safety_gate": scoreboard,
                "equal_risk_reader_brief_live_summary": reader,
            }
        ),
        **matured_counts,
        report_registry_entry=_report_registry_entry(
            "equal_risk_forward_aging_live_health_summary",
            "Equal-Risk Forward-Aging Live Health Summary",
            "aits research strategies equal-risk-forward-aging-live-health-summary",
            "outputs/research_strategies/simple_baselines/"
            "equal_risk_forward_aging_live_health_summary",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_controlled_growth_v2_candidate_summary(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    sources = _growth_analysis_sources(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidates = _candidate_rows_from_sources(sources)
    rejected = [
        row
        for row in candidates
        if row["dominance_status"] != "CANDIDATE_NON_DOMINATED"
        or _float(row.get("beta_adjusted_edge")) <= 0.0
        or bool(row.get("blocked_reasons"))
    ]
    usable = [row for row in candidates if row not in rejected]
    source_blockers = [
        report_id
        for report_id, source in sources.items()
        if _blocked_status(str(source.get("status")))
    ]
    if source_blockers:
        status = "CONTROLLED_GROWTH_V2_BLOCKED"
    elif usable:
        status = "CONTROLLED_GROWTH_V2_CANDIDATES_FOUND"
    elif candidates:
        status = "CONTROLLED_GROWTH_V2_INCONCLUSIVE"
    else:
        status = "NO_CONTROLLED_GROWTH_V2_CANDIDATE"
    payload = _payload(
        report_type="controlled_growth_v2_candidate_summary",
        title="Controlled Growth V2 Candidate Summary",
        status=status,
        summary={
            "candidate_count": len(candidates),
            "usable_candidate_count": len(usable),
            "rejected_candidate_count": len(rejected),
            "top_growth_candidate": candidates[0].get("candidate_id") if candidates else None,
            "data_quality_status": _first_data_quality_status(sources.values()),
            **_safety_summary(),
        },
        top_by_annual_return=_sort_candidates(candidates, "annual_return", reverse=True),
        top_by_return_edge_vs_100_qqq=_sort_candidates(
            candidates, "return_edge_vs_100_qqq", reverse=True
        ),
        top_by_beta_adjusted_edge=_sort_candidates(
            candidates, "beta_adjusted_edge", reverse=True
        ),
        top_by_calmar=_sort_candidates(candidates, "calmar", reverse=True),
        top_by_sharpe=_sort_candidates(candidates, "sharpe", reverse=True),
        top_by_low_drawdown=_sort_candidates(candidates, "max_drawdown", reverse=True),
        top_by_low_turnover=_sort_candidates(candidates, "turnover", reverse=False),
        rejected_candidates=rejected,
        candidate_count=len(candidates),
        source_statuses={key: value.get("status") for key, value in sources.items()},
        source_artifacts=_source_artifacts(sources),
        blockers=source_blockers,
        report_registry_entry=_report_registry_entry(
            "controlled_growth_v2_candidate_summary",
            "Controlled Growth V2 Candidate Summary",
            "aits research strategies controlled-growth-v2-candidate-summary",
            "outputs/research_strategies/growth_components/"
            "controlled_growth_v2_candidate_summary",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_controlled_growth_beta_adjusted_edge_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    candidate_summary = run_controlled_growth_v2_candidate_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    period = run_growth_component_period_drawdown_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidates = _records(candidate_summary.get("top_by_beta_adjusted_edge"))
    candidate = candidates[0] if candidates else {}
    config = _load_config(config_path)
    complexity_penalty = _complexity_penalty(candidate, config)
    net_edge_after_penalty = _float(candidate.get("net_edge_after_penalty")) - (
        complexity_penalty * _edge_complexity_weight(config)
    )
    edge_row = {
        "candidate_id": candidate.get("candidate_id"),
        "raw_return_edge_vs_100_qqq": candidate.get("raw_return_edge_vs_100_qqq")
        or candidate.get("return_edge_vs_100_qqq"),
        "effective_qqq_beta": candidate.get("effective_qqq_beta"),
        "beta_adjusted_return_edge": candidate.get("beta_adjusted_edge"),
        "beta_adjusted_sharpe_edge": candidate.get("beta_adjusted_sharpe_edge"),
        "beta_adjusted_calmar_edge": candidate.get("beta_adjusted_calmar_edge"),
        "drawdown_penalty": candidate.get("drawdown_penalty"),
        "turnover_penalty": candidate.get("turnover_penalty"),
        "tqqq_path_dependency_penalty": candidate.get("path_dependency_penalty"),
        "complexity_penalty": _round(complexity_penalty),
        "net_edge_after_penalty": _round(net_edge_after_penalty),
        "edge_explanation": _edge_explanation(candidate, net_edge_after_penalty, period),
    }
    blockers = []
    if _blocked_status(str(candidate_summary.get("status"))):
        blockers.append("candidate_summary_blocked")
    if not candidate:
        blockers.append("no_candidate_for_beta_adjusted_review")
    net_minimum = _beta_edge_net_minimum(config)
    if blockers:
        status = "BETA_ADJUSTED_EDGE_BLOCKED"
    elif str(period.get("status")) == "GROWTH_REGIME_CONCENTRATED":
        status = "EDGE_REGIME_CONCENTRATED"
    elif _float(candidate.get("raw_return_edge_vs_100_qqq")) > 0.0 and _float(
        candidate.get("beta_adjusted_edge")
    ) <= 0.0:
        status = "BETA_EXPLAINS_EDGE"
    elif net_edge_after_penalty >= net_minimum and _drawdown_turnover_acceptable(candidate, config):
        status = "BETA_ADJUSTED_EDGE_MATERIAL"
    else:
        status = "EDGE_WEAK_AFTER_PENALTY"
    payload = _payload(
        report_type="controlled_growth_beta_adjusted_edge_review",
        title="Controlled Growth Beta-Adjusted Edge Review",
        status=status,
        summary={
            "candidate_id": candidate.get("candidate_id"),
            "net_edge_after_penalty": _round(net_edge_after_penalty),
            "net_edge_minimum": net_minimum,
            "period_status": period.get("status"),
            **_safety_summary(),
        },
        **edge_row,
        benchmark_comparisons=_benchmark_comparisons(
            candidate_id=str(candidate.get("candidate_id") or ""),
            prices_path=prices_path,
            config_path=config_path,
            start_date=start_date,
            end_date=end_date,
        ),
        source_statuses={
            "controlled_growth_v2_candidate_summary": candidate_summary.get("status"),
            "growth_component_period_drawdown_validation": period.get("status"),
        },
        source_artifacts=_source_artifacts(
            {
                "controlled_growth_v2_candidate_summary": candidate_summary,
                "growth_component_period_drawdown_validation": period,
            }
        ),
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "controlled_growth_beta_adjusted_edge_review",
            "Controlled Growth Beta-Adjusted Edge Review",
            "aits research strategies controlled-growth-beta-adjusted-edge-review",
            "outputs/research_strategies/growth_components/"
            "controlled_growth_beta_adjusted_edge_review",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_controlled_growth_period_drawdown_cost_triage(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    candidate_summary = run_controlled_growth_v2_candidate_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    period = run_growth_component_period_drawdown_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    cost = run_growth_component_cost_turnover_sensitivity(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidates = _records(candidate_summary.get("top_by_beta_adjusted_edge"))
    rows = [
        _triage_row(candidate, period=period, cost=cost)
        for candidate in candidates
        if candidate.get("candidate_id")
    ]
    blockers = [
        report_id
        for report_id, source in {
            "controlled_growth_v2_candidate_summary": candidate_summary,
            "growth_component_period_drawdown_validation": period,
            "growth_component_cost_turnover_sensitivity": cost,
        }.items()
        if _blocked_status(str(source.get("status")))
    ]
    if blockers:
        status = "GROWTH_TRIAGE_BLOCKED"
    elif str(cost.get("status")) in {"GROWTH_COST_BLOCKED", "GROWTH_TURNOVER_TOO_HIGH"}:
        status = "GROWTH_TRIAGE_COST_BLOCKED"
    elif str(period.get("status")) == "GROWTH_DRAWDOWN_RISK_TOO_HIGH" or any(
        row.get("drawdown_episode_status") == "DRAWDOWN_RISK_TOO_HIGH" for row in rows
    ):
        status = "GROWTH_TRIAGE_DRAWDOWN_RISK_TOO_HIGH"
    elif str(period.get("status")) == "GROWTH_REGIME_CONCENTRATED" or any(
        row.get("ai_rally_dependency") for row in rows
    ):
        status = "GROWTH_TRIAGE_REGIME_CONCENTRATED"
    elif str(cost.get("status")) == "GROWTH_COST_SENSITIVE":
        status = "GROWTH_TRIAGE_WARN"
    else:
        status = "GROWTH_TRIAGE_PASS"
    payload = _payload(
        report_type="controlled_growth_period_drawdown_cost_triage",
        title="Controlled Growth Period Drawdown Cost Triage",
        status=status,
        summary={
            "candidate_count": len(rows),
            "period_status": period.get("status"),
            "cost_sensitivity_status": cost.get("status"),
            "data_quality_status": _first_data_quality_status(
                [candidate_summary, period, cost]
            ),
            **_safety_summary(),
        },
        triage_rows=rows,
        required_coverage=[
            "2022_rate_hike_bear_market",
            "2023_recovery",
            "2024_ai_rally",
            "2025_to_latest",
            "largest_qqq_drawdown",
            "largest_tqqq_drawdown",
            "high_rate_sgov_carry_period",
        ],
        source_statuses={
            "controlled_growth_v2_candidate_summary": candidate_summary.get("status"),
            "growth_component_period_drawdown_validation": period.get("status"),
            "growth_component_cost_turnover_sensitivity": cost.get("status"),
        },
        source_artifacts=_source_artifacts(
            {
                "controlled_growth_v2_candidate_summary": candidate_summary,
                "growth_component_period_drawdown_validation": period,
                "growth_component_cost_turnover_sensitivity": cost,
            }
        ),
        blockers=blockers,
        report_registry_entry=_report_registry_entry(
            "controlled_growth_period_drawdown_cost_triage",
            "Controlled Growth Period Drawdown Cost Triage",
            "aits research strategies controlled-growth-period-drawdown-cost-triage",
            "outputs/research_strategies/growth_components/"
            "controlled_growth_period_drawdown_cost_triage",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_controlled_growth_component_final_gate(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    candidate_summary = run_controlled_growth_v2_candidate_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    beta = run_controlled_growth_beta_adjusted_edge_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    triage = run_controlled_growth_period_drawdown_cost_triage(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    readiness = run_growth_component_readiness_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=config_path,
        output_root=output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    candidate = _candidate_by_id(
        _records(candidate_summary.get("top_by_beta_adjusted_edge")),
        str(beta.get("candidate_id") or ""),
    )
    if not candidate:
        rows = _records(candidate_summary.get("top_by_beta_adjusted_edge"))
        candidate = rows[0] if rows else {}
    blocking_reasons = _final_gate_blockers(candidate, beta, triage, readiness)
    warning_reasons = _final_gate_warnings(candidate, triage)
    source_blocked = [
        report_id
        for report_id, source in {
            "controlled_growth_v2_candidate_summary": candidate_summary,
            "controlled_growth_beta_adjusted_edge_review": beta,
            "controlled_growth_period_drawdown_cost_triage": triage,
            "growth_component_readiness_gate": readiness,
        }.items()
        if _source_artifact_blocked_status(str(source.get("status")))
    ]
    if source_blocked:
        status = "GROWTH_COMPONENT_BLOCKED"
    elif not candidate or str(beta.get("status")) in {
        "BETA_EXPLAINS_EDGE",
        "EDGE_WEAK_AFTER_PENALTY",
        "EDGE_REGIME_CONCENTRATED",
    }:
        status = "NO_MATERIAL_GROWTH_EDGE"
    elif blocking_reasons:
        status = "GROWTH_COMPONENT_KEEP_RESEARCH_ONLY"
    else:
        status = "GROWTH_COMPONENT_REVIEWABLE"
    component_ready_review_allowed = status == "GROWTH_COMPONENT_REVIEWABLE"
    payload = _payload(
        report_type="controlled_growth_component_final_gate",
        title="Controlled Growth Component Final Gate",
        status=status,
        summary={
            "candidate_id": candidate.get("candidate_id"),
            "component_ready_review_allowed": component_ready_review_allowed,
            "blocking_reason_count": len(blocking_reasons) + len(source_blocked),
            "warning_reason_count": len(warning_reasons),
            **_safety_summary(),
        },
        candidate_id=candidate.get("candidate_id"),
        component_ready_review_allowed=component_ready_review_allowed,
        recommended_role=(
            "component_ready_review_candidate"
            if component_ready_review_allowed
            else "research_only_growth_candidate"
        ),
        blocking_reasons=_dedupe_text([*source_blocked, *blocking_reasons]),
        warning_reasons=warning_reasons,
        minimum_next_tasks=_minimum_next_tasks(component_ready_review_allowed),
        selected_candidate=candidate,
        source_statuses={
            "controlled_growth_v2_candidate_summary": candidate_summary.get("status"),
            "controlled_growth_beta_adjusted_edge_review": beta.get("status"),
            "controlled_growth_period_drawdown_cost_triage": triage.get("status"),
            "growth_component_readiness_gate": readiness.get("status"),
        },
        source_artifacts=_source_artifacts(
            {
                "controlled_growth_v2_candidate_summary": candidate_summary,
                "controlled_growth_beta_adjusted_edge_review": beta,
                "controlled_growth_period_drawdown_cost_triage": triage,
                "growth_component_readiness_gate": readiness,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "controlled_growth_component_final_gate",
            "Controlled Growth Component Final Gate",
            "aits research strategies controlled-growth-component-final-gate",
            "outputs/research_strategies/growth_components/"
            "controlled_growth_component_final_gate",
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_dual_track_owner_decision_pack(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    health = run_equal_risk_forward_aging_live_health_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=simple_config_path,
        output_root=simple_output_root,
        as_of_date=as_of_date,
    )
    candidate_summary = run_controlled_growth_v2_candidate_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    final_gate = run_controlled_growth_component_final_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    answers = {
        "1_equal_risk_forward_aging_healthy": health.get("status")
        == "EQUAL_RISK_FORWARD_AGING_HEALTHY",
        "2_equal_risk_defensive_primary_continues": True,
        "3_controlled_growth_v2_candidate_found": candidate_summary.get("status")
        == "CONTROLLED_GROWTH_V2_CANDIDATES_FOUND",
        "4_growth_candidate_component_ready_review_allowed": bool(
            final_gate.get("component_ready_review_allowed")
        ),
        "5_continue_growth_research_if_none": final_gate.get("status")
        in {"GROWTH_COMPONENT_KEEP_RESEARCH_ONLY", "NO_MATERIAL_GROWTH_EDGE"},
        "6_layer1_selector_archived_dry_run_only": True,
        "7_qqq_plus_growth_inactive_reference": True,
        "8_tail_risk_fallback_quarantined": True,
        "9_leaps_wheel_options_blocked": True,
        "10_no_paper_shadow_no_production_no_broker": True,
    }
    if _blocked_status(str(health.get("status"))) or _blocked_status(str(final_gate.get("status"))):
        recommendation = "BLOCKED"
    elif final_gate.get("component_ready_review_allowed") is True:
        recommendation = "PROMOTE_GROWTH_TO_COMPONENT_REVIEW"
    elif candidate_summary.get("status") == "NO_CONTROLLED_GROWTH_V2_CANDIDATE":
        recommendation = "CONTINUE_EQUAL_RISK_FORWARD_AGING_ONLY"
    elif final_gate.get("status") == "NO_MATERIAL_GROWTH_EDGE":
        recommendation = "PAUSE_GROWTH_RESEARCH"
    else:
        recommendation = "CONTINUE_EQUAL_RISK_AND_GROWTH_RESEARCH"
    payload = _payload(
        report_type="dual_track_owner_decision_pack",
        title="Dual-Track Owner Decision Pack",
        status="DUAL_TRACK_OWNER_DECISION_PACK_READY",
        summary={
            "owner_recommendation": recommendation,
            "equal_risk_health_status": health.get("status"),
            "growth_candidate_status": candidate_summary.get("status"),
            "growth_final_gate_status": final_gate.get("status"),
            **_safety_summary(),
        },
        owner_recommendation=recommendation,
        required_answers=answers,
        source_statuses={
            "equal_risk_forward_aging_live_health_summary": health.get("status"),
            "controlled_growth_v2_candidate_summary": candidate_summary.get("status"),
            "controlled_growth_component_final_gate": final_gate.get("status"),
        },
        source_artifacts=_source_artifacts(
            {
                "equal_risk_forward_aging_live_health_summary": health,
                "controlled_growth_v2_candidate_summary": candidate_summary,
                "controlled_growth_component_final_gate": final_gate,
            }
        ),
        report_registry_entry=_report_registry_entry(
            "dual_track_owner_decision_pack",
            "Dual-Track Owner Decision Pack",
            "aits research strategies dual-track-owner-decision-pack",
            "outputs/research_strategies/roadmap/dual_track_owner_decision_pack",
            extra_artifact_globs=["docs/research/dual_track_owner_decision_pack.md"],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Dual-Track Owner Decision Pack")
    payload["owner_decision_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def run_roadmap_v2_real_result_master_review(
    *,
    prices_path: Path = DEFAULT_PRICES_PATH,
    marketstack_prices_path: Path = DEFAULT_MARKETSTACK_PRICES_PATH,
    rates_path: Path = DEFAULT_RATES_PATH,
    simple_config_path: Path = DEFAULT_SIMPLE_BASELINE_REGISTRY_CONFIG_PATH,
    growth_config_path: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_CONFIG_PATH,
    simple_output_root: Path = DEFAULT_SIMPLE_BASELINE_OUTPUT_ROOT,
    growth_output_root: Path = DEFAULT_CONTROLLED_GROWTH_COMPONENT_OUTPUT_ROOT,
    output_root: Path = DEFAULT_ROADMAP_V2_REAL_RESULT_OUTPUT_ROOT,
    docs_path: Path = DEFAULT_ROADMAP_V2_REAL_RESULT_MASTER_REVIEW_DOC_PATH,
    growth_owner_docs_path: Path = DEFAULT_GROWTH_COMPONENT_OWNER_DECISION_DOC_PATH,
    growth_roadmap_docs_path: Path = DEFAULT_GROWTH_COMPONENT_ROADMAP_DOC_PATH,
    dual_track_docs_path: Path = DEFAULT_DUAL_TRACK_OWNER_DECISION_DOC_PATH,
    as_of_date: date | None = None,
    decision_date: date | None = None,
    start_date: date = DEFAULT_AI_REGIME_BACKTEST_START,
    end_date: date | None = None,
) -> dict[str, Any]:
    suite = run_equal_risk_growth_v2_real_cli_suite(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        output_root=output_root,
        growth_owner_docs_path=growth_owner_docs_path,
        growth_roadmap_docs_path=growth_roadmap_docs_path,
        as_of_date=as_of_date,
        decision_date=decision_date,
        start_date=start_date,
        end_date=end_date,
    )
    health = run_equal_risk_forward_aging_live_health_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=simple_config_path,
        output_root=simple_output_root,
        as_of_date=as_of_date,
    )
    candidate_summary = run_controlled_growth_v2_candidate_summary(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    beta = run_controlled_growth_beta_adjusted_edge_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    triage = run_controlled_growth_period_drawdown_cost_triage(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    final_gate = run_controlled_growth_component_final_gate(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        config_path=growth_config_path,
        output_root=growth_output_root,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    owner = run_dual_track_owner_decision_pack(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_prices_path,
        rates_path=rates_path,
        simple_config_path=simple_config_path,
        growth_config_path=growth_config_path,
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        output_root=output_root,
        docs_path=dual_track_docs_path,
        as_of_date=as_of_date,
        start_date=start_date,
        end_date=end_date,
    )
    source_payloads = {
        "equal_risk_growth_v2_real_cli_suite_summary": suite,
        "equal_risk_forward_aging_live_health_summary": health,
        "controlled_growth_v2_candidate_summary": candidate_summary,
        "controlled_growth_beta_adjusted_edge_review": beta,
        "controlled_growth_period_drawdown_cost_triage": triage,
        "controlled_growth_component_final_gate": final_gate,
        "dual_track_owner_decision_pack": owner,
    }
    if any(
        _source_artifact_blocked_status(str(source.get("status")))
        for source in source_payloads.values()
    ):
        status = "ROADMAP_V2_BLOCKED"
    elif final_gate.get("component_ready_review_allowed") is True:
        status = "GROWTH_COMPONENT_REVIEWABLE"
    elif candidate_summary.get("status") in {
        "NO_CONTROLLED_GROWTH_V2_CANDIDATE",
        "CONTROLLED_GROWTH_V2_INCONCLUSIVE",
    } or final_gate.get("status") == "NO_MATERIAL_GROWTH_EDGE":
        status = "NO_GROWTH_EDGE_FOUND"
    elif owner.get("owner_recommendation") == "CONTINUE_EQUAL_RISK_FORWARD_AGING_ONLY":
        status = "CONTINUE_EQUAL_RISK_FORWARD_AGING"
    else:
        status = "CONTINUE_CONTROLLED_GROWTH_RESEARCH"
    answers = {
        "1_real_run_1031_to_1048_complete": _int(
            _mapping(suite.get("summary")).get("source_command_count")
        )
        == _int(suite.get("required_command_count")),
        "2_equal_risk_forward_aging_healthy": health.get("status")
        == "EQUAL_RISK_FORWARD_AGING_HEALTHY",
        "3_maturity_scoreboard_insufficient_constraints_preserved": health.get(
            "scoreboard_status"
        )
        in {"INSUFFICIENT", "RESEARCH_ONLY_READY"},
        "4_controlled_growth_material_candidate_found": candidate_summary.get("status")
        == "CONTROLLED_GROWTH_V2_CANDIDATES_FOUND",
        "5_edge_survives_beta_adjustment": beta.get("status")
        == "BETA_ADJUSTED_EDGE_MATERIAL",
        "6_growth_candidate_component_reviewable": final_gate.get(
            "component_ready_review_allowed"
        )
        is True,
        "7_layer1_selector_remains_archived": True,
        "8_next_minimum_task": _roadmap_next_task(status),
    }
    payload = _payload(
        report_type="roadmap_v2_real_result_master_review",
        title="Roadmap V2 Real Result Master Review",
        status=status,
        summary={
            "final_status": status,
            "equal_risk_health_status": health.get("status"),
            "growth_candidate_status": candidate_summary.get("status"),
            "beta_adjusted_edge_status": beta.get("status"),
            "triage_status": triage.get("status"),
            "final_gate_status": final_gate.get("status"),
            "owner_recommendation": owner.get("owner_recommendation"),
            **_safety_summary(),
        },
        required_answers=answers,
        source_statuses={key: value.get("status") for key, value in source_payloads.items()},
        source_artifacts=_source_artifacts(source_payloads),
        final_conclusions=[status, "KEEP_ALL_RESEARCH_ONLY"],
        owner_next_action=_roadmap_next_task(status),
        report_registry_entry=_report_registry_entry(
            "roadmap_v2_real_result_master_review",
            "Roadmap V2 Real Result Master Review",
            "aits research strategies roadmap-v2-real-result-master-review",
            "outputs/research_strategies/roadmap/roadmap_v2_real_result_master_review",
            extra_artifact_globs=[
                "docs/research/roadmap_v2_real_result_master_review.md"
            ],
        ),
    )
    _write_pair(payload, output_root, payload["report_type"])
    _write_owner_doc(payload, docs_path, "Roadmap V2 Real Result Master Review")
    payload["roadmap_doc_path"] = str(docs_path)
    _write_pair(payload, output_root, payload["report_type"])
    return payload


def _real_cli_source_runs(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    simple_config_path: Path,
    growth_config_path: Path,
    simple_output_root: Path,
    growth_output_root: Path,
    roadmap_output_root: Path,
    growth_owner_docs_path: Path,
    growth_roadmap_docs_path: Path,
    as_of_date: date | None,
    decision_date: date | None,
    start_date: date,
    end_date: date | None,
) -> list[tuple[str, str, Callable[[], dict[str, Any]]]]:
    return [
        (
            "equal_risk_forward_aging_scheduler_integration",
            "aits research strategies equal-risk-forward-aging-scheduler-integration",
            lambda: run_equal_risk_forward_aging_scheduler_integration(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=simple_config_path,
                output_root=simple_output_root,
                as_of_date=as_of_date,
                decision_date=decision_date,
            ),
        ),
        (
            "equal_risk_observation_continuity_check",
            "aits research strategies equal-risk-observation-continuity-check",
            lambda: run_equal_risk_observation_continuity_check(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=simple_config_path,
                output_root=simple_output_root,
                as_of_date=as_of_date,
            ),
        ),
        (
            "equal_risk_first_maturity_monitor",
            "aits research strategies equal-risk-first-maturity-monitor",
            lambda: run_equal_risk_first_maturity_monitor(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=simple_config_path,
                output_root=simple_output_root,
                as_of_date=as_of_date,
            ),
        ),
        (
            "equal_risk_forward_aging_scoreboard_safety_gate",
            "aits research strategies equal-risk-forward-aging-scoreboard-safety-gate",
            lambda: run_equal_risk_forward_aging_scoreboard_safety_gate(
                config_path=simple_config_path,
                output_root=simple_output_root,
            ),
        ),
        (
            "equal_risk_reader_brief_live_summary",
            "aits research strategies equal-risk-reader-brief-live-summary",
            lambda: run_equal_risk_reader_brief_live_summary(
                config_path=simple_config_path,
                output_root=simple_output_root,
            ),
        ),
        (
            "layer2_growth_component_restart_contract",
            "aits research strategies layer2-growth-component-restart-contract",
            lambda: run_layer2_growth_component_restart_contract(
                config_path=growth_config_path,
                output_root=growth_output_root,
            ),
        ),
        (
            "controlled_growth_component_registry_v2_review",
            "aits research strategies controlled-growth-component-registry-v2-review",
            lambda: run_controlled_growth_component_registry_v2_review(
                config_path=growth_config_path,
                output_root=growth_output_root,
            ),
        ),
        (
            "beta_adjusted_growth_edge_contract",
            "aits research strategies beta-adjusted-growth-edge-contract",
            lambda: run_beta_adjusted_growth_edge_contract(
                config_path=growth_config_path,
                output_root=growth_output_root,
            ),
        ),
        (
            "low_turnover_controlled_growth_search",
            "aits research strategies low-turnover-controlled-growth-search",
            lambda: run_low_turnover_controlled_growth_search(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "volatility_targeted_growth_component_search",
            "aits research strategies volatility-targeted-growth-component-search",
            lambda: run_volatility_targeted_growth_component_search(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "drawdown_guarded_growth_component_search",
            "aits research strategies drawdown-guarded-growth-component-search",
            lambda: run_drawdown_guarded_growth_component_search(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_component_beta_exposure_attribution",
            "aits research strategies growth-component-beta-exposure-attribution",
            lambda: run_growth_component_beta_exposure_attribution(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_component_period_drawdown_validation",
            "aits research strategies growth-component-period-drawdown-validation",
            lambda: run_growth_component_period_drawdown_validation(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_component_cost_turnover_sensitivity",
            "aits research strategies growth-component-cost-turnover-sensitivity",
            lambda: run_growth_component_cost_turnover_sensitivity(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_component_readiness_gate",
            "aits research strategies growth-component-readiness-gate",
            lambda: run_growth_component_readiness_gate(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "growth_component_owner_decision_pack",
            "aits research strategies growth-component-owner-decision-pack",
            lambda: run_growth_component_owner_decision_pack(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                output_root=growth_output_root,
                docs_path=growth_owner_docs_path,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
        (
            "equal_risk_and_growth_dual_track_roadmap",
            "aits research strategies equal-risk-and-growth-dual-track-roadmap",
            lambda: run_equal_risk_and_growth_dual_track_roadmap(
                output_root=roadmap_output_root
            ),
        ),
        (
            "research_roadmap_v2_master_review",
            "aits research strategies research-roadmap-v2-master-review",
            lambda: run_research_roadmap_v2_master_review(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=growth_config_path,
                simple_output_root=simple_output_root,
                growth_output_root=growth_output_root,
                output_root=roadmap_output_root,
                owner_docs_path=growth_owner_docs_path,
                docs_path=growth_roadmap_docs_path,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
    ]


def _growth_analysis_sources(
    *,
    prices_path: Path,
    marketstack_prices_path: Path,
    rates_path: Path,
    config_path: Path,
    output_root: Path,
    as_of_date: date | None,
    start_date: date,
    end_date: date | None,
) -> dict[str, dict[str, Any]]:
    return {
        "low_turnover_controlled_growth_search": run_low_turnover_controlled_growth_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        ),
        "volatility_targeted_growth_component_search": (
            run_volatility_targeted_growth_component_search(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
        ),
        "drawdown_guarded_growth_component_search": run_drawdown_guarded_growth_component_search(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        ),
        "growth_component_beta_exposure_attribution": (
            run_growth_component_beta_exposure_attribution(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
        ),
        "growth_component_period_drawdown_validation": (
            run_growth_component_period_drawdown_validation(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
        ),
        "growth_component_cost_turnover_sensitivity": (
            run_growth_component_cost_turnover_sensitivity(
                prices_path=prices_path,
                marketstack_prices_path=marketstack_prices_path,
                rates_path=rates_path,
                config_path=config_path,
                output_root=output_root,
                as_of_date=as_of_date,
                start_date=start_date,
                end_date=end_date,
            )
        ),
        "growth_component_readiness_gate": run_growth_component_readiness_gate(
            prices_path=prices_path,
            marketstack_prices_path=marketstack_prices_path,
            rates_path=rates_path,
            config_path=config_path,
            output_root=output_root,
            as_of_date=as_of_date,
            start_date=start_date,
            end_date=end_date,
        ),
    }


def _source_run_row(report_id: str, command: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    artifact_paths = _mapping(payload.get("artifact_paths"))
    return {
        "report_id": report_id,
        "command": command,
        "status": payload.get("status"),
        "warnings": _dedupe_text(
            [
                *_text_list(payload.get("warnings")),
                *_text_list(payload.get("warning_reasons")),
            ]
        ),
        "blockers": _dedupe_text(
            [
                *_text_list(payload.get("blockers")),
                *_text_list(payload.get("blocking_reasons")),
                *_text_list(payload.get("readiness_blockers")),
            ]
        ),
        "artifact_json_path": artifact_paths.get("json_path"),
        "artifact_md_path": artifact_paths.get("markdown_path"),
        "data_quality_status": _data_quality_status(payload),
        "candidate_count": _candidate_count(payload),
        "top_growth_candidate": _top_growth_candidate(payload),
        "equal_risk_forward_aging_status": payload.get("status")
        if report_id.startswith("equal_risk")
        else None,
        "growth_component_status": payload.get("status")
        if "growth" in report_id or "roadmap" in report_id
        else None,
        "paper_shadow_allowed": payload.get("paper_shadow_allowed", False),
        "production_allowed": payload.get("production_allowed", False),
        "broker_action": payload.get("broker_action", "none"),
        "summary_status": summary.get("status"),
    }


def _candidate_rows_from_sources(sources: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    data_quality_status = _first_data_quality_status(sources.values())
    attribution_by_id = {
        str(row.get("candidate_id")): row
        for row in _records(
            _mapping(sources.get("growth_component_beta_exposure_attribution")).get(
                "attribution_rows"
            )
        )
    }
    best: dict[str, dict[str, Any]] = {}
    for source_id in (
        "low_turnover_controlled_growth_search",
        "volatility_targeted_growth_component_search",
        "drawdown_guarded_growth_component_search",
    ):
        for row in _records(_mapping(sources.get(source_id)).get("candidate_results")):
            candidate = _candidate_summary_row(
                row,
                data_quality_status=data_quality_status,
                attribution=attribution_by_id.get(str(row.get("candidate_id"))),
            )
            candidate_id = str(candidate.get("candidate_id"))
            if candidate_id not in best or _float(
                candidate.get("beta_adjusted_edge")
            ) > _float(best[candidate_id].get("beta_adjusted_edge")):
                best[candidate_id] = candidate
    return sorted(
        best.values(), key=lambda row: _float(row.get("beta_adjusted_edge")), reverse=True
    )


def _candidate_summary_row(
    row: Mapping[str, Any],
    *,
    data_quality_status: str | None,
    attribution: Mapping[str, Any] | None,
) -> dict[str, Any]:
    avg = _mapping(row.get("average_weights"))
    attr = _mapping(attribution)
    candidate_family = str(row.get("candidate_type") or row.get("candidate_family") or "growth")
    return {
        "candidate_id": row.get("candidate_id") or row.get("strategy_id"),
        "candidate_family": candidate_family,
        "annual_return": row.get("annual_return"),
        "return_edge_vs_100_qqq": row.get("return_edge_vs_100_qqq")
        or row.get("annual_return_vs_qqq"),
        "raw_return_edge_vs_100_qqq": row.get("raw_return_edge_vs_100_qqq")
        or row.get("annual_return_vs_qqq"),
        "max_drawdown": row.get("max_drawdown"),
        "sharpe": row.get("sharpe"),
        "calmar": row.get("calmar"),
        "turnover": row.get("turnover"),
        "switch_count": row.get("switch_count") or row.get("rebalance_count"),
        "switches_per_year": row.get("switches_per_year"),
        "effective_qqq_beta": row.get("effective_qqq_beta"),
        "max_tqqq_weight": row.get("max_tqqq_weight")
        or row.get("max_tqqq_weight_observed"),
        "average_tqqq_weight": attr.get("average_tqqq_weight", avg.get("TQQQ", 0.0)),
        "sgov_weight": attr.get("average_sgov_weight", avg.get("SGOV", 0.0)),
        "beta_adjusted_edge": row.get("beta_adjusted_edge"),
        "beta_adjusted_sharpe_edge": row.get("beta_adjusted_sharpe_edge"),
        "beta_adjusted_calmar_edge": row.get("beta_adjusted_calmar_edge"),
        "drawdown_penalty": row.get("drawdown_penalty"),
        "turnover_penalty": row.get("turnover_penalty"),
        "path_dependency_penalty": row.get("path_dependency_penalty"),
        "net_edge_after_penalty": row.get("net_edge_after_penalty"),
        "dominance_status": row.get("dominance_status", "UNKNOWN"),
        "definition_hash": row.get("definition_hash") or row.get("policy_hash"),
        "data_quality_status": data_quality_status,
        "blocked_reasons": _text_list(row.get("blocked_reasons")),
        "research_commentary": _candidate_commentary(row),
    }


def _candidate_commentary(row: Mapping[str, Any]) -> str:
    if row.get("dominance_status") == "DOMINATED_BY_100_QQQ":
        return "dominated_by_100_qqq_under_current_research_metrics"
    if _float(row.get("beta_adjusted_edge")) > 0.0:
        return "beta_adjusted_edge_positive_research_only"
    return "edge_not_material_after_beta_adjustment"


def _benchmark_comparisons(
    *,
    candidate_id: str,
    prices_path: Path,
    config_path: Path,
    start_date: date,
    end_date: date | None,
) -> list[dict[str, Any]]:
    config = _load_config(config_path)
    benchmark_rows = _metric_rows(
        _benchmark_specs(config),
        prices_path=prices_path,
        config=config,
        start_date=start_date,
        end_date=end_date,
    )
    wanted = {"100_qqq", "equal_risk_qqq_sgov", "qqq_60_sgov_40", "qqq_50_sgov_50"}
    rows = []
    for row in benchmark_rows:
        benchmark_id = str(row.get("candidate_id") or row.get("strategy_id"))
        if benchmark_id not in wanted:
            continue
        rows.append(
            {
                "candidate_id": candidate_id,
                "benchmark_id": benchmark_id,
                "benchmark_annual_return": row.get("annual_return"),
                "benchmark_max_drawdown": row.get("max_drawdown"),
                "benchmark_sharpe": row.get("sharpe"),
                "benchmark_calmar": row.get("calmar"),
                "benchmark_effective_qqq_beta": row.get("effective_qqq_beta"),
            }
        )
    return rows


def _triage_row(
    candidate: Mapping[str, Any],
    *,
    period: Mapping[str, Any],
    cost: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id"))
    period_rows = [
        row
        for row in _records(period.get("period_rows"))
        if str(row.get("strategy_id")) == candidate_id
    ]
    drawdown_rows = [
        row
        for row in _records(period.get("drawdown_episode_rows"))
        if str(row.get("strategy_id")) == candidate_id
    ]
    cost_rows = [
        row
        for row in _records(cost.get("scenario_rows"))
        if str(row.get("candidate_id")) == candidate_id
    ]
    covered_periods = [row for row in period_rows if row.get("coverage_status") == "COVERED"]
    worst_period = min(
        covered_periods,
        key=lambda row: _float(row.get("annual_return_vs_qqq")),
        default={},
    )
    best_period = max(
        covered_periods,
        key=lambda row: _float(row.get("annual_return_vs_qqq")),
        default={},
    )
    recovery_rows = [
        row
        for row in drawdown_rows
        if str(row.get("episode_id")) in {"2023_recovery", "2024_ai_rally"}
    ]
    risk_off_rows = [
        row for row in drawdown_rows if _float(row.get("max_drawdown_vs_qqq")) < 0.0
    ]
    period_status = "PERIOD_SPLIT_PASS"
    if str(period.get("status")) == "GROWTH_REGIME_CONCENTRATED":
        period_status = "REGIME_CONCENTRATED"
    elif any(row.get("coverage_status") != "COVERED" for row in period_rows):
        period_status = "PERIOD_COVERAGE_WARN"
    drawdown_status = "DRAWDOWN_EPISODE_PASS"
    if str(period.get("status")) == "GROWTH_DRAWDOWN_RISK_TOO_HIGH" or any(
        row.get("tqqq_drawdown_unacceptable") for row in drawdown_rows
    ):
        drawdown_status = "DRAWDOWN_RISK_TOO_HIGH"
    cost_status = str(cost.get("status") or "UNKNOWN")
    ai_rally_dependency = _candidate_ai_rally_dependency(candidate_id, period)
    missed_rebound_cost = abs(
        min((_float(row.get("strategy_return_vs_qqq")) for row in recovery_rows), default=0.0)
    )
    late_risk_off_cost = abs(
        min((_float(row.get("max_drawdown_vs_qqq")) for row in risk_off_rows), default=0.0)
    )
    late_risk_on_cost = missed_rebound_cost
    cost_drag = max(
        (_float(row.get("annual_return_degradation")) for row in cost_rows),
        default=0.0,
    )
    return {
        "candidate_id": candidate_id,
        "period_split_status": period_status,
        "drawdown_episode_status": drawdown_status,
        "cost_sensitivity_status": cost_status,
        "worst_period": worst_period.get("period_id"),
        "best_period": best_period.get("period_id"),
        "ai_rally_dependency": ai_rally_dependency,
        "bear_market_drawdown": _bear_market_drawdown(drawdown_rows),
        "missed_rebound_cost": _round(missed_rebound_cost),
        "late_risk_off_cost": _round(late_risk_off_cost),
        "late_risk_on_cost": _round(late_risk_on_cost),
        "cost_drag": _round(cost_drag),
        "turnover": candidate.get("turnover"),
        "switch_count": candidate.get("switch_count"),
        "triage_commentary": _triage_commentary(
            period_status=period_status,
            drawdown_status=drawdown_status,
            cost_status=cost_status,
        ),
    }


def _final_gate_blockers(
    candidate: Mapping[str, Any],
    beta: Mapping[str, Any],
    triage: Mapping[str, Any],
    readiness: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not candidate:
        return ["no_growth_candidate"]
    if beta.get("status") != "BETA_ADJUSTED_EDGE_MATERIAL":
        blockers.append("beta_adjusted_edge_not_material")
    if candidate.get("dominance_status") == "DOMINATED_BY_100_QQQ":
        blockers.append("dominated_by_100_qqq")
    if _float(candidate.get("beta_adjusted_calmar_edge")) < 0.0 and _float(
        candidate.get("beta_adjusted_sharpe_edge")
    ) < 0.0:
        blockers.append("risk_adjusted_metrics_weaker_after_beta_adjustment")
    if str(triage.get("status")) in {
        "GROWTH_TRIAGE_REGIME_CONCENTRATED",
        "GROWTH_TRIAGE_DRAWDOWN_RISK_TOO_HIGH",
        "GROWTH_TRIAGE_COST_BLOCKED",
        "GROWTH_TRIAGE_BLOCKED",
    }:
        blockers.append(f"triage_status:{triage.get('status')}")
    blockers.extend(_text_list(readiness.get("blocking_reasons")))
    if not candidate.get("definition_hash"):
        blockers.append("definition_hash_missing")
    if candidate.get("data_quality_status") in {"FAIL", "BLOCKED"}:
        blockers.append("data_quality_blocked")
    return _dedupe_text(blockers)


def _final_gate_warnings(
    candidate: Mapping[str, Any],
    triage: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if str(triage.get("status")) == "GROWTH_TRIAGE_WARN":
        warnings.append("triage_warn")
    if _float(candidate.get("turnover")) > 0.0 and not candidate.get("switch_count"):
        warnings.append("turnover_present_without_switch_count")
    return warnings


def _matured_counts(
    maturity: Mapping[str, Any],
    scoreboard: Mapping[str, Any],
) -> dict[str, int]:
    scoreboard_counts = _mapping(scoreboard.get("matured_counts"))
    return {
        key: _int(maturity.get(key, scoreboard_counts.get(key)))
        for key in (
            "matured_5d_count",
            "matured_10d_count",
            "matured_20d_count",
            "matured_60d_count",
            "matured_120d_count",
        )
    }


def _source_artifacts(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    return {report_id: source.get("artifact_paths", {}) for report_id, source in sources.items()}


def _data_quality_status(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    data_quality = _mapping(payload.get("data_quality"))
    return (
        _text(summary.get("data_quality_status"))
        or _text(payload.get("data_quality_status"))
        or _text(data_quality.get("status"))
        or None
    )


def _first_data_quality_status(sources: Any) -> str | None:
    for source in sources:
        status = _data_quality_status(_mapping(source))
        if status:
            return status
    return None


def _candidate_count(payload: Mapping[str, Any]) -> int:
    summary = _mapping(payload.get("summary"))
    for key in ("candidate_count", "usable_candidate_count"):
        if key in summary:
            return _int(summary.get(key))
    for key in ("candidate_results", "attribution_rows", "candidate_summaries"):
        rows = _records(payload.get(key))
        if rows:
            return len(rows)
    return 0


def _top_growth_candidate(payload: Mapping[str, Any]) -> str | None:
    summary = _mapping(payload.get("summary"))
    for key in ("top_growth_candidate", "top_candidate", "candidate_id"):
        value = _text(summary.get(key))
        if value:
            return value
    value = _text(payload.get("candidate_id"))
    if value:
        return value
    rows = _records(payload.get("candidate_results"))
    if rows:
        return _text(rows[0].get("candidate_id") or rows[0].get("strategy_id")) or None
    return None


def _latest_status(
    rows: list[Mapping[str, Any]],
    *,
    prefix: str | None = None,
    contains: str | None = None,
) -> str | None:
    selected = []
    for row in rows:
        report_id = str(row.get("report_id") or "")
        if prefix and report_id.startswith(prefix):
            selected.append(str(row.get("status")))
        elif contains and contains in report_id:
            selected.append(str(row.get("status")))
    return selected[-1] if selected else None


def _candidate_by_id(rows: list[dict[str, Any]], candidate_id: str) -> dict[str, Any]:
    if not candidate_id:
        return {}
    return next((row for row in rows if str(row.get("candidate_id")) == candidate_id), {})


def _candidate_ai_rally_dependency(candidate_id: str, period: Mapping[str, Any]) -> bool:
    return any(
        str(row.get("candidate_id")) == candidate_id and bool(row.get("only_ai_rally_effective"))
        for row in _records(period.get("candidate_summaries"))
    )


def _bear_market_drawdown(drawdown_rows: list[Mapping[str, Any]]) -> Any:
    for row in drawdown_rows:
        if str(row.get("episode_id")) == "2022_rate_hike_bear":
            return row.get("max_drawdown")
    return None


def _sort_candidates(
    candidates: list[dict[str, Any]],
    key: str,
    *,
    reverse: bool,
) -> list[dict[str, Any]]:
    return sorted(candidates, key=lambda row: _float(row.get(key)), reverse=reverse)


def _edge_explanation(
    candidate: Mapping[str, Any],
    net_edge_after_penalty: float,
    period: Mapping[str, Any],
) -> str:
    if not candidate:
        return "no_candidate_available_for_edge_review"
    if str(period.get("status")) == "GROWTH_REGIME_CONCENTRATED":
        return "edge_concentrated_in_limited_regime"
    if _float(candidate.get("beta_adjusted_edge")) <= 0.0:
        return "raw_edge_does_not_survive_beta_adjustment"
    if net_edge_after_penalty > 0.0:
        return "edge_survives_beta_and_penalty_review_research_only"
    return "edge_positive_before_full_penalty_but_weak_after_penalty"


def _triage_commentary(
    *,
    period_status: str,
    drawdown_status: str,
    cost_status: str,
) -> str:
    if drawdown_status == "DRAWDOWN_RISK_TOO_HIGH":
        return "drawdown_episode_blocks_component_review"
    if period_status == "REGIME_CONCENTRATED":
        return "period_split_is_regime_concentrated"
    if cost_status in {"GROWTH_COST_SENSITIVE", "GROWTH_TURNOVER_TOO_HIGH"}:
        return "cost_or_turnover_requires_more_research"
    return "period_drawdown_cost_checks_do_not_block_research_summary"


def _drawdown_turnover_acceptable(candidate: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    readiness = _mapping(_mapping(config.get("research_policy")).get("readiness_gate"))
    limits = _mapping(_mapping(config.get("research_policy")).get("candidate_limits"))
    return _float(candidate.get("max_drawdown")) >= _float(
        readiness.get("max_drawdown_floor")
    ) and _float(candidate.get("switches_per_year")) <= _float(
        limits.get("max_switches_per_year")
    )


def _complexity_penalty(candidate: Mapping[str, Any], config: Mapping[str, Any]) -> float:
    penalties = _mapping(_mapping(config.get("research_policy")).get("complexity_penalties"))
    family = str(candidate.get("candidate_family") or candidate.get("candidate_type") or "")
    return _float(penalties.get(family), _float(penalties.get("other_growth")))


def _edge_complexity_weight(config: Mapping[str, Any]) -> float:
    policy = _mapping(_mapping(config.get("research_policy")).get("edge_significance"))
    return _float(policy.get("complexity_penalty_weight"))


def _beta_edge_net_minimum(config: Mapping[str, Any]) -> float:
    policy = _mapping(_mapping(config.get("research_policy")).get("beta_adjusted_edge_contract"))
    return _float(policy.get("net_edge_minimum"))


def _minimum_next_tasks(component_ready_review_allowed: bool) -> list[str]:
    if component_ready_review_allowed:
        return [
            "owner_manual_component_ready_review",
            "definition_hash_lock_review",
            "independent_data_source_validation_before_any_next_stage",
        ]
    return [
        "keep_growth_research_only",
        "review_beta_adjusted_edge_and_triage_blockers",
        "continue_equal_risk_forward_aging_observation",
    ]


def _roadmap_next_task(status: str) -> str:
    if status == "GROWTH_COMPONENT_REVIEWABLE":
        return "owner_component_ready_review_without_paper_shadow_or_production"
    if status == "NO_GROWTH_EDGE_FOUND":
        return "continue_equal_risk_forward_aging_and_decide_whether_to_pause_growth"
    if status == "ROADMAP_V2_BLOCKED":
        return "resolve_blocked_source_artifact_before_interpretation"
    return "continue_controlled_growth_research_with_research_only_boundary"


def _payload(
    *,
    report_type: str,
    title: str,
    status: str,
    summary: Mapping[str, Any],
    **extra: Any,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        **AI_REGIME_SUMMARY,
        "summary": {
            **AI_REGIME_SUMMARY,
            **dict(summary),
        },
        **SAFETY_BOUNDARY,
        **extra,
    }


def _write_pair(payload: dict[str, Any], output_root: Path, artifact_id: str) -> None:
    payload["artifact_paths"] = {
        "json_path": str(output_root / f"{artifact_id}.json"),
        "markdown_path": str(output_root / f"{artifact_id}.md"),
    }
    write_foundation_artifact_pair(payload, output_root=output_root, artifact_id=artifact_id)


def _write_owner_doc(payload: Mapping[str, Any], path: Path, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    answers = _mapping(payload.get("required_answers"))
    lines = [
        f"# {title}",
        "",
        f"- status: `{payload.get('status')}`",
        f"- owner_recommendation: `{payload.get('owner_recommendation', 'N/A')}`",
        "- paper_shadow_allowed: `false`",
        "- production_allowed: `false`",
        "- broker_action: `none`",
        "- manual_review_required: `true`",
        "",
        "## Required Answers",
        "",
        "|Question|Answer|",
        "|---|---|",
    ]
    for key, value in answers.items():
        lines.append(f"|`{key}`|`{value}`|")
    lines.extend(
        [
            "",
            "## Source Statuses",
            "",
            "|Source|Status|",
            "|---|---|",
        ]
    )
    for key, value in _mapping(payload.get("source_statuses")).items():
        lines.append(f"|`{key}`|`{value}`|")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _report_registry_entry(
    report_id: str,
    title: str,
    command: str,
    artifact_prefix: str,
    *,
    extra_artifact_globs: list[str] | None = None,
) -> dict[str, Any]:
    artifact_globs = [f"{artifact_prefix}.json", f"{artifact_prefix}.md"]
    artifact_globs.extend(extra_artifact_globs or [])
    return {
        "report_id": report_id,
        "title": title,
        "group": "research",
        "cadence": "ad_hoc",
        "audience": "project_owner",
        "owner": "research_governance",
        "command": command,
        "artifact_globs": artifact_globs,
        "artifact_selection_policy": "latest_available",
        "freshness_sla_days": 30,
        "freshness_rationale": (
            "Roadmap v2 real-result convergence artifacts summarize real research-only "
            "CLI outputs and preserve no paper-shadow, no production, and no broker action."
        ),
        "owner_action": "review_research_only_real_result_convergence",
        "include_in_reader_brief": False,
        "include_in_daily_task_dashboard": False,
        "required_for_daily_reading": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _safety_summary() -> dict[str, Any]:
    return {
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "manual_review_required": True,
    }


def _blocked_status(status: str) -> bool:
    return "BLOCKED" in status or status == "FAIL"


def _source_artifact_blocked_status(status: str) -> bool:
    return status in {
        "CONTROLLED_GROWTH_V2_BLOCKED",
        "BETA_ADJUSTED_EDGE_BLOCKED",
        "GROWTH_TRIAGE_BLOCKED",
        "GROWTH_COMPONENT_BLOCKED",
    } or status == "FAIL"


def _warning_status(status: str) -> bool:
    warning_tokens = (
        "WARN",
        "INSUFFICIENT",
        "INCONCLUSIVE",
        "PARTIAL",
        "NO_",
        "KEEP_RESEARCH_ONLY",
        "EDGE_WEAK",
        "REGIME_CONCENTRATED",
        "COST_SENSITIVE",
    )
    return any(token in status for token in warning_tokens)


def _first_present(values: Any) -> Any:
    for value in values:
        if value:
            return value
    return None


def _dedupe_text(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value)
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _text_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed


def _int(value: object, default: int = 0) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed


def _round(value: float) -> float:
    return round(float(value), 6)


def _read_json_or_empty(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return dict(loaded) if isinstance(loaded, Mapping) else {}
