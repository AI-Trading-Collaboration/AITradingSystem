from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from statistics import median, pstdev
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.post_2085_research_common import (
    ANCHOR_DATE,
    ANCHOR_EVENT,
    DEFAULT_BACKTEST_START,
    MARKET_REGIME,
    clean_for_yaml,
    mapping,
    records,
    round_float,
    to_float,
    write_csv_rows,
    write_json,
    write_markdown,
)

TASK_ID = "TRADING-2327_EXPOSURE_CAP_VS_NO_CAP_DIAGNOSTICS_REVIEW"
REPORT_TYPE = "exposure_cap_vs_no_cap_diagnostics_review"
ARTIFACT_ROLE = "exposure_cap_diagnostics_review"
MODE = "diagnostics_review"
STATUS = "EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED"
DATA_QUALITY_BLOCKED_STATUS = "DATA_QUALITY_BLOCKED"
DATA_VALIDATION_POLICY = "NOT_APPLICABLE_PRIOR_VALIDATED_DRY_RUN_ARTIFACTS_ONLY"
SELECTED_BASELINE = "static_etf_allocation_baseline"

DEFAULT_DRY_RUN_ROOT = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "source_bound_exposure_cap_dry_run_static_etf_baseline"
)
DEFAULT_SOURCE_BINDING_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_simulation_source_binding"
)
DEFAULT_BASELINE_DECISION_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "portfolio_baseline_source_decision"
)
DEFAULT_SIMULATION_POLICY_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "exposure_cap_mechanics_simulation"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "diagnostics_only": True,
    "dry_run_only": True,
    "manual_review_only": True,
    "promotion_allowed": False,
    "paper_shadow_allowed": False,
    "production_allowed": False,
    "broker_action": "none",
    "production_effect": "none",
    "portfolio_effect": "none",
    "real_portfolio_effect": "none",
    "target_weight_generated": False,
    "rebalance_instruction_generated": False,
    "broker_order_generated": False,
    "paper_shadow_order_generated": False,
    "production_decision_generated": False,
}

# TRADING-2327 pilot diagnostics labels. These are not promotion or policy
# thresholds; the requirements doc records the exit condition for calibration.
CAP_BINDING_LOW_THRESHOLD = 0.05
CAP_BINDING_MODERATE_THRESHOLD = 0.20
CAP_BINDING_HIGH_THRESHOLD = 0.40
EXPOSURE_REDUCTION_MODEST_THRESHOLD = 0.01
EXPOSURE_REDUCTION_MATERIAL_THRESHOLD = 0.05
EXPOSURE_REDUCTION_TOO_AGGRESSIVE_THRESHOLD = 0.25
RETURN_MATERIALITY_THRESHOLD = 0.01
DRAWDOWN_MATERIALITY_THRESHOLD = 0.01
TURNOVER_HIGH_THRESHOLD = 1.0
FALSE_COST_MODERATE_THRESHOLD = 0.05
FALSE_COST_HIGH_THRESHOLD = 0.25


class ExposureCapDiagnosticsReviewError(ValueError):
    pass


def run_exposure_cap_vs_no_cap_diagnostics_review(
    *,
    dry_run_dir: Path = DEFAULT_DRY_RUN_ROOT,
    source_binding_dir: Path = DEFAULT_SOURCE_BINDING_ROOT,
    baseline_decision_dir: Path = DEFAULT_BASELINE_DECISION_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise ExposureCapDiagnosticsReviewError(
            f"exposure-cap diagnostics review only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_exposure_cap_diagnostics_review_inputs(
        dry_run_dir=dry_run_dir,
        source_binding_dir=source_binding_dir,
        baseline_decision_dir=baseline_decision_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    dry_run_rows = records(inputs["dry_run"]["dry_run_result"].get("rows"))
    binding_rows = records(inputs["dry_run"]["binding_day_matrix"].get("rows"))
    comparison = mapping(inputs["dry_run"]["comparison"])
    turnover_report = mapping(inputs["dry_run"]["turnover_report"])
    cooldown_report = mapping(inputs["dry_run"]["cooldown_report"])
    false_cost_report = mapping(inputs["dry_run"]["false_cost_report"])
    missed_upside_report = mapping(inputs["dry_run"]["missed_upside_report"])
    downside_report = mapping(inputs["dry_run"]["downside_report"])
    data_quality_report = mapping(inputs["dry_run"]["data_quality_report"])
    data_quality_status = str(data_quality_report.get("data_quality_status", "FAIL"))

    cap_binding = build_cap_binding_diagnostics(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        comparison=comparison,
        data_quality_status=data_quality_status,
    )
    exposure_reduction = build_exposure_reduction_diagnostics(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        data_quality_status=data_quality_status,
    )
    return_drawdown = build_return_drawdown_proxy_diagnostics(
        comparison=comparison,
        data_quality_status=data_quality_status,
    )
    turnover_cooldown = build_turnover_cooldown_diagnostics(
        turnover_report=turnover_report,
        cooldown_report=cooldown_report,
        data_quality_status=data_quality_status,
    )
    false_cost_missed_upside = build_false_cost_missed_upside_diagnostics(
        false_cost_report=false_cost_report,
        missed_upside_report=missed_upside_report,
        data_quality_status=data_quality_status,
    )
    downside = build_downside_protection_diagnostics(
        downside_report=downside_report,
        data_quality_status=data_quality_status,
    )
    period_attribution_rows = build_cap_binding_period_attribution(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        data_quality_status=data_quality_status,
    )
    policy_sensitivity = build_policy_sensitivity_recommendation_matrix(
        simulation_policy=inputs["simulation_policy"],
        cap_binding=cap_binding,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost_missed_upside,
        downside=downside,
        data_quality_status=data_quality_status,
    )
    dynamic_baseline = build_dynamic_baseline_readiness_recommendation(
        cap_binding=cap_binding,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost_missed_upside,
        downside=downside,
        data_quality_status=data_quality_status,
    )
    decision = build_exposure_cap_diagnostics_decision_matrix(
        cap_binding=cap_binding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost_missed_upside,
        downside=downside,
        data_quality_status=data_quality_status,
    )
    task_route = build_exposure_cap_2328_task_route(decision)
    boundary = build_diagnostics_interpretation_boundary(
        generated_at=generated_at,
        data_quality_status=data_quality_status,
    )
    summary = build_exposure_cap_diagnostics_review_summary(
        generated_at=generated_at,
        dry_run_dir=dry_run_dir,
        source_binding_dir=source_binding_dir,
        baseline_decision_dir=baseline_decision_dir,
        simulation_policy_dir=simulation_policy_dir,
        cap_binding=cap_binding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost_missed_upside,
        downside=downside,
        period_attribution_rows=period_attribution_rows,
        policy_sensitivity=policy_sensitivity,
        dynamic_baseline=dynamic_baseline,
        decision=decision,
        task_route=task_route,
        data_quality_status=data_quality_status,
    )
    paths = write_exposure_cap_diagnostics_review_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        cap_binding=cap_binding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost_missed_upside,
        downside=downside,
        period_attribution_rows=period_attribution_rows,
        policy_sensitivity=policy_sensitivity,
        dynamic_baseline=dynamic_baseline,
        decision=decision,
        task_route=task_route,
        boundary=boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_exposure_cap_diagnostics_review_inputs(
    *,
    dry_run_dir: Path,
    source_binding_dir: Path,
    baseline_decision_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    dry_run = load_trading_2326_dry_run_outputs(dry_run_dir)
    baseline_decision = load_trading_2325_baseline_decision(baseline_decision_dir)
    source_binding = load_trading_2324_source_binding_context(source_binding_dir)
    simulation_policy = load_trading_2323_simulation_policy_context(simulation_policy_dir)
    return {
        "dry_run": dry_run,
        "baseline_decision": baseline_decision,
        "source_binding": source_binding,
        "simulation_policy": simulation_policy,
    }


def load_trading_2326_dry_run_outputs(dry_run_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": dry_run_dir / "source_bound_static_etf_dry_run_summary.json",
        "dry_run_result": dry_run_dir
        / "source_bound_static_etf_exposure_cap_dry_run_result.json",
        "comparison": dry_run_dir / "exposure_cap_vs_no_cap_static_etf_comparison.json",
        "binding_day_matrix": dry_run_dir / "exposure_cap_binding_day_matrix.json",
        "turnover_report": dry_run_dir / "exposure_cap_turnover_impact_report.json",
        "cooldown_report": dry_run_dir / "exposure_cap_cooldown_impact_report.json",
        "false_cost_report": dry_run_dir / "exposure_cap_false_risk_cap_cost_report.json",
        "missed_upside_report": dry_run_dir / "exposure_cap_missed_upside_cost_report.json",
        "downside_report": dry_run_dir / "exposure_cap_downside_protection_proxy_report.json",
        "data_quality_report": dry_run_dir / "exposure_cap_data_quality_report.json",
        "interpretation_boundary": dry_run_dir
        / "exposure_cap_simulation_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2326 static ETF dry-run")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2326 {key}", payload)
    _validate_2326_boundary(mapping(payloads["interpretation_boundary"]))
    if not records(payloads["dry_run_result"].get("rows")):
        raise ExposureCapDiagnosticsReviewError("TRADING-2326 dry-run rows missing")
    if not records(payloads["binding_day_matrix"].get("rows")):
        raise ExposureCapDiagnosticsReviewError("TRADING-2326 binding rows missing")
    if "data_quality_status" not in mapping(payloads["data_quality_report"]):
        raise ExposureCapDiagnosticsReviewError(
            "TRADING-2326 data quality report missing data_quality_status"
        )
    return {"source_dir": str(dry_run_dir), **payloads}


def load_trading_2325_baseline_decision(baseline_decision_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": baseline_decision_dir
        / "portfolio_baseline_source_decision_summary.json",
        "recommended_baseline": baseline_decision_dir
        / "recommended_exposure_cap_simulation_baseline.json",
        "task_route": baseline_decision_dir / "exposure_cap_2326_task_route.json",
        "safety_boundary": baseline_decision_dir
        / "portfolio_baseline_source_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2325 baseline decision")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2325 {key}", payload)
    selected = str(mapping(payloads["recommended_baseline"]).get("selected_for_2326"))
    if selected != SELECTED_BASELINE:
        raise ExposureCapDiagnosticsReviewError(
            f"TRADING-2327 requires {SELECTED_BASELINE}; got {selected}"
        )
    if mapping(payloads["summary"]).get("simulation_executed") is not False:
        raise ExposureCapDiagnosticsReviewError(
            "TRADING-2325 baseline decision must not have executed simulation"
        )
    return {"source_dir": str(baseline_decision_dir), **payloads}


def load_trading_2324_source_binding_context(source_binding_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": source_binding_dir / "exposure_cap_source_binding_summary.json",
        "source_inventory": source_binding_dir / "exposure_cap_source_inventory.json",
        "source_gap_matrix": source_binding_dir / "exposure_cap_source_gap_matrix.json",
        "risk_cap_trigger_binding": source_binding_dir
        / "risk_cap_trigger_series_binding_report.json",
        "market_data_binding": source_binding_dir / "market_data_binding_report.json",
        "portfolio_baseline_binding": source_binding_dir
        / "portfolio_baseline_binding_report.json",
        "turnover_rebalance_assumption": source_binding_dir
        / "turnover_rebalance_assumption_report.json",
        "dry_run_readiness": source_binding_dir
        / "source_bound_dry_run_simulation_readiness.json",
        "safety_boundary": source_binding_dir / "source_bound_dry_run_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2324 source binding")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2324 {key}", payload)
    return {"source_dir": str(source_binding_dir), **payloads}


def load_trading_2323_simulation_policy_context(
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": simulation_policy_dir / "exposure_cap_mechanics_simulation_summary.json",
        "readiness": simulation_policy_dir / "exposure_cap_simulation_readiness_matrix.json",
        "metric_contract": simulation_policy_dir / "exposure_cap_simulation_metric_contract.json",
        "safety_boundary": simulation_policy_dir
        / "exposure_cap_simulation_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2323 simulation policy")
    optional_policy = simulation_policy_dir / "exposure_cap_policy.json"
    if optional_policy.exists():
        payloads["policy"] = _load_json(optional_policy)
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return {"source_dir": str(simulation_policy_dir), **payloads}


def build_cap_binding_diagnostics(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    binding_rows: Sequence[Mapping[str, Any]],
    comparison: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    active_dry_rows = [
        row for row in dry_run_rows if row.get("simulated_cap_binding_active") is True
    ]
    active_binding_rows = [
        row for row in binding_rows if row.get("cap_binding_active_any_asset") is True
    ]
    cap_binding_days = int(
        to_float(comparison.get("cap_binding_days"), len(active_binding_rows))
    )
    record_count = int(to_float(comparison.get("record_count"), len(dry_run_rows)))
    cap_binding_rate = to_float(comparison.get("cap_binding_rate"))
    if cap_binding_rate == 0.0 and binding_rows:
        cap_binding_rate = cap_binding_days / len(binding_rows)
    cluster_lengths = _cluster_lengths(active_binding_rows, binding_rows)
    intensity_values = [
        to_float(row.get("risk_cap_intensity_average")) for row in active_binding_rows
    ]
    max_intensity = _max_intensity(
        str(row.get("risk_cap_intensity_max", "none")) for row in active_binding_rows
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.cap_binding_diagnostics.v1",
        "task_id": TASK_ID,
        "record_count": record_count,
        "simulation_start": comparison.get("simulation_start", ""),
        "simulation_end": comparison.get("simulation_end", ""),
        "cap_binding_days": cap_binding_days,
        "cap_binding_rate": round_float(cap_binding_rate),
        "cap_binding_assets": sorted(
            {
                str(asset)
                for row in active_binding_rows
                for asset in row.get("cap_binding_assets", [])
                if str(asset)
            }
        ),
        "cap_binding_asset_distribution": dict(
            sorted(Counter(str(row.get("asset")) for row in active_dry_rows).items())
        ),
        "cap_binding_horizon_distribution": _cap_binding_horizon_distribution(
            active_dry_rows
        ),
        "average_risk_cap_intensity_on_binding_days": round_float(
            sum(intensity_values) / len(intensity_values) if intensity_values else 0.0
        ),
        "max_risk_cap_intensity_on_binding_days": max_intensity,
        "max_risk_cap_intensity_rank_on_binding_days": _intensity_rank(max_intensity),
        "cap_binding_cluster_count": len(cluster_lengths),
        "average_cap_binding_cluster_length": round_float(
            sum(cluster_lengths) / len(cluster_lengths) if cluster_lengths else 0.0
        ),
        "max_cap_binding_cluster_length": max(cluster_lengths) if cluster_lengths else 0,
        "cap_binding_frequency_label": _cap_binding_frequency_label(cap_binding_rate),
        "cap_binding_clustering_label": _cap_binding_clustering_label(cluster_lengths),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_exposure_reduction_diagnostics(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    binding_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> dict[str, Any]:
    reductions = [to_float(row.get("simulated_exposure_delta")) for row in dry_run_rows]
    exposure_by_asset: defaultdict[str, float] = defaultdict(float)
    exposure_by_period: defaultdict[str, float] = defaultdict(float)
    stress_values: list[float] = []
    non_stress_values: list[float] = []
    binding_dates = {
        str(row.get("date"))
        for row in binding_rows
        if row.get("cap_binding_active_any_asset") is True
    }
    for row in dry_run_rows:
        value = to_float(row.get("simulated_exposure_delta"))
        exposure_by_asset[str(row.get("asset"))] += value
        period = str(row.get("date", ""))[:7] or "unknown"
        exposure_by_period[period] += value
        if row.get("risk_cap_triggered") is True or str(row.get("date")) in binding_dates:
            stress_values.append(value)
        else:
            non_stress_values.append(value)
    average = sum(reductions) / len(reductions) if reductions else 0.0
    payload = {
        "schema_version": f"{REPORT_TYPE}.exposure_reduction_diagnostics.v1",
        "task_id": TASK_ID,
        "average_exposure_reduction": round_float(average),
        "median_exposure_reduction": round_float(median(reductions) if reductions else 0.0),
        "max_exposure_reduction": round_float(max(reductions) if reductions else 0.0),
        "total_exposure_reduction": round_float(sum(reductions)),
        "exposure_reduction_by_asset": _round_mapping(exposure_by_asset),
        "exposure_reduction_by_period": _round_mapping(exposure_by_period),
        "exposure_reduction_on_stress_windows": round_float(sum(stress_values)),
        "exposure_reduction_on_non_stress_windows": round_float(sum(non_stress_values)),
        "average_exposure_reduction_on_stress_windows": round_float(
            sum(stress_values) / len(stress_values) if stress_values else 0.0
        ),
        "average_exposure_reduction_on_non_stress_windows": round_float(
            sum(non_stress_values) / len(non_stress_values) if non_stress_values else 0.0
        ),
        "exposure_reduction_label": _exposure_reduction_label(average),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_return_drawdown_proxy_diagnostics(
    *,
    comparison: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    return_delta = to_float(comparison.get("return_proxy_delta"))
    drawdown_delta = to_float(comparison.get("drawdown_proxy_delta"))
    return_materiality = _return_materiality_label(return_delta)
    drawdown_materiality = _drawdown_materiality_label(drawdown_delta)
    payload = {
        "schema_version": f"{REPORT_TYPE}.return_drawdown_proxy_diagnostics.v1",
        "task_id": TASK_ID,
        "no_cap_return_proxy": round_float(comparison.get("no_cap_return_proxy")),
        "capped_return_proxy": round_float(comparison.get("capped_return_proxy")),
        "return_proxy_delta": round_float(return_delta),
        "no_cap_max_drawdown_proxy": round_float(
            comparison.get("no_cap_max_drawdown_proxy")
        ),
        "capped_max_drawdown_proxy": round_float(
            comparison.get("capped_max_drawdown_proxy")
        ),
        "drawdown_proxy_delta": round_float(drawdown_delta),
        "return_drawdown_tradeoff_label": _return_drawdown_tradeoff_label(
            return_materiality,
            drawdown_materiality,
        ),
        "return_proxy_materiality_label": return_materiality,
        "drawdown_proxy_materiality_label": drawdown_materiality,
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_turnover_cooldown_diagnostics(
    *,
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    turnover_total = to_float(turnover_report.get("turnover_proxy_total"))
    cooldown_delta = to_float(cooldown_report.get("cooldown_return_proxy_delta"))
    payload = {
        "schema_version": f"{REPORT_TYPE}.turnover_cooldown_diagnostics.v1",
        "task_id": TASK_ID,
        "turnover_proxy_total": round_float(turnover_total),
        "turnover_proxy_average": round_float(
            turnover_report.get(
                "turnover_proxy_average",
                turnover_report.get("average_daily_turnover_proxy"),
            )
        ),
        "turnover_spike_days": list(turnover_report.get("turnover_spike_days", [])),
        "turnover_proxy_from_cap_entry": round_float(
            turnover_report.get("turnover_proxy_from_cap_entry")
        ),
        "turnover_proxy_from_cap_exit": round_float(
            turnover_report.get("turnover_proxy_from_cap_exit")
        ),
        "turnover_proxy_from_cooldown": round_float(
            turnover_report.get("turnover_proxy_from_cooldown")
        ),
        "turnover_impact_label": turnover_report.get("turnover_impact_label", ""),
        "cooldown_trigger_count": int(
            to_float(cooldown_report.get("cooldown_trigger_count"))
        ),
        "cooldown_active_days": int(to_float(cooldown_report.get("cooldown_active_days"))),
        "average_cooldown_length": round_float(
            cooldown_report.get("average_cooldown_length")
        ),
        "cooldown_prevented_reentry_days": int(
            to_float(cooldown_report.get("cooldown_prevented_reentry_days"))
        ),
        "cooldown_return_proxy_delta": round_float(cooldown_delta),
        "cooldown_false_cost_proxy": round_float(
            cooldown_report.get("cooldown_false_cost_proxy")
        ),
        "cooldown_impact_label": cooldown_report.get("cooldown_impact_label", ""),
        "turnover_cooldown_label": _turnover_cooldown_label(
            turnover_total,
            cooldown_delta,
        ),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_false_cost_missed_upside_diagnostics(
    *,
    false_cost_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    false_cost = to_float(
        false_cost_report.get(
            "false_risk_cap_cost_proxy",
            false_cost_report.get("missed_upside_cost_proxy"),
        )
    )
    missed_upside = to_float(
        missed_upside_report.get(
            "missed_upside_cost_proxy",
            false_cost_report.get("missed_upside_cost_proxy"),
        )
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.false_cost_missed_upside_diagnostics.v1",
        "task_id": TASK_ID,
        "false_risk_cap_count": int(
            to_float(false_cost_report.get("false_risk_cap_count"))
        ),
        "false_risk_cap_days": int(to_float(false_cost_report.get("false_risk_cap_days"))),
        "false_risk_cap_cost_proxy": round_float(false_cost),
        "missed_upside_cost_proxy": round_float(missed_upside),
        "strong_upside_after_cap_count": int(
            to_float(false_cost_report.get("strong_upside_after_cap_count"))
        ),
        "mild_drawdown_after_cap_count": int(
            to_float(false_cost_report.get("mild_drawdown_after_cap_count"))
        ),
        "source_false_cost_label": false_cost_report.get(
            "false_risk_cap_cost_label",
            false_cost_report.get("false_cost_label", ""),
        ),
        "false_cost_label": _false_cost_label(false_cost),
        "missed_upside_label": _missed_upside_label(missed_upside),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_downside_protection_diagnostics(
    *,
    downside_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    protection = to_float(downside_report.get("downside_protection_proxy"))
    payload = {
        "schema_version": f"{REPORT_TYPE}.downside_protection_diagnostics.v1",
        "task_id": TASK_ID,
        "risk_cap_trigger_count": int(
            to_float(downside_report.get("risk_cap_trigger_count"))
        ),
        "post_trigger_drawdown_capture_count": int(
            to_float(downside_report.get("post_trigger_drawdown_capture_count"))
        ),
        "post_trigger_stress_capture_count": int(
            to_float(downside_report.get("post_trigger_stress_capture_count"))
        ),
        "downside_tail_capture_count": int(
            to_float(downside_report.get("downside_tail_capture_count"))
        ),
        "downside_protection_proxy": round_float(protection),
        "drawdown_reduction_proxy": round_float(
            downside_report.get("drawdown_reduction_proxy", protection)
        ),
        "stress_window_exposure_reduction": round_float(
            downside_report.get("stress_window_exposure_reduction")
        ),
        "source_downside_protection_label": downside_report.get(
            "downside_protection_label",
            "",
        ),
        "downside_protection_label": _downside_protection_label(protection),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_cap_binding_period_attribution(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    binding_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    daily_returns = _daily_return_proxy(dry_run_rows)
    dry_rows_by_date: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in dry_run_rows:
        dry_rows_by_date[str(row.get("date"))].append(row)
    clusters = _binding_clusters(binding_rows)
    rows: list[dict[str, Any]] = []
    for index, cluster in enumerate(clusters, start=1):
        dates = [str(row.get("date")) for row in cluster]
        date_set = set(dates)
        period_dry_rows = [
            row for current in dates for row in dry_rows_by_date.get(current, [])
        ]
        no_cap_returns = [daily_returns[current]["no_cap"] for current in dates]
        capped_returns = [daily_returns[current]["capped"] for current in dates]
        no_cap_drawdown = _max_drawdown(no_cap_returns)
        capped_drawdown = _max_drawdown(capped_returns)
        period_false_cost = _missed_upside_cost(period_dry_rows)
        period_downside = _downside_protection(period_dry_rows)
        period_return_delta = sum(capped_returns) - sum(no_cap_returns)
        period_drawdown_delta = capped_drawdown - no_cap_drawdown
        row = {
            "schema_version": f"{REPORT_TYPE}.cap_binding_period_attribution.v1",
            "task_id": TASK_ID,
            "period_id": f"binding_period_{index:03d}",
            "period_start": dates[0],
            "period_end": dates[-1],
            "binding_day_count": len(date_set),
            "binding_assets": sorted(
                {
                    str(asset)
                    for binding_row in cluster
                    for asset in binding_row.get("cap_binding_assets", [])
                }
            ),
            "average_risk_cap_intensity": round_float(
                sum(to_float(row.get("risk_cap_intensity_average")) for row in cluster)
                / len(cluster)
                if cluster
                else 0.0
            ),
            "period_market_return_proxy": round_float(sum(no_cap_returns)),
            "period_max_drawdown_proxy": round_float(no_cap_drawdown),
            "period_volatility_proxy": round_float(
                pstdev(no_cap_returns) if len(no_cap_returns) > 1 else 0.0
            ),
            "period_return_proxy_delta": round_float(period_return_delta),
            "period_drawdown_proxy_delta": round_float(period_drawdown_delta),
            "period_false_cost_proxy": round_float(period_false_cost),
            "period_downside_protection_proxy": round_float(period_downside),
            "period_label": _period_label(
                return_delta=period_return_delta,
                drawdown_delta=period_drawdown_delta,
                false_cost=period_false_cost,
                downside_protection=period_downside,
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
        rows.append(row)
    return clean_for_yaml(rows)


def build_policy_sensitivity_recommendation_matrix(
    *,
    simulation_policy: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    action = _recommended_policy_action(
        cap_binding=cap_binding,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost,
        downside=downside,
        data_quality_status=data_quality_status,
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.policy_sensitivity_recommendation.v1",
        "task_id": TASK_ID,
        "current_policy_summary": {
            "source_dir": simulation_policy.get("source_dir", ""),
            "source_status": mapping(simulation_policy.get("summary")).get("status", ""),
            "source_task_id": mapping(simulation_policy.get("summary")).get("task_id", ""),
            "portfolio_source_mode": SELECTED_BASELINE,
        },
        "observed_issue": _observed_policy_issue(
            cap_binding,
            return_drawdown,
            turnover_cooldown,
            false_cost,
            downside,
            data_quality_status,
        ),
        "recommended_policy_action": action,
        "candidate_policy_variants": _candidate_policy_variants(action),
        "next_task_if_needed": _policy_next_task(action, data_quality_status),
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_dynamic_baseline_readiness_recommendation(
    *,
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    status = _dynamic_baseline_status(
        cap_binding,
        return_drawdown,
        turnover_cooldown,
        false_cost,
        downside,
        data_quality_status,
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.dynamic_baseline_readiness.v1",
        "task_id": TASK_ID,
        "static_baseline_diagnostics_status": STATUS
        if data_quality_status != "FAIL"
        else DATA_QUALITY_BLOCKED_STATUS,
        "static_baseline_evidence_quality": data_quality_status,
        "dynamic_baseline_next_step_recommended": status,
        "required_dynamic_baseline_sources": [
            "PIT dynamic strategy target exposure artifact",
            "replayable target exposure history",
            "data-quality report for dynamic baseline source",
        ],
        "blocking_gaps": _dynamic_baseline_blocking_gaps(status),
        "recommended_next_task": "TRADING-2328_Dynamic_Target_Baseline_Preparation"
        if status == "DYNAMIC_BASELINE_PREPARATION_RECOMMENDED"
        else "TRADING-2328_Exposure_Cap_Policy_Refinement_Plan"
        if status == "POLICY_REFINEMENT_BEFORE_DYNAMIC_BASELINE"
        else "TRADING-2328_Static_Baseline_Data_Remediation"
        if status == "INCONCLUSIVE" and data_quality_status == "FAIL"
        else "TRADING-2328_Risk_Cap_Forward_Observe_Exposure_Cap_Joint_Evidence_Review",
        "data_quality_status": data_quality_status,
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_exposure_cap_diagnostics_decision_matrix(
    *,
    cap_binding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    overall = _overall_recommendation(
        cap_binding=cap_binding,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost,
        downside=downside,
        data_quality_status=data_quality_status,
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.diagnostics_decision_matrix.v1",
        "task_id": TASK_ID,
        "diagnostics_status": DATA_QUALITY_BLOCKED_STATUS
        if data_quality_status == "FAIL"
        else STATUS,
        "cap_binding_assessment": cap_binding.get("cap_binding_frequency_label"),
        "exposure_reduction_assessment": exposure_reduction.get(
            "exposure_reduction_label"
        ),
        "return_drawdown_tradeoff_assessment": return_drawdown.get(
            "return_drawdown_tradeoff_label"
        ),
        "turnover_cooldown_assessment": turnover_cooldown.get(
            "turnover_cooldown_label"
        ),
        "false_cost_assessment": false_cost.get("false_cost_label"),
        "downside_protection_assessment": downside.get("downside_protection_label"),
        "data_quality_status": data_quality_status,
        "overall_recommendation": overall,
        "next_task_recommendation": _route_for_overall_recommendation(overall),
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_exposure_cap_2328_task_route(
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    overall = str(decision.get("overall_recommendation", "INCONCLUSIVE"))
    next_task = _route_for_overall_recommendation(overall)
    payload = {
        "schema_version": f"{REPORT_TYPE}.task_route.v1",
        "task_id": TASK_ID,
        "next_task": next_task,
        "overall_recommendation": overall,
        "route_reason": _route_reason(overall),
        "data_quality_status": decision.get("data_quality_status", ""),
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_diagnostics_interpretation_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
) -> dict[str, Any]:
    payload = {
        "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
        "task_id": TASK_ID,
        "generated_at": generated_at.isoformat(),
        "portfolio_source_mode": SELECTED_BASELINE,
        "data_quality_status": data_quality_status,
        "forbidden_interpretations": [
            "real_account_performance",
            "real_position_advice",
            "paper_shadow_signal",
            "production_strategy",
            "broker_action",
        ],
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def build_exposure_cap_diagnostics_review_summary(
    *,
    generated_at: datetime,
    dry_run_dir: Path,
    source_binding_dir: Path,
    baseline_decision_dir: Path,
    simulation_policy_dir: Path,
    cap_binding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    period_attribution_rows: Sequence[Mapping[str, Any]],
    policy_sensitivity: Mapping[str, Any],
    dynamic_baseline: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    diagnostics_status = (
        DATA_QUALITY_BLOCKED_STATUS if data_quality_status == "FAIL" else STATUS
    )
    payload = {
        "schema_version": f"{REPORT_TYPE}.v1",
        "report_type": REPORT_TYPE,
        "artifact_role": ARTIFACT_ROLE,
        "task_id": TASK_ID,
        "title": "Exposure-Cap vs No-Cap Diagnostics Review",
        "status": diagnostics_status,
        "diagnostics_status": diagnostics_status,
        "generated_at": generated_at.isoformat(),
        "mode": MODE,
        "market_regime": MARKET_REGIME,
        "selected_market_regime": MARKET_REGIME,
        "anchor_event": ANCHOR_EVENT,
        "anchor_date": ANCHOR_DATE,
        "default_backtest_start": DEFAULT_BACKTEST_START,
        "dry_run_dir": str(dry_run_dir),
        "source_binding_dir": str(source_binding_dir),
        "baseline_decision_dir": str(baseline_decision_dir),
        "simulation_policy_dir": str(simulation_policy_dir),
        "selected_baseline": SELECTED_BASELINE,
        "portfolio_source_mode": SELECTED_BASELINE,
        "data_quality_status": data_quality_status,
        "data_validation_policy": DATA_VALIDATION_POLICY,
        "data_quality_gate_required": False,
        "data_quality_gate_executed": False,
        "aits_validate_data_executed": False,
        "prior_data_quality_report_present": True,
        "exposure_cap_diagnostics_review_cli": True,
        "cap_binding_diagnostics_generated": True,
        "exposure_reduction_diagnostics_generated": True,
        "return_drawdown_proxy_diagnostics_generated": True,
        "turnover_cooldown_diagnostics_generated": True,
        "false_cost_missed_upside_diagnostics_generated": True,
        "downside_protection_diagnostics_generated": True,
        "cap_binding_period_attribution_generated": True,
        "policy_sensitivity_recommendation_generated": True,
        "dynamic_baseline_readiness_recommendation_generated": True,
        "diagnostics_decision_matrix_generated": True,
        "2328_task_route_generated": True,
        "record_count": cap_binding.get("record_count", 0),
        "simulation_start": cap_binding.get("simulation_start", ""),
        "simulation_end": cap_binding.get("simulation_end", ""),
        "cap_binding_days": cap_binding.get("cap_binding_days", 0),
        "cap_binding_rate": cap_binding.get("cap_binding_rate", 0.0),
        "cap_binding_frequency_label": cap_binding.get(
            "cap_binding_frequency_label",
            "",
        ),
        "exposure_reduction_label": exposure_reduction.get(
            "exposure_reduction_label",
            "",
        ),
        "return_proxy_delta": return_drawdown.get("return_proxy_delta", 0.0),
        "drawdown_proxy_delta": return_drawdown.get("drawdown_proxy_delta", 0.0),
        "return_drawdown_tradeoff_label": return_drawdown.get(
            "return_drawdown_tradeoff_label",
            "",
        ),
        "turnover_cooldown_label": turnover_cooldown.get("turnover_cooldown_label", ""),
        "false_cost_label": false_cost.get("false_cost_label", ""),
        "missed_upside_label": false_cost.get("missed_upside_label", ""),
        "downside_protection_label": downside.get("downside_protection_label", ""),
        "cap_binding_period_count": len(period_attribution_rows),
        "recommended_policy_action": policy_sensitivity.get(
            "recommended_policy_action",
            "",
        ),
        "dynamic_baseline_next_step_recommended": dynamic_baseline.get(
            "dynamic_baseline_next_step_recommended",
            "",
        ),
        "overall_recommendation": decision.get("overall_recommendation", ""),
        "next_task": task_route.get("next_task", ""),
        **SAFETY_FIELDS,
    }
    return clean_for_yaml(payload)


def write_exposure_cap_diagnostics_review_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    period_attribution_rows: Sequence[Mapping[str, Any]],
    policy_sensitivity: Mapping[str, Any],
    dynamic_baseline: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
    boundary: Mapping[str, Any],
) -> dict[str, str]:
    outputs = [
        summary,
        cap_binding,
        exposure_reduction,
        return_drawdown,
        turnover_cooldown,
        false_cost,
        downside,
        *period_attribution_rows,
        policy_sensitivity,
        dynamic_baseline,
        decision,
        task_route,
        boundary,
    ]
    for index, payload in enumerate(outputs):
        _validate_no_unsafe_fields(f"TRADING-2327 output {index}", payload)
    paths = {
        "summary": output_dir / "exposure_cap_diagnostics_review_summary.json",
        "cap_binding_json": output_dir / "cap_binding_diagnostics_matrix.json",
        "cap_binding_csv": output_dir / "cap_binding_diagnostics_matrix.csv",
        "exposure_reduction_json": output_dir
        / "exposure_reduction_diagnostics_matrix.json",
        "exposure_reduction_csv": output_dir
        / "exposure_reduction_diagnostics_matrix.csv",
        "return_drawdown_json": output_dir / "return_drawdown_proxy_diagnostics.json",
        "return_drawdown_csv": output_dir / "return_drawdown_proxy_diagnostics.csv",
        "turnover_cooldown_json": output_dir / "turnover_cooldown_diagnostics.json",
        "turnover_cooldown_csv": output_dir / "turnover_cooldown_diagnostics.csv",
        "false_cost_json": output_dir / "false_cost_missed_upside_diagnostics.json",
        "false_cost_csv": output_dir / "false_cost_missed_upside_diagnostics.csv",
        "downside_json": output_dir / "downside_protection_diagnostics.json",
        "downside_csv": output_dir / "downside_protection_diagnostics.csv",
        "period_attribution_json": output_dir / "cap_binding_period_attribution.json",
        "period_attribution_csv": output_dir / "cap_binding_period_attribution.csv",
        "policy_sensitivity": output_dir
        / "policy_sensitivity_recommendation_matrix.json",
        "dynamic_baseline": output_dir
        / "dynamic_baseline_readiness_recommendation.json",
        "decision": output_dir / "exposure_cap_diagnostics_decision_matrix.json",
        "task_route": output_dir / "exposure_cap_2328_task_route.json",
        "boundary": output_dir / "diagnostics_interpretation_boundary.json",
        "main_doc": docs_root / "exposure_cap_vs_no_cap_diagnostics_review.md",
        "binding_doc": docs_root / "exposure_cap_binding_diagnostics.md",
        "return_drawdown_doc": docs_root
        / "exposure_cap_return_drawdown_proxy_review.md",
        "turnover_cooldown_doc": docs_root
        / "exposure_cap_turnover_cooldown_review.md",
        "false_downside_doc": docs_root
        / "exposure_cap_false_cost_downside_protection_review.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["cap_binding_json"], dict(cap_binding))
    write_csv_rows(paths["cap_binding_csv"], [cap_binding])
    write_json(paths["exposure_reduction_json"], dict(exposure_reduction))
    write_csv_rows(paths["exposure_reduction_csv"], [exposure_reduction])
    write_json(paths["return_drawdown_json"], dict(return_drawdown))
    write_csv_rows(paths["return_drawdown_csv"], [return_drawdown])
    write_json(paths["turnover_cooldown_json"], dict(turnover_cooldown))
    write_csv_rows(paths["turnover_cooldown_csv"], [turnover_cooldown])
    write_json(paths["false_cost_json"], dict(false_cost))
    write_csv_rows(paths["false_cost_csv"], [false_cost])
    write_json(paths["downside_json"], dict(downside))
    write_csv_rows(paths["downside_csv"], [downside])
    write_json(
        paths["period_attribution_json"],
        {**dict(summary), "rows": list(period_attribution_rows)},
    )
    write_csv_rows(paths["period_attribution_csv"], period_attribution_rows)
    write_json(paths["policy_sensitivity"], dict(policy_sensitivity))
    write_json(paths["dynamic_baseline"], dict(dynamic_baseline))
    write_json(paths["decision"], dict(decision))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["boundary"], dict(boundary))
    write_markdown(
        paths["main_doc"],
        _render_main_doc(
            summary,
            cap_binding,
            exposure_reduction,
            return_drawdown,
            turnover_cooldown,
            false_cost,
            downside,
            decision,
            task_route,
        ),
    )
    write_markdown(paths["binding_doc"], _render_binding_doc(cap_binding))
    write_markdown(
        paths["return_drawdown_doc"],
        _render_return_drawdown_doc(return_drawdown),
    )
    write_markdown(
        paths["turnover_cooldown_doc"],
        _render_turnover_cooldown_doc(turnover_cooldown),
    )
    write_markdown(
        paths["false_downside_doc"],
        _render_false_downside_doc(false_cost, downside),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_main_doc(
    summary: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap vs No-Cap Diagnostics Review",
            "",
            "TRADING-2327 只读取 TRADING-2326 已验证的 static ETF baseline dry-run "
            "artifacts，不重新执行 simulation，也不重新消费 cached market data。所有结论"
            "都是 research proxy diagnostics，不得解释为真实账户表现、真实仓位建议、"
            "paper-shadow signal、production strategy 或 broker action。",
            "",
            f"- status: `{summary['status']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_validation_policy: `{summary['data_validation_policy']}`",
            f"- simulation_start: `{summary['simulation_start']}`",
            f"- simulation_end: `{summary['simulation_end']}`",
            f"- cap_binding_days: `{cap_binding['cap_binding_days']}`",
            f"- cap_binding_rate: `{cap_binding['cap_binding_rate']}`",
            f"- cap_binding_frequency_label: "
            f"`{cap_binding['cap_binding_frequency_label']}`",
            f"- exposure_reduction_label: "
            f"`{exposure_reduction['exposure_reduction_label']}`",
            f"- return_proxy_delta: `{return_drawdown['return_proxy_delta']}`",
            f"- drawdown_proxy_delta: `{return_drawdown['drawdown_proxy_delta']}`",
            f"- return_drawdown_tradeoff_label: "
            f"`{return_drawdown['return_drawdown_tradeoff_label']}`",
            f"- turnover_cooldown_label: "
            f"`{turnover_cooldown['turnover_cooldown_label']}`",
            f"- false_cost_label: `{false_cost['false_cost_label']}`",
            f"- missed_upside_label: `{false_cost['missed_upside_label']}`",
            f"- downside_protection_label: "
            f"`{downside['downside_protection_label']}`",
            f"- overall_recommendation: `{decision['overall_recommendation']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## 解释边界",
            "",
            "本报告不能用于真实减仓、真实调仓、paper-shadow、production 或 broker "
            "action。若后续进入 dynamic baseline 或 policy refinement，需要单独的 source "
            "binding、data-quality gate、replayability audit 和 owner review。",
            "",
        ]
    )


def _render_binding_doc(cap_binding: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Binding Diagnostics",
            "",
            "本报告只解释 static ETF baseline dry-run 中 cap binding 的频率、资产分布和"
            "聚集性。label 为 TRADING-2327 pilot diagnostics，不是长期 policy threshold。",
            "",
            f"- cap_binding_days: `{cap_binding['cap_binding_days']}`",
            f"- cap_binding_rate: `{cap_binding['cap_binding_rate']}`",
            f"- cap_binding_assets: `{', '.join(cap_binding['cap_binding_assets'])}`",
            f"- cap_binding_cluster_count: `{cap_binding['cap_binding_cluster_count']}`",
            f"- average_cap_binding_cluster_length: "
            f"`{cap_binding['average_cap_binding_cluster_length']}`",
            f"- max_cap_binding_cluster_length: "
            f"`{cap_binding['max_cap_binding_cluster_length']}`",
            f"- cap_binding_frequency_label: "
            f"`{cap_binding['cap_binding_frequency_label']}`",
            f"- cap_binding_clustering_label: "
            f"`{cap_binding['cap_binding_clustering_label']}`",
            "",
        ]
    )


def _render_return_drawdown_doc(return_drawdown: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Return / Drawdown Proxy Review",
            "",
            "本报告比较 static ETF baseline dry-run 下 no-cap 与 capped path 的 return / "
            "drawdown proxy。risk-cap 不是收益增强器；收益损失和回撤改善只能作为后续"
            "研究取舍输入。",
            "",
            f"- no_cap_return_proxy: `{return_drawdown['no_cap_return_proxy']}`",
            f"- capped_return_proxy: `{return_drawdown['capped_return_proxy']}`",
            f"- return_proxy_delta: `{return_drawdown['return_proxy_delta']}`",
            f"- no_cap_max_drawdown_proxy: "
            f"`{return_drawdown['no_cap_max_drawdown_proxy']}`",
            f"- capped_max_drawdown_proxy: "
            f"`{return_drawdown['capped_max_drawdown_proxy']}`",
            f"- drawdown_proxy_delta: `{return_drawdown['drawdown_proxy_delta']}`",
            f"- return_drawdown_tradeoff_label: "
            f"`{return_drawdown['return_drawdown_tradeoff_label']}`",
            "",
        ]
    )


def _render_turnover_cooldown_doc(turnover_cooldown: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Exposure-Cap Turnover / Cooldown Review",
            "",
            "本报告只披露 simulated exposure delta 的 turnover proxy 和 cooldown proxy，"
            "不代表真实交易成本、真实换手或执行建议。",
            "",
            f"- turnover_proxy_total: `{turnover_cooldown['turnover_proxy_total']}`",
            f"- turnover_proxy_average: `{turnover_cooldown['turnover_proxy_average']}`",
            f"- turnover_proxy_from_cap_entry: "
            f"`{turnover_cooldown['turnover_proxy_from_cap_entry']}`",
            f"- turnover_proxy_from_cap_exit: "
            f"`{turnover_cooldown['turnover_proxy_from_cap_exit']}`",
            f"- cooldown_trigger_count: `{turnover_cooldown['cooldown_trigger_count']}`",
            f"- cooldown_active_days: `{turnover_cooldown['cooldown_active_days']}`",
            f"- cooldown_return_proxy_delta: "
            f"`{turnover_cooldown['cooldown_return_proxy_delta']}`",
            f"- cooldown_false_cost_proxy: "
            f"`{turnover_cooldown['cooldown_false_cost_proxy']}`",
            f"- turnover_cooldown_label: "
            f"`{turnover_cooldown['turnover_cooldown_label']}`",
            "",
        ]
    )


def _render_false_downside_doc(
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Exposure-Cap False Cost and Downside Protection Review",
            "",
            "本报告汇总 false risk-cap / missed upside 和 downside protection proxy。"
            "这些数值不能解释为真实机会成本或真实保护效果。",
            "",
            f"- false_risk_cap_count: `{false_cost['false_risk_cap_count']}`",
            f"- false_risk_cap_days: `{false_cost['false_risk_cap_days']}`",
            f"- false_risk_cap_cost_proxy: "
            f"`{false_cost['false_risk_cap_cost_proxy']}`",
            f"- missed_upside_cost_proxy: `{false_cost['missed_upside_cost_proxy']}`",
            f"- false_cost_label: `{false_cost['false_cost_label']}`",
            f"- missed_upside_label: `{false_cost['missed_upside_label']}`",
            f"- downside_protection_proxy: "
            f"`{downside['downside_protection_proxy']}`",
            f"- drawdown_reduction_proxy: `{downside['drawdown_reduction_proxy']}`",
            f"- downside_protection_label: "
            f"`{downside['downside_protection_label']}`",
            "",
        ]
    )


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise ExposureCapDiagnosticsReviewError(
            f"{label} required artifacts missing: " + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ExposureCapDiagnosticsReviewError(f"JSON must be object: {path}")
    return payload


def _validate_2326_boundary(boundary: Mapping[str, Any]) -> None:
    if boundary.get("research_only") is not True:
        raise ExposureCapDiagnosticsReviewError(
            "TRADING-2326 interpretation boundary must be research_only"
        )
    if boundary.get("dry_run_only") is not True:
        raise ExposureCapDiagnosticsReviewError(
            "TRADING-2326 interpretation boundary must be dry_run_only"
        )


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    banned_values = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
    }
    banned_keys = {
        "target_weight",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
    }
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise ExposureCapDiagnosticsReviewError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise ExposureCapDiagnosticsReviewError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise ExposureCapDiagnosticsReviewError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise ExposureCapDiagnosticsReviewError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
        ):
            if item.get(forbidden) is True:
                raise ExposureCapDiagnosticsReviewError(f"{name} opens {forbidden}")
        for key in banned_keys:
            if key in item:
                raise ExposureCapDiagnosticsReviewError(f"{name} emits banned key {key}")
        for value in item.values():
            if isinstance(value, str) and value in banned_values:
                raise ExposureCapDiagnosticsReviewError(
                    f"{name} emits banned value {value}"
                )


def _walk_mappings(payload: Any) -> list[Mapping[str, Any]]:
    found: list[Mapping[str, Any]] = []
    if isinstance(payload, Mapping):
        found.append(payload)
        for value in payload.values():
            found.extend(_walk_mappings(value))
    elif isinstance(payload, list | tuple):
        for value in payload:
            found.extend(_walk_mappings(value))
    return found


def _cluster_lengths(
    active_binding_rows: Sequence[Mapping[str, Any]],
    binding_rows: Sequence[Mapping[str, Any]],
) -> list[int]:
    if not active_binding_rows:
        return []
    return [len(cluster) for cluster in _binding_clusters(binding_rows)]


def _binding_clusters(
    binding_rows: Sequence[Mapping[str, Any]],
) -> list[list[Mapping[str, Any]]]:
    clusters: list[list[Mapping[str, Any]]] = []
    current_cluster: list[Mapping[str, Any]] = []
    for row in sorted(binding_rows, key=lambda item: str(item.get("date"))):
        if row.get("cap_binding_active_any_asset") is True:
            current_cluster.append(row)
        elif current_cluster:
            clusters.append(current_cluster)
            current_cluster = []
    if current_cluster:
        clusters.append(current_cluster)
    return clusters


def _cap_binding_frequency_label(cap_binding_rate: float) -> str:
    if cap_binding_rate < 0:
        return "BINDING_FREQUENCY_INCONCLUSIVE"
    if cap_binding_rate < CAP_BINDING_LOW_THRESHOLD:
        return "LOW_BINDING_FREQUENCY"
    if cap_binding_rate < CAP_BINDING_MODERATE_THRESHOLD:
        return "MODERATE_BINDING_FREQUENCY"
    if cap_binding_rate < CAP_BINDING_HIGH_THRESHOLD:
        return "HIGH_BINDING_FREQUENCY"
    return "EXCESSIVE_BINDING_FREQUENCY"


def _cap_binding_clustering_label(cluster_lengths: Sequence[int]) -> str:
    if not cluster_lengths:
        return "BINDING_CLUSTERING_INCONCLUSIVE"
    average = sum(cluster_lengths) / len(cluster_lengths)
    if average < 2:
        return "DISPERSED_BINDING_CLUSTERING"
    if average < 5:
        return "MODERATE_BINDING_CLUSTERING"
    return "CONCENTRATED_BINDING_CLUSTERING"


def _cap_binding_horizon_distribution(
    active_dry_rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    values = Counter(
        str(
            row.get(
                "risk_cap_horizon",
                row.get("horizon", row.get("source_horizon", "not_provided")),
            )
        )
        for row in active_dry_rows
    )
    return dict(sorted(values.items()))


def _exposure_reduction_label(average_reduction: float) -> str:
    if average_reduction < 0:
        return "EXPOSURE_REDUCTION_INCONCLUSIVE"
    if average_reduction < EXPOSURE_REDUCTION_MODEST_THRESHOLD:
        return "EXPOSURE_REDUCTION_TOO_WEAK"
    if average_reduction < EXPOSURE_REDUCTION_MATERIAL_THRESHOLD:
        return "EXPOSURE_REDUCTION_MODEST"
    if average_reduction < EXPOSURE_REDUCTION_TOO_AGGRESSIVE_THRESHOLD:
        return "EXPOSURE_REDUCTION_MATERIAL"
    return "EXPOSURE_REDUCTION_TOO_AGGRESSIVE"


def _return_materiality_label(return_delta: float) -> str:
    if return_delta > RETURN_MATERIALITY_THRESHOLD:
        return "RETURN_PROXY_IMPROVED"
    if return_delta < -RETURN_MATERIALITY_THRESHOLD:
        return "RETURN_PROXY_COSTLY"
    return "RETURN_PROXY_NO_MATERIAL_DIFFERENCE"


def _drawdown_materiality_label(drawdown_delta: float) -> str:
    if drawdown_delta > DRAWDOWN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_PROXY_IMPROVED"
    if drawdown_delta < -DRAWDOWN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_PROXY_WORSENED"
    return "DRAWDOWN_PROXY_NO_MATERIAL_DIFFERENCE"


def _return_drawdown_tradeoff_label(
    return_materiality: str,
    drawdown_materiality: str,
) -> str:
    if (
        return_materiality == "RETURN_PROXY_NO_MATERIAL_DIFFERENCE"
        and drawdown_materiality == "DRAWDOWN_PROXY_NO_MATERIAL_DIFFERENCE"
    ):
        return "NO_MATERIAL_DIFFERENCE"
    if drawdown_materiality == "DRAWDOWN_PROXY_IMPROVED":
        if return_materiality == "RETURN_PROXY_COSTLY":
            return "DRAWDOWN_IMPROVED_RETURN_COSTLY"
        return "DRAWDOWN_IMPROVED_RETURN_ACCEPTABLE"
    if return_materiality == "RETURN_PROXY_COSTLY":
        return "DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY"
    if return_materiality == "RETURN_PROXY_IMPROVED":
        return "RETURN_IMPROVED_WITH_DRAWDOWN_ACCEPTABLE"
    return "INCONCLUSIVE"


def _turnover_cooldown_label(turnover_total: float, cooldown_delta: float) -> str:
    high_turnover = turnover_total > TURNOVER_HIGH_THRESHOLD
    if high_turnover and cooldown_delta > 0:
        return "TURNOVER_HIGH_BUT_COOLDOWN_HELPFUL"
    if high_turnover and cooldown_delta < 0:
        return "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
    if not high_turnover and cooldown_delta == 0:
        return "COOLDOWN_NEUTRAL"
    if not high_turnover:
        return "TURNOVER_COOLDOWN_ACCEPTABLE"
    return "TURNOVER_COOLDOWN_INCONCLUSIVE"


def _false_cost_label(value: float) -> str:
    if value == 0:
        return "FALSE_COST_ACCEPTABLE"
    if value < FALSE_COST_MODERATE_THRESHOLD:
        return "FALSE_COST_MODERATE"
    if value < FALSE_COST_HIGH_THRESHOLD:
        return "FALSE_COST_HIGH"
    return "FALSE_COST_BLOCKING"


def _missed_upside_label(value: float) -> str:
    if value == 0:
        return "MISSED_UPSIDE_ACCEPTABLE"
    if value < FALSE_COST_MODERATE_THRESHOLD:
        return "MISSED_UPSIDE_MODERATE"
    if value < FALSE_COST_HIGH_THRESHOLD:
        return "MISSED_UPSIDE_HIGH"
    return "MISSED_UPSIDE_BLOCKING"


def _downside_protection_label(value: float) -> str:
    if value > 0:
        return "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
    if value < 0:
        return "DOWNSIDE_PROTECTION_NEGATIVE_PROXY"
    return "DOWNSIDE_PROTECTION_INCONCLUSIVE"


def _period_label(
    *,
    return_delta: float,
    drawdown_delta: float,
    false_cost: float,
    downside_protection: float,
) -> str:
    if downside_protection > false_cost and drawdown_delta > 0:
        return "HELPFUL_STRESS_PERIOD"
    if false_cost > downside_protection and return_delta < 0:
        return "COSTLY_REBOUND_PERIOD"
    if (
        return_delta == 0
        and drawdown_delta == 0
        and false_cost == 0
        and downside_protection == 0
    ):
        return "NO_MATERIAL_EFFECT_PERIOD"
    if false_cost or downside_protection:
        return "MIXED_PERIOD"
    return "INCONCLUSIVE_PERIOD"


def _recommended_policy_action(
    *,
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "STOP_POLICY_REFINEMENT"
    if _should_reject(return_drawdown, false_cost, downside):
        return "STOP_POLICY_REFINEMENT"
    if _should_prepare_dynamic_baseline(
        cap_binding,
        return_drawdown,
        turnover_cooldown,
        false_cost,
        downside,
    ):
        return "MOVE_TO_DYNAMIC_BASELINE_BEFORE_POLICY_CHANGE"
    if false_cost.get("false_cost_label") in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}:
        return "RELAX_CAP_THRESHOLD"
    if turnover_cooldown.get("turnover_cooldown_label") == (
        "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
    ):
        return "SHORTEN_COOLDOWN"
    return "KEEP_CURRENT_POLICY_FOR_NEXT_REVIEW"


def _observed_policy_issue(
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "static_baseline_data_quality_blocked"
    issues: list[str] = []
    if cap_binding.get("cap_binding_frequency_label") == "EXCESSIVE_BINDING_FREQUENCY":
        issues.append("excessive_cap_binding_frequency")
    if return_drawdown.get("return_proxy_materiality_label") == "RETURN_PROXY_COSTLY":
        issues.append("costly_return_proxy_delta")
    if turnover_cooldown.get("turnover_cooldown_label") == (
        "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
    ):
        issues.append("turnover_high_and_cooldown_costly")
    if false_cost.get("false_cost_label") == "FALSE_COST_BLOCKING":
        issues.append("false_cost_blocking")
    if downside.get("downside_protection_label") == "DOWNSIDE_PROTECTION_POSITIVE_PROXY":
        issues.append("downside_protection_positive_proxy")
    return ",".join(issues) if issues else "no_material_policy_issue_detected"


def _candidate_policy_variants(action: str) -> list[str]:
    variants = {
        "KEEP_CURRENT_POLICY_FOR_NEXT_REVIEW": ["current_policy_observe_only"],
        "RELAX_CAP_THRESHOLD": ["higher_cap_threshold", "high_intensity_only_cap"],
        "SHORTEN_COOLDOWN": ["shorter_cooldown_days", "cooldown_exit_on_intensity_decay"],
        "MOVE_TO_DYNAMIC_BASELINE_BEFORE_POLICY_CHANGE": [
            "dynamic_target_baseline_replay",
            "static_vs_dynamic_baseline_comparison",
        ],
        "STOP_POLICY_REFINEMENT": ["archive_current_mechanics"],
    }
    return variants.get(action, ["manual_review_only_variant"])


def _policy_next_task(action: str, data_quality_status: str) -> str:
    if data_quality_status == "FAIL":
        return "TRADING-2328_Static_Baseline_Data_Remediation"
    if action == "MOVE_TO_DYNAMIC_BASELINE_BEFORE_POLICY_CHANGE":
        return "TRADING-2328_Dynamic_Target_Baseline_Preparation"
    if action in {"RELAX_CAP_THRESHOLD", "SHORTEN_COOLDOWN"}:
        return "TRADING-2328_Exposure_Cap_Policy_Refinement_Plan"
    if action == "STOP_POLICY_REFINEMENT":
        return "TRADING-2328_Archive_Current_Exposure_Cap_Mechanics"
    return "TRADING-2328_Risk_Cap_Forward_Observe_Exposure_Cap_Joint_Evidence_Review"


def _dynamic_baseline_status(
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "INCONCLUSIVE"
    if _should_reject(return_drawdown, false_cost, downside):
        return "STOP_BEFORE_DYNAMIC_BASELINE"
    if _should_prepare_dynamic_baseline(
        cap_binding,
        return_drawdown,
        turnover_cooldown,
        false_cost,
        downside,
    ):
        return "DYNAMIC_BASELINE_PREPARATION_RECOMMENDED"
    if false_cost.get("false_cost_label") in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}:
        return "POLICY_REFINEMENT_BEFORE_DYNAMIC_BASELINE"
    return "STATIC_BASELINE_DIAGNOSTICS_FIRST"


def _dynamic_baseline_blocking_gaps(status: str) -> list[str]:
    if status == "DYNAMIC_BASELINE_PREPARATION_RECOMMENDED":
        return [
            "dynamic target exposure artifact not yet bound",
            "dynamic baseline replayability audit not yet complete",
        ]
    if status == "STATIC_BASELINE_DIAGNOSTICS_FIRST":
        return ["static baseline diagnostics should be reviewed before dynamic baseline"]
    if status == "POLICY_REFINEMENT_BEFORE_DYNAMIC_BASELINE":
        return ["policy refinement evidence required before dynamic baseline route"]
    if status == "STOP_BEFORE_DYNAMIC_BASELINE":
        return ["current mechanics not supported by static baseline diagnostics"]
    return ["data quality or diagnostics evidence inconclusive"]


def _overall_recommendation(
    *,
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "DATA_QUALITY_REMEDIATION_REQUIRED"
    if _should_reject(return_drawdown, false_cost, downside):
        return "REJECT_CURRENT_EXPOSURE_CAP_MECHANICS"
    if _should_prepare_dynamic_baseline(
        cap_binding,
        return_drawdown,
        turnover_cooldown,
        false_cost,
        downside,
    ):
        return "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION"
    if _needs_policy_refinement(turnover_cooldown, false_cost):
        return "REFINE_EXPOSURE_CAP_POLICY"
    if (
        downside.get("downside_protection_label")
        == "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
    ):
        return "CONTINUE_EXPOSURE_CAP_RESEARCH"
    return "INCONCLUSIVE"


def _should_reject(
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
) -> bool:
    return (
        return_drawdown.get("return_drawdown_tradeoff_label")
        == "DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY"
        and downside.get("downside_protection_label")
        != "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
        and false_cost.get("false_cost_label") == "FALSE_COST_BLOCKING"
    )


def _should_prepare_dynamic_baseline(
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
) -> bool:
    return (
        cap_binding.get("cap_binding_frequency_label") == "EXCESSIVE_BINDING_FREQUENCY"
        and return_drawdown.get("return_drawdown_tradeoff_label")
        == "DRAWDOWN_IMPROVED_RETURN_COSTLY"
        and downside.get("downside_protection_label")
        == "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
        and (
            false_cost.get("false_cost_label") == "FALSE_COST_BLOCKING"
            or turnover_cooldown.get("turnover_cooldown_label")
            == "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
        )
    )


def _needs_policy_refinement(
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
) -> bool:
    return (
        turnover_cooldown.get("turnover_cooldown_label")
        == "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
        or false_cost.get("false_cost_label") in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}
    )


def _route_for_overall_recommendation(overall: str) -> str:
    routes = {
        "CONTINUE_EXPOSURE_CAP_RESEARCH": (
            "TRADING-2328_Risk_Cap_Forward_Observe_Exposure_Cap_Joint_Evidence_Review"
        ),
        "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION": (
            "TRADING-2328_Dynamic_Target_Baseline_Preparation"
        ),
        "REFINE_EXPOSURE_CAP_POLICY": "TRADING-2328_Exposure_Cap_Policy_Refinement_Plan",
        "DATA_QUALITY_REMEDIATION_REQUIRED": (
            "TRADING-2328_Static_Baseline_Data_Remediation"
        ),
        "REJECT_CURRENT_EXPOSURE_CAP_MECHANICS": (
            "TRADING-2328_Archive_Current_Exposure_Cap_Mechanics"
        ),
        "STATIC_BASELINE_DIAGNOSTICS_ONLY": (
            "TRADING-2328_Risk_Cap_Forward_Observe_Exposure_Cap_Joint_Evidence_Review"
        ),
    }
    return routes.get(overall, "TRADING-2328_Exposure_Cap_Policy_Refinement_Plan")


def _route_reason(overall: str) -> str:
    reasons = {
        "CONTINUE_EXPOSURE_CAP_RESEARCH": "diagnostics support continued research review",
        "MOVE_TO_DYNAMIC_TARGET_BASELINE_PREPARATION": (
            "static baseline diagnostics are useful but cap binding / false cost "
            "suggest dynamic baseline should be prepared before policy changes"
        ),
        "REFINE_EXPOSURE_CAP_POLICY": "diagnostics indicate policy side effects need review",
        "DATA_QUALITY_REMEDIATION_REQUIRED": "source data quality blocked diagnostics",
        "REJECT_CURRENT_EXPOSURE_CAP_MECHANICS": (
            "return cost is not offset by downside protection"
        ),
    }
    return reasons.get(overall, "diagnostics inconclusive; manual policy review required")


def _daily_return_proxy(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    daily: dict[str, dict[str, float]] = defaultdict(lambda: {"no_cap": 0.0, "capped": 0.0})
    for row in dry_run_rows:
        day = str(row.get("date"))
        daily[day]["no_cap"] += to_float(row.get("no_cap_return_contribution_proxy"))
        daily[day]["capped"] += to_float(row.get("capped_return_contribution_proxy"))
    return daily


def _max_drawdown(returns: Sequence[float]) -> float:
    value = 1.0
    peak = 1.0
    drawdown = 0.0
    for item in returns:
        value *= 1.0 + to_float(item)
        peak = max(peak, value)
        if peak > 0:
            drawdown = min(drawdown, value / peak - 1.0)
    return drawdown


def _missed_upside_cost(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("no_cap_return_contribution_proxy"))
            - to_float(row.get("capped_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    )


def _downside_protection(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("capped_return_contribution_proxy"))
            - to_float(row.get("no_cap_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) < 0.0
    )


def _max_intensity(values: Sequence[str]) -> str:
    return max((value.lower() for value in values if value), key=_intensity_rank, default="none")


def _intensity_rank(value: str) -> int:
    return {"none": 0, "low": 1, "medium": 2, "high": 3}.get(value.lower(), 0)


def _round_mapping(values: Mapping[str, float]) -> dict[str, float]:
    return {key: round_float(value) for key, value in sorted(values.items())}
