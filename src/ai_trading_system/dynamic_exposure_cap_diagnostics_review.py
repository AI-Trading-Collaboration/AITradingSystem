from __future__ import annotations

import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from statistics import pstdev
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_target_baseline_dry_run_readiness import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_READINESS_ROOT,
)
from ai_trading_system.dynamic_target_baseline_timestamp_remediation import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
)
from ai_trading_system.dynamic_target_exposure_cap_dry_run import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DYNAMIC_DRY_RUN_ROOT,
)
from ai_trading_system.exposure_cap_diagnostics_review import (
    DEFAULT_DRY_RUN_ROOT as DEFAULT_STATIC_DRY_RUN_ROOT,
)
from ai_trading_system.exposure_cap_diagnostics_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_STATIC_DIAGNOSTICS_ROOT,
)
from ai_trading_system.exposure_cap_diagnostics_review import (
    DEFAULT_SIMULATION_POLICY_ROOT,
)
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

TASK_ID = "TRADING-2333_DYNAMIC_EXPOSURE_CAP_VS_NO_CAP_DIAGNOSTICS_REVIEW"
REPORT_TYPE = "dynamic_exposure_cap_vs_no_cap_diagnostics_review"
ARTIFACT_ROLE = "dynamic_exposure_cap_diagnostics_review"
MODE = "dynamic_diagnostics_review"
STATUS = "DYNAMIC_EXPOSURE_CAP_DIAGNOSTICS_REVIEW_READY_PROMOTION_BLOCKED"
DATA_QUALITY_BLOCKED_STATUS = "DATA_QUALITY_BLOCKED"
DATA_VALIDATION_POLICY = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_DYNAMIC_DRY_RUN_ARTIFACTS_ONLY"
)
PORTFOLIO_SOURCE_MODE = "dynamic_target_baseline_wrapper_with_pit_caveat"
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"

NEXT_POLICY_REFINEMENT_TASK = (
    "TRADING-2334_Dynamic_Exposure_Cap_Policy_Refinement_Plan"
)
NEXT_HIGH_INTENSITY_TASK = (
    "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan"
)
NEXT_MANUAL_REVIEW_TASK = "TRADING-2334_Manual_Review_Only_Risk_Cap_Plan"
NEXT_ARCHIVE_TASK = "TRADING-2334_Archive_Current_Exposure_Cap_Mechanics"
NEXT_DATA_TASK = "TRADING-2334_Dynamic_Target_Baseline_Data_Remediation"
NEXT_CONTINUE_TASK = "TRADING-2334_Continue_Research_Without_Policy_Change"

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

# TRADING-2333 pilot review labels. These constants are diagnostics-only and
# are documented in the requirements file; they are not production policy.
CAP_BINDING_LOW_THRESHOLD = 0.05
CAP_BINDING_MODERATE_THRESHOLD = 0.20
CAP_BINDING_HIGH_THRESHOLD = 0.40
RETURN_MATERIALITY_THRESHOLD = 0.01
RETURN_HIGH_COST_THRESHOLD = 0.05
DRAWDOWN_MATERIALITY_THRESHOLD = 0.01
DRAWDOWN_STRONG_IMPROVEMENT_THRESHOLD = 0.05
FALSE_COST_MODERATE_THRESHOLD = 0.05
FALSE_COST_HIGH_THRESHOLD = 0.25
TURNOVER_HIGH_THRESHOLD = 1.0
REDUNDANT_BINDING_HIGH_THRESHOLD = 0.50


class DynamicExposureCapDiagnosticsReviewError(ValueError):
    pass


def run_dynamic_exposure_cap_vs_no_cap_diagnostics_review(
    *,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    static_diagnostics_dir: Path = DEFAULT_STATIC_DIAGNOSTICS_ROOT,
    static_dry_run_dir: Path = DEFAULT_STATIC_DRY_RUN_ROOT,
    readiness_dir: Path = DEFAULT_READINESS_ROOT,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise DynamicExposureCapDiagnosticsReviewError(
            f"dynamic exposure-cap diagnostics review only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_dynamic_exposure_cap_diagnostics_review_inputs(
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        static_diagnostics_dir=static_diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    dynamic = inputs["dynamic_dry_run"]
    dry_run_rows = records(dynamic["dry_run_result"].get("rows"))
    binding_rows = records(dynamic["binding_day_matrix"].get("rows"))
    comparison = mapping(dynamic["comparison"])
    data_quality = mapping(dynamic["data_quality_report"])
    data_quality_status = str(data_quality.get("data_quality_status", "FAIL"))

    cap_binding = build_dynamic_cap_binding_diagnostics(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        comparison=comparison,
        data_quality_status=data_quality_status,
    )
    exposure_reduction = build_dynamic_exposure_reduction_diagnostics(
        exposure_report=mapping(dynamic["exposure_reduction_report"]),
        data_quality_status=data_quality_status,
    )
    return_drawdown = build_dynamic_return_drawdown_tradeoff_diagnostics(
        comparison=comparison,
        return_drawdown_report=mapping(dynamic["return_drawdown_report"]),
        data_quality_status=data_quality_status,
    )
    false_cost = build_dynamic_false_cost_missed_upside_diagnostics(
        false_cost_report=mapping(dynamic["false_cost_report"]),
        missed_upside_report=mapping(dynamic["missed_upside_report"]),
        data_quality_status=data_quality_status,
    )
    downside = build_dynamic_downside_protection_diagnostics(
        downside_report=mapping(dynamic["downside_report"]),
        data_quality_status=data_quality_status,
    )
    turnover_cooldown = build_dynamic_turnover_cooldown_diagnostics(
        turnover_report=mapping(dynamic["turnover_report"]),
        cooldown_report=mapping(dynamic["cooldown_report"]),
        data_quality_status=data_quality_status,
    )
    strategy_overlap = build_dynamic_strategy_overlap_diagnostics(
        overlap_report=mapping(dynamic["strategy_overlap_report"]),
        data_quality_status=data_quality_status,
    )
    overbinding = build_dynamic_overbinding_diagnostics(
        dry_run_rows=dry_run_rows,
        cap_binding=cap_binding,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        strategy_overlap=strategy_overlap,
        data_quality_status=data_quality_status,
    )
    static_dynamic = build_static_vs_dynamic_exposure_cap_evidence_comparison(
        static_diagnostics=inputs["static_diagnostics"],
        static_dry_run=inputs["static_dry_run"],
        dynamic_cap_binding=cap_binding,
        dynamic_return_drawdown=return_drawdown,
        dynamic_false_cost=false_cost,
        dynamic_downside=downside,
        dynamic_strategy_overlap=strategy_overlap,
        data_quality_status=data_quality_status,
    )
    period_attribution_rows = build_dynamic_cap_binding_period_attribution(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        data_quality_status=data_quality_status,
    )
    policy_sensitivity = build_dynamic_policy_sensitivity_recommendation_matrix(
        simulation_policy=inputs["simulation_policy"],
        cap_binding=cap_binding,
        overbinding=overbinding,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        strategy_overlap=strategy_overlap,
        data_quality_status=data_quality_status,
    )
    decision = build_dynamic_exposure_cap_decision_matrix(
        cap_binding=cap_binding,
        overbinding=overbinding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        turnover_cooldown=turnover_cooldown,
        false_cost=false_cost,
        downside=downside,
        strategy_overlap=strategy_overlap,
        static_dynamic=static_dynamic,
        pit_boundary=mapping(dynamic["interpretation_boundary"]),
        data_quality_status=data_quality_status,
    )
    task_route = build_dynamic_2334_task_route(decision)
    boundary = build_dynamic_exposure_cap_interpretation_boundary(
        generated_at=generated_at,
        data_quality_status=data_quality_status,
    )
    summary = build_dynamic_exposure_cap_diagnostics_review_summary(
        generated_at=generated_at,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        static_diagnostics_dir=static_diagnostics_dir,
        static_dry_run_dir=static_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        simulation_policy_dir=simulation_policy_dir,
        cap_binding=cap_binding,
        overbinding=overbinding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        turnover_cooldown=turnover_cooldown,
        strategy_overlap=strategy_overlap,
        static_dynamic=static_dynamic,
        period_attribution_rows=period_attribution_rows,
        policy_sensitivity=policy_sensitivity,
        decision=decision,
        task_route=task_route,
        data_quality_status=data_quality_status,
    )
    paths = write_dynamic_exposure_cap_diagnostics_review_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        cap_binding=cap_binding,
        overbinding=overbinding,
        exposure_reduction=exposure_reduction,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        turnover_cooldown=turnover_cooldown,
        strategy_overlap=strategy_overlap,
        static_dynamic=static_dynamic,
        period_attribution_rows=period_attribution_rows,
        policy_sensitivity=policy_sensitivity,
        decision=decision,
        task_route=task_route,
        boundary=boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_dynamic_exposure_cap_diagnostics_review_inputs(
    *,
    dynamic_dry_run_dir: Path,
    static_diagnostics_dir: Path,
    static_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    return {
        "dynamic_dry_run": load_trading_2332_dynamic_dry_run_outputs(
            dynamic_dry_run_dir
        ),
        "static_diagnostics": load_trading_2327_static_diagnostics_reference(
            static_diagnostics_dir
        ),
        "static_dry_run": load_trading_2326_static_dry_run_reference(
            static_dry_run_dir
        ),
        "readiness": load_trading_2331_readiness_context(readiness_dir),
        "timestamp_remediation": load_trading_2330_timestamp_context(
            timestamp_remediation_dir
        ),
        "simulation_policy": load_trading_2323_simulation_policy_context(
            simulation_policy_dir
        ),
    }


def load_trading_2332_dynamic_dry_run_outputs(
    dynamic_dry_run_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": dynamic_dry_run_dir
        / "dynamic_target_exposure_cap_dry_run_summary.json",
        "source_report": dynamic_dry_run_dir
        / "dynamic_target_baseline_source_report.json",
        "dry_run_result": dynamic_dry_run_dir
        / "dynamic_target_exposure_cap_dry_run_result.json",
        "comparison": dynamic_dry_run_dir
        / "dynamic_target_cap_vs_no_cap_comparison.json",
        "binding_day_matrix": dynamic_dry_run_dir
        / "dynamic_target_cap_binding_day_matrix.json",
        "exposure_reduction_report": dynamic_dry_run_dir
        / "dynamic_target_exposure_reduction_report.json",
        "return_drawdown_report": dynamic_dry_run_dir
        / "dynamic_target_return_drawdown_proxy_report.json",
        "turnover_report": dynamic_dry_run_dir
        / "dynamic_target_turnover_impact_report.json",
        "cooldown_report": dynamic_dry_run_dir
        / "dynamic_target_cooldown_impact_report.json",
        "false_cost_report": dynamic_dry_run_dir
        / "dynamic_target_false_risk_cap_cost_report.json",
        "missed_upside_report": dynamic_dry_run_dir
        / "dynamic_target_missed_upside_cost_report.json",
        "downside_report": dynamic_dry_run_dir
        / "dynamic_target_downside_protection_proxy_report.json",
        "strategy_overlap_report": dynamic_dry_run_dir
        / "dynamic_target_strategy_overlap_report.json",
        "static_dynamic_report": dynamic_dry_run_dir
        / "dynamic_target_static_vs_dynamic_comparison.json",
        "data_quality_report": dynamic_dry_run_dir
        / "dynamic_target_data_quality_report.json",
        "interpretation_boundary": dynamic_dry_run_dir
        / "dynamic_target_pit_caveat_interpretation_boundary.json",
        "task_route": dynamic_dry_run_dir / "dynamic_target_2333_task_route.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2332 dynamic dry-run")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2332 {key}", payload)
    boundary = mapping(payloads["interpretation_boundary"])
    if boundary.get("strict_pit_ready") is True:
        raise DynamicExposureCapDiagnosticsReviewError(
            "TRADING-2332 boundary must not mark strict_pit_ready=true"
        )
    if str(boundary.get("known_at_policy", "")) != KNOWN_AT_POLICY:
        raise DynamicExposureCapDiagnosticsReviewError(
            "TRADING-2333 requires NEXT_SESSION_DECISION_POLICY"
        )
    if "data_quality_status" not in mapping(payloads["data_quality_report"]):
        raise DynamicExposureCapDiagnosticsReviewError(
            "TRADING-2332 data quality report missing data_quality_status"
        )
    return {
        "source_dir": str(dynamic_dry_run_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2327_static_diagnostics_reference(
    static_diagnostics_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": static_diagnostics_dir
        / "exposure_cap_diagnostics_review_summary.json",
        "cap_binding": static_diagnostics_dir / "cap_binding_diagnostics_matrix.json",
        "exposure_reduction": static_diagnostics_dir
        / "exposure_reduction_diagnostics_matrix.json",
        "return_drawdown": static_diagnostics_dir
        / "return_drawdown_proxy_diagnostics.json",
        "turnover_cooldown": static_diagnostics_dir
        / "turnover_cooldown_diagnostics.json",
        "false_cost": static_diagnostics_dir
        / "false_cost_missed_upside_diagnostics.json",
        "downside": static_diagnostics_dir / "downside_protection_diagnostics.json",
        "decision": static_diagnostics_dir
        / "exposure_cap_diagnostics_decision_matrix.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2327 static diagnostics")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2327 {key}", payload)
    return {
        "source_dir": str(static_diagnostics_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2326_static_dry_run_reference(
    static_dry_run_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": static_dry_run_dir / "source_bound_static_etf_dry_run_summary.json",
        "comparison": static_dry_run_dir
        / "exposure_cap_vs_no_cap_static_etf_comparison.json",
        "binding_day_matrix": static_dry_run_dir / "exposure_cap_binding_day_matrix.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2326 static dry-run reference")
    optional = {
        "false_cost_report": static_dry_run_dir
        / "exposure_cap_false_risk_cap_cost_report.json",
        "downside_report": static_dry_run_dir
        / "exposure_cap_downside_protection_proxy_report.json",
    }
    for key, path in optional.items():
        if path.exists():
            payloads[key] = _load_json(path)
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2326 {key}", payload)
    return {
        "source_dir": str(static_dry_run_dir),
        "paths": {key: str(path) for key, path in {**paths, **optional}.items()},
        **payloads,
    }


def load_trading_2331_readiness_context(readiness_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": readiness_dir / "dynamic_dry_run_readiness_summary.json",
        "pit_acceptance": readiness_dir
        / "dynamic_dry_run_pit_caveat_acceptance_report.json",
        "interpretation_boundary": readiness_dir
        / "dynamic_dry_run_interpretation_boundary.json",
        "readiness_matrix": readiness_dir / "dynamic_dry_run_2332_readiness_matrix.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2331 readiness context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2331 {key}", payload)
    return {
        "source_dir": str(readiness_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2330_timestamp_context(
    timestamp_remediation_dir: Path,
) -> dict[str, Any]:
    paths = {
        "pit_caveat": timestamp_remediation_dir
        / "dynamic_target_timestamp_pit_caveat_report.json",
        "known_at": timestamp_remediation_dir
        / "dynamic_target_known_at_semantics_report.json",
        "latency_policy": timestamp_remediation_dir
        / "dynamic_target_latency_policy_report.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2330 timestamp context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2330 {key}", payload)
    return {
        "source_dir": str(timestamp_remediation_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2323_simulation_policy_context(
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    summary_path = simulation_policy_dir / "exposure_cap_mechanics_simulation_summary.json"
    readiness_path = _first_existing_path(
        [
            simulation_policy_dir / "exposure_cap_simulation_readiness.json",
            simulation_policy_dir / "exposure_cap_simulation_readiness_matrix.json",
        ]
    )
    paths = {"summary": summary_path, "readiness": readiness_path}
    payloads = _load_required_payloads(paths, "TRADING-2323 simulation policy context")
    policy_path = simulation_policy_dir / "exposure_cap_policy.json"
    if policy_path.exists():
        payloads["policy"] = _load_json(policy_path)
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return {
        "source_dir": str(simulation_policy_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def build_dynamic_cap_binding_diagnostics(
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
    cluster_lengths = _cluster_lengths(binding_rows)
    intensity_values = [
        to_float(row.get("risk_cap_intensity_average")) for row in active_binding_rows
    ]
    max_intensity = _max_intensity(
        str(row.get("risk_cap_intensity_max", "none")) for row in active_binding_rows
    )
    return clean_for_yaml(
        {
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
                sorted(Counter(_row_asset(row) for row in active_dry_rows).items())
            ),
            "cap_binding_horizon_distribution": _cap_binding_horizon_distribution(
                active_dry_rows
            ),
            "average_risk_cap_intensity_on_binding_days": round_float(
                sum(intensity_values) / len(intensity_values)
                if intensity_values
                else 0.0
            ),
            "max_risk_cap_intensity_on_binding_days": max_intensity,
            "cap_binding_cluster_count": len(cluster_lengths),
            "average_cap_binding_cluster_length": round_float(
                sum(cluster_lengths) / len(cluster_lengths) if cluster_lengths else 0.0
            ),
            "max_cap_binding_cluster_length": max(cluster_lengths)
            if cluster_lengths
            else 0,
            "cap_binding_frequency_label": _cap_binding_frequency_label(
                cap_binding_rate,
                data_quality_status,
            ),
            "cap_binding_clustering_label": _cap_binding_clustering_label(
                cluster_lengths
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_overbinding_diagnostics(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    cap_binding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    cap_rows = [
        row for row in dry_run_rows if row.get("simulated_cap_binding_active") is True
    ]
    low_risk_rows = [
        row
        for row in cap_rows
        if str(row.get("risk_cap_intensity", "none")).lower() in {"none", "low"}
    ]
    rebound_rows = [row for row in cap_rows if to_float(row.get("asset_return")) > 0.0]
    no_stress_rows = [row for row in cap_rows if to_float(row.get("asset_return")) >= 0.0]
    cap_binding_rate = to_float(cap_binding.get("cap_binding_rate"))
    incremental_rate = to_float(strategy_overlap.get("incremental_binding_rate"))
    redundant_rate = to_float(strategy_overlap.get("redundant_binding_rate"))
    return_delta = to_float(return_drawdown.get("return_proxy_delta"))
    drawdown_delta = to_float(return_drawdown.get("drawdown_proxy_delta"))
    false_cost_proxy = to_float(false_cost.get("false_risk_cap_cost_proxy"))
    downside_proxy = to_float(downside.get("downside_protection_proxy"))
    overbinding_score = (
        cap_binding_rate
        + redundant_rate
        + max(0.0, false_cost_proxy - downside_proxy)
        + max(0.0, -return_delta)
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.overbinding_diagnostics.v1",
            "task_id": TASK_ID,
            "cap_binding_rate": round_float(cap_binding_rate),
            "incremental_binding_rate": round_float(incremental_rate),
            "redundant_binding_rate": round_float(redundant_rate),
            "dynamic_strategy_already_de_risked_count": int(
                to_float(strategy_overlap.get("dynamic_strategy_derisked_count"))
            ),
            "risk_cap_incremental_binding_count": int(
                to_float(strategy_overlap.get("risk_cap_incremental_binding_count"))
            ),
            "risk_cap_redundant_binding_count": int(
                to_float(strategy_overlap.get("risk_cap_redundant_binding_count"))
            ),
            "binding_without_later_stress_count": len(no_stress_rows),
            "binding_before_rebound_count": len(rebound_rows),
            "binding_during_low_risk_window_count": len(low_risk_rows),
            "overbinding_score": round_float(overbinding_score),
            "overbinding_label": _overbinding_label(
                cap_binding_rate=cap_binding_rate,
                return_delta=return_delta,
                drawdown_delta=drawdown_delta,
                redundant_rate=redundant_rate,
                false_cost_label=str(false_cost.get("false_cost_label", "")),
                data_quality_status=data_quality_status,
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_exposure_reduction_diagnostics(
    *,
    exposure_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    incremental_count = int(to_float(exposure_report.get("incremental_binding_count")))
    non_incremental_count = int(to_float(exposure_report.get("non_incremental_binding_count")))
    incremental_rate = to_float(exposure_report.get("incremental_binding_rate"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.exposure_reduction_diagnostics.v1",
            "task_id": TASK_ID,
            "average_exposure_reduction": round_float(
                exposure_report.get("average_exposure_reduction")
            ),
            "median_exposure_reduction": round_float(
                exposure_report.get("median_exposure_reduction")
            ),
            "max_exposure_reduction": round_float(
                exposure_report.get("max_exposure_reduction")
            ),
            "total_exposure_reduction": round_float(
                exposure_report.get("total_exposure_reduction")
            ),
            "exposure_reduction_by_asset": mapping(
                exposure_report.get("exposure_reduction_by_asset")
            ),
            "exposure_reduction_by_period": mapping(
                exposure_report.get("exposure_reduction_by_period")
            ),
            "incremental_exposure_reduction_after_dynamic_derisk": round_float(
                exposure_report.get(
                    "incremental_exposure_reduction_after_dynamic_derisk"
                )
            ),
            "non_incremental_binding_count": non_incremental_count,
            "incremental_binding_count": incremental_count,
            "incremental_binding_rate": round_float(incremental_rate),
            "exposure_reduction_label": _exposure_reduction_label(
                incremental_count=incremental_count,
                non_incremental_count=non_incremental_count,
                incremental_rate=incremental_rate,
                average_reduction=to_float(
                    exposure_report.get("average_exposure_reduction")
                ),
                data_quality_status=data_quality_status,
            ),
            "source_exposure_reduction_label": exposure_report.get(
                "exposure_reduction_label",
                "",
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_return_drawdown_tradeoff_diagnostics(
    *,
    comparison: Mapping[str, Any],
    return_drawdown_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    return_delta = to_float(
        comparison.get("return_proxy_delta", return_drawdown_report.get("return_proxy_delta"))
    )
    drawdown_delta = to_float(
        comparison.get(
            "drawdown_proxy_delta",
            return_drawdown_report.get("drawdown_proxy_delta"),
        )
    )
    return_cost = _return_cost_materiality(return_delta)
    drawdown_materiality = _drawdown_improvement_materiality(drawdown_delta)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.return_drawdown_tradeoff.v1",
            "task_id": TASK_ID,
            "dynamic_no_cap_return_proxy": round_float(
                comparison.get(
                    "dynamic_no_cap_return_proxy",
                    return_drawdown_report.get("dynamic_no_cap_return_proxy"),
                )
            ),
            "dynamic_capped_return_proxy": round_float(
                comparison.get(
                    "dynamic_capped_return_proxy",
                    return_drawdown_report.get("dynamic_capped_return_proxy"),
                )
            ),
            "return_proxy_delta": round_float(return_delta),
            "dynamic_no_cap_max_drawdown_proxy": round_float(
                comparison.get(
                    "dynamic_no_cap_max_drawdown_proxy",
                    return_drawdown_report.get("dynamic_no_cap_max_drawdown_proxy"),
                )
            ),
            "dynamic_capped_max_drawdown_proxy": round_float(
                comparison.get(
                    "dynamic_capped_max_drawdown_proxy",
                    return_drawdown_report.get("dynamic_capped_max_drawdown_proxy"),
                )
            ),
            "drawdown_proxy_delta": round_float(drawdown_delta),
            "return_cost_materiality": return_cost,
            "drawdown_improvement_materiality": drawdown_materiality,
            "return_drawdown_tradeoff_label": _return_drawdown_tradeoff_label(
                return_delta=return_delta,
                drawdown_delta=drawdown_delta,
                data_quality_status=data_quality_status,
            ),
            "source_return_drawdown_tradeoff_label": return_drawdown_report.get(
                "return_drawdown_tradeoff_label",
                "",
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_false_cost_missed_upside_diagnostics(
    *,
    false_cost_report: Mapping[str, Any],
    missed_upside_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    false_cost = to_float(false_cost_report.get("false_risk_cap_cost_proxy"))
    missed_upside = to_float(missed_upside_report.get("missed_upside_cost_proxy"))
    if missed_upside == 0.0:
        missed_upside = false_cost
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.false_cost_missed_upside.v1",
            "task_id": TASK_ID,
            "false_risk_cap_count": int(
                to_float(false_cost_report.get("false_risk_cap_count"))
            ),
            "false_risk_cap_days": int(
                to_float(false_cost_report.get("false_risk_cap_days"))
            ),
            "false_risk_cap_cost_proxy": round_float(false_cost),
            "missed_upside_cost_proxy": round_float(missed_upside),
            "strong_upside_after_cap_count": int(
                to_float(false_cost_report.get("strong_upside_after_cap_count"))
            ),
            "missed_upside_after_incremental_cap_count": int(
                to_float(
                    missed_upside_report.get(
                        "missed_upside_after_incremental_cap_count"
                    )
                )
            ),
            "missed_upside_after_redundant_cap_count": int(
                to_float(
                    missed_upside_report.get("missed_upside_after_redundant_cap_count")
                )
            ),
            "false_cost_label": _false_cost_label(false_cost, data_quality_status),
            "missed_upside_label": _missed_upside_label(
                missed_upside,
                data_quality_status,
            ),
            "source_false_cost_label": false_cost_report.get(
                "false_risk_cap_cost_label",
                "",
            ),
            "source_missed_upside_label": missed_upside_report.get(
                "missed_upside_label",
                "",
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_downside_protection_diagnostics(
    *,
    downside_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    protection = to_float(downside_report.get("downside_protection_proxy"))
    incremental = to_float(
        downside_report.get("incremental_downside_protection_proxy")
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.downside_protection.v1",
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
            "incremental_downside_protection_proxy": round_float(incremental),
            "downside_protection_label": _downside_protection_label(
                protection=protection,
                incremental=incremental,
                data_quality_status=data_quality_status,
            ),
            "source_downside_protection_label": downside_report.get(
                "downside_protection_label",
                "",
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_turnover_cooldown_diagnostics(
    *,
    turnover_report: Mapping[str, Any],
    cooldown_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    turnover_total = to_float(turnover_report.get("turnover_proxy_total"))
    cooldown_delta = to_float(cooldown_report.get("cooldown_return_proxy_delta"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.turnover_cooldown.v1",
            "task_id": TASK_ID,
            "turnover_proxy_total": round_float(turnover_total),
            "turnover_proxy_average": round_float(
                turnover_report.get("turnover_proxy_average")
            ),
            "turnover_proxy_from_cap_entry": round_float(
                turnover_report.get("turnover_proxy_from_cap_entry")
            ),
            "turnover_proxy_from_cap_exit": round_float(
                turnover_report.get("turnover_proxy_from_cap_exit")
            ),
            "turnover_proxy_from_cooldown": round_float(
                turnover_report.get("turnover_proxy_from_cooldown")
            ),
            "turnover_spike_days": list(turnover_report.get("turnover_spike_days", [])),
            "cooldown_trigger_count": int(
                to_float(cooldown_report.get("cooldown_trigger_count"))
            ),
            "cooldown_active_days": int(
                to_float(cooldown_report.get("cooldown_active_days"))
            ),
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
            "turnover_cooldown_label": _turnover_cooldown_label(
                turnover_total=turnover_total,
                cooldown_delta=cooldown_delta,
                data_quality_status=data_quality_status,
            ),
            "source_turnover_impact_label": turnover_report.get(
                "turnover_impact_label",
                "",
            ),
            "source_cooldown_impact_label": cooldown_report.get(
                "cooldown_impact_label",
                "",
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_strategy_overlap_diagnostics(
    *,
    overlap_report: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    trigger_count = int(to_float(overlap_report.get("risk_cap_trigger_count")))
    incremental_count = int(
        to_float(overlap_report.get("risk_cap_incremental_binding_count"))
    )
    redundant_count = int(
        to_float(overlap_report.get("risk_cap_redundant_binding_count"))
    )
    binding_without_dynamic_count = int(
        to_float(overlap_report.get("risk_cap_binding_without_dynamic_derisk_count"))
    )
    overlap_count = int(
        to_float(overlap_report.get("risk_cap_and_dynamic_derisk_overlap_count"))
    )
    overlap_rate = overlap_count / trigger_count if trigger_count else 0.0
    incremental_rate = incremental_count / trigger_count if trigger_count else 0.0
    redundant_rate = redundant_count / trigger_count if trigger_count else 0.0
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.strategy_overlap.v1",
            "task_id": TASK_ID,
            "record_count": int(to_float(overlap_report.get("record_count"))),
            "risk_cap_trigger_count": trigger_count,
            "dynamic_strategy_derisked_count": int(
                to_float(overlap_report.get("dynamic_strategy_derisked_count"))
            ),
            "risk_cap_and_dynamic_derisk_overlap_count": overlap_count,
            "risk_cap_incremental_binding_count": incremental_count,
            "risk_cap_redundant_binding_count": redundant_count,
            "risk_cap_binding_without_dynamic_derisk_count": binding_without_dynamic_count,
            "dynamic_derisk_without_risk_cap_count": int(
                to_float(overlap_report.get("dynamic_derisk_without_risk_cap_count"))
            ),
            "overlap_rate": round_float(overlap_rate),
            "incremental_binding_rate": round_float(incremental_rate),
            "redundant_binding_rate": round_float(redundant_rate),
            "overlap_label": _strategy_overlap_label(
                trigger_count=trigger_count,
                incremental_count=incremental_count,
                redundant_count=redundant_count,
                binding_without_dynamic_count=binding_without_dynamic_count,
                data_quality_status=data_quality_status,
            ),
            "source_overlap_label": overlap_report.get("overlap_label", ""),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_static_vs_dynamic_exposure_cap_evidence_comparison(
    *,
    static_diagnostics: Mapping[str, Any],
    static_dry_run: Mapping[str, Any],
    dynamic_cap_binding: Mapping[str, Any],
    dynamic_return_drawdown: Mapping[str, Any],
    dynamic_false_cost: Mapping[str, Any],
    dynamic_downside: Mapping[str, Any],
    dynamic_strategy_overlap: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    static_cap = mapping(static_diagnostics.get("cap_binding"))
    static_return = mapping(static_diagnostics.get("return_drawdown"))
    static_false = mapping(static_diagnostics.get("false_cost"))
    static_downside = mapping(static_diagnostics.get("downside"))
    static_comparison = mapping(static_dry_run.get("comparison"))

    static_binding_rate = _first_float(
        static_cap.get("cap_binding_rate"),
        static_comparison.get("cap_binding_rate"),
    )
    static_return_delta = _first_float(
        static_return.get("return_proxy_delta"),
        static_comparison.get("return_proxy_delta"),
    )
    static_drawdown_delta = _first_float(
        static_return.get("drawdown_proxy_delta"),
        static_comparison.get("drawdown_proxy_delta"),
    )
    static_false_cost = _first_float(
        static_false.get("false_risk_cap_cost_proxy"),
        static_comparison.get("false_risk_cap_cost_proxy"),
    )
    static_protection = _first_float(
        static_downside.get("downside_protection_proxy"),
        static_comparison.get("downside_protection_proxy"),
    )
    dynamic_binding_rate = to_float(dynamic_cap_binding.get("cap_binding_rate"))
    dynamic_return_delta = to_float(dynamic_return_drawdown.get("return_proxy_delta"))
    dynamic_drawdown_delta = to_float(dynamic_return_drawdown.get("drawdown_proxy_delta"))
    dynamic_false = to_float(dynamic_false_cost.get("false_risk_cap_cost_proxy"))
    dynamic_protection = to_float(dynamic_downside.get("downside_protection_proxy"))
    false_delta = dynamic_false - static_false_cost
    protection_delta = dynamic_protection - static_protection
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.static_dynamic_comparison.v1",
            "task_id": TASK_ID,
            "static_cap_binding_rate": round_float(static_binding_rate),
            "dynamic_cap_binding_rate": round_float(dynamic_binding_rate),
            "cap_binding_rate_delta": round_float(
                dynamic_binding_rate - static_binding_rate
            ),
            "static_return_proxy_delta": round_float(static_return_delta),
            "dynamic_return_proxy_delta": round_float(dynamic_return_delta),
            "return_cost_delta": round_float(dynamic_return_delta - static_return_delta),
            "static_drawdown_proxy_delta": round_float(static_drawdown_delta),
            "dynamic_drawdown_proxy_delta": round_float(dynamic_drawdown_delta),
            "drawdown_protection_delta": round_float(protection_delta),
            "static_false_cost_proxy": round_float(static_false_cost),
            "dynamic_false_cost_proxy": round_float(dynamic_false),
            "false_cost_delta": round_float(false_delta),
            "static_downside_protection_proxy": round_float(static_protection),
            "dynamic_downside_protection_proxy": round_float(dynamic_protection),
            "comparison_label": _static_dynamic_label(
                static_binding=static_binding_rate,
                dynamic_binding=dynamic_binding_rate,
                static_return_delta=static_return_delta,
                dynamic_return_delta=dynamic_return_delta,
                static_drawdown_delta=static_drawdown_delta,
                dynamic_drawdown_delta=dynamic_drawdown_delta,
                false_delta=false_delta,
                protection_delta=protection_delta,
                overlap_label=str(dynamic_strategy_overlap.get("overlap_label", "")),
                data_quality_status=data_quality_status,
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_cap_binding_period_attribution(
    *,
    dry_run_rows: Sequence[Mapping[str, Any]],
    binding_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    daily_returns = _dynamic_daily_return_proxy(dry_run_rows)
    dry_rows_by_date: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in dry_run_rows:
        dry_rows_by_date[str(row.get("date"))].append(row)
    rows: list[dict[str, Any]] = []
    for index, cluster in enumerate(_binding_clusters(binding_rows), start=1):
        dates = [str(row.get("date")) for row in cluster]
        period_rows = [
            row for current in dates for row in dry_rows_by_date.get(current, [])
        ]
        no_cap_returns = [daily_returns[current]["no_cap"] for current in dates]
        capped_returns = [daily_returns[current]["capped"] for current in dates]
        no_cap_drawdown = _max_drawdown(no_cap_returns)
        capped_drawdown = _max_drawdown(capped_returns)
        period_return_delta = sum(capped_returns) - sum(no_cap_returns)
        period_drawdown_delta = capped_drawdown - no_cap_drawdown
        period_false_cost = _missed_upside_cost(period_rows)
        period_downside = _downside_protection(period_rows)
        rows.append(
            {
                "schema_version": f"{REPORT_TYPE}.cap_binding_period_attribution.v1",
                "task_id": TASK_ID,
                "period_id": f"dynamic_binding_period_{index:03d}",
                "period_start": dates[0],
                "period_end": dates[-1],
                "binding_day_count": len(set(dates)),
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
                "period_dynamic_return_proxy": round_float(sum(no_cap_returns)),
                "period_dynamic_capped_return_proxy": round_float(sum(capped_returns)),
                "period_dynamic_max_drawdown_proxy": round_float(no_cap_drawdown),
                "period_dynamic_capped_max_drawdown_proxy": round_float(capped_drawdown),
                "period_volatility_proxy": round_float(
                    pstdev(no_cap_returns) if len(no_cap_returns) > 1 else 0.0
                ),
                "period_return_proxy_delta": round_float(period_return_delta),
                "period_drawdown_proxy_delta": round_float(period_drawdown_delta),
                "period_false_cost_proxy": round_float(period_false_cost),
                "period_downside_protection_proxy": round_float(period_downside),
                "period_overlap_label": _period_overlap_label(period_rows),
                "period_label": _period_label(
                    return_delta=period_return_delta,
                    drawdown_delta=period_drawdown_delta,
                    false_cost=period_false_cost,
                    downside_protection=period_downside,
                    data_quality_status=data_quality_status,
                ),
                "data_quality_status": data_quality_status,
                **SAFETY_FIELDS,
            }
        )
    return clean_for_yaml(rows)


def build_dynamic_policy_sensitivity_recommendation_matrix(
    *,
    simulation_policy: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    action = _recommended_policy_action(
        cap_binding=cap_binding,
        overbinding=overbinding,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        strategy_overlap=strategy_overlap,
        data_quality_status=data_quality_status,
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.policy_sensitivity.v1",
            "task_id": TASK_ID,
            "current_policy_summary": {
                "source_dir": simulation_policy.get("source_dir", ""),
                "source_status": mapping(simulation_policy.get("summary")).get(
                    "status",
                    "",
                ),
                "source_task_id": mapping(simulation_policy.get("summary")).get(
                    "task_id",
                    "",
                ),
                "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
                "known_at_policy": KNOWN_AT_POLICY,
            },
            "observed_issue": _observed_policy_issue(
                cap_binding=cap_binding,
                overbinding=overbinding,
                return_drawdown=return_drawdown,
                false_cost=false_cost,
                downside=downside,
                strategy_overlap=strategy_overlap,
                data_quality_status=data_quality_status,
            ),
            "recommended_policy_action": action,
            "candidate_policy_variants": _candidate_policy_variants(action),
            "next_task_if_needed": _task_for_policy_action(
                action,
                data_quality_status,
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_exposure_cap_decision_matrix(
    *,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    pit_boundary: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    pit_caveat_status = _pit_caveat_status(pit_boundary)
    overall = _overall_recommendation(
        cap_binding=cap_binding,
        overbinding=overbinding,
        return_drawdown=return_drawdown,
        false_cost=false_cost,
        downside=downside,
        strategy_overlap=strategy_overlap,
        static_dynamic=static_dynamic,
        data_quality_status=data_quality_status,
    )
    diagnostics_status = (
        DATA_QUALITY_BLOCKED_STATUS if data_quality_status == "FAIL" else STATUS
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.decision_matrix.v1",
            "task_id": TASK_ID,
            "diagnostics_status": diagnostics_status,
            "data_quality_status": data_quality_status,
            "pit_caveat_status": pit_caveat_status,
            "cap_binding_assessment": cap_binding.get("cap_binding_frequency_label"),
            "overbinding_assessment": overbinding.get("overbinding_label"),
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
            "missed_upside_assessment": false_cost.get("missed_upside_label"),
            "downside_protection_assessment": downside.get(
                "downside_protection_label"
            ),
            "dynamic_strategy_overlap_assessment": strategy_overlap.get(
                "overlap_label"
            ),
            "static_vs_dynamic_assessment": static_dynamic.get("comparison_label"),
            "overall_recommendation": overall,
            "next_task_recommendation": _route_for_overall_recommendation(overall),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_2334_task_route(decision: Mapping[str, Any]) -> dict[str, Any]:
    overall = str(decision.get("overall_recommendation", "INCONCLUSIVE"))
    next_task = _route_for_overall_recommendation(overall)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.task_route.v1",
            "task_id": TASK_ID,
            "next_task": next_task,
            "overall_recommendation": overall,
            "route_reason": _route_reason(overall),
            "allowed_routes": [
                NEXT_POLICY_REFINEMENT_TASK,
                NEXT_HIGH_INTENSITY_TASK,
                NEXT_MANUAL_REVIEW_TASK,
                NEXT_ARCHIVE_TASK,
                NEXT_DATA_TASK,
                NEXT_CONTINUE_TASK,
            ],
            "data_quality_status": decision.get("data_quality_status", ""),
            **SAFETY_FIELDS,
        }
    )


def build_dynamic_exposure_cap_interpretation_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.interpretation_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "research_only": True,
            "diagnostics_only": True,
            "dry_run_only": True,
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "known_at_policy": KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "real_portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_only": True,
            "forbidden_interpretations": [
                "real_account_performance",
                "real_position_advice",
                "paper_shadow_signal",
                "production_strategy",
                "broker_action",
            ],
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "research_only",
                    "diagnostics_only",
                    "dry_run_only",
                    "manual_review_only",
                    "promotion_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "broker_action",
                    "production_effect",
                    "real_portfolio_effect",
                }
            },
        }
    )


def build_dynamic_exposure_cap_diagnostics_review_summary(
    *,
    generated_at: datetime,
    dynamic_dry_run_dir: Path,
    static_diagnostics_dir: Path,
    static_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    simulation_policy_dir: Path,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    period_attribution_rows: Sequence[Mapping[str, Any]],
    policy_sensitivity: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    diagnostics_status = (
        DATA_QUALITY_BLOCKED_STATUS if data_quality_status == "FAIL" else STATUS
    )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.v1",
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "task_id": TASK_ID,
            "title": "Dynamic Exposure-Cap vs No-Cap Diagnostics Review",
            "status": diagnostics_status,
            "diagnostics_status": diagnostics_status,
            "generated_at": generated_at.isoformat(),
            "mode": MODE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "static_diagnostics_dir": str(static_diagnostics_dir),
            "static_dry_run_dir": str(static_dry_run_dir),
            "readiness_dir": str(readiness_dir),
            "timestamp_remediation_dir": str(timestamp_remediation_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "portfolio_source_mode": PORTFOLIO_SOURCE_MODE,
            "known_at_policy": KNOWN_AT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "data_quality_status": data_quality_status,
            "data_validation_policy": DATA_VALIDATION_POLICY,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_executed": False,
            "prior_data_quality_report_present": True,
            "dynamic_exposure_cap_diagnostics_cli": True,
            "dynamic_cap_binding_diagnostics_generated": True,
            "dynamic_overbinding_diagnostics_generated": True,
            "dynamic_exposure_reduction_diagnostics_generated": True,
            "dynamic_return_drawdown_tradeoff_generated": True,
            "dynamic_false_cost_missed_upside_generated": True,
            "dynamic_downside_protection_generated": True,
            "dynamic_turnover_cooldown_generated": True,
            "dynamic_strategy_overlap_generated": True,
            "static_vs_dynamic_comparison_generated": True,
            "dynamic_cap_binding_period_attribution_generated": True,
            "dynamic_policy_sensitivity_recommendation_generated": True,
            "dynamic_decision_matrix_generated": True,
            "2334_task_route_generated": True,
            "record_count": cap_binding.get("record_count", 0),
            "simulation_start": cap_binding.get("simulation_start", ""),
            "simulation_end": cap_binding.get("simulation_end", ""),
            "cap_binding_days": cap_binding.get("cap_binding_days", 0),
            "cap_binding_rate": cap_binding.get("cap_binding_rate", 0.0),
            "cap_binding_frequency_label": cap_binding.get(
                "cap_binding_frequency_label",
                "",
            ),
            "overbinding_label": overbinding.get("overbinding_label", ""),
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
            "false_cost_label": false_cost.get("false_cost_label", ""),
            "missed_upside_label": false_cost.get("missed_upside_label", ""),
            "downside_protection_label": downside.get("downside_protection_label", ""),
            "turnover_cooldown_label": turnover_cooldown.get(
                "turnover_cooldown_label",
                "",
            ),
            "dynamic_strategy_overlap_label": strategy_overlap.get("overlap_label", ""),
            "static_vs_dynamic_comparison_label": static_dynamic.get(
                "comparison_label",
                "",
            ),
            "cap_binding_period_count": len(period_attribution_rows),
            "recommended_policy_action": policy_sensitivity.get(
                "recommended_policy_action",
                "",
            ),
            "overall_recommendation": decision.get("overall_recommendation", ""),
            "next_task": task_route.get("next_task", ""),
            **SAFETY_FIELDS,
        }
    )


def write_dynamic_exposure_cap_diagnostics_review_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    exposure_reduction: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    turnover_cooldown: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    period_attribution_rows: Sequence[Mapping[str, Any]],
    policy_sensitivity: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
    boundary: Mapping[str, Any],
) -> dict[str, str]:
    outputs = [
        summary,
        cap_binding,
        overbinding,
        exposure_reduction,
        return_drawdown,
        false_cost,
        downside,
        turnover_cooldown,
        strategy_overlap,
        static_dynamic,
        *period_attribution_rows,
        policy_sensitivity,
        decision,
        task_route,
        boundary,
    ]
    for index, payload in enumerate(outputs):
        _validate_no_unsafe_fields(f"TRADING-2333 output {index}", payload)
    paths = {
        "summary": output_dir / "dynamic_exposure_cap_diagnostics_review_summary.json",
        "cap_binding_json": output_dir / "dynamic_cap_binding_diagnostics_matrix.json",
        "cap_binding_csv": output_dir / "dynamic_cap_binding_diagnostics_matrix.csv",
        "overbinding_json": output_dir / "dynamic_overbinding_diagnostics.json",
        "overbinding_csv": output_dir / "dynamic_overbinding_diagnostics.csv",
        "exposure_reduction_json": output_dir
        / "dynamic_exposure_reduction_diagnostics.json",
        "exposure_reduction_csv": output_dir
        / "dynamic_exposure_reduction_diagnostics.csv",
        "return_drawdown_json": output_dir
        / "dynamic_return_drawdown_tradeoff_diagnostics.json",
        "return_drawdown_csv": output_dir
        / "dynamic_return_drawdown_tradeoff_diagnostics.csv",
        "false_cost_json": output_dir
        / "dynamic_false_cost_missed_upside_diagnostics.json",
        "false_cost_csv": output_dir
        / "dynamic_false_cost_missed_upside_diagnostics.csv",
        "downside_json": output_dir / "dynamic_downside_protection_diagnostics.json",
        "downside_csv": output_dir / "dynamic_downside_protection_diagnostics.csv",
        "turnover_cooldown_json": output_dir
        / "dynamic_turnover_cooldown_diagnostics.json",
        "turnover_cooldown_csv": output_dir
        / "dynamic_turnover_cooldown_diagnostics.csv",
        "strategy_overlap_json": output_dir
        / "dynamic_strategy_overlap_diagnostics.json",
        "strategy_overlap_csv": output_dir
        / "dynamic_strategy_overlap_diagnostics.csv",
        "static_dynamic_json": output_dir
        / "static_vs_dynamic_exposure_cap_evidence_comparison.json",
        "static_dynamic_csv": output_dir
        / "static_vs_dynamic_exposure_cap_evidence_comparison.csv",
        "period_attribution_json": output_dir
        / "dynamic_cap_binding_period_attribution.json",
        "period_attribution_csv": output_dir
        / "dynamic_cap_binding_period_attribution.csv",
        "policy_sensitivity": output_dir
        / "dynamic_policy_sensitivity_recommendation_matrix.json",
        "decision": output_dir / "dynamic_exposure_cap_decision_matrix.json",
        "task_route": output_dir / "dynamic_2334_task_route.json",
        "boundary": output_dir / "dynamic_exposure_cap_interpretation_boundary.json",
        "main_doc": docs_root / "dynamic_exposure_cap_vs_no_cap_diagnostics_review.md",
        "binding_doc": docs_root
        / "dynamic_cap_binding_and_overbinding_diagnostics.md",
        "return_drawdown_doc": docs_root / "dynamic_return_drawdown_tradeoff_review.md",
        "false_downside_doc": docs_root
        / "dynamic_false_cost_downside_protection_review.md",
        "overlap_doc": docs_root / "dynamic_strategy_overlap_diagnostics.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["cap_binding_json"], dict(cap_binding))
    write_csv_rows(paths["cap_binding_csv"], [cap_binding])
    write_json(paths["overbinding_json"], dict(overbinding))
    write_csv_rows(paths["overbinding_csv"], [overbinding])
    write_json(paths["exposure_reduction_json"], dict(exposure_reduction))
    write_csv_rows(paths["exposure_reduction_csv"], [exposure_reduction])
    write_json(paths["return_drawdown_json"], dict(return_drawdown))
    write_csv_rows(paths["return_drawdown_csv"], [return_drawdown])
    write_json(paths["false_cost_json"], dict(false_cost))
    write_csv_rows(paths["false_cost_csv"], [false_cost])
    write_json(paths["downside_json"], dict(downside))
    write_csv_rows(paths["downside_csv"], [downside])
    write_json(paths["turnover_cooldown_json"], dict(turnover_cooldown))
    write_csv_rows(paths["turnover_cooldown_csv"], [turnover_cooldown])
    write_json(paths["strategy_overlap_json"], dict(strategy_overlap))
    write_csv_rows(paths["strategy_overlap_csv"], [strategy_overlap])
    write_json(paths["static_dynamic_json"], dict(static_dynamic))
    write_csv_rows(paths["static_dynamic_csv"], [static_dynamic])
    write_json(
        paths["period_attribution_json"],
        {**dict(summary), "rows": list(period_attribution_rows)},
    )
    write_csv_rows(paths["period_attribution_csv"], period_attribution_rows)
    write_json(paths["policy_sensitivity"], dict(policy_sensitivity))
    write_json(paths["decision"], dict(decision))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["boundary"], dict(boundary))
    write_markdown(
        paths["main_doc"],
        _render_main_doc(
            summary=summary,
            cap_binding=cap_binding,
            overbinding=overbinding,
            return_drawdown=return_drawdown,
            false_cost=false_cost,
            downside=downside,
            strategy_overlap=strategy_overlap,
            static_dynamic=static_dynamic,
            decision=decision,
            task_route=task_route,
        ),
    )
    write_markdown(
        paths["binding_doc"],
        _render_binding_doc(cap_binding=cap_binding, overbinding=overbinding),
    )
    write_markdown(
        paths["return_drawdown_doc"],
        _render_return_drawdown_doc(return_drawdown),
    )
    write_markdown(
        paths["false_downside_doc"],
        _render_false_downside_doc(false_cost=false_cost, downside=downside),
    )
    write_markdown(paths["overlap_doc"], _render_overlap_doc(strategy_overlap))
    return {key: str(path) for key, path in paths.items()}


def _render_main_doc(
    *,
    summary: Mapping[str, Any],
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Exposure-Cap vs No-Cap Diagnostics Review",
            "",
            "TRADING-2333 只读取 TRADING-2332 已验证的 dynamic target baseline "
            "dry-run artifacts，并把 TRADING-2327 / 2326 static reference 作为对照。"
            "本任务不重新执行 simulation，不修改 policy，不生成交易指令。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_validation_policy: `{summary['data_validation_policy']}`",
            f"- known_at_policy: `{summary['known_at_policy']}`",
            f"- strict_pit_ready: `{summary['strict_pit_ready']}`",
            f"- cap_binding_rate: `{cap_binding['cap_binding_rate']}`",
            f"- cap_binding_frequency_label: "
            f"`{cap_binding['cap_binding_frequency_label']}`",
            f"- overbinding_label: `{overbinding['overbinding_label']}`",
            f"- return_proxy_delta: `{return_drawdown['return_proxy_delta']}`",
            f"- drawdown_proxy_delta: `{return_drawdown['drawdown_proxy_delta']}`",
            f"- return_drawdown_tradeoff_label: "
            f"`{return_drawdown['return_drawdown_tradeoff_label']}`",
            f"- false_cost_label: `{false_cost['false_cost_label']}`",
            f"- missed_upside_label: `{false_cost['missed_upside_label']}`",
            f"- downside_protection_label: `{downside['downside_protection_label']}`",
            f"- dynamic_strategy_overlap_label: `{strategy_overlap['overlap_label']}`",
            f"- static_vs_dynamic_comparison_label: "
            f"`{static_dynamic['comparison_label']}`",
            f"- overall_recommendation: `{decision['overall_recommendation']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## 解释边界",
            "",
            "Dynamic wrapper 使用 `NEXT_SESSION_DECISION_POLICY`，不是 strict PIT。"
            "这些 diagnostics 不能解释为真实账户表现、真实仓位建议、paper-shadow "
            "signal、production strategy 或 broker action。",
            "",
        ]
    )


def _render_binding_doc(
    *,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic Cap Binding and Overbinding Diagnostics",
            "",
            "本报告检查 dynamic target baseline 下 exposure-cap 是否过于频繁地约束 "
            "dynamic target exposure，以及这些约束是否主要是 redundant binding。",
            "",
            f"- cap_binding_days: `{cap_binding['cap_binding_days']}`",
            f"- cap_binding_rate: `{cap_binding['cap_binding_rate']}`",
            f"- cap_binding_frequency_label: "
            f"`{cap_binding['cap_binding_frequency_label']}`",
            f"- cap_binding_cluster_count: `{cap_binding['cap_binding_cluster_count']}`",
            f"- average_cap_binding_cluster_length: "
            f"`{cap_binding['average_cap_binding_cluster_length']}`",
            f"- incremental_binding_rate: `{overbinding['incremental_binding_rate']}`",
            f"- redundant_binding_rate: `{overbinding['redundant_binding_rate']}`",
            f"- binding_before_rebound_count: "
            f"`{overbinding['binding_before_rebound_count']}`",
            f"- overbinding_label: `{overbinding['overbinding_label']}`",
            "",
        ]
    )


def _render_return_drawdown_doc(return_drawdown: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Return Drawdown Tradeoff Review",
            "",
            "本报告只比较 dynamic no-cap 与 dynamic capped proxy 指标，不代表真实收益"
            "或真实回撤保护。",
            "",
            f"- dynamic_no_cap_return_proxy: "
            f"`{return_drawdown['dynamic_no_cap_return_proxy']}`",
            f"- dynamic_capped_return_proxy: "
            f"`{return_drawdown['dynamic_capped_return_proxy']}`",
            f"- return_proxy_delta: `{return_drawdown['return_proxy_delta']}`",
            f"- dynamic_no_cap_max_drawdown_proxy: "
            f"`{return_drawdown['dynamic_no_cap_max_drawdown_proxy']}`",
            f"- dynamic_capped_max_drawdown_proxy: "
            f"`{return_drawdown['dynamic_capped_max_drawdown_proxy']}`",
            f"- drawdown_proxy_delta: `{return_drawdown['drawdown_proxy_delta']}`",
            f"- return_cost_materiality: "
            f"`{return_drawdown['return_cost_materiality']}`",
            f"- drawdown_improvement_materiality: "
            f"`{return_drawdown['drawdown_improvement_materiality']}`",
            f"- return_drawdown_tradeoff_label: "
            f"`{return_drawdown['return_drawdown_tradeoff_label']}`",
            "",
        ]
    )


def _render_false_downside_doc(
    *,
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic False Cost and Downside Protection Review",
            "",
            "本报告复核 false risk-cap、missed upside 与 downside protection proxy 的"
            "相对关系，用于判断 automatic exposure cap 是否成本过高。",
            "",
            f"- false_risk_cap_count: `{false_cost['false_risk_cap_count']}`",
            f"- false_risk_cap_cost_proxy: "
            f"`{false_cost['false_risk_cap_cost_proxy']}`",
            f"- missed_upside_cost_proxy: "
            f"`{false_cost['missed_upside_cost_proxy']}`",
            f"- false_cost_label: `{false_cost['false_cost_label']}`",
            f"- missed_upside_label: `{false_cost['missed_upside_label']}`",
            f"- downside_protection_proxy: "
            f"`{downside['downside_protection_proxy']}`",
            f"- incremental_downside_protection_proxy: "
            f"`{downside['incremental_downside_protection_proxy']}`",
            f"- downside_protection_label: `{downside['downside_protection_label']}`",
            "",
        ]
    )


def _render_overlap_doc(strategy_overlap: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic Strategy Overlap Diagnostics",
            "",
            "本报告检查 risk-cap 与 dynamic strategy 自身降仓的重叠程度，用于区分"
            "边际保护和重复约束。",
            "",
            f"- record_count: `{strategy_overlap['record_count']}`",
            f"- risk_cap_trigger_count: `{strategy_overlap['risk_cap_trigger_count']}`",
            f"- dynamic_strategy_derisked_count: "
            f"`{strategy_overlap['dynamic_strategy_derisked_count']}`",
            f"- risk_cap_and_dynamic_derisk_overlap_count: "
            f"`{strategy_overlap['risk_cap_and_dynamic_derisk_overlap_count']}`",
            f"- risk_cap_incremental_binding_count: "
            f"`{strategy_overlap['risk_cap_incremental_binding_count']}`",
            f"- risk_cap_redundant_binding_count: "
            f"`{strategy_overlap['risk_cap_redundant_binding_count']}`",
            f"- overlap_rate: `{strategy_overlap['overlap_rate']}`",
            f"- incremental_binding_rate: "
            f"`{strategy_overlap['incremental_binding_rate']}`",
            f"- redundant_binding_rate: `{strategy_overlap['redundant_binding_rate']}`",
            f"- overlap_label: `{strategy_overlap['overlap_label']}`",
            "",
        ]
    )


def _load_required_payloads(paths: Mapping[str, Path], label: str) -> dict[str, Any]:
    missing = [str(path) for path in paths.values() if not path.exists()]
    if missing:
        raise DynamicExposureCapDiagnosticsReviewError(
            f"{label} required artifacts missing: " + ", ".join(missing)
        )
    return {key: _load_json(path) for key, path in paths.items()}


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicExposureCapDiagnosticsReviewError(f"JSON must be object: {path}")
    return payload


def _first_existing_path(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _validate_no_unsafe_fields(name: str, payload: Mapping[str, Any]) -> None:
    banned_values = {
        "PROMOTION_READY",
        "PAPER_SHADOW_READY",
        "PRODUCTION_READY",
        "BROKER_READY",
        "BUY_SIGNAL",
        "SELL_SIGNAL",
        "BROKER_ACTION",
    }
    banned_lower_values = {value.lower() for value in banned_values}
    banned_keys = {
        "target_weight",
        "target_weight_action",
        "rebalance_instruction",
        "buy_signal",
        "sell_signal",
    }
    for item in _walk_mappings(payload):
        if item.get("promotion_allowed") is True:
            raise DynamicExposureCapDiagnosticsReviewError(f"{name} opens promotion")
        if item.get("paper_shadow_allowed") is True:
            raise DynamicExposureCapDiagnosticsReviewError(f"{name} opens paper_shadow")
        if item.get("production_allowed") is True:
            raise DynamicExposureCapDiagnosticsReviewError(f"{name} opens production")
        if str(item.get("broker_action", "none")).lower() != "none":
            raise DynamicExposureCapDiagnosticsReviewError(f"{name} opens broker_action")
        for forbidden in (
            "target_weight_generated",
            "rebalance_instruction_generated",
            "broker_order_generated",
            "paper_shadow_order_generated",
            "production_decision_generated",
            "paper_shadow_ready",
            "production_ready",
        ):
            if item.get(forbidden) is True:
                raise DynamicExposureCapDiagnosticsReviewError(
                    f"{name} opens {forbidden}"
                )
        for key in banned_keys:
            if key in item:
                raise DynamicExposureCapDiagnosticsReviewError(
                    f"{name} emits banned key {key}"
                )
        for value in item.values():
            if isinstance(value, str) and value.lower() in banned_lower_values:
                raise DynamicExposureCapDiagnosticsReviewError(
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


def _row_asset(row: Mapping[str, Any]) -> str:
    return str(row.get("target_asset", row.get("asset", "")))


def _cap_binding_horizon_distribution(
    active_dry_rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    return dict(
        sorted(
            Counter(
                str(
                    row.get(
                        "risk_cap_horizon",
                        row.get("horizon", row.get("source_horizon", "not_provided")),
                    )
                )
                for row in active_dry_rows
            ).items()
        )
    )


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


def _cluster_lengths(binding_rows: Sequence[Mapping[str, Any]]) -> list[int]:
    return [len(cluster) for cluster in _binding_clusters(binding_rows)]


def _max_intensity(values: Sequence[str]) -> str:
    return max((value.lower() for value in values if value), key=_intensity_rank, default="none")


def _intensity_rank(value: str) -> int:
    return {"none": 0, "low": 1, "medium": 2, "high": 3}.get(value.lower(), 0)


def _cap_binding_frequency_label(
    cap_binding_rate: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL" or cap_binding_rate < 0.0:
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
    if average < 2.0:
        return "DISPERSED_BINDING_CLUSTERING"
    if average < 5.0:
        return "MODERATE_BINDING_CLUSTERING"
    return "CONCENTRATED_BINDING_CLUSTERING"


def _overbinding_label(
    *,
    cap_binding_rate: float,
    return_delta: float,
    drawdown_delta: float,
    redundant_rate: float,
    false_cost_label: str,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "OVERBINDING_INCONCLUSIVE"
    return_costly = return_delta < -RETURN_MATERIALITY_THRESHOLD
    drawdown_weak = drawdown_delta <= DRAWDOWN_STRONG_IMPROVEMENT_THRESHOLD
    if (
        cap_binding_rate >= CAP_BINDING_HIGH_THRESHOLD
        and return_costly
        and drawdown_weak
    ):
        return "OVERBINDING_BLOCKING"
    if cap_binding_rate >= CAP_BINDING_HIGH_THRESHOLD and return_costly:
        return "OVERBINDING_HIGH"
    if (
        cap_binding_rate >= CAP_BINDING_MODERATE_THRESHOLD
        or redundant_rate >= REDUNDANT_BINDING_HIGH_THRESHOLD
        or false_cost_label in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}
    ):
        return "OVERBINDING_MODERATE"
    return "OVERBINDING_LOW"


def _exposure_reduction_label(
    *,
    incremental_count: int,
    non_incremental_count: int,
    incremental_rate: float,
    average_reduction: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "EXPOSURE_REDUCTION_INCONCLUSIVE"
    if average_reduction > 0.25 and incremental_rate < 0.5:
        return "EXPOSURE_REDUCTION_TOO_AGGRESSIVE"
    if incremental_count == 0 and non_incremental_count > 0:
        return "MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"
    if incremental_rate >= 0.5 and incremental_count > 0:
        return "INCREMENTAL_EXPOSURE_REDUCTION_MATERIAL"
    if incremental_count > 0:
        return "INCREMENTAL_EXPOSURE_REDUCTION_MODEST"
    return "EXPOSURE_REDUCTION_INCONCLUSIVE"


def _return_cost_materiality(return_delta: float) -> str:
    if return_delta <= -RETURN_HIGH_COST_THRESHOLD:
        return "RETURN_COST_HIGH"
    if return_delta <= -RETURN_MATERIALITY_THRESHOLD:
        return "RETURN_COST_MATERIAL"
    if return_delta < 0.0:
        return "RETURN_COST_LOW"
    if return_delta > RETURN_MATERIALITY_THRESHOLD:
        return "RETURN_IMPROVED"
    return "RETURN_COST_NONE"


def _drawdown_improvement_materiality(drawdown_delta: float) -> str:
    if drawdown_delta >= DRAWDOWN_STRONG_IMPROVEMENT_THRESHOLD:
        return "DRAWDOWN_IMPROVEMENT_MATERIAL"
    if drawdown_delta > DRAWDOWN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_IMPROVEMENT_MODEST"
    if drawdown_delta > 0.0:
        return "DRAWDOWN_IMPROVEMENT_WEAK"
    if drawdown_delta < -DRAWDOWN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_WORSENED"
    return "DRAWDOWN_NO_MATERIAL_DIFFERENCE"


def _return_drawdown_tradeoff_label(
    *,
    return_delta: float,
    drawdown_delta: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "INCONCLUSIVE"
    if abs(return_delta) <= RETURN_MATERIALITY_THRESHOLD and abs(
        drawdown_delta
    ) <= DRAWDOWN_MATERIALITY_THRESHOLD:
        return "NO_MATERIAL_DIFFERENCE"
    if return_delta > RETURN_MATERIALITY_THRESHOLD and drawdown_delta >= 0.0:
        return "RETURN_IMPROVED_WITH_DRAWDOWN_ACCEPTABLE"
    if (
        return_delta <= -RETURN_HIGH_COST_THRESHOLD
        and 0.0 < drawdown_delta <= DRAWDOWN_STRONG_IMPROVEMENT_THRESHOLD
    ):
        return "DRAWDOWN_PROTECTION_WEAK_RETURN_COST_HIGH"
    if drawdown_delta > DRAWDOWN_MATERIALITY_THRESHOLD and return_delta < 0.0:
        return "DRAWDOWN_IMPROVED_RETURN_COSTLY"
    if drawdown_delta > DRAWDOWN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_IMPROVED_RETURN_ACCEPTABLE"
    if return_delta < -RETURN_MATERIALITY_THRESHOLD:
        return "DRAWDOWN_NOT_IMPROVED_RETURN_COSTLY"
    return "INCONCLUSIVE"


def _false_cost_label(value: float, data_quality_status: str) -> str:
    if data_quality_status == "FAIL":
        return "FALSE_COST_INCONCLUSIVE"
    if value == 0.0:
        return "FALSE_COST_ACCEPTABLE"
    if value < FALSE_COST_MODERATE_THRESHOLD:
        return "FALSE_COST_MODERATE"
    if value < FALSE_COST_HIGH_THRESHOLD:
        return "FALSE_COST_HIGH"
    return "FALSE_COST_BLOCKING"


def _missed_upside_label(value: float, data_quality_status: str) -> str:
    if data_quality_status == "FAIL":
        return "MISSED_UPSIDE_INCONCLUSIVE"
    if value == 0.0:
        return "MISSED_UPSIDE_ACCEPTABLE"
    if value < FALSE_COST_MODERATE_THRESHOLD:
        return "MISSED_UPSIDE_MODERATE"
    if value < FALSE_COST_HIGH_THRESHOLD:
        return "MISSED_UPSIDE_HIGH"
    return "MISSED_UPSIDE_BLOCKING"


def _downside_protection_label(
    *,
    protection: float,
    incremental: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "DOWNSIDE_PROTECTION_INCONCLUSIVE"
    if protection > 0.0 and incremental > 0.0:
        return "DOWNSIDE_PROTECTION_POSITIVE_PROXY"
    if protection > 0.0:
        return "DOWNSIDE_PROTECTION_WEAK_PROXY"
    if protection < 0.0:
        return "DOWNSIDE_PROTECTION_NEGATIVE_PROXY"
    return "DOWNSIDE_PROTECTION_INCONCLUSIVE"


def _turnover_cooldown_label(
    *,
    turnover_total: float,
    cooldown_delta: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "TURNOVER_COOLDOWN_INCONCLUSIVE"
    high_turnover = turnover_total > TURNOVER_HIGH_THRESHOLD
    if high_turnover and cooldown_delta > 0.0:
        return "TURNOVER_HIGH_BUT_COOLDOWN_HELPFUL"
    if high_turnover and cooldown_delta < 0.0:
        return "TURNOVER_HIGH_AND_COOLDOWN_COSTLY"
    if not high_turnover and cooldown_delta == 0.0:
        return "COOLDOWN_NEUTRAL"
    if not high_turnover:
        return "TURNOVER_COOLDOWN_ACCEPTABLE"
    return "TURNOVER_COOLDOWN_INCONCLUSIVE"


def _strategy_overlap_label(
    *,
    trigger_count: int,
    incremental_count: int,
    redundant_count: int,
    binding_without_dynamic_count: int,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL" or trigger_count == 0:
        return "OVERLAP_INCONCLUSIVE"
    if redundant_count == trigger_count and redundant_count > 0:
        return "DYNAMIC_STRATEGY_ALREADY_HANDLES_RISK"
    if binding_without_dynamic_count > redundant_count:
        return "RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK"
    if incremental_count > redundant_count:
        return "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"
    if redundant_count > incremental_count:
        return "RISK_CAP_MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"
    return "OVERLAP_INCONCLUSIVE"


def _static_dynamic_label(
    *,
    static_binding: float,
    dynamic_binding: float,
    static_return_delta: float,
    dynamic_return_delta: float,
    static_drawdown_delta: float,
    dynamic_drawdown_delta: float,
    false_delta: float,
    protection_delta: float,
    overlap_label: str,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "DYNAMIC_BASELINE_INCONCLUSIVE"
    if dynamic_binding < static_binding and false_delta <= 0.0:
        return "DYNAMIC_BASELINE_REDUCES_OVERBINDING"
    if (
        false_delta > 0.0
        and dynamic_return_delta < static_return_delta
        and dynamic_drawdown_delta <= static_drawdown_delta
    ):
        return "DYNAMIC_BASELINE_SHOWS_FALSE_COST_WORSE"
    if "REDUNDANT" in overlap_label or "ALREADY_HANDLES" in overlap_label:
        return "DYNAMIC_BASELINE_SHOWS_RISK_CAP_REDUNDANT"
    if protection_delta > 0.0 and false_delta <= 0.0:
        return "DYNAMIC_BASELINE_CONFIRMS_RISK_CAP_VALUE"
    return "DYNAMIC_BASELINE_INCONCLUSIVE"


def _period_overlap_label(period_rows: Sequence[Mapping[str, Any]]) -> str:
    triggered = [row for row in period_rows if row.get("risk_cap_triggered") is True]
    incremental = [
        row for row in period_rows if row.get("risk_cap_incremental_binding") is True
    ]
    redundant = [
        row
        for row in period_rows
        if row.get("risk_cap_triggered") is True
        and row.get("dynamic_strategy_already_de_risked") is True
    ]
    binding_without_dynamic = [
        row
        for row in period_rows
        if row.get("risk_cap_triggered") is True
        and row.get("simulated_cap_binding_active") is True
        and row.get("dynamic_strategy_already_de_risked") is not True
    ]
    return _strategy_overlap_label(
        trigger_count=len(triggered),
        incremental_count=len(incremental),
        redundant_count=len(redundant),
        binding_without_dynamic_count=len(binding_without_dynamic),
        data_quality_status="PASS",
    )


def _period_label(
    *,
    return_delta: float,
    drawdown_delta: float,
    false_cost: float,
    downside_protection: float,
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "INCONCLUSIVE_PERIOD"
    if downside_protection > false_cost and drawdown_delta > 0.0:
        return "HELPFUL_STRESS_PERIOD"
    if false_cost > downside_protection and return_delta < 0.0:
        return "COSTLY_REBOUND_PERIOD"
    if (
        return_delta == 0.0
        and drawdown_delta == 0.0
        and false_cost == 0.0
        and downside_protection == 0.0
    ):
        return "NO_MATERIAL_EFFECT_PERIOD"
    if false_cost or downside_protection:
        return "MIXED_PERIOD"
    return "INCONCLUSIVE_PERIOD"


def _pit_caveat_status(pit_boundary: Mapping[str, Any]) -> str:
    if pit_boundary.get("strict_pit_ready") is True:
        return "STRICT_PIT_UNEXPECTED"
    if pit_boundary.get("pit_approximation_ready") is True:
        return "PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_ONLY"
    return "PIT_CAVEAT_INCONCLUSIVE"


def _recommended_policy_action(
    *,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "STOP_POLICY_REFINEMENT"
    if _archive_supported(false_cost, downside, strategy_overlap):
        return "ARCHIVE_CURRENT_MECHANICS"
    if overbinding.get("overbinding_label") == "OVERBINDING_BLOCKING":
        return "HIGH_INTENSITY_ONLY_RISK_CAP"
    if false_cost.get("false_cost_label") in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}:
        return "MANUAL_REVIEW_ONLY_FOR_HIGH_INTENSITY"
    if (
        cap_binding.get("cap_binding_frequency_label")
        == "EXCESSIVE_BINDING_FREQUENCY"
        and return_drawdown.get("return_cost_materiality") == "RETURN_COST_HIGH"
    ):
        return "RELAX_CAP_THRESHOLD"
    if downside.get("downside_protection_label") == "DOWNSIDE_PROTECTION_POSITIVE_PROXY":
        return "KEEP_CURRENT_POLICY_FOR_NEXT_REVIEW"
    return "MANUAL_REVIEW_ONLY_FOR_HIGH_INTENSITY"


def _observed_policy_issue(
    *,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "dynamic_baseline_data_quality_blocked"
    issues: list[str] = []
    if cap_binding.get("cap_binding_frequency_label") == "EXCESSIVE_BINDING_FREQUENCY":
        issues.append("excessive_cap_binding_frequency")
    if overbinding.get("overbinding_label") in {
        "OVERBINDING_HIGH",
        "OVERBINDING_BLOCKING",
    }:
        issues.append("overbinding_risk")
    if return_drawdown.get("return_cost_materiality") == "RETURN_COST_HIGH":
        issues.append("high_return_proxy_cost")
    if false_cost.get("false_cost_label") in {"FALSE_COST_HIGH", "FALSE_COST_BLOCKING"}:
        issues.append("false_cost_elevated")
    if false_cost.get("missed_upside_label") in {
        "MISSED_UPSIDE_HIGH",
        "MISSED_UPSIDE_BLOCKING",
    }:
        issues.append("missed_upside_elevated")
    if downside.get("downside_protection_label") == "DOWNSIDE_PROTECTION_WEAK_PROXY":
        issues.append("downside_protection_weak")
    if "REDUNDANT" in str(strategy_overlap.get("overlap_label", "")):
        issues.append("dynamic_strategy_overlap_redundant")
    return ",".join(issues) if issues else "no_material_policy_issue_detected"


def _candidate_policy_variants(action: str) -> list[str]:
    variants = {
        "KEEP_CURRENT_POLICY_FOR_NEXT_REVIEW": ["current_policy_research_only_review"],
        "TIGHTEN_CAP_THRESHOLD": ["lower_cap_threshold"],
        "RELAX_CAP_THRESHOLD": ["higher_cap_threshold", "high_intensity_only_cap"],
        "SHORTEN_COOLDOWN": ["shorter_cooldown_days"],
        "LENGTHEN_COOLDOWN": ["longer_cooldown_days"],
        "HIGH_INTENSITY_ONLY_RISK_CAP": ["high_intensity_only_cap_trigger"],
        "MANUAL_REVIEW_ONLY_FOR_HIGH_INTENSITY": [
            "manual_review_only_high_intensity_warning"
        ],
        "STOP_POLICY_REFINEMENT": ["stop_current_policy_refinement"],
        "ARCHIVE_CURRENT_MECHANICS": ["archive_current_exposure_cap_mechanics"],
    }
    return variants.get(action, ["manual_review_only_variant"])


def _task_for_policy_action(action: str, data_quality_status: str) -> str:
    if data_quality_status == "FAIL":
        return NEXT_DATA_TASK
    if action in {"RELAX_CAP_THRESHOLD", "TIGHTEN_CAP_THRESHOLD", "SHORTEN_COOLDOWN"}:
        return NEXT_POLICY_REFINEMENT_TASK
    if action == "HIGH_INTENSITY_ONLY_RISK_CAP":
        return NEXT_HIGH_INTENSITY_TASK
    if action == "MANUAL_REVIEW_ONLY_FOR_HIGH_INTENSITY":
        return NEXT_MANUAL_REVIEW_TASK
    if action in {"STOP_POLICY_REFINEMENT", "ARCHIVE_CURRENT_MECHANICS"}:
        return NEXT_ARCHIVE_TASK
    return NEXT_CONTINUE_TASK


def _overall_recommendation(
    *,
    cap_binding: Mapping[str, Any],
    overbinding: Mapping[str, Any],
    return_drawdown: Mapping[str, Any],
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
    static_dynamic: Mapping[str, Any],
    data_quality_status: str,
) -> str:
    if data_quality_status == "FAIL":
        return "DATA_QUALITY_REMEDIATION_REQUIRED"
    if _archive_supported(false_cost, downside, strategy_overlap):
        return "ARCHIVE_CURRENT_EXPOSURE_CAP_MECHANICS"
    if overbinding.get("overbinding_label") == "OVERBINDING_BLOCKING":
        return "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
    if (
        cap_binding.get("cap_binding_frequency_label")
        == "EXCESSIVE_BINDING_FREQUENCY"
        and return_drawdown.get("return_cost_materiality") == "RETURN_COST_HIGH"
        and downside.get("downside_protection_label")
        in {"DOWNSIDE_PROTECTION_POSITIVE_PROXY", "DOWNSIDE_PROTECTION_WEAK_PROXY"}
    ):
        return "REFINE_EXPOSURE_CAP_POLICY"
    if (
        strategy_overlap.get("overlap_label")
        in {
            "RISK_CAP_BINDING_WHEN_DYNAMIC_MISSES_RISK",
            "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY",
        }
        and return_drawdown.get("return_cost_materiality")
        in {"RETURN_COST_HIGH", "RETURN_COST_MATERIAL"}
    ):
        return "MANUAL_REVIEW_ONLY_RISK_CAP"
    if static_dynamic.get("comparison_label") == "DYNAMIC_BASELINE_CONFIRMS_RISK_CAP_VALUE":
        return "CONTINUE_EXPOSURE_CAP_RESEARCH"
    return "INCONCLUSIVE"


def _archive_supported(
    false_cost: Mapping[str, Any],
    downside: Mapping[str, Any],
    strategy_overlap: Mapping[str, Any],
) -> bool:
    return (
        false_cost.get("false_cost_label") == "FALSE_COST_BLOCKING"
        and false_cost.get("missed_upside_label") == "MISSED_UPSIDE_BLOCKING"
        and downside.get("downside_protection_label")
        in {"DOWNSIDE_PROTECTION_WEAK_PROXY", "DOWNSIDE_PROTECTION_NEGATIVE_PROXY"}
        and strategy_overlap.get("overlap_label")
        in {
            "RISK_CAP_MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY",
            "DYNAMIC_STRATEGY_ALREADY_HANDLES_RISK",
        }
    )


def _route_for_overall_recommendation(overall: str) -> str:
    routes = {
        "REFINE_EXPOSURE_CAP_POLICY": NEXT_POLICY_REFINEMENT_TASK,
        "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE": NEXT_HIGH_INTENSITY_TASK,
        "MANUAL_REVIEW_ONLY_RISK_CAP": NEXT_MANUAL_REVIEW_TASK,
        "ARCHIVE_CURRENT_EXPOSURE_CAP_MECHANICS": NEXT_ARCHIVE_TASK,
        "DATA_QUALITY_REMEDIATION_REQUIRED": NEXT_DATA_TASK,
        "CONTINUE_EXPOSURE_CAP_RESEARCH": NEXT_CONTINUE_TASK,
        "INCONCLUSIVE": NEXT_CONTINUE_TASK,
    }
    return routes.get(overall, NEXT_CONTINUE_TASK)


def _route_reason(overall: str) -> str:
    reasons = {
        "REFINE_EXPOSURE_CAP_POLICY": (
            "dynamic diagnostics show broad cap binding with costly return tradeoff"
        ),
        "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE": (
            "automatic policy appears too broad; high-intensity-only evidence is next"
        ),
        "MANUAL_REVIEW_ONLY_RISK_CAP": (
            "risk-cap has warning value but automatic cap remains too costly"
        ),
        "ARCHIVE_CURRENT_EXPOSURE_CAP_MECHANICS": (
            "false cost and redundancy dominate downside protection proxy"
        ),
        "DATA_QUALITY_REMEDIATION_REQUIRED": "dynamic dry-run data quality failed",
        "CONTINUE_EXPOSURE_CAP_RESEARCH": "diagnostics support continued research only",
        "INCONCLUSIVE": "diagnostics are inconclusive; continue research without policy change",
    }
    return reasons.get(overall, "manual review required")


def _dynamic_daily_return_proxy(
    dry_run_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    daily: dict[str, dict[str, float]] = defaultdict(lambda: {"no_cap": 0.0, "capped": 0.0})
    for row in dry_run_rows:
        day = str(row.get("date"))
        daily[day]["no_cap"] += to_float(
            row.get("dynamic_no_cap_return_contribution_proxy")
        )
        daily[day]["capped"] += to_float(
            row.get("dynamic_capped_return_contribution_proxy")
        )
    return daily


def _max_drawdown(returns: Sequence[float]) -> float:
    value = 1.0
    peak = 1.0
    drawdown = 0.0
    for item in returns:
        value *= 1.0 + to_float(item)
        peak = max(peak, value)
        if peak > 0.0:
            drawdown = min(drawdown, value / peak - 1.0)
    return drawdown


def _missed_upside_cost(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("dynamic_no_cap_return_contribution_proxy"))
            - to_float(row.get("dynamic_capped_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) > 0.0
    )


def _downside_protection(dry_run_rows: Sequence[Mapping[str, Any]]) -> float:
    return sum(
        max(
            0.0,
            to_float(row.get("dynamic_capped_return_contribution_proxy"))
            - to_float(row.get("dynamic_no_cap_return_contribution_proxy")),
        )
        for row in dry_run_rows
        if row.get("simulated_cap_binding_active") is True
        and to_float(row.get("asset_return")) < 0.0
    )


def _first_float(*values: Any) -> float:
    for value in values:
        if value is not None and value != "":
            return to_float(value)
    return 0.0
