from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
)
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

TASK_ID = "TRADING-2334_HIGH_INTENSITY_ONLY_RISK_CAP_FORWARD_OBSERVE_PLAN"
REPORT_TYPE = "high_intensity_risk_cap_forward_observe_plan"
ARTIFACT_ROLE = "high_intensity_risk_cap_forward_observe_plan"
MODE = "high_intensity_forward_observe_plan"
STATUS = "HIGH_INTENSITY_FORWARD_OBSERVE_PLAN_READY_PROMOTION_BLOCKED"
DATA_QUALITY_BLOCKED_STATUS = "BLOCKED_BY_DYNAMIC_DRY_RUN_DATA_QUALITY"
DATA_VALIDATION_POLICY = "NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_ARTIFACTS_ONLY"
EXPECTED_2333_RECOMMENDATION = "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
EXPECTED_2334_TASK = "TRADING-2334_High_Intensity_Only_Risk_Cap_Forward_Observe_Plan"
NEXT_THRESHOLD_TASK = "TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection"
NEXT_EVENT_LOGGER_TASK = (
    "TRADING-2335_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger"
)
NEXT_DATA_REMEDIATION_TASK = (
    "TRADING-2335_High_Intensity_Risk_Cap_Data_Contract_Remediation"
)
NEXT_DYNAMIC_DATA_TASK = "TRADING-2335_Dynamic_Target_Baseline_Data_Remediation"
NEXT_ARCHIVE_TASK = "TRADING-2335_Archive_High_Intensity_Risk_Cap_Observe_Line"
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
LATENCY_POLICY = "NEXT_TRADING_DAY_DECISION"
PIT_POLICY = "PIT_APPROXIMATION_READY"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "forward_observe_plan_only": True,
    "manual_review_only": True,
    "runtime_observe_started": False,
    "automatic_exposure_cap_allowed": False,
    "target_weight_action_allowed": False,
    "rebalance_instruction_allowed": False,
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

# TRADING-2334 pilot observe-plan thresholds. These are route candidates only;
# TRADING-2335 must select a deterministic threshold before any event logger exists.
MINIMUM_OBSERVE_EVENT_COUNT = 20
MINIMUM_OBSERVE_MONTHS = 6
MINIMUM_OUTCOME_READY_COUNT = 15


class HighIntensityForwardObservePlanError(ValueError):
    pass


def run_high_intensity_risk_cap_forward_observe_plan(
    *,
    dynamic_diagnostics_dir: Path = DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    readiness_dir: Path = DEFAULT_READINESS_ROOT,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    simulation_policy_dir: Path = DEFAULT_SIMULATION_POLICY_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityForwardObservePlanError(
            f"high-intensity risk-cap forward observe plan only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_forward_observe_plan_inputs(
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        simulation_policy_dir=simulation_policy_dir,
    )
    diagnostics = inputs["dynamic_diagnostics"]
    dry_run = inputs["dynamic_dry_run"]
    alignment_rows = _event_rows_from_alignment(
        records(dry_run["trigger_alignment"].get("rows"))
    )
    data_quality = mapping(dry_run["data_quality_report"])
    data_quality_status = str(data_quality.get("data_quality_status", "FAIL"))

    criteria = build_high_intensity_trigger_selection_criteria(
        alignment_rows=alignment_rows,
        diagnostics=diagnostics,
        data_quality_status=data_quality_status,
    )
    threshold_rows = build_high_intensity_trigger_threshold_candidate_matrix(
        alignment_rows=alignment_rows,
        diagnostics=diagnostics,
        criteria=criteria,
        data_quality_status=data_quality_status,
    )
    backtest_context = build_high_intensity_trigger_candidate_backtest_context(
        diagnostics=diagnostics,
        dry_run=dry_run,
        threshold_rows=threshold_rows,
        data_quality_status=data_quality_status,
    )
    event_schema = build_high_intensity_forward_observe_event_schema(
        timestamp_context=inputs["timestamp_remediation"],
    )
    evidence_contract = build_high_intensity_forward_observe_evidence_contract()
    outcome_contract = build_high_intensity_actual_path_outcome_contract()
    manual_review_boundary = build_high_intensity_manual_review_boundary()
    false_warning_framework = (
        build_high_intensity_false_warning_missed_stress_framework()
    )
    stop_continue_archive = build_high_intensity_stop_continue_archive_rules()
    safety_boundary = build_high_intensity_forward_observe_safety_boundary(
        generated_at=generated_at,
        data_quality_status=data_quality_status,
    )
    readiness = build_high_intensity_observe_readiness_checklist(
        criteria=criteria,
        threshold_rows=threshold_rows,
        event_schema=event_schema,
        evidence_contract=evidence_contract,
        outcome_contract=outcome_contract,
        manual_review_boundary=manual_review_boundary,
        stop_continue_archive=stop_continue_archive,
        safety_boundary=safety_boundary,
        data_quality_status=data_quality_status,
    )
    task_route = build_high_intensity_2335_task_route(readiness)
    summary = build_high_intensity_forward_observe_plan_summary(
        generated_at=generated_at,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        simulation_policy_dir=simulation_policy_dir,
        diagnostics=diagnostics,
        dry_run=dry_run,
        criteria=criteria,
        threshold_rows=threshold_rows,
        readiness=readiness,
        task_route=task_route,
        data_quality_status=data_quality_status,
    )
    paths = write_high_intensity_forward_observe_plan_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        summary=summary,
        criteria=criteria,
        threshold_rows=threshold_rows,
        backtest_context=backtest_context,
        event_schema=event_schema,
        evidence_contract=evidence_contract,
        outcome_contract=outcome_contract,
        manual_review_boundary=manual_review_boundary,
        false_warning_framework=false_warning_framework,
        stop_continue_archive=stop_continue_archive,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": paths})


def load_high_intensity_forward_observe_plan_inputs(
    *,
    dynamic_diagnostics_dir: Path,
    dynamic_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    simulation_policy_dir: Path,
) -> dict[str, Any]:
    return {
        "dynamic_diagnostics": load_trading_2333_dynamic_diagnostics_outputs(
            dynamic_diagnostics_dir
        ),
        "dynamic_dry_run": load_trading_2332_dynamic_dry_run_context(
            dynamic_dry_run_dir
        ),
        "readiness": load_trading_2331_high_intensity_readiness_context(
            readiness_dir
        ),
        "timestamp_remediation": load_trading_2330_high_intensity_timestamp_context(
            timestamp_remediation_dir
        ),
        "simulation_policy": load_trading_2323_high_intensity_policy_context(
            simulation_policy_dir
        ),
    }


def load_trading_2333_dynamic_diagnostics_outputs(
    dynamic_diagnostics_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": dynamic_diagnostics_dir
        / "dynamic_exposure_cap_diagnostics_review_summary.json",
        "cap_binding": dynamic_diagnostics_dir
        / "dynamic_cap_binding_diagnostics_matrix.json",
        "overbinding": dynamic_diagnostics_dir / "dynamic_overbinding_diagnostics.json",
        "exposure_reduction": dynamic_diagnostics_dir
        / "dynamic_exposure_reduction_diagnostics.json",
        "return_drawdown": dynamic_diagnostics_dir
        / "dynamic_return_drawdown_tradeoff_diagnostics.json",
        "false_cost_missed_upside": dynamic_diagnostics_dir
        / "dynamic_false_cost_missed_upside_diagnostics.json",
        "downside": dynamic_diagnostics_dir
        / "dynamic_downside_protection_diagnostics.json",
        "turnover_cooldown": dynamic_diagnostics_dir
        / "dynamic_turnover_cooldown_diagnostics.json",
        "strategy_overlap": dynamic_diagnostics_dir
        / "dynamic_strategy_overlap_diagnostics.json",
        "static_dynamic": dynamic_diagnostics_dir
        / "static_vs_dynamic_exposure_cap_evidence_comparison.json",
        "period_attribution": dynamic_diagnostics_dir
        / "dynamic_cap_binding_period_attribution.json",
        "policy_sensitivity": dynamic_diagnostics_dir
        / "dynamic_policy_sensitivity_recommendation_matrix.json",
        "decision": dynamic_diagnostics_dir / "dynamic_exposure_cap_decision_matrix.json",
        "task_route": dynamic_diagnostics_dir / "dynamic_2334_task_route.json",
        "interpretation_boundary": dynamic_diagnostics_dir
        / "dynamic_exposure_cap_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2333 diagnostics outputs")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2333 {key}", payload)
    summary = mapping(payloads["summary"])
    decision = mapping(payloads["decision"])
    route = mapping(payloads["task_route"])
    recommendation = str(summary.get("overall_recommendation", ""))
    next_task = str(summary.get("next_task", ""))
    if recommendation != EXPECTED_2333_RECOMMENDATION:
        raise HighIntensityForwardObservePlanError(
            "TRADING-2334 requires TRADING-2333 overall_recommendation "
            f"{EXPECTED_2333_RECOMMENDATION}"
        )
    if next_task != EXPECTED_2334_TASK:
        raise HighIntensityForwardObservePlanError(
            f"TRADING-2334 requires TRADING-2333 next_task {EXPECTED_2334_TASK}"
        )
    if str(decision.get("overall_recommendation", "")) != EXPECTED_2333_RECOMMENDATION:
        raise HighIntensityForwardObservePlanError(
            "TRADING-2333 decision matrix does not route to high-intensity-only"
        )
    if str(route.get("next_task", "")) != EXPECTED_2334_TASK:
        raise HighIntensityForwardObservePlanError(
            "TRADING-2333 task route is not high-intensity-only forward observe"
        )
    return {
        "source_dir": str(dynamic_diagnostics_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2332_dynamic_dry_run_context(dynamic_dry_run_dir: Path) -> dict[str, Any]:
    paths = {
        "summary": dynamic_dry_run_dir
        / "dynamic_target_exposure_cap_dry_run_summary.json",
        "trigger_alignment": dynamic_dry_run_dir
        / "dynamic_target_risk_cap_trigger_alignment_matrix.json",
        "dry_run_result": dynamic_dry_run_dir
        / "dynamic_target_exposure_cap_dry_run_result.json",
        "binding_day_matrix": dynamic_dry_run_dir
        / "dynamic_target_cap_binding_day_matrix.json",
        "strategy_overlap": dynamic_dry_run_dir
        / "dynamic_target_strategy_overlap_report.json",
        "false_cost": dynamic_dry_run_dir
        / "dynamic_target_false_risk_cap_cost_report.json",
        "missed_upside": dynamic_dry_run_dir
        / "dynamic_target_missed_upside_cost_report.json",
        "downside": dynamic_dry_run_dir
        / "dynamic_target_downside_protection_proxy_report.json",
        "data_quality_report": dynamic_dry_run_dir
        / "dynamic_target_data_quality_report.json",
        "pit_boundary": dynamic_dry_run_dir
        / "dynamic_target_pit_caveat_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2332 dynamic dry-run context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2332 {key}", payload)
    boundary = mapping(payloads["pit_boundary"])
    if boundary.get("strict_pit_ready") is True:
        raise HighIntensityForwardObservePlanError(
            "TRADING-2332 boundary must not mark strict_pit_ready=true"
        )
    if str(boundary.get("known_at_policy", "")) != KNOWN_AT_POLICY:
        raise HighIntensityForwardObservePlanError(
            "TRADING-2334 requires NEXT_SESSION_DECISION_POLICY"
        )
    if "data_quality_status" not in mapping(payloads["data_quality_report"]):
        raise HighIntensityForwardObservePlanError(
            "TRADING-2332 data quality report missing data_quality_status"
        )
    return {
        "source_dir": str(dynamic_dry_run_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2331_high_intensity_readiness_context(readiness_dir: Path) -> dict[str, Any]:
    paths = {
        "pit_acceptance": readiness_dir
        / "dynamic_dry_run_pit_caveat_acceptance_report.json",
        "interpretation_boundary": readiness_dir
        / "dynamic_dry_run_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2331 readiness context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2331 {key}", payload)
    return {
        "source_dir": str(readiness_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2330_high_intensity_timestamp_context(
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


def load_trading_2323_high_intensity_policy_context(
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
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2323 {key}", payload)
    return {
        "source_dir": str(simulation_policy_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def build_high_intensity_trigger_selection_criteria(
    *,
    alignment_rows: Sequence[Mapping[str, Any]],
    diagnostics: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    scores = [to_float(row.get("risk_cap_score")) for row in alignment_rows]
    triggered_scores = [
        to_float(row.get("risk_cap_score"))
        for row in alignment_rows
        if row.get("risk_cap_triggered") is True
    ]
    p90 = _percentile(triggered_scores or scores, 0.90)
    p95 = _percentile(triggered_scores or scores, 0.95)
    diagnostics_summary = mapping(diagnostics.get("summary"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.trigger_selection_criteria.v1",
            "task_id": TASK_ID,
            "criteria_id": "high_intensity_risk_cap_forward_observe_v1",
            "criteria_version": "v1",
            "source_signal_family": "volatility_regime_scope_narrowed_risk_cap",
            "source_context": "dynamic_exposure_cap_diagnostics_review",
            "selected_usage": "high_intensity_forward_observe",
            "blocked_usage": [
                "automatic_exposure_cap",
                "target_weight_action",
                "rebalance_instruction",
                "paper_shadow",
                "production",
                "broker_action",
            ],
            "high_intensity_definition": {
                "intensity_field": "risk_cap_intensity",
                "score_field": "risk_cap_score",
                "trigger_flag_field": "risk_cap_triggered",
                "scope_active_field": "scope_active",
                "signal_direction_field": "signal_direction",
                "high_intensity_label": "high",
            },
            "candidate_thresholds": [
                {
                    "threshold_id": "P90_RISK_CAP_SCORE",
                    "threshold_type": "INTENSITY_PERCENTILE_THRESHOLD",
                    "threshold_value": "P90",
                    "numeric_threshold_value": round_float(p90),
                    "threshold_source": "TRADING-2332 triggered risk_cap_score distribution",
                    "expected_trigger_density": _density_for_threshold(
                        alignment_rows,
                        p90,
                    ),
                    "purpose": "reduce trigger frequency",
                    "caveat": "candidate only; not optimized on future returns",
                },
                {
                    "threshold_id": "P95_RISK_CAP_SCORE",
                    "threshold_type": "INTENSITY_PERCENTILE_THRESHOLD",
                    "threshold_value": "P95",
                    "numeric_threshold_value": round_float(p95),
                    "threshold_source": "TRADING-2332 triggered risk_cap_score distribution",
                    "expected_trigger_density": _density_for_threshold(
                        alignment_rows,
                        p95,
                    ),
                    "purpose": "strict high-intensity-only warning",
                    "caveat": "score saturation may make P90 and P95 equivalent",
                },
                {
                    "threshold_id": "COMPOSITE_HIGH_INTENSITY_RULE",
                    "threshold_type": "COMPOSITE_HIGH_INTENSITY_RULE",
                    "threshold_value": (
                        "risk_cap_triggered AND scope_active AND "
                        f"risk_cap_score >= {round_float(p90)} AND "
                        "signal_direction != none"
                    ),
                    "numeric_threshold_value": round_float(p90),
                    "threshold_source": "TRADING-2334 plan candidate derived from 2332 fields",
                    "expected_trigger_density": _density_for_threshold(
                        alignment_rows,
                        p90,
                        composite=True,
                    ),
                    "purpose": "avoid low-quality trigger expansion",
                    "caveat": "requires deterministic selection in TRADING-2335",
                },
            ],
            "medium_low_policy": {
                "low_intensity": "record_only",
                "medium_intensity": "record_only",
                "high_intensity": "forward_observe_event_candidate",
            },
            "diagnostics_recommendation": diagnostics_summary.get(
                "overall_recommendation",
                EXPECTED_2333_RECOMMENDATION,
            ),
            "data_quality_status": data_quality_status,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_trigger_threshold_candidate_matrix(
    *,
    alignment_rows: Sequence[Mapping[str, Any]],
    diagnostics: Mapping[str, Any],
    criteria: Mapping[str, Any],
    data_quality_status: str,
) -> list[dict[str, Any]]:
    threshold_specs = records(criteria.get("candidate_thresholds"))
    summary = mapping(diagnostics.get("summary"))
    false_cost = mapping(diagnostics.get("false_cost_missed_upside"))
    downside = mapping(diagnostics.get("downside"))
    overbinding_label = str(summary.get("overbinding_label", ""))
    rows: list[dict[str, Any]] = []
    for spec in threshold_specs:
        threshold_id = str(spec.get("threshold_id"))
        threshold_value = to_float(spec.get("numeric_threshold_value"))
        composite = str(spec.get("threshold_type")) == "COMPOSITE_HIGH_INTENSITY_RULE"
        candidate_rows = _rows_for_threshold(
            alignment_rows,
            threshold_value,
            composite=composite,
        )
        trigger_count = len(candidate_rows)
        density = trigger_count / len(alignment_rows) if alignment_rows else 0.0
        rows.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.threshold_candidate.v1",
                    "task_id": TASK_ID,
                    "threshold_id": threshold_id,
                    "threshold_type": spec.get("threshold_type"),
                    "threshold_value": spec.get("threshold_value"),
                    "numeric_threshold_value": round_float(threshold_value),
                    "trigger_count_estimate": trigger_count,
                    "trigger_density_estimate": round_float(density),
                    "historical_binding_overlap_count": trigger_count,
                    "historical_false_cost_context": false_cost.get(
                        "false_cost_label",
                        false_cost.get("false_risk_cap_cost_label", ""),
                    ),
                    "historical_false_cost_proxy": round_float(
                        false_cost.get("false_risk_cap_cost_proxy")
                    ),
                    "historical_downside_context": downside.get(
                        "downside_protection_label",
                        "",
                    ),
                    "historical_downside_proxy": round_float(
                        downside.get("downside_protection_proxy")
                    ),
                    "expected_observe_event_frequency": (
                        f"{trigger_count} historical candidate events across "
                        f"{len(alignment_rows)} decision dates"
                    ),
                    "overbinding_risk": _overbinding_risk(
                        threshold_id,
                        overbinding_label,
                    ),
                    "missed_stress_risk": _missed_stress_risk(
                        threshold_id,
                        trigger_count,
                    ),
                    "interpretability_score": _interpretability_score(threshold_id),
                    "implementation_complexity": _implementation_complexity(
                        threshold_id
                    ),
                    "recommended_status": _threshold_recommended_status(
                        threshold_id=threshold_id,
                        trigger_count=trigger_count,
                        data_quality_status=data_quality_status,
                        overbinding_label=overbinding_label,
                    ),
                    **SAFETY_FIELDS,
                }
            )
        )
    return rows


def build_high_intensity_trigger_candidate_backtest_context(
    *,
    diagnostics: Mapping[str, Any],
    dry_run: Mapping[str, Any],
    threshold_rows: Sequence[Mapping[str, Any]],
    data_quality_status: str,
) -> dict[str, Any]:
    summary = mapping(dry_run.get("summary"))
    diagnostics_summary = mapping(diagnostics.get("summary"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.candidate_backtest_context.v1",
            "task_id": TASK_ID,
            "context_role": "historical_context_only_no_new_backtest",
            "new_backtest_executed": False,
            "dynamic_dry_run_record_count": summary.get("record_count", 0),
            "dynamic_dry_run_cap_binding_days": summary.get("cap_binding_days", 0),
            "dynamic_dry_run_cap_binding_rate": summary.get("cap_binding_rate", 0.0),
            "dynamic_return_proxy_delta": summary.get("return_proxy_delta", 0.0),
            "dynamic_drawdown_proxy_delta": summary.get("drawdown_proxy_delta", 0.0),
            "diagnostics_overall_recommendation": diagnostics_summary.get(
                "overall_recommendation",
                "",
            ),
            "threshold_candidate_count": len(threshold_rows),
            "candidate_status_distribution": _status_distribution(threshold_rows),
            "data_quality_status": data_quality_status,
            "data_validation_policy": DATA_VALIDATION_POLICY,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_observe_event_schema(
    *,
    timestamp_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    known_at_report = mapping((timestamp_context or {}).get("known_at"))
    latency_report = mapping((timestamp_context or {}).get("latency_policy"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.event_schema.v1",
            "task_id": TASK_ID,
            "event_id": {"type": "string", "required": True},
            "event_date": {"type": "date", "required": True},
            "target_asset": {"type": "string", "required": True},
            "signal_family": {
                "type": "string",
                "value": "volatility_regime_scope_narrowed_risk_cap",
            },
            "source_candidate_id": {"type": "string", "required": True},
            "trigger_threshold_id": {"type": "string", "required": True},
            "risk_cap_triggered": {"type": "boolean", "required": True},
            "risk_cap_intensity": {"type": "string", "required": True},
            "risk_cap_score": {"type": "number", "required": True},
            "scope_active": {"type": "boolean", "required": True},
            "signal_direction": {"type": "string", "required": True},
            "high_intensity_triggered": {"type": "boolean", "required": True},
            "high_intensity_reason": {"type": "string", "required": True},
            "as_of_timestamp": {"type": "datetime", "required": True},
            "decision_timestamp": {"type": "datetime", "required": True},
            "known_at_policy": known_at_report.get("known_at_policy", KNOWN_AT_POLICY),
            "latency_policy": latency_report.get("decision_delay", LATENCY_POLICY),
            "pit_policy": PIT_POLICY,
            "manual_review_observation_flag": {
                "type": "boolean",
                "required": True,
            },
            "manual_review_reason": {"type": "string", "required": True},
            "allowed_usage": [
                "research_only_forward_observe",
                "manual_review_context",
            ],
            "blocked_usage": [
                "automatic_exposure_cap",
                "target_weight_action",
                "rebalance_instruction",
                "paper_shadow",
                "production",
                "broker_action",
            ],
            "event_status": "OBSERVE_PENDING",
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_observe_evidence_contract() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.evidence_contract.v1",
            "task_id": TASK_ID,
            "event_id": {"required": True},
            "event_date": {"required": True},
            "target_asset": {"required": True},
            "horizon_1d_required": True,
            "horizon_5d_required": True,
            "horizon_10d_required": True,
            "horizon_20d_required": True,
            "forward_return_required": True,
            "forward_max_drawdown_required": True,
            "forward_volatility_required": True,
            "stress_event_classification_required": True,
            "rebound_event_classification_required": True,
            "missed_upside_classification_required": True,
            "dynamic_strategy_overlap_required": True,
            "manual_review_note_optional": True,
            "outcome_binding_status": {
                "one_of": [
                    "OUTCOME_PENDING",
                    "OUTCOME_PARTIAL",
                    "OUTCOME_READY",
                    "OUTCOME_BLOCKED",
                ]
            },
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_actual_path_outcome_contract() -> dict[str, Any]:
    template = {
        "event_id": "",
        "event_date": "",
        "target_asset": "",
        "outcome_as_of_date": "",
        "horizon": "",
        "forward_return": None,
        "forward_max_drawdown": None,
        "forward_min_return": None,
        "forward_max_return": None,
        "realized_volatility": None,
        "stress_detected": None,
        "rebound_detected": None,
        "false_warning_candidate": None,
        "missed_stress_candidate": None,
        "missed_upside_candidate": None,
        "downside_capture_candidate": None,
        "manual_review_would_have_helped": None,
        "outcome_quality_status": "OUTCOME_PENDING",
        "pit_outcome_binding": "OUTCOME_BOUND_AFTER_EVENT_ONLY",
    }
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.actual_path_outcome_contract.v1",
            "task_id": TASK_ID,
            "allowed_horizons": ["1d", "5d", "10d", "20d"],
            "outcome_record_template": template,
            "future_outcome_use_policy": (
                "Actual path outcome can only be filled after event creation; "
                "future outcomes must not modify trigger creation."
            ),
            **template,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_manual_review_boundary() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_boundary.v1",
            "task_id": TASK_ID,
            "manual_review_only": True,
            "research_only": True,
            "forward_observe_only": True,
            "automatic_exposure_cap_allowed": False,
            "target_weight_action_allowed": False,
            "rebalance_instruction_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "allowed_outputs": [
                "manual_review_observation_flag",
                "manual_review_reason",
                "risk_warning_context",
                "observe_event_id",
            ],
            "forbidden_outputs": [
                "reduce_position_instruction",
                "increase_cash_instruction",
                "target_weight",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "paper_shadow_ready",
                "production_ready",
                "broker_action",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_false_warning_missed_stress_framework() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.false_warning_missed_stress.v1",
            "task_id": TASK_ID,
            "false_warning_definition": {
                "description": (
                    "High-intensity trigger followed by no 5d/10d stress and "
                    "material upside opportunity cost."
                ),
                "candidate_rules": [
                    "high_intensity_triggered AND no_stress_5d AND forward_return_5d_positive",
                    "high_intensity_triggered AND no_stress_10d AND missed_upside_candidate",
                ],
            },
            "missed_stress_definition": {
                "description": (
                    "Subsequent material drawdown or stress without a high-intensity "
                    "trigger before the event."
                ),
                "candidate_rules": [
                    "stress_detected AND high_intensity_triggered=false",
                    "forward_max_drawdown_10d_material AND no_prior_high_intensity_event",
                ],
            },
            "missed_upside_definition": {
                "description": (
                    "High-intensity trigger followed by fast rebound where automatic "
                    "cap would have caused material opportunity cost."
                ),
                "candidate_rules": [
                    "high_intensity_triggered AND rebound_detected AND forward_return_5d_positive",
                    "missed_upside_candidate AND false_warning_candidate",
                ],
            },
            "downside_capture_definition": {
                "description": (
                    "High-intensity trigger followed by drawdown/stress, indicating "
                    "warning value for manual review."
                ),
                "candidate_rules": [
                    "high_intensity_triggered AND stress_detected",
                    "high_intensity_triggered AND forward_max_drawdown_10d_material",
                ],
            },
            "metrics": [
                "high_intensity_event_count",
                "outcome_ready_count",
                "false_warning_count",
                "missed_stress_count",
                "missed_upside_count",
                "downside_capture_count",
                "precision_proxy",
                "recall_proxy",
                "false_warning_rate",
                "missed_stress_rate",
                "missed_upside_rate",
                "downside_capture_rate",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_stop_continue_archive_rules() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.stop_continue_archive_rules.v1",
            "task_id": TASK_ID,
            "minimum_observe_event_count": MINIMUM_OBSERVE_EVENT_COUNT,
            "minimum_observe_months": MINIMUM_OBSERVE_MONTHS,
            "minimum_outcome_ready_count": MINIMUM_OUTCOME_READY_COUNT,
            "continue_conditions": [
                "downside_capture_rate improves",
                "false_warning_rate acceptable",
                "missed_upside_rate acceptable",
                "trigger_density remains low",
                "manual review usefulness positive",
            ],
            "archive_conditions": [
                "false_warning_rate high",
                "missed_stress_rate high",
                "missed_upside_rate high",
                "downside_capture_rate weak",
                "trigger_density still too broad",
                "no incremental value over dynamic strategy",
            ],
            "refine_threshold_conditions": [
                "P90 too broad -> try P95",
                "absolute intensity threshold unstable -> switch percentile",
                "composite rule improves false-warning rate",
            ],
            "manual_review_only_conditions": [
                "event evidence useful but not stable enough for automation",
                "PIT caveat remains unresolved",
                "owner review requires qualitative context",
            ],
            "owner_review_conditions": [
                "minimum outcome-ready sample reached",
                "archive condition triggered",
                "threshold refinement changes event density materially",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_observe_readiness_checklist(
    *,
    criteria: Mapping[str, Any],
    threshold_rows: Sequence[Mapping[str, Any]],
    event_schema: Mapping[str, Any],
    evidence_contract: Mapping[str, Any],
    outcome_contract: Mapping[str, Any],
    manual_review_boundary: Mapping[str, Any],
    stop_continue_archive: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if data_quality_status == "FAIL":
        blockers.append("dynamic dry-run data quality FAIL")
    if not threshold_rows:
        blockers.append("threshold candidate matrix empty")
    candidate_count = sum(
        1
        for row in threshold_rows
        if row.get("recommended_status") == "CANDIDATE_FOR_2335_SELECTION"
    )
    if candidate_count == 0:
        warnings.append("no candidate threshold currently marked clean candidate")
    if blockers:
        readiness_status = "PLAN_BLOCKED"
    else:
        readiness_status = "THRESHOLD_SELECTION_REQUIRED"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.observe_readiness_checklist.v1",
            "task_id": TASK_ID,
            "trigger_selection_criteria_ready": bool(criteria),
            "threshold_candidate_matrix_ready": bool(threshold_rows),
            "event_schema_ready": bool(event_schema),
            "evidence_contract_ready": bool(evidence_contract),
            "actual_path_outcome_contract_ready": bool(outcome_contract),
            "manual_review_boundary_ready": bool(manual_review_boundary),
            "stop_continue_archive_rules_ready": bool(stop_continue_archive),
            "safety_boundary_ready": bool(safety_boundary),
            "runtime_observe_started": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": readiness_status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings
            or [
                "TRADING-2334 defines threshold candidates only; deterministic "
                "selection is deferred to TRADING-2335",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_2335_task_route(
    readiness: Mapping[str, Any],
) -> dict[str, Any]:
    status = str(readiness.get("readiness_status", "PLAN_BLOCKED"))
    if status == "PLAN_READY_FOR_2335_EVENT_LOGGER":
        next_task = NEXT_EVENT_LOGGER_TASK
        route_reason = "plan ready for observe-only event logger"
    elif status == "THRESHOLD_SELECTION_REQUIRED":
        next_task = NEXT_THRESHOLD_TASK
        route_reason = "TRADING-2334 generated candidates but did not select final threshold"
    elif status == "DATA_CONTRACT_REMEDIATION_REQUIRED":
        next_task = NEXT_DATA_REMEDIATION_TASK
        route_reason = "event or outcome data contract requires remediation"
    else:
        next_task = NEXT_ARCHIVE_TASK
        route_reason = "plan blocked or not useful enough to continue observe line"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.task_route.v1",
            "task_id": TASK_ID,
            "allowed_routes": [
                NEXT_THRESHOLD_TASK,
                NEXT_EVENT_LOGGER_TASK,
                NEXT_DATA_REMEDIATION_TASK,
                NEXT_ARCHIVE_TASK,
            ],
            "readiness_status": status,
            "next_task": next_task,
            "route_reason": route_reason,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_observe_safety_boundary(
    *,
    generated_at: datetime,
    data_quality_status: str,
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "data_quality_status": data_quality_status,
            "research_only": True,
            "forward_observe_plan_only": True,
            "runtime_observe_started": False,
            "automatic_exposure_cap_allowed": False,
            "portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_only": True,
            "forbidden_outputs": [
                "target_weight_action",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "paper_shadow_ready",
                "production_ready",
                "broker_action",
                "automatic_exposure_cap",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_forward_observe_plan_summary(
    *,
    generated_at: datetime,
    dynamic_diagnostics_dir: Path,
    dynamic_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    simulation_policy_dir: Path,
    diagnostics: Mapping[str, Any],
    dry_run: Mapping[str, Any],
    criteria: Mapping[str, Any],
    threshold_rows: Sequence[Mapping[str, Any]],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    data_quality_status: str,
) -> dict[str, Any]:
    dry_summary = mapping(dry_run.get("summary"))
    diagnostics_summary = mapping(diagnostics.get("summary"))
    status = STATUS
    next_task = str(task_route.get("next_task", NEXT_THRESHOLD_TASK))
    if data_quality_status == "FAIL":
        status = DATA_QUALITY_BLOCKED_STATUS
        next_task = NEXT_DYNAMIC_DATA_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity-Only Risk-Cap Forward Observe Plan",
            "mode": MODE,
            "status": status,
            "generated_at": generated_at.isoformat(),
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "market_regime": MARKET_REGIME,
            "selected_market_regime": MARKET_REGIME,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "dynamic_diagnostics_dir": str(dynamic_diagnostics_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "readiness_dir": str(readiness_dir),
            "timestamp_remediation_dir": str(timestamp_remediation_dir),
            "simulation_policy_dir": str(simulation_policy_dir),
            "data_quality_status": data_quality_status,
            "data_validation_policy": DATA_VALIDATION_POLICY,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_executed": False,
            "prior_2332_data_quality_gate_executed": bool(
                dry_summary.get("data_quality_gate_executed")
            ),
            "prior_2333_overall_recommendation": diagnostics_summary.get(
                "overall_recommendation",
                "",
            ),
            "prior_2333_next_task": diagnostics_summary.get("next_task", ""),
            "record_count": dry_summary.get("record_count", 0),
            "cap_binding_days": dry_summary.get("cap_binding_days", 0),
            "cap_binding_rate": dry_summary.get("cap_binding_rate", 0.0),
            "return_proxy_delta": dry_summary.get("return_proxy_delta", 0.0),
            "drawdown_proxy_delta": dry_summary.get("drawdown_proxy_delta", 0.0),
            "trigger_selection_criteria_generated": bool(criteria),
            "threshold_candidate_matrix_generated": bool(threshold_rows),
            "threshold_candidate_count": len(threshold_rows),
            "forward_observe_event_schema_generated": True,
            "forward_observe_evidence_contract_generated": True,
            "actual_path_outcome_contract_generated": True,
            "manual_review_boundary_generated": True,
            "false_warning_missed_stress_framework_generated": True,
            "stop_continue_archive_rules_generated": True,
            "observe_readiness_checklist_generated": True,
            "2335_task_route_generated": True,
            "runtime_observe_started": False,
            "readiness_status": readiness.get("readiness_status", ""),
            "overall_recommendation": "HIGH_INTENSITY_ONLY_PLAN_READY_FOR_2335",
            "next_task": next_task,
            "known_at_policy": KNOWN_AT_POLICY,
            "latency_policy": LATENCY_POLICY,
            "pit_policy": PIT_POLICY,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_forward_observe_plan_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    summary: Mapping[str, Any],
    criteria: Mapping[str, Any],
    threshold_rows: Sequence[Mapping[str, Any]],
    backtest_context: Mapping[str, Any],
    event_schema: Mapping[str, Any],
    evidence_contract: Mapping[str, Any],
    outcome_contract: Mapping[str, Any],
    manual_review_boundary: Mapping[str, Any],
    false_warning_framework: Mapping[str, Any],
    stop_continue_archive: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    outputs = [
        summary,
        criteria,
        *threshold_rows,
        backtest_context,
        event_schema,
        evidence_contract,
        outcome_contract,
        manual_review_boundary,
        false_warning_framework,
        stop_continue_archive,
        readiness,
        task_route,
        safety_boundary,
    ]
    for index, payload in enumerate(outputs):
        _validate_no_unsafe_fields(f"TRADING-2334 output {index}", payload)
    paths = {
        "summary": output_dir / "high_intensity_forward_observe_plan_summary.json",
        "criteria": output_dir / "high_intensity_trigger_selection_criteria.json",
        "threshold_json": output_dir
        / "high_intensity_trigger_threshold_candidate_matrix.json",
        "threshold_csv": output_dir
        / "high_intensity_trigger_threshold_candidate_matrix.csv",
        "backtest_context": output_dir
        / "high_intensity_trigger_candidate_backtest_context.json",
        "event_schema": output_dir
        / "high_intensity_forward_observe_event_schema.json",
        "evidence_contract": output_dir
        / "high_intensity_forward_observe_evidence_contract.json",
        "outcome_contract": output_dir
        / "high_intensity_actual_path_outcome_contract.json",
        "manual_review_boundary": output_dir
        / "high_intensity_manual_review_boundary.json",
        "false_warning_framework": output_dir
        / "high_intensity_false_warning_missed_stress_framework.json",
        "stop_continue_archive": output_dir
        / "high_intensity_stop_continue_archive_rules.json",
        "readiness": output_dir / "high_intensity_observe_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2335_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_forward_observe_safety_boundary.json",
        "main_doc": docs_root / "high_intensity_risk_cap_forward_observe_plan.md",
        "criteria_doc": docs_root / "high_intensity_trigger_selection_criteria.md",
        "event_schema_doc": docs_root
        / "high_intensity_forward_observe_event_schema.md",
        "evidence_doc": docs_root
        / "high_intensity_actual_path_evidence_contract.md",
        "rules_doc": docs_root / "high_intensity_stop_continue_archive_rules.md",
    }
    write_json(paths["summary"], dict(summary))
    write_json(paths["criteria"], dict(criteria))
    write_json(
        paths["threshold_json"],
        {
            "schema_version": f"{REPORT_TYPE}.threshold_candidate_matrix.v1",
            "task_id": TASK_ID,
            "rows": list(threshold_rows),
            **SAFETY_FIELDS,
        },
    )
    write_csv_rows(paths["threshold_csv"], threshold_rows)
    write_json(paths["backtest_context"], dict(backtest_context))
    write_json(paths["event_schema"], dict(event_schema))
    write_json(paths["evidence_contract"], dict(evidence_contract))
    write_json(paths["outcome_contract"], dict(outcome_contract))
    write_json(paths["manual_review_boundary"], dict(manual_review_boundary))
    write_json(paths["false_warning_framework"], dict(false_warning_framework))
    write_json(paths["stop_continue_archive"], dict(stop_continue_archive))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))
    write_markdown(
        paths["main_doc"],
        _render_main_doc(
            summary=summary,
            threshold_rows=threshold_rows,
            task_route=task_route,
        ),
    )
    write_markdown(paths["criteria_doc"], _render_criteria_doc(criteria))
    write_markdown(paths["event_schema_doc"], _render_event_schema_doc(event_schema))
    write_markdown(
        paths["evidence_doc"],
        _render_evidence_doc(
            evidence_contract=evidence_contract,
            outcome_contract=outcome_contract,
            manual_review_boundary=manual_review_boundary,
        ),
    )
    write_markdown(
        paths["rules_doc"],
        _render_rules_doc(
            false_warning_framework=false_warning_framework,
            stop_continue_archive=stop_continue_archive,
        ),
    )
    return {key: str(path) for key, path in paths.items()}


def _render_main_doc(
    *,
    summary: Mapping[str, Any],
    threshold_rows: Sequence[Mapping[str, Any]],
    task_route: Mapping[str, Any],
) -> str:
    rows = "\n".join(
        f"- `{row['threshold_id']}`: `{row['recommended_status']}` "
        f"(density `{row['trigger_density_estimate']}`)"
        for row in threshold_rows
    )
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Forward Observe Plan",
            "",
            "TRADING-2334 承接 TRADING-2333 `HIGH_INTENSITY_ONLY_FORWARD_OBSERVE` "
            "route，把 broad exposure-cap mechanics 收窄为 research-only high-intensity "
            "risk warning plan。本任务不启动 runtime observe，不执行新的 dry-run，不生成交易指令。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_validation_policy: `{summary['data_validation_policy']}`",
            f"- prior_2333_overall_recommendation: "
            f"`{summary['prior_2333_overall_recommendation']}`",
            f"- cap_binding_rate: `{summary['cap_binding_rate']}`",
            f"- return_proxy_delta: `{summary['return_proxy_delta']}`",
            f"- drawdown_proxy_delta: `{summary['drawdown_proxy_delta']}`",
            f"- readiness_status: `{summary['readiness_status']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- runtime_observe_started: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Threshold Candidates",
            "",
            rows,
            "",
            "## 解释边界",
            "",
            "当前 broad exposure-cap mechanics 不应继续作为 automatic exposure limiter。"
            "High-intensity trigger 仅作为 future forward observe / manual review context，"
            "medium / low intensity trigger 只记录，不触发观察升级。任何 actual-path "
            "outcome 必须在事件发生后自然填充，不得反向修改 event trigger。",
            "",
        ]
    )


def _render_criteria_doc(criteria: Mapping[str, Any]) -> str:
    thresholds = records(criteria.get("candidate_thresholds"))
    lines = [
        f"- `{row['threshold_id']}`: `{row['threshold_type']}` / "
        f"`{row['threshold_value']}`"
        for row in thresholds
    ]
    return "\n".join(
        [
            "# High-Intensity Trigger Selection Criteria",
            "",
            "本报告定义 high-intensity risk-cap trigger 的候选标准。"
            "这些标准只用于 2335 threshold selection，不是 production policy。",
            "",
            f"- criteria_id: `{criteria['criteria_id']}`",
            f"- source_signal_family: `{criteria['source_signal_family']}`",
            f"- selected_usage: `{criteria['selected_usage']}`",
            "- low_intensity: `record_only`",
            "- medium_intensity: `record_only`",
            "- high_intensity: `forward_observe_event_candidate`",
            "",
            "## Candidate Thresholds",
            "",
            *lines,
            "",
            "禁止用途包括 automatic exposure cap、target weight action、rebalance "
            "instruction、paper-shadow、production 和 broker action。",
            "",
        ]
    )


def _render_event_schema_doc(event_schema: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Forward Observe Event Schema",
            "",
            "本 schema 定义未来 observe-only event logger 应记录的字段。"
            "2334 只定义 schema，不启动 logger。",
            "",
            "- event_id: required",
            "- event_date: required",
            "- target_asset: required",
            f"- known_at_policy: `{event_schema['known_at_policy']}`",
            f"- latency_policy: `{event_schema['latency_policy']}`",
            f"- pit_policy: `{event_schema['pit_policy']}`",
            f"- event_status: `{event_schema['event_status']}`",
            "- manual_review_observation_flag: required",
            "",
            "该 flag 只是研究观察标记，不是减仓建议、rebalance instruction 或 "
            "broker action。",
            "",
        ]
    )


def _render_evidence_doc(
    *,
    evidence_contract: Mapping[str, Any],
    outcome_contract: Mapping[str, Any],
    manual_review_boundary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Actual-Path Evidence Contract",
            "",
            "本 contract 定义每个 high-intensity observe event 后续需要绑定的 "
            "actual-path evidence。未来 outcome 只能在事件发生后填充，不得在 event "
            "creation 时使用未来数据。",
            "",
            f"- horizon_1d_required: `{evidence_contract['horizon_1d_required']}`",
            f"- horizon_5d_required: `{evidence_contract['horizon_5d_required']}`",
            f"- horizon_10d_required: `{evidence_contract['horizon_10d_required']}`",
            f"- horizon_20d_required: `{evidence_contract['horizon_20d_required']}`",
            f"- allowed_horizons: `{outcome_contract['allowed_horizons']}`",
            f"- pit_outcome_binding: `{outcome_contract['pit_outcome_binding']}`",
            f"- manual_review_only: `{manual_review_boundary['manual_review_only']}`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
        ]
    )


def _render_rules_doc(
    *,
    false_warning_framework: Mapping[str, Any],
    stop_continue_archive: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity Stop Continue Archive Rules",
            "",
            "本报告定义 forward observe 后如何评价 high-intensity trigger 的质量，"
            "以及何时继续、调整 threshold、降级 manual-review-only 或 archive。",
            "",
            f"- minimum_observe_event_count: "
            f"`{stop_continue_archive['minimum_observe_event_count']}`",
            f"- minimum_observe_months: "
            f"`{stop_continue_archive['minimum_observe_months']}`",
            f"- minimum_outcome_ready_count: "
            f"`{stop_continue_archive['minimum_outcome_ready_count']}`",
            "",
            "## Metrics",
            "",
            *[f"- `{metric}`" for metric in false_warning_framework["metrics"]],
            "",
            "这些规则只用于 research-only observe line，不允许 promotion、paper-shadow、"
            "production 或 broker action。",
            "",
        ]
    )


def _event_rows_from_alignment(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_date: dict[str, dict[str, Any]] = {}
    for row in rows:
        event_date = str(row.get("date", ""))
        if not event_date:
            continue
        existing = by_date.setdefault(
            event_date,
            {
                "date": event_date,
                "target_assets": set(),
                "risk_cap_triggered": False,
                "risk_cap_intensity": "none",
                "risk_cap_score": 0.0,
                "scope_active": False,
                "signal_direction": "none",
                "decision_timestamp": row.get("decision_timestamp", ""),
                "risk_cap_decision_timestamp": row.get(
                    "risk_cap_decision_timestamp",
                    "",
                ),
                "trigger_source_hash": row.get("trigger_source_hash", ""),
            },
        )
        asset = str(row.get("target_asset", ""))
        if asset:
            existing["target_assets"].add(asset)
        score = to_float(row.get("risk_cap_score"))
        existing["risk_cap_score"] = max(to_float(existing["risk_cap_score"]), score)
        if row.get("risk_cap_triggered") is True:
            existing["risk_cap_triggered"] = True
            existing["scope_active"] = existing["scope_active"] or bool(
                row.get("scope_active")
            )
            intensity = str(row.get("risk_cap_intensity", "none"))
            if _intensity_rank(intensity) >= _intensity_rank(
                str(existing["risk_cap_intensity"])
            ):
                existing["risk_cap_intensity"] = intensity
            signal_direction = str(row.get("signal_direction", "none"))
            if signal_direction != "none":
                existing["signal_direction"] = signal_direction
            risk_timestamp = str(row.get("risk_cap_decision_timestamp", ""))
            if risk_timestamp:
                existing["risk_cap_decision_timestamp"] = risk_timestamp
    return [
        {**row, "target_assets": sorted(row["target_assets"])}
        for row in sorted(by_date.values(), key=lambda item: str(item["date"]))
    ]


def _rows_for_threshold(
    rows: Sequence[Mapping[str, Any]],
    threshold_value: float,
    *,
    composite: bool = False,
) -> list[Mapping[str, Any]]:
    selected = [
        row
        for row in rows
        if row.get("risk_cap_triggered") is True
        and to_float(row.get("risk_cap_score")) >= threshold_value
    ]
    if composite:
        selected = [
            row
            for row in selected
            if row.get("scope_active") is True
            and str(row.get("signal_direction", "none")) != "none"
        ]
    return selected


def _density_for_threshold(
    rows: Sequence[Mapping[str, Any]],
    threshold_value: float,
    *,
    composite: bool = False,
) -> float:
    return round_float(
        len(_rows_for_threshold(rows, threshold_value, composite=composite))
        / len(rows)
        if rows
        else 0.0
    )


def _threshold_recommended_status(
    *,
    threshold_id: str,
    trigger_count: int,
    data_quality_status: str,
    overbinding_label: str,
) -> str:
    if data_quality_status == "FAIL":
        return "BLOCKED"
    if threshold_id == "P90_RISK_CAP_SCORE" and "OVERBINDING" in overbinding_label:
        return "TOO_BROAD_OVERBINDING_RISK"
    if threshold_id == "P95_RISK_CAP_SCORE" and trigger_count < MINIMUM_OBSERVE_EVENT_COUNT:
        return "TOO_NARROW_MISSED_STRESS_RISK"
    if threshold_id == "P95_RISK_CAP_SCORE":
        return "TOO_NARROW_MISSED_STRESS_RISK"
    if threshold_id == "COMPOSITE_HIGH_INTENSITY_RULE":
        return "CANDIDATE_FOR_2335_SELECTION"
    return "INSUFFICIENT_CONTEXT"


def _overbinding_risk(threshold_id: str, overbinding_label: str) -> str:
    if threshold_id == "P90_RISK_CAP_SCORE" and "OVERBINDING" in overbinding_label:
        return "HIGH_FROM_2333_BROAD_MECHANICS_CONTEXT"
    if threshold_id == "COMPOSITE_HIGH_INTENSITY_RULE":
        return "LOWER_THAN_SIMPLE_PERCENTILE"
    return "MEDIUM"


def _missed_stress_risk(threshold_id: str, trigger_count: int) -> str:
    if threshold_id == "P95_RISK_CAP_SCORE":
        return "HIGH_IF_SCORE_SATURATION_DROPS_STRESS_EVENTS"
    if trigger_count < MINIMUM_OBSERVE_EVENT_COUNT:
        return "HIGH_SAMPLE_TOO_SMALL"
    return "MEDIUM"


def _interpretability_score(threshold_id: str) -> float:
    scores = {
        "P90_RISK_CAP_SCORE": 0.8,
        "P95_RISK_CAP_SCORE": 0.8,
        "COMPOSITE_HIGH_INTENSITY_RULE": 0.7,
    }
    return scores.get(threshold_id, 0.5)


def _implementation_complexity(threshold_id: str) -> str:
    if threshold_id == "COMPOSITE_HIGH_INTENSITY_RULE":
        return "MEDIUM"
    return "LOW"


def _status_distribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    distribution: dict[str, int] = {}
    for row in rows:
        status = str(row.get("recommended_status", "UNKNOWN"))
        distribution[status] = distribution.get(status, 0) + 1
    return dict(sorted(distribution.items()))


def _percentile(values: Sequence[float], q: float) -> float:
    clean = sorted(value for value in values if value >= 0.0)
    if not clean:
        return 0.0
    index = round((len(clean) - 1) * q)
    return clean[max(0, min(index, len(clean) - 1))]


def _intensity_rank(value: str) -> int:
    ranks = {"none": 0, "low": 1, "medium": 2, "high": 3}
    return ranks.get(value.lower(), 0)


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityForwardObservePlanError(
                f"{label} required artifact missing: {path}"
            )
        payloads[key] = _load_json(path)
    return payloads


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _first_existing_path(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    return paths[0]


def _validate_no_unsafe_fields(label: str, payload: Any) -> None:
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in {"promotion_allowed", "paper_shadow_allowed", "production_allowed"}:
            if value is True:
                raise HighIntensityForwardObservePlanError(
                    f"{label} unsafe field {'.'.join(path)}=true"
                )
        if key == "broker_action" and str(value).lower() not in {"", "none"}:
            raise HighIntensityForwardObservePlanError(
                f"{label} unsafe broker_action={value}"
            )
        if key in {
            "target_weight_action",
            "target_weight",
            "rebalance_instruction",
            "reduce_position_instruction",
            "increase_cash_instruction",
            "buy_signal",
            "sell_signal",
            "paper_shadow_ready",
            "production_ready",
        } and value not in {False, None, "", "none", "NONE"}:
            raise HighIntensityForwardObservePlanError(
                f"{label} unsafe field {'.'.join(path)}={value}"
            )


def _walk_payload(payload: Any, prefix: tuple[str, ...] = ()) -> list[tuple[tuple[str, ...], Any]]:
    items: list[tuple[tuple[str, ...], Any]] = []
    if isinstance(payload, Mapping):
        for key, value in payload.items():
            items.extend(_walk_payload(value, (*prefix, str(key))))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            items.extend(_walk_payload(value, (*prefix, str(index))))
    else:
        items.append((prefix, payload))
    return items
