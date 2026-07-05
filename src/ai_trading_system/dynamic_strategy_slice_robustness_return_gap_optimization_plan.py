from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.data_foundation import utc_now_iso
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_candidate_optimization_divergence_review import (
    READY_STATUS as SOURCE_2375_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT,
    DEFAULT_SOURCE_CANDIDATE_RANKING_PATH,
    DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH,
    PRIMARY_EXECUTION_CADENCE,
)
from ai_trading_system.dynamic_strategy_cost_turnover_cooldown_sensitivity import (
    READY_STATUS as SOURCE_2366_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_event_driven_retest import (
    READY_STATUS as SOURCE_2365_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    DECISION_CONTINUE_OPTIMIZATION,
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT,
    PRIMARY_CANDIDATE_ID,
)
from ai_trading_system.dynamic_strategy_optimized_candidate_targeted_retest import (
    READY_STATUS as SOURCE_2376_READY_STATUS,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    NEXT_ROUTE as SOURCE_2377_EXPECTED_ROUTE,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    OWNER_DECISION as SOURCE_2377_OWNER_DECISION,
)
from ai_trading_system.dynamic_strategy_targeted_retest_owner_review_decision import (
    READY_STATUS as SOURCE_2377_READY_STATUS,
)
from ai_trading_system.execution_semantics import AI_REGIME_SUMMARY, _file_sha256

TASK_ID = "TRADING-2378"
TASK_REGISTER_ID = (
    "TRADING-2378_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_"
    "OPTIMIZATION_PLAN"
)
REPORT_TYPE = "dynamic_strategy_slice_robustness_return_gap_optimization_plan"
SCHEMA_VERSION = (
    "dynamic_strategy_slice_robustness_return_gap_optimization_plan.v1"
)
READY_STATUS = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_READY"
)
BLOCKED_SOURCE_STATUS = (
    "DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_AND_RETURN_GAP_OPTIMIZATION_PLAN_"
    "BLOCKED_SOURCE_ARTIFACT"
)
NEXT_ROUTE = (
    "TRADING-2379_Dynamic_Strategy_Slice_Robustness_Optimized_Variant_Retest"
)
RANKING_TOP_FALLBACK = "equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1"
DATA_QUALITY_GATE_REASON = (
    "NOT_APPLICABLE_PRIOR_ARTIFACT_OPTIMIZATION_PLAN_ONLY_NO_FRESH_MARKET_DATA"
)
SOURCE_TASKS: tuple[str, ...] = (
    "TRADING-2365",
    "TRADING-2366",
    "TRADING-2375",
    "TRADING-2376",
    "TRADING-2377",
)
PLANNED_VARIANTS: tuple[str, ...] = (
    "dynamic_regime_overlay_v0_4_reentry_repair_v1",
    "dynamic_regime_overlay_v0_4_upside_capture_guarded_v1",
    "dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1",
    "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
    "dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1",
)
FORBIDDEN_OPTIMIZATION_PATHS: tuple[str, ...] = (
    "remove_lower_turnover_guardrail_without_replacement",
    "use_monthly_rebalance_as_primary",
    "allow_stale_signal_carry_forward",
    "increase_growth_tilt_without_risk_cap",
    "remove_cooldown_entirely_as_final_candidate",
    "increase_turnover_without_cost_stress",
    "optimize_only_for_total_return",
    "ignore_drawdown_or_regime_slice_failures",
)
OPTIMIZATION_FOCUS: tuple[str, ...] = (
    "time_slice_robustness_improvement",
    "regime_slice_robustness_improvement",
    "return_gap_repair_vs_ranking_top",
    "upside_capture_without_turnover_increase",
    "valid_until_window_parameter_tuning",
)

DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_strategies" / REPORT_TYPE
)
DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_DOCS_ROOT = (
    PROJECT_ROOT / "docs" / "research"
)
DEFAULT_SOURCE_2365_EVENT_RETEST_PATH = DEFAULT_SOURCE_EVENT_DRIVEN_RETEST_PATH
DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH = DEFAULT_SOURCE_CANDIDATE_RANKING_PATH
DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "sensitivity_result.json"
)
DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_COST_TURNOVER_COOLDOWN_SENSITIVITY_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "divergence_review_result.json"
)
DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_CANDIDATE_OPTIMIZATION_DIVERGENCE_REVIEW_OUTPUT_ROOT
    / "candidate_decision_update.json"
)
DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "targeted_retest_result.json"
)
DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_OPTIMIZED_CANDIDATE_TARGETED_RETEST_OUTPUT_ROOT
    / "decision_update.json"
)
DEFAULT_SOURCE_2377_OWNER_REVIEW_DECISION_PATH = (
    DEFAULT_DYNAMIC_STRATEGY_TARGETED_RETEST_OWNER_REVIEW_DECISION_OUTPUT_ROOT
    / "owner_review_decision.json"
)


def run_dynamic_strategy_slice_robustness_return_gap_optimization_plan(
    *,
    source_event_retest_path: Path = DEFAULT_SOURCE_2365_EVENT_RETEST_PATH,
    source_candidate_ranking_path: Path = DEFAULT_SOURCE_2365_CANDIDATE_RANKING_PATH,
    source_sensitivity_result_path: Path = DEFAULT_SOURCE_2366_SENSITIVITY_RESULT_PATH,
    source_sensitivity_decision_update_path: Path = (
        DEFAULT_SOURCE_2366_SENSITIVITY_DECISION_UPDATE_PATH
    ),
    source_optimization_review_path: Path = DEFAULT_SOURCE_2375_OPTIMIZATION_REVIEW_PATH,
    source_optimization_decision_update_path: Path = (
        DEFAULT_SOURCE_2375_OPTIMIZATION_DECISION_UPDATE_PATH
    ),
    source_targeted_retest_path: Path = DEFAULT_SOURCE_2376_TARGETED_RETEST_PATH,
    source_targeted_decision_update_path: Path = (
        DEFAULT_SOURCE_2376_TARGETED_DECISION_UPDATE_PATH
    ),
    source_owner_review_decision_path: Path = (
        DEFAULT_SOURCE_2377_OWNER_REVIEW_DECISION_PATH
    ),
    output_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_OUTPUT_ROOT
    ),
    docs_root: Path = (
        DEFAULT_DYNAMIC_STRATEGY_SLICE_ROBUSTNESS_RETURN_GAP_OPTIMIZATION_DOCS_ROOT
    ),
    as_of_date: date | None = None,
) -> dict[str, Any]:
    sources = _load_sources(
        source_event_retest_path=source_event_retest_path,
        source_candidate_ranking_path=source_candidate_ranking_path,
        source_sensitivity_result_path=source_sensitivity_result_path,
        source_sensitivity_decision_update_path=(
            source_sensitivity_decision_update_path
        ),
        source_optimization_review_path=source_optimization_review_path,
        source_optimization_decision_update_path=(
            source_optimization_decision_update_path
        ),
        source_targeted_retest_path=source_targeted_retest_path,
        source_targeted_decision_update_path=source_targeted_decision_update_path,
        source_owner_review_decision_path=source_owner_review_decision_path,
    )
    ready = not sources["source_validation_errors"]
    resolved_as_of = _resolve_as_of(as_of_date, sources["owner_review_decision"])
    time_diagnosis = _time_slice_gap_diagnosis(sources)
    regime_diagnosis = _regime_slice_gap_diagnosis(sources)
    return_gap_plan = _return_gap_repair_plan(sources)
    variant_plan = _variant_plan()
    evaluation_plan = _variant_evaluation_plan()
    payload = _base_payload(
        status=READY_STATUS if ready else BLOCKED_SOURCE_STATUS,
        as_of_date=resolved_as_of,
        sources=sources,
        ready=ready,
        time_diagnosis=time_diagnosis,
        regime_diagnosis=regime_diagnosis,
        return_gap_plan=return_gap_plan,
        variant_plan=variant_plan,
        evaluation_plan=evaluation_plan,
    )
    _write_outputs(payload=payload, output_root=output_root, docs_root=docs_root)
    return payload


def _load_sources(
    *,
    source_event_retest_path: Path,
    source_candidate_ranking_path: Path,
    source_sensitivity_result_path: Path,
    source_sensitivity_decision_update_path: Path,
    source_optimization_review_path: Path,
    source_optimization_decision_update_path: Path,
    source_targeted_retest_path: Path,
    source_targeted_decision_update_path: Path,
    source_owner_review_decision_path: Path,
) -> dict[str, Any]:
    event_retest = _load_json_document(source_event_retest_path)
    candidate_ranking = _load_json_document(source_candidate_ranking_path)
    sensitivity_result = _load_json_document(source_sensitivity_result_path)
    sensitivity_decision_update = _load_json_document(
        source_sensitivity_decision_update_path
    )
    optimization_review = _load_json_document(source_optimization_review_path)
    optimization_decision_update = _load_json_document(
        source_optimization_decision_update_path
    )
    targeted_retest = _load_json_document(source_targeted_retest_path)
    targeted_decision_update = _load_json_document(source_targeted_decision_update_path)
    owner_review_decision = _load_json_document(source_owner_review_decision_path)
    source_status = {
        "event_retest": _as_mapping(event_retest).get("status"),
        "candidate_ranking": _as_mapping(candidate_ranking).get("status"),
        "sensitivity_result": _as_mapping(sensitivity_result).get("status"),
        "sensitivity_decision_update": _as_mapping(
            sensitivity_decision_update
        ).get("status"),
        "optimization_review": _as_mapping(optimization_review).get("status"),
        "optimization_decision_update": _as_mapping(
            optimization_decision_update
        ).get("status"),
        "targeted_retest": _as_mapping(targeted_retest).get("status"),
        "targeted_decision_update": _as_mapping(targeted_decision_update).get("status"),
        "owner_review_decision": _as_mapping(owner_review_decision).get("status"),
    }
    primary_candidate = _primary_candidate(targeted_retest, owner_review_decision)
    ranking_top = _ranking_top(
        event_retest=event_retest,
        candidate_ranking=candidate_ranking,
        optimization_review=optimization_review,
        owner_review_decision=owner_review_decision,
        targeted_retest=targeted_retest,
    )
    decision_from_2377 = str(_as_mapping(owner_review_decision).get("owner_decision"))
    decision_from_2376 = _decision_from_2376(
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
    )
    validation_errors = _source_validation_errors(
        source_status=source_status,
        event_retest=event_retest,
        sensitivity_result=sensitivity_result,
        optimization_review=optimization_review,
        targeted_retest=targeted_retest,
        targeted_decision_update=targeted_decision_update,
        owner_review_decision=owner_review_decision,
        primary_candidate=primary_candidate,
        ranking_top=ranking_top,
        decision_from_2376=decision_from_2376,
        decision_from_2377=decision_from_2377,
    )
    return {
        "event_retest": event_retest,
        "candidate_ranking": candidate_ranking,
        "sensitivity_result": sensitivity_result,
        "sensitivity_decision_update": sensitivity_decision_update,
        "optimization_review": optimization_review,
        "optimization_decision_update": optimization_decision_update,
        "targeted_retest": targeted_retest,
        "targeted_decision_update": targeted_decision_update,
        "owner_review_decision": owner_review_decision,
        "source_status": source_status,
        "source_validation_errors": validation_errors,
        "source_ready_for_optimization_plan": not validation_errors,
        "primary_candidate": primary_candidate,
        "ranking_top_reference": ranking_top,
        "decision_from_2376": decision_from_2376,
        "decision_from_2377": decision_from_2377,
        "source_artifacts": {
            "event_retest": _source_artifact(source_event_retest_path, event_retest),
            "candidate_ranking": _source_artifact(
                source_candidate_ranking_path,
                candidate_ranking,
            ),
            "sensitivity_result": _source_artifact(
                source_sensitivity_result_path,
                sensitivity_result,
            ),
            "sensitivity_decision_update": _source_artifact(
                source_sensitivity_decision_update_path,
                sensitivity_decision_update,
            ),
            "optimization_review": _source_artifact(
                source_optimization_review_path,
                optimization_review,
            ),
            "optimization_decision_update": _source_artifact(
                source_optimization_decision_update_path,
                optimization_decision_update,
            ),
            "targeted_retest": _source_artifact(
                source_targeted_retest_path,
                targeted_retest,
            ),
            "targeted_decision_update": _source_artifact(
                source_targeted_decision_update_path,
                targeted_decision_update,
            ),
            "owner_review_decision": _source_artifact(
                source_owner_review_decision_path,
                owner_review_decision,
            ),
        },
    }


def _source_validation_errors(
    *,
    source_status: Mapping[str, Any],
    event_retest: Any,
    sensitivity_result: Any,
    optimization_review: Any,
    targeted_retest: Any,
    targeted_decision_update: Any,
    owner_review_decision: Any,
    primary_candidate: str,
    ranking_top: str,
    decision_from_2376: str,
    decision_from_2377: str,
) -> list[str]:
    errors: list[str] = []
    for key in ("event_retest", "candidate_ranking"):
        if source_status.get(key) != SOURCE_2365_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("sensitivity_result", "sensitivity_decision_update"):
        if source_status.get(key) != SOURCE_2366_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("optimization_review", "optimization_decision_update"):
        if source_status.get(key) != SOURCE_2375_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    for key in ("targeted_retest", "targeted_decision_update"):
        if source_status.get(key) != SOURCE_2376_READY_STATUS:
            errors.append(f"{key}_status_not_ready")
    if source_status.get("owner_review_decision") != SOURCE_2377_READY_STATUS:
        errors.append("owner_review_decision_status_not_ready")
    if primary_candidate != PRIMARY_CANDIDATE_ID:
        errors.append("primary_candidate_not_dynamic_regime_overlay_v0_4_lower_turnover")
    if ranking_top != RANKING_TOP_FALLBACK:
        errors.append("ranking_top_reference_not_expected")
    if decision_from_2376 != DECISION_CONTINUE_OPTIMIZATION:
        errors.append("decision_from_2376_not_continue_optimization")
    if decision_from_2377 != SOURCE_2377_OWNER_DECISION:
        errors.append("decision_from_2377_not_continue_optimization")
    if _as_mapping(owner_review_decision).get("recommended_next_research_task") != (
        SOURCE_2377_EXPECTED_ROUTE
    ):
        errors.append("owner_review_next_route_not_trading_2378")
    if _as_mapping(owner_review_decision).get("continue_optimization_approved") is not True:
        errors.append("continue_optimization_not_approved")
    if _as_mapping(owner_review_decision).get("research_only_observation_approved"):
        errors.append("research_only_observation_approved_true")
    for source_name, source in (
        ("event_retest", event_retest),
        ("sensitivity_result", sensitivity_result),
        ("optimization_review", optimization_review),
        ("targeted_retest", targeted_retest),
        ("targeted_decision_update", targeted_decision_update),
        ("owner_review_decision", owner_review_decision),
    ):
        errors.extend(_side_effect_validation_errors(source_name, _as_mapping(source)))
    for source_name, source in (
        ("event_retest", event_retest),
        ("sensitivity_result", sensitivity_result),
        ("optimization_review", optimization_review),
        ("targeted_retest", targeted_retest),
        ("owner_review_decision", owner_review_decision),
    ):
        if _as_mapping(source).get("primary_execution_cadence") not in (
            None,
            PRIMARY_EXECUTION_CADENCE,
        ):
            errors.append(f"{source_name}_primary_cadence_not_valid_until_window")
    return errors


def _time_slice_gap_diagnosis(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _primary_rows(sources, "time_slice_retest_result")
    rows_by_id = {_as_mapping(row).get("scenario_id"): _as_mapping(row) for row in rows}
    specs = [
        (
            "early_period",
            "early_period_underperformance",
            "early AI-regime response still gives up ranking-top upside.",
            "reentry_repair_v1",
        ),
        (
            "middle_period",
            "middle_period_underperformance",
            "middle slice is acceptable but still monitored for missed signals.",
            "preserve_current_guardrails",
        ),
        (
            "recent_period",
            "recent_period_underperformance",
            "recent low-turnover posture is too defensive versus static and ranking top.",
            "upside_capture_guarded_v1",
        ),
        (
            "post_2023_ai_cycle",
            "post_2023_ai_cycle_underperformance",
            "post-2023 AI cycle remains close but still trails ranking top.",
            "return_gap_repair_fusion_v1",
        ),
        (
            "high_volatility_periods",
            "high_volatility_period_underperformance",
            "high-volatility reaction preserves caution but misses enough upside.",
            "valid_until_decay_tuned_v1",
        ),
        (
            "drawdown_recovery_periods",
            "drawdown_recovery_lag",
            "recovery-phase re-entry is too slow after defensive posture.",
            "reentry_repair_v1",
        ),
    ]
    return [
        _time_diagnosis_row(
            row=rows_by_id.get(scenario_id, {}),
            affected_slice=scenario_id,
            failure_source=failure_source,
            likely_cause=likely_cause,
            proposed_fix=proposed_fix,
        )
        for scenario_id, failure_source, likely_cause, proposed_fix in specs
    ]


def _time_diagnosis_row(
    *,
    row: Mapping[str, Any],
    affected_slice: str,
    failure_source: str,
    likely_cause: str,
    proposed_fix: str,
) -> dict[str, Any]:
    rel = _as_mapping(row.get("relative_metrics"))
    execution = _as_mapping(row.get("execution_metrics"))
    gap_static = _float(rel.get("dynamic_vs_static_gap"))
    gap_ranking = _float(rel.get("dynamic_vs_ranking_top_gap"))
    passed = row.get("slice_passed") is True
    return {
        "failure_source": failure_source,
        "affected_slice": affected_slice,
        "slice_passed_in_2376": passed,
        "underperformance_vs_static": gap_static is not None and gap_static <= 0,
        "underperformance_vs_ranking_top": gap_ranking is not None and gap_ranking < 0,
        "dynamic_vs_static_gap": gap_static,
        "dynamic_vs_ranking_top_gap": gap_ranking,
        "turnover": execution.get("turnover"),
        "fragility_reason": row.get("fragility_reason"),
        "likely_cause": likely_cause,
        "proposed_fix": proposed_fix,
        "expected_turnover_impact": _expected_turnover_impact(proposed_fix),
        "expected_drawdown_impact": _expected_drawdown_impact(proposed_fix),
        "retest_required": (not passed)
        or (gap_static is not None and gap_static <= 0)
        or (gap_ranking is not None and gap_ranking < 0),
    }


def _regime_slice_gap_diagnosis(sources: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _primary_rows(sources, "regime_slice_retest_result")
    rows_by_id = {_as_mapping(row).get("scenario_id"): _as_mapping(row) for row in rows}
    specs = [
        (
            "risk_on",
            "risk_on_too_defensive",
            "Risk-on slice still trails ranking top despite positive static gap.",
            "Allow guarded risk-on weight step when trend and valid-until agree.",
        ),
        (
            "risk_off",
            "risk_off_not_defensive_enough",
            "Risk-off slice is still negative versus static, so risk cap needs preservation.",
            "Preserve risk cap and tune expiry/decay instead of raising gross exposure.",
        ),
        (
            "high_volatility",
            "high_volatility_reaction_too_late",
            "High-volatility reaction misses upside and does not beat static.",
            "Tune valid-until decay and high-volatility confirmation lag.",
        ),
        (
            "low_volatility",
            "low_volatility_upside_capture_insufficient",
            "Low-volatility behavior passes return gap but needs drawdown guard.",
            "Add modest growth tilt only under low volatility with risk cap preserved.",
        ),
        (
            "trend_confirmed",
            "trend_confirmed_reentry_too_slow",
            "Trend-confirmed slice strongly trails ranking top.",
            "Shorten re-entry delay and allow small step-up under confirmed trend.",
        ),
        (
            "trend_uncertain",
            "trend_uncertain_false_signal",
            "Trend-uncertain slice remains negative versus static.",
            "Block stale signals and decay near valid-until expiry.",
        ),
        (
            "recovery",
            "recovery_phase_reentry_lag",
            "Recovery slice shows both static and ranking-top shortfall.",
            "Use reentry repair with cooldown preserved.",
        ),
    ]
    return [
        _regime_diagnosis_row(
            row=rows_by_id.get(scenario_id, {}),
            affected_regime=scenario_id,
            failure_type=failure_type,
            candidate_behavior=candidate_behavior,
            proposed_adjustment=proposed_adjustment,
        )
        for scenario_id, failure_type, candidate_behavior, proposed_adjustment in specs
    ]


def _regime_diagnosis_row(
    *,
    row: Mapping[str, Any],
    affected_regime: str,
    failure_type: str,
    candidate_behavior: str,
    proposed_adjustment: str,
) -> dict[str, Any]:
    rel = _as_mapping(row.get("relative_metrics"))
    gap_static = _float(rel.get("dynamic_vs_static_gap"))
    gap_ranking = _float(rel.get("dynamic_vs_ranking_top_gap"))
    passed = row.get("slice_passed") is True
    return {
        "affected_regime": affected_regime,
        "failure_type": failure_type,
        "slice_passed_in_2376": passed,
        "candidate_behavior": candidate_behavior,
        "desired_behavior": _desired_regime_behavior(affected_regime),
        "dynamic_vs_static_gap": gap_static,
        "dynamic_vs_ranking_top_gap": gap_ranking,
        "fragility_reason": row.get("fragility_reason"),
        "proposed_parameter_adjustment": proposed_adjustment,
        "expected_benefit": _expected_regime_benefit(affected_regime),
        "expected_cost": _expected_regime_cost(affected_regime),
        "retest_required": True,
    }


def _return_gap_repair_plan(sources: Mapping[str, Any]) -> dict[str, Any]:
    decision = _targeted_decision_update(sources)
    full_gap = _float(decision.get("dynamic_vs_ranking_top_gap"))
    components = [
        _return_gap_component(
            "upside_capture_gap",
            full_gap,
            "repairable_with_moderate_turnover_increase",
            "upside_capture_guarded_v1",
            "Improve low-volatility and risk-on upside without removing risk cap.",
        ),
        _return_gap_component(
            "risk_on_weight_gap",
            full_gap,
            "repairable_with_moderate_turnover_increase",
            "return_gap_repair_fusion_v1",
            "Add limited growth tilt only inside valid-until window.",
        ),
        _return_gap_component(
            "reentry_delay_gap",
            full_gap,
            "repairable_without_turnover_increase",
            "reentry_repair_v1",
            "Shorten recovery and trend-confirmed re-entry lag.",
        ),
        _return_gap_component(
            "excessive_defensive_floor_gap",
            full_gap,
            "repair_requires_higher_drawdown_risk",
            "return_gap_repair_fusion_v1",
            "Only repair if drawdown does not materially worsen in 2379.",
        ),
        _return_gap_component(
            "valid_until_expiry_gap",
            full_gap,
            "repairable_without_turnover_increase",
            "valid_until_decay_tuned_v1",
            "Reduce near-expiry drag and block stale signal carry-forward.",
        ),
        _return_gap_component(
            "cooldown_block_gap",
            full_gap,
            "repairable_without_turnover_increase",
            "cooldown_balanced_v1",
            "Tune cooldown/min holding without removing cooldown entirely.",
        ),
        _return_gap_component(
            "turnover_cap_opportunity_cost",
            full_gap,
            "not_recommended_to_repair",
            "preserve_conservative_turnover_cap",
            "Do not raise turnover unless cost stress remains positive.",
        ),
        _return_gap_component(
            "growth_tilt_intensity_gap",
            full_gap,
            "repairable_with_moderate_turnover_increase",
            "upside_capture_guarded_v1",
            "Increase growth tilt modestly only when low-volatility/risk-on filters agree.",
        ),
    ]
    return {
        "schema_version": "dynamic_strategy_return_gap_repair_plan.v1",
        "ranking_top_reference": sources["ranking_top_reference"],
        "primary_candidate": sources["primary_candidate"],
        "full_sample_dynamic_vs_ranking_top_gap": full_gap,
        "return_gap_components": components,
        "repair_plan_ready": True,
        "not_recommended_to_repair": [
            item["component"]
            for item in components
            if item["repair_assessment"] == "not_recommended_to_repair"
        ],
    }


def _return_gap_component(
    component: str,
    full_gap: float | None,
    assessment: str,
    proposed_variant: str,
    rationale: str,
) -> dict[str, Any]:
    return {
        "component": component,
        "observed_full_sample_gap": full_gap,
        "repair_assessment": assessment,
        "repairable_without_turnover_increase": assessment
        == "repairable_without_turnover_increase",
        "repairable_with_moderate_turnover_increase": assessment
        == "repairable_with_moderate_turnover_increase",
        "repair_requires_higher_drawdown_risk": assessment
        == "repair_requires_higher_drawdown_risk",
        "not_recommended_to_repair": assessment == "not_recommended_to_repair",
        "proposed_variant": proposed_variant,
        "rationale": rationale,
        "retest_required": assessment != "not_recommended_to_repair",
    }


def _variant_plan() -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": PRIMARY_CANDIDATE_ID,
            "role": "base_reference",
            "purpose": "current robustness top reference",
            "changes": [],
            "expected_benefit": "reference baseline for 2379",
            "expected_risk": "existing time/regime/return-gap blockers remain",
            "expected_turnover_impact": "none",
            "expected_cost_impact": "none",
            "retest_priority": "reference",
        },
        {
            "candidate_id": "dynamic_regime_overlay_v0_4_reentry_repair_v1",
            "role": "optimized_variant",
            "purpose": "reduce recovery / trend-confirmed reentry lag",
            "changes": [
                "shorten_reentry_delay_after_risk_off",
                "allow_small_weight_step_when_trend_confirmed",
                "preserve_turnover_cap",
            ],
            "expected_benefit": "improve recovery and trend-confirmed slices",
            "expected_risk": "moderate drawdown risk if re-entry is too early",
            "expected_turnover_impact": "low_to_moderate",
            "expected_cost_impact": "low",
            "retest_priority": "high",
        },
        {
            "candidate_id": "dynamic_regime_overlay_v0_4_upside_capture_guarded_v1",
            "role": "optimized_variant",
            "purpose": "improve low-volatility and risk-on upside capture",
            "changes": [
                "modest_growth_tilt_when_volatility_low",
                "growth_tilt_only_when_valid_until_window_active",
                "preserve_risk_cap",
            ],
            "expected_benefit": "reduce upside capture and risk-on weight gap",
            "expected_risk": "possible drawdown increase if growth tilt is too strong",
            "expected_turnover_impact": "moderate",
            "expected_cost_impact": "moderate_cost_stress_required",
            "retest_priority": "high",
        },
        {
            "candidate_id": "dynamic_regime_overlay_v0_4_valid_until_decay_tuned_v1",
            "role": "optimized_variant",
            "purpose": "reduce stale signal and near-expiry drag",
            "changes": [
                "introduce_signal_decay_near_valid_until_expiry",
                "block_signal_after_expiry",
                "preserve_cooldown",
            ],
            "expected_benefit": "improve trend-uncertain and high-volatility behavior",
            "expected_risk": "may miss some late-cycle upside",
            "expected_turnover_impact": "low",
            "expected_cost_impact": "low",
            "retest_priority": "medium",
        },
        {
            "candidate_id": "dynamic_regime_overlay_v0_4_cooldown_balanced_v1",
            "role": "optimized_variant",
            "purpose": "reduce missed upside caused by cooldown without overtrading",
            "changes": [
                "tune_cooldown_days",
                "tune_min_holding_days",
                "preserve_max_single_step_weight_delta",
            ],
            "expected_benefit": "reduce cooldown block gap while preserving turnover profile",
            "expected_risk": "could increase whipsaw if cooldown is too short",
            "expected_turnover_impact": "low_to_moderate",
            "expected_cost_impact": "moderate_cost_stress_required",
            "retest_priority": "medium",
        },
        {
            "candidate_id": "dynamic_regime_overlay_v0_4_return_gap_repair_fusion_v1",
            "role": "optimized_variant",
            "purpose": "combine lower-turnover guardrail with limited growth tilt",
            "changes": [
                "lower_turnover_guardrail",
                "valid_until_window",
                "modest_growth_tilt",
                "risk_cap_preserved",
                "conservative_turnover_cap",
            ],
            "expected_benefit": "largest expected return-gap repair candidate",
            "expected_risk": "highest variant risk; must not degrade drawdown/regime slices",
            "expected_turnover_impact": "moderate",
            "expected_cost_impact": "moderate_to_high_cost_stress_required",
            "retest_priority": "high",
        },
    ]


def _variant_evaluation_plan() -> dict[str, Any]:
    return {
        "schema_version": "dynamic_strategy_variant_evaluation_plan.v1",
        "required_2379_tests": {
            "execution_cadence": [
                "valid_until_window",
                "cooldown_limited_event_driven",
            ],
            "cost_stress": ["base", "realistic", "conservative", "harsh"],
            "slice_tests": [
                "full_available_window",
                "recent_period",
                "post_2023_ai_cycle",
                "high_volatility_periods",
                "drawdown_recovery_periods",
            ],
            "regime_tests": [
                "risk_on",
                "risk_off",
                "high_volatility",
                "low_volatility",
                "trend_confirmed",
                "recovery",
            ],
            "required_comparisons": [
                "static_baseline",
                PRIMARY_CANDIDATE_ID,
                RANKING_TOP_FALLBACK,
            ],
        },
        "acceptance_criteria_for_variant": {
            "must": [
                "outperform_static_on_cost_adjusted_basis",
                "improve_time_slice_robustness_vs_base_candidate",
                "improve_or_preserve_regime_slice_robustness",
                "reduce_return_gap_vs_ranking_top",
                "preserve_lower_turnover_profile",
                "survive_realistic_cost",
                "survive_conservative_cost",
            ],
            "should": [
                "not_materially_worsen_max_drawdown",
                "not_materially_increase_turnover",
                "not_rely_on_single_time_slice",
                "not_degrade_high_volatility_behavior",
            ],
            "must_not": [
                "increase_broker_or_execution_risk",
                "require_scheduler",
                "require_event_append",
                "require_outcome_binding",
                "require_paper_shadow",
            ],
        },
        "variant_evaluation_plan_ready": True,
        "recommended_next_research_task": NEXT_ROUTE,
    }


def _base_payload(
    *,
    status: str,
    as_of_date: date,
    sources: Mapping[str, Any],
    ready: bool,
    time_diagnosis: list[dict[str, Any]],
    regime_diagnosis: list[dict[str, Any]],
    return_gap_plan: Mapping[str, Any],
    variant_plan: list[dict[str, Any]],
    evaluation_plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "task_register_id": TASK_REGISTER_ID,
        "report_type": REPORT_TYPE,
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "generated_at": utc_now_iso(),
        "as_of": as_of_date.isoformat(),
        "source_tasks": list(SOURCE_TASKS),
        "source_artifacts": sources["source_artifacts"],
        "source_status": sources["source_status"],
        "source_ready_for_optimization_plan": ready,
        "source_validation_errors": sources["source_validation_errors"],
        "primary_candidate": sources["primary_candidate"],
        "ranking_top_reference": sources["ranking_top_reference"],
        "decision_from_2376": sources["decision_from_2376"],
        "decision_from_2377": sources["decision_from_2377"],
        "primary_execution_cadence": PRIMARY_EXECUTION_CADENCE,
        "market_regime": "ai_after_chatgpt",
        "market_regime_summary": AI_REGIME_SUMMARY,
        "data_quality_gate_executed": False,
        "data_quality_gate_reason": DATA_QUALITY_GATE_REASON,
        "optimization_plan_ready": ready,
        "time_slice_gap_diagnosis_ready": ready,
        "regime_slice_gap_diagnosis_ready": ready,
        "return_gap_repair_plan_ready": ready,
        "variant_plan_ready": ready,
        "variant_evaluation_plan_ready": ready,
        "time_slice_gap_diagnosis": time_diagnosis,
        "regime_slice_gap_diagnosis": regime_diagnosis,
        "return_gap_repair_plan": dict(return_gap_plan),
        "variant_plan": variant_plan,
        "planned_variants": list(PLANNED_VARIANTS),
        "forbidden_optimization_paths": list(FORBIDDEN_OPTIMIZATION_PATHS),
        "variant_evaluation_plan": dict(evaluation_plan),
        "research_only": True,
        "observe_only": True,
        "optimization_plan_only": True,
        "backtest_run": False,
        "fresh_market_data_read": False,
        "manual_review_required": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "paper_trade_created": False,
        "shadow_position_created": False,
        "scheduler_enabled": False,
        "scheduled_task_created": False,
        "event_append_enabled": False,
        "historical_event_log_mutated": False,
        "outcome_binding_enabled": False,
        "outcome_store_mutated": False,
        "production_allowed": False,
        "production_enabled": False,
        "production_effect": "none",
        "broker_action": "none",
        "broker_action_enabled": False,
        "order_generated": False,
        "daily_report_generated": False,
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "summary_findings": {
            "optimization_focus": list(OPTIMIZATION_FOCUS),
            "paper_shadow_remains_disabled": True,
            "valid_until_window_remains_default": True,
            "planned_variant_count": len(PLANNED_VARIANTS),
            "time_slice_gap_count": len(
                [item for item in time_diagnosis if item["retest_required"]]
            ),
            "regime_slice_gap_count": len(
                [item for item in regime_diagnosis if item["retest_required"]]
            ),
            "return_gap_repair_plan_ready": ready,
            "recommended_next_research_task": NEXT_ROUTE,
        },
        "required_outputs_ready": _required_outputs_ready(ready),
        "artifact_paths": {},
    }


def _write_outputs(
    *,
    payload: dict[str, Any],
    output_root: Path,
    docs_root: Path,
) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    docs_root.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "json_path": str(output_root / "optimization_plan_result.json"),
        "time_regime_slice_gap_diagnosis_json": str(
            output_root / "time_regime_slice_gap_diagnosis.json"
        ),
        "return_gap_repair_variant_plan_json": str(
            output_root / "return_gap_repair_variant_plan.json"
        ),
        "variant_evaluation_plan_json": str(
            output_root / "variant_evaluation_plan.json"
        ),
        "markdown_path": str(
            docs_root
            / "dynamic_strategy_slice_robustness_return_gap_optimization_plan.md"
        ),
        "slice_diagnosis_markdown": str(
            docs_root / "dynamic_strategy_time_regime_slice_gap_diagnosis.md"
        ),
        "variant_plan_markdown": str(
            docs_root / "dynamic_strategy_return_gap_repair_variant_plan.md"
        ),
        "next_route_markdown": str(docs_root / "dynamic_strategy_2379_route.md"),
    }
    payload["artifact_paths"] = artifact_paths
    _write_json(Path(artifact_paths["json_path"]), payload)
    _write_json(
        Path(artifact_paths["time_regime_slice_gap_diagnosis_json"]),
        {
            "report_type": "dynamic_strategy_time_regime_slice_gap_diagnosis",
            "schema_version": "dynamic_strategy_time_regime_slice_gap_diagnosis.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "time_slice_gap_diagnosis": payload["time_slice_gap_diagnosis"],
            "regime_slice_gap_diagnosis": payload["regime_slice_gap_diagnosis"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["return_gap_repair_variant_plan_json"]),
        {
            "report_type": "dynamic_strategy_return_gap_repair_variant_plan",
            "schema_version": "dynamic_strategy_return_gap_repair_variant_plan.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "return_gap_repair_plan": payload["return_gap_repair_plan"],
            "variant_plan": payload["variant_plan"],
            "forbidden_optimization_paths": payload["forbidden_optimization_paths"],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    _write_json(
        Path(artifact_paths["variant_evaluation_plan_json"]),
        {
            "report_type": "dynamic_strategy_variant_evaluation_plan",
            "schema_version": "dynamic_strategy_variant_evaluation_plan.v1",
            "status": payload["status"],
            "generated_at": payload["generated_at"],
            "variant_evaluation_plan": payload["variant_evaluation_plan"],
            "recommended_next_research_task": payload[
                "recommended_next_research_task"
            ],
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    Path(artifact_paths["markdown_path"]).write_text(
        _main_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["slice_diagnosis_markdown"]).write_text(
        _slice_diagnosis_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["variant_plan_markdown"]).write_text(
        _variant_plan_markdown(payload),
        encoding="utf-8",
    )
    Path(artifact_paths["next_route_markdown"]).write_text(
        _route_markdown(payload),
        encoding="utf-8",
    )


def _main_markdown(payload: Mapping[str, Any]) -> str:
    summary = _as_mapping(payload.get("summary_findings"))
    return "\n".join(
        [
            "# 动态策略 slice robustness and return-gap optimization plan",
            "",
            "## Executive summary",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- ranking top reference：`{payload.get('ranking_top_reference')}`",
            f"- decision from 2377：`{payload.get('decision_from_2377')}`",
            f"- primary execution cadence：`{payload.get('primary_execution_cadence')}`",
            f"- planned variant count：`{summary.get('planned_variant_count')}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            "",
            "## Source findings from TRADING-2376 / 2377",
            "",
            "- 2376 decision remained `CONTINUE_OPTIMIZATION`.",
            "- 2377 owner decision kept the line research-only and approved optimization.",
            "- 2376 did not justify research-only observation or paper-shadow.",
            "",
            "## Why optimization continues",
            "",
            "- cost stress remained positive, so the candidate is not discarded.",
            "- time/regime slice robustness failed, so observation is not approved.",
            "- return gap vs ranking top remains negative, so variant retest is required.",
            "",
            "## Time-slice gap diagnosis",
            "",
            _diagnosis_table(payload, "time_slice_gap_diagnosis", "affected_slice"),
            "",
            "## Regime-slice gap diagnosis",
            "",
            _diagnosis_table(payload, "regime_slice_gap_diagnosis", "affected_regime"),
            "",
            "## Return gap decomposition",
            "",
            _return_gap_table(payload),
            "",
            "## Variant plan",
            "",
            _variant_table(payload),
            "",
            "## Forbidden optimization paths",
            "",
            "\n".join(
                f"- `{item}`"
                for item in _as_list(payload.get("forbidden_optimization_paths"))
            ),
            "",
            "## 2379 evaluation plan",
            "",
            "- variants must beat static on cost-adjusted basis.",
            "- variants must improve time-slice robustness vs base candidate.",
            "- variants must improve or preserve regime-slice robustness.",
            "- variants must reduce return gap vs ranking top.",
            (
                "- variants must preserve lower-turnover profile and survive "
                "realistic/conservative cost."
            ),
            "",
            "## Explicit non-goals",
            "",
            "- 不启用 scheduler，不创建 scheduled task。",
            "- 不 append event，不 bind outcome，不 mutate outcome store。",
            "- 不启用 paper-shadow，不创建 paper trade 或 shadow position。",
            "- 不进入 production，不调用 broker，不生成 order。",
            "- 不生成 daily report。",
            "",
            "## Recommended next route",
            "",
            f"`{payload.get('recommended_next_research_task')}`",
        ]
    )


def _slice_diagnosis_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 time/regime slice gap diagnosis",
            "",
            f"- status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            "",
            "## Time slices",
            "",
            _diagnosis_table(payload, "time_slice_gap_diagnosis", "affected_slice"),
            "",
            "## Regime slices",
            "",
            _diagnosis_table(payload, "regime_slice_gap_diagnosis", "affected_regime"),
        ]
    )


def _variant_plan_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# 动态策略 return-gap repair variant plan",
            "",
            f"- status：`{payload.get('status')}`",
            f"- ranking top reference：`{payload.get('ranking_top_reference')}`",
            "",
            "## Return gap components",
            "",
            _return_gap_table(payload),
            "",
            "## Variants for 2379",
            "",
            _variant_table(payload),
            "",
            "## Forbidden paths",
            "",
            "\n".join(
                f"- `{item}`"
                for item in _as_list(payload.get("forbidden_optimization_paths"))
            ),
        ]
    )


def _route_markdown(payload: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# TRADING-2379 route",
            "",
            f"- current status：`{payload.get('status')}`",
            f"- primary candidate：`{payload.get('primary_candidate')}`",
            f"- planned variants：`{', '.join(_as_list(payload.get('planned_variants')))}`",
            f"- next route：`{payload.get('recommended_next_research_task')}`",
            (
                "- route boundary：slice-robustness optimized variant retest；"
                "不是 observation、paper-shadow、scheduler、production、broker "
                "或 daily report approval。"
            ),
        ]
    )


def _diagnosis_table(payload: Mapping[str, Any], key: str, label: str) -> str:
    lines = [
        f"|{label}|issue|gap_static|gap_ranking|fix|turnover|retest|",
        "|---|---|---:|---:|---|---|---|",
    ]
    for row in _as_list(payload.get(key)):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get(label)}`",
                    f"`{item.get('failure_source') or item.get('failure_type')}`",
                    _fmt(item.get("dynamic_vs_static_gap")),
                    _fmt(item.get("dynamic_vs_ranking_top_gap")),
                    f"`{item.get('proposed_fix') or item.get('proposed_parameter_adjustment')}`",
                    f"`{item.get('expected_turnover_impact') or item.get('expected_cost')}`",
                    f"`{item.get('retest_required')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _return_gap_table(payload: Mapping[str, Any]) -> str:
    components = _as_list(
        _as_mapping(payload.get("return_gap_repair_plan")).get(
            "return_gap_components"
        )
    )
    lines = ["|component|assessment|variant|retest|", "|---|---|---|---|"]
    for row in components:
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('component')}`",
                    f"`{item.get('repair_assessment')}`",
                    f"`{item.get('proposed_variant')}`",
                    f"`{item.get('retest_required')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _variant_table(payload: Mapping[str, Any]) -> str:
    lines = ["|variant|purpose|turnover|cost|priority|", "|---|---|---|---|---|"]
    for row in _as_list(payload.get("variant_plan")):
        item = _as_mapping(row)
        lines.append(
            "|"
            + "|".join(
                [
                    f"`{item.get('candidate_id')}`",
                    str(item.get("purpose")),
                    f"`{item.get('expected_turnover_impact')}`",
                    f"`{item.get('expected_cost_impact')}`",
                    f"`{item.get('retest_priority')}`",
                ]
            )
            + "|"
        )
    return "\n".join(lines)


def _primary_rows(sources: Mapping[str, Any], key: str) -> list[Any]:
    targeted = _as_mapping(sources["targeted_retest"])
    primary = sources["primary_candidate"]
    return [
        row
        for row in _as_list(targeted.get(key))
        if _as_mapping(row).get("candidate_id") == primary
    ]


def _targeted_decision_update(sources: Mapping[str, Any]) -> Mapping[str, Any]:
    targeted = _as_mapping(sources["targeted_retest"])
    decision_doc = _as_mapping(sources["targeted_decision_update"])
    return _as_mapping(
        _first_present(targeted.get("decision_update"), decision_doc.get("decision_update"))
    )


def _side_effect_validation_errors(
    source_name: str,
    source: Mapping[str, Any],
) -> list[str]:
    errors: list[str] = []
    for field in _side_effect_false_fields():
        if bool(source.get(field)):
            errors.append(f"{source_name}_{field}_true")
    for field in (
        "paper_shadow_allowed",
        "production_allowed",
        "promotion_allowed",
        "research_only_observation_approved",
    ):
        if source.get(field) is True:
            errors.append(f"{source_name}_{field}_true")
    if source.get("broker_action") not in (None, "none"):
        errors.append(f"{source_name}_broker_action_not_none")
    if source.get("production_effect") not in (None, "none"):
        errors.append(f"{source_name}_production_effect_not_none")
    return errors


def _side_effect_false_fields() -> tuple[str, ...]:
    return (
        "paper_shadow_enabled",
        "paper_trade_created",
        "shadow_position_created",
        "event_append_enabled",
        "event_append_attempted",
        "historical_event_log_mutated",
        "outcome_binding_enabled",
        "outcome_binding_attempted",
        "outcome_store_mutated",
        "scheduler_enabled",
        "scheduled_task_created",
        "production_enabled",
        "broker_action_enabled",
        "broker_action_attempted",
        "order_generated",
        "daily_report_generated",
    )


def _required_outputs_ready(ready: bool) -> dict[str, bool]:
    return {
        "optimization_plan": ready,
        "time_slice_gap_diagnosis": ready,
        "regime_slice_gap_diagnosis": ready,
        "return_gap_repair_plan": ready,
        "variant_plan": ready,
        "variant_evaluation_plan": ready,
        "planned_variants_nonempty": ready,
        "forbidden_optimization_paths": ready,
        "scheduler_enabled_false": ready,
        "event_append_enabled_false": ready,
        "outcome_binding_enabled_false": ready,
        "paper_shadow_enabled_false": ready,
        "production_enabled_false": ready,
        "broker_action_enabled_false": ready,
        "daily_report_generated_false": ready,
        "next_route": ready,
    }


def _primary_candidate(targeted_retest: Any, owner_review_decision: Any) -> str:
    return str(
        _first_present(
            _as_mapping(owner_review_decision).get("primary_candidate"),
            _as_mapping(targeted_retest).get("primary_candidate"),
            PRIMARY_CANDIDATE_ID,
        )
    )


def _ranking_top(
    *,
    event_retest: Any,
    candidate_ranking: Any,
    optimization_review: Any,
    owner_review_decision: Any,
    targeted_retest: Any,
) -> str:
    return str(
        _first_present(
            _as_mapping(owner_review_decision).get("ranking_top_from_2365"),
            _as_mapping(targeted_retest).get("ranking_top_from_2365"),
            _as_mapping(optimization_review).get("ranking_top_from_2365"),
            _extract_ranked_candidate(
                _as_list(_as_mapping(candidate_ranking).get("candidate_ranking")),
                rank_field="rank",
            ),
            _extract_ranked_candidate(
                _as_list(_as_mapping(event_retest).get("candidate_ranking")),
                rank_field="rank",
            ),
            RANKING_TOP_FALLBACK,
        )
    )


def _decision_from_2376(*, targeted_retest: Any, targeted_decision_update: Any) -> str:
    decision = _as_mapping(
        _first_present(
            _as_mapping(targeted_retest).get("decision_update"),
            _as_mapping(targeted_decision_update).get("decision_update"),
        )
    )
    return str(
        _first_present(
            _as_mapping(targeted_retest).get("candidate_decision_after_targeted_retest"),
            decision.get("candidate_decision_after_targeted_retest"),
            "",
        )
    )


def _extract_ranked_candidate(rows: list[Any], *, rank_field: str) -> str | None:
    best_row: Mapping[str, Any] | None = None
    best_rank: float | None = None
    for row in rows:
        item = _as_mapping(row)
        candidate_id = item.get("candidate_id")
        rank = _float(item.get(rank_field))
        if not candidate_id or rank is None:
            continue
        if best_rank is None or rank < best_rank:
            best_rank = rank
            best_row = item
    if best_row is None:
        return None
    return str(best_row.get("candidate_id"))


def _expected_turnover_impact(proposed_fix: str) -> str:
    return {
        "reentry_repair_v1": "low_to_moderate",
        "upside_capture_guarded_v1": "moderate",
        "valid_until_decay_tuned_v1": "low",
        "cooldown_balanced_v1": "low_to_moderate",
        "return_gap_repair_fusion_v1": "moderate",
        "preserve_current_guardrails": "none",
    }.get(proposed_fix, "unknown")


def _expected_drawdown_impact(proposed_fix: str) -> str:
    return {
        "reentry_repair_v1": "watch_for_moderate_drawdown_increase",
        "upside_capture_guarded_v1": "watch_growth_tilt_drawdown",
        "valid_until_decay_tuned_v1": "likely_preserve_or_improve",
        "cooldown_balanced_v1": "watch_whipsaw_drawdown",
        "return_gap_repair_fusion_v1": "highest_drawdown_watch",
        "preserve_current_guardrails": "none",
    }.get(proposed_fix, "unknown")


def _desired_regime_behavior(regime: str) -> str:
    return {
        "risk_on": "capture more upside without dropping lower-turnover guardrail",
        "risk_off": "stay defensive and avoid static underperformance",
        "high_volatility": "react earlier while preserving cost discipline",
        "low_volatility": "allow modest guarded upside capture",
        "trend_confirmed": "re-enter faster with small steps",
        "trend_uncertain": "block stale or ambiguous signals",
        "recovery": "reduce recovery re-entry lag",
    }.get(regime, "preserve robustness while reducing return gap")


def _expected_regime_benefit(regime: str) -> str:
    return {
        "risk_on": "reduce risk-on weight gap",
        "risk_off": "preserve defensive robustness",
        "high_volatility": "reduce late reaction cost",
        "low_volatility": "improve upside capture",
        "trend_confirmed": "reduce ranking-top return gap",
        "trend_uncertain": "reduce false signal drag",
        "recovery": "reduce recovery lag",
    }.get(regime, "improve regime robustness")


def _expected_regime_cost(regime: str) -> str:
    return {
        "risk_on": "moderate turnover and drawdown watch",
        "risk_off": "possible missed upside if too defensive",
        "high_volatility": "cost stress required",
        "low_volatility": "growth tilt drawdown watch",
        "trend_confirmed": "small turnover increase",
        "trend_uncertain": "possible missed late signal",
        "recovery": "whipsaw risk if re-entry is early",
    }.get(regime, "requires 2379 retest")


def _source_artifact(path: Path, payload: Any) -> dict[str, Any]:
    return {
        "path": str(path),
        "status": _as_mapping(payload).get("status"),
        "sha256": _file_sha256(path),
        "size_bytes": path.stat().st_size,
    }


def _resolve_as_of(as_of_date: date | None, source: Mapping[str, Any]) -> date:
    if as_of_date is not None:
        return as_of_date
    raw = source.get("as_of")
    if isinstance(raw, str):
        try:
            return date.fromisoformat(raw[:10])
        except ValueError:
            pass
    return date.today()


def _load_json_document(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    numeric = _float(value)
    if numeric is None:
        return ""
    return f"{numeric:.6f}".rstrip("0").rstrip(".")
