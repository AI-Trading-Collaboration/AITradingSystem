from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
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
from ai_trading_system.high_intensity_risk_cap_forward_observe_plan import (
    DEFAULT_OUTPUT_ROOT as DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
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

TASK_ID = "TRADING-2335_HIGH_INTENSITY_RISK_CAP_THRESHOLD_SELECTION"
REPORT_TYPE = "high_intensity_risk_cap_threshold_selection"
ARTIFACT_ROLE = "high_intensity_risk_cap_threshold_selection"
MODE = "threshold_selection"
STATUS = "HIGH_INTENSITY_THRESHOLD_SELECTION_READY_PROMOTION_BLOCKED"
STATUS_WITH_WARNINGS = (
    "HIGH_INTENSITY_THRESHOLD_SELECTION_READY_WITH_WARNINGS_PROMOTION_BLOCKED"
)
BLOCKED_STATUS = "HIGH_INTENSITY_THRESHOLD_SELECTION_BLOCKED_PROMOTION_BLOCKED"
DATA_VALIDATION_POLICY = (
    "NOT_APPLICABLE_PRIOR_VALIDATED_2332_2333_2334_ARTIFACTS_ONLY"
)
EXPECTED_2334_STATUS = "HIGH_INTENSITY_FORWARD_OBSERVE_PLAN_READY_PROMOTION_BLOCKED"
EXPECTED_2334_READINESS = "THRESHOLD_SELECTION_REQUIRED"
EXPECTED_2335_TASK = "TRADING-2335_High_Intensity_Risk_Cap_Threshold_Selection"
NEXT_EVENT_LOGGER_TASK = (
    "TRADING-2336_High_Intensity_Risk_Cap_Forward_Observe_Event_Logger"
)
NEXT_THRESHOLD_REMEDIATION_TASK = (
    "TRADING-2336_High_Intensity_Risk_Cap_Threshold_Candidate_Remediation"
)
NEXT_DATA_REMEDIATION_TASK = (
    "TRADING-2336_High_Intensity_Risk_Cap_Data_Contract_Remediation"
)
NEXT_ARCHIVE_TASK = "TRADING-2336_Archive_High_Intensity_Risk_Cap_Observe_Line"
KNOWN_AT_POLICY = "NEXT_SESSION_DECISION_POLICY"
LATENCY_POLICY = "NEXT_TRADING_DAY_DECISION"
PIT_POLICY = "PIT_APPROXIMATION_READY"

DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_trends" / REPORT_TYPE
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

MAX_TRIGGER_DENSITY = 0.10
DENSITY_WARNING_THRESHOLD = 0.08
DENSITY_BLOCKING_THRESHOLD = 0.12
MAX_MONTHLY_EVENT_COUNT = 3
MAX_CONSECUTIVE_TRIGGER_DAYS = 5
MIN_EXPECTED_EVENT_COUNT_FOR_OBSERVE = 20

SAFETY_FIELDS: dict[str, Any] = {
    "research_only": True,
    "threshold_selection_only": True,
    "forward_observe_line": True,
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


class HighIntensityThresholdSelectionError(ValueError):
    pass


def run_high_intensity_risk_cap_threshold_selection(
    *,
    forward_observe_plan_dir: Path = DEFAULT_FORWARD_OBSERVE_PLAN_ROOT,
    dynamic_diagnostics_dir: Path = DEFAULT_DYNAMIC_DIAGNOSTICS_ROOT,
    dynamic_dry_run_dir: Path = DEFAULT_DYNAMIC_DRY_RUN_ROOT,
    readiness_dir: Path = DEFAULT_READINESS_ROOT,
    timestamp_remediation_dir: Path = DEFAULT_TIMESTAMP_REMEDIATION_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
    mode: str = MODE,
) -> dict[str, Any]:
    if mode != MODE:
        raise HighIntensityThresholdSelectionError(
            f"high-intensity threshold selection only supports {MODE} mode"
        )

    generated_at = datetime.now(tz=UTC).replace(microsecond=0)
    inputs = load_high_intensity_threshold_selection_inputs(
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
    )
    candidate_rows = records(
        inputs["forward_observe_plan"]["threshold_candidate_matrix"].get("rows")
    )
    alignment_rows = _event_rows_from_alignment(
        records(inputs["dynamic_dry_run"]["trigger_alignment"].get("rows"))
    )
    scoring_rows = build_high_intensity_threshold_candidate_scoring_matrix(
        candidate_rows=candidate_rows,
        diagnostics=inputs["dynamic_diagnostics"],
    )
    selected_candidate = select_high_intensity_threshold_candidate(scoring_rows)
    selected_event_rows = _rows_for_selected_candidate(
        alignment_rows,
        selected_candidate,
    )
    guardrail = build_high_intensity_trigger_density_guardrail(
        selected_candidate=selected_candidate,
        selected_event_rows=selected_event_rows,
    )
    decision = build_high_intensity_threshold_selection_decision_matrix(
        scoring_rows=scoring_rows,
        selected_candidate=selected_candidate,
        guardrail=guardrail,
    )

    paths = _build_output_paths(output_dir=output_dir, docs_root=docs_root)
    selected_rule = build_high_intensity_selected_trigger_rule(
        selected_candidate=selected_candidate,
        guardrail=guardrail,
        output_path=paths["selected_rule_json"],
    )
    selected_contract = build_high_intensity_selected_trigger_contract(
        selected_rule=selected_rule,
        selected_rule_path=paths["selected_rule_json"],
    )
    caveat_report = build_high_intensity_threshold_selection_caveat_report(
        selected_candidate=selected_candidate,
        guardrail=guardrail,
    )
    event_logger_contract = build_high_intensity_event_logger_input_contract(
        selected_rule=selected_rule,
        selected_rule_path=paths["selected_rule_json"],
        forward_observe_plan=inputs["forward_observe_plan"],
    )
    backtest_context = build_high_intensity_selected_rule_backtest_context(
        selected_candidate=selected_candidate,
        selected_event_rows=selected_event_rows,
        diagnostics=inputs["dynamic_diagnostics"],
        dynamic_dry_run=inputs["dynamic_dry_run"],
    )
    false_warning_context = build_high_intensity_selected_rule_false_warning_context(
        selected_candidate=selected_candidate,
        diagnostics=inputs["dynamic_diagnostics"],
    )
    missed_stress_context = build_high_intensity_selected_rule_missed_stress_context(
        selected_candidate=selected_candidate,
    )
    manual_review_boundary = (
        build_high_intensity_selected_rule_manual_review_boundary()
    )
    safety_boundary = build_high_intensity_threshold_selection_safety_boundary(
        generated_at=generated_at,
        decision=decision,
    )
    readiness = build_high_intensity_2336_readiness_checklist(
        decision=decision,
        guardrail=guardrail,
        selected_rule=selected_rule,
        selected_contract=selected_contract,
        event_logger_contract=event_logger_contract,
        safety_boundary=safety_boundary,
    )
    task_route = build_high_intensity_2336_task_route(
        readiness=readiness,
        decision=decision,
    )
    summary = build_high_intensity_threshold_selection_summary(
        generated_at=generated_at,
        forward_observe_plan_dir=forward_observe_plan_dir,
        dynamic_diagnostics_dir=dynamic_diagnostics_dir,
        dynamic_dry_run_dir=dynamic_dry_run_dir,
        readiness_dir=readiness_dir,
        timestamp_remediation_dir=timestamp_remediation_dir,
        forward_observe_plan=inputs["forward_observe_plan"],
        dynamic_diagnostics=inputs["dynamic_diagnostics"],
        dynamic_dry_run=inputs["dynamic_dry_run"],
        scoring_rows=scoring_rows,
        selected_candidate=selected_candidate,
        guardrail=guardrail,
        decision=decision,
        readiness=readiness,
        task_route=task_route,
    )
    artifact_paths = write_high_intensity_threshold_selection_outputs(
        output_dir=output_dir,
        docs_root=docs_root,
        paths=paths,
        summary=summary,
        scoring_rows=scoring_rows,
        guardrail=guardrail,
        decision=decision,
        selected_rule=selected_rule,
        selected_contract=selected_contract,
        caveat_report=caveat_report,
        event_logger_contract=event_logger_contract,
        backtest_context=backtest_context,
        false_warning_context=false_warning_context,
        missed_stress_context=missed_stress_context,
        manual_review_boundary=manual_review_boundary,
        readiness=readiness,
        task_route=task_route,
        safety_boundary=safety_boundary,
    )
    return clean_for_yaml({**summary, "artifact_paths": artifact_paths})


def load_high_intensity_threshold_selection_inputs(
    *,
    forward_observe_plan_dir: Path,
    dynamic_diagnostics_dir: Path,
    dynamic_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
) -> dict[str, Any]:
    return {
        "forward_observe_plan": load_trading_2334_forward_observe_plan_outputs(
            forward_observe_plan_dir
        ),
        "dynamic_diagnostics": load_trading_2333_dynamic_diagnostics_context(
            dynamic_diagnostics_dir
        ),
        "dynamic_dry_run": load_trading_2332_dynamic_dry_run_context(
            dynamic_dry_run_dir
        ),
        "readiness": load_trading_2331_readiness_context(readiness_dir),
        "timestamp_remediation": load_trading_2330_timestamp_context(
            timestamp_remediation_dir
        ),
    }


def load_trading_2334_forward_observe_plan_outputs(
    forward_observe_plan_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": forward_observe_plan_dir
        / "high_intensity_forward_observe_plan_summary.json",
        "selection_criteria": forward_observe_plan_dir
        / "high_intensity_trigger_selection_criteria.json",
        "threshold_candidate_matrix": forward_observe_plan_dir
        / "high_intensity_trigger_threshold_candidate_matrix.json",
        "candidate_backtest_context": forward_observe_plan_dir
        / "high_intensity_trigger_candidate_backtest_context.json",
        "event_schema": forward_observe_plan_dir
        / "high_intensity_forward_observe_event_schema.json",
        "evidence_contract": forward_observe_plan_dir
        / "high_intensity_forward_observe_evidence_contract.json",
        "actual_path_contract": forward_observe_plan_dir
        / "high_intensity_actual_path_outcome_contract.json",
        "manual_review_boundary": forward_observe_plan_dir
        / "high_intensity_manual_review_boundary.json",
        "false_warning_missed_stress": forward_observe_plan_dir
        / "high_intensity_false_warning_missed_stress_framework.json",
        "stop_continue_archive": forward_observe_plan_dir
        / "high_intensity_stop_continue_archive_rules.json",
        "readiness_checklist": forward_observe_plan_dir
        / "high_intensity_observe_readiness_checklist.json",
        "task_route": forward_observe_plan_dir / "high_intensity_2335_task_route.json",
        "safety_boundary": forward_observe_plan_dir
        / "high_intensity_forward_observe_safety_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2334 forward observe plan")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2334 {key}", payload)
    summary = mapping(payloads["summary"])
    route = mapping(payloads["task_route"])
    matrix = mapping(payloads["threshold_candidate_matrix"])
    if str(summary.get("status", "")) != EXPECTED_2334_STATUS:
        raise HighIntensityThresholdSelectionError(
            f"TRADING-2335 requires TRADING-2334 status {EXPECTED_2334_STATUS}"
        )
    if str(summary.get("readiness_status", "")) != EXPECTED_2334_READINESS:
        raise HighIntensityThresholdSelectionError(
            "TRADING-2334 readiness_status must be THRESHOLD_SELECTION_REQUIRED"
        )
    if str(summary.get("next_task", "")) != EXPECTED_2335_TASK:
        raise HighIntensityThresholdSelectionError(
            f"TRADING-2334 summary must route to {EXPECTED_2335_TASK}"
        )
    if str(route.get("next_task", "")) != EXPECTED_2335_TASK:
        raise HighIntensityThresholdSelectionError(
            f"TRADING-2334 task route must be {EXPECTED_2335_TASK}"
        )
    if summary.get("runtime_observe_started") is True:
        raise HighIntensityThresholdSelectionError(
            "TRADING-2334 runtime_observe_started must be false"
        )
    if "rows" not in matrix:
        raise HighIntensityThresholdSelectionError(
            "TRADING-2334 threshold candidate matrix missing rows"
        )
    return {
        "source_dir": str(forward_observe_plan_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2333_dynamic_diagnostics_context(
    dynamic_diagnostics_dir: Path,
) -> dict[str, Any]:
    paths = {
        "summary": dynamic_diagnostics_dir
        / "dynamic_exposure_cap_diagnostics_review_summary.json",
        "cap_binding": dynamic_diagnostics_dir
        / "dynamic_cap_binding_diagnostics_matrix.json",
        "overbinding": dynamic_diagnostics_dir / "dynamic_overbinding_diagnostics.json",
        "return_drawdown": dynamic_diagnostics_dir
        / "dynamic_return_drawdown_tradeoff_diagnostics.json",
        "false_cost_missed_upside": dynamic_diagnostics_dir
        / "dynamic_false_cost_missed_upside_diagnostics.json",
        "downside": dynamic_diagnostics_dir
        / "dynamic_downside_protection_diagnostics.json",
        "strategy_overlap": dynamic_diagnostics_dir
        / "dynamic_strategy_overlap_diagnostics.json",
        "static_dynamic": dynamic_diagnostics_dir
        / "static_vs_dynamic_exposure_cap_evidence_comparison.json",
        "policy_sensitivity": dynamic_diagnostics_dir
        / "dynamic_policy_sensitivity_recommendation_matrix.json",
        "decision": dynamic_diagnostics_dir / "dynamic_exposure_cap_decision_matrix.json",
        "task_route": dynamic_diagnostics_dir / "dynamic_2334_task_route.json",
        "interpretation_boundary": dynamic_diagnostics_dir
        / "dynamic_exposure_cap_interpretation_boundary.json",
    }
    payloads = _load_required_payloads(paths, "TRADING-2333 diagnostics context")
    for key, payload in payloads.items():
        _validate_no_unsafe_fields(f"TRADING-2333 {key}", payload)
    summary = mapping(payloads["summary"])
    if str(summary.get("overall_recommendation", "")) != (
        "HIGH_INTENSITY_ONLY_FORWARD_OBSERVE"
    ):
        raise HighIntensityThresholdSelectionError(
            "TRADING-2335 requires TRADING-2333 high-intensity-only recommendation"
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
    if str(boundary.get("known_at_policy", "")) != KNOWN_AT_POLICY:
        raise HighIntensityThresholdSelectionError(
            "TRADING-2335 requires NEXT_SESSION_DECISION_POLICY"
        )
    return {
        "source_dir": str(dynamic_dry_run_dir),
        "paths": {key: str(path) for key, path in paths.items()},
        **payloads,
    }


def load_trading_2331_readiness_context(readiness_dir: Path) -> dict[str, Any]:
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


def load_trading_2330_timestamp_context(timestamp_remediation_dir: Path) -> dict[str, Any]:
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


def build_high_intensity_threshold_candidate_scoring_matrix(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    diagnostics: Mapping[str, Any],
) -> list[dict[str, Any]]:
    false_cost = mapping(diagnostics.get("false_cost_missed_upside"))
    downside = mapping(diagnostics.get("downside"))
    rows: list[dict[str, Any]] = []
    for candidate in candidate_rows:
        density = round_float(candidate.get("trigger_density_estimate"))
        trigger_count = int(to_float(candidate.get("trigger_count_estimate")))
        recommended_status = str(candidate.get("recommended_status", ""))
        threshold_id = str(candidate.get("threshold_id", ""))
        label = _selection_label_for_candidate(
            candidate,
            density=density,
            recommended_status=recommended_status,
        )
        selection_score = _candidate_selection_score(
            candidate,
            density=density,
            label=label,
        )
        rows.append(
            clean_for_yaml(
                {
                    "schema_version": f"{REPORT_TYPE}.candidate_scoring.v1",
                    "task_id": TASK_ID,
                    "threshold_id": threshold_id,
                    "threshold_type": candidate.get("threshold_type", ""),
                    "threshold_value": candidate.get("threshold_value", ""),
                    "numeric_threshold_value": candidate.get(
                        "numeric_threshold_value",
                        candidate.get("threshold_value", ""),
                    ),
                    "rule_description": _candidate_rule_description(candidate),
                    "trigger_count_estimate": trigger_count,
                    "trigger_density_estimate": density,
                    "expected_observe_event_frequency": candidate.get(
                        "expected_observe_event_frequency",
                        "",
                    ),
                    "historical_binding_overlap_count": candidate.get(
                        "historical_binding_overlap_count",
                        trigger_count,
                    ),
                    "historical_false_cost_context": false_cost.get(
                        "false_cost_label",
                        candidate.get("historical_false_cost_context", ""),
                    ),
                    "historical_downside_context": downside.get(
                        "downside_protection_label",
                        candidate.get("historical_downside_context", ""),
                    ),
                    "historical_missed_upside_context": false_cost.get(
                        "missed_upside_label",
                        candidate.get("historical_missed_upside_context", ""),
                    ),
                    "overbinding_risk": candidate.get("overbinding_risk", ""),
                    "missed_stress_risk": candidate.get("missed_stress_risk", ""),
                    "false_warning_risk": _false_warning_risk_label(candidate),
                    "interpretability_score": round_float(
                        candidate.get("interpretability_score")
                    ),
                    "implementation_complexity": candidate.get(
                        "implementation_complexity",
                        "UNKNOWN",
                    ),
                    "stability_score": _candidate_stability_score(
                        density=density,
                        trigger_count=trigger_count,
                    ),
                    "selection_score": selection_score,
                    "selection_label": label,
                    "source_recommended_status": recommended_status,
                    **SAFETY_FIELDS,
                }
            )
        )
    selected = [row for row in rows if row["selection_label"] == "SELECTED"]
    if len(selected) > 1:
        top_id = max(rows, key=lambda row: to_float(row["selection_score"]))[
            "threshold_id"
        ]
        for row in rows:
            if row["threshold_id"] != top_id and row["selection_label"] == "SELECTED":
                row["selection_label"] = "ACCEPTABLE_BACKUP"
    return sorted(rows, key=lambda row: to_float(row["selection_score"]), reverse=True)


def select_high_intensity_threshold_candidate(
    scoring_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected = [row for row in scoring_rows if row.get("selection_label") == "SELECTED"]
    if selected:
        return dict(max(selected, key=lambda row: to_float(row.get("selection_score"))))
    backups = [
        row
        for row in scoring_rows
        if row.get("selection_label") == "ACCEPTABLE_BACKUP"
    ]
    if backups:
        return dict(max(backups, key=lambda row: to_float(row.get("selection_score"))))
    return {}


def build_high_intensity_trigger_density_guardrail(
    *,
    selected_candidate: Mapping[str, Any],
    selected_event_rows: Sequence[Mapping[str, Any]],
    max_trigger_density: float = MAX_TRIGGER_DENSITY,
    max_monthly_event_count: int = MAX_MONTHLY_EVENT_COUNT,
    max_consecutive_trigger_days: int = MAX_CONSECUTIVE_TRIGGER_DAYS,
    min_expected_event_count_for_observe: int = MIN_EXPECTED_EVENT_COUNT_FOR_OBSERVE,
    density_warning_threshold: float = DENSITY_WARNING_THRESHOLD,
    density_blocking_threshold: float = DENSITY_BLOCKING_THRESHOLD,
) -> dict[str, Any]:
    selected_density = round_float(selected_candidate.get("trigger_density_estimate"))
    selected_count = int(to_float(selected_candidate.get("trigger_count_estimate")))
    event_dates = [str(row.get("date")) for row in selected_event_rows if row.get("date")]
    monthly_counts = Counter(date_value[:7] for date_value in event_dates)
    max_observed_monthly_count = max(monthly_counts.values(), default=0)
    max_observed_consecutive_days = _max_consecutive_calendar_days(event_dates)
    warnings: list[str] = []
    blockers: list[str] = []
    if not selected_candidate:
        blockers.append("NO_SELECTED_THRESHOLD")
    if selected_density > density_blocking_threshold:
        blockers.append("DENSITY_ABOVE_BLOCKING_THRESHOLD")
    elif selected_density > density_warning_threshold:
        warnings.append("DENSITY_ABOVE_WARNING_THRESHOLD")
    if selected_density > max_trigger_density:
        warnings.append("DENSITY_ABOVE_TARGET_MAX_TRIGGER_DENSITY")
    if selected_count < min_expected_event_count_for_observe:
        warnings.append("LOW_OBSERVE_EVENT_COUNT")
    if max_observed_monthly_count > max_monthly_event_count:
        warnings.append("MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL")
    if max_observed_consecutive_days > max_consecutive_trigger_days:
        blockers.append("CONSECUTIVE_TRIGGER_DAYS_ABOVE_GUARDRAIL")
    status = "PASS"
    if blockers:
        status = "BLOCKED"
    elif warnings:
        status = "PASS_WITH_WARNINGS"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.density_guardrail.v1",
            "task_id": TASK_ID,
            "guardrail_id": "HIGH_INTENSITY_TRIGGER_DENSITY_GUARDRAIL_V1",
            "max_trigger_density": max_trigger_density,
            "max_monthly_event_count": max_monthly_event_count,
            "max_consecutive_trigger_days": max_consecutive_trigger_days,
            "min_expected_event_count_for_observe": min_expected_event_count_for_observe,
            "density_warning_threshold": density_warning_threshold,
            "density_blocking_threshold": density_blocking_threshold,
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "selected_threshold_density": selected_density,
            "selected_trigger_count_estimate": selected_count,
            "observed_monthly_event_count_max": max_observed_monthly_count,
            "observed_consecutive_trigger_days_max": max_observed_consecutive_days,
            "monthly_event_counts": dict(sorted(monthly_counts.items())),
            "density_guardrail_status": status,
            "density_guardrail_warnings": warnings,
            "density_guardrail_blockers": blockers,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_threshold_selection_decision_matrix(
    *,
    scoring_rows: Sequence[Mapping[str, Any]],
    selected_candidate: Mapping[str, Any],
    guardrail: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_count = len(scoring_rows)
    acceptable_count = sum(
        1
        for row in scoring_rows
        if row.get("selection_label") in {"SELECTED", "ACCEPTABLE_BACKUP"}
    )
    guardrail_status = str(guardrail.get("density_guardrail_status", "BLOCKED"))
    if candidate_count == 0:
        status = "THRESHOLD_SELECTION_BLOCKED_NO_ACCEPTABLE_CANDIDATE"
        overall = "REQUIRE_THRESHOLD_CANDIDATE_REMEDIATION"
        next_task = NEXT_THRESHOLD_REMEDIATION_TASK
    elif not selected_candidate or acceptable_count == 0:
        status = "THRESHOLD_SELECTION_BLOCKED_NO_ACCEPTABLE_CANDIDATE"
        overall = "REQUIRE_THRESHOLD_CANDIDATE_REMEDIATION"
        next_task = NEXT_THRESHOLD_REMEDIATION_TASK
    elif guardrail_status == "BLOCKED":
        status = "THRESHOLD_SELECTION_BLOCKED_NO_ACCEPTABLE_CANDIDATE"
        overall = "REQUIRE_THRESHOLD_CANDIDATE_REMEDIATION"
        next_task = NEXT_THRESHOLD_REMEDIATION_TASK
    elif guardrail_status == "PASS_WITH_WARNINGS":
        status = "THRESHOLD_SELECTED_WITH_WARNINGS_PROMOTION_BLOCKED"
        overall = "PROCEED_TO_EVENT_LOGGER_WITH_CAVEAT"
        next_task = NEXT_EVENT_LOGGER_TASK
    else:
        status = "THRESHOLD_SELECTED_PROMOTION_BLOCKED"
        overall = "PROCEED_TO_EVENT_LOGGER"
        next_task = NEXT_EVENT_LOGGER_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.decision_matrix.v1",
            "task_id": TASK_ID,
            "threshold_selection_status": status,
            "candidate_count": candidate_count,
            "acceptable_candidate_count": acceptable_count,
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "selected_threshold_type": selected_candidate.get("threshold_type", ""),
            "selected_threshold_value": selected_candidate.get("threshold_value", ""),
            "selected_rule_summary": _candidate_rule_description(selected_candidate),
            "selection_rationale": _selection_rationale(
                selected_candidate,
                guardrail_status=guardrail_status,
            ),
            "trigger_density_status": guardrail_status,
            "false_warning_context_status": "MONITORING_REQUIRED",
            "missed_stress_context_status": "MONITORING_REQUIRED",
            "interpretability_status": "PASS_RESEARCH_ONLY",
            "implementation_status": "READY_FOR_EVENT_LOGGER_CONTRACT",
            "overall_decision": overall,
            "next_task_recommendation": next_task,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_trigger_rule(
    *,
    selected_candidate: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    output_path: Path,
) -> dict[str, Any]:
    threshold_value = selected_candidate.get("numeric_threshold_value")
    if threshold_value in {"", None}:
        threshold_value = selected_candidate.get("threshold_value", "")
    boolean_expression = _selected_boolean_expression(selected_candidate)
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.selected_trigger_rule.v1",
            "task_id": TASK_ID,
            "selected_rule_id": selected_candidate.get("threshold_id", ""),
            "selected_rule_version": "v1",
            "source_signal_family": "volatility_regime_scope_narrowed_risk_cap",
            "usage_mode": "high_intensity_forward_observe",
            "selected_rule_path": str(output_path),
            "trigger_rule": {
                "required_fields": [
                    "risk_cap_triggered",
                    "risk_cap_intensity",
                    "risk_cap_score",
                    "scope_active",
                    "signal_direction",
                ],
                "boolean_expression": boolean_expression,
                "threshold_type": selected_candidate.get("threshold_type", ""),
                "threshold_value": threshold_value,
                "density_guardrail": guardrail.get("guardrail_id", ""),
                "known_at_policy": KNOWN_AT_POLICY,
                "pit_policy": PIT_POLICY,
                "latency_policy": LATENCY_POLICY,
            },
            "selected_rule_rationale": _selection_rationale(
                selected_candidate,
                guardrail_status=str(guardrail.get("density_guardrail_status", "")),
            ),
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
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_trigger_contract(
    *,
    selected_rule: Mapping[str, Any],
    selected_rule_path: Path,
) -> dict[str, Any]:
    if not selected_rule.get("selected_rule_id"):
        raise HighIntensityThresholdSelectionError(
            "missing selected rule for selected trigger contract"
        )
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.selected_trigger_contract.v1",
            "task_id": TASK_ID,
            "contract_id": "HIGH_INTENSITY_SELECTED_TRIGGER_CONTRACT_V1",
            "contract_version": "v1",
            "selected_rule_path": str(selected_rule_path),
            "selected_rule_hash": _hash_payload(selected_rule),
            "required_input_fields": [
                "date",
                "target_asset",
                "risk_cap_triggered",
                "risk_cap_intensity",
                "risk_cap_score",
                "scope_active",
                "signal_direction",
                "as_of_timestamp",
                "decision_timestamp",
                "known_at_policy",
                "pit_policy",
            ],
            "output_event_fields": [
                "event_id",
                "event_date",
                "target_asset",
                "selected_rule_id",
                "high_intensity_triggered",
                "high_intensity_reason",
                "manual_review_observation_flag",
                "event_status",
            ],
            "safety_fields": {
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_threshold_selection_caveat_report(
    *,
    selected_candidate: Mapping[str, Any],
    guardrail: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.caveat_report.v1",
            "task_id": TASK_ID,
            "selection_caveat_id": "HIGH_INTENSITY_THRESHOLD_SELECTION_CAVEAT_V1",
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "uses_prior_dynamic_dry_run_context": True,
            "uses_future_return_optimization": False,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "known_at_policy": KNOWN_AT_POLICY,
            "selection_is_research_only": True,
            "selection_not_validated_forward": True,
            "threshold_overfit_risk": "MODERATE",
            "threshold_stability_risk": _threshold_stability_risk(guardrail),
            "missed_stress_risk": selected_candidate.get(
                "missed_stress_risk",
                "INCONCLUSIVE",
            ),
            "false_warning_risk": selected_candidate.get(
                "false_warning_risk",
                "MODERATE",
            ),
            "allowed_usage": [
                "research_only_forward_observe_event_logger",
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
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_event_logger_input_contract(
    *,
    selected_rule: Mapping[str, Any],
    selected_rule_path: Path,
    forward_observe_plan: Mapping[str, Any],
) -> dict[str, Any]:
    paths = mapping(forward_observe_plan.get("paths"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.event_logger_input_contract.v1",
            "task_id": TASK_ID,
            "event_logger_contract_id": (
                "HIGH_INTENSITY_EVENT_LOGGER_INPUT_CONTRACT_V1"
            ),
            "contract_version": "v1",
            "selected_trigger_rule": {
                "path": str(selected_rule_path),
                "hash": _hash_payload(selected_rule),
            },
            "source_event_schema": _path_hash(paths.get("event_schema", "")),
            "evidence_contract": _path_hash(paths.get("evidence_contract", "")),
            "actual_path_outcome_contract": _path_hash(
                paths.get("actual_path_contract", "")
            ),
            "manual_review_boundary": _path_hash(
                paths.get("manual_review_boundary", "")
            ),
            "required_runtime_inputs": [
                "risk_cap_trigger_series",
                "selected_trigger_rule",
                "known_at_timestamp",
                "target_asset",
                "trading_calendar",
            ],
            "runtime_outputs": [
                "observe_event_log",
                "pending_outcome_registry",
            ],
            "runtime_observe_allowed": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key not in {"paper_shadow_allowed", "production_allowed", "broker_action"}
            },
        }
    )


def build_high_intensity_selected_rule_backtest_context(
    *,
    selected_candidate: Mapping[str, Any],
    selected_event_rows: Sequence[Mapping[str, Any]],
    diagnostics: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
) -> dict[str, Any]:
    dry_summary = mapping(dynamic_dry_run.get("summary"))
    diagnostics_summary = mapping(diagnostics.get("summary"))
    dates = [str(row.get("date")) for row in selected_event_rows if row.get("date")]
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.selected_rule_backtest_context.v1",
            "task_id": TASK_ID,
            "context_role": "historical_context_only_no_new_backtest",
            "new_backtest_executed": False,
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "record_count_context": dry_summary.get("record_count", 0),
            "trigger_count_estimate": selected_candidate.get(
                "trigger_count_estimate",
                0,
            ),
            "trigger_density_estimate": selected_candidate.get(
                "trigger_density_estimate",
                0.0,
            ),
            "historical_context_window_start": min(dates) if dates else "",
            "historical_context_window_end": max(dates) if dates else "",
            "historical_false_warning_context": selected_candidate.get(
                "historical_false_cost_context",
                "",
            ),
            "historical_downside_capture_context": selected_candidate.get(
                "historical_downside_context",
                "",
            ),
            "historical_missed_upside_context": selected_candidate.get(
                "historical_missed_upside_context",
                "",
            ),
            "dynamic_strategy_overlap_context": mapping(
                diagnostics.get("strategy_overlap")
            ).get("dynamic_strategy_overlap_label", ""),
            "static_vs_dynamic_context": mapping(diagnostics.get("static_dynamic")).get(
                "static_vs_dynamic_comparison_label",
                diagnostics_summary.get("static_vs_dynamic_comparison_label", ""),
            ),
            "context_limitations": [
                "prior_dynamic_dry_run_context_only",
                "not_forward_validated",
                "not_a_production_rule",
                "future_return_optimization_not_used",
            ],
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_rule_false_warning_context(
    *,
    selected_candidate: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    false_cost = mapping(diagnostics.get("false_cost_missed_upside"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.false_warning_context.v1",
            "task_id": TASK_ID,
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "estimated_false_warning_count": false_cost.get(
                "false_risk_cap_count",
                selected_candidate.get("trigger_count_estimate", 0),
            ),
            "estimated_false_warning_density": selected_candidate.get(
                "trigger_density_estimate",
                0.0,
            ),
            "estimated_missed_upside_context": false_cost.get(
                "missed_upside_label",
                "",
            ),
            "false_warning_risk_label": "MODERATE",
            "false_warning_monitoring_required": True,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_rule_missed_stress_context(
    *,
    selected_candidate: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.missed_stress_context.v1",
            "task_id": TASK_ID,
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "estimated_missed_stress_context": selected_candidate.get(
                "missed_stress_risk",
                "INCONCLUSIVE",
            ),
            "too_narrow_risk_label": _missed_stress_context_label(
                selected_candidate
            ),
            "missed_stress_monitoring_required": True,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_selected_rule_manual_review_boundary() -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.manual_review_boundary.v1",
            "task_id": TASK_ID,
            "manual_review_only": True,
            "research_only": True,
            "forward_observe_only": True,
            "runtime_observe_allowed_for_2336": True,
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
            "blocked_outputs": [
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
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "manual_review_only",
                    "research_only",
                    "automatic_exposure_cap_allowed",
                    "target_weight_action_allowed",
                    "rebalance_instruction_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "broker_action",
                }
            },
        }
    )


def build_high_intensity_2336_readiness_checklist(
    *,
    decision: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
    selected_contract: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings = list(guardrail.get("density_guardrail_warnings", []))
    if not selected_rule.get("selected_rule_id"):
        blockers.append("SELECTED_TRIGGER_RULE_MISSING")
    if not selected_contract.get("selected_rule_hash"):
        blockers.append("SELECTED_TRIGGER_CONTRACT_MISSING_HASH")
    if not event_logger_contract.get("event_logger_contract_id"):
        blockers.append("EVENT_LOGGER_INPUT_CONTRACT_MISSING")
    if guardrail.get("density_guardrail_status") == "BLOCKED":
        blockers.extend(guardrail.get("density_guardrail_blockers", []))
    if blockers:
        readiness_status = "THRESHOLD_SELECTION_REMEDIATION_REQUIRED"
    elif warnings:
        readiness_status = "READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT"
    else:
        readiness_status = "READY_FOR_2336_EVENT_LOGGER"
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2336_readiness_checklist.v1",
            "task_id": TASK_ID,
            "selected_trigger_rule_ready": bool(selected_rule.get("selected_rule_id")),
            "selected_trigger_contract_ready": bool(
                selected_contract.get("selected_rule_hash")
            ),
            "event_logger_input_contract_ready": bool(
                event_logger_contract.get("event_logger_contract_id")
            ),
            "event_schema_ready": True,
            "evidence_contract_ready": True,
            "actual_path_contract_ready": True,
            "manual_review_boundary_ready": True,
            "density_guardrail_passed": guardrail.get("density_guardrail_status")
            in {"PASS", "PASS_WITH_WARNINGS"},
            "safety_boundary_ready": bool(safety_boundary),
            "runtime_observe_started": False,
            "paper_shadow_started": False,
            "production_started": False,
            "broker_action": "none",
            "readiness_status": readiness_status,
            "readiness_blockers": blockers,
            "readiness_warnings": warnings,
            "threshold_selection_status": decision.get(
                "threshold_selection_status",
                "",
            ),
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key not in {"broker_action"}
            },
        }
    )


def build_high_intensity_2336_task_route(
    *,
    readiness: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    readiness_status = str(readiness.get("readiness_status", "PLAN_BLOCKED"))
    threshold_status = str(decision.get("threshold_selection_status", ""))
    caveat = ""
    if readiness_status == "READY_FOR_2336_EVENT_LOGGER":
        next_task = NEXT_EVENT_LOGGER_TASK
    elif readiness_status == "READY_FOR_2336_EVENT_LOGGER_WITH_CAVEAT":
        next_task = NEXT_EVENT_LOGGER_TASK
        caveat = "THRESHOLD_SELECTION_RESEARCH_ONLY"
    elif threshold_status == "THRESHOLD_SELECTION_BLOCKED_DATA_CONTRACT":
        next_task = NEXT_DATA_REMEDIATION_TASK
    elif threshold_status == "THRESHOLD_SELECTION_BLOCKED_NO_ACCEPTABLE_CANDIDATE":
        next_task = NEXT_THRESHOLD_REMEDIATION_TASK
    else:
        next_task = NEXT_ARCHIVE_TASK
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.2336_task_route.v1",
            "task_id": TASK_ID,
            "allowed_routes": [
                NEXT_EVENT_LOGGER_TASK,
                NEXT_THRESHOLD_REMEDIATION_TASK,
                NEXT_DATA_REMEDIATION_TASK,
                NEXT_ARCHIVE_TASK,
            ],
            "readiness_status": readiness_status,
            "threshold_selection_status": threshold_status,
            "next_task": next_task,
            "caveat": caveat,
            "runtime_observe_started": False,
            **SAFETY_FIELDS,
        }
    )


def build_high_intensity_threshold_selection_safety_boundary(
    *,
    generated_at: datetime,
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.safety_boundary.v1",
            "task_id": TASK_ID,
            "generated_at": generated_at.isoformat(),
            "threshold_selection_status": decision.get(
                "threshold_selection_status",
                "",
            ),
            "research_only": True,
            "threshold_selection_only": True,
            "forward_observe_line": True,
            "runtime_observe_started": False,
            "automatic_exposure_cap_allowed": False,
            "portfolio_effect": "none",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "manual_review_only": True,
            "blocked_outputs": [
                "target_weight_action",
                "rebalance_instruction",
                "buy_signal",
                "sell_signal",
                "paper_shadow_ready",
                "production_ready",
                "broker_action",
                "automatic_exposure_cap",
            ],
            **{
                key: value
                for key, value in SAFETY_FIELDS.items()
                if key
                not in {
                    "research_only",
                    "threshold_selection_only",
                    "forward_observe_line",
                    "runtime_observe_started",
                    "automatic_exposure_cap_allowed",
                    "portfolio_effect",
                    "production_effect",
                    "broker_action",
                    "promotion_allowed",
                    "paper_shadow_allowed",
                    "production_allowed",
                    "manual_review_only",
                }
            },
        }
    )


def build_high_intensity_threshold_selection_summary(
    *,
    generated_at: datetime,
    forward_observe_plan_dir: Path,
    dynamic_diagnostics_dir: Path,
    dynamic_dry_run_dir: Path,
    readiness_dir: Path,
    timestamp_remediation_dir: Path,
    forward_observe_plan: Mapping[str, Any],
    dynamic_diagnostics: Mapping[str, Any],
    dynamic_dry_run: Mapping[str, Any],
    scoring_rows: Sequence[Mapping[str, Any]],
    selected_candidate: Mapping[str, Any],
    guardrail: Mapping[str, Any],
    decision: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> dict[str, Any]:
    warnings = list(readiness.get("readiness_warnings", []))
    blockers = list(readiness.get("readiness_blockers", []))
    status = STATUS
    if blockers:
        status = BLOCKED_STATUS
    elif warnings:
        status = STATUS_WITH_WARNINGS
    plan_summary = mapping(forward_observe_plan.get("summary"))
    diagnostics_summary = mapping(dynamic_diagnostics.get("summary"))
    dry_summary = mapping(dynamic_dry_run.get("summary"))
    data_quality = mapping(dynamic_dry_run.get("data_quality_report"))
    return clean_for_yaml(
        {
            "schema_version": f"{REPORT_TYPE}.summary.v1",
            "task_id": TASK_ID,
            "report_type": REPORT_TYPE,
            "artifact_role": ARTIFACT_ROLE,
            "title": "High-Intensity Risk-Cap Threshold Selection",
            "mode": MODE,
            "generated_at": generated_at.isoformat(),
            "status": status,
            "threshold_selection_status": decision.get(
                "threshold_selection_status",
                "",
            ),
            "selected_market_regime": MARKET_REGIME,
            "market_regime": MARKET_REGIME,
            "anchor_event": ANCHOR_EVENT,
            "anchor_date": ANCHOR_DATE,
            "default_backtest_start": DEFAULT_BACKTEST_START,
            "forward_observe_plan_dir": str(forward_observe_plan_dir),
            "dynamic_diagnostics_dir": str(dynamic_diagnostics_dir),
            "dynamic_dry_run_dir": str(dynamic_dry_run_dir),
            "readiness_dir": str(readiness_dir),
            "timestamp_remediation_dir": str(timestamp_remediation_dir),
            "data_validation_policy": DATA_VALIDATION_POLICY,
            "data_quality_gate_required": False,
            "data_quality_gate_executed": False,
            "aits_validate_data_executed": False,
            "aits_validate_data_applicability": "not_applicable",
            "data_quality_status": data_quality.get(
                "data_quality_status",
                plan_summary.get("data_quality_status", "UNKNOWN"),
            ),
            "prior_2334_status": plan_summary.get("status", ""),
            "prior_2334_readiness_status": plan_summary.get("readiness_status", ""),
            "prior_2334_next_task": plan_summary.get("next_task", ""),
            "prior_2333_overall_recommendation": diagnostics_summary.get(
                "overall_recommendation",
                "",
            ),
            "record_count": dry_summary.get("record_count", 0),
            "cap_binding_rate": dry_summary.get("cap_binding_rate", 0.0),
            "return_proxy_delta": dry_summary.get("return_proxy_delta", 0.0),
            "drawdown_proxy_delta": dry_summary.get("drawdown_proxy_delta", 0.0),
            "candidate_count": len(scoring_rows),
            "acceptable_candidate_count": decision.get(
                "acceptable_candidate_count",
                0,
            ),
            "selected_threshold_id": selected_candidate.get("threshold_id", ""),
            "selected_threshold_type": selected_candidate.get("threshold_type", ""),
            "selected_threshold_value": selected_candidate.get("threshold_value", ""),
            "selected_threshold_density": guardrail.get(
                "selected_threshold_density",
                0.0,
            ),
            "density_guardrail_status": guardrail.get(
                "density_guardrail_status",
                "",
            ),
            "density_guardrail_warnings": guardrail.get(
                "density_guardrail_warnings",
                [],
            ),
            "overall_decision": decision.get("overall_decision", ""),
            "readiness_status": readiness.get("readiness_status", ""),
            "next_task": task_route.get("next_task", ""),
            "runtime_observe_allowed_for_2336": task_route.get("next_task")
            == NEXT_EVENT_LOGGER_TASK,
            "runtime_observe_started": False,
            "candidate_scoring_matrix_generated": True,
            "density_guardrail_generated": True,
            "threshold_selection_decision_matrix_generated": True,
            "selected_trigger_rule_generated": bool(
                selected_candidate.get("threshold_id")
            ),
            "selected_trigger_contract_generated": bool(
                selected_candidate.get("threshold_id")
            ),
            "threshold_selection_caveat_report_generated": True,
            "event_logger_input_contract_generated": bool(
                selected_candidate.get("threshold_id")
            ),
            "2336_readiness_checklist_generated": True,
            "2336_task_route_generated": True,
            "uses_future_return_optimization": False,
            "research_only_selected_rule": True,
            "broad_exposure_cap_still_blocked": True,
            "strict_pit_ready": False,
            "pit_approximation_ready": True,
            "pit_policy": PIT_POLICY,
            "known_at_policy": KNOWN_AT_POLICY,
            "latency_policy": LATENCY_POLICY,
            **SAFETY_FIELDS,
        }
    )


def write_high_intensity_threshold_selection_outputs(
    *,
    output_dir: Path,
    docs_root: Path,
    paths: Mapping[str, Path],
    summary: Mapping[str, Any],
    scoring_rows: Sequence[Mapping[str, Any]],
    guardrail: Mapping[str, Any],
    decision: Mapping[str, Any],
    selected_rule: Mapping[str, Any],
    selected_contract: Mapping[str, Any],
    caveat_report: Mapping[str, Any],
    event_logger_contract: Mapping[str, Any],
    backtest_context: Mapping[str, Any],
    false_warning_context: Mapping[str, Any],
    missed_stress_context: Mapping[str, Any],
    manual_review_boundary: Mapping[str, Any],
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
    safety_boundary: Mapping[str, Any],
) -> dict[str, str]:
    del output_dir, docs_root
    outputs: list[Any] = [
        summary,
        *scoring_rows,
        guardrail,
        decision,
        selected_rule,
        selected_contract,
        caveat_report,
        event_logger_contract,
        backtest_context,
        false_warning_context,
        missed_stress_context,
        manual_review_boundary,
        readiness,
        task_route,
        safety_boundary,
    ]
    for index, payload in enumerate(outputs):
        _validate_no_unsafe_fields(f"TRADING-2335 output {index}", payload)
    write_json(paths["summary"], dict(summary))
    write_json(
        paths["candidate_scoring_json"],
        {
            "schema_version": f"{REPORT_TYPE}.candidate_scoring_matrix.v1",
            "task_id": TASK_ID,
            "rows": list(scoring_rows),
            **SAFETY_FIELDS,
        },
    )
    write_csv_rows(paths["candidate_scoring_csv"], scoring_rows)
    write_json(paths["density_guardrail"], dict(guardrail))
    write_json(paths["decision_matrix"], dict(decision))
    write_json(paths["selected_rule_json"], dict(selected_rule))
    write_markdown(paths["selected_rule_md"], _render_selected_rule_doc(selected_rule))
    write_json(paths["selected_contract"], dict(selected_contract))
    write_json(paths["caveat_report"], dict(caveat_report))
    write_json(paths["event_logger_contract"], dict(event_logger_contract))
    write_json(paths["backtest_context"], dict(backtest_context))
    write_json(paths["false_warning_context"], dict(false_warning_context))
    write_json(paths["missed_stress_context"], dict(missed_stress_context))
    write_json(paths["manual_review_boundary"], dict(manual_review_boundary))
    write_json(paths["readiness"], dict(readiness))
    write_json(paths["task_route"], dict(task_route))
    write_json(paths["safety_boundary"], dict(safety_boundary))

    write_markdown(
        paths["main_doc"],
        _render_main_doc(
            summary=summary,
            scoring_rows=scoring_rows,
            guardrail=guardrail,
            decision=decision,
            task_route=task_route,
        ),
    )
    write_markdown(paths["selected_rule_doc"], _render_selected_rule_doc(selected_rule))
    write_markdown(paths["caveat_doc"], _render_caveat_doc(caveat_report))
    write_markdown(
        paths["event_logger_contract_doc"],
        _render_event_logger_contract_doc(event_logger_contract),
    )
    write_markdown(
        paths["readiness_route_doc"],
        _render_readiness_route_doc(readiness=readiness, task_route=task_route),
    )
    return {key: str(path) for key, path in paths.items()}


def _build_output_paths(*, output_dir: Path, docs_root: Path) -> dict[str, Path]:
    return {
        "summary": output_dir / "high_intensity_threshold_selection_summary.json",
        "candidate_scoring_json": output_dir
        / "high_intensity_threshold_candidate_scoring_matrix.json",
        "candidate_scoring_csv": output_dir
        / "high_intensity_threshold_candidate_scoring_matrix.csv",
        "density_guardrail": output_dir
        / "high_intensity_trigger_density_guardrail.json",
        "decision_matrix": output_dir
        / "high_intensity_threshold_selection_decision_matrix.json",
        "selected_rule_json": output_dir / "high_intensity_selected_trigger_rule.json",
        "selected_rule_md": output_dir / "high_intensity_selected_trigger_rule.md",
        "selected_contract": output_dir
        / "high_intensity_selected_trigger_contract.json",
        "caveat_report": output_dir
        / "high_intensity_threshold_selection_caveat_report.json",
        "event_logger_contract": output_dir
        / "high_intensity_event_logger_input_contract.json",
        "backtest_context": output_dir
        / "high_intensity_selected_rule_backtest_context.json",
        "false_warning_context": output_dir
        / "high_intensity_selected_rule_false_warning_context.json",
        "missed_stress_context": output_dir
        / "high_intensity_selected_rule_missed_stress_context.json",
        "manual_review_boundary": output_dir
        / "high_intensity_selected_rule_manual_review_boundary.json",
        "readiness": output_dir / "high_intensity_2336_readiness_checklist.json",
        "task_route": output_dir / "high_intensity_2336_task_route.json",
        "safety_boundary": output_dir
        / "high_intensity_threshold_selection_safety_boundary.json",
        "main_doc": docs_root / "high_intensity_risk_cap_threshold_selection.md",
        "selected_rule_doc": docs_root / "high_intensity_selected_trigger_rule.md",
        "caveat_doc": docs_root
        / "high_intensity_threshold_selection_caveat_report.md",
        "event_logger_contract_doc": docs_root
        / "high_intensity_event_logger_input_contract.md",
        "readiness_route_doc": docs_root / "high_intensity_2336_readiness_route.md",
    }


def _render_main_doc(
    *,
    summary: Mapping[str, Any],
    scoring_rows: Sequence[Mapping[str, Any]],
    guardrail: Mapping[str, Any],
    decision: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    rows = "\n".join(
        f"- `{row['threshold_id']}`: `{row['selection_label']}` "
        f"(density `{row['trigger_density_estimate']}`, "
        f"score `{row['selection_score']}`)"
        for row in scoring_rows
    )
    return "\n".join(
        [
            "# High-Intensity Risk-Cap Threshold Selection",
            "",
            "TRADING-2335 承接 TRADING-2334 `THRESHOLD_SELECTION_REQUIRED` "
            "route，只做 deterministic threshold selection。"
            "本任务不启动 runtime observe，不生成 target weight、rebalance "
            "instruction、paper-shadow、production 或 broker action。",
            "",
            f"- status: `{summary['status']}`",
            f"- selected_market_regime: `{summary['selected_market_regime']}`",
            f"- data_quality_status: `{summary['data_quality_status']}`",
            f"- data_validation_policy: `{summary['data_validation_policy']}`",
            "- aits validate-data: `not applicable`，因为本任务只读取 prior "
            "validated research artifacts。",
            f"- threshold_selection_status: "
            f"`{decision['threshold_selection_status']}`",
            f"- selected_threshold_id: `{summary['selected_threshold_id']}`",
            f"- selected_threshold_density: "
            f"`{guardrail['selected_threshold_density']}`",
            f"- density_guardrail_status: "
            f"`{guardrail['density_guardrail_status']}`",
            f"- readiness_status: `{summary['readiness_status']}`",
            f"- next_task: `{task_route['next_task']}`",
            "- runtime_observe_started: `False`",
            "- promotion_allowed: `False`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "## Candidate Scoring",
            "",
            rows,
            "",
            "## Interpretation Boundary",
            "",
            "Selected rule 只允许作为 TRADING-2336 observe-only event logger "
            "的 research input；它不是 production rule，不是 automatic "
            "exposure cap，不是减仓建议，也不能生成 broker action。",
        ]
    )


def _render_selected_rule_doc(selected_rule: Mapping[str, Any]) -> str:
    trigger_rule = mapping(selected_rule.get("trigger_rule"))
    return "\n".join(
        [
            "# High-Intensity Selected Trigger Rule",
            "",
            f"- selected_rule_id: `{selected_rule.get('selected_rule_id', '')}`",
            f"- selected_rule_version: `{selected_rule.get('selected_rule_version', '')}`",
            f"- usage_mode: `{selected_rule.get('usage_mode', '')}`",
            f"- boolean_expression: `{trigger_rule.get('boolean_expression', '')}`",
            f"- threshold_type: `{trigger_rule.get('threshold_type', '')}`",
            f"- threshold_value: `{trigger_rule.get('threshold_value', '')}`",
            f"- known_at_policy: `{trigger_rule.get('known_at_policy', '')}`",
            f"- pit_policy: `{trigger_rule.get('pit_policy', '')}`",
            "",
            "该规则仅用于 research-only forward observe event logger 和 manual "
            "review context，不允许自动 exposure cap、target weight、rebalance、"
            "paper-shadow、production 或 broker action。",
        ]
    )


def _render_caveat_doc(caveat_report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Threshold Selection Caveat Report",
            "",
            f"- selection_caveat_id: "
            f"`{caveat_report.get('selection_caveat_id', '')}`",
            f"- selected_threshold_id: "
            f"`{caveat_report.get('selected_threshold_id', '')}`",
            "- uses_future_return_optimization: `False`",
            "- strict_pit_ready: `False`",
            "- pit_approximation_ready: `True`",
            f"- threshold_stability_risk: "
            f"`{caveat_report.get('threshold_stability_risk', '')}`",
            f"- missed_stress_risk: "
            f"`{caveat_report.get('missed_stress_risk', '')}`",
            f"- false_warning_risk: "
            f"`{caveat_report.get('false_warning_risk', '')}`",
            "",
            "Selected threshold 不是已经 forward validated 的 production rule；"
            "它只是进入 TRADING-2336 observe-only event logger 的研究规则。",
        ]
    )


def _render_event_logger_contract_doc(contract: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# High-Intensity Event Logger Input Contract",
            "",
            f"- event_logger_contract_id: "
            f"`{contract.get('event_logger_contract_id', '')}`",
            f"- contract_version: `{contract.get('contract_version', '')}`",
            f"- runtime_observe_allowed: "
            f"`{contract.get('runtime_observe_allowed')}`",
            "- paper_shadow_allowed: `False`",
            "- production_allowed: `False`",
            "- broker_action: `none`",
            "",
            "Runtime observe allowed 仅表示 TRADING-2336 可以实现 observe-only "
            "event logger；不代表 paper-shadow、production、target weight、"
            "rebalance 或 broker action。",
        ]
    )


def _render_readiness_route_doc(
    *,
    readiness: Mapping[str, Any],
    task_route: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# High-Intensity 2336 Readiness Route",
            "",
            f"- readiness_status: `{readiness.get('readiness_status', '')}`",
            f"- next_task: `{task_route.get('next_task', '')}`",
            f"- caveat: `{task_route.get('caveat', '')}`",
            "- runtime_observe_started: `False`",
            "- paper_shadow_started: `False`",
            "- production_started: `False`",
            "- broker_action: `none`",
            "",
            "TRADING-2336 route 只允许进入 observe-only event logger、threshold "
            "candidate remediation、data contract remediation 或 archive line。",
        ]
    )


def _selection_label_for_candidate(
    candidate: Mapping[str, Any],
    *,
    density: float,
    recommended_status: str,
) -> str:
    if not candidate:
        return "BLOCKED"
    if density > DENSITY_BLOCKING_THRESHOLD:
        return "TOO_BROAD_OVERBINDING_RISK"
    if recommended_status == "TOO_BROAD_OVERBINDING_RISK":
        return "TOO_BROAD_OVERBINDING_RISK"
    if recommended_status == "TOO_NARROW_MISSED_STRESS_RISK":
        return "TOO_NARROW_MISSED_STRESS_RISK"
    if str(candidate.get("threshold_type")) == "COMPOSITE_HIGH_INTENSITY_RULE":
        return "SELECTED"
    if to_float(candidate.get("interpretability_score")) < 0.5:
        return "LOW_INTERPRETABILITY"
    if density <= MAX_TRIGGER_DENSITY:
        return "ACCEPTABLE_BACKUP"
    return "INSUFFICIENT_CONTEXT"


def _candidate_selection_score(
    candidate: Mapping[str, Any],
    *,
    density: float,
    label: str,
) -> float:
    base = to_float(candidate.get("interpretability_score"))
    complexity = str(candidate.get("implementation_complexity", "UNKNOWN"))
    complexity_score = {"LOW": 0.9, "MEDIUM": 0.75, "HIGH": 0.45}.get(
        complexity,
        0.55,
    )
    stability = _candidate_stability_score(
        density=density,
        trigger_count=int(to_float(candidate.get("trigger_count_estimate"))),
    )
    label_bonus = {
        "SELECTED": 0.35,
        "ACCEPTABLE_BACKUP": 0.18,
        "TOO_BROAD_OVERBINDING_RISK": -0.35,
        "TOO_NARROW_MISSED_STRESS_RISK": -0.30,
        "LOW_INTERPRETABILITY": -0.25,
        "INSUFFICIENT_CONTEXT": -0.20,
        "BLOCKED": -0.50,
    }.get(label, 0.0)
    density_penalty = max(density - DENSITY_WARNING_THRESHOLD, 0.0) * 2.0
    score = (base * 0.35) + (complexity_score * 0.2) + (stability * 0.25)
    score += label_bonus - density_penalty
    return round_float(max(min(score, 1.0), 0.0))


def _candidate_stability_score(*, density: float, trigger_count: int) -> float:
    if trigger_count <= 0:
        return 0.0
    if density <= DENSITY_WARNING_THRESHOLD and (
        trigger_count >= MIN_EXPECTED_EVENT_COUNT_FOR_OBSERVE
    ):
        return 0.85
    if density <= MAX_TRIGGER_DENSITY:
        return 0.7
    if density <= DENSITY_BLOCKING_THRESHOLD:
        return 0.45
    return 0.2


def _false_warning_risk_label(candidate: Mapping[str, Any]) -> str:
    false_context = str(candidate.get("historical_false_cost_context", ""))
    density = to_float(candidate.get("trigger_density_estimate"))
    if "BLOCKING" in false_context or density > DENSITY_WARNING_THRESHOLD:
        return "MODERATE"
    return "LOW"


def _candidate_rule_description(candidate: Mapping[str, Any]) -> str:
    if not candidate:
        return "no selected high-intensity threshold"
    threshold_type = str(candidate.get("threshold_type", ""))
    threshold_value = candidate.get("threshold_value", "")
    if threshold_type == "COMPOSITE_HIGH_INTENSITY_RULE":
        return (
            "risk_cap_triggered == true AND scope_active == true AND "
            "signal_direction != none AND risk_cap_score >= "
            f"{candidate.get('numeric_threshold_value', threshold_value)}"
        )
    return f"{threshold_type} using threshold {threshold_value}"


def _selection_rationale(
    selected_candidate: Mapping[str, Any],
    *,
    guardrail_status: str,
) -> str:
    if not selected_candidate:
        return "No acceptable threshold candidate was available."
    return (
        f"{selected_candidate.get('threshold_id')} was selected because it is "
        "the only candidate with TRADING-2334 selection status, keeps trigger "
        f"density at {selected_candidate.get('trigger_density_estimate')}, "
        "uses no future return optimization, and remains research-only. "
        f"Density guardrail status: {guardrail_status}."
    )


def _selected_boolean_expression(selected_candidate: Mapping[str, Any]) -> str:
    if not selected_candidate:
        return ""
    threshold = selected_candidate.get(
        "numeric_threshold_value",
        selected_candidate.get("threshold_value", ""),
    )
    if selected_candidate.get("threshold_type") == "COMPOSITE_HIGH_INTENSITY_RULE":
        return (
            "risk_cap_triggered == true AND scope_active == true AND "
            f"risk_cap_score >= {threshold} AND signal_direction != none"
        )
    return (
        "risk_cap_triggered == true AND scope_active == true AND "
        f"risk_cap_score >= {threshold}"
    )


def _threshold_stability_risk(guardrail: Mapping[str, Any]) -> str:
    if guardrail.get("density_guardrail_status") == "BLOCKED":
        return "HIGH"
    if guardrail.get("density_guardrail_warnings"):
        return "MODERATE"
    return "LOW"


def _missed_stress_context_label(selected_candidate: Mapping[str, Any]) -> str:
    risk = str(selected_candidate.get("missed_stress_risk", ""))
    if "HIGH" in risk:
        return "HIGH"
    if "MEDIUM" in risk:
        return "MODERATE"
    if not risk:
        return "INCONCLUSIVE"
    return "LOW"


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


def _rows_for_selected_candidate(
    rows: Sequence[Mapping[str, Any]],
    selected_candidate: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    if not selected_candidate:
        return []
    threshold_value = to_float(selected_candidate.get("numeric_threshold_value"))
    selected = [
        row
        for row in rows
        if row.get("risk_cap_triggered") is True
        and to_float(row.get("risk_cap_score")) >= threshold_value
    ]
    if selected_candidate.get("threshold_type") == "COMPOSITE_HIGH_INTENSITY_RULE":
        selected = [
            row
            for row in selected
            if row.get("scope_active") is True
            and str(row.get("signal_direction", "none")) != "none"
        ]
    return selected


def _intensity_rank(value: str) -> int:
    ranks = {"none": 0, "low": 1, "medium": 2, "high": 3}
    return ranks.get(value.lower(), 0)


def _max_consecutive_calendar_days(event_dates: Sequence[str]) -> int:
    max_run = 0
    current_run = 0
    previous: date | None = None
    for value in sorted(set(event_dates)):
        try:
            current = date.fromisoformat(value)
        except ValueError:
            continue
        if previous is not None and (current - previous).days == 1:
            current_run += 1
        else:
            current_run = 1
        max_run = max(max_run, current_run)
        previous = current
    return max_run


def _load_required_payloads(
    paths: Mapping[str, Path],
    label: str,
) -> dict[str, Any]:
    payloads: dict[str, Any] = {}
    for key, path in paths.items():
        if not path.exists():
            raise HighIntensityThresholdSelectionError(
                f"{label} required artifact missing: {path}"
            )
        payloads[key] = _load_json(path)
    return payloads


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _hash_payload(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        clean_for_yaml(dict(payload)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _path_hash(path_value: str) -> dict[str, str]:
    path = Path(path_value)
    if not path.exists():
        raise HighIntensityThresholdSelectionError(
            f"required contract source path missing for hash: {path}"
        )
    return {"path": str(path), "hash": hashlib.sha256(path.read_bytes()).hexdigest()}


def _validate_no_unsafe_fields(label: str, payload: Any) -> None:
    false_required_keys = {
        "promotion_allowed",
        "paper_shadow_allowed",
        "production_allowed",
        "paper_shadow_ready",
        "production_ready",
        "paper_shadow_started",
        "production_started",
        "automatic_exposure_cap_allowed",
        "target_weight_action_allowed",
        "rebalance_instruction_allowed",
        "target_weight_generated",
        "rebalance_instruction_generated",
        "broker_order_generated",
        "paper_shadow_order_generated",
        "production_decision_generated",
    }
    blocked_value_keys = {
        "target_weight_action",
        "target_weight",
        "rebalance_instruction",
        "reduce_position_instruction",
        "increase_cash_instruction",
        "buy_signal",
        "sell_signal",
        "automatic_exposure_cap",
    }
    for path, value in _walk_payload(payload):
        key = path[-1] if path else ""
        if key in false_required_keys and value is True:
            raise HighIntensityThresholdSelectionError(
                f"{label} unsafe field {'.'.join(path)}=true"
            )
        if key == "broker_action" and str(value).lower() not in {"", "none"}:
            raise HighIntensityThresholdSelectionError(
                f"{label} unsafe broker_action={value}"
            )
        if key in blocked_value_keys and value not in {False, None, "", "none", "NONE"}:
            raise HighIntensityThresholdSelectionError(
                f"{label} unsafe field {'.'.join(path)}={value}"
            )


def _walk_payload(
    payload: Any,
    prefix: tuple[str, ...] = (),
) -> list[tuple[tuple[str, ...], Any]]:
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
