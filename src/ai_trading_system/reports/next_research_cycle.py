from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports import executable_research_binding as binding_reports
from ai_trading_system.reports import return_to_research_reset as reset_reports

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
MARKET_REGIME = "unified_primary_2021"
AI_REGIME_START = "2022-12-01"
PRIMARY_RESEARCH_START = "2021-02-22"
CANDIDATE_BACKFILL_COMPLETE = "CANDIDATE_BACKFILL_COMPLETE"
CANDIDATE_BACKFILL_PARTIAL = "CANDIDATE_BACKFILL_PARTIAL"
CANDIDATE_BACKFILL_BLOCKED = "CANDIDATE_BACKFILL_BLOCKED"
CANDIDATE_BACKFILL_METRIC_STATUSES = {
    CANDIDATE_BACKFILL_COMPLETE,
    CANDIDATE_BACKFILL_PARTIAL,
}
RESEARCH_GATE_DECISIONS = {
    "RESEARCH_PROMISING",
    "NEEDS_MORE_EVIDENCE",
    "RETURN_TO_HYPOTHESIS_BACKLOG",
    "REJECT_RESEARCH_CANDIDATE",
}
EXECUTABLE_RESEARCH_CYCLE_STATUSES = {
    "EXECUTABLE_RESEARCH_CYCLE_PROMISING",
    "EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE",
    "EXECUTABLE_RESEARCH_CYCLE_RETURN_TO_BACKLOG",
    "EXECUTABLE_RESEARCH_CYCLE_REJECT",
    "EXECUTABLE_RESEARCH_CYCLE_BLOCKED",
}
# TRADING-465 diagnostic bands are conservative review cutoffs, not promotion
# thresholds. They only classify blockers/warnings from already-computed metrics.
STRESS_BLOCKING_DRAWDOWN_PROXY = -0.20
STRESS_WARNING_DRAWDOWN_PROXY = -0.10
DEFAULT_COST_MEANINGFUL_THRESHOLD = 0.0025
DEFAULT_COST_SCENARIOS: tuple[dict[str, Any], ...] = (
    {"scenario_id": "zero", "label": "Zero Cost", "total_cost_bps": 0.0},
    {"scenario_id": "low", "label": "Low Cost", "total_cost_bps": 3.0},
    {"scenario_id": "medium", "label": "Medium Cost", "total_cost_bps": 10.0},
    {"scenario_id": "high", "label": "High Cost", "total_cost_bps": 25.0},
)

INTAKE_REPORT_TYPE = "next_research_cycle_intake"
FROZEN_SPEC_REPORT_TYPE = "next_candidate_spec_frozen"
BACKFILL_REPORT_TYPE = "next_candidate_backfill"
STRESS_REVIEW_REPORT_TYPE = "next_candidate_stress_review"
COST_BENCHMARK_REVIEW_REPORT_TYPE = "next_candidate_cost_benchmark_review"
VS_RETURNED_REPORT_TYPE = "next_candidate_vs_returned_candidate_comparison"
SIGNAL_ROBUSTNESS_REPORT_TYPE = "next_candidate_signal_robustness_review"
WINDOW_SENSITIVITY_REPORT_TYPE = "next_candidate_overfit_window_sensitivity"
RESEARCH_GATE_REPORT_TYPE = "next_candidate_research_gate"
OWNER_REVIEW_PACKET_REPORT_TYPE = "next_candidate_owner_research_review_packet"
CYCLE_SNAPSHOT_REPORT_TYPE = "next_candidate_research_cycle_snapshot"

VALIDATION_SUFFIX = "_validation"

REPORT_PREFIXES: dict[str, str] = {
    INTAKE_REPORT_TYPE: "next_research_cycle_intake",
    FROZEN_SPEC_REPORT_TYPE: "next_candidate_spec_frozen",
    BACKFILL_REPORT_TYPE: "next_candidate_backfill",
    STRESS_REVIEW_REPORT_TYPE: "next_candidate_stress_review",
    COST_BENCHMARK_REVIEW_REPORT_TYPE: "next_candidate_cost_benchmark_review",
    VS_RETURNED_REPORT_TYPE: "next_candidate_vs_returned_candidate_comparison",
    SIGNAL_ROBUSTNESS_REPORT_TYPE: "next_candidate_signal_robustness_review",
    WINDOW_SENSITIVITY_REPORT_TYPE: "next_candidate_overfit_window_sensitivity",
    RESEARCH_GATE_REPORT_TYPE: "next_candidate_research_gate",
    OWNER_REVIEW_PACKET_REPORT_TYPE: "next_candidate_owner_research_review_packet",
    CYCLE_SNAPSHOT_REPORT_TYPE: "next_candidate_research_cycle_snapshot",
}
REPORT_PREFIXES.update(
    {
        f"{report_type}{VALIDATION_SUFFIX}": f"{prefix}{VALIDATION_SUFFIX}"
        for report_type, prefix in tuple(REPORT_PREFIXES.items())
    }
)

NEXT_RESEARCH_CYCLE_REPORT_TYPES: tuple[str, ...] = (
    INTAKE_REPORT_TYPE,
    FROZEN_SPEC_REPORT_TYPE,
    BACKFILL_REPORT_TYPE,
    STRESS_REVIEW_REPORT_TYPE,
    COST_BENCHMARK_REVIEW_REPORT_TYPE,
    VS_RETURNED_REPORT_TYPE,
    SIGNAL_ROBUSTNESS_REPORT_TYPE,
    WINDOW_SENSITIVITY_REPORT_TYPE,
    RESEARCH_GATE_REPORT_TYPE,
    OWNER_REVIEW_PACKET_REPORT_TYPE,
    CYCLE_SNAPSHOT_REPORT_TYPE,
)

RESET_INTAKE_INPUT_TYPES: tuple[str, ...] = (
    reset_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE,
    reset_reports.FAILURE_MODE_ATTRIBUTION_REPORT_TYPE,
    reset_reports.REUSABLE_EVIDENCE_REPORT_TYPE,
    reset_reports.HYPOTHESIS_BACKLOG_REPORT_TYPE,
    reset_reports.NEXT_CANDIDATE_SPEC_REPORT_TYPE,
    reset_reports.RESEARCH_BACKFILL_PLAN_REPORT_TYPE,
)

REQUIRED_BACKFILL_WINDOWS: tuple[str, ...] = (
    "normal_market_regime",
    "rapid_drawdown",
    "slow_drawdown",
    "high_volatility_sideways_market",
    "ai_semiconductor_correction",
    "false_risk_off_cluster",
)

WINDOW_SENSITIVITY_SPLITS: tuple[str, ...] = (
    "early_window",
    "middle_window",
    "recent_window",
    "stress_heavy_window",
    "calm_market_window",
)
# TRADING-467 diagnostic window groupings are fixed audit buckets for sensitivity
# review. They do not promote a candidate and rely on the TRADING-465 diagnostic
# drawdown bands already used by stress review.
WINDOW_SENSITIVITY_WINDOW_MAP: dict[str, tuple[str, ...]] = {
    "early_window": (
        "normal_market_regime",
        "high_volatility_sideways_market",
        "false_risk_off_cluster",
    ),
    "middle_window": (
        "rapid_drawdown",
        "ai_semiconductor_correction",
    ),
    "recent_window": ("slow_drawdown",),
    "stress_heavy_window": (
        "rapid_drawdown",
        "slow_drawdown",
        "ai_semiconductor_correction",
    ),
    "calm_market_window": ("normal_market_regime",),
}

LATEST_PROJECT_ARTIFACT_GLOBS: dict[str, tuple[str, ...]] = {
    "stress_scenario_library": (
        "reports/etf_portfolio/dynamic_v3_rescue/stress_scenario_library/*/"
        "stress_scenario_library.json",
    ),
    "drawdown_event_casebook": (
        "reports/etf_portfolio/dynamic_v3_rescue/drawdown_event_casebook/*/"
        "drawdown_event_casebook.json",
    ),
    "flip_rotation_event_casebook": (
        "reports/etf_portfolio/dynamic_v3_rescue/flip_rotation_event_casebook/*/"
        "flip_rotation_event_casebook.json",
    ),
    "signal_input_completeness": (
        "reports/etf_portfolio/dynamic_v3_rescue/signal_input_completeness/*/"
        "signal_input_completeness_report.json",
    ),
    "cost_sensitivity_framework": (
        "reports/etf_portfolio/dynamic_v3_rescue/cost_sensitivity_review/*/"
        "cost_sensitivity_review.json",
    ),
    "benchmark_baseline_control": (
        "reports/etf_portfolio/dynamic_v3_rescue/benchmark_baseline_control/*/"
        "benchmark_baseline_control_pack.json",
    ),
}


def default_next_research_cycle_json_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.json"


def default_next_research_cycle_markdown_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.md"


def latest_next_research_cycle_json_path(report_type: str, output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, f"{REPORT_PREFIXES[report_type]}_", ".json")


def build_next_research_cycle_payloads(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    project_root: Path = PROJECT_ROOT,
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    intake = build_next_research_cycle_intake_payload(
        as_of=as_of,
        reports_dir=reports_dir,
    )
    frozen = build_next_candidate_spec_frozen_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        intake_payload=intake,
    )
    backfill = build_next_candidate_backfill_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        frozen_spec_payload=frozen,
        data_quality_gate=data_quality_gate or {},
    )
    stress = build_next_candidate_stress_review_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        project_root=project_root,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    cost_benchmark = build_next_candidate_cost_benchmark_review_payload(
        as_of=as_of,
        project_root=project_root,
        backfill_payload=backfill,
    )
    comparison = build_next_candidate_vs_returned_comparison_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost_benchmark,
    )
    signal = build_next_candidate_signal_robustness_review_payload(
        as_of=as_of,
        reports_dir=reports_dir,
        project_root=project_root,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    window = build_next_candidate_window_sensitivity_payload(
        as_of=as_of,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
    )
    gate = build_next_candidate_research_gate_payload(
        as_of=as_of,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost_benchmark,
        comparison_payload=comparison,
        signal_robustness_payload=signal,
        window_sensitivity_payload=window,
    )
    owner_packet = build_next_candidate_owner_research_review_packet_payload(
        as_of=as_of,
        research_gate_payload=gate,
    )
    snapshot = build_next_research_cycle_snapshot_payload(
        as_of=as_of,
        intake_payload=intake,
        frozen_spec_payload=frozen,
        backfill_payload=backfill,
        stress_review_payload=stress,
        cost_benchmark_payload=cost_benchmark,
        comparison_payload=comparison,
        signal_robustness_payload=signal,
        window_sensitivity_payload=window,
        research_gate_payload=gate,
        owner_packet_payload=owner_packet,
    )
    return {
        INTAKE_REPORT_TYPE: intake,
        FROZEN_SPEC_REPORT_TYPE: frozen,
        BACKFILL_REPORT_TYPE: backfill,
        STRESS_REVIEW_REPORT_TYPE: stress,
        COST_BENCHMARK_REVIEW_REPORT_TYPE: cost_benchmark,
        VS_RETURNED_REPORT_TYPE: comparison,
        SIGNAL_ROBUSTNESS_REPORT_TYPE: signal,
        WINDOW_SENSITIVITY_REPORT_TYPE: window,
        RESEARCH_GATE_REPORT_TYPE: gate,
        OWNER_REVIEW_PACKET_REPORT_TYPE: owner_packet,
        CYCLE_SNAPSHOT_REPORT_TYPE: snapshot,
    }


def build_next_research_cycle_intake_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    inputs = _load_reset_intake_inputs(reports_dir, as_of)
    snapshot = _payload_from_inputs(inputs, reset_reports.GOVERNANCE_SNAPSHOT_REPORT_TYPE)
    failure = _payload_from_inputs(inputs, reset_reports.FAILURE_MODE_ATTRIBUTION_REPORT_TYPE)
    reusable = _payload_from_inputs(inputs, reset_reports.REUSABLE_EVIDENCE_REPORT_TYPE)
    backlog = _payload_from_inputs(inputs, reset_reports.HYPOTHESIS_BACKLOG_REPORT_TYPE)
    draft = _payload_from_inputs(inputs, reset_reports.NEXT_CANDIDATE_SPEC_REPORT_TYPE)
    plan = _payload_from_inputs(inputs, reset_reports.RESEARCH_BACKFILL_PLAN_REPORT_TYPE)

    failure_modes = _records(failure.get("ranked_failure_modes"))
    reusable_rows = _records(reusable.get("reusable_evidence"))
    invalidated_rows = _records(reusable.get("invalidated_evidence"))
    hypotheses = _records(backlog.get("hypotheses"))
    p0_hypotheses = [row for row in hypotheses if _text(row.get("priority")) == "P0"]
    candidate_specs = _records(draft.get("candidate_specs"))
    windows = _records(plan.get("required_backfill_windows"))
    requested_range = _date_range_from_windows(windows)
    previous_summary = _mapping(snapshot.get("summary"))
    summary = {
        "intake_status": "NEXT_RESEARCH_CYCLE_INTAKE_READY",
        "previous_candidate_id": _text(
            previous_summary.get("candidate_id"),
            reset_reports.CANDIDATE_ID,
        ),
        "previous_candidate_status": _text(previous_summary.get("candidate_status")),
        "owner_action": _text(previous_summary.get("owner_action")),
        "why_returned_to_research": _text(
            _mapping(failure.get("summary")).get("recommended_research_direction"),
            "redesign_candidate_for_cost_survival_and_benchmark_relative_strength",
        ),
        "reusable_evidence_count": len(reusable_rows),
        "invalidated_or_weak_evidence_count": len(invalidated_rows)
        + len(
            [
                row
                for row in _records(reusable.get("evidence_classification"))
                if _text(row.get("classification")) == "weak but informative"
            ]
        ),
        "p0_hypothesis_count": len(p0_hypotheses),
        "next_candidate_spec_count": len(candidate_specs),
        "selected_market_regime": MARKET_REGIME,
        "requested_date_range": requested_range,
        "paper_shadow_candidate_created": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=INTAKE_REPORT_TYPE,
        as_of=as_of,
        status=summary["intake_status"],
        purpose=(
            "Create the research-only intake pack after the previous candidate was "
            "returned to research."
        ),
        input_artifacts=_input_paths(inputs),
        output_decision=summary["intake_status"],
        summary=summary,
        body={
            "why_previous_candidate_returned_to_research": [
                {
                    "failure_mode_id": _text(row.get("failure_mode_id")),
                    "classification": _text(row.get("classification")),
                    "evidence": _text(row.get("evidence")),
                    "recommended_action": _text(row.get("recommended_action")),
                }
                for row in failure_modes[:5]
            ],
            "reusable_evidence": reusable_rows,
            "invalidated_or_weak_evidence": _invalidated_or_weak_evidence(reusable),
            "p0_hypotheses": p0_hypotheses,
            "next_candidate_spec_proposal": candidate_specs,
            "required_backfill_windows": windows,
        },
        reader_brief=_reader_brief(
            summary=(
                "Next research-cycle intake is ready; prior candidate remains "
                "returned to research."
            ),
            key_result=summary["intake_status"],
            blocking_issues="paper_shadow_candidate_creation_forbidden",
            warnings="cost and benchmark evidence remain invalidating for the old candidate",
            next_action="freeze_next_research_candidate_spec",
        ),
        next_action="freeze_next_research_candidate_spec",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Intake is a research entry point only.",
            "It does not create a paper-shadow candidate.",
        ],
        requested_date_range=requested_range,
    )


def build_next_candidate_spec_frozen_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    intake_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if intake_payload is None:
        intake_path = default_next_research_cycle_json_path(
            INTAKE_REPORT_TYPE,
            reports_dir,
            as_of,
        )
        intake_payload = _read_json_mapping(intake_path)
    reset_draft = _read_json_mapping(
        reset_reports.default_return_to_research_json_path(
            reset_reports.NEXT_CANDIDATE_SPEC_REPORT_TYPE,
            reports_dir,
            as_of,
        )
    )
    reset_plan = _read_json_mapping(
        reset_reports.default_return_to_research_json_path(
            reset_reports.RESEARCH_BACKFILL_PLAN_REPORT_TYPE,
            reports_dir,
            as_of,
        )
    )
    candidate_specs = _records(reset_draft.get("candidate_specs"))
    if not candidate_specs:
        raise ValueError("next_candidate_spec_draft has no candidate_specs rows")
    selected = candidate_specs[0]
    windows = _records(reset_plan.get("required_backfill_windows"))
    frozen_id = _frozen_candidate_id(_text(selected.get("candidate_id")))
    p0_hypotheses = _records(intake_payload.get("p0_hypotheses"))
    stop_conditions = [
        _text(row.get("stop_condition"))
        for row in p0_hypotheses
        if _text(row.get("stop_condition"))
    ]
    stop_conditions.extend(
        [
            "stop if cost/benchmark review remains unavailable or weak",
            "stop if signal completeness is warning/blocking",
            "stop if owner has not approved continued research validation",
        ]
    )
    frozen_spec = {
        "candidate_id": frozen_id,
        "source_draft_candidate_id": _text(selected.get("candidate_id")),
        "status": "FROZEN_RESEARCH_SPEC",
        "source_hypotheses": _list_values(selected.get("source_hypotheses")),
        "selected_p0_hypotheses": [
            {
                "hypothesis_id": _text(row.get("hypothesis_id")),
                "name": _text(row.get("name")),
                "expected_improvement": _text(row.get("expected_improvement")),
            }
            for row in p0_hypotheses
        ],
        "signal_inputs": _list_values(selected.get("signal_inputs")),
        "regime_assumptions": _list_values(selected.get("regime_filter_assumptions")),
        "drawdown_handling": _list_values(selected.get("drawdown_handling")),
        "rotation_handling": _list_values(selected.get("rotation_handling")),
        "turnover_constraints": _list_values(selected.get("turnover_constraints")),
        "cost_expectations": _list_values(selected.get("cost_sensitivity_expectations")),
        "benchmark_expectations": _list_values(
            selected.get("benchmark_comparison_expectations")
        ),
        "validation_windows": windows,
        "stop_conditions": stop_conditions,
        "market_regime": MARKET_REGIME,
        "ai_regime_start": AI_REGIME_START,
        "paper_shadow_eligible": False,
        "requires_executable_signal_binding_before_backfill": True,
        "executable_signal_binding_path": "",
        "executable_signal_binding_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
    }
    summary = {
        "frozen_spec_status": "NEXT_CANDIDATE_SPEC_FROZEN",
        "candidate_id": frozen_id,
        "source_draft_candidate_id": _text(selected.get("candidate_id")),
        "source_p0_hypothesis_count": len(p0_hypotheses),
        "validation_window_count": len(windows),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _date_range_from_windows(windows),
        "paper_shadow_eligible": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=FROZEN_SPEC_REPORT_TYPE,
        as_of=as_of,
        status=summary["frozen_spec_status"],
        purpose="Freeze one research-only next candidate spec from the P0 backlog.",
        input_artifacts={
            "next_research_cycle_intake": _artifact_id(intake_payload),
            "next_candidate_spec_draft": _artifact_id(reset_draft),
            "research_backfill_plan_for_next_candidate": _artifact_id(reset_plan),
        },
        output_decision=summary["frozen_spec_status"],
        summary=summary,
        body={"frozen_candidate_spec": frozen_spec},
        reader_brief=_reader_brief(
            summary=(
                f"Frozen research spec {frozen_id} is not paper-shadow eligible."
            ),
            key_result=summary["frozen_spec_status"],
            blocking_issues="executable_signal_binding_required_before_backfill",
            warnings="research spec only; no official weights or broker/order path",
            next_action="run_research_only_backfill_after_data_quality_gate",
        ),
        next_action="run_research_only_backfill_after_data_quality_gate",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Frozen spec is research-only.",
            "It is not paper-shadow eligible without separate validation and owner review.",
            "It intentionally does not define official target weights.",
        ],
        requested_date_range=summary["requested_date_range"],
    )


def build_next_candidate_backfill_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    frozen_spec_payload: Mapping[str, Any] | None = None,
    data_quality_gate: Mapping[str, Any] | None = None,
    prices_path: Path = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
) -> dict[str, Any]:
    if frozen_spec_payload is None:
        frozen_spec_payload = _read_json_mapping(
            default_next_research_cycle_json_path(
                FROZEN_SPEC_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        )
    frozen_spec = _mapping(frozen_spec_payload.get("frozen_candidate_spec"))
    windows = _records(frozen_spec.get("validation_windows"))
    data_quality = _normalize_data_quality_gate(data_quality_gate)
    data_quality_passed = data_quality["passed"]
    binding_inputs = _load_executable_backfill_inputs(
        reports_dir=reports_dir,
        as_of=as_of,
    )
    binding_blockers = _binding_backfill_blockers(binding_inputs)
    current_weight = _mapping(
        _mapping(binding_inputs.get("weight_payload")).get("hypothetical_research_weight")
    )
    previous_weight = _mapping(
        _mapping(binding_inputs.get("weight_payload")).get("previous_hypothetical_weight")
    )
    weight_map = _weight_values(current_weight)
    previous_weight_map = _weight_values(previous_weight)
    weight_symbols = sorted(symbol for symbol in weight_map if symbol != "CASH")
    price_history: dict[str, dict[date, float]] = {}
    price_blockers: list[str] = []
    if data_quality_passed and not binding_blockers and weight_symbols:
        price_history, price_blockers = _read_price_history(
            prices_path=prices_path,
            symbols=weight_symbols,
        )
    else:
        price_blockers = []
    window_results = []
    for window in windows:
        if data_quality_passed and not binding_blockers and not price_blockers:
            window_results.append(
                _executable_backfill_window_result(
                    window,
                    price_history=price_history,
                    weight_map=weight_map,
                    previous_weight_map=previous_weight_map,
                    signal_rows=_records(
                        _mapping(binding_inputs.get("signal_payload")).get(
                            "candidate_signal_series"
                        )
                    ),
                    weight_rows=_records(
                        _mapping(binding_inputs.get("weight_payload")).get(
                            "hypothetical_research_weight_series"
                        )
                    ),
                )
            )
        else:
            window_results.append(
                _blocked_backfill_window_result(
                    window,
                    data_quality=data_quality,
                    missing_inputs=[
                        *binding_blockers,
                        *price_blockers,
                    ],
                )
            )
    missing_data = sorted(
        {
            item
            for row in window_results
            for item in _list_values(row.get("missing_data_list"))
        }
    )
    metric_windows = [
        row
        for row in window_results
        if row.get("return_proxy") is not None and row.get("drawdown_proxy") is not None
    ]
    partial_reasons = _backfill_partial_reasons(
        window_results=window_results,
        signal_payload=_mapping(binding_inputs.get("signal_payload")),
        weight_payload=_mapping(binding_inputs.get("weight_payload")),
    )
    blocking_issues = []
    if not data_quality_passed:
        blocking_issues.append("data_quality_gate_not_passed")
    blocking_issues.extend(binding_blockers)
    blocking_issues.extend(price_blockers)
    if not metric_windows and not blocking_issues:
        blocking_issues.append("no_backfill_metrics_computed")
    if blocking_issues:
        status = CANDIDATE_BACKFILL_BLOCKED
    elif partial_reasons:
        status = CANDIDATE_BACKFILL_PARTIAL
    else:
        status = CANDIDATE_BACKFILL_COMPLETE
    aggregate_metrics = _aggregate_backfill_metrics(
        window_results,
        turnover_proxy=_weight_turnover(weight_map, previous_weight_map),
        signal_payload=_mapping(binding_inputs.get("signal_payload")),
        weight_payload=_mapping(binding_inputs.get("weight_payload")),
    )
    summary = {
        "candidate_backfill_status": status,
        "candidate_id": _text(frozen_spec.get("candidate_id")),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _date_range_from_windows(windows),
        "window_count": len(window_results),
        "completed_window_count": len(
            [row for row in window_results if row.get("backfill_window_status") == "READY"]
        ),
        "partial_window_count": len(
            [row for row in window_results if row.get("backfill_window_status") == "PARTIAL"]
        ),
        "data_quality_status": data_quality["status"],
        "data_quality_report_path": data_quality["report_path"],
        "safety_audit_status": _text(
            _mapping(binding_inputs.get("safety_payload")).get("status"),
            "MISSING",
        ),
        "safety_audit_validation_status": _text(
            _mapping(binding_inputs.get("safety_validation")).get("status"),
            "MISSING",
        ),
        "backfill_metric_mode": (
            "static_hypothetical_research_weight_proxy"
            if metric_windows
            else "blocked_no_metrics"
        ),
        "real_metrics_generated": bool(metric_windows),
        "return_proxy_available": bool(metric_windows),
        "drawdown_proxy_available": bool(metric_windows),
        "turnover_available": bool(metric_windows),
        "aggregate_return_proxy": aggregate_metrics.get("aggregate_return_proxy"),
        "aggregate_drawdown_proxy": aggregate_metrics.get("aggregate_drawdown_proxy"),
        "turnover_proxy": aggregate_metrics.get("turnover_proxy"),
        "rotation_count": aggregate_metrics.get("rotation_count"),
        "false_risk_off_count": aggregate_metrics.get("false_risk_off_count"),
        "constraint_hit_count": aggregate_metrics.get("constraint_hit_count"),
        "signal_completeness": aggregate_metrics.get("signal_completeness"),
        "signal_completeness_ratio": aggregate_metrics.get("signal_completeness_ratio"),
        "missing_data_count": len(missing_data),
        "partial_reason_count": len(partial_reasons),
        "paper_shadow_outputs_generated": False,
        "official_target_weights_generated": False,
        "broker_order_generated": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=BACKFILL_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Run the research-only backfill for the frozen candidate spec using "
            "validated executable signal and research-weight bindings."
        ),
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
            "data_quality_report": data_quality["report_path"],
            **_binding_input_artifact_paths(binding_inputs),
            "prices_path": str(prices_path),
        },
        output_decision=status,
        summary=summary,
        body={
            "backfill_windows": window_results,
            "aggregate_metrics": aggregate_metrics,
            "missing_data_list": missing_data,
            "partial_reasons": [
                {"issue_id": issue, "recommended_action": _backfill_blocker_action(issue)}
                for issue in partial_reasons
            ],
            "cost_proxy_inputs": _cost_proxy_inputs(bool(metric_windows)),
            "blocking_issues": [
                {"issue_id": issue, "recommended_action": _backfill_blocker_action(issue)}
                for issue in blocking_issues
            ],
        },
        reader_brief=_reader_brief(
            summary=(
                f"Backfill status is {status}; metrics are research-only and "
                "not official allocation outputs."
            ),
            key_result=status,
            blocking_issues="; ".join(blocking_issues) or "none",
            warnings=(
                "; ".join(partial_reasons)
                if partial_reasons
                else f"data_quality_status={data_quality['status']}"
            ),
            next_action=(
                "repair_backfill_inputs_before_research_rerun"
                if blocking_issues
                else "run_stress_cost_benchmark_review_from_real_metrics"
            ),
        ),
        next_action=(
            "repair_backfill_inputs_before_research_rerun"
            if blocking_issues
            else "run_stress_cost_benchmark_review_from_real_metrics"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "research_only_binding_backfill",
            "hypothetical_research_weights_used": bool(metric_windows),
            "backfill_metrics_generated": bool(metric_windows),
            "official_target_weights_generated": False,
            "broker_order_generated": False,
            "paper_shadow_activation_allowed": False,
        },
        limitations=[
            "Backfill output is research-only and cannot produce official weights.",
            (
                "Current executable binding contains one latest signal/weight row; "
                "computed metrics are static research-weight proxies, not a full "
                "historical dynamic strategy path."
            ),
            "No paper-shadow, broker/order, owner decision, or production state is touched.",
        ],
        requested_date_range=summary["requested_date_range"],
    )


def build_next_candidate_stress_review_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    project_root: Path = PROJECT_ROOT,
    frozen_spec_payload: Mapping[str, Any] | None = None,
    backfill_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if frozen_spec_payload is None:
        frozen_spec_payload = _read_json_mapping(
            default_next_research_cycle_json_path(
                FROZEN_SPEC_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        )
    if backfill_payload is None:
        backfill_payload = _read_json_mapping(
            default_next_research_cycle_json_path(BACKFILL_REPORT_TYPE, reports_dir, as_of)
        )
    sources = _load_project_sources(project_root)
    backfill_summary = _mapping(backfill_payload.get("summary"))
    executable_backfill = _backfill_metrics_available(
        _text(backfill_summary.get("candidate_backfill_status"))
    )
    backfill_windows_by_id = {
        _text(row.get("window_id")): row
        for row in _records(backfill_payload.get("backfill_windows"))
    }
    scenario_reviews = [
        _stress_scenario_review(
            window_id,
            executable_backfill=executable_backfill,
            window_metrics=backfill_windows_by_id.get(window_id, {}),
            backfill_status=_text(backfill_summary.get("candidate_backfill_status")),
        )
        for window_id in REQUIRED_BACKFILL_WINDOWS
    ]
    blocking = [
        row for row in scenario_reviews if row["scenario_status"] in {"BLOCKING", "FAIL"}
    ]
    warnings = [row for row in scenario_reviews if row["scenario_status"] == "WARNING"]
    stress_result = (
        "WEAK"
        if blocking
        else "MIXED_WITH_WARNINGS"
        if warnings
        else "STRESS_REVIEW_PASS"
    )
    summary = {
        "stress_result": stress_result,
        "candidate_id": _candidate_id_from_frozen(frozen_spec_payload),
        "scenario_count": len(scenario_reviews),
        "blocking_scenario_count": len(blocking),
        "warning_scenario_count": len(warnings),
        "source_backfill_status": _text(backfill_summary.get("candidate_backfill_status")),
        "partial_static_proxy": _text(backfill_summary.get("backfill_metric_mode"))
        == "static_hypothetical_research_weight_proxy",
        "worst_drawdown_proxy": min(
            [_float(row.get("drawdown_proxy")) for row in scenario_reviews],
            default=0.0,
        ),
        "worst_return_proxy": min(
            [_float(row.get("return_proxy")) for row in scenario_reviews],
            default=0.0,
        ),
        "major_blocker_count": len(blocking),
        "major_warning_count": len(warnings),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _text(backfill_summary.get("requested_date_range")),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=STRESS_REVIEW_REPORT_TYPE,
        as_of=as_of,
        status=stress_result,
        purpose=(
            "Evaluate the frozen next candidate against stress scenarios and "
            "historical casebooks."
        ),
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
            "next_candidate_backfill": _artifact_id(backfill_payload),
            **_source_paths(sources),
        },
        output_decision=stress_result,
        summary=summary,
        body={
            "scenario_reviews": scenario_reviews,
            "blocking_scenarios": blocking,
            "warning_scenarios": warnings,
            "reusable_positive_evidence": _reusable_positive_stress_evidence(sources),
            "failure_cases": blocking,
            "major_blockers": [
                {"issue_id": _text(row.get("scenario_id")), "reason": row.get("evaluation")}
                for row in blocking
            ],
            "major_warnings": [
                {"issue_id": _text(row.get("scenario_id")), "reason": row.get("evaluation")}
                for row in warnings
            ],
        },
        reader_brief=_reader_brief(
            summary=f"Stress review is {stress_result} from binding-backed backfill metrics.",
            key_result=stress_result,
            blocking_issues=_issue_names(blocking, "scenario_id"),
            warnings=_issue_names(warnings, "scenario_id"),
            next_action="review_cost_benchmark_after_stress_metrics",
        ),
        next_action="review_cost_benchmark_after_stress_metrics",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Stress review does not optimize the candidate.",
            "Partial static-proxy backfill metrics cannot prove full dynamic robustness.",
        ],
        requested_date_range=summary["requested_date_range"],
    )


def build_next_candidate_cost_benchmark_review_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    backfill_payload: Mapping[str, Any],
) -> dict[str, Any]:
    sources = _load_project_sources(project_root)
    cost_payload = _mapping(_mapping(sources.get("cost_sensitivity_framework")).get("payload"))
    benchmark_payload = _mapping(
        _mapping(sources.get("benchmark_baseline_control")).get("payload")
    )
    backfill_status = _text(
        _mapping(backfill_payload.get("summary")).get("candidate_backfill_status")
    )
    backfill_ready = _backfill_metrics_available(backfill_status)
    scenario_rows = _cost_scenario_reviews(
        cost_payload,
        backfill_payload=backfill_payload,
        backfill_ready=backfill_ready,
    )
    baseline_rows = _benchmark_reviews(
        benchmark_payload,
        backfill_payload=backfill_payload,
        backfill_ready=backfill_ready,
    )
    cost_blockers = [
        row
        for row in scenario_rows
        if _text(row.get("cost_survival_status")) in {"COST_SURVIVAL_FAIL", "UNTESTED"}
    ]
    cost_warnings = [
        row
        for row in scenario_rows
        if _text(row.get("cost_survival_status")) == "COST_SURVIVAL_WARNING"
    ]
    benchmark_blockers = [
        row
        for row in baseline_rows
        if _text(row.get("benchmark_relative_status"))
        in {"BENCHMARK_UNDERPERFORMS", "UNTESTED"}
    ]
    benchmark_warnings = [
        row
        for row in baseline_rows
        if _text(row.get("benchmark_relative_status")) == "BENCHMARK_MIXED"
    ]
    status = (
        "COST_BENCHMARK_REVIEW_WEAK"
        if cost_blockers or benchmark_blockers
        else "COST_BENCHMARK_REVIEW_MIXED"
        if cost_warnings or benchmark_warnings or backfill_status == CANDIDATE_BACKFILL_PARTIAL
        else "COST_BENCHMARK_REVIEW_PASS"
    )
    cost_survival_status = (
        "COST_SURVIVAL_UNTESTED_WITHOUT_EXECUTABLE_BACKFILL"
        if not backfill_ready
        else "COST_SURVIVAL_FAIL"
        if cost_blockers
        else "COST_SURVIVAL_WARNING"
        if cost_warnings or backfill_status == CANDIDATE_BACKFILL_PARTIAL
        else "COST_SURVIVAL_PASS"
    )
    benchmark_relative_status = (
        "RELATIVE_STATUS_UNTESTED_WITHOUT_EXECUTABLE_BACKFILL"
        if not backfill_ready
        else "BENCHMARK_UNDERPERFORMS"
        if benchmark_blockers
        else "BENCHMARK_MIXED"
        if benchmark_warnings or backfill_status == CANDIDATE_BACKFILL_PARTIAL
        else "BENCHMARK_OUTPERFORMS"
    )
    status = (
        status
        if backfill_ready
        else "COST_BENCHMARK_NEEDS_EXECUTABLE_BACKFILL"
    )
    summary = {
        "cost_survival_status": cost_survival_status,
        "benchmark_relative_status": benchmark_relative_status,
        "turnover_penalty": _cost_turnover_penalty(scenario_rows),
        "net_proxy_result": "UNAVAILABLE" if not backfill_ready else "AVAILABLE",
        "source_backfill_status": backfill_status,
        "aggregate_return_proxy": _mapping(backfill_payload.get("summary")).get(
            "aggregate_return_proxy"
        ),
        "turnover_proxy": _mapping(backfill_payload.get("summary")).get("turnover_proxy"),
        "scenario_count": len(scenario_rows),
        "baseline_count": len(baseline_rows),
        "major_blocker_count": len(cost_blockers) + len(benchmark_blockers),
        "major_warning_count": len(cost_warnings) + len(benchmark_warnings),
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=COST_BENCHMARK_REVIEW_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Evaluate cost survival and benchmark comparison for the frozen candidate "
            "without optimizing it."
        ),
        input_artifacts={
            "next_candidate_backfill": _artifact_id(backfill_payload),
            **_source_paths(
                {
                    key: value
                    for key, value in sources.items()
                    if key in {"cost_sensitivity_framework", "benchmark_baseline_control"}
                }
            ),
        },
        output_decision=status,
        summary=summary,
        body={
            "cost_scenario_reviews": scenario_rows,
            "benchmark_reviews": baseline_rows,
            "major_blockers": [
                *[
                    {
                        "issue_id": _text(row.get("scenario_id")),
                        "reason": _text(row.get("cost_survival_status")),
                    }
                    for row in cost_blockers
                ],
                *[
                    {
                        "issue_id": _text(row.get("baseline_id")),
                        "reason": _text(row.get("benchmark_relative_status")),
                    }
                    for row in benchmark_blockers
                ],
            ],
            "major_warnings": [
                *[
                    {
                        "issue_id": _text(row.get("scenario_id")),
                        "reason": _text(row.get("cost_survival_status")),
                    }
                    for row in cost_warnings
                ],
                *[
                    {
                        "issue_id": _text(row.get("baseline_id")),
                        "reason": _text(row.get("benchmark_relative_status")),
                    }
                    for row in benchmark_warnings
                ],
            ],
            "blocking_issues": [
                {
                    "issue_id": "candidate_net_proxy_unavailable",
                    "recommended_action": (
                        "complete_research_backfill_before_cost_benchmark_review"
                    ),
                }
            ]
            if not backfill_ready
            else [],
        },
        reader_brief=_reader_brief(
            summary=f"Cost/benchmark review is {status} from real backfill metrics.",
            key_result=status,
            blocking_issues=(
                "candidate_net_proxy_unavailable"
                if not backfill_ready
                else _review_issue_names(cost_blockers + benchmark_blockers)
            ),
            warnings=_review_issue_names(cost_warnings + benchmark_warnings),
            next_action="compare_against_returned_candidate_with_real_metrics",
        ),
        next_action="compare_against_returned_candidate_with_real_metrics",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Evaluation only; does not tune or optimize the candidate.",
            "Partial static-proxy backfill limits benchmark interpretation.",
        ],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_vs_returned_comparison_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
) -> dict[str, Any]:
    failure = _read_json_mapping(
        reset_reports.default_return_to_research_json_path(
            reset_reports.FAILURE_MODE_ATTRIBUTION_REPORT_TYPE,
            reports_dir,
            as_of,
        )
    )
    reusable = _read_json_mapping(
        reset_reports.default_return_to_research_json_path(
            reset_reports.REUSABLE_EVIDENCE_REPORT_TYPE,
            reports_dir,
            as_of,
        )
    )
    backfill_status = _text(
        _mapping(backfill_payload.get("summary")).get("candidate_backfill_status")
    )
    backfill_ready = _backfill_metrics_available(backfill_status)
    comparison_rows = _comparison_rows(
        backfill_payload=backfill_payload,
        stress_review_payload=stress_review_payload,
        cost_benchmark_payload=cost_benchmark_payload,
        failure_payload=failure,
        reusable_payload=reusable,
        backfill_ready=backfill_ready,
    )
    comparison_result = _comparison_result(comparison_rows, backfill_ready)
    blocking_issue_names = _comparison_issue_names(
        comparison_rows,
        {
            "NO_IMPROVEMENT",
            "REGRESSED_VS_REUSABLE_EVIDENCE",
            "REPEATS_FAILURE_MODE",
            "UNMEASURED",
        },
    )
    warning_names = _comparison_issue_names(comparison_rows, {"MIXED"})
    summary = {
        "comparison_result": comparison_result,
        "previous_candidate_id": reset_reports.CANDIDATE_ID,
        "new_candidate_id": _text(_mapping(backfill_payload.get("summary")).get("candidate_id")),
        "real_metrics_available": backfill_ready,
        "measurable_improvement_established": comparison_result
        == "IMPROVED_OVER_RETURNED_CANDIDATE",
        "source_backfill_status": backfill_status,
        "stress_result": _text(
            _mapping(stress_review_payload.get("summary")).get("stress_result"),
            _text(stress_review_payload.get("status")),
        ),
        "cost_survival_status": _text(
            _mapping(cost_benchmark_payload.get("summary")).get("cost_survival_status")
        ),
        "benchmark_relative_status": _text(
            _mapping(cost_benchmark_payload.get("summary")).get(
                "benchmark_relative_status"
            )
        ),
        "repeated_failure_mode_count": len(
            [
                row
                for row in comparison_rows
                if row["comparison_status"] == "REPEATS_FAILURE_MODE"
            ]
        ),
        "improved_metric_count": len(
            [row for row in comparison_rows if row["comparison_status"] == "IMPROVED"]
        ),
        "mixed_metric_count": len(
            [row for row in comparison_rows if row["comparison_status"] == "MIXED"]
        ),
        "no_improvement_count": len(
            [
                row
                for row in comparison_rows
                if row["comparison_status"]
                in {"NO_IMPROVEMENT", "REGRESSED_VS_REUSABLE_EVIDENCE"}
            ]
        ),
        "governance_blockers": blocking_issue_names,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=VS_RETURNED_REPORT_TYPE,
        as_of=as_of,
        status=comparison_result,
        purpose="Compare the new research candidate against the returned candidate.",
        input_artifacts={
            "candidate_failure_mode_attribution": _artifact_id(failure),
            "reusable_evidence_extraction": _artifact_id(reusable),
            "next_candidate_backfill": _artifact_id(backfill_payload),
            "next_candidate_stress_review": _artifact_id(stress_review_payload),
            "next_candidate_cost_benchmark_review": _artifact_id(cost_benchmark_payload),
        },
        output_decision=comparison_result,
        summary=summary,
        body={
            "comparison_rows": comparison_rows,
            "returned_candidate_failure_modes": _records(failure.get("ranked_failure_modes")),
            "reusable_previous_evidence": _records(reusable.get("reusable_evidence")),
        },
        reader_brief=_reader_brief(
            summary=(
                f"Vs-returned comparison is {comparison_result} from real "
                "binding-backed metrics."
            ),
            key_result=comparison_result,
            blocking_issues=summary["governance_blockers"],
            warnings=warning_names,
            next_action="run_signal_robustness_and_window_sensitivity",
        ),
        next_action="run_signal_robustness_and_window_sensitivity",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Comparison uses research-only partial/static proxy metrics.",
            "Returned candidate numeric metric history is available as failure attribution, "
            "not as a complete normalized metric table.",
            "No improvement claim can activate paper-shadow or official weights.",
        ],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_signal_robustness_review_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    project_root: Path = PROJECT_ROOT,
    frozen_spec_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
    signal_binding_payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sources = _load_project_sources(project_root)
    signal_source = _mapping(sources.get("signal_input_completeness"))
    if signal_binding_payload is None:
        signal_binding_path = binding_reports.default_executable_binding_json_path(
            binding_reports.SIGNAL_BINDING_REPORT_TYPE,
            reports_dir,
            as_of,
        )
        signal_binding_payload = (
            _read_json_mapping(signal_binding_path)
            if signal_binding_path.exists()
            else {}
        )
    signal_summary = _mapping(signal_binding_payload.get("summary"))
    signal_findings = _records(signal_binding_payload.get("signal_input_findings"))
    signal_rows = _records(signal_binding_payload.get("candidate_signal_series"))
    backfill_summary = _mapping(backfill_payload.get("summary"))
    warning_reasons = _list_values(signal_binding_payload.get("warning_reasons"))
    checks = [
        _signal_robustness_check(
            "missing_feature_columns",
            blocked=_signal_missing_feature_columns(signal_findings),
            warning=False,
            evidence="required feature/signal columns present in signal binding findings",
        ),
        _signal_robustness_check(
            "partial_signal_series",
            blocked=_text(backfill_summary.get("signal_completeness")) != "COMPLETE",
            warning=False,
            evidence=(
                f"signal_row_count={signal_summary.get('signal_row_count')}; "
                f"backfill_signal_completeness={backfill_summary.get('signal_completeness')}"
            ),
        ),
        _signal_robustness_check(
            "stale_signal_series",
            blocked="signal_date_outside_frozen_validation_windows" in warning_reasons,
            warning=_signal_staleness_warning(signal_findings),
            evidence=(
                f"latest_signal_date={signal_summary.get('latest_signal_date')}; "
                f"warning_reasons={','.join(warning_reasons)}"
            ),
        ),
        _signal_robustness_check(
            "schema_version_mismatch",
            blocked=_signal_schema_mismatch(signal_findings),
            warning=False,
            evidence="schema and feature versions inspected from signal binding findings",
        ),
        _signal_robustness_check(
            "market_coverage_gap",
            blocked=_signal_market_coverage_gap(signal_findings, backfill_payload),
            warning=False,
            evidence=f"missing_data_count={backfill_summary.get('missing_data_count')}",
        ),
    ]
    blocking = [row for row in checks if row["status"] == "BLOCKING"]
    warnings = [row for row in checks if row["status"] == "WARNING"]
    status = (
        "SIGNAL_ROBUSTNESS_BLOCKED"
        if blocking
        else "SIGNAL_ROBUSTNESS_WARNING"
        if warnings
        else "SIGNAL_ROBUSTNESS_PASS"
    )
    summary = {
        "signal_robustness_status": status,
        "source_signal_binding_status": _text(signal_binding_payload.get("status"), "MISSING"),
        "signal_row_count": len(signal_rows),
        "signal_input_status": _text(signal_summary.get("signal_input_status"), "MISSING"),
        "backfill_signal_completeness": _text(backfill_summary.get("signal_completeness")),
        "historical_signal_series_available": _text(
            backfill_summary.get("signal_completeness")
        )
        == "COMPLETE",
        "fail_closed_behavior": True,
        "blocking_check_count": len(blocking),
        "warning_check_count": len(warnings),
        "sensitivity_to_missing_inputs": "HIGH" if blocking else "LOW",
        "required_monitoring_field_count": 5,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=SIGNAL_ROBUSTNESS_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Check whether the frozen next candidate is robust to signal input quality.",
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
            "next_candidate_backfill": _artifact_id(backfill_payload),
            "next_candidate_signal_binding": _artifact_id(signal_binding_payload),
            **_source_paths({"signal_input_completeness": signal_source}),
        },
        output_decision=status,
        summary=summary,
        body={
            "signal_quality_checks": checks,
            "required_monitoring_fields": [
                "feature_columns_present",
                "signal_series_completeness",
                "signal_series_freshness",
                "schema_version",
                "market_coverage",
            ],
            "fail_closed_behavior": {
                "missing_or_stale_inputs_block_research_gate": True,
                "signal_completeness_rules_relaxed": False,
            },
        },
        reader_brief=_reader_brief(
            summary=f"Signal robustness is {status} from executable binding inputs.",
            key_result=status,
            blocking_issues=_issue_names(blocking, "check_id"),
            warnings=_issue_names(warnings, "check_id"),
            next_action="review_window_sensitivity_before_research_gate",
        ),
        next_action="review_window_sensitivity_before_research_gate",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Review does not relax signal completeness rules.",
            "One latest signal row is not a historical signal series for backfill windows.",
        ],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_window_sensitivity_payload(
    *,
    as_of: date,
    frozen_spec_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
) -> dict[str, Any]:
    backfill_summary = _mapping(backfill_payload.get("summary"))
    backfill_status = _text(backfill_summary.get("candidate_backfill_status"))
    backfill_ready = _backfill_metrics_available(backfill_status)
    backfill_windows = _records(backfill_payload.get("backfill_windows"))
    windows_by_id = {_text(row.get("window_id")): row for row in backfill_windows}
    splits = [
        _window_sensitivity_split(
            split_id,
            windows_by_id,
            backfill_ready=backfill_ready,
        )
        for split_id in WINDOW_SENSITIVITY_SPLITS
    ]
    weak_splits = [row for row in splits if row["status"] == "WEAK"]
    partial_splits = [row for row in splits if row["status"] == "PARTIAL_STATIC_PROXY"]
    unavailable_splits = [row for row in splits if row["status"] == "METRICS_UNAVAILABLE"]
    mixed_splits = [row for row in splits if row["status"] == "MIXED"]
    status = (
        "WINDOW_FRAGILE"
        if weak_splits or partial_splits or unavailable_splits
        else "WINDOW_MIXED"
        if mixed_splits
        else "WINDOW_STABLE"
    )
    performance_values = [
        _float(row.get("average_return_proxy"))
        for row in splits
        if row.get("average_return_proxy") is not None
    ]
    turnover_values = [
        _float(row.get("average_turnover_proxy"))
        for row in splits
        if row.get("average_turnover_proxy") is not None
    ]
    drawdown_values = [
        _float(row.get("worst_drawdown_proxy"))
        for row in splits
        if row.get("worst_drawdown_proxy") is not None
    ]
    false_flip_values = [
        _int(row.get("false_flip_proxy"))
        for row in splits
        if row.get("false_flip_proxy") is not None
    ]
    blocking_issue_ids = [
        _text(row.get("window_split_id"))
        for row in [*weak_splits, *partial_splits, *unavailable_splits]
    ]
    warning_issue_ids = [_text(row.get("window_split_id")) for row in mixed_splits]
    overfit_risk = "HIGH" if status == "WINDOW_FRAGILE" else "MEDIUM" if mixed_splits else "LOW"
    summary = {
        "window_sensitivity_status": status,
        "source_backfill_status": backfill_status,
        "source_backfill_metric_mode": _text(backfill_summary.get("backfill_metric_mode")),
        "split_count": len(splits),
        "weak_split_count": len(weak_splits),
        "partial_static_proxy_split_count": len(partial_splits),
        "unavailable_split_count": len(unavailable_splits),
        "mixed_split_count": len(mixed_splits),
        "performance_dispersion": _range_or_none(performance_values),
        "turnover_dispersion": _range_or_none(turnover_values),
        "drawdown_behavior_dispersion": _range_or_none(drawdown_values),
        "false_flip_dispersion": _range_or_none(false_flip_values),
        "overfit_risk": overfit_risk,
        "blocking_conditions": "; ".join(blocking_issue_ids) or "none",
        "warning_conditions": "; ".join(warning_issue_ids) or "none",
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=WINDOW_SENSITIVITY_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Check whether the next candidate only works in narrow windows.",
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
            "next_candidate_backfill": _artifact_id(backfill_payload),
        },
        output_decision=status,
        summary=summary,
        body={
            "window_splits": splits,
            "blocking_issues": [
                {
                    "issue_id": issue_id,
                    "recommended_action": "complete_dynamic_binding_before_window_stability_claim",
                }
                for issue_id in blocking_issue_ids
            ],
            "warning_issues": [
                {
                    "issue_id": issue_id,
                    "recommended_action": "review_window_metric_dispersion",
                }
                for issue_id in warning_issue_ids
            ],
        },
        reader_brief=_reader_brief(
            summary=(
                f"Window sensitivity is {status}; overfit risk is {overfit_risk} "
                "from executable backfill window metrics."
            ),
            key_result=status,
            blocking_issues=summary["blocking_conditions"],
            warnings=summary["warning_conditions"],
            next_action="rerun_research_gate_with_signal_and_window_reviews",
        ),
        next_action="rerun_research_gate_with_signal_and_window_reviews",
        safety_boundary=_safety_boundary(),
        limitations=[
            "Window sensitivity uses research-only executable backfill metrics.",
            (
                "Partial static-proxy windows remain fragile because historical "
                "dynamic signal/weight rows are unavailable."
            ),
            "No window result can activate paper-shadow or official weights.",
        ],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_research_gate_payload(
    *,
    as_of: date,
    frozen_spec_payload: Mapping[str, Any],
    safety_audit_payload: Mapping[str, Any] | None = None,
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
) -> dict[str, Any]:
    safety_audit = _mapping(safety_audit_payload)
    blockers = _research_gate_blockers(
        safety_audit_payload=safety_audit,
        backfill_payload=backfill_payload,
        stress_review_payload=stress_review_payload,
        cost_benchmark_payload=cost_benchmark_payload,
        comparison_payload=comparison_payload,
        signal_robustness_payload=signal_robustness_payload,
        window_sensitivity_payload=window_sensitivity_payload,
    )
    decision = _research_gate_decision(
        blockers=blockers,
        comparison_payload=comparison_payload,
    )
    source_statuses = _research_gate_source_statuses(
        safety_audit_payload=safety_audit,
        backfill_payload=backfill_payload,
        stress_review_payload=stress_review_payload,
        cost_benchmark_payload=cost_benchmark_payload,
        comparison_payload=comparison_payload,
        signal_robustness_payload=signal_robustness_payload,
        window_sensitivity_payload=window_sensitivity_payload,
    )
    positive_evidence = _research_gate_positive_evidence(source_statuses)
    negative_evidence = _research_gate_negative_evidence(blockers)
    required_next_action = _research_gate_required_next_action(decision, blockers)
    summary = {
        "research_gate_decision": decision,
        "candidate_id": _candidate_id_from_frozen(frozen_spec_payload),
        "safety_audit_status": source_statuses["executable_binding_safety_audit"],
        "source_backfill_status": source_statuses["next_candidate_backfill"],
        "stress_result": source_statuses["next_candidate_stress_review"],
        "cost_benchmark_status": source_statuses["next_candidate_cost_benchmark_review"],
        "vs_returned_status": source_statuses[
            "next_candidate_vs_returned_candidate_comparison"
        ],
        "signal_robustness_status": source_statuses[
            "next_candidate_signal_robustness_review"
        ],
        "window_sensitivity_status": source_statuses[
            "next_candidate_overfit_window_sensitivity"
        ],
        "blocker_count": len(blockers),
        "strongest_positive_evidence_count": len(positive_evidence),
        "strongest_negative_evidence_count": len(negative_evidence),
        "required_next_action": required_next_action,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=RESEARCH_GATE_REPORT_TYPE,
        as_of=as_of,
        status=decision,
        purpose="Decide whether the new research candidate deserves deeper validation.",
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
            "executable_binding_safety_audit": _artifact_id(safety_audit),
            "next_candidate_backfill": _artifact_id(backfill_payload),
            "next_candidate_stress_review": _artifact_id(stress_review_payload),
            "next_candidate_cost_benchmark_review": _artifact_id(cost_benchmark_payload),
            "next_candidate_vs_returned_candidate_comparison": _artifact_id(comparison_payload),
            "next_candidate_signal_robustness_review": _artifact_id(signal_robustness_payload),
            "next_candidate_overfit_window_sensitivity": _artifact_id(window_sensitivity_payload),
        },
        output_decision=decision,
        summary=summary,
        body={
            "source_statuses": source_statuses,
            "strongest_positive_evidence": positive_evidence,
            "strongest_negative_evidence": negative_evidence,
            "blocker_list": blockers,
            "required_next_action": required_next_action,
        },
        reader_brief=_reader_brief(
            summary=f"Research gate decision is {decision}; paper-shadow remains forbidden.",
            key_result=decision,
            blocking_issues=_issue_names(blockers, "issue_id"),
            warnings="research gate cannot activate paper-shadow",
            next_action=required_next_action,
        ),
        next_action=required_next_action,
        safety_boundary=_safety_boundary(),
        limitations=[
            "This research gate does not allow paper-shadow activation.",
            "Partial/static proxy evidence cannot be promoted to paper-shadow.",
            "Gate output remains research-only and cannot write official weights.",
        ],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_owner_research_review_packet_payload(
    *,
    as_of: date,
    research_gate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    gate_summary = _mapping(research_gate_payload.get("summary"))
    gate_decision = _text(gate_summary.get("research_gate_decision"))
    gate_blockers = _records(research_gate_payload.get("blocker_list"))
    gate_required_next_action = _text(
        gate_summary.get("required_next_action"),
        _text(research_gate_payload.get("next_action"), "MISSING"),
    )
    blocker_names = _issue_names(gate_blockers, "issue_id")
    options = [
        _owner_option(
            "continue_research_validation",
            evidence_required=[
                "repair or explain gate blockers before any promotion claim",
                gate_required_next_action,
                "fresh research gate rerun after blocker repair",
            ],
            risks=[
                "research time spent while current gate remains non-promising",
                f"current blockers remain: {blocker_names}",
            ],
            next_action=gate_required_next_action,
        ),
        _owner_option(
            "revise_hypothesis",
            evidence_required=[
                "owner-reviewed hypothesis revision",
                "updated frozen spec that addresses repeated failure modes",
                "new executable binding contract or policy revision if inputs change",
            ],
            risks=[
                "revision may move away from original P0 failure attribution",
                "revised hypothesis still may not survive cost/benchmark review",
            ],
            next_action="return_to_hypothesis_backlog_for_revision",
        ),
        _owner_option(
            "return_to_hypothesis_backlog",
            evidence_required=[
                "owner decision to stop this candidate path for now",
                "backlog note preserving current blockers and reusable evidence",
            ],
            risks=[
                "candidate-specific implementation work pauses",
                "useful executable binding infrastructure remains but candidate evidence resets",
            ],
            next_action="record_backlog_return_without_owner_decision_append",
        ),
        _owner_option(
            "reject_research_candidate",
            evidence_required=[
                "owner decision to reject research candidate",
                "documented rejection rationale",
            ],
            risks=[
                "may discard still-useful diagnostic evidence",
                "requires separate owner decision before any state change",
            ],
            next_action="create_research_rejection_postmortem",
        ),
        _owner_option(
            "hold_for_more_data",
            evidence_required=[
                "specific data/source or sample condition to wait for",
                "review date or exit condition for the hold",
            ],
            risks=[
                "research cycle remains incomplete",
                "current weak/fragile evidence remains unresolved during hold",
            ],
            next_action="wait_for_required_evidence_without_state_mutation",
        ),
    ]
    summary = {
        "owner_packet_status": "OWNER_RESEARCH_REVIEW_PACKET_READY",
        "source_research_gate_decision": gate_decision,
        "source_gate_blocker_count": len(gate_blockers),
        "source_gate_required_next_action": gate_required_next_action,
        "option_count": len(options),
        "owner_option_ids": [row["option_id"] for row in options],
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "order_ticket_generated": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=OWNER_REVIEW_PACKET_REPORT_TYPE,
        as_of=as_of,
        status=summary["owner_packet_status"],
        purpose="Prepare manual owner options for the research-stage candidate.",
        input_artifacts={"next_candidate_research_gate": _artifact_id(research_gate_payload)},
        output_decision=summary["owner_packet_status"],
        summary=summary,
        body={
            "source_gate_blockers": gate_blockers,
            "owner_options": options,
            "explicit_non_actions": {
                "paper_shadow_activation": False,
                "extended_shadow": False,
                "live_trading": False,
                "official_weights": False,
                "broker_order": False,
                "owner_decision_append": False,
                "production_mutation": False,
            },
        },
        reader_brief=_reader_brief(
            summary="Owner research review packet is ready; no decision is appended.",
            key_result=summary["owner_packet_status"],
            blocking_issues=blocker_names,
            warnings="manual owner decision required before any state transition",
            next_action="owner_review_research_options_manually",
        ),
        next_action="owner_review_research_options_manually",
        safety_boundary=_safety_boundary(),
        limitations=[
            "This packet does not append owner decisions automatically.",
            "No option in this packet activates paper-shadow, official weights, or broker/order.",
        ],
        requested_date_range=_text(research_gate_payload.get("requested_date_range")),
    )


def build_next_research_cycle_snapshot_payload(
    *,
    as_of: date,
    intake_payload: Mapping[str, Any],
    frozen_spec_payload: Mapping[str, Any],
    executable_contract_payload: Mapping[str, Any] | None = None,
    signal_binding_payload: Mapping[str, Any] | None = None,
    weight_binding_payload: Mapping[str, Any] | None = None,
    safety_audit_payload: Mapping[str, Any] | None = None,
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
    research_gate_payload: Mapping[str, Any],
    owner_packet_payload: Mapping[str, Any],
) -> dict[str, Any]:
    gate_decision = _text(
        _mapping(research_gate_payload.get("summary")).get("research_gate_decision")
    )
    executable_contract = _required_source_payload(
        executable_contract_payload,
        binding_reports.CONTRACT_REPORT_TYPE,
    )
    signal_binding = _required_source_payload(
        signal_binding_payload,
        binding_reports.SIGNAL_BINDING_REPORT_TYPE,
    )
    weight_binding = _required_source_payload(
        weight_binding_payload,
        binding_reports.WEIGHT_BINDING_REPORT_TYPE,
    )
    safety_audit = _required_source_payload(
        safety_audit_payload,
        binding_reports.SAFETY_AUDIT_REPORT_TYPE,
    )
    source_payloads = (
        intake_payload,
        frozen_spec_payload,
        executable_contract,
        signal_binding,
        weight_binding,
        safety_audit,
        backfill_payload,
        stress_review_payload,
        cost_benchmark_payload,
        comparison_payload,
        signal_robustness_payload,
        window_sensitivity_payload,
        research_gate_payload,
        owner_packet_payload,
    )
    source_statuses = _snapshot_source_statuses(source_payloads)
    missing_required = _snapshot_missing_required_artifacts(source_statuses)
    status = _executable_research_cycle_status(
        gate_decision=gate_decision,
        missing_required=missing_required,
    )
    summary = {
        "research_cycle_snapshot_status": status,
        "research_gate_decision": gate_decision,
        "candidate_id": _candidate_id_from_frozen(frozen_spec_payload),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _text(backfill_payload.get("requested_date_range")),
        "artifact_count": len(source_payloads),
        "executable_artifact_count": len(
            [executable_contract, signal_binding, weight_binding, safety_audit]
        ),
        "missing_required_artifact_count": len(missing_required),
        "missing_required_artifacts": missing_required,
        "owner_packet_ready": _text(owner_packet_payload.get("status"))
        == "OWNER_RESEARCH_REVIEW_PACKET_READY",
        "source_backfill_status": _text(backfill_payload.get("status"), "MISSING"),
        "stress_result": _text(stress_review_payload.get("status"), "MISSING"),
        "cost_benchmark_status": _text(cost_benchmark_payload.get("status"), "MISSING"),
        "vs_returned_status": _text(comparison_payload.get("status"), "MISSING"),
        "signal_robustness_status": _text(
            signal_robustness_payload.get("status"),
            "MISSING",
        ),
        "window_sensitivity_status": _text(
            window_sensitivity_payload.get("status"),
            "MISSING",
        ),
        "owner_packet_status": _text(owner_packet_payload.get("status"), "MISSING"),
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "owner_decision_appended": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CYCLE_SNAPSHOT_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Generate the final executable research-cycle snapshot.",
        input_artifacts={
            _text(item.get("report_type")): _artifact_id(item)
            for item in source_payloads
        },
        output_decision=status,
        summary=summary,
        body={
            "source_statuses": source_statuses,
            "source_reader_briefs": {
                _text(item.get("report_type")): _mapping(item.get("reader_brief"))
                for item in source_payloads
            },
            "missing_required_artifacts": missing_required,
            "final_interpretation": {
                "based_on_real_executable_binding": not missing_required,
                "fabricated_metrics": False,
                "paper_shadow_activation_allowed": False,
                "official_target_weights_generated": False,
                "broker_order_allowed": False,
                "production_effect": PRODUCTION_EFFECT,
            },
        },
        reader_brief=_reader_brief(
            summary=f"Executable research-cycle snapshot is {status}.",
            key_result=status,
            blocking_issues=(
                "; ".join(missing_required)
                if missing_required
                else "evidence_required_before_owner_ready"
                if status != "EXECUTABLE_RESEARCH_CYCLE_PROMISING"
                else "none"
            ),
            warnings="research-only; no paper-shadow/live/weights/broker approval",
            next_action=(
                "repair_missing_executable_research_cycle_artifacts"
                if missing_required
                else "complete_missing_research_evidence"
                if status != "EXECUTABLE_RESEARCH_CYCLE_PROMISING"
                else "manual_owner_research_review"
            ),
        ),
        next_action=(
            "repair_missing_executable_research_cycle_artifacts"
            if missing_required
            else "complete_missing_research_evidence"
            if status != "EXECUTABLE_RESEARCH_CYCLE_PROMISING"
            else "manual_owner_research_review"
        ),
        safety_boundary=_safety_boundary(),
        limitations=[
            "Final snapshot is a research-cycle state report, not a trading approval.",
            "No generated metric is fabricated; missing executable artifacts block the snapshot.",
            "Snapshot cannot activate paper-shadow or write official weights.",
        ],
        requested_date_range=summary["requested_date_range"],
    )


def validate_next_research_cycle_payload(
    payload: Mapping[str, Any],
    *,
    expected_report_type: str | None = None,
) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    expected = expected_report_type or report_type
    validation_report_type = f"{expected}{VALIDATION_SUFFIX}"
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        report_type == expected,
        f"report_type must be {expected}.",
        "regenerate_expected_next_research_cycle_artifact",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "production_effect must remain none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_present",
        bool(_mapping(payload.get("reader_brief")).get("key_result")),
        "Reader Brief fields must be present.",
        "repair_reader_brief_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_locked",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Safety boundary must forbid shadow/live/weights/broker/production mutation.",
        "restore_next_research_cycle_safety_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "market_regime_disclosed",
        _text(payload.get("market_regime")) == MARKET_REGIME,
        f"market_regime must be {MARKET_REGIME}.",
        "disclose_ai_after_chatgpt_regime",
    )
    _append_check(
        checks,
        blocking_issues,
        "requested_date_range_disclosed",
        bool(_text(payload.get("requested_date_range"))),
        "requested date range must be disclosed.",
        "restore_requested_date_range",
    )
    if report_type == BACKFILL_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        backfill_status = _text(payload.get("status"))
        metric_rows = [
            row
            for row in _records(payload.get("backfill_windows"))
            if row.get("return_proxy") is not None and row.get("drawdown_proxy") is not None
        ]
        _append_check(
            checks,
            blocking_issues,
            "data_quality_visible",
            bool(_text(summary.get("data_quality_status"))),
            "Backfill must disclose data quality status.",
            "run_validate_data_before_backfill",
        )
        _append_check(
            checks,
            blocking_issues,
            "official_weights_not_generated",
            summary.get("official_target_weights_generated") is False,
            "Backfill must not generate official target weights.",
            "remove_official_weight_output_from_backfill",
        )
        _append_check(
            checks,
            blocking_issues,
            "allowed_backfill_status",
            backfill_status
            in {
                CANDIDATE_BACKFILL_COMPLETE,
                CANDIDATE_BACKFILL_PARTIAL,
                CANDIDATE_BACKFILL_BLOCKED,
            },
            "Backfill status must use TRADING-464 taxonomy.",
            "restore_backfill_status_taxonomy",
        )
        if backfill_status in CANDIDATE_BACKFILL_METRIC_STATUSES:
            _append_check(
                checks,
                blocking_issues,
                "real_metric_rows_present",
                bool(metric_rows),
                "Complete or partial backfill must include computed metric rows.",
                "restore_backfill_metric_rows",
            )
            _append_check(
                checks,
                blocking_issues,
                "safety_audit_visible",
                bool(_text(summary.get("safety_audit_status"))),
                "Backfill with metrics must disclose executable binding safety audit status.",
                "restore_safety_audit_disclosure",
            )
            _append_check(
                checks,
                blocking_issues,
                "no_official_or_broker_side_effects",
                summary.get("broker_order_generated") is False
                and summary.get("owner_decision_appended") is False,
                "Backfill must not generate broker/order or owner-decision side effects.",
                "remove_side_effects_from_backfill",
            )
    if report_type == STRESS_REVIEW_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        stress_status = _text(payload.get("status"))
        scenario_rows = _records(payload.get("scenario_reviews"))
        source_backfill_status = _text(summary.get("source_backfill_status"))
        _append_check(
            checks,
            blocking_issues,
            "allowed_stress_status",
            stress_status in {"WEAK", "MIXED_WITH_WARNINGS", "STRESS_REVIEW_PASS"},
            "Stress review status must use TRADING-465 taxonomy.",
            "restore_stress_review_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "stress_scenarios_present",
            bool(scenario_rows),
            "Stress review must include scenario review rows.",
            "restore_stress_scenario_rows",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_backfill_status_visible",
            bool(source_backfill_status),
            "Stress review must disclose source backfill status.",
            "restore_source_backfill_disclosure",
        )
        if source_backfill_status in CANDIDATE_BACKFILL_METRIC_STATUSES:
            _append_check(
                checks,
                blocking_issues,
                "stress_metric_rows_present",
                all(
                    row.get("return_proxy") is not None
                    and row.get("drawdown_proxy") is not None
                    for row in scenario_rows
                ),
                "Stress review must carry backfill return/drawdown proxy metrics.",
                "restore_stress_review_metric_rows",
            )
    if report_type == COST_BENCHMARK_REVIEW_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        review_status = _text(payload.get("status"))
        cost_rows = _records(payload.get("cost_scenario_reviews"))
        benchmark_rows = _records(payload.get("benchmark_reviews"))
        source_backfill_status = _text(summary.get("source_backfill_status"))
        _append_check(
            checks,
            blocking_issues,
            "allowed_cost_benchmark_status",
            review_status
            in {
                "COST_BENCHMARK_REVIEW_PASS",
                "COST_BENCHMARK_REVIEW_MIXED",
                "COST_BENCHMARK_REVIEW_WEAK",
                "COST_BENCHMARK_NEEDS_EXECUTABLE_BACKFILL",
            },
            "Cost/benchmark review status must use TRADING-465 taxonomy.",
            "restore_cost_benchmark_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "cost_scenarios_present",
            bool(cost_rows),
            "Cost/benchmark review must include cost scenario rows.",
            "restore_cost_scenario_rows",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_backfill_status_visible",
            bool(source_backfill_status),
            "Cost/benchmark review must disclose source backfill status.",
            "restore_source_backfill_disclosure",
        )
        if source_backfill_status in CANDIDATE_BACKFILL_METRIC_STATUSES:
            _append_check(
                checks,
                blocking_issues,
                "cost_metric_rows_present",
                all(row.get("net_proxy_result") is not None for row in cost_rows),
                "Cost review must carry net proxy metrics from backfill inputs.",
                "restore_cost_metric_rows",
            )
            _append_check(
                checks,
                blocking_issues,
                "benchmark_metric_rows_present",
                not benchmark_rows
                or all(
                    row.get("candidate_delta_vs_baseline") is not None
                    for row in benchmark_rows
                ),
                "Benchmark review rows must carry candidate-vs-baseline deltas.",
                "restore_benchmark_metric_rows",
            )
    if report_type == VS_RETURNED_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        comparison_status = _text(payload.get("status"))
        comparison_rows = _records(payload.get("comparison_rows"))
        _append_check(
            checks,
            blocking_issues,
            "allowed_vs_returned_status",
            comparison_status
            in {
                "IMPROVED_OVER_RETURNED_CANDIDATE",
                "MIXED_VS_RETURNED_CANDIDATE",
                "NO_IMPROVEMENT",
                "WORSE_THAN_RETURNED_CANDIDATE",
            },
            "Vs-returned comparison status must use TRADING-466 taxonomy.",
            "restore_vs_returned_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "comparison_rows_present",
            bool(comparison_rows),
            "Vs-returned comparison must include row-level metric comparisons.",
            "restore_vs_returned_comparison_rows",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_metrics_visible",
            bool(_text(summary.get("source_backfill_status")))
            and bool(_text(summary.get("stress_result")))
            and bool(_text(summary.get("benchmark_relative_status"))),
            "Vs-returned comparison must disclose source backfill/stress/benchmark statuses.",
            "restore_vs_returned_source_status_disclosure",
        )
        if _text(summary.get("benchmark_relative_status")) == "BENCHMARK_UNDERPERFORMS":
            _append_check(
                checks,
                blocking_issues,
                "repeated_benchmark_failure_disclosed",
                any(
                    _text(row.get("metric_id")) == "benchmark_relative_behavior"
                    and _text(row.get("comparison_status")) == "REPEATS_FAILURE_MODE"
                    for row in comparison_rows
                ),
                "Benchmark underperformance must be marked as a repeated failure mode.",
                "restore_repeated_benchmark_failure_disclosure",
            )
    if report_type == SIGNAL_ROBUSTNESS_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        robustness_status = _text(payload.get("status"))
        quality_checks = _records(payload.get("signal_quality_checks"))
        checks_by_id = {_text(row.get("check_id")): row for row in quality_checks}
        _append_check(
            checks,
            blocking_issues,
            "allowed_signal_robustness_status",
            robustness_status
            in {
                "SIGNAL_ROBUSTNESS_PASS",
                "SIGNAL_ROBUSTNESS_WARNING",
                "SIGNAL_ROBUSTNESS_BLOCKED",
            },
            "Signal robustness status must use TRADING-467 taxonomy.",
            "restore_signal_robustness_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "signal_quality_checks_present",
            bool(quality_checks),
            "Signal robustness review must include signal quality checks.",
            "restore_signal_quality_checks",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_signal_binding_status_visible",
            bool(_text(summary.get("source_signal_binding_status"))),
            "Signal robustness review must disclose source signal binding status.",
            "restore_signal_binding_status_disclosure",
        )
        if _text(summary.get("backfill_signal_completeness")) != "COMPLETE":
            _append_check(
                checks,
                blocking_issues,
                "partial_signal_series_fail_closed",
                _text(checks_by_id.get("partial_signal_series", {}).get("status"))
                == "BLOCKING",
                "Partial signal series must fail closed.",
                "restore_partial_signal_series_blocker",
            )
    if report_type == WINDOW_SENSITIVITY_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        window_status = _text(payload.get("status"))
        window_splits = _records(payload.get("window_splits"))
        source_backfill_status = _text(summary.get("source_backfill_status"))
        _append_check(
            checks,
            blocking_issues,
            "allowed_window_sensitivity_status",
            window_status in {"WINDOW_STABLE", "WINDOW_MIXED", "WINDOW_FRAGILE"},
            "Window sensitivity status must use TRADING-467 taxonomy.",
            "restore_window_sensitivity_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "window_splits_present",
            bool(window_splits),
            "Window sensitivity review must include window split rows.",
            "restore_window_splits",
        )
        if source_backfill_status in CANDIDATE_BACKFILL_METRIC_STATUSES:
            _append_check(
                checks,
                blocking_issues,
                "window_metrics_present",
                all(
                    row.get("average_return_proxy") is not None
                    and row.get("worst_drawdown_proxy") is not None
                    for row in window_splits
                ),
                "Backfill-backed window sensitivity must include real split metrics.",
                "restore_window_split_metrics",
            )
        if window_status == "WINDOW_FRAGILE":
            _append_check(
                checks,
                blocking_issues,
                "fragility_conditions_visible",
                _text(summary.get("overfit_risk")) == "HIGH"
                and _text(summary.get("blocking_conditions"), "none") != "none",
                "Fragile window sensitivity must disclose overfit risk and blockers.",
                "restore_window_fragility_disclosure",
            )
    if report_type == RESEARCH_GATE_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        gate_status = _text(payload.get("status"))
        blockers = _records(payload.get("blocker_list"))
        source_statuses = _mapping(payload.get("source_statuses"))
        positive_evidence = _records(payload.get("strongest_positive_evidence"))
        negative_evidence = _records(payload.get("strongest_negative_evidence"))
        blocker_ids = {_text(row.get("issue_id")) for row in blockers}
        _append_check(
            checks,
            blocking_issues,
            "allowed_research_gate_decision",
            gate_status in RESEARCH_GATE_DECISIONS,
            "Research gate status must use TRADING-468 taxonomy.",
            "restore_research_gate_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_statuses_present",
            all(
                _text(source_statuses.get(report_id))
                for report_id in (
                    "executable_binding_safety_audit",
                    "next_candidate_backfill",
                    "next_candidate_stress_review",
                    "next_candidate_cost_benchmark_review",
                    "next_candidate_vs_returned_candidate_comparison",
                    "next_candidate_signal_robustness_review",
                    "next_candidate_overfit_window_sensitivity",
                )
            ),
            "Research gate must disclose all source statuses.",
            "restore_research_gate_source_statuses",
        )
        _append_check(
            checks,
            blocking_issues,
            "safety_audit_status_visible",
            bool(_text(summary.get("safety_audit_status"))),
            "Research gate must disclose executable binding safety audit status.",
            "restore_gate_safety_audit_disclosure",
        )
        _append_check(
            checks,
            blocking_issues,
            "positive_evidence_present",
            bool(positive_evidence),
            "Research gate must include strongest positive evidence.",
            "restore_gate_positive_evidence",
        )
        if gate_status != "RESEARCH_PROMISING":
            _append_check(
                checks,
                blocking_issues,
                "non_promising_blockers_present",
                bool(blockers) and bool(negative_evidence),
                "Non-promising research gate must include blockers and negative evidence.",
                "restore_gate_blocker_evidence",
            )
        _append_check(
            checks,
            blocking_issues,
            "paper_shadow_not_allowed",
            summary.get("paper_shadow_activation_allowed") is False
            and summary.get("official_target_weights_generated") is False
            and summary.get("broker_order_allowed") is False,
            "Research gate must not allow paper-shadow, official weights, or broker/order.",
            "restore_research_gate_safety_boundary",
        )
        if _text(summary.get("signal_robustness_status")) == "SIGNAL_ROBUSTNESS_BLOCKED":
            _append_check(
                checks,
                blocking_issues,
                "signal_blocker_visible",
                "signal_robustness_blocked" in blocker_ids,
                "Blocked signal robustness must be visible in research gate blockers.",
                "restore_signal_gate_blocker",
            )
        if _text(summary.get("window_sensitivity_status")) == "WINDOW_FRAGILE":
            _append_check(
                checks,
                blocking_issues,
                "window_fragility_blocker_visible",
                "window_sensitivity_fragile" in blocker_ids,
                "Fragile window sensitivity must be visible in research gate blockers.",
                "restore_window_gate_blocker",
            )
    if report_type == OWNER_REVIEW_PACKET_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        packet_status = _text(payload.get("status"))
        owner_options = _records(payload.get("owner_options"))
        option_ids = {_text(row.get("option_id")) for row in owner_options}
        required_option_ids = {
            "continue_research_validation",
            "revise_hypothesis",
            "return_to_hypothesis_backlog",
            "reject_research_candidate",
            "hold_for_more_data",
        }
        _append_check(
            checks,
            blocking_issues,
            "allowed_owner_packet_status",
            packet_status == "OWNER_RESEARCH_REVIEW_PACKET_READY",
            "Owner research review packet must use TRADING-469 status.",
            "restore_owner_packet_status",
        )
        _append_check(
            checks,
            blocking_issues,
            "source_research_gate_visible",
            bool(_text(summary.get("source_research_gate_decision"))),
            "Owner packet must disclose source research gate decision.",
            "restore_owner_packet_gate_disclosure",
        )
        _append_check(
            checks,
            blocking_issues,
            "required_owner_options_present",
            required_option_ids <= option_ids,
            "Owner packet must include continue/revise/return/reject/hold options.",
            "restore_owner_option_set",
        )
        _append_check(
            checks,
            blocking_issues,
            "owner_options_complete",
            all(
                _list_values(row.get("evidence_required"))
                and _list_values(row.get("risks"))
                and bool(_text(row.get("next_action")))
                for row in owner_options
            ),
            "Every owner option must include evidence, risks, and next action.",
            "restore_owner_option_details",
        )
        _append_check(
            checks,
            blocking_issues,
            "owner_decision_not_appended",
            summary.get("owner_decision_appended") is False,
            "Owner packet must not append owner decision automatically.",
            "restore_owner_packet_no_append_boundary",
        )
        _append_check(
            checks,
            blocking_issues,
            "no_shadow_live_weights_broker",
            summary.get("paper_shadow_activation_allowed") is False
            and summary.get("extended_shadow_allowed") is False
            and summary.get("live_trading_allowed") is False
            and summary.get("official_target_weights_generated") is False
            and summary.get("broker_order_allowed") is False
            and summary.get("order_ticket_generated") is False,
            "Owner packet must forbid shadow/live/official weights/broker/order.",
            "restore_owner_packet_safety_boundary",
        )
    if report_type == CYCLE_SNAPSHOT_REPORT_TYPE:
        summary = _mapping(payload.get("summary"))
        snapshot_status = _text(payload.get("status"))
        source_statuses = _records(payload.get("source_statuses"))
        source_types = {_text(row.get("report_type")) for row in source_statuses}
        required_source_types = {
            binding_reports.CONTRACT_REPORT_TYPE,
            binding_reports.SIGNAL_BINDING_REPORT_TYPE,
            binding_reports.WEIGHT_BINDING_REPORT_TYPE,
            binding_reports.SAFETY_AUDIT_REPORT_TYPE,
            BACKFILL_REPORT_TYPE,
            STRESS_REVIEW_REPORT_TYPE,
            COST_BENCHMARK_REVIEW_REPORT_TYPE,
            VS_RETURNED_REPORT_TYPE,
            SIGNAL_ROBUSTNESS_REPORT_TYPE,
            WINDOW_SENSITIVITY_REPORT_TYPE,
            RESEARCH_GATE_REPORT_TYPE,
            OWNER_REVIEW_PACKET_REPORT_TYPE,
        }
        _append_check(
            checks,
            blocking_issues,
            "allowed_executable_cycle_status",
            snapshot_status in EXECUTABLE_RESEARCH_CYCLE_STATUSES,
            "Executable research-cycle snapshot must use TRADING-470 taxonomy.",
            "restore_executable_cycle_status_taxonomy",
        )
        _append_check(
            checks,
            blocking_issues,
            "required_sources_present",
            required_source_types <= source_types,
            "Executable research-cycle snapshot must include all required sources.",
            "restore_executable_cycle_source_statuses",
        )
        _append_check(
            checks,
            blocking_issues,
            "owner_packet_ready_visible",
            bool(summary.get("owner_packet_ready")),
            "Executable research-cycle snapshot must disclose owner packet readiness.",
            "restore_cycle_owner_packet_disclosure",
        )
        _append_check(
            checks,
            blocking_issues,
            "no_fabricated_missing_artifacts",
            _int(summary.get("missing_required_artifact_count")) == len(
                _list_values(summary.get("missing_required_artifacts"))
            ),
            "Missing required artifact count must match listed missing artifacts.",
            "restore_cycle_missing_artifact_accounting",
        )
        _append_check(
            checks,
            blocking_issues,
            "no_shadow_live_weights_broker_owner_append",
            summary.get("paper_shadow_activation_allowed") is False
            and summary.get("extended_shadow_allowed") is False
            and summary.get("live_trading_allowed") is False
            and summary.get("official_target_weights_generated") is False
            and summary.get("broker_order_allowed") is False
            and summary.get("owner_decision_appended") is False,
            (
                "Executable research-cycle snapshot must forbid shadow/live/"
                "official weights/broker/order/owner append."
            ),
            "restore_executable_cycle_safety_boundary",
        )
    status = FAIL_STATUS if blocking_issues else PASS_STATUS
    return _payload(
        report_type=validation_report_type,
        as_of=_date_from_payload(payload),
        status=status,
        purpose=f"Validate {expected} schema, disclosure, and safety boundary.",
        input_artifacts={expected: _artifact_id(payload)},
        output_decision=status,
        summary={
            "validation_status": status,
            "source_report_type": report_type,
            "check_count": len(checks),
            "failed_check_count": len(blocking_issues),
            "production_effect": PRODUCTION_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking_issues,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{expected} validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking_issues, "issue_id"),
            warnings="none",
            next_action=(
                "repair_next_research_cycle_artifact"
                if status == FAIL_STATUS
                else "use_validated_next_research_cycle_artifact"
            ),
        ),
        next_action=(
            "repair_next_research_cycle_artifact"
            if status == FAIL_STATUS
            else "use_validated_next_research_cycle_artifact"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def write_next_research_cycle_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_next_research_cycle_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_next_research_cycle_markdown(payload), encoding="utf-8")
    return output_path


def render_next_research_cycle_markdown(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_title(report_type)} {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- status: {_md_cell(payload.get('status'))}",
        f"- output_decision: {_md_cell(payload.get('output_decision'))}",
        f"- market_regime: {_md_cell(payload.get('market_regime'))}",
        f"- requested_date_range: {_md_cell(payload.get('requested_date_range'))}",
        f"- production_effect: {_md_cell(payload.get('production_effect'))}",
        f"- next_action: {_md_cell(payload.get('next_action'))}",
    ]
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            lines.append(f"- {key}: {_md_cell(value)}")
    lines.extend(["", "## Reader Brief", ""])
    for key, value in _mapping(payload.get("reader_brief")).items():
        lines.append(f"- {key}: {_md_cell(value)}")
    for title, key in _markdown_table_keys(report_type):
        lines.extend(_table_records(title, payload.get(key)))
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def _payload(
    *,
    report_type: str,
    as_of: date,
    status: str,
    purpose: str,
    input_artifacts: Mapping[str, Any],
    output_decision: str,
    summary: Mapping[str, Any],
    body: Mapping[str, Any],
    reader_brief: Mapping[str, Any],
    next_action: str,
    safety_boundary: Mapping[str, Any],
    limitations: Sequence[str],
    requested_date_range: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "market_regime": MARKET_REGIME,
        "ai_regime_start": AI_REGIME_START,
        "requested_date_range": requested_date_range,
        "purpose": purpose,
        "input_artifacts": dict(input_artifacts),
        "output_decision": output_decision,
        "summary": dict(summary),
        **dict(body),
        "reader_brief": dict(reader_brief),
        "safety_boundary": dict(safety_boundary),
        "limitations": list(limitations),
        "next_action": next_action,
        "methodology": {
            "collector_mode": "read_existing_return_to_research_and_research_artifacts",
            "does_not_refresh_data": True,
            "does_not_fabricate_data": True,
            "does_not_create_paper_shadow_candidate": True,
            "does_not_resume_normal_paper_shadow": True,
            "does_not_approve_extended_shadow": True,
            "does_not_approve_live_trading": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_mutate_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def _reader_brief(
    *,
    summary: str,
    key_result: str,
    blocking_issues: str,
    warnings: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "summary": summary,
        "key_result": key_result,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "safety_boundary": (
            "research-only next cycle; no paper-shadow activation, no extended shadow, "
            "no live trading, no official target weights, no broker/order, "
            "production_effect=none."
        ),
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "next_research_cycle_reports_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "paper_shadow_candidate_created": False,
        "normal_paper_shadow_resumed": False,
        "normal_shadow_signoff_packet_generated": False,
        "observation_clock_started": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_decision_appended": False,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
    }


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("paper_shadow_candidate_created") is False
        and safety.get("normal_paper_shadow_resumed") is False
        and safety.get("normal_shadow_signoff_packet_generated") is False
        and safety.get("observation_clock_started") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("owner_decision_appended") is False
        and safety.get("strategy_outputs_mutated") is False
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
    )


def _load_reset_intake_inputs(reports_dir: Path, as_of: date) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for report_type in RESET_INTAKE_INPUT_TYPES:
        path = reset_reports.default_return_to_research_json_path(report_type, reports_dir, as_of)
        result[report_type] = {"path": _display_path(path), "payload": _read_json_mapping(path)}
    return result


def _load_project_sources(project_root: Path) -> dict[str, dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    for source_id, patterns in LATEST_PROJECT_ARTIFACT_GLOBS.items():
        path = _latest_matching_path(project_root, patterns)
        payload = _read_json_mapping(path) if path is not None else {}
        sources[source_id] = {
            "path": "" if path is None else _display_path(path),
            "payload": payload,
            "available": path is not None,
        }
    return sources


def _payload_from_inputs(
    inputs: Mapping[str, Mapping[str, Any]],
    report_type: str,
) -> dict[str, Any]:
    return _mapping(_mapping(inputs.get(report_type)).get("payload"))


def _input_paths(inputs: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {report_type: _text(item.get("path")) for report_type, item in inputs.items()}


def _source_paths(sources: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    return {source_id: _text(source.get("path")) for source_id, source in sources.items()}


def _invalidated_or_weak_evidence(reusable_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    result = list(_records(reusable_payload.get("invalidated_evidence")))
    result.extend(
        row
        for row in _records(reusable_payload.get("evidence_classification"))
        if _text(row.get("classification")) == "weak but informative"
    )
    return result


def _frozen_candidate_id(source_candidate_id: str) -> str:
    if source_candidate_id.endswith("_draft"):
        return source_candidate_id[: -len("_draft")]
    if source_candidate_id:
        return f"{source_candidate_id}_frozen_research"
    return "regime_mismatch_filter_v2_research"


def _candidate_id_from_frozen(payload: Mapping[str, Any]) -> str:
    return _text(_mapping(payload.get("frozen_candidate_spec")).get("candidate_id"))


def _normalize_data_quality_gate(value: Mapping[str, Any] | None) -> dict[str, Any]:
    data = _mapping(value)
    status = _text(data.get("status"), _text(data.get("data_quality_status"), "MISSING"))
    passed = data.get("passed")
    if passed is None:
        passed = status in {PASS_STATUS, PASS_WITH_WARNINGS_STATUS}
    return {
        "status": status,
        "passed": bool(passed),
        "error_count": _int(data.get("error_count")),
        "warning_count": _int(data.get("warning_count")),
        "report_path": _text(data.get("report_path")),
    }


def _blocked_backfill_window_result(
    window: Mapping[str, Any],
    *,
    data_quality: Mapping[str, Any],
    missing_inputs: Sequence[str],
) -> dict[str, Any]:
    missing = list(missing_inputs)
    if not data_quality.get("passed"):
        missing.append("validated_data_quality_gate")
    return {
        "window_id": _text(window.get("window_id")),
        "start": _text(window.get("start")),
        "end": _text(window.get("end")),
        "market_regime": _text(window.get("market_regime"), MARKET_REGIME),
        "backfill_window_status": "BLOCKED",
        "return_proxy": None,
        "drawdown_proxy": None,
        "turnover": None,
        "rotation_count": None,
        "false_risk_off_count": None,
        "constraint_hit_count": None,
        "signal_completeness": "MISSING",
        "signal_completeness_ratio": 0.0,
        "missing_data_list": missing,
        "cost_proxy_inputs": _cost_proxy_inputs(False),
        "production_effect": PRODUCTION_EFFECT,
    }


def _executable_backfill_window_result(
    window: Mapping[str, Any],
    *,
    price_history: Mapping[str, Mapping[date, float]],
    weight_map: Mapping[str, float],
    previous_weight_map: Mapping[str, float],
    signal_rows: Sequence[Mapping[str, Any]],
    weight_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    start = _parse_date_value(window.get("start"))
    end = _parse_date_value(window.get("end"))
    window_id = _text(window.get("window_id"))
    non_cash_symbols = [
        symbol for symbol, weight in weight_map.items() if symbol != "CASH" and weight
    ]
    common_dates = _common_price_dates(
        price_history=price_history,
        symbols=non_cash_symbols,
        start=start,
        end=end,
    )
    missing = []
    if len(common_dates) < 2:
        missing.append(f"insufficient_price_history:{window_id}")
    signal_dates = {
        parsed
        for parsed in (_parse_date_value(row.get("signal_date")) for row in signal_rows)
        if parsed is not None and start is not None and end is not None and start <= parsed <= end
    }
    if not signal_dates:
        missing.append(f"historical_signal_series:{window_id}")
    if missing and len(common_dates) < 2:
        return {
            "window_id": window_id,
            "start": _text(window.get("start")),
            "end": _text(window.get("end")),
            "market_regime": _text(window.get("market_regime"), MARKET_REGIME),
            "backfill_window_status": "BLOCKED",
            "return_proxy": None,
            "drawdown_proxy": None,
            "turnover": None,
            "rotation_count": None,
            "false_risk_off_count": None,
            "constraint_hit_count": None,
            "signal_completeness": "MISSING",
            "signal_completeness_ratio": 0.0,
            "missing_data_list": missing,
            "cost_proxy_inputs": _cost_proxy_inputs(False),
            "production_effect": PRODUCTION_EFFECT,
        }
    daily_returns = _weighted_daily_returns(
        price_history=price_history,
        symbols=non_cash_symbols,
        dates=common_dates,
        weights=weight_map,
    )
    return_proxy, drawdown_proxy = _return_and_drawdown(daily_returns)
    turnover = _weight_turnover(weight_map, previous_weight_map)
    signal_completeness_ratio = (
        min(1.0, len(signal_dates) / len(common_dates)) if common_dates else 0.0
    )
    false_risk_off_count = len(
        [
            row
            for row in signal_rows
            if _signal_row_inside_window(row, start, end)
            and _text(row.get("risk_state")) in {"risk_off", "blocked"}
        ]
    )
    constraint_hit_count = sum(len(_list_values(row.get("constraint_hit"))) for row in weight_rows)
    return {
        "window_id": window_id,
        "start": _text(window.get("start")),
        "end": _text(window.get("end")),
        "market_regime": _text(window.get("market_regime"), MARKET_REGIME),
        "backfill_window_status": "PARTIAL" if missing else "READY",
        "return_proxy": _round_float(return_proxy),
        "drawdown_proxy": _round_float(drawdown_proxy),
        "turnover": _round_float(turnover),
        "rotation_count": 1 if turnover > 0 else 0,
        "false_risk_off_count": false_risk_off_count,
        "constraint_hit_count": constraint_hit_count,
        "signal_completeness": "PARTIAL_STATIC_BINDING" if missing else "AVAILABLE",
        "signal_completeness_ratio": _round_float(signal_completeness_ratio),
        "price_observation_count": len(common_dates),
        "return_observation_count": len(daily_returns),
        "missing_data_list": missing,
        "cost_proxy_inputs": _cost_proxy_inputs(True),
        "production_effect": PRODUCTION_EFFECT,
    }


def _cost_proxy_inputs(metrics_available: bool) -> dict[str, Any]:
    return {
        "turnover_available": metrics_available,
        "gross_return_proxy_available": metrics_available,
        "cost_scenario_inputs_available": metrics_available,
        "missing_reason": "" if metrics_available else "executable_candidate_backfill_missing",
        "production_effect": PRODUCTION_EFFECT,
    }


def _backfill_blocker_action(issue_id: str) -> str:
    if issue_id == "data_quality_gate_not_passed":
        return "run_aits_validate_data_and_stop_until_passed"
    if issue_id == "executable_candidate_signal_binding_missing":
        return "define_reviewed_research_signal_and_weight_binding_before_backfill"
    if issue_id == "historical_dynamic_binding_unavailable":
        return "extend_binding_to_historical_signal_and_weight_series"
    if issue_id == "executable_binding_safety_audit_not_passed":
        return "repair_safety_audit_before_backfill"
    if issue_id.startswith("missing_") or issue_id.startswith("insufficient_"):
        return "repair_required_backfill_input_data"
    return "repair_backfill_input"


def _load_executable_backfill_inputs(
    *,
    reports_dir: Path,
    as_of: date,
) -> dict[str, Any]:
    paths = {
        "next_candidate_signal_binding": binding_reports.default_executable_binding_json_path(
            binding_reports.SIGNAL_BINDING_REPORT_TYPE,
            reports_dir,
            as_of,
        ),
        "next_candidate_signal_binding_validation": (
            binding_reports.default_executable_binding_json_path(
                binding_reports.SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        "next_candidate_research_weight_binding": (
            binding_reports.default_executable_binding_json_path(
                binding_reports.WEIGHT_BINDING_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        "next_candidate_research_weight_binding_validation": (
            binding_reports.default_executable_binding_json_path(
                binding_reports.WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        "executable_binding_safety_audit": (
            binding_reports.default_executable_binding_json_path(
                binding_reports.SAFETY_AUDIT_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
        "executable_binding_safety_audit_validation": (
            binding_reports.default_executable_binding_json_path(
                binding_reports.SAFETY_AUDIT_VALIDATION_REPORT_TYPE,
                reports_dir,
                as_of,
            )
        ),
    }
    missing = [f"missing_{name}" for name, path in paths.items() if not path.exists()]
    return {
        "paths": paths,
        "missing_inputs": missing,
        "signal_payload": _read_json_if_exists(paths["next_candidate_signal_binding"]),
        "signal_validation": _read_json_if_exists(
            paths["next_candidate_signal_binding_validation"]
        ),
        "weight_payload": _read_json_if_exists(
            paths["next_candidate_research_weight_binding"]
        ),
        "weight_validation": _read_json_if_exists(
            paths["next_candidate_research_weight_binding_validation"]
        ),
        "safety_payload": _read_json_if_exists(paths["executable_binding_safety_audit"]),
        "safety_validation": _read_json_if_exists(
            paths["executable_binding_safety_audit_validation"]
        ),
    }


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _read_json_mapping(path)


def _binding_input_artifact_paths(binding_inputs: Mapping[str, Any]) -> dict[str, str]:
    paths = _mapping(binding_inputs.get("paths"))
    return {key: str(path) for key, path in paths.items() if isinstance(path, Path)}


def _binding_backfill_blockers(binding_inputs: Mapping[str, Any]) -> list[str]:
    blockers = list(_list_values(binding_inputs.get("missing_inputs")))
    signal_payload = _mapping(binding_inputs.get("signal_payload"))
    signal_validation = _mapping(binding_inputs.get("signal_validation"))
    weight_payload = _mapping(binding_inputs.get("weight_payload"))
    weight_validation = _mapping(binding_inputs.get("weight_validation"))
    safety_payload = _mapping(binding_inputs.get("safety_payload"))
    safety_validation = _mapping(binding_inputs.get("safety_validation"))
    safety_status = _text(safety_payload.get("status"), "MISSING")
    safety_summary = _mapping(safety_payload.get("summary"))
    safety_acceptable = safety_status == binding_reports.SAFETY_PASS or (
        safety_status == binding_reports.SAFETY_WARNING
        and safety_summary.get("acceptable_warning") is True
    )
    if _text(safety_validation.get("status")) != PASS_STATUS or not safety_acceptable:
        blockers.append("executable_binding_safety_audit_not_passed")
    if _text(signal_validation.get("status")) != PASS_STATUS:
        blockers.append("signal_binding_validation_not_passed")
    if _text(weight_validation.get("status")) != PASS_STATUS:
        blockers.append("research_weight_binding_validation_not_passed")
    if not _records(signal_payload.get("candidate_signal_series")):
        blockers.append("missing_candidate_signal_series")
    if not _records(weight_payload.get("hypothetical_research_weight_series")):
        blockers.append("missing_hypothetical_research_weight_series")
    current_weight = _mapping(weight_payload.get("hypothetical_research_weight"))
    weight_map = _weight_values(current_weight)
    if not weight_map:
        blockers.append("missing_hypothetical_research_weight")
    if abs(sum(weight_map.values()) - 1.0) > 0.000001:
        blockers.append("hypothetical_research_weight_sum_invalid")
    if not _binding_payload_research_only(signal_payload):
        blockers.append("signal_binding_not_research_only")
    if not _binding_payload_research_only(weight_payload):
        blockers.append("research_weight_binding_not_research_only")
    return sorted(set(blockers))


def _binding_payload_research_only(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect"), "none") == "none"
        and _text(payload.get("order_effect"), "none") == "none"
    )


def _weight_values(weight_object: Mapping[str, Any]) -> dict[str, float]:
    weights = _mapping(weight_object.get("weights"))
    return {
        _text(symbol).upper(): _float(weight)
        for symbol, weight in weights.items()
        if _text(symbol) and _float(weight) >= 0
    }


def _read_price_history(
    *,
    prices_path: Path,
    symbols: Sequence[str],
) -> tuple[dict[str, dict[date, float]], list[str]]:
    if not prices_path.exists():
        return {}, ["missing_price_file"]
    wanted = {symbol.upper() for symbol in symbols}
    history: dict[str, dict[date, float]] = {symbol: {} for symbol in wanted}
    with prices_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = _text(
                row.get("symbol"),
                _text(row.get("ticker"), _text(row.get("canonical_symbol"))),
            ).upper()
            if symbol not in wanted:
                continue
            parsed_date = _parse_date_value(row.get("date"))
            price = _float(row.get("adj_close"), _float(row.get("close")))
            if parsed_date is None or price <= 0:
                continue
            history.setdefault(symbol, {})[parsed_date] = price
    missing = [
        f"missing_price_history:{symbol}"
        for symbol in sorted(wanted)
        if not history.get(symbol)
    ]
    return history, missing


def _common_price_dates(
    *,
    price_history: Mapping[str, Mapping[date, float]],
    symbols: Sequence[str],
    start: date | None,
    end: date | None,
) -> list[date]:
    if start is None or end is None or not symbols:
        return []
    date_sets = []
    for symbol in symbols:
        date_sets.append(
            {
                price_date
                for price_date in price_history.get(symbol, {})
                if start <= price_date <= end
            }
        )
    if not date_sets:
        return []
    return sorted(set.intersection(*date_sets))


def _weighted_daily_returns(
    *,
    price_history: Mapping[str, Mapping[date, float]],
    symbols: Sequence[str],
    dates: Sequence[date],
    weights: Mapping[str, float],
) -> list[float]:
    returns: list[float] = []
    for previous_date, current_date in zip(dates, dates[1:], strict=False):
        daily_return = 0.0
        for symbol in symbols:
            previous_price = price_history[symbol][previous_date]
            current_price = price_history[symbol][current_date]
            daily_return += _float(weights.get(symbol)) * (
                current_price / previous_price - 1.0
            )
        returns.append(daily_return)
    return returns


def _return_and_drawdown(daily_returns: Sequence[float]) -> tuple[float, float]:
    equity = 1.0
    peak = 1.0
    max_drawdown = 0.0
    for daily_return in daily_returns:
        equity *= 1.0 + daily_return
        peak = max(peak, equity)
        drawdown = equity / peak - 1.0
        max_drawdown = min(max_drawdown, drawdown)
    return equity - 1.0, max_drawdown


def _backfill_partial_reasons(
    *,
    window_results: Sequence[Mapping[str, Any]],
    signal_payload: Mapping[str, Any],
    weight_payload: Mapping[str, Any],
) -> list[str]:
    reasons = []
    if any(_text(row.get("backfill_window_status")) == "PARTIAL" for row in window_results):
        reasons.append("historical_dynamic_binding_unavailable")
    if len(_records(signal_payload.get("candidate_signal_series"))) <= 1:
        reasons.append("single_point_signal_binding_used_as_static_proxy")
    if len(_records(weight_payload.get("hypothetical_research_weight_series"))) <= 1:
        reasons.append("single_point_weight_binding_used_as_static_proxy")
    return sorted(set(reasons))


def _aggregate_backfill_metrics(
    window_results: Sequence[Mapping[str, Any]],
    *,
    turnover_proxy: float,
    signal_payload: Mapping[str, Any],
    weight_payload: Mapping[str, Any],
) -> dict[str, Any]:
    metric_rows = [
        row
        for row in window_results
        if row.get("return_proxy") is not None and row.get("drawdown_proxy") is not None
    ]
    returns = [_float(row.get("return_proxy")) for row in metric_rows]
    drawdowns = [_float(row.get("drawdown_proxy")) for row in metric_rows]
    signal_ratios = [_float(row.get("signal_completeness_ratio")) for row in metric_rows]
    weight_rows = _records(weight_payload.get("hypothetical_research_weight_series"))
    return {
        "aggregation_method": "mean_of_required_window_proxies",
        "metric_window_count": len(metric_rows),
        "aggregate_return_proxy": _round_float(sum(returns) / len(returns))
        if returns
        else None,
        "aggregate_drawdown_proxy": _round_float(min(drawdowns)) if drawdowns else None,
        "turnover_proxy": _round_float(turnover_proxy) if metric_rows else None,
        "rotation_count": 1 if metric_rows and turnover_proxy > 0 else 0,
        "false_risk_off_count": sum(_int(row.get("false_risk_off_count")) for row in metric_rows),
        "constraint_hit_count": sum(
            len(_list_values(row.get("constraint_hit"))) for row in weight_rows
        ),
        "signal_completeness": (
            "AVAILABLE"
            if signal_ratios and min(signal_ratios) >= 1.0
            else "PARTIAL_STATIC_BINDING"
            if signal_ratios
            else "MISSING"
        ),
        "signal_completeness_ratio": _round_float(
            sum(signal_ratios) / len(signal_ratios)
        )
        if signal_ratios
        else 0.0,
        "source_signal_row_count": len(_records(signal_payload.get("candidate_signal_series"))),
        "source_weight_row_count": len(weight_rows),
        "production_effect": PRODUCTION_EFFECT,
    }


def _weight_turnover(
    current_weight: Mapping[str, float],
    previous_weight: Mapping[str, float],
) -> float:
    symbols = set(current_weight) | set(previous_weight)
    return (
        sum(
            abs(_float(current_weight.get(symbol)) - _float(previous_weight.get(symbol)))
            for symbol in symbols
        )
        / 2.0
    )


def _parse_date_value(value: Any) -> date | None:
    try:
        return date.fromisoformat(_text(value))
    except ValueError:
        return None


def _signal_row_inside_window(
    row: Mapping[str, Any],
    start: date | None,
    end: date | None,
) -> bool:
    signal_date = _parse_date_value(row.get("signal_date"))
    return bool(signal_date and start and end and start <= signal_date <= end)


def _backfill_metrics_available(backfill_status: str) -> bool:
    return backfill_status in CANDIDATE_BACKFILL_METRIC_STATUSES


def _stress_scenario_review(
    window_id: str,
    *,
    executable_backfill: bool,
    window_metrics: Mapping[str, Any],
    backfill_status: str,
) -> dict[str, Any]:
    if not executable_backfill or not window_metrics:
        return {
            "scenario_id": window_id,
            "scenario_status": "BLOCKING",
            "evaluation": "Executable backfill metrics are unavailable for this scenario.",
            "return_proxy": None,
            "drawdown_proxy": None,
            "turnover_proxy": None,
            "rotation_count": None,
            "false_risk_off_count": None,
            "rapid_drawdown_behavior": None,
            "slow_drawdown_behavior": None,
            "v_shaped_recovery_behavior": None,
            "high_volatility_sideways_behavior": None,
            "false_risk_off_cluster_behavior": None,
            "ai_semiconductor_correction_behavior": None,
            "recommended_action": "complete_executable_backfill_before_stress_review",
            "production_effect": PRODUCTION_EFFECT,
        }
    return_proxy = _float(window_metrics.get("return_proxy"))
    drawdown_proxy = _float(window_metrics.get("drawdown_proxy"))
    partial = _text(window_metrics.get("backfill_window_status")) == "PARTIAL"
    if drawdown_proxy <= STRESS_BLOCKING_DRAWDOWN_PROXY:
        scenario_status = "FAIL"
        evaluation = "Drawdown proxy breaches conservative stress blocker."
    elif drawdown_proxy <= STRESS_WARNING_DRAWDOWN_PROXY or return_proxy < 0:
        scenario_status = "WARNING"
        evaluation = "Return/drawdown proxy remains weak in this stress window."
    elif partial or backfill_status == CANDIDATE_BACKFILL_PARTIAL:
        scenario_status = "WARNING"
        evaluation = "Metric is real but partial because binding lacks historical signals."
    else:
        scenario_status = "PASS"
        evaluation = "Backfill proxy is non-negative and drawdown is within diagnostic band."
    return {
        "scenario_id": window_id,
        "scenario_status": scenario_status,
        "evaluation": evaluation,
        "return_proxy": window_metrics.get("return_proxy"),
        "drawdown_proxy": window_metrics.get("drawdown_proxy"),
        "turnover_proxy": window_metrics.get("turnover"),
        "rotation_count": window_metrics.get("rotation_count"),
        "false_risk_off_count": window_metrics.get("false_risk_off_count"),
        "rapid_drawdown_behavior": _scenario_metric_label(
            window_id,
            "rapid_drawdown",
            drawdown_proxy,
        ),
        "slow_drawdown_behavior": _scenario_metric_label(
            window_id,
            "slow_drawdown",
            drawdown_proxy,
        ),
        "v_shaped_recovery_behavior": None,
        "high_volatility_sideways_behavior": _scenario_metric_label(
            window_id,
            "high_volatility_sideways_market",
            drawdown_proxy,
        ),
        "false_risk_off_cluster_behavior": window_metrics.get("false_risk_off_count"),
        "ai_semiconductor_correction_behavior": _scenario_metric_label(
            window_id,
            "ai_semiconductor_correction",
            drawdown_proxy,
        ),
        "recommended_action": (
            "review_stress_metrics"
            if scenario_status == "PASS"
            else "complete_executable_backfill_before_stress_review"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reusable_positive_stress_evidence(
    sources: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows = []
    for source_id in (
        "stress_scenario_library",
        "drawdown_event_casebook",
        "flip_rotation_event_casebook",
    ):
        source = _mapping(sources.get(source_id))
        rows.append(
            {
                "source_id": source_id,
                "available": bool(source.get("available")),
                "artifact_path": _text(source.get("path")),
                "use": "diagnostic_context_only",
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return rows


def _cost_scenario_reviews(
    cost_payload: Mapping[str, Any],
    *,
    backfill_ready: bool,
    backfill_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = _records(_mapping(cost_payload.get("policy")).get("scenarios"))
    if not rows:
        rows = _records(cost_payload.get("scenario_results"))
    if not rows:
        rows = [dict(row) for row in DEFAULT_COST_SCENARIOS]
    backfill_summary = _mapping(backfill_payload.get("summary"))
    gross_proxy = _float(backfill_summary.get("aggregate_return_proxy"))
    turnover_proxy = _float(backfill_summary.get("turnover_proxy"))
    threshold = _float(
        cost_payload.get("meaningful_improvement_threshold"),
        DEFAULT_COST_MEANINGFUL_THRESHOLD,
    )
    result = []
    for row in rows:
        scenario_id = _text(row.get("scenario_id"), _text(row.get("label")))
        total_cost_bps = _float(row.get("total_cost_bps"))
        cost_drag = turnover_proxy * total_cost_bps / 10000.0
        net_proxy = gross_proxy - cost_drag
        if not backfill_ready:
            survival_status = "UNTESTED"
        elif net_proxy < 0:
            survival_status = "COST_SURVIVAL_FAIL"
        elif net_proxy < threshold:
            survival_status = "COST_SURVIVAL_WARNING"
        else:
            survival_status = "COST_SURVIVAL_PASS"
        result.append(
            {
                "scenario_id": scenario_id,
                "label": _text(row.get("label"), scenario_id),
                "total_cost_bps": total_cost_bps,
                "turnover_proxy": turnover_proxy if backfill_ready else None,
                "gross_return_proxy": gross_proxy if backfill_ready else None,
                "cost_drag": _round_float(cost_drag) if backfill_ready else None,
                "net_proxy_result": _round_float(net_proxy) if backfill_ready else None,
                "meaningful_threshold": threshold,
                "cost_survival_status": survival_status,
                "source_backfill_status": _text(backfill_summary.get("candidate_backfill_status")),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return result


def _scenario_metric_label(
    window_id: str,
    target_window_id: str,
    drawdown_proxy: float,
) -> str | None:
    if window_id != target_window_id:
        return None
    if drawdown_proxy <= STRESS_BLOCKING_DRAWDOWN_PROXY:
        return "WEAK"
    if drawdown_proxy <= STRESS_WARNING_DRAWDOWN_PROXY:
        return "MIXED"
    return "OK"


def _cost_turnover_penalty(rows: Sequence[Mapping[str, Any]]) -> float | None:
    penalties = [
        _float(row.get("cost_drag"))
        for row in rows
        if row.get("cost_drag") is not None
    ]
    if not penalties:
        return None
    return _round_float(max(penalties))


def _review_issue_names(rows: Sequence[Mapping[str, Any]]) -> str:
    names = [
        _text(row.get("scenario_id"), _text(row.get("baseline_id")))
        for row in rows
    ]
    return "; ".join(name for name in names if name) or "none"


def _benchmark_reviews(
    benchmark_payload: Mapping[str, Any],
    *,
    backfill_ready: bool,
    backfill_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    baselines = _records(benchmark_payload.get("baselines"))
    backfill_summary = _mapping(backfill_payload.get("summary"))
    candidate_proxy = _float(backfill_summary.get("aggregate_return_proxy"))
    threshold = _float(
        benchmark_payload.get("minimum_outperformance_threshold"),
        DEFAULT_COST_MEANINGFUL_THRESHOLD,
    )
    result = []
    for row in baselines:
        baseline_proxy = _float(row.get("baseline_net_performance_proxy"))
        delta = candidate_proxy - baseline_proxy
        if not backfill_ready:
            relative_status = "UNTESTED"
        elif delta < 0:
            relative_status = "BENCHMARK_UNDERPERFORMS"
        elif delta < threshold:
            relative_status = "BENCHMARK_MIXED"
        else:
            relative_status = "BENCHMARK_OUTPERFORMS"
        result.append(
            {
                "baseline_id": _text(row.get("baseline_id")),
                "benchmark_relative_status": relative_status,
                "candidate_return_proxy": candidate_proxy if backfill_ready else None,
                "baseline_return_proxy": baseline_proxy if backfill_ready else None,
                "candidate_delta_vs_baseline": _round_float(delta)
                if backfill_ready
                else None,
                "minimum_outperformance_threshold": threshold,
                "source_backfill_status": _text(backfill_summary.get("candidate_backfill_status")),
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return result


def _comparison_rows(
    *,
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    failure_payload: Mapping[str, Any],
    reusable_payload: Mapping[str, Any],
    backfill_ready: bool,
) -> list[dict[str, Any]]:
    backfill_summary = _mapping(backfill_payload.get("summary"))
    stress_summary = _mapping(stress_review_payload.get("summary"))
    cost_summary = _mapping(cost_benchmark_payload.get("summary"))
    failure_modes = _failure_modes_by_id(failure_payload)
    reusable_evidence = _evidence_by_source(reusable_payload, "reusable_evidence")
    invalidated_evidence = _evidence_by_source(reusable_payload, "invalidated_evidence")
    aggregate_drawdown = _float(backfill_summary.get("aggregate_drawdown_proxy"))
    stress_result = _text(
        stress_summary.get("stress_result"),
        _text(stress_review_payload.get("status")),
    )
    source_backfill_status = _text(backfill_summary.get("candidate_backfill_status"))
    cost_survival_status = _text(cost_summary.get("cost_survival_status"))
    benchmark_relative_status = _text(cost_summary.get("benchmark_relative_status"))
    signal_completeness = _text(backfill_summary.get("signal_completeness"))
    partial_static_proxy = source_backfill_status == CANDIDATE_BACKFILL_PARTIAL

    return [
        _comparison_row(
            "drawdown_mismatch",
            status=(
                "UNMEASURED"
                if not backfill_ready
                else "REGRESSED_VS_REUSABLE_EVIDENCE"
                if stress_result == "WEAK"
                or aggregate_drawdown <= STRESS_BLOCKING_DRAWDOWN_PROXY
                else "MIXED"
                if partial_static_proxy
                else "IMPROVED"
            ),
            new_evidence=(
                f"stress_result={stress_result}; "
                f"aggregate_drawdown_proxy={backfill_summary.get('aggregate_drawdown_proxy')}; "
                f"worst_drawdown_proxy={stress_summary.get('worst_drawdown_proxy')}"
            ),
            returned_context=_returned_context(
                reusable_evidence.get("drawdown_mismatch_reduction"),
                failure_modes,
                "drawdown_mismatch",
            ),
            returned_failure_mode_id="",
            interpretation=(
                "Drawdown proxy remains weak despite reusable prior mismatch evidence."
                if backfill_ready
                else "No executable metric available for drawdown comparison."
            ),
        ),
        _comparison_row(
            "flip_rotation_behavior",
            status="UNMEASURED"
            if not backfill_ready
            else "MIXED"
            if partial_static_proxy
            else "IMPROVED",
            new_evidence=(
                f"rotation_count={backfill_summary.get('rotation_count')}; "
                f"false_risk_off_count={backfill_summary.get('false_risk_off_count')}; "
                f"metric_mode={backfill_summary.get('backfill_metric_mode')}"
            ),
            returned_context=_returned_context(
                reusable_evidence.get("flip_rotation_reduction"),
                failure_modes,
                "flip_rotation_behavior",
            ),
            returned_failure_mode_id="",
            interpretation=(
                "Static proxy shows low rotation and no false risk-off count, "
                "but historical dynamic signal behavior is still unavailable."
            ),
        ),
        _comparison_row(
            "turnover",
            status="UNMEASURED"
            if not backfill_ready
            else "MIXED"
            if partial_static_proxy
            else "IMPROVED",
            new_evidence=f"turnover_proxy={backfill_summary.get('turnover_proxy')}",
            returned_context=(
                "Returned candidate failure attribution did not include a normalized "
                "turnover metric."
            ),
            returned_failure_mode_id="",
            interpretation=(
                "Turnover proxy is real but comes from one static research weight transition."
            ),
        ),
        _comparison_row(
            "cost_survival",
            status="UNMEASURED"
            if not backfill_ready
            else "REPEATS_FAILURE_MODE"
            if cost_survival_status == "COST_SURVIVAL_FAIL"
            else "MIXED"
            if cost_survival_status == "COST_SURVIVAL_WARNING"
            else "IMPROVED",
            new_evidence=(
                f"cost_survival_status={cost_survival_status}; "
                f"turnover_penalty={cost_summary.get('turnover_penalty')}; "
                f"net_proxy_result={cost_summary.get('net_proxy_result')}"
            ),
            returned_context=_returned_context(
                invalidated_evidence.get("cost_sensitivity"),
                failure_modes,
                "cost_survival_failure",
            ),
            returned_failure_mode_id="cost_survival_failure",
            interpretation=(
                "Cost survival is not a hard fail, but partial proxy status keeps it a warning."
            ),
        ),
        _comparison_row(
            "benchmark_relative_behavior",
            status="UNMEASURED"
            if not backfill_ready
            else "REPEATS_FAILURE_MODE"
            if benchmark_relative_status == "BENCHMARK_UNDERPERFORMS"
            else "MIXED"
            if benchmark_relative_status == "BENCHMARK_MIXED"
            else "IMPROVED",
            new_evidence=f"benchmark_relative_status={benchmark_relative_status}",
            returned_context=_returned_context(
                invalidated_evidence.get("benchmark_baseline"),
                failure_modes,
                "benchmark_relative_failure",
            ),
            returned_failure_mode_id="benchmark_relative_failure",
            interpretation=(
                "New candidate still underperforms at least one baseline, repeating "
                "the returned candidate failure mode."
            ),
        ),
        _comparison_row(
            "signal_robustness",
            status="UNMEASURED"
            if not backfill_ready
            else "NO_IMPROVEMENT"
            if signal_completeness != "COMPLETE"
            else "MIXED",
            new_evidence=(
                f"signal_completeness={signal_completeness}; "
                f"signal_completeness_ratio={backfill_summary.get('signal_completeness_ratio')}"
            ),
            returned_context=_returned_context(
                failure_modes.get("signal_input_stability_warning"),
                failure_modes,
                "signal_input_stability_warning",
            ),
            returned_failure_mode_id="signal_input_stability_warning",
            interpretation="Historical signal series remain incomplete for the required windows.",
        ),
        _comparison_row(
            "governance_blockers",
            status="UNMEASURED"
            if not backfill_ready
            else "NO_IMPROVEMENT"
            if partial_static_proxy
            or stress_result == "WEAK"
            or benchmark_relative_status == "BENCHMARK_UNDERPERFORMS"
            else "MIXED",
            new_evidence=(
                f"backfill_status={source_backfill_status}; "
                f"stress_result={stress_result}; "
                f"cost_benchmark_status={cost_benchmark_payload.get('status')}"
            ),
            returned_context=(
                "Returned candidate had governance blockers around resumption, "
                "owner hold, and evidence quality."
            ),
            returned_failure_mode_id="normal_resumption_gate_blocked",
            interpretation=(
                "Research can continue, but comparison does not clear governance "
                "blockers."
            ),
        ),
    ]


def _comparison_row(
    metric_id: str,
    *,
    status: str,
    new_evidence: str,
    returned_context: str,
    returned_failure_mode_id: str,
    interpretation: str,
) -> dict[str, Any]:
    return {
        "metric_id": metric_id,
        "comparison_status": status,
        "new_candidate_evidence": new_evidence,
        "returned_candidate_failure_context": returned_context,
        "returned_failure_mode_id": returned_failure_mode_id,
        "interpretation": interpretation,
        "production_effect": PRODUCTION_EFFECT,
    }


def _comparison_result(rows: Sequence[Mapping[str, Any]], backfill_ready: bool) -> str:
    if not backfill_ready:
        return "NO_IMPROVEMENT"
    statuses = {_text(row.get("comparison_status")) for row in rows}
    if "REPEATS_FAILURE_MODE" in statuses or "REGRESSED_VS_REUSABLE_EVIDENCE" in statuses:
        return "MIXED_VS_RETURNED_CANDIDATE"
    if statuses <= {"IMPROVED"}:
        return "IMPROVED_OVER_RETURNED_CANDIDATE"
    if statuses <= {"NO_IMPROVEMENT", "UNMEASURED"}:
        return "NO_IMPROVEMENT"
    return "MIXED_VS_RETURNED_CANDIDATE"


def _failure_modes_by_id(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        _text(row.get("failure_mode_id")): row
        for row in _records(payload.get("ranked_failure_modes"))
        if _text(row.get("failure_mode_id"))
    }


def _evidence_by_source(
    payload: Mapping[str, Any],
    key: str,
) -> dict[str, dict[str, Any]]:
    return {
        _text(row.get("source_id")): row
        for row in _records(payload.get(key))
        if _text(row.get("source_id"))
    }


def _returned_context(
    row: Mapping[str, Any] | None,
    failure_modes: Mapping[str, Mapping[str, Any]],
    fallback_id: str,
) -> str:
    data = _mapping(row)
    if data:
        return (
            f"{_text(data.get('source_id'), fallback_id)}="
            f"{_text(data.get('status'), 'AVAILABLE')}; "
            f"classification={_text(data.get('classification'), 'unknown')}"
        )
    failure = _mapping(failure_modes.get(fallback_id))
    if failure:
        return (
            f"{fallback_id}: evidence={_text(failure.get('evidence'))}; "
            f"domain={_text(failure.get('domain'))}"
        )
    return f"{fallback_id}: no returned numeric metric row available"


def _comparison_issue_names(
    rows: Sequence[Mapping[str, Any]],
    statuses: set[str],
) -> str:
    names = [
        _text(row.get("metric_id"))
        for row in rows
        if _text(row.get("comparison_status")) in statuses
    ]
    return "; ".join(name for name in names if name) or "none"


def _signal_robustness_check(
    check_id: str,
    *,
    blocked: bool,
    warning: bool,
    evidence: str,
) -> dict[str, Any]:
    status = "BLOCKING" if blocked else "WARNING" if warning else "PASS"
    return {
        "check_id": check_id,
        "status": status,
        "evidence": evidence,
        "fail_closed": True,
        "signal_completeness_rules_relaxed": False,
        "recommended_action": (
            "repair_signal_binding_inputs"
            if blocked
            else "monitor_signal_binding_warning"
            if warning
            else "retain_signal_binding_evidence"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _signal_missing_feature_columns(findings: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        _list_values(row.get("missing_required_columns"))
        for row in findings
        if _text(row.get("input_id")) in {"etf_feature_matrix", "etf_signal_series"}
    )


def _signal_schema_mismatch(findings: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        _list_values(row.get("incompatible_schema_versions"))
        or _list_values(row.get("incompatible_feature_versions"))
        for row in findings
    )


def _signal_staleness_warning(findings: Sequence[Mapping[str, Any]]) -> bool:
    return any(
        _text(row.get("stale_reason")) not in {"", "within_policy_window"}
        for row in findings
    )


def _signal_market_coverage_gap(
    findings: Sequence[Mapping[str, Any]],
    backfill_payload: Mapping[str, Any],
) -> bool:
    finding_gap = any(
        _list_values(row.get("missing_coverage_values"))
        for row in findings
    )
    window_missing = [
        item
        for row in _records(backfill_payload.get("backfill_windows"))
        for item in _list_values(row.get("missing_data_list"))
    ]
    historical_gap = any(
        _text(item).startswith("historical_signal_series:")
        for item in [*_list_values(backfill_payload.get("missing_data_list")), *window_missing]
    )
    return finding_gap or historical_gap


def _range_or_none(values: Sequence[float | int]) -> float | int | None:
    if not values:
        return None
    return _round_float(max(values) - min(values))


def _window_sensitivity_split(
    split_id: str,
    windows_by_id: Mapping[str, Mapping[str, Any]],
    *,
    backfill_ready: bool,
) -> dict[str, Any]:
    source_ids = WINDOW_SENSITIVITY_WINDOW_MAP.get(split_id, ())
    rows = [windows_by_id[window_id] for window_id in source_ids if window_id in windows_by_id]
    metric_rows = [
        row
        for row in rows
        if row.get("return_proxy") is not None and row.get("drawdown_proxy") is not None
    ]
    if not backfill_ready or not metric_rows:
        return {
            "window_split_id": split_id,
            "source_windows": list(source_ids),
            "available_source_windows": [_text(row.get("window_id")) for row in rows],
            "status": "METRICS_UNAVAILABLE",
            "average_return_proxy": None,
            "average_turnover_proxy": None,
            "worst_drawdown_proxy": None,
            "false_flip_proxy": None,
            "rotation_proxy": None,
            "metric_window_count": 0,
            "partial_window_count": 0,
            "evaluation": "Executable backfill metrics are unavailable for this split.",
            "recommended_action": "complete_executable_backfill_before_window_sensitivity",
            "production_effect": PRODUCTION_EFFECT,
        }
    returns = [_float(row.get("return_proxy")) for row in metric_rows]
    drawdowns = [_float(row.get("drawdown_proxy")) for row in metric_rows]
    turnovers = [_float(row.get("turnover")) for row in metric_rows]
    false_flips = [_int(row.get("false_risk_off_count")) for row in metric_rows]
    rotations = [_int(row.get("rotation_count")) for row in metric_rows]
    partial_count = len(
        [row for row in metric_rows if _text(row.get("backfill_window_status")) == "PARTIAL"]
    )
    average_return = _round_float(sum(returns) / len(returns))
    worst_drawdown = _round_float(min(drawdowns))
    average_turnover = _round_float(sum(turnovers) / len(turnovers))
    if worst_drawdown <= STRESS_BLOCKING_DRAWDOWN_PROXY:
        status = "WEAK"
        evaluation = "Worst drawdown proxy breaches conservative stress blocker."
        recommended_action = "repair_or_revise_candidate_before_research_gate"
    elif partial_count:
        status = "PARTIAL_STATIC_PROXY"
        evaluation = "Metrics exist but rely on partial static binding evidence."
        recommended_action = "complete_dynamic_binding_before_window_stability_claim"
    elif worst_drawdown <= STRESS_WARNING_DRAWDOWN_PROXY or average_return < 0:
        status = "MIXED"
        evaluation = "Return/drawdown proxy is mixed in this diagnostic split."
        recommended_action = "review_window_metric_dispersion"
    else:
        status = "STABLE"
        evaluation = "Return/drawdown proxy is stable within the diagnostic band."
        recommended_action = "retain_window_sensitivity_evidence"
    return {
        "window_split_id": split_id,
        "source_windows": list(source_ids),
        "available_source_windows": [_text(row.get("window_id")) for row in rows],
        "status": status,
        "average_return_proxy": average_return,
        "average_turnover_proxy": average_turnover,
        "worst_drawdown_proxy": worst_drawdown,
        "false_flip_proxy": sum(false_flips),
        "rotation_proxy": sum(rotations),
        "metric_window_count": len(metric_rows),
        "partial_window_count": partial_count,
        "evaluation": evaluation,
        "recommended_action": recommended_action,
        "performance_proxy": average_return,
        "turnover_proxy": average_turnover,
        "drawdown_behavior_proxy": worst_drawdown,
        "production_effect": PRODUCTION_EFFECT,
    }


def _research_gate_blockers(
    *,
    safety_audit_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    safety_status = _text(safety_audit_payload.get("status"), "MISSING")
    safety_summary = _mapping(safety_audit_payload.get("summary"))
    safety_acceptable = safety_status == binding_reports.SAFETY_PASS or (
        safety_status == binding_reports.SAFETY_WARNING
        and safety_summary.get("acceptable_warning") is True
    )
    cost_summary = _mapping(cost_benchmark_payload.get("summary"))
    comparison_summary = _mapping(comparison_payload.get("summary"))
    candidates = [
        (
            "safety_audit_not_acceptable",
            not safety_acceptable,
            _text(safety_audit_payload.get("next_action"), "repair_executable_binding_safety"),
            safety_status,
        ),
        (
            "backfill_not_ready",
            _text(backfill_payload.get("status")) not in CANDIDATE_BACKFILL_METRIC_STATUSES,
            _text(backfill_payload.get("next_action")),
            _text(backfill_payload.get("status")),
        ),
        (
            "stress_review_weak",
            _text(stress_review_payload.get("status")) in {"WEAK", "FAIL"},
            _text(stress_review_payload.get("next_action")),
            _text(stress_review_payload.get("status")),
        ),
        (
            "cost_benchmark_unavailable",
            _text(cost_benchmark_payload.get("status"))
            == "COST_BENCHMARK_NEEDS_EXECUTABLE_BACKFILL",
            _text(cost_benchmark_payload.get("next_action")),
            _text(cost_benchmark_payload.get("status")),
        ),
        (
            "cost_benchmark_weak",
            _text(cost_benchmark_payload.get("status")) == "COST_BENCHMARK_REVIEW_WEAK",
            _text(cost_benchmark_payload.get("next_action")),
            (
                f"{_text(cost_benchmark_payload.get('status'))}; "
                f"benchmark={_text(cost_summary.get('benchmark_relative_status'))}; "
                f"cost={_text(cost_summary.get('cost_survival_status'))}"
            ),
        ),
        (
            "repeated_failure_mode_unresolved",
            _int(comparison_summary.get("repeated_failure_mode_count")) > 0,
            _text(comparison_payload.get("next_action")),
            (
                f"repeated_failure_mode_count="
                f"{_int(comparison_summary.get('repeated_failure_mode_count'))}"
            ),
        ),
        (
            "no_improvement_established",
            _text(comparison_payload.get("status"))
            in {"NO_IMPROVEMENT", "WORSE_THAN_RETURNED_CANDIDATE"},
            _text(comparison_payload.get("next_action")),
            _text(comparison_payload.get("status")),
        ),
        (
            "signal_robustness_blocked",
            _text(signal_robustness_payload.get("status")) == "SIGNAL_ROBUSTNESS_BLOCKED",
            _text(signal_robustness_payload.get("next_action")),
            (
                f"{_text(signal_robustness_payload.get('status'))}; "
                f"blocking_checks="
                f"{_int(_mapping(signal_robustness_payload.get('summary')).get('blocking_check_count'))}"
            ),
        ),
        (
            "window_sensitivity_fragile",
            _text(window_sensitivity_payload.get("status"))
            in {"WINDOW_FRAGILE", "OVERFIT_RISK_HIGH"},
            _text(window_sensitivity_payload.get("next_action")),
            (
                f"{_text(window_sensitivity_payload.get('status'))}; "
                f"overfit_risk="
                f"{_text(_mapping(window_sensitivity_payload.get('summary')).get('overfit_risk'))}"
            ),
        ),
    ]
    return [
        {
            "issue_id": issue_id,
            "recommended_action": recommended_action,
            "evidence": evidence,
            "production_effect": PRODUCTION_EFFECT,
        }
        for issue_id, active, recommended_action, evidence in candidates
        if active
    ]


def _research_gate_source_statuses(
    *,
    safety_audit_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
) -> dict[str, str]:
    return {
        "executable_binding_safety_audit": _text(
            safety_audit_payload.get("status"),
            "MISSING",
        ),
        "next_candidate_backfill": _text(backfill_payload.get("status"), "MISSING"),
        "next_candidate_stress_review": _text(
            stress_review_payload.get("status"),
            "MISSING",
        ),
        "next_candidate_cost_benchmark_review": _text(
            cost_benchmark_payload.get("status"),
            "MISSING",
        ),
        "next_candidate_vs_returned_candidate_comparison": _text(
            comparison_payload.get("status"),
            "MISSING",
        ),
        "next_candidate_signal_robustness_review": _text(
            signal_robustness_payload.get("status"),
            "MISSING",
        ),
        "next_candidate_overfit_window_sensitivity": _text(
            window_sensitivity_payload.get("status"),
            "MISSING",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _research_gate_positive_evidence(
    source_statuses: Mapping[str, str],
) -> list[dict[str, Any]]:
    candidates = [
        (
            "safety_audit_acceptable",
            source_statuses.get("executable_binding_safety_audit")
            in {binding_reports.SAFETY_PASS, binding_reports.SAFETY_WARNING},
            source_statuses.get("executable_binding_safety_audit"),
        ),
        (
            "real_backfill_metrics_available",
            source_statuses.get("next_candidate_backfill") in CANDIDATE_BACKFILL_METRIC_STATUSES,
            source_statuses.get("next_candidate_backfill"),
        ),
        (
            "row_level_vs_returned_comparison_available",
            bool(source_statuses.get("next_candidate_vs_returned_candidate_comparison")),
            source_statuses.get("next_candidate_vs_returned_candidate_comparison"),
        ),
    ]
    return [
        {
            "evidence_id": evidence_id,
            "source_status": _text(source_status),
            "interpretation": "available_research_evidence_not_promotion_clearance",
            "production_effect": PRODUCTION_EFFECT,
        }
        for evidence_id, include, source_status in candidates
        if include
    ]


def _research_gate_negative_evidence(
    blockers: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "evidence_id": _text(row.get("issue_id")),
            "evidence": _text(row.get("evidence")),
            "recommended_action": _text(row.get("recommended_action")),
            "production_effect": PRODUCTION_EFFECT,
        }
        for row in blockers
    ]


def _research_gate_decision(
    *,
    blockers: Sequence[Mapping[str, Any]],
    comparison_payload: Mapping[str, Any],
) -> str:
    if not blockers:
        return "RESEARCH_PROMISING"
    blocker_ids = {_text(row.get("issue_id")) for row in blockers}
    incomplete_ids = {
        "safety_audit_not_acceptable",
        "backfill_not_ready",
        "signal_robustness_blocked",
        "window_sensitivity_fragile",
    }
    comparison_status = _text(comparison_payload.get("status"))
    if comparison_status == "WORSE_THAN_RETURNED_CANDIDATE" and not (
        blocker_ids & incomplete_ids
    ):
        return "REJECT_RESEARCH_CANDIDATE"
    if {
        "stress_review_weak",
        "cost_benchmark_weak",
        "repeated_failure_mode_unresolved",
        "no_improvement_established",
    } & blocker_ids and not (blocker_ids & incomplete_ids):
        return "RETURN_TO_HYPOTHESIS_BACKLOG"
    return "NEEDS_MORE_EVIDENCE"


def _research_gate_required_next_action(
    decision: str,
    blockers: Sequence[Mapping[str, Any]],
) -> str:
    if decision == "RESEARCH_PROMISING":
        return "prepare_deeper_research_validation_plan"
    if decision == "RETURN_TO_HYPOTHESIS_BACKLOG":
        return "revise_hypothesis_from_repeated_failure_modes"
    if decision == "REJECT_RESEARCH_CANDIDATE":
        return "prepare_research_rejection_postmortem"
    if any(
        _text(row.get("issue_id"))
        in {"signal_robustness_blocked", "window_sensitivity_fragile"}
        for row in blockers
    ):
        return "repair_signal_window_evidence_before_gate_rerun"
    return "collect_missing_research_evidence_before_gate_rerun"


def _snapshot_source_statuses(
    source_payloads: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "report_type": _text(item.get("report_type"), "MISSING"),
            "status": _text(item.get("status"), "MISSING"),
            "next_action": _text(item.get("next_action")),
            "production_effect": _text(item.get("production_effect"), PRODUCTION_EFFECT),
        }
        for item in source_payloads
    ]


def _required_source_payload(
    payload: Mapping[str, Any] | None,
    report_type: str,
) -> dict[str, Any]:
    data = _mapping(payload)
    if data:
        return data
    return {
        "report_type": report_type,
        "status": "MISSING",
        "next_action": "generate_required_executable_research_cycle_artifact",
        "production_effect": PRODUCTION_EFFECT,
    }


def _snapshot_missing_required_artifacts(
    source_statuses: Sequence[Mapping[str, Any]],
) -> list[str]:
    status_by_type = {
        _text(row.get("report_type")): _text(row.get("status"))
        for row in source_statuses
    }
    required = (
        binding_reports.CONTRACT_REPORT_TYPE,
        binding_reports.SIGNAL_BINDING_REPORT_TYPE,
        binding_reports.WEIGHT_BINDING_REPORT_TYPE,
        binding_reports.SAFETY_AUDIT_REPORT_TYPE,
        BACKFILL_REPORT_TYPE,
        STRESS_REVIEW_REPORT_TYPE,
        COST_BENCHMARK_REVIEW_REPORT_TYPE,
        VS_RETURNED_REPORT_TYPE,
        SIGNAL_ROBUSTNESS_REPORT_TYPE,
        WINDOW_SENSITIVITY_REPORT_TYPE,
        RESEARCH_GATE_REPORT_TYPE,
        OWNER_REVIEW_PACKET_REPORT_TYPE,
    )
    return [
        report_type
        for report_type in required
        if not _text(status_by_type.get(report_type))
        or _text(status_by_type.get(report_type)) == "MISSING"
    ]


def _executable_research_cycle_status(
    *,
    gate_decision: str,
    missing_required: Sequence[str],
) -> str:
    if missing_required:
        return "EXECUTABLE_RESEARCH_CYCLE_BLOCKED"
    if gate_decision == "RESEARCH_PROMISING":
        return "EXECUTABLE_RESEARCH_CYCLE_PROMISING"
    if gate_decision == "NEEDS_MORE_EVIDENCE":
        return "EXECUTABLE_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    if gate_decision == "RETURN_TO_HYPOTHESIS_BACKLOG":
        return "EXECUTABLE_RESEARCH_CYCLE_RETURN_TO_BACKLOG"
    if gate_decision == "REJECT_RESEARCH_CANDIDATE":
        return "EXECUTABLE_RESEARCH_CYCLE_REJECT"
    return "EXECUTABLE_RESEARCH_CYCLE_BLOCKED"


def _owner_option(
    option_id: str,
    *,
    evidence_required: Sequence[str],
    risks: Sequence[str],
    next_action: str,
) -> dict[str, Any]:
    return {
        "option_id": option_id,
        "evidence_required": list(evidence_required),
        "risks": list(risks),
        "next_action": next_action,
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_allowed": False,
        "broker_order_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _date_range_from_windows(windows: Sequence[Mapping[str, Any]]) -> str:
    starts = [_text(row.get("start")) for row in windows if _text(row.get("start"))]
    ends = [_text(row.get("end")) for row in windows if _text(row.get("end"))]
    if not starts or not ends:
        return f"{PRIMARY_RESEARCH_START}..unspecified"
    return f"{min(starts)}..{max(ends)}"


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _read_json_mapping(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(raw)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_values(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _round_float(value: float, digits: int = 6) -> float:
    return round(float(value), digits)


def _date_from_payload(payload: Mapping[str, Any]) -> date:
    try:
        return date.fromisoformat(_text(payload.get("as_of")))
    except ValueError:
        return date.today()


def _artifact_id(payload: Mapping[str, Any]) -> str:
    report_type = _text(payload.get("report_type"), "artifact")
    as_of = _text(payload.get("as_of"), "unknown")
    return f"{report_type}:{as_of}"


def _issue_names(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    values = [_text(row.get(key)) for row in rows if _text(row.get(key))]
    return "; ".join(values) if values else "none"


def _display_path(value: Any) -> str:
    path = Path(_text(value))
    try:
        rel = path.resolve().relative_to(PROJECT_ROOT.resolve())
        return rel.as_posix()
    except (ValueError, OSError):
        return str(path)


def _latest_matching_path(project_root: Path, patterns: Sequence[str]) -> Path | None:
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(project_root.glob(pattern))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}$")
    candidates: list[tuple[date, Path]] = []
    if not output_dir.exists():
        return None
    for path in output_dir.iterdir():
        match = pattern.match(path.name)
        if not match:
            continue
        try:
            candidates.append((date.fromisoformat(match.group(1)), path))
        except ValueError:
            continue
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _md_cell(value: Any) -> str:
    text = "N/A" if value is None else _text(value)
    return text.replace("|", "\\|").replace("\n", "<br/>")


def _title(report_type: str) -> str:
    return report_type.replace("_", " ").title()


def _markdown_table_keys(report_type: str) -> list[tuple[str, str]]:
    if report_type == INTAKE_REPORT_TYPE:
        return [
            ("P0 Hypotheses", "p0_hypotheses"),
            ("Reusable Evidence", "reusable_evidence"),
            ("Invalidated Or Weak Evidence", "invalidated_or_weak_evidence"),
        ]
    if report_type == BACKFILL_REPORT_TYPE:
        return [("Backfill Windows", "backfill_windows"), ("Blocking Issues", "blocking_issues")]
    if report_type == STRESS_REVIEW_REPORT_TYPE:
        return [
            ("Scenario Reviews", "scenario_reviews"),
            ("Blocking Scenarios", "blocking_scenarios"),
        ]
    if report_type == COST_BENCHMARK_REVIEW_REPORT_TYPE:
        return [
            ("Cost Scenarios", "cost_scenario_reviews"),
            ("Benchmark Reviews", "benchmark_reviews"),
        ]
    if report_type == VS_RETURNED_REPORT_TYPE:
        return [("Comparison Rows", "comparison_rows")]
    if report_type == SIGNAL_ROBUSTNESS_REPORT_TYPE:
        return [("Signal Quality Checks", "signal_quality_checks")]
    if report_type == WINDOW_SENSITIVITY_REPORT_TYPE:
        return [("Window Splits", "window_splits")]
    if report_type == RESEARCH_GATE_REPORT_TYPE:
        return [("Blockers", "blocker_list")]
    if report_type == OWNER_REVIEW_PACKET_REPORT_TYPE:
        return [("Owner Options", "owner_options")]
    if report_type.endswith(VALIDATION_SUFFIX):
        return [("Checks", "checks"), ("Blocking Issues", "blocking_issues")]
    return [("Source Statuses", "source_statuses")]


def _table_records(title: str, value: Any) -> list[str]:
    rows = _records(value)
    if not rows:
        return ["", f"## {title}", "", "No rows."]
    keys = list(rows[0].keys())[:8]
    lines = [
        "",
        f"## {title}",
        "",
        "|" + "|".join(keys) + "|",
        "|" + "|".join(["---"] * len(keys)) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(_md_cell(row.get(key)) for key in keys) + "|")
    return lines


__all__ = [
    "BACKFILL_REPORT_TYPE",
    "COST_BENCHMARK_REVIEW_REPORT_TYPE",
    "CYCLE_SNAPSHOT_REPORT_TYPE",
    "FROZEN_SPEC_REPORT_TYPE",
    "INTAKE_REPORT_TYPE",
    "NEXT_RESEARCH_CYCLE_REPORT_TYPES",
    "OWNER_REVIEW_PACKET_REPORT_TYPE",
    "REPORT_PREFIXES",
    "RESEARCH_GATE_REPORT_TYPE",
    "SIGNAL_ROBUSTNESS_REPORT_TYPE",
    "STRESS_REVIEW_REPORT_TYPE",
    "VALIDATION_SUFFIX",
    "VS_RETURNED_REPORT_TYPE",
    "WINDOW_SENSITIVITY_REPORT_TYPE",
    "build_next_candidate_backfill_payload",
    "build_next_candidate_cost_benchmark_review_payload",
    "build_next_candidate_owner_research_review_packet_payload",
    "build_next_candidate_research_gate_payload",
    "build_next_candidate_signal_robustness_review_payload",
    "build_next_candidate_spec_frozen_payload",
    "build_next_candidate_stress_review_payload",
    "build_next_candidate_vs_returned_comparison_payload",
    "build_next_candidate_window_sensitivity_payload",
    "build_next_research_cycle_intake_payload",
    "build_next_research_cycle_payloads",
    "build_next_research_cycle_snapshot_payload",
    "default_next_research_cycle_json_path",
    "default_next_research_cycle_markdown_path",
    "latest_next_research_cycle_json_path",
    "render_next_research_cycle_markdown",
    "validate_next_research_cycle_payload",
    "write_next_research_cycle_json",
    "write_next_research_cycle_markdown",
]
