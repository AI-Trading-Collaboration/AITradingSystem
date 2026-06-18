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
MARKET_REGIME = "ai_after_chatgpt"
AI_REGIME_START = "2022-12-01"
CANDIDATE_BACKFILL_COMPLETE = "CANDIDATE_BACKFILL_COMPLETE"
CANDIDATE_BACKFILL_PARTIAL = "CANDIDATE_BACKFILL_PARTIAL"
CANDIDATE_BACKFILL_BLOCKED = "CANDIDATE_BACKFILL_BLOCKED"
CANDIDATE_BACKFILL_METRIC_STATUSES = {
    CANDIDATE_BACKFILL_COMPLETE,
    CANDIDATE_BACKFILL_PARTIAL,
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
    comparison_rows = _comparison_rows(backfill_ready)
    comparison_result = (
        "MIXED_VS_RETURNED_CANDIDATE"
        if backfill_ready
        else "NO_IMPROVEMENT"
    )
    summary = {
        "comparison_result": comparison_result,
        "previous_candidate_id": reset_reports.CANDIDATE_ID,
        "new_candidate_id": _text(_mapping(backfill_payload.get("summary")).get("candidate_id")),
        "measurable_improvement_established": backfill_ready,
        "governance_blockers": (
            "executable_backfill_missing" if not backfill_ready else "none"
        ),
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
                "Improvement over the returned candidate is not established without "
                "new executable backfill evidence."
            ),
            key_result=comparison_result,
            blocking_issues=summary["governance_blockers"],
            warnings="do_not_reuse_returned_candidate_failure_mode",
            next_action="resolve_new_candidate_evidence_before_comparison_claim",
        ),
        next_action="resolve_new_candidate_evidence_before_comparison_claim",
        safety_boundary=_safety_boundary(),
        limitations=["No improvement is claimed from missing new-candidate metrics."],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_signal_robustness_review_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    frozen_spec_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
) -> dict[str, Any]:
    sources = _load_project_sources(project_root)
    signal_source = _mapping(sources.get("signal_input_completeness"))
    frozen_spec = _mapping(frozen_spec_payload.get("frozen_candidate_spec"))
    executable_ready = _text(frozen_spec.get("executable_signal_binding_status")) == "AVAILABLE"
    checks = [
        _signal_check("missing_feature_columns", executable_ready, signal_source),
        _signal_check("partial_signal_series", executable_ready, signal_source),
        _signal_check("stale_signal_series", executable_ready, signal_source),
        _signal_check("schema_version_mismatch", executable_ready, signal_source),
        _signal_check("market_coverage_gap", executable_ready, signal_source),
    ]
    blocking = [row for row in checks if row["status"] == "BLOCKING"]
    status = "SIGNAL_ROBUSTNESS_BLOCKED" if blocking else "SIGNAL_ROBUSTNESS_READY"
    summary = {
        "signal_robustness_status": status,
        "fail_closed_behavior": True,
        "blocking_check_count": len(blocking),
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
            summary="Signal robustness is blocked until executable signal inputs exist.",
            key_result=status,
            blocking_issues=_issue_names(blocking, "check_id"),
            warnings="signal completeness rules remain unchanged",
            next_action="provide_executable_signal_binding_and_rerun_completeness",
        ),
        next_action="provide_executable_signal_binding_and_rerun_completeness",
        safety_boundary=_safety_boundary(),
        limitations=["Review does not relax signal completeness rules."],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_window_sensitivity_payload(
    *,
    as_of: date,
    frozen_spec_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
) -> dict[str, Any]:
    backfill_status = _text(
        _mapping(backfill_payload.get("summary")).get("candidate_backfill_status")
    )
    backfill_ready = _backfill_metrics_available(backfill_status)
    frozen_spec = _mapping(frozen_spec_payload.get("frozen_candidate_spec"))
    windows = _records(frozen_spec.get("validation_windows"))
    splits = [
        _window_sensitivity_split(split_id, windows, backfill_ready=backfill_ready)
        for split_id in WINDOW_SENSITIVITY_SPLITS
    ]
    status = "WINDOW_MIXED" if backfill_ready else "WINDOW_FRAGILE"
    summary = {
        "window_sensitivity_status": status,
        "split_count": len(splits),
        "performance_dispersion": None,
        "turnover_dispersion": None,
        "drawdown_behavior_dispersion": None,
        "false_flip_dispersion": None,
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
            "blocking_issues": (
                [
                    {
                        "issue_id": "window_metrics_unavailable",
                        "recommended_action": (
                            "complete_executable_backfill_before_window_sensitivity"
                        ),
                    }
                ]
                if not backfill_ready
                else []
            ),
        },
        reader_brief=_reader_brief(
            summary="Window sensitivity is fragile until window metrics exist.",
            key_result=status,
            blocking_issues="window_metrics_unavailable" if not backfill_ready else "none",
            warnings="does_not_promote_narrow_window_result",
            next_action="complete_backfill_before_window_stability_claim",
        ),
        next_action="complete_backfill_before_window_stability_claim",
        safety_boundary=_safety_boundary(),
        limitations=["No stability claim is made without executable backfill metrics."],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_research_gate_payload(
    *,
    as_of: date,
    frozen_spec_payload: Mapping[str, Any],
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
) -> dict[str, Any]:
    blockers = _research_gate_blockers(
        backfill_payload=backfill_payload,
        stress_review_payload=stress_review_payload,
        cost_benchmark_payload=cost_benchmark_payload,
        comparison_payload=comparison_payload,
        signal_robustness_payload=signal_robustness_payload,
        window_sensitivity_payload=window_sensitivity_payload,
    )
    decision = "NEEDS_MORE_EVIDENCE" if blockers else "RESEARCH_PROMISING"
    summary = {
        "research_gate_decision": decision,
        "candidate_id": _candidate_id_from_frozen(frozen_spec_payload),
        "blocker_count": len(blockers),
        "strongest_positive_evidence_count": 2,
        "strongest_negative_evidence_count": len(blockers),
        "paper_shadow_activation_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=RESEARCH_GATE_REPORT_TYPE,
        as_of=as_of,
        status=decision,
        purpose="Decide whether the new research candidate deserves deeper validation.",
        input_artifacts={
            "next_candidate_spec_frozen": _artifact_id(frozen_spec_payload),
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
            "strongest_positive_evidence": [
                "P0 hypotheses directly target cost survival and benchmark weakness.",
                (
                    "Stress/drawdown/flip evidence from the old candidate remains "
                    "reusable as diagnostics."
                ),
            ],
            "strongest_negative_evidence": [
                _text(row.get("issue_id")) for row in blockers
            ],
            "blocker_list": blockers,
            "required_next_action": (
                "bind_executable_research_candidate_and_regenerate_backfill"
                if blockers
                else "prepare_deeper_research_validation_plan"
            ),
        },
        reader_brief=_reader_brief(
            summary=f"Research gate decision is {decision}; paper-shadow remains forbidden.",
            key_result=decision,
            blocking_issues=_issue_names(blockers, "issue_id"),
            warnings="research gate cannot activate paper-shadow",
            next_action=(
                "bind_executable_research_candidate_and_regenerate_backfill"
                if blockers
                else "prepare_deeper_research_validation_plan"
            ),
        ),
        next_action=(
            "bind_executable_research_candidate_and_regenerate_backfill"
            if blockers
            else "prepare_deeper_research_validation_plan"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["This research gate does not allow paper-shadow activation."],
        requested_date_range=_text(_mapping(backfill_payload.get("summary")).get("requested_date_range")),
    )


def build_next_candidate_owner_research_review_packet_payload(
    *,
    as_of: date,
    research_gate_payload: Mapping[str, Any],
) -> dict[str, Any]:
    gate_decision = _text(
        _mapping(research_gate_payload.get("summary")).get("research_gate_decision")
    )
    options = [
        _owner_option(
            "continue_research_validation",
            evidence_required=[
                "executable research signal/weight binding",
                "new candidate backfill with data quality PASS",
                "stress/cost/benchmark/signal/window validation reruns",
            ],
            risks=[
                "research time spent before metrics prove improvement",
                "old failure mode may repeat if cost/benchmark blockers persist",
            ],
            next_action="bind_and_rerun_research_validation_chain",
        ),
        _owner_option(
            "revise_hypothesis",
            evidence_required=[
                "owner-reviewed hypothesis revision",
                "updated frozen spec with explicit executable binding",
            ],
            risks=["revision may move away from original P0 failure attribution"],
            next_action="return_to_hypothesis_backlog_for_revision",
        ),
        _owner_option(
            "reject_research_candidate",
            evidence_required=[
                "owner decision to reject research candidate",
                "documented rejection rationale",
            ],
            risks=["may discard still-useful diagnostic evidence"],
            next_action="create_research_rejection_postmortem",
        ),
        _owner_option(
            "hold_for_more_data",
            evidence_required=["specific data/source or sample condition to wait for"],
            risks=["research cycle remains incomplete"],
            next_action="wait_for_required_evidence_without_state_mutation",
        ),
    ]
    summary = {
        "owner_packet_status": "OWNER_RESEARCH_REVIEW_PACKET_READY",
        "source_research_gate_decision": gate_decision,
        "option_count": len(options),
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
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
        body={"owner_options": options},
        reader_brief=_reader_brief(
            summary="Owner research review packet is ready; no decision is appended.",
            key_result=summary["owner_packet_status"],
            blocking_issues="none",
            warnings="manual owner decision required before any state transition",
            next_action="owner_review_research_options_manually",
        ),
        next_action="owner_review_research_options_manually",
        safety_boundary=_safety_boundary(),
        limitations=["This packet does not append owner decisions automatically."],
        requested_date_range=_text(research_gate_payload.get("requested_date_range")),
    )


def build_next_research_cycle_snapshot_payload(
    *,
    as_of: date,
    intake_payload: Mapping[str, Any],
    frozen_spec_payload: Mapping[str, Any],
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
    if gate_decision == "RESEARCH_PROMISING":
        status = "NEXT_RESEARCH_CYCLE_READY_FOR_OWNER_REVIEW"
    elif gate_decision in {"NEEDS_MORE_EVIDENCE", "RETURN_TO_HYPOTHESIS_BACKLOG"}:
        status = "NEXT_RESEARCH_CYCLE_NEEDS_MORE_EVIDENCE"
    else:
        status = "NEXT_RESEARCH_CYCLE_BLOCKED"
    source_payloads = (
        intake_payload,
        frozen_spec_payload,
        backfill_payload,
        stress_review_payload,
        cost_benchmark_payload,
        comparison_payload,
        signal_robustness_payload,
        window_sensitivity_payload,
        research_gate_payload,
        owner_packet_payload,
    )
    summary = {
        "research_cycle_snapshot_status": status,
        "research_gate_decision": gate_decision,
        "candidate_id": _candidate_id_from_frozen(frozen_spec_payload),
        "market_regime": MARKET_REGIME,
        "requested_date_range": _text(backfill_payload.get("requested_date_range")),
        "artifact_count": len(source_payloads),
        "owner_packet_ready": _text(owner_packet_payload.get("status"))
        == "OWNER_RESEARCH_REVIEW_PACKET_READY",
        "paper_shadow_activation_allowed": False,
        "extended_shadow_allowed": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "broker_order_allowed": False,
        "production_effect": PRODUCTION_EFFECT,
    }
    return _payload(
        report_type=CYCLE_SNAPSHOT_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose="Generate the final snapshot for the new research cycle.",
        input_artifacts={
            _text(item.get("report_type")): _artifact_id(item)
            for item in source_payloads
        },
        output_decision=status,
        summary=summary,
        body={
            "source_statuses": [
                {
                    "report_type": _text(item.get("report_type")),
                    "status": _text(item.get("status")),
                    "next_action": _text(item.get("next_action")),
                }
                for item in source_payloads
            ],
            "source_reader_briefs": {
                _text(item.get("report_type")): _mapping(item.get("reader_brief"))
                for item in source_payloads
            },
        },
        reader_brief=_reader_brief(
            summary=f"Next research-cycle snapshot is {status}.",
            key_result=status,
            blocking_issues=(
                "evidence_required_before_owner_ready"
                if status != "NEXT_RESEARCH_CYCLE_READY_FOR_OWNER_REVIEW"
                else "none"
            ),
            warnings="research-only; no paper-shadow/live/weights/broker approval",
            next_action=(
                "complete_missing_research_evidence"
                if status != "NEXT_RESEARCH_CYCLE_READY_FOR_OWNER_REVIEW"
                else "manual_owner_research_review"
            ),
        ),
        next_action=(
            "complete_missing_research_evidence"
            if status != "NEXT_RESEARCH_CYCLE_READY_FOR_OWNER_REVIEW"
            else "manual_owner_research_review"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Final snapshot is a research-cycle state report, not a trading approval."],
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


def _comparison_rows(backfill_ready: bool) -> list[dict[str, Any]]:
    metrics = (
        "drawdown_mismatch_reduction",
        "flip_rotation_reduction",
        "turnover",
        "cost_survival",
        "benchmark_relative_behavior",
        "signal_robustness",
        "governance_blockers",
    )
    return [
        {
            "metric_id": metric,
            "comparison_status": "UNMEASURED" if not backfill_ready else "MIXED",
            "new_candidate_evidence": None,
            "returned_candidate_failure_context": "available",
            "production_effect": PRODUCTION_EFFECT,
        }
        for metric in metrics
    ]


def _signal_check(
    check_id: str,
    executable_ready: bool,
    signal_source: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if executable_ready else "BLOCKING",
        "source_available": bool(signal_source.get("available")),
        "source_artifact_path": _text(signal_source.get("path")),
        "fail_closed": True,
        "signal_completeness_rules_relaxed": False,
        "recommended_action": (
            "review_signal_completeness_output"
            if executable_ready
            else "provide_executable_candidate_signal_series"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _window_sensitivity_split(
    split_id: str,
    windows: Sequence[Mapping[str, Any]],
    *,
    backfill_ready: bool,
) -> dict[str, Any]:
    return {
        "window_split_id": split_id,
        "source_windows": [_text(row.get("window_id")) for row in windows],
        "status": "READY" if backfill_ready else "METRICS_UNAVAILABLE",
        "performance_proxy": None,
        "turnover_proxy": None,
        "drawdown_behavior_proxy": None,
        "false_flip_proxy": None,
        "production_effect": PRODUCTION_EFFECT,
    }


def _research_gate_blockers(
    *,
    backfill_payload: Mapping[str, Any],
    stress_review_payload: Mapping[str, Any],
    cost_benchmark_payload: Mapping[str, Any],
    comparison_payload: Mapping[str, Any],
    signal_robustness_payload: Mapping[str, Any],
    window_sensitivity_payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates = [
        (
            "backfill_not_ready",
            _text(backfill_payload.get("status")) not in CANDIDATE_BACKFILL_METRIC_STATUSES,
            _text(backfill_payload.get("next_action")),
        ),
        (
            "stress_review_weak",
            _text(stress_review_payload.get("status")) in {"WEAK", "FAIL"},
            _text(stress_review_payload.get("next_action")),
        ),
        (
            "cost_benchmark_unavailable",
            _text(cost_benchmark_payload.get("status"))
            == "COST_BENCHMARK_NEEDS_EXECUTABLE_BACKFILL",
            _text(cost_benchmark_payload.get("next_action")),
        ),
        (
            "no_improvement_established",
            _text(comparison_payload.get("status"))
            in {"NO_IMPROVEMENT", "WORSE_THAN_RETURNED_CANDIDATE"},
            _text(comparison_payload.get("next_action")),
        ),
        (
            "signal_robustness_blocked",
            _text(signal_robustness_payload.get("status")) == "SIGNAL_ROBUSTNESS_BLOCKED",
            _text(signal_robustness_payload.get("next_action")),
        ),
        (
            "window_sensitivity_fragile",
            _text(window_sensitivity_payload.get("status"))
            in {"WINDOW_FRAGILE", "OVERFIT_RISK_HIGH"},
            _text(window_sensitivity_payload.get("next_action")),
        ),
    ]
    return [
        {
            "issue_id": issue_id,
            "recommended_action": recommended_action,
            "production_effect": PRODUCTION_EFFECT,
        }
        for issue_id, active, recommended_action in candidates
        if active
    ]


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
        return f"{AI_REGIME_START}..unspecified"
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
