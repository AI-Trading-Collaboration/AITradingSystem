from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_gate_policy_v2_reconciliation import (
    run_first_layer_gate_policy_v2_reconciliation_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_ACTUAL_PATH_YAML_PATH,
    DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    run_first_layer_performance_gate_audit_pack,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    load_mapping,
    mapping,
    records,
    round_float,
    strings,
    to_float,
    write_json,
    write_markdown,
    write_yaml,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_active_selection_rule_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2276_FIRST_LAYER_ACTIVE_SELECTION_RULE_AUDIT"
SELECTION_MODES = (
    "no_active_selection",
    "relaxed_active_selection",
    "current_active_selection",
    "strict_active_selection",
)
RECOMMENDED_ACTIONS = {
    "keep_current",
    "relax_threshold",
    "convert_to_ranked_review",
    "convert_to_owner_review_queue",
    "split_selection_and_promotion",
    "remove_active_selection",
    "inconclusive_more_candidates_needed",
}
METRIC_FIELD_MAP = {
    "false_risk_on_delta": "false_risk_on_delta",
    "false_risk_off_delta": "false_risk_off_delta",
    "drawdown_delta": "max_drawdown_delta",
    "turnover_delta": "turnover_delta",
    "cost_adjusted_return_delta": "cost_adjusted_return_delta",
    "benchmark_consistency_delta": "benchmark_consistency_delta",
    "stress_slice_delta": "stress_2022_slice_delta",
    "beta_dependency_delta": "beta_dependency_delta",
    "tqqq_dependency_delta": "tqqq_dependency_delta",
}


def run_first_layer_active_selection_rule_audit_pack(
    *,
    active_selection_rule_path: Path = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    actual_path_path: Path = DEFAULT_ACTUAL_PATH_YAML_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    source_root = output_root / "_regenerated_sources"
    source_docs_root = source_root / "docs"
    performance_audit = run_first_layer_performance_gate_audit_pack(
        actual_path_path=actual_path_path,
        selection_rule_path=active_selection_rule_path,
        output_root=source_root / "trading_2274_gate_audit",
        docs_root=source_docs_root / "trading_2274_gate_audit",
    )
    performance_paths = mapping(performance_audit.get("artifact_paths"))
    gate_policy_v2 = run_first_layer_gate_policy_v2_reconciliation_pack(
        recommended_gate_policy_path=Path(str(performance_paths["recommended_gate_policy"])),
        gate_ablation_matrix_path=Path(str(performance_paths["gate_ablation_matrix"])),
        threshold_sensitivity_path=Path(str(performance_paths["threshold_sensitivity_report"])),
        active_selection_rule_path=active_selection_rule_path,
        output_root=source_root / "trading_2275_gate_policy_v2",
        docs_root=source_docs_root / "trading_2275_gate_policy_v2",
    )
    active_selection_rule = load_mapping(active_selection_rule_path)
    actual_path = load_mapping(actual_path_path)

    candidate_rows = [
        row
        for row in records(performance_audit.get("candidate_rows"))
        if not bool(row.get("diagnostic_only_exclusion"))
    ]
    actual_path_rows = {
        str(row.get("policy_id")): row for row in records(actual_path.get("policy_rows"))
    }
    v2_gate_rows = {
        str(row.get("gate_id")): row for row in records(gate_policy_v2.get("gate_policy_v2_rows"))
    }
    candidate_assessments = [
        _candidate_assessment(
            row=row,
            actual_path_row=mapping(actual_path_rows.get(str(row.get("policy_id")))),
            v2_gate_rows=v2_gate_rows,
        )
        for row in candidate_rows
    ]
    mode_rows = [
        _selection_mode_row(
            mode=mode,
            candidate_assessments=candidate_assessments,
            active_selection_rule=active_selection_rule,
        )
        for mode in SELECTION_MODES
    ]
    current_row = _mode_row(mode_rows, "current_active_selection")
    counterfactual = _counterfactual_report(
        candidate_assessments=candidate_assessments,
        mode_rows=mode_rows,
    )
    threshold_sensitivity = _threshold_sensitivity(
        candidate_assessments=candidate_assessments,
        mode_rows=mode_rows,
    )
    conclusion = _conclusion(
        candidate_assessments=candidate_assessments,
        mode_rows=mode_rows,
        current_row=current_row,
    )
    summary = _summary(
        performance_audit=performance_audit,
        gate_policy_v2=gate_policy_v2,
        mode_rows=mode_rows,
        conclusion=conclusion,
    )
    common = {
        "schema_version": "first_layer_active_selection_rule_audit.v1",
        "report_type": "first_layer_active_selection_rule_audit",
        "title": "First-Layer Active Selection Rule Audit",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": performance_audit.get("market_regime"),
        "anchor_event": performance_audit.get("anchor_event"),
        "anchor_date": performance_audit.get("anchor_date"),
        "requested_start": performance_audit.get("requested_start"),
        "actual_signal_start": performance_audit.get("actual_signal_start"),
        "actual_path_requested_start": performance_audit.get("actual_path_requested_start"),
        "actual_path_actual_start": performance_audit.get("actual_path_actual_start"),
        "data_quality_status": performance_audit.get("data_quality_status"),
        "source_generation": {
            "recommended_gate_policy_v2_source": (
                "regenerated_from_trading_2274_and_2275_code_paths"
            ),
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "active_selection_rule": str(active_selection_rule_path),
            "actual_path": str(actual_path_path),
            "regenerated_2274_artifacts": clean_for_yaml(dict(performance_paths)),
            "regenerated_2275_artifacts": clean_for_yaml(
                dict(mapping(gate_policy_v2.get("artifact_paths")))
            ),
        },
        "summary": summary,
        "conclusion": conclusion,
        "research_only": True,
        "active_policy_change_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = {
        **common,
        "selection_modes": list(SELECTION_MODES),
        "active_selection_rule": clean_for_yaml(dict(active_selection_rule)),
        "candidate_assessments": candidate_assessments,
        "mode_rows": mode_rows,
        "counterfactual": counterfactual,
        "threshold_sensitivity": threshold_sensitivity,
        "recommended_policy": _recommended_policy(conclusion),
        "safety_boundary": _safety_boundary(),
    }

    paths = {
        "active_selection_rule_audit_report": docs_root / "active_selection_rule_audit_report.md",
        "active_selection_ablation_matrix": output_root / "active_selection_ablation_matrix.json",
        "active_selection_counterfactual_report": output_root
        / "active_selection_counterfactual_report.json",
        "active_selection_threshold_sensitivity": output_root
        / "active_selection_threshold_sensitivity.json",
        "active_selection_recommended_policy": output_root
        / "active_selection_recommended_policy.yaml",
    }
    write_json(
        paths["active_selection_ablation_matrix"],
        {**common, "mode_rows": mode_rows, "candidate_assessments": candidate_assessments},
    )
    write_json(
        paths["active_selection_counterfactual_report"],
        {**common, **counterfactual},
    )
    write_json(
        paths["active_selection_threshold_sensitivity"],
        {**common, **threshold_sensitivity},
    )
    write_yaml(
        paths["active_selection_recommended_policy"],
        {**common, **_recommended_policy(conclusion)},
    )
    write_markdown(
        paths["active_selection_rule_audit_report"],
        _render_report(payload, paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _candidate_assessment(
    *,
    row: Mapping[str, Any],
    actual_path_row: Mapping[str, Any],
    v2_gate_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    failed_gates = strings(row.get("failed_current_gates"))
    blocked_reasons: list[str] = []
    owner_review_reasons: list[str] = []
    diagnostic_reasons: list[str] = []
    score_penalty_reasons: list[str] = []
    for gate_id in failed_gates:
        if gate_id == "diagnostic_only_policy":
            blocked_reasons.append(gate_id)
            continue
        gate = mapping(v2_gate_rows.get(gate_id))
        failure_action = str(gate.get("failure_action"))
        if failure_action == "BLOCKED":
            blocked_reasons.append(gate_id)
        elif failure_action == "OWNER_REVIEW_REQUIRED":
            owner_review_reasons.append(gate_id)
        elif failure_action == "OWNER_REVIEW_REQUIRED_OR_BLOCKED_BY_SEVERITY":
            if _severe_slice_regression(row):
                blocked_reasons.append(gate_id)
            else:
                owner_review_reasons.append(gate_id)
        elif failure_action in {"DIAGNOSTIC_ONLY", "THRESHOLD_SENSITIVITY_ONLY"}:
            diagnostic_reasons.append(gate_id)
        elif failure_action == "SCORE_PENALTY":
            score_penalty_reasons.append(gate_id)
        else:
            owner_review_reasons.append(gate_id)
    if blocked_reasons:
        gate_policy_v2_state = "BLOCKED"
    elif owner_review_reasons:
        gate_policy_v2_state = "OWNER_REVIEW_REQUIRED"
    else:
        gate_policy_v2_state = "ACCEPTED"
    return {
        "policy_id": row.get("policy_id"),
        "gate_policy_v2_state": gate_policy_v2_state,
        "gate_policy_v2_blocked_reasons": blocked_reasons,
        "gate_policy_v2_owner_review_reasons": owner_review_reasons,
        "gate_policy_v2_diagnostic_reasons": diagnostic_reasons,
        "gate_policy_v2_score_penalty_reasons": score_penalty_reasons,
        "failed_current_gates": failed_gates,
        "first_prediction_date": row.get("first_prediction_date"),
        "does_coverage_pass_rule": bool(actual_path_row.get("does_coverage_pass_rule")),
        "covered_2022": bool(row.get("covered_2022")),
        "covered_2022_risk_off_window": bool(row.get("covered_2022_risk_off_window")),
        "covered_2022_recovery_window": bool(row.get("covered_2022_recovery_window")),
        "actual_path_improved_probe_count": row.get("actual_path_improved_probe_count"),
        "no_major_regression_in_defensive_probe": bool(
            mapping(row.get("current_gate_status")).get("no_major_regression_in_defensive_probe")
        ),
        "net_of_cost_not_worse": bool(
            mapping(row.get("current_gate_status")).get("net_of_cost_not_worse")
        ),
        "2022_slice_not_worse_than_flat_reference": bool(
            mapping(row.get("current_gate_status")).get("2022_slice_not_worse_than_flat_reference")
        ),
        "same_risk_comparison_reported": bool(actual_path_row.get("same_risk_comparison_reported")),
        "actual_path_utility_proxy": row.get("actual_path_utility_proxy"),
        "metric_snapshot": _candidate_metric_snapshot(row),
    }


def _selection_mode_row(
    *,
    mode: str,
    candidate_assessments: Sequence[Mapping[str, Any]],
    active_selection_rule: Mapping[str, Any],
) -> dict[str, Any]:
    decisions = [
        _candidate_mode_decision(
            candidate=candidate,
            mode=mode,
            active_selection_rule=active_selection_rule,
        )
        for candidate in candidate_assessments
    ]
    accepted = [row for row in decisions if row["selection_state"] == "ACCEPTED"]
    owner_review = [row for row in decisions if row["selection_state"] == "OWNER_REVIEW_REQUIRED"]
    blocked = [row for row in decisions if row["selection_state"] == "BLOCKED"]
    rejected = [row for row in decisions if row["selection_state"] != "ACCEPTED"]
    accepted_candidates = _candidate_subset(candidate_assessments, accepted)
    current_baseline: list[Mapping[str, Any]] = []
    metric_deltas = _metric_deltas(accepted_candidates, current_baseline)
    return {
        "mode": mode,
        "selection_threshold_profile": _selection_threshold_profile(mode),
        "accepted_policy_ids": [str(row["policy_id"]) for row in accepted],
        "owner_review_required_policy_ids": [str(row["policy_id"]) for row in owner_review],
        "blocked_policy_ids": [str(row["policy_id"]) for row in blocked],
        "rejected_policy_ids": [str(row["policy_id"]) for row in rejected],
        "accepted_candidate_count": len(accepted),
        "owner_review_required_count": len(owner_review),
        "blocked_candidate_count": len(blocked),
        "rejected_candidate_count": len(rejected),
        "best_accepted_candidate_utility": _best_utility(accepted),
        "best_rejected_candidate_utility": _best_utility(rejected),
        "rejected_candidate_counterfactual_utility": _average_utility(rejected),
        **metric_deltas,
        "candidate_decisions": decisions,
    }


def _candidate_mode_decision(
    *,
    candidate: Mapping[str, Any],
    mode: str,
    active_selection_rule: Mapping[str, Any],
) -> dict[str, Any]:
    gate_state = str(candidate.get("gate_policy_v2_state"))
    reasons: list[str] = []
    suppressed_owner_review = False
    if mode in {"no_active_selection", "relaxed_active_selection"}:
        if gate_state == "BLOCKED":
            state = "BLOCKED"
            reasons.extend(strings(candidate.get("gate_policy_v2_blocked_reasons")))
        elif gate_state == "OWNER_REVIEW_REQUIRED":
            state = "OWNER_REVIEW_REQUIRED"
            reasons.extend(strings(candidate.get("gate_policy_v2_owner_review_reasons")))
        elif mode == "relaxed_active_selection" and not bool(candidate.get("covered_2022")):
            state = "OWNER_REVIEW_REQUIRED"
            reasons.append("relaxed_selection_2022_coverage_review")
        else:
            state = "ACCEPTED"
    elif mode == "current_active_selection":
        passes_current, current_reasons = _passes_current_selection(
            candidate,
            active_selection_rule,
        )
        if gate_state == "OWNER_REVIEW_REQUIRED":
            suppressed_owner_review = True
            state = "BLOCKED"
            reasons.extend(strings(candidate.get("gate_policy_v2_owner_review_reasons")))
            reasons.append("owner_review_required_treated_as_blocked_by_current_selection")
        elif gate_state == "BLOCKED":
            state = "BLOCKED"
            reasons.extend(strings(candidate.get("gate_policy_v2_blocked_reasons")))
        elif not passes_current:
            state = "BLOCKED"
            reasons.extend(current_reasons)
        else:
            state = "ACCEPTED"
    elif mode == "strict_active_selection":
        passes_current, current_reasons = _passes_current_selection(
            candidate,
            active_selection_rule,
        )
        strict_reasons = list(current_reasons)
        if int(candidate.get("actual_path_improved_probe_count") or 0) < 8:
            strict_reasons.append("strict_requires_all_eight_probe_improvements")
        if gate_state == "OWNER_REVIEW_REQUIRED":
            suppressed_owner_review = True
            state = "BLOCKED"
            reasons.extend(strings(candidate.get("gate_policy_v2_owner_review_reasons")))
            reasons.append("owner_review_required_treated_as_blocked_by_strict_selection")
        elif gate_state == "BLOCKED":
            state = "BLOCKED"
            reasons.extend(strings(candidate.get("gate_policy_v2_blocked_reasons")))
        elif strict_reasons or not passes_current:
            state = "BLOCKED"
            reasons.extend(strict_reasons)
        else:
            state = "ACCEPTED"
    else:
        raise ValueError(f"Unknown active selection mode: {mode}")
    return {
        "policy_id": candidate.get("policy_id"),
        "selection_state": state,
        "gate_policy_v2_state": gate_state,
        "selection_reasons": list(dict.fromkeys(reasons)),
        "owner_review_suppressed_by_selection": suppressed_owner_review,
        "actual_path_utility_proxy": candidate.get("actual_path_utility_proxy"),
        "metric_snapshot": candidate.get("metric_snapshot"),
    }


def _passes_current_selection(
    candidate: Mapping[str, Any],
    active_selection_rule: Mapping[str, Any],
) -> tuple[bool, list[str]]:
    conditions = mapping(active_selection_rule.get("selection_conditions"))
    reasons: list[str] = []
    if bool(conditions.get("coverage_pass_rule_satisfied")) and not bool(
        candidate.get("does_coverage_pass_rule")
    ):
        reasons.append("coverage_pass_rule_satisfied_failed")
    min_improved = int(conditions.get("actual_path_improved_probe_count_min") or 0)
    if int(candidate.get("actual_path_improved_probe_count") or 0) < min_improved:
        reasons.append("actual_path_improved_probe_count_below_current_min")
    for field in (
        "no_major_regression_in_defensive_probe",
        "net_of_cost_not_worse",
        "2022_slice_not_worse_than_flat_reference",
        "same_risk_comparison_reported",
    ):
        if bool(conditions.get(field)) and not bool(candidate.get(field)):
            reasons.append(f"{field}_failed")
    return not reasons, reasons


def _counterfactual_report(
    *,
    candidate_assessments: Sequence[Mapping[str, Any]],
    mode_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    current = _mode_row(mode_rows, "current_active_selection")
    best_candidate = max(
        candidate_assessments,
        key=lambda row: to_float(row.get("actual_path_utility_proxy")),
    )
    current_decisions = {
        str(row.get("policy_id")): row for row in records(current.get("candidate_decisions"))
    }
    suppressed = [
        row
        for row in records(current.get("candidate_decisions"))
        if bool(row.get("owner_review_suppressed_by_selection"))
    ]
    return {
        "best_actual_path_candidate_policy_id": best_candidate.get("policy_id"),
        "best_actual_path_candidate_utility": best_candidate.get("actual_path_utility_proxy"),
        "best_candidate_current_selection_state": mapping(
            current_decisions.get(str(best_candidate.get("policy_id")))
        ).get("selection_state"),
        "active_selection_blocks_best_candidate": mapping(
            current_decisions.get(str(best_candidate.get("policy_id")))
        ).get("selection_state")
        != "ACCEPTED",
        "owner_review_candidates_suppressed_by_selection": len(suppressed),
        "suppressed_owner_review_policy_ids": [str(row.get("policy_id")) for row in suppressed],
        "candidate_counterfactual_rows": [
            {
                "policy_id": candidate.get("policy_id"),
                "gate_policy_v2_state": candidate.get("gate_policy_v2_state"),
                "actual_path_utility_proxy": candidate.get("actual_path_utility_proxy"),
                "current_selection_state": mapping(
                    current_decisions.get(str(candidate.get("policy_id")))
                ).get("selection_state"),
                "current_selection_reasons": strings(
                    mapping(current_decisions.get(str(candidate.get("policy_id")))).get(
                        "selection_reasons"
                    )
                ),
            }
            for candidate in candidate_assessments
        ],
    }


def _threshold_sensitivity(
    *,
    candidate_assessments: Sequence[Mapping[str, Any]],
    mode_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    rows = []
    previous_count: int | None = None
    previous_best: float | None = None
    for row in mode_rows:
        best_accepted = row.get("best_accepted_candidate_utility")
        accepted_count = int(row.get("accepted_candidate_count") or 0)
        rows.append(
            {
                "mode": row.get("mode"),
                "selection_threshold_profile": row.get("selection_threshold_profile"),
                "accepted_candidate_count": accepted_count,
                "owner_review_required_count": row.get("owner_review_required_count"),
                "best_accepted_candidate_utility": best_accepted,
                "accepted_count_jump_vs_previous_mode": None
                if previous_count is None
                else accepted_count - previous_count,
                "best_utility_jump_vs_previous_mode": None
                if previous_best is None or best_accepted is None
                else round_float(to_float(best_accepted) - previous_best),
            }
        )
        previous_count = accepted_count
        previous_best = None if best_accepted is None else to_float(best_accepted)
    boundary_candidates = [
        {
            "policy_id": row.get("policy_id"),
            "actual_path_utility_proxy": row.get("actual_path_utility_proxy"),
            "boundary_reason": _boundary_reason(row),
            "does_coverage_pass_rule": row.get("does_coverage_pass_rule"),
            "covered_2022": row.get("covered_2022"),
            "gate_policy_v2_state": row.get("gate_policy_v2_state"),
        }
        for row in candidate_assessments
        if _boundary_reason(row)
    ]
    return {
        "threshold_sensitivity_rows": rows,
        "boundary_candidate_rows": boundary_candidates,
        "threshold_stability": "unstable_boundary_jump_to_zero_at_current_selection",
    }


def _conclusion(
    *,
    candidate_assessments: Sequence[Mapping[str, Any]],
    mode_rows: Sequence[Mapping[str, Any]],
    current_row: Mapping[str, Any],
) -> dict[str, Any]:
    counterfactual = _counterfactual_report(
        candidate_assessments=candidate_assessments,
        mode_rows=mode_rows,
    )
    conflicts = int(counterfactual["owner_review_candidates_suppressed_by_selection"]) > 0
    blocks_best = bool(counterfactual["active_selection_blocks_best_candidate"])
    if blocks_best and conflicts and int(current_row.get("accepted_candidate_count") or 0) == 0:
        marginal_utility = "negative"
        action = "split_selection_and_promotion"
    else:
        marginal_utility = "inconclusive"
        action = "inconclusive_more_candidates_needed"
    return {
        "active_selection_marginal_utility": marginal_utility,
        "active_selection_blocks_best_candidate": blocks_best,
        "active_selection_conflicts_with_gate_policy_v2": conflicts,
        "owner_review_candidates_suppressed_by_selection": counterfactual[
            "owner_review_candidates_suppressed_by_selection"
        ],
        "recommended_action": action,
        "supporting_actions": [
            "convert_to_owner_review_queue",
            "relax_threshold",
            "convert_to_ranked_review",
        ],
    }


def _summary(
    *,
    performance_audit: Mapping[str, Any],
    gate_policy_v2: Mapping[str, Any],
    mode_rows: Sequence[Mapping[str, Any]],
    conclusion: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "source_task_ids": [
            mapping(performance_audit.get("summary")).get("task_id"),
            gate_policy_v2.get("task_id"),
        ],
        "candidate_count_before_active_selection": mapping(performance_audit.get("summary")).get(
            "candidate_count_before_gate"
        ),
        "candidate_count_after_current_performance_gates": mapping(
            performance_audit.get("summary")
        ).get("candidate_count_after_current_performance_gates"),
        "current_active_selection_accept_count": _mode_row(
            mode_rows,
            "current_active_selection",
        ).get("accepted_candidate_count"),
        "selection_modes": list(SELECTION_MODES),
        "active_selection_marginal_utility": conclusion.get("active_selection_marginal_utility"),
        "recommended_action": conclusion.get("recommended_action"),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _recommended_policy(conclusion: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": "recommended_first_layer_active_selection_policy_v1",
        "recommended_action": conclusion.get("recommended_action"),
        "allowed_recommended_actions": sorted(RECOMMENDED_ACTIONS),
        "active_selection_marginal_utility": conclusion.get("active_selection_marginal_utility"),
        "active_selection_blocks_best_candidate": conclusion.get(
            "active_selection_blocks_best_candidate"
        ),
        "active_selection_conflicts_with_gate_policy_v2": conclusion.get(
            "active_selection_conflicts_with_gate_policy_v2"
        ),
        "owner_review_candidates_suppressed_by_selection": conclusion.get(
            "owner_review_candidates_suppressed_by_selection"
        ),
        "recommended_semantics": {
            "selection_state_outputs": [
                "BLOCKED",
                "OWNER_REVIEW_REQUIRED",
                "OFFLINE_VALIDATION_READY",
                "PROMOTION_READY_FALSE",
            ],
            "owner_review_required_is_not_blocked": True,
            "active_selection_allowed_to_route_owner_review": True,
            "active_selection_allowed_to_promote": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
    }


def _metric_deltas(
    accepted_candidates: Sequence[Mapping[str, Any]],
    baseline_candidates: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    deltas: dict[str, Any] = {}
    for output_field, metric_field in METRIC_FIELD_MAP.items():
        accepted_value = _average_candidate_metric(accepted_candidates, metric_field)
        baseline_value = _average_candidate_metric(baseline_candidates, metric_field)
        deltas[output_field] = None
        if accepted_value is not None and baseline_value is not None:
            deltas[output_field] = round_float(accepted_value - baseline_value)
        elif accepted_value is not None and not baseline_candidates:
            deltas[output_field] = round_float(accepted_value)
    return deltas


def _candidate_metric_snapshot(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        output_field: row.get(metric_field)
        for output_field, metric_field in METRIC_FIELD_MAP.items()
    }


def _candidate_subset(
    candidates: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    ids = {str(row.get("policy_id")) for row in decisions}
    return [row for row in candidates if str(row.get("policy_id")) in ids]


def _average_candidate_metric(
    candidates: Sequence[Mapping[str, Any]],
    field: str,
) -> float | None:
    values = [
        to_float(mapping(row.get("metric_snapshot")).get(_output_field(field)))
        for row in candidates
        if mapping(row.get("metric_snapshot")).get(_output_field(field)) is not None
    ]
    if not values:
        return None
    return sum(values) / len(values)


def _output_field(metric_field: str) -> str:
    for output_field, source_field in METRIC_FIELD_MAP.items():
        if source_field == metric_field:
            return output_field
    return metric_field


def _best_utility(rows: Sequence[Mapping[str, Any]]) -> float | None:
    values = [
        to_float(row.get("actual_path_utility_proxy"))
        for row in rows
        if row.get("actual_path_utility_proxy") is not None
    ]
    if not values:
        return None
    return round_float(max(values))


def _average_utility(rows: Sequence[Mapping[str, Any]]) -> float | None:
    values = [
        to_float(row.get("actual_path_utility_proxy"))
        for row in rows
        if row.get("actual_path_utility_proxy") is not None
    ]
    if not values:
        return None
    return round_float(sum(values) / len(values))


def _mode_row(
    rows: Sequence[Mapping[str, Any]],
    mode: str,
) -> Mapping[str, Any]:
    for row in rows:
        if row.get("mode") == mode:
            return row
    raise ValueError(f"Missing active selection mode row: {mode}")


def _selection_threshold_profile(mode: str) -> dict[str, Any]:
    profiles = {
        "no_active_selection": {
            "coverage_requirement": "none_beyond_gate_policy_v2_state",
            "owner_review_queue_allowed": True,
        },
        "relaxed_active_selection": {
            "coverage_requirement": "covered_2022_or_owner_review_queue",
            "owner_review_queue_allowed": True,
        },
        "current_active_selection": {
            "coverage_requirement": "does_coverage_pass_rule",
            "owner_review_queue_allowed": False,
        },
        "strict_active_selection": {
            "coverage_requirement": "does_coverage_pass_rule_plus_all_probe_improvement",
            "owner_review_queue_allowed": False,
        },
    }
    return profiles[mode]


def _boundary_reason(row: Mapping[str, Any]) -> str:
    if row.get("gate_policy_v2_state") == "OWNER_REVIEW_REQUIRED":
        return "owner_review_state_suppressed_by_current_selection_boundary"
    if bool(row.get("covered_2022")) and not bool(row.get("does_coverage_pass_rule")):
        return "covered_2022_but_current_coverage_pass_rule_false"
    return ""


def _severe_slice_regression(row: Mapping[str, Any]) -> bool:
    stress_delta = row.get("stress_2022_slice_delta")
    return stress_delta is not None and to_float(stress_delta) < -0.05


def _safety_boundary() -> dict[str, Any]:
    return {
        "active_selection_release_is_not_promotion": True,
        "owner_review_required_is_not_paper_shadow": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_report(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    summary = mapping(payload.get("summary"))
    conclusion = mapping(payload.get("conclusion"))
    source_generation = mapping(payload.get("source_generation"))
    lines = [
        "# First-Layer Active Selection Rule Audit",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        (
            "- current active selection accept count: "
            f"`{summary.get('current_active_selection_accept_count')}`"
        ),
        (
            "- active_selection_marginal_utility: "
            f"`{conclusion.get('active_selection_marginal_utility')}`; "
            f"recommended_action: `{conclusion.get('recommended_action')}`"
        ),
        (
            "- promotion_allowed=`false`, paper_shadow_allowed=`false`, "
            "production_allowed=`false`, broker_action=`none`."
        ),
        (
            "- gate policy v2 evidence source: "
            f"`{source_generation.get('recommended_gate_policy_v2_source')}`；"
            "ignored `outputs/` artifact 不是唯一 source of truth。"
        ),
        "",
        "## Ablation Matrix",
        "",
        ("| mode | accepted | owner_review | blocked | rejected | best_accepted | best_rejected |"),
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in records(payload.get("mode_rows")):
        lines.append(
            f"|`{row['mode']}`|{row['accepted_candidate_count']}|"
            f"{row['owner_review_required_count']}|{row['blocked_candidate_count']}|"
            f"{row['rejected_candidate_count']}|"
            f"{row.get('best_accepted_candidate_utility')}|"
            f"{row.get('best_rejected_candidate_utility')}|"
        )
    lines.extend(
        [
            "",
            "## Boundary Candidates",
            "",
            "| policy_id | utility | gate_policy_v2_state | boundary_reason |",
            "|---|---:|---|---|",
        ]
    )
    for row in records(
        mapping(payload.get("threshold_sensitivity")).get("boundary_candidate_rows")
    ):
        lines.append(
            f"|`{row['policy_id']}`|{row.get('actual_path_utility_proxy')}|"
            f"`{row.get('gate_policy_v2_state')}`|`{row.get('boundary_reason')}`|"
        )
    lines.extend(
        [
            "",
            "## 关键结论",
            "",
            (
                "- current selection 阻断 best actual-path candidate: "
                f"`{conclusion.get('active_selection_blocks_best_candidate')}`。"
            ),
            (
                "- 与 gate policy v2 分层语义冲突: "
                f"`{conclusion.get('active_selection_conflicts_with_gate_policy_v2')}`。"
            ),
            (
                "- owner-review candidates suppressed by selection: "
                f"`{conclusion.get('owner_review_candidates_suppressed_by_selection')}`。"
            ),
            "",
            "## 产物",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)
