from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_defensive_regression_diagnosis import (
    DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_2022_SLICE_YAML_PATH,
    DEFAULT_ACTUAL_PATH_YAML_PATH,
    DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
)
from ai_trading_system.first_layer_walk_forward_coverage import (
    DEFAULT_FINAL_MATRIX_YAML_PATH as DEFAULT_COVERAGE_FINAL_MATRIX_YAML_PATH,
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

DEFAULT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "first_layer_performance_gate_audit_policy.yaml"
)
DEFAULT_CURRENT_STATE_SUMMARY_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_current_state_summary.yaml"
)
DEFAULT_FAILURE_TAXONOMY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "research_trends"
    / "first_layer_current_state"
    / "first_layer_failure_taxonomy.json"
)
DEFAULT_OBJECTIVE_VALIDATION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_objective_validation_redesign.yaml"
)
DEFAULT_CHALLENGER_MATRIX_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "first_layer_proxy_challenger_experiments.yaml"
)
DEFAULT_RETURN_SEEKING_2022_CONTRAST_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_2022_vs_2023_contrast.yaml"
)
DEFAULT_RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_PATH = (
    PROJECT_ROOT / "inputs" / "research_reviews" / "return_seeking_beta_tqqq_attribution.yaml"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_performance_gate_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_PERFORMANCE_GATE_AUDIT_READY_PROMOTION_BLOCKED"
GATE_MODES = ("no_gate", "relaxed_gate", "current_gate", "strict_gate")
DIAGNOSTIC_ONLY_POLICY_IDS = {"wf_warm_start_diagnostic"}
EVIDENCE_LIMITED_GATES = {
    "not_beta_dependency",
    "not_tqqq_dependency",
    "probability_threshold_0_55",
    "probability_threshold_0_60",
    "all_slices_not_worse",
}
RECOMMENDED_ACTIONS = {
    "keep_as_hard_gate",
    "keep_as_performance_gate",
    "relax_threshold",
    "tighten_threshold",
    "convert_to_owner_review",
    "convert_to_score_penalty",
    "remove_gate",
}
CORE_DELTA_FIELDS = (
    "net_return_delta",
    "max_drawdown_delta",
    "sharpe_delta",
    "calmar_delta",
    "turnover_delta",
    "cost_adjusted_return_delta",
    "false_risk_on_delta",
    "false_risk_off_delta",
    "drawdown_warning_lead_time_delta",
    "recovery_delay_delta",
    "stress_2022_slice_delta",
    "recovery_2023_plus_slice_delta",
    "benchmark_consistency_delta",
    "beta_dependency_delta",
    "tqqq_dependency_delta",
)


def run_first_layer_performance_gate_audit_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    current_state_summary_path: Path = DEFAULT_CURRENT_STATE_SUMMARY_PATH,
    failure_taxonomy_path: Path = DEFAULT_FAILURE_TAXONOMY_PATH,
    objective_validation_path: Path = DEFAULT_OBJECTIVE_VALIDATION_PATH,
    challenger_matrix_path: Path = DEFAULT_CHALLENGER_MATRIX_PATH,
    actual_path_path: Path = DEFAULT_ACTUAL_PATH_YAML_PATH,
    coverage_simulation_path: Path = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    slice_matrix_path: Path = DEFAULT_2022_SLICE_YAML_PATH,
    defensive_inventory_path: Path = DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
    selection_rule_path: Path = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    coverage_final_path: Path = DEFAULT_COVERAGE_FINAL_MATRIX_YAML_PATH,
    return_seeking_2022_contrast_path: Path = DEFAULT_RETURN_SEEKING_2022_CONTRAST_PATH,
    return_seeking_beta_tqqq_attribution_path: Path = (
        DEFAULT_RETURN_SEEKING_BETA_TQQQ_ATTRIBUTION_PATH
    ),
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    current_state_summary = load_mapping(current_state_summary_path)
    failure_taxonomy = load_mapping(failure_taxonomy_path)
    objective_validation = load_mapping(objective_validation_path)
    challenger_matrix = load_mapping(challenger_matrix_path)
    actual_path = load_mapping(actual_path_path)
    coverage_simulation = load_mapping(coverage_simulation_path)
    slice_matrix = load_mapping(slice_matrix_path)
    defensive_inventory = load_mapping(defensive_inventory_path)
    selection_rule = load_mapping(selection_rule_path)
    coverage_final = load_mapping(coverage_final_path)
    return_seeking_2022_contrast = load_mapping(return_seeking_2022_contrast_path)
    return_seeking_beta_tqqq_attribution = load_mapping(return_seeking_beta_tqqq_attribution_path)

    gate_ids = tuple(str(gate_id) for gate_id in mapping(policy.get("gate_modes")))
    dependency_evidence = _dependency_evidence(
        return_seeking_2022_contrast=return_seeking_2022_contrast,
        return_seeking_beta_tqqq_attribution=return_seeking_beta_tqqq_attribution,
    )
    objective_context = _objective_context(objective_validation)
    candidate_rows = _candidate_rows(
        policy=policy,
        actual_path=actual_path,
        coverage_simulation=coverage_simulation,
        slice_matrix=slice_matrix,
        defensive_inventory=defensive_inventory,
        selection_rule=selection_rule,
        objective_context=objective_context,
        dependency_evidence=dependency_evidence,
    )
    challenger_rows = _challenger_rows(challenger_matrix)
    current_accept_ids = _accepted_policy_ids(
        candidate_rows,
        gate_ids=gate_ids,
        replacement_gate_id=None,
        replacement_mode="current_gate",
    )
    ablation_rows = _ablation_rows(candidate_rows, gate_ids, current_accept_ids)
    gate_rows = _gate_rows(
        policy=policy,
        candidate_rows=candidate_rows,
        ablation_rows=ablation_rows,
        gate_ids=gate_ids,
        current_accept_ids=current_accept_ids,
    )
    rejected_rows = _rejected_candidate_rows(
        candidate_rows=candidate_rows,
        gate_ids=gate_ids,
        current_accept_ids=current_accept_ids,
    )
    input_artifacts = _input_artifacts(
        policy_path=policy_path,
        current_state_summary_path=current_state_summary_path,
        failure_taxonomy_path=failure_taxonomy_path,
        objective_validation_path=objective_validation_path,
        challenger_matrix_path=challenger_matrix_path,
        actual_path_path=actual_path_path,
        coverage_simulation_path=coverage_simulation_path,
        slice_matrix_path=slice_matrix_path,
        defensive_inventory_path=defensive_inventory_path,
        selection_rule_path=selection_rule_path,
        coverage_final_path=coverage_final_path,
        return_seeking_2022_contrast_path=return_seeking_2022_contrast_path,
        return_seeking_beta_tqqq_attribution_path=return_seeking_beta_tqqq_attribution_path,
    )
    summary = _summary(
        actual_path=actual_path,
        current_state_summary=current_state_summary,
        failure_taxonomy=failure_taxonomy,
        objective_validation=objective_validation,
        challenger_rows=challenger_rows,
        coverage_final=coverage_final,
        candidate_rows=candidate_rows,
        gate_rows=gate_rows,
        current_accept_ids=current_accept_ids,
        dependency_evidence=dependency_evidence,
    )
    common = _common_payload(
        actual_path=actual_path,
        current_state_summary=current_state_summary,
        policy=policy,
        summary=summary,
        input_artifacts=input_artifacts,
    )
    evidence_limitations = _evidence_limitations(
        objective_context=objective_context,
        dependency_evidence=dependency_evidence,
        challenger_rows=challenger_rows,
    )
    paths = {
        "gate_ablation_matrix": output_root / "gate_ablation_matrix.json",
        "threshold_sensitivity_report": output_root / "threshold_sensitivity_report.json",
        "rejected_candidate_counterfactual_report": output_root
        / "rejected_candidate_counterfactual_report.json",
        "recommended_gate_policy": output_root / "recommended_gate_policy.yaml",
        "gate_acceptance_audit_report": docs_root / "gate_acceptance_audit_report.md",
    }
    write_json(
        paths["gate_ablation_matrix"],
        {
            **common,
            "candidate_rows": candidate_rows,
            "challenger_rows": challenger_rows,
            "gate_rows": gate_rows,
            "ablation_rows": ablation_rows,
            "evidence_limitations": evidence_limitations,
        },
    )
    write_json(
        paths["threshold_sensitivity_report"],
        {
            **common,
            "actual_path_utility_proxy": mapping(policy.get("actual_path_utility_proxy")),
            "marginal_utility_thresholds": mapping(policy.get("marginal_utility_thresholds")),
            "allowed_recommended_actions": sorted(RECOMMENDED_ACTIONS),
            "gate_rows": gate_rows,
            "mode_rows": ablation_rows,
            "evidence_limitations": evidence_limitations,
        },
    )
    write_json(
        paths["rejected_candidate_counterfactual_report"],
        {
            **common,
            "candidate_rows": candidate_rows,
            "challenger_rows": challenger_rows,
            "rejected_candidate_rows": rejected_rows,
            "objective_context": objective_context,
            "dependency_evidence": dependency_evidence,
            "evidence_limitations": evidence_limitations,
        },
    )
    write_yaml(
        paths["recommended_gate_policy"],
        _recommended_policy(common=common, policy=policy, gate_rows=gate_rows),
    )
    write_markdown(
        paths["gate_acceptance_audit_report"],
        _render_report(
            summary,
            gate_rows,
            rejected_rows,
            candidate_rows,
            challenger_rows,
            evidence_limitations,
            paths,
        ),
    )
    return clean_for_yaml(
        {
            **common,
            "gate_rows": gate_rows,
            "ablation_rows": ablation_rows,
            "rejected_candidate_rows": rejected_rows,
            "candidate_rows": candidate_rows,
            "challenger_rows": challenger_rows,
            "evidence_limitations": evidence_limitations,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _candidate_rows(
    *,
    policy: Mapping[str, Any],
    actual_path: Mapping[str, Any],
    coverage_simulation: Mapping[str, Any],
    slice_matrix: Mapping[str, Any],
    defensive_inventory: Mapping[str, Any],
    selection_rule: Mapping[str, Any],
    objective_context: Mapping[str, Any],
    dependency_evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    actual_rows = _by_policy(records(actual_path.get("policy_rows")))
    coverage_rows = _by_policy(records(coverage_simulation.get("policy_rows")))
    slice_rows = _by_policy(records(slice_matrix.get("policy_rows")))
    probe_rows = _probe_rows_by_policy(actual_path)
    defensive_counts = _defensive_regression_counts(defensive_inventory)
    defensive_metrics = _defensive_metrics_by_policy(defensive_inventory)
    current_min_improved = _to_int(
        mapping(selection_rule.get("selection_conditions")).get(
            "actual_path_improved_probe_count_min"
        ),
        2,
    )
    utility_policy = mapping(policy.get("actual_path_utility_proxy"))
    return_weight = to_float(utility_policy.get("return_weight"), 1.0)
    drawdown_weight = to_float(utility_policy.get("drawdown_penalty_weight"), 0.75)
    turnover_weight = to_float(utility_policy.get("turnover_penalty_weight"), 0.00025)

    rows = []
    for policy_id, actual in actual_rows.items():
        coverage = mapping(coverage_rows.get(policy_id))
        slice_row = mapping(slice_rows.get(policy_id))
        probes = probe_rows.get(policy_id, [])
        defensive = mapping(defensive_metrics.get(policy_id))
        avg_return = _mean(to_float(row.get("v2_annual_return")) for row in probes)
        avg_abs_drawdown = _mean(abs(to_float(row.get("v2_max_drawdown"))) for row in probes)
        avg_sharpe = _mean(to_float(row.get("v2_sharpe")) for row in probes)
        avg_calmar = _mean(to_float(row.get("v2_calmar")) for row in probes)
        avg_turnover = _mean(to_float(row.get("v2_turnover")) for row in probes)
        utility = (
            avg_return * return_weight
            - avg_abs_drawdown * drawdown_weight
            - avg_turnover * turnover_weight
        )
        first_prediction_date = actual.get(
            "first_prediction_date", coverage.get("first_prediction_date")
        )
        prediction_count_2022 = _to_int(slice_row.get("prediction_count_2022"))
        slice_2022_ok = bool(
            slice_row.get(
                "2022_slice_not_worse_than_flat_reference",
                actual.get("2022_slice_not_worse_than_flat_reference"),
            )
        )
        not_2023_plus_only = prediction_count_2022 > 0 or _date_on_or_before(
            first_prediction_date, "2022-12-31"
        )
        net_return_delta = _optional_round(defensive.get("annual_return_delta"))
        turnover_delta = _optional_round(defensive.get("turnover_delta"))
        cost_adjusted_return_delta = None
        if net_return_delta is not None and turnover_delta is not None:
            cost_adjusted_return_delta = round_float(
                net_return_delta - turnover_delta * turnover_weight
            )
        gate_status = {
            "actual_path_improved_probe_count_min": _to_int(
                actual.get("actual_path_improved_probe_count")
            )
            >= current_min_improved,
            "no_major_regression_in_defensive_probe": bool(
                actual.get("no_major_regression_in_defensive_probe")
            ),
            "2022_slice_not_worse_than_flat_reference": slice_2022_ok,
            "net_of_cost_not_worse": bool(actual.get("net_of_cost_not_worse")),
            "not_2023_plus_only": not_2023_plus_only,
            "not_beta_dependency": True,
            "not_tqqq_dependency": True,
            "probability_threshold_0_55": True,
            "probability_threshold_0_60": True,
            "all_slices_not_worse": slice_2022_ok,
            "no_slice_regression": slice_2022_ok,
        }
        diagnostic_only = policy_id in DIAGNOSTIC_ONLY_POLICY_IDS
        failed = [gate for gate, passed in gate_status.items() if not passed]
        if diagnostic_only:
            failed.append("diagnostic_only_policy")
        rows.append(
            {
                "policy_id": policy_id,
                "candidate_type": "diagnostic_only" if diagnostic_only else "candidate",
                "candidate_source": "frozen_second_layer_actual_path_matrix",
                "actual_path_status": "candidate_level_actual_path_available",
                "diagnostic_only_exclusion": diagnostic_only,
                "first_prediction_date": first_prediction_date,
                "covered_2022": bool(coverage.get("covered_2022")),
                "covered_2022_risk_off_window": bool(coverage.get("covered_2022_risk_off_window")),
                "covered_2022_recovery_window": bool(coverage.get("covered_2022_recovery_window")),
                "prediction_count_2022": prediction_count_2022,
                "actual_path_improved_probe_count": _to_int(
                    actual.get("actual_path_improved_probe_count")
                ),
                "probe_count": _to_int(actual.get("probe_count"), len(probes)),
                "defensive_role_regression_count": defensive_counts.get(policy_id, 0),
                "average_probe_annual_return": round_float(avg_return),
                "average_abs_max_drawdown": round_float(avg_abs_drawdown),
                "average_probe_sharpe": round_float(avg_sharpe),
                "average_probe_calmar": round_float(avg_calmar),
                "average_probe_turnover": round_float(avg_turnover),
                "actual_path_utility_proxy": round_float(utility),
                "net_return_delta": net_return_delta,
                "max_drawdown_delta": _optional_round(defensive.get("max_drawdown_delta")),
                "sharpe_delta": _optional_round(defensive.get("sharpe_delta")),
                "calmar_delta": _optional_round(defensive.get("calmar_delta")),
                "turnover_delta": turnover_delta,
                "cost_adjusted_return_delta": cost_adjusted_return_delta,
                "false_risk_on_delta": None,
                "false_risk_off_delta": None,
                "drawdown_warning_lead_time_delta": None,
                "recovery_delay_delta": None,
                "stress_2022_slice_delta": _optional_round(
                    slice_row.get("average_return_delta_vs_warm_start_diagnostic")
                ),
                "recovery_2023_plus_slice_delta": None,
                "benchmark_consistency_delta": None,
                "beta_dependency_delta": None,
                "tqqq_dependency_delta": None,
                "candidate_level_objective_delta_status": (
                    "unavailable_for_frozen_actual_path_policy_rows"
                ),
                "candidate_level_dependency_delta_status": (
                    "unavailable_candidate_level_beta_tqqq_dependency_not_run"
                ),
                "candidate_level_probability_status": (
                    "unavailable_probability_distribution_not_in_actual_path_matrix"
                ),
                "all_slice_actual_path_status": objective_context.get(
                    "all_slice_actual_path_status"
                ),
                "return_seeking_dependency_evidence_scope": dependency_evidence.get(
                    "evidence_scope"
                ),
                "current_gate_status": gate_status,
                "current_gate_accept": not failed,
                "failed_current_gates": list(dict.fromkeys(failed)),
                "target_path_metrics_role": actual.get(
                    "target_path_metrics_role", "diagnostic_only"
                ),
                "broker_action": actual.get("broker_action", "none"),
                "production_effect": actual.get("production_effect", "none"),
            }
        )
    return sorted(
        rows,
        key=lambda row: (
            row["candidate_type"] == "diagnostic_only",
            -to_float(row["actual_path_utility_proxy"]),
            str(row["policy_id"]),
        ),
    )


def _challenger_rows(challenger_matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in records(challenger_matrix.get("experiments")):
        rows.append(
            {
                "experiment_id": row.get("experiment_id"),
                "candidate_source": "offline_validation_ready_challenger_matrix",
                "validation_ready": bool(row.get("validation_ready")),
                "validation_ready_scope": row.get("validation_ready_scope"),
                "actual_path_status": "unavailable_actual_path_not_run",
                "actual_path_utility_proxy": None,
                "target_objective_terms": strings(row.get("target_objective_terms")),
                "missing_proxy_ids": strings(row.get("missing_proxy_ids")),
                "promotion_allowed": bool(row.get("promotion_allowed")),
                "paper_shadow_allowed": bool(row.get("paper_shadow_allowed")),
                "production_allowed": bool(row.get("production_allowed")),
                "broker_action": row.get("broker_action", "none"),
                "promotion_blockers": strings(row.get("promotion_blockers")),
            }
        )
    return rows


def _ablation_rows(
    candidate_rows: Sequence[Mapping[str, Any]],
    gate_ids: Sequence[str],
    current_accept_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for gate_id in gate_ids:
        for mode in GATE_MODES:
            accepted_ids = _accepted_policy_ids(
                candidate_rows,
                gate_ids=gate_ids,
                replacement_gate_id=gate_id,
                replacement_mode=mode,
            )
            accepted_rows = [row for row in candidate_rows if str(row["policy_id"]) in accepted_ids]
            rows.append(
                {
                    "gate_id": gate_id,
                    "mode": mode,
                    "accepted_policy_ids": sorted(accepted_ids),
                    "accepted_candidate_count": len(accepted_ids),
                    "accepted_average_actual_path_utility_proxy": _average_metric(
                        accepted_rows,
                        "actual_path_utility_proxy",
                        default=0.0,
                    ),
                    "accepted_best_utility_proxy": round_float(
                        max(
                            (
                                to_float(row.get("actual_path_utility_proxy"))
                                for row in accepted_rows
                            ),
                            default=0.0,
                        )
                    ),
                    "accepted_metric_snapshot": _metric_snapshot(accepted_rows),
                    "newly_accepted_vs_current": sorted(accepted_ids - current_accept_ids),
                    "newly_rejected_vs_current": sorted(current_accept_ids - accepted_ids),
                    "candidate_decisions": [
                        {
                            "policy_id": row["policy_id"],
                            "accepted": str(row["policy_id"]) in accepted_ids,
                            "actual_path_utility_proxy": row["actual_path_utility_proxy"],
                            "failed_current_gates": strings(row.get("failed_current_gates")),
                        }
                        for row in candidate_rows
                        if not bool(row.get("diagnostic_only_exclusion"))
                    ],
                }
            )
    return rows


def _gate_rows(
    *,
    policy: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    ablation_rows: Sequence[Mapping[str, Any]],
    gate_ids: Sequence[str],
    current_accept_ids: set[str],
) -> list[dict[str, Any]]:
    thresholds = mapping(policy.get("marginal_utility_thresholds"))
    positive_min = to_float(thresholds.get("positive_delta_min"), 0.005)
    neutral_max = to_float(thresholds.get("neutral_abs_delta_max"), 0.005)
    material_cost_min = to_float(thresholds.get("material_opportunity_cost_min"), 0.01)
    modes = {(str(row["gate_id"]), str(row["mode"])): row for row in ablation_rows}
    candidates = {str(row["policy_id"]): row for row in candidate_rows}
    gate_policy = mapping(policy.get("gate_modes"))
    owner_overrides = mapping(policy.get("owner_decision_overrides"))
    rows = []
    for gate_id in gate_ids:
        no_gate_ids = set(strings(modes[(gate_id, "no_gate")].get("accepted_policy_ids")))
        current_ids = set(strings(modes[(gate_id, "current_gate")].get("accepted_policy_ids")))
        relaxed_ids = set(strings(modes[(gate_id, "relaxed_gate")].get("accepted_policy_ids")))
        strict_ids = set(strings(modes[(gate_id, "strict_gate")].get("accepted_policy_ids")))
        blocked_ids = sorted(no_gate_ids - current_ids)
        no_gate_rows = [candidates[policy_id] for policy_id in sorted(no_gate_ids)]
        current_rows = [candidates[policy_id] for policy_id in sorted(current_ids)]
        blocked_rows = [candidates[policy_id] for policy_id in blocked_ids]
        blocked_utilities = [to_float(row.get("actual_path_utility_proxy")) for row in blocked_rows]
        blocked_mean = _mean(blocked_utilities)
        no_gate_avg = _average_metric(
            no_gate_rows,
            "actual_path_utility_proxy",
            default=0.0,
        )
        current_avg = _average_metric(
            current_rows,
            "actual_path_utility_proxy",
            default=0.0,
        )
        opportunity_cost = max(blocked_utilities, default=0.0)
        best_blocked = ""
        if blocked_rows:
            best_blocked = str(
                max(
                    blocked_rows,
                    key=lambda row: to_float(row.get("actual_path_utility_proxy")),
                ).get("policy_id")
            )
        utility = _classify_utility(
            gate_id=gate_id,
            blocked_mean=blocked_mean,
            blocked_count=len(blocked_rows),
            positive_min=positive_min,
            neutral_max=neutral_max,
        )
        material_cost = opportunity_cost >= material_cost_min
        metric_deltas = {
            field: _metric_delta(current_rows, no_gate_rows, field) for field in CORE_DELTA_FIELDS
        }
        threshold_sensitivity = {
            mode: {
                "candidate_count_after_gate": len(
                    strings(modes[(gate_id, mode)].get("accepted_policy_ids"))
                ),
                "accepted_policy_ids": strings(modes[(gate_id, mode)].get("accepted_policy_ids")),
                "accepted_average_actual_path_utility_proxy": modes[(gate_id, mode)].get(
                    "accepted_average_actual_path_utility_proxy"
                ),
            }
            for mode in GATE_MODES
        }
        threshold_stability = _threshold_stability(
            gate_id=gate_id,
            threshold_sensitivity=threshold_sensitivity,
            utility=utility,
            material_cost=material_cost,
        )
        owner_override = mapping(owner_overrides.get(gate_id))
        rows.append(
            {
                "gate_id": gate_id,
                "candidate_count_before_gate": len(no_gate_ids),
                "candidate_count_after_gate": len(current_ids),
                "rejected_candidate_count": len(blocked_ids),
                "accepted_candidate_actual_path_utility": current_avg,
                "rejected_candidate_counterfactual_utility": round_float(blocked_mean),
                "marginal_utility_contribution": _utility_delta(current_avg, no_gate_avg),
                "gate_marginal_utility": utility,
                "gate_failure_mode_reduced": mapping(gate_policy.get(gate_id)).get(
                    "failure_mode_reduced", ""
                ),
                "opportunity_cost": round_float(opportunity_cost),
                "best_blocked_policy_id": best_blocked,
                "blocked_policy_ids_vs_no_gate": blocked_ids,
                "blocked_average_actual_path_utility_proxy": round_float(blocked_mean),
                "accepted_current_policy_ids": sorted(current_ids),
                "accepted_no_gate_policy_ids": sorted(no_gate_ids),
                "accepted_relaxed_gate_policy_ids": sorted(relaxed_ids),
                "accepted_strict_gate_policy_ids": sorted(strict_ids),
                "material_opportunity_cost": material_cost,
                "threshold_stability": threshold_stability,
                "threshold_sensitivity": threshold_sensitivity,
                "evidence_status": _gate_evidence_status(gate_id),
                "objective_delta_status": _objective_delta_status(gate_id),
                "dependency_delta_status": _dependency_delta_status(gate_id),
                "recommended_action": _recommended_action(
                    gate_id=gate_id,
                    utility=utility,
                    material_cost=material_cost,
                    threshold_stability=threshold_stability,
                    current_accept_count=len(current_accept_ids),
                    owner_override=owner_override,
                ),
                "owner_decision_override": owner_override,
                **metric_deltas,
            }
        )
    return rows


def _rejected_candidate_rows(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    gate_ids: Sequence[str],
    current_accept_ids: set[str],
) -> list[dict[str, Any]]:
    rows = []
    for row in candidate_rows:
        policy_id = str(row["policy_id"])
        if policy_id in current_accept_ids or bool(row.get("diagnostic_only_exclusion")):
            continue
        mode_acceptance = {}
        for gate_id in gate_ids:
            mode_acceptance[gate_id] = {
                mode: policy_id
                in _accepted_policy_ids(
                    candidate_rows,
                    gate_ids=gate_ids,
                    replacement_gate_id=gate_id,
                    replacement_mode=mode,
                )
                for mode in GATE_MODES
            }
        rows.append(
            {
                "policy_id": policy_id,
                "current_gate_accept": False,
                "failed_current_gates": strings(row.get("failed_current_gates")),
                "actual_path_utility_proxy": row.get("actual_path_utility_proxy"),
                "average_probe_annual_return": row.get("average_probe_annual_return"),
                "average_abs_max_drawdown": row.get("average_abs_max_drawdown"),
                "average_probe_sharpe": row.get("average_probe_sharpe"),
                "average_probe_calmar": row.get("average_probe_calmar"),
                "average_probe_turnover": row.get("average_probe_turnover"),
                "net_return_delta": row.get("net_return_delta"),
                "max_drawdown_delta": row.get("max_drawdown_delta"),
                "sharpe_delta": row.get("sharpe_delta"),
                "calmar_delta": row.get("calmar_delta"),
                "turnover_delta": row.get("turnover_delta"),
                "cost_adjusted_return_delta": row.get("cost_adjusted_return_delta"),
                "stress_2022_slice_delta": row.get("stress_2022_slice_delta"),
                "acceptance_by_single_gate_counterfactual": mode_acceptance,
                "owner_review_candidate": _owner_review_candidate(row),
                "candidate_level_objective_delta_status": row.get(
                    "candidate_level_objective_delta_status"
                ),
                "candidate_level_dependency_delta_status": row.get(
                    "candidate_level_dependency_delta_status"
                ),
            }
        )
    return sorted(
        rows,
        key=lambda row: (-to_float(row["actual_path_utility_proxy"]), row["policy_id"]),
    )


def _accepted_policy_ids(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    gate_ids: Sequence[str],
    replacement_gate_id: str | None,
    replacement_mode: str,
) -> set[str]:
    accepted = set()
    for row in candidate_rows:
        if bool(row.get("diagnostic_only_exclusion")):
            continue
        if all(
            _gate_passes(
                row,
                gate_id,
                replacement_mode if gate_id == replacement_gate_id else "current_gate",
            )
            for gate_id in gate_ids
        ):
            accepted.add(str(row["policy_id"]))
    return accepted


def _gate_passes(row: Mapping[str, Any], gate_id: str, mode: str) -> bool:
    if mode == "no_gate":
        return True
    if gate_id == "actual_path_improved_probe_count_min":
        improved = _to_int(row.get("actual_path_improved_probe_count"))
        if mode == "relaxed_gate":
            return improved >= 1
        if mode == "strict_gate":
            return improved >= _to_int(row.get("probe_count"))
    if gate_id == "no_major_regression_in_defensive_probe":
        regressions = _to_int(row.get("defensive_role_regression_count"))
        if mode == "relaxed_gate":
            return regressions <= 2
        if mode == "strict_gate":
            return regressions == 0
    if gate_id == "net_of_cost_not_worse":
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return (
                _current_status(row, gate_id)
                and to_float(row.get("average_probe_turnover")) <= 75.0
            )
    if gate_id == "2022_slice_not_worse_than_flat_reference":
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return (
                _current_status(row, gate_id) and _to_int(row.get("prediction_count_2022")) >= 500
            )
    if gate_id == "not_2023_plus_only":
        if mode == "relaxed_gate":
            return _date_on_or_before(row.get("first_prediction_date"), "2023-06-30")
        if mode == "strict_gate":
            return _to_int(row.get("prediction_count_2022")) >= 60
    if gate_id in {"not_beta_dependency", "not_tqqq_dependency"}:
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return False
    if gate_id in {"probability_threshold_0_55", "probability_threshold_0_60"}:
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return False
    if gate_id == "all_slices_not_worse":
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return _current_status(row, gate_id) and _to_int(row.get("prediction_count_2022")) >= 60
    if gate_id == "no_slice_regression":
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return (
                _current_status(row, gate_id) and _to_int(row.get("prediction_count_2022")) >= 500
            )
    return _current_status(row, gate_id)


def _summary(
    *,
    actual_path: Mapping[str, Any],
    current_state_summary: Mapping[str, Any],
    failure_taxonomy: Mapping[str, Any],
    objective_validation: Mapping[str, Any],
    challenger_rows: Sequence[Mapping[str, Any]],
    coverage_final: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    gate_rows: Sequence[Mapping[str, Any]],
    current_accept_ids: set[str],
    dependency_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    real_candidates = [
        row for row in candidate_rows if not bool(row.get("diagnostic_only_exclusion"))
    ]
    best = max(
        real_candidates,
        key=lambda row: to_float(row.get("actual_path_utility_proxy")),
    )
    objective_summary = mapping(objective_validation.get("summary"))
    challenger_ready = [row for row in challenger_rows if bool(row.get("validation_ready"))]
    challenger_actual_path_available = [
        row
        for row in challenger_rows
        if row.get("actual_path_status") != "unavailable_actual_path_not_run"
    ]
    failure_summary = mapping(failure_taxonomy.get("summary"))
    coverage_summary = mapping(coverage_final.get("summary"))
    return {
        "task_id": "TRADING-2274_FIRST_LAYER_PERFORMANCE_GATE_ACCEPTANCE_AUDIT",
        "market_regime": current_state_summary.get(
            "market_regime", actual_path.get("market_regime")
        ),
        "anchor_date": current_state_summary.get("anchor_date", actual_path.get("anchor_date")),
        "requested_start": current_state_summary.get(
            "requested_start", actual_path.get("requested_start")
        ),
        "actual_signal_start": current_state_summary.get("actual_signal_start"),
        "actual_path_requested_start": actual_path.get("requested_start"),
        "actual_path_actual_start": actual_path.get("actual_start"),
        "data_quality_status": current_state_summary.get("data_quality_status"),
        "candidate_count_before_gate": len(real_candidates),
        "candidate_count_after_current_performance_gates": len(current_accept_ids),
        "rejected_candidate_count": len(real_candidates) - len(current_accept_ids),
        "current_gate_accept_policy_ids": sorted(current_accept_ids),
        "active_selection_rule_current_accept_count": _to_int(
            coverage_summary.get("coverage_aware_selection_pass_count")
        ),
        "best_actual_path_candidate_policy_id": best["policy_id"],
        "best_actual_path_candidate_utility_proxy": best["actual_path_utility_proxy"],
        "offline_validation_ready_challenger_count": len(challenger_ready),
        "challenger_actual_path_available_count": len(challenger_actual_path_available),
        "challenger_actual_path_limitation": (
            "offline_ready_rows_have_no_complete_two_layer_actual_path_backtest"
        ),
        "failure_event_count": failure_summary.get(
            "failure_event_count",
            mapping(current_state_summary.get("summary")).get("failure_event_count"),
        ),
        "objective_contract_status": objective_validation.get("status"),
        "stress_validation_allowed": bool(objective_summary.get("stress_validation_allowed")),
        "true_breadth_replaced": bool(objective_summary.get("true_breadth_replaced")),
        "negative_marginal_utility_gates": [
            row["gate_id"] for row in gate_rows if row["gate_marginal_utility"] == "negative"
        ],
        "positive_marginal_utility_gates": [
            row["gate_id"] for row in gate_rows if row["gate_marginal_utility"] == "positive"
        ],
        "neutral_marginal_utility_gates": [
            row["gate_id"] for row in gate_rows if row["gate_marginal_utility"] == "neutral"
        ],
        "inconclusive_marginal_utility_gates": [
            row["gate_id"] for row in gate_rows if row["gate_marginal_utility"] == "inconclusive"
        ],
        "dependency_evidence_scope": dependency_evidence.get("evidence_scope"),
        "return_seeking_depends_on_2023_plus": dependency_evidence.get(
            "return_seeking_depends_on_2023_plus"
        ),
        "return_seeking_tqqq_dependency_suspected_count": dependency_evidence.get(
            "return_seeking_tqqq_dependency_suspected_count"
        ),
        "active_policy_change_allowed": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
        "coverage_final_status": coverage_final.get("status"),
    }


def _common_payload(
    *,
    actual_path: Mapping[str, Any],
    current_state_summary: Mapping[str, Any],
    policy: Mapping[str, Any],
    summary: Mapping[str, Any],
    input_artifacts: Mapping[str, str],
) -> dict[str, Any]:
    safety = mapping(policy.get("safety_boundary"))
    return {
        "schema_version": "first_layer_performance_gate_audit.v2",
        "report_type": "first_layer_performance_gate_audit",
        "title": "First-Layer Performance Gate Acceptance Audit",
        "status": STATUS,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": current_state_summary.get(
            "market_regime", actual_path.get("market_regime")
        ),
        "anchor_event": current_state_summary.get("anchor_event", actual_path.get("anchor_event")),
        "anchor_date": current_state_summary.get("anchor_date", actual_path.get("anchor_date")),
        "requested_start": current_state_summary.get(
            "requested_start", actual_path.get("requested_start")
        ),
        "actual_signal_start": current_state_summary.get("actual_signal_start"),
        "actual_signal_end": current_state_summary.get("actual_signal_end"),
        "actual_path_requested_start": actual_path.get("requested_start"),
        "actual_path_actual_start": actual_path.get("actual_start"),
        "actual_path_actual_portfolio_start": actual_path.get("actual_portfolio_start"),
        "end": actual_path.get("end"),
        "data_quality_status": current_state_summary.get("data_quality_status"),
        "input_artifacts": dict(input_artifacts),
        "summary": clean_for_yaml(dict(summary)),
        "research_only": bool(safety.get("research_only", True)),
        "actual_path_required": bool(safety.get("actual_path_required", True)),
        "target_path_metrics_role": safety.get("target_path_metrics_role", "diagnostic_only"),
        "active_policy_change_allowed": bool(safety.get("active_policy_change_allowed", False)),
        "promotion_allowed": bool(safety.get("promotion_allowed", False)),
        "paper_shadow_allowed": bool(safety.get("paper_shadow_allowed", False)),
        "production_allowed": bool(safety.get("production_allowed", False)),
        "broker_action": safety.get("broker_action", "none"),
        "dynamic_promotion_status": safety.get("dynamic_promotion_status", "BLOCKED"),
    }


def _recommended_policy(
    *,
    common: Mapping[str, Any],
    policy: Mapping[str, Any],
    gate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        **dict(common),
        "schema_version": "first_layer_performance_gate_recommendation.v2",
        "policy_id": "recommended_first_layer_performance_gate_policy_v2",
        "source_policy_id": policy.get("policy_id"),
        "status": "OWNER_REVIEW_REQUIRED",
        "allowed_recommended_actions": sorted(RECOMMENDED_ACTIONS),
        "summary": {
            **mapping(common.get("summary")),
            "active_policy_change_allowed": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
        },
        "recommended_gate_actions": [
            {
                "gate_id": row["gate_id"],
                "gate_marginal_utility": row["gate_marginal_utility"],
                "gate_failure_mode_reduced": row["gate_failure_mode_reduced"],
                "opportunity_cost": row["opportunity_cost"],
                "threshold_stability": row["threshold_stability"],
                "recommended_action": row["recommended_action"],
                "evidence_status": row["evidence_status"],
                "owner_decision_override": mapping(row.get("owner_decision_override")),
            }
            for row in gate_rows
        ],
        "hard_boundary_gates_excluded_from_performance_waiver": [
            "pit_safety",
            "no_lookahead",
            "data_quality",
            "actual_path_required",
            "owner_approval",
            "production_boundary",
        ],
        "safety_boundary": {
            "research_only": True,
            "actual_path_required": True,
            "target_path_metrics_role": "diagnostic_only",
            "active_policy_change_allowed": False,
            "promotion_allowed": False,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "broker_action": "none",
            "dynamic_promotion_status": "BLOCKED",
        },
    }


def _render_report(
    summary: Mapping[str, Any],
    gate_rows: Sequence[Mapping[str, Any]],
    rejected_rows: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
    challenger_rows: Sequence[Mapping[str, Any]],
    evidence_limitations: Sequence[Mapping[str, Any]],
    paths: Mapping[str, Path],
) -> str:
    lines = [
        "# First-Layer Performance Gate Acceptance Audit",
        "",
        "## 摘要",
        "",
        (
            f"- task_id: `{summary.get('task_id')}`; "
            f"market_regime: `{summary.get('market_regime')}`; "
            f"requested_start: `{summary.get('requested_start')}`; "
            f"actual_signal_start: `{summary.get('actual_signal_start')}`; "
            f"data_quality_status: `{summary.get('data_quality_status')}`"
        ),
        (
            "- mandatory performance gate current accept count: "
            f"`{summary.get('candidate_count_after_current_performance_gates')}` / "
            f"`{summary.get('candidate_count_before_gate')}`; "
            "active selection rule accept count remains "
            f"`{summary.get('active_selection_rule_current_accept_count')}`."
        ),
        (
            "- offline validation-ready challenger rows: "
            f"`{summary.get('offline_validation_ready_challenger_count')}`; "
            "complete two-layer actual-path rows available: "
            f"`{summary.get('challenger_actual_path_available_count')}`."
        ),
        (
            "- promotion_allowed=`false`, paper_shadow_allowed=`false`, "
            "production_allowed=`false`, broker_action=`none`."
        ),
        (
            "- best_actual_path_candidate: "
            f"`{summary.get('best_actual_path_candidate_policy_id')}` = "
            f"`{summary.get('best_actual_path_candidate_utility_proxy')}`"
        ),
        "",
        "## Gate 结论",
        "",
        (
            "| gate | before | after_current | gate_marginal_utility | "
            "opportunity_cost | threshold_stability | recommended_action |"
        ),
        "|---|---:|---:|---|---:|---|---|",
    ]
    for row in gate_rows:
        lines.append(
            f"|`{row['gate_id']}`|`{row['candidate_count_before_gate']}`|"
            f"`{row['candidate_count_after_gate']}`|"
            f"`{row['gate_marginal_utility']}`|`{row['opportunity_cost']}`|"
            f"`{row['threshold_stability']}`|`{row['recommended_action']}`|"
        )
    lines.extend(
        [
            "",
            "## Rejected Candidate Counterfactual",
            "",
            "| policy_id | failed_current_gates | utility_proxy | owner_review_candidate |",
            "|---|---|---:|---|",
        ]
    )
    for row in rejected_rows:
        failed = ",".join(f"`{gate}`" for gate in strings(row.get("failed_current_gates")))
        lines.append(
            f"|`{row['policy_id']}`|{failed}|"
            f"`{row['actual_path_utility_proxy']}`|"
            f"`{row['owner_review_candidate']}`|"
        )
    lines.extend(
        [
            "",
            "## Candidate Utility Proxy",
            "",
            (
                "| policy_id | avg_return | avg_abs_drawdown | avg_sharpe | "
                "avg_calmar | avg_turnover | utility_proxy |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in candidate_rows:
        if bool(row.get("diagnostic_only_exclusion")):
            continue
        lines.append(
            f"|`{row['policy_id']}`|`{row['average_probe_annual_return']}`|"
            f"`{row['average_abs_max_drawdown']}`|"
            f"`{row['average_probe_sharpe']}`|"
            f"`{row['average_probe_calmar']}`|"
            f"`{row['average_probe_turnover']}`|"
            f"`{row['actual_path_utility_proxy']}`|"
        )
    lines.extend(
        [
            "",
            "## Challenger Rows",
            "",
            "| experiment_id | validation_ready | actual_path_status |",
            "|---|---|---|",
        ]
    )
    for row in challenger_rows:
        lines.append(
            f"|`{row['experiment_id']}`|`{row['validation_ready']}`|`{row['actual_path_status']}`|"
        )
    lines.extend(["", "## Evidence Limitations", ""])
    for row in evidence_limitations:
        lines.append(
            f"- `{row['limitation_id']}`: {row['impact']} action=`{row['required_action']}`"
        )
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            (
                "本 audit 只读既有 current-state / objective spec / challenger matrix / "
                "frozen actual-path evidence，不改变 active selection rule。PIT、"
                "no-lookahead、data quality、actual-path、owner approval 和 production "
                "boundary 仍是 hard gates，不参与收益表现豁免。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _input_artifacts(**paths: Path) -> dict[str, str]:
    return {key: str(path) for key, path in paths.items()}


def _by_policy(rows: Sequence[Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    return {str(row.get("policy_id")): row for row in rows}


def _probe_rows_by_policy(
    actual_path: Mapping[str, Any],
) -> dict[str, list[Mapping[str, Any]]]:
    rows: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records(actual_path.get("probe_rows")):
        rows[str(row.get("policy_id"))].append(row)
    return rows


def _defensive_regression_counts(
    defensive_inventory: Mapping[str, Any],
) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in records(defensive_inventory.get("probe_rows")):
        if "defensive_probe_regression" in strings(row.get("regression_type")):
            counts[str(row.get("policy_id"))] += 1
    return counts


def _defensive_metrics_by_policy(
    defensive_inventory: Mapping[str, Any],
) -> dict[str, dict[str, float]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in records(defensive_inventory.get("probe_rows")):
        grouped[str(row.get("policy_id"))].append(row)
    metrics = {}
    for policy_id, rows in grouped.items():
        metrics[policy_id] = {
            field: _mean(to_float(row.get(field)) for row in rows)
            for field in (
                "annual_return_delta",
                "max_drawdown_delta",
                "sharpe_delta",
                "calmar_delta",
                "turnover_delta",
            )
        }
    return metrics


def _objective_context(objective_validation: Mapping[str, Any]) -> dict[str, Any]:
    stress = mapping(objective_validation.get("stress_slice_minimum_requirements"))
    return {
        "stress_validation_allowed": bool(stress.get("stress_validation_allowed")),
        "stress_slice_requirements_met": bool(stress.get("all_requirements_met")),
        "required_slice_count": _to_int(stress.get("required_slice_count")),
        "met_slice_count": _to_int(stress.get("met_slice_count")),
        "blocked_slice_ids": strings(stress.get("blocked_slice_ids")),
        "all_slice_actual_path_status": (
            "incomplete_required_slice_actual_path_evidence"
            if not bool(stress.get("all_requirements_met"))
            else "all_required_slices_available"
        ),
    }


def _dependency_evidence(
    *,
    return_seeking_2022_contrast: Mapping[str, Any],
    return_seeking_beta_tqqq_attribution: Mapping[str, Any],
) -> dict[str, Any]:
    contrast_summary = mapping(return_seeking_2022_contrast.get("summary"))
    attribution_summary = mapping(return_seeking_beta_tqqq_attribution.get("summary"))
    return {
        "evidence_scope": "return_seeking_diagnostic_lane_not_candidate_level_policy_rows",
        "return_seeking_depends_on_2023_plus": bool(contrast_summary.get("depends_on_2023_plus")),
        "positive_delta_count_2022": _to_int(contrast_summary.get("positive_delta_count_2022")),
        "positive_delta_count_2023_plus": _to_int(
            contrast_summary.get("positive_delta_count_2023_plus")
        ),
        "post_2023_positive_delta_share": round_float(
            contrast_summary.get("post_2023_positive_delta_share")
        ),
        "return_seeking_qqq_beta_dependency_suspected_count": _to_int(
            attribution_summary.get("qqq_beta_dependency_suspected_count")
        ),
        "return_seeking_tqqq_dependency_suspected_count": _to_int(
            attribution_summary.get("tqqq_beta_dependency_suspected_count")
        ),
        "no_tqqq_reference_avg_qqq_equivalent_exposure": round_float(
            attribution_summary.get("no_tqqq_reference_avg_qqq_equivalent_exposure")
        ),
    }


def _evidence_limitations(
    *,
    objective_context: Mapping[str, Any],
    dependency_evidence: Mapping[str, Any],
    challenger_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, str]]:
    limitations = [
        {
            "limitation_id": "challenger_actual_path_not_run",
            "impact": (
                "offline validation-ready challenger rows cannot be scored as "
                "complete two-layer actual-path candidates"
            ),
            "required_action": "run_future_candidate_level_actual_path_backtest",
        },
        {
            "limitation_id": "candidate_level_objective_deltas_unavailable",
            "impact": (
                "false risk-on/off, lead-time, recovery-delay and benchmark "
                "consistency deltas are not present in frozen actual-path policy rows"
            ),
            "required_action": "extend_actual_path_backtest_to_objective_terms",
        },
        {
            "limitation_id": "candidate_level_dependency_deltas_unavailable",
            "impact": (
                "beta/TQQQ dependency evidence is lane-level diagnostic evidence, "
                "not row-level gate proof for each first-layer coverage policy"
            ),
            "required_action": (
                "convert_dependency_gates_to_owner_review_until_row_level_evidence_exists"
            ),
        },
        {
            "limitation_id": "candidate_level_probability_distribution_unavailable",
            "impact": (
                "0.55/0.60 probability thresholds cannot be calibrated from the "
                "frozen actual-path matrix"
            ),
            "required_action": "treat_probability_thresholds_as_score_penalty_or_owner_review",
        },
    ]
    if not bool(objective_context.get("stress_slice_requirements_met")):
        limitations.append(
            {
                "limitation_id": "all_required_slice_actual_path_incomplete",
                "impact": (
                    "all-slices gate cannot be promoted to a hard performance gate "
                    "because required stress slice evidence remains incomplete"
                ),
                "required_action": "owner_review_or_future_all_slice_backtest",
            }
        )
    if not any(
        row.get("actual_path_status") != "unavailable_actual_path_not_run"
        for row in challenger_rows
    ):
        limitations.append(
            {
                "limitation_id": "no_challenger_counterfactual_actual_path_rows",
                "impact": "challenger matrix contributes readiness context only",
                "required_action": "do_not_use_challenger_rows_for_promotion",
            }
        )
    if dependency_evidence.get("return_seeking_depends_on_2023_plus"):
        limitations.append(
            {
                "limitation_id": "return_seeking_2023_plus_dependency_diagnostic",
                "impact": (
                    "2023+ dependency exists as diagnostic evidence but not as "
                    "candidate-level gate delta"
                ),
                "required_action": "owner_review_before_hard_gate_application",
            }
        )
    return limitations


def _current_status(row: Mapping[str, Any], gate_id: str) -> bool:
    return bool(mapping(row.get("current_gate_status")).get(gate_id))


def _date_on_or_before(value: object, boundary: str) -> bool:
    return bool(value) and str(value) <= boundary


def _classify_utility(
    *,
    gate_id: str,
    blocked_mean: float,
    blocked_count: int,
    positive_min: float,
    neutral_max: float,
) -> str:
    if gate_id in EVIDENCE_LIMITED_GATES:
        return "inconclusive"
    if blocked_count == 0:
        return "neutral"
    if blocked_mean > positive_min:
        return "negative"
    if abs(blocked_mean) <= neutral_max:
        return "neutral"
    if blocked_mean < -positive_min:
        return "positive"
    return "inconclusive"


def _threshold_stability(
    *,
    gate_id: str,
    threshold_sensitivity: Mapping[str, Mapping[str, Any]],
    utility: str,
    material_cost: bool,
) -> str:
    if gate_id in {
        "not_beta_dependency",
        "not_tqqq_dependency",
        "probability_threshold_0_55",
        "probability_threshold_0_60",
    }:
        return "insufficient_candidate_level_evidence"
    if gate_id == "all_slices_not_worse":
        return "incomplete_all_slice_actual_path_evidence"
    accepted_sets = {
        tuple(strings(row.get("accepted_policy_ids"))) for row in threshold_sensitivity.values()
    }
    if len(accepted_sets) == 1:
        return "stable"
    if utility == "negative" and material_cost:
        return "unstable_material_opportunity_cost"
    return "sensitive_to_threshold"


def _gate_evidence_status(gate_id: str) -> str:
    if gate_id in {"not_beta_dependency", "not_tqqq_dependency"}:
        return "dependency_evidence_lane_level_not_candidate_level"
    if gate_id in {"probability_threshold_0_55", "probability_threshold_0_60"}:
        return "candidate_probability_distribution_unavailable"
    if gate_id == "all_slices_not_worse":
        return "all_required_slice_actual_path_incomplete"
    return "candidate_level_actual_path_evidence_available"


def _objective_delta_status(gate_id: str) -> str:
    if gate_id in {
        "all_slices_not_worse",
        "no_slice_regression",
        "2022_slice_not_worse_than_flat_reference",
    }:
        return "partial_slice_delta_available_2022_only"
    return "objective_term_deltas_unavailable_for_candidate_rows"


def _dependency_delta_status(gate_id: str) -> str:
    if gate_id in {"not_beta_dependency", "not_tqqq_dependency", "not_2023_plus_only"}:
        return "dependency_delta_unavailable_candidate_level"
    return "not_applicable"


def _recommended_action(
    *,
    gate_id: str,
    utility: str,
    material_cost: bool,
    threshold_stability: str,
    current_accept_count: int,
    owner_override: Mapping[str, Any],
) -> str:
    override_action = str(owner_override.get("recommended_action", ""))
    if override_action in RECOMMENDED_ACTIONS:
        return override_action
    if gate_id == "no_major_regression_in_defensive_probe" and utility == "positive":
        return "keep_as_hard_gate"
    if gate_id == "not_2023_plus_only" and utility == "negative":
        return "convert_to_owner_review"
    if gate_id in {"not_beta_dependency", "not_tqqq_dependency", "all_slices_not_worse"}:
        return "convert_to_owner_review"
    if gate_id in {"probability_threshold_0_55", "probability_threshold_0_60"}:
        return "convert_to_score_penalty"
    if gate_id == "actual_path_improved_probe_count_min" and utility == "neutral":
        return "convert_to_score_penalty"
    if gate_id == "net_of_cost_not_worse" and utility == "neutral":
        return "convert_to_score_penalty"
    if gate_id in {
        "2022_slice_not_worse_than_flat_reference",
        "no_slice_regression",
    }:
        return "keep_as_performance_gate"
    if utility == "positive":
        return "keep_as_performance_gate"
    if utility == "negative" and material_cost:
        return "convert_to_owner_review"
    if threshold_stability == "stable" and current_accept_count == 0:
        return "remove_gate"
    return "convert_to_score_penalty"


def _owner_review_candidate(row: Mapping[str, Any]) -> bool:
    failed = set(strings(row.get("failed_current_gates")))
    reviewable = {
        "not_2023_plus_only",
        "not_beta_dependency",
        "not_tqqq_dependency",
        "probability_threshold_0_55",
        "probability_threshold_0_60",
        "all_slices_not_worse",
        "net_of_cost_not_worse",
        "2022_slice_not_worse_than_flat_reference",
    }
    return to_float(row.get("actual_path_utility_proxy")) > 0.0 and failed.issubset(reviewable)


def _metric_snapshot(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {field: _average_metric(rows, field, default=None) for field in CORE_DELTA_FIELDS}


def _metric_delta(
    current_rows: Sequence[Mapping[str, Any]],
    no_gate_rows: Sequence[Mapping[str, Any]],
    field: str,
) -> float | None:
    current = _average_metric(current_rows, field, default=None)
    no_gate = _average_metric(no_gate_rows, field, default=None)
    if current is None or no_gate is None:
        return None
    return round_float(current - no_gate)


def _utility_delta(current_avg: float | None, no_gate_avg: float | None) -> float:
    return round_float(to_float(current_avg) - to_float(no_gate_avg))


def _average_metric(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    default: float | None,
) -> float | None:
    values = []
    for row in rows:
        value = row.get(field)
        if value in (None, "") or isinstance(value, bool):
            continue
        values.append(to_float(value))
    if not values:
        return default
    return round_float(_mean(values))


def _optional_round(value: object) -> float | None:
    if value in (None, ""):
        return None
    return round_float(value)


def _mean(values: Sequence[float] | Any) -> float:
    items = [to_float(value) for value in values if value is not None]
    return sum(items) / len(items) if items else 0.0


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
