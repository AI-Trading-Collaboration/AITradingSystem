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
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_performance_gate_audit"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_PERFORMANCE_GATE_AUDIT_READY_PROMOTION_BLOCKED"
GATE_MODES = ("no_gate", "relaxed_gate", "current_gate", "strict_gate")
DIAGNOSTIC_ONLY_POLICY_IDS = {"wf_warm_start_diagnostic"}


def run_first_layer_performance_gate_audit_pack(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    actual_path_path: Path = DEFAULT_ACTUAL_PATH_YAML_PATH,
    coverage_simulation_path: Path = DEFAULT_COVERAGE_SIMULATION_YAML_PATH,
    slice_matrix_path: Path = DEFAULT_2022_SLICE_YAML_PATH,
    defensive_inventory_path: Path = DEFAULT_REGRESSION_INVENTORY_YAML_PATH,
    selection_rule_path: Path = DEFAULT_COVERAGE_SELECTION_RULE_PATH,
    coverage_final_path: Path = DEFAULT_COVERAGE_FINAL_MATRIX_YAML_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    policy = load_mapping(policy_path)
    actual_path = load_mapping(actual_path_path)
    coverage_simulation = load_mapping(coverage_simulation_path)
    slice_matrix = load_mapping(slice_matrix_path)
    defensive_inventory = load_mapping(defensive_inventory_path)
    selection_rule = load_mapping(selection_rule_path)
    coverage_final = load_mapping(coverage_final_path)

    gate_ids = tuple(str(gate_id) for gate_id in mapping(policy.get("gate_modes")))
    candidate_rows = _candidate_rows(
        policy=policy,
        actual_path=actual_path,
        coverage_simulation=coverage_simulation,
        slice_matrix=slice_matrix,
        defensive_inventory=defensive_inventory,
        selection_rule=selection_rule,
    )
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
    summary = _summary(
        actual_path=actual_path,
        coverage_final=coverage_final,
        candidate_rows=candidate_rows,
        gate_rows=gate_rows,
        current_accept_ids=current_accept_ids,
    )
    common = _common_payload(actual_path, policy, summary)
    paths = {
        "gate_ablation_matrix": output_root / "gate_ablation_matrix.json",
        "threshold_sensitivity_report": output_root
        / "threshold_sensitivity_report.json",
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
            "gate_rows": gate_rows,
            "ablation_rows": ablation_rows,
        },
    )
    write_json(
        paths["threshold_sensitivity_report"],
        {
            **common,
            "actual_path_utility_proxy": mapping(
                policy.get("actual_path_utility_proxy")
            ),
            "marginal_utility_thresholds": mapping(
                policy.get("marginal_utility_thresholds")
            ),
            "gate_rows": gate_rows,
            "mode_rows": ablation_rows,
        },
    )
    write_json(
        paths["rejected_candidate_counterfactual_report"],
        {
            **common,
            "candidate_rows": candidate_rows,
            "rejected_candidate_rows": rejected_rows,
        },
    )
    write_yaml(
        paths["recommended_gate_policy"],
        _recommended_policy(common=common, policy=policy, gate_rows=gate_rows),
    )
    write_markdown(
        paths["gate_acceptance_audit_report"],
        _render_report(summary, gate_rows, rejected_rows, candidate_rows, paths),
    )
    return clean_for_yaml(
        {
            **common,
            "gate_rows": gate_rows,
            "ablation_rows": ablation_rows,
            "rejected_candidate_rows": rejected_rows,
            "candidate_rows": candidate_rows,
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
) -> list[dict[str, Any]]:
    actual_rows = _by_policy(records(actual_path.get("policy_rows")))
    coverage_rows = _by_policy(records(coverage_simulation.get("policy_rows")))
    slice_rows = _by_policy(records(slice_matrix.get("policy_rows")))
    probe_rows = _probe_rows_by_policy(actual_path)
    defensive_counts = _defensive_regression_counts(defensive_inventory)
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
        avg_return = _mean(to_float(row.get("v2_annual_return")) for row in probes)
        avg_abs_drawdown = _mean(
            abs(to_float(row.get("v2_max_drawdown"))) for row in probes
        )
        avg_turnover = _mean(to_float(row.get("v2_turnover")) for row in probes)
        utility = (
            avg_return * return_weight
            - avg_abs_drawdown * drawdown_weight
            - avg_turnover * turnover_weight
        )
        gate_status = {
            "coverage_pass_rule": bool(actual.get("does_coverage_pass_rule")),
            "actual_path_improved_probe_count_min": _to_int(
                actual.get("actual_path_improved_probe_count")
            )
            >= current_min_improved,
            "no_major_regression_in_defensive_probe": bool(
                actual.get("no_major_regression_in_defensive_probe")
            ),
            "net_of_cost_not_worse": bool(actual.get("net_of_cost_not_worse")),
            "2022_slice_not_worse_than_flat_reference": bool(
                slice_row.get(
                    "2022_slice_not_worse_than_flat_reference",
                    actual.get("2022_slice_not_worse_than_flat_reference"),
                )
            ),
            "same_risk_comparison_reported": bool(
                actual.get("same_risk_comparison_reported")
            ),
        }
        diagnostic_only = policy_id in DIAGNOSTIC_ONLY_POLICY_IDS
        failed = [gate for gate, passed in gate_status.items() if not passed]
        if diagnostic_only:
            failed.append("diagnostic_only_policy")
        rows.append(
            {
                "policy_id": policy_id,
                "candidate_type": "diagnostic_only" if diagnostic_only else "candidate",
                "diagnostic_only_exclusion": diagnostic_only,
                "first_prediction_date": actual.get(
                    "first_prediction_date", coverage.get("first_prediction_date")
                ),
                "covered_2022": bool(coverage.get("covered_2022")),
                "covered_2022_risk_off_window": bool(
                    coverage.get("covered_2022_risk_off_window")
                ),
                "covered_2022_recovery_window": bool(
                    coverage.get("covered_2022_recovery_window")
                ),
                "prediction_count_2022": _to_int(
                    slice_row.get("prediction_count_2022")
                ),
                "actual_path_improved_probe_count": _to_int(
                    actual.get("actual_path_improved_probe_count")
                ),
                "probe_count": _to_int(actual.get("probe_count"), len(probes)),
                "defensive_role_regression_count": defensive_counts.get(policy_id, 0),
                "average_probe_annual_return": round_float(avg_return),
                "average_abs_max_drawdown": round_float(avg_abs_drawdown),
                "average_probe_turnover": round_float(avg_turnover),
                "actual_path_utility_proxy": round_float(utility),
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
            accepted_rows = [
                row for row in candidate_rows if str(row["policy_id"]) in accepted_ids
            ]
            rows.append(
                {
                    "gate_id": gate_id,
                    "mode": mode,
                    "accepted_policy_ids": sorted(accepted_ids),
                    "accepted_candidate_count": len(accepted_ids),
                    "accepted_average_actual_path_utility_proxy": round_float(
                        _mean(
                            to_float(row.get("actual_path_utility_proxy"))
                            for row in accepted_rows
                        )
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
                    "newly_accepted_vs_current": sorted(
                        accepted_ids - current_accept_ids
                    ),
                    "newly_rejected_vs_current": sorted(
                        current_accept_ids - accepted_ids
                    ),
                    "candidate_decisions": [
                        {
                            "policy_id": row["policy_id"],
                            "accepted": str(row["policy_id"]) in accepted_ids,
                            "actual_path_utility_proxy": row[
                                "actual_path_utility_proxy"
                            ],
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
    material_cost_min = to_float(
        thresholds.get("material_opportunity_cost_min"), 0.01
    )
    modes = {
        (str(row["gate_id"]), str(row["mode"])): row for row in ablation_rows
    }
    candidates = {str(row["policy_id"]): row for row in candidate_rows}
    gate_policy = mapping(policy.get("gate_modes"))
    rows = []
    for gate_id in gate_ids:
        no_gate_ids = set(strings(modes[(gate_id, "no_gate")].get("accepted_policy_ids")))
        current_ids = set(
            strings(modes[(gate_id, "current_gate")].get("accepted_policy_ids"))
        )
        blocked_ids = sorted(no_gate_ids - current_ids)
        blocked_rows = [candidates[policy_id] for policy_id in blocked_ids]
        blocked_utilities = [
            to_float(row.get("actual_path_utility_proxy")) for row in blocked_rows
        ]
        blocked_mean = _mean(blocked_utilities)
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
            blocked_mean=blocked_mean,
            blocked_count=len(blocked_rows),
            positive_min=positive_min,
            neutral_max=neutral_max,
        )
        material_cost = opportunity_cost >= material_cost_min
        rows.append(
            {
                "gate_id": gate_id,
                "gate_marginal_utility": utility,
                "gate_failure_mode_reduced": mapping(gate_policy.get(gate_id)).get(
                    "failure_mode_reduced", ""
                ),
                "opportunity_cost": round_float(opportunity_cost),
                "best_blocked_policy_id": best_blocked,
                "blocked_policy_ids_vs_no_gate": blocked_ids,
                "blocked_average_actual_path_utility_proxy": round_float(
                    blocked_mean
                ),
                "accepted_current_policy_ids": sorted(current_ids),
                "accepted_no_gate_policy_ids": sorted(no_gate_ids),
                "accepted_relaxed_gate_policy_ids": strings(
                    modes[(gate_id, "relaxed_gate")].get("accepted_policy_ids")
                ),
                "accepted_strict_gate_policy_ids": strings(
                    modes[(gate_id, "strict_gate")].get("accepted_policy_ids")
                ),
                "material_opportunity_cost": material_cost,
                "recommended_action": _recommended_action(
                    gate_id=gate_id,
                    utility=utility,
                    material_cost=material_cost,
                    current_accept_count=len(current_accept_ids),
                ),
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
                "average_probe_turnover": row.get("average_probe_turnover"),
                "acceptance_by_single_gate_counterfactual": mode_acceptance,
                "owner_review_candidate": _owner_review_candidate(row),
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
                replacement_mode
                if gate_id == replacement_gate_id
                else "current_gate",
            )
            for gate_id in gate_ids
        ):
            accepted.add(str(row["policy_id"]))
    return accepted


def _gate_passes(row: Mapping[str, Any], gate_id: str, mode: str) -> bool:
    if mode == "no_gate":
        return True
    if gate_id == "coverage_pass_rule":
        if mode == "relaxed_gate":
            return bool(row.get("covered_2022")) and _date_on_or_before(
                row.get("first_prediction_date"), "2022-12-31"
            )
        if mode == "strict_gate":
            return (
                bool(row.get("covered_2022_risk_off_window"))
                and bool(row.get("covered_2022_recovery_window"))
                and _date_on_or_before(row.get("first_prediction_date"), "2022-02-18")
            )
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
            return _current_status(row, gate_id) and to_float(
                row.get("average_probe_turnover")
            ) <= 75.0
    if gate_id == "2022_slice_not_worse_than_flat_reference":
        if mode == "relaxed_gate":
            return True
        if mode == "strict_gate":
            return _current_status(row, gate_id) and _to_int(
                row.get("prediction_count_2022")
            ) >= 500
    if gate_id == "same_risk_comparison_reported" and mode in {
        "relaxed_gate",
        "strict_gate",
    }:
        return True if mode == "relaxed_gate" else _current_status(row, gate_id)
    return _current_status(row, gate_id)


def _summary(
    *,
    actual_path: Mapping[str, Any],
    coverage_final: Mapping[str, Any],
    candidate_rows: Sequence[Mapping[str, Any]],
    gate_rows: Sequence[Mapping[str, Any]],
    current_accept_ids: set[str],
) -> dict[str, Any]:
    real_candidates = [
        row for row in candidate_rows if not bool(row.get("diagnostic_only_exclusion"))
    ]
    best = max(
        real_candidates,
        key=lambda row: to_float(row.get("actual_path_utility_proxy")),
    )
    return {
        "market_regime": actual_path.get("market_regime"),
        "anchor_date": actual_path.get("anchor_date"),
        "requested_start": actual_path.get("requested_start"),
        "actual_start": actual_path.get("actual_start"),
        "actual_portfolio_start": actual_path.get("actual_portfolio_start"),
        "candidate_count": len(real_candidates),
        "current_gate_accept_count": len(current_accept_ids),
        "current_gate_accept_policy_ids": sorted(current_accept_ids),
        "best_actual_path_candidate_policy_id": best["policy_id"],
        "best_actual_path_candidate_utility_proxy": best[
            "actual_path_utility_proxy"
        ],
        "negative_marginal_utility_gates": [
            row["gate_id"]
            for row in gate_rows
            if row["gate_marginal_utility"] == "negative"
        ],
        "positive_marginal_utility_gates": [
            row["gate_id"]
            for row in gate_rows
            if row["gate_marginal_utility"] == "positive"
        ],
        "neutral_marginal_utility_gates": [
            row["gate_id"]
            for row in gate_rows
            if row["gate_marginal_utility"] == "neutral"
        ],
        "active_policy_change_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
        "coverage_final_status": coverage_final.get("status"),
    }


def _common_payload(
    actual_path: Mapping[str, Any],
    policy: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    safety = mapping(policy.get("safety_boundary"))
    return {
        "schema_version": "first_layer_performance_gate_audit.v1",
        "report_type": "first_layer_performance_gate_audit",
        "title": "First-Layer Performance Gate Acceptance Audit",
        "status": STATUS,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": actual_path.get("market_regime"),
        "anchor_event": actual_path.get("anchor_event"),
        "anchor_date": actual_path.get("anchor_date"),
        "requested_start": actual_path.get("requested_start"),
        "actual_start": actual_path.get("actual_start"),
        "actual_portfolio_start": actual_path.get("actual_portfolio_start"),
        "end": actual_path.get("end"),
        "summary": clean_for_yaml(dict(summary)),
        "research_only": bool(safety.get("research_only", True)),
        "actual_path_required": bool(safety.get("actual_path_required", True)),
        "target_path_metrics_role": safety.get(
            "target_path_metrics_role", "diagnostic_only"
        ),
        "active_policy_change_allowed": bool(
            safety.get("active_policy_change_allowed", False)
        ),
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
        "schema_version": "first_layer_performance_gate_recommendation.v1",
        "policy_id": "recommended_first_layer_performance_gate_policy_v1",
        "source_policy_id": policy.get("policy_id"),
        "status": "OWNER_REVIEW_REQUIRED",
        "summary": {
            **mapping(common.get("summary")),
            "active_policy_change_allowed": False,
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
                "recommended_action": row["recommended_action"],
            }
            for row in gate_rows
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
    paths: Mapping[str, Path],
) -> str:
    lines = [
        "# First-Layer Performance Gate Acceptance Audit",
        "",
        "## 摘要",
        "",
        (
            f"- market_regime: `{summary.get('market_regime')}`; "
            f"anchor_date: `{summary.get('anchor_date')}`; "
            f"requested_start: `{summary.get('requested_start')}`; "
            f"actual_start: `{summary.get('actual_start')}`"
        ),
        (
            f"- current_gate_accept_count: "
            f"`{summary.get('current_gate_accept_count')}`; "
            "promotion/paper-shadow/production/broker 均 blocked/none。"
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
            "| gate | gate_marginal_utility | gate_failure_mode_reduced | "
            "opportunity_cost | recommended_action |"
        ),
        "|---|---|---|---:|---|",
    ]
    for row in gate_rows:
        lines.append(
            f"|`{row['gate_id']}`|`{row['gate_marginal_utility']}`|"
            f"`{row['gate_failure_mode_reduced']}`|`{row['opportunity_cost']}`|"
            f"`{row['recommended_action']}`|"
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
            "| policy_id | avg_return | avg_abs_drawdown | avg_turnover | utility_proxy |",
            "|---|---:|---:|---:|---:|",
        ]
    )
    for row in candidate_rows:
        if bool(row.get("diagnostic_only_exclusion")):
            continue
        lines.append(
            f"|`{row['policy_id']}`|`{row['average_probe_annual_return']}`|"
            f"`{row['average_abs_max_drawdown']}`|"
            f"`{row['average_probe_turnover']}`|"
            f"`{row['actual_path_utility_proxy']}`|"
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
                "本 audit 只读既有 actual-path / coverage / 2022 slice / "
                "defensive regression evidence，不改变 active selection rule。"
            ),
            "",
        ]
    )
    return "\n".join(lines)


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


def _current_status(row: Mapping[str, Any], gate_id: str) -> bool:
    return bool(mapping(row.get("current_gate_status")).get(gate_id))


def _date_on_or_before(value: object, boundary: str) -> bool:
    return bool(value) and str(value) <= boundary


def _classify_utility(
    *,
    blocked_mean: float,
    blocked_count: int,
    positive_min: float,
    neutral_max: float,
) -> str:
    if blocked_count == 0:
        return "neutral"
    if blocked_mean > positive_min:
        return "negative"
    if abs(blocked_mean) <= neutral_max:
        return "neutral"
    if blocked_mean < -positive_min:
        return "positive"
    return "inconclusive"


def _recommended_action(
    *,
    gate_id: str,
    utility: str,
    material_cost: bool,
    current_accept_count: int,
) -> str:
    if gate_id == "coverage_pass_rule" and utility == "negative":
        return "convert_to_owner_review_evidence_gate"
    if utility == "positive":
        return "keep_current_gate"
    if gate_id == "same_risk_comparison_reported":
        return "retain_as_audit_completeness_gate_not_performance_gate"
    if gate_id in {
        "net_of_cost_not_worse",
        "2022_slice_not_worse_than_flat_reference",
    }:
        return "retain_as_owner_review_audit_gate_pending_more_variation"
    if utility == "negative" and material_cost:
        return "convert_to_owner_review_evidence_gate"
    if current_accept_count == 0:
        return "mark_redundant_monitor_only"
    return "monitor_pending_more_variation"


def _owner_review_candidate(row: Mapping[str, Any]) -> bool:
    failed = set(strings(row.get("failed_current_gates")))
    reviewable = {
        "coverage_pass_rule",
        "same_risk_comparison_reported",
        "net_of_cost_not_worse",
        "2022_slice_not_worse_than_flat_reference",
    }
    return to_float(row.get("actual_path_utility_proxy")) > 0.0 and failed.issubset(
        reviewable
    )


def _mean(values: Sequence[float] | Any) -> float:
    items = [to_float(value) for value in values if value is not None]
    return sum(items) / len(items) if items else 0.0


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
