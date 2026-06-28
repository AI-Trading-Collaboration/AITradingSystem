from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_challenger_matrix_v2 import (
    run_first_layer_challenger_matrix_v2_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_CHALLENGER_MATRIX_PATH,
    run_first_layer_performance_gate_audit_pack,
)
from ai_trading_system.post_2085_research_common import (
    clean_for_yaml,
    mapping,
    records,
    strings,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_boundary_owner_review"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_BOUNDARY_OWNER_REVIEW_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2279_FIRST_LAYER_BOUNDARY_CANDIDATE_OWNER_REVIEW_PACKAGE"
BOUNDARY_CANDIDATES = ("wf_504d_baseline", "wf_378d_initial")


def run_first_layer_boundary_candidate_owner_review_pack(
    *,
    challenger_matrix_path: Path = DEFAULT_CHALLENGER_MATRIX_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    source_root = output_root / "_regenerated_sources"
    source_docs_root = source_root / "docs"
    challenger_v2 = run_first_layer_challenger_matrix_v2_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2278_challenger_matrix_v2",
        docs_root=source_docs_root / "trading_2278_challenger_matrix_v2",
    )
    performance_audit = run_first_layer_performance_gate_audit_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2274_gate_audit",
        docs_root=source_docs_root / "trading_2274_gate_audit",
    )
    performance_rows = {
        str(row.get("policy_id")): row for row in records(performance_audit.get("candidate_rows"))
    }
    challenger_rows = records(challenger_v2.get("candidate_rows"))
    rows_by_id = {str(row.get("candidate_id")): row for row in challenger_rows}

    boundary_rows = [
        _boundary_candidate_row(
            candidate_id=candidate_id,
            v2_row=mapping(rows_by_id.get(candidate_id)),
            performance_row=mapping(performance_rows.get(candidate_id)),
        )
        for candidate_id in BOUNDARY_CANDIDATES
    ]
    offline_summary = _offline_validation_ready_summary(challenger_rows)
    blocked_summary = _blocked_candidate_summary(challenger_rows, performance_rows)
    next_plan = _recommended_next_experiment_plan(boundary_rows, offline_summary, blocked_summary)
    summary = _summary(boundary_rows, offline_summary, blocked_summary, next_plan)
    common = {
        "schema_version": "first_layer_boundary_candidate_owner_review.v1",
        "report_type": "first_layer_boundary_candidate_owner_review",
        "title": "First-Layer Boundary Candidate Owner Review",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": challenger_v2.get("market_regime"),
        "anchor_event": challenger_v2.get("anchor_event"),
        "anchor_date": challenger_v2.get("anchor_date"),
        "requested_start": challenger_v2.get("requested_start"),
        "actual_signal_start": challenger_v2.get("actual_signal_start"),
        "data_quality_status": challenger_v2.get("data_quality_status"),
        "source_generation": {
            "challenger_matrix_v2_source": "regenerated_from_trading_2278_code_path",
            "performance_metric_source": "regenerated_from_trading_2274_code_path",
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "challenger_matrix": str(challenger_matrix_path),
            "regenerated_2278_artifacts": clean_for_yaml(
                dict(mapping(challenger_v2.get("artifact_paths")))
            ),
            "regenerated_2274_artifacts": clean_for_yaml(
                dict(mapping(performance_audit.get("artifact_paths")))
            ),
        },
        "summary": summary,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = {
        **common,
        "boundary_candidate_rows": boundary_rows,
        "offline_validation_ready_candidate_summary": offline_summary,
        "blocked_candidate_failure_reason_summary": blocked_summary,
        "recommended_next_experiment_plan": next_plan,
        "safety_boundary": _safety_boundary(),
    }
    paths = {
        "first_layer_boundary_candidate_owner_review": docs_root
        / "first_layer_boundary_candidate_owner_review.md",
        "boundary_candidate_comparison_matrix": output_root
        / "boundary_candidate_comparison_matrix.json",
        "owner_review_candidate_tradeoff_summary": docs_root
        / "owner_review_candidate_tradeoff_summary.md",
        "offline_validation_ready_candidate_summary": output_root
        / "offline_validation_ready_candidate_summary.json",
        "blocked_candidate_failure_reason_summary": output_root
        / "blocked_candidate_failure_reason_summary.json",
        "recommended_next_experiment_plan": docs_root / "recommended_next_experiment_plan.md",
    }
    write_json(
        paths["boundary_candidate_comparison_matrix"],
        {**common, "boundary_candidate_rows": boundary_rows},
    )
    write_json(
        paths["offline_validation_ready_candidate_summary"],
        {**common, **offline_summary},
    )
    write_json(
        paths["blocked_candidate_failure_reason_summary"],
        {**common, **blocked_summary},
    )
    write_markdown(
        paths["first_layer_boundary_candidate_owner_review"],
        _render_owner_review(payload, paths),
    )
    write_markdown(
        paths["owner_review_candidate_tradeoff_summary"],
        _render_tradeoff_summary(payload, paths),
    )
    write_markdown(
        paths["recommended_next_experiment_plan"],
        _render_next_plan(payload, paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _boundary_candidate_row(
    *,
    candidate_id: str,
    v2_row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _metric_block(v2_row=v2_row, performance_row=performance_row)
    decision = _owner_review_decision(candidate_id, v2_row, performance_row, metrics)
    return {
        "candidate_id": candidate_id,
        "candidate_state": v2_row.get("selection_policy_v2_state"),
        "v1_active_selection_state": v2_row.get("v1_active_selection_state"),
        "candidate_state_transition_from_v1": v2_row.get("candidate_state_transition_from_v1"),
        "gate_policy_v2_state": v2_row.get("gate_policy_v2_state"),
        "risk_flags": strings(v2_row.get("risk_flags")),
        "primary_risk_flag": decision["primary_risk_flag"],
        "metrics": metrics,
        "owner_review_decision": decision["owner_review_decision"],
        "utility_tradeoff_acceptable": decision["utility_tradeoff_acceptable"],
        "decision_rationale": decision["decision_rationale"],
        "recommended_next_action": decision["recommended_next_action"],
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _metric_block(
    *,
    v2_row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
) -> dict[str, Any]:
    rank_features = mapping(v2_row.get("rank_features"))
    current_gate_status = mapping(performance_row.get("current_gate_status"))
    unavailable = "unavailable_for_frozen_actual_path_policy_rows"
    dependency_unavailable = "unavailable_candidate_level_beta_tqqq_dependency_not_run"
    return {
        "actual_path_utility": performance_row.get(
            "actual_path_utility_proxy", v2_row.get("utility")
        ),
        "net_return": performance_row.get("average_probe_annual_return"),
        "excess_return_delta": performance_row.get("net_return_delta"),
        "max_drawdown": performance_row.get("average_abs_max_drawdown"),
        "max_drawdown_delta": performance_row.get(
            "max_drawdown_delta", rank_features.get("drawdown_delta")
        ),
        "calmar": performance_row.get("average_probe_calmar"),
        "calmar_delta": performance_row.get("calmar_delta"),
        "sharpe": performance_row.get("average_probe_sharpe"),
        "sharpe_delta": performance_row.get("sharpe_delta"),
        "turnover": performance_row.get("average_probe_turnover"),
        "turnover_delta": performance_row.get(
            "turnover_delta", rank_features.get("turnover_delta")
        ),
        "net_of_cost_impact": performance_row.get("cost_adjusted_return_delta"),
        "false_risk_on": performance_row.get("false_risk_on_delta"),
        "false_risk_on_status": unavailable,
        "false_risk_off": performance_row.get("false_risk_off_delta"),
        "false_risk_off_status": unavailable,
        "defensive_probe_result": {
            "no_major_regression": current_gate_status.get(
                "no_major_regression_in_defensive_probe"
            ),
            "defensive_role_regression_count": performance_row.get(
                "defensive_role_regression_count"
            ),
        },
        "stress_2022_slice": {
            "covered_2022": performance_row.get("covered_2022"),
            "covered_2022_risk_off_window": performance_row.get("covered_2022_risk_off_window"),
            "covered_2022_recovery_window": performance_row.get("covered_2022_recovery_window"),
            "prediction_count_2022": performance_row.get("prediction_count_2022"),
            "stress_2022_slice_delta": performance_row.get(
                "stress_2022_slice_delta", rank_features.get("stress_2022_slice_delta")
            ),
        },
        "dependency_2023_plus": {
            "depends_on_2023_plus": rank_features.get("depends_on_2023_plus"),
            "first_prediction_date": performance_row.get("first_prediction_date"),
            "risk_flag_present": "2023_plus_dependency" in strings(v2_row.get("risk_flags")),
        },
        "beta_attribution": performance_row.get("beta_dependency_delta"),
        "beta_attribution_status": dependency_unavailable,
        "tqqq_attribution": performance_row.get("tqqq_dependency_delta"),
        "tqqq_attribution_status": dependency_unavailable,
        "benchmark_consistency": performance_row.get("benchmark_consistency_delta"),
        "benchmark_consistency_status": unavailable,
        "recovery_delay": performance_row.get("recovery_delay_delta"),
        "recovery_delay_status": unavailable,
        "drawdown_warning_lead_time": performance_row.get("drawdown_warning_lead_time_delta"),
        "drawdown_warning_lead_time_status": unavailable,
        "actual_path_improved_probe_count": performance_row.get("actual_path_improved_probe_count"),
        "probe_count": performance_row.get("probe_count"),
    }


def _owner_review_decision(
    candidate_id: str,
    v2_row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> dict[str, Any]:
    risk_flags = strings(v2_row.get("risk_flags"))
    if candidate_id == "wf_504d_baseline":
        return {
            "owner_review_decision": "expand_neighborhood",
            "primary_risk_flag": "2023_plus_dependency",
            "utility_tradeoff_acceptable": "inconclusive",
            "recommended_next_action": "expand_wf_504d_neighborhood_with_2022_coverage_constraint",
            "decision_rationale": (
                "actual-path utility 最高，但 first_prediction_date 在 2023-02-22，"
                "2022 coverage 缺失；应继续研究邻域而不是 promotion。"
            ),
        }
    if candidate_id == "wf_378d_initial":
        primary = "coverage_rule_not_satisfied" if risk_flags else "none"
        return {
            "owner_review_decision": "continue_research",
            "primary_risk_flag": primary,
            "utility_tradeoff_acceptable": True,
            "recommended_next_action": "use_as_safer_first_layer_challenger_family_baseline",
            "decision_rationale": (
                "utility 低于 wf_504d_baseline，但覆盖 2022 risk-off/recovery，"
                "且 defensive probe 无 major regression，适合作为安全基线。"
            ),
        }
    if (
        mapping(performance_row.get("current_gate_status")).get(
            "no_major_regression_in_defensive_probe"
        )
        is False
    ):
        return {
            "owner_review_decision": "downgrade_to_blocked",
            "primary_risk_flag": "defensive_regression",
            "utility_tradeoff_acceptable": False,
            "recommended_next_action": "do_not_expand_until_defensive_regression_resolved",
            "decision_rationale": (
                "defensive probe regression remains a strong performance blocker."
            ),
        }
    return {
        "owner_review_decision": "inconclusive",
        "primary_risk_flag": risk_flags[0] if risk_flags else "none",
        "utility_tradeoff_acceptable": "inconclusive",
        "recommended_next_action": "more_candidate_level_evidence_required",
        "decision_rationale": "insufficient boundary-candidate evidence.",
    }


def _offline_validation_ready_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    offline_rows = [
        row for row in rows if row.get("selection_policy_v2_state") == "OFFLINE_VALIDATION_READY"
    ]
    objective_counter: Counter[str] = Counter()
    for row in offline_rows:
        objective_counter.update(
            strings(mapping(row.get("rank_features")).get("target_objective_terms"))
        )
    return {
        "candidate_count": len(offline_rows),
        "candidate_ids": [str(row.get("candidate_id")) for row in offline_rows],
        "common_characteristics": [
            "offline_challenger_experiment_only_not_promotion",
            "actual_path_utility_unavailable",
            "promotion_allowed_false",
            "requires_future_candidate_level_actual_path_backtest",
        ],
        "target_objective_term_counts": dict(sorted(objective_counter.items())),
        "misclassification_check": "no_obvious_misclassification_detected",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _blocked_candidate_summary(
    rows: Sequence[Mapping[str, Any]],
    performance_rows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    blocked_rows = [row for row in rows if row.get("selection_policy_v2_state") == "BLOCKED"]
    reason_counter: Counter[str] = Counter()
    blocked_details = []
    for row in blocked_rows:
        candidate_id = str(row.get("candidate_id"))
        performance_row = mapping(performance_rows.get(candidate_id))
        reasons = strings(row.get("gate_policy_v2_blocked_reasons"))
        if not reasons:
            reasons = strings(performance_row.get("failed_current_gates"))
        if not reasons:
            reasons = strings(mapping(row.get("rank_features")).get("missing_proxy_ids"))
        reason_counter.update(reasons or ["unspecified_blocker"])
        blocked_details.append(
            {
                "candidate_id": candidate_id,
                "candidate_source": row.get("candidate_source"),
                "blocked_reasons": reasons or ["unspecified_blocker"],
                "defensive_probe_result": mapping(performance_row.get("current_gate_status")).get(
                    "no_major_regression_in_defensive_probe"
                ),
                "utility": performance_row.get("actual_path_utility_proxy", row.get("utility")),
                "misclassification_risk": _blocked_misclassification_risk(row, performance_row),
                "promotion_allowed": False,
            }
        )
    return {
        "candidate_count": len(blocked_rows),
        "reason_counts": dict(sorted(reason_counter.items())),
        "blocked_candidate_rows": blocked_details,
        "misclassification_check": _blocked_overall_misclassification(blocked_details),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _blocked_misclassification_risk(
    row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
) -> str:
    if row.get("candidate_source") == "offline_challenger_experiment":
        return "blocked_due_missing_proxy_inputs_reasonable"
    if (
        mapping(performance_row.get("current_gate_status")).get(
            "no_major_regression_in_defensive_probe"
        )
        is False
    ):
        return "blocked_due_strong_defensive_regression_reasonable"
    return "review_required"


def _blocked_overall_misclassification(details: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("misclassification_risk") == "review_required" for row in details):
        return "possible_misclassification_requires_owner_review"
    return "no_obvious_misclassification_detected"


def _recommended_next_experiment_plan(
    boundary_rows: Sequence[Mapping[str, Any]],
    offline_summary: Mapping[str, Any],
    blocked_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "plan_id": "first_layer_boundary_candidate_next_experiment_plan_v1",
        "recommended_actions": [
            {
                "action_id": "expand_wf_504d_neighborhood",
                "priority": "P0",
                "reason": "highest actual-path utility but 2023+ dependency remains unresolved",
                "acceptance_criteria": [
                    "include 2022 coverage constraint",
                    "retain no_major_regression_in_defensive_probe as strong gate",
                    "report candidate-level beta/TQQQ and benchmark consistency if available",
                ],
            },
            {
                "action_id": "use_wf_378d_as_safety_baseline",
                "priority": "P0",
                "reason": "lower utility but better 2022 and defensive coverage",
                "acceptance_criteria": [
                    "compare against expanded wf_504d neighborhood",
                    "do not convert research accepted to paper-shadow",
                ],
            },
            {
                "action_id": "run_offline_ready_candidate_actual_path_backtests",
                "priority": "P1",
                "reason": (
                    f"{offline_summary.get('candidate_count')} offline-ready rows "
                    "lack actual-path utility"
                ),
                "acceptance_criteria": [
                    "complete two-layer actual-path utility",
                    "false risk-on/off and recovery-delay objective terms",
                ],
            },
        ],
        "blocked_candidate_policy": blocked_summary.get("misclassification_check"),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _summary(
    boundary_rows: Sequence[Mapping[str, Any]],
    offline_summary: Mapping[str, Any],
    blocked_summary: Mapping[str, Any],
    next_plan: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "boundary_candidate_count": len(boundary_rows),
        "offline_validation_ready_count": offline_summary.get("candidate_count"),
        "blocked_candidate_count": blocked_summary.get("candidate_count"),
        "wf_504d_owner_review_decision": _decision_for(boundary_rows, "wf_504d_baseline"),
        "wf_378d_owner_review_decision": _decision_for(boundary_rows, "wf_378d_initial"),
        "recommended_next_action_count": len(records(next_plan.get("recommended_actions"))),
        "misclassification_check": blocked_summary.get("misclassification_check"),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _decision_for(rows: Sequence[Mapping[str, Any]], candidate_id: str) -> str:
    for row in rows:
        if row.get("candidate_id") == candidate_id:
            return str(row.get("owner_review_decision"))
    return "missing"


def _safety_boundary() -> dict[str, Any]:
    return {
        "owner_review_required_is_not_promotion": True,
        "research_accepted_is_not_paper_shadow": True,
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_owner_review(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    lines = [
        "# First-Layer Boundary Candidate Owner Review",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        "- OWNER_REVIEW_REQUIRED 不等于 promotion；RESEARCH_ACCEPTED 不等于 paper-shadow。",
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "## Boundary Candidate Decisions",
        "",
        (
            "| candidate | state | utility | owner_review_decision | "
            "primary_risk_flag | utility_tradeoff_acceptable |"
        ),
        "|---|---|---:|---|---|---|",
    ]
    for row in records(payload.get("boundary_candidate_rows")):
        metrics = mapping(row.get("metrics"))
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('candidate_state')}`|"
            f"{metrics.get('actual_path_utility')}|`{row.get('owner_review_decision')}`|"
            f"`{row.get('primary_risk_flag')}`|"
            f"`{row.get('utility_tradeoff_acceptable')}`|"
        )
    lines.extend(
        [
            "",
            "## Return / Risk Metrics",
            "",
            (
                "| candidate | net_return | excess_return_delta | max_drawdown | "
                "Calmar | Sharpe | turnover | net_of_cost_impact |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in records(payload.get("boundary_candidate_rows")):
        metrics = mapping(row.get("metrics"))
        lines.append(
            f"|`{row.get('candidate_id')}`|`{metrics.get('net_return')}`|"
            f"`{metrics.get('excess_return_delta')}`|`{metrics.get('max_drawdown')}`|"
            f"`{metrics.get('calmar')}`|`{metrics.get('sharpe')}`|"
            f"`{metrics.get('turnover')}`|`{metrics.get('net_of_cost_impact')}`|"
        )
    lines.extend(
        [
            "",
            "## Objective / Dependency Metrics",
            "",
            (
                "| candidate | false_risk_on | false_risk_off | defensive_probe | "
                "2022_stress_slice | 2023+ dependency | beta/TQQQ attribution | "
                "benchmark_consistency | recovery_delay | drawdown_warning_lead_time |"
            ),
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in records(payload.get("boundary_candidate_rows")):
        metrics = mapping(row.get("metrics"))
        defensive = mapping(metrics.get("defensive_probe_result"))
        stress = mapping(metrics.get("stress_2022_slice"))
        dependency = mapping(metrics.get("dependency_2023_plus"))
        lines.append(
            f"|`{row.get('candidate_id')}`|"
            f"`{metrics.get('false_risk_on_status')}`|"
            f"`{metrics.get('false_risk_off_status')}`|"
            f"`no_major_regression={defensive.get('no_major_regression')}; "
            f"count={defensive.get('defensive_role_regression_count')}`|"
            f"`covered_2022={stress.get('covered_2022')}; "
            f"delta={stress.get('stress_2022_slice_delta')}`|"
            f"`depends={dependency.get('depends_on_2023_plus')}; "
            f"first_prediction={dependency.get('first_prediction_date')}`|"
            f"`{metrics.get('beta_attribution_status')}; "
            f"{metrics.get('tqqq_attribution_status')}`|"
            f"`{metrics.get('benchmark_consistency_status')}`|"
            f"`{metrics.get('recovery_delay_status')}`|"
            f"`{metrics.get('drawdown_warning_lead_time_status')}`|"
        )
    offline_summary = mapping(payload.get("offline_validation_ready_candidate_summary"))
    blocked_summary = mapping(payload.get("blocked_candidate_failure_reason_summary"))
    lines.extend(
        [
            "",
            "## Offline Validation Ready Summary",
            "",
            f"- candidate_count: `{offline_summary.get('candidate_count')}`",
            "- candidate_ids: "
            + ", ".join(
                f"`{candidate_id}`"
                for candidate_id in strings(offline_summary.get("candidate_ids"))
            ),
            "- common_characteristics: "
            + ", ".join(
                f"`{item}`" for item in strings(offline_summary.get("common_characteristics"))
            ),
            "",
            "## Blocked Candidate Failure Reasons",
            "",
            f"- candidate_count: `{blocked_summary.get('candidate_count')}`",
            f"- misclassification_check: `{blocked_summary.get('misclassification_check')}`",
            "",
            "| candidate | blocked_reasons | utility | defensive_probe | misclassification_risk |",
            "|---|---|---:|---|---|",
        ]
    )
    for row in records(blocked_summary.get("blocked_candidate_rows")):
        reasons = ", ".join(f"`{reason}`" for reason in strings(row.get("blocked_reasons")))
        lines.append(
            f"|`{row.get('candidate_id')}`|{reasons}|`{row.get('utility')}`|"
            f"`{row.get('defensive_probe_result')}`|`{row.get('misclassification_risk')}`|"
        )
    lines.extend(
        [
            "",
            "## 指标说明",
            "",
            (
                "false risk-on/off、beta/TQQQ attribution、benchmark consistency、"
                "recovery delay 和 drawdown warning lead time 当前没有 candidate-level "
                "actual-path 数值；本 package 保留 unavailable status，不补造 0。"
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


def _render_tradeoff_summary(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    lines = [
        "# Owner Review Candidate Tradeoff Summary",
        "",
        "| candidate | decision | rationale | next_action | promotion_ready |",
        "|---|---|---|---|---|",
    ]
    for row in records(payload.get("boundary_candidate_rows")):
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('owner_review_decision')}`|"
            f"{row.get('decision_rationale')}|`{row.get('recommended_next_action')}`|"
            f"`{row.get('promotion_ready')}`|"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            (
                "这些 decision 是 owner-review research routing，"
                "不是 promotion / paper-shadow decision。"
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


def _render_next_plan(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    plan = mapping(payload.get("recommended_next_experiment_plan"))
    lines = [
        "# Recommended Next Experiment Plan",
        "",
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "| action | priority | reason |",
        "|---|---|---|",
    ]
    for row in records(plan.get("recommended_actions")):
        lines.append(f"|`{row.get('action_id')}`|`{row.get('priority')}`|{row.get('reason')}|")
    lines.extend(
        [
            "",
            "## Blocked Candidate Policy",
            "",
            f"- `{plan.get('blocked_candidate_policy')}`",
            "",
            "## 产物",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)
