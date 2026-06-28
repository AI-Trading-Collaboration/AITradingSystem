from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_boundary_candidate_owner_review import (
    run_first_layer_boundary_candidate_owner_review_pack,
)
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
    to_float,
    write_json,
    write_markdown,
)

DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_candidate_actual_path_validation"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_CANDIDATE_ACTUAL_PATH_VALIDATION_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2280_FIRST_LAYER_CANDIDATE_LEVEL_ACTUAL_PATH_VALIDATION"
MISSING_SIGNAL_STATUS = "missing_candidate_signal_artifact"
UNAVAILABLE_FROZEN_STATUS = "unavailable_for_frozen_actual_path_policy_rows"
UNAVAILABLE_DEPENDENCY_STATUS = "unavailable_candidate_level_beta_tqqq_dependency_not_run"


def _display(value: Any, *, status: str | None = None) -> Any:
    if value is None:
        return status or "unavailable"
    return value


def run_first_layer_candidate_actual_path_validation_pack(
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
    boundary_review = run_first_layer_boundary_candidate_owner_review_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2279_boundary_owner_review",
        docs_root=source_docs_root / "trading_2279_boundary_owner_review",
    )
    performance_audit = run_first_layer_performance_gate_audit_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2274_gate_audit",
        docs_root=source_docs_root / "trading_2274_gate_audit",
    )
    performance_rows = {
        str(row.get("policy_id")): row for row in records(performance_audit.get("candidate_rows"))
    }
    boundary_rows = {
        str(row.get("candidate_id")): row
        for row in records(boundary_review.get("boundary_candidate_rows"))
    }
    source_rows = [
        row
        for row in records(challenger_v2.get("candidate_rows"))
        if row.get("selection_policy_v2_state") != "BLOCKED"
    ]
    validation_rows = [
        _candidate_validation_row(
            v2_row=row,
            performance_row=mapping(performance_rows.get(str(row.get("candidate_id")))),
            boundary_row=mapping(boundary_rows.get(str(row.get("candidate_id")))),
        )
        for row in source_rows
    ]
    validation_rows = _assign_utility_ranks(validation_rows)
    risk_rows = [_risk_attribution_row(row) for row in validation_rows]
    state_reclassification = _state_reclassification(validation_rows)
    updated_research_queue = _updated_queue(validation_rows, {"RESEARCH_ACCEPTED"})
    updated_owner_review_queue = _updated_queue(validation_rows, {"OWNER_REVIEW_REQUIRED"})
    updated_offline_queue = _updated_offline_queue(validation_rows)
    summary = _summary(
        validation_rows,
        updated_research_queue,
        updated_owner_review_queue,
        updated_offline_queue,
    )
    common = {
        "schema_version": "first_layer_candidate_actual_path_validation.v1",
        "report_type": "first_layer_candidate_actual_path_validation",
        "title": "First-Layer Candidate-Level Actual-Path Validation",
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
            "boundary_review_source": "regenerated_from_trading_2279_code_path",
            "performance_metric_source": "regenerated_from_trading_2274_code_path",
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "challenger_matrix": str(challenger_matrix_path),
            "regenerated_2278_artifacts": clean_for_yaml(
                dict(mapping(challenger_v2.get("artifact_paths")))
            ),
            "regenerated_2279_artifacts": clean_for_yaml(
                dict(mapping(boundary_review.get("artifact_paths")))
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
        "candidate_rows": validation_rows,
        "candidate_risk_attribution_rows": risk_rows,
        "candidate_state_reclassification_rows": state_reclassification,
        "updated_research_candidate_queue": updated_research_queue,
        "updated_owner_review_queue": updated_owner_review_queue,
        "updated_offline_validation_queue": updated_offline_queue,
        "safety_boundary": _safety_boundary(),
    }
    paths = {
        "candidate_actual_path_validation_report": docs_root
        / "candidate_actual_path_validation_report.md",
        "candidate_actual_path_matrix": output_root / "candidate_actual_path_matrix.json",
        "candidate_risk_attribution_matrix": output_root / "candidate_risk_attribution_matrix.json",
        "candidate_state_reclassification_report": docs_root
        / "candidate_state_reclassification_report.md",
        "updated_research_candidate_queue": output_root / "updated_research_candidate_queue.json",
        "updated_owner_review_queue": output_root / "updated_owner_review_queue.json",
        "updated_offline_validation_queue": output_root / "updated_offline_validation_queue.json",
    }
    write_json(paths["candidate_actual_path_matrix"], {**common, "candidate_rows": validation_rows})
    write_json(
        paths["candidate_risk_attribution_matrix"],
        {**common, "candidate_risk_attribution_rows": risk_rows},
    )
    write_json(
        paths["updated_research_candidate_queue"],
        {**common, "updated_research_candidate_queue": updated_research_queue},
    )
    write_json(
        paths["updated_owner_review_queue"],
        {**common, "updated_owner_review_queue": updated_owner_review_queue},
    )
    write_json(
        paths["updated_offline_validation_queue"],
        {**common, "updated_offline_validation_queue": updated_offline_queue},
    )
    write_markdown(
        paths["candidate_actual_path_validation_report"],
        _render_validation_report(payload, paths),
    )
    write_markdown(
        paths["candidate_state_reclassification_report"],
        _render_state_reclassification_report(payload, paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _candidate_validation_row(
    *,
    v2_row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
    boundary_row: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = str(v2_row.get("candidate_id"))
    previous_state = str(v2_row.get("selection_policy_v2_state"))
    has_actual_path = bool(performance_row)
    metrics = (
        _actual_path_metrics(v2_row=v2_row, performance_row=performance_row)
        if has_actual_path
        else _missing_actual_path_metrics()
    )
    if has_actual_path:
        updated_state = _updated_state_for_actual_path(previous_state, metrics)
        validation_status = "candidate_level_actual_path_available"
        validation_blockers: list[str] = []
        primary_risk_flag = _primary_risk_flag(v2_row, boundary_row)
        continue_research = updated_state in {"RESEARCH_ACCEPTED", "OWNER_REVIEW_REQUIRED"}
        expand_neighborhood = candidate_id == "wf_504d_baseline" and continue_research
        reclassification_reason = "state_stable_after_candidate_level_actual_path_validation"
    else:
        updated_state = "INCONCLUSIVE"
        validation_status = MISSING_SIGNAL_STATUS
        validation_blockers = [
            "candidate_signal_artifact_missing",
            "candidate_actual_path_backtest_not_run",
        ]
        primary_risk_flag = MISSING_SIGNAL_STATUS
        continue_research = False
        expand_neighborhood = False
        reclassification_reason = "offline_ready_reclassified_missing_candidate_signal_artifact"
    return {
        "candidate_id": candidate_id,
        "candidate_source": v2_row.get("candidate_source"),
        "previous_state": previous_state,
        "updated_state": updated_state,
        "state_transition": f"{previous_state} -> {updated_state}",
        "state_transition_stability": _state_transition_stability(
            previous_state,
            updated_state,
            validation_status,
        ),
        "reclassification_reason": reclassification_reason,
        "validation_status": validation_status,
        "validation_blockers": validation_blockers,
        "gate_policy_v2_state": v2_row.get("gate_policy_v2_state"),
        "active_selection_policy_v2_applied": True,
        "gate_policy_v2_applied": True,
        "risk_flags": strings(v2_row.get("risk_flags")),
        "primary_risk_flag": primary_risk_flag,
        "metrics": metrics,
        "utility_rank": None,
        "continue_research": continue_research,
        "expand_neighborhood": expand_neighborhood,
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _actual_path_metrics(
    *,
    v2_row: Mapping[str, Any],
    performance_row: Mapping[str, Any],
) -> dict[str, Any]:
    rank_features = mapping(v2_row.get("rank_features"))
    current_gate_status = mapping(performance_row.get("current_gate_status"))
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
        "false_risk_on_status": UNAVAILABLE_FROZEN_STATUS,
        "false_risk_off": performance_row.get("false_risk_off_delta"),
        "false_risk_off_status": UNAVAILABLE_FROZEN_STATUS,
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
        "beta_attribution_status": UNAVAILABLE_DEPENDENCY_STATUS,
        "tqqq_attribution": performance_row.get("tqqq_dependency_delta"),
        "tqqq_attribution_status": UNAVAILABLE_DEPENDENCY_STATUS,
        "benchmark_consistency": performance_row.get("benchmark_consistency_delta"),
        "benchmark_consistency_status": UNAVAILABLE_FROZEN_STATUS,
        "recovery_delay": performance_row.get("recovery_delay_delta"),
        "recovery_delay_status": UNAVAILABLE_FROZEN_STATUS,
        "drawdown_warning_lead_time": performance_row.get("drawdown_warning_lead_time_delta"),
        "drawdown_warning_lead_time_status": UNAVAILABLE_FROZEN_STATUS,
        "actual_path_improved_probe_count": performance_row.get("actual_path_improved_probe_count"),
        "probe_count": performance_row.get("probe_count"),
    }


def _missing_actual_path_metrics() -> dict[str, Any]:
    fields = (
        "actual_path_utility",
        "net_return",
        "excess_return_delta",
        "max_drawdown",
        "max_drawdown_delta",
        "calmar",
        "calmar_delta",
        "sharpe",
        "sharpe_delta",
        "turnover",
        "turnover_delta",
        "net_of_cost_impact",
        "false_risk_on",
        "false_risk_off",
        "beta_attribution",
        "tqqq_attribution",
        "benchmark_consistency",
        "recovery_delay",
        "drawdown_warning_lead_time",
        "actual_path_improved_probe_count",
        "probe_count",
    )
    metrics = {field: None for field in fields}
    metrics.update(
        {
            "actual_path_status": MISSING_SIGNAL_STATUS,
            "metric_status": MISSING_SIGNAL_STATUS,
            "false_risk_on_status": MISSING_SIGNAL_STATUS,
            "false_risk_off_status": MISSING_SIGNAL_STATUS,
            "defensive_probe_result": {
                "no_major_regression": None,
                "defensive_role_regression_count": None,
                "status": MISSING_SIGNAL_STATUS,
            },
            "stress_2022_slice": {
                "covered_2022": None,
                "covered_2022_risk_off_window": None,
                "covered_2022_recovery_window": None,
                "prediction_count_2022": None,
                "stress_2022_slice_delta": None,
                "status": MISSING_SIGNAL_STATUS,
            },
            "dependency_2023_plus": {
                "depends_on_2023_plus": None,
                "first_prediction_date": None,
                "risk_flag_present": None,
                "status": MISSING_SIGNAL_STATUS,
            },
            "beta_attribution_status": MISSING_SIGNAL_STATUS,
            "tqqq_attribution_status": MISSING_SIGNAL_STATUS,
            "benchmark_consistency_status": MISSING_SIGNAL_STATUS,
            "recovery_delay_status": MISSING_SIGNAL_STATUS,
            "drawdown_warning_lead_time_status": MISSING_SIGNAL_STATUS,
        }
    )
    return metrics


def _updated_state_for_actual_path(previous_state: str, metrics: Mapping[str, Any]) -> str:
    defensive = mapping(metrics.get("defensive_probe_result"))
    if defensive.get("no_major_regression") is False:
        return "BLOCKED"
    if previous_state in {"RESEARCH_ACCEPTED", "OWNER_REVIEW_REQUIRED"}:
        return previous_state
    return "INCONCLUSIVE"


def _primary_risk_flag(v2_row: Mapping[str, Any], boundary_row: Mapping[str, Any]) -> str:
    boundary_flag = str(boundary_row.get("primary_risk_flag") or "")
    if boundary_flag:
        return boundary_flag
    flags = strings(v2_row.get("risk_flags"))
    return flags[0] if flags else "none"


def _state_transition_stability(
    previous_state: str,
    updated_state: str,
    validation_status: str,
) -> str:
    if validation_status == MISSING_SIGNAL_STATUS:
        return "unstable_missing_candidate_signal_artifact"
    if previous_state == updated_state:
        return "stable"
    return "reclassified_after_candidate_level_actual_path_validation"


def _assign_utility_ranks(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [dict(row) for row in rows]
    rows_with_utility = [
        row
        for row in sorted(
            result,
            key=lambda item: -to_float(mapping(item.get("metrics")).get("actual_path_utility")),
        )
        if mapping(row.get("metrics")).get("actual_path_utility") is not None
    ]
    ranks = {str(row.get("candidate_id")): index + 1 for index, row in enumerate(rows_with_utility)}
    for row in result:
        row["utility_rank"] = ranks.get(str(row.get("candidate_id")))
    return result


def _risk_attribution_row(row: Mapping[str, Any]) -> dict[str, Any]:
    metrics = mapping(row.get("metrics"))
    return {
        "candidate_id": row.get("candidate_id"),
        "previous_state": row.get("previous_state"),
        "updated_state": row.get("updated_state"),
        "validation_status": row.get("validation_status"),
        "primary_risk_flag": row.get("primary_risk_flag"),
        "false_risk_on": metrics.get("false_risk_on"),
        "false_risk_on_status": metrics.get("false_risk_on_status"),
        "false_risk_off": metrics.get("false_risk_off"),
        "false_risk_off_status": metrics.get("false_risk_off_status"),
        "defensive_probe_result": metrics.get("defensive_probe_result"),
        "stress_2022_slice": metrics.get("stress_2022_slice"),
        "dependency_2023_plus": metrics.get("dependency_2023_plus"),
        "beta_attribution": metrics.get("beta_attribution"),
        "beta_attribution_status": metrics.get("beta_attribution_status"),
        "tqqq_attribution": metrics.get("tqqq_attribution"),
        "tqqq_attribution_status": metrics.get("tqqq_attribution_status"),
        "benchmark_consistency": metrics.get("benchmark_consistency"),
        "benchmark_consistency_status": metrics.get("benchmark_consistency_status"),
        "recovery_delay": metrics.get("recovery_delay"),
        "recovery_delay_status": metrics.get("recovery_delay_status"),
        "drawdown_warning_lead_time": metrics.get("drawdown_warning_lead_time"),
        "drawdown_warning_lead_time_status": metrics.get("drawdown_warning_lead_time_status"),
        "validation_blockers": row.get("validation_blockers"),
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _state_reclassification(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": row.get("candidate_id"),
            "previous_state": row.get("previous_state"),
            "updated_state": row.get("updated_state"),
            "state_transition": row.get("state_transition"),
            "state_transition_stability": row.get("state_transition_stability"),
            "reclassification_reason": row.get("reclassification_reason"),
            "utility_rank": row.get("utility_rank"),
            "primary_risk_flag": row.get("primary_risk_flag"),
            "continue_research": row.get("continue_research"),
            "expand_neighborhood": row.get("expand_neighborhood"),
            "promotion_ready": False,
        }
        for row in rows
    ]


def _updated_queue(rows: Sequence[Mapping[str, Any]], states: set[str]) -> dict[str, Any]:
    queue_rows = [
        _queue_row(row)
        for row in sorted(
            [row for row in rows if row.get("updated_state") in states],
            key=_queue_sort_key,
        )
    ]
    return {
        "candidate_count": len(queue_rows),
        "candidate_rows": queue_rows,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _updated_offline_queue(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    queue_rows = [
        _queue_row(row)
        for row in sorted(
            [row for row in rows if row.get("updated_state") == "OFFLINE_VALIDATION_READY"],
            key=_queue_sort_key,
        )
    ]
    reclassified = [
        _queue_row(row)
        for row in sorted(
            [
                row
                for row in rows
                if row.get("previous_state") == "OFFLINE_VALIDATION_READY"
                and row.get("updated_state") != "OFFLINE_VALIDATION_READY"
            ],
            key=lambda item: str(item.get("candidate_id")),
        )
    ]
    return {
        "candidate_count": len(queue_rows),
        "candidate_rows": queue_rows,
        "reclassified_from_offline_validation_ready_count": len(reclassified),
        "reclassified_from_offline_validation_ready_rows": reclassified,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _queue_row(row: Mapping[str, Any]) -> dict[str, Any]:
    metrics = mapping(row.get("metrics"))
    return {
        "candidate_id": row.get("candidate_id"),
        "previous_state": row.get("previous_state"),
        "updated_state": row.get("updated_state"),
        "utility_rank": row.get("utility_rank"),
        "actual_path_utility": metrics.get("actual_path_utility"),
        "primary_risk_flag": row.get("primary_risk_flag"),
        "validation_status": row.get("validation_status"),
        "validation_blockers": row.get("validation_blockers"),
        "continue_research": row.get("continue_research"),
        "expand_neighborhood": row.get("expand_neighborhood"),
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _queue_sort_key(row: Mapping[str, Any]) -> tuple[int, float, str]:
    utility_rank = row.get("utility_rank")
    rank = 9999 if utility_rank is None else int(utility_rank)
    utility = mapping(row.get("metrics")).get("actual_path_utility")
    score = -9999.0 if utility is None else to_float(utility)
    return (rank, -score, str(row.get("candidate_id")))


def _summary(
    rows: Sequence[Mapping[str, Any]],
    research_queue: Mapping[str, Any],
    owner_queue: Mapping[str, Any],
    offline_queue: Mapping[str, Any],
) -> dict[str, Any]:
    state_counts = Counter(str(row.get("updated_state")) for row in rows)
    available = [
        row
        for row in rows
        if row.get("validation_status") == "candidate_level_actual_path_available"
    ]
    missing = [row for row in rows if row.get("validation_status") == MISSING_SIGNAL_STATUS]
    best = min(
        [row for row in rows if row.get("utility_rank") is not None],
        key=lambda row: int(row["utility_rank"]),
        default=None,
    )
    return {
        "task_id": TASK_ID,
        "covered_candidate_count": len(rows),
        "candidate_level_actual_path_available_count": len(available),
        "missing_candidate_signal_artifact_count": len(missing),
        "updated_state_counts": dict(sorted(state_counts.items())),
        "updated_research_candidate_count": research_queue.get("candidate_count"),
        "updated_owner_review_count": owner_queue.get("candidate_count"),
        "updated_offline_validation_count": offline_queue.get("candidate_count"),
        "offline_ready_reclassified_count": offline_queue.get(
            "reclassified_from_offline_validation_ready_count"
        ),
        "best_actual_path_candidate": _best_summary(best),
        "promotion_ready_count": sum(1 for row in rows if row.get("promotion_ready") is True),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _best_summary(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    metrics = mapping(row.get("metrics"))
    return {
        "candidate_id": row.get("candidate_id"),
        "updated_state": row.get("updated_state"),
        "actual_path_utility": metrics.get("actual_path_utility"),
        "utility_rank": row.get("utility_rank"),
        "primary_risk_flag": row.get("primary_risk_flag"),
        "promotion_ready": False,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "research_accepted_is_not_promotion": True,
        "owner_review_required_is_not_promotion": True,
        "offline_validation_ready_is_not_paper_shadow": True,
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _render_validation_report(payload: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# Candidate-Level Actual-Path Validation",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        (
            f"- covered_candidate_count=`{summary.get('covered_candidate_count')}`; "
            "candidate_level_actual_path_available_count="
            f"`{summary.get('candidate_level_actual_path_available_count')}`; "
            "missing_candidate_signal_artifact_count="
            f"`{summary.get('missing_candidate_signal_artifact_count')}`."
        ),
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "## 候选验证矩阵",
        "",
        (
            "| candidate | previous_state | updated_state | utility_rank | utility | "
            "primary_risk_flag | continue | expand | validation_status |"
        ),
        "|---|---|---|---:|---:|---|---|---|---|",
    ]
    for row in records(payload.get("candidate_rows")):
        metrics = mapping(row.get("metrics"))
        validation_status = str(row.get("validation_status") or "unavailable")
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('previous_state')}`|"
            f"`{row.get('updated_state')}`|"
            f"`{_display(row.get('utility_rank'), status=validation_status)}`|"
            f"`{_display(metrics.get('actual_path_utility'), status=validation_status)}`|"
            f"`{row.get('primary_risk_flag')}`|"
            f"`{row.get('continue_research')}`|`{row.get('expand_neighborhood')}`|"
            f"`{validation_status}`|"
        )
    lines.extend(
        [
            "",
            "## Actual-Path 指标",
            "",
            (
                "| candidate | net_return | excess_return_delta | max_drawdown | "
                "Calmar | Sharpe | turnover | net_of_cost_impact | defensive_probe | 2022 | 2023+ |"
            ),
            "|---|---:|---:|---:|---:|---:|---:|---:|---|---|---|",
        ]
    )
    for row in records(payload.get("candidate_rows")):
        metrics = mapping(row.get("metrics"))
        defensive = mapping(metrics.get("defensive_probe_result"))
        stress = mapping(metrics.get("stress_2022_slice"))
        dependency = mapping(metrics.get("dependency_2023_plus"))
        validation_status = str(row.get("validation_status") or "unavailable")
        lines.append(
            f"|`{row.get('candidate_id')}`|"
            f"`{_display(metrics.get('net_return'), status=validation_status)}`|"
            f"`{_display(metrics.get('excess_return_delta'), status=validation_status)}`|"
            f"`{_display(metrics.get('max_drawdown'), status=validation_status)}`|"
            f"`{_display(metrics.get('calmar'), status=validation_status)}`|"
            f"`{_display(metrics.get('sharpe'), status=validation_status)}`|"
            f"`{_display(metrics.get('turnover'), status=validation_status)}`|"
            f"`{_display(metrics.get('net_of_cost_impact'), status=validation_status)}`|"
            f"`no_major_regression="
            f"{_display(defensive.get('no_major_regression'), status=validation_status)}`|"
            f"`covered_2022={_display(stress.get('covered_2022'), status=validation_status)}; "
            f"delta={_display(stress.get('stress_2022_slice_delta'), status=validation_status)}`|"
            "`depends="
            f"{_display(dependency.get('depends_on_2023_plus'), status=validation_status)}; "
            f"first={_display(dependency.get('first_prediction_date'), status=validation_status)}`|"
        )
    lines.extend(
        [
            "",
            "## 证据边界",
            "",
            (
                "- 4 个 offline challenger rows 缺少 candidate signal / prediction artifact，"
                "因此 reclassify 为 `INCONCLUSIVE`；本报告不把 baseline evidence 复制成 "
                "candidate-level actual-path metrics。"
            ),
            (
                "- false risk-on/off、beta/TQQQ attribution、benchmark consistency、"
                "recovery delay 和 drawdown warning lead time 对已有 frozen policy rows "
                "仍为 unavailable status。"
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


def _render_state_reclassification_report(
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    lines = [
        "# Candidate State Reclassification Report",
        "",
        "## 状态重分类矩阵",
        "",
        (
            "| candidate | previous_state | updated_state | stability | reason | "
            "continue_research | expand_neighborhood | promotion_ready |"
        ),
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in records(payload.get("candidate_state_reclassification_rows")):
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('previous_state')}`|"
            f"`{row.get('updated_state')}`|`{row.get('state_transition_stability')}`|"
            f"`{row.get('reclassification_reason')}`|"
            f"`{row.get('continue_research')}`|`{row.get('expand_neighborhood')}`|"
            f"`{row.get('promotion_ready')}`|"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            (
                "`RESEARCH_ACCEPTED` / `OWNER_REVIEW_REQUIRED` / "
                "`OFFLINE_VALIDATION_READY` 都不是 promotion、paper-shadow 或 production state。"
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
