from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_active_selection_rule_audit import (
    run_first_layer_active_selection_rule_audit_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_CHALLENGER_MATRIX_PATH,
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
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_active_selection_policy_v2"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_ACTIVE_SELECTION_POLICY_V2_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2277_SPLIT_ACTIVE_SELECTION_AND_PROMOTION_POLICY"
SELECTION_STATES = (
    "RESEARCH_ACCEPTED",
    "OWNER_REVIEW_REQUIRED",
    "OFFLINE_VALIDATION_READY",
    "BLOCKED",
    "INCONCLUSIVE",
    "PROMOTION_READY",
)
RANK_FIELDS = (
    "utility",
    "false_risk_on_delta",
    "false_risk_off_delta",
    "drawdown_delta",
    "turnover_delta",
    "benchmark_consistency_delta",
    "stress_2022_slice_delta",
    "depends_on_2023_plus",
    "beta_dependency_delta",
    "tqqq_dependency_delta",
)


def run_first_layer_active_selection_policy_v2_pack(
    *,
    challenger_matrix_path: Path = DEFAULT_CHALLENGER_MATRIX_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    source_root = output_root / "_regenerated_sources"
    source_docs_root = source_root / "docs"
    active_selection_audit = run_first_layer_active_selection_rule_audit_pack(
        output_root=source_root / "trading_2276_active_selection_audit",
        docs_root=source_docs_root / "trading_2276_active_selection_audit",
    )
    challenger_matrix = load_mapping(challenger_matrix_path)

    candidate_rows = _candidate_rows(active_selection_audit)
    offline_rows = _offline_challenger_rows(challenger_matrix)
    matrix_rows = candidate_rows + offline_rows
    research_queue = _research_candidate_queue(matrix_rows)
    owner_queue = _owner_review_queue(matrix_rows)
    blocked_rows = [row for row in matrix_rows if row["selection_state"] == "BLOCKED"]
    boundary_rows = _boundary_rows(matrix_rows)
    promotion_boundary = _promotion_boundary(matrix_rows)
    summary = _summary(
        matrix_rows=matrix_rows,
        research_queue=research_queue,
        owner_queue=owner_queue,
        blocked_rows=blocked_rows,
        promotion_boundary=promotion_boundary,
    )
    common = {
        "schema_version": "first_layer_active_selection_policy_v2.v1",
        "report_type": "first_layer_active_selection_policy_v2",
        "title": "First-Layer Active Selection Policy v2",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": active_selection_audit.get("market_regime"),
        "anchor_event": active_selection_audit.get("anchor_event"),
        "anchor_date": active_selection_audit.get("anchor_date"),
        "requested_start": active_selection_audit.get("requested_start"),
        "actual_signal_start": active_selection_audit.get("actual_signal_start"),
        "data_quality_status": active_selection_audit.get("data_quality_status"),
        "source_generation": {
            "active_selection_audit_source": "regenerated_from_trading_2276_code_path",
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "challenger_matrix": str(challenger_matrix_path),
            "regenerated_2276_artifacts": clean_for_yaml(
                dict(mapping(active_selection_audit.get("artifact_paths")))
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
    policy = _policy_payload(common=common)
    payload = {
        **common,
        "policy": policy,
        "updated_challenger_selection_matrix": matrix_rows,
        "research_candidate_queue": research_queue,
        "owner_review_queue": owner_queue,
        "blocked_list": blocked_rows,
        "boundary_candidate_rows": boundary_rows,
        "promotion_boundary": promotion_boundary,
    }
    paths = {
        "active_selection_policy_v2_markdown": docs_root / "active_selection_policy_v2.md",
        "active_selection_policy_v2_yaml": output_root / "active_selection_policy_v2.yaml",
        "research_candidate_queue": output_root / "research_candidate_queue.json",
        "owner_review_queue": output_root / "owner_review_queue.json",
        "promotion_boundary_report": docs_root / "promotion_boundary_report.md",
        "updated_challenger_selection_matrix": output_root
        / "updated_challenger_selection_matrix.json",
    }
    write_yaml(paths["active_selection_policy_v2_yaml"], {**common, **policy})
    write_json(
        paths["research_candidate_queue"],
        {**common, "research_candidate_queue": research_queue},
    )
    write_json(
        paths["owner_review_queue"],
        {**common, "owner_review_queue": owner_queue},
    )
    write_json(
        paths["updated_challenger_selection_matrix"],
        {**common, "candidate_rows": matrix_rows},
    )
    write_markdown(
        paths["active_selection_policy_v2_markdown"],
        _render_policy_markdown(payload, paths),
    )
    write_markdown(
        paths["promotion_boundary_report"],
        _render_promotion_boundary_report(payload, paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _candidate_rows(active_selection_audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for candidate in records(active_selection_audit.get("candidate_assessments")):
        state = _state_for_candidate(candidate)
        risk_flags = _risk_flags(candidate)
        tradeoff_summary = _tradeoff_summary(candidate, state)
        rows.append(
            {
                "candidate_id": candidate.get("policy_id"),
                "candidate_source": "actual_path_candidate",
                "selection_state": state,
                "queue": _queue_for_state(state),
                "gate_policy_v2_state": candidate.get("gate_policy_v2_state"),
                "gate_policy_v2_blocked_reasons": strings(
                    candidate.get("gate_policy_v2_blocked_reasons")
                ),
                "gate_policy_v2_owner_review_reasons": strings(
                    candidate.get("gate_policy_v2_owner_review_reasons")
                ),
                "risk_flags": risk_flags,
                "tradeoff_summary": tradeoff_summary,
                "rank_features": _rank_features(candidate),
                "rank_score": _rank_score(candidate, state),
                "promotion_ready": False,
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
                "boundary_candidate_required_coverage": str(candidate.get("policy_id"))
                in {"wf_504d_baseline", "wf_378d_initial"},
            }
        )
    return sorted(rows, key=_sort_key)


def _offline_challenger_rows(challenger_matrix: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in records(challenger_matrix.get("experiments")):
        validation_ready = bool(row.get("validation_ready"))
        state = "OFFLINE_VALIDATION_READY" if validation_ready else "BLOCKED"
        missing_proxy_ids = strings(row.get("missing_proxy_ids"))
        risk_flags = []
        if missing_proxy_ids:
            risk_flags.append("missing_proxy_inputs")
        rows.append(
            {
                "candidate_id": row.get("experiment_id"),
                "candidate_source": "offline_challenger_experiment",
                "selection_state": state,
                "queue": _queue_for_state(state),
                "gate_policy_v2_state": "ACCEPTED" if validation_ready else "BLOCKED",
                "gate_policy_v2_blocked_reasons": [] if validation_ready else missing_proxy_ids,
                "gate_policy_v2_owner_review_reasons": [],
                "risk_flags": risk_flags,
                "tradeoff_summary": row.get("validation_ready_scope", ""),
                "rank_features": {
                    "utility": None,
                    "false_risk_on_delta": None,
                    "false_risk_off_delta": None,
                    "drawdown_delta": None,
                    "turnover_delta": None,
                    "benchmark_consistency_delta": None,
                    "stress_2022_slice_delta": None,
                    "depends_on_2023_plus": None,
                    "beta_dependency_delta": None,
                    "tqqq_dependency_delta": None,
                    "target_objective_terms": strings(row.get("target_objective_terms")),
                    "missing_proxy_ids": missing_proxy_ids,
                },
                "rank_score": None,
                "promotion_ready": False,
                "promotion_allowed": False,
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": row.get("broker_action", "none"),
                "boundary_candidate_required_coverage": False,
            }
        )
    return sorted(rows, key=_sort_key)


def _state_for_candidate(candidate: Mapping[str, Any]) -> str:
    gate_state = str(candidate.get("gate_policy_v2_state"))
    if gate_state == "BLOCKED":
        return "BLOCKED"
    if gate_state == "OWNER_REVIEW_REQUIRED":
        return "OWNER_REVIEW_REQUIRED"
    if gate_state == "ACCEPTED":
        return "RESEARCH_ACCEPTED"
    return "INCONCLUSIVE"


def _queue_for_state(state: str) -> str:
    if state == "OWNER_REVIEW_REQUIRED":
        return "owner_review_queue"
    if state in {"RESEARCH_ACCEPTED", "OFFLINE_VALIDATION_READY", "INCONCLUSIVE"}:
        return "ranked_review_queue"
    return "blocked_list"


def _risk_flags(candidate: Mapping[str, Any]) -> list[str]:
    flags = []
    reasons = strings(candidate.get("gate_policy_v2_owner_review_reasons"))
    if "not_2023_plus_only" in reasons or not bool(candidate.get("covered_2022")):
        flags.append("2023_plus_dependency")
    if not bool(candidate.get("does_coverage_pass_rule")):
        flags.append("coverage_rule_not_satisfied")
    if not bool(candidate.get("no_major_regression_in_defensive_probe")):
        flags.append("defensive_regression")
    return list(dict.fromkeys(flags))


def _tradeoff_summary(candidate: Mapping[str, Any], state: str) -> str:
    if state == "OWNER_REVIEW_REQUIRED":
        return (
            "候选有较高 actual-path utility，但 gate policy v2 将其风险定义为 "
            "owner review，而不是 hard block。"
        )
    if state == "RESEARCH_ACCEPTED":
        return "候选通过 gate policy v2 research selection，可进入 ranked review。"
    if state == "BLOCKED":
        return "候选存在 strong performance / hard-block failure，不能进入 research queue。"
    return "证据不足，需要补充 candidate-level evidence。"


def _rank_features(candidate: Mapping[str, Any]) -> dict[str, Any]:
    metrics = mapping(candidate.get("metric_snapshot"))
    return {
        "utility": candidate.get("actual_path_utility_proxy"),
        "false_risk_on_delta": metrics.get("false_risk_on_delta"),
        "false_risk_off_delta": metrics.get("false_risk_off_delta"),
        "drawdown_delta": metrics.get("drawdown_delta"),
        "turnover_delta": metrics.get("turnover_delta"),
        "benchmark_consistency_delta": metrics.get("benchmark_consistency_delta"),
        "stress_2022_slice_delta": metrics.get("stress_slice_delta"),
        "depends_on_2023_plus": "2023_plus_dependency" in _risk_flags(candidate),
        "beta_dependency_delta": metrics.get("beta_dependency_delta"),
        "tqqq_dependency_delta": metrics.get("tqqq_dependency_delta"),
    }


def _rank_score(candidate: Mapping[str, Any], state: str) -> float | None:
    if state == "BLOCKED":
        return None
    utility = candidate.get("actual_path_utility_proxy")
    if utility is None:
        return None
    return round_float(utility)


def _research_candidate_queue(
    matrix_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ranked_rows = [
        row
        for row in matrix_rows
        if row["selection_state"] in {"RESEARCH_ACCEPTED", "OWNER_REVIEW_REQUIRED", "INCONCLUSIVE"}
    ]
    offline_rows = [
        row for row in matrix_rows if row["selection_state"] == "OFFLINE_VALIDATION_READY"
    ]
    return {
        "ranked_review_queue": [
            _queue_row(row, rank=index + 1)
            for index, row in enumerate(sorted(ranked_rows, key=_ranked_review_sort_key))
        ],
        "offline_validation_queue": [
            _queue_row(row, rank=index + 1)
            for index, row in enumerate(sorted(offline_rows, key=_sort_key))
        ],
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _owner_review_queue(matrix_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [row for row in matrix_rows if row["selection_state"] == "OWNER_REVIEW_REQUIRED"]
    return {
        "owner_review_queue": [
            _queue_row(row, rank=index + 1) for index, row in enumerate(sorted(rows, key=_sort_key))
        ],
        "owner_review_required_count": len(rows),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _queue_row(row: Mapping[str, Any], *, rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "candidate_id": row.get("candidate_id"),
        "candidate_source": row.get("candidate_source"),
        "selection_state": row.get("selection_state"),
        "queue": row.get("queue"),
        "rank_score": row.get("rank_score"),
        "rank_features": row.get("rank_features"),
        "risk_flags": row.get("risk_flags"),
        "tradeoff_summary": row.get("tradeoff_summary"),
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _boundary_rows(matrix_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    wanted = {"wf_504d_baseline", "wf_378d_initial"}
    return [
        {
            "candidate_id": row.get("candidate_id"),
            "utility": mapping(row.get("rank_features")).get("utility"),
            "gate_policy_v2_state": row.get("gate_policy_v2_state"),
            "selection_state": row.get("selection_state"),
            "expected_state": "OWNER_REVIEW_REQUIRED"
            if row.get("candidate_id") == "wf_504d_baseline"
            else "RESEARCH_ACCEPTED",
            "passes_expected_state": (
                row.get("selection_state")
                == (
                    "OWNER_REVIEW_REQUIRED"
                    if row.get("candidate_id") == "wf_504d_baseline"
                    else "RESEARCH_ACCEPTED"
                )
            ),
            "promotion_allowed": False,
        }
        for row in matrix_rows
        if str(row.get("candidate_id")) in wanted
    ]


def _promotion_boundary(matrix_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "promotion_gate_independent": True,
        "active_selection_can_set_promotion_ready": False,
        "promotion_ready_count": sum(
            1 for row in matrix_rows if row.get("selection_state") == "PROMOTION_READY"
        ),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "required_future_gate": "promotion_gate_v2_not_part_of_active_selection_policy_v2",
    }


def _summary(
    *,
    matrix_rows: Sequence[Mapping[str, Any]],
    research_queue: Mapping[str, Any],
    owner_queue: Mapping[str, Any],
    blocked_rows: Sequence[Mapping[str, Any]],
    promotion_boundary: Mapping[str, Any],
) -> dict[str, Any]:
    state_counts = {
        state: sum(1 for row in matrix_rows if row.get("selection_state") == state)
        for state in SELECTION_STATES
    }
    return {
        "task_id": TASK_ID,
        "candidate_count": len(matrix_rows),
        "state_counts": state_counts,
        "ranked_review_count": len(records(research_queue.get("ranked_review_queue"))),
        "offline_validation_ready_count": len(
            records(research_queue.get("offline_validation_queue"))
        ),
        "owner_review_required_count": owner_queue.get("owner_review_required_count"),
        "blocked_count": len(blocked_rows),
        "promotion_ready_count": promotion_boundary.get("promotion_ready_count"),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _policy_payload(*, common: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "policy_id": "first_layer_active_selection_policy_v2",
        "selection_states": list(SELECTION_STATES),
        "state_semantics": {
            "RESEARCH_ACCEPTED": "research queue state; not promotion",
            "OWNER_REVIEW_REQUIRED": "owner review queue state; not blocked",
            "OFFLINE_VALIDATION_READY": "offline validation queue state; not paper-shadow",
            "BLOCKED": "hard research or strong performance failure",
            "INCONCLUSIVE": "evidence insufficient; ranked review only",
            "PROMOTION_READY": "reserved for independent future promotion gate",
        },
        "transition_rules": {
            "gate_policy_v2_state_OWNER_REVIEW_REQUIRED": {
                "selection_state": "OWNER_REVIEW_REQUIRED",
                "may_be_rewritten_to_BLOCKED_by_active_selection": False,
                "required_queue": "owner_review_queue",
                "risk_flags_required": True,
                "tradeoff_summary_required": True,
            },
            "gate_policy_v2_state_ACCEPTED": {
                "selection_state": "RESEARCH_ACCEPTED",
                "may_enter_promotion": False,
            },
            "severe_defensive_regression_or_hard_gate_violation": {
                "selection_state": "BLOCKED",
                "promotion_allowed": False,
            },
        },
        "ranking_fields": list(RANK_FIELDS),
        "promotion_boundary": {
            "active_selection_decides_promotion": False,
            "promotion_allowed": common.get("promotion_allowed", False),
            "paper_shadow_allowed": common.get("paper_shadow_allowed", False),
            "production_allowed": common.get("production_allowed", False),
            "broker_action": common.get("broker_action", "none"),
        },
    }


def _sort_key(row: Mapping[str, Any]) -> tuple[int, float, str]:
    state_order = {
        "RESEARCH_ACCEPTED": 0,
        "OWNER_REVIEW_REQUIRED": 1,
        "OFFLINE_VALIDATION_READY": 2,
        "INCONCLUSIVE": 3,
        "BLOCKED": 4,
        "PROMOTION_READY": 5,
    }
    rank_score = row.get("rank_score")
    score = -9999.0 if rank_score is None else to_float(rank_score)
    return (
        state_order.get(str(row.get("selection_state")), 99),
        -score,
        str(row.get("candidate_id")),
    )


def _ranked_review_sort_key(row: Mapping[str, Any]) -> tuple[float, int, str]:
    state_order = {
        "RESEARCH_ACCEPTED": 0,
        "OWNER_REVIEW_REQUIRED": 1,
        "INCONCLUSIVE": 2,
    }
    rank_score = row.get("rank_score")
    score = -9999.0 if rank_score is None else to_float(rank_score)
    return (
        -score,
        state_order.get(str(row.get("selection_state")), 99),
        str(row.get("candidate_id")),
    )


def _display_cell(value: Any) -> str:
    if value is None:
        return "n/a"
    return str(value)


def _render_policy_markdown(
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# Active Selection Policy v2",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        "- active selection 只决定 research / owner-review / offline-validation / blocked queues。",
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "## State Counts",
        "",
        "| state | count |",
        "|---|---:|",
    ]
    for state, count in mapping(summary.get("state_counts")).items():
        lines.append(f"|`{state}`|{count}|")
    lines.extend(
        [
            "",
            "## Policy Semantics",
            "",
            "- `RESEARCH_ACCEPTED` 是 research queue state，不等于 promotion。",
            "- `OWNER_REVIEW_REQUIRED` 是 owner-review queue state，不等于 `BLOCKED`。",
            "- `OFFLINE_VALIDATION_READY` 是 offline validation queue state，不等于 paper-shadow。",
            "- `PROMOTION_READY` 只保留给未来独立 promotion gate，本批 count=`0`。",
            (
                "- Ranked review queue 按 actual-path utility 排序；"
                "其它风险字段只展示，未加未校准权重。"
            ),
            "",
            "## Boundary Candidates",
            "",
            "| candidate | utility | gate_policy_v2_state | policy_v2_state | expected | pass |",
            "|---|---:|---|---|---|---|",
        ]
    )
    for row in records(payload.get("boundary_candidate_rows")):
        lines.append(
            f"|`{row['candidate_id']}`|{row.get('utility')}|"
            f"`{row.get('gate_policy_v2_state')}`|`{row.get('selection_state')}`|"
            f"`{row.get('expected_state')}`|`{row.get('passes_expected_state')}`|"
        )
    lines.extend(
        [
            "",
            "## Ranked Review Queue",
            "",
            (
                "| rank | candidate | state | utility | 2023+ dependency | "
                "beta delta | TQQQ delta | risk flags |"
            ),
            "|---:|---|---|---:|---|---:|---:|---|",
        ]
    )
    for row in records(mapping(payload.get("research_candidate_queue")).get("ranked_review_queue")):
        features = mapping(row.get("rank_features"))
        flags = ", ".join(strings(row.get("risk_flags"))) or "none"
        lines.append(
            f"|{row.get('rank')}|`{row.get('candidate_id')}`|"
            f"`{row.get('selection_state')}`|{features.get('utility')}|"
            f"`{features.get('depends_on_2023_plus')}`|"
            f"{_display_cell(features.get('beta_dependency_delta'))}|"
            f"{_display_cell(features.get('tqqq_dependency_delta'))}|"
            f"{flags}|"
        )
    lines.extend(
        [
            "",
            "## Offline Validation Queue",
            "",
            "| rank | candidate | state | tradeoff summary |",
            "|---:|---|---|---|",
        ]
    )
    for row in records(
        mapping(payload.get("research_candidate_queue")).get("offline_validation_queue")
    ):
        lines.append(
            f"|{row.get('rank')}|`{row.get('candidate_id')}`|"
            f"`{row.get('selection_state')}`|{row.get('tradeoff_summary')}|"
        )
    lines.extend(
        [
            "",
            "## Owner Review Queue",
            "",
            "| rank | candidate | utility | risk flags | tradeoff summary |",
            "|---:|---|---:|---|---|",
        ]
    )
    for row in records(mapping(payload.get("owner_review_queue")).get("owner_review_queue")):
        features = mapping(row.get("rank_features"))
        flags = ", ".join(strings(row.get("risk_flags"))) or "none"
        lines.append(
            f"|{row.get('rank')}|`{row.get('candidate_id')}`|"
            f"{features.get('utility')}|{flags}|{row.get('tradeoff_summary')}|"
        )
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _render_promotion_boundary_report(
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    boundary = mapping(payload.get("promotion_boundary"))
    lines = [
        "# Promotion Boundary Report",
        "",
        "Active selection policy v2 不决定 promotion；promotion gate 仍独立关闭。",
        "",
        f"- promotion_gate_independent: `{boundary.get('promotion_gate_independent')}`",
        (
            "- active_selection_can_set_promotion_ready: "
            f"`{boundary.get('active_selection_can_set_promotion_ready')}`"
        ),
        f"- promotion_ready_count: `{boundary.get('promotion_ready_count')}`",
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "## Boundary Candidates",
        "",
        "| candidate | selection_state | promotion_allowed |",
        "|---|---|---|",
    ]
    for row in records(payload.get("boundary_candidate_rows")):
        lines.append(
            f"|`{row['candidate_id']}`|`{row.get('selection_state')}`|"
            f"`{row.get('promotion_allowed')}`|"
        )
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)
