from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.first_layer_active_selection_policy_v2 import (
    run_first_layer_active_selection_policy_v2_pack,
)
from ai_trading_system.first_layer_active_selection_rule_audit import (
    run_first_layer_active_selection_rule_audit_pack,
)
from ai_trading_system.first_layer_performance_gate_audit import (
    DEFAULT_CHALLENGER_MATRIX_PATH,
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
    PROJECT_ROOT / "outputs" / "research_trends" / "first_layer_challenger_matrix_v2"
)
DEFAULT_DOCS_ROOT = PROJECT_ROOT / "docs" / "research"

STATUS = "FIRST_LAYER_CHALLENGER_MATRIX_V2_READY_PROMOTION_BLOCKED"
TASK_ID = "TRADING-2278_FIRST_LAYER_CHALLENGER_MATRIX_V2_RERUN"


def run_first_layer_challenger_matrix_v2_pack(
    *,
    challenger_matrix_path: Path = DEFAULT_CHALLENGER_MATRIX_PATH,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    docs_root: Path = DEFAULT_DOCS_ROOT,
) -> dict[str, Any]:
    source_root = output_root / "_regenerated_sources"
    source_docs_root = source_root / "docs"
    active_selection_audit = run_first_layer_active_selection_rule_audit_pack(
        output_root=source_root / "trading_2276_active_selection_rule_audit",
        docs_root=source_docs_root / "trading_2276_active_selection_rule_audit",
    )
    active_selection_policy_v2 = run_first_layer_active_selection_policy_v2_pack(
        challenger_matrix_path=challenger_matrix_path,
        output_root=source_root / "trading_2277_active_selection_policy_v2",
        docs_root=source_docs_root / "trading_2277_active_selection_policy_v2",
    )
    current_decisions = _current_active_selection_decisions(active_selection_audit)
    candidate_rows = [
        _candidate_row_v2(row=row, current_decisions=current_decisions)
        for row in records(active_selection_policy_v2.get("updated_challenger_selection_matrix"))
    ]
    research_queue = _queue(candidate_rows, {"RESEARCH_ACCEPTED", "OFFLINE_VALIDATION_READY"})
    owner_review_queue = _queue(candidate_rows, {"OWNER_REVIEW_REQUIRED"})
    blocked_queue = _queue(candidate_rows, {"BLOCKED"})
    promotion_boundary_check = _promotion_boundary_check(candidate_rows)
    summary = _summary(
        candidate_rows=candidate_rows,
        research_queue=research_queue,
        owner_review_queue=owner_review_queue,
        blocked_queue=blocked_queue,
        promotion_boundary_check=promotion_boundary_check,
    )
    common = {
        "schema_version": "first_layer_challenger_matrix_v2.v1",
        "report_type": "first_layer_challenger_matrix_v2",
        "title": "First-Layer Challenger Matrix v2",
        "status": STATUS,
        "task_id": TASK_ID,
        "generated_at": datetime.now(tz=UTC).replace(microsecond=0).isoformat(),
        "market_regime": active_selection_policy_v2.get("market_regime"),
        "anchor_event": active_selection_policy_v2.get("anchor_event"),
        "anchor_date": active_selection_policy_v2.get("anchor_date"),
        "requested_start": active_selection_policy_v2.get("requested_start"),
        "actual_signal_start": active_selection_policy_v2.get("actual_signal_start"),
        "data_quality_status": active_selection_policy_v2.get("data_quality_status"),
        "source_generation": {
            "gate_policy_v2_source": "regenerated_through_trading_2276_and_2277_code_paths",
            "active_selection_policy_v2_source": "regenerated_from_trading_2277_code_path",
            "v1_active_selection_source": "regenerated_from_trading_2276_code_path",
            "ignored_outputs_not_required_as_source_of_truth": True,
            "regenerated_source_root": str(source_root),
        },
        "input_artifacts": {
            "challenger_matrix": str(challenger_matrix_path),
            "regenerated_2276_artifacts": clean_for_yaml(
                dict(mapping(active_selection_audit.get("artifact_paths")))
            ),
            "regenerated_2277_artifacts": clean_for_yaml(
                dict(mapping(active_selection_policy_v2.get("artifact_paths")))
            ),
        },
        "summary": summary,
        "gate_policy_v2_applied": True,
        "active_selection_policy_v2_applied": True,
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
        "dynamic_promotion_status": "BLOCKED",
    }
    payload = {
        **common,
        "candidate_rows": candidate_rows,
        "research_candidate_queue_v2": research_queue,
        "owner_review_queue_v2": owner_review_queue,
        "blocked_candidate_queue_v2": blocked_queue,
        "promotion_boundary_check": promotion_boundary_check,
    }
    paths = {
        "first_layer_challenger_matrix_v2": output_root / "first_layer_challenger_matrix_v2.json",
        "first_layer_challenger_report_v2": docs_root / "first_layer_challenger_report_v2.md",
        "research_candidate_queue_v2": output_root / "research_candidate_queue_v2.json",
        "owner_review_queue_v2": output_root / "owner_review_queue_v2.json",
        "blocked_candidate_queue_v2": output_root / "blocked_candidate_queue_v2.json",
        "promotion_boundary_check_v2": docs_root / "promotion_boundary_check_v2.md",
    }
    write_json(paths["first_layer_challenger_matrix_v2"], payload)
    write_json(
        paths["research_candidate_queue_v2"],
        {**common, "research_candidate_queue_v2": research_queue},
    )
    write_json(
        paths["owner_review_queue_v2"],
        {**common, "owner_review_queue_v2": owner_review_queue},
    )
    write_json(
        paths["blocked_candidate_queue_v2"],
        {**common, "blocked_candidate_queue_v2": blocked_queue},
    )
    write_markdown(
        paths["first_layer_challenger_report_v2"],
        _render_report(payload=payload, paths=paths),
    )
    write_markdown(
        paths["promotion_boundary_check_v2"],
        _render_promotion_boundary_check(payload=payload, paths=paths),
    )
    return clean_for_yaml(
        {
            **payload,
            "artifact_paths": {key: str(path) for key, path in paths.items()},
        }
    )


def _current_active_selection_decisions(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    for row in records(payload.get("mode_rows")):
        if row.get("mode") == "current_active_selection":
            return {
                str(decision.get("policy_id")): decision
                for decision in records(row.get("candidate_decisions"))
            }
    return {}


def _candidate_row_v2(
    *,
    row: Mapping[str, Any],
    current_decisions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    candidate_id = str(row.get("candidate_id"))
    current_decision = mapping(current_decisions.get(candidate_id))
    v1_state = str(current_decision.get("selection_state") or "NOT_IN_V1_ACTIVE_SELECTION")
    v2_state = str(row.get("selection_state"))
    rank_features = mapping(row.get("rank_features"))
    risk_flags = strings(row.get("risk_flags"))
    promotion_ready = v2_state == "PROMOTION_READY"
    return {
        "candidate_id": candidate_id,
        "candidate_source": row.get("candidate_source"),
        "v1_active_selection_state": v1_state,
        "gate_policy_v2_state": row.get("gate_policy_v2_state"),
        "selection_policy_v2_state": v2_state,
        "candidate_state_transition_from_v1": f"{v1_state} -> {v2_state}",
        "queue": _queue_name(v2_state),
        "utility": rank_features.get("utility"),
        "rank_score": row.get("rank_score"),
        "rank_features": rank_features,
        "risk_flags": risk_flags,
        "tradeoff_summary": row.get("tradeoff_summary"),
        "gate_policy_v2_blocked_reasons": strings(row.get("gate_policy_v2_blocked_reasons")),
        "gate_policy_v2_owner_review_reasons": strings(
            row.get("gate_policy_v2_owner_review_reasons")
        ),
        "v1_active_selection_reasons": strings(current_decision.get("selection_reasons")),
        "owner_review_suppressed_by_v1_selection": bool(
            current_decision.get("owner_review_suppressed_by_selection")
        ),
        "active_selection_policy_v2_applied": True,
        "gate_policy_v2_applied": True,
        "promotion_ready": promotion_ready,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _queue_name(state: str) -> str:
    if state == "OWNER_REVIEW_REQUIRED":
        return "owner_review_queue_v2"
    if state in {"RESEARCH_ACCEPTED", "OFFLINE_VALIDATION_READY", "INCONCLUSIVE"}:
        return "research_candidate_queue_v2"
    return "blocked_candidate_queue_v2"


def _queue(
    rows: Sequence[Mapping[str, Any]],
    states: set[str],
) -> dict[str, Any]:
    queue_rows = [
        _queue_row(row, rank=index + 1)
        for index, row in enumerate(
            sorted(
                [row for row in rows if row.get("selection_policy_v2_state") in states],
                key=_sort_key,
            )
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


def _queue_row(row: Mapping[str, Any], *, rank: int) -> dict[str, Any]:
    return {
        "rank": rank,
        "candidate_id": row.get("candidate_id"),
        "candidate_source": row.get("candidate_source"),
        "selection_policy_v2_state": row.get("selection_policy_v2_state"),
        "v1_active_selection_state": row.get("v1_active_selection_state"),
        "candidate_state_transition_from_v1": row.get("candidate_state_transition_from_v1"),
        "utility": row.get("utility"),
        "rank_features": row.get("rank_features"),
        "risk_flags": row.get("risk_flags"),
        "tradeoff_summary": row.get("tradeoff_summary"),
        "promotion_ready": False,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _summary(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    research_queue: Mapping[str, Any],
    owner_review_queue: Mapping[str, Any],
    blocked_queue: Mapping[str, Any],
    promotion_boundary_check: Mapping[str, Any],
) -> dict[str, Any]:
    research_rows = [
        row for row in candidate_rows if row.get("selection_policy_v2_state") == "RESEARCH_ACCEPTED"
    ]
    owner_rows = [
        row
        for row in candidate_rows
        if row.get("selection_policy_v2_state") == "OWNER_REVIEW_REQUIRED"
    ]
    blocked_rows = [
        row for row in candidate_rows if row.get("selection_policy_v2_state") == "BLOCKED"
    ]
    return {
        "task_id": TASK_ID,
        "candidate_count": len(candidate_rows),
        "research_accepted_count": len(research_rows),
        "offline_validation_ready_count": sum(
            1
            for row in candidate_rows
            if row.get("selection_policy_v2_state") == "OFFLINE_VALIDATION_READY"
        ),
        "owner_review_required_count": len(owner_rows),
        "blocked_count": len(blocked_rows),
        "promotion_ready_count": sum(
            1 for row in candidate_rows if row.get("selection_policy_v2_state") == "PROMOTION_READY"
        ),
        "research_queue_count": research_queue.get("candidate_count"),
        "owner_review_queue_count": owner_review_queue.get("candidate_count"),
        "blocked_queue_count": blocked_queue.get("candidate_count"),
        "best_research_candidate": _best_candidate(research_rows),
        "best_owner_review_candidate": _best_candidate(owner_rows),
        "best_blocked_candidate": _best_candidate(blocked_rows),
        "active_selection_policy_v2_applied": True,
        "gate_policy_v2_applied": True,
        "promotion_boundary_check_passed": promotion_boundary_check.get("passed"),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _best_candidate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    rows_with_utility = [row for row in rows if row.get("utility") is not None]
    if not rows_with_utility:
        return None
    row = max(rows_with_utility, key=lambda item: to_float(item.get("utility")))
    return {
        "candidate_id": row.get("candidate_id"),
        "selection_policy_v2_state": row.get("selection_policy_v2_state"),
        "utility": row.get("utility"),
        "candidate_state_transition_from_v1": row.get("candidate_state_transition_from_v1"),
        "risk_flags": row.get("risk_flags"),
        "promotion_allowed": False,
    }


def _promotion_boundary_check(candidate_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows_by_id = {str(row.get("candidate_id")): row for row in candidate_rows}
    wf_504d = mapping(rows_by_id.get("wf_504d_baseline"))
    wf_378d = mapping(rows_by_id.get("wf_378d_initial"))
    checks = [
        _check(
            "wf_504d_baseline_owner_review_required",
            wf_504d.get("selection_policy_v2_state") == "OWNER_REVIEW_REQUIRED",
            "wf_504d_baseline must remain OWNER_REVIEW_REQUIRED, not BLOCKED.",
        ),
        _check(
            "wf_378d_initial_research_or_offline_ready",
            wf_378d.get("selection_policy_v2_state")
            in {"RESEARCH_ACCEPTED", "OFFLINE_VALIDATION_READY"},
            "wf_378d_initial must remain research accepted or offline validation ready.",
        ),
        _check(
            "promotion_allowed_false",
            all(row.get("promotion_allowed") is False for row in candidate_rows),
            "promotion_allowed must remain false.",
        ),
        _check(
            "paper_shadow_allowed_false",
            all(row.get("paper_shadow_allowed") is False for row in candidate_rows),
            "paper_shadow_allowed must remain false.",
        ),
        _check(
            "production_allowed_false",
            all(row.get("production_allowed") is False for row in candidate_rows),
            "production_allowed must remain false.",
        ),
        _check(
            "broker_action_none",
            all(row.get("broker_action") == "none" for row in candidate_rows),
            "broker_action must remain none.",
        ),
        _check(
            "owner_review_not_promotion_ready",
            all(
                row.get("selection_policy_v2_state") != "PROMOTION_READY"
                for row in candidate_rows
                if row.get("gate_policy_v2_state") == "OWNER_REVIEW_REQUIRED"
            ),
            "OWNER_REVIEW_REQUIRED must not become PROMOTION_READY.",
        ),
        _check(
            "research_accepted_not_promotion",
            all(
                row.get("promotion_allowed") is False and row.get("promotion_ready") is False
                for row in candidate_rows
                if row.get("selection_policy_v2_state") == "RESEARCH_ACCEPTED"
            ),
            "RESEARCH_ACCEPTED must not trigger promotion.",
        ),
    ]
    return {
        "checks": checks,
        "passed": all(bool(row["passed"]) for row in checks),
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "broker_action": "none",
    }


def _check(check_id: str, passed: bool, description: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": passed, "description": description}


def _sort_key(row: Mapping[str, Any]) -> tuple[int, float, str]:
    state_order = {
        "RESEARCH_ACCEPTED": 0,
        "OWNER_REVIEW_REQUIRED": 1,
        "OFFLINE_VALIDATION_READY": 2,
        "INCONCLUSIVE": 3,
        "BLOCKED": 4,
        "PROMOTION_READY": 5,
    }
    utility = row.get("utility")
    score = -9999.0 if utility is None else to_float(utility)
    return (
        state_order.get(str(row.get("selection_policy_v2_state")), 99),
        -score,
        str(row.get("candidate_id")),
    )


def _render_report(
    *,
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    summary = mapping(payload.get("summary"))
    lines = [
        "# First-Layer Challenger Matrix v2",
        "",
        "## 摘要",
        "",
        f"- task_id: `{payload.get('task_id')}`; status: `{payload.get('status')}`",
        ("- gate_policy_v2_applied=`true`; active_selection_policy_v2_applied=`true`."),
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        (
            f"- research_accepted_count=`{summary.get('research_accepted_count')}`; "
            f"owner_review_required_count=`{summary.get('owner_review_required_count')}`; "
            f"blocked_count=`{summary.get('blocked_count')}`; "
            f"promotion_ready_count=`{summary.get('promotion_ready_count')}`."
        ),
        "",
        "## Best Candidates",
        "",
        "| bucket | candidate | state | utility | transition_from_v1 |",
        "|---|---|---|---:|---|",
    ]
    for bucket, key in (
        ("research", "best_research_candidate"),
        ("owner_review", "best_owner_review_candidate"),
        ("blocked", "best_blocked_candidate"),
    ):
        row = mapping(summary.get(key))
        lines.append(
            f"|`{bucket}`|`{row.get('candidate_id')}`|"
            f"`{row.get('selection_policy_v2_state')}`|{row.get('utility')}|"
            f"`{row.get('candidate_state_transition_from_v1')}`|"
        )
    lines.extend(
        [
            "",
            "## Boundary Candidate State Transitions",
            "",
            "| candidate | v1 state | v2 state | transition | promotion_allowed |",
            "|---|---|---|---|---|",
        ]
    )
    for row in records(payload.get("candidate_rows")):
        if row.get("candidate_id") not in {"wf_504d_baseline", "wf_378d_initial"}:
            continue
        lines.append(
            f"|`{row.get('candidate_id')}`|`{row.get('v1_active_selection_state')}`|"
            f"`{row.get('selection_policy_v2_state')}`|"
            f"`{row.get('candidate_state_transition_from_v1')}`|"
            f"`{row.get('promotion_allowed')}`|"
        )
    lines.extend(
        [
            "",
            "## Queues",
            "",
            "| queue | count |",
            "|---|---:|",
            f"|`research_candidate_queue_v2`|{summary.get('research_queue_count')}|",
            f"|`owner_review_queue_v2`|{summary.get('owner_review_queue_count')}|",
            f"|`blocked_candidate_queue_v2`|{summary.get('blocked_queue_count')}|",
            "",
            "## 产物",
            "",
        ]
    )
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)


def _render_promotion_boundary_check(
    *,
    payload: Mapping[str, Any],
    paths: Mapping[str, Path],
) -> str:
    boundary = mapping(payload.get("promotion_boundary_check"))
    lines = [
        "# Promotion Boundary Check v2",
        "",
        f"- passed: `{boundary.get('passed')}`",
        (
            "- promotion_allowed=`false`; paper_shadow_allowed=`false`; "
            "production_allowed=`false`; broker_action=`none`."
        ),
        "",
        "| check_id | passed | description |",
        "|---|---|---|",
    ]
    for row in records(boundary.get("checks")):
        lines.append(f"|`{row.get('check_id')}`|`{row.get('passed')}`|{row.get('description')}|")
    lines.extend(["", "## 产物", ""])
    for key, path in paths.items():
        lines.append(f"- `{key}`: `{path}`")
    lines.append("")
    return "\n".join(lines)
