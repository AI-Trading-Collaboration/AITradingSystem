from __future__ import annotations

from collections.abc import Mapping
from typing import Any

GATE_SCHEMA_VERSION = "dynamic_strategy_pit_gate_result.v1"
BLOCKER_SUMMARY_SCHEMA_VERSION = "dynamic_strategy_pit_blocker_summary.v1"
NOT_SAFE_PIT_STATUSES: set[str] = {"UNKNOWN", "NOT_PIT_SAFE"}


def evaluate_pit_gate(
    matrix_rows: list[Mapping[str, Any]],
    *,
    scope: str = "dynamic_strategy",
    observation_approved: bool = False,
    owner_review_recorded: bool = False,
) -> dict[str, Any]:
    blocking_rows = [row for row in matrix_rows if row.get("severity") == "BLOCKING"]
    material_or_blocking_rows = [
        row for row in matrix_rows if row.get("severity") in {"BLOCKING", "MATERIAL"}
    ]
    blockers = _blocker_codes(blocking_rows)
    required_signal_not_safe = [
        row
        for row in matrix_rows
        if row.get("input_type") == "SIGNAL"
        and row.get("point_in_time_status") in NOT_SAFE_PIT_STATUSES
    ]
    required_execution_not_safe = [
        row
        for row in matrix_rows
        if row.get("input_type") == "EXECUTION_SEMANTIC"
        and row.get("point_in_time_status") in NOT_SAFE_PIT_STATUSES
    ]
    candidate_reasons = list(blockers)
    if required_signal_not_safe:
        candidate_reasons.append("REQUIRED_SIGNAL_PIT_STATUS_UNKNOWN_OR_NOT_SAFE")
    if required_execution_not_safe:
        candidate_reasons.append(
            "REQUIRED_EXECUTION_SEMANTIC_PIT_STATUS_UNKNOWN_OR_NOT_SAFE"
        )
    observation_reasons = list(candidate_reasons)
    if _has_input(matrix_rows, "valid_until_window"):
        observation_reasons.append("VALID_UNTIL_WINDOW_NOT_GROUNDED")
    if _has_input(matrix_rows, "no_stale_signal_carry_forward"):
        observation_reasons.append("STALE_SIGNAL_RULE_NOT_VERIFIABLE")
    if required_signal_not_safe:
        observation_reasons.append(
            "CORE_RETURN_SIGNAL_NOT_TRUE_OR_OWNER_APPROVED_APPROXIMATE_PIT"
        )
    paper_reasons = []
    if not observation_approved:
        paper_reasons.append("RESEARCH_ONLY_OBSERVATION_NOT_APPROVED")
    if material_or_blocking_rows:
        paper_reasons.append("ANY_MATERIAL_OR_BLOCKING_PIT_GAP")
    if not owner_review_recorded:
        paper_reasons.append("OWNER_REVIEW_NOT_RECORDED")
    production_reasons = ["CURRENT_PHASE_PRODUCTION_DISABLED"]

    return {
        "schema_version": GATE_SCHEMA_VERSION,
        "scope": scope,
        "candidate_search_allowed": not candidate_reasons,
        "research_only_observation_allowed": not observation_reasons,
        "paper_shadow_allowed": not paper_reasons,
        "production_allowed": False,
        "blockers": blockers,
        "candidate_search": {
            "allowed": not candidate_reasons,
            "blocked_if": [
                "any_required_input_severity_BLOCKING",
                "core_return_signal_pit_status_UNKNOWN_or_NOT_PIT_SAFE",
                "execution_validity_semantic_pit_status_UNKNOWN_or_NOT_PIT_SAFE",
            ],
            "reasons": _unique(candidate_reasons),
        },
        "research_only_observation": {
            "allowed": not observation_reasons,
            "blocked_if": [
                "any_required_input_severity_BLOCKING",
                "valid_until_window_not_grounded",
                "stale_signal_rule_not_verifiable",
                "core_return_signal_not_true_or_owner_approved_approximate_pit",
            ],
            "reasons": _unique(observation_reasons),
        },
        "paper_shadow": {
            "allowed": not paper_reasons,
            "blocked_if": [
                "research_only_observation_not_approved",
                "any_material_or_blocking_pit_gap",
                "owner_review_not_recorded",
            ],
            "reasons": _unique(paper_reasons),
        },
        "production": {
            "allowed": False,
            "blocked_if": ["current_phase_production_disabled"],
            "reasons": production_reasons,
        },
        "gate_derivation_sources": _gate_derivation_sources(),
        "policy_note": (
            "PIT gate is a policy-derived safety gate, not a statistically "
            "calibrated empirical threshold."
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def build_pit_blocker_summary(
    matrix_rows: list[Mapping[str, Any]],
    gate_result: Mapping[str, Any],
    *,
    scope: str = "dynamic_strategy",
) -> dict[str, Any]:
    blocking_rows = [row for row in matrix_rows if row.get("severity") == "BLOCKING"]
    details = {
        str(row.get("input_id")): {
            "severity": row.get("severity"),
            "pit_status": row.get("point_in_time_status"),
            "pit_confidence": row.get("pit_confidence"),
            "risk_flags": list(_as_list(row.get("risk_flags"))),
            "candidate_search_blocker": row.get("candidate_search_blocker") is True,
            "observation_blocker": row.get("observation_blocker") is True,
            "paper_shadow_blocker": row.get("paper_shadow_blocker") is True,
            "production_blocker": row.get("production_blocker") is True,
            "remediation_owner": row.get("remediation_owner"),
            "recommended_action": row.get("recommended_action"),
        }
        for row in blocking_rows
    }
    return {
        "schema_version": BLOCKER_SUMMARY_SCHEMA_VERSION,
        "scope": scope,
        "blocking_gaps": [str(row.get("input_id")) for row in blocking_rows],
        "blocking_gap_details": details,
        "candidate_search_allowed": gate_result.get("candidate_search_allowed") is True,
        "research_only_observation_allowed": (
            gate_result.get("research_only_observation_allowed") is True
        ),
        "paper_shadow_allowed": gate_result.get("paper_shadow_allowed") is True,
        "production_allowed": gate_result.get("production_allowed") is True,
        "blockers": list(_as_list(gate_result.get("blockers"))),
        "production_effect": "none",
        "broker_action": "none",
    }


def _blocker_codes(rows: list[Mapping[str, Any]]) -> list[str]:
    return [_blocker_code(str(row.get("input_id"))) for row in rows]


def _blocker_code(input_id: str) -> str:
    return f"BLOCKING_GAP_{input_id.upper()}"


def _has_input(rows: list[Mapping[str, Any]], input_id: str) -> bool:
    return any(row.get("input_id") == input_id for row in rows)


def _gate_derivation_sources() -> dict[str, list[str]]:
    return {
        "principle_based": [
            "no lookahead",
            "no future outcome dependency",
            "no stale signal carry-forward without explicit rule",
        ],
        "role_based": [
            "core return signal has stricter threshold",
            "execution semantic has stricter threshold",
            "regime label affects evaluation but not necessarily signal generation",
            "reporting input has lower severity",
        ],
        "phase_based": [
            "candidate search allows limited approximate PIT but not blocking core inputs",
            "research-only observation requires true or owner-approved approximate PIT",
            "paper-shadow requires stronger evidence",
            "production remains blocked",
        ],
        "empirical_status": [
            "policy_derived_safety_gate",
            "not_statistically_calibrated_yet",
            "threshold_meta_dataset_required_for_future_calibration",
        ],
    }


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []
