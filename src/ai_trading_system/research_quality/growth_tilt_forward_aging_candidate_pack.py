from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_forward_aging_candidate_pack.v1"
FORWARD_AGING_PACK_SCHEMA_VERSION = "growth_tilt_forward_aging_candidate_pack_details.v1"
CANDIDATE_TRACKING_SCHEMA_VERSION = "growth_tilt_forward_aging_candidate_tracking.v1"
FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION = (
    "growth_tilt_forward_observation_contract.v1"
)
NO_EFFECT_BOUNDARY_SCHEMA_VERSION = "growth_tilt_forward_aging_no_effect.v1"

READY_STATUS = "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_READY"
BLOCKED_PIT_REPLAY_STATUS = (
    "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE"
)
BLOCKED_DATA_QUALITY_STATUS = (
    "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_DATA_QUALITY_GATE"
)
BLOCKED_EVIDENCE_STATUS = (
    "GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_EVIDENCE_GAPS"
)
EXPECTED_2438_STATUS = "GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_READY"
EXPECTED_2438_NEXT_ROUTE = "TRADING-2439_Growth_Tilt_Forward_Aging_Candidate_Pack"
OBSERVATION_HORIZONS: tuple[str, ...] = ("1d", "5d", "10d", "20d")
NEXT_ROUTE = "TRADING-2440_Growth_Tilt_Paper_Shadow_Candidate_Promotion_Review"
BLOCKED_ROUTE = "TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation"
REPORT_TYPE = "growth_tilt_forward_aging_candidate_pack"
REQUIRED_REPORT_IDS: tuple[str, ...] = (
    REPORT_TYPE,
    "growth_tilt_top3_candidate_pit_replay",
)
REQUIRED_CATALOG_REFERENCES: tuple[str, ...] = (
    "aits research strategies growth-tilt-forward-aging-candidate-pack",
    "outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/"
    "forward_aging_candidate_pack_result.json",
    "outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/"
    "forward_aging_candidate_pack.json",
    "outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/"
    "candidate_tracking_artifact.json",
    "outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/"
    "forward_observation_contract.json",
    "outputs/research_strategies/growth_tilt_forward_aging_candidate_pack/"
    "no_effect_boundary.json",
    "docs/research/growth_tilt_forward_aging_candidate_pack.md",
    "docs/research/growth_tilt_forward_aging_candidate_pack_details.md",
    "docs/research/growth_tilt_forward_aging_candidate_tracking.md",
    "docs/research/growth_tilt_forward_observation_contract.md",
    "docs/research/growth_tilt_forward_aging_no_effect_boundary.md",
    "docs/research/dynamic_strategy_2439_blocked_route.md",
)
REQUIRED_SYSTEM_FLOW_REFERENCES: tuple[str, ...] = (
    "growth-tilt-forward-aging-candidate-pack",
    BLOCKED_PIT_REPLAY_STATUS,
    BLOCKED_ROUTE,
)
PIT_REPLAY_REQUIREMENT_IDS: tuple[str, ...] = (
    "source_2438_top3_pit_replay_ready",
    "pit_replay_pass_candidate_available",
)


def build_growth_tilt_forward_aging_candidate_pack(
    source_2438_pit_replay: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
    pit_replay_blocker_summary: Mapping[str, Any],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_doc_texts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    forward_candidates = _select_forward_aging_candidates(
        source_2438_pit_replay,
        pit_replay_evidence,
    )
    research_text = "\n".join(str(text) for text in (research_doc_texts or {}).values())
    requirements = _requirements(
        source_2438_pit_replay,
        forward_candidates,
        data_quality_summary,
        report_registry=report_registry,
        artifact_catalog_text=artifact_catalog_text,
        system_flow_text=system_flow_text,
        research_text=research_text,
    )
    gaps = [
        _gap_from_requirement(requirement)
        for requirement in requirements
        if requirement["status"] != "PASS"
    ]
    status = _status_from_gaps(gaps)
    candidates = forward_candidates if status == READY_STATUS else []
    pack = _forward_aging_pack(status, candidates, gaps)
    tracking = _candidate_tracking_artifact(status, candidates, gaps)
    contract = _forward_observation_contract(status, gaps)
    boundary = _no_effect_boundary(status, gaps)

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2439",
        "status": status,
        "readiness_status": status,
        "source_tasks": ["TRADING-2438"],
        "source_2438_ready": _source_2438_ready(source_2438_pit_replay),
        "pit_replay_source_status": source_2438_pit_replay.get("status"),
        "pit_replay_pass_candidate_count": len(forward_candidates),
        "pit_replay_pass_count_from_source": int(
            source_2438_pit_replay.get("pit_replay_pass_count", 0) or 0
        ),
        "pit_replay_tested_count_from_source": int(
            source_2438_pit_replay.get("pit_candidates_tested", 0) or 0
        ),
        "pit_replay_blocked_count_from_source": int(
            source_2438_pit_replay.get("pit_replay_blocked_count", 0) or 0
        ),
        "pit_replay_blocker_summary_status": pit_replay_blocker_summary.get("status"),
        "forward_aging_candidate_pack_ready": pack[
            "forward_aging_candidate_pack_ready"
        ],
        "candidate_tracking_artifact_ready": tracking[
            "candidate_tracking_artifact_ready"
        ],
        "forward_observation_contract_ready": contract[
            "forward_observation_contract_ready"
        ],
        "no_effect_boundary_ready": boundary["no_effect_boundary_ready"],
        "data_quality_gate_executed": data_quality_summary.get(
            "data_quality_gate_executed"
        )
        is True,
        "data_quality_gate_passed": _data_quality_passed(data_quality_summary),
        "data_quality_status": data_quality_summary.get("data_quality_status"),
        "data_quality_report_path": data_quality_summary.get("data_quality_report_path"),
        "forward_aging_candidate_count": len(candidates),
        "forward_aging_candidate_count_if_unblocked": len(forward_candidates),
        "observation_horizons": list(OBSERVATION_HORIZONS),
        "valid_until_outcome_capture_ready": status == READY_STATUS,
        "candidate_evidence_refresh_cadence": (
            "daily_after_observation_window_maturity"
            if status == READY_STATUS
            else "not_started_pit_replay_gate_blocked"
        ),
        "forward_aging_candidate_pack": pack,
        "candidate_tracking_artifact": tracking,
        "forward_observation_contract": contract,
        "no_effect_boundary": boundary,
        "requirements": requirements,
        "gaps": gaps,
        "evidence_gap_count": len(gaps),
        "evidence_gap_ids": [gap["requirement_id"] for gap in gaps],
        "market_data_experiment_run": False,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "candidate_tracking_started": status == READY_STATUS,
        "historical_screen_run": False,
        "pit_replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "daily_report_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "manual_review_required": True,
        "manual_review_only": True,
        "observe_only": True,
        "generated_signal": False,
        "new_signal_generated": False,
        "generated_trading_advice": False,
        "trading_advice_generated": False,
        "actionable_allocation_generated": False,
        "outcome_backfilled": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "recommended_next_research_task": NEXT_ROUTE
        if status == READY_STATUS
        else BLOCKED_ROUTE,
        "recommended_next_research_task_reason": (
            "Forward aging candidate pack is ready for promotion review."
            if status == READY_STATUS
            else "TRADING-2438 did not produce PIT replay pass candidates; "
            "remediate the PIT replay engine/input specs before forward aging."
        ),
    }


def _requirements(
    source_2438: Mapping[str, Any],
    forward_candidates: Sequence[Mapping[str, Any]],
    data_quality_summary: Mapping[str, Any],
    *,
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    research_text: str,
) -> list[dict[str, Any]]:
    return [
        _requirement(
            "source_2438_top3_pit_replay_ready",
            _source_2438_ready(source_2438),
            "pit_replay_gate_gap",
            {
                "status": source_2438.get("status"),
                "next_route": source_2438.get("recommended_next_research_task"),
                "pit_candidates_tested": source_2438.get("pit_candidates_tested"),
                "pit_replay_pass_count": source_2438.get("pit_replay_pass_count"),
            },
        ),
        _requirement(
            "pit_replay_pass_candidate_available",
            len(forward_candidates) > 0,
            "pit_replay_gate_gap",
            {"pit_replay_pass_candidate_count": len(forward_candidates)},
        ),
        _requirement(
            "data_quality_gate_passed",
            _data_quality_passed(data_quality_summary),
            "data_quality_gate_gap",
            {
                "data_quality_status": data_quality_summary.get("data_quality_status"),
                "report_path": data_quality_summary.get("data_quality_report_path"),
            },
        ),
        _requirement(
            "prior_research_doc_coverage",
            "PIT" in research_text or "pit" in research_text,
            "research_doc_gap",
            {"required_references": ["PIT replay", "forward aging"]},
        ),
        _requirement(
            "report_registry_coverage",
            _report_registry_has(report_registry, REQUIRED_REPORT_IDS),
            "registry_catalog_doc_gap",
            {"required_report_ids": list(REQUIRED_REPORT_IDS)},
        ),
        _requirement(
            "artifact_catalog_coverage",
            _contains_all(artifact_catalog_text, REQUIRED_CATALOG_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_CATALOG_REFERENCES)},
        ),
        _requirement(
            "system_flow_coverage",
            _contains_all(system_flow_text, REQUIRED_SYSTEM_FLOW_REFERENCES),
            "registry_catalog_doc_gap",
            {"required_references": list(REQUIRED_SYSTEM_FLOW_REFERENCES)},
        ),
    ]


def _source_2438_ready(payload: Mapping[str, Any]) -> bool:
    return (
        payload.get("status") == EXPECTED_2438_STATUS
        and payload.get("recommended_next_research_task") == EXPECTED_2438_NEXT_ROUTE
        and int(payload.get("pit_candidates_tested", 0) or 0) > 0
        and int(payload.get("pit_replay_pass_count", 0) or 0) > 0
        and payload.get("pit_replay_executed") is True
    )


def _data_quality_passed(data_quality_summary: Mapping[str, Any]) -> bool:
    if data_quality_summary.get("data_quality_gate_passed") is True:
        return True
    status = str(data_quality_summary.get("data_quality_status", ""))
    return status in {"PASS", "PASS_WITH_WARNINGS"}


def _select_forward_aging_candidates(
    source_2438: Mapping[str, Any],
    pit_replay_evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if not _source_2438_ready(source_2438):
        return []
    rows = _sequence(_mapping(pit_replay_evidence.get("pit_replay_evidence")).get("rows"))
    if not rows:
        rows = _sequence(_mapping(source_2438.get("pit_replay_evidence")).get("rows"))
    candidates: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping) or row.get("pit_replay_passed") is not True:
            continue
        candidates.append(
            {
                "candidate_id": row.get("candidate_id"),
                "status": "ready_for_forward_aging",
                "primary_value": row.get("primary_value"),
                "key_risk": row.get("key_risk", "forward_outcome_not_yet_observed"),
                "observation_horizons": list(OBSERVATION_HORIZONS),
                "valid_until_outcome_capture_required": True,
                "evidence_refresh_cadence": "daily_after_observation_window_maturity",
                "paper_shadow_candidate": False,
                "production_effect": "none",
                "broker_action": "none",
            }
        )
    return candidates


def _status_from_gaps(gaps: Sequence[Mapping[str, Any]]) -> str:
    if not gaps:
        return READY_STATUS
    gap_ids = {str(gap.get("requirement_id")) for gap in gaps}
    if "data_quality_gate_passed" in gap_ids:
        return BLOCKED_DATA_QUALITY_STATUS
    if gap_ids.intersection(PIT_REPLAY_REQUIREMENT_IDS):
        return BLOCKED_PIT_REPLAY_STATUS
    return BLOCKED_EVIDENCE_STATUS


def _forward_aging_pack(
    status: str,
    candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": FORWARD_AGING_PACK_SCHEMA_VERSION,
        "status": status,
        "forward_aging_candidate_pack_ready": status == READY_STATUS,
        "forward_aging_candidate_count": len(candidates),
        "observation_horizons": list(OBSERVATION_HORIZONS),
        "candidates": list(candidates),
        "valid_until_outcome_capture_ready": status == READY_STATUS,
        "candidate_evidence_refresh_cadence": (
            "daily_after_observation_window_maturity"
            if status == READY_STATUS
            else "not_started_pit_replay_gate_blocked"
        ),
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "production_effect": "none",
        "broker_action": "none",
    }


def _candidate_tracking_artifact(
    status: str,
    candidates: Sequence[Mapping[str, Any]],
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": CANDIDATE_TRACKING_SCHEMA_VERSION,
        "status": status,
        "candidate_tracking_artifact_ready": True,
        "tracking_started": status == READY_STATUS,
        "forward_aging_candidate_count": len(candidates),
        "tracking_rows": [
            {
                "candidate_id": candidate.get("candidate_id"),
                "tracking_status": "pending_forward_observation",
                "observation_horizons": list(OBSERVATION_HORIZONS),
                "valid_until_outcome_capture_required": True,
                "production_effect": "none",
                "broker_action": "none",
            }
            for candidate in candidates
        ],
        "blocked_reason": None
        if status == READY_STATUS
        else "pit_replay_gate_not_ready",
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "production_effect": "none",
        "broker_action": "none",
    }


def _forward_observation_contract(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": FORWARD_OBSERVATION_CONTRACT_SCHEMA_VERSION,
        "status": status,
        "forward_observation_contract_ready": True,
        "observation_horizons": list(OBSERVATION_HORIZONS),
        "valid_until_outcome_capture_required": True,
        "baseline_comparison_required": True,
        "candidate_evidence_refresh_cadence": (
            "daily_after_observation_window_maturity"
            if status == READY_STATUS
            else "not_started_pit_replay_gate_blocked"
        ),
        "outcome_capture_started": False,
        "forward_observation_started": False,
        "evidence_gap_count": len(gaps),
        "blocking_gap_ids": [gap["requirement_id"] for gap in gaps],
        "production_effect": "none",
        "broker_action": "none",
    }


def _no_effect_boundary(
    status: str,
    gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": NO_EFFECT_BOUNDARY_SCHEMA_VERSION,
        "status": status,
        "no_effect_boundary_ready": True,
        "paper_shadow_enabled": False,
        "paper_shadow_schedule_enabled": False,
        "production_enabled": False,
        "broker_enabled": False,
        "automatic_execution_allowed": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_order_generated": False,
        "portfolio_weight_mutated": False,
        "forward_aging_observation_started": False,
        "forward_aging_observation_written": False,
        "outcome_binding_executed": False,
        "outcome_store_mutated": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "evidence_gap_count": len(gaps),
        "gaps": list(gaps),
        "production_effect": "none",
        "broker_action": "none",
    }


def _requirement(
    requirement_id: str,
    passed: bool,
    classification: str,
    evidence: Any,
) -> dict[str, Any]:
    return {
        "requirement_id": requirement_id,
        "status": "PASS" if passed else "FAIL",
        "classification": classification,
        "evidence": evidence,
        "production_effect": "none",
        "broker_action": "none",
    }


def _gap_from_requirement(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "requirement_id": requirement["requirement_id"],
        "classification": requirement["classification"],
        "gap": f"{requirement['requirement_id']} did not pass.",
        "evidence": requirement.get("evidence"),
        "production_effect": "none",
        "broker_action": "none",
    }


def _report_registry_has(
    report_registry: Mapping[str, Any],
    report_ids: Sequence[str],
) -> bool:
    reports = report_registry.get("reports")
    if not isinstance(reports, Sequence):
        return False
    present = {
        str(report.get("report_id"))
        for report in reports
        if isinstance(report, Mapping) and report.get("report_id")
    }
    return set(report_ids).issubset(present)


def _contains_all(text: str, references: Sequence[str]) -> bool:
    return all(reference in text for reference in references)


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, str):
        return value
    return ()


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
