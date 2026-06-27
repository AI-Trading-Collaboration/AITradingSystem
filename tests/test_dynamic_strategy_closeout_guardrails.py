from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUTS = PROJECT_ROOT / "inputs" / "research_reviews"
POLICY_PATH = PROJECT_ROOT / "config" / "research" / "dynamic_strategy_closeout_policy.yaml"


def test_dynamic_promotion_remains_blocked_after_closeout() -> None:
    final_status = _load_yaml(INPUTS / "dynamic_strategy_final_status.yaml")
    snapshot = _load_yaml(INPUTS / "dynamic_strategy_closeout_snapshot.yaml")
    policy = _load_yaml(POLICY_PATH)

    assert "PROMOTION_BLOCKED" in final_status["final_statuses"]
    assert final_status["status_controls"]["promotion"]["enabled"] is False
    assert snapshot["dynamic_promotion"] == "BLOCKED"
    assert snapshot["promotion_allowed"] is False
    assert policy["dynamic_strategy_policy"]["full_allocation"]["promotion_enabled"] is False
    assert policy["safety_boundary"]["promotion_allowed"] is False


def test_defensive_overlay_status_does_not_enable_full_allocation_promotion() -> None:
    overlay = _load_yaml(INPUTS / "dynamic_defensive_overlay_feasibility.yaml")
    disposition = _load_yaml(INPUTS / "dynamic_strategy_candidate_disposition_matrix.yaml")
    policy = _load_yaml(POLICY_PATH)

    overlay_policy = policy["dynamic_strategy_policy"]["defensive_overlay"]
    assert overlay_policy["status"] == "RESEARCH_ACTIVE"
    assert overlay_policy["execution_mode"] == "observe_only"
    assert overlay_policy["promotion_enabled"] is False
    assert overlay["overlay_conclusion"]["automatic_production_execution_allowed"] is False

    overlay_candidates = [
        row
        for row in disposition["candidate_rows"]
        if row["recommended_disposition"] == "DOWNGRADE_TO_DEFENSIVE_OVERLAY"
    ]
    assert overlay_candidates
    assert all(row["promotion_readiness"] == "BLOCKED" for row in overlay_candidates)


def test_advisory_diagnostic_status_does_not_enable_broker_execution() -> None:
    final_status = _load_yaml(INPUTS / "dynamic_strategy_final_status.yaml")
    policy = _load_yaml(POLICY_PATH)
    overlay = _load_yaml(INPUTS / "dynamic_defensive_overlay_feasibility.yaml")

    advisory_policy = policy["dynamic_strategy_policy"]["advisory_diagnostic"]
    assert advisory_policy["status"] == "ACTIVE"
    assert advisory_policy["output_only"] is True
    assert advisory_policy["broker_enabled"] is False
    assert final_status["status_controls"]["advisory_diagnostic"]["broker_enabled"] is False
    assert overlay["broker_action"] == "none"
    assert overlay["production_allowed"] is False


def test_legacy_dynamic_evidence_cannot_unlock_promotion() -> None:
    snapshot = _load_yaml(INPUTS / "dynamic_strategy_closeout_snapshot.yaml")
    blocker_inventory = _load_yaml(INPUTS / "dynamic_strategy_blocker_inventory.yaml")
    policy = _load_yaml(POLICY_PATH)

    assert snapshot["legacy_evidence_label"] == "CLOSEOUT_REVIEWED_LEGACY_EVIDENCE"
    assert snapshot["legacy_dynamic_evidence_can_unlock_promotion"] is False
    assert policy["legacy_evidence_policy"]["target_path_metrics_role"] == "diagnostic_only"
    assert policy["legacy_evidence_policy"]["legacy_dynamic_result_role"] == (
        "historical_research_evidence_only"
    )

    legacy_blocker = next(
        item
        for item in blocker_inventory["blockers"]
        if item["blocker_id"] == "DYN-CLOSEOUT-001_TARGET_PATH_LEGACY_EVIDENCE"
    )
    assert legacy_blocker["severity"] == "blocking"
    assert legacy_blocker["status"] == "accepted"


def test_target_path_metrics_cannot_reopen_full_allocation_research() -> None:
    reopen = _load_yaml(INPUTS / "dynamic_full_allocation_reopen_criteria.yaml")
    snapshot = _load_yaml(INPUTS / "dynamic_strategy_closeout_snapshot.yaml")
    policy = _load_yaml(POLICY_PATH)

    assert reopen["target_path_metrics_role"] == "diagnostic_only"
    assert "target_path_metrics" in reopen["disallowed_reopen_evidence"]
    assert snapshot["target_path_metrics_can_unlock_promotion"] is False
    can_reopen = policy["legacy_evidence_policy"][
        "target_path_metrics_can_reopen_full_allocation"
    ]
    assert can_reopen is False
    assert reopen["current_reopen_status"] == "NOT_ALLOWED"


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
