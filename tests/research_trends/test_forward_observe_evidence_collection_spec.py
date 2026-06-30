from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    build_forward_observe_daily_report_contract,
    build_forward_observe_evidence_collection_spec,
    build_forward_observe_weekly_review_contract,
    load_scope_narrowed_forward_observe_readiness_inputs,
)


def _inputs(tmp_path: Path):
    fixture = build_scope_narrowed_forward_observe_readiness_fixture(tmp_path)
    return load_scope_narrowed_forward_observe_readiness_inputs(
        scope_validation_dir=fixture["scope_validation_dir"],
        scope_generator_dir=fixture["scope_narrowed_generator_dir"],
        scope_review_dir=fixture["scope_review_dir"],
        candidate=RISK_CAP_CANDIDATE_ID,
        rejected_candidates=CONFIRMATION_CANDIDATE_ID,
        archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
    )


def test_evidence_collection_spec_contains_daily_weekly_trigger_and_outcome_fields(
    tmp_path: Path,
) -> None:
    spec = build_forward_observe_evidence_collection_spec(_inputs(tmp_path))

    assert "risk_cap_triggered" in spec["daily_evidence_fields"]
    assert "active_trigger_count" in spec["weekly_review_fields"]
    assert "risk_cap_intensity" in spec["trigger_event_fields"]
    assert "post_trigger_max_drawdown" in spec["outcome_followup_fields"]
    assert "manual_review_notes" in spec["manual_review_fields"]


def test_evidence_collection_spec_is_observe_only(tmp_path: Path) -> None:
    spec = build_forward_observe_evidence_collection_spec(_inputs(tmp_path))

    assert spec["allowed_action"] == "observe_only"
    assert "target_weight" not in spec["daily_evidence_fields"]
    assert spec["broker_action"] == "none"
    assert spec["forward_observe_started"] is False


def test_daily_report_contract_allows_only_observe_action(tmp_path: Path) -> None:
    contract = build_forward_observe_daily_report_contract(_inputs(tmp_path))

    assert contract["allowed_action"] == "observe_only"
    assert "sell" in contract["forbidden_allowed_actions"]
    assert "target_weight" in contract["forbidden_fields"]


def test_weekly_review_contract_preserves_safety_gates(tmp_path: Path) -> None:
    contract = build_forward_observe_weekly_review_contract(_inputs(tmp_path))

    assert "active_trigger_count" in contract["fields"]
    assert contract["promotion_allowed"] is False
    assert contract["paper_shadow_allowed"] is False
    assert contract["production_allowed"] is False
    assert contract["broker_action"] == "none"
