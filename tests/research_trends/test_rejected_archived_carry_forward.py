from __future__ import annotations

from pathlib import Path

from regenerated_candidate_test_helpers import (
    build_scope_narrowed_forward_observe_readiness_fixture,
)

from ai_trading_system.scope_narrowed_forward_observe_readiness_review import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    RISK_CAP_CANDIDATE_ID,
    build_archived_candidate_carry_forward_matrix,
    build_rejected_candidate_carry_forward_matrix,
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


def test_baseline_confirmation_rejected_carry_forward_exists(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    rows = build_rejected_candidate_carry_forward_matrix(
        inputs,
        {"candidate_id": RISK_CAP_CANDIDATE_ID},
    )

    assert rows[0]["candidate_id"] == CONFIRMATION_CANDIDATE_ID
    assert rows[0]["carry_forward_status"] == "rejected_current_form"
    assert rows[0]["included_in_forward_observe_readiness"] is False


def test_risk_appetite_archived_carry_forward_exists(tmp_path: Path) -> None:
    rows = build_archived_candidate_carry_forward_matrix(_inputs(tmp_path))

    assert rows[0]["candidate_id"] == RISK_APPETITE_ARCHIVE_CANDIDATE
    assert rows[0]["carry_forward_status"] == "archived_current_form"
    assert rows[0]["included_in_forward_observe_readiness"] is False


def test_carry_forward_rows_preserve_safety_gates(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    rows = build_rejected_candidate_carry_forward_matrix(
        inputs,
        {"candidate_id": RISK_CAP_CANDIDATE_ID},
    ) + build_archived_candidate_carry_forward_matrix(inputs)

    for row in rows:
        assert row["promotion_allowed"] is False
        assert row["paper_shadow_allowed"] is False
        assert row["production_allowed"] is False
        assert row["broker_action"] == "none"
