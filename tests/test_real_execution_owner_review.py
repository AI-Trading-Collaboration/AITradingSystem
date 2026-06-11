from __future__ import annotations

from pathlib import Path

from real_snapshot_helpers import real_snapshot_dry_run_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    create_real_execution_owner_review,
    record_real_execution_owner_decision,
    validate_real_execution_owner_review,
)


def test_real_execution_owner_review_creation_and_recording(tmp_path: Path) -> None:
    fixture = real_snapshot_dry_run_fixture(tmp_path)
    owner_review = create_real_execution_owner_review(
        dry_run_id=fixture["dry_run"]["dry_run_id"],
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        output_dir=tmp_path / "real_execution_owner_review",
    )
    pending_validation = validate_real_execution_owner_review(
        review_id=owner_review["review_id"],
        output_dir=tmp_path / "real_execution_owner_review",
    )
    recorded = record_real_execution_owner_decision(
        review_id=owner_review["review_id"],
        decision="monitor",
        output_dir=tmp_path / "real_execution_owner_review",
    )
    recorded_validation = validate_real_execution_owner_review(
        review_id=owner_review["review_id"],
        output_dir=tmp_path / "real_execution_owner_review",
    )

    assert pending_validation["status"] == "PASS"
    assert recorded_validation["status"] == "PASS"
    assert recorded["decision"]["owner_decision"] == "monitor"
    assert recorded["decision"]["broker_action_taken"] is False
    assert recorded["decision"]["order_ticket_generated"] is False
    assert recorded["decision"]["production_effect"] == "none"
