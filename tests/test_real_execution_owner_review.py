from __future__ import annotations

import json
from pathlib import Path

import pytest
from real_snapshot_helpers import real_snapshot_dry_run_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    RealSnapshotError,
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


def test_owner_review_rejects_and_detects_sensitive_notes(tmp_path: Path) -> None:
    fixture = real_snapshot_dry_run_fixture(tmp_path)
    owner_review = create_real_execution_owner_review(
        dry_run_id=fixture["dry_run"]["dry_run_id"],
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        output_dir=tmp_path / "real_execution_owner_review",
    )
    review_dir = owner_review["review_dir"]

    with pytest.raises(RealSnapshotError, match="owner notes contain sensitive account data"):
        record_real_execution_owner_decision(
            review_id=owner_review["review_id"],
            decision="monitor",
            owner_notes="broker account 123456",
            output_dir=tmp_path / "real_execution_owner_review",
        )

    unchanged = json.loads(
        (review_dir / "owner_execution_decision.json").read_text(encoding="utf-8")
    )
    assert unchanged["owner_decision"] == "pending"
    assert unchanged["owner_notes"] == ""

    unchanged["owner_notes"] = "statement_path C:/private/broker.pdf"
    (review_dir / "owner_execution_decision.json").write_text(
        json.dumps(unchanged, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_path = review_dir / "real_execution_owner_review_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["owner_notes"] = "tax_lot details"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validation = validate_real_execution_owner_review(
        review_id=owner_review["review_id"],
        output_dir=tmp_path / "real_execution_owner_review",
    )

    assert validation["status"] == "FAIL"
    failed_checks = {
        row["check_id"] for row in validation["checks"] if row["passed"] is not True
    }
    assert "owner_notes_redaction_safe" in failed_checks
