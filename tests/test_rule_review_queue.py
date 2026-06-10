from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    build_rule_review_queue,
    rule_review_queue_report_payload,
    validate_rule_review_queue_artifact,
)


def test_rule_review_queue_keeps_not_ready_items_out_of_owner_action(
    tmp_path: Path,
) -> None:
    fixture = cycle_fixture(tmp_path)
    queue = build_rule_review_queue(
        cycle_id=fixture["cycle"]["cycle_id"],
        output_dir=tmp_path / "rule_review_queue",
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 6, 10, 4, tzinfo=UTC),
    )

    summary = queue["queue_summary"]
    assert summary["pending_count"] == 0
    assert summary["ready_for_owner_review_count"] == 0
    assert summary["not_ready_count"] == 3
    assert all(row["queue_status"] == "not_ready" for row in queue["queue_items"])
    assert all(row["policy_change_allowed"] is False for row in queue["queue_items"])
    assert all(row["broker_action_allowed"] is False for row in queue["queue_items"])

    payload = rule_review_queue_report_payload(
        queue_id=queue["queue_id"],
        output_dir=tmp_path / "rule_review_queue",
    )
    assert payload["queue_id"] == queue["queue_id"]
    assert (
        validate_rule_review_queue_artifact(
            queue_id=queue["queue_id"],
            output_dir=tmp_path / "rule_review_queue",
        )["status"]
        == "PASS"
    )
