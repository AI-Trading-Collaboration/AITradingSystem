from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from dynamic_v3_confirmation_cycle_helpers import cycle_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_cycle import (
    create_rule_owner_decision,
    record_rule_owner_decision,
    run_rule_review_cycle,
)
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
        generated_at=datetime(2026, 7, 31, 20, tzinfo=UTC),
    )

    summary = queue["queue_summary"]
    assert summary["pending_count"] == 0
    assert summary["ready_for_owner_review_count"] == 0
    assert summary["not_ready_count"] == 1
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

    summary_path = Path(queue["queue_dir"]) / "queue_summary.json"
    summary_path.write_text("{}\n", encoding="utf-8")
    assert (
        validate_rule_review_queue_artifact(
            queue_id=queue["queue_id"], output_dir=tmp_path / "rule_review_queue"
        )["status"]
        == "FAIL"
    )

    second_cycle = run_rule_review_cycle(
        registry_id=fixture["registry"]["registry_id"],
        progress_id=fixture["progress"]["progress_id"],
        evaluation_id=fixture["evaluation"]["evaluation_id"],
        registry_dir=fixture["registry_dir"],
        progress_dir=fixture["progress_dir"],
        evaluation_dir=fixture["evaluation_dir"],
        output_dir=fixture["cycle_dir"],
        generated_at=datetime(2026, 8, 1, tzinfo=UTC),
    )
    decision = create_rule_owner_decision(
        cycle_id=fixture["cycle"]["cycle_id"],
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 8, 1, 1, tzinfo=UTC),
    )
    record_rule_owner_decision(
        decision_id=decision["decision_id"],
        decision="continue_tracking",
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 8, 1, 2, tzinfo=UTC),
    )
    cross_cycle_queue = build_rule_review_queue(
        cycle_id=second_cycle["cycle_id"],
        output_dir=tmp_path / "cross_cycle_queue",
        cycle_dir=fixture["cycle_dir"],
        journal_path=fixture["journal_path"],
        generated_at=datetime(2026, 8, 1, 3, tzinfo=UTC),
    )
    assert all(row["queue_status"] != "reviewed" for row in cross_cycle_queue["queue_items"])
    assert all(not row["owner_decision_id"] for row in cross_cycle_queue["queue_items"])
    assert (
        validate_rule_review_queue_artifact(
            queue_id=cross_cycle_queue["queue_id"],
            output_dir=tmp_path / "cross_cycle_queue",
        )["status"]
        == "PASS"
    )
