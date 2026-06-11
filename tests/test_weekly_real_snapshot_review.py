from __future__ import annotations

from datetime import date
from pathlib import Path

from real_snapshot_helpers import (
    real_snapshot_paper_action_fixture,
    report_index_for_real_snapshot_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    run_weekly_real_snapshot_review,
    validate_weekly_real_snapshot_review,
)
from ai_trading_system.reports import reader_brief


def test_weekly_real_snapshot_review_summarizes_latest_loop(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    weekly = run_weekly_real_snapshot_review(
        week_ending=date(2026, 6, 21),
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        owner_review_dir=tmp_path / "real_execution_owner_review",
        paper_action_dir=tmp_path / "real_snapshot_paper_action",
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    fixture["weekly_review"] = weekly
    validation = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly["weekly_real_review_id"],
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    summary = weekly["weekly_real_snapshot_summary"]

    assert validation["status"] == "PASS"
    assert summary["owner_decision"] == "monitor"
    assert summary["paper_action_taken"] is False
    assert summary["broker_action_taken"] is False
    assert summary["order_ticket_generated"] is False
    assert summary["next_action"] == "continue_monitoring"
    assert (weekly["weekly_real_review_dir"] / "reader_brief_section.md").exists()


def test_reader_brief_summarizes_weekly_real_snapshot_review(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    weekly = run_weekly_real_snapshot_review(
        week_ending=date(2026, 6, 21),
        dry_run_dir=tmp_path / "real_snapshot_dry_run",
        owner_review_dir=tmp_path / "real_execution_owner_review",
        paper_action_dir=tmp_path / "real_snapshot_paper_action",
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    fixture["weekly_review"] = weekly
    report_index = report_index_for_real_snapshot_review(fixture)

    summary = reader_brief._etf_dynamic_v3_real_snapshot_review_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["weekly_real_review_id"] == weekly["weekly_real_review_id"]
    assert summary["owner_decision"] == "monitor"
    assert summary["broker_action_taken"] is False
    assert summary["order_ticket_generated"] is False
    assert summary["next_action"] == "continue_monitoring"
