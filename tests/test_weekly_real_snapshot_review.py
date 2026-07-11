from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from real_snapshot_helpers import (
    real_snapshot_owner_review_fixture,
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
        week_ending=datetime.now(UTC).date(),
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    fixture["weekly_review"] = weekly
    validation = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly["weekly_real_review_id"],
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    summary = weekly["weekly_real_snapshot_summary"]

    assert validation["status"] == "PASS"
    assert summary["chain_status"] == "COMPLETE"
    assert summary["owner_decision"] == "monitor"
    assert summary["paper_action_taken"] is False
    assert summary["broker_action_taken"] is False
    assert summary["order_ticket_generated"] is False
    assert summary["next_action"] == "continue_monitoring"
    assert (weekly["weekly_real_review_dir"] / "reader_brief_section.md").exists()


def test_reader_brief_summarizes_weekly_real_snapshot_review(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    weekly = run_weekly_real_snapshot_review(
        week_ending=datetime.now(UTC).date(),
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    fixture["weekly_review"] = weekly
    report_index = report_index_for_real_snapshot_review(fixture)

    summary = reader_brief._etf_dynamic_v3_real_snapshot_review_summary(report_index)

    assert summary["availability"] == "AVAILABLE"
    assert summary["weekly_real_review_id"] == weekly["weekly_real_review_id"]
    assert summary["chain_status"] == "COMPLETE"
    assert summary["owner_decision"] == "monitor"
    assert summary["broker_action_taken"] is False
    assert summary["order_ticket_generated"] is False
    assert summary["next_action"] == "continue_monitoring"


def test_weekly_review_does_not_cross_join_latest_paper_from_another_chain(
    tmp_path: Path,
) -> None:
    first = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    second = real_snapshot_owner_review_fixture(tmp_path, decision="needs_more_data")

    weekly = run_weekly_real_snapshot_review(
        week_ending=datetime.now(UTC).date(),
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    summary = weekly["weekly_real_snapshot_summary"]

    assert summary["latest_owner_review_id"] == second["owner_review"]["review_id"]
    assert summary["latest_dry_run_id"] == second["dry_run"]["dry_run_id"]
    assert summary["latest_paper_action_id"] == "MISSING"
    assert summary["latest_paper_action_id"] != first["paper_action"]["paper_action_id"]
    assert summary["chain_status"] == "PAPER_ACTION_MISSING"
    assert summary["next_action"] == "paper_action_tracking_required"

    first["weekly_review"] = weekly
    reader_summary = reader_brief._etf_dynamic_v3_real_snapshot_review_summary(
        report_index_for_real_snapshot_review(first)
    )
    assert reader_summary["chain_status"] == "PAPER_ACTION_MISSING"
    assert reader_summary["latest_owner_review_id"] == second["owner_review"]["review_id"]
    assert reader_summary["paper_action_path"] == ""


def test_weekly_review_excludes_sources_after_week_ending(tmp_path: Path) -> None:
    real_snapshot_paper_action_fixture(tmp_path, decision="monitor")

    weekly = run_weekly_real_snapshot_review(
        week_ending=datetime.now(UTC).date() - timedelta(days=1),
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    summary = weekly["weekly_real_snapshot_summary"]
    validation = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly["weekly_real_review_id"],
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )

    assert validation["status"] == "PASS"
    assert summary["chain_status"] == "MISSING_DRY_RUN"
    assert summary["latest_dry_run_id"] == "MISSING"
    assert summary["latest_owner_review_id"] == "MISSING"
    assert summary["latest_paper_action_id"] == "MISSING"


def test_weekly_review_validator_detects_summary_and_source_drift(tmp_path: Path) -> None:
    fixture = real_snapshot_paper_action_fixture(tmp_path, decision="monitor")
    weekly = run_weekly_real_snapshot_review(
        week_ending=datetime.now(UTC).date(),
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )
    summary_path = weekly["weekly_real_review_dir"] / "weekly_real_snapshot_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["owner_decision"] = "no_trade"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_validation = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly["weekly_real_review_id"],
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )

    decision_path = (
        fixture["owner_review"]["review_dir"] / "owner_execution_decision.json"
    )
    decision = json.loads(decision_path.read_text(encoding="utf-8"))
    decision["owner_notes"] = "source changed"
    decision_path.write_text(
        json.dumps(decision, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    source_validation = validate_weekly_real_snapshot_review(
        weekly_real_review_id=weekly["weekly_real_review_id"],
        **_source_roots(tmp_path),
        output_dir=tmp_path / "weekly_real_snapshot_review",
    )

    assert output_validation["status"] == "FAIL"
    assert "weekly_summary_content_derived" in _failed_check_ids(output_validation)
    assert source_validation["status"] == "FAIL"
    assert "source_checksums_match" in _failed_check_ids(source_validation)


def _source_roots(root: Path) -> dict[str, Path]:
    return {
        "dry_run_dir": root / "real_snapshot_dry_run",
        "owner_review_dir": root / "real_execution_owner_review",
        "paper_action_dir": root / "real_snapshot_paper_action",
        "manual_snapshot_dir": root / "manual_portfolio_snapshot",
        "drift_dir": root / "position_drift",
        "guardrail_dir": root / "execution_guardrails",
    }


def _failed_check_ids(payload: dict[str, object]) -> set[str]:
    checks = payload["checks"]
    assert isinstance(checks, list)
    return {
        str(row["check_id"])
        for row in checks
        if isinstance(row, dict) and row.get("passed") is not True
    }
