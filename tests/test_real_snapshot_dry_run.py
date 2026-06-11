from __future__ import annotations

from pathlib import Path

from real_snapshot_helpers import real_snapshot_dry_run_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_real_snapshot import (
    validate_real_snapshot_dry_run,
)


def test_real_snapshot_dry_run_links_manual_review_chain(tmp_path: Path) -> None:
    fixture = real_snapshot_dry_run_fixture(tmp_path)
    dry_run = fixture["dry_run"]
    summary = dry_run["real_snapshot_dry_run_summary"]
    links = dry_run["dry_run_artifact_links"]
    validation = validate_real_snapshot_dry_run(
        dry_run_id=dry_run["dry_run_id"],
        output_dir=tmp_path / "real_snapshot_dry_run",
    )

    assert validation["status"] == "PASS"
    assert links["snapshot_intake_id"] == fixture["intake"]["snapshot_intake_id"]
    assert links["manual_portfolio_snapshot_id"]
    assert links["exposure_id"]
    assert links["drift_id"]
    assert links["guardrail_id"]
    assert links["manual_execution_review_id"]
    assert summary["order_ticket_generated"] is False
    assert summary["broker_action_allowed"] is False
    assert summary["owner_approval_required"] is True
    assert (dry_run["dry_run_dir"] / "reader_brief_section.md").exists()
