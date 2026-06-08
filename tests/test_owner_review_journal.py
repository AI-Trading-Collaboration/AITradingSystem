from __future__ import annotations

from datetime import date
from pathlib import Path

from dynamic_v3_position_readiness_helpers import (
    position_advisory_config,
    shadow_shortlist_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    create_owner_review,
    record_owner_review_decision,
    run_position_advisory_daily,
    run_shadow_shortlist_monitor,
    validate_owner_review_artifact,
)


def test_owner_review_create_and_record_monitor_decision(tmp_path: Path) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=daily["daily_advisory_id"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    updated = record_owner_review_decision(
        review_id=review["review_id"],
        decision="monitor",
        output_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    assert review["review"]["owner_decision"] == "pending"
    assert updated["review"]["owner_decision"] == "monitor"
    assert updated["review"]["broker_action_taken"] is False
    assert (
        validate_owner_review_artifact(
            review_id=review["review_id"],
            output_dir=tmp_path / "owner_review_journal",
        )["status"]
        == "PASS"
    )


def test_owner_review_paper_action_stays_paper_only(tmp_path: Path) -> None:
    daily = _daily_advisory(tmp_path)
    review = create_owner_review(
        daily_advisory_id=daily["daily_advisory_id"],
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        output_dir=tmp_path / "owner_review_journal",
    )
    updated = record_owner_review_decision(
        review_id=review["review_id"],
        decision="paper_adjustment",
        output_dir=tmp_path / "owner_review_journal",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
    )

    assert updated["review"]["paper_action"]["enabled"] is True
    assert updated["review"]["broker_action_taken"] is False
    assert (tmp_path / "owner_review_journal" / "paper_action_log.jsonl").exists()


def _daily_advisory(tmp_path: Path) -> dict[str, object]:
    fixture = shadow_shortlist_fixture(tmp_path)
    monitor = run_shadow_shortlist_monitor(
        shadow_shortlist_id=fixture["shadow"]["shadow_shortlist_id"],
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    return run_position_advisory_daily(
        shadow_monitor_run_id=monitor["monitor_run_id"],
        config_path=position_advisory_config(tmp_path),
        shadow_monitor_run_dir=tmp_path / "shadow_monitor_runs",
        output_dir=tmp_path / "position_advisory_daily",
    )
