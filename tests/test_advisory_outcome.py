from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_daily_advisory,
    write_market_cache,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_advisory_outcome_artifact,
)


def test_advisory_outcome_track_creates_pending_windows(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_daily_advisory(tmp_path)

    result = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )

    assert result["manifest"]["status"] == "PENDING"
    assert result["manifest"]["tracked_windows"] == [1, 5, 10, 20]
    assert {row["outcome_status"] for row in result["outcome_windows"]} == {"PENDING"}
    assert (
        validate_advisory_outcome_artifact(
            outcome_id=result["outcome_id"],
            output_dir=tmp_path / "advisory_outcome",
        )["status"]
        == "PASS"
    )


def test_advisory_outcome_update_marks_available_with_quality_gate(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    review = write_validated_owner_review(
        tmp_path / "validated_source",
        owner_decision="paper_adjustment",
        as_of=date(2026, 6, 8),
    )
    apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=review["owner_review_dir"],
        daily_advisory_dir=review["daily_advisory_dir"],
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )
    prices_path, rates_path = write_market_cache(tmp_path, start="2026-06-08")

    updated = update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "advisory_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )

    assert updated["manifest"]["status"] == "AVAILABLE"
    assert updated["manifest"]["data_quality_status"] == "PASS"
    assert {row["outcome_status"] for row in updated["outcome_windows"]} == {"AVAILABLE"}
    assert updated["advisory_event"]["paper_action_weights"] != {
        "CASH": 0.2,
        "QQQ": 0.5,
        "SMH": 0.2,
        "TLT": 0.1,
    }


def test_advisory_outcome_update_marks_pending_and_insufficient_data(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_daily_advisory(tmp_path, as_of="2026-06-08")
    future = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "future_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )

    future_updated = update_advisory_outcome(
        outcome_id=future["outcome_id"],
        as_of=date(2026, 6, 14),
        output_dir=tmp_path / "future_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, tzinfo=UTC),
    )

    assert future_updated["manifest"]["status"] == "PENDING"
    assert future_updated["manifest"]["data_quality_status"] == "NOT_RUN_FUTURE_AS_OF"

    short = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "short_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
    )
    prices_path, rates_path = write_market_cache(
        tmp_path / "short_cache",
        start="2026-06-08",
        end="2026-06-08",
    )

    short_updated = update_advisory_outcome(
        outcome_id=short["outcome_id"],
        as_of=date(2026, 6, 8),
        output_dir=tmp_path / "short_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 6, 8, tzinfo=UTC),
    )

    assert short_updated["manifest"]["status"] == "INSUFFICIENT_DATA"
    assert {row["outcome_status"] for row in short_updated["outcome_windows"]} == {
        "INSUFFICIENT_DATA"
    }
