from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from dynamic_v3_paper_tracking_helpers import (
    paper_config_path,
    write_market_cache,
    write_validated_daily_advisory,
    write_validated_owner_review,
)

from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DynamicV3PaperTrackingError,
    _compute_outcome_window_rows,
    _outcome_update_event_checksum,
    _rollup_outcome_status,
    apply_owner_review_to_paper_portfolio,
    init_paper_portfolio,
    track_advisory_outcome,
    update_advisory_outcome,
    validate_advisory_outcome_artifact,
)


def test_advisory_outcome_track_creates_pending_windows(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 7))

    result = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 7, 15, tzinfo=UTC),
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
        generated_at=datetime(2026, 6, 8, 14, tzinfo=UTC),
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "advisory_outcome",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
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
    assert updated["outcome_windows"][0]["paper_action_effective_date"] == "2026-06-09"
    assert (
        validate_advisory_outcome_artifact(
            outcome_id=outcome["outcome_id"],
            output_dir=tmp_path / "advisory_outcome",
        )["status"]
        == "PASS"
    )


def test_advisory_outcome_update_marks_pending_and_insufficient_data(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    future = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "future_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )

    future_updated = update_advisory_outcome(
        outcome_id=future["outcome_id"],
        as_of=date(2026, 6, 14),
        output_dir=tmp_path / "future_outcome",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 16, tzinfo=UTC),
    )

    assert future_updated["manifest"]["status"] == "PENDING"
    assert future_updated["manifest"]["data_quality_status"] == "NOT_RUN_FUTURE_AS_OF"

    short = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "short_outcome",
        daily_advisory_dir=tmp_path / "position_advisory_daily",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
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
        generated_at=datetime(2026, 6, 8, 16, tzinfo=UTC),
    )

    assert short_updated["manifest"]["status"] == "INSUFFICIENT_DATA"
    assert {row["outcome_status"] for row in short_updated["outcome_windows"]} == {
        "INSUFFICIENT_DATA"
    }
    assert all(
        row["relative_to_no_trade"] is None
        and row["insufficient_reason"] == "MISSING_COMPLETE_TRADING_DATES"
        for row in short_updated["outcome_windows"]
    )


def test_advisory_outcome_update_is_append_only_and_rejects_time_reversal(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    outcome = track_advisory_outcome(
        daily_advisory_id=advisory["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "outcomes",
        daily_advisory_dir=advisory["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(tmp_path / "cache", start="2026-06-08")
    event_path = outcome["outcome_dir"] / "advisory_event.json"
    immutable_event = event_path.read_bytes()
    update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "outcomes",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    ledger_path = outcome["outcome_dir"] / "outcome_update_events.jsonl"
    first_line = ledger_path.read_text(encoding="utf-8").splitlines()[0]
    update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "outcomes",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 16, tzinfo=UTC),
    )
    assert event_path.read_bytes() == immutable_event
    assert ledger_path.read_text(encoding="utf-8").splitlines()[0] == first_line
    with pytest.raises(DynamicV3PaperTrackingError, match="cannot move backward"):
        update_advisory_outcome(
            outcome_id=outcome["outcome_id"],
            as_of=date(2026, 7, 9),
            output_dir=tmp_path / "outcomes",
            paper_portfolio_dir=tmp_path / "paper_portfolio",
            prices_path=prices_path,
            rates_path=rates_path,
            generated_at=datetime(2026, 7, 17, tzinfo=UTC),
        )
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 2


def test_advisory_outcome_excludes_paper_action_not_known_at_update(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    review = write_validated_owner_review(
        tmp_path / "source", owner_decision="paper_adjustment", as_of=date(2026, 6, 8)
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "outcomes",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=review["owner_review_dir"],
        daily_advisory_dir=review["daily_advisory_dir"],
        generated_at=datetime(2026, 7, 20, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(tmp_path / "cache", start="2026-06-08")
    updated = update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "outcomes",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    binding = updated["update_event"]["paper_action_binding"]
    assert binding["status"] == "NOT_RECORDED_BY_AS_OF"
    assert binding["excluded_future_action_count"] == 1
    assert all(
        row["paper_portfolio_return"] == row["no_trade_return"]
        and row["paper_action_effective_date"] == ""
        for row in updated["outcome_windows"]
    )


def test_advisory_outcome_cost_policy_and_content_replay_validation(
    tmp_path: Path,
) -> None:
    config_path = paper_config_path(
        tmp_path, transaction_cost_bps=8, slippage_bps=2
    )
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    review = write_validated_owner_review(
        tmp_path / "source", owner_decision="paper_adjustment", as_of=date(2026, 6, 8)
    )
    action = apply_owner_review_to_paper_portfolio(
        review_id=review["review_id"],
        config_path=config_path,
        output_dir=tmp_path / "paper_portfolio",
        owner_review_dir=review["owner_review_dir"],
        daily_advisory_dir=review["daily_advisory_dir"],
        generated_at=datetime(2026, 6, 8, 14, tzinfo=UTC),
    )
    outcome = track_advisory_outcome(
        daily_advisory_id=review["daily_advisory_id"],
        config_path=config_path,
        output_dir=tmp_path / "outcomes",
        daily_advisory_dir=review["daily_advisory_dir"],
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        generated_at=datetime(2026, 6, 8, 15, tzinfo=UTC),
    )
    prices_path, rates_path = write_market_cache(tmp_path / "cache", start="2026-06-08")
    updated = update_advisory_outcome(
        outcome_id=outcome["outcome_id"],
        as_of=date(2026, 7, 10),
        output_dir=tmp_path / "outcomes",
        paper_portfolio_dir=tmp_path / "paper_portfolio",
        prices_path=prices_path,
        rates_path=rates_path,
        generated_at=datetime(2026, 7, 15, tzinfo=UTC),
    )
    turnover = 0.5 * sum(
        abs(value) for value in action["event"]["applied_paper_deltas"].values()
    )
    assert updated["outcome_windows"][0]["paper_transaction_cost"] == pytest.approx(
        turnover * 0.001
    )
    ledger_path = outcome["outcome_dir"] / "outcome_update_events.jsonl"
    event = json.loads(ledger_path.read_text(encoding="utf-8").splitlines()[0])
    event["outcome_windows"][0]["paper_portfolio_return"] += 0.2
    event["event_checksum"] = _outcome_update_event_checksum(event)
    ledger_path.write_text(json.dumps(event, sort_keys=True) + "\n", encoding="utf-8")
    validation = validate_advisory_outcome_artifact(
        outcome_id=outcome["outcome_id"], output_dir=tmp_path / "outcomes"
    )
    assert validation["status"] == "FAIL"
    assert any(
        check["check_id"] == "source_and_update_replay" and not check["passed"]
        for check in validation["checks"]
    )


def test_advisory_outcome_blocks_duplicate_tracker(tmp_path: Path) -> None:
    config_path = paper_config_path(tmp_path)
    init_paper_portfolio(config_path=config_path, output_dir=tmp_path / "paper_portfolio")
    advisory = write_validated_daily_advisory(tmp_path, as_of=date(2026, 6, 8))
    kwargs = {
        "daily_advisory_id": advisory["daily_advisory_id"],
        "config_path": config_path,
        "output_dir": tmp_path / "outcomes",
        "daily_advisory_dir": advisory["daily_advisory_dir"],
        "paper_portfolio_dir": tmp_path / "paper_portfolio",
        "generated_at": datetime(2026, 6, 8, 15, tzinfo=UTC),
    }
    track_advisory_outcome(**kwargs)
    with pytest.raises(DynamicV3PaperTrackingError, match="already tracked"):
        track_advisory_outcome(**kwargs)


def test_outcome_math_marks_incomplete_required_symbol_path_as_partial() -> None:
    dates = [item.date() for item in pd.bdate_range("2026-06-08", periods=24)]
    rows = [
        {"_date": day, "symbol": symbol, "_adj_close": 100.0 + index}
        for index, day in enumerate(dates)
        for symbol in ("QQQ", "SMH")
        if not (index == 2 and symbol == "QQQ")
    ]
    weights = {"QQQ": 0.5, "SMH": 0.5}
    outcome_rows = _compute_outcome_window_rows(
        advisory_event={
            "daily_advisory_id": "daily-test",
            "as_of": "2026-06-08",
            "no_trade_weights": weights,
            "baseline_weights": weights,
            "target_weights": weights,
            "limited_adjustment_weights": weights,
        },
        config={
            "outcome_tracking": {"windows_trading_days": [1, 5, 10, 20]},
            "simulation": {"transaction_cost_bps": 0, "slippage_bps": 0},
        },
        paper_binding={"status": "NOT_RECORDED_BY_AS_OF", "after_weights": {}},
        prices=pd.DataFrame(rows),
        updated_as_of=dates[-1],
        data_gate_ran=True,
    )
    assert outcome_rows[0]["outcome_status"] == "AVAILABLE"
    assert {row["outcome_status"] for row in outcome_rows[1:]} == {
        "INSUFFICIENT_DATA"
    }
    assert all(row["paper_portfolio_return"] is None for row in outcome_rows[1:])
    assert _rollup_outcome_status(outcome_rows) == "PARTIAL"
