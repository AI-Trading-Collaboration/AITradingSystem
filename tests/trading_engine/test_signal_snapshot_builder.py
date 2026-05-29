from __future__ import annotations

import json
from pathlib import Path

from ai_trading_system.trading_engine.signal_snapshots import (
    REQUIRED_SIGNALS,
    run_signal_snapshot_build,
    signal_snapshot_summary,
    validate_signal_snapshot_payload,
)
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_build_signal_snapshot_from_price_data_and_neutral_fallbacks(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=240, min_history_days=20)

    run = run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
    )

    assert run.json_path.exists()
    assert run.markdown_path.exists()
    payload = json.loads(run.json_path.read_text(encoding="utf-8"))
    assert payload["metadata"]["production_effect"] == "none"
    assert payload["metadata"]["manual_review_required"] is True
    assert payload["metadata"]["auto_promotion"] is False
    assert set(payload["signals"]) == set(REQUIRED_SIGNALS)
    assert validate_signal_snapshot_payload(payload) == []

    trend = payload["signals"]["trend_momentum"]
    sector = payload["signals"]["sector_strength"]
    earnings = payload["signals"]["earnings_quality"]
    event = payload["signals"]["event_risk"]
    assert trend["status"] == "OK"
    assert trend["quality"] == "price_derived"
    assert trend["values"]
    assert sector["status"] == "OK"
    assert sector["quality"] == "price_derived"
    assert sector["values"]
    assert earnings["status"] == "NEUTRAL_FALLBACK"
    assert earnings["values"][0]["value"] == 0.5
    assert event["status"] == "NEUTRAL_FALLBACK"
    assert event["values"][0]["reason"] == "No PIT event dataset available in v0.1."

    summary = signal_snapshot_summary(payload)
    assert summary["status"] == "LIMITED"
    assert summary["real_signal_count"] == 2
    assert summary["fallback_signal_count"] >= 3
    assert summary["missing_signal_count"] == 0
    assert summary["can_run_full_signal_backtest"] is True
    assert summary["can_promote_candidate"] is False


def test_price_derived_only_snapshot_marks_missing_fallback_signals(tmp_path: Path) -> None:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=80, min_history_days=20)

    run = run_signal_snapshot_build(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"] / "signal_snapshots",
        price_derived_only=True,
    )

    summary = signal_snapshot_summary(run.payload)
    assert summary["real_signal_count"] == 2
    assert set(summary["missing_signals"]) == {
        "macro_liquidity",
        "earnings_quality",
        "valuation_risk",
        "event_risk",
    }
    assert validate_signal_snapshot_payload(run.payload)
