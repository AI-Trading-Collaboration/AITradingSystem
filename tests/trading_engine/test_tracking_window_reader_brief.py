from __future__ import annotations

from pathlib import Path

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    run_portfolio_tracking_review,
)
from trading_engine.test_portfolio_tracking_review import _tracking_review_fixture
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_reader_brief_explains_days_until_short_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )
    snapshot_path = _write_decision_snapshot(tmp_path, fixture["as_of"])

    payload = build_reader_brief_payload(
        as_of=fixture["as_of"],
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    summary = review["portfolio_tracking_review_summary"].lower()
    assert review["portfolio_tracking_review_tracking_days"] == 1
    assert review["portfolio_tracking_review_stage"] == "initial_observation"
    assert review["portfolio_tracking_review_days_until_short_review"] == 4
    assert "only 1 tracking day is available" in summary
    assert "at least 5 valid tracking days are required" in summary
