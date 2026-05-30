from __future__ import annotations

from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    run_portfolio_tracking_review,
)
from trading_engine.test_portfolio_tracking_review import _tracking_review_fixture
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata


def test_dashboard_shows_tracking_window_progress(tmp_path: Path) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )
    metadata_path = _write_dashboard_metadata(tmp_path, fixture["as_of"])

    report = build_daily_task_dashboard_report(
        as_of=fixture["as_of"],
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["portfolio_tracking_review_summary"]
    assert card["tracking_days"] == 1
    assert card["review_stage"] == "initial_observation"
    assert card["days_until_short_review"] == 4
    assert card["days_until_extended_review"] == 19
    assert card["done_condition_met"] is False
    assert "initial_observation" in html
    assert "days until short review" in html
