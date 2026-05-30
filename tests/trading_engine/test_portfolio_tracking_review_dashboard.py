from __future__ import annotations

from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    run_portfolio_tracking_review,
)
from trading_engine.test_portfolio_tracking_review import _tracking_review_fixture
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_portfolio_tracking_review_summary(tmp_path: Path) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    review_run = run_portfolio_tracking_review(
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
    assert card["exists"] is True
    assert card["recommendation"] == "needs_more_data"
    assert card["candidate_profile"] == review_run.payload["candidate"]["profile_name"]
    assert card["production_effect"] == "none"
    assert "Portfolio Tracking Review" in html
    assert "Tracking Review Markdown" in html


def test_reader_brief_displays_portfolio_tracking_review_summary(
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
    assert review["portfolio_tracking_review_recommendation"] == "needs_more_data"
    assert "needs-more-data" in review["portfolio_tracking_review_summary"]
    assert review["portfolio_tracking_review_tracking_days"] == 1
