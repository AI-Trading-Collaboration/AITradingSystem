from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from ai_trading_system.trading_engine.market_data_freshness import run_market_data_freshness
from trading_engine.test_market_data_freshness import _freshness_fixture
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_market_data_freshness_summary(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [as_of], "NVDA": [as_of]},
        manifest_date=as_of,
    )
    run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["market_data_freshness_summary"]
    assert card["exists"] is True
    assert card["freshness_status"] == "OK"
    assert card["tracking_date"] == as_of.isoformat()
    assert card["production_effect"] == "none"
    assert "Market Data Freshness" in html
    assert "Freshness Markdown" in html


def test_reader_brief_displays_market_data_freshness_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    as_of = date(2026, 1, 6)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [as_of], "NVDA": [as_of]},
        manifest_date=as_of,
    )
    run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path / "outputs" / "reports",
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["market_data_freshness_status"] == "OK"
    assert "Market data freshness is OK" in review["market_data_freshness_summary"]
