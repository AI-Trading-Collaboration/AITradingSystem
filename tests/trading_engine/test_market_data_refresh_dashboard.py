from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import build_reader_brief_payload
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.test_signal_snapshot_dashboard import _write_decision_snapshot


def test_dashboard_reads_market_data_refresh_summary(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    _write_refresh_summary(tmp_path, as_of, status="OK")
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["market_data_refresh_summary"]
    assert card["exists"] is True
    assert card["status"] == "OK"
    assert card["target_date"] == as_of.isoformat()
    assert card["before_freshness"] == "STALE"
    assert card["after_freshness"] == "OK"
    assert card["candidate_tracking_recovered"] == "active_tracking"
    assert "Market Data Refresh" in html
    assert "Refresh Markdown" in html


def test_reader_brief_displays_market_data_refresh_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    as_of = date(2026, 1, 6)
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    _write_refresh_summary(tmp_path, as_of, status="SOURCE_DELAYED")
    snapshot_path = _write_decision_snapshot(tmp_path, as_of)

    payload = build_reader_brief_payload(
        as_of=as_of,
        reports_dir=tmp_path / "outputs" / "reports",
        decision_snapshot_path=snapshot_path,
    )

    review = payload["parameter_shadow_review"]
    assert review["market_data_refresh_status"] == "SOURCE_DELAYED"
    assert "data source has not provided" in review["market_data_refresh_summary"]


def _write_refresh_summary(tmp_path: Path, as_of: date, *, status: str) -> Path:
    path = (
        tmp_path
        / "artifacts"
        / "data_refresh"
        / as_of.isoformat()
        / "market_data_refresh_summary.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path = path.with_suffix(".md")
    markdown_path.write_text("# Market Data Refresh Summary\n", encoding="utf-8")
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "market_data_refresh",
                "metadata": {
                    "run_id": f"market-data-refresh-{as_of.isoformat()}",
                    "generated_at": "2026-01-07T00:00:00+00:00",
                    "status": status,
                    "reason": "test",
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                },
                "before": {
                    "freshness_status": "STALE",
                    "tracking_date": as_of.isoformat(),
                    "effective_data_date": "2026-01-05",
                    "tracking_readiness": "cannot_track",
                },
                "actions": {
                    "fetched_assets": ["GOOGL", "BRK.B"],
                    "target_date": as_of.isoformat(),
                    "source": "fmp",
                    "updated_price_cache_registry": status == "OK",
                    "refreshed_backtest_manifest": status == "OK",
                },
                "after": {
                    "freshness_status": "OK" if status == "OK" else "STALE",
                    "effective_data_date": as_of.isoformat() if status == "OK" else "2026-01-05",
                    "tracking_readiness": "can_track" if status == "OK" else "cannot_track",
                    "candidate_tracking_status": (
                        "active_tracking" if status == "OK" else "tracking_blocked"
                    ),
                },
                "safety": {
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                    "production_write_allowed": False,
                    "fake_price_rows_generated": False,
                    "synthetic_latest_bar_generated": False,
                    "data_quality_gate_lowered": False,
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
