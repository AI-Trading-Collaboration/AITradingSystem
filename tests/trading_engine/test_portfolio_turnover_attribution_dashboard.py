from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from trading_engine.test_portfolio_turnover_attribution import (
    write_portfolio_turnover_attribution_artifact,
)
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata


def test_dashboard_reads_portfolio_turnover_attribution_summary_card(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 5, 28)
    write_portfolio_turnover_attribution_artifact(tmp_path, as_of=as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["portfolio_turnover_attribution_summary"]
    assert card["exists"] is True
    assert card["status"] == "TURNOVER_FAILURE_EXPLAINED"
    assert card["root_cause_category"] in {
        "rebalance_threshold_too_low",
        "weight_search_too_aggressive",
    }
    assert card["failed_candidate_count"] == 1
    assert card["production_effect"] == "none"
    assert "Portfolio Turnover Attribution" in html
    assert "production_effect" in html
