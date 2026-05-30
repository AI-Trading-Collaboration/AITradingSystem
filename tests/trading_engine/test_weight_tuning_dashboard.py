from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.parameters.weight_tuning import (
    write_weight_tuning_summary,
)
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.weight_tuning_helpers import sample_weight_tuning_payload


def test_dashboard_reads_weight_tuning_summary_card(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    artifact_dir = tmp_path / "artifacts" / "weight_tuning" / as_of.isoformat()
    write_weight_tuning_summary(
        sample_weight_tuning_payload(as_of=as_of),
        artifact_dir / "weight_tuning_summary.json",
        artifact_dir / "weight_tuning_summary.md",
    )
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["weight_tuning_summary"]
    assert card["exists"] is True
    assert card["status"] == "NO_CANDIDATE"
    assert card["portfolio_profile"] == "lower_rebalance_threshold_2pct"
    assert card["candidates_evaluated"] == 240
    assert card["recommended_status"] == "rejected"
    assert card["production_effect"] == "none"
    assert card["guardrail_status"] == "FAIL"
    assert "Weight Tuning Summary" in html
    assert "production_effect" in html
    assert "shadow-only artifact" in html
