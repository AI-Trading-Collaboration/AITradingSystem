from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.test_weight_tuning_failure_attribution import (
    write_weight_tuning_failure_artifact,
)


def test_dashboard_reads_weight_tuning_failure_summary_card(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    write_weight_tuning_failure_artifact(tmp_path, as_of=as_of)
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["weight_tuning_failure_summary"]
    assert card["exists"] is True
    assert card["status"] == "NO_CANDIDATE_EXPLAINED"
    assert card["root_cause_category"] == "portfolio_turnover_too_high"
    assert card["top_failure_reason"] == "turnover_guardrail_failed"
    assert card["production_effect"] == "none"
    assert "Weight Tuning Failure Attribution" in html
    assert "production_effect" in html
