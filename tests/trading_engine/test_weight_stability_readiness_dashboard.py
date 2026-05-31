from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.parameters.weight_stability_readiness import (
    write_weight_stability_readiness_summary,
)
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.weight_stability_readiness_helpers import (
    sample_weight_stability_readiness_payload,
)


def test_dashboard_reads_weight_stability_readiness_card(tmp_path: Path) -> None:
    as_of = date(2026, 5, 29)
    artifact_dir = tmp_path / "artifacts" / "weight_stability_readiness" / as_of.isoformat()
    write_weight_stability_readiness_summary(
        sample_weight_stability_readiness_payload(as_of=as_of),
        artifact_dir / "weight_stability_readiness_summary.json",
        artifact_dir / "weight_stability_readiness_summary.md",
    )
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["weight_stability_readiness_summary"]
    assert card["exists"] is True
    assert card["status"] == "RECOVERY_FAILED"
    assert card["can_run"] is False
    assert card["freshness_status"] == "MISSING"
    assert card["price_coverage_status"] == "FAILED"
    assert "price_coverage" in card["blocking_checks"]
    assert "Weight Stability Readiness" in html
    assert "Stable Weight Tuning Readiness" in html
