from __future__ import annotations

from datetime import date
from pathlib import Path

from ai_trading_system.daily_task_dashboard import (
    build_daily_task_dashboard_payload,
    build_daily_task_dashboard_report,
    render_daily_task_dashboard,
)
from ai_trading_system.trading_engine.parameters.weight_stability import (
    write_weight_stability_summary,
)
from trading_engine.test_shadow_parameter_backtest import _write_dashboard_metadata
from trading_engine.weight_stability_helpers import sample_weight_stability_payload


def test_dashboard_reads_weight_stability_summary_card(tmp_path: Path) -> None:
    as_of = date(2026, 5, 28)
    artifact_dir = tmp_path / "artifacts" / "weight_stability" / as_of.isoformat()
    write_weight_stability_summary(
        sample_weight_stability_payload(as_of=as_of),
        artifact_dir / "weight_stability_summary.json",
        artifact_dir / "weight_stability_summary.md",
    )
    metadata_path = _write_dashboard_metadata(tmp_path, as_of)

    report = build_daily_task_dashboard_report(
        as_of=as_of,
        metadata_path=metadata_path,
        reports_dir=tmp_path / "outputs" / "reports",
    )
    payload = build_daily_task_dashboard_payload(report)
    html = render_daily_task_dashboard(report)

    card = payload["weight_stability_summary"]
    assert card["exists"] is True
    assert card["status"] == "LIMITED"
    assert card["previous_root_cause"] == "weight_search_too_aggressive"
    assert card["candidates_generated"] == 120
    assert card["rejected_by_stability"] == 60
    assert card["rejected_by_turnover_prefilter"] == 20
    assert card["recommended_status"] == "no_candidate"
    assert card["production_effect"] == "none"
    assert "Weight Search Stability" in html
    assert "turnover guardrail" in html
