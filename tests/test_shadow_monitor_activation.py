from __future__ import annotations

from datetime import date
from pathlib import Path

from dynamic_v3_position_readiness_helpers import shadow_shortlist_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    activate_shadow_monitoring,
    run_shadow_shortlist_monitor,
    validate_shadow_monitor_run_artifact,
)


def test_shadow_monitor_activation_and_run(tmp_path: Path) -> None:
    fixture = shadow_shortlist_fixture(tmp_path)
    shadow_id = fixture["shadow"]["shadow_shortlist_id"]

    activation = activate_shadow_monitoring(
        shadow_shortlist_id=shadow_id,
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )
    assert activation["manifest"]["monitoring_status"] == "active"
    assert activation["manifest"]["broker_action_allowed"] is False

    result = run_shadow_shortlist_monitor(
        shadow_shortlist_id=shadow_id,
        as_of=date(2026, 6, 7),
        shadow_shortlist_dir=tmp_path / "shadow_shortlist",
        output_dir=tmp_path / "shadow_monitor_runs",
    )

    assert (
        result["summary"]["active_count"]
        == fixture["shadow"]["manifest"]["shadow_candidate_count"]
    )
    assert result["summary"]["summary_recommendation"] == "continue_monitoring"
    assert result["manifest"]["broker_action_allowed"] is False
    assert (
        validate_shadow_monitor_run_artifact(
            monitor_run_id=result["monitor_run_id"],
            output_dir=tmp_path / "shadow_monitor_runs",
        )["status"]
        == "PASS"
    )
