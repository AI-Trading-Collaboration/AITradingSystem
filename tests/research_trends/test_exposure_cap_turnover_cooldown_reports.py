from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    load_dry_run_components,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    build_cooldown_impact_report,
    build_turnover_impact_report,
)


def test_exposure_cap_turnover_and_cooldown_reports_use_proxy_metrics(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    rows = load_dry_run_components(fixture)["dry_run_rows"]

    turnover = build_turnover_impact_report(rows)
    cooldown = build_cooldown_impact_report(rows)

    assert turnover["turnover_proxy_total"] > 0
    assert turnover["turnover_proxy_from_cap_entry"] > 0
    assert turnover["turnover_impact_label"] in {
        "LOW_TURNOVER_IMPACT",
        "TURNOVER_IMPACT_INCONCLUSIVE",
    }
    assert cooldown["cooldown_trigger_count"] == 3
    assert cooldown["cooldown_active_days"] > 0
    assert cooldown["cooldown_prevented_reentry_days"] > 0
    assert cooldown["cooldown_impact_label"] in {
        "COOLDOWN_HELPFUL_PROXY",
        "COOLDOWN_COSTLY_PROXY",
        "COOLDOWN_NEUTRAL_PROXY",
    }
    assert cooldown["broker_action"] == "none"
