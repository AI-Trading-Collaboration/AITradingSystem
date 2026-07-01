from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    load_dry_run_components,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    build_downside_protection_proxy_report,
    build_false_risk_cap_cost_report,
    build_missed_upside_cost_report,
)


def test_exposure_cap_false_cost_and_downside_reports_remain_proxy_only(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    rows = load_dry_run_components(fixture)["dry_run_rows"]

    false_cost = build_false_risk_cap_cost_report(rows)
    missed_upside = build_missed_upside_cost_report(rows)
    downside = build_downside_protection_proxy_report(rows)

    assert false_cost["false_risk_cap_count"] > 0
    assert false_cost["false_risk_cap_cost_proxy"] >= 0
    assert false_cost["false_risk_cap_cost_label"] in {
        "FALSE_COST_ACCEPTABLE",
        "FALSE_COST_INCONCLUSIVE",
    }
    assert missed_upside["interpretation_boundary"] == (
        "missed_upside_proxy_not_real_opportunity_cost"
    )
    assert downside["post_trigger_drawdown_capture_count"] > 0
    assert downside["downside_protection_label"] in {
        "DOWNSIDE_PROTECTION_POSITIVE_PROXY",
        "DOWNSIDE_PROTECTION_INCONCLUSIVE",
    }
    assert downside["production_allowed"] is False
