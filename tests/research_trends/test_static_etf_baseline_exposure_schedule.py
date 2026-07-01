from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    load_dry_run_components,
)


def test_static_etf_baseline_exposure_schedule_is_source_bound(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    components = load_dry_run_components(fixture)
    schedule_rows = components["schedule_rows"]

    assert len(schedule_rows) == len(components["simulation_dates"]) * 4
    by_date: dict[str, float] = defaultdict(float)
    for row in schedule_rows:
        by_date[row["date"]] += row["baseline_weight_for_dry_run"]
        assert row["target_weight_generated"] is False
        assert row["rebalance_instruction_generated"] is False
        assert row["broker_order_generated"] is False
        assert row["baseline_source"] == str(fixture["portfolio_config_dir"] / "assets.yaml")
        assert row["baseline_source_hash"]

    assert all(round(total, 6) == 1.0 for total in by_date.values())
    cash = next(row for row in schedule_rows if row["asset"] == "CASH")
    assert cash["cash_like_flag"] is True
    assert cash["defensive_asset_flag"] is True
    assert cash["risk_asset_flag"] is False
    first_date = min(by_date)
    assert any(row["date"] == first_date and row["rebalance_flag"] for row in schedule_rows)
