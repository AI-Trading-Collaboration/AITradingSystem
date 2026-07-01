from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    build_source_bound_static_etf_dry_run_fixture,
    load_dry_run_components,
)


def test_source_bound_exposure_cap_dry_run_scales_static_risk_assets(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    components = load_dry_run_components(fixture)
    rows = components["dry_run_rows"]

    first_day = [row for row in rows if row["date"] == "2023-01-04"]
    risk_total = sum(
        row["simulated_final_exposure_after_cap"]
        for row in first_day
        if row["asset"] in {"QQQ", "SPY", "SMH"}
    )
    risk_rows = [row for row in first_day if row["asset"] in {"QQQ", "SPY", "SMH"}]
    assert round(risk_total, 6) == 0.7
    assert all(row["simulated_cap_binding_active"] for row in risk_rows)
    cash = next(row for row in first_day if row["asset"] == "CASH")
    assert cash["simulated_final_exposure_after_cap"] == 0.15
    assert cash["simulated_cap_binding_active"] is False

    cooldown_day = [
        row for row in rows if row["date"] == "2023-01-05" and row["asset"] == "QQQ"
    ][0]
    assert cooldown_day["risk_cap_triggered"] is False
    assert cooldown_day["simulated_cooldown_state"] == "active"
    assert cooldown_day["simulated_cap_binding_active"] is True
    assert cooldown_day["target_weight_generated"] is False
    assert cooldown_day["broker_order_generated"] is False
