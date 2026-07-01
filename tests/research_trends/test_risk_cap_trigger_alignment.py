from __future__ import annotations

from pathlib import Path

from source_bound_static_etf_dry_run_fixtures import (
    TARGET_ASSETS,
    build_source_bound_static_etf_dry_run_fixture,
    load_dry_run_components,
)

from ai_trading_system.source_bound_static_etf_dry_run import (
    build_risk_cap_trigger_alignment_matrix,
)


def test_risk_cap_trigger_alignment_matrix_marks_eligible_records(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    components = load_dry_run_components(fixture)
    alignment_rows = components["alignment_rows"]

    row = next(
        item
        for item in alignment_rows
        if item["date"] == "2023-01-04" and item["target_asset"] == "QQQ"
    )
    assert row["risk_cap_triggered"] is True
    assert row["risk_cap_intensity"] == "medium"
    assert row["market_data_available"] is True
    assert row["baseline_exposure_available"] is True
    assert row["simulation_eligible"] is True


def test_risk_cap_trigger_alignment_matrix_exposes_missing_inputs(
    tmp_path: Path,
) -> None:
    fixture = build_source_bound_static_etf_dry_run_fixture(tmp_path)
    components = load_dry_run_components(fixture)
    price_matrix = components["price_matrix"].drop(columns=["SMH"])
    schedule_rows = [
        row for row in components["schedule_rows"] if row.get("asset") != "SPY"
    ]

    rows = build_risk_cap_trigger_alignment_matrix(
        simulation_dates=components["simulation_dates"],
        target_assets=TARGET_ASSETS,
        schedule_rows=schedule_rows,
        price_matrix=price_matrix,
        date_trigger_map=components["trigger_map"],
        trigger_source_hash=str(components["trigger_frame"].attrs.get("source_hash", "")),
    )

    missing_market = next(
        row for row in rows if row["date"] == "2023-01-04" and row["target_asset"] == "SMH"
    )
    missing_baseline = next(
        row for row in rows if row["date"] == "2023-01-04" and row["target_asset"] == "SPY"
    )
    assert missing_market["simulation_eligible"] is False
    assert "missing_market_data" in missing_market["ineligible_reason"]
    assert missing_baseline["simulation_eligible"] is False
    assert "missing_baseline_exposure" in missing_baseline["ineligible_reason"]
