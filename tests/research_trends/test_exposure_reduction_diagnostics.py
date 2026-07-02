from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_exposure_reduction_diagnostics,
)


def test_exposure_reduction_diagnostics_summarizes_assets_and_periods() -> None:
    dry_run_rows = [
        _dry_row("2023-01-03", "QQQ", 0.00, False),
        _dry_row("2023-01-03", "SPY", 0.03, True),
        _dry_row("2023-01-04", "QQQ", 0.06, True),
        _dry_row("2023-02-01", "SMH", 0.09, False),
    ]
    binding_rows = [
        {"date": "2023-01-03", "cap_binding_active_any_asset": True},
        {"date": "2023-01-04", "cap_binding_active_any_asset": True},
        {"date": "2023-02-01", "cap_binding_active_any_asset": False},
    ]

    diagnostics = build_exposure_reduction_diagnostics(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        data_quality_status="PASS",
    )

    assert diagnostics["average_exposure_reduction"] == 0.045
    assert diagnostics["max_exposure_reduction"] == 0.09
    assert diagnostics["total_exposure_reduction"] == 0.18
    assert diagnostics["exposure_reduction_by_asset"] == {
        "QQQ": 0.06,
        "SMH": 0.09,
        "SPY": 0.03,
    }
    assert diagnostics["exposure_reduction_by_period"] == {
        "2023-01": 0.09,
        "2023-02": 0.09,
    }
    assert diagnostics["exposure_reduction_label"] == "EXPOSURE_REDUCTION_MODEST"


def _dry_row(
    current: str,
    asset: str,
    reduction: float,
    triggered: bool,
) -> dict[str, object]:
    return {
        "date": current,
        "asset": asset,
        "simulated_exposure_delta": reduction,
        "risk_cap_triggered": triggered,
    }
