from __future__ import annotations

from ai_trading_system.exposure_cap_diagnostics_review import (
    build_cap_binding_diagnostics,
)


def test_cap_binding_diagnostics_computes_frequency_and_clusters() -> None:
    binding_rows = [
        _binding_row("2023-01-03", True, ["QQQ"], "medium", 2.0),
        _binding_row("2023-01-04", True, ["QQQ", "SPY"], "high", 2.5),
        _binding_row("2023-01-05", False, [], "none", 0.0),
        _binding_row("2023-01-06", True, ["SMH"], "low", 1.0),
        _binding_row("2023-01-09", False, [], "none", 0.0),
    ]
    dry_run_rows = [
        {"asset": "QQQ", "simulated_cap_binding_active": True, "horizon": "10d"},
        {"asset": "SPY", "simulated_cap_binding_active": True, "horizon": "10d"},
        {"asset": "SMH", "simulated_cap_binding_active": True, "horizon": "20d"},
    ]
    comparison = {
        "record_count": 15,
        "simulation_start": "2023-01-03",
        "simulation_end": "2023-01-09",
        "cap_binding_days": 3,
        "cap_binding_rate": 0.6,
    }

    diagnostics = build_cap_binding_diagnostics(
        dry_run_rows=dry_run_rows,
        binding_rows=binding_rows,
        comparison=comparison,
        data_quality_status="PASS",
    )

    assert diagnostics["cap_binding_days"] == 3
    assert diagnostics["cap_binding_rate"] == 0.6
    assert diagnostics["cap_binding_cluster_count"] == 2
    assert diagnostics["average_cap_binding_cluster_length"] == 1.5
    assert diagnostics["max_cap_binding_cluster_length"] == 2
    assert diagnostics["cap_binding_frequency_label"] == "EXCESSIVE_BINDING_FREQUENCY"
    assert diagnostics["cap_binding_asset_distribution"] == {
        "QQQ": 1,
        "SMH": 1,
        "SPY": 1,
    }


def _binding_row(
    current: str,
    active: bool,
    assets: list[str],
    intensity: str,
    average: float,
) -> dict[str, object]:
    return {
        "date": current,
        "cap_binding_active_any_asset": active,
        "cap_binding_assets": assets,
        "risk_cap_intensity_max": intensity,
        "risk_cap_intensity_average": average,
    }
