from __future__ import annotations

from ai_trading_system.dynamic_exposure_cap_diagnostics_review import (
    build_dynamic_strategy_overlap_diagnostics,
)


def test_strategy_overlap_mostly_redundant_label_is_computed() -> None:
    payload = build_dynamic_strategy_overlap_diagnostics(
        overlap_report={
            "record_count": 20,
            "risk_cap_trigger_count": 10,
            "dynamic_strategy_derisked_count": 9,
            "risk_cap_and_dynamic_derisk_overlap_count": 8,
            "risk_cap_incremental_binding_count": 1,
            "risk_cap_redundant_binding_count": 8,
            "risk_cap_binding_without_dynamic_derisk_count": 1,
            "dynamic_derisk_without_risk_cap_count": 1,
        },
        data_quality_status="PASS",
    )

    assert payload["overlap_rate"] == 0.8
    assert payload["incremental_binding_rate"] == 0.1
    assert payload["redundant_binding_rate"] == 0.8
    assert payload["overlap_label"] == "RISK_CAP_MOSTLY_REDUNDANT_WITH_DYNAMIC_STRATEGY"


def test_strategy_overlap_incremental_label_is_computed() -> None:
    payload = build_dynamic_strategy_overlap_diagnostics(
        overlap_report={
            "record_count": 20,
            "risk_cap_trigger_count": 10,
            "dynamic_strategy_derisked_count": 2,
            "risk_cap_and_dynamic_derisk_overlap_count": 2,
            "risk_cap_incremental_binding_count": 7,
            "risk_cap_redundant_binding_count": 2,
            "risk_cap_binding_without_dynamic_derisk_count": 0,
            "dynamic_derisk_without_risk_cap_count": 0,
        },
        data_quality_status="PASS",
    )

    assert payload["overlap_label"] == "RISK_CAP_INCREMENTAL_TO_DYNAMIC_STRATEGY"
