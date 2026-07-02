from __future__ import annotations

from ai_trading_system.dynamic_target_baseline_preparation import (
    build_dynamic_target_market_data_alignment_readiness,
    build_dynamic_target_risk_cap_alignment_readiness,
)


def test_alignment_ready_for_matching_dates_assets_and_timestamp_schema() -> None:
    source = {
        "source_id": "ready",
        "source_available": True,
        "history_start": "2023-01-06",
        "history_end": "2023-01-06",
        "target_assets_supported": ["QQQ", "SPY", "SMH"],
        "horizons_supported": ["10d"],
        "field_coverage": {"as_of_timestamp": True, "decision_timestamp": True},
    }
    reference = {
        "risk_cap_trigger_series_available": True,
        "market_data_available": True,
        "risk_cap_trigger_date_coverage": "2023-01-06..2023-01-10",
        "market_data_coverage_start": "2023-01-06",
        "market_data_coverage_end": "2023-01-10",
        "required_assets": ["QQQ", "SPY", "SMH"],
        "risk_cap_assets": ["QQQ", "SPY", "SMH"],
        "market_assets": ["QQQ", "SPY", "SMH"],
        "horizons": ["10d"],
    }

    risk_rows = build_dynamic_target_risk_cap_alignment_readiness(
        inventory_rows=[source],
        reference=reference,
    )
    market_rows = build_dynamic_target_market_data_alignment_readiness(
        inventory_rows=[source],
        reference=reference,
        data_quality_status="PASS",
    )

    assert risk_rows[0]["alignment_readiness_status"] == "ALIGNMENT_READY"
    assert risk_rows[0]["overlap_start"] == "2023-01-06"
    assert risk_rows[0]["asset_overlap"] == ["QQQ", "SMH", "SPY"]
    assert market_rows[0]["alignment_readiness_status"] == "ALIGNMENT_READY"


def test_alignment_blocks_missing_date_coverage() -> None:
    source = {
        "source_id": "missing_date",
        "source_available": True,
        "history_start": "",
        "history_end": "",
        "target_assets_supported": ["QQQ", "SPY", "SMH"],
        "horizons_supported": ["10d"],
        "field_coverage": {"as_of_timestamp": True, "decision_timestamp": True},
    }
    reference = {
        "risk_cap_trigger_series_available": True,
        "risk_cap_trigger_date_coverage": "2023-01-06..2023-01-10",
        "required_assets": ["QQQ", "SPY", "SMH"],
        "horizons": ["10d"],
    }

    rows = build_dynamic_target_risk_cap_alignment_readiness(
        inventory_rows=[source],
        reference=reference,
    )

    assert rows[0]["alignment_readiness_status"] == "ALIGNMENT_BLOCKED_BY_DATE_COVERAGE"
