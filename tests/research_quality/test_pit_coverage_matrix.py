from __future__ import annotations

from ai_trading_system.research_quality.pit_coverage_matrix import (
    blocking_gap_ids,
    build_pit_coverage_matrix,
    build_pit_remediation_matrix,
)
from ai_trading_system.research_quality.pit_input_registry import (
    DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH,
    load_pit_input_registry,
)


def test_dynamic_strategy_pit_registry_loads_and_generates_matrix() -> None:
    registry = load_pit_input_registry(DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH)

    assert registry["validation_status"] == "PASS"
    assert registry["validation_errors"] == []
    assert registry["scope"] == "dynamic_strategy"
    assert registry["entry_count"] == 17

    matrix = build_pit_coverage_matrix(registry)
    rows = matrix["pit_coverage_matrix"]
    by_id = {row["input_id"]: row for row in rows}

    assert matrix["row_count"] == 17
    assert blocking_gap_ids(rows) == ["growth_tilt_engine", "valid_until_window"]
    assert by_id["growth_tilt_engine"]["input_type"] == "SIGNAL"
    assert by_id["growth_tilt_engine"]["severity"] == "BLOCKING"
    assert by_id["growth_tilt_engine"]["candidate_search_blocker"] is True
    assert by_id["valid_until_window"]["input_type"] == "EXECUTION_SEMANTIC"
    assert by_id["valid_until_window"]["severity"] == "BLOCKING"
    assert by_id["threshold_meta_dataset"]["severity"] == "MATERIAL"


def test_dynamic_strategy_pit_remediation_matrix_routes_blockers() -> None:
    registry = load_pit_input_registry(DEFAULT_DYNAMIC_STRATEGY_PIT_INPUT_REGISTRY_PATH)
    matrix = build_pit_coverage_matrix(registry)

    remediation = build_pit_remediation_matrix(matrix["pit_coverage_matrix"])
    rows = remediation["pit_remediation_matrix"]
    by_id = {row["input_id"]: row for row in rows}

    assert remediation["row_count"] == 17
    assert by_id["growth_tilt_engine"]["next_task"] == (
        "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_"
        "Remediation_Plan"
    )
    assert by_id["valid_until_window"]["next_task"] == (
        "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_"
        "Remediation_Plan"
    )
    assert by_id["regime_expectation_score"]["next_task"] == (
        "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan"
    )
    assert by_id["threshold_meta_dataset"]["next_task"] == (
        "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan"
    )
