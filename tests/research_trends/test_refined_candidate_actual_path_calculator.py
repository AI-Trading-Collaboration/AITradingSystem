from __future__ import annotations

from pathlib import Path

import pandas as pd
from regenerated_candidate_test_helpers import write_price_fixture

from ai_trading_system.refined_candidate_actual_path_validation import (
    calculate_actual_path,
    load_actual_price_matrix,
)


def _record(*, decision: str = "2023-01-03T21:00:00+00:00", horizon: str = "5d"):
    return {
        "candidate_id": "risk_appetite_refined_confidence_v1",
        "refined_candidate_id": "risk_appetite_refined_confidence_v1",
        "original_candidate_id": "risk_appetite",
        "target_asset": "QQQ",
        "decision_timestamp": decision,
        "horizon": horizon,
        "signal_direction": "risk_on",
        "signal_value": 0.5,
        "signal_confidence": 0.8,
        "refined_signal_value": 0.5,
        "refined_signal_confidence": 0.8,
        "high_conviction_flag": True,
    }


def test_refined_calculator_computes_forward_path_metrics(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ,SPY,SMH")

    actual = calculate_actual_path(_record(), matrix)

    assert actual["validation_eligible"] is True
    assert actual["forward_return"] is not None
    assert actual["max_drawdown_during_horizon"] is not None
    assert actual["max_runup_during_horizon"] is not None
    assert actual["realized_volatility"] >= 0.0


def test_refined_calculator_marks_missing_decision_price_ineligible(
    tmp_path: Path,
) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")

    actual = calculate_actual_path(_record(decision="2023-01-07T21:00:00+00:00"), matrix)

    assert actual["actual_path_status"] == "missing_decision_price"
    assert actual["validation_eligible"] is False


def test_refined_calculator_marks_incomplete_future_window_ineligible(
    tmp_path: Path,
) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")

    actual = calculate_actual_path(_record(decision="2023-02-14T21:00:00+00:00"), matrix)

    assert actual["actual_path_status"] == "incomplete_future_window"
    assert actual["validation_eligible"] is False


def test_refined_calculator_marks_partial_coverage_warning(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")
    matrix.loc[pd.Timestamp("2023-01-05"), "QQQ"] = pd.NA

    actual = calculate_actual_path(_record(), matrix)

    assert actual["actual_path_status"] == "partial_price_coverage"
    assert actual["data_quality_warning"] is True
    assert actual["validation_eligible"] is True


def test_refined_calculator_below_coverage_threshold_ineligible(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")
    for day in ("2023-01-04", "2023-01-05", "2023-01-06"):
        matrix.loc[pd.Timestamp(day), "QQQ"] = pd.NA

    actual = calculate_actual_path(_record(), matrix)

    assert actual["actual_path_status"] == "partial_price_coverage"
    assert actual["validation_eligible"] is False
    assert actual["data_coverage_ratio"] < 0.8
