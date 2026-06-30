from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from regenerated_candidate_test_helpers import (
    build_scope_narrowed_candidate_actual_path_validation_fixture,
    write_price_fixture,
)

from ai_trading_system.regenerated_candidate_actual_path_validation import (
    calculate_actual_path,
    load_actual_price_matrix,
)
from ai_trading_system.scope_narrowed_candidate_actual_path_validation import (
    CONFIRMATION_CANDIDATE_ID,
    RISK_APPETITE_ARCHIVE_CANDIDATE,
    run_scope_narrowed_candidate_actual_path_validation,
)


def _record(*, decision: str = "2023-01-03T21:00:00+00:00", horizon: str = "5d"):
    return {
        "scope_narrowed_candidate_id": CONFIRMATION_CANDIDATE_ID,
        "candidate_id": CONFIRMATION_CANDIDATE_ID,
        "refined_candidate_id": "baseline_plus_trend_structure_refined_confidence_v1",
        "original_candidate_id": "baseline_plus_trend_structure",
        "usage_role": "confirmation_only",
        "target_asset": "QQQ",
        "decision_timestamp": decision,
        "horizon": horizon,
        "signal_direction": "trend_confirming",
        "scope_active": True,
    }


def test_runner_validates_only_active_scope_records(tmp_path: Path) -> None:
    fixture = build_scope_narrowed_candidate_actual_path_validation_fixture(tmp_path)
    output_dir = tmp_path / "out"

    run_scope_narrowed_candidate_actual_path_validation(
        scope_narrowed_generator_dir=fixture["scope_narrowed_generator_dir"],
        scope_review_dir=fixture["scope_review_dir"],
        refined_validation_dir=fixture["refined_validation_dir"],
        include_candidates=(
            "baseline_plus_trend_structure_scope_narrowed_confirmation_v1,"
            "volatility_regime_scope_narrowed_risk_cap_v1"
        ),
        archived_candidates=RISK_APPETITE_ARCHIVE_CANDIDATE,
        target_assets="QQQ,SPY,SMH",
        horizons="5d,10d,20d",
        output_dir=output_dir,
        mode="scope_narrowed_actual_path_validation",
        prices_path=fixture["prices_path"],
        rates_path=fixture["rates_path"],
        marketstack_prices_path=None,
        docs_root=tmp_path / "docs",
    )

    active = json.loads(
        (output_dir / "scope_narrowed_active_actual_path_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]
    inactive_reference = json.loads(
        (output_dir / "scope_narrowed_inactive_reference_matrix.json").read_text(
            encoding="utf-8"
        )
    )["rows"]

    assert active
    assert all(row["scope_active"] is True for row in active)
    assert inactive_reference
    assert all(row["reference_only"] is True for row in inactive_reference)


def test_calculator_computes_forward_return_drawdown_and_volatility(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")
    actual = calculate_actual_path(_record(), matrix)
    position = matrix.index.get_loc(pd.Timestamp("2023-01-03"))
    expected = matrix["QQQ"].iloc[position + 5] / matrix["QQQ"].iloc[position] - 1.0

    assert actual["validation_eligible"] is True
    assert actual["forward_return"] == round(expected, 6)
    assert actual["max_drawdown_during_horizon"] <= actual["max_runup_during_horizon"]
    assert actual["realized_volatility"] >= 0.0


def test_calculator_marks_incomplete_future_window_ineligible(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")

    actual = calculate_actual_path(_record(decision="2023-02-14T21:00:00+00:00"), matrix)

    assert actual["actual_path_status"] == "incomplete_future_window"
    assert actual["validation_eligible"] is False


def test_calculator_marks_missing_decision_price_ineligible(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")

    actual = calculate_actual_path(_record(decision="2023-01-07T21:00:00+00:00"), matrix)

    assert actual["actual_path_status"] == "missing_decision_price"
    assert actual["validation_eligible"] is False


def test_calculator_partial_coverage_warns(tmp_path: Path) -> None:
    price_path = write_price_fixture(tmp_path)
    matrix = load_actual_price_matrix(price_path, "QQQ")
    matrix.loc[pd.Timestamp("2023-01-05"), "QQQ"] = pd.NA

    actual = calculate_actual_path(_record(), matrix)

    assert actual["actual_path_status"] == "partial_price_coverage"
    assert actual["data_quality_warning"] is True
