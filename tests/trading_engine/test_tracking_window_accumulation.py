from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from ai_trading_system.shadow.lineage import sha256_file
from ai_trading_system.trading_engine.parameters import shadow_backtest
from ai_trading_system.trading_engine.portfolio_tracking_review import (
    load_portfolio_tracking_review_windows_config,
    run_portfolio_tracking_review,
    validate_portfolio_tracking_review_payload,
)
from trading_engine.test_portfolio_tracking_review import (
    _clone_tracking_days,
    _tracking_review_fixture,
)


@pytest.mark.parametrize(
    (
        "tracking_days",
        "expected_stage",
        "days_until_short",
        "days_until_extended",
        "can_short",
        "can_extended",
    ),
    [
        (1, "initial_observation", 4, 19, False, False),
        (4, "initial_observation", 1, 16, False, False),
        (5, "short_window_review", 0, 15, True, False),
        (19, "short_window_review", 0, 1, True, False),
        (20, "extended_review_ready", 0, 0, True, True),
    ],
)
def test_tracking_window_stage_boundaries(
    tmp_path: Path,
    tracking_days: int,
    expected_stage: str,
    days_until_short: int,
    days_until_extended: int,
    can_short: bool,
    can_extended: bool,
) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    if tracking_days > 1:
        _clone_tracking_days(
            tracking_run.payload,
            tracking_run.json_path.parent.parent,
            count=tracking_days,
            candidate_return_step=0.002,
            baseline_return_step=0.001,
            candidate_rebalance=7,
            baseline_rebalance=3,
            candidate_signal_score=0.9,
            baseline_signal_score=0.7,
        )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"] + timedelta(days=tracking_days - 1),
        config_path=tracking_review_config,
        window="since-start",
    )

    assert validate_portfolio_tracking_review_payload(run.payload) == []
    window = run.payload["tracking_window"]
    assert window["tracking_days"] == tracking_days
    assert window["stage"] == expected_stage
    assert window["days_until_short_review"] == days_until_short
    assert window["days_until_extended_review"] == days_until_extended
    assert window["can_form_short_window_conclusion"] is can_short
    assert window["can_form_extended_review_conclusion"] is can_extended
    assert run.payload["safety"]["forbid_backfilled_tracking_days"] is True
    assert run.payload["safety"]["forbid_synthetic_tracking_days"] is True
    if tracking_days < 5:
        assert run.payload["recommendation"]["status"] == "needs_more_data"
        assert window["done_condition_met"] is False


def test_tracking_window_forces_needs_more_data_before_short_window(tmp_path: Path) -> None:
    fixture, tracking_run, tracking_review_config = _tracking_review_fixture(tmp_path)
    _clone_tracking_days(
        tracking_run.payload,
        tracking_run.json_path.parent.parent,
        count=4,
        candidate_return_step=0.01,
        baseline_return_step=0.0,
        candidate_rebalance=10,
        baseline_rebalance=1,
        candidate_signal_score=0.95,
        baseline_signal_score=0.4,
    )

    run = run_portfolio_tracking_review(
        as_of=fixture["as_of"] + timedelta(days=3),
        config_path=tracking_review_config,
    )

    assert run.payload["tracking_window"]["stage"] == "initial_observation"
    assert run.payload["recommendation"]["status"] == "needs_more_data"
    assert "At least 5 valid tracking days" in run.payload["recommendation"]["reason"]


def test_tracking_window_policy_config_loads() -> None:
    config = load_portfolio_tracking_review_windows_config()

    assert config["tracking_windows"]["min_days_for_short_review"] == 5
    assert config["tracking_windows"]["min_days_for_extended_review"] == 20
    assert config["safety"]["production_effect"] == "none"
    assert config["safety"]["forbid_synthetic_tracking_days"] is True


def test_shadow_backtest_explains_minimum_tracking_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture, _, tracking_review_config = _tracking_review_fixture(tmp_path)
    baseline_before = sha256_file(fixture["baseline_path"])
    review_run = run_portfolio_tracking_review(
        as_of=fixture["as_of"],
        config_path=tracking_review_config,
    )
    monkeypatch.setattr(shadow_backtest, "PROJECT_ROOT", tmp_path)

    shadow_run = shadow_backtest.run_shadow_parameter_backtest(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        dry_run=True,
    )

    decision = shadow_run.payload["promotion_decision"]
    assert decision["status"] == "rejected"
    assert decision["portfolio_tracking_review_recommendation"] == "needs_more_data"
    assert decision["portfolio_tracking_review_tracking_days"] == 1
    assert decision["portfolio_tracking_review_stage"] == "initial_observation"
    assert "only 1 tracking day is available" in decision["reason"].lower()
    assert "at least 5 tracking days are required" in decision["reason"].lower()
    assert decision["supporting_artifacts"]["portfolio_tracking_review"] == str(
        review_run.json_path
    )
    assert sha256_file(fixture["baseline_path"]) == baseline_before
