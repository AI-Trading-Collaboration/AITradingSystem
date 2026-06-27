from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from test_execution_semantics import _write_execution_caches
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    ACTUAL_PATH_OWNER_REVIEW_BASELINES,
    ACTUAL_PATH_OWNER_REVIEW_CANDIDATES,
    _actual_position_path,
    _attach_path_return_columns,
    run_execution_semantics_rebacktest,
)


def test_staleness_filter_does_not_use_future_returns() -> None:
    dates = pd.bdate_range("2026-01-02", periods=8)
    target = _stale_signal_target(dates)
    policy = _validity_policy()
    profile = _profile(stale_action="suppress_rebalance")

    actual, rows = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=target,
        policy=policy,
        signal_validity_profile=profile,
        enable_staleness_filter=True,
    )
    prices_a = _prices_with_future_return(dates, future_return=0.50)
    prices_b = _prices_with_future_return(dates, future_return=-0.50)

    _attach_path_return_columns(
        prices=prices_a,
        target_weights=target,
        actual_weights=actual,
        path_rows=rows,
        cost_bps=0.0,
    )
    decision_rows_a = [
        (row["date"], row["rebalance_executed"], row["trigger_reason"])
        for row in rows
    ]

    actual_b, rows_b = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=target,
        policy=policy,
        signal_validity_profile=profile,
        enable_staleness_filter=True,
    )
    _attach_path_return_columns(
        prices=prices_b,
        target_weights=target,
        actual_weights=actual_b,
        path_rows=rows_b,
        cost_bps=0.0,
    )

    assert decision_rows_a == [
        (row["date"], row["rebalance_executed"], row["trigger_reason"])
        for row in rows_b
    ]
    assert rows[5]["staleness_filter_suppressed"] is True
    assert rows[5]["trigger_reason"] == "stale_signal_suppress_rebalance"


def test_staleness_filter_does_not_peek_future_signal_persistence() -> None:
    dates = pd.bdate_range("2026-01-02", periods=9)
    persistent_target = _stale_signal_target(dates)
    reversing_target = persistent_target.copy()
    reversing_target.iloc[7] = {"QQQ": 1.0, "TQQQ": 0.0, "SGOV": 0.0}

    policy = _validity_policy()
    profile = _profile(stale_action="suppress_rebalance")
    _actual_a, rows_a = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=persistent_target,
        policy=policy,
        signal_validity_profile=profile,
        enable_staleness_filter=True,
    )
    _actual_b, rows_b = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=reversing_target,
        policy=policy,
        signal_validity_profile=profile,
        enable_staleness_filter=True,
    )

    assert rows_a[5]["staleness_filter_suppressed"] is True
    assert rows_b[5]["staleness_filter_suppressed"] is True
    assert rows_a[:6] == rows_b[:6]


def test_actual_execution_date_uses_policy_not_future_outcome() -> None:
    dates = pd.bdate_range("2026-01-02", periods=8)
    target = _stale_signal_target(dates)
    _actual, rows = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=target,
        policy=_validity_policy(),
        signal_validity_profile=_profile(stale_action="fallback_to_static_baseline"),
        enable_staleness_filter=True,
    )

    assert rows[5]["signal_generation_date"] == dates[1].date().isoformat()
    assert rows[5]["first_executable_date"] == dates[1].date().isoformat()
    assert rows[5]["actual_execution_date"] == dates[5].date().isoformat()
    assert rows[5]["position_effective_date"] == dates[5].date().isoformat()
    assert rows[5]["signal_age_at_execution_days"] == 4


def test_repaired_candidate_target_path_cannot_unlock_promotion(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics"

    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=[*ACTUAL_PATH_OWNER_REVIEW_BASELINES, *ACTUAL_PATH_OWNER_REVIEW_CANDIDATES],
        as_of_date=as_of,
        enable_staleness_filter=True,
        include_repaired_watch_only=True,
        emit_staleness_decomposition=True,
        emit_lag_decomposition=True,
        staleness_input_summary_path=tmp_path / "input_summary.md",
        staleness_repair_matrix_path=tmp_path / "staleness_repair_matrix.yaml",
        staleness_repair_review_path=tmp_path / "repair_review.md",
    )

    repaired = next(
        row
        for row in payload["strategy_rows"]
        if row["strategy_id"] == "limited_adjustment_staleness_aware_v1"
    )
    assert repaired["promotion_eligible"] is False
    assert repaired["promotion_final_status"] == "blocked"
    assert repaired["target_vs_actual_annual_return_gap"] is not None

    target_metrics = (
        output_root
        / "limited_adjustment_staleness_aware_v1"
        / "metrics_target_path.json"
    ).read_text(encoding="utf-8")
    assert '"promotion_metric_source": false' in target_metrics
    assert (output_root / "staleness_repair_summary.csv").exists()
    assert (tmp_path / "staleness_repair_matrix.yaml").exists()


def test_watch_only_repaired_candidate_remains_blocked_without_owner_review(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics"

    run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
        enable_staleness_filter=True,
        include_repaired_watch_only=True,
        staleness_input_summary_path=tmp_path / "input_summary.md",
        staleness_repair_matrix_path=tmp_path / "staleness_repair_matrix.yaml",
        staleness_repair_review_path=tmp_path / "repair_review.md",
    )

    promotion = (
        output_root
        / "limited_adjustment_staleness_aware_v1"
        / "promotion_readiness.json"
    ).read_text(encoding="utf-8")
    assert '"owner_manual_review_pending"' in promotion
    assert '"target_path_metrics_used_for_promotion": false' in promotion


def test_actual_path_metrics_required_for_staleness_repair_decision(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics"

    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        strategy_ids=["limited_adjustment"],
        as_of_date=as_of,
        enable_staleness_filter=True,
        include_repaired_watch_only=True,
        staleness_input_summary_path=tmp_path / "input_summary.md",
        staleness_repair_matrix_path=tmp_path / "staleness_repair_matrix.yaml",
        staleness_repair_review_path=tmp_path / "repair_review.md",
    )

    assert payload["summary"]["promotion_decision_source"] == "actual_path_only"
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert payload["summary"]["dynamic_promotion_blocked"] is True
    assert "limited_adjustment_staleness_aware_v1" in {
        row["strategy_id"] for row in payload["strategy_rows"]
    }


@pytest.mark.parametrize(
    ("stale_action", "expected_executed", "expected_qqq"),
    [
        ("suppress_rebalance", False, 1.0),
        ("hold_previous_position", False, 1.0),
        ("fallback_to_static_baseline", True, 0.6),
        ("no_trade", True, 0.0),
    ],
)
def test_stale_action_semantics(
    stale_action: str,
    expected_executed: bool,
    expected_qqq: float,
) -> None:
    dates = pd.bdate_range("2026-01-02", periods=8)
    target = _stale_signal_target(dates)
    _actual, rows = _actual_position_path(
        strategy_id="toy",
        execution_policy_id="validity_5d",
        target_weights=target,
        policy=_validity_policy(),
        signal_validity_profile=_profile(stale_action=stale_action),
        enable_staleness_filter=True,
    )

    assert rows[5]["rebalance_executed"] is expected_executed
    assert rows[5]["actual_weight_qqq"] == expected_qqq
    assert rows[5]["stale_action"] == stale_action


def test_execution_semantics_rebacktest_cli_accepts_staleness_repair_flags(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "cli_rebacktest"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "execution-semantics-rebacktest",
            "--strategy",
            "limited_adjustment",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output",
            str(output_root),
            "--enable-staleness-filter",
            "--include-repaired-watch-only",
            "--emit-staleness-decomposition",
            "--emit-lag-decomposition",
            "--staleness-input-summary-path",
            str(tmp_path / "input_summary.md"),
            "--staleness-repair-matrix-path",
            str(tmp_path / "staleness_repair_matrix.yaml"),
            "--staleness-repair-review-path",
            str(tmp_path / "repair_review.md"),
        ],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )

    assert result.exit_code == 0, result.output
    assert (
        output_root
        / "limited_adjustment_staleness_aware_v1"
        / "signal_staleness_decomposition.json"
    ).exists()
    assert (tmp_path / "staleness_repair_matrix.yaml").exists()


def _stale_signal_target(dates: pd.DatetimeIndex) -> pd.DataFrame:
    target = pd.DataFrame(
        {
            "QQQ": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0][: len(dates)],
            "TQQQ": [0.0] * len(dates),
            "SGOV": [0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0][: len(dates)],
        },
        index=dates,
    )
    return target


def _prices_with_future_return(
    dates: pd.DatetimeIndex,
    *,
    future_return: float,
) -> pd.DataFrame:
    qqq = [100.0] * len(dates)
    if len(qqq) > 6:
        qqq[6] = qqq[5] * (1.0 + future_return)
        for index in range(7, len(qqq)):
            qqq[index] = qqq[index - 1]
    return pd.DataFrame(
        {
            "QQQ": qqq,
            "TQQQ": [10.0] * len(dates),
            "SGOV": [100.0] * len(dates),
        },
        index=dates,
    )


def _validity_policy() -> dict[str, object]:
    return {
        "execution_policy_id": "validity_5d",
        "execution_frequency": "validity_period",
        "signal_to_execution_lag": 0,
        "minimum_holding_period": 0,
        "drift_threshold": None,
        "validity_period_days": 5,
        "max_turnover_per_period": 1.0,
        "cost_model": {"explicit_cost_bps": 0.0},
    }


def _profile(stale_action: str) -> dict[str, object]:
    return {
        "primary_signal_class": "medium_signal",
        "stale_after_days": 2,
        "near_stale_within_days": 1,
        "stale_action": stale_action,
        "actual_path_only": True,
    }
