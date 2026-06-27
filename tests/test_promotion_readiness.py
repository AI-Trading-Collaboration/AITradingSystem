from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from test_execution_semantics import _write_execution_caches

from ai_trading_system.execution_semantics import (
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    _promotion_readiness_for_rebacktest,
    run_execution_semantics_rebacktest,
)


def test_target_metrics_cannot_unlock_readiness_without_actual_metrics() -> None:
    readiness = _promotion_readiness_for_rebacktest(
        strategy_id="limited_adjustment",
        binding=_binding(),
        policy=_policy(),
        metrics_actual={},
        metrics_target=_raw_metrics(annual_return=0.40),
        lag_cost={"review_status": "pass", "status": "EXECUTION_LAG_COST_READY"},
        staleness={"review_status": "pass", "status": "SIGNAL_STALENESS_COST_READY"},
        gate={
            "status": "EXECUTION_SEMANTICS_ACTUAL_PATH_REVIEWABLE",
            "promotion_eligible": True,
            "blocking_reasons": [],
        },
        policy_hash="policy-hash",
    )

    assert readiness["final_status"] == "blocked"
    assert readiness["promotion_eligible"] is False
    assert readiness["decision_inputs"]["promotion_decision_source"] == "actual_path_only"
    assert readiness["decision_inputs"]["target_path_metrics_used_for_promotion"] is False
    assert "actual_path_rebacktest_fail" in readiness["blocking_reason_codes"]
    _assert_blocking_reason_details(readiness)


def test_owner_manual_review_pending_blocks_derived_final_status() -> None:
    readiness = _promotion_readiness_for_rebacktest(
        strategy_id="limited_adjustment",
        binding=_binding(),
        policy=_policy(),
        metrics_actual=_raw_metrics(annual_return=0.18),
        metrics_target=_raw_metrics(annual_return=0.20),
        lag_cost={"review_status": "pass", "status": "EXECUTION_LAG_COST_READY"},
        staleness={"review_status": "pass", "status": "SIGNAL_STALENESS_COST_READY"},
        gate={
            "status": "EXECUTION_SEMANTICS_ACTUAL_PATH_REVIEWABLE",
            "promotion_eligible": True,
            "blocking_reasons": [],
        },
        policy_hash="policy-hash",
    )

    assert readiness["checks"]["owner_manual_review"]["status"] == "pending"
    assert readiness["final_status"] == "blocked"
    assert readiness["status"] == "NOT_PROMOTION_ELIGIBLE"
    assert "owner_manual_review_pending" in readiness["blocking_reason_codes"]
    _assert_blocking_reason_details(readiness)


def test_lag_and_staleness_materiality_warn_blocks_until_owner_review() -> None:
    readiness = _promotion_readiness_for_rebacktest(
        strategy_id="limited_adjustment",
        binding=_binding(),
        policy=_policy(),
        metrics_actual=_raw_metrics(annual_return=0.08),
        metrics_target=_raw_metrics(annual_return=0.12),
        lag_cost={"review_status": "warn", "status": "EXECUTION_LAG_COST_MATERIAL"},
        staleness={
            "review_status": "warn",
            "status": "SIGNAL_STALENESS_COST_MATERIAL",
        },
        gate={
            "status": "EXECUTION_SEMANTICS_ACTUAL_PATH_REVIEWABLE",
            "promotion_eligible": True,
            "blocking_reasons": [],
        },
        policy_hash="policy-hash",
    )

    assert readiness["final_status"] == "blocked"
    assert "lag_cost_review_warn" in readiness["blocking_reason_codes"]
    assert "signal_staleness_review_warn" in readiness["blocking_reason_codes"]
    assert readiness["owner_waiver_schema"]["enabled_by_default"] is False


def test_rebacktest_writes_actual_path_only_aggregate_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "execution_semantics"

    payload = run_execution_semantics_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )

    assert payload["summary"]["promotion_decision_source"] == "actual_path_only"
    assert payload["summary"]["target_path_metrics_role"] == "diagnostic_only"
    assert payload["summary"]["strategy_count"] == 8
    for file_name in (
        "index.json",
        "leaderboard_actual_path.csv",
        "target_vs_actual_gap_summary.csv",
        "promotion_readiness_summary.json",
        "owner_review_pack.md",
    ):
        assert (output_root / file_name).exists()

    leaderboard = pd.read_csv(output_root / "leaderboard_actual_path.csv")
    assert "actual_path_sharpe_daily_zero_rf" in leaderboard.columns
    assert "target_path_sharpe_daily_zero_rf" not in leaderboard.columns
    assert set(leaderboard["strategy_id"]) == {
        "no_trade",
        "100_qqq",
        "qqq_60_sgov_40",
        "qqq_50_sgov_50",
        "limited_adjustment",
        "defensive_limited_adjustment",
        "dynamic_regime_overlay_v0_4_lower_turnover",
        "dynamic_v0_5_ai_trend_confirmed_only",
    }

    actual_metrics = json.loads(
        (output_root / "limited_adjustment" / "metrics_actual_path.json").read_text(
            encoding="utf-8"
        )
    )
    assert actual_metrics["promotion_metric_source"] is True
    assert set(actual_metrics["metrics"]) >= {
        "actual_path_annual_return",
        "actual_path_max_drawdown_daily_equity",
        "actual_path_sharpe_daily_zero_rf",
        "actual_path_calmar_daily_equity_dd",
    }
    assert "max_drawdown" not in actual_metrics["metrics"]

    readiness = json.loads(
        (output_root / "limited_adjustment" / "promotion_readiness.json").read_text(
            encoding="utf-8"
        )
    )
    assert readiness["schema_version"] == "dynamic_promotion_readiness.v1"
    assert readiness["final_status"] == "blocked"
    assert readiness["checks"]["target_path_not_used_for_promotion"]["status"] == "pass"
    assert readiness["decision_inputs"]["target_path_metrics_used_for_promotion"] is False
    _assert_blocking_reason_details(readiness)
    assert (
        "Target-path metrics are diagnostic only and are not eligible for promotion decisions."
        in (output_root / "owner_review_pack.md").read_text(encoding="utf-8")
    )
    policy_snapshot = yaml.safe_load(
        (output_root / "limited_adjustment" / "execution_policy_snapshot.yaml").read_text(
            encoding="utf-8"
        )
    )
    assert policy_snapshot["policy_hash"] == readiness["policy_hash"]
    assert policy_snapshot["normalized_execution_policy_contract"]["strategy_id"] == (
        "limited_adjustment"
    )
    assert policy_snapshot["normalized_execution_policy_contract"]["promotion_allowed"] is False


def test_missing_policy_binding_is_blocked_in_index(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    registry = yaml.safe_load(
        DEFAULT_EXECUTION_POLICY_REGISTRY_PATH.read_text(encoding="utf-8")
    )
    registry["strategy_execution_policies"] = [
        row
        for row in registry["strategy_execution_policies"]
        if row["strategy_id"] != "limited_adjustment"
    ]
    registry_path = tmp_path / "strategy_execution_policy_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    output_root = tmp_path / "execution_semantics"

    payload = run_execution_semantics_rebacktest(
        strategy_ids=["limited_adjustment"],
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        policy_registry_path=registry_path,
        output_root=output_root,
        as_of_date=as_of,
    )

    assert payload["status"] == "EXECUTION_SEMANTICS_REBACKTEST_COMPLETE_WITH_BLOCKED_ROWS"
    index = json.loads((output_root / "index.json").read_text(encoding="utf-8"))
    assert index["blocked_rows"][0]["strategy_id"] == "limited_adjustment"
    assert "EXECUTION_POLICY_MISSING" in index["blocked_rows"][0]["blocking_reasons"]


def _assert_blocking_reason_details(readiness: dict[str, object]) -> None:
    details = readiness["blocking_reason_details"]
    assert details
    for detail in details:
        assert detail["status"]
        assert detail["severity"]
        assert "required_action" in detail
        assert detail["evidence_artifact"]


def _binding() -> dict[str, object]:
    return {
        "strategy_id": "limited_adjustment",
        "strategy_type": "dynamic",
        "execution_policy_id": "monthly_plus_threshold_5pct_v1",
    }


def _policy() -> dict[str, object]:
    return {
        "execution_policy_id": "monthly_plus_threshold_5pct_v1",
        "signal_to_execution_lag": 1,
        "execution_frequency": "monthly_plus_threshold",
        "validity_period_days": 20,
    }


def _raw_metrics(*, annual_return: float) -> dict[str, float]:
    return {
        "annual_return": annual_return,
        "volatility_daily_annualized": 0.12,
        "max_drawdown_daily_equity": -0.08,
        "sharpe_daily_zero_rf": 1.5,
        "calmar_daily_equity_dd": 2.0,
        "turnover": 0.4,
        "constraint_hit_rate": 0.0,
    }
