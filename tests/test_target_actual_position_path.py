from __future__ import annotations

from pathlib import Path

from test_execution_semantics import _write_execution_caches

from ai_trading_system.execution_semantics import (
    run_execution_semantics_rebacktest,
    run_target_vs_actual_position_path_builder,
)


def test_target_actual_path_rows_include_execution_semantics_fields(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)

    payload = run_target_vs_actual_position_path_builder(
        strategy_id="limited_adjustment",
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "outputs",
        as_of_date=as_of,
    )

    assert payload["status"] == "TARGET_ACTUAL_PATH_READY"
    row = payload["path_rows"][0]
    assert {
        "date",
        "signal_date",
        "signal_asof_date",
        "target_weight_qqq",
        "actual_weight_qqq",
        "rebalance_allowed",
        "execution_date",
        "execution_lag_bdays",
        "signal_age_bdays",
        "is_signal_stale",
        "turnover",
        "portfolio_return_target_path",
        "portfolio_return_actual_path",
        "lag_cost_return_diff",
    } <= set(row)
    assert "max_drawdown_daily_equity" in payload["performance_metrics"]
    assert "sharpe_daily_zero_rf" in payload["performance_metrics"]


def test_rebacktest_metrics_default_to_actual_path_for_readiness(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "outputs"

    payload = run_execution_semantics_rebacktest(
        strategy_ids=["limited_adjustment"],
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )

    row = payload["strategy_rows"][0]
    actual_metrics = output_root / "limited_adjustment" / "metrics_actual_path.json"
    target_metrics = output_root / "limited_adjustment" / "metrics_target_path.json"
    promotion = output_root / "limited_adjustment" / "promotion_readiness.json"

    assert row["position_path_used_for_metrics"] == "ACTUAL"
    assert '"promotion_metric_source": true' in actual_metrics.read_text(encoding="utf-8")
    assert '"promotion_metric_source": false' in target_metrics.read_text(encoding="utf-8")
    assert '"position_path_used_for_metrics": "ACTUAL"' in promotion.read_text(
        encoding="utf-8"
    )
