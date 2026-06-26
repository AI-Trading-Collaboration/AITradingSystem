# Static Baseline External Manual Runbook

- 状态：`MANUAL_RUNBOOK_READY`
- paper_shadow_allowed：`false`
- production_allowed：`false`
- broker_action：`none`
- manual_review_required：`true`

## Baselines

|strategy_id|asset_weights|rebalance_frequency|
|---|---|---|
|`100_qqq`|`{"QQQ": 1.0}`|`monthly`|
|`qqq_50_sgov_50`|`{"QQQ": 0.5, "SGOV": 0.5}`|`monthly`|
|`qqq_60_sgov_40`|`{"QQQ": 0.6, "SGOV": 0.4}`|`monthly`|

## Required External Platform Steps

1. Run each baseline on Portfolio Visualizer, testfol.io, or another platform that supports ETF portfolio backtests.
2. Match the internal requested date range, monthly rebalance, and dividend reinvestment setting where the platform allows it.
3. Record annual return, max drawdown, Sharpe, Calmar, turnover, and monthly returns availability. Use `metric_unavailable_on_platform` when unavailable.
4. Record SGOV handling as `unknown`, `price_only`, `adjusted`, `total_return`, or `platform_default`.
5. Save a screenshot or export CSV and fill `inputs/external_validation/manual_external_records/static_baseline_external_records.yaml` or `.csv`.

Do not include broker account data, personal account screenshots, real trading instructions, or production readiness claims.
