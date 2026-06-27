# Execution Semantics Actual-Path Rebacktest Review

## 结论

- Review status: `NON_BLOCKING_WARNING`
- Dynamic promotion status: `BLOCKED`
- Runtime source: `outputs/research_strategies/execution_semantics/`
- Snapshot: `docs/research/artifact_snapshots/execution_semantics_actual_path_rebacktest_snapshot.yaml`

本次复核把本地 ignored runtime artifacts 固化为 tracked evidence snapshot。`PASS_WITH_WARNINGS`
的唯一 `WARNING` 是主价格源 1 行 `prices_adjustment_ratio_jump`，数据质量 gate
仍为 `passed=true`、`error_count=0`。该 warning 需要继续披露，但本次没有证据显示它改变
actual-path strategy ranking；actual 与 target 排序差异来自 execution policy、lag 和
staleness，而不是数据质量 gate failure。

Dynamic promotion 没有重新打开。4 个 dynamic 策略仍为 `NOT_PROMOTION_ELIGIBLE`，主要阻断
原因是 `SIGNAL_STALENESS_COST_MATERIAL`、`EXECUTION_LAG_COST_MATERIAL` 和
`OWNER_REVIEW_REQUIRED_BEFORE_DYNAMIC_PROMOTION`。

## Run Metadata

|字段|值|
|---|---|
|gate generated_at|`2026-06-27T02:51:04Z`|
|rebacktest generated_at|`2026-06-27T02:51:15Z`|
|data gate checked_at|`2026-06-27T02:51:13.740122+00:00`|
|market regime|`ai_after_chatgpt`|
|date range|`2022-12-01` to `2026-06-26`|
|gate command|`aits research strategies execution-semantics-rebacktest-gate`|
|rebacktest command|`aits research strategies execution-semantics-rebacktest --policy-registry config/research/strategy_execution_policy_registry.yaml --output outputs/research_strategies/execution_semantics`|
|policy registry path|`config/research/strategy_execution_policy_registry.yaml`|
|policy registry sha256|`5c1b64260ffa4e9154dc27c9596b7cf2111fb07d03121b3ad5610f3f23922df0`|

## Strategies Included

- `defensive_limited_adjustment`
- `limited_adjustment`
- `no_trade`
- `dynamic_regime_overlay_v0_4_lower_turnover`
- `dynamic_v0_5_ai_trend_confirmed_only`

## Artifact Completeness

|strategy_id|summary|metrics_actual_path|metrics_target_path|target_vs_actual_position_path|lag_cost_report|signal_staleness_report|execution_policy_snapshot|promotion_readiness|complete|
|---|---|---|---|---|---|---|---|---|---|
|`defensive_limited_adjustment`|present|present|present|present|present|present|present|present|yes|
|`limited_adjustment`|present|present|present|present|present|present|present|present|yes|
|`no_trade`|present|present|present|present|present|present|present|present|yes|
|`dynamic_regime_overlay_v0_4_lower_turnover`|present|present|present|present|present|present|present|present|yes|
|`dynamic_v0_5_ai_trend_confirmed_only`|present|present|present|present|present|present|present|present|yes|

## Gate Status

|字段|值|
|---|---|
|gate status|`EXECUTION_SEMANTICS_REBACKTEST_REQUIRED`|
|gate strategy_id|`equal_risk_qqq_sgov`|
|gate strategy_type|`dynamic`|
|promotion_eligible|`false`|
|rebacktest_required|`true`|
|legacy result tags|`PRE_EXECUTION_SEMANTICS`, `REBACKTEST_REQUIRED`, `NOT_PROMOTION_ELIGIBLE`|
|blocked reasons|`PRE_EXECUTION_SEMANTICS`, `TARGET_PATH_NOT_PROMOTION_ELIGIBLE`, `EXECUTION_SEMANTICS_REBACKTEST_REQUIRED`|

The gate artifact is an intentionally conservative legacy-result freeze. It remains valid even
after the five actual-path artifacts were generated, because dynamic promotion still also requires
owner review and promotion-readiness checks.

## Data Quality Status

|字段|值|
|---|---|
|status|`PASS_WITH_WARNINGS`|
|passed|`true`|
|error_count|`0`|
|warning_count|`1`|
|price_path|`data/raw/prices_daily.csv`|
|secondary_prices_path|`data/raw/prices_marketstack_daily.csv`|
|rates_path|`data/raw/rates_daily.csv`|
|price checksum in rebacktest artifact|`5d4f5a4843a8cd5541e794e26865e35105345f212554216fd7fba338a76f689d`|
|rate checksum in rebacktest artifact|`279a1af07cda2aaf17e938ac40d84fbd84c0724a9791af61800d09396754841a`|

### PASS_WITH_WARNINGS 明细

|severity|source|code|rows|interpretation|
|---|---|---|---:|---|
|WARNING|价格主源|`prices_adjustment_ratio_jump`|1|复权比例出现明显跳变。同日 data quality report 的样例为 `TQQQ` on `2025-11-20`，`_adjustment_ratio=0.9946178686759957`。|

### INFO 明细

|source|code|rows|interpretation|
|---|---|---:|---|
|价格主源|`prices_index_volume_not_applicable`|2163|指数或非成交标的 volume 不适用，不阻断。|
|价格主源|`prices_suspicious_adj_close_move`|20|较大调整收盘价波动，未达到 extreme error 阈值。|
|价格主源|`prices_known_split_adjustment_ratio_jump`|6|匹配已配置 corporate action。|
|第二行情源 Marketstack|`prices_non_positive_close`|14|第二源自检问题，主源可用时不改写主源。|
|第二行情源 Marketstack|`prices_non_positive_adj_close`|14|第二源自检问题，主源可用时不改写主源。|
|第二行情源 Marketstack|`prices_invalid_ohlc`|28|第二源 OHLC 自检问题。|
|第二行情源 Marketstack|`prices_extreme_adj_close_move`|5|第二源 adjusted close 异常。|
|第二行情源 Marketstack|`prices_suspicious_adj_close_move`|21|第二源较大调整收盘价波动。|
|第二行情源 Marketstack|`prices_known_split_adjustment_ratio_jump`|7|第二源匹配已配置 corporate action。|
|跨源核验：主价格源 vs Marketstack|`secondary_prices_adjustment_basis_warning`|1328|raw close 通过，adjusted close 差异按供应商复权口径限制披露。|
|跨源核验：主价格源 vs Marketstack|`secondary_prices_adjustment_basis_info`|681|raw close 通过，adjusted close 差异按分红复权口径记录。|

### Warning Impact Diagnosis

- Warning classification: `NON_BLOCKING_WARNING`
- Ranking impact: `not_evidenced`
- Required action: `keep_warning_visible_and_recheck_if_validation_escalates`

理由：

- `_data_quality_gate` 在 rebacktest 中返回 `passed=true` 且 `error_count=0`，没有缺失 schema、
  缺失 ticker、freshness failure 或 duplicate-key hard failure。
- 5 个策略均完成 actual-path artifacts，`metrics_actual_path.json` 和
  `metrics_target_path.json` 全部存在。
- strategy ranking 使用同一 validated primary price matrix、同一 date range 和同一
  execution policy registry；没有 strategy-specific data-quality blocker。
- actual-path 与 target-path 排序差异和 lag/staleness 指标一致，属于 execution semantics
  结果，不属于数据质量 warning 的独立排序扰动。

## Actual Path vs Target Path

|strategy_id|annual_return_target|annual_return_actual|annual_return_actual_minus_target|max_drawdown_target|max_drawdown_actual|sharpe_target|sharpe_actual|turnover_target|turnover_actual|
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|`limited_adjustment`|0.195349|0.192658|-0.002691|-0.122844|-0.116204|1.633195|1.609148|3.60|2.40|
|`dynamic_regime_overlay_v0_4_lower_turnover`|0.197382|0.183619|-0.013763|-0.115790|-0.124682|1.577809|1.443630|3.00|1.80|
|`dynamic_v0_5_ai_trend_confirmed_only`|0.176717|0.182386|0.005669|-0.087536|-0.091418|1.591866|1.634858|23.85|15.75|
|`defensive_limited_adjustment`|0.161453|0.151676|-0.009777|-0.096949|-0.075597|1.694363|1.651764|7.40|3.00|
|`no_trade`|0.047230|0.047230|0.000000|-0.000231|-0.000231|19.775242|19.775242|0.00|0.00|

Actual-path annual-return ranking among dynamic strategies:

1. `limited_adjustment`：0.192658
2. `dynamic_regime_overlay_v0_4_lower_turnover`：0.183619
3. `dynamic_v0_5_ai_trend_confirmed_only`：0.182386
4. `defensive_limited_adjustment`：0.151676

Target-path annual-return ranking among dynamic strategies:

1. `dynamic_regime_overlay_v0_4_lower_turnover`：0.197382
2. `limited_adjustment`：0.195349
3. `dynamic_v0_5_ai_trend_confirmed_only`：0.176717
4. `defensive_limited_adjustment`：0.161453

This top-rank reversal is an execution-semantics finding. It reinforces that promotion/ranking
must use actual path metrics, not target path metrics.

## Lag Cost Summary

|strategy_id|lag status|annual_return_lag_cost|drawdown_lag_cost|sharpe_lag_cost|
|---|---|---:|---:|---:|
|`defensive_limited_adjustment`|`EXECUTION_LAG_COST_WARN`|0.009777|-0.021352|0.042599|
|`limited_adjustment`|`EXECUTION_LAG_COST_READY`|0.002691|-0.006640|0.024047|
|`no_trade`|`EXECUTION_LAG_COST_READY`|0.000000|0.000000|0.000000|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`EXECUTION_LAG_COST_MATERIAL`|0.013763|0.008892|0.134179|
|`dynamic_v0_5_ai_trend_confirmed_only`|`EXECUTION_LAG_COST_WARN`|-0.005669|0.003882|-0.042992|

## Signal Staleness Summary

|strategy_id|staleness status|average_signal_age_bdays|p95_signal_age_bdays|stale_signal_days|stale_signal_day_pct|
|---|---|---:|---:|---:|---:|
|`defensive_limited_adjustment`|`SIGNAL_STALENESS_COST_MATERIAL`|10.143|20.0|420|0.469799|
|`limited_adjustment`|`SIGNAL_STALENESS_COST_MATERIAL`|9.952|20.0|33|0.036913|
|`no_trade`|`SIGNAL_STALENESS_COST_READY`|446.500|849.0|0|0.000000|
|`dynamic_regime_overlay_v0_4_lower_turnover`|`SIGNAL_STALENESS_COST_MATERIAL`|66.709|167.0|613|0.685682|
|`dynamic_v0_5_ai_trend_confirmed_only`|`SIGNAL_STALENESS_COST_MATERIAL`|28.652|94.0|418|0.467562|

## Promotion Readiness Summary

|strategy_id|strategy_type|promotion_readiness_status|promotion_eligible|blocked reasons|
|---|---|---|---|---|
|`defensive_limited_adjustment`|dynamic|`NOT_PROMOTION_ELIGIBLE`|false|`SIGNAL_STALENESS_COST_MATERIAL`, `OWNER_REVIEW_REQUIRED_BEFORE_DYNAMIC_PROMOTION`|
|`limited_adjustment`|dynamic|`NOT_PROMOTION_ELIGIBLE`|false|`SIGNAL_STALENESS_COST_MATERIAL`, `OWNER_REVIEW_REQUIRED_BEFORE_DYNAMIC_PROMOTION`|
|`no_trade`|static|`PROMOTION_REVIEWABLE`|true|none|
|`dynamic_regime_overlay_v0_4_lower_turnover`|dynamic|`NOT_PROMOTION_ELIGIBLE`|false|`EXECUTION_LAG_COST_MATERIAL`, `SIGNAL_STALENESS_COST_MATERIAL`, `OWNER_REVIEW_REQUIRED_BEFORE_DYNAMIC_PROMOTION`|
|`dynamic_v0_5_ai_trend_confirmed_only`|dynamic|`NOT_PROMOTION_ELIGIBLE`|false|`SIGNAL_STALENESS_COST_MATERIAL`, `OWNER_REVIEW_REQUIRED_BEFORE_DYNAMIC_PROMOTION`|

`no_trade` being reviewable is not dynamic promotion re-enable; it is a static no-rebalance/safe
baseline row with no target/actual divergence. Dynamic promotion remains blocked.

## Remaining Actions

- Keep dynamic promotion disabled until owner review explicitly accepts actual-path evidence,
  lag/staleness costs, metric namespace, and policy binding.
- Keep `prices_adjustment_ratio_jump` visible in downstream reports; if it escalates to an error,
  changes ticker coverage, or maps to a strategy-specific hard blocker, rerun this snapshot and
  mark the warning diagnosis as `BLOCKED`.
