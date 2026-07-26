# Decision Target Tail-Risk Robustness / Falsification Audit

- 结论：`INSUFFICIENT_ROBUSTNESS_EVIDENCE`
- 状态：`TAIL_RISK_ROBUSTNESS_AUDIT_READY`
- Primary target：`QQQ_FUTURE_WORST_1D_RETURN`
- Selected research window：`2021-02-22..2026-07-24`
- Actual evaluated range：`2021-08-11..2026-06-25`
- 数据角色：historical-seen falsification audit，不是prospective/OOS业绩证明。

## 证伪门禁

|Gate|Pass|
|---|---:|
|Exact reconstruction|true|
|Mandatory variants|false|
|Fold influence|true|
|Regime concentration|false|
|Event calibration|false|
|Placebo rejection|true|

## Variant 结果

|Variant|Mandatory|Passing horizons|Target supported|
|---|---:|---|---:|
|EXACT_PRIMARY|true|1d, 5d, 10d|true|
|FEATURE_LAG_1|true|5d, 10d|true|
|EMBARGO_40|true|5d, 10d|true|
|DROP_SPY_DERIVED|true|5d|false|
|DROP_SGOV_DERIVED|true|5d, 10d|true|
|PRICE_TREND_ONLY|false|1d, 5d|false|
|VOLATILITY_DRAWDOWN_ONLY|false|5d, 10d|true|
|CROSS_ASSET_STATE_ONLY|false|1d, 5d, 10d|true|

## 不足证据

- `REGIME_STRATUM_NOT_EVALUABLE`
- `EVENT_CALIBRATION_NOT_EVALUABLE`

Regime strata 预注册样本地板为80；以下 pooled strata 未达到地板：

|Regime|Horizon|Stratum|Rows|Spearman|
|---|---|---|---:|---:|
|DRAWDOWN|10d|LOW|63|0.2410|
|DRAWDOWN|1d|LOW|65|0.0105|
|DRAWDOWN|5d|LOW|64|0.2463|
|VOLATILITY|10d|HIGH|69|0.0579|
|VOLATILITY|1d|HIGH|69|0.1228|
|VOLATILITY|5d|HIGH|69|0.2780|

Event calibration 每个 horizon/quantile 预注册要求至少6个eligible folds；实际如下：

|Horizon|Tail quantile|Eligible folds / 7|Passing folds|
|---|---:|---:|---:|
|1d|0.10|2 / 7|1|
|1d|0.20|2 / 7|1|
|5d|0.10|1 / 7|1|
|5d|0.20|1 / 7|1|
|10d|0.10|1 / 7|0|
|10d|0.20|1 / 7|1|

## Placebo

|Horizon|Actual Spearman|Null p95|Empirical p|Pass|
|---|---:|---:|---:|---:|
|1d|0.0517|0.0346|0.005|true|
|5d|0.1723|0.1410|0.025|true|
|10d|0.1851|0.1139|0.010|true|

## 解释

至少一个预注册mandatory axis不可评估，不能把缺失证据解释为稳健或失败。

## 下一步

`CLOSE_TAIL_RISK_PATH_OR_REDESIGN_DECISION_TARGET`。本任务没有建立risk overlay；只有`ROBUST`才允许Owner另立Decision Value Audit，仍不自动批准candidate或权重。

## 数据质量与安全边界

- full canonical DQ=`FAIL`
- QQQ/SPY/SGOV scoped DQ=`PASS`
- `global_cache_pass_claimed=false`
- `candidate_family_created=false`
- `risk_overlay_created=false`
- `strategy_backtest_executed=false`
- `target_weights_generated=false`
- `qld_used_as_signal=false`
- `production_effect=none`
- `broker_action=none`
