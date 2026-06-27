# Signal Validity Staleness Repair Review

- status: `STALENESS_REPAIR_MATRIX_READY`
- market_regime: `ai_after_chatgpt`
- dynamic_promotion: `BLOCKED`
- target_path_metrics_role: `diagnostic_only`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`

## 1. Actual-path repair comparison

|strategy_id|actual_return|max_drawdown|sharpe|turnover|staleness_cost|lag_cost|promotion|
|---|---|---|---|---|---|---|---|
|limited_adjustment|0.192658|-0.116204|1.609148|2.4|0.0|0.002691|blocked|
|dynamic_v0_5_ai_trend_confirmed_only|0.182386|-0.091418|1.634858|15.75|0.0|-0.005669|blocked|
|limited_adjustment_staleness_aware_v1|0.192658|-0.116204|1.609148|2.4|0.0|0.002691|blocked|
|dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1|0.182386|-0.091418|1.634858|15.75|0.0|-0.005669|blocked|

## 2. Repair verdict matrix

|strategy_id|repaired_variant|verdict|annual_return_delta|staleness_cost_delta|lag_cost_delta|
|---|---|---|---|---|---|
|limited_adjustment|limited_adjustment_staleness_aware_v1|NO_MATERIAL_IMPROVEMENT|0.0|0.0|0.0|
|dynamic_v0_5_ai_trend_confirmed_only|dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1|NO_MATERIAL_IMPROVEMENT|0.0|0.0|0.0|

## 3. Staleness repair summary

|strategy_id|repaired_variant|baseline_total_staleness_cost|repaired_total_staleness_cost|staleness_cost_delta|expired_signal_suppression_delta|near_stale_signal_delta|verdict|
|---|---|---|---|---|---|---|---|
|limited_adjustment|limited_adjustment_staleness_aware_v1|0.0|0.0|0.0|48.0|0.0|NO_MATERIAL_IMPROVEMENT|
|dynamic_v0_5_ai_trend_confirmed_only|dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1|0.0|0.0|0.0|25.0|0.0|NO_MATERIAL_IMPROVEMENT|

## 4. Lag repair summary

|strategy_id|repaired_variant|baseline_total_lag_cost|repaired_total_lag_cost|lag_cost_delta|annual_return_delta|max_drawdown_delta|turnover_delta|verdict|
|---|---|---|---|---|---|---|---|---|
|limited_adjustment|limited_adjustment_staleness_aware_v1|0.002691|0.002691|0.0|0.0|0.0|0.0|NO_MATERIAL_IMPROVEMENT|
|dynamic_v0_5_ai_trend_confirmed_only|dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1|-0.005669|-0.005669|0.0|0.0|0.0|0.0|NO_MATERIAL_IMPROVEMENT|

## 5. Owner questions

1. 原始 surviving candidates 的主要实际执行损耗来自 lag/staleness sensitivity 和 static baseline underperformance。
2. staleness-aware variants 只用于 watch-only research evidence。
3. target-path metrics 没有参与 ranking、promotion 或 owner decision。
4. 没有候选可直接 promotion；dynamic promotion 继续 BLOCKED。
5. 若 matrix 标记 CANDIDATE_IDENTIFIED，下一步也只是 PAPER_SHADOW_PREFLIGHT owner review。
