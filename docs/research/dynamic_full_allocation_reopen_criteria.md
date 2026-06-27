# Dynamic Full Allocation Reopen Criteria

元数据：

- review_id：`dynamic_strategy_closeout_2026-06-27`
- source_commit：`28cabc10b042bd9da98780070aea9f85d54c5b5d`
- market_regime：`ai_after_chatgpt`
- requested date range：`2022-12-01`～`2026-06-26`
- metric_namespace：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- promotion_status：`BLOCKED`
- owner_review_status：`OWNER_REVIEW_REQUIRED`

## 当前状态

Full allocation research 重新打开状态为 `NOT_ALLOWED`。当前 closeout 之后，任何 target-path metrics、pre-execution-semantics legacy result、single-sample historical tuning 或缺 hash 的 runtime artifact 都不能作为 reopen evidence。

## 必须满足的条件

|条件|当前状态|验收要求|
|---|---|---|
|`LOCKED_SAMPLE_ACTUAL_PATH_EDGE`|`NOT_MET`|新 actual-path candidate 在 locked validation sample 稳定优于 `qqq_60_sgov_40`。|
|`MULTI_REGIME_POSITIVE_CONTRIBUTION`|`NOT_MET`|至少两个不同 market regime 中净贡献为正。|
|`RISK_TIMING_ATTRIBUTION_POSITIVE`|`NOT_MET`|risk-off / risk-on timing attribution 扣除 missed-upside 后为正。|
|`NET_OF_COST_EDGE_SURVIVES`|`NOT_MET`|net-of-cost annual return 与 risk-adjusted metrics 仍有效。|
|`STRESS_METRICS_NOT_WORSE`|`NOT_MET`|stress-day metrics 不劣于 `qqq_60_sgov_40` 与 `qqq_50_sgov_50`。|
|`TURNOVER_CONTROLLED`|`NOT_MET`|turnover 受控，且不是 noisy event override 驱动。|
|`PIT_DATA_AVAILABILITY_AUDIT_PASS`|`NOT_MET`|timestamped PIT audit 无 promotion-blocking unknown 或 approximated input。|
|`WALK_FORWARD_OUT_OF_SAMPLE_PASS`|`NOT_MET`|walk-forward / out-of-sample 无 regime overfit 或 parameter sensitivity blocker。|
|`OWNER_APPROVED_PAPER_SHADOW_PREFLIGHT`|`NOT_MET`|上述条件全部通过后，owner 手动批准 paper-shadow preflight。|

## 执行纪律

重新打开 full allocation research 必须先更新 task register 和支持文档。任何 reopen attempt 都必须说明 selected market regime、requested date range、data quality status、policy/config version 和 validation evidence。
