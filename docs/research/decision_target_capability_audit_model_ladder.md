# Decision Target Capability Audit Batch 2

- 结论：`TAIL_RISK_ONLY_SKILL`
- 运行状态：`CAPABILITY_AUDIT_READY`
- outer folds：7
- predictions：118300
- primary classification model：`M1_RIDGE_LINEAR`
- primary feature prefix：`CROSS_ASSET_STATE`
- selected research window：`2021-02-22..2026-07-24`
- actual evaluated feature/label range：`2021-08-11..2026-06-25`
- data quality：full canonical=`FAIL`，QQQ/SPY/SGOV scoped=`PASS`，`global_cache_pass_claimed=false`
- 数据角色：historical-seen capability audit，不是 prospective/OOS 业绩证明。

## Target 能力

|Target|通过 horizon|结论|
|---|---:|---|
|`QQQ_MINUS_SGOV`|0|`TARGET_SKILL_NOT_SUPPORTED`|
|`SPY_MINUS_SGOV`|0|`TARGET_SKILL_NOT_SUPPORTED`|
|`QQQ_MINUS_SPY`|0|`TARGET_SKILL_NOT_SUPPORTED`|
|`QQQ_FUTURE_MAX_DRAWDOWN`|1|`TARGET_SKILL_NOT_SUPPORTED`|
|`QQQ_FUTURE_WORST_1D_RETURN`|3|`TARGET_SKILL_SUPPORTED`|

## Purged walk-forward

|Fold|Train cutoff|Test range|Train sessions|Test sessions|
|---|---|---|---:|---:|
|`F01`|2023-01-11|2023-02-10..2023-08-11|358|126|
|`F02`|2023-07-14|2023-08-14..2024-02-12|484|126|
|`F03`|2024-01-12|2024-02-13..2024-08-13|610|126|
|`F04`|2024-07-16|2024-08-14..2025-02-13|736|126|
|`F05`|2025-01-15|2025-02-14..2025-08-15|862|126|
|`F06`|2025-07-18|2025-08-18..2026-02-17|988|126|
|`F07`|2026-01-16|2026-02-18..2026-06-25|1114|89|

## 解释

收益目标没有通过预注册门槛，但左尾目标具有稳定能力；后续只能研究risk gate或defensive overlay，不能把它解释为收益生成策略。

本报告只决定下一步值得研究的风格。它没有创建 candidate family，没有计算交易成本后策略收益，没有生成 target weights，也没有把 QLD 用作信号。

## 下一步

`OWNER_STYLE_REVIEW_NO_AUTOMATIC_CANDIDATE`：必须由 Owner 复核后另立新 family 或终止该方向。

## 安全边界

- `prospective_accessed=false`
- `candidate_family_created=false`
- `strategy_backtest_executed=false`
- `target_weights_generated=false`
- `qld_used_as_signal=false`
- `production_effect=none`
- `broker_action=none`
