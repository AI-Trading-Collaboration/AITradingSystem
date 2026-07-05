# 动态策略 research-only shadow observation replay validation

## Executive summary

- status：`DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_REPLAY_NO_SIDE_EFFECT_VALIDATION_READY`
- observation mode：`RESEARCH_ONLY_DRY_RUN_REPLAY_VALIDATION`
- replay count：`3`
- stable semantic replay passed：`True`
- observation decision：`OWNER_REVIEW_REQUIRED`
- owner review required：`True`

## Source dry-run from TRADING-2369

- source status：`DYNAMIC_STRATEGY_RESEARCH_ONLY_SHADOW_OBSERVATION_DRY_RUN_READY`

## Replay validation method

- 对 stable semantic fields 做 canonical JSON 和 SHA-256 hash。
- 对 volatile runtime fields 执行排除规则。
- 同一 stable semantic payload replay 3 次并比较 hash。

## Stable semantic fields

`task_id`, `status`, `source_tasks`, `observation_mode`, `primary_observation_candidate`, `ranking_top_from_2365`, `robustness_top_from_2366`, `execution_cadence`, `observation_protocol_loaded`, `observation_field_schema_loaded`, `review_thresholds_loaded`, `observation_decision`, `owner_review_required`, `research_only_shadow_observation_allowed`, `paper_shadow_enabled`, `paper_trade_created`, `shadow_position_created`, `event_append_enabled`, `event_append_attempted`, `outcome_binding_enabled`, `outcome_binding_attempted`, `scheduler_enabled`, `production_enabled`, `broker_action_enabled`, `broker_action_attempted`, `daily_report_generated`, `recommended_next_research_task`

## Volatile field exclusion rule

`generated_at`, `created_at`, `updated_at`, `runtime_id`, `runtime_artifact`, `runtime_artifact_path`, `duration_ms`, `elapsed_seconds`, `local_path`, `absolute_path`, `host`, `machine`, `process_id`, `git_dirty_state_when_generated`

## Replay result table

|replay|hash|decision|side effects false|
|---|---|---|---|
|1|`0b08060f43c2a54c83d039a399349f5464cf524b2458432fb1395a5880ff1d89`|`OWNER_REVIEW_REQUIRED`|`True`|
|2|`0b08060f43c2a54c83d039a399349f5464cf524b2458432fb1395a5880ff1d89`|`OWNER_REVIEW_REQUIRED`|`True`|
|3|`0b08060f43c2a54c83d039a399349f5464cf524b2458432fb1395a5880ff1d89`|`OWNER_REVIEW_REQUIRED`|`True`|

## No-side-effect evidence

- 是否创建 paper trade：否。
- 是否创建 shadow position：否。
- 是否写 event：否。
- 是否 bind outcome：否。
- 是否生成 daily report：否。
- 是否触发 production / broker：否。

## Observation decision stability

- observation decision：`OWNER_REVIEW_REQUIRED`
- owner review required：`True`

## Explicit non-goals

- 不读取 fresh market data，不运行新 backtest，不生成新 signal。
- 不启用 scheduler，不创建 scheduled task。
- 不 append event，不 bind outcome，不 mutate outcome store。
- 不启用 paper-shadow，不创建 paper trade 或 shadow position。
- 不进入 production，不调用 broker，不发送 order。

## Recommended next route

- next route：`TRADING-2371_Dynamic_Strategy_Research_Only_Shadow_Observation_Owner_Review_Decision`