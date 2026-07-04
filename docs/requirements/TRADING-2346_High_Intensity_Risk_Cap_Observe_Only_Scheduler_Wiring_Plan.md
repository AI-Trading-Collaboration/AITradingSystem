# TRADING-2346 High-Intensity Risk-Cap Observe-Only Scheduler Wiring Plan

最后更新：2026-07-05

## Status

- task_id: `TRADING-2346_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_WIRING_PLAN`
- status: `DONE`
- priority: `P0`
- owner: `系统实现 + 项目 owner 后续复核`
- last_update: `2026-07-05`

## Context

TRADING-2345 已完成 high-intensity risk-cap observe-only scheduler dry-run，
真实 run status 为
`OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`。
`would_append_event_count=0` 来自 deterministic de-dup 命中既有
TRADING-2336 historical event log / cluster registry，不是 trigger rule 失败。

本任务承接 TRADING-2345 route，只生成 observe-only scheduler wiring plan 和
TRADING-2347 disabled wiring implementation contract。TRADING-2346 不实现
scheduler wiring，不启用 scheduler，不写 enabled scheduler config，不接入每日自动任务，
不 append event，不绑定 outcome，不进入 paper-shadow、production 或 broker action。

## Scope

1. 新增
   `aits research trends high-intensity-risk-cap-observe-only-scheduler-wiring-plan`。
2. 读取 TRADING-2345 scheduler dry-run outputs、TRADING-2344 scheduler integration
   plan、TRADING-2335 selected rule 和 TRADING-2336 event / cluster / pending
   lineage。
3. Fail-closed 校验 scheduler dry-run 已通过且 promotion 仍 blocked。
4. 生成 disabled-by-default scheduler config entry plan、manual-run-only contract、
   dry-run-only mode contract、job wiring order、artifact / registry wiring plan、
   failure handling、rollback、owner review requirement、wiring safety gate、
   implementation contract、2347 readiness checklist 和 task route。
5. 生成 research docs，并更新 report registry、artifact catalog、system flow 和
   task register。

## Safety Boundary

- `scheduler_enabled=false`
- `scheduler_default_enabled=false`
- `manual_run_only=true`
- `dry_run_only=true`
- `observe_only=true`
- `event_append_executed=false`
- `outcome_binding_executed=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`

Forbidden outputs include `target_weight_action`, `rebalance_instruction`,
`buy_signal`, `sell_signal`, `reduce_position_instruction`,
`increase_cash_instruction`, `paper_shadow_ready`, `production_ready`,
`broker_action`, `automatic_exposure_cap`, `scheduler_enabled`, and
`scheduler_default_enabled`.

## Acceptance Criteria

- Focused loader/config/policy/manual-run/safety-gate/route/CLI tests pass.
- Real CLI run writes all TRADING-2346 runtime artifacts under
  `outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_wiring_plan/`.
- Generated docs clearly state this is wiring plan only, not scheduler activation.
- `source_validate_data_executed=true`,
  `source_validate_data_as_of=2026-06-29`,
  `source_validate_data_status=PASS_WITH_WARNINGS`, and
  `source_validate_data_error_count=0` are inherited and reported.
- `aits validate-data` is not rerun unless implementation starts consuming fresh
  market data, which this task must not do.
- `docs/system_flow.md`, `docs/artifact_catalog.md`,
  `config/report_registry.yaml`, and `docs/task_register.md` are updated in the
  same change.

## Progress Notes

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。核心验收点是把
  TRADING-2345 dry-run 结果转成可实现的 disabled-by-default scheduler wiring
  plan，而不是启用 scheduler 或生成任何交易动作。
- 2026-07-05: 实现完成并进入 `VALIDATING`。真实 CLI run 写出 scheduler
  wiring plan artifacts，status=
  `OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，
  source validation=`2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0，
  2347 route 指向 disabled wiring implementation。未重跑 `aits validate-data`，
  因为本任务只读取 prior validated research artifacts，不读取 fresh market data、
  不 append event、不绑定 outcome。
- 2026-07-05: 完整验证通过并归档 `DONE`。后续只能进入
  `TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Disabled_Wiring_Implementation`
  with caveats；TRADING-2346 仍不是 scheduler enabled、daily task enabled、
  event append、outcome binding、paper-shadow、production 或 broker readiness。
