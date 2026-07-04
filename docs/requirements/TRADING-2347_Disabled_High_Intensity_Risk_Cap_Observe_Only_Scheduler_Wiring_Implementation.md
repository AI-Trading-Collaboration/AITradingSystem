# TRADING-2347 Disabled High-Intensity Risk-Cap Observe-Only Scheduler Wiring Implementation

最后更新：2026-07-05

## Status

- task_id: `TRADING-2347_HIGH_INTENSITY_RISK_CAP_OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTATION`
- status: `DONE`
- priority: `P0`
- owner: `系统实现 + 项目 owner 后续复核`
- last_update: `2026-07-05`

## Context

TRADING-2345 已完成 observe-only scheduler dry-run，真实 run status 为
`OBSERVE_ONLY_SCHEDULER_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`。
TRADING-2346 已完成 observe-only scheduler wiring plan，真实 run status 为
`OBSERVE_ONLY_SCHEDULER_WIRING_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，
并 route 到
`TRADING-2347_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Disabled_Wiring_Implementation`。

本任务只实现 disabled-by-default scheduler wiring implementation skeleton。它
让系统能读取 2345 / 2346 既有研究产物、生成 disabled implementation manifest、
暴露 CLI 检查入口和 2348 smoke dry-run route；它不是 scheduler activation，
不是每日自动任务，不 append event，不绑定 outcome，不进入 paper-shadow、
production 或 broker path。

## Scope

1. 新增
   `aits research trends high-intensity-risk-cap-observe-only-scheduler-disabled-wiring`。
2. 读取 TRADING-2346 wiring plan outputs 和 TRADING-2345 scheduler dry-run
   summary / route / safety artifacts。
3. Fail-closed 校验 2346 status、route、readiness、safety gate、disabled
   flags、source validation 和 2345 reference。
4. 生成 disabled wiring implementation manifest、guardrail status、
   referenced artifact manifest、no-real-scheduler assertion、2348 readiness
   checklist、task route、interpretation boundary 和 safety boundary。
5. 更新 report registry、artifact catalog、system flow、task register 和
   research docs。

## Safety Boundary

- `scheduler_enabled=false`
- `manual_run_only=true`
- `dry_run_only=true`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `paper_shadow_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `promotion_allowed=false`

Forbidden behavior includes enabling scheduler, creating cron / Windows Task /
GitHub Action schedule, appending historical event log, mutating outcome stores,
binding advisory outcomes, enabling paper-shadow or production, sending orders,
calling broker APIs, reading fresh market data, emitting target weights, or
generating rebalance instructions.

## Acceptance Criteria

- Focused loader/manifest/guardrail/no-real-scheduler/route/CLI tests pass.
- Real CLI run writes all TRADING-2347 runtime artifacts under
  `outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_disabled_wiring/`.
- Generated docs clearly state this is disabled wiring implementation only, not
  scheduler activation.
- Status equals
  `OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_PROMOTION_BLOCKED`.
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
  TRADING-2346 wiring plan 落成 disabled-by-default implementation manifest
  和 CLI 检查入口，而不是启用 scheduler、daily task、paper-shadow、
  production 或 broker action。
- 2026-07-05: 实现完成并进入 `VALIDATING`。真实 CLI run 写出 disabled
  wiring implementation artifacts，status=
  `OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_IMPLEMENTED_WITH_CAVEATS_PROMOTION_BLOCKED`，
  guardrail_status=`PASS`，source validation=`2026-06-29` /
  `PASS_WITH_WARNINGS` / error_count=0，2348 route 指向 disabled scheduler
  wiring smoke dry-run / guardrail evidence。未重跑 `aits validate-data`，
  因为本任务只读取 prior validated research artifacts，不读取 fresh market data、
  不 append event、不绑定 outcome。
- 2026-07-05: 完整验证通过并归档 `DONE`。后续只能进入
  `TRADING-2348_Disabled_Scheduler_Wiring_Smoke_Dry_Run_And_Guardrail_Evidence`
  with caveats；TRADING-2347 仍不是 scheduler enabled、daily task enabled、
  event append、outcome binding、paper-shadow、production 或 broker readiness。
