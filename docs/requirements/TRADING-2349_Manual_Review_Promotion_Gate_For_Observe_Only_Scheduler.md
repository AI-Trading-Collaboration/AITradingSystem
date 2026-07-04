# TRADING-2349 Manual Review Promotion Gate For Observe-Only Scheduler

最后更新：2026-07-05

## Status

- task_id: `TRADING-2349_MANUAL_REVIEW_PROMOTION_GATE_FOR_OBSERVE_ONLY_SCHEDULER`
- status: `DONE`
- priority: `P0`
- owner: `系统实现 + 项目 owner 后续复核`
- last_update: `2026-07-05`

## Context

TRADING-2347 已完成 disabled-by-default high-intensity risk-cap observe-only
scheduler wiring implementation。TRADING-2348 已完成 disabled wiring smoke
dry-run and guardrail evidence，真实 status 为
`OBSERVE_ONLY_SCHEDULER_DISABLED_WIRING_SMOKE_DRY_RUN_PASSED_WITH_CAVEATS_PROMOTION_BLOCKED`。

本任务新增人工评审 promotion gate package，用来汇总 2347 / 2348 evidence
并判断是否只具备进入下一阶段 manual-run interface dry-run review 的条件。
2349 本身仍必须保持 promotion blocked；它不是 scheduler enablement、不是
paper-shadow、不是 production，也不是 broker path。

## Scope

1. 新增
   `aits research trends high-intensity-risk-cap-observe-only-scheduler-manual-review-gate`。
2. 读取 TRADING-2347 disabled wiring artifacts 和 TRADING-2348 smoke dry-run
   evidence artifacts。
3. Fail-closed 校验 2347 / 2348 status、route、guardrails、side-effect
   assertions、source validation carry-forward 和全部 disabled safety flags。
4. 生成 manual review promotion gate package、source artifact review、
   blocked promotion decision、2350 manual-run interface route、interpretation
   boundary 和 safety boundary。
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
- `manual_review_required=true`

Forbidden behavior includes enabling scheduler, creating cron / Windows Task /
GitHub Actions schedule, appending or mutating historical event logs, binding
advisory outcomes, mutating outcome stores, entering paper-shadow or production,
calling broker APIs, sending orders, reading fresh market data, emitting target
weights, or generating rebalance instructions.

## Acceptance Criteria

- Focused loader/review/decision/route/CLI tests pass.
- Real CLI run writes all TRADING-2349 artifacts under
  `outputs/research_trends/high_intensity_risk_cap_observe_only_scheduler_manual_review_gate/`.
- Generated docs clearly state this is a manual review promotion gate only, not
  scheduler activation.
- Status equals
  `OBSERVE_ONLY_SCHEDULER_MANUAL_REVIEW_GATE_READY_WITH_CAVEATS_PROMOTION_BLOCKED`.
- Promotion decision equals `BLOCKED`.
- Evidence exposes `promotion_allowed=false`, `scheduler_enabled=false`,
  `manual_run_only=true`, `dry_run_only=true`, `manual_review_required=true`,
  `paper_shadow_enabled=false`, `production_enabled=false`, and
  `broker_action_enabled=false`.
- Review findings expose `disabled_wiring_present=true`,
  `smoke_dry_run_passed=true`, `guardrail_evidence_present=true`,
  `side_effect_assertions_present=true`, and
  `promotion_evidence_sufficient_for_enablement=false`.
- Next route equals
  `TRADING-2350_Observe_Only_Scheduler_Manual_Run_Interface_Dry_Run`.
- `source_validate_data_executed=true`,
  `source_validate_data_as_of=2026-06-29`,
  `source_validate_data_status=PASS_WITH_WARNINGS`, and
  `source_validate_data_error_count=0` are inherited and reported.
- `aits validate-data` is not rerun unless implementation starts consuming fresh
  market data, which this task must not do.

## Progress Notes

- 2026-07-05: 根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接
  TRADING-2348 `READY_FOR_2349_WITH_CAVEATS` / manual review promotion gate
  route，只汇总 2347 / 2348 evidence 并输出 blocked promotion decision；
  不得启用 scheduler、创建 cron / Windows Task / GitHub Actions schedule、
  append event、绑定 outcome、读取 fresh market data、进入 paper-shadow /
  production / broker。
- 2026-07-05: 实现完成并归档 `DONE`。真实 CLI run status=
  `OBSERVE_ONLY_SCHEDULER_MANUAL_REVIEW_GATE_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，
  promotion_decision=`BLOCKED`，promotion_allowed=false，
  manual_review_required=true，scheduler_enabled=false，manual_run_only=true，
  dry_run_only=true，paper_shadow_enabled=false，production_enabled=false，
  broker_action_enabled=false，readiness=`READY_FOR_2350_WITH_CAVEATS`，
  next route=`TRADING-2350_Observe_Only_Scheduler_Manual_Run_Interface_Dry_Run`。
  review findings 证明 disabled wiring、smoke dry-run、guardrail evidence 和
  side-effect assertions 存在，但
  `promotion_evidence_sufficient_for_enablement=false`。未重跑
  `aits validate-data`，因为本任务只读取 prior validated TRADING-2347 /
  TRADING-2348 artifacts，不读取 fresh market data、不 append event、不绑定
  outcome。验证通过 Ruff、compileall、focused parallel pytest 15 passed、真实
  CLI run、docs freshness 533 docs PASS、documentation contract 1246 reports
  PASS、task-register consistency run/validate PASS、contract-validation 197
  passed（runtime artifact=
  `outputs/validation_runtime/contract-validation_20260704T191536Z/test_runtime_summary.json`）
  和 `git diff --check`。
