# TRADING-2344 High-Intensity Risk-Cap Observe-Only Runtime Scheduler Integration Plan

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2343 已完成 high-intensity risk-cap observe-only runtime dry-run，真实 run 为 `OBSERVE_ONLY_RUNTIME_DRY_RUN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，`record_count=2490`，`detected_event_count=168`，`would_append_event_count=0`，`2344_readiness=READY_FOR_2344_WITH_CAVEATS`，next task 为 `TRADING-2344_High_Intensity_Risk_Cap_Observe_Only_Runtime_Scheduler_Integration_Plan`。

`would_append_event_count=0` 不是失败；它来自 historical replay 命中既有 TRADING-2336 trigger-day / event / cluster registry 去重。2343 已证明 `COMPOSITE_HIGH_INTENSITY_RULE` 可以稳定检测 high-intensity trigger，且 append-only / de-dup contract 可以避免重复污染 event log。

TRADING-2344 的目标是设计 observe-only scheduler integration plan。它仍然不能启用 scheduler、不能执行真实每日任务、不能生成新 event、不能绑定新 outcome、不能进入 paper-shadow / production / broker action。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-observe-only-runtime-scheduler-integration-plan`。
2. Fail-closed 读取 TRADING-2343 runtime dry-run outputs，并确认 2343 route 允许进入 scheduler integration plan。
3. 读取 TRADING-2342 runtime contracts、TRADING-2341 continuation decision、TRADING-2335 selected rule lineage 和 TRADING-2336 event logger lineage。
4. 生成 scheduler scope / cadence / input contracts。
5. 生成 event detection、event append、cluster update、pending outcome update、actual-path outcome update、manual-review context 和 monthly concentration monitoring job contracts。
6. 生成 artifact path / registry / retention plan、disabled-by-default policy、dry-run execution plan、failure mode matrix、integration risk register、fail-closed scheduler safety gate、2345 readiness / route、interpretation boundary 和 safety boundary。
7. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不启用真实 scheduler。
- 不写入每日自动任务或 enabled scheduler config。
- 不修改 existing production scheduler。
- 不生成新的 observe event。
- 不 append event log。
- 不更新 cluster registry。
- 不更新 pending outcome registry。
- 不绑定新的 actual-path outcome。
- 不重新读取 market data。
- 不重新选择 threshold 或修改 selected trigger rule。
- 不重新执行 exposure-cap dry-run。
- 不读取真实券商账户或真实持仓。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不进入 paper-shadow、production 或 broker action。
- 不把 manual-review context 解释为减仓建议。

## Data Validation Policy

TRADING-2344 默认只读取 prior validated TRADING-2343 / 2342 research artifacts，不直接消费 fresh market data，不生成新事件，不绑定 outcome，不启用 scheduler，因此默认不重跑 `aits validate-data`。必须读取并披露 source validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data 或 runtime signal source，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 future runtime 的 data-quality requirement。

## 验收标准

- CLI 可运行并生成附件要求的 scheduler integration plan artifacts 和 research docs。
- 缺少 required TRADING-2343 / 2342 / 2341 / 2335 / 2336 artifacts、2343 route 不是 observe-only scheduler integration plan、2343 safety gate fail、2343 contract validation fail、selected rule 不是 `COMPOSITE_HIGH_INTENSITY_RULE`，或任何 input artifact 打开 promotion / paper-shadow / production / broker / target weight / rebalance 时 fail closed。
- Scheduler scope contract 固定 `scheduler_enabled=false`、`scheduler_default_enabled=false`、`observe_only=true`、`scheduler_integration_plan_only=true`。
- Cadence plan 只定义候选 trading-day 时序、market-calendar gating 和 timezone policy，不实际启用。
- Input contract 覆盖 selected rule、trigger series、known-at/PIT policy、trading calendar、prior event / cluster / pending registry 和 monthly concentration state，并阻断 broker/live portfolio inputs。
- Job contracts 覆盖 event detection、append-only event append、cluster update、pending outcome update、future outcome update、manual-review context 和 monthly concentration monitoring。
- Outcome update job contract 必须声明 future job 需要 market data 和 canonical validate-data，但 2344 不启用该 job。
- Disabled-by-default policy 明确 activation 需要 future task、owner review、dry-run pass、safety gate pass，并且 2344 不能激活。
- Failure mode matrix 覆盖 missing selected rule、missing trigger series、missing known-at timestamp、missing PIT policy、duplicate event id、cluster conflict、monthly concentration breach、forbidden manual-review field、target weight、rebalance、paper-shadow、production 和 broker action。
- 2345 route 只允许进入 observe-only scheduler dry-run、scheduler plan remediation、scheduler safety remediation 或 archive runtime line。
- 所有 outputs 固定 `scheduler_enabled=false`、`scheduler_default_enabled=false`、`event_append_executed=false`、`outcome_binding_executed=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2344 focused parallel pytest files
- 真实 2344 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier because CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2343 `READY_FOR_2344_WITH_CAVEATS` / observe-only scheduler integration plan route，只做 scheduler integration planning；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 CLI / loader / builder / writer、scheduler scope/cadence/input contracts、event detection / append / cluster / pending outcome / outcome update / manual-review / monthly monitoring job contracts、fail-closed safety gate、disabled-by-default policy、dry-run execution plan、failure mode matrix、integration risk register、2345 readiness / route、research docs、report registry、artifact catalog、system flow 和 focused tests。真实 run status=`OBSERVE_ONLY_SCHEDULER_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，readiness=`READY_FOR_2345_WITH_CAVEATS`，next_task=`TRADING-2345_High_Intensity_Risk_Cap_Observe_Only_Scheduler_Dry_Run`。
- 2026-07-04：完整验证通过并归档 `DONE`。验证通过 Ruff、compileall、focused parallel pytest 32 passed、真实 CLI run、docs freshness 523 docs PASS、documentation contract 1241 reports PASS、task-register consistency run/validate PASS、contract-validation 193 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260704T123210Z/test_runtime_summary.json`）、full parallel pytest 4312 passed / 643 warnings（runtime artifact=`outputs/validation_runtime/full_20260704T123527Z/test_runtime_summary.json`）和 `git diff --check`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2343 / 2342 artifacts，不直接读取 cached market data 或绑定 outcome；source validation 继承 `2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0。
