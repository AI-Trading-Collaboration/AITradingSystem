# TRADING-2342 High-Intensity Risk-Cap Observe-Only Runtime Integration Plan

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2341 已把 high-intensity risk-cap continuation decision 固化为 `CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT`，selected rule 继续使用 `COMPOSITE_HIGH_INTENSITY_RULE`，readiness 为 `READY_FOR_2342_WITH_CAVEATS`，next task 为 `TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan`。promotion、paper-shadow、production 和 broker action 均继续关闭。

TRADING-2342 的目标是把 historical research artifact 推进到 observe-only runtime integration plan，明确未来 runtime dry-run 可如何做 event detection、append-only event log、cluster update、pending outcome registry update、outcome update job、monthly concentration monitoring 和 manual-review context display boundary。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-observe-only-runtime-integration-plan`。
2. Fail-closed 读取 TRADING-2341 continuation decision outputs，并确认 2341 route、selected rule、readiness 和 safety boundary。
3. 读取 TRADING-2340 / 2339 / 2337 / 2336 / 2335 / 2334 lineage 和 contracts，继承 partial coverage caveat、monthly concentration warning、manual-review-only boundary 和 source validation context。
4. 生成 runtime scope / input / event detection / event append / cluster update / pending outcome update / outcome update job / manual-review context / monthly concentration monitoring / artifact path registry / report registry update / fail-closed safety gate / observe-only dry-run / risk register / 2343 readiness and route / interpretation boundary / safety boundary artifacts。
5. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不启用 runtime scheduler。
- 不接入每日自动任务。
- 不生成新的 observe event。
- 不 append runtime event log。
- 不更新 pending outcome registry。
- 不绑定新的 outcome。
- 不重新读取 market data。
- 不重新选择 threshold 或修改 selected trigger rule。
- 不重新执行 exposure-cap dry-run。
- 不读取真实券商账户或真实持仓。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不进入 paper-shadow、production 或 broker action。
- 不把 manual-review context 解释为减仓建议。

## Data Validation Policy

TRADING-2342 默认只读取 prior validated TRADING-2341 / 2340 / 2337 artifacts，不直接消费 cached market data，因此默认不重跑 `aits validate-data`。必须读取并披露 TRADING-2337 的 source data validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 future runtime 的 data-quality requirement。

## 验收标准

- CLI 可运行并生成附件要求的 runtime integration plan artifacts 和 research docs。
- 缺少 required TRADING-2341 / 2340 / 2337 / 2336 / 2335 / 2334 artifacts、2341 decision 不是 `CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT`、2341 route 不是 observe-only runtime integration plan、selected rule contract 不完整，或任何 input artifact 打开 promotion / paper-shadow / production / broker / target weight / rebalance 时 fail closed。
- Runtime scope contract 固定 `observe_line=high_intensity_risk_cap`、`runtime_mode=observe_only`、`runtime_integration_plan_only=true`、`runtime_scheduler_enabled=false`，并保留 partial coverage caveat 和 monthly concentration monitoring requirement。
- Event detection / append / cluster update / pending outcome update / outcome update job contracts 明确 append-only、dedup、deterministic ids、1d / 5d / 10d / 20d horizons、known-at / PIT policy 和 blocked trading outputs。
- Fail-closed safety gate 明确 missing selected rule、missing known-at timestamp、missing PIT policy、target weight、rebalance、paper-shadow、production 和 broker action 全部阻断。
- 2343 route 只允许进入 observe-only runtime dry-run、runtime prerequisite remediation、outcome update job plan 或 archive runtime line。
- 所有 outputs 固定 `runtime_scheduler_enabled=false`、`new_event_logging_executed=false`、`outcome_binding_executed=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2342 focused parallel pytest files
- 真实 2342 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier because CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2341 `READY_FOR_2342_WITH_CAVEATS` / `TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan` route，只做 observe-only runtime integration plan；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-observe-only-runtime-integration-plan`，真实 run status=`OBSERVE_ONLY_RUNTIME_INTEGRATION_PLAN_READY_WITH_CAVEATS_PROMOTION_BLOCKED`，selected_rule=`COMPOSITE_HIGH_INTENSITY_RULE` 且 hash preserved，event logger trigger days=`168`、clusters=`60`，coverage=`231/240`，not_due=`9`，readiness=`READY_FOR_2343_WITH_CAVEATS`，next task=`TRADING-2343_High_Intensity_Risk_Cap_Observe_Only_Runtime_Dry_Run`，route caveats=`PIT_APPROXIMATION_CAVEAT` / `MONTHLY_CONCENTRATION_MONITORING_REQUIRED` / `PARTIAL_COVERAGE_CAVEAT` / `OBSERVE_ONLY_NO_PAPER_SHADOW`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2341 / 2340 / 2337 artifacts，不直接读取 cached market data；所有 promotion / paper-shadow / production / broker gates 仍关闭，未启动 scheduler、未生成 event、未 append runtime log、未更新 pending outcome registry、未绑定 outcome。
- 2026-07-04：完整验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 19 passed、真实 CLI run、docs freshness 521 docs PASS、documentation contract 1239 reports PASS、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4246 passed / 643 warnings 和 `git diff --check`；contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260704T101518Z/test_runtime_summary.json`，full runtime artifact=`outputs/validation_runtime/full_20260704T101901Z/test_runtime_summary.json`。最终 route 进入 TRADING-2343 observe-only runtime dry-run with caveats；这仍不是 scheduler enabled、paper-shadow、production 或 broker readiness。
