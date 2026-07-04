# TRADING-2341 High-Intensity Risk-Cap Continue Forward Observe Decision

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2340 已完成 high-intensity risk-cap forward outcome review with partial coverage caveat，真实结论为 `overall_recommendation=CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE`，`next_task=TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision`。该结论仍是 research-only / observe-only，不允许 paper-shadow、production、broker action 或任何 target weight / rebalance instruction。

TRADING-2341 的目标是把 2340 的 continue observe 结论固化为正式 continuation decision，明确 selected `COMPOSITE_HIGH_INTENSITY_RULE` 是否继续沿用、partial coverage caveat 是否继续带入、monthly concentration warning 如何监控、event logger / outcome binder 的后续运行边界，以及是否允许进入 TRADING-2342 observe-only runtime integration plan。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-continue-forward-observe-decision`。
2. Fail-closed 读取 TRADING-2340 forward outcome review outputs，并确认 recommendation / route / safety gate。
3. 读取 TRADING-2339 partial readiness、TRADING-2337 outcome binder data-quality context、TRADING-2336 event logger lineage、TRADING-2335 selected rule context 和 TRADING-2334 stop / continue / archive context。
4. 生成 continuation decision matrix、selected rule continuation contract、observe continuation scope、partial coverage carryforward caveat、monthly concentration monitoring plan、event logger continuation contract、outcome update policy、manual review context policy、stop / refine / archive policy、runtime integration prerequisite checklist、2342 readiness / route、interpretation boundary 和 safety boundary。
5. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不启动 observe runtime。
- 不接入 scheduler。
- 不生成新的 observe event。
- 不重新绑定 actual-path outcome。
- 不重新读取 market data。
- 不重新选择 threshold，不修改 selected trigger rule。
- 不重新执行 exposure-cap dry-run。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不进入 paper-shadow、production 或 broker action。
- 不判断 high-intensity risk-cap 可以实盘使用。

## Data Validation Policy

TRADING-2341 默认只读取 prior validated TRADING-2340 / 2339 / 2337 artifacts，不直接消费 cached market data，因此默认不重跑 `aits validate-data`。必须读取并披露 TRADING-2337 的 source data validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 data-quality rule。

## 验收标准

- CLI 可运行并生成附件要求的 runtime artifacts 和 research docs。
- 缺少 required TRADING-2340 / 2339 / 2337 / 2336 / 2335 / 2334 artifacts、2340 recommendation 不是 `CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE`、2340 route 不是 2341 continue decision，或任何 input artifact 打开 promotion / paper-shadow / production / broker / target weight / rebalance 时 fail closed。
- Continue observe decision matrix 确认 observe-only continuation，并保留 partial coverage caveat 和 monthly concentration monitoring requirement。
- Selected rule continuation contract 保留 selected rule hash，`rule_continued=true`、`rule_changed=false`、`rule_change_allowed_in_2341=false`。
- Observe continuation scope 允许 future observe event logging / outcome registry update / monthly concentration monitoring / manual-review context，但阻断 automatic exposure cap、target weight、rebalance、paper-shadow、production 和 broker。
- 2342 route 只允许进入 observe-only runtime integration plan、runtime prerequisite remediation、wait full 20d coverage、threshold refinement plan 或 archive line。
- 所有 outputs 固定 `runtime_scheduler_enabled=false`、`new_event_logging_executed=false`、`outcome_binding_executed=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2341 focused parallel pytest files
- 真实 2341 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier because CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2340 `CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE` / `TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision` route，只做 continuation decision、observe-only boundary 和 TRADING-2342 route；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-continue-forward-observe-decision`，真实 run status=`CONTINUE_FORWARD_OBSERVE_DECISION_CONFIRMED_WITH_CAVEATS_PROMOTION_BLOCKED`，decision=`CONTINUE_OBSERVE_ONLY_WITH_PARTIAL_COVERAGE_CAVEAT`，selected_rule=`COMPOSITE_HIGH_INTENSITY_RULE` 且 hash preserved，coverage=`231/240`，not_due=`9`，critical_clusters_with_not_due=`0`，monthly warning=`MONTHLY_EVENT_CONCENTRATION_ABOVE_GUARDRAIL`，monitoring=`MONITORING_REQUIRED_WITH_STRICT_GUARDRAILS`，readiness=`READY_FOR_2342_WITH_CAVEATS`，next task=`TRADING-2342_High_Intensity_Risk_Cap_Observe_Only_Runtime_Integration_Plan`，route caveats=`PARTIAL_COVERAGE_CAVEAT` / `MONTHLY_CONCENTRATION_MONITORING_REQUIRED`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2340 / 2339 / 2337 artifacts，不直接读取 cached market data；所有 promotion / paper-shadow / production / broker gates 仍关闭，未启动 runtime、未生成新 event、未重新绑定 outcome。
- 2026-07-04：完整验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 14 passed、真实 CLI run、docs freshness、documentation contract、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4227 passed / 643 warnings 和 `git diff --check`；contract-validation runtime artifact=`outputs/validation_runtime/contract-validation_20260704T082451Z/test_runtime_summary.json`，full runtime artifact=`outputs/validation_runtime/full_20260704T082723Z/test_runtime_summary.json`。最终 route 进入 TRADING-2342 observe-only runtime integration plan with caveats；这仍不是 runtime start、paper-shadow、production 或 broker readiness。
