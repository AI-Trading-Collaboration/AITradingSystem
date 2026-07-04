# TRADING-2339 High-Intensity Risk-Cap Partial Outcome Readiness Review

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2337 已完成 high-intensity risk-cap actual-path outcome binder，真实 run 为 `PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS`。当前 60 个 cluster × 4 个 horizon 共 240 个 cluster outcome slots，其中 231 个已绑定，9 个为 not-due horizons；`validate_data_as_of=2026-06-29`，`validate_data_status=PASS_WITH_WARNINGS`，`validate_data_error_count=0`。

TRADING-2339 的目标是判断这 231/240 的 partial outcome coverage 是否足够进入 TRADING-2340 forward outcome review，或者是否应等待 not-due horizons / 进入数据修复 / 仅做局部 review。本任务不重新绑定 outcome、不重新读取 market data、不评价 high-intensity risk-cap 是否有效。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-partial-outcome-readiness-review`。
2. Fail-closed 读取 TRADING-2337 outcome binder summary、event / cluster outcome matrix、coverage report、data quality report、interpretation boundary、2339 route 和 safety boundary。
3. 读取 TRADING-2336 event logger lineage，确认 event / cluster / pending outcome lineage 和 monthly concentration warning。
4. 读取 TRADING-2335 selected rule context 和 TRADING-2334 false-warning / missed-stress / stop-continue-archive context。
5. 生成 partial outcome coverage matrix、not-due horizon matrix、not-due cluster impact report、asset / horizon distribution、horizon readiness matrix、cluster readiness matrix、sufficiency report、wait-vs-review decision matrix、partial review input contract、interpretation boundary、2340 readiness checklist、2340 task route 和 safety boundary。
6. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不重新绑定 actual-path outcome。
- 不重新读取 market data。
- 不重新计算 forward return / drawdown。
- 不修改 TRADING-2336 event log 或 TRADING-2337 outcome matrix。
- 不重新选择 threshold。
- 不重新执行 exposure-cap dry-run。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不进入 paper-shadow、production 或 broker action。
- 不判断 high-intensity risk-cap 已经有效。

## Data Validation Policy

TRADING-2339 默认只读取 prior validated TRADING-2337 outcome artifacts，不直接消费 cached market data，因此默认不重跑 `aits validate-data`。必须读取并披露 TRADING-2337 的 source data validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 data-quality rule。

## 验收标准

- CLI 可运行并生成附件要求的 runtime artifacts 和 research docs。
- 缺少 required TRADING-2337 artifacts、2337 route 不是 partial outcome readiness review、source data validation 未执行、data quality fail、或任何 input artifact 打开 promotion / paper-shadow / production / broker 时 fail closed。
- Coverage matrix 正确披露 event / cluster × 1d / 5d / 10d / 20d 的 expected / bound / not-due / blocked / coverage ratio。
- Not-due horizon matrix 正确标记 asset、cluster、horizon、due status、criticality 和 impact level。
- Horizon / cluster readiness matrix 能区分 full ready、20d not-due caveat、partial not-due 和 blocked。
- Wait-vs-review decision matrix 和 2340 route 只允许进入 forward outcome review、review with partial caveat、wait、partial-only、data remediation 或 archive。
- 所有 outputs 固定 `outcome_binding_executed=false`、`original_event_log_mutated=false`、`runtime_scheduler_enabled=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2339 focused parallel pytest files
- 真实 2339 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier if CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2337 partial outcome route，只做 readiness review 和 TRADING-2340 next-task routing；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-partial-outcome-readiness-review`，真实 run status=`READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT`，source validation=`2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0，expected_outcome_count=`240`，bound_outcome_count=`231`，not_due_outcome_count=`9`，blocked_outcome_count=`0`，coverage_ratio=`0.9625`，critical_clusters_with_not_due=`0`，not_due_concentration_label=`NOT_DUE_RECENT_20D_CONCENTRATION`，decision=`PROCEED_TO_FORWARD_OUTCOME_REVIEW_WITH_CAVEAT`，next_task=`TRADING-2340_High_Intensity_Risk_Cap_Forward_Outcome_Review_With_Partial_Coverage_Caveat`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2337 outcome artifacts，不直接读取 cached market data；所有 promotion / paper-shadow / production / broker gates 仍关闭，未重新绑定 outcome。
- 2026-07-04：验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 18 passed、真实 CLI run、docs freshness、documentation contract、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4196 passed / 643 warnings 和 `git diff --check`；full runtime artifact=`outputs/validation_runtime/full_20260704T065125Z/test_runtime_summary.json`。
