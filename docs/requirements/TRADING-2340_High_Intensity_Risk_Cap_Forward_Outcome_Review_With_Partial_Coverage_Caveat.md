# TRADING-2340 High-Intensity Risk-Cap Forward Outcome Review With Partial Coverage Caveat

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2339 已确认 high-intensity partial outcome readiness 可以进入 2340：`status=READY_FOR_2340_FORWARD_OUTCOME_REVIEW_WITH_PARTIAL_COVERAGE_CAVEAT`、coverage=`231/240`、not_due=`9`、blocked=`0`、critical_clusters_with_not_due=`0`。TRADING-2337 已完成 actual-path outcome binding，source data validation 为 `2026-06-29` / `PASS_WITH_WARNINGS` / error_count=`0`。

TRADING-2340 的目标是在 partial coverage caveat 明确存在的前提下，基于 60 个 high-intensity clusters 的实际后续路径，评估 warning 后是否捕捉 downside stress、false warning 是否偏多、missed upside 是否明显、manual-review-only context 是否有 proxy value，并给出 TRADING-2341 continue / refine / manual-review-only / wait / archive route。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-forward-outcome-review`。
2. Fail-closed 读取 TRADING-2339 partial readiness summary / route / coverage artifacts / safety boundary。
3. Fail-closed 读取 TRADING-2337 event-level / cluster-level actual-path outcome matrix、false warning、missed upside、downside capture、manual-review usefulness、data quality 和 safety artifacts。
4. 读取 TRADING-2336 event logger lineage 与 monthly concentration warning。
5. 读取 TRADING-2335 selected `COMPOSITE_HIGH_INTENSITY_RULE` context 与 TRADING-2334 stop / continue / archive / manual-review boundary。
6. 以 cluster-level 为主分析单位生成 cluster outcome review matrix，trigger-day-level 只作为 context。
7. 生成 horizon outcome review、false warning / missed upside / downside capture / manual-review usefulness / rebound-stress reviews、partial coverage caveat、monthly concentration effect、cluster weighted evidence、selected rule outcome assessment、continue/refine/archive decision matrix、2341 readiness / route、interpretation boundary 和 safety boundary。
8. 输出 research docs，并更新 report registry、artifact catalog、system flow 和 task register。

## 边界

- 不重新绑定 actual-path outcome。
- 不重新读取 market data。
- 不重新选择 threshold。
- 不重新执行 exposure-cap dry-run。
- 不修改 TRADING-2336 event log 或 TRADING-2337 outcome matrix。
- 不生成新的 observe event。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不进入 paper-shadow、production 或 broker action。
- 不判断 high-intensity risk-cap 已经可以实盘使用。

## Data Validation Policy

TRADING-2340 默认只读取 prior validated TRADING-2337 / TRADING-2339 artifacts，不直接消费 cached market data，因此默认不重跑 `aits validate-data`。必须读取并披露 TRADING-2337 的 source data validation 信息：

```text
source_validate_data_executed=true
source_validate_data_as_of=2026-06-29
source_validate_data_status=PASS_WITH_WARNINGS
source_validate_data_error_count=0
```

如果实现中重新读取 market data，则必须运行 `aits validate-data --as-of 2026-06-29`，且不得放宽 data-quality rule。

## 验收标准

- CLI 可运行并生成附件要求的 runtime artifacts 和 research docs。
- 缺少 required TRADING-2339 / 2337 artifacts、2339 route 不是 2340 forward outcome review with partial coverage caveat、2339 coverage blocked、critical not-due cluster 非零、2337 data quality fail、original event log mutated，或任何 input artifact 打开 promotion / paper-shadow / production / broker 时 fail closed。
- Cluster outcome review matrix 覆盖每个 cluster 的 1d / 5d / 10d / 20d status、returns、drawdown、stress、rebound、false warning、missed upside、downside capture、manual-review proxy、evidence label 和 cluster weight。
- Horizon outcome review matrix 正确披露 1d / 5d / 10d / 20d 的 coverage、downside capture、false warning、missed upside、rebound、stress、return / drawdown context 和 evidence label。
- False warning、missed upside、downside capture、manual-review usefulness、rebound-stress、partial coverage caveat 和 monthly concentration effect reports 均生成并保留 partial coverage caveat。
- Continue/refine/archive decision matrix 与 2341 route 只允许进入 continue observe、threshold refinement、manual-review-only continuation、wait full 20d coverage、archive 或 data remediation。
- 所有 outputs 固定 `outcome_binding_executed=false`、`original_event_log_mutated=false`、`runtime_scheduler_enabled=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2340 focused parallel pytest files
- 真实 2340 CLI run
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier because CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批只做 forward outcome review 和 2341 routing；当前 worktree 有两个既有无关 research 文档未提交改动，本任务必须 selective staging，不能混入无关改动。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-forward-outcome-review`，真实 run status=`FORWARD_OUTCOME_REVIEW_COMPLETE_WITH_PARTIAL_COVERAGE_CAVEAT`，source validation=`2026-06-29` / `PASS_WITH_WARNINGS` / error_count=0，coverage=`231/240`，not_due=`9`，critical_clusters_with_not_due=`0`，false warning=`0.383333` / `FALSE_WARNING_MODERATE`，missed upside=`0.466667` / `MISSED_UPSIDE_MODERATE`，downside capture=`0.35` / `DOWNSIDE_CAPTURE_MODERATE`，manual-review proxy=`0.583333` / `MANUAL_REVIEW_CONTEXT_USEFUL_PROXY`，monthly concentration effect=`CONCENTRATION_MODERATE_IMPACT`，partial coverage caveat=`PARTIAL_COVERAGE_LOW_IMPACT`，overall recommendation=`CONTINUE_HIGH_INTENSITY_FORWARD_OBSERVE`，next task=`TRADING-2341_High_Intensity_Risk_Cap_Continue_Forward_Observe_Decision`。本任务未重跑 `aits validate-data`，因为只读取 prior validated TRADING-2337 / TRADING-2339 artifacts，不直接读取 cached market data；所有 promotion / paper-shadow / production / broker gates 仍关闭，未重新绑定 outcome。
- 2026-07-04：验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 17 passed、真实 CLI run、docs freshness、documentation contract、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4213 passed / 643 warnings 和 `git diff --check`；full runtime artifact=`outputs/validation_runtime/full_20260704T074410Z/test_runtime_summary.json`。
