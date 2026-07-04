# TRADING-2337 High-Intensity Risk-Cap Actual Path Outcome Binder

最后更新：2026-07-04

## 状态

`DONE`

## 背景

TRADING-2336 已完成 high-intensity risk-cap observe-only event logger，真实 run 生成 168 个 trigger days、60 个 de-duplicated observe events 和 60 个 clusters。TRADING-2336 只建立 pending outcome registry 和 outcome collection schedule，没有绑定 future actual-path outcome。

TRADING-2337 的目标是读取 TRADING-2336 event log、cluster registry、pending outcome registry、outcome schedule，以及 canonical price cache，为每个 observe event 的 `1d / 5d / 10d / 20d` horizon 绑定 actual-path outcome。TRADING-2338 已被 B2 data-quality as-of 修复占用，因此本任务完成后的 route 使用 TRADING-2339。

## 实施范围

1. 新增 CLI `aits research trends high-intensity-risk-cap-actual-path-outcome-binder`。
2. Fail-closed 读取 TRADING-2336 summary / event log / cluster registry / pending outcome registry / outcome schedule / 2337 route / safety boundary。
3. 读取 TRADING-2335 selected rule context 和 TRADING-2334 actual-path outcome contract / false-warning framework。
4. 读取 canonical market price cache，并按实际 latest price cache date 执行 `aits validate-data` 同源数据质量门。
5. 生成 event-level actual-path outcome matrix。
6. 生成 cluster-level actual-path outcome matrix，并作为后续 TRADING-2339 主分析单位。
7. 生成 trigger-day actual-path context，但明确排除出主统计样本。
8. 生成 rebound / stress / false-warning / missed-upside / downside-capture candidate classification。
9. 生成 manual-review usefulness proxy。
10. 生成 outcome coverage、horizon quality、actual-path data quality、interpretation boundary、2339 readiness / route 和 safety boundary。

## 边界

- 不生成新的 observe event。
- 不修改 TRADING-2336 原始 event log、cluster registry 或 pending outcome registry。
- 不重新选择 threshold。
- 不重新执行 dynamic dry-run 或 exposure-cap simulation。
- 不根据 outcome 反向修改 trigger rule、event status 或 risk-cap policy。
- 不生成 target exposure、target weight、rebalance instruction、buy / sell signal、reduce position instruction、paper-shadow-ready、production-ready 或 broker-ready artifact。
- 不读取真实券商账户或真实持仓。
- 不进入 paper-shadow、production 或 broker action。
- 不证明 high-intensity risk-cap 可以实盘使用。

## Data Quality Policy

TRADING-2337 读取 cached market price data 并绑定 actual-path outcome，因此必须执行 cached data quality gate。默认以 price cache latest available date 作为 `validate_data_as_of`，当前预期为 `2026-06-29`，并在 summary / data quality report / docs 中披露：

```text
validate_data_executed: true
validate_data_as_of: <actual_as_of>
validate_data_status: PASS or PASS_WITH_WARNINGS
validate_data_error_count: 0
```

不得因为当前 wall-clock date 晚于本地 cache latest date 而放宽 freshness、schema、checksum 或 completeness 规则。

## Cluster Weighting Policy

本任务必须生成 `high_intensity_cluster_weighting_policy.json`，并固定：

```text
primary_analysis_level=cluster
secondary_analysis_level=event
trigger_day_level_usage=context_only
original_event_log_mutated=false
outcome_binding_derivative_only=true
future_outcome_used_for_event_creation=false
future_outcome_used_for_rule_selection=false
promotion_allowed=false
paper_shadow_allowed=false
production_allowed=false
broker_action=none
```

该策略用于避免同一 risk episode 的连续触发污染 false warning、missed upside、downside capture 和 manual-review usefulness 统计。

## 验收标准

- CLI 可运行并生成附件要求的 runtime artifacts 和 research docs。
- 缺少 required TRADING-2336 artifacts、2336 route 不是 outcome binder、或输入 artifact 打开 promotion / paper-shadow / production / broker 时 fail closed。
- Event-level matrix 覆盖 `1d / 5d / 10d / 20d` outcome slots。
- Cluster-level matrix 以 `primary_event_id / cluster_start_date` 为起点，且作为主分析单位。
- Trigger-day context 固定 `context_only=true`、`excluded_from_primary_sample=true`。
- Classification thresholds 显式写入 policy，且标记为 research candidate，不是 production threshold。
- Coverage / data quality report 披露 latest market data date、missing data、duplicate outcome id、安全字段和 validate-data status。
- 2339 readiness checklist 和 route 只允许 forward outcome review、partial readiness review、outcome data remediation、clustering remediation 或 archive。
- 所有 outputs 固定 `outcome_binding_executed=true`、`original_event_log_mutated=false`、`runtime_scheduler_enabled=false`、`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- TRADING-2337 focused parallel pytest files
- 真实 2337 CLI run
- `aits validate-data --as-of <actual_price_cache_latest_date>`
- docs freshness
- documentation contract
- task-register consistency run / validate
- contract-validation tier
- full validation tier if CLI / registry / docs contract surface changes
- `git diff --check`

## 进展记录

- 2026-07-04：根据 owner 附件新增并进入 `IN_PROGRESS`。本批承接 TRADING-2336 observe-only event logger 输出，重点是把 future actual-path evidence pipeline 打通；成功标准不是证明 high-intensity risk-cap 有效，而是让 60 个 de-duplicated events / clusters 的后续 actual path 可审计、可聚合、可进入 TRADING-2339 review。
- 2026-07-04：实现完成并进入 `VALIDATING`。新增 `aits research trends high-intensity-risk-cap-actual-path-outcome-binder`，真实 run status=`PARTIAL_OUTCOME_BINDING_WITH_NOT_DUE_HORIZONS`，data_quality_status=`PASS_WITH_WARNINGS`，`validate_data_as_of=2026-06-29`，`validate_data_error_count=0`，event_count=`60`，cluster_count=`60`，pending_outcome_count=`240`，event_actual_path_outcome_count=`240`，cluster_actual_path_outcome_count=`240`，cluster_outcome_bound_count=`231`，coverage_status=`PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS`，readiness_status=`PARTIAL_OUTCOME_REVIEW_REQUIRED`，next_task=`TRADING-2339_High_Intensity_Risk_Cap_Partial_Outcome_Readiness_Review`。Partial status 来自尾部实际交易日 horizon 尚未成熟，不是 data-quality failure；所有 promotion / paper-shadow / production / broker gates 仍关闭，原始 TRADING-2336 event log 不被修改。
- 2026-07-04：验证通过并归档 `DONE`。验证覆盖 Ruff、compileall、focused parallel pytest 17 passed、真实 CLI run、`aits validate-data --as-of 2026-06-29` PASS_WITH_WARNINGS / error_count=0、docs freshness、documentation contract、task-register consistency run / validate、contract-validation 193 passed、full parallel pytest 4178 passed / 643 warnings 和 `git diff --check`；full runtime artifact=`outputs/validation_runtime/full_20260704T061120Z/test_runtime_summary.json`。
