# TRADING-2320 Event Gating Validation

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2318 已完成 event calendar source feasibility audit，真实 run 显示
`pit_ready_source_count=0`、`source_audit_required_count=8`。TRADING-2319 已实现
source-blocked event calendar gating generator POC，真实 run status 为：

```text
EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL
```

2319 没有下载或读取 event rows，没有生成 executable gating signal，也没有生成
event gating signal series。因此 TRADING-2320 的最佳实现路径当前被阻断：真实
event gating validation 需要 PIT-safe event rows、known_at / available_at timestamp、
executable gating signal series、risk-on / add records、event-window turnover records、
earnings cluster exposure records，以及 cached market data quality gate。

本任务不得用 current-view event calendar、手工假 event rows、静态日期清单或未审计
signal stub 替代真实 validation。当前允许实现的是 source-blocked validation package：
把 validation objectives、required datasets、source blockers、metric contract 和 safety
boundary 审计化，明确不会执行 event gating validation 或输出效果结论。

## 目标

新增 CLI：

```bash
aits research trends event-gating-validation
```

读取 TRADING-2319 source-blocked generator POC artifacts，输出 validation readiness /
source-blocked package。该包覆盖 owner roadmap 中的验证目标：

- `pre_event_false_risk_on`
- `event_window_overtrading`
- `earnings_cluster_exposure_risk`

当前所有目标必须标记为 `SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`。

## 输入

默认读取：

```text
config/research/event_gating_validation_policy.yaml
outputs/research_trends/event_calendar_gating_generator_poc/
```

必须验证 TRADING-2319 summary、inactive signal spec、use-case readiness matrix、
source blocker report、manual review trigger contract 和 safety boundary。若 2319 已经
不再是 source-blocked/no-signal package，应 fail closed，要求改用可执行 event
validation policy。

## 产物

- `event_gating_validation_summary.json`
- `event_gating_validation_metric_contract.json`
- `event_gating_validation_readiness_matrix.json`
- `event_gating_validation_readiness_matrix.csv`
- `event_gating_validation_blocker_report.json`
- `event_gating_validation_blocker_report.csv`
- `event_gating_validation_data_requirement_matrix.json`
- `event_gating_validation_data_requirement_matrix.csv`
- `event_gating_validation_safety_boundary.json`
- `docs/research/event_gating_validation.md`

## 实施边界

1. Source-blocked validation package。
   - CLI 和 package 可以存在。
   - Event rows、market data、turnover data、portfolio/exposure data 不得读取。
   - Event gating validation 不得执行。
   - Validation result / effect claim 不得生成。

2. Metric contract。
   - 只能描述 future validation 所需字段、分组、comparison window、quality gates 和
     blocked reason。
   - 不得输出 event gating 有效、无效、降低风险或提高收益等结论。

3. Validation objectives。
   - 覆盖 `pre_event_false_risk_on`、`event_window_overtrading`、
     `earnings_cluster_exposure_risk`。
   - 每个 objective 当前必须标记 `SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`。

4. 安全边界。
   - `source_blocked_no_validation=true`
   - `event_rows_consumed=false`
   - `gating_signal_consumed=false`
   - `event_gating_signal_series_consumed=false`
   - `market_data_consumed=false`
   - `turnover_records_consumed=false`
   - `exposure_records_consumed=false`
   - `event_gating_validation_executed=false`
   - `validation_result_generated=false`
   - `event_outcome_prediction_allowed=false`
   - `trading_direction_prediction_allowed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends event-gating-validation`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、TRADING-2319 source status、
  source signal status、objective count、blocked objective count、data quality status 和
  safety flags。
- Readiness matrix 覆盖三个 owner validation objectives，并全部 source-blocked。
- Blocker report 说明真实 validation 缺少 PIT event rows、executable gating signal
  series、event-window outcome/turnover/exposure records。
- Metric contract 不包含 effect result，不读取 market cache，不预测事件结果或交易方向。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2320 source-blocked validation flow。

## 进展记录

- 2026-07-01: 根据 owner roadmap 和 TRADING-2319 source-blocked next step 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。真实 validation 的最佳方案被 TRADING-2318 /
  TRADING-2319 的 source 和 signal blocker 阻断；本批只允许实现 source-blocked
  validation package，不得伪造 event rows、gating signal 或效果验证结论。
- 2026-07-01: 实现 source-blocked validation package 并真实运行 CLI。真实 run 输出
  status=`EVENT_GATING_VALIDATION_SOURCE_BLOCKED_NOT_EXECUTED`，
  data_quality_status=`NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_VALIDATION`，
  source_status=`EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL`，
  source_signal_spec_status=`SOURCE_BLOCKED_INACTIVE_SPEC_ONLY`，
  validation_objective_count=3，blocked_objective_count=3，
  data_requirement_count=15，blocker_count=18，
  validation_status=`PASS_SOURCE_BLOCKED_EXPECTED`。三个 owner validation objectives
  均为 `SOURCE_BLOCKED_VALIDATION_NOT_EXECUTED`；未读取 event rows、market data、
  turnover records 或 exposure records，未执行 event gating validation，未生成
  validation result 或 effect claim，promotion / paper-shadow / production / broker
  均保持 false/none。验证通过 Ruff、compileall、focused 2320 parallel pytest
  8 passed、event-line adjacent focused parallel pytest 23 passed、docs/registry focused
  parallel pytest 27 passed、真实 CLI run、contract-validation 193 passed（runtime
  artifact:
  `outputs/validation_runtime/contract-validation_20260701T073730Z/test_runtime_summary.json`）
  和 `git diff --check`。`aits validate-data` 不适用，因为本命令只读取 TRADING-2319
  static artifacts，不读取 cached market data、event rows、turnover records 或 exposure
  records；后续任何读取 event rows、features、prices、turnover/exposure、scoring、
  backtest 或 reports 的 workflow 仍必须执行 provider source schema gate 和
  `aits validate-data` / 同源 validation code path。
