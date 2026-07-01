# TRADING-2319 Event Calendar Gating Generator POC

最后更新：2026-07-01

## 状态

`VALIDATING`

## 背景

TRADING-2318 已完成 event calendar data feasibility audit，真实 run 结果为：

```text
status=EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY
pit_ready_source_count=0
source_audit_required_count=8
source_blocked_count=4
gating_use_case_count=4
```

因此 TRADING-2319 的最佳实现路径当前被阻断：可执行 generator POC 需要
provider-specific event rows、source manifest、known-at / available-at timestamp、
row count、checksum 和 schema validation。仓库当前只有 source feasibility package，
没有 PIT-safe event rows。

本任务不得用当前日期日历、current-view vendor calendar、手工假 rows 或 ETF basic
event flag 替代 PIT-safe source manifest。当前允许实现的是 source-blocked generator
POC 包：把 blocker、inactive spec、use-case readiness 和 safety boundary 审计化，
明确不会生成 event gating signal 或 signal series。

## 目标

新增 CLI：

```bash
aits research trends event-calendar-gating-generator-poc
```

读取 TRADING-2318 artifacts，输出 source-blocked generator POC package。该包说明
`pre_event_no_add`、`post_event_confirmation_window`、`manual_review_trigger` 和
`earnings_cluster_risk` 需要哪些 source 条件，为什么当前不能生成 executable signal，
以及后续解除 blocker 的条件。

## 输入

默认读取：

```text
config/research/event_calendar_gating_generator_policy.yaml
outputs/research_trends/event_calendar_data_feasibility_audit/
```

必须验证 TRADING-2318 source summary、source inventory、gating use-case matrix 和
manual review trigger contract。若 source status、safety flags 或 PIT-ready condition
不符合 policy，必须 fail closed。

## 产物

- `event_calendar_gating_generator_summary.json`
- `event_gating_signal_spec.json`
- `event_gating_use_case_readiness_matrix.json`
- `event_gating_use_case_readiness_matrix.csv`
- `event_gating_source_blocker_report.json`
- `event_gating_source_blocker_report.csv`
- `event_gating_manual_review_trigger_contract.json`
- `event_gating_generator_validation_summary.json`
- `event_gating_generator_safety_boundary.json`
- `docs/research/event_calendar_gating_generator_poc.md`

## 实施边界

1. Source-blocked generator POC。
   - CLI 和 package 可以存在。
   - Event rows 不得下载、不得读取、不得写入 cache。
   - Event gating signal / signal series 不得生成。

2. Inactive signal spec。
   - Spec 只能描述 future executable generator 的 required fields、known-at policy、
     validation gate 和 blocked actions。
   - Spec status 必须是 source-blocked / inactive，不得被解读为 ready-to-run signal。

3. Gating use cases。
   - 覆盖 `pre_event_no_add`、`post_event_confirmation_window`、
     `manual_review_trigger`、`earnings_cluster_risk`。
   - 每个 use case 当前必须标记 `SOURCE_BLOCKED_NO_GENERATOR`。

4. 安全边界。
   - `source_blocked_no_generation=true`
   - `event_rows_consumed=false`
   - `event_rows_downloaded=false`
   - `event_calendar_cache_written=false`
   - `gating_signal_generated=false`
   - `event_gating_signal_series_generated=false`
   - `event_outcome_prediction_allowed=false`
   - `trading_direction_prediction_allowed=false`
   - `promotion_allowed=false`
   - `paper_shadow_allowed=false`
   - `production_allowed=false`
   - `broker_action=none`

## 验收标准

- CLI implemented: `aits research trends event-calendar-gating-generator-poc`。
- Summary 披露 selected market regime=`ai_after_chatgpt`、TRADING-2318 source status、
  PIT-ready source count、blocked use-case count、data quality status 和 safety flags。
- Use-case readiness matrix 覆盖 owner roadmap 的四个用途，并全部 source-blocked。
- Source blocker report 覆盖 TRADING-2318 的 8 个 event source families。
- Inactive signal spec 不包含 event rows、不生成 executable signal、不预测事件结果或交易方向。
- `docs/system_flow.md`、`docs/artifact_catalog.md` 和 `config/report_registry.yaml`
  同步登记 2319 source-blocked generator flow。

## 进展记录

- 2026-07-01: 根据 owner roadmap 和 TRADING-2318 recommended next task 新增并进入
  `IN_PROGRESS`。当前 worktree 有两个无关 research 文档未提交改动，本任务必须
  selective staging，不能混入无关改动。
- 2026-07-01: 实现 source-blocked generator POC 并真实运行 CLI。真实 run 输出
  status=`EVENT_CALENDAR_GATING_GENERATOR_POC_SOURCE_BLOCKED_NO_SIGNAL`，
  data_quality_status=`NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_GENERATOR`，
  source_status=`EVENT_CALENDAR_FEASIBILITY_AUDIT_READY_SOURCE_AUDIT_ONLY`，
  source_pit_ready_source_count=0，source_blocker_count=8，
  use_case_readiness_count=4，blocked_use_case_count=4，
  validation_status=`PASS_SOURCE_BLOCKED_EXPECTED`。四个 owner roadmap use cases
  均为 `SOURCE_BLOCKED_NO_GENERATOR`；未读取或下载 event rows，未写 event cache，
  未生成 gating signal 或 signal series，未预测事件结果或交易方向，promotion /
  paper-shadow / production / broker 均保持 false/none。验证通过 Ruff、compileall、
  focused 2319 parallel pytest 8 passed、adjacent event/regime focused parallel pytest
  21 passed、docs/registry focused parallel pytest 27 passed、真实 CLI run、
  contract-validation 193 passed（runtime artifact:
  `outputs/validation_runtime/contract-validation_20260701T072144Z/test_runtime_summary.json`）
  和 `git diff --check`。`aits validate-data` 不适用，因为本命令只读取 TRADING-2318
  static artifacts，不读取 cached market data 或 event rows；后续任何读取 event rows、
  features、prices、scoring、backtest 或 reports 的 workflow 仍必须执行 provider source
  schema gate 和 `aits validate-data` / 同源 validation code path。
