# TRADING-736 Data Source Remediation Execution
最后更新：2026-06-23

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only remediation execution reporting
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-735 已输出 source qualification matrix 和 remediation plan，但这些 artifacts
仍只是计划层。TRADING-736 负责读取该计划，按 P0、blocked、current-view 风险排序，
对每个 remediation item 输出最小可执行修复记录和 updated qualification matrix。

本阶段不抓取新数据、不补造 source manifest、不把无法证明 as-of availability 的
source 升级为 promotion-ready。可执行的最小修复只包括 usage isolation、contract
标记、diagnostic-only 边界和 updated qualification reporting。

## 安全边界

- validation-only / observe-only。
- `production_effect=none`。
- `broker_action=none`。
- `promotion_gate_allowed=false`。
- `paper_shadow_change_allowed=false`。
- `production_weight_change_allowed=false`。
- 不修改 production、paper-shadow、official weights。
- 不触发 broker、order、live trading、paper-shadow activation。
- 不放宽 PIT、data-quality、lineage gate。
- 无法证明 as-of availability 时继续 `BLOCKED_UNTIL_QUALIFIED`、`CURRENT_VIEW_ONLY`
  或 `DIAGNOSTIC_ONLY`。

## 输入

- `outputs/data_quality/data_foundation_acceptance/data_foundation_acceptance_report.json`
- `outputs/data_quality/data_source_qualification/data_source_qualification_matrix.json`
- `outputs/data_quality/data_source_qualification/data_foundation_remediation_plan.json`
- `outputs/data_quality/data_source_qualification/data_foundation_acceptance_summary_updated.json`

## 输出

- `outputs/data_quality/data_source_remediation_execution/data_source_remediation_execution_report.json/md`
- `outputs/data_quality/data_source_remediation_execution/data_source_remediation_item_results.json/md`
- `outputs/data_quality/data_source_remediation_execution/data_source_qualification_matrix_updated.json/md`

## Remediation Result 必填字段

- `before_status`
- `after_status`
- `blocked_reason`
- `fix_applied`
- `remaining_gap`
- `repairable_without_relaxing_gate`
- `promotion_grade_candidate_after_fix`
- `allowed_uses`

## Current-View 与 Research-Label 规则

- `CURRENT_VIEW_ONLY` 无 as-of snapshot 时保持 `CURRENT_VIEW_ONLY` 或 `DIAGNOSTIC_ONLY`。
- `CURRENT_VIEW_ONLY` 不允许进入 promotion evidence，不允许作为 strategy input。
- `RESEARCH_LABEL_ONLY` 必须 `strategy_input_allowed=false`、
  `promotion_gate_allowed=false`，`allowed_uses=[analysis, casebook, stratified_reporting]`。

## 进度记录

- 2026-06-21：任务登记进入 IN_PROGRESS；固定只读 remediation execution 和 no gate
  relaxation 范围。
- 2026-06-21：实现 `aits data source-qualification execute-remediation`，读取
  TRADING-735 artifacts、重新运行 data foundation acceptance，并输出 per-item
  remediation results 与 updated qualification matrix；默认 run 保持
  `BLOCKED_UNTIL_QUALIFIED`、P0_remaining_count=9、P0_resolved_count=0、
  current_view_only_isolated_count=1、research_label_only_restricted_count=1、
  `lookahead_violation_count=0`，所有安全字段保持 none/false。
- 2026-06-21：新增 report registry entry、artifact catalog 行、system flow 行、
  schema 和 focused tests；compileall、Ruff、Black check、focused pytest 37 passed、
  fast-unit 94 passed、contract-validation 93 passed、report-validation 55 passed。
