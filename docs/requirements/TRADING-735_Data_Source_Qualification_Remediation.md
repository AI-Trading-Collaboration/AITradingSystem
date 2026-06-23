# TRADING-735 Data Source Qualification Remediation
最后更新：2026-06-23

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only source qualification remediation planning
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-734 `data_foundation_acceptance_report.json` 输出
`BLOCKED_UNTIL_QUALIFIED_DATA`，且 `promotion_grade_ready_count=0`、
`diagnostic_only_count=2`、`blocked_until_qualified_count=3`、
`lookahead_violation_count=0`。TRADING-735 的目标是把这些 blocked / diagnostic-only
结果拆成 module-level qualification matrix 和 remediation plan，而不是把任何 source
直接升级为合格。

## 安全边界

- validation-only / observe-only。
- `production_effect=none`。
- `broker_action=none`。
- `promotion_gate_allowed=false`。
- `paper_shadow_change_allowed=false`。
- `production_weight_change_allowed=false`。
- `lookahead_violation_count=0` 必须保持。
- 不修改 production、paper-shadow、official weights。
- 不触发 broker、order、live trading、paper-shadow activation。
- 不放宽 PIT、data-quality、lineage gate。
- 无法证明 as-of availability 时继续 `BLOCKED_UNTIL_QUALIFIED` 或 `DIAGNOSTIC_ONLY`。

## 输入

- `outputs/data_quality/data_foundation_acceptance/data_foundation_acceptance_report.json`

## Module-Level Qualification 覆盖

- `pit_feature_store`
- `asset_master`
- `tradable_universe`
- `cost_liquidity_model`
- `regime_event_cluster_labels`
- `run_registry`
- `execution_cache`
- `forward_evidence_archive`
- `research_case_library`

## Matrix 分类

- `PROMOTION_GRADE_READY`
- `DIAGNOSTIC_ONLY`
- `BLOCKED_UNTIL_QUALIFIED`
- `RESEARCH_LABEL_ONLY`
- `CURRENT_VIEW_ONLY`
- `UNKNOWN_REQUIRES_MANUAL_REVIEW`

## Remediation Row 必填字段

- `component`
- `current_status`
- `blocked_reason`
- `missing_contract`
- `missing_source_manifest`
- `missing_available_time`
- `current_view_only_risk`
- `lineage_gap`
- `PIT_risk`
- `repairable_without_relaxing_gate`
- `required_fix`
- `expected_promotion_grade_gain_if_fixed`

## P0 Remediation 优先级

- price / adjusted price / corporate actions。
- SEC / fundamental PIT availability。
- asset master / ticker / tradability。
- event labels as-known-before vs post-hoc。
- cost / spread / liquidity assumptions。

## 输出

- `outputs/data_quality/data_source_qualification/data_source_qualification_matrix.json/md`
- `outputs/data_quality/data_source_qualification/data_foundation_remediation_plan.json/md`
- `outputs/data_quality/data_source_qualification/data_foundation_acceptance_summary_updated.json/md`

## 进度记录

- 2026-06-21：任务登记进入 IN_PROGRESS；固定只读 remediation planning 范围和 no gate relaxation 安全边界。
- 2026-06-21：实现 `aits data source-qualification remediate`，读取 TRADING-734
  acceptance report 并输出 qualification matrix、remediation plan、updated
  acceptance summary；默认 run 结果为 `BLOCKED_UNTIL_QUALIFIED` matrix，覆盖
  9 个 module rows、10 个 remediation items、5 个 P0 source families，
  `lookahead_violation_count=0`，安全边界全部为 none/false。
- 2026-06-21：新增 report registry entry、artifact catalog 行、system flow 行、
  matrix/remediation schema 和 focused tests；compileall、Ruff、Black check、
  focused pytest 38 passed、fast-unit 91 passed、contract-validation 90 passed、
  report-validation 55 passed。
