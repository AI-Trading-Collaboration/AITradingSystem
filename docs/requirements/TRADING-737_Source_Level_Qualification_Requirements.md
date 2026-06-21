# TRADING-737 Source-Level Qualification Requirements

## 状态

- 状态：VALIDATING
- 日期：2026-06-21
- 范围：validation-only / observe-only source-level requirement matrix
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-736 updated matrix 仍显示 `P0_remaining_count=9`、`P0_resolved_count=0`。
TRADING-737 不修复或升级数据源，而是把每个 P0 remaining item 拆成 source-level
qualification requirement，明确缺少的 proof、raw source、timestamp fields、source
manifest、as-of snapshot 和 corporate action / revision policy。

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
- 不尝试升级任何数据源状态。

## 输入

- `outputs/data_quality/data_source_remediation_execution/data_source_remediation_execution_report.json`
- `outputs/data_quality/data_source_remediation_execution/data_source_remediation_item_results.json`
- `outputs/data_quality/data_source_remediation_execution/data_source_qualification_matrix_updated.json`

## 输出

- `outputs/data_quality/data_source_requirements/data_source_requirement_matrix.json/md`

## 每项 Requirement 字段

- `component`
- `current_status`
- `missing_proof`
- `required_raw_source`
- `required_timestamp_fields`
- `required_source_manifest`
- `required_as_of_snapshot`
- `required_corporate_action_revision_policy`
- `can_fix_with_existing_data`
- `requires_new_data_source`
- `can_remain_diagnostic_only`
- `promotion_grade_blocker`

## 分类

- `SOURCE_MANIFEST_REQUIRED`
- `AVAILABLE_TIME_REQUIRED`
- `AS_OF_SNAPSHOT_REQUIRED`
- `CORPORATE_ACTION_POLICY_REQUIRED`
- `VENDOR_CURRENT_VIEW_ONLY`
- `RESEARCH_LABEL_ONLY_BY_DESIGN`
- `TRUE_PIT_LIMITATION`
- `MANUAL_REVIEW_REQUIRED`

## 进度记录

- 2026-06-21：任务登记进入 IN_PROGRESS；固定只读 requirement matrix 范围。
- 2026-06-21：实现 `aits data source-qualification requirements`，默认 run 生成
  `data_source_requirement_matrix.json/md`；`P0_remaining_count=9`、
  `source_requirement_count=9`、`requires_new_data_source_count=8`、
  `can_fix_with_existing_data_count=0`、`promotion_grade_blocker_count=9`、
  `status_upgrade_attempted=false`，并保持 `production_effect=none`、
  `broker_action=none`、`promotion_gate_allowed=false`。
- 2026-06-21：focused validation 通过：
  `python -m pytest -n 16 --dist loadfile tests/test_data_source_requirement_matrix.py tests/test_data_source_remediation_execution.py tests/test_data_source_qualification_remediation.py tests/test_data_foundation_acceptance.py tests/test_documentation_contract.py tests/test_report_index.py tests/test_artifact_lineage.py -q --durations=20 --durations-min=1`
  输出 40 passed。
- 2026-06-21：validation tier 通过：`fast-unit` 97 passed、
  `contract-validation` 96 passed、`report-validation` 55 passed
  （62 warnings，来自既有 numpy divide runtime warning）。
