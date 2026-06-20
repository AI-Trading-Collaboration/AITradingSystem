# TRADING-697 Dynamic Allocation / Trend Threshold Calibration Prep

最后更新：2026-06-20

## 背景

TRADING-695/696 已完成第一批 indicator research evidence gate 阈值的
validation-only sensitivity 和 follow-up planning。本任务进入第二批
dynamic allocation / trend calibration 阈值的 calibration prep，只建立待测情景、
影响路径和数据需求，不修改任何阈值当前值，不升级为 validated statistical boundary。

## 安全边界

- 不修改任何 threshold `current_value`。
- 不修改 production / paper-shadow / official weights。
- 不改变 production scoring、position gate、paper shadow state、broker/order 或
  official target weights。
- 不允许把任何阈值升级为 `VALIDATED_BOUNDARY`；最多只能进入
  `SENSITIVITY_TESTED`。
- 所有输出继续固定：
  - `promotion_gate_allowed=false`
  - `paper_shadow_change_allowed=false`
  - `production_effect=none`

## 范围

只对以下 3 个第二批阈值做 calibration prep：

- `dynamic_allocation.risk_off_score_thresholds`
- `dynamic_allocation.risk_on_confirmation_thresholds`
- `trend_calibration.score_bands`

## 输出

`dynamic_trend_threshold_calibration_prep_report.json/md` 每个阈值输出：

- `current_value`
- `where_used`
- `decision_affecting_path`
- `tested_values`
- `sensitivity_impact`
- `recommendation_by_value`
- `false_risk_off_impact`
- `false_risk_on_impact`
- `turnover_constraint_hit_impact`
- `drawdown_missed_upside_impact`

## 验收

- Report 只覆盖 3 个目标阈值。
- 每个目标阈值有 tested values 和 per-value recommendation rows。
- 每个目标阈值披露 false risk-off/risk-on、turnover/constraint hit、drawdown/missed
  upside impact。
- 所有 recommendations 都是 calibration-prep / collect-evidence only，不建议修改
  current value。
- 所有输出保持 `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`。
- focused 并行 pytest、Ruff、py_compile、真实 CLI、`git diff --check` 和 validation
  tier 通过或记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮只建立 second-batch
  dynamic/trend threshold calibration prep；继续保持 validation-only，不修改
  production / paper-shadow / official weights。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `build_dynamic_trend_threshold_calibration_prep_report()` 与
  `aits research indicators dynamic-trend-threshold-calibration-prep`，真实 CLI
  生成 `outputs/research_indicators/dynamic_trend_threshold_calibration_prep_report.json/md`：
  `prepared_threshold_count=3`、`calibration_prepared_count=3`、
  `sensitivity_tested_count=0`、`validated_boundary_count=0`、
  `thresholds_changed_count=0`、`max_allowed_status=SENSITIVITY_TESTED`、
  `production_effect=none`。目标阈值 current_value 未改变。验证通过 Black、
  py_compile、Ruff、focused indicator pytest `40 passed`、task/docs/config 并行 pytest
  `27 passed`、`git diff --check` 和 full 并行 validation tier
  `2981 passed / 642 warnings / 124.96s`。Runtime artifact：
  `outputs/validation_runtime/full_20260620T120931Z/test_runtime_summary.json`。
