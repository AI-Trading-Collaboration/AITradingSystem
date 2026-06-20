# TRADING-696 Threshold Calibration Follow-up Plan

最后更新：2026-06-20

## 背景

TRADING-695 已经对第一批中的 5 个 indicator research evidence gate 阈值生成
validation-only sensitivity report。本任务只把该 report 转成后续校准计划和数据缺口
清单，不修改任何阈值当前值，不批准 promotion，不改变 production / paper-shadow /
official weights。

## 安全边界

- 不修改任何 `current_value`。
- 不修改 production / paper-shadow / official weights。
- 不改变 production scoring、position gate、paper shadow state、broker/order 或
  official target weights。
- 所有输出继续固定：
  - `promotion_gate_allowed=false`
  - `production_weight_change_allowed=false`
  - `paper_shadow_change_allowed=false`
  - `production_effect=none`

## 范围

从 `threshold_calibration_report` 抽取以下 5 个阈值的逐项结果：

- `indicator_research.effectiveness_min_available_outcome_cases`
- `indicator_research.robustness_cluster_dominance_share`
- `indicator_research.effectiveness_missed_upside_acceptable_rate`
- `indicator_research.masking_high_min`
- `indicator_research.dominant_share_of_adjustment_min`

对 `insufficient_data` 阈值输出 data gap plan；对 `keep_current_value` 阈值明确
`calibration_status=SENSITIVITY_TESTED`、`not_validated_statistical_boundary=true` 和
no production / promotion effect。

## 输出

1. `threshold_calibration_followup_plan.json/md`
   - `threshold_id`
   - `current_value`
   - `tested_values`
   - `recommendation`
   - `reason`
   - `evidence_strength`
   - `recommendation_changed`
   - `remaining_data_gap`

2. `data_gap_plan` for insufficient data thresholds：
   - 缺什么样本
   - 缺哪个 horizon
   - 缺 full_advisory 还是 component/backtest
   - 是否受 PIT gate 限制
   - 是否需要等待 forward maturity
   - 是否可以通过更早 historical replay 补齐

3. `config/research/threshold_registry.yaml` validation-only summary：
   - `sensitivity_tested_count=5`
   - `validated_boundary_count=0`
   - `still_uncalibrated_high_impact_count=31`
   - `thresholds_still_blocking_promotion_count=36`

## 验收

- Follow-up plan 从 calibration report 读取，不重算或改变阈值。
- 只覆盖 TRADING-695 的 5 个阈值。
- `insufficient_data` 阈值有可审计 data gap plan。
- `keep_current_value` 阈值仍为 `SENSITIVITY_TESTED`，不是 validated statistical boundary。
- Registry 只更新 summary metadata，不修改任何 `current_value`。
- focused 并行 pytest、Ruff、py_compile、真实 CLI、`git diff --check` 和 validation tier
  通过或记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮只生成 follow-up plan 和 registry
  summary metadata；继续保持 validation-only，不修改 production / paper-shadow /
  official weights。
- 2026-06-20：实现完成并进入 `VALIDATING`。新增
  `build_threshold_calibration_followup_plan()` 与
  `aits research indicators threshold-calibration-followup`，真实 CLI 从
  `outputs/research_indicators/threshold_calibration_report.json` 生成
  `outputs/research_indicators/threshold_calibration_followup_plan.json/md`：
  `threshold_count=5`、`insufficient_data_threshold_count=2`、
  `keep_current_value_threshold_count=3`、`recommendation_changed_count=2`、
  `thresholds_changed_count=0`、`validated_boundary_count=0`、
  `production_effect=none`。`recommendation_changed` 仅表示 sensitivity 场景下
  recommendation 有变化，不表示 threshold current value 改变。验证通过 Black、
  py_compile、Ruff、focused indicator pytest `39 passed`、task/docs/config 并行 pytest
  `27 passed`、`git diff --check` 和 full 并行 validation tier
  `2980 passed / 642 warnings / 135.64s`。Runtime artifact：
  `outputs/validation_runtime/full_20260620T114551Z/test_runtime_summary.json`。
