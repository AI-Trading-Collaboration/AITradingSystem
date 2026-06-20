# TRADING-695 Indicator Research Evidence Gate Threshold Calibration

最后更新：2026-06-20

## 背景

TRADING-693 已经生成 threshold registry、high-impact prioritization report，并把
第一批 calibration candidates 限定为 validation-only backlog。本任务只处理第一批
中的 indicator research evidence gate 阈值，不触碰 dynamic allocation、trend
calibration、production / paper-shadow / official weights。

## 安全边界

- 本轮只生成 validation-only artifacts。
- 不修改 production / paper-shadow / official weights。
- 不改变 production 权重计算逻辑。
- 不修改 production scoring、position gate、paper shadow state、broker/order 或
  official target weights。
- 所有输出继续固定：
  - `promotion_gate_allowed=false`
  - `production_weight_change_allowed=false`
  - `paper_shadow_change_allowed=false`
  - `production_effect=none`

## 范围

只校准以下 5 个 indicator research evidence gate 阈值：

- `indicator_research.effectiveness_min_available_outcome_cases`
- `indicator_research.robustness_cluster_dominance_share`
- `indicator_research.effectiveness_missed_upside_acceptable_rate`
- `indicator_research.masking_high_min`
- `indicator_research.dominant_share_of_adjustment_min`

不校准本批中的 dynamic allocation / trend calibration / production weight 或 paper-shadow
相关阈值。

## 输出

1. `threshold_calibration_report.json/md`
   - 每个阈值输出：
     - `current_value`
     - `tested_values`
     - `recommendation_by_value`
     - `promotion_gate_by_value`
     - `false_promotion_risk`
     - `false_rejection_risk`
     - `sample_quality_impact`
     - `valuation_crowding_recommendation_changes`
     - `recommended_status`
     - `keep_current_value` / `adjust_candidate` / `insufficient_data`
     - `reason`
     - `evidence_strength`
     - `remaining_limitations`

2. floor 50 类阈值解释：
   - `threshold_type=heuristic_guardrail`
   - `not_validated_statistical_boundary=true`
   - `calibration_status` 最多从 `UNCALIBRATED` 升级到 `SENSITIVITY_TESTED`

3. Robustness checks：
   - date-equal-weight aggregation
   - asset-equal-weight aggregation
   - cluster-equal-weight aggregation
   - leave-one-date-out
   - leave-one-cluster-out
   - full_advisory_only vs all_sources consistency

4. Validation pack 增加 `threshold_calibration_summary`：
   - `tested_threshold_count`
   - `sensitivity_tested_count`
   - `still_uncalibrated_high_impact_count`
   - `thresholds_still_blocking_promotion_count`
   - `thresholds_changed_count`
   - `production_effect=none`

## 验收

- `build_threshold_calibration_report()` 输出 `threshold_calibration_report` artifact。
- `aits research indicators threshold-calibration` 可写出 JSON/Markdown artifact。
- Validation pack 输出 `threshold_calibration_summary`。
- 只测试本任务指定的 5 个阈值。
- floor 50 类阈值仍标记为 heuristic guardrail，不被写成 validated statistical boundary。
- Robustness checks 出现在 report 中，且不允许 promotion。
- focused 并行 pytest、Ruff、py_compile、真实 CLI、validation pack、`git diff --check`
  和 full 并行 validation tier 通过或记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮基于 TRADING-693 第一批
  calibration candidates，只做 indicator research evidence gate 阈值 sensitivity
  analysis；继续保持 validation-only，不修改 production / paper-shadow / official
  weights。
- 2026-06-20：实现 `build_threshold_calibration_report()` 与
  `aits research indicators threshold-calibration`，并接入 validation pack
  `threshold_calibration_summary` 和 stability projection。真实 CLI 使用
  `historical_multi_stage_weight_trace_2026-05-18_to_2026-06-18_expanded.json`、
  `2026-06-17/prices_daily.csv`、historical gate audit root 和 bridge root 生成
  `outputs/research_indicators/threshold_calibration_report.json/md`：
  `tested_threshold_count=5`、`sensitivity_tested_count=5`、
  `still_uncalibrated_high_impact_count=31`、
  `thresholds_still_blocking_promotion_count=36`、
  `thresholds_changed_count=0`、`production_effect=none`。其中
  `effectiveness_min_available_outcome_cases` 和
  `robustness_cluster_dominance_share` 因样本/单 cluster 证据不足保持
  `insufficient_data`；其余 3 个阈值为 `keep_current_value` sensitivity-tested。
  所有 report / validation-pack 顶层与 summary 继续显式保持
  `promotion_gate_allowed=false`、`production_weight_change_allowed=false`、
  `paper_shadow_change_allowed=false`。
- 2026-06-20：实现完成并进入 `VALIDATING`。验证通过 Black、py_compile、
  Ruff、focused indicator pytest `38 passed`、task/docs/config 并行 pytest
  `27 passed`、真实 `threshold-calibration` CLI、真实 `validation-pack` CLI、
  `validation-pack-stability` CLI（`PASS stable=True artifact_count=35`）、
  `git diff --check` 和 full 并行 validation tier
  `2979 passed / 643 warnings / 166.15s`。Runtime artifact：
  `outputs/validation_runtime/full_20260620T110625Z/test_runtime_summary.json`。
