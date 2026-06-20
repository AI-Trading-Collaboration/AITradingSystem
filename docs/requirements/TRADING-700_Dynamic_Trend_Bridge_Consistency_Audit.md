# TRADING-700 Dynamic Trend Bridge Consistency Audit

## 状态

- 状态：VALIDATING
- 日期：2026-06-20
- 范围：validation-only source-layer consistency audit
- Owner：系统实现 + 项目 owner 后续复核

## 背景

TRADING-699 已将 `dynamic_trend_threshold_sensitivity_review` 的 coverage 扩展到
`backtest_trace_bridge`，当前基线为：

- `full_advisory_score_daily_trace=22`
- `backtest_trace_bridge=1728`
- `cluster_count=4`
- `regime_count=4`
- `recommendation=sensitivity_tested_only`
- `evidence_strength=low`

该扩展提高了 diagnostic coverage，但不能把 `backtest_trace_bridge` 当作
full-advisory 等价证据，也不能单独作为 promotion gate evidence。

## 目标阈值

本轮只审计以下 3 个 threshold 的 source-layer consistency：

1. `dynamic_allocation.risk_off_score_thresholds`
2. `dynamic_allocation.risk_on_confirmation_thresholds`
3. `trend_calibration.score_bands`

## 输出要求

新增 `dynamic_trend_bridge_consistency_audit.json/md`，并在 validation pack 中增加
`dynamic_trend_bridge_consistency_summary`。每个 threshold 必须输出：

- `recommendation_by_full_advisory_only`
- `recommendation_by_backtest_bridge_only`
- `direction_agreement`
- `metric_delta_by_source`
- `false_risk_off_delta`
- `false_risk_on_delta`
- `missed_upside_delta`
- `drawdown_preservation_delta`
- `turnover_delta`
- `constraint_hit_delta`
- `bridge_reliability`

`bridge_reliability` 只能使用以下标签：

- `bridge_consistent_with_full_advisory`
- `bridge_directionally_consistent_but_magnitude_uncertain`
- `bridge_conflicts_with_full_advisory`
- `insufficient_full_advisory_to_assess`

如果 full-advisory 样本不足以判断一致性，必须保持：

- `evidence_strength=low`
- `recommendation=sensitivity_tested_only`
- `validated_boundary=false`

## 安全边界

- 不修改任何 threshold current value。
- 不修改 production / paper-shadow / official weights。
- `production_effect=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `backtest_trace_bridge` 不能单独作为 promotion gate evidence。
- 本轮不得升级为 `VALIDATED_BOUNDARY`。

## 实现计划

1. 从 TRADING-699 `dynamic_trend_threshold_sensitivity_review` 读取每个 threshold 的
   full-advisory-only 与 `backtest_trace_bridge` variant metrics。
2. 对 current / stricter / relaxed / capped-or-smoothed / no-change variants 计算
   source-layer metric delta。
3. 按 threshold 汇总 recommendation、direction agreement 和 bridge reliability。
4. 写出 JSON/Markdown artifact，并纳入 validation pack summary 与 stability projection。
5. 运行 focused tests、CLI smoke、Ruff、py_compile、`git diff --check` 和 validation tier。

## 验收标准

- 三个目标 threshold 都有 source-layer consistency record。
- report summary 披露 full-advisory / bridge case count、cluster_count、regime_count、
  reliability label counts 和 no-change safety flags。
- full-advisory 样本不足时不提升 evidence strength、不改变 recommendation、不设置
  validated boundary。
- validation pack 输出 `dynamic_trend_bridge_consistency_summary`。
- 所有输出继续保持 validation-only 和 no production effect。

## 进度记录

- 2026-06-20：按 owner 指令新增；进入 IN_PROGRESS；实现前固定 validation-only
  safety boundary 和 source-layer consistency 输出契约。
- 2026-06-20：实现 `build_dynamic_trend_bridge_consistency_audit()`、
  `aits research indicators dynamic-trend-bridge-consistency-audit`、validation pack
  `dynamic_trend_bridge_consistency_summary` 和 stability projection。真实 CLI 读取
  `outputs/research_indicators/dynamic_trend_threshold_sensitivity_review.json`，输出
  `outputs/research_indicators/dynamic_trend_bridge_consistency_audit.json/md`：
  `tested_threshold_count=3`、`full_advisory_case_count=22`、
  `backtest_bridge_case_count=1728`、`cluster_count=4`、`regime_count=4`、
  `bridge_reliability_counts={insufficient_full_advisory_to_assess:3}`、
  `recommendation=sensitivity_tested_only`、`evidence_strength=low`、
  `validated_boundary_count=0`、`thresholds_changed_count=0`、
  `bridge_only_promotion_gate_evidence_allowed=false`、`production_effect=none`。
  Validation pack 使用 expanded trace、2026-06-17 prices、historical gate audit root
  和 `outputs/research_campaigns` coverage root 重写，summary 同步 22/1728 基线。
  验证通过 py_compile、Ruff、Black check、focused indicator pytest 43 passed、
  task/docs/config 并行 pytest 27 passed、`git diff --check` 和 full 并行 validation
  tier 2984 passed / 643 warnings / 128.71s，runtime artifact
  `outputs/validation_runtime/full_20260620T142349Z/test_runtime_summary.json`。
