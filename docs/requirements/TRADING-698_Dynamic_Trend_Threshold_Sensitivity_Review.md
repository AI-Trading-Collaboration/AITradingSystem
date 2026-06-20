# TRADING-698 Dynamic / Trend Threshold Sensitivity Review

最后更新：2026-06-20

## 背景

TRADING-697 已为第二批 dynamic allocation / trend calibration 阈值建立
calibration prep。本任务在不修改阈值当前值的前提下，对相同 3 个阈值做
validation-only sensitivity review，并把真实 trace/outcome 证据、样本成熟度和
data gap 分开披露。

## 安全边界

- 不修改任何 threshold `current_value`。
- 不修改 production / paper-shadow / official weights。
- 不改变 production scoring、position gate、paper shadow state、broker/order 或
  official target weights。
- 不允许升级为 `VALIDATED_BOUNDARY`；本轮最高只能标记为
  `SENSITIVITY_TESTED`。
- 所有输出继续固定：
  - `production_effect=none`
  - `promotion_gate_allowed=false`
  - `paper_shadow_change_allowed=false`
  - `production_weight_change_allowed=false`

## 范围

只覆盖以下 3 个阈值：

- `dynamic_allocation.risk_off_score_thresholds`
- `dynamic_allocation.risk_on_confirmation_thresholds`
- `trend_calibration.score_bands`

## 输出要求

`dynamic_trend_threshold_sensitivity_review.json/md` 每个阈值输出：

- `current_value`
- `tested_values` / scenario variants：
  - `current_value`
  - `stricter`
  - `relaxed`
  - `capped` 或 `smoothed_candidate`
  - `no_change_baseline`
- 每个 variant 输出：
  - `avg_return_1d/5d/10d/20d`
  - `hit_rate_1d/5d/10d/20d`
  - `max_drawdown`
  - `drawdown_preservation`
  - `turnover`
  - `constraint_hit_count`
  - `risk_off_trigger_count`
  - `risk_on_confirmation_count`
  - `false_risk_off_count`
  - `false_risk_on_count`
  - `missed_upside_count`
- 分层输出：
  - by horizon
  - by asset
  - by date
  - by regime
  - by event window
  - full_advisory_only
  - component/backtest bridge when applicable
  - correlated asset cluster
- 样本质量必须披露：
  - `mature_date_count`
  - `mature_case_count`
  - `full_advisory_case_count`
  - `cluster_count`
  - `regime_count`
  - `sample_quality_breakdown`

## Recommendation 约束

Recommendation 只能是 validation recommendation：

- `keep_current_value`
- `adjust_candidate`
- `insufficient_data`
- `collect_evidence_only`
- `sensitivity_tested_only`

本轮不得输出 production/paper-shadow/promotion recommendation。

## 已知数据限制

当前可用 historical trace 是 component-level multi-stage trace，包含 trend、
risk_sentiment 等 component scores 和 realized price outcomes，但不等价于完整
dynamic allocation production replay。报告必须明确：

- direct dynamic allocation score history 是否可用；
- full advisory equivalent cases 是否足够；
- component/backtest bridge 能覆盖哪些指标；
- 哪些 conclusion 只能作为 validation-only sensitivity evidence。

## 验收

- Report 只覆盖 3 个目标阈值。
- 每个目标阈值都有 5 类 variants，且每个 variant 输出指定 metrics。
- 不只用 row_count 下结论，必须输出成熟样本、full advisory、cluster/regime 和
  sample quality breakdown。
- 所有 recommendation 保持 validation-only，不修改 current value。
- 所有输出保持 `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- focused 并行 pytest、Ruff、py_compile、真实 CLI、`git diff --check` 和 validation
  tier 通过或记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。本轮只做实际 sensitivity review，
  不修改 production / paper-shadow / official weights，不修改当前 threshold value。
- 2026-06-20：实现 `dynamic_trend_threshold_sensitivity_review.json/md` 和
  validation pack summary 集成，并进入 `VALIDATING`。真实 CLI 输出
  `tested_threshold_count=3`、`sensitivity_tested_count=3`、
  `validated_boundary_count=0`、`thresholds_changed_count=0`；
  成熟样本按 horizon 为 `1d=20`、`5d=16`、`10d=11`、`20d=0`，
  `full_advisory_case_count=22`、`cluster_count=1`、`regime_count=1`。
  所有 3 个阈值 recommendation 均为 `sensitivity_tested_only`，
  `evidence_strength=low`，原因是 direct dynamic allocation score history
  不可用、`GrowthLeadershipScore` 缺失、20d mature cases 尚未形成充分样本。
  输出继续固定 `production_effect=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false`。
- 2026-06-20：验证通过 `py_compile`、Ruff、Black check、focused indicator
  pytest `41 passed`、task/docs/config 并行 pytest `27 passed`、
  `git diff --check` 和 full 并行 validation tier
  `2982 passed / 643 warnings / 159.76s`。Runtime artifact:
  `outputs/validation_runtime/full_20260620T124053Z/test_runtime_summary.json`。
