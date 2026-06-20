# TRADING-690 Horizon Effectiveness Conclusion Matrix

最后更新：2026-06-20

## 背景

TRADING-689 已把 valuation/crowding masking 的 realized outcome maturity
改为 horizon-specific，并确认当前 expanded historical trace hard missing 为 0。
当前基线：

- total_cases=1232
- outcome_missing_count=0
- 1d/5d/10d/20d mature counts = 1176 / 952 / 672 / 56
- full-advisory 1d/5d/10d/20d mature cases = 168 / 136 / 96 / 8
- recommendation = `preliminary_short_horizon_only`
- `promotion_gate_allowed=false`

TRADING-690 在此基础上扩展 masking effectiveness review，使 baseline、
no valuation/crowding masking、capped masking 三个 scenario 可以按 horizon
形成可审计的 conclusion matrix，并把 recommendation 规则从全局聚合改为
短/中 horizon 保守比较。

## 安全边界

- 只生成 read-only / validation-only artifacts。
- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production data_quality_gate。
- signal generation 继续保持 PIT/as-of，不使用未来数据。
- outcome evaluation 只能读取 `decision_time` 之后 realized price。
- 所有 recommendation 都是 validation recommendation，`promotion_gate_allowed=false`。

## 输出要求

Masking effectiveness review 需要对三个 scenario 按每个 horizon 单独输出：

- `avg_return`
- `median_return`
- `hit_rate`
- `downside_capture`
- `max_drawdown`
- `drawdown_reduced_count`
- `missed_upside_count`
- `false_risk_off_count`
- `turnover`
- `constraint_hit_count`
- `sample_count`
- `full_advisory_sample_count`
- `component_only_sample_count`
- `backtest_bridge_sample_count`

分层输出至少包括：

- by horizon
- by asset
- by date
- by regime
- by event window
- full_advisory_only
- component_only
- backtest_bridge
- correlated asset cluster

报告不得只用 row_count 下结论，必须展示：

- `mature_date_count`
- `mature_asset_count`
- `mature_case_count`
- `full_advisory_mature_case_count`
- `unique_regime_count`
- `correlated_asset_cluster_count`

20d 必须单独标记 `insufficient_long_horizon_evidence`；不得用当前仅 8 个
full-advisory 20d mature cases 推导正式结论。

## Recommendation 规则

所有 recommendation 仍固定 `promotion_gate_allowed=false`：

- capped masking 在 1d/5d/10d 上收益不差于 baseline，且
  missed_upside / false_risk_off 更低：`prefer_capped_masking_candidate`
- no-mask 在 1d/5d/10d 上收益更高，且 drawdown 未明显恶化：
  `baseline_over_defensive_candidate`
- baseline 明显减少 drawdown，且 missed_upside 可接受：
  `keep_baseline_masking_candidate`
- 不同 horizon 结论冲突：`preliminary_short_horizon_only`
- 样本仍不足：`insufficient_evidence`
- 仅在 validation 层输出 candidate，不允许进入 promotion gate。

## Conclusion Matrix

新增 conclusion matrix，每个 scenario × horizon 一行，包含：

- sample quality
- return profile
- risk profile
- false risk-off
- missed upside
- recommendation contribution

## 验收

- horizon-specific effectiveness aggregation test 通过；
- 20d insufficient 不影响 1d/5d/10d review；
- full_advisory_only 分层稳定输出；
- scenario comparison schema 完整；
- `promotion_gate_allowed=false`；
- conflicting horizon 时稳定保持 `preliminary_short_horizon_only`；
- 并行 pytest / validation tier 通过或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。
- 2026-06-20：实现完成并进入 `VALIDATING`。基于
  `historical_multi_stage_weight_trace_2026-05-18_to_2026-06-18_expanded.json`
  重跑 masking effectiveness review，status=`PASS_WITH_WARNINGS`，
  conclusion_matrix_row_count=12，`promotion_gate_allowed=false`。Horizon
  maturity 维持 TRADING-689 口径：1d/5d/10d/20d mature counts 为
  1176/952/672/56，full-advisory mature cases 为 168/136/96/8，
  outcome_missing_count=0。20d baseline row 标记
  `insufficient_long_horizon_evidence`，full_advisory_sample_count=8，不用于
  policy conclusion。1d/5d baseline rows 为 neutral/incomplete，10d baseline row
  contribution=`supports_keep_baseline_masking_candidate`，因此最终
  recommendation 仍为 `preliminary_short_horizon_only`，原因是 1d/5d/10d
  matrix 结论 mixed or incomplete。Validation pack 仍为
  `INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS`，
  artifacts=29；validation-pack-stability `PASS`，stable=true。验证通过 Ruff、
  py_compile、focused 并行 pytest 37 passed、`git diff --check` 和 full 并行
  validation tier 2972 passed / 643 warnings / 175.77s。
