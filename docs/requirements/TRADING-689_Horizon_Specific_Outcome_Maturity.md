# TRADING-689 Horizon-Specific Outcome Maturity

最后更新：2026-06-20

## 背景

TRADING-688 已把 realized outcome 缺失拆分为 hard missing 和
`outcome_not_mature`，并确认当前 expanded historical trace 的 hard missing 为 0。
但 availability 仍以“所有 1d/5d/10d/20d 窗口全部成熟”作为 case-level available
口径，导致 20d 未成熟会掩盖已成熟的 1d / 5d / 10d outcome。

本任务把 outcome maturity 和 masking effectiveness review 改为 horizon-specific，
使短 horizon 已成熟样本可以独立参与 validation-only review，同时继续保留 20d
成熟度限制。

## 安全边界

- 只做 read-only / validation-only artifacts。
- 不修改 paper-shadow / live / broker / order / official weights。
- 不改变 production 权重计算逻辑。
- 不放宽 production data_quality_gate。
- signal generation 必须保持 PIT/as-of，不得使用未来数据。
- outcome evaluation 只能读取 `decision_time` 之后的 realized price。
- outcome 窗口使用交易日窗口，不使用自然日窗口。
- 所有 recommendation 都是 validation recommendation，`promotion_gate_allowed=false`。

## 输出要求

在 TRADING-688 outcome availability audit 基础上新增 horizon-specific maturity：

- `1d_mature_case_count`
- `5d_mature_case_count`
- `10d_mature_case_count`
- `20d_mature_case_count`
- `1d_not_mature_count`
- `5d_not_mature_count`
- `10d_not_mature_count`
- `20d_not_mature_count`

新增 evaluation cutoff 规则：对每个 horizon，仅当
`as_of_date + horizon trading days <= latest_available_price_date` 时计入 mature
outcome；否则标记 `outcome_not_mature`，不得计入 hard missing。

## Mature Sample Quality Report

输出 mature outcome sample quality report，至少包括：

- `mature_date_count_by_horizon`
- `mature_asset_count_by_horizon`
- `mature_case_count_by_horizon`
- `full_advisory_mature_count_by_horizon`
- `component_only_mature_count_by_horizon`
- `backtest_bridge_mature_count_by_horizon`
- by-asset availability
- by-date availability
- by-regime availability
- by-event-window availability

## Effectiveness Review

masking effectiveness review 必须保留既有分层，并新增 by-horizon：

- full_advisory_only
- component_only
- backtest_bridge
- by date
- by asset
- by regime
- by event window
- by horizon

每个 horizon 独立比较 baseline / no valuation-crowding masking / capped masking，
输出 avg return、hit rate、drawdown_reduced_count、missed_upside_count 和
false_risk_off_count。20d 未成熟不得让 1d / 5d 已成熟 outcome 从 review 中消失。

## Recommendation 规则

本任务使用 validation-only pilot recommendation baseline，不进入 production policy：

- mature sample 仍不足：`insufficient_evidence`
- 1d/5d 已有足够 mature sample，但 10d/20d 不足：
  `preliminary_short_horizon_only`
- capped masking 在 mature horizons 上优于 baseline：
  `prefer_capped_masking_candidate`
- no-mask 优于 baseline 且 drawdown 未明显恶化：
  `baseline_over_defensive_candidate`
- baseline 明显减少 drawdown 且 missed upside 可接受：
  `keep_baseline_masking_candidate`

`promotion_gate_allowed` 必须保持 false。

## 验收

- horizon-specific outcome maturity test 通过；
- `outcome_not_mature` 不计入 hard missing；
- 20d 未成熟不影响 1d / 5d review；
- evaluation cutoff 正确；
- `promotion_gate_allowed=false`；
- mature sample 不足时稳定输出 `insufficient_evidence`；
- 并行 pytest / validation tier 通过或明确记录阻塞。

## 状态记录

- 2026-06-20：新增并进入 `IN_PROGRESS`。
- 2026-06-20：实现完成并进入 `VALIDATING`。真实 expanded historical
  trace rerun 仍保持 `total_cases=1232`、`outcome_missing_count=0`，但
  maturity 已拆为 horizon-specific：`1d_mature_case_count=1176`、
  `5d_mature_case_count=952`、`10d_mature_case_count=672`、
  `20d_mature_case_count=56`；对应 not-mature 为 56 / 280 / 560 / 1176。
  Full-advisory mature count by horizon 为 672 / 544 / 384 / 32，
  backtest-bridge mature count by horizon 为 504 / 408 / 288 / 24。重跑
  masking effectiveness review 后 full-advisory by-horizon mature cases 为
  168 / 136 / 96 / 8，recommendation 为
  `preliminary_short_horizon_only`，原因是 1d/5d 样本已可用于初步短 horizon
  诊断，但 10d/20d 仍不足以支持完整 policy 结论；所有 recommendation 仍是
  validation-only，`promotion_gate_allowed=false`。尝试向更早 2026-05-19
  生成 isolated replay trace 时被 PIT feature availability gate fail-closed
  阻断，未放宽 production gate。验证通过 focused 并行 pytest 35 passed、
  Ruff、py_compile、`git diff --check` 和 full 并行 validation tier 2970
  passed / 643 warnings / 130.29s。
