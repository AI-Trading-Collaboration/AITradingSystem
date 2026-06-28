# TRADING-2267 Norgate Trial 2Y Partial Effectiveness Validation

最后更新：2026-06-28

## Status

- Task id: `TRADING-2267_NORGATE_TRIAL_2Y_PARTIAL_EFFECTIVENESS_VALIDATION`
- Status: `VALIDATING`
- Last updated: 2026-06-28
- Owner: project owner

## Scope

本批使用 Norgate trial 可访问的 2Y price history 和 Nasdaq-100 / `$NDX`
date-aware membership snapshot，评估 Norgate breadth / participation features
在最近 2 年内是否具有局部研究价值，并为是否值得 owner 批准正式 Platinum
历史订阅提供证据。

本批必须明确区分：

- `engineering_validated`
- `feature_numeric_validated`
- `local_signal_evidence`
- `primary_window_validated=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`

## Non-Goals

- 不把 2Y trial 结果当作 2021-02-22 primary window validation。
- 不恢复 first-layer、v4、minimal forward diagnostic、promotion、paper-shadow、
  production 或 broker。
- 不输出 target weights、trade advice、allocation 或 broker/order action。
- 不提交 vendor raw prices、本地 Norgate cache、credentials 或完整 member symbol list。
- 不自动购买正式 Platinum；任何 purchase 仍需要 owner manual approval。

## Required Outputs

1. `coverage_report`
   - earliest price date。
   - latest price date。
   - member-day coverage ratio。
   - missing price ratio。
   - failed join count。
2. `breadth_feature_report`
   - `pct_above_ma20`
   - `pct_above_ma50`
   - `pct_above_ma200`
   - `equal_weight_return`
   - `cap_weight_proxy_return`
   - `advance_decline_proxy`
   - `breadth_momentum`
3. `local_signal_report`
   - breadth bucket vs next 5D / 10D / 20D QQQ return。
   - breadth deterioration vs future drawdown。
   - baseline first-layer proxy vs baseline + breadth。
   - false risk-off / false risk-on comparison。
4. `conclusion_matrix`
   - `source_engineering_useful: true/false`
   - `source_feature_useful_2y: true/false/weak`
   - `purchase_platinum_evidence_strength: weak/moderate/strong`
   - `model_ready_for_2021_primary_window: false`
   - `reopen_gate_allowed: false`

## Policy Notes

该任务会引入局部有效性分桶、deterioration event 和 false risk-on/off
诊断口径。所有阈值和分桶规则必须放入
`config/research/norgate_trial_partial_effectiveness_policy.yaml`，并标记为
trial diagnostic policy，不得作为 production threshold 或 promotion gate。

## Acceptance Criteria

- 新增 CLI 可以在缺少 Norgate package/local DB 时 fail-closed。
- 有 Norgate trial/local DB 时生成 coverage、feature、local signal 和 conclusion
  summary artifacts。
- 输出只包含日期级 / 聚合 feature 和 hash/count，不提交完整 member symbol list 或
  raw vendor price table。
- 结论可以支持“是否值得继续购买正式历史”判断，但必须保持
  `primary_window_validated=false`、`reopen_gate_allowed=false`、
  `promotion_allowed=false`、`paper_shadow_allowed=false`、
  `production_allowed=false`、`broker_action=none`。
- Report registry、artifact catalog、system flow 和 task register 已更新。
- Focused pytest、Ruff、compileall、documentation/report/task checks、diff checks 和
  contract validation 通过。

## Progress Log

- 2026-06-28: Task created from owner request. Implementation starts with a
  Norgate 2Y partial effectiveness policy, CLI runner, summary-only artifacts,
  local signal diagnostics and guardrail tests.
- 2026-06-28: Implementation completed and moved to `VALIDATING`. Added
  `aits data norgate partial-effectiveness`, diagnostic policy, coverage report,
  breadth feature report, local signal report, conclusion matrix, report
  registry, artifact catalog, system flow and focused tests. Real local run
  returned `NORGATE_2Y_PARTIAL_EFFECTIVENESS_READY`: earliest price date
  `2024-06-28`, latest price date `2026-06-26`,
  `member_day_coverage_ratio=1.0`, `missing_price_ratio=0.0`,
  `failed_join_count=0`, `engineering_validated=true`,
  `feature_numeric_validated=true`, `local_signal_evidence=none`,
  `source_feature_useful_2y=weak` and
  `purchase_platinum_evidence_strength=moderate`. Primary window remains not
  validated: `primary_window_validated=false`,
  `model_ready_for_2021_primary_window=false`, `reopen_gate_allowed=false`,
  `promotion_allowed=false`, `paper_shadow_allowed=false`,
  `production_allowed=false`, `broker_action=none`.
- 2026-06-28: Validation passed: Ruff, compileall, focused Norgate partial
  effectiveness/trial tests, report/documentation/task-register tests,
  research audit/governance tests and `contract-validation`. Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260628T103915Z/test_runtime_summary.json`.
