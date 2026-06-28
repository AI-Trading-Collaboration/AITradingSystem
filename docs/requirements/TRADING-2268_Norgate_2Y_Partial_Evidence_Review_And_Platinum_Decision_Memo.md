# TRADING-2268 Norgate 2Y Partial Evidence Review And Platinum Decision Memo

最后更新：2026-06-28

## Status

- Task id: `TRADING-2268_NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_AND_PLATINUM_DECISION_MEMO`
- Status: `VALIDATING`
- Last updated: 2026-06-28
- Owner: project owner

## Scope

本批基于 TRADING-2267 已生成的 Norgate trial 2Y coverage、breadth feature、
local signal 和 conclusion artifacts，复盘为什么
`local_signal_evidence=none`，并形成是否购买 Norgate Platinum full-history 的
owner decision memo。

本批只回答：

- 2Y trial 是否提供局部判断和购买决策证据。
- `local_signal_evidence=none` 主要来自样本、feature、outcome、event、
  baseline 增量、benchmark 一致性，还是缺少 2022 类压力窗口。
- 是否需要购买正式 full-history 才能回答 primary-window / stress-window 问题。

本批不能回答：

- Norgate 是否已经让第一层策略通过正式验收。
- 2021-02-22 primary window 是否 model-ready。
- 是否恢复 first-layer、v4、minimal forward diagnostic、promotion、paper-shadow、
  production 或 broker。

## Required Checks

1. Breadth feature 是否有足够时间序列变化，而不是近似平坦。
2. Breadth bucket 样本数量是否足够，是否存在 bucket imbalance。
3. Next 5D / 10D / 20D outcome 是否被少数异常日主导。
4. Breadth deterioration / recovery event 数量是否足够。
5. Baseline first-layer proxy vs baseline + breadth 的差异是否接近 0，还是方向不稳定。
6. QQQ / SPY / SMH 三类 benchmark 下结论是否一致。
7. Trial 2Y 区间是否缺少 2022 类压力样本，导致无法验证 breadth 的主要价值场景。

## Required Outputs

- `docs/research/norgate_2y_partial_evidence_review.md`
- `docs/research/norgate_platinum_decision_memo.md`
- `outputs/research_trends/norgate_trial/norgate_2y_partial_evidence_review.json`
- `outputs/research_trends/norgate_trial/norgate_2y_benchmark_consistency.csv`
- `outputs/research_trends/norgate_trial/norgate_2y_partial_evidence_conclusion_matrix.json`
- `inputs/research_reviews/norgate_2y_partial_evidence_review.yaml`
- `inputs/research_reviews/norgate_platinum_decision_memo.yaml`
- `inputs/research_reviews/norgate_2y_partial_evidence_conclusion_matrix.yaml`

## Conclusion Fields

- `local_signal_evidence_reason`: one of `insufficient_sample`,
  `no_incremental_value`, `metric_design_issue`, `inconclusive`。
- `trial_2y_feature_value`: one of `weak`, `moderate`, `strong`。
- `full_history_needed_for_final_answer`: true/false。
- `purchase_platinum_recommendation`: legacy/direct-trial field, one of `no`,
  `defer`, `yes`。
- `trial_based_purchase_recommendation`: one of `no`, `defer`, `yes`。
- `stress_window_paid_experiment_recommendation`: one of `no`, `defer`,
  `conditional_yes`, `not_reviewed`, `not_required`。
- `owner_decision_required`: true/false。
- `purchase_allowed`: true/false；当前默认必须为 false。
- `purchase_rationale`: one of `engineering_only`,
  `trial_no_incremental_value_stress_window_required`, `strong_trial_signal`,
  `weak_evidence`。
- Gate status 必须保持 false：
  `primary_window_validated=false`、`model_ready_for_2021_primary_window=false`、
  `reopen_gate_allowed=false`、`promotion_allowed=false`、
  `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## Policy Notes

该任务会引入 outlier dominance、bucket balance、event count、baseline delta
解释边界。所有会影响购买决策解释的阈值必须放在
`config/research/norgate_2y_partial_evidence_review_policy.yaml`，并标记为
trial review policy，不得作为 production threshold、promotion gate 或正式
first-layer acceptance rule。

## Acceptance Criteria

- 新增 CLI 能读取 TRADING-2267 artifacts，并在 benchmark price 不可用时 fail-closed
  或标记 blocked，不重跑完整 membership scan。
- 生成 evidence review、Platinum decision memo、benchmark consistency CSV 和更新后的
  conclusion matrix。
- 输出只包含日期级 / 聚合 summary，不提交完整 member symbol list 或 raw vendor price table。
- QQQ、SPY、SMH benchmark consistency 使用相同 2Y trial window 的聚合结果。
- 结论必须拆分 trial-based purchase answer 与 paid stress-window experiment owner
  review；当前 no-incremental-value 路径为
  `trial_based_purchase_recommendation=no`、
  `stress_window_paid_experiment_recommendation=conditional_yes`、
  `purchase_allowed=false`，并必须说明不是自动购买，也不是 primary-window
  validation。
- Report registry、artifact catalog、system flow、task register 和 research audit
  metadata 已更新。
- Focused pytest、Ruff、compileall、documentation/report/task checks、diff checks 和
  contract validation 通过。

## Progress Log

- 2026-06-28: Task created from owner request. Implementation starts with a
  policy-governed review runner that consumes TRADING-2267 artifacts and writes
  summary-only evidence review / Platinum decision memo artifacts.
- 2026-06-28: Implementation completed and moved to `VALIDATING`. Added
  `aits data norgate partial-evidence-review`,
  `norgate_2y_partial_evidence_review_policy`, evidence review, benchmark
  consistency CSV, Platinum decision memo, updated conclusion matrix, report
  registry, artifact catalog, system flow and focused tests. Real local run
  returned `NORGATE_2Y_PARTIAL_EVIDENCE_REVIEW_READY`: feature variation and
  bucket/event counts were sufficient and outcomes were not dominated by a few
  days, but baseline+breadth worsened false risk-off/risk-on rates and
  QQQ/SPY/SMH all had zero supporting horizons across 5D/10D/20D. Conclusion:
  `local_signal_evidence_reason=no_incremental_value`,
  `trial_2y_feature_value=weak`,
  `full_history_needed_for_final_answer=true`,
  `purchase_platinum_recommendation=yes`,
  `purchase_rationale=stress_window_required`, with
  `purchase_allowed_without_owner_approval=false` and all strategy gates false.
- 2026-06-28: TRADING-2269 reconciled the purchase wording. The same evidence
  now records `purchase_platinum_recommendation=no`,
  `trial_based_purchase_recommendation=no`,
  `stress_window_paid_experiment_recommendation=conditional_yes`,
  `purchase_rationale=trial_no_incremental_value_stress_window_required`,
  `owner_decision_required=true`, `purchase_allowed=false`, and all strategy
  gates false. This supersedes the earlier ambiguous `yes` wording.
- 2026-06-28: Validation passed: Ruff, compileall, focused Norgate partial
  evidence/effectiveness tests, report/documentation/task-register tests,
  research audit/governance tests and `contract-validation`. Runtime artifact:
  `outputs/validation_runtime/contract-validation_20260628T110122Z/test_runtime_summary.json`.
