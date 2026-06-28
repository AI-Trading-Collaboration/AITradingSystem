# TRADING-2276 First-Layer Active Selection Rule Audit

最后更新：2026-06-28

## 背景

TRADING-2274/2275 将 performance gates 从 binary block 拆成分层 gate policy，但
active selection rule 仍显示 current accept=`0`。下一步必须单独验证 active selection
rule 是否真的提升完整 actual-path utility，还是在 performance gates 后继续过度过滤候选，
特别是是否把 `OWNER_REVIEW_REQUIRED` 候选继续当作 `BLOCKED` 处理。

## 范围

- 对 offline validation-ready candidates 执行 `no_active_selection`、
  `relaxed_active_selection`、`current_active_selection`、`strict_active_selection`。
- 比较 accepted / owner-review-required / blocked / rejected candidate count、
  best accepted / rejected utility、rejected candidate counterfactual utility、
  false risk-on/off delta、drawdown delta、turnover delta、cost-adjusted return delta、
  benchmark consistency delta、stress slice delta、beta dependency delta 和 TQQQ
  dependency delta。
- 检查 current selection 是否阻断 actual-path utility 最好的候选、是否与 gate policy
  v2 分层语义冲突、是否仍把 owner-review 风险当成 blocked 处理，以及 selection
  threshold 是否存在边界跳变。
- 输出 active selection rule audit artifacts。

## 输出

- `active_selection_rule_audit_report.md`
- `active_selection_ablation_matrix.json`
- `active_selection_counterfactual_report.json`
- `active_selection_threshold_sensitivity.json`
- `active_selection_recommended_policy.yaml`

## 边界

Active selection 放行不等于 promotion allowed；`OWNER_REVIEW_REQUIRED` 不等于
paper-shadow。promotion、paper-shadow、production 和 broker action 继续固定 false/none。

## 进展

- 2026-06-28：新增为 `READY`，依赖 TRADING-2275 gate policy v2 reconciliation 完成。
- 2026-06-28：按 owner 最新要求扩展验收范围并进入 `IN_PROGRESS`；本批必须通过可复现
  2274/2275 command/code path 再生 gate policy evidence，不能把 ignored `outputs/`
  artifact 当作唯一 source of truth。
- 2026-06-28：实现完成并转入 `VALIDATING`；新增
  `aits research trends first-layer-active-selection-rule-audit`，生成
  `active_selection_rule_audit_report.md`、`active_selection_ablation_matrix.json`、
  `active_selection_counterfactual_report.json`、`active_selection_threshold_sensitivity.json`
  和 `active_selection_recommended_policy.yaml`。真实 run：current active selection
  accept=`0`；no/relaxed selection 为 accepted=`1`、owner_review=`1`、blocked=`2`；
  current/strict selection blocked=`4`。Best actual-path candidate `wf_504d_baseline`
  在 gate policy v2 下是 `OWNER_REVIEW_REQUIRED`，但被 current selection 当作
  `BLOCKED`；结论 `active_selection_marginal_utility=negative`，
  recommended_action=`split_selection_and_promotion`。promotion/paper-shadow/production
  继续 false，broker_action=`none`。
- 2026-06-28：验证通过 Ruff、compileall、focused parallel pytest（48 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T144247Z/test_runtime_summary.json`。
