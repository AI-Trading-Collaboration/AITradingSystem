# TRADING-2276 First-Layer Active Selection Rule Audit

最后更新：2026-06-28

## 背景

TRADING-2274/2275 将 performance gates 从 binary block 拆成分层 gate policy，但
active selection rule 仍显示 current accept=`0`。下一步必须单独验证 active selection
rule 是否真的提升完整 actual-path utility，还是在 performance gates 后继续过度过滤候选。

## 范围

- 对 offline validation-ready candidates 执行 `no_active_selection`、
  `relaxed_active_selection`、`current_active_selection`、`strict_active_selection`。
- 比较 accepted candidate count、owner-review-required count、rejected candidate
  counterfactual utility、best rejected utility、false risk-on/off delta、drawdown delta、
  turnover delta 和 benchmark consistency delta。
- 输出 active selection rule audit artifacts。

## 输出

- `active_selection_rule_audit_report.md`
- `active_selection_ablation_matrix.json`
- `active_selection_recommended_policy.yaml`

## 边界

Active selection 放行不等于 promotion allowed；promotion、paper-shadow、production 和
broker action 继续固定 false/none。

## 进展

- 2026-06-28：新增为 `READY`，依赖 TRADING-2275 gate policy v2 reconciliation 完成。
