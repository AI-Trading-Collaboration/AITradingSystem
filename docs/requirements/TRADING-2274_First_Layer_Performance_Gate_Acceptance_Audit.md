# TRADING-2274 First-Layer Performance Gate Acceptance Audit

最后更新：2026-06-28

## 背景

TRADING-2270～2273 已经生成 first-layer current state、failure taxonomy、
objective validation spec 和 offline challenger matrix。TRADING-2274 承接这些
artifacts，专门审计 current first-layer performance gates 是否真的提升完整两层
actual-path utility，而不是只过滤局部 first-layer diagnostic 指标。

## 范围

- 读取 first-layer current state report、failure taxonomy、objective validation spec、
  challenger matrix、offline validation-ready challenger rows 和 current promotion gate
  policy。
- 对每个 performance gate 执行 `no_gate`、`relaxed_gate`、`current_gate`、
  `strict_gate` 单变量反事实 acceptance。
- 对 accepted / rejected frozen actual-path candidates 汇总完整两层 actual-path utility
  proxy、return/drawdown/Sharpe/Calmar/turnover/cost-adjusted deltas 和 slice / dependency
  evidence status。
- 明确区分已有 candidate-level actual-path 回测、offline challenger readiness、以及
  beta/TQQQ/2023+ dependency diagnostic evidence，避免把证据不足项伪装成收益型贡献。

## 必须审计的 Gates

- `actual_path_improved_probe_count_min`
- `no_major_regression_in_defensive_probe`
- `2022_slice_not_worse_than_flat_reference`
- `net_of_cost_not_worse`
- `not_2023_plus_only`
- `not_beta_dependency`
- `not_tqqq_dependency`
- `probability_threshold_0_55`
- `probability_threshold_0_60`
- `all_slices_not_worse`
- `no_slice_regression`

## 非目标

- 不训练 challenger model。
- 不把 offline validation-ready challenger rows 当作 actual-path validated candidates。
- 不修改 active first-layer selection rule、promotion gate policy 或 production boundary。
- 不允许收益表现 gate 豁免 PIT / no-lookahead / data quality / actual-path /
  owner approval / production hard gates。
- 不恢复 promotion、paper-shadow、production 或 broker action。

## 验收标准

- 生成并更新 `docs/research/gate_acceptance_audit_report.md`。
- 生成 `outputs/research_trends/first_layer_performance_gate_audit/gate_ablation_matrix.json`。
- 生成 `outputs/research_trends/first_layer_performance_gate_audit/threshold_sensitivity_report.json`。
- 生成 `outputs/research_trends/first_layer_performance_gate_audit/rejected_candidate_counterfactual_report.json`。
- 生成 `outputs/research_trends/first_layer_performance_gate_audit/recommended_gate_policy.yaml`。
- 每个 gate 必须披露 `gate_marginal_utility`、`gate_failure_mode_reduced`、
  `opportunity_cost`、`threshold_stability` 和 `recommended_action`。
- `recommended_action` 只能使用：
  `keep_as_hard_gate`、`keep_as_performance_gate`、`relax_threshold`、
  `tighten_threshold`、`convert_to_owner_review`、`convert_to_score_penalty`、
  `remove_gate`。
- 所有输出必须继续固定 `promotion_allowed=false`、`paper_shadow_allowed=false`、
  `production_allowed=false`、`broker_action=none`。

## 进展

- 2026-06-28：新增并进入 `IN_PROGRESS`；本批把既有
  `first-layer-performance-gate-audit` 升级为 TRADING-2274 gate acceptance audit，
  覆盖 mandatory gate 列表和 TRADING-2270～2273 输入 artifacts。实现必须披露
  challenger rows 尚未 actual-path 回测的限制，不改变 active policy。
- 2026-06-28：实现完成并转入 `VALIDATING`；真实 run 生成 5 个指定 artifacts。
  Mandatory performance gate current accept count=`1/4`，active selection rule accept
  count 仍为 `0`；`no_major_regression_in_defensive_probe` marginal utility=`positive`，
  recommended_action=`keep_as_hard_gate`；`not_2023_plus_only` marginal utility=`negative`，
  opportunity_cost=`0.070283`，recommended_action=`convert_to_owner_review`。
  `not_beta_dependency`、`not_tqqq_dependency`、`probability_threshold_0_55`、
  `probability_threshold_0_60` 和 `all_slices_not_worse` 因 candidate-level evidence
  不足或 all-slice actual-path 不完整为 `inconclusive`。Offline challenger
  validation-ready rows=`4`，complete two-layer challenger actual-path rows=`0`；
  promotion/paper-shadow/production/broker 保持 false/none/BLOCKED。
