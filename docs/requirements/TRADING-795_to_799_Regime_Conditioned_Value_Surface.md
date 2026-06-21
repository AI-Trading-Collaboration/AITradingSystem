# TRADING-795 to TRADING-799: Regime-Conditioned Value Surface

## Context

TRADING-790～794 已确认当前无条件 value surface 存在结构性问题：

- high beat rate 与 negative mean delta 并存；
- losing_case_average_delta 明显大于 winning_case_average_delta；
- tail_loss_contribution 与 max_loss_concentration_share 显示尾部亏损和集中亏损；
- horizon_cliff_count 仍高；
- TRADING-794 direction decision 为 `PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE`。

本批目标不是继续扩大样本，也不是把 value surface 推向 paper-shadow，而是把当前主线从
unconditional value surface 改造成 controlled-only 的条件化研究协议，并验证 tail-loss
guardrail / benchmark fallback 是否能显著改善 payoff asymmetry。

## Safety Boundary

本批全部输出仍为 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不进入 paper-shadow，不训练 GBDT 策略，不扩展 regret casebook，不生成 official
  target weights 或 order ticket。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-795 | Regime-Conditioned Value Surface Design | 定义 regime-conditioned value surface protocol、regime variables、tail-loss regime definitions、allowed action changes by regime、benchmark fallback rules 和 controlled-only validation plan | VALIDATING |
| TRADING-796 | Tail-Loss Guardrail and Fallback Policy | 对比 original、regime-conditioned、tail-loss guarded、benchmark fallback variants，输出 mean delta、beat rate、losing avg、tail loss、concentration、turnover/cost/drawdown | VALIDATING |
| TRADING-797 | Regime / Horizon Loss Attribution Matrix | 拆解 554 losing cases 的 loss_by_regime/asset/horizon/action/cluster/utility_profile/date_window，说明 max_loss_concentration_share 来源 | VALIDATING |
| TRADING-798 | GBDT Residual Hypothesis Triage for Regime Conditioning | 从 residual cases 中输出 top residual features、large residual regimes/horizons/assets、residual sign classification 和 regime-conditioning hypotheses；不训练策略 | VALIDATING |
| TRADING-799 | Regime-Conditioned Value Surface Controlled Review | 汇总 795～798，给出 `CONTINUE|WATCHLIST|KILL_CURRENT_VALUE_SURFACE|PIVOT_TO_TAIL_RISK_POLICY|PIVOT_TO_BENCHMARK_FALLBACK|DATA_REQUIRED` | VALIDATING |

## Implementation Plan

1. 在 next-stage controlled research config 中新增 795～799 policy，所有阈值只作为
   diagnostic sorting / controlled ablation 规则。
2. 新增五个 runner 和 CLI：
   - `aits research strategies regime-conditioned-value-surface-design`
   - `aits research strategies tail-loss-guardrail-fallback-policy`
   - `aits research strategies regime-horizon-loss-attribution-matrix`
   - `aits research strategies gbdt-residual-hypothesis-regime-conditioning`
   - `aits research strategies regime-conditioned-value-surface-controlled-review`
3. 更新 report registry、artifact catalog、system flow 和 focused tests。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-795 必须明确哪些 regime 保留 value surface、哪些 regime fallback benchmark、
  哪些 regime 只允许 low-risk action，以及哪些 horizon 需要禁用或降权。
- TRADING-796 必须以相同 case universe 对比 original / conditioned / guarded / fallback，
  不能只报告单一版本。
- TRADING-797 必须说明 losing cases 的集中来源；若集中在少数 regime / horizon / asset，
  输出应保留可修复假设，若广泛分布则标记结构性风险。
- TRADING-798 必须保持 diagnostic-only，不生成策略 signal 或直接 action policy。
- TRADING-799 不默认继续，必须基于 mean delta、losing avg、tail loss、beat rate、
  turnover/cost/drawdown 变化给出 controlled review 决策。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确当前阶段从解释 failure
  进入 regime-conditioned value surface / tail-loss guardrail / benchmark fallback 设计与
  controlled review，仍禁止 promotion、paper-shadow 和 production mutation。
- 2026-06-21：实现完成并转入 `VALIDATING`；真实 CLI run 输出 TRADING-795
  `REGIME_CONDITIONED_PROTOCOL_DEFINED`、regime_variable_count=6、
  tail_loss_regime_count=2、disabled_or_downweighted_horizon_count=4；
  TRADING-796 `GUARDRAIL_FALLBACK_POLICY_REVIEWED`、case_count=2152、
  variant_count=4、original mean_delta_vs_benchmark=-0.02366、
  best_variant_by_mean_delta=`regime_conditioned_value_surface`、
  best_variant_mean_delta_vs_benchmark=-0.000033、tail-loss guarded mean=-0.011846、
  tail_loss_guardrail_reduces_tail_loss=true；TRADING-797
  `LOSS_ATTRIBUTION_MATRIX_COMPLETE`、losing_case_count=554、
  losing_case_average_delta=-0.105917、loss_distribution_assessment=`CONCENTRATED_REPAIRABLE`，
  top action concentration 为 `hold_cash` loss_share=1.0，top regime concentration 为
  `ai_after_chatgpt_full` loss_share=0.923086，top horizon concentration 为 60d
  loss_share=0.380702 与 20d loss_share=0.330245；TRADING-798
  `RESIDUAL_REGIME_HYPOTHESES_TRIAGED`、residual_case_count=23672、
  large_residual_case_count=12363、top_residual_feature=`horizon`；TRADING-799
  `REGIME_CONDITIONED_CONTROLLED_REVIEW_COMPLETE`、controlled_review_decision=`WATCHLIST`、
  best_variant_by_mean_delta=`regime_conditioned_value_surface`、mean_delta_improved=true、
  tail_loss_reduced=true、beat_rate_retained=true、turnover_cost_not_worse=false。所有输出固定
  production_effect=none、broker_action=none、promotion_gate_allowed=false、
  paper_shadow_change_allowed=false、production_weight_change_allowed=false、
  lookahead_violation_count=0。
- 2026-06-21：focused parallel pytest、完整 controlled strategy pytest、Ruff、Black check、
  compileall、git diff check、fast-unit、contract-validation 和 report-validation 已通过。
  Runtime artifacts：
  `outputs/validation_runtime/fast-unit_20260621T122408Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T122809Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T123221Z/test_runtime_summary.json`。
