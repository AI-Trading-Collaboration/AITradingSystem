# TRADING-800 to TRADING-804: Value Surface v2 Cost / Horizon / Holdout Review

## Context

TRADING-795～799 显示 regime-conditioned value surface 可以把 original mean delta 从
`-0.02366` 改善到接近 0，但代价是 `turnover_cost_not_worse=false`。这说明当前修复
可能用换手和成本换来了收益修复，不能继续扩大或进入 paper-shadow。

本批目标是把 value surface 主线从 regime-conditioned v1 推进到 controlled-only 的 v2
诊断：cost-aware、horizon-restricted、fallback-controlled，并通过 holdout 检查避免只对
已知失败窗口过拟合。

## Safety Boundary

本批全部输出仍为 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不进入 paper-shadow，不训练 GBDT/NN/RL 策略，不扩展 regret casebook，不生成 official
  target weights 或 order ticket。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-800 | Cost/Turnover-Aware Regime-Conditioned Value Surface | 比较 turnover penalty、action hysteresis、no-trade band、benchmark fallback、max action-change cap variants，输出收益、尾损、换手/成本、drawdown、flip/switch 指标 | VALIDATING |
| TRADING-801 | Long-Horizon Quarantine / Horizon Selection Review | 对 20d / 60d 做禁用、降权、regime-only、confirmation、shorter-horizon fallback 对比，输出 horizon loss/return/turnover/cliff matrix | VALIDATING |
| TRADING-802 | ai_after_chatgpt_full Regime Attribution Review | 单独拆解 top losing regime 的 asset/horizon/action/cluster、benchmark stability 和 value surface over-optimism | VALIDATING |
| TRADING-803 | Regime-Conditioned Walk-Forward Holdout | 做 leave-one-regime/horizon/asset-cluster/date-window-out，检查修复是否只对已知失败窗口过拟合 | VALIDATING |
| TRADING-804 | Value Surface v2 Controlled Review | 汇总 800～803，给出 `CONTINUE_TO_LARGER_CONTROLLED_RESEARCH|WATCHLIST|PIVOT_TO_TAIL_RISK_POLICY|PIVOT_TO_HORIZON_SELECTOR|KILL_VALUE_SURFACE|DATA_REQUIRED` | VALIDATING |

## Implementation Plan

1. 在 next-stage controlled research config 中新增 800～804 policy，所有阈值只作为
   controlled diagnostic / owner-review sorting 规则。
2. 新增五个 runner 和 CLI：
   - `aits research strategies cost-turnover-aware-regime-conditioned-value-surface`
   - `aits research strategies long-horizon-quarantine-selection-review`
   - `aits research strategies ai-after-chatgpt-full-regime-attribution-review`
   - `aits research strategies regime-conditioned-walk-forward-holdout`
   - `aits research strategies value-surface-v2-controlled-review`
3. 更新 report registry、artifact catalog、system flow 和 focused tests。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-800 不能只看 mean delta，必须同时输出 beat rate、losing avg、tail loss、
  turnover_delta、cost_delta、drawdown_delta、action_flip_count、horizon_switch_count。
- TRADING-801 必须单独输出 20d/60d disable-vs-downgrade comparison，并区分 horizon
  selector 问题与 action model 问题。
- TRADING-802 必须解释 top regime 中 asset/horizon/action/cluster 谁贡献亏损，以及 benchmark
  为什么更稳。
- TRADING-803 必须输出 leave-one-regime/horizon/asset-cluster/date-window-out 结果，防止已知失败
  regime 过拟合。
- TRADING-804 不能因为 mean delta 接近 0 就继续；只有 mean delta 改善、tail loss 下降、
  turnover/cost 不恶化、beat rate 不大幅下降同时成立，才可继续更大 controlled research。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确下一阶段主线是
  cost-aware + horizon-restricted + fallback-controlled value surface v2，仍禁止
  promotion、paper-shadow 和 production mutation。
- 2026-06-21：实现完成并进入 `VALIDATING`。真实 CLI run 输出：
  TRADING-800 `COST_TURNOVER_AWARE_VARIANTS_REVIEWED`，case_count=2152，
  variant_count=6，best_variant_by_v2_score=`regime_conditioned_benchmark_fallback`；
  original mean_delta_vs_benchmark=-0.02366，best mean_delta_vs_benchmark=-0.000033，
  tail_loss_reduced=true，beat_rate_retained=true，但 turnover_cost_not_worse=false，
  best turnover_delta=1235，cost_delta=0.6175。
- 2026-06-21：TRADING-801 `LONG_HORIZON_QUARANTINE_REVIEW_COMPLETE`，
  reviewed_horizon_count=2，best_comparison_variant=`regime_only_20d_60d`，
  tail_loss_reduction_best_variant=0.297009，horizon_selector_issue_likely=true。
  TRADING-802 `AI_REGIME_ATTRIBUTION_REVIEW_COMPLETE` 显示 target_regime=
  `ai_after_chatgpt_full`，regime_case_count=1640，top_loss_asset=AMD，
  top_loss_horizon=60d，top_loss_action=`hold_cash`，benchmark_more_stable=true。
- 2026-06-21：TRADING-803 `REGIME_CONDITIONED_HOLDOUT_REVIEW_COMPLETE`，
  holdout_case_count=14，holdout_pass_count=0，holdout_pass_rate=0，
  overfit_risk=`HIGH`。TRADING-804 最终 `VALUE_SURFACE_V2_CONTROLLED_REVIEW_COMPLETE`
  决策为 `PIVOT_TO_HORIZON_SELECTOR`；mean_delta_condition_met=false，
  tail_loss_condition_met=true，turnover_cost_condition_met=false，
  beat_rate_condition_met=true，holdout_condition_met=false。所有 safety flags
  仍为 false，`production_effect=none`、`broker_action=none`。
- 2026-06-21：验证通过。Focused parallel pytest、完整 controlled strategy pytest、
  docs/registry contract pytest、Ruff、Black check、compileall、fast-unit、
  contract-validation 和 report-validation 均通过。Runtime artifacts:
  `outputs/validation_runtime/fast-unit_20260621T130910Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T131402Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T131855Z/test_runtime_summary.json`。
