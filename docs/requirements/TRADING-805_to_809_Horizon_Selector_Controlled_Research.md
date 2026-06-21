# TRADING-805 to TRADING-809: Horizon Selector Controlled Research

## Context

TRADING-804 的真实 controlled review 给出 `PIVOT_TO_HORIZON_SELECTOR`：
value surface v2 能改善 tail loss 和 beat rate，但 `turnover_cost_not_worse=false`，
且 TRADING-803 holdout_pass_rate=0。当前不能把 value surface 作为直接主策略继续扩大，
应降级为 action scoring submodule，并把主研究线转向 horizon selector、long-horizon
quarantine 和 cost-aware fallback。

## Safety Boundary

本批全部输出仍为 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不进入 paper-shadow，不训练 GBDT/NN/RL 策略，不继续微调 utility boundary，不生成
  official target weights 或 order ticket。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-805 | Horizon Selector Problem Contract | 定义 candidate horizons、horizon status、selector output schema、review_interval 和 invalidation_condition；说明 target_horizon 不是持有期承诺 | VALIDATING |
| TRADING-806 | Long-Horizon Quarantine / Fallback Review | 对 baseline、disable 60d、disable 20d/60d、confirmation、fallback 5d/10d、fallback benchmark 做 controlled comparison | VALIDATING |
| TRADING-807 | Horizon Selector Controlled Prototype | 第一版规则 selector，输出 horizon_decision_by_date、confidence、quarantine/fallback counts、tail_loss_after_selector、cost_after_selector | VALIDATING |
| TRADING-808 | Cost-Aware Horizon Hysteresis | 增加 horizon switch hysteresis / no-trade band / high-cost flip limits / long-horizon confirmation | VALIDATING |
| TRADING-809 | Horizon Selector Holdout Review | 做 leave-one-regime/horizon/asset-cluster/date-window-out，并给出受限决策 enum | VALIDATING |

## Implementation Plan

1. 在 `config/research/controlled_strategy_next_stage_research.yaml` 中新增 TRADING-805～809
   policy；所有阈值都是 controlled diagnostic / owner-review sorting 规则。
2. 新增五个 runner 和 CLI：
   - `aits research strategies horizon-selector-problem-contract`
   - `aits research strategies long-horizon-quarantine-fallback-review`
   - `aits research strategies horizon-selector-controlled-prototype`
   - `aits research strategies cost-aware-horizon-hysteresis`
   - `aits research strategies horizon-selector-holdout-review`
3. 更新 report registry、artifact catalog、system flow 和 focused tests。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-805 必须显式列出 1d / 5d / 10d / 20d / 60d，horizon status 只能为
  `ALLOWED|DOWNWEIGHTED|QUARANTINED|FALLBACK_ONLY`，selector output 必须包含
  allowed_horizons、preferred_horizon、fallback_horizon、horizon_confidence、
  invalidation_condition 和 review_interval。
- TRADING-806 必须输出 mean_delta_vs_benchmark、tail_loss_contribution、losing_avg、
  beat_rate_retention、turnover_delta、cost_delta、drawdown_delta 和 holdout pass rate。
- TRADING-807 必须保持 controlled-only，不训练 ML；规则可以处理 20d/60d quarantine、
  horizon cliff、uncertainty、turnover/cost pressure。
- TRADING-808 必须证明 horizon hysteresis 对 horizon_switch_count、action_flip_count、
  turnover_delta、cost_delta、utility_lost_to_hysteresis 和 tail_loss_reduction 的影响。
- TRADING-809 只有 selector 在 holdout 中降低 tail loss 且 cost 不恶化时才允许
  `CONTINUE`；否则只能 watchlist、pivot、kill 或 data-required。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确当前主线从直接
  value surface 切换为 horizon selector + long-horizon quarantine + cost-aware fallback，
  且继续禁止 promotion、paper-shadow 和 production mutation。
- 2026-06-21：实现完成并进入 `VALIDATING`。真实 CLI run 输出：
  TRADING-805 `HORIZON_SELECTOR_CONTRACT_DEFINED`，candidate_horizon_count=5，
  allowed_horizon_count=3，preferred_horizon=5d，fallback_horizon=5d，
  target_horizon_is_holding_commitment=false，regime_change_can_invalidate_horizon=true。
- 2026-06-21：TRADING-806 `LONG_HORIZON_FALLBACK_REVIEW_COMPLETE`，case_count=2152，
  variant_count=6，best_variant_by_tail_loss=`long_horizon_fallback_to_5d_10d`，
  tail_loss_reduction=0.33586，beat_rate_retention=1.047559，
  mean_delta_vs_benchmark=-0.006597，但 turnover_delta=7、cost_delta=0.0035、
  holdout_pass_rate=0，best_variant_turnover_cost_not_worse=false。
- 2026-06-21：TRADING-807 `HORIZON_SELECTOR_PROTOTYPE_REVIEWED`，
  decision_row_count=250，quarantined_horizon_count=528，fallback_count=1159，
  tail_loss_after_selector=0.271052，cost_after_selector=0.427，model_run_executed=false。
  TRADING-808 `COST_AWARE_HORIZON_HYSTERESIS_REVIEWED`，horizon_switch_count=1592，
  action_flip_count=693，turnover_delta=1009，cost_delta=0.5045，
  utility_lost_to_hysteresis=2.3166，tail_loss_reduction=-0.006032。
- 2026-06-21：TRADING-809 `HORIZON_SELECTOR_HOLDOUT_REVIEW_COMPLETE`，
  horizon_selector_decision=`KILL_VALUE_SURFACE_AS_ACTION_POLICY`，holdout_case_count=14，
  holdout_pass_count=0，holdout_pass_rate=0，tail_loss_condition_met=false，
  turnover_cost_condition_met=false。所有 safety flags 仍为 false，
  `production_effect=none`、`broker_action=none`。
- 2026-06-21：验证通过。Focused parallel pytest、完整 controlled strategy pytest、
  docs/registry contract pytest、Ruff、Black check、compileall、fast-unit、
  contract-validation 和 report-validation 均通过。Runtime artifacts:
  `outputs/validation_runtime/fast-unit_20260621T135746Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T140328Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T140912Z/test_runtime_summary.json`。
