# TRADING-810 to TRADING-815: Tail-Risk Policy Family
最后更新：2026-06-23

## Context

TRADING-809 的真实 holdout review 给出
`KILL_VALUE_SURFACE_AS_ACTION_POLICY`，holdout_pass_rate=0，tail_loss_condition_met=false，
turnover_cost_condition_met=false。当前证据足以说明 value surface 仍有诊断价值，但不适合
作为直接仓位、action 或 horizon policy 继续扩张。

下一阶段研究问题改为 benchmark-first tail-risk policy family：默认跟随 benchmark /
simple trend / static allocation，只有风险信号明确时降风险，只有确认信号足够强且成本可控时
才允许偏离 benchmark。

## Safety Boundary

本批全部输出仍为 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不进入 paper-shadow，不训练 GBDT/NN/RL 直接策略，不继续无条件 value surface
  expansion，不继续微调 horizon selector 或 utility boundary，不生成 official target
  weights 或 order ticket。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-810 | Value Surface Policy Kill & Diagnostic Downgrade | 正式输出 action_policy_allowed=false、promotion_gate_allowed=false，并限制 allowed_uses 为 diagnostic/residual/tail-loss/horizon-risk/fallback-trigger | VALIDATING |
| TRADING-811 | Benchmark-First Tail-Risk Policy Contract | 定义 base_policy、allowed_deviation、risk_downshift_condition、risk_recovery_condition、max_turnover_budget、fallback_policy 和 review_interval | VALIDATING |
| TRADING-812 | Tail-Loss Avoidance Classifier Prototype | 生成 large_loss_case、tail_loss_case、benchmark_underperformance_case、long_horizon_failure_case 标签；输出只用于 gate value surface/aggressive action，不直接决定仓位 | VALIDATING |
| TRADING-813 | Conservative Horizon Risk Filter | 将 horizon 研究改成风险过滤器：1d/5d/10d 默认可用，20d quarantine，60d fallback-only，长 horizon 只在低风险 regime 才可启用 | VALIDATING |
| TRADING-814 | Benchmark Fallback / Drawdown Guard Controlled Prototype | 构造简单保守 fallback policy，并比较 mean_delta、tail_loss_contribution、losing_avg、max_drawdown、turnover、cost、beat_rate_retention | VALIDATING |
| TRADING-815 | Controlled Review of Tail-Risk Policy Family | 汇总 811～814，决策只能为 CONTINUE/WATCHLIST/KILL/PIVOT/DATA_REQUIRED | VALIDATING |

## Implementation Plan

1. 在 `config/research/controlled_strategy_next_stage_research.yaml` 中新增 TRADING-810～815
   policy；所有阈值都是 controlled diagnostic / owner-review sorting 规则，不是生产风控边界。
2. 新增六个 runner 和 CLI：
   - `aits research strategies value-surface-policy-kill-diagnostic-downgrade`
   - `aits research strategies benchmark-first-tail-risk-policy-contract`
   - `aits research strategies tail-loss-avoidance-classifier-prototype`
   - `aits research strategies conservative-horizon-risk-filter`
   - `aits research strategies benchmark-fallback-drawdown-guard-prototype`
   - `aits research strategies tail-risk-policy-family-controlled-review`
3. 更新 report registry、artifact catalog、system flow 和 focused tests，防止后续把 value
   surface 重新误读为 action policy。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-810 必须把 `action_policy_allowed=false` 和 `promotion_gate_allowed=false` 固化到
  artifact，并列出 disallowed uses：direct_action_policy、horizon_selector_policy、
  paper_shadow_signal、production_weight_policy、broker_order_instruction。
- TRADING-811 必须体现 benchmark-first：base policy 是 benchmark/simple trend/static
  allocation，allowed deviation 只允许 risk_downshift、drawdown_guard、cash_fallback 或
  low-cost confirmed deviation。
- TRADING-812 不能直接输出 action/position；classifier labels 只能作为是否允许 value surface /
  aggressive action 生效的 gate。
- TRADING-813 不能继续做最优 horizon selector；它只能阻断危险 horizon，20d 默认
  `QUARANTINED`，60d 默认 `FALLBACK_ONLY`。
- TRADING-814 必须证明 fallback/drawdown guard 对 tail loss、drawdown、turnover/cost 和
  beat-rate retention 的影响；若简单 fallback 比复杂 value surface 更稳，后续应优先保留 fallback。
- TRADING-815 只有在 tail loss 降低、turnover/cost 不显著恶化、holdout 有效、优于 simple
  benchmark 且保持解释性时才允许 `CONTINUE`；否则必须 watchlist、pivot、kill 或 data-required。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确当前主线从直接 value
  surface / horizon selector 改为 benchmark-first tail-risk-aware fallback policy family，
  且继续禁止 promotion、paper-shadow 和 production mutation。
- 2026-06-21：实现完成并进入 `VALIDATING`。真实 CLI run 输出：
  TRADING-810 `VALUE_SURFACE_POLICY_KILLED_DIAGNOSTIC_DOWNGRADE_COMPLETE`，
  action_policy_allowed=false，allowed_use_count=5，
  prior_horizon_selector_decision=`KILL_VALUE_SURFACE_AS_ACTION_POLICY`。
- 2026-06-21：TRADING-811 `BENCHMARK_FIRST_TAIL_RISK_POLICY_CONTRACT_DEFINED`，
  base_policy=`benchmark_or_simple_trend_static_allocation`，allowed_deviation_count=4，
  risk_downshift_condition_count=4，fallback_policy=`benchmark_first`，
  review_interval=`daily`。TRADING-812 `TAIL_LOSS_AVOIDANCE_CLASSIFIER_PROTOTYPED`，
  case_count=2152，large_loss_case_count=286，tail_loss_case_count=56，
  benchmark_underperformance_case_count=554，long_horizon_failure_case_count=192，
  gate_block_count=554，strategy_signal_generated=false。
- 2026-06-21：TRADING-813 `CONSERVATIVE_HORIZON_RISK_FILTER_REVIEWED`，
  allowed_horizon_count=3，quarantined_horizon_count=1，fallback_only_horizon_count=1，
  fallback_count=528，tail_loss_after_filter=0.314787，
  selector_mode=`risk_filter_not_optimal_horizon_selector`。
- 2026-06-21：TRADING-814 `BENCHMARK_FALLBACK_DRAWDOWN_GUARD_REVIEWED`，
  variant_count=5，best_variant_by_tail_loss=`tail_risk_benchmark_fallback`，
  best_variant_tail_loss_reduction=1.0，best_variant_turnover_cost_not_worse=true，
  holdout_pass_rate=1.0，best mean_delta_vs_benchmark=0.003607。TRADING-815
  `TAIL_RISK_POLICY_FAMILY_CONTROLLED_REVIEW_COMPLETE`，
  tail_risk_policy_decision=`CONTINUE`，tail_loss_condition_met=true，
  turnover_cost_condition_met=true，holdout_condition_met=true，
  explainability_condition_met=true。所有 safety flags 仍为 false，
  `production_effect=none`、`broker_action=none`。
- 2026-06-21：验证通过。Focused parallel pytest、完整 controlled strategy pytest、
  Ruff、Black check、compileall、fast-unit、contract-validation 和 report-validation 均通过。
  Runtime artifacts:
  `outputs/validation_runtime/fast-unit_20260621T144110Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T144914Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T145722Z/test_runtime_summary.json`。
