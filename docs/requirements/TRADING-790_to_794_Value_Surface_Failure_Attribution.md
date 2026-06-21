# TRADING-790 to TRADING-794: Value Surface Failure Attribution

## Context

TRADING-785 controlled walk-forward review 首次明确了 value surface 的关键矛盾：

- `value_surface_beats_benchmark_rate=0.742565`
- overall `mean_delta_vs_benchmark=-0.02366`
- `controlled_walk_forward_decision=WATCHLIST`

这说明当前 horizon-conditioned value surface 可能有信号，但也可能存在 tail loss、
horizon cliff、utility ranking fragility 或 benchmark-relative downside。下一阶段不能继续
扩大样本，而应解释“多数时候赢但整体输”的原因。

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
| TRADING-790 | Value Surface Failure Attribution | 解释 high beat rate / negative mean delta，输出 winning/losing average delta、top losing cases、loss concentration、tail/cost/drawdown/downside attribution | VALIDATING |
| TRADING-791 | Horizon Cliff and Utility Ranking Stabilization Review | 检查 1d/5d/10d/20d ranking jumps、single-horizon action、utility profile cliff、horizon smoothing 和 Pareto frontier 方向；不能 validated boundary | VALIDATING |
| TRADING-792 | GBDT Residual Hypothesis Triage | 从 residual cases 中提炼预测错误场景、asset/horizon/regime/feature 解释、可解释修复规则和新 hypothesis | VALIDATING |
| TRADING-793 | Forward Evidence Continuity Extension | 继续追踪 daily archive continuity、missing archives、append-only integrity、1d/5d/10d/20d/60d maturity 和 output coverage | VALIDATING |
| TRADING-794 | Value Surface Direction Review | 汇总 790～793，给出 `CONTINUE_LOCAL_FIX|WATCHLIST|PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE|PIVOT_TO_PARETO_FRONTIER_POLICY|PIVOT_TO_TAIL_RISK_FILTER|KILL_CURRENT_VALUE_SURFACE_VERSION` | VALIDATING |

## Implementation Plan

1. 在 next-stage controlled research config 中新增 790～794 review policy，明确阈值只是
   diagnostic sorting / direction-review criteria。
2. 新增五个 runner 和 CLI：
   - `aits research strategies value-surface-failure-attribution`
   - `aits research strategies horizon-cliff-utility-ranking-stabilization-review`
   - `aits research strategies gbdt-residual-hypothesis-triage`
   - `aits forward-evidence continuity-extension`
   - `aits research strategies value-surface-direction-review`
3. 更新 report registry、artifact catalog、system flow 和 focused tests。
4. 运行真实 CLI artifacts，再执行项目 required validation。

## Acceptance Criteria

- TRADING-790 明确输出 loss severity / concentration / tail contribution，而不是只报告
  beat rate。
- TRADING-791 保持 `SENSITIVITY_TESTED`，`not_validated_utility_boundary=true`，
  不升级为 `VALIDATED_BOUNDARY`。
- TRADING-792 只能输出 diagnostic hypotheses，不生成策略 signal。
- TRADING-793 不能把 forward continuity 解释为当前策略好坏或 paper-shadow readiness。
- TRADING-794 不默认继续，必须根据 failure attribution、ranking stability、residual triage 和
  forward evidence 给出方向决策。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确下一阶段研究重点从
  “继续扩张 value surface”切换为“解释 value surface 为什么多数时候赢但整体输”。
- 2026-06-21：实现完成并转入 `VALIDATING`；真实 CLI run 输出 TRADING-790
  `FAILURE_ATTRIBUTION_COMPLETE`，case_count=2152、winning_case_count=1598、
  losing_case_count=554、winning_case_average_delta=0.004858、
  losing_case_average_delta=-0.105917、overall_mean_delta_vs_benchmark=-0.02366、
  tail_loss_contribution=0.46771、max_loss_concentration_share=0.923086，说明
  high beat rate 被少数高幅度亏损和集中亏损组抵消。TRADING-791 输出
  `SENSITIVITY_TESTED`、horizon_cliff_count=30、ranking_jump_count=0、
  utility_profile_cliff_count=30、validated_boundary_count=0；TRADING-792 输出
  `RESIDUAL_HYPOTHESIS_TRIAGED`、residual_case_count=23672、
  large_residual_case_count=12363、feature_explanation_count=6、
  repair_rule_candidate_count=5、strategy_signal_generated=false；TRADING-793 输出
  `PASS_WITH_WARNINGS`、ledger_event_count=1、missing_daily_archive_count=0、
  append_only_integrity_pass=true、output_coverage_present=true；TRADING-794 输出
  `DIRECTION_REVIEW_COMPLETE`、direction_decision=`PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE`、
  do_not_default_continue=true。所有输出固定 production_effect=none、broker_action=none、
  promotion_gate_allowed=false、paper_shadow_change_allowed=false、
  production_weight_change_allowed=false、lookahead_violation_count=0。
- 2026-06-21：focused parallel pytest、Ruff、Black check、compileall、git diff check、
  fast-unit、contract-validation 和 report-validation 已通过。Runtime artifacts：
  `outputs/validation_runtime/fast-unit_20260621T114501Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T114829Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T115154Z/test_runtime_summary.json`。
