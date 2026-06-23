# TRADING-785 to TRADING-789: Value Surface Walk-Forward Expansion
最后更新：2026-06-23

## Context

TRADING-780～784 已把 `value_surface` 从 batch-1 `CONTINUE` 推进到
controlled warning review：

- TRADING-780：`CONTROLLED_REVIEW_COMPLETE`，
  `controlled_expansion_review_decision=CONTINUE`，`warning_count=5`；
- TRADING-781：`SENSITIVITY_TESTED`，`validated_boundary_count=0`；
- TRADING-782：`PASS_WITH_WARNINGS`，`ledger_event_count=1`；
- TRADING-783：`PIVOT_DIRECTION_SELECTED`，
  `selected_pivot_direction=gbdt_value_surface_residual_model`；
- TRADING-784：`WATCHLIST_NOT_READY`，
  `regret_casebook_expansion_allowed=false`。

本批目标不是 promotion，而是把 horizon-conditioned value surface 放到更严格的
controlled walk-forward、utility ranking、forward evidence continuity、GBDT residual
diagnostic 和 regret activation recheck 下继续审计。

## Market Regime

- regime id：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default start：2022-12-01

所有结论必须声明实际 date range，不能把 2022-12-01 前历史当作默认 AI-cycle
结论窗口。

## Safety Boundary

本批全部输出只能是 controlled research / diagnostic review：

- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `broker_action=none`
- `production_effect=none`
- 不生成 official target weights、order ticket、paper-shadow eligibility 或 production
  review。

## Stage Breakdown

| Task | Scope | Acceptance Criteria | Status |
|---|---|---|---|
| TRADING-785 | Value Surface Controlled Walk-Forward Expansion | 输出 walk-forward windows、date/asset/horizon/regime coverage、benchmark comparison、warning taxonomy、sample concentration、negative controls 和 future leakage trap；结论只允许 `CONTINUE|WATCHLIST|DATA_REQUIRED|PAUSE|KILL` | VALIDATING |
| TRADING-786 | Value Surface Utility / Pareto Ranking Review | 输出 utility profile count、ranking flip count、Pareto candidate count、dominant metric、horizon cliff count；最多 `SENSITIVITY_TESTED`，`validated_boundary_count=0` | VALIDATING |
| TRADING-787 | Forward Evidence Daily Continuity | 输出 ledger event count、missing daily archive、append-only integrity、1d/5d/10d/20d/60d maturity 和 baseline/benchmark/value_surface coverage | VALIDATING |
| TRADING-788 | GBDT Value Surface Residual Diagnostic Prototype | 只解释 residual，不生成策略；输出 residual case count、by asset/horizon/regime、feature importance 和 hypothesis candidates | VALIDATING |
| TRADING-789 | Regret Casebook Activation Recheck | 复核 value-surface losing、benchmark disagreement、teacher/oracle better 和 regret type stability；条件不足时继续 watchlist | VALIDATING |

## Implementation Plan

1. 在 `config/research/controlled_strategy_next_stage_research.yaml` 增加 785～789
   诊断 policy 和所有 review thresholds，明确这些 threshold 是 audit sorting /
   controlled review gate，不是 validated investment boundary。
2. 在 `controlled_strategy_batch.py` 增加五个 runner，并复用现有 data-quality gate、
   value surface rows、utility profiles、forward ledger、regret gate 和 safety metadata。
3. 增加 CLI：
   - `aits research strategies value-surface-controlled-walk-forward-expansion`
   - `aits research strategies value-surface-utility-pareto-ranking-review`
   - `aits forward-evidence daily-continuity-review`
   - `aits research strategies gbdt-value-surface-residual-diagnostic-prototype`
   - `aits research strategies regret-casebook-activation-recheck`
4. 更新 report registry、artifact catalog、system flow 和 focused tests。
5. 运行真实 CLI 生成 artifacts，再执行 required validation。

## Acceptance Criteria

- TRADING-785 输出 `walk_forward_window_count`、`decision_date_count`、
  `asset_count`、`horizon_count`、`regime_count`、benchmark / negative control /
  future leakage trap results 和 allowed decision。
- TRADING-786 输出 `utility_profile_count`、`ranking_flip_count`、
  `pareto_candidate_count`、`dominant_metric_by_candidate`、`horizon_cliff_count`，
  且 `not_validated_utility_boundary=true`。
- TRADING-787 输出 append-only ledger、missing daily archive、maturity tracker 和
  output coverage。
- TRADING-788 输出 residual diagnostics 和 hypothesis candidates，且
  `strategy_signal_generated=false`。
- TRADING-789 不直接扩展 regret casebook；条件不足时保持 `WATCHLIST_NOT_READY`。
- 所有新增 artifacts 固定 safety flags，且不能 promotion / paper-shadow /
  production weight。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确 TRADING-785 是下一步
  主任务，前三项为主线，TRADING-788～789 为辅助诊断线。本批目标是更严格地验证
  value surface 是否仍值得继续，而不是晋级。
- 2026-06-21：实现 785～789 controlled review baseline 并转入 `VALIDATING`。
  真实 CLI run 输出：TRADING-785 `CONTROLLED_WALK_FORWARD_REVIEW_COMPLETE`、
  `controlled_walk_forward_decision=WATCHLIST`、`walk_forward_window_count=6`、
  `decision_date_count=72`、overall `mean_delta_vs_benchmark=-0.02366`、
  `value_surface_beats_benchmark_rate=0.742565`；TRADING-786
  `SENSITIVITY_TESTED`、`utility_profile_count=5`、`ranking_flip_count=1`、
  `pareto_candidate_count=32`、`horizon_cliff_count=30`；TRADING-787
  `PASS_WITH_WARNINGS`、`ledger_event_count=1`、`missing_daily_archive_count=0`、
  `continuity_ready_for_longer_review=false`；TRADING-788
  `DIAGNOSTIC_PROTOTYPE_COMPLETE`、`residual_case_count=23672`、
  `hypothesis_candidate_count=10`、`strategy_signal_generated=false`；TRADING-789
  `WATCHLIST_NOT_READY`、`value_surface_losing_case_count=692`、
  `benchmark_disagreement_case_count=830`、`teacher_oracle_better_case_count=0`、
  `stable_regret_type_count=9`、`regret_casebook_expansion_allowed=false`。所有输出继续
  固定 `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false` 和
  `lookahead_violation_count=0`。
- 2026-06-21：focused parallel pytest、Ruff、Black check、compileall、
  `git diff --check`、fast-unit、contract-validation 和 report-validation 已通过。
  Runtime artifacts：`outputs/validation_runtime/fast-unit_20260621T111023Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T111311Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T111559Z/test_runtime_summary.json`。
