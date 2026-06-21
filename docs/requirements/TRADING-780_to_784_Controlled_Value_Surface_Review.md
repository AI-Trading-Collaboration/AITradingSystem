# TRADING-780 to TRADING-784: Controlled Value Surface Review

## Background

TRADING-775～779 已完成 next-stage controlled research baseline：

- TRADING-775 value surface expansion：`PASS_WITH_WARNINGS`
- TRADING-776 utility boundary audit：`SENSITIVITY_TESTED`，`validated_boundary_count=0`
- TRADING-777 forward maturity tracker：`PASS_WITH_WARNINGS`，`ledger_event_count=1`
- TRADING-778 GBDT pivot review：`PIVOT_REVIEW_READY`，`model_run_executed=false`
- TRADING-779 regret casebook expansion gate：`WATCHLIST_NOT_READY`

本批任务解释 warning、评估 value surface 是否继续受控扩大、检查 utility ranking
robustness、补充 forward evidence continuity、选择 GBDT pivot 方向，并定义从 value
surface failure cases 激活 regret casebook 的输入条件。仍不得 promotion、paper-shadow、
production review、official weight change 或 broker/order。

## Market Regime

- regime：`ai_after_chatgpt`
- anchor event：ChatGPT public launch on 2022-11-30
- default backtest start：2022-12-01

输出必须披露实际 requested date range。pre-2022 数据只能用于 warm-up、stress
test 或 regime comparison，不得作为默认 AI-cycle 结论窗口。

## Safety Boundary

所有输出固定：

- `production_effect=none`
- `broker_action=none`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `lookahead_violation_count=0`

TRADING-782 继续依赖 cached market / macro data，必须先复用 `aits validate-data`
同一质量门禁并在输出中披露 `data_foundation_status`。

## Heuristic Governance

本批继续使用 `config/research/controlled_strategy_next_stage_research.yaml`。
新增 warning taxonomy、sample concentration floor、forward continuity policy、
GBDT pivot selection criteria 和 regret activation inputs 均为 controlled research
review baseline，不是 validated utility boundary 或 promotion policy。

## Stage Breakdown

| Task | Stage | Goal | Status |
|---|---|---|---|
| TRADING-780 | Value Surface Warning Triage & Controlled Expansion Review | 解释 TRADING-775 `PASS_WITH_WARNINGS`，输出 warning taxonomy、decision-date breakdown、by asset/horizon/regime/cluster、benchmark、negative controls、turnover/cost/drawdown、utility ranking stability 和 sample concentration，并给出 `CONTINUE|WATCHLIST|DATA_REQUIRED|PAUSE|KILL` 决策 | VALIDATING |
| TRADING-781 | Utility Ranking Robustness / Pareto Frontier Audit | 检查 utility profile ranking reversal、维度主导、Pareto frontier 稳定性，并确认 utility boundary 只适合 diagnostic | VALIDATING |
| TRADING-782 | Forward Evidence Daily Continuity & Maturity Tracker | 检查 daily dry-run 连续性、missing archive、append-only integrity、1d/5d/10d/20d/60d maturity 和 baseline/benchmark/value_surface coverage | VALIDATING |
| TRADING-783 | GBDT Pivot Direction Selection | 在不训练模型的前提下选择 pivot 方向，并为每个候选方向记录 MVE、required data、failure mode、kill criteria 和区别于 action-utility model 的原因 | VALIDATING |
| TRADING-784 | Regret Casebook Activation Inputs from Value Surface Failures | 从 value surface losing / benchmark disagreement / oracle-teacher better / false-risk-off / missed-upside cases 定义 activation inputs；条件不足时继续 watchlist | VALIDATING |

## Implementation Plan

1. 扩展 next-stage policy config，记录 780～784 的 warning、robustness、continuity、
   pivot selection 和 regret activation policy。
2. 在 `ai_trading_system.controlled_strategy_batch` 中新增 controlled review runners：
   - `run_value_surface_warning_triage_review`
   - `run_utility_ranking_robustness_pareto_audit`
   - `run_forward_evidence_daily_continuity_maturity_tracker`
   - `run_gbdt_pivot_direction_selection`
   - `run_regret_activation_inputs_from_value_surface_failures`
3. 新增 CLI：
   - `aits research strategies value-surface-warning-triage-review`
   - `aits research strategies utility-ranking-robustness-pareto-audit`
   - `aits forward-evidence daily-continuity-maturity-tracker`
   - `aits research strategies gbdt-pivot-direction-selection`
   - `aits research strategies regret-activation-inputs-from-value-surface-failures`
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和
   `docs/system_flow.md`。
5. 新增 focused tests，覆盖 review decision enum、utility status cap、daily continuity、
   no model training、watchlist activation 和 safety boundary。
6. 运行真实 CLI 生成 780～784 artifacts，再执行 required validation。

## Acceptance Criteria

- TRADING-780 输出 `warning_taxonomy_present=true`
- TRADING-780 输出 `controlled_expansion_review_decision` 且只能为
  `CONTINUE|WATCHLIST|DATA_REQUIRED|PAUSE|KILL`
- TRADING-780 输出 decision-date、asset、horizon、regime、cluster、benchmark、
  controls、turnover/cost/drawdown、utility ranking stability 和 sample concentration
- TRADING-781 输出 `status=SENSITIVITY_TESTED`
- TRADING-781 输出 `not_validated_utility_boundary=true`
- TRADING-781 输出 ranking reversal、dominant dimension 和 Pareto frontier audit
- TRADING-782 输出 daily continuity、missing archive、append-only integrity、maturity
  by 1d/5d/10d/20d/60d 和 output coverage
- TRADING-783 输出 selected pivot direction、MVE、required data、failure mode、kill
  criteria 和 difference from previous action-utility model，且 `model_run_executed=false`
- TRADING-784 输出 activation criteria；条件不足时 `regret_activation_ready=false`、
  `regret_state_machine_status=WATCHLIST`
- 所有输出固定 safety boundary，不允许 promotion、paper-shadow、production 或 broker。

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确 TRADING-780 为
  最优先，目标是解释 TRADING-775 warnings 并判断 value surface 是否继续扩大；
  TRADING-781～784 只做 robustness / continuity / pivot-selection / activation-input
  review，不训练新模型，不硬扩 regret state machine。
- 2026-06-21：实现 780～784 controlled review baseline 并转入 `VALIDATING`。
  真实 CLI run 输出：TRADING-780 `CONTROLLED_REVIEW_COMPLETE`、
  `controlled_expansion_review_decision=CONTINUE`、`warning_count=5`；TRADING-781
  `SENSITIVITY_TESTED`、`validated_boundary_count=0`；TRADING-782
  `PASS_WITH_WARNINGS`、`ledger_event_count=1`、`missing_daily_archive_count=0`、
  `append_only_integrity_pass=true`；TRADING-783 `PIVOT_DIRECTION_SELECTED`、
  `selected_pivot_direction=gbdt_value_surface_residual_model`、
  `model_run_executed=false`；TRADING-784 `WATCHLIST_NOT_READY`、
  `value_surface_losing_case_count=692`、`benchmark_disagreement_case_count=830`、
  `regret_activation_ready=false`。所有输出继续固定 `production_effect=none`、
  `broker_action=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false` 和
  `lookahead_violation_count=0`。
- 2026-06-21：focused parallel pytest、Ruff、Black check、compileall、
  `git diff --check`、fast-unit、contract-validation 和 report-validation 已通过。
  Runtime artifacts：`outputs/validation_runtime/fast-unit_20260621T102929Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T103150Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T103406Z/test_runtime_summary.json`。
