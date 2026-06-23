# TRADING-770 to TRADING-774: Controlled Strategy Candidate Research
最后更新：2026-06-23

## Background

TRADING-765～769 已完成 controlled research expansion baseline：benchmark/control
expansion、Marketstack limited second-source closure、FMP controlled-research source
closure、forward evidence daily dry-run archive 和 reverse diagnostics activation gate
均已进入 `VALIDATING` / `READY_FOR_CONTROLLED_ACTIVATION`。当前仍不是
`PROMOTION_READY`。

本批任务启动第一批受控策略候选研究，但只允许 validation-only / observe-only。
任何候选都不能进入 promotion、paper-shadow、production review、official target
weight 或 broker/order 路径。

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

每个候选输出必须包含 `data_foundation_status`、`evidence_source_mix`、
`control_results`、`benchmark_comparison` 和 `remaining_blockers`。

## Heuristic Governance

本批的 action ranking、state transition rule、selector rule 和 diagnostic model utility
均为 first-batch controlled heuristic baseline。报告必须标记：

- `ranking_policy=heuristic`
- `not_validated_utility_boundary=true`
- `heuristic_policy_version=controlled_strategy_batch_1_heuristic_v1`

这些规则只能用于可证伪研究排序，不得解释为 validated utility boundary、promotion
evidence、paper-shadow candidate 或 production weight policy。

## Stage Breakdown

| Task | Stage | Goal | Status |
|---|---|---|---|
| TRADING-770 | Horizon-Conditioned Value Surface Controlled Prototype | 输出 PIT state x candidate action x horizon 的透明多指标 value surface、benchmark comparison、controls 和 horizon leakage audit | VALIDATING |
| TRADING-771 | Regret-Driven State Machine Controlled Prototype | 把 regret taxonomy 转成可解释 state transition、action by state、turnover/whipsaw guardrail 和 casebook | VALIDATING |
| TRADING-772 | Simple Strategy Ensemble / Selector Pilot | 构建 simple strategy zoo 和规则型 selector，比较 best simple benchmark 并披露 overfit risk | VALIDATING |
| TRADING-773 | GBDT Action-Utility Diagnostic Baseline | 建立轻量 tree/boosting diagnostic utility line，输出 split、negative control、random label、feature importance 和 no-future-feature audit | VALIDATING |
| TRADING-774 | Controlled Strategy Batch Review | 统一评审 770～773，给出 continue/watchlist/data-required/pause/kill/pivot/infra-review 决策和下一批建议 | VALIDATING |

## Implementation Plan

1. 新增 `ai_trading_system.controlled_strategy_batch`：
   - 所有 cached data dependent runner 先调用 `validate_data_cache`；
   - 读取 FMP price cache，代表性 universe 为 `SPY, QQQ, SMH, MSFT, GOOGL,
     NVDA, AMD, TSM`，并显式记录 `cash` action；
   - 所有输出写入附件指定目录；
   - Markdown 报告保持中文摘要和安全边界。
2. 新增 CLI：
   - `aits research strategies value-surface-controlled-prototype`
   - `aits research strategies regret-state-machine-controlled-prototype`
   - `aits research strategies simple-strategy-selector-pilot`
   - `aits research strategies gbdt-action-utility-baseline`
   - `aits research ops controlled-strategy-batch-review`
3. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和
   `docs/system_flow.md`。
4. 新增 focused tests，覆盖附件测试项和安全边界。
5. 运行真实 CLI 生成 batch-1 artifacts，再执行 required validation。

## Acceptance Criteria

- `value_surface_generated=true`
- `candidate_action_count >= configured_minimum`
- `horizon_count >= configured_minimum`
- `benchmark_comparison_present=true`
- `negative_control_promotion_count=0`
- `future_leakage_trap_blocked=true`
- `horizon_leakage_check_pass=true`
- `sample_quality_report_present=true`
- `state_transition_explainable=true`
- `regret_type_mapping_present=true`
- `turnover_guardrail_reported=true`
- `whipsaw_report_present=true`
- `simple_strategy_count >= configured_minimum`
- `selector_rules_present=true`
- `best_simple_benchmark_comparison_present=true`
- `selector_overfit_warning_present=true`
- `model_run_complete=true`
- `negative_control_pass=true`
- `simple_baseline_comparison_present=true`
- `feature_importance_report_present=true`
- `future_feature_violation_count=0`
- `all_candidates_have_decision=true`
- `no_candidate_promoted_without_policy=true`
- `kill_pause_pivot_decisions_present=true`
- `next_batch_recommendation_present=true`
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `production_effect=none`

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 要求完成附件
  TRADING-770～774。本批只允许 controlled-research-only，不允许 promotion、
  paper-shadow、production review、official weight change 或 broker/order side effect。
- 2026-06-21：实现并运行 batch-1 controlled strategy candidate research。新增
  `config/research/controlled_strategy_candidate_research.yaml`、runner、CLI、report
  registry、artifact catalog、system flow 和 focused tests。真实 CLI run 输出：
  value surface `candidate_action_count=11`、`horizon_count=5`、
  `horizon_leakage_check_pass=true`、`negative_control_promotion_count=0`；
  regret state machine `state_transition_explainable=true`、
  `regret_type_mapping_present=true`、`turnover_guardrail_reported=true`；
  simple selector `simple_strategy_count=10`、`recommendation=KEEP_SIMPLE_BENCHMARK`；
  GBDT diagnostic `model_run_complete=true`、`negative_control_pass=true`、
  `future_feature_violation_count=0`。TRADING-774 review board 状态为
  `CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE`，candidate decisions 为
  value_surface `CONTINUE`、regret_state_machine `WATCHLIST`、
  simple_strategy_selector `KILL`、gbdt_action_utility `PIVOT`。所有输出继续固定
  `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false` 和
  `lookahead_violation_count=0`。
- 2026-06-21：验证通过 focused parallel pytest、Ruff、Black check、compileall、
  `git diff --check`、fast-unit、contract-validation 和 report-validation。Runtime
  artifacts：
  `outputs/validation_runtime/fast-unit_20260621T091321Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T091512Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T091702Z/test_runtime_summary.json`。
