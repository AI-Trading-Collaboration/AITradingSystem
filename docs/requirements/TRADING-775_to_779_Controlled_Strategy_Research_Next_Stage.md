# TRADING-775 to TRADING-779: Controlled Strategy Research Next Stage
最后更新：2026-06-23

## Background

TRADING-770～774 已完成第一轮 controlled strategy candidate research，review board
状态为 `CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE`。候选决策为：

- value_surface：`CONTINUE`
- regret_state_machine：`WATCHLIST`
- simple_strategy_selector：`KILL`
- gbdt_action_utility：`PIVOT`

下一阶段只继续扩大 value surface 研究范围，并建立 utility boundary audit、
forward evidence maturity tracker、GBDT pivot review 和 regret casebook expansion
gate。simple selector 暂停，不进入 paper-shadow，不引入神经网络 / RL，不继续
GBDT 局部调参。

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

TRADING-775 和 TRADING-777 依赖 cached market / macro data，必须先复用
`aits validate-data` 同一质量门禁并在输出中披露 `data_foundation_status`。

## Heuristic Governance

下一阶段使用 `config/research/controlled_strategy_next_stage_research.yaml` 作为
policy manifest。所有 value-surface ranking、utility sensitivity profile、maturity
floor、GBDT pivot option 和 regret expansion gate floor 都是 controlled research
baseline，不是 validated utility boundary。

报告必须继续标记：

- `ranking_policy=heuristic`
- `not_validated_utility_boundary=true`
- `heuristic_policy_version=controlled_strategy_next_stage_heuristic_v1`

TRADING-776 最多输出 `SENSITIVITY_TESTED`，不得升级为 validated boundary。

## Stage Breakdown

| Task | Stage | Goal | Status |
|---|---|---|---|
| TRADING-775 | Value Surface Controlled Expansion | 扩大 value surface 受控研究范围，输出 action × horizon surface、benchmark comparison、smoothness/leakage audit、by asset/regime/cluster、gross/net、turnover/cost/drawdown 和 negative controls | VALIDATING |
| TRADING-776 | Utility Boundary and Ranking Policy Audit | 拆解 return / risk / cost / uncertainty 对 ranking 的影响，记录 profile reversal、single-weight dominance 和 Pareto frontier；只允许 `SENSITIVITY_TESTED` | VALIDATING |
| TRADING-777 | Forward Evidence Daily Dry-Run Maturity Tracker | 读取 append-only daily dry-run ledger，按 1d / 5d / 10d / 20d / 60d 追踪 maturity，并记录 benchmark / value surface / controls / candidate artifacts 留存状态 | VALIDATING |
| TRADING-778 | GBDT Pivot Review | 对 batch-1 GBDT baseline 做 design-only pivot review，禁止继续局部调树参数，比较 action-ranking、regret-type、state-transition、residual-model pivot 方向 | VALIDATING |
| TRADING-779 | Regret Casebook Expansion Gate | 定义 regret state machine 扩展激活条件；case 数、regret type 分布、teacher/oracle 差异和 value surface failure attribution 不满足时保持 watchlist | VALIDATING |

## Implementation Plan

1. 新增 next-stage policy config，显式记录扩展样本、cluster、utility profile、
   maturity horizon、GBDT pivot options 和 regret casebook gate floor。
2. 在 `ai_trading_system.controlled_strategy_batch` 中新增 next-stage runners：
   - `run_value_surface_controlled_expansion`
   - `run_utility_boundary_ranking_policy_audit`
   - `run_forward_evidence_maturity_tracker`
   - `run_gbdt_pivot_review`
   - `run_regret_casebook_expansion_gate`
3. 新增 CLI：
   - `aits research strategies value-surface-controlled-expansion`
   - `aits research strategies utility-boundary-ranking-policy-audit`
   - `aits forward-evidence maturity-tracker`
   - `aits research strategies gbdt-pivot-review`
   - `aits research strategies regret-casebook-expansion-gate`
4. 更新 `config/report_registry.yaml`、`docs/artifact_catalog.md` 和
   `docs/system_flow.md`。
5. 新增 focused tests，覆盖 schema、safety boundary、utility boundary status cap、
   forward maturity append-only、GBDT design-only pivot 和 regret expansion gate。
6. 运行真实 CLI 生成下一阶段 artifacts，再执行 required validation。

## Acceptance Criteria

- `value_surface_expansion_generated=true`
- `action_horizon_surface_present=true`
- `benchmark_comparison_present=true`
- `horizon_smoothness_audit_present=true`
- `horizon_leakage_check_pass=true`
- `by_asset_breakdown_present=true`
- `by_regime_breakdown_present=true`
- `by_cluster_breakdown_present=true`
- `gross_net_turnover_drawdown_present=true`
- `negative_control_promotion_count=0`
- `utility_boundary_status=SENSITIVITY_TESTED`
- `validated_boundary_count=0`
- `not_validated_utility_boundary=true`
- `profile_reversal_report_present=true`
- `pareto_frontier_present=true`
- `forward_maturity_tracker_generated=true`
- `future_outcomes_appended_only=true`
- `horizon_maturity_recorded=true`
- `gbdt_pivot_review_status=PIVOT_REVIEW_READY`
- `model_run_executed=false`
- `local_parameter_tuning_allowed=false`
- `regret_casebook_expansion_allowed=false` unless all configured gates pass
- `promotion_gate_allowed=false`
- `paper_shadow_change_allowed=false`
- `production_weight_change_allowed=false`
- `production_effect=none`

## Progress Notes

- 2026-06-21：新增本需求文档并进入 `IN_PROGRESS`；owner 明确下一阶段主线为
  value surface controlled expansion，并行稳定 forward evidence，GBDT 只做 pivot
  design，regret state machine 保持 watchlist，simple selector 暂停。本阶段目标不是
  `PROMOTION_READY`、`PAPER_SHADOW_READY` 或 `PRODUCTION_READY`。
- 2026-06-21：实现 next-stage controlled research baseline 并转入 `VALIDATING`。
  新增 policy config、runner、CLI、report registry、artifact catalog、system flow 和
  focused tests。真实 CLI run 输出：TRADING-775 `PASS_WITH_WARNINGS`、
  `decision_date_count=72`、`candidate_action_count=11`、
  `horizon_leakage_check_pass=true`；TRADING-776 `SENSITIVITY_TESTED`、
  `validated_boundary_count=0`；TRADING-777 `PASS_WITH_WARNINGS`、
  `ledger_event_count=1`、`future_outcomes_appended_only=true`；TRADING-778
  `PIVOT_REVIEW_READY`、`model_run_executed=false`、
  `local_parameter_tuning_allowed=false`；TRADING-779 `WATCHLIST_NOT_READY`、
  `case_count=24`、`regret_casebook_expansion_allowed=false`。所有输出继续固定
  `production_effect=none`、`broker_action=none`、`promotion_gate_allowed=false`、
  `paper_shadow_change_allowed=false`、`production_weight_change_allowed=false` 和
  `lookahead_violation_count=0`。
- 2026-06-21：验证通过 focused parallel pytest、Ruff、Black check、compileall、
  `git diff --check`、fast-unit、contract-validation 和 report-validation。Runtime
  artifacts：
  `outputs/validation_runtime/fast-unit_20260621T095425Z/test_runtime_summary.json`、
  `outputs/validation_runtime/contract-validation_20260621T095616Z/test_runtime_summary.json`、
  `outputs/validation_runtime/report-validation_20260621T095808Z/test_runtime_summary.json`。
