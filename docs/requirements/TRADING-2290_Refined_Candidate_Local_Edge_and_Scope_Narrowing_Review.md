# TRADING-2290 Refined Candidate Local Edge and Scope Narrowing Review

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2290_REFINED_CANDIDATE_LOCAL_EDGE_SCOPE_NARROWING_REVIEW`
- priority: `P0`
- status: `VALIDATING`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2289 已完成 refined candidates actual-path validation。真实 run 输出
95,220 条 actual-path records，其中 73,188 条 validation eligible；data quality 为
`PASS_WITH_WARNINGS` / error_count=0。`baseline_plus_trend_structure_refined_confidence_v1`
和 `volatility_regime_refined_confidence_v1` 建议 continue research，
`risk_appetite_refined_confidence_v1` 建议 reject current form，owner review candidate
recommendation 全部为 false。

因此 TRADING-2290 不生成 owner review package，只做 local edge / scope narrowing
review，回答 continue research candidates 是否存在可收窄的局部弱优势，以及
`risk_appetite_refined_confidence_v1` 是否正式记录 current-form reject。

## 非目标

- 不生成新的 candidate signals。
- 不修改 refined generator。
- 不重新执行 actual-path validation。
- 不做 owner review package。
- 不做 promotion、paper-shadow、production 或 broker action。
- 不做组合层仓位回测或新参数搜索。
- 不重新打开 `risk_appetite` refinement。
- 不改变 TRADING-2281 permanently inconclusive、TRADING-2285 original inconclusive 或
  TRADING-2289 refined validation 结论。
- 不把 local edge 解释为 paper-shadow-ready、production-ready 或 broker-ready。

## 实施步骤

1. 新增 `aits research trends refined-candidate-local-edge-scope-review` CLI，只允许
   `mode=local_edge_scope_review`。
2. 新增 loader，读取 TRADING-2289 refined validation outputs、TRADING-2288 refined
   generator artifacts 和 TRADING-2287 guardrail / refinement plan outputs。
3. 对 required inputs 做 fail-closed safety validation：promotion / paper-shadow /
   production / broker 必须保持 false/none，recommendation 不得出现 promotion-ready
   系列状态。
4. 基于 2289 prediction/outcome matrix、scorecard、high-conviction drilldown、false-cost
   matrix、guardrail matrix、comparison 和 state recommendation，生成 local edge、asset、
   horizon、direction、high-conviction、regime 和 false-cost scope matrices。
5. 为 `risk_appetite_refined_confidence_v1` 生成 current-form reject record。
6. 生成 scope narrowing recommendation、next-task recommendation 和 top-level summary。
7. 写出 research docs，并更新 report registry、artifact catalog、system flow 和 task
   register。

## Safety Boundary

所有 TRADING-2290 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `owner_review_required=false`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `broker_action_recommendation_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`
- `trading_2285_original_inconclusive_decisions_changed=false`
- `trading_2289_refined_state_decisions_changed=false`

## Pilot Threshold Governance

TRADING-2290 的 local edge、scope keep/drop、false-cost blocked、minimum sample 和
scope narrowing action 都是 research-only review baseline。实现必须使用命名常量或
TRADING-2287/2289 input guardrail 字段，不得引入无解释的 investment-facing numeric
literal。

## 输出产物

Runtime artifacts 写入
`outputs/research_trends/refined_candidate_local_edge_scope_review/`：

- `local_edge_scope_review_summary.json`
- `candidate_local_edge_matrix.json`
- `candidate_local_edge_matrix.csv`
- `candidate_asset_scope_matrix.json`
- `candidate_asset_scope_matrix.csv`
- `candidate_horizon_scope_matrix.json`
- `candidate_horizon_scope_matrix.csv`
- `candidate_direction_scope_matrix.json`
- `candidate_direction_scope_matrix.csv`
- `candidate_high_conviction_scope_matrix.json`
- `candidate_high_conviction_scope_matrix.csv`
- `candidate_regime_scope_matrix.json`
- `candidate_regime_scope_matrix.csv`
- `candidate_false_cost_scope_matrix.json`
- `candidate_false_cost_scope_matrix.csv`
- `candidate_scope_narrowing_recommendation_matrix.json`
- `candidate_scope_narrowing_recommendation_matrix.csv`
- `risk_appetite_reject_record.json`
- `risk_appetite_reject_record.md`
- `candidate_next_task_recommendation_matrix.json`
- `candidate_scope_review_decision_summary.json`

Research docs：

- `docs/research/refined_candidate_local_edge_scope_review.md`
- `docs/research/refined_candidate_scope_narrowing_recommendation.md`
- `docs/research/risk_appetite_reject_record.md`
- `docs/research/candidate_next_task_recommendation_after_2289.md`

## 验收标准

- CLI 能读取 TRADING-2289 / 2288 / 2287 inputs，并写出全部 required runtime
  artifacts。
- 缺少 scorecard、high-conviction drilldown、original-vs-refined comparison、state
  recommendation 或 data-quality report 时 fail closed。
- Input artifact 打开 promotion、paper-shadow、production 或 broker action 时 fail
  closed。
- Continue-research candidates 缺失时 fail closed。
- `risk_appetite_refined_confidence_v1` 生成 current-form reject record，且 reject
  current form 不等于永久 reject risk appetite concept。
- Scope matrices 覆盖 asset、horizon、direction、high-conviction、regime 和 false-cost
  维度。
- Scope recommendation 不得输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、
  `PRODUCTION_READY` 或 `BROKER_READY`。
- 所有 outputs 固定 promotion/paper-shadow/production/broker false/none。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for TRADING-2290 tests
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 根据 owner 附件新增并进入 `IN_PROGRESS`；范围限定为 local edge /
  scope narrowing review 和 risk_appetite current-form reject record，不执行 owner
  review package、promotion、paper-shadow、production 或 broker action。
- 2026-06-30: 实现完成并转入 `VALIDATING`。真实 CLI run 读取 TRADING-2289
  actual-path records=95,220、validation eligible=73,188、input data quality
  status=`PASS_WITH_WARNINGS`；`baseline_plus_trend_structure_refined_confidence_v1`
  标记 `LOCAL_EDGE_PRESENT`，建议 `SCOPE_NARROW_AND_REGENERATE` /
  `confirmation_only`；`volatility_regime_refined_confidence_v1` 标记
  `LOCAL_EDGE_WEAK`，建议 `SCOPE_NARROW_AND_REGENERATE` / `risk_cap_only`；
  `risk_appetite_refined_confidence_v1` 标记 `LOCAL_EDGE_NOT_FOUND`，生成
  current-form reject record 并建议 archive current form。验证通过 Ruff、compileall、
  focused parallel pytest 34 passed、full parallel pytest 3664 passed、docs freshness、
  documentation contract、contract-validation 193 passed 和 `git diff --check`；所有
  outputs 继续固定 promotion/paper-shadow/production/broker false/none。
