# TRADING-2289 Refined Candidate Actual-Path Validation

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2289_REFINED_CANDIDATE_ACTUAL_PATH_VALIDATION`
- priority: `P0`
- status: `VALIDATING`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2288 已读取 TRADING-2287 confidence scaling refinement plan，并生成三类 refined
candidate-bound artifacts：

- `baseline_plus_trend_structure_refined_confidence_v1`
- `risk_appetite_refined_confidence_v1`
- `volatility_regime_refined_confidence_v1`

TRADING-2288 只执行 refined regeneration，不执行 actual-path validation。TRADING-2289
读取 refined artifacts、TRADING-2285 original actual-path outputs 和 TRADING-2287
guardrails，验证 refined confidence scaling 是否真的改善 actual-path evidence，并生成
original-vs-refined comparison。

## 非目标

- 不修改 refined generator。
- 不重新生成 refined artifacts。
- 不执行新的 confidence scaling plan 或 parameter search。
- 不做组合层仓位回测。
- 不做 owner final decision。
- 不执行 promotion、paper-shadow、production 或 broker action。
- 不改变 TRADING-2281 permanently inconclusive 结论。
- 不改变 TRADING-2285 original inconclusive 结论。
- 不把 high-confidence ratio 提升本身解释为策略有效。

## 实施步骤

1. 新增 `aits research trends refined-candidate-actual-path-validation` CLI，只允许
   `mode=refined_actual_path_validation`。
2. 新增 refined validation loader，读取 TRADING-2288 top-level artifacts 和每个 refined
   candidate 子目录下的 signal spec、signal series、prediction artifact、generation /
   validation summary、parameter report 和 original-vs-refined delta。
3. 读取 TRADING-2285 original validation scorecard / outcome matrix / data quality /
   state recommendation outputs，并 fail-closed 校验 safety fields。
4. 读取 TRADING-2287 guardrail matrix，并把 original candidate 映射到 refined candidate。
5. 复用 TRADING-2285 actual-path price loader、path calculator 和 outcome alignment
   logic，计算 refined actual-path matrix 与 refined prediction/outcome matrix。
6. 生成 refined candidate scorecard、high-conviction outcome drilldown、false signal cost
   matrix、guardrail validation matrix、original-vs-refined comparison、state
   recommendation、data quality report 和 error attribution seed。
7. 写出 research docs，并更新 report registry、artifact catalog、system flow 和 task
   register。

## Safety Boundary

所有 TRADING-2289 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `broker_action_recommendation_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`
- `trading_2285_original_inconclusive_decisions_changed=false`

允许输出 `owner_review_candidate_recommendation=true`，但这只表示后续 owner review
候选，不代表 promotion、paper-shadow、production 或 broker readiness。

## Pilot Threshold Governance

TRADING-2289 的 minimum data coverage、minimum high-conviction sample、false-cost
guardrails 和 comparison materiality threshold 都是 research-only evidence
classification baseline。实现必须使用命名常量或 TRADING-2287 guardrail artifact 字段，
不得引入无解释的 investment-facing numeric literal。

## 输出产物

Runtime artifacts 写入
`outputs/research_trends/refined_candidate_actual_path_validation/`：

- `refined_candidate_actual_path_validation_summary.json`
- `refined_candidate_actual_path_matrix.json`
- `refined_candidate_actual_path_matrix.csv`
- `refined_candidate_prediction_outcome_matrix.json`
- `refined_candidate_prediction_outcome_matrix.csv`
- `refined_candidate_validation_scorecard.json`
- `refined_high_conviction_outcome_drilldown.json`
- `refined_high_conviction_outcome_drilldown.csv`
- `refined_false_signal_cost_matrix.json`
- `refined_false_signal_cost_matrix.csv`
- `refined_guardrail_validation_matrix.json`
- `refined_guardrail_validation_matrix.csv`
- `original_vs_refined_actual_path_comparison.json`
- `original_vs_refined_actual_path_comparison.csv`
- `refined_candidate_state_recommendation_matrix.json`
- `refined_candidate_error_attribution_seed.json`
- `refined_candidate_data_quality_report.json`

Research docs：

- `docs/research/refined_candidate_actual_path_validation_report.md`
- `docs/research/refined_high_conviction_outcome_drilldown.md`
- `docs/research/original_vs_refined_actual_path_comparison.md`
- `docs/research/refined_candidate_state_recommendation.md`

## 验收标准

- CLI 能读取 TRADING-2288 refined artifacts、TRADING-2285 original validation outputs 和
  TRADING-2287 guardrails，并写出全部 required runtime artifacts。
- Refined input artifacts 缺少 refined/original/candidate ids、source hash、timestamps、
  horizon、provenance、selected parameter set ids，或打开 promotion / paper-shadow /
  production / broker fields 时 fail closed。
- Original validation scorecard 或 outcome matrix 缺失时 fail closed。
- Guardrail matrix 缺失或无法映射 candidate 时 fail closed 或在 candidate row 明确
  `guardrail_status=FAIL`、`validation_status=FAIL_CLOSED`。
- Refined actual-path matrix 使用与 TRADING-2285 可比的 price path calculation 和
  outcome alignment logic。
- High-conviction drilldown 能单独统计 high / non-high alignment、false risk-on/off cost
  和 edge label。
- Original-vs-refined comparison 输出 alignment / weighted score / confidence-weighted
  score / false-cost deltas 和 comparison label。
- Guardrail FAIL 时不得推荐 owner review candidate。
- State recommendation 只允许 refined research states，不允许 `PROMOTION_READY`、
  `PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。
- 所有 outputs 固定 promotion/paper-shadow/production/broker false/none。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for TRADING-2289 tests
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`；范围限定为 refined actual-path evidence、
  high-conviction drilldown、guardrail validation 和 original-vs-refined comparison，不执行
  owner final decision、promotion、paper-shadow、production 或 broker action。
- 2026-06-30: 实现完成并转入 `VALIDATING`。真实 CLI run 生成 95,220 条
  actual-path records，其中 73,188 条 validation eligible、22,032 条 validation
  ineligible；source data quality status=`PASS_WITH_WARNINGS` / error_count=0。
  三类 refined candidates 的 guardrail status 均为 `PASS_WITH_WARNINGS`；
  baseline_plus_trend_structure 和 volatility_regime 建议
  `REFINED_ACTUAL_PATH_VALIDATED_CONTINUE_RESEARCH`，risk_appetite 建议
  `REFINED_ACTUAL_PATH_VALIDATED_REJECT_RECOMMENDED`；owner review candidate
  recommendation 全部为 false。验证通过 Ruff、compileall、focused parallel pytest
  35 passed、full parallel pytest 3626 passed、docs freshness、documentation
  contract、contract-validation 193 passed、task-register consistency run/validate 和
  `git diff --check`；所有 outputs 继续固定 promotion/paper-shadow/production/broker
  false/none。
