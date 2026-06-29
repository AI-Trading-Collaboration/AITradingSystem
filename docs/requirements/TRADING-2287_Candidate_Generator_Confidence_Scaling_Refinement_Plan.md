# TRADING-2287 Candidate Generator Confidence Scaling Refinement Plan

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2287_CANDIDATE_GENERATOR_CONFIDENCE_SCALING_REFINEMENT_PLAN`
- priority: `P0`
- status: `DONE`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2284 已生成 `baseline_plus_trend_structure`、`risk_appetite` 和
`volatility_regime` 三类 regenerated first-layer executable candidate artifacts。
TRADING-2285 已完成 candidate-level actual-path validation，并生成 actual-path
matrix、prediction/outcome matrix、scorecard、data quality report、error
attribution seed 和 state recommendation。TRADING-2286 进一步诊断 inconclusive
原因，真实 run 读取 95,220 条 records、73,188 条 eligible records，三类
candidate 的 primary reason 均为 `LOW_CONFIDENCE_SIGNAL`，recommended action 均为
`REFINE_CONFIDENCE_SCALING`，data quality impact 均为
`DATA_QUALITY_NOT_MATERIAL`。

TRADING-2287 只生成 confidence scaling refinement plan 和 TRADING-2288 实施规格。
它不修改 2284 generator 生产逻辑，不重新生成 signals，不重跑 actual-path
validation，也不把任何 candidate 标记为 owner-review、paper-shadow、production
或 broker ready。

## 非目标

- 不修改 TRADING-2284 generator 生产逻辑。
- 不重新生成 candidate signal series 或 prediction artifacts。
- 不重新执行 TRADING-2285 actual-path validation。
- 不做策略参数搜索、owner review、promotion、paper-shadow、production 或 broker
  action。
- 不改变 TRADING-2281 旧 proxy candidates 的 permanently inconclusive 结论。
- 不改变 TRADING-2285 的 `ACTUAL_PATH_VALIDATED_INCONCLUSIVE` 结论。
- 不将 confidence scaling proposal 视为已经验证有效。

## 实施步骤

1. 新增 `aits research trends candidate-generator-confidence-scaling-refinement-plan`
   CLI，只允许 `mode=refinement_plan`。
2. 新增 loader，读取 TRADING-2286 diagnostics outputs、TRADING-2285 validation
   outputs 和 TRADING-2284 generator context。
3. 对 diagnostics summary、refinement recommendation、actual-path scorecard 和
   generator artifacts 执行 fail-closed safety checks；generator context 可 partial，
   但必须显式记录 warning。
4. 生成 confidence failure diagnosis、confidence distribution retargeting、scaling
   proposal、small parameter grid、guardrail、expected risk impact 和 2288
   implementation plan matrices。
5. 写出 runtime artifacts 和 research docs，并更新 artifact catalog、system flow、
   report registry config 和 task register。
6. 新增 focused loader / diagnosis / retargeting / proposal / parameter grid /
   guardrail / CLI tests。

## Safety Boundary

所有 TRADING-2287 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `owner_review_required=false`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `regeneration_executed=false`
- `actual_path_validation_executed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`
- `trading_2285_inconclusive_decisions_changed=false`

任何 proposal / recommendation 不得输出 `PROMOTION_READY`、
`PAPER_SHADOW_READY`、`PRODUCTION_READY` 或 `BROKER_READY`。

## Pilot Threshold Governance

TRADING-2287 的 confidence retargeting、parameter grid 和 guardrail 数值仅用于
TRADING-2288 refinement design。它们是 research-only pilot baselines，不是
promotion gate、paper-shadow gate、production rule 或 broker rule。

实现时必须使用命名常量并在代码注释中指向本需求文档。报告必须披露这些数值只是
下一步 refinement 约束，不证明任何 candidate 已经具备上线资格。

## 输出产物

Runtime artifacts 写入
`outputs/research_trends/candidate_generator_confidence_scaling_refinement_plan/`：

- `confidence_scaling_refinement_summary.json`
- `candidate_confidence_failure_diagnosis_matrix.json/csv`
- `candidate_confidence_distribution_retargeting_matrix.json/csv`
- `candidate_confidence_scaling_proposal_matrix.json/csv`
- `candidate_confidence_scaling_parameter_grid.json/csv`
- `candidate_guardrail_matrix.json/csv`
- `candidate_expected_risk_impact_matrix.json/csv`
- `candidate_2288_implementation_plan.json/csv`

Research docs：

- `docs/research/candidate_confidence_scaling_refinement_plan.md`
- `docs/research/candidate_confidence_failure_diagnosis.md`
- `docs/research/candidate_confidence_scaling_guardrails.md`
- `docs/research/candidate_2288_refined_regeneration_plan.md`

## 验收标准

- CLI 能读取 2286 diagnostics、2285 validation 和 2284 generator context，并写出所有 required artifacts。
- Loader 对缺少 diagnostics summary、缺少 actual-path scorecard、`promotion_allowed=true`
  或 `broker_action!=none` 的 input fail closed。
- Failure diagnosis 能从 `LOW_CONFIDENCE_SIGNAL` 拆解出具体 failure modes，覆盖
  high confidence ratio 过低、neutral ratio 偏高、confidence cap 偏低和 missing
  proxy penalty 过强。
- Retargeting matrix 生成 high confidence / low confidence / neutral /
  directional signal 的目标区间，且不超过 guardrail。
- Proposal matrix 每个 candidate 至少有一条合法 proposal，并包含 expected effect
  和 risk control note。
- Parameter grid 非空，每个 candidate parameter set 不超过 24，且不执行 regeneration。
- Guardrail matrix 强制 promotion/paper-shadow/production/broker false/none。
- 2288 implementation plan 默认只建议 `CONFIDENCE_SCALING_ONLY`，除非 diagnostics
  明确指向 direction / asset / horizon scope。
- TRADING-2281 permanently inconclusive decisions 和 TRADING-2285 inconclusive
  decisions 不被修改。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for seven TRADING-2287 test files
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`；范围限定为 confidence scaling
  refinement plan，不做 regeneration、actual-path validation、owner review、
  promotion、paper-shadow、production 或 broker action。
- 2026-06-30: 实现完成并归档为 `DONE`。新增
  `aits research trends candidate-generator-confidence-scaling-refinement-plan`、
  TRADING-2286 / 2285 / 2284 input loader、confidence failure diagnosis、
  distribution retargeting、candidate-specific scaling proposal、bounded parameter
  grid、guardrails、expected risk impact 和 TRADING-2288 implementation plan。
  真实 run 读取 95,220 条 prediction/outcome records，其中 73,188 条 eligible；
  三类 candidate high confidence ratio 均为 0，dominant failure mode 均为
  `INSUFFICIENT_HIGH_CONVICTION_RULE`，生成 9 条 proposals 和 27 个 parameter
  sets。所有 outputs 固定 promotion/paper-shadow/production false、broker_action=none、
  `regeneration_executed=false`、`actual_path_validation_executed=false`。验证通过
  Ruff、compileall、focused parallel pytest 19 passed、full parallel pytest 3576 passed、
  docs freshness、documentation contract、contract-validation 193 passed、task-register
  consistency 和 `git diff --check`。
