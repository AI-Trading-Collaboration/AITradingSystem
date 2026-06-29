# TRADING-2288 Refined Candidate Regeneration with Adjusted Confidence Scaling

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2288_REFINED_CANDIDATE_REGENERATION_WITH_ADJUSTED_CONFIDENCE_SCALING`
- priority: `P0`
- status: `DONE`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2284 已生成 `baseline_plus_trend_structure`、`risk_appetite` 和
`volatility_regime` 三类 regenerated executable candidate artifacts。TRADING-2285
完成 actual-path validation 后，三类 candidate 均为
`ACTUAL_PATH_VALIDATED_INCONCLUSIVE`。TRADING-2286 将主要原因诊断为
`LOW_CONFIDENCE_SIGNAL`，TRADING-2287 进一步生成 confidence scaling refinement
plan；真实 run 读取 95,220 条 prediction/outcome records，其中 73,188 条 eligible，
三类 candidate 均诊断为 `INSUFFICIENT_HIGH_CONVICTION_RULE`，并生成 9 条 scaling
proposals 和 27 个 parameter grid entries。

TRADING-2288 只读取 2287 refinement plan 和 2284 original regenerated artifacts，
应用 adjusted confidence scaling / high-conviction rules，并生成新的 refined
candidate-bound artifacts。TRADING-2288 不执行 actual-path validation；refined
actual-path validation 由 TRADING-2289 承接。

## 非目标

- 不执行 actual-path validation。
- 不计算 refined candidate 的 outcome score 或 utility improvement。
- 不做 owner review、promotion、paper-shadow、production 或 broker action。
- 不引入新外部数据源，不大规模重写 candidate family。
- 不覆盖 TRADING-2284 original artifacts。
- 不改变 TRADING-2281 permanently inconclusive 结论。
- 不改变 TRADING-2285 original regenerated candidates 的 inconclusive 结论。
- 不把 refined artifact 标记为 actual-path validation ready。

## 实施步骤

1. 新增 `aits research trends refined-candidate-generators-regenerate` CLI，只允许
   `mode=refined_regeneration`。
2. 新增 loader，读取 TRADING-2287 refinement plan required outputs 和 TRADING-2284
   original generator artifacts。
3. 对 input artifacts 和 refinement plan 执行 fail-closed safety checks；任何
   promotion、paper-shadow、production 或 broker action 开启均失败。
4. 为每个 original candidate 生成新的 refined candidate id：
   `baseline_plus_trend_structure_refined_confidence_v1`、
   `risk_appetite_refined_confidence_v1`、
   `volatility_regime_refined_confidence_v1`。
5. 从 2287 parameter grid 中为每个 candidate 选择至多 3 个 guardrail-compliant
   parameter sets，并生成 parameter application report。
6. 对 original signal series / prediction records 应用 append-only confidence scaling
   字段，生成 refined signal spec、refined signal series、refined prediction artifact、
   refined generation summary 和 refined validation summary。
7. 生成 original-vs-refined delta summary，只比较 signal distribution，不比较未来
   outcome 或 utility。
8. 写出 research docs，并更新 artifact catalog、system flow、report registry config
   和 task register。

## Safety Boundary

所有 TRADING-2288 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `actual_path_validation_ready=false`
- `owner_review_required=false`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `promotion_eligible=false`
- `permanently_inconclusive_override_allowed=false`
- `actual_path_validation_executed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`
- `trading_2285_inconclusive_decisions_changed=false`

## Pilot Threshold Governance

TRADING-2288 使用 TRADING-2287 生成的 confidence retargeting、parameter grid 和
guardrail 数值。它们是 research-only refined regeneration baselines，不是
promotion gate、paper-shadow gate、production rule 或 broker rule。实现必须使用
命名常量或来自 2287 artifacts 的显式字段，不得引入无解释的 investment-facing
numeric literal。

## 输出产物

Runtime artifacts 写入
`outputs/research_trends/refined_candidate_generators_regenerated/`：

- `refined_regeneration_run_summary.json`
- `refined_regeneration_validation_summary.json`
- `refined_original_vs_refined_delta_summary.json`
- 每个 refined candidate 子目录下的：
  - `refined_candidate_signal_spec.json`
  - `refined_candidate_signal_series.csv`
  - `refined_candidate_prediction_artifact.json`
  - `refined_generation_summary.json`
  - `refined_validation_summary.json`
  - `refined_parameter_application_report.json`
  - `refined_original_vs_refined_delta.json`

Research docs：

- `docs/research/refined_candidate_regeneration_report.md`
- `docs/research/refined_candidate_parameter_application_report.md`
- `docs/research/refined_original_vs_refined_delta_summary.md`

## 验收标准

- CLI 能读取 2287 refinement plan 和 2284 original artifacts，并写出所有 required
  refined artifacts。
- Loader 对缺少 proposal matrix、parameter grid、guardrail matrix、implementation
  plan、`promotion_allowed=true` 或 `broker_action!=none` 的 input fail closed。
- 每个 refined candidate 使用不同于 original candidate 的 native candidate id。
- Refined signal series 包含 confidence scaling / high-conviction append-only 字段。
- High-conviction rule 能正确设置 flag，且 high-confidence ratio 不超过 guardrail。
- Refined prediction artifact 通过 candidate-bound validator 和 2288 refined
  fail-closed checks。
- Original-vs-refined delta 只比较 signal distribution，不输出 actual-path
  improvement、utility improvement 或 owner-review readiness。
- 所有 safety fields 固定 false / none。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for six TRADING-2288 test files
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`；范围限定为 adjusted-confidence
  refined regeneration，不执行 actual-path validation、owner review、promotion、
  paper-shadow、production 或 broker action。
- 2026-06-30: 实现完成并归档为 `DONE`。新增
  `aits research trends refined-candidate-generators-regenerate`、TRADING-2287 /
  TRADING-2284 input loader、guardrail-compliant parameter application、append-only
  confidence scaling / high-conviction fields、refined candidate-bound signal spec /
  series / prediction artifact、refined validation summary、parameter application
  report 和 original-vs-refined delta summary。真实 CLI run 读取 95,220 条
  TRADING-2287 input records，其中 73,188 条 eligible；生成
  `baseline_plus_trend_structure_refined_confidence_v1`、
  `risk_appetite_refined_confidence_v1` 和
  `volatility_regime_refined_confidence_v1`，selected proposals=9、
  selected parameter sets=9，top-level validation status=`PASS`。Refined
  high-confidence ratio 分别为 0.349975、0.0 和 0.349988，均未超过 guardrail
  0.35。所有 outputs 固定 promotion/paper-shadow/production false、
  broker_action=none、actual_path_validation_ready=false、
  actual_path_validation_executed=false；TRADING-2281 permanently inconclusive 和
  TRADING-2285 inconclusive 结论不变。验证通过 Ruff、compileall、focused
  parallel pytest 15 passed、full parallel pytest 3591 passed / 643 warnings、
  docs freshness、documentation contract、contract-validation、task-register
  consistency 和 `git diff --check`。
