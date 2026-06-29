# TRADING-2286 Regenerated Candidate Inconclusive Diagnostics and Signal Utility Drilldown

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2286_REGENERATED_CANDIDATE_INCONCLUSIVE_DIAGNOSTICS`
- priority: `P0`
- status: `DONE`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2285 已读取 TRADING-2284 regenerated candidate artifacts 并完成
candidate-level actual-path validation。真实 run 生成 95,220 条 actual-path /
prediction-outcome records，其中 73,188 条 validation eligible；三类 regenerated
candidates 当前均为 `ACTUAL_PATH_VALIDATED_INCONCLUSIVE`，promotion、paper-shadow、
production 和 broker gate 继续固定 false / none。

TRADING-2286 只诊断 2285 inconclusive 的原因，并下钻 signal utility。它不生成新
signal、不修改 2284 artifacts、不重跑 actual-path validation、不做 owner final
decision，也不允许 promotion、paper-shadow、production 或 broker action。

## 非目标

- 不生成新的 candidate signal。
- 不修改 TRADING-2284 regenerated candidate artifacts。
- 不重新执行 TRADING-2285 actual-path validation。
- 不做 owner final decision、promotion、paper-shadow、production 或 broker action。
- 不做组合层仓位回测或策略参数搜索。
- 不把任何 inconclusive candidate 升级为 paper-shadow candidate。
- 不改变 TRADING-2281 旧 proxy candidates 的 permanently inconclusive 结论。

## 实施步骤

1. 新增 `aits research trends regenerated-candidate-inconclusive-diagnostics` CLI，
   只允许 `mode=inconclusive_diagnostics`。
2. 新增 TRADING-2285 output loader，读取 summary、actual-path matrix、outcome
   matrix、scorecard、error attribution seed、data quality report 和 state
   recommendation matrix，并对 promotion / broker safety fields fail closed。
3. 读取 TRADING-2284 generator context 作为 signal spec / generation summary /
   provenance 补充；若 generator context 缺失但 2285 outputs 完整，记录 partial
   warning，不做 silent fallback。
4. 生成 signal density、confidence distribution、horizon / asset / direction
   alignment、false signal cost、candidate overlap、data quality impact 和
   diagnostic-only regime drilldown matrices。
5. 生成 candidate refinement recommendation matrix 和 utility drilldown summary。
6. 写出 research docs、report registry、artifact catalog、system flow 和 task
   register 更新。
7. 新增 focused loader / density / horizon-asset / false-cost / overlap /
   recommendation / CLI tests。

## Safety Boundary

所有 TRADING-2286 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `owner_review_required=false`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`

任何 recommendation 不得输出 `PROMOTION_READY`、`PAPER_SHADOW_READY`、
`PRODUCTION_READY` 或 `BROKER_READY`。

## Pilot Threshold Governance

TRADING-2286 的 neutral dominance、confidence、local edge、false-cost、overlap、
data-quality materiality 和 regime drilldown thresholds 仅用于 research-only
diagnostics。它们必须以命名常量实现，并由后续 TRADING-2287 refinement /
validation design 复核；不得作为 promotion gate、paper-shadow gate、production rule
或 broker rule。

## 验收标准

- CLI 能读取 TRADING-2285 outputs 并写出所有 required diagnostic runtime artifacts。
- Loader 对 missing actual-path matrix、missing outcome matrix、missing scorecard、
  `promotion_allowed=true` 和 `broker_action!=none` fail closed。
- Signal density 覆盖 neutral ratio、directional ratio、high confidence ratio、
  over-neutralized 和 low conviction labels。
- Horizon / asset drilldown 覆盖 local weak edge、local negative edge、insufficient
  local sample 和 mixed-by-horizon / mixed-by-asset。
- False signal cost 覆盖 false risk-on/off cost、cost asymmetry 和 dominant cost
  labels。
- Candidate overlap 覆盖 signal value correlation、direction agreement、high
  redundancy、complementary 和 unstable disagreement。
- Refinement recommendation 覆盖 over-neutralized、false risk cost、horizon
  mismatch、redundancy、no measurable edge，并保持 promotion blocked。
- TRADING-2281 permanently inconclusive decisions 不被修改。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for seven TRADING-2286 test files
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `python -m ai_trading_system.cli reports task-register-consistency run --as-of 2026-06-30`
- `python -m ai_trading_system.cli reports task-register-consistency validate --latest`
- `git diff --check`

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`；范围限定为 inconclusive diagnostics
  和 signal utility drilldown，不做 promotion、paper-shadow、production 或 broker。
- 2026-06-30: 实现完成并归档为 `DONE`。新增
  `aits research trends regenerated-candidate-inconclusive-diagnostics`、2285 output
  loader、generator context safety check、signal density / confidence / horizon asset /
  direction / false cost / overlap / data-quality impact / diagnostic-only regime
  drilldown、refinement recommendation、utility summary、research docs、report registry、
  artifact catalog、system flow 和 focused tests。真实 run 读取 95,220 条 actual-path /
  prediction-outcome records，其中 73,188 条 validation eligible；3 个 candidates
  primary reason 均为 `LOW_CONFIDENCE_SIGNAL`，recommended action 均为
  `REFINE_CONFIDENCE_SCALING`，next task recommendation 为
  `TRADING-2287_Candidate_Generator_Refinement_Plan`。Data quality impact 对三类
  candidate 均为 `DATA_QUALITY_NOT_MATERIAL`，overlap 结果为 1 个
  `PARTIALLY_REDUNDANT` 和 2 个 `COMPLEMENTARY`，false signal cost 显示
  baseline_plus_trend_structure / risk_appetite 为 `FALSE_RISK_ON_COST_TOO_HIGH`。
  所有 outputs 继续 promotion/paper-shadow/production false、broker_action=none。
  验证通过 Ruff、compileall、focused parallel pytest 19 passed、full parallel pytest
  3557 passed、docs freshness、documentation contract、contract-validation 193 passed、
  task-register consistency 和 `git diff --check`。
