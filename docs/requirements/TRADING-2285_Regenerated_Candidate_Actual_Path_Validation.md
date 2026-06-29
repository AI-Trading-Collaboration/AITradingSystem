# TRADING-2285 Regenerated Candidate Actual-Path Validation

最后更新：2026-06-30

## 状态

- task_id: `TRADING-2285_REGENERATED_CANDIDATE_ACTUAL_PATH_VALIDATION`
- priority: `P0`
- status: `DONE`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-30

## 背景

TRADING-2284 已在 TRADING-2283 executable candidate generator framework 上实现
`baseline_plus_trend_structure`、`risk_appetite` 和 `volatility_regime` 三个
regenerated first-layer executable candidate generators，并能原生生成
candidate-bound signal spec、signal series、prediction artifact、generation summary
和 validation summary。

TRADING-2285 读取这些 regenerated candidate artifacts，对每条 candidate-bound
prediction record 计算 future actual path，生成 candidate-level outcome matrix、
prediction/outcome scorecard、data quality report、error attribution seed 和 research
state recommendation。

## 非目标

- 不生成新的 candidate signal。
- 不重新训练或修改 TRADING-2284 generator。
- 不修改 2284 regenerated candidate artifacts。
- 不做 owner final decision、promotion、paper-shadow、production 或 broker action。
- 不改变 TRADING-2281 旧 proxy candidates 的 permanently inconclusive 结论。
- 不做策略参数搜索、组合层仓位回测或实盘交易决策。

## 实施步骤

1. 新增 `aits research trends regenerated-candidate-actual-path-validation` CLI，
   只允许 `mode=actual_path_validation`。
2. 新增 artifact loader，读取每个 candidate 的 signal spec、signal series、
   prediction artifact、generation summary 和 validation summary，并复用 2282/2283/2284
   validator fail closed。
3. 复用本地 price cache，按 prediction record 的 `decision_timestamp`、`target_asset`
   和 `horizon` 计算 decision price、horizon end price、forward return、intrahorizon
   drawdown/runup、realized volatility、stress/upside/tail events 和 coverage ratio。
4. 为 incomplete future window、missing decision price、partial price coverage 和
   low coverage ratio 生成显式 data quality status，不做 silent fallback。
5. 实现按 signal direction 区分的 outcome alignment：
   risk-on/trend-confirming、risk-off/trend-weakening/volatility-expansion、neutral 和
   volatility-compression 使用不同正负向解释。
6. 生成 candidate scorecard、prediction outcome matrix、actual-path matrix、data quality
   report、error attribution seed 和 candidate state recommendation matrix。
7. 更新 research docs、report registry、artifact catalog、system flow 和 task register。
8. 新增 focused loader/calculator/alignment/scorecard/CLI tests。

## 安全边界

所有 TRADING-2285 outputs 必须固定：

- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `paper_shadow_recommendation_allowed=false`
- `production_recommendation_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`

`owner_review_required=true` 只允许作为 research recommendation，不代表 owner approval、
paper-shadow readiness、production readiness 或 broker action。

## Pilot Threshold Governance

TRADING-2285 的 alignment、coverage 和 error-classification thresholds 仅用于
research-only actual-path evidence seed。它们必须以命名常量实现，并在后续
TRADING-2286 owner review / risk attribution 中复核；不得作为 promotion gate、
paper-shadow gate、production rule 或 broker rule。

## 验收标准

- CLI 能读取 TRADING-2284 regenerated artifacts 并写出 8 个 required output files。
- Loader 对 missing prediction artifact、invalid schema、promotion_allowed=true 和
  broker_action!=none fail closed。
- Calculator 覆盖 forward return、max drawdown、max runup、realized volatility、missing
  decision price、incomplete window、partial coverage 和 below-threshold coverage。
- Alignment 覆盖 risk-on、risk-off、neutral、volatility-expansion 和
  volatility-compression 的正负向路径。
- Scorecard 生成 alignment rate、confidence weighted score、false risk-on/off counts and
  costs、best/worst horizon 和 research status recommendation。
- 所有输出保持 promotion、paper-shadow、production、broker 阻断。
- TRADING-2281 permanently inconclusive decisions 不被修改。

## 验证计划

- `ruff check .`
- `python -m compileall -q src tests`
- focused parallel pytest for five TRADING-2285 test files
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- task-register consistency
- `git diff --check`

## 进展记录

- 2026-06-30: 新增任务并进入 `IN_PROGRESS`；范围限定为 regenerated candidate
  actual-path validation evidence，不做 owner final decision、promotion、paper-shadow、
  production 或 broker。
- 2026-06-30: 实现完成并归档为 `DONE`。新增
  `aits research trends regenerated-candidate-actual-path-validation`、artifact loader、
  actual-path calculator、alignment classifier、scorecard/data-quality/state recommendation
  输出、error attribution seed、report registry、artifact catalog、system flow 和 focused
  tests。真实 run 读取 TRADING-2284 regenerated artifacts，生成 95,220 条 actual-path /
  prediction-outcome records，其中 73,188 条 validation eligible；source data quality
  status=`PASS_WITH_WARNINGS`、error_count=0，三类 candidate 当前均为
  `ACTUAL_PATH_VALIDATED_INCONCLUSIVE`，promotion/paper-shadow/production/broker 继续
  false/none。验证通过 Ruff、compileall、focused parallel pytest 24 passed、full parallel
  pytest 3538 passed、docs freshness、documentation contract、contract-validation 193
  passed 和 `git diff --check`。
