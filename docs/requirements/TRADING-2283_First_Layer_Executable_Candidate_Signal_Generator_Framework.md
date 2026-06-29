# TRADING-2283 First-Layer Executable Candidate Signal Generator Framework

最后更新：2026-06-29

## 状态

- task_id: `TRADING-2283_FIRST_LAYER_EXECUTABLE_CANDIDATE_SIGNAL_GENERATOR_FRAMEWORK`
- priority: `P0`
- status: `VALIDATING`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-29

## 背景

TRADING-2282 已定义 candidate signal binding schema、candidate-bound signal series /
prediction artifact contract，并用 `baseline` frozen composer prediction CSV 证明字段可
rewrap 成 schema-valid POC artifact。该 POC 只证明 schema mapping 可行，不是 historical
executable candidate artifact，也不能进入 actual-path validation、promotion、paper-shadow、
production 或 broker action。

TRADING-2283 的目标是建立 native first-layer executable candidate generator framework，
让后续 first-layer candidates 从生成时写出 candidate-bound signal spec、signal series、
prediction artifact 和 provenance，而不是依赖事后 rewrap。

## 非目标

- 不实现真实 `baseline_plus_trend_structure`、`risk_appetite` 或 `volatility_regime` generator。
- 不做 actual-path validation、owner review、promotion、paper-shadow、production 或 broker action。
- 不把 TRADING-2282 baseline rewrap POC 升级为 historical executable artifact。
- 不改变 TRADING-2281 四个 permanently inconclusive candidates 的状态。
- 不做策略收益优化、参数搜索或投资结论升级。

## 实施步骤

1. 定义 `FirstLayerCandidateSignalGenerator` protocol、`CandidateGeneratorContext`、
   `CandidateSignalSpec`、generation bundle/result/error 等基础类型。
2. 定义 generator registry，支持 register / get / list，并 fail closed unknown generator。
3. 定义 runtime，负责 context 构造、generator 调用、bundle validation、artifact 写出和 summary。
4. 扩展 TRADING-2282 validator，新增 candidate signal spec validation，并对
   `framework_smoke_test` artifact 强制 fail-closed safety rules。
5. 实现 `framework_smoke_candidate` generator，生成 deterministic synthetic records，只用于
   framework validation。
6. 新增 CLI：
   `aits research trends first-layer-candidate-generator-framework`，初期只允许
   `--mode framework_smoke_test`。
7. 新增 focused unit / CLI tests，覆盖 interface、registry、runtime、smoke generator、
   required fields、validator failure paths 和 promotion boundary。
8. 更新 research docs、report registry、artifact catalog、system flow 和 task register。

## 安全边界

所有 TRADING-2283 输出必须固定：

- `artifact_role=framework_smoke_test`
- `historical_executable_artifact=false`
- `actual_path_validation_ready=false`
- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `permanently_inconclusive_override_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`

以下情况必须失败：缺少 candidate binding、PIT timestamps、schema versions、snapshot hashes、
source artifact hash、provenance regeneration/pit/binding fields，或任何 promotion /
paper-shadow / production / broker safety field 打开。

## 验收标准

- generator framework implemented。
- `framework_smoke_candidate` 可以生成 signal spec、signal series、prediction artifact、
  generation summary 和 validation summary。
- validation summary 为 `PASS`，且复用 TRADING-2282 candidate signal binding validator。
- registry 能 list / get generator，unknown generator 必须失败。
- CLI smoke run 成功写出所有 artifacts。
- framework smoke artifact 不可进入 actual-path validation，不可视为 historical executable artifact。
- report registry / artifact catalog 不把 framework smoke artifacts 标为 promotion artifact。
- TRADING-2281 permanently inconclusive decisions 保持不变。

## 进展记录

- 2026-06-29: 新增任务并进入 `IN_PROGRESS`；开始实现 native generator framework，不触发任何投资解释升级或生产路径。
- 2026-06-29: 实现完成并转入 `VALIDATING`；新增 generator interface、registry、runtime、framework smoke generator、CLI、validator 扩展、focused tests、research docs、registry/catalog/system flow 更新。真实 smoke run 输出 65 条 records，validation summary 为 `PASS`，所有 promotion/paper-shadow/production/broker safety fields 继续 false/none。
- 2026-06-29: 验证通过 Ruff、compileall、focused parallel pytest（24 passed）、CLI smoke、docs freshness、documentation contract、contract-validation tier（193 passed；runtime artifact=`outputs/validation_runtime/contract-validation_20260629T010912Z/test_runtime_summary.json`）和 `git diff --check`。全量 parallel pytest 仍有 2 个非本任务 daily scheduler ordering failures；task-register consistency 当时被 TQQQ data quality watchlist 的旧非法 task id 阻断，后续由 TRADING-2298 重新编号并修复。
- 2026-06-29: TRADING-2283 validation cleanup 完成并转入 `DONE`。TRADING-2298 修复 malformed task id 与 daily scheduler ordering tests 后，复验 full parallel pytest 3504 passed / 643 warnings、task-register consistency run/validate PASS、docs freshness PASS、documentation contract PASS、Ruff PASS、`contract-validation` tier 193 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260629T142253Z/test_runtime_summary.json`）和 `git diff --check` PASS。2283 不启动 trend/risk/volatility executable generators；下一步另开 TRADING-2284。
