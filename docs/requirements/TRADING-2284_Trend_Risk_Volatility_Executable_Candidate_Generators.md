# TRADING-2284 Trend / Risk / Volatility Executable Candidate Generators

最后更新：2026-06-29

## 状态

- task_id: `TRADING-2284_TREND_RISK_VOLATILITY_EXECUTABLE_CANDIDATE_GENERATORS`
- priority: `P0`
- status: `DONE`
- owner: 系统实现 + 项目 owner 后续复核
- last_update: 2026-06-29

## 背景

TRADING-2282 已定义 candidate signal binding schema、candidate-bound artifact
contract、validator 和 baseline rewrap POC。TRADING-2283 已完成 first-layer
executable candidate generator framework、registry、runtime、framework smoke generator
和 CLI，并通过 full validation。

TRADING-2284 在 2283 framework 上新增真实 first-layer executable candidate
generators：

- `baseline_plus_trend_structure`
- `risk_appetite`
- `volatility_regime`

本批只负责依据历史输入重新生成 schema-valid、candidate-bound、
promotion-blocked artifacts，为 TRADING-2285 candidate-level actual-path validation
提供输入。

## 非目标

- 不做 candidate-level actual-path validation。
- 不做 risk attribution review、owner review、promotion、paper-shadow、production 或 broker action。
- 不修改 TRADING-2281 中旧 proxy candidates 的 permanently inconclusive 结论。
- 不使用 TRADING-2282 baseline rewrap POC 作为真实 candidate evidence。
- 不做策略参数搜索、utility 最终排序或 promotion-ready 标记。

## 实施步骤

1. 扩展 TRADING-2283 generator framework，使 `regenerated_candidate_artifacts` mode 能
   生成 `regenerated_executable_candidate_artifact`，同时保留 `framework_smoke_test`
   的 historical executable / actual-path safety 禁止规则。
2. 新增共用 regenerated candidate generator helper，用本地可复现价格输入构造 PIT-aware
   signal records、prediction artifact、provenance、missing input disclosure 和 safety gates。
3. 实现 `baseline_plus_trend_structure` generator，输出 trend structure /
   confirmation / weakening / relative strength signals。
4. 实现 `risk_appetite` generator，输出 risk appetite / risk-on confirmation /
   risk-off pressure / semiconductor risk appetite signals。
5. 实现 `volatility_regime` generator，输出 volatility regime / expansion / stress /
   compression signals，并在缺少 VIX 时显式记录 realized-volatility-only proxy。
6. 注册 3 个 generators，并新增批量 CLI：
   `aits research trends first-layer-candidate-generators-regenerate`。
7. 每个 candidate 写出 `candidate_signal_spec.json`、`candidate_signal_series.csv`、
   `candidate_prediction_artifact.json`、`generation_summary.json` 和
   `validation_summary.json`；顶层写出 `generator_registry.json`、
   `regeneration_run_summary.json` 和 `validation_summary.json`。
8. 新增 focused unit / CLI / regression tests，覆盖 validator PASS、registry、
   safety gates、NaN/inf fail-closed、TRADING-2281/2282/2283 promotion boundary。
9. 更新 research docs、report registry、artifact catalog、system flow 和 task register。

## 安全边界

所有 TRADING-2284 artifacts 必须固定：

- `artifact_role=regenerated_executable_candidate_artifact`
- `historical_executable_artifact=true`
- `actual_path_validation_ready=false`
- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `permanently_inconclusive_override_allowed=false`
- `trading_2281_permanently_inconclusive_decisions_changed=false`

`historical_executable_artifact=true` 只表示 artifact 由 executable generator 按历史输入重新生成；
它不代表 promotion-ready，不代表 actual-path validation 已完成，也不改变 2281 旧候选结论。

## 输入与 provenance

优先使用本地可复现 price cache 中的 QQQ、SPY、SMH 以及可选 proxy ticker。缺少可选
proxy 时必须在 generation summary / provenance 中记录 `missing_inputs`、
`proxy_input_used` 或 `volatility_proxy_mode`，不得 silent fallback。若核心输入不足，
输出 neutral / low-confidence signal，并记录原因；若无法满足 candidate-bound contract，
必须 fail closed。

## 验收标准

- 3 个 generators 均实现 `build_signal_spec`、`generate_signal_series`、
  `generate_prediction_artifact`。
- 3 个 generators 均可通过 registry list / get 获取，unknown generator 失败。
- CLI 能按 candidates、target assets、date range、horizons 和 output dir 批量生成 artifacts。
- 每个 candidate signal spec、series、prediction artifact 和 generation / validation summary 存在。
- 所有 candidate validation summary 为 `PASS`，并复用 TRADING-2282 validator。
- 顶层 regeneration run summary 标记
  `REGENERATED_CANDIDATE_ARTIFACTS_READY_ACTUAL_PATH_VALIDATION_BLOCKED`。
- 所有 promotion / paper-shadow / production / broker / actual-path gates 保持 false/none。
- TRADING-2281 permanently inconclusive decisions 不被修改。
- TRADING-2282 baseline rewrap POC 不被提升为 promotion artifact。
- TRADING-2283 framework smoke artifact 不被提升为 actual-path validation ready。

## 验证计划

- `ruff check .`
- `python -m compileall src tests`
- focused parallel pytest for the five TRADING-2284 test files
- full parallel pytest
- `python -m ai_trading_system.cli docs validate-freshness`
- `python -m ai_trading_system.cli docs report-contract --latest`
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`
- `git diff --check`

## 进展记录

- 2026-06-29: 新增任务并进入 `IN_PROGRESS`；范围限定为 regenerated candidate artifacts，
  不做 actual-path validation、owner review、promotion、paper-shadow、production 或 broker。
- 2026-06-29: 实现完成并归档为 `DONE`。新增三类 regenerated first-layer executable
  candidate generators、批量 CLI、validator 扩展、registry 更新、research 文档、artifact
  catalog、report registry、system flow 和 focused tests。真实 regenerate run 在
  `outputs/research_trends/first_layer_candidate_generators_regenerated/` 生成 3 个
  candidate-bound artifact bundle，顶层 validation status=`PASS`，data quality status=
  `PASS_WITH_WARNINGS` 且 error_count=0；所有 artifacts 保持 actual-path validation、
  promotion、paper-shadow、production 和 broker action 阻断。验证通过 Ruff、compileall、
  focused parallel pytest 10 passed、full parallel pytest 3514 passed、docs freshness、
  documentation contract、contract-validation 193 passed 和 `git diff --check`。
