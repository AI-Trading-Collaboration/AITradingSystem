# TRADING-2282 Candidate Signal Binding Schema + Baseline Rewrap POC

最后更新：2026-06-29

## 背景

TRADING-2281 确认 4 个 TRADING-2280 `INCONCLUSIVE` candidates 没有完整
candidate signal / prediction artifact，且 `baseline` 的 frozen composer prediction CSV
只能作为 source evidence。该 CSV 缺少 `candidate_id` 与 candidate signal binding schema，
不能被反向声明为 historical executable candidate artifact。

TRADING-2282 的目标是定义 future first-layer candidate artifact schema，并用
`baseline` source evidence 做一次严格受限的 schema migration / rewrap POC。该 POC 只验证
source evidence 能否映射到 candidate-bound shape，不恢复 TRADING-2281 的候选状态。

## 范围

- 新增 `aits research trends candidate-signal-binding-schema-poc`。
- 定义 candidate signal binding schema、candidate-bound signal series contract 和
  candidate-bound prediction artifact contract。
- 读取 `outputs/research_trends/models/first_layer_composer_v2_predictions.csv`，计算 source
  artifact hash，并生成 `baseline` rewrapped schema migration POC artifacts。
- 新增 `CandidateSignalBindingValidator`，校验 candidate binding、PIT timestamp、schema
  version、snapshot hash、source hash、provenance 和 promotion / paper-shadow /
  production / broker gate。
- 生成 Markdown design/contract/report 文档和 runtime JSON/CSV artifacts。
- 更新 report registry、artifact catalog、system flow 和 focused tests。

## 非目标

- 不改变 TRADING-2281 的 `permanently_inconclusive=true` 结论。
- 不把 rewrapped artifact 声称为历史上真实存在的 executable candidate artifact。
- 不执行新的 actual-path validation、策略参数搜索、owner review promotion、paper-shadow、
  production 或 broker action。
- 不实现 `baseline_plus_trend_structure`、`risk_appetite` 或 `volatility_regime` 的 executable
  signal generator。

## 阶段拆解

1. Schema / contract model：新增 schema model 与 machine-readable contract artifacts。
2. Validator：实现 candidate-bound signal series、prediction artifact 和 promotion gating
   校验。
3. Baseline rewrap POC：把 frozen composer prediction rows 映射为 `candidate_id=baseline`
   的 schema migration POC signal series 和 prediction artifact。
4. CLI / docs / registry：接入 research trends CLI，生成 design docs、POC report、
   report registry、artifact catalog 和 system flow 更新。
5. Validation：覆盖缺字段失败、non-PIT gate 禁用、TRADING-2281 状态不被覆盖和 registry
   不标记 promotion artifact。

## 输出

- `docs/research/candidate_signal_binding_schema.md`
- `docs/research/candidate_bound_artifact_contract.md`
- `docs/research/baseline_frozen_composer_rewrap_poc_report.md`
- `outputs/research_trends/candidate_signal_binding_schema/candidate_signal_binding_schema.json`
- `outputs/research_trends/candidate_signal_binding_schema/candidate_bound_signal_series_contract.json`
- `outputs/research_trends/candidate_signal_binding_schema/candidate_bound_prediction_artifact_contract.json`
- `outputs/research_trends/candidate_signal_binding_schema/baseline_rewrapped_candidate_signal_series.csv`
- `outputs/research_trends/candidate_signal_binding_schema/baseline_rewrapped_candidate_prediction_artifact.json`
- `outputs/research_trends/candidate_signal_binding_schema/baseline_rewrap_provenance_report.json`
- `outputs/research_trends/candidate_signal_binding_schema/baseline_rewrap_validation_summary.json`

## 验收标准

- CLI 能生成所有 schema、contract、rewrap、provenance 和 validation artifacts。
- Rewrapped rows/artifact 包含 `candidate_id=baseline`、`source_artifact_hash`、
  `as_of_timestamp`、`decision_timestamp`、`horizon`、`signal_spec_version`、
  `prediction_schema_version` 和 required provenance fields。
- `provenance.regeneration_mode=schema_migration_poc`、
  `provenance.pit_policy=non_pit_source_evidence_only`、
  `provenance.candidate_binding_method=rewrap_mapping` 且
  `promotion_eligible=false`。
- Safety fields 固定为 `promotion_allowed=false`、`paper_shadow_allowed=false`、
  `production_allowed=false`、`broker_action=none`、
  `permanently_inconclusive_override_allowed=false`。
- Report registry 不把 POC artifact 标成 promotion artifact。
- Ruff、compileall、focused parallel pytest、docs freshness、documentation/task consistency
  checks 和 contract-validation 通过。

## 进展

- 2026-06-29：新增并进入 `IN_PROGRESS`；本批承接 TRADING-2281，只做 schema /
  provenance POC，不恢复旧 candidate state，不允许 promotion / paper-shadow / production /
  broker。
- 2026-06-29：实现完成并转入 `VALIDATING`；新增 schema model、validator、baseline rewrap
  builder 和 CLI `aits research trends candidate-signal-binding-schema-poc`。真实 run 读取
  baseline frozen composer predictions，source_row_count=`2205`，生成 rewrapped signal records
  `2205`、prediction records `2205`，validation status=`PASS`，source CSV hash=`c9a8a288f483cb3a37a84383d465e35a5b76ac890e120e5d913468e51a98cde9`。
- 2026-06-29：验证通过 Ruff、compileall、focused parallel pytest（12 passed）、docs
  freshness、documentation/task consistency checks（14 passed）、`git diff --check` 和
  `contract-validation` tier（193 passed）；runtime artifact=`outputs/validation_runtime/contract-validation_20260628T163658Z/test_runtime_summary.json`。
