# TRADING-2281 Candidate Signal / Prediction Artifact Backfill and Provenance Audit

最后更新：2026-06-29

## 背景

TRADING-2280 将 4 个 `OFFLINE_VALIDATION_READY` challenger rows 重分类为
`INCONCLUSIVE`，原因是缺少 candidate-level signal / prediction artifact，不能执行
actual-path validation。TRADING-2281 的目标不是优化策略或补造收益，而是补齐证据链：
确认缺失的是哪类 artifact、缺失原因是什么、是否可以通过可复现命令 backfill 或重新注册。

## 范围

- 覆盖 4 个 TRADING-2280 `INCONCLUSIVE` candidates：
  `baseline`、`baseline_plus_trend_structure`、`risk_appetite` 和 `volatility_regime`。
- 审计 required artifact types：
  candidate signal spec、candidate signal series、candidate prediction artifact、
  candidate actual-path backtest artifact、risk attribution artifact 和 registry reference。
- 判断缺失原因：
  `never_generated`、`generated_but_unregistered`、`registry_missing_reference`、
  `path_drift`、`schema_incompatible`、`ignored_outputs_cleaned`。
- 对可恢复 artifact 输出 backfill / registry repair plan；对不可恢复 artifact 明确标记
  permanently inconclusive，且不允许 promotion / paper-shadow。
- 输出 candidate-level provenance matrix，不训练模型、不调参、不打开 promotion、paper-shadow、
  production 或 broker。

## 输出

- `docs/research/candidate_signal_prediction_artifact_gap_report.md`
- `docs/research/candidate_artifact_provenance_matrix.md`
- `docs/research/inconclusive_candidate_recovery_plan.md`
- `outputs/research_trends/candidate_signal_prediction_artifact_audit/candidate_artifact_provenance_matrix.json`
- `outputs/research_trends/candidate_signal_prediction_artifact_audit/candidate_artifact_gap_matrix.json`
- `outputs/research_trends/candidate_signal_prediction_artifact_audit/inconclusive_candidate_recovery_plan.json`

## 验收标准

- 每个 `INCONCLUSIVE` candidate 都有明确缺失 artifact 类型和原因。
- 每个 candidate 都有 `backfill_possible` 和 `recovery_action` 判断。
- 可 backfill 的 artifact 必须有可复现命令或 registry repair action。
- 不可 backfill 的 candidate 明确 `permanently_inconclusive=true`，并固定
  `promotion_allowed=false`、`paper_shadow_allowed=false`。
- report registry、artifact catalog、system flow 和 focused tests 同步更新。
- `contract-validation` 继续通过。

## 进展

- 2026-06-29：新增并进入 `IN_PROGRESS`；先审计现有 candidate definitions、
  report registry、artifact catalog 和本地 outputs，禁止把 offline experiment definition
  当作 executable signal / prediction artifact。
- 2026-06-29：实现完成并转入 `VALIDATING`；新增 CLI
  `aits research trends candidate-signal-prediction-artifact-audit`，生成 gap report、
  provenance matrix、recovery plan 和对应 JSON artifacts。真实 run 覆盖 4 个
  `INCONCLUSIVE` candidates、28 个 artifact checks，完整 candidate signal/prediction
  artifact count=`0`，backfill_possible_candidate_count=`0`，
  backfilled_artifact_count=`0`，permanently_inconclusive_count=`4`。
- 2026-06-29：验证通过 Ruff、compileall、focused parallel pytest（48 passed）、
  docs freshness、`git diff --check` 和 `contract-validation` tier（193 passed）；
  runtime artifact=`outputs/validation_runtime/contract-validation_20260628T161659Z/test_runtime_summary.json`。

## 当前结论

- 4 个 candidates 都有已注册的 offline experiment definition，但这不是 executable
  candidate signal spec。
- `baseline` 有 frozen composer prediction source
  `outputs/research_trends/models/first_layer_composer_v2_predictions.csv`，但缺少
  `candidate_id` 和 candidate signal binding schema，只能作为 source evidence，不能
  视为 candidate-bound signal / prediction artifact。
- `baseline_plus_trend_structure`、`risk_appetite` 和 `volatility_regime` 的 signal spec、
  signal series、prediction artifact、actual-path backtest、risk attribution 和
  first-layer candidate registry reference 当前均未生成。
- 本批没有可直接 backfill 的完整 candidate-level artifact；4 个 candidates 在当前证据链下
  均为 `permanently_inconclusive=true`，promotion_ready 仍为 false。
