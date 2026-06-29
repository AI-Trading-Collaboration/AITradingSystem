# Regenerated Candidate Artifact Summary

## 结论

TRADING-2284 生成的 `baseline_plus_trend_structure`、`risk_appetite` 和
`volatility_regime` artifacts 是 regenerated executable candidate artifacts。它们满足
candidate-bound schema 和 provenance contract，但仍处于 actual-path validation blocked 状态。

顶层 run summary 必须为：

- `task_id=TRADING-2284_TREND_RISK_VOLATILITY_EXECUTABLE_CANDIDATE_GENERATORS`
- `status=REGENERATED_CANDIDATE_ARTIFACTS_READY_ACTUAL_PATH_VALIDATION_BLOCKED`
- `candidate_count=3`
- `target_assets=QQQ,SPY,SMH`
- `horizons=5d,10d,20d`
- `next_task=TRADING-2285_Regenerated_Candidate_Actual_Path_Validation`

## Artifact Contract

每个 candidate 必须包含：

- `candidate_signal_spec.json`
- `candidate_signal_series.csv`
- `candidate_prediction_artifact.json`
- `generation_summary.json`
- `validation_summary.json`

每个 validation summary 必须显示：

- candidate signal spec validation PASS
- candidate-bound signal series validation PASS
- candidate-bound prediction artifact validation PASS
- `candidate_bound_validator_reused=true`

## Provenance

每条 signal / prediction record 必须保留：

- native `candidate_id`
- `source_artifact_hash`
- PIT-aware `as_of_timestamp`、`decision_timestamp`、`valid_from`、`valid_until`
- `signal_spec_version`
- `prediction_schema_version`
- `input_snapshot_hash`
- `feature_snapshot_hash`
- `provenance.regeneration_mode=deterministic_regeneration`
- `provenance.candidate_binding_method=native_candidate_id`
- `provenance.source_schema_status=candidate_bound`

缺失可选 proxy input 时，generation summary / provenance 必须披露 `missing_inputs`、
`proxy_input_used`、`proxy_input_reason` 或 `volatility_proxy_mode`，不得 silent fallback。

## 不得误读

- `historical_executable_artifact=true` 不等于 promotion-ready。
- `actual_path_validation_ready=false` 表示 TRADING-2285 尚未完成 candidate-level actual-path validation。
- `promotion_eligible=false` 表示不得进入 promotion、paper-shadow、production 或 broker。
- TRADING-2282 baseline rewrap POC 仍只是 schema migration POC。
- TRADING-2283 framework smoke artifact 仍只是 framework validation。
- TRADING-2281 permanently inconclusive decisions 不因 2284 自动修改。
