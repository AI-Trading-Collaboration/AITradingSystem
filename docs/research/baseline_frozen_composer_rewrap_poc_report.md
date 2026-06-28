# Baseline Frozen Composer Rewrap POC Report

## 摘要

- task_id: `TRADING-2282_CANDIDATE_SIGNAL_BINDING_SCHEMA_BASELINE_REWRAP_POC`; status: `CANDIDATE_SIGNAL_BINDING_SCHEMA_POC_READY_PROMOTION_BLOCKED`
- source CSV: `outputs\research_trends\models\first_layer_composer_v2_predictions.csv`
- source CSV hash: `c9a8a288f483cb3a37a84383d465e35a5b76ac890e120e5d913468e51a98cde9`
- source schema status: `source_evidence_only`
- source rows=`2205`; rewrapped records=`2205`; validation=`PASS`.

## Rewrap Mapping

| target_field | source |
|---|---|
|`candidate_id`|`constant:baseline`|
|`candidate_family`|`constant:first_layer_proxy_candidate`|
|`source_experiment_id`|`model_id`|
|`source_artifact_hash`|`sha256(source CSV bytes)`|
|`as_of_timestamp`|`available_at`|
|`decision_timestamp`|`decision_at`|
|`horizon`|`expected_horizon_days + 'd'`|
|`signal_value`|`confidence`|
|`signal_direction`|`trend_state enum mapping`|
|`valid_from`|`decision_at`|
|`valid_until`|`decision_at + validity_days`|
|`feature_snapshot_hash`|`feature_snapshot_hash`|
|`model_or_rule_version`|`model_version`|
|`provenance.regeneration_mode`|`schema_migration_poc`|
|`provenance.pit_policy`|`non_pit_source_evidence_only`|
|`promotion_eligible`|`constant:false`|

## Generated Candidate-Bound POC Artifacts

- `candidate_signal_binding_schema_json`: `outputs\research_trends\candidate_signal_binding_schema\candidate_signal_binding_schema.json`
- `candidate_bound_signal_series_contract_json`: `outputs\research_trends\candidate_signal_binding_schema\candidate_bound_signal_series_contract.json`
- `candidate_bound_prediction_artifact_contract_json`: `outputs\research_trends\candidate_signal_binding_schema\candidate_bound_prediction_artifact_contract.json`
- `baseline_rewrapped_candidate_signal_series_csv`: `outputs\research_trends\candidate_signal_binding_schema\baseline_rewrapped_candidate_signal_series.csv`
- `baseline_rewrapped_candidate_prediction_artifact_json`: `outputs\research_trends\candidate_signal_binding_schema\baseline_rewrapped_candidate_prediction_artifact.json`
- `baseline_rewrap_provenance_report_json`: `outputs\research_trends\candidate_signal_binding_schema\baseline_rewrap_provenance_report.json`
- `baseline_rewrap_validation_summary_json`: `outputs\research_trends\candidate_signal_binding_schema\baseline_rewrap_validation_summary.json`
- `candidate_signal_binding_schema_md`: `D:\Work\AITradingSystem\docs\research\candidate_signal_binding_schema.md`
- `candidate_bound_artifact_contract_md`: `D:\Work\AITradingSystem\docs\research\candidate_bound_artifact_contract.md`
- `baseline_frozen_composer_rewrap_poc_report_md`: `D:\Work\AITradingSystem\docs\research\baseline_frozen_composer_rewrap_poc_report.md`

## Validation Summary

- candidate_bound_minimum_fields_satisfied: `True`
- signal error count: `0`
- prediction error count: `0`

## Safety Boundary

- rewrap artifact 不允许 promotion、paper-shadow、production 或 broker action；`promotion_allowed=false`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。
- rewrap 不改变 TRADING-2281 的 permanently inconclusive 结论；它不是 historical executable candidate artifact，也不是 actual-path validation ready。
