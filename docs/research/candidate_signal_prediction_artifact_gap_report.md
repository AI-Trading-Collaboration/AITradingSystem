# Candidate Signal / Prediction Artifact Gap Report

## 摘要

- task_id: `TRADING-2281_CANDIDATE_SIGNAL_PREDICTION_ARTIFACT_BACKFILL`; status: `CANDIDATE_SIGNAL_PREDICTION_ARTIFACT_AUDIT_READY_PROMOTION_BLOCKED`
- inconclusive_candidate_count=`4`; artifact_row_count=`28`; backfilled_artifact_count=`0`.
- promotion_allowed=`false`; paper_shadow_allowed=`false`; production_allowed=`false`; broker_action=`none`.

## Candidate Gap Summary

| candidate | primary_missing_reason | missing_artifact_types | backfill_possible | permanently_inconclusive |
|---|---|---|---:|---:|
|`baseline`|`never_generated`|`candidate_signal_spec, candidate_signal_series, candidate_prediction_artifact, candidate_actual_path_backtest, candidate_risk_attribution, registry_reference`|`False`|`True`|
|`baseline_plus_trend_structure`|`never_generated`|`candidate_signal_spec, candidate_signal_series, candidate_prediction_artifact, candidate_actual_path_backtest, candidate_risk_attribution, registry_reference`|`False`|`True`|
|`risk_appetite`|`never_generated`|`candidate_signal_spec, candidate_signal_series, candidate_prediction_artifact, candidate_actual_path_backtest, candidate_risk_attribution, registry_reference`|`False`|`True`|
|`volatility_regime`|`never_generated`|`candidate_signal_spec, candidate_signal_series, candidate_prediction_artifact, candidate_actual_path_backtest, candidate_risk_attribution, registry_reference`|`False`|`True`|

## 结论

- 4 个 candidates 都只有 offline experiment definition；缺少可执行 candidate signal spec / signal series / prediction artifact，因此当前不能执行 candidate-level actual-path backtest。
- `baseline` 有 frozen composer prediction source，但该 CSV 缺少 `candidate_id` 和 candidate signal binding schema，只能作为 source evidence，不能直接视为 candidate-bound artifact。
- 本批没有可直接 backfill 的完整 candidate-level artifact；所有 4 个 candidates 在当前证据链下保持 permanently inconclusive。

## 产物

- `candidate_signal_prediction_artifact_gap_report`: `D:\Work\AITradingSystem\docs\research\candidate_signal_prediction_artifact_gap_report.md`
- `candidate_artifact_provenance_matrix_md`: `D:\Work\AITradingSystem\docs\research\candidate_artifact_provenance_matrix.md`
- `inconclusive_candidate_recovery_plan_md`: `D:\Work\AITradingSystem\docs\research\inconclusive_candidate_recovery_plan.md`
- `candidate_artifact_provenance_matrix_json`: `D:\Work\AITradingSystem\outputs\research_trends\candidate_signal_prediction_artifact_audit\candidate_artifact_provenance_matrix.json`
- `candidate_artifact_gap_matrix_json`: `D:\Work\AITradingSystem\outputs\research_trends\candidate_signal_prediction_artifact_audit\candidate_artifact_gap_matrix.json`
- `inconclusive_candidate_recovery_plan_json`: `D:\Work\AITradingSystem\outputs\research_trends\candidate_signal_prediction_artifact_audit\inconclusive_candidate_recovery_plan.json`
