# Risk-Cap Daily Observe Record Schema

每日 observe record 是 append-only research evidence，不是交易指令。

- schema_version: `risk_cap_daily_observe_record_schema.v1`
- allowed_action_values: `observe_only`
- data_quality_status_values: `PASS, PASS_WITH_WARNINGS`

|field|required|type|
|---|---:|---|
|`record_id`|True|`string`|
|`report_date`|True|`date`|
|`generated_at`|True|`datetime`|
|`candidate_id`|True|`string`|
|`market_regime`|True|`string`|
|`data_quality_status`|True|`string`|
|`risk_cap_triggered`|True|`boolean`|
|`triggered_assets`|True|`array[string]`|
|`triggered_horizons`|True|`array[string]`|
|`risk_cap_score`|True|`number`|
|`risk_cap_intensity`|True|`number`|
|`risk_cap_reason`|True|`string`|
|`trigger_interpretation`|True|`string`|
|`source_signal_records`|True|`array[object]`|
|`source_artifact_paths`|True|`array[string]`|
|`source_artifact_checksums`|True|`object`|
|`allowed_action`|True|`string`|
|`manual_review_notes`|False|`string`|
|`row_checksum`|True|`string`|
