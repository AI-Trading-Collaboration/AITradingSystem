# Risk-Cap Trigger Follow-Up Schema

Trigger follow-up 只记录 5d / 10d / 20d 后验路径用于复盘。

- schema_version: `risk_cap_trigger_followup_schema.v1`
- followup_horizons: `5d, 10d, 20d`

|field|required|type|
|---|---:|---|
|`followup_record_id`|True|`string`|
|`trigger_record_id`|True|`string`|
|`candidate_id`|True|`string`|
|`target_asset`|True|`string`|
|`followup_horizon`|True|`string`|
|`trigger_date`|True|`date`|
|`followup_due_date`|True|`date`|
|`followup_status`|True|`string`|
|`actual_forward_return`|False|`number`|
|`post_trigger_max_drawdown`|False|`number`|
|`post_trigger_realized_volatility`|False|`number`|
|`stress_event_observed`|True|`boolean`|
|`false_risk_cap_case`|True|`boolean`|
|`missed_stress_case`|True|`boolean`|
|`data_quality_status`|True|`string`|
|`source_price_artifact_path`|True|`string`|
|`row_checksum`|True|`string`|
