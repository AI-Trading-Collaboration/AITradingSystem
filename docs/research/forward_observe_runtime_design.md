# Forward Observe Runtime Design

TRADING-2294 只定义 risk-cap observe-only evidence accumulation contract。

- candidate_id: `volatility_regime_scope_narrowed_risk_cap_v1`
- source_readiness_gate_status: `FORWARD_OBSERVE_READY_WITH_WARNINGS`
- source_data_quality_status: `PASS_WITH_WARNINGS`
- runtime_started: `False`
- daily_report_integration: `design_only`
- weekly_report_integration: `design_only`

## Pipeline Contract

- `run_aits_validate_data_or_same_code_path_before_record_generation`
- `load_latest_scope_narrowed_risk_cap_candidate_signal`
- `derive_trigger_state_without_portfolio_action`
- `write_daily_observe_record_with_source_hashes`
- `schedule_followup_records_for_5d_10d_20d`

## Decision Matrix

|case_id|decision|source_warning_active|
|---|---|---:|
|`sufficient_stable_triggers`|`continue_observe`|False|
|`sparse_trigger_sample`|`extend_observe`|True|
|`data_quality_failure`|`stop_record_generation_until_quality_passes`|False|
|`false_risk_cap_cost_high`|`stop_observe_or_redesign`|False|
|`stress_capture_stable`|`owner_precheck_candidate_only`|False|
|`long_active_stale`|`staleness_warning_and_manual_review`|False|

本设计不启动 runtime，不接生产日报，不产生 paper-shadow、production 或 broker action。
