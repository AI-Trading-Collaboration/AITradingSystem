# Forward Observe Stop / Continue Rules

Continue observe if:
- `sufficient_triggers_accumulating`
- `false_risk_cap_cost_controlled`
- `data_quality_pass_or_pass_with_warnings`
- `operational_report_generation_stable`

Extend observe if:
- `trigger_count_below_minimum`
- `sample_thin_but_no_evidence_of_harm`
- `sparse_asset_horizon_distribution`

Stop observe if:
- `repeated_false_risk_cap_triggers`
- `false_risk_cap_cost_materially_high`
- `data_quality_fails_repeatedly`
- `trigger_staleness_high`
- `signal_no_longer_discriminates_risk`

Escalate to owner precheck if:
- `sufficient_triggers`
- `risk_cap_capture_evidence_positive`
- `false_risk_cap_cost_controlled`
- `evidence_stable_across_at_least_two_review_windows`

禁止 auto_promotion、auto_paper_shadow、auto_production 和 auto_broker_action。
