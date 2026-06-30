# Scope-Narrowed Candidate Scope Filter Report

|scope_narrowed_candidate_id|usage_role|source_record_count|active_record_count|active_ratio|inactive_reasons|
|---|---|---:|---:|---:|---|
|baseline_plus_trend_structure_scope_narrowed_confirmation_v1|confirmation_only|31428|3667|0.116679|{"neutral_not_active": 14352, "not_high_conviction_scope": 20429, "outside_kept_direction_scope": 14352, "outside_kept_horizon_scope": 20952, "usage_role_incompatible": 14352}|
|volatility_regime_scope_narrowed_risk_cap_v1|risk_cap_only|32364|373|0.011525|{"neutral_not_active": 16938, "not_high_conviction_scope": 21037, "outside_kept_asset_scope": 21576, "outside_kept_direction_scope": 16938, "outside_kept_horizon_scope": 21576, "usage_role_incompatible": 25212}|

`baseline_plus_trend_structure` 被收窄为 `confirmation_only`；`volatility_regime` 被收窄为 `risk_cap_only`。
Inactive records 保留在 artifacts 中，并通过 inactive reasons 显式记录过滤原因。
