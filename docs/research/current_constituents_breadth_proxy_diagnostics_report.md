# Current Constituents Breadth Proxy Diagnostics

TRADING-2303 只生成 current constituents proxy diagnostics-only 包。

- status: `CURRENT_CONSTITUENTS_PROXY_DIAGNOSTICS_SOURCE_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `source_blocked_current_snapshot_diagnostics`
- data_quality_status: `NOT_APPLICABLE_SOURCE_BLOCKED_STATIC_DIAGNOSTICS`
- source_snapshot_status: `ALL_TARGET_CURRENT_CONSTITUENTS_SNAPSHOTS_MISSING`
- recommended_next_action: `REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT`

## Source Coverage

|target_etf|snapshot_found|source_status|required_before_computation|
|---|---:|---|---|
|`QQQ`|False|`MISSING_CURRENT_CONSTITUENTS_SNAPSHOT`|frozen_current_constituent_snapshot; source/provider record; download timestamp; row count; checksum; constituent price coverage audit|
|`SPY`|False|`MISSING_CURRENT_CONSTITUENTS_SNAPSHOT`|frozen_current_constituent_snapshot; source/provider record; download timestamp; row count; checksum; constituent price coverage audit|
|`SMH`|False|`MISSING_CURRENT_CONSTITUENTS_SNAPSHOT`|frozen_current_constituent_snapshot; source/provider record; download timestamp; row count; checksum; constituent price coverage audit|

## Signal Distribution

|signal_name|distribution_status|diagnostics_grade|reason|
|---|---|---|---|
|`breadth_participation_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`advance_decline_participation_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`constituent_momentum_breadth_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`new_high_new_low_proxy_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`mega_cap_concentration_risk_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`sector_leadership_diffusion_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|
|`trend_fragility_score`|`NOT_COMPUTABLE_CURRENT_CONSTITUENTS_SNAPSHOT_MISSING`|`source_blocked`|No auditable frozen current constituents snapshot exists for QQQ / SPY / SMH.|

## Asset / Horizon Drilldown

- row_count: `27`
- all rows remain source-blocked until frozen current constituent snapshots exist.

## Bias Warning

- warning_status: `CURRENT_CONSTITUENTS_PROXY_HIGH_BIAS_SOURCE_BLOCKED`
- `survivorship_bias`: severity=`high`, risk=invalid historical breadth conclusion if backfilled
- `lookahead_bias`: severity=`high`, risk=false confidence in actual-path validation
- `mega_cap_concentration`: severity=`moderate_high`, risk=fragility warning may be overstated or understated without weights

## Next Step

- recommendation_status: `REQUEST_CURRENT_CONSTITUENTS_SNAPSHOT`
- owner_input_required: `True`
- exit_condition_for_source_blocker: Frozen current constituents snapshot plus constituent price coverage audit for every target ETF.

## Safety

pit_status=`current_constituents_proxy_only`, strict_pit_ready=`False`, promotion_allowed=`False`, paper_shadow_allowed=`False`, production_allowed=`False`, broker_action=`none`, candidate_artifact_generated=`False`, actual_path_validation_executed=`False`.

本报告不得用于 candidate generation、actual-path validation、promotion、paper-shadow、production 或 broker action。
