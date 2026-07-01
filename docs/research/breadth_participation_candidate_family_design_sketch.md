# Breadth Participation Candidate Family Design Sketch

本设计草案不生成 candidate-bound artifacts。

## Candidate IDs

- `qqq_breadth_participation_v1`
- `smh_breadth_participation_v1`
- `cross_asset_breadth_quality_v1`
- `mega_cap_concentration_fragility_v1`

## Signal Concepts

|signal_name|usage_role|horizons|data_feasibility|bias_risk|
|---|---|---|---|---|
|`breadth_participation_score`|`confirmation_only`|10d, 20d|CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW|`HIGH_SURVIVORSHIP_BIAS`|
|`advance_decline_participation_score`|`confirmation_only`|5d, 10d, 20d|PROXY_ONLY_UNTIL_HISTORICAL_MEMBERSHIP_SOURCE|`HIGH_SURVIVORSHIP_BIAS`|
|`constituent_momentum_breadth_score`|`confirmation_only`|10d, 20d|BLOCKED_NO_RELIABLE_DATA_FOR_STRICT_PIT|`UNACCEPTABLE_FOR_VALIDATION`|
|`new_high_new_low_proxy_score`|`trend_fragility_warning`|10d, 20d|BLOCKED_NO_RELIABLE_DATA_FOR_STRICT_PIT|`UNACCEPTABLE_FOR_VALIDATION`|
|`mega_cap_concentration_risk_score`|`trend_fragility_warning`|10d, 20d|CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW|`HIGH_LOOKAHEAD_BIAS`|
|`sector_leadership_diffusion_score`|`diagnostic_only`|10d, 20d|PROXY_ONLY_UNTIL_SECTOR_METADATA_AND_MEMBERSHIP_AUDIT|`MODERATE_BIAS`|
|`trend_fragility_score`|`risk_cap_modifier`|10d, 20d|CURRENT_CONSTITUENTS_PROXY_ONLY_FOR_NOW|`HIGH_SURVIVORSHIP_BIAS`|

优先 horizon 是 `10d` / `20d`；breadth 不应作为 1d 噪音信号。
