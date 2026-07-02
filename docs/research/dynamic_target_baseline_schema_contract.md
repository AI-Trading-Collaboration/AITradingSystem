# Dynamic Target Baseline Schema Contract

- baseline_schema_version: `dynamic_target_baseline.v1`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Required Fields

- `baseline_id`
- `source_id`
- `source_type`
- `source_path`
- `source_hash`
- `date`
- `target_asset`
- `target_exposure`
- `risk_asset_exposure`
- `asset_weight`
- `cash_weight`
- `as_of_timestamp`
- `decision_timestamp`
- `valid_from`
- `valid_until`
- `rebalance_flag`
- `rebalance_timestamp`
- `source_artifact_hash`
- `signal_source_id`
- `advisory_id`
- `generated_at`
- `baseline_schema_version`
- `pit_policy`
- `replayability_status`
- `known_at_semantics`
- `promotion_allowed`
- `paper_shadow_allowed`
- `production_allowed`
- `broker_action`

`target_exposure` 是 research baseline field，不得解释为交易 target weight。
