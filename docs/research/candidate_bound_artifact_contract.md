# Candidate-Bound Artifact Contract

## Signal Series Artifact Contract

- format: `csv`
- required columns:
`candidate_id`, `candidate_family`, `source_experiment_id`, `source_artifact_id`, `source_artifact_path`, `source_artifact_hash`, `signal_spec_version`, `prediction_schema_version`, `generated_at`, `as_of_timestamp`, `decision_timestamp`, `target_asset`, `horizon`, `signal_name`, `signal_value`, `signal_direction`, `signal_confidence`, `valid_from`, `valid_until`, `input_snapshot_hash`, `feature_snapshot_hash`, `model_or_rule_version`, `provenance`, `promotion_eligible`, `promotion_allowed`, `paper_shadow_allowed`, `production_allowed`, `broker_action`, `permanently_inconclusive_override_allowed`, `source_row_index`, `source_date`, `source_trend_state`, `source_confidence`, `source_prediction_flags`

## Prediction Artifact Contract

- format: `json`
- required top-level fields:
`artifact_id`, `artifact_role`, `candidate_id`, `candidate_family`, `source_experiment_id`, `source_artifact_id`, `source_artifact_path`, `source_artifact_hash`, `signal_spec_version`, `prediction_schema_version`, `generated_at`, `as_of_timestamp`, `decision_timestamp`, `target_asset`, `horizon`, `signal_name`, `signal_value`, `signal_direction`, `signal_confidence`, `valid_from`, `valid_until`, `input_snapshot_hash`, `feature_snapshot_hash`, `model_or_rule_version`, `provenance`, `promotion_eligible`, `promotion_allowed`, `paper_shadow_allowed`, `production_allowed`, `broker_action`, `permanently_inconclusive_override_allowed`, `prediction_records`, `historical_executable_artifact`, `actual_path_validation_ready`

## Validation Rules

- candidate_id exists and is non-empty
- as_of_timestamp and decision_timestamp exist
- valid_from and valid_until exist and valid_until >= valid_from
- horizon, signal_spec_version, and prediction_schema_version exist
- input_snapshot_hash, feature_snapshot_hash, and source_artifact_hash exist
- provenance.regeneration_mode, pit_policy, and candidate_binding_method exist
- promotion_eligible exists and schema_migration_poc forces it to false
- non_pit_source_evidence_only forces paper_shadow_allowed=false
- non_pit_source_evidence_only forces production_allowed=false
- non_pit_source_evidence_only forces broker_action=none

## Schema Versioning

- signal series: additive_changes_only_until_v2_owner_review
- prediction artifact: v1 is append-only; breaking changes require v2 contract

## Backward Compatibility

- signal series: v1 readers must fail closed when required candidate binding, PIT, hash, or provenance fields are absent
- prediction artifact: missing required binding, PIT, hash, provenance, or safety fields fails closed

## Failure Examples

- missing candidate_id
- missing as_of_timestamp
- valid_until earlier than valid_from
- schema_migration_poc with promotion_eligible=true
- non_pit_source_evidence_only with paper_shadow_allowed=true
- prediction artifact lacks source_artifact_hash
- prediction artifact lacks provenance.regeneration_mode
- schema_migration_poc marked promotion_eligible=true
- non_pit_source_evidence_only marked production_allowed=true
- broker_action other than none
