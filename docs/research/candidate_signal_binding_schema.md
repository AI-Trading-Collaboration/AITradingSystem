# Candidate Signal Binding Schema

## 设计目标

本 schema 要求 first-layer candidate signal / prediction artifact 显式绑定 candidate、source artifact、schema version、PIT 时间字段和 provenance。

## 必需字段

`candidate_id`, `candidate_family`, `source_experiment_id`, `source_artifact_id`, `source_artifact_path`, `source_artifact_hash`, `signal_spec_version`, `prediction_schema_version`, `generated_at`, `as_of_timestamp`, `decision_timestamp`, `target_asset`, `horizon`, `signal_name`, `signal_value`, `signal_direction`, `signal_confidence`, `valid_from`, `valid_until`, `input_snapshot_hash`, `feature_snapshot_hash`, `model_or_rule_version`, `provenance`

## Provenance 字段

`source_paths`, `source_hashes`, `regeneration_mode`, `pit_policy`, `candidate_binding_method`, `source_schema_status`, `promotion_eligible`

## PIT 与 Candidate Binding

`as_of_timestamp`、`decision_timestamp`、`valid_from`、`valid_until` 和 `horizon` 必须存在；缺少这些字段的 artifact 不能进入 candidate-level actual-path validation。

## Source Evidence 边界

`source_evidence` 只能证明上游文件存在；`schema_migration_poc_artifact` 只证明 字段可映射；两者都不能被反向声明为 historical executable artifact。

## Promotion Gating

`schema_migration_poc` 必须 `promotion_eligible=false`；`non_pit_source_evidence_only` 必须 `paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`。

## Future Generator 对接

后续 executable generator 应 native 写出本 schema，而不是依赖事后 rewrap；generator 输出通过 validator 后才可进入 candidate-level actual-path validation。
