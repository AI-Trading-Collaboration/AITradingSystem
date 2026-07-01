# Signal Validity / Aging Runtime Design

TRADING-2322 承接 TRADING-2321 design-only execution mechanics。本报告只定义 signal validity / trigger aging runtime schema contract，不写入 runtime records，不读取或写入 portfolio weights，不生成 target weight、rebalance instruction 或 broker order，不启动 paper-shadow、production 或 broker path。

- status: `SIGNAL_VALIDITY_AGING_RUNTIME_DESIGN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- data_quality_status: `NOT_APPLICABLE_DESIGN_ONLY_SIGNAL_VALIDITY_AGING`
- source_status: `RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED`
- source_runtime_started: `False`
- source_execution_runtime_started: `False`
- lifecycle_field_count: `6`
- aging_rule_count: `5`
- trigger_aging_state_count: `4`
- release_restore_rule_count: `4`
- aging_runtime_started: `False`
- target_weight_generated: `False`
- rebalance_instruction_generated: `False`
- broker_order_generated: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Lifecycle Fields

|field_id|field_role|allowed_current_effect|
|---|---|---|
|`valid_from`|first_timestamp_or_trading_date_when_signal_context_is_observable|schema_contract_only|
|`valid_until`|design_placeholder_for_latest_review_date_or_expiry_boundary|schema_contract_only_no_expiry_execution|
|`decay`|non_executable_signal_strength_decay_state|design_contract_only_no_decay_multiplier|
|`staleness`|manual_review_route_for_old_or_unresolved_signal_context|manual_review_contract_only|
|`trigger_aging`|elapsed_followup_checkpoint_state_for_risk_cap_trigger|aging_state_contract_only|
|`release_restore_rule`|owner_review_required_rule_for_leaving_aging_or_cooldown_state|release_restore_schema_only|

## Aging Rules

|rule_id|lifecycle_field|trigger_state|rule_status|
|---|---|---|---|
|`record_valid_from_on_observe_context`|`valid_from`|`fresh_observe_context`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`keep_valid_until_unexecuted_until_calibrated`|`valid_until`|`valid_until_design_placeholder`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`route_decay_to_checkpoint_review`|`decay`|`checkpoint_review_pending`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`route_staleness_to_manual_review`|`staleness`|`stale_pending_review`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`release_restore_requires_review_record`|`release_restore_rule`|`release_restore_review_pending`|`DESIGN_ONLY_NOT_EXECUTABLE`|

## Trigger Aging States

|state_id|state_role|aging_status|
|---|---|---|
|`fresh_observe_context`|valid_from_context_recorded_design_only|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`checkpoint_review_pending`|followup_checkpoint_requires_manual_review|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`stale_pending_review`|stale_or_unresolved_trigger_requires_manual_review|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`release_restore_review_pending`|release_or_restore_request_requires_owner_review|`DESIGN_ONLY_NOT_EXECUTABLE`|

## Release / Restore Rules

|rule_id|source_state|restore_target_state|release_allowed_now|
|---|---|---|---|
|`no_automatic_release_from_no_add`|`no_add_mode`|`normal_observe_context`|`False`|
|`no_automatic_release_from_reduced_exposure`|`reduced_max_exposure_mode`|`exposure_cap_policy_review`|`False`|
|`manual_review_required_for_stale_signal`|`manual_review_mode`|`manual_review_resolution_record`|`False`|
|`cooldown_release_requires_followup_review`|`cooldown_mode`|`normal_observe_context`|`False`|

## Boundary

当前 validity duration、valid_until expiry action、decay multiplier、staleness threshold 和 release / restore threshold 均未校准为可执行 policy。后续若进入 TRADING-2323 simulation 或 runtime implementation，必须重新执行 data-quality gate、owner review 和 broker / production safety review。
