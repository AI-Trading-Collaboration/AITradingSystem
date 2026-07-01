# Risk-Cap Cooldown / Decay Design

TRADING-2321 承接 TRADING-2294 observe-only evidence plan。本报告只定义 risk-cap trigger 后的 design-only execution mechanics，不读取或写入 portfolio weights，不生成 target weight、rebalance instruction 或 broker order，不启动 paper-shadow、production 或 broker path。

- status: `RISK_CAP_COOLDOWN_DECAY_DESIGN_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- data_quality_status: `NOT_APPLICABLE_DESIGN_ONLY_EXECUTION_MECHANICS`
- source_status: `FORWARD_OBSERVE_EVIDENCE_ACCUMULATION_PLAN_READY_PROMOTION_BLOCKED`
- source_observe_mode: `observe_only`
- source_runtime_started: `False`
- state_count: `4`
- cooldown_decay_rule_count: `3`
- exposure_cap_state_count: `4`
- transition_count: `5`
- target_weight_generated: `False`
- rebalance_instruction_generated: `False`
- broker_order_generated: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Execution States

|state_id|state_role|allowed_current_effect|
|---|---|---|
|`cooldown_mode`|hold_execution_state_after_trigger_clears_until_review_checkpoint|cooldown_design_contract_only|
|`manual_review_mode`|route_ambiguous_or_high_impact_trigger_to_manual_review|manual_review_contract_only|
|`no_add_mode`|block_new_risk_on_additions_design_only|observe_only_design_contract|
|`reduced_max_exposure_mode`|cap_future_max_exposure_design_only|design_contract_only_no_numeric_cap|

## Cooldown / Decay Rules

|rule_id|checkpoint|rule_status|
|---|---|---|
|`cooldown_decay_checkpoint_5d`|`5d`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`cooldown_decay_checkpoint_10d`|`10d`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`cooldown_decay_checkpoint_20d`|`20d`|`DESIGN_ONLY_NOT_EXECUTABLE`|

## Exposure Cap States

|state_id|cap_scope|cap_multiplier_status|
|---|---|---|
|`cooldown_mode`|hold_release_until_review_checkpoint_design|`owner_calibration_required_after_simulation`|
|`manual_review_mode`|manual_review_no_numeric_cap|`owner_calibration_required_after_simulation`|
|`no_add_mode`|block_new_additions_only_design|`owner_calibration_required_after_simulation`|
|`reduced_max_exposure_mode`|future_max_exposure_cap_design_requires_calibration|`owner_calibration_required_after_simulation`|

## Transitions

|rule_id|from_state|to_state|transition_status|
|---|---|---|---|
|`trigger_to_no_add`|`normal_observe`|`no_add_mode`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`trigger_to_reduced_max_exposure_review`|`no_add_mode`|`reduced_max_exposure_mode`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`ambiguous_trigger_to_manual_review`|`any_design_state`|`manual_review_mode`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`cleared_trigger_to_cooldown`|`no_add_mode`|`cooldown_mode`|`DESIGN_ONLY_NOT_EXECUTABLE`|
|`cooldown_to_manual_review_release`|`cooldown_mode`|`manual_review_mode`|`DESIGN_ONLY_NOT_EXECUTABLE`|

## Boundary

当前 cap multiplier、cooldown duration 和 release threshold 均未校准为可执行 policy。后续若进入 TRADING-2323 simulation 或 runtime implementation，必须重新执行 data-quality gate、owner review 和 broker / production safety review。
