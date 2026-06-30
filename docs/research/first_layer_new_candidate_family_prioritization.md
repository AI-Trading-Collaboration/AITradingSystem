# First-Layer New Candidate Family Prioritization

TRADING-2301 把 owner 附件的新 candidate family 排序固化为 research-only backlog。

- status: `FIRST_LAYER_NEW_CANDIDATE_FAMILY_BACKLOG_READY_PROMOTION_BLOCKED`
- selected_market_regime: `ai_after_chatgpt`
- actual_requested_date_range: `owner_static_research_prioritization`
- next_new_family_task: `TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC`
- next_mainline_task: `TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN`

## Ranking

|rank|family|score|priority|next task|
|---:|---|---:|---|---|
|0|`volatility_risk_cap_forward_observe`|87|P0|`TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN`|
|1|`breadth_participation`|83|P1|`TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC`|
|2|`ai_semiconductor_leadership`|80|P1|`TRADING-2303_AI_SEMICONDUCTOR_LEADERSHIP_CANDIDATE_FAMILY_SPEC`|
|3|`liquidity_rates_pressure`|77|P1/P2|`TRADING-2304_LIQUIDITY_RATES_PRESSURE_PROXY_AUDIT_AND_CANDIDATE_SPEC`|
|4|`regime_state_machine`|72|P2|`TRADING-2305_REGIME_STATE_MACHINE_DIAGNOSTIC_LABEL_FRAMEWORK`|
|5|`event_calendar_gating`|65|P2|`TRADING-2306_EVENT_CALENDAR_GATING_FEASIBILITY_AUDIT`|
|6|`execution_cooldown_decay_cap_mechanics`|69|P0.5/P2|`TRADING-2307_FORWARD_OBSERVE_RUNTIME_EVIDENCE_AND_CAP_MECHANICS_PLAN`|

## Feasibility Focus

- `volatility_risk_cap_forward_observe`: pit_policy=`validated_prior_artifacts`, source_schema_status=`source_artifacts_validated_with_warnings`
- `breadth_participation`: pit_policy=`pit_required_or_pit_approximation`, source_schema_status=`proxy_source_until_true_breadth_approved`
- `ai_semiconductor_leadership`: pit_policy=`price_relative_strength_first_then_event_pit_audit`, source_schema_status=`price_proxy_ready_event_sources_pending`
- `liquidity_rates_pressure`: pit_policy=`price_proxy_available_macro_timestamp_audit_required`, source_schema_status=`proxy_source_with_pit_audit_required`
- `regime_state_machine`: pit_policy=`diagnostic_only_pit_transition_rules_required`, source_schema_status=`label_framework_not_model_ready`
- `event_calendar_gating`: pit_policy=`event_timestamp_pit_audit_required`, source_schema_status=`event_calendar_source_audit_required`
- `execution_cooldown_decay_cap_mechanics`: pit_policy=`observe_only_runtime_evidence_required`, source_schema_status=`runtime_contract_not_started`

当前输出不生成 candidate-bound artifacts，不执行 actual-path validation。
