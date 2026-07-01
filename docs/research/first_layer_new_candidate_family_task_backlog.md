# First-Layer New Candidate Family Task Backlog

|task_id|legacy_alias|priority|next_step|blocked_until|
|---|---|---|---|---|
|`TRADING-2294_EVIDENCE_ACCUMULATION_EXTENSION_PLAN`|`TRADING-2294 mainline`|P0|risk-cap forward observe runtime evidence and cap mechanics plan|TRADING-2301_validated_and_owner_sequence_reviewed|
|`TRADING-2302_BREADTH_PARTICIPATION_DATA_FEASIBILITY_AND_CANDIDATE_SPEC`|`TRADING-2294B`|P1|data feasibility + candidate spec|TRADING-2301_validated|
|`TRADING-2307_AI_SEMICONDUCTOR_LEADERSHIP_CANDIDATE_FAMILY_FEASIBILITY_AUDIT`|`TRADING-2295B`|P1|feasibility audit + candidate family design sketch|TRADING-2301_validated_and_breadth_path_started|
|`TRADING-2311_LIQUIDITY_RATES_PRESSURE_DATA_FEASIBILITY_AUDIT`|`TRADING-2296B`|P1/P2|proxy audit + PIT design|TRADING-2301_validated_and_p1_family_sequence_confirmed|
|`TRADING-2315_REGIME_STATE_MACHINE_DESIGN_AUDIT`|`TRADING-2297B`|P2|regime label framework|TRADING-2301_validated_and_owner_sequence_reviewed|
|`TRADING-2318_EVENT_CALENDAR_DATA_FEASIBILITY_AUDIT`|`TRADING-2298B`|P2|PIT event calendar audit|TRADING-2301_validated_and_owner_sequence_reviewed|
|`TRADING-2321_RISK_CAP_COOLDOWN_DECAY_DESIGN`|`TRADING-2294 runtime / execution mechanics`|P0.5/P2|pair with risk-cap runtime|TRADING-2293_owner_review_or_TRADING-2301_route_confirmed|

## Standard Validation Path

`candidate_family_spec` -> `pit_data_feasibility_audit` -> `candidate_bound_generator` -> `actual_path_validation` -> `inconclusive_diagnostics` -> `scope_narrowing` -> `forward_observe_readiness`

禁止跳过 candidate-bound artifact、PIT timestamp、source hash、provenance、actual-path validation 和 safety gates。
