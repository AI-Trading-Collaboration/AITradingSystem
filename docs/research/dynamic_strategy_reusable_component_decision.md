# 动态策略 reusable component decision

- status：`DYNAMIC_STRATEGY_COMPONENT_ATTRIBUTION_TARGETED_ABLATION_RETEST_READY`
- best reusable component：`growth_tilt_engine`
- observation approved：`False`
- paper-shadow enabled：`False`

```json
{
  "best_reusable_component": "growth_tilt_engine",
  "best_reusable_component_decision": "REUSABLE_COMPONENT",
  "broker_action_enabled": false,
  "component_decisions": {
    "combined_turnover_budgeting_and_valid_until": "CONTINUE_COMPONENT_RESEARCH",
    "growth_tilt_engine": "REUSABLE_COMPONENT",
    "guarded_turnover_transfer": "OWNER_REVIEW_REQUIRED",
    "lower_turnover_guardrail": "USE_ONLY_AS_GUARDRAIL",
    "turnover_budgeting": "CONTINUE_COMPONENT_RESEARCH",
    "valid_until_strictness": "CONTINUE_COMPONENT_RESEARCH"
  },
  "guardrail_only_components": [
    "lower_turnover_guardrail"
  ],
  "owner_review_required_components": [
    "guarded_turnover_transfer"
  ],
  "paper_shadow_enabled": false,
  "production_enabled": false,
  "recombination_candidate_direction": "RECOMBINE_GROWTH_TILT_WITH_TURNOVER_AND_VALID_UNTIL_GUARDRAILS",
  "recommended_next_research_task": "TRADING-2394_Dynamic_Strategy_Component_Ablation_Owner_Review_And_Recombination_Decision",
  "research_only_observation_approved": false,
  "reusable_component_decision_ready": true,
  "reusable_components": [
    "growth_tilt_engine"
  ],
  "schema_version": "dynamic_strategy_reusable_component_decision.v1"
}
```
