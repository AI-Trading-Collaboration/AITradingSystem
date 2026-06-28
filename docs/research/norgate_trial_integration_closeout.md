# Norgate Trial Integration Closeout

Final status: `NORGATE_PAID_PLATINUM_RECOMMENDED`

## Summary

- norgate_package_access_status: `NORGATE_ENV_READY`
- membership_query_status: `NORGATE_HISTORICAL_MEMBERSHIP_QUERY_VALIDATED`
- delisted_visibility_status: `DELISTED_VISIBILITY_CONFIRMED`
- price_coverage_status: `NORGATE_PRICE_COVERAGE_READY_2Y_LIMITED`
- daily_membership_snapshot_status: `NORGATE_DAILY_MEMBERSHIP_SNAPSHOT_PROTOTYPE_READY`
- breadth_prototype_status: `NORGATE_BREADTH_PROTOTYPE_2Y_ONLY`
- pit_leakage_audit_status: `NORGATE_TRIAL_PIT_AUDIT_READY`
- raw_data_governance_status: `NORGATE_TRIAL_CACHE_GOVERNANCE_PASS`
- paid_platinum_decision: `NORGATE_PAID_PLATINUM_RECOMMENDED`
- trial_price_history_limited_to_2y: `True`
- primary_window_full_validation_requires_paid_platinum: `True`
- model_ready_for_2021_primary_window: `False`
- research_only: `True`
- candidate_count: `0`
- first_layer_reopen_allowed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`
- production_effect: `none`
- dynamic_promotion_status: `BLOCKED`

## Safety

- Raw vendor data is not committed.
- Trial 2Y price limit blocks primary-window model-ready validation.
- Promotion, paper-shadow, production and broker remain disabled.

## Blocking Facts

- trial_price_history_limited_to_2y: `true`
- earliest_price_date: `2024-06-28`
- primary_window_starts_at: `2021-02-22`
- model_ready_for_2021_primary_window: `false`
- promotion_allowed: `false`
- paper_shadow_allowed: `false`
- production_allowed: `false`
- broker_action: `none`
- purchase_allowed_without_owner_approval: `false`
