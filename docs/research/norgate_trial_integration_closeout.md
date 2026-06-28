# Norgate Trial Integration Closeout

Final status: `NORGATE_TRIAL_INCONCLUSIVE`

## Summary

- norgate_package_access_status: `NORGATE_ENV_MISSING_PACKAGE`
- membership_query_status: `NORGATE_MEMBERSHIP_QUERY_NOT_VALIDATED`
- delisted_visibility_status: `DELISTED_VISIBILITY_NOT_CONFIRMED`
- price_coverage_status: `NORGATE_PRICE_COVERAGE_BLOCKED`
- daily_membership_snapshot_status: `NORGATE_DAILY_MEMBERSHIP_SNAPSHOT_BLOCKED`
- breadth_prototype_status: `NORGATE_BREADTH_PROTOTYPE_BLOCKED`
- pit_leakage_audit_status: `NORGATE_TRIAL_PIT_AUDIT_READY`
- raw_data_governance_status: `NORGATE_TRIAL_CACHE_GOVERNANCE_PASS`
- paid_platinum_decision: `NORGATE_TRIAL_INCONCLUSIVE`
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

## Validation

- `python -m ruff check src tests`: PASS.
- `python -m compileall -q src tests`: PASS.
- `python -m pytest -n 16 --dist loadfile tests/test_norgate_trial_integration.py`: 7 passed.
- `python -m pytest -n 16 --dist loadfile tests/test_paid_data_due_diligence.py`: 7 passed.
- `python -m pytest -n 16 --dist loadfile tests/test_first_layer_reopen_gate.py`: 6 passed.
- `python -m pytest -n 16 --dist loadfile tests/test_report_index.py tests/test_documentation_contract.py tests/test_task_register_consistency.py`: 27 passed.
- `python -m pytest -n 16 --dist loadfile tests/test_research_audit_metadata.py tests/test_research_artifact_governance.py`: 25 passed.
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`: 193 passed.
- Runtime artifact: `outputs/validation_runtime/contract-validation_20260628T095754Z/test_runtime_summary.json`.
- Runtime reader brief: `outputs/validation_runtime/contract-validation_20260628T095754Z/test_runtime_reader_brief.md`.
