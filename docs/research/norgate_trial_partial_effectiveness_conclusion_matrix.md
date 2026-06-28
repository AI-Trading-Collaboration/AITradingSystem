# Norgate Trial Partial Effectiveness Conclusion Matrix

- status: `NORGATE_2Y_PARTIAL_EFFECTIVENESS_READY`
- source_engineering_useful: `True`
- source_feature_useful_2y: `weak`
- purchase_platinum_evidence_strength: `moderate`
- model_ready_for_2021_primary_window: `False`
- reopen_gate_allowed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

2Y trial 不是无效证据；它可以支持购买决策，但不能替代 2021 primary-window validation。

## Validation

- `python -m ruff check src tests`: PASS.
- `python -m compileall -q src tests`: PASS.
- `python -m pytest -n 16 --dist loadfile tests/test_norgate_partial_effectiveness.py tests/test_norgate_trial_integration.py tests/test_report_index.py tests/test_documentation_contract.py tests/test_task_register_consistency.py tests/test_research_audit_metadata.py tests/test_research_artifact_governance.py`: 63 passed.
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`: 193 passed.
- Runtime artifact: `outputs/validation_runtime/contract-validation_20260628T103915Z/test_runtime_summary.json`.
