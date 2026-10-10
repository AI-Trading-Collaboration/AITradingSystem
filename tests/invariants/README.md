# Invariant suite (GOV-008 zone C)

These tests guard the research guarantees G1–G4 (see `docs/requirements/GOV-008_Research_First_Governance_Refactor.md`).
They run in the normal PR suite. They live in zone C: changing or deleting a file here needs an
`Owner-Decision:` trailer (`config/gov008_ship.yaml`), so a change cannot quietly weaken a guard.

Each new test below has a **negative control**: either a deliberately defective input that must be
caught, or a recorded mutation of the guarded code that makes the test fail.

| Guarantee | What is guarded | Where |
|---|---|---|
| G1 data trusted | A failed data-quality check stops `build-features` and writes no features, and the failure stays auditable | `test_dq_gate_blocks_downstream.py` |
| G2 no look-ahead | Features at an as-of date do not change when later data changes | `test_features_do_not_look_ahead.py` |
| G2 reproducible | The run manifest records commit, `code_modified` and config content hashes | `test_run_manifest_records_provenance.py` |

Existing tests that already guard the same guarantees and stay in the suite (not duplicated here):

| Guarantee | Existing guard |
|---|---|
| G1 | `tests/test_data_quality.py` (schema, duplicates, freshness, suspicious values) |
| G2 PIT | `tests/test_feature_availability.py::test_feature_availability_source_check_blocks_future_available_time` |
| G2 window | `tests/test_unified_primary_research_window.py`, `tests/test_research_window_contracts.py` |
| G2 window default | `tests/test_active_window_script_guard.py::test_active_python_defaults_do_not_reintroduce_legacy_comparison_start` (2022-12-01 must not return as an active default) |
| G3 | `tests/test_trading2457_uncontaminated_selection_protocol.py`, `tests/test_research_governance_end_to_end_pack.py` |
| G4 thresholds | `tests/test_heuristic_governance.py::test_default_heuristic_governance_audit_passes` |
| G4 safety | `tests/test_production_boundary_static_scan.py`, `tests/test_scheduled_tasks.py::test_scheduled_tasks_config_registers_required_cadences_and_safety` |
| G4 broker boundary | `tests/trading_engine/test_safety_boundaries.py::test_non_trading_engine_modules_do_not_import_broker_adapters` |

## Recorded mutation evidence (2026-10-10)

| Mutation of the guarded code | Result |
|---|---|
| `DataQualityReport.passed` always true (gate removed) | `missing_required_ticker` and `non_positive_close` cases fail. `duplicate_price_key` still passes: another independent check also rejects duplicate keys, so that case is not evidence for the gate itself. |
| `_prepare_prices` ignores the as-of date (look-ahead introduced) | `test_market_features_do_not_depend_on_data_after_as_of` fails; the leak-detection control still passes. |
| `provenance_block` returns nothing (provenance dropped from the manifest) | all three manifest tests fail (`KeyError: 'config_sha256'` / `'git'`). |
