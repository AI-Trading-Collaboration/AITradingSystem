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
| G2/G3 research bench | Every research result goes through one bench: benchmarks are the series they name, targets never use later data, every declared parameter is read, no metric is a constant, cost and timing are right, runs are reproducible, and the bench agrees with the old simulator (TRADING-2565 S3a) | `test_research_bench_invariants.py` |
| G3 trial ledger | Every research run is recorded, failed ones too, in a hash chain; trials are counted and capped; the validation window is read once per question and only for a preregistration approved on main; records hold summaries only; the deflated Sharpe ratio matches the paper's worked example; this repository's records were never changed or removed and decisions cite only recorded runs (TRADING-2565 S3b) | `test_research_ledger_invariants.py` |
| G3 standard falsification | A result counts only after it survives the standard falsification, and each check can fail: the placebo is a same-exposure random timing that a strategy knowing the next return beats every time; the bootstrap interval is zero, positive or around zero as it should be and widens with noise and with lumpy edges; leave-one-out exposes an edge earned in one year or one episode; cost and delay reruns use the bench; validation and consistency runs are always falsified and their rules read it; a rule naming a metric no run has refuses the read (TRADING-2565 S3c) | `test_research_falsify_invariants.py`; invariant 22 in `test_research_ledger_invariants.py` |

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

## Recorded mutation evidence (2026-10-13, research bench)

Each mutation was applied to `src/ai_trading_system/research_bench/` and the bench tests were run
(`tests/invariants/test_research_bench_invariants.py`, `tests/test_research_bench.py`).

| Mutation of the bench | Tests that failed |
|---|---|
| Trade at the decision close (no one-day lag) | the timing test for delays 0, 1 and 2; the old-simulator agreement; three unit tests |
| Look-ahead check skipped | `test_lookahead_check_passes_an_honest_strategy_and_catches_a_leak` (and the parameter test, whose problems are dropped with it) |
| Parameter check skipped | `test_parameter_check_catches_a_declared_parameter_that_is_never_read` |
| `longest_drawdown_days` written as a constant | `test_every_metric_moves_when_the_returns_move`; one unit test |
| Matched benchmark ignores the strategy's average exposure | `test_constant_strategy_equals_its_matched_benchmark` |
| Cost dropped | `test_cost_is_zero_without_turnover_and_grows_with_bps`; the metric test; one unit test |
| Holdings do not drift between trades (the old simulator's constant weights) | the fixed-number simulation unit test |

## Recorded mutation evidence (2026-10-13, trial ledger)

Each mutation was applied to `src/ai_trading_system/research_bench/` and the ledger and bench tests
were run (`tests/invariants/test_research_ledger_invariants.py`, `tests/test_research_ledger.py`,
`tests/test_research_bench.py`).

| Mutation | Tests that failed |
|---|---|
| A run that fails its checks is not recorded | `test_every_run_is_recorded_including_failed_ones` |
| The chain ignores `prev_sha256` | `test_a_changed_or_removed_record_breaks_the_chain` |
| The trial cap is not enforced | `test_trials_are_counted_and_capped` |
| Strategy code is not part of the trial | `test_trials_are_counted_and_capped` |
| Approval is not checked before a validation read | `test_validation_needs_an_approved_prereg` |
| The validation window can be read twice | `test_the_validation_window_is_read_once` |
| A permit is not checked against the read register | `test_the_validation_window_is_read_once` |
| One read can run a strategy twice | `test_the_validation_window_is_read_once` |
| Validation parameters can be overridden | `test_the_validation_window_is_read_once` |
| The git-history check ignores rewritten reads | `test_history_and_admission_checks_catch_rewrites` |
| A decision may cite an unrecorded run | `test_history_and_admission_checks_catch_rewrites` |
| The deflated Sharpe ratio drops its second quantile term | `test_deflated_sharpe_matches_the_worked_example` |
| The bench opens the validation window without a permit | `test_the_validation_window_is_read_once`; one unit test |

## Recorded mutation evidence (2026-10-13, standard falsification)

Each mutation was applied to `src/ai_trading_system/research_bench/` and the falsification, ledger
and bench tests were run (`tests/invariants/test_research_falsify_invariants.py`,
`tests/invariants/test_research_ledger_invariants.py`, `tests/test_research_falsify.py`,
`tests/test_research_ledger.py`, `tests/test_research_bench.py`).

| Mutation | Tests that failed |
|---|---|
| Validation runs are not falsified | `test_validation_and_consistency_runs_always_face_the_falsification` |
| Rules ignore the falsification | `test_validation_and_consistency_runs_always_face_the_falsification` |
| Unknown rule metrics are not checked before a read | `test_validation_and_consistency_runs_always_face_the_falsification` |
| The placebo keeps the strategy's timing | `test_placebos_reorder_the_acted_targets_and_only_the_timing`; `test_a_strategy_that_knows_the_next_return_beats_every_placebo`; one unit test |
| The placebo changes the exposure | `test_placebos_reorder_the_acted_targets_and_only_the_timing`; `test_a_strategy_that_knows_the_next_return_beats_every_placebo`; one unit test |
| The bootstrap does not resample | `test_the_bootstrap_interval_is_zero_positive_or_around_zero_as_it_should_be`; one unit test |
| The bootstrap resamples single days, not blocks | `test_the_bootstrap_interval_is_zero_positive_or_around_zero_as_it_should_be`; one unit test |
| Leave-one-out removes nothing | `test_leave_one_out_exposes_an_edge_from_one_year_or_one_episode` |
| The worst leave-one-out value is the best | `test_leave_one_out_exposes_an_edge_from_one_year_or_one_episode` |
| Cost reruns ignore the cost | `test_reruns_use_the_bench_cost_and_delay` |
| Delay reruns ignore the delay | `test_reruns_use_the_bench_cost_and_delay` |
| The placebo rank drops ties | `test_a_strategy_that_knows_the_next_return_beats_every_placebo`; one unit test |
| The relative left-tail change has the wrong sign | `test_paired_statistics_agree_with_the_bench_metrics` (unit) |
