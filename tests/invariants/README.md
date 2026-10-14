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
| G2/G3 vectorization and event accounting | Batch simulation preserves scalar results, random timing order and costs; losses stop the batch; descriptive events and episode lags follow hand-checked definitions; S4 settings are separate from the original fingerprint (TRADING-2565 S4c, invariants 23–27) | `test_research_vector_events_invariants.py` |

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

## S4c 不变量及反向对照（2026-10-14）

以下全部在 `test_research_vector_events_invariants.py`，验收文件由 Claude 提供，原样保留。

| 编号 | 守住的语义 / 测试 | 反向对照 |
|---|---|---|
| 23 | 批模拟逐项等于单条模拟：`test_simulate_many_equals_simulate_row_by_row`，含漂移、成本、调度、交易带和延迟；另查目标宽度 | 不扣成本、漂移不除组合总收益的变异；错误宽度及不可执行、越界目标必须拒绝 |
| 24 | 亏光停止并点名路径与日期：`test_a_path_that_loses_everything_names_the_path_and_the_date` | 合成崩盘中仅第 1 条路径持有归零的 lev3；其余路径不亏光；`test_simulate_many_names_the_path_of_an_unusable_target` 检查上下界和开仓 NaN |
| 25 | 随机择时结果不变：`test_the_placebo_equals_the_per_path_reference`；`test_the_placebo_is_unchanged_across_chunks` | 原 `placebo_targets` + 原 `simulate` 独立逐路径对照；7 路径块、30 次抽样覆盖末尾不满块，防乱序、重复或漏路径 |
| 26 | 手算事件与固定下跌段：`test_events_follow_their_definitions`；`test_event_edges_follow_their_definitions` | gap 等于合并阈值不能合并；收益等于分类阈值必须归类；短事件等长不算短；多事件连乘而非相加；段末当天回补延迟为 0；无减仓/无回补为 None；错位收益日期拒绝 |
| 27 | 设置生效、默认值与指纹隔离：`test_every_event_setting_is_read`；`test_runs_carry_the_event_table_outside_the_fingerprint` 等 | 逐项移动设置，事件表必须变化；配置边界和额外字段拒绝；不同事件设置不能改变指纹；原 bench 配置 sha256 锁定；独立模拟核对 run 自己的事件表，ledger 保存且 report 可读旧记录 |

## S4c 变异证据（2026-10-14）

只在本地短暂变异生产实现，验收测试没有改动；每项以 `-n 16 --dist loadfile` 运行，随后还原。

| 变异 | 失败的验收测试 | 结果 |
|---|---|---|
| `simulate_many` 净收益不扣成本（写为 gross） | `test_simulate_many_equals_simulate_row_by_row` 的 `[rebalance0-0-4.0]`、`[rebalance1-1-25.0]`、`[rebalance3-0-15.0]` | 3 failed，零成本反向对照仍通过 |
| 漂移时不除 `1 + gross` | `test_simulate_many_equals_simulate_row_by_row` 的四个参数用例 | 4 failed |
| 事件合并条件 `<` 改为 `<=` | `test_events_follow_their_definitions`、`test_event_edges_follow_their_definitions` | 2 failed |
| 回补只从段末下一天搜索（保持相对段末的 +1 位置换算） | `test_events_follow_their_definitions` | 1 failed，段末当天已经回补时错误报 1 而非 0；另一个边缘用例通过 |

变异及合成计时日志保存在运行目录 `TRADING-2565-05`，没有读取真实快照。合成计时（3,000 个交易日、1,000 条路径、月度调度）：逐条 `simulate` 19.082099 秒，`simulate_many` 0.153203 秒，约 124.55 倍；净收益最大绝对差为 0。这些是执行性能证据，不是投资结论。
