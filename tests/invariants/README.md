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
| G1/G2 signal intake | 发布滞后和缺失年龄、Ken French 首块和单位、本机内容地址、检查后才能读且拒绝漂移、汇总不泄露数据值、严格来源规格与原配置不变（TRADING-2565 S4a，不变量 28–33） | `test_research_sources_invariants.py` |
| G3 read-only segment tables | The slow-bear and long-bull tables of trial runs: holding the market itself loses nothing inside a segment, D is the compounded return difference inside the segment's months, the tables never touch a fingerprint, a metric or a gate rule, the gate's lists are the fixed rule's segments of each development window, and the plans and the probes' gate copies stay byte for byte (TRADING-2565-36, invariants 48–52) | `test_research_segment_invariants.py` |
| G1/G2 monthly long-history sources | A monthly value is used only once its month is known: month M is published on the first trading day on or after the first of month M + 1 + the release lag, used after the known lag and carried over at most one missing month, checked by hand, day by day and against later changes; specs stay strict; the bench inputs, the screen, the trial and both commands align by the rule; Shiller's sheet is read by its header cells and y.1 is October; snapshots carry the monthly sources and the check reads them by month; the seven approved sources, their lags and the US market base are fixed (TRADING-2565-37, invariants 53–58) | `test_research_monthly_sources_invariants.py` |
| G3 P1 portfolio ruler | A fixed mix holding a reserve other than cash is judged by the portfolio ruler: a cash reserve gives every number zero, the geometric difference is exactly the reserve's own excess plus the rebalancing return, rebalancing earns on an independent reserve and loses on a correlated one, the quiet-period excess removes a constant carry and nothing else, the declines are the fixed 10% rule's of each development window, the gate changes only for reserve configurations (the timing rules stay word for word, a window the ruler does not name records no_ruler), and the plans, the probes' gate copies and the verdicts of earlier records stay as they were (TRADING-2565-39, invariants 59–63) | `test_research_portfolio_invariants.py` |

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

## S4a 不变量及反向对照（2026-10-14）

以下全部在 `test_research_sources_invariants.py`，Claude 提供的验收文件按 SHA256SUMS 原样复制，未修改；只用合成数据。

| 编号 | 守住的语义 / 测试 | 反向对照 |
|---|---|---|
| 28 | `test_known_at_follows_the_publication_lag`、`test_known_at_drops_stale_values`、`test_known_at_never_uses_a_later_value` 及默认过期、缺失和非交易日边缘用例 | 改未来值不能改变当时已知值；不减 lag、不丢 NaN、过期 `>=` 的变异必须失败；正好 5 日保留 |
| 29 | `test_the_ken_french_parser_reads_the_first_block_in_decimals`、`test_the_csv_comes_out_of_the_zip` | 行业文件第二块 9.99 不能进入首块；-99.99/-999 转 NaN；非 zip、零个/多个 CSV 拒绝 |
| 30 | `test_snapshots_are_content_addressed`、`test_the_id_is_twelve_hex_digits_and_the_manifest_has_no_fetch_time` | 不同目录同数据同 id；改一个值改变 id；抓取时间进清单的变异失败 |
| 31 | `test_load_refuses_unchecked_failed_and_altered_snapshots`、`test_check_fails_unordered_empty_and_altered_sources_and_warns_on_gaps`、`test_loaded_frames_are_the_fetched_values_on_their_dates` | 无检查、重复、乱序、空来源、校验和漂移拒绝；WARN 可读；载入值和抓取值一致；正好缺 5 个工作日 PASS，6 个 WARN |
| 32 | `test_no_data_value_is_printed_or_written_to_check_md`、`test_no_value_leaks_from_first_last_or_duplicated_rows` | 合成标记放在首行、末行及重复行，stdout/stderr、check.json/check.md 均不能出现原值或两位小数值 |
| 33 | `test_source_specs_are_strict`、`test_source_specs_refuse_empty_text_and_negative_lags`、`test_duplicate_source_ids_are_refused`、批准来源和原配置哈希测试 | 非法 provider/dataset、额外字段、空文本、负 lag、FRED 零 lag、重复 id 必须拒绝；research_bench.yaml SHA-256 锁定 |

## S4a 变异证据（2026-10-14）

每项只在本地临时变异实现，用 `pytest -n 16 --dist loadfile tests/invariants/test_research_sources_invariants.py -q` 运行，随后还原；验收文件未改。

| 变异 | 失败的验收测试 | 结果 |
|---|---|---|
| `known_at` 参照位置不减 lag | `test_known_at_follows_the_publication_lag`、`test_known_at_drops_stale_values`、`test_known_at_never_uses_a_later_value[1]` 和 `[3]` | 4 failed，28 passed |
| `known_at` 不去掉 NaN | `test_known_at_follows_the_publication_lag`、`test_known_at_does_not_carry_missing_values_past_the_stale_limit` | 2 failed，30 passed |
| 过期判断 `>` 改成 `>=` | `test_known_at_drops_stale_values`、`test_known_at_drops_values_older_than_five_trading_days_by_default`、`test_known_at_does_not_carry_missing_values_past_the_stale_limit`、`test_known_at_places_an_off_calendar_date_on_the_calendar_day_before` | 4 failed，28 passed |
| 解析器非数据行 `break` 改成 `continue`，读到第二块 | `test_the_ken_french_parser_reads_the_first_block_in_decimals`、`test_load_refuses_unchecked_failed_and_altered_snapshots`、`test_check_fails_unordered_empty_and_altered_sources_and_warns_on_gaps`、`test_loaded_frames_are_the_fetched_values_on_their_dates`、`test_a_gap_is_more_than_five_missing_business_days` | 5 failed，27 passed |
| 清单放进抓取时间 | `test_snapshots_are_content_addressed`、`test_the_id_is_twelve_hex_digits_and_the_manifest_has_no_fetch_time` | 2 failed，30 passed |

日志保存在运行目录 `TRADING-2565-06` 的 `mutation-*.log`，不包含真实快照。这些是数据入口正确性证据，不是投资结论。

## S4b 不变量及反向对照（2026-10-14）

以下全部在 `test_research_screen_invariants.py`，Claude 提供的验收文件按 SHA256SUMS 原样复制（`c5612c3d90c6f3717a2f56582bfb887c017695c54696f4f265dbe1b06e27bb20`），没有修改；只用合成数据。

| 编号 | 守住的语义 / 测试 | 反向对照 |
|---|---|---|
| 34 | `test_the_screen_never_reads_past_the_screen_end`、`test_market_statistics_cut_at_the_screen_end`、`test_a_source_enters_at_its_known_time` | 每个市场和来源在 2010-12-31 后污染或截短，JSON 必须完全一致；lag=1 与手算移一天再 lag=0 一致，缺来源拒绝 |
| 35 | `test_a_signal_equal_to_the_coming_return_is_found`、risk oracle、`test_the_expanding_percentile_by_hand`、`test_the_group_table_by_hand`、`test_leave_out_and_partial_correlation_by_hand`、独立噪声和样本不足测试 | oracle IC=1、最小 p；独立噪声在 MDE 内；手算 ties、NaN、分组、前向回撤、逐年/十年剔除和 OLS 残差；行业不得做 bootstrap |
| 36 | `test_a_signal_that_looks_ahead_is_refused`、`test_baselines_do_not_look_ahead`、`test_the_baselines_follow_their_formulas` | 未来收益信号拒绝；未来污染不能改早期基准值，回看窗口用独立数组公式核对 |
| 37 | `test_a_copy_of_a_baseline_is_seen_as_one` | 动量基准复制相关为 1，partial_ic=None，裁决的复制阈值另测 |
| 38 | `test_the_ledger_shows_edits_deletions_and_reordering`、Windows 换行、重复组合拒绝、`test_benjamini_hochberg_runs_over_the_whole_family` | 中间记录改、删、换序要报坏链；CRLF 保持哈希；换规格是新成员，换数据仅增加记录；BH 用全家族固定数值核对 |
| 39 | `test_the_verdict_follows_the_approved_rules`（17 个用例）、QQQ-only、批准默认值 | 淘汰优先；广度 2/3 的舍入容差、p=q、区间上端=0、corr=0.9 边界；剔除不稳不能候选；QQQ-only 永不候选 |
| 40 | `test_the_minimum_detectable_effect_is_the_closed_form`、`test_results_hold_summaries_only`、`test_the_bench_settings_file_is_unchanged` | MDE 固定数值；JSON 无逐日列表，行业区间 None；原 research_bench.yaml SHA-256 锁定 |

## S4b 变异证据（2026-10-14）

每项只在本地临时变异实现，以 `pytest -n 16 --dist loadfile tests/invariants/test_research_screen_invariants.py -q -k ...` 运行对应验收用例，随后还原；验收文件未改。

| 变异 | 失败的验收测试 | 结果 |
|---|---|---|
| 前向收益从 t 起算（shift 从 −h 改成 −(h−1)） | `test_a_signal_equal_to_the_coming_return_is_found`、`test_the_group_table_by_hand` | 2 failed，4.41s |
| 收益不先截到 screen_end | `test_the_screen_never_reads_past_the_screen_end`、`test_market_statistics_cut_at_the_screen_end` | 2 failed，7.40s |
| 跳过 check_no_lookahead | `test_a_signal_that_looks_ahead_is_refused` | 1 failed，6.60s |
| BH 不取从大到小累积最小 | `test_benjamini_hochberg_runs_over_the_whole_family` | 1 failed，4.43s |
| candidate 不要求 leave_out_stable=True | `test_the_verdict_follows_the_approved_rules[change5-inconclusive]` | 1 failed、16 passed，4.35s |
| 不校验 prev_sha256 | `test_the_ledger_shows_edits_deletions_and_reordering` | 1 failed，4.35s |
| 在所有市场做 bootstrap | `test_results_hold_summaries_only` | 1 failed，7.23s |

日志在运行目录 `TRADING-2565-07/mutation-*.log`。51 市场（US 和 49 行业 1926-07..2010、QQQ 1986..2010）、63 日动量、21/63/126 日持有期、默认 2,000 次自助法的 `screen_signal` 合成计时为 **15.655203 秒**（小于 180 秒）；不读真实快照、不打印逐日值，临时合成模块目录在计时后自动删除。这些是工程证据，不是候选或投资结论。

## S4d 不变量及反向对照（2026-10-14）

以下全部在 `test_research_signal_strategy_invariants.py`，Claude 提供的验收文件按 SHA256SUMS 原样复制（`ceaaec51f671350e6e1903416f4e4064218ed0668da1c48ad1300c0951734d77`），没有修改；只用合成数据。

| 编号 | 守住的语义 / 测试 | 反向对照 |
|---|---|---|
| 41 | `test_the_expanding_percentile_is_the_screen_one` | 手算 ties、NaN、扩张热身；改变未来值不影响早期分位 |
| 42 | `test_percentile_scores_and_mappings`、`test_orientation_follows_expected_sign_and_target`、`test_sign_scores_need_no_warm_up_and_average_across_signals`、边界及参数规格测试 | 三种映射、默认参数、step 等边取上一级、零点 0.5、任一信号 NaN 传播、首个有效值前 NaN、之后保持；规格里的 scale=-2 独立验证不允许预注册覆盖；非法参数拒绝 |
| 43 | `test_the_preregistration_pins_each_signal` | 改模块、改规格、缺文件必须拒绝；CRLF 当 LF 时目标不变 |
| 44 | `test_the_bench_check_catches_a_signal_that_looks_ahead`、`test_every_numeric_mapping_parameter_is_read` | 诚实动量通过，未来收益信号被原 bench 前视检查抓到；每个数值映射参数扰动能改变目标，signals/mapping 记 unchecked |
| 45 | `test_sources_enter_the_inputs_as_known_at_each_close` | 与独立 known_at(lag=1) 逐值对照，原输入和收益不变；缺来源拒绝，来源前视信号被抓到 |
| 46 | `test_the_signal_snapshot_is_fixed`、`test_screens_are_cited_by_number`、`test_qqq_q1_reads_as_before`、原配置哈希测试 | LATEST、错误 sig 格式、短筛选号、家族大小 0 拒绝；QQQ-Q1 读旧预注册取新增字段默认值，未指定信号快照的来源元数据不变 |
| 47 | `test_the_generic_strategy_equals_the_rule_written_by_hand` | 与独立手写 210 日均线规则逐日目标、完整运行 strategy/comparison 和参数检查一致 |

## S4d 变异证据（2026-10-14）

每项只在本地临时变异实现，以 `pytest -n 16 --dist loadfile tests/invariants/test_research_signal_strategy_invariants.py -q -k ...` 运行对应验收用例，随后还原；验收文件未改。

| 变异 | 失败的验收测试 | 结果 |
|---|---|---|
| 去掉目标的 ffill（不保持上一仓位） | `test_percentile_scores_and_mappings` 全部五种映射用例 | 5 failed，4.43s |
| favourable 只看 expected_sign，不看 target | `test_orientation_follows_expected_sign_and_target[spec1-expected1]` 和 `[spec2-expected2]` | 2 failed，1 passed，4.93s |
| 比较模块哈希时使用原始字节，不把 CRLF 当 LF | `test_the_preregistration_pins_each_signal` | 1 failed，4.39s |
| `with_signal_sources` 把来源 lag 当 0 | `test_sources_enter_the_inputs_as_known_at_each_close` | 1 failed，4.45s |
| sign 在零点给 1 而不是 0.5 | `test_sign_scores_need_no_warm_up_and_average_across_signals` | 1 failed，4.38s |

日志在运行目录 `TRADING-2565-08/mutation-*.log`。这是工程正确性证据，不是候选、paper-shadow 或生产批准；production_effect=none；broker_action=none。

## TRADING-2565-37 变异证据（2026-10-21）

不变量 53–58 在 `test_research_monthly_sources_invariants.py`；另有两条命令的单测（`tests/test_research_signal_strategy.py`、`tests/test_research_screen.py` 里标着 TRADING-2565-37 的）。每项只在参考实现的副本上临时变异，用 `pytest -n 4 --dist loadfile` 跑上面这个文件、`test_research_sources_invariants.py` 和这两个测试文件（144 项），随后还原；验收文件未改。29 项全部被抓到。

| 变异 | 失败的验收测试（举例） | 结果 |
|---|---|---|
| 不加发布滞后月数；不加已知滞后天数；发布日取 1 日之后的交易日；月份 M 从它自己的 1 日起用 | `test_a_month_is_used_from_the_day_it_is_known`、逐日对照 `[seed]`、问题数据命令那条 | 5–9 failed |
| 不过期；连缺两个月仍带着 | `test_a_second_missing_month_empties_the_source`、逐日对照 `[2]`、`[5]` | 3 failed |
| 不去掉 NaN；同一个月两条也收 | 逐日对照；`test_the_monthly_rule_refuses_bad_inputs` | 7 failed；1 failed |
| `align_source` 把月度当日度；评估台来源列、筛选、试跑、筛选命令、问题数据命令各自只传天数 | `test_align_source_chooses_the_rule_by_frequency`、`test_bench_inputs_take_monthly_sources_by_their_rule`、`test_the_screen_reads_monthly_sources_by_their_rule`、`test_the_trial_reads_monthly_sources_by_their_rule`、`test_cli_screen_aligns_each_source_by_its_spec`、`test_cli_data_aligns_a_monthly_source_from_the_day_its_month_is_known` | 每项 1–5 failed |
| Shiller 允许日度；Ken French 允许月度；日度来源带发布滞后 | `test_monthly_specs_are_strict` 对应的一组 | 各 1 failed |
| 数字 y.1 读成 1 月；字符串 "y.1" 读成 1 月；空行之后继续读；重复表头取第一个；表头行不认 "Date"；重复月份也收 | `test_the_shiller_sheet_is_read_by_its_headers`、`test_the_shiller_rows_end_at_the_first_row_without_a_month`、`test_a_shiller_sheet_without_its_headers_or_rows_is_refused` | 各 1–2 failed |
| 每个 Shiller 来源各下载一次；`fetch.json` 写成 Ken French 的地址 | `test_a_snapshot_carries_the_monthly_sources` | 各 1 failed |
| 检查对月度来源用工作日缺口；不在月初也不判 FAIL；缺一个月不算缺口 | `test_the_check_reads_monthly_dates_by_month` 等 | 1–4 failed |
| 配置里股息的发布滞后写成 3 个月 | `test_the_approved_monthly_sources_are_in_the_settings_file` | 1 failed |

第一轮有 3 项没被抓到（问题数据命令只传天数、字符串 "y.1"、空行之后继续读），补了问题数据命令里一月份观测那条和 `test_the_shiller_rows_end_at_the_first_row_without_a_month` 以后都抓到。变异脚本和逐项结果在项目文件 `knowledge/claude/qqq-options/auto-loop/build/ref-TRADING-2565-37/`。这些是数据入口正确性证据，不是投资结论。

## TRADING-2565-39 变异证据（2026-10-21）

不变量 59–63 在 `test_research_portfolio_invariants.py`；验收单测在 `tests/test_research_portfolio.py` 和 `tests/test_research_reserve.py`、`test_research_trial.py` 里标着组合尺子的几条。每项只在参考实现的副本上临时变异，先跑 `test_research_portfolio.py` 和这个不变量文件，没抓到再跑 `test_research_reserve.py`、`test_research_trial.py`、`test_research_gate_false_pass.py`、`test_research_falsify.py`（`pytest -x -n 4`），随后还原；验收文件未改。53 项里 52 项被抓到，剩下 1 项是等价变异。

| 变异 | 失败的验收测试（举例） | 结果 |
|---|---|---|
| 计入线改成 −3%；平时超额退回线改成 60%；从不退回或总是退回；平时日子漏掉段的首月或末月；平时超额按自然日年化或用简单收益 | `test_a_decline_counts_when_the_cash_mix_loses_more_than_two_percent`、`test_the_fallback_line_is_half_the_return_days`、和探针逐项相等那条、`test_a_constant_carry_is_removed_and_cushions_nothing` | 各 1–2 failed |
| 腿按 365 天年化；市场腿用现金；整窗超额用差不用比；几何差反号；储备自身超额乘 w；再平衡的 B 用储备 | 和探针逐项相等那条、`test_the_geometric_difference_is_own_excess_plus_rebalancing` | 各 1–4 failed |
| 缓冲多扣一个月或扣整份储备；r 除以策略的损失；段内现金组合的损失取策略的；r = 0 算有缓冲；中位换均值、最小换最大；不计入名单列成计入的 | 和探针逐项相等那条、`test_a_decline_with_no_cushion_is_not_cushioned`、`test_a_decline_where_the_cash_mix_loses_little_is_listed_not_counted` | 各 1–2 failed |
| 尺子不查空的下跌段清单（`portfolio_section` 自己也要拒绝）；对照用基准的现金 | `test_the_ruler_needs_a_reserve_and_the_declines`、`test_the_fallback_line_is_half_the_return_days` | 各 1–2 failed |
| 开段线 12%；正好跌到线上不开段；下跌段用 20% 的线；核对不分种类；核对漏报清单外的段；慢熊要 7 个月 | `test_the_declines_are_the_probe_declines`、`test_a_fall_of_exactly_the_line_opens_a_decline`、`test_the_decline_check_compares_declines_only`、`test_recomputing_the_declines_agrees_and_names_any_difference`、`test_the_split_keeps_the_six_month_line_of_the_drops` | 各 1 failed |
| 现金的固定比例也用尺子；尺子判每个窗口；没有尺子算没过、算过、排在没过之前；权重固定 0.6；择时规则不只读；门的清单检查漏三种；相接的两段不算重叠；重复的窗口名放过；核对读慢熊清单；没有清单照跑 | `test_only_a_fixed_mix_with_a_reserve_uses_the_ruler`、`test_no_ruler_is_neither_a_pass_nor_a_failure`、`test_each_fixed_mix_is_ruled_at_its_own_weight`、`test_malformed_portfolio_fields_are_refused`、`test_a_reserve_mix_on_a_window_without_declines_is_refused` 等 | 各 1 failed |
| 报告对调 A、B；老记录不注；账本漏 `gate_outcome`；现金储备也算尺子；正式题能引用 `portfolio.*`；缺节时报错；`trial-run` 不印没有尺子；`trial-segments` 不认 `--declines` | 报告、账本和命令的那几条，`test_a_question_cannot_rule_on_the_portfolio_yet`、`test_a_missing_section_fails_a_rule_without_an_error` | 各 1–2 failed |

第一轮跑了 51 项（第 53 项超时没跑，第 30 项的锚点对上两处没跑成），13 项没被抓到：一项是下面的等价变异，一项（第 23 项）乘的是 0、等于没变，改成「段内现金组合的损失取策略的」。补了 10 条测试后重跑，其余 11 项和改过的第 23 项、第 30 项、第 53 项都抓到。补的测试：2% 计入线两侧、平时超额退回线两侧（并和探针的平时超额逐数相等）、r = 0 不算有缓冲、`portfolio_section` 自己拒绝空清单、正好跌到 10% 开段、清单核对只比下跌段、拆出循环后慢熊仍是 6 个月、相接的两段算重叠、各配置按自己的权重、门里没有清单的窗口拒绝运行。剩下的等价变异：「−2% 正好计入」（段内收益算不出正好 −0.02：1 − 0.02 − 1 是比 −0.02 略小的浮点数）。变异脚本和逐项结果在项目文件 `knowledge/claude/qqq-options/auto-loop/build/ref-TRADING-2565-39/`。这些是评估台正确性证据，不是投资结论。
