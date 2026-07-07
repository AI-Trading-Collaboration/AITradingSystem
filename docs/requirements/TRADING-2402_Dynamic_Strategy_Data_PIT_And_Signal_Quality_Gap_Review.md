# TRADING-2402: Dynamic Strategy Data PIT And Signal Quality Gap Review

最后更新：2026-07-07

## Context

TRADING-2401 已确认当前 recombination line 进入 plateau，并记录 owner
decision=`PAUSE_RECOMBINATION_LINE_AND_REVIEW_DATA_PIT_SIGNAL_QUALITY`。2401
不批准 observation / paper-shadow / production，并把下一步限定为
`TRADING-2402_Dynamic_Strategy_Data_PIT_And_Signal_Quality_Gap_Review`。

从 TRADING-2364 到 TRADING-2401，dynamic strategy 已完成 cadence bias audit、
candidate expansion、gate calibration、component attribution、recombination
candidate retest、targeted gate evidence retest 和 plateau decision。当前结论是：
候选存在收益或组件价值，但没有候选达到 observation preview。2402 需要判断瓶颈是否
来自 data quality、PIT coverage、signal construction、valid-until / stale-signal、
regime labeling 或 threshold calibration meta-dataset，而不是继续生成局部候选。

## Scope

This task will:

1. Read TRADING-2401 plateau decision artifacts.
2. Read TRADING-2400 owner-review artifacts.
3. Read TRADING-2399 targeted gate evidence retest artifacts.
4. Read TRADING-2386 expanded candidate pool retest artifacts.
5. Read TRADING-2364 cadence bias audit artifacts when available.
6. Run or consume `aits validate-data --as-of 2026-07-05`.
7. Summarize data quality warnings and classify their dynamic strategy relevance.
8. Summarize PIT coverage gaps for features, signals, advisory valid-from /
   valid-until fields, and outcome binding.
9. Summarize signal quality, valid-until, stale-signal and signal-lag gaps.
10. Summarize regime labeling gaps and whether regime expectation scoring is needed.
11. Summarize threshold meta-dataset gaps.
12. Produce a prioritized gap matrix and TRADING-2403 route.

## Non-Goals

- Do not run a new strategy backtest.
- Do not generate a new trading signal or scoring output.
- Do not approve research-only observation for any candidate.
- Do not enable scheduler, event append, outcome binding, paper-shadow,
  paper trade, shadow position, production, broker/order, scheduled task or
  daily report paths.
- Do not resume strategy candidate search from this task.

## Data Quality Gate

Because 2402 explicitly reviews data / PIT / signal quality, the default closeout
will run:

```text
aits validate-data --as-of 2026-07-05
```

If implementation later changes to only consume prior validation summaries, the
completed notes must explicitly state why the fresh gate was not rerun. The
expected default is to run the gate and expose status, error count, warning count,
warning details and relevance classification in the outputs.

## Expected Decision

Expected default next-direction decision:

- `OPTION_B_BUILD_PIT_COVERAGE_MATRIX`
- `OPTION_C_REVIEW_SIGNAL_CONSTRUCTION_FRAMEWORK`
- `OPTION_E_BUILD_THRESHOLD_META_DATASET`

Expected route:

```text
TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review
```

Expected safety result:

- `resume_candidate_search_recommended=false`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/gap_review_result.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/data_quality_gap_matrix.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/pit_coverage_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/signal_quality_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/regime_labeling_gap_review.json`
- `outputs/research_strategies/dynamic_strategy_data_pit_signal_quality_gap_review/threshold_meta_dataset_gap_review.json`
- `docs/research/dynamic_strategy_data_pit_signal_quality_gap_review.md`
- `docs/research/dynamic_strategy_data_quality_gap_matrix.md`
- `docs/research/dynamic_strategy_pit_coverage_gap_review.md`
- `docs/research/dynamic_strategy_signal_quality_gap_review.md`
- `docs/research/dynamic_strategy_2403_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-data-pit-signal-quality-gap-review`
  returns `DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`.
- Output reads and validates TRADING-2401 route to TRADING-2402.
- Output records latest validate-data status, error count and warning count.
- Output classifies validate-data warnings by dynamic strategy relevance.
- Output sets data quality, PIT coverage, signal quality, regime labeling,
  threshold meta-dataset and prioritized gap review readiness fields to true.
- Output sets recombination line paused=true.
- Output sets resume candidate search recommended=false.
- Output recommends PIT coverage matrix, signal construction review, regime
  expectation scoring review and threshold meta-dataset.
- Output routes to
  `TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review`.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, real CLI run, validate-data, docs freshness, task-register
  consistency, report contract, contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation is scoped to data / PIT / signal / regime / threshold gap
  review only. It must not run a new strategy backtest, generate new signals,
  approve observation, resume candidate search or touch execution paths.
- 2026-07-07: Ran `aits validate-data --as-of 2026-07-05`; result
  `PASS_WITH_WARNINGS`, errors=0, warnings=2, report
  `outputs/reports/data_quality_2026-07-05.md`, audit artifact
  `artifacts/data_refresh_audit/validation/validate_data_2026-07-05_63e4bc4b675972a7.json`.
  The two warnings are `prices_download_manifest_checksum_missing` and
  `prices_adjustment_ratio_jump` for TQQQ adjustment-ratio review.
- 2026-07-07: Implemented
  `aits research strategies dynamic-strategy-data-pit-signal-quality-gap-review`,
  builder module, JSON / Markdown outputs, report registry entry, artifact
  catalog entry, system flow update and focused tests. Real CLI run returned
  `DYNAMIC_STRATEGY_DATA_PIT_AND_SIGNAL_QUALITY_GAP_REVIEW_READY`, with
  recombination line paused=true, resume candidate search recommended=false,
  PIT coverage matrix / signal construction review / regime expectation scoring
  review / threshold meta-dataset recommended=true, next route
  `TRADING-2403_Dynamic_Strategy_PIT_Coverage_Matrix_And_Signal_Construction_Review`,
  and all safety fields false/none.
- 2026-07-07: Focused validation passed:
  `python -m ruff check src\ai_trading_system\dynamic_strategy_data_pit_signal_quality_gap_review.py tests\research_strategies\test_dynamic_strategy_data_pit_signal_quality_gap_review.py src\ai_trading_system\cli_commands\research_execution_semantics.py`,
  `python -m py_compile ...`, and
  `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_dynamic_strategy_data_pit_signal_quality_gap_review.py`
  with 3 passed. Final closeout validation is recorded in the completed archive.
- 2026-07-07: Full closeout validation passed: Ruff, compileall, docs freshness
  589 docs / 0 issues, documentation contract 1299 reports / 0 errors / 0
  warnings, task-register consistency active=319 / completed=463 / failed=0,
  task-register consistency validate checks=5 / failed=0 / warnings=0,
  no terminal DONE row left in current task register, `git diff --check`, and
  contract-validation 197 passed with runtime artifact
  `outputs/validation_runtime/contract-validation_20260707T063644Z/test_runtime_summary.json`.
