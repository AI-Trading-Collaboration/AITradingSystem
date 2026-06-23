# TRADING-894 to 910 Simple Baseline Forward Aging Convergence

## Status

IN_PROGRESS as of 2026-06-23.

## Scope

Implement the simple-baseline candidate convergence and forward-aging batch requested for TRADING-894 through TRADING-910:

- reconcile TRADING-865 through TRADING-893 real-run artifacts;
- freeze the forward-aging candidate set;
- lock primary and comparator strategy definitions with hashes;
- define and write research-only forward-aging observations;
- mature observation windows and generate a scoreboard;
- expose data-quality, paper-shadow threshold, daily-reader summary, automation-readiness, risk-budget, absolute-return-gap, owner-review, and master-review artifacts;
- keep `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`, `manual_review_required=true`.

The existing repository already has a dirty `docs/requirements/TRADING-894_Daily_Incremental_Refactor_Simple_Baseline_CLI_Boundary.md` file from prior work. This batch therefore uses the combined task-register ID `TRADING-894_to_910_SIMPLE_BASELINE_FORWARD_AGING_CONVERGENCE` to avoid creating a duplicate standalone `TRADING-894` row, while the report names and CLI commands still implement the owner-requested TRADING-894 through TRADING-910 semantics.

## Non-Goals

- No production configuration mutation.
- No broker integration, broker read/write, order ticket, or target-weight instruction.
- No paper-shadow activation.
- No LEAPS, Wheel, TQQQ-heavy, or tail-risk fallback restart.
- No expansion of the strategy pool beyond the frozen primary/comparator/challenger set unless a future owner-approved task updates this document and the task register.

## Candidate Freeze

Primary candidate:

- `equal_risk_qqq_sgov`

Static comparators:

- `qqq_50_sgov_50`
- `qqq_60_sgov_40`
- `100_qqq` as the public comparator id, mapped to registry strategy `qqq_100_static`

Dynamic challenger:

- `dyn_tqqq_capped_trend`

## Forward-Aging Contract

Observation windows are `5d`, `10d`, `20d`, `60d`, and `120d` trading-day windows. The initial review, weak review, and paper-shadow-review prerequisites each require at least 20 matured observations at the relevant window, as configured in `config/research/simple_baseline_strategy_registry.yaml`.

Observation artifacts are research-only samples. They must preserve original decision-date weights and signal inputs, and maturity updates may only add forward-window outcomes and maturation timestamps.

## Acceptance Criteria

- Add the 17 requested `aits research strategies ...` commands.
- Generate JSON and Markdown artifacts for each requested report family, including date-scoped forward-aging observation artifacts.
- Register every report in `config/report_registry.yaml` with `artifact_selection_policy=latest_available`, `required_for_daily_reading=false`, `production_effect=none`, and `broker_action=none`.
- Document every artifact family in `docs/artifact_catalog.md` and update `docs/system_flow.md`.
- Add a minimal Reader Brief forward-aging summary that never emits trade advice or rebalance language.
- Keep all safety fields false/none/manual-review-required in payloads and summaries.
- Cover the new functions, CLI smoke path, report registry entries, Reader Brief summary, and artifact writing in focused pytest.
- Run focused validation, Ruff, compileall, and `git diff --check`; if full validation is blocked, document the blocker and command output.

## Progress Log

- 2026-06-23: Created this requirement document and task-register row before implementation, per task-register discipline. Owner-requested safety boundary remains research-only / observe-only.
- 2026-06-23: Implementation completed and moved to validating. Added `src/ai_trading_system/simple_baseline_forward_aging.py`, 17 CLI commands, forward-aging policy config, Reader Brief summary, report registry entries, artifact catalog row, system-flow paragraph, owner/master review docs, and focused tests. Focused parallel pytest passed for `tests/test_simple_baseline_portfolio_control.py`; report/documentation/task/artifact focused pytest passed; `python -m ruff check .`, `python -m compileall src tests scripts`, and `git diff --check` passed. Real CLI smoke ran all 17 commands; current local cached data quality returned FAIL, so data-dependent forward aging outputs correctly stopped at `DATA_QUALITY_BLOCKED`, `MARKET_DATA_MISSING`, `MATURITY_BLOCKED`, `RISK_BUDGET_REVIEW_BLOCKED`, `ABSOLUTE_RETURN_GAP_REVIEW_BLOCKED`, `AUTOMATION_NEEDS_FIXES`, and `NEED_MORE_BACKTEST_REVIEW` rather than emitting mature conclusions.
