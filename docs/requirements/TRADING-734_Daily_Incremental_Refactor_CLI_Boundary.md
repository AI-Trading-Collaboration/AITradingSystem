# TRADING-734: Daily Incremental Refactor CLI Boundary
最后更新：2026-06-23

## Status

- Status: DONE
- Date: 2026-06-21
- Scope: low-risk CLI module boundary refactor
- Owner: system implementation

## Context

The daily refactor automation found no qualifying baseline commit that also
updated `docs/refactor_log.md`. After the pending TRADING-703 through
TRADING-733 baseline was committed, the largest new maintenance risk was
`src/ai_trading_system/cli_commands/research.py` growing to carry both research
roadmap commands and the new data-foundation research subtrees.

## Safety Boundary

- `production_effect=none`.
- No production, paper-shadow, or official weight mutation.
- No broker, order, or live trading action.
- No threshold, score band, promotion gate, backtest acceptance, position limit,
  or data quality gate change.
- External CLI command names, arguments, output payloads, artifact paths, and
  exit semantics must remain compatible.

## Implementation Plan

1. Extract `aits research labels|runs|execution|cases` Typer subtrees into a
   dedicated `research_foundation` CLI module.
2. Keep `research.py` as the registration point for `aits research` while
   delegating the data-foundation research subtrees to the new module.
3. Update system flow and refactor log so future incremental refactors can use
   this run as a reference point.
4. Run focused CLI tests and validation tiers that cover research/data
   foundation command contracts.

## Acceptance Criteria

- `aits research labels`, `aits research runs`, `aits research execution`, and
  `aits research cases` commands still exist and pass the existing CLI smoke
  tests.
- Focused pytest for `tests/test_research_master_roadmap.py` and
  `tests/test_data_foundation_roadmap.py` passes with xdist.
- Scoped Ruff, compileall, affected-file Black check, and `git diff --check`
  pass.
- `docs/refactor_log.md` records the run and final refactor commit SHA.

## Progress

- 2026-06-21: Task opened by daily incremental refactor automation after
  committing TRADING-703 through TRADING-733 baseline. Implementation started.
- 2026-06-21: Completed. `aits research labels|runs|execution|cases` now
  register through `src/ai_trading_system/cli_commands/research_foundation.py`
  while the external `aits research ...` command surface remains unchanged.
  Validation passed: focused pytest 10 passed, scoped Ruff, compileall,
  affected-file Black check, `fast-unit` 84 passed, `contract-validation`
  83 passed, and `git diff --check`.
