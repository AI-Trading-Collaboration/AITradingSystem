---
name: research-run
description: Plan, run, or review an AITradingSystem research experiment or backtest (preregistration, run authorization, research runs, result admission, prospective captures under a capture hold). Use for any strategy research, backtest, or investment-facing conclusion.
---

# Research runs

## Window and data

- Default research and backtest window starts on 2021-02-22 (`config/research/primary_research_window_policy.yaml`).
  Earlier data only for labelled sensitivity, proxy, or stress tests with data-quality caveats.
  2022-12-01 is not a default, comparator, or minimum start.
- Run `aits validate-data` (or the same code path) first and stop on failure. State the data quality
  status in the result.
- Every report states the selected window and the actual requested and evaluated date ranges.

## Order

1. **Preregister before reading any outcome**: hypothesis, window, cost model, kill criteria, and the
   decision rule, committed (and shipped) before the first result is read. Record every trial,
   including failed and abandoned ones.
2. **Run** through the research command (`aits research ...`) or the line's documented script. Each run
   records the commit, whether result-affecting code was modified, configuration hashes, and the
   requested/evaluated ranges (`core/provenance.py`). A run on modified code is a draft, not evidence.
3. **Admit** results only through the line's result-admission or decision record. A validation PASS never
   means a candidate is valid, paper-shadow eligible, or approved for production or a broker.

## Thresholds and policy

Any threshold, score band, sample floor, position cap, promotion gate, or acceptance rule that can
change investment interpretation lives in a reviewed configuration (owner, version, rationale, intended
effect, validation, review condition) or is a named, commented invariant. Never introduce an unexplained
number in scoring, gates, backtests, or reports.

## Prospective captures

Captures run under a capture hold (steps in `docs/operations/operations_runbook.md` section 7):
`python scripts/capture_hold.py acquire --candidate-commit <HEAD> --path <output dir> --actor <name>
--ttl-minutes <n>`, launch with `--source-hold-id <hold-id>`, then `python scripts/capture_hold.py release
--hold-id <hold-id>`. The hold needs the exact commit with unmodified `src/config/scripts/tools`. It is a
local self-attestation, not an independent authority. Never retry a capture key or re-sign a time.

## Research records are evidence

Configs, docs, and files that research records reference or pin by hash (including
`config/etf_portfolio/`, `src/ai_trading_system/etf_portfolio/regime.py`, the appendix of
`docs/system_flow.md`, and `registry/development_tasks/2f/`) must not be edited or moved. A research
line's requirements change only by owner decision.

Write conclusions in Chinese; keep tickers, feature ids, file names, schema columns, and status codes in English.
