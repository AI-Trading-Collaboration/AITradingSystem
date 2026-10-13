---
name: research-run
description: Plan, run or review AITradingSystem strategy research, a backtest, or an investment-facing conclusion. Research runs go through the evaluation bench (TRADING-2565); start from research/.
---

# Research runs

Every research question is preregistered and every run goes through the evaluation bench
(`python -m ai_trading_system.research_bench`, TRADING-2565). `research/README.md` lists the steps; a
pi research run follows `research/_template/brief.md` and runs only the bench commands it lists.

## Read first

- `research/README.md`: how research is recorded now and how a new research question starts.
- `research/ledger.md`: the 22 earlier research lines, each with a verdict (保留, 拒绝, 证据不足, 未测成)
  and a reopen condition. The ledger, not a task's status, is the research conclusion.
- `research/negative_results.md`: read before proposing any candidate. A proposal inside a falsified
  scope says how it differs from the earlier result.
- `research/assets.md`: reuse these before writing new evaluation code.
- Removed research code and reports are under tag `research-archive-2026-10-12`
  (`git show research-archive-2026-10-12:<path>`). Do not restore them into the working tree without
  an owner decision.

## Window and data

- Default research and backtest window starts on 2021-02-22 (`config/research/primary_research_window_policy.yaml`).
  Earlier data only for labelled sensitivity, proxy, or stress tests with data-quality caveats.
  2022-12-01 is not a default, comparator, or minimum start.
- QQQ direction questions (TRADING-2567) use `question_windows.qqq_direction` in that policy: develop
  on 1999-03-10..2010-12-31; read 2011-01-03..2021-02-19 once, after the preregistration ships;
  2021-02-22 onward is a consistency check only and cannot rescue a failed validation.
- Their data is the long-history snapshot: `python -m ai_trading_system.research_data.qqq_long_history
  fetch` in the main checkout on the owner's machine (`--prices-path` to the ops runtime copy's
  `prices_daily.csv`), read through `load_daily`. It stays local: never commit it, print its rows,
  or send them to an external model. Agents never open `data/raw/research/` or
  `data/raw/external_request_cache/` (the pi guard blocks it): the bench reads the data, opens the
  validation window only through a registered read, and prints summaries.
- Run `aits validate-data` (or the same code path) first and stop on failure when reading the daily
  cache. State the data quality status in the result.
- Every report states the selected window and the actual requested and evaluated date ranges.

## Order

1. **Preregister before reading any outcome**: hypothesis, window, cost model, kill criteria, and the
   decision rule, committed (and shipped) before the first result is read. Record every trial,
   including failed and abandoned ones.
2. **Run**: `run`, `validate`, `consistency` on the bench. Each run is recorded under
   `research/<question>/runs/` with its commit, settings and module hashes, trial number and window;
   a result from anywhere else is a draft, not evidence. The preregistration and the strategy module
   are zone C: after the owner approves them, changing either needs a new owner decision.
3. **Admit**: a conclusion enters `research/ledger.md` only after human review; agents may propose
   entries for `research/negative_results.md` but do not edit it. A validation PASS never means a
   candidate is valid, paper-shadow eligible, or approved for production or a broker.

## Thresholds and policy

Any threshold, score band, sample floor, position cap, promotion gate, or acceptance rule that can
change investment interpretation lives in a reviewed configuration (owner, version, rationale, intended
effect, validation, review condition) or is a named, commented invariant. Never introduce an unexplained
number in scoring, gates, backtests, or reports.

Write conclusions in Chinese; keep tickers, feature ids, file names, schema columns, and status codes in English.
