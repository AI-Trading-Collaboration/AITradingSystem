# Project Engineering Rules

This project is an investment decision-support system. Data quality, auditability,
and correctness are product requirements, not optional polish.

## AI Regime Priority

The project primarily studies U.S. AI-related trading after the public launch of
ChatGPT. Strategy design, information collection, backtests, and report
interpretation should prioritize the configured `ai_after_chatgpt` market regime:

- anchor event: ChatGPT public launch on 2022-11-30;
- default backtest start: 2022-12-01, the first full U.S. trading day after that
  event;
- pre-2022 history may be used for warm-up, stress testing, and regime
  comparison, but it must not be treated as the default AI-cycle conclusion
  window.

Backtest and strategy reports should state the selected market regime and the
actual requested date range. If a primary conclusion relies on data before the AI
regime start, document why that older regime is relevant.

## System Flow Diagram Maintenance

`docs/system_flow.md` is the source-of-truth diagram for the path from data
inputs to intermediate evaluation and final conclusions. Any change that affects
CLI commands, critical configuration files, cache schemas, report outputs, data
quality gates, scoring modules, backtest behavior, market-regime interpretation,
or major new modules must update that diagram in the same change.

If a change intentionally does not affect the documented data flow, no diagram
update is required.

## No Silent Workarounds

When development hits a blocker, do not bypass it with a temporary workaround by
default.

Required process:

1. Identify the intended best solution.
2. Explain why that solution is currently blocked: technical cause, dependency
   limitation, data-source limitation, cost, latency, API permission, or missing
   requirement.
3. Evaluate whether the blocker should be fixed directly before continuing.
4. If a temporary workaround is truly necessary, discuss it with the project
   owner before implementing it.
5. Document every accepted workaround with:
   - reason it exists;
   - behavioral impact;
   - risk;
   - validation coverage;
   - exit condition for removing it.

Temporary code must not be hidden behind vague names such as `quick_fix`,
`fallback`, or `hack` without a tracked explanation.

## Data Source Discipline

Market, macro, fundamental, valuation, and news data are critical inputs. Data
source choices must be explicit and reviewable.

For each data source integration:

- record provider name, endpoint, request parameters, download timestamp, row
  count, and checksum where practical;
- distinguish primary sources, paid vendor sources, public convenience sources,
  and manual inputs;
- validate schema, completeness, freshness, duplicate keys, and suspicious
  values before downstream scoring;
- treat provider inconsistencies as investigation items, not as values to smooth
  over silently.

## Required Data Quality Gate

`aits validate-data` is the required quality gate for cached market and macro
data.

Any command, workflow, or module that produces technical features, scoring
outputs, backtest results, or daily reports from cached data must either:

1. run `aits validate-data` first and stop on failure; or
2. call the same validation code path directly and stop on failure.

Passing validation must be visible in downstream outputs. Reports that depend on
cached data must state the data quality status or link to the generated quality
report.

CI cannot validate local untracked market data because the cache is intentionally
not committed. This does not weaken the runtime requirement: local data-dependent
commands must enforce the gate themselves.

## Change Discipline

- Prefer durable, well-tested fixes over local patches that only satisfy the
  immediate command.
- Keep scope narrow, but do not trade correctness for speed in the scoring,
  data, or backtest path.
- If a design decision affects investment interpretation, record it in docs or
  configuration rather than leaving it implicit in code.

## Output Language

Project-facing conclusions, Markdown reports, and CLI result summaries should be
written in Chinese by default. Keep standard identifiers such as ticker symbols,
feature IDs, file names, schema columns, status codes, and established market
terms in English when translating them would reduce precision or break data
compatibility.
