# Project Engineering Rules

This project is an investment decision-support system. Data quality, auditability,
and correctness are product requirements, not optional polish.

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

## Change Discipline

- Prefer durable, well-tested fixes over local patches that only satisfy the
  immediate command.
- Keep scope narrow, but do not trade correctness for speed in the scoring,
  data, or backtest path.
- If a design decision affects investment interpretation, record it in docs or
  configuration rather than leaving it implicit in code.
