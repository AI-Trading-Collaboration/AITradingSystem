# TRADING-1155 to 1164 Manual External Platform Evidence and Signoff

## Background

TRADING-1129 to 1140 established the external validation and reconciliation chain, and
TRADING-1141 to 1154 used that chain as a balanced-core research-only launch gate. The
current remaining limitation is manual: the system can replay internal weight paths and
explain warning states, but it still lacks real external platform exports, screenshots,
metric-definition confirmation, and SGOV convention signoff.

This batch records and reviews manual external evidence for static baselines. It does
not connect to brokers, does not upload account information, does not produce trading
recommendations, and does not advance paper-shadow or production state.

## Scope

Implement TRADING-1155 to TRADING-1164:

|Task|Stage|Acceptance Criteria|
|---|---|---|
|TRADING-1155|Manual external record template|Create YAML/CSV templates, input guide, CLI, and artifacts with required fields and safety metadata.|
|TRADING-1156|Static baseline manual runbook|Generate owner-facing runbook for `100_qqq`, `qqq_50_sgov_50`, and `qqq_60_sgov_40`, including weights, date range, dividend/SGOV notes, screenshots, and exports.|
|TRADING-1157|Manual input ingestion|Read YAML and/or CSV manual records, validate strategy IDs, date range, weights, metric availability markers, evidence references, and SGOV convention.|
|TRADING-1158|Metric convention signoff|Record platform metric definitions and owner confirmation status for annual return, max drawdown, Sharpe, Calmar, turnover, rebalance, and dividend handling.|
|TRADING-1159|SGOV convention signoff|Record external SGOV handling, internal convention, annual-return delta, and static/dynamic impact assessment.|
|TRADING-1160|Final static reconciliation after manual input|Compare ingested external records to internal static baseline metrics and classify reconciled, explained warning, mismatch, or blocked.|
|TRADING-1161|Dynamic weight-path external support check|Classify Portfolio Visualizer, testfol.io, QuantConnect, TradingView, and local independent notebook support for dynamic weight-path replay.|
|TRADING-1162|QuantConnect weight-path replay preflight|Define CSV/custom-data schema, symbol mapping, rebalance/execution/cash/SGOV handling, output metrics, implementation steps, and blockers.|
|TRADING-1163|Manual evidence owner signoff|Summarize manual records, metric convention, SGOV convention, final reconciliation, dynamic replay need, and safety status.|
|TRADING-1164|Manual evidence master review|Aggregate 1155 to 1163 and answer whether external validation can be accepted with warnings while keeping no paper-shadow/no production/no broker.|

## Implementation Plan

1. Add input templates under `inputs/external_validation/manual_external_records/` and
   owner guide/runbook docs under `docs/research/`.
2. Extend `src/ai_trading_system/external_validation.py` with deterministic builders
   for the manual-evidence stage. Builders must not fabricate external records; missing
   owner input remains an explicit missing or unknown status.
3. Register new commands in `src/ai_trading_system/cli_commands/research_external_validation.py`.
4. Add report registry entries and artifact catalog/system-flow documentation.
5. Extend focused tests for builders, CLI smoke path, report registry entries, and safety
   metadata.

## Safety Boundary

All artifacts and summaries must keep:

- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `manual_review_required=true`
- `production_effect=none`

This batch does not integrate with the formal Reader Brief. Any future Reader Brief use
may only show manual external evidence status, static reconciliation status, SGOV
convention status, and the safety boundary.

## Progress Notes

- 2026-06-26: Added as `IN_PROGRESS` for owner-requested TRADING-1155 to 1164 manual
  external evidence and signoff workflow. Implementation must preserve research-only
  boundaries and fail closed when real external platform records are absent.
- 2026-06-26: Implementation completed and moved to `BLOCKED_OWNER_INPUT`. Added 10
  research CLI commands, YAML/CSV templates, owner guide/runbook, manual ingestion,
  metric/SGOV signoff, final reconciliation, dynamic support, QC preflight, owner/master
  review, report registry entries, artifact catalog/system flow documentation, and
  focused tests. Validation passed with Ruff, compileall, focused parallel pytest for
  external validation and equal-risk growth tilt, task/register/report/docs contract
  pytest, and `git diff --check`. Actual external platform evidence remains blocked
  until the project owner supplies real exports/screenshots and manual convention signoff.
- 2026-06-27: Owner supplied `G:\Download\Portfolio_20260626180410.xlsx`, a real
  Portfolio Visualizer export for `100_qqq` covering 2022-12-01 to 2026-06-25. The
  workbook is valid evidence for the single-asset QQQ static baseline, but it does not
  cover `qqq_50_sgov_50`, `qqq_60_sgov_40`, metric convention signoff, or SGOV convention
  signoff. The ingestion layer must preserve the platform's `No rebalancing` text for
  this single-asset case and treat it only as an audited single-asset equivalence, not as
  a general replacement for monthly rebalance validation.
- 2026-06-27: The supplied workbook was retained under
  `inputs/external_validation/manual_external_records/evidence/` and summarized in
  `static_baseline_external_records.yaml` with provider, URL, parameters, timestamp,
  non-empty row count, and SHA256. Real ingestion returns `MANUAL_EXTERNAL_INPUT_PARTIAL`
  with one valid `100_qqq` record and missing `qqq_50_sgov_50` / `qqq_60_sgov_40`.
  Final reconciliation returns `STATIC_BASELINE_MANUAL_MISMATCH`: annual return is close,
  but Portfolio Visualizer reports monthly-return max drawdown while internal static
  validation uses daily equity-path drawdown. Metric convention signoff remains
  `METRIC_CONVENTIONS_STILL_UNKNOWN`, SGOV signoff remains `SGOV_CONVENTION_STILL_UNKNOWN`,
  owner recommendation is `NEED_MORE_MANUAL_EVIDENCE`, and master status is
  `EXTERNAL_MANUAL_EVIDENCE_NEEDS_MORE_INPUT`.
