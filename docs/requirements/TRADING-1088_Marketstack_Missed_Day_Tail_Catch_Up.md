# TRADING-1088 Marketstack Missed-Day Tail Catch-Up

最后更新：2026-07-03

## Status

- Status: VALIDATING
- Priority: P0
- Owner / next responsible party: system implementation + project owner review
- Created: 2026-07-03

## Context

The daily PIT automation on 2026-07-03 showed that TRADING-1087's
owner-approved Marketstack overage path correctly permits only a single-window,
single-day tail refresh. The 2026-07-01 run was blocked even though the
projected quota shortfall was below the 10% cap, because the Marketstack cache
was latest through 2026-06-29 and the requested tail window was
2026-06-30..2026-07-01.

The intended fix is not to raise `max_calendar_days_per_window` globally. A
missed-day catch-up must remain auditable, bounded, and separate from
full-history refreshes, new-ticker backfills, or large quota shortfalls.

## Scope

1. Add a dedicated Marketstack tail catch-up budget profile for missed daily
   gaps.
2. Split eligible Marketstack tail gaps into one U.S. equity trading day per
   fetch window.
3. Preserve fail-closed behavior for full-history refreshes, new ticker gaps,
   multi-window stale mixes, missing quota limit, over-10% projected shortfall,
   or estimated usage above the approved small catch-up boundary.
4. Expose the applied catch-up profile, original windows, split windows,
   projected shortfall, ratio, and violation reasons in manifest and failure
   diagnostics.
5. Keep `aits validate-data` as the required downstream quality gate.

## Safety Boundary

- No production weights.
- No active shadow weights.
- No broker or trading action.
- No `--without-marketstack` as a routine fix.
- No hidden conversion of full-history or repair backfill into daily catch-up.

## Acceptance Criteria

- A two-trading-day Marketstack tail gap with total estimated usage in the
  approved small boundary can proceed as an audited catch-up with split
  one-day windows.
- A full-history Marketstack request remains fail-closed under insufficient
  quota.
- An over-10% projected shortfall remains fail-closed.
- Failure reports include the owner-approved budget profile and
  `violation_reasons`, including calendar-window blockers.
- Download manifests expose catch-up metadata and request budget status.
- Focused tests cover allowed catch-up, blocked full-history, blocked quota
  ratio, and diagnostic rendering.
- Operations runbook and system flow document the catch-up path and production
  boundary.

## Progress Notes

- 2026-07-03: Added after daily automation hit a missed-day Marketstack tail
  gap. Owner requested long-term engineering repair after unrelated full
  validation completed.
- 2026-07-03: Implemented dedicated `owner_approved_tail_catch_up` profile,
  split-window catch-up execution, richer quota failure diagnostics, CLI budget
  visibility, runbook/system-flow documentation, and focused tests. Validation
  passed: `tests/test_data_download.py` 16 passed, `tests/test_config.py`
  + `tests/test_data_download.py` 33 passed, Ruff, compileall, docs freshness,
  task-register consistency run/validate, and contract-validation 193 passed.
