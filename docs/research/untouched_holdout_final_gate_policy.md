# Untouched Holdout And Final Gate Policy

最后更新：2026-06-19

状态：`UNTOUCHED_HOLDOUT_AND_FINAL_GATE_FROZEN`

## Window Sets

- Development windows：AI cycle development context plus known diagnostic windows.
- Mini-backfill windows：`normal_market_regime` first, then selected stress/AI correction diagnostics.
- Full-backfill windows：AI cycle full pre-holdout window only after mini gates pass.
- Untouched holdout：`future_ai_cycle_holdout_2026_h2`，2026-07-01 至 2026-12-31。

## Access Policy

Development, mini-backfill and full-backfill runs may not access untouched holdout. Final gate
may access holdout only after B1-B6 full-backfill is complete without hard-stop blockers,
validate-data covers the full holdout, the holdout end date has passed, and manual owner review
authorizes access.

## Fail-Closed Rule

Any B1-B6 run that overlaps the untouched holdout before final gate must fail closed.

## Reader Brief

Holdout is frozen but not yet available. B1 can run only on the approved mini-backfill window.
