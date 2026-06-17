# TRADING-387 Readiness And Health Recovery Chain

## Status

DONE on 2026-06-17.

## Context

TRADING-384 owner review found the governance chain blocked by stale or missing
signal inputs, stale readiness / health evidence, insufficient cost and benchmark
metrics, and missing owner decision evidence. TRADING-385 restored the canonical
signal input artifacts with a remaining `latest_signal_snapshot` warning, and
TRADING-386 reran signal input completeness to produce a recovery artifact.

This task adds the next narrow recovery step: rerun the minimum evidence
staleness, shadow continuation readiness, and canonical paper-shadow health
chain after signal input recovery, then record whether normal paper-shadow
observation may resume.

## Scope

- Add a recovery-chain artifact family under
  `reports/etf_portfolio/dynamic_v3_rescue/readiness_health_recovery/`.
- Add CLI entry points:
  - `aits etf dynamic-v3-rescue readiness-health-recovery run`
  - `aits etf dynamic-v3-rescue readiness-health-recovery report --latest`
  - `aits etf dynamic-v3-rescue validate-readiness-health-recovery --latest`
- Reuse existing staleness, readiness, and health policy logic without changing
  their thresholds or classifications.
- Emit one final status:
  - `PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION`
  - `PAPER_SHADOW_STILL_BLOCKED`
  - `MANUAL_REVIEW_REQUIRED`
- Keep the output strictly limited to normal paper-shadow observation recovery.

## Safety Boundary

The recovery chain must not run promotion board logic, approve or execute
extended shadow, create official target weights, connect to broker/order
systems, mutate paper accounts, fabricate source artifacts, or mutate production
state.

`PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION` only means normal paper-shadow
observation is clean under the existing source gates. It is not promotion,
extended-shadow, official target, live trading, broker, or production approval.

## Acceptance Criteria

- Source artifact ids, statuses, validation statuses, blocking reasons, warning
  reasons, and next action are visible in JSON, Markdown, Reader Brief section,
  and CLI summaries.
- Any source validation failure, signal blocking status, evidence blocking
  status, blocked readiness state, blocked health state, missing source, or
  health blocking reason maps to `PAPER_SHADOW_STILL_BLOCKED`.
- Any warning-only source state maps to `MANUAL_REVIEW_REQUIRED`, not automatic
  resumption.
- Clean `OK` signal inputs, fresh / acceptable staleness, `READY_TO_CONTINUE`
  readiness, `HEALTHY` health, passing validations, and no warnings map to
  `PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION`.
- Validation fails closed if the artifact is missing required files, status is
  invalid, source statuses are hidden, or safety fields permit promotion,
  extended shadow, official targets, broker/order, paper-account mutation, or
  production mutation.

## Implementation Notes

- `src/ai_trading_system/etf_portfolio/dynamic_v3_readiness_health_recovery.py`
  implements the orchestration and validation artifact.
- `src/ai_trading_system/cli_commands/etf_portfolio.py` wires the new Typer
  command group and validation command.
- `tests/test_readiness_health_recovery.py` covers clean resumption, blocked
  health, warning/manual-review, and CLI run/report/validate paths.

## Validation

- Focused pytest: `tests/test_readiness_health_recovery.py`
- Ruff: new module, CLI wiring, and focused tests
- Compileall
- Documentation contract
- Real artifact run and validation for 2026-06-17:
  - recovery: `readiness-health-recovery_4c4fa150becc7305`
  - evidence staleness: `evidence-staleness-monitor_7d34cee566379931`
  - shadow continuation readiness: `shadow-continuation-readiness_332c166162e75c1c`
  - paper-shadow health: `paper-shadow-health_6b0b66768b4a1c59`
  - recovery validation: PASS
  - source validations: PASS

## Remaining Limitations

The 2026-06-17 real recovery chain reports `MANUAL_REVIEW_REQUIRED`, not normal
paper-shadow resumption. Current source status is `signal_input_status=WARNING`,
`evidence_freshness_status=ACCEPTABLE`,
`shadow_continuation_readiness=MANUAL_REVIEW_REQUIRED`, and
`paper_shadow_health_status=MANUAL_REVIEW_REQUIRED`. Warning reasons include the
remaining signal input warning, weekly coverage manual review, data refresh audit
warnings, and shadow continuation manual review. This is expected fail-closed
behavior and must not be bypassed in this task.
