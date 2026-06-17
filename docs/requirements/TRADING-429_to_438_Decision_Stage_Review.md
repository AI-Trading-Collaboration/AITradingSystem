# TRADING-429 to TRADING-438 Decision-Stage Review

Last update: 2026-06-18

## Background

TRADING-420 to TRADING-428 completed the targeted recovery chain but left the
governance pack blocked by substantive recovery, owner, gate, and observation
conditions. The latest confirmed recovery state is:

- recovery governance pack: `RECOVERY_GOVERNANCE_BLOCKED`;
- remaining blockers: 8;
- remaining warnings: 9;
- report index: `PASS_WITH_EXPLICIT_WAIVERS`, `unwaived=0`;
- data validation: PASS;
- documentation contract: PASS;
- task-register consistency: PASS;
- report quality gate: `PASS_WITH_WARNINGS`, blocking=0;
- normal gate: `RESUME_NORMAL_SHADOW_BLOCKED`;
- owner action: `hold`.

This work is diagnosis and owner decision support only. It must not weaken any
gate, append a real owner decision without an explicit instruction, generate a
normal paper-shadow signoff packet while the gate is blocked, start the
observation clock, approve extended shadow, approve live trading, create
official target weights, or add broker/order artifacts.

## Scope

The implementation must add machine-readable JSON reports, Markdown reports,
Reader Brief sections, validation commands where required, and focused tests for
the following decision-stage artifacts:

1. TRADING-429 exact eight-blocker decision review.
2. TRADING-430 normal shadow gate gap analysis.
3. TRADING-431 promotion blocker after metrics review.
4. TRADING-432 candidate research return assessment.
5. TRADING-433 owner decision options packet.
6. TRADING-434 owner decision append dry-run machinery.
7. TRADING-435 observation clock readiness plan.
8. TRADING-436 post-decision rerun plan.
9. TRADING-437 report quality warning drilldown.
10. TRADING-438 governance status snapshot after decision review.

## Sequencing

1. Build TRADING-429 to read the latest governance artifacts and list the exact
   eight blockers with source fields, classification, next action, code/data
   fixability, owner judgment requirement, and research-return implication.
2. Build TRADING-430 to explain every normal paper-shadow resumption gate
   condition and why `RESUME_NORMAL_SHADOW_BLOCKED` remains true.
3. Build TRADING-431 to reconcile available cost/benchmark metrics with the
   still-blocked promotion conclusion.
4. Build TRADING-432 to produce an advisory candidate-level decision assessment
   for `median_plus_regime_mismatch_filter`.
5. Build TRADING-433 to turn the diagnostic outputs into conservative manual
   owner options without writing a decision.
6. Build TRADING-434 dry-run support for proposed owner decision entries and
   append-only validation behavior.
7. Build TRADING-435 to define when the normal observation clock can start.
8. Build TRADING-436 to document rerun plans for each possible owner decision.
9. Build TRADING-437 to drill into report quality warnings and fix only safe
   metadata/template issues.
10. Build TRADING-438 to summarize the post-review governance state.

## Acceptance Criteria

- All new report outputs are generated under `outputs/reports/` with stable
  artifact ids, `production_effect=none`, Reader Brief-facing sections, source
  links, and explicit safety boundaries.
- The exact blocker review lists eight blockers, not just the count.
- The normal shadow gate gap analysis exposes required value, actual value,
  pass/warning/fail status, upstream dependency, and owner action requirement for
  every gate condition.
- The promotion review keeps unfavorable or insufficient cost/benchmark evidence
  visible and does not force promotion.
- The candidate assessment is advisory and does not mutate owner action,
  candidate state, paper-shadow state, production state, official weights,
  broker state, or order artifacts.
- The owner options packet includes `keep_hold`,
  `approve_resume_normal_shadow`, `return_to_research`, and `reject_candidate`,
  and states which options are allowed by current gates.
- The owner decision machinery can dry-run one of those options and validate
  append-only behavior without writing a real decision entry in this task.
- The observation clock readiness plan does not start the clock while normal
  paper-shadow is blocked or owner action remains `hold`.
- The post-decision rerun plan contains no branch that permits extended shadow,
  live trading, official target weights, broker actions, or order artifacts.
- The report quality warning drilldown lists all current warnings exactly and
  only reduces warnings when the fix is a safe metadata/template correction.
- The final governance snapshot states blocker count, warning count,
  recommended owner action, normal-shadow resumability, and extended/live trading
  prohibitions.
- Focused tests, documentation contract, task-register consistency, report
  validation, and relevant CLI smoke commands pass or record an explicit blocker.

## Progress Notes

- 2026-06-18: Created task and requirements record from owner attachment.
  Initial status was `IN_PROGRESS`; no blocker logic or owner decisions had been
  changed at creation time.
- 2026-06-18: Implementation completed and validated. The decision-stage review
  now generates all TRADING-429 to TRADING-438 JSON/Markdown artifacts, Reader
  Brief navigation metadata, report registry entries, artifact catalog entries,
  system-flow documentation, README guidance, exact-eight validation, and owner
  decision dry-run support. The live 2026-06-17 run remains governance-blocked:
  remaining blockers=8, remaining warnings=9, normal paper-shadow=false,
  extended/live trading forbidden=true, recommended owner action
  `return_to_research`, and `owner_decision_dry_run` kept
  `would_append=false` / `real_entry_written=false`.

## Validation Results

- `python -m py_compile ...` passed for the new decision-stage report module,
  report CLI wiring, Reader Brief integration, owner decision/template updates,
  normal resumption gate, and report quality gate.
- `python -m ruff check ...` passed for the touched source and focused tests.
- Focused pytest passed: `tests/test_decision_stage_review.py`,
  `tests/test_owner_decision_audit_log.py`,
  `tests/test_owner_review_template_v2.py`, and
  `tests/test_normal_paper_shadow_resumption_gate.py` returned 24 passed.
- Documentation/report consistency pytest passed:
  `tests/test_documentation_contract.py`, `tests/test_report_index.py`,
  `tests/test_reader_brief.py`, `tests/test_reader_brief_consistency.py`, and
  `tests/test_task_register_consistency.py` returned 35 passed.
- Combined focused/documentation pytest returned 59 passed.
- `python -m ai_trading_system.cli validate-data --as-of 2026-06-17` returned
  PASS with 0 errors and 0 warnings.
- `aits reports decision-stage-review --as-of 2026-06-17` generated all ten
  decision-stage reports with `production_effect=none`.
- `aits reports validate-eight-blocker-decision-review --as-of 2026-06-17`
  returned PASS with exact_blockers=8 and failed=0.
- `aits reports index --as-of 2026-06-17` returned
  `PASS_WITH_EXPLICIT_WAIVERS` with unwaived=0.
- `aits reports reader-brief --as-of 2026-06-17` returned
  `LIMITED_READER_CONTEXT`, preserving the existing limited context while adding
  the decision-stage governance snapshot.
- `aits reports quality-gate --date 2026-06-17` returned `PASS_WITH_WARNINGS`
  with blocking=0.

## Final State

This task is `DONE` for engineering scope. It does not clear the underlying
governance blockers and does not authorize normal paper-shadow, extended shadow,
live trading, official target weights, broker actions, order artifacts, or any
production mutation. Current owner action remains a manual decision outside this
implementation.
