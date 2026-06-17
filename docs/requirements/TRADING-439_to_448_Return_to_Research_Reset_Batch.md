# TRADING-439 to TRADING-448 Return-to-Research Reset Batch

Last update: 2026-06-18

## Completion Status

Status: DONE on 2026-06-18.

The implemented reset is governance-only and research-only. It records the
owner `return_to_research` decision, archives the current candidate as
`RETURNED_TO_RESEARCH`, opens a draft research redesign backlog/spec/backfill
path, and keeps normal paper-shadow, extended shadow, live trading, official
target weights, broker/order artifacts, production mutation, and candidate
rejection disabled.

## Background

TRADING-429 to TRADING-438 completed the decision-stage governance review after
the targeted recovery chain. The final decision-stage snapshot remains blocked:

- final snapshot: blocked;
- blockers: 8;
- warnings: 9;
- recommended owner action: `return_to_research`;
- exact-eight validation: PASS;
- report index: `PASS_WITH_EXPLICIT_WAIVERS`;
- report quality gate: `PASS_WITH_WARNINGS`, blocking=0;
- data validation: PASS;
- documentation contract: PASS;
- task-register consistency: PASS;
- normal paper-shadow: disabled;
- extended shadow: forbidden;
- live trading: forbidden.

The project owner has now instructed the system to complete TRADING-439 through
TRADING-448. TRADING-439 is an explicit owner decision to record
`owner_action=return_to_research` for `median_plus_regime_mismatch_filter`.
The remaining tasks formalize the research reset and must remain
research-only/manual-review-only.

## Safety Boundary

- Do not resume normal paper-shadow.
- Do not approve extended shadow.
- Do not approve live trading.
- Do not create official target weights.
- Do not create broker/order artifacts.
- Do not mutate production state.
- Do not automatically control positions.
- Do not reject the candidate unless a separate owner decision says reject.
- Do not create a new paper-shadow candidate from the draft spec.
- Do not weaken gates or fabricate missing evidence.

## Scope

The implementation must add machine-readable JSON reports, Markdown reports,
Reader Brief visibility, report registry entries, documentation, and focused
tests for:

1. TRADING-439 owner return-to-research decision record.
2. TRADING-440 candidate return-to-research transition pack.
3. TRADING-441 candidate failure-mode attribution.
4. TRADING-442 reusable evidence extraction.
5. TRADING-443 return-to-research hypothesis backlog.
6. TRADING-444 next candidate spec draft.
7. TRADING-445 research backfill plan for next candidate.
8. TRADING-446 archived candidate status update.
9. TRADING-447 research-cycle reset pack.
10. TRADING-448 return-to-research governance snapshot.

## Sequencing

1. Append a validated owner decision audit record with
   `owner_action=return_to_research`.
2. Generate a transition pack that confirms the candidate leaves the normal
   resumption path.
3. Attribute the failure modes that blocked normal paper-shadow continuation.
4. Classify reusable, weak, invalidated, stale, and non-comparable evidence.
5. Build a prioritized research hypothesis backlog from the failure modes.
6. Draft the next research-only candidate spec from P0 hypotheses.
7. Define backfill windows, metrics, and pass/fail/needs-more-evidence rules.
8. Update the candidate status ledger/report so the current candidate is
   returned to research, not rejected.
9. Generate the research-cycle reset pack.
10. Generate the final return-to-research governance snapshot.

## Acceptance Criteria

- Owner decision audit log append succeeds once and validates as PASS.
- The owner decision artifact id is stable and linked from downstream reports.
- Current candidate status is visible as `RETURNED_TO_RESEARCH` or
  `RETURN_TO_RESEARCH`.
- Normal paper-shadow remains disabled.
- Extended shadow and live trading remain forbidden.
- Transition/failure/evidence/backlog/spec/backfill/archive/reset/snapshot
  reports are generated under `outputs/reports/`.
- Each report includes purpose, input artifacts, output decision, safety
  boundary, limitations, next action, and Reader Brief fields.
- Report registry, artifact catalog, system flow, README, and task register are
  updated in the same change.
- Focused tests cover the append-only decision, report payloads, CLI output, and
  safety invariants.
- Data quality gate, documentation/report gates, and task-register consistency
  pass or a blocker is recorded explicitly.

## Progress Notes

- 2026-06-18: Created task and requirements record from owner attachment.
  Initial status is `IN_PROGRESS`; no report code or owner decision append had
  been changed at creation time.
- 2026-06-18: Completed implementation and generated real 2026-06-17 artifacts.
  `docs/decisions/TRADING-439_return_to_research_decision_2026-06-17.json` was
  created and `data/governance/owner_decision_audit_log.jsonl` received one
  append-only record for `TRADING-439_return_to_research_2026-06-17`.
  Downstream reset reports, final snapshot, and validation were generated under
  `outputs/reports/`; final snapshot is `RETURN_TO_RESEARCH_COMPLETE`, candidate
  status is `RETURNED_TO_RESEARCH`, validation is PASS, and Reader Brief now
  displays the Return-To-Research Governance Snapshot. Verified with
  `validate-data --as-of 2026-06-17` PASS, focused pytest, report/Reader
  regression pytest, documentation/task register pytest, ruff, py_compile, and
  `git diff --check`.
