# TRADING-2398: Dynamic Strategy Recombination Candidate Gate Evidence And Targeted Improvement Plan

最后更新：2026-07-07

## Context

TRADING-2397 completed the recombination candidate owner review and observation
decision. The best recombination candidate is
`growth_tilt_lower_turnover_guarded_transfer_v1`; TRADING-2396 classified it as
`OWNER_REVIEW_REQUIRED`, and TRADING-2397 retained owner review without approving
research-only observation.

TRADING-2398 is the plan-only follow-up. It must identify the remaining gate
evidence gaps and define targeted improvement variants for TRADING-2399 to test.

## Scope

This task will:

1. Read prior validated TRADING-2397 owner decision artifacts.
2. Read prior validated TRADING-2396 recombination retest artifacts.
3. Read prior validated TRADING-2395 recombination candidate plan artifacts.
4. Read prior validated TRADING-2393 component ablation artifacts.
5. Generate a gate evidence gap analysis for
   `growth_tilt_lower_turnover_guarded_transfer_v1`.
6. Generate a targeted improvement plan and six targeted variants for TRADING-2399.
7. Generate a TRADING-2399 retest plan, acceptance criteria, route JSON and
   research Markdown docs.

## Non-Goals

- Do not run a new backtest.
- Do not read fresh cached market/macro data.
- Do not generate new technical features, signals, scoring output, daily report
  output, paper-shadow state, production state, broker action, order, event
  append, outcome binding or scheduled task.
- Do not approve research-only observation.

## Safety Boundary

- `production_effect=none`
- `broker_action=none`
- `research_only_observation_approved=false`
- `paper_shadow_enabled=false`
- `event_append_enabled=false`
- `outcome_binding_enabled=false`
- `scheduler_enabled=false`
- `production_enabled=false`
- `broker_action_enabled=false`
- `daily_report_generated=false`

If the implementation only reads prior validated artifacts, `aits validate-data
--as-of 2026-07-05` is not required; completion notes must state that no fresh
cached market/macro data was read and no cached-data-dependent output was
generated.

## Planned Outputs

- `outputs/research_strategies/dynamic_strategy_recombination_candidate_gate_evidence_plan/gate_evidence_plan_result.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_gate_evidence_plan/gate_evidence_gap_summary.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_gate_evidence_plan/targeted_improvement_plan.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_gate_evidence_plan/retest_plan_2399.json`
- `outputs/research_strategies/dynamic_strategy_recombination_candidate_gate_evidence_plan/next_route.json`
- `docs/research/dynamic_strategy_recombination_candidate_gate_evidence_plan.md`
- `docs/research/dynamic_strategy_recombination_gate_evidence_gap_summary.md`
- `docs/research/dynamic_strategy_recombination_targeted_improvement_plan.md`
- `docs/research/dynamic_strategy_2399_route.md`

## Acceptance Criteria

- `aits research strategies dynamic-strategy-recombination-candidate-gate-evidence-plan`
  runs successfully and returns
  `DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY`.
- Output identifies candidate under review as
  `growth_tilt_lower_turnover_guarded_transfer_v1`.
- Output confirms decision from TRADING-2396 is `OWNER_REVIEW_REQUIRED` and owner
  decision from TRADING-2397 is
  `KEEP_OWNER_REVIEW_REQUIRED_WITH_NO_OBSERVATION_APPROVAL_AND_TARGET_GATE_EVIDENCE`.
- Output marks `gate_evidence_gap_summary_ready`,
  `targeted_improvement_plan_ready` and `retest_plan_2399_ready` as true.
- Output includes non-empty planned targeted variants and the recommended route
  `TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_Evidence_Retest`.
- Safety fields remain false/none, including no observation, paper-shadow,
  scheduler, event append, outcome binding, production, broker or daily report.
- Report registry, artifact catalog, system flow, task register and completed
  archive are updated.
- Focused tests, docs freshness, task/register consistency, report contract,
  contract validation and diff checks pass.

## Progress

- 2026-07-07: Task opened from owner attachment and moved to `IN_PROGRESS`.
  Implementation will follow the TRADING-2397 source-validation and no-side-effect
  pattern.
- 2026-07-07: Implementation completed and moved to `DONE`. Added the
  `dynamic-strategy-recombination-candidate-gate-evidence-plan` CLI, builder,
  focused tests, report registry entry, artifact catalog row, system flow update
  and research docs. Real run returned
  `DYNAMIC_STRATEGY_RECOMBINATION_CANDIDATE_GATE_EVIDENCE_AND_TARGETED_IMPROVEMENT_PLAN_READY`
  with 6 planned targeted variants and next route
  `TRADING-2399_Dynamic_Strategy_Recombination_Candidate_Targeted_Gate_Evidence_Retest`.
  The run did not execute `aits validate-data --as-of 2026-07-05` because it
  only read prior validated TRADING-2397 / 2396 / 2395 / 2393 artifacts and did
  not read fresh cached market data, run a new backtest, generate technical
  features, scoring, daily report output or trading advice.
