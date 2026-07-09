# TRADING-2438I Growth Tilt Top3 Candidate PIT Replay Recheck After Remaining Blocker Closure

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438I_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE`
- owner route source: TRADING-2438H
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`

TRADING-2438H completed remaining candidate PIT replay blocker closure and
returned `GROWTH_TILT_REMAINING_CANDIDATE_PIT_REPLAY_BLOCKER_CLOSURE_READY`.
That READY status only means the three candidates can enter a new independent
recheck. It is not a replay PASS / FAIL conclusion, not a forward-aging
handoff, and not a paper-shadow or production signal.

## Scope

TRADING-2438I reads the TRADING-2438H remaining blocker closure result, its
replay recheck readiness handoff, the remaining blocker before/after matrix,
and the three candidate replay output records. It independently classifies each
candidate as `PASS`, `FAIL`, or `BLOCKED` based on materialized replay output
records and evidence completeness after 2438H.

This task must fail closed. A candidate that still has `replay_status=BLOCKED`,
missing metric materialization, or a persistent blocker reason remains
`BLOCKED`; it must not be silently converted to `FAIL` or to a no-passing
candidate conclusion.

## Outputs

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/recheck_after_remaining_blocker_closure_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/candidate_pass_fail_blocked_decision_matrix.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/forward_aging_handoff_readiness_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/persistent_candidate_replay_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_recheck_after_remaining_blocker_closure.md`
- `docs/research/growth_tilt_candidate_recheck_after_remaining_blocker_decision_matrix.md`
- `docs/research/growth_tilt_candidate_forward_aging_after_remaining_blocker_handoff_summary.md`
- `docs/research/growth_tilt_persistent_candidate_replay_blocker_summary.md`
- `docs/research/growth_tilt_candidate_recheck_after_remaining_blocker_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438J_or_2439A_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-after-remaining-blocker-closure --as-of 2026-07-08
```

## Decision Rules

- If TRADING-2438H is not READY or does not route to 2438I, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED`.
- If 2438H did not close remaining blockers to after count zero, status is
  BLOCKED.
- If replay recheck handoff is not ready, if the candidate recheckable count is
  not three, or if the three candidate replay output records are incomplete,
  status is BLOCKED.
- If any candidate remains `BLOCKED`, final status is BLOCKED and next route is
  `TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation`.
- If no candidate is BLOCKED and at least one candidate is `PASS`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_READY`
  and next route is
  `TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_PIT_Replay_Recheck`.
- If pass/fail/blocked is exactly `0/3/0`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_NO_PASSING_CANDIDATE`
  and next route is
  `TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review`.

## Safety Boundary

This task does not run a fresh PIT replay, backtest, scoring job, daily report,
paper-shadow job, paper-shadow schedule, broker workflow, production workflow,
outcome binding, signal generation, trading advice generation, or portfolio
weight mutation.

## Acceptance Criteria

- The new CLI runs deterministically and writes all JSON / Markdown artifacts.
- 2438H READY, remaining blocker after count zero, replay recheck handoff, and
  three candidate replay output records are validated.
- Each candidate decision row includes candidate id, replay status, evidence
  references, reason fields, forward-aging handoff key / eligibility, and safety
  fields set to false / none.
- `0/0/3` remains BLOCKED and routes to TRADING-2438J; it is not mislabeled as
  no-passing candidate or forward-aging ready.
- `0/3/0` routes to no-passing candidate evidence review.
- Any PASS with no BLOCKED routes to forward-aging candidate pack rebuild.
- Persistent blocker summary, no-effect boundary, registry, catalog, system
  flow, task register, completed archive, and research docs stay aligned.
- Paper-shadow, schedule, production, broker, trading advice, and portfolio
  mutation flags remain disabled / false / none.

## Progress Notes

- 2026-07-10: 根据 owner roadmap 附件新增并进入 `IN_PROGRESS`。实现必须承接
  2438H READY handoff 后重新判定 PASS / FAIL / BLOCKED，并保留真实 `0/0/3`
  candidate output records 的 BLOCKED 结论，不能把 closure handoff 误标为
  replay PASS/FAIL、NO_PASSING_CANDIDATE、forward-aging ready、paper-shadow
  candidate 或 2440 no-candidate。
- 2026-07-10: 实现完成并进入 `DONE`。真实 CLI 输出
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_REMAINING_BLOCKER_CLOSURE_BLOCKED`；
  source_2438h_remaining_blocker_closure_ready=true，remaining blocker after=0，
  replay_recheck_handoff_ready=true，candidate_recheckable_after_closure_count=3，
  candidate pass/fail/blocked=`0/0/3`，persistent blocker count=3，
  forward-aging handoff=false，下一跳为
  `TRADING-2438J_Growth_Tilt_Persistent_Candidate_PIT_Replay_Blocker_Escalation`。
  本结论不是 replay FAIL、NO_PASSING_CANDIDATE、forward-aging eligibility、
  paper-shadow candidate found 或 2440 promotion review 结论。
