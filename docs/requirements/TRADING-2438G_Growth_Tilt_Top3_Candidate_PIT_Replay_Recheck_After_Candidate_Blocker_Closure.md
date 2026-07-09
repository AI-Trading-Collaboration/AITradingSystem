# TRADING-2438G Growth Tilt Top-3 Candidate PIT Replay Recheck After Candidate Blocker Closure

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438G_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE`
- owner route source: TRADING-2438F
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`

TRADING-2438F completed candidate-level replayability blocker closure and
produced `replayability_handoff_ready=true`. That READY status only means the
three top-3 candidates can be rechecked by this task. It does not mean any
candidate has passed PIT replay, failed PIT replay, qualified for forward aging,
or become a paper-shadow candidate.

## Scope

TRADING-2438G reads the TRADING-2438F blocker closure result, replayability
handoff manifest, candidate-level closure records, and the three candidate
replay output records. It independently classifies each candidate as `PASS`,
`FAIL`, or `BLOCKED` after candidate blocker closure.

The command must fail closed to `BLOCKED` when replayability handoff is not
ready, source artifacts are incomplete, candidate record count is not three, or
any candidate still lacks enough post-closure replay evidence to become an
explicit PASS or FAIL.

## Outputs

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure/recheck_after_candidate_blocker_closure_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure/candidate_pass_fail_blocked_decision_matrix.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure/forward_aging_handoff_readiness_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure/remaining_candidate_replay_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.md`
- `docs/research/growth_tilt_candidate_recheck_after_candidate_blocker_decision_matrix.md`
- `docs/research/growth_tilt_candidate_forward_aging_handoff_readiness_summary.md`
- `docs/research/growth_tilt_remaining_candidate_replay_blocker_summary.md`
- `docs/research/growth_tilt_candidate_recheck_after_candidate_blocker_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438H_or_2439A_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-after-candidate-blocker-closure --as-of 2026-07-08
```

## Decision Rules

- If any candidate remains `BLOCKED`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_BLOCKED`.
- If no candidate is blocked and at least one candidate is `PASS`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_READY`.
- If no candidate is blocked, no candidate is `PASS`, and all three candidates
  are `FAIL`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_NO_PASSING_CANDIDATE`.
- `NO_PASSING_CANDIDATE` is only valid for pass/fail/blocked counts `0/3/0`.
- `0/0/3` must stay `BLOCKED`.

## Safety Boundary

This task does not enable paper-shadow, paper-shadow schedule, production,
broker, event append, outcome binding, forward-aging observation, trading
advice, signal generation, or portfolio weight mutation. A READY result only
permits the next forward-aging candidate pack rebuild gate; it is not a
paper-shadow candidate finding.

## Acceptance Criteria

- The new CLI runs deterministically and writes all JSON / Markdown artifacts.
- TRADING-2438F READY, replayability handoff, candidate closure records, and the
  three candidate replay records are validated.
- PASS / FAIL / BLOCKED counts are computed from candidate records and evidence.
- Any remaining BLOCKED candidate routes to `TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure`.
- `0/3/0` routes to `TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review`.
- `>=1 PASS` and `0 BLOCKED` routes to `TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_PIT_Replay_Recheck`.
- Registry, artifact catalog, system flow, task register, completed archive, and
  research docs stay aligned.
- Focused tests, Ruff, compileall, cached data validation, docs gates, task
  register consistency, contract validation, and `git diff --check` pass.

## Progress Notes

- 2026-07-10: 根据 owner roadmap 附件新增，进入 `IN_PROGRESS`。当前 2438F 真实输出 READY，`replayability_handoff_ready=true`、candidate-level blocker after count=0，但 3 条 candidate replay output records 仍为 pass/fail/blocked=`0/0/3` 且 metric summary 为空；2438G 必须重新判定并 fail-closed 保留 BLOCKED，而不是误标 no-candidate、FAIL、forward-aging ready 或 paper-shadow candidate。
- 2026-07-10: 实现完成并进入 `DONE`。真实 CLI 输出 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_BLOCKED`，source_2438f_blocker_closure_ready=true、candidate_level_blocker_closure_ready=true、replayability_handoff_ready=true、candidate_replay_outputs_complete=true、candidate_replay_output_record_count=3、pass/fail/blocked=`0/0/3`、remaining_candidate_replay_blocker_count=3、forward_aging_handoff_ready=false。下一跳为 `TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure`；本结论不是 no-passing candidate、forward-aging eligibility、paper-shadow candidate found 或 2440 no-candidate。

## Final Result

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_BLOCKED
```

关键字段：

- prior_status=`GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY`
- source_2438f_blocker_closure_ready=true
- candidate_level_blocker_closure_ready=true
- replayability_handoff_ready=true
- data_quality_status=`PASS_WITH_WARNINGS`
- candidate_replay_outputs_complete=true
- candidate_replay_output_record_count=3
- candidate_replay_pass_count=0
- candidate_replay_fail_count=0
- candidate_replay_blocked_count=3
- remaining_candidate_replay_blocker_count=3
- forward_aging_handoff_ready=false
- forward_aging_candidate_count=0
- registry_catalog_docs_alignment=true
- paper_shadow_candidate_found=false
- paper_shadow_enabled=false
- production_enabled=false
- broker_enabled=false
- generated_trading_advice=false
- portfolio_weight_mutated=false
- next_route=`TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure`
