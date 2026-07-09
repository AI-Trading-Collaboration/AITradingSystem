# TRADING-2438L Growth Tilt Top3 Candidate PIT Replay Recheck After Runtime Remediation

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438L_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION`
- owner route source: TRADING-2438K
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`

TRADING-2438K completed replay runtime materialization and returned
`GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`.
All three top-3 candidates are runtime executable, but their replay outcome has
not been rechecked yet and pass/fail/blocked remains `0/0/3`.

TRADING-2438L performs the independent candidate PIT replay outcome recheck after
runtime remediation. It must distinguish true `PASS`, true `FAIL`, and continued
`BLOCKED` without treating a blocked candidate as a failed candidate.

## Scope

TRADING-2438L reads the TRADING-2438K remediation result, executable replay
readiness handoff, runtime materialization remediation, runtime execution audit
trail, and TRADING-2438D candidate replay output records. It evaluates each
candidate against explicit runtime metric materialization, baseline comparison,
threshold evaluation, traceability, as-of boundary, valid-until policy, outcome
linkage, and handoff evidence.

The builder is evidence-driven. It may use explicit runtime threshold evaluation
status when present. It must not infer PASS or FAIL from missing or null metric
values.

## Outputs

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/recheck_after_runtime_remediation_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/runtime_remediation_after_recheck.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/candidate_pass_fail_blocked_decision_matrix.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/forward_aging_handoff_readiness_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/post_runtime_candidate_replay_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_recheck_after_runtime_remediation.md`
- `docs/research/growth_tilt_runtime_remediation_after_recheck.md`
- `docs/research/growth_tilt_candidate_runtime_recheck_decision_matrix.md`
- `docs/research/growth_tilt_candidate_forward_aging_after_runtime_remediation_handoff_summary.md`
- `docs/research/growth_tilt_post_runtime_candidate_replay_blocker_summary.md`
- `docs/research/growth_tilt_candidate_recheck_after_runtime_remediation_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438M_or_2439A_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-after-runtime-remediation --as-of 2026-07-08
```

## Decision Rules

- If TRADING-2438K is not
  `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`,
  status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED`.
- If runtime executable count is less than 3, executable handoff is not ready, or
  any candidate runtime record is not executable, status is BLOCKED.
- If required runtime metric values, baseline comparison, threshold evaluation,
  source traceability, as-of boundary, valid-until policy, outcome linkage, or
  forward-aging handoff evidence is missing for a candidate, that candidate is
  `BLOCKED`.
- If at least one candidate is `PASS` and no candidate is `BLOCKED`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_READY`
  and only PASS candidates enter the forward-aging handoff summary.
- If all three candidates are explicit `FAIL` and no candidate is `BLOCKED`,
  status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_NO_PASSING_CANDIDATE`.
- If any candidate remains `BLOCKED`, status is
  `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED`.

## Safety Boundary

This task does not continue runtime remediation, rebuild output completeness,
generate a forward-aging candidate pack, run paper-shadow, create a paper-shadow
schedule, run production, trigger broker workflow, generate trading advice, or
mutate portfolio weights.

## Acceptance Criteria

- The new CLI runs deterministically and writes all JSON / Markdown artifacts.
- TRADING-2438K READY, runtime executable count 3, and executable handoff READY
  are validated.
- Each candidate decision record includes runtime execution reference, metric
  summary reference, baseline comparison reference, threshold evaluation
  reference, evidence reference, traceability, boundaries, linkage, pass/fail/
  blocker reason, forward-aging handoff key, and no-effect safety fields.
- `NO_PASSING_CANDIDATE` is emitted only for pass/fail/blocked=`0/3/0`.
- A still-BLOCKED candidate routes to
  `TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_Resolution`.
- Paper-shadow, schedule, production, broker, trading advice, and portfolio
  mutation flags remain disabled / false / none.

## Progress Notes

- 2026-07-10: 根据 owner roadmap 附件新增并进入 `IN_PROGRESS`。实现必须在
  2438K runtime remediation READY 后独立重判 candidate replay outcome；真实
  2438K runtime records 当前 metric values 仍为 null，因此不得把缺失指标误标为
  FAIL 或 NO_PASSING_CANDIDATE。
- 2026-07-10: 实现完成并归档 `DONE`。真实 CLI 已先执行
  `aits validate-data --as-of 2026-07-08`，数据质量状态为
  `PASS_WITH_WARNINGS`、error=0、warning=2、info=12；随后运行 2438L CLI，
  status=`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_RUNTIME_REMEDIATION_BLOCKED`，
  prior_status=`GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`，
  runtime_blocker_count_after=0，candidate_replay_runtime_executable_count=3，
  executable_replay_readiness_handoff_ready=true，candidate_replay_outcome_rechecked=true，
  runtime_metric_materialization_output_ready=false，
  threshold_evaluator_runtime_output_ready=false，pass/fail/blocked=`0/0/3`，
  post_runtime_candidate_replay_blocker_count=3，forward_aging_handoff_ready=false，
  evidence_gap_count=0，next route=`TRADING-2438M_Growth_Tilt_Post_Runtime_Candidate_PIT_Replay_Blocker_Resolution`。
  本任务没有把 runtime metric/threshold 缺口误标为 FAIL 或
  NO_PASSING_CANDIDATE，没有启用 paper-shadow、schedule、production 或 broker，
  没有生成 trading advice 或 portfolio mutation。
