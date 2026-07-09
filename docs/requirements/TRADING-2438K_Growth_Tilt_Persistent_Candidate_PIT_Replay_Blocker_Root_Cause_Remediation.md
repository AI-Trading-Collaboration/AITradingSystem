# TRADING-2438K Growth Tilt Persistent Candidate PIT Replay Blocker Root Cause Remediation

最后更新：2026-07-10

## Context

- task register id: `TRADING-2438K_GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION`
- owner route source: TRADING-2438J
- default as-of: `2026-07-08`
- market regime: `ai_after_chatgpt`

TRADING-2438J completed persistent blocker escalation and returned
`GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY`.
All three current top-3 candidates still have pass/fail/blocked=`0/0/3`, and
all three have primary root cause
`replay_engine_contract_ready_but_runtime_not_executable`.

TRADING-2438K remediates the runtime materialization layer so TRADING-2438L can
perform an independent candidate replay outcome recheck. This task must not
convert any candidate to `PASS` or `FAIL`.

## Scope

TRADING-2438K reads the TRADING-2438J escalation artifact, TRADING-2438J
root-cause matrix, TRADING-2438I blocked recheck output, TRADING-2438H,
TRADING-2438F, TRADING-2438D, and TRADING-2438B closure artifacts, plus the
candidate replay output records. It materializes the candidate spec to runtime
input adapter, replay runtime entrypoint shell, replay window, baseline
comparison adapter, metric materialization adapter, pass/fail threshold evaluator
shell, runtime bindings, deterministic smoke checks, and execution audit trail.

This task only verifies runtime executability. It does not execute the 2438L
replay outcome recheck.

## Outputs

- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/root_cause_remediation_result.json`
- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/runtime_materialization_remediation.json`
- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/runtime_before_after_matrix.json`
- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/executable_replay_readiness_handoff.json`
- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/remaining_runtime_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation/runtime_execution_audit_trail.json`
- `docs/research/growth_tilt_persistent_candidate_pit_replay_blocker_root_cause_remediation.md`
- `docs/research/growth_tilt_candidate_replay_runtime_materialization.md`
- `docs/research/growth_tilt_runtime_before_after_matrix.md`
- `docs/research/growth_tilt_executable_replay_readiness_handoff.md`
- `docs/research/growth_tilt_remaining_replay_runtime_blockers.md`
- `docs/research/growth_tilt_runtime_execution_audit_trail.md`
- `docs/research/dynamic_strategy_2438L_route.md`

## CLI

```bash
aits research strategies growth-tilt-persistent-candidate-pit-replay-blocker-root-cause-remediation --as-of 2026-07-08
```

## Decision Rules

- If TRADING-2438J is not
  `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ESCALATION_READY`, status
  is
  `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_BLOCKED`.
- If any candidate root cause is not
  `replay_engine_contract_ready_but_runtime_not_executable`, remediation is
  blocked because this runtime remediation route is not applicable.
- If any required runtime materialization item is missing, remediation is
  blocked and the remaining runtime blocker summary must identify the candidate
  and required next action.
- If all three candidates have runtime input materialized, replay window
  materialized, baseline comparison materialized, metric materialization ready,
  threshold evaluator ready, runtime bindings ready, and smoke check `PASS`,
  status is
  `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`.
- `ROOT_CAUSE_REMEDIATION_READY` means runtime materialization is ready only. It
  does not mean replay outcome ready, candidate pass/fail, forward-aging ready,
  paper-shadow candidate found, production ready, or broker ready.

## Runtime Materialization Requirements

- `candidate_spec_to_runtime_input_adapter_ready`
- `replay_runtime_entrypoint_ready`
- `replay_window_materialization_ready`
- `baseline_comparison_runtime_ready`
- `metric_materialization_runtime_ready`
- `pass_fail_threshold_evaluator_ready`
- `source_traceability_runtime_bindings_ready`
- `as_of_boundary_enforced_at_runtime`
- `valid_until_policy_bound_at_runtime`
- `outcome_linkage_key_runtime_bound`
- `forward_aging_handoff_key_runtime_bound`
- `execution_audit_trail_ready`
- `deterministic_runtime_output_supported`

## Safety Boundary

This task does not run a fresh candidate replay outcome recheck, backtest,
scoring job, daily report, paper-shadow job, paper-shadow schedule, broker
workflow, production workflow, outcome binding, signal generation, trading advice
generation, or portfolio weight mutation.

## Acceptance Criteria

- The new CLI runs deterministically and writes all JSON / Markdown artifacts.
- TRADING-2438J READY and root cause
  `replay_engine_contract_ready_but_runtime_not_executable` are validated.
- TRADING-2438I/H/F/D/B closure history is validated.
- Each candidate runtime remediation record includes runtime input, replay
  window, baseline comparison, metric materialization, threshold evaluator,
  runtime bindings, smoke check, remaining blocker, `NOT_RECHECKED`, and safety
  fields.
- Runtime materialization remediation, before/after matrix, executable replay
  readiness handoff, remaining runtime blocker summary, audit trail, registry,
  catalog, system flow, task register, completed archive, and research docs stay
  aligned.
- Paper-shadow, schedule, production, broker, trading advice, and portfolio
  mutation flags remain disabled / false / none.

## Progress Notes

- 2026-07-10: 根据 owner roadmap 附件新增并进入 `IN_PROGRESS`。实现必须修复
  2438J 定位的 runtime materialization root cause，并输出 2438L 独立 replay
  recheck handoff；不得在 2438K 直接判定 PASS/FAIL，不得进入 forward-aging、
  paper-shadow、production 或 broker。
- 2026-07-10: 实现完成并进入 `DONE` 归档。新增 2438K builder、wrapper、CLI、
  registry/catalog/system flow、research docs 和 19 项 focused tests；真实 CLI
  返回
  `GROWTH_TILT_PERSISTENT_CANDIDATE_PIT_REPLAY_BLOCKER_ROOT_CAUSE_REMEDIATION_READY`，
  runtime blocker count 从 3 降为 0，3 个 candidate runtime smoke check 均
  PASS，`candidate_replay_outcome_rechecked=false`，pass/fail/blocked 仍为
  `0/0/3`，forward-aging、paper-shadow、production、broker、trading advice 和
  portfolio mutation 均保持 disabled / false / none。下一跳为
  `TRADING-2438L_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Runtime_Remediation`。
