# TRADING-2438C Growth Tilt Top-3 Candidate PIT Replay Recheck

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2438C_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

承接 TRADING-2438B 的
`GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY` 结果，对 Growth Tilt top-3
candidate PIT replay evidence 进行独立 recheck，判断 blocker closure 后 candidate
replay outputs 是否真正完整、可追溯、可进入 forward-aging candidate pack。

本任务不启用 paper-shadow，不生成交易建议，不运行 production / broker，不直接宣布
paper-shadow candidate found，也不把无 passing candidate 误标为 2440 promotion
review 的 no-candidate 结论。

## 输入

- TRADING-2438B blocker closure result
- TRADING-2438A remediation result
- TRADING-2438 top-3 candidate PIT replay result
- TRADING-2438 PIT replay evidence and blocker summary
- data quality gate report from `aits validate-data` or the same validation code path
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/pit_replay_recheck_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/candidate_replay_evidence.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/candidate_replay_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/remaining_recheck_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_recheck.md`
- `docs/research/growth_tilt_top3_candidate_replay_evidence.md`
- `docs/research/growth_tilt_top3_candidate_replay_summary.md`
- `docs/research/growth_tilt_top3_candidate_recheck_remaining_blockers.md`
- `docs/research/growth_tilt_top3_candidate_recheck_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2439A_or_2438D_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck --as-of 2026-07-08
```

## READY 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_READY
```

## Fail-Closed 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED
```

## 无 Passing Candidate 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_NO_PASSING_CANDIDATE
```

`NO_PASSING_CANDIDATE` 只代表 top-3 candidate PIT replay 层没有候选通过，不等于
2440 paper-shadow promotion review 的 no-candidate 结论。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED / NO_PASSING_CANDIDATE 状态。
- 能读取 2438B closure artifact，并确认 8 项 closure readiness 仍为 ready。
- 能读取 2438A / 2438 artifacts，并重新复核 top-3 candidate selection、PIT replay evidence、candidate pass/fail/blocked count、source traceability、as-of boundary、valid-until boundary、outcome linkage 和 forward-aging handoff。
- replay evidence 完整且至少 1 个 candidate pass 时，next route 为 `TRADING-2439A_Growth_Tilt_Forward_Aging_Candidate_Pack_Rebuild_After_PIT_Replay_Recheck`。
- replay evidence 完整且 0 个 candidate pass 时，next route 为 `TRADING-2439A_Growth_Tilt_No_Passing_PIT_Candidate_Evidence_Review`。
- replay evidence 不完整时不得伪造 READY 或 NO_PASSING_CANDIDATE，next route 为 `TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`。
- paper-shadow、schedule、production、broker、automatic execution、trading advice 和 portfolio mutation 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive 一致。
- focused tests、Ruff、compileall、data validation、CLI run、docs freshness、documentation contract、task-register consistency、contract validation 和 diff check 通过或明确记录无法通过的原因。

## 进展记录

- 2026-07-09：根据 owner roadmap 附件新增，进入 `IN_PROGRESS`。当前 authoritative 2438B artifact 为 blocker closure READY；原 2438 candidate replay rows 仍是 `blocked_replay_engine_gap`，因此实现必须独立检查 replay output completeness，不能把 2438B closure READY 直接当作 candidate replay pass。
- 2026-07-09：实现完成并归档 `DONE`。新增 2438C builder、wrapper、CLI、research docs、report registry、artifact catalog、system flow 和 focused tests；真实 CLI 读取当前 2438B/2438A/2438 artifacts 后输出 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED`，`source_2438b_blocker_closure_ready=true`，8 项 closure readiness 均为 true，但 `candidate_replay_outputs_complete=false`、pass/fail/blocked=`0/0/3`，next route=`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`。该 DONE 是 2438C recheck flow 完成，不是 candidate replay pass、forward-aging eligibility、paper-shadow candidate 或 2440 no-candidate 结论。

## 实际运行结果

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED
```

关键字段：

- prior_blocker_closure_status=`GROWTH_TILT_PIT_REPLAY_ENGINE_BLOCKER_CLOSURE_READY`
- prior_remediation_status=`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED`
- prior_pit_replay_status=`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`
- source_2438b_blocker_closure_ready=true
- source_2438a_remediation_blocked=true
- source_2438_pit_replay_blocked=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- pit_replay_recheck_ready=false
- pit_replay_engine_ready=true
- input_specs_ready=true
- evidence_completeness_ready=true
- source_traceability_ready=true
- as_of_boundary_ready=true
- valid_until_boundary_ready=true
- outcome_linkage_ready=true
- forward_aging_handoff_ready=true
- top3_candidate_selection_resolves=true
- pit_replay_evidence_exists=true
- candidate_replay_outputs_complete=false
- top3_candidate_count=3
- candidate_replay_pass_count=0
- candidate_replay_fail_count=0
- candidate_replay_blocked_count=3
- registry_catalog_docs_alignment=true
- paper_shadow_candidate_found=false
- paper_shadow_enabled=false
- production_enabled=false
- broker_enabled=false
- automatic_execution_allowed=false
- generated_trading_advice=false
- broker_order_generated=false
- portfolio_weight_mutated=false
- next route=`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`
