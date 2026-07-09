# TRADING-2438F Growth Tilt Top-3 Candidate-Level PIT Replay Blocker Closure

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2438F_GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

承接 TRADING-2438E 的
`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS`
结果，逐个关闭 3 个 top-3 candidate 的 candidate-level PIT replay blockers。

当前真实 2438E 输出为 output records 完整、record_count=3、pass/fail/blocked=0/0/3。
因此 2438F 只负责生成 candidate-level blocker closure 和 replayability handoff，
不直接把 candidate 判定为 `PASS` 或 `FAIL`。

## 输入

- TRADING-2438E after-output-closure recheck result
- TRADING-2438E candidate-level blocker summary
- TRADING-2438D candidate replay output records
- data quality gate report from `aits validate-data` or the same validation code path
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/blocker_closure_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/candidate_level_blocker_closure_records.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/candidate_level_before_after_matrix.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/unresolved_candidate_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/replayability_handoff_manifest.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_level_pit_replay_blocker_closure/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_level_pit_replay_blocker_closure.md`
- `docs/research/growth_tilt_candidate_level_pit_replay_blocker_closure_records.md`
- `docs/research/growth_tilt_candidate_level_pit_replay_before_after.md`
- `docs/research/growth_tilt_unresolved_candidate_level_pit_replay_blockers.md`
- `docs/research/growth_tilt_candidate_replayability_handoff_manifest.md`
- `docs/research/growth_tilt_candidate_level_pit_replay_blocker_closure_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438G_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-level-pit-replay-blocker-closure --as-of 2026-07-08
```

## 状态

- `GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY`
- `GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_BLOCKED`

## Next Routes

- all candidate-level blockers closed：`TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure`
- remaining candidate-level blockers：`TRADING-2438G_Growth_Tilt_Remaining_Candidate_Level_PIT_Replay_Blocker_Closure`

## 验收标准

- CLI 可真实运行并输出 deterministic status / next route。
- 能读取 2438E after-output-closure recheck artifact。
- 能读取 3 条 candidate replay output records 和 candidate-level blocker summary。
- 2438E 不是 candidate-level BLOCKED 状态时必须 fail-closed。
- candidate output records 不完整或 record_count 不等于 3 时必须 fail-closed。
- 每个 blocked candidate 必须有 prior blocker reason。
- 每个 candidate closure record 必须有 closure action、closure evidence ref 和 after state。
- 3 个 candidate blocker 全部关闭时输出 READY。
- 仍有 candidate blocker 时输出 BLOCKED，并列出 remaining candidate blocker。
- READY 仍不得把 candidate 标记为 PASS 或 FAIL。
- READY 仍不得宣布 `paper_shadow_candidate_found=true`。
- replayability handoff ready 只表示 2438G 可独立 recheck，不表示 forward-aging handoff ready。
- paper-shadow、schedule、production、broker、automatic execution、trading advice 和 portfolio mutation 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive 一致。
- focused tests、Ruff、compileall、data validation、真实 CLI、docs freshness、documentation contract、task-register consistency、contract validation 和 diff check 通过或明确记录阻塞原因。

## 进展记录

- 2026-07-09：根据 owner roadmap 附件新增，进入 `IN_PROGRESS`。当前 2438E output records 完整且 pass/fail/blocked=0/0/3；2438F 将关闭 candidate-level replayability blockers 并 route 到 2438G recheck，而不判定 replay outcome、不启用 paper-shadow、不重跑 2440。
- 2026-07-09：实现完成并进入 `DONE`。真实 CLI 输出 `GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY`，`source_2438e_candidate_level_blocked=true`、`candidate_replay_outputs_complete=true`、`candidate_replay_output_record_count=3`、`candidate_level_blocker_count_before=3`、`candidate_level_blocker_count_after=0`、`candidate_replayable_after_closure_count=3`、`replayability_handoff_ready=true`、`forward_aging_handoff_ready=false`、pass/fail/blocked=0/0/3。下一跳为 `TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure`；本结论只表示可交给 2438G 独立 recheck，不表示 replay PASS、FAIL、no-candidate、forward-aging eligibility 或 paper-shadow candidate。

## 真实 CLI 结果

```text
GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY
```

关键字段：

- prior_status=`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_OUTPUT_CLOSURE_BLOCKED_BY_CANDIDATE_LEVEL_REPLAY_BLOCKERS`
- source_2438e_candidate_level_blocked=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- candidate_replay_outputs_complete=true
- candidate_replay_output_record_count=3
- candidate_level_blocker_closure_records_complete=true
- candidate_level_blocker_closure_ready=true
- candidate_level_blocker_count_before=3
- candidate_level_blocker_count_after=0
- candidate_replayable_after_closure_count=3
- replayability_handoff_ready=true
- forward_aging_handoff_ready=false
- candidate_replay_pass_count=0
- candidate_replay_fail_count=0
- candidate_replay_blocked_count=3
- top3_candidate_count=3
- each_candidate_has_prior_blocker_reason=true
- each_candidate_has_closure_action=true
- each_candidate_has_closure_evidence_ref=true
- each_candidate_has_after_state=true
- all_candidate_blockers_closed=true
- registry_catalog_docs_alignment=true
- paper_shadow_candidate_found=false
- paper_shadow_enabled=false
- paper_shadow_schedule_enabled=false
- production_enabled=false
- broker_enabled=false
- automatic_execution_allowed=false
- generated_trading_advice=false
- broker_order_generated=false
- portfolio_weight_mutated=false
- next_route=`TRADING-2438G_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Candidate_Blocker_Closure`

## Data Quality Gate

本任务已运行：

```bash
aits validate-data --as-of 2026-07-08
```

结果：

- data quality status：`PASS_WITH_WARNINGS`
- report：`outputs/reports/data_quality_2026-07-08.md`
- audit record：`artifacts/data_refresh_audit/validation/validate_data_2026-07-08_1ba58880b198e2d1.json`
- errors：0
- warnings：2
- info：12

## 安全边界

2438F 的 `READY` 表示 3 个 candidate-level replayability blockers 已闭合，可交给
2438G 独立 recheck。它不表示 candidate replay PASS、FAIL、no-candidate、
forward-aging eligibility、paper-shadow candidate found 或 2440 promotion review 结论。

本任务未补 replay metric，未把 candidate 改为 PASS/FAIL，未启用 forward-aging
handoff、paper-shadow、schedule、production、broker 或 automatic execution；未重跑
2440 promotion review，未生成 trading advice、broker order、portfolio weight
mutation、signal/outcome artifact、backtest、scoring 或 daily report。
