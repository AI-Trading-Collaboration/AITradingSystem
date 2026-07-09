# TRADING-2438D Growth Tilt Top-3 Candidate PIT Replay Recheck Blocker Closure

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2438D_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

承接 TRADING-2438C 的
`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED` 结果，关闭
`candidate_replay_outputs_complete=false` 暴露出的输出完整性 blocker。2438D 的目标
是为 3 个 top-3 candidate 生成结构化 replay output record，让后续 2438E 可以
独立判断每个 candidate 是 `PASS`、`FAIL` 还是仍然 `BLOCKED`。

本任务不是继续修 PIT replay engine contract，也不是保证 candidate pass。当前真实
证据仍显示原 2438 replay rows 为 `blocked_replay_engine_gap`，因此 2438D 可以在
output record 层闭合完整性，同时让 candidate status 保持 `BLOCKED`。

## 输入

- TRADING-2438C recheck blocked result
- TRADING-2438B blocker closure result
- TRADING-2438 top-3 candidate PIT replay result
- TRADING-2438 PIT replay evidence and blocker summary
- data quality gate report from `aits validate-data` or the same validation code path
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/blocker_closure_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/candidate_replay_output_records.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/output_completeness_closure.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/before_after_matrix.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/remaining_output_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.md`
- `docs/research/growth_tilt_candidate_replay_output_records.md`
- `docs/research/growth_tilt_candidate_replay_output_completeness_closure.md`
- `docs/research/growth_tilt_candidate_replay_output_before_after.md`
- `docs/research/growth_tilt_candidate_replay_output_remaining_blockers.md`
- `docs/research/growth_tilt_candidate_replay_output_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438E_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure --as-of 2026-07-08
```

## READY 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY
```

`READY` 只表示 candidate replay output records 已完整，允许后续 2438E 重新
判定 pass/fail/blocked counts。它不表示 pass_count > 0，不表示 paper-shadow
candidate found，也不表示 forward-aging candidate pack 已生成。

## Fail-Closed 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_BLOCKED
```

若 2438C blocked artifact 不匹配、2438B closure 不 ready、top-3 ids 不完整、
candidate output record 少于 3 条，或任一 output record 缺少 replay status、
status reason、input spec ref、source traceability ref、evidence ref、as-of、
valid-until policy ref、outcome linkage key、forward-aging handoff key，则必须
fail-closed。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 能读取 2438C blocked artifact，并确认 blocker 是 `candidate_replay_outputs_complete=false`。
- 能读取 2438B closure READY artifact。
- 3 个 top-3 candidate output record 全部生成。
- 每个 candidate 均有明确 `PASS` / `FAIL` / `BLOCKED` 状态。
- 每个 candidate 均有明确 status reason。
- output completeness closure、before / after matrix、remaining output blocker summary 和 no-effect boundary 均生成。
- output 完整时 status 为 READY，并 route 到 `TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Output_Closure`。
- output 仍不完整时 status 为 BLOCKED，并 route 到 `TRADING-2438E_Growth_Tilt_Candidate_Replay_Output_Remaining_Blocker_Closure`。
- READY 状态允许 candidate_replay_pass_count=0；不得把 0 pass 误标为 2440 no-candidate promotion review。
- paper-shadow、schedule、production、broker、automatic execution、trading advice 和 portfolio mutation 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive 一致。
- focused tests、Ruff、compileall、data validation、CLI run、docs freshness、documentation contract、task-register consistency、contract validation 和 diff check 通过或明确记录无法通过的原因。

## 进展记录

- 2026-07-09：根据 owner roadmap 附件新增，进入 `IN_PROGRESS`。当前 2438C 已确认 2438B closure READY，但 candidate replay outputs incomplete；2438D 将关闭 output record completeness，不伪造 replay pass。
- 2026-07-09：实现完成并进入 `DONE`。真实 CLI 输出 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY`，`candidate_replay_outputs_complete=true`、`candidate_replay_output_record_count=3`，但 3 条 candidate record 均为 `BLOCKED`，`candidate_replay_pass_count=0`、`candidate_replay_fail_count=0`、`candidate_replay_blocked_count=3`。下一跳为 `TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Output_Closure`；本结论不表示 candidate pass、paper-shadow candidate found、forward-aging eligibility 或 2440 no-candidate。

## 真实 CLI 结果

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure --as-of 2026-07-08
```

关键字段：

- status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKER_CLOSURE_READY`
- prior_status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_BLOCKED`
- prior_candidate_replay_outputs_complete=false
- source_2438c_recheck_blocked=true
- source_2438b_blocker_closure_ready=true
- source_2438_pit_replay_artifact_resolves=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- candidate_replay_outputs_complete=true
- candidate_replay_output_record_count=3
- top3_candidate_ids_present=true
- each_candidate_has_replay_status=true
- each_candidate_has_status_reason=true
- each_candidate_has_input_spec_ref=true
- each_candidate_has_source_traceability_ref=true
- each_candidate_has_evidence_ref=true
- each_candidate_has_as_of_boundary=true
- each_candidate_has_valid_until_policy_ref=true
- each_candidate_has_outcome_linkage_key=true
- each_candidate_has_forward_aging_handoff_key=true
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
- next_route=`TRADING-2438E_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_After_Output_Closure`

## Data Quality Gate

```bash
aits validate-data --as-of 2026-07-08
```

结果：

- data quality status：`PASS_WITH_WARNINGS`
- report：`outputs/reports/data_quality_2026-07-08.md`
- audit record：`artifacts/data_refresh_audit/validation/validate_data_2026-07-08_406369803f103230.json`
- errors：0
- warnings：2
- info：12

## 验证记录

- `python -m ruff check src\ai_trading_system\dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py src\ai_trading_system\research_quality\growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py src\ai_trading_system\cli_commands\research_execution_growth_tilt.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py`：PASS，20 passed
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_pit_replay_engine_blocker_closure.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py`：PASS，56 passed
- `aits validate-data --as-of 2026-07-08`：PASS_WITH_WARNINGS，0 errors，2 warnings，12 info
- `aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-blocker-closure --as-of 2026-07-08`：PASS，READY，record_count=3，pass/fail/blocked=0/0/3
- `aits docs validate-freshness`：PASS，633 docs，0 issues
- `aits docs report-contract --latest`：PASS，1341 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 507，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260709T141838Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS，无匹配
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

TRADING-2438D 的 `DONE` 只表示 output record completeness blocker 已闭合。它不表示 replay pass、forward-aging eligibility、paper-shadow candidate found、production readiness 或 2440 no-candidate 结论。

本任务未启用 paper-shadow、schedule、production、broker 或 automatic execution；未生成 trading advice、broker order、portfolio weight mutation、signal/outcome artifact、backtest、scoring 或 daily report。
