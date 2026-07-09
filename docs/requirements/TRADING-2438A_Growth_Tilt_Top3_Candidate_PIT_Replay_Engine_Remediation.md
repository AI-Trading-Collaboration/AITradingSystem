# TRADING-2438A Growth Tilt Top-3 Candidate PIT Replay Engine Remediation

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2438A_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION`
- status：`DONE`
- owner：项目 owner 后续复核 `TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure`
- 创建日期：2026-07-09

## 目标

承接 TRADING-2440 的 `GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE` 结果，回到 TRADING-2438 top-3 candidate PIT replay engine / replay evidence / candidate handoff 层，明确 forward aging gate 被阻断的真实原因，并生成可审计 remediation 输出。

本任务不是 paper-shadow promotion，不启用 paper-shadow，不生成交易建议，也不把 blocked 状态误标为 no-candidate。若 replay engine evidence 仍不完整，必须 fail closed 输出 remediation blocked 和 blocker closure route。

## 输入

- TRADING-2440 promotion review result
- TRADING-2439 forward aging candidate pack result
- TRADING-2438 top-3 candidate PIT replay result
- TRADING-2438 PIT replay evidence and blocker summary
- data quality gate report from `aits validate-data` or the same validation code path
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/remediation_result.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/remediation_evidence.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/before_after_comparison.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/remaining_blocker_summary.json`
- `outputs/research_strategies/growth_tilt_top3_candidate_pit_replay_engine_remediation/no_effect_boundary.json`
- `docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_evidence.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_before_after.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_remaining_blockers.md`
- `docs/research/growth_tilt_top3_candidate_pit_replay_engine_remediation_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438B_route.md`

## CLI

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-engine-remediation --as-of 2026-07-08
```

## 期望 READY 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_READY
```

## Fail-Closed 状态

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED
```

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 能读取 2440 blocked-by-forward-aging-gate artifact，并明确该状态不是 no-candidate。
- 能读取 2439 / 2438 artifacts，并定位 PIT replay engine / input specs / source traceability / as-of / valid-until / outcome linkage / handoff blocker。
- 生成 remediation artifact、before/after comparison、remaining blocker summary、no-effect boundary 和 next route。
- evidence 完整时 `remediation_ready=true`，且 next route 为 `TRADING-2438B_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck`。
- evidence 不完整时不得伪造 READY，且 next route 为 `TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure`。
- paper-shadow、schedule、production、broker、automatic execution、trading advice 和 portfolio mutation 全部保持 disabled / false / none。
- report registry、artifact catalog、system flow、task register 和 completed archive 一致。
- focused tests、Ruff、compileall、data validation、CLI run、docs freshness、documentation contract、task-register consistency、contract validation 和 diff check 通过或明确记录无法通过的原因。

## 进展记录

- 2026-07-09：根据 owner roadmap 附件新增，进入 `IN_PROGRESS`。当前 authoritative artifacts 显示：TRADING-2440 blocked by forward aging gate；TRADING-2439 blocked by PIT replay gate；TRADING-2438 blocked by replay engine gap，且 `candidate_pit_replay_engine_available=false`、candidate replay input / source traceability / as-of / valid-until / outcome linkage specs 均未就绪。
- 2026-07-09：实现完成并归档 `DONE`。新增 remediation builder、CLI、JSON/Markdown artifacts、registry/catalog/system flow wiring 和 focused tests；真实运行 `aits validate-data --as-of 2026-07-08` 后执行 CLI，输出 `GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED`，不是 no-candidate 结论。

## 真实运行结果

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_ENGINE_REMEDIATION_BLOCKED
```

关键字段：

- prior_promotion_review_status：`GROWTH_TILT_PAPER_SHADOW_CANDIDATE_PROMOTION_REVIEW_BLOCKED_BY_FORWARD_AGING_GATE`
- prior_forward_aging_status：`GROWTH_TILT_FORWARD_AGING_CANDIDATE_PACK_BLOCKED_BY_PIT_REPLAY_GATE`
- prior_pit_replay_status：`GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP`
- blocked_by_forward_aging_gate=true
- not_no_candidate_status=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- candidate_selection_resolves=true
- top3_candidate_ids_present=true
- pit_replay_artifacts_present=true
- candidate_pit_replay_engine_available=false
- candidate_replay_input_specs_ready=false
- source_traceability_complete=false
- as_of_boundary_explicit=false
- valid_until_boundary_explicit=false
- outcome_linkage_complete=false
- forward_aging_handoff_ready=false
- registry_catalog_docs_alignment=true
- remediation_ready=false
- remediation_gap_count=8
- unresolved_engine_blocker_count=8
- paper_shadow_enabled=false
- paper_shadow_schedule_enabled=false
- production_enabled=false
- broker_enabled=false
- automatic_execution_allowed=false
- generated_trading_advice=false
- broker_order_generated=false
- portfolio_weight_mutated=false
- next_route=`TRADING-2438B_Growth_Tilt_PIT_Replay_Engine_Blocker_Closure`

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_engine_remediation.py`：PASS，17 passed
- `aits validate-data --as-of 2026-07-08`：PASS_WITH_WARNINGS，0 errors，2 warnings，12 info
- `aits research strategies growth-tilt-top3-candidate-pit-replay-engine-remediation --as-of 2026-07-08`：PASS，fail-closed blocked
- `aits docs validate-freshness`：PASS，630 docs，0 issues
- `aits docs report-contract --latest`：PASS，1338 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 504，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260709T015507Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning
