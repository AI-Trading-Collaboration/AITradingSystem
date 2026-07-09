# TRADING-2438C Growth Tilt Top-3 Candidate PIT Replay Recheck

最后更新：2026-07-09

## 完成状态

- task register id：`TRADING-2438C_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK`
- status：`DONE`
- commit：待提交
- production_effect：`none`
- broker_action：`none`

## 实现摘要

TRADING-2438C 已完成 top-3 candidate PIT replay recheck gate。该节点承接
TRADING-2438B blocker closure READY，但不把 closure READY 直接解释为 candidate
replay pass。它独立读取 2438B、2438A 和原 2438 PIT replay artifacts，重新检查
candidate replay evidence 是否完整到可进入 forward-aging handoff。

- 新增 research-quality builder 和执行 wrapper
- 新增 CLI：

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck --as-of 2026-07-08
```

- 真实运行 data-quality gate，并把 gate status 写入输出
- 输出 PIT replay recheck result、candidate replay evidence、candidate replay summary、
  remaining recheck blocker summary、no-effect boundary 和 2439A/2438D route artifacts
- 更新 research docs、report registry、artifact catalog、system flow、task register
  和 completed archive

## 真实 CLI 结果

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
- not_no_candidate_status=true
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
- paper_shadow_schedule_enabled=false
- production_enabled=false
- broker_enabled=false
- automatic_execution_allowed=false
- generated_trading_advice=false
- broker_order_generated=false
- portfolio_weight_mutated=false
- next_route=`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`

## Data Quality Gate

本任务已运行：

```bash
aits validate-data --as-of 2026-07-08
```

结果：

- data quality status：`PASS_WITH_WARNINGS`
- report：`outputs/reports/data_quality_2026-07-08.md`
- audit record：`artifacts/data_refresh_audit/validation/validate_data_2026-07-08_3de166bfd75404a6.json`
- errors：0
- warnings：2
- info：12

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck.py`：PASS，19 passed
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_engine_remediation.py tests\research_strategies\test_growth_tilt_pit_replay_engine_blocker_closure.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck.py tests\research_strategies\test_growth_tilt_forward_aging_candidate_pack.py`：PASS，66 passed
- `aits validate-data --as-of 2026-07-08`：PASS_WITH_WARNINGS，0 errors，2 warnings，12 info
- `aits research strategies growth-tilt-top3-candidate-pit-replay-recheck --as-of 2026-07-08`：PASS，recheck BLOCKED
- `aits docs validate-freshness`：PASS，632 docs，0 issues
- `aits docs report-contract --latest`：PASS，1340 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 506，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260709T134126Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

2438C 的 `DONE` 表示 recheck engine、CLI、artifacts、docs 和 validation 已闭环。
它不表示 candidate replay 通过，也不表示 forward-aging eligibility、paper-shadow
candidate found 或 2440 no-candidate promotion review 结论。

本次真实 recheck 的 blocker 是 candidate replay outputs incomplete：原 2438 PIT
replay rows 仍为 `blocked_replay_engine_gap`，且 `pit_replay_executed=false`。因此
下一步必须进入
`TRADING-2438D_Growth_Tilt_Top3_Candidate_PIT_Replay_Recheck_Blocker_Closure`。

本任务未启用 paper-shadow、schedule、production、broker 或 automatic execution；
未生成 trading advice、broker order、portfolio weight mutation、signal/outcome artifact、
backtest、scoring 或 daily report。
