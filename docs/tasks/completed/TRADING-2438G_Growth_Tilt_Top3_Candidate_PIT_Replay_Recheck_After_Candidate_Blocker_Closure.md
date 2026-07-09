# TRADING-2438G Growth Tilt Top-3 Candidate PIT Replay Recheck After Candidate Blocker Closure

最后更新：2026-07-10

## 完成状态

- task register id：`TRADING-2438G_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE`
- status：`DONE`
- commit：待提交
- production_effect：`none`
- broker_action：`none`

## 实现摘要

TRADING-2438G 已完成 top-3 candidate PIT replay recheck after candidate blocker
closure gate。该节点承接 TRADING-2438F replayability handoff READY，读取
2438F blocker closure result、replayability handoff manifest、candidate-level
closure records 和 2438D candidate replay output records，重新判定 3 个
candidate 的 `PASS` / `FAIL` / `BLOCKED`。

当前真实 2438F READY 只表示 candidate-level replayability blockers 已闭合；
2438D candidate records 仍为 `0/0/3` 且 metric summary 为空。因此 2438G
fail-closed 输出 BLOCKED，并 route 到 2438H remaining candidate PIT replay
blocker closure。

- 新增 research-quality builder 和执行 wrapper
- 新增 CLI：

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-after-candidate-blocker-closure --as-of 2026-07-08
```

- 真实运行 data-quality gate，并把 gate status 写入输出
- 输出 recheck result、candidate pass/fail/blocked decision matrix、
  forward-aging handoff readiness summary、remaining candidate replay blocker
  summary、no-effect boundary 和 2438H/2439A route artifacts
- 更新 research docs、report registry、artifact catalog、system flow、task
  register 和 completed archive

## 真实 CLI 结果

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_RECHECK_AFTER_CANDIDATE_BLOCKER_CLOSURE_BLOCKED
```

关键字段：

- prior_status=`GROWTH_TILT_TOP3_CANDIDATE_LEVEL_PIT_REPLAY_BLOCKER_CLOSURE_READY`
- source_2438f_blocker_closure_ready=true
- candidate_level_blocker_closure_ready=true
- replayability_handoff_ready=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- candidate_replay_outputs_complete=true
- candidate_replay_output_record_count=3
- candidate_records_recheckable_after_candidate_blocker_closure=true
- pit_replay_recheck_after_candidate_blocker_closure_complete=false
- candidate_replay_pass_count=0
- candidate_replay_fail_count=0
- candidate_replay_blocked_count=3
- remaining_candidate_replay_blocker_count=3
- forward_aging_handoff_ready=false
- forward_aging_candidate_count=0
- top3_candidate_count=3
- handoff_candidate_count=3
- candidate_level_closure_record_count=3
- each_candidate_has_replay_status=true
- each_candidate_has_status_reason=true
- pass_fail_blocked_counts_consistent=true
- blocked_candidates_have_blocker_reason=true
- pass_candidates_have_forward_aging_handoff_key=true
- forward_aging_handoff_pass_only=true
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
- source_validation_error_count=0
- next_route=`TRADING-2438H_Growth_Tilt_Remaining_Candidate_PIT_Replay_Blocker_Closure`

## Data Quality Gate

本任务已运行：

```bash
aits validate-data --as-of 2026-07-08
```

结果：

- data quality status：`PASS_WITH_WARNINGS`
- report：`outputs/reports/data_quality_2026-07-08.md`
- audit record：`artifacts/data_refresh_audit/validation/validate_data_2026-07-08_edb3cf48ed59a118.json`
- errors：0
- warnings：2
- info：12

## 验证记录

- `python -m ruff check src\ai_trading_system\research_quality\growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.py src\ai_trading_system\dynamic_strategy_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.py src\ai_trading_system\cli_commands\research_execution_growth_tilt.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.py`：PASS
- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.py`：PASS，16 passed
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_blocker_closure.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_after_output_closure.py tests\research_strategies\test_growth_tilt_top3_candidate_level_pit_replay_blocker_closure.py tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay_recheck_after_candidate_blocker_closure.py`：PASS，88 passed
- `aits validate-data --as-of 2026-07-08`：PASS_WITH_WARNINGS，0 errors，2 warnings，12 info
- `aits research strategies growth-tilt-top3-candidate-pit-replay-recheck-after-candidate-blocker-closure --as-of 2026-07-08`：PASS，BLOCKED，pass/fail/blocked=0/0/3
- `aits docs validate-freshness`：PASS，636 docs，0 issues
- `aits docs report-contract --latest`：PASS，1344 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 510，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260709T154323Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS，无匹配
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

2438G 的 `BLOCKED` 表示 replayability blocker closure 后仍无法把 3 个 candidate
明确判定为 PASS 或 FAIL；真实 pass/fail/blocked 仍为 0/0/3，remaining candidate
replay blocker count=3。

本任务未把 BLOCKED candidate 误标为 FAIL、NO_PASSING_CANDIDATE、forward-aging
ready、paper-shadow candidate found 或 2440 no-candidate；未启用 paper-shadow、
schedule、production、broker 或 automatic execution；未生成 trading advice、broker
order、portfolio weight mutation、signal/outcome artifact、backtest、scoring 或 daily
report。
