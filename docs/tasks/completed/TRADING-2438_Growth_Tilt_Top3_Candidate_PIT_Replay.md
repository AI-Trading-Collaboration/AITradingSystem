# TRADING-2438 Growth Tilt Top-3 Candidate PIT Replay

最后更新：2026-07-09

## 完成状态

- task register id：`TRADING-2438_GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY`
- status：`DONE`
- commit：待提交
- production_effect：`none`
- broker_action：`none`

## 实现摘要

TRADING-2438 已完成 top-3 candidate PIT replay readiness attempt，并在缺少真实
Growth Tilt candidate-specific PIT replay engine 和 replay input specs 时 fail-closed：

- 新增 research-quality builder 和执行 wrapper
- 新增 CLI：

```bash
aits research strategies growth-tilt-top3-candidate-pit-replay --as-of 2026-07-08
```

- 真实运行 data-quality gate，并把 gate status 写入输出
- 输出 top-3 candidate selection、PIT replay evidence、PIT replay blocker summary
  和 no-effect boundary artifacts
- 更新 research docs、report registry、artifact catalog、system flow、task register
  和 completed archive

## 真实 CLI 结果

```text
GROWTH_TILT_TOP3_CANDIDATE_PIT_REPLAY_BLOCKED_BY_REPLAY_ENGINE_GAP
```

关键字段：

- source_2437_ready=true
- source_2433_batch_screen_ready=true
- source_2431_existing_candidate_evidence_ready=true
- candidate_set_2433_ready=true
- data_quality_gate_executed=true
- data_quality_gate_passed=true
- data_quality_status=`PASS_WITH_WARNINGS`
- pit_candidates_selected=3
- pit_candidates_tested=0
- pit_replay_pass_count=0
- pit_replay_fail_count=0
- pit_replay_blocked_count=3
- promotion_review_candidate_count=0
- candidate_pit_replay_engine_available=false
- candidate_replay_input_specs_ready=false
- candidate_source_traceability_manifests_ready=false
- candidate_as_of_boundary_specs_ready=false
- candidate_valid_until_boundary_specs_ready=false
- candidate_outcome_linkage_specs_ready=false
- source_traceability_verified_count=0
- as_of_boundary_verified_count=0
- valid_until_boundary_verified_count=0
- outcome_linkage_ready_count=0
- pit_replay_run=false
- pit_replay_executed=false
- computed_new_metrics=false
- paper_shadow_enabled=false
- production_enabled=false
- broker_enabled=false
- next_route=`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`

## Data Quality Gate

本任务已运行：

```bash
aits validate-data --as-of 2026-07-08
```

结果：

- data quality status：`PASS_WITH_WARNINGS`
- report：`outputs/reports/data_quality_2026-07-08.md`
- audit record：`artifacts/data_refresh_audit/validation/validate_data_2026-07-08_e4a80d0cc6af51a0.json`
- errors：0
- warnings：2
- info：12

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_top3_candidate_pit_replay.py`：PASS，6 passed
- `aits validate-data --as-of 2026-07-08`：PASS_WITH_WARNINGS，0 errors，2 warnings，12 info
- `aits research strategies growth-tilt-top3-candidate-pit-replay --as-of 2026-07-08`：PASS，blocked by replay engine gap
- `aits docs validate-freshness`：PASS，626 docs，0 issues
- `aits docs report-contract --latest`：PASS，1335 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 500，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260708T191911Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

2438 没有执行真实 candidate PIT replay、historical screen、backtest、scoring、daily
report、outcome binding、signal generation、outcome backfill、trading advice、
paper-shadow schedule、production 或 broker/order path。`pit_candidates_tested=0`、
`pit_replay_pass_count=0` 和 `promotion_review_candidate_count=0` 是 fail-closed
结果，不是候选 alpha 失败或通过结论；进入 forward aging candidate pack 前必须先完成
`TRADING-2438A_Growth_Tilt_Top3_Candidate_PIT_Replay_Engine_Remediation`。
