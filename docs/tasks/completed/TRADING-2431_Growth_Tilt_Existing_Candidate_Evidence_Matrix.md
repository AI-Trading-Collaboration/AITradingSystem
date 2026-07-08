# TRADING-2431 Growth Tilt Existing Candidate Evidence Matrix

## 完成摘要

- task register：`TRADING-2431_GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX`
- status：`DONE`
- CLI：`aits research strategies growth-tilt-existing-candidate-evidence-matrix --as-of 2026-07-08`
- 真实 CLI status：`GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY`
- next route：`TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness`
- 完成日期：2026-07-09

## 输出产物

- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/existing_candidate_evidence_matrix_result.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_evidence_matrix.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_status_summary.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/candidate_metric_coverage.json`
- `outputs/research_strategies/growth_tilt_existing_candidate_evidence_matrix/no_effect_boundary.json`
- `docs/research/growth_tilt_existing_candidate_evidence_matrix.md`
- `docs/research/growth_tilt_existing_candidate_evidence_matrix_table.md`
- `docs/research/growth_tilt_existing_candidate_status_summary.md`
- `docs/research/growth_tilt_existing_candidate_metric_coverage.md`
- `docs/research/growth_tilt_existing_candidate_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2432_route.md`

## 真实运行结果

```text
GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY
source_2430_ready=true
candidate_registry_ready=true
prior_candidate_evidence_ready=true
component_value_evidence_ready=true
existing_candidate_evidence_matrix_ready=true
candidate_status_summary_ready=true
candidate_metric_coverage_ready=true
no_effect_boundary_ready=true
candidate_count=6
required_candidate_group_count=6
rejected_count=0
component_value_count=4
needs_pit_count=2
promotion_candidate_count=0
promotion_candidate_found=false
metric_coverage_available_count=0
metric_coverage_partial_count=6
metric_coverage_missing_count=0
evidence_gap_count=0
engineering_readiness_is_alpha_evidence=false
market_data_experiment_run=false
historical_screen_run=false
pit_replay_run=false
candidate_gauntlet_run=false
manual_review_required=true
automatic_execution_allowed=false
generated_signal=false
new_signal_generated=false
generated_trading_advice=false
trading_advice_generated=false
actionable_allocation_generated=false
outcome_backfilled=false
outcome_binding_executed=false
paper_shadow_enabled=false
paper_shadow_schedule_enabled=false
paper_shadow_daily_job_run=false
scheduler_enabled=false
scheduled_task_created=false
production_enabled=false
broker_enabled=false
broker_order_generated=false
portfolio_weight_mutated=false
daily_report_generated=false
daily_report_run=false
backtest_run=false
scoring_run=false
fresh_market_data_read=false
source_validation_error_count=0
next_route=TRADING-2432_Growth_Tilt_Candidate_Gauntlet_Harness
```

## 安全边界

TRADING-2431 只整理已有候选证据，不运行新的 market-data experiment、
historical screen、PIT replay、candidate gauntlet、backtest 或 scoring。工程
readiness 不等于 alpha evidence；本任务未生成真实 signal、未回填真实 outcome、
未 mutate outcome store、未生成 trading advice、actionable allocation、broker order、
daily report、backtest 或 scoring output；未修改实际组合权重；未启用 paper-shadow、
paper-shadow schedule、scheduler、scheduled task、paper-shadow daily job、production 或
broker/order；未读取 fresh cached market/outcome data。

## Data Quality Gate

未运行 `aits validate-data`。原因：本任务只读取 TRADING-2430 prior artifact、
candidate registry、prior candidate evidence、component value matrix、report registry、
artifact catalog、system flow 和 research docs，不读取 fresh cached market/outcome
data，不运行 historical screen / PIT replay / candidate gauntlet / backtest / scoring /
daily report，不生成 feature/signal，不回填 outcome，也不生成交易建议。

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_existing_candidate_evidence_matrix.py`：PASS，7 passed
- `aits research strategies growth-tilt-existing-candidate-evidence-matrix --as-of 2026-07-08`：PASS，输出 `GROWTH_TILT_EXISTING_CANDIDATE_EVIDENCE_MATRIX_READY`
- `aits docs validate-freshness`：PASS，检查文档数 619，问题数 0
- `aits docs report-contract --latest`：PASS，reports 1328，errors 0，warnings 0
- `aits reports task-register-consistency run`：PASS，active 320，completed 492，checks 13，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts\run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260708T175214Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS，无命中
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning
