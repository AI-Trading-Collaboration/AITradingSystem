# TRADING-2435 Growth Tilt Valid-Until Outcome Hit-Rate Study

最后更新：2026-07-09

## 完成状态

- task register id：`TRADING-2435_GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY`
- status：`DONE`
- commit：待提交
- production_effect：`none`
- broker_action：`none`

## 实现摘要

TRADING-2435 已完成 research-only valid-until outcome hit-rate study：

- 新增 research-quality builder 和执行 wrapper
- 新增 CLI：

```bash
aits research strategies growth-tilt-valid-until-outcome-hit-rate-study --as-of 2026-07-08
```

- 输出 hit-rate study result、valid-until hit-rate matrix、stale signal reduction
  summary、expiry failure audit 和 no-effect boundary artifacts
- 更新 research docs、report registry、artifact catalog、system flow、task register
  和 completed archive

## 真实 CLI 结果

```text
GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY
```

关键字段：

- source_2434_ready=true
- source_2418_valid_until_evidence_ready=true
- source_2429_forward_outcome_boundary_ready=true
- candidate_set_valid_until_metric_ready=true
- candidate_set_valid_until_candidate_group_ready=true
- hit_rate_study_ready=true
- valid_until_hit_rate_matrix_ready=true
- stale_signal_reduction_summary_ready=true
- expiry_failure_audit_ready=true
- no_effect_boundary_ready=true
- valid_until_component_value_found=true
- valid_until_hit_rate_delta=0.0
- stale_signal_reduction=0.0
- expiry_failure_count=0
- outcome_sample_count=0
- observed_outcome_hit_rate_available=false
- candidate_status=`component_value`
- computed_new_metrics=false
- market_data_hit_rate_study_run=false
- real_outcome_binding_run=false
- historical_screen_run=false
- pit_replay_run=false
- backtest_run=false
- scoring_run=false
- fresh_market_data_read=false
- fresh_outcome_data_read=false
- outcome_binding_executed=false
- paper_shadow_enabled=false
- production_enabled=false
- broker_enabled=false
- next_route=`TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study`

## Data Quality Gate

本任务未运行 `aits validate-data`。原因：2435 只读取 prior artifacts、
candidate-set config、report registry、artifact catalog、system flow 和 research docs，
只做 prior-artifact / contract-level study；不读取 fresh cached market/outcome data，
不运行 historical screen、PIT replay、backtest、scoring、daily report 或 outcome binding，
不生成 feature / signal / outcome 或交易建议。

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_valid_until_outcome_hit_rate_study.py`：PASS，7 passed
- `aits research strategies growth-tilt-valid-until-outcome-hit-rate-study --as-of 2026-07-08`：PASS，READY
- `aits docs validate-freshness`：PASS，623 docs，0 issues
- `aits docs report-contract --latest`：PASS，1332 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 319，completed 497，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260708T184200Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS，无命中
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

2435 未读取 fresh market/outcome data，未运行 historical screen、PIT replay、backtest、
scoring、daily report、outcome binding、signal generation、outcome backfill、trading
advice、paper-shadow schedule、production 或 broker/order path。`valid_until_hit_rate_delta`
和 `stale_signal_reduction` 的 0.0 值表示本任务未执行真实 outcome hit-rate 计算，不是收益
或 alpha 结论；valid-until 仅被确认具备 lifecycle-quality component value。
