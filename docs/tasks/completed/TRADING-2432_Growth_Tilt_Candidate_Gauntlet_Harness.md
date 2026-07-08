# TRADING-2432 Growth Tilt Candidate Gauntlet Harness

最后更新：2026-07-09

## 完成状态

- task register id：`TRADING-2432_GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS`
- status：`DONE`
- commit：待提交
- production_effect：`none`
- broker_action：`none`

## 实现摘要

TRADING-2432 已完成 Growth Tilt batch candidate gauntlet harness contract：

- 新增 `research/configs/growth_tilt/candidate_set_2432.yaml`
- 新增 research-quality builder 和执行 wrapper
- 新增 CLI：

```bash
aits research strategies growth-tilt-candidate-gauntlet --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/candidate_set_2432.yaml
```

- 输出 candidate gauntlet result、candidate set snapshot、baseline contract、
  metric contract、criteria contract、regime-plateau-ablation contract 和
  no-effect boundary artifacts
- 更新 research docs、report registry、artifact catalog、system flow、task register
  和 completed archive

## 真实 CLI 结果

```text
GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY
```

关键字段：

- source_2431_ready=true
- candidate_set_ready=true
- candidate_set_id=`growth_tilt_batch_2432`
- harness_ready=true
- baseline_ready=true
- metrics_ready=true
- kill_criteria_ready=true
- promotion_criteria_ready=true
- regime_slices_ready=true
- parameter_plateau_check_ready=true
- ablation_output_ready=true
- candidate_group_count=6
- candidates_tested=0
- required_metric_count=11
- configured_metric_count=11
- kill_criteria_count=3
- promotion_criteria_count=3
- regime_slice_count=4
- parameter_plateau_dimension_count=5
- ablation_output_count=5
- new_investment_threshold_values_set=false
- threshold_policy_required_for_execution=true
- criteria_threshold_values_all_null=true
- contract_gap_count=0
- candidate_gauntlet_run=false
- candidate_batch_screen_run=false
- backtest_run=false
- scoring_run=false
- fresh_market_data_read=false
- paper_shadow_enabled=false
- production_enabled=false
- broker_enabled=false
- next_route=`TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen`

## Data Quality Gate

本任务未运行 `aits validate-data`。原因：2432 只读取 TRADING-2431 prior artifact、
candidate-set config、report registry、artifact catalog、system flow 和 research docs，
只建立 harness contract，不读取 fresh cached market/outcome data，不运行 candidate
batch screen、historical screen、PIT replay、backtest、scoring 或 daily report，不生成
feature / signal / outcome 或交易建议。

## 验证记录

- `python -m ruff check .`：PASS
- `python -m compileall -q src tests`：PASS
- `python -m pytest -n 16 --dist loadfile tests\research_strategies\test_growth_tilt_candidate_gauntlet_harness.py`：PASS，7 passed
- `aits research strategies growth-tilt-candidate-gauntlet --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/candidate_set_2432.yaml`：PASS，READY
- `aits docs validate-freshness`：PASS，620 docs，0 issues
- `aits docs report-contract --latest`：PASS，1329 reports，0 errors，0 warnings
- `aits reports task-register-consistency run`：PASS，active 320，completed 493，failed 0
- `aits reports task-register-consistency validate --latest`：PASS，checks 5，failed 0，warnings 0
- `python scripts/run_validation_tier.py contract-validation --write-runtime-artifact`：PASS，197 passed；runtime artifact：`outputs/validation_runtime/contract-validation_20260708T180616Z/test_runtime_summary.json`
- task register active DONE/BASELINE_DONE/DROPPED scan：PASS，无命中
- `git diff --check`：PASS，仅 Git CRLF 归一化 warning

## 安全边界确认

2432 未执行 candidate batch screen、historical screen、PIT replay、backtest、scoring、
daily report、signal generation、outcome backfill、trading advice、paper-shadow schedule、
production 或 broker/order path。Harness readiness 不等于 candidate alpha evidence；
后续 TRADING-2433 必须在 governed screen policy 下才可执行具体 batch screen。
