# TRADING-2433 Growth Tilt False Risk-Off / Missed Upside Batch Screen

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2433_GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2432 candidate gauntlet harness READY 后，建立并运行 research-only
false risk-off / missed upside candidate triage screen。2433 批量筛选减少过早
defensive、growth recovery 恢复过慢、错误 defensive 天数和 missed upside 的候选想法，
但不读取 fresh cached market data、不运行 backtest/scoring/PIT replay，也不把结果当作
paper-shadow promotion evidence。

## 输入

- TRADING-2432 candidate gauntlet harness artifact
- `research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml`
- TRADING-2431 / TRADING-2432 research docs
- report registry
- artifact catalog
- system flow

## 输出

- `outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/batch_screen_result.json`
- `outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/candidate_screen_matrix.json`
- `outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/batch_decision_summary.json`
- `outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/research_question_coverage.json`
- `outputs/research_strategies/growth_tilt_false_risk_off_missed_upside_batch_screen/no_effect_boundary.json`
- `docs/research/growth_tilt_false_risk_off_missed_upside_batch_screen.md`
- `docs/research/growth_tilt_false_risk_off_missed_upside_candidate_screen_matrix.md`
- `docs/research/growth_tilt_false_risk_off_missed_upside_batch_decision_summary.md`
- `docs/research/growth_tilt_false_risk_off_missed_upside_research_question_coverage.md`
- `docs/research/growth_tilt_false_risk_off_missed_upside_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2434_route.md`

## CLI

```bash
aits research strategies growth-tilt-false-risk-off-missed-upside-batch-screen --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml
```

## 期望状态

```text
GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY
```

如果 required harness artifact、candidate-set config、registry、catalog 或 system flow
缺失，则 fail closed：

```text
GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_BLOCKED_BY_SCREEN_CONTRACT_GAPS
```

## 研究问题

- 系统是否过早进入 defensive？
- 系统是否在 growth recovery 时恢复太慢？
- 是否有办法减少错误 defensive 天数？
- 是否能减少 missed upside 而不显著增加 drawdown？

## 输出分类

允许的 batch decision：

- `rejected`
- `component_value`
- `pit_candidate`
- `promotion_candidate`

2433 默认不允许产生 `promotion_candidate`，除非后续有独立 owner-approved evidence
和 governed threshold policy。当前任务预期只输出 component value / PIT candidate
分类与下一跳路线。

## Heuristic Governance

2433 不设置新的投资阈值数值，不计算新的指标排名。候选 config 可声明
`threshold_source=future_pit_or_component_validation_policy_required` 和
`threshold_value=null`；真正的阈值、pass/fail band、promotion gate 必须在后续
component validation / PIT replay 任务中以 reviewed policy manifest 或同等治理记录补齐。

## 安全边界

本任务不得：

- 运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/outcome/event data
- 读取 fresh cached market data
- 运行 historical screen、PIT replay、backtest、scoring 或 daily report
- 生成新 signal
- 回填真实 outcome
- 生成 trading advice 或 actionable allocation
- 生成 broker order
- 修改实际组合权重
- 启用 paper-shadow、paper-shadow schedule、scheduler、production 或 broker

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2433 只读取 TRADING-2432 harness
artifact、candidate-set config、prior research docs、registry、catalog 和 system flow，
并按配置做 research-only candidate triage；不读取 fresh cached market data、不运行
historical screen、PIT replay、backtest、scoring 或 daily report，不生成 feature /
signal / outcome / trading advice。

如果实现阶段引入 fresh cached market/features/signals/outcome/event data 读取，或
执行 backtest-like / scoring-like comparison，必须重新引入 `aits validate-data` 或
同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- candidate-set config 存在且 candidate_set_id=`false_risk_off_missed_upside_2433`。
- 输出 research question coverage。
- 输出 batch_decision summary，含 rejected/component_value/pit_candidate/promotion_candidate count。
- 默认真实 run 不产生 promotion candidate。
- 明确不运行 market-data / PIT / backtest / scoring path。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- next route：`TRADING-2434_Defensive_Limited_Adjustment_Component_Validation`。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_false_risk_off_missed_upside_batch_screen.py
aits research strategies growth-tilt-false-risk-off-missed-upside-batch-screen --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/false_risk_off_missed_upside_2433.yaml
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并归档 `DONE`。真实 CLI run 输出
  `GROWTH_TILT_FALSE_RISK_OFF_MISSED_UPSIDE_BATCH_SCREEN_READY`，
  source_2432_ready=true，candidate_set_ready=true，
  candidate_set_id=`false_risk_off_missed_upside_2433`，batch_screen_ready=true，
  candidate_screen_matrix_ready=true，batch_decision_summary_ready=true，
  research_question_coverage_ready=true，no_effect_boundary_ready=true，
  candidate_count=6，candidates_screened=6，rejected_count=0，
  component_value_count=3，pit_candidate_count=3，promotion_candidate_count=0，
  promotion_candidate_found=false，research_question_count=4，
  research_question_covered_count=4，new_investment_threshold_values_set=false，
  threshold_policy_required_for_pit_or_promotion=true，
  criteria_threshold_values_all_null=true，computed_new_metrics=false，
  screen_contract_gap_count=0，candidate_batch_screen_run=true，
  market_data_candidate_screen_run=false，historical_screen_run=false，
  pit_replay_run=false，backtest_run=false，scoring_run=false，
  fresh_market_data_read=false，paper_shadow_enabled=false，production_enabled=false，
  broker_enabled=false，next route 指向
  `TRADING-2434_Defensive_Limited_Adjustment_Component_Validation`。
  本任务未运行 `aits validate-data`，因为只读取 TRADING-2432 prior artifact、
  candidate-set config、registry、catalog、system flow 和 research docs，不读取
  fresh cached market/outcome data、不运行 historical screen / PIT replay / backtest /
  scoring / daily report、不生成 feature / signal / outcome 或交易建议。
