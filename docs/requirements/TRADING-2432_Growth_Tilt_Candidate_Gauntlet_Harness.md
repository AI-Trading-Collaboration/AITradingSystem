# TRADING-2432 Growth Tilt Candidate Gauntlet Harness

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2432_GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2431 existing candidate evidence matrix READY 后，建立 Growth Tilt
batch candidate gauntlet harness。2432 只定义 candidate-set config、batch runner
contract、统一 baseline、统一 metrics、kill / promotion criteria、parameter plateau、
regime slice 和 ablation output contract，不执行真实 candidate batch screen。

后续 TRADING-2433 才使用该 harness 批量验证 false risk-off / missed upside 策略想法。

## 输入

- TRADING-2431 existing candidate evidence matrix artifact
- `research/configs/growth_tilt/candidate_set_2432.yaml`
- report registry
- artifact catalog
- system flow
- TRADING-2431 research docs

## 输出

- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/candidate_gauntlet_result.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/candidate_set_snapshot.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/gauntlet_baseline_contract.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/gauntlet_metric_contract.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/gauntlet_criteria_contract.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/regime_plateau_ablation_contract.json`
- `outputs/research_strategies/growth_tilt_candidate_gauntlet_harness/no_effect_boundary.json`
- `docs/research/growth_tilt_candidate_gauntlet_harness.md`
- `docs/research/growth_tilt_candidate_set_2432.md`
- `docs/research/growth_tilt_candidate_gauntlet_baseline_contract.md`
- `docs/research/growth_tilt_candidate_gauntlet_metric_contract.md`
- `docs/research/growth_tilt_candidate_gauntlet_criteria_contract.md`
- `docs/research/growth_tilt_candidate_gauntlet_regime_plateau_ablation_contract.md`
- `docs/research/growth_tilt_candidate_gauntlet_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2433_route.md`

## CLI

```bash
aits research strategies growth-tilt-candidate-gauntlet --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/candidate_set_2432.yaml
```

## 期望状态

```text
GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY
```

如果 required prior artifact、candidate-set config、registry、catalog 或 system flow
缺失，则 fail closed：

```text
GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_BLOCKED_BY_CONTRACT_GAPS
```

## Heuristic Governance

2432 只建立 harness contract，不设置新的投资阈值数值。kill / promotion criteria
以 `threshold_source=future_screen_policy_required`、`threshold_value=null` 记录，
要求 TRADING-2433 及后续具体 screen 在独立配置或 policy manifest 中补齐阈值、
owner、rationale、validation evidence 和 review condition 后才能执行 candidate
ranking / promotion 判断。

## 安全边界

本任务不得：

- 运行 `aits validate-data`，除非实现读取 fresh cached market/features/signals/outcome/event data
- 读取 fresh cached market data
- 执行 candidate batch screen、historical screen、PIT replay、backtest、scoring 或 daily report
- 生成新 signal
- 回填真实 outcome
- 生成 trading advice 或 actionable allocation
- 生成 broker order
- 修改实际组合权重
- 启用 paper-shadow、paper-shadow schedule、scheduler、production 或 broker

## Data Quality Gate

默认不运行 `aits validate-data`。原因：TRADING-2432 只读取 TRADING-2431 prior
artifact、candidate-set config、registry、catalog、system flow 和 research docs，
只生成 harness contract，不读取 fresh cached market data，不执行 batch screen、
historical screen、PIT replay、backtest、scoring 或 daily report，不生成 feature /
signal / outcome / trading advice。

如果实现阶段引入 fresh cached market/features/signals/outcome/event data 读取，或
执行 backtest-like / scoring-like comparison，必须重新引入 `aits validate-data` 或
同等代码路径。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- candidate-set config 存在且 candidate_set_id=`growth_tilt_batch_2432`。
- 输出 `candidates_tested=0`，明确 harness ready 不是 batch screen 已执行。
- baseline、metrics、kill criteria、promotion criteria、parameter plateau check、
  regime slice check、ablation output 均有 contract artifact。
- 不设置新的投资阈值数值；后续 screen 必须提供 governed threshold policy。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- next route：`TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen`。
- report registry、artifact catalog、system flow、task register 与 completed archive
  一致。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_candidate_gauntlet_harness.py
aits research strategies growth-tilt-candidate-gauntlet --as-of 2026-07-08 --candidate-set research/configs/growth_tilt/candidate_set_2432.yaml
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
  `GROWTH_TILT_CANDIDATE_GAUNTLET_HARNESS_READY`，source_2431_ready=true，
  candidate_set_ready=true，candidate_set_id=`growth_tilt_batch_2432`，
  harness_ready=true，baseline_ready=true，metrics_ready=true，
  kill_criteria_ready=true，promotion_criteria_ready=true，regime_slices_ready=true，
  parameter_plateau_check_ready=true，ablation_output_ready=true，
  candidate_group_count=6，candidates_tested=0，required_metric_count=11，
  configured_metric_count=11，kill_criteria_count=3，promotion_criteria_count=3，
  regime_slice_count=4，parameter_plateau_dimension_count=5，
  ablation_output_count=5，new_investment_threshold_values_set=false，
  threshold_policy_required_for_execution=true，criteria_threshold_values_all_null=true，
  contract_gap_count=0，candidate_gauntlet_run=false，
  candidate_batch_screen_run=false，backtest_run=false，scoring_run=false，
  fresh_market_data_read=false，paper_shadow_enabled=false，production_enabled=false，
  broker_enabled=false，next route 指向
  `TRADING-2433_Growth_Tilt_False_Risk_Off_Missed_Upside_Batch_Screen`。
  本任务未运行 `aits validate-data`，因为只读取 TRADING-2431 prior artifact、
  candidate-set config、registry、catalog、system flow 和 research docs，不读取
  fresh cached market/outcome data、不运行 candidate batch screen / historical screen /
  PIT replay / backtest / scoring / daily report、不生成 feature / signal / outcome
  或交易建议。
