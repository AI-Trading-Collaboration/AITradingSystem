# TRADING-2435 Growth Tilt Valid-Until Outcome Hit-Rate Study

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2435_GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2434 defensive limited adjustment component validation READY 后，研究
valid-until 逻辑是否提升信号生命周期质量和 outcome 可解释性。2435 只做
prior-artifact / contract-level hit-rate study，不回填真实 outcome，不把 0 样本的
delta 当作策略 alpha evidence。

## 输入

- TRADING-2434 component validation result
- TRADING-2418 growth tilt valid-until alignment evidence
- TRADING-2418 stale signal policy evidence
- TRADING-2429 forward outcome binding boundary result
- TRADING-2432 candidate-set config
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/hit_rate_study_result.json`
- `outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/valid_until_hit_rate_matrix.json`
- `outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/stale_signal_reduction_summary.json`
- `outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/expiry_failure_audit.json`
- `outputs/research_strategies/growth_tilt_valid_until_outcome_hit_rate_study/no_effect_boundary.json`
- `docs/research/growth_tilt_valid_until_outcome_hit_rate_study.md`
- `docs/research/growth_tilt_valid_until_hit_rate_matrix.md`
- `docs/research/growth_tilt_valid_until_stale_signal_reduction_summary.md`
- `docs/research/growth_tilt_valid_until_expiry_failure_audit.md`
- `docs/research/growth_tilt_valid_until_outcome_hit_rate_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2436_route.md`

## CLI

```bash
aits research strategies growth-tilt-valid-until-outcome-hit-rate-study --as-of 2026-07-08
```

## 期望状态

```text
GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY
```

## 安全边界

本任务不得读取 fresh cached market/outcome data，不得运行 historical screen、PIT replay、
backtest、scoring、daily report 或 outcome binding，不得生成 signal/outcome/trading advice，
不得启用 paper-shadow / schedule / production / broker。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 明确 `valid_until_component_value_found`、`valid_until_hit_rate_delta`、
  `stale_signal_reduction`、`expiry_failure_count` 和 `candidate_status`。
- 0 样本 / 未计算新指标必须显式披露，不得 silent pass。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- next route：`TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study`。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_valid_until_outcome_hit_rate_study.py
aits research strategies growth-tilt-valid-until-outcome-hit-rate-study --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并进入 `DONE`。新增 valid-until outcome hit-rate study builder、CLI、hit-rate study result / valid-until hit-rate matrix / stale signal reduction summary / expiry failure audit / no-effect boundary artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 CLI 输出 `GROWTH_TILT_VALID_UNTIL_OUTCOME_HIT_RATE_STUDY_READY`，`valid_until_component_value_found=true`，`valid_until_hit_rate_delta=0.0`，`stale_signal_reduction=0.0`，`expiry_failure_count=0`，`outcome_sample_count=0`，`observed_outcome_hit_rate_available=false`，`computed_new_metrics=false`，`candidate_status=component_value`，paper-shadow / production / broker 全部 disabled，next route=`TRADING-2436_Growth_Tilt_Turnover_Cooldown_Parameter_Plateau_Study`。本任务未运行 `aits validate-data`，因为只读取 prior artifacts、candidate-set config、registry/catalog/system flow 和 research docs，不读取 fresh cached market/outcome data、不运行 historical screen / PIT replay / backtest / scoring / daily report / outcome binding、不生成 feature/signal/outcome 或交易建议。
