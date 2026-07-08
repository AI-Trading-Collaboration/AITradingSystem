# TRADING-2436 Growth Tilt Turnover Cooldown Parameter Plateau Study

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2436_GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2435 valid-until outcome hit-rate study READY 后，研究 turnover budget
和 cooldown 是否形成稳定参数平台，而不是单点优化。2436 只做 prior-artifact /
contract-level plateau readiness study；不运行真实参数 sweep，不把 config-level
parameter_plateau_check 误报为已发现稳定 plateau。

## 输入

- TRADING-2435 valid-until outcome hit-rate study result
- TRADING-2432 candidate gauntlet harness result
- TRADING-2432 candidate-set config
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/parameter_plateau_study_result.json`
- `outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/parameter_plateau_matrix.json`
- `outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/turnover_cooldown_check_summary.json`
- `outputs/research_strategies/growth_tilt_turnover_cooldown_parameter_plateau_study/no_effect_boundary.json`
- `docs/research/growth_tilt_turnover_cooldown_parameter_plateau_study.md`
- `docs/research/growth_tilt_turnover_cooldown_parameter_plateau_matrix.md`
- `docs/research/growth_tilt_turnover_cooldown_check_summary.md`
- `docs/research/growth_tilt_turnover_cooldown_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2437_route.md`

## CLI

```bash
aits research strategies growth-tilt-turnover-cooldown-parameter-plateau-study --as-of 2026-07-08
```

## 期望状态

```text
GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_READY
```

## 安全边界

本任务不得读取 fresh cached market/outcome data，不得运行 parameter sweep、historical
screen、PIT replay、backtest、scoring、daily report 或 outcome binding，不得生成
signal/outcome/trading advice，不得启用 paper-shadow / schedule / production / broker。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 明确 `parameter_plateau_found`、`isolated_winner`、`robust_region_count`、
  `component_value_found` 和 `candidate_status`。
- 未执行参数 sweep 时不得 silent pass；`parameter_plateau_found` 必须保持 false。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- next route：`TRADING-2437_Growth_Tilt_Regime_Slice_Attribution_Review`。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_turnover_cooldown_parameter_plateau_study.py
aits research strategies growth-tilt-turnover-cooldown-parameter-plateau-study --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并进入 `DONE`。新增 turnover / cooldown parameter plateau study builder、CLI、parameter plateau study result / matrix / check summary / no-effect boundary artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 CLI 输出 `GROWTH_TILT_TURNOVER_COOLDOWN_PARAMETER_PLATEAU_STUDY_READY`，`parameter_plateau_found=false`，`isolated_winner=false`，`robust_region_count=0`，`component_value_found=false`，`candidate_status=needs_pit`，`computed_new_metrics=false`，`parameter_sweep_run=false`，paper-shadow / production / broker 全部 disabled，next route=`TRADING-2437_Growth_Tilt_Regime_Slice_Attribution_Review`。本任务未运行 `aits validate-data`，因为只读取 prior artifacts、candidate-set config、registry/catalog/system flow 和 research docs，不读取 fresh cached market/outcome data、不运行 parameter sweep / historical screen / PIT replay / backtest / scoring / daily report / outcome binding、不生成 feature/signal/outcome 或交易建议。
