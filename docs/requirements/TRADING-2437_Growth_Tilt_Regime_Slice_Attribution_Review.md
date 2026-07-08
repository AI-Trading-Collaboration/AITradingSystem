# TRADING-2437 Growth Tilt Regime Slice Attribution Review

最后更新：2026-07-09

## 状态

- task register id：`TRADING-2437_GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW`
- status：`DONE`
- owner：系统实现 + 项目 owner 后续复核
- 创建日期：2026-07-09

## 目标

在 TRADING-2436 turnover / cooldown parameter plateau study READY 后，对 Growth
Tilt candidate-set 的 regime slice attribution contract 进行复核，建立推荐
regime slices 的归因矩阵和 no-effect 边界。2437 只做 prior-artifact /
contract-level attribution review；不运行真实 regime attribution，不把
candidate-set 的 slice contract coverage 误报为已证明某个 regime 有 alpha。

## 输入

- TRADING-2436 turnover / cooldown parameter plateau study result
- TRADING-2432 candidate gauntlet harness result
- TRADING-2432 candidate-set config
- report registry
- artifact catalog
- system flow
- research docs

## 输出

- `outputs/research_strategies/growth_tilt_regime_slice_attribution_review/regime_slice_attribution_review_result.json`
- `outputs/research_strategies/growth_tilt_regime_slice_attribution_review/regime_slice_attribution_matrix.json`
- `outputs/research_strategies/growth_tilt_regime_slice_attribution_review/candidate_status_by_regime.json`
- `outputs/research_strategies/growth_tilt_regime_slice_attribution_review/no_effect_boundary.json`
- `docs/research/growth_tilt_regime_slice_attribution_review.md`
- `docs/research/growth_tilt_regime_slice_attribution_matrix.md`
- `docs/research/growth_tilt_candidate_status_by_regime.md`
- `docs/research/growth_tilt_regime_slice_no_effect_boundary.md`
- `docs/research/dynamic_strategy_2438_route.md`

## CLI

```bash
aits research strategies growth-tilt-regime-slice-attribution-review --as-of 2026-07-08
```

## 期望状态

```text
GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY
```

## 推荐 regime slices

- `growth_bull`
- `growth_drawdown`
- `rate_shock`
- `volatility_spike`
- `liquidity_stress`
- `post_drawdown_recovery`
- `sideways_chop`
- `semiconductor_leadership`
- `mega_cap_concentration`

## 安全边界

本任务不得读取 fresh cached market/outcome data，不得运行真实 regime attribution、
parameter sweep、historical screen、PIT replay、backtest、scoring、daily report 或
outcome binding，不得生成 signal/outcome/trading advice，不得启用 paper-shadow /
schedule / production / broker。

## 验收标准

- CLI 可真实运行并输出 deterministic READY / BLOCKED 状态。
- 明确 `regime_robustness_score`、`single_regime_dependency_detected`、
  `candidate_status_by_regime`、source/candidate-set coverage 和 next route。
- 未执行真实 attribution 时，不得 silent pass；所有推荐 regime slice status 必须保持
  `inconclusive`。
- paper-shadow、schedule、production、broker、automatic execution 均保持 disabled。
- next route：`TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay`。
- focused tests、Ruff、compileall、docs freshness、documentation contract、task
  register consistency、contract validation 和 diff check 通过。

## 验证计划

```bash
python -m ruff check .
python -m compileall -q src tests
python -m pytest -n 16 --dist loadfile tests/research_strategies/test_growth_tilt_regime_slice_attribution_review.py
aits research strategies growth-tilt-regime-slice-attribution-review --as-of 2026-07-08
aits docs validate-freshness
aits docs report-contract --latest
aits reports task-register-consistency run
aits reports task-register-consistency validate --latest
python scripts/run_validation_tier.py contract-validation --write-runtime-artifact
git diff --check
```

## 进展记录

- 2026-07-09：根据 owner 2426-2440 roadmap 新增，进入 `IN_PROGRESS`。
- 2026-07-09：实现完成并进入 `DONE`。新增 regime slice attribution review builder、CLI、regime slice attribution review result / matrix / candidate status by regime / no-effect boundary artifacts、research docs、registry、catalog、system flow 和 focused tests；真实 CLI 输出 `GROWTH_TILT_REGIME_SLICE_ATTRIBUTION_REVIEW_READY`，source_2436_ready=true、source_2432_gauntlet_ready=true、candidate_set_regime_slice_contract_ready=true、candidate_set_required_metrics_ready=true、recommended_regime_slice_count=9、candidate_set_regime_slice_count=4、regime_robustness_score=0.0、single_regime_dependency_detected=false、single_regime_dependency_assessed=false、regime_pass_count=0、regime_fail_count=0、regime_inconclusive_count=9、all_recommended_regime_status_inconclusive=true、component_value_found=false、candidate_status=needs_pit、computed_new_metrics=false、regime_attribution_run=false，paper-shadow / production / broker 全部 disabled，next route=`TRADING-2438_Growth_Tilt_Top3_Candidate_PIT_Replay`。本任务未运行 `aits validate-data`，因为只读取 prior artifacts、candidate-set config、registry/catalog/system flow 和 research docs，不读取 fresh cached market/outcome data、不运行真实 regime attribution / parameter sweep / historical screen / PIT replay / backtest / scoring / daily report / outcome binding、不生成 feature/signal/outcome 或交易建议。
