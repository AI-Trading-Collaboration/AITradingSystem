# Trend / Risk / Volatility Executable Candidate Generators

## 范围

TRADING-2284 在 TRADING-2283 first-layer executable candidate generator framework 上新增
三个真实 regenerated candidate generators：

- `baseline_plus_trend_structure`
- `risk_appetite`
- `volatility_regime`

它们不再只输出 offline experiment definition，而是 native 写出 candidate-bound signal spec、
signal series、prediction artifact、generation summary 和 validation summary。

## CLI

```bash
aits research trends first-layer-candidate-generators-regenerate \
  --candidates baseline_plus_trend_structure,risk_appetite,volatility_regime \
  --target-assets QQQ,SPY,SMH \
  --start-date 2023-01-01 \
  --end-date 2026-06-28 \
  --horizons 5d,10d,20d \
  --output-dir outputs/research_trends/first_layer_candidate_generators_regenerated \
  --mode regenerated_candidate_artifacts
```

`--mode` 初期只允许 `regenerated_candidate_artifacts`。`promotion`、`paper_shadow`、
`production` 和 `broker_action` 都不是可用模式。

## 输出

顶层输出：

- `generator_registry.json`
- `regeneration_run_summary.json`
- `validation_summary.json`

每个 candidate 子目录输出：

- `candidate_signal_spec.json`
- `candidate_signal_series.csv`
- `candidate_prediction_artifact.json`
- `generation_summary.json`
- `validation_summary.json`

所有 series 和 prediction artifact 复用 TRADING-2282 candidate signal binding validator。

## Candidate 语义

`baseline_plus_trend_structure` 生成趋势结构、趋势确认、趋势弱化和相对强弱信号，用于解释
baseline composer 之外的 trend structure 状态。

`risk_appetite` 生成市场风险偏好、risk-on confirmation、risk-off pressure 和 semiconductor
risk appetite 信号。它适合作为 confirm signal、exposure limiter 或 risk cap input，不是
单独调仓触发器。

`volatility_regime` 生成 volatility regime、volatility expansion、stress regime 和
volatility compression 信号。它主要用于 risk cap、veto、cooldown 或 exposure limiter，
不是 direct return prediction signal。若 VIX 缺失，generator 使用 realized volatility proxy，
并在 provenance 中标记 `volatility_proxy_mode=realized_volatility_only`。

## 安全边界

所有 TRADING-2284 artifacts 固定：

- `artifact_role=regenerated_executable_candidate_artifact`
- `historical_executable_artifact=true`
- `actual_path_validation_ready=false`
- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `permanently_inconclusive_override_allowed=false`

`historical_executable_artifact=true` 只表示 artifact 由 executable generator 根据历史输入重新生成。
它不代表 promotion-ready，不代表 actual-path validation 已完成，也不改变 TRADING-2281 的
permanently inconclusive 结论。

## 后续

TRADING-2285 才会读取这些 regenerated candidate artifacts，执行 candidate-level actual-path
validation、risk/error attribution seed 和 owner review readiness 判断。在 TRADING-2285 完成且
owner review 之前，promotion、paper-shadow、production 和 broker path 继续阻断。
