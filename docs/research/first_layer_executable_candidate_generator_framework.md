# First-Layer Executable Candidate Generator Framework

## 范围

TRADING-2283 建立 first-layer executable candidate generator framework。该框架把
candidate definition 到 candidate-bound artifacts 的生产路径固定为：

```text
generator registry
  -> FirstLayerCandidateSignalGenerator
  -> candidate signal spec
  -> candidate-bound signal series
  -> candidate-bound prediction artifact
  -> TRADING-2282 candidate binding validator
  -> generation / validation summaries
```

本框架只解决证据链生产能力，不评价收益，不执行 actual-path validation，不创建 promotion、
paper-shadow、production 或 broker action。

## CLI

```bash
aits research trends first-layer-candidate-generator-framework \
  --candidate-id framework_smoke_candidate \
  --target-asset QQQ \
  --start-date 2023-01-01 \
  --end-date 2023-03-31 \
  --horizon 10d \
  --output-dir outputs/research_trends/first_layer_candidate_generators \
  --mode framework_smoke_test
```

初期只允许 `framework_smoke_test` mode。`promotion`、`paper_shadow`、`production` 和
`broker_action` 不是可用模式。

## Artifact Contract

runtime 输出：

- `generator_registry.json`
- `framework_smoke_candidate_signal_spec.json`
- `framework_smoke_candidate_signal_series.csv`
- `framework_smoke_candidate_prediction_artifact.json`
- `framework_smoke_candidate_generation_summary.json`
- `framework_smoke_candidate_validation_summary.json`

signal series 和 prediction artifact 复用 TRADING-2282 candidate-bound contract。signal spec
新增 validator checks，要求 candidate/generator/schema/target/horizon/PIT policy 和安全字段完整。

## 安全边界

所有 TRADING-2283 artifacts 固定：

- `artifact_role=framework_smoke_test`
- `historical_executable_artifact=false`
- `actual_path_validation_ready=false`
- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`
- `permanently_inconclusive_override_allowed=false`

`framework_smoke_candidate` 只用于 framework validation。它不是真实交易策略，不代表
`baseline_plus_trend_structure`、`risk_appetite` 或 `volatility_regime`，也不能改变
TRADING-2281 permanently inconclusive 结论。
