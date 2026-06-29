# Framework Smoke Candidate Generation Report

## 结论

`framework_smoke_candidate` 是 TRADING-2283 的 deterministic framework smoke-test candidate。
它只证明 generator runtime 可以原生写出 candidate signal spec、candidate-bound signal
series、candidate-bound prediction artifact、generation summary 和 validation summary。

该 smoke candidate 不代表真实 strategy family，不进入 actual-path validation，不进入 owner
review，不允许 promotion、paper-shadow、production 或 broker action。

## 预期命令

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

## Validation Contract

validation summary 必须为 `PASS`，并披露：

- `candidate_bound_validator_reused=true`
- signal spec validation PASS
- candidate-bound signal series validation PASS
- candidate-bound prediction artifact validation PASS
- prediction record count 等于 signal series row count
- `historical_executable_artifact=false`
- `actual_path_validation_ready=false`
- `promotion_eligible=false`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `broker_action=none`

## TRADING-2281 边界

本 smoke report 不改变 `baseline`、`baseline_plus_trend_structure`、`risk_appetite` 和
`volatility_regime` 的 permanently inconclusive 状态。TRADING-2284 才会实现真实 trend /
risk / volatility executable generators。
