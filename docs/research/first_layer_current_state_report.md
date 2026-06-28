# First-layer current state report

- status: `FIRST_LAYER_CURRENT_STATE_READY_PROMOTION_BLOCKED`
- market_regime: `ai_after_chatgpt`
- requested_date_range: `2022-12-01` to `latest`
- actual_signal_range: `2023-02-22` to `2026-03-27`
- data_quality_status: `PASS_WITH_WARNINGS`
- safety: `promotion_allowed=false`, `paper_shadow_allowed=false`, `production_allowed=false`, `broker_action=none`

## 结论

当前 first-layer composer v2 只能作为失败归因基线。它没有覆盖 2022 stress slice，并且仍出现 false risk-on/off、late risk-on/off 与 regime flip 诊断事件；这些结果支持继续做 TRADING-2271～2273，而不是恢复 reopen gate 或 paper-shadow。

## Failure taxonomy

|failure_type|event_count|promotion interpretation|
|---|---:|---|
|`false_risk_on`|198|diagnostic_only_not_gate_evidence|
|`false_risk_off`|499|diagnostic_only_not_gate_evidence|
|`late_risk_off`|86|diagnostic_only_not_gate_evidence|
|`late_risk_on`|306|diagnostic_only_not_gate_evidence|

## Benchmark coverage

|ticker|required|data_available|history_start|history_end|rows|
|---|---:|---:|---|---|---:|
|`QQQ`|True|True|2018-01-02|2026-06-26|2132|
|`SPY`|True|True|2018-01-02|2026-06-26|2132|
|`SMH`|True|True|2018-01-02|2026-06-26|2132|
|`IWM`|False|False|||0|
|`RSP`|False|False|||0|

## Audit notes

- benchmark_consistency_score: `0.392621`
- 2022_stress_slice_signal_coverage: `missing`
- IWM / RSP 缺失时不做 proxy 填补，也不把 QQQ/SPY/SMH consistency 泛化成 true breadth。
