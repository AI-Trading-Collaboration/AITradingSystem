# Risk-Cap-Only Actual-Path Validation

最后更新：2026-06-30

|candidate|eligible|capture|stress|tail|false_risk_cap|status|
|---|---:|---:|---:|---:|---:|---|
|`volatility_regime_scope_narrowed_risk_cap_v1`|288|0.375|0.149306|0.017361|48|`RISK_CAP_SCOPE_VALIDATED_LOCAL_EDGE`|

`risk_cap_only` candidate 不要求市场最终一定下跌；若 active window 出现 intrahorizon drawdown、stress 或 volatility expansion，也可构成 risk-cap alignment。
该 family 的 2291 active records 只有 373 条，虽整体样本通过本轮 minimum active eligible floor，但 subgroup 解释仍应保持保守；forward observe candidate 只进入 TRADING-2293 readiness review，不得直接进入 paper-shadow。
