# Ablation Runner Scope Freeze

最后更新：2026-06-19

状态：`ABLATION_RUNNER_SCOPE_FROZEN`

## Scope

| Layer | Definition | Forbidden Substitute |
|---|---|---|
| B0 | static strategic baseline only | mixed dynamic allocation |
| B1 | B0 + execution / no-trade / turnover control only | trend, momentum, relative strength, risk, regime, confidence |
| B2 | B0 + fast asymmetric risk scaler only | slow tilt, regime, confidence, mixed logic |
| B3 | B0 + slow relative tilt only | risk scaler, regime, confidence, mixed logic |
| B4 | B2 + B3 combination only | new signal or regime logic |
| B5 | B4 + confidence shrinkage only | new tilt, risk or regime logic |
| B6 | B5 + regime information only | new tilt, risk or confidence policy |

Existing P0 dynamic strategy must not be used as a substitute for B1-B6 because it blends
trend, momentum, relative strength, risk and regime logic.

## Runner Rules

- Each runner must declare its exact added mechanism.
- Each runner must reject unapproved mixed logic.
- Each runner must emit research-only artifacts.
- No runner may emit official target weights.
- No runner may activate paper-shadow.
- No runner may create broker/order/live/production mutation artifacts.

## Next Action

Run `aits etf weight-research validate-contracts` before B1. B1 may only use the B1-only
execution-control runner.
