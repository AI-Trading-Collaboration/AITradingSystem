# Signal Robustness Entry Contract

最后更新：2026-06-19

状态：`SIGNAL_ROBUSTNESS_ENTRY_CONTRACT_FROZEN`

## Required Fields

- required inputs
- required feature columns
- required signal series
- coverage threshold
- stale-input behavior
- schema compatibility
- fail-closed behavior
- allowed warnings
- blocking conditions

## B1 Exception

B1 is execution-control only. It may run with no signal series, but it must fail if trend,
momentum, relative strength, risk, regime or confidence inputs are supplied.

## B2-B6 Rule

B2-B6 must pass signal robustness before execution. Missing required features, stale inputs,
unknown schema versions, coverage below threshold, blocked signal robustness, data quality
failure or mixed P0 dynamic logic must fail closed.

## Reader Brief

Signal robustness entry contract is frozen. B1 can proceed as a no-signal execution-control
runner after contract validation; later layers remain blocked until their signal series pass.
