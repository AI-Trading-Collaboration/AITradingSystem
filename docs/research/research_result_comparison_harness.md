# Research Result Comparison Harness

最后更新：2026-06-19

状态：`COMPARISON_HARNESS_CONTRACT_READY`

比较状态限定为 `IMPROVED`、`MIXED`、`NO_IMPROVEMENT`、`WORSE` 和
`INVALID_COMPARISON`。缺 previous-layer result、data quality gate 失败、window catalog 不一致、
scorecard version mismatch 或 official target weight 语义时，必须使用
`INVALID_COMPARISON`，不得补造指标。

每个比较必须说明改善来自哪个模块，以及 cost、worst-window、benchmark、stress 和 signal
robustness 是否恶化。
