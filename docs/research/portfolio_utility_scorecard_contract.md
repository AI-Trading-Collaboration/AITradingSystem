# Portfolio Utility Scorecard Contract

最后更新：2026-06-19

状态：`SCORECARD_CONTRACT_FROZEN_FOR_MINI_BACKFILL`

## Policy Metadata

- owner: system
- version: `weight_research_program_v1_scorecard_2026-06-19`
- status: `pilot_baseline_pre_experiment`
- rationale: 在 B0-B6 实验前冻结 utility 评分，避免看到结果后调整 lambda。
- intended effect: 同时比较 net return、drawdown、turnover、tracking error、worst-window、
  dispersion、cost drag 和 signal robustness。
- validation evidence: focused contract tests 和 B0/B1 mini-backfill 后复核。
- review condition: B0-B3 mini-backfill 后，或 owner 在实验前改变风险/成本偏好时复核。

## Status Taxonomy

`UTILITY_IMPROVED`、`UTILITY_MIXED`、`UTILITY_WEAK`、`UTILITY_INVALID`

`UTILITY_IMPROVED` 不能覆盖 data quality、stress、cost/benchmark、signal robustness 或
window fragility hard stop。
