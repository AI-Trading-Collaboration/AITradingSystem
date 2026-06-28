# TRADING-1129 to 1140 External Backtest Validation Reconciliation

最后更新：2026-06-28

## Background

The system now has two research-only forward-aging observation strategies:

- defensive primary: `equal_risk_qqq_sgov`
- balanced core candidate: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

This batch adds an external validation and reconciliation workflow. External
validation is not a replacement for the internal research system. It is an
audit layer to check whether static baselines and exported dynamic weight paths
can be independently replayed under comparable data, rebalance, execution and
metric definitions.

## Scope

Priority validation objects:

- P0: `100_qqq`, `qqq_50_sgov_50`, `qqq_60_sgov_40`
- P1: `equal_risk_qqq_sgov`
- P2: `equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`

Out of scope:

- Layer-1 selector
- Controlled Growth V2
- QQQ-plus growth
- tail-risk fallback
- LEAPS, Wheel, Options

## Safety Boundary

Every artifact must keep:

```text
paper_shadow_allowed = false
production_allowed = false
broker_action = none
manual_review_required = true
production_effect = none
```

The workflow must not connect to brokers, accounts, external credentials, or
production config. It must not output real trading advice.

## Implementation Stages

### Stage 1: Scope And Static Baseline Reconciliation

- TRADING-1129 adds `aits research strategies external-validation-scope-contract`.
- TRADING-1130 adds `aits research strategies static-baseline-external-reconciliation`.
- TRADING-1133 adds `aits research strategies metric-definition-reconciliation`.
- TRADING-1134 adds `aits research strategies sgov-total-return-external-check`.

External platform records are explicit inputs. If they are missing, the static
reconciliation artifact must state that manual external input is pending; it
must not silently treat internal metrics as external evidence.

### Stage 2: Dynamic Weight Path Replay

- TRADING-1131 adds `aits research strategies strategy-weight-path-export`.
- TRADING-1132 adds `aits research strategies external-independent-return-replay`.
- TRADING-1137 adds `aits research strategies external-validation-difference-attribution`.

The replay uses exported internal target weights and an independent replay path
to calculate returns and metrics. It reconciles replay metrics to internal
metrics for `equal_risk_qqq_sgov` and the balanced-core candidate.

### Stage 3: External Platform Feasibility

- TRADING-1135 adds `aits research strategies external-platform-feasibility-review`.
- TRADING-1136 adds `aits research strategies quantconnect-replication-dry-run-plan`.

This stage only evaluates feasibility. It does not require full QuantConnect or
TradingView implementation.

### Stage 4: Owner, Master And Reader Brief Preview

- TRADING-1138 adds `aits research strategies external-validation-owner-report`.
- TRADING-1139 adds `aits research strategies external-validation-master-review`.
- TRADING-1140 adds `aits research strategies external-validation-reader-brief-safe-preview`.

The Reader Brief step is preview-only and must not wire into the formal daily
Reader Brief.

## Required Artifacts

- `outputs/research_strategies/external_validation/external_validation_scope_contract.json/md`
- `outputs/research_strategies/external_validation/static_baseline_external_reconciliation.json/md`
- `outputs/research_strategies/external_validation/weight_paths/equal_risk_qqq_sgov_weight_path.csv`
- `outputs/research_strategies/external_validation/weight_paths/equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1_weight_path.csv`
- `outputs/research_strategies/external_validation/strategy_weight_path_export.json/md`
- `outputs/research_strategies/external_validation/external_independent_return_replay.json/md`
- `outputs/research_strategies/external_validation/metric_definition_reconciliation.json/md`
- `outputs/research_strategies/external_validation/sgov_total_return_external_check.json/md`
- `outputs/research_strategies/external_validation/external_platform_feasibility_review.json/md`
- `outputs/research_strategies/external_validation/quantconnect_replication_dry_run_plan.json/md`
- `outputs/research_strategies/external_validation/external_validation_difference_attribution.json/md`
- `outputs/research_strategies/external_validation/external_validation_owner_report.json`
- `docs/research/external_validation_owner_report.md`
- `outputs/research_strategies/external_validation/external_validation_master_review.json`
- `docs/research/external_validation_master_review.md`
- `outputs/research_strategies/external_validation/external_validation_reader_brief_safe_preview.json/md`

## Progress Notes

- 2026-06-26: Created as IN_PROGRESS. Implementation must update report
  registry, artifact catalog, system flow, task register and focused tests in
  the same change. Required validation is the owner-provided command suite.
- 2026-06-26: Implementation complete and moved to VALIDATING. Added the
  external validation builder module, CLI registration, report registry entries,
  artifact catalog rows, system-flow paragraph and `tests/test_external_validation.py`.
  Focused fixture validation covers static baseline reconciliation with explicit
  external records, dynamic weight path CSV export, independent replay, metric
  definition reconciliation, SGOV total-return handling, feasibility / dry-run
  plans, difference attribution, owner/master reports and Reader Brief safe
  preview. During validation, fixed a weight-path CSV reader index-alignment bug
  that caused independent replay to treat exported weights as zero.
