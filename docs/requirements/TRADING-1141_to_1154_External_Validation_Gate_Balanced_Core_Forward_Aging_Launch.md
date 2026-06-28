# TRADING-1141 to 1154 External Validation Gate And Balanced Core Forward-Aging Launch

最后更新：2026-06-28

## Background

TRADING-1129 to 1140 established the external backtest validation and
reconciliation workflow. TRADING-1119 to 1128 established the research-only
balanced-core forward-aging launch path for
`equal_risk_growth_tilt_vol_target_v1_tv4_w120_q7_s1`.

This batch connects those two tracks. It reads the real external validation
artifacts, performs final static/dynamic/metric/SGOV signoffs, gates the launch,
writes the first balanced-core observation only after the gate passes, and
generates the dual forward-aging after-launch review artifacts.

## Safety Boundary

Every artifact must keep:

```text
paper_shadow_allowed = false
production_allowed = false
broker_action = none
manual_review_required = true
production_effect = none
```

The batch must not activate paper-shadow, mutate production configuration,
connect to a broker, emit broker actions, overwrite
`equal_risk_qqq_sgov`, or output real trading advice.

## Stage Breakdown

### Stage 1: External Validation Final Gate

- TRADING-1141 adds
  `aits research strategies external-validation-real-result-status-reader`.
- TRADING-1142 adds
  `aits research strategies static-baseline-reconciliation-final-check`.
- TRADING-1143 adds
  `aits research strategies dynamic-weight-path-replay-final-check`.
- TRADING-1144 adds
  `aits research strategies metric-and-sgov-reconciliation-signoff`.
- TRADING-1145 adds
  `aits research strategies external-validation-to-launch-gate`.

These commands read or regenerate the real TRADING-1129 to 1140 artifacts and
summarize whether static baselines, dynamic weight path replay, metric
definitions, SGOV total-return handling, difference attribution and owner
recommendation support a research-only launch.

### Stage 2: Balanced-Core Launch After External Validation

- TRADING-1146 adds
  `aits research strategies balanced-core-launch-preflight`.
- TRADING-1147 adds
  `aits research strategies balanced-core-first-observation-write-after-validation`.
- TRADING-1148 adds
  `aits research strategies balanced-core-observation-idempotency-proof`.

Observation writing is append-only and must be blocked when the external
validation launch gate is blocked. Re-running for the same date must return
already-exists without changing target weights, comparator weights, external
validation status, data quality status or definition hash.

### Stage 3: Dual Forward-Aging After-Launch Review

- TRADING-1149 adds
  `aits research strategies dual-forward-aging-comparator-panel-after-launch`.
- TRADING-1150 adds
  `aits research strategies dual-forward-aging-scoreboard-safety-review`.
- TRADING-1151 adds
  `aits research strategies dual-forward-aging-reader-brief-safe-preview-after-launch`.

The dual panel compares defensive primary, balanced-core candidate, hard
benchmark and static references. Scoreboard and Reader Brief preview must keep
sample discipline and avoid paper-shadow / production / broker wording.

### Stage 4: Owner, Master And Monthly Monitor

- TRADING-1152 adds
  `aits research strategies balanced-core-launch-owner-report`.
- TRADING-1153 adds
  `aits research strategies external-validation-balanced-core-launch-master-review`.
- TRADING-1154 adds
  `aits research strategies dual-forward-aging-monthly-monitor-contract`.

The owner and master reports summarize the launch result, safety posture and
next minimum task. The monthly monitor contract defines the later monthly
research-only review rules without adding scheduler entries in this batch.

## Required Artifacts

- `outputs/research_strategies/external_validation/external_validation_real_result_status_reader.json/md`
- `outputs/research_strategies/external_validation/static_baseline_reconciliation_final_check.json/md`
- `outputs/research_strategies/external_validation/dynamic_weight_path_replay_final_check.json/md`
- `outputs/research_strategies/external_validation/metric_sgov_reconciliation_signoff.json/md`
- `outputs/research_strategies/external_validation/external_validation_to_launch_gate.json/md`
- `outputs/research_strategies/growth_components/balanced_core_launch_preflight.json/md`
- `outputs/research_strategies/growth_components/forward_aging_observations/balanced_core_forward_aging_observation_<date>.json/md`
- `outputs/research_strategies/growth_components/balanced_core_observation_idempotency_proof.json/md`
- `outputs/research_strategies/roadmap/dual_forward_panel_after_launch.json/md`
- `outputs/research_strategies/roadmap/dual_forward_aging_scoreboard_safety_review.json/md`
- `outputs/research_strategies/roadmap/dual_forward_aging_reader_brief_after_launch_safe_preview.json/md`
- `outputs/research_strategies/roadmap/balanced_core_launch_owner_report.json`
- `docs/research/balanced_core_launch_owner_report.md`
- `outputs/research_strategies/roadmap/external_validation_balanced_core_launch_master_review.json`
- `docs/research/external_validation_balanced_core_launch_master_review.md`
- `outputs/research_strategies/roadmap/dual_forward_aging_monthly_monitor_contract.json`
- `docs/research/dual_forward_aging_monthly_monitor_contract.md`

## Acceptance

- External validation launch gate passes or warns only when master status,
  static final check, dynamic replay final check, metric/SGOV signoff,
  difference attribution and definition hash are acceptable.
- Balanced-core observation is written only after the external validation gate
  and preflight pass or warn.
- Duplicate proof confirms same-date reruns preserve the original observation.
- Dual panel, scoreboard safety review and Reader Brief preview remain
  research-only and never emit trading advice.
- Owner report, master review and monthly monitor contract answer the required
  project-owner questions and preserve the safety boundary.
- Report registry, artifact catalog, system flow and focused tests are updated
  in the same change.

## Progress Notes

- 2026-06-26: Created as IN_PROGRESS from owner task package. Implementation
  must reuse existing TRADING-1119 to 1140 artifacts, add the required wrapper
  CLI/artifacts, update registry/catalog/system-flow documentation, and pass the
  owner-provided validation suite.
- 2026-06-26: Implementation completed and moved to VALIDATING. Added the 14
  required CLI/artifacts, report registry entries, artifact catalog rows,
  system-flow paragraph, focused dual forward-aging tests and status-mapping
  wrappers that preserve the existing append-only observation reader contract.
  Validation passed Ruff, compileall, focused parallel pytest for external
  validation / growth tilt / dual forward aging, task-register/report/docs
  parallel pytest, and git diff check.
