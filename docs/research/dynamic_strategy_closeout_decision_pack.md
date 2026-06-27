# Dynamic Strategy Closeout Decision Pack

元数据：

- review_id：`dynamic_strategy_closeout_2026-06-27`
- source_commit：`28cabc10b042bd9da98780070aea9f85d54c5b5d`
- market_regime：`ai_after_chatgpt`
- requested date range：`2022-12-01`～`2026-06-26`
- metric_namespace：`actual_path_only`
- target_path_metrics_role：`diagnostic_only`
- promotion_status：`BLOCKED`
- owner_review_status：`OWNER_REVIEW_REQUIRED`
- recommended_owner_decision：`APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`

## 1. Executive Summary

Dynamic strategy full allocation promotion should remain blocked. The recommended owner decision is `APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`: pause full allocation research, keep defensive overlay / advisory diagnostic research active, and preserve all evidence as auditable closeout material.

## 2. What Was Validated

- Execution semantics and actual-position-path rebacktest are explicit.
- Target-path metrics are diagnostic-only and cannot unlock promotion.
- `limited_adjustment` has the best actual-path dynamic evidence, with marginal edge versus `qqq_60_sgov_40`.
- PIT, walk-forward, taxonomy, timing, cost/cash, stress, regime and artifact governance reviews were generated for the AI-after-ChatGPT regime.
- Artifact governance confirms tracked snapshots, hashes, metric namespace, legacy evidence blocker and target-path guardrails.

## 3. What Failed

- Full allocation edge is not stable enough.
- `limited_adjustment` still underperforms `100_qqq`.
- `dynamic_v0_5_ai_trend_confirmed_only` is false-risk-off dominated and regime overfitted.
- Staleness-aware variants produced `NO_MATERIAL_IMPROVEMENT`.
- Event override variants are either too costly or too noisy and still lack runtime event taxonomy provenance.
- Stress and regime reviews keep dynamic promotion blocked.

## 4. Remaining Blockers

See `inputs/research_reviews/dynamic_strategy_blocker_inventory.yaml`. The most important blockers are actual-path edge insufficiency, simple baseline underperformance, risk-off timing noise, stress gate blocked, walk-forward/regime fragility, PIT timestamp limitations and target-path/legacy evidence misuse risk.

## 5. Candidate-by-Candidate Disposition

|Candidate|Disposition|Reason|
|---|---|---|
|`limited_adjustment`|`DOWNGRADE_TO_DEFENSIVE_OVERLAY`|Best actual-path candidate, but not enough for full allocation promotion.|
|`dynamic_v0_5_ai_trend_confirmed_only`|`ADVISORY_DIAGNOSTIC_ONLY`|False risk-off dominates and walk-forward verdict is `REGIME_OVERFITTED`.|
|`limited_adjustment_staleness_aware_v1`|`ARCHIVE_AS_RESEARCH_EVIDENCE`|No material improvement versus parent.|
|`dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1`|`ARCHIVE_AS_RESEARCH_EVIDENCE`|No material improvement and parent remains weak.|
|`limited_adjustment_event_override_v1`|`ADVISORY_DIAGNOSTIC_ONLY`|Event override increases turnover too much.|
|`dynamic_v0_5_ai_trend_confirmed_event_override_v1`|`ADVISORY_DIAGNOSTIC_ONLY`|Event override is too noisy with high turnover.|

## 6. Why Promotion Remains Blocked

Promotion remains blocked because the full allocation case requires more than one acceptable metric. The system still lacks stable actual-path edge, robust regime behavior, clean stress review, sufficiently proven event taxonomy provenance, and owner-approved paper-shadow preflight. Target-path and legacy dynamic evidence are explicitly forbidden for promotion.

## 7. Defensive Overlay Recommendation

Keep defensive overlay research active, but only as observe-only and risk-reduction advisory. The overlay may surface event risk, regime diagnosis, high-vol stress warnings, cash/SGOV fallback suggestions and manual review prompts. It may not automatically trade, restore risk-on, enter paper-shadow, write production weights or send broker orders.

## 8. Conditions to Reopen Full Allocation Research

Reopen conditions are defined in `inputs/research_reviews/dynamic_full_allocation_reopen_criteria.yaml`. They require a new actual-path candidate, locked-sample validation, multi-regime contribution, positive timing attribution, net-of-cost edge, non-worsening stress metrics, controlled turnover, PIT audit pass, walk-forward / out-of-sample pass and owner-approved paper-shadow preflight.

## 9. Owner Decision Required

Owner decision options:

- `APPROVE_DOWNGRADE_TO_DEFENSIVE_OVERLAY`：recommended.
- `KEEP_FULL_ALLOCATION_RESEARCH_OPEN`：not recommended unless owner accepts overfitting and evidence gaps.
- `ARCHIVE_DYNAMIC_STRATEGY_RESEARCH`：acceptable if no observe-only overlay work is desired.
- `REQUEST_ADDITIONAL_EVIDENCE`：acceptable, but should target forward observe-only evidence rather than historical tuning.
