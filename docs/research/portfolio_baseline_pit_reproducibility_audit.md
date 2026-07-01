# Portfolio Baseline PIT Reproducibility Audit

- status: `PORTFOLIO_BASELINE_SOURCE_DECISION_READY_PROMOTION_BLOCKED`

|baseline|pit_status|replayable|artifact_hash|source_version|recommendation|
|---|---|---|---|---|---|
|`synthetic_observe_only_baseline`|`SYNTHETIC_OBSERVE_ONLY`|`True`|`False`|`True`|Use only as fallback proxy diagnostics.|
|`static_etf_allocation_baseline`|`PIT_APPROXIMATION_READY`|`True`|`True`|`True`|Use for TRADING-2326 first source-bound dry-run.|
|`dynamic_strategy_target_exposure_baseline`|`BLOCKED`|`False`|`False`|`False`|Keep as medium-term baseline after artifact remediation.|
|`paper_portfolio_advisory_baseline`|`REPLAYABLE_BUT_NOT_STRICT_PIT`|`True`|`True`|`True`|Reserve for forward observe comparison after continuity audit.|
|`actual_holdings_derived_baseline`|`MANUAL_REFERENCE_ONLY`|`False`|`True`|`False`|Do not use for current research-layer simulation.|
