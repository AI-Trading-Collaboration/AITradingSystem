# Dynamic Dry-Run PIT Caveat Acceptance Report

- acceptance_status: `PIT_CAVEAT_ACCEPTED_FOR_RESEARCH_DRY_RUN_WITH_WARNINGS`
- strict_pit_ready: `False`
- pit_approximation_ready: `True`
- allowed_usage: `research_only_dynamic_dry_run, source_bound_simulation_proxy, exposure_cap_diagnostics`
- blocked_usage: `promotion, paper_shadow, production, broker_action, real_portfolio_decision`

## Carry-Forward Caveats

- TRADING-2332 must re-run data quality gate before consuming cached market data
- decision timestamp uses next trading day policy
- native intraday known-at timestamp missing
- source remediation alignment blockers carried forward for 2331
- target_exposure remains a research baseline field only
- timestamp remediation is PIT approximation and research-only
