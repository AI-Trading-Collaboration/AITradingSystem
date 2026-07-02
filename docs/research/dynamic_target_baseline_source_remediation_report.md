# Dynamic Target Baseline Source Remediation

TRADING-2329 只做 source remediation、schema adapter 和 research-only wrapper preparation。
TRADING-2328 已发现 34 个 candidate artifacts，但 0 个 PIT-ready source，因此不能直接进入 dynamic target baseline simulation。

- status: `DYNAMIC_TARGET_BASELINE_SOURCE_REMEDIATION_READY_PROMOTION_BLOCKED`
- upstream_candidate_artifacts_found: `34`
- upstream_pit_ready_source_count: `0`
- remediable_source_count: `4`
- wrapper_generated: `True`
- wrapper_validation_status: `PASS_WITH_WARNINGS`
- readiness_status: `DYNAMIC_WRAPPER_SCHEMA_ADAPTER_REQUIRED`
- next_task: `TRADING-2330_Dynamic_Target_Baseline_Timestamp_Remediation`
- simulation_executed: `False`
- promotion_allowed: `False`
- paper_shadow_allowed: `False`
- production_allowed: `False`
- broker_action: `none`

## Data Quality

`aits validate-data` 不适用：TRADING-2329 只读取 prior research outputs、static config、registry 和 candidate artifacts，不读取 cached market data 或 runtime exposure data。

## Source Families

- `dynamic_strategy_target_exposure`: best_source_id=`xecution_semantics_dynamic_regime_overlay_v0_4_lower_turnover_target_vs_actual_position_path_csv`, ranking_label=`REMEDIABLE_WITH_PIT_CAVEAT`, score=`0.294643`
- `manual_review_only_target_exposure`: best_source_id=`outputs_research_strategies_execution_semantics_target_vs_actual_position_path_builder_json`, ranking_label=`REMEDIABLE_WITH_SCHEMA_ADAPTER`, score=`0.2675`
- `paper_portfolio_advisory_target`: best_source_id=``, ranking_label=`NOT_REMEDIABLE`, score=`0.0`
- `etf_allocation_dynamic_output`: best_source_id=``, ranking_label=`NOT_REMEDIABLE`, score=`0.0`
- `risk_budget_target_exposure`: best_source_id=`outputs_tmp_qqq_growth_smoke_full_qqq_plus_risk_budget_review_json`, ranking_label=`NOT_REMEDIABLE`, score=`-0.191429`

## Remediation

- `_semantics_dynamic_v0_5_ai_trend_confirmed_staleness_aware_v1_target_vs_actual_position_path_csv`: `REMEDIATION_READY_WITH_PIT_CAVEAT` / `ADD_PIT_CAVEAT_AND_WRAPPER`
- `gies_execution_semantics_dynamic_v0_5_ai_trend_confirmed_only_target_vs_actual_position_path_csv`: `REMEDIATION_READY_WITH_PIT_CAVEAT` / `ADD_PIT_CAVEAT_AND_WRAPPER`
- `n_semantics_dynamic_v0_5_ai_trend_confirmed_event_override_v1_target_vs_actual_position_path_csv`: `REMEDIATION_READY_WITH_PIT_CAVEAT` / `ADD_PIT_CAVEAT_AND_WRAPPER`
- `xecution_semantics_dynamic_regime_overlay_v0_4_lower_turnover_target_vs_actual_position_path_csv`: `REMEDIATION_READY_WITH_PIT_CAVEAT` / `ADD_PIT_CAVEAT_AND_WRAPPER`

## Safety Boundary

Wrapper 中的 `target_exposure` 只是 research baseline field，不是 trading target weight、rebalance instruction、buy/sell signal、paper-shadow、production 或 broker action。
