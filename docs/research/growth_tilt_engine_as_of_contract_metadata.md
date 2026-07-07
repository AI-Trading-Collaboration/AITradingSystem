# Growth tilt engine as-of contract metadata

- status：`GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`
- as-of remediation completed：`True`
- lookahead violation count：`0`
- next route：`TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation`

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "lookahead_allowed": false,
  "metadata_rows": [
    {
      "as_of_contract_id": "growth_tilt_engine:drawdown_features:as_of:v1",
      "as_of_date": "2026-07-08",
      "as_of_remediation_status": "as_of_semantics_remediated",
      "as_of_semantics_contract": "Feature inputs must be computed only from source rows with source_date <= as_of_date; forward/evaluation windows after as_of_date are forbidden.",
      "as_of_semantics_status": "ready",
      "as_of_timestamp": "2026-07-08T00:00:00+00:00",
      "broker_action": "none",
      "contract_ready": false,
      "effective_date": "next_growth_tilt_engine_decision_after_as_of_date",
      "feature_computed_at": "growth_tilt_engine_research_pipeline_for_as_of_date",
      "feature_id": "drawdown_features",
      "forward_window_used": false,
      "known_at": "after_source_rows_with_source_date_lte_as_of_date_are_available",
      "lookahead_allowed": false,
      "lookback_window": "drawdown materiality gate and risk-off evidence",
      "no_lookahead_contract": {
        "effective_decision_boundary": "next_growth_tilt_engine_decision_after_as_of_date",
        "forbidden_inputs": [
          "future_returns_after_as_of_date",
          "future_drawdown_after_as_of_date",
          "post_signal_evaluation_window"
        ],
        "input_date_filter": "source_date <= as_of_date",
        "lookahead_allowed": false
      },
      "pit_gate_status": "blocked_pending_pit_evidence",
      "pit_safe": "unknown",
      "pit_safe_reason": "as-of semantics are explicit, but 2412 does not complete source snapshot traceability or PIT gate evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_as_of_contract_metadata.v1",
      "source_feature_name": "drawdown_features",
      "source_observed_at": "source_rows_with_source_date_lte_as_of_date",
      "source_traceability_status": "not_ready_missing_source_snapshot",
      "validity_dependency_status": "not_assessed_in_2412"
    },
    {
      "as_of_contract_id": "growth_tilt_engine:volatility_inputs:as_of:v1",
      "as_of_date": "2026-07-08",
      "as_of_remediation_status": "as_of_semantics_remediated",
      "as_of_semantics_contract": "Feature inputs must be computed only from source rows with source_date <= as_of_date; forward/evaluation windows after as_of_date are forbidden.",
      "as_of_semantics_status": "ready",
      "as_of_timestamp": "2026-07-08T00:00:00+00:00",
      "broker_action": "none",
      "contract_ready": false,
      "effective_date": "next_growth_tilt_engine_decision_after_as_of_date",
      "feature_computed_at": "growth_tilt_engine_research_pipeline_for_as_of_date",
      "feature_id": "volatility_inputs",
      "forward_window_used": false,
      "known_at": "after_source_rows_with_source_date_lte_as_of_date_are_available",
      "lookahead_allowed": false,
      "lookback_window": "vol target / risk and regime diagnostics",
      "no_lookahead_contract": {
        "effective_decision_boundary": "next_growth_tilt_engine_decision_after_as_of_date",
        "forbidden_inputs": [
          "future_returns_after_as_of_date",
          "future_drawdown_after_as_of_date",
          "post_signal_evaluation_window"
        ],
        "input_date_filter": "source_date <= as_of_date",
        "lookahead_allowed": false
      },
      "pit_gate_status": "blocked_pending_pit_evidence",
      "pit_safe": "unknown",
      "pit_safe_reason": "as-of semantics are explicit, but 2412 does not complete source snapshot traceability or PIT gate evidence",
      "production_effect": "none",
      "schema_version": "growth_tilt_engine_as_of_contract_metadata.v1",
      "source_feature_name": "volatility_inputs",
      "source_observed_at": "source_rows_with_source_date_lte_as_of_date",
      "source_traceability_status": "not_ready_missing_source_snapshot",
      "validity_dependency_status": "not_assessed_in_2412"
    }
  ],
  "production_effect": "none",
  "schema_version": "growth_tilt_engine_as_of_contract_metadata.v1"
}
```