# Growth tilt engine as-of semantics remediation

## 结论摘要

- status：`GROWTH_TILT_ENGINE_AS_OF_SEMANTICS_REMEDIATION_READY_WITH_REMAINING_BLOCKERS`
- input gap count：`7`
- as-of gap count：`2`
- as-of remediated count：`2`
- remaining blocked or gap count：`7`
- contract ready count：`0`
- next route：`TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation`

2412 只补齐 as-of semantics 和 no-lookahead contract。source traceability、validity dependency、PIT gate 和 valid_until_window 仍未在本任务中修复，因此 `growth_tilt_engine` blocker 不能解除或降级。

## Before / After

```json
{
  "broker_action": "none",
  "engine_id": "growth_tilt_engine",
  "production_effect": "none",
  "record_count": 2,
  "records": [
    {
      "after": {
        "as_of_contract_metadata": {
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
        "as_of_remediation_status": "as_of_semantics_remediated",
        "as_of_semantics": "Feature inputs must be computed only from source rows with source_date <= as_of_date; forward/evaluation windows after as_of_date are forbidden.",
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "source_traceability_status",
          "pit_gate_status"
        ],
        "feature_id": "drawdown_features",
        "mapping_status": "mapped_with_caveats",
        "mapping_status_reason": "as-of semantics remediated in TRADING-2412; source traceability and PIT gate evidence remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "drawdown_features",
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "before": {
        "as_of_semantics": "research artifacts use drawdown evidence but PIT field lineage is missing",
        "as_of_semantics_status": "missing",
        "contract_ready": false,
        "feature_id": "drawdown_features",
        "mapping_status": "missing_as_of_semantics",
        "pit_eligibility": "APPROXIMATE_PIT",
        "source_feature_name": "drawdown_features",
        "traceability_status": "missing",
        "validity_dependency": "none_identified_in_2410"
      },
      "feature_id": "drawdown_features"
    },
    {
      "after": {
        "as_of_contract_metadata": {
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
        },
        "as_of_remediation_status": "as_of_semantics_remediated",
        "as_of_semantics": "Feature inputs must be computed only from source rows with source_date <= as_of_date; forward/evaluation windows after as_of_date are forbidden.",
        "as_of_semantics_status": "ready",
        "contract_ready": false,
        "contract_ready_blocking_dimensions": [
          "source_traceability_status",
          "pit_gate_status"
        ],
        "feature_id": "volatility_inputs",
        "mapping_status": "mapped_with_caveats",
        "mapping_status_reason": "as-of semantics remediated in TRADING-2412; source traceability and PIT gate evidence remain unresolved",
        "pit_gate_status": "blocked_pending_pit_evidence",
        "pit_safe": "unknown",
        "source_feature_name": "volatility_inputs",
        "source_traceability_status": "not_ready_missing_source_snapshot",
        "validity_dependency_status": "not_assessed_in_2412"
      },
      "before": {
        "as_of_semantics": "rolling windows appear historical but explicit as-of lineage is missing",
        "as_of_semantics_status": "missing",
        "contract_ready": false,
        "feature_id": "volatility_inputs",
        "mapping_status": "missing_as_of_semantics",
        "pit_eligibility": "APPROXIMATE_PIT",
        "source_feature_name": "volatility_inputs",
        "traceability_status": "missing",
        "validity_dependency": "none_identified_in_2410"
      },
      "feature_id": "volatility_inputs"
    }
  ],
  "schema_version": "growth_tilt_engine_as_of_before_after_remediation.v1"
}
```

## As-Of Contract Metadata

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

## Remaining Blockers

```json
{
  "as_of_remediated_count": 2,
  "broker_action": "none",
  "broker_enabled": false,
  "candidate_search_enabled": false,
  "contract_ready_count": 0,
  "engine_id": "growth_tilt_engine",
  "growth_tilt_engine_blocker_downgraded": false,
  "growth_tilt_engine_blocker_resolved": false,
  "input_gap_count": 7,
  "observation_enabled": false,
  "paper_shadow_enabled": false,
  "production_effect": "none",
  "production_enabled": false,
  "recommended_next_task": "TRADING-2413_Growth_Tilt_Engine_Source_Traceability_Remediation",
  "remaining_blocked_or_gap_count": 7,
  "remaining_blocking_reasons": [
    "source_traceability_remediation_not_completed_in_2412",
    "pit_gate_evidence_not_completed_in_2412",
    "valid_until_window_blocker_not_remediated_in_2412"
  ],
  "schema_version": "growth_tilt_engine_as_of_remaining_blocker_summary.v1",
  "valid_until_window_blocker_downgraded": false,
  "valid_until_window_blocker_resolved": false
}
```

## Data Quality Boundary

- data_quality_gate_executed：`False`
- data_quality_gate_reason：`NOT_APPLICABLE_AS_OF_SEMANTICS_REMEDIATION_PRIOR_ARTIFACTS_ONLY_NO_FRESH_MARKET_DATA`

## Safety Boundary

- growth_tilt_engine_blocker_resolved：`False`
- growth_tilt_engine_blocker_downgraded：`False`
- valid_until_window_blocker_resolved：`False`
- valid_until_window_blocker_downgraded：`False`
- candidate_search_enabled：`False`
- observation_enabled：`False`
- paper_shadow_enabled：`False`
- production_enabled：`False`
- broker_enabled：`False`