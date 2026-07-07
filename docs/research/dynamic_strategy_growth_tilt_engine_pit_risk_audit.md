# Dynamic strategy growth tilt engine PIT risk audit

- status：`DYNAMIC_STRATEGY_GROWTH_TILT_ENGINE_PIT_AND_SIGNAL_CONSTRUCTION_REMEDIATION_PLAN_READY`
- risk count：`6`

{
  "blocking_risk_count": 3,
  "broker_action": "none",
  "input_under_review": "growth_tilt_engine",
  "production_effect": "none",
  "risks": [
    {
      "affected_feature_or_signal": "growth_tilt_engine",
      "evidence": "source features and signal horizon are not emitted with a deterministic as-of replay contract",
      "recommended_fix": "Add as_of_date, source_data_cutoff and window end-date to every growth tilt signal record.",
      "remediation_required": true,
      "risk_category": "LOOKAHEAD_RISK",
      "risk_id": "GTE-PIT-LOOKAHEAD-01",
      "severity": "BLOCKING"
    },
    {
      "affected_feature_or_signal": "adjusted_prices / returns",
      "evidence": "adjusted-price basis remains a caveat from prior data-quality review",
      "recommended_fix": "Carry adjusted-price basis and validate-data report link into growth tilt signal artifacts.",
      "remediation_required": true,
      "risk_category": "REVISION_RISK",
      "risk_id": "GTE-PIT-REVISION-01",
      "severity": "MATERIAL"
    },
    {
      "affected_feature_or_signal": "trend_features / volatility_inputs",
      "evidence": "feature-level generated_at and source_data_cutoff are not normalized into a manifest",
      "recommended_fix": "Generate feature inventory with window start, window end and source cutoff before replay validation.",
      "remediation_required": true,
      "risk_category": "BACKFILL_RISK",
      "risk_id": "GTE-PIT-BACKFILL-01",
      "severity": "MATERIAL"
    },
    {
      "affected_feature_or_signal": "execution_signal_validity_policy",
      "evidence": "signal validity window exists as policy but natural signal expiry is not grounded",
      "recommended_fix": "Route valid-from / valid-until semantics and no-stale carry-forward checks to TRADING-2407.",
      "remediation_required": true,
      "risk_category": "STALE_SIGNAL_RISK",
      "risk_id": "GTE-PIT-STALE-01",
      "severity": "BLOCKING"
    },
    {
      "affected_feature_or_signal": "growth_tilt_engine_signal_artifact",
      "evidence": "no standalone signal artifact exposes as_of_date, generated_at, valid_from or valid_until",
      "recommended_fix": "Define the signal artifact schema before any severity downgrade.",
      "remediation_required": true,
      "risk_category": "AS_OF_MISSING_RISK",
      "risk_id": "GTE-PIT-ASOF-01",
      "severity": "BLOCKING"
    },
    {
      "affected_feature_or_signal": "risk_on_trend_filter_context",
      "evidence": "risk-on behavior and regime pass/failure evidence remain mixed in prior research artifacts",
      "recommended_fix": "Separate ex-ante trend / volatility conditions from ex-post regime labels and drawdown evaluation.",
      "remediation_required": true,
      "risk_category": "REGIME_CONFIRMATION_RISK",
      "risk_id": "GTE-PIT-REGIME-01",
      "severity": "MATERIAL"
    }
  ],
  "schema_version": "dynamic_strategy_growth_tilt_engine_pit_risk_audit.v1"
}
