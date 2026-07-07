# Dynamic strategy PIT coverage matrix reusable implementation

## Executive summary

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`
- PIT input registry created：`True`
- PIT coverage matrix generator ready：`True`
- PIT gate checker ready：`True`
- blocking gaps：`['growth_tilt_engine', 'valid_until_window']`
- candidate search allowed：`False`
- research-only observation allowed：`False`
- paper-shadow allowed：`False`
- production allowed：`False`
- next route：`TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan`
- data quality gate：not run；reason=`NOT_APPLICABLE_PRIOR_VALIDATED_ARTIFACT_AND_REGISTRY_ONLY_NO_FRESH_MARKET_DATA`

## Source findings from TRADING-2404

- source validation errors：`[]`
- policy note：`PIT gate is a policy-derived safety gate, not a statistically calibrated empirical threshold.`

## PIT input registry implementation

{
  "entries": [
    {
      "as_of_field": null,
      "candidate_search_blocker": true,
      "generated_at_field": null,
      "input_id": "growth_tilt_engine",
      "input_type": "SIGNAL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Review source features, as-of semantics, signal horizon, and PIT safety before resuming candidate search.",
      "remediation_owner": "TRADING-2406",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "SIGNAL_HORIZON_UNGROUNDED"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "TBD_TRADING_2406",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": true,
      "generated_at_field": "generated_at",
      "input_id": "valid_until_window",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "VALID_UNTIL_UNGROUNDED",
        "STALE_DATA_RISK"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "TBD_TRADING_2407",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": "valid_from",
      "valid_until_field": "valid_until"
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "no_stale_signal_carry_forward",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Make stale-signal suppression verifiable from generated signal metadata.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "STALE_DATA_RISK",
        "VALID_UNTIL_UNGROUNDED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2407",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": "valid_from",
      "valid_until_field": "valid_until"
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "signal_to_execution_lag",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep lag distribution visible in gate evidence before observation approval.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "targeted_gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "lower_turnover_guardrail",
      "input_type": "SIGNAL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep as guardrail-only until source signal and execution semantics are remediated.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "SIGNAL_HORIZON_UNGROUNDED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "recombination_candidate_plan",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "turnover_budgeting",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Preserve turnover budget evidence and cost visibility in reusable matrix output.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "recombination_candidate_plan",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "cooldown_balancing",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep cooldown logic auditable before any observation route.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "targeted_gate_evidence_retest",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_risk_on",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Replace coarse regime pass-rate with expectation-aware scoring.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_risk_off",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Make risk-off expectation and failure attribution explicit.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_high_volatility",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Ground high-volatility classification and expected behavior before observation.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_low_volatility",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep low-volatility label construction auditable.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_trend_confirmed",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Explicitly document trend confirmation timing and lookahead boundary.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_recovery",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Record recovery regime expectation and timing assumptions.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "time_slice_pass_rate",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "pit_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Keep gate-input threshold provenance visible until threshold meta-dataset exists.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_expectation_score",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "pit_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Implement expectation-aware regime scoring before using regime evidence positively.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED",
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "drawdown_materiality",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "pit_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Keep drawdown materiality threshold policy visible until calibrated.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "threshold_meta_dataset",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "pit_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Build historical candidate x gate x decision meta-dataset before statistical calibration.",
      "remediation_owner": "TRADING-2409",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2409",
      "used_by": [
        "dynamic_strategy_research",
        "calibrated_gate_review"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    }
  ],
  "entry_count": 17,
  "intended_effect": "Make PIT status, confidence, severity, blockers, and remediation ownership explicit before candidate search, observation, paper-shadow, or production interpretation can resume.\n",
  "owner": "research_governance",
  "path": "D:\\Work\\AITradingSystem\\config\\research\\dynamic_strategy_pit_input_registry.yaml",
  "rationale": "Registry-backed source of truth for dynamic strategy PIT coverage matrix generation and safety gate evaluation after TRADING-2404.\n",
  "review_condition": "Review after TRADING-2406 and TRADING-2407 remediation plans update the core return signal and valid-until semantics evidence.\n",
  "schema_version": "dynamic_strategy_pit_input_registry.v1",
  "scope": "dynamic_strategy",
  "status": "active_baseline",
  "validation_errors": [],
  "validation_evidence": "TRADING-2403 PIT coverage matrix and TRADING-2404 implementation plan identify growth_tilt_engine and valid_until_window as blocking gaps. TRADING-2405 adds reusable registry validation, matrix generation, and gate checks.\n",
  "validation_status": "PASS"
}

## PIT coverage matrix generator

{
  "broker_action": "none",
  "pit_coverage_matrix": [
    {
      "as_of_field": null,
      "candidate_search_blocker": true,
      "generated_at_field": null,
      "input_id": "growth_tilt_engine",
      "input_type": "SIGNAL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "point_in_time_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Review source features, as-of semantics, signal horizon, and PIT safety before resuming candidate search.",
      "remediation_owner": "TRADING-2406",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "SIGNAL_HORIZON_UNGROUNDED"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "TBD_TRADING_2406",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": true,
      "generated_at_field": "generated_at",
      "input_id": "valid_until_window",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "point_in_time_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "VALID_UNTIL_UNGROUNDED",
        "STALE_DATA_RISK"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "TBD_TRADING_2407",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": "valid_from",
      "valid_until_field": "valid_until"
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "no_stale_signal_carry_forward",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Make stale-signal suppression verifiable from generated signal metadata.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "STALE_DATA_RISK",
        "VALID_UNTIL_UNGROUNDED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2407",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": "valid_from",
      "valid_until_field": "valid_until"
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "signal_to_execution_lag",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep lag distribution visible in gate evidence before observation approval.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "targeted_gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "lower_turnover_guardrail",
      "input_type": "SIGNAL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep as guardrail-only until source signal and execution semantics are remediated.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "SIGNAL_HORIZON_UNGROUNDED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "recombination_candidate_plan",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "turnover_budgeting",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Preserve turnover budget evidence and cost visibility in reusable matrix output.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "recombination_candidate_plan",
      "used_by": [
        "dynamic_strategy_research",
        "recombination_candidates"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "cooldown_balancing",
      "input_type": "EXECUTION_SEMANTIC",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep cooldown logic auditable before any observation route.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "STALE_DATA_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "targeted_gate_evidence_retest",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_risk_on",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Replace coarse regime pass-rate with expectation-aware scoring.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_risk_off",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Make risk-off expectation and failure attribution explicit.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_high_volatility",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Ground high-volatility classification and expected behavior before observation.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_low_volatility",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Keep low-volatility label construction auditable.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_trend_confirmed",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Explicitly document trend confirmation timing and lookahead boundary.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_recovery",
      "input_type": "REGIME_LABEL",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "MEDIUM",
      "point_in_time_status": "APPROXIMATE_PIT",
      "production_blocker": true,
      "recommended_action": "Record recovery regime expectation and timing assumptions.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "time_slice_pass_rate",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "point_in_time_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Keep gate-input threshold provenance visible until threshold meta-dataset exists.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_expectation_score",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "point_in_time_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Implement expectation-aware regime scoring before using regime evidence positively.",
      "remediation_owner": "TRADING-2408",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED",
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2408",
      "used_by": [
        "dynamic_strategy_research",
        "regime_expectation_scoring"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "drawdown_materiality",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "point_in_time_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Keep drawdown materiality threshold policy visible until calibrated.",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "gate_evidence_matrix",
      "used_by": [
        "dynamic_strategy_research",
        "targeted_gate_evidence_retest"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "threshold_meta_dataset",
      "input_type": "GATE_INPUT",
      "observation_blocker": true,
      "owner_module": "dynamic_strategy",
      "paper_shadow_blocker": true,
      "pit_confidence": "UNKNOWN",
      "point_in_time_status": "NOT_APPLICABLE",
      "production_blocker": true,
      "recommended_action": "Build historical candidate x gate x decision meta-dataset before statistical calibration.",
      "remediation_owner": "TRADING-2409",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TBD_TRADING_2409",
      "used_by": [
        "dynamic_strategy_research",
        "calibrated_gate_review"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    }
  ],
  "production_effect": "none",
  "registry_path": "D:\\Work\\AITradingSystem\\config\\research\\dynamic_strategy_pit_input_registry.yaml",
  "registry_schema_version": "dynamic_strategy_pit_input_registry.v1",
  "registry_validation_status": "PASS",
  "row_count": 17,
  "schema_version": "dynamic_strategy_pit_coverage_matrix.v1",
  "scope": "dynamic_strategy"
}

## PIT severity gate checker

{
  "blockers": [
    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
    "BLOCKING_GAP_VALID_UNTIL_WINDOW"
  ],
  "broker_action": "none",
  "candidate_search": {
    "allowed": false,
    "blocked_if": [
      "any_required_input_severity_BLOCKING",
      "core_return_signal_pit_status_UNKNOWN_or_NOT_PIT_SAFE",
      "execution_validity_semantic_pit_status_UNKNOWN_or_NOT_PIT_SAFE"
    ],
    "reasons": [
      "BLOCKING_GAP_GROWTH_TILT_ENGINE",
      "BLOCKING_GAP_VALID_UNTIL_WINDOW",
      "REQUIRED_SIGNAL_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "REQUIRED_EXECUTION_SEMANTIC_PIT_STATUS_UNKNOWN_OR_NOT_SAFE"
    ]
  },
  "candidate_search_allowed": false,
  "gate_derivation_sources": {
    "empirical_status": [
      "policy_derived_safety_gate",
      "not_statistically_calibrated_yet",
      "threshold_meta_dataset_required_for_future_calibration"
    ],
    "phase_based": [
      "candidate search allows limited approximate PIT but not blocking core inputs",
      "research-only observation requires true or owner-approved approximate PIT",
      "paper-shadow requires stronger evidence",
      "production remains blocked"
    ],
    "principle_based": [
      "no lookahead",
      "no future outcome dependency",
      "no stale signal carry-forward without explicit rule"
    ],
    "role_based": [
      "core return signal has stricter threshold",
      "execution semantic has stricter threshold",
      "regime label affects evaluation but not necessarily signal generation",
      "reporting input has lower severity"
    ]
  },
  "paper_shadow": {
    "allowed": false,
    "blocked_if": [
      "research_only_observation_not_approved",
      "any_material_or_blocking_pit_gap",
      "owner_review_not_recorded"
    ],
    "reasons": [
      "RESEARCH_ONLY_OBSERVATION_NOT_APPROVED",
      "ANY_MATERIAL_OR_BLOCKING_PIT_GAP",
      "OWNER_REVIEW_NOT_RECORDED"
    ]
  },
  "paper_shadow_allowed": false,
  "policy_note": "PIT gate is a policy-derived safety gate, not a statistically calibrated empirical threshold.",
  "production": {
    "allowed": false,
    "blocked_if": [
      "current_phase_production_disabled"
    ],
    "reasons": [
      "CURRENT_PHASE_PRODUCTION_DISABLED"
    ]
  },
  "production_allowed": false,
  "production_effect": "none",
  "research_only_observation": {
    "allowed": false,
    "blocked_if": [
      "any_required_input_severity_BLOCKING",
      "valid_until_window_not_grounded",
      "stale_signal_rule_not_verifiable",
      "core_return_signal_not_true_or_owner_approved_approximate_pit"
    ],
    "reasons": [
      "BLOCKING_GAP_GROWTH_TILT_ENGINE",
      "BLOCKING_GAP_VALID_UNTIL_WINDOW",
      "REQUIRED_SIGNAL_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "REQUIRED_EXECUTION_SEMANTIC_PIT_STATUS_UNKNOWN_OR_NOT_SAFE",
      "VALID_UNTIL_WINDOW_NOT_GROUNDED",
      "STALE_SIGNAL_RULE_NOT_VERIFIABLE",
      "CORE_RETURN_SIGNAL_NOT_TRUE_OR_OWNER_APPROVED_APPROXIMATE_PIT"
    ]
  },
  "research_only_observation_allowed": false,
  "schema_version": "dynamic_strategy_pit_gate_result.v1",
  "scope": "dynamic_strategy"
}

## Current gate result

{
  "blocking_gaps": [
    "growth_tilt_engine",
    "valid_until_window"
  ],
  "candidate_search_allowed": false,
  "paper_shadow_allowed": false,
  "production_allowed": false,
  "research_only_observation_allowed": false
}

## Blocking gaps

{
  "blockers": [
    "BLOCKING_GAP_GROWTH_TILT_ENGINE",
    "BLOCKING_GAP_VALID_UNTIL_WINDOW"
  ],
  "blocking_gap_details": {
    "growth_tilt_engine": {
      "candidate_search_blocker": true,
      "observation_blocker": true,
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Review source features, as-of semantics, signal horizon, and PIT safety before resuming candidate search.",
      "remediation_owner": "TRADING-2406",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "SIGNAL_HORIZON_UNGROUNDED"
      ],
      "severity": "BLOCKING"
    },
    "valid_until_window": {
      "candidate_search_blocker": true,
      "observation_blocker": true,
      "paper_shadow_blocker": true,
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "production_blocker": true,
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "remediation_owner": "TRADING-2407",
      "risk_flags": [
        "LOOKAHEAD_RISK",
        "VALID_UNTIL_UNGROUNDED",
        "STALE_DATA_RISK"
      ],
      "severity": "BLOCKING"
    }
  },
  "blocking_gaps": [
    "growth_tilt_engine",
    "valid_until_window"
  ],
  "broker_action": "none",
  "candidate_search_allowed": false,
  "paper_shadow_allowed": false,
  "production_allowed": false,
  "production_effect": "none",
  "research_only_observation_allowed": false,
  "schema_version": "dynamic_strategy_pit_blocker_summary.v1",
  "scope": "dynamic_strategy"
}

## Remediation routes

{
  "broker_action": "none",
  "production_effect": "none",
  "recommended_next_research_task": "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan",
  "route_reason": "growth_tilt_engine is the core return-engine blocking PIT gap",
  "routes": {
    "growth_tilt_engine": {
      "candidate_search_blocker": true,
      "next_task": "TRADING-2406_Growth_Tilt_Engine_PIT_And_Signal_Construction_Remediation_Plan",
      "recommended_action": "Review source features, as-of semantics, signal horizon, and PIT safety before resuming candidate search.",
      "severity": "BLOCKING"
    },
    "regime_expectation_scoring": {
      "next_task": "TRADING-2408_Regime_Expectation_Scoring_Implementation_Plan",
      "severity": "MATERIAL"
    },
    "threshold_meta_dataset": {
      "next_task": "TRADING-2409_Threshold_Meta_Dataset_Implementation_Plan",
      "severity": "MATERIAL"
    },
    "valid_until_window": {
      "candidate_search_blocker": true,
      "next_task": "TRADING-2407_Valid_Until_Window_Semantics_And_Stale_Signal_Remediation_Plan",
      "recommended_action": "Ground valid-from, valid-until, expiry, stale-signal carry-forward, and near-expiry behavior before candidate search.",
      "severity": "BLOCKING"
    }
  },
  "schema_version": "dynamic_strategy_pit_remediation_routes.v1"
}

## Explicit non-approval list

- `candidate_search_resume`
- `candidate_auto_accept`
- `research_only_observation`
- `paper_shadow`
- `paper_trade`
- `shadow_position`
- `event_append`
- `outcome_binding`
- `scheduler`
- `scheduled_task`
- `daily_report`
- `production`
- `broker_order`
- `new_strategy_backtest`
- `new_trading_signal`
- `new_scoring`
- `clear_blocking_gap_without_evidence`
