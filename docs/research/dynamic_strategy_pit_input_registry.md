# Dynamic strategy PIT input registry

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_REUSABLE_IMPLEMENTATION_READY`
- registry path：`D:\Work\AITradingSystem\config\research\dynamic_strategy_pit_input_registry.yaml`

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
