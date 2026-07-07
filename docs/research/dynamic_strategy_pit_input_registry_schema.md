# Dynamic strategy PIT input registry schema

- status：`DYNAMIC_STRATEGY_PIT_COVERAGE_MATRIX_IMPLEMENTATION_PLAN_READY`

{
  "fields": {
    "as_of_field": {
      "required": false,
      "type": "string"
    },
    "generated_at_field": {
      "required": false,
      "type": "string"
    },
    "input_id": {
      "required": true,
      "type": "string"
    },
    "input_type": {
      "required": true,
      "type": "enum",
      "values": [
        "MARKET_DATA",
        "FEATURE",
        "SIGNAL",
        "EXECUTION_SEMANTIC",
        "REGIME_LABEL",
        "GATE_INPUT",
        "REPORTING_INPUT"
      ]
    },
    "owner_module": {
      "required": true,
      "type": "string"
    },
    "pit_confidence": {
      "required": true,
      "type": "enum",
      "values": [
        "HIGH",
        "MEDIUM",
        "LOW",
        "UNKNOWN"
      ]
    },
    "pit_status": {
      "required": true,
      "type": "enum",
      "values": [
        "TRUE_PIT",
        "APPROXIMATE_PIT",
        "NOT_PIT_SAFE",
        "UNKNOWN",
        "NOT_APPLICABLE"
      ]
    },
    "recommended_action": {
      "required": true,
      "type": "string"
    },
    "remediation_owner": {
      "required": false,
      "type": "string"
    },
    "risk_flags": {
      "required": false,
      "type": "list",
      "values": [
        "LOOKAHEAD_RISK",
        "REVISION_RISK",
        "STALE_DATA_RISK",
        "MISSING_DATA_RISK",
        "VALID_UNTIL_UNGROUNDED",
        "REGIME_LABEL_LOOKAHEAD_RISK",
        "THRESHOLD_UNCALIBRATED"
      ]
    },
    "severity": {
      "required": true,
      "type": "enum",
      "values": [
        "BLOCKING",
        "MATERIAL",
        "MINOR",
        "INFO"
      ]
    },
    "source_artifact_or_config": {
      "required": true,
      "type": "string"
    },
    "used_by": {
      "required": true,
      "type": "list"
    },
    "valid_from_field": {
      "required": false,
      "type": "string"
    },
    "valid_until_field": {
      "required": false,
      "type": "string"
    }
  },
  "planned_initial_entries": [
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "market_prices",
      "input_type": "MARKET_DATA",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "HIGH",
      "pit_status": "TRUE_PIT",
      "recommended_action": "keep as core market data input; retain validate-data link in reports",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "INFO",
      "source_artifact_or_config": "data/raw/prices_daily.csv; validate-data audit",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2402",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "adjusted_prices",
      "input_type": "MARKET_DATA",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "resolve TQQQ adjustment-ratio warning before promotion-quality ranking",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "data/raw/prices_daily.csv adjusted close fields",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "volume",
      "input_type": "MARKET_DATA",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "recommended_action": "include volume field lineage only if future signal uses it",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "LOOKAHEAD_RISK"
      ],
      "severity": "MINOR",
      "source_artifact_or_config": "price cache volume columns if present",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "returns",
      "input_type": "FEATURE",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "record feature window start/end and adjusted-price basis per candidate",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "derived from adjusted prices",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "volatility_inputs",
      "input_type": "FEATURE",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "add window end-date and no-forward-fill assertion to PIT matrix",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "rolling price-derived volatility features",
      "used_by": [
        "TRADING-2364",
        "TRADING-2386",
        "TRADING-2399"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "trend_features",
      "input_type": "FEATURE",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "make trend feature windows explicit before observation review",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "historical price trend / momentum windows",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "drawdown_features",
      "input_type": "FEATURE",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "separate live-available drawdown inputs from ex-post evaluation metrics",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "historical drawdown windows",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": true,
      "generated_at_field": "generated_at",
      "input_id": "growth_tilt_engine",
      "input_type": "SIGNAL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "recommended_action": "define source features, horizon, confidence, decay and valid-until lineage",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "LOOKAHEAD_RISK"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "TRADING-2386 signal family screening; TRADING-2399 gate matrix",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2402"
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
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "promote turnover guardrail to explicit execution constraint policy",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "candidate construction and targeted variants",
      "used_by": [
        "TRADING-2396",
        "TRADING-2399",
        "TRADING-2403"
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
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "build valid-from / valid-until PIT lineage and signal-age evidence",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "VALID_UNTIL_UNGROUNDED",
        "LOOKAHEAD_RISK"
      ],
      "severity": "BLOCKING",
      "source_artifact_or_config": "valid_until_window / validity_10d_v1 family",
      "used_by": [
        "TRADING-2364",
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2403"
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
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "UNKNOWN",
      "recommended_action": "define allowable lag and near-expiry handling before further retests",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TRADING-2399 execution metrics",
      "used_by": [
        "TRADING-2399",
        "TRADING-2402",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "stale_signal_detection",
      "input_type": "EXECUTION_SEMANTIC",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "extract stale signal detection into reusable signal audit",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "TRADING-2399 valid_until_stale_signal_evidence",
      "used_by": [
        "TRADING-2399",
        "TRADING-2402",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "regime_labels",
      "input_type": "REGIME_LABEL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "replace raw pass-rate with strategy-specific regime expectation scoring",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "risk_on/risk_off/high_volatility/low_volatility/trend_confirmed/recovery",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "as_of_field": "as_of",
      "candidate_search_blocker": false,
      "generated_at_field": "generated_at",
      "input_id": "gate_inputs",
      "input_type": "GATE_INPUT",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "MEDIUM",
      "pit_status": "NOT_APPLICABLE",
      "recommended_action": "normalize candidate x gate x decision history into meta-dataset",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "time/regime/drawdown/return/cost/turnover gate evidence",
      "used_by": [
        "TRADING-2386",
        "TRADING-2399",
        "TRADING-2402",
        "TRADING-2403"
      ],
      "valid_from_field": null,
      "valid_until_field": null
    },
    {
      "candidate_search_blocker": false,
      "input_id": "no_stale_signal_carry_forward",
      "input_type": "EXECUTION_SEMANTIC",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "regime_risk_on",
      "input_type": "REGIME_LABEL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "regime_risk_off",
      "input_type": "REGIME_LABEL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "regime_high_volatility",
      "input_type": "REGIME_LABEL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "regime_recovery",
      "input_type": "REGIME_LABEL",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "APPROXIMATE_PIT",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "time_slice_pass_rate",
      "input_type": "GATE_INPUT",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "NOT_APPLICABLE",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "regime_expectation_score",
      "input_type": "GATE_INPUT",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "NOT_APPLICABLE",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "REGIME_LABEL_LOOKAHEAD_RISK"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "drawdown_materiality",
      "input_type": "GATE_INPUT",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "NOT_APPLICABLE",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    },
    {
      "candidate_search_blocker": false,
      "input_id": "threshold_meta_dataset",
      "input_type": "GATE_INPUT",
      "owner_module": "dynamic_strategy_pit_coverage_matrix",
      "pit_confidence": "LOW",
      "pit_status": "NOT_APPLICABLE",
      "recommended_action": "include in reusable PIT registry implementation",
      "remediation_owner": "research_governance",
      "risk_flags": [
        "THRESHOLD_UNCALIBRATED"
      ],
      "severity": "MATERIAL",
      "source_artifact_or_config": "planned_registry_entry",
      "used_by": [
        "dynamic_strategy_research"
      ]
    }
  ],
  "recommended_path": "config/research/dynamic_strategy_pit_input_registry.yaml",
  "record_ready": true,
  "schema_version": "dynamic_strategy_pit_input_registry_schema.v1"
}
