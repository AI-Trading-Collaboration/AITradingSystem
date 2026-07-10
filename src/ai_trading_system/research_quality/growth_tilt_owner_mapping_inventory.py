from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

SCHEMA_VERSION = "growth_tilt_owner_mapping_inventory.v1"
SIGNAL_SCHEMA_VERSION = "growth_tilt_baseline_signal_inventory.v1"
CONFIRMATION_SCHEMA_VERSION = "growth_tilt_baseline_confirmation_inventory.v1"
VETO_SCHEMA_VERSION = "growth_tilt_baseline_veto_inventory.v1"
REGIME_SCHEMA_VERSION = "growth_tilt_baseline_regime_inventory.v1"
EXPOSURE_SCHEMA_VERSION = "growth_tilt_baseline_exposure_unit_inventory.v1"
TRACE_SCHEMA_VERSION = "growth_tilt_baseline_transition_trace_sample.v1"

REPORT_TYPE = "growth_tilt_owner_mapping_inventory"
READY_STATUS = "GROWTH_TILT_OWNER_MAPPING_INVENTORY_READY_OWNER_REVIEW_REQUIRED"
BLOCKED_STATUS = "GROWTH_TILT_OWNER_MAPPING_INVENTORY_BLOCKED_SOURCE_CONTRACT"
M2_BLOCKED_STATUS = "BLOCKED_UNRESOLVED_BASELINE_RUNTIME_MAPPING"
M2_READY_STATUS = "READY_FOR_OWNER_PREREGISTRATION"
NEXT_ROUTE = (
    "TRADING-2438M1C_GROWTH_TILT_BASELINE_RUNTIME_MAPPING_INVENTORY_AND_"
    "OWNER_PREREGISTRATION"
)

EXPECTED_SCHEMAS = {
    "channel_config": "channel_specific_first_layer_v3_config.v1",
    "signal_usage_matrix": "first_layer_signal_usage_matrix.v2",
    "final_matrix": "channel_specific_first_layer_v3_final_matrix.v1",
    "composer_config": "first_layer_composer_v2.v1",
    "base_policy": "base_overlay_veto_policy_schema.v1",
    "risk_veto_policy": "risk_on_veto_policy.v1",
    "probe_registry": "dynamic_second_layer_probe_registry.v2",
    "dynamic_allocation_policy": "etf_dynamic_allocation_policy_v1",
}
EXPECTED_VETO_IDS = (
    "risk_off_veto",
    "volatility_veto",
    "event_risk_veto",
    "trend_break_veto",
    "tqqq_veto",
)
EXPECTED_TREND_STATES = (
    "risk_off",
    "defensive",
    "neutral",
    "constructive",
    "risk_on",
)

REQUIRED_CATALOG_REFERENCES = (
    "growth-tilt-owner-mapping-inventory",
    "growth_tilt_baseline_transition_trace_source.csv",
    "baseline_signal_inventory.json",
    "baseline_confirmation_inventory.json",
    "baseline_veto_inventory.json",
    "baseline_regime_inventory.json",
    "baseline_exposure_unit_inventory.json",
    "baseline_transition_trace_sample.json",
)
REQUIRED_FLOW_REFERENCES = (
    "TRADING-2438M1C",
    READY_STATUS,
    M2_BLOCKED_STATUS,
    "re_risk_allowed_probability",
    "do_not_de_risk_pass=false",
)


def build_growth_tilt_owner_mapping_inventory(
    sources: Mapping[str, Any],
    *,
    baseline_prediction_rows: Sequence[Mapping[str, Any]] = (),
    channel_prediction_rows: Sequence[Mapping[str, Any]] = (),
    compiler_trace_rows: Sequence[Mapping[str, Any]] = (),
    source_artifacts: Sequence[Mapping[str, Any]] = (),
    report_registry: Mapping[str, Any] | None = None,
    artifact_catalog_text: str = "",
    system_flow_text: str = "",
    requirement_text: str = "",
    as_of: str,
) -> dict[str, Any]:
    strict_errors = _strict_validation_errors(
        sources,
        report_registry or {},
        artifact_catalog_text,
        system_flow_text,
        requirement_text,
    )
    channel_config = _mapping(sources.get("channel_config"))
    signal_usage = _mapping(sources.get("signal_usage_matrix"))
    final_matrix = _mapping(sources.get("final_matrix"))
    composer_config = _mapping(sources.get("composer_config"))
    base_policy = _mapping(sources.get("base_policy"))
    risk_veto_policy = _mapping(sources.get("risk_veto_policy"))
    probe_registry = _mapping(sources.get("probe_registry"))
    dynamic_policy = _mapping(sources.get("dynamic_allocation_policy"))
    channel_code = str(sources.get("channel_code_text") or "")
    compiler_code = str(sources.get("compiler_code_text") or "")
    expanded_code = str(
        sources.get("expanded_code_text") or sources.get("compiler_code_text") or ""
    )

    final_summary = _mapping(final_matrix.get("summary"))
    do_not_de_risk_pass = final_summary.get("do_not_de_risk_pass") is True
    risk_on_veto_pass = final_summary.get("risk_on_veto_pass") is True
    usage_by_signal = _records_by_key(
        _sequence(signal_usage.get("signals")), "signal_name"
    )

    baseline = _baseline_contract(
        base_policy,
        composer_config,
        channel_config,
    )
    signals = _signal_inventory(
        usage_by_signal,
        channel_config,
        channel_code,
        do_not_de_risk_pass=do_not_de_risk_pass,
        risk_on_veto_pass=risk_on_veto_pass,
    )
    confirmations = _confirmation_inventory(
        channel_config,
        dynamic_policy,
        channel_code,
        do_not_de_risk_pass=do_not_de_risk_pass,
    )
    vetoes = _veto_inventory(
        risk_veto_policy,
        channel_code,
        compiler_code,
        risk_on_veto_pass=risk_on_veto_pass,
    )
    regimes = _regime_inventory(
        probe_registry,
        baseline_prediction_rows,
        composer_config,
    )
    exposure = _exposure_inventory(base_policy, expanded_code)
    traces = _transition_trace_samples(
        baseline_prediction_rows,
        channel_prediction_rows,
        compiler_trace_rows,
    )

    eligible_recovery_signal_ids = [
        str(row["signal_id"])
        for row in signals
        if row.get("candidate_a_eligible") is True
    ]
    eligible_soft_confirmation_ids = [
        str(row["confirmation_id"])
        for row in confirmations
        if row.get("candidate_b_eligible") is True
    ]
    required_veto_ids = [
        str(item) for item in _sequence(risk_veto_policy.get("veto_types"))
    ]
    resolved_veto_ids = [
        str(row["veto_id"])
        for row in vetoes
        if row.get("required_by_baseline") is True
        and row.get("callable_runtime_source") is True
        and row.get("pit_approved") is True
        and row.get("semantic_binding_resolved") is True
    ]
    unresolved_veto_ids = sorted(set(required_veto_ids) - set(resolved_veto_ids))
    governed_regime_ids = [
        str(row["regime_id"])
        for row in regimes
        if row.get("transition_contract_status") == "GOVERNED"
    ]

    candidate_rows = [
        {
            "candidate_id": "recovery_reentry_speedup_guard",
            "mapping_ready": bool(
                eligible_recovery_signal_ids
                and not unresolved_veto_ids
                and governed_regime_ids
                and exposure.get("scalar_binding_ready") is True
                and baseline.get("recovery_persistence_contract_ready") is True
            ),
            "eligible_signal_ids": eligible_recovery_signal_ids,
            "required_hard_veto_ids": required_veto_ids,
            "unresolved_hard_veto_ids": unresolved_veto_ids,
            "blocker_codes": _candidate_a_blockers(
                eligible_recovery_signal_ids,
                unresolved_veto_ids,
                governed_regime_ids,
                baseline,
                exposure,
            ),
        },
        {
            "candidate_id": "false_risk_off_confirmation_relaxation",
            "mapping_ready": bool(
                len(eligible_soft_confirmation_ids) == 1
                and not unresolved_veto_ids
                and governed_regime_ids
                and exposure.get("scalar_binding_ready") is True
            ),
            "eligible_soft_confirmation_ids": eligible_soft_confirmation_ids,
            "required_hard_veto_ids": required_veto_ids,
            "unresolved_hard_veto_ids": unresolved_veto_ids,
            "blocker_codes": _candidate_b_blockers(
                eligible_soft_confirmation_ids,
                unresolved_veto_ids,
                governed_regime_ids,
                exposure,
            ),
        },
    ]
    mapping_ready_count = sum(row["mapping_ready"] is True for row in candidate_rows)
    mapping_blockers = sorted(
        {
            str(code)
            for row in candidate_rows
            for code in _sequence(row.get("blocker_codes"))
        }
    )
    m2_mapping_status = (
        M2_READY_STATUS if mapping_ready_count == len(candidate_rows) else M2_BLOCKED_STATUS
    )
    status = BLOCKED_STATUS if strict_errors else READY_STATUS
    safety = _safety()

    return {
        "schema_version": SCHEMA_VERSION,
        "task_id": "TRADING-2438M1C",
        "report_type": REPORT_TYPE,
        "status": status,
        "as_of": as_of,
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": "2022-12-01",
        "source_artifacts": [dict(item) for item in source_artifacts],
        "baseline": baseline,
        "do_not_de_risk_pass": do_not_de_risk_pass,
        "risk_on_veto_pass": risk_on_veto_pass,
        "candidate_mapping_readiness": candidate_rows,
        "owner_mapping_ready_count": mapping_ready_count,
        "owner_mapping_required_count": len(candidate_rows),
        "m2_mapping_status": m2_mapping_status,
        "m2_eligible_candidate_count": 0,
        "m2_eligible_candidate_ids": [],
        "mapping_blocker_count": len(mapping_blockers),
        "mapping_blocker_codes": mapping_blockers,
        "eligible_recovery_signal_ids": eligible_recovery_signal_ids,
        "eligible_soft_confirmation_ids": eligible_soft_confirmation_ids,
        "required_hard_veto_ids": required_veto_ids,
        "resolved_hard_veto_ids": resolved_veto_ids,
        "unresolved_hard_veto_ids": unresolved_veto_ids,
        "governed_regime_ids": governed_regime_ids,
        "strict_validation_errors": strict_errors,
        "strict_validation_error_count": len(strict_errors),
        "recommended_next_research_task": NEXT_ROUTE,
        "next_route": NEXT_ROUTE,
        "baseline_signal_inventory": {
            "schema_version": SIGNAL_SCHEMA_VERSION,
            "status": status,
            "baseline": baseline,
            "signals": signals,
            "eligible_recovery_signal_ids": eligible_recovery_signal_ids,
        },
        "baseline_confirmation_inventory": {
            "schema_version": CONFIRMATION_SCHEMA_VERSION,
            "status": status,
            "confirmations": confirmations,
            "eligible_soft_confirmation_ids": eligible_soft_confirmation_ids,
        },
        "baseline_veto_inventory": {
            "schema_version": VETO_SCHEMA_VERSION,
            "status": status,
            "vetoes": vetoes,
            "required_hard_veto_ids": required_veto_ids,
            "resolved_hard_veto_ids": resolved_veto_ids,
            "unresolved_hard_veto_ids": unresolved_veto_ids,
            "complete_callable_pit_valid_set_ready": not unresolved_veto_ids,
        },
        "baseline_regime_inventory": {
            "schema_version": REGIME_SCHEMA_VERSION,
            "status": status,
            "regimes": regimes,
            "governed_regime_ids": governed_regime_ids,
        },
        "baseline_exposure_unit_inventory": {
            "schema_version": EXPOSURE_SCHEMA_VERSION,
            "status": status,
            "exposure": exposure,
        },
        "baseline_transition_trace_sample": {
            "schema_version": TRACE_SCHEMA_VERSION,
            "status": status,
            "decision_trace_schema_version": baseline.get(
                "decision_trace_schema_version"
            ),
            "trace_sample_count": len(traces),
            "trace_samples": traces,
            "sample_role": "PRIOR_RESEARCH_ARTIFACT_EVIDENCE_ONLY",
        },
        "data_quality_gate_executed": False,
        "data_quality_gate_passed": None,
        "data_quality_status": "NOT_APPLICABLE_READ_ONLY_PRIOR_ARTIFACT_INVENTORY",
        "data_quality_gate_reason": (
            "M1C inventories versioned config, code contracts, and prior research "
            "artifacts only; it does not compute features, scores, backtests, or replay."
        ),
        **safety,
    }


def _baseline_contract(
    base_policy: Mapping[str, Any],
    composer_config: Mapping[str, Any],
    channel_config: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "config_id": base_policy.get("policy_id"),
        "config_version": base_policy.get("schema_version"),
        "composer_config_id": composer_config.get("policy_id"),
        "channel_config_id": channel_config.get("policy_id"),
        "decision_trace_schema_version": None,
        "evaluation_cadence": "TRADING_DAY_ROW_OBSERVED_NOT_GOVERNED",
        "exposure_unit": "QQQ_EQUIVALENT_EXPOSURE_QQQ_PLUS_3X_TQQQ",
        "target_exposure_field": "target_weights -> QQQ + 3*TQQQ",
        "current_exposure_field": "current_weights -> QQQ + 3*TQQQ",
        "runtime_entrypoint": (
            "ai_trading_system.two_layer_policy_compiler.compile_two_layer_policy"
        ),
        "binding_status": "BLOCKED_NO_GROWTH_TILT_BASELINE_RUNTIME_BINDING",
        "recovery_persistence_contract_ready": False,
        "recovery_persistence_blocker": (
            "No growth-tilt-bound baseline recovery persistence rule or versioned "
            "transition trace schema exists."
        ),
    }


def _signal_inventory(
    usage_by_signal: Mapping[str, Mapping[str, Any]],
    channel_config: Mapping[str, Any],
    channel_code: str,
    *,
    do_not_de_risk_pass: bool,
    risk_on_veto_pass: bool,
) -> list[dict[str, Any]]:
    channels = _mapping(channel_config.get("channels"))
    do_not = _mapping(channels.get("do_not_de_risk"))
    risk_veto = _mapping(channels.get("risk_on_veto"))
    rows = [
        _signal_row(
            "re_risk_allowed_probability",
            "outputs/research_trends/channel_specific_v3/"
            "channel_composer_v3_predictions.csv:re_risk_allowed_probability",
            "defensive",
            "drawdown_recovery",
            pit_approved=False,
            usage=_mapping(usage_by_signal.get("re_risk_allowed")),
            callable_runtime_source=(
                'frame["re_risk_allowed_probability"]' in channel_code
                and "_build_composer_predictions" in channel_code
            ),
            selection_status=(
                "PASS" if do_not_de_risk_pass else "FAILED_FINAL_SELECTION"
            ),
            candidate_a_eligible=False,
            threshold=_mapping(do_not.get("thresholds")).get(
                "do_not_de_risk_active_probability_min"
            ),
            offline_selection_pass=do_not_de_risk_pass,
            pit_lineage_valid=False,
            baseline_consumption_ready=False,
        ),
        _signal_row(
            "re_risk_allowed",
            "src/ai_trading_system/channel_specific_first_layer_v3.py:signal_state",
            "defensive",
            "drawdown_recovery",
            pit_approved=False,
            usage=_mapping(usage_by_signal.get("re_risk_allowed")),
            callable_runtime_source='"re_risk_allowed":' in channel_code,
            selection_status=(
                "PASS" if do_not_de_risk_pass else "FAILED_FINAL_SELECTION"
            ),
            candidate_a_eligible=False,
            threshold=0.55,
            offline_selection_pass=do_not_de_risk_pass,
            pit_lineage_valid=False,
            baseline_consumption_ready=False,
        ),
        _signal_row(
            "growth_allowed",
            "outputs/research_trends/channel_specific_v3/"
            "channel_composer_v3_predictions.csv:growth_allowed",
            "risk_veto",
            "volatility_compression+rates_liquidity",
            pit_approved=risk_on_veto_pass,
            usage=_mapping(usage_by_signal.get("growth_allowed")),
            callable_runtime_source='frame["growth_allowed"]' in channel_code,
            selection_status="PASS" if risk_on_veto_pass else "FAILED_FINAL_SELECTION",
            candidate_a_eligible=False,
            threshold=_mapping(risk_veto.get("thresholds")).get(
                "veto_active_probability_min"
            ),
            offline_selection_pass=risk_on_veto_pass,
            pit_lineage_valid=risk_on_veto_pass,
            baseline_consumption_ready=True,
        ),
        _signal_row(
            "first_layer_composer_v2_trend_state",
            "outputs/research_trends/models/"
            "first_layer_composer_v2_predictions.csv:trend_state",
            "first_layer_state",
            "multi_family_composer",
            pit_approved=True,
            usage={},
            callable_runtime_source=True,
            selection_status="FROZEN_RESEARCH_BASELINE",
            candidate_a_eligible=False,
            threshold=None,
            offline_selection_pass=True,
            pit_lineage_valid=True,
            baseline_consumption_ready=True,
        ),
    ]
    rows[0]["blocker_codes"] = [
        "RECOVERY_PIT_LINEAGE_CONTRACT_MISSING",
        "RECOVERY_BASELINE_CONSUMPTION_CONTRACT_MISSING",
        "RECOVERY_PERSISTENCE_CONTRACT_MISSING",
    ]
    rows[0]["selection_notes"] = [
        "do_not_de_risk_pass is an offline selection result, not a runtime value",
        "offline selection failure does not by itself invalidate the callable producer",
    ]
    return rows


def _signal_row(
    signal_id: str,
    output_path: str,
    channel: str,
    source_family: str,
    *,
    pit_approved: bool,
    usage: Mapping[str, Any],
    callable_runtime_source: bool,
    selection_status: str,
    candidate_a_eligible: bool,
    threshold: Any,
    offline_selection_pass: bool,
    pit_lineage_valid: bool,
    baseline_consumption_ready: bool,
) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "output_path": output_path,
        "channel": channel,
        "source_family": source_family,
        "pit_approved": pit_approved,
        "pit_lineage_valid": pit_lineage_valid,
        "allowed_usage": _sequence(usage.get("allowed_usage")),
        "blocked_usage": _sequence(usage.get("blocked_usage")),
        "diagnostic_only": usage.get("diagnostic_only"),
        "callable_runtime_source": callable_runtime_source,
        "producer_callable": callable_runtime_source,
        "output_path_resolved": bool(output_path),
        "semantics_registered": bool(usage),
        "baseline_consumption_ready": baseline_consumption_ready,
        "offline_selection_pass": offline_selection_pass,
        "offline_selection_role": "OFFLINE_SELECTION_RESULT_NOT_RUNTIME_VALUE",
        "selection_status": selection_status,
        "threshold": threshold,
        "candidate_a_eligible": candidate_a_eligible,
        "blocker_codes": [],
    }


def _confirmation_inventory(
    channel_config: Mapping[str, Any],
    dynamic_policy: Mapping[str, Any],
    channel_code: str,
    *,
    do_not_de_risk_pass: bool,
) -> list[dict[str, Any]]:
    do_not = _mapping(_mapping(channel_config.get("channels")).get("do_not_de_risk"))
    threshold = _mapping(do_not.get("thresholds")).get(
        "do_not_de_risk_active_probability_min"
    )
    rebalance = _mapping(dynamic_policy.get("rebalance_policy"))
    return [
        {
            "confirmation_id": "do_not_de_risk_same_row_neutralization_threshold",
            "transition": {
                "from": ["risk_off", "defensive"],
                "to": ["neutral"],
            },
            "hard_or_soft": "UNCLASSIFIED",
            "current_rule": (
                "do_not_de_risk_probability >= 0.55 changes baseline trend_state "
                "to neutral on the same row"
            ),
            "required_steps": 1,
            "threshold": threshold,
            "output_path": (
                "src/ai_trading_system/channel_specific_first_layer_v3.py:"
                "_do_not_de_risk_variant"
            ),
            "pit_approved": do_not_de_risk_pass,
            "callable_runtime_source": "_do_not_de_risk_variant" in channel_code,
            "sole_transition_cause_traceable": False,
            "baseline_binding_status": "RESEARCH_VARIANT_NOT_GROWTH_TILT_BASELINE",
            "candidate_b_eligible": False,
            "blocker_codes": [
                "CONFIRMATION_NOT_CLASSIFIED_SOFT",
                "SOLE_CAUSE_NOT_TRACEABLE",
                "DO_NOT_DE_RISK_CHANNEL_FAILED_FINAL_SELECTION",
            ],
        },
        {
            "confirmation_id": "dynamic_allocation_regime_confirmation_window",
            "transition": {"from": ["any"], "to": ["rebalance_candidate"]},
            "hard_or_soft": "UNCLASSIFIED",
            "current_rule": "confirmed_regime_days >= regime_confirmation_days",
            "required_steps": rebalance.get("regime_confirmation_days"),
            "threshold": rebalance.get("regime_confirmation_days"),
            "output_path": (
                "config/etf_portfolio/dynamic_allocation_policy.yaml:"
                "rebalance_policy.regime_confirmation_days"
            ),
            "pit_approved": False,
            "callable_runtime_source": True,
            "sole_transition_cause_traceable": False,
            "baseline_binding_status": "DIFFERENT_ASSET_UNIVERSE_AND_POLICY_FAMILY",
            "candidate_b_eligible": False,
            "blocker_codes": ["NOT_BOUND_TO_GROWTH_TILT_BASELINE"],
        },
    ]


def _veto_inventory(
    risk_veto_policy: Mapping[str, Any],
    channel_code: str,
    compiler_code: str,
    *,
    risk_on_veto_pass: bool,
) -> list[dict[str, Any]]:
    required = set(str(item) for item in _sequence(risk_veto_policy.get("veto_types")))
    producer = {
        "risk_off_veto": (
            '"risk_off_veto": row.get("growth_allowed") is False' in channel_code,
            risk_on_veto_pass,
            False,
            "AMBIGUOUS_ALIAS_OF_RISK_ON_VETO_NOT_BASELINE_RISK_OFF_STATE",
        ),
        "volatility_veto": (
            '"volatility_veto":' in channel_code,
            risk_on_veto_pass,
            True,
            "RESOLVED_CHANNEL_V3_VOLATILITY_VETO",
        ),
        "event_risk_veto": (
            False,
            False,
            False,
            "EVENT_RISK_FAMILY_PIT_BLOCKED_NO_RUNTIME_PRODUCER",
        ),
        "trend_break_veto": (
            False,
            False,
            False,
            "NO_CHANNEL_V3_RUNTIME_PRODUCER",
        ),
        "tqqq_veto": (
            '"tqqq_veto": True' in channel_code,
            True,
            True,
            "RESOLVED_CONSTANT_NO_TQQQ_GUARD",
        ),
    }
    rows: list[dict[str, Any]] = []
    for veto_id in EXPECTED_VETO_IDS:
        callable_source, pit_approved, semantic_resolved, binding_status = producer[veto_id]
        rows.append(
            {
                "veto_id": veto_id,
                "hard_or_soft": "HARD",
                "output_path": (
                    "ai_trading_system.two_layer_policy_compiler."
                    f"_active_veto:signal_state.{veto_id}"
                ),
                "priority": "BEFORE_GROWTH_OVERLAY",
                "pit_approved": pit_approved,
                "callable_runtime_source": bool(
                    callable_source and f'"{veto_id}"' in compiler_code
                ),
                "semantic_binding_resolved": semantic_resolved,
                "semantic_binding_status": binding_status,
                "required_by_baseline": veto_id in required,
            }
        )
    rows.append(
        {
            "veto_id": "rates_liquidity_veto",
            "hard_or_soft": "EXPECTED_SEMANTIC_CATEGORY_UNMAPPED",
            "output_path": (
                "channel_specific_v3.veto_reasons:rates_liquidity_unfavorable/"
                "rate_shock"
            ),
            "priority": "UNRESOLVED",
            "pit_approved": risk_on_veto_pass,
            "callable_runtime_source": False,
            "semantic_binding_resolved": False,
            "semantic_binding_status": (
                "NO_INDEPENDENT_BASELINE_VETO_ID_COLLAPSED_INTO_GROWTH_ALLOWED"
            ),
            "required_by_baseline": False,
        }
    )
    return rows


def _regime_inventory(
    probe_registry: Mapping[str, Any],
    baseline_rows: Sequence[Mapping[str, Any]],
    composer_config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    configured = [str(item) for item in _sequence(probe_registry.get("trend_states"))]
    observed = _observed_transitions(baseline_rows)
    rows: list[dict[str, Any]] = []
    for regime_id in EXPECTED_TREND_STATES:
        rows.append(
            {
                "regime_id": regime_id,
                "regime_type": "FIRST_LAYER_TREND_STATE",
                "output_path": (
                    "outputs/research_trends/models/"
                    "first_layer_composer_v2_predictions.csv:trend_state"
                ),
                "allowed_transitions": observed.get(regime_id, []),
                "transition_contract_status": "OBSERVED_NOT_GOVERNED",
                "configured": regime_id in configured,
                "state_precedence": _sequence(composer_config.get("state_precedence")),
            }
        )
    rows.append(
        {
            "regime_id": "ai_after_chatgpt",
            "regime_type": "MARKET_RESEARCH_WINDOW",
            "output_path": "config/research/channel_specific_first_layer_v3.yaml:market_regime",
            "allowed_transitions": [],
            "transition_contract_status": "NOT_A_PORTFOLIO_STATE",
            "configured": True,
            "state_precedence": [],
        }
    )
    return rows


def _exposure_inventory(
    base_policy: Mapping[str, Any], expanded_code: str
) -> dict[str, Any]:
    caps = _mapping(base_policy.get("caps"))
    return {
        "unit": "QQQ_EQUIVALENT_EXPOSURE",
        "formula": "QQQ_weight + 3.0 * TQQQ_weight",
        "formula_callable_runtime_source": (
            'weights.get("QQQ", 0.0) + 3.0 * weights.get("TQQQ", 0.0)'
            in expanded_code
            or "qqq + 3.0 * tqqq" in expanded_code
        ),
        "minimum_increment": None,
        "maximum_value": caps.get("QQQ_equivalent_exposure_max"),
        "baseline_transition_caps": {
            "TQQQ_max_weight": caps.get("TQQQ_max_weight"),
            "QQQ_equivalent_exposure_max": caps.get(
                "QQQ_equivalent_exposure_max"
            ),
            "turnover_max": caps.get("turnover_max"),
            "defensive_overlay_delta": base_policy.get("defensive_overlay_delta"),
        },
        "target_exposure_field": "target_weights -> derived QQQ equivalent",
        "current_exposure_field": "current_weights -> derived QQQ equivalent",
        "scalar_binding_ready": False,
        "blocker_codes": [
            "MINIMUM_EXPOSURE_INCREMENT_NOT_GOVERNED",
            "GROWTH_TILT_BASELINE_SCALAR_EXPOSURE_BINDING_MISSING",
        ],
    }


def _transition_trace_samples(
    baseline_rows: Sequence[Mapping[str, Any]],
    channel_rows: Sequence[Mapping[str, Any]],
    compiler_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    channel_by_date = {str(row.get("date")): dict(row) for row in channel_rows}
    compiler_by_date = {str(row.get("date")): dict(row) for row in compiler_rows}
    wanted = {
        ("defensive", "neutral"),
        ("neutral", "defensive"),
        ("constructive", "defensive"),
    }
    found: dict[tuple[str, str], dict[str, Any]] = {}
    previous: Mapping[str, Any] | None = None
    for raw in baseline_rows:
        row = dict(raw)
        if previous is not None:
            transition = (
                str(previous.get("trend_state")),
                str(row.get("trend_state")),
            )
            if transition in wanted and transition not in found:
                event_date = str(row.get("date"))
                found[transition] = {
                    "transition_id": f"{transition[0]}_to_{transition[1]}",
                    "previous_baseline_row": dict(previous),
                    "current_baseline_row": row,
                    "same_date_channel_row": channel_by_date.get(event_date),
                    "same_date_compiler_row": compiler_by_date.get(event_date),
                    "cause_attribution_ready": False,
                    "cause_attribution_blocker": (
                        "Prior artifacts do not emit a versioned transition-cause "
                        "contract tying one confirmation to the state change."
                    ),
                }
        previous = row
        if len(found) == len(wanted):
            break
    return [found[key] for key in sorted(found)]


def _observed_transitions(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[str]]:
    observed: dict[str, set[str]] = {}
    previous: str | None = None
    for row in rows:
        state = str(row.get("trend_state") or "")
        if previous and state and state != previous:
            observed.setdefault(previous, set()).add(state)
        if state:
            previous = state
    return {key: sorted(values) for key, values in observed.items()}


def _candidate_a_blockers(
    signals: Sequence[str],
    unresolved_vetoes: Sequence[str],
    governed_regimes: Sequence[str],
    baseline: Mapping[str, Any],
    exposure: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if not signals:
        blockers.append(
            "A_RECOVERY_PERMISSION_PIT_LINEAGE_AND_BASELINE_BINDING_UNRESOLVED"
        )
    if baseline.get("recovery_persistence_contract_ready") is not True:
        blockers.append("A_BASELINE_RECOVERY_PERSISTENCE_CONTRACT_UNRESOLVED")
    if unresolved_vetoes:
        blockers.append("A_COMPLETE_HARD_VETO_SET_UNRESOLVED")
    if not governed_regimes:
        blockers.append("A_GOVERNED_TRANSITION_SCOPE_UNRESOLVED")
    if exposure.get("scalar_binding_ready") is not True:
        blockers.append("A_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED")
    return blockers


def _candidate_b_blockers(
    confirmations: Sequence[str],
    unresolved_vetoes: Sequence[str],
    governed_regimes: Sequence[str],
    exposure: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    if len(confirmations) != 1:
        blockers.append("B_EXACTLY_ONE_CALLABLE_PIT_SOFT_CONFIRMATION_UNRESOLVED")
    if unresolved_vetoes:
        blockers.append("B_COMPLETE_HARD_VETO_SET_UNRESOLVED")
    if not governed_regimes:
        blockers.append("B_GOVERNED_TRANSITION_SCOPE_UNRESOLVED")
    if exposure.get("scalar_binding_ready") is not True:
        blockers.append("B_QQQ_EQUIVALENT_EXPOSURE_BINDING_UNRESOLVED")
    return blockers


def _strict_validation_errors(
    sources: Mapping[str, Any],
    report_registry: Mapping[str, Any],
    artifact_catalog_text: str,
    system_flow_text: str,
    requirement_text: str,
) -> list[str]:
    errors: list[str] = []
    for source_id, expected_schema in EXPECTED_SCHEMAS.items():
        source = _mapping(sources.get(source_id))
        if source.get("schema_version") != expected_schema:
            errors.append(f"source_schema_mismatch:{source_id}")
    risk_policy_ids = [
        str(item)
        for item in _sequence(_mapping(sources.get("risk_veto_policy")).get("veto_types"))
    ]
    if tuple(risk_policy_ids) != EXPECTED_VETO_IDS:
        errors.append("baseline_veto_identity_or_order_mismatch")
    trend_states = [
        str(item)
        for item in _sequence(_mapping(sources.get("probe_registry")).get("trend_states"))
    ]
    if tuple(trend_states) != EXPECTED_TREND_STATES:
        errors.append("baseline_trend_state_identity_or_order_mismatch")
    report_ids = {
        str(row.get("report_id"))
        for row in _sequence(report_registry.get("reports"))
        if isinstance(row, Mapping)
    }
    if REPORT_TYPE not in report_ids:
        errors.append("report_registry_alignment_failed")
    if not all(item in artifact_catalog_text for item in REQUIRED_CATALOG_REFERENCES):
        errors.append("artifact_catalog_alignment_failed")
    if not all(item in system_flow_text for item in REQUIRED_FLOW_REFERENCES):
        errors.append("system_flow_alignment_failed")
    if not all(
        item in requirement_text
        for item in (
            "TRADING-2438M1C",
            "Read-only baseline contract inventory",
            "Do not run real PIT replay yet",
            "re_risk_allowed_probability",
        )
    ):
        errors.append("requirement_alignment_failed")
    return sorted(set(errors))


def _records_by_key(rows: Sequence[Any], key: str) -> dict[str, Mapping[str, Any]]:
    return {
        str(row.get(key)): row
        for row in rows
        if isinstance(row, Mapping) and row.get(key)
    }


def _safety() -> dict[str, Any]:
    return {
        "validation_only": True,
        "read_only_inventory": True,
        "observe_only": True,
        "replay_run": False,
        "backtest_run": False,
        "scoring_run": False,
        "market_data_experiment_run": False,
        "fresh_market_data_read": False,
        "fresh_outcome_data_read": False,
        "owner_preregistration_completed": False,
        "policy_approval_recorded": False,
        "paper_shadow_allowed": False,
        "paper_shadow_enabled": False,
        "production_allowed": False,
        "production_enabled": False,
        "portfolio_weight_mutated": False,
        "generated_signal": False,
        "generated_trading_advice": False,
        "broker_enabled": False,
        "broker_action": "none",
        "production_effect": "none",
        "manual_review_required": True,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return list(value)
    return []
