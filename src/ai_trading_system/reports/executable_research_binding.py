from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import (
    dynamic_v3_signal_input_completeness as signal_inputs,
)
from ai_trading_system.reports import next_research_cycle, return_to_research_reset

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
BROKER_EFFECT = "none"
ORDER_EFFECT = "none"
PASS_STATUS = "PASS"
FAIL_STATUS = "FAIL"
MARKET_REGIME = "ai_after_chatgpt"
AI_REGIME_START = "2022-12-01"

CONTRACT_REPORT_TYPE = "next_candidate_executable_binding_contract"
VALIDATION_SUFFIX = "_validation"
CONTRACT_VALIDATION_REPORT_TYPE = f"{CONTRACT_REPORT_TYPE}{VALIDATION_SUFFIX}"
SIGNAL_BINDING_REPORT_TYPE = "next_candidate_signal_binding"
SIGNAL_BINDING_VALIDATION_REPORT_TYPE = f"{SIGNAL_BINDING_REPORT_TYPE}{VALIDATION_SUFFIX}"
WEIGHT_BINDING_REPORT_TYPE = "next_candidate_research_weight_binding"
WEIGHT_BINDING_VALIDATION_REPORT_TYPE = f"{WEIGHT_BINDING_REPORT_TYPE}{VALIDATION_SUFFIX}"
SAFETY_AUDIT_REPORT_TYPE = "executable_binding_safety_audit"
SAFETY_AUDIT_VALIDATION_REPORT_TYPE = f"{SAFETY_AUDIT_REPORT_TYPE}{VALIDATION_SUFFIX}"
BINDING_VERSION = "next_candidate_executable_binding_contract_v1"
SIGNAL_BINDING_VERSION = "next_candidate_signal_binding_v1"
WEIGHT_BINDING_VERSION = "next_candidate_research_weight_binding_v1"
INPUT_SCHEMA_ID = "next_candidate_executable_binding_input_v1"
OUTPUT_SCHEMA_ID = "next_candidate_executable_binding_output_v1"
SIGNAL_BINDING_OUTPUT_SCHEMA_ID = "next_candidate_signal_binding_output_v1"
WEIGHT_BINDING_OUTPUT_SCHEMA_ID = "next_candidate_research_weight_binding_output_v1"
SIGNAL_BINDING_COMPLETE = "CANDIDATE_SIGNAL_BINDING_COMPLETE"
SIGNAL_BINDING_COMPLETE_WITH_WARNINGS = "CANDIDATE_SIGNAL_BINDING_COMPLETE_WITH_WARNINGS"
SIGNAL_BINDING_BLOCKED = "CANDIDATE_SIGNAL_BINDING_BLOCKED"
WEIGHT_BINDING_COMPLETE = "CANDIDATE_RESEARCH_WEIGHT_BINDING_COMPLETE"
WEIGHT_BINDING_COMPLETE_WITH_WARNINGS = (
    "CANDIDATE_RESEARCH_WEIGHT_BINDING_COMPLETE_WITH_WARNINGS"
)
WEIGHT_BINDING_BLOCKED = "CANDIDATE_RESEARCH_WEIGHT_BINDING_BLOCKED"
SAFETY_PASS = "EXECUTABLE_BINDING_SAFETY_PASS"
SAFETY_WARNING = "EXECUTABLE_BINDING_SAFETY_WARNING"
SAFETY_BLOCKED = "EXECUTABLE_BINDING_SAFETY_BLOCKED"
DEFAULT_SIGNAL_INPUT_POLICY_PATH = (
    signal_inputs.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH
)
DEFAULT_SIGNAL_BINDING_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "next_candidate_signal_binding_v1.yaml"
)
DEFAULT_WEIGHT_BINDING_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "next_candidate_research_weight_binding_v1.yaml"
)
RESEARCH_SIGNAL_SYMBOLS: tuple[str, ...] = ("QQQ", "SMH", "SOXX")
REQUIRED_SIGNAL_SYMBOLS: tuple[str, ...] = (*RESEARCH_SIGNAL_SYMBOLS, "SPY")

REPORT_PREFIXES: dict[str, str] = {
    CONTRACT_REPORT_TYPE: "next_candidate_executable_binding_contract",
    CONTRACT_VALIDATION_REPORT_TYPE: "next_candidate_executable_binding_contract_validation",
    SIGNAL_BINDING_REPORT_TYPE: "next_candidate_signal_binding",
    SIGNAL_BINDING_VALIDATION_REPORT_TYPE: "next_candidate_signal_binding_validation",
    WEIGHT_BINDING_REPORT_TYPE: "next_candidate_research_weight_binding",
    WEIGHT_BINDING_VALIDATION_REPORT_TYPE: (
        "next_candidate_research_weight_binding_validation"
    ),
    SAFETY_AUDIT_REPORT_TYPE: "executable_binding_safety_audit",
    SAFETY_AUDIT_VALIDATION_REPORT_TYPE: "executable_binding_safety_audit_validation",
}

REQUIRED_OUTPUT_TYPES: tuple[str, ...] = (
    "signal_score",
    "regime_state",
    "risk_state",
    "rotation_state",
    "hypothetical_research_weight",
    "confidence_uncertainty",
    "blocking_reason",
)

FORBIDDEN_OUTPUT_TYPES: tuple[str, ...] = (
    "official_target_weights",
    "broker_order",
    "order_ticket",
    "live_allocation",
    "production_target_weight",
    "paper_shadow_activation",
    "extended_shadow_approval",
    "owner_decision_append",
    "production_state_mutation",
)


def default_executable_binding_json_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.json"


def default_executable_binding_markdown_path(
    report_type: str,
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"{REPORT_PREFIXES[report_type]}_{as_of.isoformat()}.md"


def latest_executable_binding_json_path(report_type: str, output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, f"{REPORT_PREFIXES[report_type]}_", ".json")


def build_next_candidate_executable_binding_contract_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    frozen_path = next_research_cycle.default_next_research_cycle_json_path(
        next_research_cycle.FROZEN_SPEC_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    snapshot_path = next_research_cycle.default_next_research_cycle_json_path(
        next_research_cycle.CYCLE_SNAPSHOT_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    plan_path = return_to_research_reset.default_return_to_research_json_path(
        return_to_research_reset.RESEARCH_BACKFILL_PLAN_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    gate_path = next_research_cycle.default_next_research_cycle_json_path(
        next_research_cycle.RESEARCH_GATE_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    frozen = _read_json_mapping(frozen_path)
    snapshot = _read_json_mapping(snapshot_path)
    plan = _read_json_mapping(plan_path)
    gate = _read_json_mapping(gate_path)
    frozen_spec = _mapping(frozen.get("frozen_candidate_spec"))
    frozen_summary = _mapping(frozen.get("summary"))
    snapshot_summary = _mapping(snapshot.get("summary"))
    candidate_id = _text(
        frozen_spec.get("candidate_id"),
        _text(snapshot_summary.get("candidate_id"), "MISSING"),
    )
    windows = _records(frozen_spec.get("validation_windows")) or _records(
        plan.get("required_backfill_windows")
    )
    requested_range = _text(
        frozen.get("requested_date_range"),
        _date_range_from_windows(windows),
    )
    source_inputs = _source_inputs_from_frozen_spec(frozen_spec)
    input_schema = _input_schema(
        candidate_id=candidate_id,
        frozen_spec=frozen_spec,
        source_inputs=source_inputs,
    )
    output_schema = _output_schema(candidate_id=candidate_id)
    binding_contract = {
        "candidate_id": candidate_id,
        "binding_version": BINDING_VERSION,
        "contract_status": "CONTRACT_DEFINED_ONLY",
        "input_schema": input_schema,
        "output_schema": output_schema,
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
        "strategy_behavior_implemented": False,
        "signal_binding_implemented": False,
        "weight_binding_implemented": False,
        "paper_shadow_activation_allowed": False,
        "allowed_research_only_state_outputs": list(REQUIRED_OUTPUT_TYPES),
        "allowed_hypothetical_allocation_outputs": [
            "hypothetical_research_weight",
            "previous_hypothetical_weight",
            "rotation_delta",
            "turnover_proxy",
            "constraint_hit",
        ],
        "forbidden_outputs": list(FORBIDDEN_OUTPUT_TYPES),
    }
    summary = {
        "contract_status": "EXECUTABLE_BINDING_CONTRACT_READY",
        "candidate_id": candidate_id,
        "binding_version": BINDING_VERSION,
        "input_schema_id": INPUT_SCHEMA_ID,
        "output_schema_id": OUTPUT_SCHEMA_ID,
        "source_research_gate_decision": _text(
            _mapping(gate.get("summary")).get("research_gate_decision"),
            _text(gate.get("status"), "MISSING"),
        ),
        "source_cycle_snapshot_status": _text(
            snapshot_summary.get("research_cycle_snapshot_status"),
            _text(snapshot.get("status"), "MISSING"),
        ),
        "market_regime": _text(frozen_summary.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_range,
        "required_signal_input_count": len(input_schema["required_signal_inputs"]),
        "required_feature_input_count": len(input_schema["required_feature_inputs"]),
        "allowed_output_count": len(REQUIRED_OUTPUT_TYPES),
        "forbidden_output_count": len(FORBIDDEN_OUTPUT_TYPES),
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }
    return _payload(
        report_type=CONTRACT_REPORT_TYPE,
        as_of=as_of,
        status="EXECUTABLE_BINDING_CONTRACT_READY",
        purpose=(
            "Define the executable research-only binding contract for the frozen "
            "next candidate without implementing strategy behavior."
        ),
        input_artifacts={
            "next_candidate_spec_frozen": str(frozen_path),
            "next_candidate_research_cycle_snapshot": str(snapshot_path),
            "research_backfill_plan_for_next_candidate": str(plan_path),
            "next_candidate_research_gate": str(gate_path),
        },
        output_decision="EXECUTABLE_BINDING_CONTRACT_READY",
        summary=summary,
        body={
            "binding_contract": binding_contract,
            "source_statuses": [
                _source_status("next_candidate_spec_frozen", frozen),
                _source_status("next_candidate_research_cycle_snapshot", snapshot),
                _source_status("research_backfill_plan_for_next_candidate", plan),
                _source_status("next_candidate_research_gate", gate),
            ],
            "hard_stop_conditions": [
                "stop_if_signal_binding_cannot_consume_validated_inputs",
                "stop_if_weight_binding_risks_official_target_weight_semantics",
                "stop_if_executable_binding_safety_audit_fails",
                "stop_if_backfill_cannot_compute_real_metrics",
            ],
        },
        reader_brief=_reader_brief(
            summary=(
                "Executable binding contract is defined; strategy behavior is not "
                "implemented yet."
            ),
            key_result="EXECUTABLE_BINDING_CONTRACT_READY",
            blocking_issues="strategy_behavior_not_implemented_by_contract_task",
            warnings="contract_only_no_metrics_or_weights",
            next_action="implement_research_only_signal_binding",
        ),
        next_action="implement_research_only_signal_binding",
        safety_boundary=_safety_boundary(),
        limitations=[
            "TRADING-460 defines the contract only; it does not compute signals.",
            "Hypothetical research weights are schema-permitted only, not produced yet.",
        ],
        requested_date_range=requested_range,
    )


def build_next_candidate_signal_binding_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    project_root: Path = PROJECT_ROOT,
    signal_input_policy_path: Path = signal_inputs.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    signal_binding_policy_path: Path = DEFAULT_SIGNAL_BINDING_POLICY_PATH,
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    contract_path = default_executable_binding_json_path(
        CONTRACT_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    contract_validation_path = default_executable_binding_json_path(
        CONTRACT_VALIDATION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    frozen_path = next_research_cycle.default_next_research_cycle_json_path(
        next_research_cycle.FROZEN_SPEC_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    contract_payload = _read_json_mapping(contract_path)
    contract_validation = _read_json_mapping(contract_validation_path)
    frozen = _read_json_mapping(frozen_path)
    contract = _mapping(contract_payload.get("binding_contract"))
    frozen_spec = _mapping(frozen.get("frozen_candidate_spec"))
    windows = _records(frozen_spec.get("validation_windows"))
    requested_range = _text(
        frozen.get("requested_date_range"),
        _date_range_from_windows(windows),
    )
    candidate_id = _text(
        contract.get("candidate_id"),
        _text(frozen_spec.get("candidate_id"), "MISSING"),
    )
    normalized_data_quality = _normalize_data_quality_gate(data_quality_gate)
    signal_input_policy = signal_inputs.load_signal_input_completeness_policy(
        signal_input_policy_path
    )
    signal_binding_policy = _json_safe(
        _load_signal_binding_policy(signal_binding_policy_path)
    )
    input_findings = signal_inputs._signal_input_findings(  # noqa: SLF001
        policy=signal_input_policy,
        as_of=as_of,
    )
    input_summary = signal_inputs._signal_input_summary(  # noqa: SLF001
        policy=signal_input_policy,
        findings=input_findings,
    )
    blocking_reasons = _signal_binding_blocking_reasons(
        contract_validation=contract_validation,
        data_quality_gate=normalized_data_quality,
        input_summary=input_summary,
        input_findings=input_findings,
    )
    signal_rows: list[dict[str, Any]] = []
    if not blocking_reasons:
        signal_rows, window_blocking_reasons = _candidate_signal_series(
            candidate_id=candidate_id,
            signal_input_policy=signal_input_policy,
            signal_binding_policy=signal_binding_policy,
            validation_windows=windows,
            project_root=project_root,
        )
        blocking_reasons.extend(window_blocking_reasons)
    signal_state = _signal_state_from_series(
        candidate_id=candidate_id,
        signal_rows=signal_rows,
        blocking_reasons=blocking_reasons,
    )
    warning_reasons = _signal_binding_warning_reasons(
        data_quality_gate=normalized_data_quality,
        input_summary=input_summary,
        signal_state=signal_state,
    )
    if blocking_reasons:
        status = SIGNAL_BINDING_BLOCKED
    elif warning_reasons:
        status = SIGNAL_BINDING_COMPLETE_WITH_WARNINGS
    else:
        status = SIGNAL_BINDING_COMPLETE
    output_decision = status
    latest_signal_date = _text(signal_state.get("signal_date"))
    confidence_uncertainty = _mapping(signal_state.get("confidence_uncertainty"))
    summary = {
        "signal_binding_status": status,
        "candidate_id": candidate_id,
        "binding_version": SIGNAL_BINDING_VERSION,
        "source_contract_version": _text(contract.get("binding_version")),
        "market_regime": _text(
            _mapping(frozen.get("summary")).get("market_regime"),
            MARKET_REGIME,
        ),
        "requested_date_range": requested_range,
        "signal_row_count": len(signal_rows),
        "latest_signal_date": latest_signal_date,
        "latest_signal_score": signal_state.get("signal_score"),
        "regime_state": _text(signal_state.get("regime_state"), "blocked"),
        "risk_state": _text(signal_state.get("risk_state"), "blocked"),
        "rotation_state": _text(signal_state.get("rotation_state"), "blocked"),
        "confidence": _text(confidence_uncertainty.get("confidence")),
        "uncertainty": _text(confidence_uncertainty.get("uncertainty")),
        "blocking_reason": _join_reasons(blocking_reasons),
        "warning_reason": _join_reasons(warning_reasons),
        "contract_validation_status": _text(contract_validation.get("status"), "MISSING"),
        "data_quality_status": _text(normalized_data_quality.get("status"), "MISSING"),
        "data_quality_passed": normalized_data_quality.get("passed") is True,
        "signal_input_status": _text(input_summary.get("signal_input_status"), "MISSING"),
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }
    return _payload(
        report_type=SIGNAL_BINDING_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Transform validated frozen next-candidate feature and signal inputs "
            "into research-only signal states without producing weights or metrics."
        ),
        input_artifacts={
            "next_candidate_executable_binding_contract": str(contract_path),
            "next_candidate_executable_binding_contract_validation": str(
                contract_validation_path
            ),
            "next_candidate_spec_frozen": str(frozen_path),
            "signal_input_policy": str(signal_input_policy_path),
            "signal_binding_policy": str(signal_binding_policy_path),
            "data_quality_report": _text(normalized_data_quality.get("report_path")),
        },
        output_decision=output_decision,
        summary=summary,
        body={
            "signal_binding": {
                "candidate_id": candidate_id,
                "binding_version": SIGNAL_BINDING_VERSION,
                "source_contract_version": _text(contract.get("binding_version")),
                "output_schema_id": SIGNAL_BINDING_OUTPUT_SCHEMA_ID,
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": BROKER_EFFECT,
                "order_effect": ORDER_EFFECT,
                "hypothetical_research_weight_produced": False,
                "backfill_metrics_produced": False,
            },
            "candidate_signal_series": signal_rows,
            "signal_state": signal_state,
            "confidence_uncertainty": _mapping(
                signal_state.get("confidence_uncertainty")
            ),
            "blocking_reasons": list(blocking_reasons),
            "warning_reasons": list(warning_reasons),
            "data_quality_gate": normalized_data_quality,
            "signal_input_summary": dict(input_summary),
            "signal_input_findings": [dict(row) for row in input_findings],
            "signal_binding_policy": signal_binding_policy,
            "validation_windows": [dict(row) for row in windows],
        },
        reader_brief=_reader_brief(
            summary=(
                "Research-only signal binding transformed validated inputs into "
                f"{len(signal_rows)} candidate signal row(s)."
            ),
            key_result=status,
            blocking_issues=_join_reasons(blocking_reasons) or "none",
            warnings=_join_reasons(warning_reasons) or "none",
            next_action=(
                "repair_signal_binding_inputs"
                if blocking_reasons
                else "implement_research_only_weight_binding"
            ),
        ),
        next_action=(
            "repair_signal_binding_inputs"
            if blocking_reasons
            else "implement_research_only_weight_binding"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "executable_research_signal_binding",
            "signal_binding_implemented": not bool(blocking_reasons),
            "hypothetical_research_weights_generated": False,
            "backfill_metrics_generated": False,
        },
        limitations=[
            "TRADING-461 outputs signal state only; it does not generate weights.",
            "Backfill return, drawdown, turnover, cost, and benchmark metrics remain uncomputed.",
            "Historical backfill availability is evaluated by later tasks.",
        ],
        requested_date_range=requested_range,
        methodology_overrides={
            "collector_mode": "read_validated_signal_and_feature_inputs",
            "contract_only": False,
            "does_not_implement_strategy_behavior": False,
            "signal_binding_implemented": not bool(blocking_reasons),
            "does_not_generate_hypothetical_weights": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def build_next_candidate_research_weight_binding_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    weight_binding_policy_path: Path = DEFAULT_WEIGHT_BINDING_POLICY_PATH,
    data_quality_gate: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    signal_path = default_executable_binding_json_path(
        SIGNAL_BINDING_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    signal_validation_path = default_executable_binding_json_path(
        SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    signal_payload = _read_json_mapping(signal_path)
    signal_validation = _read_json_mapping(signal_validation_path)
    signal_summary = _mapping(signal_payload.get("summary"))
    candidate_id = _text(signal_summary.get("candidate_id"), "MISSING")
    requested_range = _text(signal_payload.get("requested_date_range"), "not_applicable")
    normalized_data_quality = _normalize_data_quality_gate(data_quality_gate)
    policy = _json_safe(_load_signal_binding_policy(weight_binding_policy_path))
    blocking_reasons = _weight_binding_blocking_reasons(
        signal_payload=signal_payload,
        signal_validation=signal_validation,
        data_quality_gate=normalized_data_quality,
    )
    weight_series: list[dict[str, Any]] = []
    if not blocking_reasons:
        weight_series, constraint_blocking = _research_weight_series(
            candidate_id=candidate_id,
            signal_rows=_records(signal_payload.get("candidate_signal_series")),
            policy=policy,
        )
        blocking_reasons.extend(constraint_blocking)
    latest_weight_state = _latest_weight_state(
        candidate_id=candidate_id,
        weight_series=weight_series,
        blocking_reasons=blocking_reasons,
        policy=policy,
    )
    warning_reasons = _weight_binding_warning_reasons(
        data_quality_gate=normalized_data_quality,
        signal_payload=signal_payload,
        weight_series=weight_series,
    )
    if blocking_reasons:
        status = WEIGHT_BINDING_BLOCKED
    elif warning_reasons:
        status = WEIGHT_BINDING_COMPLETE_WITH_WARNINGS
    else:
        status = WEIGHT_BINDING_COMPLETE
    summary = {
        "research_weight_binding_status": status,
        "candidate_id": candidate_id,
        "binding_version": WEIGHT_BINDING_VERSION,
        "source_signal_binding_version": _text(signal_summary.get("binding_version")),
        "market_regime": _text(signal_payload.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_range,
        "weight_row_count": len(weight_series),
        "latest_signal_date": _text(latest_weight_state.get("signal_date")),
        "risk_state": _text(latest_weight_state.get("risk_state"), "blocked"),
        "rotation_state": _text(latest_weight_state.get("rotation_state"), "blocked"),
        "turnover_proxy": latest_weight_state.get("turnover_proxy"),
        "constraint_hit_count": len(_records(latest_weight_state.get("constraint_hit"))),
        "blocking_reason": _join_reasons(blocking_reasons),
        "warning_reason": _join_reasons(warning_reasons),
        "signal_binding_status": _text(signal_payload.get("status"), "MISSING"),
        "signal_binding_validation_status": _text(signal_validation.get("status"), "MISSING"),
        "data_quality_status": _text(normalized_data_quality.get("status"), "MISSING"),
        "data_quality_passed": normalized_data_quality.get("passed") is True,
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }
    return _payload(
        report_type=WEIGHT_BINDING_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Convert validated research-only signal state into hypothetical "
            "research weight output without producing official target weights."
        ),
        input_artifacts={
            "next_candidate_signal_binding": str(signal_path),
            "next_candidate_signal_binding_validation": str(signal_validation_path),
            "research_weight_binding_policy": str(weight_binding_policy_path),
            "data_quality_report": _text(normalized_data_quality.get("report_path")),
        },
        output_decision=status,
        summary=summary,
        body={
            "research_weight_binding": {
                "candidate_id": candidate_id,
                "binding_version": WEIGHT_BINDING_VERSION,
                "source_signal_binding_version": _text(signal_summary.get("binding_version")),
                "output_schema_id": WEIGHT_BINDING_OUTPUT_SCHEMA_ID,
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": BROKER_EFFECT,
                "order_effect": ORDER_EFFECT,
                "paper_shadow_activation_produced": False,
                "broker_order_produced": False,
                "backfill_metrics_produced": False,
            },
            "hypothetical_research_weight": latest_weight_state[
                "hypothetical_research_weight"
            ],
            "previous_hypothetical_weight": latest_weight_state[
                "previous_hypothetical_weight"
            ],
            "rotation_delta": latest_weight_state["rotation_delta"],
            "turnover_proxy": latest_weight_state["turnover_proxy"],
            "risk_state": latest_weight_state["risk_state"],
            "constraint_hit": latest_weight_state["constraint_hit"],
            "blocking_reason": latest_weight_state["blocking_reason"],
            "hypothetical_research_weight_series": weight_series,
            "blocking_reasons": list(blocking_reasons),
            "warning_reasons": list(warning_reasons),
            "data_quality_gate": normalized_data_quality,
            "source_signal_summary": dict(signal_summary),
            "weight_binding_policy": policy,
        },
        reader_brief=_reader_brief(
            summary=(
                "Research-only weight binding produced "
                f"{len(weight_series)} hypothetical weight row(s)."
            ),
            key_result=status,
            blocking_issues=_join_reasons(blocking_reasons) or "none",
            warnings=_join_reasons(warning_reasons) or "none",
            next_action=(
                "repair_research_weight_binding_inputs"
                if blocking_reasons
                else "run_executable_binding_safety_audit"
            ),
        ),
        next_action=(
            "repair_research_weight_binding_inputs"
            if blocking_reasons
            else "run_executable_binding_safety_audit"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "executable_research_weight_binding",
            "signal_binding_consumed": True,
            "hypothetical_research_weights_generated": not bool(blocking_reasons),
            "official_target_weights_generated": False,
            "broker_order_generated": False,
            "backfill_metrics_generated": False,
        },
        limitations=[
            "TRADING-462 outputs hypothetical research weights only.",
            "The output is not paper-shadow, live allocation, or official target weights.",
            "Backfill return, drawdown, cost, and benchmark metrics remain uncomputed.",
        ],
        requested_date_range=requested_range,
        methodology_overrides={
            "collector_mode": "read_validated_signal_binding",
            "contract_only": False,
            "does_not_implement_strategy_behavior": False,
            "research_weight_binding_implemented": not bool(blocking_reasons),
            "produces_hypothetical_research_weights": not bool(blocking_reasons),
            "does_not_generate_official_target_weights": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def build_executable_binding_safety_audit_payload(
    *,
    as_of: date,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
    project_root: Path = PROJECT_ROOT,
    static_scan_paths: Sequence[Path] | None = None,
) -> dict[str, Any]:
    signal_path = default_executable_binding_json_path(
        SIGNAL_BINDING_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    signal_validation_path = default_executable_binding_json_path(
        SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    weight_path = default_executable_binding_json_path(
        WEIGHT_BINDING_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    weight_validation_path = default_executable_binding_json_path(
        WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
        reports_dir,
        as_of,
    )
    signal_payload = _read_json_mapping(signal_path)
    signal_validation = _read_json_mapping(signal_validation_path)
    weight_payload = _read_json_mapping(weight_path)
    weight_validation = _read_json_mapping(weight_validation_path)
    signal_summary = _mapping(signal_payload.get("summary"))
    weight_summary = _mapping(weight_payload.get("summary"))
    candidate_id = _text(
        weight_summary.get("candidate_id"),
        _text(signal_summary.get("candidate_id"), "MISSING"),
    )
    requested_range = _text(
        weight_payload.get("requested_date_range"),
        _text(signal_payload.get("requested_date_range"), "not_applicable"),
    )
    artifact_checks = _executable_binding_artifact_safety_checks(
        signal_payload=signal_payload,
        signal_validation=signal_validation,
        weight_payload=weight_payload,
        weight_validation=weight_validation,
    )
    scan_paths = list(static_scan_paths or _default_safety_scan_paths(reports_dir, as_of))
    scan_findings = _focused_safety_scan(scan_paths=scan_paths, project_root=project_root)
    blocking_findings = [
        finding for finding in scan_findings if finding.get("severity") == "BLOCKING"
    ]
    warning_findings = [
        finding for finding in scan_findings if finding.get("severity") == "WARNING"
    ]
    failed_artifact_checks = [
        check for check in artifact_checks if check.get("status") == "FAIL"
    ]
    warning_artifact_checks = [
        check for check in artifact_checks if check.get("status") == "WARNING"
    ]
    if failed_artifact_checks or blocking_findings:
        status = SAFETY_BLOCKED
    elif warning_artifact_checks or warning_findings:
        status = SAFETY_WARNING
    else:
        status = SAFETY_PASS
    summary = {
        "safety_audit_status": status,
        "candidate_id": candidate_id,
        "market_regime": _text(signal_payload.get("market_regime"), MARKET_REGIME),
        "requested_date_range": requested_range,
        "artifact_check_count": len(artifact_checks),
        "failed_artifact_check_count": len(failed_artifact_checks),
        "warning_artifact_check_count": len(warning_artifact_checks),
        "static_scan_path_count": len(scan_paths),
        "static_scan_finding_count": len(scan_findings),
        "blocking_static_finding_count": len(blocking_findings),
        "warning_static_finding_count": len(warning_findings),
        "acceptable_warning": status == SAFETY_WARNING and not failed_artifact_checks,
        "signal_binding_research_only": _executable_signal_binding_safe(signal_payload),
        "weight_binding_hypothetical_only": _executable_weight_binding_safe(weight_payload),
        "official_target_weights": False,
        "paper_shadow_activation": False,
        "owner_decision_append": False,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }
    return _payload(
        report_type=SAFETY_AUDIT_REPORT_TYPE,
        as_of=as_of,
        status=status,
        purpose=(
            "Audit executable signal and research weight bindings before any "
            "backfill rerun."
        ),
        input_artifacts={
            "next_candidate_signal_binding": str(signal_path),
            "next_candidate_signal_binding_validation": str(signal_validation_path),
            "next_candidate_research_weight_binding": str(weight_path),
            "next_candidate_research_weight_binding_validation": str(
                weight_validation_path
            ),
        },
        output_decision=status,
        summary=summary,
        body={
            "artifact_checks": artifact_checks,
            "static_scan_findings": scan_findings,
            "scan_paths": [str(path) for path in scan_paths],
            "blocking_reasons": [
                *[_text(row.get("issue_id")) for row in failed_artifact_checks],
                *[_text(row.get("finding_id")) for row in blocking_findings],
            ],
            "warning_reasons": [
                *[_text(row.get("issue_id")) for row in warning_artifact_checks],
                *[_text(row.get("finding_id")) for row in warning_findings],
            ],
            "source_statuses": [
                _source_status("next_candidate_signal_binding", signal_payload),
                _source_status("next_candidate_signal_binding_validation", signal_validation),
                _source_status("next_candidate_research_weight_binding", weight_payload),
                _source_status(
                    "next_candidate_research_weight_binding_validation",
                    weight_validation,
                ),
            ],
        },
        reader_brief=_reader_brief(
            summary=f"Executable binding safety audit status is {status}.",
            key_result=status,
            blocking_issues=_issue_names(failed_artifact_checks, "issue_id")
            if failed_artifact_checks
            else _issue_names(blocking_findings, "finding_id"),
            warnings=_issue_names(warning_artifact_checks, "issue_id")
            if warning_artifact_checks
            else _issue_names(warning_findings, "finding_id"),
            next_action=(
                "repair_executable_binding_safety_blockers"
                if status == SAFETY_BLOCKED
                else "run_backfill_with_executable_binding"
            ),
        ),
        next_action=(
            "repair_executable_binding_safety_blockers"
            if status == SAFETY_BLOCKED
            else "run_backfill_with_executable_binding"
        ),
        safety_boundary=_safety_boundary()
        | {
            "mode": "executable_binding_safety_audit",
            "static_scan_executed": True,
            "backfill_executed": False,
            "hypothetical_research_weights_are_official": False,
        },
        limitations=[
            "Safety audit is read-only and does not run backfill.",
            "Static scanner warnings require review but do not by themselves authorize trading.",
        ],
        requested_date_range=requested_range,
        methodology_overrides={
            "collector_mode": "read_existing_binding_artifacts_and_scan_sources",
            "contract_only": False,
            "does_not_implement_strategy_behavior": False,
            "safety_audit_only": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def validate_executable_binding_payload(
    payload: Mapping[str, Any],
    *,
    expected_report_type: str | None = None,
) -> dict[str, Any]:
    expected = expected_report_type or CONTRACT_REPORT_TYPE
    if expected == SIGNAL_BINDING_REPORT_TYPE:
        return _validate_signal_binding_payload(payload)
    if expected == WEIGHT_BINDING_REPORT_TYPE:
        return _validate_weight_binding_payload(payload)
    if expected == SAFETY_AUDIT_REPORT_TYPE:
        return _validate_safety_audit_payload(payload)
    report_type = _text(payload.get("report_type"))
    contract = _mapping(payload.get("binding_contract"))
    summary = _mapping(payload.get("summary"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == expected,
        f"report_type must be {expected}.",
        "regenerate_expected_executable_binding_artifact",
    )
    _append_check(
        checks,
        blocking,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "production_effect must be none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking,
        "broker_and_order_effect_none",
        _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT,
        "broker_effect and order_effect must be none.",
        "remove_broker_or_order_surface",
    )
    _append_check(
        checks,
        blocking,
        "candidate_id_present",
        bool(_text(contract.get("candidate_id"))),
        "binding contract must include candidate_id.",
        "restore_candidate_id",
    )
    _append_check(
        checks,
        blocking,
        "binding_version_present",
        _text(contract.get("binding_version")) == BINDING_VERSION,
        f"binding_version must be {BINDING_VERSION}.",
        "restore_binding_version",
    )
    _append_check(
        checks,
        blocking,
        "input_output_schema_present",
        bool(_mapping(contract.get("input_schema")))
        and bool(_mapping(contract.get("output_schema"))),
        "binding contract must include input_schema and output_schema.",
        "restore_binding_schemas",
    )
    _append_check(
        checks,
        blocking,
        "research_only_metadata",
        contract.get("research_only") is True
        and contract.get("manual_review_only") is True
        and contract.get("official_target_weights") is False
        and contract.get("not_official_target_weights") is True,
        "binding contract must be research-only and not official target weights.",
        "restore_research_only_metadata",
    )
    output_types = {
        _text(row.get("output_type"))
        for row in _records(_mapping(contract.get("output_schema")).get("outputs"))
    }
    _append_check(
        checks,
        blocking,
        "required_output_types",
        set(REQUIRED_OUTPUT_TYPES).issubset(output_types),
        "output_schema must include all required output types.",
        "restore_required_output_types",
    )
    forbidden_outputs = {_text(row) for row in _records(contract.get("forbidden_outputs"))}
    _append_check(
        checks,
        blocking,
        "forbidden_outputs_declared",
        set(FORBIDDEN_OUTPUT_TYPES).issubset(forbidden_outputs),
        "binding contract must declare forbidden output types.",
        "restore_forbidden_output_declarations",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "safety boundary must forbid shadow/live/weights/broker/order/production.",
        "restore_safety_boundary",
    )
    _append_check(
        checks,
        blocking,
        "contract_only",
        contract.get("strategy_behavior_implemented") is False
        and contract.get("signal_binding_implemented") is False
        and contract.get("weight_binding_implemented") is False,
        "TRADING-460 must define contract only, not strategy behavior.",
        "move_strategy_behavior_to_later_task",
    )
    status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=f"{expected}{VALIDATION_SUFFIX}",
        as_of=_date_from_payload(payload),
        status=status,
        purpose=f"Validate {expected} schema and research-only safety boundary.",
        input_artifacts={expected: _text(payload.get("artifact_id"))},
        output_decision=status,
        summary={
            "validation_status": status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
            "broker_effect": BROKER_EFFECT,
            "order_effect": ORDER_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{expected} validation is {status}.",
            key_result=status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_executable_binding_contract"
                if status == FAIL_STATUS
                else "use_validated_executable_binding_contract"
            ),
        ),
        next_action=(
            "repair_executable_binding_contract"
            if status == FAIL_STATUS
            else "use_validated_executable_binding_contract"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
    )


def _validate_signal_binding_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    signal_binding = _mapping(payload.get("signal_binding"))
    signal_state = _mapping(payload.get("signal_state"))
    data_quality_gate = _mapping(payload.get("data_quality_gate"))
    input_summary = _mapping(payload.get("signal_input_summary"))
    signal_rows = _records(payload.get("candidate_signal_series"))
    blocking_reasons = _records(payload.get("blocking_reasons"))
    status = _text(payload.get("status"))
    complete = status in {SIGNAL_BINDING_COMPLETE, SIGNAL_BINDING_COMPLETE_WITH_WARNINGS}
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == SIGNAL_BINDING_REPORT_TYPE,
        f"report_type must be {SIGNAL_BINDING_REPORT_TYPE}.",
        "regenerate_expected_signal_binding_artifact",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status
        in {
            SIGNAL_BINDING_COMPLETE,
            SIGNAL_BINDING_COMPLETE_WITH_WARNINGS,
            SIGNAL_BINDING_BLOCKED,
        },
        "signal binding status must be a recognized TRADING-461 status.",
        "restore_signal_binding_status",
    )
    _append_check(
        checks,
        blocking,
        "production_broker_order_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT,
        "production, broker, and order effects must be none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking,
        "research_only_metadata",
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and signal_binding.get("research_only") is True
        and signal_binding.get("manual_review_only") is True
        and signal_binding.get("official_target_weights") is False
        and signal_binding.get("not_official_target_weights") is True,
        "signal binding must be research-only and not official target weights.",
        "restore_research_only_metadata",
    )
    _append_check(
        checks,
        blocking,
        "candidate_id_and_binding_version",
        bool(_text(signal_binding.get("candidate_id")))
        and _text(signal_binding.get("binding_version")) == SIGNAL_BINDING_VERSION,
        f"signal binding must use binding_version {SIGNAL_BINDING_VERSION}.",
        "restore_signal_binding_identity",
    )
    _append_check(
        checks,
        blocking,
        "contract_validation_passed",
        (not complete) or _text(summary.get("contract_validation_status")) == PASS_STATUS,
        "complete signal binding requires source executable binding contract validation to pass.",
        "rerun_or_repair_executable_binding_contract_validation",
    )
    _append_check(
        checks,
        blocking,
        "data_quality_gate_passed",
        (not complete) or data_quality_gate.get("passed") is True,
        "complete signal binding requires validated cached data gate to pass.",
        "run_aits_validate_data_and_stop_on_failure",
    )
    _append_check(
        checks,
        blocking,
        "signal_input_not_blocking",
        (not complete) or _text(input_summary.get("signal_input_status")) != "BLOCKING",
        "complete signal binding requires non-BLOCKING signal input completeness.",
        "repair_signal_or_feature_inputs",
    )
    _append_check(
        checks,
        blocking,
        "blocked_artifact_has_reason",
        status != SIGNAL_BINDING_BLOCKED or bool(blocking_reasons),
        "blocked signal binding artifacts must expose blocking_reasons.",
        "add_exact_blocking_reason",
    )
    _append_check(
        checks,
        blocking,
        "complete_artifact_has_signal_series",
        (not complete) or bool(signal_rows),
        "complete signal binding must include candidate_signal_series.",
        "restore_candidate_signal_series",
    )
    _append_check(
        checks,
        blocking,
        "complete_artifact_has_signal_state",
        (not complete)
        or (
            bool(_text(signal_state.get("signal_date")))
            and _text(signal_state.get("risk_state")) != "blocked"
            and _text(signal_state.get("rotation_state")) != "blocked"
        ),
        "complete signal binding must include latest signal_state.",
        "restore_latest_signal_state",
    )
    _append_check(
        checks,
        blocking,
        "signal_rows_research_only",
        all(_signal_row_research_only(row) for row in signal_rows),
        "each signal row must carry research-only safety metadata.",
        "restore_signal_row_safety_metadata",
    )
    forbidden_keys = {
        "order_ticket",
        "broker_order",
        "live_allocation",
        "production_target_weight",
        "hypothetical_research_weight",
    }
    payload_keys = _flatten_keys(payload)
    _append_check(
        checks,
        blocking,
        "forbidden_outputs_absent",
        not (forbidden_keys & payload_keys),
        "TRADING-461 must not produce weights, broker/order, or live allocation outputs.",
        "remove_forbidden_outputs_from_signal_binding",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "safety boundary must forbid shadow/live/weights/broker/order/production.",
        "restore_safety_boundary",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=SIGNAL_BINDING_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-461 research-only signal binding output.",
        input_artifacts={SIGNAL_BINDING_REPORT_TYPE: _text(payload.get("artifact_id"))},
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "signal_binding_status": status,
            "signal_row_count": len(signal_rows),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
            "broker_effect": BROKER_EFFECT,
            "order_effect": ORDER_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{SIGNAL_BINDING_REPORT_TYPE} validation is {validation_status}.",
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_next_candidate_signal_binding"
                if validation_status == FAIL_STATUS
                else "use_validated_next_candidate_signal_binding"
            ),
        ),
        next_action=(
            "repair_next_candidate_signal_binding"
            if validation_status == FAIL_STATUS
            else "use_validated_next_candidate_signal_binding"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
        methodology_overrides={
            "contract_only": False,
            "does_not_implement_strategy_behavior": False,
            "validates_signal_binding_only": True,
            "does_not_generate_hypothetical_weights": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def _validate_weight_binding_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    binding_meta = _mapping(payload.get("research_weight_binding"))
    data_quality_gate = _mapping(payload.get("data_quality_gate"))
    source_signal_summary = _mapping(payload.get("source_signal_summary"))
    weight_series = _records(payload.get("hypothetical_research_weight_series"))
    blocking_reasons = _records(payload.get("blocking_reasons"))
    status = _text(payload.get("status"))
    complete = status in {WEIGHT_BINDING_COMPLETE, WEIGHT_BINDING_COMPLETE_WITH_WARNINGS}
    current_weight = _mapping(payload.get("hypothetical_research_weight"))
    previous_weight = _mapping(payload.get("previous_hypothetical_weight"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == WEIGHT_BINDING_REPORT_TYPE,
        f"report_type must be {WEIGHT_BINDING_REPORT_TYPE}.",
        "regenerate_expected_weight_binding_artifact",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status
        in {
            WEIGHT_BINDING_COMPLETE,
            WEIGHT_BINDING_COMPLETE_WITH_WARNINGS,
            WEIGHT_BINDING_BLOCKED,
        },
        "research weight binding status must be recognized.",
        "restore_research_weight_binding_status",
    )
    _append_check(
        checks,
        blocking,
        "production_broker_order_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT,
        "production, broker, and order effects must be none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking,
        "research_only_metadata",
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and binding_meta.get("research_only") is True
        and binding_meta.get("manual_review_only") is True
        and binding_meta.get("official_target_weights") is False
        and binding_meta.get("not_official_target_weights") is True,
        "weight binding must be research-only and not official target weights.",
        "restore_weight_binding_safety_metadata",
    )
    _append_check(
        checks,
        blocking,
        "candidate_id_and_binding_version",
        bool(_text(binding_meta.get("candidate_id")))
        and _text(binding_meta.get("binding_version")) == WEIGHT_BINDING_VERSION,
        f"weight binding must use binding_version {WEIGHT_BINDING_VERSION}.",
        "restore_weight_binding_identity",
    )
    _append_check(
        checks,
        blocking,
        "signal_binding_validation_passed",
        (not complete)
        or _text(summary.get("signal_binding_validation_status")) == PASS_STATUS,
        "complete weight binding requires validated signal binding.",
        "rerun_or_repair_signal_binding_validation",
    )
    _append_check(
        checks,
        blocking,
        "source_signal_binding_not_blocked",
        (not complete)
        or _text(source_signal_summary.get("signal_binding_status"))
        != SIGNAL_BINDING_BLOCKED,
        "complete weight binding requires non-blocked source signal binding.",
        "repair_signal_binding_before_weight_binding",
    )
    _append_check(
        checks,
        blocking,
        "data_quality_gate_passed",
        (not complete) or data_quality_gate.get("passed") is True,
        "complete weight binding requires validated cached data gate to pass.",
        "run_aits_validate_data_and_stop_on_failure",
    )
    _append_check(
        checks,
        blocking,
        "required_output_fields_present",
        all(
            key in payload
            for key in (
                "hypothetical_research_weight",
                "previous_hypothetical_weight",
                "rotation_delta",
                "turnover_proxy",
                "risk_state",
                "constraint_hit",
                "blocking_reason",
            )
        ),
        "TRADING-462 required output fields must be present.",
        "restore_required_weight_binding_fields",
    )
    _append_check(
        checks,
        blocking,
        "blocked_artifact_has_reason",
        status != WEIGHT_BINDING_BLOCKED or bool(blocking_reasons),
        "blocked weight binding artifacts must expose blocking_reasons.",
        "add_exact_blocking_reason",
    )
    _append_check(
        checks,
        blocking,
        "complete_artifact_has_weight_series",
        (not complete) or bool(weight_series),
        "complete weight binding must include a weight series.",
        "restore_hypothetical_research_weight_series",
    )
    _append_check(
        checks,
        blocking,
        "weight_outputs_research_only",
        _weight_object_research_only(current_weight)
        and _weight_object_research_only(previous_weight)
        and all(_weight_series_row_research_only(row) for row in weight_series),
        "all weight outputs must include research_only=true and safety metadata.",
        "restore_research_only_weight_metadata",
    )
    _append_check(
        checks,
        blocking,
        "complete_weight_sum_valid",
        (not complete) or _weight_sum_is_valid(current_weight),
        "complete current hypothetical weight must sum to the policy total.",
        "repair_weight_policy_or_binding_output",
    )
    _append_check(
        checks,
        blocking,
        "no_execution_surface",
        binding_meta.get("broker_order_produced") is False
        and binding_meta.get("paper_shadow_activation_produced") is False
        and binding_meta.get("backfill_metrics_produced") is False,
        "weight binding must not produce broker/order, paper-shadow, or metrics.",
        "remove_execution_or_metric_surface",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "safety boundary must forbid shadow/live/official weights/broker/order/production.",
        "restore_safety_boundary",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=WEIGHT_BINDING_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-462 research-only hypothetical weight binding output.",
        input_artifacts={WEIGHT_BINDING_REPORT_TYPE: _text(payload.get("artifact_id"))},
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "research_weight_binding_status": status,
            "weight_row_count": len(weight_series),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
            "broker_effect": BROKER_EFFECT,
            "order_effect": ORDER_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{WEIGHT_BINDING_REPORT_TYPE} validation is {validation_status}.",
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_next_candidate_research_weight_binding"
                if validation_status == FAIL_STATUS
                else "use_validated_next_candidate_research_weight_binding"
            ),
        ),
        next_action=(
            "repair_next_candidate_research_weight_binding"
            if validation_status == FAIL_STATUS
            else "use_validated_next_candidate_research_weight_binding"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
        methodology_overrides={
            "contract_only": False,
            "does_not_implement_strategy_behavior": False,
            "validates_research_weight_binding_only": True,
            "does_not_generate_official_target_weights": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def _validate_safety_audit_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    report_type = _text(payload.get("report_type"))
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("status"))
    artifact_checks = _records(payload.get("artifact_checks"))
    scan_findings = _records(payload.get("static_scan_findings"))
    checks: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking,
        "report_type",
        report_type == SAFETY_AUDIT_REPORT_TYPE,
        f"report_type must be {SAFETY_AUDIT_REPORT_TYPE}.",
        "regenerate_expected_safety_audit_artifact",
    )
    _append_check(
        checks,
        blocking,
        "allowed_status",
        status in {SAFETY_PASS, SAFETY_WARNING, SAFETY_BLOCKED},
        "safety audit status must be PASS, WARNING, or BLOCKED.",
        "restore_safety_audit_status",
    )
    _append_check(
        checks,
        blocking,
        "not_blocked",
        status != SAFETY_BLOCKED,
        "backfill cannot proceed when executable binding safety audit is BLOCKED.",
        "repair_safety_blockers_before_backfill",
    )
    _append_check(
        checks,
        blocking,
        "artifact_checks_no_failures",
        _int(summary.get("failed_artifact_check_count")) == 0,
        "artifact safety checks must not fail.",
        "repair_binding_artifact_safety_metadata",
    )
    _append_check(
        checks,
        blocking,
        "static_scan_no_blockers",
        _int(summary.get("blocking_static_finding_count")) == 0,
        "static scan must not find unsafe positive forbidden behavior.",
        "remove_forbidden_binding_behavior",
    )
    _append_check(
        checks,
        blocking,
        "signal_binding_research_only",
        summary.get("signal_binding_research_only") is True,
        "signal binding must be research-only.",
        "repair_signal_binding_safety_metadata",
    )
    _append_check(
        checks,
        blocking,
        "weight_binding_hypothetical_only",
        summary.get("weight_binding_hypothetical_only") is True,
        "weight binding must be hypothetical-only and not official target weights.",
        "repair_weight_binding_safety_metadata",
    )
    _append_check(
        checks,
        blocking,
        "production_broker_order_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT,
        "production, broker, and order effects must be none.",
        "restore_research_only_boundary",
    )
    _append_check(
        checks,
        blocking,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "safety boundary must forbid shadow/live/official weights/broker/order/production.",
        "restore_safety_boundary",
    )
    _append_check(
        checks,
        blocking,
        "audit_evidence_present",
        bool(artifact_checks) and bool(payload.get("scan_paths")),
        "safety audit must include artifact checks and scan paths.",
        "restore_safety_audit_evidence",
    )
    validation_status = FAIL_STATUS if blocking else PASS_STATUS
    return _payload(
        report_type=SAFETY_AUDIT_VALIDATION_REPORT_TYPE,
        as_of=_date_from_payload(payload),
        status=validation_status,
        purpose="Validate TRADING-463 executable binding safety audit output.",
        input_artifacts={SAFETY_AUDIT_REPORT_TYPE: _text(payload.get("artifact_id"))},
        output_decision=validation_status,
        summary={
            "validation_status": validation_status,
            "source_report_type": report_type,
            "candidate_id": _text(summary.get("candidate_id")),
            "safety_audit_status": status,
            "acceptable_warning": summary.get("acceptable_warning") is True,
            "artifact_check_count": len(artifact_checks),
            "static_scan_finding_count": len(scan_findings),
            "check_count": len(checks),
            "failed_check_count": len(blocking),
            "production_effect": PRODUCTION_EFFECT,
            "broker_effect": BROKER_EFFECT,
            "order_effect": ORDER_EFFECT,
        },
        body={
            "checks": checks,
            "blocking_issues": blocking,
            "warning_issues": [],
        },
        reader_brief=_reader_brief(
            summary=f"{SAFETY_AUDIT_REPORT_TYPE} validation is {validation_status}.",
            key_result=validation_status,
            blocking_issues=_issue_names(blocking, "issue_id"),
            warnings="none",
            next_action=(
                "repair_executable_binding_safety_audit"
                if validation_status == FAIL_STATUS
                else "use_validated_executable_binding_safety_audit"
            ),
        ),
        next_action=(
            "repair_executable_binding_safety_audit"
            if validation_status == FAIL_STATUS
            else "use_validated_executable_binding_safety_audit"
        ),
        safety_boundary=_safety_boundary(),
        limitations=["Validation is read-only."],
        requested_date_range=_text(payload.get("requested_date_range"), "not_applicable"),
        methodology_overrides={
            "contract_only": False,
            "validates_safety_audit_only": True,
            "does_not_compute_backfill_metrics": True,
        },
    )


def write_executable_binding_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_executable_binding_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_executable_binding_markdown(payload), encoding="utf-8")
    return output_path


def render_executable_binding_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_title(_text(payload.get('report_type')))} {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- status: {_md_cell(payload.get('status'))}",
        f"- output_decision: {_md_cell(payload.get('output_decision'))}",
        f"- market_regime: {_md_cell(payload.get('market_regime'))}",
        f"- requested_date_range: {_md_cell(payload.get('requested_date_range'))}",
        f"- production_effect: {_md_cell(payload.get('production_effect'))}",
        f"- broker_effect: {_md_cell(payload.get('broker_effect'))}",
        f"- order_effect: {_md_cell(payload.get('order_effect'))}",
        f"- next_action: {_md_cell(payload.get('next_action'))}",
    ]
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            lines.append(f"- {key}: {_md_cell(value)}")
    contract = _mapping(payload.get("binding_contract"))
    if contract:
        lines.extend(["", "## Binding Contract", ""])
        lines.extend(
            [
                f"- candidate_id: {_md_cell(contract.get('candidate_id'))}",
                f"- binding_version: {_md_cell(contract.get('binding_version'))}",
                f"- research_only: {_md_cell(contract.get('research_only'))}",
                f"- manual_review_only: {_md_cell(contract.get('manual_review_only'))}",
                f"- official_target_weights: {_md_cell(contract.get('official_target_weights'))}",
                f"- production_effect: {_md_cell(contract.get('production_effect'))}",
                f"- broker_effect: {_md_cell(contract.get('broker_effect'))}",
                f"- order_effect: {_md_cell(contract.get('order_effect'))}",
            ]
        )
        output_rows = _records(_mapping(contract.get("output_schema")).get("outputs"))
        lines.extend(_records_table("Output Schema", output_rows))
        forbidden = [
            {"forbidden_output": row}
            for row in _records(contract.get("forbidden_outputs"))
        ]
        lines.extend(_records_table("Forbidden Outputs", forbidden))
    signal_binding = _mapping(payload.get("signal_binding"))
    if signal_binding:
        lines.extend(["", "## Signal Binding", ""])
        official_target_weights = _md_cell(signal_binding.get("official_target_weights"))
        hypothetical_weight = _md_cell(
            signal_binding.get("hypothetical_research_weight_produced")
        )
        backfill_metrics = _md_cell(signal_binding.get("backfill_metrics_produced"))
        lines.extend(
            [
                f"- candidate_id: {_md_cell(signal_binding.get('candidate_id'))}",
                f"- binding_version: {_md_cell(signal_binding.get('binding_version'))}",
                f"- research_only: {_md_cell(signal_binding.get('research_only'))}",
                f"- manual_review_only: {_md_cell(signal_binding.get('manual_review_only'))}",
                f"- official_target_weights: {official_target_weights}",
                f"- production_effect: {_md_cell(signal_binding.get('production_effect'))}",
                f"- broker_effect: {_md_cell(signal_binding.get('broker_effect'))}",
                f"- order_effect: {_md_cell(signal_binding.get('order_effect'))}",
                f"- hypothetical_research_weight_produced: {hypothetical_weight}",
                f"- backfill_metrics_produced: {backfill_metrics}",
            ]
        )
        signal_state = _mapping(payload.get("signal_state"))
        if signal_state:
            lines.extend(["", "## Latest Signal State", ""])
            for key in (
                "signal_date",
                "signal_score",
                "regime_state",
                "risk_state",
                "rotation_state",
                "blocking_reason",
            ):
                lines.append(f"- {key}: {_md_cell(signal_state.get(key))}")
            lines.append(
                "- confidence_uncertainty: "
                f"{_md_cell(signal_state.get('confidence_uncertainty'))}"
            )
        series_rows = _records(payload.get("candidate_signal_series"))
        lines.extend(
            _records_table(
                "Candidate Signal Series",
                [
                    {
                        "signal_date": row.get("signal_date"),
                        "signal_score": row.get("signal_score"),
                        "regime_state": row.get("regime_state"),
                        "risk_state": row.get("risk_state"),
                        "rotation_state": row.get("rotation_state"),
                        "blocking_reason": row.get("blocking_reason"),
                        "research_only": row.get("research_only"),
                    }
                    for row in series_rows
                    if isinstance(row, Mapping)
                ],
            )
        )
        lines.extend(
            _records_table(
                "Signal Input Findings",
                _records(payload.get("signal_input_findings")),
            )
        )
        data_quality_gate = _mapping(payload.get("data_quality_gate"))
        if data_quality_gate:
            lines.extend(["", "## Data Quality Gate", ""])
            for key, value in data_quality_gate.items():
                lines.append(f"- {key}: {_md_cell(value)}")
    weight_binding = _mapping(payload.get("research_weight_binding"))
    if weight_binding:
        lines.extend(["", "## Research Weight Binding", ""])
        official_target_weights = _md_cell(weight_binding.get("official_target_weights"))
        not_official = _md_cell(weight_binding.get("not_official_target_weights"))
        paper_shadow_activation = _md_cell(
            weight_binding.get("paper_shadow_activation_produced")
        )
        backfill_metrics = _md_cell(weight_binding.get("backfill_metrics_produced"))
        lines.extend(
            [
                f"- candidate_id: {_md_cell(weight_binding.get('candidate_id'))}",
                f"- binding_version: {_md_cell(weight_binding.get('binding_version'))}",
                f"- research_only: {_md_cell(weight_binding.get('research_only'))}",
                f"- manual_review_only: {_md_cell(weight_binding.get('manual_review_only'))}",
                f"- official_target_weights: {official_target_weights}",
                f"- not_official_target_weights: {not_official}",
                f"- production_effect: {_md_cell(weight_binding.get('production_effect'))}",
                f"- broker_effect: {_md_cell(weight_binding.get('broker_effect'))}",
                f"- order_effect: {_md_cell(weight_binding.get('order_effect'))}",
                f"- paper_shadow_activation_produced: {paper_shadow_activation}",
                f"- broker_order_produced: {_md_cell(weight_binding.get('broker_order_produced'))}",
                f"- backfill_metrics_produced: {backfill_metrics}",
            ]
        )
        lines.extend(["", "## Latest Hypothetical Research Weight", ""])
        for key in (
            "hypothetical_research_weight",
            "previous_hypothetical_weight",
            "rotation_delta",
            "turnover_proxy",
            "risk_state",
            "constraint_hit",
            "blocking_reason",
        ):
            lines.append(f"- {key}: {_md_cell(payload.get(key))}")
        weight_rows = _records(payload.get("hypothetical_research_weight_series"))
        lines.extend(
            _records_table(
                "Hypothetical Research Weight Series",
                [
                    {
                        "signal_date": row.get("signal_date"),
                        "risk_state": row.get("risk_state"),
                        "rotation_state": row.get("rotation_state"),
                        "turnover_proxy": row.get("turnover_proxy"),
                        "constraint_hit": row.get("constraint_hit"),
                        "blocking_reason": row.get("blocking_reason"),
                        "research_only": row.get("research_only"),
                    }
                    for row in weight_rows
                    if isinstance(row, Mapping)
                ],
            )
        )
    if payload.get("report_type") == SAFETY_AUDIT_REPORT_TYPE:
        lines.extend(["", "## Safety Audit", ""])
        for key in (
            "safety_audit_status",
            "artifact_check_count",
            "failed_artifact_check_count",
            "static_scan_path_count",
            "static_scan_finding_count",
            "blocking_static_finding_count",
            "warning_static_finding_count",
            "acceptable_warning",
        ):
            lines.append(f"- {key}: {_md_cell(summary.get(key))}")
        lines.extend(
            _records_table("Artifact Safety Checks", _records(payload.get("artifact_checks")))
        )
        lines.extend(
            _records_table("Static Scan Findings", _records(payload.get("static_scan_findings")))
        )
    lines.extend(_records_table("Source Statuses", _records(payload.get("source_statuses"))))
    lines.extend(_records_table("Validation Checks", _records(payload.get("checks"))))
    lines.extend(["", "## Reader Brief", ""])
    for key, value in _mapping(payload.get("reader_brief")).items():
        lines.append(f"- {key}: {_md_cell(value)}")
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def _payload(
    *,
    report_type: str,
    as_of: date,
    status: str,
    purpose: str,
    input_artifacts: Mapping[str, Any],
    output_decision: str,
    summary: Mapping[str, Any],
    body: Mapping[str, Any],
    reader_brief: Mapping[str, Any],
    next_action: str,
    safety_boundary: Mapping[str, Any],
    limitations: Sequence[str],
    requested_date_range: str,
    methodology_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    methodology = {
        "collector_mode": "read_existing_research_artifacts",
        "contract_only": True,
        "does_not_refresh_data": True,
        "does_not_fabricate_data": True,
        "does_not_implement_strategy_behavior": True,
        "does_not_create_paper_shadow_candidate": True,
        "does_not_resume_normal_paper_shadow": True,
        "does_not_approve_extended_shadow": True,
        "does_not_approve_live_trading": True,
        "does_not_generate_official_target_weights": True,
        "does_not_touch_broker_or_orders": True,
        "does_not_append_owner_decision": True,
        "does_not_mutate_production": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }
    methodology.update(dict(methodology_overrides or {}))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "market_regime": MARKET_REGIME,
        "ai_regime_start": AI_REGIME_START,
        "requested_date_range": requested_date_range,
        "purpose": purpose,
        "input_artifacts": dict(input_artifacts),
        "output_decision": output_decision,
        "summary": dict(summary),
        **dict(body),
        "reader_brief": dict(reader_brief),
        "safety_boundary": dict(safety_boundary),
        "limitations": list(limitations),
        "next_action": next_action,
        "methodology": methodology,
    }


def _input_schema(
    *,
    candidate_id: str,
    frozen_spec: Mapping[str, Any],
    source_inputs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    frozen_signal_inputs = [_text(row) for row in _records(frozen_spec.get("signal_inputs"))]
    return {
        "schema_id": INPUT_SCHEMA_ID,
        "candidate_id": candidate_id,
        "required_signal_inputs": [
            {
                "input_id": "etf_signal_series",
                "source_path": "data/etf_portfolio/signals.csv",
                "required_columns": [
                    "date",
                    "symbol",
                    "trend_score",
                    "momentum_score",
                    "relative_strength_score",
                    "risk_score",
                    "composite_score",
                    "direction",
                    "confidence",
                    "reason_codes",
                    "model_version",
                    "feature_version",
                    "created_at",
                ],
                "required_coverage": ["QQQ", "SMH", "SOXX", "SPY"],
                "fail_closed_checks": [
                    "missing_signal_series",
                    "stale_signal_series",
                    "schema_mismatch",
                    "empty_signal_window",
                    "insufficient_market_coverage",
                ],
            },
            {
                "input_id": "latest_signal_snapshot",
                "source_glob": "outputs/reports/signal_snapshot_*.json",
                "required_json_paths": ["metadata.required_signals", "signals"],
                "warning_statuses": ["LIMITED"],
                "blocking_statuses": ["FAIL", "FAILED", "BLOCKED"],
                "fail_closed_checks": [
                    "missing_signal_snapshot",
                    "stale_signal_snapshot",
                    "missing_required_signal_groups",
                ],
            },
        ],
        "required_feature_inputs": [
            {
                "input_id": "etf_feature_matrix",
                "source_path": "data/etf_portfolio/features.csv",
                "required_columns": [
                    "date",
                    "symbol",
                    "close",
                    "adj_close",
                    "volume",
                    "ret_20d",
                    "ret_60d",
                    "ret_120d",
                    "ma_20",
                    "ma_50",
                    "ma_100",
                    "ma_200",
                    "realized_vol_20d",
                    "drawdown_63d",
                    "rs_vs_spy_60d",
                    "rs_vs_qqq_60d",
                    "rs_vs_smh_60d",
                    "feature_version",
                    "created_at",
                ],
                "required_coverage": ["CASH", "QQQ", "SMH", "SOXX", "SPY"],
                "fail_closed_checks": [
                    "missing_feature_matrix",
                    "stale_feature_matrix",
                    "missing_feature_columns",
                    "schema_mismatch",
                    "insufficient_market_coverage",
                ],
            },
            {
                "input_id": "daily_feature_records",
                "source_path": "data/processed/features_daily.csv",
                "required_columns": [
                    "as_of",
                    "source_date",
                    "category",
                    "subject",
                    "feature",
                    "value",
                    "unit",
                    "lookback",
                    "source",
                    "notes",
                ],
                "required_coverage": [
                    "macro_liquidity",
                    "price",
                    "relative_strength",
                    "risk_sentiment",
                    "trend",
                ],
                "fail_closed_checks": [
                    "missing_daily_feature_records",
                    "stale_daily_feature_records",
                    "missing_required_feature_category",
                ],
            },
        ],
        "source_frozen_signal_inputs": frozen_signal_inputs,
        "required_governance_inputs": [
            "validated_data_quality_gate",
            "next_candidate_spec_frozen",
            "next_candidate_research_cycle_snapshot",
            "research_backfill_plan_for_next_candidate",
            "next_candidate_research_gate",
            "signal_input_completeness_monitor",
        ],
        "source_inputs": [dict(row) for row in source_inputs],
        "freshness_policy_reference": (
            "config/etf_portfolio/dynamic_v3_rescue/"
            "signal_input_completeness_v1.yaml"
        ),
    }


def _output_schema(*, candidate_id: str) -> dict[str, Any]:
    return {
        "schema_id": OUTPUT_SCHEMA_ID,
        "candidate_id": candidate_id,
        "outputs": [
            {
                "output_type": "signal_score",
                "field": "signal_score",
                "type": "number",
                "allowed_range": [-1.0, 1.0],
                "research_only": True,
                "description": "Normalized research signal strength for diagnostics.",
            },
            {
                "output_type": "regime_state",
                "field": "regime_state",
                "type": "enum",
                "allowed_values": [
                    "normal_market_regime",
                    "rapid_drawdown",
                    "slow_drawdown",
                    "high_volatility_sideways_market",
                    "ai_semiconductor_correction",
                    "false_risk_off_cluster",
                    "unknown",
                ],
                "research_only": True,
            },
            {
                "output_type": "risk_state",
                "field": "risk_state",
                "type": "enum",
                "allowed_values": ["risk_on", "neutral", "risk_off", "blocked"],
                "research_only": True,
            },
            {
                "output_type": "rotation_state",
                "field": "rotation_state",
                "type": "enum",
                "allowed_values": [
                    "increase_ai_risk",
                    "hold_current_research_weight",
                    "reduce_ai_risk",
                    "move_to_cash_research_proxy",
                    "blocked",
                ],
                "research_only": True,
            },
            {
                "output_type": "hypothetical_research_weight",
                "field": "hypothetical_research_weight",
                "type": "mapping",
                "required_metadata": {
                    "research_only": True,
                    "not_official_target_weights": True,
                    "production_effect": PRODUCTION_EFFECT,
                    "broker_effect": BROKER_EFFECT,
                    "order_effect": ORDER_EFFECT,
                },
                "description": (
                    "Hypothetical allocation vector permitted only for research "
                    "backfill metrics."
                ),
            },
            {
                "output_type": "confidence_uncertainty",
                "field": "confidence_uncertainty",
                "type": "mapping",
                "required_fields": ["confidence", "uncertainty", "reasons"],
                "research_only": True,
            },
            {
                "output_type": "blocking_reason",
                "field": "blocking_reason",
                "type": "string_or_null",
                "required_when": "any input validation or safety check fails",
                "research_only": True,
            },
        ],
    }


def _source_inputs_from_frozen_spec(frozen_spec: Mapping[str, Any]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for index, value in enumerate(_records(frozen_spec.get("signal_inputs")), start=1):
        result.append(
            {
                "source_input_id": f"frozen_signal_input_{index}",
                "description": _text(value),
                "source": "next_candidate_spec_frozen",
            }
        )
    return result


def _reader_brief(
    *,
    summary: str,
    key_result: str,
    blocking_issues: str,
    warnings: str,
    next_action: str,
) -> dict[str, Any]:
    return {
        "summary": summary,
        "key_result": key_result,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "safety_boundary": (
            "research-only executable binding; no paper-shadow activation, no "
            "extended shadow, no live trading, no official target weights, no "
            "broker/order, production_effect=none."
        ),
        "next_action": next_action,
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "executable_research_binding_contract_only",
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
        "manual_review_only": True,
        "research_only": True,
        "paper_shadow_candidate_created": False,
        "paper_shadow_activation_allowed": False,
        "normal_paper_shadow_resumed": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
        "official_target_weights_generated": False,
        "hypothetical_research_weights_are_official": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_decision_appended": False,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
    }


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and _text(safety.get("broker_effect")) == BROKER_EFFECT
        and _text(safety.get("order_effect")) == ORDER_EFFECT
        and safety.get("manual_review_only") is True
        and safety.get("research_only") is True
        and safety.get("paper_shadow_candidate_created") is False
        and safety.get("paper_shadow_activation_allowed") is False
        and safety.get("normal_paper_shadow_resumed") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("hypothetical_research_weights_are_official") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("owner_decision_appended") is False
        and safety.get("strategy_outputs_mutated") is False
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
    )


def _normalize_data_quality_gate(value: Mapping[str, Any] | None) -> dict[str, Any]:
    gate = _mapping(value)
    return {
        "status": _text(gate.get("status"), "MISSING"),
        "passed": gate.get("passed") is True,
        "error_count": _int(gate.get("error_count")),
        "warning_count": _int(gate.get("warning_count")),
        "report_path": _text(gate.get("report_path")),
    }


def _load_signal_binding_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"signal binding policy not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"signal binding policy must be a mapping: {path}")
    return dict(raw)


def _signal_binding_blocking_reasons(
    *,
    contract_validation: Mapping[str, Any],
    data_quality_gate: Mapping[str, Any],
    input_summary: Mapping[str, Any],
    input_findings: Sequence[Mapping[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if _text(contract_validation.get("status")) != PASS_STATUS:
        reasons.append("contract_validation_not_passed")
    if data_quality_gate.get("passed") is not True:
        reasons.append("data_quality_gate_not_passed")
    if _text(input_summary.get("signal_input_status")) == "BLOCKING":
        reasons.append("signal_input_completeness_blocking")
    for finding in input_findings:
        if _text(finding.get("severity")) != "BLOCKING":
            continue
        input_id = _text(finding.get("input_id"), "unknown_input")
        for issue_code in _records(finding.get("issue_codes")):
            reasons.append(f"{input_id}:{_text(issue_code, 'unknown_issue')}")
    return sorted(set(reasons))


def _signal_binding_warning_reasons(
    *,
    data_quality_gate: Mapping[str, Any],
    input_summary: Mapping[str, Any],
    signal_state: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if _int(data_quality_gate.get("warning_count")):
        reasons.append("data_quality_gate_passed_with_warnings")
    if _text(input_summary.get("signal_input_status")) == "WARNING":
        reasons.append("signal_input_completeness_warning")
    uncertainty = _mapping(signal_state.get("confidence_uncertainty"))
    reasons.extend(_records(uncertainty.get("reasons")))
    return sorted({_text(reason) for reason in reasons if _text(reason)})


def _candidate_signal_series(
    *,
    candidate_id: str,
    signal_input_policy: Mapping[str, Any],
    signal_binding_policy: Mapping[str, Any],
    validation_windows: Sequence[Any],
    project_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    required_inputs = _mapping(signal_input_policy.get("required_inputs"))
    signal_rule = _mapping(required_inputs.get("etf_signal_series"))
    feature_rule = _mapping(required_inputs.get("etf_feature_matrix"))
    signal_path = _resolve_input_path(_text(signal_rule.get("path")), project_root)
    feature_path = _resolve_input_path(_text(feature_rule.get("path")), project_root)
    signal_rows = _read_csv_dicts(signal_path)
    feature_rows = _read_csv_dicts(feature_path)
    signal_date_column = _text(signal_rule.get("date_column"), "date")
    signal_symbol_column = _text(signal_rule.get("coverage_column"), "symbol")
    feature_date_column = _text(feature_rule.get("date_column"), "date")
    feature_symbol_column = _text(feature_rule.get("coverage_column"), "symbol")
    feature_by_date_symbol: dict[tuple[str, str], Mapping[str, str]] = {}
    for row in feature_rows:
        feature_by_date_symbol[
            (_text(row.get(feature_date_column)), _text(row.get(feature_symbol_column)))
        ] = row
    rows_by_date: dict[str, dict[str, Mapping[str, str]]] = {}
    for row in signal_rows:
        signal_date = _text(row.get(signal_date_column))
        symbol = _text(row.get(signal_symbol_column))
        if not signal_date or not symbol:
            continue
        rows_by_date.setdefault(signal_date, {})[symbol] = row
    output_rows: list[dict[str, Any]] = []
    blocked: list[str] = []
    for signal_date in sorted(rows_by_date):
        by_symbol = rows_by_date[signal_date]
        missing_signal_symbols = [
            symbol for symbol in REQUIRED_SIGNAL_SYMBOLS if symbol not in by_symbol
        ]
        missing_feature_symbols = [
            symbol
            for symbol in REQUIRED_SIGNAL_SYMBOLS
            if (signal_date, symbol) not in feature_by_date_symbol
        ]
        if missing_signal_symbols or missing_feature_symbols:
            blocked.append("insufficient_market_coverage")
            continue
        output_rows.append(
            _candidate_signal_row(
                candidate_id=candidate_id,
                signal_date=signal_date,
                signal_rows_by_symbol=by_symbol,
                feature_rows_by_symbol={
                    symbol: feature_by_date_symbol[(signal_date, symbol)]
                    for symbol in REQUIRED_SIGNAL_SYMBOLS
                },
                signal_binding_policy=signal_binding_policy,
                validation_windows=validation_windows,
            )
        )
    if not output_rows:
        blocked.append("empty_signal_window")
    return output_rows, sorted(set(blocked))


def _candidate_signal_row(
    *,
    candidate_id: str,
    signal_date: str,
    signal_rows_by_symbol: Mapping[str, Mapping[str, str]],
    feature_rows_by_symbol: Mapping[str, Mapping[str, str]],
    signal_binding_policy: Mapping[str, Any],
    validation_windows: Sequence[Any],
) -> dict[str, Any]:
    score_policy = _mapping(signal_binding_policy.get("score_normalization"))
    center = _float(score_policy.get("composite_score_center"), 50.0)
    span = _float(score_policy.get("composite_score_span"), 50.0) or 50.0
    min_score = _float(score_policy.get("normalized_min"), -1.0)
    max_score = _float(score_policy.get("normalized_max"), 1.0)
    symbol_scores = {
        symbol: _float(row.get("composite_score"))
        for symbol, row in signal_rows_by_symbol.items()
    }
    normalized_symbol_scores = {
        symbol: _clamp((score - center) / span, min_score, max_score)
        for symbol, score in symbol_scores.items()
    }
    ai_scores = [normalized_symbol_scores[symbol] for symbol in RESEARCH_SIGNAL_SYMBOLS]
    signal_score = _round_float(sum(ai_scores) / len(ai_scores))
    ai_median_score = _round_float(_median(ai_scores))
    spy_signal_score = _round_float(normalized_symbol_scores["SPY"])
    ai_vs_spy_delta = _round_float(ai_median_score - spy_signal_score)
    directions = {
        _text(signal_rows_by_symbol[symbol].get("direction")).lower()
        for symbol in REQUIRED_SIGNAL_SYMBOLS
    }
    confidence_values = [
        _text(signal_rows_by_symbol[symbol].get("confidence")).lower()
        for symbol in REQUIRED_SIGNAL_SYMBOLS
    ]
    risk_state = _risk_state_from_directions(
        directions=directions,
        signal_binding_policy=signal_binding_policy,
    )
    rotation_state = _rotation_state(
        risk_state=risk_state,
        ai_vs_spy_delta=ai_vs_spy_delta,
        signal_binding_policy=signal_binding_policy,
    )
    regime_state = _regime_state_for_date(signal_date, validation_windows)
    confidence_uncertainty = _confidence_uncertainty(
        confidence_values=confidence_values,
        directions=directions,
        regime_state=regime_state,
        ai_vs_spy_delta=ai_vs_spy_delta,
    )
    return {
        "candidate_id": candidate_id,
        "binding_version": SIGNAL_BINDING_VERSION,
        "signal_date": signal_date,
        "signal_score": signal_score,
        "regime_state": regime_state,
        "risk_state": risk_state,
        "rotation_state": rotation_state,
        "confidence_uncertainty": confidence_uncertainty,
        "blocking_reason": None,
        "component_scores": {
            "symbol_composite_scores": {
                symbol: _round_float(score) for symbol, score in symbol_scores.items()
            },
            "normalized_symbol_scores": {
                symbol: _round_float(score)
                for symbol, score in normalized_symbol_scores.items()
            },
            "ai_median_signal_score": ai_median_score,
            "spy_signal_score": spy_signal_score,
            "ai_vs_spy_delta": ai_vs_spy_delta,
            "directions": sorted(directions),
            "confidence_values": sorted(set(confidence_values)),
        },
        "feature_inputs": {
            symbol: {
                "drawdown_63d": _round_float(
                    _float(feature_rows_by_symbol[symbol].get("drawdown_63d"))
                ),
                "realized_vol_20d": _round_float(
                    _float(feature_rows_by_symbol[symbol].get("realized_vol_20d"))
                ),
                "rs_vs_spy_60d": _round_float(
                    _float(feature_rows_by_symbol[symbol].get("rs_vs_spy_60d"))
                ),
            }
            for symbol in REQUIRED_SIGNAL_SYMBOLS
        },
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }


def _signal_state_from_series(
    *,
    candidate_id: str,
    signal_rows: Sequence[Mapping[str, Any]],
    blocking_reasons: Sequence[str],
) -> dict[str, Any]:
    if not signal_rows:
        return {
            "candidate_id": candidate_id,
            "binding_version": SIGNAL_BINDING_VERSION,
            "signal_date": "",
            "signal_score": None,
            "regime_state": "blocked",
            "risk_state": "blocked",
            "rotation_state": "blocked",
            "confidence_uncertainty": {
                "confidence": "none",
                "uncertainty": "high",
                "reasons": list(blocking_reasons) or ["empty_signal_window"],
            },
            "blocking_reason": _join_reasons(blocking_reasons or ["empty_signal_window"]),
            "research_only": True,
            "manual_review_only": True,
            "official_target_weights": False,
            "not_official_target_weights": True,
            "production_effect": PRODUCTION_EFFECT,
            "broker_effect": BROKER_EFFECT,
            "order_effect": ORDER_EFFECT,
        }
    latest = dict(max(signal_rows, key=lambda row: _text(row.get("signal_date"))))
    latest["blocking_reason"] = _join_reasons(blocking_reasons) or None
    return latest


def _risk_state_from_directions(
    *,
    directions: set[str],
    signal_binding_policy: Mapping[str, Any],
) -> str:
    risk_policy = _mapping(signal_binding_policy.get("risk_state_rules"))
    risk_on_values = set(_texts(risk_policy.get("risk_on_direction_values"))) or {"bullish"}
    risk_off_values = set(_texts(risk_policy.get("risk_off_direction_values"))) or {"bearish"}
    if directions and directions.issubset(risk_on_values):
        return "risk_on"
    if directions & risk_off_values:
        return "risk_off"
    return "neutral"


def _rotation_state(
    *,
    risk_state: str,
    ai_vs_spy_delta: float,
    signal_binding_policy: Mapping[str, Any],
) -> str:
    rotation_policy = _mapping(signal_binding_policy.get("rotation_state_rules"))
    neutral_band = _float(rotation_policy.get("neutral_band"), 0.0)
    if risk_state == "risk_off":
        return "reduce_ai_risk"
    if ai_vs_spy_delta > neutral_band:
        return "increase_ai_risk"
    if ai_vs_spy_delta < -neutral_band:
        return "reduce_ai_risk"
    return "hold_current_research_weight"


def _regime_state_for_date(signal_date: str, validation_windows: Sequence[Any]) -> str:
    parsed = _parse_iso_date(signal_date)
    if parsed is None:
        return "unknown"
    for window in _records(validation_windows):
        row = _mapping(window)
        start = _parse_iso_date(_text(row.get("start")))
        end = _parse_iso_date(_text(row.get("end")))
        if start is None or end is None:
            continue
        if start <= parsed <= end:
            return _text(row.get("window_id"), _text(row.get("name"), "unknown"))
    return "unknown"


def _confidence_uncertainty(
    *,
    confidence_values: Sequence[str],
    directions: set[str],
    regime_state: str,
    ai_vs_spy_delta: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    confidence_set = {value for value in confidence_values if value}
    if len(directions) > 1:
        reasons.append("direction_disagreement")
    if regime_state == "unknown":
        reasons.append("signal_date_outside_frozen_validation_windows")
    if ai_vs_spy_delta < 0:
        reasons.append("ai_signal_below_spy_signal")
    if "low" in confidence_set:
        confidence = "low"
        uncertainty = "high"
    elif len(confidence_set) == 1 and "high" in confidence_set and not reasons:
        confidence = "high"
        uncertainty = "low"
    else:
        confidence = "medium"
        uncertainty = "medium"
    return {
        "confidence": confidence,
        "uncertainty": uncertainty,
        "reasons": reasons,
    }


def _resolve_input_path(raw_path: str, project_root: Path) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def _read_csv_dicts(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _signal_row_research_only(row: Any) -> bool:
    mapping = _mapping(row)
    return (
        mapping.get("research_only") is True
        and mapping.get("manual_review_only") is True
        and mapping.get("official_target_weights") is False
        and mapping.get("not_official_target_weights") is True
        and _text(mapping.get("production_effect")) == PRODUCTION_EFFECT
        and _text(mapping.get("broker_effect")) == BROKER_EFFECT
        and _text(mapping.get("order_effect")) == ORDER_EFFECT
    )


def _flatten_keys(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        result = {str(key) for key in value}
        for child in value.values():
            result.update(_flatten_keys(child))
        return result
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        result: set[str] = set()
        for child in value:
            result.update(_flatten_keys(child))
        return result
    return set()


def _json_safe(value: Any) -> Any:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(child) for key, child in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(child) for child in value]
    return value


def _weight_binding_blocking_reasons(
    *,
    signal_payload: Mapping[str, Any],
    signal_validation: Mapping[str, Any],
    data_quality_gate: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if _text(signal_validation.get("status")) != PASS_STATUS:
        reasons.append("signal_binding_validation_not_passed")
    if data_quality_gate.get("passed") is not True:
        reasons.append("data_quality_gate_not_passed")
    if _text(signal_payload.get("status")) == SIGNAL_BINDING_BLOCKED:
        reasons.append("signal_binding_blocked")
    signal_state = _mapping(signal_payload.get("signal_state"))
    if not signal_state:
        reasons.append("missing_signal_state")
    if _text(signal_state.get("risk_state")) == "blocked":
        reasons.append("signal_state_blocked")
    return sorted(set(reasons))


def _weight_binding_warning_reasons(
    *,
    data_quality_gate: Mapping[str, Any],
    signal_payload: Mapping[str, Any],
    weight_series: Sequence[Mapping[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if _int(data_quality_gate.get("warning_count")):
        reasons.append("data_quality_gate_passed_with_warnings")
    reasons.extend(_records(signal_payload.get("warning_reasons")))
    if weight_series:
        first = _mapping(weight_series[0])
        if _text(first.get("previous_weight_source")) == "policy_initial_previous_weight":
            reasons.append("initial_previous_hypothetical_weight_from_policy")
    return sorted({_text(reason) for reason in reasons if _text(reason)})


def _research_weight_series(
    *,
    candidate_id: str,
    signal_rows: Sequence[Any],
    policy: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[str]]:
    universe = _texts(policy.get("research_weight_universe"))
    previous_weights = _weights_from_mapping(
        _mapping(policy.get("initial_previous_hypothetical_weight")),
        universe=universe,
    )
    previous_source = "policy_initial_previous_weight"
    output_rows: list[dict[str, Any]] = []
    blocking: list[str] = []
    for raw_signal_row in sorted(
        (_mapping(row) for row in signal_rows),
        key=lambda row: _text(row.get("signal_date")),
    ):
        target_weights = _target_weights_for_signal(
            signal_row=raw_signal_row,
            previous_weights=previous_weights,
            policy=policy,
            universe=universe,
        )
        constraint_hit = _weight_constraint_hits(target_weights, policy=policy)
        if constraint_hit:
            blocking.extend(constraint_hit)
        rotation_delta = {}
        for symbol in universe:
            current_weight = target_weights.get(symbol, 0.0)
            previous_weight = previous_weights.get(symbol, 0.0)
            rotation_delta[symbol] = _round_float(current_weight - previous_weight)
        turnover_proxy = _round_float(
            sum(abs(value) for value in rotation_delta.values()) / 2
        )
        blocking_reason = _join_reasons(constraint_hit) or None
        output_rows.append(
            {
                "candidate_id": candidate_id,
                "binding_version": WEIGHT_BINDING_VERSION,
                "signal_date": _text(raw_signal_row.get("signal_date")),
                "risk_state": _text(raw_signal_row.get("risk_state"), "blocked"),
                "rotation_state": _text(raw_signal_row.get("rotation_state"), "blocked"),
                "hypothetical_research_weight": _research_weight_object(
                    weights=target_weights,
                    signal_date=_text(raw_signal_row.get("signal_date")),
                    source="rotation_profile",
                ),
                "previous_hypothetical_weight": _research_weight_object(
                    weights=previous_weights,
                    signal_date=_text(raw_signal_row.get("signal_date")),
                    source=previous_source,
                ),
                "previous_weight_source": previous_source,
                "rotation_delta": rotation_delta,
                "turnover_proxy": turnover_proxy,
                "constraint_hit": constraint_hit,
                "blocking_reason": blocking_reason,
                "research_only": True,
                "manual_review_only": True,
                "official_target_weights": False,
                "not_official_target_weights": True,
                "production_effect": PRODUCTION_EFFECT,
                "broker_effect": BROKER_EFFECT,
                "order_effect": ORDER_EFFECT,
            }
        )
        previous_weights = target_weights
        previous_source = "prior_hypothetical_research_weight_row"
    if not output_rows:
        blocking.append("empty_weight_binding_window")
    return output_rows, sorted(set(blocking))


def _target_weights_for_signal(
    *,
    signal_row: Mapping[str, Any],
    previous_weights: Mapping[str, float],
    policy: Mapping[str, Any],
    universe: Sequence[str],
) -> dict[str, float]:
    profiles = _mapping(policy.get("rotation_profiles"))
    rotation_state = _text(signal_row.get("rotation_state"), "blocked")
    profile = profiles.get(rotation_state)
    if _text(profile) == "previous":
        return {symbol: _round_float(previous_weights.get(symbol, 0.0)) for symbol in universe}
    if not isinstance(profile, Mapping):
        profile = _mapping(profiles.get("blocked"))
    return _weights_from_mapping(_mapping(profile), universe=universe)


def _weights_from_mapping(value: Mapping[str, Any], *, universe: Sequence[str]) -> dict[str, float]:
    return {symbol: _round_float(_float(value.get(symbol))) for symbol in universe}


def _weight_constraint_hits(
    weights: Mapping[str, float],
    *,
    policy: Mapping[str, Any],
) -> list[str]:
    constraints = _mapping(policy.get("constraints"))
    min_weight = _float(constraints.get("min_weight"), 0.0)
    max_weight = _float(constraints.get("max_single_weight"), 1.0)
    total_weight = _float(constraints.get("total_weight"), 1.0)
    tolerance = _float(constraints.get("total_weight_tolerance"), 0.000001)
    hits: list[str] = []
    for symbol, weight in weights.items():
        if weight < min_weight:
            hits.append(f"{symbol}:below_min_weight")
        if weight > max_weight:
            hits.append(f"{symbol}:above_max_single_weight")
    if abs(sum(weights.values()) - total_weight) > tolerance:
        hits.append("total_weight_mismatch")
    return hits


def _research_weight_object(
    *,
    weights: Mapping[str, float],
    signal_date: str,
    source: str,
) -> dict[str, Any]:
    return {
        "weight_type": "hypothetical_research_weight",
        "signal_date": signal_date,
        "source": source,
        "weights": {symbol: _round_float(weight) for symbol, weight in weights.items()},
        "research_only": True,
        "manual_review_only": True,
        "official_target_weights": False,
        "not_official_target_weights": True,
        "production_effect": PRODUCTION_EFFECT,
        "broker_effect": BROKER_EFFECT,
        "order_effect": ORDER_EFFECT,
    }


def _latest_weight_state(
    *,
    candidate_id: str,
    weight_series: Sequence[Mapping[str, Any]],
    blocking_reasons: Sequence[str],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    if weight_series:
        latest = dict(max(weight_series, key=lambda row: _text(row.get("signal_date"))))
        latest["blocking_reason"] = _join_reasons(blocking_reasons) or latest.get(
            "blocking_reason"
        )
        return latest
    empty_weight = _research_weight_object(
        weights=_weights_from_mapping(
            _mapping(_mapping(policy.get("rotation_profiles")).get("blocked")),
            universe=_texts(policy.get("research_weight_universe")),
        ),
        signal_date="",
        source="blocked_no_weight_output",
    )
    return {
        "candidate_id": candidate_id,
        "binding_version": WEIGHT_BINDING_VERSION,
        "signal_date": "",
        "risk_state": "blocked",
        "rotation_state": "blocked",
        "hypothetical_research_weight": empty_weight,
        "previous_hypothetical_weight": empty_weight,
        "rotation_delta": {},
        "turnover_proxy": None,
        "constraint_hit": [],
        "blocking_reason": _join_reasons(blocking_reasons or ["empty_weight_binding_window"]),
    }


def _weight_object_research_only(value: Mapping[str, Any]) -> bool:
    return (
        value.get("research_only") is True
        and value.get("manual_review_only") is True
        and value.get("official_target_weights") is False
        and value.get("not_official_target_weights") is True
        and _text(value.get("production_effect")) == PRODUCTION_EFFECT
        and _text(value.get("broker_effect")) == BROKER_EFFECT
        and _text(value.get("order_effect")) == ORDER_EFFECT
    )


def _weight_series_row_research_only(row: Any) -> bool:
    mapping = _mapping(row)
    return (
        mapping.get("research_only") is True
        and mapping.get("manual_review_only") is True
        and mapping.get("official_target_weights") is False
        and mapping.get("not_official_target_weights") is True
        and _text(mapping.get("production_effect")) == PRODUCTION_EFFECT
        and _text(mapping.get("broker_effect")) == BROKER_EFFECT
        and _text(mapping.get("order_effect")) == ORDER_EFFECT
        and _weight_object_research_only(_mapping(mapping.get("hypothetical_research_weight")))
        and _weight_object_research_only(_mapping(mapping.get("previous_hypothetical_weight")))
    )


def _weight_sum_is_valid(value: Mapping[str, Any]) -> bool:
    weights = _mapping(value.get("weights"))
    total = sum(_float(weight) for weight in weights.values())
    return bool(weights) and abs(total - 1.0) <= 0.000001


SAFETY_TERM_PATTERNS: dict[str, tuple[str, ...]] = {
    "official_target_weights": (
        "official_target_weights",
        "official target weights",
        "official target weight",
    ),
    "broker_integration": (
        "broker integration",
        "broker_order",
        "broker action",
        "broker_action",
    ),
    "order_ticket": ("order_ticket", "order ticket", "order tickets"),
    "live_allocation": ("live_allocation", "live allocation", "live trading"),
    "production_mutation": (
        "production_state_mutated",
        "production mutation",
        "production state",
    ),
    "paper_shadow_activation": (
        "paper_shadow_activation",
        "paper-shadow activation",
        "paper shadow activation",
    ),
    "owner_decision_append": (
        "owner_decision_appended",
        "owner decision append",
        "append owner decision",
    ),
    "account_id": ("account_id", "account id", "accountid"),
    "api_key": ("api_key", "api key", "apikey", "secret_key", "secret key"),
}

SAFETY_SAFE_MARKERS: tuple[str, ...] = (
    "false",
    "none",
    "no ",
    "not ",
    "without ",
    "forbidden",
    "blocked",
    "read-only",
    "research-only",
    "manual_review_only",
    "not_official_target_weights",
    "does_not_",
    "production_effect=none",
    "不得",
    "不生成",
    "不触发",
    "不修改",
    "禁止",
)


def _default_safety_scan_paths(reports_dir: Path, as_of: date) -> list[Path]:
    return [
        PROJECT_ROOT / "src" / "ai_trading_system" / "reports" / "executable_research_binding.py",
        DEFAULT_SIGNAL_BINDING_POLICY_PATH,
        DEFAULT_WEIGHT_BINDING_POLICY_PATH,
        default_executable_binding_json_path(SIGNAL_BINDING_REPORT_TYPE, reports_dir, as_of),
        default_executable_binding_json_path(WEIGHT_BINDING_REPORT_TYPE, reports_dir, as_of),
    ]


def _focused_safety_scan(
    *,
    scan_paths: Sequence[Path],
    project_root: Path,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for path in scan_paths:
        resolved = path if path.is_absolute() else project_root / path
        if not resolved.exists() or resolved.is_dir():
            findings.append(
                {
                    "finding_id": f"missing_scan_path:{resolved}",
                    "severity": "WARNING",
                    "term_family": "scan_path",
                    "path": str(resolved),
                    "line_number": None,
                    "line_excerpt": "",
                    "safe_context": True,
                }
            )
            continue
        try:
            lines = resolved.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            findings.append(
                {
                    "finding_id": f"unreadable_scan_path:{resolved}",
                    "severity": "WARNING",
                    "term_family": "scan_path",
                    "path": str(resolved),
                    "line_number": None,
                    "line_excerpt": "",
                    "safe_context": True,
                }
            )
            continue
        for line_number, line in enumerate(lines, start=1):
            normalized = line.lower()
            for family, patterns in SAFETY_TERM_PATTERNS.items():
                if not any(pattern in normalized for pattern in patterns):
                    continue
                safe_context = _safety_line_has_safe_context(
                    lines,
                    line_number - 1,
                    normalized,
                )
                severity = "WARNING" if safe_context else "BLOCKING"
                findings.append(
                    {
                        "finding_id": f"{family}:{resolved.name}:{line_number}",
                        "severity": severity,
                        "term_family": family,
                        "path": str(resolved),
                        "line_number": line_number,
                        "line_excerpt": line.strip()[:240],
                        "safe_context": safe_context,
                    }
                )
    return findings


def _safety_line_has_safe_context(
    lines: Sequence[str],
    line_index: int,
    normalized_line: str,
) -> bool:
    window_start = max(0, line_index - 40)
    window_end = min(len(lines), line_index + 2)
    context = "\n".join(row.lower() for row in lines[window_start:window_end])
    if any(marker in context for marker in SAFETY_SAFE_MARKERS):
        return True
    if any(
        marker in context
        for marker in (
            "forbidden_output_types",
            "forbidden_keys",
            "safety_term_patterns",
            "safety_safe_markers",
            "_append_safety_check",
        )
    ):
        return True
    return any(
        marker in normalized_line
        for marker in (
            "_md_cell(",
            ".get(",
            "line_excerpt",
            "finding_id",
            "term_family",
        )
    )


def _executable_binding_artifact_safety_checks(
    *,
    signal_payload: Mapping[str, Any],
    signal_validation: Mapping[str, Any],
    weight_payload: Mapping[str, Any],
    weight_validation: Mapping[str, Any],
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    _append_safety_check(
        checks,
        "signal_validation_pass",
        _text(signal_validation.get("status")) == PASS_STATUS,
        "Signal binding validation must pass.",
    )
    _append_safety_check(
        checks,
        "weight_validation_pass",
        _text(weight_validation.get("status")) == PASS_STATUS,
        "Research weight binding validation must pass.",
    )
    _append_safety_check(
        checks,
        "signal_binding_research_only",
        _executable_signal_binding_safe(signal_payload),
        "Signal binding must be research-only and effect-free.",
    )
    _append_safety_check(
        checks,
        "weight_binding_hypothetical_only",
        _executable_weight_binding_safe(weight_payload),
        "Weight binding must be hypothetical-only and effect-free.",
    )
    _append_safety_check(
        checks,
        "no_paper_shadow_activation",
        _safety_boundary_no_effects(signal_payload)
        and _safety_boundary_no_effects(weight_payload),
        (
            "Binding artifacts must not activate paper-shadow, owner decision, "
            "broker/order, or production."
        ),
    )
    _append_safety_check(
        checks,
        "no_sensitive_account_or_secret_fields",
        _no_sensitive_account_or_secret_fields(signal_payload, weight_payload),
        "Binding artifacts must not expose account id, API key, or secret key fields.",
    )
    return checks


def _append_safety_check(
    checks: list[dict[str, Any]],
    issue_id: str,
    passed: bool,
    message: str,
) -> None:
    checks.append(
        {
            "issue_id": issue_id,
            "status": "PASS" if passed else "FAIL",
            "severity": "OK" if passed else "BLOCKING",
            "message": message,
        }
    )


def _executable_signal_binding_safe(payload: Mapping[str, Any]) -> bool:
    binding_meta = _mapping(payload.get("signal_binding"))
    return (
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and binding_meta.get("research_only") is True
        and binding_meta.get("official_target_weights") is False
        and binding_meta.get("hypothetical_research_weight_produced") is False
        and _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT
    )


def _executable_weight_binding_safe(payload: Mapping[str, Any]) -> bool:
    binding_meta = _mapping(payload.get("research_weight_binding"))
    current_weight = _mapping(payload.get("hypothetical_research_weight"))
    previous_weight = _mapping(payload.get("previous_hypothetical_weight"))
    return (
        payload.get("research_only") is True
        and payload.get("manual_review_only") is True
        and binding_meta.get("research_only") is True
        and binding_meta.get("official_target_weights") is False
        and binding_meta.get("broker_order_produced") is False
        and binding_meta.get("paper_shadow_activation_produced") is False
        and binding_meta.get("backfill_metrics_produced") is False
        and _weight_object_research_only(current_weight)
        and _weight_object_research_only(previous_weight)
        and _text(payload.get("production_effect")) == PRODUCTION_EFFECT
        and _text(payload.get("broker_effect")) == BROKER_EFFECT
        and _text(payload.get("order_effect")) == ORDER_EFFECT
    )


def _safety_boundary_no_effects(payload: Mapping[str, Any]) -> bool:
    safety = _mapping(payload.get("safety_boundary"))
    return (
        safety.get("paper_shadow_candidate_created") is False
        and safety.get("paper_shadow_activation_allowed") is False
        and safety.get("normal_paper_shadow_resumed") is False
        and safety.get("extended_shadow_approved") is False
        and safety.get("live_trading_allowed") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("owner_decision_appended") is False
        and safety.get("production_state_mutated") is False
    )


def _no_sensitive_account_or_secret_fields(*payloads: Mapping[str, Any]) -> bool:
    sensitive_keys = {"account_id", "accountid", "api_key", "apikey", "secret_key"}
    flattened_keys: set[str] = set()
    for payload in payloads:
        flattened_keys.update(key.lower() for key in _flatten_keys(payload))
    return not (sensitive_keys & flattened_keys)


def _source_status(source_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "report_type": _text(payload.get("report_type")),
        "status": _text(payload.get("status")),
        "artifact_id": _text(payload.get("artifact_id")),
        "production_effect": _text(payload.get("production_effect")),
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    issue_id: str,
    passed: bool,
    message: str,
    remediation: str,
) -> None:
    row = {
        "issue_id": issue_id,
        "status": "PASS" if passed else "FAIL",
        "message": message,
        "remediation": remediation,
    }
    checks.append(row)
    if not passed:
        blocking_issues.append(row)


def _date_from_payload(payload: Mapping[str, Any]) -> date:
    try:
        return date.fromisoformat(_text(payload.get("as_of")))
    except ValueError:
        return date.today()


def _date_range_from_windows(windows: Sequence[Mapping[str, Any]]) -> str:
    starts = [_text(row.get("start")) for row in windows if _text(row.get("start"))]
    ends = [_text(row.get("end")) for row in windows if _text(row.get("end"))]
    if not starts or not ends:
        return "not_applicable"
    return f"{min(starts)}..{max(ends)}"


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object artifact: {path}")
    return payload


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob(f"{prefix}*{suffix}"):
        stem = path.name.removeprefix(prefix).removesuffix(suffix)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", stem):
            continue
        try:
            report_date = date.fromisoformat(stem)
        except ValueError:
            continue
        candidates.append((report_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _records(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _texts(value: Any) -> list[str]:
    return [_text(row) for row in _records(value) if _text(row)]


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _round_float(value: float, digits: int = 6) -> float:
    return round(float(value), digits)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _parse_iso_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(_text(value))
    except ValueError:
        return None


def _join_reasons(reasons: Sequence[Any]) -> str:
    values = [_text(reason) for reason in reasons if _text(reason)]
    return ",".join(values) if values else ""


def _issue_names(rows: Sequence[Mapping[str, Any]], key: str) -> str:
    values = [_text(row.get(key)) for row in rows if _text(row.get(key))]
    return ",".join(values) if values else "none"


def _title(report_type: str) -> str:
    return report_type.replace("_", " ").title()


def _records_table(title: str, rows: Sequence[Any]) -> list[str]:
    dict_rows = [dict(row) for row in rows if isinstance(row, Mapping)]
    if not dict_rows:
        return []
    keys = sorted({key for row in dict_rows for key in row})
    lines = [
        "",
        f"## {title}",
        "",
        "|" + "|".join(keys) + "|",
        "|" + "|".join("---" for _ in keys) + "|",
    ]
    for row in dict_rows:
        lines.append("|" + "|".join(_md_cell(row.get(key)) for key in keys) + "|")
    return lines


def _md_cell(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    elif value is None:
        text = ""
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br/>")


__all__ = [
    "BINDING_VERSION",
    "CONTRACT_REPORT_TYPE",
    "CONTRACT_VALIDATION_REPORT_TYPE",
    "DEFAULT_SIGNAL_INPUT_POLICY_PATH",
    "DEFAULT_SIGNAL_BINDING_POLICY_PATH",
    "DEFAULT_WEIGHT_BINDING_POLICY_PATH",
    "SAFETY_AUDIT_REPORT_TYPE",
    "SAFETY_AUDIT_VALIDATION_REPORT_TYPE",
    "SAFETY_BLOCKED",
    "SAFETY_PASS",
    "SAFETY_WARNING",
    "SIGNAL_BINDING_BLOCKED",
    "SIGNAL_BINDING_COMPLETE",
    "SIGNAL_BINDING_COMPLETE_WITH_WARNINGS",
    "SIGNAL_BINDING_REPORT_TYPE",
    "SIGNAL_BINDING_VALIDATION_REPORT_TYPE",
    "SIGNAL_BINDING_VERSION",
    "WEIGHT_BINDING_BLOCKED",
    "WEIGHT_BINDING_COMPLETE",
    "WEIGHT_BINDING_COMPLETE_WITH_WARNINGS",
    "WEIGHT_BINDING_REPORT_TYPE",
    "WEIGHT_BINDING_VALIDATION_REPORT_TYPE",
    "WEIGHT_BINDING_VERSION",
    "VALIDATION_SUFFIX",
    "build_executable_binding_safety_audit_payload",
    "build_next_candidate_executable_binding_contract_payload",
    "build_next_candidate_research_weight_binding_payload",
    "build_next_candidate_signal_binding_payload",
    "default_executable_binding_json_path",
    "default_executable_binding_markdown_path",
    "latest_executable_binding_json_path",
    "render_executable_binding_markdown",
    "validate_executable_binding_payload",
    "write_executable_binding_json",
    "write_executable_binding_markdown",
]
