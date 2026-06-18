from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
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
BINDING_VERSION = "next_candidate_executable_binding_contract_v1"
INPUT_SCHEMA_ID = "next_candidate_executable_binding_input_v1"
OUTPUT_SCHEMA_ID = "next_candidate_executable_binding_output_v1"

REPORT_PREFIXES: dict[str, str] = {
    CONTRACT_REPORT_TYPE: "next_candidate_executable_binding_contract",
    CONTRACT_VALIDATION_REPORT_TYPE: "next_candidate_executable_binding_contract_validation",
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


def validate_executable_binding_payload(
    payload: Mapping[str, Any],
    *,
    expected_report_type: str | None = None,
) -> dict[str, Any]:
    expected = expected_report_type or CONTRACT_REPORT_TYPE
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
) -> dict[str, Any]:
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
        "methodology": {
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
        },
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
    "VALIDATION_SUFFIX",
    "build_next_candidate_executable_binding_contract_payload",
    "default_executable_binding_json_path",
    "default_executable_binding_markdown_path",
    "latest_executable_binding_json_path",
    "render_executable_binding_markdown",
    "validate_executable_binding_payload",
    "write_executable_binding_json",
    "write_executable_binding_markdown",
]
