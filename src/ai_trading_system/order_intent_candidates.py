from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote

from ai_trading_system.core import ArtifactRef, ProductionEffect

ORDER_INTENT_CANDIDATES_SCHEMA_VERSION = 1
MANDATORY_BLOCKERS = ("trading_engine_not_enabled", "manual_approval_required")

TraceRecord = dict[str, Any]


def default_order_intent_candidates_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"order_intent_candidates_{as_of.isoformat()}.json"


def write_order_intent_candidates_json(
    *,
    as_of: date,
    daily_decision_summary_path: Path,
    output_path: Path,
    project_root: Path,
    decision_snapshot_path: Path | None = None,
    generated_at: datetime | None = None,
) -> Path:
    payload = build_order_intent_candidates_payload(
        as_of=as_of,
        daily_decision_summary_path=daily_decision_summary_path,
        project_root=project_root,
        decision_snapshot_path=decision_snapshot_path,
        generated_at=generated_at,
        artifact_base_dir=output_path.parent,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def build_order_intent_candidates_payload(
    *,
    as_of: date,
    daily_decision_summary_path: Path,
    project_root: Path,
    decision_snapshot_path: Path | None = None,
    generated_at: datetime | None = None,
    artifact_base_dir: Path | None = None,
) -> TraceRecord:
    generated = generated_at or datetime.now(tz=UTC)
    base_dir = artifact_base_dir or daily_decision_summary_path.parent
    daily_summary = _read_json_object(daily_decision_summary_path)
    resolved_snapshot_path = _resolve_decision_snapshot_path(
        as_of=as_of,
        daily_summary=daily_summary,
        daily_decision_summary_path=daily_decision_summary_path,
        explicit_path=decision_snapshot_path,
        project_root=project_root,
    )
    decision_snapshot = _read_json_object(resolved_snapshot_path)
    candidate = _build_candidate(
        as_of=as_of,
        daily_summary=daily_summary,
        decision_snapshot=decision_snapshot,
        decision_snapshot_path=resolved_snapshot_path,
    )
    source_artifacts = [
        _artifact_record(
            "daily_decision_summary",
            "daily decision summary",
            daily_decision_summary_path,
            base_dir,
        ),
        _artifact_record(
            "decision_snapshot",
            "score decision snapshot",
            resolved_snapshot_path,
            base_dir,
        ),
    ]
    return {
        "schema_version": ORDER_INTENT_CANDIDATES_SCHEMA_VERSION,
        "report_type": "order_intent_candidates",
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "run_id": _string(daily_summary.get("run_id")),
        "production_effect": ProductionEffect.NONE.value,
        "status": _payload_status(
            candidate=candidate,
            daily_summary=daily_summary,
            decision_snapshot=decision_snapshot,
        ),
        "execution_boundary": {
            "creates_order_intent": False,
            "creates_execution_action": False,
            "broker_api_allowed": False,
            "paper_broker_allowed": False,
            "account_state_required": False,
            "trading_engine_connected": False,
        },
        "source_inputs": {
            "daily_decision_summary": _source_input_record(
                daily_decision_summary_path,
                base_dir,
            ),
            "decision_snapshot": _source_input_record(resolved_snapshot_path, base_dir),
        },
        "source_artifacts": source_artifacts,
        "candidate_count": 1 if candidate is not None else 0,
        "candidates": [] if candidate is None else [candidate],
    }


def _build_candidate(
    *,
    as_of: date,
    daily_summary: TraceRecord,
    decision_snapshot: TraceRecord,
    decision_snapshot_path: Path,
) -> TraceRecord | None:
    if not daily_summary:
        return None
    investment = _mapping(daily_summary.get("investment_conclusion"))
    data_gate = _mapping(daily_summary.get("data_gate"))
    summary_status = _string(daily_summary.get("status"))
    action_bias = _string(investment.get("action_bias"))
    candidate_action = _candidate_action(action_bias)
    positions = _mapping(decision_snapshot.get("positions"))
    position_gates = _position_gates(decision_snapshot)
    binding_gates = tuple(gate for gate in position_gates if gate.get("triggered") is True)
    snapshot_id = _string(decision_snapshot.get("snapshot_id")) or f"decision_snapshot:{as_of}"
    run_id = _string(daily_summary.get("run_id")) or f"daily_decision_summary:{as_of}"
    blocked_by = _candidate_blockers(
        daily_summary=daily_summary,
        decision_snapshot=decision_snapshot,
        data_gate=data_gate,
        summary_status=summary_status,
    )
    return {
        "schema_version": "1.0",
        "candidate_id": f"order_intent_candidate:{as_of.isoformat()}:ai_exposure",
        "strategy_id": "daily_decision_bus",
        "strategy_version": "candidate_schema_v1",
        "run_id": run_id,
        "mode": "paper",
        "candidate_type": "ai_exposure_adjustment",
        "candidate_action": candidate_action,
        "candidate_action_label": _candidate_action_label(candidate_action),
        "scope": "ai_risk_asset_bucket",
        "statement": _candidate_statement(candidate_action, investment),
        "blocked": True,
        "blocked_by": list(blocked_by),
        "production_effect": ProductionEffect.NONE.value,
        "execution_action": "none",
        "would_create_order_intent": False,
        "would_submit_order": False,
        "manual_approval_required": True,
        "trading_engine_connected": False,
        "account_state_dependency": False,
        "score_snapshot_id": snapshot_id,
        "confidence": 0.0,
        "reason_codes": [candidate_action],
        "metadata": {
            "source": "daily_decision_summary",
            "as_of": as_of.isoformat(),
        },
        "source_decision": {
            "action_bias": action_bias or "missing",
            "confidence": _string(investment.get("confidence")) or "missing",
            "position_band": _string(investment.get("position_band")) or "missing",
            "data_gate_status": _string(data_gate.get("status")) or "MISSING",
            "major_risks": _string_list(investment.get("major_risks")),
        },
        "score_snapshot": {
            "snapshot_id": snapshot_id,
            "path": str(decision_snapshot_path),
            "exists": bool(decision_snapshot),
            "overall_score": _mapping(decision_snapshot.get("scores")).get("overall_score"),
            "confidence_score": _mapping(decision_snapshot.get("scores")).get(
                "confidence_score"
            ),
        },
        "position_context": {
            "daily_summary_band": _string(investment.get("position_band")) or "missing",
            "snapshot_final_risk_asset_ai_band": _format_band_record(
                positions.get("final_risk_asset_ai_band")
            ),
            "snapshot_confidence_adjusted_risk_asset_ai_band": _format_band_record(
                positions.get("confidence_adjusted_risk_asset_ai_band")
            ),
            "snapshot_total_asset_ai_band": _format_band_record(
                positions.get("total_asset_ai_band")
            ),
            "snapshot_final_total_risk_asset_band": _format_band_record(
                positions.get("final_total_risk_asset_band")
            ),
            "binding_position_gates": list(binding_gates),
            "position_gates": list(position_gates),
        },
        "non_execution_policy": {
            "no_symbol_level_order": True,
            "no_quantity": True,
            "no_notional": True,
            "no_limit_price": True,
            "no_broker_route": True,
        },
    }


def _candidate_blockers(
    *,
    daily_summary: TraceRecord,
    decision_snapshot: TraceRecord,
    data_gate: TraceRecord,
    summary_status: str,
) -> tuple[str, ...]:
    blockers = list(MANDATORY_BLOCKERS)
    data_gate_status = _string(data_gate.get("status"))
    if not data_gate_status or data_gate_status != "PASS":
        blockers.append("data_gate_blocked")
    if summary_status in {"limited", "missing"}:
        blockers.append("daily_decision_summary_limited")
    if not decision_snapshot:
        blockers.append("score_snapshot_missing")
    if _mapping(daily_summary.get("investment_conclusion")).get("availability") == "missing":
        blockers.append("investment_conclusion_missing")
    return tuple(dict.fromkeys(blockers))


def _payload_status(
    *,
    candidate: TraceRecord | None,
    daily_summary: TraceRecord,
    decision_snapshot: TraceRecord,
) -> str:
    if candidate is None:
        return "missing"
    summary_status = _string(daily_summary.get("status"))
    investment = _mapping(daily_summary.get("investment_conclusion"))
    if (
        summary_status in {"limited", "missing"}
        or not decision_snapshot
        or investment.get("availability") == "missing"
    ):
        return "limited"
    return "blocked"


def _candidate_action(action_bias: str) -> str:
    normalized = action_bias.strip().lower()
    if not normalized:
        return "UNKNOWN_BLOCKED"
    if any(token in normalized for token in ("buy", "increase", "add", "加", "增", "提高")):
        return "INCREASE_AI_EXPOSURE_BLOCKED"
    if any(token in normalized for token in ("sell", "reduce", "trim", "减", "降", "卖")):
        return "DECREASE_AI_EXPOSURE_BLOCKED"
    if any(token in normalized for token in ("observe", "hold", "watch", "观察", "等待")):
        return "HOLD_OR_OBSERVE_BLOCKED"
    return "REVIEW_DIRECTION_BLOCKED"


def _candidate_action_label(candidate_action: str) -> str:
    labels = {
        "INCREASE_AI_EXPOSURE_BLOCKED": "future candidate: increase AI exposure",
        "DECREASE_AI_EXPOSURE_BLOCKED": "future candidate: decrease AI exposure",
        "HOLD_OR_OBSERVE_BLOCKED": "future candidate: hold or observe",
        "REVIEW_DIRECTION_BLOCKED": "future candidate: review direction manually",
        "UNKNOWN_BLOCKED": "future candidate unavailable",
    }
    return labels.get(candidate_action, "future candidate unavailable")


def _candidate_statement(candidate_action: str, investment: TraceRecord) -> str:
    position_band = _string(investment.get("position_band")) or "missing"
    confidence = _string(investment.get("confidence")) or "missing"
    return (
        "Blocked candidate only: if future trading is allowed, review "
        f"{candidate_action} with target band {position_band} and confidence {confidence}."
    )


def _resolve_decision_snapshot_path(
    *,
    as_of: date,
    daily_summary: TraceRecord,
    daily_decision_summary_path: Path,
    explicit_path: Path | None,
    project_root: Path,
) -> Path:
    if explicit_path is not None:
        return explicit_path
    evidence_path = _evidence_dashboard_json_path(daily_summary, daily_decision_summary_path)
    if evidence_path is not None:
        evidence = _read_json_object(evidence_path)
        artifact_path = _mapping(evidence.get("artifacts")).get("decision_snapshot_path")
        if isinstance(artifact_path, str) and artifact_path:
            return Path(artifact_path)
    return (
        project_root
        / "data"
        / "processed"
        / "decision_snapshots"
        / f"decision_snapshot_{as_of.isoformat()}.json"
    )


def _evidence_dashboard_json_path(
    daily_summary: TraceRecord,
    daily_decision_summary_path: Path,
) -> Path | None:
    for artifact in _list_mappings(daily_summary.get("source_artifacts")):
        label = _string(artifact.get("label")).lower()
        path = _string(artifact.get("path"))
        if label == "evidence dashboard json" and path:
            return Path(path)
    sibling = daily_decision_summary_path.with_name(
        daily_decision_summary_path.name.replace(
            "daily_decision_summary_",
            "evidence_dashboard_",
        )
    )
    return sibling if sibling.exists() else None


def _position_gates(snapshot: TraceRecord) -> tuple[TraceRecord, ...]:
    raw_gates = _mapping(snapshot.get("positions")).get("position_gates")
    gates = []
    for gate in _list_mappings(raw_gates):
        gates.append(
            {
                "gate_id": _string(gate.get("gate_id")) or "unknown",
                "label": _string(gate.get("label")),
                "source": _string(gate.get("source")),
                "triggered": bool(gate.get("triggered")),
                "max_position": gate.get("max_position"),
                "reason": _string(gate.get("reason")),
            }
        )
    return tuple(gates)


def _format_band_record(value: object) -> str:
    band = _mapping(value)
    min_text = _format_percent(band.get("min_position"))
    max_text = _format_percent(band.get("max_position"))
    label = _string(band.get("label"))
    if min_text and max_text:
        return f"{min_text}-{max_text}" + (f" ({label})" if label else "")
    return "missing"


def _format_percent(value: object) -> str:
    if not isinstance(value, (int, float)):
        return ""
    return f"{float(value) * 100:.0f}%"


def _artifact_record(
    artifact_id: str,
    label: str,
    path: Path,
    base_dir: Path,
) -> TraceRecord:
    ref = ArtifactRef.from_path(path)
    return {
        "id": artifact_id,
        "label": label,
        "path": str(path),
        "href": _href(path, base_dir),
        "exists": ref.exists,
        "artifact_type": ref.artifact_type,
        "checksum_sha256": ref.sha256,
        "size_bytes": ref.size_bytes,
    }


def _source_input_record(path: Path, base_dir: Path) -> TraceRecord:
    ref = ArtifactRef.from_path(path)
    return {
        "path": str(path),
        "href": _href(path, base_dir),
        "exists": ref.exists,
        "checksum_sha256": ref.sha256,
    }


def _href(path: Path, base_dir: Path) -> str:
    try:
        link_path = path.relative_to(base_dir)
    except ValueError:
        try:
            link_path = path.resolve().relative_to(base_dir.resolve())
        except (OSError, RuntimeError, ValueError):
            if path.is_absolute():
                try:
                    return path.as_uri()
                except ValueError:
                    pass
            link_path = path
    return quote(link_path.as_posix(), safe="/._-:%#?=&")


def _read_json_object(path: Path) -> TraceRecord:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _mapping(value: object) -> TraceRecord:
    return value if isinstance(value, dict) else {}


def _list_mappings(value: object) -> tuple[TraceRecord, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, dict))


def _string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]
