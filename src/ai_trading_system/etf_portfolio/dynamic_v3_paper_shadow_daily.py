from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_PAPER_SHADOW_DAILY_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_daily"
PAPER_SHADOW_DAILY_REVIEW_FIELDS = (
    "signal_output",
    "hypothetical_weight_recommendation",
    "risk_off_risk_on_state",
    "drawdown_state",
    "rotation_event",
    "mismatch_event",
    "benchmark_comparison",
    "manual_reviewer_notes",
)
PAPER_SHADOW_DAILY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "paper_shadow_daily_only": True,
    "observation_only": True,
    "hypothetical_weight_paper_shadow_only": True,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "production_state_mutated": False,
    "paper_account_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}


def run_paper_shadow_daily_observation(
    *,
    candidate: str,
    observation_date: str,
    market_panel_artifact: Path,
    signal_artifact: Path,
    signal_output: str,
    hypothetical_weight_recommendation: str,
    risk_off_risk_on_state: str,
    drawdown_state: str,
    rotation_event: str,
    mismatch_event: str,
    benchmark_comparison: str,
    manual_reviewer_notes: str,
    contract_id: str | None = None,
    protocol_id: str | None = None,
    contract_dir: Path = readiness.DEFAULT_FORMAL_RESEARCH_METHOD_CONTRACT_DIR,
    protocol_dir: Path = readiness.DEFAULT_PAPER_SHADOW_PROTOCOL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    contract_payload = readiness.formal_research_method_contract_report_payload(
        contract_id=contract_id,
        latest=contract_id is None,
        output_dir=contract_dir,
    )
    protocol_payload = readiness.paper_shadow_protocol_report_payload(
        protocol_id=protocol_id,
        latest=protocol_id is None,
        output_dir=protocol_dir,
    )
    contract_decision = _mapping(contract_payload.get("formal_research_method_decision"))
    contract = _mapping(contract_payload.get("formal_research_method_contract"))
    protocol = _mapping(protocol_payload.get("paper_shadow_protocol"))
    source_contract_id = _text(contract_payload.get("contract_id"))
    source_protocol_id = _text(protocol_payload.get("protocol_id"))
    input_artifacts = [
        _input_artifact("market_panel_artifact", market_panel_artifact),
        _input_artifact("signal_artifact", signal_artifact),
    ]
    blocking_reasons = _blocking_reasons(
        candidate=candidate,
        input_artifacts=input_artifacts,
        contract=contract,
        contract_decision=contract_decision,
        protocol=protocol,
        daily_fields={
            "signal_output": signal_output,
            "hypothetical_weight_recommendation": hypothetical_weight_recommendation,
            "risk_off_risk_on_state": risk_off_risk_on_state,
            "drawdown_state": drawdown_state,
            "rotation_event": rotation_event,
            "mismatch_event": mismatch_event,
            "benchmark_comparison": benchmark_comparison,
            "manual_reviewer_notes": manual_reviewer_notes,
        },
    )
    observation_status = "RECORDED" if not blocking_reasons else "BLOCKED"
    observation_id = st._stable_id(
        "paper-shadow-daily",
        candidate,
        observation_date,
        source_contract_id,
        source_protocol_id,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / observation_id)
    root.mkdir(parents=True, exist_ok=False)
    observation = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_daily_observation",
        "observation_id": root.name,
        "candidate": candidate,
        "observation_date": observation_date,
        "generated_at": generated.isoformat(),
        "observation_status": observation_status,
        "blocking_reasons": blocking_reasons,
        "source_contract_id": source_contract_id,
        "source_contract_status": contract_decision.get("formal_research_method_status"),
        "source_contract_promotion_state": contract_decision.get("promotion_state"),
        "source_protocol_id": source_protocol_id,
        "source_protocol_status": protocol.get("protocol_status"),
        "input_artifacts": input_artifacts,
        "daily_review": {
            "signal_output": signal_output,
            "hypothetical_weight_recommendation": {
                "value": hypothetical_weight_recommendation,
                "paper_shadow_only": True,
                "not_official_target_weights": True,
            },
            "risk_off_risk_on_state": risk_off_risk_on_state,
            "drawdown_state": drawdown_state,
            "rotation_event": rotation_event,
            "mismatch_event": mismatch_event,
            "benchmark_comparison": benchmark_comparison,
            "manual_reviewer_notes": manual_reviewer_notes,
        },
        "next_required_action": (
            "continue_daily_paper_shadow_observation"
            if observation_status == "RECORDED"
            else "resolve_blocking_inputs_before_observation"
        ),
        **PAPER_SHADOW_DAILY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_daily_manifest",
        "observation_id": root.name,
        "candidate": candidate,
        "observation_date": observation_date,
        "generated_at": generated.isoformat(),
        "status": "PASS" if observation_status == "RECORDED" else "FAIL",
        "observation_status": observation_status,
        "source_contract_id": source_contract_id,
        "source_protocol_id": source_protocol_id,
        "paper_shadow_daily_manifest_path": str(root / "paper_shadow_daily_manifest.json"),
        "paper_shadow_daily_observation_path": str(
            root / "paper_shadow_daily_observation.json"
        ),
        "paper_shadow_daily_report_path": str(root / "paper_shadow_daily_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_daily_validation.json"),
        **PAPER_SHADOW_DAILY_SAFETY,
    }
    reader = render_paper_shadow_daily_reader_brief(observation)
    st._write_json(root / "paper_shadow_daily_manifest.json", manifest)
    st._write_json(root / "paper_shadow_daily_observation.json", observation)
    st._write_text(
        root / "paper_shadow_daily_report.md",
        render_paper_shadow_daily_report(manifest, observation),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_paper_shadow_daily",
        root.name,
        root / "paper_shadow_daily_manifest.json",
    )
    validation = validate_paper_shadow_daily_artifact(
        observation_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "observation_id": root.name,
        "observation_dir": root,
        "manifest": manifest,
        "paper_shadow_daily_observation": observation,
        "reader_brief_section": reader,
        "paper_shadow_daily_validation": validation,
    }


def paper_shadow_daily_report_payload(
    *,
    observation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=observation_id,
        latest_pointer="latest_paper_shadow_daily",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_daily_manifest.json",
    )
    payload = {
        **st._read_json(root / "paper_shadow_daily_manifest.json"),
        "paper_shadow_daily_observation": st._read_json(
            root / "paper_shadow_daily_observation.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "observation_dir": str(root),
    }
    validation = st._read_optional_json(root / "paper_shadow_daily_validation.json")
    if validation:
        payload["paper_shadow_daily_validation"] = validation
    return payload


def validate_paper_shadow_daily_artifact(
    *,
    observation_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DAILY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / observation_id
    manifest = st._read_optional_json(root / "paper_shadow_daily_manifest.json") or {}
    observation = st._read_optional_json(root / "paper_shadow_daily_observation.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    daily_review = _mapping(observation.get("daily_review"))
    hypothetical = _mapping(daily_review.get("hypothetical_weight_recommendation"))
    input_artifacts = _records(observation.get("input_artifacts"))
    checks = st._required_file_checks(
        root,
        (
            "paper_shadow_daily_manifest.json",
            "paper_shadow_daily_observation.json",
            "paper_shadow_daily_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "observation_id_matches",
                manifest.get("observation_id") == observation_id,
                "",
            ),
            st._check("candidate_visible", bool(_text(observation.get("candidate"))), ""),
            st._check("date_visible", bool(_text(observation.get("observation_date"))), ""),
            st._check(
                "source_contract_visible",
                bool(_text(observation.get("source_contract_id"))),
                "",
            ),
            st._check(
                "source_protocol_visible",
                bool(_text(observation.get("source_protocol_id"))),
                "",
            ),
            st._check(
                "input_artifacts_exist",
                all(row.get("exists") is True for row in input_artifacts),
                "",
            ),
            st._check(
                "input_checksums_visible",
                all(bool(_text(row.get("checksum_sha256"))) for row in input_artifacts),
                "",
            ),
            st._check(
                "daily_review_fields_complete",
                all(
                    _daily_field_present(daily_review, field)
                    for field in PAPER_SHADOW_DAILY_REVIEW_FIELDS
                ),
                "",
            ),
            st._check(
                "hypothetical_weight_paper_shadow_only",
                hypothetical.get("paper_shadow_only") is True
                and hypothetical.get("not_official_target_weights") is True,
                "",
            ),
            st._check(
                "reader_brief_fields",
                "paper_shadow_daily_observation_id" in reader,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, observation), ""),
            st._check(
                "paper_account_not_mutated",
                observation.get("paper_account_state_mutated") is False
                and manifest.get("paper_account_state_mutated") is False,
                "",
            ),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_paper_shadow_daily_validation",
        observation_id,
        checks,
    )
    if write_output:
        st._write_json(root / "paper_shadow_daily_validation.json", validation)
        st._write_text(
            root / "paper_shadow_daily_validation.md",
            render_paper_shadow_daily_validation_report(validation),
        )
    return validation


def render_paper_shadow_daily_reader_brief(observation: Mapping[str, Any]) -> str:
    daily = _mapping(observation.get("daily_review"))
    return "\n".join(
        [
            "## Paper Shadow Daily Observation",
            "",
            f"- paper_shadow_daily_observation_id: {observation.get('observation_id')}",
            f"- paper_shadow_daily_candidate: {observation.get('candidate')}",
            f"- paper_shadow_daily_date: {observation.get('observation_date')}",
            f"- paper_shadow_daily_status: {observation.get('observation_status')}",
            f"- paper_shadow_daily_signal_output: {daily.get('signal_output')}",
            f"- paper_shadow_daily_risk_state: {daily.get('risk_off_risk_on_state')}",
            f"- paper_shadow_daily_next_action: {observation.get('next_required_action')}",
            "- safety_boundary: observation only / paper-shadow-only hypothetical "
            "weights / no official target / no broker / no production",
            "",
        ]
    )


def render_paper_shadow_daily_report(
    manifest: Mapping[str, Any],
    observation: Mapping[str, Any],
) -> str:
    daily = _mapping(observation.get("daily_review"))
    hypothetical = _mapping(daily.get("hypothetical_weight_recommendation")).get("value")
    input_lines = [
        f"- {row.get('source_id')}: path={row.get('path')} checksum={row.get('checksum_sha256')}"
        for row in _records(observation.get("input_artifacts"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Daily Observation {manifest.get('observation_id')}",
            "",
            "## Purpose",
            "Record one observation-only paper-shadow day for a formal research candidate.",
            "",
            "## Input Artifacts",
            *input_lines,
            f"- source_contract_id: {observation.get('source_contract_id')}",
            f"- source_protocol_id: {observation.get('source_protocol_id')}",
            "",
            "## Output Decision",
            f"- observation_status: {observation.get('observation_status')}",
            f"- next_required_action: {observation.get('next_required_action')}",
            "",
            "## Daily Review",
            f"- signal_output: {daily.get('signal_output')}",
            f"- hypothetical_weight_recommendation: {hypothetical}",
            f"- risk_off_risk_on_state: {daily.get('risk_off_risk_on_state')}",
            f"- drawdown_state: {daily.get('drawdown_state')}",
            f"- rotation_event: {daily.get('rotation_event')}",
            f"- mismatch_event: {daily.get('mismatch_event')}",
            f"- benchmark_comparison: {daily.get('benchmark_comparison')}",
            f"- manual_reviewer_notes: {daily.get('manual_reviewer_notes')}",
            "",
            "## Safety Boundary",
            "- observation only",
            "- hypothetical weight recommendation is paper-shadow-only",
            "- no official target weights",
            "- no paper account mutation",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_paper_shadow_daily_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Daily Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *checks,
            "",
        ]
    )


def _blocking_reasons(
    *,
    candidate: str,
    input_artifacts: list[dict[str, Any]],
    contract: Mapping[str, Any],
    contract_decision: Mapping[str, Any],
    protocol: Mapping[str, Any],
    daily_fields: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if any(row.get("exists") is not True for row in input_artifacts):
        reasons.append("missing_input_artifact")
    if _text(contract.get("candidate")) not in ("", candidate):
        reasons.append("candidate_mismatch_contract")
    if contract_decision.get("promotion_state") != "FORMAL_RESEARCH_READY":
        reasons.append("formal_contract_not_ready")
    if protocol.get("protocol_status") != "PROTOCOL_READY":
        reasons.append("paper_shadow_protocol_not_ready")
    missing_fields = [
        field
        for field in PAPER_SHADOW_DAILY_REVIEW_FIELDS
        if not _daily_field_present(daily_fields, field)
    ]
    if missing_fields:
        reasons.append("missing_daily_review_fields")
    if not st._payload_safe(contract, contract_decision, protocol):
        reasons.append("unsafe_source_artifact")
    return reasons


def _input_artifact(source_id: str, path: Path) -> dict[str, Any]:
    exists = path.exists()
    return {
        "source_id": source_id,
        "path": str(path),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists else 0,
        "checksum_sha256": _sha256(path) if exists else "",
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _daily_field_present(payload: Mapping[str, Any], field: str) -> bool:
    if field == "hypothetical_weight_recommendation":
        raw = payload.get(field)
        value = _mapping(raw).get("value") if isinstance(raw, Mapping) else raw
        return bool(_text(value))
    return bool(_text(payload.get(field)))


_mapping = st._mapping
_records = st._records
_text = st._text
