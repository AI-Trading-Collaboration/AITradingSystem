# ruff: noqa: E501

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as sic
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import (
    cached_artifact_validation,
    with_artifact_validation_session,
)

DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_input_completeness_recovery"
)
SIGNAL_INPUT_COMPLETENESS_RECOVERY_STATUSES = (
    "SIGNAL_INPUTS_RESTORED",
    "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
    "SIGNAL_INPUTS_STILL_BLOCKED",
    "NO_RECOVERY_EVIDENCE",
)
SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY = {
    **sic.SIGNAL_INPUT_COMPLETENESS_SAFETY,
    "signal_input_completeness_recovery_only": True,
    "completeness_monitor_rerun_by_recovery": True,
    "upstream_generation_executed_by_recovery": False,
    "signal_artifacts_fabricated": False,
    "completeness_policy_weakened": False,
    "production_effect": "none",
}
RECOVERY_INPUT_SCHEMA = "signal_input_completeness_recovery_input_snapshot.v2"
RECOVERY_VIEWS = (
    "signal_input_completeness_recovery_manifest.json",
    "signal_input_completeness_recovery_report.json",
    "signal_input_completeness_recovery_report.md",
    "reader_brief_section.md",
)
RECOVERY_FILES = (*RECOVERY_VIEWS, "signal_input_completeness_recovery_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text
_texts = st._texts


class DynamicV3SignalCompletenessRecoveryError(ValueError):
    """Raised when a recovery transition cannot be reproduced."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SignalCompletenessRecoveryError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _time(payload: Mapping[str, Any], field: str) -> datetime:
    try:
        value = datetime.fromisoformat(_text(payload.get("generated_at")).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DynamicV3SignalCompletenessRecoveryError(f"invalid {field}.generated_at") from exc
    _require(value.tzinfo is not None, f"{field}.generated_at must be timezone-aware")
    return value.astimezone(UTC)


def _validated_monitor(monitor_id: str, output_dir: Path) -> dict[str, Any]:
    validation = cached_artifact_validation(
        validator=sic.validate_signal_input_completeness_artifact,
        validator_key="monitor_id", artifact_id=monitor_id, root=output_dir,
    )
    _require(validation.get("status") == "PASS", f"signal monitor validation failed: {monitor_id}")
    payload = sic.signal_input_completeness_report_payload(
        monitor_id=monitor_id, output_dir=output_dir
    )
    _require(payload.get("monitor_id") == monitor_id, "signal monitor id mismatch")
    return payload


def _transition(
    previous: Mapping[str, Any] | None, current: Mapping[str, Any], generated: datetime
) -> dict[str, Any]:
    current_report = _mapping(current.get("signal_input_completeness_report"))
    current_time = _time(current, "restored monitor")
    _require(current_time <= generated, "restored monitor generated after recovery")
    previous_report = (
        _mapping(previous.get("signal_input_completeness_report")) if previous else {}
    )
    previous_time = _time(previous, "prior monitor") if previous else None
    chronology = previous_time is not None and previous_time < current_time
    distinct = previous is not None and previous.get("monitor_id") != current.get("monitor_id")
    prior_blocking = previous_report.get("signal_input_status") == "BLOCKING"
    current_status = _text(current_report.get("signal_input_status"), "BLOCKING")
    policy_lineage = bool(
        previous
        and previous_report.get("policy_id")
        and previous_report.get("policy_id") == current_report.get("policy_id")
        and previous_report.get("policy_version")
        and previous_report.get("policy_version") == current_report.get("policy_version")
    )
    as_of_lineage = bool(
        previous
        and previous_report.get("requested_as_of")
        and previous_report.get("requested_as_of") == current_report.get("requested_as_of")
    )
    valid_transition = (
        prior_blocking and chronology and distinct and policy_lineage and as_of_lineage
    )
    restored = valid_transition and current_status in {"OK", "WARNING"}
    attempted_but_blocked = valid_transition and current_status == "BLOCKING"
    if restored and current_status == "WARNING":
        status = "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    elif restored:
        status = "SIGNAL_INPUTS_RESTORED"
    elif attempted_but_blocked:
        status = "SIGNAL_INPUTS_STILL_BLOCKED"
    else:
        status = "NO_RECOVERY_EVIDENCE"
    return {
        "status": status,
        "prior_blocking": prior_blocking,
        "chronology_valid": chronology,
        "distinct_monitors": distinct,
        "policy_lineage_valid": policy_lineage,
        "requested_as_of_lineage_valid": as_of_lineage,
        "restored": restored,
        "previous_status": previous_report.get("signal_input_status") or "MISSING",
        "current_status": current_status,
    }


def _material(
    *, root: Path, recovery_id: str, previous: Mapping[str, Any] | None,
    current: Mapping[str, Any], generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    transition = _transition(previous, current, generated)
    current_report = _mapping(current.get("signal_input_completeness_report"))
    findings = _records(current.get("signal_input_completeness_findings"))
    status = _text(transition.get("status"))
    blockers = [
        f"{input_id}:blocking" for input_id in _texts(current_report.get("blocking_input_ids"))
    ]
    if status == "NO_RECOVERY_EVIDENCE":
        blockers.append("verifiable_prior_blocker_and_later_restored_monitor_required")
    warnings = [
        f"{input_id}:warning" for input_id in _texts(current_report.get("warning_input_ids"))
    ]
    for row in findings:
        if row.get("severity") == "WARNING":
            warnings.extend(
                f"{row.get('input_id')}:{code}" for code in _texts(row.get("issue_codes"))
            )
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_recovery_report",
        "recovery_id": recovery_id,
        "as_of": current_report.get("requested_as_of"),
        "requested_as_of": current_report.get("requested_as_of"),
        "generated_at": generated.isoformat(),
        "policy_id": current_report.get("policy_id"),
        "policy_version": current_report.get("policy_version"),
        "prior_monitor_id": previous.get("monitor_id") if previous else None,
        "source_monitor_id": current.get("monitor_id"),
        "restored_monitor_id": current.get("monitor_id"),
        "previous_signal_input_status": transition.get("previous_status"),
        "signal_input_status": transition.get("current_status"),
        "recovery_status": status,
        "transition_evidence": transition,
        "blocking_count": current_report.get("blocking_count"),
        "warning_count": current_report.get("warning_count"),
        "blocker_list": sorted(set(blockers)),
        "warning_list": sorted(set(warnings)),
        "recovery_checks": [
            {"check_id": "prior_monitor_blocking", "passed": transition.get("prior_blocking") is True},
            {"check_id": "monitor_chronology", "passed": transition.get("chronology_valid") is True},
            {"check_id": "distinct_monitor_artifacts", "passed": transition.get("distinct_monitors") is True},
            {"check_id": "policy_lineage", "passed": transition.get("policy_lineage_valid") is True},
            {"check_id": "requested_as_of_lineage", "passed": transition.get("requested_as_of_lineage_valid") is True},
            {"check_id": "later_monitor_non_blocking", "passed": transition.get("restored") is True},
        ],
        "check_summary": {
            "check_count": 6,
            "failed_check_count": sum(
                value is not True
                for value in (
                    transition.get("prior_blocking"), transition.get("chronology_valid"),
                    transition.get("distinct_monitors"), transition.get("policy_lineage_valid"),
                    transition.get("requested_as_of_lineage_valid"), transition.get("restored"),
                )
            ),
        },
        "hard_stop_triggered": status in {"SIGNAL_INPUTS_STILL_BLOCKED", "NO_RECOVERY_EVIDENCE"},
        "next_required_action": _next_action(status),
        **SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_recovery_manifest",
        "recovery_id": recovery_id,
        "as_of": report.get("as_of"),
        "requested_as_of": report.get("requested_as_of"),
        "generated_at": generated.isoformat(),
        "status": status,
        "recovery_status": status,
        "prior_monitor_id": report.get("prior_monitor_id"),
        "source_monitor_id": report.get("source_monitor_id"),
        "restored_monitor_id": report.get("restored_monitor_id"),
        "signal_input_completeness_recovery_manifest_path": str(root / RECOVERY_VIEWS[0]),
        "signal_input_completeness_recovery_report_path": str(root / RECOVERY_VIEWS[1]),
        "signal_input_completeness_recovery_markdown_path": str(root / RECOVERY_VIEWS[2]),
        "reader_brief_section_path": str(root / RECOVERY_VIEWS[3]),
        "signal_input_completeness_recovery_input_snapshot_path": str(
            root / "signal_input_completeness_recovery_input_snapshot.json"
        ),
        **SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY,
    }
    reader = render_signal_input_completeness_recovery_reader_brief(report)
    return manifest, {
        RECOVERY_VIEWS[0]: foundation._json_bytes(manifest),
        RECOVERY_VIEWS[1]: foundation._json_bytes(report),
        RECOVERY_VIEWS[2]: foundation._text_file_bytes(
            render_signal_input_completeness_recovery_report(manifest, report)
        ),
        RECOVERY_VIEWS[3]: foundation._text_file_bytes(reader),
    }


@with_artifact_validation_session
def run_signal_input_completeness_recovery(
    *, as_of: date | None = None, monitor_id: str | None = None,
    prior_monitor_id: str | None = None, restored_monitor_id: str | None = None,
    rerun_monitor: bool = True,
    policy_path: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    signal_input_dir: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    prior_id = prior_monitor_id or monitor_id
    previous = _validated_monitor(prior_id, signal_input_dir) if prior_id else None
    if restored_monitor_id:
        current = _validated_monitor(restored_monitor_id, signal_input_dir)
    elif rerun_monitor:
        result = sic.run_signal_input_completeness_monitor(
            as_of=as_of, policy_path=policy_path, output_dir=signal_input_dir,
            generated_at=generated,
        )
        current = _validated_monitor(_text(result.get("monitor_id")), signal_input_dir)
    elif prior_id:
        current = _validated_monitor(prior_id, signal_input_dir)
    else:
        raise DynamicV3SignalCompletenessRecoveryError(
            "restored_monitor_id or rerun_monitor is required"
        )
    transition = _transition(previous, current, generated)
    recovery_id = foundation._stable_id(
        "signal-input-completeness-recovery", prior_id, current.get("monitor_id"),
        transition.get("status"), generated.isoformat(),
    )
    root = foundation._unique_dir(output_dir / recovery_id)
    _, views = _material(
        root=root, recovery_id=root.name, previous=previous, current=current, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    current_source = foundation._artifact_binding(
        kind="signal_input_completeness", artifact_id=_text(current.get("monitor_id")),
        root=signal_input_dir / _text(current.get("monitor_id")), names=sic.COMPLETENESS_FILES,
    )
    previous_source = (
        foundation._artifact_binding(
            kind="signal_input_completeness", artifact_id=_text(previous.get("monitor_id")),
            root=signal_input_dir / _text(previous.get("monitor_id")), names=sic.COMPLETENESS_FILES,
        )
        if previous else None
    )
    policy_source = _mapping(_mapping(current.get("input_snapshot")).get("policy_source"))
    snapshot = {
        "schema_version": RECOVERY_INPUT_SCHEMA,
        "recovery_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "prior_monitor_source": previous_source,
        "restored_monitor_source": current_source,
        "view_hashes": foundation._view_hashes(root, RECOVERY_VIEWS),
        **SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY,
    }
    foundation._write_snapshot(
        root / "signal_input_completeness_recovery_input_snapshot.json", snapshot
    )
    foundation._write_latest_pointer(
        "latest_signal_input_completeness_recovery", root.name, root / RECOVERY_VIEWS[0]
    )
    validation = validate_signal_input_completeness_recovery_artifact(
        recovery_id=root.name, output_dir=output_dir, write_output=True
    )
    payload = signal_input_completeness_recovery_report_payload(
        recovery_id=root.name, output_dir=output_dir
    )
    return {
        **payload, "recovery_dir": root,
        "manifest": foundation._read_json(root / RECOVERY_VIEWS[0]),
        "signal_input_completeness_recovery_validation": validation,
        "source_monitor_payload": current,
    }


def signal_input_completeness_recovery_report_payload(
    *, recovery_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=recovery_id, latest_pointer="latest_signal_input_completeness_recovery",
        latest=latest, output_dir=output_dir, required_name=RECOVERY_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / RECOVERY_VIEWS[0]),
        "signal_input_completeness_recovery_report": foundation._read_json(root / RECOVERY_VIEWS[1]),
        "reader_brief_section": (root / RECOVERY_VIEWS[3]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(
            root / "signal_input_completeness_recovery_input_snapshot.json"
        ),
        "recovery_dir": str(root),
    }
    validation = st._read_optional_json(root / "signal_input_completeness_recovery_validation.json")
    if validation:
        payload["signal_input_completeness_recovery_validation"] = validation
    return payload


def _source_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    foundation._validate_artifact_binding(binding, kind="signal_input_completeness")
    monitor_id = _text(binding.get("artifact_id"))
    root = Path(_text(binding.get("source_dir"))).parent
    return _validated_monitor(monitor_id, root)


def _rebuild(root: Path, recovery_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(
        root / "signal_input_completeness_recovery_input_snapshot.json"
    )
    foundation._validate_file_binding(_mapping(snapshot.get("policy_source")))
    current = _source_payload(_mapping(snapshot.get("restored_monitor_source")))
    previous_binding = snapshot.get("prior_monitor_source")
    previous = _source_payload(_mapping(previous_binding)) if isinstance(previous_binding, Mapping) else None
    generated = _generated_time(
        datetime.fromisoformat(_text(snapshot.get("generated_at")).replace("Z", "+00:00"))
    )
    _, expected = _material(
        root=root, recovery_id=recovery_id, previous=previous, current=current, generated=generated
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_signal_input_completeness_recovery_artifact(
    *, recovery_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / recovery_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root, snapshot_name="signal_input_completeness_recovery_input_snapshot.json",
        schema=RECOVERY_INPUT_SCHEMA, id_key="recovery_id", artifact_id=recovery_id,
        view_names=RECOVERY_VIEWS,
    )
    validation = (
        diagnosis._validate_content(
            report_type="etf_dynamic_v3_signal_input_completeness_recovery_validation",
            artifact_id=recovery_id, checks=checks, rebuild=lambda: _rebuild(root, recovery_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_signal_input_completeness_recovery_validation", recovery_id, checks
        )
    )
    if write_output:
        st._write_json(root / "signal_input_completeness_recovery_validation.json", validation)
        st._write_text(
            root / "signal_input_completeness_recovery_validation.md",
            render_signal_input_completeness_recovery_validation_report(validation),
        )
    return validation


def _next_action(status: str) -> str:
    if status == "SIGNAL_INPUTS_RESTORED":
        return "continue_manual_research_review"
    if status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS":
        return "review_signal_input_warnings_before_continuing"
    if status == "SIGNAL_INPUTS_STILL_BLOCKED":
        return "stop_and_restore_signal_inputs"
    return "collect_verifiable_prior_blocker_and_later_restored_monitor"


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def render_signal_input_completeness_recovery_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Signal Input Completeness Recovery", "",
        f"- signal_input_completeness_recovery_id: {report.get('recovery_id')}",
        f"- signal_input_completeness_recovery_status: {report.get('recovery_status')}",
        f"- prior_monitor_id: {report.get('prior_monitor_id') or 'none'}",
        f"- restored_monitor_id: {report.get('restored_monitor_id')}",
        f"- next_required_action: {report.get('next_required_action')}",
        "- safety: evidence-only transition / no artifact fabrication / no production", "",
    ])


def render_signal_input_completeness_recovery_report(
    manifest: Mapping[str, Any], report: Mapping[str, Any]
) -> str:
    return "\n".join([
        f"# Signal Input Completeness Recovery {manifest.get('recovery_id')}", "",
        f"- recovery_status：{report.get('recovery_status')}",
        f"- previous_status：{report.get('previous_signal_input_status')}",
        f"- current_status：{report.get('signal_input_status')}",
        f"- prior_monitor_id：{report.get('prior_monitor_id')}",
        f"- restored_monitor_id：{report.get('restored_monitor_id')}",
        f"- next_required_action：{report.get('next_required_action')}",
        "- RESTORED requires a validated prior BLOCKING monitor and a distinct later non-blocking monitor.",
        "- safety：monitor-only / no signal fabrication / no broker / no production", "",
    ])


def render_signal_input_completeness_recovery_validation_report(
    validation: Mapping[str, Any]
) -> str:
    rows = [f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}" for row in _records(validation.get("checks"))]
    return "\n".join([
        f"# Signal Input Completeness Recovery Validation {validation.get('artifact_id')}", "",
        f"- status: {validation.get('status')}",
        f"- failed_check_count: {validation.get('failed_check_count')}",
        "- production_effect: none", "", "## Checks", *rows, "",
    ])
