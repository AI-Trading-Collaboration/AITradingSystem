# ruff: noqa: E501

from __future__ import annotations

from collections.abc import Mapping, Sequence
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

DEFAULT_SIGNAL_INPUT_RECOVERY_DIR = st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_input_recovery"
SIGNAL_INPUT_RECOVERY_STATUSES = (
    "SIGNAL_INPUTS_RESTORED",
    "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
    "SIGNAL_INPUTS_STILL_BLOCKED",
    "NO_RECOVERY_EVIDENCE",
)
SIGNAL_INPUT_RECOVERY_SAFETY = {
    **sic.SIGNAL_INPUT_COMPLETENESS_SAFETY,
    "signal_input_recovery_report_only": True,
    "upstream_pipeline_executed_by_report": False,
    "artifacts_fabricated": False,
    "manual_signal_artifact_fabrication": False,
    "production_effect": "none",
}
RECOVERY_INPUT_SCHEMA = "signal_input_recovery_input_snapshot.v2"
RECOVERY_VIEWS = (
    "signal_input_recovery_manifest.json",
    "signal_input_recovery_report.json",
    "signal_input_recovery_report.md",
    "reader_brief_section.md",
)
RECOVERY_FILES = (*RECOVERY_VIEWS, "signal_input_recovery_input_snapshot.json")

CANONICAL_GENERATION_PATHS = {
    "etf_feature_matrix": {
        "intended_command": "aits etf features --as-of <as_of>",
        "output_artifact": "data/etf_portfolio/features.csv",
    },
    "etf_signal_series": {
        "intended_command": "aits etf signals --as-of <as_of>",
        "output_artifact": "data/etf_portfolio/signals.csv",
    },
}

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text
_texts = st._texts


class DynamicV3SignalInputRecoveryError(ValueError):
    """Raised when signal recovery evidence cannot be reproduced."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SignalInputRecoveryError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def _time(payload: Mapping[str, Any], label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(payload.get("generated_at")).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DynamicV3SignalInputRecoveryError(f"invalid {label}.generated_at") from exc
    _require(parsed.tzinfo is not None, f"{label}.generated_at must be timezone-aware")
    return parsed.astimezone(UTC)


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
    previous: Mapping[str, Any] | None, restored: Mapping[str, Any], generated: datetime
) -> dict[str, Any]:
    current_report = _mapping(restored.get("signal_input_completeness_report"))
    current_time = _time(restored, "restored monitor")
    _require(current_time <= generated, "restored monitor generated after recovery")
    previous_report = (
        _mapping(previous.get("signal_input_completeness_report")) if previous else {}
    )
    previous_time = _time(previous, "previous monitor") if previous else None
    chronology = previous_time is not None and previous_time < current_time
    distinct = previous is not None and previous.get("monitor_id") != restored.get("monitor_id")
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
    if valid_transition and current_status == "OK":
        status = "SIGNAL_INPUTS_RESTORED"
    elif valid_transition and current_status == "WARNING":
        status = "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    elif valid_transition and current_status == "BLOCKING":
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
        "previous_status": previous_report.get("signal_input_status") or "MISSING",
        "current_status": current_status,
    }


def _classify_root_cause(input_id: str, issue_codes: Sequence[str]) -> str:
    issues = set(issue_codes)
    if "stale_signal_file" in issues:
        return "stale_cache"
    if "missing_signal_file" in issues and input_id == "latest_signal_snapshot":
        return "report_alias_or_snapshot_missing"
    if "missing_signal_file" in issues:
        return "upstream_artifact_missing"
    if "incompatible_schema_version" in issues:
        return "schema_mismatch"
    if "partial_market_coverage" in issues:
        return "market_coverage_gap"
    if "point_in_time_violation" in issues:
        return "point_in_time_violation"
    if "empty_signal_series_as_of" in issues:
        return "empty_signal_series_as_of"
    if "missing_required_feature_columns" in issues:
        return "schema_or_required_field_gap"
    return "unclassified_blocking_signal_input"


def _root_cause_rows(
    previous: Mapping[str, Any] | None, restored: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if previous is None:
        return []
    previous_findings = _records(previous.get("signal_input_completeness_findings"))
    restored_by_id = {
        _text(row.get("input_id")): row
        for row in _records(restored.get("signal_input_completeness_findings"))
    }
    rows: list[dict[str, Any]] = []
    for before in previous_findings:
        if before.get("severity") != "BLOCKING":
            continue
        input_id = _text(before.get("input_id"))
        after = _mapping(restored_by_id.get(input_id))
        rows.append({
            "schema_version": st.SCHEMA_VERSION,
            "input_id": input_id,
            "root_cause": _classify_root_cause(input_id, _texts(before.get("issue_codes"))),
            "issue_codes": _texts(before.get("issue_codes")),
            "previous_severity": before.get("severity"),
            "previous_latest_date": before.get("latest_date"),
            "restored_severity": after.get("severity") or "MISSING",
            "restored_latest_date": after.get("latest_date"),
            "restoration_result": (
                "RESTORED" if after.get("severity") in {"OK", "WARNING"} else "STILL_BLOCKED"
            ),
            "intended_command": _mapping(CANONICAL_GENERATION_PATHS.get(input_id)).get(
                "intended_command"
            ),
            "output_artifact": _mapping(CANONICAL_GENERATION_PATHS.get(input_id)).get(
                "output_artifact"
            ),
            **SIGNAL_INPUT_RECOVERY_SAFETY,
        })
    return rows


def _source_rows(restored: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "input_id": row.get("input_id"),
            "source_path": row.get("source_path"),
            "source_sha256": row.get("checksum_sha256"),
            "source_size_bytes": row.get("size_bytes"),
            "latest_date": row.get("latest_date"),
            "severity": row.get("severity"),
            "canonical_artifact_id": None,
        }
        for row in _records(restored.get("signal_input_completeness_findings"))
    ]


def _material(
    *, root: Path, recovery_id: str, previous: Mapping[str, Any] | None,
    restored: Mapping[str, Any], generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    transition = _transition(previous, restored, generated)
    current_report = _mapping(restored.get("signal_input_completeness_report"))
    root_rows = _root_cause_rows(previous, restored)
    source_rows = _source_rows(restored)
    status = _text(transition.get("status"))
    blockers = [
        f"{input_id}:blocking" for input_id in _texts(current_report.get("blocking_input_ids"))
    ]
    if status == "NO_RECOVERY_EVIDENCE":
        blockers.append("verifiable_prior_blocker_and_later_restored_monitor_required")
    counts: dict[str, int] = {}
    for row in root_rows:
        cause = _text(row.get("root_cause"))
        counts[cause] = counts.get(cause, 0) + 1
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_recovery_report",
        "recovery_id": recovery_id,
        "as_of": current_report.get("requested_as_of"),
        "requested_as_of": current_report.get("requested_as_of"),
        "generated_at": generated.isoformat(),
        "source_monitor_id": restored.get("monitor_id"),
        "restored_monitor_id": restored.get("monitor_id"),
        "previous_monitor_id": previous.get("monitor_id") if previous else None,
        "previous_signal_input_status": transition.get("previous_status"),
        "signal_input_status": transition.get("current_status"),
        "restoration_status": status,
        "transition_evidence": transition,
        "root_cause_rows": root_rows,
        "root_cause_summary": {
            "primary_root_cause": max(counts, key=counts.get) if counts else None,
            "root_cause_counts": counts,
            "root_cause_count": len(root_rows),
        },
        "canonical_generation_paths": CANONICAL_GENERATION_PATHS,
        "restored_source_bindings": source_rows,
        "restored_artifacts": [],
        "restored_etf_feature_matrix_artifact_id": None,
        "restored_etf_signal_series_artifact_id": None,
        "blocking_reasons": sorted(set(blockers)),
        "warnings": [
            f"{input_id}:warning" for input_id in _texts(current_report.get("warning_input_ids"))
        ],
        "next_required_action": _next_action(status, current_report),
        "hard_stop_triggered": status in {"SIGNAL_INPUTS_STILL_BLOCKED", "NO_RECOVERY_EVIDENCE"},
        **SIGNAL_INPUT_RECOVERY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_recovery_manifest",
        "recovery_id": recovery_id,
        "as_of": report.get("as_of"),
        "requested_as_of": report.get("requested_as_of"),
        "generated_at": generated.isoformat(),
        "status": status,
        "restoration_status": status,
        "source_monitor_id": report.get("source_monitor_id"),
        "previous_monitor_id": report.get("previous_monitor_id"),
        "signal_input_recovery_manifest_path": str(root / RECOVERY_VIEWS[0]),
        "signal_input_recovery_report_path": str(root / RECOVERY_VIEWS[1]),
        "signal_input_recovery_markdown_path": str(root / RECOVERY_VIEWS[2]),
        "reader_brief_section_path": str(root / RECOVERY_VIEWS[3]),
        "signal_input_recovery_input_snapshot_path": str(
            root / "signal_input_recovery_input_snapshot.json"
        ),
        **SIGNAL_INPUT_RECOVERY_SAFETY,
    }
    reader = render_signal_input_recovery_reader_brief(report)
    return manifest, {
        RECOVERY_VIEWS[0]: foundation._json_bytes(manifest),
        RECOVERY_VIEWS[1]: foundation._json_bytes(report),
        RECOVERY_VIEWS[2]: foundation._text_file_bytes(
            render_signal_input_recovery_report(manifest, report)
        ),
        RECOVERY_VIEWS[3]: foundation._text_file_bytes(reader),
    }


@with_artifact_validation_session
def run_signal_input_root_cause_recovery(
    *, as_of: date | None = None, restored_monitor_id: str | None = None,
    previous_monitor_id: str | None = None,
    signal_input_dir: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    policy_path: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    del as_of, policy_path
    generated = _generated_time(generated_at)
    _require(bool(restored_monitor_id), "explicit restored_monitor_id is required")
    restored = _validated_monitor(_text(restored_monitor_id), signal_input_dir)
    previous = (
        _validated_monitor(_text(previous_monitor_id), signal_input_dir)
        if previous_monitor_id else None
    )
    transition = _transition(previous, restored, generated)
    recovery_id = foundation._stable_id(
        "signal-input-root-cause-recovery", previous_monitor_id, restored_monitor_id,
        transition.get("status"), generated.isoformat(),
    )
    root = foundation._unique_dir(output_dir / recovery_id)
    _, views = _material(
        root=root, recovery_id=root.name, previous=previous, restored=restored, generated=generated
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    restored_source = foundation._artifact_binding(
        kind="signal_input_completeness", artifact_id=_text(restored_monitor_id),
        root=signal_input_dir / _text(restored_monitor_id), names=sic.COMPLETENESS_FILES,
    )
    previous_source = (
        foundation._artifact_binding(
            kind="signal_input_completeness", artifact_id=_text(previous_monitor_id),
            root=signal_input_dir / _text(previous_monitor_id), names=sic.COMPLETENESS_FILES,
        )
        if previous_monitor_id else None
    )
    snapshot = {
        "schema_version": RECOVERY_INPUT_SCHEMA,
        "recovery_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": _mapping(_mapping(restored.get("input_snapshot")).get("policy_source")),
        "previous_monitor_source": previous_source,
        "restored_monitor_source": restored_source,
        "view_hashes": foundation._view_hashes(root, RECOVERY_VIEWS),
        **SIGNAL_INPUT_RECOVERY_SAFETY,
    }
    foundation._write_snapshot(root / "signal_input_recovery_input_snapshot.json", snapshot)
    foundation._write_latest_pointer(
        "latest_signal_input_recovery", root.name, root / RECOVERY_VIEWS[0]
    )
    validation = validate_signal_input_recovery_artifact(
        recovery_id=root.name, output_dir=output_dir, write_output=True
    )
    payload = signal_input_recovery_report_payload(recovery_id=root.name, output_dir=output_dir)
    return {
        **payload, "recovery_dir": root,
        "manifest": foundation._read_json(root / RECOVERY_VIEWS[0]),
        "signal_input_recovery_validation": validation,
    }


def signal_input_recovery_report_payload(
    *, recovery_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=recovery_id, latest_pointer="latest_signal_input_recovery",
        latest=latest, output_dir=output_dir, required_name=RECOVERY_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / RECOVERY_VIEWS[0]),
        "signal_input_recovery_report": foundation._read_json(root / RECOVERY_VIEWS[1]),
        "reader_brief_section": (root / RECOVERY_VIEWS[3]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(root / "signal_input_recovery_input_snapshot.json"),
        "recovery_dir": str(root),
    }
    validation = st._read_optional_json(root / "signal_input_recovery_validation.json")
    if validation:
        payload["signal_input_recovery_validation"] = validation
    return payload


def _source_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    foundation._validate_artifact_binding(binding, kind="signal_input_completeness")
    monitor_id = _text(binding.get("artifact_id"))
    output_dir = Path(_text(binding.get("source_dir"))).parent
    return _validated_monitor(monitor_id, output_dir)


def _rebuild(root: Path, recovery_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(root / "signal_input_recovery_input_snapshot.json")
    foundation._validate_file_binding(_mapping(snapshot.get("policy_source")))
    restored = _source_payload(_mapping(snapshot.get("restored_monitor_source")))
    previous_binding = snapshot.get("previous_monitor_source")
    previous = _source_payload(_mapping(previous_binding)) if isinstance(previous_binding, Mapping) else None
    generated = _generated_time(
        datetime.fromisoformat(_text(snapshot.get("generated_at")).replace("Z", "+00:00"))
    )
    _, expected = _material(
        root=root, recovery_id=recovery_id, previous=previous, restored=restored, generated=generated
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_signal_input_recovery_artifact(
    *, recovery_id: str, output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / recovery_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root, snapshot_name="signal_input_recovery_input_snapshot.json",
        schema=RECOVERY_INPUT_SCHEMA, id_key="recovery_id", artifact_id=recovery_id,
        view_names=RECOVERY_VIEWS,
    )
    validation = (
        diagnosis._validate_content(
            report_type="etf_dynamic_v3_signal_input_recovery_validation",
            artifact_id=recovery_id, checks=checks, rebuild=lambda: _rebuild(root, recovery_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_signal_input_recovery_validation", recovery_id, checks
        )
    )
    if write_output:
        st._write_json(root / "signal_input_recovery_validation.json", validation)
        st._write_text(
            root / "signal_input_recovery_validation.md",
            render_signal_input_recovery_validation_report(validation),
        )
    return validation


def _next_action(status: str, report: Mapping[str, Any]) -> str:
    if status == "SIGNAL_INPUTS_RESTORED":
        return _text(report.get("next_required_action"), "continue_manual_research_review")
    if status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS":
        return "review_signal_input_warnings_before_continuing"
    if status == "SIGNAL_INPUTS_STILL_BLOCKED":
        return "stop_and_restore_signal_inputs"
    return "collect_verifiable_prior_blocker_and_later_restored_monitor"


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def render_signal_input_recovery_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Signal Input Recovery", "",
        f"- signal_input_recovery_id: {report.get('recovery_id')}",
        f"- signal_input_recovery_status: {report.get('restoration_status')}",
        f"- previous_monitor_id: {report.get('previous_monitor_id') or 'none'}",
        f"- restored_monitor_id: {report.get('restored_monitor_id')}",
        f"- root_cause_count: {_mapping(report.get('root_cause_summary')).get('root_cause_count')}",
        f"- next_required_action: {report.get('next_required_action')}",
        "- safety: recovery evidence only / no fabricated artifact ids / no production", "",
    ])


def render_signal_input_recovery_report(
    manifest: Mapping[str, Any], report: Mapping[str, Any]
) -> str:
    rows = [
        f"- {row.get('input_id')}: root_cause={row.get('root_cause')} "
        f"previous={row.get('previous_severity')} restored={row.get('restored_severity')}"
        for row in _records(report.get("root_cause_rows"))
    ]
    return "\n".join([
        f"# Signal Input Root Cause Recovery {manifest.get('recovery_id')}", "",
        f"- restoration_status：{report.get('restoration_status')}",
        f"- previous_monitor_id：{report.get('previous_monitor_id')}",
        f"- restored_monitor_id：{report.get('restored_monitor_id')}",
        f"- next_required_action：{report.get('next_required_action')}", "",
        "## Root Cause Rows", *(rows or ["- none: no verifiable prior blocker"]), "",
        "- Source paths/hashes are evidence bindings; this report does not synthesize producer artifact ids.",
        "- safety：report-only / no upstream execution / no broker / no production", "",
    ])


def render_signal_input_recovery_validation_report(validation: Mapping[str, Any]) -> str:
    rows = [f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}" for row in _records(validation.get("checks"))]
    return "\n".join([
        f"# Signal Input Recovery Validation {validation.get('artifact_id')}", "",
        f"- status: {validation.get('status')}",
        f"- failed_check_count: {validation.get('failed_check_count')}",
        "- production_effect: none", "", "## Checks", *rows, "",
    ])
