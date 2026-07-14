from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as legacy
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_hardening as hardening
from ai_trading_system.etf_portfolio import dynamic_v3_system_target_portfolio as target_core
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_promotion as promotion,
)
from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_readiness as readiness,
)
from ai_trading_system.etf_portfolio.dynamic_v3_confirmation_operations import (
    _file_bytes_match,
    _json_bytes,
    _jsonl_bytes,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _check,
    _mapping,
    _read_json,
    _read_jsonl,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
)
from ai_trading_system.etf_portfolio.dynamic_v3_pressure_validation import (
    _write_views_atomic,
)

SMOOTHED_FORWARD_PROGRESS_SNAPSHOT_SCHEMA = "smoothed_forward_progress_input_snapshot.v2"
SMOOTHED_WEEKLY_DASHBOARD_SNAPSHOT_SCHEMA = "smoothed_weekly_dashboard_input_snapshot.v2"
SMOOTHED_EVENT_MONITOR_SNAPSHOT_SCHEMA = "smoothed_event_monitor_input_snapshot.v2"
SMOOTHED_SWITCH_READINESS_SNAPSHOT_SCHEMA = "smoothed_switch_readiness_input_snapshot.v2"
SMOOTHED_OWNER_RENEWAL_SNAPSHOT_SCHEMA = "smoothed_owner_renewal_input_snapshot.v2"

DEFAULT_SMOOTHED_FORWARD_BINDING_DIR = promotion.DEFAULT_SMOOTHED_FORWARD_BINDING_DIR
DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR = promotion.DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR
DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR = promotion.DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR
DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR = legacy.DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR
DEFAULT_SMOOTHED_EVENT_MONITOR_DIR = legacy.DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
DEFAULT_SMOOTHED_SWITCH_READINESS_DIR = legacy.DEFAULT_SMOOTHED_SWITCH_READINESS_DIR
DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR = legacy.DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR
DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR = legacy.DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR
DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR = legacy.DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR
SYSTEM_TARGET_SAFETY = promotion.SYSTEM_TARGET_SAFETY


class DynamicV3SmoothedOperationsError(ValueError):
    """Raised when smoothed operations evidence cannot be reproduced exactly."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SmoothedOperationsError(message)


def _generated_at(value: datetime | None) -> datetime:
    try:
        return target_core._generated_at(value)
    except Exception as exc:  # noqa: BLE001
        raise DynamicV3SmoothedOperationsError(str(exc)) from exc


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    artifact_id_key: str,
) -> dict[str, Any]:
    return target_core._validation_payload(
        report_type,
        artifact_id,
        checks,
        artifact_id_key=artifact_id_key,
    )


def _write(root: Path, views: Mapping[str, bytes], pointer: str, manifest: str) -> None:
    _write_views_atomic(root, views)
    _update_latest_pointer(pointer, root.name, root / manifest)


def _view_errors(root: Path, views: Mapping[str, bytes]) -> list[str]:
    return [name for name, payload in views.items() if not _file_bytes_match(root / name, payload)]


def _artifact_root(
    output_dir: Path,
    artifact_id: str | None,
    latest: bool,
    pointer: str,
) -> Path:
    return hardening._artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=artifact_id if not latest else None,
        pointer_name=pointer,
    )


def _simple_payload(
    binding: Mapping[str, Any], manifest: str, views: Mapping[str, str]
) -> dict[str, Any]:
    return promotion._simple_payload(binding, manifest, views)


def _local_source_binding(
    *,
    kind: str,
    artifact_id: str,
    root: Path,
    validator: Any,
    validator_key: str,
    json_views: Sequence[str],
    jsonl_views: Sequence[str] = (),
    text_views: Sequence[str] = (),
) -> dict[str, Any]:
    return readiness._source_binding(
        kind=kind,
        artifact_id=artifact_id,
        root=root,
        validator=validator,
        validator_key=validator_key,
        json_views=tuple(name for name in json_views if not name.endswith("_input_snapshot.json")),
        jsonl_views=jsonl_views,
        text_views=text_views,
    )


def _validate_local_binding(
    binding: Mapping[str, Any],
    *,
    kind: str,
    validator: Any,
    validator_key: str,
) -> list[str]:
    return readiness._validate_binding(
        binding,
        kind=kind,
        validator=validator,
        validator_key=validator_key,
    )


def _source_manifest(binding: Mapping[str, Any], name: str) -> dict[str, Any]:
    return hardening._bundle_json(binding, name)


def _source_jsonl(binding: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    return hardening._bundle_jsonl(binding, name)


def _chronology(generated: datetime, *manifests: Mapping[str, Any]) -> None:
    for index, manifest in enumerate(manifests):
        source_time = target_core._datetime(
            manifest.get("generated_at"), field=f"operations source {index} generated_at"
        )
        _require(source_time <= generated, "operations source generated after consumer")


def _candidate(value: Any) -> str | None:
    if value is None:
        return None
    candidate = _text(value)
    _require(bool(candidate), "candidate_method must be null or non-empty")
    return candidate


def _positive_integer(value: Any, *, field: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{field} must be a positive integer",
    )
    return int(value)


def _safe_payload(*payloads: Mapping[str, Any]) -> bool:
    for payload in payloads:
        if payload.get("broker_action_allowed") is not False:
            return False
        if payload.get("production_effect") != "none":
            return False
        if payload.get("not_official_target_weights") is not True:
            return False
    return True


def _progress_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_forward_progress",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_forward_progress_artifact,
        validator_key="progress_id",
        json_views=(
            "smoothed_forward_progress_input_snapshot.json",
            "smoothed_forward_progress_manifest.json",
            "progress_evidence_commitments.json",
            "smoothed_forward_progress_summary.json",
        ),
        jsonl_views=("smoothed_target_progress.jsonl",),
        text_views=("smoothed_forward_progress_report.md", "reader_brief_section.md"),
    )


def _progress_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_source_manifest(binding, "smoothed_forward_progress_manifest.json"),
        "progress_evidence_commitments": _source_manifest(
            binding, "progress_evidence_commitments.json"
        ),
        "smoothed_forward_progress_summary": _source_manifest(
            binding, "smoothed_forward_progress_summary.json"
        ),
        "smoothed_target_progress": _source_jsonl(binding, "smoothed_target_progress.jsonl"),
    }


def _dashboard_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_weekly_dashboard",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_weekly_dashboard_artifact,
        validator_key="dashboard_id",
        json_views=(
            "smoothed_weekly_dashboard_input_snapshot.json",
            "smoothed_weekly_dashboard_manifest.json",
            "smoothed_dashboard_summary.json",
            "smoothed_target_status_table.json",
        ),
        text_views=("smoothed_weekly_dashboard_report.md", "reader_brief_section.md"),
    )


def _dashboard_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_weekly_dashboard_manifest.json",
        {
            "smoothed_dashboard_summary": "smoothed_dashboard_summary.json",
            "smoothed_target_status_table": "smoothed_target_status_table.json",
        },
    )


def _monitor_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_event_monitor",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_event_monitor_artifact,
        validator_key="monitor_id",
        json_views=(
            "smoothed_event_monitor_input_snapshot.json",
            "smoothed_event_monitor_manifest.json",
            "event_accumulation_summary.json",
        ),
        jsonl_views=("sideways_event_inventory.jsonl", "recovery_event_inventory.jsonl"),
        text_views=("smoothed_event_monitor_report.md",),
    )


def _monitor_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **_source_manifest(binding, "smoothed_event_monitor_manifest.json"),
        "event_accumulation_summary": _source_manifest(binding, "event_accumulation_summary.json"),
        "sideways_event_inventory": _source_jsonl(binding, "sideways_event_inventory.jsonl"),
        "recovery_event_inventory": _source_jsonl(binding, "recovery_event_inventory.jsonl"),
    }


def _recheck_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_switch_readiness",
        artifact_id=artifact_id,
        root=root,
        validator=validate_smoothed_switch_readiness_artifact,
        validator_key="recheck_id",
        json_views=(
            "smoothed_switch_readiness_input_snapshot.json",
            "smoothed_switch_readiness_manifest.json",
            "switch_readiness_decision.json",
            "switch_readiness_criteria.json",
        ),
        text_views=("smoothed_switch_readiness_report.md", "reader_brief_section.md"),
    )


def _recheck_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_switch_readiness_manifest.json",
        {
            "switch_readiness_decision": "switch_readiness_decision.json",
            "switch_readiness_criteria": "switch_readiness_criteria.json",
        },
    )


def _owner_promotion_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_owner_promotion",
        artifact_id=artifact_id,
        root=root,
        validator=promotion.validate_smoothed_owner_promotion_artifact,
        validator_key="decision_id",
        json_views=(
            "smoothed_owner_promotion_input_snapshot.json",
            "smoothed_owner_promotion_manifest.json",
            "owner_promotion_decision.json",
        ),
        text_views=(
            "owner_promotion_checklist.md",
            "smoothed_owner_promotion_report.md",
            "reader_brief_section.md",
        ),
    )


def _owner_promotion_payload(binding: Mapping[str, Any]) -> dict[str, Any]:
    return _simple_payload(
        binding,
        "smoothed_owner_promotion_manifest.json",
        {"owner_promotion_decision": "owner_promotion_decision.json"},
    )


def _outcome_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_outcome_update",
        artifact_id=artifact_id,
        root=root,
        validator=legacy.validate_smoothed_outcome_update_artifact,
        validator_key="update_id",
        json_views=(
            "smoothed_outcome_update_manifest.json",
            "smoothed_outcome_delta_summary.json",
        ),
        jsonl_views=("updated_smoothed_outcomes.jsonl",),
    )


def _classification_binding(artifact_id: str, root: Path) -> dict[str, Any]:
    return _local_source_binding(
        kind="smoothed_forward_classification",
        artifact_id=artifact_id,
        root=root,
        validator=legacy.validate_smoothed_forward_classification_artifact,
        validator_key="classification_id",
        json_views=(
            "smoothed_forward_classification_manifest.json",
            "smoothed_forward_classification_summary.json",
        ),
        jsonl_views=("classified_forward_events.jsonl",),
    )


def _source_event_rows(
    sources: Sequence[Mapping[str, Any]],
    *,
    manifest_name: str,
    rows_name: str,
    binding_id: str,
    candidate: str | None,
    target_ids: set[str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for source in sources:
        manifest = _source_manifest(source, manifest_name)
        _require(manifest.get("binding_id") == binding_id, "operations evidence binding mismatch")
        _require(
            manifest.get("candidate_method") == candidate,
            "operations evidence candidate mismatch",
        )
        for row in _source_jsonl(source, rows_name):
            target_id = _text(row.get("target_id"))
            _require(target_id in target_ids, "operations evidence target mismatch")
            event_id = _text(row.get("event_id"))
            window = row.get("window_days")
            _require(event_id and isinstance(window, int) and window > 0, "event identity invalid")
            key = (event_id, int(window), target_id)
            _require(key not in seen, "duplicate operations evidence event")
            seen.add(key)
            rows.append(dict(row))
    return rows


def _required_key(row: Mapping[str, Any]) -> str:
    keys = [
        key
        for key in (
            "required_forward_events",
            "required_sideways_events",
            "required_recovery_events",
        )
        if key in row
    ]
    _require(len(keys) == 1, "target must define exactly one event requirement")
    return keys[0]


def _progress_targets(
    bound: Mapping[str, Any],
    outcomes: Sequence[Mapping[str, Any]],
    classifications: Sequence[Mapping[str, Any]],
    generated_at: str,
) -> list[dict[str, Any]]:
    candidate = _candidate(bound.get("candidate_method"))
    source_targets = _records(bound.get("targets"))
    _require(
        (
            candidate is None
            and not source_targets
            and bound.get("binding_status") == "NOT_REGISTERED"
        )
        or (candidate is not None and bool(source_targets)),
        "binding candidate/targets semantics invalid",
    )
    result: list[dict[str, Any]] = []
    for source in source_targets:
        target_id = _text(source.get("target_id"))
        _require(target_id and source.get("method") == candidate, "target candidate mismatch")
        key = _required_key(source)
        required = _positive_integer(source.get(key), field=key)
        evidence_rows = [row for row in outcomes if row.get("target_id") == target_id]
        class_rows = [row for row in classifications if row.get("target_id") == target_id]
        relevant = class_rows if key != "required_forward_events" else evidence_rows
        status_field = "event_status" if key != "required_forward_events" else "outcome_status"
        available = len(
            {
                _text(row.get("event_id"))
                for row in relevant
                if row.get(status_field) == "AVAILABLE" and row.get("event_id")
            }
        )
        windows = [int(item) for item in source.get("windows", []) if isinstance(item, int)]
        by_window = {
            str(window): len(
                {
                    _text(row.get("event_id"))
                    for row in relevant
                    if row.get(status_field) == "AVAILABLE"
                    and row.get("window_days") == window
                    and row.get("event_id")
                }
            )
            for window in windows
        }
        watch_only = source.get("status") == "WATCH_ONLY" or key == "required_recovery_events"
        progress_status = (
            "READY_FOR_REVIEW"
            if available >= required
            else ("IN_PROGRESS" if available > 0 else "INSUFFICIENT_EVENTS")
        )
        result.append(
            {
                "schema_version": 2,
                "target_id": target_id,
                "method": candidate,
                "baseline": source.get("baseline"),
                key: required,
                "available_events": available,
                "available_forward_events": available,
                "available_by_window": by_window,
                "current_metrics": {
                    "evidence_event_count": available,
                    "lag_warning_count": sum(
                        1 for row in class_rows if row.get("lag_warning") is True
                    ),
                },
                "progress_status": progress_status,
                "blocking_reasons": []
                if available >= required
                else [f"insufficient_{key.removeprefix('required_')}"],
                "watch_only": watch_only,
                "last_updated": generated_at,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return result


def _target_required(row: Mapping[str, Any]) -> int:
    for key in (
        "required_forward_events",
        "required_sideways_events",
        "required_recovery_events",
    ):
        if key in row:
            return int(row.get(key, 0))
    return 0


def _summary(
    binding_id: str,
    candidate: str | None,
    binding_status: str,
    targets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    def totals(key: str) -> tuple[int, int]:
        rows = [row for row in targets if key in row]
        return (
            sum(int(row.get(key, 0)) for row in rows),
            sum(int(row.get("available_events", 0)) for row in rows),
        )

    forward_required, forward_available = totals("required_forward_events")
    sideways_required, sideways_available = totals("required_sideways_events")
    recovery_required, recovery_available = totals("required_recovery_events")
    return {
        "schema_version": 2,
        "progress_id": "",
        "binding_id": binding_id,
        "candidate_method": candidate,
        "binding_status": binding_status,
        "progress_status": "NOT_REGISTERED" if candidate is None else "IN_PROGRESS",
        "targets_total": len(targets),
        "ready_for_review_count": sum(
            row.get("progress_status") == "READY_FOR_REVIEW" for row in targets
        ),
        "in_progress_count": sum(
            row.get("progress_status") in {"IN_PROGRESS", "INSUFFICIENT_EVENTS"} for row in targets
        ),
        "watch_only_count": sum(row.get("watch_only") is True for row in targets),
        "required_forward_events_total": forward_required,
        "available_forward_events_total": forward_available,
        "required_sideways_events": sideways_required,
        "available_sideways_events": sideways_available,
        "required_recovery_events": recovery_required,
        "available_recovery_events": recovery_available,
        "summary_recommendation": (
            "request_more_forward_data" if candidate is None else "continue_observation"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _progress_views(
    snapshot: Mapping[str, Any], *, progress_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    binding = promotion._binding_payload(_mapping(snapshot.get("binding_source")))
    bound = _mapping(binding.get("bound_confirmation_targets"))
    candidate = _candidate(bound.get("candidate_method"))
    target_ids = {_text(row.get("target_id")) for row in _records(bound.get("targets"))}
    outcomes = _source_event_rows(
        _records(snapshot.get("outcome_sources")),
        manifest_name="smoothed_outcome_update_manifest.json",
        rows_name="updated_smoothed_outcomes.jsonl",
        binding_id=_text(binding.get("binding_id")),
        candidate=candidate,
        target_ids=target_ids,
    )
    classifications = _source_event_rows(
        _records(snapshot.get("classification_sources")),
        manifest_name="smoothed_forward_classification_manifest.json",
        rows_name="classified_forward_events.jsonl",
        binding_id=_text(binding.get("binding_id")),
        candidate=candidate,
        target_ids=target_ids,
    )
    targets = _progress_targets(
        bound, outcomes, classifications, _text(snapshot.get("generated_at"))
    )
    summary = _summary(
        _text(binding.get("binding_id")),
        candidate,
        _text(bound.get("binding_status")),
        targets,
    )
    summary["progress_id"] = progress_id
    commitments = {
        "schema_version": 2,
        "progress_id": progress_id,
        "binding_id": binding.get("binding_id"),
        "candidate_method": candidate,
        "target_ids": sorted(target_ids),
        "outcome_source_ids": [
            row.get("artifact_id") for row in _records(snapshot.get("outcome_sources"))
        ],
        "classification_source_ids": [
            row.get("artifact_id") for row in _records(snapshot.get("classification_sources"))
        ],
        "updated_outcomes": outcomes,
        "classified_events": classifications,
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_forward_progress_manifest",
        "progress_id": progress_id,
        "binding_id": binding.get("binding_id"),
        "candidate_method": candidate,
        "binding_status": bound.get("binding_status"),
        "target_count": len(targets),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_forward_progress_input_snapshot_path": str(
            root / "smoothed_forward_progress_input_snapshot.json"
        ),
        "smoothed_forward_progress_manifest_path": str(
            root / "smoothed_forward_progress_manifest.json"
        ),
        "smoothed_target_progress_path": str(root / "smoothed_target_progress.jsonl"),
        "progress_evidence_commitments_path": str(root / "progress_evidence_commitments.json"),
        "smoothed_forward_progress_summary_path": str(
            root / "smoothed_forward_progress_summary.json"
        ),
        "smoothed_forward_progress_report_path": str(root / "smoothed_forward_progress_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_progress_report(manifest, summary, targets)
    reader = _render_progress_reader(summary)
    views = {
        "smoothed_forward_progress_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_forward_progress_manifest.json": _json_bytes(manifest),
        "smoothed_target_progress.jsonl": _jsonl_bytes(targets),
        "progress_evidence_commitments.json": _json_bytes(commitments),
        "smoothed_forward_progress_summary.json": _json_bytes(summary),
        "smoothed_forward_progress_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_target_progress": targets,
        "progress_evidence_commitments": commitments,
        "smoothed_forward_progress_summary": summary,
        "reader_brief_section": reader,
    }


def _validate_progress_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_FORWARD_PROGRESS_SNAPSHOT_SCHEMA,
            "forward progress snapshot schema invalid",
        )
        binding_source = _mapping(snapshot.get("binding_source"))
        errors.extend(
            promotion._validate_binding(
                binding_source,
                kind="smoothed_forward_binding",
                validator=promotion.validate_smoothed_forward_binding_artifact,
                validator_key="binding_id",
            )
        )
        bound = promotion._binding_payload(binding_source)
        manifest = _source_manifest(binding_source, "smoothed_forward_binding_manifest.json")
        candidate = _candidate(manifest.get("candidate_method"))
        target_ids = {
            _text(row.get("target_id"))
            for row in _records(_mapping(bound.get("bound_confirmation_targets")).get("targets"))
        }
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="forward progress generated_at"
        )
        manifests: list[Mapping[str, Any]] = [manifest]
        source_specs = (
            (
                "outcome_sources",
                "smoothed_outcome_update",
                legacy.validate_smoothed_outcome_update_artifact,
                "update_id",
                "smoothed_outcome_update_manifest.json",
                "updated_smoothed_outcomes.jsonl",
            ),
            (
                "classification_sources",
                "smoothed_forward_classification",
                legacy.validate_smoothed_forward_classification_artifact,
                "classification_id",
                "smoothed_forward_classification_manifest.json",
                "classified_forward_events.jsonl",
            ),
        )
        for field, kind, validator, key, manifest_name, rows_name in source_specs:
            sources = _records(snapshot.get(field))
            if candidate is None:
                _require(not sources, "candidate-less progress cannot consume event sources")
            for source in sources:
                errors.extend(
                    _validate_local_binding(
                        source,
                        kind=kind,
                        validator=validator,
                        validator_key=key,
                    )
                )
                source_manifest = _source_manifest(source, manifest_name)
                manifests.append(source_manifest)
                _require(
                    source_manifest.get("binding_id") == manifest.get("binding_id")
                    and source_manifest.get("candidate_method") == candidate,
                    "progress event source lineage mismatch",
                )
                for row in _source_jsonl(source, rows_name):
                    _require(row.get("target_id") in target_ids, "progress event target mismatch")
        _chronology(generated, *manifests)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def update_smoothed_forward_progress(
    *,
    binding_id: str,
    binding_dir: Path = DEFAULT_SMOOTHED_FORWARD_BINDING_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    outcome_update_dir: Path = DEFAULT_SMOOTHED_OUTCOME_UPDATE_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    outcome_update_ids: Sequence[str] = (),
    classification_ids: Sequence[str] = (),
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_FORWARD_PROGRESS_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "binding_source": promotion._binding_binding(binding_id, binding_dir),
        "outcome_sources": [
            _outcome_binding(artifact_id, outcome_update_dir) for artifact_id in outcome_update_ids
        ],
        "classification_sources": [
            _classification_binding(artifact_id, classification_dir)
            for artifact_id in classification_ids
        ],
        "production_effect": "none",
    }
    errors = _validate_progress_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-forward-progress", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _progress_views(snapshot, progress_id=root.name, root=root)
    _write(
        root, views, "latest_smoothed_forward_progress", "smoothed_forward_progress_manifest.json"
    )
    return {"progress_id": root.name, "progress_dir": root, **payload}


def smoothed_forward_progress_report_payload(
    *,
    progress_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, progress_id, latest, "latest_smoothed_forward_progress")
    return {
        **_read_json(root / "smoothed_forward_progress_manifest.json"),
        "smoothed_target_progress": _read_jsonl(root / "smoothed_target_progress.jsonl"),
        "progress_evidence_commitments": _read_json(root / "progress_evidence_commitments.json"),
        "smoothed_forward_progress_summary": _read_json(
            root / "smoothed_forward_progress_summary.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_forward_progress_input_snapshot.json"),
        "progress_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_forward_progress_artifact(
    *, progress_id: str, output_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR
) -> dict[str, Any]:
    root = output_dir / progress_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_forward_progress_input_snapshot.json") or {}
    )
    errors = _validate_progress_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _progress_views(snapshot, progress_id=progress_id, root=root)
        mismatches = _view_errors(root, views)
        summary = _mapping(payload.get("smoothed_forward_progress_summary"))
        if summary.get("candidate_method") is None:
            _require(
                summary.get("targets_total") == 0
                and summary.get("progress_status") == "NOT_REGISTERED",
                "candidate-less progress must remain unregistered",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_forward_progress_validation",
        progress_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="progress_id",
    )


def _dashboard_summary(
    progress: Mapping[str, Any], targets: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    source = _mapping(progress.get("smoothed_forward_progress_summary"))
    candidate = _candidate(source.get("candidate_method"))
    non_watch = [row for row in targets if row.get("watch_only") is not True]
    ready = bool(candidate and non_watch) and all(
        row.get("progress_status") == "READY_FOR_REVIEW" for row in non_watch
    )
    return {
        "schema_version": 2,
        "dashboard_id": "",
        "progress_id": source.get("progress_id"),
        "candidate_method": candidate,
        "secondary_method": None,
        "current_owner_decision": None,
        "gate_decision": None,
        "decision_confidence": None,
        "forward_confirmation_status": (
            "NOT_REGISTERED"
            if candidate is None
            else ("READY_FOR_REVIEW" if ready else "IN_PROGRESS")
        ),
        "ready_for_switch_recheck": ready,
        "weekly_recommendation": (
            "request_more_forward_data" if candidate is None else "continue_observation"
        ),
        **{
            key: source.get(key, 0)
            for key in (
                "required_forward_events_total",
                "available_forward_events_total",
                "required_sideways_events",
                "available_sideways_events",
                "required_recovery_events",
                "available_recovery_events",
            )
        },
        "target_status_count": len(targets),
        **SYSTEM_TARGET_SAFETY,
    }


def _status_table(targets: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for row in targets:
        required = _target_required(row)
        available = int(row.get("available_events", 0))
        watch_only = row.get("watch_only") is True
        status = "WATCH_ONLY" if watch_only else _text(row.get("progress_status"))
        rows.append(
            {
                "target_id": row.get("target_id"),
                "status": status,
                "available_events": available,
                "required_events": required,
                "progress_pct": round(available / required * 100.0, 4) if required else 0.0,
                "decision": (
                    "watch_for_recovery_lag"
                    if watch_only
                    else (
                        "ready_for_review" if status == "READY_FOR_REVIEW" else "continue_tracking"
                    )
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": 2, "targets": rows, **SYSTEM_TARGET_SAFETY}


def _dashboard_views(
    snapshot: Mapping[str, Any], *, dashboard_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    progress = _progress_payload(_mapping(snapshot.get("progress_source")))
    targets = _records(progress.get("smoothed_target_progress"))
    summary = _dashboard_summary(progress, targets)
    summary["dashboard_id"] = dashboard_id
    table = _status_table(targets)
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_weekly_dashboard_manifest",
        "dashboard_id": dashboard_id,
        "progress_id": progress.get("progress_id"),
        "candidate_method": summary.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_weekly_dashboard_input_snapshot_path": str(
            root / "smoothed_weekly_dashboard_input_snapshot.json"
        ),
        "smoothed_weekly_dashboard_manifest_path": str(
            root / "smoothed_weekly_dashboard_manifest.json"
        ),
        "smoothed_dashboard_summary_path": str(root / "smoothed_dashboard_summary.json"),
        "smoothed_target_status_table_path": str(root / "smoothed_target_status_table.json"),
        "smoothed_weekly_dashboard_report_path": str(root / "smoothed_weekly_dashboard_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_dashboard_report(manifest, summary, table)
    reader = _render_dashboard_reader(summary)
    views = {
        "smoothed_weekly_dashboard_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_weekly_dashboard_manifest.json": _json_bytes(manifest),
        "smoothed_dashboard_summary.json": _json_bytes(summary),
        "smoothed_target_status_table.json": _json_bytes(table),
        "smoothed_weekly_dashboard_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "smoothed_dashboard_summary": summary,
        "smoothed_target_status_table": table,
        "reader_brief_section": reader,
    }


def _validate_dashboard_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_WEEKLY_DASHBOARD_SNAPSHOT_SCHEMA,
            "weekly dashboard snapshot schema invalid",
        )
        source = _mapping(snapshot.get("progress_source"))
        errors.extend(
            _validate_local_binding(
                source,
                kind="smoothed_forward_progress",
                validator=validate_smoothed_forward_progress_artifact,
                validator_key="progress_id",
            )
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="weekly dashboard generated_at"
        )
        _chronology(generated, _source_manifest(source, "smoothed_forward_progress_manifest.json"))
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def build_smoothed_weekly_dashboard(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_WEEKLY_DASHBOARD_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "progress_source": _progress_binding(progress_id, progress_dir),
        "production_effect": "none",
    }
    errors = _validate_dashboard_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-weekly-dashboard", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _dashboard_views(snapshot, dashboard_id=root.name, root=root)
    _write(
        root, views, "latest_smoothed_weekly_dashboard", "smoothed_weekly_dashboard_manifest.json"
    )
    return {"dashboard_id": root.name, "dashboard_dir": root, **payload}


def smoothed_weekly_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, dashboard_id, latest, "latest_smoothed_weekly_dashboard")
    return {
        **_read_json(root / "smoothed_weekly_dashboard_manifest.json"),
        "smoothed_dashboard_summary": _read_json(root / "smoothed_dashboard_summary.json"),
        "smoothed_target_status_table": _read_json(root / "smoothed_target_status_table.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_weekly_dashboard_input_snapshot.json"),
        "dashboard_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_weekly_dashboard_artifact(
    *, dashboard_id: str, output_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR
) -> dict[str, Any]:
    root = output_dir / dashboard_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_weekly_dashboard_input_snapshot.json") or {}
    )
    errors = _validate_dashboard_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _dashboard_views(snapshot, dashboard_id=dashboard_id, root=root)
        mismatches = _view_errors(root, views)
        summary = _mapping(payload.get("smoothed_dashboard_summary"))
        if summary.get("candidate_method") is None:
            _require(
                summary.get("forward_confirmation_status") == "NOT_REGISTERED"
                and summary.get("ready_for_switch_recheck") is False,
                "candidate-less dashboard readiness invalid",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_weekly_dashboard_validation",
        dashboard_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="dashboard_id",
    )


def _event_summary(
    progress: Mapping[str, Any],
    sideways: Sequence[Mapping[str, Any]],
    recovery: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    summary = _mapping(progress.get("smoothed_forward_progress_summary"))
    candidate = _candidate(summary.get("candidate_method"))
    sideways_required = int(summary.get("required_sideways_events", 0))
    recovery_required = int(summary.get("required_recovery_events", 0))
    sideways_available = len(
        {_text(row.get("event_id")) for row in sideways if row.get("event_status") == "AVAILABLE"}
    )
    recovery_available = len(
        {_text(row.get("event_id")) for row in recovery if row.get("event_status") == "AVAILABLE"}
    )
    lag_warnings = sum(row.get("lag_warning") is True for row in recovery)
    not_registered = candidate is None
    return {
        "schema_version": 2,
        "monitor_id": "",
        "progress_id": summary.get("progress_id"),
        "candidate_method": candidate,
        "sideways_events": {
            "required": sideways_required,
            "available": sideways_available,
            "pending": sum(row.get("event_status") == "PENDING" for row in sideways),
            "progress_pct": round(sideways_available / sideways_required * 100.0, 4)
            if sideways_required
            else 0.0,
        },
        "recovery_events": {
            "required": recovery_required,
            "available": recovery_available,
            "pending": sum(row.get("event_status") == "PENDING" for row in recovery),
            "progress_pct": round(recovery_available / recovery_required * 100.0, 4)
            if recovery_required
            else 0.0,
        },
        "sideways_status": "NOT_REGISTERED"
        if not_registered
        else (
            "READY"
            if sideways_required and sideways_available >= sideways_required
            else "IN_PROGRESS"
        ),
        "recovery_lag_status": "NOT_REGISTERED"
        if not_registered
        else (
            "WARNING"
            if lag_warnings
            else (
                "NO_WARNING"
                if recovery_required and recovery_available >= recovery_required
                else "IN_PROGRESS"
            )
        ),
        "lag_warning_count": lag_warnings,
        "recommended_action": "request_more_forward_data"
        if not_registered
        else "continue_event_collection",
        **SYSTEM_TARGET_SAFETY,
    }


def _monitor_views(
    snapshot: Mapping[str, Any], *, monitor_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    progress = _progress_payload(_mapping(snapshot.get("progress_source")))
    commitments = _mapping(progress.get("progress_evidence_commitments"))
    classified = _records(commitments.get("classified_events"))
    sideways = [row for row in classified if row.get("sideways_relevant") is True]
    recovery = [row for row in classified if row.get("recovery_lag_relevant") is True]
    summary = _event_summary(progress, sideways, recovery)
    summary["monitor_id"] = monitor_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_event_monitor_manifest",
        "monitor_id": monitor_id,
        "progress_id": progress.get("progress_id"),
        "candidate_method": summary.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_event_monitor_input_snapshot_path": str(
            root / "smoothed_event_monitor_input_snapshot.json"
        ),
        "smoothed_event_monitor_manifest_path": str(root / "smoothed_event_monitor_manifest.json"),
        "sideways_event_inventory_path": str(root / "sideways_event_inventory.jsonl"),
        "recovery_event_inventory_path": str(root / "recovery_event_inventory.jsonl"),
        "event_accumulation_summary_path": str(root / "event_accumulation_summary.json"),
        "smoothed_event_monitor_report_path": str(root / "smoothed_event_monitor_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_monitor_report(manifest, summary, sideways, recovery)
    views = {
        "smoothed_event_monitor_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_event_monitor_manifest.json": _json_bytes(manifest),
        "sideways_event_inventory.jsonl": _jsonl_bytes(sideways),
        "recovery_event_inventory.jsonl": _jsonl_bytes(recovery),
        "event_accumulation_summary.json": _json_bytes(summary),
        "smoothed_event_monitor_report.md": report.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "sideways_event_inventory": sideways,
        "recovery_event_inventory": recovery,
        "event_accumulation_summary": summary,
    }


def _validate_monitor_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_EVENT_MONITOR_SNAPSHOT_SCHEMA,
            "event monitor snapshot schema invalid",
        )
        source = _mapping(snapshot.get("progress_source"))
        errors.extend(
            _validate_local_binding(
                source,
                kind="smoothed_forward_progress",
                validator=validate_smoothed_forward_progress_artifact,
                validator_key="progress_id",
            )
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="event monitor generated_at"
        )
        _chronology(generated, _source_manifest(source, "smoothed_forward_progress_manifest.json"))
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def update_smoothed_event_monitor(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_SMOOTHED_FORWARD_PROGRESS_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    classification_dir: Path = DEFAULT_SMOOTHED_FORWARD_CLASSIFICATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    del classification_dir  # Classification commitments are frozen by Progress, not rescanned.
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_EVENT_MONITOR_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "progress_source": _progress_binding(progress_id, progress_dir),
        "production_effect": "none",
    }
    errors = _validate_monitor_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-event-monitor", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _monitor_views(snapshot, monitor_id=root.name, root=root)
    _write(root, views, "latest_smoothed_event_monitor", "smoothed_event_monitor_manifest.json")
    return {"monitor_id": root.name, "monitor_dir": root, **payload}


def smoothed_event_monitor_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, monitor_id, latest, "latest_smoothed_event_monitor")
    return {
        **_read_json(root / "smoothed_event_monitor_manifest.json"),
        "sideways_event_inventory": _read_jsonl(root / "sideways_event_inventory.jsonl"),
        "recovery_event_inventory": _read_jsonl(root / "recovery_event_inventory.jsonl"),
        "event_accumulation_summary": _read_json(root / "event_accumulation_summary.json"),
        "input_snapshot": _read_json(root / "smoothed_event_monitor_input_snapshot.json"),
        "monitor_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_event_monitor_artifact(
    *, monitor_id: str, output_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR
) -> dict[str, Any]:
    root = output_dir / monitor_id
    snapshot = legacy._read_optional_json(root / "smoothed_event_monitor_input_snapshot.json") or {}
    errors = _validate_monitor_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _monitor_views(snapshot, monitor_id=monitor_id, root=root)
        mismatches = _view_errors(root, views)
        summary = _mapping(payload.get("event_accumulation_summary"))
        if summary.get("candidate_method") is None:
            _require(
                summary.get("sideways_status") == "NOT_REGISTERED"
                and summary.get("recovery_lag_status") == "NOT_REGISTERED",
                "candidate-less event monitor status invalid",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_event_monitor_validation",
        monitor_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="monitor_id",
    )


def _criteria(
    dashboard: Mapping[str, Any], monitor: Mapping[str, Any], candidate: str | None
) -> dict[str, Any]:
    table = _mapping(dashboard.get("smoothed_target_status_table"))
    event_summary = _mapping(monitor.get("event_accumulation_summary"))
    rows: list[dict[str, Any]] = []
    if candidate is not None:
        for target in _records(table.get("targets")):
            status = _text(target.get("status"))
            criterion_status = (
                "PASS" if status in {"READY_FOR_REVIEW", "WATCH_ONLY"} else "IN_PROGRESS"
            )
            if status == "WATCH_ONLY" and event_summary.get("recovery_lag_status") == "WARNING":
                criterion_status = "FAIL"
            rows.append(
                {
                    "criterion": target.get("target_id"),
                    "required": target.get("required_events"),
                    "available": target.get("available_events"),
                    "status": criterion_status,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    hard_blockers = (
        ["no_eligible_candidate"]
        if candidate is None
        else [_text(row.get("criterion")) for row in rows if row.get("status") == "FAIL"]
    )
    warnings = [_text(row.get("criterion")) for row in rows if row.get("status") == "IN_PROGRESS"]
    return {
        "schema_version": 2,
        "recheck_id": "",
        "candidate_method": candidate,
        "criteria": rows,
        "hard_blockers": hard_blockers,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _recheck_decision(
    dashboard: Mapping[str, Any], switch: Mapping[str, Any], criteria: Mapping[str, Any]
) -> dict[str, Any]:
    summary = _mapping(dashboard.get("smoothed_dashboard_summary"))
    plan = _mapping(switch.get("primary_switch_plan"))
    candidate = _candidate(summary.get("candidate_method"))
    statuses = {_text(row.get("status")) for row in _records(criteria.get("criteria"))}
    if candidate is None:
        decision = "NO_ELIGIBLE_CANDIDATE"
    elif "FAIL" in statuses:
        decision = "REJECT"
    elif statuses and statuses == {"PASS"}:
        decision = "READY_FOR_OWNER_REVIEW"
    else:
        decision = "WAIT_FOR_MORE_FORWARD_DATA"
    forward_progress = (
        f"{summary.get('available_forward_events_total', 0)}/"
        f"{summary.get('required_forward_events_total', 0)}"
    )
    sideways_progress = (
        f"{summary.get('available_sideways_events', 0)}/"
        f"{summary.get('required_sideways_events', 0)}"
    )
    recovery_progress = (
        f"{summary.get('available_recovery_events', 0)}/"
        f"{summary.get('required_recovery_events', 0)}"
    )
    return {
        "schema_version": 2,
        "recheck_id": "",
        "candidate_method": candidate,
        "current_owner_decision": None,
        "previous_gate_decision": None,
        "switch_plan_id": switch.get("switch_plan_id"),
        "switch_decision": plan.get("switch_decision"),
        "recheck_decision": decision,
        "decision_confidence": summary.get("decision_confidence"),
        "can_execute_switch": False,
        "owner_decision_required": candidate is not None,
        "auto_switch": False,
        "forward_progress": forward_progress,
        "sideways_progress": sideways_progress,
        "recovery_progress": recovery_progress,
        **SYSTEM_TARGET_SAFETY,
    }


def _recheck_views(
    snapshot: Mapping[str, Any], *, recheck_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    dashboard = _dashboard_payload(_mapping(snapshot.get("dashboard_source")))
    monitor = _monitor_payload(_mapping(snapshot.get("monitor_source")))
    switch = promotion._switch_payload(_mapping(snapshot.get("switch_source")))
    candidate = _candidate(
        _mapping(dashboard.get("smoothed_dashboard_summary")).get("candidate_method")
    )
    criteria = _criteria(dashboard, monitor, candidate)
    criteria["recheck_id"] = recheck_id
    decision = _recheck_decision(dashboard, switch, criteria)
    decision["recheck_id"] = recheck_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_switch_readiness_manifest",
        "recheck_id": recheck_id,
        "dashboard_id": dashboard.get("dashboard_id"),
        "monitor_id": monitor.get("monitor_id"),
        "switch_plan_id": switch.get("switch_plan_id"),
        "progress_id": dashboard.get("progress_id"),
        "candidate_method": candidate,
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_switch_readiness_input_snapshot_path": str(
            root / "smoothed_switch_readiness_input_snapshot.json"
        ),
        "smoothed_switch_readiness_manifest_path": str(
            root / "smoothed_switch_readiness_manifest.json"
        ),
        "switch_readiness_decision_path": str(root / "switch_readiness_decision.json"),
        "switch_readiness_criteria_path": str(root / "switch_readiness_criteria.json"),
        "smoothed_switch_readiness_report_path": str(root / "smoothed_switch_readiness_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    report = _render_recheck_report(manifest, decision, criteria)
    reader = _render_recheck_reader(decision)
    views = {
        "smoothed_switch_readiness_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_switch_readiness_manifest.json": _json_bytes(manifest),
        "switch_readiness_decision.json": _json_bytes(decision),
        "switch_readiness_criteria.json": _json_bytes(criteria),
        "smoothed_switch_readiness_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "switch_readiness_decision": decision,
        "switch_readiness_criteria": criteria,
        "reader_brief_section": reader,
    }


def _validate_recheck_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_SWITCH_READINESS_SNAPSHOT_SCHEMA,
            "switch readiness snapshot schema invalid",
        )
        specs = (
            (
                "dashboard_source",
                "smoothed_weekly_dashboard",
                validate_smoothed_weekly_dashboard_artifact,
                "dashboard_id",
                "smoothed_weekly_dashboard_manifest.json",
            ),
            (
                "monitor_source",
                "smoothed_event_monitor",
                validate_smoothed_event_monitor_artifact,
                "monitor_id",
                "smoothed_event_monitor_manifest.json",
            ),
            (
                "switch_source",
                "paper_shadow_primary_switch",
                promotion.validate_paper_shadow_primary_switch_artifact,
                "switch_plan_id",
                "paper_shadow_primary_switch_manifest.json",
            ),
        )
        manifests: dict[str, dict[str, Any]] = {}
        for field, kind, validator, key, manifest_name in specs:
            source = _mapping(snapshot.get(field))
            if field == "switch_source":
                errors.extend(
                    promotion._validate_binding(
                        source, kind=kind, validator=validator, validator_key=key
                    )
                )
            else:
                errors.extend(
                    _validate_local_binding(
                        source, kind=kind, validator=validator, validator_key=key
                    )
                )
            manifests[field] = _source_manifest(source, manifest_name)
        dashboard = manifests["dashboard_source"]
        monitor = manifests["monitor_source"]
        switch = manifests["switch_source"]
        _require(
            dashboard.get("progress_id") == monitor.get("progress_id"),
            "recheck progress lineage mismatch",
        )
        _require(
            len(
                {
                    dashboard.get("candidate_method"),
                    monitor.get("candidate_method"),
                    switch.get("candidate_method"),
                }
            )
            == 1,
            "recheck candidate lineage mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="switch readiness generated_at"
        )
        _chronology(generated, dashboard, monitor, switch)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def recheck_smoothed_switch_readiness(
    *,
    dashboard_id: str,
    monitor_id: str,
    switch_plan_id: str,
    dashboard_dir: Path = DEFAULT_SMOOTHED_WEEKLY_DASHBOARD_DIR,
    monitor_dir: Path = DEFAULT_SMOOTHED_EVENT_MONITOR_DIR,
    switch_plan_dir: Path = DEFAULT_PAPER_SHADOW_PRIMARY_SWITCH_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_SWITCH_READINESS_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "dashboard_source": _dashboard_binding(dashboard_id, dashboard_dir),
        "monitor_source": _monitor_binding(monitor_id, monitor_dir),
        "switch_source": promotion._switch_binding(switch_plan_id, switch_plan_dir),
        "production_effect": "none",
    }
    errors = _validate_recheck_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-switch-readiness", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _recheck_views(snapshot, recheck_id=root.name, root=root)
    _write(
        root, views, "latest_smoothed_switch_readiness", "smoothed_switch_readiness_manifest.json"
    )
    return {"recheck_id": root.name, "recheck_dir": root, **payload}


def smoothed_switch_readiness_report_payload(
    *,
    recheck_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, recheck_id, latest, "latest_smoothed_switch_readiness")
    return {
        **_read_json(root / "smoothed_switch_readiness_manifest.json"),
        "switch_readiness_decision": _read_json(root / "switch_readiness_decision.json"),
        "switch_readiness_criteria": _read_json(root / "switch_readiness_criteria.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_switch_readiness_input_snapshot.json"),
        "recheck_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_switch_readiness_artifact(
    *, recheck_id: str, output_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR
) -> dict[str, Any]:
    root = output_dir / recheck_id
    snapshot = (
        legacy._read_optional_json(root / "smoothed_switch_readiness_input_snapshot.json") or {}
    )
    errors = _validate_recheck_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _recheck_views(snapshot, recheck_id=recheck_id, root=root)
        mismatches = _view_errors(root, views)
        decision = _mapping(payload.get("switch_readiness_decision"))
        _require(decision.get("can_execute_switch") is False, "recheck cannot execute switch")
        if decision.get("candidate_method") is None:
            _require(
                decision.get("recheck_decision") == "NO_ELIGIBLE_CANDIDATE"
                and decision.get("owner_decision_required") is False,
                "candidate-less recheck decision invalid",
            )
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_switch_readiness_validation",
        recheck_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="recheck_id",
    )


def _owner_options(recheck: Mapping[str, Any], owner: Mapping[str, Any]) -> dict[str, Any]:
    decision = _mapping(recheck.get("switch_readiness_decision"))
    previous = _mapping(owner.get("owner_promotion_decision"))
    candidate = _candidate(decision.get("candidate_method"))
    _require(previous.get("candidate_method") == candidate, "renewal owner candidate mismatch")
    recheck_decision = _text(decision.get("recheck_decision"))
    recommended = (
        _text(previous.get("recommended_owner_action"), "request_more_forward_data")
        if candidate is None
        else (
            "continue_observation"
            if recheck_decision != "READY_FOR_OWNER_REVIEW"
            else "review_for_manual_promotion_decision"
        )
    )
    choices = (
        "continue_observation",
        "request_more_forward_data",
        "promote_to_primary_research_candidate",
        "defer",
        "reject",
    )
    rows = []
    for choice in choices:
        available = not (
            choice == "promote_to_primary_research_candidate"
            and recheck_decision != "READY_FOR_OWNER_REVIEW"
        )
        rows.append(
            {
                "decision": choice,
                "available": available,
                "recommended": choice == recommended,
                "reason": (
                    "Promotion requires READY_FOR_OWNER_REVIEW."
                    if choice == "promote_to_primary_research_candidate" and not available
                    else "Manual owner option; this artifact never executes a switch."
                ),
            }
        )
    return {
        "schema_version": 2,
        "renewal_id": "",
        "candidate_method": candidate,
        "previous_owner_decision": previous.get("owner_decision"),
        "current_recheck_decision": recheck_decision,
        "recommended_owner_action": recommended,
        "forward_progress": decision.get("forward_progress"),
        "sideways_progress": decision.get("sideways_progress"),
        "recovery_lag_status": (
            "NOT_REGISTERED" if candidate is None else "REVIEW_FROM_EVENT_MONITOR"
        ),
        "owner_options": rows,
        "auto_switch": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _renewal_views(
    snapshot: Mapping[str, Any], *, renewal_id: str, root: Path
) -> tuple[dict[str, bytes], dict[str, Any]]:
    recheck = _recheck_payload(_mapping(snapshot.get("recheck_source")))
    owner = _owner_promotion_payload(_mapping(snapshot.get("owner_promotion_source")))
    options = _owner_options(recheck, owner)
    options["renewal_id"] = renewal_id
    manifest = {
        "schema_version": 2,
        "report_type": "etf_dynamic_v3_smoothed_owner_renewal_manifest",
        "renewal_id": renewal_id,
        "recheck_id": recheck.get("recheck_id"),
        "owner_promotion_id": owner.get("decision_id"),
        "candidate_method": options.get("candidate_method"),
        "generated_at": snapshot.get("generated_at"),
        "status": "PASS",
        "smoothed_owner_renewal_input_snapshot_path": str(
            root / "smoothed_owner_renewal_input_snapshot.json"
        ),
        "smoothed_owner_renewal_manifest_path": str(root / "smoothed_owner_renewal_manifest.json"),
        "owner_renewal_options_path": str(root / "owner_renewal_options.json"),
        "owner_renewal_checklist_path": str(root / "owner_renewal_checklist.md"),
        "smoothed_owner_renewal_report_path": str(root / "smoothed_owner_renewal_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = _render_renewal_checklist(options)
    report = _render_renewal_report(manifest, options)
    reader = _render_renewal_reader(options)
    views = {
        "smoothed_owner_renewal_input_snapshot.json": _json_bytes(dict(snapshot)),
        "smoothed_owner_renewal_manifest.json": _json_bytes(manifest),
        "owner_renewal_options.json": _json_bytes(options),
        "owner_renewal_checklist.md": checklist.encode("utf-8"),
        "smoothed_owner_renewal_report.md": report.encode("utf-8"),
        "reader_brief_section.md": reader.encode("utf-8"),
    }
    return views, {
        "manifest": manifest,
        "owner_renewal_options": options,
        "owner_renewal_checklist": checklist,
        "reader_brief_section": reader,
    }


def _validate_renewal_snapshot(snapshot: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    try:
        _require(
            snapshot.get("schema_version") == SMOOTHED_OWNER_RENEWAL_SNAPSHOT_SCHEMA,
            "owner renewal snapshot schema invalid",
        )
        recheck_source = _mapping(snapshot.get("recheck_source"))
        owner_source = _mapping(snapshot.get("owner_promotion_source"))
        errors.extend(
            _validate_local_binding(
                recheck_source,
                kind="smoothed_switch_readiness",
                validator=validate_smoothed_switch_readiness_artifact,
                validator_key="recheck_id",
            )
        )
        errors.extend(
            _validate_local_binding(
                owner_source,
                kind="smoothed_owner_promotion",
                validator=promotion.validate_smoothed_owner_promotion_artifact,
                validator_key="decision_id",
            )
        )
        recheck = _source_manifest(recheck_source, "smoothed_switch_readiness_manifest.json")
        owner = _source_manifest(owner_source, "smoothed_owner_promotion_manifest.json")
        _require(
            recheck.get("candidate_method") == owner.get("candidate_method"),
            "owner renewal candidate lineage mismatch",
        )
        generated = target_core._datetime(
            snapshot.get("generated_at"), field="owner renewal generated_at"
        )
        _chronology(generated, recheck, owner)
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return errors


@promotion._with_validation_session
def build_smoothed_owner_renewal_pack(
    *,
    recheck_id: str,
    owner_promotion_id: str,
    recheck_dir: Path = DEFAULT_SMOOTHED_SWITCH_READINESS_DIR,
    owner_promotion_dir: Path = DEFAULT_SMOOTHED_OWNER_PROMOTION_DIR,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_at(generated_at)
    snapshot = {
        "schema_version": SMOOTHED_OWNER_RENEWAL_SNAPSHOT_SCHEMA,
        "generated_at": generated.isoformat(),
        "recheck_source": _recheck_binding(recheck_id, recheck_dir),
        "owner_promotion_source": _owner_promotion_binding(owner_promotion_id, owner_promotion_dir),
        "production_effect": "none",
    }
    errors = _validate_renewal_snapshot(snapshot)
    _require(not errors, "; ".join(errors))
    artifact_id = _stable_id("smoothed-owner-renewal", snapshot)
    root = _unique_dir(output_dir / artifact_id)
    views, payload = _renewal_views(snapshot, renewal_id=root.name, root=root)
    _write(root, views, "latest_smoothed_owner_renewal", "smoothed_owner_renewal_manifest.json")
    return {"renewal_id": root.name, "renewal_dir": root, **payload}


def smoothed_owner_renewal_report_payload(
    *,
    renewal_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR,
) -> dict[str, Any]:
    root = _artifact_root(output_dir, renewal_id, latest, "latest_smoothed_owner_renewal")
    return {
        **_read_json(root / "smoothed_owner_renewal_manifest.json"),
        "owner_renewal_options": _read_json(root / "owner_renewal_options.json"),
        "owner_renewal_checklist": (root / "owner_renewal_checklist.md").read_text(
            encoding="utf-8"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(encoding="utf-8"),
        "input_snapshot": _read_json(root / "smoothed_owner_renewal_input_snapshot.json"),
        "renewal_dir": str(root),
    }


@promotion._with_validation_session
def validate_smoothed_owner_renewal_artifact(
    *, renewal_id: str, output_dir: Path = DEFAULT_SMOOTHED_OWNER_RENEWAL_DIR
) -> dict[str, Any]:
    root = output_dir / renewal_id
    snapshot = legacy._read_optional_json(root / "smoothed_owner_renewal_input_snapshot.json") or {}
    errors = _validate_renewal_snapshot(snapshot)
    mismatches: list[str] = []
    try:
        views, payload = _renewal_views(snapshot, renewal_id=renewal_id, root=root)
        mismatches = _view_errors(root, views)
        options = _mapping(payload.get("owner_renewal_options"))
        promote = [
            row
            for row in _records(options.get("owner_options"))
            if row.get("decision") == "promote_to_primary_research_candidate"
        ]
        _require(len(promote) == 1, "renewal promote option missing")
        if options.get("candidate_method") is None:
            _require(promote[0].get("available") is False, "candidate-less promote option enabled")
    except Exception as exc:  # noqa: BLE001
        errors.append(str(exc))
    return _validation_payload(
        "etf_dynamic_v3_smoothed_owner_renewal_validation",
        renewal_id,
        [
            _check("snapshot_and_live_inputs", not errors, "; ".join(errors)),
            _check("content_derived_views", not mismatches, ",".join(mismatches)),
        ],
        artifact_id_key="renewal_id",
    )


def _render_progress_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Forward Progress",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- binding_status: {summary.get('binding_status')}",
            f"- targets_total: {summary.get('targets_total')}",
            f"- summary_recommendation: {summary.get('summary_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_progress_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Forward Progress {manifest.get('progress_id')}",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- binding_status: {summary.get('binding_status')}",
            f"- targets_total: {len(targets)}",
            f"- recommendation: {summary.get('summary_recommendation')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Candidate 与 targets 只来自 validated Binding；null/0-target 不补造。",
            "",
        ]
    )


def _render_dashboard_reader(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Weekly Dashboard",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- ready_for_switch_recheck: {str(summary.get('ready_for_switch_recheck')).lower()}",
            f"- weekly_recommendation: {summary.get('weekly_recommendation')}",
            "- production_effect: none",
            "",
        ]
    )


def _render_dashboard_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any], table: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            f"# Smoothed Weekly Dashboard {manifest.get('dashboard_id')}",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- forward_confirmation_status: {summary.get('forward_confirmation_status')}",
            f"- target_status_count: {len(_records(table.get('targets')))}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_monitor_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    sideways: Sequence[Mapping[str, Any]],
    recovery: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Event Monitor {manifest.get('monitor_id')}",
            "",
            f"- candidate_method: {summary.get('candidate_method')}",
            f"- sideways_status: {summary.get('sideways_status')}",
            f"- recovery_lag_status: {summary.get('recovery_lag_status')}",
            f"- sideways_inventory_count: {len(sideways)}",
            f"- recovery_inventory_count: {len(recovery)}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_recheck_reader(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Switch Readiness",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- recheck_decision: {decision.get('recheck_decision')}",
            f"- owner_decision_required: {str(decision.get('owner_decision_required')).lower()}",
            "- can_execute_switch: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_recheck_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    criteria: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Smoothed Switch Readiness {manifest.get('recheck_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- recheck_decision: {decision.get('recheck_decision')}",
            f"- criteria_count: {len(_records(criteria.get('criteria')))}",
            "- can_execute_switch: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_renewal_checklist(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Smoothed Owner Renewal Checklist",
            "",
            "- [ ] candidate 是否来自 validated Binding/Recheck/Owner lineage？",
            "- [ ] null candidate 时 promote option 是否不可用？",
            "- [ ] 是否确认 no official target weights / no broker / no production？",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            "- no broker / no production",
            "",
        ]
    )


def _render_renewal_reader(options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Smoothed Owner Renewal",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- previous_owner_decision: {options.get('previous_owner_decision')}",
            f"- current_recheck_decision: {options.get('current_recheck_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            "- auto_switch: false",
            "- production_effect: none",
            "",
        ]
    )


def _render_renewal_report(manifest: Mapping[str, Any], options: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Smoothed Owner Renewal {manifest.get('renewal_id')}",
            "",
            f"- candidate_method: {options.get('candidate_method')}",
            f"- previous_owner_decision: {options.get('previous_owner_decision')}",
            f"- current_recheck_decision: {options.get('current_recheck_decision')}",
            f"- recommended_owner_action: {options.get('recommended_owner_action')}",
            "- auto_switch: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )
