from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_diagnostics as diagnostics
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic

DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "flip_rotation_event_casebook_v1.yaml"
)
DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "flip_rotation_event_casebook"
)
REQUIRED_FLIP_ROTATION_EVENT_FIELDS = (
    "date",
    "previous_state",
    "new_state",
    "trigger_signal",
    "flip_useful",
    "false_positive",
    "turnover_impact",
    "candidate_behavior",
)
FLIP_ROTATION_EVENT_CASEBOOK_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "flip_rotation_event_casebook_only": True,
    "research_diagnostic_only": True,
    "not_trading_signal": True,
    "data_downloaded_by_casebook": False,
    "pipelines_executed_by_casebook": False,
    "evidence_role": "MANUAL_DIAGNOSTIC",
    "quantitative_evidence_eligible": False,
    "promotion_evidence_eligible": False,
    "automatic_candidate_promotion": False,
    "production_effect": "none",
}
FLIP_INPUT_SCHEMA = "flip_rotation_event_casebook_input_snapshot.v2"
FLIP_VIEWS = (
    "flip_rotation_casebook_manifest.json",
    "flip_rotation_event_casebook.json",
    "flip_rotation_event_casebook_report.md",
    "flip_rotation_event_casebook_reader_brief.md",
)
FLIP_SNAPSHOT = "flip_rotation_event_casebook_input_snapshot.json"


def build_flip_rotation_event_casebook(
    *,
    config_path: Path = DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_CONFIG_PATH,
    output_dir: Path = DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    policy_source = foundation._file_binding(config_path)
    config = st._load_yaml_mapping(config_path)
    normalized = _normalized_casebook(config, config_path=config_path)
    _validate_manual_casebook_source(normalized, generated=generated)
    casebook_run_id = st._stable_id(
        "flip-rotation-event-casebook",
        normalized.get("casebook_id"),
        normalized.get("version"),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / casebook_run_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest, casebook, views = _flip_material(
        root=root,
        casebook_run_id=root.name,
        normalized=normalized,
        generated=generated,
    )
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)
    snapshot = {
        "schema_version": FLIP_INPUT_SCHEMA,
        "casebook_run_id": root.name,
        "generated_at": generated.isoformat(),
        "policy_source": policy_source,
        "policy_lineage": {
            "casebook_id": normalized.get("casebook_id"),
            "version": normalized.get("version"),
            "status": normalized.get("status"),
            "owner": normalized.get("owner"),
        },
        "chronology": {
            "generated_at": generated.isoformat(),
            "latest_manual_event_date": max(
                (_text(row.get("date")) for row in _records(normalized.get("events"))),
                default=None,
            ),
        },
        "evidence_role": "MANUAL_DIAGNOSTIC",
        "view_hashes": foundation._view_hashes(root, FLIP_VIEWS),
    }
    foundation._write_snapshot(root / FLIP_SNAPSHOT, snapshot)
    st._write_latest_pointer(
        "latest_flip_rotation_event_casebook",
        root.name,
        root / "flip_rotation_casebook_manifest.json",
    )
    validation = validate_flip_rotation_event_casebook_artifact(
        casebook_run_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "casebook_run_id": root.name,
        "casebook_dir": root,
        "manifest": manifest,
        "flip_rotation_event_casebook": casebook,
        "flip_rotation_event_casebook_reader_brief": (
            root / "flip_rotation_event_casebook_reader_brief.md"
        ).read_text(encoding="utf-8"),
        "input_snapshot": snapshot,
        "flip_rotation_event_casebook_validation": validation,
    }


def _flip_material(
    *,
    root: Path,
    casebook_run_id: str,
    normalized: Mapping[str, Any],
    generated: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, bytes]]:
    events = _records(normalized.get("events"))
    config_path = Path(_text(normalized.get("config_path")))
    casebook = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_flip_rotation_event_casebook",
        "casebook_run_id": casebook_run_id,
        "flip_rotation_casebook_id": normalized.get("casebook_id"),
        "version": normalized.get("version"),
        "status": "MANUAL_DIAGNOSTIC",
        "observed_evidence_status": "NOT_APPLICABLE",
        "owner": normalized.get("owner"),
        "source_type": normalized.get("source_type"),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "event_count": len(events),
        "useful_flip_count": _count_bool(events, "flip_useful", True),
        "false_positive_count": _count_bool(events, "false_positive", True),
        "dominant_trigger_signal": _dominant_value(events, "trigger_signal"),
        "turnover_impact_summary": _value_summary(events, "turnover_impact"),
        "event_type_summary": _value_summary(events, "event_type"),
        "next_review_action": "manual_qualitative_review_only",
        "casebook_policy": normalized.get("casebook_policy"),
        "events": events,
        **FLIP_ROTATION_EVENT_CASEBOOK_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_flip_rotation_casebook_manifest",
        "casebook_run_id": casebook_run_id,
        "flip_rotation_casebook_id": normalized.get("casebook_id"),
        "version": normalized.get("version"),
        "status": "PASS",
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "flip_rotation_casebook_manifest_path": str(
            root / "flip_rotation_casebook_manifest.json"
        ),
        "flip_rotation_event_casebook_path": str(
            root / "flip_rotation_event_casebook.json"
        ),
        "flip_rotation_event_casebook_report_path": str(
            root / "flip_rotation_event_casebook_report.md"
        ),
        "flip_rotation_event_casebook_reader_brief_path": str(
            root / "flip_rotation_event_casebook_reader_brief.md"
        ),
        **FLIP_ROTATION_EVENT_CASEBOOK_SAFETY,
    }
    reader = render_flip_rotation_event_casebook_reader_brief(casebook)
    views = {
        "flip_rotation_casebook_manifest.json": foundation._json_bytes(manifest),
        "flip_rotation_event_casebook.json": foundation._json_bytes(casebook),
        "flip_rotation_event_casebook_report.md": foundation._text_file_bytes(
            render_flip_rotation_event_casebook_report(manifest, casebook)
        ),
        "flip_rotation_event_casebook_reader_brief.md": foundation._text_file_bytes(reader),
    }
    return manifest, casebook, views


def flip_rotation_event_casebook_report_payload(
    *,
    casebook_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=casebook_run_id,
        latest_pointer="latest_flip_rotation_event_casebook",
        latest=latest,
        output_dir=output_dir,
        required_name="flip_rotation_casebook_manifest.json",
    )
    payload = {
        **st._read_json(root / "flip_rotation_casebook_manifest.json"),
        "flip_rotation_event_casebook": st._read_json(
            root / "flip_rotation_event_casebook.json"
        ),
        "flip_rotation_event_casebook_reader_brief": (
            root / "flip_rotation_event_casebook_reader_brief.md"
        ).read_text(encoding="utf-8"),
        "casebook_dir": str(root),
    }
    snapshot = st._read_optional_json(root / FLIP_SNAPSHOT)
    if snapshot:
        payload["input_snapshot"] = snapshot
    validation = st._read_optional_json(root / "flip_rotation_event_casebook_validation.json")
    if validation:
        payload["flip_rotation_event_casebook_validation"] = validation
    return payload


def validate_flip_rotation_event_casebook_artifact(
    *,
    casebook_run_id: str,
    output_dir: Path = DEFAULT_FLIP_ROTATION_EVENT_CASEBOOK_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / casebook_run_id
    checks, ok = diagnostics._snapshot_preflight(
        root=root,
        snapshot_name=FLIP_SNAPSHOT,
        schema=FLIP_INPUT_SCHEMA,
        id_key="casebook_run_id",
        artifact_id=casebook_run_id,
        view_names=FLIP_VIEWS,
    )
    if ok:
        validation = diagnostics._validate_content(
            report_type="etf_dynamic_v3_flip_rotation_event_casebook_validation",
            artifact_id=casebook_run_id,
            checks=checks,
            rebuild=lambda: _rebuild_flip_casebook(root, casebook_run_id),
        )
    else:
        validation = st._validation_payload(
            "etf_dynamic_v3_flip_rotation_event_casebook_validation",
            casebook_run_id,
            checks,
        )
    if write_output:
        st._write_json(root / "flip_rotation_event_casebook_validation.json", validation)
        st._write_text(
            root / "flip_rotation_event_casebook_validation.md",
            render_flip_rotation_event_casebook_validation_report(validation),
        )
    return validation


def _rebuild_flip_casebook(root: Path, casebook_run_id: str) -> list[dict[str, Any]]:
    snapshot = st._read_json(root / FLIP_SNAPSHOT)
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    generated = _aware_utc(snapshot.get("generated_at"), "snapshot.generated_at")
    config_path = Path(_text(policy_source.get("path")))
    normalized = _normalized_casebook(st._load_yaml_mapping(config_path), config_path=config_path)
    _validate_manual_casebook_source(normalized, generated=generated)
    lineage = _mapping(snapshot.get("policy_lineage"))
    for key in ("casebook_id", "version", "status", "owner"):
        if lineage.get(key) != normalized.get(key):
            raise ValueError(f"flip casebook policy lineage mismatch: {key}")
    _, _, expected = _flip_material(
        root=root,
        casebook_run_id=casebook_run_id,
        normalized=normalized,
        generated=generated,
    )
    return diagnostics._check_bytes(root, expected)


def render_flip_rotation_event_casebook_reader_brief(casebook: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Flip Rotation Event Casebook",
            "",
            f"- flip_rotation_casebook_id: {casebook.get('flip_rotation_casebook_id')}",
            f"- flip_rotation_casebook_event_count: {casebook.get('event_count')}",
            f"- flip_rotation_useful_count: {casebook.get('useful_flip_count')}",
            f"- flip_rotation_false_positive_count: {casebook.get('false_positive_count')}",
            f"- flip_rotation_dominant_trigger: {casebook.get('dominant_trigger_signal')}",
            f"- flip_rotation_next_action: {casebook.get('next_review_action')}",
            "- safety_boundary: research diagnostic only / not trading signal / "
            "no data refresh / no official target / no broker / no production",
            "",
        ]
    )


def render_flip_rotation_event_casebook_report(
    manifest: Mapping[str, Any],
    casebook: Mapping[str, Any],
) -> str:
    event_rows = [
        "| "
        f"`{row.get('event_id')}` | "
        f"{row.get('date')} | "
        f"{row.get('previous_state')} -> {row.get('new_state')} | "
        f"{row.get('trigger_signal')} | "
        f"{row.get('flip_useful')} | "
        f"{row.get('false_positive')} | "
        f"{row.get('turnover_impact')} |"
        for row in _records(casebook.get("events"))
    ]
    return "\n".join(
        [
            f"# Flip Rotation Event Casebook {manifest.get('casebook_run_id')}",
            "",
            "## Summary",
            f"- casebook: {casebook.get('flip_rotation_casebook_id')} / {casebook.get('version')}",
            f"- event_count: {casebook.get('event_count')}",
            f"- useful_flip_count: {casebook.get('useful_flip_count')}",
            f"- false_positive_count: {casebook.get('false_positive_count')}",
            f"- dominant_trigger_signal: {casebook.get('dominant_trigger_signal')}",
            f"- next_review_action: {casebook.get('next_review_action')}",
            "",
            "## Source Discipline",
            f"- source_type: {casebook.get('source_type')}",
            "- useful/false-positive and turnover impact labels are manual "
            "diagnostic classifications, not recalculated trading evidence.",
            "",
            "## Events",
            "| event_id | date | state_change | trigger_signal | useful | "
            "false_positive | turnover |",
            "|---|---|---|---|---|---|---|",
            *event_rows,
            "",
            "## Safety Boundary",
            "- research diagnostic only",
            "- not a trading signal",
            "- no data download or upstream rerun",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_flip_rotation_event_casebook_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Flip Rotation Event Casebook Validation {validation.get('artifact_id')}",
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


def _normalized_casebook(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {**FLIP_ROTATION_EVENT_CASEBOOK_SAFETY, **_mapping(config.get("safety"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "casebook_id": _text(
            config.get("casebook_id"),
            "dynamic_v3_rescue_flip_rotation_event_casebook_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "manual_diagnostic_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "source_type": _text(config.get("source_type"), "manual_research_casebook"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "casebook_policy": _mapping(config.get("casebook_policy")),
        "events": [_normalized_event(row) for row in _records(config.get("events"))],
        "safety": safety,
        **safety,
    }


def _normalized_event(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "event_id": _text(row.get("event_id")),
        "event_name": _text(row.get("event_name")),
        "event_type": _text(row.get("event_type")),
        "date": _text(row.get("date")),
        "previous_state": _text(row.get("previous_state")),
        "new_state": _text(row.get("new_state")),
        "trigger_signal": _text(row.get("trigger_signal")),
        "flip_useful": _bool(row.get("flip_useful")),
        "false_positive": _bool(row.get("false_positive")),
        "turnover_impact": _text(row.get("turnover_impact")),
        "candidate_behavior": _text(row.get("candidate_behavior")),
        "review_notes": _texts(row.get("review_notes")),
        "evidence_role": "MANUAL_DIAGNOSTIC",
        "quantitative_evidence_eligible": False,
        "promotion_evidence_eligible": False,
        **FLIP_ROTATION_EVENT_CASEBOOK_SAFETY,
    }


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    return _aware_utc(generated, "generated_at")


def _aware_utc(value: object, field: str) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(_text(value))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return parsed.astimezone(UTC)


def _validate_manual_casebook_source(
    casebook: Mapping[str, Any], *, generated: datetime
) -> None:
    metadata = {
        "flip_rotation_casebook_id": casebook.get("casebook_id"),
        "version": casebook.get("version"),
        "status": casebook.get("status"),
        "owner": casebook.get("owner"),
        "source_type": casebook.get("source_type"),
    }
    if not _metadata_visible(metadata):
        raise ValueError("flip casebook policy metadata is incomplete")
    if _text(casebook.get("status")) not in {
        "manual_diagnostic_baseline",
        "reviewed_manual_diagnostic",
    }:
        raise ValueError("flip casebook policy is not reviewed manual diagnostic")
    events = _records(casebook.get("events"))
    if not events or not all(_event_complete(row) for row in events):
        raise ValueError("flip casebook manual events are incomplete")
    if len({_text(row.get("event_id")) for row in events}) != len(events):
        raise ValueError("flip casebook event ids must be unique")
    if not all(_date_text(row.get("date")) for row in events):
        raise ValueError("flip casebook event dates are invalid")
    if any(_date_text(row.get("date")) > generated.date().isoformat() for row in events):
        raise ValueError("flip casebook event occurs after generated_at")


def _casebook_complete(casebook: Mapping[str, Any]) -> bool:
    events = _records(casebook.get("events"))
    return bool(events) and all(_event_complete(row) for row in events)


def _metadata_visible(casebook: Mapping[str, Any]) -> bool:
    return all(
        bool(_text(casebook.get(key)))
        for key in (
            "flip_rotation_casebook_id",
            "version",
            "status",
            "owner",
            "source_type",
        )
    )


def _event_complete(row: Mapping[str, Any]) -> bool:
    return bool(_text(row.get("event_id"))) and all(
        _field_present(row, field) for field in REQUIRED_FLIP_ROTATION_EVENT_FIELDS
    )


def _field_present(row: Mapping[str, Any], field: str) -> bool:
    if field in {"flip_useful", "false_positive"}:
        return isinstance(row.get(field), bool)
    return bool(_text(row.get(field)))


def _event_booleans_valid(row: Mapping[str, Any]) -> bool:
    return isinstance(row.get("flip_useful"), bool) and isinstance(
        row.get("false_positive"),
        bool,
    )


def _date_text(value: object) -> str:
    text = _text(value)
    if len(text) >= 10:
        return text[:10]
    return ""


def _count_bool(events: list[Mapping[str, Any]], field: str, expected: bool) -> int:
    return sum(1 for row in events if row.get(field) is expected)


def _value_summary(events: list[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts = Counter(_text(row.get(field)) for row in events if _text(row.get(field)))
    return dict(sorted(counts.items()))


def _dominant_value(events: list[Mapping[str, Any]], field: str) -> str:
    counts = _value_summary(events, field)
    if not counts:
        return "MISSING"
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _next_review_action(casebook: Mapping[str, Any]) -> str:
    return _text(
        _mapping(casebook.get("casebook_policy")).get("next_action"),
        "use_casebook_in_next_flip_rotation_review",
    )


def _bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
