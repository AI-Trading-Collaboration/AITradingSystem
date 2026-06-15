from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_DRAWDOWN_EVENT_CASEBOOK_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "drawdown_event_casebook_v1.yaml"
)
DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "drawdown_event_casebook"
)
REQUIRED_DRAWDOWN_EVENT_FIELDS = (
    "event_name",
    "start_date",
    "end_date",
    "max_drawdown",
    "recovery_behavior",
    "regime_label",
    "candidate_response",
    "benchmark_response",
    "review_notes",
)
DRAWDOWN_EVENT_CASEBOOK_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "drawdown_event_casebook_only": True,
    "research_diagnostic_only": True,
    "not_trading_signal": True,
    "data_downloaded_by_casebook": False,
    "pipelines_executed_by_casebook": False,
}


def build_drawdown_event_casebook(
    *,
    config_path: Path = DEFAULT_DRAWDOWN_EVENT_CASEBOOK_CONFIG_PATH,
    output_dir: Path = DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = st._load_yaml_mapping(config_path)
    normalized = _normalized_casebook(config, config_path=config_path)
    casebook_run_id = st._stable_id(
        "drawdown-event-casebook",
        normalized.get("casebook_id"),
        normalized.get("version"),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / casebook_run_id)
    root.mkdir(parents=True, exist_ok=False)
    casebook = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_drawdown_event_casebook",
        "casebook_run_id": root.name,
        "drawdown_casebook_id": normalized.get("casebook_id"),
        "version": normalized.get("version"),
        "status": normalized.get("status"),
        "owner": normalized.get("owner"),
        "source_type": normalized.get("source_type"),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "event_count": len(_records(normalized.get("events"))),
        "worst_event": _worst_event_id(normalized),
        "regime_coverage": _regime_coverage(normalized),
        "candidate_response_summary": _candidate_response_summary(normalized),
        "next_review_action": _next_review_action(normalized),
        "casebook_policy": normalized.get("casebook_policy"),
        "events": normalized.get("events"),
        **DRAWDOWN_EVENT_CASEBOOK_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_drawdown_casebook_manifest",
        "casebook_run_id": root.name,
        "drawdown_casebook_id": normalized.get("casebook_id"),
        "version": normalized.get("version"),
        "status": "PASS" if _casebook_complete(normalized) else "FAIL",
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "drawdown_casebook_manifest_path": str(root / "drawdown_casebook_manifest.json"),
        "drawdown_event_casebook_path": str(root / "drawdown_event_casebook.json"),
        "drawdown_event_casebook_report_path": str(
            root / "drawdown_event_casebook_report.md"
        ),
        "drawdown_event_casebook_reader_brief_path": str(
            root / "drawdown_event_casebook_reader_brief.md"
        ),
        **DRAWDOWN_EVENT_CASEBOOK_SAFETY,
    }
    reader = render_drawdown_event_casebook_reader_brief(casebook)
    st._write_json(root / "drawdown_casebook_manifest.json", manifest)
    st._write_json(root / "drawdown_event_casebook.json", casebook)
    st._write_text(
        root / "drawdown_event_casebook_report.md",
        render_drawdown_event_casebook_report(manifest, casebook),
    )
    st._write_text(root / "drawdown_event_casebook_reader_brief.md", reader)
    st._write_latest_pointer(
        "latest_drawdown_event_casebook",
        root.name,
        root / "drawdown_casebook_manifest.json",
    )
    validation = validate_drawdown_event_casebook_artifact(
        casebook_run_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "casebook_run_id": root.name,
        "casebook_dir": root,
        "manifest": manifest,
        "drawdown_event_casebook": casebook,
        "drawdown_event_casebook_reader_brief": reader,
        "drawdown_event_casebook_validation": validation,
    }


def drawdown_event_casebook_report_payload(
    *,
    casebook_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=casebook_run_id,
        latest_pointer="latest_drawdown_event_casebook",
        latest=latest,
        output_dir=output_dir,
        required_name="drawdown_casebook_manifest.json",
    )
    payload = {
        **st._read_json(root / "drawdown_casebook_manifest.json"),
        "drawdown_event_casebook": st._read_json(root / "drawdown_event_casebook.json"),
        "drawdown_event_casebook_reader_brief": (
            root / "drawdown_event_casebook_reader_brief.md"
        ).read_text(encoding="utf-8"),
        "casebook_dir": str(root),
    }
    validation = st._read_optional_json(root / "drawdown_event_casebook_validation.json")
    if validation:
        payload["drawdown_event_casebook_validation"] = validation
    return payload


def validate_drawdown_event_casebook_artifact(
    *,
    casebook_run_id: str,
    output_dir: Path = DEFAULT_DRAWDOWN_EVENT_CASEBOOK_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / casebook_run_id
    manifest = st._read_optional_json(root / "drawdown_casebook_manifest.json") or {}
    casebook = st._read_optional_json(root / "drawdown_event_casebook.json") or {}
    reader = (
        (root / "drawdown_event_casebook_reader_brief.md").read_text(encoding="utf-8")
        if (root / "drawdown_event_casebook_reader_brief.md").exists()
        else ""
    )
    events = _records(casebook.get("events"))
    event_ids = {_text(row.get("event_id")) for row in events}
    checks = st._required_file_checks(
        root,
        (
            "drawdown_casebook_manifest.json",
            "drawdown_event_casebook.json",
            "drawdown_event_casebook_report.md",
            "drawdown_event_casebook_reader_brief.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "casebook_run_id_matches",
                manifest.get("casebook_run_id") == casebook_run_id,
                "",
            ),
            st._check("metadata_visible", _metadata_visible(casebook), ""),
            st._check("event_count", len(events) >= 3, str(len(events))),
            st._check("event_ids_unique", len(event_ids) == len(events), ""),
            st._check(
                "event_schema_complete",
                all(_event_complete(row) for row in events),
                "",
            ),
            st._check("date_order_valid", all(_event_dates_valid(row) for row in events), ""),
            st._check(
                "max_drawdown_negative",
                all(_float(row.get("max_drawdown")) < 0 for row in events),
                "",
            ),
            st._check("source_type_visible", casebook.get("source_type") != "", ""),
            st._check("reader_brief_fields", "drawdown_casebook_event_count" in reader, ""),
            st._check(
                "casebook_read_only",
                casebook.get("data_downloaded_by_casebook") is False
                and casebook.get("pipelines_executed_by_casebook") is False,
                "",
            ),
            st._check("not_trading_signal", casebook.get("not_trading_signal") is True, ""),
            st._check("broker_forbidden", st._payload_safe(manifest, casebook), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_drawdown_event_casebook_validation",
        casebook_run_id,
        checks,
    )
    if write_output:
        st._write_json(root / "drawdown_event_casebook_validation.json", validation)
        st._write_text(
            root / "drawdown_event_casebook_validation.md",
            render_drawdown_event_casebook_validation_report(validation),
        )
    return validation


def render_drawdown_event_casebook_reader_brief(casebook: Mapping[str, Any]) -> str:
    regime_coverage = ", ".join(_texts(casebook.get("regime_coverage")))
    return "\n".join(
        [
            "## Drawdown Event Casebook",
            "",
            f"- drawdown_casebook_id: {casebook.get('drawdown_casebook_id')}",
            f"- drawdown_casebook_event_count: {casebook.get('event_count')}",
            f"- drawdown_casebook_worst_event: {casebook.get('worst_event')}",
            f"- drawdown_casebook_regime_coverage: {regime_coverage}",
            f"- drawdown_casebook_next_action: {casebook.get('next_review_action')}",
            "- safety_boundary: research diagnostic only / not trading signal / "
            "no data refresh / no official target / no broker / no production",
            "",
        ]
    )


def render_drawdown_event_casebook_report(
    manifest: Mapping[str, Any],
    casebook: Mapping[str, Any],
) -> str:
    event_rows = [
        "| "
        f"`{row.get('event_id')}` | "
        f"{row.get('start_date')} to {row.get('end_date')} | "
        f"{row.get('max_drawdown')} | "
        f"{row.get('regime_label')} | "
        f"{row.get('candidate_response')} |"
        for row in _records(casebook.get("events"))
    ]
    return "\n".join(
        [
            f"# Drawdown Event Casebook {manifest.get('casebook_run_id')}",
            "",
            "## Summary",
            f"- casebook: {casebook.get('drawdown_casebook_id')} / {casebook.get('version')}",
            f"- event_count: {casebook.get('event_count')}",
            f"- worst_event: {casebook.get('worst_event')}",
            f"- regime_coverage: {', '.join(_texts(casebook.get('regime_coverage')))}",
            f"- next_review_action: {casebook.get('next_review_action')}",
            "",
            "## Source Discipline",
            f"- source_type: {casebook.get('source_type')}",
            "- max_drawdown values are manual diagnostic proxies, "
            "not recalculated performance evidence.",
            "",
            "## Events",
            "| event_id | window | max_drawdown | regime | candidate_response |",
            "|---|---|---|---|---|",
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


def render_drawdown_event_casebook_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Drawdown Event Casebook Validation {validation.get('artifact_id')}",
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
    safety = {**DRAWDOWN_EVENT_CASEBOOK_SAFETY, **_mapping(config.get("safety"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "casebook_id": _text(
            config.get("casebook_id"),
            "dynamic_v3_rescue_drawdown_event_casebook_v1",
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
        "start_date": _text(row.get("start_date")),
        "end_date": _text(row.get("end_date")),
        "max_drawdown": _float(row.get("max_drawdown")),
        "max_drawdown_source": _text(row.get("max_drawdown_source")),
        "recovery_behavior": _text(row.get("recovery_behavior")),
        "regime_label": _text(row.get("regime_label")),
        "stress_scenario_id": _text(row.get("stress_scenario_id")),
        "candidate_response": _text(row.get("candidate_response")),
        "benchmark_response": _text(row.get("benchmark_response")),
        "review_notes": _texts(row.get("review_notes")),
        **DRAWDOWN_EVENT_CASEBOOK_SAFETY,
    }


def _casebook_complete(casebook: Mapping[str, Any]) -> bool:
    events = _records(casebook.get("events"))
    return bool(events) and all(_event_complete(row) for row in events)


def _metadata_visible(casebook: Mapping[str, Any]) -> bool:
    return all(
        bool(_text(casebook.get(key)))
        for key in (
            "drawdown_casebook_id",
            "version",
            "status",
            "owner",
            "source_type",
        )
    )


def _event_complete(row: Mapping[str, Any]) -> bool:
    return bool(_text(row.get("event_id"))) and all(
        _field_present(row, field) for field in REQUIRED_DRAWDOWN_EVENT_FIELDS
    )


def _field_present(row: Mapping[str, Any], field: str) -> bool:
    if field == "max_drawdown":
        return _float(row.get(field)) < 0
    if field == "review_notes":
        return bool(_texts(row.get(field)))
    return bool(_text(row.get(field)))


def _event_dates_valid(row: Mapping[str, Any]) -> bool:
    start = _date_text(row.get("start_date"))
    end = _date_text(row.get("end_date"))
    return bool(start and end and start <= end)


def _date_text(value: object) -> str:
    text = _text(value)
    if len(text) >= 10:
        return text[:10]
    return ""


def _worst_event_id(casebook: Mapping[str, Any]) -> str:
    events = _records(casebook.get("events"))
    if not events:
        return "MISSING"
    return _text(min(events, key=lambda row: _float(row.get("max_drawdown"))).get("event_id"))


def _regime_coverage(casebook: Mapping[str, Any]) -> list[str]:
    return sorted({_text(row.get("regime_label")) for row in _records(casebook.get("events"))})


def _candidate_response_summary(casebook: Mapping[str, Any]) -> dict[str, int]:
    counts = Counter(
        _text(row.get("candidate_response"))
        for row in _records(casebook.get("events"))
        if _text(row.get("candidate_response"))
    )
    return dict(sorted(counts.items()))


def _next_review_action(casebook: Mapping[str, Any]) -> str:
    return _text(
        _mapping(casebook.get("casebook_policy")).get("next_action"),
        "use_casebook_in_next_drawdown_mismatch_review",
    )


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
_float = st._float
