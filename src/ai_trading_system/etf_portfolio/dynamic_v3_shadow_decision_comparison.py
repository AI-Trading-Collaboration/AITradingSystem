from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_readiness as readiness,
)
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_SHADOW_DECISION_COMPARISON_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "shadow_decision_comparison"
)
SHADOW_DECISION_COMPARISON_STATUSES = (
    "DECISION_COMPARISON_COMPLETE",
    "DECISION_COMPARISON_WITH_WARNINGS",
    "BLOCKED_MISSING_CURRENT_DECISION",
    "BLOCKED_MISSING_PREVIOUS_DECISION",
)
DECISION_CHANGE_CLASSIFICATIONS = (
    "NO_CHANGE",
    "IMPROVED",
    "DETERIORATED",
    "BLOCKED",
    "RECOVERED",
)
COMPARISON_FIELDS = (
    "safe_to_continue_shadow",
    "readiness_status",
    "weekly_decision",
    "drift_severity",
    "stale_artifacts",
    "missing_artifacts",
    "signal_input_completeness",
    "fallback_status",
    "safety_boundary_status",
)
SHADOW_DECISION_COMPARISON_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "shadow_decision_comparison_only": True,
    "read_only_comparison": True,
    "decision_mutated": False,
    "data_downloaded_by_comparison": False,
    "pipelines_executed_by_comparison": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}

READINESS_RANK = {
    "READY_TO_CONTINUE": 5,
    "READY_WITH_WARNINGS": 4,
    "MANUAL_REVIEW_REQUIRED": 3,
    "BLOCKED_STALE_DATA": 2,
    "BLOCKED_MISSING_ARTIFACTS": 1,
    "BLOCKED_SAFETY_BOUNDARY": 1,
    "MISSING": 0,
    "UNKNOWN": 0,
}
WEEKLY_DECISION_RANK = {
    "CONTINUE": 5,
    "WATCH": 4,
    "RETURN_TO_RESEARCH": 2,
    "REJECT": 1,
    "MISSING": 0,
    "UNKNOWN": 0,
}
DRIFT_SEVERITY_RANK = {
    "NONE": 5,
    "OK": 5,
    "PASS": 5,
    "WATCH": 4,
    "WARNING": 3,
    "BLOCKING": 1,
    "MISSING": 0,
    "UNKNOWN": 0,
}
STATUS_RANK = {
    "PASS": 5,
    "OK": 5,
    "PRIMARY_OK": 5,
    "READY": 5,
    "AVAILABLE": 5,
    "HEALTHY": 5,
    "FRESH": 5,
    "ACCEPTABLE": 5,
    "PASS_WITH_WARNINGS": 4,
    "WARNING": 4,
    "READY_WITH_WARNINGS": 4,
    "HEALTHY_WITH_WARNINGS": 4,
    "MANUAL_REVIEW_REQUIRED": 3,
    "FALLBACK_USED": 3,
    "WATCH": 3,
    "BLOCKING": 1,
    "FAIL": 1,
    "BLOCKED": 1,
    "BLOCKED_SIGNAL_INPUTS": 1,
    "BLOCKED_DATA": 1,
    "BLOCKED_DRIFT": 1,
    "BLOCKED_SAFETY": 1,
    "FALLBACK_UNAVAILABLE": 1,
    "BLOCKED_NO_VALID_SOURCE": 1,
    "MISSING": 0,
    "UNKNOWN": 0,
}


def run_shadow_decision_comparison(
    *,
    as_of: date | None = None,
    current_readiness_id: str | None = None,
    previous_readiness_id: str | None = None,
    readiness_dir: Path = readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    output_dir: Path = DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    current_source = _readiness_source(
        readiness_id=current_readiness_id,
        latest=current_readiness_id is None,
        output_dir=readiness_dir,
    )
    previous_source = _previous_readiness_source(
        previous_readiness_id=previous_readiness_id,
        current_readiness_id=_text(current_source.get("artifact_id")),
        output_dir=readiness_dir,
    )
    current_state = _decision_state(
        current_source,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_dir=paper_shadow_health_dir,
    )
    previous_state = _decision_state(
        previous_source,
        weekly_review_dir=weekly_review_dir,
        paper_shadow_health_dir=paper_shadow_health_dir,
    )
    effective_as_of = (
        as_of
        or _parse_optional_date(current_state.get("as_of"))
        or _parse_optional_date(previous_state.get("as_of"))
        or generated.date()
    )
    delta_summary = _delta_summary(previous_state, current_state)
    status = _comparison_status(current_source, previous_source, delta_summary)
    classification = _change_classification(
        status=status,
        previous_state=previous_state,
        current_state=current_state,
        delta_summary=delta_summary,
    )
    decision_changed = bool(delta_summary["changed_fields"]) or classification != "NO_CHANGE"
    change_reason = _change_reason(
        classification=classification,
        delta_summary=delta_summary,
        current_state=current_state,
        previous_state=previous_state,
    )
    owner_action = _recommended_owner_action(classification)
    candidate = _text(
        current_state.get("candidate"),
        _text(previous_state.get("candidate"), "UNKNOWN"),
    )
    comparison_id = st._stable_id(
        "shadow-decision-comparison",
        candidate,
        effective_as_of.isoformat(),
        _text(current_source.get("artifact_id")),
        _text(previous_source.get("artifact_id")),
        status,
        classification,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / comparison_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_decision_comparison_report",
        "comparison_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "shadow_decision_comparison_status": status,
        "current_readiness_id": current_source.get("artifact_id"),
        "previous_readiness_id": previous_source.get("artifact_id"),
        "decision_changed": decision_changed,
        "change_classification": classification,
        "change_reason": change_reason,
        "previous_state": previous_state,
        "current_state": current_state,
        "delta_summary": delta_summary,
        "recommended_owner_action": owner_action,
        "source_artifacts": {
            "current_shadow_continuation_readiness": current_source,
            "previous_shadow_continuation_readiness": previous_source,
        },
        "blocking_reasons": _blocking_reasons(status, current_source, previous_source),
        "warnings": _warnings(
            status=status,
            previous_state=previous_state,
            current_state=current_state,
            delta_summary=delta_summary,
        ),
        "next_required_action": owner_action,
        "limitations": [
            "shadow decision comparison only",
            "reads existing readiness, weekly and health artifacts",
            "does not refresh data or rerun upstream paper-shadow artifacts",
            "does not mutate weekly decision or paper-shadow state",
            "does not approve production target weights or broker actions",
        ],
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_shadow_decision_comparison_manifest",
        "comparison_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "shadow_decision_comparison_status": status,
        "decision_changed": decision_changed,
        "change_classification": classification,
        "change_reason": change_reason,
        "current_readiness_id": current_source.get("artifact_id"),
        "previous_readiness_id": previous_source.get("artifact_id"),
        "recommended_owner_action": owner_action,
        "shadow_decision_comparison_manifest_path": str(
            root / "shadow_decision_comparison_manifest.json"
        ),
        "shadow_decision_comparison_report_path": str(
            root / "shadow_decision_comparison_report.json"
        ),
        "shadow_decision_comparison_markdown_path": str(
            root / "shadow_decision_comparison_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "shadow_decision_comparison_validation.json"),
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }
    reader = render_shadow_decision_comparison_reader_brief(report)
    st._write_json(root / "shadow_decision_comparison_manifest.json", manifest)
    st._write_json(root / "shadow_decision_comparison_report.json", report)
    st._write_text(
        root / "shadow_decision_comparison_report.md",
        render_shadow_decision_comparison_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_shadow_decision_comparison",
        root.name,
        root / "shadow_decision_comparison_manifest.json",
    )
    validation = validate_shadow_decision_comparison_artifact(
        comparison_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "comparison_id": root.name,
        "comparison_dir": root,
        "manifest": manifest,
        "shadow_decision_comparison_report": report,
        "reader_brief_section": reader,
        "shadow_decision_comparison_validation": validation,
    }


def shadow_decision_comparison_report_payload(
    *,
    comparison_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=comparison_id,
        latest_pointer="latest_shadow_decision_comparison",
        latest=latest,
        output_dir=output_dir,
        required_name="shadow_decision_comparison_manifest.json",
    )
    payload = {
        **st._read_json(root / "shadow_decision_comparison_manifest.json"),
        "shadow_decision_comparison_report": st._read_json(
            root / "shadow_decision_comparison_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "comparison_dir": str(root),
    }
    validation = st._read_optional_json(root / "shadow_decision_comparison_validation.json")
    if validation:
        payload["shadow_decision_comparison_validation"] = validation
    return payload


def validate_shadow_decision_comparison_artifact(
    *,
    comparison_id: str,
    output_dir: Path = DEFAULT_SHADOW_DECISION_COMPARISON_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / comparison_id
    manifest = st._read_optional_json(root / "shadow_decision_comparison_manifest.json") or {}
    report = st._read_optional_json(root / "shadow_decision_comparison_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    previous_state = _mapping(report.get("previous_state"))
    current_state = _mapping(report.get("current_state"))
    delta_summary = _mapping(report.get("delta_summary"))
    checks = st._required_file_checks(
        root,
        (
            "shadow_decision_comparison_manifest.json",
            "shadow_decision_comparison_report.json",
            "shadow_decision_comparison_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_report_id_match",
                manifest.get("comparison_id")
                == report.get("comparison_id")
                == comparison_id,
                "",
            ),
            st._check(
                "status_allowed",
                report.get("shadow_decision_comparison_status")
                in SHADOW_DECISION_COMPARISON_STATUSES,
                _text(report.get("shadow_decision_comparison_status")),
            ),
            st._check(
                "classification_allowed",
                report.get("change_classification") in DECISION_CHANGE_CLASSIFICATIONS,
                _text(report.get("change_classification")),
            ),
            st._check(
                "required_comparison_fields_visible",
                set(COMPARISON_FIELDS).issubset(set(_records_by_field(delta_summary))),
                ",".join(sorted(_records_by_field(delta_summary))),
            ),
            st._check(
                "previous_current_states_visible",
                set(COMPARISON_FIELDS).issubset(previous_state)
                and set(COMPARISON_FIELDS).issubset(current_state),
                "",
            ),
            st._check(
                "decision_change_fields_visible",
                isinstance(report.get("decision_changed"), bool)
                and bool(report.get("change_reason"))
                and bool(report.get("recommended_owner_action")),
                "",
            ),
            st._check(
                "blocked_missing_current_fail_closed",
                (
                    _mapping(
                        _mapping(report.get("source_artifacts")).get(
                            "current_shadow_continuation_readiness"
                        )
                    ).get("exists")
                    is not False
                    or report.get("shadow_decision_comparison_status")
                    == "BLOCKED_MISSING_CURRENT_DECISION"
                ),
                "",
            ),
            st._check(
                "blocked_missing_previous_fail_closed",
                (
                    _mapping(
                        _mapping(report.get("source_artifacts")).get(
                            "previous_shadow_continuation_readiness"
                        )
                    ).get("exists")
                    is not False
                    or report.get("shadow_decision_comparison_status")
                    == "BLOCKED_MISSING_PREVIOUS_DECISION"
                ),
                "",
            ),
            st._check(
                "reader_brief_fields",
                "shadow_decision_comparison_status" in reader
                and "decision_changed" in reader
                and "change_classification" in reader
                and "recommended_owner_action" in reader,
                "",
            ),
            st._check(
                "read_only_comparison",
                report.get("shadow_decision_comparison_only") is True
                and report.get("read_only_comparison") is True
                and report.get("decision_mutated") is False
                and report.get("data_downloaded_by_comparison") is False
                and report.get("pipelines_executed_by_comparison") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_shadow_decision_comparison_validation",
        comparison_id,
        checks,
    )
    if write_output:
        st._write_json(root / "shadow_decision_comparison_validation.json", validation)
        st._write_text(
            root / "shadow_decision_comparison_validation.md",
            render_shadow_decision_comparison_validation_report(validation),
        )
    return validation


def render_shadow_decision_comparison_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Shadow Decision Comparison",
            "",
            f"- shadow_decision_comparison_id: {report.get('comparison_id')}",
            f"- shadow_decision_comparison_status: "
            f"{report.get('shadow_decision_comparison_status')}",
            f"- current_readiness_id: {report.get('current_readiness_id')}",
            f"- previous_readiness_id: {report.get('previous_readiness_id')}",
            f"- decision_changed: {report.get('decision_changed')}",
            f"- change_classification: {report.get('change_classification')}",
            f"- change_reason: {report.get('change_reason')}",
            f"- current_readiness_status: "
            f"{_mapping(report.get('current_state')).get('readiness_status')}",
            f"- previous_readiness_status: "
            f"{_mapping(report.get('previous_state')).get('readiness_status')}",
            f"- recommended_owner_action: {report.get('recommended_owner_action')}",
            "- safety_boundary: read-only shadow decision comparison / no weekly decision "
            "mutation / no data refresh / no official target / no broker / no production",
            "",
        ]
    )


def render_shadow_decision_comparison_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    delta_rows = [
        (
            f"| `{row.get('field')}` | {_markdown_text(row.get('previous_value'))} | "
            f"{_markdown_text(row.get('current_value'))} | {row.get('changed')} | "
            f"{row.get('direction')} |"
        )
        for row in _records(_mapping(report.get("delta_summary")).get("field_deltas"))
    ]
    return "\n".join(
        [
            f"# Shadow Decision Comparison {manifest.get('comparison_id')}",
            "",
            "## Purpose",
            "Compare the current and previous paper-shadow continuation decision state "
            "without mutating upstream decisions or production state.",
            "",
            "## Summary",
            f"- shadow_decision_comparison_status: "
            f"{report.get('shadow_decision_comparison_status')}",
            f"- current_readiness_id: {report.get('current_readiness_id')}",
            f"- previous_readiness_id: {report.get('previous_readiness_id')}",
            f"- decision_changed: {report.get('decision_changed')}",
            f"- change_classification: {report.get('change_classification')}",
            f"- change_reason: {report.get('change_reason')}",
            f"- recommended_owner_action: {report.get('recommended_owner_action')}",
            "",
            "## Delta Summary",
            "| field | previous | current | changed | direction |",
            "|---|---|---|---|---|",
            *delta_rows,
            "",
            "## Safety Boundary",
            "- shadow decision comparison only",
            "- reads existing source artifacts only",
            "- no data refresh or upstream rerun",
            "- no weekly decision mutation",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_shadow_decision_comparison_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Shadow Decision Comparison Validation {validation.get('artifact_id')}",
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


def _readiness_source(
    *,
    readiness_id: str | None,
    latest: bool,
    output_dir: Path,
) -> dict[str, Any]:
    try:
        payload = readiness.shadow_continuation_readiness_report_payload(
            readiness_id=readiness_id,
            latest=latest,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source(
            "shadow_continuation_readiness",
            f"shadow continuation readiness missing: {exc}",
        )
    report = _mapping(payload.get("shadow_continuation_readiness_report"))
    validation = _mapping(payload.get("shadow_continuation_readiness_validation"))
    return _source(
        "shadow_continuation_readiness",
        exists=True,
        artifact_id=_text(payload.get("readiness_id"), _text(report.get("readiness_id"))),
        status=_text(report.get("shadow_continuation_readiness"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=Path(_text(payload.get("shadow_continuation_readiness_manifest_path"))),
        summary=_readiness_summary(report),
        payload=report,
    )


def _previous_readiness_source(
    *,
    previous_readiness_id: str | None,
    current_readiness_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    if previous_readiness_id:
        return _readiness_source(
            readiness_id=previous_readiness_id,
            latest=False,
            output_dir=output_dir,
        )
    previous_id = _latest_prior_readiness_id(
        output_dir=output_dir,
        current_readiness_id=current_readiness_id,
    )
    if not previous_id:
        return _missing_source(
            "shadow_continuation_readiness",
            "previous shadow continuation readiness artifact missing",
        )
    return _readiness_source(
        readiness_id=previous_id,
        latest=False,
        output_dir=output_dir,
    )


def _latest_prior_readiness_id(
    *,
    output_dir: Path,
    current_readiness_id: str,
) -> str:
    candidates: list[tuple[datetime, str]] = []
    if not output_dir.exists():
        return ""
    for manifest_path in output_dir.glob("*/shadow_continuation_readiness_manifest.json"):
        artifact_id = manifest_path.parent.name
        if artifact_id == current_readiness_id:
            continue
        manifest = st._read_optional_json(manifest_path) or {}
        generated = _parse_optional_datetime(manifest.get("generated_at"))
        timestamp = generated or datetime.fromtimestamp(
            manifest_path.stat().st_mtime,
            tz=UTC,
        )
        candidates.append((timestamp, artifact_id))
    if not candidates:
        return ""
    return max(candidates, key=lambda row: (row[0], row[1]))[1]


def _decision_state(
    source: Mapping[str, Any],
    *,
    weekly_review_dir: Path,
    paper_shadow_health_dir: Path,
) -> dict[str, Any]:
    if source.get("exists") is False:
        return _missing_state(_text(source.get("limitation"), "decision artifact missing"))
    report = _mapping(source.get("payload"))
    source_artifacts = _mapping(report.get("source_artifacts"))
    weekly_source = _mapping(source_artifacts.get("paper_shadow_weekly_review"))
    weekly_summary = _source_summary(weekly_source)
    drift_source = _mapping(source_artifacts.get("paper_shadow_drift_monitor"))
    signal_source = _mapping(source_artifacts.get("signal_input_completeness"))
    signal_summary = _source_summary(signal_source)
    fallback_summary = _mapping(report.get("fallback_policy_summary"))
    health_report = _matching_health_report(
        readiness_id=_text(report.get("readiness_id")),
        output_dir=paper_shadow_health_dir,
    )
    weekly_review = _weekly_review_payload(
        weekly_review_id=_text(weekly_source.get("artifact_id")),
        output_dir=weekly_review_dir,
    )
    fallback_status = _text(
        report.get("fallback_status"),
        _text(
            fallback_summary.get("fallback_status"),
            _text(health_report.get("fallback_status"), "MISSING"),
        ),
    )
    signal_status = _text(
        report.get("signal_input_status"),
        _text(
            signal_summary.get("signal_input_status"),
            _text(
                signal_source.get("status"),
                _text(health_report.get("signal_input_status"), "MISSING"),
            ),
        ),
    )
    weekly_decision = _text(
        weekly_review.get("weekly_decision"),
        _text(
            weekly_summary.get("weekly_decision"),
            _text(
                weekly_source.get("weekly_decision"),
                _text(weekly_source.get("status"), "MISSING"),
            ),
        ),
    )
    weekly_drift_trend = _mapping(weekly_review.get("summary")).get(
        "drift_severity_trend"
    )
    weekly_drift_status = (
        _mapping(weekly_drift_trend).get("max_severity")
        if isinstance(weekly_drift_trend, Mapping)
        else ""
    )
    drift_status = _text(
        weekly_drift_status,
        _text(
            _mapping(weekly_summary.get("drift_severity_trend")).get("max_severity"),
            _text(
                drift_source.get("drift_status"),
                _text(
                    drift_source.get("status"),
                    _text(health_report.get("drift_status"), "MISSING"),
                ),
            ),
        ),
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "readiness_id": _text(report.get("readiness_id"), _text(source.get("artifact_id"))),
        "candidate": _text(report.get("candidate"), "UNKNOWN"),
        "as_of": _text(report.get("as_of")),
        "generated_at": _text(report.get("generated_at")),
        "safe_to_continue_shadow": report.get("safe_to_continue_shadow") is True,
        "readiness_status": _text(
            report.get("shadow_continuation_readiness"),
            _text(source.get("status"), "UNKNOWN"),
        ),
        "weekly_decision": weekly_decision,
        "drift_severity": drift_status,
        "stale_artifacts": _texts(report.get("stale_artifacts")),
        "missing_artifacts": _texts(report.get("missing_artifacts")),
        "signal_input_completeness": signal_status,
        "fallback_status": fallback_status,
        "safety_boundary_status": _text(report.get("safety_boundary_status"), "MISSING"),
        "blocking_artifacts": _texts(report.get("blocking_artifacts")),
        "manual_review_required": report.get("manual_review_required") is True,
        "next_required_action": _text(report.get("next_required_action")),
        "source_validation_status": _text(source.get("validation_status"), "NOT_RUN"),
        "weekly_review_id": _text(weekly_source.get("artifact_id")),
        "paper_shadow_health_id": _text(health_report.get("health_id")),
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }


def _matching_health_report(*, readiness_id: str, output_dir: Path) -> dict[str, Any]:
    if not readiness_id or not output_dir.exists():
        return {}
    latest: tuple[datetime, dict[str, Any]] | None = None
    for report_path in output_dir.glob("*/paper_shadow_health_report.json"):
        report = st._read_optional_json(report_path) or {}
        source_artifacts = _mapping(report.get("source_artifacts"))
        readiness_source = _mapping(source_artifacts.get("shadow_continuation_readiness"))
        if readiness_id not in {
            _text(readiness_source.get("artifact_id")),
            _text(_source_summary(readiness_source).get("readiness_id")),
            _text(report.get("readiness_id")),
        }:
            continue
        generated = _parse_optional_datetime(report.get("generated_at")) or datetime.fromtimestamp(
            report_path.stat().st_mtime,
            tz=UTC,
        )
        if latest is None or generated > latest[0]:
            latest = (generated, report)
    return latest[1] if latest else {}


def _weekly_review_payload(*, weekly_review_id: str, output_dir: Path) -> dict[str, Any]:
    if not weekly_review_id:
        return {}
    try:
        payload = weekly.paper_shadow_weekly_review_report_payload(
            weekly_review_id=weekly_review_id,
            output_dir=output_dir,
        )
    except Exception:
        return {}
    return _mapping(payload.get("paper_shadow_weekly_review"))


def _delta_summary(
    previous_state: Mapping[str, Any],
    current_state: Mapping[str, Any],
) -> dict[str, Any]:
    rows = []
    changed_fields = []
    improved_fields = []
    deteriorated_fields = []
    for field in COMPARISON_FIELDS:
        previous_value = previous_state.get(field)
        current_value = current_state.get(field)
        changed = _normalized_value(previous_value) != _normalized_value(current_value)
        direction = _field_direction(field, previous_value, current_value)
        if changed:
            changed_fields.append(field)
        if direction == "IMPROVED":
            improved_fields.append(field)
        if direction == "DETERIORATED":
            deteriorated_fields.append(field)
        rows.append(
            {
                "schema_version": st.SCHEMA_VERSION,
                "field": field,
                "previous_value": previous_value,
                "current_value": current_value,
                "changed": changed,
                "direction": direction,
                **SHADOW_DECISION_COMPARISON_SAFETY,
            }
        )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "changed_fields": changed_fields,
        "changed_field_count": len(changed_fields),
        "improved_fields": improved_fields,
        "deteriorated_fields": deteriorated_fields,
        "field_deltas": rows,
        "previous_score": _state_score(previous_state),
        "current_score": _state_score(current_state),
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }


def _field_direction(field: str, previous_value: Any, current_value: Any) -> str:
    previous_norm = _normalized_value(previous_value)
    current_norm = _normalized_value(current_value)
    if previous_norm == current_norm:
        return "NO_CHANGE"
    if field == "safe_to_continue_shadow":
        if previous_value is False and current_value is True:
            return "IMPROVED"
        if previous_value is True and current_value is False:
            return "DETERIORATED"
        return "CHANGED"
    if field in {"stale_artifacts", "missing_artifacts"}:
        previous_count = len(_texts(previous_value))
        current_count = len(_texts(current_value))
        if current_count < previous_count:
            return "IMPROVED"
        if current_count > previous_count:
            return "DETERIORATED"
        return "CHANGED"
    previous_rank = _field_rank(field, previous_value)
    current_rank = _field_rank(field, current_value)
    if current_rank > previous_rank:
        return "IMPROVED"
    if current_rank < previous_rank:
        return "DETERIORATED"
    return "CHANGED"


def _change_classification(
    *,
    status: str,
    previous_state: Mapping[str, Any],
    current_state: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
) -> str:
    if status in {"BLOCKED_MISSING_CURRENT_DECISION", "BLOCKED_MISSING_PREVIOUS_DECISION"}:
        return "BLOCKED"
    previous_safe = previous_state.get("safe_to_continue_shadow") is True
    current_safe = current_state.get("safe_to_continue_shadow") is True
    current_readiness = _text(current_state.get("readiness_status"))
    if not _texts(delta_summary.get("changed_fields")):
        return "NO_CHANGE"
    if current_readiness.startswith("BLOCKED_") or (
        previous_safe and not current_safe
    ):
        return "BLOCKED"
    if not previous_safe and current_safe:
        return "RECOVERED"
    deteriorated = _texts(delta_summary.get("deteriorated_fields"))
    improved = _texts(delta_summary.get("improved_fields"))
    if deteriorated and not improved:
        return "DETERIORATED"
    if improved and not deteriorated:
        return "IMPROVED"
    previous_score = _int(delta_summary.get("previous_score"))
    current_score = _int(delta_summary.get("current_score"))
    if current_score > previous_score:
        return "IMPROVED"
    if current_score < previous_score:
        return "DETERIORATED"
    return "NO_CHANGE"


def _comparison_status(
    current_source: Mapping[str, Any],
    previous_source: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
) -> str:
    if current_source.get("exists") is False:
        return "BLOCKED_MISSING_CURRENT_DECISION"
    if previous_source.get("exists") is False:
        return "BLOCKED_MISSING_PREVIOUS_DECISION"
    if _texts(delta_summary.get("deteriorated_fields")):
        return "DECISION_COMPARISON_WITH_WARNINGS"
    return "DECISION_COMPARISON_COMPLETE"


def _change_reason(
    *,
    classification: str,
    delta_summary: Mapping[str, Any],
    current_state: Mapping[str, Any],
    previous_state: Mapping[str, Any],
) -> str:
    changed_fields = _texts(delta_summary.get("changed_fields"))
    if classification == "NO_CHANGE":
        return "no tracked shadow decision fields changed"
    if classification == "BLOCKED":
        blockers = _texts(current_state.get("blocking_artifacts"))
        if blockers:
            return f"current decision blocked by {','.join(blockers)}"
        return "current or previous decision artifact is blocked or missing"
    if classification == "RECOVERED":
        return "safe_to_continue_shadow recovered from false to true"
    if changed_fields:
        return f"{classification.lower()} fields: {','.join(changed_fields)}"
    return f"shadow decision classified as {classification}"


def _recommended_owner_action(classification: str) -> str:
    if classification == "NO_CHANGE":
        return "continue_existing_owner_review_cadence"
    if classification == "IMPROVED":
        return "review_improvement_before_continuing_shadow"
    if classification == "DETERIORATED":
        return "review_deterioration_before_continuing_shadow"
    if classification == "RECOVERED":
        return "confirm_recovery_before_resuming_shadow"
    return "hold_shadow_until_blockers_resolved"


def _blocking_reasons(
    status: str,
    current_source: Mapping[str, Any],
    previous_source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if status == "BLOCKED_MISSING_CURRENT_DECISION":
        reasons.append(f"current_shadow_continuation_readiness:{current_source.get('limitation')}")
    if status == "BLOCKED_MISSING_PREVIOUS_DECISION":
        reasons.append(
            f"previous_shadow_continuation_readiness:{previous_source.get('limitation')}"
        )
    return reasons


def _warnings(
    *,
    status: str,
    previous_state: Mapping[str, Any],
    current_state: Mapping[str, Any],
    delta_summary: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if status == "DECISION_COMPARISON_WITH_WARNINGS":
        warnings.append("decision_state_deteriorated")
    for field in _texts(delta_summary.get("deteriorated_fields")):
        warnings.append(f"{field}:deteriorated")
    if previous_state.get("source_validation_status") not in {"PASS", "NOT_RUN"}:
        warnings.append("previous_decision_validation_not_pass")
    if current_state.get("source_validation_status") not in {"PASS", "NOT_RUN"}:
        warnings.append("current_decision_validation_not_pass")
    return warnings


def _state_score(state: Mapping[str, Any]) -> int:
    return sum(_field_rank(field, state.get(field)) for field in COMPARISON_FIELDS)


def _field_rank(field: str, value: Any) -> int:
    if field == "safe_to_continue_shadow":
        return 5 if value is True else 1 if value is False else 0
    if field == "readiness_status":
        return READINESS_RANK.get(_text(value), 0)
    if field == "weekly_decision":
        return WEEKLY_DECISION_RANK.get(_text(value), 0)
    if field == "drift_severity":
        return DRIFT_SEVERITY_RANK.get(_text(value), 0)
    if field in {"stale_artifacts", "missing_artifacts"}:
        return max(0, 5 - len(_texts(value)))
    return STATUS_RANK.get(_text(value), 0)


def _readiness_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "readiness_id": report.get("readiness_id"),
        "candidate": report.get("candidate"),
        "as_of": report.get("as_of"),
        "safe_to_continue_shadow": report.get("safe_to_continue_shadow"),
        "readiness_status": report.get("shadow_continuation_readiness"),
        "stale_artifacts": report.get("stale_artifacts"),
        "missing_artifacts": report.get("missing_artifacts"),
        "blocking_artifacts": report.get("blocking_artifacts"),
        "signal_input_status": report.get("signal_input_status"),
        "fallback_status": report.get("fallback_status"),
        "safety_boundary_status": report.get("safety_boundary_status"),
        "next_required_action": report.get("next_required_action"),
    }


def _missing_state(limitation: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "readiness_id": "MISSING",
        "candidate": "UNKNOWN",
        "as_of": "",
        "generated_at": "",
        "safe_to_continue_shadow": False,
        "readiness_status": "MISSING",
        "weekly_decision": "MISSING",
        "drift_severity": "MISSING",
        "stale_artifacts": [],
        "missing_artifacts": ["shadow_continuation_readiness"],
        "signal_input_completeness": "MISSING",
        "fallback_status": "MISSING",
        "safety_boundary_status": "MISSING",
        "blocking_artifacts": ["shadow_continuation_readiness"],
        "manual_review_required": True,
        "next_required_action": "provide_previous_and_current_shadow_decision_artifacts",
        "source_validation_status": "MISSING",
        "weekly_review_id": "MISSING",
        "paper_shadow_health_id": "MISSING",
        "limitation": limitation,
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }


def _source(
    source_id: str,
    *,
    exists: bool,
    artifact_id: str,
    status: str,
    validation_status: str,
    source_path: Path | None,
    summary: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "payload": dict(payload),
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }


def _missing_source(source_id: str, limitation: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {},
        "payload": {},
        "limitation": limitation,
        **SHADOW_DECISION_COMPARISON_SAFETY,
    }


def _source_summary(source: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(source.get("summary"))


def _records_by_field(delta_summary: Mapping[str, Any]) -> set[str]:
    return {
        _text(row.get("field"))
        for row in _records(delta_summary.get("field_deltas"))
        if _text(row.get("field"))
    }


def _normalized_value(value: Any) -> Any:
    if isinstance(value, list | tuple | set):
        return sorted(_texts(value))
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_text(item) for item in value if _text(item)]
    return []


def _joined_texts(value: Any) -> str:
    return ",".join(_texts(value)) or "none"


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return str(value)
    text = str(value)
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_optional_date(value: Any) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _parse_optional_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _markdown_text(value: Any) -> str:
    if isinstance(value, list | tuple | set):
        return _joined_texts(value).replace("|", "\\|")
    return _text(value).replace("|", "\\|")
