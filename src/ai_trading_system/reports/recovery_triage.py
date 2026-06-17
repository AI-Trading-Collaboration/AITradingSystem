from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.research_governance_recovery_pack import (
    SOURCE_REPORT_SPECS,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _read_optional_json_mapping,
    _records,
    _report_index_entry,
    _text,
)

SCHEMA_VERSION = 1

RECOVERY_BLOCKER_TRIAGE_REPORT_TYPE = "recovery_blocker_triage"
RECOVERY_BLOCKER_TRIAGE_VALIDATION_REPORT_TYPE = "recovery_blocker_triage_validation"
REPORT_INDEX_WARNING_TRIAGE_REPORT_TYPE = "report_index_warning_triage"
REPORT_INDEX_WARNING_TRIAGE_VALIDATION_REPORT_TYPE = (
    "report_index_warning_triage_validation"
)
RECOVERY_PACK_SOURCE_DEPTH_AUDIT_REPORT_TYPE = "recovery_pack_source_depth_audit"
RECOVERY_PACK_SOURCE_DEPTH_AUDIT_VALIDATION_REPORT_TYPE = (
    "recovery_pack_source_depth_audit_validation"
)
RECOVERY_OWNER_ACTION_MAP_REPORT_TYPE = "recovery_owner_action_map"
RECOVERY_OWNER_ACTION_MAP_VALIDATION_REPORT_TYPE = "recovery_owner_action_map_validation"

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

RECOVERY_BLOCKERS_PRESENT = "RECOVERY_BLOCKERS_PRESENT"
RECOVERY_BLOCKERS_CLEAR = "RECOVERY_BLOCKERS_CLEAR"
REPORT_INDEX_WARNINGS_PRESENT = "REPORT_INDEX_WARNINGS_PRESENT"
REPORT_INDEX_WARNINGS_CLEAR = "REPORT_INDEX_WARNINGS_CLEAR"
RECOVERY_SOURCE_HEALTHY = "RECOVERY_SOURCE_HEALTHY"
RECOVERY_SOURCE_HEALTHY_WITH_WARNINGS = "RECOVERY_SOURCE_HEALTHY_WITH_WARNINGS"
RECOVERY_SOURCE_BLOCKED = "RECOVERY_SOURCE_BLOCKED"
OWNER_ACTIONS_REQUIRED = "OWNER_ACTIONS_REQUIRED"
OWNER_ACTIONS_CLEAR = "OWNER_ACTIONS_CLEAR"

CORE_READER_BRIEF_FIELDS = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)

BLOCKER_POLICY: dict[str, dict[str, Any]] = {
    "normal_paper_shadow_resumption_gate": {
        "category": "owner_resumption_gate",
        "related_dimensions": ("owner", "safety"),
        "required_artifact_or_owner_action": (
            "record manual owner review and rerun normal paper-shadow resumption gate"
        ),
    },
    "cost_sensitivity_metrics": {
        "category": "metric_validity",
        "related_dimensions": ("metric",),
        "required_artifact_or_owner_action": (
            "regenerate cost sensitivity evidence only after candidate net improvement "
            "survives costs"
        ),
    },
    "benchmark_baseline_metrics": {
        "category": "metric_validity",
        "related_dimensions": ("metric",),
        "required_artifact_or_owner_action": (
            "return candidate to research until it outperforms baseline controls"
        ),
    },
    "monthly_review": {
        "category": "governance_review",
        "related_dimensions": ("owner", "roadmap"),
        "required_artifact_or_owner_action": (
            "resolve monthly review blockers or record explicit owner hold"
        ),
    },
    "promotion_board": {
        "category": "promotion_safety_gate",
        "related_dimensions": ("owner", "safety"),
        "required_artifact_or_owner_action": (
            "keep promotion board blocked until evidence blockers are resolved"
        ),
    },
    "observation_clock": {
        "category": "observation_metric",
        "related_dimensions": ("metric", "safety"),
        "required_artifact_or_owner_action": (
            "continue collecting valid observation days before extended shadow review"
        ),
    },
    "extended_shadow_protocol": {
        "category": "extended_shadow_safety_gate",
        "related_dimensions": ("safety",),
        "required_artifact_or_owner_action": (
            "resolve extended shadow blockers before owner review"
        ),
    },
    "roadmap_dashboard": {
        "category": "roadmap_governance",
        "related_dimensions": ("roadmap",),
        "required_artifact_or_owner_action": "review top roadmap blockers",
    },
    "decision_snapshot_lifecycle": {
        "category": "registry_data_lifecycle",
        "related_dimensions": ("data", "registry"),
        "required_artifact_or_owner_action": (
            "run canonical score-daily or keep same-day Reader Brief blocked"
        ),
    },
}

SOURCE_STATUS_FIELDS = {
    _text(spec.get("source_id")): tuple(str(field) for field in spec["status_fields"])
    for spec in SOURCE_REPORT_SPECS
}


def default_recovery_blocker_triage_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_blocker_triage_{as_of.isoformat()}.json"


def default_recovery_blocker_triage_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_blocker_triage_{as_of.isoformat()}.md"


def default_recovery_blocker_triage_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_blocker_triage_validation_{as_of.isoformat()}.json"


def default_recovery_blocker_triage_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_blocker_triage_validation_{as_of.isoformat()}.md"


def latest_recovery_blocker_triage_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "recovery_blocker_triage_", ".json")


def default_report_index_warning_triage_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_warning_triage_{as_of.isoformat()}.json"


def default_report_index_warning_triage_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"report_index_warning_triage_{as_of.isoformat()}.md"


def default_report_index_warning_triage_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"report_index_warning_triage_validation_{as_of.isoformat()}.json"


def default_report_index_warning_triage_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"report_index_warning_triage_validation_{as_of.isoformat()}.md"


def latest_report_index_warning_triage_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "report_index_warning_triage_", ".json")


def default_recovery_pack_source_depth_audit_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_pack_source_depth_audit_{as_of.isoformat()}.json"


def default_recovery_pack_source_depth_audit_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_pack_source_depth_audit_{as_of.isoformat()}.md"


def default_recovery_pack_source_depth_audit_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"recovery_pack_source_depth_audit_validation_{as_of.isoformat()}.json"
    )


def default_recovery_pack_source_depth_audit_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return (
        output_dir
        / f"recovery_pack_source_depth_audit_validation_{as_of.isoformat()}.md"
    )


def latest_recovery_pack_source_depth_audit_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "recovery_pack_source_depth_audit_", ".json")


def default_recovery_owner_action_map_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_owner_action_map_{as_of.isoformat()}.json"


def default_recovery_owner_action_map_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_owner_action_map_{as_of.isoformat()}.md"


def default_recovery_owner_action_map_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_owner_action_map_validation_{as_of.isoformat()}.json"


def default_recovery_owner_action_map_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_owner_action_map_validation_{as_of.isoformat()}.md"


def latest_recovery_owner_action_map_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "recovery_owner_action_map_", ".json")


def build_recovery_blocker_triage_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any] | None = None,
    recovery_pack_path: Path | None = None,
) -> dict[str, Any]:
    if recovery_pack_payload is None:
        if recovery_pack_path is None:
            recovery_pack_path = (
                PROJECT_ROOT
                / "outputs"
                / "reports"
                / f"research_governance_recovery_pack_{as_of.isoformat()}.json"
            )
        recovery_pack_payload = _read_json_mapping(recovery_pack_path)

    source_by_id = {
        _text(source.get("source_id")): source
        for source in _records(recovery_pack_payload.get("source_reports"))
    }
    blockers = [
        _blocker_triage_item(blocker, source_by_id)
        for blocker in _records(recovery_pack_payload.get("remaining_blockers"))
    ]
    summary = _blocker_triage_summary(recovery_pack_payload, blockers)
    status = RECOVERY_BLOCKERS_PRESENT if blockers else RECOVERY_BLOCKERS_CLEAR
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RECOVERY_BLOCKER_TRIAGE_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "triage_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "triage_only": True,
        "input_artifacts": {
            "research_governance_recovery_pack": (
                "" if recovery_pack_path is None else str(recovery_pack_path)
            )
        },
        "summary": summary,
        "blocker_triage": blockers,
        "reader_brief": _blocker_triage_reader_brief(summary, blockers),
        "safety_boundary": _triage_safety_boundary(),
        "limitations": [
            "This report triages blockers only and does not resolve them.",
            "Normal paper-shadow remains blocked unless source blockers are resolved.",
            "Extended shadow and live trading remain forbidden.",
        ],
        "next_action": summary["next_owner_action"],
    }


def validate_recovery_blocker_triage_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    triage_items = _records(payload.get("blocker_triage"))
    summary = _mapping(payload.get("summary"))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == RECOVERY_BLOCKER_TRIAGE_REPORT_TYPE,
        f"report_type must be {RECOVERY_BLOCKER_TRIAGE_REPORT_TYPE}.",
        "regenerate_recovery_blocker_triage",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_count_matches_items",
        _int(summary.get("recovery_blocker_count")) == len(triage_items),
        "Blocker summary count must match blocker_triage items.",
        "regenerate_recovery_blocker_triage",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_item_fields",
        all(_blocker_item_complete(item) for item in triage_items),
        "Every blocker triage item must expose source, category, field, action, and boundaries.",
        "restore_blocker_triage_required_fields",
    )
    blocker_count = _int(summary.get("recovery_blocker_count"))
    _append_check(
        checks,
        blocking_issues,
        "blocked_boundaries_preserved",
        (
            blocker_count == 0
            or (
                summary.get("normal_paper_shadow_may_resume") is False
                and summary.get("extended_shadow_remains_forbidden") is True
                and summary.get("live_trading_remains_forbidden") is True
            )
        ),
        "Visible blockers must not loosen paper/extended/live trading boundaries.",
        "restore_fail_closed_recovery_boundaries",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Recovery blocker triage must preserve the read-only safety boundary.",
        "restore_recovery_triage_safety_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_recovery_blocker_triage_reader_brief",
    )
    return _validation_payload(
        report_type=RECOVERY_BLOCKER_TRIAGE_VALIDATION_REPORT_TYPE,
        as_of=_text(payload.get("as_of"), date.today().isoformat()),
        checks=checks,
        blocking_issues=blocking_issues,
        warning_issues=_visible_warning_issues(
            "recovery_blockers_still_present",
            blocker_count,
            "Recovery blockers remain present; this task triages them only.",
            _text(payload.get("next_action"), "review_recovery_blocker_triage"),
        ),
        input_artifacts=_mapping(payload.get("input_artifacts")),
        summary_extra={
            "recovery_blocker_count": blocker_count,
            "normal_paper_shadow_may_resume": bool(
                summary.get("normal_paper_shadow_may_resume")
            ),
            "extended_shadow_remains_forbidden": bool(
                summary.get("extended_shadow_remains_forbidden")
            ),
            "live_trading_remains_forbidden": bool(
                summary.get("live_trading_remains_forbidden")
            ),
        },
    )


def build_report_index_warning_triage_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
) -> dict[str, Any]:
    if report_index_payload is None:
        if report_index_path is None:
            report_index_path = (
                PROJECT_ROOT / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
            )
        report_index_payload = _read_json_mapping(report_index_path)

    warning_items = _report_index_warning_items(report_index_payload)
    summary = _report_index_warning_summary(report_index_payload, warning_items)
    status = REPORT_INDEX_WARNINGS_PRESENT if warning_items else REPORT_INDEX_WARNINGS_CLEAR
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_INDEX_WARNING_TRIAGE_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "triage_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "triage_only": True,
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path)
        },
        "summary": summary,
        "warning_triage": warning_items,
        "reader_brief": _report_index_warning_reader_brief(summary, warning_items),
        "safety_boundary": _triage_safety_boundary(),
        "limitations": [
            "This report triages unwaived report-index warnings only.",
            "It does not create, renew, or apply waivers.",
            "It does not regenerate stale artifacts or refresh data.",
        ],
        "next_action": summary["next_action"],
    }


def validate_report_index_warning_triage_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warnings = _records(payload.get("warning_triage"))
    summary = _mapping(payload.get("summary"))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_INDEX_WARNING_TRIAGE_REPORT_TYPE,
        f"report_type must be {REPORT_INDEX_WARNING_TRIAGE_REPORT_TYPE}.",
        "regenerate_report_index_warning_triage",
    )
    _append_check(
        checks,
        blocking_issues,
        "warning_count_matches_items",
        _int(summary.get("unwaived_warning_count")) == len(warnings),
        "Unwaived warning summary count must match warning_triage items.",
        "regenerate_report_index_warning_triage",
    )
    _append_check(
        checks,
        blocking_issues,
        "warnings_are_not_silently_waived",
        all(_text(item.get("waiver_action")) == "not_applied" for item in warnings),
        "Warning triage must not apply waivers.",
        "remove_silent_warning_waiver",
    )
    _append_check(
        checks,
        blocking_issues,
        "explicit_waiver_recommendations_are_complete",
        all(_waiver_recommendation_complete(item) for item in warnings),
        "Explicit waiver recommendations must include reason, owner, expiry, and task link.",
        "complete_explicit_waiver_recommendation_metadata",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_report_index_warning_triage_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Report-index warning triage must preserve the read-only safety boundary.",
        "restore_recovery_triage_safety_boundary",
    )
    warning_count = _int(summary.get("unwaived_warning_count"))
    return _validation_payload(
        report_type=REPORT_INDEX_WARNING_TRIAGE_VALIDATION_REPORT_TYPE,
        as_of=_text(payload.get("as_of"), date.today().isoformat()),
        checks=checks,
        blocking_issues=blocking_issues,
        warning_issues=_visible_warning_issues(
            "report_index_unwaived_warnings_remain",
            warning_count,
            "Unwaived report-index warnings remain visible after triage.",
            _text(payload.get("next_action"), "review_report_index_warning_triage"),
        ),
        input_artifacts=_mapping(payload.get("input_artifacts")),
        summary_extra={
            "unwaived_warning_count": warning_count,
            "true_blocker_count": _int(summary.get("true_blocker_count")),
            "silent_waiver_count": _int(summary.get("silent_waiver_count")),
        },
    )


def build_recovery_pack_source_depth_audit_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any] | None = None,
    recovery_pack_path: Path | None = None,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if recovery_pack_payload is None:
        if recovery_pack_path is None:
            recovery_pack_path = (
                project_root
                / "outputs"
                / "reports"
                / f"research_governance_recovery_pack_{as_of.isoformat()}.json"
            )
        recovery_pack_payload = _read_json_mapping(recovery_pack_path)
    if report_index_payload is None:
        resolved_index = report_index_path or _pack_report_index_path(
            recovery_pack_payload,
            project_root,
        )
        if resolved_index is not None and resolved_index.exists():
            report_index_payload = _read_json_mapping(resolved_index)
            report_index_path = resolved_index
        else:
            report_index_payload = {}

    source_audits = [
        _source_depth_item(source, report_index_payload, project_root)
        for source in _records(recovery_pack_payload.get("source_reports"))
    ]
    summary = _source_depth_summary(recovery_pack_payload, source_audits)
    status = _source_depth_status(summary)
    unhealthy_sources = [
        source for source in source_audits if _text(source.get("health_status")) != "HEALTHY"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RECOVERY_PACK_SOURCE_DEPTH_AUDIT_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "source_depth_audit_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "audit_only": True,
        "input_artifacts": {
            "research_governance_recovery_pack": (
                "" if recovery_pack_path is None else str(recovery_pack_path)
            ),
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "summary": summary,
        "source_depth": source_audits,
        "unhealthy_sources": unhealthy_sources,
        "reader_brief": _source_depth_reader_brief(summary, unhealthy_sources),
        "safety_boundary": _triage_safety_boundary(),
        "limitations": [
            "Source depth audit reads existing recovery pack sources only.",
            "It does not rerun source reports or repair stale/blocked artifacts.",
        ],
        "next_action": summary["next_action"],
    }


def validate_recovery_pack_source_depth_audit_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    source_depth = _records(payload.get("source_depth"))
    summary = _mapping(payload.get("summary"))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == RECOVERY_PACK_SOURCE_DEPTH_AUDIT_REPORT_TYPE,
        f"report_type must be {RECOVERY_PACK_SOURCE_DEPTH_AUDIT_REPORT_TYPE}.",
        "regenerate_recovery_pack_source_depth_audit",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_count_matches_items",
        _int(summary.get("source_report_count")) == len(source_depth),
        "Source depth summary count must match source_depth items.",
        "regenerate_recovery_pack_source_depth_audit",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_availability_matches_count",
        _text(summary.get("source_availability"))
        == f"{_int(summary.get('available_source_count'))}/{len(source_depth)}",
        "Source availability must expose available/total source count.",
        "restore_source_availability_summary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_source_fields",
        all(_source_depth_item_complete(source) for source in source_depth),
        "Every source depth item must expose id, date, status, freshness, health, and action.",
        "restore_source_depth_required_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_source_depth_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Source depth audit must preserve the read-only safety boundary.",
        "restore_recovery_triage_safety_boundary",
    )
    unhealthy_count = _int(summary.get("unhealthy_source_count"))
    return _validation_payload(
        report_type=RECOVERY_PACK_SOURCE_DEPTH_AUDIT_VALIDATION_REPORT_TYPE,
        as_of=_text(payload.get("as_of"), date.today().isoformat()),
        checks=checks,
        blocking_issues=blocking_issues,
        warning_issues=_visible_warning_issues(
            "recovery_sources_unhealthy",
            unhealthy_count,
            "At least one recovery pack source is blocked, warning, stale, or partial.",
            _text(payload.get("next_action"), "review_recovery_source_depth_audit"),
        ),
        input_artifacts=_mapping(payload.get("input_artifacts")),
        summary_extra={
            "source_availability": _text(summary.get("source_availability")),
            "unhealthy_source_count": unhealthy_count,
            "blocked_source_count": _int(summary.get("blocked_source_count")),
            "warning_source_count": _int(summary.get("warning_source_count")),
            "stale_source_count": _int(summary.get("stale_source_count")),
            "partial_source_count": _int(summary.get("partial_source_count")),
        },
    )


def build_recovery_owner_action_map_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any],
    blocker_triage_payload: Mapping[str, Any],
    report_index_warning_triage_payload: Mapping[str, Any],
    source_depth_audit_payload: Mapping[str, Any],
    input_artifacts: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    blockers = _records(blocker_triage_payload.get("blocker_triage"))
    warnings = _records(report_index_warning_triage_payload.get("warning_triage"))
    unhealthy_sources = _records(source_depth_audit_payload.get("unhealthy_sources"))
    action_groups = _owner_action_groups(blockers, warnings, unhealthy_sources)
    summary = _owner_action_summary(recovery_pack_payload, action_groups)
    status = OWNER_ACTIONS_REQUIRED if summary["open_action_count"] else OWNER_ACTIONS_CLEAR
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": RECOVERY_OWNER_ACTION_MAP_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "owner_action_map_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "action_map_only": True,
        "input_artifacts": dict(input_artifacts or {}),
        "summary": summary,
        "action_groups": action_groups,
        "recommended_order": _recommended_order(action_groups),
        "paper_shadow_resumption_preconditions": _paper_shadow_preconditions(blockers),
        "extended_shadow_forbidden_reasons": _extended_shadow_forbidden_reasons(blockers),
        "live_trading_forbidden": True,
        "reader_brief": _owner_action_reader_brief(summary, action_groups),
        "safety_boundary": _triage_safety_boundary(),
        "limitations": [
            "Owner action map is a checklist, not an owner decision.",
            "It does not resolve blockers, apply waivers, or rerun upstream sources.",
            "Live trading remains forbidden.",
        ],
        "next_action": summary["next_owner_action"],
    }


def validate_recovery_owner_action_map_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    action_groups = _mapping(payload.get("action_groups"))
    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == RECOVERY_OWNER_ACTION_MAP_REPORT_TYPE,
        f"report_type must be {RECOVERY_OWNER_ACTION_MAP_REPORT_TYPE}.",
        "regenerate_recovery_owner_action_map",
    )
    _append_check(
        checks,
        blocking_issues,
        "live_trading_forbidden",
        payload.get("live_trading_forbidden") is True,
        "Owner action map must keep live trading forbidden.",
        "restore_live_trading_forbidden_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_next_actions_present",
        all(
            _text(summary.get(field))
            for field in ("next_owner_action", "next_code_action", "next_data_action")
        ),
        "Owner action map must expose owner, code, and data next actions.",
        "restore_owner_action_map_next_actions",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocked_actions_visible",
        _int(summary.get("remain_blocked_action_count"))
        == len(_records(action_groups.get("actions_that_should_remain_blocked"))),
        "Remain-blocked action count must match action group items.",
        "restore_remain_blocked_action_group",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_owner_action_map_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Owner action map must preserve the read-only safety boundary.",
        "restore_recovery_triage_safety_boundary",
    )
    open_action_count = _int(summary.get("open_action_count"))
    return _validation_payload(
        report_type=RECOVERY_OWNER_ACTION_MAP_VALIDATION_REPORT_TYPE,
        as_of=_text(payload.get("as_of"), date.today().isoformat()),
        checks=checks,
        blocking_issues=blocking_issues,
        warning_issues=_visible_warning_issues(
            "recovery_owner_actions_required",
            open_action_count,
            "Recovery governance still has owner/code/data actions to complete.",
            _text(payload.get("next_action"), "review_recovery_owner_action_map"),
        ),
        input_artifacts=_mapping(payload.get("input_artifacts")),
        summary_extra={
            "open_action_count": open_action_count,
            "live_trading_forbidden": bool(payload.get("live_trading_forbidden")),
            "remain_blocked_action_count": _int(
                summary.get("remain_blocked_action_count")
            ),
        },
    )


def build_recovery_governance_rerun_triage_context(
    *,
    blocker_triage_payload: Mapping[str, Any] | None,
    report_index_warning_triage_payload: Mapping[str, Any] | None,
    source_depth_audit_payload: Mapping[str, Any] | None,
    owner_action_map_payload: Mapping[str, Any] | None,
    input_artifacts: Mapping[str, str],
) -> dict[str, Any]:
    blocker_summary = _mapping(
        (blocker_triage_payload or {}).get("summary")
        if blocker_triage_payload is not None
        else {}
    )
    warning_summary = _mapping(
        (report_index_warning_triage_payload or {}).get("summary")
        if report_index_warning_triage_payload is not None
        else {}
    )
    source_summary = _mapping(
        (source_depth_audit_payload or {}).get("summary")
        if source_depth_audit_payload is not None
        else {}
    )
    action_summary = _mapping(
        (owner_action_map_payload or {}).get("summary")
        if owner_action_map_payload is not None
        else {}
    )
    return {
        "context_status": "TRIAGE_CONTEXT_AVAILABLE",
        "input_artifacts": dict(input_artifacts),
        "summary": {
            "recovery_blocker_count": _int(
                blocker_summary.get("recovery_blocker_count")
            ),
            "report_index_unwaived_warning_count": _int(
                warning_summary.get("unwaived_warning_count")
            ),
            "unhealthy_source_count": _int(source_summary.get("unhealthy_source_count")),
            "open_action_count": _int(action_summary.get("open_action_count")),
            "next_owner_action": _text(action_summary.get("next_owner_action")),
            "next_code_action": _text(action_summary.get("next_code_action")),
            "next_data_action": _text(action_summary.get("next_data_action")),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _triage_safety_boundary(),
    }


def attach_triage_context_to_recovery_pack(
    recovery_pack_payload: Mapping[str, Any],
    triage_context: Mapping[str, Any],
) -> dict[str, Any]:
    payload = dict(recovery_pack_payload)
    input_artifacts = dict(_mapping(payload.get("input_artifacts")))
    input_artifacts.update(_mapping(triage_context.get("input_artifacts")))
    payload["input_artifacts"] = input_artifacts
    payload["triage_context"] = dict(triage_context)
    reader_brief = dict(_mapping(payload.get("reader_brief")))
    context_summary = _mapping(triage_context.get("summary"))
    reader_brief["triage_context"] = (
        "blockers="
        f"{_int(context_summary.get('recovery_blocker_count'))}; "
        "report_index_warnings="
        f"{_int(context_summary.get('report_index_unwaived_warning_count'))}; "
        f"unhealthy_sources={_int(context_summary.get('unhealthy_source_count'))}; "
        f"open_actions={_int(context_summary.get('open_action_count'))}"
    )
    payload["reader_brief"] = reader_brief
    return payload


def write_recovery_blocker_triage_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    return _write_json(payload, output_path)


def write_recovery_blocker_triage_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_recovery_blocker_triage_markdown(payload), output_path)


def write_recovery_blocker_triage_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_recovery_blocker_triage_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_validation_markdown(payload), output_path)


def write_report_index_warning_triage_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_report_index_warning_triage_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_report_index_warning_triage_markdown(payload), output_path)


def write_report_index_warning_triage_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_report_index_warning_triage_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_validation_markdown(payload), output_path)


def write_recovery_pack_source_depth_audit_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_recovery_pack_source_depth_audit_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_recovery_pack_source_depth_audit_markdown(payload), output_path)


def write_recovery_pack_source_depth_audit_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_recovery_pack_source_depth_audit_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_validation_markdown(payload), output_path)


def write_recovery_owner_action_map_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_recovery_owner_action_map_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_recovery_owner_action_map_markdown(payload), output_path)


def write_recovery_owner_action_map_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_recovery_owner_action_map_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_validation_markdown(payload), output_path)


def render_recovery_blocker_triage_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Recovery Blocker Triage - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- triage_status: {payload.get('triage_status')}",
        f"- recovery_blocker_count: {summary.get('recovery_blocker_count')}",
        f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
        (
            "- extended_shadow_remains_forbidden: "
            f"{summary.get('extended_shadow_remains_forbidden')}"
        ),
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- next_owner_action: {summary.get('next_owner_action')}",
        "",
        "## Blockers",
        "",
        "|source_id|category|field|status|related|action|normal|extended|live|",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for item in _records(payload.get("blocker_triage")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    item.get("source_id"),
                    item.get("blocker_category"),
                    item.get("exact_blocking_field"),
                    item.get("source_status"),
                    ",".join(str(v) for v in item.get("related_dimensions", [])),
                    item.get("required_artifact_or_owner_action"),
                    item.get("blocks_normal_paper_shadow"),
                    item.get("blocks_extended_shadow"),
                    item.get("live_trading_implication"),
                )
            )
            + "|"
        )
    lines.extend(_reader_brief_markdown_lines(payload))
    lines.extend(_safety_markdown_lines(payload))
    return "\n".join(lines)


def render_report_index_warning_triage_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Report Index Warning Triage - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- triage_status: {payload.get('triage_status')}",
        f"- unwaived_warning_count: {summary.get('unwaived_warning_count')}",
        f"- true_blocker_count: {summary.get('true_blocker_count')}",
        f"- explicit_waiver_recommendation_count: "
        f"{summary.get('explicit_waiver_recommendation_count')}",
        f"- silent_waiver_count: {summary.get('silent_waiver_count')}",
        "",
        "## Warnings",
        "",
        "|issue_id|report_id|classification|proposed_action|reason|",
        "|---|---|---|---|---|",
    ]
    for item in _records(payload.get("warning_triage")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    item.get("issue_id"),
                    item.get("report_id"),
                    item.get("warning_classification"),
                    item.get("proposed_action"),
                    item.get("reason"),
                )
            )
            + "|"
        )
    lines.extend(_reader_brief_markdown_lines(payload))
    lines.extend(_safety_markdown_lines(payload))
    return "\n".join(lines)


def render_recovery_pack_source_depth_audit_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Recovery Pack Source Depth Audit - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- source_depth_audit_status: {payload.get('source_depth_audit_status')}",
        f"- source_availability: {summary.get('source_availability')}",
        f"- unhealthy_source_count: {summary.get('unhealthy_source_count')}",
        f"- blocked_source_count: {summary.get('blocked_source_count')}",
        f"- warning_source_count: {summary.get('warning_source_count')}",
        f"- stale_source_count: {summary.get('stale_source_count')}",
        f"- partial_source_count: {summary.get('partial_source_count')}",
        "",
        "## Source Depth",
        "",
        "|source_id|artifact_id|source_date|status|freshness|health|next_action|",
        "|---|---|---|---|---|---|---|",
    ]
    for item in _records(payload.get("source_depth")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    item.get("source_id"),
                    item.get("source_artifact_id"),
                    item.get("source_date"),
                    item.get("source_status"),
                    item.get("source_freshness"),
                    item.get("health_status"),
                    item.get("next_required_action"),
                )
            )
            + "|"
        )
    lines.extend(_reader_brief_markdown_lines(payload))
    lines.extend(_safety_markdown_lines(payload))
    return "\n".join(lines)


def render_recovery_owner_action_map_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Recovery Owner Action Map - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- owner_action_map_status: {payload.get('owner_action_map_status')}",
        f"- next_owner_action: {summary.get('next_owner_action')}",
        f"- next_code_action: {summary.get('next_code_action')}",
        f"- next_data_action: {summary.get('next_data_action')}",
        f"- live_trading_forbidden: {payload.get('live_trading_forbidden')}",
        f"- open_action_count: {summary.get('open_action_count')}",
        "",
        "## Recommended Order",
        "",
    ]
    for item in _records(payload.get("recommended_order")):
        lines.append(
            f"- {item.get('step')}: {item.get('action')} ({item.get('reason')})"
        )
    lines.extend(["", "## Action Groups", ""])
    for group_name, items in _mapping(payload.get("action_groups")).items():
        lines.extend([f"### {group_name}", ""])
        records = _records(items)
        if not records:
            lines.append("- none")
        for item in records:
            lines.append(
                f"- {item.get('source_id', item.get('report_id'))}: "
                f"{item.get('recommended_action', item.get('proposed_action'))}"
            )
        lines.append("")
    lines.extend(_reader_brief_markdown_lines(payload))
    lines.extend(_safety_markdown_lines(payload))
    return "\n".join(lines)


def render_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# {_text(payload.get('report_type'), 'Validation')} - "
        f"{_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed_checks: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Checks",
        "",
        "|check_id|status|message|recommended_action|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    check.get("check_id"),
                    check.get("status"),
                    check.get("message"),
                    check.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.extend(_reader_brief_markdown_lines(payload))
    return "\n".join(lines)


def _blocker_triage_item(
    blocker: Mapping[str, Any],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_id = _text(blocker.get("source_id"))
    source = _mapping(source_by_id.get(source_id))
    policy = BLOCKER_POLICY.get(
        source_id,
        {
            "category": "governance_blocker",
            "related_dimensions": ("safety",),
            "required_artifact_or_owner_action": _text(
                blocker.get("recommended_action"),
                "review_recovery_governance_blocker",
            ),
        },
    )
    field = SOURCE_STATUS_FIELDS.get(source_id, ("source_status",))[0]
    required_action = _text(
        blocker.get("recommended_action"),
        _text(policy.get("required_artifact_or_owner_action")),
    )
    related_dimensions = list(policy.get("related_dimensions", ()))
    return {
        "issue_id": _text(blocker.get("issue_id")),
        "source_id": source_id,
        "source_report": _text(source.get("report_id")),
        "source_artifact_path": _text(blocker.get("artifact_path")),
        "blocker_category": _text(policy.get("category"), "governance_blocker"),
        "exact_blocking_field": f"{field}={_text(blocker.get('source_status'))}",
        "blocking_field_name": field,
        "source_status": _text(blocker.get("source_status")),
        "validation_status": _text(blocker.get("validation_status")),
        "related_dimensions": related_dimensions,
        "is_data_related": "data" in related_dimensions,
        "is_signal_related": "signal" in related_dimensions,
        "is_metric_related": "metric" in related_dimensions,
        "is_owner_related": "owner" in related_dimensions,
        "is_safety_related": "safety" in related_dimensions,
        "is_registry_related": "registry" in related_dimensions,
        "is_roadmap_related": "roadmap" in related_dimensions,
        "required_artifact_or_owner_action": _text(
            policy.get("required_artifact_or_owner_action"),
            required_action,
        ),
        "recommended_action": required_action,
        "blocks_normal_paper_shadow": True,
        "blocks_extended_shadow": True,
        "live_trading_implication": False,
        "live_trading_note": "Live trading remains forbidden regardless of this blocker.",
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocker_triage_summary(
    recovery_pack_payload: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pack_summary = _mapping(recovery_pack_payload.get("summary"))
    normal = _mapping(recovery_pack_payload.get("normal_paper_shadow_boundary"))
    extended = _mapping(recovery_pack_payload.get("extended_shadow_boundary"))
    live = _mapping(recovery_pack_payload.get("live_trading_boundary"))
    relation_counts = Counter(
        relation
        for blocker in blockers
        for relation in _records_as_text(blocker.get("related_dimensions"))
    )
    return {
        "recovery_governance_status": _text(
            recovery_pack_payload.get("recovery_governance_status")
        ),
        "source_recovery_blocker_count": _int(
            pack_summary.get("remaining_blocker_count")
        ),
        "recovery_blocker_count": len(blockers),
        "blocker_category_counts": dict(
            Counter(_text(blocker.get("blocker_category")) for blocker in blockers)
        ),
        "related_dimension_counts": dict(relation_counts),
        "normal_paper_shadow_may_resume": bool(normal.get("normal_paper_shadow_may_resume")),
        "extended_shadow_remains_forbidden": bool(
            extended.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(live.get("live_trading_remains_forbidden")),
        "top_remaining_blocker": _text(pack_summary.get("top_remaining_blocker")),
        "next_owner_action": _text(pack_summary.get("next_owner_action")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocker_triage_reader_brief(
    summary: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Recovery blocker triage found "
            f"{_int(summary.get('recovery_blocker_count'))} blockers; "
            "normal paper-shadow, extended shadow, and live trading boundaries remain closed."
        ),
        "key_result": _text(summary.get("recovery_governance_status")),
        "blocking_issues": _issue_list(blockers, "source_id", "source_status"),
        "warnings": "none",
        "safety_boundary": (
            "Triage only; no blocker resolution, no waiver, no broker/order, "
            "production_effect=none."
        ),
        "next_action": _text(summary.get("next_owner_action"), "review_blocker_triage"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _report_index_warning_items(report_index_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    reports = _records(report_index_payload.get("reports"))
    issue_ids = [
        _text(issue_id)
        for issue_id in _records_as_text(
            _mapping(report_index_payload.get("visibility_audit")).get("unwaived_issue_ids")
        )
        if _text(issue_id)
    ]
    if not issue_ids:
        issue_ids = [_text(warning).split(":", 1)[0] for warning in _records_as_text(
            report_index_payload.get("warnings")
        )]
    entry_by_issue = {
        _text(_mapping(report.get("visibility_issue")).get("issue_id")): report
        for report in reports
    }
    items: list[dict[str, Any]] = []
    for issue_id in issue_ids:
        report = _mapping(entry_by_issue.get(issue_id))
        issue = _mapping(report.get("visibility_issue"))
        if not report:
            issue = {"issue_id": issue_id, "warning_text": issue_id, "issue_status": "UNKNOWN"}
        classification = _classify_report_index_warning(report, issue)
        proposed_action = _propose_report_index_warning_action(report, classification)
        waiver_requirements = _waiver_requirements(
            report,
            issue,
            enabled=proposed_action == "create_explicit_expiring_waiver",
        )
        items.append(
            {
                "issue_id": issue_id,
                "report_id": _text(report.get("report_id")),
                "title": _text(report.get("title")),
                "warning_text": _text(issue.get("warning_text"), issue_id),
                "issue_status": _text(issue.get("issue_status"), "UNKNOWN"),
                "warning_classification": classification,
                "proposed_action": proposed_action,
                "reason": _warning_triage_reason(report, issue, classification),
                "owner": _text(report.get("owner"), "system"),
                "cadence": _text(report.get("cadence")),
                "group": _text(report.get("group")),
                "required_for_daily_reading": bool(report.get("required_for_daily_reading")),
                "artifact_status": _text(report.get("artifact_status")),
                "freshness_status": _text(report.get("freshness_status")),
                "age_days": _int(report.get("age_days")),
                "freshness_sla_days": _int(report.get("freshness_sla_days")),
                "latest_artifact_path": _text(report.get("latest_artifact_path")),
                "owner_action": _text(report.get("owner_action")),
                "waiver_action": "not_applied",
                "waiver_requirements": waiver_requirements,
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return items


def _report_index_warning_summary(
    report_index_payload: Mapping[str, Any],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    classification_counts = Counter(
        _text(warning.get("warning_classification")) for warning in warnings
    )
    proposed_action_counts = Counter(_text(warning.get("proposed_action")) for warning in warnings)
    return {
        "report_index_status": _text(report_index_payload.get("status")),
        "report_count": _int(_mapping(report_index_payload.get("summary")).get("report_count")),
        "source_unwaived_warning_count": _int(
            _mapping(report_index_payload.get("summary")).get("unwaived_warning_count")
        ),
        "unwaived_warning_count": len(warnings),
        "true_blocker_count": classification_counts.get("true_blocker", 0),
        "governance_warning_count": classification_counts.get("governance_warning", 0),
        "stale_artifact_warning_count": classification_counts.get(
            "stale_artifact_warning",
            0,
        ),
        "missing_metadata_warning_count": classification_counts.get(
            "missing_metadata_warning",
            0,
        ),
        "legacy_warning_candidate_count": classification_counts.get(
            "legacy_warning_candidate",
            0,
        ),
        "documentation_inconsistency_count": classification_counts.get(
            "documentation_inconsistency",
            0,
        ),
        "classification_counts": dict(classification_counts),
        "proposed_action_counts": dict(proposed_action_counts),
        "explicit_waiver_recommendation_count": proposed_action_counts.get(
            "create_explicit_expiring_waiver",
            0,
        ),
        "silent_waiver_count": 0,
        "next_action": _next_report_index_warning_action(warnings),
        "production_effect": PRODUCTION_EFFECT,
    }


def _report_index_warning_reader_brief(
    summary: Mapping[str, Any],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Report index warning triage found "
            f"{_int(summary.get('unwaived_warning_count'))} unwaived warnings; "
            "no warning was waived by this report."
        ),
        "key_result": _text(summary.get("report_index_status")),
        "blocking_issues": _issue_list(
            [w for w in warnings if w.get("warning_classification") == "true_blocker"],
            "report_id",
            "issue_status",
        ),
        "warnings": _issue_list(warnings, "report_id", "warning_classification"),
        "safety_boundary": (
            "Warning triage only; no waiver creation, no artifact regeneration, "
            "production_effect=none."
        ),
        "next_action": _text(summary.get("next_action"), "review_report_index_warning_triage"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _classify_report_index_warning(
    report: Mapping[str, Any],
    issue: Mapping[str, Any],
) -> str:
    if bool(report.get("required_for_daily_reading")):
        return "true_blocker"
    if not _text(report.get("latest_artifact_path")) or _text(report.get("artifact_status")) in {
        "",
        "UNKNOWN",
    }:
        return "missing_metadata_warning"
    report_id = _text(report.get("report_id"))
    group = _text(report.get("group"))
    if group == "governance":
        return "governance_warning"
    if report_id.startswith("docs_") or "documentation" in report_id:
        return "documentation_inconsistency"
    if _text(issue.get("issue_status")) == "MISSING" or _text(report.get("cadence")) == "legacy":
        return "legacy_warning_candidate"
    return "stale_artifact_warning"


def _propose_report_index_warning_action(
    report: Mapping[str, Any],
    classification: str,
) -> str:
    if classification in {
        "true_blocker",
        "stale_artifact_warning",
        "governance_warning",
        "missing_metadata_warning",
        "documentation_inconsistency",
    }:
        return "fix_now"
    if classification == "legacy_warning_candidate":
        return "archive_legacy_artifact"
    return "leave_blocking"


def _warning_triage_reason(
    report: Mapping[str, Any],
    issue: Mapping[str, Any],
    classification: str,
) -> str:
    if classification == "true_blocker":
        return "Required daily reading artifact is stale and must stay visible."
    if classification == "governance_warning":
        return "Governance artifact is stale and should be regenerated before review."
    if classification == "missing_metadata_warning":
        return "Report index entry lacks enough artifact metadata for safe interpretation."
    if classification == "legacy_warning_candidate":
        return "Optional legacy artifact should be archived or explicitly waived with expiry."
    if classification == "documentation_inconsistency":
        return "Documentation or registry metadata needs correction before waiver."
    return (
        f"{_text(report.get('title'), _text(report.get('report_id')))} is "
        f"{_text(issue.get('issue_status'), 'WARNING')} relative to its SLA."
    )


def _waiver_requirements(
    report: Mapping[str, Any],
    issue: Mapping[str, Any],
    *,
    enabled: bool,
) -> dict[str, Any]:
    if not enabled:
        return {
            "required": False,
            "reason": "",
            "owner": "",
            "expires_at": "",
            "linked_task_id": "",
        }
    return {
        "required": True,
        "reason": _warning_triage_reason(report, issue, "legacy_warning_candidate"),
        "owner": _text(report.get("owner"), "system"),
        "expires_at": "",
        "linked_task_id": "TRADING-402_REPORT_INDEX_UNWAIVED_WARNING_TRIAGE",
    }


def _next_report_index_warning_action(warnings: Sequence[Mapping[str, Any]]) -> str:
    if not warnings:
        return "no_unwaived_report_index_warning_action"
    for warning in warnings:
        if _text(warning.get("warning_classification")) == "true_blocker":
            return _text(warning.get("owner_action"), "regenerate_required_daily_artifact")
    return _text(warnings[0].get("owner_action"), "review_unwaived_report_index_warnings")


def _source_depth_item(
    source: Mapping[str, Any],
    report_index_payload: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    source_path = _path_or_none(_text(source.get("source_payload_path")))
    validation_path = _path_or_none(_text(source.get("validation_payload_path")))
    source_payload = _read_optional_json_mapping(source_path) if source_path else {}
    validation_payload = _read_optional_json_mapping(validation_path) if validation_path else {}
    report_entry = _report_index_entry(report_index_payload, _text(source.get("report_id")))
    freshness = _text(report_entry.get("freshness_status"), "UNKNOWN")
    visibility_issue = _mapping(report_entry.get("visibility_issue"))
    blocking_state = _text(source.get("conclusion_status")) == "BLOCKING"
    warning_state = (
        _text(source.get("conclusion_status")) == "WARNING"
        or "WARNING" in _text(source.get("validation_status")).upper()
    )
    stale_state = freshness == "STALE" or _text(visibility_issue.get("issue_status")) == "STALE"
    partial_state = _source_partial(source, source_payload, validation_payload)
    health_status = _source_health_status(
        blocking_state=blocking_state,
        warning_state=warning_state,
        stale_state=stale_state,
        partial_state=partial_state,
    )
    return {
        "source_id": _text(source.get("source_id")),
        "source_report": _text(source.get("report_id")),
        "source_artifact_id": _source_artifact_id(source_payload, source_path),
        "source_date": _source_date(source_payload, report_entry, source_path),
        "source_status": _text(source.get("source_status")),
        "validation_status": _text(source.get("validation_status")),
        "source_freshness": freshness,
        "age_days": _int(report_entry.get("age_days")),
        "freshness_sla_days": _int(report_entry.get("freshness_sla_days")),
        "blocking_state": blocking_state,
        "warning_state": warning_state,
        "stale_state": stale_state,
        "partial_state": partial_state,
        "partial_or_complete": "partial" if partial_state else "complete",
        "health_status": health_status,
        "source_payload_path": "" if source_path is None else str(source_path),
        "validation_payload_path": "" if validation_path is None else str(validation_path),
        "next_required_action": _text(
            source.get("next_action"),
            "review_recovery_pack_source_depth",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_depth_summary(
    recovery_pack_payload: Mapping[str, Any],
    source_audits: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    health_counts = Counter(_text(source.get("health_status")) for source in source_audits)
    available_count = len(
        [
            source
            for source in _records(recovery_pack_payload.get("source_reports"))
            if _text(source.get("availability")) == "AVAILABLE"
        ]
    )
    unhealthy = [
        source for source in source_audits if _text(source.get("health_status")) != "HEALTHY"
    ]
    return {
        "recovery_governance_status": _text(
            recovery_pack_payload.get("recovery_governance_status")
        ),
        "source_report_count": len(source_audits),
        "available_source_count": available_count,
        "source_availability": f"{available_count}/{len(source_audits)}",
        "source_health_summary": dict(health_counts),
        "healthy_source_count": health_counts.get("HEALTHY", 0),
        "warning_source_count": health_counts.get("WARNING", 0),
        "blocked_source_count": health_counts.get("BLOCKED", 0),
        "stale_source_count": health_counts.get("STALE", 0),
        "partial_source_count": health_counts.get("PARTIAL", 0),
        "unhealthy_source_count": len(unhealthy),
        "next_action": (
            _text(unhealthy[0].get("next_required_action"))
            if unhealthy
            else "sources_healthy_continue_owner_review"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_depth_status(summary: Mapping[str, Any]) -> str:
    if _int(summary.get("blocked_source_count")) > 0:
        return RECOVERY_SOURCE_BLOCKED
    if _int(summary.get("unhealthy_source_count")) > 0:
        return RECOVERY_SOURCE_HEALTHY_WITH_WARNINGS
    return RECOVERY_SOURCE_HEALTHY


def _source_depth_reader_brief(
    summary: Mapping[str, Any],
    unhealthy_sources: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            "Recovery source depth audit reports source_availability="
            f"{_text(summary.get('source_availability'))}; unhealthy_sources="
            f"{_int(summary.get('unhealthy_source_count'))}."
        ),
        "key_result": _text(summary.get("recovery_governance_status")),
        "blocking_issues": _issue_list(
            [s for s in unhealthy_sources if s.get("health_status") == "BLOCKED"],
            "source_id",
            "source_status",
        ),
        "warnings": _issue_list(unhealthy_sources, "source_id", "health_status"),
        "safety_boundary": (
            "Source audit only; no source regeneration, no data refresh, "
            "production_effect=none."
        ),
        "next_action": _text(summary.get("next_action"), "review_source_depth_audit"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_partial(
    source: Mapping[str, Any],
    source_payload: Mapping[str, Any],
    validation_payload: Mapping[str, Any],
) -> bool:
    if _text(source.get("availability")) != "AVAILABLE":
        return True
    if _text(source.get("validation_availability")) != "AVAILABLE":
        return True
    statuses = " ".join(
        [
            _text(source.get("source_status")),
            _text(source.get("validation_status")),
            _text(source_payload.get("status")),
            _text(validation_payload.get("status")),
        ]
    ).upper()
    return "PARTIAL" in statuses or "MISSING" in statuses


def _source_health_status(
    *,
    blocking_state: bool,
    warning_state: bool,
    stale_state: bool,
    partial_state: bool,
) -> str:
    if blocking_state:
        return "BLOCKED"
    if partial_state:
        return "PARTIAL"
    if stale_state:
        return "STALE"
    if warning_state:
        return "WARNING"
    return "HEALTHY"


def _source_artifact_id(payload: Mapping[str, Any], path: Path | None) -> str:
    for key, value in payload.items():
        if key.endswith("_id") and _text(value):
            return _text(value)
    summary = _mapping(payload.get("summary"))
    for key, value in summary.items():
        if key.endswith("_id") and _text(value):
            return _text(value)
    if path is not None:
        return path.parent.name if path.parent.name else path.stem
    return "UNKNOWN"


def _source_date(
    payload: Mapping[str, Any],
    report_entry: Mapping[str, Any],
    path: Path | None,
) -> str:
    for key in ("as_of", "date", "run_date", "target_as_of"):
        value = _text(payload.get(key))
        if value:
            return value[:10]
    if _text(report_entry.get("artifact_date")):
        return _text(report_entry.get("artifact_date"))
    if path is not None:
        return _date_from_name(path.name)
    return ""


def _owner_action_groups(
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    unhealthy_sources: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    artifact_actions = [
        _warning_action_item(warning)
        for warning in warnings
        if _text(warning.get("proposed_action")) == "fix_now"
    ]
    waiver_actions = [
        _warning_action_item(warning)
        for warning in warnings
        if _text(warning.get("proposed_action")) == "create_explicit_expiring_waiver"
    ]
    data_actions = [
        _source_action_item(source)
        for source in unhealthy_sources
        if _source_requires_data_action(source)
    ]
    code_data_actions = [
        *artifact_actions,
        *data_actions,
    ]
    owner_actions = [
        _blocker_action_item(blocker)
        for blocker in blockers
        if bool(blocker.get("is_owner_related")) or bool(blocker.get("is_roadmap_related"))
    ]
    remain_blocked = [_blocker_action_item(blocker) for blocker in blockers]
    return {
        "actions_requiring_code_data_rerun": code_data_actions,
        "actions_requiring_owner_review": owner_actions,
        "actions_requiring_explicit_waiver": waiver_actions,
        "actions_requiring_artifact_regeneration": artifact_actions,
        "actions_that_should_remain_blocked": remain_blocked,
    }


def _owner_action_summary(
    recovery_pack_payload: Mapping[str, Any],
    action_groups: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    pack_summary = _mapping(recovery_pack_payload.get("summary"))
    next_owner = _text(
        _mapping(recovery_pack_payload.get("next_owner_action")).get("action"),
        _text(pack_summary.get("next_owner_action")),
    )
    code_data = _records(action_groups.get("actions_requiring_code_data_rerun"))
    data_actions = [action for action in code_data if _text(action.get("action_type")) == "data"]
    remain_blocked = _records(action_groups.get("actions_that_should_remain_blocked"))
    open_count = sum(len(_records(items)) for items in action_groups.values())
    return {
        "recovery_governance_status": _text(
            recovery_pack_payload.get("recovery_governance_status")
        ),
        "next_owner_action": next_owner or "review_recovery_governance_pack",
        "next_code_action": (
            _text(code_data[0].get("recommended_action"))
            if code_data
            else "no_code_action_until_triage_review"
        ),
        "next_data_action": (
            _text(data_actions[0].get("recommended_action"))
            if data_actions
            else "no_data_action_until_source_refresh_is_requested"
        ),
        "open_action_count": open_count,
        "remain_blocked_action_count": len(remain_blocked),
        "paper_shadow_normal_may_resume": bool(
            pack_summary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            pack_summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_forbidden": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _recommended_order(
    action_groups: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    return [
        {
            "step": 1,
            "action": "review_unwaived_report_index_warnings",
            "reason": (
                "Refresh or classify stale daily/governance artifacts before "
                "recovery review."
            ),
            "action_count": len(
                _records(action_groups.get("actions_requiring_artifact_regeneration"))
            ),
        },
        {
            "step": 2,
            "action": "review_unhealthy_recovery_sources",
            "reason": "Blocked or warning source conclusions drive the recovery governance status.",
            "action_count": len(_records(action_groups.get("actions_that_should_remain_blocked"))),
        },
        {
            "step": 3,
            "action": "record_owner_review_only_after_blockers_clear",
            "reason": "Owner action cannot resume normal paper-shadow while blockers remain.",
            "action_count": len(_records(action_groups.get("actions_requiring_owner_review"))),
        },
        {
            "step": 4,
            "action": "keep_extended_shadow_and_live_trading_forbidden",
            "reason": (
                "Extended shadow has separate protocol blockers and live trading "
                "is out of scope."
            ),
            "action_count": 1,
        },
    ]


def _paper_shadow_preconditions(
    blockers: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "precondition_id": "all_recovery_blockers_resolved",
            "status": "BLOCKED" if blockers else "PASS",
            "required_action": "resolve every recovery blocker without silent waiver",
            "blocking_source_count": len(blockers),
        },
        {
            "precondition_id": "owner_continue_normal_shadow_recorded",
            "status": "BLOCKED",
            "required_action": (
                "record explicit continue_normal_shadow owner review after blockers clear"
            ),
            "blocking_source_count": len(
                [b for b in blockers if bool(b.get("is_owner_related"))]
            ),
        },
        {
            "precondition_id": "normal_resumption_gate_non_blocked",
            "status": "BLOCKED",
            "required_action": (
                "rerun normal paper-shadow resumption gate after source blockers clear"
            ),
            "blocking_source_count": len(
                [b for b in blockers if b.get("source_id") == "normal_paper_shadow_resumption_gate"]
            ),
        },
    ]


def _extended_shadow_forbidden_reasons(
    blockers: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    related = [
        blocker
        for blocker in blockers
        if blocker.get("source_id") in {"observation_clock", "extended_shadow_protocol"}
        or bool(blocker.get("is_safety_related"))
    ]
    if not related and blockers:
        related = list(blockers)
    return [
        {
            "source_id": _text(blocker.get("source_id")),
            "source_status": _text(blocker.get("source_status")),
            "reason": _text(blocker.get("recommended_action")),
            "production_effect": PRODUCTION_EFFECT,
        }
        for blocker in related
    ]


def _owner_action_reader_brief(
    summary: Mapping[str, Any],
    action_groups: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    return {
        "summary": (
            f"Recovery owner action map has {_int(summary.get('open_action_count'))} "
            "open actions; live trading remains forbidden."
        ),
        "key_result": _text(summary.get("recovery_governance_status")),
        "blocking_issues": _issue_list(
            _records(action_groups.get("actions_that_should_remain_blocked")),
            "source_id",
            "source_status",
        ),
        "warnings": _issue_list(
            _records(action_groups.get("actions_requiring_artifact_regeneration")),
            "report_id",
            "issue_status",
        ),
        "safety_boundary": (
            "Checklist only; no owner decision write, no waiver, no broker/order, "
            "production_effect=none."
        ),
        "next_action": _text(summary.get("next_owner_action"), "review_owner_action_map"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocker_action_item(blocker: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": _text(blocker.get("source_id")),
        "source_status": _text(blocker.get("source_status")),
        "action_type": "owner" if bool(blocker.get("is_owner_related")) else "blocker",
        "recommended_action": _text(blocker.get("recommended_action")),
        "reason": _text(blocker.get("exact_blocking_field")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _warning_action_item(warning: Mapping[str, Any]) -> dict[str, Any]:
    action_type = "data" if _text(warning.get("group")) == "data_quality" else "artifact"
    return {
        "report_id": _text(warning.get("report_id")),
        "issue_id": _text(warning.get("issue_id")),
        "issue_status": _text(warning.get("issue_status")),
        "action_type": action_type,
        "proposed_action": _text(warning.get("proposed_action")),
        "recommended_action": _text(warning.get("owner_action"), "regenerate_stale_artifact"),
        "reason": _text(warning.get("reason")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_action_item(source: Mapping[str, Any]) -> dict[str, Any]:
    action_type = "data" if _text(source.get("source_id")) in {
        "decision_snapshot_lifecycle",
        "signal_input_restoration",
        "signal_completeness_recovery",
    } else "artifact"
    return {
        "source_id": _text(source.get("source_id")),
        "source_status": _text(source.get("source_status")),
        "health_status": _text(source.get("health_status")),
        "action_type": action_type,
        "recommended_action": _text(source.get("next_required_action")),
        "reason": _text(source.get("health_status")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _source_requires_data_action(source: Mapping[str, Any]) -> bool:
    return _text(source.get("source_id")) in {
        "decision_snapshot_lifecycle",
        "signal_input_restoration",
        "signal_completeness_recovery",
    } or _text(source.get("health_status")) in {"STALE", "PARTIAL"}


def _validation_payload(
    *,
    report_type: str,
    as_of: str,
    checks: Sequence[Mapping[str, Any]],
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
    input_artifacts: Mapping[str, Any],
    summary_extra: Mapping[str, Any],
) -> dict[str, Any]:
    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    summary = {
        "check_count": len(checks),
        "failed_check_count": len(
            [check for check in checks if _text(check.get("status")) == FAIL_STATUS]
        ),
        "warning_check_count": len(warning_issues),
        **dict(summary_extra),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "as_of": as_of,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "summary": summary,
        "checks": list(checks),
        "blocking_issues": list(blocking_issues),
        "warning_issues": list(warning_issues),
        "input_artifacts": dict(input_artifacts),
        "reader_brief": {
            "summary": (
                f"{report_type} is {validation_status}; "
                f"failed_checks={summary['failed_check_count']}."
            ),
            "key_result": validation_status,
            "blocking_issues": _issue_list(blocking_issues, "issue_id", "message"),
            "warnings": _issue_list(warning_issues, "issue_id", "message"),
            "safety_boundary": "read-only recovery triage validation; production_effect=none",
            "next_action": (
                "repair_recovery_triage_report"
                if validation_status == FAIL_STATUS
                else "review_recovery_triage_findings"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _triage_safety_boundary(),
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": PASS_STATUS if passed else FAIL_STATUS,
            "message": message,
            "recommended_action": recommended_action,
        }
    )
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _visible_warning_issues(
    issue_id: str,
    count: int,
    message: str,
    recommended_action: str,
) -> list[dict[str, Any]]:
    if count <= 0:
        return []
    return [
        {
            "issue_id": issue_id,
            "message": message,
            "recommended_action": recommended_action,
            "count": count,
        }
    ]


def _blocker_item_complete(item: Mapping[str, Any]) -> bool:
    required = (
        "source_id",
        "source_report",
        "blocker_category",
        "exact_blocking_field",
        "related_dimensions",
        "required_artifact_or_owner_action",
        "blocks_normal_paper_shadow",
        "blocks_extended_shadow",
        "live_trading_implication",
    )
    return all(key in item and item.get(key) not in (None, "") for key in required)


def _waiver_recommendation_complete(item: Mapping[str, Any]) -> bool:
    if _text(item.get("proposed_action")) != "create_explicit_expiring_waiver":
        return True
    requirements = _mapping(item.get("waiver_requirements"))
    return all(
        _text(requirements.get(field))
        for field in ("reason", "owner", "expires_at", "linked_task_id")
    )


def _source_depth_item_complete(item: Mapping[str, Any]) -> bool:
    required = (
        "source_id",
        "source_artifact_id",
        "source_date",
        "source_status",
        "source_freshness",
        "blocking_state",
        "warning_state",
        "partial_or_complete",
        "health_status",
        "next_required_action",
    )
    return all(key in item and item.get(key) not in (None, "") for key in required)


def _reader_brief_complete(value: Any) -> bool:
    reader_brief = _mapping(value)
    return all(bool(_text(reader_brief.get(field))) for field in CORE_READER_BRIEF_FIELDS)


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("does_not_run_upstream_commands") is True
        and safety.get("does_not_refresh_data") is True
        and safety.get("does_not_generate_missing_artifacts") is True
        and safety.get("does_not_write_owner_decision") is True
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("live_trading_allowed") is False
    )


def _triage_safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_recovery_governance_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_write_owner_decision": True,
        "does_not_modify_strategy_logic": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "automatic_owner_approval": False,
        "automatic_candidate_promotion": False,
        "normal_paper_shadow_auto_resume": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
    }


def _pack_report_index_path(payload: Mapping[str, Any], project_root: Path) -> Path | None:
    raw_path = _text(_mapping(payload.get("input_artifacts")).get("report_index"))
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return project_root / path


def _path_or_none(value: str) -> Path | None:
    if not value:
        return None
    return Path(value)


def _date_from_name(name: str) -> str:
    parts = name.replace("_", "-").split("-")
    for index in range(len(parts) - 2):
        candidate = "-".join(parts[index : index + 3])[:10]
        if len(candidate) == 10 and candidate[4] == "-" and candidate[7] == "-":
            return candidate
    return ""


def _records_as_text(value: Any) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_text(item) for item in value]
    if value is None:
        return []
    return [_text(value)]


def _issue_list(
    records: Sequence[Mapping[str, Any]],
    key_field: str,
    value_field: str,
) -> str:
    if not records:
        return "none"
    return "; ".join(
        f"{_text(record.get(key_field))}:{_text(record.get(value_field))}"
        for record in records[:5]
    )


def _reader_brief_markdown_lines(payload: Mapping[str, Any]) -> list[str]:
    reader_brief = _mapping(payload.get("reader_brief"))
    return [
        "",
        "## Reader Brief",
        "",
        f"- summary: {reader_brief.get('summary')}",
        f"- key_result: {reader_brief.get('key_result')}",
        f"- blocking_issues: {reader_brief.get('blocking_issues')}",
        f"- warnings: {reader_brief.get('warnings')}",
        f"- next_action: {reader_brief.get('next_action')}",
    ]


def _safety_markdown_lines(payload: Mapping[str, Any]) -> list[str]:
    return [
        "",
        "## Safety Boundary",
        "",
        "- read-only recovery triage.",
        f"- production_effect: {payload.get('production_effect')}",
        "- no upstream rerun, no data refresh, no owner decision write, no official target, "
        "no broker, no order ticket.",
        "",
    ]


def _write_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def _write_text(text: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path


__all__ = [
    "PASS_STATUS",
    "PASS_WITH_WARNINGS_STATUS",
    "build_recovery_blocker_triage_payload",
    "build_recovery_governance_rerun_triage_context",
    "build_recovery_owner_action_map_payload",
    "build_recovery_pack_source_depth_audit_payload",
    "build_report_index_warning_triage_payload",
    "attach_triage_context_to_recovery_pack",
    "default_recovery_blocker_triage_json_path",
    "default_recovery_blocker_triage_markdown_path",
    "default_recovery_blocker_triage_validation_json_path",
    "default_recovery_blocker_triage_validation_markdown_path",
    "default_recovery_owner_action_map_json_path",
    "default_recovery_owner_action_map_markdown_path",
    "default_recovery_owner_action_map_validation_json_path",
    "default_recovery_owner_action_map_validation_markdown_path",
    "default_recovery_pack_source_depth_audit_json_path",
    "default_recovery_pack_source_depth_audit_markdown_path",
    "default_recovery_pack_source_depth_audit_validation_json_path",
    "default_recovery_pack_source_depth_audit_validation_markdown_path",
    "default_report_index_warning_triage_json_path",
    "default_report_index_warning_triage_markdown_path",
    "default_report_index_warning_triage_validation_json_path",
    "default_report_index_warning_triage_validation_markdown_path",
    "latest_recovery_blocker_triage_json_path",
    "latest_recovery_owner_action_map_json_path",
    "latest_recovery_pack_source_depth_audit_json_path",
    "latest_report_index_warning_triage_json_path",
    "validate_recovery_blocker_triage_payload",
    "validate_recovery_owner_action_map_payload",
    "validate_recovery_pack_source_depth_audit_payload",
    "validate_report_index_warning_triage_payload",
    "write_recovery_blocker_triage_json",
    "write_recovery_blocker_triage_markdown",
    "write_recovery_blocker_triage_validation_json",
    "write_recovery_blocker_triage_validation_markdown",
    "write_recovery_owner_action_map_json",
    "write_recovery_owner_action_map_markdown",
    "write_recovery_owner_action_map_validation_json",
    "write_recovery_owner_action_map_validation_markdown",
    "write_recovery_pack_source_depth_audit_json",
    "write_recovery_pack_source_depth_audit_markdown",
    "write_recovery_pack_source_depth_audit_validation_json",
    "write_recovery_pack_source_depth_audit_validation_markdown",
    "write_report_index_warning_triage_json",
    "write_report_index_warning_triage_markdown",
    "write_report_index_warning_triage_validation_json",
    "write_report_index_warning_triage_validation_markdown",
]
