from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.recovery_triage import (
    latest_recovery_blocker_triage_json_path,
    latest_recovery_owner_action_map_json_path,
    latest_recovery_pack_source_depth_audit_json_path,
    latest_report_index_warning_triage_json_path,
)
from ai_trading_system.reports.research_governance_recovery_pack import (
    latest_research_governance_recovery_pack_json_path,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _records,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "remaining_blocker_resolution_ledger"
VALIDATION_REPORT_TYPE = "remaining_blocker_resolution_ledger_validation"

LEDGER_READY = "LEDGER_READY"
LEDGER_READY_WITH_WARNINGS = "LEDGER_READY_WITH_WARNINGS"
LEDGER_EMPTY = "LEDGER_EMPTY"

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

CORE_READER_BRIEF_FIELDS = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)


def default_remaining_blocker_resolution_ledger_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"remaining_blocker_resolution_ledger_{as_of.isoformat()}.json"


def default_remaining_blocker_resolution_ledger_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"remaining_blocker_resolution_ledger_{as_of.isoformat()}.md"


def default_remaining_blocker_resolution_ledger_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"remaining_blocker_resolution_ledger_validation_{as_of.isoformat()}.json"


def default_remaining_blocker_resolution_ledger_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"remaining_blocker_resolution_ledger_validation_{as_of.isoformat()}.md"


def latest_remaining_blocker_resolution_ledger_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "remaining_blocker_resolution_ledger_", ".json")


def build_remaining_blocker_resolution_ledger_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any] | None = None,
    recovery_pack_path: Path | None = None,
    blocker_triage_payload: Mapping[str, Any] | None = None,
    blocker_triage_path: Path | None = None,
    warning_triage_payload: Mapping[str, Any] | None = None,
    warning_triage_path: Path | None = None,
    source_depth_audit_payload: Mapping[str, Any] | None = None,
    source_depth_audit_path: Path | None = None,
    owner_action_map_payload: Mapping[str, Any] | None = None,
    owner_action_map_path: Path | None = None,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    recovery_pack_path, recovery_pack_payload = _payload_or_latest(
        recovery_pack_payload,
        recovery_pack_path,
        latest_research_governance_recovery_pack_json_path,
        reports_dir,
        "research_governance_recovery_pack",
    )
    blocker_triage_path, blocker_triage_payload = _payload_or_latest(
        blocker_triage_payload,
        blocker_triage_path,
        latest_recovery_blocker_triage_json_path,
        reports_dir,
        "recovery_blocker_triage",
    )
    warning_triage_path, warning_triage_payload = _payload_or_latest(
        warning_triage_payload,
        warning_triage_path,
        latest_report_index_warning_triage_json_path,
        reports_dir,
        "report_index_warning_triage",
    )
    source_depth_audit_path, source_depth_audit_payload = _payload_or_latest(
        source_depth_audit_payload,
        source_depth_audit_path,
        latest_recovery_pack_source_depth_audit_json_path,
        reports_dir,
        "recovery_pack_source_depth_audit",
    )
    owner_action_map_path, owner_action_map_payload = _payload_or_latest(
        owner_action_map_payload,
        owner_action_map_path,
        latest_recovery_owner_action_map_json_path,
        reports_dir,
        "recovery_owner_action_map",
    )

    source_depth_by_id = {
        _text(source.get("source_id")): source
        for source in _records(source_depth_audit_payload.get("source_depth"))
        + _records(source_depth_audit_payload.get("unhealthy_sources"))
    }
    owner_actions = _owner_action_lookup(owner_action_map_payload)
    blockers = [
        _blocker_ledger_row(blocker, source_depth_by_id, owner_actions)
        for blocker in _records(blocker_triage_payload.get("blocker_triage"))
    ]
    warning_triage_by_report = {
        _text(warning.get("report_id")): warning
        for warning in _records(warning_triage_payload.get("warning_triage"))
    }
    pack_warnings = _records(recovery_pack_payload.get("remaining_warnings"))
    warnings = (
        [
            _recovery_pack_warning_ledger_row(warning, warning_triage_by_report)
            for warning in pack_warnings
        ]
        if pack_warnings
        else [
            _warning_ledger_row(warning)
            for warning in _records(warning_triage_payload.get("warning_triage"))
        ]
    )
    summary = _summary(recovery_pack_payload, blockers, warnings)
    status = LEDGER_EMPTY
    if blockers or warnings:
        status = LEDGER_READY_WITH_WARNINGS if warnings else LEDGER_READY
    reader_brief = _reader_brief(summary, blockers, warnings)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "ledger_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "ledger_only": True,
        "input_artifacts": {
            "research_governance_recovery_pack": _path_text(recovery_pack_path),
            "recovery_blocker_triage": _path_text(blocker_triage_path),
            "report_index_warning_triage": _path_text(warning_triage_path),
            "recovery_pack_source_depth_audit": _path_text(source_depth_audit_path),
            "recovery_owner_action_map": _path_text(owner_action_map_path),
        },
        "summary": summary,
        "blocker_resolution_ledger": blockers,
        "warning_resolution_ledger": warnings,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "This ledger prioritizes resolution work but does not resolve blockers.",
            "It does not run upstream commands, refresh data, create waivers, or write decisions.",
            "Extended shadow and live trading remain forbidden.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_remaining_blocker_resolution_ledger_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    blockers = _records(payload.get("blocker_resolution_ledger"))
    warnings = _records(payload.get("warning_resolution_ledger"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_remaining_blocker_resolution_ledger",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_count_matches",
        _int(summary.get("blocker_count")) == len(blockers),
        "blocker_count must match blocker ledger rows.",
        "regenerate_remaining_blocker_resolution_ledger",
    )
    _append_check(
        checks,
        blocking_issues,
        "warning_count_matches",
        _int(summary.get("warning_count")) == len(warnings),
        "warning_count must match warning ledger rows.",
        "regenerate_remaining_blocker_resolution_ledger",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_required_fields",
        all(_blocker_row_complete(row) for row in blockers),
        (
            "Every blocker ledger row must expose source, dependency, root cause, "
            "actions, and boundaries."
        ),
        "repair_blocker_resolution_ledger_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "warning_required_fields",
        all(_warning_row_complete(row) for row in warnings),
        (
            "Every warning ledger row must expose warning id, source report, waiver, "
            "metadata, and owner review fields."
        ),
        "repair_warning_resolution_ledger_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Ledger must preserve read-only recovery safety boundary.",
        "restore_ledger_safety_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_ledger_reader_brief",
    )
    if blockers:
        warning_issues.append(
            {
                "issue_id": "remaining_blockers_still_present",
                "message": "Ledger is valid but recovery blockers still need resolution.",
                "count": len(blockers),
                "recommended_action": _text(payload.get("next_action")),
            }
        )
    if warnings:
        warning_issues.append(
            {
                "issue_id": "remaining_warnings_still_present",
                "message": "Ledger is valid but report/source warnings still need review.",
                "count": len(warnings),
                "recommended_action": "review_warning_resolution_ledger",
            }
        )
    status = FAIL_STATUS
    if not blocking_issues:
        status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([c for c in checks if c["status"] == FAIL_STATUS]),
        "warning_check_count": len(warning_issues),
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "normal_paper_shadow_may_resume": bool(
            summary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            summary.get("live_trading_remains_forbidden")
        ),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Remaining blocker resolution ledger validation is {status}.",
            "key_result": status,
            "blocking_issues": _issue_list(blocking_issues, "issue_id", "message"),
            "warnings": _issue_list(warning_issues, "issue_id", "message"),
            "safety_boundary": "read-only validation; production_effect=none.",
            "next_action": (
                "repair_remaining_blocker_resolution_ledger"
                if status == FAIL_STATUS
                else "review_remaining_blocker_resolution_order"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_remaining_blocker_resolution_ledger_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_remaining_blocker_resolution_ledger_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_remaining_blocker_resolution_ledger_markdown(payload), output_path)


def write_remaining_blocker_resolution_ledger_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_remaining_blocker_resolution_ledger_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(
        render_remaining_blocker_resolution_ledger_validation_markdown(payload),
        output_path,
    )


def render_remaining_blocker_resolution_ledger_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Remaining Blocker Resolution Ledger {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- ledger_status: {payload.get('ledger_status')}",
        f"- recovery_governance_status: {summary.get('recovery_governance_status')}",
        f"- blocker_count: {summary.get('blocker_count')}",
        f"- warning_count: {summary.get('warning_count')}",
        f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
        f"- extended_shadow_remains_forbidden: {summary.get('extended_shadow_remains_forbidden')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Blocker Ledger",
        "",
        "|blocker_id|source_report|source_artifact_id|upstream_dependency|root_cause_category|owner_action_needed|code_data_action_needed|normal|extended|live|",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("blocker_resolution_ledger")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("blocker_id"),
                    row.get("source_report"),
                    row.get("source_artifact_id"),
                    row.get("upstream_dependency"),
                    row.get("root_cause_category"),
                    row.get("owner_action_needed"),
                    row.get("code_data_action_needed"),
                    row.get("blocks_normal_paper_shadow"),
                    row.get("blocks_extended_shadow"),
                    row.get("blocks_live_trading"),
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Warning Ledger",
            "",
            "|warning_id|source_report|waivable|metadata_repair_needed|owner_review_needed|action|",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("warning_resolution_ledger")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    row.get("warning_id"),
                    row.get("source_report"),
                    row.get("waivable"),
                    row.get("metadata_repair_needed"),
                    row.get("owner_review_needed"),
                    row.get("recommended_action"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Safety Boundary", "", "- read-only ledger; production_effect=none.", ""])
    return "\n".join(lines)


def render_remaining_blocker_resolution_ledger_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Remaining Blocker Resolution Ledger Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- blocker_count: {summary.get('blocker_count')}",
        f"- warning_count: {summary.get('warning_count')}",
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
    lines.append("")
    return "\n".join(lines)


def _payload_or_latest(
    payload: Mapping[str, Any] | None,
    path: Path | None,
    latest_fn: Any,
    reports_dir: Path,
    label: str,
) -> tuple[Path | None, Mapping[str, Any]]:
    if payload is not None:
        return path, payload
    source_path = path or latest_fn(reports_dir)
    if source_path is None:
        raise FileNotFoundError(f"{label} JSON not found in {reports_dir}")
    return source_path, _read_json_mapping(source_path)


def _blocker_ledger_row(
    blocker: Mapping[str, Any],
    source_depth_by_id: Mapping[str, Mapping[str, Any]],
    owner_actions: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_id = _text(blocker.get("source_id"))
    source_depth = _mapping(source_depth_by_id.get(source_id))
    action = _mapping(owner_actions.get(source_id))
    root_cause = _text(blocker.get("blocker_category"), "governance_blocker")
    return {
        "blocker_id": _text(blocker.get("issue_id"), f"{source_id}_source_blocking"),
        "source_report": _text(
            blocker.get("source_report"),
            _text(source_depth.get("source_report")),
        ),
        "source_id": source_id,
        "source_artifact_id": _text(source_depth.get("source_artifact_id"), "unknown"),
        "source_artifact_path": _text(
            blocker.get("source_artifact_path"),
            _text(source_depth.get("source_payload_path")),
        ),
        "upstream_dependency": _upstream_dependency(source_id, blocker, source_depth),
        "root_cause_category": root_cause,
        "source_status": _text(blocker.get("source_status")),
        "exact_blocking_field": _text(blocker.get("exact_blocking_field")),
        "owner_action_needed": _owner_action_needed(blocker, action),
        "code_data_action_needed": _code_data_action_needed(blocker, action, source_depth),
        "resolution_priority": _resolution_priority(root_cause, source_id),
        "blocks_normal_paper_shadow": bool(blocker.get("blocks_normal_paper_shadow")),
        "blocks_extended_shadow": bool(blocker.get("blocks_extended_shadow")),
        "blocks_live_trading": False,
        "live_trading_note": "Live trading remains forbidden regardless of this blocker.",
        "production_effect": PRODUCTION_EFFECT,
    }


def _warning_ledger_row(warning: Mapping[str, Any]) -> dict[str, Any]:
    classification = _text(warning.get("warning_classification"))
    proposed_action = _text(warning.get("proposed_action"))
    return {
        "warning_id": _text(warning.get("issue_id")),
        "source_report": _text(warning.get("report_id")),
        "source_artifact_path": _text(warning.get("latest_artifact_path")),
        "warning_classification": classification,
        "issue_status": _text(warning.get("issue_status")),
        "waivable": proposed_action == "create_explicit_expiring_waiver",
        "metadata_repair_needed": classification in {
            "missing_metadata_warning",
            "documentation_inconsistency",
        },
        "owner_review_needed": classification in {
            "true_blocker",
            "governance_warning",
            "legacy_warning_candidate",
        },
        "recommended_action": _text(
            warning.get("owner_action"),
            _text(warning.get("proposed_action")),
        ),
        "waiver_action": _text(warning.get("waiver_action"), "not_applied"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _recovery_pack_warning_ledger_row(
    warning: Mapping[str, Any],
    warning_triage_by_report: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    source_id = _text(warning.get("source_id"))
    triage = _mapping(warning_triage_by_report.get(source_id))
    classification = _text(
        triage.get("warning_classification"),
        _recovery_pack_warning_classification(warning),
    )
    recommended_action = _text(
        warning.get("recommended_action"),
        _text(triage.get("proposed_action"), "review_source_warning"),
    )
    return {
        "warning_id": _text(warning.get("issue_id"), f"{source_id}_warning"),
        "source_report": _text(warning.get("source_report"), source_id),
        "source_id": source_id,
        "source_artifact_path": _text(warning.get("artifact_path")),
        "warning_classification": classification,
        "issue_status": _text(warning.get("source_status")),
        "source_status": _text(warning.get("source_status")),
        "validation_status": _text(warning.get("validation_status")),
        "waivable": False,
        "metadata_repair_needed": classification in {
            "missing_metadata_warning",
            "documentation_inconsistency",
        },
        "owner_review_needed": "owner" in recommended_action.lower()
        or classification in {"true_blocker", "governance_warning"},
        "recommended_action": recommended_action,
        "waiver_action": "not_applied",
        "production_effect": PRODUCTION_EFFECT,
    }


def _recovery_pack_warning_classification(warning: Mapping[str, Any]) -> str:
    source_status = _text(warning.get("source_status"))
    validation_status = _text(warning.get("validation_status"))
    if validation_status == PASS_WITH_WARNINGS_STATUS:
        return "source_validation_warning"
    if source_status == "MANUAL_REVIEW_REQUIRED":
        return "governance_warning"
    return "source_warning"


def _owner_action_lookup(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    groups = _mapping(payload.get("action_groups"))
    result: dict[str, Mapping[str, Any]] = {}
    for key in (
        "actions_requiring_code_data_rerun",
        "actions_requiring_owner_review",
        "actions_requiring_artifact_regeneration",
        "actions_that_should_remain_blocked",
    ):
        for action in _records(groups.get(key)):
            source_id = _text(action.get("source_id"))
            report_id = _text(action.get("report_id"))
            result[source_id or report_id] = action
    return result


def _summary(
    recovery_pack: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pack_summary = _mapping(recovery_pack.get("summary"))
    blocker_categories = Counter(_text(row.get("root_cause_category")) for row in blockers)
    warning_classes = Counter(_text(row.get("warning_classification")) for row in warnings)
    return {
        "recovery_governance_status": _text(recovery_pack.get("recovery_governance_status")),
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "blocker_category_counts": dict(blocker_categories),
        "warning_classification_counts": dict(warning_classes),
        "normal_paper_shadow_may_resume": bool(
            pack_summary.get("normal_paper_shadow_may_resume")
        ),
        "extended_shadow_remains_forbidden": bool(
            pack_summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            pack_summary.get("live_trading_remains_forbidden")
        ),
        "first_resolution_priority": (
            _text(blockers[0].get("resolution_priority")) if blockers else "none"
        ),
        "next_owner_action": _text(
            pack_summary.get("next_owner_action"),
            "review_remaining_blocker_resolution_ledger",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(
    summary: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            f"Remaining blocker ledger has {len(blockers)} blockers and "
            f"{len(warnings)} warnings; extended shadow and live trading remain forbidden."
        ),
        "key_result": _text(summary.get("recovery_governance_status")),
        "blocking_issues": _issue_list(blockers, "source_id", "source_status"),
        "warnings": _issue_list(warnings, "warning_id", "warning_classification"),
        "safety_boundary": (
            "Resolution ledger only; no upstream rerun, no waiver, no owner decision write, "
            "no broker/order, production_effect=none."
        ),
        "next_action": _text(
            summary.get("next_owner_action"),
            "review_remaining_blocker_resolution_ledger",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _upstream_dependency(
    source_id: str,
    blocker: Mapping[str, Any],
    source_depth: Mapping[str, Any],
) -> str:
    if source_id in {"normal_paper_shadow_resumption_gate", "monthly_review", "promotion_board"}:
        return "owner_manual_review_and_recovery_source_chain"
    if "cost" in source_id:
        return "candidate_cost_metrics_and_cost_sensitivity_review"
    if "benchmark" in source_id:
        return "candidate_benchmark_metrics_and_baseline_control"
    if source_id == "decision_snapshot_lifecycle":
        return "canonical_daily_score_decision_snapshot"
    if source_id == "observation_clock":
        return "valid_normal_paper_shadow_observation_days"
    if source_id == "extended_shadow_protocol":
        return "minimum_observation_period_and_all_extension_evidence"
    value = _text(
        source_depth.get("next_required_action"),
        _text(blocker.get("recommended_action")),
    )
    return value or "review_source_dependency"


def _owner_action_needed(
    blocker: Mapping[str, Any],
    action: Mapping[str, Any],
) -> str:
    action_type = _text(action.get("action_type"))
    if action_type == "owner" or bool(blocker.get("is_owner_related")):
        return _text(action.get("recommended_action"), _text(blocker.get("recommended_action")))
    return "owner_review_after_code_data_blockers_clear"


def _code_data_action_needed(
    blocker: Mapping[str, Any],
    action: Mapping[str, Any],
    source_depth: Mapping[str, Any],
) -> str:
    action_type = _text(action.get("action_type"))
    if action_type in {"data", "artifact"}:
        return _text(action.get("recommended_action"))
    if bool(blocker.get("is_data_related")) or _text(source_depth.get("health_status")) in {
        "STALE",
        "PARTIAL",
    }:
        return _text(source_depth.get("next_required_action"), "repair_source_data_or_artifact")
    if bool(blocker.get("is_metric_related")):
        return _text(blocker.get("recommended_action"))
    return "no_code_data_action_until_owner_or_source_review"


def _resolution_priority(root_cause: str, source_id: str) -> str:
    if source_id == "decision_snapshot_lifecycle":
        return "P0_data_artifact_lifecycle"
    if root_cause in {"metric_validity", "registry_data_lifecycle"}:
        return "P0_recovery_evidence"
    if root_cause in {"owner_resumption_gate", "governance_review"}:
        return "P1_owner_governance"
    if root_cause in {"observation_metric", "extended_shadow_safety_gate"}:
        return "P1_extended_shadow_remains_forbidden"
    return "P1_manual_recovery_review"


def _blocker_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "blocker_id",
        "source_report",
        "source_artifact_id",
        "upstream_dependency",
        "root_cause_category",
        "owner_action_needed",
        "code_data_action_needed",
        "blocks_normal_paper_shadow",
        "blocks_extended_shadow",
        "blocks_live_trading",
    )
    return all(key in row and row.get(key) not in (None, "") for key in required)


def _warning_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "warning_id",
        "source_report",
        "waivable",
        "metadata_repair_needed",
        "owner_review_needed",
    )
    return all(key in row and row.get(key) not in (None, "") for key in required)


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


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_recovery_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "ledger_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_write_owner_decision": True,
        "does_not_create_waivers": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
    }


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


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _write_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def _write_text(text: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path
