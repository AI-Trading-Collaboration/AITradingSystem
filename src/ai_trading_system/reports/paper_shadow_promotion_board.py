from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _list_values,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _read_optional_json_mapping,
    _records,
    _report_index_entry,
    _resolve_artifact_path,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "paper_shadow_promotion_board"
VALIDATION_REPORT_TYPE = "paper_shadow_promotion_board_validation"

EXTEND_SHADOW = "EXTEND_SHADOW"
CONTINUE_NORMAL_SHADOW = "CONTINUE_NORMAL_SHADOW"
RETURN_TO_RESEARCH = "RETURN_TO_RESEARCH"
REJECT = "REJECT"
HOLD_FOR_MORE_DATA = "HOLD_FOR_MORE_DATA"
BOARD_DECISIONS = (
    EXTEND_SHADOW,
    CONTINUE_NORMAL_SHADOW,
    RETURN_TO_RESEARCH,
    REJECT,
    HOLD_FOR_MORE_DATA,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"
EXTENDED_SHADOW_OWNER_STATUS_MARKERS = (
    "ENTER_EXTENDED_SHADOW",
    "EXTEND_SHADOW",
    "EXTENDED_SHADOW",
)

EVIDENCE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "check_id": "monthly_review_not_blocked",
        "source_id": "monthly_review",
        "report_id": "research_monthly_review_pack",
        "label": "Research monthly review pack",
        "preferred_json_names": (
            "research_monthly_review_pack.json",
            "research_monthly_review_pack_validation.json",
        ),
        "status_fields": ("monthly_review_status", "status"),
        "pass_statuses": ("MONTHLY_REVIEW_READY", "PASS"),
        "warning_markers": ("WARNING", "MANUAL_REVIEW_REQUIRED"),
        "block_markers": ("BLOCKED", "BLOCK", "FAIL"),
        "required_for_extension": True,
    },
    {
        "check_id": "paper_shadow_health_not_blocking",
        "source_id": "paper_shadow_health",
        "report_id": "etf_dynamic_v3_paper_shadow_health",
        "label": "Canonical paper-shadow health",
        "preferred_json_names": (
            "paper_shadow_health_report.json",
            "paper_shadow_health_manifest.json",
            "paper_shadow_health_validation.json",
        ),
        "status_fields": ("paper_shadow_health_status", "status"),
        "pass_statuses": ("HEALTHY", "PASS"),
        "warning_markers": ("WARNING", "MANUAL_REVIEW_REQUIRED"),
        "block_markers": ("BLOCKED", "BLOCK", "FAIL"),
        "required_for_extension": True,
    },
    {
        "check_id": "weekly_review_available",
        "source_id": "weekly_reviews",
        "report_id": "etf_dynamic_v3_paper_shadow_weekly_review",
        "label": "Paper-shadow weekly review",
        "preferred_json_names": (
            "paper_shadow_weekly_review.json",
            "paper_shadow_weekly_manifest.json",
            "paper_shadow_weekly_validation.json",
        ),
        "status_fields": ("coverage_status", "weekly_decision", "status"),
        "pass_statuses": ("FULL_WEEK_REVIEW", "CONTINUE", "PASS"),
        "warning_markers": ("MANUAL_REVIEW_REQUIRED", "RECOVERY_MODE", "PARTIAL"),
        "block_markers": ("BLOCK", "INSUFFICIENT", "MISSING", "REJECT"),
        "required_for_extension": True,
    },
    {
        "check_id": "readiness_allows_shadow",
        "source_id": "readiness_reports",
        "report_id": "etf_dynamic_v3_shadow_continuation_readiness",
        "label": "Shadow continuation readiness",
        "preferred_json_names": (
            "shadow_continuation_readiness_report.json",
            "shadow_continuation_readiness_manifest.json",
            "shadow_continuation_readiness_validation.json",
        ),
        "status_fields": ("shadow_continuation_readiness", "status"),
        "pass_statuses": ("READY_TO_CONTINUE", "READY_WITH_WARNINGS", "PASS"),
        "warning_markers": ("WARNING", "MANUAL_REVIEW_REQUIRED"),
        "block_markers": ("BLOCK", "STALE", "MISSING", "SAFETY"),
        "required_for_extension": True,
    },
    {
        "check_id": "drift_not_blocking",
        "source_id": "drift_reports",
        "report_id": "etf_dynamic_v3_paper_shadow_drift_monitor",
        "label": "Paper-shadow drift monitor",
        "preferred_json_names": (
            "paper_shadow_drift_report.json",
            "paper_shadow_drift_manifest.json",
            "paper_shadow_drift_validation.json",
        ),
        "status_fields": ("drift_status", "drift_severity", "status"),
        "pass_statuses": ("NONE", "LOW", "OK", "PASS"),
        "warning_markers": ("WATCH", "WARNING", "MEDIUM"),
        "block_markers": ("HIGH", "BLOCK", "FAIL"),
        "required_for_extension": True,
    },
    {
        "check_id": "cost_sensitivity_acceptable",
        "source_id": "cost_sensitivity",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "label": "Cost sensitivity review",
        "preferred_json_names": (
            "cost_sensitivity_review.json",
            "cost_sensitivity_manifest.json",
            "cost_sensitivity_validation.json",
        ),
        "status_fields": ("cost_sensitivity_status", "status"),
        "pass_statuses": (
            "MEANINGFUL_ALL_SCENARIOS",
            "MEANINGFUL_LOW_MEDIUM_ONLY",
            "COST_REVIEW_PASS",
            "PASS",
        ),
        "warning_markers": ("LOW_MEDIUM_ONLY", "WARNING"),
        "block_markers": ("INSUFFICIENT", "NOT_MEANINGFUL", "BLOCK"),
        "required_for_extension": True,
    },
    {
        "check_id": "benchmark_comparison_available",
        "source_id": "benchmark_comparison",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "label": "Benchmark baseline control",
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
            "benchmark_baseline_validation.json",
        ),
        "status_fields": ("benchmark_baseline_status", "status"),
        "pass_statuses": (
            "CANDIDATE_OUTPERFORMS_BASELINES",
            "MIXED_BASELINE_RESULT",
            "BASELINE_CONTROL_PASS",
            "PASS",
        ),
        "warning_markers": ("MIXED", "WARNING"),
        "block_markers": ("INSUFFICIENT", "UNDERPERFORMS", "BLOCK"),
        "required_for_extension": True,
    },
    {
        "check_id": "safety_not_blocked",
        "source_id": "safety_audit",
        "report_id": "research_safety_boundary_audit",
        "label": "Research safety boundary audit",
        "preferred_json_names": ("research_safety_boundary_audit.json",),
        "status_fields": ("safety_status", "status"),
        "pass_statuses": ("SAFETY_PASS", "PASS"),
        "warning_markers": ("WARNING", "WARNINGS"),
        "block_markers": ("SAFETY_BLOCKED", "BLOCK"),
        "required_for_extension": True,
    },
    {
        "check_id": "owner_review_available",
        "source_id": "owner_review",
        "report_id": "etf_dynamic_v3_owner_review",
        "label": "Owner review",
        "preferred_json_names": ("latest_owner_review.json", "owner_review_validation.json"),
        "status_fields": ("owner_decision", "recommended_action", "status"),
        "pass_statuses": (
            "monitor",
            "continue",
            "continue_shadow",
            "hold",
            "enter_extended_shadow",
            "extend_shadow",
            "extended_shadow",
            "PASS",
        ),
        "warning_markers": ("monitor", "hold", "review"),
        "block_markers": ("reject", "return_to_research"),
        "required_for_extension": True,
    },
    {
        "check_id": "owner_decision_audit_recorded",
        "source_id": "owner_decision_audit_log",
        "report_id": "owner_decision_audit_log",
        "label": "Owner decision audit log",
        "preferred_json_names": ("owner_decision_audit_log.json",),
        "status_fields": ("audit_log_status", "status"),
        "pass_statuses": ("AUDIT_LOG_PASS", "PASS"),
        "warning_markers": ("WARNING",),
        "block_markers": ("AUDIT_LOG_BLOCKED", "EMPTY", "FAIL"),
        "required_for_extension": True,
    },
    {
        "check_id": "lineage_available",
        "source_id": "lineage_graph",
        "report_id": "artifact_lineage_graph",
        "label": "Artifact lineage graph",
        "preferred_json_names": ("artifact_lineage_graph.json",),
        "status_fields": ("lineage_status", "status"),
        "pass_statuses": ("PASS",),
        "warning_markers": ("WARNING", "STALE"),
        "block_markers": ("FAIL", "BLOCK", "MISSING"),
        "required_for_extension": True,
    },
)


def default_paper_shadow_promotion_board_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"paper_shadow_promotion_board_{as_of.isoformat()}.json"


def default_paper_shadow_promotion_board_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"paper_shadow_promotion_board_{as_of.isoformat()}.md"


def default_paper_shadow_promotion_board_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"paper_shadow_promotion_board_validation_{as_of.isoformat()}.json"


def default_paper_shadow_promotion_board_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"paper_shadow_promotion_board_validation_{as_of.isoformat()}.md"


def latest_paper_shadow_promotion_board_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "paper_shadow_promotion_board_", ".json")


def build_paper_shadow_promotion_board_payload(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    if report_index_payload is None:
        source_path = report_index_path or (
            project_root / "outputs" / "reports" / f"report_index_{as_of.isoformat()}.json"
        )
        report_index_payload = _read_json_mapping(source_path)
        report_index_path = source_path

    checklist = [
        _evidence_check(spec, report_index_payload, project_root=project_root)
        for spec in EVIDENCE_SPECS
    ]
    blocking_reasons = [
        _blocking_reason(check)
        for check in checklist
        if check.get("check_status") == "BLOCKED"
    ]
    warning_reasons = [
        _blocking_reason(check)
        for check in checklist
        if check.get("check_status") == "WARNING"
    ]
    board_decision = _board_decision(checklist, blocking_reasons, warning_reasons)
    candidate_id = _candidate_id(checklist)
    summary = {
        "candidate_id": candidate_id,
        "board_decision": board_decision,
        "evidence_check_count": len(checklist),
        "passed_evidence_count": len([c for c in checklist if c["check_status"] == "PASS"]),
        "blocked_evidence_count": len(blocking_reasons),
        "warning_evidence_count": len(warning_reasons),
        "required_evidence_count": len(
            [c for c in checklist if c.get("required_for_extension") is True]
        ),
        "required_evidence_passed_count": len(
            [
                c
                for c in checklist
                if c.get("required_for_extension") is True and c["check_status"] == "PASS"
            ]
        ),
        "safety_status": _status_for_source(checklist, "safety_audit"),
        "readiness_status": _status_for_source(checklist, "readiness_reports"),
        "owner_decision_status": _status_for_source(checklist, "owner_decision_audit_log"),
        "lineage_status": _status_for_source(checklist, "lineage_graph"),
        "manual_owner_review_required": True,
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(board_decision, summary, blocking_reasons, warning_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": board_decision,
        "board_decision": board_decision,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "research_governance_only": True,
        "purpose": (
            "Provide a paper-shadow-only promotion board decision from existing "
            "weekly, readiness, drift, cost, benchmark, safety, owner, and lineage evidence."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": board_decision,
        "summary": summary,
        "candidate_id": candidate_id,
        "required_evidence_checklist": checklist,
        "blocking_reasons": blocking_reasons,
        "warning_reasons": warning_reasons,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Board decision is paper-shadow-only and manual-review-only.",
            "The board does not change candidate, paper-shadow, or production state.",
            "HOLD_FOR_MORE_DATA is a valid fail-closed decision when evidence is incomplete.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_existing_report_index_and_source_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_paper_shadow_promotion_board_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    checklist = _records(payload.get("required_evidence_checklist"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_paper_shadow_promotion_board",
    )
    _append_check(
        checks,
        blocking_issues,
        "board_decision_enum",
        _text(payload.get("board_decision")) in BOARD_DECISIONS,
        "board_decision must use the supported paper-shadow promotion board enum.",
        "restore_supported_board_decision_enum",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "promotion board must be production_effect=none.",
        "restore_paper_shadow_only_boundary",
    )
    required_source_ids = {spec["source_id"] for spec in EVIDENCE_SPECS}
    _append_check(
        checks,
        blocking_issues,
        "required_evidence_sources_present",
        required_source_ids.issubset({_text(item.get("source_id")) for item in checklist}),
        "Every required evidence source must be represented in the checklist.",
        "regenerate_board_with_all_required_evidence_sources",
    )
    missing_sources = [
        item
        for item in checklist
        if item.get("availability") != "AVAILABLE" and item.get("required_for_extension") is True
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_evidence_artifacts_available",
        not missing_sources,
        "Required evidence artifacts must have report-index latest pointers.",
        "run_or_repair_required_evidence_artifacts_before_board_review",
        details={"missing_source_ids": [_text(item.get("source_id")) for item in missing_sources]},
    )
    unsafe_sources = [
        item
        for item in checklist
        if item.get("availability") == "AVAILABLE"
        and _text(item.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT
    ]
    _append_check(
        checks,
        blocking_issues,
        "source_production_effect_none",
        not unsafe_sources,
        "Available evidence artifacts must expose production_effect=none.",
        "repair_unsafe_evidence_artifact_before_board_review",
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_live_promotion",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("official_target_weights_generated") is False
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
            and safety.get("automatic_candidate_promotion") is False
        ),
        "Promotion board must not mutate live, official, broker, order, or candidate state.",
        "restore_paper_shadow_promotion_board_safety_boundary",
    )
    reader_brief = _mapping(payload.get("reader_brief"))
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        all(
            bool(_text(reader_brief.get(field)))
            for field in (
                "summary",
                "key_result",
                "blocking_issues",
                "warnings",
                "safety_boundary",
                "next_action",
            )
        ),
        "Reader Brief section must expose summary, key result, blockers, warnings, safety, and next action.",
        "restore_promotion_board_reader_brief_fields",
    )
    extend_prerequisites_satisfied = _extension_prerequisites_satisfied(checklist)
    _append_check(
        checks,
        blocking_issues,
        "extend_shadow_requires_all_prerequisites",
        _text(payload.get("board_decision")) != EXTEND_SHADOW
        or (
            extend_prerequisites_satisfied
            and _extended_shadow_owner_requested(checklist)
        ),
        "EXTEND_SHADOW requires every required evidence check to pass and an explicit owner extended-shadow request.",
        "hold_or_return_candidate_until_extended_shadow_prerequisites_are_satisfied",
        details={
            "failed_required_source_ids": _failed_required_source_ids(checklist),
            "owner_requested_extended_shadow": _extended_shadow_owner_requested(checklist),
        },
    )
    if _int(summary.get("blocked_evidence_count")) > 0 or _int(
        summary.get("warning_evidence_count")
    ) > 0:
        warning_issues.append(
            {
                "issue_id": "promotion_board_contains_evidence_limitations",
                "board_decision": _text(payload.get("board_decision")),
                "message": "Board is structurally valid but contains evidence blockers or warnings.",
                "recommended_action": _text(payload.get("next_action")),
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len([check for check in checks if check["status"] == FAIL_STATUS]),
        "warning_check_count": len(warning_issues),
        "blocking_issue_count": len(blocking_issues),
        "source_blocker_count": _int(summary.get("blocked_evidence_count")),
        "source_warning_count": _int(summary.get("warning_evidence_count")),
        "board_decision": _text(payload.get("board_decision")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_board_decision": _text(payload.get("board_decision"), "UNKNOWN"),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "purpose": "Validate paper-shadow promotion board schema, evidence, and safety boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "source_summary": dict(summary),
        "source_blocking_reasons": _records(payload.get("blocking_reasons")),
        "source_warning_reasons": _records(payload.get("warning_reasons")),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not run upstream evidence commands.",
            "PASS_WITH_WARNINGS means the board is usable for manual review but cannot extend shadow without resolving disclosed blockers or warnings.",
        ],
        "next_action": (
            "use_board_for_manual_owner_review"
            if validation_status != FAIL_STATUS
            else "repair_promotion_board_schema_or_safety_before_owner_review"
        ),
        "reader_brief": _reader_brief(
            _text(payload.get("board_decision"), "UNKNOWN"),
            summary,
            _records(payload.get("blocking_reasons")),
            _records(payload.get("warning_reasons")),
        ),
        "methodology": {
            "collector_mode": "validate_existing_paper_shadow_promotion_board_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_paper_shadow_promotion_board_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_paper_shadow_promotion_board_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_paper_shadow_promotion_board_markdown(payload), encoding="utf-8")
    return output_path


def write_paper_shadow_promotion_board_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_paper_shadow_promotion_board_json(payload, output_path)


def write_paper_shadow_promotion_board_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_paper_shadow_promotion_board_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_paper_shadow_promotion_board_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Paper Shadow Promotion Board {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- board_decision: {payload.get('board_decision')}",
        f"- candidate_id: {summary.get('candidate_id')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- evidence_checks: {summary.get('evidence_check_count')}",
        f"- blocked_evidence: {summary.get('blocked_evidence_count')}",
        f"- warning_evidence: {summary.get('warning_evidence_count')}",
        f"- safety_status: {summary.get('safety_status')}",
        f"- readiness_status: {summary.get('readiness_status')}",
        f"- owner_decision_status: {summary.get('owner_decision_status')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Required Evidence Checklist",
        "",
        "|check_id|source_id|status|source_status|candidate_id|next_action|",
        "|---|---|---|---|---|---|",
    ]
    for check in _records(payload.get("required_evidence_checklist")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    check.get("check_id"),
                    check.get("source_id"),
                    check.get("check_status"),
                    check.get("source_status"),
                    check.get("candidate_id"),
                    check.get("next_action"),
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Blocking Reasons",
            "",
            "|source_id|status|message|recommended_action|",
            "|---|---|---|---|",
        ]
    )
    for reason in _records(payload.get("blocking_reasons")):
        lines.append(_reason_row(reason))
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "|field|value|",
            "|---|---|",
        ]
    )
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_paper_shadow_promotion_board_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Paper Shadow Promotion Board Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_board_decision: {payload.get('source_board_decision')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- source_blockers: {summary.get('source_blocker_count')}",
        f"- source_warnings: {summary.get('source_warning_count')}",
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


def _evidence_check(
    spec: Mapping[str, Any],
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    entry = _report_index_entry(report_index, _text(spec.get("report_id")))
    artifact_path = _resolve_artifact_path(_text(entry.get("latest_artifact_path")), project_root)
    payload_path, payload = _read_source_payload(
        artifact_path,
        _list_values(spec.get("preferred_json_names")),
    )
    availability = (
        "AVAILABLE" if artifact_path is not None and artifact_path.exists() else "MISSING"
    )
    source_status = _source_status(spec, payload, entry)
    candidate_id = _candidate_id_from_payload(payload)
    next_action = _next_action_from_payload(spec, payload)
    check_status = _check_status(spec, availability, source_status, payload)
    return {
        "check_id": _text(spec.get("check_id")),
        "source_id": _text(spec.get("source_id")),
        "report_id": _text(spec.get("report_id")),
        "label": _text(spec.get("label")),
        "required_for_extension": spec.get("required_for_extension") is True,
        "availability": availability,
        "check_status": check_status,
        "source_status": source_status,
        "candidate_id": candidate_id,
        "next_action": next_action,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "source_payload_path": "" if payload_path is None else str(payload_path),
        "production_effect": _text(
            payload.get("production_effect"),
            _text(entry.get("production_effect"), PRODUCTION_EFFECT),
        ),
        "summary": _compact_summary(_mapping(payload.get("summary"))),
    }


def _read_source_payload(
    artifact_path: Path | None,
    preferred_json_names: Sequence[str],
) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [artifact_path.parent / name for name in preferred_json_names]
    candidates.append(
        artifact_path if artifact_path.suffix == ".json" else artifact_path.with_suffix(".json")
    )
    seen: set[str] = set()
    for candidate in candidates:
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _source_status(
    spec: Mapping[str, Any],
    payload: Mapping[str, Any],
    entry: Mapping[str, Any],
) -> str:
    for field in _list_values(spec.get("status_fields")):
        value = _text(payload.get(field), _text(_mapping(payload.get("summary")).get(field)))
        if value:
            return value
    for field in ("status", "freshness_status", "artifact_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _check_status(
    spec: Mapping[str, Any],
    availability: str,
    source_status: str,
    payload: Mapping[str, Any],
) -> str:
    if availability != "AVAILABLE":
        return "BLOCKED"
    normalized = source_status.upper()
    if _int(payload.get("failed_check_count")) > 0:
        return "BLOCKED"
    if any(marker.upper() in normalized for marker in _list_values(spec.get("block_markers"))):
        return "BLOCKED"
    if any(marker.upper() in normalized for marker in _list_values(spec.get("warning_markers"))):
        return "WARNING"
    if any(normalized == value.upper() for value in _list_values(spec.get("pass_statuses"))):
        return "PASS"
    if "PASS_WITH_WARNINGS" in normalized:
        return "WARNING"
    if "PASS" in normalized or "OK" == normalized:
        return "PASS"
    return "WARNING"


def _board_decision(
    checklist: Sequence[Mapping[str, Any]],
    blocking_reasons: Sequence[Mapping[str, Any]],
    warning_reasons: Sequence[Mapping[str, Any]],
) -> str:
    status_blob = " ".join(
        _text(check.get("source_status")) + " " + _text(check.get("next_action"))
        for check in checklist
    ).upper()
    if "REJECT" in status_blob:
        return REJECT
    if "RETURN_TO_RESEARCH" in status_blob:
        return RETURN_TO_RESEARCH
    if blocking_reasons:
        return HOLD_FOR_MORE_DATA
    if _extended_shadow_owner_requested(checklist):
        if not _extension_prerequisites_satisfied(checklist):
            return HOLD_FOR_MORE_DATA
        return EXTEND_SHADOW
    if warning_reasons:
        return CONTINUE_NORMAL_SHADOW
    return CONTINUE_NORMAL_SHADOW


def _extension_prerequisites_satisfied(checklist: Sequence[Mapping[str, Any]]) -> bool:
    return all(
        check.get("required_for_extension") is not True or check.get("check_status") == "PASS"
        for check in checklist
    )


def _failed_required_source_ids(checklist: Sequence[Mapping[str, Any]]) -> list[str]:
    return [
        _text(check.get("source_id"))
        for check in checklist
        if check.get("required_for_extension") is True and check.get("check_status") != "PASS"
    ]


def _extended_shadow_owner_requested(checklist: Sequence[Mapping[str, Any]]) -> bool:
    owner_status = _status_for_source(checklist, "owner_review").upper()
    return any(marker in owner_status for marker in EXTENDED_SHADOW_OWNER_STATUS_MARKERS)


def _blocking_reason(check: Mapping[str, Any]) -> dict[str, Any]:
    source_id = _text(check.get("source_id"))
    status = _text(check.get("source_status"), "UNKNOWN")
    return {
        "reason_id": f"{source_id}_{_text(check.get('check_status')).lower()}",
        "source_id": source_id,
        "status": status,
        "candidate_id": _text(check.get("candidate_id")),
        "message": (
            f"{_text(check.get('label'))} evidence is {_text(check.get('check_status'))} "
            f"with source_status={status}."
        ),
        "recommended_action": _text(
            check.get("next_action"),
            "review_or_repair_evidence_before_promotion_board_decision",
        ),
        "artifact_path": _text(check.get("artifact_path")),
        "production_effect": _text(check.get("production_effect"), PRODUCTION_EFFECT),
    }


def _reader_brief(
    board_decision: str,
    summary: Mapping[str, Any],
    blocking_reasons: Sequence[Mapping[str, Any]],
    warning_reasons: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            f"Paper-shadow promotion board decision is {board_decision} for "
            f"{_text(summary.get('candidate_id'), 'unknown candidate')}."
        ),
        "key_result": board_decision,
        "blocking_issues": (
            "none"
            if not blocking_reasons
            else "; ".join(
                f"{_text(reason.get('source_id'))}:{_text(reason.get('status'))}"
                for reason in blocking_reasons[:5]
            )
        ),
        "warnings": (
            "none"
            if not warning_reasons
            else "; ".join(
                f"{_text(reason.get('source_id'))}:{_text(reason.get('status'))}"
                for reason in warning_reasons[:5]
            )
        ),
        "safety_boundary": (
            "Paper-shadow-only promotion board; no live trading, no official target "
            "weights, no broker/order, no candidate or production mutation, "
            "production_effect=none."
        ),
        "next_action": _reader_next_action(board_decision),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_next_action(board_decision: str) -> str:
    if board_decision == HOLD_FOR_MORE_DATA:
        return "resolve_evidence_blockers_or_record_owner_hold_decision"
    if board_decision == RETURN_TO_RESEARCH:
        return "return_candidate_to_research_queue_after_owner_review"
    if board_decision == REJECT:
        return "record_rejection_postmortem_before_revisiting_candidate"
    if board_decision == EXTEND_SHADOW:
        return "owner_may_review_extended_shadow_protocol_before_any_next_step"
    return "continue_normal_paper_shadow_and_review_next_weekly_pack"


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "automatic_owner_approval": False,
        "automatic_candidate_promotion": False,
        "live_trading_allowed": False,
    }


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
    *,
    details: Mapping[str, Any] | None = None,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    if details:
        check["details"] = dict(details)
    checks.append(check)
    if not passed:
        issue = {
            "issue_id": check_id,
            "message": message,
            "recommended_action": recommended_action,
        }
        if details:
            issue["details"] = dict(details)
        blocking_issues.append(issue)


def _candidate_id(checklist: Sequence[Mapping[str, Any]]) -> str:
    for check in checklist:
        candidate = _text(check.get("candidate_id"))
        if candidate:
            return candidate
    return ""


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("candidate", "candidate_id"):
        value = _text(payload.get(key))
        if value:
            return value
    for section_id in ("promotion_board_inputs", "monthly_review_pack_inputs"):
        section = _mapping(payload.get(section_id))
        for key in ("candidate", "candidate_id"):
            value = _text(section.get(key))
            if value:
                return value
    return ""


def _next_action_from_payload(spec: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    for key in ("next_required_action", "next_action", "recommended_action"):
        value = _text(payload.get(key), _text(_mapping(payload.get("summary")).get(key)))
        if value:
            return value
    for section_id in ("promotion_board_inputs", "reader_brief"):
        value = _text(_mapping(payload.get(section_id)).get("next_action"))
        if value:
            return value
    return f"review_{_text(spec.get('source_id'))}_before_promotion_board"


def _status_for_source(checklist: Sequence[Mapping[str, Any]], source_id: str) -> str:
    for check in checklist:
        if _text(check.get("source_id")) == source_id:
            return _text(check.get("source_status"), "MISSING")
    return "MISSING"


def _compact_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[_text(key)] = value
        if len(compact) >= 16:
            break
    return compact


def _reason_row(reason: Mapping[str, Any]) -> str:
    return (
        f"|{_md_cell(reason.get('source_id'))}|{_md_cell(reason.get('status'))}|"
        f"{_md_cell(reason.get('message'))}|{_md_cell(reason.get('recommended_action'))}|"
    )
