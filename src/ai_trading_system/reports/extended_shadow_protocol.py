from __future__ import annotations

import json
from collections.abc import Mapping
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
REPORT_TYPE = "extended_shadow_protocol"
VALIDATION_REPORT_TYPE = "extended_shadow_protocol_validation"

EXTENDED_SHADOW_ELIGIBLE = "EXTENDED_SHADOW_ELIGIBLE"
EXTENDED_SHADOW_BLOCKED = "EXTENDED_SHADOW_BLOCKED"
ELIGIBILITY_STATUSES = (EXTENDED_SHADOW_ELIGIBLE, EXTENDED_SHADOW_BLOCKED)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

# TRADING-350 established this as the paper-shadow pilot baseline; changing it
# requires a policy/task-register update because it affects investment interpretation.
MINIMUM_OBSERVATION_TRADING_DAYS = 20

CHECK_SPECS: tuple[dict[str, Any], ...] = (
    {
        "check_id": "promotion_board_allows_extended_review",
        "source_id": "promotion_board",
        "report_id": "paper_shadow_promotion_board",
        "label": "Paper-shadow promotion board",
        "preferred_json_names": ("paper_shadow_promotion_board.json",),
        "status_fields": ("board_decision", "status"),
        "pass_statuses": ("EXTEND_SHADOW",),
        "warning_markers": ("CONTINUE_NORMAL_SHADOW",),
        "block_markers": ("HOLD_FOR_MORE_DATA", "RETURN_TO_RESEARCH", "REJECT", "MISSING"),
    },
    {
        "check_id": "no_blocking_stale_data",
        "source_id": "staleness_monitor",
        "report_id": "etf_dynamic_v3_evidence_staleness_monitor",
        "label": "Evidence staleness monitor",
        "preferred_json_names": (
            "evidence_staleness_monitor_report.json",
            "evidence_staleness_monitor_manifest.json",
        ),
        "status_fields": ("evidence_freshness_status", "status"),
        "pass_statuses": ("ACCEPTABLE", "FRESH", "PASS"),
        "warning_markers": ("WARNING", "PASS_WITH_WARNINGS"),
        "block_markers": ("BLOCK", "STALE", "MISSING", "FAIL"),
    },
    {
        "check_id": "stable_weekly_review_coverage",
        "source_id": "weekly_review",
        "report_id": "etf_dynamic_v3_paper_shadow_weekly_review",
        "label": "Paper-shadow weekly review",
        "preferred_json_names": (
            "paper_shadow_weekly_review.json",
            "paper_shadow_weekly_manifest.json",
            "paper_shadow_weekly_validation.json",
        ),
        "status_fields": ("coverage_status", "coverage_classification", "weekly_decision"),
        "pass_statuses": ("FULL_WEEK_REVIEW", "CONTINUE", "PASS"),
        "warning_markers": ("RECOVERY", "PARTIAL", "WATCH", "MANUAL_REVIEW_REQUIRED"),
        "block_markers": ("INSUFFICIENT", "REJECT", "RETURN_TO_RESEARCH", "MISSING"),
    },
    {
        "check_id": "readiness_not_blocked",
        "source_id": "readiness",
        "report_id": "etf_dynamic_v3_shadow_continuation_readiness",
        "label": "Shadow continuation readiness",
        "preferred_json_names": (
            "shadow_continuation_readiness_report.json",
            "shadow_continuation_readiness_manifest.json",
        ),
        "status_fields": ("shadow_continuation_readiness", "status"),
        "pass_statuses": ("READY_TO_CONTINUE", "PASS"),
        "warning_markers": ("READY_WITH_WARNINGS", "WARNING"),
        "block_markers": ("BLOCK", "STALE", "MISSING", "SAFETY"),
    },
    {
        "check_id": "safety_has_no_unresolved_warnings",
        "source_id": "safety_audit",
        "report_id": "research_safety_boundary_audit",
        "label": "Research safety boundary audit",
        "preferred_json_names": ("research_safety_boundary_audit.json",),
        "status_fields": ("safety_status", "status"),
        "pass_statuses": ("SAFETY_PASS", "PASS"),
        "warning_markers": (),
        "block_markers": ("WARNING", "WARNINGS", "SAFETY_BLOCKED", "BLOCK", "MISSING"),
    },
    {
        "check_id": "cost_sensitivity_acceptable",
        "source_id": "cost_sensitivity",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "label": "Cost sensitivity review",
        "preferred_json_names": (
            "cost_sensitivity_review.json",
            "cost_sensitivity_manifest.json",
        ),
        "status_fields": ("cost_sensitivity_status", "status"),
        "pass_statuses": (
            "MEANINGFUL_ALL_SCENARIOS",
            "MEANINGFUL_LOW_MEDIUM_ONLY",
            "COST_REVIEW_PASS",
            "PASS",
        ),
        "warning_markers": ("LOW_MEDIUM_ONLY", "WARNING"),
        "block_markers": ("INSUFFICIENT", "NOT_MEANINGFUL", "BLOCK", "MISSING"),
    },
    {
        "check_id": "benchmark_comparison_available",
        "source_id": "benchmark_comparison",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "label": "Benchmark baseline control",
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
        ),
        "status_fields": ("benchmark_baseline_status", "status"),
        "pass_statuses": (
            "CANDIDATE_OUTPERFORMS_BASELINES",
            "MIXED_BASELINE_RESULT",
            "BASELINE_CONTROL_PASS",
            "PASS",
        ),
        "warning_markers": ("MIXED", "WARNING"),
        "block_markers": ("INSUFFICIENT", "BLOCK", "MISSING"),
    },
    {
        "check_id": "owner_review_complete",
        "source_id": "owner_decision_audit_log",
        "report_id": "owner_decision_audit_log",
        "label": "Owner decision audit log",
        "preferred_json_names": ("owner_decision_audit_log.json",),
        "status_fields": ("audit_log_status", "status"),
        "pass_statuses": ("AUDIT_LOG_PASS", "PASS"),
        "warning_markers": (),
        "block_markers": ("EMPTY", "AUDIT_LOG_BLOCKED", "FAIL", "MISSING"),
    },
    {
        "check_id": "lineage_graph_available",
        "source_id": "lineage_graph",
        "report_id": "artifact_lineage_graph",
        "label": "Artifact lineage graph",
        "preferred_json_names": ("artifact_lineage_graph.json",),
        "status_fields": ("lineage_status", "status"),
        "pass_statuses": ("PASS",),
        "warning_markers": ("WARNING", "STALE"),
        "block_markers": ("FAIL", "BLOCK", "MISSING"),
    },
)


def default_extended_shadow_protocol_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"extended_shadow_protocol_{as_of.isoformat()}.json"


def default_extended_shadow_protocol_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"extended_shadow_protocol_{as_of.isoformat()}.md"


def default_extended_shadow_protocol_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"extended_shadow_protocol_validation_{as_of.isoformat()}.json"


def default_extended_shadow_protocol_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"extended_shadow_protocol_validation_{as_of.isoformat()}.md"


def latest_extended_shadow_protocol_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "extended_shadow_protocol_", ".json")


def build_extended_shadow_protocol_payload(
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

    checks = [
        _eligibility_check(spec, report_index_payload, project_root=project_root)
        for spec in CHECK_SPECS
    ]
    observation_days = _observation_days(checks)
    observation_check = _observation_period_check(observation_days)
    checklist = [*checks, observation_check]
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
    eligibility_status = (
        EXTENDED_SHADOW_BLOCKED if blocking_reasons or warning_reasons else EXTENDED_SHADOW_ELIGIBLE
    )
    candidate_id = _candidate_id(checklist)
    summary = {
        "candidate_id": candidate_id,
        "eligibility_status": eligibility_status,
        "minimum_observation_trading_days": MINIMUM_OBSERVATION_TRADING_DAYS,
        "observed_trading_days": observation_days,
        "check_count": len(checklist),
        "passed_check_count": len([c for c in checklist if c["check_status"] == "PASS"]),
        "blocked_check_count": len(blocking_reasons),
        "warning_check_count": len(warning_reasons),
        "readiness_status": _status_for_source(checklist, "readiness"),
        "safety_status": _status_for_source(checklist, "safety_audit"),
        "owner_decision_status": _status_for_source(checklist, "owner_decision_audit_log"),
        "lineage_status": _status_for_source(checklist, "lineage_graph"),
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, blocking_reasons, warning_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": eligibility_status,
        "eligibility_status": eligibility_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "research_governance_only": True,
        "purpose": (
            "Evaluate whether a candidate is eligible for extended paper-shadow "
            "observation under stricter governance requirements."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": eligibility_status,
        "summary": summary,
        "candidate_id": candidate_id,
        "minimum_observation_policy": {
            "minimum_observation_trading_days": MINIMUM_OBSERVATION_TRADING_DAYS,
            "policy_basis": "TRADING-350 paper-shadow protocol pilot baseline",
            "production_effect": PRODUCTION_EFFECT,
        },
        "eligibility_checklist": checklist,
        "blocking_reasons": blocking_reasons,
        "warning_reasons": warning_reasons,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Extended shadow protocol is still paper-shadow-only and manual-review-only.",
            "Eligibility does not promote to live trading or official allocation.",
            (
                "Missing observation days, owner decisions, cost, benchmark, or safety "
                "clearance are not fabricated."
            ),
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_existing_report_index_and_source_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_extended_shadow_protocol_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    checklist = _records(payload.get("eligibility_checklist"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_extended_shadow_protocol",
    )
    _append_check(
        checks,
        blocking_issues,
        "eligibility_status_enum",
        _text(payload.get("eligibility_status")) in ELIGIBILITY_STATUSES,
        "eligibility_status must use the supported extended-shadow enum.",
        "restore_supported_extended_shadow_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "Extended shadow protocol must be production_effect=none.",
        "restore_extended_shadow_safety_boundary",
    )
    required_source_ids = {spec["source_id"] for spec in CHECK_SPECS}
    present_source_ids = {_text(item.get("source_id")) for item in checklist}
    _append_check(
        checks,
        blocking_issues,
        "required_sources_present",
        required_source_ids.issubset(present_source_ids),
        "Every required extended-shadow source must be represented.",
        "regenerate_protocol_with_required_sources",
    )
    missing_sources = [
        item
        for item in checklist
        if item.get("availability") != "AVAILABLE"
        and item.get("source_id") != "minimum_observation_period"
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_source_artifacts_available",
        not missing_sources,
        "Required source artifacts must have latest report-index pointers.",
        "run_or_repair_required_sources_before_extended_shadow_review",
        details={"missing_source_ids": [_text(item.get("source_id")) for item in missing_sources]},
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_live_promotion",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        "Extended shadow protocol must not mutate live, official, broker, or candidate state.",
        "restore_extended_shadow_safety_boundary",
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
        (
            "Reader Brief section must expose summary, key result, blockers, warnings, "
            "safety, and next action."
        ),
        "restore_extended_shadow_reader_brief_fields",
    )
    if _int(summary.get("blocked_check_count")) > 0 or _int(summary.get("warning_check_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "extended_shadow_protocol_contains_evidence_limitations",
                "eligibility_status": _text(payload.get("eligibility_status")),
                "message": "Protocol is structurally valid but extended shadow remains blocked.",
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
        "source_blocker_count": _int(summary.get("blocked_check_count")),
        "source_warning_count": _int(summary.get("warning_check_count")),
        "eligibility_status": _text(payload.get("eligibility_status")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_eligibility_status": _text(payload.get("eligibility_status"), "UNKNOWN"),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "purpose": "Validate extended shadow protocol schema, evidence, and safety boundary.",
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
            "Validation does not run upstream commands or alter paper-shadow state.",
            "PASS_WITH_WARNINGS means the report is usable but extended shadow is not eligible.",
        ],
        "next_action": (
            "use_protocol_for_manual_owner_review"
            if validation_status != FAIL_STATUS
            else "repair_extended_shadow_protocol_schema_or_safety"
        ),
        "reader_brief": _reader_brief(
            summary,
            _records(payload.get("blocking_reasons")),
            _records(payload.get("warning_reasons")),
        ),
        "methodology": {
            "collector_mode": "validate_existing_extended_shadow_protocol_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_extended_shadow_protocol_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_extended_shadow_protocol_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_extended_shadow_protocol_markdown(payload), encoding="utf-8")
    return output_path


def write_extended_shadow_protocol_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_extended_shadow_protocol_json(payload, output_path)


def write_extended_shadow_protocol_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_extended_shadow_protocol_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_extended_shadow_protocol_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Extended Shadow Protocol {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- eligibility_status: {payload.get('eligibility_status')}",
        f"- candidate_id: {summary.get('candidate_id')}",
        f"- observed_trading_days: {summary.get('observed_trading_days')}",
        f"- minimum_observation_trading_days: {summary.get('minimum_observation_trading_days')}",
        f"- blocked_checks: {summary.get('blocked_check_count')}",
        f"- warning_checks: {summary.get('warning_check_count')}",
        f"- safety_status: {summary.get('safety_status')}",
        f"- readiness_status: {summary.get('readiness_status')}",
        f"- owner_decision_status: {summary.get('owner_decision_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Eligibility Checklist",
        "",
        "|check_id|source_id|status|source_status|candidate_id|next_action|",
        "|---|---|---|---|---|---|",
    ]
    for check in _records(payload.get("eligibility_checklist")):
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
    lines.extend(["", "## Blocking Reasons", "", "|source_id|status|message|", "|---|---|---|"])
    for reason in _records(payload.get("blocking_reasons")):
        lines.append(
            f"|{_md_cell(reason.get('source_id'))}|{_md_cell(reason.get('status'))}|"
            f"{_md_cell(reason.get('message'))}|"
        )
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_extended_shadow_protocol_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Extended Shadow Protocol Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_eligibility_status: {payload.get('source_eligibility_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
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


def _eligibility_check(
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
    check_status = _check_status(spec, availability, source_status, payload)
    return {
        "check_id": _text(spec.get("check_id")),
        "source_id": _text(spec.get("source_id")),
        "report_id": _text(spec.get("report_id")),
        "label": _text(spec.get("label")),
        "availability": availability,
        "check_status": check_status,
        "source_status": source_status,
        "candidate_id": _candidate_id_from_payload(payload),
        "next_action": _next_action_from_payload(spec, payload),
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "source_payload_path": "" if payload_path is None else str(payload_path),
        "production_effect": _text(
            payload.get("production_effect"),
            _text(entry.get("production_effect"), PRODUCTION_EFFECT),
        ),
        "summary": {
            key: value
            for key, value in _mapping(payload.get("summary")).items()
            if isinstance(value, (str, int, float, bool)) or value is None
        },
    }


def _read_source_payload(
    artifact_path: Path | None,
    preferred_json_names: list[str],
) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [artifact_path.parent / name for name in preferred_json_names]
    candidates.extend([artifact_path, artifact_path.with_suffix(".json")])
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
    summary = _mapping(payload.get("summary"))
    for field in _list_values(spec.get("status_fields")):
        value = _text(payload.get(field), _text(summary.get(field)))
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


def _observation_days(checks: list[dict[str, Any]]) -> int:
    candidate_keys = (
        "observation_trading_days",
        "observed_trading_days",
        "paper_shadow_observation_trading_days",
        "observation_day_count",
    )
    values: list[int] = []
    for check in checks:
        summary = _mapping(check.get("summary"))
        for key in candidate_keys:
            value = _int(summary.get(key), _int(check.get(key)))
            if value > 0:
                values.append(value)
    return max(values) if values else 0


def _observation_period_check(observed_days: int) -> dict[str, Any]:
    status = "PASS" if observed_days >= MINIMUM_OBSERVATION_TRADING_DAYS else "BLOCKED"
    return {
        "check_id": "minimum_observation_period_met",
        "source_id": "minimum_observation_period",
        "report_id": "extended_shadow_policy",
        "label": "Minimum observation period",
        "availability": "AVAILABLE",
        "check_status": status,
        "source_status": f"{observed_days}/{MINIMUM_OBSERVATION_TRADING_DAYS}",
        "candidate_id": "",
        "next_action": (
            "continue_normal_shadow_until_minimum_observation_period_is_met"
            if status == "BLOCKED"
            else "owner_may_review_extended_shadow_eligibility"
        ),
        "artifact_path": "",
        "source_payload_path": "",
        "production_effect": PRODUCTION_EFFECT,
        "summary": {
            "observed_trading_days": observed_days,
            "minimum_observation_trading_days": MINIMUM_OBSERVATION_TRADING_DAYS,
        },
    }


def _blocking_reason(check: Mapping[str, Any]) -> dict[str, Any]:
    status = _text(check.get("source_status"), "UNKNOWN")
    return {
        "reason_id": f"{_text(check.get('source_id'))}_{_text(check.get('check_status')).lower()}",
        "source_id": _text(check.get("source_id")),
        "status": status,
        "candidate_id": _text(check.get("candidate_id")),
        "message": (
            f"{_text(check.get('label'))} is {_text(check.get('check_status'))} "
            f"with source_status={status}."
        ),
        "recommended_action": _text(
            check.get("next_action"),
            "resolve_extended_shadow_eligibility_evidence_before_owner_review",
        ),
        "artifact_path": _text(check.get("artifact_path")),
        "production_effect": _text(check.get("production_effect"), PRODUCTION_EFFECT),
    }


def _reader_brief(
    summary: Mapping[str, Any],
    blocking_reasons: list[Mapping[str, Any]],
    warning_reasons: list[Mapping[str, Any]],
) -> dict[str, Any]:
    status = _text(summary.get("eligibility_status"), EXTENDED_SHADOW_BLOCKED)
    return {
        "summary": (
            f"Extended shadow protocol status is {status} for "
            f"{_text(summary.get('candidate_id'), 'unknown candidate')}."
        ),
        "key_result": status,
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
            "Extended shadow remains paper-shadow-only; no live trading, no official "
            "target weights, no broker/order, no candidate or production mutation, "
            "production_effect=none."
        ),
        "next_action": (
            "resolve_extended_shadow_blockers_before_owner_review"
            if status == EXTENDED_SHADOW_BLOCKED
            else "owner_may_review_extended_shadow_observation_plan"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


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


def _candidate_id(checks: list[Mapping[str, Any]]) -> str:
    for check in checks:
        candidate = _text(check.get("candidate_id"))
        if candidate:
            return candidate
    return ""


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("candidate_id", "candidate"):
        value = _text(payload.get(key))
        if value:
            return value
    for section_id in ("summary", "candidate_summary"):
        section = _mapping(payload.get(section_id))
        for key in ("candidate_id", "candidate"):
            value = _text(section.get(key))
            if value:
                return value
    return ""


def _next_action_from_payload(spec: Mapping[str, Any], payload: Mapping[str, Any]) -> str:
    for key in ("next_required_action", "next_action", "recommended_action"):
        value = _text(payload.get(key), _text(_mapping(payload.get("summary")).get(key)))
        if value:
            return value
    return f"review_{_text(spec.get('source_id'))}_before_extended_shadow"


def _status_for_source(checks: list[Mapping[str, Any]], source_id: str) -> str:
    for check in checks:
        if _text(check.get("source_id")) == source_id:
            return _text(check.get("source_status"), "MISSING")
    return "MISSING"
