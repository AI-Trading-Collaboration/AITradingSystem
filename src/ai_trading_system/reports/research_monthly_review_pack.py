from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "research_monthly_review_pack"
VALIDATION_REPORT_TYPE = "research_monthly_review_pack_validation"
PRODUCTION_EFFECT = "none"

READY_STATUS = "MONTHLY_REVIEW_READY"
WARN_STATUS = "MONTHLY_REVIEW_READY_WITH_WARNINGS"
BLOCKED_STATUS = "MONTHLY_REVIEW_BLOCKED"

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

CORE_READER_BRIEF_SECTIONS: tuple[str, ...] = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)

SOURCE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "source_id": "candidate_ledgers",
        "report_id": "etf_dynamic_v3_candidate_decision_ledger",
        "label": "Candidate decision ledger",
        "source_group": "candidate_research",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "candidate_decision_record.json",
            "candidate_decision_ledger_manifest.json",
            "candidate_decision_ledger_validation.json",
        ),
        "status_fields": ("final_decision", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "paper_shadow_weekly_reviews",
        "report_id": "etf_dynamic_v3_paper_shadow_weekly_review",
        "label": "Paper-shadow weekly review",
        "source_group": "paper_shadow",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "paper_shadow_weekly_review.json",
            "paper_shadow_weekly_manifest.json",
            "paper_shadow_weekly_validation.json",
        ),
        "status_fields": ("coverage_status", "weekly_decision", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "staleness_monitors",
        "report_id": "etf_dynamic_v3_evidence_staleness_monitor",
        "label": "Evidence staleness monitor",
        "source_group": "paper_shadow",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "evidence_staleness_report.json",
            "evidence_staleness_manifest.json",
            "evidence_staleness_validation.json",
        ),
        "status_fields": ("evidence_freshness_status", "status"),
        "next_action_fields": ("next_refresh_action", "next_required_action", "next_action"),
    },
    {
        "source_id": "readiness_reports",
        "report_id": "etf_dynamic_v3_shadow_continuation_readiness",
        "label": "Shadow continuation readiness",
        "source_group": "paper_shadow",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "shadow_continuation_readiness_report.json",
            "shadow_continuation_readiness_manifest.json",
            "shadow_continuation_readiness_validation.json",
        ),
        "status_fields": ("shadow_continuation_readiness", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "safety_boundary_audits",
        "report_id": "research_safety_boundary_audit",
        "label": "Research safety boundary audit",
        "source_group": "safety",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("research_safety_boundary_audit.json",),
        "status_fields": ("safety_status", "status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "owner_decisions",
        "report_id": "owner_decision_audit_log",
        "label": "Owner decision audit log",
        "source_group": "owner_decision",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("owner_decision_audit_log.json",),
        "status_fields": ("audit_log_status", "status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "cost_sensitivity_reports",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "label": "Cost sensitivity review",
        "source_group": "research_validity",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "cost_sensitivity_review.json",
            "cost_sensitivity_manifest.json",
            "cost_sensitivity_validation.json",
        ),
        "status_fields": ("cost_sensitivity_status", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "benchmark_comparison_reports",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "label": "Benchmark baseline control",
        "source_group": "research_validity",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
            "benchmark_baseline_validation.json",
        ),
        "status_fields": ("benchmark_baseline_status", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "lineage_graph",
        "report_id": "artifact_lineage_graph",
        "label": "Artifact lineage graph",
        "source_group": "lineage",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("artifact_lineage_graph.json",),
        "status_fields": ("lineage_status", "status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "data_refresh_audit",
        "report_id": "data_refresh_audit",
        "label": "Data refresh audit",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("data_refresh_audit.json",),
        "status_fields": ("status", "validation_status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "data_source_fallback_policy",
        "report_id": "data_source_fallback_policy",
        "label": "Data source fallback policy",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("data_source_fallback_policy.json",),
        "status_fields": ("fallback_status", "status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "cache_catalog",
        "report_id": "cache_catalog",
        "label": "Checksum and cache catalog",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("cache_catalog.json",),
        "status_fields": ("cache_integrity_status", "status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "pit_source_manifest",
        "report_id": "pit_source_manifest",
        "label": "PIT source manifest",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": ("pit_source_manifest.json",),
        "status_fields": ("status", "validation_status"),
        "next_action_fields": ("next_action",),
    },
    {
        "source_id": "signal_input_completeness",
        "report_id": "etf_dynamic_v3_signal_input_completeness",
        "label": "Signal input completeness",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "signal_input_completeness_report.json",
            "signal_input_completeness_manifest.json",
            "signal_input_completeness_validation.json",
        ),
        "status_fields": ("signal_input_status", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
    {
        "source_id": "paper_shadow_health",
        "report_id": "etf_dynamic_v3_paper_shadow_health",
        "label": "Canonical paper-shadow health",
        "source_group": "data_governance",
        "required": True,
        "blocking_if_missing": True,
        "preferred_json_names": (
            "paper_shadow_health_report.json",
            "paper_shadow_health_manifest.json",
            "paper_shadow_health_validation.json",
        ),
        "status_fields": ("paper_shadow_health_status", "health_status", "status"),
        "next_action_fields": ("next_required_action", "next_action"),
    },
)

REQUIRED_SOURCE_IDS = tuple(spec["source_id"] for spec in SOURCE_SPECS if spec["required"])
PAPER_SHADOW_SOURCE_IDS = {
    "paper_shadow_weekly_reviews",
    "staleness_monitors",
    "readiness_reports",
    "cost_sensitivity_reports",
    "benchmark_comparison_reports",
    "signal_input_completeness",
    "paper_shadow_health",
}
DATA_GOVERNANCE_SOURCE_IDS = {
    "data_refresh_audit",
    "data_source_fallback_policy",
    "cache_catalog",
    "pit_source_manifest",
    "signal_input_completeness",
    "paper_shadow_health",
}
SOURCE_SPECIFIC_BLOCKING_STATUS_MARKERS = {
    # Upstream governance status enums that mean evidence is present but still
    # blocks monthly governance clearance. These are not tunable thresholds.
    "cost_sensitivity_reports": ("NOT_MEANINGFUL",),
    "benchmark_comparison_reports": ("UNDERPERFORMS",),
}


def default_research_monthly_review_pack_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_monthly_review_pack_{as_of.isoformat()}.json"


def default_research_monthly_review_pack_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"research_monthly_review_pack_{as_of.isoformat()}.md"


def default_research_monthly_review_pack_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_monthly_review_pack_validation_{as_of.isoformat()}.json"


def default_research_monthly_review_pack_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"research_monthly_review_pack_validation_{as_of.isoformat()}.md"


def latest_research_monthly_review_pack_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "research_monthly_review_pack_", ".json")


def build_research_monthly_review_pack_payload(
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

    source_aggregation = [
        _source_item(spec, report_index_payload, project_root=project_root)
        for spec in SOURCE_SPECS
    ]
    candidates = _candidate_sections(source_aggregation)
    major_blockers = _major_issues(source_aggregation, "blocking")
    major_warnings = _major_issues(source_aggregation, "warning")
    safety_status = _source_status_by_id(
        source_aggregation,
        "safety_boundary_audits",
        default="MISSING",
    )
    data_governance_status = _data_governance_status(source_aggregation)
    owner_decision_status = _owner_decision_status(source_aggregation)

    status = _monthly_status(
        safety_status=safety_status,
        data_governance_status=data_governance_status["status"],
        major_blockers=major_blockers,
        major_warnings=major_warnings,
    )
    summary = {
        "source_family_count": len(source_aggregation),
        "required_source_family_count": len(REQUIRED_SOURCE_IDS),
        "available_source_family_count": len(
            [source for source in source_aggregation if source["availability"] == "AVAILABLE"]
        ),
        "metadata_only_source_family_count": len(
            [
                source
                for source in source_aggregation
                if source["availability"] == "AVAILABLE"
                and source["payload_read_status"] != "PAYLOAD_READ"
            ]
        ),
        "active_candidate_count": len(candidates["active_candidates"]),
        "rejected_candidate_count": len(candidates["rejected_candidates"]),
        "paper_shadow_candidate_count": len(candidates["paper_shadow_candidates"]),
        "needs_evidence_candidate_count": len(candidates["candidates_needing_evidence"]),
        "major_blocker_count": len(major_blockers),
        "major_warning_count": len(major_warnings),
        "safety_audit_status": safety_status,
        "data_governance_status": data_governance_status["status"],
        "owner_decision_status": owner_decision_status["status"],
        "manual_owner_review_required": True,
        "production_effect": PRODUCTION_EFFECT,
    }
    monthly_reader_brief = _monthly_reader_brief(
        status=status,
        summary=summary,
        major_blockers=major_blockers,
        major_warnings=major_warnings,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "monthly_review_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Aggregate candidate research, paper-shadow, safety, owner decision, "
            "cost, benchmark, lineage, and data-governance evidence into a monthly "
            "manual research governance review pack."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": status,
        "summary": summary,
        "candidate_summary": candidates["candidate_summary"],
        "active_candidates": candidates["active_candidates"],
        "rejected_candidates": candidates["rejected_candidates"],
        "paper_shadow_candidates": candidates["paper_shadow_candidates"],
        "candidates_needing_evidence": candidates["candidates_needing_evidence"],
        "major_blockers": major_blockers,
        "major_warnings": major_warnings,
        "safety_audit_status": {
            "status": safety_status,
            "source": _source_by_id(source_aggregation, "safety_boundary_audits"),
        },
        "data_governance_status": data_governance_status,
        "owner_decision_status": owner_decision_status,
        "source_aggregation": source_aggregation,
        "monthly_reader_brief": monthly_reader_brief,
        "reader_brief": monthly_reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The pack reads report index latest pointers and existing source artifacts only.",
            (
                "Validation artifacts prove source-family visibility but do not replace "
                "source reports."
            ),
            (
                "Candidate blockers are review conclusions; they do not create automatic "
                "rejection or promotion."
            ),
            "Owner decision audit log may be empty when no owner decision has been recorded yet.",
        ],
        "next_action": _next_action(status, major_blockers, major_warnings),
        "methodology": {
            "collector_mode": "read_existing_report_index_and_source_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_modify_strategy_outputs": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_research_monthly_review_pack_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    source_aggregation = _records(payload.get("source_aggregation"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_research_monthly_review_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "monthly review pack must be production_effect=none.",
        "restore_research_only_production_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_source_families_present",
        set(REQUIRED_SOURCE_IDS).issubset(
            {_text(source.get("source_id")) for source in source_aggregation}
        ),
        "Every required source family must be represented in source_aggregation.",
        "regenerate_pack_with_all_required_source_families",
    )
    missing_required = [
        source
        for source in source_aggregation
        if source.get("required") is True and source.get("availability") != "AVAILABLE"
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_source_artifacts_available",
        not missing_required,
        "Required source families must have a report-index artifact pointer.",
        "run_or_repair_missing_required_source_reports_before_monthly_review",
        details={
            "missing_source_ids": [
                _text(source.get("source_id")) for source in missing_required
            ]
        },
    )
    monthly_reader_brief = _mapping(payload.get("monthly_reader_brief"))
    has_core_reader_brief_sections = all(
        bool(_text(monthly_reader_brief.get(section)))
        for section in CORE_READER_BRIEF_SECTIONS
    )
    _append_check(
        checks,
        blocking_issues,
        "monthly_reader_brief_core_sections",
        has_core_reader_brief_sections,
        (
            "monthly_reader_brief must include Summary, Key Result, Blocking Issues, "
            "Warnings, Safety Boundary, and Next Action fields."
        ),
        "restore_monthly_reader_brief_core_sections",
    )
    _append_check(
        checks,
        blocking_issues,
        "candidate_summary_present",
        isinstance(payload.get("candidate_summary"), Mapping)
        and "active_candidates" in payload,
        "Candidate summary and candidate sections must be present.",
        "restore_candidate_summary_sections",
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("official_target_weights_generated") is False
            and safety.get("strategy_outputs_mutated") is False
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        (
            "Safety boundary must block strategy, candidate, paper-shadow, broker, "
            "order, and production mutation."
        ),
        "restore_monthly_pack_read_only_safety_boundary",
    )
    unsafe_source_effects = [
        source
        for source in source_aggregation
        if source.get("availability") == "AVAILABLE"
        and _text(source.get("production_effect"), PRODUCTION_EFFECT) != PRODUCTION_EFFECT
    ]
    _append_check(
        checks,
        blocking_issues,
        "source_production_effect_none",
        not unsafe_source_effects,
        "Available source artifacts must expose production_effect=none.",
        "repair_unsafe_source_artifact_before_monthly_review",
        details={
            "unsafe_source_ids": [
                _text(source.get("source_id")) for source in unsafe_source_effects
            ]
        },
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_audit_not_safety_blocked",
        _text(summary.get("safety_audit_status")) != "SAFETY_BLOCKED",
        "Research safety boundary audit must not be SAFETY_BLOCKED.",
        "resolve_research_safety_boundary_blocker_before_monthly_review",
    )

    metadata_only_sources = [
        source
        for source in source_aggregation
        if source.get("availability") == "AVAILABLE"
        and source.get("payload_read_status") != "PAYLOAD_READ"
    ]
    if metadata_only_sources:
        warning_issues.append(
            {
                "issue_id": "metadata_only_source_payloads",
                "source_ids": [_text(source.get("source_id")) for source in metadata_only_sources],
                "message": "Some source families were available only as report-index metadata.",
                "recommended_action": "confirm_source_report_json_sidecars_before_manual_review",
            }
        )
    if _text(payload.get("monthly_review_status")) != READY_STATUS:
        warning_issues.append(
            {
                "issue_id": "monthly_pack_contains_review_blockers_or_warnings",
                "monthly_review_status": _text(payload.get("monthly_review_status")),
                "message": (
                    "Monthly pack is structurally valid but contains review blockers "
                    "or warnings."
                ),
                "recommended_action": _text(payload.get("next_action")),
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = (
            PASS_WITH_WARNINGS_STATUS
            if warning_issues
            or _int(summary.get("major_warning_count")) > 0
            or _int(summary.get("major_blocker_count")) > 0
            else PASS_STATUS
        )
    validation_summary = {
        "check_count": len(checks),
        "failed_check_count": len(
            [check for check in checks if check["status"] == FAIL_STATUS]
        ),
        "warning_check_count": len(warning_issues),
        "blocking_issue_count": len(blocking_issues),
        "source_family_count": len(source_aggregation),
        "required_source_family_count": len(REQUIRED_SOURCE_IDS),
        "source_major_blocker_count": _int(summary.get("major_blocker_count")),
        "source_major_warning_count": _int(summary.get("major_warning_count")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_monthly_review_status": _text(
            payload.get("monthly_review_status"),
            _text(payload.get("status"), "UNKNOWN"),
        ),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Validate monthly research review pack schema, source coverage, and "
            "safety boundary."
        ),
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "source_summary": dict(summary),
        "source_major_blockers": _records(payload.get("major_blockers")),
        "source_major_warnings": _records(payload.get("major_warnings")),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not rerun upstream source reports.",
            (
                "PASS_WITH_WARNINGS means the pack is usable for manual review but "
                "contains disclosed candidate or governance limitations."
            ),
        ],
        "next_action": (
            "use_monthly_pack_for_manual_owner_review"
            if validation_status != FAIL_STATUS
            else "repair_monthly_pack_source_or_safety_failures_before_owner_review"
        ),
        "reader_brief": _monthly_reader_brief(
            status=_text(payload.get("monthly_review_status"), "UNKNOWN"),
            summary=summary,
            major_blockers=_records(payload.get("major_blockers")),
            major_warnings=_records(payload.get("major_warnings")),
        ),
        "methodology": {
            "collector_mode": "validate_existing_monthly_review_pack_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_outputs": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_research_monthly_review_pack_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_research_monthly_review_pack_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_research_monthly_review_pack_markdown(payload), encoding="utf-8")
    return output_path


def write_research_monthly_review_pack_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_research_monthly_review_pack_json(payload, output_path)


def write_research_monthly_review_pack_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_research_monthly_review_pack_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_research_monthly_review_pack_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Monthly Review Pack {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- monthly_review_status: {payload.get('monthly_review_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- active_candidates: {summary.get('active_candidate_count')}",
        f"- rejected_candidates: {summary.get('rejected_candidate_count')}",
        f"- paper_shadow_candidates: {summary.get('paper_shadow_candidate_count')}",
        f"- candidates_needing_evidence: {summary.get('needs_evidence_candidate_count')}",
        f"- major_blockers: {summary.get('major_blocker_count')}",
        f"- major_warnings: {summary.get('major_warning_count')}",
        f"- safety_audit_status: {summary.get('safety_audit_status')}",
        f"- data_governance_status: {summary.get('data_governance_status')}",
        f"- owner_decision_status: {summary.get('owner_decision_status')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Candidate Overview",
        "",
        "|section|candidate_count|candidates|",
        "|---|---:|---|",
    ]
    for section_id in (
        "active_candidates",
        "rejected_candidates",
        "paper_shadow_candidates",
        "candidates_needing_evidence",
    ):
        candidates = _records(payload.get(section_id))
        candidate_ids = ", ".join(
            _text(candidate.get("candidate_id")) for candidate in candidates
        )
        lines.append(
            f"|{section_id}|{len(candidates)}|"
            f"{_md_cell(candidate_ids)}|"
        )
    lines.extend(
        [
            "",
            "## Major Blockers",
            "",
            "|source_id|candidate_id|status|message|recommended_action|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("major_blockers")):
        lines.append(_issue_row(issue))
    lines.extend(
        [
            "",
            "## Major Warnings",
            "",
            "|source_id|candidate_id|status|message|recommended_action|",
            "|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("major_warnings")):
        lines.append(_issue_row(issue))
    lines.extend(
        [
            "",
            "## Source Aggregation",
            "",
            "|source_id|report_id|availability|status|candidate_id|payload|next_action|",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for source in _records(payload.get("source_aggregation")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("report_id"),
                    source.get("availability"),
                    source.get("source_status"),
                    source.get("candidate_id"),
                    source.get("payload_read_status"),
                    source.get("next_action"),
                )
            )
            + "|"
        )
    brief = _mapping(payload.get("monthly_reader_brief"))
    lines.extend(
        [
            "",
            "## Monthly Reader Brief",
            "",
            f"- Summary: {_text(brief.get('summary'))}",
            f"- Key Result: {_text(brief.get('key_result'))}",
            f"- Blocking Issues: {_text(brief.get('blocking_issues'))}",
            f"- Warnings: {_text(brief.get('warnings'))}",
            f"- Safety Boundary: {_text(brief.get('safety_boundary'))}",
            f"- Next Action: {_text(brief.get('next_action'))}",
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


def render_research_monthly_review_pack_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Research Monthly Review Pack Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_monthly_review_status: {payload.get('source_monthly_review_status')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- source_major_blockers: {summary.get('source_major_blocker_count')}",
        f"- source_major_warnings: {summary.get('source_major_warning_count')}",
        f"- next_action: {payload.get('next_action')}",
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
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|message|recommended_action|",
            "|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(
            f"|{_md_cell(issue.get('issue_id'))}|"
            f"{_md_cell(issue.get('message'))}|{_md_cell(issue.get('recommended_action'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _source_item(
    spec: Mapping[str, Any],
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    report_id = _text(spec.get("report_id"))
    source_id = _text(spec.get("source_id"))
    entry = _report_index_entry(report_index, report_id)
    artifact_path = _resolve_artifact_path(_text(entry.get("latest_artifact_path")), project_root)
    json_path, payload = _read_source_payload(
        artifact_path,
        _list_values(spec.get("preferred_json_names")),
    )
    availability = (
        "AVAILABLE" if artifact_path is not None and artifact_path.exists() else "MISSING"
    )
    payload_read_status = "PAYLOAD_READ" if payload else "METADATA_ONLY"
    if availability == "MISSING":
        payload_read_status = "MISSING"
    source_status = _source_status(spec, payload, entry)
    source_summary = _mapping(payload.get("summary"))
    candidate_id = _candidate_id(payload, source_summary)
    next_action = _source_next_action(spec, payload, source_summary)
    production_effect = _source_production_effect(payload, entry, availability)
    blocking = _source_blocking(spec, availability, source_status, payload)
    warning = _source_warning(spec, availability, payload_read_status, source_status, payload)
    return {
        "source_id": source_id,
        "label": _text(spec.get("label")),
        "source_group": _text(spec.get("source_group")),
        "report_id": report_id,
        "required": spec.get("required") is True,
        "availability": availability,
        "payload_read_status": payload_read_status,
        "source_status": source_status,
        "candidate_id": candidate_id,
        "next_action": next_action,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "source_payload_path": "" if json_path is None else str(json_path),
        "report_index_freshness_status": _text(entry.get("freshness_status")),
        "report_index_artifact_status": _text(entry.get("artifact_status")),
        "production_effect": production_effect,
        "blocking": blocking,
        "warning": warning,
        "blocking_reasons": _source_reasons(payload, source_summary, "blocking"),
        "warning_reasons": _source_reasons(payload, source_summary, "warning"),
        "source_summary": _compact_summary(source_summary),
    }


def _source_blocking(
    spec: Mapping[str, Any],
    availability: str,
    source_status: str,
    payload: Mapping[str, Any],
) -> bool:
    if availability != "AVAILABLE":
        return spec.get("blocking_if_missing") is True
    status = source_status.upper()
    source_id = _text(spec.get("source_id"))
    if any(
        marker in status
        for marker in SOURCE_SPECIFIC_BLOCKING_STATUS_MARKERS.get(source_id, ())
    ):
        return True
    if status in {"FAIL", "FAILED", "ERROR", "BLOCKING", "SAFETY_BLOCKED"}:
        return True
    if status.startswith("BLOCKED") or status.startswith("INSUFFICIENT"):
        return True
    if "BLOCKED" in status or "NO_VALID_SOURCE" in status:
        return True
    for field in (
        "blocking_issue_count",
        "blocking_count",
        "failed_check_count",
        "error_count",
        "missing_required_count",
        "checksum_mismatch_count",
        "cache_blocking_entry_count",
        "signal_input_blocking_count",
    ):
        if _int(payload.get(field), _int(_mapping(payload.get("summary")).get(field))) > 0:
            return True
    return False


def _source_warning(
    spec: Mapping[str, Any],
    availability: str,
    payload_read_status: str,
    source_status: str,
    payload: Mapping[str, Any],
) -> bool:
    if availability != "AVAILABLE":
        return spec.get("required") is True
    if payload_read_status != "PAYLOAD_READ":
        return True
    status = source_status.upper()
    if any(
        marker in status
        for marker in (
            "WARNING",
            "WARNINGS",
            "WARN",
            "MANUAL_REVIEW_REQUIRED",
            "STALE",
            "LIMITED",
            "UNKNOWN",
            "EMPTY",
        )
    ):
        return True
    for field in (
        "warning_issue_count",
        "warning_count",
        "validation_warning_count",
        "stale_count",
        "missing_count",
        "metadata_issue_count",
        "missing_metadata_count",
        "signal_input_warning_count",
    ):
        if _int(payload.get(field), _int(_mapping(payload.get("summary")).get(field))) > 0:
            return True
    return False


def _major_issues(
    source_aggregation: Sequence[Mapping[str, Any]],
    kind: str,
) -> list[dict[str, Any]]:
    flag = "blocking" if kind == "blocking" else "warning"
    issues: list[dict[str, Any]] = []
    for source in source_aggregation:
        if source.get(flag) is not True:
            continue
        source_id = _text(source.get("source_id"))
        reasons = _list_values(
            source.get("blocking_reasons") if kind == "blocking" else source.get("warning_reasons")
        )
        if not reasons:
            reasons = [
                (
                    "source_artifact_missing"
                    if source.get("availability") != "AVAILABLE"
                    else f"source_status={_text(source.get('source_status'), 'UNKNOWN')}"
                )
            ]
        issues.append(
            {
                "issue_id": f"{source_id}_{kind}",
                "source_id": source_id,
                "candidate_id": _text(source.get("candidate_id")),
                "status": _text(source.get("source_status"), "UNKNOWN"),
                "message": "; ".join(reasons[:3]),
                "recommended_action": _text(
                    source.get("next_action"),
                    (
                        "repair_source_before_monthly_review"
                        if kind == "blocking"
                        else "review_source_warning_before_owner_signoff"
                    ),
                ),
                "artifact_path": _text(source.get("artifact_path")),
                "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
            }
        )
    return issues


def _candidate_sections(source_aggregation: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_candidate: dict[str, dict[str, Any]] = {}
    for source in source_aggregation:
        candidate_id = _text(source.get("candidate_id"))
        if not candidate_id:
            continue
        entry = by_candidate.setdefault(
            candidate_id,
            {
                "candidate_id": candidate_id,
                "source_ids": [],
                "statuses": [],
                "next_actions": [],
                "blocker_count": 0,
                "warning_count": 0,
                "paper_shadow": False,
                "rejected": False,
                "needs_evidence": False,
            },
        )
        source_id = _text(source.get("source_id"))
        status = _text(source.get("source_status"), "UNKNOWN")
        entry["source_ids"].append(source_id)
        entry["statuses"].append(status)
        if _text(source.get("next_action")):
            entry["next_actions"].append(_text(source.get("next_action")))
        if source_id in PAPER_SHADOW_SOURCE_IDS:
            entry["paper_shadow"] = True
        if source.get("blocking") is True:
            entry["blocker_count"] += 1
            entry["needs_evidence"] = True
        if source.get("warning") is True:
            entry["warning_count"] += 1
            entry["needs_evidence"] = True
        status_blob = " ".join(
            [_text(source.get("source_status")), _text(source.get("next_action"))]
            + _list_values(source.get("blocking_reasons"))
            + _list_values(source.get("warning_reasons"))
        ).upper()
        if "REJECT" in status_blob:
            entry["rejected"] = True
        if any(
            marker in status_blob
            for marker in (
                "INSUFFICIENT",
                "MISSING",
                "STALE",
                "BLOCKED",
                "NEEDS_MORE_EVIDENCE",
                "PROVIDE_",
                "MANUAL_REVIEW_REQUIRED",
            )
        ):
            entry["needs_evidence"] = True

    candidate_summary = [
        {
            **entry,
            "source_ids": sorted(set(_list_values(entry.get("source_ids")))),
            "statuses": sorted(set(_list_values(entry.get("statuses")))),
            "next_actions": sorted(set(_list_values(entry.get("next_actions")))),
        }
        for entry in by_candidate.values()
    ]
    candidate_summary.sort(key=lambda item: _text(item.get("candidate_id")))
    rejected = [candidate for candidate in candidate_summary if candidate.get("rejected") is True]
    active = [
        candidate
        for candidate in candidate_summary
        if candidate.get("rejected") is not True
    ]
    paper_shadow = [
        candidate
        for candidate in candidate_summary
        if candidate.get("paper_shadow") is True
    ]
    needs_evidence = [
        candidate
        for candidate in candidate_summary
        if candidate.get("needs_evidence") is True
    ]
    return {
        "candidate_summary": {
            "candidate_count": len(candidate_summary),
            "status_counts": dict(
                Counter(
                    status
                    for candidate in candidate_summary
                    for status in candidate["statuses"]
                )
            ),
        },
        "active_candidates": active,
        "rejected_candidates": rejected,
        "paper_shadow_candidates": paper_shadow,
        "candidates_needing_evidence": needs_evidence,
    }


def _monthly_status(
    *,
    safety_status: str,
    data_governance_status: str,
    major_blockers: Sequence[Mapping[str, Any]],
    major_warnings: Sequence[Mapping[str, Any]],
) -> str:
    if safety_status == "SAFETY_BLOCKED":
        return BLOCKED_STATUS
    if data_governance_status == "BLOCKED":
        return BLOCKED_STATUS
    if major_blockers:
        return BLOCKED_STATUS
    if major_warnings:
        return WARN_STATUS
    return READY_STATUS


def _data_governance_status(source_aggregation: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sources = [
        source
        for source in source_aggregation
        if _text(source.get("source_id")) in DATA_GOVERNANCE_SOURCE_IDS
    ]
    blocking = [source for source in sources if source.get("blocking") is True]
    warning = [source for source in sources if source.get("warning") is True]
    status = "PASS"
    if blocking:
        status = "BLOCKED"
    elif warning:
        status = "PASS_WITH_WARNINGS"
    return {
        "status": status,
        "source_count": len(sources),
        "blocking_source_count": len(blocking),
        "warning_source_count": len(warning),
        "source_statuses": {
            _text(source.get("source_id")): _text(source.get("source_status"), "UNKNOWN")
            for source in sources
        },
        "next_action": (
            "resolve_data_governance_blockers_before_interpreting_candidate_evidence"
            if blocking
            else (
                "review_data_governance_warnings_before_owner_signoff"
                if warning
                else "data_governance_sources_clear_for_monthly_review"
            )
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _owner_decision_status(source_aggregation: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    source = _source_by_id(source_aggregation, "owner_decisions")
    status = _text(source.get("source_status"), "MISSING")
    return {
        "status": status,
        "availability": _text(source.get("availability"), "MISSING"),
        "artifact_path": _text(source.get("artifact_path")),
        "next_action": _text(source.get("next_action")),
        "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
    }


def _monthly_reader_brief(
    *,
    status: str,
    summary: Mapping[str, Any],
    major_blockers: Sequence[Mapping[str, Any]],
    major_warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            f"Monthly research review pack aggregates {summary.get('source_family_count')} "
            f"source families, {summary.get('active_candidate_count')} active candidate(s), "
            f"{summary.get('paper_shadow_candidate_count')} paper-shadow candidate(s), and "
            f"{summary.get('needs_evidence_candidate_count')} candidate(s) needing evidence."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not major_blockers
            else "; ".join(
                f"{_text(issue.get('source_id'))}:{_text(issue.get('status'))}"
                for issue in major_blockers[:5]
            )
        ),
        "warnings": (
            "none"
            if not major_warnings
            else "; ".join(
                f"{_text(issue.get('source_id'))}:{_text(issue.get('status'))}"
                for issue in major_warnings[:5]
            )
        ),
        "safety_boundary": (
            "Read-only monthly governance pack; no upstream rerun, no data refresh, "
            "no strategy/candidate/paper-shadow/production mutation, no official target weights, "
            "no broker/order, production_effect=none."
        ),
        "next_action": _next_action(status, major_blockers, major_warnings),
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_action(
    status: str,
    major_blockers: Sequence[Mapping[str, Any]],
    major_warnings: Sequence[Mapping[str, Any]],
) -> str:
    if status == BLOCKED_STATUS:
        has_safety_blocker = any(
            _text(issue.get("source_id")) == "safety_boundary_audits"
            for issue in major_blockers
        )
        if has_safety_blocker:
            return "resolve_research_safety_boundary_blocker_before_owner_review"
        return "resolve_monthly_review_blockers_or_record_owner_hold_decision"
    if major_warnings:
        return "review_monthly_pack_warnings_before_owner_signoff"
    return "use_monthly_pack_for_manual_owner_review"


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "strategy_outputs_mutated": False,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "official_target_weights_generated": False,
        "automatic_owner_approval": False,
        "automatic_candidate_promotion": False,
    }


def _report_index_entry(report_index: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(report_index.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return dict(report)
    return {}


def _read_source_payload(
    artifact_path: Path | None,
    preferred_json_names: Sequence[str],
) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates: list[Path] = []
    for name in preferred_json_names:
        candidates.append(artifact_path.parent / name)
    if artifact_path.suffix.lower() == ".json":
        candidates.append(artifact_path)
    else:
        candidates.append(artifact_path.with_suffix(".json"))
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _source_status(
    spec: Mapping[str, Any],
    payload: Mapping[str, Any],
    report_index_entry: Mapping[str, Any],
) -> str:
    for field in _list_values(spec.get("status_fields")):
        value = _text(payload.get(field))
        if value:
            return value
        summary_value = _text(_mapping(payload.get("summary")).get(field))
        if summary_value:
            return summary_value
    for field in ("status", "validation_status", "freshness_status"):
        value = _text(report_index_entry.get(field))
        if value:
            return value
    return "MISSING" if not report_index_entry else "UNKNOWN"


def _source_next_action(
    spec: Mapping[str, Any],
    payload: Mapping[str, Any],
    source_summary: Mapping[str, Any],
) -> str:
    for field in _list_values(spec.get("next_action_fields")):
        value = _text(payload.get(field))
        if value:
            return value
        summary_value = _text(source_summary.get(field))
        if summary_value:
            return summary_value
    for section_id in ("monthly_review_pack_inputs", "promotion_board_inputs", "reader_brief"):
        value = _text(_mapping(payload.get(section_id)).get("next_action"))
        if value:
            return value
    return "review_source_before_monthly_owner_review"


def _source_production_effect(
    payload: Mapping[str, Any],
    report_index_entry: Mapping[str, Any],
    availability: str,
) -> str:
    if availability != "AVAILABLE":
        return "UNKNOWN"
    return _text(
        payload.get("production_effect"),
        _text(report_index_entry.get("production_effect"), PRODUCTION_EFFECT),
    )


def _source_reasons(
    payload: Mapping[str, Any],
    source_summary: Mapping[str, Any],
    kind: str,
) -> list[str]:
    keys = (
        ("blocking_reasons", "blocking_artifacts", "blocking_issues", "missing_required")
        if kind == "blocking"
        else ("warnings", "warning_issues", "stale_artifacts", "missing_artifacts")
    )
    reasons: list[str] = []
    for key in keys:
        reasons.extend(_reason_values(payload.get(key)))
        reasons.extend(_reason_values(source_summary.get(key)))
    if kind == "blocking":
        for field in (
            "signal_input_blocking_input_ids",
            "fallback_blocking_data_types",
            "cache_blocking_entry_ids",
        ):
            reasons.extend(f"{field}:{item}" for item in _list_values(payload.get(field)))
    return sorted(set(reason for reason in reasons if reason))


def _reason_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Mapping):
        text = _text(
            value.get("issue_id"),
            _text(value.get("reason"), _text(value.get("message"), "")),
        )
        return [text] if text else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        reasons: list[str] = []
        for item in value:
            reasons.extend(_reason_values(item))
        return reasons
    return [_text(value)] if _text(value) else []


def _candidate_id(payload: Mapping[str, Any], source_summary: Mapping[str, Any]) -> str:
    for key in ("candidate", "candidate_id"):
        value = _text(payload.get(key), _text(source_summary.get(key)))
        if value:
            return value
    for section_id in ("monthly_review_pack_inputs", "promotion_board_inputs"):
        section = _mapping(payload.get(section_id))
        for key in ("candidate", "candidate_id"):
            value = _text(section.get(key))
            if value:
                return value
    return ""


def _source_by_id(
    source_aggregation: Sequence[Mapping[str, Any]],
    source_id: str,
) -> dict[str, Any]:
    for source in source_aggregation:
        if _text(source.get("source_id")) == source_id:
            return dict(source)
    return {}


def _source_status_by_id(
    source_aggregation: Sequence[Mapping[str, Any]],
    source_id: str,
    *,
    default: str,
) -> str:
    return _text(_source_by_id(source_aggregation, source_id).get("source_status"), default)


def _compact_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[_text(key)] = value
        if len(compact) >= 20:
            break
    return compact


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


def _read_json_mapping(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON payload must be an object: {path}")
    return payload


def _read_optional_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_artifact_path(path_text: str, project_root: Path) -> Path | None:
    if not path_text:
        return None
    path = Path(path_text)
    if path.is_absolute():
        return path
    return project_root / path


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    if not output_dir.exists():
        return None
    pattern = re.compile(rf"^{re.escape(prefix)}(\d{{4}}-\d{{2}}-\d{{2}}){re.escape(suffix)}$")
    candidates: list[tuple[date, Path]] = []
    for path in output_dir.glob(f"{prefix}*{suffix}"):
        match = pattern.match(path.name)
        if not match:
            continue
        candidates.append((date.fromisoformat(match.group(1)), path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_md_cell(issue.get('source_id'))}|{_md_cell(issue.get('candidate_id'))}|"
        f"{_md_cell(issue.get('status'))}|{_md_cell(issue.get('message'))}|"
        f"{_md_cell(issue.get('recommended_action'))}|"
    )


def _md_cell(value: Any) -> str:
    text = _text(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _list_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_text(item) for item in value if _text(item)]
    return [_text(value)] if _text(value) else []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list, tuple, set)):
        return default
    return str(value)


def _int(value: Any, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return default
    return default
