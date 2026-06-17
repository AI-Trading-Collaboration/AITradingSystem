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
REPORT_TYPE = "recovery_evidence_pack"
VALIDATION_REPORT_TYPE = "recovery_evidence_pack_validation"

RECOVERY_EVIDENCE_COMPLETE = "RECOVERY_EVIDENCE_COMPLETE"
RECOVERY_EVIDENCE_PARTIAL = "RECOVERY_EVIDENCE_PARTIAL"
RECOVERY_EVIDENCE_BLOCKED = "RECOVERY_EVIDENCE_BLOCKED"
RECOVERY_EVIDENCE_STATUSES = (
    RECOVERY_EVIDENCE_COMPLETE,
    RECOVERY_EVIDENCE_PARTIAL,
    RECOVERY_EVIDENCE_BLOCKED,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

REQUIRED_SECTIONS = (
    "source_reports",
    "remaining_recovery_blockers",
    "warning_items",
    "next_actions",
    "reader_brief",
    "safety_boundary",
)

SOURCE_REPORT_SPECS: tuple[dict[str, Any], ...] = (
    {
        "source_id": "signal_input_completeness_recovery",
        "report_id": "etf_dynamic_v3_signal_input_completeness_recovery",
        "label": "Signal input completeness recovery",
        "preferred_json_names": (
            "signal_input_completeness_recovery_report.json",
            "signal_input_completeness_recovery_manifest.json",
        ),
        "validation_json_names": ("signal_input_completeness_recovery_validation.json",),
        "status_fields": ("recovery_status", "restoration_status", "signal_input_status"),
        "pass_statuses": ("SIGNAL_INPUTS_RESTORED",),
        "warning_markers": ("SIGNAL_INPUTS_RESTORED_WITH_WARNINGS", "WARNING"),
        "block_markers": ("SIGNAL_INPUTS_STILL_BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "signal_input_completeness",
        "report_id": "etf_dynamic_v3_signal_input_completeness",
        "label": "Signal input completeness",
        "preferred_json_names": (
            "signal_input_completeness_report.json",
            "signal_input_completeness_manifest.json",
        ),
        "validation_json_names": ("signal_input_completeness_validation.json",),
        "status_fields": ("signal_input_status", "status"),
        "pass_statuses": ("OK", "PASS"),
        "warning_markers": ("WARNING",),
        "block_markers": ("BLOCKING", "FAIL"),
    },
    {
        "source_id": "evidence_staleness",
        "report_id": "etf_dynamic_v3_evidence_staleness_monitor",
        "label": "Evidence staleness monitor",
        "preferred_json_names": (
            "evidence_staleness_report.json",
            "evidence_staleness_manifest.json",
        ),
        "validation_json_names": ("evidence_staleness_validation.json",),
        "status_fields": ("evidence_freshness_status", "staleness_status", "status"),
        "pass_statuses": ("ACCEPTABLE", "PASS"),
        "warning_markers": ("WARNING", "STALE_WITH_WARNINGS"),
        "block_markers": ("BLOCKED", "STALE_BLOCKING", "FAIL"),
    },
    {
        "source_id": "shadow_continuation_readiness",
        "report_id": "etf_dynamic_v3_shadow_continuation_readiness",
        "label": "Shadow continuation readiness",
        "preferred_json_names": (
            "shadow_continuation_readiness_report.json",
            "shadow_continuation_readiness_manifest.json",
        ),
        "validation_json_names": ("shadow_continuation_readiness_validation.json",),
        "status_fields": ("shadow_continuation_readiness", "readiness_status", "status"),
        "pass_statuses": ("READY_TO_CONTINUE",),
        "warning_markers": ("READY_WITH_WARNINGS", "MANUAL_REVIEW_REQUIRED", "WARNING"),
        "block_markers": ("BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "paper_shadow_health",
        "report_id": "etf_dynamic_v3_paper_shadow_health",
        "label": "Canonical paper-shadow health",
        "preferred_json_names": (
            "paper_shadow_health_report.json",
            "paper_shadow_health_manifest.json",
        ),
        "validation_json_names": ("paper_shadow_health_validation.json",),
        "status_fields": ("paper_shadow_health_status", "health_status", "status"),
        "pass_statuses": ("HEALTHY",),
        "warning_markers": ("HEALTHY_WITH_WARNINGS", "MANUAL_REVIEW_REQUIRED", "WARNING"),
        "block_markers": ("BLOCKED", "BLOCKING", "FAIL"),
    },
    {
        "source_id": "cost_metrics_materialization",
        "report_id": "etf_dynamic_v3_cost_metrics_materialization",
        "label": "Cost sensitivity metrics materialization",
        "preferred_json_names": (
            "cost_metrics_materialization_report.json",
            "cost_metrics_materialization_manifest.json",
        ),
        "validation_json_names": ("cost_metrics_materialization_validation.json",),
        "status_fields": ("cost_metrics_materialization_status", "status"),
        "pass_statuses": ("COST_INPUTS_AVAILABLE",),
        "warning_markers": ("COST_INPUTS_PARTIAL", "WARNING"),
        "block_markers": ("INSUFFICIENT_COST_INPUTS", "BLOCKED", "FAIL"),
    },
    {
        "source_id": "cost_sensitivity_review",
        "report_id": "etf_dynamic_v3_cost_sensitivity_review",
        "label": "Cost sensitivity review",
        "preferred_json_names": ("cost_sensitivity_review.json", "cost_sensitivity_manifest.json"),
        "validation_json_names": ("cost_sensitivity_validation.json",),
        "status_fields": ("cost_sensitivity_status", "status"),
        "pass_statuses": ("MEANINGFUL_ALL_SCENARIOS", "MEANINGFUL_LOW_MEDIUM_ONLY"),
        "warning_markers": ("LOW_MEDIUM_ONLY", "WARNING"),
        "block_markers": ("INSUFFICIENT", "NOT_MEANINGFUL", "BLOCKED", "FAIL"),
    },
    {
        "source_id": "benchmark_baseline_metrics_materialization",
        "report_id": "etf_dynamic_v3_benchmark_baseline_metrics_materialization",
        "label": "Benchmark baseline metrics materialization",
        "preferred_json_names": (
            "benchmark_baseline_metrics_materialization_report.json",
            "benchmark_baseline_metrics_materialization_manifest.json",
        ),
        "validation_json_names": (
            "benchmark_baseline_metrics_materialization_validation.json",
        ),
        "status_fields": ("benchmark_baseline_metrics_status", "status"),
        "pass_statuses": ("BASELINE_METRICS_AVAILABLE",),
        "warning_markers": ("BASELINE_METRICS_PARTIAL", "WARNING"),
        "block_markers": ("INSUFFICIENT_BASELINE_METRICS", "BLOCKED", "FAIL"),
    },
    {
        "source_id": "benchmark_baseline_control",
        "report_id": "etf_dynamic_v3_benchmark_baseline_control",
        "label": "Benchmark baseline control",
        "preferred_json_names": (
            "benchmark_baseline_control_pack.json",
            "benchmark_baseline_manifest.json",
        ),
        "validation_json_names": ("benchmark_baseline_validation.json",),
        "status_fields": ("benchmark_baseline_status", "status"),
        "pass_statuses": ("CANDIDATE_OUTPERFORMS_BASELINES",),
        "warning_markers": ("MIXED_BASELINE_RESULT", "WARNING"),
        "block_markers": ("INSUFFICIENT", "UNDERPERFORMS", "BLOCKED", "FAIL"),
    },
    {
        "source_id": "research_safety_boundary_audit",
        "report_id": "research_safety_boundary_audit",
        "validation_report_id": "research_safety_boundary_validation",
        "label": "Research safety boundary audit",
        "preferred_json_names": ("research_safety_boundary_audit.json",),
        "validation_json_names": ("research_safety_boundary_validation.json",),
        "status_fields": ("safety_status", "status"),
        "pass_statuses": ("SAFETY_PASS", "PASS"),
        "warning_markers": ("SAFETY_PASS_WITH_WARNINGS", "WARNING"),
        "block_markers": ("SAFETY_BLOCKED", "BLOCKED", "FAIL"),
    },
)


def default_recovery_evidence_pack_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_evidence_pack_{as_of.isoformat()}.json"


def default_recovery_evidence_pack_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"recovery_evidence_pack_{as_of.isoformat()}.md"


def default_recovery_evidence_pack_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_evidence_pack_validation_{as_of.isoformat()}.json"


def default_recovery_evidence_pack_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"recovery_evidence_pack_validation_{as_of.isoformat()}.md"


def latest_recovery_evidence_pack_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "recovery_evidence_pack_", ".json")


def build_recovery_evidence_pack_payload(
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

    source_reports = [
        _source_record(spec, report_index_payload, project_root=project_root)
        for spec in SOURCE_REPORT_SPECS
    ]
    structural_blockers = _structural_blockers(source_reports)
    partial_reasons = _partial_reasons(source_reports)
    remaining_blockers = _remaining_recovery_blockers(source_reports)
    warning_items = _warning_items(source_reports)
    pack_status = _pack_status(structural_blockers, partial_reasons)
    next_actions = _next_actions(structural_blockers, remaining_blockers, warning_items)
    summary = _summary(
        pack_status,
        source_reports,
        structural_blockers,
        partial_reasons,
        remaining_blockers,
        warning_items,
        next_actions,
    )
    reader_brief = _reader_brief(summary, remaining_blockers, warning_items)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": pack_status,
        "recovery_evidence_status": pack_status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "recovery_evidence_pack_only": True,
        "purpose": (
            "Consolidate post-recovery signal, readiness, health, cost, benchmark, "
            "and safety evidence for manual owner review."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            **{
                source["source_id"]: source.get("source_payload_path", "")
                for source in source_reports
            },
            **{
                f"{source['source_id']}_validation": source.get(
                    "validation_payload_path",
                    "",
                )
                for source in source_reports
            },
        },
        "summary": summary,
        "source_reports": source_reports,
        "structural_blockers": structural_blockers,
        "partial_reasons": partial_reasons,
        "remaining_recovery_blockers": remaining_blockers,
        "warning_items": warning_items,
        "next_actions": next_actions,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Evidence pack is read-only and does not run recovery commands.",
            "Complete evidence does not mean the candidate passed governance gates.",
            "Remaining source blockers must stay visible for owner review.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_report_index_and_latest_recovery_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_write_owner_decision": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "benchmark_comparison_live_signal": False,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_recovery_evidence_pack_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    source_reports = _records(payload.get("source_reports"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_recovery_evidence_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "recovery_evidence_status_enum",
        _text(payload.get("recovery_evidence_status")) in RECOVERY_EVIDENCE_STATUSES,
        "recovery_evidence_status must use the supported enum.",
        "restore_supported_recovery_evidence_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_source_count",
        len(source_reports) == len(SOURCE_REPORT_SPECS),
        "Recovery evidence pack must include every required source family.",
        "regenerate_pack_with_required_sources",
    )
    missing_sections = [
        section for section in REQUIRED_SECTIONS if not _section_present(payload, section)
    ]
    _append_check(
        checks,
        blocking_issues,
        "required_sections_present",
        not missing_sections,
        "Recovery evidence pack must include every required section.",
        "regenerate_pack_with_required_sections",
        details={"missing_sections": missing_sections},
    )
    structural_blockers = _records(payload.get("structural_blockers"))
    _append_check(
        checks,
        blocking_issues,
        "required_sources_readable",
        not structural_blockers,
        "Required recovery source artifacts and validations must be readable.",
        "run_or_repair_missing_recovery_sources_before_pack",
        details={
            "structural_blockers": [
                _text(issue.get("issue_id")) for issue in structural_blockers
            ]
        },
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("does_not_run_upstream_commands") is True
            and safety.get("does_not_refresh_data") is True
            and safety.get("does_not_generate_missing_artifacts") is True
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
            and safety.get("live_trading_allowed") is False
        ),
        "Recovery evidence pack must preserve the research-only safety boundary.",
        "restore_recovery_evidence_pack_safety_boundary",
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
            "Reader Brief section must expose summary, result, blockers, "
            "warnings, safety, and next action."
        ),
        "restore_recovery_evidence_reader_brief_fields",
    )
    if _int(summary.get("remaining_recovery_blocker_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_evidence_contains_remaining_source_blockers",
                "message": (
                    "Evidence pack is readable but source conclusions still block "
                    "governance progress."
                ),
                "recommended_action": _text(payload.get("next_action")),
            }
        )
    if _int(summary.get("warning_item_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_evidence_contains_source_warnings",
                "message": "Evidence pack contains source warnings requiring manual review.",
                "recommended_action": _text(payload.get("next_action")),
            }
        )
    if _int(summary.get("partial_reason_count")) > 0:
        warning_issues.append(
            {
                "issue_id": "recovery_evidence_partial_validation_coverage",
                "message": (
                    "Evidence pack is partial because at least one source validation "
                    "is missing."
                ),
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
        "source_report_count": len(source_reports),
        "available_source_count": _int(summary.get("available_source_count")),
        "remaining_recovery_blocker_count": _int(
            summary.get("remaining_recovery_blocker_count")
        ),
        "warning_item_count": _int(summary.get("warning_item_count")),
        "recovery_evidence_status": _text(payload.get("recovery_evidence_status")),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_recovery_evidence_status": _text(payload.get("recovery_evidence_status")),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "reader_brief": {
            "summary": (
                f"Recovery evidence pack validation is {validation_status}; "
                f"failed_checks={validation_summary['failed_check_count']}."
            ),
            "key_result": validation_status,
            "blocking_issues": (
                "none"
                if not blocking_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in blocking_issues[:5])
            ),
            "warnings": (
                "none"
                if not warning_issues
                else "; ".join(_text(issue.get("issue_id")) for issue in warning_issues[:5])
            ),
            "safety_boundary": "read-only recovery evidence validation; production_effect=none",
            "next_action": (
                "repair_recovery_evidence_pack"
                if validation_status == FAIL_STATUS
                else "review_recovery_evidence_pack_findings"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_recovery_evidence_pack_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_recovery_evidence_pack_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_recovery_evidence_pack_markdown(payload), encoding="utf-8")
    return output_path


def write_recovery_evidence_pack_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def write_recovery_evidence_pack_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_recovery_evidence_pack_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_recovery_evidence_pack_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Recovery Evidence Pack - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- recovery_evidence_status: {payload.get('recovery_evidence_status')}",
        f"- source_reports: {summary.get('source_report_count')}",
        f"- available_sources: {summary.get('available_source_count')}",
        f"- remaining_recovery_blockers: {summary.get('remaining_recovery_blocker_count')}",
        f"- warnings: {summary.get('warning_item_count')}",
        f"- next_action: {summary.get('next_action')}",
        f"- production_effect: {payload.get('production_effect')}",
        "",
        "## Source Reports",
        "",
        "|source_id|source_status|validation_status|conclusion|artifact|validation|",
        "|---|---|---|---|---|---|",
    ]
    for source in _records(payload.get("source_reports")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("source_status"),
                    source.get("validation_status"),
                    source.get("conclusion_status"),
                    source.get("source_payload_path"),
                    source.get("validation_payload_path"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Remaining Recovery Blockers", ""])
    if not _records(payload.get("remaining_recovery_blockers")):
        lines.append("- none")
    for blocker in _records(payload.get("remaining_recovery_blockers")):
        lines.append(_reason_line(blocker))
    lines.extend(["", "## Next Actions", ""])
    if not _records(payload.get("next_actions")):
        lines.append("- none")
    for action in _records(payload.get("next_actions")):
        lines.append(
            f"- {action.get('source_id')}: {action.get('recommended_action')} "
            f"({action.get('priority')})"
        )
    lines.extend(
        [
            "",
            "## Safety Boundary",
            "",
            "- evidence pack is advisory only.",
            f"- production_effect: {payload.get('production_effect')}",
            "- no upstream rerun, no data refresh, no missing-artifact fabrication, "
            "no owner decision write, no official target, no broker, no order ticket.",
            "",
        ]
    )
    return "\n".join(lines)


def render_recovery_evidence_pack_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Recovery Evidence Pack Validation - {_text(payload.get('as_of'), 'UNKNOWN')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_recovery_evidence_status: {payload.get('source_recovery_evidence_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed_checks: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- remaining_recovery_blockers: {summary.get('remaining_recovery_blocker_count')}",
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
    lines.append("")
    return "\n".join(lines)


def _source_record(
    spec: Mapping[str, Any],
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> dict[str, Any]:
    entry = _report_index_entry(report_index, _text(spec.get("report_id")))
    artifact_path = _resolve_artifact_path(_text(entry.get("latest_artifact_path")), project_root)
    source_payload_path, source_payload = _read_source_payload(
        artifact_path,
        _list_values(spec.get("preferred_json_names")),
    )
    validation_entry = entry
    validation_report_id = _text(spec.get("validation_report_id"))
    if validation_report_id:
        validation_entry = _report_index_entry(report_index, validation_report_id)
    validation_artifact_path = _resolve_artifact_path(
        _text(validation_entry.get("latest_artifact_path")),
        project_root,
    )
    validation_payload_path, validation_payload = _read_source_payload(
        validation_artifact_path,
        _list_values(spec.get("validation_json_names")),
        allow_artifact_fallback=bool(validation_report_id),
    )
    availability = "AVAILABLE" if source_payload_path is not None else "MISSING"
    validation_availability = (
        "AVAILABLE" if validation_payload_path is not None else "MISSING"
    )
    source_status = _source_status(spec, source_payload, entry)
    validation_status = _validation_status(validation_payload)
    conclusion_status = _conclusion_status(spec, availability, source_status)
    return {
        "source_id": _text(spec.get("source_id")),
        "report_id": _text(spec.get("report_id")),
        "label": _text(spec.get("label")),
        "availability": availability,
        "validation_availability": validation_availability,
        "source_status": source_status,
        "validation_status": validation_status,
        "conclusion_status": conclusion_status,
        "candidate_id": _candidate_id_from_payload(source_payload),
        "next_action": _next_action_from_payload(spec, source_payload),
        "latest_artifact_path": "" if artifact_path is None else str(artifact_path),
        "source_payload_path": (
            "" if source_payload_path is None else str(source_payload_path)
        ),
        "validation_payload_path": (
            "" if validation_payload_path is None else str(validation_payload_path)
        ),
        "production_effect": _text(
            source_payload.get("production_effect"),
            _text(entry.get("production_effect"), PRODUCTION_EFFECT),
        ),
        "validation_production_effect": _text(
            validation_payload.get("production_effect"),
            PRODUCTION_EFFECT,
        ),
        "summary": _compact_summary(_mapping(source_payload.get("summary"))),
        "validation_summary": _compact_summary(_mapping(validation_payload.get("summary"))),
    }


def _read_source_payload(
    artifact_path: Path | None,
    preferred_json_names: Sequence[str],
    *,
    allow_artifact_fallback: bool = True,
) -> tuple[Path | None, dict[str, Any]]:
    if artifact_path is None:
        return None, {}
    candidates = [artifact_path.parent / name for name in preferred_json_names]
    if allow_artifact_fallback:
        candidates.append(
            artifact_path
            if artifact_path.suffix == ".json"
            else artifact_path.with_suffix(".json")
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
    summary = _mapping(payload.get("summary"))
    for field in _list_values(spec.get("status_fields")):
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    for field in ("status", "freshness_status", "artifact_status"):
        value = _text(payload.get(field), _text(entry.get(field)))
        if value:
            return value
    return "MISSING" if not payload else "UNKNOWN"


def _validation_status(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    for field in ("validation_status", "status"):
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    return "MISSING" if not payload else "UNKNOWN"


def _conclusion_status(
    spec: Mapping[str, Any],
    availability: str,
    source_status: str,
) -> str:
    if availability != "AVAILABLE":
        return "MISSING"
    normalized = source_status.upper()
    if any(marker.upper() in normalized for marker in _list_values(spec.get("block_markers"))):
        return "BLOCKING"
    if any(
        marker.upper() in normalized for marker in _list_values(spec.get("warning_markers"))
    ):
        return "WARNING"
    if any(normalized == value.upper() for value in _list_values(spec.get("pass_statuses"))):
        return "PASS"
    if "PASS_WITH_WARNINGS" in normalized:
        return "WARNING"
    if "PASS" in normalized or normalized == "OK":
        return "PASS"
    return "WARNING"


def _structural_blockers(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        if _text(source.get("availability")) != "AVAILABLE":
            blockers.append(
                _issue(
                    f"{source_id}_missing",
                    source_id,
                    "BLOCKING",
                    "Required recovery source report is missing or unreadable.",
                    "run_or_restore_required_recovery_source",
                    source,
                )
            )
            continue
        validation_status = _text(source.get("validation_status"))
        if "FAIL" in validation_status.upper():
            blockers.append(
                _issue(
                    f"{source_id}_validation_failed",
                    source_id,
                    "BLOCKING",
                    f"Required source validation failed: {validation_status}.",
                    "repair_source_validation_before_recovery_pack",
                    source,
                )
            )
        if _text(source.get("production_effect")) != PRODUCTION_EFFECT:
            blockers.append(
                _issue(
                    f"{source_id}_unsafe_production_effect",
                    source_id,
                    "BLOCKING",
                    "Required source does not declare production_effect=none.",
                    "restore_source_safety_boundary",
                    source,
                )
            )
    return blockers


def _partial_reasons(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        if _text(source.get("availability")) == "AVAILABLE" and _text(
            source.get("validation_availability")
        ) != "AVAILABLE":
            reasons.append(
                _issue(
                    f"{source_id}_validation_missing",
                    source_id,
                    "PARTIAL",
                    "Source report is available but its validation artifact is missing.",
                    "run_source_validation_before_using_recovery_pack",
                    source,
                )
            )
    return reasons


def _remaining_recovery_blockers(
    source_reports: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for source in source_reports:
        if _text(source.get("conclusion_status")) == "BLOCKING":
            blockers.append(
                _issue(
                    f"{_text(source.get('source_id'))}_source_blocking",
                    _text(source.get("source_id")),
                    "BLOCKING",
                    (
                        f"{_text(source.get('label'))} conclusion remains blocking: "
                        f"{_text(source.get('source_status'))}."
                    ),
                    _text(source.get("next_action"), "review_recovery_source_blocker"),
                    source,
                )
            )
    return blockers


def _warning_items(source_reports: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    for source in source_reports:
        source_id = _text(source.get("source_id"))
        validation_status = _text(source.get("validation_status")).upper()
        if _text(source.get("conclusion_status")) == "WARNING":
            warnings.append(
                _issue(
                    f"{source_id}_source_warning",
                    source_id,
                    "WARNING",
                    (
                        f"{_text(source.get('label'))} requires review: "
                        f"{_text(source.get('source_status'))}."
                    ),
                    _text(source.get("next_action"), "review_recovery_source_warning"),
                    source,
                )
            )
        elif "WARNING" in validation_status:
            warnings.append(
                _issue(
                    f"{source_id}_validation_warning",
                    source_id,
                    "WARNING",
                    f"Source validation has warning status: {source.get('validation_status')}.",
                    "review_source_validation_warning",
                    source,
                )
            )
    return warnings


def _pack_status(
    structural_blockers: Sequence[Mapping[str, Any]],
    partial_reasons: Sequence[Mapping[str, Any]],
) -> str:
    if structural_blockers:
        return RECOVERY_EVIDENCE_BLOCKED
    if partial_reasons:
        return RECOVERY_EVIDENCE_PARTIAL
    return RECOVERY_EVIDENCE_COMPLETE


def _summary(
    pack_status: str,
    source_reports: Sequence[Mapping[str, Any]],
    structural_blockers: Sequence[Mapping[str, Any]],
    partial_reasons: Sequence[Mapping[str, Any]],
    remaining_blockers: Sequence[Mapping[str, Any]],
    warning_items: Sequence[Mapping[str, Any]],
    next_actions: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    validation_statuses = [_text(source.get("validation_status")) for source in source_reports]
    return {
        "recovery_evidence_status": pack_status,
        "source_report_count": len(source_reports),
        "available_source_count": len(
            [source for source in source_reports if source.get("availability") == "AVAILABLE"]
        ),
        "validation_available_count": len(
            [
                source
                for source in source_reports
                if source.get("validation_availability") == "AVAILABLE"
            ]
        ),
        "validation_pass_count": len(
            [status for status in validation_statuses if status in {"PASS", "OK"}]
        ),
        "validation_warning_count": len(
            [status for status in validation_statuses if "WARNING" in status.upper()]
        ),
        "structural_blocker_count": len(structural_blockers),
        "partial_reason_count": len(partial_reasons),
        "remaining_recovery_blocker_count": len(remaining_blockers),
        "warning_item_count": len(warning_items),
        "top_remaining_blocker": (
            _text(remaining_blockers[0].get("source_id")) if remaining_blockers else "none"
        ),
        "next_action": (
            _text(next_actions[0].get("recommended_action")) if next_actions else "none"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_actions(
    structural_blockers: Sequence[Mapping[str, Any]],
    remaining_blockers: Sequence[Mapping[str, Any]],
    warning_items: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    ordered = [*structural_blockers[:5], *remaining_blockers[:5], *warning_items[:3]]
    return [
        {
            "source_id": _text(item.get("source_id")),
            "priority": _text(item.get("severity"), "P1"),
            "reason": _text(item.get("message")),
            "recommended_action": _text(item.get("recommended_action")),
        }
        for item in ordered
    ]


def _reader_brief(
    summary: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    status = _text(summary.get("recovery_evidence_status"), RECOVERY_EVIDENCE_BLOCKED)
    return {
        "summary": (
            f"Recovery evidence pack is {status}; sources="
            f"{_int(summary.get('available_source_count'))}/"
            f"{_int(summary.get('source_report_count'))}, remaining_blockers="
            f"{_int(summary.get('remaining_recovery_blocker_count'))}."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if not blockers
            else "; ".join(
                f"{_text(blocker.get('source_id'))}:{_text(blocker.get('source_status'))}"
                for blocker in blockers[:5]
            )
        ),
        "warnings": (
            "none"
            if not warnings
            else "; ".join(
                f"{_text(warning.get('source_id'))}:{_text(warning.get('source_status'))}"
                for warning in warnings[:5]
            )
        ),
        "safety_boundary": (
            "Advisory recovery evidence only; no promotion, no live trading, no official "
            "target weights, no broker/order, production_effect=none."
        ),
        "next_action": _text(summary.get("next_action"), "review_recovery_evidence_pack"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _issue(
    issue_id: str,
    source_id: str,
    severity: str,
    message: str,
    recommended_action: str,
    source: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = source or {}
    return {
        "issue_id": issue_id,
        "source_id": source_id,
        "severity": severity,
        "message": message,
        "recommended_action": recommended_action,
        "source_status": _text(source.get("source_status")),
        "validation_status": _text(source.get("validation_status")),
        "artifact_path": _text(source.get("source_payload_path")),
        "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
    }


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("candidate", "candidate_id"):
        value = _text(payload.get(key))
        if value:
            return value
    for section_id in ("summary", "promotion_board_inputs", "monthly_review_pack_inputs"):
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
    reader_brief = _mapping(payload.get("reader_brief"))
    if _text(reader_brief.get("next_action")):
        return _text(reader_brief.get("next_action"))
    return f"review_{_text(spec.get('source_id'))}_before_recovery_pack_use"


def _compact_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            compact[_text(key)] = value
        if len(compact) >= 16:
            break
    return compact


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_report_index_and_recovery_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "advisory_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_write_owner_decision": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "automatic_owner_approval": False,
        "automatic_candidate_promotion": False,
        "extended_shadow_approved": False,
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


def _section_present(payload: Mapping[str, Any], section: str) -> bool:
    value = payload.get(section)
    if isinstance(value, (dict, list)):
        return True
    return bool(_text(value))


def _reason_line(reason: Mapping[str, Any]) -> str:
    return (
        f"- {reason.get('source_id')}: {reason.get('source_status')} "
        f"-> {reason.get('recommended_action')}"
    )
