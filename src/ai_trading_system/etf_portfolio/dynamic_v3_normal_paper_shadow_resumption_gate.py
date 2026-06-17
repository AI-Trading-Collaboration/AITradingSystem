from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import (
    dynamic_v3_readiness_health_recovery as recovery,
)
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.reports import owner_decision_audit_log as owner_log

DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "normal_paper_shadow_resumption_gate"
)
DEFAULT_OWNER_DECISION_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"

NORMAL_PAPER_SHADOW_RESUMPTION_STATUSES = (
    "RESUME_NORMAL_SHADOW_ALLOWED",
    "RESUME_NORMAL_SHADOW_WITH_WARNINGS",
    "RESUME_NORMAL_SHADOW_BLOCKED",
)
SAFE_OWNER_ACTIONS = (
    "hold",
    "keep_hold",
    "continue_normal_shadow",
    "approve_resume_normal_shadow",
    "return_to_research",
    "reject_candidate",
)
RESUMPTION_OWNER_ACTIONS = ("continue_normal_shadow", "approve_resume_normal_shadow")

NORMAL_PAPER_SHADOW_RESUMPTION_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "normal_paper_shadow_resumption_gate_only": True,
    "normal_paper_shadow_observation_only": True,
    "manual_owner_review_required_before_resumption": True,
    "promotion_board_allowed": False,
    "promotion_approval_allowed": False,
    "extended_shadow_allowed": False,
    "extended_shadow_approval_allowed": False,
    "live_trading_allowed": False,
    "official_target_weights_allowed": False,
    "official_target_weights_written": False,
    "paper_account_state_mutated": False,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "production_state_mutated": False,
}


def run_normal_paper_shadow_resumption_gate(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    readiness_health_recovery_id: str | None = None,
    readiness_health_recovery_dir: Path = recovery.DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
    owner_action: str | None = None,
    manual_owner_review_completed: bool = False,
    owner_decision_report_path: Path | None = None,
    owner_decision_reports_dir: Path = DEFAULT_OWNER_DECISION_REPORTS_DIR,
    owner_decision_log_path: Path = owner_log.DEFAULT_OWNER_DECISION_AUDIT_LOG_PATH,
    output_dir: Path = DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()

    recovery_payload = recovery.readiness_health_recovery_report_payload(
        recovery_id=readiness_health_recovery_id,
        latest=readiness_health_recovery_id is None,
        output_dir=readiness_health_recovery_dir,
    )
    recovery_report = _mapping(recovery_payload.get("readiness_health_recovery_report"))
    source_statuses = _mapping(recovery_report.get("source_statuses"))
    source_validations = _mapping(recovery_report.get("source_validations"))
    owner_context = _owner_decision_context(
        as_of=effective_as_of,
        explicit_owner_action=owner_action,
        explicit_manual_review_completed=manual_owner_review_completed,
        owner_decision_report_path=owner_decision_report_path,
        owner_decision_reports_dir=owner_decision_reports_dir,
        owner_decision_log_path=owner_decision_log_path,
    )
    requirements = _resumption_requirements(
        recovery_payload=recovery_payload,
        recovery_report=recovery_report,
        owner_context=owner_context,
    )
    blocking_reasons = _blocking_reasons(requirements, owner_context)
    warning_reasons = _warning_reasons(requirements, recovery_report)
    final_status = _final_status(blocking_reasons, warning_reasons, owner_context)
    may_resume = final_status in {
        "RESUME_NORMAL_SHADOW_ALLOWED",
        "RESUME_NORMAL_SHADOW_WITH_WARNINGS",
    }
    gate_id = st._stable_id(
        "normal-paper-shadow-resumption-gate",
        candidate,
        effective_as_of.isoformat(),
        _text(recovery_report.get("recovery_id")),
        _text(owner_context.get("owner_action")),
        final_status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / gate_id)
    root.mkdir(parents=True, exist_ok=False)

    source_artifacts = {
        "readiness_health_recovery": {
            "artifact_id": _text(recovery_report.get("recovery_id")),
            "status": _text(recovery_report.get("readiness_health_recovery_status")),
            "report_path": _text(recovery_payload.get("readiness_health_recovery_report_path")),
            "validation_status": _text(
                _mapping(recovery_payload.get("readiness_health_recovery_validation")).get(
                    "status"
                ),
                "MISSING",
            ),
        },
        "owner_decision": owner_context,
    }
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_normal_paper_shadow_resumption_gate_report",
        "gate_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "normal_paper_shadow_resumption_gate_status": final_status,
        "normal_paper_shadow_may_resume": may_resume,
        "manual_owner_review_required_before_resumption": True,
        "manual_owner_review_completed": owner_context.get("manual_owner_review_completed")
        is True,
        "owner_action": _text(owner_context.get("owner_action")),
        "owner_action_is_safe_non_promotion": owner_context.get(
            "owner_action_is_safe_non_promotion"
        )
        is True,
        "owner_action_authorizes_normal_resumption": owner_context.get(
            "owner_action_authorizes_normal_resumption"
        )
        is True,
        "source_artifacts": source_artifacts,
        "source_statuses": source_statuses,
        "source_validations": source_validations,
        "readiness_health_recovery_id": _text(recovery_report.get("recovery_id")),
        "readiness_health_recovery_status": _text(
            recovery_report.get("readiness_health_recovery_status")
        ),
        "resumption_requirements": requirements,
        "blocking_reasons": blocking_reasons,
        "warning_reasons": warning_reasons,
        "next_required_action": _next_action(final_status, owner_context),
        "allowed_scope": "normal_paper_shadow_observation_only",
        "forbidden_scope": [
            "promotion",
            "extended_shadow",
            "live_trading",
            "official_target_weights",
            "broker_action",
            "order_ticket",
            "production_mutation",
            "automatic_position_control",
        ],
        "limitations": [
            "This gate can only permit normal paper-shadow observation after manual owner review.",
            "Owner action hold is safe and non-promotional, but it does not authorize resumption.",
            (
                "continue_normal_shadow or approve_resume_normal_shadow authorizes "
                "only normal observation, not extended shadow."
            ),
            (
                "The gate reads recovery and owner decision evidence; it does not "
                "rerun upstream data pipelines."
            ),
        ],
        **NORMAL_PAPER_SHADOW_RESUMPTION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_normal_paper_shadow_resumption_gate_manifest",
        "gate_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "normal_paper_shadow_resumption_gate_status": final_status,
        "normal_paper_shadow_may_resume": may_resume,
        "readiness_health_recovery_id": _text(recovery_report.get("recovery_id")),
        "owner_action": _text(owner_context.get("owner_action")),
        "normal_paper_shadow_resumption_gate_manifest_path": str(
            root / "normal_paper_shadow_resumption_gate_manifest.json"
        ),
        "normal_paper_shadow_resumption_gate_report_path": str(
            root / "normal_paper_shadow_resumption_gate_report.json"
        ),
        "normal_paper_shadow_resumption_gate_markdown_path": str(
            root / "normal_paper_shadow_resumption_gate_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "normal_paper_shadow_resumption_gate_validation.json"),
        **NORMAL_PAPER_SHADOW_RESUMPTION_SAFETY,
    }
    reader = render_normal_paper_shadow_resumption_gate_reader_brief(report)
    st._write_json(root / "normal_paper_shadow_resumption_gate_manifest.json", manifest)
    st._write_json(root / "normal_paper_shadow_resumption_gate_report.json", report)
    st._write_text(
        root / "normal_paper_shadow_resumption_gate_report.md",
        render_normal_paper_shadow_resumption_gate_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_normal_paper_shadow_resumption_gate",
        root.name,
        root / "normal_paper_shadow_resumption_gate_manifest.json",
    )
    validation = validate_normal_paper_shadow_resumption_gate_artifact(
        gate_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "gate_id": root.name,
        "gate_dir": root,
        "manifest": manifest,
        "normal_paper_shadow_resumption_gate_report": report,
        "reader_brief_section": reader,
        "normal_paper_shadow_resumption_gate_validation": validation,
    }


def normal_paper_shadow_resumption_gate_report_payload(
    *,
    gate_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=gate_id,
        latest_pointer="latest_normal_paper_shadow_resumption_gate",
        latest=latest,
        output_dir=output_dir,
        required_name="normal_paper_shadow_resumption_gate_manifest.json",
    )
    payload = {
        **st._read_json(root / "normal_paper_shadow_resumption_gate_manifest.json"),
        "normal_paper_shadow_resumption_gate_report": st._read_json(
            root / "normal_paper_shadow_resumption_gate_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "gate_dir": str(root),
    }
    validation = st._read_optional_json(
        root / "normal_paper_shadow_resumption_gate_validation.json"
    )
    if validation:
        payload["normal_paper_shadow_resumption_gate_validation"] = validation
    return payload


def validate_normal_paper_shadow_resumption_gate_artifact(
    *,
    gate_id: str,
    output_dir: Path = DEFAULT_NORMAL_PAPER_SHADOW_RESUMPTION_GATE_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / gate_id
    manifest = (
        st._read_optional_json(root / "normal_paper_shadow_resumption_gate_manifest.json")
        or {}
    )
    report = (
        st._read_optional_json(root / "normal_paper_shadow_resumption_gate_report.json")
        or {}
    )
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    status = _text(report.get("normal_paper_shadow_resumption_gate_status"))
    requirements = _records(report.get("resumption_requirements"))
    owner_context = _mapping(_mapping(report.get("source_artifacts")).get("owner_decision"))
    recomputed_blockers = _blocking_reasons(requirements, owner_context)
    recomputed_status = _final_status(
        recomputed_blockers,
        _texts(report.get("warning_reasons")),
        owner_context,
    )
    may_resume = status in {
        "RESUME_NORMAL_SHADOW_ALLOWED",
        "RESUME_NORMAL_SHADOW_WITH_WARNINGS",
    }
    checks = st._required_file_checks(
        root,
        (
            "normal_paper_shadow_resumption_gate_manifest.json",
            "normal_paper_shadow_resumption_gate_report.json",
            "normal_paper_shadow_resumption_gate_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("gate_id_matches", manifest.get("gate_id") == gate_id, ""),
            st._check(
                "status_enum_valid",
                status in NORMAL_PAPER_SHADOW_RESUMPTION_STATUSES,
                status,
            ),
            st._check(
                "final_status_consistent",
                status == recomputed_status,
                f"{status} != {recomputed_status}",
            ),
            st._check(
                "requirements_visible",
                {
                    "signal_input_completeness_not_blocking",
                    "evidence_staleness_not_blocking",
                    "readiness_not_blocked",
                    "canonical_health_not_blocked",
                    "safety_boundary_not_blocked",
                    "owner_action_safe_non_promotion",
                    "manual_owner_review_before_resumption",
                    "owner_action_authorizes_normal_resumption",
                }.issubset({str(row.get("requirement_id")) for row in requirements}),
                "",
            ),
            st._check(
                "resume_flag_matches_status",
                report.get("normal_paper_shadow_may_resume") is may_resume,
                status,
            ),
            st._check(
                "manual_review_completed_before_non_blocked_status",
                status == "RESUME_NORMAL_SHADOW_BLOCKED"
                or report.get("manual_owner_review_completed") is True,
                status,
            ),
            st._check(
                "owner_action_authorizes_normal_before_non_blocked_status",
                status == "RESUME_NORMAL_SHADOW_BLOCKED"
                or report.get("owner_action") in RESUMPTION_OWNER_ACTIONS,
                _text(report.get("owner_action")),
            ),
            st._check(
                "hold_does_not_resume",
                _text(report.get("owner_action")) != "hold"
                or status == "RESUME_NORMAL_SHADOW_BLOCKED",
                status,
            ),
            st._check(
                "forbidden_scope_locked",
                report.get("promotion_board_allowed") is False
                and report.get("promotion_approval_allowed") is False
                and report.get("extended_shadow_allowed") is False
                and report.get("extended_shadow_approval_allowed") is False
                and report.get("live_trading_allowed") is False
                and report.get("official_target_weights_allowed") is False
                and report.get("official_target_weights_written") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
            st._check(
                "reader_brief_quality_fields",
                "normal_paper_shadow_resumption_gate_status" in reader
                and "normal_paper_shadow_may_resume" in reader
                and "owner_action" in reader
                and "next_required_action" in reader,
                "",
            ),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_normal_paper_shadow_resumption_gate_validation",
        gate_id,
        checks,
    )
    if write_output:
        st._write_json(root / "normal_paper_shadow_resumption_gate_validation.json", validation)
        st._write_text(
            root / "normal_paper_shadow_resumption_gate_validation.md",
            render_normal_paper_shadow_resumption_gate_validation_report(validation),
        )
    return validation


def render_normal_paper_shadow_resumption_gate_reader_brief(
    report: Mapping[str, Any],
) -> str:
    statuses = _mapping(report.get("source_statuses"))
    return "\n".join(
        [
            "## Normal Paper Shadow Resumption Gate",
            "",
            f"- normal_paper_shadow_resumption_gate_id: {report.get('gate_id')}",
            "- normal_paper_shadow_resumption_gate_status: "
            f"{report.get('normal_paper_shadow_resumption_gate_status')}",
            f"- normal_paper_shadow_may_resume: {report.get('normal_paper_shadow_may_resume')}",
            f"- owner_action: {report.get('owner_action')}",
            "- manual_owner_review_completed: "
            f"{report.get('manual_owner_review_completed')}",
            f"- readiness_health_recovery_id: {report.get('readiness_health_recovery_id')}",
            "- readiness_health_recovery_status: "
            f"{report.get('readiness_health_recovery_status')}",
            f"- signal_input_status: {statuses.get('signal_input_status')}",
            f"- evidence_freshness_status: {statuses.get('evidence_freshness_status')}",
            "- shadow_continuation_readiness: "
            f"{statuses.get('shadow_continuation_readiness')}",
            f"- paper_shadow_health_status: {statuses.get('paper_shadow_health_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warning_reasons: {_joined_texts(report.get('warning_reasons'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: normal observation only / no promotion / no extended shadow / "
            "no live trading / no official target / no broker / no production",
            "",
        ]
    )


def render_normal_paper_shadow_resumption_gate_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    requirement_lines = [
        (
            f"- {row.get('requirement_id')}: status={row.get('status')} "
            f"detail={row.get('detail')}"
        )
        for row in _records(report.get("resumption_requirements"))
    ]
    return "\n".join(
        [
            f"# Normal Paper Shadow Resumption Gate {manifest.get('gate_id')}",
            "",
            "## Purpose",
            (
                "Decide whether recovered readiness/health evidence and manual owner "
                "review permit normal paper-shadow observation to resume."
            ),
            "",
            "## Summary",
            "- normal_paper_shadow_resumption_gate_status: "
            f"{report.get('normal_paper_shadow_resumption_gate_status')}",
            f"- normal_paper_shadow_may_resume: {report.get('normal_paper_shadow_may_resume')}",
            f"- owner_action: {report.get('owner_action')}",
            "- manual_owner_review_completed: "
            f"{report.get('manual_owner_review_completed')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warning_reasons: {_joined_texts(report.get('warning_reasons'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Requirements",
            *requirement_lines,
            "",
            "## Safety Boundary",
            "- normal paper-shadow observation only",
            "- no promotion approval or promotion board execution",
            "- no extended-shadow approval or protocol execution",
            "- no live trading",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_normal_paper_shadow_resumption_gate_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Normal Paper Shadow Resumption Gate Validation {validation.get('artifact_id')}",
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


def _owner_decision_context(
    *,
    as_of: date,
    explicit_owner_action: str | None,
    explicit_manual_review_completed: bool,
    owner_decision_report_path: Path | None,
    owner_decision_reports_dir: Path,
    owner_decision_log_path: Path,
) -> dict[str, Any]:
    if explicit_owner_action:
        action = _text(explicit_owner_action)
        return _owner_context_from_action(
            owner_action=action,
            source="explicit_cli_owner_action",
            manual_review_completed=explicit_manual_review_completed,
            owner_decision_id="",
            owner_decision_source_path="",
            audit_log_status="EXPLICIT_OWNER_ACTION",
            safety_status="",
        )

    report_path = owner_decision_report_path
    if report_path is None:
        report_path = owner_log.latest_owner_decision_audit_log_json_path(
            owner_decision_reports_dir
        )
    if report_path and report_path.exists():
        payload = st._read_json(report_path)
        summary = _mapping(payload.get("summary"))
        action = _text(summary.get("latest_owner_action"))
        return _owner_context_from_action(
            owner_action=action,
            source="owner_decision_audit_log_report",
            manual_review_completed=bool(summary.get("latest_decision_id")),
            owner_decision_id=_text(summary.get("latest_decision_id")),
            owner_decision_source_path=str(report_path),
            audit_log_status=_text(payload.get("audit_log_status"), _text(payload.get("status"))),
            safety_status=_text(summary.get("latest_safety_status")),
        )

    payload = owner_log.build_owner_decision_audit_log_payload(
        as_of=as_of,
        log_path=owner_decision_log_path,
    )
    summary = _mapping(payload.get("summary"))
    action = _text(summary.get("latest_owner_action"))
    return _owner_context_from_action(
        owner_action=action,
        source="owner_decision_audit_log_jsonl",
        manual_review_completed=bool(summary.get("latest_decision_id")),
        owner_decision_id=_text(summary.get("latest_decision_id")),
        owner_decision_source_path=str(owner_decision_log_path),
        audit_log_status=_text(payload.get("audit_log_status"), _text(payload.get("status"))),
        safety_status=_text(summary.get("latest_safety_status")),
    )


def _owner_context_from_action(
    *,
    owner_action: str,
    source: str,
    manual_review_completed: bool,
    owner_decision_id: str,
    owner_decision_source_path: str,
    audit_log_status: str,
    safety_status: str,
) -> dict[str, Any]:
    return {
        "source": source,
        "owner_action": owner_action,
        "owner_decision_id": owner_decision_id,
        "owner_decision_source_path": owner_decision_source_path,
        "audit_log_status": audit_log_status,
        "latest_safety_status": safety_status,
        "manual_owner_review_completed": manual_review_completed,
        "owner_action_is_safe_non_promotion": owner_action in SAFE_OWNER_ACTIONS,
        "owner_action_authorizes_normal_resumption": owner_action
        in RESUMPTION_OWNER_ACTIONS,
        "promotion_action_detected": bool(owner_action)
        and owner_action not in SAFE_OWNER_ACTIONS,
    }


def _resumption_requirements(
    *,
    recovery_payload: Mapping[str, Any],
    recovery_report: Mapping[str, Any],
    owner_context: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_statuses = _mapping(recovery_report.get("source_statuses"))
    source_validations = _mapping(recovery_report.get("source_validations"))
    signal_status = _text(source_statuses.get("signal_input_status"), "MISSING")
    evidence_status = _text(source_statuses.get("evidence_freshness_status"), "MISSING")
    readiness_status = _text(source_statuses.get("shadow_continuation_readiness"), "MISSING")
    health_status = _text(source_statuses.get("paper_shadow_health_status"), "MISSING")
    source_validation_statuses = [_text(value, "MISSING") for value in source_validations.values()]
    safety_passed = (
        st._payload_safe(_mapping(recovery_payload), recovery_report)
        and source_validation_statuses
        and set(source_validation_statuses) == {"PASS"}
        and "BLOCKED_SAFETY" not in {readiness_status, health_status}
    )
    return [
        _requirement(
            "signal_input_completeness_not_blocking",
            signal_status in {"OK", "WARNING"},
            "WARNING" if signal_status == "WARNING" else "PASS",
            f"signal_input_status={signal_status}",
        ),
        _requirement(
            "evidence_staleness_not_blocking",
            evidence_status not in {"BLOCKING", "MISSING", ""},
            "WARNING" if evidence_status == "ACCEPTABLE" else "PASS",
            f"evidence_freshness_status={evidence_status}",
        ),
        _requirement(
            "readiness_not_blocked",
            bool(readiness_status) and not readiness_status.startswith("BLOCKED_"),
            "WARNING"
            if readiness_status in {"READY_WITH_WARNINGS", "MANUAL_REVIEW_REQUIRED"}
            else "PASS",
            f"shadow_continuation_readiness={readiness_status}",
        ),
        _requirement(
            "canonical_health_not_blocked",
            bool(health_status) and not health_status.startswith("BLOCKED_"),
            "WARNING"
            if health_status in {"HEALTHY_WITH_WARNINGS", "MANUAL_REVIEW_REQUIRED"}
            else "PASS",
            f"paper_shadow_health_status={health_status}",
        ),
        _requirement(
            "safety_boundary_not_blocked",
            safety_passed,
            "PASS",
            "normal gate safety fields are locked and source validations are not FAIL",
        ),
        _requirement(
            "owner_action_safe_non_promotion",
            owner_context.get("owner_action_is_safe_non_promotion") is True,
            "PASS",
            f"owner_action={_text(owner_context.get('owner_action'), 'MISSING')}",
        ),
        _requirement(
            "manual_owner_review_before_resumption",
            owner_context.get("manual_owner_review_completed") is True,
            "PASS",
            "manual owner review must be recorded before resumption",
        ),
        _requirement(
            "owner_action_authorizes_normal_resumption",
            owner_context.get("owner_action_authorizes_normal_resumption") is True,
            "PASS",
            (
                "continue_normal_shadow or approve_resume_normal_shadow is required "
                "to resume; hold/keep_hold and research/reject actions are safe but "
                "keep observation paused"
            ),
        ),
    ]


def _requirement(
    requirement_id: str,
    passed: bool,
    non_blocking_status: str,
    detail: str,
) -> dict[str, Any]:
    status = non_blocking_status if passed else "BLOCKED"
    return {
        "requirement_id": requirement_id,
        "status": status,
        "passed": passed,
        "detail": detail,
    }


def _final_status(
    blocking_reasons: list[str],
    warning_reasons: list[str],
    owner_context: Mapping[str, Any],
) -> str:
    if blocking_reasons:
        return "RESUME_NORMAL_SHADOW_BLOCKED"
    if (
        owner_context.get("owner_action_authorizes_normal_resumption") is not True
        or owner_context.get("manual_owner_review_completed") is not True
    ):
        return "RESUME_NORMAL_SHADOW_BLOCKED"
    if warning_reasons:
        return "RESUME_NORMAL_SHADOW_WITH_WARNINGS"
    return "RESUME_NORMAL_SHADOW_ALLOWED"


def _blocking_reasons(
    requirements: list[Mapping[str, Any]],
    owner_context: Mapping[str, Any],
) -> list[str]:
    reasons = [
        _text(row.get("requirement_id"))
        for row in requirements
        if _text(row.get("status")) == "BLOCKED"
    ]
    owner_action = _text(owner_context.get("owner_action"))
    if not owner_action:
        reasons.append("owner_action:missing_manual_owner_review")
    elif owner_action in {"hold", "keep_hold"}:
        reasons.append("owner_action:hold")
    elif owner_action not in SAFE_OWNER_ACTIONS:
        reasons.append("owner_action:not_allowed_for_normal_resumption")
    return sorted(set(reason for reason in reasons if reason))


def _warning_reasons(
    requirements: list[Mapping[str, Any]],
    recovery_report: Mapping[str, Any],
) -> list[str]:
    warnings = [
        _text(row.get("requirement_id"))
        for row in requirements
        if _text(row.get("status")) == "WARNING"
    ]
    warnings.extend(
        f"readiness_health_recovery:{item}"
        for item in _texts(recovery_report.get("warning_reasons"))
    )
    return sorted(set(warning for warning in warnings if warning))


def _next_action(status: str, owner_context: Mapping[str, Any]) -> str:
    owner_action = _text(owner_context.get("owner_action"))
    if status == "RESUME_NORMAL_SHADOW_ALLOWED":
        return "resume_normal_paper_shadow_observation_only"
    if status == "RESUME_NORMAL_SHADOW_WITH_WARNINGS":
        return "resume_normal_paper_shadow_observation_only_with_warning_monitoring"
    if not owner_action:
        return "record_manual_owner_review_before_normal_shadow_resumption"
    if owner_action in {"hold", "keep_hold"}:
        return (
            "keep_normal_paper_shadow_on_hold_until_owner_records_"
            "approve_resume_normal_shadow"
        )
    if owner_action in {"return_to_research", "reject_candidate"}:
        return "owner_decision_moves_candidate_out_of_normal_resumption_path"
    if owner_action not in SAFE_OWNER_ACTIONS:
        return (
            "replace_promotion_or_extended_shadow_action_with_hold_or_"
            "approve_resume_normal_shadow"
        )
    return "clear_blocking_resumption_requirements_before_normal_shadow_resumption"


def _joined_texts(value: object, sep: str = ", ") -> str:
    return sep.join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
