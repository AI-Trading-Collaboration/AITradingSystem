from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.normal_paper_shadow_observation_clock import (
    OBSERVATION_PERIOD_MET,
    latest_normal_paper_shadow_observation_clock_json_path,
)
from ai_trading_system.reports.owner_decision_audit_log import (
    latest_owner_decision_audit_log_json_path,
)
from ai_trading_system.reports.remaining_blocker_resolution_ledger import (
    latest_remaining_blocker_resolution_ledger_json_path,
)
from ai_trading_system.reports.report_index_warning_cleanup import (
    latest_report_index_warning_cleanup_json_path,
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
REPORT_TYPE = "post_recovery_governance_pack"
VALIDATION_REPORT_TYPE = "post_recovery_governance_pack_validation"

POST_RECOVERY_HEALTHY = "POST_RECOVERY_HEALTHY"
POST_RECOVERY_HEALTHY_WITH_WARNINGS = "POST_RECOVERY_HEALTHY_WITH_WARNINGS"
POST_RECOVERY_MANUAL_REVIEW_REQUIRED = "POST_RECOVERY_MANUAL_REVIEW_REQUIRED"
POST_RECOVERY_BLOCKED = "POST_RECOVERY_BLOCKED"
POST_RECOVERY_STATUSES = (
    POST_RECOVERY_HEALTHY,
    POST_RECOVERY_HEALTHY_WITH_WARNINGS,
    POST_RECOVERY_MANUAL_REVIEW_REQUIRED,
    POST_RECOVERY_BLOCKED,
)

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


def default_post_recovery_governance_pack_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"post_recovery_governance_pack_{as_of.isoformat()}.json"


def default_post_recovery_governance_pack_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"post_recovery_governance_pack_{as_of.isoformat()}.md"


def default_post_recovery_governance_pack_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"post_recovery_governance_pack_validation_{as_of.isoformat()}.json"


def default_post_recovery_governance_pack_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"post_recovery_governance_pack_validation_{as_of.isoformat()}.md"


def latest_post_recovery_governance_pack_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "post_recovery_governance_pack_", ".json")


def build_post_recovery_governance_pack_payload(
    *,
    as_of: date,
    recovery_pack_payload: Mapping[str, Any] | None = None,
    recovery_pack_path: Path | None = None,
    blocker_ledger_payload: Mapping[str, Any] | None = None,
    blocker_ledger_path: Path | None = None,
    warning_cleanup_payload: Mapping[str, Any] | None = None,
    warning_cleanup_path: Path | None = None,
    normal_observation_clock_payload: Mapping[str, Any] | None = None,
    normal_observation_clock_path: Path | None = None,
    owner_decision_audit_log_payload: Mapping[str, Any] | None = None,
    owner_decision_audit_log_path: Path | None = None,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    recovery_pack_path, recovery_pack = _payload_or_latest(
        recovery_pack_payload,
        recovery_pack_path,
        latest_research_governance_recovery_pack_json_path,
        reports_dir,
        "research_governance_recovery_pack",
    )
    blocker_ledger_path, blocker_ledger = _payload_or_latest_optional(
        blocker_ledger_payload,
        blocker_ledger_path,
        latest_remaining_blocker_resolution_ledger_json_path,
        reports_dir,
    )
    warning_cleanup_path, warning_cleanup = _payload_or_latest_optional(
        warning_cleanup_payload,
        warning_cleanup_path,
        latest_report_index_warning_cleanup_json_path,
        reports_dir,
    )
    normal_observation_clock_path, normal_clock = _payload_or_latest_optional(
        normal_observation_clock_payload,
        normal_observation_clock_path,
        latest_normal_paper_shadow_observation_clock_json_path,
        reports_dir,
    )
    owner_decision_audit_log_path, owner_log = _payload_or_latest_optional(
        owner_decision_audit_log_payload,
        owner_decision_audit_log_path,
        latest_owner_decision_audit_log_json_path,
        reports_dir,
    )

    summary = _summary(recovery_pack, blocker_ledger, warning_cleanup, normal_clock, owner_log)
    status = _post_recovery_status(summary)
    source_statuses = _source_statuses(
        recovery_pack=recovery_pack,
        blocker_ledger=blocker_ledger,
        warning_cleanup=warning_cleanup,
        normal_clock=normal_clock,
        owner_log=owner_log,
    )
    reader_brief = _reader_brief(status, summary)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "post_recovery_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "input_artifacts": {
            "research_governance_recovery_pack": _path_text(recovery_pack_path),
            "remaining_blocker_resolution_ledger": _path_text(blocker_ledger_path),
            "report_index_warning_cleanup": _path_text(warning_cleanup_path),
            "normal_paper_shadow_observation_clock": _path_text(normal_observation_clock_path),
            "owner_decision_audit_log": _path_text(owner_decision_audit_log_path),
        },
        "summary": summary,
        "source_statuses": source_statuses,
        "remaining_blockers": _records(recovery_pack.get("remaining_blockers")),
        "remaining_warnings": _records(recovery_pack.get("remaining_warnings")),
        "normal_paper_shadow_boundary": dict(
            _mapping(recovery_pack.get("normal_paper_shadow_boundary"))
        ),
        "extended_shadow_boundary": dict(_mapping(recovery_pack.get("extended_shadow_boundary"))),
        "live_trading_boundary": dict(_mapping(recovery_pack.get("live_trading_boundary"))),
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Post-recovery pack is a read-only governance summary.",
            "Normal paper-shadow may resume only when the resumption gate is non-blocked.",
            "Extended shadow requires completed observation and separate protocol eligibility.",
            "Live trading remains forbidden.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_post_recovery_governance_pack_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    status = _text(payload.get("post_recovery_status"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_post_recovery_governance_pack",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        status in POST_RECOVERY_STATUSES,
        "post_recovery_status must use the supported enum.",
        "restore_post_recovery_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "summary_required_fields",
        all(
            field in summary
            for field in (
                "normal_paper_shadow_may_resume",
                "extended_shadow_remains_forbidden",
                "live_trading_remains_forbidden",
                "remaining_blocker_count",
                "remaining_warning_count",
                "next_owner_action",
            )
        ),
        "Summary must expose paper, extended, live, blocker, warning, and owner action fields.",
        "repair_post_recovery_summary",
    )
    _append_check(
        checks,
        blocking_issues,
        "live_trading_forbidden",
        summary.get("live_trading_remains_forbidden") is True,
        "Post-recovery pack must keep live trading forbidden.",
        "restore_post_recovery_live_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_post_recovery_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Post-recovery pack must preserve read-only governance safety boundary.",
        "restore_post_recovery_safety_boundary",
    )
    if status != POST_RECOVERY_HEALTHY:
        warning_issues.append(
            {
                "issue_id": "post_recovery_not_fully_healthy",
                "message": (
                    "Post-recovery governance still has blockers, warnings, "
                    "or manual review."
                ),
                "post_recovery_status": status,
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
        "post_recovery_status": status,
        "remaining_blocker_count": _int(summary.get("remaining_blocker_count")),
        "remaining_warning_count": _int(summary.get("remaining_warning_count")),
        "normal_paper_shadow_may_resume": summary.get("normal_paper_shadow_may_resume"),
        "extended_shadow_remains_forbidden": summary.get("extended_shadow_remains_forbidden"),
        "live_trading_remains_forbidden": summary.get("live_trading_remains_forbidden"),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_post_recovery_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Post-recovery governance validation is {validation_status}.",
            "key_result": validation_status,
            "blocking_issues": _issue_list(blocking_issues, "issue_id", "message"),
            "warnings": _issue_list(warning_issues, "issue_id", "message"),
            "safety_boundary": "read-only validation; live trading remains forbidden.",
            "next_action": (
                "repair_post_recovery_governance_pack"
                if validation_status == FAIL_STATUS
                else "review_post_recovery_governance_pack"
            ),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
    }


def write_post_recovery_governance_pack_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_post_recovery_governance_pack_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_post_recovery_governance_pack_markdown(payload), output_path)


def write_post_recovery_governance_pack_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_post_recovery_governance_pack_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(
        render_post_recovery_governance_pack_validation_markdown(payload),
        output_path,
    )


def render_post_recovery_governance_pack_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Post-Recovery Governance Pack {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- post_recovery_status: {payload.get('post_recovery_status')}",
        f"- recovery_governance_status: {summary.get('recovery_governance_status')}",
        f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
        f"- normal_observation_clock_status: {summary.get('normal_observation_clock_status')}",
        f"- extended_shadow_remains_forbidden: {summary.get('extended_shadow_remains_forbidden')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- remaining_blocker_count: {summary.get('remaining_blocker_count')}",
        f"- remaining_warning_count: {summary.get('remaining_warning_count')}",
        (
            "- report_index_unwaived_warning_count: "
            f"{summary.get('report_index_unwaived_warning_count')}"
        ),
        f"- latest_owner_action: {summary.get('latest_owner_action')}",
        f"- next_owner_action: {summary.get('next_owner_action')}",
        "",
        "## Source Statuses",
        "",
        "|source_id|status|key_result|artifact_path|",
        "|---|---|---|---|",
    ]
    for source in _records(payload.get("source_statuses")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("status"),
                    source.get("key_result"),
                    source.get("artifact_path"),
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Boundaries",
            "",
            "- normal paper-shadow resumes only if the normal resumption gate allows it.",
            "- extended shadow remains forbidden until observation and protocol gates are met.",
            "- live trading remains forbidden.",
            "",
        ]
    )
    return "\n".join(lines)


def render_post_recovery_governance_pack_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Post-Recovery Governance Pack Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_post_recovery_status: {payload.get('source_post_recovery_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- remaining_blocker_count: {summary.get('remaining_blocker_count')}",
        f"- remaining_warning_count: {summary.get('remaining_warning_count')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
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
    latest_fn: Callable[[Path], Path | None],
    reports_dir: Path,
    label: str,
) -> tuple[Path | None, Mapping[str, Any]]:
    if payload is not None:
        return path, payload
    source_path = path or latest_fn(reports_dir)
    if source_path is None:
        raise FileNotFoundError(f"{label} JSON not found in {reports_dir}")
    return source_path, _read_json_mapping(source_path)


def _payload_or_latest_optional(
    payload: Mapping[str, Any] | None,
    path: Path | None,
    latest_fn: Callable[[Path], Path | None],
    reports_dir: Path,
) -> tuple[Path | None, Mapping[str, Any]]:
    if payload is not None:
        return path, payload
    source_path = path or latest_fn(reports_dir)
    if source_path is None or not source_path.exists():
        return source_path, {}
    return source_path, _read_json_mapping(source_path)


def _summary(
    recovery_pack: Mapping[str, Any],
    blocker_ledger: Mapping[str, Any],
    warning_cleanup: Mapping[str, Any],
    normal_clock: Mapping[str, Any],
    owner_log: Mapping[str, Any],
) -> dict[str, Any]:
    pack_summary = _mapping(recovery_pack.get("summary"))
    cleanup_summary = _mapping(warning_cleanup.get("summary"))
    owner_summary = _mapping(owner_log.get("summary"))
    remaining_blocker_count = _int(
        pack_summary.get("remaining_blocker_count"),
        len(_records(blocker_ledger.get("blocker_resolution_ledger"))),
    )
    remaining_warning_count = _int(
        pack_summary.get("remaining_warning_count"),
        len(_records(blocker_ledger.get("warning_resolution_ledger"))),
    )
    normal_may_resume = bool(pack_summary.get("normal_paper_shadow_may_resume"))
    normal_clock_status = _text(
        normal_clock.get("normal_observation_clock_status"),
        "MISSING",
    )
    observation_met = normal_clock_status == OBSERVATION_PERIOD_MET
    extended_forbidden = (
        bool(pack_summary.get("extended_shadow_remains_forbidden", True))
        or not observation_met
        or remaining_blocker_count > 0
    )
    live_forbidden = True
    return {
        "recovery_governance_status": _text(recovery_pack.get("recovery_governance_status")),
        "normal_paper_shadow_may_resume": normal_may_resume,
        "normal_observation_clock_status": normal_clock_status,
        "normal_observation_period_met": observation_met,
        "extended_shadow_remains_forbidden": extended_forbidden,
        "live_trading_remains_forbidden": live_forbidden,
        "remaining_blocker_count": remaining_blocker_count,
        "remaining_warning_count": remaining_warning_count,
        "report_index_unwaived_warning_count": _int(
            cleanup_summary.get("remaining_unwaived_count"),
            _int(pack_summary.get("report_index_unwaived_warning_count")),
        ),
        "manual_review_item_count": _int(pack_summary.get("manual_review_item_count")),
        "latest_owner_action": _text(owner_summary.get("latest_owner_action")),
        "latest_decision_id": _text(owner_summary.get("latest_decision_id")),
        "next_owner_action": _text(
            pack_summary.get("next_owner_action"),
            _text(recovery_pack.get("next_owner_action"), "review_post_recovery_governance_pack"),
        ),
        "recovery_pack_validation_status": _text(
            pack_summary.get("recovery_pack_validation_status")
        ),
        "ledger_status": _text(blocker_ledger.get("ledger_status"), "MISSING"),
        "warning_cleanup_status": _text(warning_cleanup.get("cleanup_status"), "MISSING"),
        "production_effect": PRODUCTION_EFFECT,
    }


def _post_recovery_status(summary: Mapping[str, Any]) -> str:
    if (
        _int(summary.get("remaining_blocker_count")) > 0
        or summary.get("normal_paper_shadow_may_resume") is not True
    ):
        return POST_RECOVERY_BLOCKED
    if _int(summary.get("manual_review_item_count")) > 0:
        return POST_RECOVERY_MANUAL_REVIEW_REQUIRED
    if (
        _int(summary.get("remaining_warning_count")) > 0
        or _int(summary.get("report_index_unwaived_warning_count")) > 0
        or summary.get("normal_observation_period_met") is not True
    ):
        return POST_RECOVERY_HEALTHY_WITH_WARNINGS
    return POST_RECOVERY_HEALTHY


def _source_statuses(
    *,
    recovery_pack: Mapping[str, Any],
    blocker_ledger: Mapping[str, Any],
    warning_cleanup: Mapping[str, Any],
    normal_clock: Mapping[str, Any],
    owner_log: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows = [
        _source_row(
            "research_governance_recovery_pack",
            _text(recovery_pack.get("recovery_governance_status")),
            _text(_mapping(recovery_pack.get("summary")).get("next_owner_action")),
            _text(_mapping(recovery_pack.get("input_artifacts")).get("report_index")),
        ),
        _source_row(
            "remaining_blocker_resolution_ledger",
            _text(blocker_ledger.get("ledger_status"), "MISSING"),
            _text(blocker_ledger.get("next_action")),
            _text(_mapping(blocker_ledger.get("input_artifacts")).get("recovery_blocker_triage")),
        ),
        _source_row(
            "report_index_warning_cleanup",
            _text(warning_cleanup.get("cleanup_status"), "MISSING"),
            _text(warning_cleanup.get("next_action")),
            _text(_mapping(warning_cleanup.get("input_artifacts")).get("report_index")),
        ),
        _source_row(
            "normal_paper_shadow_observation_clock",
            _text(normal_clock.get("normal_observation_clock_status"), "MISSING"),
            _text(normal_clock.get("next_action")),
            _text(_mapping(normal_clock.get("input_artifacts")).get("report_index")),
        ),
        _source_row(
            "owner_decision_audit_log",
            _text(owner_log.get("audit_log_status"), "MISSING"),
            _text(_mapping(owner_log.get("summary")).get("latest_owner_action")),
            _text(_mapping(owner_log.get("input_artifacts")).get("owner_decision_audit_log")),
        ),
    ]
    for source in _records(recovery_pack.get("source_reports")):
        rows.append(
            _source_row(
                _text(source.get("source_id")),
                _text(source.get("source_status")),
                _text(source.get("next_required_action")),
                _text(source.get("source_payload_path")),
            )
        )
    return rows


def _source_row(
    source_id: str,
    status: str,
    key_result: str,
    artifact_path: str,
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "status": status or "UNKNOWN",
        "key_result": key_result,
        "artifact_path": artifact_path,
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(status: str, summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "summary": (
            f"Post-recovery governance is {status}: "
            f"{_int(summary.get('remaining_blocker_count'))} blockers and "
            f"{_int(summary.get('remaining_warning_count'))} warnings remain."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if status != POST_RECOVERY_BLOCKED
            else f"remaining_blockers={_int(summary.get('remaining_blocker_count'))}; "
            f"normal_paper_shadow_may_resume={summary.get('normal_paper_shadow_may_resume')}"
        ),
        "warnings": (
            "none"
            if _int(summary.get("remaining_warning_count")) == 0
            else f"remaining_warnings={_int(summary.get('remaining_warning_count'))}"
        ),
        "safety_boundary": (
            "Read-only governance pack; no official target weights, broker/order, "
            "paper-shadow mutation, production mutation, extended approval, or live trading."
        ),
        "next_action": _text(summary.get("next_owner_action")),
        "production_effect": PRODUCTION_EFFECT,
    }


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
        "mode": "read_existing_post_recovery_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "governance_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
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


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    check = {
        "check_id": check_id,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "message": message,
        "recommended_action": recommended_action,
    }
    checks.append(check)
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


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
