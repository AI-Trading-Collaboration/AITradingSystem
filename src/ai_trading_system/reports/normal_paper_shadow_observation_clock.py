from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.extended_shadow_protocol import (
    MINIMUM_OBSERVATION_TRADING_DAYS,
)
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
REPORT_TYPE = "normal_paper_shadow_observation_clock"
VALIDATION_REPORT_TYPE = "normal_paper_shadow_observation_clock_validation"

OBSERVATION_NOT_STARTED = "OBSERVATION_NOT_STARTED"
OBSERVATION_IN_PROGRESS = "OBSERVATION_IN_PROGRESS"
OBSERVATION_PERIOD_UNMET = "OBSERVATION_PERIOD_UNMET"
OBSERVATION_PERIOD_MET = "OBSERVATION_PERIOD_MET"
OBSERVATION_STATUSES = (
    OBSERVATION_NOT_STARTED,
    OBSERVATION_IN_PROGRESS,
    OBSERVATION_PERIOD_UNMET,
    OBSERVATION_PERIOD_MET,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

GATE_REPORT_ID = "etf_dynamic_v3_normal_paper_shadow_resumption_gate"
WEEKLY_REVIEW_REPORT_ID = "etf_dynamic_v3_paper_shadow_weekly_review"
GATE_JSON_NAMES = (
    "normal_paper_shadow_resumption_gate_report.json",
    "normal_paper_shadow_resumption_gate_manifest.json",
    "normal_paper_shadow_resumption_gate_validation.json",
)
WEEKLY_REVIEW_JSON_NAMES = (
    "paper_shadow_weekly_review.json",
    "paper_shadow_weekly_manifest.json",
    "paper_shadow_weekly_validation.json",
)
CORE_READER_BRIEF_FIELDS = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)


def default_normal_paper_shadow_observation_clock_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"normal_paper_shadow_observation_clock_{as_of.isoformat()}.json"


def default_normal_paper_shadow_observation_clock_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"normal_paper_shadow_observation_clock_{as_of.isoformat()}.md"


def default_normal_paper_shadow_observation_clock_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"normal_paper_shadow_observation_clock_validation_{as_of.isoformat()}.json"


def default_normal_paper_shadow_observation_clock_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"normal_paper_shadow_observation_clock_validation_{as_of.isoformat()}.md"


def latest_normal_paper_shadow_observation_clock_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "normal_paper_shadow_observation_clock_", ".json")


def build_normal_paper_shadow_observation_clock_payload(
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

    gate_source = _source_evidence(
        report_index_payload,
        GATE_REPORT_ID,
        "normal_paper_shadow_resumption_gate",
        GATE_JSON_NAMES,
        project_root=project_root,
    )
    weekly_source = _source_evidence(
        report_index_payload,
        WEEKLY_REVIEW_REPORT_ID,
        "paper_shadow_weekly_review",
        WEEKLY_REVIEW_JSON_NAMES,
        project_root=project_root,
    )
    gate_payload = _mapping(gate_source.get("payload"))
    weekly_payload = _mapping(weekly_source.get("payload"))
    normal_may_resume = _normal_may_resume(gate_payload)
    gate_status = _source_status(
        gate_payload,
        _mapping(gate_source.get("report_index_entry")),
        ("normal_paper_shadow_resumption_gate_status", "status"),
    )
    candidate_id = _candidate_id(gate_payload, weekly_payload)
    start_date = _observation_start_date(gate_payload, as_of) if normal_may_resume else ""
    complete_days = _complete_observation_days(weekly_payload, start_date)
    current_count = len(complete_days) if normal_may_resume else 0
    missing_count = max(MINIMUM_OBSERVATION_TRADING_DAYS - current_count, 0)
    status = _clock_status(normal_may_resume, current_count)
    invalid_reasons = _invalid_reasons(
        status=status,
        normal_may_resume=normal_may_resume,
        current_count=current_count,
        gate_status=gate_status,
        weekly_source=weekly_source,
    )
    summary = {
        "candidate_id": candidate_id,
        "normal_observation_clock_status": status,
        "normal_paper_shadow_may_resume": normal_may_resume,
        "gate_status": gate_status,
        "observation_start_date": start_date,
        "current_count": current_count,
        "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
        "missing_day_count": missing_count,
        "complete_observation_day_count": len(complete_days),
        "extended_shadow_remains_forbidden": True,
        "live_trading_remains_forbidden": True,
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, invalid_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "normal_observation_clock_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "normal_paper_shadow_observation_only": True,
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "normal_paper_shadow_resumption_gate": _text(gate_source.get("payload_path")),
            "paper_shadow_weekly_review": _text(weekly_source.get("payload_path")),
        },
        "summary": summary,
        "candidate_id": candidate_id,
        "observation_window": {
            "observation_start_date": start_date,
            "as_of": as_of.isoformat(),
            "complete_observation_trading_days": complete_days,
            "current_count": current_count,
            "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
            "missing_day_count": missing_count,
            "invalid_reasons": invalid_reasons,
        },
        "source_evidence": [_compact_source(gate_source), _compact_source(weekly_source)],
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Clock starts only after normal paper-shadow resumption gate permits observation.",
            (
                "This report does not run paper-shadow, fabricate missing days, "
                "or approve extended shadow."
            ),
            "Live trading remains forbidden regardless of clock status.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_normal_paper_shadow_observation_clock_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    window = _mapping(payload.get("observation_window"))
    status = _text(payload.get("normal_observation_clock_status"))
    current_count = _int(window.get("current_count"))
    complete_days = _list_values(window.get("complete_observation_trading_days"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_normal_paper_shadow_observation_clock",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        status in OBSERVATION_STATUSES,
        "normal_observation_clock_status must use the supported enum.",
        "restore_normal_observation_clock_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_count_policy",
        _int(window.get("required_count")) == MINIMUM_OBSERVATION_TRADING_DAYS,
        "required_count must match governed observation period policy.",
        "restore_minimum_observation_policy_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "current_count_matches_complete_days",
        not complete_days or current_count == len(complete_days),
        "current_count must match complete_observation_trading_days when dates are present.",
        "repair_normal_observation_clock_count",
    )
    _append_check(
        checks,
        blocking_issues,
        "not_started_requires_gate_blocked",
        status != OBSERVATION_NOT_STARTED
        or (
            summary.get("normal_paper_shadow_may_resume") is False
            and current_count == 0
        ),
        "OBSERVATION_NOT_STARTED requires normal paper-shadow resumption gate to be blocked.",
        "repair_normal_observation_clock_gate_logic",
    )
    _append_check(
        checks,
        blocking_issues,
        "live_trading_forbidden",
        summary.get("live_trading_remains_forbidden") is True,
        "Normal observation clock must keep live trading forbidden.",
        "restore_normal_observation_clock_live_boundary",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_normal_observation_clock_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Normal observation clock must preserve paper-shadow-only safety boundary.",
        "restore_normal_observation_clock_safety_boundary",
    )
    if status != OBSERVATION_PERIOD_MET:
        warning_issues.append(
            {
                "issue_id": "normal_observation_period_not_met",
                "message": "Clock is valid but normal observation period is not complete.",
                "status": status,
                "current_count": current_count,
                "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
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
        "normal_observation_clock_status": status,
        "normal_paper_shadow_may_resume": summary.get("normal_paper_shadow_may_resume"),
        "current_count": current_count,
        "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
        "live_trading_remains_forbidden": summary.get("live_trading_remains_forbidden"),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_normal_observation_clock_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": _reader_brief(summary, _list_values(window.get("invalid_reasons"))),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not start observation or approve trading.",
            "PASS_WITH_WARNINGS means the artifact is valid but observation remains incomplete.",
        ],
        "next_action": (
            "repair_normal_observation_clock"
            if validation_status == FAIL_STATUS
            else "use_clock_as_post_recovery_input"
        ),
    }


def write_normal_paper_shadow_observation_clock_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_normal_paper_shadow_observation_clock_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_normal_paper_shadow_observation_clock_markdown(payload), output_path)


def write_normal_paper_shadow_observation_clock_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_normal_paper_shadow_observation_clock_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(
        render_normal_paper_shadow_observation_clock_validation_markdown(payload),
        output_path,
    )


def render_normal_paper_shadow_observation_clock_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    window = _mapping(payload.get("observation_window"))
    lines = [
        f"# Normal Paper Shadow Observation Clock {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- normal_observation_clock_status: {payload.get('normal_observation_clock_status')}",
        f"- normal_paper_shadow_may_resume: {summary.get('normal_paper_shadow_may_resume')}",
        f"- gate_status: {summary.get('gate_status')}",
        f"- observation_start_date: {summary.get('observation_start_date')}",
        f"- current_count: {summary.get('current_count')}",
        f"- required_count: {summary.get('required_count')}",
        f"- missing_day_count: {summary.get('missing_day_count')}",
        f"- extended_shadow_remains_forbidden: {summary.get('extended_shadow_remains_forbidden')}",
        f"- live_trading_remains_forbidden: {summary.get('live_trading_remains_forbidden')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Invalid Reasons",
        "",
    ]
    invalid_reasons = _list_values(window.get("invalid_reasons"))
    lines.extend(f"- {reason}" for reason in invalid_reasons) if invalid_reasons else lines.append(
        "- none"
    )
    lines.extend(
        [
            "",
            "## Source Evidence",
            "",
            "|source_id|availability|source_status|artifact_path|payload_path|",
            "|---|---|---|---|---|",
        ]
    )
    for source in _records(payload.get("source_evidence")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    source.get("source_id"),
                    source.get("availability"),
                    source.get("source_status"),
                    source.get("artifact_path"),
                    source.get("payload_path"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Safety Boundary", "", "- paper-shadow-only, read-only clock.", ""])
    return "\n".join(lines)


def render_normal_paper_shadow_observation_clock_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Normal Paper Shadow Observation Clock Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        (
            "- source_normal_observation_clock_status: "
            f"{payload.get('source_normal_observation_clock_status')}"
        ),
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- current_count: {summary.get('current_count')}",
        f"- required_count: {summary.get('required_count')}",
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


def _source_evidence(
    report_index: Mapping[str, Any],
    report_id: str,
    source_id: str,
    preferred_json_names: Sequence[str],
    *,
    project_root: Path,
) -> dict[str, Any]:
    entry = _report_index_entry(report_index, report_id)
    artifact_path = _resolve_artifact_path(_text(entry.get("latest_artifact_path")), project_root)
    payload_path, payload = _read_source_payload(artifact_path, preferred_json_names)
    source_status = _source_status(payload, entry, ("status", "freshness_status"))
    return {
        "source_id": source_id,
        "report_id": report_id,
        "availability": (
            "AVAILABLE" if artifact_path is not None and artifact_path.exists() else "MISSING"
        ),
        "source_status": source_status,
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "payload_path": "" if payload_path is None else str(payload_path),
        "report_index_entry": entry,
        "payload": payload,
        "production_effect": _text(
            payload.get("production_effect"),
            _text(entry.get("production_effect"), PRODUCTION_EFFECT),
        ),
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
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        payload = _read_optional_json_mapping(candidate)
        if payload:
            return candidate, payload
    return None, {}


def _source_status(
    payload: Mapping[str, Any],
    entry: Mapping[str, Any],
    status_fields: Sequence[str],
) -> str:
    summary = _mapping(payload.get("summary"))
    for field in status_fields:
        value = _text(payload.get(field), _text(summary.get(field)))
        if value:
            return value
    for field in ("status", "validation_status", "freshness_status"):
        value = _text(entry.get(field))
        if value:
            return value
    return "MISSING" if not entry else "UNKNOWN"


def _normal_may_resume(gate_payload: Mapping[str, Any]) -> bool:
    summary = _mapping(gate_payload.get("summary"))
    if gate_payload.get("normal_paper_shadow_may_resume") is True:
        return True
    if summary.get("normal_paper_shadow_may_resume") is True:
        return True
    status = _text(
        gate_payload.get("normal_paper_shadow_resumption_gate_status"),
        _text(summary.get("normal_paper_shadow_resumption_gate_status")),
    )
    return status in {"RESUME_NORMAL_SHADOW_ALLOWED", "RESUME_NORMAL_SHADOW_WITH_WARNINGS"}


def _observation_start_date(gate_payload: Mapping[str, Any], as_of: date) -> str:
    summary = _mapping(gate_payload.get("summary"))
    for key in (
        "observation_start_date",
        "normal_observation_start_date",
        "resumption_date",
        "as_of",
        "requested_as_of",
    ):
        value = _text(gate_payload.get(key), _text(summary.get(key)))
        if value:
            return value
    return as_of.isoformat()


def _complete_observation_days(weekly_payload: Mapping[str, Any], start_date: str) -> list[str]:
    if not start_date:
        return []
    days = set(_list_values(weekly_payload.get("complete_observation_trading_days")))
    days.update(_list_values(_mapping(weekly_payload.get("summary")).get("covered_market_days")))
    for observation in _records(weekly_payload.get("daily_observations")):
        status = _text(observation.get("observation_status"))
        observation_date = _text(observation.get("observation_date"))
        if observation_date and status in {"RECORDED", "PASS", "OK", ""}:
            days.add(observation_date)
    return sorted(day for day in days if day >= start_date)


def _clock_status(normal_may_resume: bool, current_count: int) -> str:
    if not normal_may_resume:
        return OBSERVATION_NOT_STARTED
    if current_count >= MINIMUM_OBSERVATION_TRADING_DAYS:
        return OBSERVATION_PERIOD_MET
    if current_count > 0:
        return OBSERVATION_IN_PROGRESS
    return OBSERVATION_PERIOD_UNMET


def _invalid_reasons(
    *,
    status: str,
    normal_may_resume: bool,
    current_count: int,
    gate_status: str,
    weekly_source: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    if not normal_may_resume:
        reasons.append(f"normal_paper_shadow_resumption_gate_not_allowed:{gate_status}")
    if normal_may_resume and current_count == 0:
        reasons.append("normal_observation_started_but_no_valid_days_found")
    if current_count < MINIMUM_OBSERVATION_TRADING_DAYS:
        reasons.append(
            f"minimum_observation_period_unmet_{current_count}_of_"
            f"{MINIMUM_OBSERVATION_TRADING_DAYS}"
        )
    if _text(weekly_source.get("availability")) != "AVAILABLE":
        reasons.append("missing_paper_shadow_weekly_review_source")
    if status == OBSERVATION_PERIOD_MET:
        return []
    return reasons


def _candidate_id(*payloads: Mapping[str, Any]) -> str:
    for payload in payloads:
        summary = _mapping(payload.get("summary"))
        for key in ("candidate_id", "candidate"):
            value = _text(payload.get(key), _text(summary.get(key)))
            if value:
                return value
    return ""


def _compact_source(source: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_id": _text(source.get("source_id")),
        "report_id": _text(source.get("report_id")),
        "availability": _text(source.get("availability")),
        "source_status": _text(source.get("source_status")),
        "artifact_path": _text(source.get("artifact_path")),
        "payload_path": _text(source.get("payload_path")),
        "production_effect": _text(source.get("production_effect"), PRODUCTION_EFFECT),
    }


def _reader_brief(summary: Mapping[str, Any], invalid_reasons: Sequence[Any]) -> dict[str, Any]:
    status = _text(summary.get("normal_observation_clock_status"), OBSERVATION_NOT_STARTED)
    current_count = _int(summary.get("current_count"))
    required_count = _int(summary.get("required_count"), MINIMUM_OBSERVATION_TRADING_DAYS)
    return {
        "summary": (
            f"Normal paper-shadow observation clock is {status}: "
            f"{current_count}/{required_count} trading days."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if status == OBSERVATION_PERIOD_MET
            else "; ".join(_text(reason) for reason in invalid_reasons[:5])
        ),
        "warnings": (
            "none" if status == OBSERVATION_PERIOD_MET else "normal observation incomplete"
        ),
        "safety_boundary": (
            "Normal observation clock is read-only; no extended-shadow approval, live trading, "
            "official target weights, broker/order, paper-shadow mutation, or production mutation."
        ),
        "next_action": (
            "continue_collecting_normal_paper_shadow_observation_days"
            if status in {OBSERVATION_IN_PROGRESS, OBSERVATION_PERIOD_UNMET}
            else (
                "record_continue_normal_shadow_owner_decision_before_observation_starts"
                if status == OBSERVATION_NOT_STARTED
                else "use_clock_as_one_input_to_extended_shadow_review"
            )
        ),
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
        "mode": "read_existing_normal_resumption_and_weekly_review_sources_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "normal_paper_shadow_observation_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
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


def _write_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def _write_text(text: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path
