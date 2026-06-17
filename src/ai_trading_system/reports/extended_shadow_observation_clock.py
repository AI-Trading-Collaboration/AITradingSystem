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
REPORT_TYPE = "extended_shadow_observation_clock"
VALIDATION_REPORT_TYPE = "extended_shadow_observation_clock_validation"

OBSERVATION_PERIOD_UNMET = "OBSERVATION_PERIOD_UNMET"
OBSERVATION_PERIOD_PARTIAL = "OBSERVATION_PERIOD_PARTIAL"
OBSERVATION_PERIOD_MET = "OBSERVATION_PERIOD_MET"
OBSERVATION_STATUSES = (
    OBSERVATION_PERIOD_UNMET,
    OBSERVATION_PERIOD_PARTIAL,
    OBSERVATION_PERIOD_MET,
)

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

SOURCE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "source_id": "paper_shadow_weekly_review",
        "report_id": "etf_dynamic_v3_paper_shadow_weekly_review",
        "label": "Paper-shadow weekly review",
        "preferred_json_names": (
            "paper_shadow_weekly_review.json",
            "paper_shadow_weekly_manifest.json",
            "paper_shadow_weekly_validation.json",
        ),
        "status_fields": ("coverage_status", "coverage_classification", "weekly_decision"),
    },
    {
        "source_id": "promotion_board",
        "report_id": "paper_shadow_promotion_board",
        "label": "Paper-shadow promotion board",
        "preferred_json_names": ("paper_shadow_promotion_board.json",),
        "status_fields": ("board_decision", "status"),
    },
)

OBSERVATION_COUNT_KEYS = (
    "observation_trading_days",
    "observed_trading_days",
    "paper_shadow_observation_trading_days",
    "observation_day_count",
)
OBSERVATION_START_KEYS = (
    "observation_start_date",
    "paper_shadow_observation_start_date",
    "start_date",
)
COMPLETE_DAYS_KEYS = (
    "complete_observation_trading_days",
    "complete_observation_days",
    "observed_day_dates",
)
MISSING_DAYS_KEYS = ("missing_days", "missing_observation_days")
INVALID_DAYS_KEYS = ("invalid_days", "invalid_observation_days")


def default_extended_shadow_observation_clock_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"extended_shadow_observation_clock_{as_of.isoformat()}.json"


def default_extended_shadow_observation_clock_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"extended_shadow_observation_clock_{as_of.isoformat()}.md"


def default_extended_shadow_observation_clock_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"extended_shadow_observation_clock_validation_{as_of.isoformat()}.json"


def default_extended_shadow_observation_clock_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"extended_shadow_observation_clock_validation_{as_of.isoformat()}.md"


def latest_extended_shadow_observation_clock_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "extended_shadow_observation_clock_", ".json")


def build_extended_shadow_observation_clock_payload(
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

    sources = [
        _source_evidence(spec, report_index_payload, project_root=project_root)
        for spec in SOURCE_SPECS
    ]
    clock = _clock_from_sources(sources)
    current_count = _int(clock.get("current_count"))
    invalid_day_count = _int(clock.get("invalid_day_count"))
    missing_day_count = max(MINIMUM_OBSERVATION_TRADING_DAYS - current_count, 0)
    status = _clock_status(current_count, invalid_day_count)
    invalid_reasons = _invalid_reasons(
        status=status,
        current_count=current_count,
        invalid_day_count=invalid_day_count,
        sources=sources,
    )
    candidate_id = _candidate_id(sources)
    summary = {
        "candidate_id": candidate_id,
        "observation_clock_status": status,
        "observation_start_date": _text(clock.get("observation_start_date")),
        "current_count": current_count,
        "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
        "missing_day_count": missing_day_count,
        "invalid_day_count": invalid_day_count,
        "complete_observation_day_count": len(_list_values(clock.get("complete_days"))),
        "source_count": len(sources),
        "available_source_count": len([s for s in sources if s.get("availability") == "AVAILABLE"]),
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, invalid_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "observation_clock_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "research_governance_only": True,
        "purpose": (
            "Track extended-shadow observation period eligibility without fabricating "
            "missing paper-shadow observation days."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": status,
        "summary": summary,
        "candidate_id": candidate_id,
        "observation_window": {
            "observation_start_date": _text(clock.get("observation_start_date")),
            "as_of": as_of.isoformat(),
            "complete_observation_trading_days": _list_values(clock.get("complete_days")),
            "missing_days": _list_values(clock.get("missing_days")),
            "missing_day_count": missing_day_count,
            "invalid_days": _list_values(clock.get("invalid_days")),
            "invalid_day_count": invalid_day_count,
            "invalid_reasons": invalid_reasons,
            "current_count": current_count,
            "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
        },
        "source_evidence": sources,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Clock is read-only and does not run paper-shadow observation.",
            "Missing observation dates are not fabricated.",
            "OBSERVATION_PERIOD_MET is not extended-shadow approval or live approval.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_existing_report_index_and_observation_sources_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_artifacts": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_extended_shadow_observation_clock_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    window = _mapping(payload.get("observation_window"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_extended_shadow_observation_clock",
    )
    _append_check(
        checks,
        blocking_issues,
        "status_enum",
        _text(payload.get("observation_clock_status")) in OBSERVATION_STATUSES,
        "observation_clock_status must use the supported enum.",
        "restore_supported_observation_clock_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "required_count_policy",
        _int(window.get("required_count")) == MINIMUM_OBSERVATION_TRADING_DAYS,
        "required_count must match the governed extended-shadow observation policy.",
        "restore_minimum_observation_policy_count",
    )
    current_count = _int(window.get("current_count"))
    complete_days = _list_values(window.get("complete_observation_trading_days"))
    _append_check(
        checks,
        blocking_issues,
        "current_count_matches_complete_days_when_dates_present",
        not complete_days or current_count == len(complete_days),
        "current_count must match complete_observation_trading_days when date list is present.",
        "repair_observation_clock_count_or_source_dates",
    )
    _append_check(
        checks,
        blocking_issues,
        "production_effect_none",
        _text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        "Observation clock must be production_effect=none.",
        "restore_observation_clock_safety_boundary",
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_state_mutation",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("candidate_state_mutated") is False
            and safety.get("paper_shadow_state_mutated") is False
            and safety.get("production_state_mutated") is False
            and safety.get("official_target_weights_generated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        "Observation clock must not mutate paper-shadow, production, broker, or order state.",
        "restore_observation_clock_safety_boundary",
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
        "restore_observation_clock_reader_brief_fields",
    )
    if _text(payload.get("observation_clock_status")) != OBSERVATION_PERIOD_MET:
        warning_issues.append(
            {
                "issue_id": "minimum_observation_period_not_met",
                "status": _text(payload.get("observation_clock_status")),
                "current_count": current_count,
                "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
                "message": (
                    "Observation clock is valid but extended-shadow minimum period is "
                    "not met."
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
        "blocking_issue_count": len(blocking_issues),
        "observation_clock_status": _text(payload.get("observation_clock_status")),
        "current_count": current_count,
        "required_count": MINIMUM_OBSERVATION_TRADING_DAYS,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_observation_clock_status": _text(payload.get("observation_clock_status")),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "paper_shadow_only": True,
        "purpose": "Validate extended-shadow observation clock schema and safety boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "source_summary": dict(summary),
        "source_observation_window": dict(window),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not run or fabricate observation days.",
            "PASS_WITH_WARNINGS means the clock is valid but extended shadow remains unmet.",
        ],
        "next_action": (
            "use_observation_clock_for_extended_shadow_protocol"
            if validation_status != FAIL_STATUS
            else "repair_observation_clock_schema_or_safety"
        ),
        "reader_brief": _reader_brief(summary, _list_values(window.get("invalid_reasons"))),
        "methodology": {
            "collector_mode": "validate_existing_extended_shadow_observation_clock_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_candidate_state": True,
            "does_not_modify_paper_shadow_state": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_extended_shadow_observation_clock_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_extended_shadow_observation_clock_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_extended_shadow_observation_clock_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_extended_shadow_observation_clock_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_extended_shadow_observation_clock_json(payload, output_path)


def write_extended_shadow_observation_clock_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_extended_shadow_observation_clock_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_extended_shadow_observation_clock_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    window = _mapping(payload.get("observation_window"))
    lines = [
        f"# Extended Shadow Observation Clock {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- observation_clock_status: {payload.get('observation_clock_status')}",
        f"- candidate_id: {summary.get('candidate_id')}",
        f"- observation_start_date: {summary.get('observation_start_date')}",
        f"- current_count: {summary.get('current_count')}",
        f"- required_count: {summary.get('required_count')}",
        f"- missing_day_count: {summary.get('missing_day_count')}",
        f"- invalid_day_count: {summary.get('invalid_day_count')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Invalid Reasons",
        "",
    ]
    invalid_reasons = _list_values(window.get("invalid_reasons"))
    if invalid_reasons:
        lines.extend(f"- {reason}" for reason in invalid_reasons)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Source Evidence",
            "",
            "|source_id|availability|source_status|observation_count|artifact_path|",
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
                    source.get("observation_count"),
                    source.get("artifact_path"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_extended_shadow_observation_clock_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Extended Shadow Observation Clock Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_observation_clock_status: {payload.get('source_observation_clock_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- current_count: {summary.get('current_count')}",
        f"- required_count: {summary.get('required_count')}",
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
    source_status = _source_status(spec, payload, entry)
    observation_count = _observation_count(payload)
    return {
        "source_id": _text(spec.get("source_id")),
        "report_id": _text(spec.get("report_id")),
        "label": _text(spec.get("label")),
        "availability": (
            "AVAILABLE" if artifact_path is not None and artifact_path.exists() else "MISSING"
        ),
        "source_status": source_status,
        "candidate_id": _candidate_id_from_payload(payload),
        "observation_start_date": _observation_start_date(payload),
        "observation_count": observation_count,
        "complete_days": _observation_day_list(payload, COMPLETE_DAYS_KEYS),
        "missing_days": _observation_day_list(payload, MISSING_DAYS_KEYS),
        "invalid_days": _observation_day_list(payload, INVALID_DAYS_KEYS),
        "next_action": _next_action_from_payload(payload),
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "source_payload_path": "" if payload_path is None else str(payload_path),
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


def _clock_from_sources(sources: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best_source = max(sources, key=lambda source: _int(source.get("observation_count")), default={})
    current_count = _int(best_source.get("observation_count"))
    complete_days = _list_values(best_source.get("complete_days"))
    if complete_days:
        current_count = len(complete_days)
    return {
        "observation_start_date": _text(best_source.get("observation_start_date")),
        "current_count": current_count,
        "complete_days": complete_days,
        "missing_days": _list_values(best_source.get("missing_days")),
        "invalid_days": _list_values(best_source.get("invalid_days")),
        "invalid_day_count": len(_list_values(best_source.get("invalid_days"))),
    }


def _clock_status(current_count: int, invalid_day_count: int) -> str:
    if current_count >= MINIMUM_OBSERVATION_TRADING_DAYS and invalid_day_count == 0:
        return OBSERVATION_PERIOD_MET
    if current_count > 0:
        return OBSERVATION_PERIOD_PARTIAL
    return OBSERVATION_PERIOD_UNMET


def _invalid_reasons(
    *,
    status: str,
    current_count: int,
    invalid_day_count: int,
    sources: Sequence[Mapping[str, Any]],
) -> list[str]:
    reasons: list[str] = []
    if current_count < MINIMUM_OBSERVATION_TRADING_DAYS:
        reasons.append(
            f"minimum_observation_period_unmet_{current_count}_of_"
            f"{MINIMUM_OBSERVATION_TRADING_DAYS}"
        )
    if current_count == 0:
        reasons.append("no_valid_extended_shadow_observation_days_found")
    if invalid_day_count:
        reasons.append(f"invalid_observation_days_present_{invalid_day_count}")
    missing_sources = [
        _text(source.get("source_id"))
        for source in sources
        if source.get("availability") != "AVAILABLE"
    ]
    if missing_sources:
        reasons.append("missing_observation_source:" + ",".join(missing_sources))
    if status == OBSERVATION_PERIOD_MET and not reasons:
        return []
    return reasons


def _observation_count(payload: Mapping[str, Any]) -> int:
    for container in (payload, _mapping(payload.get("summary"))):
        for key in OBSERVATION_COUNT_KEYS:
            value = _int(container.get(key))
            if value > 0:
                return value
    day_list = _observation_day_list(payload, COMPLETE_DAYS_KEYS)
    return len(day_list)


def _observation_start_date(payload: Mapping[str, Any]) -> str:
    for container in (payload, _mapping(payload.get("summary"))):
        for key in OBSERVATION_START_KEYS:
            value = _text(container.get(key))
            if value:
                return value
    complete_days = _observation_day_list(payload, COMPLETE_DAYS_KEYS)
    return complete_days[0] if complete_days else ""


def _observation_day_list(payload: Mapping[str, Any], keys: Sequence[str]) -> list[str]:
    for container in (payload, _mapping(payload.get("summary"))):
        for key in keys:
            values = _list_values(container.get(key))
            if values:
                return [_text(value) for value in values if _text(value)]
    return []


def _candidate_id(sources: Sequence[Mapping[str, Any]]) -> str:
    for source in sources:
        candidate = _text(source.get("candidate_id"))
        if candidate:
            return candidate
    return ""


def _candidate_id_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("candidate_id", "candidate"):
        value = _text(payload.get(key))
        if value:
            return value
    summary = _mapping(payload.get("summary"))
    for key in ("candidate_id", "candidate"):
        value = _text(summary.get(key))
        if value:
            return value
    return ""


def _next_action_from_payload(payload: Mapping[str, Any]) -> str:
    for key in ("next_required_action", "next_action", "recommended_action"):
        value = _text(payload.get(key), _text(_mapping(payload.get("summary")).get(key)))
        if value:
            return value
    return "continue_collecting_valid_observation_days"


def _reader_brief(summary: Mapping[str, Any], invalid_reasons: Sequence[Any]) -> dict[str, Any]:
    status = _text(summary.get("observation_clock_status"), OBSERVATION_PERIOD_UNMET)
    current_count = _int(summary.get("current_count"))
    required_count = _int(summary.get("required_count"), MINIMUM_OBSERVATION_TRADING_DAYS)
    return {
        "summary": (
            f"Extended-shadow observation clock is {status}: "
            f"{current_count}/{required_count} trading days."
        ),
        "key_result": status,
        "blocking_issues": (
            "none"
            if status == OBSERVATION_PERIOD_MET
            else "; ".join(_text(reason) for reason in invalid_reasons[:5])
        ),
        "warnings": "none",
        "safety_boundary": (
            "Observation clock is read-only; no extended-shadow approval, live trading, "
            "official target weights, broker/order, paper-shadow mutation, or production mutation."
        ),
        "next_action": (
            "continue_collecting_valid_observation_days"
            if status != OBSERVATION_PERIOD_MET
            else "use_clock_as_one_input_to_extended_shadow_protocol"
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_observation_sources_only",
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
