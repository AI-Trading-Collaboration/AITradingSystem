from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.decision_snapshots import (
    DEFAULT_DECISION_SNAPSHOT_DIR,
    default_decision_snapshot_path,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _append_check,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_optional_json_mapping,
    _records,
    _report_index_entry,
    _resolve_artifact_path,
    _text,
)
from ai_trading_system.trading_calendar import us_equity_market_session

SCHEMA_VERSION = 1
REPORT_TYPE = "decision_snapshot_lifecycle_policy"
VALIDATION_REPORT_TYPE = "decision_snapshot_lifecycle_policy_validation"

SNAPSHOT_AVAILABLE = "SNAPSHOT_AVAILABLE"
SNAPSHOT_NOT_DUE = "SNAPSHOT_NOT_DUE"
SNAPSHOT_MISSING_BLOCKING = "SNAPSHOT_MISSING_BLOCKING"
SNAPSHOT_MISSING_NON_BLOCKING = "SNAPSHOT_MISSING_NON_BLOCKING"
SNAPSHOT_STATUSES = (
    SNAPSHOT_AVAILABLE,
    SNAPSHOT_NOT_DUE,
    SNAPSHOT_MISSING_BLOCKING,
    SNAPSHOT_MISSING_NON_BLOCKING,
)

STRICT_CONTEXT_MODE = "strict_same_day"
LATEST_CONTEXT_MODE = "latest_available_reader_context"

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

SOURCE_DEPENDENCY_SPECS: tuple[dict[str, str], ...] = (
    {
        "dependency_id": "daily_decision_summary",
        "report_id": "daily_decision_summary",
        "role": "same-day decision summary that should reference the canonical snapshot",
    },
    {
        "dependency_id": "daily_report",
        "report_id": "daily_report",
        "role": "human daily report generated from the same scoring run",
    },
    {
        "dependency_id": "calculation_explainers",
        "report_id": "calculation_explainers",
        "role": "formula explanations that consume the canonical decision snapshot",
    },
    {
        "dependency_id": "reader_brief",
        "report_id": "reader_brief",
        "role": "Reader Brief consumer; must not fabricate missing snapshots",
    },
    {
        "dependency_id": "report_index",
        "report_id": "report_index",
        "role": "latest artifact discovery and freshness source",
    },
)


def default_decision_snapshot_lifecycle_policy_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"decision_snapshot_lifecycle_policy_{as_of.isoformat()}.json"


def default_decision_snapshot_lifecycle_policy_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"decision_snapshot_lifecycle_policy_{as_of.isoformat()}.md"


def default_decision_snapshot_lifecycle_policy_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"decision_snapshot_lifecycle_policy_validation_{as_of.isoformat()}.json"


def default_decision_snapshot_lifecycle_policy_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"decision_snapshot_lifecycle_policy_validation_{as_of.isoformat()}.md"


def latest_decision_snapshot_lifecycle_policy_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "decision_snapshot_lifecycle_policy_", ".json")


def build_decision_snapshot_lifecycle_policy_payload(
    *,
    as_of: date,
    decision_snapshot_path: Path | None = None,
    snapshot_dir: Path = DEFAULT_DECISION_SNAPSHOT_DIR,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    allow_latest_context: bool = False,
    today: date | None = None,
    project_root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    target_snapshot_path = decision_snapshot_path or default_decision_snapshot_path(
        snapshot_dir,
        as_of,
    )
    snapshot_payload = _read_optional_json_mapping(target_snapshot_path)
    latest_snapshot = _latest_available_snapshot(snapshot_dir, as_of=as_of)
    latest_snapshot_payload = (
        _read_optional_json_mapping(latest_snapshot[1]) if latest_snapshot is not None else {}
    )
    session = us_equity_market_session(as_of)
    effective_today = today or date.today()
    context_mode = LATEST_CONTEXT_MODE if allow_latest_context else STRICT_CONTEXT_MODE
    status = _snapshot_status(
        as_of=as_of,
        snapshot_exists=target_snapshot_path.exists() and bool(snapshot_payload),
        session_is_trading_day=session.is_trading_day,
        today=effective_today,
        allow_latest_context=allow_latest_context,
        latest_snapshot=latest_snapshot,
    )
    blocking_impact = _blocking_impact(status)
    invalid_reasons = _invalid_reasons(
        status=status,
        as_of=as_of,
        snapshot_path=target_snapshot_path,
        snapshot_payload=snapshot_payload,
        latest_snapshot=latest_snapshot,
        session_is_trading_day=session.is_trading_day,
        today=effective_today,
    )
    source_dependencies = _source_dependencies(
        report_index_payload or {},
        project_root=project_root,
    )
    summary = {
        "snapshot_lifecycle_status": status,
        "target_as_of": as_of.isoformat(),
        "context_mode": context_mode,
        "snapshot_path": str(target_snapshot_path),
        "snapshot_exists": target_snapshot_path.exists(),
        "snapshot_parseable": bool(snapshot_payload),
        "snapshot_signal_date": _snapshot_signal_date(snapshot_payload),
        "latest_available_snapshot_date": "" if latest_snapshot is None else latest_snapshot[0],
        "latest_available_snapshot_path": (
            "" if latest_snapshot is None else str(latest_snapshot[1])
        ),
        "latest_available_signal_date": _snapshot_signal_date(latest_snapshot_payload),
        "market_session_status": session.session_status,
        "market_session_kind": session.session_kind,
        "market_session_reason": session.reason,
        "is_trading_day": session.is_trading_day,
        "today": effective_today.isoformat(),
        "blocking_impact": blocking_impact,
        "invalid_reason_count": len(invalid_reasons),
        "source_dependency_count": len(source_dependencies),
        "available_source_dependency_count": len(
            [source for source in source_dependencies if source["availability"] == "AVAILABLE"]
        ),
        "production_effect": PRODUCTION_EFFECT,
    }
    reader_brief = _reader_brief(summary, invalid_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "snapshot_lifecycle_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": (
            "Clarify decision snapshot lifecycle and missing snapshot behavior without "
            "fabricating missing decision_snapshot artifacts."
        ),
        "input_artifacts": {
            "decision_snapshot": str(target_snapshot_path),
            "snapshot_dir": str(snapshot_dir),
            "report_index": "" if report_index_path is None else str(report_index_path),
        },
        "output_decision": status,
        "summary": summary,
        "snapshot_check": {
            "target_as_of": as_of.isoformat(),
            "canonical_snapshot_path": str(target_snapshot_path),
            "exists": target_snapshot_path.exists(),
            "parseable": bool(snapshot_payload),
            "signal_date": _snapshot_signal_date(snapshot_payload),
            "latest_available_snapshot_date": (
                "" if latest_snapshot is None else latest_snapshot[0]
            ),
            "latest_available_snapshot_path": (
                "" if latest_snapshot is None else str(latest_snapshot[1])
            ),
            "context_mode": context_mode,
            "missing_snapshot_behavior": _missing_snapshot_behavior(status),
            "invalid_reasons": invalid_reasons,
        },
        "lifecycle_policy": _lifecycle_policy(),
        "source_report_dependencies": source_dependencies,
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Lifecycle policy is read-only and does not run score-daily.",
            "Missing decision snapshots are not fabricated, copied, or backfilled.",
            "SNAPSHOT_AVAILABLE only means the canonical snapshot artifact exists and parses.",
            "This report does not approve paper-shadow, extended shadow, official target "
            "weights, broker/order, live trading, or production mutation.",
        ],
        "next_action": reader_brief["next_action"],
        "methodology": {
            "collector_mode": "read_existing_decision_snapshot_and_optional_report_index_only",
            "does_not_run_score_daily": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_snapshot": True,
            "does_not_modify_snapshot": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_decision_snapshot_lifecycle_policy_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    summary = _mapping(payload.get("summary"))
    snapshot_check = _mapping(payload.get("snapshot_check"))
    status = _text(payload.get("snapshot_lifecycle_status"), _text(payload.get("status")))
    snapshot_path = Path(_text(snapshot_check.get("canonical_snapshot_path"), "__missing__"))
    snapshot_exists = snapshot_path.exists()
    as_of_text = _text(payload.get("as_of"), date.today().isoformat())
    signal_date = _text(snapshot_check.get("signal_date"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_decision_snapshot_lifecycle_policy",
    )
    _append_check(
        checks,
        blocking_issues,
        "snapshot_lifecycle_status_enum",
        status in SNAPSHOT_STATUSES,
        "snapshot_lifecycle_status must use the supported enum.",
        "restore_supported_decision_snapshot_lifecycle_status",
    )
    _append_check(
        checks,
        blocking_issues,
        "available_status_requires_existing_snapshot",
        status != SNAPSHOT_AVAILABLE or snapshot_exists,
        "SNAPSHOT_AVAILABLE requires the canonical decision snapshot file to exist.",
        "rerun_policy_after_canonical_snapshot_exists",
    )
    _append_check(
        checks,
        blocking_issues,
        "missing_status_does_not_claim_existing_snapshot",
        status == SNAPSHOT_AVAILABLE or not bool(snapshot_check.get("exists")),
        "Missing/not-due statuses must not claim the target snapshot exists.",
        "restore_missing_snapshot_status_or_canonical_path",
    )
    _append_check(
        checks,
        blocking_issues,
        "snapshot_date_alignment_when_available",
        status != SNAPSHOT_AVAILABLE or not signal_date or signal_date == as_of_text,
        "Available decision snapshot signal_date must match policy as_of.",
        "regenerate_policy_for_matching_snapshot_date",
        details={"signal_date": signal_date, "as_of": as_of_text},
    )
    safety = _mapping(payload.get("safety_boundary"))
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary_no_snapshot_fabrication",
        (
            _text(safety.get("production_effect")) == PRODUCTION_EFFECT
            and safety.get("score_daily_executed") is False
            and safety.get("snapshot_created_by_policy") is False
            and safety.get("snapshot_fabricated") is False
            and safety.get("snapshot_copied_from_prior_date") is False
            and safety.get("production_state_mutated") is False
            and safety.get("broker_action_taken") is False
            and safety.get("order_ticket_generated") is False
        ),
        "Lifecycle policy must not create, copy, or fabricate decision snapshots.",
        "restore_decision_snapshot_lifecycle_safety_boundary",
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
        "restore_decision_snapshot_lifecycle_reader_brief_fields",
    )
    if status == SNAPSHOT_MISSING_BLOCKING:
        warning_issues.append(
            {
                "issue_id": "decision_snapshot_missing_blocking",
                "status": status,
                "target_as_of": as_of_text,
                "snapshot_path": _text(snapshot_check.get("canonical_snapshot_path")),
                "message": (
                    "Canonical decision snapshot is missing for a due trading day; "
                    "same-day Reader Brief conclusion must remain blocked."
                ),
                "recommended_action": _text(payload.get("next_action")),
            }
        )
    elif status == SNAPSHOT_MISSING_NON_BLOCKING:
        warning_issues.append(
            {
                "issue_id": "decision_snapshot_missing_non_blocking_latest_context",
                "status": status,
                "target_as_of": as_of_text,
                "latest_available_snapshot_date": _text(
                    snapshot_check.get("latest_available_snapshot_date")
                ),
                "message": (
                    "Target decision snapshot is missing; report explicitly uses latest "
                    "available snapshot only as limited reader context."
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
        "source_snapshot_lifecycle_status": status,
        "target_as_of": as_of_text,
        "snapshot_exists": snapshot_exists,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": as_of_text,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_snapshot_lifecycle_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "research_governance_only": True,
        "purpose": "Validate decision snapshot lifecycle policy schema and safety boundary.",
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "output_decision": validation_status,
        "summary": validation_summary,
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "source_summary": dict(summary),
        "source_snapshot_check": dict(snapshot_check),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation is read-only and does not run score-daily.",
            "PASS_WITH_WARNINGS can mean the policy is valid while the target snapshot "
            "is missing and blocking same-day conclusions.",
        ],
        "next_action": (
            "use_decision_snapshot_lifecycle_policy_for_governance"
            if validation_status != FAIL_STATUS
            else "repair_decision_snapshot_lifecycle_policy_schema_or_safety"
        ),
        "reader_brief": _reader_brief(
            summary,
            _mapping(payload.get("snapshot_check")).get("invalid_reasons"),
        ),
        "methodology": {
            "collector_mode": "validate_existing_decision_snapshot_lifecycle_policy_only",
            "does_not_run_score_daily": True,
            "does_not_refresh_data": True,
            "does_not_generate_missing_snapshot": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_decision_snapshot_lifecycle_policy_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return output_path


def write_decision_snapshot_lifecycle_policy_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_decision_snapshot_lifecycle_policy_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_decision_snapshot_lifecycle_policy_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_decision_snapshot_lifecycle_policy_json(payload, output_path)


def write_decision_snapshot_lifecycle_policy_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_decision_snapshot_lifecycle_policy_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_decision_snapshot_lifecycle_policy_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    snapshot_check = _mapping(payload.get("snapshot_check"))
    lines = [
        f"# Decision Snapshot Lifecycle Policy {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- snapshot_lifecycle_status: {payload.get('snapshot_lifecycle_status')}",
        f"- target_as_of: {summary.get('target_as_of')}",
        f"- context_mode: {summary.get('context_mode')}",
        f"- snapshot_path: {summary.get('snapshot_path')}",
        f"- snapshot_exists: {summary.get('snapshot_exists')}",
        f"- snapshot_signal_date: {summary.get('snapshot_signal_date')}",
        f"- latest_available_snapshot_date: {summary.get('latest_available_snapshot_date')}",
        f"- market_session_status: {summary.get('market_session_status')}",
        f"- blocking_impact: {summary.get('blocking_impact')}",
        f"- production_effect: {payload.get('production_effect')}",
        f"- next_action: {payload.get('next_action')}",
        "",
        "## Missing Snapshot Behavior",
        "",
        f"- behavior: {snapshot_check.get('missing_snapshot_behavior')}",
        "",
        "## Invalid Reasons",
        "",
    ]
    invalid_reasons = _records_from_strings(snapshot_check.get("invalid_reasons"))
    if invalid_reasons:
        lines.extend(f"- {reason}" for reason in invalid_reasons)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Source Report Dependencies",
            "",
            "|dependency_id|availability|report_id|artifact_path|role|",
            "|---|---|---|---|---|",
        ]
    )
    for dependency in _records(payload.get("source_report_dependencies")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(value)
                for value in (
                    dependency.get("dependency_id"),
                    dependency.get("availability"),
                    dependency.get("report_id"),
                    dependency.get("artifact_path"),
                    dependency.get("role"),
                )
            )
            + "|"
        )
    lines.extend(["", "## Lifecycle Policy", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("lifecycle_policy")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.extend(["", "## Safety Boundary", "", "|field|value|", "|---|---|"])
    for key, value in _mapping(payload.get("safety_boundary")).items():
        lines.append(f"|{_md_cell(key)}|{_md_cell(value)}|")
    lines.append("")
    return "\n".join(lines)


def render_decision_snapshot_lifecycle_policy_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Decision Snapshot Lifecycle Policy Validation {payload.get('as_of')}",
        "",
        "## 摘要",
        "",
        f"- validation_status: {payload.get('validation_status')}",
        f"- source_snapshot_lifecycle_status: {payload.get('source_snapshot_lifecycle_status')}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- snapshot_exists: {summary.get('snapshot_exists')}",
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


def _snapshot_status(
    *,
    as_of: date,
    snapshot_exists: bool,
    session_is_trading_day: bool,
    today: date,
    allow_latest_context: bool,
    latest_snapshot: tuple[str, Path] | None,
) -> str:
    if snapshot_exists:
        return SNAPSHOT_AVAILABLE
    if as_of > today or not session_is_trading_day:
        return SNAPSHOT_NOT_DUE
    if allow_latest_context and latest_snapshot is not None:
        return SNAPSHOT_MISSING_NON_BLOCKING
    return SNAPSHOT_MISSING_BLOCKING


def _latest_available_snapshot(snapshot_dir: Path, *, as_of: date) -> tuple[str, Path] | None:
    if not snapshot_dir.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in snapshot_dir.glob("decision_snapshot_????-??-??.json"):
        try:
            snapshot_date = date.fromisoformat(path.stem.removeprefix("decision_snapshot_"))
        except ValueError:
            continue
        if snapshot_date <= as_of:
            candidates.append((snapshot_date, path))
    if not candidates:
        return None
    snapshot_date, path = max(candidates, key=lambda item: item[0])
    return snapshot_date.isoformat(), path


def _snapshot_signal_date(snapshot_payload: Mapping[str, Any]) -> str:
    signal_date = _text(snapshot_payload.get("signal_date"))
    if signal_date:
        return signal_date
    snapshot_id = _text(snapshot_payload.get("snapshot_id"))
    if snapshot_id.startswith("decision_snapshot:"):
        return snapshot_id.removeprefix("decision_snapshot:")
    return ""


def _invalid_reasons(
    *,
    status: str,
    as_of: date,
    snapshot_path: Path,
    snapshot_payload: Mapping[str, Any],
    latest_snapshot: tuple[str, Path] | None,
    session_is_trading_day: bool,
    today: date,
) -> list[str]:
    reasons: list[str] = []
    if status == SNAPSHOT_AVAILABLE:
        signal_date = _snapshot_signal_date(snapshot_payload)
        if signal_date and signal_date != as_of.isoformat():
            reasons.append(f"snapshot_signal_date_mismatch:{signal_date}")
        return reasons
    if status == SNAPSHOT_NOT_DUE:
        if as_of > today:
            reasons.append(f"snapshot_date_future:{as_of.isoformat()}")
        if not session_is_trading_day:
            reasons.append("market_session_not_trading_day")
        return reasons
    if status == SNAPSHOT_MISSING_BLOCKING:
        reasons.append(f"canonical_decision_snapshot_missing:{snapshot_path}")
        reasons.append("same_day_reader_brief_must_remain_blocked")
    elif status == SNAPSHOT_MISSING_NON_BLOCKING:
        reasons.append(f"canonical_decision_snapshot_missing:{snapshot_path}")
        if latest_snapshot is not None:
            reasons.append(f"latest_available_reader_context:{latest_snapshot[0]}")
        reasons.append("same_day_conclusion_not_claimed")
    return reasons


def _blocking_impact(status: str) -> str:
    if status == SNAPSHOT_AVAILABLE:
        return "same_day_snapshot_available"
    if status == SNAPSHOT_NOT_DUE:
        return "no_same_day_snapshot_due"
    if status == SNAPSHOT_MISSING_NON_BLOCKING:
        return "latest_context_only_same_day_conclusion_limited"
    return "blocks_same_day_reader_brief_and_decision_conclusion"


def _missing_snapshot_behavior(status: str) -> str:
    if status == SNAPSHOT_AVAILABLE:
        return "snapshot_exists_use_canonical_artifact"
    if status == SNAPSHOT_NOT_DUE:
        return "do_not_create_snapshot_until_due"
    if status == SNAPSHOT_MISSING_NON_BLOCKING:
        return "do_not_fabricate_target_snapshot_use_latest_context_only"
    return "do_not_fabricate_target_snapshot_block_same_day_conclusion"


def _source_dependencies(
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
) -> list[dict[str, Any]]:
    dependencies: list[dict[str, Any]] = []
    for spec in SOURCE_DEPENDENCY_SPECS:
        report_id = spec["report_id"]
        entry = _report_index_entry(report_index, report_id)
        artifact_path = _resolve_artifact_path(
            _text(entry.get("latest_artifact_path")),
            project_root,
        )
        dependencies.append(
            {
                "dependency_id": spec["dependency_id"],
                "report_id": report_id,
                "role": spec["role"],
                "availability": (
                    "AVAILABLE"
                    if artifact_path is not None and artifact_path.exists()
                    else "MISSING"
                ),
                "artifact_status": _text(entry.get("artifact_status"), "MISSING"),
                "freshness_status": _text(entry.get("freshness_status"), "MISSING"),
                "artifact_path": "" if artifact_path is None else str(artifact_path),
                "production_effect": _text(entry.get("production_effect"), PRODUCTION_EFFECT),
            }
        )
    return dependencies


def _lifecycle_policy() -> dict[str, Any]:
    return {
        "created_by": "canonical_score_daily_after_data_quality_gate",
        "canonical_path_template": (
            "data/processed/decision_snapshots/decision_snapshot_YYYY-MM-DD.json"
        ),
        "as_of_date_rule": "snapshot signal_date must equal requested as_of date",
        "required_inputs": (
            "validated cached data, daily score inputs, position gates, manual review fields, "
            "trace bundle, score architecture, and same-day decision context"
        ),
        "validation_rules": (
            "status enum, snapshot existence/status alignment, signal_date/as_of alignment, "
            "Reader Brief fields, and no-fabrication safety boundary"
        ),
        "missing_behavior": (
            "missing due trading-day snapshot blocks same-day conclusions; latest snapshot may "
            "only be explicit limited reader context and never a substitute target snapshot"
        ),
    }


def _safety_boundary() -> dict[str, Any]:
    return {
        "production_effect": PRODUCTION_EFFECT,
        "score_daily_executed": False,
        "data_refreshed": False,
        "snapshot_created_by_policy": False,
        "snapshot_fabricated": False,
        "snapshot_copied_from_prior_date": False,
        "snapshot_modified": False,
        "production_state_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "official_target_weights_generated": False,
        "live_trading_approved": False,
        "paper_shadow_resumed": False,
        "extended_shadow_approved": False,
    }


def _reader_brief(summary: Mapping[str, Any], invalid_reasons: Any) -> dict[str, Any]:
    status = _text(summary.get("snapshot_lifecycle_status"), "UNKNOWN")
    reasons = _records_from_strings(invalid_reasons)
    blockers: list[str] = []
    warnings: list[str] = []
    if status == SNAPSHOT_MISSING_BLOCKING:
        blockers = reasons or ["canonical decision snapshot missing for due trading day"]
    elif status == SNAPSHOT_MISSING_NON_BLOCKING:
        warnings = reasons or ["latest available snapshot used only as limited context"]
    elif status == SNAPSHOT_NOT_DUE:
        warnings = reasons
    return {
        "summary": (
            f"Decision snapshot lifecycle={status}; "
            f"target={_text(summary.get('target_as_of'))}; "
            f"snapshot_exists={summary.get('snapshot_exists')}; "
            f"latest={_text(summary.get('latest_available_snapshot_date'), 'none')}."
        ),
        "key_result": status,
        "blocking_issues": "; ".join(blockers) if blockers else "none",
        "warnings": "; ".join(warnings) if warnings else "none",
        "safety_boundary": (
            "read-only; does not run score-daily, refresh data, fabricate snapshots, "
            "or mutate production/broker/order state"
        ),
        "next_action": _next_action(status),
    }


def _next_action(status: str) -> str:
    if status == SNAPSHOT_AVAILABLE:
        return "use_canonical_decision_snapshot_for_reader_brief_and_governance"
    if status == SNAPSHOT_NOT_DUE:
        return "wait_until_snapshot_due_after_canonical_daily_scoring_window"
    if status == SNAPSHOT_MISSING_NON_BLOCKING:
        return "disclose_latest_context_limitation_and_do_not_claim_same_day_snapshot"
    return "run_canonical_score_daily_or_keep_same_day_reader_brief_blocked_without_fabrication"


def _records_from_strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Mapping):
        return [_text(value.get("issue_id"), _text(value.get("message")))]
    if isinstance(value, (list, tuple)):
        records: list[str] = []
        for item in value:
            if isinstance(item, Mapping):
                records.append(_text(item.get("issue_id"), _text(item.get("message"))))
            else:
                text = _text(item)
                if text:
                    records.append(text)
        return records
    text = _text(value)
    return [text] if text else []
