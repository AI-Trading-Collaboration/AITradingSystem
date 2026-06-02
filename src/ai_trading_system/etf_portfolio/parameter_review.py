from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION = "etf_parameter_review_evidence_v1"

DEFAULT_PARAMETER_REVIEW_REPORT_DIR = (
    PROJECT_ROOT / "reports" / "etf_portfolio" / "parameter_review"
)
DEFAULT_PARAMETER_REVIEW_AGGREGATION_DIR = DEFAULT_PARAMETER_REVIEW_REPORT_DIR / "aggregation"
DEFAULT_PARAMETER_REVIEW_VALIDATION_DIR = DEFAULT_PARAMETER_REVIEW_REPORT_DIR / "validation"

PARAMETER_REVIEW_SAFETY = {
    "observe_only": True,
    "candidate_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "manual_review_required": True,
}

REQUIRED_EVIDENCE_FIELDS: tuple[str, ...] = (
    "review_id",
    "parameter_review_id",
    "candidate_id",
    "experiment_id",
    "source_pack_id",
    "source_run_id",
    "baseline_config_hash",
    "candidate_config_hash",
    "review_start_date",
    "review_end_date",
    "forward_days",
    "evidence_sources",
    "metrics",
    "journal_links",
    "weekly_review_links",
    "validation_status",
    "safety",
)

REQUIRED_METRIC_FIELDS: tuple[str, ...] = (
    "return_since_enrollment",
    "excess_return_vs_baseline",
    "excess_return_vs_QQQ",
    "excess_return_vs_SPY",
    "max_drawdown_since_enrollment",
    "drawdown_delta_vs_baseline",
    "turnover_since_enrollment",
    "turnover_delta_vs_baseline",
    "constraint_hit_rate",
    "regime_transition_count",
    "weight_stability_score",
    "data_coverage_ratio",
    "manual_review_count",
    "accepted_review_count",
    "rejected_review_count",
    "deferred_review_count",
)

REQUIRED_EVIDENCE_SOURCE_TYPES = frozenset(
    {
        "forward_dashboard",
        "weekly_review",
        "decision_journal",
        "experiment_report",
        "candidate_gate",
        "validation_gate",
    }
)

ALLOWED_INCOMPLETE_STATUSES = frozenset({"needs_more_data", "missing_data"})


class ParameterReviewError(ValueError):
    """Raised when a parameter review evidence record violates governance schema."""


def build_parameter_review_evidence_record(
    *,
    candidate_id: str,
    experiment_id: str,
    source_pack_id: str,
    source_run_id: str,
    baseline_config_hash: str,
    candidate_config_hash: str,
    review_start_date: date | str,
    review_end_date: date | str,
    forward_days: int,
    evidence_sources: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, Any],
    journal_links: Sequence[Mapping[str, Any]] | None = None,
    weekly_review_links: Sequence[Mapping[str, Any]] | None = None,
    validation_status: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
    review_id: str | None = None,
    parameter_review_id: str | None = None,
    extra_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    start_text = _date_text(review_start_date)
    end_text = _date_text(review_end_date)
    safe_review_id = review_id or _stable_id(
        "etf-parameter-review",
        candidate_id,
        experiment_id,
        start_text,
        end_text,
    )
    safe_parameter_review_id = parameter_review_id or _stable_id(
        "parameter-review",
        candidate_id,
        source_run_id,
        end_text,
    )
    payload = {
        "schema_version": PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION,
        "record_type": "etf_parameter_review_evidence",
        "review_id": safe_review_id,
        "parameter_review_id": safe_parameter_review_id,
        "candidate_id": _text(candidate_id),
        "experiment_id": _text(experiment_id),
        "source_pack_id": _text(source_pack_id),
        "source_run_id": _text(source_run_id),
        "baseline_config_hash": _text(baseline_config_hash),
        "candidate_config_hash": _text(candidate_config_hash),
        "review_start_date": start_text,
        "review_end_date": end_text,
        "forward_days": int(forward_days),
        "generated_at": generated.isoformat(),
        "evidence_sources": [dict(item) for item in evidence_sources],
        "metrics": dict(metrics),
        "journal_links": [dict(item) for item in journal_links or []],
        "weekly_review_links": [dict(item) for item in weekly_review_links or []],
        "validation_status": dict(validation_status or {"status": "available"}),
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }
    payload.update(dict(extra_fields or {}))
    validate_parameter_review_evidence_record(payload)
    return payload


def validate_parameter_review_evidence_record(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(record.get("schema_version")) != PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                f"schema_version must be {PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION}",
            )
        )
    for field in REQUIRED_EVIDENCE_FIELDS:
        if field not in record:
            issues.append(_issue(field, f"required field missing: {field}"))
        elif field not in {"metrics", "evidence_sources", "journal_links", "weekly_review_links"}:
            if _text(record.get(field)) == "":
                issues.append(_issue(field, f"required field blank: {field}"))
    issues.extend(_safety_issues(record, owner_id=_text(record.get("candidate_id"), "record")))
    safety = _mapping(record.get("safety"))
    if not safety:
        issues.append(_issue("safety", "safety must be a mapping"))
    else:
        issues.extend(
            _safety_issues(safety, owner_id=f"{_text(record.get('candidate_id'), 'record')}.safety")
        )
    issues.extend(_date_range_issues(record))
    issues.extend(_metric_issues(_mapping(record.get("metrics"))))
    issues.extend(_evidence_source_issues(record))
    if not isinstance(record.get("journal_links"), list):
        issues.append(_issue("journal_links", "journal_links must be a list"))
    if not isinstance(record.get("weekly_review_links"), list):
        issues.append(_issue("weekly_review_links", "weekly_review_links must be a list"))
    if issues:
        raise ParameterReviewError(_format_issues(issues))
    return issues


def parameter_review_evidence_to_json(record: Mapping[str, Any]) -> str:
    validate_parameter_review_evidence_record(record)
    return json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_parameter_review_evidence_json(record: Mapping[str, Any], path: Path) -> Path:
    text = parameter_review_evidence_to_json(record)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _metric_issues(metrics: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if not metrics:
        return [_issue("metrics", "metrics must be a mapping")]
    null_reasons = _mapping(metrics.get("metric_null_reasons"))
    for field in REQUIRED_METRIC_FIELDS:
        if field not in metrics:
            issues.append(_issue(field, f"required metric missing: {field}"))
            continue
        if metrics.get(field) is None and not _text(null_reasons.get(field)):
            issues.append(
                _issue(
                    field,
                    f"metric {field} is null and must include metric_null_reasons.{field}",
                )
            )
    return issues


def _evidence_source_issues(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    sources = _records(record.get("evidence_sources"))
    if not isinstance(record.get("evidence_sources"), list):
        issues.append(_issue("evidence_sources", "evidence_sources must be a list"))
        return issues
    if not sources:
        issues.append(_issue("evidence_sources", "at least one evidence source link is required"))
        return issues
    source_types = set()
    for index, source in enumerate(sources):
        source_type = _text(source.get("source_type"))
        source_path = _text(source.get("source_report_path"))
        if not source_type:
            issues.append(_issue(f"evidence_sources[{index}].source_type", "source_type required"))
        if not source_path:
            issues.append(
                _issue(
                    f"evidence_sources[{index}].source_report_path",
                    "source_report_path required",
                )
            )
        if source_type:
            source_types.add(source_type)
    missing = sorted(REQUIRED_EVIDENCE_SOURCE_TYPES - source_types)
    if missing and not _missing_sources_are_explained(record, missing):
        issues.append(
            _issue(
                "evidence_sources",
                "missing required evidence source type(s) without needs_more_data reason: "
                + ", ".join(missing),
            )
        )
    return issues


def _missing_sources_are_explained(record: Mapping[str, Any], missing: Sequence[str]) -> bool:
    validation = _mapping(record.get("validation_status"))
    status = _text(validation.get("status"))
    reasons = _mapping(validation.get("missing_source_reasons"))
    return status in ALLOWED_INCOMPLETE_STATUSES and all(
        _text(reasons.get(item)) for item in missing
    )


def _date_range_issues(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    start = _safe_date(record.get("review_start_date"))
    end = _safe_date(record.get("review_end_date"))
    if start is None:
        issues.append(_issue("review_start_date", "review_start_date must be YYYY-MM-DD"))
    if end is None:
        issues.append(_issue("review_end_date", "review_end_date must be YYYY-MM-DD"))
    if start is not None and end is not None and start > end:
        issues.append(_issue("review_date_range", "review_start_date must be <= review_end_date"))
    try:
        forward_days = int(record.get("forward_days"))
    except (TypeError, ValueError):
        issues.append(_issue("forward_days", "forward_days must be an integer"))
    else:
        if forward_days < 0:
            issues.append(_issue("forward_days", "forward_days must be non-negative"))
    return issues


def _safety_issues(payload: Mapping[str, Any], *, owner_id: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for field, expected in PARAMETER_REVIEW_SAFETY.items():
        if payload.get(field) != expected:
            issues.append(
                _issue(field, f"{owner_id} safety field {field} must be {expected!r}")
            )
    return issues


def _stable_id(prefix: str, *parts: object) -> str:
    basis = "|".join(_text(part) for part in parts)
    return prefix + "-" + sha256(basis.encode("utf-8")).hexdigest()[:12]


def _date_text(value: date | str) -> str:
    return value.isoformat() if isinstance(value, date) else _text(value)


def _safe_date(value: object) -> date | None:
    try:
        return date.fromisoformat(_text(value))
    except ValueError:
        return None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _issue(check_id: str, message: str) -> dict[str, Any]:
    return {"check_id": check_id, "status": "FAIL", "message": message}


def _format_issues(issues: Sequence[Mapping[str, Any]]) -> str:
    return "; ".join(_text(issue.get("message"), "validation issue") for issue in issues)
