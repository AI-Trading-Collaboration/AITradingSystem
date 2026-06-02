from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
)

PARAMETER_REVIEW_EVIDENCE_SCHEMA_VERSION = "etf_parameter_review_evidence_v1"
PARAMETER_REVIEW_AGGREGATION_SCHEMA_VERSION = "etf_parameter_review_aggregation_v1"

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

SOURCE_REPORT_TYPES = {
    "etf_forward_dashboard": "forward_dashboard",
    "etf_weekly_review": "weekly_review",
    "etf_decision_journal_report": "decision_journal",
    "etf_experiment_comparison": "experiment_report",
    "etf_experiment_candidate_selection": "candidate_gate",
    "etf_forward_validation": "validation_gate",
    "etf_experiment_validation": "validation_gate",
    "etf_weekly_review_validation": "validation_gate",
    "etf_decision_journal_validation": "validation_gate",
    "etf_credibility_gate": "validation_gate",
    "etf_forward_weekly_review": "forward_weekly_review",
    "etf_forward_watchlist": "watchlist",
    "etf_shadow_candidates": "shadow_registry",
}

AGGREGATION_SOURCE_REPORT_IDS: tuple[str, ...] = tuple(SOURCE_REPORT_TYPES)
REQUIRED_AGGREGATION_REPORT_IDS = frozenset({"etf_forward_dashboard"})


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


def build_parameter_review_aggregation(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    report_registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    project_root: Path = PROJECT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    report_index = _load_or_build_report_index(
        as_of=as_of,
        report_index_payload=report_index_payload,
        report_index_path=report_index_path,
        report_registry_path=report_registry_path,
        project_root=project_root,
    )
    source_reports = [
        _source_report_record(report_index, report_id=report_id)
        for report_id in AGGREGATION_SOURCE_REPORT_IDS
    ]
    source_payloads = {
        _text(record.get("report_id")): _mapping(record.get("payload"))
        for record in source_reports
    }
    forward_dashboard = source_payloads.get("etf_forward_dashboard", {})
    candidate_rows = _records(forward_dashboard.get("candidate_summary_table"))
    missing_required_reports = [
        _source_report_public(record)
        for record in source_reports
        if record.get("report_id") in REQUIRED_AGGREGATION_REPORT_IDS
        and record.get("status") != "loaded"
    ]
    if missing_required_reports or not candidate_rows:
        status = "needs_more_data"
        reason = "INSUFFICIENT_FORWARD_EVIDENCE"
    else:
        status = "available"
        reason = ""
    evidence_records = []
    for row in candidate_rows:
        evidence_records.append(
            _evidence_record_from_forward_row(
                row,
                as_of=as_of,
                source_reports=source_reports,
                source_payloads=source_payloads,
                generated_at=generated,
            )
        )
    warnings = _aggregation_warnings(
        source_reports=source_reports,
        evidence_records=evidence_records,
        no_forward_rows=not candidate_rows,
    )
    blocking_warning_types = {
        "incomplete_evidence_record",
        "insufficient_forward_evidence",
    }
    if status == "available" and any(
        warning.get("warning_type") in blocking_warning_types for warning in warnings
    ):
        status = "needs_more_data"
        reason = "PARTIAL_EVIDENCE"
    payload = {
        "schema_version": PARAMETER_REVIEW_AGGREGATION_SCHEMA_VERSION,
        "report_type": "etf_parameter_review_evidence_aggregation",
        "review_id": f"etf-parameter-review-{as_of.isoformat()}",
        "parameter_review_id": f"parameter-review-{as_of.isoformat()}",
        "status": status,
        "reason": reason,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "evidence_record_count": len(evidence_records),
        "candidate_count": len(candidate_rows),
        "source_reports": [_source_report_public(record) for record in source_reports],
        "missing_required_sources": missing_required_reports,
        "warnings": warnings,
        "evidence_records": evidence_records,
        "source_payloads": source_payloads,
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }
    validate_parameter_review_aggregation(payload)
    return payload


def validate_parameter_review_aggregation(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(payload.get("schema_version")) != PARAMETER_REVIEW_AGGREGATION_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                f"schema_version must be {PARAMETER_REVIEW_AGGREGATION_SCHEMA_VERSION}",
            )
        )
    issues.extend(_safety_issues(payload, owner_id="parameter_review_aggregation"))
    safety = _mapping(payload.get("safety"))
    issues.extend(_safety_issues(safety, owner_id="parameter_review_aggregation.safety"))
    for record in _records(payload.get("evidence_records")):
        try:
            validate_parameter_review_evidence_record(record)
        except ParameterReviewError as exc:
            issues.append(
                _issue(
                    "evidence_record",
                    f"evidence record validation failed for {record.get('candidate_id')}: {exc}",
                )
            )
    if _text(payload.get("status")) == "available" and not _records(
        payload.get("evidence_records")
    ):
        issues.append(_issue("evidence_records", "available aggregation requires evidence records"))
    if issues:
        raise ParameterReviewError(_format_issues(issues))
    return issues


def write_parameter_review_aggregation(
    payload: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    validate_parameter_review_aggregation(payload)
    _write_json(payload, json_path)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(
        render_parameter_review_aggregation_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def render_parameter_review_aggregation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# ETF Parameter Review Evidence",
        "",
        "## Safety Banner",
        "",
        "- observe_only = true",
        "- candidate_only = true",
        "- production_effect = none",
        "- broker_action = none",
        "- manual_review_required = true",
        "- 本报告只汇总 forward evidence，不修改 production weights 或 broker state。",
        "",
        "## Review Metadata",
        "",
        f"- Status: {payload.get('status')}",
        f"- Reason: {_text(payload.get('reason'), 'none')}",
        f"- As Of: {payload.get('as_of')}",
        f"- Candidate Count: {payload.get('candidate_count')}",
        f"- Evidence Record Count: {payload.get('evidence_record_count')}",
        "",
        "## Evidence Source Summary",
        "",
        "| Source | Type | Status | Path |",
        "|---|---|---|---|",
    ]
    for source in _records(payload.get("source_reports")):
        lines.append(
            f"| {source.get('report_id')} | {source.get('source_type')} | "
            f"{source.get('status')} | {source.get('source_report_path')} |"
        )
    lines.extend(
        [
            "",
            "## Candidate Evidence Records",
            "",
            (
                "| Candidate | Experiment | Forward Days | Excess vs Baseline | "
                "Journal Reviews | Status |"
            ),
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for record in _records(payload.get("evidence_records")):
        metrics = _mapping(record.get("metrics"))
        validation = _mapping(record.get("validation_status"))
        lines.append(
            f"| {record.get('candidate_id')} | {record.get('experiment_id')} | "
            f"{record.get('forward_days')} | "
            f"{_fmt_number(metrics.get('excess_return_vs_baseline'))} | "
            f"{metrics.get('manual_review_count')} | {validation.get('status')} |"
        )
    if not _records(payload.get("evidence_records")):
        lines.append("| none | none | 0 | n/a | 0 | needs_more_data |")
    lines.extend(["", "## Warnings", ""])
    warnings = _records(payload.get("warnings"))
    if warnings:
        lines.extend(
            (
                f"- {warning.get('warning_type')}: "
                f"{warning.get('reason_code') or warning.get('message')}"
            )
            for warning in warnings
        )
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def _load_or_build_report_index(
    *,
    as_of: date,
    report_index_payload: Mapping[str, Any] | None,
    report_index_path: Path | None,
    report_registry_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    if report_index_payload is not None:
        return dict(report_index_payload)
    if report_index_path is not None and report_index_path.exists():
        payload = _read_json_object(report_index_path)
        return payload
    return build_report_index_payload(
        as_of=as_of,
        project_root=project_root,
        registry_path=report_registry_path,
    )


def _source_report_record(
    report_index: Mapping[str, Any],
    *,
    report_id: str,
) -> dict[str, Any]:
    index_record = _report_index_record(report_index, report_id)
    path = _path_or_none(index_record.get("latest_artifact_path"))
    payload = _read_json_object(path) if path is not None else {}
    loaded = bool(payload)
    if loaded:
        status = "loaded"
    elif path is not None and path.exists():
        status = "available_unstructured"
    else:
        status = "missing_data"
    return {
        "report_id": report_id,
        "source_type": SOURCE_REPORT_TYPES[report_id],
        "status": status,
        "loaded": loaded,
        "required": report_id in REQUIRED_AGGREGATION_REPORT_IDS,
        "source_report_path": "" if path is None else str(path),
        "artifact_status": _text(index_record.get("artifact_status"), "MISSING"),
        "freshness_status": _text(index_record.get("freshness_status"), "MISSING"),
        "reason_code": "LOADED" if loaded else "REPORT_NOT_FOUND",
        "payload": payload,
    }


def _source_report_public(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.items()
        if key not in {"payload"} and key in {
            "report_id",
            "source_type",
            "status",
            "loaded",
            "required",
            "source_report_path",
            "artifact_status",
            "freshness_status",
            "reason_code",
        }
    }


def _evidence_record_from_forward_row(
    row: Mapping[str, Any],
    *,
    as_of: date,
    source_reports: Sequence[Mapping[str, Any]],
    source_payloads: Mapping[str, Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    candidate_id = _text(row.get("candidate_id"), "UNKNOWN_CANDIDATE")
    experiment_id = _text(row.get("experiment_id"), "UNKNOWN_EXPERIMENT")
    shadow_candidate = _candidate_metadata(
        source_payloads.get("etf_shadow_candidates", {}),
        candidate_id=candidate_id,
        experiment_id=experiment_id,
    )
    selection_candidate = _candidate_metadata(
        source_payloads.get("etf_experiment_candidate_selection", {}),
        candidate_id=candidate_id,
        experiment_id=experiment_id,
        candidate_key="candidates",
    )
    selection_metadata = _mapping(
        source_payloads.get("etf_experiment_candidate_selection", {}).get("run_metadata")
    )
    source_run_id = (
        _text(shadow_candidate.get("source_run_id"))
        or _text(selection_candidate.get("source_run_id"))
        or _text(selection_metadata.get("run_id"))
        or _candidate_id_run_prefix(candidate_id)
        or "UNKNOWN_SOURCE_RUN"
    )
    source_pack_id = (
        _text(shadow_candidate.get("source_pack_id"))
        or _text(selection_metadata.get("pack_id"))
        or "UNKNOWN_SOURCE_PACK"
    )
    candidate_config_hash = (
        _text(shadow_candidate.get("config_hash"))
        or _text(selection_candidate.get("config_hash"))
        or _text(row.get("config_hash"))
        or "UNKNOWN_CANDIDATE_CONFIG_HASH"
    )
    baseline_config_hash = (
        _text(source_payloads.get("etf_forward_dashboard", {}).get("baseline_config_hash"))
        or _text(selection_metadata.get("config_hash"))
        or "UNKNOWN_BASELINE_CONFIG_HASH"
    )
    forward_days = _int_or_default(row.get("days_since_enrollment"), 0)
    review_end = _safe_date(row.get("last_evaluated_date")) or _safe_date(
        source_payloads.get("etf_forward_dashboard", {}).get("as_of")
    ) or as_of
    review_start = _safe_date(shadow_candidate.get("enrollment_date")) or _safe_date(
        shadow_candidate.get("start_date")
    ) or review_end - timedelta(days=max(forward_days, 0))
    metrics = _metrics_from_forward_row(
        row,
        forward_days=forward_days,
        journal_entries=_journal_entries_for_candidate(
            source_payloads.get("etf_decision_journal_report", {}),
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        ),
    )
    evidence_sources, missing_source_reasons = _candidate_evidence_sources(
        source_reports,
        candidate_id=candidate_id,
    )
    validation_status = _validation_status_for_record(
        source_payloads=source_payloads,
        missing_source_reasons=missing_source_reasons,
    )
    return build_parameter_review_evidence_record(
        candidate_id=candidate_id,
        experiment_id=experiment_id,
        source_pack_id=source_pack_id,
        source_run_id=source_run_id,
        baseline_config_hash=baseline_config_hash,
        candidate_config_hash=candidate_config_hash,
        review_start_date=review_start,
        review_end_date=review_end,
        forward_days=forward_days,
        evidence_sources=evidence_sources,
        metrics=metrics,
        journal_links=_journal_links_for_candidate(
            source_payloads.get("etf_decision_journal_report", {}),
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        ),
        weekly_review_links=_weekly_review_links_for_candidate(
            source_payloads.get("etf_weekly_review", {}),
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        ),
        validation_status=validation_status,
        generated_at=generated_at,
        extra_fields={
            "shadow_id": row.get("shadow_id"),
            "source_forward_status": row.get("status"),
        },
    )


def _metrics_from_forward_row(
    row: Mapping[str, Any],
    *,
    forward_days: int,
    journal_entries: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    metric_null_reasons = dict(_mapping(row.get("metric_null_reasons")))
    accepted = sum(
        1 for entry in journal_entries if _decision_status(entry) == "accept_recommendation"
    )
    rejected = sum(
        1 for entry in journal_entries if _decision_status(entry) == "reject_recommendation"
    )
    deferred = sum(
        1 for entry in journal_entries if _text(entry.get("decision_status")) == "defer_decision"
    )
    constraint_hit_rate = _float_or_none(row.get("constraint_hit_rate"))
    if constraint_hit_rate is None:
        hits = _float_or_none(row.get("constraint_hits_since_enrollment"))
        if hits is not None and forward_days > 0:
            constraint_hit_rate = hits / forward_days
    metrics = {
        "return_since_enrollment": _float_or_none(row.get("return_since_enrollment")),
        "excess_return_vs_baseline": _float_or_none(row.get("excess_return_vs_baseline")),
        "excess_return_vs_QQQ": _float_or_none(row.get("excess_return_vs_QQQ")),
        "excess_return_vs_SPY": _float_or_none(row.get("excess_return_vs_SPY")),
        "excess_return_vs_SMH": _float_or_none(row.get("excess_return_vs_SMH")),
        "max_drawdown_since_enrollment": _float_or_none(
            row.get("max_drawdown_since_enrollment")
        ),
        "drawdown_delta_vs_baseline": _float_or_none(row.get("drawdown_delta_vs_baseline")),
        "turnover_since_enrollment": _float_or_none(row.get("turnover_since_enrollment")),
        "turnover_delta_vs_baseline": _float_or_none(row.get("turnover_delta_vs_baseline")),
        "constraint_hit_rate": constraint_hit_rate,
        "regime_transition_count": _float_or_none(row.get("regime_transition_count")),
        "weight_stability_score": _float_or_none(row.get("weight_stability_score")),
        "data_coverage_ratio": _data_coverage_ratio(row),
        "manual_review_count": len(journal_entries),
        "accepted_review_count": accepted,
        "rejected_review_count": rejected,
        "deferred_review_count": deferred,
        "metric_null_reasons": metric_null_reasons,
    }
    for field in REQUIRED_METRIC_FIELDS:
        if metrics.get(field) is None:
            metric_null_reasons.setdefault(field, f"{field} unavailable in forward evidence")
    metrics["metric_null_reasons"] = metric_null_reasons
    return metrics


def _candidate_evidence_sources(
    source_reports: Sequence[Mapping[str, Any]],
    *,
    candidate_id: str,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    sources = []
    missing_reasons: dict[str, str] = {}
    loaded_types: set[str] = set()
    for source in source_reports:
        source_type = _text(source.get("source_type"))
        if source.get("loaded") is not True:
            continue
        loaded_types.add(source_type)
        sources.append(
            {
                "source_type": source_type,
                "source_module": _text(source.get("report_id")),
                "source_report_path": _text(source.get("source_report_path")),
                "source_metric": _source_metric_for_type(source_type),
                "time_window": candidate_id,
                "reason_code": "LOADED",
            }
        )
    for source_type in sorted(REQUIRED_EVIDENCE_SOURCE_TYPES - loaded_types):
        missing_reasons.setdefault(source_type, "REPORT_NOT_FOUND")
    return sources, missing_reasons


def _validation_status_for_record(
    *,
    source_payloads: Mapping[str, Mapping[str, Any]],
    missing_source_reasons: Mapping[str, str],
) -> dict[str, Any]:
    validation_payloads = [
        source_payloads.get(report_id, {})
        for report_id, source_type in SOURCE_REPORT_TYPES.items()
        if source_type == "validation_gate"
    ]
    gates = [
        {
            "report_type": payload.get("report_type"),
            "status": _text(payload.get("status"), "MISSING"),
        }
        for payload in validation_payloads
        if payload
    ]
    failed = [gate for gate in gates if gate["status"] not in {"PASS", "PASS_WITH_WARNINGS"}]
    if missing_source_reasons:
        status = "needs_more_data"
    elif failed:
        status = "blocked"
    else:
        status = "available"
    return {
        "status": status,
        "gates": gates,
        "missing_source_reasons": dict(missing_source_reasons),
    }


def _journal_links_for_candidate(
    journal_report: Mapping[str, Any],
    *,
    candidate_id: str,
    experiment_id: str,
) -> list[dict[str, Any]]:
    source_path = _text(journal_report.get("source_journal_path")) or _text(
        journal_report.get("source_report_path")
    )
    links = []
    for entry in _journal_entries_for_candidate(
        journal_report,
        candidate_id=candidate_id,
        experiment_id=experiment_id,
    ):
        links.append(
            {
                "decision_id": entry.get("decision_id"),
                "decision_status": entry.get("decision_status"),
                "confidence": entry.get("confidence"),
                "source_weekly_review": entry.get("source_weekly_review"),
                "source_report_path": _text(entry.get("linked_report"), source_path),
            }
        )
    return links


def _journal_entries_for_candidate(
    journal_report: Mapping[str, Any],
    *,
    candidate_id: str,
    experiment_id: str,
) -> list[dict[str, Any]]:
    entries = _records(journal_report.get("entries"))
    return [
        entry
        for entry in entries
        if _candidate_matches(
            _text(entry.get("linked_candidate")),
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        )
    ]


def _weekly_review_links_for_candidate(
    weekly_review: Mapping[str, Any],
    *,
    candidate_id: str,
    experiment_id: str,
) -> list[dict[str, Any]]:
    links = []
    review_id = _text(weekly_review.get("review_id"))
    shadow = _mapping(_mapping(weekly_review.get("sections")).get("shadow_candidate_review"))
    rows = _records(shadow.get("active_shadow_candidates"))
    for row in rows:
        row_candidate = _text(row.get("candidate_id"))
        row_experiment = _text(row.get("experiment_id"))
        matches_candidate = _candidate_matches(
            row_candidate,
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        )
        if not (
            matches_candidate or row_experiment == experiment_id
        ):
            continue
        links.append(
            {
                "review_id": review_id,
                "candidate_id": row_candidate,
                "recommended_observation_action": row.get("recommended_observation_action"),
                "source_report_path": _text(shadow.get("source_report_path")),
            }
        )
    return links


def _candidate_metadata(
    payload: Mapping[str, Any],
    *,
    candidate_id: str,
    experiment_id: str,
    candidate_key: str = "candidates",
) -> dict[str, Any]:
    for candidate in _records(payload.get(candidate_key)):
        if _candidate_matches(
            _text(candidate.get("candidate_id")),
            candidate_id=candidate_id,
            experiment_id=experiment_id,
        ) or _text(candidate.get("experiment_id")) == experiment_id:
            return candidate
    return {}


def _candidate_matches(value: str, *, candidate_id: str, experiment_id: str) -> bool:
    return value in {candidate_id, experiment_id} or value.endswith(f":{experiment_id}")


def _decision_status(entry: Mapping[str, Any]) -> str:
    return _text(entry.get("decision_status"))


def _candidate_id_run_prefix(candidate_id: str) -> str:
    return candidate_id.split(":", 1)[0] if ":" in candidate_id else ""


def _source_metric_for_type(source_type: str) -> str:
    return {
        "forward_dashboard": "candidate_summary_table",
        "weekly_review": "manual_review_actions",
        "decision_journal": "entries",
        "experiment_report": "metrics_table",
        "candidate_gate": "candidates",
        "validation_gate": "status",
        "forward_weekly_review": "candidate_status_changes",
        "watchlist": "attention_required",
        "shadow_registry": "candidates",
    }.get(source_type, "status")


def _aggregation_warnings(
    *,
    source_reports: Sequence[Mapping[str, Any]],
    evidence_records: Sequence[Mapping[str, Any]],
    no_forward_rows: bool,
) -> list[dict[str, Any]]:
    warnings = []
    for source in source_reports:
        if source.get("status") != "loaded":
            warnings.append(
                {
                    "warning_type": "missing_source",
                    "report_id": source.get("report_id"),
                    "source_type": source.get("source_type"),
                    "reason_code": source.get("reason_code"),
                    "source_report_path": source.get("source_report_path"),
                }
            )
    if no_forward_rows:
        warnings.append(
            {
                "warning_type": "insufficient_forward_evidence",
                "reason_code": "INSUFFICIENT_FORWARD_EVIDENCE",
                "message": "No forward dashboard candidate_summary_table rows were available.",
            }
        )
    for record in evidence_records:
        validation = _mapping(record.get("validation_status"))
        if validation.get("status") != "available":
            warnings.append(
                {
                    "warning_type": "incomplete_evidence_record",
                    "candidate_id": record.get("candidate_id"),
                    "reason_code": "PARTIAL_EVIDENCE",
                    "missing_source_reasons": validation.get("missing_source_reasons"),
                }
            )
    return warnings


def _report_index_record(payload: Mapping[str, Any], report_id: str) -> dict[str, Any]:
    for report in _records(payload.get("reports")):
        if _text(report.get("report_id")) == report_id:
            return report
    return {
        "report_id": report_id,
        "latest_artifact_path": "",
        "artifact_status": "MISSING",
        "freshness_status": "MISSING",
    }


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(payload: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
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


def _path_or_none(value: object) -> Path | None:
    text = _text(value)
    return None if not text else Path(text)


def _float_or_none(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed != parsed:
        return None
    return parsed


def _int_or_default(value: object, default: int) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return default


def _data_coverage_ratio(row: Mapping[str, Any]) -> float:
    required_forward_fields = (
        "return_since_enrollment",
        "excess_return_vs_baseline",
        "excess_return_vs_QQQ",
        "excess_return_vs_SPY",
        "max_drawdown_since_enrollment",
        "turnover_since_enrollment",
    )
    available = sum(
        1 for field in required_forward_fields if _float_or_none(row.get(field)) is not None
    )
    return round(available / len(required_forward_fields), 6)


def _fmt_number(value: object) -> str:
    parsed = _float_or_none(value)
    return "n/a" if parsed is None else f"{parsed:.4f}"


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
