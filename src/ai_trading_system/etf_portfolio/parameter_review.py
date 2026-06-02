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
PARAMETER_REVIEW_COMPARISON_SCHEMA_VERSION = "etf_parameter_review_candidate_comparison_v1"
PARAMETER_REVIEW_JOURNAL_LINKAGE_SCHEMA_VERSION = (
    "etf_parameter_review_journal_linkage_v1"
)
PARAMETER_REVIEW_PROPOSAL_SCHEMA_VERSION = "etf_parameter_review_proposals_v1"

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

# Pilot comparison policy for observe-only TRADING-070 evidence triage. These values do not
# approve production changes; TRADING-070F owns the later fail-closed governance gate.
PARAMETER_REVIEW_COMPARISON_POLICY = {
    "policy_id": "etf_parameter_review_comparison_v1",
    "owner": "TRADING-070",
    "status": "pilot_baseline",
    "rationale": (
        "Classify candidate-only forward evidence before manual review without mutating "
        "production ETF allocation."
    ),
    "intended_effect": (
        "Separate outperforming, risky, underperforming, mixed, blocked and "
        "needs-more-data candidates."
    ),
    "validation_evidence": "Focused deterministic TRADING-070C tests.",
    "review_condition": (
        "Revisit after TRADING-070F governance scoring and real weekly owner review evidence."
    ),
    "min_forward_days": 20,
    "min_data_coverage_ratio": 0.8,
    "high_turnover_since_enrollment": 1.0,
    "high_turnover_delta_vs_baseline": 0.5,
    "high_drawdown_worsening_vs_baseline": -0.02,
    "high_constraint_hit_rate": 0.25,
    "backtest_gap_warning_abs": 0.05,
}

PARAMETER_REVIEW_COMPARISON_STATUSES = frozenset(
    {
        "outperforming_with_acceptable_risk",
        "outperforming_but_risky",
        "underperforming",
        "needs_more_data",
        "mixed_evidence",
        "blocked_by_governance",
    }
)

PARAMETER_REVIEW_HUMAN_SUPPORT_STATUSES = frozenset(
    {"supportive", "neutral", "conflicted", "negative", "insufficient_review"}
)

SUPPORTIVE_DECISION_STATUSES = frozenset({"accept_recommendation", "continue_observation"})
NEGATIVE_DECISION_STATUSES = frozenset(
    {"reject_recommendation", "archive_candidate_after_review"}
)
NEUTRAL_DECISION_STATUSES = frozenset(
    {"defer_decision", "mark_watch", "request_more_data", "start_new_experiment"}
)

ALLOWED_PARAMETER_REVIEW_PROPOSAL_TYPES = frozenset(
    {
        "continue_observation",
        "defer_parameter_change",
        "reject_candidate",
        "propose_candidate_for_extended_shadow",
        "propose_baseline_parameter_review",
    }
)

DISALLOWED_PARAMETER_REVIEW_PROPOSAL_TYPES = frozenset(
    {"apply_baseline_change", "promote_to_production", "enable_broker_action"}
)


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


def compare_parameter_review_evidence(
    aggregation_payload: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
    policy: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    validate_parameter_review_aggregation(aggregation_payload)
    generated = generated_at or datetime.now(tz=UTC)
    comparison_policy = _comparison_policy(policy)
    source_payloads = _mapping(aggregation_payload.get("source_payloads"))
    comparisons = [
        _candidate_evidence_comparison(
            record,
            source_payloads=source_payloads,
            policy=comparison_policy,
        )
        for record in _records(aggregation_payload.get("evidence_records"))
    ]
    if _text(aggregation_payload.get("status")) == "needs_more_data" and not comparisons:
        status = "needs_more_data"
        reason = _text(aggregation_payload.get("reason"), "INSUFFICIENT_FORWARD_EVIDENCE")
    else:
        status = "available"
        reason = ""
    payload = {
        "schema_version": PARAMETER_REVIEW_COMPARISON_SCHEMA_VERSION,
        "report_type": "etf_parameter_review_candidate_comparison",
        "comparison_id": _stable_id(
            "etf-parameter-review-comparison",
            aggregation_payload.get("parameter_review_id"),
            generated.isoformat(),
        ),
        "parameter_review_id": aggregation_payload.get("parameter_review_id"),
        "source_review_id": aggregation_payload.get("review_id"),
        "status": status,
        "reason": reason,
        "as_of": aggregation_payload.get("as_of"),
        "generated_at": generated.isoformat(),
        "policy": comparison_policy,
        "summary": _comparison_summary(comparisons),
        "comparisons": comparisons,
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }
    validate_parameter_review_comparison(payload)
    return payload


def validate_parameter_review_comparison(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(payload.get("schema_version")) != PARAMETER_REVIEW_COMPARISON_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                f"schema_version must be {PARAMETER_REVIEW_COMPARISON_SCHEMA_VERSION}",
            )
        )
    issues.extend(_safety_issues(payload, owner_id="parameter_review_comparison"))
    issues.extend(
        _safety_issues(
            _mapping(payload.get("safety")),
            owner_id="parameter_review_comparison.safety",
        )
    )
    for index, item in enumerate(_records(payload.get("comparisons"))):
        status = _text(item.get("status"))
        if status not in PARAMETER_REVIEW_COMPARISON_STATUSES:
            issues.append(
                _issue(
                    f"comparisons[{index}].status",
                    f"unsupported comparison status: {status}",
                )
            )
        issues.extend(_safety_issues(item, owner_id=f"comparison[{index}]"))
    if _text(payload.get("status")) == "available" and not isinstance(
        payload.get("comparisons"),
        list,
    ):
        issues.append(_issue("comparisons", "comparisons must be a list"))
    if issues:
        raise ParameterReviewError(_format_issues(issues))
    return issues


def link_decision_journal_evidence(
    aggregation_payload: Mapping[str, Any],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_parameter_review_aggregation(aggregation_payload)
    generated = generated_at or datetime.now(tz=UTC)
    source_payloads = _mapping(aggregation_payload.get("source_payloads"))
    journal_report = _mapping(source_payloads.get("etf_decision_journal_report"))
    candidates = [
        _candidate_journal_linkage(
            record,
            journal_report=journal_report,
        )
        for record in _records(aggregation_payload.get("evidence_records"))
    ]
    payload = {
        "schema_version": PARAMETER_REVIEW_JOURNAL_LINKAGE_SCHEMA_VERSION,
        "report_type": "etf_parameter_review_journal_linkage",
        "linkage_id": _stable_id(
            "etf-parameter-review-journal-linkage",
            aggregation_payload.get("parameter_review_id"),
            generated.isoformat(),
        ),
        "parameter_review_id": aggregation_payload.get("parameter_review_id"),
        "source_review_id": aggregation_payload.get("review_id"),
        "status": "needs_more_data" if not candidates else "available",
        "reason": "INSUFFICIENT_FORWARD_EVIDENCE" if not candidates else "",
        "as_of": aggregation_payload.get("as_of"),
        "generated_at": generated.isoformat(),
        "candidate_count": len(candidates),
        "linked_journal_entry_count": sum(
            len(_records(item.get("linked_journal_entries"))) for item in candidates
        ),
        "support_status_counts": _support_status_counts(candidates),
        "candidate_journal_evidence": candidates,
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }
    validate_decision_journal_evidence_links(payload)
    return payload


def validate_decision_journal_evidence_links(
    payload: Mapping[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(payload.get("schema_version")) != PARAMETER_REVIEW_JOURNAL_LINKAGE_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                f"schema_version must be {PARAMETER_REVIEW_JOURNAL_LINKAGE_SCHEMA_VERSION}",
            )
        )
    issues.extend(_safety_issues(payload, owner_id="parameter_review_journal_linkage"))
    issues.extend(
        _safety_issues(
            _mapping(payload.get("safety")),
            owner_id="parameter_review_journal_linkage.safety",
        )
    )
    for index, item in enumerate(_records(payload.get("candidate_journal_evidence"))):
        status = _text(item.get("human_support_status"))
        if status not in PARAMETER_REVIEW_HUMAN_SUPPORT_STATUSES:
            issues.append(
                _issue(
                    f"candidate_journal_evidence[{index}].human_support_status",
                    f"unsupported human_support_status: {status}",
                )
            )
        issues.extend(_safety_issues(item, owner_id=f"journal_linkage[{index}]"))
        for entry_index, entry in enumerate(_records(item.get("linked_journal_entries"))):
            if not _text(entry.get("decision_id")):
                issues.append(
                    _issue(
                        f"candidate_journal_evidence[{index}].entries[{entry_index}]",
                        "linked journal entry decision_id is required",
                    )
                )
    if issues:
        raise ParameterReviewError(_format_issues(issues))
    return issues


def generate_parameter_change_proposals(
    aggregation_payload: Mapping[str, Any],
    *,
    comparison_payload: Mapping[str, Any] | None = None,
    journal_linkage_payload: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    validate_parameter_review_aggregation(aggregation_payload)
    generated = generated_at or datetime.now(tz=UTC)
    comparison = comparison_payload or compare_parameter_review_evidence(
        aggregation_payload,
        generated_at=generated,
    )
    journal_linkage = journal_linkage_payload or link_decision_journal_evidence(
        aggregation_payload,
        generated_at=generated,
    )
    validate_parameter_review_comparison(comparison)
    validate_decision_journal_evidence_links(journal_linkage)
    evidence_by_candidate = {
        _text(record.get("candidate_id")): record
        for record in _records(aggregation_payload.get("evidence_records"))
    }
    journal_by_candidate = {
        _text(item.get("candidate_id")): item
        for item in _records(journal_linkage.get("candidate_journal_evidence"))
    }
    proposals = [
        _parameter_change_proposal(
            comparison_item,
            evidence_record=evidence_by_candidate.get(
                _text(comparison_item.get("candidate_id")),
                {},
            ),
            journal_evidence=journal_by_candidate.get(
                _text(comparison_item.get("candidate_id")),
                {},
            ),
            created_at=generated,
        )
        for comparison_item in _records(comparison.get("comparisons"))
    ]
    if not proposals and _text(aggregation_payload.get("status")) == "needs_more_data":
        status = "needs_more_data"
        reason = _text(aggregation_payload.get("reason"), "INSUFFICIENT_FORWARD_EVIDENCE")
    else:
        status = "available"
        reason = ""
    payload = {
        "schema_version": PARAMETER_REVIEW_PROPOSAL_SCHEMA_VERSION,
        "report_type": "etf_parameter_review_proposals",
        "proposal_batch_id": _stable_id(
            "etf-parameter-review-proposals",
            aggregation_payload.get("parameter_review_id"),
            generated.isoformat(),
        ),
        "parameter_review_id": aggregation_payload.get("parameter_review_id"),
        "source_review_id": aggregation_payload.get("review_id"),
        "status": status,
        "reason": reason,
        "as_of": aggregation_payload.get("as_of"),
        "created_at": generated.isoformat(),
        "proposal_count": len(proposals),
        "proposal_type_counts": _proposal_type_counts(proposals),
        "proposals": proposals,
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }
    validate_parameter_change_proposals(payload)
    return payload


def validate_parameter_change_proposals(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if _text(payload.get("schema_version")) != PARAMETER_REVIEW_PROPOSAL_SCHEMA_VERSION:
        issues.append(
            _issue(
                "schema_version",
                f"schema_version must be {PARAMETER_REVIEW_PROPOSAL_SCHEMA_VERSION}",
            )
        )
    issues.extend(_safety_issues(payload, owner_id="parameter_review_proposals"))
    issues.extend(
        _safety_issues(
            _mapping(payload.get("safety")),
            owner_id="parameter_review_proposals.safety",
        )
    )
    for index, proposal in enumerate(_records(payload.get("proposals"))):
        proposal_type = _text(proposal.get("proposal_type"))
        if proposal_type not in ALLOWED_PARAMETER_REVIEW_PROPOSAL_TYPES:
            issues.append(
                _issue(
                    f"proposals[{index}].proposal_type",
                    f"unsupported proposal_type: {proposal_type}",
                )
            )
        if proposal_type in DISALLOWED_PARAMETER_REVIEW_PROPOSAL_TYPES:
            issues.append(
                _issue(
                    f"proposals[{index}].proposal_type",
                    f"disallowed proposal_type: {proposal_type}",
                )
            )
        issues.extend(_safety_issues(proposal, owner_id=f"proposal[{index}]"))
        if proposal.get("production_effect") != "none":
            issues.append(
                _issue(
                    f"proposals[{index}].production_effect",
                    "proposal production_effect must remain none",
                )
            )
        if proposal.get("broker_action") != "none":
            issues.append(
                _issue(
                    f"proposals[{index}].broker_action",
                    "proposal broker_action must remain none",
                )
            )
    if issues:
        raise ParameterReviewError(_format_issues(issues))
    return issues


def _candidate_evidence_comparison(
    record: Mapping[str, Any],
    *,
    source_payloads: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    metrics = _mapping(record.get("metrics"))
    experiment_id = _text(record.get("experiment_id"))
    experiment_row = _experiment_metrics_row(
        _mapping(source_payloads.get("etf_experiment_comparison")),
        experiment_id=experiment_id,
    )
    comparison_metrics = _comparison_metrics(metrics, experiment_row)
    journal_outcome = _journal_outcome_summary(record)
    weekly_status = _weekly_review_status(record)
    status, reason_codes = _classify_candidate_comparison(
        record,
        comparison_metrics=comparison_metrics,
        journal_outcome=journal_outcome,
        weekly_status=weekly_status,
        policy=policy,
    )
    return {
        "candidate_id": record.get("candidate_id"),
        "experiment_id": experiment_id,
        "status": status,
        "reason_codes": reason_codes,
        "comparison_metrics": comparison_metrics,
        "baseline_comparison": {
            "excess_return": comparison_metrics.get("excess_return"),
            "drawdown_reduction": comparison_metrics.get("drawdown_reduction"),
            "status": _comparison_direction(comparison_metrics.get("excess_return")),
        },
        "benchmark_comparison": {
            "QQQ": {
                "excess_return": _float_or_none(metrics.get("excess_return_vs_QQQ")),
                "status": _comparison_direction(metrics.get("excess_return_vs_QQQ")),
            },
            "SPY": {
                "excess_return": _float_or_none(metrics.get("excess_return_vs_SPY")),
                "status": _comparison_direction(metrics.get("excess_return_vs_SPY")),
            },
            "SMH": {
                "excess_return": _float_or_none(metrics.get("excess_return_vs_SMH")),
                "status": _comparison_direction(metrics.get("excess_return_vs_SMH")),
            },
        },
        "backtest_expectation": {
            "experiment_metric_status": "available" if experiment_row else "missing_data",
            "expected_excess_return_vs_baseline": _float_or_none(
                experiment_row.get("excess_return_vs_baseline")
            ),
            "forward_vs_backtest_gap": comparison_metrics.get("forward_vs_backtest_gap"),
        },
        "weekly_review_status": weekly_status,
        "human_decision_outcomes": journal_outcome,
        "evidence_source_count": len(_records(record.get("evidence_sources"))),
        "source_review_id": record.get("review_id"),
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }


def _comparison_metrics(
    metrics: Mapping[str, Any],
    experiment_row: Mapping[str, Any],
) -> dict[str, Any]:
    excess_return = _float_or_none(metrics.get("excess_return_vs_baseline"))
    expected_excess = _float_or_none(experiment_row.get("excess_return_vs_baseline"))
    return {
        "excess_return": excess_return,
        "drawdown_reduction": _float_or_none(metrics.get("drawdown_delta_vs_baseline")),
        "turnover_delta": _float_or_none(metrics.get("turnover_delta_vs_baseline")),
        "turnover_since_enrollment": _float_or_none(metrics.get("turnover_since_enrollment")),
        "stability_delta": _float_or_none(metrics.get("weight_stability_score")),
        "constraint_hit_delta": _float_or_none(metrics.get("constraint_hit_rate")),
        "regime_behavior": _regime_behavior(metrics.get("regime_transition_count")),
        "forward_vs_backtest_gap": _subtract_optional(excess_return, expected_excess),
        "journal_support_ratio": _journal_support_ratio(metrics),
        "data_coverage_ratio": _float_or_none(metrics.get("data_coverage_ratio")),
    }


def _classify_candidate_comparison(
    record: Mapping[str, Any],
    *,
    comparison_metrics: Mapping[str, Any],
    journal_outcome: Mapping[str, Any],
    weekly_status: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> tuple[str, list[str]]:
    reason_codes: list[str] = []
    if _safety_issues(record, owner_id=_text(record.get("candidate_id"), "record")):
        return "blocked_by_governance", ["UNSAFE_PARAMETER_REVIEW_RECORD"]
    validation = _mapping(record.get("validation_status"))
    validation_status = _text(validation.get("status"), "available")
    if validation_status == "blocked":
        return "blocked_by_governance", ["VALIDATION_GATE_BLOCKED"]
    forward_days = _int_or_default(record.get("forward_days"), 0)
    min_days = int(policy["min_forward_days"])
    if forward_days < min_days:
        reason_codes.append(f"INSUFFICIENT_FORWARD_DAYS:{forward_days}<{min_days}")
    if validation_status in ALLOWED_INCOMPLETE_STATUSES:
        reason_codes.append("PARTIAL_EVIDENCE")
    if comparison_metrics.get("excess_return") is None:
        reason_codes.append("NO_BASELINE_COMPARISON")
    coverage = comparison_metrics.get("data_coverage_ratio")
    min_coverage = float(policy["min_data_coverage_ratio"])
    if coverage is None or coverage < min_coverage:
        reason_codes.append(f"LOW_DATA_COVERAGE:{_fmt_number(coverage)}<{min_coverage:.4f}")
    if reason_codes:
        return "needs_more_data", reason_codes
    risk_flags = _risk_reason_codes(comparison_metrics, policy)
    reason_codes.extend(risk_flags)
    excess = float(comparison_metrics["excess_return"])
    backtest_gap = comparison_metrics.get("forward_vs_backtest_gap")
    gap_limit = float(policy["backtest_gap_warning_abs"])
    if backtest_gap is not None and backtest_gap <= -gap_limit:
        reason_codes.append("FORWARD_UNDERPERFORMED_BACKTEST_EXPECTATION")
    weekly_action = _text(weekly_status.get("latest_recommended_action"))
    if weekly_action in {"reject_candidate", "reject_pending_review", "archive_after_review"}:
        reason_codes.append("WEEKLY_REVIEW_NEGATIVE")
    support_status = _text(journal_outcome.get("human_support_status"))
    if excess > 0:
        if risk_flags:
            return "outperforming_but_risky", reason_codes
        if support_status in {"negative", "conflicted"} or "WEEKLY_REVIEW_NEGATIVE" in reason_codes:
            reason_codes.append("FORWARD_EVIDENCE_CONFLICTS_WITH_HUMAN_OR_WEEKLY_REVIEW")
            return "mixed_evidence", reason_codes
        return "outperforming_with_acceptable_risk", reason_codes or ["FORWARD_OUTPERFORMANCE"]
    if support_status == "supportive":
        reason_codes.append("JOURNAL_SUPPORT_CONFLICTS_WITH_FORWARD")
        return "mixed_evidence", reason_codes
    if backtest_gap is not None and backtest_gap > gap_limit:
        reason_codes.append("FORWARD_EXCEEDED_BACKTEST_BUT_BASELINE_EXCESS_NONPOSITIVE")
        return "mixed_evidence", reason_codes
    reason_codes.append("FORWARD_UNDERPERFORMED_BASELINE")
    return "underperforming", reason_codes


def _risk_reason_codes(
    metrics: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    turnover_delta = _float_or_none(metrics.get("turnover_delta"))
    turnover = _float_or_none(metrics.get("turnover_since_enrollment"))
    if (
        turnover_delta is not None
        and turnover_delta > float(policy["high_turnover_delta_vs_baseline"])
    ) or (
        turnover is not None and turnover > float(policy["high_turnover_since_enrollment"])
    ):
        reasons.append("HIGH_TURNOVER")
    drawdown_reduction = _float_or_none(metrics.get("drawdown_reduction"))
    if (
        drawdown_reduction is not None
        and drawdown_reduction < float(policy["high_drawdown_worsening_vs_baseline"])
    ):
        reasons.append("HIGH_DRAWDOWN")
    constraint_hit_rate = _float_or_none(metrics.get("constraint_hit_delta"))
    if (
        constraint_hit_rate is not None
        and constraint_hit_rate > float(policy["high_constraint_hit_rate"])
    ):
        reasons.append("HIGH_CONSTRAINT_HITS")
    return reasons


def _journal_outcome_summary(record: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(record.get("metrics"))
    accepted = _int_or_default(metrics.get("accepted_review_count"), 0)
    rejected = _int_or_default(metrics.get("rejected_review_count"), 0)
    deferred = _int_or_default(metrics.get("deferred_review_count"), 0)
    manual_count = _int_or_default(metrics.get("manual_review_count"), 0)
    if accepted > 0 and rejected > 0:
        support_status = "conflicted"
    elif accepted > 0:
        support_status = "supportive"
    elif rejected > 0:
        support_status = "negative"
    elif deferred > 0 or manual_count > 0:
        support_status = "neutral"
    else:
        support_status = "insufficient_review"
    latest_note = ""
    links = _records(record.get("journal_links"))
    if links:
        latest = links[-1]
        latest_note = _text(latest.get("decision_status"))
    return {
        "manual_review_count": manual_count,
        "accepted_count": accepted,
        "rejected_count": rejected,
        "deferred_count": deferred,
        "journal_support_ratio": _journal_support_ratio(metrics),
        "human_support_status": support_status,
        "latest_human_note": latest_note,
    }


def _candidate_journal_linkage(
    record: Mapping[str, Any],
    *,
    journal_report: Mapping[str, Any],
) -> dict[str, Any]:
    candidate_id = _text(record.get("candidate_id"))
    experiment_id = _text(record.get("experiment_id"))
    entries = _journal_entries_for_candidate(
        journal_report,
        candidate_id=candidate_id,
        experiment_id=experiment_id,
    )
    linked_entries = [_journal_entry_evidence(entry, journal_report) for entry in entries]
    status_counts = _decision_status_counts(entries)
    support_status = _human_support_status(status_counts)
    conflict_flags = _decision_conflict_flags(status_counts, entries)
    confidence_summary = _confidence_distribution(entries)
    return {
        "candidate_id": candidate_id,
        "experiment_id": experiment_id,
        "linked_journal_entries": linked_entries,
        "human_decisions": [
            {
                "decision_id": entry.get("decision_id"),
                "decision_status": entry.get("decision_status"),
                "human_decision": entry.get("human_decision"),
                "confidence": entry.get("confidence"),
            }
            for entry in linked_entries
        ],
        "human_support_status": support_status,
        "manual_confidence_score": confidence_summary.get("average_confidence"),
        "decision_conflict_flags": conflict_flags,
        "journal_summary": {
            "rationale_summary": _rationale_summary(entries),
            "confidence_distribution": confidence_summary,
            "follow_up_tasks": [
                _text(entry.get("follow_up_task")) for entry in entries if _text(
                    entry.get("follow_up_task")
                )
            ],
            "accepted_count": int(status_counts.get("accept_recommendation", 0)),
            "rejected_count": int(status_counts.get("reject_recommendation", 0)),
            "deferred_count": int(status_counts.get("defer_decision", 0)),
            "supportive_count": _supportive_count(status_counts),
            "negative_count": _negative_count(status_counts),
            "neutral_count": _neutral_count(status_counts),
            "latest_human_note": _latest_human_note(entries),
        },
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }


def _parameter_change_proposal(
    comparison_item: Mapping[str, Any],
    *,
    evidence_record: Mapping[str, Any],
    journal_evidence: Mapping[str, Any],
    created_at: datetime,
) -> dict[str, Any]:
    proposal_type = _proposal_type_for_evidence(comparison_item, journal_evidence)
    candidate_id = _text(comparison_item.get("candidate_id"))
    risk_summary = _proposal_risk_summary(comparison_item)
    return {
        "proposal_id": _stable_id(
            "etf-parameter-change-proposal",
            candidate_id,
            proposal_type,
            created_at.isoformat(),
        ),
        "candidate_id": candidate_id,
        "experiment_id": comparison_item.get("experiment_id"),
        "proposal_type": proposal_type,
        "current_baseline_config_hash": evidence_record.get("baseline_config_hash"),
        "candidate_config_hash": evidence_record.get("candidate_config_hash"),
        "proposed_parameter_delta": _proposed_parameter_delta(evidence_record),
        "supporting_evidence": _supporting_evidence(
            comparison_item,
            evidence_record=evidence_record,
            journal_evidence=journal_evidence,
        ),
        "blocking_evidence": _blocking_evidence(
            comparison_item,
            journal_evidence=journal_evidence,
        ),
        "risk_summary": risk_summary,
        "comparison_status": comparison_item.get("status"),
        "human_support_status": journal_evidence.get("human_support_status"),
        "manual_review_required": True,
        "production_effect": "none",
        "broker_action": "none",
        "created_at": created_at.isoformat(),
        "safety": dict(PARAMETER_REVIEW_SAFETY),
        **PARAMETER_REVIEW_SAFETY,
    }


def _proposal_type_for_evidence(
    comparison_item: Mapping[str, Any],
    journal_evidence: Mapping[str, Any],
) -> str:
    status = _text(comparison_item.get("status"))
    human_status = _text(journal_evidence.get("human_support_status"), "insufficient_review")
    conflict_flags = {
        _text(item) for item in journal_evidence.get("decision_conflict_flags") or []
    }
    if status == "outperforming_with_acceptable_risk":
        if human_status == "supportive" and "CONFLICTING_HUMAN_DECISIONS" not in conflict_flags:
            return "propose_baseline_parameter_review"
        return "propose_candidate_for_extended_shadow"
    if status == "outperforming_but_risky":
        return "propose_candidate_for_extended_shadow"
    if status == "underperforming":
        return "reject_candidate"
    if status == "needs_more_data":
        return "continue_observation"
    if status in {"mixed_evidence", "blocked_by_governance"}:
        return "defer_parameter_change"
    return "defer_parameter_change"


def _proposed_parameter_delta(evidence_record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "delta_status": "manual_review_required",
        "baseline_config_hash": evidence_record.get("baseline_config_hash"),
        "candidate_config_hash": evidence_record.get("candidate_config_hash"),
        "parameter_delta": evidence_record.get("parameter_delta", {}),
        "mutation_allowed": False,
        "automatic_application_allowed": False,
        "production_weights_mutated": False,
    }


def _supporting_evidence(
    comparison_item: Mapping[str, Any],
    *,
    evidence_record: Mapping[str, Any],
    journal_evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    sources = [
        {
            "evidence_type": "comparison_status",
            "status": comparison_item.get("status"),
            "reason_codes": comparison_item.get("reason_codes"),
            "source_review_id": comparison_item.get("source_review_id"),
        },
        {
            "evidence_type": "journal_support",
            "human_support_status": journal_evidence.get("human_support_status"),
            "linked_journal_entry_count": len(
                _records(journal_evidence.get("linked_journal_entries"))
            ),
        },
    ]
    sources.extend(
        {
            "evidence_type": "source_report",
            "source_type": source.get("source_type"),
            "source_module": source.get("source_module"),
            "source_report_path": source.get("source_report_path"),
            "source_metric": source.get("source_metric"),
        }
        for source in _records(evidence_record.get("evidence_sources"))
    )
    return sources


def _blocking_evidence(
    comparison_item: Mapping[str, Any],
    *,
    journal_evidence: Mapping[str, Any],
) -> list[dict[str, Any]]:
    items = []
    for reason in comparison_item.get("reason_codes") or []:
        reason_text = _text(reason)
        if reason_text and reason_text != "FORWARD_OUTPERFORMANCE":
            items.append({"evidence_type": "comparison_reason", "reason_code": reason_text})
    for flag in journal_evidence.get("decision_conflict_flags") or []:
        flag_text = _text(flag)
        if flag_text:
            items.append({"evidence_type": "journal_conflict", "reason_code": flag_text})
    return items


def _proposal_risk_summary(comparison_item: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _mapping(comparison_item.get("comparison_metrics"))
    reason_codes = [_text(item) for item in comparison_item.get("reason_codes") or []]
    risk_flags = [
        reason for reason in reason_codes if reason in {
            "HIGH_TURNOVER",
            "HIGH_DRAWDOWN",
            "HIGH_CONSTRAINT_HITS",
        }
    ]
    return {
        "risk_flags": risk_flags,
        "turnover_delta": metrics.get("turnover_delta"),
        "turnover_since_enrollment": metrics.get("turnover_since_enrollment"),
        "drawdown_reduction": metrics.get("drawdown_reduction"),
        "constraint_hit_delta": metrics.get("constraint_hit_delta"),
        "data_coverage_ratio": metrics.get("data_coverage_ratio"),
    }


def _proposal_type_counts(proposals: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {proposal_type: 0 for proposal_type in sorted(ALLOWED_PARAMETER_REVIEW_PROPOSAL_TYPES)}
    for proposal in proposals:
        proposal_type = _text(proposal.get("proposal_type"))
        if proposal_type in counts:
            counts[proposal_type] += 1
    return counts


def _journal_entry_evidence(
    entry: Mapping[str, Any],
    journal_report: Mapping[str, Any],
) -> dict[str, Any]:
    source_path = _text(entry.get("linked_report")) or _text(
        journal_report.get("source_journal_path")
    )
    return {
        "decision_id": entry.get("decision_id"),
        "review_id": entry.get("review_id"),
        "review_date": entry.get("review_date"),
        "action_item_id": entry.get("action_item_id"),
        "linked_candidate": entry.get("linked_candidate"),
        "linked_report": entry.get("linked_report"),
        "source_weekly_review": entry.get("source_weekly_review"),
        "decision_status": entry.get("decision_status"),
        "human_decision": entry.get("human_decision"),
        "rationale": entry.get("rationale"),
        "confidence": entry.get("confidence"),
        "follow_up_task": entry.get("follow_up_task"),
        "source_report_path": source_path,
    }


def _human_support_status(status_counts: Mapping[str, int]) -> str:
    supportive = _supportive_count(status_counts)
    negative = _negative_count(status_counts)
    neutral = _neutral_count(status_counts)
    if supportive > 0 and negative > 0:
        return "conflicted"
    if supportive > 0:
        return "supportive"
    if negative > 0:
        return "negative"
    if neutral > 0:
        return "neutral"
    return "insufficient_review"


def _decision_conflict_flags(
    status_counts: Mapping[str, int],
    entries: Sequence[Mapping[str, Any]],
) -> list[str]:
    flags: list[str] = []
    if not entries:
        flags.append("NO_JOURNAL_ENTRIES")
    if _supportive_count(status_counts) > 0 and _negative_count(status_counts) > 0:
        flags.append("CONFLICTING_HUMAN_DECISIONS")
    if any(not _text(entry.get("linked_report")) for entry in entries):
        flags.append("MISSING_LINKED_REPORT")
    if any(_float_or_none(entry.get("confidence")) is None for entry in entries):
        flags.append("MISSING_CONFIDENCE")
    return flags


def _decision_status_counts(entries: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for entry in entries:
        status = _text(entry.get("decision_status"), "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def _supportive_count(status_counts: Mapping[str, int]) -> int:
    return sum(int(status_counts.get(status, 0)) for status in SUPPORTIVE_DECISION_STATUSES)


def _negative_count(status_counts: Mapping[str, int]) -> int:
    return sum(int(status_counts.get(status, 0)) for status in NEGATIVE_DECISION_STATUSES)


def _neutral_count(status_counts: Mapping[str, int]) -> int:
    return sum(int(status_counts.get(status, 0)) for status in NEUTRAL_DECISION_STATUSES)


def _confidence_distribution(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = [
        value
        for value in (_float_or_none(entry.get("confidence")) for entry in entries)
        if value is not None
    ]
    return {
        "count": len(values),
        "average_confidence": None if not values else round(sum(values) / len(values), 6),
        "min_confidence": None if not values else min(values),
        "max_confidence": None if not values else max(values),
        "values": values,
    }


def _rationale_summary(entries: Sequence[Mapping[str, Any]]) -> str:
    rationales = [_text(entry.get("rationale")) for entry in entries if _text(
        entry.get("rationale")
    )]
    return "; ".join(rationales)


def _latest_human_note(entries: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not entries:
        return {}
    latest = entries[-1]
    return {
        "decision_id": latest.get("decision_id"),
        "decision_status": latest.get("decision_status"),
        "rationale": latest.get("rationale"),
        "confidence": latest.get("confidence"),
        "follow_up_task": latest.get("follow_up_task"),
    }


def _support_status_counts(candidates: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {status: 0 for status in sorted(PARAMETER_REVIEW_HUMAN_SUPPORT_STATUSES)}
    for item in candidates:
        status = _text(item.get("human_support_status"))
        if status in counts:
            counts[status] += 1
    return counts


def _weekly_review_status(record: Mapping[str, Any]) -> dict[str, Any]:
    links = _records(record.get("weekly_review_links"))
    latest_action = ""
    if links:
        latest_action = _text(links[-1].get("recommended_observation_action"))
    return {
        "linked_weekly_review_count": len(links),
        "latest_recommended_action": latest_action or "missing_data",
    }


def _experiment_metrics_row(
    experiment_report: Mapping[str, Any],
    *,
    experiment_id: str,
) -> dict[str, Any]:
    for key in ("metrics_table", "ranking_table", "comparison_table"):
        for row in _records(experiment_report.get(key)):
            if _text(row.get("experiment_id")) == experiment_id:
                return row
    return {}


def _comparison_summary(comparisons: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = {status: 0 for status in sorted(PARAMETER_REVIEW_COMPARISON_STATUSES)}
    for item in comparisons:
        status = _text(item.get("status"))
        if status in counts:
            counts[status] += 1
    return {
        "candidate_count": len(comparisons),
        "status_counts": counts,
        "eligible_manual_review_candidate_count": counts[
            "outperforming_with_acceptable_risk"
        ],
        "risky_outperformer_count": counts["outperforming_but_risky"],
        "needs_more_data_count": counts["needs_more_data"],
        "underperforming_count": counts["underperforming"],
        "mixed_evidence_count": counts["mixed_evidence"],
        "blocked_by_governance_count": counts["blocked_by_governance"],
    }


def _comparison_policy(policy: Mapping[str, Any] | None) -> dict[str, Any]:
    merged = dict(PARAMETER_REVIEW_COMPARISON_POLICY)
    merged.update(dict(policy or {}))
    merged["safety"] = dict(PARAMETER_REVIEW_SAFETY)
    merged.update(PARAMETER_REVIEW_SAFETY)
    return merged


def _comparison_direction(value: object) -> str:
    parsed = _float_or_none(value)
    if parsed is None:
        return "needs_more_data"
    if parsed > 0:
        return "outperforming"
    if parsed < 0:
        return "underperforming"
    return "flat"


def _journal_support_ratio(metrics: Mapping[str, Any]) -> float | None:
    manual_count = _int_or_default(metrics.get("manual_review_count"), 0)
    if manual_count <= 0:
        return None
    accepted = _int_or_default(metrics.get("accepted_review_count"), 0)
    return round(accepted / manual_count, 6)


def _regime_behavior(value: object) -> str:
    transitions = _float_or_none(value)
    if transitions is None:
        return "unavailable"
    if transitions > 0:
        return "regime_transitions_observed"
    return "single_regime_observed"


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
    drawdown_delta = _float_or_none(row.get("drawdown_delta_vs_baseline"))
    if drawdown_delta is None:
        drawdown_delta = _drawdown_reduction_from_values(
            row.get("max_drawdown_since_enrollment"),
            row.get("baseline_max_drawdown_since_enrollment"),
        )
    turnover_delta = _float_or_none(row.get("turnover_delta_vs_baseline"))
    if turnover_delta is None:
        turnover_delta = _subtract_optional(
            row.get("turnover_since_enrollment"),
            row.get("baseline_turnover_since_enrollment"),
        )
    metrics = {
        "return_since_enrollment": _float_or_none(row.get("return_since_enrollment")),
        "baseline_return_since_enrollment": _float_or_none(
            row.get("baseline_return_since_enrollment")
        ),
        "excess_return_vs_baseline": _float_or_none(row.get("excess_return_vs_baseline")),
        "excess_return_vs_QQQ": _float_or_none(row.get("excess_return_vs_QQQ")),
        "excess_return_vs_SPY": _float_or_none(row.get("excess_return_vs_SPY")),
        "excess_return_vs_SMH": _float_or_none(row.get("excess_return_vs_SMH")),
        "max_drawdown_since_enrollment": _float_or_none(
            row.get("max_drawdown_since_enrollment")
        ),
        "baseline_max_drawdown_since_enrollment": _float_or_none(
            row.get("baseline_max_drawdown_since_enrollment")
        ),
        "drawdown_delta_vs_baseline": drawdown_delta,
        "turnover_since_enrollment": _float_or_none(row.get("turnover_since_enrollment")),
        "baseline_turnover_since_enrollment": _float_or_none(
            row.get("baseline_turnover_since_enrollment")
        ),
        "turnover_delta_vs_baseline": turnover_delta,
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


def _subtract_optional(left: object, right: object) -> float | None:
    left_value = _float_or_none(left)
    right_value = _float_or_none(right)
    if left_value is None or right_value is None:
        return None
    return left_value - right_value


def _drawdown_reduction_from_values(
    candidate_drawdown: object,
    baseline_drawdown: object,
) -> float | None:
    candidate = _float_or_none(candidate_drawdown)
    baseline = _float_or_none(baseline_drawdown)
    if candidate is None or baseline is None:
        return None
    return abs(baseline) - abs(candidate)


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
