from __future__ import annotations

import csv
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system import cache_catalog, data_refresh_audit
from ai_trading_system import data_source_fallback_policy as fallback_policy
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as signal_inputs
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_PAPER_SHADOW_HEALTH_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_health"
)
DEFAULT_MARKET_PANEL_REPORT_DIR = st.PROJECT_ROOT / "outputs" / "reports"
DEFAULT_DATA_SOURCE_FALLBACK_DIR = fallback_policy.DEFAULT_DATA_SOURCE_FALLBACK_DIR
DEFAULT_CACHE_CATALOG_DIR = cache_catalog.DEFAULT_CACHE_CATALOG_DIR
DEFAULT_DATA_REFRESH_AUDIT_DIR = data_refresh_audit.DEFAULT_DATA_REFRESH_AUDIT_DIR

PAPER_SHADOW_HEALTH_STATUSES = (
    "HEALTHY",
    "HEALTHY_WITH_WARNINGS",
    "MANUAL_REVIEW_REQUIRED",
    "BLOCKED_DATA",
    "BLOCKED_SIGNAL_INPUTS",
    "BLOCKED_DRIFT",
    "BLOCKED_SAFETY",
)
PAPER_SHADOW_HEALTH_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "paper_shadow_health_check_only": True,
    "read_only_health_aggregation": True,
    "data_downloaded_by_health_check": False,
    "pipelines_executed_by_health_check": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
DATA_BLOCKING_FALLBACK_STATUSES = {
    "FALLBACK_UNAVAILABLE",
    "BLOCKED_NO_VALID_SOURCE",
    "MISSING",
}


def run_paper_shadow_health_report(
    *,
    as_of: date | None = None,
    price_cache_path: Path = st.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Path = DEFAULT_MARKET_PANEL_REPORT_DIR,
    signal_input_completeness_id: str | None = None,
    signal_input_completeness_report_path: Path | None = None,
    paper_shadow_daily_id: str | None = None,
    paper_shadow_drift_monitor_id: str | None = None,
    paper_shadow_weekly_review_id: str | None = None,
    evidence_staleness_monitor_id: str | None = None,
    shadow_continuation_readiness_id: str | None = None,
    fallback_policy_report_path: Path | None = None,
    cache_catalog_report_path: Path | None = None,
    data_refresh_audit_id: str | None = None,
    signal_input_completeness_dir: Path = signal_inputs.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    paper_shadow_daily_dir: Path = daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    evidence_staleness_monitor_dir: Path = readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    shadow_continuation_readiness_dir: Path = (
        readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR
    ),
    fallback_policy_output_dir: Path = DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_output_dir: Path = DEFAULT_CACHE_CATALOG_DIR,
    data_refresh_audit_dir: Path = DEFAULT_DATA_REFRESH_AUDIT_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()
    source_artifacts = _paper_shadow_health_sources(
        as_of=effective_as_of,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=evidence_staleness_monitor_id,
        shadow_continuation_readiness_id=shadow_continuation_readiness_id,
        fallback_policy_report_path=fallback_policy_report_path,
        cache_catalog_report_path=cache_catalog_report_path,
        data_refresh_audit_id=data_refresh_audit_id,
        signal_input_completeness_dir=signal_input_completeness_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        shadow_continuation_readiness_dir=shadow_continuation_readiness_dir,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_output_dir=cache_catalog_output_dir,
        data_refresh_audit_dir=data_refresh_audit_dir,
    )
    blocking_reasons = _health_blocking_reasons(source_artifacts)
    warnings = _health_warnings(source_artifacts)
    health_status = _canonical_health_status(
        source_artifacts=source_artifacts,
        blocking_reasons=blocking_reasons,
        warnings=warnings,
    )
    next_action = _health_next_action(
        health_status=health_status,
        source_artifacts=source_artifacts,
        blocking_reasons=blocking_reasons,
    )
    readiness_report = _mapping(source_artifacts["shadow_continuation_readiness"].get("summary"))
    safe_to_continue = (
        health_status in {"HEALTHY", "HEALTHY_WITH_WARNINGS"}
        and readiness_report.get("safe_to_continue_shadow") is True
    )
    health_id = st._stable_id(
        "paper-shadow-health",
        effective_as_of.isoformat(),
        health_status,
        _text(source_artifacts["signal_input_completeness"].get("artifact_id")),
        _text(source_artifacts["shadow_continuation_readiness"].get("artifact_id")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / health_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_report",
        "health_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "paper_shadow_health_status": health_status,
        "safe_to_continue_shadow": safe_to_continue,
        "data_freshness_status": _source_status(source_artifacts, "evidence_staleness"),
        "signal_input_status": _source_status(
            source_artifacts,
            "signal_input_completeness",
        ),
        "fallback_status": _source_status(source_artifacts, "fallback_policy"),
        "cache_integrity_status": _source_status(source_artifacts, "cache_catalog"),
        "weekly_review_coverage_status": _weekly_coverage_status(source_artifacts),
        "drift_status": _source_status(source_artifacts, "paper_shadow_drift_monitor"),
        "readiness_status": _source_status(
            source_artifacts,
            "shadow_continuation_readiness",
        ),
        "data_refresh_audit_status": _source_status(source_artifacts, "data_refresh_audit"),
        "blocking_reasons": blocking_reasons,
        "warnings": warnings,
        "next_required_action": next_action,
        "source_artifacts": source_artifacts,
        "limitations": [
            "canonical health aggregation only",
            "does not run upstream paper-shadow commands",
            "does not refresh data or repair cache/signal artifacts",
            "does not approve production or official target weights",
        ],
        **PAPER_SHADOW_HEALTH_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_health_manifest",
        "health_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": health_status,
        "paper_shadow_health_status": health_status,
        "safe_to_continue_shadow": safe_to_continue,
        "paper_shadow_health_manifest_path": str(root / "paper_shadow_health_manifest.json"),
        "paper_shadow_health_report_path": str(root / "paper_shadow_health_report.json"),
        "paper_shadow_health_markdown_path": str(root / "paper_shadow_health_report.md"),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_health_validation.json"),
        **PAPER_SHADOW_HEALTH_SAFETY,
    }
    reader = render_paper_shadow_health_reader_brief(report)
    st._write_json(root / "paper_shadow_health_manifest.json", manifest)
    st._write_json(root / "paper_shadow_health_report.json", report)
    st._write_text(
        root / "paper_shadow_health_report.md",
        render_paper_shadow_health_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_paper_shadow_health",
        root.name,
        root / "paper_shadow_health_manifest.json",
    )
    validation = validate_paper_shadow_health_artifact(
        health_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "health_id": root.name,
        "health_dir": root,
        "manifest": manifest,
        "paper_shadow_health_report": report,
        "reader_brief_section": reader,
        "paper_shadow_health_validation": validation,
    }


def paper_shadow_health_report_payload(
    *,
    health_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_HEALTH_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=health_id,
        latest_pointer="latest_paper_shadow_health",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_health_manifest.json",
    )
    payload = {
        **st._read_json(root / "paper_shadow_health_manifest.json"),
        "paper_shadow_health_report": st._read_json(root / "paper_shadow_health_report.json"),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "health_dir": str(root),
    }
    validation = st._read_optional_json(root / "paper_shadow_health_validation.json")
    if validation:
        payload["paper_shadow_health_validation"] = validation
    return payload


def validate_paper_shadow_health_artifact(
    *,
    health_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / health_id
    manifest = st._read_optional_json(root / "paper_shadow_health_manifest.json") or {}
    report = st._read_optional_json(root / "paper_shadow_health_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    source_artifacts = _mapping(report.get("source_artifacts"))
    checks = st._required_file_checks(
        root,
        (
            "paper_shadow_health_manifest.json",
            "paper_shadow_health_report.json",
            "paper_shadow_health_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_report_id_match",
                manifest.get("health_id") == report.get("health_id") == health_id,
                "",
            ),
            st._check(
                "status_allowed",
                report.get("paper_shadow_health_status") in PAPER_SHADOW_HEALTH_STATUSES,
                _text(report.get("paper_shadow_health_status")),
            ),
            st._check(
                "required_source_artifacts_visible",
                set(source_artifacts) == set(_required_source_ids()),
                ",".join(sorted(source_artifacts)),
            ),
            st._check(
                "blocking_reasons_visible",
                isinstance(report.get("blocking_reasons"), list),
                "",
            ),
            st._check(
                "warning_list_visible",
                isinstance(report.get("warnings"), list),
                "",
            ),
            st._check(
                "signal_fail_closed",
                (
                    _source_status(source_artifacts, "signal_input_completeness")
                    in {"OK", "WARNING"}
                    or report.get("paper_shadow_health_status") == "BLOCKED_SIGNAL_INPUTS"
                ),
                _source_status(source_artifacts, "signal_input_completeness"),
            ),
            st._check(
                "drift_fail_closed",
                (
                    _source_status(source_artifacts, "paper_shadow_drift_monitor")
                    != "BLOCKING"
                    or report.get("paper_shadow_health_status") == "BLOCKED_DRIFT"
                ),
                _source_status(source_artifacts, "paper_shadow_drift_monitor"),
            ),
            st._check(
                "safety_fail_closed",
                (
                    not _unsafe_source_ids(source_artifacts)
                    or report.get("paper_shadow_health_status") == "BLOCKED_SAFETY"
                ),
                ",".join(_unsafe_source_ids(source_artifacts)),
            ),
            st._check(
                "reader_brief_fields",
                "paper_shadow_health_status" in reader
                and "safe_to_continue_shadow" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "read_only_health_check",
                report.get("data_downloaded_by_health_check") is False
                and report.get("pipelines_executed_by_health_check") is False
                and report.get("paper_shadow_health_check_only") is True,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_paper_shadow_health_validation",
        health_id,
        checks,
    )
    if write_output:
        st._write_json(root / "paper_shadow_health_validation.json", validation)
        st._write_text(
            root / "paper_shadow_health_validation.md",
            render_paper_shadow_health_validation_report(validation),
        )
    return validation


def render_paper_shadow_health_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Paper Shadow Health",
            "",
            f"- paper_shadow_health_id: {report.get('health_id')}",
            f"- paper_shadow_health_status: {report.get('paper_shadow_health_status')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- data_freshness_status: {report.get('data_freshness_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- fallback_status: {report.get('fallback_status')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- weekly_review_coverage_status: {report.get('weekly_review_coverage_status')}",
            f"- drift_status: {report.get('drift_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: read-only health aggregation / no official target / "
            "no broker / no paper account or production mutation",
            "",
        ]
    )


def render_paper_shadow_health_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    source_lines = [
        (
            f"- {source_id}: exists={source.get('exists')} "
            f"status={source.get('status')} validation={source.get('validation_status')} "
            f"artifact_id={source.get('artifact_id')} path={source.get('source_path')}"
        )
        for source_id, source in sorted(_mapping(report.get("source_artifacts")).items())
    ]
    return "\n".join(
        [
            f"# Paper Shadow Health {manifest.get('health_id')}",
            "",
            "## Purpose",
            "Aggregate latest paper-shadow data governance, signal, daily, drift, weekly, "
            "staleness and readiness artifacts into one read-only health check.",
            "",
            "## Summary",
            f"- paper_shadow_health_status: {report.get('paper_shadow_health_status')}",
            f"- safe_to_continue_shadow: {report.get('safe_to_continue_shadow')}",
            f"- data_freshness_status: {report.get('data_freshness_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- fallback_status: {report.get('fallback_status')}",
            f"- cache_integrity_status: {report.get('cache_integrity_status')}",
            f"- weekly_review_coverage_status: {report.get('weekly_review_coverage_status')}",
            f"- drift_status: {report.get('drift_status')}",
            f"- readiness_status: {report.get('readiness_status')}",
            f"- data_refresh_audit_status: {report.get('data_refresh_audit_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Source Artifacts",
            *source_lines,
            "",
            "## Safety Boundary",
            "- paper-shadow health check only",
            "- read-only aggregation of existing artifacts",
            "- no data refresh or upstream rerun",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no paper account mutation",
            "- no production mutation",
            "",
            "## Limitations",
            "- HEALTHY is paper-shadow research health only; it is not production approval.",
            "- BLOCKED_* must be resolved by fixing source artifacts, not by waiving checks here.",
            "- Missing required source artifacts remain fail-closed.",
            "",
        ]
    )


def render_paper_shadow_health_validation_report(validation: Mapping[str, Any]) -> str:
    check_lines = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Health Validation {validation.get('artifact_id')}",
            "",
            f"- status: {validation.get('status')}",
            f"- failed_check_count: {validation.get('failed_check_count')}",
            "- production_effect: none",
            "",
            "## Checks",
            *check_lines,
            "",
        ]
    )


def _paper_shadow_health_sources(
    *,
    as_of: date,
    price_cache_path: Path,
    market_panel_dir: Path,
    signal_input_completeness_id: str | None,
    signal_input_completeness_report_path: Path | None,
    paper_shadow_daily_id: str | None,
    paper_shadow_drift_monitor_id: str | None,
    paper_shadow_weekly_review_id: str | None,
    evidence_staleness_monitor_id: str | None,
    shadow_continuation_readiness_id: str | None,
    fallback_policy_report_path: Path | None,
    cache_catalog_report_path: Path | None,
    data_refresh_audit_id: str | None,
    signal_input_completeness_dir: Path,
    paper_shadow_daily_dir: Path,
    paper_shadow_drift_monitor_dir: Path,
    paper_shadow_weekly_review_dir: Path,
    evidence_staleness_monitor_dir: Path,
    shadow_continuation_readiness_dir: Path,
    fallback_policy_output_dir: Path,
    cache_catalog_output_dir: Path,
    data_refresh_audit_dir: Path,
) -> dict[str, dict[str, Any]]:
    return {
        "price_data": _price_data_source(price_cache_path),
        "market_panel_data": _market_panel_source(market_panel_dir=market_panel_dir, as_of=as_of),
        "signal_input_completeness": _signal_input_source(
            monitor_id=signal_input_completeness_id,
            report_path=signal_input_completeness_report_path,
            output_dir=signal_input_completeness_dir,
        ),
        "paper_shadow_daily": _daily_source(
            observation_id=paper_shadow_daily_id,
            output_dir=paper_shadow_daily_dir,
        ),
        "paper_shadow_drift_monitor": _drift_source(
            monitor_id=paper_shadow_drift_monitor_id,
            output_dir=paper_shadow_drift_monitor_dir,
        ),
        "paper_shadow_weekly_review": _weekly_source(
            review_id=paper_shadow_weekly_review_id,
            output_dir=paper_shadow_weekly_review_dir,
        ),
        "evidence_staleness": _evidence_source(
            monitor_id=evidence_staleness_monitor_id,
            output_dir=evidence_staleness_monitor_dir,
        ),
        "shadow_continuation_readiness": _readiness_source(
            readiness_id=shadow_continuation_readiness_id,
            output_dir=shadow_continuation_readiness_dir,
        ),
        "fallback_policy": _fallback_policy_source(
            report_path=fallback_policy_report_path,
            output_dir=fallback_policy_output_dir,
        ),
        "cache_catalog": _cache_catalog_source(
            report_path=cache_catalog_report_path,
            output_dir=cache_catalog_output_dir,
        ),
        "data_refresh_audit": _data_refresh_audit_source(
            audit_id=data_refresh_audit_id,
            output_dir=data_refresh_audit_dir,
        ),
    }


def _price_data_source(path: Path) -> dict[str, Any]:
    try:
        rows, columns = _read_csv_summary(path)
    except Exception as exc:
        return _missing_source("price_data", f"price cache unreadable: {exc}")
    latest_date = _latest_date(rows, "date")
    summary = {
        "row_count": len(rows),
        "column_count": len(columns),
        "latest_date": latest_date.isoformat() if latest_date else "",
    }
    return _source(
        "price_data",
        exists=True,
        artifact_id=path.name,
        status="AVAILABLE",
        validation_status="NOT_RUN",
        source_path=path,
        summary=summary,
        payload={"production_effect": "none", **PAPER_SHADOW_HEALTH_SAFETY},
    )


def _market_panel_source(*, market_panel_dir: Path, as_of: date) -> dict[str, Any]:
    path = _latest_market_panel_path(market_panel_dir, as_of=as_of)
    if path is None:
        return _missing_source("market_panel_data", "market panel artifact missing")
    payload = st._read_optional_json(path) or {}
    summary = _mapping(payload.get("summary"))
    return _source(
        "market_panel_data",
        exists=True,
        artifact_id=path.name,
        status=_text(payload.get("status"), "UNKNOWN"),
        validation_status=_text(payload.get("validation_status"), "NOT_RUN"),
        source_path=path,
        summary={
            "as_of": _text(payload.get("as_of"), _date_from_market_panel_path(path)),
            "status": _text(payload.get("status"), "UNKNOWN"),
            "proxy_count": len(_records(payload.get("proxies"))),
            "summary": summary,
        },
        payload=payload,
    )


def _signal_input_source(
    *,
    monitor_id: str | None,
    report_path: Path | None,
    output_dir: Path,
) -> dict[str, Any]:
    summary = signal_inputs.latest_signal_input_completeness_summary(
        monitor_id=monitor_id,
        report_path=report_path,
        output_dir=output_dir,
    )
    return _source(
        "signal_input_completeness",
        exists=summary.get("exists") is True,
        artifact_id=_text(summary.get("monitor_id"), "MISSING"),
        status=_text(summary.get("signal_input_status"), "MISSING"),
        validation_status=_text(summary.get("validation_status"), "MISSING"),
        source_path=_optional_path(summary.get("report_path")),
        summary=summary,
        payload=summary,
    )


def _daily_source(*, observation_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = daily.paper_shadow_daily_report_payload(
            observation_id=observation_id,
            latest=observation_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_daily", f"daily artifact missing: {exc}")
    observation = _mapping(payload.get("paper_shadow_daily_observation"))
    validation = _mapping(payload.get("paper_shadow_daily_validation"))
    return _source(
        "paper_shadow_daily",
        exists=True,
        artifact_id=_text(payload.get("observation_id"), "UNKNOWN"),
        status=_text(observation.get("observation_status"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=_optional_path(payload.get("paper_shadow_daily_observation_path")),
        summary=observation,
        payload=payload,
    )


def _drift_source(*, monitor_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = drift.paper_shadow_drift_monitor_report_payload(
            monitor_id=monitor_id,
            latest=monitor_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_drift_monitor", f"drift artifact missing: {exc}")
    report = _mapping(payload.get("paper_shadow_drift_report"))
    validation = _mapping(payload.get("paper_shadow_drift_validation"))
    return _source(
        "paper_shadow_drift_monitor",
        exists=True,
        artifact_id=_text(payload.get("monitor_id"), "UNKNOWN"),
        status=_text(report.get("drift_severity"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=_optional_path(payload.get("paper_shadow_drift_report_path")),
        summary=report,
        payload=payload,
    )


def _weekly_source(*, review_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = weekly.paper_shadow_weekly_review_report_payload(
            weekly_review_id=review_id,
            latest=review_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("paper_shadow_weekly_review", f"weekly artifact missing: {exc}")
    report = _mapping(payload.get("paper_shadow_weekly_review"))
    validation = _mapping(payload.get("paper_shadow_weekly_validation"))
    return _source(
        "paper_shadow_weekly_review",
        exists=True,
        artifact_id=_text(payload.get("review_id"), "UNKNOWN"),
        status=_text(report.get("weekly_decision"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=_optional_path(payload.get("paper_shadow_weekly_report_path")),
        summary=report,
        payload=payload,
    )


def _evidence_source(*, monitor_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = readiness.evidence_staleness_monitor_report_payload(
            monitor_id=monitor_id,
            latest=monitor_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source("evidence_staleness", f"staleness artifact missing: {exc}")
    report = _mapping(payload.get("evidence_staleness_report"))
    validation = _mapping(payload.get("evidence_staleness_validation"))
    return _source(
        "evidence_staleness",
        exists=True,
        artifact_id=_text(payload.get("monitor_id"), "UNKNOWN"),
        status=_text(report.get("evidence_freshness_status"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=_optional_path(payload.get("evidence_staleness_report_path")),
        summary=report,
        payload=payload,
    )


def _readiness_source(*, readiness_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = readiness.shadow_continuation_readiness_report_payload(
            readiness_id=readiness_id,
            latest=readiness_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source(
            "shadow_continuation_readiness",
            f"readiness artifact missing: {exc}",
        )
    report = _mapping(payload.get("shadow_continuation_readiness_report"))
    validation = _mapping(payload.get("shadow_continuation_readiness_validation"))
    return _source(
        "shadow_continuation_readiness",
        exists=True,
        artifact_id=_text(payload.get("readiness_id"), "UNKNOWN"),
        status=_text(report.get("shadow_continuation_readiness"), "UNKNOWN"),
        validation_status=_text(validation.get("status"), "NOT_RUN"),
        source_path=_optional_path(payload.get("shadow_continuation_readiness_report_path")),
        summary=report,
        payload=payload,
    )


def _fallback_policy_source(*, report_path: Path | None, output_dir: Path) -> dict[str, Any]:
    summary = fallback_policy.latest_data_source_fallback_policy_summary(
        report_path=report_path,
        output_dir=output_dir,
    )
    return _source(
        "fallback_policy",
        exists=summary.get("availability") == "AVAILABLE",
        artifact_id=_text(summary.get("report_id"), "MISSING"),
        status=_text(summary.get("fallback_status"), "MISSING"),
        validation_status=_text(summary.get("validation_status"), "MISSING"),
        source_path=_optional_path(summary.get("report_path")),
        summary=summary,
        payload=summary,
    )


def _cache_catalog_source(*, report_path: Path | None, output_dir: Path) -> dict[str, Any]:
    summary = cache_catalog.latest_cache_catalog_summary(
        report_path=report_path,
        output_dir=output_dir,
    )
    return _source(
        "cache_catalog",
        exists=summary.get("availability") == "AVAILABLE",
        artifact_id=_text(summary.get("catalog_id"), "MISSING"),
        status=_text(summary.get("cache_integrity_status"), "MISSING"),
        validation_status=_text(summary.get("validation_status"), "MISSING"),
        source_path=_optional_path(summary.get("report_path")),
        summary=summary,
        payload=summary,
    )


def _data_refresh_audit_source(*, audit_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        path = data_refresh_audit.resolve_data_refresh_audit_path(
            audit_id=audit_id,
            latest=audit_id is None,
            output_dir=output_dir,
        )
        payload = data_refresh_audit.load_data_refresh_audit_payload(path)
    except Exception as exc:
        return _missing_source("data_refresh_audit", f"refresh audit missing: {exc}")
    summary = _mapping(payload.get("summary"))
    return _source(
        "data_refresh_audit",
        exists=True,
        artifact_id=_text(payload.get("audit_id"), "UNKNOWN"),
        status=_text(payload.get("status"), "UNKNOWN"),
        validation_status=_text(payload.get("validation_status"), "NOT_RUN"),
        source_path=path,
        summary={
            "status": _text(payload.get("status"), "UNKNOWN"),
            "validation_status": _text(payload.get("validation_status"), "NOT_RUN"),
            "failed_record_count": _int(summary.get("failed_record_count")),
            "warning_count": _int(summary.get("warning_count")),
            "next_action": _text(summary.get("next_action"), "UNKNOWN"),
        },
        payload=payload,
    )


def _source(
    source_id: str,
    *,
    exists: bool,
    artifact_id: str,
    status: str,
    validation_status: str,
    source_path: Path | None,
    summary: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "safety_status": "PASS" if st._payload_safe(payload) else "FAIL",
        "production_effect": _text(payload.get("production_effect"), "none"),
    }


def _missing_source(source_id: str, reason: str) -> dict[str, Any]:
    return {
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {"limitation": reason},
        "safety_status": "PASS",
        "production_effect": "none",
    }


def _health_blocking_reasons(source_artifacts: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []
    for source_id, source in sorted(_mapping(source_artifacts).items()):
        if _mapping(source).get("exists") is not True:
            reasons.append(f"{source_id}:missing")
    if _source_status(source_artifacts, "signal_input_completeness") not in {"OK", "WARNING"}:
        reasons.append("signal_input_completeness:blocking")
    if _source_status(source_artifacts, "paper_shadow_drift_monitor") == "BLOCKING":
        reasons.append("paper_shadow_drift:blocking")
    if _source_status(source_artifacts, "fallback_policy") in DATA_BLOCKING_FALLBACK_STATUSES:
        reasons.append("fallback_policy:blocking")
    if _source_status(source_artifacts, "cache_catalog") not in {"OK"}:
        reasons.append("cache_catalog:blocking")
    if _source_status(source_artifacts, "data_refresh_audit") in {"FAIL", "MISSING"}:
        reasons.append("data_refresh_audit:blocking")
    evidence = _mapping(_mapping(source_artifacts).get("evidence_staleness"))
    evidence_summary = _mapping(evidence.get("summary"))
    evidence_blockers = _texts(evidence_summary.get("blocking_artifacts"))
    data_blockers = [item for item in evidence_blockers if item != "signal_input_completeness"]
    if _source_status(source_artifacts, "evidence_staleness") == "BLOCKING" and data_blockers:
        reasons.append("evidence_staleness:blocking_data")
    readiness_status = _source_status(source_artifacts, "shadow_continuation_readiness")
    if readiness_status == "BLOCKED_SAFETY_BOUNDARY":
        reasons.append("shadow_continuation:safety_blocked")
    elif readiness_status in {"BLOCKED_MISSING_ARTIFACTS", "BLOCKED_STALE_DATA"}:
        if _source_status(source_artifacts, "signal_input_completeness") in {"OK", "WARNING"}:
            reasons.append(f"shadow_continuation:{readiness_status.lower()}")
    for source_id in _unsafe_source_ids(source_artifacts):
        reasons.append(f"{source_id}:unsafe_safety_boundary")
    return _dedupe(reasons)


def _health_warnings(source_artifacts: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    if _source_status(source_artifacts, "signal_input_completeness") == "WARNING":
        warnings.append("signal_input_completeness:warning")
    if _source_status(source_artifacts, "paper_shadow_drift_monitor") in {"WATCH", "WARNING"}:
        warnings.append("paper_shadow_drift:watch_or_warning")
    if _source_status(source_artifacts, "fallback_policy") == "FALLBACK_USED":
        warnings.append("fallback_policy:fallback_used")
    cache = _mapping(_mapping(source_artifacts).get("cache_catalog"))
    if _texts(_mapping(cache.get("summary")).get("warning_entry_ids")):
        warnings.append("cache_catalog:warnings")
    if _source_status(source_artifacts, "data_refresh_audit") == "PASS_WITH_WARNINGS":
        warnings.append("data_refresh_audit:warnings")
    evidence_summary = _mapping(_mapping(source_artifacts.get("evidence_staleness")).get("summary"))
    if _texts(evidence_summary.get("stale_artifacts")):
        warnings.append("evidence_staleness:stale_artifacts")
    if _weekly_coverage_status(source_artifacts) not in {"FULL", "FULL_WEEK_REVIEW", "OK"}:
        warnings.append("weekly_review:coverage_manual_review")
    readiness_summary = _mapping(
        _mapping(source_artifacts.get("shadow_continuation_readiness")).get("summary")
    )
    if readiness_summary.get("manual_review_required") is True:
        warnings.append("shadow_continuation:manual_review_required")
    return _dedupe(warnings)


def _canonical_health_status(
    *,
    source_artifacts: Mapping[str, Any],
    blocking_reasons: list[str],
    warnings: list[str],
) -> str:
    if _unsafe_source_ids(source_artifacts) or any(
        "safety" in reason for reason in blocking_reasons
    ):
        return "BLOCKED_SAFETY"
    if _source_status(source_artifacts, "signal_input_completeness") not in {"OK", "WARNING"}:
        return "BLOCKED_SIGNAL_INPUTS"
    if _source_status(source_artifacts, "paper_shadow_drift_monitor") == "BLOCKING":
        return "BLOCKED_DRIFT"
    if any(reason.endswith(":missing") for reason in blocking_reasons):
        return "BLOCKED_DATA"
    if any(":blocking" in reason or "blocking_data" in reason for reason in blocking_reasons):
        return "BLOCKED_DATA"
    readiness_summary = _mapping(
        _mapping(source_artifacts.get("shadow_continuation_readiness")).get("summary")
    )
    if readiness_summary.get("safe_to_continue_shadow") is not True:
        return "MANUAL_REVIEW_REQUIRED"
    if warnings:
        return "HEALTHY_WITH_WARNINGS"
    return "HEALTHY"


def _health_next_action(
    *,
    health_status: str,
    source_artifacts: Mapping[str, Any],
    blocking_reasons: list[str],
) -> str:
    if health_status == "BLOCKED_SIGNAL_INPUTS":
        return _text(
            _mapping(source_artifacts["signal_input_completeness"].get("summary")).get(
                "next_required_action"
            ),
            "restore_signal_inputs_before_paper_shadow",
        )
    if health_status == "BLOCKED_DRIFT":
        return _text(
            _mapping(source_artifacts["paper_shadow_drift_monitor"].get("summary")).get(
                "next_action"
            ),
            "return_to_research_for_drift_review",
        )
    if health_status == "BLOCKED_SAFETY":
        return "stop_paper_shadow_until_safety_boundary_is_restored"
    if health_status == "BLOCKED_DATA":
        return _data_next_action(source_artifacts, blocking_reasons)
    if health_status == "MANUAL_REVIEW_REQUIRED":
        return _text(
            _mapping(source_artifacts["shadow_continuation_readiness"].get("summary")).get(
                "next_required_action"
            ),
            "complete_manual_paper_shadow_health_review",
        )
    if health_status == "HEALTHY_WITH_WARNINGS":
        return "continue_shadow_after_owner_reviews_health_warnings"
    return "continue_paper_shadow_observation"


def _data_next_action(
    source_artifacts: Mapping[str, Any],
    blocking_reasons: list[str],
) -> str:
    if any(reason.startswith("fallback_policy") for reason in blocking_reasons):
        return _text(
            _mapping(source_artifacts["fallback_policy"].get("summary")).get("next_action"),
            "restore_primary_or_eligible_fallback_source",
        )
    if any(reason.startswith("cache_catalog") for reason in blocking_reasons):
        return _text(
            _mapping(source_artifacts["cache_catalog"].get("summary")).get("next_action"),
            "repair_cache_catalog_inputs",
        )
    if any(reason.startswith("data_refresh_audit") for reason in blocking_reasons):
        return _text(
            _mapping(source_artifacts["data_refresh_audit"].get("summary")).get("next_action"),
            "rerun_validate_data_and_data_refresh_audit",
        )
    return _text(
        _mapping(source_artifacts["evidence_staleness"].get("summary")).get(
            "next_refresh_action"
        ),
        "restore_data_artifacts_before_paper_shadow",
    )


def _unsafe_source_ids(source_artifacts: Mapping[str, Any]) -> list[str]:
    return sorted(
        source_id
        for source_id, source in _mapping(source_artifacts).items()
        if _mapping(source).get("safety_status") == "FAIL"
    )


def _source_status(source_artifacts: Mapping[str, Any], source_id: str) -> str:
    return _text(_mapping(_mapping(source_artifacts).get(source_id)).get("status"), "MISSING")


def _weekly_coverage_status(source_artifacts: Mapping[str, Any]) -> str:
    weekly_summary = _mapping(
        _mapping(source_artifacts.get("paper_shadow_weekly_review")).get("summary")
    )
    return _text(
        weekly_summary.get("coverage_status")
        or weekly_summary.get("coverage_classification"),
        "MISSING",
    )


def _required_source_ids() -> tuple[str, ...]:
    return (
        "price_data",
        "market_panel_data",
        "signal_input_completeness",
        "paper_shadow_daily",
        "paper_shadow_drift_monitor",
        "paper_shadow_weekly_review",
        "evidence_staleness",
        "shadow_continuation_readiness",
        "fallback_policy",
        "cache_catalog",
        "data_refresh_audit",
    )


def _read_csv_summary(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
        columns = list(reader.fieldnames or [])
    return rows, columns


def _latest_date(rows: list[Mapping[str, Any]], column: str) -> date | None:
    dates: list[date] = []
    for row in rows:
        value = _parse_date(row.get(column))
        if value is not None:
            dates.append(value)
    return max(dates) if dates else None


def _parse_date(value: object) -> date | None:
    if isinstance(value, date):
        return value
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _latest_market_panel_path(report_dir: Path, *, as_of: date) -> Path | None:
    candidates: list[tuple[date, Path]] = []
    for path in report_dir.glob("market_panel_*.json"):
        candidate_date = _date_from_market_panel_path(path)
        if path.is_file() and candidate_date is not None and candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _date_from_market_panel_path(path: Path | None) -> date | None:
    if path is None:
        return None
    raw = path.stem.removeprefix("market_panel_")
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _optional_path(value: object) -> Path | None:
    text = _text(value)
    if not text or text == "MISSING":
        return None
    return Path(text)


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = _text(value)
        if text and text not in seen:
            result.append(text)
            seen.add(text)
    return result


def _joined_texts(value: object) -> str:
    return ", ".join(_texts(value)) or "none"


def _int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
