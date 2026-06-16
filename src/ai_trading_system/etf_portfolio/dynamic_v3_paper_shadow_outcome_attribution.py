from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_OUTCOME_ATTRIBUTION_CONFIG_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "paper_shadow_outcome_attribution_v1.yaml"
)
DEFAULT_OUTCOME_ATTRIBUTION_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_outcome_attribution"
)
OUTCOME_ATTRIBUTION_STATUSES = (
    "OUTCOME_ATTRIBUTION_COMPLETE",
    "OUTCOME_ATTRIBUTION_WITH_WARNINGS",
    "UNKNOWN_OUTCOME_DRIVERS",
    "BLOCKED_MISSING_WEEKLY_REVIEW",
    "BLOCKED_POLICY",
)
DRIVER_STATUSES = ("ACTIVE", "INACTIVE", "UNKNOWN")
CONFIDENCE_LEVELS = ("HIGH", "MEDIUM", "LOW", "UNKNOWN")
REQUIRED_DRIVER_IDS = (
    "market_move",
    "signal_change",
    "regime_transition",
    "data_stale_warning",
    "fallback_source_used",
    "signal_input_incompleteness",
    "drift_warning",
    "weekly_coverage_warning",
    "manual_owner_decision",
)
OUTCOME_ATTRIBUTION_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "research_only": True,
    "outcome_attribution_only": True,
    "read_only_attribution": True,
    "weekly_decision_mutated": False,
    "data_downloaded_by_attribution": False,
    "pipelines_executed_by_attribution": False,
    "official_target_weights": False,
    "official_target_weights_mutated": False,
    "not_official_target_weights": True,
    "broker_effect": "none",
    "order_effect": "none",
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "paper_account_state_mutated": False,
    "production_state_mutated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}


def load_outcome_attribution_policy(
    config_path: Path = DEFAULT_OUTCOME_ATTRIBUTION_CONFIG_PATH,
) -> dict[str, Any]:
    return _normalized_policy(st._load_yaml_mapping(config_path), config_path=config_path)


def run_paper_shadow_outcome_attribution(
    *,
    as_of: date | None = None,
    weekly_review_id: str | None = None,
    weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    paper_shadow_health_id: str | None = None,
    paper_shadow_health_report_path: Path | None = None,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    config_path: Path = DEFAULT_OUTCOME_ATTRIBUTION_CONFIG_PATH,
    output_dir: Path = DEFAULT_OUTCOME_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    policy = load_outcome_attribution_policy(config_path)
    weekly_source = _weekly_source(
        weekly_review_id=weekly_review_id,
        output_dir=weekly_review_dir,
    )
    health_source = _health_source(
        health_id=paper_shadow_health_id,
        report_path=paper_shadow_health_report_path,
        output_dir=paper_shadow_health_dir,
    )
    policy_blockers = _policy_blockers(policy)
    effective_as_of = (
        as_of
        or _parse_optional_date(_mapping(weekly_source.get("summary")).get("week_end"))
        or generated.date()
    )
    driver_rows = _driver_rows(
        policy=policy,
        weekly_source=weekly_source,
        health_source=health_source,
    )
    driver_summary = _driver_summary(driver_rows)
    status = _attribution_status(
        weekly_source=weekly_source,
        health_source=health_source,
        policy_blockers=policy_blockers,
        driver_summary=driver_summary,
    )
    candidate = _text(
        _mapping(weekly_source.get("summary")).get("candidate"),
        "UNKNOWN",
    )
    attribution_id = st._stable_id(
        "paper-shadow-outcome-attribution",
        candidate,
        effective_as_of.isoformat(),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        _text(weekly_source.get("artifact_id")),
        _text(health_source.get("artifact_id")),
        status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_outcome_attribution_report",
        "attribution_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "config_path": str(config_path),
        "policy": policy,
        "policy_id": policy.get("policy_id"),
        "policy_version": policy.get("version"),
        "paper_shadow_outcome_attribution_status": status,
        "weekly_review_id": weekly_source.get("artifact_id"),
        "weekly_decision": _mapping(weekly_source.get("summary")).get(
            "weekly_decision"
        ),
        "weekly_coverage_status": _mapping(weekly_source.get("summary")).get(
            "coverage_status"
        ),
        "dominant_driver": driver_summary.get("dominant_driver"),
        "dominant_driver_category": driver_summary.get("dominant_driver_category"),
        "dominant_confidence": driver_summary.get("dominant_confidence"),
        "active_driver_count": driver_summary.get("active_driver_count"),
        "unknown_driver_count": driver_summary.get("unknown_driver_count"),
        "driver_summary": driver_summary,
        "attribution_drivers": driver_rows,
        "source_artifacts": {
            "paper_shadow_weekly_review": weekly_source,
            "paper_shadow_health": health_source,
        },
        "policy_blockers": policy_blockers,
        "blocking_reasons": _blocking_reasons(
            status=status,
            weekly_source=weekly_source,
            policy_blockers=policy_blockers,
        ),
        "warnings": _warnings(
            status=status,
            health_source=health_source,
            driver_summary=driver_summary,
        ),
        "next_required_action": _next_required_action(status),
        "limitations": [
            "paper-shadow outcome attribution only",
            "does not recalculate weekly decisions, signals, regimes, or performance",
            "does not refresh data or rerun upstream paper-shadow artifacts",
            "does not approve candidate promotion or production target weights",
        ],
        **OUTCOME_ATTRIBUTION_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_outcome_attribution_manifest",
        "attribution_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "paper_shadow_outcome_attribution_status": status,
        "weekly_review_id": weekly_source.get("artifact_id"),
        "dominant_driver": driver_summary.get("dominant_driver"),
        "dominant_confidence": driver_summary.get("dominant_confidence"),
        "paper_shadow_outcome_attribution_manifest_path": str(
            root / "paper_shadow_outcome_attribution_manifest.json"
        ),
        "paper_shadow_outcome_attribution_report_path": str(
            root / "paper_shadow_outcome_attribution_report.json"
        ),
        "paper_shadow_outcome_attribution_markdown_path": str(
            root / "paper_shadow_outcome_attribution_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "paper_shadow_outcome_attribution_validation.json"),
        **OUTCOME_ATTRIBUTION_SAFETY,
    }
    reader = render_outcome_attribution_reader_brief(report)
    st._write_json(root / "paper_shadow_outcome_attribution_manifest.json", manifest)
    st._write_json(root / "paper_shadow_outcome_attribution_report.json", report)
    st._write_text(
        root / "paper_shadow_outcome_attribution_report.md",
        render_outcome_attribution_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_paper_shadow_outcome_attribution",
        root.name,
        root / "paper_shadow_outcome_attribution_manifest.json",
    )
    validation = validate_paper_shadow_outcome_attribution_artifact(
        attribution_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "attribution_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "paper_shadow_outcome_attribution_report": report,
        "reader_brief_section": reader,
        "paper_shadow_outcome_attribution_validation": validation,
    }


def paper_shadow_outcome_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OUTCOME_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_paper_shadow_outcome_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_outcome_attribution_manifest.json",
    )
    payload = {
        **st._read_json(root / "paper_shadow_outcome_attribution_manifest.json"),
        "paper_shadow_outcome_attribution_report": st._read_json(
            root / "paper_shadow_outcome_attribution_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8",
        ),
        "attribution_dir": str(root),
    }
    validation = st._read_optional_json(
        root / "paper_shadow_outcome_attribution_validation.json"
    )
    if validation:
        payload["paper_shadow_outcome_attribution_validation"] = validation
    return payload


def validate_paper_shadow_outcome_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_OUTCOME_ATTRIBUTION_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    manifest = (
        st._read_optional_json(root / "paper_shadow_outcome_attribution_manifest.json")
        or {}
    )
    report = (
        st._read_optional_json(root / "paper_shadow_outcome_attribution_report.json")
        or {}
    )
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    drivers = _records(report.get("attribution_drivers"))
    driver_ids = {_text(row.get("driver_id")) for row in drivers}
    driver_statuses = {_text(row.get("driver_status")) for row in drivers}
    confidences = {_text(row.get("confidence")) for row in drivers}
    weekly_source = _mapping(
        _mapping(report.get("source_artifacts")).get("paper_shadow_weekly_review")
    )
    checks = st._required_file_checks(
        root,
        (
            "paper_shadow_outcome_attribution_manifest.json",
            "paper_shadow_outcome_attribution_report.json",
            "paper_shadow_outcome_attribution_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check(
                "manifest_report_id_match",
                manifest.get("attribution_id")
                == report.get("attribution_id")
                == attribution_id,
                "",
            ),
            st._check(
                "status_allowed",
                report.get("paper_shadow_outcome_attribution_status")
                in OUTCOME_ATTRIBUTION_STATUSES,
                _text(report.get("paper_shadow_outcome_attribution_status")),
            ),
            st._check(
                "required_drivers_present",
                set(REQUIRED_DRIVER_IDS).issubset(driver_ids),
                ",".join(sorted(driver_ids)),
            ),
            st._check(
                "driver_statuses_valid",
                driver_statuses.issubset(set(DRIVER_STATUSES)),
                ",".join(sorted(driver_statuses)),
            ),
            st._check(
                "confidence_values_valid",
                confidences.issubset(set(CONFIDENCE_LEVELS)),
                ",".join(sorted(confidences)),
            ),
            st._check(
                "dominant_driver_visible",
                bool(_text(report.get("dominant_driver")))
                or report.get("paper_shadow_outcome_attribution_status")
                in {"BLOCKED_MISSING_WEEKLY_REVIEW", "BLOCKED_POLICY"},
                _text(report.get("dominant_driver")),
            ),
            st._check(
                "weekly_source_visible",
                bool(weekly_source.get("source_id")),
                _text(weekly_source.get("source_id")),
            ),
            st._check(
                "missing_weekly_fail_closed",
                weekly_source.get("exists") is not False
                or report.get("paper_shadow_outcome_attribution_status")
                == "BLOCKED_MISSING_WEEKLY_REVIEW",
                _text(weekly_source.get("limitation")),
            ),
            st._check(
                "reader_brief_fields",
                "paper_shadow_outcome_attribution_status" in reader
                and "dominant_driver" in reader
                and "active_driver_count" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "read_only_attribution",
                report.get("outcome_attribution_only") is True
                and report.get("read_only_attribution") is True
                and report.get("weekly_decision_mutated") is False
                and report.get("data_downloaded_by_attribution") is False
                and report.get("pipelines_executed_by_attribution") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_paper_shadow_outcome_attribution_validation",
        attribution_id,
        checks,
    )
    if write_output:
        st._write_json(root / "paper_shadow_outcome_attribution_validation.json", validation)
        st._write_text(
            root / "paper_shadow_outcome_attribution_validation.md",
            render_outcome_attribution_validation_report(validation),
        )
    return validation


def render_outcome_attribution_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Paper Shadow Outcome Attribution",
            "",
            f"- paper_shadow_outcome_attribution_id: {report.get('attribution_id')}",
            f"- paper_shadow_outcome_attribution_status: "
            f"{report.get('paper_shadow_outcome_attribution_status')}",
            f"- weekly_review_id: {report.get('weekly_review_id')}",
            f"- weekly_decision: {report.get('weekly_decision')}",
            f"- dominant_driver: {report.get('dominant_driver')}",
            f"- dominant_driver_category: {report.get('dominant_driver_category')}",
            f"- dominant_confidence: {report.get('dominant_confidence')}",
            f"- active_driver_count: {report.get('active_driver_count')}",
            f"- unknown_driver_count: {report.get('unknown_driver_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: research-only outcome attribution / read-only weekly "
            "explanation / no broker / no order / no official target / no production",
            "",
        ]
    )


def render_outcome_attribution_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    rows = [
        (
            f"| `{row.get('driver_id')}` | {row.get('driver_status')} | "
            f"{row.get('confidence')} | {row.get('priority')} | "
            f"{_markdown_text(row.get('evidence_summary'))} |"
        )
        for row in _records(report.get("attribution_drivers"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Outcome Attribution {manifest.get('attribution_id')}",
            "",
            "## Purpose",
            "Explain weekly paper-shadow outcomes by attributing them to data, signal, "
            "regime, market, drift, coverage, fallback and manual governance drivers.",
            "",
            "## Summary",
            f"- candidate: {report.get('candidate')}",
            f"- paper_shadow_outcome_attribution_status: "
            f"{report.get('paper_shadow_outcome_attribution_status')}",
            f"- weekly_review_id: {report.get('weekly_review_id')}",
            f"- weekly_decision: {report.get('weekly_decision')}",
            f"- dominant_driver: {report.get('dominant_driver')}",
            f"- dominant_confidence: {report.get('dominant_confidence')}",
            f"- active_driver_count: {report.get('active_driver_count')}",
            f"- unknown_driver_count: {report.get('unknown_driver_count')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Driver Attribution",
            "| driver | status | confidence | priority | evidence |",
            "|---|---|---|---:|---|",
            *rows,
            "",
            "## Safety Boundary",
            "- paper-shadow outcome attribution only",
            "- read-only source artifact interpretation",
            "- no weekly decision mutation",
            "- no data refresh or upstream rerun",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no paper account or production mutation",
            "",
        ]
    )


def render_outcome_attribution_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Paper Shadow Outcome Attribution Validation {validation.get('artifact_id')}",
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


def _normalized_policy(config: Mapping[str, Any], *, config_path: Path) -> dict[str, Any]:
    safety = {**OUTCOME_ATTRIBUTION_SAFETY, **_mapping(config.get("safety_boundaries"))}
    return {
        "schema_version": st.SCHEMA_VERSION,
        "policy_id": _text(
            config.get("policy_id"),
            "dynamic_v3_rescue_paper_shadow_outcome_attribution_v1",
        ),
        "version": _text(config.get("version")),
        "status": _text(config.get("status"), "pilot_manual_review_baseline"),
        "owner": _text(config.get("owner"), "system_validation"),
        "rationale": _text(config.get("rationale")),
        "intended_effect": _text(config.get("intended_effect")),
        "validation_evidence": _text(config.get("validation_evidence")),
        "review_condition": _text(config.get("review_condition")),
        "config_path": str(config_path),
        "confidence_levels": _texts(config.get("confidence_levels")),
        "driver_statuses": _texts(config.get("driver_statuses")),
        "drivers": [_normalized_driver(row) for row in _records(config.get("drivers"))],
        "safety_boundaries": safety,
        **safety,
    }


def _normalized_driver(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "driver_id": _text(row.get("driver_id")),
        "category": _text(row.get("category")),
        "priority": _int(row.get("priority"), 999),
        "confidence_when_source_present": _text(
            row.get("confidence_when_source_present"),
            "UNKNOWN",
        ),
        "rationale": _text(row.get("rationale")),
    }


def _weekly_source(*, weekly_review_id: str | None, output_dir: Path) -> dict[str, Any]:
    try:
        payload = weekly.paper_shadow_weekly_review_report_payload(
            weekly_review_id=weekly_review_id,
            latest=weekly_review_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source(
            "paper_shadow_weekly_review",
            f"paper-shadow weekly review missing: {exc}",
        )
    review = _mapping(payload.get("paper_shadow_weekly_review"))
    return _source(
        "paper_shadow_weekly_review",
        exists=True,
        artifact_id=_text(payload.get("weekly_review_id")),
        status=_text(review.get("weekly_decision"), _text(payload.get("status"), "UNKNOWN")),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_weekly_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_weekly_manifest_path"))),
        summary={
            "weekly_review_id": payload.get("weekly_review_id"),
            "candidate": review.get("candidate"),
            "week_start": review.get("week_start"),
            "week_end": review.get("week_end"),
            "weekly_decision": review.get("weekly_decision"),
            "coverage_status": review.get("coverage_status"),
            "coverage_classification": review.get("coverage_classification"),
            "coverage_safe_for_continuation": review.get(
                "coverage_safe_for_continuation"
            ),
            "signal_input_status": review.get("signal_input_status"),
        },
        payload=review,
    )


def _health_source(
    *,
    health_id: str | None,
    report_path: Path | None,
    output_dir: Path,
) -> dict[str, Any]:
    if report_path is not None:
        if not report_path.exists():
            return _missing_source(
                "paper_shadow_health",
                f"paper-shadow health report missing: {report_path}",
            )
        report = st._read_json(report_path)
        validation = st._read_optional_json(
            report_path.parent / "paper_shadow_health_validation.json"
        )
        return _source(
            "paper_shadow_health",
            exists=True,
            artifact_id=_text(report.get("health_id"), report_path.parent.name),
            status=_text(report.get("paper_shadow_health_status"), "UNKNOWN"),
            validation_status=_text(_mapping(validation).get("status"), "NOT_RUN"),
            source_path=report_path,
            summary=_health_summary(report),
            payload=report,
        )
    try:
        payload = health.paper_shadow_health_report_payload(
            health_id=health_id,
            latest=health_id is None,
            output_dir=output_dir,
        )
    except Exception as exc:
        return _missing_source(
            "paper_shadow_health",
            f"paper-shadow health missing: {exc}",
        )
    report = _mapping(payload.get("paper_shadow_health_report"))
    return _source(
        "paper_shadow_health",
        exists=True,
        artifact_id=_text(payload.get("health_id")),
        status=_text(report.get("paper_shadow_health_status"), "UNKNOWN"),
        validation_status=_text(
            _mapping(payload.get("paper_shadow_health_validation")).get("status"),
            "NOT_RUN",
        ),
        source_path=Path(_text(payload.get("paper_shadow_health_manifest_path"))),
        summary=_health_summary(report),
        payload=report,
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
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": exists,
        "artifact_id": artifact_id,
        "status": status,
        "validation_status": validation_status,
        "source_path": "" if source_path is None else str(source_path),
        "summary": dict(summary),
        "payload": dict(payload),
        **OUTCOME_ATTRIBUTION_SAFETY,
    }


def _missing_source(source_id: str, limitation: str) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "source_id": source_id,
        "exists": False,
        "artifact_id": "MISSING",
        "status": "MISSING",
        "validation_status": "MISSING",
        "source_path": "",
        "summary": {},
        "payload": {},
        "limitation": limitation,
        **OUTCOME_ATTRIBUTION_SAFETY,
    }


def _driver_rows(
    *,
    policy: Mapping[str, Any],
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
) -> list[dict[str, Any]]:
    drivers = _records(policy.get("drivers"))
    return [
        _driver_row(
            driver,
            weekly_source=weekly_source,
            health_source=health_source,
        )
        for driver in drivers
    ]


def _driver_row(
    driver: Mapping[str, Any],
    *,
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
) -> dict[str, Any]:
    driver_id = _text(driver.get("driver_id"))
    status, confidence, evidence, fields = _driver_evidence(
        driver_id,
        weekly_source=weekly_source,
        health_source=health_source,
        source_confidence=_text(driver.get("confidence_when_source_present")),
    )
    return {
        "schema_version": st.SCHEMA_VERSION,
        "driver_id": driver_id,
        "category": _text(driver.get("category"), driver_id),
        "priority": _int(driver.get("priority"), 999),
        "driver_status": status,
        "confidence": confidence,
        "evidence_summary": evidence,
        "source_fields": fields,
        "rationale": _text(driver.get("rationale")),
        **OUTCOME_ATTRIBUTION_SAFETY,
    }


def _driver_evidence(
    driver_id: str,
    *,
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
    source_confidence: str,
) -> tuple[str, str, str, dict[str, Any]]:
    weekly_review = _mapping(weekly_source.get("payload"))
    weekly_summary = _mapping(weekly_review.get("summary"))
    health_report = _mapping(health_source.get("payload"))
    if weekly_source.get("exists") is False:
        return "UNKNOWN", "UNKNOWN", "weekly review source missing", {}

    if driver_id == "signal_input_incompleteness":
        signal_status = _text(
            weekly_review.get("signal_input_status"),
            _text(health_report.get("signal_input_status")),
        )
        active = signal_status not in {"", "OK", "PASS"}
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=f"signal_input_status={signal_status or 'UNKNOWN'}",
            fields={"signal_input_status": signal_status},
        )
    if driver_id == "data_stale_warning":
        data_status = _text(health_report.get("data_freshness_status"))
        if health_source.get("exists") is False and not data_status:
            missing_inputs = _texts(weekly_summary.get("missing_input_artifacts"))
            active = bool(missing_inputs)
            return _status_tuple(
                active=active,
                confidence="MEDIUM" if active else "UNKNOWN",
                evidence=f"missing_inputs={_joined_texts(missing_inputs)}",
                fields={"missing_input_artifacts": missing_inputs},
            )
        active = data_status not in {"", "OK", "PASS", "FRESH", "ACCEPTABLE"}
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=f"data_freshness_status={data_status or 'UNKNOWN'}",
            fields={"data_freshness_status": data_status},
        )
    if driver_id == "weekly_coverage_warning":
        coverage_status = _text(weekly_review.get("coverage_status"))
        coverage_classification = _text(weekly_review.get("coverage_classification"))
        safe = weekly_review.get("coverage_safe_for_continuation")
        active = (
            coverage_status not in {"", "PASS"}
            or coverage_classification != "FULL_WEEK_REVIEW"
            or safe is False
        )
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=(
                f"coverage_status={coverage_status or 'UNKNOWN'},"
                f"classification={coverage_classification or 'UNKNOWN'},"
                f"safe={safe}"
            ),
            fields={
                "coverage_status": coverage_status,
                "coverage_classification": coverage_classification,
                "coverage_safe_for_continuation": safe,
            },
        )
    if driver_id == "drift_warning":
        trend = _mapping(weekly_summary.get("drift_severity_trend"))
        max_severity = _text(trend.get("max_severity"))
        active = max_severity in {"WATCH", "WARNING", "BLOCKING"}
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=f"drift_max_severity={max_severity or 'UNKNOWN'}",
            fields={"drift_severity_trend": trend},
        )
    if driver_id == "fallback_source_used":
        fallback_status = _text(health_report.get("fallback_status"))
        if health_source.get("exists") is False and not fallback_status:
            return "UNKNOWN", "UNKNOWN", "paper-shadow health source missing", {}
        return _status_tuple(
            active=fallback_status == "FALLBACK_USED",
            confidence=source_confidence,
            evidence=f"fallback_status={fallback_status or 'UNKNOWN'}",
            fields={"fallback_status": fallback_status},
        )
    if driver_id == "manual_owner_decision":
        final_decision = _text(weekly_review.get("source_ledger_final_decision"))
        manual_override = weekly_review.get("manual_coverage_override") is True
        active = manual_override or bool(final_decision)
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=(
                f"source_ledger_final_decision={final_decision or 'none'},"
                f"manual_coverage_override={manual_override}"
            ),
            fields={
                "source_ledger_final_decision": final_decision,
                "manual_coverage_override": manual_override,
            },
        )
    if driver_id == "signal_change":
        stability = _text(weekly_summary.get("signal_stability"))
        signals = sorted(
            {
                _text(row.get("signal_output"))
                for row in _records(weekly_review.get("daily_observations"))
                if _text(row.get("signal_output"))
            }
        )
        active = stability not in {"", "STABLE"} or len(signals) > 1
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=f"signal_stability={stability or 'UNKNOWN'},signals={','.join(signals)}",
            fields={"signal_stability": stability, "signal_outputs": signals},
        )
    if driver_id == "regime_transition":
        regimes = sorted(
            {
                _text(row.get("risk_off_risk_on_state"))
                for row in _records(weekly_review.get("daily_observations"))
                if _text(row.get("risk_off_risk_on_state"))
            }
        )
        active = len(regimes) > 1 or any(value not in {"risk_on", "normal"} for value in regimes)
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=f"risk_states={','.join(regimes) or 'UNKNOWN'}",
            fields={"risk_off_risk_on_states": regimes},
        )
    if driver_id == "market_move":
        benchmark_values = sorted(
            {
                _text(row.get("benchmark_comparison"))
                for row in _records(weekly_review.get("daily_observations"))
                if _text(row.get("benchmark_comparison"))
            }
        )
        drawdown_behavior = _text(weekly_summary.get("drawdown_behavior"))
        active = bool(benchmark_values) or drawdown_behavior not in {"", "STABLE"}
        return _status_tuple(
            active=active,
            confidence=source_confidence,
            evidence=(
                f"benchmark_comparison={','.join(benchmark_values) or 'UNKNOWN'},"
                f"drawdown_behavior={drawdown_behavior or 'UNKNOWN'}"
            ),
            fields={
                "benchmark_comparison_values": benchmark_values,
                "drawdown_behavior": drawdown_behavior,
            },
        )
    return "UNKNOWN", "UNKNOWN", f"unknown driver id {driver_id}", {}


def _status_tuple(
    *,
    active: bool,
    confidence: str,
    evidence: str,
    fields: Mapping[str, Any],
) -> tuple[str, str, str, dict[str, Any]]:
    return (
        "ACTIVE" if active else "INACTIVE",
        confidence if confidence in CONFIDENCE_LEVELS else "UNKNOWN",
        evidence,
        dict(fields),
    )


def _driver_summary(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    active = [row for row in rows if row.get("driver_status") == "ACTIVE"]
    unknown = [row for row in rows if row.get("driver_status") == "UNKNOWN"]
    dominant = min(
        active,
        key=lambda row: (_int(row.get("priority"), 999), _text(row.get("driver_id"))),
        default={},
    )
    return {
        "driver_count": len(rows),
        "active_driver_count": len(active),
        "inactive_driver_count": sum(
            1 for row in rows if row.get("driver_status") == "INACTIVE"
        ),
        "unknown_driver_count": len(unknown),
        "dominant_driver": _text(dominant.get("driver_id"), "UNKNOWN"),
        "dominant_driver_category": _text(dominant.get("category"), "UNKNOWN"),
        "dominant_confidence": _text(dominant.get("confidence"), "UNKNOWN"),
        "active_drivers": [_text(row.get("driver_id")) for row in active],
        "unknown_drivers": [_text(row.get("driver_id")) for row in unknown],
    }


def _attribution_status(
    *,
    weekly_source: Mapping[str, Any],
    health_source: Mapping[str, Any],
    policy_blockers: list[str],
    driver_summary: Mapping[str, Any],
) -> str:
    if policy_blockers:
        return "BLOCKED_POLICY"
    if weekly_source.get("exists") is False:
        return "BLOCKED_MISSING_WEEKLY_REVIEW"
    if int(driver_summary.get("active_driver_count") or 0) == 0:
        return "UNKNOWN_OUTCOME_DRIVERS"
    if health_source.get("exists") is False or int(driver_summary.get("unknown_driver_count") or 0):
        return "OUTCOME_ATTRIBUTION_WITH_WARNINGS"
    return "OUTCOME_ATTRIBUTION_COMPLETE"


def _policy_blockers(policy: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    driver_ids = {_text(row.get("driver_id")) for row in _records(policy.get("drivers"))}
    if not _text(policy.get("policy_id")):
        blockers.append("policy_id:missing")
    if not _text(policy.get("version")):
        blockers.append("policy_version:missing")
    missing = set(REQUIRED_DRIVER_IDS) - driver_ids
    if missing:
        blockers.append(f"required_drivers_missing:{','.join(sorted(missing))}")
    return blockers


def _blocking_reasons(
    *,
    status: str,
    weekly_source: Mapping[str, Any],
    policy_blockers: list[str],
) -> list[str]:
    reasons: list[str] = []
    if weekly_source.get("exists") is False:
        reasons.append("paper_shadow_weekly_review:missing")
    reasons.extend(policy_blockers)
    if status == "UNKNOWN_OUTCOME_DRIVERS":
        reasons.append("outcome_drivers:unknown")
    return reasons


def _warnings(
    *,
    status: str,
    health_source: Mapping[str, Any],
    driver_summary: Mapping[str, Any],
) -> list[str]:
    warnings: list[str] = []
    if health_source.get("exists") is False:
        warnings.append("paper_shadow_health:missing_optional_context")
    if int(driver_summary.get("unknown_driver_count") or 0):
        warnings.append(
            "unknown_driver_count:" f"{int(driver_summary.get('unknown_driver_count') or 0)}"
        )
    if status == "OUTCOME_ATTRIBUTION_WITH_WARNINGS":
        warnings.append("manual_review_recommended_for_attribution_warnings")
    return warnings


def _next_required_action(status: str) -> str:
    if status == "OUTCOME_ATTRIBUTION_COMPLETE":
        return "review_outcome_attribution_with_weekly_review"
    if status == "OUTCOME_ATTRIBUTION_WITH_WARNINGS":
        return "review_unknown_or_missing_context_before_owner_decision"
    if status == "UNKNOWN_OUTCOME_DRIVERS":
        return "collect_more_weekly_source_context_before_attribution"
    if status == "BLOCKED_MISSING_WEEKLY_REVIEW":
        return "generate_or_provide_weekly_review_before_attribution"
    return "fix_outcome_attribution_policy_before_attribution"


def _health_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "health_id": report.get("health_id"),
        "paper_shadow_health_status": report.get("paper_shadow_health_status"),
        "data_freshness_status": report.get("data_freshness_status"),
        "signal_input_status": report.get("signal_input_status"),
        "fallback_status": report.get("fallback_status"),
        "drift_status": report.get("drift_status"),
        "weekly_review_coverage_status": report.get("weekly_review_coverage_status"),
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(row) for row in value if isinstance(row, Mapping)]
    return []


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [_text(item) for item in value if _text(item)]
    return []


def _joined_texts(value: Any) -> str:
    return ",".join(_texts(value)) or "none"


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, bool):
        return str(value)
    text = str(value)
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_optional_date(value: Any) -> date | None:
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _markdown_text(value: Any) -> str:
    return _text(value).replace("|", "\\|")
