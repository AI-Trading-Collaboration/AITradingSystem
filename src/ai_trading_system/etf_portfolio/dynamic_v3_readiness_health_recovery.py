from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system import cache_catalog
from ai_trading_system import data_source_fallback_policy as fallback_policy
from ai_trading_system.etf_portfolio import dynamic_v3_filtered_candidate_readiness as readiness
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_daily as daily
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_drift as drift
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_health as health
from ai_trading_system.etf_portfolio import dynamic_v3_paper_shadow_weekly as weekly
from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as signal_inputs
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_READINESS_HEALTH_RECOVERY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "readiness_health_recovery"
)

READINESS_HEALTH_RECOVERY_STATUSES = (
    "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",
    "PAPER_SHADOW_STILL_BLOCKED",
    "MANUAL_REVIEW_REQUIRED",
)

READINESS_HEALTH_RECOVERY_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "readiness_health_recovery_chain_only": True,
    "paper_shadow_resumption_gate_only": True,
    "normal_paper_shadow_observation_gate_only": True,
    "official_target_weights": False,
    "official_target_weights_written": False,
    "extended_shadow_approval_allowed": False,
    "extended_shadow_protocol_executed": False,
    "promotion_board_executed": False,
    "promotion_approval_allowed": False,
    "paper_account_state_mutated": False,
    "data_downloaded_by_recovery_chain": False,
    "source_artifacts_fabricated": False,
}


def run_readiness_health_recovery_chain(
    *,
    as_of: date | None = None,
    candidate: str = readiness.TOP_FILTERED_CANDIDATE,
    evidence_id: str | None = None,
    stress_backfill_id: str | None = None,
    ab_review_id: str | None = None,
    owner_review_id: str | None = None,
    paper_shadow_daily_id: str | None = None,
    paper_shadow_drift_monitor_id: str | None = None,
    paper_shadow_weekly_review_id: str | None = None,
    signal_input_completeness_id: str | None = None,
    signal_input_completeness_report_path: Path | None = None,
    data_quality_report_path: Path | None = None,
    fallback_policy_report_path: Path | None = None,
    cache_catalog_report_path: Path | None = None,
    data_refresh_audit_id: str | None = None,
    price_cache_path: Path = st.DEFAULT_PRICE_CACHE_PATH,
    market_panel_dir: Path = readiness.DEFAULT_MARKET_PANEL_REPORT_DIR,
    evidence_staleness_policy_path: Path = readiness.DEFAULT_EVIDENCE_STALENESS_POLICY_PATH,
    evidence_dir: Path = readiness.DEFAULT_FILTERED_CANDIDATE_EVIDENCE_DIR,
    stress_backfill_dir: Path = readiness.DEFAULT_FILTERED_CANDIDATE_STRESS_BACKFILL_DIR,
    ab_review_dir: Path = readiness.DEFAULT_FILTERED_CANDIDATE_AB_REVIEW_DIR,
    owner_review_dir: Path = readiness.DEFAULT_OWNER_FILTERED_CANDIDATE_REVIEW_DIR,
    paper_shadow_daily_dir: Path = daily.DEFAULT_PAPER_SHADOW_DAILY_DIR,
    paper_shadow_drift_monitor_dir: Path = drift.DEFAULT_PAPER_SHADOW_DRIFT_MONITOR_DIR,
    paper_shadow_weekly_review_dir: Path = weekly.DEFAULT_PAPER_SHADOW_WEEKLY_REVIEW_DIR,
    signal_input_completeness_dir: Path = signal_inputs.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    data_quality_report_dir: Path = readiness.DEFAULT_MARKET_PANEL_REPORT_DIR,
    fallback_policy_output_dir: Path = fallback_policy.DEFAULT_DATA_SOURCE_FALLBACK_DIR,
    cache_catalog_output_dir: Path = cache_catalog.DEFAULT_CACHE_CATALOG_DIR,
    data_refresh_audit_dir: Path = health.DEFAULT_DATA_REFRESH_AUDIT_DIR,
    evidence_staleness_monitor_dir: Path = readiness.DEFAULT_EVIDENCE_STALENESS_MONITOR_DIR,
    shadow_continuation_readiness_dir: Path = readiness.DEFAULT_SHADOW_CONTINUATION_READINESS_DIR,
    paper_shadow_health_dir: Path = health.DEFAULT_PAPER_SHADOW_HEALTH_DIR,
    output_dir: Path = DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()

    staleness_result = readiness.run_evidence_staleness_monitor(
        as_of=effective_as_of,
        candidate=candidate,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        policy_path=evidence_staleness_policy_path,
        evidence_id=evidence_id,
        stress_backfill_id=stress_backfill_id,
        ab_review_id=ab_review_id,
        owner_review_id=owner_review_id,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        evidence_dir=evidence_dir,
        stress_backfill_dir=stress_backfill_dir,
        ab_review_dir=ab_review_dir,
        owner_review_dir=owner_review_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        fallback_policy_report_path=fallback_policy_report_path,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_report_path=cache_catalog_report_path,
        cache_catalog_output_dir=cache_catalog_output_dir,
        output_dir=evidence_staleness_monitor_dir,
        generated_at=generated,
    )
    staleness_id = _text(staleness_result.get("monitor_id"))

    readiness_result = readiness.run_shadow_continuation_readiness_report(
        as_of=effective_as_of,
        candidate=candidate,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=staleness_id,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        data_quality_report_path=data_quality_report_path,
        data_quality_report_dir=data_quality_report_dir,
        paper_shadow_daily_dir=paper_shadow_daily_dir,
        paper_shadow_drift_monitor_dir=paper_shadow_drift_monitor_dir,
        paper_shadow_weekly_review_dir=paper_shadow_weekly_review_dir,
        evidence_staleness_monitor_dir=evidence_staleness_monitor_dir,
        signal_input_completeness_dir=signal_input_completeness_dir,
        fallback_policy_report_path=fallback_policy_report_path,
        fallback_policy_output_dir=fallback_policy_output_dir,
        cache_catalog_report_path=cache_catalog_report_path,
        cache_catalog_output_dir=cache_catalog_output_dir,
        output_dir=shadow_continuation_readiness_dir,
        generated_at=generated,
    )
    readiness_id = _text(readiness_result.get("readiness_id"))

    health_result = health.run_paper_shadow_health_report(
        as_of=effective_as_of,
        price_cache_path=price_cache_path,
        market_panel_dir=market_panel_dir,
        signal_input_completeness_id=signal_input_completeness_id,
        signal_input_completeness_report_path=signal_input_completeness_report_path,
        paper_shadow_daily_id=paper_shadow_daily_id,
        paper_shadow_drift_monitor_id=paper_shadow_drift_monitor_id,
        paper_shadow_weekly_review_id=paper_shadow_weekly_review_id,
        evidence_staleness_monitor_id=staleness_id,
        shadow_continuation_readiness_id=readiness_id,
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
        output_dir=paper_shadow_health_dir,
        generated_at=generated,
    )

    source_statuses = _source_statuses(
        staleness_result=staleness_result,
        readiness_result=readiness_result,
        health_result=health_result,
    )
    source_artifacts = _source_artifacts(
        staleness_result=staleness_result,
        readiness_result=readiness_result,
        health_result=health_result,
    )
    source_validations = _source_validations(
        staleness_result=staleness_result,
        readiness_result=readiness_result,
        health_result=health_result,
    )
    blocking_reasons = _blocking_reasons(source_statuses, source_validations)
    warning_reasons = _warning_reasons(source_statuses)
    final_status = _final_status(source_statuses, source_validations)
    chain_id = st._stable_id(
        "readiness-health-recovery",
        candidate,
        effective_as_of.isoformat(),
        final_status,
        staleness_id,
        readiness_id,
        _text(health_result.get("health_id")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / chain_id)
    root.mkdir(parents=True, exist_ok=False)

    normal_resume = final_status == "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION"
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_readiness_health_recovery_report",
        "recovery_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "readiness_health_recovery_status": final_status,
        "normal_paper_shadow_may_resume": normal_resume,
        "hard_stop_triggered": final_status == "PAPER_SHADOW_STILL_BLOCKED",
        "manual_review_required": final_status != "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",
        "next_required_action": _next_action(final_status),
        "source_artifacts": source_artifacts,
        "source_statuses": source_statuses,
        "source_validations": source_validations,
        "blocking_reasons": blocking_reasons,
        "warning_reasons": warning_reasons,
        "evidence_staleness_monitor_id": staleness_id,
        "shadow_continuation_readiness_id": readiness_id,
        "paper_shadow_health_id": _text(health_result.get("health_id")),
        "promotion_board_allowed": False,
        "extended_shadow_allowed": False,
        "official_target_weights_allowed": False,
        "broker_action_allowed": False,
        "paper_account_state_mutated": False,
        "production_state_mutated": False,
        "limitations": [
            (
                "chain reruns evidence staleness, shadow continuation readiness, "
                "and canonical paper-shadow health only"
            ),
            "normal paper-shadow observation may resume only when all source gates are clean",
            "signal warnings or health/readiness warnings require manual review",
            (
                "does not run promotion board, extended-shadow protocol, target "
                "weight generation, broker, order, or production workflows"
            ),
        ],
        **READINESS_HEALTH_RECOVERY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_readiness_health_recovery_manifest",
        "recovery_id": root.name,
        "candidate": candidate,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": final_status,
        "readiness_health_recovery_status": final_status,
        "normal_paper_shadow_may_resume": normal_resume,
        "evidence_staleness_monitor_id": staleness_id,
        "shadow_continuation_readiness_id": readiness_id,
        "paper_shadow_health_id": _text(health_result.get("health_id")),
        "readiness_health_recovery_manifest_path": str(
            root / "readiness_health_recovery_manifest.json"
        ),
        "readiness_health_recovery_report_path": str(
            root / "readiness_health_recovery_report.json"
        ),
        "readiness_health_recovery_markdown_path": str(
            root / "readiness_health_recovery_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "readiness_health_recovery_validation.json"),
        **READINESS_HEALTH_RECOVERY_SAFETY,
    }
    reader = render_readiness_health_recovery_reader_brief(report)
    st._write_json(root / "readiness_health_recovery_manifest.json", manifest)
    st._write_json(root / "readiness_health_recovery_report.json", report)
    st._write_text(
        root / "readiness_health_recovery_report.md",
        render_readiness_health_recovery_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_readiness_health_recovery",
        root.name,
        root / "readiness_health_recovery_manifest.json",
    )
    validation = validate_readiness_health_recovery_artifact(
        recovery_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "recovery_id": root.name,
        "recovery_dir": root,
        "manifest": manifest,
        "readiness_health_recovery_report": report,
        "reader_brief_section": reader,
        "readiness_health_recovery_validation": validation,
        "evidence_staleness_result": staleness_result,
        "shadow_continuation_readiness_result": readiness_result,
        "paper_shadow_health_result": health_result,
    }


def readiness_health_recovery_report_payload(
    *,
    recovery_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=recovery_id,
        latest_pointer="latest_readiness_health_recovery",
        latest=latest,
        output_dir=output_dir,
        required_name="readiness_health_recovery_manifest.json",
    )
    payload = {
        **st._read_json(root / "readiness_health_recovery_manifest.json"),
        "readiness_health_recovery_report": st._read_json(
            root / "readiness_health_recovery_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "recovery_dir": str(root),
    }
    validation = st._read_optional_json(root / "readiness_health_recovery_validation.json")
    if validation:
        payload["readiness_health_recovery_validation"] = validation
    return payload


def validate_readiness_health_recovery_artifact(
    *,
    recovery_id: str,
    output_dir: Path = DEFAULT_READINESS_HEALTH_RECOVERY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / recovery_id
    manifest = st._read_optional_json(root / "readiness_health_recovery_manifest.json") or {}
    report = st._read_optional_json(root / "readiness_health_recovery_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    status = _text(report.get("readiness_health_recovery_status"))
    source_artifacts = _mapping(report.get("source_artifacts"))
    source_statuses = _mapping(report.get("source_statuses"))
    checks = st._required_file_checks(
        root,
        (
            "readiness_health_recovery_manifest.json",
            "readiness_health_recovery_report.json",
            "readiness_health_recovery_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("recovery_id_matches", manifest.get("recovery_id") == recovery_id, ""),
            st._check("status_enum_valid", status in READINESS_HEALTH_RECOVERY_STATUSES, status),
            st._check(
                "source_artifacts_visible",
                {
                    "evidence_staleness_monitor",
                    "shadow_continuation_readiness",
                    "paper_shadow_health",
                }.issubset(set(source_artifacts)),
                ",".join(sorted(source_artifacts)),
            ),
            st._check(
                "source_statuses_visible",
                {
                    "evidence_freshness_status",
                    "shadow_continuation_readiness",
                    "paper_shadow_health_status",
                    "signal_input_status",
                }.issubset(set(source_statuses)),
                ",".join(sorted(source_statuses)),
            ),
            st._check(
                "final_status_consistent",
                status == _final_status(
                    source_statuses,
                    _mapping(report.get("source_validations")),
                ),
                status,
            ),
            st._check(
                "normal_resume_only_on_clean_sources",
                report.get("normal_paper_shadow_may_resume") is False
                or status == "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION",
                status,
            ),
            st._check(
                "manual_review_visible_when_needed",
                status == "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION"
                or report.get("manual_review_required") is True,
                status,
            ),
            st._check(
                "promotion_and_extended_shadow_forbidden",
                report.get("promotion_board_allowed") is False
                and report.get("extended_shadow_allowed") is False
                and report.get("extended_shadow_approval_allowed") is False,
                "",
            ),
            st._check(
                "official_targets_forbidden",
                report.get("official_target_weights_allowed") is False
                and report.get("official_target_weights") is False,
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "readiness_health_recovery_status" in reader
                and "normal_paper_shadow_may_resume" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_readiness_health_recovery_validation",
        recovery_id,
        checks,
    )
    if write_output:
        st._write_json(root / "readiness_health_recovery_validation.json", validation)
        st._write_text(
            root / "readiness_health_recovery_validation.md",
            render_readiness_health_recovery_validation_report(validation),
        )
    return validation


def render_readiness_health_recovery_reader_brief(report: Mapping[str, Any]) -> str:
    statuses = _mapping(report.get("source_statuses"))
    return "\n".join(
        [
            "## Readiness / Health Recovery",
            "",
            f"- readiness_health_recovery_id: {report.get('recovery_id')}",
            f"- readiness_health_recovery_status: {report.get('readiness_health_recovery_status')}",
            f"- normal_paper_shadow_may_resume: {report.get('normal_paper_shadow_may_resume')}",
            f"- signal_input_status: {statuses.get('signal_input_status')}",
            f"- evidence_freshness_status: {statuses.get('evidence_freshness_status')}",
            "- shadow_continuation_readiness: "
            f"{statuses.get('shadow_continuation_readiness')}",
            f"- paper_shadow_health_status: {statuses.get('paper_shadow_health_status')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warning_reasons: {_joined_texts(report.get('warning_reasons'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: normal paper-shadow gate only / no promotion board / "
            "no extended shadow approval / no official target / no broker / no production",
            "",
        ]
    )


def render_readiness_health_recovery_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    source_lines = [
        (
            f"- {source_id}: artifact_id={source.get('artifact_id')} "
            f"status={source.get('status')} "
            f"safe_to_continue={source.get('safe_to_continue_shadow')} "
            f"validation={source.get('validation_status')} path={source.get('report_path')}"
        )
        for source_id, source in sorted(_mapping(report.get("source_artifacts")).items())
    ]
    return "\n".join(
        [
            f"# Readiness / Health Recovery {manifest.get('recovery_id')}",
            "",
            "## Purpose",
            (
                "Rerun the minimum staleness, readiness, and canonical health chain "
                "after signal input recovery."
            ),
            "",
            "## Summary",
            f"- readiness_health_recovery_status: {report.get('readiness_health_recovery_status')}",
            f"- normal_paper_shadow_may_resume: {report.get('normal_paper_shadow_may_resume')}",
            f"- hard_stop_triggered: {report.get('hard_stop_triggered')}",
            f"- manual_review_required: {report.get('manual_review_required')}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warning_reasons: {_joined_texts(report.get('warning_reasons'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Source Chain",
            *source_lines,
            "",
            "## Safety Boundary",
            "- normal paper-shadow observation gate only",
            "- no promotion board execution or approval",
            "- no extended-shadow protocol execution or approval",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no paper account mutation",
            "- no production mutation",
            "",
        ]
    )


def render_readiness_health_recovery_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Readiness / Health Recovery Validation {validation.get('artifact_id')}",
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


def _source_statuses(
    *,
    staleness_result: Mapping[str, Any],
    readiness_result: Mapping[str, Any],
    health_result: Mapping[str, Any],
) -> dict[str, Any]:
    staleness_report = _mapping(staleness_result.get("evidence_staleness_report"))
    readiness_report = _mapping(readiness_result.get("shadow_continuation_readiness_report"))
    health_report = _mapping(health_result.get("paper_shadow_health_report"))
    return {
        "evidence_freshness_status": _text(
            staleness_report.get("evidence_freshness_status"),
            "MISSING",
        ),
        "evidence_safe_to_continue_shadow": staleness_report.get("safe_to_continue_shadow")
        is True,
        "evidence_blocking_artifacts": _texts(staleness_report.get("blocking_artifacts")),
        "evidence_stale_artifacts": _texts(staleness_report.get("stale_artifacts")),
        "evidence_missing_artifacts": _texts(staleness_report.get("missing_artifacts")),
        "shadow_continuation_readiness": _text(
            readiness_report.get("shadow_continuation_readiness"),
            "MISSING",
        ),
        "shadow_continuation_safe_to_continue_shadow": readiness_report.get(
            "safe_to_continue_shadow"
        )
        is True,
        "shadow_continuation_missing_artifacts": _texts(
            readiness_report.get("missing_artifacts")
        ),
        "shadow_continuation_blocking_artifacts": _texts(
            readiness_report.get("blocking_artifacts")
        ),
        "shadow_continuation_stale_artifacts": _texts(readiness_report.get("stale_artifacts")),
        "shadow_continuation_manual_review_required": readiness_report.get(
            "manual_review_required"
        )
        is True,
        "paper_shadow_health_status": _text(
            health_report.get("paper_shadow_health_status"),
            "MISSING",
        ),
        "paper_shadow_health_safe_to_continue_shadow": health_report.get(
            "safe_to_continue_shadow"
        )
        is True,
        "paper_shadow_health_blocking_reasons": _texts(
            health_report.get("blocking_reasons")
        ),
        "paper_shadow_health_warnings": _texts(health_report.get("warnings")),
        "signal_input_status": _text(
            health_report.get("signal_input_status")
            or readiness_report.get("signal_input_status")
            or staleness_report.get("signal_input_status"),
            "MISSING",
        ),
    }


def _source_artifacts(
    *,
    staleness_result: Mapping[str, Any],
    readiness_result: Mapping[str, Any],
    health_result: Mapping[str, Any],
) -> dict[str, Any]:
    staleness_report = _mapping(staleness_result.get("evidence_staleness_report"))
    staleness_manifest = _mapping(staleness_result.get("manifest"))
    readiness_report = _mapping(readiness_result.get("shadow_continuation_readiness_report"))
    readiness_manifest = _mapping(readiness_result.get("manifest"))
    health_report = _mapping(health_result.get("paper_shadow_health_report"))
    health_manifest = _mapping(health_result.get("manifest"))
    return {
        "evidence_staleness_monitor": {
            "artifact_id": _text(staleness_result.get("monitor_id")),
            "status": _text(staleness_report.get("evidence_freshness_status"), "MISSING"),
            "safe_to_continue_shadow": staleness_report.get("safe_to_continue_shadow")
            is True,
            "validation_status": _validation_status(
                staleness_result,
                "evidence_staleness_validation",
            ),
            "report_path": _text(staleness_manifest.get("evidence_staleness_report_path")),
        },
        "shadow_continuation_readiness": {
            "artifact_id": _text(readiness_result.get("readiness_id")),
            "status": _text(
                readiness_report.get("shadow_continuation_readiness"),
                "MISSING",
            ),
            "safe_to_continue_shadow": readiness_report.get("safe_to_continue_shadow")
            is True,
            "validation_status": _validation_status(
                readiness_result,
                "shadow_continuation_readiness_validation",
            ),
            "report_path": _text(
                readiness_manifest.get("shadow_continuation_readiness_report_path")
            ),
        },
        "paper_shadow_health": {
            "artifact_id": _text(health_result.get("health_id")),
            "status": _text(health_report.get("paper_shadow_health_status"), "MISSING"),
            "safe_to_continue_shadow": health_report.get("safe_to_continue_shadow") is True,
            "validation_status": _validation_status(
                health_result,
                "paper_shadow_health_validation",
            ),
            "report_path": _text(health_manifest.get("paper_shadow_health_report_path")),
        },
    }


def _source_validations(
    *,
    staleness_result: Mapping[str, Any],
    readiness_result: Mapping[str, Any],
    health_result: Mapping[str, Any],
) -> dict[str, str]:
    return {
        "evidence_staleness_monitor": _validation_status(
            staleness_result,
            "evidence_staleness_validation",
        ),
        "shadow_continuation_readiness": _validation_status(
            readiness_result,
            "shadow_continuation_readiness_validation",
        ),
        "paper_shadow_health": _validation_status(
            health_result,
            "paper_shadow_health_validation",
        ),
    }


def _validation_status(result: Mapping[str, Any], key: str) -> str:
    return _text(_mapping(result.get(key)).get("status"), "MISSING")


def _final_status(
    source_statuses: Mapping[str, Any],
    source_validations: Mapping[str, Any],
) -> str:
    if _blocking_reasons(source_statuses, source_validations):
        return "PAPER_SHADOW_STILL_BLOCKED"
    if _clean_resume(source_statuses, source_validations):
        return "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION"
    return "MANUAL_REVIEW_REQUIRED"


def _clean_resume(
    source_statuses: Mapping[str, Any],
    source_validations: Mapping[str, Any],
) -> bool:
    return (
        set(_texts(list(_mapping(source_validations).values()))) == {"PASS"}
        and _text(source_statuses.get("signal_input_status")) == "OK"
        and _text(source_statuses.get("evidence_freshness_status")) in {"FRESH", "ACCEPTABLE"}
        and source_statuses.get("evidence_safe_to_continue_shadow") is True
        and _text(source_statuses.get("shadow_continuation_readiness")) == "READY_TO_CONTINUE"
        and source_statuses.get("shadow_continuation_safe_to_continue_shadow") is True
        and _text(source_statuses.get("paper_shadow_health_status")) == "HEALTHY"
        and source_statuses.get("paper_shadow_health_safe_to_continue_shadow") is True
        and not _warning_reasons(source_statuses)
    )


def _blocking_reasons(
    source_statuses: Mapping[str, Any],
    source_validations: Mapping[str, Any],
) -> list[str]:
    reasons: list[str] = []
    failed_validations = [
        source_id
        for source_id, status in _mapping(source_validations).items()
        if _text(status) != "PASS"
    ]
    for source_id in failed_validations:
        reasons.append(f"{source_id}:validation_not_pass")
    signal_status = _text(source_statuses.get("signal_input_status"), "MISSING")
    if signal_status not in {"OK", "WARNING"}:
        reasons.append("signal_input_completeness:blocking")
    if _text(source_statuses.get("evidence_freshness_status")) == "BLOCKING":
        reasons.append("evidence_staleness:blocking")
    reasons.extend(
        f"evidence_staleness:{item}"
        for item in _texts(source_statuses.get("evidence_blocking_artifacts"))
    )
    reasons.extend(
        f"evidence_staleness_missing:{item}"
        for item in _texts(source_statuses.get("evidence_missing_artifacts"))
    )
    readiness_status = _text(source_statuses.get("shadow_continuation_readiness"))
    if readiness_status.startswith("BLOCKED_"):
        reasons.append(f"shadow_continuation:{readiness_status.lower()}")
    reasons.extend(
        f"shadow_continuation_blocking:{item}"
        for item in _texts(source_statuses.get("shadow_continuation_blocking_artifacts"))
    )
    reasons.extend(
        f"shadow_continuation_missing:{item}"
        for item in _texts(source_statuses.get("shadow_continuation_missing_artifacts"))
    )
    health_status = _text(source_statuses.get("paper_shadow_health_status"))
    if health_status.startswith("BLOCKED_"):
        reasons.append(f"paper_shadow_health:{health_status.lower()}")
    reasons.extend(
        f"paper_shadow_health:{item}"
        for item in _texts(source_statuses.get("paper_shadow_health_blocking_reasons"))
    )
    return sorted(set(reasons))


def _warning_reasons(source_statuses: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    if _text(source_statuses.get("signal_input_status")) == "WARNING":
        warnings.append("signal_input_completeness:warning")
    if _texts(source_statuses.get("evidence_stale_artifacts")):
        warnings.extend(
            f"evidence_staleness_stale:{item}"
            for item in _texts(source_statuses.get("evidence_stale_artifacts"))
        )
    readiness_status = _text(source_statuses.get("shadow_continuation_readiness"))
    if readiness_status == "READY_WITH_WARNINGS":
        warnings.append("shadow_continuation:ready_with_warnings")
    if source_statuses.get("shadow_continuation_manual_review_required") is True:
        warnings.append("shadow_continuation:manual_review_required")
    if _texts(source_statuses.get("shadow_continuation_stale_artifacts")):
        warnings.extend(
            f"shadow_continuation_stale:{item}"
            for item in _texts(source_statuses.get("shadow_continuation_stale_artifacts"))
        )
    health_status = _text(source_statuses.get("paper_shadow_health_status"))
    if health_status == "HEALTHY_WITH_WARNINGS":
        warnings.append("paper_shadow_health:healthy_with_warnings")
    warnings.extend(
        f"paper_shadow_health:{item}"
        for item in _texts(source_statuses.get("paper_shadow_health_warnings"))
    )
    if health_status == "MANUAL_REVIEW_REQUIRED":
        warnings.append("paper_shadow_health:manual_review_required")
    return sorted(set(warnings))


def _next_action(status: str) -> str:
    if status == "PAPER_SHADOW_CAN_RESUME_NORMAL_OBSERVATION":
        return "resume_normal_paper_shadow_observation_only"
    if status == "PAPER_SHADOW_STILL_BLOCKED":
        return "stop_normal_paper_shadow_until_readiness_health_blockers_clear"
    return "owner_review_required_before_normal_paper_shadow_resumption"


def _joined_texts(value: object, sep: str = ", ") -> str:
    return sep.join(_texts(value)) or "none"


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts
