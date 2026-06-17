from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as sic
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_input_completeness_recovery"
)

SIGNAL_INPUT_COMPLETENESS_RECOVERY_STATUSES = (
    "SIGNAL_INPUTS_RESTORED",
    "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
    "SIGNAL_INPUTS_STILL_BLOCKED",
)

RECOVERY_CHECK_IDS = (
    "required_signal_files_exist",
    "schema_versions_compatible",
    "signal_series_non_empty",
    "feature_columns_present",
    "market_coverage_sufficient",
    "as_of_date_consistent",
)

SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY = {
    **sic.SIGNAL_INPUT_COMPLETENESS_SAFETY,
    "signal_input_completeness_recovery_only": True,
    "completeness_monitor_rerun_by_recovery": True,
    "upstream_generation_executed_by_recovery": False,
    "signal_artifacts_fabricated": False,
    "completeness_policy_weakened": False,
}


def run_signal_input_completeness_recovery(
    *,
    as_of: date | None = None,
    monitor_id: str | None = None,
    rerun_monitor: bool = True,
    policy_path: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    signal_input_dir: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    monitor_payload = _monitor_payload(
        as_of=as_of,
        monitor_id=monitor_id,
        rerun_monitor=rerun_monitor,
        policy_path=policy_path,
        signal_input_dir=signal_input_dir,
        generated_at=generated,
    )
    monitor_report = _mapping(monitor_payload.get("signal_input_completeness_report"))
    findings = _records(monitor_payload.get("signal_input_completeness_findings"))
    effective_as_of = as_of or _date_from_report(monitor_report) or generated.date()
    status = _recovery_status(monitor_report)
    recovery_id = st._stable_id(
        "signal-input-completeness-recovery",
        effective_as_of.isoformat(),
        _text(monitor_report.get("monitor_id")),
        status,
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / recovery_id)
    root.mkdir(parents=True, exist_ok=False)
    checks = _recovery_checks(monitor_report=monitor_report, findings=findings)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_recovery_report",
        "recovery_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "policy_path": str(policy_path),
        "policy_id": monitor_report.get("policy_id"),
        "policy_version": _text(monitor_report.get("policy_version")),
        "source_monitor_id": _text(monitor_report.get("monitor_id")),
        "signal_input_status": _text(monitor_report.get("signal_input_status"), "MISSING"),
        "recovery_status": status,
        "blocking_count": _int(monitor_report.get("blocking_count")),
        "warning_count": _int(monitor_report.get("warning_count")),
        "blocker_list": _blocker_list(monitor_report, findings),
        "warning_list": _warning_list(monitor_report, findings),
        "recovery_checks": checks,
        "check_summary": _check_summary(checks),
        "hard_stop_triggered": status == "SIGNAL_INPUTS_STILL_BLOCKED",
        "next_required_action": _next_action(status),
        "reader_brief_quality": "OK",
        **SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_recovery_manifest",
        "recovery_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "recovery_status": status,
        "source_monitor_id": report["source_monitor_id"],
        "signal_input_completeness_recovery_manifest_path": str(
            root / "signal_input_completeness_recovery_manifest.json"
        ),
        "signal_input_completeness_recovery_report_path": str(
            root / "signal_input_completeness_recovery_report.json"
        ),
        "signal_input_completeness_recovery_markdown_path": str(
            root / "signal_input_completeness_recovery_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "signal_input_completeness_recovery_validation.json"),
        **SIGNAL_INPUT_COMPLETENESS_RECOVERY_SAFETY,
    }
    reader = render_signal_input_completeness_recovery_reader_brief(report)
    st._write_json(root / "signal_input_completeness_recovery_manifest.json", manifest)
    st._write_json(root / "signal_input_completeness_recovery_report.json", report)
    st._write_text(
        root / "signal_input_completeness_recovery_report.md",
        render_signal_input_completeness_recovery_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_signal_input_completeness_recovery",
        root.name,
        root / "signal_input_completeness_recovery_manifest.json",
    )
    validation = validate_signal_input_completeness_recovery_artifact(
        recovery_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "recovery_id": root.name,
        "recovery_dir": root,
        "manifest": manifest,
        "signal_input_completeness_recovery_report": report,
        "reader_brief_section": reader,
        "signal_input_completeness_recovery_validation": validation,
        "source_monitor_payload": monitor_payload,
    }


def signal_input_completeness_recovery_report_payload(
    *,
    recovery_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=recovery_id,
        latest_pointer="latest_signal_input_completeness_recovery",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_input_completeness_recovery_manifest.json",
    )
    payload = {
        **st._read_json(root / "signal_input_completeness_recovery_manifest.json"),
        "signal_input_completeness_recovery_report": st._read_json(
            root / "signal_input_completeness_recovery_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "recovery_dir": str(root),
    }
    validation = st._read_optional_json(
        root / "signal_input_completeness_recovery_validation.json"
    )
    if validation:
        payload["signal_input_completeness_recovery_validation"] = validation
    return payload


def validate_signal_input_completeness_recovery_artifact(
    *,
    recovery_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_RECOVERY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / recovery_id
    manifest = (
        st._read_optional_json(root / "signal_input_completeness_recovery_manifest.json")
        or {}
    )
    report = (
        st._read_optional_json(root / "signal_input_completeness_recovery_report.json")
        or {}
    )
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    status = _text(report.get("recovery_status"))
    signal_status = _text(report.get("signal_input_status"))
    checks = st._required_file_checks(
        root,
        (
            "signal_input_completeness_recovery_manifest.json",
            "signal_input_completeness_recovery_report.json",
            "signal_input_completeness_recovery_report.md",
            "reader_brief_section.md",
        ),
    )
    recovery_checks = _records(report.get("recovery_checks"))
    checks.extend(
        [
            st._check("recovery_id_matches", manifest.get("recovery_id") == recovery_id, ""),
            st._check(
                "status_enum_valid",
                status in SIGNAL_INPUT_COMPLETENESS_RECOVERY_STATUSES,
                status,
            ),
            st._check(
                "status_consistent_with_monitor",
                _status_consistent(status, signal_status, _int(report.get("warning_count"))),
                f"{status}/{signal_status}",
            ),
            st._check(
                "source_monitor_visible",
                bool(_text(report.get("source_monitor_id"))),
                "",
            ),
            st._check(
                "recovery_checks_complete",
                set(RECOVERY_CHECK_IDS).issubset(
                    {_text(row.get("check_id")) for row in recovery_checks}
                ),
                ",".join(_text(row.get("check_id")) for row in recovery_checks),
            ),
            st._check(
                "blocking_reason_visible_when_blocked",
                status != "SIGNAL_INPUTS_STILL_BLOCKED"
                or bool(_texts(report.get("blocker_list"))),
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "signal_input_completeness_recovery_status" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "no_signal_fabrication",
                report.get("signal_artifacts_fabricated") is False
                and report.get("completeness_policy_weakened") is False,
                "",
            ),
            st._check(
                "no_upstream_generation",
                report.get("upstream_generation_executed_by_recovery") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_signal_input_completeness_recovery_validation",
        recovery_id,
        checks,
    )
    if write_output:
        st._write_json(root / "signal_input_completeness_recovery_validation.json", validation)
        st._write_text(
            root / "signal_input_completeness_recovery_validation.md",
            render_signal_input_completeness_recovery_validation_report(validation),
        )
    return validation


def render_signal_input_completeness_recovery_reader_brief(
    report: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Signal Input Completeness Recovery",
            "",
            f"- signal_input_completeness_recovery_id: {report.get('recovery_id')}",
            f"- signal_input_completeness_recovery_status: {report.get('recovery_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- source_monitor_id: {report.get('source_monitor_id')}",
            f"- blocking_count: {report.get('blocking_count')}",
            f"- warning_count: {report.get('warning_count')}",
            f"- blocker_list: {_joined_texts(report.get('blocker_list'))}",
            f"- warning_list: {_joined_texts(report.get('warning_list'))}",
            "- checks_failed: "
            f"{_mapping(report.get('check_summary')).get('failed_check_count', 0)}",
            f"- hard_stop_triggered: {report.get('hard_stop_triggered')}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: completeness recovery only / no signal fabrication / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_signal_input_completeness_recovery_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    check_lines = [
        (
            f"- {row.get('check_id')}: status={row.get('status')} "
            f"passed={row.get('passed')} detail={row.get('detail')}"
        )
        for row in _records(report.get("recovery_checks"))
    ]
    return "\n".join(
        [
            f"# Signal Input Completeness Recovery {manifest.get('recovery_id')}",
            "",
            "## Purpose",
            "Rerun or inspect signal input completeness after restoration.",
            "",
            "## Summary",
            f"- recovery_status: {report.get('recovery_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- source_monitor_id: {report.get('source_monitor_id')}",
            f"- blocking_count: {report.get('blocking_count')}",
            f"- warning_count: {report.get('warning_count')}",
            f"- blocker_list: {_joined_texts(report.get('blocker_list'))}",
            f"- warning_list: {_joined_texts(report.get('warning_list'))}",
            f"- hard_stop_triggered: {report.get('hard_stop_triggered')}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Recovery Checks",
            *check_lines,
            "",
            "## Safety Boundary",
            "- signal input completeness recovery rerun only",
            "- no feature/signal/snapshot generation by this report",
            "- no signal artifact fabrication",
            "- no completeness policy weakening",
            "- no official target weights",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_signal_input_completeness_recovery_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Signal Input Completeness Recovery Validation {validation.get('artifact_id')}",
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


def _monitor_payload(
    *,
    as_of: date | None,
    monitor_id: str | None,
    rerun_monitor: bool,
    policy_path: Path,
    signal_input_dir: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    if rerun_monitor:
        return sic.run_signal_input_completeness_monitor(
            as_of=as_of,
            policy_path=policy_path,
            output_dir=signal_input_dir,
            generated_at=generated_at,
        )
    return sic.signal_input_completeness_report_payload(
        monitor_id=monitor_id,
        latest=monitor_id is None,
        output_dir=signal_input_dir,
    )


def _recovery_checks(
    *,
    monitor_report: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    issues = {row.get("check_id"): row for row in _issue_check_rows(findings)}
    requested_as_of = _text(monitor_report.get("requested_as_of") or monitor_report.get("as_of"))
    future_inputs = [
        _text(row.get("input_id"))
        for row in findings
        if "timestamp_after_requested_as_of" in set(_texts(row.get("issue_codes")))
    ]
    checks = [
        _check_row(
            "required_signal_files_exist",
            not _texts(monitor_report.get("missing_signal_files")),
            _joined_texts(monitor_report.get("missing_signal_files")),
        ),
        _check_row(
            "schema_versions_compatible",
            not _texts(monitor_report.get("incompatible_schema_inputs")),
            _joined_texts(monitor_report.get("incompatible_schema_inputs")),
        ),
        _check_row(
            "signal_series_non_empty",
            not _texts(monitor_report.get("empty_signal_series_inputs")),
            _joined_texts(monitor_report.get("empty_signal_series_inputs")),
        ),
        _check_row(
            "feature_columns_present",
            not _texts(monitor_report.get("missing_required_feature_columns")),
            _joined_texts(monitor_report.get("missing_required_feature_columns")),
        ),
        _check_row(
            "market_coverage_sufficient",
            not _texts(monitor_report.get("partial_market_coverage_inputs")),
            _joined_texts(monitor_report.get("partial_market_coverage_inputs")),
        ),
        _check_row(
            "as_of_date_consistent",
            bool(requested_as_of) and not future_inputs,
            f"requested_as_of={requested_as_of}; future_inputs={','.join(future_inputs) or 'none'}",
        ),
    ]
    for check in checks:
        check.update(_mapping(issues.get(check["check_id"])))
    return checks


def _issue_check_rows(findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not findings:
        return [
            {
                "check_id": "required_signal_files_exist",
                "affected_inputs": ["signal_input_completeness_report"],
            }
        ]
    return []


def _check_row(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "detail": detail,
    }


def _check_summary(checks: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    failed = [row for row in checks if row.get("passed") is not True]
    return {
        "check_count": len(checks),
        "passed_check_count": len(checks) - len(failed),
        "failed_check_count": len(failed),
    }


def _blocker_list(
    monitor_report: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> list[str]:
    blockers = [
        f"{input_id}:blocking"
        for input_id in _texts(monitor_report.get("blocking_input_ids"))
    ]
    blockers.extend(
        f"{_text(row.get('input_id'))}:{code}"
        for row in findings
        if row.get("severity") == "BLOCKING"
        for code in _texts(row.get("issue_codes"))
    )
    return sorted(set(blockers))


def _warning_list(
    monitor_report: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> list[str]:
    warnings = [
        f"{input_id}:warning"
        for input_id in _texts(monitor_report.get("warning_input_ids"))
    ]
    warnings.extend(
        f"{_text(row.get('input_id'))}:{code}"
        for row in findings
        if row.get("severity") == "WARNING"
        for code in _texts(row.get("issue_codes"))
    )
    return sorted(set(warnings))


def _recovery_status(report: Mapping[str, Any]) -> str:
    status = _text(report.get("signal_input_status"), "BLOCKING")
    if status == "BLOCKING":
        return "SIGNAL_INPUTS_STILL_BLOCKED"
    if status == "WARNING":
        return "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    return "SIGNAL_INPUTS_RESTORED"


def _next_action(status: str) -> str:
    if status == "SIGNAL_INPUTS_STILL_BLOCKED":
        return "stop_and_report_SIGNAL_INPUTS_STILL_BLOCKED"
    if status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS":
        return "review_signal_input_warnings_before_readiness_health_recovery"
    return "continue_to_readiness_and_paper_shadow_health_recovery"


def _status_consistent(status: str, signal_status: str, warning_count: int) -> bool:
    if signal_status == "BLOCKING":
        return status == "SIGNAL_INPUTS_STILL_BLOCKED"
    if signal_status == "WARNING" or warning_count > 0:
        return status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    return status == "SIGNAL_INPUTS_RESTORED"


def _date_from_report(report: Mapping[str, Any]) -> date | None:
    text = _text(report.get("as_of") or report.get("requested_as_of"))
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


_mapping = st._mapping
_records = st._records
_text = st._text
_texts = st._texts


def _joined_texts(value: object, sep: str = ", ") -> str:
    return sep.join(_texts(value)) or "none"
