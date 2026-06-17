from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.etf_portfolio import dynamic_v3_signal_input_completeness as sic
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_SIGNAL_INPUT_RECOVERY_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_input_recovery"
)

SIGNAL_INPUT_RECOVERY_STATUSES = (
    "SIGNAL_INPUTS_RESTORED",
    "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS",
    "SIGNAL_INPUTS_STILL_BLOCKED",
)

CANONICAL_GENERATION_PATHS: dict[str, dict[str, str]] = {
    "etf_feature_matrix": {
        "intended_command": "aits etf features build --end latest",
        "source_inputs": (
            "data/raw/prices_daily.csv; config/etf_portfolio/assets.yaml; "
            "config/etf_portfolio/strategy.yaml"
        ),
        "output_artifact": "data/etf_portfolio/features.csv",
        "data_quality_gate": "aits etf data validate; aits validate-data --as-of <as_of>",
        "restoration_boundary": "regenerate from ETF price cache through feature builder only",
    },
    "etf_signal_series": {
        "intended_command": "aits etf signals generate --date latest",
        "source_inputs": "data/etf_portfolio/features.csv; config/etf_portfolio/strategy.yaml",
        "output_artifact": "data/etf_portfolio/signals.csv",
        "data_quality_gate": "feature matrix must have been built after ETF data quality PASS",
        "restoration_boundary": "generate from canonical feature matrix only",
    },
    "daily_feature_records": {
        "intended_command": "aits build-features --as-of <as_of>",
        "source_inputs": (
            "data/raw/prices_daily.csv; data/raw/rates_daily.csv; "
            "config/features.yaml"
        ),
        "output_artifact": "data/processed/features_daily.csv",
        "data_quality_gate": "aits validate-data --as-of <as_of>",
        "restoration_boundary": "build from cached market and macro inputs only",
    },
    "latest_signal_snapshot": {
        "intended_command": (
            "aits signals build-snapshot --latest; "
            "aits reports signal-snapshot --latest"
        ),
        "source_inputs": (
            "data/raw/prices_daily.csv; config/parameters/shadow_backtest.yaml; "
            "production baseline parameters"
        ),
        "output_artifact": (
            "artifacts/signal_snapshots/YYYY-MM-DD/signal_snapshot.json; "
            "outputs/reports/signal_snapshot_YYYY-MM-DD.json"
        ),
        "data_quality_gate": "latest price cache must pass validation before interpretation",
        "restoration_boundary": "build and expose report alias; do not fabricate missing snapshots",
    },
}

SIGNAL_INPUT_RECOVERY_SAFETY = {
    **sic.SIGNAL_INPUT_COMPLETENESS_SAFETY,
    "signal_input_recovery_report_only": True,
    "root_cause_report_only": True,
    "artifacts_fabricated": False,
    "manual_signal_artifact_fabrication": False,
    "upstream_pipeline_executed_by_report": False,
}


def run_signal_input_root_cause_recovery(
    *,
    as_of: date | None = None,
    restored_monitor_id: str | None = None,
    previous_monitor_id: str | None = None,
    signal_input_dir: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    policy_path: Path = sic.DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    current_payload = sic.signal_input_completeness_report_payload(
        monitor_id=restored_monitor_id,
        latest=restored_monitor_id is None,
        output_dir=signal_input_dir,
    )
    current_report = _mapping(current_payload.get("signal_input_completeness_report"))
    current_findings = _records(current_payload.get("signal_input_completeness_findings"))
    effective_as_of = as_of or _date_from_report(current_report) or generated.date()
    previous_payload = _optional_signal_payload(
        previous_monitor_id=previous_monitor_id,
        signal_input_dir=signal_input_dir,
    )
    previous_report = _mapping(previous_payload.get("signal_input_completeness_report"))
    previous_findings = _records(previous_payload.get("signal_input_completeness_findings"))
    diagnosis_source = previous_findings or current_findings
    policy = sic.load_signal_input_completeness_policy(policy_path)
    required_inputs = sorted(_mapping(policy.get("required_inputs")))
    root_causes = _root_cause_rows(
        source_findings=diagnosis_source,
        restored_findings=current_findings,
    )
    restoration_status = _recovery_status(current_report)
    recovery_id = st._stable_id(
        "signal-input-recovery",
        effective_as_of.isoformat(),
        _text(current_report.get("monitor_id")),
        _text(previous_report.get("monitor_id")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / recovery_id)
    root.mkdir(parents=True, exist_ok=False)
    restored_artifacts = _restored_artifact_rows(current_findings)
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_recovery_root_cause_report",
        "recovery_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "policy_path": str(policy_path),
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "source_monitor_id": _text(current_report.get("monitor_id")),
        "previous_monitor_id": _text(previous_report.get("monitor_id")),
        "signal_input_status": _text(current_report.get("signal_input_status"), "MISSING"),
        "blocking_count": _int(current_report.get("blocking_count")),
        "warning_count": _int(current_report.get("warning_count")),
        "restoration_status": restoration_status,
        "root_cause_summary": _root_cause_summary(root_causes),
        "root_cause_rows": root_causes,
        "canonical_generation_paths": {
            input_id: CANONICAL_GENERATION_PATHS.get(input_id, _unknown_generation_path(input_id))
            for input_id in required_inputs
        },
        "restored_artifacts": restored_artifacts,
        "restored_etf_feature_matrix_artifact_id": _artifact_id_for(
            restored_artifacts,
            "etf_feature_matrix",
        ),
        "restored_etf_signal_series_artifact_id": _artifact_id_for(
            restored_artifacts,
            "etf_signal_series",
        ),
        "blocking_reasons": _blocking_reasons(current_report),
        "warnings": _warning_reasons(current_report),
        "next_required_action": _next_action(restoration_status, current_report),
        "hard_stop_triggered": restoration_status == "SIGNAL_INPUTS_STILL_BLOCKED",
        "reader_brief_quality": "OK",
        **SIGNAL_INPUT_RECOVERY_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_recovery_manifest",
        "recovery_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": restoration_status,
        "restoration_status": restoration_status,
        "source_monitor_id": report["source_monitor_id"],
        "previous_monitor_id": report["previous_monitor_id"],
        "signal_input_recovery_manifest_path": str(
            root / "signal_input_recovery_manifest.json"
        ),
        "signal_input_recovery_report_path": str(
            root / "signal_input_recovery_report.json"
        ),
        "signal_input_recovery_markdown_path": str(
            root / "signal_input_recovery_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "signal_input_recovery_validation.json"),
        **SIGNAL_INPUT_RECOVERY_SAFETY,
    }
    reader = render_signal_input_recovery_reader_brief(report)
    st._write_json(root / "signal_input_recovery_manifest.json", manifest)
    st._write_json(root / "signal_input_recovery_report.json", report)
    st._write_text(
        root / "signal_input_recovery_report.md",
        render_signal_input_recovery_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_signal_input_recovery",
        root.name,
        root / "signal_input_recovery_manifest.json",
    )
    validation = validate_signal_input_recovery_artifact(
        recovery_id=root.name,
        output_dir=output_dir,
        write_output=True,
    )
    return {
        "recovery_id": root.name,
        "recovery_dir": root,
        "manifest": manifest,
        "signal_input_recovery_report": report,
        "reader_brief_section": reader,
        "signal_input_recovery_validation": validation,
    }


def signal_input_recovery_report_payload(
    *,
    recovery_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=recovery_id,
        latest_pointer="latest_signal_input_recovery",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_input_recovery_manifest.json",
    )
    payload = {
        **st._read_json(root / "signal_input_recovery_manifest.json"),
        "signal_input_recovery_report": st._read_json(
            root / "signal_input_recovery_report.json"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "recovery_dir": str(root),
    }
    validation = st._read_optional_json(root / "signal_input_recovery_validation.json")
    if validation:
        payload["signal_input_recovery_validation"] = validation
    return payload


def validate_signal_input_recovery_artifact(
    *,
    recovery_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_RECOVERY_DIR,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / recovery_id
    manifest = st._read_optional_json(root / "signal_input_recovery_manifest.json") or {}
    report = st._read_optional_json(root / "signal_input_recovery_report.json") or {}
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    checks = st._required_file_checks(
        root,
        (
            "signal_input_recovery_manifest.json",
            "signal_input_recovery_report.json",
            "signal_input_recovery_report.md",
            "reader_brief_section.md",
        ),
    )
    status = _text(report.get("restoration_status"))
    signal_status = _text(report.get("signal_input_status"))
    canonical = _mapping(report.get("canonical_generation_paths"))
    checks.extend(
        [
            st._check("recovery_id_matches", manifest.get("recovery_id") == recovery_id, ""),
            st._check("status_enum_valid", status in SIGNAL_INPUT_RECOVERY_STATUSES, status),
            st._check(
                "status_consistent_with_signal_inputs",
                _status_consistent(status, signal_status, _int(report.get("warning_count"))),
                f"{status}/{signal_status}",
            ),
            st._check(
                "canonical_paths_visible",
                all(
                    bool(_mapping(value).get("intended_command"))
                    and bool(_mapping(value).get("output_artifact"))
                    for value in canonical.values()
                )
                and {"etf_feature_matrix", "etf_signal_series"}.issubset(set(canonical)),
                ",".join(sorted(canonical)),
            ),
            st._check(
                "root_cause_visible",
                bool(_records(report.get("root_cause_rows")))
                or bool(_mapping(report.get("root_cause_summary")).get("primary_root_cause")),
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "signal_input_recovery_status" in reader
                and "next_required_action" in reader,
                "",
            ),
            st._check(
                "no_artifact_fabrication",
                report.get("artifacts_fabricated") is False
                and report.get("manual_signal_artifact_fabrication") is False,
                "",
            ),
            st._check(
                "read_only_report",
                report.get("upstream_pipeline_executed_by_report") is False
                and report.get("data_downloaded_by_monitor") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_signal_input_recovery_validation",
        recovery_id,
        checks,
    )
    if write_output:
        st._write_json(root / "signal_input_recovery_validation.json", validation)
        st._write_text(
            root / "signal_input_recovery_validation.md",
            render_signal_input_recovery_validation_report(validation),
        )
    return validation


def render_signal_input_recovery_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Input Recovery",
            "",
            f"- signal_input_recovery_id: {report.get('recovery_id')}",
            f"- signal_input_recovery_status: {report.get('restoration_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- source_monitor_id: {report.get('source_monitor_id')}",
            f"- previous_monitor_id: {report.get('previous_monitor_id') or 'none'}",
            "- root_cause: "
            f"{_mapping(report.get('root_cause_summary')).get('primary_root_cause', 'none')}",
            "- restored_etf_feature_matrix_artifact_id: "
            f"{report.get('restored_etf_feature_matrix_artifact_id') or 'none'}",
            "- restored_etf_signal_series_artifact_id: "
            f"{report.get('restored_etf_signal_series_artifact_id') or 'none'}",
            f"- blocking_reasons: {_joined_texts(report.get('blocking_reasons'))}",
            f"- warnings: {_joined_texts(report.get('warnings'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "- safety_boundary: root-cause report only / no signal fabrication / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_signal_input_recovery_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    root_lines = [
        (
            f"- {row.get('input_id')}: root_cause={row.get('root_cause')} "
            f"previous_severity={row.get('previous_severity')} "
            f"restored_severity={row.get('restored_severity')} "
            f"command=`{row.get('intended_command')}`"
        )
        for row in _records(report.get("root_cause_rows"))
    ]
    artifact_lines = [
        (
            f"- {row.get('input_id')}: artifact_id={row.get('artifact_id')} "
            f"latest_date={row.get('latest_date')} row_count={row.get('row_count')} "
            f"path={row.get('source_path')}"
        )
        for row in _records(report.get("restored_artifacts"))
    ]
    return "\n".join(
        [
            f"# Signal Input Root Cause Recovery {manifest.get('recovery_id')}",
            "",
            "## Purpose",
            "Record the root cause and recovery status for required signal inputs.",
            "",
            "## Summary",
            f"- restoration_status: {report.get('restoration_status')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- source_monitor_id: {report.get('source_monitor_id')}",
            f"- previous_monitor_id: {report.get('previous_monitor_id') or 'none'}",
            "- primary_root_cause: "
            f"{_mapping(report.get('root_cause_summary')).get('primary_root_cause', 'none')}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Root Cause Rows",
            *(root_lines or ["- none"]),
            "",
            "## Restored Artifacts",
            *(artifact_lines or ["- none"]),
            "",
            "## Safety Boundary",
            "- report-only recovery audit",
            "- no data download or upstream execution by this report",
            "- no manual signal artifact fabrication",
            "- no official target weights",
            "- no paper account mutation",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_signal_input_recovery_validation_report(validation: Mapping[str, Any]) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Signal Input Recovery Validation {validation.get('artifact_id')}",
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


def _optional_signal_payload(
    *,
    previous_monitor_id: str | None,
    signal_input_dir: Path,
) -> dict[str, Any]:
    if not previous_monitor_id:
        return {}
    try:
        return sic.signal_input_completeness_report_payload(
            monitor_id=previous_monitor_id,
            output_dir=signal_input_dir,
        )
    except st.DynamicV3SystemTargetError:
        return {}


def _root_cause_rows(
    *,
    source_findings: Sequence[Mapping[str, Any]],
    restored_findings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    restored_by_input = {_text(row.get("input_id")): row for row in restored_findings}
    rows: list[dict[str, Any]] = []
    for previous in source_findings:
        input_id = _text(previous.get("input_id"))
        restored = _mapping(restored_by_input.get(input_id))
        issue_codes = _texts(previous.get("issue_codes"))
        root_cause = _classify_root_cause(input_id, issue_codes)
        previous_severity = _text(previous.get("severity"), "UNKNOWN")
        restored_severity = _text(restored.get("severity"), "UNKNOWN")
        if not issue_codes and previous_severity == "OK" and restored_severity == "OK":
            continue
        generation = CANONICAL_GENERATION_PATHS.get(input_id, _unknown_generation_path(input_id))
        rows.append(
            {
                "input_id": input_id,
                "root_cause": root_cause,
                "issue_codes": issue_codes,
                "previous_severity": previous_severity,
                "previous_latest_date": _text(previous.get("latest_date")),
                "previous_age_days": previous.get("age_days"),
                "restored_severity": restored_severity,
                "restored_latest_date": _text(restored.get("latest_date")),
                "restored_age_days": restored.get("age_days"),
                "restoration_result": (
                    "RESTORED"
                    if previous_severity == "BLOCKING" and restored_severity != "BLOCKING"
                    else "STILL_BLOCKED"
                    if restored_severity == "BLOCKING"
                    else "UNCHANGED_NON_BLOCKING"
                ),
                "intended_command": generation["intended_command"],
                "output_artifact": generation["output_artifact"],
            }
        )
    if rows:
        return rows
    return [
        {
            "input_id": "signal_input_completeness",
            "root_cause": "no_current_blocking_issue",
            "issue_codes": [],
            "previous_severity": "OK",
            "previous_latest_date": "",
            "previous_age_days": None,
            "restored_severity": "OK",
            "restored_latest_date": "",
            "restored_age_days": None,
            "restoration_result": "UNCHANGED_NON_BLOCKING",
            "intended_command": (
                "aits etf dynamic-v3-rescue signal-input-completeness "
                "run --as-of <as_of>"
            ),
            "output_artifact": (
                "reports/etf_portfolio/dynamic_v3_rescue/"
                "signal_input_completeness/*"
            ),
        }
    ]


def _classify_root_cause(input_id: str, issue_codes: Sequence[str]) -> str:
    issues = set(issue_codes)
    if "stale_signal_file" in issues:
        return "stale_cache"
    if "missing_signal_file" in issues and input_id == "latest_signal_snapshot":
        return "report_alias_or_snapshot_missing"
    if "missing_signal_file" in issues:
        return "upstream_artifact_missing"
    if "incompatible_schema_version" in issues:
        return "schema_mismatch"
    if "partial_market_coverage" in issues:
        return "market_coverage_gap"
    if "timestamp_after_requested_as_of" in _texts(issue_codes):
        return "calendar_or_as_of_mismatch"
    if "empty_signal_series" in issues:
        return "empty_signal_series"
    if "missing_required_feature_columns" in issues:
        return "schema_or_required_field_gap"
    return "non_blocking_signal_input_warning" if issues else "no_current_blocking_issue"


def _root_cause_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    restored = 0
    still_blocked = 0
    for row in rows:
        root_cause = _text(row.get("root_cause"), "unknown")
        counts[root_cause] = counts.get(root_cause, 0) + 1
        if row.get("restoration_result") == "RESTORED":
            restored += 1
        if row.get("restoration_result") == "STILL_BLOCKED":
            still_blocked += 1
    primary = max(counts, key=counts.get) if counts else "none"
    return {
        "primary_root_cause": primary,
        "root_cause_counts": counts,
        "restored_count": restored,
        "still_blocked_count": still_blocked,
    }


def _restored_artifact_rows(findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for finding in findings:
        input_id = _text(finding.get("input_id"))
        path = _text(finding.get("source_path"))
        latest_date = _text(finding.get("latest_date"))
        artifact_id = f"{input_id}:{latest_date}:{_text(finding.get('checksum_sha256'))[:12]}"
        rows.append(
            {
                "input_id": input_id,
                "artifact_id": artifact_id,
                "source_path": path,
                "latest_date": latest_date,
                "row_count": _int(finding.get("row_count")),
                "checksum_sha256": _text(finding.get("checksum_sha256")),
                "severity": _text(finding.get("severity")),
                "issue_codes": _texts(finding.get("issue_codes")),
            }
        )
    return rows


def _artifact_id_for(rows: Sequence[Mapping[str, Any]], input_id: str) -> str:
    for row in rows:
        if row.get("input_id") == input_id:
            return _text(row.get("artifact_id"))
    return ""


def _blocking_reasons(report: Mapping[str, Any]) -> list[str]:
    return [
        f"{input_id}:blocking"
        for input_id in _texts(report.get("blocking_input_ids"))
    ]


def _warning_reasons(report: Mapping[str, Any]) -> list[str]:
    return [
        f"{input_id}:warning"
        for input_id in _texts(report.get("warning_input_ids"))
    ]


def _recovery_status(report: Mapping[str, Any]) -> str:
    status = _text(report.get("signal_input_status"), "BLOCKING")
    if status == "BLOCKING":
        return "SIGNAL_INPUTS_STILL_BLOCKED"
    if status == "WARNING":
        return "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    return "SIGNAL_INPUTS_RESTORED"


def _next_action(restoration_status: str, report: Mapping[str, Any]) -> str:
    if restoration_status == "SIGNAL_INPUTS_STILL_BLOCKED":
        return "stop_and_report_SIGNAL_INPUTS_STILL_BLOCKED"
    if restoration_status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS":
        return "review_signal_input_warnings_then_rerun_completeness_recovery"
    return _text(report.get("next_required_action"), "rerun_signal_input_completeness_recovery")


def _status_consistent(status: str, signal_status: str, warning_count: int) -> bool:
    if signal_status == "BLOCKING":
        return status == "SIGNAL_INPUTS_STILL_BLOCKED"
    if warning_count > 0 or signal_status == "WARNING":
        return status == "SIGNAL_INPUTS_RESTORED_WITH_WARNINGS"
    return status == "SIGNAL_INPUTS_RESTORED"


def _unknown_generation_path(input_id: str) -> dict[str, str]:
    return {
        "intended_command": "unknown_manual_review_required",
        "source_inputs": "unknown",
        "output_artifact": input_id,
        "data_quality_gate": "manual_review_required",
        "restoration_boundary": "do not fabricate missing signal input",
    }


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
