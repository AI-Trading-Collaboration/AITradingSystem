from __future__ import annotations

import csv
import hashlib
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st

DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR = (
    st.DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "signal_input_completeness"
)
DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH = (
    st.PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "signal_input_completeness_v1.yaml"
)

SIGNAL_INPUT_STATUSES = ("OK", "WARNING", "BLOCKING")
SIGNAL_INPUT_COMPLETENESS_SAFETY = {
    **st.SYSTEM_TARGET_SAFETY,
    "manual_review_only": True,
    "signal_input_completeness_monitor_only": True,
    "read_only_signal_input_check": True,
    "data_downloaded_by_monitor": False,
    "pipelines_executed_by_monitor": False,
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


def run_signal_input_completeness_monitor(
    *,
    as_of: date | None = None,
    policy_path: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    effective_as_of = as_of or generated.date()
    policy = load_signal_input_completeness_policy(policy_path)
    findings = _signal_input_findings(policy=policy, as_of=effective_as_of)
    summary = _signal_input_summary(policy=policy, findings=findings)
    monitor_id = st._stable_id(
        "signal-input-completeness",
        effective_as_of.isoformat(),
        _text(policy.get("policy_id")),
        _text(policy.get("version")),
        generated.isoformat(),
    )
    root = st._unique_dir(output_dir / monitor_id)
    root.mkdir(parents=True, exist_ok=False)
    next_action = _mapping(policy.get("default_next_actions")).get(
        summary["signal_input_status"],
        "manual_signal_input_review_required",
    )
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_report",
        "monitor_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "policy_path": str(policy_path),
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "signal_input_status": summary["signal_input_status"],
        "severity": summary["signal_input_status"],
        "blocking_count": summary["blocking_count"],
        "warning_count": summary["warning_count"],
        "ok_count": summary["ok_count"],
        "finding_count": len(findings),
        "blocking_input_ids": summary["blocking_input_ids"],
        "warning_input_ids": summary["warning_input_ids"],
        "missing_signal_files": summary["missing_signal_files"],
        "stale_signal_files": summary["stale_signal_files"],
        "incompatible_schema_inputs": summary["incompatible_schema_inputs"],
        "empty_signal_series_inputs": summary["empty_signal_series_inputs"],
        "partial_market_coverage_inputs": summary["partial_market_coverage_inputs"],
        "missing_required_feature_columns": summary["missing_required_feature_columns"],
        "next_required_action": next_action,
        "findings": findings,
        **SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_manifest",
        "monitor_id": root.name,
        "as_of": effective_as_of.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if report["signal_input_status"] != "BLOCKING" else "BLOCKING",
        "signal_input_status": report["signal_input_status"],
        "policy_path": str(policy_path),
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "signal_input_completeness_manifest_path": str(
            root / "signal_input_completeness_manifest.json"
        ),
        "signal_input_completeness_report_path": str(
            root / "signal_input_completeness_report.json"
        ),
        "signal_input_completeness_findings_path": str(
            root / "signal_input_completeness_findings.jsonl"
        ),
        "signal_input_completeness_markdown_path": str(
            root / "signal_input_completeness_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        "validation_path": str(root / "signal_input_completeness_validation.json"),
        **SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    reader = render_signal_input_completeness_reader_brief(report)
    st._write_json(root / "signal_input_completeness_manifest.json", manifest)
    st._write_json(root / "signal_input_completeness_report.json", report)
    st._write_jsonl(root / "signal_input_completeness_findings.jsonl", findings)
    st._write_text(
        root / "signal_input_completeness_report.md",
        render_signal_input_completeness_report(manifest, report),
    )
    st._write_text(root / "reader_brief_section.md", reader)
    st._write_latest_pointer(
        "latest_signal_input_completeness",
        root.name,
        root / "signal_input_completeness_manifest.json",
    )
    validation = validate_signal_input_completeness_artifact(
        monitor_id=root.name,
        output_dir=output_dir,
        policy_path=policy_path,
        write_output=True,
    )
    return {
        "monitor_id": root.name,
        "monitor_dir": root,
        "manifest": manifest,
        "signal_input_completeness_report": report,
        "signal_input_completeness_findings": findings,
        "reader_brief_section": reader,
        "signal_input_completeness_validation": validation,
    }


def signal_input_completeness_report_payload(
    *,
    monitor_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> dict[str, Any]:
    root = st._artifact_dir(
        artifact_id=monitor_id,
        latest_pointer="latest_signal_input_completeness",
        latest=latest,
        output_dir=output_dir,
        required_name="signal_input_completeness_manifest.json",
    )
    payload = {
        **st._read_json(root / "signal_input_completeness_manifest.json"),
        "signal_input_completeness_report": st._read_json(
            root / "signal_input_completeness_report.json"
        ),
        "signal_input_completeness_findings": st._read_jsonl(
            root / "signal_input_completeness_findings.jsonl"
        ),
        "reader_brief_section": (root / "reader_brief_section.md").read_text(
            encoding="utf-8"
        ),
        "monitor_dir": str(root),
    }
    validation = st._read_optional_json(root / "signal_input_completeness_validation.json")
    if validation:
        payload["signal_input_completeness_validation"] = validation
    return payload


def latest_signal_input_completeness_summary(
    *,
    monitor_id: str | None = None,
    report_path: Path | None = None,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> dict[str, Any]:
    if report_path is not None and report_path.exists():
        report = st._read_optional_json(report_path) or {}
        monitor_id = _text(report.get("monitor_id"))
        return _summary_from_report(report, report_path=report_path, exists=True)
    try:
        payload = signal_input_completeness_report_payload(
            monitor_id=monitor_id,
            latest=monitor_id is None,
            output_dir=output_dir,
        )
    except st.DynamicV3SystemTargetError:
        return _missing_signal_input_summary(report_path=report_path)
    report = _mapping(payload.get("signal_input_completeness_report"))
    return _summary_from_report(
        report,
        report_path=Path(_text(payload.get("signal_input_completeness_report_path"))),
        exists=True,
    )


def validate_signal_input_completeness_artifact(
    *,
    monitor_id: str,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    policy_path: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    write_output: bool = True,
) -> dict[str, Any]:
    root = output_dir / monitor_id
    manifest = st._read_optional_json(root / "signal_input_completeness_manifest.json") or {}
    report = st._read_optional_json(root / "signal_input_completeness_report.json") or {}
    findings = st._read_jsonl(root / "signal_input_completeness_findings.jsonl")
    reader = (
        (root / "reader_brief_section.md").read_text(encoding="utf-8")
        if (root / "reader_brief_section.md").exists()
        else ""
    )
    policy = load_signal_input_completeness_policy(policy_path) if policy_path.exists() else {}
    expected_inputs = set(_mapping(policy.get("required_inputs")))
    finding_inputs = {_text(row.get("input_id")) for row in findings}
    severity_order = set(_texts(policy.get("severity_order")))
    checks = st._required_file_checks(
        root,
        (
            "signal_input_completeness_manifest.json",
            "signal_input_completeness_report.json",
            "signal_input_completeness_findings.jsonl",
            "signal_input_completeness_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            st._check("monitor_id_matches", manifest.get("monitor_id") == monitor_id, ""),
            st._check(
                "policy_metadata_visible",
                bool(policy.get("policy_id")) and bool(policy.get("version")),
                "",
            ),
            st._check(
                "required_inputs_complete",
                expected_inputs.issubset(finding_inputs),
                ",".join(sorted(finding_inputs)),
            ),
            st._check(
                "severity_order_complete",
                set(SIGNAL_INPUT_STATUSES).issubset(severity_order),
                ",".join(sorted(severity_order)),
            ),
            st._check(
                "finding_severities_valid",
                all(row.get("severity") in SIGNAL_INPUT_STATUSES for row in findings),
                "",
            ),
            st._check(
                "status_consistent",
                report.get("signal_input_status") == _max_signal_severity(findings),
                "",
            ),
            st._check(
                "blocking_inputs_consistent",
                set(_texts(report.get("blocking_input_ids")))
                == {
                    _text(row.get("input_id"))
                    for row in findings
                    if row.get("severity") == "BLOCKING"
                },
                "",
            ),
            st._check(
                "warning_inputs_consistent",
                set(_texts(report.get("warning_input_ids")))
                == {
                    _text(row.get("input_id"))
                    for row in findings
                    if row.get("severity") == "WARNING"
                },
                "",
            ),
            st._check(
                "required_check_fields_visible",
                all(
                    key in row
                    for row in findings
                    for key in (
                        "input_id",
                        "input_type",
                        "severity",
                        "exists",
                        "row_count",
                        "latest_date",
                        "age_days",
                        "missing_required_columns",
                        "missing_coverage_values",
                        "incompatible_schema_versions",
                    )
                ),
                "",
            ),
            st._check(
                "next_action_visible",
                bool(_text(report.get("next_required_action"))),
                "",
            ),
            st._check(
                "reader_brief_quality_fields",
                "signal_input_status" in reader
                and "signal_input_blocking_count" in reader,
                "",
            ),
            st._check(
                "read_only_monitor",
                report.get("data_downloaded_by_monitor") is False
                and report.get("pipelines_executed_by_monitor") is False,
                "",
            ),
            st._check("broker_forbidden", st._payload_safe(manifest, report), ""),
        ]
    )
    validation = st._validation_payload(
        "etf_dynamic_v3_signal_input_completeness_validation",
        monitor_id,
        checks,
    )
    if write_output:
        st._write_json(root / "signal_input_completeness_validation.json", validation)
        st._write_text(
            root / "signal_input_completeness_validation.md",
            render_signal_input_completeness_validation_report(validation),
        )
    return validation


def render_signal_input_completeness_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Signal Input Completeness",
            "",
            f"- signal_input_completeness_id: {report.get('monitor_id')}",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- signal_input_blocking_count: {report.get('blocking_count')}",
            f"- signal_input_warning_count: {report.get('warning_count')}",
            f"- signal_input_missing_files: {_joined_texts(report.get('missing_signal_files'))}",
            f"- signal_input_stale_files: {_joined_texts(report.get('stale_signal_files'))}",
            "- signal_input_schema_issues: "
            f"{_joined_texts(report.get('incompatible_schema_inputs'))}",
            "- signal_input_partial_coverage: "
            f"{_joined_texts(report.get('partial_market_coverage_inputs'))}",
            "- signal_input_missing_required_columns: "
            f"{_joined_texts(report.get('missing_required_feature_columns'))}",
            f"- signal_input_next_action: {report.get('next_required_action')}",
            "- safety_boundary: read-only signal input check / no refresh / "
            "no official target / no broker / no production",
            "",
        ]
    )


def render_signal_input_completeness_report(
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    finding_lines = [
        (
            f"- {row.get('input_id')}: severity={row.get('severity')} "
            f"latest_date={row.get('latest_date')} age_days={row.get('age_days')} "
            f"missing_columns={','.join(_texts(row.get('missing_required_columns'))) or 'none'} "
            f"missing_coverage={','.join(_texts(row.get('missing_coverage_values'))) or 'none'}"
        )
        for row in _records(report.get("findings"))
    ]
    return "\n".join(
        [
            f"# Signal Input Completeness {manifest.get('monitor_id')}",
            "",
            "## Purpose",
            "Check required signal inputs before paper-shadow interpretation.",
            "",
            "## Summary",
            f"- signal_input_status: {report.get('signal_input_status')}",
            f"- blocking_count: {report.get('blocking_count')}",
            f"- warning_count: {report.get('warning_count')}",
            f"- missing_signal_files: {_joined_texts(report.get('missing_signal_files'))}",
            f"- stale_signal_files: {_joined_texts(report.get('stale_signal_files'))}",
            "- incompatible_schema_inputs: "
            f"{_joined_texts(report.get('incompatible_schema_inputs'))}",
            "- empty_signal_series_inputs: "
            f"{_joined_texts(report.get('empty_signal_series_inputs'))}",
            "- partial_market_coverage_inputs: "
            f"{_joined_texts(report.get('partial_market_coverage_inputs'))}",
            "- missing_required_feature_columns: "
            f"{_joined_texts(report.get('missing_required_feature_columns'))}",
            f"- next_required_action: {report.get('next_required_action')}",
            "",
            "## Findings",
            *finding_lines,
            "",
            "## Safety Boundary",
            "- read-only signal input completeness monitor",
            "- no data refresh or upstream rerun",
            "- no official target weights",
            "- no paper account mutation",
            "- no broker action or order ticket",
            "- no production mutation",
            "",
        ]
    )


def render_signal_input_completeness_validation_report(
    validation: Mapping[str, Any],
) -> str:
    checks = [
        f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}"
        for row in _records(validation.get("checks"))
    ]
    return "\n".join(
        [
            f"# Signal Input Completeness Validation {validation.get('artifact_id')}",
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


def load_signal_input_completeness_policy(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise st.DynamicV3SystemTargetError(f"signal input policy not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise st.DynamicV3SystemTargetError(f"signal input policy must be mapping: {path}")
    return dict(raw)


def _signal_input_findings(
    *,
    policy: Mapping[str, Any],
    as_of: date,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for input_id, raw_rule in _mapping(policy.get("required_inputs")).items():
        rule = _mapping(raw_rule)
        input_type = _text(rule.get("input_type"))
        if input_type == "csv_timeseries":
            findings.append(_csv_signal_input_finding(input_id=input_id, rule=rule, as_of=as_of))
        elif input_type == "json_report":
            findings.append(_json_signal_input_finding(input_id=input_id, rule=rule, as_of=as_of))
        else:
            findings.append(
                _base_finding(input_id=input_id, rule=rule, as_of=as_of)
                | {
                    "severity": "BLOCKING",
                    "issue_codes": ["unsupported_input_type"],
                    "stale_reason": "unsupported_input_type",
                }
            )
    return findings


def _csv_signal_input_finding(
    *,
    input_id: str,
    rule: Mapping[str, Any],
    as_of: date,
) -> dict[str, Any]:
    path = _resolve_policy_path(_text(rule.get("path")))
    finding = _base_finding(input_id=input_id, rule=rule, as_of=as_of, path=path)
    if not path.exists():
        return finding | _issue_result(
            "BLOCKING",
            ["missing_signal_file"],
            missing=True,
            stale_reason="required_file_missing",
        )
    rows, columns, read_error = _read_csv_rows(path)
    finding.update(
        {
            "exists": True,
            "size_bytes": path.stat().st_size,
            "checksum_sha256": _sha256(path),
            "row_count": len(rows),
            "column_count": len(columns),
            "columns": columns,
        }
    )
    if read_error:
        return finding | _issue_result(
            "BLOCKING",
            ["unreadable_signal_file"],
            stale_reason=read_error,
        )
    missing_columns = [
        column for column in _texts(rule.get("required_columns")) if column not in columns
    ]
    date_column = _text(rule.get("date_column"))
    coverage_column = _text(rule.get("coverage_column"))
    latest_date = _latest_row_date(rows, date_column) if date_column in columns else None
    latest_rows = [
        row
        for row in rows
        if latest_date is not None and _parse_date(row.get(date_column)) == latest_date
    ]
    missing_coverage = _missing_coverage_values(
        rows=latest_rows,
        coverage_column=coverage_column,
        required_values=_texts(rule.get("required_coverage_values")),
    )
    incompatible_schema = _incompatible_versions(
        rows=rows,
        version_column=_text(rule.get("schema_version_column")),
        allowed_versions=_texts(rule.get("allowed_schema_versions")),
    )
    incompatible_feature = _incompatible_versions(
        rows=rows,
        version_column=_text(rule.get("feature_version_column")),
        allowed_versions=_texts(rule.get("allowed_feature_versions")),
    )
    age_days = (as_of - latest_date).days if latest_date is not None else None
    stale_severity, stale_reason = _stale_severity(
        age_days=age_days,
        warning_days=_int(rule.get("stale_warning_days")),
        blocking_days=_int(rule.get("stale_blocking_days")),
    )
    issue_codes: list[str] = []
    severity = stale_severity
    if not rows:
        issue_codes.append("empty_signal_series")
        severity = "BLOCKING"
    if missing_columns:
        issue_codes.append("missing_required_feature_columns")
        severity = "BLOCKING"
    if incompatible_schema or incompatible_feature:
        issue_codes.append("incompatible_schema_version")
        severity = "BLOCKING"
    if missing_coverage:
        issue_codes.append("partial_market_coverage")
        if rule.get("required") is not False:
            severity = "BLOCKING"
        else:
            severity = _max_two_severities(severity, "WARNING")
    if stale_severity != "OK":
        issue_codes.append("stale_signal_file")
    finding.update(
        {
            "latest_date": latest_date.isoformat() if latest_date else "",
            "latest_row_count": len(latest_rows),
            "age_days": age_days,
            "missing_required_columns": missing_columns,
            "missing_coverage_values": missing_coverage,
            "incompatible_schema_versions": incompatible_schema,
            "incompatible_feature_versions": incompatible_feature,
            "observed_schema_versions": _observed_versions(
                rows,
                _text(rule.get("schema_version_column")),
            ),
            "observed_feature_versions": _observed_versions(
                rows,
                _text(rule.get("feature_version_column")),
            ),
            "stale_reason": stale_reason,
            "issue_codes": sorted(set(issue_codes)),
            "severity": severity,
            "missing": False,
        }
    )
    return finding


def _json_signal_input_finding(
    *,
    input_id: str,
    rule: Mapping[str, Any],
    as_of: date,
) -> dict[str, Any]:
    path = _resolve_json_report_path(rule)
    finding = _base_finding(input_id=input_id, rule=rule, as_of=as_of, path=path)
    if path is None or not path.exists():
        return finding | _issue_result(
            "BLOCKING",
            ["missing_signal_file"],
            missing=True,
            stale_reason="required_json_report_missing",
        )
    payload = st._read_optional_json(path) or {}
    latest_date = _parse_date(_json_path(payload, _text(rule.get("date_json_path"))))
    age_days = (as_of - latest_date).days if latest_date is not None else None
    stale_severity, stale_reason = _stale_severity(
        age_days=age_days,
        warning_days=_int(rule.get("stale_warning_days")),
        blocking_days=_int(rule.get("stale_blocking_days")),
    )
    missing_paths = [
        dotted
        for dotted in _texts(rule.get("required_json_paths"))
        if _json_path(payload, dotted) is None
    ]
    required_signals = _texts(rule.get("required_signal_keys"))
    signals = _mapping(payload.get("signals"))
    missing_signal_keys = [key for key in required_signals if key not in signals]
    report_type = _text(payload.get("report_type"))
    expected_report_type = _text(rule.get("report_type"))
    status = _text(_json_path(payload, _text(rule.get("status_json_path"))))
    issue_codes: list[str] = []
    severity = stale_severity
    if report_type != expected_report_type:
        issue_codes.append("incompatible_schema_version")
        severity = "BLOCKING"
    if missing_paths or missing_signal_keys:
        issue_codes.append("missing_required_feature_columns")
        severity = "BLOCKING"
    if status in set(_texts(rule.get("blocking_statuses"))):
        issue_codes.append("blocking_signal_snapshot_status")
        severity = "BLOCKING"
    elif status in set(_texts(rule.get("warning_statuses"))) and severity == "OK":
        issue_codes.append("warning_signal_snapshot_status")
        severity = "WARNING"
    if stale_severity != "OK":
        issue_codes.append("stale_signal_file")
    finding.update(
        {
            "exists": True,
            "source_path": str(path),
            "size_bytes": path.stat().st_size,
            "checksum_sha256": _sha256(path),
            "row_count": len(signals),
            "column_count": None,
            "latest_date": latest_date.isoformat() if latest_date else "",
            "latest_row_count": len(signals),
            "age_days": age_days,
            "report_type": report_type,
            "status": status,
            "missing_required_columns": missing_paths + missing_signal_keys,
            "missing_coverage_values": missing_signal_keys,
            "incompatible_schema_versions": (
                [] if report_type == expected_report_type else [report_type]
            ),
            "incompatible_feature_versions": [],
            "observed_schema_versions": [report_type] if report_type else [],
            "observed_feature_versions": [],
            "stale_reason": stale_reason,
            "issue_codes": sorted(set(issue_codes)),
            "severity": severity,
            "missing": False,
        }
    )
    return finding


def _base_finding(
    *,
    input_id: str,
    rule: Mapping[str, Any],
    as_of: date,
    path: Path | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "input_id": input_id,
        "input_label": rule.get("label") or input_id,
        "input_type": rule.get("input_type"),
        "required": rule.get("required") is not False,
        "requested_as_of": as_of.isoformat(),
        "source_path": "" if path is None else str(path),
        "exists": False,
        "missing": True,
        "size_bytes": 0,
        "checksum_sha256": "",
        "row_count": 0,
        "column_count": 0,
        "columns": [],
        "latest_date": "",
        "latest_row_count": 0,
        "age_days": None,
        "missing_required_columns": [],
        "missing_coverage_values": [],
        "incompatible_schema_versions": [],
        "incompatible_feature_versions": [],
        "observed_schema_versions": [],
        "observed_feature_versions": [],
        "stale_reason": "not_checked",
        "issue_codes": [],
        "severity": "OK",
    }


def _issue_result(
    severity: str,
    issue_codes: Sequence[str],
    *,
    missing: bool = False,
    stale_reason: str = "",
) -> dict[str, Any]:
    return {
        "severity": severity,
        "missing": missing,
        "issue_codes": list(issue_codes),
        "stale_reason": stale_reason,
    }


def _signal_input_summary(
    *,
    policy: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    status = _max_signal_severity(findings)
    return {
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "signal_input_status": status,
        "blocking_count": sum(1 for row in findings if row.get("severity") == "BLOCKING"),
        "warning_count": sum(1 for row in findings if row.get("severity") == "WARNING"),
        "ok_count": sum(1 for row in findings if row.get("severity") == "OK"),
        "blocking_input_ids": [
            _text(row.get("input_id")) for row in findings if row.get("severity") == "BLOCKING"
        ],
        "warning_input_ids": [
            _text(row.get("input_id")) for row in findings if row.get("severity") == "WARNING"
        ],
        "missing_signal_files": _input_ids_with_issue(findings, "missing_signal_file"),
        "stale_signal_files": _input_ids_with_issue(findings, "stale_signal_file"),
        "incompatible_schema_inputs": _input_ids_with_issue(
            findings, "incompatible_schema_version"
        ),
        "empty_signal_series_inputs": _input_ids_with_issue(findings, "empty_signal_series"),
        "partial_market_coverage_inputs": _input_ids_with_issue(
            findings, "partial_market_coverage"
        ),
        "missing_required_feature_columns": _input_ids_with_issue(
            findings, "missing_required_feature_columns"
        ),
    }


def _summary_from_report(
    report: Mapping[str, Any],
    *,
    report_path: Path | None,
    exists: bool,
) -> dict[str, Any]:
    return {
        "source_id": "signal_input_completeness",
        "exists": exists,
        "monitor_id": _text(report.get("monitor_id")),
        "signal_input_status": _text(report.get("signal_input_status"), "MISSING"),
        "severity": _text(report.get("severity") or report.get("signal_input_status"), "MISSING"),
        "blocking_count": _int(report.get("blocking_count")),
        "warning_count": _int(report.get("warning_count")),
        "blocking_input_ids": _texts(report.get("blocking_input_ids")),
        "warning_input_ids": _texts(report.get("warning_input_ids")),
        "missing_signal_files": _texts(report.get("missing_signal_files")),
        "stale_signal_files": _texts(report.get("stale_signal_files")),
        "incompatible_schema_inputs": _texts(report.get("incompatible_schema_inputs")),
        "empty_signal_series_inputs": _texts(report.get("empty_signal_series_inputs")),
        "partial_market_coverage_inputs": _texts(report.get("partial_market_coverage_inputs")),
        "missing_required_feature_columns": _texts(report.get("missing_required_feature_columns")),
        "next_required_action": _text(report.get("next_required_action")),
        "report_path": "" if report_path is None else str(report_path),
        "status": _text(report.get("status"), _text(report.get("signal_input_status"), "MISSING")),
        "generated_at": _text(report.get("generated_at")),
    }


def _missing_signal_input_summary(*, report_path: Path | None = None) -> dict[str, Any]:
    return {
        "source_id": "signal_input_completeness",
        "exists": False,
        "monitor_id": "",
        "signal_input_status": "MISSING",
        "severity": "BLOCKING",
        "blocking_count": 1,
        "warning_count": 0,
        "blocking_input_ids": ["signal_input_completeness_report"],
        "warning_input_ids": [],
        "missing_signal_files": ["signal_input_completeness_report"],
        "stale_signal_files": [],
        "incompatible_schema_inputs": [],
        "empty_signal_series_inputs": [],
        "partial_market_coverage_inputs": [],
        "missing_required_feature_columns": [],
        "next_required_action": "run_signal_input_completeness_monitor_before_paper_shadow",
        "report_path": "" if report_path is None else str(report_path),
        "status": "MISSING",
        "generated_at": "",
    }


def _max_signal_severity(findings: Sequence[Mapping[str, Any]]) -> str:
    order = {"OK": 0, "WARNING": 1, "BLOCKING": 2}
    if not findings:
        return "BLOCKING"
    return max(
        (str(row.get("severity") or "OK") for row in findings),
        key=lambda value: order.get(value, -1),
    )


def _max_two_severities(left: str, right: str) -> str:
    return _max_signal_severity([{"severity": left}, {"severity": right}])


def _input_ids_with_issue(
    findings: Sequence[Mapping[str, Any]],
    issue_code: str,
) -> list[str]:
    return sorted(
        {
            _text(row.get("input_id"))
            for row in findings
            if issue_code in set(_texts(row.get("issue_codes")))
        }
    )


def _resolve_policy_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else st.PROJECT_ROOT / path


def _resolve_json_report_path(rule: Mapping[str, Any]) -> Path | None:
    explicit = _text(rule.get("path"))
    if explicit:
        return _resolve_policy_path(explicit)
    glob_pattern = _text(rule.get("path_glob"))
    if not glob_pattern:
        return None
    candidates = [
        path
        for path in st.PROJECT_ROOT.glob(glob_pattern)
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _read_csv_rows(path: Path) -> tuple[list[dict[str, str]], list[str], str]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
            columns = list(reader.fieldnames or [])
    except OSError as exc:
        return [], [], f"csv_read_error:{exc}"
    except csv.Error as exc:
        return [], [], f"csv_parse_error:{exc}"
    return rows, columns, ""


def _latest_row_date(rows: Sequence[Mapping[str, Any]], date_column: str) -> date | None:
    dates = [_parse_date(row.get(date_column)) for row in rows]
    clean = [value for value in dates if value is not None]
    return max(clean) if clean else None


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


def _missing_coverage_values(
    *,
    rows: Sequence[Mapping[str, Any]],
    coverage_column: str,
    required_values: Sequence[str],
) -> list[str]:
    if not required_values:
        return []
    observed = {_text(row.get(coverage_column)) for row in rows}
    return [value for value in required_values if value not in observed]


def _observed_versions(
    rows: Sequence[Mapping[str, Any]],
    version_column: str,
) -> list[str]:
    if not version_column:
        return []
    return sorted(
        {_text(row.get(version_column)) for row in rows if _text(row.get(version_column))}
    )


def _incompatible_versions(
    *,
    rows: Sequence[Mapping[str, Any]],
    version_column: str,
    allowed_versions: Sequence[str],
) -> list[str]:
    if not version_column or not allowed_versions:
        return []
    allowed = set(allowed_versions)
    return [value for value in _observed_versions(rows, version_column) if value not in allowed]


def _stale_severity(
    *,
    age_days: int | None,
    warning_days: int,
    blocking_days: int,
) -> tuple[str, str]:
    if age_days is None:
        return "BLOCKING", "timestamp_missing"
    if age_days < 0:
        return "BLOCKING", "timestamp_after_requested_as_of"
    if age_days > blocking_days:
        return "BLOCKING", "older_than_blocking_policy_window"
    if age_days > warning_days:
        return "WARNING", "older_than_warning_policy_window"
    return "OK", "within_policy_window"


def _json_path(payload: Mapping[str, Any], dotted: str) -> Any:
    if not dotted:
        return None
    current: Any = payload
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
