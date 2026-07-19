# ruff: noqa: E501

from __future__ import annotations

import csv
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.etf_portfolio import dynamic_v3_signal_diagnosis_foundation as diagnosis
from ai_trading_system.etf_portfolio import dynamic_v3_system_target as st
from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_foundation as foundation
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.platform.artifacts.validation_session import with_artifact_validation_session

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
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
    "production_effect": "none",
}
COMPLETENESS_INPUT_SCHEMA = "signal_input_completeness_input_snapshot.v2"
COMPLETENESS_VIEWS = (
    "signal_input_completeness_manifest.json",
    "signal_input_completeness_report.json",
    "signal_input_completeness_findings.jsonl",
    "signal_input_completeness_report.md",
    "reader_brief_section.md",
)
COMPLETENESS_FILES = (*COMPLETENESS_VIEWS, "signal_input_completeness_input_snapshot.json")

_mapping = foundation._mapping
_records = foundation._records
_text = foundation._text
_texts = st._texts


class DynamicV3SignalInputCompletenessError(ValueError):
    """Raised when an as-of signal input view is not reproducible."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise DynamicV3SignalInputCompletenessError(message)


def _generated_time(value: datetime | None) -> datetime:
    generated = value or datetime.now(UTC)
    _require(generated.tzinfo is not None, "generated_at must be timezone-aware")
    return generated.astimezone(UTC)


def load_signal_input_completeness_policy(path: Path) -> dict[str, Any]:
    _require(path.is_file(), f"signal input policy not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    _require(isinstance(raw, Mapping), f"signal input policy must be mapping: {path}")
    policy = dict(raw)
    _require(bool(_text(policy.get("policy_id"))), "signal policy id missing")
    _require(bool(_text(policy.get("version"))), "signal policy version missing")
    _require(
        set(SIGNAL_INPUT_STATUSES).issubset(set(_texts(policy.get("severity_order")))),
        "signal policy severity order incomplete",
    )
    required = _mapping(policy.get("required_inputs"))
    _require(bool(required), "signal policy required_inputs missing")
    for input_id, raw_rule in required.items():
        rule = _mapping(raw_rule)
        _require(
            rule.get("input_type") in {"csv_timeseries", "json_report"},
            f"unsupported signal input type: {input_id}",
        )
        _require(bool(rule.get("path") or rule.get("path_glob")), f"source path missing: {input_id}")
        _require(_required_int(rule, "stale_warning_days") >= 0, f"warning days: {input_id}")
        _require(
            _required_int(rule, "stale_blocking_days")
            >= _required_int(rule, "stale_warning_days"),
            f"blocking days: {input_id}",
        )
    return policy


def _required_int(payload: Mapping[str, Any], field: str) -> int:
    value = payload.get(field)
    _require(isinstance(value, int) and not isinstance(value, bool), f"{field} must be integer")
    return value


def _material(
    *, root: Path, monitor_id: str, policy_path: Path, policy: Mapping[str, Any],
    findings: Sequence[Mapping[str, Any]], as_of: date, generated: datetime,
) -> tuple[dict[str, Any], dict[str, bytes]]:
    summary = _signal_input_summary(policy=policy, findings=findings)
    next_action = _mapping(policy.get("default_next_actions")).get(
        summary["signal_input_status"], "manual_signal_input_review_required"
    )
    report = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_report",
        "monitor_id": monitor_id,
        "as_of": as_of.isoformat(),
        "requested_as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "policy_path": str(policy_path.resolve()),
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        **summary,
        "severity": summary["signal_input_status"],
        "finding_count": len(findings),
        "next_required_action": next_action,
        "findings": [dict(row) for row in findings],
        **SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    status = report["signal_input_status"]
    manifest = {
        "schema_version": st.SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_signal_input_completeness_manifest",
        "monitor_id": monitor_id,
        "as_of": as_of.isoformat(),
        "requested_as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if status == "OK" else status,
        "signal_input_status": status,
        "policy_path": str(policy_path.resolve()),
        "policy_id": policy.get("policy_id"),
        "policy_version": _text(policy.get("version")),
        "signal_input_completeness_manifest_path": str(root / COMPLETENESS_VIEWS[0]),
        "signal_input_completeness_report_path": str(root / COMPLETENESS_VIEWS[1]),
        "signal_input_completeness_findings_path": str(root / COMPLETENESS_VIEWS[2]),
        "signal_input_completeness_markdown_path": str(root / COMPLETENESS_VIEWS[3]),
        "reader_brief_section_path": str(root / COMPLETENESS_VIEWS[4]),
        "signal_input_completeness_input_snapshot_path": str(
            root / "signal_input_completeness_input_snapshot.json"
        ),
        **SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    reader = render_signal_input_completeness_reader_brief(report)
    return manifest, {
        COMPLETENESS_VIEWS[0]: foundation._json_bytes(manifest),
        COMPLETENESS_VIEWS[1]: foundation._json_bytes(report),
        COMPLETENESS_VIEWS[2]: foundation._jsonl_bytes(findings),
        COMPLETENESS_VIEWS[3]: foundation._text_file_bytes(
            render_signal_input_completeness_report(manifest, report)
        ),
        COMPLETENESS_VIEWS[4]: foundation._text_file_bytes(reader),
    }


@with_artifact_validation_session
def run_signal_input_completeness_monitor(
    *, as_of: date | None = None,
    policy_path: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_POLICY_PATH,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = _generated_time(generated_at)
    effective_as_of = as_of or generated.date()
    policy = load_signal_input_completeness_policy(policy_path)
    findings = _signal_input_findings(policy=policy, as_of=effective_as_of)
    monitor_id = foundation._stable_id(
        "signal-input-completeness", effective_as_of.isoformat(),
        policy.get("policy_id"), policy.get("version"), generated.isoformat(),
    )
    root = foundation._unique_dir(output_dir / monitor_id)
    _, views = _material(
        root=root, monitor_id=root.name, policy_path=policy_path, policy=policy,
        findings=findings, as_of=effective_as_of, generated=generated,
    )
    root.mkdir(parents=True, exist_ok=False)
    _write_views(root, views)
    raw_sources = _freeze_raw_sources(root, findings)
    snapshot = {
        "schema_version": COMPLETENESS_INPUT_SCHEMA,
        "monitor_id": root.name,
        "generated_at": generated.isoformat(),
        "requested_as_of": effective_as_of.isoformat(),
        "policy_source": foundation._file_binding(policy_path),
        "raw_sources": raw_sources,
        "view_hashes": foundation._view_hashes(root, COMPLETENESS_VIEWS),
        **SIGNAL_INPUT_COMPLETENESS_SAFETY,
    }
    foundation._write_snapshot(root / "signal_input_completeness_input_snapshot.json", snapshot)
    foundation._write_latest_pointer(
        "latest_signal_input_completeness", root.name, root / COMPLETENESS_VIEWS[0]
    )
    validation = validate_signal_input_completeness_artifact(
        monitor_id=root.name, output_dir=output_dir, write_output=True
    )
    payload = signal_input_completeness_report_payload(monitor_id=root.name, output_dir=output_dir)
    return {
        **payload,
        "monitor_dir": root,
        "manifest": foundation._read_json(root / COMPLETENESS_VIEWS[0]),
        "signal_input_completeness_validation": validation,
    }


def signal_input_completeness_report_payload(
    *, monitor_id: str | None = None, latest: bool = False,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> dict[str, Any]:
    root = foundation._artifact_dir(
        artifact_id=monitor_id, latest_pointer="latest_signal_input_completeness",
        latest=latest, output_dir=output_dir, required_name=COMPLETENESS_VIEWS[0],
    )
    payload = {
        **foundation._read_json(root / COMPLETENESS_VIEWS[0]),
        "signal_input_completeness_report": foundation._read_json(root / COMPLETENESS_VIEWS[1]),
        "signal_input_completeness_findings": st._read_jsonl(root / COMPLETENESS_VIEWS[2]),
        "reader_brief_section": (root / COMPLETENESS_VIEWS[4]).read_text(encoding="utf-8"),
        "input_snapshot": foundation._read_json(root / "signal_input_completeness_input_snapshot.json"),
        "monitor_dir": str(root),
    }
    validation = st._read_optional_json(root / "signal_input_completeness_validation.json")
    if validation:
        payload["signal_input_completeness_validation"] = validation
    return payload


def _raw_source_bindings(findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "input_id": row.get("input_id"),
            "path": row.get("source_path"),
            "exists": row.get("exists") is True,
            "sha256": row.get("checksum_sha256"),
            "size_bytes": row.get("size_bytes"),
        }
        for row in findings
    ]


def _freeze_raw_sources(root: Path, findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    bindings = _raw_source_bindings(findings)
    frozen_root = root / "frozen_sources"
    for index, (binding, finding) in enumerate(zip(bindings, findings, strict=True)):
        if binding.get("exists") is not True:
            binding["frozen_path"] = None
            continue
        original = Path(_text(finding.get("source_path")))
        suffix = original.suffix or ".bin"
        frozen = frozen_root / f"{index:02d}_{_text(finding.get('input_id'))}{suffix}"
        write_bytes_atomic(frozen, original.read_bytes())
        frozen_binding = foundation._file_binding(frozen)
        _require(frozen_binding.get("sha256") == binding.get("sha256"), "frozen source hash drift")
        binding["frozen_path"] = frozen_binding.get("path")
    return bindings


def _validate_raw_source_bindings(bindings: Sequence[Mapping[str, Any]]) -> None:
    seen: set[str] = set()
    for binding in bindings:
        input_id = _text(binding.get("input_id"))
        _require(bool(input_id) and input_id not in seen, "duplicate raw source binding")
        seen.add(input_id)
        path = Path(_text(binding.get("frozen_path")))
        if binding.get("exists") is True:
            _require(path.is_file(), f"bound raw source missing: {input_id}")
            current = foundation._file_binding(path)
            _require(current.get("sha256") == binding.get("sha256"), f"raw source drift: {input_id}")
            _require(current.get("size_bytes") == binding.get("size_bytes"), f"raw size drift: {input_id}")


def _rebuild(root: Path, monitor_id: str) -> list[dict[str, Any]]:
    snapshot = foundation._read_json(root / "signal_input_completeness_input_snapshot.json")
    policy_source = _mapping(snapshot.get("policy_source"))
    foundation._validate_file_binding(policy_source)
    _validate_raw_source_bindings(_records(snapshot.get("raw_sources")))
    policy_path = Path(_text(policy_source.get("path")))
    policy = load_signal_input_completeness_policy(policy_path)
    as_of = date.fromisoformat(_text(snapshot.get("requested_as_of")))
    generated = _aware_datetime(snapshot.get("generated_at"), "snapshot.generated_at")
    bindings = _records(snapshot.get("raw_sources"))
    overrides = {
        _text(row.get("input_id")): (
            Path(_text(row.get("frozen_path"))) if row.get("exists") is True else None
        )
        for row in bindings
    }
    reported_paths = {
        _text(row.get("input_id")): Path(_text(row.get("path"))) for row in bindings
    }
    findings = _signal_input_findings(
        policy=policy, as_of=as_of, source_overrides=overrides,
        reported_paths=reported_paths,
    )
    _require(_raw_source_bindings(findings) == [
        {key: row.get(key) for key in ("input_id", "path", "exists", "sha256", "size_bytes")}
        for row in bindings
    ], "raw selection drift")
    _, expected = _material(
        root=root, monitor_id=monitor_id, policy_path=policy_path, policy=policy,
        findings=findings, as_of=as_of, generated=generated,
    )
    return diagnosis._check_bytes(root, expected)


@with_artifact_validation_session
def validate_signal_input_completeness_artifact(
    *, monitor_id: str, output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
    policy_path: Path | None = None, write_output: bool = True,
) -> dict[str, Any]:
    del policy_path
    root = output_dir / monitor_id
    checks, ok = diagnosis._snapshot_preflight(
        root=root, snapshot_name="signal_input_completeness_input_snapshot.json",
        schema=COMPLETENESS_INPUT_SCHEMA, id_key="monitor_id", artifact_id=monitor_id,
        view_names=COMPLETENESS_VIEWS,
    )
    validation = (
        diagnosis._validate_content(
            report_type="etf_dynamic_v3_signal_input_completeness_validation",
            artifact_id=monitor_id, checks=checks, rebuild=lambda: _rebuild(root, monitor_id),
        )
        if ok
        else st._validation_payload(
            "etf_dynamic_v3_signal_input_completeness_validation", monitor_id, checks
        )
    )
    if write_output:
        st._write_json(root / "signal_input_completeness_validation.json", validation)
        st._write_text(
            root / "signal_input_completeness_validation.md",
            render_signal_input_completeness_validation_report(validation),
        )
    return validation


def latest_signal_input_completeness_summary(
    *, monitor_id: str | None = None, report_path: Path | None = None,
    output_dir: Path = DEFAULT_SIGNAL_INPUT_COMPLETENESS_DIR,
) -> dict[str, Any]:
    try:
        if report_path is not None:
            root = report_path.resolve().parent
            monitor_id = root.name
            output_dir = root.parent
        payload = signal_input_completeness_report_payload(
            monitor_id=monitor_id, latest=monitor_id is None, output_dir=output_dir
        )
        resolved_id = _text(payload.get("monitor_id"))
        validation = validate_signal_input_completeness_artifact(
            monitor_id=resolved_id, output_dir=output_dir, write_output=False
        )
        if validation.get("status") != "PASS":
            return _missing_signal_input_summary(report_path=report_path, reason="VALIDATION_FAILED")
        return _summary_from_report(
            _mapping(payload.get("signal_input_completeness_report")),
            report_path=Path(_text(payload.get("signal_input_completeness_report_path"))),
            exists=True,
        )
    except (ValueError, OSError, json.JSONDecodeError, st.DynamicV3SystemTargetError):
        return _missing_signal_input_summary(report_path=report_path)


def _signal_input_findings(
    *, policy: Mapping[str, Any], as_of: date,
    source_overrides: Mapping[str, Path | None] | None = None,
    reported_paths: Mapping[str, Path] | None = None,
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for input_id, raw_rule in _mapping(policy.get("required_inputs")).items():
        rule = _mapping(raw_rule)
        override_present = source_overrides is not None and input_id in source_overrides
        override = source_overrides.get(input_id) if override_present and source_overrides else None
        reported = reported_paths.get(input_id) if reported_paths else None
        if rule.get("input_type") == "csv_timeseries":
            findings.append(
                _csv_finding(
                    input_id=input_id, rule=rule, as_of=as_of,
                    source_override=override, override_present=override_present,
                    reported_path=reported,
                )
            )
        else:
            findings.append(
                _json_finding(
                    input_id=input_id, rule=rule, as_of=as_of,
                    source_override=override, override_present=override_present,
                    reported_path=reported,
                )
            )
    return findings


def _base_finding(input_id: str, rule: Mapping[str, Any], as_of: date, path: Path | None) -> dict[str, Any]:
    return {
        "schema_version": st.SCHEMA_VERSION,
        "input_id": input_id,
        "input_label": rule.get("label") or input_id,
        "input_type": rule.get("input_type"),
        "required": rule.get("required") is not False,
        "requested_as_of": as_of.isoformat(),
        "source_path": str(path) if path is not None else str(_expected_path(rule)),
        "exists": False,
        "missing": True,
        "size_bytes": None,
        "checksum_sha256": None,
        "row_count": None,
        "eligible_row_count": None,
        "future_row_count": None,
        "column_count": None,
        "columns": [],
        "latest_date": None,
        "latest_row_count": None,
        "age_days": None,
        "missing_required_columns": [],
        "missing_coverage_values": [],
        "incompatible_schema_versions": [],
        "incompatible_feature_versions": [],
        "observed_schema_versions": [],
        "observed_feature_versions": [],
        "point_in_time_violation_count": 0,
        "stale_reason": "not_checked",
        "issue_codes": [],
        "severity": "OK",
    }


def _csv_finding(
    *, input_id: str, rule: Mapping[str, Any], as_of: date,
    source_override: Path | None = None, override_present: bool = False,
    reported_path: Path | None = None,
) -> dict[str, Any]:
    path = source_override if override_present else _expected_path(rule)
    display_path = reported_path or _expected_path(rule)
    finding = _base_finding(input_id, rule, as_of, display_path)
    if path is None or not path.is_file():
        return _blocked_missing(finding, "required_file_missing")
    rows, columns = _read_csv(path)
    date_column = _text(rule.get("date_column"))
    parsed = [(row, _parse_date(row.get(date_column))) for row in rows]
    eligible = [row for row, row_date in parsed if row_date is not None and row_date <= as_of]
    future = [row for row, row_date in parsed if row_date is not None and row_date > as_of]
    invalid_row_date_count = sum(1 for _, row_date in parsed if row_date is None)
    latest = _latest_row_date(eligible, date_column)
    latest_rows = [row for row in eligible if _parse_date(row.get(date_column)) == latest]
    missing_columns = [name for name in _texts(rule.get("required_columns")) if name not in columns]
    missing_coverage = _missing_coverage_values(
        latest_rows, _text(rule.get("coverage_column")), _texts(rule.get("required_coverage_values"))
    )
    incompatible_schema = _incompatible_versions(
        eligible, _text(rule.get("schema_version_column")),
        _texts(rule.get("allowed_schema_versions")),
    )
    incompatible_feature = _incompatible_versions(
        eligible, _text(rule.get("feature_version_column")),
        _texts(rule.get("allowed_feature_versions")),
    )
    pit_count = invalid_row_date_count + sum(
        1 for row in eligible if _row_pit_violation(row, date_column, as_of)
    )
    age_days = (as_of - latest).days if latest is not None else None
    severity, stale_reason = _stale_severity(
        age_days, _required_int(rule, "stale_warning_days"),
        _required_int(rule, "stale_blocking_days"),
    )
    issues: list[str] = []
    if not eligible:
        issues.append("empty_signal_series_as_of")
        severity = "BLOCKING"
    if missing_columns:
        issues.append("missing_required_feature_columns")
        severity = "BLOCKING"
    if missing_coverage:
        issues.append("partial_market_coverage")
        severity = "BLOCKING" if rule.get("required") is not False else _max_two(severity, "WARNING")
    if incompatible_schema or incompatible_feature:
        issues.append("incompatible_schema_version")
        severity = "BLOCKING"
    if pit_count:
        issues.append("point_in_time_violation")
        severity = "BLOCKING"
    if severity in {"WARNING", "BLOCKING"} and stale_reason not in {"within_policy_window", "not_checked"}:
        issues.append("stale_signal_file")
    finding.update({
        "exists": True, "missing": False, "source_path": str(display_path),
        "size_bytes": path.stat().st_size,
        "checksum_sha256": foundation._file_binding(path)["sha256"],
        "row_count": len(rows), "eligible_row_count": len(eligible),
        "future_row_count": len(future), "invalid_row_date_count": invalid_row_date_count,
        "column_count": len(columns), "columns": columns,
        "latest_date": latest.isoformat() if latest else None,
        "latest_row_count": len(latest_rows), "age_days": age_days,
        "missing_required_columns": missing_columns,
        "missing_coverage_values": missing_coverage,
        "incompatible_schema_versions": incompatible_schema,
        "incompatible_feature_versions": incompatible_feature,
        "observed_schema_versions": _observed_versions(eligible, _text(rule.get("schema_version_column"))),
        "observed_feature_versions": _observed_versions(eligible, _text(rule.get("feature_version_column"))),
        "point_in_time_violation_count": pit_count, "stale_reason": stale_reason,
        "issue_codes": sorted(set(issues)), "severity": severity,
    })
    return finding


def _json_finding(
    *, input_id: str, rule: Mapping[str, Any], as_of: date,
    source_override: Path | None = None, override_present: bool = False,
    reported_path: Path | None = None,
) -> dict[str, Any]:
    path = source_override if override_present else _resolve_json_report_path(rule, as_of)
    display_path = reported_path or path
    finding = _base_finding(input_id, rule, as_of, display_path)
    if path is None or not path.is_file():
        return _blocked_missing(finding, "required_json_report_missing_as_of")
    payload = foundation._read_json(path)
    latest = _parse_date(_json_path(payload, _text(rule.get("date_json_path"))))
    age_days = (as_of - latest).days if latest is not None else None
    severity, stale_reason = _stale_severity(
        age_days, _required_int(rule, "stale_warning_days"),
        _required_int(rule, "stale_blocking_days"),
    )
    missing_paths = [path_ for path_ in _texts(rule.get("required_json_paths")) if _json_path(payload, path_) is None]
    signals = _mapping(payload.get("signals"))
    missing_keys = [key for key in _texts(rule.get("required_signal_keys")) if key not in signals]
    report_type = _text(payload.get("report_type"))
    expected_type = _text(rule.get("report_type"))
    status = _text(_json_path(payload, _text(rule.get("status_json_path"))))
    issues: list[str] = []
    pit_count = 1 if latest is None or latest > as_of else 0
    if report_type != expected_type:
        issues.append("incompatible_schema_version")
        severity = "BLOCKING"
    if missing_paths or missing_keys:
        issues.append("missing_required_feature_columns")
        severity = "BLOCKING"
    if status in set(_texts(rule.get("blocking_statuses"))):
        issues.append("blocking_signal_snapshot_status")
        severity = "BLOCKING"
    elif status in set(_texts(rule.get("warning_statuses"))):
        issues.append("warning_signal_snapshot_status")
        severity = _max_two(severity, "WARNING")
    if pit_count:
        issues.append("point_in_time_violation")
        severity = "BLOCKING"
    if severity in {"WARNING", "BLOCKING"} and stale_reason not in {"within_policy_window", "not_checked"}:
        issues.append("stale_signal_file")
    finding.update({
        "exists": True, "missing": False, "source_path": str(display_path),
        "size_bytes": path.stat().st_size,
        "checksum_sha256": foundation._file_binding(path)["sha256"],
        "row_count": len(signals), "eligible_row_count": len(signals), "future_row_count": 0,
        "latest_date": latest.isoformat() if latest else None,
        "latest_row_count": len(signals), "age_days": age_days,
        "report_type": report_type, "status": status,
        "missing_required_columns": missing_paths + missing_keys,
        "missing_coverage_values": missing_keys,
        "incompatible_schema_versions": [] if report_type == expected_type else [report_type],
        "incompatible_feature_versions": [],
        "observed_schema_versions": [report_type] if report_type else [],
        "observed_feature_versions": [], "point_in_time_violation_count": pit_count,
        "stale_reason": stale_reason, "issue_codes": sorted(set(issues)), "severity": severity,
    })
    return finding


def _blocked_missing(finding: dict[str, Any], reason: str) -> dict[str, Any]:
    finding.update({"severity": "BLOCKING", "issue_codes": ["missing_signal_file"], "stale_reason": reason})
    return finding


def _row_pit_violation(row: Mapping[str, Any], date_column: str, as_of: date) -> bool:
    row_date = _parse_date(row.get(date_column))
    if row_date is None or row_date > as_of:
        return True
    created = _text(row.get("created_at"))
    if created:
        try:
            parsed = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except ValueError:
            return True
        if parsed.tzinfo is None or parsed.date() > as_of:
            return True
    source_date = _parse_date(row.get("source_date"))
    return source_date is not None and (source_date > as_of or source_date > row_date)


def _expected_path(rule: Mapping[str, Any]) -> Path:
    raw = _text(rule.get("path"))
    if not raw:
        return st.PROJECT_ROOT / _text(rule.get("path_glob"))
    path = Path(raw)
    return path if path.is_absolute() else st.PROJECT_ROOT / path


def _resolve_json_report_path(rule: Mapping[str, Any], as_of: date) -> Path | None:
    explicit = _text(rule.get("path"))
    if explicit:
        return _expected_path(rule)
    pattern = _text(rule.get("path_glob"))
    candidates = [path for path in st.PROJECT_ROOT.glob(pattern) if path.is_file()]
    dated: list[tuple[date, str, Path]] = []
    for path in candidates:
        try:
            payload = foundation._read_json(path)
        except (ValueError, json.JSONDecodeError):
            continue
        observed = _parse_date(_json_path(payload, _text(rule.get("date_json_path"))))
        if observed is not None and observed <= as_of:
            dated.append((observed, path.name, path))
    return max(dated)[2] if dated else None


def _read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            rows = [dict(row) for row in reader]
            columns = list(reader.fieldnames or [])
    except (OSError, csv.Error) as exc:
        raise DynamicV3SignalInputCompletenessError(f"csv read failed: {path}: {exc}") from exc
    return rows, columns


def _signal_input_summary(
    *, policy: Mapping[str, Any], findings: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    status = _max_signal_severity(findings)
    def issue(code: str) -> list[str]:
        return sorted(
            {
                _text(row.get("input_id"))
                for row in findings
                if code in set(_texts(row.get("issue_codes")))
            }
        )
    return {
        "policy_id": policy.get("policy_id"), "policy_version": _text(policy.get("version")),
        "signal_input_status": status,
        "blocking_count": sum(row.get("severity") == "BLOCKING" for row in findings),
        "warning_count": sum(row.get("severity") == "WARNING" for row in findings),
        "ok_count": sum(row.get("severity") == "OK" for row in findings),
        "blocking_input_ids": [_text(row.get("input_id")) for row in findings if row.get("severity") == "BLOCKING"],
        "warning_input_ids": [_text(row.get("input_id")) for row in findings if row.get("severity") == "WARNING"],
        "missing_signal_files": issue("missing_signal_file"),
        "stale_signal_files": issue("stale_signal_file"),
        "incompatible_schema_inputs": issue("incompatible_schema_version"),
        "empty_signal_series_inputs": issue("empty_signal_series_as_of"),
        "partial_market_coverage_inputs": issue("partial_market_coverage"),
        "missing_required_feature_columns": issue("missing_required_feature_columns"),
        "point_in_time_violation_inputs": issue("point_in_time_violation"),
    }


def _max_signal_severity(findings: Sequence[Mapping[str, Any]]) -> str:
    order = {"OK": 0, "WARNING": 1, "BLOCKING": 2}
    if not findings:
        return "BLOCKING"
    return max((_text(row.get("severity"), "BLOCKING") for row in findings), key=lambda value: order.get(value, 2))


def _max_two(left: str, right: str) -> str:
    return _max_signal_severity([{"severity": left}, {"severity": right}])


def _latest_row_date(rows: Sequence[Mapping[str, Any]], column: str) -> date | None:
    dates = [_parse_date(row.get(column)) for row in rows]
    clean = [value for value in dates if value is not None]
    return max(clean) if clean else None


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(_text(value)[:10]) if _text(value) else None
    except ValueError:
        return None


def _aware_datetime(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(_text(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DynamicV3SignalInputCompletenessError(f"invalid {field}") from exc
    _require(parsed.tzinfo is not None, f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _missing_coverage_values(
    rows: Sequence[Mapping[str, Any]], column: str, required: Sequence[str]
) -> list[str]:
    observed = {_text(row.get(column)) for row in rows}
    return [value for value in required if value not in observed]


def _observed_versions(rows: Sequence[Mapping[str, Any]], column: str) -> list[str]:
    if not column:
        return []
    return sorted({_text(row.get(column)) for row in rows if _text(row.get(column))})


def _incompatible_versions(
    rows: Sequence[Mapping[str, Any]], column: str, allowed: Sequence[str]
) -> list[str]:
    if not column or not allowed:
        return []
    return [value for value in _observed_versions(rows, column) if value not in set(allowed)]


def _stale_severity(age_days: int | None, warning_days: int, blocking_days: int) -> tuple[str, str]:
    if age_days is None:
        return "BLOCKING", "timestamp_missing_as_of"
    if age_days < 0:
        return "BLOCKING", "timestamp_after_requested_as_of"
    if age_days > blocking_days:
        return "BLOCKING", "older_than_blocking_policy_window"
    if age_days > warning_days:
        return "WARNING", "older_than_warning_policy_window"
    return "OK", "within_policy_window"


def _json_path(payload: Mapping[str, Any], dotted: str) -> Any:
    current: Any = payload
    for part in dotted.split(".") if dotted else []:
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current if dotted else None


def _write_views(root: Path, views: Mapping[str, bytes]) -> None:
    for name, payload in views.items():
        write_bytes_atomic(root / name, payload)


def _summary_from_report(
    report: Mapping[str, Any], *, report_path: Path | None, exists: bool
) -> dict[str, Any]:
    return {
        "source_id": "signal_input_completeness", "exists": exists,
        "monitor_id": _text(report.get("monitor_id")),
        "signal_input_status": _text(report.get("signal_input_status"), "MISSING"),
        "severity": _text(report.get("severity"), "MISSING"),
        "blocking_count": report.get("blocking_count"), "warning_count": report.get("warning_count"),
        "blocking_input_ids": _texts(report.get("blocking_input_ids")),
        "warning_input_ids": _texts(report.get("warning_input_ids")),
        "missing_signal_files": _texts(report.get("missing_signal_files")),
        "stale_signal_files": _texts(report.get("stale_signal_files")),
        "incompatible_schema_inputs": _texts(report.get("incompatible_schema_inputs")),
        "empty_signal_series_inputs": _texts(report.get("empty_signal_series_inputs")),
        "partial_market_coverage_inputs": _texts(report.get("partial_market_coverage_inputs")),
        "missing_required_feature_columns": _texts(report.get("missing_required_feature_columns")),
        "point_in_time_violation_inputs": _texts(report.get("point_in_time_violation_inputs")),
        "next_required_action": _text(report.get("next_required_action")),
        "report_path": str(report_path) if report_path else "",
        "status": _text(report.get("signal_input_status"), "MISSING"),
        "generated_at": _text(report.get("generated_at")),
    }


def _missing_signal_input_summary(
    *, report_path: Path | None = None, reason: str = "MISSING"
) -> dict[str, Any]:
    return {
        "source_id": "signal_input_completeness", "exists": False, "monitor_id": "",
        "signal_input_status": "MISSING", "severity": "BLOCKING",
        "blocking_count": None, "warning_count": None,
        "blocking_input_ids": ["signal_input_completeness_report"], "warning_input_ids": [],
        "missing_signal_files": ["signal_input_completeness_report"], "stale_signal_files": [],
        "incompatible_schema_inputs": [], "empty_signal_series_inputs": [],
        "partial_market_coverage_inputs": [], "missing_required_feature_columns": [],
        "point_in_time_violation_inputs": [],
        "next_required_action": "run_and_validate_signal_input_completeness_monitor",
        "report_path": str(report_path) if report_path else "", "status": reason,
        "generated_at": "",
    }


def render_signal_input_completeness_reader_brief(report: Mapping[str, Any]) -> str:
    return "\n".join([
        "## Signal Input Completeness", "",
        f"- signal_input_completeness_id: {report.get('monitor_id')}",
        f"- signal_input_status: {report.get('signal_input_status')}",
        f"- signal_input_blocking_count: {report.get('blocking_count')}",
        f"- signal_input_warning_count: {report.get('warning_count')}",
        f"- point_in_time_violations: {','.join(_texts(report.get('point_in_time_violation_inputs'))) or 'none'}",
        f"- signal_input_next_action: {report.get('next_required_action')}",
        "- safety: read-only as-of/PIT check / no refresh / no production", "",
    ])


def render_signal_input_completeness_report(
    manifest: Mapping[str, Any], report: Mapping[str, Any]
) -> str:
    rows = [
        f"- {row.get('input_id')}: severity={row.get('severity')} latest_date={row.get('latest_date')} "
        f"eligible_rows={row.get('eligible_row_count')} future_rows={row.get('future_row_count')}"
        for row in _records(report.get("findings"))
    ]
    return "\n".join([
        f"# Signal Input Completeness {manifest.get('monitor_id')}", "",
        f"- requested_as_of：{report.get('requested_as_of')}",
        f"- signal_input_status：{report.get('signal_input_status')}",
        f"- next_required_action：{report.get('next_required_action')}", "",
        "## Findings", *rows, "",
        "- CSV rows are truncated to row_date <= requested_as_of; JSON glob uses artifact as_of, not mtime.",
        "- safety：read-only / no upstream rerun / no broker / no production", "",
    ])


def render_signal_input_completeness_validation_report(validation: Mapping[str, Any]) -> str:
    rows = [f"- {row.get('check_id')}: passed={row.get('passed')} detail={row.get('detail')}" for row in _records(validation.get("checks"))]
    return "\n".join([
        f"# Signal Input Completeness Validation {validation.get('artifact_id')}", "",
        f"- status: {validation.get('status')}",
        f"- failed_check_count: {validation.get('failed_check_count')}",
        "- production_effect: none", "", "## Checks", *rows, "",
    ])
