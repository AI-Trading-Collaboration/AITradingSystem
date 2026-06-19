from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
REPORT_TYPE = "engineering_stage_b_readiness"
VALIDATION_REPORT_TYPE = "engineering_stage_b_readiness_validation"
PRODUCTION_EFFECT = "none"

DEFAULT_ENGINEERING_CLOSEOUT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "engineering_closeout_policy.yaml"
)
DEFAULT_CONFIG_CONTRACT_REGISTRY_PATH = PROJECT_ROOT / "config" / "config_contract_registry.yaml"

READY_STATUS = "ENGINEERING_STAGE_B_READY"
READY_WITH_LIMITATIONS_STATUS = "ENGINEERING_STAGE_B_READY_WITH_LIMITATIONS"
BLOCKED_STATUS = "ENGINEERING_STAGE_B_BLOCKED"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

SAFE_PRODUCTION_EFFECTS = frozenset({"", "none", "read_only", "advisory"})
DEFAULT_REQUIRED_ARTIFACT_FIELDS = (
    "schema_version",
    "report_type",
    "status",
    "production_effect",
)
DEFAULT_REQUIRED_RUN_MANIFEST_FIELDS = (
    "git_commit",
    "command",
    "resolved_config",
    "input_artifacts",
    "input_checksums",
    "schema_versions",
    "as_of",
    "random_seed",
    "environment_summary",
    "output_artifacts",
    "elapsed_seconds",
    "warnings",
)


def default_engineering_stage_b_readiness_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"engineering_stage_b_readiness_{as_of.isoformat()}.json"


def default_engineering_stage_b_readiness_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"engineering_stage_b_readiness_{as_of.isoformat()}.md"


def default_engineering_stage_b_readiness_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_stage_b_readiness_validation_{as_of.isoformat()}.json"


def default_engineering_stage_b_readiness_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_stage_b_readiness_validation_{as_of.isoformat()}.md"


def latest_engineering_stage_b_readiness_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "engineering_stage_b_readiness_", ".json")


def build_engineering_stage_b_readiness_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_ENGINEERING_CLOSEOUT_POLICY_PATH,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Path | None = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    policy = _load_policy(policy_path)
    stage_b = _mapping(policy.get("stage_b"))
    report_index = build_report_index_payload(
        as_of=as_of,
        project_root=project_root,
        registry_path=registry_path,
        waiver_path=waiver_path,
    )
    latest_artifact_records = _latest_artifact_records(
        report_index,
        project_root=project_root,
        required_fields=_strings(
            stage_b.get("required_artifact_fields"),
            default=list(DEFAULT_REQUIRED_ARTIFACT_FIELDS),
        ),
    )
    config_contract_registry_path = _project_path(
        stage_b.get("config_contract_registry_path"),
        project_root=project_root,
        default=project_root / "config" / DEFAULT_CONFIG_CONTRACT_REGISTRY_PATH.name,
    )
    registered_config_contracts = _config_contract_registry_paths(
        config_contract_registry_path,
        project_root=project_root,
    )
    config_records = _config_records(
        project_root / "config",
        registered_contract_paths=registered_config_contracts,
    )
    run_manifest_records = _run_manifest_records(
        project_root,
        required_fields=_strings(
            stage_b.get("required_run_manifest_fields"),
            default=list(DEFAULT_REQUIRED_RUN_MANIFEST_FIELDS),
        ),
        policy_effective_date=_policy_effective_date(policy),
    )
    validation_tier_summary = _validation_tier_summary(
        project_root=project_root,
        required_tiers=_strings(stage_b.get("required_validation_tiers")),
    )
    error_taxonomy_summary = _error_taxonomy_summary(
        project_root=project_root,
        required_categories=_strings(stage_b.get("required_error_categories")),
    )
    reports_boundary_summary = _reports_boundary_summary(project_root)
    summary = _summary(
        report_index=report_index,
        latest_artifact_records=latest_artifact_records,
        config_records=config_records,
        run_manifest_records=run_manifest_records,
        validation_tier_summary=validation_tier_summary,
        error_taxonomy_summary=error_taxonomy_summary,
        reports_boundary_summary=reports_boundary_summary,
    )
    blocking_issues = _blocking_issues(
        report_index=report_index,
        policy=policy,
        validation_tier_summary=validation_tier_summary,
    )
    warning_issues = _warning_issues(
        summary=summary,
        validation_tier_summary=validation_tier_summary,
        error_taxonomy_summary=error_taxonomy_summary,
        reports_boundary_summary=reports_boundary_summary,
    )
    status = _readiness_status(
        blocking_issues=blocking_issues,
        warning_issues=warning_issues,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "readiness_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_engineering_stage_b_readiness",
        "purpose": (
            "Audit Stage B engineering-closeout readiness for schema/config, "
            "reproducibility manifests, validation tiers, reporting boundaries, "
            "and structured error taxonomy."
        ),
        "input_artifacts": {
            "engineering_closeout_policy": str(policy_path),
            "config_contract_registry": str(config_contract_registry_path),
            "report_registry": str(registry_path),
            "report_index_visibility_waivers": "" if waiver_path is None else str(waiver_path),
            "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        },
        "policy": {
            "policy_id": _text(policy.get("policy_id")),
            "policy_version": _text(policy.get("policy_version")),
            "policy_metadata": _mapping(policy.get("policy_metadata")),
            "stage_b": stage_b,
        },
        "summary": summary,
        "latest_artifact_schema_records": latest_artifact_records,
        "config_schema_records": config_records,
        "run_manifest_records": run_manifest_records,
        "validation_tier_summary": validation_tier_summary,
        "error_taxonomy_summary": error_taxonomy_summary,
        "reports_module_boundary_summary": reports_boundary_summary,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": (
                f"Stage B readiness is {status}; "
                f"schema missing={summary['latest_json_schema_version_missing_count']}; "
                f"missing validation tiers={summary['missing_validation_tier_count']}."
            ),
            "key_result": status,
            "blocking_issues": "; ".join(issue["issue_id"] for issue in blocking_issues)
            or "none",
            "warnings": "; ".join(issue["issue_id"] for issue in warning_issues) or "none",
            "next_action": _next_action(status),
        },
        "safety_boundary": _safety_boundary(),
        "methodology": {
            "mode": "read_existing_engineering_contracts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_logic": True,
            "does_not_modify_research_decisions": True,
            "does_not_activate_paper_shadow": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_engineering_stage_b_readiness_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message="payload must be engineering_stage_b_readiness.",
        recommended_action="regenerate_engineering_stage_b_readiness",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="Stage B readiness report must be read-only.",
        recommended_action="restore_production_effect_none",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="report_index_unwaived_warnings_zero",
        passed=int(summary.get("report_index_unwaived_issue_count") or 0) == 0,
        severity="BLOCKING",
        message="Stage B readiness cannot proceed with unwaived report-index issues.",
        recommended_action="refresh_missing_or_stale_artifacts_or_add_reviewed_waiver",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="expired_waivers_zero",
        passed=int(summary.get("report_index_expired_waiver_count") or 0) == 0,
        severity="BLOCKING",
        message="Stage B readiness cannot depend on expired waivers.",
        recommended_action="renew_or_remove_expired_report_index_waivers",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="unsafe_production_effect_zero",
        passed=int(summary.get("latest_artifact_production_effect_risk_count") or 0) == 0,
        severity="BLOCKING",
        message="Latest artifacts must not have unsafe production effects.",
        recommended_action="investigate_latest_artifact_production_effect_risks",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="validation_tier_runner_available",
        passed=bool(_mapping(payload.get("validation_tier_summary")).get("runner_exists")),
        severity="BLOCKING",
        message="Validation tier runner must exist before Stage B closeout.",
        recommended_action="restore_scripts_run_validation_tier",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="schema_version_coverage_complete",
        passed=int(summary.get("latest_json_schema_version_missing_count") or 0) == 0,
        severity="WARNING",
        message="Some latest JSON artifacts still lack schema_version.",
        recommended_action="migrate_core_artifacts_to_explicit_schema_version",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="config_policy_metadata_complete",
        passed=int(summary.get("config_without_schema_or_policy_metadata_count") or 0) == 0,
        severity="WARNING",
        message="Some config files lack schema_version, policy_metadata, or registry coverage.",
        recommended_action="consolidate_config_schema_and_policy_metadata",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="run_manifest_required_fields_complete",
        passed=int(summary.get("run_manifest_missing_required_field_count") or 0) == 0,
        severity="WARNING",
        message="Run manifests do not yet expose all Stage B reproducibility fields.",
        recommended_action="extend_run_manifest_contract_before_release_acceptance",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="required_validation_tiers_complete",
        passed=int(summary.get("missing_validation_tier_count") or 0) == 0,
        severity="WARNING",
        message="Validation tier taxonomy is missing one or more Stage B required tiers.",
        recommended_action="add_or_map_missing_validation_tiers",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="error_taxonomy_centralized",
        passed=bool(_mapping(payload.get("error_taxonomy_summary")).get("central_module_exists")),
        severity="WARNING",
        message="Structured error taxonomy is not yet centralized in source.",
        recommended_action="add_central_error_taxonomy_and_adopt_in_cli_boundaries",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="reports_module_boundary_within_budget",
        passed=bool(_mapping(payload.get("reports_module_boundary_summary")).get("within_budget")),
        severity="WARNING",
        message="reports.py remains above Stage B refactor budget.",
        recommended_action="split_cli_adapters_from_report_builders_incrementally",
    )
    blocking_issues = _dedupe_issues(blocking_issues)
    warning_issues = _dedupe_issues(warning_issues)
    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "validation_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "source_readiness_status": _text(
            payload.get("readiness_status"),
            _text(payload.get("status")),
        ),
        "input_artifacts": {
            "engineering_stage_b_readiness": _text(
                _mapping(payload.get("input_artifacts")).get("engineering_stage_b_readiness")
            )
        },
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check.get("status") == FAIL_STATUS]
            ),
            "warning_check_count": len(
                [check for check in checks if check.get("status") == WARN_STATUS]
            ),
            "blocking_issue_count": len(blocking_issues),
            "warning_issue_count": len(warning_issues),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Engineering Stage B readiness validation is {status}.",
            "key_result": status,
            "blocking_issues": "; ".join(issue["issue_id"] for issue in blocking_issues)
            or "none",
            "warnings": "; ".join(issue["issue_id"] for issue in warning_issues) or "none",
            "next_action": (
                "resolve_stage_b_validation_blockers"
                if blocking_issues
                else "work_stage_b_warning_backlog_before_platform_freeze"
                if warning_issues
                else "stage_b_ready_for_release_candidate_review"
            ),
        },
        "safety_boundary": _safety_boundary(),
    }


def write_engineering_stage_b_readiness_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_engineering_stage_b_readiness_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_engineering_stage_b_readiness_markdown(payload), encoding="utf-8")
    return output_path


def write_engineering_stage_b_readiness_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_engineering_stage_b_readiness_json(payload, output_path)


def write_engineering_stage_b_readiness_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_engineering_stage_b_readiness_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_engineering_stage_b_readiness_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Engineering Stage B Readiness {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('readiness_status'), _text(payload.get('status')))}",
        f"- latest_json_artifact_count: {summary.get('latest_json_artifact_count')}",
        f"- schema_version_missing: {summary.get('latest_json_schema_version_missing_count')}",
        f"- config_file_count: {summary.get('config_file_count')}",
        f"- config_contract_registry_present_count: "
        f"{summary.get('config_contract_registry_present_count')}",
        f"- config_without_schema_or_policy_metadata_count: "
        f"{summary.get('config_without_schema_or_policy_metadata_count')}",
        f"- run_manifest_count: {summary.get('run_manifest_count')}",
        f"- current_policy_run_manifest_count: "
        f"{summary.get('current_policy_run_manifest_count')}",
        f"- legacy_pre_policy_run_manifest_count: "
        f"{summary.get('legacy_pre_policy_run_manifest_count')}",
        f"- missing_validation_tier_count: {summary.get('missing_validation_tier_count')}",
        f"- missing_error_category_count: {summary.get('missing_error_category_count')}",
        "",
        "## Validation Tiers",
        "",
        "|tier|status|",
        "|---|---|",
    ]
    tier_summary = _mapping(payload.get("validation_tier_summary"))
    available = set(_strings(tier_summary.get("available_tiers")))
    for tier in _strings(tier_summary.get("required_tiers")):
        lines.append(f"|{_md_cell(tier)}|{'AVAILABLE' if tier in available else 'MISSING'}|")
    lines.extend(
        [
            "",
            "## Warning Issues",
            "",
        ]
    )
    warnings = _records(payload.get("warning_issues"))
    if not warnings:
        lines.append("- none")
    else:
        lines.extend(f"- {_text(issue.get('issue_id'))}" for issue in warnings)
    lines.extend(["", "## Safety Boundary", ""])
    for key, value in sorted(_mapping(payload.get("safety_boundary")).items()):
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def render_engineering_stage_b_readiness_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    lines = [
        f"# Engineering Stage B Readiness Validation {payload.get('as_of')}",
        "",
        f"- status: {_text(payload.get('validation_status'), _text(payload.get('status')))}",
        f"- production_effect: {_text(payload.get('production_effect'))}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|recommended_action|",
        "|---|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_md_cell(check.get('check_id'))}|"
            f"{_md_cell(check.get('status'))}|"
            f"{_md_cell(check.get('severity'))}|"
            f"{_md_cell(check.get('message'))}|"
            f"{_md_cell(check.get('recommended_action'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _load_policy(policy_path: Path) -> dict[str, Any]:
    if not policy_path.exists():
        return {}
    raw = safe_load_yaml_path(policy_path)
    return dict(raw) if isinstance(raw, Mapping) else {}


def _latest_artifact_records(
    report_index: Mapping[str, Any],
    *,
    project_root: Path,
    required_fields: Sequence[str],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for report in _records(report_index.get("reports")):
        path = _report_path(report, project_root)
        payload = _read_json_payload(path)
        missing_fields = [
            field
            for field in required_fields
            if field not in payload or _text(payload.get(field)) == ""
        ]
        records.append(
            {
                "report_id": _text(report.get("report_id")),
                "latest_artifact_path": "" if path is None else str(path),
                "latest_artifact_name": _text(report.get("latest_artifact_name")),
                "latest_artifact_exists": bool(report.get("exists")),
                "latest_artifact_is_json": path is not None and path.suffix.lower() == ".json",
                "json_payload_parseable": bool(payload),
                "schema_version": _text(payload.get("schema_version")),
                "report_type": _text(payload.get("report_type")),
                "status": _text(payload.get("status"), _text(payload.get("validation_status"))),
                "production_effect": _text(
                    payload.get("production_effect"),
                    _text(report.get("artifact_production_effect")),
                ),
                "artifact_production_effect_risk": bool(
                    report.get("artifact_production_effect_risk")
                ),
                "missing_required_fields": missing_fields,
                "missing_required_field_count": len(missing_fields),
            }
        )
    return records


def _config_records(
    config_root: Path,
    *,
    registered_contract_paths: set[Path],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(list(config_root.rglob("*.yaml")) + list(config_root.rglob("*.json"))):
        payload = _read_config_payload(path)
        resolved_path = _resolve_path(path)
        records.append(
            {
                "path": str(path),
                "schema_version_present": "schema_version" in payload,
                "policy_metadata_present": "policy_metadata" in payload,
                "contract_registry_present": resolved_path in registered_contract_paths,
                "profile_hint": "profile" in path.name.lower()
                or "profiles" in {str(key).lower() for key in payload.keys()},
            }
        )
    return records


def _config_contract_registry_paths(
    registry_path: Path,
    *,
    project_root: Path,
) -> set[Path]:
    payload = _read_config_payload(registry_path)
    paths: set[Path] = set()
    for record in _records(payload.get("config_contracts")):
        raw_path = _text(record.get("path"))
        if raw_path:
            paths.add(_resolve_path(_project_path(raw_path, project_root=project_root)))
    return paths


def _policy_effective_date(policy: Mapping[str, Any]) -> date | None:
    return _parse_date_prefix(_text(policy.get("policy_version")))


def _manifest_generated_date(payload: Mapping[str, Any]) -> date | None:
    for key in (
        "generated_at",
        "started_at_utc",
        "started_at",
        "as_of",
        "execution_timestamp_utc",
    ):
        parsed = _parse_date_prefix(_text(payload.get(key)))
        if parsed is not None:
            return parsed
    return None


def _parse_date_prefix(value: str) -> date | None:
    if len(value) < 10:
        return None
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _run_manifest_records(
    project_root: Path,
    *,
    required_fields: Sequence[str],
    policy_effective_date: date | None,
) -> list[dict[str, Any]]:
    patterns = (
        "outputs/runs/daily/**/manifest.json",
        "outputs/validation_runtime/**/test_runtime_summary.json",
        "run/**/run_manifest.json",
    )
    records: list[dict[str, Any]] = []
    seen: set[Path] = set()
    for pattern in patterns:
        for path in sorted(project_root.glob(pattern)):
            if not path.is_file() or path in seen:
                continue
            seen.add(path)
            payload = _read_json_payload(path)
            missing_fields = [
                field
                for field in required_fields
                if field not in payload or payload.get(field) in (None, "")
            ]
            generated_date = _manifest_generated_date(payload)
            legacy_pre_policy = (
                policy_effective_date is not None
                and generated_date is not None
                and generated_date < policy_effective_date
            )
            records.append(
                {
                    "path": str(path),
                    "manifest_type": path.name,
                    "parseable": bool(payload),
                    "status": _text(payload.get("status")),
                    "manifest_generated_date": (
                        "" if generated_date is None else generated_date.isoformat()
                    ),
                    "contract_scope": (
                        "legacy_pre_policy" if legacy_pre_policy else "current_policy"
                    ),
                    "missing_required_fields": [] if legacy_pre_policy else missing_fields,
                    "missing_required_field_count": 0 if legacy_pre_policy else len(missing_fields),
                    "legacy_missing_required_fields": missing_fields if legacy_pre_policy else [],
                    "legacy_missing_required_field_count": (
                        len(missing_fields) if legacy_pre_policy else 0
                    ),
                }
            )
    return records


def _validation_tier_summary(project_root: Path, required_tiers: Sequence[str]) -> dict[str, Any]:
    runner_path = project_root / "scripts" / "run_validation_tier.py"
    text = _read_text(runner_path)
    available_tiers = sorted(set(re.findall(r'"([^"]+)": TierSpec\(', text)))
    missing_tiers = [tier for tier in required_tiers if tier not in available_tiers]
    runtime_summaries = list(
        (project_root / "outputs" / "validation_runtime").glob("**/test_runtime_summary.json")
    )
    return {
        "runner_path": str(runner_path),
        "runner_exists": runner_path.exists(),
        "required_tiers": list(required_tiers),
        "available_tiers": available_tiers,
        "missing_tiers": missing_tiers,
        "runtime_summary_count": len(runtime_summaries),
        "production_effect": PRODUCTION_EFFECT,
    }


def _error_taxonomy_summary(
    project_root: Path,
    required_categories: Sequence[str],
) -> dict[str, Any]:
    searchable_paths = list((project_root / "src" / "ai_trading_system").rglob("*.py"))
    searchable_paths.extend((project_root / "scripts").glob("*.py"))
    category_counts: Counter[str] = Counter()
    for path in searchable_paths:
        text = _read_text(path)
        for category in required_categories:
            if category in text:
                category_counts[category] += 1
    missing_categories = [
        category for category in required_categories if category_counts[category] == 0
    ]
    central_module = project_root / "src" / "ai_trading_system" / "error_taxonomy.py"
    return {
        "required_categories": list(required_categories),
        "category_source_counts": dict(category_counts),
        "missing_categories": missing_categories,
        "central_module_exists": central_module.exists(),
        "central_module_path": str(central_module),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reports_boundary_summary(project_root: Path) -> dict[str, Any]:
    reports_cli = project_root / "src" / "ai_trading_system" / "cli_commands" / "reports.py"
    text = _read_text(reports_cli)
    line_count = len(text.splitlines()) if text else 0
    command_count = text.count("@reports_app.command")
    budget_line_count = 12000
    return {
        "reports_cli_path": str(reports_cli),
        "line_count": line_count,
        "command_count": command_count,
        "budget_line_count": budget_line_count,
        "within_budget": line_count <= budget_line_count,
        "recommended_boundary": [
            "domain models",
            "artifact resolvers",
            "report builders",
            "validators",
            "Reader Brief renderers",
            "CLI adapters",
            "registry definitions",
        ],
        "production_effect": PRODUCTION_EFFECT,
    }


def _read_config_payload(path: Path) -> Mapping[str, Any]:
    try:
        if path.suffix.lower() == ".json":
            return _read_json_payload(path)
        raw = safe_load_yaml_path(path)
    except Exception:
        return {}
    return raw if isinstance(raw, Mapping) else {}


def _project_path(
    value: Any,
    *,
    project_root: Path,
    default: Path | None = None,
) -> Path:
    if value in (None, ""):
        return project_root if default is None else default
    path = Path(str(value))
    return path if path.is_absolute() else project_root / path


def _resolve_path(path: Path) -> Path:
    try:
        return path.resolve()
    except OSError:
        return path.absolute()


def _read_json_payload(path: Path | None) -> Mapping[str, Any]:
    if path is None or not path.exists() or path.suffix.lower() != ".json":
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _report_path(record: Mapping[str, Any], project_root: Path) -> Path | None:
    raw = _text(record.get("latest_artifact_path"))
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else project_root / path


def _readiness_status(
    *,
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> str:
    if blocking_issues:
        return BLOCKED_STATUS
    if warning_issues:
        return READY_WITH_LIMITATIONS_STATUS
    return READY_STATUS


def _summary(
    *,
    report_index: Mapping[str, Any],
    latest_artifact_records: Sequence[Mapping[str, Any]],
    config_records: Sequence[Mapping[str, Any]],
    run_manifest_records: Sequence[Mapping[str, Any]],
    validation_tier_summary: Mapping[str, Any],
    error_taxonomy_summary: Mapping[str, Any],
    reports_boundary_summary: Mapping[str, Any],
) -> dict[str, Any]:
    report_summary = _mapping(report_index.get("summary"))
    visibility = _mapping(report_index.get("visibility_audit"))
    latest_json_records = [
        record for record in latest_artifact_records if bool(record.get("latest_artifact_is_json"))
    ]
    return {
        "report_index_status": _text(report_index.get("status"), "UNKNOWN"),
        "report_index_raw_unwaived_issue_count": len(
            _strings(visibility.get("unwaived_issue_ids"))
        ),
        "report_index_unwaived_issue_count": len(_effective_unwaived_issue_ids(visibility)),
        "report_index_expired_waiver_count": int(report_summary.get("expired_waiver_count") or 0),
        "latest_artifact_count": len(latest_artifact_records),
        "latest_json_artifact_count": len(latest_json_records),
        "latest_json_schema_version_missing_count": len(
            [record for record in latest_json_records if not _text(record.get("schema_version"))]
        ),
        "latest_json_required_field_missing_count": sum(
            int(record.get("missing_required_field_count") or 0)
            for record in latest_json_records
        ),
        "latest_artifact_production_effect_risk_count": len(
            [
                record
                for record in latest_artifact_records
                if bool(record.get("artifact_production_effect_risk"))
                or _text(record.get("production_effect")) not in SAFE_PRODUCTION_EFFECTS
            ]
        ),
        "config_file_count": len(config_records),
        "config_schema_version_present_count": len(
            [record for record in config_records if bool(record.get("schema_version_present"))]
        ),
        "config_policy_metadata_present_count": len(
            [record for record in config_records if bool(record.get("policy_metadata_present"))]
        ),
        "config_contract_registry_present_count": len(
            [record for record in config_records if bool(record.get("contract_registry_present"))]
        ),
        "config_without_schema_or_policy_metadata_count": len(
            [
                record
                for record in config_records
                if not bool(record.get("schema_version_present"))
                and not bool(record.get("policy_metadata_present"))
                and not bool(record.get("contract_registry_present"))
            ]
        ),
        "run_manifest_count": len(run_manifest_records),
        "current_policy_run_manifest_count": len(
            [
                record
                for record in run_manifest_records
                if _text(record.get("contract_scope")) == "current_policy"
            ]
        ),
        "legacy_pre_policy_run_manifest_count": len(
            [
                record
                for record in run_manifest_records
                if _text(record.get("contract_scope")) == "legacy_pre_policy"
            ]
        ),
        "run_manifest_missing_required_field_count": sum(
            int(record.get("missing_required_field_count") or 0)
            for record in run_manifest_records
        ),
        "legacy_run_manifest_missing_required_field_count": sum(
            int(record.get("legacy_missing_required_field_count") or 0)
            for record in run_manifest_records
        ),
        "missing_validation_tier_count": len(
            _strings(validation_tier_summary.get("missing_tiers"))
        ),
        "validation_runtime_summary_count": int(
            validation_tier_summary.get("runtime_summary_count") or 0
        ),
        "missing_error_category_count": len(
            _strings(error_taxonomy_summary.get("missing_categories"))
        ),
        "error_taxonomy_central_module_exists": bool(
            error_taxonomy_summary.get("central_module_exists")
        ),
        "reports_cli_line_count": int(reports_boundary_summary.get("line_count") or 0),
        "reports_cli_command_count": int(reports_boundary_summary.get("command_count") or 0),
        "reports_cli_within_budget": bool(reports_boundary_summary.get("within_budget")),
        "production_effect": PRODUCTION_EFFECT,
    }


def _blocking_issues(
    *,
    report_index: Mapping[str, Any],
    policy: Mapping[str, Any],
    validation_tier_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    report_summary = _mapping(report_index.get("summary"))
    visibility = _mapping(report_index.get("visibility_audit"))
    unwaived = _effective_unwaived_issue_ids(visibility)
    if unwaived:
        issues.append(
            {
                "issue_id": "report_index_unwaived_warnings",
                "severity": "BLOCKING",
                "details": unwaived,
            }
        )
    if int(report_summary.get("expired_waiver_count") or 0):
        issues.append({"issue_id": "report_index_expired_waivers", "severity": "BLOCKING"})
    if int(report_summary.get("production_effect_risk_count") or 0):
        issues.append(
            {"issue_id": "latest_artifact_production_effect_risk", "severity": "BLOCKING"}
        )
    if not policy:
        issues.append({"issue_id": "engineering_closeout_policy_missing", "severity": "BLOCKING"})
    if not bool(validation_tier_summary.get("runner_exists")):
        issues.append({"issue_id": "validation_tier_runner_missing", "severity": "BLOCKING"})
    return issues


def _warning_issues(
    *,
    summary: Mapping[str, Any],
    validation_tier_summary: Mapping[str, Any],
    error_taxonomy_summary: Mapping[str, Any],
    reports_boundary_summary: Mapping[str, Any],
) -> list[dict[str, Any]]:
    candidates = (
        ("schema_version_missing_latest_artifacts", "latest_json_schema_version_missing_count"),
        (
            "config_without_schema_or_policy_metadata",
            "config_without_schema_or_policy_metadata_count",
        ),
        ("run_manifest_missing_required_fields", "run_manifest_missing_required_field_count"),
        ("validation_tier_reproducibility_missing", "missing_validation_tier_count"),
        ("error_taxonomy_categories_missing", "missing_error_category_count"),
    )
    issues: list[dict[str, Any]] = []
    for issue_id, count_key in candidates:
        count = int(summary.get(count_key) or 0)
        if count:
            issues.append({"issue_id": issue_id, "severity": "WARNING", "count": count})
    if not bool(error_taxonomy_summary.get("central_module_exists")):
        issues.append({"issue_id": "error_taxonomy_not_centralized", "severity": "WARNING"})
    if not bool(reports_boundary_summary.get("within_budget")):
        issues.append(
            {
                "issue_id": "reports_cli_growth_requires_refactor",
                "severity": "WARNING",
                "line_count": reports_boundary_summary.get("line_count"),
            }
        )
    missing_tiers = _strings(validation_tier_summary.get("missing_tiers"))
    if missing_tiers:
        issues.append(
            {
                "issue_id": "missing_required_validation_tiers",
                "severity": "WARNING",
                "details": missing_tiers,
            }
        )
    return _dedupe_issues(issues)


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_engineering_contracts_only",
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_modify_strategy_logic": True,
        "does_not_modify_research_decisions": True,
        "does_not_activate_paper_shadow": True,
        "does_not_generate_official_target_weights": True,
        "does_not_touch_broker_or_orders": True,
        "does_not_modify_production": True,
        "production_effect": PRODUCTION_EFFECT,
    }


def _effective_unwaived_issue_ids(visibility: Mapping[str, Any]) -> list[str]:
    self_generated_prefixes = (
        "engineering_stage_b_readiness_",
        "engineering_stage_b_readiness_validation_",
    )
    return [
        issue_id
        for issue_id in _strings(visibility.get("unwaived_issue_ids"))
        if not issue_id.startswith(self_generated_prefixes)
    ]


def _next_action(status: str) -> str:
    if status == BLOCKED_STATUS:
        return "resolve_stage_b_readiness_blockers_before_platform_freeze"
    if status == READY_WITH_LIMITATIONS_STATUS:
        return "work_stage_b_warning_backlog_before_platform_freeze"
    return "proceed_to_clean_clone_release_acceptance"


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    status = PASS_STATUS if passed else WARN_STATUS if severity == "WARNING" else FAIL_STATUS
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
            "details": dict(details or {}),
        }
    )
    if passed:
        return
    issues.append(
        {
            "issue_id": check_id,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
            "details": dict(details or {}),
        }
    )


def _dedupe_issues(issues: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for issue in issues:
        issue_id = _text(issue.get("issue_id"))
        if issue_id in seen:
            continue
        seen.add(issue_id)
        result.append(dict(issue))
    return result


def _latest_dated_path(directory: Path, prefix: str, suffix: str) -> Path | None:
    paths = [path for path in directory.glob(f"{prefix}*{suffix}") if path.is_file()]
    if not paths:
        return None
    return max(paths, key=lambda path: (path.stat().st_mtime, path.name))


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[Mapping[str, Any]]:
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _strings(value: Any, *, default: Sequence[str] = ()) -> list[str]:
    if value is None:
        value = list(default)
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _md_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", "<br/>")
