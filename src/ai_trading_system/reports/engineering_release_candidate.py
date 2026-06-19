from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT

SCHEMA_VERSION = 1
REPORT_TYPE = "engineering_closeout_release_candidate"
VALIDATION_REPORT_TYPE = "engineering_closeout_release_candidate_validation"
PRODUCTION_EFFECT = "none"

READY_STATUS = "ENGINEERING_CLOSEOUT_READY"
READY_WITH_LIMITATIONS_STATUS = "ENGINEERING_CLOSEOUT_READY_WITH_DOCUMENTED_LIMITATIONS"
BLOCKED_STATUS = "ENGINEERING_CLOSEOUT_BLOCKED"

PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

DEFAULT_RELEASE_BLOCKING_TIERS = (
    "fast-unit",
    "contract-validation",
    "report-validation",
    "reproducibility",
)
STABLE_CLI_SURFACES = (
    "aits system status --as-of YYYY-MM-DD",
    "aits system doctor --as-of YYYY-MM-DD",
    "aits reports latest --report-id REPORT_ID --as-of YYYY-MM-DD",
    "aits reports index --as-of YYYY-MM-DD",
    "aits reports reader-brief-consistency --as-of YYYY-MM-DD",
    "aits reports engineering-stage-b-readiness --as-of YYYY-MM-DD",
    (
        "python scripts/run_validation_tier.py "
        "fast-unit|contract-validation|report-validation|reproducibility"
    ),
    "python scripts/run_clean_clone_release_acceptance.py --as-of YYYY-MM-DD",
)
STABLE_SCHEMA_SURFACES = (
    "report_registry_v1",
    "report_index_visibility_waivers_v1",
    "engineering_surface_inventory schema_version=1",
    "artifact_lifecycle_inventory schema_version=1",
    "engineering_stage_b_readiness schema_version=1",
    "clean_clone_release_acceptance schema_version=1",
    "engineering_closeout_release_candidate schema_version=1",
)


def default_engineering_closeout_release_candidate_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_closeout_release_candidate_{as_of.isoformat()}.json"


def default_engineering_closeout_release_candidate_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"engineering_closeout_release_candidate_{as_of.isoformat()}.md"


def default_engineering_closeout_release_candidate_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / (
        f"engineering_closeout_release_candidate_validation_{as_of.isoformat()}.json"
    )


def default_engineering_closeout_release_candidate_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / (
        f"engineering_closeout_release_candidate_validation_{as_of.isoformat()}.md"
    )


def latest_engineering_closeout_release_candidate_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "engineering_closeout_release_candidate_", ".json")


def build_engineering_closeout_release_candidate_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    reports_dir: Path | None = None,
    validation_runtime_dir: Path | None = None,
    release_tag: str | None = None,
    release_version: str | None = None,
    enforce_git_clean: bool = True,
) -> dict[str, Any]:
    resolved_reports_dir = reports_dir or project_root / "outputs" / "reports"
    resolved_runtime_dir = validation_runtime_dir or project_root / "outputs" / "validation_runtime"
    release_tag_name = release_tag or f"engineering-closeout-{as_of.isoformat()}-rc1"
    release_version_name = release_version or f"0.1.0+engineering.closeout.{as_of:%Y%m%d}.rc1"

    git_state = _git_state(project_root)
    source_artifacts = _source_artifacts(resolved_reports_dir)
    validation_tiers = _validation_tier_records(resolved_runtime_dir)
    release_metadata = {
        "release_version": release_version_name,
        "release_tag": release_tag_name,
        "git_commit": git_state["commit"],
        "git_worktree_clean": git_state["is_clean"],
        "git_tag_exists": release_tag_name in git_state["tags"],
        "supported_platforms": [
            "Windows PowerShell local development workspace",
            "Clean local clone with editable Python package install",
        ],
        "python_requires": ">=3.11",
        "current_python_major_minor": git_state["python_runtime"],
        "stable_cli_surfaces": list(STABLE_CLI_SURFACES),
        "stable_schema_surfaces": list(STABLE_SCHEMA_SURFACES),
        "compatibility_policy": {
            "stable_entrypoints": "Canonical CLI/status/report surfaces stay available.",
            "legacy_entrypoints": (
                "Task-specific commands remain compatibility entrypoints until a documented "
                "replacement and compatibility window exist."
            ),
            "schema_changes": (
                "Breaking schema/config changes require schema version bump, migration notes, "
                "contract tests, report registry/catalog update, and task-register entry."
            ),
            "post_freeze_change_admission": [
                "bug/security fix",
                "research-blocking infrastructure fix",
                "measurable complexity reduction",
            ],
            "post_freeze_rejected_change_types": [
                "new task-specific dashboard",
                "duplicate governance pack",
                "CLI that only wraps an old report without new workflow value",
                "abstraction without real use case",
            ],
        },
        "changelog": [
            "Canonical status/doctor and report latest entrypoints established.",
            "Artifact lifecycle, Stage B readiness, and Reader Brief consistency gates added.",
            "Report index legacy waivers migrated to registry visibility policy.",
            "Validation tiers formalized for fast, contract, report, and reproducibility gates.",
            "Clean-clone release acceptance runner added.",
            "Weight research turn RFC created for TRADING-500 to TRADING-504.",
        ],
    }
    checks = _completion_checks(
        source_artifacts=source_artifacts,
        validation_tiers=validation_tiers,
        release_metadata=release_metadata,
        enforce_git_clean=enforce_git_clean,
    )
    blocking = [
        check for check in checks if check["severity"] == "BLOCKING" and not check["passed"]
    ]
    warnings = [check for check in checks if check["severity"] == "WARNING" and not check["passed"]]
    status = _closeout_status(blocking_issues=blocking, warning_issues=warnings)

    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "closeout_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "market_regime": "ai_after_chatgpt",
        "requested_date_range": "not_applicable_engineering_closeout_release",
        "purpose": (
            "Audit TRADING-487_to_504 engineering closeout release candidate and "
            "platform-freeze readiness."
        ),
        "release_metadata": release_metadata,
        "source_artifacts": source_artifacts,
        "validation_tiers": validation_tiers,
        "completion_checks": checks,
        "blocking_issues": [_issue_from_check(check) for check in blocking],
        "warning_issues": [_issue_from_check(check) for check in warnings],
        "summary": {
            "completion_check_count": len(checks),
            "passed_check_count": len([check for check in checks if check["passed"]]),
            "blocking_issue_count": len(blocking),
            "warning_issue_count": len(warnings),
            "release_blocking_tier_count": len(DEFAULT_RELEASE_BLOCKING_TIERS),
            "release_blocking_tier_pass_count": len(
                [
                    tier
                    for tier in validation_tiers
                    if tier["tier"] in DEFAULT_RELEASE_BLOCKING_TIERS
                    and tier["status"] == PASS_STATUS
                ]
            ),
        },
        "reader_brief": {
            "summary": (
                f"Engineering closeout release candidate is {status}; "
                f"release_tag={release_tag_name}."
            ),
            "key_result": status,
            "blocking_issues": [check["check_id"] for check in blocking],
            "warnings": [check["check_id"] for check in warnings],
            "safety_boundary": (
                "read_only_platform_freeze_audit; production_effect=none; no data refresh, "
                "no strategy change, no paper-shadow/live activation, no official target "
                "weights, no broker/order, no production mutation."
            ),
            "next_action": (
                "tag_and_freeze_engineering_closeout"
                if status == READY_STATUS
                else "resolve_closeout_release_candidate_blockers"
            ),
        },
        "methodology": {
            "mode": "read_existing_release_evidence_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_strategy_logic": True,
            "does_not_generate_official_target_weights": True,
            "does_not_touch_broker_or_orders": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def validate_engineering_closeout_release_candidate_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    completion_checks = _records(payload.get("completion_checks"))
    blocking_completion_checks = [
        check
        for check in completion_checks
        if _text(check.get("severity")) == "BLOCKING" and not bool(check.get("passed"))
    ]
    warning_completion_checks = [
        check
        for check in completion_checks
        if _text(check.get("severity")) == "WARNING" and not bool(check.get("passed"))
    ]
    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message="payload must be engineering_closeout_release_candidate.",
        recommended_action="regenerate_engineering_closeout_release_candidate",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect_none",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="release candidate report must be read-only.",
        recommended_action="restore_production_effect_none",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="completion_checks_pass",
        passed=not blocking_completion_checks,
        severity="BLOCKING",
        message="all blocking engineering closeout completion checks must pass.",
        recommended_action="resolve_release_candidate_blockers_and_regenerate",
    )
    _append_check(
        checks,
        warning_issues,
        check_id="completion_warnings_documented",
        passed=not warning_completion_checks,
        severity="WARNING",
        message="release candidate has documented limitations.",
        recommended_action="review_or_resolve_documented_closeout_limitations",
    )
    validation_status = FAIL_STATUS if blocking_issues else PASS_STATUS
    if not blocking_issues and warning_issues:
        validation_status = WARN_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "production_effect": PRODUCTION_EFFECT,
        "input_artifacts": {},
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(blocking_issues),
            "warning_check_count": len(warning_issues),
            "completion_check_count": len(completion_checks),
            "blocking_completion_check_count": len(blocking_completion_checks),
            "warning_completion_check_count": len(warning_completion_checks),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": f"Engineering closeout release validation is {validation_status}.",
            "key_result": validation_status,
            "blocking_issues": [issue["issue_id"] for issue in blocking_issues],
            "warnings": [issue["issue_id"] for issue in warning_issues],
            "safety_boundary": "read_only_validation; production_effect=none",
            "next_action": (
                "freeze_engineering_closeout"
                if validation_status == PASS_STATUS
                else "fix_release_candidate_validation_blockers"
            ),
        },
    }


def write_engineering_closeout_release_candidate_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_engineering_closeout_release_candidate_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_engineering_closeout_release_candidate_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def write_engineering_closeout_release_candidate_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_engineering_closeout_release_candidate_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_engineering_closeout_release_candidate_validation_markdown(payload),
        encoding="utf-8",
    )
    return output_path


def render_engineering_closeout_release_candidate_markdown(payload: Mapping[str, Any]) -> str:
    metadata = _mapping(payload.get("release_metadata"))
    summary = _mapping(payload.get("summary"))
    reader_brief = _mapping(payload.get("reader_brief"))
    lines = [
        f"# Engineering Closeout Release Candidate {payload.get('as_of')}",
        "",
        "## Reader Brief",
        "",
        f"- Summary: {_text(reader_brief.get('summary'))}",
        f"- Key Result: {_text(reader_brief.get('key_result'))}",
        f"- Blocking Issues: {_list_cell(reader_brief.get('blocking_issues'))}",
        f"- Warnings: {_list_cell(reader_brief.get('warnings'))}",
        f"- Safety Boundary: {_text(reader_brief.get('safety_boundary'))}",
        f"- Next Action: {_text(reader_brief.get('next_action'))}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('closeout_status'))}",
        f"- release_version: {_text(metadata.get('release_version'))}",
        f"- release_tag: {_text(metadata.get('release_tag'))}",
        f"- git_commit: {_text(metadata.get('git_commit'))}",
        f"- git_worktree_clean: {bool(metadata.get('git_worktree_clean'))}",
        f"- git_tag_exists: {bool(metadata.get('git_tag_exists'))}",
        f"- production_effect: {_text(payload.get('production_effect'))}",
        (
            f"- completion_checks: {summary.get('passed_check_count')}/"
            f"{summary.get('completion_check_count')}"
        ),
        f"- blocking_issues: {summary.get('blocking_issue_count')}",
        f"- warnings: {summary.get('warning_issue_count')}",
        "",
        "## Completion Checks",
        "",
        "|check_id|status|severity|evidence|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("completion_checks")):
        status = "PASS" if check.get("passed") else "FAIL"
        lines.append(
            f"|{_cell(check.get('check_id'))}|{status}|{_cell(check.get('severity'))}|"
            f"{_cell(check.get('evidence'))}|"
        )
    lines.extend(["", "## Changelog", ""])
    for item in _strings(metadata.get("changelog")):
        lines.append(f"- {item}")
    lines.extend(["", "## Stable CLI", ""])
    for item in _strings(metadata.get("stable_cli_surfaces")):
        lines.append(f"- `{item}`")
    lines.extend(["", "## Stable Schemas", ""])
    for item in _strings(metadata.get("stable_schema_surfaces")):
        lines.append(f"- `{item}`")
    policy = _mapping(metadata.get("compatibility_policy"))
    lines.extend(
        [
            "",
            "## Compatibility Policy",
            "",
            f"- stable_entrypoints: {_text(policy.get('stable_entrypoints'))}",
            f"- legacy_entrypoints: {_text(policy.get('legacy_entrypoints'))}",
            f"- schema_changes: {_text(policy.get('schema_changes'))}",
            "",
            "## Safety Boundary",
            "",
            "This report is a read-only platform freeze audit. It does not refresh data, "
            "modify strategy logic, activate paper shadow, generate official target weights, "
            "touch broker/order systems, or mutate production state.",
            "",
        ]
    )
    return "\n".join(lines)


def render_engineering_closeout_release_candidate_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Engineering Closeout Release Candidate Validation {payload.get('as_of')}",
        "",
        "## Summary",
        "",
        f"- status: {_text(payload.get('validation_status'))}",
        f"- checks: {summary.get('check_count')}",
        f"- failed: {summary.get('failed_check_count')}",
        f"- warnings: {summary.get('warning_check_count')}",
        f"- production_effect: {_text(payload.get('production_effect'))}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_cell(check.get('check_id'))}|{_cell(check.get('status'))}|"
            f"{_cell(check.get('severity'))}|{_cell(check.get('message'))}|"
        )
    lines.append("")
    return "\n".join(lines)


def _completion_checks(
    *,
    source_artifacts: Sequence[Mapping[str, Any]],
    validation_tiers: Sequence[Mapping[str, Any]],
    release_metadata: Mapping[str, Any],
    enforce_git_clean: bool,
) -> list[dict[str, Any]]:
    artifacts = {_text(record.get("artifact_id")): record for record in source_artifacts}
    tiers = {_text(record.get("tier")): record for record in validation_tiers}
    return [
        _check(
            "clear_user_entrypoints",
            _artifact_status(artifacts, "canonical_system_status")
            == "ENGINEERING_CONTROL_PLANE_READY",
            "BLOCKING",
            "canonical status is first-screen ready",
        ),
        _check(
            "standard_workflow_not_task_id_dependent",
            _artifact_exists(artifacts, "canonical_system_status")
            and _artifact_exists(artifacts, "report_index"),
            "BLOCKING",
            "canonical status/report index/latest surfaces are present",
        ),
        _check(
            "single_canonical_status_source",
            _artifact_status(artifacts, "canonical_system_doctor") == PASS_STATUS,
            "BLOCKING",
            "system doctor is PASS",
        ),
        _check(
            "artifact_traceability_and_reproducibility",
            _artifact_status(artifacts, "engineering_stage_b_readiness")
            == "ENGINEERING_STAGE_B_READY"
            and _tier_status(tiers, "reproducibility") == PASS_STATUS,
            "BLOCKING",
            "Stage B readiness and reproducibility tier are PASS",
        ),
        _check(
            "cli_schema_config_compatibility_policy",
            bool(release_metadata.get("stable_cli_surfaces"))
            and bool(release_metadata.get("stable_schema_surfaces"))
            and bool(release_metadata.get("compatibility_policy")),
            "BLOCKING",
            "stable CLI/schema surfaces and compatibility policy are recorded",
        ),
        _check(
            "reader_brief_structure_consistent",
            _artifact_status(artifacts, "reader_brief_consistency_validation") == PASS_STATUS,
            "BLOCKING",
            "Reader Brief consistency validation is PASS",
        ),
        _check(
            "release_blocking_tests_pass",
            all(
                _tier_status(tiers, tier) == PASS_STATUS
                for tier in DEFAULT_RELEASE_BLOCKING_TIERS
            ),
            "BLOCKING",
            "release-blocking validation tiers are PASS",
        ),
        _check(
            "clean_clone_minimal_e2e_pass",
            _artifact_status(artifacts, "clean_clone_release_acceptance")
            == "CLEAN_CLONE_ACCEPTANCE_PASS",
            "BLOCKING",
            "clean clone release acceptance is PASS",
        ),
        _check(
            "non_research_warnings_orphans_expired_waivers_zero",
            _artifact_status(artifacts, "report_index") == PASS_STATUS
            and _artifact_status(artifacts, "task_register_consistency_validation") == PASS_STATUS,
            "BLOCKING",
            "report index and task register consistency are PASS",
        ),
        _check(
            "platform_versioned_and_freeze_policy_recorded",
            bool(release_metadata.get("release_version"))
            and bool(release_metadata.get("release_tag"))
            and bool(release_metadata.get("changelog")),
            "BLOCKING",
            "release version, release tag, changelog, and freeze policy are recorded",
        ),
        _check(
            "git_worktree_clean",
            bool(release_metadata.get("git_worktree_clean")) or not enforce_git_clean,
            "BLOCKING",
            "git worktree is clean for release candidate audit",
        ),
    ]


def _source_artifacts(reports_dir: Path) -> list[dict[str, Any]]:
    specs = (
        ("canonical_system_status", "canonical_system_status_"),
        ("canonical_system_doctor", "canonical_system_doctor_"),
        ("report_index", "report_index_"),
        ("engineering_stage_b_readiness", "engineering_stage_b_readiness_"),
        (
            "engineering_stage_b_readiness_validation",
            "engineering_stage_b_readiness_validation_",
        ),
        ("reader_brief_consistency_validation", "reader_brief_consistency_validation_"),
        ("task_register_consistency_validation", "task_register_consistency_validation_"),
        ("clean_clone_release_acceptance", "clean_clone_release_acceptance_"),
    )
    records: list[dict[str, Any]] = []
    for artifact_id, prefix in specs:
        path = _latest_dated_path(reports_dir, prefix, ".json")
        payload = _read_json(path)
        records.append(
            {
                "artifact_id": artifact_id,
                "path": "" if path is None else str(path),
                "exists": path is not None,
                "status": _payload_status(payload),
                "production_effect": _text(payload.get("production_effect")),
                "as_of": _text(payload.get("as_of")),
            }
        )
    return records


def _validation_tier_records(validation_runtime_dir: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for tier in DEFAULT_RELEASE_BLOCKING_TIERS:
        path = _latest_validation_tier_summary(validation_runtime_dir, tier)
        payload = _read_json(path)
        records.append(
            {
                "tier": tier,
                "path": "" if path is None else str(path),
                "exists": path is not None,
                "status": _payload_status(payload),
                "resolved_tier": _text(payload.get("resolved_tier")),
                "promotion_blocking": bool(payload.get("promotion_blocking")),
                "production_effect": _text(payload.get("production_effect")),
            }
        )
    return records


def _git_state(project_root: Path) -> dict[str, Any]:
    commit = _run_git(project_root, "rev-parse", "HEAD")
    status = _run_git(project_root, "status", "--porcelain")
    tags = _run_git(project_root, "tag", "--points-at", "HEAD").splitlines()
    return {
        "commit": commit,
        "is_clean": status == "",
        "dirty_file_count": len([line for line in status.splitlines() if line.strip()]),
        "tags": tags,
        "python_runtime": ">=3.11",
    }


def _run_git(project_root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=project_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def _check(check_id: str, passed: bool, severity: str, evidence: str) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "severity": severity,
        "evidence": evidence,
        "recommended_action": _recommended_action(check_id, passed),
    }


def _recommended_action(check_id: str, passed: bool) -> str:
    if passed:
        return "none"
    return {
        "clean_clone_minimal_e2e_pass": "run_clean_clone_release_acceptance_on_clean_worktree",
        "git_worktree_clean": "commit_or_stash_closeout_changes_before_release_candidate",
        "release_blocking_tests_pass": "rerun_release_blocking_validation_tiers",
    }.get(check_id, "inspect_source_artifact_and_fix_closeout_blocker")


def _issue_from_check(check: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": _text(check.get("check_id")),
        "severity": _text(check.get("severity")),
        "message": _text(check.get("evidence")),
        "recommended_action": _text(check.get("recommended_action")),
    }


def _closeout_status(
    *,
    blocking_issues: Sequence[Mapping[str, Any]],
    warning_issues: Sequence[Mapping[str, Any]],
) -> str:
    if blocking_issues:
        return BLOCKED_STATUS
    if warning_issues:
        return READY_WITH_LIMITATIONS_STATUS
    return READY_STATUS


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
) -> None:
    status = "PASS" if passed else "FAIL"
    check = {
        "check_id": check_id,
        "status": status,
        "passed": passed,
        "severity": severity,
        "message": message,
        "recommended_action": "none" if passed else recommended_action,
    }
    checks.append(check)
    if not passed:
        issues.append(
            {
                "issue_id": check_id,
                "severity": severity,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _artifact_exists(artifacts: Mapping[str, Mapping[str, Any]], artifact_id: str) -> bool:
    return bool(artifacts.get(artifact_id, {}).get("exists"))


def _artifact_status(artifacts: Mapping[str, Mapping[str, Any]], artifact_id: str) -> str:
    return _text(artifacts.get(artifact_id, {}).get("status"))


def _tier_status(tiers: Mapping[str, Mapping[str, Any]], tier: str) -> str:
    return _text(tiers.get(tier, {}).get("status"))


def _payload_status(payload: Mapping[str, Any]) -> str:
    for key in (
        "closeout_status",
        "release_acceptance_status",
        "validation_status",
        "readiness_status",
        "status",
    ):
        value = _text(payload.get(key))
        if value:
            return value
    return "MISSING" if not payload else "AVAILABLE"


def _latest_validation_tier_summary(root: Path, tier: str) -> Path | None:
    candidates = [
        path
        for path in root.glob(f"{tier}_*/test_runtime_summary.json")
        if path.is_file()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: (path.stat().st_mtime, path.name))


def _latest_dated_path(root: Path, prefix: str, suffix: str) -> Path | None:
    candidates: list[tuple[date, float, Path]] = []
    for path in root.glob(f"{prefix}*{suffix}"):
        if not path.is_file():
            continue
        if not re.match(r"\d{4}-\d{2}-\d{2}", path.name.removeprefix(prefix)):
            continue
        artifact_date = _date_from_path(path)
        candidates.append((artifact_date or date.min, path.stat().st_mtime, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1], item[2].name))[2]


def _date_from_path(path: Path) -> date | None:
    match = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    if match is None:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _records(value: object) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _strings(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_text(item) for item in value if _text(item)]


def _text(value: object) -> str:
    return "" if value is None else str(value)


def _cell(value: object) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _list_cell(value: object) -> str:
    if isinstance(value, list):
        return "; ".join(_text(item) for item in value) or "none"
    return _text(value) or "none"
