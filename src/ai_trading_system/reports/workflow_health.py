from __future__ import annotations

import hashlib
import json
import re
import subprocess
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_json_atomic, write_text_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

REPORT_SCHEMA_VERSION = "workflow_health_report.v1"
CANDIDATE_SCHEMA_VERSION = "workflow_optimization_candidates.v1"
VALIDATION_SCHEMA_VERSION = "workflow_health_validation.v1"
CYCLE_RECEIPT_SCHEMA_VERSION = "workflow_health_cycle_receipt.v1"
REPORT_TYPE = "workflow_health"
CANDIDATE_REPORT_TYPE = "workflow_optimization_candidates"
VALIDATION_REPORT_TYPE = "workflow_health_validation"
POLICY_SCHEMA_VERSION = "workflow_health_policy.v1"
PRODUCTION_EFFECT = "none"
BROKER_ACTION = "none"
DATA_QUALITY_STATUS = "NOT_APPLICABLE_DEVELOPER_TELEMETRY"
DEFAULT_POLICY_PATH = PROJECT_ROOT / "config" / "architecture" / "workflow_health_policy.yaml"
DEFAULT_REPORTS_DIR = PROJECT_ROOT / "outputs" / "reports"
_FAILED_NODE_RE = re.compile(r"(?:FAILED|ERROR)\s+([^\s]+?\.py(?:::[^\s]+)*)")
_RUN_ID_TIMESTAMP_RE = re.compile(r"(\d{8}T\d{6}Z)")
_DATE_TOKEN_RE = re.compile(r"(20\d{6})")
_WORKFLOW_HEALTH_REPORT_RE = re.compile(r"workflow_health_(\d{4}-\d{2}-\d{2})\.json$")


def default_workflow_health_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_health_{as_of.isoformat()}.json"


def default_workflow_health_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_health_{as_of.isoformat()}.md"


def default_workflow_candidates_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_optimization_candidates_{as_of.isoformat()}.json"


def default_workflow_health_validation_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_health_validation_{as_of.isoformat()}.json"


def default_workflow_health_validation_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_health_validation_{as_of.isoformat()}.md"


def default_workflow_health_cycle_receipt_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"workflow_health_cycle_receipt_{as_of.isoformat()}.json"


def latest_workflow_health_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "workflow_health_", ".json")


def latest_workflow_candidates_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "workflow_optimization_candidates_", ".json")


def load_workflow_health_policy(path: Path = DEFAULT_POLICY_PATH) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise ValueError(f"workflow health policy must be a mapping: {path}")
    policy = dict(raw)
    if policy.get("schema_version") != POLICY_SCHEMA_VERSION:
        raise ValueError(f"workflow health policy schema must be {POLICY_SCHEMA_VERSION}: {path}")
    metadata = policy.get("policy_metadata")
    required_metadata = {
        "owner",
        "status",
        "rationale",
        "intended_effect",
        "validation_evidence",
        "review_condition",
    }
    if not isinstance(metadata, Mapping) or not required_metadata.issubset(metadata):
        raise ValueError(f"workflow health policy metadata is incomplete: {path}")
    cadence = policy.get("cadence")
    if not isinstance(cadence, Mapping) or int(cadence.get("lookback_days", 0)) <= 0:
        raise ValueError(f"workflow health policy lookback_days must be positive: {path}")
    automatic = cadence.get("automatic_report_generation")
    expected_automatic = {
        "enabled": True,
        "existing_automation_id": "aitradingsystem-pit",
        "deduplication_basis": "ISO_WEEK_VALIDATED_BUNDLE",
        "failure_retry": "NEXT_EXISTING_DAILY_AUTOMATION_INVOCATION",
    }
    if not isinstance(automatic, Mapping) or any(
        automatic.get(key) != value for key, value in expected_automatic.items()
    ):
        raise ValueError(f"workflow health automatic report policy is incomplete: {path}")
    if cadence.get("automatic_command_dispatch_enabled") is not True:
        raise ValueError(f"workflow health automatic report generation is disabled: {path}")
    owner_decision_id = str(automatic.get("owner_decision_id", ""))
    if not owner_decision_id.startswith("owner_decision:DEVX-012:"):
        raise ValueError(f"workflow health automatic report owner decision is invalid: {path}")
    safety = policy.get("safety_boundary")
    expected_safety = {
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": BROKER_ACTION,
        "data_quality_status": DATA_QUALITY_STATUS,
        "automatic_execution_allowed": False,
        "task_register_mutation_allowed": False,
        "gate_relaxation_allowed": False,
    }
    if not isinstance(safety, Mapping) or any(
        safety.get(k) != v for k, v in expected_safety.items()
    ):
        raise ValueError(f"workflow health policy safety boundary is unsafe: {path}")
    if not isinstance(policy.get("candidate_rules"), Mapping):
        raise ValueError(f"workflow health policy candidate_rules must be a mapping: {path}")
    return policy


def build_workflow_health_payloads(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_POLICY_PATH,
    generated_at: datetime | None = None,
    git_commit_records: Sequence[Mapping[str, Any]] | None = None,
    history_dir: Path | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = load_workflow_health_policy(policy_path)
    lookback_days = int(policy["cadence"]["lookback_days"])
    window_start = datetime.combine(as_of - timedelta(days=lookback_days - 1), time.min, tzinfo=UTC)
    window_end = datetime.combine(as_of + timedelta(days=1), time.min, tzinfo=UTC)
    generated = (generated_at or datetime.now(tz=UTC)).astimezone(UTC)
    telemetry_gaps: list[dict[str, str]] = []

    validation_root = project_root / "outputs" / "validation_runtime"
    validation = _collect_validation_metrics(
        validation_root=validation_root,
        window_start=window_start,
        window_end=window_end,
        telemetry_gaps=telemetry_gaps,
    )
    transaction_root = (
        project_root
        / "outputs"
        / "architecture"
        / "arch_005_integration_publication_fence"
        / "transactions"
    )
    publication = _collect_publication_metrics(
        transaction_root=transaction_root,
        window_start=window_start,
        window_end=window_end,
        policy=policy,
        telemetry_gaps=telemetry_gaps,
    )
    commits = (
        [dict(record) for record in git_commit_records]
        if git_commit_records is not None
        else _collect_git_commits(
            project_root=project_root,
            git_ref=str(policy["sources"].get("git_ref", "main")),
            window_start=window_start,
            window_end=window_end,
            telemetry_gaps=telemetry_gaps,
        )
    )
    git_metrics = _build_git_metrics(commits=commits, policy=policy)
    metrics = {
        "validation": validation,
        "publication": publication,
        "git": git_metrics,
    }
    candidates = _build_candidates(metrics=metrics, policy=policy)
    previous_bundle = _latest_previous_validated_bundle(
        reports_dir=(history_dir or project_root / "outputs" / "reports"),
        as_of=as_of,
    )
    optimization_progress = _build_optimization_progress(
        metrics=metrics,
        candidates=candidates,
        previous_bundle=previous_bundle,
    )
    policy_sha256 = _sha256_path(policy_path)
    git_ref = str(policy["sources"].get("git_ref", "main"))
    git_head = _resolve_git_ref(project_root, git_ref, telemetry_gaps)
    report_identity = {
        "as_of": as_of.isoformat(),
        "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha256,
        "git_ref": git_ref,
        "git_head": git_head,
        "metrics": metrics,
        "optimization_progress": optimization_progress,
    }
    report_id = f"workflow-health-{_stable_hash(report_identity)[:20]}"
    for candidate in candidates:
        candidate["source_report_id"] = report_id
    candidate_bundle = {
        "schema_version": CANDIDATE_SCHEMA_VERSION,
        "report_type": CANDIDATE_REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "source_report_id": report_id,
        "policy_version": policy["policy_version"],
        "status": "PROPOSED_REVIEW_ONLY" if candidates else "NO_CANDIDATES",
        "candidate_count": len(candidates),
        "candidates": candidates,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": BROKER_ACTION,
        "data_quality_status": DATA_QUALITY_STATUS,
        "automatic_execution_allowed": False,
        "task_register_mutation_allowed": False,
        "gate_relaxation_allowed": False,
        "safety_boundary": (
            "review_only_developer_optimization_candidates; no automatic code, task, gate, "
            "production, portfolio, order or broker mutation"
        ),
    }
    status = "WORKFLOW_HEALTH_HEALTHY"
    if candidates:
        status = "WORKFLOW_HEALTH_OPTIMIZATION_CANDIDATES_FOUND"
    if telemetry_gaps:
        status = "WORKFLOW_HEALTH_LIMITED"
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "report_id": report_id,
        "as_of": as_of.isoformat(),
        "generated_at": generated.isoformat(),
        "status": status,
        "window": {
            "start_inclusive_utc": window_start.isoformat(),
            "end_exclusive_utc": window_end.isoformat(),
            "lookback_days": lookback_days,
            "timestamp_basis": policy["cadence"]["timestamp_basis"],
        },
        "policy": {
            "path": _portable_path(policy_path, project_root),
            "version": policy["policy_version"],
            "sha256": policy_sha256,
            "status": policy["policy_metadata"]["status"],
            "review_condition": policy["policy_metadata"]["review_condition"],
        },
        "git_snapshot": {"ref": git_ref, "head": git_head},
        "input_artifacts": {
            "validation_runtime": _portable_path(validation_root, project_root),
            "publication_transactions": _portable_path(transaction_root, project_root),
            "git_ref": git_ref,
        },
        "metrics": metrics,
        "telemetry_gaps": telemetry_gaps,
        "optimization_candidates": {
            "candidate_count": len(candidates),
            "candidate_ids": [item["candidate_id"] for item in candidates],
            "artifact": f"workflow_optimization_candidates_{as_of.isoformat()}.json",
        },
        "optimization_progress": optimization_progress,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": BROKER_ACTION,
        "data_quality_status": DATA_QUALITY_STATUS,
        "market_data_read": False,
        "automatic_command_dispatch_enabled": True,
        "automatic_report_generation_only": True,
        "reader_brief": {
            "summary": (
                f"研发流程健康状态 {status}；validation={validation['summary_count']}，"
                f"publication transactions={publication['transaction_count']}，"
                f"optimization candidates={len(candidates)}；"
                f"outcome={optimization_progress['status']}。"
            ),
            "key_result": status,
            "next_action": (
                "review_candidates_and_register_only_owner_accepted_optimization_work"
                if candidates
                else "continue_weekly_observation"
            ),
            "safety_boundary": (
                "developer telemetry only; production_effect=none; no gate relaxation, "
                "strategy, weight, task, broker or order mutation"
            ),
        },
    }
    return report, candidate_bundle


def latest_current_week_validated_bundle(
    *, reports_dir: Path, as_of: date
) -> dict[str, Any] | None:
    return _latest_validated_bundle(
        reports_dir=reports_dir,
        as_of=as_of,
        same_iso_week=True,
    )


def build_workflow_health_cycle_receipt(
    *,
    as_of: date,
    action: str,
    status: str,
    owner_decision_id: str,
    checkout_identity: Mapping[str, Any],
    report: Mapping[str, Any] | None = None,
    candidate_bundle: Mapping[str, Any] | None = None,
    validation: Mapping[str, Any] | None = None,
    artifact_paths: Sequence[Path] = (),
    blocker_codes: Sequence[str] = (),
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    iso = as_of.isocalendar()
    commitments = []
    for path in sorted(artifact_paths, key=lambda item: str(item)):
        if path.exists() and path.is_file():
            commitments.append(
                {
                    "path": str(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256_path(path),
                }
            )
    return {
        "schema_version": CYCLE_RECEIPT_SCHEMA_VERSION,
        "report_type": "workflow_health_cycle_receipt",
        "as_of": as_of.isoformat(),
        "iso_week": f"{iso.year}-W{iso.week:02d}",
        "generated_at": (generated_at or datetime.now(tz=UTC)).astimezone(UTC).isoformat(),
        "action": action,
        "status": status,
        "owner_decision_id": owner_decision_id,
        "existing_automation_id": "aitradingsystem-pit",
        "source_report_id": None if report is None else report.get("report_id"),
        "candidate_count": (
            0 if candidate_bundle is None else int(candidate_bundle.get("candidate_count", 0))
        ),
        "optimization_progress_status": (
            None
            if report is None
            else dict(report.get("optimization_progress", {})).get("status")
        ),
        "validation_status": (
            None if validation is None else validation.get("validation_status")
        ),
        "checkout_identity": dict(checkout_identity),
        "artifact_commitments": commitments,
        "blocker_codes": sorted(set(str(item) for item in blocker_codes if str(item))),
        "automatic_report_generation": True,
        "automatic_optimization_execution": False,
        "task_register_mutation_allowed": False,
        "gate_relaxation_allowed": False,
        "market_data_read": False,
        "data_quality_status": DATA_QUALITY_STATUS,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": BROKER_ACTION,
    }


def resolve_workflow_health_checkout_identity(
    *, project_root: Path, governed_paths: Sequence[Path]
) -> tuple[dict[str, Any], tuple[str, ...]]:
    blockers: list[str] = []

    def git(*args: str) -> tuple[int, str]:
        completed = subprocess.run(
            ["git", "-C", str(project_root), *args],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return completed.returncode, completed.stdout.strip()

    values: dict[str, str | None] = {}
    for key, args in (
        ("branch", ("branch", "--show-current")),
        ("head", ("rev-parse", "HEAD")),
        ("local_main", ("rev-parse", "refs/heads/main")),
        ("origin_main", ("rev-parse", "refs/remotes/origin/main")),
    ):
        return_code, value = git(*args)
        if return_code != 0 or not value:
            blockers.append(f"GIT_{key.upper()}_UNAVAILABLE")
            values[key] = None
        else:
            values[key] = value
    if values.get("branch") != "main":
        blockers.append("CHECKOUT_BRANCH_NOT_MAIN")
    if not (
        values.get("head")
        and values.get("head") == values.get("local_main") == values.get("origin_main")
    ):
        blockers.append("CHECKOUT_MAIN_ORIGIN_IDENTITY_MISMATCH")

    relative_paths: list[str] = []
    resolved_root = project_root.resolve()
    for path in governed_paths:
        try:
            relative_path = path.resolve().relative_to(resolved_root).as_posix()
            relative_paths.append(relative_path)
        except ValueError:
            blockers.append("GOVERNED_PATH_OUTSIDE_PROJECT_ROOT")
            continue
        if not path.exists() or not path.is_file():
            blockers.append(f"GOVERNED_PATH_MISSING:{relative_path}")
            continue
        return_code, _ = git("ls-files", "--error-unmatch", "--", relative_path)
        if return_code != 0:
            blockers.append(f"GOVERNED_PATH_UNTRACKED:{relative_path}")
    if relative_paths:
        for code, diff_args in (
            ("GOVERNED_PATHS_UNSTAGED_DIRTY", ("diff", "--quiet", "--")),
            ("GOVERNED_PATHS_STAGED_DIRTY", ("diff", "--cached", "--quiet", "--")),
        ):
            return_code, _ = git(*diff_args, *relative_paths)
            if return_code != 0:
                blockers.append(code)
    return (
        {
            "project_root": str(project_root.resolve()),
            "branch": values.get("branch"),
            "head": values.get("head"),
            "local_main": values.get("local_main"),
            "origin_main": values.get("origin_main"),
            "governed_paths": sorted(relative_paths),
        },
        tuple(sorted(set(blockers))),
    )


def _build_optimization_progress(
    *,
    metrics: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    previous_bundle: Mapping[str, Any] | None,
) -> dict[str, Any]:
    current_candidate_ids = sorted(str(item.get("candidate_id")) for item in candidates)
    if previous_bundle is None:
        return {
            "status": "NO_BASELINE",
            "baseline_as_of": None,
            "baseline_report_id": None,
            "metric_comparisons": [],
            "improved_metric_ids": [],
            "regressed_metric_ids": [],
            "unchanged_metric_ids": [],
            "candidate_lifecycle": {
                "new_candidate_ids": current_candidate_ids,
                "recurring_candidate_ids": [],
                "resolved_candidate_ids": [],
            },
            "summary": "没有更早且可独立重验的 weekly bundle；本周作为后续趋势基线。",
            "causal_claim_allowed": False,
        }

    previous_report = dict(previous_bundle["report"])
    previous_candidates = dict(previous_bundle["candidates"])
    previous_metrics = dict(previous_report.get("metrics", {}))
    metric_specs = (
        (
            "failed_validation_runtime_ratio",
            _ratio(
                float(dict(metrics.get("validation", {})).get("failed_elapsed_seconds", 0)),
                float(dict(metrics.get("validation", {})).get("elapsed_seconds", 0)),
            ),
            _ratio(
                float(
                    dict(previous_metrics.get("validation", {})).get(
                        "failed_elapsed_seconds", 0
                    )
                ),
                float(dict(previous_metrics.get("validation", {})).get("elapsed_seconds", 0)),
            ),
        ),
        (
            "failed_full_runtime_ratio",
            float(dict(metrics.get("validation", {})).get("failed_full_runtime_ratio", 0)),
            float(
                dict(previous_metrics.get("validation", {})).get(
                    "failed_full_runtime_ratio", 0
                )
            ),
        ),
        (
            "failed_full_count",
            float(dict(metrics.get("validation", {})).get("failed_full_count", 0)),
            float(dict(previous_metrics.get("validation", {})).get("failed_full_count", 0)),
        ),
        (
            "non_admin_failed_terminal_ratio",
            float(
                dict(metrics.get("publication", {})).get(
                    "non_admin_failed_terminal_ratio", 0
                )
            ),
            float(
                dict(previous_metrics.get("publication", {})).get(
                    "non_admin_failed_terminal_ratio", 0
                )
            ),
        ),
        (
            "authority_only_commit_ratio",
            float(dict(metrics.get("git", {})).get("authority_only_commit_ratio", 0)),
            float(
                dict(previous_metrics.get("git", {})).get("authority_only_commit_ratio", 0)
            ),
        ),
        (
            "duplicate_validation_group_count",
            float(
                dict(metrics.get("validation", {})).get(
                    "duplicate_validation_group_count", 0
                )
            ),
            float(
                dict(previous_metrics.get("validation", {})).get(
                    "duplicate_validation_group_count", 0
                )
            ),
        ),
        (
            "optimization_candidate_count",
            float(len(candidates)),
            float(previous_candidates.get("candidate_count", 0)),
        ),
    )
    comparisons: list[dict[str, Any]] = []
    for metric_id, current_value, previous_value in metric_specs:
        delta = round(current_value - previous_value, 6)
        classification = "UNCHANGED"
        if delta < 0:
            classification = "IMPROVED"
        elif delta > 0:
            classification = "REGRESSED"
        comparisons.append(
            {
                "metric_id": metric_id,
                "direction": "LOWER_IS_BETTER",
                "previous_value": round(previous_value, 6),
                "current_value": round(current_value, 6),
                "delta": delta,
                "classification": classification,
            }
        )
    improved = sorted(
        item["metric_id"] for item in comparisons if item["classification"] == "IMPROVED"
    )
    regressed = sorted(
        item["metric_id"] for item in comparisons if item["classification"] == "REGRESSED"
    )
    unchanged = sorted(
        item["metric_id"] for item in comparisons if item["classification"] == "UNCHANGED"
    )
    if improved and regressed:
        status = "MIXED"
    elif improved:
        status = "IMPROVED"
    elif regressed:
        status = "REGRESSED"
    else:
        status = "STABLE"
    previous_candidate_ids = {
        str(item.get("candidate_id")) for item in _records(previous_candidates.get("candidates"))
    }
    current_candidate_id_set = set(current_candidate_ids)
    lifecycle = {
        "new_candidate_ids": sorted(current_candidate_id_set - previous_candidate_ids),
        "recurring_candidate_ids": sorted(current_candidate_id_set & previous_candidate_ids),
        "resolved_candidate_ids": sorted(previous_candidate_ids - current_candidate_id_set),
    }
    return {
        "status": status,
        "baseline_as_of": previous_report.get("as_of"),
        "baseline_report_id": previous_report.get("report_id"),
        "metric_comparisons": comparisons,
        "improved_metric_ids": improved,
        "regressed_metric_ids": regressed,
        "unchanged_metric_ids": unchanged,
        "candidate_lifecycle": lifecycle,
        "summary": (
            f"相对 {previous_report.get('as_of')}：改善 {len(improved)} 项，"
            f"回退 {len(regressed)} 项，持平 {len(unchanged)} 项；"
            f"新增候选 {len(lifecycle['new_candidate_ids'])}，"
            f"持续候选 {len(lifecycle['recurring_candidate_ids'])}，"
            f"已消退候选 {len(lifecycle['resolved_candidate_ids'])}。"
        ),
        "causal_claim_allowed": False,
    }


def _latest_previous_validated_bundle(
    *, reports_dir: Path, as_of: date
) -> dict[str, Any] | None:
    return _latest_validated_bundle(reports_dir=reports_dir, as_of=as_of, same_iso_week=False)


def _latest_validated_bundle(
    *, reports_dir: Path, as_of: date, same_iso_week: bool
) -> dict[str, Any] | None:
    if not reports_dir.exists():
        return None
    current_week = as_of.isocalendar()[:2]
    candidates: list[tuple[date, Path]] = []
    for path in reports_dir.glob("workflow_health_????-??-??.json"):
        match = _WORKFLOW_HEALTH_REPORT_RE.fullmatch(path.name)
        if match is None:
            continue
        try:
            report_date = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if report_date > as_of:
            continue
        matches_week = report_date.isocalendar()[:2] == current_week
        if matches_week is not same_iso_week:
            continue
        candidates.append((report_date, path))
    for report_date, report_path in sorted(candidates, reverse=True):
        candidate_path = default_workflow_candidates_json_path(reports_dir, report_date)
        report_markdown_path = default_workflow_health_markdown_path(reports_dir, report_date)
        validation_path = default_workflow_health_validation_json_path(
            reports_dir, report_date
        )
        validation_markdown_path = default_workflow_health_validation_markdown_path(
            reports_dir, report_date
        )
        if not all(
            path.exists()
            for path in (
                candidate_path,
                report_markdown_path,
                validation_path,
                validation_markdown_path,
            )
        ):
            continue
        try:
            report = _read_json_object(report_path)
            candidate_bundle = _read_json_object(candidate_path)
            stored_validation = _read_json_object(validation_path)
            validation = validate_workflow_health_payloads(report, candidate_bundle)
        except (OSError, ValueError, json.JSONDecodeError, TypeError):
            continue
        if validation["validation_status"] not in {"PASS", "PASS_WITH_WARNINGS"}:
            continue
        if (
            stored_validation.get("source_report_id") != report.get("report_id")
            or stored_validation.get("validation_status") != validation["validation_status"]
        ):
            continue
        return {
            "report": report,
            "candidates": candidate_bundle,
            "validation": validation,
            "report_path": report_path,
            "report_markdown_path": report_markdown_path,
            "candidate_path": candidate_path,
            "validation_path": validation_path,
            "validation_markdown_path": validation_markdown_path,
        }
    return None


def validate_workflow_health_payloads(
    report: Mapping[str, Any], candidate_bundle: Mapping[str, Any]
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, str]] = []
    warning_issues: list[dict[str, str]] = []

    def check(check_id: str, passed: bool, message: str) -> None:
        checks.append({"check_id": check_id, "status": "PASS" if passed else "FAIL"})
        if not passed:
            blocking_issues.append({"issue_id": check_id, "message": message})

    check(
        "report_schema",
        report.get("schema_version") == REPORT_SCHEMA_VERSION
        and report.get("report_type") == REPORT_TYPE,
        "workflow health report schema/type mismatch",
    )
    check(
        "candidate_schema",
        candidate_bundle.get("schema_version") == CANDIDATE_SCHEMA_VERSION
        and candidate_bundle.get("report_type") == CANDIDATE_REPORT_TYPE,
        "workflow optimization candidate schema/type mismatch",
    )
    check(
        "report_candidate_binding",
        candidate_bundle.get("source_report_id") == report.get("report_id"),
        "candidate bundle does not bind the source report id",
    )
    report_candidates = report.get("optimization_candidates")
    candidates = _records(candidate_bundle.get("candidates"))
    expected_count = len(candidates)
    check(
        "candidate_counts",
        isinstance(report_candidates, Mapping)
        and int(report_candidates.get("candidate_count", -1)) == expected_count
        and int(candidate_bundle.get("candidate_count", -1)) == expected_count,
        "candidate counts are inconsistent",
    )
    candidate_ids = [str(item.get("candidate_id", "")) for item in candidates]
    check(
        "candidate_ids_unique",
        bool(all(candidate_ids)) and len(candidate_ids) == len(set(candidate_ids))
        if candidates
        else True,
        "candidate ids must be non-empty and unique",
    )
    unsafe_candidates = [
        item
        for item in candidates
        if item.get("status") != "PROPOSED_REVIEW_ONLY"
        or item.get("automatic_execution_allowed") is not False
        or item.get("task_register_mutation_allowed") is not False
        or item.get("gate_relaxation_allowed") is not False
    ]
    check(
        "candidate_safety",
        not unsafe_candidates,
        "candidate bundle contains an executable, task-mutating or gate-relaxing candidate",
    )
    check(
        "production_boundary",
        report.get("production_effect") == PRODUCTION_EFFECT
        and report.get("broker_action") == BROKER_ACTION
        and candidate_bundle.get("production_effect") == PRODUCTION_EFFECT
        and candidate_bundle.get("broker_action") == BROKER_ACTION,
        "workflow health artifacts must remain production_effect=none and broker_action=none",
    )
    check(
        "data_quality_boundary",
        report.get("data_quality_status") == DATA_QUALITY_STATUS
        and candidate_bundle.get("data_quality_status") == DATA_QUALITY_STATUS
        and report.get("market_data_read") is False,
        "developer telemetry must not claim or consume market data quality evidence",
    )
    progress = report.get("optimization_progress")
    progress_candidate_lifecycle = (
        progress.get("candidate_lifecycle") if isinstance(progress, Mapping) else None
    )
    current_candidate_ids = set(candidate_ids)
    lifecycle_current_ids = set()
    lifecycle_resolved_ids: set[str] = set()
    if isinstance(progress_candidate_lifecycle, Mapping):
        lifecycle_current_ids = {
            str(item)
            for key in ("new_candidate_ids", "recurring_candidate_ids")
            for item in progress_candidate_lifecycle.get(key, [])
        }
        lifecycle_resolved_ids = {
            str(item) for item in progress_candidate_lifecycle.get("resolved_candidate_ids", [])
        }
    check(
        "optimization_progress_contract",
        isinstance(progress, Mapping)
        and progress.get("status")
        in {"NO_BASELINE", "IMPROVED", "REGRESSED", "MIXED", "STABLE"}
        and isinstance(progress.get("metric_comparisons"), list)
        and isinstance(progress_candidate_lifecycle, Mapping)
        and lifecycle_current_ids == current_candidate_ids
        and not (lifecycle_resolved_ids & current_candidate_ids),
        "optimization progress must bind current candidates and use a supported status",
    )
    check(
        "automatic_report_only_boundary",
        report.get("automatic_command_dispatch_enabled") is True
        and report.get("automatic_report_generation_only") is True
        and candidate_bundle.get("automatic_execution_allowed") is False
        and candidate_bundle.get("task_register_mutation_allowed") is False
        and candidate_bundle.get("gate_relaxation_allowed") is False,
        "automatic dispatch may generate reports only and must not execute optimization work",
    )
    window = report.get("window")
    check(
        "window_contract",
        isinstance(window, Mapping)
        and int(window.get("lookback_days", 0)) > 0
        and bool(window.get("start_inclusive_utc"))
        and bool(window.get("end_exclusive_utc")),
        "workflow health window is incomplete",
    )
    gaps = _records(report.get("telemetry_gaps"))
    if gaps:
        warning_issues.append(
            {
                "issue_id": "telemetry_gaps_present",
                "message": f"report discloses {len(gaps)} telemetry gap(s)",
            }
        )
    status = "FAIL" if blocking_issues else "PASS_WITH_WARNINGS" if warning_issues else "PASS"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": report.get("as_of"),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "validation_status": status,
        "status": status,
        "source_report_id": report.get("report_id"),
        "check_count": len(checks),
        "failed_check_count": len(blocking_issues),
        "warning_check_count": len(warning_issues),
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "production_effect": PRODUCTION_EFFECT,
        "broker_action": BROKER_ACTION,
        "data_quality_status": DATA_QUALITY_STATUS,
    }


def render_workflow_health_markdown(report: Mapping[str, Any]) -> str:
    validation = dict(report.get("metrics", {}).get("validation", {}))
    publication = dict(report.get("metrics", {}).get("publication", {}))
    git_metrics = dict(report.get("metrics", {}).get("git", {}))
    progress = dict(report.get("optimization_progress", {}))
    lines = [
        f"# 研发流程健康周报（{report.get('as_of')}）",
        "",
        f"- 状态：`{report.get('status')}`",
        f"- 窗口：`{report.get('window', {}).get('start_inclusive_utc')}` 至 "
        f"`{report.get('window', {}).get('end_exclusive_utc')}`（右开）",
        f"- Policy：`{report.get('policy', {}).get('version')}`",
        f"- Git：`{report.get('git_snapshot', {}).get('ref')}` @ "
        f"`{report.get('git_snapshot', {}).get('head')}`",
        f"- 安全边界：`production_effect={PRODUCTION_EFFECT}`，"
        f"`data_quality_status={DATA_QUALITY_STATUS}`",
        "",
        "## 核心指标",
        "",
        "|指标|结果|",
        "|---|---:|",
        f"|Validation summaries|{validation.get('summary_count', 0)}|",
        f"|Validation runner hours|{_hours(validation.get('elapsed_seconds', 0))}|",
        f"|Failed validation runner hours|{_hours(validation.get('failed_elapsed_seconds', 0))}|",
        f"|Full / failed Full|{validation.get('full_count', 0)} / "
        f"{validation.get('failed_full_count', 0)}|",
        f"|Failed Full runtime ratio|{_percent(validation.get('failed_full_runtime_ratio', 0))}|",
        f"|Publication transactions|{publication.get('transaction_count', 0)}|",
        f"|Non-admin failed terminals|{publication.get('non_admin_failed_terminal_count', 0)}|",
        f"|Administrative stops|{publication.get('administrative_stop_count', 0)}|",
        f"|Git commits / authority-only|{git_metrics.get('commit_count', 0)} / "
        f"{git_metrics.get('authority_only_commit_count', 0)}|",
        "|Optimization candidates|"
        f"{report.get('optimization_candidates', {}).get('candidate_count', 0)}|",
        "",
        "## Validation tiers",
        "",
        "|Tier|Runs|Failed|Hours|",
        "|---|---:|---:|---:|",
    ]
    for item in validation.get("tier_breakdown", []):
        lines.append(
            f"|`{item['tier']}`|{item['run_count']}|{item['failed_count']}|"
            f"{_hours(item['elapsed_seconds'])}|"
        )
    lines.extend(
        [
            "",
            "## Publication 热点任务",
            "",
            "|Task|Transactions|Failed|Admin stops|",
            "|---|---:|---:|---:|",
        ]
    )
    for item in publication.get("top_task_churn", [])[:10]:
        lines.append(
            f"|`{item['task_id']}`|{item['transaction_count']}|"
            f"{item['failed_terminal_count']}|{item['administrative_stop_count']}|"
        )
    lines.extend(
        [
            "",
            "## 优化成果回顾",
            "",
            f"- 结论：`{progress.get('status', 'NO_BASELINE')}`",
            f"- 对比基线：`{progress.get('baseline_as_of') or 'NONE'}`",
            f"- 摘要：{progress.get('summary', '暂无可信历史基线。')}",
            "",
            "|指标|上期|本期|Delta|分类|",
            "|---|---:|---:|---:|---|",
        ]
    )
    for item in progress.get("metric_comparisons", []):
        lines.append(
            f"|`{item.get('metric_id')}`|{item.get('previous_value')}|"
            f"{item.get('current_value')}|{item.get('delta')}|`{item.get('classification')}`|"
        )
    lifecycle = dict(progress.get("candidate_lifecycle", {}))
    lines.extend(
        [
            "",
            f"- 新增候选：{len(lifecycle.get('new_candidate_ids', []))}",
            f"- 持续候选：{len(lifecycle.get('recurring_candidate_ids', []))}",
            f"- 已消退候选：{len(lifecycle.get('resolved_candidate_ids', []))}",
        ]
    )
    candidate_ids = report.get("optimization_candidates", {}).get("candidate_ids", [])
    lines.extend(["", "## 优化候选", ""])
    if candidate_ids:
        lines.extend(f"- `{candidate_id}`" for candidate_id in candidate_ids)
        lines.append("")
        lines.append("候选仅供 review；不会自动修改任务、代码或门禁。")
    else:
        lines.append("本窗口没有触发 policy candidate rule。")
    gaps = _records(report.get("telemetry_gaps"))
    lines.extend(["", "## Telemetry 限制", ""])
    if gaps:
        lines.extend(f"- `{item.get('source')}`：{item.get('reason')}" for item in gaps)
    else:
        lines.append("本窗口未发现 telemetry gap。")
    lines.extend(
        [
            "",
            "## 安全边界",
            "",
            "本报告只读工程 evidence；不读取 market cache，不运行数据质量门禁，不改变策略、"
            "权重、任务状态、production 或 broker/order。任何候选必须经 owner 接受并另行"
            "登记后实施。",
            "",
        ]
    )
    return "\n".join(lines)


def render_workflow_health_validation_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        f"# 研发流程健康报告校验（{payload.get('as_of')}）",
        "",
        f"- 状态：`{payload.get('validation_status')}`",
        f"- Checks：{payload.get('check_count', 0)}",
        f"- Failed：{payload.get('failed_check_count', 0)}",
        f"- Warnings：{payload.get('warning_check_count', 0)}",
        f"- Production effect：`{payload.get('production_effect')}`",
        "",
        "## Checks",
        "",
        "|Check|Status|",
        "|---|---|",
    ]
    for item in payload.get("checks", []):
        lines.append(f"|`{item['check_id']}`|`{item['status']}`|")
    for heading, key in (("Blocking issues", "blocking_issues"), ("Warnings", "warning_issues")):
        lines.extend(["", f"## {heading}", ""])
        records = _records(payload.get(key))
        if records:
            lines.extend(f"- `{item.get('issue_id')}`：{item.get('message')}" for item in records)
        else:
            lines.append("无。")
    lines.append("")
    return "\n".join(lines)


def write_workflow_health_json(payload: Mapping[str, Any], path: Path) -> Path:
    return write_json_atomic(path, dict(payload), sort_keys=True).path


def write_workflow_health_markdown(payload: Mapping[str, Any], path: Path) -> Path:
    return write_text_atomic(path, render_workflow_health_markdown(payload)).path


def write_workflow_candidates_json(payload: Mapping[str, Any], path: Path) -> Path:
    return write_json_atomic(path, dict(payload), sort_keys=True).path


def write_workflow_health_validation_json(payload: Mapping[str, Any], path: Path) -> Path:
    return write_json_atomic(path, dict(payload), sort_keys=True).path


def write_workflow_health_validation_markdown(payload: Mapping[str, Any], path: Path) -> Path:
    return write_text_atomic(path, render_workflow_health_validation_markdown(payload)).path


def write_workflow_health_cycle_receipt(payload: Mapping[str, Any], path: Path) -> Path:
    return write_json_atomic(path, dict(payload), sort_keys=True).path


def _collect_validation_metrics(
    *,
    validation_root: Path,
    window_start: datetime,
    window_end: datetime,
    telemetry_gaps: list[dict[str, str]],
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    if not validation_root.exists():
        telemetry_gaps.append(
            {
                "source": "validation_runtime",
                "path": str(validation_root),
                "reason": "source_root_missing",
            }
        )
        return _empty_validation_metrics()
    for path in sorted(validation_root.glob("**/test_runtime_summary.json")):
        hint = _path_timestamp_hint(path)
        try:
            payload = _read_json_object(path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            if hint is None or window_start <= hint < window_end:
                telemetry_gaps.append(
                    {
                        "source": "validation_runtime",
                        "path": str(path),
                        "reason": f"invalid_json:{type(exc).__name__}",
                    }
                )
            continue
        ended_at = _parse_datetime(payload.get("ended_at_utc"))
        if ended_at is None:
            if hint is None or window_start <= hint < window_end:
                telemetry_gaps.append(
                    {
                        "source": "validation_runtime",
                        "path": str(path),
                        "reason": "missing_or_invalid_ended_at_utc",
                    }
                )
            continue
        if not window_start <= ended_at < window_end:
            continue
        tier = str(
            payload.get("resolved_tier")
            or payload.get("tier")
            or payload.get("requested_tier")
            or "UNKNOWN"
        )
        status = str(payload.get("status") or "UNKNOWN").upper()
        elapsed = _non_negative_float(payload.get("elapsed_seconds"))
        nodeids = _failed_nodeids(payload.get("pytest_output_tail"))
        records.append(
            {
                "path": str(path),
                "tier": tier,
                "status": status,
                "elapsed_seconds": elapsed,
                "git_commit": str(payload.get("git_commit") or ""),
                "ended_at_utc": ended_at.isoformat(),
                "failed_nodeids": nodeids,
            }
        )
    tier_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        tier_groups[record["tier"]].append(record)
    tier_breakdown = []
    for tier, group in sorted(tier_groups.items()):
        tier_breakdown.append(
            {
                "tier": tier,
                "run_count": len(group),
                "failed_count": len([item for item in group if item["status"] != "PASS"]),
                "elapsed_seconds": round(sum(item["elapsed_seconds"] for item in group), 3),
                "failed_elapsed_seconds": round(
                    sum(item["elapsed_seconds"] for item in group if item["status"] != "PASS"), 3
                ),
            }
        )
    full_records = [item for item in records if item["tier"] == "full"]
    failed_full = [item for item in full_records if item["status"] != "PASS"]
    full_runtime = sum(item["elapsed_seconds"] for item in full_records)
    failed_full_runtime = sum(item["elapsed_seconds"] for item in failed_full)
    duplicate_counter = Counter(
        (item["git_commit"], item["tier"])
        for item in records
        if item["git_commit"] and item["tier"] != "UNKNOWN"
    )
    duplicate_groups = [
        {"git_commit": commit, "tier": tier, "run_count": count, "duplicate_run_count": count - 1}
        for (commit, tier), count in duplicate_counter.items()
        if count > 1
    ]
    duplicate_groups.sort(key=lambda item: (-item["run_count"], item["tier"], item["git_commit"]))
    cluster_counter: Counter[str] = Counter()
    for record in records:
        if record["status"] == "PASS":
            continue
        cluster_counter.update({_node_file(nodeid) for nodeid in record["failed_nodeids"]})
    failure_clusters = [
        {"test_file": test_file, "failed_summary_count": count}
        for test_file, count in cluster_counter.most_common()
        if test_file
    ]
    failed_records = [item for item in records if item["status"] != "PASS"]
    return {
        "summary_count": len(records),
        "pass_count": len(records) - len(failed_records),
        "failed_count": len(failed_records),
        "elapsed_seconds": round(sum(item["elapsed_seconds"] for item in records), 3),
        "failed_elapsed_seconds": round(sum(item["elapsed_seconds"] for item in failed_records), 3),
        "full_count": len(full_records),
        "failed_full_count": len(failed_full),
        "full_elapsed_seconds": round(full_runtime, 3),
        "failed_full_elapsed_seconds": round(failed_full_runtime, 3),
        "failed_full_runtime_ratio": round(_ratio(failed_full_runtime, full_runtime), 6),
        "tier_breakdown": tier_breakdown,
        "duplicate_validation_group_count": len(duplicate_groups),
        "duplicate_validation_run_count": sum(
            item["duplicate_run_count"] for item in duplicate_groups
        ),
        "duplicate_validation_groups": duplicate_groups[:20],
        "failure_clusters": failure_clusters[:20],
    }


def _collect_publication_metrics(
    *,
    transaction_root: Path,
    window_start: datetime,
    window_end: datetime,
    policy: Mapping[str, Any],
    telemetry_gaps: list[dict[str, str]],
) -> dict[str, Any]:
    if not transaction_root.exists():
        telemetry_gaps.append(
            {
                "source": "publication_transactions",
                "path": str(transaction_root),
                "reason": "source_root_missing",
            }
        )
        return _empty_publication_metrics()
    admin_policy = policy["sources"].get("administrative_stop", {})
    admin_generators = sorted(str(item) for item in admin_policy.get("exact_generator_ids", []))
    admin_phases = {str(item) for item in admin_policy.get("last_non_terminal_phases", [])}
    records: list[dict[str, Any]] = []
    for directory in sorted(path for path in transaction_root.iterdir() if path.is_dir()):
        transaction_path = directory / "transaction.json"
        if not transaction_path.exists():
            continue
        hint = _transaction_date_hint(directory.name)
        try:
            transaction = _read_json_object(transaction_path)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            if hint is None or window_start.date() <= hint < window_end.date():
                telemetry_gaps.append(
                    {
                        "source": "publication_transactions",
                        "path": str(transaction_path),
                        "reason": f"invalid_json:{type(exc).__name__}",
                    }
                )
            continue
        created_at = _parse_datetime(transaction.get("created_at"))
        if created_at is None:
            if hint is None or window_start.date() <= hint < window_end.date():
                telemetry_gaps.append(
                    {
                        "source": "publication_transactions",
                        "path": str(transaction_path),
                        "reason": "missing_or_invalid_created_at",
                    }
                )
            continue
        if not window_start <= created_at < window_end:
            continue
        events: list[dict[str, Any]] = []
        event_root = directory / "events"
        for event_path in sorted(event_root.glob("*.json")) if event_root.exists() else []:
            try:
                event = _read_json_object(event_path)
            except (OSError, ValueError, json.JSONDecodeError) as exc:
                telemetry_gaps.append(
                    {
                        "source": "publication_transactions",
                        "path": str(event_path),
                        "reason": f"invalid_event:{type(exc).__name__}",
                    }
                )
                continue
            occurred_at = _parse_datetime(event.get("occurred_at"))
            if occurred_at is None:
                telemetry_gaps.append(
                    {
                        "source": "publication_transactions",
                        "path": str(event_path),
                        "reason": "missing_or_invalid_event_timestamp",
                    }
                )
                continue
            if occurred_at < window_end:
                event["_occurred_at"] = occurred_at
                events.append(event)
        events.sort(key=lambda item: (int(item.get("sequence", 0)), item["_occurred_at"]))
        phases = [str(event.get("phase") or "UNKNOWN") for event in events]
        last = events[-1] if events else None
        last_phase = str(last.get("phase") or "NO_EVENTS") if last else "NO_EVENTS"
        terminal = bool(last and last.get("terminal") is True)
        failed_terminal = terminal and last_phase == "FAILED"
        released = terminal and last_phase == "RELEASED"
        last_non_terminal_phase = next(
            (phase for phase in reversed(phases) if phase not in {"FAILED", "RELEASED"}),
            "NO_EVENTS",
        )
        generator_ids = sorted(str(item) for item in transaction.get("generator_ids", []))
        administrative_stop = (
            failed_terminal
            and generator_ids == admin_generators
            and last_non_terminal_phase in admin_phases
        )
        ended_at = last["_occurred_at"] if last else min(window_end, datetime.now(tz=UTC))
        records.append(
            {
                "transaction_id": str(transaction.get("transaction_id") or directory.name),
                "task_id": str(transaction.get("task_id") or "UNKNOWN"),
                "created_at": created_at.isoformat(),
                "last_event_at": ended_at.isoformat(),
                "duration_seconds": round(max((ended_at - created_at).total_seconds(), 0.0), 3),
                "last_phase": last_phase,
                "last_non_terminal_phase": last_non_terminal_phase,
                "terminal": terminal,
                "released": released,
                "failed_terminal": failed_terminal,
                "administrative_stop": administrative_stop,
                "candidate_reached": "CANDIDATE_COMMIT_PRE" in phases,
                "formal_validation_reached": "FORMAL_VALIDATION_PRE" in phases,
                "full_dispatched": "FULL_DISPATCHED" in phases,
            }
        )
    task_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        task_groups[record["task_id"]].append(record)
    task_churn = []
    for task_id, group in task_groups.items():
        failed = [item for item in group if item["failed_terminal"]]
        admin = [item for item in group if item["administrative_stop"]]
        non_admin_terminal = [
            item for item in group if item["terminal"] and not item["administrative_stop"]
        ]
        non_admin_failed = [item for item in failed if not item["administrative_stop"]]
        task_churn.append(
            {
                "task_id": task_id,
                "transaction_count": len(group),
                "terminal_count": len([item for item in group if item["terminal"]]),
                "failed_terminal_count": len(failed),
                "administrative_stop_count": len(admin),
                "non_admin_failed_terminal_count": len(non_admin_failed),
                "non_admin_failed_ratio": round(
                    _ratio(len(non_admin_failed), len(non_admin_terminal)), 6
                ),
                "duration_seconds": round(sum(item["duration_seconds"] for item in group), 3),
            }
        )
    task_churn.sort(key=lambda item: (-item["transaction_count"], item["task_id"]))
    phase_counts = Counter(item["last_non_terminal_phase"] for item in records if item["terminal"])
    admin_stops = [item for item in records if item["administrative_stop"]]
    non_admin_terminals = [
        item for item in records if item["terminal"] and not item["administrative_stop"]
    ]
    non_admin_failed = [item for item in non_admin_terminals if item["failed_terminal"]]
    early_failed = [item for item in non_admin_failed if not item["candidate_reached"]]
    return {
        "transaction_count": len(records),
        "terminal_count": len([item for item in records if item["terminal"]]),
        "released_count": len([item for item in records if item["released"]]),
        "failed_terminal_count": len([item for item in records if item["failed_terminal"]]),
        "administrative_stop_count": len(admin_stops),
        "non_admin_terminal_count": len(non_admin_terminals),
        "non_admin_failed_terminal_count": len(non_admin_failed),
        "non_admin_failed_terminal_ratio": round(
            _ratio(len(non_admin_failed), len(non_admin_terminals)), 6
        ),
        "early_non_admin_failed_terminal_count": len(early_failed),
        "early_non_admin_failed_terminal_ratio": round(
            _ratio(len(early_failed), len(non_admin_terminals)), 6
        ),
        "open_transaction_count": len([item for item in records if not item["terminal"]]),
        "duration_seconds": round(sum(item["duration_seconds"] for item in records), 3),
        "terminal_last_phase_counts": dict(sorted(phase_counts.items())),
        "top_task_churn": task_churn[:20],
    }


def _collect_git_commits(
    *,
    project_root: Path,
    git_ref: str,
    window_start: datetime,
    window_end: datetime,
    telemetry_gaps: list[dict[str, str]],
) -> list[dict[str, Any]]:
    command = [
        "git",
        "-C",
        str(project_root),
        "log",
        git_ref,
        f"--since={window_start.isoformat()}",
        f"--until={window_end.isoformat()}",
        "--format=%x1e%H%x1f%cI",
        "--name-only",
    ]
    completed = subprocess.run(
        command, capture_output=True, text=True, encoding="utf-8", check=False
    )
    if completed.returncode != 0:
        telemetry_gaps.append(
            {
                "source": "git",
                "path": git_ref,
                "reason": f"git_log_failed:{completed.stderr.strip()[:200]}",
            }
        )
        return []
    records: list[dict[str, Any]] = []
    for block in completed.stdout.split("\x1e"):
        block = block.strip()
        if not block:
            continue
        header, *path_lines = block.splitlines()
        if "\x1f" not in header:
            continue
        commit, committed_at = header.split("\x1f", 1)
        paths = sorted({line.strip().replace("\\", "/") for line in path_lines if line.strip()})
        records.append(
            {"commit": commit.strip(), "committed_at": committed_at.strip(), "paths": paths}
        )
    return records


def _build_git_metrics(
    *, commits: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    sources = policy["sources"]
    implementation_prefixes = tuple(
        str(item) for item in sources.get("implementation_path_prefixes", [])
    )
    authority_prefixes = tuple(str(item) for item in sources.get("authority_path_prefixes", []))
    implementation_count = 0
    authority_count = 0
    authority_only_count = 0
    for commit in commits:
        paths = [str(path).replace("\\", "/") for path in commit.get("paths", [])]
        implementation = any(_matches_prefix(path, implementation_prefixes) for path in paths)
        authority = any(_matches_prefix(path, authority_prefixes) for path in paths)
        implementation_count += int(implementation)
        authority_count += int(authority)
        authority_only_count += int(authority and not implementation)
    return {
        "commit_count": len(commits),
        "implementation_touching_commit_count": implementation_count,
        "authority_touching_commit_count": authority_count,
        "authority_only_commit_count": authority_only_count,
        "authority_only_commit_ratio": round(_ratio(authority_only_count, len(commits)), 6),
    }


def _build_candidates(
    *, metrics: Mapping[str, Any], policy: Mapping[str, Any]
) -> list[dict[str, Any]]:
    rules = policy["candidate_rules"]
    validation = metrics["validation"]
    publication = metrics["publication"]
    git_metrics = metrics["git"]
    candidates: list[dict[str, Any]] = []

    def add(rule_id: str, scope: str, evidence: Mapping[str, Any]) -> None:
        rule = rules[rule_id]
        fingerprint = _stable_hash({"rule_id": rule_id, "scope": scope})
        candidates.append(
            {
                "candidate_id": f"workflow-opt-{fingerprint[:20]}",
                "fingerprint": fingerprint,
                "rule_id": rule_id,
                "scope": scope,
                "priority": rule["priority"],
                "status": "PROPOSED_REVIEW_ONLY",
                "evidence": dict(evidence),
                "recommended_experiment": rule["recommended_experiment"],
                "guardrails": [
                    "preserve_all_existing_fail_closed_gates",
                    "register_owner_accepted_work_before_implementation",
                    "validate_against_exact_candidate_sha",
                    "production_effect_none",
                ],
                "automatic_execution_allowed": False,
                "task_register_mutation_allowed": False,
                "gate_relaxation_allowed": False,
            }
        )

    rule = rules["failed_full_runtime"]
    if (
        rule.get("enabled")
        and validation["failed_full_count"] >= int(rule["minimum_failed_runs"])
        and validation["failed_full_runtime_ratio"] >= float(rule["minimum_failed_runtime_ratio"])
    ):
        add(
            "failed_full_runtime",
            "repository",
            {
                "failed_full_count": validation["failed_full_count"],
                "failed_full_elapsed_seconds": validation["failed_full_elapsed_seconds"],
                "failed_full_runtime_ratio": validation["failed_full_runtime_ratio"],
            },
        )
    rule = rules["early_transaction_churn"]
    if (
        rule.get("enabled")
        and publication["early_non_admin_failed_terminal_count"]
        >= int(rule["minimum_failed_terminals"])
        and publication["early_non_admin_failed_terminal_ratio"]
        >= float(rule["minimum_failed_ratio"])
    ):
        add(
            "early_transaction_churn",
            "repository",
            {
                "early_non_admin_failed_terminal_count": publication[
                    "early_non_admin_failed_terminal_count"
                ],
                "early_non_admin_failed_terminal_ratio": publication[
                    "early_non_admin_failed_terminal_ratio"
                ],
                "administrative_stop_count": publication["administrative_stop_count"],
            },
        )
    rule = rules["authority_only_amplification"]
    if (
        rule.get("enabled")
        and git_metrics["commit_count"] >= int(rule["minimum_commit_count"])
        and git_metrics["authority_only_commit_ratio"]
        >= float(rule["minimum_authority_only_ratio"])
    ):
        add(
            "authority_only_amplification",
            "repository",
            {
                "commit_count": git_metrics["commit_count"],
                "authority_only_commit_count": git_metrics["authority_only_commit_count"],
                "authority_only_commit_ratio": git_metrics["authority_only_commit_ratio"],
            },
        )
    rule = rules["task_retry_churn"]
    if rule.get("enabled"):
        qualifying = [
            item
            for item in publication["top_task_churn"]
            if item["transaction_count"] >= int(rule["minimum_transaction_count"])
            and item["non_admin_failed_ratio"] >= float(rule["minimum_failed_ratio"])
        ]
        for item in qualifying[: int(rule["maximum_candidates"])]:
            add("task_retry_churn", item["task_id"], item)
    rule = rules["duplicate_validation_dispatch"]
    duplicate_groups = [
        item
        for item in validation["duplicate_validation_groups"]
        if item["run_count"] >= int(rule["minimum_group_size"])
    ]
    if rule.get("enabled") and len(duplicate_groups) >= int(rule["minimum_duplicate_group_count"]):
        add(
            "duplicate_validation_dispatch",
            "repository",
            {
                "qualifying_group_count": len(duplicate_groups),
                "duplicate_run_count": sum(
                    item["duplicate_run_count"] for item in duplicate_groups
                ),
                "top_groups": duplicate_groups[:10],
            },
        )
    rule = rules["validation_failure_cluster"]
    if rule.get("enabled") and validation["failed_count"] >= int(rule["minimum_failed_summaries"]):
        clusters = [
            item
            for item in validation["failure_clusters"]
            if item["failed_summary_count"] >= int(rule["minimum_cluster_count"])
        ]
        for item in clusters[: int(rule["maximum_candidates"])]:
            add("validation_failure_cluster", item["test_file"], item)
    candidates.sort(key=lambda item: (item["priority"], item["rule_id"], item["scope"]))
    return candidates


def _empty_validation_metrics() -> dict[str, Any]:
    return {
        "summary_count": 0,
        "pass_count": 0,
        "failed_count": 0,
        "elapsed_seconds": 0.0,
        "failed_elapsed_seconds": 0.0,
        "full_count": 0,
        "failed_full_count": 0,
        "full_elapsed_seconds": 0.0,
        "failed_full_elapsed_seconds": 0.0,
        "failed_full_runtime_ratio": 0.0,
        "tier_breakdown": [],
        "duplicate_validation_group_count": 0,
        "duplicate_validation_run_count": 0,
        "duplicate_validation_groups": [],
        "failure_clusters": [],
    }


def _empty_publication_metrics() -> dict[str, Any]:
    return {
        "transaction_count": 0,
        "terminal_count": 0,
        "released_count": 0,
        "failed_terminal_count": 0,
        "administrative_stop_count": 0,
        "non_admin_terminal_count": 0,
        "non_admin_failed_terminal_count": 0,
        "non_admin_failed_terminal_ratio": 0.0,
        "early_non_admin_failed_terminal_count": 0,
        "early_non_admin_failed_terminal_ratio": 0.0,
        "open_transaction_count": 0,
        "duration_seconds": 0.0,
        "terminal_last_phase_counts": {},
        "top_task_churn": [],
    }


def _read_json_object(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("JSON root must be an object")
    return payload


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC)


def _path_timestamp_hint(path: Path) -> datetime | None:
    match = _RUN_ID_TIMESTAMP_RE.search(path.parent.name)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        return None


def _transaction_date_hint(name: str) -> date | None:
    matches = _DATE_TOKEN_RE.findall(name)
    if not matches:
        return None
    try:
        return datetime.strptime(matches[-1], "%Y%m%d").date()
    except ValueError:
        return None


def _failed_nodeids(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        text = "\n".join(str(item) for item in value)
    else:
        return []
    return sorted(
        set(match.group(1).replace("\\", "/") for match in _FAILED_NODE_RE.finditer(text))
    )


def _node_file(nodeid: str) -> str:
    return nodeid.split("::", 1)[0]


def _resolve_git_ref(
    project_root: Path, git_ref: str, telemetry_gaps: list[dict[str, str]]
) -> str | None:
    completed = subprocess.run(
        ["git", "-C", str(project_root), "rev-parse", git_ref],
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if completed.returncode == 0:
        return completed.stdout.strip()
    telemetry_gaps.append(
        {
            "source": "git",
            "path": git_ref,
            "reason": f"git_ref_unresolved:{completed.stderr.strip()[:200]}",
        }
    )
    return None


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()


def _sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _portable_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _latest_dated_path(output_dir: Path, prefix: str, suffix: str) -> Path | None:
    filename_pattern = re.compile(
        rf"^{re.escape(prefix)}\d{{4}}-\d{{2}}-\d{{2}}{re.escape(suffix)}$"
    )
    candidates = sorted(
        path
        for path in output_dir.glob(f"{prefix}*{suffix}")
        if path.is_file() and filename_pattern.fullmatch(path.name)
    )
    return candidates[-1] if candidates else None


def _matches_prefix(path: str, prefixes: Sequence[str]) -> bool:
    return any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in prefixes)


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _non_negative_float(value: Any) -> float:
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _ratio(numerator: float | int, denominator: float | int) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _hours(seconds: Any) -> str:
    return f"{_non_negative_float(seconds) / 3600:.2f}"


def _percent(value: Any) -> str:
    return f"{_non_negative_float(value) * 100:.1f}%"
