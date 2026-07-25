from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_json_atomic, write_markdown_atomic

DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "ops_scheduler_checkout.yaml"
)
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class OpsSchedulerCheckoutPolicy:
    policy_id: str
    version: str
    status: str
    scheduler_marker_name: str
    scheduler_marker_value: str
    checkout_root_name: str
    release_commit_name: str
    development_checkout_root_name: str
    expected_remote: str
    exact_release_commit_required: bool
    clean_checkout_required: bool
    independent_from_development_checkout: bool
    current_process_must_run_from_ops_checkout: bool
    unified_external_trigger: tuple[str, ...]
    separate_periodic_scheduler_entries_allowed: bool
    activation_authorized: bool
    scheduler_installed: bool
    scheduler_enabled: bool


GitRunner = Callable[..., subprocess.CompletedProcess[str]]


def load_ops_scheduler_checkout_policy(
    path: Path = DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
) -> OpsSchedulerCheckoutPolicy:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("ops scheduler checkout policy must be a mapping")
    if payload.get("schema_version") != "ops_scheduler_checkout_policy.v1":
        raise ValueError("unsupported ops scheduler checkout policy schema")
    if payload.get("status") != "REVIEWED_ENGINEERING_READY_OWNER_DEPLOYMENT_REQUIRED":
        raise ValueError("ops scheduler checkout policy must remain owner-deployment gated")
    environment = _mapping(payload.get("environment"), "environment")
    repository = _mapping(payload.get("repository"), "repository")
    trigger = _mapping(payload.get("trigger"), "trigger")
    deployment = _mapping(payload.get("deployment"), "deployment")
    safety = _mapping(payload.get("safety"), "safety")
    trigger_command = trigger.get("unified_external_trigger")
    if not isinstance(trigger_command, list) or not all(
        isinstance(item, str) and item for item in trigger_command
    ):
        raise ValueError("unified_external_trigger must be a non-empty string list")
    if tuple(trigger_command) != ("aits", "ops", "daily-run"):
        raise ValueError("aits ops daily-run must remain the unified external trigger")
    expected_safety = {
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }
    if any(safety.get(key) != value for key, value in expected_safety.items()):
        raise ValueError("ops scheduler checkout safety boundary invalid")
    policy = OpsSchedulerCheckoutPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        version=_text(payload.get("version"), "version"),
        status=_text(payload.get("status"), "status"),
        scheduler_marker_name=_text(
            environment.get("scheduler_marker_name"),
            "scheduler_marker_name",
        ),
        scheduler_marker_value=_text(
            environment.get("scheduler_marker_value"),
            "scheduler_marker_value",
        ),
        checkout_root_name=_text(
            environment.get("checkout_root_name"),
            "checkout_root_name",
        ),
        release_commit_name=_text(
            environment.get("release_commit_name"),
            "release_commit_name",
        ),
        development_checkout_root_name=_text(
            environment.get("development_checkout_root_name"),
            "development_checkout_root_name",
        ),
        expected_remote=_text(repository.get("expected_remote"), "expected_remote"),
        exact_release_commit_required=_bool(
            repository.get("exact_release_commit_required"),
            "exact_release_commit_required",
        ),
        clean_checkout_required=_bool(
            repository.get("clean_checkout_required"),
            "clean_checkout_required",
        ),
        independent_from_development_checkout=_bool(
            repository.get("independent_from_development_checkout"),
            "independent_from_development_checkout",
        ),
        current_process_must_run_from_ops_checkout=_bool(
            repository.get("current_process_must_run_from_ops_checkout"),
            "current_process_must_run_from_ops_checkout",
        ),
        unified_external_trigger=tuple(trigger_command),
        separate_periodic_scheduler_entries_allowed=_bool(
            trigger.get("separate_periodic_scheduler_entries_allowed"),
            "separate_periodic_scheduler_entries_allowed",
        ),
        activation_authorized=_bool(
            deployment.get("activation_authorized"),
            "activation_authorized",
        ),
        scheduler_installed=_bool(
            deployment.get("scheduler_installed"),
            "scheduler_installed",
        ),
        scheduler_enabled=_bool(
            deployment.get("scheduler_enabled"),
            "scheduler_enabled",
        ),
    )
    if (
        not policy.exact_release_commit_required
        or not policy.clean_checkout_required
        or not policy.independent_from_development_checkout
        or not policy.current_process_must_run_from_ops_checkout
        or policy.separate_periodic_scheduler_entries_allowed
        or policy.activation_authorized
        or policy.scheduler_installed
        or policy.scheduler_enabled
    ):
        raise ValueError("scheduler checkout/deployment safety invariants invalid")
    return policy


def inspect_ops_scheduler_checkout(
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
    env: Mapping[str, str] | None = None,
    checkout_root: Path | None = None,
    release_commit: str | None = None,
    require_current_process_checkout: bool = False,
    runner: GitRunner = subprocess.run,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    policy = load_ops_scheduler_checkout_policy(policy_path)
    checked_env = dict(os.environ if env is None else env)
    observed = observed_at or datetime.now(tz=UTC)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    configured_root = checkout_root
    if configured_root is None:
        raw_root = checked_env.get(policy.checkout_root_name)
        configured_root = Path(raw_root) if raw_root else None
    configured_commit = release_commit or checked_env.get(policy.release_commit_name)
    raw_development_root = checked_env.get(policy.development_checkout_root_name)
    development_candidate = Path(raw_development_root) if raw_development_root else None
    development_root = (
        development_candidate.resolve()
        if development_candidate is not None
        else (None if require_current_process_checkout else project_root.resolve())
    )
    checks: list[dict[str, object]] = []

    def check(check_id: str, passed: bool, evidence: object) -> None:
        checks.append(
            {
                "check_id": check_id,
                "status": "PASS" if passed else "FAIL",
                "evidence": evidence,
            }
        )

    marker_value = checked_env.get(policy.scheduler_marker_name)
    check(
        "scheduler_marker",
        marker_value == policy.scheduler_marker_value,
        {
            "name": policy.scheduler_marker_name,
            "present": marker_value is not None,
            "value_matches": marker_value == policy.scheduler_marker_value,
        },
    )
    check(
        "checkout_root_configured",
        configured_root is not None and configured_root.is_absolute(),
        str(configured_root) if configured_root is not None else None,
    )
    resolved_root = configured_root.resolve() if configured_root is not None else None
    root_exists = resolved_root is not None and resolved_root.is_dir()
    check("checkout_root_exists", root_exists, str(resolved_root) if resolved_root else None)
    check(
        "development_checkout_root_configured",
        (
            development_root is not None
            and (
                development_candidate is None
                or development_candidate.is_absolute()
            )
        ),
        str(development_root) if development_root else None,
    )
    independent = (
        root_exists
        and development_root is not None
        and resolved_root != development_root
    )
    check(
        "independent_checkout",
        bool(independent),
        {
            "candidate": str(resolved_root) if resolved_root else None,
            "development": str(development_root) if development_root else None,
        },
    )
    commit_valid = isinstance(configured_commit, str) and bool(
        _COMMIT_PATTERN.fullmatch(configured_commit)
    )
    check("release_commit_format", commit_valid, configured_commit)

    head_commit: str | None = None
    remote: str | None = None
    dirty_paths: tuple[str, ...] = ()
    git_errors: list[str] = []
    if root_exists:
        head_commit, head_error = _git_text(
            runner,
            resolved_root,
            ("rev-parse", "HEAD"),
        )
        remote, remote_error = _git_text(
            runner,
            resolved_root,
            ("remote", "get-url", "origin"),
        )
        dirty_text, dirty_error = _git_text(
            runner,
            resolved_root,
            ("status", "--porcelain", "--untracked-files=all"),
        )
        for error in (head_error, remote_error, dirty_error):
            if error:
                git_errors.append(error)
        if dirty_text:
            dirty_paths = tuple(
                line[3:].strip() if len(line) > 3 else line.strip()
                for line in dirty_text.splitlines()
                if line.strip()
            )
    check("git_checkout_readable", root_exists and not git_errors, git_errors)
    check(
        "exact_release_commit",
        bool(commit_valid and head_commit == configured_commit),
        {"expected": configured_commit, "observed": head_commit},
    )
    check(
        "expected_remote",
        remote == policy.expected_remote,
        {"expected": policy.expected_remote, "observed": remote},
    )
    check(
        "clean_checkout",
        root_exists and not dirty_paths and not git_errors,
        {"dirty_path_count": len(dirty_paths), "dirty_paths": list(dirty_paths)},
    )
    current_checkout_matches = resolved_root == project_root.resolve() if root_exists else False
    check(
        "current_process_checkout",
        (not require_current_process_checkout) or current_checkout_matches,
        {
            "required": require_current_process_checkout,
            "matches": current_checkout_matches,
        },
    )
    failed = [str(item["check_id"]) for item in checks if item["status"] != "PASS"]
    return {
        "schema_version": "ops_scheduler_checkout_preflight.v1",
        "policy_id": policy.policy_id,
        "policy_version": policy.version,
        "policy_path": _relative_path(policy_path, project_root),
        "observed_at": observed.isoformat(),
        "status": "PASS" if not failed else "BLOCKED",
        "blocker_codes": [f"OPS_CHECKOUT_{item.upper()}" for item in failed],
        "checks": checks,
        "checkout_root": str(resolved_root) if resolved_root else None,
        "release_commit": configured_commit,
        "head_commit": head_commit,
        "unified_external_trigger": list(policy.unified_external_trigger),
        "scheduler_execution_ready": not failed,
        "activation_authorized": False,
        "scheduler_installed": False,
        "scheduler_enabled": False,
        "owner_deployment_required": True,
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }


def write_ops_scheduler_checkout_preflight(
    payload: Mapping[str, object],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    write_json_atomic(json_path, dict(payload))
    write_markdown_atomic(markdown_path, _preflight_markdown(payload))
    return json_path, markdown_path


def _git_text(
    runner: GitRunner,
    root: Path,
    args: Sequence[str],
) -> tuple[str | None, str | None]:
    try:
        result = runner(
            ("git", "-C", str(root), *args),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        return None, f"{type(exc).__name__}: {exc}"
    if result.returncode != 0:
        return None, (result.stderr or result.stdout or "git command failed").strip()
    return (result.stdout or "").strip(), None


def _preflight_markdown(payload: Mapping[str, object]) -> str:
    lines = [
        "# Ops Scheduler Checkout Preflight",
        "",
        f"- 状态：`{payload['status']}`",
        f"- checkout：`{payload.get('checkout_root') or 'MISSING'}`",
        f"- release commit：`{payload.get('release_commit') or 'MISSING'}`",
        f"- scheduler execution ready：`{str(payload['scheduler_execution_ready']).lower()}`",
        "- activation authorized：`false`（等待 owner deployment）",
        "- production_effect：`none`",
        "",
        "| check | status |",
        "|---|---|",
    ]
    for item in payload["checks"]:
        lines.append(f"| {item['check_id']} | {item['status']} |")
    lines.extend(
        [
            "",
            "Preflight PASS 不安装或启用 scheduler；owner 必须独立完成 checkout、release pin、",
            "credential scope 与系统 scheduler deployment。",
            "",
        ]
    )
    return "\n".join(lines)


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"ops scheduler checkout policy {field} must be a mapping")
    return value


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"ops scheduler checkout policy {field} must be non-empty text")
    return value


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"ops scheduler checkout policy {field} must be boolean")
    return value


def _relative_path(path: Path, project_root: Path) -> str:
    try:
        return path.resolve().relative_to(project_root.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


__all__ = [
    "DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH",
    "OpsSchedulerCheckoutPolicy",
    "inspect_ops_scheduler_checkout",
    "load_ops_scheduler_checkout_policy",
    "write_ops_scheduler_checkout_preflight",
]
