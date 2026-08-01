from __future__ import annotations

import json
import os
import re
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.ops_release_promotion import (
    OpsReleasePromotionError,
    load_ops_release_promotion_policy,
    validate_ops_deployment_acceptance,
)
from ai_trading_system.platform.architecture.checkout_guard import (
    CheckoutGuardError,
    collect_checkout_dirty_paths,
    load_checkout_guard_policy,
)
from ai_trading_system.platform.artifacts import write_json_atomic, write_markdown_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "ops_scheduler_checkout.yaml"
)
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class OpsSchedulerTerminalDisposition(StrEnum):
    RECOVERABLE_SAME_AS_OF_TAIL = "RECOVERABLE_SAME_AS_OF_TAIL"
    WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY = "WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY"
    READY_FOR_NEW_AS_OF_ORDINARY = "READY_FOR_NEW_AS_OF_ORDINARY"
    BLOCKED_EXTERNAL_OR_OWNER = "BLOCKED_EXTERNAL_OR_OWNER"


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
    deployment_receipt_name: str
    runtime_python_name: str
    expected_remote: str
    reviewed_remote_ref: str
    exact_release_commit_required: bool
    clean_checkout_required: bool
    independent_from_development_checkout: bool
    independent_git_common_dir_required: bool
    current_process_must_run_from_ops_checkout: bool
    unified_external_trigger: tuple[str, ...]
    manual_execution_option: str
    separate_periodic_scheduler_entries_allowed: bool
    terminal_dispositions: tuple[str, ...]
    same_as_of_ordinary_allowed: bool
    ordinary_requires_as_of_strictly_after_parent: bool
    ordinary_requires_fresh_idempotency_key: bool
    ordinary_requires_no_active_lock: bool
    nonrecoverable_parent_recovery_allowed: bool
    parent_bytes_must_remain_immutable: bool
    new_business_scheduler_entry_allowed: bool
    activation_mode: str
    acceptance_schema: str
    active_receipt_relative_path: str
    runtime_python_must_be_below_checkout: bool
    imported_package_must_be_below_checkout: bool
    scheduler_id: str
    scheduler_entry_count: int
    scheduler_provider: str
    windows_task_scheduler_entries_allowed: bool


GitRunner = Callable[..., subprocess.CompletedProcess[str]]


def load_ops_scheduler_checkout_policy(
    path: Path = DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
) -> OpsSchedulerCheckoutPolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise ValueError("ops scheduler checkout policy must be a mapping")
    if payload.get("schema_version") != "ops_scheduler_checkout_policy.v3":
        raise ValueError("unsupported ops scheduler checkout policy schema")
    if payload.get("status") != "REVIEWED_RECEIPT_GATED_DEPLOYMENT":
        raise ValueError("ops scheduler checkout policy must remain receipt gated")
    environment = _mapping(payload.get("environment"), "environment")
    repository = _mapping(payload.get("repository"), "repository")
    trigger = _mapping(payload.get("trigger"), "trigger")
    terminal_routing = _mapping(payload.get("terminal_parent_routing"), "terminal_parent_routing")
    deployment = _mapping(payload.get("deployment"), "deployment")
    safety = _mapping(payload.get("safety"), "safety")
    trigger_command = trigger.get("unified_external_trigger")
    if not isinstance(trigger_command, list) or not all(
        isinstance(item, str) and item for item in trigger_command
    ):
        raise ValueError("unified_external_trigger must be a non-empty string list")
    if tuple(trigger_command) != ("aits", "ops", "daily-run"):
        raise ValueError("aits ops daily-run must remain the unified external trigger")
    if terminal_routing.get("schema_version") != "ops_scheduler_terminal_disposition_policy.v1":
        raise ValueError("unsupported terminal-parent disposition policy schema")
    raw_dispositions = terminal_routing.get("dispositions")
    if not isinstance(raw_dispositions, list) or not all(
        isinstance(item, str) and item for item in raw_dispositions
    ):
        raise ValueError("terminal dispositions must be a non-empty string list")
    expected_dispositions = tuple(item.value for item in OpsSchedulerTerminalDisposition)
    if tuple(raw_dispositions) != expected_dispositions:
        raise ValueError("terminal dispositions must match the reviewed routing set")
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
        deployment_receipt_name=_text(
            environment.get("deployment_receipt_name"),
            "deployment_receipt_name",
        ),
        runtime_python_name=_text(
            environment.get("runtime_python_name"),
            "runtime_python_name",
        ),
        expected_remote=_text(repository.get("expected_remote"), "expected_remote"),
        reviewed_remote_ref=_text(
            repository.get("reviewed_remote_ref"),
            "reviewed_remote_ref",
        ),
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
        independent_git_common_dir_required=_bool(
            repository.get("independent_git_common_dir_required"),
            "independent_git_common_dir_required",
        ),
        current_process_must_run_from_ops_checkout=_bool(
            repository.get("current_process_must_run_from_ops_checkout"),
            "current_process_must_run_from_ops_checkout",
        ),
        unified_external_trigger=tuple(trigger_command),
        manual_execution_option=_text(
            trigger.get("manual_execution_option"),
            "manual_execution_option",
        ),
        separate_periodic_scheduler_entries_allowed=_bool(
            trigger.get("separate_periodic_scheduler_entries_allowed"),
            "separate_periodic_scheduler_entries_allowed",
        ),
        terminal_dispositions=tuple(raw_dispositions),
        same_as_of_ordinary_allowed=_bool(
            terminal_routing.get("same_as_of_ordinary_allowed"),
            "same_as_of_ordinary_allowed",
        ),
        ordinary_requires_as_of_strictly_after_parent=_bool(
            terminal_routing.get("ordinary_requires_as_of_strictly_after_parent"),
            "ordinary_requires_as_of_strictly_after_parent",
        ),
        ordinary_requires_fresh_idempotency_key=_bool(
            terminal_routing.get("ordinary_requires_fresh_idempotency_key"),
            "ordinary_requires_fresh_idempotency_key",
        ),
        ordinary_requires_no_active_lock=_bool(
            terminal_routing.get("ordinary_requires_no_active_lock"),
            "ordinary_requires_no_active_lock",
        ),
        nonrecoverable_parent_recovery_allowed=_bool(
            terminal_routing.get("nonrecoverable_parent_recovery_allowed"),
            "nonrecoverable_parent_recovery_allowed",
        ),
        parent_bytes_must_remain_immutable=_bool(
            terminal_routing.get("parent_bytes_must_remain_immutable"),
            "parent_bytes_must_remain_immutable",
        ),
        new_business_scheduler_entry_allowed=_bool(
            terminal_routing.get("new_business_scheduler_entry_allowed"),
            "new_business_scheduler_entry_allowed",
        ),
        activation_mode=_text(
            deployment.get("activation_mode"),
            "activation_mode",
        ),
        acceptance_schema=_text(
            deployment.get("acceptance_schema"),
            "acceptance_schema",
        ),
        active_receipt_relative_path=_relative_text(
            deployment.get("active_receipt_relative_path"),
            "active_receipt_relative_path",
        ),
        runtime_python_must_be_below_checkout=_bool(
            deployment.get("runtime_python_must_be_below_checkout"),
            "runtime_python_must_be_below_checkout",
        ),
        imported_package_must_be_below_checkout=_bool(
            deployment.get("imported_package_must_be_below_checkout"),
            "imported_package_must_be_below_checkout",
        ),
        scheduler_id=_text(deployment.get("scheduler_id"), "scheduler_id"),
        scheduler_entry_count=_positive_int(
            deployment.get("scheduler_entry_count"),
            "scheduler_entry_count",
        ),
        scheduler_provider=_text(
            deployment.get("scheduler_provider"),
            "scheduler_provider",
        ),
        windows_task_scheduler_entries_allowed=_bool(
            deployment.get("windows_task_scheduler_entries_allowed"),
            "windows_task_scheduler_entries_allowed",
        ),
    )
    if (
        not policy.exact_release_commit_required
        or not policy.clean_checkout_required
        or not policy.independent_from_development_checkout
        or not policy.independent_git_common_dir_required
        or not policy.current_process_must_run_from_ops_checkout
        or policy.manual_execution_option != "--manual-execution"
        or policy.separate_periodic_scheduler_entries_allowed
        or policy.same_as_of_ordinary_allowed
        or not policy.ordinary_requires_as_of_strictly_after_parent
        or not policy.ordinary_requires_fresh_idempotency_key
        or not policy.ordinary_requires_no_active_lock
        or policy.nonrecoverable_parent_recovery_allowed
        or not policy.parent_bytes_must_remain_immutable
        or policy.new_business_scheduler_entry_allowed
        or policy.activation_mode != "ACTIVE_OWNER_ACCEPTED_RECEIPT_REQUIRED"
        or policy.acceptance_schema != "ops_deployment_acceptance.v1"
        or not policy.runtime_python_must_be_below_checkout
        or not policy.imported_package_must_be_below_checkout
        or policy.scheduler_entry_count != 1
        or policy.scheduler_provider != "codex_automation"
        or policy.windows_task_scheduler_entries_allowed
    ):
        raise ValueError("scheduler checkout/deployment safety invariants invalid")
    return policy


def resolve_ops_scheduler_terminal_disposition(
    *,
    parent_status: str,
    parent_as_of: date,
    resolved_as_of: date,
    recovery_eligible: bool,
    fresh_state_exists: bool,
    fresh_lock_exists: bool,
    active_deployment_accepted: bool,
    external_or_owner_blocked: bool = False,
    policy: OpsSchedulerCheckoutPolicy | None = None,
) -> dict[str, object]:
    """Resolve scheduler routing without executing or mutating the daily workflow."""

    checked_policy = policy or load_ops_scheduler_checkout_policy()
    if parent_status not in {"FAILED", "BLOCKED"}:
        raise ValueError("terminal parent status must be FAILED or BLOCKED")

    disposition: OpsSchedulerTerminalDisposition
    reasons: list[str]
    trigger_mode = "NONE"
    trigger_allowed = False
    recovery_arguments_required = False

    if external_or_owner_blocked:
        disposition = OpsSchedulerTerminalDisposition.BLOCKED_EXTERNAL_OR_OWNER
        reasons = ["EXTERNAL_OR_OWNER_BLOCKED"]
    elif resolved_as_of < parent_as_of:
        disposition = OpsSchedulerTerminalDisposition.BLOCKED_EXTERNAL_OR_OWNER
        reasons = ["RESOLVED_AS_OF_PRECEDES_PARENT"]
    elif resolved_as_of == parent_as_of:
        if recovery_eligible:
            disposition = OpsSchedulerTerminalDisposition.RECOVERABLE_SAME_AS_OF_TAIL
            reasons = ["SAME_AS_OF_RECOVERY_ELIGIBLE"]
            trigger_mode = "RECOVERY"
            trigger_allowed = True
            recovery_arguments_required = True
        else:
            disposition = (
                OpsSchedulerTerminalDisposition.WAIT_FOR_NEXT_PROVIDER_READY_AS_OF_ORDINARY
            )
            reasons = ["RECOVERY_BOUNDARY_NOT_ELIGIBLE_WAIT_FOR_LATER_AS_OF"]
    else:
        reasons = []
        if fresh_state_exists:
            reasons.append("FRESH_IDEMPOTENCY_KEY_STATE_ALREADY_EXISTS")
        if fresh_lock_exists:
            reasons.append("FRESH_IDEMPOTENCY_KEY_ACTIVE_LOCK_EXISTS")
        if not active_deployment_accepted:
            reasons.append("ACTIVE_DEPLOYMENT_NOT_ACCEPTED")
        if reasons:
            disposition = OpsSchedulerTerminalDisposition.BLOCKED_EXTERNAL_OR_OWNER
        else:
            disposition = OpsSchedulerTerminalDisposition.READY_FOR_NEW_AS_OF_ORDINARY
            reasons = ["NEW_PROVIDER_READY_AS_OF_ORDINARY_ACCEPTANCE_READY"]
            trigger_mode = "ORDINARY"
            trigger_allowed = True

    return {
        "schema_version": "ops_scheduler_terminal_disposition.v1",
        "disposition": disposition.value,
        "parent_status": parent_status,
        "parent_as_of": parent_as_of.isoformat(),
        "resolved_as_of": resolved_as_of.isoformat(),
        "reason_codes": reasons,
        "trigger_allowed": trigger_allowed,
        "trigger_mode": trigger_mode,
        "recovery_arguments_required": recovery_arguments_required,
        "same_as_of_ordinary_allowed": checked_policy.same_as_of_ordinary_allowed,
        "parent_bytes_must_remain_immutable": (checked_policy.parent_bytes_must_remain_immutable),
        "scheduler_entry_count": checked_policy.scheduler_entry_count,
        "unified_external_trigger": list(checked_policy.unified_external_trigger),
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }


def inspect_ops_scheduler_checkout(
    *,
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_OPS_SCHEDULER_CHECKOUT_POLICY_PATH,
    env: Mapping[str, str] | None = None,
    checkout_root: Path | None = None,
    release_commit: str | None = None,
    deployment_receipt_path: Path | None = None,
    runtime_python: Path | None = None,
    require_current_process_checkout: bool = False,
    require_active_deployment: bool = False,
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
    configured_receipt = deployment_receipt_path
    if configured_receipt is None:
        raw_receipt = checked_env.get(policy.deployment_receipt_name)
        configured_receipt = Path(raw_receipt) if raw_receipt else None
    configured_python = runtime_python
    if configured_python is None:
        raw_python = checked_env.get(policy.runtime_python_name)
        configured_python = Path(raw_python) if raw_python else None
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
            and (development_candidate is None or development_candidate.is_absolute())
        ),
        str(development_root) if development_root else None,
    )
    independent = root_exists and development_root is not None and resolved_root != development_root
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
    remote_commit: str | None = None
    ops_git_common_dir: str | None = None
    development_git_common_dir: str | None = None
    dirty_paths: tuple[str, ...] = ()
    git_errors: list[str] = []
    if root_exists:
        assert resolved_root is not None
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
        remote_commit, remote_ref_error = _git_text(
            runner,
            resolved_root,
            ("rev-parse", policy.reviewed_remote_ref),
        )
        ops_git_common_dir, ops_common_error = _git_text(
            runner,
            resolved_root,
            ("rev-parse", "--path-format=absolute", "--git-common-dir"),
        )
        if development_root is not None and development_root.is_dir():
            development_git_common_dir, development_common_error = _git_text(
                runner,
                development_root,
                ("rev-parse", "--path-format=absolute", "--git-common-dir"),
            )
        else:
            development_common_error = "development checkout root unavailable"
        try:
            guard_policy = load_checkout_guard_policy(
                resolved_root / "config" / "architecture" / "arch_005_s4d_checkout_guard.yaml"
            )
            exclusions = tuple(item.path for item in guard_policy.known_unrelated_exclusions)
            dirty_paths = collect_checkout_dirty_paths(
                resolved_root,
                exclusions=exclusions,
            )
            dirty_error = None
        except (OSError, ValueError, CheckoutGuardError) as exc:
            dirty_error = f"{type(exc).__name__}: {exc}"
        for error in (
            head_error,
            remote_error,
            remote_ref_error,
            ops_common_error,
            development_common_error,
            dirty_error,
        ):
            if error:
                git_errors.append(error)
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
        "reviewed_remote_ref",
        bool(commit_valid and remote_commit == configured_commit),
        {
            "ref": policy.reviewed_remote_ref,
            "expected": configured_commit,
            "observed": remote_commit,
        },
    )
    common_dir_independent = (
        ops_git_common_dir is not None
        and development_git_common_dir is not None
        and os.path.normcase(str(Path(ops_git_common_dir).resolve()))
        != os.path.normcase(str(Path(development_git_common_dir).resolve()))
    )
    check(
        "independent_git_common_dir",
        bool(common_dir_independent),
        {
            "ops": ops_git_common_dir,
            "development": development_git_common_dir,
        },
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
    receipt_payload: Mapping[str, object] | None = None
    receipt_error: str | None = None
    resolved_receipt = configured_receipt.resolve() if configured_receipt is not None else None
    resolved_python = configured_python.resolve() if configured_python is not None else None
    if require_active_deployment:
        try:
            if resolved_root is None or development_root is None:
                raise ValueError("checkout roots unavailable")
            if resolved_receipt is None or not resolved_receipt.is_file():
                raise ValueError("active deployment receipt missing")
            expected_receipt = (resolved_root / policy.active_receipt_relative_path).resolve()
            if resolved_receipt != expected_receipt:
                raise ValueError(f"active deployment receipt path mismatch: {resolved_receipt}")
            if resolved_python is None:
                raise ValueError("runtime python missing")
            loaded = json.loads(resolved_receipt.read_text(encoding="utf-8"))
            if not isinstance(loaded, Mapping):
                raise ValueError("active deployment receipt must be mapping")
            receipt_payload = loaded
            promotion_policy = load_ops_release_promotion_policy(
                resolved_root / "config" / "operations" / "ops_release_promotion.yaml"
            )
            validate_ops_deployment_acceptance(
                receipt_payload,
                policy=promotion_policy,
                runtime_root=resolved_root,
                development_root=development_root,
                runtime_python=resolved_python,
                verify_live_runtime=True,
            )
            release = receipt_payload.get("release")
            if (
                not isinstance(release, Mapping)
                or release.get("candidate_commit") != configured_commit
            ):
                raise ValueError("deployment receipt release mismatch")
        except (
            OSError,
            ValueError,
            json.JSONDecodeError,
            OpsReleasePromotionError,
        ) as exc:
            receipt_error = f"{type(exc).__name__}: {exc}"
    check(
        "active_deployment_receipt",
        (not require_active_deployment) or receipt_error is None,
        {
            "required": require_active_deployment,
            "path": str(resolved_receipt) if resolved_receipt else None,
            "deployment_id": (
                receipt_payload.get("deployment_id") if receipt_payload is not None else None
            ),
            "error": receipt_error,
        },
    )
    failed = [str(item["check_id"]) for item in checks if item["status"] != "PASS"]
    active = require_active_deployment and receipt_error is None and receipt_payload is not None
    return {
        "schema_version": "ops_scheduler_checkout_preflight.v2",
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
        "reviewed_remote_commit": remote_commit,
        "git_common_dir": ops_git_common_dir,
        "development_git_common_dir": development_git_common_dir,
        "unified_external_trigger": list(policy.unified_external_trigger),
        "scheduler_execution_ready": not failed,
        "active_deployment_required": require_active_deployment,
        "deployment_receipt": str(resolved_receipt) if resolved_receipt else None,
        "deployment_id": (
            receipt_payload.get("deployment_id") if receipt_payload is not None else None
        ),
        "runtime_python": str(resolved_python) if resolved_python else None,
        "activation_authorized": active,
        "scheduler_installed": active,
        "scheduler_enabled": active,
        "owner_deployment_required": not active,
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
        f"- active deployment required：`{str(payload['active_deployment_required']).lower()}`",
        f"- activation authorized：`{str(payload['activation_authorized']).lower()}`",
        f"- deployment id：`{payload.get('deployment_id') or 'MISSING'}`",
        "- production_effect：`none`",
        "",
        "| check | status |",
        "|---|---|",
    ]
    raw_checks = payload.get("checks")
    if not isinstance(raw_checks, list):
        raise ValueError("ops scheduler checkout preflight checks must be list")
    for raw_item in raw_checks:
        if not isinstance(raw_item, Mapping):
            raise ValueError("ops scheduler checkout preflight check must be mapping")
        item = raw_item
        lines.append(f"| {item['check_id']} | {item['status']} |")
    lines.extend(
        [
            "",
            "Candidate-only PASS 不代表 scheduler 已启用；scheduler mode 只有 active owner-",
            "accepted deployment receipt 与全部 live identity checks PASS 才可继续。",
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


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"ops scheduler checkout policy {field} must be positive integer")
    return value


def _relative_text(value: object, field: str) -> str:
    text = _text(value, field)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"ops scheduler checkout policy {field} must be relative")
    return text


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
