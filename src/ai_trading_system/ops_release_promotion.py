from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.architecture.checkout_guard import CheckoutLeaseGuard
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH = (
    PROJECT_ROOT / "config" / "operations" / "ops_release_promotion.yaml"
)
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_RELEASE_CANDIDATE_SCHEMA = "ops_release_candidate.v1"
_DEPLOYMENT_ACCEPTANCE_SCHEMA = "ops_deployment_acceptance.v2"
_LEGACY_DEPLOYMENT_ACCEPTANCE_SCHEMA = "ops_deployment_acceptance.v1"
_PROMOTION_TRANSACTION_SCHEMA = "ops_release_promotion_transaction.v1"
_SCHEDULER_OBSERVATION_SCHEMA = "ops_scheduler_observation.v2"
_SCHEDULER_BINDING_SCHEMA = "ops_scheduler_binding.v2"
_PRE_RELEASE_CANARY_SCHEMA = "ops_release_preflight_canary.v1"
_INCIDENT_REGRESSION_RUNNER_SCHEMA = "pytest_incident_regression.v1"
# These node IDs are release invariants: each reproduces a real zero-provider
# incident class and must run on the exact candidate before promotion.
_INCIDENT_REGRESSION_TEST_NODES = {
    "risk_event_unknown_id_referential_integrity": (
        "tests/test_risk_event_llm_formal.py::"
        "test_llm_formal_unknown_risk_id_fails_before_any_output"
    ),
    "signed_eps_revision_90d_pct": (
        "tests/test_valuation.py::"
        "test_validate_valuation_snapshot_store_accepts_negative_eps_revision"
    ),
}
_REQUIRED_VALIDATION_TIERS = (
    "fast-unit",
    "architecture-fitness",
    "contract-validation",
    "integration",
    "reproducibility",
    "full",
)
_REQUIRED_CRITICAL_PATHS = (
    "config/architecture/arch_005_parallel_control_policy.yaml",
    "config/architecture/arch_005_s4d_checkout_guard.yaml",
    "config/operations/ops_release_promotion.yaml",
    "config/operations/ops_scheduler_checkout.yaml",
    "config/operations/aitradingsystem_pit_automation_prompt.md",
    "docs/operations/operations_runbook.md",
    "pyproject.toml",
    "src/ai_trading_system/cli_commands/ops.py",
    "src/ai_trading_system/ops_release_promotion.py",
    "src/ai_trading_system/ops_scheduler_checkout.py",
)
_RUNTIME_GIT_EXCLUDE_PATTERNS = (
    "/outputs/",
    "/artifacts/",
    "/data/derived/",
)
_RUNTIME_GIT_EXCLUDE_HEADER = (
    "# AITradingSystem managed ops runtime exclusions: ops_release_promotion_v1"
)
_EXPECTED_SCHEDULER_INVOCATION_WINDOWS = (
    ("PRIMARY", 9, 30),
    ("SAME_DAY_RESCUE", 17, 30),
)


class OpsReleasePromotionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class OpsReleasePromotionPolicy:
    policy_id: str
    version: str
    status: str
    expected_remote: str
    reviewed_remote_ref: str
    candidate_must_equal_reviewed_remote_ref: bool
    independent_git_common_dir_required: bool
    active_receipt_relative_path: str
    history_relative_path: str
    transaction_relative_path: str
    evidence_relative_path: str
    required_validation_tiers: tuple[str, ...]
    required_critical_paths: tuple[str, ...]
    lock_relative_path: str
    active_daily_lease_required_count: int
    pre_switch_checkout_policy_source: str
    automatic_latest_selection: bool
    automatic_stash_clean_reset: bool
    previous_release_retained: bool
    runtime_python_relative_path: str
    package_module: str
    distribution_name: str
    installed_distribution_inventory_required: bool
    git_exclude_managed: bool
    git_exclude_patterns: tuple[str, ...]
    executable_must_be_below_checkout: bool
    imported_package_must_be_below_checkout: bool
    scheduler_provider: str
    scheduler_id: str
    scheduler_entry_count: int
    unified_external_trigger: tuple[str, ...]
    windows_task_scheduler_entries_allowed: bool
    required_environment_names: tuple[str, ...]
    legacy_deployment_acceptance_schema: str
    legacy_active_release_commits: tuple[str, ...]
    release_identity_authority: str
    legacy_release_assertion_name: str
    legacy_release_assertion_mode: str
    scheduler_observation_schema: str
    canonical_prompt_relative_path: str
    expected_scheduler_status: str
    expected_scheduler_rrule: str
    expected_scheduler_model: str
    expected_scheduler_reasoning_effort: str
    expected_scheduler_execution_environment: str
    expected_scheduler_target_type: str
    expected_scheduler_cwds: tuple[str, ...]
    scheduler_carrier_mode: str
    scheduler_local_timezone: str
    scheduler_invocation_windows: tuple[tuple[str, int, int], ...]
    scheduler_per_invocation_business_trigger_max: int
    scheduler_same_entry_for_all_windows: bool
    observation_must_not_predate_promotion: bool
    pre_release_canary_schema: str
    pre_release_canary_runner_schema: str
    pre_release_canary_required: bool
    pre_release_canary_provider_request_performed: bool
    pre_release_canary_required_scenarios: tuple[str, ...]
    allowed_secret_names: tuple[str, ...]
    required_credential_groups: tuple[tuple[str, ...], ...]
    forbidden_name_patterns: tuple[str, ...]


def load_ops_release_promotion_policy(
    path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
) -> OpsReleasePromotionPolicy:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise OpsReleasePromotionError("PROMOTION_POLICY_INVALID", "payload must be mapping")
    if payload.get("schema_version") != "ops_release_promotion_policy.v2":
        raise OpsReleasePromotionError(
            "PROMOTION_POLICY_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "REVIEWED_OWNER_GATED":
        raise OpsReleasePromotionError("PROMOTION_POLICY_STATUS", str(payload.get("status")))
    repository = _mapping(payload.get("repository"), "repository")
    receipts = _mapping(payload.get("receipts"), "receipts")
    promotion = _mapping(payload.get("promotion"), "promotion")
    runtime = _mapping(payload.get("runtime"), "runtime")
    scheduler = _mapping(payload.get("scheduler"), "scheduler")
    canary = _mapping(payload.get("pre_release_canary"), "pre_release_canary")
    credentials = _mapping(payload.get("credentials"), "credentials")
    safety = _mapping(payload.get("safety"), "safety")
    _require_exact_mapping(
        safety,
        {
            "production_effect": "none",
            "production_weight_write": False,
            "active_shadow_weight_write": False,
            "broker_action": False,
            "trading_action": False,
        },
        "safety",
    )
    schemas = {
        "release_candidate_schema": _RELEASE_CANDIDATE_SCHEMA,
        "deployment_acceptance_schema": _DEPLOYMENT_ACCEPTANCE_SCHEMA,
        "promotion_transaction_schema": _PROMOTION_TRANSACTION_SCHEMA,
    }
    for field, expected in schemas.items():
        if receipts.get(field) != expected:
            raise OpsReleasePromotionError("PROMOTION_POLICY_SCHEMA_BINDING", field)
    policy = OpsReleasePromotionPolicy(
        policy_id=_text(payload.get("policy_id"), "policy_id"),
        version=_text(payload.get("version"), "version"),
        status=_text(payload.get("status"), "status"),
        expected_remote=_text(repository.get("expected_remote"), "expected_remote"),
        reviewed_remote_ref=_text(
            repository.get("reviewed_remote_ref"),
            "reviewed_remote_ref",
        ),
        candidate_must_equal_reviewed_remote_ref=_bool(
            repository.get("candidate_must_equal_reviewed_remote_ref"),
            "candidate_must_equal_reviewed_remote_ref",
        ),
        independent_git_common_dir_required=_bool(
            repository.get("independent_git_common_dir_required"),
            "independent_git_common_dir_required",
        ),
        active_receipt_relative_path=_relative_policy_path(
            receipts.get("active_receipt_relative_path"),
            "active_receipt_relative_path",
        ),
        history_relative_path=_relative_policy_path(
            receipts.get("history_relative_path"),
            "history_relative_path",
        ),
        transaction_relative_path=_relative_policy_path(
            receipts.get("transaction_relative_path"),
            "transaction_relative_path",
        ),
        evidence_relative_path=_relative_policy_path(
            receipts.get("evidence_relative_path"),
            "evidence_relative_path",
        ),
        required_validation_tiers=_text_tuple(
            receipts.get("required_validation_tiers"),
            "required_validation_tiers",
        ),
        required_critical_paths=_text_tuple(
            receipts.get("required_critical_paths"),
            "required_critical_paths",
        ),
        lock_relative_path=_relative_policy_path(
            promotion.get("lock_relative_path"),
            "lock_relative_path",
        ),
        active_daily_lease_required_count=_nonnegative_int(
            promotion.get("active_daily_lease_required_count"),
            "active_daily_lease_required_count",
        ),
        pre_switch_checkout_policy_source=_text(
            promotion.get("pre_switch_checkout_policy_source"),
            "pre_switch_checkout_policy_source",
        ),
        automatic_latest_selection=_bool(
            promotion.get("automatic_latest_selection"),
            "automatic_latest_selection",
        ),
        automatic_stash_clean_reset=_bool(
            promotion.get("automatic_stash_clean_reset"),
            "automatic_stash_clean_reset",
        ),
        previous_release_retained=_bool(
            promotion.get("previous_release_retained"),
            "previous_release_retained",
        ),
        runtime_python_relative_path=_relative_policy_path(
            runtime.get("python_relative_path"),
            "python_relative_path",
        ),
        package_module=_text(runtime.get("package_module"), "package_module"),
        distribution_name=_text(
            runtime.get("distribution_name"),
            "distribution_name",
        ),
        installed_distribution_inventory_required=_bool(
            runtime.get("installed_distribution_inventory_required"),
            "installed_distribution_inventory_required",
        ),
        git_exclude_managed=_bool(
            runtime.get("git_exclude_managed"),
            "git_exclude_managed",
        ),
        git_exclude_patterns=_text_tuple(
            runtime.get("git_exclude_patterns"),
            "git_exclude_patterns",
        ),
        executable_must_be_below_checkout=_bool(
            runtime.get("executable_must_be_below_checkout"),
            "executable_must_be_below_checkout",
        ),
        imported_package_must_be_below_checkout=_bool(
            runtime.get("imported_package_must_be_below_checkout"),
            "imported_package_must_be_below_checkout",
        ),
        scheduler_provider=_text(scheduler.get("provider"), "scheduler.provider"),
        scheduler_id=_text(scheduler.get("scheduler_id"), "scheduler.scheduler_id"),
        scheduler_entry_count=_positive_int(
            scheduler.get("scheduler_entry_count"),
            "scheduler.scheduler_entry_count",
        ),
        unified_external_trigger=_text_tuple(
            scheduler.get("unified_external_trigger"),
            "unified_external_trigger",
        ),
        windows_task_scheduler_entries_allowed=_bool(
            scheduler.get("windows_task_scheduler_entries_allowed"),
            "windows_task_scheduler_entries_allowed",
        ),
        required_environment_names=_text_tuple(
            scheduler.get("required_environment_names"),
            "required_environment_names",
        ),
        legacy_deployment_acceptance_schema=_text(
            receipts.get("legacy_deployment_acceptance_schema"),
            "legacy_deployment_acceptance_schema",
        ),
        legacy_active_release_commits=_text_tuple(
            receipts.get("legacy_active_release_commits"),
            "legacy_active_release_commits",
        ),
        release_identity_authority=_text(
            scheduler.get("release_identity_authority"),
            "release_identity_authority",
        ),
        legacy_release_assertion_name=_text(
            scheduler.get("legacy_release_assertion_name"),
            "legacy_release_assertion_name",
        ),
        legacy_release_assertion_mode=_text(
            scheduler.get("legacy_release_assertion_mode"),
            "legacy_release_assertion_mode",
        ),
        scheduler_observation_schema=_text(
            scheduler.get("observation_schema"),
            "observation_schema",
        ),
        canonical_prompt_relative_path=_relative_policy_path(
            scheduler.get("canonical_prompt_relative_path"),
            "canonical_prompt_relative_path",
        ),
        expected_scheduler_status=_text(
            scheduler.get("expected_status"),
            "expected_status",
        ),
        expected_scheduler_rrule=_text(
            scheduler.get("expected_rrule"),
            "expected_rrule",
        ),
        expected_scheduler_model=_text(
            scheduler.get("expected_model"),
            "expected_model",
        ),
        expected_scheduler_reasoning_effort=_text(
            scheduler.get("expected_reasoning_effort"),
            "expected_reasoning_effort",
        ),
        expected_scheduler_execution_environment=_text(
            scheduler.get("expected_execution_environment"),
            "expected_execution_environment",
        ),
        expected_scheduler_target_type=_text(
            scheduler.get("expected_target_type"),
            "expected_target_type",
        ),
        expected_scheduler_cwds=_text_tuple(
            scheduler.get("expected_cwds"),
            "expected_cwds",
        ),
        scheduler_carrier_mode=_text(
            scheduler.get("carrier_mode"),
            "carrier_mode",
        ),
        scheduler_local_timezone=_text(
            scheduler.get("local_timezone"),
            "local_timezone",
        ),
        scheduler_invocation_windows=_scheduler_invocation_windows(
            scheduler.get("invocation_windows"),
            "invocation_windows",
        ),
        scheduler_per_invocation_business_trigger_max=_positive_int(
            scheduler.get("per_invocation_business_trigger_max"),
            "per_invocation_business_trigger_max",
        ),
        scheduler_same_entry_for_all_windows=_bool(
            scheduler.get("same_scheduler_entry_for_all_windows"),
            "same_scheduler_entry_for_all_windows",
        ),
        observation_must_not_predate_promotion=_bool(
            scheduler.get("observation_must_not_predate_promotion"),
            "observation_must_not_predate_promotion",
        ),
        pre_release_canary_schema=_text(
            canary.get("schema_version"),
            "pre_release_canary.schema_version",
        ),
        pre_release_canary_runner_schema=_text(
            canary.get("runner_schema_version"),
            "pre_release_canary.runner_schema_version",
        ),
        pre_release_canary_required=_bool(
            canary.get("required"),
            "pre_release_canary.required",
        ),
        pre_release_canary_provider_request_performed=_bool(
            canary.get("provider_request_performed"),
            "pre_release_canary.provider_request_performed",
        ),
        pre_release_canary_required_scenarios=_text_tuple(
            canary.get("required_scenarios"),
            "pre_release_canary.required_scenarios",
        ),
        allowed_secret_names=_text_tuple(
            credentials.get("allowed_secret_names"),
            "allowed_secret_names",
        ),
        required_credential_groups=_text_tuple_groups(
            credentials.get("required_credential_groups"),
            "required_credential_groups",
        ),
        forbidden_name_patterns=_text_tuple(
            credentials.get("forbidden_name_patterns"),
            "forbidden_name_patterns",
        ),
    )
    if (
        not policy.candidate_must_equal_reviewed_remote_ref
        or not policy.independent_git_common_dir_required
        or policy.automatic_latest_selection
        or policy.automatic_stash_clean_reset
        or not policy.previous_release_retained
        or policy.pre_switch_checkout_policy_source != "coordinator_candidate"
        or policy.required_validation_tiers != _REQUIRED_VALIDATION_TIERS
        or policy.required_critical_paths != _REQUIRED_CRITICAL_PATHS
        or policy.legacy_deployment_acceptance_schema != _LEGACY_DEPLOYMENT_ACCEPTANCE_SCHEMA
        or any(not _COMMIT_PATTERN.fullmatch(row) for row in policy.legacy_active_release_commits)
        or not policy.installed_distribution_inventory_required
        or not policy.git_exclude_managed
        or policy.git_exclude_patterns != _RUNTIME_GIT_EXCLUDE_PATTERNS
        or not policy.executable_must_be_below_checkout
        or not policy.imported_package_must_be_below_checkout
        or policy.scheduler_entry_count != 1
        or policy.unified_external_trigger != ("aits", "ops", "daily-run")
        or policy.windows_task_scheduler_entries_allowed
        or policy.release_identity_authority != "active_deployment_receipt"
        or policy.legacy_release_assertion_name != "AITS_OPS_RELEASE_COMMIT"
        or policy.legacy_release_assertion_mode != "exact_match_if_present"
        or policy.scheduler_observation_schema != _SCHEDULER_OBSERVATION_SCHEMA
        or policy.expected_scheduler_status != "ACTIVE"
        or policy.expected_scheduler_execution_environment != "local"
        or policy.expected_scheduler_target_type != "projectless"
        or policy.expected_scheduler_cwds != ("~",)
        or policy.scheduler_carrier_mode != "PROJECTLESS_ISOLATED"
        or policy.scheduler_local_timezone != "Asia/Tokyo"
        or policy.scheduler_invocation_windows != _EXPECTED_SCHEDULER_INVOCATION_WINDOWS
        or policy.scheduler_per_invocation_business_trigger_max != 1
        or not policy.scheduler_same_entry_for_all_windows
        or not policy.observation_must_not_predate_promotion
        or policy.pre_release_canary_schema != _PRE_RELEASE_CANARY_SCHEMA
        or policy.pre_release_canary_runner_schema != _INCIDENT_REGRESSION_RUNNER_SCHEMA
        or not policy.pre_release_canary_required
        or policy.pre_release_canary_provider_request_performed
        or set(policy.pre_release_canary_required_scenarios) != set(_INCIDENT_REGRESSION_TEST_NODES)
    ):
        raise OpsReleasePromotionError(
            "PROMOTION_POLICY_SAFETY_INVARIANT",
            policy.policy_id,
        )
    return policy


def build_ops_release_candidate(
    *,
    project_root: Path,
    candidate_commit: str,
    validation_artifact_paths: Sequence[Path],
    critical_path_commitments: Sequence[Path],
    pre_release_canary_path: Path | None = None,
    owner_decision_ref: str,
    previous_release_commit: str | None = None,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    policy = load_ops_release_promotion_policy(policy_path)
    root = project_root.resolve()
    timestamp = _aware(observed_at)
    _require_commit(candidate_commit, "candidate_commit")
    if previous_release_commit is not None:
        _require_commit(previous_release_commit, "previous_release_commit")
    if not owner_decision_ref.startswith("owner_decision:"):
        raise OpsReleasePromotionError(
            "RELEASE_OWNER_DECISION_INVALID",
            owner_decision_ref,
        )
    head = _git_text(root, "rev-parse", "HEAD")
    remote = _git_text(root, "remote", "get-url", "origin")
    remote_commit = _git_text(root, "rev-parse", policy.reviewed_remote_ref)
    if remote != policy.expected_remote:
        raise OpsReleasePromotionError("RELEASE_REMOTE_MISMATCH", remote)
    if head != candidate_commit:
        raise OpsReleasePromotionError(
            "RELEASE_HEAD_MISMATCH",
            f"head={head};candidate={candidate_commit}",
        )
    if remote_commit != candidate_commit:
        raise OpsReleasePromotionError(
            "RELEASE_REMOTE_REF_MISMATCH",
            f"remote={remote_commit};candidate={candidate_commit}",
        )
    _git_is_ancestor(root, candidate_commit, policy.reviewed_remote_ref)
    dirty_paths = _governed_dirty_paths(root)
    if dirty_paths:
        raise OpsReleasePromotionError(
            "RELEASE_CHECKOUT_DIRTY",
            ",".join(dirty_paths),
        )
    validations = _file_commitments(
        validation_artifact_paths,
        root=root,
        require_validation_pass=True,
        expected_validation_commit=candidate_commit,
    )
    critical = _file_commitments(
        critical_path_commitments,
        root=root,
        require_validation_pass=False,
    )
    if not validations:
        raise OpsReleasePromotionError("RELEASE_VALIDATION_EVIDENCE_MISSING", "empty")
    _validate_required_validation_tiers(validations, policy.required_validation_tiers)
    if not critical:
        raise OpsReleasePromotionError("RELEASE_CRITICAL_COMMITMENTS_MISSING", "empty")
    _validate_required_critical_paths(critical, policy.required_critical_paths)
    selected_canary_path = pre_release_canary_path or (
        root / "outputs" / "operations" / "deployment" / "evidence" / "pre_release_canary.json"
    )
    canary_payload = _read_json_mapping(selected_canary_path)
    validate_ops_release_preflight_canary(
        canary_payload,
        policy=policy,
        expected_commit=candidate_commit,
        verify_live_artifacts=True,
        artifact_root=root,
    )
    canary_commitment = {
        **_portable_file_commitment(selected_canary_path.resolve(), root=root),
        "schema_version": canary_payload["schema_version"],
        "status": canary_payload["status"],
        "candidate_commit": canary_payload["candidate_commit"],
        "provider_request_performed": canary_payload["provider_request_performed"],
        "scenario_ids": [
            row["scenario_id"]
            for row in _mapping_rows(canary_payload.get("scenarios"), "canary.scenarios")
        ],
    }
    payload: dict[str, object] = {
        "schema_version": _RELEASE_CANDIDATE_SCHEMA,
        "status": "OWNER_APPROVED",
        "policy_id": policy.policy_id,
        "policy_version": policy.version,
        "owner_decision_ref": owner_decision_ref,
        "candidate_commit": candidate_commit,
        "previous_release_commit": previous_release_commit,
        "remote": {
            "url": remote,
            "reviewed_ref": policy.reviewed_remote_ref,
            "reviewed_ref_commit": remote_commit,
            "candidate_is_ancestor": True,
        },
        "validation_artifacts": validations,
        "critical_path_commitments": critical,
        "pre_release_canary": canary_commitment,
        "generated_at": timestamp.isoformat(),
        **_safety_boundary(),
    }
    payload["release_id"] = _content_id("ops_release_", payload, "release_id")
    validate_ops_release_candidate(
        payload,
        policy=policy,
        verify_live_artifacts=True,
        artifact_root=root,
    )
    return payload


def validate_ops_release_candidate(
    payload: Mapping[str, object],
    *,
    policy: OpsReleasePromotionPolicy | None = None,
    verify_live_artifacts: bool = False,
    artifact_root: Path | None = None,
) -> None:
    checked_policy = policy or load_ops_release_promotion_policy()
    if payload.get("schema_version") != _RELEASE_CANDIDATE_SCHEMA:
        raise OpsReleasePromotionError("RELEASE_RECEIPT_SCHEMA", str(payload.get("schema_version")))
    if payload.get("status") != "OWNER_APPROVED":
        raise OpsReleasePromotionError("RELEASE_RECEIPT_STATUS", str(payload.get("status")))
    if payload.get("policy_id") != checked_policy.policy_id:
        raise OpsReleasePromotionError("RELEASE_POLICY_ID", str(payload.get("policy_id")))
    if payload.get("policy_version") != checked_policy.version:
        raise OpsReleasePromotionError(
            "RELEASE_POLICY_VERSION",
            str(payload.get("policy_version")),
        )
    commit = _text(payload.get("candidate_commit"), "candidate_commit")
    _require_commit(commit, "candidate_commit")
    previous = payload.get("previous_release_commit")
    if previous is not None:
        _require_commit(_text(previous, "previous_release_commit"), "previous_release_commit")
    owner_ref = _text(payload.get("owner_decision_ref"), "owner_decision_ref")
    if not owner_ref.startswith("owner_decision:"):
        raise OpsReleasePromotionError("RELEASE_OWNER_DECISION_INVALID", owner_ref)
    remote = _mapping(payload.get("remote"), "remote")
    if remote.get("url") != checked_policy.expected_remote:
        raise OpsReleasePromotionError("RELEASE_REMOTE_MISMATCH", str(remote.get("url")))
    if remote.get("reviewed_ref") != checked_policy.reviewed_remote_ref:
        raise OpsReleasePromotionError(
            "RELEASE_REMOTE_REF_MISMATCH",
            str(remote.get("reviewed_ref")),
        )
    if (
        remote.get("reviewed_ref_commit") != commit
        or remote.get("candidate_is_ancestor") is not True
    ):
        raise OpsReleasePromotionError("RELEASE_REMOTE_BINDING_INVALID", commit)
    _validate_commitment_rows(
        payload.get("validation_artifacts"),
        field="validation_artifacts",
        verify_live=verify_live_artifacts,
        artifact_root=artifact_root,
        require_validation_pass=True,
        expected_validation_commit=commit,
    )
    _validate_required_validation_tiers(
        payload.get("validation_artifacts"),
        checked_policy.required_validation_tiers,
    )
    _validate_commitment_rows(
        payload.get("critical_path_commitments"),
        field="critical_path_commitments",
        verify_live=verify_live_artifacts,
        artifact_root=artifact_root,
        require_validation_pass=False,
    )
    _validate_required_critical_paths(
        payload.get("critical_path_commitments"),
        checked_policy.required_critical_paths,
    )
    canary_summary = _mapping(payload.get("pre_release_canary"), "pre_release_canary")
    _validate_portable_commitment_row(
        canary_summary,
        verify_live=verify_live_artifacts,
        artifact_root=artifact_root,
    )
    _validate_pre_release_canary_summary(canary_summary, checked_policy, commit)
    if verify_live_artifacts:
        if artifact_root is None:
            raise OpsReleasePromotionError("RELEASE_ARTIFACT_ROOT_REQUIRED", "pre_release_canary")
        canary_path = _resolve_portable_commitment_path(artifact_root, canary_summary)
        validate_ops_release_preflight_canary(
            _read_json_mapping(canary_path),
            policy=checked_policy,
            expected_commit=commit,
            verify_live_artifacts=True,
            artifact_root=artifact_root,
        )
    _require_safety_boundary(payload, "release")
    release_id = _text(payload.get("release_id"), "release_id")
    expected_id = _content_id("ops_release_", payload, "release_id")
    if release_id != expected_id:
        raise OpsReleasePromotionError(
            "RELEASE_RECEIPT_ID_MISMATCH",
            f"expected={expected_id};observed={release_id}",
        )


def build_ops_release_preflight_canary(
    *,
    candidate_commit: str,
    scenario_evidence_paths: Sequence[Path],
    project_root: Path = PROJECT_ROOT,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Aggregate exact, zero-provider incident regressions for release admission."""

    policy = load_ops_release_promotion_policy(policy_path)
    _require_commit(candidate_commit, "candidate_commit")
    root = project_root.resolve()
    scenarios: list[dict[str, object]] = []
    seen: set[str] = set()
    for path in scenario_evidence_paths:
        resolved = path.resolve()
        payload = _read_json_mapping(resolved)
        if payload.get("schema_version") != "ops_release_incident_regression.v1":
            raise OpsReleasePromotionError(
                "RELEASE_CANARY_SCENARIO_SCHEMA",
                str(payload.get("schema_version")),
            )
        scenario_id = _text(payload.get("scenario_id"), "scenario_id")
        if scenario_id in seen:
            raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_DUPLICATE", scenario_id)
        seen.add(scenario_id)
        expected_node = _INCIDENT_REGRESSION_TEST_NODES.get(scenario_id)
        if expected_node is None:
            raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_UNKNOWN", scenario_id)
        _validate_incident_regression_evidence(
            payload,
            scenario_id=scenario_id,
            candidate_commit=candidate_commit,
            expected_test_node=expected_node,
            policy=policy,
        )
        scenarios.append(
            {
                "scenario_id": scenario_id,
                "status": "PASS",
                "git_commit": candidate_commit,
                "provider_request_performed": False,
                "test_node_id": expected_node,
                "evidence": _portable_file_commitment(resolved, root=root),
            }
        )
    required = set(policy.pre_release_canary_required_scenarios)
    if seen != required:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_SCENARIO_SET_MISMATCH",
            f"expected={','.join(sorted(required))};observed={','.join(sorted(seen))}",
        )
    scenarios.sort(key=lambda row: str(row["scenario_id"]))
    result: dict[str, object] = {
        "schema_version": policy.pre_release_canary_schema,
        "status": "PASS",
        "candidate_commit": candidate_commit,
        "provider_request_performed": False,
        "scenarios": scenarios,
        "generated_at": _aware(observed_at).isoformat(),
        **_safety_boundary(),
    }
    result["canary_id"] = _content_id("ops_release_canary_", result, "canary_id")
    validate_ops_release_preflight_canary(
        result,
        policy=policy,
        expected_commit=candidate_commit,
        verify_live_artifacts=True,
        artifact_root=root,
    )
    return result


def run_ops_release_incident_regressions(
    *,
    candidate_commit: str,
    project_root: Path = PROJECT_ROOT,
    output_dir: Path | None = None,
    python_executable: Path | None = None,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> tuple[Path, ...]:
    """Run exact-candidate incident tests and write zero-provider audit evidence."""

    policy = load_ops_release_promotion_policy(policy_path)
    _require_commit(candidate_commit, "candidate_commit")
    root = project_root.resolve()
    head = _git_text(root, "rev-parse", "HEAD")
    if head != candidate_commit:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_HEAD_MISMATCH",
            f"head={head};candidate={candidate_commit}",
        )
    executable = (python_executable or Path(sys.executable)).resolve()
    if not executable.is_file():
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_PYTHON_MISSING",
            str(executable),
        )
    evidence_root = (
        output_dir.resolve()
        if output_dir is not None
        else root / "outputs" / "operations" / "deployment" / "evidence" / "scenarios"
    )
    evidence_root.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for scenario_id in policy.pre_release_canary_required_scenarios:
        test_node = _INCIDENT_REGRESSION_TEST_NODES[scenario_id]
        pytest_args = (
            "-m",
            "pytest",
            "-n",
            "16",
            "--dist",
            "loadfile",
            test_node,
            "-q",
        )
        environment = os.environ.copy()
        environment["PYTHONPATH"] = str(root / "src")
        removed_credentials = sorted(
            name for name in policy.allowed_secret_names if environment.pop(name, None) is not None
        )
        started_at = _aware(observed_at)
        try:
            completed = subprocess.run(
                (str(executable), *pytest_args),
                cwd=root,
                env=environment,
                text=True,
                capture_output=True,
                check=False,
            )
        except OSError as exc:
            raise OpsReleasePromotionError(
                "RELEASE_CANARY_EXECUTION_FAILED",
                f"{scenario_id}:{type(exc).__name__}:{exc}",
            ) from exc
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        payload: dict[str, object] = {
            "schema_version": "ops_release_incident_regression.v1",
            "scenario_id": scenario_id,
            "status": "PASS" if completed.returncode == 0 else "FAIL",
            "git_commit": candidate_commit,
            "test_node_id": test_node,
            "provider_request_performed": False,
            "runner": {
                "schema_version": policy.pre_release_canary_runner_schema,
                "python_executable": str(executable),
                "pytest_args": list(pytest_args),
                "return_code": completed.returncode,
                "stdout_sha256": hashlib.sha256(stdout.encode("utf-8")).hexdigest(),
                "stderr_sha256": hashlib.sha256(stderr.encode("utf-8")).hexdigest(),
                "provider_credential_names_removed": removed_credentials,
                "executed_at": started_at.isoformat(),
            },
            **_safety_boundary(),
        }
        path = evidence_root / f"{scenario_id}.json"
        write_json_atomic(path, payload)
        if completed.returncode != 0:
            raise OpsReleasePromotionError(
                "RELEASE_CANARY_SCENARIO_NOT_PASS",
                f"{scenario_id}:return_code={completed.returncode}:evidence={path}",
            )
        _validate_incident_regression_evidence(
            payload,
            scenario_id=scenario_id,
            candidate_commit=candidate_commit,
            expected_test_node=test_node,
            policy=policy,
        )
        paths.append(path)
    return tuple(paths)


def validate_ops_release_preflight_canary(
    payload: Mapping[str, object],
    *,
    policy: OpsReleasePromotionPolicy | None = None,
    expected_commit: str | None = None,
    verify_live_artifacts: bool = False,
    artifact_root: Path | None = None,
) -> None:
    checked_policy = policy or load_ops_release_promotion_policy()
    if payload.get("schema_version") != checked_policy.pre_release_canary_schema:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("status") != "PASS":
        raise OpsReleasePromotionError("RELEASE_CANARY_NOT_PASS", str(payload.get("status")))
    commit = _text(payload.get("candidate_commit"), "candidate_commit")
    _require_commit(commit, "candidate_commit")
    if expected_commit is not None and commit != expected_commit:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_COMMIT_MISMATCH",
            f"expected={expected_commit};observed={commit}",
        )
    if payload.get("provider_request_performed") is not False:
        raise OpsReleasePromotionError("RELEASE_CANARY_PROVIDER_REQUEST_FORBIDDEN", commit)
    rows = _mapping_rows(payload.get("scenarios"), "canary.scenarios")
    scenario_ids = [_text(row.get("scenario_id"), "scenario_id") for row in rows]
    if len(scenario_ids) != len(set(scenario_ids)):
        raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_DUPLICATE", ",".join(scenario_ids))
    if set(scenario_ids) != set(checked_policy.pre_release_canary_required_scenarios):
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_SCENARIO_SET_MISMATCH",
            ",".join(sorted(scenario_ids)),
        )
    for row in rows:
        scenario_id = _text(row.get("scenario_id"), "scenario_id")
        if (
            row.get("status") != "PASS"
            or row.get("git_commit") != commit
            or row.get("provider_request_performed") is not False
            or row.get("test_node_id") != _INCIDENT_REGRESSION_TEST_NODES.get(scenario_id)
        ):
            raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_INVALID", scenario_id)
        evidence = _mapping(row.get("evidence"), "scenario.evidence")
        _validate_portable_commitment_row(
            evidence,
            verify_live=verify_live_artifacts,
            artifact_root=artifact_root,
        )
        if verify_live_artifacts:
            if artifact_root is None:
                raise OpsReleasePromotionError(
                    "RELEASE_ARTIFACT_ROOT_REQUIRED",
                    f"canary scenario {scenario_id}",
                )
            evidence_path = _resolve_portable_commitment_path(artifact_root, evidence)
            _validate_incident_regression_evidence(
                _read_json_mapping(evidence_path),
                scenario_id=scenario_id,
                candidate_commit=commit,
                expected_test_node=_INCIDENT_REGRESSION_TEST_NODES[scenario_id],
                policy=checked_policy,
            )
    _require_safety_boundary(payload, "pre-release canary")
    canary_id = _text(payload.get("canary_id"), "canary_id")
    expected_id = _content_id("ops_release_canary_", payload, "canary_id")
    if canary_id != expected_id:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_ID_MISMATCH",
            f"expected={expected_id};observed={canary_id}",
        )


def _validate_incident_regression_evidence(
    payload: Mapping[str, object],
    *,
    scenario_id: str,
    candidate_commit: str,
    expected_test_node: str,
    policy: OpsReleasePromotionPolicy,
) -> None:
    if payload.get("schema_version") != "ops_release_incident_regression.v1":
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_SCENARIO_SCHEMA",
            str(payload.get("schema_version")),
        )
    if payload.get("scenario_id") != scenario_id:
        raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_ID_MISMATCH", scenario_id)
    if payload.get("status") != "PASS":
        raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_NOT_PASS", scenario_id)
    if payload.get("git_commit") != candidate_commit:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_SCENARIO_COMMIT_MISMATCH",
            scenario_id,
        )
    if payload.get("test_node_id") != expected_test_node:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_TEST_NODE_MISMATCH",
            scenario_id,
        )
    if payload.get("provider_request_performed") is not False:
        raise OpsReleasePromotionError(
            "RELEASE_CANARY_PROVIDER_REQUEST_FORBIDDEN",
            scenario_id,
        )
    runner = _mapping(payload.get("runner"), "scenario.runner")
    if runner.get("schema_version") != policy.pre_release_canary_runner_schema:
        raise OpsReleasePromotionError("RELEASE_CANARY_RUNNER_SCHEMA", scenario_id)
    expected_args = [
        "-m",
        "pytest",
        "-n",
        "16",
        "--dist",
        "loadfile",
        expected_test_node,
        "-q",
    ]
    if runner.get("pytest_args") != expected_args or runner.get("return_code") != 0:
        raise OpsReleasePromotionError("RELEASE_CANARY_RUNNER_RESULT", scenario_id)
    for field in ("stdout_sha256", "stderr_sha256"):
        if not re.fullmatch(r"[0-9a-f]{64}", _text(runner.get(field), field)):
            raise OpsReleasePromotionError("RELEASE_CANARY_RUNNER_DIGEST", scenario_id)
    removed = runner.get("provider_credential_names_removed")
    if not isinstance(removed, list) or not all(isinstance(row, str) for row in removed):
        raise OpsReleasePromotionError("RELEASE_CANARY_RUNNER_CREDENTIALS", scenario_id)
    if set(removed) - set(policy.allowed_secret_names):
        raise OpsReleasePromotionError("RELEASE_CANARY_RUNNER_CREDENTIALS", scenario_id)
    _parse_aware_datetime(runner.get("executed_at"), "scenario.runner.executed_at")
    _require_safety_boundary(payload, f"canary scenario {scenario_id}")


def _validate_pre_release_canary_summary(
    payload: Mapping[str, object],
    policy: OpsReleasePromotionPolicy,
    commit: str,
) -> None:
    if (
        payload.get("schema_version") != policy.pre_release_canary_schema
        or payload.get("status") != "PASS"
        or payload.get("candidate_commit") != commit
        or payload.get("provider_request_performed") is not False
    ):
        raise OpsReleasePromotionError("RELEASE_CANARY_SUMMARY_INVALID", commit)
    raw_ids = payload.get("scenario_ids")
    if not isinstance(raw_ids, list) or not all(isinstance(row, str) for row in raw_ids):
        raise OpsReleasePromotionError("RELEASE_CANARY_SUMMARY_SCENARIOS", str(raw_ids))
    if set(raw_ids) != set(policy.pre_release_canary_required_scenarios):
        raise OpsReleasePromotionError("RELEASE_CANARY_SCENARIO_SET_MISMATCH", ",".join(raw_ids))


def install_ops_runtime_git_exclusions(
    *,
    runtime_root: Path,
    development_root: Path,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    policy = load_ops_release_promotion_policy(policy_path)
    root = runtime_root.resolve()
    development = development_root.resolve()
    if not root.is_dir() or not development.is_dir():
        raise OpsReleasePromotionError(
            "RUNTIME_ROOT_MISSING",
            f"runtime={root};development={development}",
        )
    runtime_common = _git_common_dir(root)
    development_common = _git_common_dir(development)
    if _path_key(runtime_common) == _path_key(development_common):
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_COMMON_DIR_SHARED",
            str(runtime_common),
        )
    remote = _git_text(root, "remote", "get-url", "origin")
    if remote != policy.expected_remote:
        raise OpsReleasePromotionError(
            "RUNTIME_REMOTE_MISMATCH",
            remote,
        )
    active_leases = _active_checkout_leases(
        runtime_root=root,
        policy_source_root=development,
    )
    if active_leases:
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_EXCLUDE_ACTIVE_LEASE",
            ",".join(active_leases),
        )
    exclude_path = _runtime_git_exclude_path(root)
    expected = _runtime_git_exclude_bytes(policy)
    previous = exclude_path.read_bytes() if exclude_path.is_file() else b""
    if previous == expected:
        action = "REUSED_EXACT"
    else:
        try:
            previous_text = previous.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise OpsReleasePromotionError(
                "RUNTIME_GIT_EXCLUDE_EXISTING_INVALID",
                str(exclude_path),
            ) from exc
        behavioral_lines = [
            line.strip()
            for line in previous_text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]
        if behavioral_lines:
            raise OpsReleasePromotionError(
                "RUNTIME_GIT_EXCLUDE_EXISTING_RULES",
                ",".join(behavioral_lines),
            )
        _write_bytes_atomic(exclude_path, expected)
        action = "INSTALLED"
    commitment = _inspect_runtime_git_exclusions(root, policy)
    payload: dict[str, object] = {
        "schema_version": "ops_runtime_git_exclusion_installation.v1",
        "status": "PASS",
        "action": action,
        "runtime_root": str(root),
        "development_root": str(development),
        "runtime_git_common_dir": str(runtime_common),
        "development_git_common_dir": str(development_common),
        "runtime_origin": remote,
        "previous_size_bytes": len(previous),
        "previous_sha256": hashlib.sha256(previous).hexdigest(),
        "git_exclude": commitment,
        "installed_at": _aware(observed_at).isoformat(),
        **_safety_boundary(),
    }
    payload["installation_id"] = _content_id(
        "ops_runtime_git_exclude_",
        payload,
        "installation_id",
    )
    return payload


def inspect_runtime_provenance(
    *,
    runtime_root: Path,
    development_root: Path,
    runtime_python: Path,
    candidate_commit: str,
    policy: OpsReleasePromotionPolicy | None = None,
) -> dict[str, object]:
    checked_policy = policy or load_ops_release_promotion_policy()
    root = runtime_root.resolve()
    dev_root = development_root.resolve()
    python_path = runtime_python.resolve()
    _require_commit(candidate_commit, "candidate_commit")
    if not root.is_dir() or not dev_root.is_dir():
        raise OpsReleasePromotionError(
            "RUNTIME_ROOT_MISSING",
            f"runtime={root};development={dev_root}",
        )
    head = _git_text(root, "rev-parse", "HEAD")
    remote = _git_text(root, "remote", "get-url", "origin")
    remote_commit = _git_text(root, "rev-parse", checked_policy.reviewed_remote_ref)
    runtime_common = _git_common_dir(root)
    development_common = _git_common_dir(dev_root)
    # Post-switch provenance must be self-contained in the exact runtime release.
    # The development checkout only proves Git isolation and may be retired after
    # deployment; binding live audits to it would make scheduler preflight fragile.
    dirty_paths = _governed_dirty_paths(root)
    if dirty_paths:
        raise OpsReleasePromotionError("RUNTIME_CHECKOUT_DIRTY", ",".join(dirty_paths))
    if head != candidate_commit or remote_commit != candidate_commit:
        raise OpsReleasePromotionError(
            "RUNTIME_RELEASE_MISMATCH",
            f"head={head};remote={remote_commit};candidate={candidate_commit}",
        )
    if remote != checked_policy.expected_remote:
        raise OpsReleasePromotionError("RUNTIME_REMOTE_MISMATCH", remote)
    if checked_policy.independent_git_common_dir_required and (
        _path_key(runtime_common) == _path_key(development_common)
    ):
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_COMMON_DIR_SHARED",
            str(runtime_common),
        )
    git_exclude = _inspect_runtime_git_exclusions(root, checked_policy)
    if checked_policy.executable_must_be_below_checkout and not python_path.is_relative_to(root):
        raise OpsReleasePromotionError(
            "RUNTIME_PYTHON_OUTSIDE_CHECKOUT",
            str(python_path),
        )
    if not python_path.is_file():
        raise OpsReleasePromotionError("RUNTIME_PYTHON_MISSING", str(python_path))
    probe = _runtime_probe(
        runtime_root=root,
        runtime_python=python_path,
        package_module=checked_policy.package_module,
    )
    observed_executable = Path(_text(probe.get("executable"), "probe.executable")).resolve()
    module_file = Path(_text(probe.get("module_file"), "probe.module_file")).resolve()
    project_root = Path(_text(probe.get("project_root"), "probe.project_root")).resolve()
    installed_distributions = _normalize_distribution_inventory(
        probe.get("installed_distributions")
    )
    if not any(
        row["name"].casefold() == checked_policy.distribution_name.casefold()
        for row in installed_distributions
    ):
        raise OpsReleasePromotionError(
            "RUNTIME_DISTRIBUTION_MISSING",
            checked_policy.distribution_name,
        )
    environment_fingerprint = _distribution_fingerprint(installed_distributions)
    if observed_executable != python_path:
        raise OpsReleasePromotionError(
            "RUNTIME_EXECUTABLE_MISMATCH",
            f"expected={python_path};observed={observed_executable}",
        )
    if checked_policy.imported_package_must_be_below_checkout and not module_file.is_relative_to(
        root
    ):
        raise OpsReleasePromotionError(
            "RUNTIME_PACKAGE_OUTSIDE_CHECKOUT",
            str(module_file),
        )
    if project_root != root:
        raise OpsReleasePromotionError(
            "RUNTIME_PROJECT_ROOT_MISMATCH",
            f"expected={root};observed={project_root}",
        )
    return {
        "root": str(root),
        "development_checkout_root": str(dev_root),
        "git_common_dir": str(runtime_common),
        "development_git_common_dir": str(development_common),
        "head_commit": head,
        "origin": remote,
        "reviewed_remote_ref": checked_policy.reviewed_remote_ref,
        "reviewed_remote_commit": remote_commit,
        "dirty_path_count": 0,
        "python_executable": str(python_path),
        "python_sha256": _sha256_file(python_path),
        "package_module": checked_policy.package_module,
        "package_file": str(module_file),
        "project_root": str(project_root),
        "git_exclude": git_exclude,
        "installed_distributions": installed_distributions,
        "environment_fingerprint": environment_fingerprint,
    }


def build_ops_deployment_acceptance(
    *,
    release_candidate: Mapping[str, object],
    release_candidate_path: Path,
    runtime_root: Path,
    development_root: Path,
    runtime_python: Path,
    scheduler_observation: Mapping[str, object],
    promotion_event_path: Path,
    credential_names: Sequence[str],
    credential_attestation_ref: str,
    owner_decision_ref: str,
    previous_acceptance_path: Path | None = None,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    policy = load_ops_release_promotion_policy(policy_path)
    validate_ops_release_candidate(
        release_candidate,
        policy=policy,
        verify_live_artifacts=True,
        artifact_root=runtime_root,
    )
    timestamp = _aware(observed_at)
    if not owner_decision_ref.startswith("owner_decision:"):
        raise OpsReleasePromotionError("DEPLOYMENT_OWNER_DECISION_INVALID", owner_decision_ref)
    if not credential_attestation_ref.startswith("owner_attestation:"):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_CREDENTIAL_ATTESTATION_INVALID",
            credential_attestation_ref,
        )
    runtime = inspect_runtime_provenance(
        runtime_root=runtime_root,
        development_root=development_root,
        runtime_python=runtime_python,
        candidate_commit=_text(release_candidate.get("candidate_commit"), "candidate_commit"),
        policy=policy,
    )
    scheduler = validate_scheduler_observation(
        scheduler_observation,
        runtime_root=runtime_root,
        runtime_python=runtime_python,
        policy=policy,
    )
    promotion_event = _read_json_mapping(promotion_event_path)
    if promotion_event.get("schema_version") != _PROMOTION_TRANSACTION_SCHEMA:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PROMOTION_EVENT_SCHEMA",
            str(promotion_event.get("schema_version")),
        )
    if promotion_event.get("state") != "PROMOTED_NOT_ACTIVATED":
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PROMOTION_EVENT_STATE",
            str(promotion_event.get("state")),
        )
    if promotion_event.get("candidate_commit") != release_candidate.get(
        "candidate_commit"
    ) or promotion_event.get("release_id") != release_candidate.get("release_id"):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PROMOTION_EVENT_RELEASE_MISMATCH",
            str(promotion_event_path),
        )
    if policy.observation_must_not_predate_promotion:
        scheduler_observed_at = _parse_aware_datetime(
            scheduler.get("observed_at"),
            "scheduler.observed_at",
        )
        promotion_observed_at = _parse_aware_datetime(
            promotion_event.get("observed_at"),
            "promotion_event.observed_at",
        )
        if scheduler_observed_at < promotion_observed_at:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_SCHEDULER_OBSERVATION_STALE",
                (
                    f"scheduler={scheduler_observed_at.isoformat()};"
                    f"promotion={promotion_observed_at.isoformat()}"
                ),
            )
    event_candidate_receipt = _mapping(
        promotion_event.get("release_candidate_receipt"),
        "promotion_event.release_candidate_receipt",
    )
    _validate_commitment_row(event_candidate_receipt, True)
    if (
        Path(
            _text(
                event_candidate_receipt.get("absolute_path"),
                "promotion_event.release_candidate_receipt.absolute_path",
            )
        ).resolve()
        != release_candidate_path.resolve()
    ):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PROMOTION_RECEIPT_PATH_MISMATCH",
            str(release_candidate_path),
        )
    normalized_credentials = tuple(sorted(set(credential_names)))
    unknown_credentials = sorted(set(normalized_credentials) - set(policy.allowed_secret_names))
    forbidden_credentials = sorted(
        name
        for name in normalized_credentials
        if any(pattern.casefold() in name.casefold() for pattern in policy.forbidden_name_patterns)
    )
    if unknown_credentials:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_CREDENTIAL_SCOPE_UNKNOWN",
            ",".join(unknown_credentials),
        )
    if forbidden_credentials:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_CREDENTIAL_SCOPE_FORBIDDEN",
            ",".join(forbidden_credentials),
        )
    missing_credential_groups = [
        group
        for group in policy.required_credential_groups
        if not set(group).intersection(normalized_credentials)
    ]
    if missing_credential_groups:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_REQUIRED_CREDENTIAL_MISSING",
            ";".join("|".join(group) for group in missing_credential_groups),
        )
    candidate_path = release_candidate_path.resolve()
    resolved_runtime_root = runtime_root.resolve()
    if not candidate_path.is_relative_to(resolved_runtime_root):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_RELEASE_RECEIPT_OUTSIDE_RUNTIME",
            str(candidate_path),
        )
    resolved_promotion_event = promotion_event_path.resolve()
    if not resolved_promotion_event.is_relative_to(resolved_runtime_root):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PROMOTION_EVENT_OUTSIDE_RUNTIME",
            str(resolved_promotion_event),
        )
    candidate_commitment = _single_file_commitment(candidate_path, root=candidate_path.parent)
    previous_acceptance = (
        None
        if previous_acceptance_path is None
        else _single_file_commitment(
            previous_acceptance_path.resolve(),
            root=previous_acceptance_path.resolve().parent,
        )
    )
    payload: dict[str, object] = {
        "schema_version": _DEPLOYMENT_ACCEPTANCE_SCHEMA,
        "status": "ACTIVE_OWNER_ACCEPTED",
        "release_lifecycle_state": "SCHEDULER_BOUND",
        "policy_id": policy.policy_id,
        "policy_version": policy.version,
        "owner_decision_ref": owner_decision_ref,
        "release": {
            "release_id": _text(release_candidate.get("release_id"), "release_id"),
            "candidate_commit": _text(
                release_candidate.get("candidate_commit"),
                "candidate_commit",
            ),
            "receipt": candidate_commitment,
        },
        "promotion_event": _single_file_commitment(
            resolved_promotion_event,
            root=resolved_promotion_event.parent,
        ),
        "previous_acceptance": previous_acceptance,
        "runtime": runtime,
        "scheduler": scheduler,
        "credentials": {
            "attestation_ref": credential_attestation_ref,
            "secret_names": list(normalized_credentials),
            "required_groups_satisfied": [
                list(group) for group in policy.required_credential_groups
            ],
            "secret_values_recorded": False,
            "forbidden_names_present": [],
        },
        "accepted_at": timestamp.isoformat(),
        **_safety_boundary(),
    }
    payload["deployment_id"] = _content_id("ops_deployment_", payload, "deployment_id")
    validate_ops_deployment_acceptance(payload, policy=policy)
    return payload


def validate_scheduler_observation(
    payload: Mapping[str, object],
    *,
    runtime_root: Path,
    runtime_python: Path,
    policy: OpsReleasePromotionPolicy | None = None,
) -> dict[str, object]:
    checked_policy = policy or load_ops_release_promotion_policy()
    if payload.get("schema_version") != checked_policy.scheduler_observation_schema:
        raise OpsReleasePromotionError(
            "SCHEDULER_OBSERVATION_SCHEMA",
            str(payload.get("schema_version")),
        )
    expected = {
        "provider": checked_policy.scheduler_provider,
        "scheduler_id": checked_policy.scheduler_id,
        "entry_count": checked_policy.scheduler_entry_count,
        "enabled": True,
        "windows_task_scheduler_entry_count": 0,
        "status": checked_policy.expected_scheduler_status,
        "rrule": checked_policy.expected_scheduler_rrule,
        "model": checked_policy.expected_scheduler_model,
        "reasoning_effort": checked_policy.expected_scheduler_reasoning_effort,
        "execution_environment": checked_policy.expected_scheduler_execution_environment,
        "carrier_mode": checked_policy.scheduler_carrier_mode,
        "local_timezone": checked_policy.scheduler_local_timezone,
        "per_invocation_business_trigger_max": (
            checked_policy.scheduler_per_invocation_business_trigger_max
        ),
        "same_scheduler_entry_for_all_windows": (
            checked_policy.scheduler_same_entry_for_all_windows
        ),
    }
    for field, value in expected.items():
        if payload.get(field) != value:
            raise OpsReleasePromotionError(
                "SCHEDULER_OBSERVATION_MISMATCH",
                f"{field}={payload.get(field)!r}",
            )
    trigger = _text_tuple(payload.get("unified_external_trigger"), "unified_external_trigger")
    if trigger != checked_policy.unified_external_trigger:
        raise OpsReleasePromotionError(
            "SCHEDULER_TRIGGER_MISMATCH",
            " ".join(trigger),
        )
    working_directory = Path(_text(payload.get("working_directory"), "working_directory")).resolve()
    executable = Path(_text(payload.get("runtime_python"), "runtime_python")).resolve()
    if working_directory != runtime_root.resolve():
        raise OpsReleasePromotionError(
            "SCHEDULER_WORKING_DIRECTORY_MISMATCH",
            str(working_directory),
        )
    if executable != runtime_python.resolve():
        raise OpsReleasePromotionError(
            "SCHEDULER_EXECUTABLE_MISMATCH",
            str(executable),
        )
    environment_names = _text_tuple(payload.get("environment_names"), "environment_names")
    if set(environment_names) != set(checked_policy.required_environment_names):
        raise OpsReleasePromotionError(
            "SCHEDULER_ENVIRONMENT_CONTRACT_MISMATCH",
            (
                f"expected={','.join(checked_policy.required_environment_names)};"
                f"observed={','.join(environment_names)}"
            ),
        )
    if checked_policy.legacy_release_assertion_name in environment_names:
        raise OpsReleasePromotionError(
            "SCHEDULER_MUTABLE_RELEASE_AUTHORITY_FORBIDDEN",
            checked_policy.legacy_release_assertion_name,
        )
    target = _mapping(payload.get("target"), "target")
    if target.get("type") != checked_policy.expected_scheduler_target_type:
        raise OpsReleasePromotionError(
            "SCHEDULER_TARGET_MISMATCH",
            str(target.get("type")),
        )
    if target.get("project_id") is not None:
        raise OpsReleasePromotionError(
            "SCHEDULER_PROJECT_TARGET_FORBIDDEN",
            str(target.get("project_id")),
        )
    observed_cwds = payload.get("cwds")
    if (
        not isinstance(observed_cwds, list)
        or tuple(observed_cwds) != checked_policy.expected_scheduler_cwds
    ):
        raise OpsReleasePromotionError(
            "SCHEDULER_DEVELOPMENT_CWD_FORBIDDEN",
            (f"expected={checked_policy.expected_scheduler_cwds!r};" f"observed={observed_cwds!r}"),
        )
    observed_windows = _scheduler_invocation_windows(
        payload.get("invocation_windows"),
        "invocation_windows",
    )
    if observed_windows != checked_policy.scheduler_invocation_windows:
        raise OpsReleasePromotionError(
            "SCHEDULER_INVOCATION_WINDOWS_MISMATCH",
            repr(observed_windows),
        )
    config = _mapping(payload.get("config"), "config")
    config_path = Path(_text(config.get("absolute_path"), "config.absolute_path")).resolve()
    _validate_commitment_row(config, True)
    try:
        config_payload = tomllib.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise OpsReleasePromotionError(
            "SCHEDULER_CONFIG_READ_FAILED",
            f"{config_path}:{type(exc).__name__}:{exc}",
        ) from exc
    live_fields = {
        "scheduler_id": config_payload.get("id"),
        "status": config_payload.get("status"),
        "rrule": config_payload.get("rrule"),
        "model": config_payload.get("model"),
        "reasoning_effort": config_payload.get("reasoning_effort"),
        "execution_environment": config_payload.get("execution_environment"),
        "target": config_payload.get("target"),
        "cwds": list(_automation_cwds(config_payload)),
    }
    drift = [field for field, value in live_fields.items() if payload.get(field) != value]
    if drift:
        raise OpsReleasePromotionError("SCHEDULER_CONFIG_OBSERVATION_DRIFT", ",".join(drift))
    prompt = _mapping(payload.get("prompt"), "prompt")
    canonical_path = (
        runtime_root.resolve() / checked_policy.canonical_prompt_relative_path
    ).resolve()
    expected_prompt_path = Path(
        _text(prompt.get("canonical_absolute_path"), "prompt.canonical_absolute_path")
    ).resolve()
    if expected_prompt_path != canonical_path or not canonical_path.is_file():
        raise OpsReleasePromotionError(
            "SCHEDULER_CANONICAL_PROMPT_PATH_MISMATCH",
            str(expected_prompt_path),
        )
    canonical_bytes = canonical_path.read_bytes()
    canonical_text = _canonical_prompt_text(canonical_bytes, canonical_path)
    actual_prompt = _text(config_payload.get("prompt"), "config.prompt")
    if actual_prompt != canonical_text:
        raise OpsReleasePromotionError("SCHEDULER_PROMPT_DRIFT", str(config_path))
    expected_file_sha = hashlib.sha256(canonical_bytes).hexdigest()
    expected_semantic_sha = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
    if (
        prompt.get("canonical_path") != checked_policy.canonical_prompt_relative_path
        or prompt.get("canonical_file_sha256") != expected_file_sha
        or prompt.get("semantic_sha256") != expected_semantic_sha
    ):
        raise OpsReleasePromotionError("SCHEDULER_PROMPT_COMMITMENT_MISMATCH", str(config_path))
    observed_at = _parse_aware_datetime(payload.get("observed_at"), "observed_at")
    config_updated_at = _parse_automation_updated_at(
        config_payload.get("updated_at"),
        "config.updated_at",
    )
    if payload.get("config_updated_at") != config_updated_at.isoformat():
        raise OpsReleasePromotionError(
            "SCHEDULER_CONFIG_UPDATED_AT_MISMATCH",
            str(payload.get("config_updated_at")),
        )
    if config_updated_at > observed_at:
        raise OpsReleasePromotionError(
            "SCHEDULER_OBSERVATION_PREDATES_CONFIG",
            observed_at.isoformat(),
        )
    return {
        "schema_version": _SCHEDULER_BINDING_SCHEMA,
        "provider": checked_policy.scheduler_provider,
        "scheduler_id": checked_policy.scheduler_id,
        "entry_count": checked_policy.scheduler_entry_count,
        "enabled": True,
        "windows_task_scheduler_entry_count": 0,
        "unified_external_trigger": list(trigger),
        "working_directory": str(working_directory),
        "runtime_python": str(executable),
        "environment_names": sorted(environment_names),
        "status": checked_policy.expected_scheduler_status,
        "rrule": checked_policy.expected_scheduler_rrule,
        "model": checked_policy.expected_scheduler_model,
        "reasoning_effort": checked_policy.expected_scheduler_reasoning_effort,
        "execution_environment": checked_policy.expected_scheduler_execution_environment,
        "carrier_mode": checked_policy.scheduler_carrier_mode,
        "local_timezone": checked_policy.scheduler_local_timezone,
        "invocation_windows": [
            {"role": role, "hour": hour, "minute": minute}
            for role, hour, minute in checked_policy.scheduler_invocation_windows
        ],
        "per_invocation_business_trigger_max": (
            checked_policy.scheduler_per_invocation_business_trigger_max
        ),
        "same_scheduler_entry_for_all_windows": (
            checked_policy.scheduler_same_entry_for_all_windows
        ),
        "target": dict(target),
        "cwds": list(observed_cwds),
        "config": dict(config),
        "config_updated_at": config_updated_at.isoformat(),
        "prompt": dict(prompt),
        "release_identity_authority": checked_policy.release_identity_authority,
        "mutable_release_environment_present": False,
        "observed_at": observed_at.isoformat(),
    }


def observe_codex_automation_config(
    *,
    automation_path: Path,
    runtime_root: Path,
    runtime_python: Path,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> dict[str, object]:
    """Read the actual Codex automation config and bind it to the canonical prompt."""

    policy = load_ops_release_promotion_policy(policy_path)
    path = automation_path.resolve()
    if not path.is_file() or path.is_symlink():
        raise OpsReleasePromotionError("SCHEDULER_CONFIG_FILE_REQUIRED", str(path))
    try:
        raw = path.read_bytes()
        config = tomllib.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise OpsReleasePromotionError(
            "SCHEDULER_CONFIG_READ_FAILED",
            f"{path}:{type(exc).__name__}:{exc}",
        ) from exc
    prompt_value = _text(config.get("prompt"), "config.prompt")
    canonical_path = (runtime_root.resolve() / policy.canonical_prompt_relative_path).resolve()
    if not canonical_path.is_file():
        raise OpsReleasePromotionError(
            "SCHEDULER_CANONICAL_PROMPT_MISSING",
            str(canonical_path),
        )
    canonical_bytes = canonical_path.read_bytes()
    canonical_text = _canonical_prompt_text(canonical_bytes, canonical_path)
    if prompt_value != canonical_text:
        raise OpsReleasePromotionError("SCHEDULER_PROMPT_DRIFT", str(path))
    target = _mapping(config.get("target"), "config.target")
    cwds = _automation_cwds(config)
    timestamp = _aware(observed_at)
    config_updated_at = _parse_automation_updated_at(config.get("updated_at"), "config.updated_at")
    if config_updated_at > timestamp:
        raise OpsReleasePromotionError(
            "SCHEDULER_OBSERVATION_PREDATES_CONFIG",
            timestamp.isoformat(),
        )
    payload: dict[str, object] = {
        "schema_version": policy.scheduler_observation_schema,
        "provider": policy.scheduler_provider,
        "scheduler_id": config.get("id"),
        "entry_count": 1,
        "enabled": config.get("status") == policy.expected_scheduler_status,
        "windows_task_scheduler_entry_count": 0,
        "unified_external_trigger": list(policy.unified_external_trigger),
        "working_directory": str(runtime_root.resolve()),
        "runtime_python": str(runtime_python.resolve()),
        "environment_names": list(policy.required_environment_names),
        "status": config.get("status"),
        "rrule": config.get("rrule"),
        "model": config.get("model"),
        "reasoning_effort": config.get("reasoning_effort"),
        "execution_environment": config.get("execution_environment"),
        "carrier_mode": policy.scheduler_carrier_mode,
        "local_timezone": policy.scheduler_local_timezone,
        "invocation_windows": [
            {"role": role, "hour": hour, "minute": minute}
            for role, hour, minute in policy.scheduler_invocation_windows
        ],
        "per_invocation_business_trigger_max": (
            policy.scheduler_per_invocation_business_trigger_max
        ),
        "same_scheduler_entry_for_all_windows": policy.scheduler_same_entry_for_all_windows,
        "target": dict(target),
        "cwds": list(cwds),
        "config": {
            "path": str(path),
            "absolute_path": str(path),
            "size_bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
        "config_updated_at": config_updated_at.isoformat(),
        "prompt": {
            "canonical_path": policy.canonical_prompt_relative_path,
            "canonical_absolute_path": str(canonical_path),
            "canonical_file_sha256": hashlib.sha256(canonical_bytes).hexdigest(),
            "semantic_sha256": hashlib.sha256(canonical_text.encode("utf-8")).hexdigest(),
        },
        "observed_at": timestamp.isoformat(),
    }
    validate_scheduler_observation(
        payload,
        runtime_root=runtime_root,
        runtime_python=runtime_python,
        policy=policy,
    )
    return payload


def _validate_scheduler_binding_record(
    payload: Mapping[str, object],
    *,
    policy: OpsReleasePromotionPolicy,
    runtime_root: Path | None,
    verify_live: bool,
) -> None:
    expected = {
        "schema_version": _SCHEDULER_BINDING_SCHEMA,
        "provider": policy.scheduler_provider,
        "scheduler_id": policy.scheduler_id,
        "entry_count": policy.scheduler_entry_count,
        "enabled": True,
        "windows_task_scheduler_entry_count": 0,
        "status": policy.expected_scheduler_status,
        "rrule": policy.expected_scheduler_rrule,
        "model": policy.expected_scheduler_model,
        "reasoning_effort": policy.expected_scheduler_reasoning_effort,
        "execution_environment": policy.expected_scheduler_execution_environment,
        "carrier_mode": policy.scheduler_carrier_mode,
        "local_timezone": policy.scheduler_local_timezone,
        "per_invocation_business_trigger_max": (
            policy.scheduler_per_invocation_business_trigger_max
        ),
        "same_scheduler_entry_for_all_windows": (policy.scheduler_same_entry_for_all_windows),
        "release_identity_authority": policy.release_identity_authority,
        "mutable_release_environment_present": False,
    }
    for field, expected_value in expected.items():
        if payload.get(field) != expected_value:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_SCHEDULER_BINDING_MISMATCH",
                f"{field}={payload.get(field)!r}",
            )
    environment_names = _text_tuple(payload.get("environment_names"), "environment_names")
    if set(environment_names) != set(policy.required_environment_names):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_SCHEDULER_ENVIRONMENT_MISMATCH",
            ",".join(environment_names),
        )
    if policy.legacy_release_assertion_name in environment_names:
        raise OpsReleasePromotionError(
            "SCHEDULER_MUTABLE_RELEASE_AUTHORITY_FORBIDDEN",
            policy.legacy_release_assertion_name,
        )
    target = _mapping(payload.get("target"), "scheduler.target")
    if target.get("type") != policy.expected_scheduler_target_type:
        raise OpsReleasePromotionError(
            "SCHEDULER_TARGET_MISMATCH",
            str(target.get("type")),
        )
    if target.get("project_id") is not None:
        raise OpsReleasePromotionError(
            "SCHEDULER_PROJECT_TARGET_FORBIDDEN",
            str(target.get("project_id")),
        )
    cwds = payload.get("cwds")
    if not isinstance(cwds, list) or tuple(cwds) != policy.expected_scheduler_cwds:
        raise OpsReleasePromotionError(
            "SCHEDULER_DEVELOPMENT_CWD_FORBIDDEN",
            f"expected={policy.expected_scheduler_cwds!r};observed={cwds!r}",
        )
    if (
        _scheduler_invocation_windows(
            payload.get("invocation_windows"),
            "scheduler.invocation_windows",
        )
        != policy.scheduler_invocation_windows
    ):
        raise OpsReleasePromotionError(
            "SCHEDULER_INVOCATION_WINDOWS_MISMATCH",
            repr(payload.get("invocation_windows")),
        )
    config = _mapping(payload.get("config"), "scheduler.config")
    _validate_commitment_row(config, verify_live)
    prompt = _mapping(payload.get("prompt"), "scheduler.prompt")
    canonical_path_text = _text(
        prompt.get("canonical_absolute_path"),
        "scheduler.prompt.canonical_absolute_path",
    )
    if prompt.get("canonical_path") != policy.canonical_prompt_relative_path:
        raise OpsReleasePromotionError(
            "SCHEDULER_PROMPT_COMMITMENT_MISMATCH",
            str(prompt.get("canonical_path")),
        )
    for field in ("canonical_file_sha256", "semantic_sha256"):
        value = _text(prompt.get(field), f"scheduler.prompt.{field}")
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise OpsReleasePromotionError("SCHEDULER_PROMPT_COMMITMENT_MISMATCH", field)
    observed_at = _parse_aware_datetime(payload.get("observed_at"), "scheduler.observed_at")
    config_updated_at = _parse_aware_datetime(
        payload.get("config_updated_at"),
        "scheduler.config_updated_at",
    )
    if config_updated_at > observed_at:
        raise OpsReleasePromotionError(
            "SCHEDULER_OBSERVATION_PREDATES_CONFIG",
            observed_at.isoformat(),
        )
    if verify_live:
        if runtime_root is None:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_LIVE_ARGUMENT_MISSING",
                "runtime_root for scheduler binding",
            )
        canonical_path = Path(canonical_path_text).resolve()
        expected_path = (runtime_root.resolve() / policy.canonical_prompt_relative_path).resolve()
        if canonical_path != expected_path or not canonical_path.is_file():
            raise OpsReleasePromotionError(
                "SCHEDULER_CANONICAL_PROMPT_PATH_MISMATCH",
                str(canonical_path),
            )
        canonical_bytes = canonical_path.read_bytes()
        canonical_text = _canonical_prompt_text(canonical_bytes, canonical_path)
        if (
            prompt.get("canonical_file_sha256") != hashlib.sha256(canonical_bytes).hexdigest()
            or prompt.get("semantic_sha256")
            != hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
        ):
            raise OpsReleasePromotionError(
                "SCHEDULER_PROMPT_COMMITMENT_MISMATCH",
                str(canonical_path),
            )


def validate_ops_deployment_acceptance(
    payload: Mapping[str, object],
    *,
    policy: OpsReleasePromotionPolicy | None = None,
    runtime_root: Path | None = None,
    development_root: Path | None = None,
    runtime_python: Path | None = None,
    verify_live_runtime: bool = False,
) -> None:
    checked_policy = policy or load_ops_release_promotion_policy()
    schema = payload.get("schema_version")
    if schema not in {
        _DEPLOYMENT_ACCEPTANCE_SCHEMA,
        checked_policy.legacy_deployment_acceptance_schema,
    }:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_RECEIPT_SCHEMA",
            str(schema),
        )
    legacy = schema == checked_policy.legacy_deployment_acceptance_schema
    if payload.get("status") != "ACTIVE_OWNER_ACCEPTED":
        raise OpsReleasePromotionError(
            "DEPLOYMENT_RECEIPT_STATUS",
            str(payload.get("status")),
        )
    release = _mapping(payload.get("release"), "release")
    release_commit = _text(release.get("candidate_commit"), "release.candidate_commit")
    _require_commit(release_commit, "release.candidate_commit")
    if legacy:
        if release_commit not in checked_policy.legacy_active_release_commits:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_LEGACY_RELEASE_NOT_ALLOWLISTED",
                release_commit,
            )
    else:
        if payload.get("policy_id") != checked_policy.policy_id:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_POLICY_ID",
                str(payload.get("policy_id")),
            )
        if payload.get("policy_version") != checked_policy.version:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_POLICY_VERSION",
                str(payload.get("policy_version")),
            )
        if payload.get("release_lifecycle_state") != "SCHEDULER_BOUND":
            raise OpsReleasePromotionError(
                "DEPLOYMENT_LIFECYCLE_STATE",
                str(payload.get("release_lifecycle_state")),
            )
    _validate_commitment_row(
        _mapping(release.get("receipt"), "release.receipt"),
        verify_live_runtime,
    )
    _validate_commitment_row(
        _mapping(payload.get("promotion_event"), "promotion_event"),
        verify_live_runtime,
    )
    runtime = _mapping(payload.get("runtime"), "runtime")
    if runtime.get("head_commit") != release_commit:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_RUNTIME_RELEASE_MISMATCH",
            str(runtime.get("head_commit")),
        )
    installed_distributions = _normalize_distribution_inventory(
        runtime.get("installed_distributions")
    )
    expected_environment_fingerprint = _distribution_fingerprint(installed_distributions)
    if runtime.get("environment_fingerprint") != expected_environment_fingerprint:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_ENVIRONMENT_FINGERPRINT_MISMATCH",
            str(runtime.get("environment_fingerprint")),
        )
    if not any(
        row["name"].casefold() == checked_policy.distribution_name.casefold()
        for row in installed_distributions
    ):
        raise OpsReleasePromotionError(
            "RUNTIME_DISTRIBUTION_MISSING",
            checked_policy.distribution_name,
        )
    if not legacy:
        _validate_scheduler_binding_record(
            _mapping(payload.get("scheduler"), "scheduler"),
            policy=checked_policy,
            runtime_root=runtime_root,
            verify_live=verify_live_runtime,
        )
    credentials = _mapping(payload.get("credentials"), "credentials")
    if credentials.get("secret_values_recorded") is not False:
        raise OpsReleasePromotionError("DEPLOYMENT_SECRET_VALUE_RECORDED", "true")
    if credentials.get("forbidden_names_present") != []:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_FORBIDDEN_CREDENTIAL",
            str(credentials.get("forbidden_names_present")),
        )
    secret_names = _text_tuple(credentials.get("secret_names"), "credentials.secret_names")
    if set(secret_names) - set(checked_policy.allowed_secret_names):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_CREDENTIAL_SCOPE_UNKNOWN",
            ",".join(sorted(set(secret_names) - set(checked_policy.allowed_secret_names))),
        )
    expected_groups = [list(group) for group in checked_policy.required_credential_groups]
    if credentials.get("required_groups_satisfied") != expected_groups:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_CREDENTIAL_GROUP_ATTESTATION",
            str(credentials.get("required_groups_satisfied")),
        )
    if any(
        not set(group).intersection(secret_names)
        for group in checked_policy.required_credential_groups
    ):
        raise OpsReleasePromotionError(
            "DEPLOYMENT_REQUIRED_CREDENTIAL_MISSING",
            ",".join(secret_names),
        )
    _require_safety_boundary(payload, "deployment")
    deployment_id = _text(payload.get("deployment_id"), "deployment_id")
    expected_id = _content_id("ops_deployment_", payload, "deployment_id")
    if deployment_id != expected_id:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_RECEIPT_ID_MISMATCH",
            f"expected={expected_id};observed={deployment_id}",
        )
    if verify_live_runtime:
        if runtime_root is None or development_root is None or runtime_python is None:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_LIVE_ARGUMENT_MISSING",
                "runtime_root/development_root/runtime_python",
            )
        observed_runtime = inspect_runtime_provenance(
            runtime_root=runtime_root,
            development_root=development_root,
            runtime_python=runtime_python,
            candidate_commit=release_commit,
            policy=checked_policy,
        )
        live_fields = (
            "root",
            "development_checkout_root",
            "git_common_dir",
            "development_git_common_dir",
            "head_commit",
            "origin",
            "reviewed_remote_ref",
            "reviewed_remote_commit",
            "python_executable",
            "python_sha256",
            "package_module",
            "package_file",
            "project_root",
            "git_exclude",
            "installed_distributions",
            "environment_fingerprint",
        )
        drift = [
            field for field in live_fields if runtime.get(field) != observed_runtime.get(field)
        ]
        if drift:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_RUNTIME_DRIFT",
                ",".join(drift),
            )
        if not legacy:
            promotion_path = Path(
                _text(
                    _mapping(payload.get("promotion_event"), "promotion_event").get(
                        "absolute_path"
                    ),
                    "promotion_event.absolute_path",
                )
            ).resolve()
            promotion_payload = _read_json_mapping(promotion_path)
            scheduler_payload = _mapping(payload.get("scheduler"), "scheduler")
            if _parse_aware_datetime(
                scheduler_payload.get("observed_at"),
                "scheduler.observed_at",
            ) < _parse_aware_datetime(
                promotion_payload.get("observed_at"),
                "promotion_event.observed_at",
            ):
                raise OpsReleasePromotionError(
                    "DEPLOYMENT_SCHEDULER_OBSERVATION_STALE",
                    str(promotion_path),
                )


def activate_ops_deployment(
    payload: Mapping[str, object],
    *,
    runtime_root: Path,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
) -> Path:
    policy = load_ops_release_promotion_policy(policy_path)
    validate_ops_deployment_acceptance(
        payload,
        policy=policy,
        runtime_root=runtime_root,
        development_root=Path(
            _text(
                _mapping(payload.get("runtime"), "runtime").get("development_checkout_root"),
                "runtime.development_checkout_root",
            )
        ),
        runtime_python=Path(
            _text(
                _mapping(payload.get("runtime"), "runtime").get("python_executable"),
                "runtime.python_executable",
            )
        ),
        verify_live_runtime=True,
    )
    root = runtime_root.resolve()
    active_path = _safe_runtime_path(root, policy.active_receipt_relative_path)
    history_root = _safe_runtime_path(root, policy.history_relative_path)
    if active_path.exists():
        existing = _read_json_mapping(active_path)
        validate_ops_deployment_acceptance(existing, policy=policy)
        if dict(existing) == dict(payload):
            return active_path
        previous_commitment = payload.get("previous_acceptance")
        if not isinstance(previous_commitment, Mapping):
            raise OpsReleasePromotionError(
                "DEPLOYMENT_PREVIOUS_ACCEPTANCE_REQUIRED",
                str(active_path),
            )
        _validate_commitment_row(previous_commitment, True)
        previous_path = Path(
            _text(
                previous_commitment.get("absolute_path"),
                "previous_acceptance.absolute_path",
            )
        ).resolve()
        if previous_path != active_path:
            raise OpsReleasePromotionError(
                "DEPLOYMENT_PREVIOUS_ACCEPTANCE_PATH_MISMATCH",
                str(previous_path),
            )
        existing_id = _text(existing.get("deployment_id"), "deployment_id")
        history_path = history_root / f"{existing_id}.json"
        if not history_path.exists():
            write_json_atomic(history_path, dict(existing))
    elif payload.get("previous_acceptance") is not None:
        raise OpsReleasePromotionError(
            "DEPLOYMENT_PREVIOUS_ACCEPTANCE_UNEXPECTED",
            str(payload.get("previous_acceptance")),
        )
    write_json_atomic(active_path, dict(payload))
    return active_path


def promote_ops_release(
    *,
    coordinator_root: Path,
    runtime_root: Path,
    development_root: Path,
    release_candidate_path: Path,
    runtime_python: Path,
    policy_path: Path = DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH,
    observed_at: datetime | None = None,
) -> tuple[dict[str, object], Path]:
    policy = load_ops_release_promotion_policy(policy_path)
    coordinator = coordinator_root.resolve()
    root = runtime_root.resolve()
    timestamp = _aware(observed_at)
    if not root.is_dir():
        raise OpsReleasePromotionError("PROMOTION_RUNTIME_ROOT_MISSING", str(root))
    candidate = _read_json_mapping(release_candidate_path)
    validate_ops_release_candidate(
        candidate,
        policy=policy,
        verify_live_artifacts=True,
        artifact_root=coordinator,
    )
    candidate_commit = _text(candidate.get("candidate_commit"), "candidate_commit")
    lock_path = _safe_runtime_path(root, policy.lock_relative_path)
    transaction_root = _safe_runtime_path(root, policy.transaction_relative_path)
    release_id = _text(candidate.get("release_id"), "release_id")
    candidate_evidence_path = (
        _safe_runtime_path(root, policy.evidence_relative_path)
        / release_id
        / "release_candidate.json"
    )
    transaction_id = (
        "ops_promotion_"
        + hashlib.sha256(f"{candidate_commit}|{timestamp.isoformat()}".encode()).hexdigest()[:24]
    )
    lock_payload = {
        "schema_version": "ops_release_promotion_lock.v1",
        "transaction_id": transaction_id,
        "candidate_commit": candidate_commit,
        "coordinator_root": str(coordinator),
        "acquired_at": timestamp.isoformat(),
        "production_effect": "none",
    }
    _write_json_exclusive(lock_path, lock_payload)
    previous_commit: str | None = None
    switched = False
    try:
        dirty_paths = _governed_dirty_paths(
            root,
            policy_source_root=coordinator,
        )
        if dirty_paths:
            raise OpsReleasePromotionError(
                "PROMOTION_RUNTIME_DIRTY",
                ",".join(dirty_paths),
            )
        active_leases = _active_checkout_leases(
            runtime_root=root,
            policy_source_root=coordinator,
        )
        if len(active_leases) != policy.active_daily_lease_required_count:
            raise OpsReleasePromotionError(
                "PROMOTION_ACTIVE_DAILY_LEASE",
                ",".join(active_leases),
            )
        runtime_common = _git_common_dir(root)
        development_common = _git_common_dir(development_root.resolve())
        if _path_key(runtime_common) == _path_key(development_common):
            raise OpsReleasePromotionError(
                "PROMOTION_GIT_COMMON_DIR_SHARED",
                str(runtime_common),
            )
        previous_commit = _git_text(root, "rev-parse", "HEAD")
        if candidate_evidence_path.exists():
            existing_candidate = _read_json_mapping(candidate_evidence_path)
            if dict(existing_candidate) != dict(candidate):
                raise OpsReleasePromotionError(
                    "PROMOTION_CANDIDATE_EVIDENCE_CONFLICT",
                    str(candidate_evidence_path),
                )
        else:
            write_json_atomic(candidate_evidence_path, dict(candidate))
        candidate_evidence = _single_file_commitment(
            candidate_evidence_path,
            root=root,
        )
        _write_promotion_event(
            transaction_root=transaction_root,
            transaction_id=transaction_id,
            sequence=1,
            state="PREPARED",
            candidate=candidate,
            previous_commit=previous_commit,
            observed_at=timestamp,
            candidate_receipt=candidate_evidence,
        )
        _copy_release_validation_evidence(
            candidate,
            source_root=coordinator,
            target_root=root,
            transaction_id=transaction_id,
        )
        _git_run(root, "fetch", "--no-tags", "origin", "main")
        fetched_commit = _git_text(root, "rev-parse", policy.reviewed_remote_ref)
        if fetched_commit != candidate_commit:
            raise OpsReleasePromotionError(
                "PROMOTION_REMOTE_REF_MISMATCH",
                f"fetched={fetched_commit};candidate={candidate_commit}",
            )
        _git_is_ancestor(root, candidate_commit, policy.reviewed_remote_ref)
        _git_run(root, "checkout", "--detach", candidate_commit)
        switched = True
        _write_promotion_event(
            transaction_root=transaction_root,
            transaction_id=transaction_id,
            sequence=2,
            state="SWITCHED",
            candidate=candidate,
            previous_commit=previous_commit,
            observed_at=_aware(None),
            candidate_receipt=candidate_evidence,
        )
        validate_ops_release_candidate(
            candidate,
            policy=policy,
            verify_live_artifacts=True,
            artifact_root=root,
        )
        runtime = inspect_runtime_provenance(
            runtime_root=root,
            development_root=development_root,
            runtime_python=runtime_python,
            candidate_commit=candidate_commit,
            policy=policy,
        )
        final_payload, final_path = _write_promotion_event(
            transaction_root=transaction_root,
            transaction_id=transaction_id,
            sequence=3,
            state="PROMOTED_NOT_ACTIVATED",
            candidate=candidate,
            previous_commit=previous_commit,
            observed_at=_aware(None),
            runtime=runtime,
            candidate_receipt=candidate_evidence,
        )
        return final_payload, final_path
    except Exception as exc:
        rollback_error: str | None = None
        if switched and previous_commit is not None:
            try:
                _git_run(root, "checkout", "--detach", previous_commit)
            except Exception as rollback_exc:  # pragma: no cover - tested via injected Git failure
                rollback_error = f"{type(rollback_exc).__name__}: {rollback_exc}"
        state = "ROLLBACK_FAILED" if rollback_error else "ROLLED_BACK"
        _write_promotion_event(
            transaction_root=transaction_root,
            transaction_id=transaction_id,
            sequence=4,
            state=state,
            candidate=candidate,
            previous_commit=previous_commit,
            observed_at=_aware(None),
            error=f"{type(exc).__name__}: {exc}",
            rollback_error=rollback_error,
            candidate_receipt=(
                None
                if not candidate_evidence_path.exists()
                else _single_file_commitment(candidate_evidence_path, root=root)
            ),
        )
        if rollback_error is not None:
            raise OpsReleasePromotionError(
                "PROMOTION_ROLLBACK_FAILED",
                rollback_error,
            ) from exc
        raise
    finally:
        _release_promotion_lock(lock_path, transaction_id=transaction_id)


def _write_promotion_event(
    *,
    transaction_root: Path,
    transaction_id: str,
    sequence: int,
    state: str,
    candidate: Mapping[str, object],
    previous_commit: str | None,
    observed_at: datetime,
    runtime: Mapping[str, object] | None = None,
    candidate_receipt: Mapping[str, object] | None = None,
    error: str | None = None,
    rollback_error: str | None = None,
) -> tuple[dict[str, object], Path]:
    payload: dict[str, object] = {
        "schema_version": _PROMOTION_TRANSACTION_SCHEMA,
        "transaction_id": transaction_id,
        "sequence": sequence,
        "state": state,
        "release_id": candidate.get("release_id"),
        "candidate_commit": candidate.get("candidate_commit"),
        "previous_commit": previous_commit,
        "runtime": None if runtime is None else dict(runtime),
        "release_candidate_receipt": (
            None if candidate_receipt is None else dict(candidate_receipt)
        ),
        "error": error,
        "rollback_error": rollback_error,
        "observed_at": observed_at.isoformat(),
        **_safety_boundary(),
    }
    event_path = transaction_root / transaction_id / f"{sequence:02d}_{state}.json"
    if event_path.exists():
        raise OpsReleasePromotionError("PROMOTION_EVENT_EXISTS", str(event_path))
    write_json_atomic(event_path, payload)
    return payload, event_path


def _active_checkout_leases(
    *,
    runtime_root: Path,
    policy_source_root: Path,
) -> tuple[str, ...]:
    guard = CheckoutLeaseGuard(
        project_root=runtime_root,
        policy_path=(
            policy_source_root / "config" / "architecture" / "arch_005_s4d_checkout_guard.yaml"
        ),
        parallel_policy_path=(
            policy_source_root / "config" / "architecture" / "arch_005_parallel_control_policy.yaml"
        ),
    )
    replay = guard.replay()
    return tuple(sorted(lease.lease_id for lease in replay.lease_heads if lease.state == "ACTIVE"))


def _runtime_probe(
    *,
    runtime_root: Path,
    runtime_python: Path,
    package_module: str,
) -> Mapping[str, object]:
    code = (
        "import importlib,importlib.metadata,json,sys;"
        f"m=importlib.import_module({package_module!r});"
        "c=importlib.import_module('ai_trading_system.config');"
        "d=sorted(({'name':str(x.metadata.get('Name') or ''),'version':str(x.version)} "
        "for x in importlib.metadata.distributions()),"
        "key=lambda r:(r['name'].casefold(),r['version']));"
        "print(json.dumps({'executable':sys.executable,'module_file':m.__file__,"
        "'project_root':str(c.PROJECT_ROOT),'installed_distributions':d}))"
    )
    probe_environment = os.environ.copy()
    # The coordinator may need PYTHONPATH to load the candidate CLI before a
    # release is installed. That path must never influence the runtime-local
    # interpreter probe or make a valid isolated runtime appear to import from
    # the development checkout.
    probe_environment.pop("PYTHONPATH", None)
    probe_environment.pop("PYTHONHOME", None)
    try:
        result = subprocess.run(
            (str(runtime_python), "-c", code),
            cwd=runtime_root,
            env=probe_environment,
            check=False,
            text=True,
            capture_output=True,
        )
    except OSError as exc:
        raise OpsReleasePromotionError(
            "RUNTIME_PROBE_EXECUTION",
            f"{type(exc).__name__}: {exc}",
        ) from exc
    if result.returncode != 0:
        raise OpsReleasePromotionError(
            "RUNTIME_PROBE_FAILED",
            (result.stderr or result.stdout or "unknown").strip(),
        )
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise OpsReleasePromotionError("RUNTIME_PROBE_JSON", str(exc)) from exc
    if not isinstance(payload, Mapping):
        raise OpsReleasePromotionError("RUNTIME_PROBE_PAYLOAD", "not mapping")
    return payload


def _normalize_distribution_inventory(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError(
            "RUNTIME_DISTRIBUTION_INVENTORY_INVALID",
            "missing-or-empty",
        )
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in value:
        row = _mapping(raw, "installed_distribution")
        name = _text(row.get("name"), "installed_distribution.name")
        version = _text(row.get("version"), "installed_distribution.version")
        key = name.casefold()
        if key in seen:
            raise OpsReleasePromotionError(
                "RUNTIME_DISTRIBUTION_DUPLICATE",
                name,
            )
        seen.add(key)
        rows.append({"name": name, "version": version})
    return sorted(
        rows,
        key=lambda row: (row["name"].casefold(), row["version"]),
    )


def _distribution_fingerprint(rows: Sequence[Mapping[str, str]]) -> str:
    return hashlib.sha256(
        json.dumps(
            list(rows),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _runtime_git_exclude_bytes(policy: OpsReleasePromotionPolicy) -> bytes:
    text = "\n".join((_RUNTIME_GIT_EXCLUDE_HEADER, *policy.git_exclude_patterns, ""))
    return text.encode("utf-8")


def _runtime_git_exclude_path(root: Path) -> Path:
    common = _git_common_dir(root)
    raw = _git_text(
        root,
        "rev-parse",
        "--path-format=absolute",
        "--git-path",
        "info/exclude",
    )
    path = Path(raw).resolve()
    if not path.is_relative_to(common):
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_EXCLUDE_PATH_OUTSIDE_COMMON_DIR",
            str(path),
        )
    return path


def _inspect_runtime_git_exclusions(
    root: Path,
    policy: OpsReleasePromotionPolicy,
) -> dict[str, object]:
    path = _runtime_git_exclude_path(root)
    if not path.is_file():
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_EXCLUDE_MISSING",
            str(path),
        )
    expected = _runtime_git_exclude_bytes(policy)
    observed = path.read_bytes()
    if observed != expected:
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_EXCLUDE_DRIFT",
            str(path),
        )
    return {
        **_single_file_commitment(path, root=_git_common_dir(root)),
        "patterns": list(policy.git_exclude_patterns),
        "managed": True,
    }


def _governed_dirty_paths(
    root: Path,
    *,
    policy_source_root: Path | None = None,
) -> tuple[str, ...]:
    policy_root = root.resolve() if policy_source_root is None else policy_source_root.resolve()
    guard = CheckoutLeaseGuard(
        project_root=root,
        policy_path=(policy_root / "config" / "architecture" / "arch_005_s4d_checkout_guard.yaml"),
        parallel_policy_path=(
            policy_root / "config" / "architecture" / "arch_005_parallel_control_policy.yaml"
        ),
    )
    dirty_paths: tuple[str, ...] = guard.audit_worktree().dirty_paths
    return dirty_paths


def _copy_release_validation_evidence(
    candidate: Mapping[str, object],
    *,
    source_root: Path,
    target_root: Path,
    transaction_id: str,
) -> None:
    validation_rows = candidate.get("validation_artifacts")
    if not isinstance(validation_rows, list) or not validation_rows:
        raise OpsReleasePromotionError(
            "RELEASE_VALIDATION_EVIDENCE_MISSING",
            "empty",
        )

    canary_row = _mapping(candidate.get("pre_release_canary"), "pre_release_canary")
    canary_source = _resolve_portable_commitment_path(source_root, canary_row)
    canary_payload = _read_json_mapping(canary_source)
    scenario_rows = [
        _mapping(row.get("evidence"), "canary.scenario.evidence")
        for row in _mapping_rows(canary_payload.get("scenarios"), "canary.scenarios")
    ]
    rows = [
        *(_mapping(raw, "validation_artifact") for raw in validation_rows),
        *scenario_rows,
        canary_row,
    ]
    copied_paths: set[str] = set()
    for row in rows:
        relative = _text(row.get("path"), "path")
        if relative in copied_paths:
            continue
        copied_paths.add(relative)
        source = _resolve_portable_commitment_path(source_root, row)
        target = _resolve_portable_commitment_path(target_root, row)
        if target.exists():
            _validate_portable_commitment_row(
                row,
                verify_live=True,
                artifact_root=target_root,
            )
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{transaction_id}.tmp")
        if temporary.exists():
            raise OpsReleasePromotionError(
                "PROMOTION_VALIDATION_TEMP_CONFLICT",
                str(temporary),
            )
        try:
            with source.open("rb") as source_handle:
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                )
                with os.fdopen(descriptor, "wb") as target_handle:
                    for block in iter(lambda: source_handle.read(1024 * 1024), b""):
                        target_handle.write(block)
                    target_handle.flush()
                    os.fsync(target_handle.fileno())
            if target.exists():
                raise OpsReleasePromotionError(
                    "PROMOTION_VALIDATION_EVIDENCE_CONFLICT",
                    str(target),
                )
            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)
        _validate_portable_commitment_row(
            row,
            verify_live=True,
            artifact_root=target_root,
        )


def _file_commitments(
    paths: Sequence[Path],
    *,
    root: Path,
    require_validation_pass: bool,
    expected_validation_commit: str | None = None,
) -> list[dict[str, object]]:
    rows = [
        _portable_file_commitment(path.resolve(), root=root)
        for path in sorted(paths, key=lambda item: str(item.resolve()).casefold())
    ]
    for row in rows:
        if require_validation_pass:
            validation_path = _resolve_portable_commitment_path(root, row)
            row["validation_status"] = _read_validation_status(validation_path)
            row["validation_tier"] = _read_validation_tier(validation_path)
            validation_commit = _read_validation_commit(validation_path)
            if validation_commit != expected_validation_commit:
                raise OpsReleasePromotionError(
                    "RELEASE_VALIDATION_COMMIT_MISMATCH",
                    (
                        f"{validation_path}:expected={expected_validation_commit};"
                        f"observed={validation_commit}"
                    ),
                )
            row["validation_git_commit"] = validation_commit
    return rows


def _portable_file_commitment(path: Path, *, root: Path) -> dict[str, object]:
    resolved_root = root.resolve()
    if not path.is_relative_to(resolved_root):
        raise OpsReleasePromotionError(
            "RELEASE_COMMITMENT_OUTSIDE_CHECKOUT",
            str(path),
        )
    if not path.is_file():
        raise OpsReleasePromotionError("RECEIPT_COMMITMENT_MISSING", str(path))
    relative = path.relative_to(resolved_root).as_posix()
    return {
        "path": relative,
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _single_file_commitment(path: Path, *, root: Path) -> dict[str, object]:
    if not path.is_file():
        raise OpsReleasePromotionError("RECEIPT_COMMITMENT_MISSING", str(path))
    try:
        display_path = path.relative_to(root.resolve()).as_posix()
    except ValueError:
        display_path = str(path)
    return {
        "path": display_path,
        "absolute_path": str(path),
        "size_bytes": path.stat().st_size,
        "sha256": _sha256_file(path),
    }


def _read_validation_status(path: Path) -> str:
    payload = _read_json_mapping(path)
    for field in ("status", "overall_status"):
        value = payload.get(field)
        if value is not None:
            status = str(value)
            if status != "PASS":
                raise OpsReleasePromotionError(
                    "RELEASE_VALIDATION_NOT_PASS",
                    f"{path}:{status}",
                )
            return status
    raise OpsReleasePromotionError("RELEASE_VALIDATION_STATUS_MISSING", str(path))


def _read_validation_commit(path: Path) -> str:
    payload = _read_json_mapping(path)
    commit = _text(payload.get("git_commit"), "git_commit")
    _require_commit(commit, "git_commit")
    return commit


def _read_validation_tier(path: Path) -> str:
    payload = _read_json_mapping(path)
    return _text(payload.get("tier"), "tier")


def _validate_required_validation_tiers(
    value: object,
    required_tiers: tuple[str, ...],
) -> None:
    if not isinstance(value, list):
        raise OpsReleasePromotionError("RELEASE_VALIDATION_TIERS_INVALID", "not-list")
    observed = [
        _text(_mapping(row, "validation_artifact").get("validation_tier"), "validation_tier")
        for row in value
    ]
    if len(observed) != len(set(observed)):
        raise OpsReleasePromotionError(
            "RELEASE_VALIDATION_TIER_DUPLICATE",
            ",".join(observed),
        )
    if set(observed) != set(required_tiers):
        raise OpsReleasePromotionError(
            "RELEASE_VALIDATION_TIER_SET_MISMATCH",
            (f"expected={','.join(required_tiers)};observed={','.join(sorted(observed))}"),
        )


def _validate_required_critical_paths(
    value: object,
    required_paths: tuple[str, ...],
) -> None:
    if not isinstance(value, list):
        raise OpsReleasePromotionError("RELEASE_CRITICAL_PATHS_INVALID", "not-list")
    observed = [
        _text(_mapping(row, "critical_path_commitment").get("path"), "path") for row in value
    ]
    if len(observed) != len(set(observed)):
        raise OpsReleasePromotionError(
            "RELEASE_CRITICAL_PATH_DUPLICATE",
            ",".join(observed),
        )
    if tuple(sorted(observed, key=str.casefold)) != tuple(sorted(required_paths, key=str.casefold)):
        raise OpsReleasePromotionError(
            "RELEASE_CRITICAL_PATH_SET_MISMATCH",
            (
                f"expected={','.join(required_paths)};"
                f"observed={','.join(sorted(observed, key=str.casefold))}"
            ),
        )


def _validate_commitment_rows(
    value: object,
    *,
    field: str,
    verify_live: bool,
    artifact_root: Path | None,
    require_validation_pass: bool,
    expected_validation_commit: str | None = None,
) -> None:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError("RECEIPT_COMMITMENTS_INVALID", field)
    for raw in value:
        row = _mapping(raw, field)
        _validate_portable_commitment_row(
            row,
            verify_live=verify_live,
            artifact_root=artifact_root,
        )
        if require_validation_pass and row.get("validation_status") != "PASS":
            raise OpsReleasePromotionError("RELEASE_VALIDATION_NOT_PASS", str(row.get("path")))
        if (
            require_validation_pass
            and row.get("validation_git_commit") != expected_validation_commit
        ):
            raise OpsReleasePromotionError(
                "RELEASE_VALIDATION_COMMIT_MISMATCH",
                (
                    f"{row.get('path')}:expected={expected_validation_commit};"
                    f"observed={row.get('validation_git_commit')}"
                ),
            )


def _validate_portable_commitment_row(
    row: Mapping[str, object],
    *,
    verify_live: bool,
    artifact_root: Path | None,
) -> None:
    relative = Path(_text(row.get("path"), "path"))
    if relative.is_absolute() or ".." in relative.parts:
        raise OpsReleasePromotionError(
            "RELEASE_COMMITMENT_PATH_INVALID",
            str(relative),
        )
    size = _nonnegative_int(row.get("size_bytes"), "size_bytes")
    sha256 = _text(row.get("sha256"), "sha256")
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise OpsReleasePromotionError("RECEIPT_COMMITMENT_SHA", sha256)
    if verify_live:
        if artifact_root is None:
            raise OpsReleasePromotionError(
                "RELEASE_ARTIFACT_ROOT_REQUIRED",
                str(relative),
            )
        path = _resolve_portable_commitment_path(artifact_root.resolve(), row)
        if not path.is_file():
            raise OpsReleasePromotionError("RECEIPT_COMMITMENT_MISSING", str(path))
        if path.stat().st_size != size or _sha256_file(path) != sha256:
            raise OpsReleasePromotionError("RECEIPT_COMMITMENT_DRIFT", str(path))


def _resolve_portable_commitment_path(
    root: Path,
    row: Mapping[str, object],
) -> Path:
    relative = Path(_text(row.get("path"), "path"))
    path = (root.resolve() / relative).resolve()
    if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(root.resolve()):
        raise OpsReleasePromotionError(
            "RELEASE_COMMITMENT_PATH_INVALID",
            str(relative),
        )
    return path


def _validate_commitment_row(row: Mapping[str, object], verify_live: bool) -> None:
    path = Path(_text(row.get("absolute_path"), "absolute_path"))
    size = _nonnegative_int(row.get("size_bytes"), "size_bytes")
    sha256 = _text(row.get("sha256"), "sha256")
    if not re.fullmatch(r"[0-9a-f]{64}", sha256):
        raise OpsReleasePromotionError("RECEIPT_COMMITMENT_SHA", sha256)
    if verify_live:
        if not path.is_file():
            raise OpsReleasePromotionError("RECEIPT_COMMITMENT_MISSING", str(path))
        if path.stat().st_size != size or _sha256_file(path) != sha256:
            raise OpsReleasePromotionError("RECEIPT_COMMITMENT_DRIFT", str(path))


def _read_json_mapping(path: Path) -> Mapping[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OpsReleasePromotionError(
            "RECEIPT_READ_FAILED",
            f"{path}:{type(exc).__name__}:{exc}",
        ) from exc
    if not isinstance(payload, Mapping):
        raise OpsReleasePromotionError("RECEIPT_PAYLOAD_INVALID", str(path))
    return payload


def _write_json_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    except FileExistsError as exc:
        raise OpsReleasePromotionError("PROMOTION_LOCK_ACTIVE", str(path)) from exc
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        path.unlink(missing_ok=True)
        raise


def _write_bytes_atomic(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.aits-ops-runtime-exclude.tmp")
    if temporary.exists():
        raise OpsReleasePromotionError(
            "RUNTIME_GIT_EXCLUDE_TEMP_CONFLICT",
            str(temporary),
        )
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
        )
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _release_promotion_lock(path: Path, *, transaction_id: str) -> None:
    if not path.exists():
        return
    payload = _read_json_mapping(path)
    if payload.get("transaction_id") != transaction_id:
        raise OpsReleasePromotionError(
            "PROMOTION_LOCK_IDENTITY_DRIFT",
            str(payload.get("transaction_id")),
        )
    path.unlink()


def _git_text(root: Path, *args: str) -> str:
    result = _git_result(root, *args)
    return (result.stdout or "").strip()


def _git_run(root: Path, *args: str) -> None:
    _git_result(root, *args)


def _git_result(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ("git", "-C", str(root), *args),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise OpsReleasePromotionError(
            "PROMOTION_GIT_EXECUTION",
            f"{type(exc).__name__}: {exc}",
        ) from exc
    if result.returncode != 0:
        raise OpsReleasePromotionError(
            "PROMOTION_GIT_FAILED",
            (result.stderr or result.stdout or "unknown").strip(),
        )
    return result


def _git_is_ancestor(root: Path, ancestor: str, descendant: str) -> None:
    try:
        result = subprocess.run(
            ("git", "-C", str(root), "merge-base", "--is-ancestor", ancestor, descendant),
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        raise OpsReleasePromotionError("PROMOTION_GIT_EXECUTION", str(exc)) from exc
    if result.returncode != 0:
        raise OpsReleasePromotionError(
            "RELEASE_NOT_ANCESTOR",
            f"{ancestor}!<={descendant}",
        )


def _git_common_dir(root: Path) -> Path:
    value = _git_text(root, "rev-parse", "--path-format=absolute", "--git-common-dir")
    path = Path(value)
    return path.resolve()


def _safe_runtime_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise OpsReleasePromotionError("RUNTIME_PATH_OUTSIDE", str(path))
    return path


def _content_id(prefix: str, payload: Mapping[str, object], id_field: str) -> str:
    canonical = {key: value for key, value in payload.items() if key != id_field}
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return prefix + hashlib.sha256(encoded).hexdigest()[:40]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _safety_boundary() -> dict[str, object]:
    return {
        "production_effect": "none",
        "production_weight_write": False,
        "active_shadow_weight_write": False,
        "broker_action": False,
        "trading_action": False,
    }


def _require_safety_boundary(payload: Mapping[str, object], field: str) -> None:
    expected = _safety_boundary()
    for key, value in expected.items():
        if payload.get(key) != value:
            raise OpsReleasePromotionError(
                "RECEIPT_SAFETY_BOUNDARY",
                f"{field}.{key}={payload.get(key)!r}",
            )


def _require_exact_mapping(
    payload: Mapping[str, object],
    expected: Mapping[str, object],
    field: str,
) -> None:
    for key, value in expected.items():
        if payload.get(key) != value:
            raise OpsReleasePromotionError(
                "PROMOTION_POLICY_SAFETY",
                f"{field}.{key}={payload.get(key)!r}",
            )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise OpsReleasePromotionError("RECEIPT_FIELD_MAPPING", field)
    return value


def _mapping_rows(value: object, field: str) -> list[Mapping[str, object]]:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError("RECEIPT_MAPPING_ROWS_REQUIRED", field)
    return [_mapping(row, field) for row in value]


def _canonical_prompt_text(raw: bytes, path: Path) -> str:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise OpsReleasePromotionError(
            "SCHEDULER_CANONICAL_PROMPT_ENCODING",
            str(path),
        ) from exc
    normalized_newlines = text.replace("\r\n", "\n")
    if "\r" in normalized_newlines:
        raise OpsReleasePromotionError("SCHEDULER_CANONICAL_PROMPT_INVALID", str(path))
    normalized = (
        normalized_newlines[:-1] if normalized_newlines.endswith("\n") else normalized_newlines
    )
    if not normalized:
        raise OpsReleasePromotionError("SCHEDULER_CANONICAL_PROMPT_INVALID", str(path))
    return normalized


def _parse_aware_datetime(value: object, field: str) -> datetime:
    text = _text(value, field)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise OpsReleasePromotionError("RECEIPT_TIMESTAMP_INVALID", field) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OpsReleasePromotionError("RECEIPT_TIMESTAMP_TIMEZONE_REQUIRED", field)
    return parsed


def _parse_automation_updated_at(value: object, field: str) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OpsReleasePromotionError("SCHEDULER_CONFIG_UPDATED_AT_INVALID", field)
    return datetime.fromtimestamp(value / 1000, tz=UTC)


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise OpsReleasePromotionError("RECEIPT_FIELD_TEXT", field)
    return value


def _bool(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise OpsReleasePromotionError("PROMOTION_POLICY_BOOL", field)
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise OpsReleasePromotionError("PROMOTION_POLICY_POSITIVE_INT", field)
    return value


def _nonnegative_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OpsReleasePromotionError("PROMOTION_POLICY_NONNEGATIVE_INT", field)
    return value


def _text_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError("RECEIPT_FIELD_TEXT_LIST", field)
    if not all(isinstance(item, str) and item for item in value):
        raise OpsReleasePromotionError("RECEIPT_FIELD_TEXT_LIST", field)
    if len(set(value)) != len(value):
        raise OpsReleasePromotionError("RECEIPT_FIELD_DUPLICATE", field)
    return tuple(value)


def _text_tuple_groups(value: object, field: str) -> tuple[tuple[str, ...], ...]:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError("RECEIPT_FIELD_TEXT_GROUPS", field)
    groups: list[tuple[str, ...]] = []
    for index, raw in enumerate(value):
        groups.append(_text_tuple(raw, f"{field}[{index}]"))
    if len(set(groups)) != len(groups):
        raise OpsReleasePromotionError("RECEIPT_FIELD_DUPLICATE", field)
    return tuple(groups)


def _scheduler_invocation_windows(
    value: object,
    field: str,
) -> tuple[tuple[str, int, int], ...]:
    if not isinstance(value, list) or not value:
        raise OpsReleasePromotionError("SCHEDULER_INVOCATION_WINDOWS_INVALID", field)
    windows: list[tuple[str, int, int]] = []
    for index, raw in enumerate(value):
        row = _mapping(raw, f"{field}[{index}]")
        role = _text(row.get("role"), f"{field}[{index}].role")
        hour = _nonnegative_int(row.get("hour"), f"{field}[{index}].hour")
        minute = _nonnegative_int(row.get("minute"), f"{field}[{index}].minute")
        if hour > 23 or minute > 59:
            raise OpsReleasePromotionError(
                "SCHEDULER_INVOCATION_WINDOWS_INVALID",
                f"{field}[{index}]={hour:02d}:{minute:02d}",
            )
        windows.append((role, hour, minute))
    if len(set(windows)) != len(windows):
        raise OpsReleasePromotionError("SCHEDULER_INVOCATION_WINDOWS_INVALID", field)
    return tuple(windows)


def _automation_cwds(config: Mapping[str, object]) -> tuple[str, ...]:
    value = config.get("cwds")
    if value is None:
        return ()
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise OpsReleasePromotionError("SCHEDULER_CONFIG_CWDS_INVALID", "cwds")
    if len(set(value)) != len(value):
        raise OpsReleasePromotionError("SCHEDULER_CONFIG_CWDS_INVALID", "cwds")
    return tuple(value)


def _relative_policy_path(value: object, field: str) -> str:
    text = _text(value, field)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts:
        raise OpsReleasePromotionError("PROMOTION_POLICY_PATH", field)
    return text


def _require_commit(value: str, field: str) -> None:
    if not _COMMIT_PATTERN.fullmatch(value):
        raise OpsReleasePromotionError("RELEASE_COMMIT_FORMAT", field)


def _aware(value: datetime | None) -> datetime:
    timestamp = value or datetime.now(tz=UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise OpsReleasePromotionError("PROMOTION_TIME_NAIVE", timestamp.isoformat())
    return timestamp


def _path_key(path: Path) -> str:
    return os.path.normcase(str(path.resolve()))


__all__ = [
    "DEFAULT_OPS_RELEASE_PROMOTION_POLICY_PATH",
    "OpsReleasePromotionError",
    "OpsReleasePromotionPolicy",
    "activate_ops_deployment",
    "build_ops_deployment_acceptance",
    "build_ops_release_candidate",
    "inspect_runtime_provenance",
    "load_ops_release_promotion_policy",
    "promote_ops_release",
    "validate_ops_deployment_acceptance",
    "validate_ops_release_candidate",
    "validate_scheduler_observation",
]
