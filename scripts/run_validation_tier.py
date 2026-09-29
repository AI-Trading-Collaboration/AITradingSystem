# ruff: noqa: E402
from __future__ import annotations

import argparse
import codecs
import hashlib
import json
import math
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import TYPE_CHECKING, Any, TypedDict, TypeGuard

if TYPE_CHECKING:
    from ai_trading_system.platform.architecture.source_preservation import HeldGitConfiguration
    from ai_trading_system.platform.architecture.workflow_coordination import (
        ExecutionLifecycle,
        WindowsWorkerExchange,
    )
    from ai_trading_system.platform.architecture.workflow_execution import WindowsWorkerToken

# The runner must prefer this worktree's src package when executed as a script.
REPO_ROOT_FOR_IMPORTS = Path(__file__).resolve().parents[1]
SRC_ROOT_FOR_IMPORTS = REPO_ROOT_FOR_IMPORTS / "src"
if str(SRC_ROOT_FOR_IMPORTS) in sys.path:
    sys.path.remove(str(SRC_ROOT_FOR_IMPORTS))
sys.path.insert(0, str(SRC_ROOT_FOR_IMPORTS))

from ai_trading_system.platform.architecture.integration_publication_fence import (  # noqa: E402
    FULL_PROFILE_EVIDENCE_BUDGET_BYTES,
    IntegrationPublicationFence,
    PublicationFenceError,
)
from ai_trading_system.platform.architecture.validation_readiness import (
    CHECKER_IDS,
    check_full_readiness,
)
from ai_trading_system.platform.architecture.workflow_execution import (
    DEVX015_ACCEPTANCE_TASK,
    MANDATORY_ACCEPTANCE_REQUEST_ENV,
    ExecutionContainmentError,
    acceptance_runtime_identity,
    bind_acceptance_checkout,
    bind_acceptance_implementation,
    bind_inspector_implementation,
    bind_mandatory_acceptance,
    capture_acceptance_implementation,
    validate_mandatory_acceptance_result,
)
from ai_trading_system.platform.validation_parent_run_import import (
    PARENT_RUN_IMPORT_ENV as VALIDATION_PARENT_RUN_IMPORT_ENV,
)
from ai_trading_system.platform.validation_parent_run_import import (
    validate_parent_run_import,
)
from ai_trading_system.platform.validation_scheduling import (  # noqa: E402
    REAL_FULL_CHAIN_MARKER,
    SCHEDULING_MANIFEST_RELATIVE_PATH,
    SPLIT_SCOPE_EVIDENCE_SCHEMA_VERSION,
    SchedulingManifest,
    SchedulingManifestError,
    load_scheduling_manifest,
    parse_scheduling_manifest,
    split_scope_evidence_error,
)
from ai_trading_system.platform.validation_trigger_provenance import (  # noqa: E402
    BOUNDARY_ID_ENV as VALIDATION_BOUNDARY_ID_ENV,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    FULL_TRIGGER_REASONS as FULL_VALIDATION_TRIGGER_REASONS,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    IDENTIFIER_RE as PROVENANCE_IDENTIFIER_RE,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    PARENT_RUN_ENV as VALIDATION_PARENT_RUN_ENV,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    PROFILE_JSON_ENV as RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    SCHEMA_VERSION as VALIDATION_PROVENANCE_SCHEMA_VERSION,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    TASK_ID_ENV as VALIDATION_TASK_ID_ENV,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    TRIGGER_REASON_ENV as VALIDATION_TRIGGER_REASON_ENV,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    TRIGGER_REASONS as VALIDATION_TRIGGER_REASONS,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    load_json as load_provenance_json,
)
from ai_trading_system.platform.validation_trigger_provenance import (
    validate_full_provenance,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path, safe_load_yaml_text  # noqa: E402

DEFAULT_WORKERS = "16"
DEFAULT_DIST = "loadfile"
DEFAULT_ARTIFACT_ROOT = Path("outputs/validation_runtime")
SERIAL_WORKER_VALUES = {"", "0", "1", "serial", "none"}
PYTEST_OUTPUT_LOG_NAME = "pytest_output.log"
BENCHMARK_SUMMARY_NAME = "validation_benchmark_summary.json"
RUNTIME_PROFILE_OUTPUT_NAME = "test_runtime_profile.json"
RUNTIME_PROFILE_SCHEMA_VERSION = "test_runtime_profile.v1"
RUNTIME_PROFILE_OUTPUT_ENV = "AITS_PYTEST_RUNTIME_PROFILE_OUTPUT"
RUNTIME_PROFILE_FORMAL_SELECTION_ENV = "AITS_PYTEST_RUNTIME_PROFILE_FORMAL_SELECTION"
BENCHMARK_REMOVED_VALIDATION_ENV_VARS = (
    RUNTIME_PROFILE_OUTPUT_ENV,
    RUNTIME_PROFILE_FORMAL_SELECTION_ENV,
    RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV,
    VALIDATION_TRIGGER_REASON_ENV,
    VALIDATION_TASK_ID_ENV,
    VALIDATION_BOUNDARY_ID_ENV,
    VALIDATION_PARENT_RUN_ENV,
    VALIDATION_PARENT_RUN_IMPORT_ENV,
)
FULL_RUNTIME_PROFILE_PLUGIN = "scripts.pytest_runtime_profile"
FULL_DURATION_PROFILE_MANIFEST = "inputs/architecture/arch_004g2_full_duration_profile.yaml"
FULL_TEST_MANIFEST = "inputs/architecture/arch_004e_test_manifest.yaml"
FULL_NON_SELECTION_PYTEST_ARG_OPTIONS = (
    "--junitxml",
    "--junit-xml",
    "--junit-prefix",
)
PYTEST_SLOW_DURATION_RE = re.compile(
    r"^\s*(?P<seconds>\d+(?:\.\d+)?)s\s+"
    r"(?P<phase>call|setup|teardown)\s+"
    r"(?P<nodeid>.+?)\s*$"
)
SAFETY_BOUNDARY = {
    "strategy_logic_changed": False,
    "production_effect": "none",
    "production_state_mutated": False,
    "cached_data_mutated": False,
    "broker_action_allowed": False,
    "broker_action_taken": False,
}


@dataclass(frozen=True)
class TierSpec:
    description: str
    suite_family: str
    promotion_blocking: bool
    slow_suite_allowed: bool
    paths: tuple[str, ...] = ()
    match_terms: tuple[str, ...] = ()
    manifest_categories: tuple[str, ...] = ()
    pytest_args: tuple[str, ...] = ("-q", "--durations=20", "--durations-min=1")
    # DEVX-018 O2: markers deselected in this pre-Full tier. The marks come from the
    # reviewed scheduling manifest via the runtime profile plugin; Full never excludes.
    excluded_markers: tuple[str, ...] = ()


TIER_SPECS: dict[str, TierSpec] = {
    "fast-unit": TierSpec(
        description=(
            "Fast local gate for CLI wiring, validation runner behavior, documentation contract, "
            "and report registry changes."
        ),
        suite_family="fast_unit",
        promotion_blocking=True,
        slow_suite_allowed=False,
        paths=(
            "tests/test_validation_tier_script.py",
            "tests/test_arch_005_prebootstrap.py",
            "tests/test_arch_005_bootstrap_handoff.py",
            "tests/test_arch_005_task_registry_shadow.py",
            "tests/test_arch_005_s2_kernel.py",
            "tests/test_arch_005_s3_scheduler.py",
            "tests/test_arch_005_s4_dispatch.py",
            "tests/test_arch_004g_deprecation.py",
            "tests/test_documentation_contract.py",
            "tests/test_report_index.py",
            "tests/test_clean_clone_release_acceptance.py",
            "tests/test_engineering_release_candidate.py",
            "tests/test_artifact_lineage.py",
            "tests/test_report_quality_gate.py",
            "tests/test_cli_direct.py",
            "tests/test_etf_cli_aliases.py",
            "tests/test_research_master_roadmap.py",
            "tests/test_data_foundation_roadmap.py",
            "tests/test_data_foundation_acceptance.py",
            "tests/test_data_source_qualification_remediation.py",
            "tests/test_data_source_remediation_execution.py",
            "tests/test_data_source_requirement_matrix.py",
            "tests/test_current_subscription_data_coverage_audit.py",
            "tests/test_current_subscription_source_qualification.py",
            "tests/test_controlled_strategy_value_surface.py",
            "tests/test_controlled_strategy_regime_horizon.py",
            "tests/test_controlled_strategy_tail_risk_policy.py",
            "tests/test_controlled_strategy_candidate_batch.py",
            "tests/test_controlled_strategy_batch.py",
            "tests/test_tail_risk_fallback_falsification_audit.py",
            "tests/test_tail_risk_independent_validation_governance.py",
        ),
    ),
    "contract-validation": TierSpec(
        description=(
            "Promotion-facing contract gate for documentation, report registry, runtime "
            "validation artifacts, and formal research safety contracts."
        ),
        suite_family="contract_validation",
        promotion_blocking=True,
        slow_suite_allowed=False,
        paths=(
            "tests/test_validation_tier_script.py",
            "tests/test_arch_004g_deprecation.py",
            "tests/test_documentation_contract.py",
            "tests/test_report_index.py",
            "tests/test_clean_clone_release_acceptance.py",
            "tests/test_engineering_release_candidate.py",
            "tests/test_artifact_lineage.py",
            "tests/test_report_quality_gate.py",
            "tests/test_formal_research_method_contract.py",
            "tests/test_promotion_gate_threshold_calibration.py",
            "tests/test_paper_shadow_protocol.py",
            "tests/test_paper_shadow_daily.py",
            "tests/test_paper_shadow_drift_monitor.py",
            "tests/test_candidate_decision_ledger.py",
            "tests/test_evidence_staleness_monitor.py",
            "tests/test_stress_scenario_library.py",
            "tests/test_drawdown_event_casebook.py",
            "tests/test_flip_rotation_event_casebook.py",
            "tests/test_research_master_roadmap.py",
            "tests/test_data_foundation_roadmap.py",
            "tests/test_data_foundation_acceptance.py",
            "tests/test_data_source_qualification_remediation.py",
            "tests/test_data_source_remediation_execution.py",
            "tests/test_data_source_requirement_matrix.py",
            "tests/test_current_subscription_data_coverage_audit.py",
            "tests/test_current_subscription_source_qualification.py",
            "tests/test_controlled_strategy_value_surface.py",
            "tests/test_controlled_strategy_regime_horizon.py",
            "tests/test_controlled_strategy_tail_risk_policy.py",
            "tests/test_controlled_strategy_candidate_batch.py",
            "tests/test_controlled_strategy_batch.py",
            "tests/test_tail_risk_fallback_falsification_audit.py",
            "tests/test_tail_risk_independent_validation_governance.py",
        ),
    ),
    "report-validation": TierSpec(
        description="Reader Brief, report index, report navigation, and reader UX validation.",
        suite_family="report_validation",
        promotion_blocking=True,
        slow_suite_allowed=False,
        paths=(
            "tests/test_report_index.py",
            "tests/test_artifact_lineage.py",
            "tests/test_report_quality_gate.py",
            "tests/test_reader_brief.py",
            "tests/test_reader_brief_dynamic_v3_defensive_evidence.py",
            "tests/trading_engine",
        ),
        pytest_args=(
            "-k",
            "report_index or reader_brief",
            "-q",
            "--durations=25",
            "--durations-min=1",
        ),
    ),
    "integration": TierSpec(
        description=(
            "Scheduler, trading_engine, portfolio tooling, and cross-module integration tests."
        ),
        suite_family="integration",
        promotion_blocking=False,
        slow_suite_allowed=True,
        paths=("tests/trading_engine", "tests/test_ops_daily.py", "tests/test_scheduled_tasks.py"),
        pytest_args=("-q", "--durations=40", "--durations-min=1"),
    ),
    "reproducibility": TierSpec(
        description=(
            "Artifact lineage, source manifests, run manifest contracts, and engineering "
            "reproducibility readiness."
        ),
        suite_family="reproducibility",
        promotion_blocking=True,
        slow_suite_allowed=False,
        paths=(
            "tests/test_artifact_lineage.py",
            "tests/test_artifact_lifecycle_inventory.py",
            "tests/test_engineering_stage_b_readiness.py",
            "tests/test_pit_source_manifest.py",
            "tests/trading_engine/test_backtest_snapshot_manifest.py",
            "tests/trading_engine/test_backtest_manifest_refresh.py",
            "tests/trading_engine/test_validate_data_manifest_context.py",
        ),
        pytest_args=("-q", "--durations=25", "--durations-min=1"),
    ),
    "architecture-fitness": TierSpec(
        description=(
            "Generated ownership/test manifests, dependency ratchets, aggregate reproducibility, "
            "and architecture control-plane tests."
        ),
        suite_family="architecture_fitness",
        promotion_blocking=True,
        slow_suite_allowed=False,
        manifest_categories=("architecture",),
        excluded_markers=(REAL_FULL_CHAIN_MARKER,),
    ),
    "slow-research-regression": TierSpec(
        description="ETF dynamic-v3 rescue, historical replay, simulation, and advisory tests.",
        suite_family="slow_research_regression",
        promotion_blocking=False,
        slow_suite_allowed=True,
        match_terms=("dynamic_v3", "test_etf_dynamic_rescue", "test_backtest_sim_", "test_sim_"),
        pytest_args=("-q", "--durations=40", "--durations-min=1"),
    ),
    "full": TierSpec(
        description="Complete pytest gate. Keep this for final validation on broad changes.",
        suite_family="full_pytest",
        promotion_blocking=True,
        slow_suite_allowed=True,
        paths=("tests",),
        pytest_args=("-q", "--durations=50", "--durations-min=1"),
    ),
}
TIER_ALIASES: dict[str, str] = {
    "fast": "fast-unit",
    "reader-brief": "report-validation",
    "dynamic-v3": "slow-research-regression",
    "trading-engine": "integration",
    "artifact-reproduce": "reproducibility",
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _as_posix(path: Path) -> str:
    return path.as_posix()


def _discover_matching_tests(repo_root: Path, match_terms: Sequence[str]) -> list[str]:
    test_root = repo_root / "tests"
    discovered: list[str] = []
    for path in sorted(test_root.rglob("test_*.py")):
        normalized = _as_posix(path.relative_to(repo_root))
        if any(term in normalized for term in match_terms):
            discovered.append(normalized)
    return discovered


def _discover_manifest_tests(repo_root: Path, manifest_categories: Sequence[str]) -> list[str]:
    manifest_path = repo_root / "inputs/architecture/arch_004e_test_manifest.yaml"
    if not manifest_path.is_file():
        raise ValueError(f"generated test manifest not found: {manifest_path}")
    payload = safe_load_yaml_path(manifest_path)
    if not isinstance(payload, dict) or not isinstance(payload.get("tests"), list):
        raise ValueError("generated test manifest must contain a tests list")
    categories = set(manifest_categories)
    paths = []
    for row in payload["tests"]:
        if not isinstance(row, dict):
            continue
        if row.get("file_role") != "test" or str(row.get("category")) not in categories:
            continue
        path = str(row.get("path") or "")
        if path:
            paths.append(path)
    return sorted(set(paths))


def resolve_tier(tier: str) -> str:
    return TIER_ALIASES.get(tier, tier)


def _unique(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            unique_values.append(value)
    return unique_values


def _pytest_parallel_args(workers: str, dist: str) -> list[str]:
    normalized_workers = workers.strip().lower()
    if normalized_workers in SERIAL_WORKER_VALUES:
        return []
    return ["-n", workers.strip(), "--dist", dist.strip()]


def _runtime_worker_contract(workers: str, dist: str) -> tuple[int | None, str]:
    normalized_workers = workers.strip().lower()
    if normalized_workers in SERIAL_WORKER_VALUES:
        return 1, "no"
    try:
        worker_count = int(normalized_workers)
    except ValueError:
        worker_count = None
    return worker_count, dist.strip().lower()


def build_command(
    tier: str,
    *,
    python_executable: str,
    repo_root: Path,
    extra_pytest_args: Sequence[str] = (),
    workers: str = DEFAULT_WORKERS,
    dist: str = DEFAULT_DIST,
) -> list[str]:
    resolved_tier = resolve_tier(tier)
    spec = TIER_SPECS[resolved_tier]
    paths = list(spec.paths)
    if spec.match_terms:
        paths.extend(_discover_matching_tests(repo_root, spec.match_terms))
    if spec.manifest_categories:
        paths.extend(_discover_manifest_tests(repo_root, spec.manifest_categories))
    paths = _unique(paths)
    if not paths:
        raise ValueError(f"tier {tier!r} did not resolve any pytest paths")
    runtime_profile_prefix = (
        [
            "-p",
            FULL_RUNTIME_PROFILE_PLUGIN,
            "--aits-duration-profile",
            FULL_DURATION_PROFILE_MANIFEST,
        ]
        if resolved_tier == "full"
        else []
    )
    runtime_profile_suffix = ["--no-loadscope-reorder"] if resolved_tier == "full" else []
    if spec.excluded_markers and resolved_tier == "full":
        raise ValueError("Full must not exclude any marker")
    marker_exclusion = (
        [
            "-p",
            FULL_RUNTIME_PROFILE_PLUGIN,
            "-m",
            " and ".join(f"not {marker}" for marker in spec.excluded_markers),
        ]
        if spec.excluded_markers
        else []
    )
    return [
        python_executable,
        "-m",
        "pytest",
        *_pytest_parallel_args(workers, dist),
        *runtime_profile_prefix,
        *marker_exclusion,
        *paths,
        *spec.pytest_args,
        *extra_pytest_args,
        *runtime_profile_suffix,
    ]


def _test_selection_policy(spec: TierSpec, repo_root: Path) -> dict[str, object] | None:
    """Audit record for DEVX-018 marker exclusion; the manifest identity is re-read."""
    if not spec.excluded_markers:
        return None
    try:
        manifest = load_scheduling_manifest(repo_root)
    except SchedulingManifestError as exc:
        manifest_record: dict[str, object] | None = {"error": str(exc)}
    else:
        manifest_record = (
            None
            if manifest is None
            else {
                "path": manifest.relative_path,
                "sha256": manifest.sha256,
                "version": manifest.version,
                "real_full_chain_function_count": len(manifest.real_full_chain_functions),
            }
        )
    return {
        "excluded_markers": list(spec.excluded_markers),
        "formal_authority_for_excluded_nodes": "full",
        "scheduling_manifest": manifest_record,
    }


def _format_command(command: Sequence[str]) -> str:
    return " ".join(command)


def _command_exit_code(result: Mapping[str, object]) -> int:
    value = result["exit_code"]
    if type(value) is not int:
        raise TypeError("command exit_code must be an integer")
    return value


def _runtime_float(value: object) -> float:
    if not isinstance(value, (str, int, float)):
        raise TypeError("runtime numeric value must be a number or numeric string")
    return float(value)


def _run_leased_command(
    *,
    request: Mapping[str, Any],
    lifecycle: ExecutionLifecycle,
    actor: str,
    environment: Mapping[str, str],
    request_path: Path | None = None,
    validation_identity: Mapping[str, object] | None = None,
    worker_token: WindowsWorkerToken | None = None,
) -> dict[str, object]:
    """Run one reserved process tree; result adoption remains with the Full caller."""
    from ai_trading_system.platform.architecture.workflow_execution import (
        WindowsJobProcess,
        WindowsWorkerToken,
        execution_environment_sha256,
    )
    from ai_trading_system.platform.artifacts import canonical_json_bytes

    started = time.perf_counter()
    if worker_token is not None:
        if type(worker_token) is not WindowsWorkerToken:
            raise ExecutionContainmentError("WORKER_TOKEN_CAPABILITY_REQUIRED")
        worker_binding = worker_token.validate_launcher()
        if (request_path is None or validation_identity is None
                or validation_identity.get("worker_identity") != worker_binding):
            raise ExecutionContainmentError("FULL_WORKER_IDENTITY_UNBOUND")
    elif validation_identity is not None and "worker_identity" in validation_identity:
        raise ExecutionContainmentError("WORKER_TOKEN_CAPABILITY_REQUIRED")
    # Reject changed inputs before reserving execution or writing evidence.
    if execution_environment_sha256(environment) != request["environment_sha256"]:
        raise ExecutionContainmentError("FULL_EXECUTION_ENVIRONMENT_CHANGED")
    if validation_identity is not None:
        identity_raw = canonical_json_bytes(dict(validation_identity))
        if hashlib.sha256(identity_raw).hexdigest() != request["validation_identity_sha256"]:
            raise ExecutionContainmentError("FULL_VALIDATION_IDENTITY_CHANGED")
    reserved = lifecycle.reserve(request, actor=actor)
    if reserved["dispatch_allowed"] is not True:
        raise ExecutionContainmentError("FULL_EXECUTION_REPLAY_ONLY")
    parts: list[str] = []
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    process = None
    try:
        # Renew the existing execution lease; this foreground loop owns no scheduler.
        lifecycle.store.heartbeat(request["lease_id"], actor=actor, now=datetime.now(UTC))
        heartbeat_interval = lifecycle.store.policy.lease_ttl_seconds / 3
        heartbeat_due = time.monotonic() + heartbeat_interval
        if request_path is not None:
            from ai_trading_system.platform.architecture.workflow_contract import write_bound_once

            write_bound_once(
                request_path.parent, request_path.name, canonical_json_bytes(dict(request)),
            )
            if validation_identity is not None:
                write_bound_once(
                    request_path.parent, "execution_validation_identity.json", identity_raw,
                )
        launch = dict(
            argv=request["argv"],
            cwd=Path(request["cwd"]),
            environment=environment,
            stdout_path=Path(request["stdout_path"]),
            job_name=request["job_name"],
        )
        process = (
            WindowsJobProcess.create(**launch) if worker_token is None
            else WindowsJobProcess.create_as_worker(worker_token=worker_token, **launch)
        )
        lifecycle.bind(request["lease_id"], process, actor=actor)
        lifecycle.resume(request["lease_id"], process, actor=actor)
        with Path(request["stdout_path"]).open("rb") as stream:
            while True:
                # A polling interval is not a run timeout or permission to relaunch.
                try:
                    code = process.wait(timeout=0.25)
                except TimeoutError:
                    code = None
                if time.monotonic() >= heartbeat_due:
                    lifecycle.store.heartbeat(
                        request["lease_id"], actor=actor, now=datetime.now(UTC),
                    )
                    heartbeat_due = time.monotonic() + heartbeat_interval
                chunk = decoder.decode(stream.read(), final=code is not None)
                if chunk:
                    print(chunk, end="", flush=True)
                    parts.append(chunk)
                if code is not None:
                    break
        lifecycle.confirm_exit(request["lease_id"], process, actor=actor)
        identity = process.identity()
    except BaseException as exc:
        if process is not None:
            # Keep the live handle until both termination and custody are proved.
            try:
                process.terminate()
                lifecycle.confirm_exit(request["lease_id"], process, actor=actor)
                lifecycle.record_incomplete_result(request["lease_id"], actor=actor)
            except BaseException as cleanup_error:
                exc.add_note("execution custody incomplete: " + str(cleanup_error))
        raise
    finally:
        if process is not None:
            process.close()
    return {
        "command": list(request["argv"]),
        "exit_code": code,
        "elapsed_seconds": round(time.perf_counter() - started, 2),
        "pytest_output": "".join(parts),
        "execution_request_id": request["request_id"],
        "execution_process": identity,
        "execution_exit_confirmed": True,
    }


class _FullCommandRunner:
    """Bind the final argv/environment to the already-admitted ordinary Full claim."""

    def __init__(
        self, *, args: argparse.Namespace, root: Path, artifact_dir: Path,
        publication_binding: Mapping[str, object], provenance: Mapping[str, object],
        worker_token: WindowsWorkerToken | None = None,
        worker_environment: Mapping[str, str] | None = None,
        worker_exchange: WindowsWorkerExchange | None = None,
    ) -> None:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            WindowsWorkerExchange,
        )
        from ai_trading_system.platform.architecture.workflow_execution import WindowsWorkerToken

        if (worker_token is None) != (worker_environment is None):
            raise ExecutionContainmentError("FULL_WORKER_ENVIRONMENT_REQUIRED")
        if worker_token is not None and type(worker_token) is not WindowsWorkerToken:
            raise ExecutionContainmentError("WORKER_TOKEN_CAPABILITY_REQUIRED")
        self.worker_token = worker_token
        if worker_exchange is not None:
            if type(worker_exchange) is not WindowsWorkerExchange or worker_token is None:
                raise ExecutionContainmentError("FULL_WORKER_EXCHANGE_REQUIRED")
            worker_exchange.validate_worker(worker_token)
        self.worker_exchange = worker_exchange
        self.worker_environment = (
            dict(worker_environment) if worker_environment is not None else None
        )
        self.args = args
        self.root = root.resolve()
        self.artifact_dir = artifact_dir.resolve()
        self.binding = dict(publication_binding)
        self.provenance = dict(provenance)
        self.request: dict[str, Any] | None = None
        self.fence = IntegrationPublicationFence(project_root=self.root)
        if not self.artifact_dir.is_relative_to(self.root / DEFAULT_ARTIFACT_ROOT):
            raise ExecutionContainmentError("FULL_EXECUTION_ARTIFACT_SCOPE")

    def effective_environment(
        self, overrides: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """One environment source for mandatory identity and actual dispatch."""
        return {
            **(os.environ if self.worker_environment is None else self.worker_environment),
            **(overrides or {}),
        }

    def __call__(
        self, command: Sequence[str], *, cwd: Path,
        env_overrides: Mapping[str, str] | None = None,
    ) -> dict[str, object]:
        from ai_trading_system.platform.architecture.workflow_contract import canonical_digest
        from ai_trading_system.platform.architecture.workflow_coordination import machine_host_id
        from ai_trading_system.platform.architecture.workflow_execution import (
            execution_environment_sha256,
        )
        from ai_trading_system.platform.artifacts import canonical_json_bytes

        if cwd.resolve() != self.root or self.request is not None:
            raise ExecutionContainmentError("FULL_EXECUTION_CONTEXT_REUSE")
        if not command or Path(command[0]).absolute() != Path(sys.executable).absolute():
            raise ExecutionContainmentError("FULL_EXECUTION_INTERPRETER_UNBOUND")
        parent_path = _publication_parent_path(self.provenance, repo_root=self.root)
        current = self.fence.validate(
            self.args.publication_transaction, exact_phase="FULL_DISPATCHED",
            task_id=str(self.provenance["task_id"]), validation_tier="full",
            parent_path=parent_path,
            require_candidate=True,
        )
        _require_new_incomplete_parent_candidate(
            self.provenance, current, fence=self.fence,
            transaction=self.args.publication_transaction,
        )
        for key in ("transaction_sha256", "lease_id", "candidate_sha"):
            if current[key] != self.binding[key]:
                raise ExecutionContainmentError("FULL_EXECUTION_BINDING_CHANGED")
        self.fence.require_full_launcher(self.args.publication_transaction)
        # Final committed DONE tasks are allowed only by the existing fence.
        # Bind their real canonical fragment, never revive an old task authority.
        task_commitment = _full_task_commitment(
            self.root, str(self.provenance["task_id"]), str(current["candidate_sha"]),
        )
        if task_commitment != self.binding.get("task_commitment"):
            raise ExecutionContainmentError("FULL_EXECUTION_TASK_CHANGED")
        replay = self.fence.replay(self.args.publication_transaction)
        event = next(row for row in replay.events if row["phase"] == "FULL_DISPATCHED")
        lease = next(
            row for row in self.fence.guard.store.replay().active_leases
            if row.lease_id == current["lease_id"]
        )
        coordination = self.fence.guard.store.coordination_binding
        if coordination is not None:
            coordination.assert_current(operation="observe")
        worker_binding = (
            self.worker_token.validate_launcher() if self.worker_token is not None else None
        )
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        environment = self.effective_environment(env_overrides)
        request_id = canonical_digest({
            "transaction": current["transaction_sha256"],
            "full_run_id": event["payload"]["full_run_id"],
        })
        validation_identity = {
            "candidate_sha": current["candidate_sha"],
            "argv": list(command), "cwd": self.root.as_posix(),
            "environment_sha256": execution_environment_sha256(environment),
            "runtime": acceptance_runtime_identity(environment),
            "publication_policy_sha256": self.fence.policy_sha256,
            "pre_dispatch_readiness": self.binding["pre_dispatch_readiness"],
            "mandatory_acceptance_binding": self.binding.get("mandatory_acceptance_binding"),
            "task_authority_sha256": task_commitment["fragment_sha256"],
        }
        if worker_binding is not None:
            validation_identity["worker_identity"] = worker_binding
        self.request = {
            "schema_version": "workflow_execution_request.v1",
            "request_id": request_id, "lease_id": lease.lease_id,
            "manifest_sha256": lease.change_manifest_sha256,
            "subject_task_id": lease.task_id,
            "candidate_sha": current["candidate_sha"],
            "validation_identity_sha256": hashlib.sha256(
                canonical_json_bytes(validation_identity)
            ).hexdigest(),
            "task_authority_sha256": task_commitment["fragment_sha256"],
            "argv": list(command), "cwd": self.root.as_posix(),
            "environment_sha256": validation_identity["environment_sha256"],
            "stdout_path": (self.artifact_dir / "execution.stdout.log").as_posix(),
            "result_path": (self.artifact_dir / "execution_result.json").as_posix(),
            "job_name": "Local\\AITS-DEVX015-full-" + request_id,
            "host_id": coordination.host_id if coordination else machine_host_id(),
            "writer_epoch": coordination.epoch if coordination else "UNENROLLED_LEGACY",
        }
        result = _run_leased_command(
            request=self.request, lifecycle=self.fence.guard.store.execution_lifecycle(),
            actor="integration-coordinator", environment=environment,
            request_path=self.artifact_dir / "execution_request.json",
            validation_identity=validation_identity,
            worker_token=self.worker_token,
        )
        result["validation_identity_sha256"] = self.request["validation_identity_sha256"]
        return result

    def record_summary(self, summary_path: Path, *, status: str) -> None:
        from ai_trading_system.platform.architecture.workflow_contract import (
            bounded_regular_bytes,
            write_bound_once,
        )
        from ai_trading_system.platform.artifacts import canonical_json_bytes
        from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

        if self.request is None:
            if status == "PASS":
                raise ExecutionContainmentError("FULL_RESULT_WITHOUT_EXECUTION")
            return  # A pre-execution mandatory check may fail without a process.
        raw = bounded_regular_bytes(summary_path)
        summary = load_strict_json_text(raw.decode("utf-8"))
        expected = {
            "git_commit": self.request["candidate_sha"], "status": status,
            "execution_request_id": self.request["request_id"],
            "validation_identity_sha256": self.request["validation_identity_sha256"],
        }
        if not isinstance(summary, dict) or any(summary.get(k) != v for k, v in expected.items()):
            raise ExecutionContainmentError("FULL_RESULT_SUMMARY_BINDING")
        result = {
            "schema_version": "full_execution_result.v1",
            "candidate_sha": self.request["candidate_sha"],
            "validation_identity_sha256": self.request["validation_identity_sha256"],
            "request_id": self.request["request_id"], "status": status,
            "summary": {"path": summary_path.absolute().as_posix(),
                        "sha256": hashlib.sha256(raw).hexdigest()},
        }
        result_path = Path(self.request["result_path"])
        result_raw = canonical_json_bytes(result)
        if result_path.exists():
            if bounded_regular_bytes(result_path) != result_raw:
                raise ExecutionContainmentError("FULL_RESULT_REPLAY_CHANGED")
        self.fence.guard.store.execution_lifecycle().commit_full_result(
            self.request["lease_id"], actor="integration-coordinator", record=result,
        )
        if not result_path.exists():
            write_bound_once(result_path.parent, result_path.name, result_raw)
        self.fence.guard.store.execution_lifecycle().record_result(
            self.request["lease_id"], actor="integration-coordinator", result_path=result_path,
            expected_sha256=hashlib.sha256(result_raw).hexdigest(),
        )


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    env_overrides: Mapping[str, str] | None = None,
    env_removals: Sequence[str] = (),
) -> dict[str, object]:
    started = time.perf_counter()
    output_parts: list[str] = []
    process_env = None
    if env_overrides or env_removals:
        process_env = dict(os.environ)
        for variable_name in env_removals:
            process_env.pop(str(variable_name), None)
    if env_overrides:
        assert process_env is not None
        process_env.update(env_overrides)
    with subprocess.Popen(
        command,
        cwd=cwd,
        env=process_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    ) as process:
        if process.stdout is not None:
            for line in process.stdout:
                print(line, end="", flush=True)
                output_parts.append(line)
        return_code = process.wait()
    elapsed = round(time.perf_counter() - started, 2)
    return {
        "command": list(command),
        "exit_code": return_code,
        "elapsed_seconds": elapsed,
        "pytest_output": "".join(output_parts),
    }


def _recheck_launcher_identity(value: object) -> list[dict[str, object]]:
    """Read committed launcher evidence only beneath this inspector's fixed root."""
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes

    fixed_root = _repo_root().resolve()
    if not isinstance(value, list) or not value or len(value) > 10000:
        raise ValueError("Full launcher source inventory is invalid")
    verified: dict[str, dict[str, object]] = {}
    for row in value:
        if not isinstance(row, dict) or set(row) != {"path", "sha256", "size_bytes"}:
            raise ValueError("Full launcher source record is invalid")
        path = Path(str(row["path"]))
        if (not path.is_absolute() or ".." in path.parts or path.suffix != ".py"
                or not any(path.is_relative_to(fixed_root / directory)
                           for directory in ("src", "scripts"))
                or path.as_posix() in verified):
            raise ValueError("Full launcher source is outside the fixed implementation")
        raw = bounded_regular_bytes(path)
        if (type(row["size_bytes"]) is not int or len(raw) != row["size_bytes"]
                or hashlib.sha256(raw).hexdigest() != row["sha256"]):
            raise ValueError("Full launcher source changed")
        verified[path.as_posix()] = dict(row)
    if (fixed_root / "scripts" / "run_validation_tier.py").as_posix() not in verified:
        raise ValueError("Full launcher runner source is missing")
    return list(verified.values())


def _allocate_runtime_profile(
    runner: _FullCommandRunner | None,
) -> tuple[tempfile.TemporaryDirectory[str] | None, Path]:
    if runner is not None and runner.worker_token is not None:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            WindowsWorkerExchange,
        )

        exchange = runner.worker_exchange
        if type(exchange) is not WindowsWorkerExchange:
            raise ExecutionContainmentError("FULL_WORKER_EXCHANGE_REQUIRED")
        exchange.validate_worker(runner.worker_token)
        return None, exchange.profile_directory / RUNTIME_PROFILE_OUTPUT_NAME
    directory = tempfile.TemporaryDirectory(prefix="aits_pytest_runtime_profile_")
    return directory, Path(directory.name) / RUNTIME_PROFILE_OUTPUT_NAME


def _run_mandatory_acceptance_command(
    command: Sequence[str],
    *,
    cwd: Path,
    binding: dict[str, object],
    expected_collections: int,
    env_overrides: Mapping[str, str] | None = None,
    command_runner: Callable[..., dict[str, object]] | None = None,
) -> dict[str, object]:
    """Use the ordinary subprocess runner, with mandatory evidence checked before PASS."""
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
        create_bound_recoverable_file,
        hold_bound_read_file,
        write_bound_once,
    )
    from ai_trading_system.platform.architecture.workflow_coordination import WindowsWorkerExchange

    exchange = (
        command_runner.worker_exchange if isinstance(command_runner, _FullCommandRunner) else None
    )
    if exchange is not None and type(exchange) is not WindowsWorkerExchange:
        raise ExecutionContainmentError("FULL_WORKER_EXCHANGE_REQUIRED")
    if (isinstance(command_runner, _FullCommandRunner)
            and command_runner.worker_token is not None and exchange is None):
        raise ExecutionContainmentError("FULL_WORKER_EXCHANGE_REQUIRED")
    directory_context = (
        exchange.directory() if exchange is not None
        else tempfile.TemporaryDirectory(prefix="aits_mandatory_acceptance_")
    )
    with directory_context as directory:
        request, output = Path(directory) / "request.json", Path(directory) / "result.json"
        try:
            checkout_identity = bind_acceptance_checkout(cwd, binding)
            if Path(command[0]).absolute() != Path(sys.executable).absolute():
                raise ExecutionContainmentError("ACCEPTANCE_INTERPRETER_UNBOUND")
            effective_environment = (
                command_runner.effective_environment(env_overrides)
                if isinstance(command_runner, _FullCommandRunner)
                else {**os.environ, **(env_overrides or {})}
            )
            dependency_inputs: dict[Path, bytes] = {}
            runtime_identity = acceptance_runtime_identity(
                effective_environment, captured_dependencies=dependency_inputs
            )
            launcher_identity = None
            if exchange is not None:
                launcher_identity = bind_inspector_implementation(
                    _repo_root(), runtime_inputs=dependency_inputs,
                )
                runner_identity = capture_acceptance_implementation(
                    cwd, str(binding["candidate_sha"]),
                )
            else:
                runner_identity = bind_acceptance_implementation(
                    cwd, str(binding["candidate_sha"]), include_runner=True,
                    runtime_inputs=dependency_inputs,
                )
            implementation_identity = runner_identity
        except ExecutionContainmentError as exc:
            return {
                "command": list(command),
                "exit_code": 1,
                "elapsed_seconds": 0.0,
                "pytest_output": "",
                "mandatory_acceptance": {"status": "FAIL", "reason": str(exc)},
            }
        root_info = Path(directory).stat()
        root_identity = (root_info.st_dev, root_info.st_ino)
        result_identity: tuple[int, int] | None = None
        request_raw = b""

        def record_result_identity(identity: tuple[int, int]) -> None:
            nonlocal result_identity, request_raw
            result_identity = identity
            request_raw = json.dumps(
                {
                    "repo_root": str(cwd.resolve()),
                    "binding": binding,
                    "output": str(output),
                    "checkout_identity": checkout_identity,
                    "runtime_identity": runtime_identity,
                    "implementation_identity": implementation_identity,
                    "result_identity": result_identity,
                    "result_root_identity": root_identity,
                },
                sort_keys=True,
            ).encode()
            # Durable creation authority is written while the original result
            # descriptor is held, before delete-on-close reservation is committed.
            write_bound_once(
                Path(directory), request.name, request_raw, expected_root_identity=root_identity
            )

        def record_result(descriptor: int) -> None:
            info = os.fstat(descriptor)
            record_result_identity((info.st_dev, info.st_ino))

        if exchange is None:
            create_bound_recoverable_file(
                Path(directory), output.name, b"", record_created=record_result,
                expected_root_identity=root_identity,
            )
        else:
            with hold_bound_read_file(
                Path(directory), output.name, expected=b"",
                expected_identity=exchange.result_identity,
                expected_root_identity=root_identity, expected_parent_identities={},
                allow_parent_updates=True,
            ):
                record_result_identity(exchange.result_identity)
        assert result_identity is not None
        request_info = request.stat()
        request_descriptor = json.dumps(
            {
                "path": str(request),
                "identity": [request_info.st_dev, request_info.st_ino],
                "sha256": hashlib.sha256(request_raw).hexdigest(),
            }
        )
        guarded_command = [
            *command[:3],
            "-p",
            "ai_trading_system.platform.architecture.workflow_execution",
            *command[3:],
        ]
        result = (command_runner or _run_command)(
            guarded_command,
            cwd=cwd,
            env_overrides={
                **(env_overrides or {}),
                MANDATORY_ACCEPTANCE_REQUEST_ENV: request_descriptor,
            },
        )
        try:
            if bind_mandatory_acceptance(cwd, str(binding["candidate_sha"])) != binding:
                raise ExecutionContainmentError("ACCEPTANCE_BINDING_CHANGED")
            if bind_acceptance_checkout(cwd, binding) != checkout_identity:
                raise ExecutionContainmentError("ACCEPTANCE_CHECKOUT_CHANGED")
            terminal_dependencies: dict[Path, bytes] = {}
            terminal_environment = (
                command_runner.effective_environment(env_overrides)
                if isinstance(command_runner, _FullCommandRunner)
                else {**os.environ, **(env_overrides or {})}
            )
            if (
                acceptance_runtime_identity(
                    terminal_environment,
                    captured_dependencies=terminal_dependencies,
                )
                != runtime_identity
            ):
                raise ExecutionContainmentError("ACCEPTANCE_RUNTIME_CHANGED")
            if exchange is not None:
                terminal_launcher = bind_inspector_implementation(
                    _repo_root(), runtime_inputs=terminal_dependencies,
                )
                terminal_launcher_map = {row["path"]: row for row in terminal_launcher}
                if launcher_identity is None or any(
                    terminal_launcher_map.get(row["path"]) != row for row in launcher_identity
                ):
                    raise ExecutionContainmentError("ACCEPTANCE_LAUNCHER_CHANGED")
                terminal_implementation = capture_acceptance_implementation(
                    cwd, str(binding["candidate_sha"]),
                )
            else:
                terminal_implementation = bind_acceptance_implementation(
                    cwd, str(binding["candidate_sha"]), include_runner=True,
                    runtime_inputs=terminal_dependencies,
                )
            if terminal_implementation != runner_identity:
                raise ExecutionContainmentError("ACCEPTANCE_IMPLEMENTATION_CHANGED")
            result_raw = bounded_regular_bytes(output, expected_identity=result_identity)
            evidence = validate_mandatory_acceptance_result(
                result_raw,
                binding,
                exit_code=_command_exit_code(result),
                expected_collections=expected_collections,
            )
            if (
                evidence["checkout_identity"] != checkout_identity
                or evidence["runtime_identity"] != runtime_identity
                or evidence["implementation_identity"] != implementation_identity
                or len(evidence["worker_inputs"]) != expected_collections
                or any(
                    row
                    != {
                        "checkout_identity": checkout_identity,
                        "origin_valid": True,
                        "runtime_identity": runtime_identity,
                        "implementation_identity": implementation_identity,
                    }
                    for row in evidence["worker_inputs"]
                )
            ):
                raise ExecutionContainmentError("ACCEPTANCE_WORKER_INPUT_CHANGED")
            result["mandatory_acceptance"] = {
                "status": "PASS",
                "evidence": evidence,
                "runner_identity": runner_identity,
                "launcher_identity": launcher_identity,
                "result_custody": {
                    "request_sha256": hashlib.sha256(request_raw).hexdigest(),
                    "request_identity": [request_info.st_dev, request_info.st_ino],
                    "result_identity": list(result_identity),
                    "root_identity": list(root_identity),
                    "result_sha256": hashlib.sha256(result_raw).hexdigest(),
                    "result_size_bytes": len(result_raw),
                },
            }
        except (
            OSError,
            KeyError,
            TypeError,
            ExecutionContainmentError,
            WorkflowContractError,
        ) as exc:
            result["mandatory_acceptance"] = {"status": "FAIL", "reason": str(exc)}
            if result["exit_code"] == 0:
                result["exit_code"] = 1
        return result


class _SlowDuration(TypedDict):
    seconds: float
    phase: str
    nodeid: str


def _parse_pytest_slow_durations(pytest_output: str) -> list[_SlowDuration]:
    durations: list[_SlowDuration] = []
    for line in pytest_output.splitlines():
        match = PYTEST_SLOW_DURATION_RE.match(line)
        if match is None:
            continue
        durations.append(
            {
                "seconds": float(match.group("seconds")),
                "phase": match.group("phase"),
                "nodeid": match.group("nodeid"),
            }
        )
    return sorted(durations, key=lambda row: float(row["seconds"]), reverse=True)


def _tail_lines(text: str, line_count: int = 80) -> list[str]:
    if not text:
        return []
    return text.splitlines()[-line_count:]


def _pytest_output_summary(
    pytest_output: str,
    *,
    artifact_dir: Path | None = None,
    log_name: str = PYTEST_OUTPUT_LOG_NAME,
    repo_root: Path | None = None,
) -> dict[str, object]:
    slow_durations = _parse_pytest_slow_durations(pytest_output)
    total_slow_duration = round(
        sum(float(row["seconds"]) for row in slow_durations),
        2,
    )
    summary: dict[str, object] = {
        "pytest_output_captured": bool(pytest_output),
        "pytest_output_tail": _tail_lines(pytest_output),
        "pytest_slow_durations": slow_durations,
        "pytest_slow_duration_count": len(slow_durations),
        "pytest_slow_duration_total_seconds": total_slow_duration,
    }
    if slow_durations:
        summary["pytest_slowest_duration"] = slow_durations[0]
        summary["pytest_slowest_nodeid"] = slow_durations[0]["nodeid"]
    if artifact_dir is not None and pytest_output:
        summary["pytest_output_log_path"] = _artifact_locator(
            artifact_dir / log_name,
            repo_root=repo_root,
        )
    return summary


def _split_cli_values(values: Sequence[str]) -> list[str]:
    split_values: list[str] = []
    for raw_value in values:
        for value in raw_value.split(","):
            normalized = value.strip()
            if normalized:
                split_values.append(normalized)
    return _unique(split_values)


def _safe_variant_id(*parts: str) -> str:
    safe_parts = []
    for part in parts:
        safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", part.strip() or "default")
        safe_parts.append(safe)
    return "_".join(safe_parts)


class _BenchmarkVariant(TypedDict):
    workers: str
    dist: str
    command: list[str]
    variant_id: str


def _benchmark_variants(
    *,
    tier: str,
    python_executable: str,
    repo_root: Path,
    extra_pytest_args: Sequence[str],
    workers: str,
    dist: str,
    benchmark_workers: Sequence[str],
    benchmark_dists: Sequence[str],
) -> list[_BenchmarkVariant]:
    worker_values = _split_cli_values(benchmark_workers) or [workers]
    dist_values = _split_cli_values(benchmark_dists) or [dist]
    variants: list[_BenchmarkVariant] = []
    for worker_value in worker_values:
        for dist_value in dist_values:
            command = build_command(
                tier,
                python_executable=python_executable,
                repo_root=repo_root,
                extra_pytest_args=extra_pytest_args,
                workers=worker_value,
                dist=dist_value,
            )
            variants.append(
                {
                    "workers": worker_value,
                    "dist": dist_value,
                    "command": command,
                    "variant_id": _safe_variant_id(worker_value, dist_value),
                }
            )
    return variants


def _summarize_benchmark_runs(runs: Sequence[dict[str, object]]) -> dict[str, object]:
    completed_runs = [run for run in runs if run.get("elapsed_seconds") is not None]
    successful_runs = [run for run in completed_runs if run.get("status") == "PASS"]
    best_run = min(
        successful_runs,
        key=lambda run: _runtime_float(run.get("elapsed_seconds") or float("inf")),
        default=None,
    )
    slowest_run = max(
        completed_runs,
        key=lambda run: _runtime_float(run.get("elapsed_seconds") or 0),
        default=None,
    )
    return {
        "benchmark_variant_count": len(runs),
        "benchmark_pass_count": len(successful_runs),
        "benchmark_fail_count": len(completed_runs) - len(successful_runs),
        "benchmark_best_variant": best_run,
        "benchmark_slowest_variant": slowest_run,
    }


def _write_report(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _runtime_run_id(resolved_tier: str, started_at: datetime) -> str:
    timestamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    return f"{resolved_tier}_{timestamp}"


def _incomplete_recovery_report(
    replay: Any, execution: Mapping[str, Any],
) -> dict[str, object]:
    from ai_trading_system.platform.architecture.workflow_contract import canonical_digest

    return {
        "schema_version": "full_incomplete_recovery.v1",
        "transaction_sha256": replay.transaction["transaction_sha256"],
        "candidate_sha": replay.candidate_sha,
        "execution_request_id": execution["request"]["request_id"],
        "execution_sha256": canonical_digest(execution),
        "technical_status": "INSUFFICIENT", "original_exit": execution["exit"],
        "reason": "FULL_VALIDATION_COMMITMENT_MISSING",
        "dispatch_performed": False, "publication_allowed": False,
    }


def _validated_incomplete_parent_binding(
    parent_path: Path, *, repo_root: Path, task_id: str | None,
) -> tuple[dict[str, object] | None, list[str]]:
    """Read original terminal authority; never recover, dispatch or repair it."""
    from ai_trading_system.platform.architecture.workflow_contract import (
        bounded_regular_bytes,
        canonical_digest,
    )
    from ai_trading_system.platform.architecture.workflow_coordination import validate_execution
    from ai_trading_system.platform.artifacts import canonical_json_bytes

    try:
        fence = IntegrationPublicationFence(project_root=repo_root)
        path = parent_path.absolute()
        if (".." in parent_path.parts or path.name != "full_incomplete_recovery.json"
                or path.parent.parent != fence.runtime_root / "transactions"):
            raise ValueError("proof must be the original transaction's canonical sibling")
        transaction_path = path.parent / "transaction.json"
        receipt_path = path.parent / "closeout_receipt.json"
        claim_path = path.parent / "full_dispatch_claim.json"
        captures = {
            item: bounded_regular_bytes(item)
            for item in (path, transaction_path, receipt_path, claim_path)
        }
        proof = load_provenance_json(captures[path].decode("utf-8"))
        transaction = load_provenance_json(captures[transaction_path].decode("utf-8"))
        receipt = load_provenance_json(captures[receipt_path].decode("utf-8"))
        replay = fence.replay(transaction_path)
        if (replay.status != "PASS" or replay.phase != "FAILED" or not task_id
                or replay.transaction["task_id"] != task_id
                or replay.transaction["transaction_id"] != path.parent.name
                or canonical_digest(transaction) != canonical_digest(replay.transaction)
                or any(event["phase"] == "FORMAL_VALIDATION_RESULT" for event in replay.events)):
            raise ValueError("original transaction/task/terminal phase differs")
        claim = fence._full_claim(replay)
        if captures[claim_path] != canonical_json_bytes(claim):
            raise ValueError("original dispatch claim differs")
        lease = fence._terminal_replay_lease(replay)
        if (lease is None or lease.state != "RELEASED"
                or lease.actor != "integration-coordinator"
                or lease.actor != replay.transaction["actor"] or lease.execution is None):
            raise ValueError("original released execution is missing")
        lease_snapshot = canonical_digest(lease.to_dict())
        fence._require_publication_lease_intent(replay, lease)
        validate_execution(lease)
        execution = lease.execution
        if (execution["state"] != "RESULT_RECORDED"
                or execution.get("full_result_commitment") is not None):
            raise ValueError("parent must be an uncommitted terminal Full execution")
        request = execution["request"]
        expected_id = canonical_digest({
            "transaction": replay.transaction["transaction_sha256"],
            "full_run_id": claim["full_run_id"],
        })
        if (request["schema_version"] != "workflow_execution_request.v1"
                or request["candidate_sha"] != replay.candidate_sha
                or request["request_id"] != expected_id
                or request["job_name"] != "Local\\AITS-DEVX015-full-" + expected_id
                or execution["launcher"] != claim["launcher"]):
            raise ValueError("original request/candidate/Job/launcher binding differs")
        expected_proof = _incomplete_recovery_report(replay, execution)
        if canonical_digest(proof) != canonical_digest(expected_proof):
            raise ValueError("proof differs from the original terminal execution")
        proof_ref = {
            "path": path.relative_to(repo_root).as_posix(),
            "sha256": hashlib.sha256(captures[path]).hexdigest(),
            "size_bytes": len(captures[path]),
        }
        if (canonical_digest(receipt) != canonical_digest(fence._terminal_receipt(replay))
                or receipt["evidence"] != [proof_ref]
                or replay.events[-1]["payload"]["evidence"] != [proof_ref]):
            raise ValueError("proof/terminal event/receipt binding differs")
        final_lease = fence._terminal_replay_lease(replay)
        if (final_lease is None or canonical_digest(final_lease.to_dict()) != lease_snapshot
                or fence.replay(transaction_path) != replay
                or any(bounded_regular_bytes(item) != raw for item, raw in captures.items())):
            raise ValueError("parent authority changed during inspection")
        return {
            "run_id": claim["full_run_id"],
            "recovery_path": proof_ref["path"], "recovery_sha256": proof_ref["sha256"],
            "recovery_size_bytes": len(captures[path]),
            "receipt_sha256": hashlib.sha256(captures[receipt_path]).hexdigest(),
            "transaction_id": replay.transaction["transaction_id"],
            "transaction_sha256": replay.transaction["transaction_sha256"],
            "terminal_event_id": replay.events[-1]["event_id"], "lease_id": lease.lease_id,
            "execution_request_id": request["request_id"],
            "execution_sha256": canonical_digest(execution), "candidate_sha": replay.candidate_sha,
            "report_type": "full_incomplete_recovery", "resolved_tier": "full",
            "status": "INSUFFICIENT", "failure_basis": "FULL_VALIDATION_COMMITMENT_MISSING",
            "production_effect": "none",
        }, []
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        return None, [f"parent_run incomplete recovery is invalid: {exc}"]


def _publication_parent_path(
    provenance: Mapping[str, object], *, repo_root: Path,
) -> Path | None:
    parent = provenance.get("parent_run")
    if isinstance(parent, Mapping):
        if parent.get("report_type") == "full_incomplete_recovery":
            value = parent.get("recovery_path")
            if not isinstance(value, str) or not value:
                raise PublicationFenceError("PUBLICATION_FULL_PARENT_INVALID", "recovery_path")
            checked, errors = _validated_incomplete_parent_binding(
                repo_root / value, repo_root=repo_root, task_id=str(provenance.get("task_id", "")),
            )
            if errors or checked != parent:
                raise PublicationFenceError("PUBLICATION_FULL_PARENT_CHANGED", "; ".join(errors))
            return Path(value)
        parent = parent.get("summary_path")
    if parent is not None and (not isinstance(parent, str) or not parent.strip()):
        raise PublicationFenceError("PUBLICATION_FULL_PARENT_INVALID", "summary_path")
    return Path(parent) if isinstance(parent, str) else None


def _require_new_incomplete_parent_candidate(
    provenance: Mapping[str, object], binding: Mapping[str, object],
    *, fence: IntegrationPublicationFence, transaction: Path,
) -> None:
    parent = provenance.get("parent_run")
    if isinstance(parent, Mapping) and parent.get("report_type") == "full_incomplete_recovery":
        replay = fence.replay(transaction)
        expected_parent = {
            "path": parent["recovery_path"], "sha256": parent["recovery_sha256"],
            "size_bytes": parent["recovery_size_bytes"],
        }
        if (replay.status != "PASS" or replay.phase != binding["phase"]
                or replay.transaction["transaction_sha256"] != binding["transaction_sha256"]
                or replay.transaction.get("full_parent") != expected_parent):
            raise PublicationFenceError("PUBLICATION_FULL_PARENT_CHANGED", "frozen parent differs")
        if any(parent.get(key) == binding.get(key)
               for key in ("transaction_sha256", "candidate_sha", "lease_id")):
            raise PublicationFenceError("PUBLICATION_FULL_PARENT_REUSED", "new candidate required")


def _validated_parent_run_binding(
    raw_parent_run: str,
    *,
    raw_parent_run_import: str | None = None,
    repo_root: Path,
    task_id: str | None = None,
) -> tuple[dict[str, object] | None, list[str]]:
    candidate = Path(raw_parent_run)
    parent_path = candidate if candidate.is_absolute() else repo_root / candidate
    if candidate.name == "full_incomplete_recovery.json":
        if raw_parent_run_import is not None:
            return None, ["parent_run incomplete recovery cannot use portable import"]
        return _validated_incomplete_parent_binding(
            parent_path, repo_root=repo_root, task_id=task_id,
        )
    try:
        resolved_path = parent_path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        return None, [f"parent_run summary does not exist or could not be resolved: {exc}"]
    try:
        allowed_root = (repo_root / DEFAULT_ARTIFACT_ROOT).resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        return None, [f"validation artifact root could not be resolved: {exc}"]
    if not _is_relative_to(resolved_path, allowed_root):
        return None, ["parent_run summary must be under outputs/validation_runtime"]
    if resolved_path.name != "test_runtime_summary.json":
        return None, ["parent_run must reference test_runtime_summary.json"]
    try:
        raw_bytes = resolved_path.read_bytes()
        summary = load_provenance_json(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        return None, [f"parent_run summary is invalid: {exc}"]
    errors: list[str] = []
    if summary.get("schema_version") != 1:
        errors.append("parent_run schema_version must be 1")
    if summary.get("report_type") != "test_runtime_summary":
        errors.append("parent_run report_type must be test_runtime_summary")
    if summary.get("resolved_tier") != "full":
        errors.append("parent_run resolved_tier must be full")
    if summary.get("production_effect") != "none":
        errors.append("parent_run production_effect must be none")
    if summary.get("print_only") is not False:
        errors.append("parent_run must be an executed non-print-only Full")
    if summary.get("benchmark_mode") is not False:
        errors.append("parent_run must be a non-benchmark Full")

    parent_status = summary.get("status")
    exit_code = summary.get("exit_code")
    valid_exit_code = not isinstance(exit_code, bool) and isinstance(exit_code, int)
    if not valid_exit_code:
        errors.append("parent_run exit_code must be an integer pytest exit status")
    if not isinstance(parent_status, str) or parent_status not in ("PASS", "FAIL"):
        errors.append("parent_run status must be PASS or FAIL")
    elif valid_exit_code and ((parent_status == "PASS") is not (exit_code == 0)):
        errors.append("parent_run status and exit_code are inconsistent")

    parent_provenance = summary.get("validation_provenance")
    parent_provenance_errors = validate_full_provenance(parent_provenance)
    if parent_provenance_errors:
        errors.append(
            "parent_run validation provenance is invalid: " + "; ".join(parent_provenance_errors)
        )
    elif summary.get("validation_provenance_status") != "PASS":
        errors.append("parent_run validation_provenance_status must match PASS provenance")

    profile_path = resolved_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    resolved_profile_path: Path | None = None
    runtime_profile_sha256: str | None = None
    runtime_profile_size: int | None = None
    try:
        candidate_profile_path = profile_path.resolve(strict=True)
    except (OSError, RuntimeError, ValueError) as exc:
        errors.append(f"parent_run runtime profile does not exist or could not be resolved: {exc}")
        runtime_profile_bytes = None
    else:
        if (
            not _is_relative_to(candidate_profile_path, allowed_root)
            or candidate_profile_path.parent != resolved_path.parent
            or candidate_profile_path.name != RUNTIME_PROFILE_OUTPUT_NAME
        ):
            errors.append(
                "parent_run runtime profile must resolve to the fixed sibling "
                "in its parent run directory"
            )
            runtime_profile_bytes = None
        else:
            resolved_profile_path = candidate_profile_path
            try:
                runtime_profile_bytes = resolved_profile_path.read_bytes()
            except OSError as exc:
                errors.append(f"parent_run runtime profile could not be read: {exc}")
                runtime_profile_bytes = None
            else:
                runtime_profile_sha256 = hashlib.sha256(runtime_profile_bytes).hexdigest()
                runtime_profile_size = len(runtime_profile_bytes)

    parent_import: dict[str, object] | None = None
    if raw_parent_run_import is not None:
        if (
            resolved_profile_path is None
            or runtime_profile_bytes is None
            or runtime_profile_sha256 is None
            or runtime_profile_size is None
        ):
            errors.append(
                "parent_run_import requires a readable current fixed-sibling runtime profile"
            )
        else:
            import_candidate = Path(raw_parent_run_import)
            import_path = (
                import_candidate if import_candidate.is_absolute() else repo_root / import_candidate
            )
            parent_import, import_errors = validate_parent_run_import(
                import_path,
                parent_summary_path=resolved_path,
                parent_profile_path=resolved_profile_path,
                repo_root=repo_root,
                summary_bytes=raw_bytes,
                summary=summary,
                profile_bytes=runtime_profile_bytes,
            )
            errors.extend(f"parent_run_import {error}" for error in import_errors)

    output_artifacts = summary.get("output_artifacts")
    matching_profile_records: list[Mapping[str, object]] = []
    if isinstance(output_artifacts, list):
        for record in output_artifacts:
            if not isinstance(record, Mapping) or not isinstance(record.get("path"), str):
                continue
            if parent_import is not None:
                if record["path"] == parent_import.get("source_profile_inventory_path"):
                    matching_profile_records.append(record)
                continue
            try:
                record_path = Path(str(record["path"]))
                record_resolved = (
                    record_path if record_path.is_absolute() else repo_root / record_path
                ).resolve()
            except (OSError, RuntimeError, ValueError, TypeError):
                errors.append("parent_run output_artifacts contains an invalid path")
                continue
            if resolved_profile_path is not None and record_resolved == resolved_profile_path:
                matching_profile_records.append(record)
    if len(matching_profile_records) != 1:
        errors.append("parent_run must inventory exactly one runtime profile sidecar")
    elif runtime_profile_sha256 is not None and runtime_profile_size is not None:
        profile_record = matching_profile_records[0]
        if (
            profile_record.get("exists") is not True
            or profile_record.get("sha256") != runtime_profile_sha256
            or profile_record.get("size_bytes") != runtime_profile_size
        ):
            errors.append("parent_run runtime profile inventory hash/size is stale")

    captured_profile_payload: object | None = None
    if runtime_profile_bytes is not None:
        try:
            captured_profile_payload = json.loads(
                runtime_profile_bytes.decode("utf-8"),
                object_pairs_hook=_reject_duplicate_json_keys,
                parse_constant=_reject_non_finite_json_constant,
            )
        except (UnicodeError, ValueError, json.JSONDecodeError):
            captured_profile_payload = None

    validated_profile: dict[str, object] | None = None
    if valid_exit_code and resolved_profile_path is not None and runtime_profile_bytes is not None:
        assert isinstance(exit_code, int)  # Established by valid_exit_code above.
        canonical_persisted_failure = bool(
            isinstance(parent_provenance, Mapping)
            and _is_canonical_persisted_runtime_profile_failure(
                captured_profile_payload,
                pytest_exitstatus=int(exit_code),
                expected_validation_provenance=parent_provenance,
            )
        )
        if canonical_persisted_failure:
            assert isinstance(captured_profile_payload, Mapping)
            validated_profile = dict(captured_profile_payload)
        else:
            validated_profile = _read_runtime_profile_payload(
                resolved_profile_path,
                pytest_exitstatus=int(exit_code),
                formal_selection_eligible=True,
                expected_validation_provenance=(
                    parent_provenance if isinstance(parent_provenance, Mapping) else None
                ),
                raw_bytes=runtime_profile_bytes,
            )
        profile_telemetry = validated_profile.get("telemetry")
        if not canonical_persisted_failure and (
            not isinstance(profile_telemetry, Mapping)
            or profile_telemetry.get("missing_runtime_profile_artifact") is True
        ):
            errors.append("parent_run runtime profile is missing or invalid")
        else:
            derived_profile_summary = _summarize_runtime_profile(
                validated_profile,
                final_path=resolved_profile_path,
            )
            recorded_profile_path = summary.get("runtime_profile_path")
            if not isinstance(recorded_profile_path, str):
                errors.append("parent_run runtime_profile_path is missing")
            elif parent_import is not None:
                if recorded_profile_path != parent_import.get("source_runtime_profile_path"):
                    errors.append(
                        "parent_run runtime_profile_path does not match its imported source locator"
                    )
            else:
                try:
                    recorded_candidate = Path(recorded_profile_path)
                    recorded_resolved = (
                        recorded_candidate
                        if recorded_candidate.is_absolute()
                        else repo_root / recorded_candidate
                    ).resolve()
                except (OSError, RuntimeError, ValueError, TypeError):
                    errors.append("parent_run runtime_profile_path is invalid")
                else:
                    if recorded_resolved != resolved_profile_path:
                        errors.append("parent_run runtime_profile_path does not match its sidecar")
            for field_name in (
                "runtime_profile_status",
                "formal_full_selection_eligible",
                "runtime_profile_summary",
            ):
                if summary.get(field_name) != derived_profile_summary[field_name]:
                    errors.append(f"parent_run {field_name} does not match its runtime profile")

    runtime_profile = (
        validated_profile.get("performance_evidence_status")
        if isinstance(validated_profile, Mapping)
        else None
    )
    failure_basis: str | None = None
    if valid_exit_code and parent_status == "FAIL" and exit_code != 0 and runtime_profile == "FAIL":
        failure_basis = "PYTEST_FAIL"
    elif (
        valid_exit_code and parent_status == "PASS" and exit_code == 0 and runtime_profile == "FAIL"
    ):
        failure_basis = "RUNTIME_PROFILE_FAIL"
    else:
        errors.append(
            "parent_run must contain a consistent failed pytest or runtime profile result"
        )

    run_id = resolved_path.parent.name
    if PROVENANCE_IDENTIFIER_RE.fullmatch(run_id) is None:
        errors.append("parent_run directory name is not a stable run id")
    if errors:
        return None, errors
    try:
        resolved_repo_root = repo_root.resolve()
        relative_path = resolved_path.relative_to(resolved_repo_root).as_posix()
    except (OSError, RuntimeError, ValueError) as exc:
        return None, [f"repository root could not be resolved for parent_run summary: {exc}"]
    binding: dict[str, object] = {
        "run_id": run_id,
        "summary_path": relative_path,
        "summary_sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "runtime_profile_sha256": str(runtime_profile_sha256),
        "report_type": "test_runtime_summary",
        "resolved_tier": "full",
        "status": parent_status,
        "failure_basis": str(failure_basis),
        "production_effect": "none",
    }
    if parent_import is not None:
        binding.update(
            {
                "locator_mode": "portable_import_v1",
                "import_manifest_path": parent_import["manifest_relative_path"],
                "import_manifest_sha256": parent_import["manifest_sha256"],
            }
        )
    return binding, []


def _validation_trigger_provenance(
    args: argparse.Namespace,
    *,
    resolved_tier: str,
    repo_root: Path,
) -> dict[str, object]:
    """Build the canonical S4 object shared by summary, profile, and Reader Brief."""

    cli_values = {
        "trigger_reason": args.trigger_reason,
        "task_id": args.task_id,
        "boundary_id": args.boundary_id,
        "parent_run": args.parent_run,
    }
    env_names = {
        "trigger_reason": VALIDATION_TRIGGER_REASON_ENV,
        "task_id": VALIDATION_TASK_ID_ENV,
        "boundary_id": VALIDATION_BOUNDARY_ID_ENV,
        "parent_run": VALIDATION_PARENT_RUN_ENV,
    }
    cli_parent_run_import = args.parent_run_import
    if any(value is not None for value in cli_values.values()) or cli_parent_run_import is not None:
        envelope_source = "cli"
        raw_values = cli_values
        raw_parent_run_import = cli_parent_run_import
    elif any(env_name in os.environ for env_name in env_names.values()) or (
        VALIDATION_PARENT_RUN_IMPORT_ENV in os.environ
    ):
        envelope_source = "environment"
        raw_values = {
            field_name: os.environ.get(env_name) for field_name, env_name in env_names.items()
        }
        raw_parent_run_import = os.environ.get(VALIDATION_PARENT_RUN_IMPORT_ENV)
    else:
        envelope_source = "unset"
        raw_values = {field_name: None for field_name in env_names}
        raw_parent_run_import = None
    values: dict[str, object] = {
        field_name: (str(raw_value).strip() or None) if raw_value is not None else None
        for field_name, raw_value in raw_values.items()
    }
    field_sources = {
        field_name: envelope_source if raw_values[field_name] is not None else "unset"
        for field_name in raw_values
    }
    parent_run_import = (
        str(raw_parent_run_import).strip() or None if raw_parent_run_import is not None else None
    )

    required_for_tier = resolved_tier == "full"
    any_declared = envelope_source != "unset"
    errors: list[str] = []
    trigger_reason = values["trigger_reason"]
    if trigger_reason is not None and trigger_reason not in VALIDATION_TRIGGER_REASONS:
        errors.append(f"unsupported trigger_reason={trigger_reason}")
    if any_declared:
        if trigger_reason is None:
            errors.append("declared provenance requires non-empty trigger_reason")
        for field_name in ("task_id", "boundary_id"):
            if values[field_name] is None:
                errors.append(f"declared provenance requires non-empty {field_name}")
    for field_name in ("task_id", "boundary_id"):
        value = values[field_name]
        if value is not None and PROVENANCE_IDENTIFIER_RE.fullmatch(str(value)) is None:
            errors.append(f"{field_name} must be a 1-256 character stable audit identifier")
    if trigger_reason == "failure_fix_rerun":
        raw_parent_run = values["parent_run"]
        if not isinstance(raw_parent_run, str):
            errors.append("failure_fix_rerun requires non-empty parent_run")
        else:
            parent_binding, parent_errors = _validated_parent_run_binding(
                raw_parent_run,
                raw_parent_run_import=parent_run_import,
                repo_root=repo_root,
                task_id=str(values["task_id"]) if values["task_id"] is not None else None,
            )
            errors.extend(parent_errors)
            values["parent_run"] = parent_binding
    elif values["parent_run"] is not None:
        errors.append("parent_run is only allowed for failure_fix_rerun")
    elif parent_run_import is not None:
        errors.append("parent_run_import is only allowed for failure_fix_rerun")

    if required_for_tier:
        if trigger_reason not in FULL_VALIDATION_TRIGGER_REASONS:
            errors.append(
                "full requires trigger_reason in " + ",".join(FULL_VALIDATION_TRIGGER_REASONS)
            )
        for field_name in ("task_id", "boundary_id"):
            if values[field_name] is None:
                errors.append(f"full requires non-empty {field_name}")

    if errors:
        status = "FAIL"
    elif required_for_tier or any_declared:
        status = "PASS"
    else:
        status = "NOT_REQUIRED"
    payload: dict[str, object] = {
        "schema_version": VALIDATION_PROVENANCE_SCHEMA_VERSION,
        "status": status,
        "required_for_tier": required_for_tier,
        **values,
        "envelope_source": envelope_source,
        "field_sources": field_sources,
        "cli_over_environment_precedence": "whole_envelope",
        "validation_errors": errors,
    }
    if required_for_tier and not errors:
        contract_errors = validate_full_provenance(payload)
        if contract_errors:
            payload["status"] = "FAIL"
            payload["validation_errors"] = contract_errors
    return payload


def _attach_validation_provenance(
    payload: Mapping[str, object],
    provenance: Mapping[str, object],
) -> dict[str, object]:
    bound = dict(payload)
    bound["validation_provenance"] = dict(provenance)
    return bound


def _formal_full_selection_eligible(
    extra_pytest_args: Sequence[str],
    *,
    pytest_addopts: str,
) -> bool:
    """Allow audited reporting instrumentation without treating Full as filtered."""

    if pytest_addopts.strip():
        return False
    index = 0
    while index < len(extra_pytest_args):
        argument = str(extra_pytest_args[index]).strip()
        if any(
            argument.startswith(f"{option}=") for option in FULL_NON_SELECTION_PYTEST_ARG_OPTIONS
        ):
            index += 1
            continue
        if argument in FULL_NON_SELECTION_PYTEST_ARG_OPTIONS:
            if index + 1 >= len(extra_pytest_args):
                return False
            value = str(extra_pytest_args[index + 1]).strip()
            if not value or value.startswith("-"):
                return False
            index += 2
            continue
        return False
    return True


def _artifact_dir(repo_root: Path, args: argparse.Namespace, run_id: str) -> Path | None:
    if not args.write_runtime_artifact:
        return None
    if args.artifact_dir:
        return Path(args.artifact_dir)
    return repo_root / DEFAULT_ARTIFACT_ROOT / run_id


def _reserved_runtime_artifact_paths(
    artifact_dir: Path,
    *,
    resolved_tier: str,
    benchmark_variants: Sequence[Mapping[str, object]] = (),
) -> set[Path]:
    reserved = {
        artifact_dir / "test_runtime_summary.json",
        artifact_dir / "test_runtime_reader_brief.md",
        artifact_dir / PYTEST_OUTPUT_LOG_NAME,
    }
    if resolved_tier == "full":
        reserved.add(artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME)
        reserved.update(artifact_dir / name for name in (
            "execution_request.json", "execution_validation_identity.json",
            "execution.stdout.log", "execution_result.json",
        ))
    if benchmark_variants:
        reserved.add(artifact_dir / BENCHMARK_SUMMARY_NAME)
        reserved.update(
            artifact_dir / f"pytest_output_{variant['variant_id']}.log"
            for variant in benchmark_variants
        )
    return {path.resolve() for path in reserved}


def _runtime_payload(
    *,
    repo_root: Path,
    requested_tier: str,
    resolved_tier: str,
    spec: TierSpec,
    command: Sequence[str],
    workers: str,
    dist: str,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    result: dict[str, object] | None = None,
    extra_pytest_args: Sequence[str] = (),
    artifact_dir: Path | None = None,
    validation_provenance: Mapping[str, object],
) -> dict[str, object]:
    elapsed = round((ended_at - started_at).total_seconds(), 2)
    input_artifacts = _command_input_artifacts(command, repo_root=repo_root)
    pytest_output = str(result.get("pytest_output") or "") if result else ""
    payload: dict[str, object] = {
        "schema_version": 1,
        "report_type": "test_runtime_summary",
        "git_commit": _git_commit(repo_root) or "unknown",
        "tier": requested_tier,
        "requested_tier": requested_tier,
        "resolved_tier": resolved_tier,
        "suite_family": spec.suite_family,
        "description": spec.description,
        "status": status,
        "promotion_blocking": spec.promotion_blocking,
        "slow_suite_allowed": spec.slow_suite_allowed,
        "can_support_promotion_evidence": status == "PASS" and spec.promotion_blocking,
        "print_only": status == "PRINT_ONLY",
        "benchmark_mode": False,
        "workers": workers,
        "dist": dist,
        "extra_pytest_args": list(extra_pytest_args),
        "command": list(command),
        "test_selection_policy": _test_selection_policy(spec, repo_root),
        "resolved_config": {"validation_tier": resolved_tier},
        "input_artifacts": input_artifacts,
        "input_checksums": _artifact_checksums(input_artifacts),
        "schema_versions": {
            "test_runtime_summary": "1",
            "validation_trigger_provenance": VALIDATION_PROVENANCE_SCHEMA_VERSION,
            **(
                {"test_runtime_profile": RUNTIME_PROFILE_SCHEMA_VERSION}
                if resolved_tier == "full"
                else {}
            ),
        },
        "as_of": started_at.date().isoformat(),
        "random_seed": "not_applicable",
        "environment_summary": _environment_summary(),
        # Final output records are populated only after the auxiliary files have
        # been materialized.  Sampling them here would record false negatives.
        "output_artifacts": [],
        "warnings": [] if status == "PASS" else [f"validation_status={status}"],
        "started_at_utc": started_at.isoformat().replace("+00:00", "Z"),
        "ended_at_utc": ended_at.isoformat().replace("+00:00", "Z"),
        "elapsed_seconds": elapsed,
        "exit_code": None,
        "validation_provenance_status": validation_provenance.get("status", "FAIL"),
        "validation_provenance": dict(validation_provenance),
        "safety_boundary": dict(SAFETY_BOUNDARY),
        **SAFETY_BOUNDARY,
    }
    if result is not None:
        payload.update({key: value for key, value in result.items() if key != "pytest_output"})
    if pytest_output:
        payload.update(
            _pytest_output_summary(
                pytest_output,
                artifact_dir=artifact_dir,
                repo_root=repo_root,
            )
        )
    if validation_provenance.get("status") == "FAIL":
        warnings = payload["warnings"]
        assert isinstance(warnings, list)
        warnings.append("validation_trigger_provenance=FAIL")
    runtime_profile_summary = payload.get("runtime_profile_summary")
    if (
        isinstance(runtime_profile_summary, dict)
        and runtime_profile_summary.get("performance_evidence_status") != "PASS"
    ):
        warnings = payload["warnings"]
        assert isinstance(warnings, list)
        warnings.append(
            "runtime_profile_performance_evidence="
            f"{runtime_profile_summary.get('performance_evidence_status', 'FAIL')}"
        )
        if resolved_tier == "full":
            payload["can_support_promotion_evidence"] = False
            payload["promotion_evidence_limitation"] = (
                "Full pytest exit remains authoritative, but formal Full promotion evidence "
                "also requires a PASS runtime profile and trigger provenance."
            )
    if status == "PRINT_ONLY":
        payload["promotion_evidence_limitation"] = (
            "PRINT_ONLY renders the command and artifact contract but does not execute pytest."
        )
    elif status != "PASS":
        payload["promotion_evidence_limitation"] = (
            "Only PASS can be used as passing validation evidence for this suite."
        )
    if artifact_dir is not None:
        payload["artifact_dir"] = _artifact_locator(artifact_dir, repo_root=repo_root)
        payload["summary_path"] = _artifact_locator(
            artifact_dir / "test_runtime_summary.json",
            repo_root=repo_root,
        )
        payload["reader_brief_path"] = _artifact_locator(
            artifact_dir / "test_runtime_reader_brief.md",
            repo_root=repo_root,
        )
    if requested_tier != resolved_tier:
        payload["legacy_alias_for"] = resolved_tier
    return payload


def _command_input_artifacts(command: Sequence[str], *, repo_root: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for token in command:
        if not token or token.startswith("-"):
            continue
        path = Path(token)
        candidate = path if path.is_absolute() else repo_root / path
        if not _is_relative_to(candidate, repo_root) or not candidate.exists():
            continue
        records.append(_artifact_record(candidate))
    return records


def _runtime_output_artifacts(
    artifact_dir: Path | None,
    *,
    repo_root: Path | None = None,
    include_runtime_profile: bool,
    include_benchmark_summary: bool = False,
) -> list[dict[str, object]]:
    if artifact_dir is None:
        return []
    records = [
        _runtime_summary_self_record(
            artifact_dir / "test_runtime_summary.json",
            repo_root=repo_root,
        ),
        _artifact_record(
            artifact_dir / "test_runtime_reader_brief.md",
            repo_root=repo_root,
        ),
        _artifact_record(artifact_dir / PYTEST_OUTPUT_LOG_NAME, repo_root=repo_root),
    ]
    if include_runtime_profile:
        records.append(
            _artifact_record(
                artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME,
                repo_root=repo_root,
            )
        )
    if include_benchmark_summary:
        records.append(
            _artifact_record(
                artifact_dir / BENCHMARK_SUMMARY_NAME,
                repo_root=repo_root,
            )
        )
    return records


def _runtime_summary_self_record(
    path: Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, object]:
    """Describe the final summary without embedding a self-invalidating digest."""
    return {
        "path": _artifact_locator(path, repo_root=repo_root),
        "exists": True,
        "artifact_type": "json",
        "sha256": None,
        "size_bytes": None,
        "file_count": None,
        "integrity_status": "SELF_REFERENCE_NOT_EMBEDDED",
        "measurement_reason": (
            "the final summary cannot embed a digest or byte size of its own final serialized bytes"
        ),
    }


def _runtime_profile_failure_payload(
    *,
    reason: str,
    pytest_exitstatus: int,
) -> dict[str, object]:
    return {
        "schema_version": RUNTIME_PROFILE_SCHEMA_VERSION,
        "report_type": "test_runtime_profile",
        "profile_status": "FAIL",
        "telemetry_status": "FAIL",
        "performance_evidence_status": "FAIL",
        "validation_provenance_binding_status": "FAIL",
        "validation_provenance": None,
        "stable_full_improvement_claimed": False,
        "pytest_exitstatus": pytest_exitstatus,
        "pytest_outcome_authoritative": True,
        "pytest_outcome_overridden": False,
        "scheduler": {
            "applied": False,
            "fallback": True,
            "fallback_reason": reason,
            "application_unverified": True,
        },
        "collection": {
            "complete": False,
            "count": 0,
            "set_sha256": None,
            "ordered_sha256": None,
        },
        "telemetry": {
            "complete": False,
            "missing_runtime_profile_artifact": True,
        },
        "node_count": 0,
        "file_count": 0,
        "worker_count": 0,
        "tail_idle_total_seconds": 0.0,
        "tail_idle_max_seconds": 0.0,
        "warnings": [reason],
        "strategy_logic_changed": False,
        "production_effect": "none",
        "cached_data_mutated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }


def _is_canonical_persisted_runtime_profile_failure(
    payload: object,
    *,
    pytest_exitstatus: int,
    expected_validation_provenance: Mapping[str, object],
) -> bool:
    """Recognize only the exact fail-closed sidecar shape persisted by this runner."""

    if not isinstance(payload, Mapping):
        return False
    warnings = payload.get("warnings")
    if (
        not isinstance(warnings, list)
        or len(warnings) != 1
        or not isinstance(warnings[0], str)
        or not warnings[0]
    ):
        return False
    expected = _attach_validation_provenance(
        _runtime_profile_failure_payload(
            reason=warnings[0],
            pytest_exitstatus=pytest_exitstatus,
        ),
        expected_validation_provenance,
    )
    return dict(payload) == expected


def _is_non_bool_int(value: object, *, minimum: int = 0) -> TypeGuard[int]:
    return not isinstance(value, bool) and isinstance(value, int) and value >= minimum


def _is_nonnegative_finite_number(value: object) -> TypeGuard[int | float]:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0.0
    )


def _numbers_close(left: object, right: float, *, tolerance: float = 1e-6) -> bool:
    return _is_nonnegative_finite_number(left) and math.isclose(
        float(left),
        right,
        rel_tol=0.0,
        abs_tol=tolerance,
    )


def _epoch_utc_iso(value: float) -> str:
    return datetime.fromtimestamp(value, tz=UTC).isoformat().replace("+00:00", "Z")


def _parse_utc_iso_epoch(value: object) -> float | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        return None
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
        offset = parsed.utcoffset()
        if offset is None or offset.total_seconds() != 0.0:
            return None
        epoch_seconds = parsed.timestamp()
    except (OSError, OverflowError, ValueError):
        return None
    return epoch_seconds if math.isfinite(epoch_seconds) else None


def _nodeid_identity(nodeids: Sequence[str]) -> dict[str, object]:
    counts = Counter(nodeids)
    return {
        "count": len(nodeids),
        "ordered_sha256": hashlib.sha256("\n".join(nodeids).encode("utf-8")).hexdigest(),
        "set_sha256": hashlib.sha256("\n".join(sorted(nodeids)).encode("utf-8")).hexdigest(),
        "duplicate_nodeids": sorted(nodeid for nodeid, count in counts.items() if count > 1),
    }


def _string_set_sha256(values: Sequence[str]) -> str:
    return hashlib.sha256("\n".join(sorted(values)).encode("utf-8")).hexdigest()


def _duration_file_rows_sha256(
    observed_seconds: Mapping[str, float],
    file_node_counts: Mapping[str, int],
) -> str:
    rows = [
        {
            "node_count": file_node_counts[path],
            "observed_seconds": observed_seconds[path],
            "path": path,
        }
        for path in sorted(observed_seconds)
    ]
    return hashlib.sha256(
        json.dumps(
            rows,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _load_expected_full_test_files(path: Path) -> tuple[set[str] | None, str | None]:
    try:
        content = path.read_bytes()
    except OSError as exc:
        return None, f"full test manifest could not be read: {exc}"
    return _parse_expected_full_test_files(content)


def _parse_expected_full_test_files(content: bytes) -> tuple[set[str] | None, str | None]:
    """Use the same full-file coverage contract for live and historical Git bytes."""
    try:
        payload = safe_load_yaml_text(content.decode("utf-8"))
    except (UnicodeError, ValueError, TypeError) as exc:
        return None, f"full test manifest could not be read: {exc}"
    rows = payload.get("tests") if isinstance(payload, Mapping) else None
    if not isinstance(rows, list):
        return None, "full test manifest tests must be a list"
    test_files = {
        str(row["path"]).replace("\\", "/")
        for row in rows
        if isinstance(row, Mapping)
        and row.get("file_role") == "test"
        and isinstance(row.get("path"), str)
        and row.get("path")
    }
    expected_count = payload.get("test_count")
    if (
        payload.get("status") != "PASS"
        or not _is_non_bool_int(expected_count)
        or len(test_files) != int(expected_count)
    ):
        return None, "full test manifest status/count contract is invalid"
    return test_files, None


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    payload: dict[str, object] = {}
    for key, value in pairs:
        if key in payload:
            raise ValueError(f"duplicate JSON key: {key}")
        payload[key] = value
    return payload


def _reject_non_finite_json_constant(value: str) -> object:
    raise ValueError(f"non-finite JSON constant: {value}")


def normalize_absolute_runtime_locator(value: object) -> str:
    """Compare a recorded absolute locator lexically, without reading its old host."""
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("runtime locator must be a non-empty absolute path")
    if value.startswith("\\") and not PureWindowsPath(value).drive:
        raise ValueError("Windows rooted runtime locator requires a drive")
    normalized = value.replace("\\", "/")
    if any(part in {".", ".."} for part in normalized.split("/")):
        raise ValueError("runtime locator must not contain dot segments")
    windows = PureWindowsPath(normalized)
    if windows.drive:
        if not windows.is_absolute():
            raise ValueError("drive-relative runtime locator is forbidden")
        return windows.as_posix().casefold()
    posix = PurePosixPath(normalized)
    if not posix.is_absolute():
        raise ValueError("runtime locator must be absolute")
    return posix.as_posix()


@dataclass(frozen=True)
class CapturedDurationManifest:
    """One immutable byte capture; the digest is never supplied by the caller."""

    content: bytes
    normalized_locator: str

    def __post_init__(self) -> None:
        if not isinstance(self.content, bytes) or not self.content:
            raise ValueError("captured duration manifest bytes are required")
        object.__setattr__(
            self,
            "normalized_locator",
            normalize_absolute_runtime_locator(self.normalized_locator),
        )


_SCHEDULING_MANIFEST_UNCHECKED = object()
_SPLIT_SCOPE_EVIDENCE_FIELDS = frozenset(
    {
        "schema_version",
        "manifest_path",
        "manifest_sha256",
        "policy_id",
        "version",
        "status",
        "heavy_concurrency_cap",
        "split_scope_files",
        "real_full_chain_marker",
        "real_full_chain_function_count",
    }
)


def _split_scope_structure_error(split_scope: object) -> str | None:
    """DEVX-018 evidence shape; exact manifest binding happens where bytes are trusted."""
    if split_scope is None:
        return None
    if not isinstance(split_scope, Mapping) or set(split_scope) != _SPLIT_SCOPE_EVIDENCE_FIELDS:
        return "runtime profile scheduler.split_scope fields are invalid"
    files = split_scope.get("split_scope_files")
    if (
        split_scope.get("schema_version") != SPLIT_SCOPE_EVIDENCE_SCHEMA_VERSION
        or split_scope.get("manifest_path") != SCHEDULING_MANIFEST_RELATIVE_PATH
        or not isinstance(split_scope.get("manifest_sha256"), str)
        or re.fullmatch(r"[0-9a-f]{64}", str(split_scope.get("manifest_sha256"))) is None
        or not _is_non_bool_int(split_scope.get("version"), minimum=1)
        or not _is_non_bool_int(split_scope.get("heavy_concurrency_cap"), minimum=1)
        or not _is_non_bool_int(split_scope.get("real_full_chain_function_count"), minimum=1)
        or not isinstance(files, list)
        or not files
        or any(not isinstance(path, str) or not path for path in files)
        or files != sorted(set(files))
    ):
        return "runtime profile scheduler.split_scope evidence is invalid"
    return None


def _runtime_profile_contract_error(
    payload: Mapping[str, object],
    *,
    pytest_exitstatus: int,
    expected_worker_count: int | None = None,
    expected_dist: str | None = None,
    formal_selection_eligible: bool | None = None,
    duration_profile_path: Path | None = None,
    expected_test_files: set[str] | None = None,
    expected_test_files_error: str | None = None,
    expected_validation_provenance: Mapping[str, object] | None = None,
    scheduling_manifest_root: Path | None = None,
) -> str | None:
    """Preserve live path semantics, then validate one capture with the pure core."""
    scheduling_manifest: object = _SCHEDULING_MANIFEST_UNCHECKED
    if scheduling_manifest_root is not None:
        try:
            scheduling_manifest = load_scheduling_manifest(scheduling_manifest_root)
        except SchedulingManifestError as exc:
            return f"runtime scheduling manifest could not be reloaded: {exc}"
    captured = None
    validation_payload = payload
    scheduler = payload.get("scheduler")
    if duration_profile_path is not None and isinstance(scheduler, Mapping):
        try:
            resolved_profile_path = duration_profile_path.resolve()
            configured_resolved = Path(str(scheduler.get("configured_manifest_path"))).resolve()
        except (OSError, ValueError, TypeError) as exc:
            return f"runtime profile configured manifest path is invalid: {exc}"
        if configured_resolved != resolved_profile_path:
            return "runtime profile configured manifest path differs from runner manifest"
        try:
            captured = CapturedDurationManifest(
                content=resolved_profile_path.read_bytes(),
                normalized_locator=str(resolved_profile_path),
            )
        except (OSError, ValueError, TypeError) as exc:
            return f"runtime duration manifest could not be reloaded: {exc}"
        # Normalize only the validation view, after checking the original live locator.
        # The caller still returns and persists its original payload and bytes.
        validation_payload = {
            **payload,
            "scheduler": {**scheduler, "configured_manifest_path": str(configured_resolved)},
        }
    return _runtime_profile_contract_error_from_inputs(
        validation_payload,
        pytest_exitstatus=pytest_exitstatus,
        expected_worker_count=expected_worker_count,
        expected_dist=expected_dist,
        formal_selection_eligible=formal_selection_eligible,
        captured_duration_manifest=captured,
        expected_test_files=expected_test_files,
        expected_test_files_error=expected_test_files_error,
        expected_validation_provenance=expected_validation_provenance,
        scheduling_manifest=scheduling_manifest,
    )


def _runtime_profile_contract_error_from_inputs(
    payload: Mapping[str, object],
    *,
    pytest_exitstatus: int,
    expected_worker_count: int | None = None,
    expected_dist: str | None = None,
    formal_selection_eligible: bool | None = None,
    captured_duration_manifest: CapturedDurationManifest | None = None,
    expected_test_files: set[str] | None = None,
    expected_test_files_error: str | None = None,
    expected_validation_provenance: Mapping[str, object] | None = None,
    scheduling_manifest: object = _SCHEDULING_MANIFEST_UNCHECKED,
) -> str | None:
    statuses = {
        key: payload.get(key)
        for key in ("profile_status", "telemetry_status", "performance_evidence_status")
    }
    if any(value not in {"PASS", "FAIL"} for value in statuses.values()):
        return f"runtime profile status fields are invalid: {statuses!r}"
    provenance_binding_status = payload.get("validation_provenance_binding_status")
    if provenance_binding_status not in {"PASS", "FAIL"}:
        return "runtime profile validation provenance binding status is invalid"
    observed_provenance = payload.get("validation_provenance")
    provenance_errors = validate_full_provenance(observed_provenance)
    if provenance_binding_status == "PASS" and provenance_errors:
        return "runtime profile validation provenance is invalid: " + "; ".join(provenance_errors)
    if expected_validation_provenance is not None:
        if provenance_binding_status != "PASS":
            return "runtime profile validation provenance binding did not pass"
        if not isinstance(observed_provenance, Mapping) or dict(observed_provenance) != dict(
            expected_validation_provenance
        ):
            return "runtime profile validation provenance does not match runner envelope"

    required_boolean_values = {
        "stable_full_improvement_claimed": False,
        "pytest_outcome_authoritative": True,
        "pytest_outcome_overridden": False,
        "strategy_logic_changed": False,
        "cached_data_mutated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
    }
    for key, expected in required_boolean_values.items():
        if payload.get(key) is not expected:
            return f"runtime profile {key} must be {expected!r}"
    if payload.get("production_effect") != "none":
        return "runtime profile production_effect must be 'none'"

    collection = payload.get("collection")
    scheduler = payload.get("scheduler")
    telemetry = payload.get("telemetry")
    if not isinstance(collection, Mapping):
        return "runtime profile collection must be a mapping"
    if not isinstance(scheduler, Mapping):
        return "runtime profile scheduler must be a mapping"
    if not isinstance(telemetry, Mapping):
        return "runtime profile telemetry must be a mapping"
    split_scope = scheduler.get("split_scope")
    split_scope_error = _split_scope_structure_error(split_scope)
    if split_scope_error is not None:
        return split_scope_error
    if scheduling_manifest is not _SCHEDULING_MANIFEST_UNCHECKED:
        assert scheduling_manifest is None or isinstance(scheduling_manifest, SchedulingManifest)
        binding_error = split_scope_evidence_error(split_scope, scheduling_manifest)
        # The split scheduler exists only for parallel loadfile runs.
        split_scope_inapplicable = scheduler.get("xdist_dist") != "loadfile" or not (
            _is_non_bool_int(scheduler.get("expected_worker_count"), minimum=2)
        )
        if binding_error is not None and not (split_scope is None and split_scope_inapplicable):
            return "runtime profile " + binding_error
    split_scope_files = (
        set(split_scope["split_scope_files"]) if isinstance(split_scope, Mapping) else set()
    )

    manifest_status = scheduler.get("manifest_status")
    if manifest_status not in {"PARTIAL_SEED", "COMPLETE"}:
        return "runtime profile scheduler.manifest_status is invalid"
    manifest_is_partial = manifest_status == "PARTIAL_SEED"
    manifest_is_complete = manifest_status == "COMPLETE"
    if scheduler.get("partial_seed") is not manifest_is_partial:
        return "runtime profile scheduler.partial_seed differs from manifest status"
    if scheduler.get("complete_profile") is not manifest_is_complete:
        return "runtime profile scheduler.complete_profile differs from manifest status"
    if scheduler.get("source_tier") != "full":
        return "runtime profile scheduler.source_tier must be full"
    source_workers = scheduler.get("source_workers")
    source_dist = scheduler.get("source_dist")
    if source_workers != 16:
        return "runtime profile scheduler.source_workers must equal 16"
    if source_dist != "loadfile":
        return "runtime profile scheduler.source_dist must be loadfile"
    if not isinstance(scheduler.get("complete_collection_verified"), bool):
        return "runtime profile scheduler.complete_collection_verified must be boolean"
    complete_hash_fields = (
        "source_collection_ordered_sha256",
        "source_collection_set_sha256",
        "source_file_set_sha256",
        "source_file_rows_sha256",
        "complete_expected_ordered_sha256",
    )
    if manifest_is_complete:
        if not _is_non_bool_int(scheduler.get("tracked_node_count"), minimum=1):
            return "runtime profile scheduler.tracked_node_count must be positive"
        for key in complete_hash_fields:
            value = scheduler.get(key)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                return f"runtime profile scheduler.{key} is invalid"
    elif (
        scheduler.get("tracked_node_count") is not None
        or scheduler.get("complete_collection_verified") is not False
        or any(scheduler.get(key) is not None for key in complete_hash_fields)
    ):
        return "runtime profile partial scheduler contains complete-only evidence"

    collection_complete = collection.get("complete")
    telemetry_complete = telemetry.get("complete")
    if not isinstance(collection_complete, bool):
        return "runtime profile collection.complete must be boolean"
    if not isinstance(telemetry_complete, bool):
        return "runtime profile telemetry.complete must be boolean"

    nodeids = collection.get("nodeids")
    if not isinstance(nodeids, list) or any(
        not isinstance(nodeid, str) or not nodeid for nodeid in nodeids
    ):
        return "runtime profile collection.nodeids must be non-empty strings"
    identity = _nodeid_identity(nodeids)
    for key in ("count", "ordered_sha256", "set_sha256", "duplicate_nodeids"):
        if collection.get(key) != identity[key]:
            return f"runtime profile collection identity mismatch for {key}"

    count_fields: dict[str, object] = {
        "node_count": payload.get("node_count"),
        "file_count": payload.get("file_count"),
        "worker_count": payload.get("worker_count"),
        "collection.expected_worker_count": collection.get("expected_worker_count"),
        "collection.observed_worker_count": collection.get("observed_worker_count"),
        "telemetry.phase_report_count": telemetry.get("phase_report_count"),
        "telemetry.invalid_phase_report_count": telemetry.get("invalid_phase_report_count"),
        "telemetry.reported_node_count": telemetry.get("reported_node_count"),
        "scheduler.expected_worker_count": scheduler.get("expected_worker_count"),
        "scheduler.tracked_file_count": scheduler.get("tracked_file_count"),
        "scheduler.matched_tracked_file_count": scheduler.get("matched_tracked_file_count"),
        "scheduler.matched_tracked_node_count": scheduler.get("matched_tracked_node_count"),
    }
    checked_counts: dict[str, int] = {}
    for key, value in count_fields.items():
        if not _is_non_bool_int(value):
            return f"runtime profile {key} must be a non-negative integer"
        checked_counts[key] = value

    node_count = checked_counts["node_count"]
    file_count = checked_counts["file_count"]
    worker_count = checked_counts["worker_count"]
    if node_count != len(nodeids):
        return "runtime profile node_count does not match collection"

    worker_identities = collection.get("worker_identities")
    if not isinstance(worker_identities, Mapping) or any(
        not isinstance(worker_id, str) or not worker_id for worker_id in worker_identities
    ):
        return "runtime profile collection.worker_identities must be a mapping"
    for worker_id, worker_identity in worker_identities.items():
        if not isinstance(worker_identity, Mapping):
            return f"runtime profile worker identity is invalid for {worker_id}"
        for key in ("count", "ordered_sha256", "set_sha256", "duplicate_nodeids"):
            if worker_identity.get(key) != identity[key]:
                return f"runtime profile collection identity mismatch for worker={worker_id}"

    observed_worker_count = int(collection["observed_worker_count"])
    collection_expected_workers = int(collection["expected_worker_count"])
    scheduler_expected_workers = int(scheduler["expected_worker_count"])
    if observed_worker_count != len(worker_identities):
        return "runtime profile observed worker count does not match worker identities"
    if scheduler_expected_workers != collection_expected_workers:
        return "runtime profile scheduler/collection worker contracts differ"

    telemetry_list_fields = (
        "missing_nodeids",
        "extra_nodeids",
        "duplicate_phase_nodeids",
        "missing_required_phase_nodeids",
        "inconsistent_worker_nodeids",
        "inactive_worker_ids",
        "unexpected_runtime_worker_ids",
    )
    for key in telemetry_list_fields:
        value = telemetry.get(key)
        if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
            return f"runtime profile telemetry.{key} must be a string list"

    nodes = payload.get("nodes")
    files = payload.get("files")
    workers = payload.get("workers")
    warnings = payload.get("warnings")
    outcome_counts = payload.get("outcome_counts")
    if not isinstance(nodes, list) or len(nodes) != node_count:
        return "runtime profile nodes do not match node_count"
    if not isinstance(files, list) or len(files) != file_count:
        return "runtime profile files do not match file_count"
    if not isinstance(workers, list) or len(workers) != worker_count:
        return "runtime profile workers do not match worker_count"
    if not isinstance(warnings, list) or any(not isinstance(item, str) for item in warnings):
        return "runtime profile warnings must be a string list"
    if not isinstance(outcome_counts, Mapping) or any(
        not isinstance(key, str) or not _is_non_bool_int(value)
        for key, value in outcome_counts.items()
    ):
        return "runtime profile outcome_counts must be a non-negative integer mapping"
    if sum(int(value) for value in outcome_counts.values()) != node_count:
        return "runtime profile outcome_counts do not sum to node_count"

    node_file_counts: Counter[str] = Counter()
    node_worker_counts: Counter[str] = Counter()
    derived_outcome_counts: Counter[str] = Counter()
    node_runtime_by_file: dict[str, list[tuple[str, float, float, float]]] = {}
    node_runtime_by_worker: dict[str, list[tuple[float, float, float]]] = {}
    phase_report_count = 0
    for expected_nodeid, row in zip(nodeids, nodes, strict=True):
        if not isinstance(row, Mapping) or row.get("nodeid") != expected_nodeid:
            return "runtime profile node rows do not preserve collection order"
        file_path = row.get("file")
        worker_id = row.get("worker_id")
        phases = row.get("phases")
        expected_file_path = expected_nodeid.split("::", 1)[0].replace("\\", "/")
        if file_path != expected_file_path:
            return f"runtime profile node file is invalid for {expected_nodeid}"
        if not isinstance(phases, list):
            return f"runtime profile phases are invalid for {expected_nodeid}"
        node_file_counts[file_path] += 1
        if isinstance(worker_id, str) and worker_id:
            node_worker_counts[worker_id] += 1
        phase_names: list[str] = []
        phase_worker_ids: set[str] = set()
        phase_starts: list[float] = []
        phase_stops: list[float] = []
        phase_durations: list[float] = []
        for phase in phases:
            if not isinstance(phase, Mapping):
                return f"runtime profile phase row is invalid for {expected_nodeid}"
            phase_name = phase.get("phase")
            phase_worker_id = phase.get("worker_id")
            if phase_name not in {"setup", "call", "teardown"}:
                return f"runtime profile phase name is invalid for {expected_nodeid}"
            if not isinstance(phase_worker_id, str) or not phase_worker_id:
                return f"runtime profile phase worker is invalid for {expected_nodeid}"
            if phase.get("outcome") not in {"passed", "failed", "skipped"}:
                return f"runtime profile phase outcome is invalid for {expected_nodeid}"
            for key in ("start_epoch_seconds", "stop_epoch_seconds", "duration_seconds"):
                if not _is_nonnegative_finite_number(phase.get(key)):
                    return f"runtime profile phase {key} is invalid for {expected_nodeid}"
            if float(phase["stop_epoch_seconds"]) < float(phase["start_epoch_seconds"]):
                return f"runtime profile phase chronology is invalid for {expected_nodeid}"
            if phase.get("start_utc") != _epoch_utc_iso(
                float(phase["start_epoch_seconds"])
            ) or phase.get("stop_utc") != _epoch_utc_iso(float(phase["stop_epoch_seconds"])):
                return (
                    f"runtime profile phase UTC timing is not epoch-derived for {expected_nodeid}"
                )
            phase_names.append(str(phase_name))
            phase_worker_ids.add(phase_worker_id)
            phase_starts.append(float(phase["start_epoch_seconds"]))
            phase_stops.append(float(phase["stop_epoch_seconds"]))
            phase_durations.append(float(phase["duration_seconds"]))
        if len(phase_names) != len(set(phase_names)):
            return f"runtime profile phase names are duplicated for {expected_nodeid}"
        if phases and (len(phase_worker_ids) != 1 or worker_id not in phase_worker_ids):
            return f"runtime profile node/phase worker differs for {expected_nodeid}"
        phase_outcomes = {
            str(phase.get("outcome") or "unknown") for phase in phases if isinstance(phase, Mapping)
        }
        if "failed" in phase_outcomes:
            derived_outcome = "failed"
        elif "skipped" in phase_outcomes:
            derived_outcome = "skipped"
        elif phase_outcomes == {"passed"}:
            derived_outcome = "passed"
        else:
            derived_outcome = "unknown"
        if row.get("outcome") != derived_outcome:
            return f"runtime profile node outcome is not phase-derived for {expected_nodeid}"
        derived_outcome_counts[derived_outcome] += 1
        if telemetry_complete:
            phase_by_name = {
                str(phase["phase"]): phase for phase in phases if isinstance(phase, Mapping)
            }
            if "setup" not in phase_by_name or "teardown" not in phase_by_name:
                return f"runtime profile required phases are missing for {expected_nodeid}"
            setup = phase_by_name["setup"]
            if setup.get("outcome") == "passed" and "call" not in phase_by_name:
                return f"runtime profile call phase is missing for {expected_nodeid}"
        if phases:
            node_start = min(phase_starts)
            node_stop = max(phase_stops)
            node_duration = round(sum(phase_durations), 9)
            if (
                not _numbers_close(row.get("start_epoch_seconds"), node_start)
                or not _numbers_close(row.get("stop_epoch_seconds"), node_stop)
                or not _numbers_close(row.get("duration_seconds"), node_duration)
                or row.get("start_utc") != _epoch_utc_iso(node_start)
                or row.get("stop_utc") != _epoch_utc_iso(node_stop)
            ):
                return f"runtime profile node timing is not phase-derived for {expected_nodeid}"
            if not isinstance(worker_id, str) or not worker_id:
                return f"runtime profile node worker is invalid for {expected_nodeid}"
            node_runtime_by_file.setdefault(file_path, []).append(
                (worker_id, node_start, node_stop, node_duration)
            )
            node_runtime_by_worker.setdefault(worker_id, []).append(
                (node_start, node_stop, node_duration)
            )
        phase_report_count += len(phases)

    all_runtime_rows = [
        runtime_row
        for runtime_rows in node_runtime_by_worker.values()
        for runtime_row in runtime_rows
    ]
    global_first_start = min((row[0] for row in all_runtime_rows), default=None)
    global_last_stop = max((row[1] for row in all_runtime_rows), default=None)

    file_row_counts: Counter[str] = Counter()
    for row in files:
        if not isinstance(row, Mapping):
            return "runtime profile file row must be a mapping"
        path = row.get("path")
        row_count = row.get("node_count")
        if not isinstance(path, str) or not path or not _is_non_bool_int(row_count, minimum=1):
            return "runtime profile file row has invalid path/node_count"
        file_row_counts[path] += int(row_count)
        if telemetry_complete:
            expected_rows = node_runtime_by_file.get(path, [])
            if not expected_rows:
                return f"runtime profile file aggregate is not node-derived for {path}"
            expected_workers = sorted({runtime_row[0] for runtime_row in expected_rows})
            expected_start = min(runtime_row[1] for runtime_row in expected_rows)
            expected_stop = max(runtime_row[2] for runtime_row in expected_rows)
            expected_duration = round(sum(runtime_row[3] for runtime_row in expected_rows), 9)
            if (
                int(row_count) != len(expected_rows)
                or row.get("worker_ids") != expected_workers
                or not _numbers_close(row.get("duration_seconds"), expected_duration)
                or row.get("start_utc") != _epoch_utc_iso(expected_start)
                or row.get("stop_utc") != _epoch_utc_iso(expected_stop)
                or not _numbers_close(
                    row.get("elapsed_envelope_seconds"),
                    round(expected_stop - expected_start, 9),
                )
            ):
                return f"runtime profile file aggregate is not node-derived for {path}"
            if (
                scheduler.get("xdist_dist") == "loadfile"
                and len(expected_workers) != 1
                and path not in split_scope_files
            ):
                return f"runtime profile loadfile assignment spans workers for {path}"

    worker_row_counts: Counter[str] = Counter()
    for row in workers:
        if not isinstance(row, Mapping):
            return "runtime profile worker row must be a mapping"
        worker_id = row.get("worker_id")
        row_count = row.get("node_count")
        if (
            not isinstance(worker_id, str)
            or not worker_id
            or not _is_non_bool_int(row_count, minimum=1)
        ):
            return "runtime profile worker row has invalid worker_id/node_count"
        worker_row_counts[worker_id] += int(row_count)
        if telemetry_complete:
            expected_worker_rows = node_runtime_by_worker.get(worker_id, [])
            if not expected_worker_rows:
                return f"runtime profile worker aggregate is not node-derived for {worker_id}"
            expected_start = min(runtime_row[0] for runtime_row in expected_worker_rows)
            expected_stop = max(runtime_row[1] for runtime_row in expected_worker_rows)
            expected_busy = round(sum(runtime_row[2] for runtime_row in expected_worker_rows), 9)
            expected_span = round(expected_stop - expected_start, 9)
            expected_internal_idle = round(max(0.0, expected_span - expected_busy), 9)
            assert global_last_stop is not None  # Nonempty worker rows contribute to this maximum.
            expected_tail_idle = round(
                max(0.0, float(global_last_stop) - expected_stop),
                9,
            )
            if (
                int(row_count) != len(expected_worker_rows)
                or row.get("first_start_utc") != _epoch_utc_iso(expected_start)
                or row.get("last_stop_utc") != _epoch_utc_iso(expected_stop)
                or not _numbers_close(row.get("busy_seconds"), expected_busy)
                or not _numbers_close(row.get("span_seconds"), expected_span)
                or not _numbers_close(row.get("internal_idle_seconds"), expected_internal_idle)
                or not _numbers_close(row.get("tail_idle_seconds"), expected_tail_idle)
            ):
                return f"runtime profile worker aggregate is not node-derived for {worker_id}"

    if expected_test_files_error is not None:
        return expected_test_files_error
    if expected_test_files is not None and set(node_file_counts) != expected_test_files:
        return "runtime profile collected file set does not match the full test manifest"

    recomputed_complete_collection_verified: bool | None = None
    if captured_duration_manifest is not None:
        try:
            configured_locator = normalize_absolute_runtime_locator(
                scheduler.get("configured_manifest_path")
            )
        except ValueError as exc:
            return f"runtime profile configured manifest path is invalid: {exc}"
        if configured_locator != captured_duration_manifest.normalized_locator:
            return "runtime profile configured manifest path differs from runner manifest"
        try:
            duration_manifest = safe_load_yaml_text(
                captured_duration_manifest.content.decode("utf-8")
            )
            manifest_sha256 = hashlib.sha256(captured_duration_manifest.content).hexdigest()
        except (UnicodeError, ValueError, TypeError) as exc:
            return f"runtime duration manifest could not be reloaded: {exc}"
        if not isinstance(duration_manifest, Mapping):
            return "runtime duration manifest root must be a mapping"
        duration_profile_id = duration_manifest.get("profile_id")
        duration_owner = duration_manifest.get("owner")
        duration_version = duration_manifest.get("version")
        duration_review = duration_manifest.get("review")
        if (
            duration_manifest.get("schema_version") != "arch_004g2_full_duration_profile.v1"
            or not isinstance(duration_profile_id, str)
            or not duration_profile_id.strip()
            or not isinstance(duration_owner, str)
            or not duration_owner.strip()
            or not _is_non_bool_int(duration_version, minimum=1)
            or not isinstance(duration_review, Mapping)
            or duration_review.get("stable_improvement_claimed") is not False
            or not isinstance(duration_review.get("conditions"), list)
            or not duration_review.get("conditions")
        ):
            return "runtime duration manifest common contract is invalid"
        source = duration_manifest.get("source")
        partial_seed = duration_manifest.get("partial_seed")
        complete_profile = duration_manifest.get("complete_profile")
        duration_rows = duration_manifest.get("files")
        duration_status = duration_manifest.get("status")
        if not isinstance(source, Mapping) or not isinstance(duration_rows, list):
            return "runtime duration manifest source/files contract is invalid"
        if duration_status not in {"PARTIAL_SEED", "COMPLETE"}:
            return "runtime duration manifest status is invalid"
        duration_is_partial = duration_status == "PARTIAL_SEED"
        duration_is_complete = duration_status == "COMPLETE"
        if duration_is_partial:
            if (
                not isinstance(partial_seed, Mapping)
                or partial_seed.get("enabled") is not True
                or complete_profile is not None
            ):
                return "runtime partial duration manifest contract is invalid"
        elif (
            partial_seed is not None
            or not isinstance(complete_profile, Mapping)
            or complete_profile.get("enabled") is not True
        ):
            return "runtime complete duration manifest contract is invalid"
        source_workers_value = source.get("workers")
        source_artifact_path = source.get("artifact_path")
        source_artifact_sha256 = source.get("artifact_sha256")
        if (
            source.get("tier") != "full"
            or source.get("dist") != "loadfile"
            or not _is_non_bool_int(source_workers_value, minimum=1)
            or int(source_workers_value) != 16
            or not isinstance(source_artifact_path, str)
            or not source_artifact_path.strip()
            or not isinstance(source_artifact_sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", source_artifact_sha256) is None
        ):
            return "runtime duration manifest source execution contract is invalid"
        duration_metadata = {
            "manifest_sha256": manifest_sha256,
            "manifest_schema_version": duration_manifest.get("schema_version"),
            "profile_id": duration_manifest.get("profile_id"),
            "owner": duration_manifest.get("owner"),
            "version": duration_manifest.get("version"),
            "manifest_status": duration_status,
            "partial_seed": duration_is_partial,
            "complete_profile": duration_is_complete,
            "source_tier": source.get("tier"),
            "source_workers": source_workers_value,
            "source_dist": source.get("dist"),
            "source_artifact_path": source.get("artifact_path"),
            "source_artifact_sha256": source.get("artifact_sha256"),
        }
        for key, expected in duration_metadata.items():
            if scheduler.get(key) != expected:
                return f"runtime profile scheduler manifest metadata mismatch for {key}"
        observed_seconds: dict[str, float] = {}
        duration_file_node_counts: dict[str, int] = {}
        for row in duration_rows:
            if not isinstance(row, Mapping):
                return "runtime duration manifest file row must be a mapping"
            path = row.get("path")
            seconds = row.get("observed_seconds")
            normalized_path = path.replace("\\", "/") if isinstance(path, str) else ""
            if (
                not isinstance(path, str)
                or not path
                or not normalized_path.startswith("tests/")
                or Path(normalized_path).is_absolute()
                or "." in Path(normalized_path).parts
                or ".." in Path(normalized_path).parts
                or not _is_nonnegative_finite_number(seconds)
                or float(seconds) <= 0.0
                or normalized_path in observed_seconds
            ):
                return "runtime duration manifest file row is invalid"
            observed_seconds[normalized_path] = float(seconds)
            if duration_is_complete:
                row_node_count = row.get("node_count")
                if not _is_non_bool_int(row_node_count, minimum=1):
                    return "runtime complete duration manifest node_count is invalid"
                duration_file_node_counts[normalized_path] = int(row_node_count)
        if scheduler.get("tracked_file_count") != len(observed_seconds):
            return "runtime profile tracked file count differs from duration manifest"
        grouped_nodeids: dict[str, list[str]] = {}
        for nodeid in nodeids:
            file_path = nodeid.split("::", 1)[0].replace("\\", "/")
            grouped_nodeids.setdefault(file_path, []).append(nodeid)
        expected_nodeids = [
            nodeid
            for _, grouped_rows in sorted(
                grouped_nodeids.items(),
                key=lambda row: -observed_seconds.get(row[0], 0.0),
            )
            for nodeid in grouped_rows
        ]
        matched_files = set(grouped_nodeids) & set(observed_seconds)
        matched_node_count = sum(len(grouped_nodeids[path]) for path in matched_files)
        recomputed_order_verified = bool(matched_files) and nodeids == expected_nodeids
        expected_order_identity = _nodeid_identity(expected_nodeids)
        if (
            scheduler.get("duration_order_verified") is not recomputed_order_verified
            or scheduler.get("matched_tracked_file_count") != len(matched_files)
            or scheduler.get("matched_tracked_node_count") != matched_node_count
            or scheduler.get("expected_ordered_sha256") != expected_order_identity["ordered_sha256"]
        ):
            return "runtime profile duration-order evidence is not reproducible"

        if duration_is_complete:
            assert isinstance(complete_profile, Mapping)  # Checked against manifest status above.
            if (
                source.get("profile_status") != "PASS"
                or source.get("telemetry_status") != "PASS"
                or source.get("performance_evidence_status") != "PASS"
                or isinstance(source.get("pytest_exitstatus"), bool)
                or source.get("pytest_exitstatus") != 0
            ):
                return "runtime complete duration source PASS contract is invalid"
            source_elapsed = source.get("elapsed_seconds")
            source_git_commit = source.get("git_commit")
            if (
                not _is_nonnegative_finite_number(source_elapsed)
                or float(source_elapsed) <= 0.0
                or not isinstance(source_git_commit, str)
                or re.fullmatch(r"[0-9a-f]{40}", source_git_commit) is None
            ):
                return "runtime complete duration source provenance is invalid"
            expected_source_node_count = complete_profile.get("source_node_count")
            expected_source_file_count = complete_profile.get("source_file_count")
            if (
                not _is_non_bool_int(expected_source_node_count, minimum=1)
                or not _is_non_bool_int(expected_source_file_count, minimum=1)
                or int(expected_source_file_count) != len(observed_seconds)
                or int(expected_source_node_count) != sum(duration_file_node_counts.values())
            ):
                return "runtime complete duration source counts are stale"
            complete_manifest_hashes = {
                "source_collection_ordered_sha256": complete_profile.get(
                    "source_collection_ordered_sha256"
                ),
                "source_collection_set_sha256": complete_profile.get(
                    "source_collection_set_sha256"
                ),
                "source_file_set_sha256": complete_profile.get("source_file_set_sha256"),
                "source_file_rows_sha256": complete_profile.get("source_file_rows_sha256"),
                "complete_expected_ordered_sha256": complete_profile.get(
                    "expected_scheduled_ordered_sha256"
                ),
            }
            for key, expected_hash in complete_manifest_hashes.items():
                if (
                    not isinstance(expected_hash, str)
                    or re.fullmatch(r"[0-9a-f]{64}", expected_hash) is None
                    or scheduler.get(key) != expected_hash
                ):
                    return f"runtime complete duration hash evidence mismatch for {key}"
            if scheduler.get("tracked_node_count") != expected_source_node_count:
                return "runtime complete tracked node count differs from manifest"
            expected_file_set_sha256 = _string_set_sha256(list(observed_seconds))
            if complete_profile.get(
                "source_file_set_sha256"
            ) != expected_file_set_sha256 or complete_profile.get(
                "source_file_rows_sha256"
            ) != _duration_file_rows_sha256(
                observed_seconds,
                duration_file_node_counts,
            ):
                return "runtime complete duration file hash evidence is stale"
            duration_total = complete_profile.get("source_file_duration_total_seconds")
            if not _numbers_close(duration_total, sum(observed_seconds.values())):
                return "runtime complete duration total is stale"
            if expected_test_files is not None and set(observed_seconds) != expected_test_files:
                return "runtime complete duration file set differs from full test manifest"
            current_identity = _nodeid_identity(nodeids)
            recomputed_complete_collection_verified = bool(
                not current_identity["duplicate_nodeids"]
                and len(nodeids) == int(expected_source_node_count)
                and current_identity["set_sha256"]
                == complete_profile.get("source_collection_set_sha256")
                and len(grouped_nodeids) == int(expected_source_file_count)
                and set(grouped_nodeids) == set(observed_seconds)
                and _string_set_sha256(list(grouped_nodeids))
                == complete_profile.get("source_file_set_sha256")
                and all(
                    len(grouped_nodeids[path]) == duration_file_node_counts.get(path)
                    for path in grouped_nodeids
                )
                and expected_order_identity["ordered_sha256"]
                == complete_profile.get("expected_scheduled_ordered_sha256")
            )
            if (
                scheduler.get("complete_collection_verified")
                is not recomputed_complete_collection_verified
            ):
                return "runtime complete collection coverage evidence is not reproducible"
        elif scheduler.get("complete_collection_verified") is not False:
            return "runtime partial duration profile claims complete collection coverage"

    for key in (
        "tail_idle_total_seconds",
        "tail_idle_max_seconds",
        "observed_test_window_seconds",
        "elapsed_seconds",
    ):
        if not _is_nonnegative_finite_number(payload.get(key)):
            return f"runtime profile {key} must be a finite non-negative number"

    session_start = _parse_utc_iso_epoch(payload.get("started_at_utc"))
    session_end = _parse_utc_iso_epoch(payload.get("ended_at_utc"))
    if session_start is None or session_end is None or session_end < session_start:
        return "runtime profile session UTC window is invalid"
    if not _numbers_close(payload.get("elapsed_seconds"), round(session_end - session_start, 9)):
        return "runtime profile elapsed_seconds is not session-window-derived"

    scheduler_boolean_fields = (
        "applied",
        "fallback",
        "duration_order_verified",
        "file_internal_node_order_preserved",
        "loadscope_reorder_disabled",
        "formal_full_selection_eligible",
    )
    for key in scheduler_boolean_fields:
        if not isinstance(scheduler.get(key), bool):
            return f"runtime profile scheduler.{key} must be boolean"
    scheduler_applied = bool(scheduler["applied"])
    scheduler_fallback = bool(scheduler["fallback"])
    fallback_reason = scheduler.get("fallback_reason")
    if scheduler_applied and (scheduler_fallback or fallback_reason is not None):
        return "runtime profile applied scheduler contains fallback evidence"
    if scheduler_fallback and (
        scheduler_applied or not isinstance(fallback_reason, str) or not fallback_reason.strip()
    ):
        return "runtime profile fallback scheduler evidence is invalid"
    if scheduler_applied:
        expected_applied_policy = (
            "complete_full_duration_descending_stable"
            if manifest_is_complete
            else "tracked_partial_seed_duration_descending_stable"
        )
        if (
            scheduler.get("policy") != expected_applied_policy
            or scheduler.get("equal_duration_tie_policy") != "stable_first_seen_file_order"
            or not _numbers_close(scheduler.get("untracked_file_weight_seconds"), 0.0)
            or scheduler.get("xdist_dist") != "loadfile"
            or scheduler.get("loadscope_reorder_disabled") is not True
            or scheduler.get("file_internal_node_order_preserved") is not True
        ):
            return (
                "runtime profile applied scheduler does not satisfy the formal scheduler contract"
            )
    complete_collection_verified = bool(scheduler["complete_collection_verified"])
    if manifest_is_complete:
        source_contract_matches = (
            int(source_workers) == int(scheduler["expected_worker_count"])
            and source_dist == scheduler.get("xdist_dist")
            and scheduler.get("xdist_dist") == "loadfile"
            and scheduler.get("loadscope_reorder_disabled") is True
        )
        complete_scheduler_eligible = complete_collection_verified and source_contract_matches
        if scheduler_applied and not complete_scheduler_eligible:
            return "runtime complete scheduler applied without exact coverage/worker contract"
        if not scheduler_applied and complete_scheduler_eligible:
            return "runtime complete scheduler fell back despite exact eligibility"
        if not scheduler_applied and scheduler.get("policy") != "stock_loadfile_test_count_order":
            return "runtime complete scheduler fallback policy is invalid"
        if not scheduler_applied and not scheduler_fallback:
            return "runtime complete scheduler mismatch must use explicit stock fallback"

    if telemetry_complete:
        if not collection_complete:
            return "runtime profile complete telemetry requires complete collection"
        if statuses["profile_status"] != "PASS" or statuses["telemetry_status"] != "PASS":
            return "runtime profile complete telemetry must have PASS profile/telemetry status"
        if (
            collection_expected_workers != observed_worker_count
            or worker_count != observed_worker_count
        ):
            return "runtime profile complete telemetry has inconsistent worker counts"
        if int(telemetry["invalid_phase_report_count"]) != 0 or any(
            telemetry[key] for key in telemetry_list_fields
        ):
            return "runtime profile complete telemetry contains unresolved telemetry issues"
        if int(telemetry["reported_node_count"]) != node_count:
            return "runtime profile complete telemetry reported_node_count mismatch"
        if int(telemetry["phase_report_count"]) != phase_report_count:
            return "runtime profile complete telemetry phase_report_count mismatch"
        if file_row_counts != node_file_counts or worker_row_counts != node_worker_counts:
            return "runtime profile file/worker aggregates do not match node rows"
        if set(worker_identities) != set(worker_row_counts):
            return "runtime profile collection/runtime worker identities differ"
        if dict(sorted(derived_outcome_counts.items())) != dict(outcome_counts):
            return "runtime profile outcome_counts are not node-derived"
        if pytest_exitstatus == 0 and derived_outcome_counts.get("failed", 0) != 0:
            return "runtime profile passing pytest exit contains failed node outcomes"
        if global_first_start is None or global_last_stop is None:
            return "runtime profile complete telemetry has no runtime window"
        if (
            global_first_start < session_start - 1e-6
            or global_last_stop > session_end + 1e-6
            or _runtime_float(payload["elapsed_seconds"])
            < _runtime_float(payload["observed_test_window_seconds"])
        ):
            return "runtime profile node window is outside the session window"
        expected_tail_values = [
            round(
                max(
                    0.0,
                    global_last_stop - max(runtime_row[1] for runtime_row in runtime_rows),
                ),
                9,
            )
            for runtime_rows in node_runtime_by_worker.values()
        ]
        if (
            not _numbers_close(
                payload.get("observed_test_window_seconds"),
                round(global_last_stop - global_first_start, 9),
            )
            or not _numbers_close(
                payload.get("tail_idle_total_seconds"),
                round(sum(expected_tail_values), 9),
            )
            or not _numbers_close(
                payload.get("tail_idle_max_seconds"),
                round(max(expected_tail_values, default=0.0), 9),
            )
        ):
            return "runtime profile top-level timing aggregates are not node-derived"
    elif statuses["profile_status"] != "FAIL" or statuses["telemetry_status"] != "FAIL":
        return "runtime profile incomplete telemetry must have FAIL profile/telemetry status"

    duration_verified = bool(scheduler["duration_order_verified"])
    selection_eligible = bool(scheduler["formal_full_selection_eligible"])
    performance_should_pass = (
        telemetry_complete
        and scheduler_applied
        and duration_verified
        and (not manifest_is_complete or complete_collection_verified)
        and selection_eligible
        and provenance_binding_status == "PASS"
        and pytest_exitstatus == 0
    )
    expected_performance_status = "PASS" if performance_should_pass else "FAIL"
    if statuses["performance_evidence_status"] != expected_performance_status:
        return "runtime profile performance status is inconsistent with its evidence"
    if performance_should_pass:
        expected_policy = (
            "complete_full_duration_descending_stable"
            if manifest_is_complete
            else "tracked_partial_seed_duration_descending_stable"
        )
        if (
            scheduler.get("policy") != expected_policy
            or scheduler.get("equal_duration_tie_policy") != "stable_first_seen_file_order"
            or not _numbers_close(scheduler.get("untracked_file_weight_seconds"), 0.0)
            or scheduler.get("fallback_reason") is not None
            or scheduler.get("xdist_dist") != "loadfile"
            or scheduler.get("loadscope_reorder_disabled") is not True
            or scheduler.get("file_internal_node_order_preserved") is not True
            or scheduler.get("fallback") is not False
            or bool(identity["duplicate_nodeids"])
            or int(scheduler["matched_tracked_file_count"]) < 1
            or int(scheduler["matched_tracked_node_count"]) < 1
            or scheduler.get("expected_ordered_sha256") != identity["ordered_sha256"]
            or (
                manifest_is_complete
                and (
                    int(scheduler["matched_tracked_file_count"])
                    != int(scheduler["tracked_file_count"])
                    or int(scheduler["matched_tracked_node_count"])
                    != int(scheduler["tracked_node_count"])
                    or scheduler.get("source_collection_set_sha256") != identity["set_sha256"]
                    or scheduler.get("complete_expected_ordered_sha256")
                    != identity["ordered_sha256"]
                )
            )
            or warnings
        ):
            return "runtime profile PASS evidence does not satisfy the formal scheduler contract"

    if expected_worker_count is not None and scheduler_expected_workers != expected_worker_count:
        return "runtime profile worker contract does not match runner invocation"
    if expected_dist is not None and scheduler.get("xdist_dist") != expected_dist:
        return "runtime profile distribution contract does not match runner invocation"
    if (
        formal_selection_eligible is not None
        and selection_eligible is not formal_selection_eligible
    ):
        return "runtime profile selection contract does not match runner invocation"
    return None


def _parse_runtime_profile_bytes(raw_bytes: bytes, *, pytest_exitstatus: int) -> dict[str, object]:
    try:
        payload = json.loads(
            raw_bytes.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except (UnicodeError, ValueError, TypeError) as exc:
        raise ValueError(f"runtime profile artifact missing or invalid: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("runtime profile artifact root must be a mapping")
    sidecar_exitstatus = payload.get("pytest_exitstatus")
    if (
        isinstance(sidecar_exitstatus, bool)
        or not isinstance(sidecar_exitstatus, int)
        or sidecar_exitstatus != pytest_exitstatus
    ):
        raise ValueError(
            "runtime profile pytest_exitstatus is invalid or mismatched: "
            f"sidecar={sidecar_exitstatus!r} subprocess={pytest_exitstatus!r}"
        )
    if (
        payload.get("schema_version") != RUNTIME_PROFILE_SCHEMA_VERSION
        or payload.get("report_type") != "test_runtime_profile"
    ):
        raise ValueError("runtime profile artifact schema/report_type is invalid")
    return payload


def _read_runtime_profile_payload(
    path: Path,
    *,
    pytest_exitstatus: int,
    expected_worker_count: int | None = None,
    expected_dist: str | None = None,
    formal_selection_eligible: bool | None = None,
    duration_profile_path: Path | None = None,
    expected_test_files: set[str] | None = None,
    expected_test_files_error: str | None = None,
    expected_validation_provenance: Mapping[str, object] | None = None,
    raw_bytes: bytes | None = None,
    scheduling_manifest_root: Path | None = None,
) -> dict[str, object]:
    try:
        captured_bytes = path.read_bytes() if raw_bytes is None else raw_bytes
    except OSError as exc:
        return _runtime_profile_failure_payload(
            reason=f"runtime profile artifact missing or invalid: {exc}",
            pytest_exitstatus=pytest_exitstatus,
        )
    try:
        payload = _parse_runtime_profile_bytes(captured_bytes, pytest_exitstatus=pytest_exitstatus)
    except ValueError as exc:
        return _runtime_profile_failure_payload(
            reason=str(exc), pytest_exitstatus=pytest_exitstatus
        )
    try:
        contract_error = _runtime_profile_contract_error(
            payload,
            pytest_exitstatus=pytest_exitstatus,
            expected_worker_count=expected_worker_count,
            expected_dist=expected_dist,
            formal_selection_eligible=formal_selection_eligible,
            duration_profile_path=duration_profile_path,
            expected_test_files=expected_test_files,
            expected_test_files_error=expected_test_files_error,
            expected_validation_provenance=expected_validation_provenance,
            scheduling_manifest_root=scheduling_manifest_root,
        )
    except Exception as exc:  # noqa: BLE001 - malformed evidence must not override pytest
        return _runtime_profile_failure_payload(
            reason=(
                f"runtime profile contract evaluation failed closed: {type(exc).__name__}: {exc}"
            ),
            pytest_exitstatus=pytest_exitstatus,
        )
    if contract_error is not None:
        return _runtime_profile_failure_payload(
            reason=f"runtime profile contract is invalid: {contract_error}",
            pytest_exitstatus=pytest_exitstatus,
        )
    return payload


def validate_captured_runtime_profile(
    profile_bytes: bytes,
    *,
    duration_manifest: CapturedDurationManifest,
    full_test_manifest_bytes: bytes,
    expected_validation_provenance: Mapping[str, object],
    pytest_exitstatus: int,
    expected_worker_count: int,
    expected_dist: str,
    formal_selection_eligible: bool,
) -> dict[str, object]:
    """Validate a historical profile using mandatory original manifest captures only."""
    if not isinstance(profile_bytes, bytes) or not profile_bytes:
        raise ValueError("historical runtime profile bytes are required")
    if not isinstance(duration_manifest, CapturedDurationManifest):
        raise ValueError("historical duration manifest capture is required")
    if not isinstance(full_test_manifest_bytes, bytes) or not full_test_manifest_bytes:
        raise ValueError("historical full test manifest bytes are required")
    if (
        not isinstance(expected_validation_provenance, Mapping)
        or not expected_validation_provenance
    ):
        raise ValueError("historical validation provenance is required")
    provenance_errors = validate_full_provenance(expected_validation_provenance)
    if provenance_errors:
        raise ValueError(
            "historical expected provenance is invalid: " + "; ".join(provenance_errors)
        )
    if type(pytest_exitstatus) is not int or pytest_exitstatus < 0:
        raise ValueError("historical pytest exit status must be a non-negative integer")
    if type(expected_worker_count) is not int or expected_worker_count < 1:
        raise ValueError("historical expected worker count must be a positive integer")
    if not isinstance(expected_dist, str) or not expected_dist.strip():
        raise ValueError("historical expected distribution is required")
    if type(formal_selection_eligible) is not bool:
        raise ValueError("historical formal selection eligibility must be boolean")
    try:
        payload = _parse_runtime_profile_bytes(profile_bytes, pytest_exitstatus=pytest_exitstatus)
    except ValueError as exc:
        return _runtime_profile_failure_payload(
            reason=str(exc), pytest_exitstatus=pytest_exitstatus
        )
    try:
        expected_files, file_error = _parse_expected_full_test_files(full_test_manifest_bytes)
        error = _runtime_profile_contract_error_from_inputs(
            payload,
            pytest_exitstatus=pytest_exitstatus,
            expected_worker_count=expected_worker_count,
            expected_dist=expected_dist,
            formal_selection_eligible=formal_selection_eligible,
            captured_duration_manifest=duration_manifest,
            expected_test_files=expected_files,
            expected_test_files_error=file_error,
            expected_validation_provenance=expected_validation_provenance,
        )
    except Exception as exc:  # noqa: BLE001 - malformed captured evidence fails closed
        error = f"contract evaluation failed closed: {type(exc).__name__}: {exc}"
    if error is not None:
        return _runtime_profile_failure_payload(
            reason=f"runtime profile contract is invalid: {error}",
            pytest_exitstatus=pytest_exitstatus,
        )
    return payload


def _persist_runtime_profile_before_summary(
    path: Path,
    payload: dict[str, object],
    *,
    pytest_exitstatus: int,
) -> dict[str, object]:
    temporary_path = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        _write_report(temporary_path, payload)
        temporary_path.replace(path)
    except OSError as exc:
        for candidate in (temporary_path, path):
            try:
                candidate.unlink(missing_ok=True)
            except OSError:
                pass
        return _runtime_profile_failure_payload(
            reason=f"runtime profile final artifact could not be written: {exc}",
            pytest_exitstatus=pytest_exitstatus,
        )
    return payload


def _summarize_runtime_profile(
    payload: Mapping[str, object],
    *,
    final_path: Path,
    repo_root: Path | None = None,
) -> dict[str, object]:
    collection = payload.get("collection")
    collection_summary = collection if isinstance(collection, dict) else {}
    scheduler = payload.get("scheduler")
    scheduler_summary = scheduler if isinstance(scheduler, dict) else {}
    warnings = payload.get("warnings")
    warning_rows = warnings if isinstance(warnings, list) else []
    return {
        "runtime_profile_path": _artifact_locator(final_path, repo_root=repo_root),
        "runtime_profile_status": payload.get("profile_status", "FAIL"),
        "formal_full_selection_eligible": scheduler_summary.get(
            "formal_full_selection_eligible",
            False,
        ),
        "runtime_profile_summary": {
            "schema_version": payload.get("schema_version"),
            "telemetry_status": payload.get("telemetry_status", "FAIL"),
            "performance_evidence_status": payload.get(
                "performance_evidence_status",
                "FAIL",
            ),
            "validation_provenance_binding_status": payload.get(
                "validation_provenance_binding_status",
                "FAIL",
            ),
            "formal_full_selection_eligible": scheduler_summary.get(
                "formal_full_selection_eligible",
                False,
            ),
            "stable_full_improvement_claimed": payload.get(
                "stable_full_improvement_claimed",
                False,
            ),
            "collection_count": collection_summary.get("count", 0),
            "collection_sha256": collection_summary.get("set_sha256"),
            "node_count": payload.get("node_count", 0),
            "file_count": payload.get("file_count", 0),
            "worker_count": payload.get("worker_count", 0),
            "tail_idle_total_seconds": payload.get("tail_idle_total_seconds", 0.0),
            "tail_idle_max_seconds": payload.get("tail_idle_max_seconds", 0.0),
            "scheduler_policy": scheduler_summary.get("policy"),
            "scheduler_applied": scheduler_summary.get("applied", False),
            "scheduler_fallback": scheduler_summary.get("fallback", True),
            "scheduler_fallback_reason": scheduler_summary.get("fallback_reason"),
            "duration_manifest_status": scheduler_summary.get("manifest_status"),
            "duration_complete_profile": scheduler_summary.get(
                "complete_profile",
                False,
            ),
            "duration_collection_coverage_verified": scheduler_summary.get(
                "complete_collection_verified",
                False,
            ),
            "duration_tracked_file_count": scheduler_summary.get(
                "tracked_file_count",
                0,
            ),
            "duration_tracked_node_count": scheduler_summary.get("tracked_node_count"),
            "duration_matched_file_count": scheduler_summary.get(
                "matched_tracked_file_count",
                0,
            ),
            "duration_matched_node_count": scheduler_summary.get(
                "matched_tracked_node_count",
                0,
            ),
            "warning_count": len(warning_rows),
        },
    }


def _mark_runtime_profile_not_applicable(
    payload: dict[str, object],
    *,
    reason: str,
) -> None:
    """Keep non-formal benchmark envelopes from claiming a missing Full sidecar."""

    schema_versions = payload.get("schema_versions")
    if isinstance(schema_versions, dict):
        schema_versions.pop("test_runtime_profile", None)
    payload["runtime_profile_status"] = "NOT_APPLICABLE"
    payload["runtime_profile_not_applicable_reason"] = reason


def _artifact_checksums(records: Sequence[dict[str, object]]) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for record in records:
        path = str(record.get("path") or "")
        if path:
            checksums[path] = str(record.get("sha256") or "")
    return checksums


def _artifact_locator(path: Path, *, repo_root: Path | None = None) -> str:
    if repo_root is None:
        return str(path)
    try:
        resolved_root = repo_root.resolve()
        resolved_path = path.resolve()
    except (OSError, RuntimeError):
        return str(path)
    try:
        return resolved_path.relative_to(resolved_root).as_posix()
    except ValueError:
        return str(resolved_path)


def _artifact_record(
    path: Path,
    *,
    repo_root: Path | None = None,
) -> dict[str, object]:
    exists = path.exists()
    is_file = exists and path.is_file()
    is_dir = exists and path.is_dir()
    return {
        "path": _artifact_locator(path, repo_root=repo_root),
        "exists": exists,
        "artifact_type": "directory" if is_dir else path.suffix.lower().lstrip(".") or "file",
        "sha256": _sha256_file(path) if is_file else None,
        "size_bytes": path.stat().st_size if is_file else None,
        "file_count": sum(1 for item in path.rglob("*") if item.is_file()) if is_dir else None,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _environment_summary() -> dict[str, str]:
    return {
        "python_version": sys.version.split()[0],
        "platform": platform.platform(),
        "working_directory": str(Path.cwd()),
    }


def _git_commit(repo_root: Path) -> str | None:
    from ai_trading_system.platform.architecture.source_preservation import inspection_git_result

    protected = inspection_git_result(repo_root, "rev-parse", "HEAD")
    if protected is not None:
        if protected.returncode != 0:
            return None
        return protected.stdout.decode("utf-8").strip() or None
    try:
        completed = subprocess.run(
            ("git", "rev-parse", "HEAD"),
            cwd=repo_root,
            text=True,
            capture_output=True,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout.strip() or None


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except (OSError, RuntimeError, ValueError):
        return False
    return True


def _render_runtime_reader_brief(payload: dict[str, object]) -> str:
    command = payload["command"]
    if not isinstance(command, (list, tuple)) or any(not isinstance(arg, str) for arg in command):
        raise TypeError("runtime command must be a sequence of strings")
    status = payload["status"]
    can_support = payload["can_support_promotion_evidence"]
    limitation = payload.get("promotion_evidence_limitation", "None")
    provenance = payload.get("validation_provenance")
    provenance = provenance if isinstance(provenance, dict) else {}
    lines = [
        "# Test Runtime Reader Brief",
        "",
        f"- Suite: `{payload['resolved_tier']}`",
        f"- Requested tier: `{payload['requested_tier']}`",
        f"- Status: `{status}`",
        f"- Promotion blocking: `{payload['promotion_blocking']}`",
        f"- Can support promotion evidence: `{can_support}`",
        f"- Slow suite allowed: `{payload['slow_suite_allowed']}`",
        f"- Test selection policy: `{json.dumps(payload.get('test_selection_policy'))}`",
        f"- Workers: `{payload['workers']}`",
        f"- Distribution: `{payload['dist']}`",
        f"- Elapsed seconds: `{payload['elapsed_seconds']}`",
        f"- Exit code: `{payload['exit_code']}`",
        f"- Production effect: `{payload['production_effect']}`",
        f"- Strategy logic changed: `{payload['strategy_logic_changed']}`",
        f"- Broker action allowed: `{payload['broker_action_allowed']}`",
        f"- Limitation: {limitation}",
        "",
        "## Validation Trigger Provenance",
        "",
        f"- Status: `{provenance.get('status', 'MISSING')}`",
        f"- Required for tier: `{provenance.get('required_for_tier', False)}`",
        f"- Trigger reason: `{provenance.get('trigger_reason')}`",
        f"- Task id: `{provenance.get('task_id')}`",
        f"- Boundary id: `{provenance.get('boundary_id')}`",
        f"- Parent run: `{provenance.get('parent_run')}`",
        f"- Field sources: `{json.dumps(provenance.get('field_sources', {}), sort_keys=True)}`",
        f"- Validation errors: `{json.dumps(provenance.get('validation_errors', []))}`",
        "",
        "## Command",
        "",
        "```powershell",
        _format_command(command),
        "```",
        "",
    ]
    if payload.get("benchmark_mode"):
        lines.extend(
            [
                "## Benchmark Runs",
                "",
                (
                    "- Runtime profile: `NOT_APPLICABLE` ("
                    f"{payload.get('runtime_profile_not_applicable_reason', 'benchmark mode')})"
                ),
                "",
                "|workers|dist|status|elapsed_seconds|slow_duration_count|",
                "|---|---|---|---|---|",
            ]
        )
        benchmark_runs = payload.get("benchmark_runs", [])
        if not isinstance(benchmark_runs, list):
            raise TypeError("benchmark_runs must be a list")
        for run in benchmark_runs:
            if not isinstance(run, dict):
                continue
            lines.append(
                "|"
                f"`{run.get('workers')}`|"
                f"`{run.get('dist')}`|"
                f"`{run.get('status')}`|"
                f"`{run.get('elapsed_seconds')}`|"
                f"`{run.get('pytest_slow_duration_count', 0)}`|"
            )
        lines.append("")
    slow_durations = payload.get("pytest_slow_durations")
    if isinstance(slow_durations, list) and slow_durations:
        lines.extend(
            [
                "## Slow Durations",
                "",
                "|seconds|phase|nodeid|",
                "|---|---|---|",
            ]
        )
        for row in slow_durations[:10]:
            if not isinstance(row, dict):
                continue
            lines.append(f"|`{row.get('seconds')}`|`{row.get('phase')}`|`{row.get('nodeid')}`|")
        lines.append("")
    if payload.get("pytest_output_log_path"):
        lines.extend(
            [
                "## Pytest Output",
                "",
                f"- Log: `{payload['pytest_output_log_path']}`",
                "",
            ]
        )
    runtime_profile = payload.get("runtime_profile_summary")
    if isinstance(runtime_profile, dict):
        lines.extend(
            [
                "## Runtime Profile",
                "",
                f"- Status: `{payload.get('runtime_profile_status', 'FAIL')}`",
                f"- Telemetry: `{runtime_profile.get('telemetry_status', 'FAIL')}`",
                (
                    "- Performance evidence: "
                    f"`{runtime_profile.get('performance_evidence_status', 'FAIL')}`"
                ),
                (
                    "- Provenance binding: "
                    f"`{runtime_profile.get('validation_provenance_binding_status', 'FAIL')}`"
                ),
                f"- Collection count: `{runtime_profile.get('collection_count', 0)}`",
                (f"- Duration manifest: `{runtime_profile.get('duration_manifest_status')}`"),
                (
                    "- Complete collection coverage: "
                    f"`{runtime_profile.get('duration_collection_coverage_verified', False)}`"
                ),
                (
                    "- Duration coverage files/nodes: "
                    f"`{runtime_profile.get('duration_matched_file_count', 0)}/"
                    f"{runtime_profile.get('duration_tracked_file_count', 0)}` files, "
                    f"`{runtime_profile.get('duration_matched_node_count', 0)}/"
                    f"{runtime_profile.get('duration_tracked_node_count')}` nodes"
                ),
                f"- Scheduler applied: `{runtime_profile.get('scheduler_applied', False)}`",
                f"- Scheduler fallback: `{runtime_profile.get('scheduler_fallback', True)}`",
                f"- Artifact: `{payload.get('runtime_profile_path', '')}`",
                "",
            ]
        )
    lines.extend(
        [
            "## Safety Boundary",
            "",
            (
                "This runtime artifact records pytest execution only. It does not mutate strategy "
                "logic, cached market data, production state, portfolio state, order tickets, or "
                "broker state."
            ),
            "",
        ]
    )
    return "\n".join(lines)


def _write_runtime_artifacts(
    artifact_dir: Path,
    payload: dict[str, object],
    *,
    repo_root: Path | None = None,
    pytest_output: str = "",
) -> dict[str, object]:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    # Auxiliary artifacts must reach their final bytes before their inventory is
    # sampled.  Always replacing the log also prevents stale bytes when callers
    # intentionally reuse an artifact directory with an empty pytest output.
    (artifact_dir / PYTEST_OUTPUT_LOG_NAME).write_text(pytest_output, encoding="utf-8")
    (artifact_dir / "test_runtime_reader_brief.md").write_text(
        _render_runtime_reader_brief(payload),
        encoding="utf-8",
    )
    if payload.get("benchmark_mode"):
        _write_report(artifact_dir / BENCHMARK_SUMMARY_NAME, payload)
    final_payload = dict(payload)
    final_payload["output_artifacts"] = _runtime_output_artifacts(
        artifact_dir,
        repo_root=repo_root,
        include_runtime_profile=(
            payload.get("resolved_tier") == "full"
            and payload.get("status") != "PRINT_ONLY"
            and not payload.get("benchmark_mode")
        ),
        include_benchmark_summary=bool(payload.get("benchmark_mode")),
    )
    # The summary is written last.  Its self-record intentionally omits a digest
    # and size because embedding either would change the measured bytes.
    _write_report(artifact_dir / "test_runtime_summary.json", final_payload)
    return final_payload


def _list_tiers() -> str:
    lines = [
        f"Default pytest parallelism: -n {DEFAULT_WORKERS} --dist {DEFAULT_DIST}",
        "Formal validation suites:",
    ]
    for name in sorted(TIER_SPECS):
        spec = TIER_SPECS[name]
        lines.append(
            "- "
            f"{name}: {spec.description} "
            f"(family={spec.suite_family}, promotion_blocking={spec.promotion_blocking}, "
            f"slow_suite_allowed={spec.slow_suite_allowed})"
        )
    lines.append("Legacy aliases:")
    for alias in sorted(TIER_ALIASES):
        lines.append(f"- {alias} -> {TIER_ALIASES[alias]}")
    return "\n".join(lines)


def _inspection_git_bytes(root: Path, *arguments: str) -> subprocess.CompletedProcess[bytes]:
    from ai_trading_system.platform.architecture.source_preservation import inspection_git_result

    protected = inspection_git_result(root, *arguments)
    if protected is not None:
        return protected
    return subprocess.run(
        ["git", "--no-optional-locks", *arguments], cwd=root, capture_output=True, timeout=30,
    )


def _full_task_commitment(root: Path, task_id: str, candidate: str) -> dict[str, object]:
    from ai_trading_system.platform.architecture.task_registry_canonical import (
        _canonical_fragment_path,
        validate_canonical_registry,
    )
    from ai_trading_system.platform.architecture.workflow_contract import (
        WorkflowContractError,
        bounded_regular_bytes,
        canonical_digest,
        read_bound_json,
    )
    from ai_trading_system.yaml_loader import safe_load_yaml_text

    registry = validate_canonical_registry(project_root=root)
    fragment = registry.fragment(task_id)
    status = fragment["projection"]["legacy_first_eight_cells"][3]
    if status == "DROPPED":
        raise PublicationFenceError("PUBLICATION_FULL_TASK_DROPPED", task_id)
    authority = fragment["task_record"].get("workflow_authority")
    if authority is not None and authority["status"] != "ACTIVE":
        raise PublicationFenceError("PUBLICATION_FULL_TASK_AUTHORITY_REVOKED", task_id)
    if authority is not None:
        try:
            scope = read_bound_json(root, authority["scope_ref"])
        except (WorkflowContractError, OSError, ValueError) as exc:
            raise PublicationFenceError("PUBLICATION_FULL_TASK_SCOPE_INVALID", str(exc)) from exc
        if scope.get("task_id") != task_id or scope.get("decision_id") != authority["decision_id"]:
            raise PublicationFenceError("PUBLICATION_FULL_TASK_SCOPE_INVALID", task_id)
    relative = _canonical_fragment_path(task_id)
    raw = bounded_regular_bytes(root / relative)
    object_name = candidate + ":" + relative
    size = _inspection_git_bytes(root, "cat-file", "-s", object_name)
    if size.returncode or not size.stdout.strip().isdigit() or int(size.stdout) > 16 * 1024 * 1024:
        raise PublicationFenceError("PUBLICATION_FULL_TASK_NOT_CANDIDATE", task_id)
    committed = _inspection_git_bytes(root, "cat-file", "blob", object_name)
    if (
        committed.returncode
        or raw not in {committed.stdout, committed.stdout.replace(b"\n", b"\r\n")}
        or safe_load_yaml_text(committed.stdout.decode("utf-8")) != fragment
    ):
        raise PublicationFenceError("PUBLICATION_FULL_TASK_NOT_CANDIDATE", task_id)
    return {
        "task_id": task_id, "status": status, "event_id": fragment["last_event_id"],
        "fragment_sha256": canonical_digest(fragment),
    }


def _validate_publication_transaction_for_full(
    args: argparse.Namespace,
    *,
    repo_root: Path,
    validation_provenance: Mapping[str, object],
    full_run_id: str,
    protected_launcher: bool = False,
) -> dict[str, object]:
    transaction_path = args.publication_transaction
    if transaction_path is None:
        raise PublicationFenceError(
            "PUBLICATION_TRANSACTION_REQUIRED",
            "executed Full requires --publication-transaction",
        )
    if args.benchmark_dist or args.benchmark_worker:
        raise PublicationFenceError(
            "PUBLICATION_FULL_BENCHMARK_NOT_FORMAL",
            "benchmark variants require a separate non-publication run",
        )
    task_id = validation_provenance.get("task_id")
    if not isinstance(task_id, str) or not task_id:
        raise PublicationFenceError(
            "PUBLICATION_VALIDATION_TASK_MISSING",
            str(task_id),
        )
    parent_path = _publication_parent_path(validation_provenance, repo_root=repo_root)
    fence = IntegrationPublicationFence(project_root=repo_root)
    publication_binding = fence.validate(
        transaction_path,
        exact_phase="FORMAL_VALIDATION_PRE",
        task_id=task_id,
        validation_tier="full",
        parent_path=parent_path,
        require_candidate=True,
    )
    _require_new_incomplete_parent_candidate(
        validation_provenance, publication_binding, fence=fence, transaction=transaction_path,
    )
    candidate_sha = publication_binding.get("candidate_sha")
    if not isinstance(candidate_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", candidate_sha):
        raise PublicationFenceError("FULL_READINESS_CANDIDATE_MISSING", str(candidate_sha))
    task_commitment = _full_task_commitment(repo_root, task_id, candidate_sha)
    acceptance_binding = None
    if task_id == DEVX015_ACCEPTANCE_TASK:
        try:
            acceptance_binding = bind_mandatory_acceptance(repo_root, candidate_sha)
        except ExecutionContainmentError as exc:
            raise PublicationFenceError("FULL_ACCEPTANCE_BINDING_INVALID", str(exc)) from exc
    # DEVX-015 owner decision 2026-09-24 (worker account scope = tests and Full):
    # when the registered host control declares PROTECTED_WORKER_REQUIRED, formal
    # Full runs only as the restricted worker through the installed protected
    # launcher. The ordinary entry must not bypass it; reject before the claim.
    coordination = fence.guard.store.coordination_binding
    if coordination is not None and not protected_launcher:
        from ai_trading_system.platform.architecture.workflow_coordination import (
            full_execution_policy,
        )

        state = coordination.assert_current(operation="observe")
        if full_execution_policy(state) == "PROTECTED_WORKER_REQUIRED":
            raise PublicationFenceError(
                "FULL_PROTECTED_LAUNCHER_REQUIRED",
                "registered host control requires the installed protected Full launcher",
            )
    # TRADING-2564: inspect already-bound bytes before consuming the Full claim.
    # This never renders, hydrates evidence, executes DQ, or repairs a transaction.
    from ai_trading_system.platform.architecture.source_preservation import (
        current_inspection_context,
    )

    selected_git = current_inspection_context(repo_root)
    readiness = (
        check_full_readiness(repo_root, candidate_sha) if selected_git is None
        else check_full_readiness(repo_root, candidate_sha, git_context=selected_git)
    )
    if (
        readiness.get("schema_version") != "full_validation_readiness.v1"
        or readiness.get("status") != "PASS"
        or readiness.get("candidate_sha") != candidate_sha
        or readiness.get("full_dispatch_ready") is not True
        or readiness.get("dispatch_performed") is not False
        or readiness.get("research_dispatch_allowed") is not False
        or readiness.get("dq_validation_executed") is not False
        or readiness.get("artifacts_written") is not False
    ):
        raise PublicationFenceError(
            "FULL_READINESS_BLOCKED",
            json.dumps(readiness, ensure_ascii=False, sort_keys=True),
        )
    dispatched = fence.checkpoint(
        transaction_path,
        phase="FULL_DISPATCHED",
        actor="integration-coordinator",
        full_run_id=full_run_id,
    )
    dispatched["pre_dispatch_readiness"] = readiness
    dispatched["task_commitment"] = task_commitment
    if acceptance_binding is not None:
        dispatched["mandatory_acceptance_binding"] = acceptance_binding
    return dispatched


def _record_publication_full_result(
    args: argparse.Namespace,
    *,
    repo_root: Path,
    status: str,
    summary_path: Path,
) -> dict[str, object]:
    if args.publication_transaction is None:
        raise PublicationFenceError(
            "PUBLICATION_TRANSACTION_REQUIRED",
            "Full result has no publication transaction",
        )
    return IntegrationPublicationFence(project_root=repo_root).checkpoint(
        args.publication_transaction,
        phase="FORMAL_VALIDATION_RESULT",
        actor="integration-coordinator",
        evidence_paths=(summary_path,),
        validation_status=status,
    )


def _recover_recorded_full_result(
    *, repo_root: Path, transaction: Path, task_id: str, action: str = "observe",
) -> dict[str, object]:
    """Finish a durable Full result, without executing or re-adopting loose artifacts."""
    from ai_trading_system.platform.architecture.workflow_contract import bounded_regular_bytes
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    fence = IntegrationPublicationFence(project_root=repo_root)
    if action not in {"observe", "terminate_frozen_job"}:
        raise ExecutionContainmentError("FULL_RECOVERY_ACTION")
    replay = fence.replay(transaction)
    if replay.status != "PASS" or replay.transaction["task_id"] != task_id:
        raise ExecutionContainmentError("FULL_RECOVERY_TRANSACTION_BINDING")
    if replay.phase not in {"FULL_DISPATCHED", "FORMAL_VALIDATION_RESULT", "FAILED"}:
        raise ExecutionContainmentError("FULL_RECOVERY_PHASE")
    leases = fence.guard.store.replay()
    if leases.status != "PASS":
        raise ExecutionContainmentError("FULL_RECOVERY_LEASE_REPLAY")
    lease = next(
        (row for row in leases.lease_heads if row.lease_id == replay.transaction["lease_id"]), None,
    )
    if lease is None or lease.actor != "integration-coordinator" or lease.state not in {
        "ACTIVE", "RELEASED",
    }:
        raise ExecutionContainmentError("FULL_RECOVERY_OWNER")
    execution = lease.execution
    if execution is None:
        return fence.recover_unstarted_full(transaction, actor="integration-coordinator")
    if execution.get("full_result_commitment") is None:
        from ai_trading_system.platform.architecture.workflow_contract import (
            canonical_digest,
            write_bound_once,
        )
        from ai_trading_system.platform.architecture.workflow_execution import observe_process
        from ai_trading_system.platform.artifacts import canonical_json_bytes

        if replay.phase not in {"FULL_DISPATCHED", "FAILED"}:
            raise ExecutionContainmentError("FULL_INCOMPLETE_RECOVERY_PHASE")
        claim = fence._full_claim(replay)
        request = execution["request"]
        expected_id = canonical_digest({
            "transaction": replay.transaction["transaction_sha256"],
            "full_run_id": claim["full_run_id"],
        })
        if (
            request["schema_version"] != "workflow_execution_request.v1"
            or request["candidate_sha"] != replay.candidate_sha
            or request["request_id"] != expected_id
            or request["job_name"] != "Local\\AITS-DEVX015-full-" + expected_id
            or execution["launcher"] != claim["launcher"]
        ):
            raise ExecutionContainmentError("FULL_INCOMPLETE_RECOVERY_BINDING")
        fence._require_publication_lease_intent(replay, lease)
        launcher = observe_process(**execution["launcher"])
        if launcher["state"] not in {"EXITED", "REUSED"}:
            return {
                "status": "OBSERVE_ONLY", "launcher": launcher,
                "dispatch_performed": False, "publication_allowed": False,
                "allowed_actions": ["observe", "launcher_owned_complete"],
            }
        recovered = fence.guard.store.execution_lifecycle().recover(
            lease.lease_id, actor="integration-coordinator", action=action,
        )
        if recovered["status"] not in {"RECOVERED_TERMINAL", "REPLAY_ONLY"}:
            return {**recovered, "dispatch_performed": False, "publication_allowed": False}
        terminal = recovered["execution"]
        if (
            terminal["state"] != "RESULT_RECORDED"
            or terminal.get("full_result_commitment") is not None
        ):
            raise ExecutionContainmentError("FULL_INCOMPLETE_RECOVERY_RESULT")
        # Generic custody may already preserve a self-reported PASS/FAIL file.
        # Keep that immutable fact; without the original validation commitment
        # this attempt is still INSUFFICIENT and cannot enter formal adoption.
        # Preserve original logs/summary/loose result bytes. This independent
        # closeout describes missing validation, never a fabricated pytest FAIL.
        report = _incomplete_recovery_report(replay, terminal)
        proof = fence._transaction_path(transaction).parent / "full_incomplete_recovery.json"
        raw = canonical_json_bytes(report)
        if proof.exists():
            if bounded_regular_bytes(proof) != raw:
                raise ExecutionContainmentError("FULL_INCOMPLETE_RECOVERY_CHANGED")
        else:
            write_bound_once(proof.parent, proof.name, raw)
        receipt = fence.release(
            transaction, actor="integration-coordinator", outcome="FAILED", evidence_paths=(proof,),
        )
        return {**report, "status": "RECOVERED_FAILED_ATTEMPT", "receipt": receipt}
    if lease.state != "ACTIVE" or replay.phase == "FAILED":
        raise ExecutionContainmentError("FULL_RECOVERY_OWNER")
    if execution is not None and execution["state"] == "EXIT_CONFIRMED" and (
        execution.get("full_result_commitment") is not None
    ):
        from ai_trading_system.platform.architecture.workflow_contract import write_bound_once
        from ai_trading_system.platform.architecture.workflow_execution import observe_process
        from ai_trading_system.platform.artifacts import canonical_json_bytes

        # Only the original launcher's immutable validation commitment may fill
        # this crash window. A loose file or a new caller's status is not proof.
        launcher = observe_process(**execution["launcher"])
        if launcher["state"] not in {"EXITED", "REUSED"}:
            return {
                "status": "OBSERVE_ONLY", "dispatch_performed": False,
                "publication_allowed": False, "launcher": launcher,
                "allowed_actions": ["observe", "launcher_owned_complete"],
            }
        frozen = execution["full_result_commitment"]["record"]
        result_path = Path(execution["request"]["result_path"])
        if (
            execution["request"].get("candidate_sha") != replay.candidate_sha
            or not result_path.is_absolute()
            or not result_path.is_relative_to(repo_root.resolve() / DEFAULT_ARTIFACT_ROOT)
        ):
            raise ExecutionContainmentError("FULL_RECOVERY_ARTIFACT_SCOPE")
        summary_path = result_path.parent / "test_runtime_summary.json"
        summary_ref = frozen.get("summary")
        if not isinstance(summary_ref, dict) or summary_ref.get("path") != summary_path.as_posix():
            raise ExecutionContainmentError("FULL_RECOVERY_SUMMARY_PATH")
        if (
            hashlib.sha256(bounded_regular_bytes(summary_path)).hexdigest()
            != summary_ref.get("sha256")
        ):
            raise ExecutionContainmentError("FULL_RECOVERY_SUMMARY_CHANGED")
        frozen_raw = canonical_json_bytes(frozen)
        if result_path.exists():
            if bounded_regular_bytes(result_path) != frozen_raw:
                raise ExecutionContainmentError("FULL_RECOVERY_RESULT_CHANGED")
        else:
            write_bound_once(result_path.parent, result_path.name, frozen_raw)
        recovered = fence.guard.store.execution_lifecycle().recover(
            lease.lease_id, actor="integration-coordinator",
        )
        if recovered["status"] not in {"RECOVERED_TERMINAL", "REPLAY_ONLY"}:
            return {**recovered, "dispatch_performed": False, "publication_allowed": False}
        execution = recovered["execution"]
    if execution is None or execution["state"] != "RESULT_RECORDED":
        return {
            "status": "RECOVERY_REQUIRED", "dispatch_performed": False,
            "publication_allowed": False, "reason": "FULL_RESULT_CUSTODY_NOT_RECORDED",
            "allowed_actions": ["observe", "launcher_owned_complete"],
        }
    request, custody = execution["request"], execution["result"]
    if request.get("candidate_sha") != replay.candidate_sha or custody["artifact"] is None:
        raise ExecutionContainmentError("FULL_RECOVERY_RESULT_UNPROVEN")
    result_path = Path(request["result_path"])
    expected_root = repo_root.resolve() / DEFAULT_ARTIFACT_ROOT
    if not result_path.is_absolute() or not result_path.is_relative_to(expected_root):
        raise ExecutionContainmentError("FULL_RECOVERY_ARTIFACT_SCOPE")
    result_raw = bounded_regular_bytes(result_path)
    if hashlib.sha256(result_raw).hexdigest() != custody["artifact"]["sha256"]:
        raise ExecutionContainmentError("FULL_RECOVERY_RESULT_CHANGED")
    result = load_strict_json_text(result_raw.decode("utf-8"))
    expected = {
        "schema_version": "full_execution_result.v1",
        "candidate_sha": replay.candidate_sha,
        "validation_identity_sha256": request["validation_identity_sha256"],
        "request_id": request["request_id"], "status": custody["status"],
    }
    if not isinstance(result, dict) or any(
        result.get(key) != value for key, value in expected.items()
    ):
        raise ExecutionContainmentError("FULL_RECOVERY_RESULT_BINDING")
    summary_path = result_path.parent / "test_runtime_summary.json"
    summary_ref = result.get("summary")
    if not isinstance(summary_ref, dict) or summary_ref.get("path") != summary_path.as_posix():
        raise ExecutionContainmentError("FULL_RECOVERY_SUMMARY_PATH")
    summary_raw = bounded_regular_bytes(summary_path)
    if hashlib.sha256(summary_raw).hexdigest() != summary_ref.get("sha256"):
        raise ExecutionContainmentError("FULL_RECOVERY_SUMMARY_CHANGED")
    summary = load_strict_json_text(summary_raw.decode("utf-8"))
    expected_summary = {
        "git_commit": replay.candidate_sha, "status": custody["status"],
        "execution_request_id": request["request_id"],
        "validation_identity_sha256": request["validation_identity_sha256"],
    }
    if not isinstance(summary, dict) or any(
        summary.get(key) != value for key, value in expected_summary.items()
    ):
        raise ExecutionContainmentError("FULL_RECOVERY_SUMMARY_BINDING")
    if replay.phase == "FULL_DISPATCHED":
        # Only this still-ACTIVE, independently verified recorded execution may
        # renew its own existing lease. An expired/reassigned lease was rejected above.
        fence.guard.store.heartbeat(
            lease.lease_id, actor="integration-coordinator", now=datetime.now(UTC),
        )
        fence.checkpoint(
            transaction, phase="FORMAL_VALIDATION_RESULT", actor="integration-coordinator",
            evidence_paths=(summary_path,), validation_status=str(custody["status"]),
        )
    else:
        event = replay.events[-1]
        evidence = event["payload"]["evidence"]
        if (
            event["payload"]["validation_status"] != custody["status"]
            or event["payload"].get("execution_result") != custody["artifact"]
            or not any(
                row["path"] == summary_path.relative_to(repo_root).as_posix()
                and row["sha256"] == summary_ref["sha256"] for row in evidence
            )
        ):
            raise ExecutionContainmentError("FULL_RECOVERY_FENCE_RESULT_CHANGED")
    return {
        "status": "RECOVERED_RESULT", "technical_status": custody["status"],
        "candidate_sha": replay.candidate_sha, "execution_request_id": request["request_id"],
        "dispatch_performed": False, "publication_allowed": False,
    }


def _full_readiness_semantics(
    value: object, *, root: Path, candidate: str, inspector_root: Path | None = None,
) -> dict[str, object]:
    """Require the original complete record; timings alone are observational."""
    if not isinstance(value, dict):
        raise ValueError("Full original readiness record is missing")
    checks = value.get("checks")
    if (
        value.get("schema_version") != "full_validation_readiness.v1"
        or value.get("status") != "PASS" or value.get("candidate_sha") != candidate
        or value.get("full_dispatch_ready") is not True or value.get("blockers") != []
        or value.get("inspection_code_root") != (inspector_root or root).as_posix()
        or value.get("target_root") != root.as_posix()
        or any(value.get(key) is not False for key in (
            "dispatch_performed", "research_dispatch_allowed", "dq_validation_executed",
            "artifacts_written",
        ))
        or not isinstance(checks, list)
        or any(not isinstance(row, dict) or row.get("status") != "PASS" for row in checks)
        or tuple(row.get("checker_id") for row in checks) != CHECKER_IDS
    ):
        raise ValueError("Full original readiness record is incomplete or inadmissible")
    return {
        **{key: item for key, item in value.items() if key not in {"elapsed_seconds", "checks"}},
        "checks": [{key: item for key, item in row.items() if key != "elapsed_seconds"}
                   for row in checks],
    }


def inspect_protected_full_publication_profile(
    *, repo_root: Path, transaction: Path, task_id: str, git_context: HeldGitConfiguration,
) -> dict[str, object]:
    """Read-only launcher entry using caller-held native custody for the whole probe."""
    from ai_trading_system.platform.architecture.source_preservation import HeldGitConfiguration

    if type(git_context) is not HeldGitConfiguration:
        raise ExecutionContainmentError("PROTECTED_PROFILE_CAPABILITY")
    with git_context.inspection(repo_root):
        return inspect_full_publication_profile(
            repo_root=repo_root, transaction=transaction, task_id=task_id,
        )


def inspect_full_publication_profile(
    *, repo_root: Path, transaction: Path, task_id: str,
) -> dict[str, object]:
    """Read-only profile, original readiness and execution-identity admission.

    The publication fence consumes this probe outside its short arbiter and
    rechecks the captured files and execution under that same original arbiter.
    Reuse the existing profile validator; never trust a summary's PASS booleans.
    """
    from ai_trading_system.platform.architecture.source_preservation import (
        current_inspection_context,
    )
    from ai_trading_system.platform.architecture.workflow_contract import (
        bounded_regular_bytes,
        canonical_digest,
    )
    from ai_trading_system.platform.architecture.workflow_execution import (
        bind_protected_inspector_runtime,
    )
    from ai_trading_system.platform.artifacts.json_contract import load_strict_json_text

    root = repo_root.resolve()
    fence = IntegrationPublicationFence(project_root=root)
    replay = fence.replay(transaction)
    if replay.phase not in {"FORMAL_VALIDATION_RESULT", "LOCAL_MAIN_FF_PRE"}:
        raise ValueError("Full profile inspection is outside publication admission phases")
    binding = fence.validate(
        transaction, exact_phase=replay.phase, task_id=task_id,
        validation_tier=fence.policy.heavyweight_tier, require_candidate=True,
    )
    selected_git = current_inspection_context(root)
    inspector_root = root
    if selected_git is not None:
        admission = bind_protected_inspector_runtime(
            root, str(binding["candidate_sha"]), git_context=selected_git,
        )
        inspector_root = Path(admission["inspector_root"])
        if inspector_root != _repo_root():
            raise ValueError("Full profile inspector origin differs from admitted runtime")
    lease = next(row for row in fence.guard.store.replay().active_leases
                 if row.lease_id == binding["lease_id"])
    execution = lease.execution
    if not isinstance(execution, Mapping):
        raise ValueError("Full execution is missing")
    result_events = [row for row in replay.events if row["phase"] == "FORMAL_VALIDATION_RESULT"]
    if len(result_events) != 1:
        raise ValueError("Full requires one original formal result event")
    event_payload = result_events[0]["payload"]
    if event_payload.get("validation_status") != "PASS":
        raise ValueError("Full technical result is not PASS")
    custody = fence._full_result_custody(replay, execution, "PASS", event_payload["evidence"])
    captures: dict[str, dict[str, object]] = {}

    def capture(path: Path, *, expected_sha: object = None) -> bytes:
        path = path.absolute()
        if not path.is_relative_to(root) or ".." in path.parts:
            raise ValueError("Full profile evidence is outside the candidate")
        raw = bounded_regular_bytes(path, budget=FULL_PROFILE_EVIDENCE_BUDGET_BYTES)
        digest = hashlib.sha256(raw).hexdigest()
        if expected_sha is not None and digest != expected_sha:
            raise ValueError("Full profile evidence changed: " + str(path))
        previous = captures.get(path.as_posix())
        if previous is not None and previous["sha256"] != digest:
            raise ValueError("Full profile evidence changed during inspection: " + str(path))
        captures[path.as_posix()] = {"path": path.as_posix(), "sha256": digest,
                                    "size_bytes": len(raw)}
        return raw

    record = load_strict_json_text(capture(
        Path(str(custody["path"])), expected_sha=custody["sha256"],
    ).decode("utf-8"))
    if not isinstance(record, dict) or not isinstance(record.get("summary"), dict):
        raise ValueError("Full result record is invalid")
    summary_path = Path(record["summary"]["path"])
    summary = load_strict_json_text(capture(
        summary_path, expected_sha=record["summary"]["sha256"],
    ).decode("utf-8"))
    request = execution["request"]
    identity = load_strict_json_text(capture(
        summary_path.parent / "execution_validation_identity.json",
        expected_sha=request["validation_identity_sha256"],
    ).decode("utf-8"))
    if not isinstance(summary, dict) or not isinstance(identity, dict):
        raise ValueError("Full summary or execution identity is invalid")
    candidate = str(binding["candidate_sha"])
    if (
        summary.get("git_commit") != candidate or summary.get("resolved_tier") != "full"
        or type(summary.get("exit_code")) is not int or summary["exit_code"] != 0
        or summary.get("status") != "PASS" or summary.get("print_only") is not False
        or summary.get("benchmark_mode") is not False
        or summary.get("command") != request["argv"]
        or identity.get("argv") != request["argv"]
        or identity.get("candidate_sha") != candidate
        or identity.get("environment_sha256") != request["environment_sha256"]
        or identity.get("publication_policy_sha256") != fence.policy_sha256
        or identity.get("task_authority_sha256") != _full_task_commitment(
            root, task_id, candidate,
        )["fragment_sha256"]
    ):
        raise ValueError("Full profile identity differs from the original execution")
    original_readiness = _full_readiness_semantics(
        identity.get("pre_dispatch_readiness"), root=root, candidate=candidate,
        inspector_root=inspector_root,
    )
    # Full dispatch binds and runs mandatory acceptance for the DEVX-015 task
    # (_validate_publication_transaction_for_full); other callers may bind it
    # explicitly. Any recorded binding is fully validated; the DEVX-015 task must
    # carry one; an unbound Full must carry no mandatory result at all.
    mandatory_task = (
        task_id == DEVX015_ACCEPTANCE_TASK
        or identity.get("mandatory_acceptance_binding") is not None
    )
    mandatory: dict[str, Any] | None = None
    mandatory_summary: dict[str, Any] | None = None
    mandatory_evidence: Mapping[str, Any] | None = None
    original_runtime = identity.get("runtime")
    if not isinstance(original_runtime, dict):
        raise ValueError("Full execution runtime is missing")
    if mandatory_task:
        mandatory = bind_mandatory_acceptance(root, candidate)
        if identity.get("mandatory_acceptance_binding") != mandatory:
            raise ValueError("Full mandatory binding changed")
        mandatory_summary = summary.get("mandatory_acceptance")
        if not isinstance(mandatory_summary, dict) or mandatory_summary.get("status") != "PASS":
            raise ValueError("Full mandatory result is missing")
        mandatory_evidence = validate_mandatory_acceptance_result(
            json.dumps(mandatory_summary.get("evidence"), allow_nan=False).encode(),
            mandatory, exit_code=0, expected_collections=16,
        )
        if mandatory_evidence.get("runtime_identity") != original_runtime:
            raise ValueError(
                "Full execution runtime differs from original mandatory worker evidence"
            )
    elif (
        identity.get("mandatory_acceptance_binding") is not None
        or summary.get("mandatory_acceptance") is not None
    ):
        raise ValueError("Full mandatory evidence present outside the DEVX-015 task")
    # Publication's own environment is not the old execution environment. The
    # original effective env remains bound by the request and worker evidence.
    current_dependencies: dict[Path, bytes] = {}
    current_runtime = acceptance_runtime_identity(captured_dependencies=current_dependencies)
    inspector_inputs = bind_inspector_implementation(
        _repo_root(), runtime_inputs=current_dependencies,
    )
    launcher_inputs = (
        mandatory_summary.get("launcher_identity") if mandatory_summary is not None else None
    )
    if "worker_identity" in identity or launcher_inputs is not None:
        for launcher_row in _recheck_launcher_identity(launcher_inputs):
            captures[str(launcher_row["path"])] = launcher_row
    if {key: value for key, value in original_runtime.items() if key != "environment_sha256"} != {
        key: value for key, value in current_runtime.items() if key != "environment_sha256"
    }:
        raise ValueError("Full interpreter or distribution identity changed")
    if mandatory_task and (
        mandatory_evidence is None or mandatory_summary is None or mandatory is None
        or mandatory_evidence.get("checkout_identity") != bind_acceptance_checkout(root, mandatory)
        or mandatory_summary.get("runner_identity") != capture_acceptance_implementation(
            root, candidate,
        )
    ):
        raise ValueError("Full mandatory source identity changed")
    profile_path = summary_path.parent / RUNTIME_PROFILE_OUTPUT_NAME
    records = summary.get("output_artifacts")
    matching = [
        row for row in records
        if isinstance(row, dict) and isinstance(row.get("path"), str)
        and (root / row["path"]).absolute() == profile_path
    ] if isinstance(records, list) else []
    if (
        len(matching) != 1 or matching[0].get("exists") is not True
        or re.fullmatch(r"[0-9a-f]{64}", str(matching[0].get("sha256"))) is None
    ):
        raise ValueError("Full must inventory one original runtime profile")
    raw_profile = capture(profile_path, expected_sha=matching[0].get("sha256"))
    if len(raw_profile) != matching[0].get("size_bytes"):
        raise ValueError("Full profile inventory size changed")
    provenance = summary.get("validation_provenance")
    if not isinstance(provenance, Mapping) or provenance.get("task_id") != task_id:
        raise ValueError("Full profile provenance task differs")
    duration_path = root / FULL_DURATION_PROFILE_MANIFEST
    duration_raw = capture(duration_path)
    test_manifest_raw = capture(root / FULL_TEST_MANIFEST)
    expected_files, manifest_error = _parse_expected_full_test_files(test_manifest_raw)
    if manifest_error or not expected_files:
        raise ValueError("Full file manifest is invalid")
    argv = request["argv"]
    if argv[:3] != [str(sys.executable), "-m", "pytest"]:
        raise ValueError("Full command is not the bound pytest interpreter")
    options: dict[str, object] = {}
    plugins: list[str] = []
    selected_files: set[str] = set()
    index = 3
    while index < len(argv):
        argument = argv[index]
        if argument in {"-p", "-n", "--dist", "--aits-duration-profile"}:
            if index + 1 >= len(argv):
                raise ValueError("Full command option value is missing")
            value = argv[index + 1]
            if argument == "-p":
                plugins.append(value)
            elif argument == "-n":
                if value != "16" or "-n16" in options:
                    raise ValueError("Full command worker contract differs")
                options["-n16"] = True
            elif argument in options:
                raise ValueError("Full command repeats a required option")
            else:
                options[argument] = value
            index += 2
        elif argument in {"-n16", "--no-loadscope-reorder", *TIER_SPECS["full"].pytest_args}:
            if argument in options:
                raise ValueError("Full command repeats a fixed option")
            options[argument] = True
            index += 1
        elif argument == "tests":
            selected_files.update(expected_files)
            index += 1
        elif argument in expected_files:
            selected_files.add(argument)
            index += 1
        elif _formal_full_selection_eligible(argv[index:index + 1], pytest_addopts=""):
            index += 1
        elif _formal_full_selection_eligible(argv[index:index + 2], pytest_addopts=""):
            index += 2
        else:
            raise ValueError("Full command contains unaudited selection: " + str(argument))
    if (
        selected_files != expected_files or options.get("-n16") is not True
        or options.get("--dist") != "loadfile"
        or options.get("--no-loadscope-reorder") is not True
        # The mandatory-acceptance plugin is added only by the mandatory runner
        # wrapper (_run_mandatory_acceptance_command), i.e. when a binding exists.
        or sorted(plugins) != sorted([
            *(["ai_trading_system.platform.architecture.workflow_execution"]
              if mandatory_task else []),
            FULL_RUNTIME_PROFILE_PLUGIN,
        ])
        or (root / str(options.get("--aits-duration-profile"))).absolute() != duration_path
    ):
        raise ValueError("Full command does not cover the fixed formal profile contract")
    # These inputs are independently read from C, not accepted from profile hashes.
    for relative, raw in ((FULL_DURATION_PROFILE_MANIFEST, duration_raw),
                          (FULL_TEST_MANIFEST, test_manifest_raw)):
        committed = _inspection_git_bytes(root, "show", f"{candidate}:{relative}")
        if committed.returncode or committed.stdout != raw:
            raise ValueError("Full profile input differs from candidate: " + relative)
    profile = validate_captured_runtime_profile(
        raw_profile, duration_manifest=CapturedDurationManifest(duration_raw, str(duration_path)),
        full_test_manifest_bytes=test_manifest_raw, expected_validation_provenance=provenance,
        pytest_exitstatus=0, expected_worker_count=16, expected_dist="loadfile",
        formal_selection_eligible=True,
    )
    # DEVX-018: the split-scope policy is one more candidate-bound input.
    # Only the finite read-only grammar of the protected inspector is allowed here.
    listed = _inspection_git_bytes(
        root, "ls-tree", candidate, "--", SCHEDULING_MANIFEST_RELATIVE_PATH
    )
    if listed.returncode:
        raise ValueError("Full scheduling manifest presence could not be inspected")
    committed_scheduling: SchedulingManifest | None = None
    if listed.stdout.strip():
        scheduling_raw = _inspection_git_bytes(
            root, "show", f"{candidate}:{SCHEDULING_MANIFEST_RELATIVE_PATH}"
        )
        if scheduling_raw.returncode:
            raise ValueError("Full scheduling manifest could not be read from candidate")
        try:
            committed_scheduling = parse_scheduling_manifest(scheduling_raw.stdout)
        except SchedulingManifestError as exc:
            raise ValueError(f"Full scheduling manifest is invalid: {exc}") from exc
    profile_scheduler = profile.get("scheduler")
    scheduling_error = split_scope_evidence_error(
        profile_scheduler.get("split_scope") if isinstance(profile_scheduler, Mapping) else None,
        committed_scheduling,
    )
    if scheduling_error is not None:
        raise ValueError("Full profile " + scheduling_error)
    if any(profile.get(key) != "PASS" for key in (
        "profile_status", "telemetry_status", "performance_evidence_status",
        "validation_provenance_binding_status",
    )):
        raise ValueError("Full formal profile rejected: " + str(profile.get("warnings", [])))
    # Reject cheap structural/raw/profile failures before the actual seven-checker
    # replay. This changes no acceptance condition and consumes no new Full claim.
    # Capture explicit runtime dependencies BEFORE replay, then use the existing
    # end-of-probe and locked fence rechecks. Git cleanliness alone cannot bind
    # ignored retained evidence or rendered Atlas outputs.
    from ai_trading_system.atlas.page_effectiveness import load_page_effectiveness_policy
    from ai_trading_system.contracts.strategy_research_page_effectiveness import (
        StrategyResearchPageEffectivenessManifest,
    )
    from ai_trading_system.platform.architecture import validation_readiness as readiness

    class CapturedEvidence(readiness._EvidenceCheck):
        def bindings(
            self, rows: Sequence[Mapping[str, Any]], *, sha_key: str = "sha256",
        ) -> None:
            super().bindings(rows, sha_key=sha_key)
            if self.blockers:
                raise ValueError("Full retained input binding rejected: " + str(self.blockers))
            for row in rows:
                capture(readiness._regular_file(root, row.get("path")), expected_sha=row[sha_key])

    retained = CapturedEvidence(root)
    for relative in (*readiness.RESULT_ADMISSIONS, readiness.O1_POLICY, readiness.SIGNAL_POLICY):
        capture(readiness._regular_file(root, relative))
    for relative in readiness.RESULT_ADMISSIONS:
        payload = readiness._committed_yaml(root, candidate, relative)
        retained.bindings(
            readiness._rows(payload.get("evidence_bindings"), relative), sha_key="file_sha256",
        )
    o1 = readiness._committed_yaml(root, candidate, readiness.O1_POLICY)
    isolated = readiness._mapping(o1.get("isolated_dq_evidence"), "isolated_dq_evidence")
    retained.bindings([readiness._mapping(isolated.get("gate"), "isolated_dq_evidence.gate")])
    readiness._signal_dependencies(retained, root, candidate)
    capture(readiness._regular_file(root, "config/atlas/page_effectiveness.yaml"))
    atlas_policy = load_page_effectiveness_policy(repository_root=root)
    atlas_manifest = StrategyResearchPageEffectivenessManifest.from_json_bytes(
        capture(readiness._regular_file(root, atlas_policy.manifest_path)),
    )
    for artifact in (*atlas_manifest.source_artifacts, *atlas_manifest.rendered_artifacts):
        if artifact.locator.startswith("outputs/"):
            raw = capture(
                readiness._regular_file(root, artifact.locator), expected_sha=artifact.sha256,
            )
            if len(raw) != artifact.byte_count:
                raise ValueError("Full Atlas input size changed: " + artifact.locator)
    current_readiness = _full_readiness_semantics(
        (check_full_readiness(root, candidate) if selected_git is None
         else check_full_readiness(root, candidate, git_context=selected_git)),
        root=root, candidate=candidate, inspector_root=inspector_root,
    )
    if original_readiness != current_readiness:
        raise ValueError("Full original readiness identity differs from current checked inputs")
    for relative in (
        "scripts/run_validation_tier.py",
        "src/ai_trading_system/platform/architecture/validation_readiness.py",
        "src/ai_trading_system/platform/architecture/integration_publication_fence.py",
        *([str(row["path"]) for row in mandatory_summary["runner_identity"]]
          if mandatory_summary is not None else []),
        *([str(row["path"]) for row in mandatory_evidence["checkout_identity"]]
          if mandatory_evidence is not None else []),
    ):
        capture(root / relative)
    # Catch changes during this read-only inspection as well as the fence's
    # later locked recheck. No proof file, lease renewal, or second store exists.
    for row in tuple(captures.values()):
        capture(Path(str(row["path"])), expected_sha=row["sha256"])
    final_inspector_inputs = bind_inspector_implementation(
        _repo_root(), runtime_inputs=current_dependencies,
    )
    final_inspector_map = {row["path"]: row for row in final_inspector_inputs}
    if any(final_inspector_map.get(row["path"]) != row for row in inspector_inputs):
        raise ValueError("Trusted inspector changed during inspection")
    for inspector_row in final_inspector_inputs:
        inspector_path = str(inspector_row["path"])
        previous = captures.get(inspector_path)
        if previous is not None and previous != inspector_row:
            raise ValueError("Candidate and inspector capture changed during inspection")
        captures[inspector_path] = inspector_row
    if fence.replay(transaction).events[-1]["event_id"] != replay.events[-1]["event_id"]:
        raise ValueError("Full publication state changed during profile inspection")
    return {
        "schema_version": "full_publication_profile_inspection.v1", "status": "PASS",
        "scope": "PROFILE_MANDATORY_AND_READINESS_IDENTITY",
        "transaction_sha256": binding["transaction_sha256"],
        "head_event_id": replay.events[-1]["event_id"], "candidate_sha": candidate,
        "execution_sha256": canonical_digest(execution), "captures": list(captures.values()),
        "dispatch_performed": False, "publication_performed": False,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run auditable pytest validation tiers for local development."
    )
    tier_choices = sorted({*TIER_SPECS, *TIER_ALIASES})
    parser.add_argument("tier", nargs="?", choices=tier_choices)
    parser.add_argument("--list", action="store_true", help="List available validation tiers.")
    parser.add_argument(
        "--inspect-full-publication-profile", action="store_true",
        help="只读重验原Full profile/mandatory及候选绑定；不派发、续租或发布。",
    )
    parser.add_argument(
        "--protected-full-candidate-root", type=Path,
        help="Execute governed Full through the isolated installed launcher for this candidate.",
    )
    parser.add_argument(
        "--protected-inspector", action="store_true",
        help="仅供固定安装的只读profile入口重新校验管理员文件保护；不授予权限。",
    )
    parser.add_argument(
        "--inspection-candidate-root", type=Path,
        help="只读复核的候选数据目录；仅限 --inspect-full-publication-profile。",
    )
    parser.add_argument(
        "--recover-full-action", choices=("observe", "terminate_frozen_job"), default="observe",
        help="Recovery only: observe, or terminate the independently verified original frozen Job.",
    )
    parser.add_argument(
        "--recover-full", action="store_true",
        help="仅核验已在租约收存的Full结果并补记fence；不重新执行测试或发布。",
    )
    parser.add_argument(
        "--print-only",
        action="store_true",
        help="Print the pytest command without executing it.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Optional path for a machine-readable validation summary.",
    )
    parser.add_argument(
        "--write-runtime-artifact",
        action="store_true",
        help=(
            "Write test_runtime_summary.json and test_runtime_reader_brief.md under "
            "outputs/validation_runtime/<run_id> or --artifact-dir."
        ),
    )
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        help="Optional exact directory for runtime artifacts when --write-runtime-artifact is set.",
    )
    parser.add_argument(
        "--trigger-reason",
        choices=VALIDATION_TRIGGER_REASONS,
        help=(
            "Audited reason for this validation run. Full requires one of the reviewed "
            "Full trigger reasons; CLI overrides AITS_VALIDATION_TRIGGER_REASON."
        ),
    )
    parser.add_argument(
        "--task-id",
        help="Owning task id; CLI overrides AITS_VALIDATION_TASK_ID.",
    )
    parser.add_argument(
        "--boundary-id",
        help="Stable integration/run boundary id; CLI overrides AITS_VALIDATION_BOUNDARY_ID.",
    )
    parser.add_argument(
        "--parent-run",
        help=(
            "Path to a prior failed Full test_runtime_summary.json or the original "
            "full_incomplete_recovery.json for failure_fix_rerun; "
            "CLI overrides "
            "AITS_VALIDATION_PARENT_RUN."
        ),
    )
    parser.add_argument(
        "--parent-run-import",
        help=(
            "Optional validation_parent_run_import.v1 proof for an exact-byte parent "
            "copied from another worktree; only valid with failure_fix_rerun and "
            "--parent-run. CLI overrides AITS_VALIDATION_PARENT_RUN_IMPORT."
        ),
    )
    parser.add_argument(
        "--publication-transaction",
        type=Path,
        help=(
            "Active integration_publication_fence.v1 transaction. Required for "
            "executed Full validation and ignored for --print-only."
        ),
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use for pytest. Defaults to the current interpreter.",
    )
    parser.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        help="Extra pytest argument appended to the selected tier command. Use --pytest-arg=VALUE.",
    )
    parser.add_argument(
        "--workers",
        default=DEFAULT_WORKERS,
        help=(
            "pytest-xdist worker count. Defaults to 16; pass 1, 0, serial, or none "
            "for serial reproduction."
        ),
    )
    parser.add_argument(
        "--dist",
        default=DEFAULT_DIST,
        help="pytest-xdist distribution strategy used when workers > 1. Defaults to loadfile.",
    )
    parser.add_argument(
        "--benchmark-dist",
        action="append",
        default=[],
        help=(
            "Run or print benchmark variants for one or more pytest-xdist distribution "
            "strategies. Accepts repeated values or comma-separated lists, for example "
            "--benchmark-dist=loadfile --benchmark-dist=worksteal."
        ),
    )
    parser.add_argument(
        "--benchmark-worker",
        action="append",
        default=[],
        help=(
            "Worker counts to combine with --benchmark-dist. Accepts repeated values or "
            "comma-separated lists; defaults to --workers when omitted."
        ),
    )
    return parser.parse_args(argv)


@dataclass(frozen=True)
class _ProtectedFullLaunch:
    root: Path
    worker_token: WindowsWorkerToken
    worker_environment: Mapping[str, str]
    worker_exchange: WindowsWorkerExchange


def _installed_worker_environment(profile: Path) -> dict[str, str]:
    """Build from held paths and the OS API, never the launcher's environment."""
    import ctypes
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.GetSystemDirectoryW.argtypes = [wintypes.LPWSTR, wintypes.UINT]
    kernel.GetSystemDirectoryW.restype = wintypes.UINT
    buffer = ctypes.create_unicode_buffer(32768)
    length = kernel.GetSystemDirectoryW(buffer, len(buffer))
    if not 0 < length < len(buffer):
        raise ExecutionContainmentError("WORKER_SYSTEM_DIRECTORY")
    system = Path(buffer.value)
    runtime = Path(sys.executable).absolute().parent
    return {
        "SystemRoot": str(system.parent), "WINDIR": str(system.parent),
        "COMSPEC": str(system / "cmd.exe"),
        "PATH": os.pathsep.join(str(path) for path in (
            runtime, runtime / "git/cmd", runtime / "git/mingw64/bin",
            runtime / "git/usr/bin", system,
        )),
        "TEMP": str(profile), "TMP": str(profile), "USERPROFILE": str(profile),
        "HOME": str(profile), "APPDATA": str(profile), "LOCALAPPDATA": str(profile),
        "PYTHONNOUSERSITE": "1", "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull,
    }


def run_installed_protected_full(argv: Sequence[str], *, candidate_root: Path) -> int:
    """Compose existing admission and execution; account must already be authorized.

    Does not enable accounts, repair ACLs, install software or clean retained evidence.
    Call only from the isolated protected installation. No caller-provided environment.
    """
    import uuid

    from ai_trading_system.platform.architecture.source_preservation import hold_installed_inspector
    from ai_trading_system.platform.architecture.workflow_coordination import WindowsWorkerExchange
    from ai_trading_system.platform.architecture.workflow_execution import WindowsWorkerToken

    args = _protected_full_arguments(argv)
    if (args.protected_full_candidate_root is not None
            and args.protected_full_candidate_root != candidate_root):
        raise ExecutionContainmentError("PROTECTED_FULL_ROOT")
    if not candidate_root.is_absolute():
        raise ExecutionContainmentError("PROTECTED_FULL_ROOT")
    with hold_installed_inspector(candidate_root) as context:
        with context.inspection(candidate_root):
            candidate = _git_commit(candidate_root)
        if candidate is None:
            raise ExecutionContainmentError("PROTECTED_FULL_CANDIDATE")
        with WindowsWorkerToken.logon_registered(
            candidate_root=candidate_root, candidate_sha=candidate, git_context=context,
        ) as token:
            root = (Path(sys.executable).absolute().parent.parent
                    / ("full-exchange-" + uuid.uuid4().hex))
            exchange = WindowsWorkerExchange.create(root, token)
            # The mandatory runner owns the single-use directory custody. Opening
            # it here would consume the capability before its actual consumer.
            environment = _installed_worker_environment(exchange.profile_directory)
            return run_protected_full(
                argv, candidate_root=candidate_root, git_context=context,
                worker_token=token, worker_environment=environment, worker_exchange=exchange,
            )


def _protected_full_arguments(argv: Sequence[str]) -> argparse.Namespace:
    args = parse_args(argv)
    if (args.tier != "full" or not args.write_runtime_artifact
            or args.publication_transaction is None or not args.task_id
            or args.list or args.print_only or args.recover_full
            or args.recover_full_action != "observe" or args.inspect_full_publication_profile
            or args.protected_inspector
            or args.inspection_candidate_root is not None
            or args.benchmark_dist or args.benchmark_worker):
        raise ExecutionContainmentError("PROTECTED_FULL_ARGUMENTS")
    return args


def run_protected_full(
    argv: Sequence[str], *, candidate_root: Path, git_context: HeldGitConfiguration,
    worker_token: WindowsWorkerToken, worker_environment: Mapping[str, str],
    worker_exchange: WindowsWorkerExchange,
) -> int:
    """Trusted launcher entry; live capabilities cannot be supplied through CLI JSON.

    The caller owns native custody and token lifetime through this entire call.
    This entry neither installs a runtime nor enrolls or enables an account.
    """
    from ai_trading_system.platform.architecture.source_preservation import HeldGitConfiguration
    from ai_trading_system.platform.architecture.workflow_coordination import WindowsWorkerExchange
    from ai_trading_system.platform.architecture.workflow_execution import (
        WindowsWorkerToken,
        bind_protected_inspector_runtime,
    )

    args = _protected_full_arguments(argv)
    if (type(git_context) is not HeldGitConfiguration
            or type(worker_token) is not WindowsWorkerToken
            or type(worker_exchange) is not WindowsWorkerExchange):
        raise ExecutionContainmentError("PROTECTED_FULL_CAPABILITIES")
    if not candidate_root.is_absolute():
        raise ExecutionContainmentError("PROTECTED_FULL_ROOT")
    environment = dict(worker_environment)
    if (not environment or any(
        not isinstance(key, str) or not key or "=" in key or "\0" in key
        or not isinstance(value, str) or "\0" in value for key, value in environment.items()
    ) or len({key.casefold() for key in environment}) != len(environment)):
        raise ExecutionContainmentError("PROTECTED_FULL_ENVIRONMENT")
    root = git_context.assert_current(candidate_root, {})
    worker_token.validate_launcher()
    worker_exchange.validate_worker(worker_token)
    # Verify current exchange custody before consuming the Full claim.
    _ = worker_exchange.profile_directory
    with git_context.inspection(root):
        candidate = _git_commit(root)
        if candidate is None:
            raise ExecutionContainmentError("PROTECTED_FULL_CANDIDATE")
        bind_protected_inspector_runtime(root, candidate, git_context=git_context)
        return _main(args, protected=_ProtectedFullLaunch(
            root, worker_token, environment, worker_exchange,
        ))


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    args = parse_args(arguments)
    if args.protected_full_candidate_root is not None:
        return run_installed_protected_full(
            arguments, candidate_root=args.protected_full_candidate_root,
        )
    return _main(args)


def _main(args: argparse.Namespace, *, protected: _ProtectedFullLaunch | None = None) -> int:
    if getattr(args, "protected_full_candidate_root", None) is not None and protected is None:
        raise ExecutionContainmentError("PROTECTED_FULL_CAPABILITIES")
    if args.protected_inspector and not args.inspect_full_publication_profile:
        print("error: protected inspector is restricted to read-only profile inspection",
              file=sys.stderr)
        return 2
    if args.inspection_candidate_root is not None and not args.inspect_full_publication_profile:
        print("error: candidate root is restricted to read-only profile inspection",
              file=sys.stderr)
        return 2
    if args.list:
        print(_list_tiers())
        return 0
    if not args.tier:
        print("error: choose a tier or pass --list", file=sys.stderr)
        return 2

    repo_root = _repo_root() if protected is None else protected.root
    if args.inspect_full_publication_profile:
        if (
            args.tier != "full" or args.publication_transaction is None or not args.task_id
            or args.recover_full or args.print_only or args.benchmark_dist or args.benchmark_worker
            or args.write_runtime_artifact or args.json_output
        ):
            print("error: profile inspection requires only full, transaction and task-id",
                  file=sys.stderr)
            return 2
        try:
            if args.inspection_candidate_root is not None:
                if not args.inspection_candidate_root.is_absolute():
                    raise ValueError("inspection candidate root must be absolute")
                repo_root = args.inspection_candidate_root.resolve(strict=True)
            if args.protected_inspector:
                from ai_trading_system.platform.architecture.source_preservation import (
                    hold_installed_inspector,
                )

                with hold_installed_inspector(repo_root) as git_context:
                    inspection = inspect_protected_full_publication_profile(
                        repo_root=repo_root, transaction=args.publication_transaction,
                        task_id=args.task_id, git_context=git_context,
                    )
            else:
                inspection = inspect_full_publication_profile(
                    repo_root=repo_root, transaction=args.publication_transaction,
                    task_id=args.task_id,
                )
        except (ExecutionContainmentError, PublicationFenceError, OSError, ValueError,
                KeyError, TypeError, StopIteration) as exc:
            print(f"error: Full publication profile rejected: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(inspection, ensure_ascii=False, sort_keys=True))
        return 0
    if args.recover_full_action != "observe" and not args.recover_full:
        print("error: --recover-full-action requires --recover-full", file=sys.stderr)
        return 2
    if args.recover_full:
        if (
            args.tier != "full" or args.publication_transaction is None or not args.task_id
            or args.print_only or args.benchmark_dist or args.benchmark_worker
        ):
            print("error: --recover-full requires full, transaction and task-id", file=sys.stderr)
            return 2
        try:
            recovered = _recover_recorded_full_result(
                repo_root=repo_root, transaction=args.publication_transaction, task_id=args.task_id,
                action=args.recover_full_action,
            )
        except (ExecutionContainmentError, PublicationFenceError, OSError, ValueError) as exc:
            print(f"error: Full recovery rejected: {exc}", file=sys.stderr)
            return 2
        print(json.dumps(recovered, ensure_ascii=False, sort_keys=True))
        return 0 if recovered["status"] in {"RECOVERED_RESULT", "RECOVERED_FAILED_ATTEMPT"} else 2
    resolved_tier = resolve_tier(args.tier)
    spec = TIER_SPECS[resolved_tier]
    validation_provenance = _validation_trigger_provenance(
        args,
        resolved_tier=resolved_tier,
        repo_root=repo_root,
    )
    if validation_provenance["status"] == "FAIL":
        validation_errors = validation_provenance["validation_errors"]
        if not isinstance(validation_errors, list) or any(
            not isinstance(error, str) for error in validation_errors
        ):
            raise TypeError("validation_errors must be a list of strings")
        print(
            "error: Validation trigger provenance failed: "
            + "; ".join(validation_errors),
            file=sys.stderr,
        )
        return 2
    if resolved_tier == "full" and not args.write_runtime_artifact:
        print(
            "error: Full validation requires --write-runtime-artifact for auditable "
            "summary/profile provenance",
            file=sys.stderr,
        )
        return 2
    started_at = _utc_now()
    run_id = _runtime_run_id(resolved_tier, started_at)
    publication_binding: dict[str, object] | None = None
    if resolved_tier == "full" and not args.print_only:
        try:
            publication_binding = _validate_publication_transaction_for_full(
                args,
                repo_root=repo_root,
                validation_provenance=validation_provenance,
                full_run_id=run_id,
                protected_launcher=protected is not None,
            )
        except PublicationFenceError as exc:
            print(
                f"error: Full publication transaction rejected: {exc.code}: {exc.message}",
                file=sys.stderr,
            )
            return 2
    artifact_dir = _artifact_dir(repo_root, args, run_id)
    command = build_command(
        args.tier,
        python_executable=args.python,
        repo_root=repo_root,
        extra_pytest_args=args.pytest_arg,
        workers=args.workers,
        dist=args.dist,
    )
    benchmark_mode = bool(args.benchmark_dist or args.benchmark_worker)
    benchmark_variants = (
        _benchmark_variants(
            tier=args.tier,
            python_executable=args.python,
            repo_root=repo_root,
            extra_pytest_args=args.pytest_arg,
            workers=args.workers,
            dist=args.dist,
            benchmark_workers=args.benchmark_worker,
            benchmark_dists=args.benchmark_dist,
        )
        if benchmark_mode
        else []
    )
    if args.json_output is not None and artifact_dir is not None:
        reserved_paths = _reserved_runtime_artifact_paths(
            artifact_dir,
            resolved_tier=resolved_tier,
            benchmark_variants=benchmark_variants,
        )
        if args.json_output.resolve() in reserved_paths:
            print(
                "error: --json-output must not overwrite a managed runtime artifact path",
                file=sys.stderr,
            )
            return 2
    print(f"Validation tier: {args.tier}", flush=True)
    print(f"Resolved tier: {resolved_tier}", flush=True)
    print(f"Coverage: {spec.description}", flush=True)
    print(f"Suite family: {spec.suite_family}", flush=True)
    print(f"Promotion blocking: {spec.promotion_blocking}", flush=True)
    print(f"Slow suite allowed: {spec.slow_suite_allowed}", flush=True)
    if spec.excluded_markers:
        print(
            "Excluded markers: "
            + ", ".join(spec.excluded_markers)
            + " (DEVX-018; Full remains the formal authority for these nodes)",
            flush=True,
        )
    print(f"Workers: {args.workers}", flush=True)
    print(f"Distribution: {args.dist}", flush=True)
    print(
        "Trigger provenance: "
        f"status={validation_provenance['status']} "
        f"reason={validation_provenance['trigger_reason']} "
        f"task={validation_provenance['task_id']} "
        f"boundary={validation_provenance['boundary_id']} "
        f"parent={validation_provenance['parent_run']}",
        flush=True,
    )
    print(f"Command: {_format_command(command)}", flush=True)
    if benchmark_mode:
        print(f"Benchmark variants: {len(benchmark_variants)}", flush=True)
        for variant in benchmark_variants:
            print(
                "Benchmark command "
                f"workers={variant['workers']} dist={variant['dist']}: "
                f"{_format_command(variant['command'])}",
                flush=True,
            )

    if args.print_only:
        ended_at = _utc_now()
        payload = _runtime_payload(
            repo_root=repo_root,
            requested_tier=args.tier,
            resolved_tier=resolved_tier,
            spec=spec,
            command=command,
            workers=args.workers,
            dist=args.dist,
            status="PRINT_ONLY",
            started_at=started_at,
            ended_at=ended_at,
            extra_pytest_args=args.pytest_arg,
            artifact_dir=artifact_dir,
            validation_provenance=validation_provenance,
        )
        if benchmark_mode:
            payload.update(
                {
                    "benchmark_mode": True,
                    "benchmark_runs": [
                        {
                            "workers": variant["workers"],
                            "dist": variant["dist"],
                            "status": "PRINT_ONLY",
                            "command": variant["command"],
                            "variant_id": variant["variant_id"],
                        }
                        for variant in benchmark_variants
                    ],
                    "benchmark_variant_count": len(benchmark_variants),
                    "benchmark_summary_path": (
                        _artifact_locator(
                            artifact_dir / BENCHMARK_SUMMARY_NAME,
                            repo_root=repo_root,
                        )
                        if artifact_dir is not None
                        else None
                    ),
                    "promotion_evidence_limitation": (
                        "BENCHMARK_PRINT_ONLY renders comparison commands but does not "
                        "execute pytest."
                    ),
                }
            )
            _mark_runtime_profile_not_applicable(
                payload,
                reason="benchmark_variants_are_non_formal",
            )
        if artifact_dir is not None:
            payload = _write_runtime_artifacts(
                artifact_dir,
                payload,
                repo_root=repo_root,
            )
            print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
            print(
                f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}",
                flush=True,
            )
        if args.json_output:
            _write_report(args.json_output, payload)
        return 0

    if benchmark_mode:
        if artifact_dir is not None:
            artifact_dir.mkdir(parents=True, exist_ok=True)
        benchmark_runs: list[dict[str, object]] = []
        for variant in benchmark_variants:
            print(
                f"Running benchmark variant workers={variant['workers']} dist={variant['dist']}",
                flush=True,
            )
            result = _run_command(
                variant["command"],
                cwd=repo_root,
                env_removals=BENCHMARK_REMOVED_VALIDATION_ENV_VARS,
            )
            status = "PASS" if result["exit_code"] == 0 else "FAIL"
            log_name = f"pytest_output_{variant['variant_id']}.log"
            pytest_output = str(result.get("pytest_output") or "")
            if artifact_dir is not None and pytest_output:
                (artifact_dir / log_name).write_text(pytest_output, encoding="utf-8")
            run_summary = {
                "workers": variant["workers"],
                "dist": variant["dist"],
                "variant_id": variant["variant_id"],
                "command": variant["command"],
                "status": status,
                "exit_code": result["exit_code"],
                "elapsed_seconds": result["elapsed_seconds"],
            }
            run_summary.update(
                _pytest_output_summary(
                    pytest_output,
                    artifact_dir=artifact_dir,
                    log_name=log_name,
                    repo_root=repo_root,
                )
            )
            benchmark_runs.append(run_summary)
        overall_exit_code = 0 if all(run["exit_code"] == 0 for run in benchmark_runs) else 1
        status = "PASS" if overall_exit_code == 0 else "FAIL"
        ended_at = _utc_now()
        payload = _runtime_payload(
            repo_root=repo_root,
            requested_tier=args.tier,
            resolved_tier=resolved_tier,
            spec=spec,
            command=command,
            workers=args.workers,
            dist=args.dist,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            result={"exit_code": overall_exit_code},
            extra_pytest_args=args.pytest_arg,
            artifact_dir=artifact_dir,
            validation_provenance=validation_provenance,
        )
        payload.update(
            {
                "benchmark_mode": True,
                "benchmark_runs": benchmark_runs,
                "benchmark_summary_path": (
                    _artifact_locator(
                        artifact_dir / BENCHMARK_SUMMARY_NAME,
                        repo_root=repo_root,
                    )
                    if artifact_dir is not None
                    else None
                ),
                "can_support_promotion_evidence": False,
                "promotion_evidence_limitation": (
                    "BENCHMARK_MODE compares validation runtime profiles and is not passing "
                    "promotion evidence. Run the selected tier once in normal mode for formal "
                    "validation evidence."
                ),
                **_summarize_benchmark_runs(benchmark_runs),
            }
        )
        if publication_binding is not None:
            payload["publication_transaction"] = publication_binding
        _mark_runtime_profile_not_applicable(
            payload,
            reason="benchmark_variants_are_non_formal",
        )
        print(f"Status: {status}")
        print(f"Elapsed seconds: {payload['elapsed_seconds']}")
        if artifact_dir is not None:
            payload = _write_runtime_artifacts(
                artifact_dir,
                payload,
                repo_root=repo_root,
            )
            print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
            print(
                f"Benchmark summary: {artifact_dir / BENCHMARK_SUMMARY_NAME}",
                flush=True,
            )
            print(
                f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}",
                flush=True,
            )
            if publication_binding is not None:
                try:
                    _record_publication_full_result(
                        args,
                        repo_root=repo_root,
                        status=status,
                        summary_path=artifact_dir / "test_runtime_summary.json",
                    )
                except PublicationFenceError as exc:
                    print(
                        "error: Full publication result recording failed: "
                        f"{exc.code}: {exc.message}",
                        file=sys.stderr,
                    )
                    return 2
        if args.json_output:
            _write_report(args.json_output, payload)
        return overall_exit_code

    full_runner = None
    if publication_binding is not None:
        assert artifact_dir is not None
        full_runner = _FullCommandRunner(
            args=args, root=repo_root, artifact_dir=artifact_dir,
            publication_binding=publication_binding, provenance=validation_provenance,
            worker_token=None if protected is None else protected.worker_token,
            worker_environment=None if protected is None else protected.worker_environment,
            worker_exchange=None if protected is None else protected.worker_exchange,
        )
    runtime_profile_temp_dir: tempfile.TemporaryDirectory[str] | None = None
    runtime_profile_temp_path: Path | None = None
    runtime_profile_payload: dict[str, object] | None = None
    formal_selection_eligible: bool | None = None
    env_overrides: dict[str, str] = {}
    if resolved_tier == "full" and artifact_dir is not None:
        runtime_profile_temp_dir, runtime_profile_temp_path = _allocate_runtime_profile(full_runner)
        env_overrides[RUNTIME_PROFILE_OUTPUT_ENV] = str(runtime_profile_temp_path)
        formal_selection_eligible = _formal_full_selection_eligible(
            args.pytest_arg,
            pytest_addopts=(
                full_runner.effective_environment() if full_runner is not None else os.environ
            ).get("PYTEST_ADDOPTS", ""),
        )
        env_overrides[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = (
            "1" if formal_selection_eligible else "0"
        )
        env_overrides[RUNTIME_PROFILE_VALIDATION_PROVENANCE_ENV] = json.dumps(
            validation_provenance,
            separators=(",", ":"),
            sort_keys=True,
        )

    acceptance_binding = (publication_binding or {}).get("mandatory_acceptance_binding")
    if isinstance(acceptance_binding, dict):
        result = _run_mandatory_acceptance_command(
            command,
            cwd=repo_root,
            binding=acceptance_binding,
            expected_collections=_runtime_worker_contract(args.workers, args.dist)[0] or 1,
            env_overrides=env_overrides,
            command_runner=full_runner,
        )
    else:
        result = (full_runner or _run_command)(
            command, cwd=repo_root, env_overrides=env_overrides,
        )
    if runtime_profile_temp_path is not None and artifact_dir is not None:
        subprocess_exitstatus = _command_exit_code(result)
        expected_full_test_files, expected_full_test_files_error = _load_expected_full_test_files(
            repo_root / FULL_TEST_MANIFEST
        )
        runtime_profile_payload = _read_runtime_profile_payload(
            runtime_profile_temp_path,
            pytest_exitstatus=subprocess_exitstatus,
            expected_worker_count=_runtime_worker_contract(
                args.workers,
                args.dist,
            )[0],
            expected_dist=_runtime_worker_contract(args.workers, args.dist)[1],
            formal_selection_eligible=formal_selection_eligible,
            duration_profile_path=repo_root / FULL_DURATION_PROFILE_MANIFEST,
            expected_test_files=expected_full_test_files,
            expected_test_files_error=expected_full_test_files_error,
            expected_validation_provenance=validation_provenance,
            scheduling_manifest_root=repo_root,
        )
        runtime_profile_payload = _attach_validation_provenance(
            runtime_profile_payload,
            validation_provenance,
        )
        runtime_profile_payload = _persist_runtime_profile_before_summary(
            artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME,
            runtime_profile_payload,
            pytest_exitstatus=subprocess_exitstatus,
        )
        result.update(
            _summarize_runtime_profile(
                runtime_profile_payload,
                final_path=artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME,
                repo_root=repo_root,
            )
        )
    status = "PASS" if result["exit_code"] == 0 else "FAIL"
    ended_at = _utc_now()
    payload = _runtime_payload(
        repo_root=repo_root,
        requested_tier=args.tier,
        resolved_tier=resolved_tier,
        spec=spec,
        command=command,
        workers=args.workers,
        dist=args.dist,
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        result=result,
        extra_pytest_args=args.pytest_arg,
        artifact_dir=artifact_dir,
        validation_provenance=validation_provenance,
    )
    if publication_binding is not None:
        payload["publication_transaction"] = publication_binding
    if "mandatory_acceptance" in result:
        payload["mandatory_acceptance"] = result["mandatory_acceptance"]
    for execution_key in ("execution_request_id", "validation_identity_sha256"):
        if execution_key in result:
            payload[execution_key] = result[execution_key]
    print(f"Status: {status}")
    print(f"Elapsed seconds: {result['elapsed_seconds']}")
    if artifact_dir is not None:
        payload = _write_runtime_artifacts(
            artifact_dir,
            payload,
            repo_root=repo_root,
            pytest_output=str(result.get("pytest_output") or ""),
        )
        print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
        print(f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}", flush=True)
        if runtime_profile_payload is not None:
            print(
                f"Runtime profile: {artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME}",
                flush=True,
            )
        if publication_binding is not None:
            try:
                assert full_runner is not None
                full_runner.record_summary(
                    artifact_dir / "test_runtime_summary.json", status=status,
                )
                _record_publication_full_result(
                    args,
                    repo_root=repo_root,
                    status=status,
                    summary_path=artifact_dir / "test_runtime_summary.json",
                )
            except PublicationFenceError as exc:
                print(
                    f"error: Full publication result recording failed: {exc.code}: {exc.message}",
                    file=sys.stderr,
                )
                return 2
    if args.json_output:
        _write_report(args.json_output, payload)
    if runtime_profile_temp_dir is not None:
        runtime_profile_temp_dir.cleanup()
    return _command_exit_code(result)


if __name__ == "__main__":
    raise SystemExit(main())
