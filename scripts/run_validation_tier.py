from __future__ import annotations

import argparse
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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

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
FULL_RUNTIME_PROFILE_PLUGIN = "scripts.pytest_runtime_profile"
FULL_DURATION_PROFILE_MANIFEST = "inputs/architecture/arch_004g2_full_duration_profile.yaml"
FULL_TEST_MANIFEST = "inputs/architecture/arch_004e_test_manifest.yaml"
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
    return [
        python_executable,
        "-m",
        "pytest",
        *_pytest_parallel_args(workers, dist),
        *runtime_profile_prefix,
        *paths,
        *spec.pytest_args,
        *extra_pytest_args,
        *runtime_profile_suffix,
    ]


def _format_command(command: Sequence[str]) -> str:
    return " ".join(command)


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    env_overrides: Mapping[str, str] | None = None,
) -> dict[str, object]:
    started = time.perf_counter()
    output_parts: list[str] = []
    process_env = None
    if env_overrides:
        process_env = dict(os.environ)
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


def _parse_pytest_slow_durations(pytest_output: str) -> list[dict[str, object]]:
    durations: list[dict[str, object]] = []
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
        summary["pytest_output_log_path"] = str(artifact_dir / log_name)
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
) -> list[dict[str, object]]:
    worker_values = _split_cli_values(benchmark_workers) or [workers]
    dist_values = _split_cli_values(benchmark_dists) or [dist]
    variants: list[dict[str, object]] = []
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
        key=lambda run: float(run.get("elapsed_seconds") or float("inf")),
        default=None,
    )
    slowest_run = max(
        completed_runs,
        key=lambda run: float(run.get("elapsed_seconds") or 0),
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


def _artifact_dir(repo_root: Path, args: argparse.Namespace, run_id: str) -> Path | None:
    if not args.write_runtime_artifact:
        return None
    if args.artifact_dir:
        return args.artifact_dir
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
        "workers": workers,
        "dist": dist,
        "extra_pytest_args": list(extra_pytest_args),
        "command": list(command),
        "resolved_config": {"validation_tier": resolved_tier},
        "input_artifacts": input_artifacts,
        "input_checksums": _artifact_checksums(input_artifacts),
        "schema_versions": {
            "test_runtime_summary": "1",
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
        "safety_boundary": dict(SAFETY_BOUNDARY),
        **SAFETY_BOUNDARY,
    }
    if result is not None:
        payload.update({key: value for key, value in result.items() if key != "pytest_output"})
    if pytest_output:
        payload.update(_pytest_output_summary(pytest_output, artifact_dir=artifact_dir))
    runtime_profile_summary = payload.get("runtime_profile_summary")
    if (
        isinstance(runtime_profile_summary, dict)
        and runtime_profile_summary.get("performance_evidence_status") != "PASS"
    ):
        payload["warnings"].append(
            "runtime_profile_performance_evidence="
            f"{runtime_profile_summary.get('performance_evidence_status', 'FAIL')}"
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
        payload["artifact_dir"] = str(artifact_dir)
        payload["summary_path"] = str(artifact_dir / "test_runtime_summary.json")
        payload["reader_brief_path"] = str(artifact_dir / "test_runtime_reader_brief.md")
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
    include_runtime_profile: bool,
    include_benchmark_summary: bool = False,
) -> list[dict[str, object]]:
    if artifact_dir is None:
        return []
    records = [
        _runtime_summary_self_record(artifact_dir / "test_runtime_summary.json"),
        _artifact_record(artifact_dir / "test_runtime_reader_brief.md"),
        _artifact_record(artifact_dir / PYTEST_OUTPUT_LOG_NAME),
    ]
    if include_runtime_profile:
        records.append(_artifact_record(artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME))
    if include_benchmark_summary:
        records.append(_artifact_record(artifact_dir / BENCHMARK_SUMMARY_NAME))
    return records


def _runtime_summary_self_record(path: Path) -> dict[str, object]:
    """Describe the final summary without embedding a self-invalidating digest."""
    return {
        "path": str(path),
        "exists": True,
        "artifact_type": "json",
        "sha256": None,
        "size_bytes": None,
        "file_count": None,
        "integrity_status": "SELF_REFERENCE_NOT_EMBEDDED",
        "measurement_reason": (
            "the final summary cannot embed a digest or byte size of its own "
            "final serialized bytes"
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


def _is_non_bool_int(value: object, *, minimum: int = 0) -> bool:
    return not isinstance(value, bool) and isinstance(value, int) and value >= minimum


def _is_nonnegative_finite_number(value: object) -> bool:
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
        if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0.0:
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
        payload = safe_load_yaml_path(path)
    except (OSError, ValueError, TypeError) as exc:
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
) -> str | None:
    statuses = {
        key: payload.get(key)
        for key in ("profile_status", "telemetry_status", "performance_evidence_status")
    }
    if any(value not in {"PASS", "FAIL"} for value in statuses.values()):
        return f"runtime profile status fields are invalid: {statuses!r}"

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
    for key, value in count_fields.items():
        if not _is_non_bool_int(value):
            return f"runtime profile {key} must be a non-negative integer"

    node_count = int(payload["node_count"])
    file_count = int(payload["file_count"])
    worker_count = int(payload["worker_count"])
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
                    "runtime profile phase UTC timing is not epoch-derived for "
                    f"{expected_nodeid}"
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
            if scheduler.get("xdist_dist") == "loadfile" and len(expected_workers) != 1:
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
            expected_rows = node_runtime_by_worker.get(worker_id, [])
            if not expected_rows:
                return "runtime profile worker aggregate is not node-derived for " f"{worker_id}"
            expected_start = min(runtime_row[0] for runtime_row in expected_rows)
            expected_stop = max(runtime_row[1] for runtime_row in expected_rows)
            expected_busy = round(sum(runtime_row[2] for runtime_row in expected_rows), 9)
            expected_span = round(expected_stop - expected_start, 9)
            expected_internal_idle = round(max(0.0, expected_span - expected_busy), 9)
            expected_tail_idle = round(
                max(0.0, float(global_last_stop) - expected_stop),
                9,
            )
            if (
                int(row_count) != len(expected_rows)
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
    if duration_profile_path is not None:
        resolved_profile_path = duration_profile_path.resolve()
        configured_path = scheduler.get("configured_manifest_path")
        try:
            configured_resolved = Path(str(configured_path)).resolve()
        except (OSError, ValueError, TypeError) as exc:
            return f"runtime profile configured manifest path is invalid: {exc}"
        if configured_resolved != resolved_profile_path:
            return "runtime profile configured manifest path differs from runner manifest"
        try:
            duration_manifest = safe_load_yaml_path(resolved_profile_path)
            manifest_sha256 = _sha256_file(resolved_profile_path)
        except (OSError, ValueError, TypeError) as exc:
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
            for key, expected in complete_manifest_hashes.items():
                if (
                    not isinstance(expected, str)
                    or re.fullmatch(r"[0-9a-f]{64}", expected) is None
                    or scheduler.get(key) != expected
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
            or scheduler.get("equal_duration_tie_policy")
            != "stable_first_seen_file_order"
            or not _numbers_close(scheduler.get("untracked_file_weight_seconds"), 0.0)
            or scheduler.get("xdist_dist") != "loadfile"
            or scheduler.get("loadscope_reorder_disabled") is not True
            or scheduler.get("file_internal_node_order_preserved") is not True
        ):
            return (
                "runtime profile applied scheduler does not satisfy the formal "
                "scheduler contract"
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
            or float(payload["elapsed_seconds"]) < float(payload["observed_test_window_seconds"])
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
) -> dict[str, object]:
    try:
        payload = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_non_finite_json_constant,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _runtime_profile_failure_payload(
            reason=f"runtime profile artifact missing or invalid: {exc}",
            pytest_exitstatus=pytest_exitstatus,
        )
    if not isinstance(payload, dict):
        return _runtime_profile_failure_payload(
            reason="runtime profile artifact root must be a mapping",
            pytest_exitstatus=pytest_exitstatus,
        )
    sidecar_exitstatus = payload.get("pytest_exitstatus")
    if (
        isinstance(sidecar_exitstatus, bool)
        or not isinstance(sidecar_exitstatus, int)
        or sidecar_exitstatus != pytest_exitstatus
    ):
        return _runtime_profile_failure_payload(
            reason=(
                "runtime profile pytest_exitstatus is invalid or mismatched: "
                f"sidecar={sidecar_exitstatus!r} subprocess={pytest_exitstatus!r}"
            ),
            pytest_exitstatus=pytest_exitstatus,
        )
    if (
        payload.get("schema_version") != RUNTIME_PROFILE_SCHEMA_VERSION
        or payload.get("report_type") != "test_runtime_profile"
    ):
        return _runtime_profile_failure_payload(
            reason="runtime profile artifact schema/report_type is invalid",
            pytest_exitstatus=pytest_exitstatus,
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
        )
    except Exception as exc:  # noqa: BLE001 - malformed evidence must not override pytest
        return _runtime_profile_failure_payload(
            reason=(
                "runtime profile contract evaluation failed closed: " f"{type(exc).__name__}: {exc}"
            ),
            pytest_exitstatus=pytest_exitstatus,
        )
    if contract_error is not None:
        return _runtime_profile_failure_payload(
            reason=f"runtime profile contract is invalid: {contract_error}",
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
) -> dict[str, object]:
    collection = payload.get("collection")
    collection_summary = collection if isinstance(collection, dict) else {}
    scheduler = payload.get("scheduler")
    scheduler_summary = scheduler if isinstance(scheduler, dict) else {}
    warnings = payload.get("warnings")
    warning_rows = warnings if isinstance(warnings, list) else []
    return {
        "runtime_profile_path": str(final_path),
        "runtime_profile_status": payload.get("profile_status", "FAIL"),
        "runtime_profile_summary": {
            "schema_version": payload.get("schema_version"),
            "telemetry_status": payload.get("telemetry_status", "FAIL"),
            "performance_evidence_status": payload.get(
                "performance_evidence_status",
                "FAIL",
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


def _artifact_checksums(records: Sequence[dict[str, object]]) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for record in records:
        path = str(record.get("path") or "")
        if path:
            checksums[path] = str(record.get("sha256") or "")
    return checksums


def _artifact_record(path: Path) -> dict[str, object]:
    exists = path.exists()
    is_file = exists and path.is_file()
    is_dir = exists and path.is_dir()
    return {
        "path": str(path),
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
    except ValueError:
        return False
    return True


def _render_runtime_reader_brief(payload: dict[str, object]) -> str:
    status = payload["status"]
    can_support = payload["can_support_promotion_evidence"]
    limitation = payload.get("promotion_evidence_limitation", "None")
    lines = [
        "# Test Runtime Reader Brief",
        "",
        f"- Suite: `{payload['resolved_tier']}`",
        f"- Requested tier: `{payload['requested_tier']}`",
        f"- Status: `{status}`",
        f"- Promotion blocking: `{payload['promotion_blocking']}`",
        f"- Can support promotion evidence: `{can_support}`",
        f"- Slow suite allowed: `{payload['slow_suite_allowed']}`",
        f"- Workers: `{payload['workers']}`",
        f"- Distribution: `{payload['dist']}`",
        f"- Elapsed seconds: `{payload['elapsed_seconds']}`",
        f"- Exit code: `{payload['exit_code']}`",
        f"- Production effect: `{payload['production_effect']}`",
        f"- Strategy logic changed: `{payload['strategy_logic_changed']}`",
        f"- Broker action allowed: `{payload['broker_action_allowed']}`",
        f"- Limitation: {limitation}",
        "",
        "## Command",
        "",
        "```powershell",
        _format_command(payload["command"]),  # type: ignore[arg-type]
        "```",
        "",
    ]
    if payload.get("benchmark_mode"):
        lines.extend(
            [
                "## Benchmark Runs",
                "",
                "|workers|dist|status|elapsed_seconds|slow_duration_count|",
                "|---|---|---|---|---|",
            ]
        )
        for run in payload.get("benchmark_runs", []):
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
            lines.append(
                "|" f"`{row.get('seconds')}`|" f"`{row.get('phase')}`|" f"`{row.get('nodeid')}`|"
            )
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
                f"- Collection count: `{runtime_profile.get('collection_count', 0)}`",
                ("- Duration manifest: " f"`{runtime_profile.get('duration_manifest_status')}`"),
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
        include_runtime_profile=payload.get("resolved_tier") == "full",
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run auditable pytest validation tiers for local development."
    )
    tier_choices = sorted({*TIER_SPECS, *TIER_ALIASES})
    parser.add_argument("tier", nargs="?", choices=tier_choices)
    parser.add_argument("--list", action="store_true", help="List available validation tiers.")
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


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list:
        print(_list_tiers())
        return 0
    if not args.tier:
        print("error: choose a tier or pass --list", file=sys.stderr)
        return 2

    repo_root = _repo_root()
    resolved_tier = resolve_tier(args.tier)
    spec = TIER_SPECS[resolved_tier]
    started_at = _utc_now()
    run_id = _runtime_run_id(resolved_tier, started_at)
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
    print(f"Workers: {args.workers}", flush=True)
    print(f"Distribution: {args.dist}", flush=True)
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
                        str(artifact_dir / BENCHMARK_SUMMARY_NAME)
                        if artifact_dir is not None
                        else None
                    ),
                    "promotion_evidence_limitation": (
                        "BENCHMARK_PRINT_ONLY renders comparison commands but does not "
                        "execute pytest."
                    ),
                }
            )
        if artifact_dir is not None:
            payload = _write_runtime_artifacts(artifact_dir, payload)
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
                "Running benchmark variant " f"workers={variant['workers']} dist={variant['dist']}",
                flush=True,
            )
            result = _run_command(variant["command"], cwd=repo_root)  # type: ignore[arg-type]
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
                _pytest_output_summary(pytest_output, artifact_dir=artifact_dir, log_name=log_name)
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
        )
        payload.update(
            {
                "benchmark_mode": True,
                "benchmark_runs": benchmark_runs,
                "benchmark_summary_path": (
                    str(artifact_dir / BENCHMARK_SUMMARY_NAME) if artifact_dir is not None else None
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
        print(f"Status: {status}")
        print(f"Elapsed seconds: {payload['elapsed_seconds']}")
        if artifact_dir is not None:
            payload = _write_runtime_artifacts(artifact_dir, payload)
            print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
            print(
                f"Benchmark summary: {artifact_dir / BENCHMARK_SUMMARY_NAME}",
                flush=True,
            )
            print(
                f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}",
                flush=True,
            )
        if args.json_output:
            _write_report(args.json_output, payload)
        return overall_exit_code

    runtime_profile_temp_dir: tempfile.TemporaryDirectory[str] | None = None
    runtime_profile_temp_path: Path | None = None
    runtime_profile_payload: dict[str, object] | None = None
    formal_selection_eligible: bool | None = None
    env_overrides: dict[str, str] = {}
    if resolved_tier == "full" and artifact_dir is not None:
        runtime_profile_temp_dir = tempfile.TemporaryDirectory(
            prefix="aits_pytest_runtime_profile_"
        )
        runtime_profile_temp_path = (
            Path(runtime_profile_temp_dir.name) / RUNTIME_PROFILE_OUTPUT_NAME
        )
        env_overrides[RUNTIME_PROFILE_OUTPUT_ENV] = str(runtime_profile_temp_path)
        formal_selection_eligible = (
            not args.pytest_arg and not os.environ.get("PYTEST_ADDOPTS", "").strip()
        )
        env_overrides[RUNTIME_PROFILE_FORMAL_SELECTION_ENV] = (
            "1" if formal_selection_eligible else "0"
        )

    result = _run_command(
        command,
        cwd=repo_root,
        env_overrides=env_overrides,
    )
    if runtime_profile_temp_path is not None and artifact_dir is not None:
        subprocess_exitstatus = int(result["exit_code"])
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
    )
    print(f"Status: {status}")
    print(f"Elapsed seconds: {result['elapsed_seconds']}")
    if artifact_dir is not None:
        payload = _write_runtime_artifacts(
            artifact_dir,
            payload,
            pytest_output=str(result.get("pytest_output") or ""),
        )
        print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
        print(f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}", flush=True)
        if runtime_profile_payload is not None:
            print(
                f"Runtime profile: {artifact_dir / RUNTIME_PROFILE_OUTPUT_NAME}",
                flush=True,
            )
    if args.json_output:
        _write_report(args.json_output, payload)
    if runtime_profile_temp_dir is not None:
        runtime_profile_temp_dir.cleanup()
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
