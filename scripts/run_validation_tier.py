from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import subprocess
import sys
import time
from collections.abc import Sequence
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
    return [
        python_executable,
        "-m",
        "pytest",
        *_pytest_parallel_args(workers, dist),
        *paths,
        *spec.pytest_args,
        *extra_pytest_args,
    ]


def _format_command(command: Sequence[str]) -> str:
    return " ".join(command)


def _run_command(command: Sequence[str], *, cwd: Path) -> dict[str, object]:
    started = time.perf_counter()
    output_parts: list[str] = []
    with subprocess.Popen(
        command,
        cwd=cwd,
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
    output_artifacts = _runtime_output_artifacts(artifact_dir)
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
        "schema_versions": {"test_runtime_summary": "1"},
        "as_of": started_at.date().isoformat(),
        "random_seed": "not_applicable",
        "environment_summary": _environment_summary(),
        "output_artifacts": output_artifacts,
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


def _runtime_output_artifacts(artifact_dir: Path | None) -> list[dict[str, object]]:
    if artifact_dir is None:
        return []
    return [
        _artifact_record(artifact_dir / "test_runtime_summary.json"),
        _artifact_record(artifact_dir / "test_runtime_reader_brief.md"),
        _artifact_record(artifact_dir / PYTEST_OUTPUT_LOG_NAME),
    ]


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
) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_report(artifact_dir / "test_runtime_summary.json", payload)
    (artifact_dir / "test_runtime_reader_brief.md").write_text(
        _render_runtime_reader_brief(payload),
        encoding="utf-8",
    )
    if pytest_output:
        (artifact_dir / PYTEST_OUTPUT_LOG_NAME).write_text(pytest_output, encoding="utf-8")
    if payload.get("benchmark_mode"):
        _write_report(artifact_dir / BENCHMARK_SUMMARY_NAME, payload)


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
        if args.json_output:
            _write_report(args.json_output, payload)
        if artifact_dir is not None:
            _write_runtime_artifacts(artifact_dir, payload)
            print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
            print(
                f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}",
                flush=True,
            )
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
        if args.json_output:
            _write_report(args.json_output, payload)
        if artifact_dir is not None:
            _write_runtime_artifacts(artifact_dir, payload)
            print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
            print(
                f"Benchmark summary: {artifact_dir / BENCHMARK_SUMMARY_NAME}",
                flush=True,
            )
            print(
                f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}",
                flush=True,
            )
        return overall_exit_code

    result = _run_command(command, cwd=repo_root)
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
    if args.json_output:
        _write_report(args.json_output, payload)
    if artifact_dir is not None:
        _write_runtime_artifacts(
            artifact_dir,
            payload,
            pytest_output=str(result.get("pytest_output") or ""),
        )
        print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
        print(f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}", flush=True)
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
