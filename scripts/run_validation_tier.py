from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_WORKERS = "16"
DEFAULT_DIST = "loadfile"
DEFAULT_ARTIFACT_ROOT = Path("outputs/validation_runtime")
SERIAL_WORKER_VALUES = {"", "0", "1", "serial", "none"}
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
    completed = subprocess.run(command, cwd=cwd, check=False)
    elapsed = round(time.perf_counter() - started, 2)
    return {
        "command": list(command),
        "exit_code": completed.returncode,
        "elapsed_seconds": elapsed,
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
        payload.update(result)
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
        "## Safety Boundary",
        "",
        (
            "This runtime artifact records pytest execution only. It does not mutate strategy "
            "logic, cached market data, production state, portfolio state, order tickets, or "
            "broker state."
        ),
        "",
    ]
    return "\n".join(lines)


def _write_runtime_artifacts(artifact_dir: Path, payload: dict[str, object]) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    _write_report(artifact_dir / "test_runtime_summary.json", payload)
    (artifact_dir / "test_runtime_reader_brief.md").write_text(
        _render_runtime_reader_brief(payload),
        encoding="utf-8",
    )


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
    print(f"Validation tier: {args.tier}", flush=True)
    print(f"Resolved tier: {resolved_tier}", flush=True)
    print(f"Coverage: {spec.description}", flush=True)
    print(f"Suite family: {spec.suite_family}", flush=True)
    print(f"Promotion blocking: {spec.promotion_blocking}", flush=True)
    print(f"Slow suite allowed: {spec.slow_suite_allowed}", flush=True)
    print(f"Workers: {args.workers}", flush=True)
    print(f"Distribution: {args.dist}", flush=True)
    print(f"Command: {_format_command(command)}", flush=True)

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
        _write_runtime_artifacts(artifact_dir, payload)
        print(f"Runtime artifact: {artifact_dir / 'test_runtime_summary.json'}", flush=True)
        print(f"Runtime reader brief: {artifact_dir / 'test_runtime_reader_brief.md'}", flush=True)
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
