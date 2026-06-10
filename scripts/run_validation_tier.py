from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

DEFAULT_WORKERS = "8"
DEFAULT_DIST = "loadfile"
SERIAL_WORKER_VALUES = {"", "0", "1", "serial", "none"}


@dataclass(frozen=True)
class TierSpec:
    description: str
    paths: tuple[str, ...] = ()
    match_terms: tuple[str, ...] = ()
    pytest_args: tuple[str, ...] = ("-q", "--durations=20", "--durations-min=1")


TIER_SPECS: dict[str, TierSpec] = {
    "fast": TierSpec(
        description=(
            "High-signal local gate for CLI wiring, report registry, and documentation contract "
            "changes. This is not a replacement for full pytest on broad behavior changes."
        ),
        paths=(
            "tests/test_documentation_contract.py",
            "tests/test_report_index.py",
            "tests/test_cli_direct.py",
            "tests/test_etf_cli_aliases.py",
        ),
    ),
    "reader-brief": TierSpec(
        description="Reader Brief and report navigation validation.",
        paths=("tests/test_reader_brief.py", "tests/trading_engine"),
        pytest_args=(
            "-k",
            "reader_brief",
            "-q",
            "--durations=25",
            "--durations-min=1",
        ),
    ),
    "dynamic-v3": TierSpec(
        description="ETF dynamic-v3 rescue, historical replay, simulation, and advisory tests.",
        match_terms=("dynamic_v3", "test_etf_dynamic_rescue", "test_backtest_sim_", "test_sim_"),
        pytest_args=("-q", "--durations=40", "--durations-min=1"),
    ),
    "trading-engine": TierSpec(
        description="Paper trading engine, scheduler, and portfolio tooling tests.",
        paths=("tests/trading_engine",),
        pytest_args=("-q", "--durations=40", "--durations-min=1"),
    ),
    "full": TierSpec(
        description="Complete pytest gate. Keep this for final validation on broad changes.",
        paths=("tests",),
        pytest_args=("-q", "--durations=50", "--durations-min=1"),
    ),
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
    spec = TIER_SPECS[tier]
    paths = list(spec.paths)
    if spec.match_terms:
        paths.extend(_discover_matching_tests(repo_root, spec.match_terms))
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


def _list_tiers() -> str:
    lines = [
        f"Default pytest parallelism: -n {DEFAULT_WORKERS} --dist {DEFAULT_DIST}",
        "Available validation tiers:",
    ]
    for name in sorted(TIER_SPECS):
        lines.append(f"- {name}: {TIER_SPECS[name].description}")
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run auditable pytest validation tiers for local development."
    )
    parser.add_argument("tier", nargs="?", choices=sorted(TIER_SPECS))
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
            "pytest-xdist worker count. Defaults to 8; pass 1, 0, serial, or none "
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
    command = build_command(
        args.tier,
        python_executable=args.python,
        repo_root=repo_root,
        extra_pytest_args=args.pytest_arg,
        workers=args.workers,
        dist=args.dist,
    )
    print(f"Validation tier: {args.tier}", flush=True)
    print(f"Coverage: {TIER_SPECS[args.tier].description}", flush=True)
    print(f"Workers: {args.workers}", flush=True)
    print(f"Distribution: {args.dist}", flush=True)
    print(f"Command: {_format_command(command)}", flush=True)

    if args.print_only:
        payload: dict[str, object] = {
            "tier": args.tier,
            "status": "PRINT_ONLY",
            "workers": args.workers,
            "dist": args.dist,
            "command": command,
        }
        if args.json_output:
            _write_report(args.json_output, payload)
        return 0

    result = _run_command(command, cwd=repo_root)
    status = "PASS" if result["exit_code"] == 0 else "FAIL"
    payload = {
        "tier": args.tier,
        "status": status,
        "workers": args.workers,
        "dist": args.dist,
        **result,
    }
    print(f"Status: {status}")
    print(f"Elapsed seconds: {result['elapsed_seconds']}")
    if args.json_output:
        _write_report(args.json_output, payload)
    return int(result["exit_code"])


if __name__ == "__main__":
    raise SystemExit(main())
