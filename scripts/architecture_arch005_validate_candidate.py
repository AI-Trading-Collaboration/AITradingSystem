#!/usr/bin/env python3
"""Formal validation driver for ONE frozen candidate (DEVX-016 S3).

Runs the named-parent proof stage, the four pre-Full tiers and the formal Full in the fixed order
and records an observational progress file. It is normally launched detached by
scripts/architecture_arch005_publish.py; it never publishes and never edits tracked files.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import IO

from ai_trading_system.platform.architecture.parallel_replay_scope import (
    PARALLEL_REPLAY_ENV,
    SEAL_ENV,
    SEAL_ON_VALUE,
    without_switch,
)
from ai_trading_system.platform.architecture.publication_validation import (
    CandidateValidationDriver,
    ValidationConfig,
    ValidationDriverError,
)

ROOT = Path(__file__).resolve().parents[1]
STAGE_1_ID = "named-parent-positive"


class _Child:
    """A stage process plus the evidence log handle it writes to."""

    def __init__(self, argv: Sequence[str], log_path: Path, env: Mapping[str, str]) -> None:
        self._log: IO[bytes] = log_path.open("xb")  # exclusive: never overwrite stage evidence
        self._process = subprocess.Popen(
            list(argv), cwd=ROOT, env=dict(env), stdout=self._log, stderr=subprocess.STDOUT
        )
        self.pid = self._process.pid
        self.returncode: int | None = None

    def poll(self) -> int | None:
        code = self._process.poll()
        if code is not None:
            self.returncode = code
            self._log.close()
        return code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--expected-main", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--transaction", type=Path, required=True)
    parser.add_argument("--transaction-sha256", required=True)
    parser.add_argument("--lease-id", required=True)
    parser.add_argument("--parent-run", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--readiness", type=Path, required=True)
    parser.add_argument(
        "--stage-1-parallel-replay-workers",
        type=int,
        default=0,
        help="DEVX-023 P6: parallel lease replay for stage 1 only (reviewed scope; 0 = serial)",
    )
    parser.add_argument(
        "--stage-1-lease-seal",
        action="store_true",
        help="DEVX-023 candidate B: replay seal for stage 1 only (reviewed scope; default off)",
    )
    args = parser.parse_args()
    # The parallel replay switch is never inherited: only stage 1 gets it, from the argument, so the
    # pre-Full tiers and the formal Full stay serial whatever the launching shell exported.
    environment = {
        **without_switch(os.environ),
        "PYTHONPATH": str(ROOT / "src"),
        "PYTHONDONTWRITEBYTECODE": "1",
        "AITS_NAMED_DQ_PUBLICATION_TRANSACTION": str(args.transaction),
        "AITS_NAMED_DQ_SOURCE_LEASE_ID": args.lease_id,
    }
    stage_1_environment: dict[str, str] = {}
    if args.stage_1_parallel_replay_workers >= 2:
        stage_1_environment[PARALLEL_REPLAY_ENV] = str(args.stage_1_parallel_replay_workers)
    if args.stage_1_lease_seal:
        stage_1_environment[SEAL_ENV] = SEAL_ON_VALUE
    stage_environment = {STAGE_1_ID: stage_1_environment} if stage_1_environment else {}
    config = ValidationConfig(
        run_id=args.run_id,
        repository_root=ROOT,
        python=sys.executable,
        candidate_sha=args.candidate_sha,
        expected_main=args.expected_main,
        task_id=args.task_id,
        actor=args.actor,
        transaction=args.transaction,
        transaction_sha256=args.transaction_sha256,
        lease_id=args.lease_id,
        parent_run=args.parent_run,
        evidence_dir=args.evidence_dir,
        progress_path=args.evidence_dir / f"{args.run_id}_validation_progress.json",
        readiness_path=args.readiness,
        boundary_id=f"{args.run_id}-candidate-{args.candidate_sha[:12]}",
        environment=environment,
        stage_environment=stage_environment,
    )

    def run_command(argv: Sequence[str]) -> tuple[int, str]:
        completed = subprocess.run(
            list(argv),
            cwd=ROOT,
            env=environment,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return completed.returncode, completed.stdout

    driver = CandidateValidationDriver(
        config,
        run_command=run_command,
        start_process=_Child,
        sleep=time.sleep,
        monotonic=time.monotonic,
    )
    try:
        return driver.run()
    except ValidationDriverError as error:
        sys.stderr.write(
            json.dumps({"status": "BLOCKED", "code": error.code, "message": error.message})
        )
        return 2


if __name__ == "__main__":
    sys.exit(main())
