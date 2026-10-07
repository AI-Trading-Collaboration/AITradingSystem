"""DEVX-016 S3: the formal validation driver of one frozen candidate (was a per-candidate script).

The driver runs the named-parent proof stage, the four pre-Full tiers and the formal Full for ONE
frozen candidate, in the fixed order the manual driver used, and records an observational progress
file. It never publishes and never edits tracked files. The publication lease stays the only
execution authority; the driver only keeps that lease alive (heartbeat) between stages and refuses
to start, or to continue, when the candidate identity (HEAD, main, clean worktree, transaction at
FORMAL_VALIDATION_PRE) is no longer exactly the frozen one.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from ai_trading_system.platform.artifacts.writer import write_bytes_atomic

PROGRESS_SCHEMA_VERSION = "devx_016_validation_progress.v1"
TERMINAL_STATUSES = frozenset(
    {"VALIDATION_PASS_AWAITING_PUBLICATION_REVIEW", "STOPPED", "BLOCKED", "LEASE_REVIEW_REQUIRED"}
)
PASS_STATUS = "VALIDATION_PASS_AWAITING_PUBLICATION_REVIEW"
HEARTBEAT_INTERVAL_SECONDS = 300.0  # the execution lease TTL is 6 h; 5 min keeps wide headroom
POLL_SECONDS = 1.0

# Measured baselines of the formal stages (DEVX-022 section 15.1; stage 1 grew 505 -> 579 -> 751 s
# over the last three candidates and is analysed separately). Reporting only; never a decision.
STAGE_BASELINE_SECONDS: Mapping[str, float] = {
    "named-parent-positive": 502.0,
    "contract-validation": 281.0,
    "integration": 78.0,
    "reproducibility": 48.0,
    "architecture-fitness": 1412.0,
    "full": 8738.0,
}
TIER_STAGES = (
    "contract-validation",
    "integration",
    "reproducibility",
    "architecture-fitness",
    "full",
)


class ValidationDriverError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class StageProcess(Protocol):
    pid: int
    returncode: int | None

    def poll(self) -> int | None: ...


@dataclass(frozen=True)
class ValidationConfig:
    run_id: str
    repository_root: Path
    python: str
    candidate_sha: str
    expected_main: str
    task_id: str
    actor: str
    transaction: Path
    transaction_sha256: str
    lease_id: str
    parent_run: Path
    evidence_dir: Path
    progress_path: Path
    readiness_path: Path
    boundary_id: str
    environment: Mapping[str, str]


@dataclass(frozen=True)
class StageSpec:
    stage_id: str
    argv: tuple[str, ...]
    artifact_dir: Path | None
    log_path: Path
    heartbeat: bool


def build_stages(config: ValidationConfig) -> list[StageSpec]:
    """The fixed stage list: named-parent-positive, the four pre-Full tiers, then Full."""
    python = config.python
    stages = [
        StageSpec(
            "named-parent-positive",
            (
                python, "-B", "-m", "pytest", "-n", "16", "--dist", "loadfile", "-q",
                "tests/test_named_data_quality_actual_candidate.py",
            ),
            None,
            config.evidence_dir / f"{config.run_id}_validation_named-parent-positive.log",
            True,
        )
    ]  # fmt: skip
    for tier in TIER_STAGES:
        artifact = (
            config.repository_root / "outputs" / "validation_runtime" / f"{config.run_id}-{tier}"
        )
        argv = [
            python, "-B", "scripts/run_validation_tier.py", tier, "--write-runtime-artifact",
            "--artifact-dir", str(artifact),
            "--trigger-reason", "failure_fix_rerun",
            "--parent-run", str(config.parent_run),
            "--task-id", config.task_id,
            "--boundary-id", config.boundary_id,
            "--python", python,
        ]  # fmt: skip
        if tier == "architecture-fitness":
            argv.append("--pytest-arg=-x")
        if tier == "full":
            argv += ["--publication-transaction", str(config.transaction)]
        stages.append(
            StageSpec(
                tier,
                tuple(argv),
                artifact,
                config.evidence_dir / f"{config.run_id}_validation_{tier}.log",
                tier != "full",  # the Full manages its own lease heartbeat
            )
        )
    return stages


class CandidateValidationDriver:
    def __init__(
        self,
        config: ValidationConfig,
        *,
        run_command: Callable[[Sequence[str]], tuple[int, str]],
        start_process: Callable[[Sequence[str], Path, Mapping[str, str]], StageProcess],
        sleep: Callable[[float], None],
        monotonic: Callable[[], float],
        now: Callable[[], datetime] = lambda: datetime.now(tz=UTC),
        stages: Sequence[StageSpec] | None = None,
    ) -> None:
        self.config = config
        self.run_command = run_command
        self.start_process = start_process
        self.sleep = sleep
        self.monotonic = monotonic
        self.now = now
        self.stages = tuple(stages if stages is not None else build_stages(config))
        self.coordinator_id = uuid.uuid4().hex
        self.results: list[dict[str, Any]] = []
        self._owned = False

    # -- progress record (observational; the lease is the authority)
    def _record(self, status: str, stage: str, **extra: Any) -> None:
        config = self.config
        payload = {
            "schema_version": PROGRESS_SCHEMA_VERSION,
            "status": status,
            "stage": stage,
            "updated_at": self.now().isoformat(),
            "candidate_sha": config.candidate_sha,
            "expected_main": config.expected_main,
            "transaction": str(config.transaction),
            "transaction_sha256": config.transaction_sha256,
            "lease_id": config.lease_id,
            "coordinator_pid": os.getpid(),
            "coordinator_id": self.coordinator_id,
            "results": list(self.results),
            "parent_run": str(config.parent_run),
            "formal_authority": False,
            "production_effect": "none",
            "broker_action": "none",
            **extra,
        }
        raw = (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        path = config.progress_path
        path.parent.mkdir(parents=True, exist_ok=True)
        if not self._owned:
            # Exclusive creation: a second driver for the same run fails here instead of racing.
            try:
                descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError as exc:
                raise ValidationDriverError(
                    "VALIDATION_PROGRESS_EXISTS", f"{path.name} already exists"
                ) from exc
            try:
                os.write(descriptor, raw)
            finally:
                os.close(descriptor)
            self._owned = True
            return
        current = json.loads(path.read_text(encoding="utf-8"))
        if current.get("coordinator_id") != self.coordinator_id:
            raise ValidationDriverError(
                "VALIDATION_PROGRESS_NOT_OWNED", "the progress record belongs to another driver"
            )
        write_bytes_atomic(path, raw)

    # -- admission and identity
    def _require_identity(self) -> None:
        config = self.config
        for ref, expected in (("HEAD", config.candidate_sha), ("main", config.expected_main)):
            code, out = self.run_command(("git", "rev-parse", ref))
            if code != 0 or out.strip() != expected:
                raise ValidationDriverError(
                    "VALIDATION_CANDIDATE_IDENTITY",
                    f"{ref} is {out.strip()!r}, expected {expected}",
                )
        audit = self._json(
            (
                config.python,
                "-B",
                "scripts/architecture_arch005_checkout_guard.py",
                "worktree-audit",
            ),
            "VALIDATION_WORKTREE_AUDIT",
        )
        if audit.get("status") != "PASS" or audit.get("dirty_paths"):
            raise ValidationDriverError(
                "VALIDATION_WORKTREE_DIRTY", str(audit.get("dirty_paths"))[:200]
            )
        binding = self._json(
            (
                config.python,
                "-B",
                "scripts/architecture_arch005_publication_fence.py",
                "validate",
                "--transaction",
                str(config.transaction),
                "--exact-phase",
                "FORMAL_VALIDATION_PRE",
                "--task-id",
                config.task_id,
                "--parent",
                str(config.parent_run),
                "--require-candidate",
            ),
            "VALIDATION_FENCE_ADMISSION",
        )
        if (
            binding.get("status") != "PASS"
            or binding.get("candidate_sha") != config.candidate_sha
            or binding.get("transaction_sha256") != config.transaction_sha256
            or binding.get("lease_id") != config.lease_id
        ):
            raise ValidationDriverError(
                "VALIDATION_ADMISSION_CHANGED", "the publication admission is not the frozen one"
            )

    def _json(self, argv: Sequence[str], code: str) -> dict[str, Any]:
        exit_code, output = self.run_command(argv)
        try:
            parsed = json.loads(output)
        except json.JSONDecodeError:
            parsed = None
        if exit_code != 0 or not isinstance(parsed, dict):
            raise ValidationDriverError(code, f"exit {exit_code}")
        return parsed

    def _require_readiness(self) -> None:
        try:
            ready = json.loads(self.config.readiness_path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValidationDriverError("VALIDATION_READINESS_UNREADABLE", str(exc)) from exc
        if ready.get("status") != "PASS" or ready.get("candidate_sha") != self.config.candidate_sha:
            raise ValidationDriverError(
                "VALIDATION_READINESS_NOT_PASS", "readiness is not PASS for the frozen candidate"
            )

    # -- the run
    def run(self) -> int:
        stage = "admission"
        try:
            self._record("ADMISSION", stage)
            self._require_readiness()
            for spec in self.stages:
                stage = spec.stage_id
                self._require_identity()
                for path in (spec.log_path, spec.artifact_dir):
                    if path is not None and path.exists():
                        raise ValidationDriverError(
                            "VALIDATION_EVIDENCE_EXISTS", f"refusing to overwrite {path}"
                        )
                code = self._run_stage(spec)
                if code != 0:
                    return code
            self._record(PASS_STATUS, "complete")
            return 0
        except ValidationDriverError as error:
            if self._owned:
                self._record("BLOCKED", stage, detail=f"{error.code}: {error.message}")
            raise

    def _run_stage(self, spec: StageSpec) -> int:
        self._record(
            "STAGE_ADMITTED", spec.stage_id, command=list(spec.argv), log_path=str(spec.log_path)
        )
        spec.log_path.parent.mkdir(parents=True, exist_ok=True)
        started = self.monotonic()
        process = self.start_process(spec.argv, spec.log_path, self.config.environment)
        self._record("RUNNING", spec.stage_id, child_pid=process.pid, log_path=str(spec.log_path))
        due = started + HEARTBEAT_INTERVAL_SECONDS
        heartbeat_failed = False
        while process.poll() is None:
            self.sleep(POLL_SECONDS)
            if spec.heartbeat and self.monotonic() >= due:
                code, _ = self.run_command(
                    (
                        self.config.python,
                        "-B",
                        "scripts/architecture_arch005_checkout_guard.py",
                        "heartbeat",
                        "--lease-id",
                        self.config.lease_id,
                        "--actor",
                        self.config.actor,
                    )
                )
                if code != 0:
                    heartbeat_failed = True
                    self._record(
                        "LEASE_REVIEW_REQUIRED",
                        spec.stage_id,
                        detail="No further stages will dispatch after this child exits.",
                    )
                    due = float("inf")
                else:
                    due = self.monotonic() + HEARTBEAT_INTERVAL_SECONDS
        elapsed = round(self.monotonic() - started, 2)
        returncode = int(process.returncode if process.returncode is not None else 1)
        baseline = STAGE_BASELINE_SECONDS.get(spec.stage_id)
        self.results.append(
            {
                "stage": spec.stage_id,
                "exit_code": returncode,
                "elapsed_seconds": elapsed,
                "baseline_seconds": baseline,
                "log_path": str(spec.log_path),
            }
        )
        if returncode or heartbeat_failed:
            self._record("STOPPED", spec.stage_id, heartbeat_failed=heartbeat_failed)
            return returncode or 2
        self._record("STAGE_PASS", spec.stage_id)
        return 0


def read_progress(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return loaded if isinstance(loaded, dict) else None
