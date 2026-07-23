from __future__ import annotations

import ast
import hashlib
import json
import shlex
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityExecutionContractError,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.contracts.operations import (
    DueRule,
    OperationsDispatchMode,
    OperationsDueContext,
    OperationsDuePolicy,
    OperationsDueResolution,
    OperationsRunDecision,
    OperationsShadowPlan,
    build_operations_shadow_plan,
    resolve_operations_due,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import (
    EntrypointRef,
    WorkflowCadence,
    WorkflowSpec,
    WorkflowStepSpec,
)
from ai_trading_system.core.production_effect import ProductionEffect
from ai_trading_system.platform.artifacts import write_json_atomic
from ai_trading_system.platform.operations.periodic_control import (
    PeriodicOperationsControlPolicy,
    load_periodic_operations_control_policy,
)
from ai_trading_system.platform.operations.runtime_control import OperationsRunControl
from ai_trading_system.trading_calendar import is_us_equity_trading_day

G4A_CONSUMER_IDS = (
    "daily_validate_data",
    "daily_score_daily",
    "weekly_backtest",
    "weekly_research_governance_summary_review",
    "monthly_data_source_coverage_review",
)

G4A_EXPECTED_LEGACY_ADAPTER_PATHS = (
    "src/ai_trading_system/legacy/periodic_operations_adapter.py",
    "src/ai_trading_system/legacy/scheduled_tasks_adapter.py",
)

DEFAULT_NATIVE_PERIODIC_CONSUMER_PARITY_PLAN_ROOT = (
    PROJECT_ROOT / "outputs" / "run_control" / "periodic" / "plans"
)

# G4A freezes existing direct imports. Any additional caller is migration debt, not
# an allowed way to bypass the native non-executing parity surface.
G4A_ALLOWED_LEGACY_DIRECT_CALLERS: Mapping[str, tuple[str, ...]] = {
    "ai_trading_system.legacy.periodic_operations_adapter": (
        "src/ai_trading_system/cli_commands/ops.py",
        "tests/test_arch_004f1_operations_control_plane.py",
    ),
    "ai_trading_system.legacy.scheduled_tasks_adapter": (
        "src/ai_trading_system/legacy/__init__.py",
        "src/ai_trading_system/legacy/periodic_operations_adapter.py",
        "src/ai_trading_system/ops_daily.py",
        "tests/test_arch_004f1_operations_control_plane.py",
    ),
}

_OUTPUT_ARTIFACT_TYPES: Mapping[str, tuple[str, ...]] = {
    "daily_validate_data": (
        "data_quality_execution_receipt.v1",
        "data_quality_report",
    ),
    "daily_score_daily": ("daily_score_artifact", "decision_artifact"),
    "weekly_backtest": ("backtest_report",),
    "weekly_research_governance_summary_review": ("research_governance_summary",),
    "monthly_data_source_coverage_review": ("data_source_coverage_report",),
}


class PeriodicConsumerMigrationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class DataQualityLineageRole(StrEnum):
    RECEIPT_PRODUCER_OBSERVATION = "receipt_producer_observation"
    VERIFIED_RECEIPT_CONSUMER = "verified_receipt_consumer"


class DataQualityReceiptVerifier(Protocol):
    def __call__(
        self,
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight: ...


class TradingCalendar(Protocol):
    def is_trading_day(self, value: date) -> bool: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class ScheduledTaskView(Protocol):
    @property
    def task_id(self) -> str: ...

    @property
    def command(self) -> str: ...

    @property
    def cadence(self) -> str: ...

    @property
    def production_effect(self) -> str: ...

    @property
    def activation_condition(self) -> object: ...

    @property
    def closed_market_behavior(self) -> str | None: ...

    @property
    def date_gate(self) -> str | None: ...

    @property
    def trigger_condition(self) -> str | None: ...

    @property
    def manual_review_required(self) -> bool: ...

    @property
    def max_attempts(self) -> int: ...


class ScheduledTasksConfigView(Protocol):
    def tasks_by_id(self) -> Mapping[str, ScheduledTaskView]: ...


@dataclass(frozen=True)
class SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)


@dataclass(frozen=True)
class XnysTradingCalendar:
    def is_trading_day(self, value: date) -> bool:
        return is_us_equity_trading_day(value)


@dataclass(frozen=True)
class NativeConsumerExpectedContext:
    as_of: date
    data_quality_as_of: date
    expected_policy_path: Path
    expected_input_roles: tuple[str, ...]
    daily_status: CanonicalStatus | None
    required_artifacts_ready: bool | None
    source_artifact_ids: tuple[str, ...]
    owner_gate_approved: bool | None
    owner_decision_id: str | None

    def __post_init__(self) -> None:
        if self.data_quality_as_of > self.as_of:
            raise PeriodicConsumerMigrationError(
                "G4A_DQ_AS_OF_AFTER_TRIGGER",
                (
                    f"data_quality_as_of={self.data_quality_as_of.isoformat()} "
                    f"trigger_as_of={self.as_of.isoformat()}"
                ),
            )
        roles = tuple(sorted(set(self.expected_input_roles)))
        if not roles or any(not item.strip() for item in roles):
            raise PeriodicConsumerMigrationError(
                "G4A_EXPECTED_INPUT_ROLES_INVALID", "at least one DQ input role is required"
            )
        object.__setattr__(self, "expected_input_roles", roles)
        refs = tuple(sorted(set(self.source_artifact_ids)))
        if any(not item.strip() for item in refs):
            raise PeriodicConsumerMigrationError(
                "G4A_SOURCE_ARTIFACT_REF_INVALID", "source artifact ids must be non-empty"
            )
        object.__setattr__(self, "source_artifact_ids", refs)


@dataclass(frozen=True)
class NativePeriodicConsumerDefinition:
    task_id: str
    cadence_id: str
    cadence: WorkflowCadence
    command_template: str
    resolved_command: tuple[str, ...]
    activation_condition: str
    closed_market_behavior: str | None
    date_gate: str
    condition_gate: str
    manual_review_required: bool
    expected_artifact_types: tuple[str, ...]
    data_quality_lineage_role: DataQualityLineageRole
    max_attempts: int
    legacy_production_effect: str
    production_effect: ProductionEffect = ProductionEffect.NONE

    def to_dict(self) -> dict[str, object]:
        return {
            "task_id": self.task_id,
            "cadence_id": self.cadence_id,
            "cadence": self.cadence.value,
            "command_template": self.command_template,
            "resolved_command": list(self.resolved_command),
            "activation_condition": self.activation_condition,
            "closed_market_behavior": self.closed_market_behavior,
            "date_gate": self.date_gate,
            "condition_gate": self.condition_gate,
            "manual_review_required": self.manual_review_required,
            "expected_artifact_types": list(self.expected_artifact_types),
            "data_quality_lineage_role": self.data_quality_lineage_role.value,
            "max_attempts": self.max_attempts,
            "legacy_production_effect": self.legacy_production_effect,
            "production_effect": self.production_effect.value,
        }


@dataclass(frozen=True)
class NativePeriodicConsumerPlanEntry:
    definition: NativePeriodicConsumerDefinition
    workflow_spec: WorkflowSpec
    due_resolution: OperationsDueResolution
    shadow_plan: OperationsShadowPlan
    dispatch_mode: OperationsDispatchMode
    dispatch_authorized: bool = False

    def __post_init__(self) -> None:
        if self.definition.task_id != self.workflow_spec.steps[0].step_id:
            raise PeriodicConsumerMigrationError(
                "G4A_TASK_IDENTITY_MISMATCH", self.definition.task_id
            )
        if self.workflow_spec.spec_id != self.shadow_plan.workflow_spec_id:
            raise PeriodicConsumerMigrationError(
                "G4A_WORKFLOW_SPEC_MISMATCH", self.definition.task_id
            )
        if self.dispatch_authorized:
            raise PeriodicConsumerMigrationError(
                "G4A_EXECUTION_FORBIDDEN",
                "dispatch mode is parity metadata and grants no authorization",
            )

    def to_dict(self) -> dict[str, object]:
        return {
            **self.definition.to_dict(),
            "workflow_spec": self.workflow_spec.to_dict(),
            "due_resolution": self.due_resolution.to_dict(),
            "shadow_plan": self.shadow_plan.to_dict(),
            "dispatch_mode": self.dispatch_mode.value,
            "dispatch_authorized": self.dispatch_authorized,
        }


@dataclass(frozen=True)
class NativePeriodicConsumerParityPlan:
    schema_version: str
    as_of: date
    data_quality_as_of: date
    generated_at: datetime
    status: CanonicalStatus
    entries: tuple[NativePeriodicConsumerPlanEntry, ...]
    receipt_id: str | None
    receipt_path: str | None
    blocker_codes: tuple[str, ...] = ()
    execution_enabled: bool = False
    automatic_dispatch_enabled: bool = False
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        if self.schema_version != "native_periodic_consumer_parity_plan.v1":
            raise PeriodicConsumerMigrationError("G4A_PLAN_SCHEMA_INVALID", self.schema_version)
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise PeriodicConsumerMigrationError(
                "G4A_CLOCK_INVALID", "generated_at must be timezone-aware"
            )
        if self.data_quality_as_of > self.as_of:
            raise PeriodicConsumerMigrationError(
                "G4A_DQ_AS_OF_AFTER_TRIGGER", self.data_quality_as_of.isoformat()
            )
        if tuple(entry.definition.task_id for entry in self.entries) != G4A_CONSUMER_IDS:
            raise PeriodicConsumerMigrationError(
                "G4A_CONSUMER_MATRIX_MISMATCH", "expected fixed 2/2/1 consumer order"
            )
        if self.execution_enabled or self.automatic_dispatch_enabled:
            raise PeriodicConsumerMigrationError(
                "G4A_EXECUTION_FORBIDDEN", "G4A is a non-executing parity harness"
            )
        if self.production_effect is not ProductionEffect.NONE:
            raise PeriodicConsumerMigrationError(
                "G4A_PRODUCTION_EFFECT_INVALID", self.production_effect.value
            )
        if self.status is CanonicalStatus.BLOCKED:
            if not self.blocker_codes or self.receipt_id is not None:
                raise PeriodicConsumerMigrationError(
                    "G4A_BLOCKED_PLAN_INVALID", "blocked plan must expose blockers only"
                )
        elif self.status is CanonicalStatus.PASS:
            if self.blocker_codes or self.receipt_id is None or self.receipt_path is None:
                raise PeriodicConsumerMigrationError(
                    "G4A_PASS_PLAN_INVALID", "PASS plan requires a verified receipt"
                )
        else:
            raise PeriodicConsumerMigrationError("G4A_PLAN_STATUS_INVALID", self.status.value)

    def entry(self, task_id: str) -> NativePeriodicConsumerPlanEntry:
        for item in self.entries:
            if item.definition.task_id == task_id:
                return item
        raise PeriodicConsumerMigrationError("G4A_CONSUMER_UNKNOWN", task_id)

    @property
    def plan_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"native_periodic_parity_{hashlib.sha256(material).hexdigest()[:24]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "as_of": self.as_of.isoformat(),
            "data_quality_as_of": self.data_quality_as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "entries": [entry.to_dict() for entry in self.entries],
            "receipt_id": self.receipt_id,
            "receipt_path": self.receipt_path,
            "blocker_codes": list(self.blocker_codes),
            "execution_enabled": self.execution_enabled,
            "automatic_dispatch_enabled": self.automatic_dispatch_enabled,
            "command_execution_enabled": False,
            "production_effect": self.production_effect.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"plan_id": self.plan_id, **self._semantic_payload()}


@dataclass(frozen=True)
class NativeParityRunnerResult:
    passed: bool
    retryable: bool
    artifact_refs: tuple[str, ...] = ()
    blocker_code: str | None = None

    def __post_init__(self) -> None:
        refs = tuple(sorted(set(self.artifact_refs)))
        if any(not item.strip() for item in refs):
            raise PeriodicConsumerMigrationError(
                "G4A_RUNNER_ARTIFACT_REF_INVALID", "artifact refs must be non-empty"
            )
        object.__setattr__(self, "artifact_refs", refs)
        if self.passed and (self.retryable or self.blocker_code is not None):
            raise PeriodicConsumerMigrationError(
                "G4A_RUNNER_RESULT_INVALID", "PASS cannot be retryable or blocked"
            )
        if not self.passed and (self.blocker_code is None or not self.blocker_code.strip()):
            raise PeriodicConsumerMigrationError(
                "G4A_RUNNER_RESULT_INVALID", "failed parity run requires blocker code"
            )
        if not self.passed and refs:
            raise PeriodicConsumerMigrationError(
                "G4A_RUNNER_RESULT_INVALID",
                "failed parity run cannot publish downstream artifact refs",
            )


class NativeParityRunner(Protocol):
    def __call__(self, entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult: ...


@dataclass(frozen=True)
class NativePeriodicConsumerRehearsalResult:
    task_id: str
    status: CanonicalStatus
    run_decision: OperationsRunDecision | None
    runner_called: bool
    artifact_refs: tuple[str, ...]
    blocker_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LegacyCallerRatchetAssessment:
    status: CanonicalStatus
    observed_callers: Mapping[str, tuple[str, ...]]
    unexpected_callers: tuple[str, ...]
    scan_blocker_codes: tuple[str, ...]


def build_native_periodic_consumer_parity_plan(
    receipt_path: Path,
    *,
    expected_context: NativeConsumerExpectedContext,
    scheduled: ScheduledTasksConfigView,
    verifier: DataQualityReceiptVerifier | None = None,
    calendar: TradingCalendar | None = None,
    clock: Clock | None = None,
    periodic_policy: PeriodicOperationsControlPolicy | None = None,
    project_root: Path = PROJECT_ROOT,
) -> NativePeriodicConsumerParityPlan:
    resolved_clock = clock or SystemClock()
    generated_at = resolved_clock.now()
    resolved_calendar = calendar or XnysTradingCalendar()
    resolved_periodic_policy = periodic_policy or load_periodic_operations_control_policy()
    definitions = _consumer_definitions(
        scheduled=scheduled,
        as_of=expected_context.as_of,
        calendar=resolved_calendar,
    )
    command_data_quality_as_of = _latest_trading_day(
        expected_context.as_of, calendar=resolved_calendar
    )
    if expected_context.data_quality_as_of != command_data_quality_as_of:
        raise PeriodicConsumerMigrationError(
            "G4A_DQ_AS_OF_COMMAND_MISMATCH",
            (
                f"expected={command_data_quality_as_of.isoformat()} "
                f"actual={expected_context.data_quality_as_of.isoformat()}"
            ),
        )

    try:
        resolved_verifier = verifier if verifier is not None else _verify_receipt
        preflight = resolved_verifier(
            receipt_path,
            expected_as_of=expected_context.data_quality_as_of,
            expected_policy_path=expected_context.expected_policy_path,
            expected_input_roles=expected_context.expected_input_roles,
            project_root=project_root,
        )
        preflight.assert_strict_passed()
        if preflight.as_of != expected_context.data_quality_as_of:
            raise DataQualityExecutionContractError(
                "DQ_AS_OF_MISMATCH", preflight.as_of.isoformat()
            )
    except (DataQualityExecutionContractError, OSError) as exc:
        blocker = getattr(exc, "code", "DQ_PROVENANCE_NOT_VERIFIED")
        entries = _plan_entries(
            definitions=definitions,
            expected_context=expected_context,
            generated_at=generated_at,
            calendar=resolved_calendar,
            periodic_policy=resolved_periodic_policy,
            preflight=None,
            forced_blocker=str(blocker),
        )
        return NativePeriodicConsumerParityPlan(
            schema_version="native_periodic_consumer_parity_plan.v1",
            as_of=expected_context.as_of,
            data_quality_as_of=expected_context.data_quality_as_of,
            generated_at=generated_at,
            status=CanonicalStatus.BLOCKED,
            entries=entries,
            receipt_id=None,
            receipt_path=None,
            blocker_codes=(str(blocker),),
        )

    entries = _plan_entries(
        definitions=definitions,
        expected_context=expected_context,
        generated_at=generated_at,
        calendar=resolved_calendar,
        periodic_policy=resolved_periodic_policy,
        preflight=preflight,
        forced_blocker=None,
    )
    return NativePeriodicConsumerParityPlan(
        schema_version="native_periodic_consumer_parity_plan.v1",
        as_of=expected_context.as_of,
        data_quality_as_of=expected_context.data_quality_as_of,
        generated_at=generated_at,
        status=CanonicalStatus.PASS,
        entries=entries,
        receipt_id=preflight.receipt_id,
        receipt_path=preflight.receipt_path,
    )


def default_native_periodic_consumer_parity_plan_path(
    as_of: date,
    root: Path = DEFAULT_NATIVE_PERIODIC_CONSUMER_PARITY_PLAN_ROOT,
) -> Path:
    return root / f"native_periodic_consumer_parity_{as_of.isoformat()}.json"


def write_native_periodic_consumer_parity_plan(
    plan: NativePeriodicConsumerParityPlan,
    path: Path | None = None,
) -> Path:
    """Atomically materialize only the non-executing G4A parity sidecar."""

    resolved_path = path or default_native_periodic_consumer_parity_plan_path(plan.as_of)
    return write_json_atomic(resolved_path, plan.to_dict(), sort_keys=True).path


def rehearse_native_periodic_consumer(
    plan: NativePeriodicConsumerParityPlan,
    *,
    task_id: str,
    control: OperationsRunControl,
    runner: NativeParityRunner,
    run_id: str,
    clock: Clock | None = None,
) -> NativePeriodicConsumerRehearsalResult:
    """Exercise runtime semantics without exposing a command-execution callable."""

    entry = plan.entry(task_id)
    resolution = entry.due_resolution
    blocked_by_preflight = (
        plan.status is CanonicalStatus.BLOCKED
        and entry.definition.data_quality_lineage_role
        is DataQualityLineageRole.VERIFIED_RECEIPT_CONSUMER
    )
    if blocked_by_preflight or resolution.status is CanonicalStatus.BLOCKED:
        blockers = plan.blocker_codes or resolution.reason_codes
        return NativePeriodicConsumerRehearsalResult(
            task_id=task_id,
            status=CanonicalStatus.BLOCKED,
            run_decision=None,
            runner_called=False,
            artifact_refs=(),
            blocker_codes=blockers,
        )
    if resolution.status is CanonicalStatus.NOT_DUE:
        return NativePeriodicConsumerRehearsalResult(
            task_id=task_id,
            status=CanonicalStatus.SKIPPED,
            run_decision=None,
            runner_called=False,
            artifact_refs=(),
            blocker_codes=resolution.reason_codes,
        )

    timestamp = (clock or SystemClock()).now()
    acquisition = control.acquire(
        spec=entry.workflow_spec,
        as_of=plan.as_of,
        run_id=run_id,
        now=timestamp,
    )
    if acquisition.lease is None:
        status = (
            CanonicalStatus.PASS
            if acquisition.resolution.decision is OperationsRunDecision.ALREADY_COMPLETE
            else CanonicalStatus.BLOCKED
        )
        return NativePeriodicConsumerRehearsalResult(
            task_id=task_id,
            status=status,
            run_decision=acquisition.resolution.decision,
            runner_called=False,
            artifact_refs=(),
            blocker_codes=acquisition.resolution.blocker_codes,
        )

    lease = acquisition.lease
    try:
        if task_id in acquisition.resolution.resume_completed_step_ids:
            lease.finish(CanonicalStatus.PASS, at=timestamp)
            return NativePeriodicConsumerRehearsalResult(
                task_id=task_id,
                status=CanonicalStatus.PASS,
                run_decision=acquisition.resolution.decision,
                runner_called=False,
                artifact_refs=(),
            )
        lease.start_step(task_id, at=timestamp)
        observation = runner(entry)
        if observation.passed:
            lease.pass_step(task_id, at=timestamp)
            lease.finish(CanonicalStatus.PASS, at=timestamp)
            return NativePeriodicConsumerRehearsalResult(
                task_id=task_id,
                status=CanonicalStatus.PASS,
                run_decision=acquisition.resolution.decision,
                runner_called=True,
                artifact_refs=observation.artifact_refs,
            )
        assert observation.blocker_code is not None
        retry_ready = lease.fail_step(
            task_id,
            retryable=observation.retryable,
            blocker_code=observation.blocker_code,
            at=timestamp,
        )
        if retry_ready:
            lease.release()
        return NativePeriodicConsumerRehearsalResult(
            task_id=task_id,
            status=CanonicalStatus.FAILED,
            run_decision=acquisition.resolution.decision,
            runner_called=True,
            artifact_refs=(),
            blocker_codes=(observation.blocker_code,),
        )
    except Exception:
        if not lease.released:
            lease.release()
        raise


def dispatch_native_periodic_consumer(*_: object, **__: object) -> None:
    raise PeriodicConsumerMigrationError(
        "G4A_EXECUTION_FORBIDDEN",
        "G4A exposes parity metadata only; command dispatch is not implemented",
    )


def assess_legacy_adapter_direct_callers(
    project_root: Path = PROJECT_ROOT,
) -> LegacyCallerRatchetAssessment:
    observed: dict[str, list[str]] = {module: [] for module in G4A_ALLOWED_LEGACY_DIRECT_CALLERS}
    scan_blockers: list[str] = []
    for source_root in (project_root / "src", project_root / "tests"):
        for path in sorted(source_root.rglob("*.py")):
            relative = path.relative_to(project_root).as_posix()
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError, UnicodeDecodeError):
                scan_blockers.append(f"LEGACY_CALLER_SCAN_FAILED:{relative}")
                continue
            modules = {
                node.module
                for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.module in observed
            }
            modules.update(
                alias.name
                for node in ast.walk(tree)
                if isinstance(node, ast.Import)
                for alias in node.names
                if alias.name in observed
            )
            modules.update(
                node.args[0].value
                for node in ast.walk(tree)
                if isinstance(node, ast.Call)
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and node.args[0].value in observed
                and (
                    (isinstance(node.func, ast.Name) and node.func.id == "__import__")
                    or (isinstance(node.func, ast.Name) and node.func.id == "import_module")
                    or (isinstance(node.func, ast.Attribute) and node.func.attr == "import_module")
                )
            )
            for module in modules:
                assert module is not None
                observed[module].append(relative)
    frozen = {module: tuple(paths) for module, paths in observed.items()}
    unexpected = tuple(
        sorted(
            f"{module}:{path}"
            for module, paths in frozen.items()
            for path in set(paths) - set(G4A_ALLOWED_LEGACY_DIRECT_CALLERS[module])
        )
    )
    return LegacyCallerRatchetAssessment(
        status=(
            CanonicalStatus.PASS
            if not unexpected and not scan_blockers
            else CanonicalStatus.BLOCKED
        ),
        observed_callers=frozen,
        unexpected_callers=unexpected,
        scan_blocker_codes=tuple(sorted(scan_blockers)),
    )


def _verify_receipt(
    receipt_path: Path,
    *,
    expected_as_of: date,
    expected_policy_path: Path,
    expected_input_roles: tuple[str, ...],
    project_root: Path = PROJECT_ROOT,
) -> VerifiedDataQualityPreflight:
    from ai_trading_system.data.quality_execution import (
        DataQualityExecutionError,
        verify_data_quality_execution_receipt,
    )

    try:
        return verify_data_quality_execution_receipt(
            receipt_path,
            expected_as_of=expected_as_of,
            expected_policy_path=expected_policy_path,
            expected_input_roles=expected_input_roles,
            project_root=project_root,
        )
    except DataQualityExecutionError as exc:
        raise DataQualityExecutionContractError(exc.code, exc.message) from exc


def _consumer_definitions(
    *,
    scheduled: ScheduledTasksConfigView,
    as_of: date,
    calendar: TradingCalendar,
) -> tuple[NativePeriodicConsumerDefinition, ...]:
    task_by_id = scheduled.tasks_by_id()
    missing = set(G4A_CONSUMER_IDS) - set(task_by_id)
    if missing:
        raise PeriodicConsumerMigrationError(
            "G4A_CONSUMER_MATRIX_MISMATCH", ",".join(sorted(missing))
        )
    return tuple(
        _definition(task_by_id[task_id], as_of=as_of, calendar=calendar)
        for task_id in G4A_CONSUMER_IDS
    )


def _definition(
    task: ScheduledTaskView, *, as_of: date, calendar: TradingCalendar
) -> NativePeriodicConsumerDefinition:
    expected_cadence = {
        "daily_validate_data": "daily_trading_day",
        "daily_score_daily": "daily_trading_day",
        "weekly_backtest": "weekly",
        "weekly_research_governance_summary_review": "weekly",
        "monthly_data_source_coverage_review": "monthly",
    }[task.task_id]
    if task.cadence != expected_cadence:
        raise PeriodicConsumerMigrationError(
            "G4A_CONSUMER_CADENCE_MISMATCH", f"{task.task_id}:{task.cadence}"
        )
    cadence = {
        "daily_trading_day": WorkflowCadence.DAILY,
        "weekly": WorkflowCadence.WEEKLY,
        "monthly": WorkflowCadence.MONTHLY,
    }[task.cadence]
    download_end = _latest_trading_day(as_of, calendar=calendar)
    resolved = task.command.replace("{as_of}", as_of.isoformat()).replace(
        "{download_end}", download_end.isoformat()
    )
    date_gate = (
        task.date_gate
        or {
            "daily_trading_day": "unified_daily_trigger",
            "weekly": "last_us_equity_trading_day_of_iso_week",
            "monthly": "last_us_equity_trading_day_of_month",
        }[task.cadence]
    )
    condition_gate = (
        task.trigger_condition
        or {
            "daily_trading_day": task.closed_market_behavior or "always",
            "weekly": "daily_pass_dq_pass_artifacts_ready_owner_approved",
            "monthly": "daily_pass_dq_pass_artifacts_ready_owner_approved",
        }[task.cadence]
    )
    return NativePeriodicConsumerDefinition(
        task_id=task.task_id,
        cadence_id=task.cadence,
        cadence=cadence,
        command_template=task.command,
        resolved_command=tuple(shlex.split(resolved, posix=True)),
        activation_condition=str(task.activation_condition),
        closed_market_behavior=task.closed_market_behavior,
        date_gate=date_gate,
        condition_gate=condition_gate,
        manual_review_required=task.manual_review_required,
        expected_artifact_types=_OUTPUT_ARTIFACT_TYPES[task.task_id],
        data_quality_lineage_role=(
            DataQualityLineageRole.RECEIPT_PRODUCER_OBSERVATION
            if task.task_id == "daily_validate_data"
            else DataQualityLineageRole.VERIFIED_RECEIPT_CONSUMER
        ),
        max_attempts=task.max_attempts,
        legacy_production_effect=task.production_effect,
    )


def _plan_entries(
    *,
    definitions: tuple[NativePeriodicConsumerDefinition, ...],
    expected_context: NativeConsumerExpectedContext,
    generated_at: datetime,
    calendar: TradingCalendar,
    periodic_policy: PeriodicOperationsControlPolicy,
    preflight: VerifiedDataQualityPreflight | None,
    forced_blocker: str | None,
) -> tuple[NativePeriodicConsumerPlanEntry, ...]:
    entries: list[NativePeriodicConsumerPlanEntry] = []
    for definition in definitions:
        due_policy, dispatch_mode = _due_policy(
            definition=definition,
            periodic_policy=periodic_policy,
        )
        spec = _workflow_spec(definition=definition, due_policy=due_policy)
        consumes_receipt = (
            definition.data_quality_lineage_role is DataQualityLineageRole.VERIFIED_RECEIPT_CONSUMER
        )
        if forced_blocker is not None and consumes_receipt:
            resolution = OperationsDueResolution(
                workflow_id=spec.workflow_id,
                due_policy_id=due_policy.policy_id,
                cadence=due_policy.cadence,
                as_of=expected_context.as_of,
                status=CanonicalStatus.BLOCKED,
                reason_codes=(forced_blocker,),
            )
        else:
            if consumes_receipt:
                assert preflight is not None
            is_trading_day = calendar.is_trading_day(expected_context.as_of)
            evidence_id = (
                preflight.data_quality_evidence.evidence_id
                if preflight is not None and consumes_receipt
                else None
            )
            source_artifact_ids = set(expected_context.source_artifact_ids)
            if preflight is not None and consumes_receipt:
                source_artifact_ids.update(
                    {
                        preflight.receipt_id,
                        f"receipt_sha256:{preflight.receipt_sha256}",
                    }
                )
            context = OperationsDueContext(
                as_of=expected_context.as_of,
                is_trading_day=is_trading_day,
                is_period_end=(
                    _is_period_end(
                        expected_context.as_of,
                        cadence=definition.cadence,
                        calendar=calendar,
                    )
                    if definition.cadence in {WorkflowCadence.WEEKLY, WorkflowCadence.MONTHLY}
                    else None
                ),
                daily_status=expected_context.daily_status,
                data_quality_status=(CanonicalStatus.PASS if consumes_receipt else None),
                data_quality_evidence_id=evidence_id,
                required_artifacts_ready=expected_context.required_artifacts_ready,
                source_artifact_ids=tuple(sorted(source_artifact_ids)),
                owner_gate_approved=expected_context.owner_gate_approved,
                owner_decision_id=expected_context.owner_decision_id,
            )
            resolution = resolve_operations_due(
                workflow_id=spec.workflow_id,
                policy=due_policy,
                context=context,
            )
        shadow = build_operations_shadow_plan(
            spec=spec,
            due_resolution=resolution,
            run_id=(
                f"g4a_parity:{definition.task_id}:{expected_context.as_of.isoformat()}:"
                f"{generated_at.strftime('%Y%m%dT%H%M%S%f%z')}"
            ),
            created_at=generated_at,
        )
        entries.append(
            NativePeriodicConsumerPlanEntry(
                definition=definition,
                workflow_spec=spec,
                due_resolution=resolution,
                shadow_plan=shadow,
                dispatch_mode=dispatch_mode,
            )
        )
    return tuple(entries)


def _due_policy(
    *,
    definition: NativePeriodicConsumerDefinition,
    periodic_policy: PeriodicOperationsControlPolicy,
) -> tuple[OperationsDuePolicy, OperationsDispatchMode]:
    if definition.cadence is not WorkflowCadence.DAILY:
        control = periodic_policy.cadence(definition.cadence_id)
        return control.due_policy, control.dispatch_mode
    requires_trading_day = definition.task_id == "daily_score_daily"
    return (
        OperationsDuePolicy(
            policy_id=f"g4a_{definition.task_id}_daily_trigger_v1",
            owner="system_operations",
            version="1.0.0",
            cadence=WorkflowCadence.DAILY,
            rule=DueRule.DAILY_TRIGGER,
            requires_trading_day=requires_trading_day,
            requires_completed_daily=False,
            requires_data_quality=(
                definition.data_quality_lineage_role
                is DataQualityLineageRole.VERIFIED_RECEIPT_CONSUMER
            ),
            requires_artifacts=definition.task_id == "daily_score_daily",
            requires_owner_gate=False,
        ),
        # This preserves the daily trigger's parity metadata only. The entry and
        # enclosing plan independently freeze dispatch authorization to false.
        OperationsDispatchMode.CONTROLLED_AUTOMATIC,
    )


def _workflow_spec(
    *,
    definition: NativePeriodicConsumerDefinition,
    due_policy: OperationsDuePolicy,
) -> WorkflowSpec:
    step = WorkflowStepSpec(
        step_id=definition.task_id,
        entrypoint=EntrypointRef(
            module=("ai_trading_system.platform.operations.periodic_consumer_migration"),
            callable_name="dispatch_native_periodic_consumer",
        ),
        expected_artifact_types=definition.expected_artifact_types,
        # The typed receipt is verified before the due plan and runtime lock exist.
        # RunLedger.v1 only accepts DataQualityEvidence.v1 on PASS and cannot carry
        # the stronger D0B capability, so duplicating a weaker gate here would lose
        # provenance while breaking the runtime ledger projection.
        quality_gate_required=False,
        idempotent=True,
        max_attempts=definition.max_attempts,
        production_effect=ProductionEffect.NONE,
        legacy_command=definition.resolved_command,
    )
    return WorkflowSpec(
        workflow_id=f"native_periodic_consumer_{definition.task_id}",
        owner="system_operations",
        cadence=definition.cadence,
        timezone="America/New_York",
        steps=(step,),
        due_policy_id=due_policy.policy_id,
        trading_calendar="XNYS",
    )


def _latest_trading_day(value: date, *, calendar: TradingCalendar) -> date:
    candidate = value
    for _ in range(15):
        if calendar.is_trading_day(candidate):
            return candidate
        candidate -= timedelta(days=1)
    raise PeriodicConsumerMigrationError(
        "G4A_CALENDAR_INVALID", f"no trading day within 15 days before {value.isoformat()}"
    )


def _is_period_end(value: date, *, cadence: WorkflowCadence, calendar: TradingCalendar) -> bool:
    if not calendar.is_trading_day(value):
        return False
    candidate = value + timedelta(days=1)
    for _ in range(15):
        if calendar.is_trading_day(candidate):
            if cadence is WorkflowCadence.WEEKLY:
                return candidate.isocalendar()[:2] != value.isocalendar()[:2]
            if cadence is WorkflowCadence.MONTHLY:
                return (candidate.year, candidate.month) != (value.year, value.month)
            raise PeriodicConsumerMigrationError("G4A_PERIOD_END_CADENCE_INVALID", cadence.value)
        candidate += timedelta(days=1)
    raise PeriodicConsumerMigrationError(
        "G4A_CALENDAR_INVALID", f"no trading day within 15 days after {value.isoformat()}"
    )
