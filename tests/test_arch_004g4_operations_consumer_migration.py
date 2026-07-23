from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Self, cast

import pytest
import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityExecutionContractError,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.contracts.operations import (
    OperationsDispatchMode,
    OperationsRunDecision,
    build_operations_shadow_plan,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import RunLedger
from ai_trading_system.platform.operations.periodic_consumer_migration import (
    G4A_ALLOWED_LEGACY_DIRECT_CALLERS,
    G4A_CONSUMER_IDS,
    G4A_EXPECTED_LEGACY_ADAPTER_PATHS,
    DataQualityLineageRole,
    NativeConsumerExpectedContext,
    NativeParityRunnerResult,
    NativePeriodicConsumerParityPlan,
    NativePeriodicConsumerPlanEntry,
    assess_legacy_adapter_direct_callers,
    build_native_periodic_consumer_parity_plan,
    default_native_periodic_consumer_parity_plan_path,
    rehearse_native_periodic_consumer,
    write_native_periodic_consumer_parity_plan,
)
from ai_trading_system.platform.operations.runtime_control import (
    OperationsRunControl,
    OperationsRuntimeControlPolicy,
)
from ai_trading_system.scheduled_tasks import load_scheduled_tasks_config

AS_OF_WEEK_END = date(2026, 7, 10)
NOW = datetime(2026, 7, 10, 23, 0, tzinfo=UTC)
RECEIPT_ID = "dq_execution_" + "a" * 64
RECEIPT_PATH = f"outputs/data_quality/executions/{RECEIPT_ID}/receipt.json"
RECEIPT_SHA = "b" * 64


@dataclass(frozen=True)
class FakeClock:
    value: datetime = NOW

    def now(self) -> datetime:
        return self.value


@dataclass(frozen=True)
class WeekdayCalendar:
    def is_trading_day(self, value: date) -> bool:
        return value.weekday() < 5


@dataclass(frozen=True)
class FakePreflight:
    as_of: date
    status: str
    strict_error_code: str | None = None

    @property
    def receipt_id(self) -> str:
        return RECEIPT_ID

    @property
    def receipt_path(self) -> str:
        return RECEIPT_PATH

    @property
    def receipt_sha256(self) -> str:
        return RECEIPT_SHA

    @property
    def data_quality_evidence(self) -> DataQualityEvidence:
        warning_count = 1 if self.status == "PASS_WITH_WARNINGS" else 0
        checked_at = datetime.combine(self.as_of, datetime.min.time(), tzinfo=UTC)
        return DataQualityEvidence(
            contract_id="cached_market_macro_validation",
            policy_id="DATA_QUALITY_CACHE_GATE",
            policy_version="data_quality_cache_gate.v1",
            status=self.status,
            passed=self.status != "FAIL",
            checked_at=checked_at,
            as_of=self.as_of,
            report_path="outputs/reports/data_quality.md",
            report_sha256="c" * 64,
            warning_count=warning_count,
            checked_input_count=3,
            blocking_issues=("DQ_FAILED",) if self.status == "FAIL" else (),
        )

    def assert_strict_passed(self) -> Self:
        if self.strict_error_code is not None:
            raise DataQualityExecutionContractError(self.strict_error_code, RECEIPT_ID)
        return self


class RecordingVerifier:
    def __init__(
        self,
        *,
        as_of: date,
        error_code: str | None = None,
        strict_error_code: str | None = None,
        status: str = "PASS",
    ) -> None:
        self.as_of = as_of
        self.error_code = error_code
        self.strict_error_code = strict_error_code
        self.status = status
        self.calls: list[dict[str, object]] = []

    def __call__(
        self,
        receipt_path: Path,
        *,
        expected_as_of: date,
        expected_policy_path: Path,
        expected_input_roles: tuple[str, ...],
        project_root: Path = PROJECT_ROOT,
    ) -> VerifiedDataQualityPreflight:
        self.calls.append(
            {
                "receipt_path": receipt_path,
                "expected_as_of": expected_as_of,
                "expected_policy_path": expected_policy_path,
                "expected_input_roles": expected_input_roles,
                "project_root": project_root,
            }
        )
        if self.error_code is not None:
            raise DataQualityExecutionContractError(self.error_code, str(receipt_path))
        return cast(
            VerifiedDataQualityPreflight,
            FakePreflight(
                as_of=self.as_of,
                status=self.status,
                strict_error_code=self.strict_error_code,
            ),
        )


def _data_quality_as_of(as_of: date) -> date:
    value = as_of
    while value.weekday() >= 5:
        value = date.fromordinal(value.toordinal() - 1)
    return value


def _expected_context(
    as_of: date,
    *,
    data_quality_as_of: date | None = None,
) -> NativeConsumerExpectedContext:
    return NativeConsumerExpectedContext(
        as_of=as_of,
        data_quality_as_of=data_quality_as_of or _data_quality_as_of(as_of),
        expected_policy_path=PROJECT_ROOT / "config" / "data_quality.yaml",
        expected_input_roles=("rates", "secondary_prices", "prices"),
        daily_status=CanonicalStatus.PASS,
        required_artifacts_ready=True,
        source_artifact_ids=(f"daily:{as_of.isoformat()}",),
        owner_gate_approved=True,
        owner_decision_id=f"owner:{as_of.isoformat()}",
    )


def _plan(
    tmp_path: Path,
    *,
    as_of: date = AS_OF_WEEK_END,
    verifier: RecordingVerifier | None = None,
) -> NativePeriodicConsumerParityPlan:
    context = _expected_context(as_of)
    resolved_verifier = verifier or RecordingVerifier(as_of=context.data_quality_as_of)
    return build_native_periodic_consumer_parity_plan(
        tmp_path / "receipt.json",
        expected_context=context,
        scheduled=load_scheduled_tasks_config(),
        verifier=resolved_verifier,
        calendar=WeekdayCalendar(),
        clock=FakeClock(),
        project_root=tmp_path,
    )


def _runtime_policy(*, max_run_attempts: int = 3) -> OperationsRuntimeControlPolicy:
    return OperationsRuntimeControlPolicy(
        policy_id="g4a_test_runtime_v1",
        owner="test",
        version="1.0.0",
        lock_ttl_seconds=60,
        max_run_attempts=max_run_attempts,
        resume_idempotent_steps=True,
        legacy_daily_executor_cut_in_enabled=False,
        non_daily_dispatch_enabled=False,
    )


def _control(tmp_path: Path, *, max_run_attempts: int = 3) -> OperationsRunControl:
    return OperationsRunControl(
        root=tmp_path / "runtime",
        policy=_runtime_policy(max_run_attempts=max_run_attempts),
    )


def _replace_runtime_step(
    plan: NativePeriodicConsumerParityPlan,
    *,
    task_id: str,
    idempotent: bool,
    max_attempts: int,
) -> NativePeriodicConsumerParityPlan:
    original = plan.entry(task_id)
    original_step = original.workflow_spec.steps[0]
    step = replace(
        original_step,
        idempotent=idempotent,
        max_attempts=max_attempts,
        lock_key=None if idempotent else f"g4a-test:{task_id}",
    )
    spec = replace(original.workflow_spec, steps=(step,))
    shadow = build_operations_shadow_plan(
        spec=spec,
        due_resolution=original.due_resolution,
        run_id=original.shadow_plan.run_ledger.run_id + ":runtime-variant",
        created_at=original.shadow_plan.run_ledger.created_at,
    )
    replacement = replace(original, workflow_spec=spec, shadow_plan=shadow)
    entries = tuple(
        replacement if item.definition.task_id == task_id else item for item in plan.entries
    )
    return replace(plan, entries=entries)


def test_g4a_fixed_2_2_1_matrix_preserves_identity_command_gates_and_lineage(
    tmp_path: Path,
) -> None:
    verifier = RecordingVerifier(as_of=AS_OF_WEEK_END)
    plan = _plan(tmp_path, verifier=verifier)

    assert plan.status is CanonicalStatus.PASS
    assert plan.as_of == AS_OF_WEEK_END
    assert plan.data_quality_as_of == AS_OF_WEEK_END
    assert tuple(entry.definition.task_id for entry in plan.entries) == G4A_CONSUMER_IDS
    assert [entry.definition.cadence_id for entry in plan.entries].count("daily_trading_day") == 2
    assert [entry.definition.cadence_id for entry in plan.entries].count("weekly") == 2
    assert [entry.definition.cadence_id for entry in plan.entries].count("monthly") == 1
    assert plan.execution_enabled is False
    assert plan.automatic_dispatch_enabled is False
    assert plan.production_effect.value == "none"
    assert verifier.calls == [
        {
            "receipt_path": tmp_path / "receipt.json",
            "expected_as_of": AS_OF_WEEK_END,
            "expected_policy_path": PROJECT_ROOT / "config" / "data_quality.yaml",
            "expected_input_roles": ("prices", "rates", "secondary_prices"),
            "project_root": tmp_path,
        }
    ]

    expected_commands = {
        "daily_validate_data": (
            "aits",
            "validate-data",
            "--as-of",
            "2026-07-10",
            "--execution-profile",
            "daily_default.v1",
        ),
        "daily_score_daily": ("aits", "score-daily", "--as-of", "2026-07-10"),
        "weekly_backtest": (
            "aits",
            "backtest",
            "--regime",
            "unified_primary_2021",
        ),
        "weekly_research_governance_summary_review": (
            "aits",
            "reports",
            "research-governance-summary",
            "--latest",
        ),
        "monthly_data_source_coverage_review": (
            "aits",
            "data-sources",
            "validate",
            "--as-of",
            "2026-07-10",
        ),
    }
    for entry in plan.entries:
        assert entry.definition.resolved_command == expected_commands[entry.definition.task_id]
        assert (
            entry.workflow_spec.steps[0].legacy_command
            == expected_commands[entry.definition.task_id]
        )
        assert entry.workflow_spec.steps[0].expected_artifact_types
        assert entry.workflow_spec.steps[0].production_effect.value == "none"
        assert entry.workflow_spec.steps[0].max_attempts == entry.definition.max_attempts
        assert entry.dispatch_authorized is False
        assert entry.workflow_spec.steps[0].entrypoint.module.endswith(
            ".periodic_consumer_migration"
        )
        assert entry.shadow_plan.execution_enabled is False
        assert entry.shadow_plan.run_ledger.entry(entry.definition.task_id).status in {
            CanonicalStatus.DUE,
            CanonicalStatus.NOT_DUE,
        }
        if (
            entry.due_resolution.status is CanonicalStatus.DUE
            and entry.definition.data_quality_lineage_role
            is DataQualityLineageRole.VERIFIED_RECEIPT_CONSUMER
        ):
            assert RECEIPT_ID in entry.due_resolution.source_artifact_ids
            assert f"receipt_sha256:{RECEIPT_SHA}" in entry.due_resolution.source_artifact_ids

    producer = plan.entry("daily_validate_data")
    assert producer.due_resolution.status is CanonicalStatus.DUE
    assert (
        producer.definition.data_quality_lineage_role
        is DataQualityLineageRole.RECEIPT_PRODUCER_OBSERVATION
    )
    assert producer.due_resolution.data_quality_evidence_id is None
    assert RECEIPT_ID not in producer.due_resolution.source_artifact_ids
    assert producer.dispatch_mode is OperationsDispatchMode.CONTROLLED_AUTOMATIC
    assert (
        plan.entry("daily_score_daily").dispatch_mode is OperationsDispatchMode.CONTROLLED_AUTOMATIC
    )
    assert plan.entry("weekly_backtest").dispatch_mode is OperationsDispatchMode.MANUAL_ONLY
    assert (
        plan.entry("daily_score_daily").definition.legacy_production_effect == "local_report_write"
    )
    assert plan.entry("daily_score_daily").due_resolution.status is CanonicalStatus.DUE
    assert plan.entry("weekly_backtest").due_resolution.status is CanonicalStatus.DUE
    assert (
        plan.entry("weekly_research_governance_summary_review").due_resolution.status
        is CanonicalStatus.DUE
    )
    assert (
        plan.entry("monthly_data_source_coverage_review").due_resolution.status
        is CanonicalStatus.NOT_DUE
    )


@pytest.mark.parametrize(
    ("as_of", "expected_due"),
    [
        (date(2026, 7, 9), {"daily_validate_data", "daily_score_daily"}),
        (date(2026, 7, 11), {"daily_validate_data"}),
        (
            date(2026, 7, 31),
            {
                "daily_validate_data",
                "daily_score_daily",
                "weekly_backtest",
                "weekly_research_governance_summary_review",
                "monthly_data_source_coverage_review",
            },
        ),
    ],
)
def test_fake_calendar_parity_covers_trading_closed_due_and_not_due(
    tmp_path: Path,
    as_of: date,
    expected_due: set[str],
) -> None:
    verifier = RecordingVerifier(as_of=_data_quality_as_of(as_of))
    plan = _plan(tmp_path, as_of=as_of, verifier=verifier)
    due = {
        entry.definition.task_id
        for entry in plan.entries
        if entry.due_resolution.status is CanonicalStatus.DUE
    }

    assert due == expected_due
    assert plan.as_of == as_of
    assert plan.data_quality_as_of == _data_quality_as_of(as_of)
    assert verifier.calls[0]["expected_as_of"] == _data_quality_as_of(as_of)
    if as_of == date(2026, 7, 11):
        validate_command = plan.entry("daily_validate_data").definition.resolved_command
        assert validate_command[3] == "2026-07-10"
        assert validate_command[-2:] == ("--execution-profile", "daily_default.v1")
        assert plan.entry("daily_score_daily").due_resolution.reason_codes == ("NOT_A_TRADING_DAY",)


def test_trigger_and_data_quality_dates_are_ordered_and_bound_to_validate_command(
    tmp_path: Path,
) -> None:
    with pytest.raises(RuntimeError, match="G4A_DQ_AS_OF_AFTER_TRIGGER"):
        _expected_context(
            AS_OF_WEEK_END,
            data_quality_as_of=date(2026, 7, 11),
        )

    stale_context = _expected_context(
        date(2026, 7, 11),
        data_quality_as_of=date(2026, 7, 9),
    )
    with pytest.raises(RuntimeError, match="G4A_DQ_AS_OF_COMMAND_MISMATCH"):
        build_native_periodic_consumer_parity_plan(
            tmp_path / "receipt.json",
            expected_context=stale_context,
            scheduled=load_scheduled_tasks_config(),
            verifier=RecordingVerifier(as_of=date(2026, 7, 9)),
            calendar=WeekdayCalendar(),
            clock=FakeClock(),
            project_root=tmp_path,
        )


def test_parity_plan_materializes_deterministic_non_executing_sidecar(
    tmp_path: Path,
) -> None:
    trigger_as_of = date(2026, 7, 11)
    plan = _plan(tmp_path, as_of=trigger_as_of)
    path = default_native_periodic_consumer_parity_plan_path(
        trigger_as_of,
        tmp_path / "plans",
    )

    written = write_native_periodic_consumer_parity_plan(plan, path)
    first_bytes = written.read_bytes()
    written_again = write_native_periodic_consumer_parity_plan(plan, path)
    payload = json.loads(written_again.read_text(encoding="utf-8"))

    assert written == path
    assert written_again.read_bytes() == first_bytes
    assert payload == plan.to_dict()
    assert payload["schema_version"] == "native_periodic_consumer_parity_plan.v1"
    assert payload["as_of"] == "2026-07-11"
    assert payload["data_quality_as_of"] == "2026-07-10"
    assert payload["receipt_id"] == RECEIPT_ID
    assert payload["receipt_path"] == RECEIPT_PATH
    assert payload["execution_enabled"] is False
    assert payload["automatic_dispatch_enabled"] is False
    assert payload["command_execution_enabled"] is False
    assert payload["production_effect"] == "none"
    producer, score, *_ = payload["entries"]
    assert producer["task_id"] == "daily_validate_data"
    assert producer["data_quality_lineage_role"] == "receipt_producer_observation"
    assert producer["due_resolution"]["data_quality_evidence_id"] is None
    assert RECEIPT_ID not in producer["due_resolution"]["source_artifact_ids"]
    assert score["task_id"] == "daily_score_daily"
    assert score["data_quality_lineage_role"] == "verified_receipt_consumer"
    assert score["due_resolution"]["data_quality_evidence_id"] is not None
    assert RECEIPT_ID in score["due_resolution"]["source_artifact_ids"]
    assert all(entry["dispatch_authorized"] is False for entry in payload["entries"])


@pytest.mark.parametrize(
    ("verifier_error", "strict_error", "status", "expected_code"),
    [
        ("DQ_RECEIPT_MISSING", None, "PASS", "DQ_RECEIPT_MISSING"),
        ("DQ_RECEIPT_ID_MISMATCH", None, "PASS", "DQ_RECEIPT_ID_MISMATCH"),
        ("DQ_AS_OF_MISMATCH", None, "PASS", "DQ_AS_OF_MISMATCH"),
        (None, "DQ_EXECUTION_FAILED", "FAIL", "DQ_EXECUTION_FAILED"),
        (
            None,
            "DQ_WARNING_NOT_ALLOWED",
            "PASS_WITH_WARNINGS",
            "DQ_WARNING_NOT_ALLOWED",
        ),
    ],
)
def test_dq_fail_closed_precedes_runtime_runner_and_downstream_artifacts(
    tmp_path: Path,
    verifier_error: str | None,
    strict_error: str | None,
    status: str,
    expected_code: str,
) -> None:
    verifier = RecordingVerifier(
        as_of=AS_OF_WEEK_END,
        error_code=verifier_error,
        strict_error_code=strict_error,
        status=status,
    )
    plan = _plan(tmp_path, verifier=verifier)
    runner_calls: list[str] = []

    def runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        runner_calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(passed=True, retryable=False)

    downstream_ids = G4A_CONSUMER_IDS[1:]
    results = tuple(
        rehearse_native_periodic_consumer(
            plan,
            task_id=task_id,
            control=_control(tmp_path / task_id),
            runner=runner,
            run_id=f"blocked:{task_id}",
            clock=FakeClock(),
        )
        for task_id in downstream_ids
    )

    assert plan.status is CanonicalStatus.BLOCKED
    assert plan.blocker_codes == (expected_code,)
    assert plan.entry("daily_validate_data").due_resolution.status is CanonicalStatus.DUE
    assert {plan.entry(task_id).due_resolution.status for task_id in downstream_ids} == {
        CanonicalStatus.BLOCKED
    }
    assert all(item.status is CanonicalStatus.BLOCKED for item in results)
    assert all(item.runner_called is False for item in results)
    assert all(item.artifact_refs == () for item in results)
    assert runner_calls == []
    assert not any((tmp_path / task_id / "runtime").exists() for task_id in downstream_ids)


def test_default_public_verifier_missing_receipt_keeps_only_producer_due(
    tmp_path: Path,
) -> None:
    context = replace(
        _expected_context(AS_OF_WEEK_END),
        expected_policy_path=Path("config/data_quality.yaml"),
    )

    plan = build_native_periodic_consumer_parity_plan(
        tmp_path / "outputs" / "data_quality" / "executions" / "missing" / "receipt.json",
        expected_context=context,
        scheduled=load_scheduled_tasks_config(),
        calendar=WeekdayCalendar(),
        clock=FakeClock(),
        project_root=tmp_path,
    )

    assert plan.status is CanonicalStatus.BLOCKED
    assert plan.blocker_codes == ("DQ_RECEIPT_MISSING",)
    assert plan.entry("daily_validate_data").due_resolution.status is CanonicalStatus.DUE
    assert {plan.entry(task_id).due_resolution.status for task_id in G4A_CONSUMER_IDS[1:]} == {
        CanonicalStatus.BLOCKED
    }


def test_runtime_rehearsal_pass_duplicate_and_typed_ledger_without_command_execution(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    control = _control(tmp_path)
    calls: list[tuple[str, tuple[str, ...]]] = []

    def runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        calls.append((entry.definition.task_id, entry.definition.resolved_command))
        return NativeParityRunnerResult(
            passed=True,
            retryable=False,
            artifact_refs=("artifact:weekly_backtest:parity",),
        )

    first = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=control,
        runner=runner,
        run_id="g4a:first",
        clock=FakeClock(),
    )
    duplicate = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=control,
        runner=runner,
        run_id="g4a:duplicate",
        clock=FakeClock(),
    )

    assert first.status is CanonicalStatus.PASS
    assert first.run_decision is OperationsRunDecision.START_NEW
    assert first.artifact_refs == ("artifact:weekly_backtest:parity",)
    assert duplicate.status is CanonicalStatus.PASS
    assert duplicate.run_decision is OperationsRunDecision.ALREADY_COMPLETE
    assert duplicate.runner_called is False
    assert len(calls) == 1
    ledger_path = next((tmp_path / "runtime" / "states").glob("*.run_ledger.json"))
    ledger = RunLedger.from_dict(json.loads(ledger_path.read_text(encoding="utf-8")))
    assert ledger.workflow_id == plan.entry("weekly_backtest").workflow_spec.workflow_id
    assert ledger.entry("weekly_backtest").status is CanonicalStatus.PASS


def test_failed_parity_observation_cannot_publish_artifact_refs() -> None:
    with pytest.raises(
        RuntimeError,
        match="failed parity run cannot publish downstream artifact refs",
    ):
        NativeParityRunnerResult(
            passed=False,
            retryable=True,
            artifact_refs=("artifact:must-not-escape",),
            blocker_code="G4A_TRANSIENT_PARITY_FAILURE",
        )


def test_runtime_rehearsal_active_lock_and_retry_exhaustion_never_recall_runner(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    entry = plan.entry("weekly_backtest")
    locked_control = _control(tmp_path / "locked")
    acquisition = locked_control.acquire(
        spec=entry.workflow_spec,
        as_of=plan.as_of,
        run_id="lock-owner",
        now=NOW,
    )
    assert acquisition.lease is not None
    lock_calls: list[str] = []

    def locked_runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        lock_calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(passed=True, retryable=False)

    locked = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=locked_control,
        runner=locked_runner,
        run_id="lock-contender",
        clock=FakeClock(),
    )
    acquisition.lease.release()

    retry_control = _control(tmp_path / "retry")
    retry_calls: list[str] = []

    def retrying_runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        retry_calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(
            passed=False,
            retryable=True,
            blocker_code="G4A_PARITY_OBSERVATION_FAILED",
        )

    failed = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=retry_control,
        runner=retrying_runner,
        run_id="retry:first",
        clock=FakeClock(),
    )
    exhausted = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=retry_control,
        runner=retrying_runner,
        run_id="retry:second",
        clock=FakeClock(),
    )

    assert locked.status is CanonicalStatus.BLOCKED
    assert locked.run_decision is OperationsRunDecision.BLOCKED_CONCURRENT
    assert lock_calls == []
    assert failed.status is CanonicalStatus.FAILED
    assert exhausted.status is CanonicalStatus.BLOCKED
    assert exhausted.run_decision is OperationsRunDecision.BLOCKED_RETRY_EXHAUSTED
    assert exhausted.blocker_codes == ("STEP_ATTEMPT_BUDGET_EXHAUSTED:weekly_backtest",)
    assert retry_calls == ["weekly_backtest"]


def test_runtime_rehearsal_safe_resume_and_unsafe_resume_use_f1_control(
    tmp_path: Path,
) -> None:
    base = _plan(tmp_path)
    safe_plan = _replace_runtime_step(
        base,
        task_id="weekly_backtest",
        idempotent=True,
        max_attempts=2,
    )
    safe_control = _control(tmp_path / "safe")
    observations = iter(
        (
            NativeParityRunnerResult(
                passed=False,
                retryable=True,
                blocker_code="G4A_TRANSIENT_PARITY_FAILURE",
            ),
            NativeParityRunnerResult(
                passed=True,
                retryable=False,
                artifact_refs=("artifact:safe-resume",),
            ),
        )
    )
    first = rehearse_native_periodic_consumer(
        safe_plan,
        task_id="weekly_backtest",
        control=safe_control,
        runner=lambda _: next(observations),
        run_id="safe:first",
        clock=FakeClock(),
    )
    resumed = rehearse_native_periodic_consumer(
        safe_plan,
        task_id="weekly_backtest",
        control=safe_control,
        runner=lambda _: next(observations),
        run_id="safe:resume",
        clock=FakeClock(),
    )

    completed_control = _control(tmp_path / "completed-safe")
    completed_entry = safe_plan.entry("weekly_backtest")
    completed_acquisition = completed_control.acquire(
        spec=completed_entry.workflow_spec,
        as_of=safe_plan.as_of,
        run_id="completed-safe:first",
        now=NOW,
    )
    assert completed_acquisition.lease is not None
    completed_acquisition.lease.start_step("weekly_backtest", at=NOW)
    completed_acquisition.lease.pass_step("weekly_backtest", at=NOW)
    completed_acquisition.lease.release()
    completed_resume_calls: list[str] = []

    def completed_resume_runner(
        entry: NativePeriodicConsumerPlanEntry,
    ) -> NativeParityRunnerResult:
        completed_resume_calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(passed=True, retryable=False)

    completed_resume = rehearse_native_periodic_consumer(
        safe_plan,
        task_id="weekly_backtest",
        control=completed_control,
        runner=completed_resume_runner,
        run_id="completed-safe:resume",
        clock=FakeClock(),
    )

    unsafe_plan = _replace_runtime_step(
        base,
        task_id="weekly_backtest",
        idempotent=False,
        max_attempts=1,
    )
    unsafe_entry = unsafe_plan.entry("weekly_backtest")
    unsafe_control = _control(tmp_path / "unsafe")
    acquisition = unsafe_control.acquire(
        spec=unsafe_entry.workflow_spec,
        as_of=unsafe_plan.as_of,
        run_id="unsafe:first",
        now=NOW,
    )
    assert acquisition.lease is not None
    acquisition.lease.start_step("weekly_backtest", at=NOW)
    acquisition.lease.pass_step("weekly_backtest", at=NOW)
    acquisition.lease.release()
    unsafe_calls: list[str] = []

    def unsafe_runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        unsafe_calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(passed=True, retryable=False)

    blocked = rehearse_native_periodic_consumer(
        unsafe_plan,
        task_id="weekly_backtest",
        control=unsafe_control,
        runner=unsafe_runner,
        run_id="unsafe:resume",
        clock=FakeClock(),
    )

    assert first.status is CanonicalStatus.FAILED
    assert resumed.status is CanonicalStatus.PASS
    assert resumed.run_decision is OperationsRunDecision.RESUME
    assert resumed.artifact_refs == ("artifact:safe-resume",)
    assert completed_resume.status is CanonicalStatus.PASS
    assert completed_resume.run_decision is OperationsRunDecision.RESUME
    assert completed_resume.runner_called is False
    assert completed_resume_calls == []
    assert blocked.status is CanonicalStatus.BLOCKED
    assert blocked.run_decision is OperationsRunDecision.BLOCKED_UNSAFE_RESUME
    assert blocked.blocker_codes == ("NON_IDEMPOTENT_RESUME:weekly_backtest",)
    assert unsafe_calls == []


def test_not_due_consumer_does_not_create_runtime_state_or_call_runner(tmp_path: Path) -> None:
    plan = _plan(tmp_path, as_of=date(2026, 7, 9))
    calls: list[str] = []

    def runner(entry: NativePeriodicConsumerPlanEntry) -> NativeParityRunnerResult:
        calls.append(entry.definition.task_id)
        return NativeParityRunnerResult(passed=True, retryable=False)

    result = rehearse_native_periodic_consumer(
        plan,
        task_id="weekly_backtest",
        control=_control(tmp_path),
        runner=runner,
        run_id="not-due",
        clock=FakeClock(),
    )

    assert result.status is CanonicalStatus.SKIPPED
    assert result.blocker_codes == ("NOT_PERIOD_END",)
    assert calls == []
    assert not (tmp_path / "runtime").exists()


def test_legacy_direct_caller_ratchet_has_zero_new_callers() -> None:
    assessment = assess_legacy_adapter_direct_callers()

    assert assessment.status is CanonicalStatus.PASS
    assert assessment.unexpected_callers == ()
    assert assessment.scan_blocker_codes == ()
    assert assessment.observed_callers == G4A_ALLOWED_LEGACY_DIRECT_CALLERS
    assert G4A_EXPECTED_LEGACY_ADAPTER_PATHS == (
        "src/ai_trading_system/legacy/periodic_operations_adapter.py",
        "src/ai_trading_system/legacy/scheduled_tasks_adapter.py",
    )


@pytest.mark.parametrize(
    "source_text",
    (
        "import ai_trading_system.legacy.periodic_operations_adapter as legacy_periodic\n",
        (
            "from ai_trading_system.legacy.periodic_operations_adapter "
            "import build_periodic_operations_plan\n"
        ),
        (
            "import importlib\n"
            "importlib.import_module("
            "'ai_trading_system.legacy.periodic_operations_adapter')\n"
        ),
        "__import__('ai_trading_system.legacy.periodic_operations_adapter')\n",
    ),
)
def test_legacy_direct_caller_ratchet_blocks_import_statement_bypass(
    tmp_path: Path,
    source_text: str,
) -> None:
    source = tmp_path / "src" / "new_caller.py"
    source.parent.mkdir(parents=True)
    source.write_text(source_text, encoding="utf-8")
    (tmp_path / "tests").mkdir()

    assessment = assess_legacy_adapter_direct_callers(tmp_path)

    assert assessment.status is CanonicalStatus.BLOCKED
    assert assessment.unexpected_callers == (
        "ai_trading_system.legacy.periodic_operations_adapter:src/new_caller.py",
    )
    assert assessment.scan_blocker_codes == ()


def test_legacy_direct_caller_ratchet_fails_closed_on_unparseable_source(
    tmp_path: Path,
) -> None:
    source = tmp_path / "src" / "broken.py"
    source.parent.mkdir(parents=True)
    source.write_text("def broken(:\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()

    assessment = assess_legacy_adapter_direct_callers(tmp_path)

    assert assessment.status is CanonicalStatus.BLOCKED
    assert assessment.unexpected_callers == ()
    assert assessment.scan_blocker_codes == ("LEGACY_CALLER_SCAN_FAILED:src/broken.py",)


def test_architecture_fragments_freeze_non_executing_typed_lineage() -> None:
    artifact_path = (
        PROJECT_ROOT
        / "config"
        / "architecture"
        / "fragments"
        / "artifacts"
        / "arch_004g4_periodic_consumers.yaml"
    )
    flow_path = (
        PROJECT_ROOT
        / "config"
        / "architecture"
        / "fragments"
        / "flows"
        / "arch_004g4_periodic_consumers.yaml"
    )
    artifact = yaml.safe_load(artifact_path.read_text(encoding="utf-8"))
    flow = yaml.safe_load(flow_path.read_text(encoding="utf-8"))

    assert artifact["execution_mode"] == "non_executing_parity"
    assert artifact["automatic_dispatch_enabled"] is False
    assert artifact["dispatch_authorized"] is False
    assert artifact["dispatch_mode_semantics"]["daily_value_role"] == (
        "legacy_and_future_parity_metadata_only"
    )
    assert artifact["production_effect"] == "none"
    assert artifact["lineage"]["strict_pass_required"] is True
    assert artifact["lineage"]["warning_allowed"] is False
    assert artifact["lineage"]["date_binding"] == {
        "operations_trigger": "as_of",
        "receipt_and_validate_command": "data_quality_as_of",
        "invariant": "data_quality_as_of_lte_as_of_and_matches_latest_trading_day",
    }
    assert artifact["lineage"]["producer_boundary"] == {
        "task_id": "daily_validate_data",
        "receipt_required_as_input": False,
        "requires_data_quality_due_gate": False,
    }
    assert artifact["materialization"] == {
        "default_root": "outputs/run_control/periodic/plans",
        "filename": "native_periodic_consumer_parity_YYYY-MM-DD.json",
        "writer": "ai_trading_system.platform.artifacts.write_json_atomic",
        "deterministic_to_dict": True,
        "sidecar_only": True,
    }
    assert tuple(flow["consumer_matrix"]["identities"]) == G4A_CONSUMER_IDS
    assert flow["automatic_non_daily_dispatch"] is False
    assert flow["dispatch_authorized"] is False
    assert flow["daily_controlled_automatic_is_metadata_only"] is True
    assert flow["lineage"]["operations_trigger_date"] == "as_of"
    assert flow["lineage"]["receipt_validation_date"] == "data_quality_as_of"
    assert flow["real_command_execution"] is False
    assert flow["blocked_invariants"] == {
        "applies_to": "verified_receipt_consumers",
        "runner_calls": 0,
        "downstream_artifacts": 0,
    }
