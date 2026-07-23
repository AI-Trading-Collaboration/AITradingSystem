from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Self, cast

import pytest

import ai_trading_system.cli_commands.ops as ops_cli
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.data_quality_execution import (
    DataQualityExecutionReceipt,
    VerifiedDataQualityPreflight,
)
from ai_trading_system.data.quality_execution import DataQualityExecutionError
from ai_trading_system.data.quality_execution_discovery import (
    DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
    DataQualityExecutionDiscoveryPointer,
    DiscoveredDataQualityExecution,
    default_data_quality_execution_discovery_path,
)
from ai_trading_system.ops_daily import (
    DailyOpsRunReport,
    DailyOpsStepResult,
    build_daily_ops_plan,
    resolve_daily_ops_default_as_of,
)
from ai_trading_system.platform.operations.periodic_consumer_migration import (
    default_native_periodic_consumer_parity_plan_path,
)
from ai_trading_system.trading_calendar import resolve_default_data_quality_as_of

RECEIPT_ID = "dq_execution_" + "a" * 64
RECEIPT_PATH = f"outputs/data_quality/executions/{RECEIPT_ID}/receipt.json"
RECEIPT_SHA = "b" * 64
RECEIPT_SIZE = 4096


@dataclass(frozen=True)
class _FakeReceipt:
    receipt_id: str
    as_of: date
    started_at: datetime
    ended_at: datetime


@dataclass(frozen=True)
class _FakePreflight:
    receipt: _FakeReceipt
    receipt_path: str = RECEIPT_PATH
    receipt_sha256: str = RECEIPT_SHA
    receipt_size_bytes: int = RECEIPT_SIZE
    strict_error_code: str | None = None

    @property
    def receipt_id(self) -> str:
        return self.receipt.receipt_id

    @property
    def as_of(self) -> date:
        return self.receipt.as_of

    @property
    def data_quality_evidence(self) -> DataQualityEvidence:
        return DataQualityEvidence(
            contract_id="cached_market_macro_validation",
            policy_id="DATA_QUALITY_CACHE_GATE",
            policy_version="data_quality_cache_gate.v1",
            status="PASS",
            passed=True,
            checked_at=self.receipt.ended_at,
            as_of=self.receipt.as_of,
            report_path="outputs/reports/data_quality.md",
            report_sha256="c" * 64,
            checked_input_count=3,
        )

    def assert_strict_passed(self) -> Self:
        if self.strict_error_code is not None:
            raise DataQualityExecutionError(self.strict_error_code, self.receipt_id)
        return self


class _RecordingVerifier:
    def __init__(self, preflight: _FakePreflight) -> None:
        self.preflight = preflight
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
        return cast(VerifiedDataQualityPreflight, self.preflight)


class _RecordingLoader:
    def __init__(
        self,
        discovered: DiscoveredDataQualityExecution | None = None,
        *,
        error: BaseException | None = None,
    ) -> None:
        self.discovered = discovered
        self.error = error
        self.calls: list[tuple[date, Path]] = []

    def __call__(
        self,
        as_of: date,
        *,
        project_root: Path = PROJECT_ROOT,
    ) -> DiscoveredDataQualityExecution:
        self.calls.append((as_of, project_root))
        if self.error is not None:
            raise self.error
        assert self.discovered is not None
        return self.discovered


def _run_report(
    tmp_path: Path,
    *,
    as_of: date,
    run_status: str = "PASS",
    validate_status: str = "PASS",
    current_interval: bool = True,
    include_validate_result: bool = True,
) -> tuple[DailyOpsRunReport, datetime, datetime]:
    plan = build_daily_ops_plan(
        as_of=as_of,
        project_root=tmp_path,
        include_download_data=False,
        skip_risk_event_openai_precheck=True,
    )
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    run_started_at = datetime.combine(as_of, datetime.min.time(), tzinfo=UTC)
    validate_started_at = run_started_at + timedelta(minutes=10)
    validate_ended_at = run_started_at + timedelta(minutes=20)
    results: tuple[DailyOpsStepResult, ...] = ()
    if include_validate_result:
        results = (
            DailyOpsStepResult(
                step_id=validate_step.step_id,
                title=validate_step.title,
                command=validate_step.command,
                status=validate_status,
                return_code=0 if validate_status == "PASS" else 1,
                started_at=validate_started_at if current_interval else None,
                ended_at=validate_ended_at if current_interval else None,
                duration_seconds=600.0 if current_interval else None,
                produced_paths=validate_step.produced_paths,
                blocks_downstream=True,
            ),
        )
    report = DailyOpsRunReport(
        plan=plan,
        started_at=run_started_at,
        finished_at=run_started_at + timedelta(hours=1),
        status=run_status,
        step_results=results,
    )
    return report, validate_started_at, validate_ended_at


def _discovery_and_preflight(
    tmp_path: Path,
    *,
    data_quality_as_of: date,
    validate_started_at: datetime,
) -> tuple[DiscoveredDataQualityExecution, _FakePreflight]:
    receipt = _FakeReceipt(
        receipt_id=RECEIPT_ID,
        as_of=data_quality_as_of,
        started_at=validate_started_at + timedelta(minutes=1),
        ended_at=validate_started_at + timedelta(minutes=2),
    )
    pointer = DataQualityExecutionDiscoveryPointer(
        profile_id=DEFAULT_DATA_QUALITY_EXECUTION_PROFILE_ID,
        as_of=data_quality_as_of,
        published_at=validate_started_at + timedelta(minutes=3),
        receipt_id=RECEIPT_ID,
        receipt_path=RECEIPT_PATH,
        receipt_sha256=RECEIPT_SHA,
        receipt_size_bytes=RECEIPT_SIZE,
    )
    receipt_path = tmp_path / Path(RECEIPT_PATH)
    discovered = DiscoveredDataQualityExecution(
        pointer_path=default_data_quality_execution_discovery_path(
            data_quality_as_of,
            project_root=tmp_path,
        ),
        pointer=pointer,
        receipt_path=receipt_path,
        receipt=cast(DataQualityExecutionReceipt, receipt),
    )
    return discovered, _FakePreflight(receipt=receipt)


def _load_payload(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def _entry(payload: dict[str, object], task_id: str) -> dict[str, object]:
    entries = cast(list[dict[str, object]], payload["entries"])
    return next(item for item in entries if item["task_id"] == task_id)


def _assert_blocked_outputs(
    *,
    output_root: Path,
    legacy_path: Path,
    as_of: date,
    blocker_code: str,
) -> None:
    native_path = default_native_periodic_consumer_parity_plan_path(as_of, output_root)
    assert legacy_path.is_file()
    assert native_path.is_file()
    native = _load_payload(native_path)
    assert native["status"] == "BLOCKED"
    assert native["blocker_codes"] == [blocker_code]
    assert native["receipt_id"] is None
    assert native["automatic_dispatch_enabled"] is False
    assert all(
        entry["dispatch_authorized"] is False
        for entry in cast(list[dict[str, object]], native["entries"])
    )
    legacy = _load_payload(legacy_path)
    weekly = _entry(legacy, "weekly_backtest")
    resolution = cast(
        dict[str, object],
        cast(dict[str, object], weekly["shadow_plan"])["due_resolution"],
    )
    assert resolution["status"] == "BLOCKED"
    assert resolution["data_quality_evidence_id"] is None
    assert resolution["source_artifact_ids"] == []
    assert legacy["automatic_command_dispatch_enabled"] is False
    assert all(
        entry["command_executed"] is False
        for entry in cast(list[dict[str, object]], legacy["entries"])
    )


def test_daily_plan_uses_shared_as_of_policy_and_publishes_discovery_path(
    tmp_path: Path,
) -> None:
    observed_at = datetime(2026, 7, 10, 22, 30, tzinfo=UTC)

    assert resolve_daily_ops_default_as_of(observed_at) == resolve_default_data_quality_as_of(
        observed_at
    )
    plan = build_daily_ops_plan(
        as_of=date(2026, 7, 11),
        project_root=tmp_path,
        include_download_data=False,
        skip_risk_event_openai_precheck=True,
    )
    validate_step = next(step for step in plan.steps if step.step_id == "validate_data")
    assert validate_step.command == (
        "aits",
        "validate-data",
        "--as-of",
        "2026-07-10",
        "--execution-profile",
        "daily_default.v1",
    )
    assert validate_step.produced_paths[1] == default_data_quality_execution_discovery_path(
        date(2026, 7, 10),
        project_root=tmp_path,
    )


@pytest.mark.parametrize(
    ("operations_as_of", "data_quality_as_of", "run_status"),
    [
        (date(2026, 7, 10), date(2026, 7, 10), "PASS"),
        (date(2026, 7, 11), date(2026, 7, 10), "PASS_WITH_SKIPS"),
    ],
)
def test_daily_run_writes_bound_native_and_legacy_plans_without_dispatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operations_as_of: date,
    data_quality_as_of: date,
    run_status: str,
) -> None:
    report, validate_started_at, _ = _run_report(
        tmp_path,
        as_of=operations_as_of,
        run_status=run_status,
    )
    discovered, preflight = _discovery_and_preflight(
        tmp_path,
        data_quality_as_of=data_quality_as_of,
        validate_started_at=validate_started_at,
    )
    loader = _RecordingLoader(discovered)
    verifier = _RecordingVerifier(preflight)
    output_root = tmp_path / "metadata"

    def forbidden_dispatch(*args: object, **kwargs: object) -> None:
        pytest.fail(f"dispatch must not run: args={args!r} kwargs={kwargs!r}")

    monkeypatch.setattr(ops_cli, "dispatch_periodic_operations_plan", forbidden_dispatch)
    legacy_path = ops_cli._write_periodic_plan_from_daily_run(
        run_report=report,
        output_root=output_root,
        project_root=tmp_path,
        discovery_loader=loader,
        receipt_verifier=verifier,
        generated_at=report.finished_at + timedelta(minutes=1),
    )

    native_path = default_native_periodic_consumer_parity_plan_path(
        operations_as_of,
        output_root,
    )
    native = _load_payload(native_path)
    assert native["status"] == "PASS"
    assert native["as_of"] == operations_as_of.isoformat()
    assert native["data_quality_as_of"] == data_quality_as_of.isoformat()
    assert native["receipt_id"] == RECEIPT_ID
    assert native["receipt_path"] == RECEIPT_PATH
    assert native["automatic_dispatch_enabled"] is False
    native_score = _entry(native, "daily_score_daily")
    native_resolution = cast(dict[str, object], native_score["due_resolution"])
    evidence_id = preflight.data_quality_evidence.evidence_id
    source_ids = [RECEIPT_ID, f"receipt_sha256:{RECEIPT_SHA}"]
    assert native_resolution["data_quality_evidence_id"] == evidence_id
    assert native_resolution["source_artifact_ids"] == source_ids

    legacy = _load_payload(legacy_path)
    assert legacy["as_of"] == operations_as_of.isoformat()
    weekly = _entry(legacy, "weekly_backtest")
    legacy_resolution = cast(
        dict[str, object],
        cast(dict[str, object], weekly["shadow_plan"])["due_resolution"],
    )
    assert legacy_resolution["data_quality_evidence_id"] == evidence_id
    assert legacy_resolution["source_artifact_ids"] == source_ids
    assert loader.calls == [(data_quality_as_of, tmp_path)]
    assert verifier.calls == [
        {
            "receipt_path": discovered.receipt_path,
            "expected_as_of": data_quality_as_of,
            "expected_policy_path": tmp_path / "config" / "data_quality.yaml",
            "expected_input_roles": ("prices", "rates", "secondary_prices"),
            "project_root": tmp_path,
        }
    ]


def test_missing_discovery_writes_fail_closed_native_and_legacy_plans(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 10)
    report, validate_started_at, _ = _run_report(tmp_path, as_of=as_of)
    _, preflight = _discovery_and_preflight(
        tmp_path,
        data_quality_as_of=as_of,
        validate_started_at=validate_started_at,
    )
    loader = _RecordingLoader(error=DataQualityExecutionError("DQ_RECEIPT_MISSING", "current.json"))
    verifier = _RecordingVerifier(preflight)
    output_root = tmp_path / "metadata"

    legacy_path = ops_cli._write_periodic_plan_from_daily_run(
        run_report=report,
        output_root=output_root,
        project_root=tmp_path,
        discovery_loader=loader,
        receipt_verifier=verifier,
        generated_at=report.finished_at + timedelta(minutes=1),
    )

    _assert_blocked_outputs(
        output_root=output_root,
        legacy_path=legacy_path,
        as_of=as_of,
        blocker_code="DQ_RECEIPT_MISSING",
    )
    assert verifier.calls == []


@pytest.mark.parametrize(
    ("failure", "blocker_code"),
    [
        ("preflight_hash", "DQ_RECEIPT_ID_MISMATCH"),
        ("late_pointer", "DQ_EXECUTION_CHRONOLOGY_INVALID"),
        ("warning", "DQ_WARNING_NOT_ALLOWED"),
    ],
)
def test_pointer_preflight_or_warning_failure_never_projects_legacy_pass(
    tmp_path: Path,
    failure: str,
    blocker_code: str,
) -> None:
    as_of = date(2026, 7, 10)
    report, validate_started_at, validate_ended_at = _run_report(tmp_path, as_of=as_of)
    discovered, preflight = _discovery_and_preflight(
        tmp_path,
        data_quality_as_of=as_of,
        validate_started_at=validate_started_at,
    )
    if failure == "preflight_hash":
        preflight = replace(preflight, receipt_sha256="d" * 64)
    elif failure == "late_pointer":
        discovered = replace(
            discovered,
            pointer=replace(
                discovered.pointer,
                published_at=validate_ended_at + timedelta(seconds=1),
            ),
        )
    else:
        preflight = replace(preflight, strict_error_code=blocker_code)
    output_root = tmp_path / "metadata"

    legacy_path = ops_cli._write_periodic_plan_from_daily_run(
        run_report=report,
        output_root=output_root,
        project_root=tmp_path,
        discovery_loader=_RecordingLoader(discovered),
        receipt_verifier=_RecordingVerifier(preflight),
        generated_at=report.finished_at + timedelta(minutes=1),
    )

    _assert_blocked_outputs(
        output_root=output_root,
        legacy_path=legacy_path,
        as_of=as_of,
        blocker_code=blocker_code,
    )


def test_failed_validate_step_writes_both_blocked_plans_without_discovery(
    tmp_path: Path,
) -> None:
    as_of = date(2026, 7, 10)
    report, _, _ = _run_report(tmp_path, as_of=as_of, validate_status="FAIL")
    loader = _RecordingLoader(
        error=AssertionError("discovery must not run for a failed validate step")
    )
    output_root = tmp_path / "metadata"

    legacy_path = ops_cli._write_periodic_plan_from_daily_run(
        run_report=report,
        output_root=output_root,
        project_root=tmp_path,
        discovery_loader=loader,
        generated_at=report.finished_at + timedelta(minutes=1),
    )

    _assert_blocked_outputs(
        output_root=output_root,
        legacy_path=legacy_path,
        as_of=as_of,
        blocker_code="DQ_EXECUTION_FAILED",
    )
    assert loader.calls == []


@pytest.mark.parametrize(
    ("run_status", "include_result", "current_interval"),
    [
        ("PASS", True, False),
        ("RUN_CONTROL_ALREADY_COMPLETE", False, False),
    ],
)
def test_missing_current_validate_interval_is_fail_closed(
    tmp_path: Path,
    run_status: str,
    include_result: bool,
    current_interval: bool,
) -> None:
    as_of = date(2026, 7, 10)
    report, _, _ = _run_report(
        tmp_path,
        as_of=as_of,
        run_status=run_status,
        current_interval=current_interval,
        include_validate_result=include_result,
    )
    loader = _RecordingLoader(
        error=AssertionError("discovery must not run without a current validate interval")
    )
    output_root = tmp_path / "metadata"

    legacy_path = ops_cli._write_periodic_plan_from_daily_run(
        run_report=report,
        output_root=output_root,
        project_root=tmp_path,
        discovery_loader=loader,
        generated_at=report.finished_at + timedelta(minutes=1),
    )

    _assert_blocked_outputs(
        output_root=output_root,
        legacy_path=legacy_path,
        as_of=as_of,
        blocker_code="DQ_EXECUTION_CHRONOLOGY_INVALID",
    )
    assert loader.calls == []
