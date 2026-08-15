"""Build and replay TRADING-2522 export-safe failure evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_authorization_admission as admission_v4,
)
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PACKAGE_RELATIVE_PATH = Path(
    "inputs/research/qqq_options/"
    "trading_2522_primary_window_daily_slice_revalidation_execution_v1"
)
TASK_ID = (
    "TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_"
    "REVALIDATION_EXECUTION_EVIDENCE_V1"
)
BACKTEST_ID = "60ce7e0bec3ad2d83a4d1341e0221492"
BACKTEST_NAME = "Logical Red Bison"
BUILD_ID = "2095dc-5e494a"
ENGINE_VERSION = "2.5.0.0.18004"
HOST_CLASS = "Community B-MICRO"
RESULT_FILE_SHA256 = "45e8647f4d4b0e3590252acedacca4235695341574f44bc593d8ab9b283f603e"
RESULT_BYTE_COUNT = 813_386
OBSERVED_AT_UTC = datetime(2026, 8, 15, 4, 23, 41, 754660, tzinfo=UTC)
OWNER_ADMITTED_AT_UTC = datetime(2026, 8, 15, 1, 0, tzinfo=UTC)
LOGIN_OBSERVED_AT_UTC = datetime(2026, 8, 15, 2, 4, 1, tzinfo=UTC)
PROJECT_BUILD_OBSERVED_AT_UTC = datetime(2026, 8, 15, 2, 6, 7, tzinfo=UTC)
RUN_SUBMITTED_AT_UTC = datetime(2026, 8, 15, 2, 6, 43, 559000, tzinfo=UTC)
RUN_CONSUMED_AT_UTC = datetime(2026, 8, 15, 2, 6, 44, tzinfo=UTC)
RUN_STARTED_AT_UTC = datetime(2026, 8, 15, 2, 7, 6, tzinfo=UTC)
RUN_ENDED_AT_UTC = datetime(2026, 8, 15, 3, 51, 41, tzinfo=UTC)
RUNTIME_TERMINAL = (
    "status=INVALID_INCOMPLETE|observed_sessions=0|invalid_sessions=1202"
    "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
    "|log_data=false|object_store=false"
)
RUNTIME_DIAGNOSTIC = (
    "chain_sessions=1201|valid_candidate_sessions=0|transport_rejected_sessions=1201"
    "|daily_precise_end_time=true|underlying_accessor=underlying_last_price"
)
STRICT_PARSER_REASON_CODE = "DAILY_SLICE_RESULT_PARSER_REJECTED"
STRICT_PARSER_DETAIL = (
    "COLLECTOR_RESULT_ADMISSION_REJECTED: "
    "runtime terminal status is incomplete or unsafe"
)
TYPED_FAILURE_REASON = "DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS"
_UNSEALED_SHA256 = "0" * 64
_PACKAGE_FILES = (
    "external_action_ledger.json",
    "failure_receipt.json",
    "owner_decision.txt",
    "package_manifest.json",
    "result.json",
    "run_attempt_consumption_receipt.json",
    "run_attempt_ledger.json",
)
_PACKAGE_ARTIFACTS = tuple(name for name in _PACKAGE_FILES if name != "package_manifest.json")


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _sha256(value: str, field: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _identifier(value: str, field: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-")
    if not value or value != value.strip() or any(character not in allowed for character in value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset.total_seconds() != 0:
        raise ValueError(f"{field} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _mapping(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be an object")
    return value


def _bound_file(relative: str, *, project_root: Path, field: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError(f"{field} must be repository-relative")
    root = project_root.resolve()
    resolved = (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if not resolved.is_file() or resolved.is_symlink():
        raise ValueError(f"{field} must be a regular non-symlink file")
    return resolved


def _duplicate_key_rejecting_json(raw: bytes) -> dict[str, object]:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=hook)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc
    return _mapping(payload, "record")


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _SealedModel(_FrozenModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return _sha256_bytes(_canonical_json_bytes(self.semantic_payload()))

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return _sha256_bytes(self.canonical_bytes)

    @model_validator(mode="after")
    def _verify_content_hash(self) -> Self:
        if self.content_sha256 != _UNSEALED_SHA256:
            if self.content_sha256 != self.compute_content_sha256():
                raise ValueError("content_sha256 does not match semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        draft = cls(content_sha256=_UNSEALED_SHA256, **payload)
        return cls(content_sha256=draft.compute_content_sha256(), **payload)

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        _duplicate_key_rejecting_json(raw)
        value = cls.model_validate_json(raw)
        if raw != value.canonical_bytes:
            raise ValueError("record is not canonical JSON bytes")
        return value


class DailySliceExecutionFailureReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_slice_execution_failure_receipt.v1"]
    receipt_id: Literal["trading-2522-v4-daily-slice-transport-failure"]
    task_id: Literal[
        "TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_"
        "REVALIDATION_EXECUTION_EVIDENCE_V1"
    ]
    observed_at_utc: datetime
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    daily_slice_authorization_admission_content_sha256: str
    run_attempt_ledger_content_sha256: str
    run_attempt_consumption_content_sha256: str
    external_action_ledger_content_sha256: str
    result_file_sha256: str
    result_payload_sha256: str
    target_project_id: Literal[34808569]
    project_code_lf_sha256: str
    backtest_id: Literal["60ce7e0bec3ad2d83a4d1341e0221492"]
    backtest_name: Literal["Logical Red Bison"]
    build_id: Literal["2095dc-5e494a"]
    engine_version: Literal["2.5.0.0.18004"]
    host_class: Literal["Community B-MICRO"]
    platform_identity_source: Literal["CODEX_SIGNED_IN_QC_RESULTS_UI_OBSERVATION"]
    run_started_at_utc: datetime
    run_ended_at_utc: datetime
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    expected_session_count: Literal[1202]
    state_status: Literal["Completed"]
    observed_session_count: Literal[0]
    invalid_session_count: Literal[1202]
    daily_slice_chain_session_count: Literal[1201]
    valid_candidate_session_count: Literal[0]
    transport_rejected_session_count: Literal[1201]
    orders: Literal[0]
    fills: Literal[0]
    total_fees: Literal["$0.00"]
    start_equity: Literal["100000"]
    end_equity: Literal["100000"]
    portfolio_invested: Literal[False]
    runtime_identity: str
    runtime_terminal: Literal[
        "status=INVALID_INCOMPLETE|observed_sessions=0|invalid_sessions=1202"
        "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
        "|log_data=false|object_store=false"
    ]
    runtime_diagnostic: Literal[
        "chain_sessions=1201|valid_candidate_sessions=0|transport_rejected_sessions=1201"
        "|daily_precise_end_time=true|underlying_accessor=underlying_last_price"
    ]
    chart_present: Literal[True]
    chart_series_count: Literal[0]
    strict_admission_status: Literal["FAIL"]
    strict_parser_reason_code: Literal["DAILY_SLICE_RESULT_PARSER_REJECTED"]
    strict_parser_detail: Literal[
        "COLLECTOR_RESULT_ADMISSION_REJECTED: "
        "runtime terminal status is incomplete or unsafe"
    ]
    typed_failure_reason: Literal[
        "DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS"
    ]
    failure_axis_resolution: Literal["UNRESOLVED_REQUIRES_TARGETED_DIAGNOSTIC"]
    local_derived_aggregate_dq_status: Literal["NOT_EVALUATED"]
    local_derived_aggregate_pit_status: Literal["NOT_EVALUATED"]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    authorization_consumed: Literal[True]
    further_cloud_run_authorized: Literal[False]
    external_action_scope_status: Literal["PASS"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    raw_option_rows_exported: Literal[False]
    log_data_carrier_used: Literal[False]
    object_store_used: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator(
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
        "daily_slice_authorization_admission_content_sha256",
        "run_attempt_ledger_content_sha256",
        "run_attempt_consumption_content_sha256",
        "external_action_ledger_content_sha256",
        "result_file_sha256",
        "result_payload_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("observed_at_utc", "run_started_at_utc", "run_ended_at_utc")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @model_validator(mode="after")
    def _failure_identity(self) -> Self:
        if self.result_file_sha256 != RESULT_FILE_SHA256:
            raise ValueError("result file identity drifted")
        if not self.runtime_identity.startswith(
            "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1|"
        ):
            raise ValueError("runtime identity schema drifted")
        if self.run_started_at_utc >= self.run_ended_at_utc:
            raise ValueError("run chronology is invalid")
        if (
            self.requested_start,
            self.evaluated_start,
            self.requested_end,
            self.evaluated_end,
        ) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("PRIMARY requested/evaluated range drifted")
        return self


class PackageArtifact(_FrozenModel):
    relative_path: str
    role: str
    sha256: str
    byte_count: int = Field(ge=1)

    @field_validator("relative_path", "role")
    @classmethod
    def _text(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class DailySliceExecutionEvidencePackageManifest(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_slice_execution_evidence_package.v1"]
    package_id: Literal["TRADING_2522_DAILY_SLICE_REVALIDATION_EXECUTION_EVIDENCE_V1"]
    task_id: Literal[
        "TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_"
        "REVALIDATION_EXECUTION_EVIDENCE_V1"
    ]
    created_at_utc: datetime
    failure_receipt_content_sha256: str
    run_attempt_consumption_content_sha256: str
    external_action_ledger_content_sha256: str
    result_file_sha256: str
    artifacts: tuple[PackageArtifact, ...]
    evidence_admission_status: Literal["FAIL"]
    dq_pit_status: Literal["NOT_EVALUATED"]
    typed_failure_reason: Literal[
        "DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS"
    ]
    authorization_consumed: Literal[True]
    further_cloud_run_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("created_at_utc")
    @classmethod
    def _time(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator(
        "failure_receipt_content_sha256",
        "run_attempt_consumption_content_sha256",
        "external_action_ledger_content_sha256",
        "result_file_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _artifact_inventory(self) -> Self:
        paths = tuple(item.relative_path for item in self.artifacts)
        if paths != _PACKAGE_ARTIFACTS:
            raise ValueError("package artifact inventory/order drifted")
        if len(paths) != len(set(paths)):
            raise ValueError("package artifact paths are not unique")
        return self


@dataclass(frozen=True)
class BuiltDailySliceExecutionEvidencePackage:
    admitted_authorization: admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization
    run_attempt_ledger: admission_v1.CollectionExternalActionLedger
    run_attempt_consumption: admission_v4.DailySliceRunAttemptConsumptionReceipt
    external_action_ledger: admission_v1.CollectionExternalActionLedger
    failure_receipt: DailySliceExecutionFailureReceipt
    manifest: DailySliceExecutionEvidencePackageManifest
    package_root: Path


def _runtime_identity(admitted: admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization) -> str:
    proposal = admitted.policy_load.revalidation_package.proposal
    return (
        "schema=qc_qqq_options_derived_aggregate_collector_runtime.v1"
        f"|scope={proposal.run_scope.content_sha256}"
        f"|repository={proposal.run_scope.repository_code_sha}"
        f"|policy_file={proposal.collector_policy_file_sha256}"
        f"|policy_canonical={proposal.collector_policy_canonical_sha256}"
        f"|transport={proposal.transport_map_sha256}"
    )


def _admit_owner(
    *, owner_decision_bytes: bytes, project_root: Path
) -> admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization:
    return admission_v4.admit_qc_qqq_options_daily_slice_owner_authorization(
        admission_id="trading-2521-v4-admission",
        admitted_at_utc=OWNER_ADMITTED_AT_UTC,
        owner_decision_bytes=owner_decision_bytes,
        owner_decision_source="PROJECT_OWNER_CURRENT_CODEX_DIALOG",
        project_root=project_root,
    )


def _actions(
    *,
    admitted: admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization,
    result_bytes: bytes,
) -> tuple[admission_v1.CollectionExternalAction, ...]:
    code_hash = admitted.collector_authorization.project_code_lf_sha256
    return (
        admission_v1.CollectionExternalAction(
            action_id="trading-2522-login",
            ordinal=1,
            action_type=admission_v1.CollectionActionType.QUANTCONNECT_LOGIN,
            occurred_at_utc=LOGIN_OBSERVED_AT_UTC,
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
        ),
        admission_v1.CollectionExternalAction(
            action_id="trading-2522-project-mutation",
            ordinal=2,
            action_type=admission_v1.CollectionActionType.MODIFY_EXISTING_DEDICATED_PROJECT_ONCE,
            occurred_at_utc=PROJECT_BUILD_OBSERVED_AT_UTC,
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
        ),
        admission_v1.CollectionExternalAction(
            action_id="trading-2522-cloud-run",
            ordinal=3,
            action_type=admission_v1.CollectionActionType.RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST,
            occurred_at_utc=RUN_SUBMITTED_AT_UTC,
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
            backtest_id=BACKTEST_ID,
        ),
        admission_v1.CollectionExternalAction(
            action_id="trading-2522-result-download",
            ordinal=4,
            action_type=(
                admission_v1.CollectionActionType.EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION
            ),
            occurred_at_utc=OBSERVED_AT_UTC,
            status=admission_v1.CollectionActionStatus.COMPLETED,
            target_project_id=34808569,
            project_code_lf_sha256=code_hash,
            backtest_id=BACKTEST_ID,
            result_file_sha256=_sha256_bytes(result_bytes),
        ),
    )


def _result_facts(
    *,
    result_bytes: bytes,
    admitted: admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization,
) -> tuple[dict[str, object], str]:
    if len(result_bytes) != RESULT_BYTE_COUNT or _sha256_bytes(result_bytes) != RESULT_FILE_SHA256:
        raise ValueError("result file byte identity drifted")
    payload = _duplicate_key_rejecting_json(result_bytes)
    collector_v1._reject_prohibited_result_markers(payload)  # noqa: SLF001
    state = _mapping(payload.get("state"), "state")
    if (
        state.get("Status") != "Completed"
        or state.get("RuntimeError") not in (None, "")
        or state.get("Name") != BACKTEST_NAME
        or str(state.get("OrderCount")) != "0"
        or state.get("StartTime") != "2026-08-15T02:07:06Z"
        or state.get("EndTime") != "2026-08-15T03:51:41Z"
    ):
        raise ValueError("result state identity drifted")
    hostname = state.get("Hostname")
    if not isinstance(hostname, str) or not hostname.endswith(BACKTEST_ID):
        raise ValueError("result hostname/backtest identity mismatch")
    orders = payload.get("orders")
    if not isinstance(orders, (dict, list)) or len(orders) != 0:
        raise ValueError("result orders inventory is not empty")
    statistics = _mapping(payload.get("statistics"), "statistics")
    if (
        str(statistics.get("Total Orders")) != "0"
        or statistics.get("Total Fees") != "$0.00"
        or str(statistics.get("Start Equity")) != "100000"
        or str(statistics.get("End Equity")) != "100000"
    ):
        raise ValueError("result cash-preservation statistics drifted")
    algorithm = _mapping(payload.get("algorithmConfiguration"), "algorithmConfiguration")
    if not str(algorithm.get("startDate", "")).startswith("2021-02-22"):
        raise ValueError("result requested start drifted")
    if not str(algorithm.get("endDate", "")).startswith("2025-12-02"):
        raise ValueError("result requested end drifted")
    runtime = _mapping(payload.get("runtimeStatistics"), "runtimeStatistics")
    runtime_identity = _runtime_identity(admitted)
    if runtime.get("TRADING2512_IDENTITY") != runtime_identity:
        raise ValueError("result runtime identity drifted")
    if runtime.get("TRADING2512_TERMINAL") != RUNTIME_TERMINAL:
        raise ValueError("result terminal status drifted")
    if runtime.get("TRADING2520_DIAGNOSTIC") != RUNTIME_DIAGNOSTIC:
        raise ValueError("result daily Slice diagnostic drifted")
    chart = _mapping(
        _mapping(payload.get("charts"), "charts").get(
            "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1"
        ),
        "collector chart",
    )
    if chart.get("name") != "TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1":
        raise ValueError("collector chart identity drifted")
    if _mapping(chart.get("series"), "collector chart series"):
        raise ValueError("failed result unexpectedly contains derived aggregate series")
    return payload, runtime_identity


def _build_records(
    *, owner_decision_bytes: bytes, result_bytes: bytes, project_root: Path
) -> tuple[
    admission_v4.AdmittedQCQQQOptionsDailySliceAuthorization,
    admission_v1.CollectionExternalActionLedger,
    admission_v4.DailySliceRunAttemptConsumptionReceipt,
    admission_v1.CollectionExternalActionLedger,
    DailySliceExecutionFailureReceipt,
]:
    admitted = _admit_owner(owner_decision_bytes=owner_decision_bytes, project_root=project_root)
    payload, runtime_identity = _result_facts(result_bytes=result_bytes, admitted=admitted)
    actions = _actions(admitted=admitted, result_bytes=result_bytes)
    run_ledger = admission_v4.build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id="trading-2522-run-attempt-ledger",
        sealed_at_utc=RUN_CONSUMED_AT_UTC,
        admitted_authorization=admitted,
        actions=actions[:3],
    )
    consumption = admission_v4.build_qc_qqq_options_daily_slice_run_attempt_consumption(
        consumption_id="trading-2522-v4-run-attempt-consumption",
        recorded_at_utc=RUN_CONSUMED_AT_UTC,
        admitted_authorization=admitted,
        external_action_ledger=run_ledger,
    )
    result_admission_id = "trading-2522-result-admission"
    final_ledger = admission_v4.build_qc_qqq_options_daily_slice_external_action_ledger(
        ledger_id=f"{result_admission_id}:ledger",
        sealed_at_utc=OBSERVED_AT_UTC,
        admitted_authorization=admitted,
        actions=actions,
    )
    try:
        admission_v4.build_qc_qqq_options_daily_slice_parsed_result_admission(
            result_admission_id=result_admission_id,
            admitted_at_utc=OBSERVED_AT_UTC,
            admitted_authorization=admitted,
            run_attempt_consumption=consumption,
            actions=actions,
            backtest_id=BACKTEST_ID,
            result_bytes=result_bytes,
            reviewed_project_code_lf_sha256=(
                admitted.collector_authorization.project_code_lf_sha256
            ),
            project_root=project_root,
        )
    except admission_v4.QCQQQOptionsDailySliceAuthorizationAdmissionError as exc:
        if exc.reason_code != STRICT_PARSER_REASON_CODE or exc.detail != STRICT_PARSER_DETAIL:
            raise ValueError("strict parser rejection identity drifted") from exc
    else:
        raise ValueError("invalid result unexpectedly passed strict parser")
    loaded = admitted.policy_load
    receipt = DailySliceExecutionFailureReceipt.seal(
        schema_version="qc_qqq_options_daily_slice_execution_failure_receipt.v1",
        receipt_id="trading-2522-v4-daily-slice-transport-failure",
        task_id=TASK_ID,
        observed_at_utc=OBSERVED_AT_UTC,
        owner_decision_file_sha256=_sha256_bytes(owner_decision_bytes),
        owner_decision_content_sha256=admitted.owner_candidate.owner_decision_content_sha256,
        admission_policy_file_sha256=loaded.policy_file_sha256,
        admission_policy_canonical_sha256=loaded.policy_canonical_sha256,
        daily_slice_authorization_admission_content_sha256=(
            admitted.daily_slice_admission_receipt.content_sha256
        ),
        run_attempt_ledger_content_sha256=run_ledger.content_sha256,
        run_attempt_consumption_content_sha256=consumption.content_sha256,
        external_action_ledger_content_sha256=final_ledger.content_sha256,
        result_file_sha256=_sha256_bytes(result_bytes),
        result_payload_sha256=collector_v1._canonical_sha256(payload),  # noqa: SLF001
        target_project_id=34808569,
        project_code_lf_sha256=admitted.collector_authorization.project_code_lf_sha256,
        backtest_id=BACKTEST_ID,
        backtest_name=BACKTEST_NAME,
        build_id=BUILD_ID,
        engine_version=ENGINE_VERSION,
        host_class=HOST_CLASS,
        platform_identity_source="CODEX_SIGNED_IN_QC_RESULTS_UI_OBSERVATION",
        run_started_at_utc=RUN_STARTED_AT_UTC,
        run_ended_at_utc=RUN_ENDED_AT_UTC,
        requested_start=date(2021, 2, 22),
        requested_end=date(2025, 12, 2),
        evaluated_start=date(2021, 2, 22),
        evaluated_end=date(2025, 12, 2),
        expected_session_count=1202,
        state_status="Completed",
        observed_session_count=0,
        invalid_session_count=1202,
        daily_slice_chain_session_count=1201,
        valid_candidate_session_count=0,
        transport_rejected_session_count=1201,
        orders=0,
        fills=0,
        total_fees="$0.00",
        start_equity="100000",
        end_equity="100000",
        portfolio_invested=False,
        runtime_identity=runtime_identity,
        runtime_terminal=RUNTIME_TERMINAL,
        runtime_diagnostic=RUNTIME_DIAGNOSTIC,
        chart_present=True,
        chart_series_count=0,
        strict_admission_status="FAIL",
        strict_parser_reason_code=STRICT_PARSER_REASON_CODE,
        strict_parser_detail=STRICT_PARSER_DETAIL,
        typed_failure_reason=TYPED_FAILURE_REASON,
        failure_axis_resolution="UNRESOLVED_REQUIRES_TARGETED_DIAGNOSTIC",
        local_derived_aggregate_dq_status="NOT_EVALUATED",
        local_derived_aggregate_pit_status="NOT_EVALUATED",
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        authorization_consumed=True,
        further_cloud_run_authorized=False,
        external_action_scope_status="PASS",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        raw_option_rows_exported=False,
        log_data_carrier_used=False,
        object_store_used=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    return admitted, run_ledger, consumption, final_ledger, receipt


def _build_manifest(
    *,
    failure_receipt: DailySliceExecutionFailureReceipt,
    run_attempt_consumption: admission_v4.DailySliceRunAttemptConsumptionReceipt,
    external_action_ledger: admission_v1.CollectionExternalActionLedger,
    artifact_bytes: dict[str, bytes],
) -> DailySliceExecutionEvidencePackageManifest:
    roles = {
        "external_action_ledger.json": "COMPLETE_EXTERNAL_ACTION_LEDGER",
        "failure_receipt.json": "TYPED_FAILURE_RECEIPT",
        "owner_decision.txt": "OWNER_DECISION_TOKEN",
        "result.json": "QC_RESULTS_EXPORT_SAFE",
        "run_attempt_consumption_receipt.json": "AUTHORIZATION_CONSUMPTION_RECEIPT",
        "run_attempt_ledger.json": "FIRST_RUN_ATTEMPT_LEDGER",
    }
    artifacts = tuple(
        PackageArtifact(
            relative_path=name,
            role=roles[name],
            sha256=_sha256_bytes(artifact_bytes[name]),
            byte_count=len(artifact_bytes[name]),
        )
        for name in _PACKAGE_ARTIFACTS
    )
    return DailySliceExecutionEvidencePackageManifest.seal(
        schema_version="qc_qqq_options_daily_slice_execution_evidence_package.v1",
        package_id="TRADING_2522_DAILY_SLICE_REVALIDATION_EXECUTION_EVIDENCE_V1",
        task_id=TASK_ID,
        created_at_utc=OBSERVED_AT_UTC,
        failure_receipt_content_sha256=failure_receipt.content_sha256,
        run_attempt_consumption_content_sha256=run_attempt_consumption.content_sha256,
        external_action_ledger_content_sha256=external_action_ledger.content_sha256,
        result_file_sha256=RESULT_FILE_SHA256,
        artifacts=artifacts,
        evidence_admission_status="FAIL",
        dq_pit_status="NOT_EVALUATED",
        typed_failure_reason=TYPED_FAILURE_REASON,
        authorization_consumed=True,
        further_cloud_run_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        production_effect="none",
        broker_action="none",
    )


def write_daily_slice_execution_evidence_package(
    *, project_root: Path = PROJECT_ROOT
) -> DailySliceExecutionEvidencePackageManifest:
    root = project_root.resolve()
    package_root = root / PACKAGE_RELATIVE_PATH
    owner_bytes = _bound_file(
        str(PACKAGE_RELATIVE_PATH / "owner_decision.txt"),
        project_root=root,
        field="owner decision",
    ).read_bytes()
    result_bytes = _bound_file(
        str(PACKAGE_RELATIVE_PATH / "result.json"),
        project_root=root,
        field="result artifact",
    ).read_bytes()
    admitted, run_ledger, consumption, final_ledger, receipt = _build_records(
        owner_decision_bytes=owner_bytes,
        result_bytes=result_bytes,
        project_root=root,
    )
    del admitted
    write_bytes_atomic(package_root / "run_attempt_ledger.json", run_ledger.canonical_bytes)
    write_bytes_atomic(
        package_root / "run_attempt_consumption_receipt.json", consumption.canonical_bytes
    )
    write_bytes_atomic(
        package_root / "external_action_ledger.json", final_ledger.canonical_bytes
    )
    write_bytes_atomic(package_root / "failure_receipt.json", receipt.canonical_bytes)
    artifact_bytes = {
        name: _bound_file(
            str(PACKAGE_RELATIVE_PATH / name), project_root=root, field=f"artifact {name}"
        ).read_bytes()
        for name in _PACKAGE_ARTIFACTS
    }
    manifest = _build_manifest(
        failure_receipt=receipt,
        run_attempt_consumption=consumption,
        external_action_ledger=final_ledger,
        artifact_bytes=artifact_bytes,
    )
    write_bytes_atomic(package_root / "package_manifest.json", manifest.canonical_bytes)
    return manifest


def load_daily_slice_execution_evidence_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltDailySliceExecutionEvidencePackage:
    root = project_root.resolve()
    package_root = (root / PACKAGE_RELATIVE_PATH).resolve()
    inventory = tuple(sorted(path.name for path in package_root.iterdir()))
    if inventory != _PACKAGE_FILES:
        raise ValueError("package file inventory is not exact")
    if any(not path.is_file() or path.is_symlink() for path in package_root.iterdir()):
        raise ValueError("package entries must be regular non-symlink files")
    raw = {name: (package_root / name).read_bytes() for name in _PACKAGE_FILES}
    admitted, expected_run_ledger, expected_consumption, expected_final_ledger, expected_receipt = (
        _build_records(
            owner_decision_bytes=raw["owner_decision.txt"],
            result_bytes=raw["result.json"],
            project_root=root,
        )
    )
    run_ledger = admission_v1.CollectionExternalActionLedger.from_json_bytes(
        raw["run_attempt_ledger.json"]
    )
    consumption = admission_v4.DailySliceRunAttemptConsumptionReceipt.from_json_bytes(
        raw["run_attempt_consumption_receipt.json"]
    )
    final_ledger = admission_v1.CollectionExternalActionLedger.from_json_bytes(
        raw["external_action_ledger.json"]
    )
    receipt = DailySliceExecutionFailureReceipt.from_json_bytes(raw["failure_receipt.json"])
    if run_ledger.canonical_bytes != expected_run_ledger.canonical_bytes:
        raise ValueError("run-attempt ledger differs from canonical action facts")
    if consumption.canonical_bytes != expected_consumption.canonical_bytes:
        raise ValueError("consumption receipt differs from canonical run attempt")
    if final_ledger.canonical_bytes != expected_final_ledger.canonical_bytes:
        raise ValueError("external action ledger differs from canonical lifecycle")
    if receipt.canonical_bytes != expected_receipt.canonical_bytes:
        raise ValueError("failure receipt differs from strict result facts")
    manifest = DailySliceExecutionEvidencePackageManifest.from_json_bytes(
        raw["package_manifest.json"]
    )
    expected_manifest = _build_manifest(
        failure_receipt=receipt,
        run_attempt_consumption=consumption,
        external_action_ledger=final_ledger,
        artifact_bytes={name: raw[name] for name in _PACKAGE_ARTIFACTS},
    )
    if manifest.canonical_bytes != expected_manifest.canonical_bytes:
        raise ValueError("package manifest differs from canonical package facts")
    for artifact in manifest.artifacts:
        artifact_raw = raw[artifact.relative_path]
        if (
            len(artifact_raw) != artifact.byte_count
            or _sha256_bytes(artifact_raw) != artifact.sha256
        ):
            raise ValueError(f"package artifact identity mismatch: {artifact.relative_path}")
    return BuiltDailySliceExecutionEvidencePackage(
        admitted_authorization=admitted,
        run_attempt_ledger=run_ledger,
        run_attempt_consumption=consumption,
        external_action_ledger=final_ledger,
        failure_receipt=receipt,
        manifest=manifest,
        package_root=package_root,
    )


if __name__ == "__main__":
    written = write_daily_slice_execution_evidence_package()
    print(written.canonical_sha256)
