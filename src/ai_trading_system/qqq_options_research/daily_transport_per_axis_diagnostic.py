"""Strict offline per-axis diagnostic contract for TRADING-2528."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_execution_evidence as source_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "qc_qqq_options_daily_transport_per_axis_diagnostic_v1.yaml"
)
TASK_ID = "TRADING-2528_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_DIAGNOSTIC_CONTRACT_V1"
SOURCE_TASK_ID = source_v1.TASK_ID
_UNSEALED_SHA256 = "0" * 64


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


def _git_sha(value: str, field: str) -> str:
    if len(value) != 40 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase Git SHA")
    return value


def _identifier(value: str, field: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.:-")
    if not value or value != value.strip() or any(character not in allowed for character in value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _date_iso(value: object, field: str) -> str:
    if not isinstance(value, date):
        raise ValueError(f"{field} must be a date")
    return value.isoformat()


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
    if not isinstance(payload, dict) or not all(isinstance(key, str) for key in payload):
        raise ValueError("record must be a JSON object")
    return payload


class DailyTransportAxis(StrEnum):
    OPTION_CHAIN_PRESENCE = "OPTION_CHAIN_PRESENCE"
    UNDERLYING_PRICE = "UNDERLYING_PRICE"
    BID_ASK_QUOTE = "BID_ASK_QUOTE"
    GREEKS = "GREEKS"
    IMPLIED_VOLATILITY = "IMPLIED_VOLATILITY"
    OPEN_INTEREST = "OPEN_INTEREST"
    VOLUME = "VOLUME"
    CROSS_FIELD_CONSISTENCY = "CROSS_FIELD_CONSISTENCY"


class AxisDiagnosticStatus(StrEnum):
    PRESENT = "PRESENT"
    MISSING = "MISSING"
    INVALID = "INVALID"
    NOT_EVALUATED = "NOT_EVALUATED"


class AxisDiagnosticReason(StrEnum):
    CHAIN_SESSIONS_PRESENT = "CHAIN_SESSIONS_PRESENT"
    AXIS_COUNTER_NOT_EXPORTED = "AXIS_COUNTER_NOT_EXPORTED"
    SINGLE_AXIS_MISSING = "SINGLE_AXIS_MISSING"
    SINGLE_AXIS_INVALID = "SINGLE_AXIS_INVALID"
    ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE = (
        "ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE"
    )
    ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS = (
        "ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS"
    )
    SINGLE_AXIS_REJECT_IDENTIFIED = "SINGLE_AXIS_REJECT_IDENTIFIED"
    MULTIPLE_AXIS_REJECTS_IDENTIFIED = "MULTIPLE_AXIS_REJECTS_IDENTIFIED"


class AxisRejectScope(StrEnum):
    NONE = "NONE"
    SINGLE_AXIS = "SINGLE_AXIS"
    CROSS_AXIS = "CROSS_AXIS"
    UNRESOLVED_COMBINATION = "UNRESOLVED_COMBINATION"


class DiagnosticSourceId(StrEnum):
    FAILURE_RECEIPT_CONTENT = "FAILURE_RECEIPT_CONTENT"
    FAILURE_RECEIPT_FILE = "FAILURE_RECEIPT_FILE"
    PACKAGE_MANIFEST_CONTENT = "PACKAGE_MANIFEST_CONTENT"
    PACKAGE_MANIFEST_FILE = "PACKAGE_MANIFEST_FILE"
    RESULT_FILE = "RESULT_FILE"
    RESULT_PAYLOAD = "RESULT_PAYLOAD"


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


class DailyTransportAxisDiagnosticPolicy(_FrozenModel):
    schema_version: Literal[
        "qc_qqq_options_daily_transport_per_axis_diagnostic_policy.v1"
    ]
    policy_id: Literal["qc_qqq_options_daily_transport_per_axis_diagnostic_v1"]
    contract_version: Literal["1.0.0"]
    diagnostic_repository_base_commit: str
    source_task_id: Literal[
        "TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_"
        "REVALIDATION_EXECUTION_EVIDENCE_V1"
    ]
    source_repository_commit: str
    source_failure_receipt_content_sha256: str
    source_failure_receipt_file_sha256: str
    source_package_manifest_content_sha256: str
    source_package_manifest_file_sha256: str
    source_result_file_sha256: str
    source_result_payload_sha256: str
    source_backtest_id: Literal["60ce7e0bec3ad2d83a4d1341e0221492"]
    requested_start: Literal["2021-02-22"]
    requested_end: Literal["2025-12-02"]
    evaluated_start: Literal["2021-02-22"]
    evaluated_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    chain_session_count: Literal[1201]
    valid_candidate_session_count: Literal[0]
    transport_rejected_session_count: Literal[1201]
    axis_order: tuple[DailyTransportAxis, ...]
    source_binding_order: tuple[DiagnosticSourceId, ...]
    unknown_input_maps_to: Literal["NOT_EVALUATED"]
    caller_asserted_pass_accepted: Literal[False]
    raw_option_rows_allowed: Literal[False]
    new_external_action_authorized: Literal[False]
    further_cloud_run_authorized: Literal[False]
    selection_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("diagnostic_repository_base_commit", "source_repository_commit")
    @classmethod
    def _commits(cls, value: str, info: ValidationInfo) -> str:
        return _git_sha(value, str(info.field_name))

    @field_validator(
        "source_failure_receipt_content_sha256",
        "source_failure_receipt_file_sha256",
        "source_package_manifest_content_sha256",
        "source_package_manifest_file_sha256",
        "source_result_file_sha256",
        "source_result_payload_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _exact_contract(self) -> Self:
        if self.axis_order != tuple(DailyTransportAxis):
            raise ValueError("axis_order must contain the exact canonical axis set")
        if self.source_binding_order != tuple(DiagnosticSourceId):
            raise ValueError("source_binding_order must contain the exact canonical source set")
        if self.transport_rejected_session_count != self.chain_session_count:
            raise ValueError("frozen result must preserve all-chain-session rejection")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))


@dataclass(frozen=True)
class LoadedDailyTransportAxisDiagnosticPolicy:
    policy: DailyTransportAxisDiagnosticPolicy
    path: Path
    file_sha256: str
    canonical_sha256: str


class DiagnosticSourceBinding(_FrozenModel):
    source_id: DiagnosticSourceId
    sha256: str

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class AxisDiagnosticRecord(_FrozenModel):
    axis: DailyTransportAxis
    status: AxisDiagnosticStatus
    observed_session_count: int | None = Field(default=None, ge=0)
    rejected_session_count: int | None = Field(default=None, ge=0)
    reason_codes: tuple[AxisDiagnosticReason, ...]
    source_fields: tuple[str, ...]

    @field_validator("source_fields")
    @classmethod
    def _source_fields(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("source_fields must be non-empty and unique")
        if any("raw" in item.lower() or "row" in item.lower() for item in value):
            raise ValueError("source_fields must not expose raw-row carriers")
        return tuple(_identifier(item, "source_field") for item in value)

    @model_validator(mode="after")
    def _status_semantics(self) -> Self:
        if not self.reason_codes or len(self.reason_codes) != len(set(self.reason_codes)):
            raise ValueError("reason_codes must be non-empty and unique")
        if self.status is AxisDiagnosticStatus.PRESENT:
            if self.observed_session_count is None or self.observed_session_count < 1:
                raise ValueError("PRESENT requires an observed session count")
            if self.rejected_session_count not in (None, 0):
                raise ValueError("PRESENT cannot assert rejected sessions")
            if self.reason_codes != (AxisDiagnosticReason.CHAIN_SESSIONS_PRESENT,):
                raise ValueError("PRESENT requires the chain-presence reason")
        elif self.status is AxisDiagnosticStatus.NOT_EVALUATED:
            if self.observed_session_count is not None or self.rejected_session_count is not None:
                raise ValueError("NOT_EVALUATED must not invent per-axis counts")
            if self.reason_codes != (AxisDiagnosticReason.AXIS_COUNTER_NOT_EXPORTED,):
                raise ValueError("NOT_EVALUATED requires the missing-counter reason")
        else:
            if self.rejected_session_count is None or self.rejected_session_count < 1:
                raise ValueError("MISSING/INVALID require an observed reject count")
            expected = (
                AxisDiagnosticReason.SINGLE_AXIS_MISSING
                if self.status is AxisDiagnosticStatus.MISSING
                else AxisDiagnosticReason.SINGLE_AXIS_INVALID
            )
            if expected not in self.reason_codes:
                raise ValueError("MISSING/INVALID reason does not match status")
        return self


def classify_axis_rejection(
    records: Sequence[AxisDiagnosticRecord], *, combined_rejected_session_count: int
) -> tuple[AxisRejectScope, tuple[AxisDiagnosticReason, ...]]:
    if combined_rejected_session_count < 0:
        raise ValueError("combined_rejected_session_count must be non-negative")
    rejected = tuple(
        item
        for item in records
        if item.status in (AxisDiagnosticStatus.MISSING, AxisDiagnosticStatus.INVALID)
    )
    if len(rejected) == 1:
        return (
            AxisRejectScope.SINGLE_AXIS,
            (AxisDiagnosticReason.SINGLE_AXIS_REJECT_IDENTIFIED,),
        )
    if len(rejected) > 1:
        return (
            AxisRejectScope.CROSS_AXIS,
            (AxisDiagnosticReason.MULTIPLE_AXIS_REJECTS_IDENTIFIED,),
        )
    if combined_rejected_session_count:
        return (
            AxisRejectScope.UNRESOLVED_COMBINATION,
            (
                AxisDiagnosticReason.ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE,
                AxisDiagnosticReason.ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS,
            ),
        )
    return AxisRejectScope.NONE, ()


class DailyTransportPerAxisDiagnosticEnvelope(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_transport_per_axis_diagnostic.v1"]
    diagnostic_id: Literal["trading-2528-2522-v4-daily-transport-per-axis"]
    task_id: Literal[
        "TRADING-2528_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_DIAGNOSTIC_CONTRACT_V1"
    ]
    contract_version: Literal["1.0.0"]
    diagnostic_repository_base_commit: str
    policy_id: Literal["qc_qqq_options_daily_transport_per_axis_diagnostic_v1"]
    policy_file_sha256: str
    policy_canonical_sha256: str
    source_task_id: Literal[
        "TRADING-2522_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_"
        "REVALIDATION_EXECUTION_EVIDENCE_V1"
    ]
    source_repository_commit: str
    source_bindings: tuple[DiagnosticSourceBinding, ...]
    source_backtest_id: Literal["60ce7e0bec3ad2d83a4d1341e0221492"]
    requested_start: Literal["2021-02-22"]
    requested_end: Literal["2025-12-02"]
    evaluated_start: Literal["2021-02-22"]
    evaluated_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    chain_session_count: Literal[1201]
    valid_candidate_session_count: Literal[0]
    transport_rejected_session_count: Literal[1201]
    axes: tuple[AxisDiagnosticRecord, ...]
    reject_scope: Literal[AxisRejectScope.UNRESOLVED_COMBINATION]
    reject_reason_codes: tuple[AxisDiagnosticReason, ...]
    root_cause_status: Literal["UNRESOLVED"]
    source_evidence_admission_status: Literal["FAIL"]
    local_derived_aggregate_dq_status: Literal["NOT_EVALUATED"]
    local_derived_aggregate_pit_status: Literal["NOT_EVALUATED"]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    authorization_consumed: Literal[True]
    further_cloud_run_authorized: Literal[False]
    raw_option_rows_consumed: Literal[False]
    raw_option_rows_reconstructed: Literal[False]
    external_action: Literal["none"]
    selection_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("diagnostic_repository_base_commit", "source_repository_commit")
    @classmethod
    def _commits(cls, value: str, info: ValidationInfo) -> str:
        return _git_sha(value, str(info.field_name))

    @field_validator("policy_file_sha256", "policy_canonical_sha256")
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _diagnostic_semantics(self) -> Self:
        if tuple(item.source_id for item in self.source_bindings) != tuple(DiagnosticSourceId):
            raise ValueError("source bindings must use exact canonical order")
        if len({item.source_id for item in self.source_bindings}) != len(self.source_bindings):
            raise ValueError("source bindings must be unique")
        if tuple(item.axis for item in self.axes) != tuple(DailyTransportAxis):
            raise ValueError("axes must use exact canonical order")
        if len({item.axis for item in self.axes}) != len(self.axes):
            raise ValueError("axes must be unique")
        if self.axes[0].status is not AxisDiagnosticStatus.PRESENT:
            raise ValueError("chain presence must remain PRESENT")
        if any(
            item.status is not AxisDiagnosticStatus.NOT_EVALUATED for item in self.axes[1:]
        ):
            raise ValueError("unexported per-axis evidence must remain NOT_EVALUATED")
        expected_reasons = (
            AxisDiagnosticReason.ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE,
            AxisDiagnosticReason.ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS,
        )
        if self.reject_reason_codes != expected_reasons:
            raise ValueError("unresolved combination reasons drifted")
        if self.transport_rejected_session_count != self.chain_session_count:
            raise ValueError("all-chain-session rejection identity drifted")
        return self


def load_daily_transport_axis_diagnostic_policy(
    policy_path: Path = DEFAULT_POLICY_PATH,
) -> LoadedDailyTransportAxisDiagnosticPolicy:
    resolved = policy_path.resolve()
    if not resolved.is_file() or resolved.is_symlink():
        raise ValueError("diagnostic policy must be a regular non-symlink file")
    raw = resolved.read_bytes()
    payload = safe_load_yaml_path(resolved)
    if not isinstance(payload, dict) or not all(isinstance(key, str) for key in payload):
        raise ValueError("diagnostic policy must be a YAML object")
    policy = DailyTransportAxisDiagnosticPolicy.model_validate(payload, strict=False)
    return LoadedDailyTransportAxisDiagnosticPolicy(
        policy=policy,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(policy.canonical_bytes),
    )


def _source_repository_commit(runtime_identity: str) -> str:
    values = {
        item.split("=", 1)[0]: item.split("=", 1)[1]
        for item in runtime_identity.split("|")
        if "=" in item
    }
    repository = values.get("repository")
    if repository is None:
        raise ValueError("runtime identity does not bind repository")
    return _git_sha(repository, "runtime repository")


def _expected_source_bindings(
    *,
    failure_receipt: source_v1.DailySliceExecutionFailureReceipt,
    package_manifest: source_v1.DailySliceExecutionEvidencePackageManifest,
) -> dict[DiagnosticSourceId, str]:
    return {
        DiagnosticSourceId.FAILURE_RECEIPT_CONTENT: failure_receipt.content_sha256,
        DiagnosticSourceId.FAILURE_RECEIPT_FILE: failure_receipt.canonical_sha256,
        DiagnosticSourceId.PACKAGE_MANIFEST_CONTENT: package_manifest.content_sha256,
        DiagnosticSourceId.PACKAGE_MANIFEST_FILE: package_manifest.canonical_sha256,
        DiagnosticSourceId.RESULT_FILE: failure_receipt.result_file_sha256,
        DiagnosticSourceId.RESULT_PAYLOAD: failure_receipt.result_payload_sha256,
    }


def _validate_source_facts(
    *,
    policy: DailyTransportAxisDiagnosticPolicy,
    failure_receipt: source_v1.DailySliceExecutionFailureReceipt,
    package_manifest: source_v1.DailySliceExecutionEvidencePackageManifest,
) -> None:
    expected = (
        (failure_receipt.task_id, policy.source_task_id),
        (
            _source_repository_commit(failure_receipt.runtime_identity),
            policy.source_repository_commit,
        ),
        (failure_receipt.content_sha256, policy.source_failure_receipt_content_sha256),
        (failure_receipt.canonical_sha256, policy.source_failure_receipt_file_sha256),
        (package_manifest.content_sha256, policy.source_package_manifest_content_sha256),
        (package_manifest.canonical_sha256, policy.source_package_manifest_file_sha256),
        (failure_receipt.result_file_sha256, policy.source_result_file_sha256),
        (failure_receipt.result_payload_sha256, policy.source_result_payload_sha256),
        (failure_receipt.backtest_id, policy.source_backtest_id),
        (_date_iso(failure_receipt.requested_start, "requested_start"), policy.requested_start),
        (_date_iso(failure_receipt.requested_end, "requested_end"), policy.requested_end),
        (_date_iso(failure_receipt.evaluated_start, "evaluated_start"), policy.evaluated_start),
        (_date_iso(failure_receipt.evaluated_end, "evaluated_end"), policy.evaluated_end),
        (failure_receipt.expected_session_count, policy.expected_session_count),
        (failure_receipt.daily_slice_chain_session_count, policy.chain_session_count),
        (failure_receipt.valid_candidate_session_count, policy.valid_candidate_session_count),
        (
            failure_receipt.transport_rejected_session_count,
            policy.transport_rejected_session_count,
        ),
    )
    if any(actual != frozen for actual, frozen in expected):
        raise ValueError("2522 source identity does not match diagnostic policy")
    if (
        failure_receipt.typed_failure_reason != source_v1.TYPED_FAILURE_REASON
        or failure_receipt.failure_axis_resolution
        != "UNRESOLVED_REQUIRES_TARGETED_DIAGNOSTIC"
        or failure_receipt.strict_admission_status != "FAIL"
        or failure_receipt.authorization_consumed is not True
        or failure_receipt.further_cloud_run_authorized is not False
        or failure_receipt.raw_option_rows_exported is not False
        or failure_receipt.engine_status != "POLICY_BLOCKED_CASH_PRESERVATION"
        or package_manifest.evidence_admission_status != "FAIL"
        or package_manifest.dq_pit_status != "NOT_EVALUATED"
    ):
        raise ValueError("2522 failure/safety boundary drifted")


def _axis_records(policy: DailyTransportAxisDiagnosticPolicy) -> tuple[AxisDiagnosticRecord, ...]:
    records: list[AxisDiagnosticRecord] = []
    for axis in policy.axis_order:
        if axis is DailyTransportAxis.OPTION_CHAIN_PRESENCE:
            records.append(
                AxisDiagnosticRecord(
                    axis=axis,
                    status=AxisDiagnosticStatus.PRESENT,
                    observed_session_count=policy.chain_session_count,
                    rejected_session_count=0,
                    reason_codes=(AxisDiagnosticReason.CHAIN_SESSIONS_PRESENT,),
                    source_fields=("daily_slice_chain_session_count",),
                )
            )
        else:
            records.append(
                AxisDiagnosticRecord(
                    axis=axis,
                    status=AxisDiagnosticStatus.NOT_EVALUATED,
                    reason_codes=(AxisDiagnosticReason.AXIS_COUNTER_NOT_EXPORTED,),
                    source_fields=("runtime_diagnostic:no_per_axis_counters",),
                )
            )
    return tuple(records)


def build_daily_transport_per_axis_diagnostic(
    *,
    policy_load: LoadedDailyTransportAxisDiagnosticPolicy,
    failure_receipt: source_v1.DailySliceExecutionFailureReceipt,
    package_manifest: source_v1.DailySliceExecutionEvidencePackageManifest,
    source_bindings: Sequence[DiagnosticSourceBinding],
) -> DailyTransportPerAxisDiagnosticEnvelope:
    policy = policy_load.policy
    _validate_source_facts(
        policy=policy,
        failure_receipt=failure_receipt,
        package_manifest=package_manifest,
    )
    expected_bindings = _expected_source_bindings(
        failure_receipt=failure_receipt,
        package_manifest=package_manifest,
    )
    observed: dict[DiagnosticSourceId, str] = {}
    for binding in source_bindings:
        if binding.source_id in observed:
            raise ValueError("duplicate diagnostic source binding")
        observed[binding.source_id] = binding.sha256
    if observed != expected_bindings:
        raise ValueError("diagnostic source binding set/hash mismatch")
    ordered_bindings = tuple(
        DiagnosticSourceBinding(source_id=source_id, sha256=observed[source_id])
        for source_id in policy.source_binding_order
    )
    axes = _axis_records(policy)
    reject_scope, reject_reasons = classify_axis_rejection(
        axes,
        combined_rejected_session_count=policy.transport_rejected_session_count,
    )
    if reject_scope is not AxisRejectScope.UNRESOLVED_COMBINATION:
        raise ValueError("frozen 2522 result must remain unresolved combination")
    return DailyTransportPerAxisDiagnosticEnvelope.seal(
        schema_version="qc_qqq_options_daily_transport_per_axis_diagnostic.v1",
        diagnostic_id="trading-2528-2522-v4-daily-transport-per-axis",
        task_id=TASK_ID,
        contract_version=policy.contract_version,
        diagnostic_repository_base_commit=policy.diagnostic_repository_base_commit,
        policy_id=policy.policy_id,
        policy_file_sha256=policy_load.file_sha256,
        policy_canonical_sha256=policy_load.canonical_sha256,
        source_task_id=policy.source_task_id,
        source_repository_commit=policy.source_repository_commit,
        source_bindings=ordered_bindings,
        source_backtest_id=policy.source_backtest_id,
        requested_start=policy.requested_start,
        requested_end=policy.requested_end,
        evaluated_start=policy.evaluated_start,
        evaluated_end=policy.evaluated_end,
        expected_session_count=policy.expected_session_count,
        chain_session_count=policy.chain_session_count,
        valid_candidate_session_count=policy.valid_candidate_session_count,
        transport_rejected_session_count=policy.transport_rejected_session_count,
        axes=axes,
        reject_scope=reject_scope,
        reject_reason_codes=reject_reasons,
        root_cause_status="UNRESOLVED",
        source_evidence_admission_status="FAIL",
        local_derived_aggregate_dq_status="NOT_EVALUATED",
        local_derived_aggregate_pit_status="NOT_EVALUATED",
        option_event_dq_status="NOT_EVALUATED",
        option_event_pit_status="NOT_EVALUATED",
        authorization_consumed=True,
        further_cloud_run_authorized=False,
        raw_option_rows_consumed=False,
        raw_option_rows_reconstructed=False,
        external_action="none",
        selection_authorized=False,
        engine_status=policy.engine_status,
        investment_interpretation_generated=False,
        production_effect=policy.production_effect,
        broker_action=policy.broker_action,
    )


def build_repository_daily_transport_per_axis_diagnostic(
    *, project_root: Path = PROJECT_ROOT
) -> DailyTransportPerAxisDiagnosticEnvelope:
    root = project_root.resolve()
    policy_load = load_daily_transport_axis_diagnostic_policy(
        root / DEFAULT_POLICY_PATH.relative_to(PROJECT_ROOT)
    )
    package = source_v1.load_daily_slice_execution_evidence_package(project_root=root)
    expected = _expected_source_bindings(
        failure_receipt=package.failure_receipt,
        package_manifest=package.manifest,
    )
    return build_daily_transport_per_axis_diagnostic(
        policy_load=policy_load,
        failure_receipt=package.failure_receipt,
        package_manifest=package.manifest,
        source_bindings=tuple(
            DiagnosticSourceBinding(source_id=source_id, sha256=value)
            for source_id, value in reversed(tuple(expected.items()))
        ),
    )


__all__ = [
    "AxisDiagnosticReason",
    "AxisDiagnosticRecord",
    "AxisDiagnosticStatus",
    "AxisRejectScope",
    "DailyTransportAxis",
    "DailyTransportAxisDiagnosticPolicy",
    "DailyTransportPerAxisDiagnosticEnvelope",
    "DiagnosticSourceBinding",
    "DiagnosticSourceId",
    "LoadedDailyTransportAxisDiagnosticPolicy",
    "build_daily_transport_per_axis_diagnostic",
    "build_repository_daily_transport_per_axis_diagnostic",
    "classify_axis_rejection",
    "load_daily_transport_axis_diagnostic_policy",
]
