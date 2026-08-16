"""Offline sealed proposal package for TRADING-2529.

This module only prepares and verifies a candidate export-safe aggregate collection
package.  It has no QuantConnect, network, browser, trading, or external execution
path.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    daily_transport_per_axis_diagnostic as diagnostic_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as session_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = (
    PROJECT_ROOT
    / "config"
    / "research"
    / "qc_qqq_options_daily_transport_per_axis_collection_proposal_v1.yaml"
)
DEFAULT_PACKAGE_ROOT = (
    PROJECT_ROOT
    / "inputs"
    / "research"
    / "qqq_options"
    / "trading_2529_daily_transport_per_axis_collection_proposal_v1"
)
TASK_ID = (
    "TRADING-2529_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_EXPORT_SAFE_"
    "AGGREGATE_COLLECTION_PROPOSAL_V1"
)
_UNSEALED_SHA256 = "0" * 64
_PACKAGE_FILES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)
_MANIFEST_ARTIFACTS = (
    ("PROJECT_CODE", "main.py"),
    ("OWNER_DECISION_REQUEST", "owner_decision_request.md"),
    ("PROPOSAL", "proposal.json"),
    ("RUN_SCOPE", "run_scope.json"),
)


class PerAxisCollectionProposalError(ValueError):
    """Typed fail-closed error for offline proposal construction/admission."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
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


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if path.is_absolute() or ".." in path.parts or value != path.as_posix():
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _duplicate_key_rejecting_json(raw: bytes) -> dict[str, object]:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=hook)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("record must be a JSON object")
    return value


def _bound_file(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if must_exist and (not resolved.is_file() or resolved.is_symlink()):
        raise ValueError(f"{field} must be a regular non-symlink file")
    return resolved


class Axis(StrEnum):
    OPTION_CHAIN_PRESENCE = "OPTION_CHAIN_PRESENCE"
    UNDERLYING_PRICE = "UNDERLYING_PRICE"
    BID_ASK_QUOTE = "BID_ASK_QUOTE"
    GREEKS = "GREEKS"
    IMPLIED_VOLATILITY = "IMPLIED_VOLATILITY"
    OPEN_INTEREST = "OPEN_INTEREST"
    VOLUME = "VOLUME"
    CROSS_FIELD_CONSISTENCY = "CROSS_FIELD_CONSISTENCY"


class AxisStatus(StrEnum):
    PRESENT = "PRESENT"
    MISSING = "MISSING"
    INVALID = "INVALID"
    NOT_EVALUATED = "NOT_EVALUATED"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


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


class SourceDiagnosticPolicy(_PolicyModel):
    policy_path: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    implementation_path: str
    implementation_file_sha256: str
    diagnostic_content_sha256: str
    diagnostic_canonical_sha256: str
    source_backtest_id: Literal["60ce7e0bec3ad2d83a4d1341e0221492"]
    chain_session_count: Literal[1201]
    valid_candidate_session_count: Literal[0]
    transport_rejected_session_count: Literal[1201]
    root_cause_status: Literal["UNRESOLVED"]
    reject_scope: Literal["UNRESOLVED_COMBINATION"]

    @field_validator("policy_path", "implementation_path")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "implementation_file_sha256",
        "diagnostic_content_sha256",
        "diagnostic_canonical_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class ProposalSafetyPolicy(_PolicyModel):
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_FINAL_TOKEN_REQUIRED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    log_data_carrier_allowed: Literal[False]
    object_store_allowed: Literal[False]
    api_cli_http_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class PerAxisCollectionProposalPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_daily_transport_per_axis_collection_proposal_policy.v1"
    ]
    policy_id: Literal["qc_qqq_options_daily_transport_per_axis_collection_proposal_v1"]
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_EXACT_PROPOSAL"]
    task_id: Literal[
        "TRADING-2529_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_EXPORT_SAFE_"
        "AGGREGATE_COLLECTION_PROPOSAL_V1"
    ]
    registration_base_repository_code_sha: str
    package_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_COLLECTION_PROPOSAL_V1"]
    package_root: str
    created_at_utc: datetime
    run_scope_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_SCOPE_V1"]
    proposal_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_PROPOSAL_V1"]
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    expected_session_count: Literal[1202]
    expected_first_session: date
    expected_last_session: date
    collector_id: Literal["codex_capability_coordinator"]
    independent_reviewer_id: Literal["project_owner"]
    authorization_expires_after_hours: Literal[168]
    authorization_single_use: Literal[True]
    authorization_invalidates_on_first_run_attempt: Literal[True]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    axis_order: tuple[Axis, ...]
    status_order: tuple[AxisStatus, ...]
    source_diagnostic: SourceDiagnosticPolicy
    safety: ProposalSafetyPolicy

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base_sha(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator("package_root")
    @classmethod
    def _package_root(cls, value: str) -> str:
        return _relative_path(value, "package_root")

    @model_validator(mode="after")
    def _exact_contract(self) -> Self:
        if self.axis_order != tuple(Axis):
            raise ValueError("axis_order must be the exact canonical axis set")
        if self.status_order != tuple(AxisStatus):
            raise ValueError("status_order must be the exact canonical status set")
        if (
            self.requested_start != date(2021, 2, 22)
            or self.requested_end != date(2025, 12, 2)
            or self.evaluated_start != self.requested_start
            or self.evaluated_end != self.requested_end
            or self.expected_first_session != self.requested_start
            or self.expected_last_session != self.requested_end
        ):
            raise ValueError("primary research window drifted")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))


@dataclass(frozen=True)
class LoadedPerAxisCollectionProposalPolicy:
    policy: PerAxisCollectionProposalPolicy
    path: Path
    file_sha256: str
    canonical_sha256: str


class PerAxisCollectionRunScope(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_transport_per_axis_run_scope.v1"]
    run_scope_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_SCOPE_V1"]
    created_at_utc: datetime
    repository_code_sha: str
    source_diagnostic_content_sha256: str
    source_diagnostic_canonical_sha256: str
    source_backtest_id: Literal["60ce7e0bec3ad2d83a4d1341e0221492"]
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    session_ids: tuple[date, ...]
    axes: tuple[Axis, ...]
    statuses: tuple[AxisStatus, ...]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    output_granularity: Literal["PER_AXIS_SESSION_COUNT_AGGREGATES_ONLY"]
    individual_contract_values_allowed: Literal[False]
    raw_option_rows_allowed: Literal[False]
    log_data_carrier_allowed: Literal[False]
    object_store_allowed: Literal[False]
    api_cli_http_allowed: Literal[False]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("repository_code_sha")
    @classmethod
    def _repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "source_diagnostic_content_sha256", "source_diagnostic_canonical_sha256"
    )
    @classmethod
    def _source_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _exact_scope(self) -> Self:
        if self.axes != tuple(Axis) or self.statuses != tuple(AxisStatus):
            raise ValueError("axis/status inventory drifted")
        if (
            len(self.session_ids) != 1202
            or self.session_ids[0] != self.requested_start
            or self.session_ids[-1] != self.requested_end
            or len(set(self.session_ids)) != len(self.session_ids)
            or tuple(sorted(self.session_ids)) != self.session_ids
        ):
            raise ValueError("XNYS session inventory drifted")
        return self


class PerAxisCollectionProposal(_SealedModel):
    schema_version: Literal["qc_qqq_options_daily_transport_per_axis_proposal.v1"]
    proposal_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_PROPOSAL_V1"]
    task_id: Literal[
        "TRADING-2529_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_EXPORT_SAFE_"
        "AGGREGATE_COLLECTION_PROPOSAL_V1"
    ]
    issued_at_utc: datetime
    run_scope_content_sha256: str
    run_scope_canonical_sha256: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    source_diagnostic_policy_file_sha256: str
    source_diagnostic_policy_canonical_sha256: str
    source_diagnostic_content_sha256: str
    source_diagnostic_canonical_sha256: str
    project_code_lf_sha256: str
    project_code_lf_byte_count: int
    axis_output_keys: tuple[str, ...]
    reason_code_contract: tuple[str, ...]
    allowed_actions_after_owner_token: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    expected_owner_token_template: str
    authorization_expires_after_hours: Literal[168]
    authorization_single_use: Literal[True]
    authorization_invalidates_on_first_run_attempt: Literal[True]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_FINAL_TOKEN_REQUIRED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    selection_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    raw_option_rows_allowed: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator(
        "run_scope_content_sha256",
        "run_scope_canonical_sha256",
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "source_diagnostic_policy_file_sha256",
        "source_diagnostic_policy_canonical_sha256",
        "source_diagnostic_content_sha256",
        "source_diagnostic_canonical_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _exact_proposal(self) -> Self:
        expected_keys = tuple(
            f"TRADING2529_{axis.value}_{status.value}_SESSIONS"
            for axis in Axis
            for status in AxisStatus
        )
        if self.axis_output_keys != expected_keys:
            raise ValueError("axis output key inventory drifted")
        if self.reason_code_contract != (
            "AXIS_PRESENT_VALID_VALUE_OBSERVED",
            "AXIS_MISSING_NO_VALUE_OBSERVED",
            "AXIS_INVALID_VALUE_OBSERVED_BUT_REJECTED",
            "AXIS_NOT_EVALUATED_CHAIN_ABSENT_OR_SESSION_NOT_OBSERVED",
        ):
            raise ValueError("typed reason code contract drifted")
        if len(set(self.allowed_actions_after_owner_token)) != len(
            self.allowed_actions_after_owner_token
        ):
            raise ValueError("allowed action inventory contains duplicates")
        if len(set(self.prohibited_actions)) != len(self.prohibited_actions):
            raise ValueError("prohibited action inventory contains duplicates")
        return self


class PackageArtifact(_FrozenModel):
    role: str
    relative_path: str
    sha256: str
    byte_count: int

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "relative_path")

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class PerAxisCollectionProposalPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_daily_transport_per_axis_collection_proposal_package.v1"
    ]
    package_id: Literal["TRADING_2529_DAILY_TRANSPORT_PER_AXIS_COLLECTION_PROPOSAL_V1"]
    created_at_utc: datetime
    repository_code_sha: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    source_diagnostic_policy_file_sha256: str
    source_diagnostic_policy_canonical_sha256: str
    source_diagnostic_implementation_file_sha256: str
    source_diagnostic_content_sha256: str
    source_diagnostic_canonical_sha256: str
    run_scope_content_sha256: str
    run_scope_canonical_sha256: str
    proposal_content_sha256: str
    proposal_canonical_sha256: str
    project_code_lf_sha256: str
    project_code_lf_byte_count: int
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    session_count: Literal[1202]
    axis_count: Literal[8]
    status_count: Literal[4]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_FINAL_TOKEN_REQUIRED"]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    artifacts: tuple[PackageArtifact, ...]

    @field_validator("repository_code_sha")
    @classmethod
    def _repository_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "source_diagnostic_policy_file_sha256",
        "source_diagnostic_policy_canonical_sha256",
        "source_diagnostic_implementation_file_sha256",
        "source_diagnostic_content_sha256",
        "source_diagnostic_canonical_sha256",
        "run_scope_content_sha256",
        "run_scope_canonical_sha256",
        "proposal_content_sha256",
        "proposal_canonical_sha256",
        "project_code_lf_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _artifact_inventory(self) -> Self:
        expected = tuple((role, path) for role, path in _MANIFEST_ARTIFACTS)
        actual = tuple((item.role, item.relative_path) for item in self.artifacts)
        if actual != expected:
            raise ValueError("manifest artifact inventory drifted")
        return self


@dataclass(frozen=True)
class BuiltPerAxisCollectionProposalPackage:
    policy_load: LoadedPerAxisCollectionProposalPolicy
    run_scope: PerAxisCollectionRunScope
    proposal: PerAxisCollectionProposal
    project_code_bytes: bytes
    owner_decision_request_bytes: bytes
    manifest: PerAxisCollectionProposalPackageManifest


def load_per_axis_collection_proposal_policy(
    policy_path: Path = DEFAULT_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedPerAxisCollectionProposalPolicy:
    root = project_root.resolve()
    try:
        resolved = _bound_file(policy_path, root=root, field="proposal policy", must_exist=True)
        raw = resolved.read_bytes()
        payload = safe_load_yaml_path(resolved)
        if not isinstance(payload, dict):
            raise TypeError("proposal policy root must be a mapping")
        policy = PerAxisCollectionProposalPolicy.model_validate(payload, strict=False)
        source = policy.source_diagnostic
        source_policy_path = _bound_file(
            Path(source.policy_path), root=root, field="source diagnostic policy", must_exist=True
        )
        source_load = diagnostic_v1.load_daily_transport_axis_diagnostic_policy(
            source_policy_path
        )
        source_implementation = _bound_file(
            Path(source.implementation_path),
            root=root,
            field="source diagnostic implementation",
            must_exist=True,
        )
        diagnostic = diagnostic_v1.build_repository_daily_transport_per_axis_diagnostic(
            project_root=root
        )
        source_axes = tuple(
            (item.axis.value, item.status.value) for item in diagnostic.axes
        )
        expected_axes = ((Axis.OPTION_CHAIN_PRESENCE.value, "PRESENT"),) + tuple(
            (axis.value, "NOT_EVALUATED") for axis in tuple(Axis)[1:]
        )
        if (
            source_load.file_sha256 != source.policy_file_sha256
            or source_load.canonical_sha256 != source.policy_canonical_sha256
            or _sha256_bytes(source_implementation.read_bytes())
            != source.implementation_file_sha256
            or diagnostic.content_sha256 != source.diagnostic_content_sha256
            or diagnostic.canonical_sha256 != source.diagnostic_canonical_sha256
            or diagnostic.source_backtest_id != source.source_backtest_id
            or diagnostic.chain_session_count != source.chain_session_count
            or diagnostic.valid_candidate_session_count
            != source.valid_candidate_session_count
            or diagnostic.transport_rejected_session_count
            != source.transport_rejected_session_count
            or diagnostic.root_cause_status != source.root_cause_status
            or diagnostic.reject_scope.value != source.reject_scope
            or source_axes != expected_axes
            or diagnostic.further_cloud_run_authorized is not False
            or diagnostic.raw_option_rows_consumed is not False
        ):
            raise ValueError("2528 diagnostic authority drifted")
    except PerAxisCollectionProposalError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise PerAxisCollectionProposalError(
            "PER_AXIS_PROPOSAL_POLICY_AUTHORITY_MISMATCH", str(exc)
        ) from exc
    return LoadedPerAxisCollectionProposalPolicy(
        policy=policy,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(policy.canonical_bytes),
    )


def _render_project_code(scope: PerAxisCollectionRunScope) -> bytes:
    sessions = repr(tuple(item.isoformat() for item in scope.session_ids))
    identity = (
        "schema=qc_qqq_options_daily_transport_per_axis_runtime.v1"
        f"|scope={scope.content_sha256}"
        f"|repository={scope.repository_code_sha}"
        f"|source_diagnostic={scope.source_diagnostic_content_sha256}"
    )
    template = '''from AlgorithmImports import *
from datetime import datetime
import math

# TRADING-2529 candidate only: zero-order, per-axis aggregate counters.
# No contract identifiers/values leave memory; no logs, Object Store, or network.
EXPECTED_SESSIONS = __SESSIONS__
AXES = (
    "OPTION_CHAIN_PRESENCE",
    "UNDERLYING_PRICE",
    "BID_ASK_QUOTE",
    "GREEKS",
    "IMPLIED_VOLATILITY",
    "OPEN_INTEREST",
    "VOLUME",
    "CROSS_FIELD_CONSISTENCY",
)
STATUSES = ("PRESENT", "MISSING", "INVALID", "NOT_EVALUATED")
IDENTITY = "__IDENTITY__"


class QQQOptionsDailyTransportPerAxisAggregateCollector(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021, 2, 22)
        self.set_end_date(2025, 12, 2)
        self.set_cash(100000)
        self.settings.daily_precise_end_time = True
        self.set_time_zone(TimeZones.NEW_YORK)
        self._equity = self.add_equity(
            "QQQ", Resolution.DAILY, data_normalization_mode=DataNormalizationMode.RAW
        ).symbol
        option = self.add_option("QQQ", Resolution.DAILY)
        option.set_filter(
            lambda universe: universe.contracts(
                lambda contracts: [contract.symbol for contract in contracts]
            )
        )
        self._option = option.symbol
        self._expected = set(EXPECTED_SESSIONS)
        self._seen = set()
        self._counts = {
            axis: {status: 0 for status in STATUSES}
            for axis in AXES
        }
        self._order_event_count = 0

    @staticmethod
    def _finite(value):
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    @staticmethod
    def _attribute(item, *names):
        for name in names:
            if hasattr(item, name):
                return getattr(item, name)
        return None

    @staticmethod
    def _classify(values, valid):
        observed = [value for value in values if value is not None]
        if not observed:
            return "MISSING"
        return "PRESENT" if any(valid(value) for value in observed) else "INVALID"

    def _increment(self, axis, status):
        self._counts[axis][status] += 1

    def on_data(self, data: Slice):
        session_id = data.time.date().isoformat()
        if session_id not in self._expected or session_id in self._seen:
            return
        self._seen.add(session_id)
        chain = data.option_chains.get(self._option)
        if chain is None or len(chain) == 0:
            self._increment("OPTION_CHAIN_PRESENCE", "MISSING")
            for axis in AXES[1:]:
                self._increment(axis, "NOT_EVALUATED")
            return
        contracts = list(chain)
        self._increment("OPTION_CHAIN_PRESENCE", "PRESENT")

        underlying = [
            self._finite(self._attribute(item, "underlying_last_price"))
            for item in contracts
        ]
        bids = [self._finite(self._attribute(item, "bid_price", "bidprice")) for item in contracts]
        asks = [self._finite(self._attribute(item, "ask_price", "askprice")) for item in contracts]
        ivs = [self._finite(self._attribute(item, "implied_volatility")) for item in contracts]
        ois = [self._finite(self._attribute(item, "open_interest", "openinterest")) for item in contracts]
        volumes = [self._finite(self._attribute(item, "volume")) for item in contracts]

        self._increment(
            "UNDERLYING_PRICE", self._classify(underlying, lambda value: value > 0)
        )
        quote_pairs = [
            pair for pair in zip(bids, asks)
            if pair[0] is not None or pair[1] is not None
        ]
        self._increment(
            "BID_ASK_QUOTE",
            "MISSING" if not quote_pairs else (
                "PRESENT" if any(
                    pair[0] is not None and pair[1] is not None
                    and pair[0] >= 0 and pair[1] > 0 and pair[1] >= pair[0]
                    for pair in quote_pairs
                ) else "INVALID"
            ),
        )

        greek_vectors = []
        for item in contracts:
            greeks = self._attribute(item, "greeks")
            if greeks is None:
                greek_vectors.append(None)
                continue
            greek_vectors.append(tuple(
                self._finite(self._attribute(greeks, name))
                for name in ("delta", "gamma", "vega", "theta", "rho")
            ))
        self._increment(
            "GREEKS",
            self._classify(
                greek_vectors,
                lambda vector: all(value is not None for value in vector),
            ),
        )
        self._increment(
            "IMPLIED_VOLATILITY", self._classify(ivs, lambda value: value >= 0)
        )
        self._increment("OPEN_INTEREST", self._classify(ois, lambda value: value >= 0))
        self._increment("VOLUME", self._classify(volumes, lambda value: value >= 0))

        consistent = []
        session = data.time.date()
        for index, item in enumerate(contracts):
            strike = self._finite(self._attribute(item, "strike"))
            if strike is None and hasattr(item, "symbol"):
                strike = self._finite(item.symbol.id.strike_price)
            expiry = self._attribute(item, "expiry")
            if expiry is None and hasattr(item, "symbol"):
                expiry = item.symbol.id.date
            expiry_date = expiry.date() if hasattr(expiry, "date") else expiry
            consistent.append(
                underlying[index] is not None and underlying[index] > 0
                and strike is not None and strike > 0
                and hasattr(expiry_date, "year") and expiry_date >= session
                and bids[index] is not None and asks[index] is not None
                and bids[index] >= 0 and asks[index] > 0 and asks[index] >= bids[index]
            )
        self._increment(
            "CROSS_FIELD_CONSISTENCY",
            "PRESENT" if any(consistent) else "INVALID",
        )

    def on_order_event(self, order_event):
        self._order_event_count += 1

    def on_end_of_algorithm(self):
        missing_sessions = len(self._expected - self._seen)
        self._counts["OPTION_CHAIN_PRESENCE"]["MISSING"] += missing_sessions
        for axis in AXES[1:]:
            self._counts[axis]["NOT_EVALUATED"] += missing_sessions
        for axis in AXES:
            for status in STATUSES:
                self.set_runtime_statistic(
                    "TRADING2529_" + axis + "_" + status + "_SESSIONS",
                    str(self._counts[axis][status]),
                )
        valid_counts = all(
            sum(self._counts[axis].values()) == len(self._expected)
            for axis in AXES
        )
        terminal = "COMPLETE" if (
            valid_counts and self._order_event_count == 0 and not self.portfolio.invested
        ) else "INVALID"
        self.set_runtime_statistic("TRADING2529_IDENTITY", IDENTITY)
        self.set_runtime_statistic(
            "TRADING2529_TERMINAL",
            "status=" + terminal
            + "|expected_sessions=" + str(len(self._expected))
            + "|observed_sessions=" + str(len(self._seen))
            + "|orders=0|fills=0|portfolio_invested=false"
            + "|raw_rows=false|logs_as_data=false|object_store=false",
        )
'''
    text = template.replace("__SESSIONS__", sessions).replace("__IDENTITY__", identity)
    return text.encode("utf-8")


def _owner_decision_request_bytes(
    *,
    loaded: LoadedPerAxisCollectionProposalPolicy,
    scope: PerAxisCollectionRunScope,
    proposal: PerAxisCollectionProposal,
) -> bytes:
    policy = loaded.policy
    allowed = "\n".join(
        f"- `{item}`" for item in proposal.allowed_actions_after_owner_token
    )
    prohibited = "\n".join(f"- `{item}`" for item in proposal.prohibited_actions)
    token_lines = (
        proposal.expected_owner_token_template,
        "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>",
        f"registration_base_repository_code_sha:{scope.repository_code_sha}",
        f"proposal_policy_file_sha256:{loaded.file_sha256}",
        f"proposal_policy_canonical_sha256:{loaded.canonical_sha256}",
        f"source_diagnostic_content_sha256:{scope.source_diagnostic_content_sha256}",
        f"source_diagnostic_canonical_sha256:{scope.source_diagnostic_canonical_sha256}",
        f"run_scope_content_sha256:{scope.content_sha256}",
        f"run_scope_canonical_sha256:{scope.canonical_sha256}",
        f"proposal_content_sha256:{proposal.content_sha256}",
        f"proposal_canonical_sha256:{proposal.canonical_sha256}",
        f"project_code_lf_sha256:{proposal.project_code_lf_sha256}",
        f"target_project_id:{scope.target_project_id}",
        f"requested_range:{scope.requested_start.isoformat()}..{scope.requested_end.isoformat()}",
        f"expected_session_count:{len(scope.session_ids)}",
        "maximum_project_mutations:1",
        "maximum_cloud_backtests:1",
        "maximum_orders:0",
        "maximum_fills:0",
        f"collector:{policy.collector_id}",
        f"independent_reviewer:{policy.independent_reviewer_id}",
        "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>",
        "authorization_single_use:true",
        "authorization_invalidates_on_first_run_attempt:true",
    )
    text = "\n".join(
        (
            "# TRADING-2529 Owner Decision Request",
            "",
            "状态：`OWNER_FINAL_TOKEN_REQUIRED`；当前 `external_action=none`。",
            "",
            "这只是逐轴 export-safe session/count aggregate 采集提案，不是 Cloud、raw rows、",
            "交易、订单、DQ/PIT、策略有效性或投资结论授权。",
            "",
            "## 为什么需要这一步",
            "",
            "2528 只能确认 option chain 到达，但 underlying、bid/ask、Greeks、IV、OI、volume",
            "与 cross-field consistency 七轴都没有独立计数，因此 1201 个 combined-gate rejects",
            "仍无法定位。候选运行只补齐每轴 PRESENT/MISSING/INVALID/NOT_EVALUATED 会话计数。",
            "",
            "## Exact scope",
            "",
            f"- repository base：`{scope.repository_code_sha}`",
            f"- target project：`{scope.target_project_id}`",
            f"- range / XNYS sessions：`{scope.requested_start}..{scope.requested_end}` / `{len(scope.session_ids)}`",
            f"- axes：`{', '.join(axis.value for axis in scope.axes)}`",
            f"- run scope content / file SHA-256：`{scope.content_sha256}` / `{scope.canonical_sha256}`",
            f"- proposal content / file SHA-256：`{proposal.content_sha256}` / `{proposal.canonical_sha256}`",
            f"- project code LF SHA-256 / bytes：`{proposal.project_code_lf_sha256}` / `{proposal.project_code_lf_byte_count}`",
            "",
            "## 后续 Owner token 最多允许的动作",
            "",
            allowed,
            "",
            "## 持续禁止",
            "",
            prohibited,
            "",
            "## Owner token template（当前未签署）",
            "",
            "```text",
            "\n".join(token_lines),
            "```",
            "",
            "只有 ordinary-pushed exact main、全部 hashes 与 expiry 均匹配后，Owner 对完整文本的",
            "再次明确确认才可能进入独立后继 admission；本 package 自身没有执行能力。",
            "",
        )
    )
    return text.encode("utf-8")


def _artifact(role: str, relative_path: str, raw: bytes) -> PackageArtifact:
    return PackageArtifact(
        role=role,
        relative_path=relative_path,
        sha256=_sha256_bytes(raw),
        byte_count=len(raw),
    )


def build_per_axis_collection_proposal_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltPerAxisCollectionProposalPackage:
    root = project_root.resolve()
    loaded = load_per_axis_collection_proposal_policy(project_root=root)
    policy = loaded.policy
    source = diagnostic_v1.build_repository_daily_transport_per_axis_diagnostic(
        project_root=root
    )
    session_scope = session_v1.build_qc_qqq_options_derived_aggregate_collector_run_scope(
        run_scope_id="TRADING_2529_DAILY_TRANSPORT_PER_AXIS_SCOPE_V1",
        created_at_utc=policy.created_at_utc,
        repository_code_sha=policy.registration_base_repository_code_sha,
        target_project_id=policy.target_project_id,
        requested_end=policy.requested_end,
        project_root=root,
    )
    if (
        len(session_scope.session_ids) != policy.expected_session_count
        or session_scope.session_ids[0] != policy.expected_first_session
        or session_scope.session_ids[-1] != policy.expected_last_session
    ):
        raise PerAxisCollectionProposalError(
            "PER_AXIS_PROPOSAL_SESSION_INVENTORY_MISMATCH",
            "primary XNYS session inventory differs from proposal policy",
        )
    scope = PerAxisCollectionRunScope.seal(
        schema_version="qc_qqq_options_daily_transport_per_axis_run_scope.v1",
        run_scope_id=policy.run_scope_id,
        created_at_utc=policy.created_at_utc,
        repository_code_sha=policy.registration_base_repository_code_sha,
        source_diagnostic_content_sha256=source.content_sha256,
        source_diagnostic_canonical_sha256=source.canonical_sha256,
        source_backtest_id=policy.source_diagnostic.source_backtest_id,
        target_project_id=policy.target_project_id,
        requested_start=policy.requested_start,
        requested_end=policy.requested_end,
        evaluated_start=policy.evaluated_start,
        evaluated_end=policy.evaluated_end,
        primary_research_role=policy.primary_research_role,
        exchange_calendar=policy.exchange_calendar,
        session_ids=session_scope.session_ids,
        axes=policy.axis_order,
        statuses=policy.status_order,
        maximum_project_mutations=policy.maximum_project_mutations,
        maximum_cloud_backtests=policy.maximum_cloud_backtests,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        result_carrier=policy.result_carrier,
        output_granularity="PER_AXIS_SESSION_COUNT_AGGREGATES_ONLY",
        individual_contract_values_allowed=False,
        raw_option_rows_allowed=False,
        log_data_carrier_allowed=False,
        object_store_allowed=False,
        api_cli_http_allowed=False,
        authorization_status=policy.safety.authorization_status,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
    )
    project_code = _render_project_code(scope)
    axis_keys = tuple(
        f"TRADING2529_{axis.value}_{status.value}_SESSIONS"
        for axis in Axis
        for status in AxisStatus
    )
    allowed = (
        "INTERACTIVE_LOGIN_TO_EXISTING_QUANTCONNECT_ACCOUNT",
        "MUTATE_EXISTING_PROJECT_34808569_ONCE",
        "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
        "MANUALLY_DOWNLOAD_ONE_RESULTS_JSON",
        "COLLECT_PER_AXIS_SESSION_COUNT_AGGREGATES_ONLY",
    )
    prohibited = (
        "ANY_ACTION_BEFORE_EXACT_OWNER_TOKEN_ADMISSION",
        "RAW_OPTION_ROWS_OR_INDIVIDUAL_CONTRACT_VALUES",
        "LOGS_AS_DATA_OR_OBJECT_STORE_EXPORT",
        "API_CLI_HTTP_OR_BACKGROUND_NETWORK_PATH",
        "SECOND_PROJECT_MUTATION_OR_SECOND_CLOUD_RUN",
        "ANY_ORDER_FILL_PAPER_LIVE_BROKER_OR_PRODUCTION_ACTION",
        "DQ_PIT_SELECTION_ENGINE_STRATEGY_OR_INVESTMENT_CONCLUSION",
    )
    proposal = PerAxisCollectionProposal.seal(
        schema_version="qc_qqq_options_daily_transport_per_axis_proposal.v1",
        proposal_id=policy.proposal_id,
        task_id=policy.task_id,
        issued_at_utc=policy.created_at_utc,
        run_scope_content_sha256=scope.content_sha256,
        run_scope_canonical_sha256=scope.canonical_sha256,
        proposal_policy_file_sha256=loaded.file_sha256,
        proposal_policy_canonical_sha256=loaded.canonical_sha256,
        source_diagnostic_policy_file_sha256=policy.source_diagnostic.policy_file_sha256,
        source_diagnostic_policy_canonical_sha256=(
            policy.source_diagnostic.policy_canonical_sha256
        ),
        source_diagnostic_content_sha256=source.content_sha256,
        source_diagnostic_canonical_sha256=source.canonical_sha256,
        project_code_lf_sha256=_sha256_bytes(project_code),
        project_code_lf_byte_count=len(project_code),
        axis_output_keys=axis_keys,
        reason_code_contract=(
            "AXIS_PRESENT_VALID_VALUE_OBSERVED",
            "AXIS_MISSING_NO_VALUE_OBSERVED",
            "AXIS_INVALID_VALUE_OBSERVED_BUT_REJECTED",
            "AXIS_NOT_EVALUATED_CHAIN_ABSENT_OR_SESSION_NOT_OBSERVED",
        ),
        allowed_actions_after_owner_token=allowed,
        prohibited_actions=prohibited,
        expected_owner_token_template=(
            "owner_decision:TRADING-2529:<YYYY-MM-DD>:"
            "authorize_single_zero_order_daily_transport_per_axis_"
            "export_safe_aggregate_collection_v1"
        ),
        authorization_expires_after_hours=policy.authorization_expires_after_hours,
        authorization_single_use=policy.authorization_single_use,
        authorization_invalidates_on_first_run_attempt=(
            policy.authorization_invalidates_on_first_run_attempt
        ),
        maximum_project_mutations=policy.maximum_project_mutations,
        maximum_cloud_backtests=policy.maximum_cloud_backtests,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        authorization_status=policy.safety.authorization_status,
        decision=policy.safety.decision,
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        selection_authorized=False,
        engine_status=policy.safety.engine_status,
        raw_option_rows_allowed=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )
    request = _owner_decision_request_bytes(loaded=loaded, scope=scope, proposal=proposal)
    raw_artifacts: Mapping[str, bytes] = {
        "main.py": project_code,
        "owner_decision_request.md": request,
        "proposal.json": proposal.canonical_bytes,
        "run_scope.json": scope.canonical_bytes,
    }
    artifacts = tuple(
        _artifact(role, path, raw_artifacts[path]) for role, path in _MANIFEST_ARTIFACTS
    )
    manifest = PerAxisCollectionProposalPackageManifest.seal(
        schema_version=(
            "qc_qqq_options_daily_transport_per_axis_collection_proposal_package.v1"
        ),
        package_id=policy.package_id,
        created_at_utc=policy.created_at_utc,
        repository_code_sha=scope.repository_code_sha,
        proposal_policy_file_sha256=loaded.file_sha256,
        proposal_policy_canonical_sha256=loaded.canonical_sha256,
        source_diagnostic_policy_file_sha256=(
            policy.source_diagnostic.policy_file_sha256
        ),
        source_diagnostic_policy_canonical_sha256=(
            policy.source_diagnostic.policy_canonical_sha256
        ),
        source_diagnostic_implementation_file_sha256=(
            policy.source_diagnostic.implementation_file_sha256
        ),
        source_diagnostic_content_sha256=source.content_sha256,
        source_diagnostic_canonical_sha256=source.canonical_sha256,
        run_scope_content_sha256=scope.content_sha256,
        run_scope_canonical_sha256=scope.canonical_sha256,
        proposal_content_sha256=proposal.content_sha256,
        proposal_canonical_sha256=proposal.canonical_sha256,
        project_code_lf_sha256=proposal.project_code_lf_sha256,
        project_code_lf_byte_count=proposal.project_code_lf_byte_count,
        target_project_id=scope.target_project_id,
        requested_start=scope.requested_start,
        requested_end=scope.requested_end,
        session_count=len(scope.session_ids),
        axis_count=len(scope.axes),
        status_count=len(scope.statuses),
        maximum_project_mutations=scope.maximum_project_mutations,
        maximum_cloud_backtests=scope.maximum_cloud_backtests,
        maximum_orders=scope.maximum_orders,
        maximum_fills=scope.maximum_fills,
        authorization_status=proposal.authorization_status,
        decision=proposal.decision,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
        artifacts=artifacts,
    )
    return BuiltPerAxisCollectionProposalPackage(
        policy_load=loaded,
        run_scope=scope,
        proposal=proposal,
        project_code_bytes=project_code,
        owner_decision_request_bytes=request,
        manifest=manifest,
    )


def write_per_axis_collection_proposal_package(
    *, project_root: Path = PROJECT_ROOT
) -> PerAxisCollectionProposalPackageManifest:
    root = project_root.resolve()
    built = build_per_axis_collection_proposal_package(project_root=root)
    package_root = _bound_file(
        Path(built.policy_load.policy.package_root) / "package_manifest.json",
        root=root,
        field="proposal package manifest target",
        must_exist=False,
    ).parent
    package_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
        "proposal.json": built.proposal.canonical_bytes,
        "run_scope.json": built.run_scope.canonical_bytes,
    }
    for name in _PACKAGE_FILES:
        write_bytes_atomic(package_root / name, payloads[name])
    return built.manifest


def load_per_axis_collection_proposal_package(
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> BuiltPerAxisCollectionProposalPackage:
    root = project_root.resolve()
    expected = build_per_axis_collection_proposal_package(project_root=root)
    try:
        manifest_path = _bound_file(
            package_root / "package_manifest.json",
            root=root,
            field="proposal package manifest",
            must_exist=True,
        )
        actual_root = manifest_path.parent
        if actual_root.relative_to(root).as_posix() != expected.policy_load.policy.package_root:
            raise ValueError("package root differs from reviewed policy")
        inventory = tuple(sorted(path.name for path in actual_root.iterdir()))
        if inventory != _PACKAGE_FILES:
            raise ValueError("package file inventory is not exact")
        if any(not path.is_file() or path.is_symlink() for path in actual_root.iterdir()):
            raise ValueError("package entries must be non-symlink regular files")
        raw = {name: (actual_root / name).read_bytes() for name in _PACKAGE_FILES}
        scope = PerAxisCollectionRunScope.from_json_bytes(raw["run_scope.json"])
        proposal = PerAxisCollectionProposal.from_json_bytes(raw["proposal.json"])
        manifest = PerAxisCollectionProposalPackageManifest.from_json_bytes(
            raw["package_manifest.json"]
        )
        if scope != expected.run_scope or proposal != expected.proposal:
            raise ValueError("scope/proposal semantic identity drifted")
        if raw["main.py"] != expected.project_code_bytes:
            raise ValueError("project code bytes drifted")
        if raw["owner_decision_request.md"] != expected.owner_decision_request_bytes:
            raise ValueError("owner decision request bytes drifted")
        if manifest != expected.manifest:
            raise ValueError("package manifest semantic identity drifted")
        for artifact in manifest.artifacts:
            artifact_raw = raw[artifact.relative_path]
            if (
                _sha256_bytes(artifact_raw) != artifact.sha256
                or len(artifact_raw) != artifact.byte_count
            ):
                raise ValueError(f"artifact identity drifted: {artifact.relative_path}")
    except (OSError, TypeError, ValueError) as exc:
        raise PerAxisCollectionProposalError(
            "PER_AXIS_PROPOSAL_PACKAGE_ADMISSION_FAILED", str(exc)
        ) from exc
    return BuiltPerAxisCollectionProposalPackage(
        policy_load=expected.policy_load,
        run_scope=scope,
        proposal=proposal,
        project_code_bytes=raw["main.py"],
        owner_decision_request_bytes=raw["owner_decision_request.md"],
        manifest=manifest,
    )


__all__ = [
    "DEFAULT_PACKAGE_ROOT",
    "DEFAULT_POLICY_PATH",
    "Axis",
    "AxisStatus",
    "BuiltPerAxisCollectionProposalPackage",
    "PackageArtifact",
    "PerAxisCollectionProposal",
    "PerAxisCollectionProposalError",
    "PerAxisCollectionProposalPackageManifest",
    "PerAxisCollectionProposalPolicy",
    "PerAxisCollectionRunScope",
    "build_per_axis_collection_proposal_package",
    "load_per_axis_collection_proposal_package",
    "load_per_axis_collection_proposal_policy",
    "write_per_axis_collection_proposal_package",
]
