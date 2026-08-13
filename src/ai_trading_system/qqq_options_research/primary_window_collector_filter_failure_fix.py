from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_run_proposal as proposal_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_collector_filter_failure_fix_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/"
    "trading_2518_primary_window_collector_filter_failure_fix_v1"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_BACKTEST_ID = re.compile(r"^[0-9a-f]{32}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_PACKAGE_FILES = (
    "failure_receipt.json",
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
)
_MANIFEST_ARTIFACTS = (
    ("FAILED_RUN_RECEIPT", "failure_receipt.json"),
    ("CORRECTED_QC_PROJECT_CODE", "main.py"),
    ("UNSIGNED_OWNER_REAUTHORIZATION_REQUEST", "owner_decision_request.md"),
)
_FAILED_SELECTOR = (
    "        option.set_filter("
    "lambda universe: universe.contracts(lambda symbols: symbols))\n"
)
_CORRECTED_SELECTOR = (
    "        option.set_filter(\n"
    "            lambda universe: universe.contracts(\n"
    "                lambda contracts: [contract.symbol for contract in contracts]\n"
    "            )\n"
    "        )\n"
)
_PROHIBITED_THRESHOLD_FRAGMENTS = (
    ".delta(",
    ".expiration(",
    ".include_weeklys(",
    ".iron_condor(",
    ".iron_butterfly(",
    ".strangle(",
    ".straddle(",
    ".call_spread(",
    ".put_spread(",
)


class QCQQQOptionsCollectorFilterFailureFixError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}: {detail}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode()


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def hook(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    def reject_constant(value: str) -> object:
        raise ValueError(f"non-finite JSON constant is prohibited: {value}")

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=hook,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _sha256(value: str, field: str) -> str:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase full Git SHA")
    return value


def _identifier(value: str, field: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"{field} must be a stable identifier")
    return value


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if (
        not value
        or path.is_absolute()
        or path.drive
        or ".." in path.parts
        or path.as_posix() != value
    ):
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _bound_file(path: Path, *, root: Path, field: str) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    if not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    return candidate


class HistoricalCollectorAuthority(_PolicyModel):
    proposal_policy_path: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    proposal_package_root: str
    proposal_package_manifest_file_sha256: str
    proposal_package_manifest_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    project_code_path: str
    project_code_lf_sha256: str
    refresh_admission_policy_path: str
    refresh_admission_policy_file_sha256: str
    refresh_admission_implementation_path: str
    refresh_admission_implementation_file_sha256: str

    @field_validator(
        "proposal_policy_path",
        "proposal_package_root",
        "project_code_path",
        "refresh_admission_policy_path",
        "refresh_admission_implementation_path",
    )
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator(
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "proposal_package_manifest_file_sha256",
        "proposal_package_manifest_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
        "refresh_admission_policy_file_sha256",
        "refresh_admission_implementation_file_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class FailedRunAuthority(_PolicyModel):
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    refresh_candidate_content_sha256: str
    collector_authorization_content_sha256: str
    refresh_admission_receipt_content_sha256: str
    external_action_ledger_content_sha256: str
    run_attempt_consumption_content_sha256: str
    target_project_id: Literal[34808569]
    backtest_id: str
    run_attempted_at_utc: datetime
    lifecycle_status: Literal["FAILED"]
    scope_status: Literal["FAIL"]
    reason_code: Literal["QC_RUNTIME_OPTION_FILTER_CASTING_ERROR"]
    error_type: Literal["InvalidCastException"]
    error_message: str
    error_source_line: Literal[
        "option.set_filter(lambda universe: universe.contracts(lambda symbols: symbols))"
    ]
    attempted_project_mutations: Literal[1]
    attempted_cloud_backtests: Literal[1]
    completed_results_downloads: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    authorization_consumed: Literal[True]
    authorization_invalidated_for_further_runs: Literal[True]
    evidence_collection_completed: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED"]

    @field_validator(
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "refresh_candidate_content_sha256",
        "collector_authorization_content_sha256",
        "refresh_admission_receipt_content_sha256",
        "external_action_ledger_content_sha256",
        "run_attempt_consumption_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("backtest_id")
    @classmethod
    def _backtest_id(cls, value: str) -> str:
        if not _BACKTEST_ID.fullmatch(value):
            raise ValueError("backtest_id must be a lowercase 32-character hex identifier")
        return value

    @field_validator("run_attempted_at_utc")
    @classmethod
    def _run_at(cls, value: datetime) -> datetime:
        return _utc(value, "run_attempted_at_utc")


class CollectorFilterFixSafety(_PolicyModel):
    owner_reauthorization_status: Literal["OWNER_REAUTHORIZATION_NOT_PROVIDED"]
    decision: Literal["OWNER_REAUTHORIZATION_REQUIRED"]
    executable_policy_authorized: Literal[False]
    selection_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    owner_policy_value_count: Literal[0]
    external_action_performed_by_task: Literal[False]
    second_cloud_backtest_allowed: Literal[False]
    investment_interpretation_generated: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCQQQOptionsCollectorFilterFailureFixPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_collector_filter_failure_fix_policy.v1"
    ]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REAUTHORIZATION_REQUIRED_CORRECTED_PROPOSAL"]
    task_id: Literal[
        "TRADING-2518_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_AND_REAUTHORIZATION_V1"
    ]
    registration_base_repository_code_sha: str
    created_at_utc: datetime
    package_id: str
    package_root: str
    implementation_path: str
    implementation_file_sha256: str
    target_project_id: Literal[34808569]
    requested_start: Literal["2021-02-22"]
    requested_end: Literal["2025-12-02"]
    evaluated_start: Literal["2021-02-22"]
    evaluated_end: Literal["2025-12-02"]
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    expected_session_count: Literal[1202]
    maximum_project_mutations_for_new_authorization: Literal[1]
    maximum_cloud_backtests_for_new_authorization: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    filter_semantics: Literal["SELECT_ALL_EXPLICIT_SYMBOL_LIST_NO_POLICY_THRESHOLD"]
    expected_owner_decision_token: str
    historical_authority: HistoricalCollectorAuthority
    failed_run: FailedRunAuthority
    safety: CollectorFilterFixSafety

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base_sha(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator("created_at_utc")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("package_root", "implementation_path")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator("implementation_file_sha256")
    @classmethod
    def _implementation_hash(cls, value: str) -> str:
        return _sha256(value, "implementation_file_sha256")

    @field_validator("policy_id", "package_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _authority(self) -> Self:
        if self.package_root != (
            DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT.as_posix()
        ):
            raise ValueError("package_root drifted")
        expected_token = (
            "owner_decision:TRADING-2518:<YYYY-MM-DD>:"
            "authorize_single_zero_order_primary_window_derived_aggregate_collection_v3"
        )
        if self.expected_owner_decision_token != expected_token:
            raise ValueError("successor Owner decision token template drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult:
    policy: QCQQQOptionsCollectorFilterFailureFixPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_hash(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.semantic_payload())).hexdigest()

    @model_validator(mode="after")
    def _seal(self, info: ValidationInfo) -> Self:
        if info.context and info.context.get("allow_unsealed"):
            if self.content_sha256 == _UNSEALED_SHA256:
                return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("semantic content SHA-256 mismatch")
        return self

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: object) -> Self:
        try:
            candidate = cls.model_validate(
                {**payload, "content_sha256": _UNSEALED_SHA256},
                context={"allow_unsealed": True},
            )
            return cls.model_validate(
                {**payload, "content_sha256": candidate.compute_content_sha256()}
            )
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsCollectorFilterFailureFixError(
                "COLLECTOR_FILTER_FIX_PAYLOAD_INVALID", str(exc)
            ) from exc

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            payload = _duplicate_key_rejecting_json(raw)
            if not isinstance(payload, dict):
                raise TypeError("record JSON root must be an object")
            value = cls.model_validate_json(raw)
            if raw != value.canonical_bytes:
                raise ValueError("record is not canonical JSON bytes")
            return value
        except (TypeError, ValueError) as exc:
            raise QCQQQOptionsCollectorFilterFailureFixError(
                "COLLECTOR_FILTER_FIX_RECORD_INVALID", str(exc)
            ) from exc


class CollectorFilterFailedRunReceipt(_SealedModel):
    schema_version: Literal["qc_qqq_options_collector_failed_run_receipt.v1"]
    receipt_id: str
    observed_at_utc: datetime
    project_id: Literal[34808569]
    backtest_id: str
    project_code_lf_sha256: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    external_action_ledger_content_sha256: str
    run_attempt_consumption_content_sha256: str
    lifecycle_status: Literal["FAILED"]
    scope_status: Literal["FAIL"]
    reason_code: Literal["QC_RUNTIME_OPTION_FILTER_CASTING_ERROR"]
    error_type: Literal["InvalidCastException"]
    error_message: str
    error_source_line: str
    attempted_project_mutations: Literal[1]
    attempted_cloud_backtests: Literal[1]
    completed_results_downloads: Literal[0]
    orders: Literal[0]
    fills: Literal[0]
    authorization_consumed: Literal[True]
    authorization_invalidated_for_further_runs: Literal[True]
    evidence_collection_completed: Literal[False]
    dq_pit_status: Literal["NOT_EVALUATED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("receipt_id")
    @classmethod
    def _receipt_id(cls, value: str) -> str:
        return _identifier(value, "receipt_id")

    @field_validator("observed_at_utc")
    @classmethod
    def _observed_at(cls, value: datetime) -> datetime:
        return _utc(value, "observed_at_utc")

    @field_validator("backtest_id")
    @classmethod
    def _backtest(cls, value: str) -> str:
        if not _BACKTEST_ID.fullmatch(value):
            raise ValueError("backtest_id must be a lowercase 32-character hex identifier")
        return value

    @field_validator(
        "project_code_lf_sha256",
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "external_action_ledger_content_sha256",
        "run_attempt_consumption_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class CollectorFilterFixPackageArtifact(_StrictModel):
    role: str
    relative_path: str
    sha256: str
    byte_count: int = Field(ge=1)

    @field_validator("role")
    @classmethod
    def _role(cls, value: str) -> str:
        return _identifier(value, "artifact.role")

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "artifact.relative_path")

    @field_validator("sha256")
    @classmethod
    def _artifact_hash(cls, value: str) -> str:
        return _sha256(value, "artifact.sha256")


class CollectorFilterFixPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_collector_filter_failure_fix_package.v1"
    ]
    package_id: str
    created_at_utc: datetime
    repository_code_sha: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    implementation_file_sha256: str
    predecessor_project_code_lf_sha256: str
    corrected_project_code_lf_sha256: str
    corrected_project_code_lf_byte_count: int = Field(ge=1)
    failure_receipt_content_sha256: str
    failure_receipt_canonical_sha256: str
    target_project_id: Literal[34808569]
    requested_start: Literal["2021-02-22"]
    requested_end: Literal["2025-12-02"]
    evaluated_start: Literal["2021-02-22"]
    evaluated_end: Literal["2025-12-02"]
    expected_session_count: Literal[1202]
    filter_semantics: Literal["SELECT_ALL_EXPLICIT_SYMBOL_LIST_NO_POLICY_THRESHOLD"]
    owner_reauthorization_status: Literal["OWNER_REAUTHORIZATION_NOT_PROVIDED"]
    decision: Literal["OWNER_REAUTHORIZATION_REQUIRED"]
    owner_policy_value_count: Literal[0]
    selection_authorized: Literal[False]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    external_action_performed_by_task: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    artifacts: tuple[CollectorFilterFixPackageArtifact, ...]

    @field_validator("package_id")
    @classmethod
    def _package_id(cls, value: str) -> str:
        return _identifier(value, "package_id")

    @field_validator("created_at_utc")
    @classmethod
    def _created(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _repository(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "implementation_file_sha256",
        "predecessor_project_code_lf_sha256",
        "corrected_project_code_lf_sha256",
        "failure_receipt_content_sha256",
        "failure_receipt_canonical_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _inventory(self) -> Self:
        if tuple(item.role for item in self.artifacts) != tuple(
            role for role, _ in _MANIFEST_ARTIFACTS
        ):
            raise ValueError("manifest artifact role inventory drifted")
        if tuple(item.relative_path for item in self.artifacts) != tuple(
            path for _, path in _MANIFEST_ARTIFACTS
        ):
            raise ValueError("manifest artifact path inventory drifted")
        if len({item.relative_path for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("manifest artifact paths must be unique")
        return self


@dataclass(frozen=True)
class BuiltQCQQQOptionsCollectorFilterFailureFixPackage:
    policy_load: QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult
    failure_receipt: CollectorFilterFailedRunReceipt
    corrected_project_code_bytes: bytes
    owner_decision_request_bytes: bytes
    manifest: CollectorFilterFixPackageManifest


def _corrected_project_code(
    *, policy: QCQQQOptionsCollectorFilterFailureFixPolicy, project_root: Path
) -> bytes:
    source_path = _bound_file(
        Path(policy.historical_authority.project_code_path),
        root=project_root,
        field="historical project code",
    )
    source = source_path.read_bytes()
    if b"\r\n" in source:
        raise ValueError("historical project code must use LF bytes")
    if hashlib.sha256(source).hexdigest() != (
        policy.historical_authority.project_code_lf_sha256
    ):
        raise ValueError("historical project code identity drifted")
    text = source.decode("utf-8")
    if text.count(_FAILED_SELECTOR) != 1:
        raise ValueError("historical failing selector is not unique")
    corrected = text.replace(_FAILED_SELECTOR, _CORRECTED_SELECTOR, 1)
    if _FAILED_SELECTOR in corrected or corrected.count(_CORRECTED_SELECTOR) != 1:
        raise ValueError("corrected selector replacement failed")
    lowered = corrected.lower()
    if any(fragment in lowered for fragment in _PROHIBITED_THRESHOLD_FRAGMENTS):
        raise ValueError("corrected project code introduced a policy threshold fragment")
    if "contract.symbol for contract in contracts" not in corrected:
        raise ValueError("corrected selector does not return explicit Symbol values")
    return corrected.encode("utf-8")


def load_qc_qqq_options_collector_filter_failure_fix_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _bound_file(path, root=root, field="collector filter fix policy")
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("collector filter fix policy root must be a mapping")
        policy = QCQQQOptionsCollectorFilterFailureFixPolicy.model_validate(payload)

        implementation = _bound_file(
            Path(policy.implementation_path),
            root=root,
            field="collector filter fix implementation",
        )
        if hashlib.sha256(implementation.read_bytes()).hexdigest() != (
            policy.implementation_file_sha256
        ):
            raise ValueError("collector filter fix implementation identity drifted")

        predecessor = proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
            project_root=root
        )
        authority = policy.historical_authority
        predecessor_manifest = _bound_file(
            Path(authority.proposal_package_root) / "package_manifest.json",
            root=root,
            field="historical proposal package manifest",
        )
        refresh_policy = _bound_file(
            Path(authority.refresh_admission_policy_path),
            root=root,
            field="refresh admission policy",
        )
        refresh_implementation = _bound_file(
            Path(authority.refresh_admission_implementation_path),
            root=root,
            field="refresh admission implementation",
        )
        expected = (
            predecessor.policy_load.policy_path.relative_to(root).as_posix(),
            predecessor.policy_load.policy_file_sha256,
            predecessor.policy_load.policy_canonical_sha256,
            predecessor.manifest.content_sha256,
            predecessor.proposal.content_sha256,
            predecessor.run_scope.content_sha256,
            predecessor.manifest.project_code_lf_sha256,
        )
        actual = (
            authority.proposal_policy_path,
            authority.proposal_policy_file_sha256,
            authority.proposal_policy_canonical_sha256,
            authority.proposal_package_manifest_content_sha256,
            authority.proposal_content_sha256,
            authority.run_scope_content_sha256,
            authority.project_code_lf_sha256,
        )
        if actual != expected:
            raise ValueError("2513 historical proposal authority drifted")
        if hashlib.sha256(predecessor_manifest.read_bytes()).hexdigest() != (
            authority.proposal_package_manifest_file_sha256
        ):
            raise ValueError("2513 historical package manifest file drifted")
        if hashlib.sha256(refresh_policy.read_bytes()).hexdigest() != (
            authority.refresh_admission_policy_file_sha256
        ):
            raise ValueError("2517 refresh admission policy file drifted")
        if hashlib.sha256(refresh_implementation.read_bytes()).hexdigest() != (
            authority.refresh_admission_implementation_file_sha256
        ):
            raise ValueError("2517 refresh admission implementation file drifted")
        _corrected_project_code(policy=policy, project_root=root)
    except QCQQQOptionsCollectorFilterFailureFixError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectorFilterFailureFixError(
            "COLLECTOR_FILTER_FIX_POLICY_AUTHORITY_MISMATCH", str(exc)
        ) from exc
    return QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _owner_decision_request_bytes(
    *,
    policy_load: QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult,
    corrected_code_sha256: str,
    failure_receipt: CollectorFilterFailedRunReceipt,
) -> bytes:
    policy = policy_load.policy
    token = "\n".join(
        (
            policy.expected_owner_decision_token,
            "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>",
            f"registration_base_repository_code_sha:{policy.registration_base_repository_code_sha}",
            "failure_fix_policy_file_sha256:"
            f"{policy_load.policy_file_sha256}",
            "failure_fix_policy_canonical_sha256:"
            f"{policy_load.policy_canonical_sha256}",
            "failure_fix_package_manifest_file_sha256:"
            "<CORRECTED_PACKAGE_MANIFEST_FILE_SHA256>",
            "failure_fix_package_manifest_content_sha256:"
            "<CORRECTED_PACKAGE_MANIFEST_CONTENT_SHA256>",
            f"corrected_project_code_lf_sha256:{corrected_code_sha256}",
            f"failed_backtest_id:{policy.failed_run.backtest_id}",
            "failed_run_receipt_content_sha256:"
            f"{failure_receipt.content_sha256}",
            "previous_run_attempt_consumption_content_sha256:"
            f"{policy.failed_run.run_attempt_consumption_content_sha256}",
            f"target_project_id:{policy.target_project_id}",
            f"requested_range:{policy.requested_start}..{policy.requested_end}",
            f"expected_session_count:{policy.expected_session_count}",
            "maximum_project_mutations:1",
            "maximum_cloud_backtests:1",
            "maximum_orders:0",
            "maximum_fills:0",
            "collector:codex_capability_coordinator",
            "independent_reviewer:project_owner",
            "authorization_expires_at_utc:<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_168_HOURS>",
            "authorization_single_use:true",
            "authorization_invalidates_after_first_run_attempt:true",
        )
    )
    return (
        "# TRADING-2518 corrected collector reauthorization request\n\n"
        "该文件只是未签署模板，不构成授权。2516 v2 token 已在失败 run 中消耗，"
        "不得复用。\n\n"
        "修复只把 Option universe callback 转成显式 `list[Symbol]`；不引入 DTE、"
        "delta、moneyness、spread、OI、volume 或其他策略阈值。\n\n"
        "```text\n"
        f"{token}\n"
        "```\n\n"
        "在 Project Owner 提供完整 exact v3 token 前：不得修改 QuantConnect project、"
        "不得运行第二次 Cloud backtest、不得宣称 evidence/DQ/PIT PASS，engine 保持 "
        "`POLICY_BLOCKED_CASH_PRESERVATION`。\n"
    ).encode()


def build_qc_qqq_options_collector_filter_failure_fix_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltQCQQQOptionsCollectorFilterFailureFixPackage:
    root = project_root.resolve()
    policy_load = load_qc_qqq_options_collector_filter_failure_fix_policy(
        project_root=root
    )
    policy = policy_load.policy
    failed = policy.failed_run
    failure_receipt = CollectorFilterFailedRunReceipt.seal(
        schema_version="qc_qqq_options_collector_failed_run_receipt.v1",
        receipt_id="TRADING_2518_QC_FAILED_RUN_RECEIPT_V1",
        observed_at_utc=failed.run_attempted_at_utc,
        project_id=failed.target_project_id,
        backtest_id=failed.backtest_id,
        project_code_lf_sha256=policy.historical_authority.project_code_lf_sha256,
        owner_decision_file_sha256=failed.owner_decision_file_sha256,
        owner_decision_content_sha256=failed.owner_decision_content_sha256,
        external_action_ledger_content_sha256=(
            failed.external_action_ledger_content_sha256
        ),
        run_attempt_consumption_content_sha256=(
            failed.run_attempt_consumption_content_sha256
        ),
        lifecycle_status=failed.lifecycle_status,
        scope_status=failed.scope_status,
        reason_code=failed.reason_code,
        error_type=failed.error_type,
        error_message=failed.error_message,
        error_source_line=failed.error_source_line,
        attempted_project_mutations=failed.attempted_project_mutations,
        attempted_cloud_backtests=failed.attempted_cloud_backtests,
        completed_results_downloads=failed.completed_results_downloads,
        orders=failed.orders,
        fills=failed.fills,
        authorization_consumed=failed.authorization_consumed,
        authorization_invalidated_for_further_runs=(
            failed.authorization_invalidated_for_further_runs
        ),
        evidence_collection_completed=failed.evidence_collection_completed,
        dq_pit_status=failed.dq_pit_status,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        production_effect="none",
        broker_action="none",
    )
    corrected_code = _corrected_project_code(policy=policy, project_root=root)
    corrected_code_sha = hashlib.sha256(corrected_code).hexdigest()
    owner_request = _owner_decision_request_bytes(
        policy_load=policy_load,
        corrected_code_sha256=corrected_code_sha,
        failure_receipt=failure_receipt,
    )
    artifact_payloads = {
        "failure_receipt.json": failure_receipt.canonical_bytes,
        "main.py": corrected_code,
        "owner_decision_request.md": owner_request,
    }
    artifacts = tuple(
        CollectorFilterFixPackageArtifact(
            role=role,
            relative_path=name,
            sha256=hashlib.sha256(artifact_payloads[name]).hexdigest(),
            byte_count=len(artifact_payloads[name]),
        )
        for role, name in _MANIFEST_ARTIFACTS
    )
    manifest = CollectorFilterFixPackageManifest.seal(
        schema_version=(
            "qc_qqq_options_primary_window_collector_filter_failure_fix_package.v1"
        ),
        package_id=policy.package_id,
        created_at_utc=policy.created_at_utc,
        repository_code_sha=policy.registration_base_repository_code_sha,
        policy_file_sha256=policy_load.policy_file_sha256,
        policy_canonical_sha256=policy_load.policy_canonical_sha256,
        implementation_file_sha256=policy.implementation_file_sha256,
        predecessor_project_code_lf_sha256=(
            policy.historical_authority.project_code_lf_sha256
        ),
        corrected_project_code_lf_sha256=corrected_code_sha,
        corrected_project_code_lf_byte_count=len(corrected_code),
        failure_receipt_content_sha256=failure_receipt.content_sha256,
        failure_receipt_canonical_sha256=failure_receipt.canonical_sha256,
        target_project_id=policy.target_project_id,
        requested_start=policy.requested_start,
        requested_end=policy.requested_end,
        evaluated_start=policy.evaluated_start,
        evaluated_end=policy.evaluated_end,
        expected_session_count=policy.expected_session_count,
        filter_semantics=policy.filter_semantics,
        owner_reauthorization_status=policy.safety.owner_reauthorization_status,
        decision=policy.safety.decision,
        owner_policy_value_count=policy.safety.owner_policy_value_count,
        selection_authorized=policy.safety.selection_authorized,
        executable_policy_authorized=policy.safety.executable_policy_authorized,
        engine_status=policy.safety.engine_status,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        external_action_performed_by_task=(
            policy.safety.external_action_performed_by_task
        ),
        investment_interpretation_generated=(
            policy.safety.investment_interpretation_generated
        ),
        production_effect=policy.safety.production_effect,
        broker_action=policy.safety.broker_action,
        artifacts=artifacts,
    )
    return BuiltQCQQQOptionsCollectorFilterFailureFixPackage(
        policy_load=policy_load,
        failure_receipt=failure_receipt,
        corrected_project_code_bytes=corrected_code,
        owner_decision_request_bytes=owner_request,
        manifest=manifest,
    )


def write_qc_qqq_options_collector_filter_failure_fix_package(
    package_root: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> CollectorFilterFixPackageManifest:
    root = project_root.resolve()
    built = build_qc_qqq_options_collector_filter_failure_fix_package(project_root=root)
    target = root / package_root
    target.mkdir(parents=True, exist_ok=True)
    payloads = {
        "failure_receipt.json": built.failure_receipt.canonical_bytes,
        "main.py": built.corrected_project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": built.manifest.canonical_bytes,
    }
    for name in _PACKAGE_FILES:
        write_bytes_atomic(target / name, payloads[name])
    return built.manifest


def load_qc_qqq_options_collector_filter_failure_fix_package(
    package_root: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> BuiltQCQQQOptionsCollectorFilterFailureFixPackage:
    root = project_root.resolve()
    expected = build_qc_qqq_options_collector_filter_failure_fix_package(
        project_root=root
    )
    try:
        manifest_path = _bound_file(
            package_root / "package_manifest.json",
            root=root,
            field="collector filter fix package manifest",
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
        receipt = CollectorFilterFailedRunReceipt.from_json_bytes(
            raw["failure_receipt.json"]
        )
        manifest = CollectorFilterFixPackageManifest.from_json_bytes(
            raw["package_manifest.json"]
        )
        if receipt != expected.failure_receipt:
            raise ValueError("failure receipt semantic identity drifted")
        if raw["main.py"] != expected.corrected_project_code_bytes:
            raise ValueError("corrected project code bytes drifted")
        if raw["owner_decision_request.md"] != expected.owner_decision_request_bytes:
            raise ValueError("owner decision request bytes drifted")
        if manifest != expected.manifest:
            raise ValueError("package manifest semantic identity drifted")
        for artifact in manifest.artifacts:
            value = raw[artifact.relative_path]
            if (
                hashlib.sha256(value).hexdigest() != artifact.sha256
                or len(value) != artifact.byte_count
            ):
                raise ValueError(f"artifact identity drifted: {artifact.relative_path}")
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsCollectorFilterFailureFixError(
            "COLLECTOR_FILTER_FIX_PACKAGE_ADMISSION_FAILED", str(exc)
        ) from exc
    return BuiltQCQQQOptionsCollectorFilterFailureFixPackage(
        policy_load=expected.policy_load,
        failure_receipt=receipt,
        corrected_project_code_bytes=raw["main.py"],
        owner_decision_request_bytes=raw["owner_decision_request.md"],
        manifest=manifest,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_PACKAGE_ROOT",
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_COLLECTOR_FILTER_FAILURE_FIX_POLICY_PATH",
    "BuiltQCQQQOptionsCollectorFilterFailureFixPackage",
    "CollectorFilterFailedRunReceipt",
    "CollectorFilterFixPackageArtifact",
    "CollectorFilterFixPackageManifest",
    "QCQQQOptionsCollectorFilterFailureFixError",
    "QCQQQOptionsCollectorFilterFailureFixPolicy",
    "QCQQQOptionsCollectorFilterFailureFixPolicyLoadResult",
    "build_qc_qqq_options_collector_filter_failure_fix_package",
    "load_qc_qqq_options_collector_filter_failure_fix_package",
    "load_qc_qqq_options_collector_filter_failure_fix_policy",
    "write_qc_qqq_options_collector_filter_failure_fix_package",
]
