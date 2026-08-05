from __future__ import annotations

import hashlib
import json
import re
import textwrap
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import sha256_path
from ai_trading_system.qqq_options_research.bounded_cloud_pilot_owner_review import (
    QQQOptionsBoundedPilotOwnerReviewProposalLoadResult,
    QQQOptionsBoundedPilotProposalAccountingScope,
    QQQOptionsBoundedPilotProposalExecutionScope,
    QQQOptionsBoundedPilotProposalLifecycleScope,
    QQQOptionsBoundedPilotProposalPlatformScope,
    QQQOptionsBoundedPilotProposalReconciliationScope,
    QQQOptionsBoundedPilotProposalResearchWindow,
    QQQOptionsBoundedPilotProposalSelectionScope,
    load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_PATH = Path(
    "config/research/"
    "qc_qqq_options_bounded_cloud_pilot_platform_action_authorization_v1.yaml"
)
OWNER_AUTHORIZATION_ID = (
    "owner_decision:TRADING-2492:2026-08-05:"
    "authorize_single_bounded_qc_free_cloud_pilot_v1"
)
AUTHORIZATION_TASK_ID = (
    "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1"
)
EXPECTED_PROPOSAL_POLICY_SHA256 = (
    "9b3e50731663871e01626f0360c717ecdd14278c63f81e74ed79c4c2fd4041de"
)
EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256 = (
    "69578c198823b95ba16b5f6c2780c3a7e24104babe2c6cc1fed8cd740c446bea"
)
ALLOWED_ACTIONS: tuple[str, ...] = (
    "CLOUD_BACKTEST",
    "DEDICATED_PROJECT_CREATE_OR_MODIFY",
    "EXPORT_SAFE_MANUAL_EVIDENCE_COLLECTION",
    "QUANTCONNECT_LOGIN",
)
PROHIBITED_ACTIONS: tuple[str, ...] = (
    "API",
    "BROKER",
    "CLI",
    "DIRECT_HTTP",
    "LIVE",
    "OBJECT_STORE",
    "PAPER",
    "PRODUCTION",
    "RAW_OPTIONS_DATA_DOWNLOAD",
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")


class QCBoundedCloudPilotPlatformActionContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or not _TEXT_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a portable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase 40-character Git SHA")
    return value


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _canonical_sha256(payload: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _validate_content_sha256(cls, value: str) -> str:
        return _sha256(value, "content_sha256")

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        semantic = cls.model_validate(
            {**payload, "content_sha256": "0" * 64}
        ).semantic_payload()
        return cls.model_validate(
            {**payload, "content_sha256": _canonical_sha256(semantic)}
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("record JSON root must be an object")
        record = cls.model_validate(decoded)
        if record.canonical_bytes != raw:
            raise ValueError("record bytes are not canonical")
        expected = _canonical_sha256(record.semantic_payload())
        if record.content_sha256 != expected:
            raise ValueError("record semantic content SHA-256 mismatch")
        return record


class QCBoundedCloudPilotActors(_StrictModel):
    collector_id: str
    independent_reviewer_id: str
    two_person_attestation_required: Literal[True]

    @field_validator("collector_id", "independent_reviewer_id")
    @classmethod
    def _validate_actor(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_separation(self) -> Self:
        if self.collector_id == self.independent_reviewer_id:
            raise ValueError("collector and independent reviewer must differ")
        return self


class QCBoundedCloudPilotAuthorizationSafety(_StrictModel):
    authorization_overlay_only: Literal[True]
    project_mutation_allowed: Literal[True]
    cloud_backtest_allowed: Literal[True]
    simulated_long_call_order_allowed: Literal[True]
    real_broker_order_allowed: Literal[False]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    direct_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_download_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_action_allowed: Literal[False]
    production_allowed: Literal[False]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCBoundedCloudPilotPlatformActionAuthorizationPolicy(_StrictModel):
    schema_version: Literal[
        "qc_qqq_options_bounded_cloud_pilot_platform_action_authorization_policy.v1"
    ]
    policy_id: Literal[
        "qc_qqq_options_bounded_cloud_pilot_platform_action_authorization_v1"
    ]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEWED_ACTIVE_PRE_RUN"]
    authorization_task_id: Literal[
        "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1"
    ]
    owner_authorization_id: Literal[
        "owner_decision:TRADING-2492:2026-08-05:"
        "authorize_single_bounded_qc_free_cloud_pilot_v1"
    ]
    authorization_effective_date: date
    authorization_expires_at_utc: datetime
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    platform: Literal["QuantConnect"]
    run_role: Literal["BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"]
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    revocation_condition: str
    proposal_policy_path: str
    proposal_policy_sha256: str
    proposal_authority_set_sha256: str
    research_window: QQQOptionsBoundedPilotProposalResearchWindow
    platform_scope: QQQOptionsBoundedPilotProposalPlatformScope
    selection_scope: QQQOptionsBoundedPilotProposalSelectionScope
    execution_scope: QQQOptionsBoundedPilotProposalExecutionScope
    accounting_scope: QQQOptionsBoundedPilotProposalAccountingScope
    lifecycle_scope: QQQOptionsBoundedPilotProposalLifecycleScope
    reconciliation_scope: QQQOptionsBoundedPilotProposalReconciliationScope
    actors: QCBoundedCloudPilotActors
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    safety: QCBoundedCloudPilotAuthorizationSafety
    decision: Literal["AUTHORIZED_SINGLE_BOUNDED_QC_CLOUD_PILOT_PRE_RUN"]

    @field_validator(
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "revocation_condition",
        "proposal_policy_path",
    )
    @classmethod
    def _validate_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator("proposal_policy_sha256", "proposal_authority_set_sha256")
    @classmethod
    def _validate_hash(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("authorization_expires_at_utc")
    @classmethod
    def _validate_expiry(cls, value: datetime) -> datetime:
        return _utc(value, "authorization_expires_at_utc")

    @field_validator("allowed_actions")
    @classmethod
    def _validate_allowed(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory drifted")
        return value

    @field_validator("prohibited_actions")
    @classmethod
    def _validate_prohibited(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory drifted")
        return value

    @model_validator(mode="after")
    def _validate_fixed_authorization(self) -> Self:
        if self.authorization_effective_date != date(2026, 8, 5):
            raise ValueError("authorization effective date drifted")
        if self.authorization_expires_at_utc != datetime(
            2026, 8, 12, tzinfo=UTC
        ):
            raise ValueError("authorization expiry drifted")
        if self.proposal_policy_sha256 != EXPECTED_PROPOSAL_POLICY_SHA256:
            raise ValueError("proposal policy hash differs from Owner decision")
        if (
            self.proposal_authority_set_sha256
            != EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256
        ):
            raise ValueError("proposal authority-set hash differs from Owner decision")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.semantic_payload())

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True)
class QCBoundedCloudPilotPlatformActionAuthorizationLoadResult:
    policy: QCBoundedCloudPilotPlatformActionAuthorizationPolicy
    policy_path: Path
    policy_sha256: str
    policy_canonical_sha256: str
    proposal: QQQOptionsBoundedPilotOwnerReviewProposalLoadResult


@dataclass(frozen=True)
class QCBoundedCloudPilotProjectSourceArtifact:
    file_name: Literal["main.py"]
    algorithm_class: Literal["QQQOptionsBoundedPilot"]
    source_bytes: bytes
    source_sha256: str
    byte_count: int


class QCBoundedCloudPilotPreRunAuthorizationRecord(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_bounded_cloud_pilot_pre_run_authorization_record.v1"
    ]
    record_id: str
    created_at_utc: datetime
    repository_code_sha: str
    authorization_task_id: Literal[
        "TRADING-2492_QC_QQQ_OPTIONS_BOUNDED_PLATFORM_ACTION_AUTHORIZATION_V1"
    ]
    owner_authorization_id: Literal[
        "owner_decision:TRADING-2492:2026-08-05:"
        "authorize_single_bounded_qc_free_cloud_pilot_v1"
    ]
    authorization_policy_sha256: str
    authorization_policy_canonical_sha256: str
    proposal_policy_sha256: str
    proposal_authority_set_sha256: str
    project_file_name: Literal["main.py"]
    project_algorithm_class: Literal["QQQOptionsBoundedPilot"]
    project_source_sha256: str
    project_source_byte_count: int
    requested_start: date
    requested_end: date
    authorization_expires_at_utc: datetime
    authorization_state: Literal["ACTIVE_PRE_RUN_NOT_CONSUMED"]
    project_mutation_count: Literal[0]
    cloud_backtest_count: Literal[0]
    order_count: Literal[0]
    fill_count: Literal[0]
    option_event_dq_status: Literal["NOT_EVALUATED_PRE_RUN"]
    option_event_pit_status: Literal["NOT_EVALUATED_PRE_RUN"]
    external_action_executed: Literal[False]
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    decision: Literal["AUTHORIZED_SINGLE_BOUNDED_QC_CLOUD_PILOT_PRE_RUN"]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("created_at_utc", "authorization_expires_at_utc")
    @classmethod
    def _validate_timestamp(cls, value: datetime, info: Any) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_code_sha(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "authorization_policy_sha256",
        "authorization_policy_canonical_sha256",
        "proposal_policy_sha256",
        "proposal_authority_set_sha256",
        "project_source_sha256",
    )
    @classmethod
    def _validate_sha(cls, value: str, info: Any) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("project_source_byte_count")
    @classmethod
    def _validate_project_size(cls, value: int) -> int:
        if value <= 0 or value > 32768:
            raise ValueError("project source must fit the reviewed 32768-byte boundary")
        return value

    @field_validator("allowed_actions")
    @classmethod
    def _validate_record_allowed(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != ALLOWED_ACTIONS:
            raise ValueError("record allowed actions drifted")
        return value

    @field_validator("prohibited_actions")
    @classmethod
    def _validate_record_prohibited(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != PROHIBITED_ACTIONS:
            raise ValueError("record prohibited actions drifted")
        return value

    @model_validator(mode="after")
    def _validate_record_window(self) -> Self:
        if self.requested_start != date(2025, 12, 2) or self.requested_end != date(
            2025, 12, 2
        ):
            raise ValueError("pre-run record requested range drifted")
        if self.created_at_utc > self.authorization_expires_at_utc:
            raise ValueError("pre-run record was created after authorization expiry")
        return self


def load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
    path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCBoundedCloudPilotPlatformActionAuthorizationLoadResult:
    resolved_root = project_root.resolve()
    resolved_policy = resolved_root / path
    try:
        resolved_policy = _require_bound_regular_file(
            path,
            project_root=resolved_root,
            field="platform action authorization policy",
        )
        payload = safe_load_yaml_path(resolved_policy)
        if not isinstance(payload, dict):
            raise TypeError("authorization policy root must be a mapping")
        policy = QCBoundedCloudPilotPlatformActionAuthorizationPolicy.model_validate(
            payload
        )
        proposal = load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
            Path(policy.proposal_policy_path), project_root=resolved_root
        )
        _verify_proposal_binding(policy=policy, proposal=proposal)
    except QCBoundedCloudPilotPlatformActionContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCBoundedCloudPilotPlatformActionContractError(
            "QC_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_INVALID",
            f"{resolved_policy}: {exc}",
        ) from exc
    return QCBoundedCloudPilotPlatformActionAuthorizationLoadResult(
        policy=policy,
        policy_path=resolved_policy,
        policy_sha256=sha256_path(resolved_policy),
        policy_canonical_sha256=policy.canonical_sha256,
        proposal=proposal,
    )


def build_qc_qqq_options_bounded_cloud_pilot_project_source(
    *,
    repository_code_sha: str,
    authorization_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_PATH
    ),
    project_root: Path = PROJECT_ROOT,
) -> QCBoundedCloudPilotProjectSourceArtifact:
    checked_repository_sha = _git_sha(repository_code_sha, "repository_code_sha")
    loaded = load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
        authorization_path, project_root=project_root
    )
    replacements = {
        "__OWNER_AUTHORIZATION_ID__": loaded.policy.owner_authorization_id,
        "__AUTHORIZATION_POLICY_SHA256__": loaded.policy_sha256,
        "__PROPOSAL_POLICY_SHA256__": loaded.policy.proposal_policy_sha256,
        "__REPOSITORY_CODE_SHA__": checked_repository_sha,
    }
    source = _QC_PROJECT_TEMPLATE
    for marker, value in replacements.items():
        if marker not in source:
            raise QCBoundedCloudPilotPlatformActionContractError(
                "QC_BOUNDED_CLOUD_PILOT_PROJECT_TEMPLATE_INVALID",
                f"missing project template marker: {marker}",
            )
        source = source.replace(marker, value)
    source_bytes = source.encode("utf-8")
    if len(source_bytes) > 32768:
        raise QCBoundedCloudPilotPlatformActionContractError(
            "QC_BOUNDED_CLOUD_PILOT_PROJECT_TOO_LARGE",
            f"main.py bytes={len(source_bytes)} exceed 32768",
        )
    return QCBoundedCloudPilotProjectSourceArtifact(
        file_name="main.py",
        algorithm_class="QQQOptionsBoundedPilot",
        source_bytes=source_bytes,
        source_sha256=hashlib.sha256(source_bytes).hexdigest(),
        byte_count=len(source_bytes),
    )


def build_qc_qqq_options_bounded_cloud_pilot_pre_run_record(
    *,
    record_id: str,
    created_at_utc: datetime,
    repository_code_sha: str,
    authorization_path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_PATH
    ),
    project_root: Path = PROJECT_ROOT,
) -> QCBoundedCloudPilotPreRunAuthorizationRecord:
    created = _utc(created_at_utc, "created_at_utc")
    loaded = load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
        authorization_path, project_root=project_root
    )
    if created < datetime.combine(
        loaded.policy.authorization_effective_date,
        datetime.min.time(),
        tzinfo=UTC,
    ):
        raise QCBoundedCloudPilotPlatformActionContractError(
            "QC_BOUNDED_CLOUD_PILOT_AUTHORIZATION_NOT_YET_EFFECTIVE",
            created.isoformat(),
        )
    if created > loaded.policy.authorization_expires_at_utc:
        raise QCBoundedCloudPilotPlatformActionContractError(
            "QC_BOUNDED_CLOUD_PILOT_AUTHORIZATION_EXPIRED",
            created.isoformat(),
        )
    project = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=repository_code_sha,
        authorization_path=authorization_path,
        project_root=project_root,
    )
    return QCBoundedCloudPilotPreRunAuthorizationRecord.seal(
        schema_version=(
            "qc_qqq_options_bounded_cloud_pilot_pre_run_authorization_record.v1"
        ),
        record_id=record_id,
        created_at_utc=created,
        repository_code_sha=repository_code_sha,
        authorization_task_id=loaded.policy.authorization_task_id,
        owner_authorization_id=loaded.policy.owner_authorization_id,
        authorization_policy_sha256=loaded.policy_sha256,
        authorization_policy_canonical_sha256=loaded.policy_canonical_sha256,
        proposal_policy_sha256=loaded.policy.proposal_policy_sha256,
        proposal_authority_set_sha256=(
            loaded.policy.proposal_authority_set_sha256
        ),
        project_file_name=project.file_name,
        project_algorithm_class=project.algorithm_class,
        project_source_sha256=project.source_sha256,
        project_source_byte_count=project.byte_count,
        requested_start=loaded.policy.research_window.requested_start,
        requested_end=loaded.policy.research_window.requested_end,
        authorization_expires_at_utc=loaded.policy.authorization_expires_at_utc,
        authorization_state="ACTIVE_PRE_RUN_NOT_CONSUMED",
        project_mutation_count=0,
        cloud_backtest_count=0,
        order_count=0,
        fill_count=0,
        option_event_dq_status="NOT_EVALUATED_PRE_RUN",
        option_event_pit_status="NOT_EVALUATED_PRE_RUN",
        external_action_executed=False,
        allowed_actions=loaded.policy.allowed_actions,
        prohibited_actions=loaded.policy.prohibited_actions,
        decision="AUTHORIZED_SINGLE_BOUNDED_QC_CLOUD_PILOT_PRE_RUN",
        production_effect="none",
        broker_action="none",
    )


def _verify_proposal_binding(
    *,
    policy: QCBoundedCloudPilotPlatformActionAuthorizationPolicy,
    proposal: QQQOptionsBoundedPilotOwnerReviewProposalLoadResult,
) -> None:
    if proposal.proposal_policy_sha256 != policy.proposal_policy_sha256:
        raise ValueError("live proposal policy SHA-256 mismatch")
    if proposal.authority_set_sha256 != policy.proposal_authority_set_sha256:
        raise ValueError("live proposal authority-set SHA-256 mismatch")
    proposed = proposal.proposal
    scope_pairs = (
        ("research_window", policy.research_window, proposed.research_window),
        ("platform_scope", policy.platform_scope, proposed.platform_scope),
        ("selection_scope", policy.selection_scope, proposed.selection_scope),
        ("execution_scope", policy.execution_scope, proposed.execution_scope),
        ("accounting_scope", policy.accounting_scope, proposed.accounting_scope),
        ("lifecycle_scope", policy.lifecycle_scope, proposed.lifecycle_scope),
        (
            "reconciliation_scope",
            policy.reconciliation_scope,
            proposed.reconciliation_scope,
        ),
    )
    for field, authorized, reviewed in scope_pairs:
        if authorized.model_dump(mode="json") != reviewed.model_dump(mode="json"):
            raise ValueError(f"authorized {field} differs from reviewed proposal")
    if policy.actors.collector_id != proposed.evidence_scope.collector_id:
        raise ValueError("authorization collector differs from reviewed proposal")
    if (
        policy.actors.independent_reviewer_id
        != proposed.evidence_scope.independent_reviewer_id
    ):
        raise ValueError("authorization reviewer differs from reviewed proposal")
    if not proposed.safety.proposal_only or proposed.safety.pilot_authorized:
        raise ValueError("proposal predecessor safety authority drifted")
    if proposed.owner_authorization_token != "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS":
        raise ValueError("proposal predecessor token must remain unmodified")


def _require_bound_regular_file(
    path: Path,
    *,
    project_root: Path,
    field: str,
) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    if ".." in relative.parts:
        raise ValueError(f"{field} escapes the project root")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"{field} cannot use a symlink")
    if not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    resolved = candidate.resolve(strict=True)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} resolves outside the project root") from exc
    if not resolved.is_file():
        raise ValueError(f"{field} must be a regular file")
    return resolved


_QC_PROJECT_TEMPLATE = textwrap.dedent(
    '''\
    from AlgorithmImports import *
    from datetime import timedelta

    # TRADING-2492 bounded platform smoke; one simulated long-call order maximum.
    # Owner authorization: __OWNER_AUTHORIZATION_ID__
    # Authorization policy SHA-256: __AUTHORIZATION_POLICY_SHA256__
    # Proposal policy SHA-256: __PROPOSAL_POLICY_SHA256__
    # Repository authority: __REPOSITORY_CODE_SHA__
    # No API/CLI/HTTP/Object Store/raw rows/paper/live/broker/production.


    class ReviewedOptionFeeModel(FeeModel):
        def get_order_fee(self, parameters):
            fee = 0.65 * abs(float(parameters.order.quantity))
            return OrderFee(CashAmount(fee, "USD"))


    class QQQOptionsBoundedPilot(QCAlgorithm):
        def initialize(self):
            self.set_start_date(2025, 12, 2)
            self.set_end_date(2025, 12, 2)
            self.set_cash(100000)
            self.set_time_zone("America/New_York")

            equity = self.add_equity("QQQ", Resolution.MINUTE)
            equity.set_data_normalization_mode(DataNormalizationMode.RAW)
            self.qqq_symbol = equity.symbol

            option = self.add_option("QQQ", Resolution.MINUTE)
            option.set_filter(
                lambda universe: universe.include_weeklys().strikes(-50, 50).expiration(7, 21)
            )
            self.option_symbol = option.symbol

            self.set_security_initializer(self._initialize_security)
            self.selected_symbol = None
            self.intent_time = None
            self.submit_time = None
            self.ticket = None
            self.order_count = 0
            self.fill_event_count = 0
            self.filled_quantity = 0
            self.partial_fill_seen = False
            self.chronology_valid = True
            self.option_event_dq_status = "NOT_EVALUATED"
            self.option_event_pit_status = "NOT_EVALUATED"
            self.selected_identity = "NONE"
            self.last_chain_count = 0

            self.debug(
                "QC_BOUNDED_PILOT_START"
                "|schema=qc_qqq_options_bounded_pilot_log.v1"
                "|ticker=QQQ|date=2025-12-02"
                "|equity_resolution=MINUTE|option_resolution=MINUTE"
                "|orders_authorized=1|contracts_authorized=1"
                "|broker_action=false|raw_rows_logged=false"
                "|authorization_policy_sha256=__AUTHORIZATION_POLICY_SHA256__"
                "|proposal_policy_sha256=__PROPOSAL_POLICY_SHA256__"
                "|repository_code_sha=__REPOSITORY_CODE_SHA__"
            )

        def _initialize_security(self, security):
            if security.type == SecurityType.OPTION:
                security.set_fee_model(ReviewedOptionFeeModel())
                security.set_slippage_model(ConstantSlippageModel(0.01))

        def on_data(self, slice):
            chain = slice.option_chains.get(self.option_symbol)
            if chain is None:
                return
            contracts = list(chain)
            self.last_chain_count = len(contracts)
            if not contracts:
                return
            if self.time.hour < 9 or (self.time.hour == 9 and self.time.minute < 31):
                return

            if self.selected_symbol is None:
                self._select_contract(contracts)
                return

            if self.ticket is None:
                if self.time < self.intent_time + timedelta(minutes=1):
                    return
                selected = next(
                    (contract for contract in contracts if contract.symbol == self.selected_symbol),
                    None,
                )
                if selected is None or not self._valid_quote(selected):
                    self.option_event_dq_status = "FAIL"
                    self.debug(
                        "QC_BOUNDED_PILOT_NO_ORDER|reason=STALE_MISSING_OR_CROSSED_SUBMIT_QUOTE"
                    )
                    return
                limit_price = round(float(selected.ask_price) + 0.01, 2)
                premium = limit_price * 100 + 1.00
                if premium > 2000.00:
                    self.debug("QC_BOUNDED_PILOT_NO_ORDER|reason=PREMIUM_BUDGET_EXCEEDED")
                    return
                self.submit_time = self.time
                self.ticket = self.limit_order(
                    self.selected_symbol,
                    1,
                    limit_price,
                    tag="TRADING-2492|NEXT_INDEPENDENT_MINUTE|LONG_CALL|MAX_ONE_ORDER",
                )
                self.order_count = 1
                self.debug(
                    "QC_BOUNDED_PILOT_SUBMIT"
                    f"|time={self.time.isoformat()}"
                    f"|sid={self.selected_identity}"
                    f"|limit={limit_price:.2f}"
                    "|quantity=1|order_count=1"
                    "|option_event_dq=PASS|option_event_pit=PASS"
                )
                return

            if (
                self.ticket.status not in (OrderStatus.FILLED, OrderStatus.CANCELED)
                and self.time >= self.submit_time + timedelta(minutes=1)
            ):
                self.ticket.cancel("TRADING-2492 reviewed 60-second timeout")
                self.debug(
                    "QC_BOUNDED_PILOT_CANCEL"
                    f"|time={self.time.isoformat()}"
                    f"|partial_fill_seen={str(self.partial_fill_seen).lower()}"
                    "|remainder_cancelled=true"
                )

        def _select_contract(self, contracts):
            underlying_price = float(self.securities[self.qqq_symbol].price)
            if underlying_price <= 0:
                return
            eligible = []
            for contract in contracts:
                if contract.right != OptionRight.CALL or not self._valid_quote(contract):
                    continue
                dte = (contract.expiry.date() - self.time.date()).days
                if dte < 7 or dte > 21:
                    continue
                moneyness = abs(float(contract.strike) - underlying_price) / underlying_price
                if moneyness > 0.05:
                    continue
                delta = abs(float(contract.greeks.delta))
                if delta < 0.30 or delta > 0.55:
                    continue
                bid = float(contract.bid_price)
                ask = float(contract.ask_price)
                relative_spread = (ask - bid) / ((ask + bid) / 2.0)
                if relative_spread > 0.20:
                    continue
                open_interest = int(contract.open_interest)
                volume = int(contract.volume)
                if open_interest < 10 or volume < 0:
                    continue
                sid = str(contract.symbol.id)
                rank = (
                    abs(dte - 14),
                    abs(delta - 0.40),
                    relative_spread,
                    -open_interest,
                    -volume,
                    sid,
                )
                eligible.append((rank, contract, dte, delta, relative_spread))
            if not eligible:
                self.debug(
                    "QC_BOUNDED_PILOT_NO_ORDER"
                    f"|time={self.time.isoformat()}"
                    f"|chain_count={len(contracts)}"
                    "|reason=NO_ELIGIBLE_CONTRACT|cash_preservation=true"
                )
                return
            eligible.sort(key=lambda item: item[0])
            _, contract, dte, delta, spread = eligible[0]
            self.selected_symbol = contract.symbol
            self.selected_identity = str(contract.symbol.id)
            self.intent_time = self.time
            self.option_event_dq_status = "PASS"
            self.option_event_pit_status = "PASS"
            self.debug(
                "QC_BOUNDED_PILOT_INTENT"
                f"|time={self.time.isoformat()}"
                f"|sid={self.selected_identity}"
                f"|dte={dte}|abs_delta={delta:.6f}|relative_spread={spread:.6f}"
                f"|open_interest={int(contract.open_interest)}|volume={int(contract.volume)}"
                "|next_submit_not_before_seconds=60|raw_rows_logged=false"
                "|option_event_dq=PASS|option_event_pit=PASS"
            )

        def _valid_quote(self, contract):
            bid = float(contract.bid_price)
            ask = float(contract.ask_price)
            if bid <= 0 or ask <= 0 or ask < bid:
                return False
            end_time = getattr(contract, "end_time", self.time)
            if end_time < self.time - timedelta(seconds=60):
                return False
            return True

        def on_order_event(self, order_event):
            if order_event.fill_quantity == 0:
                return
            self.fill_event_count += 1
            self.filled_quantity += int(order_event.fill_quantity)
            if order_event.status == OrderStatus.PARTIALLY_FILLED:
                self.partial_fill_seen = True
            earliest_fill = self.submit_time + timedelta(minutes=1)
            if self.time < earliest_fill:
                self.chronology_valid = False
                self.option_event_dq_status = "FAIL"
            self.debug(
                "QC_BOUNDED_PILOT_FILL"
                f"|time={self.time.isoformat()}"
                f"|sid={self.selected_identity}"
                f"|status={order_event.status}"
                f"|fill_quantity={int(order_event.fill_quantity)}"
                f"|fill_price={float(order_event.fill_price):.2f}"
                f"|chronology_valid={str(self.chronology_valid).lower()}"
                f"|option_event_dq={self.option_event_dq_status}"
                f"|option_event_pit={self.option_event_pit_status}"
            )

        def on_end_of_algorithm(self):
            self.debug(
                "QC_BOUNDED_PILOT_END"
                f"|selected={str(self.selected_symbol is not None).lower()}"
                f"|order_count={self.order_count}"
                f"|fill_event_count={self.fill_event_count}"
                f"|filled_quantity={self.filled_quantity}"
                f"|portfolio_invested={str(self.portfolio.invested).lower()}"
                f"|chronology_valid={str(self.chronology_valid).lower()}"
                f"|option_event_dq={self.option_event_dq_status}"
                f"|option_event_pit={self.option_event_pit_status}"
                f"|last_chain_count={self.last_chain_count}"
                "|raw_rows_logged=false|broker_action=false"
            )
    '''
)


__all__ = [
    "ALLOWED_ACTIONS",
    "AUTHORIZATION_TASK_ID",
    "DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_PLATFORM_ACTION_AUTHORIZATION_PATH",
    "EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256",
    "EXPECTED_PROPOSAL_POLICY_SHA256",
    "OWNER_AUTHORIZATION_ID",
    "PROHIBITED_ACTIONS",
    "QCBoundedCloudPilotActors",
    "QCBoundedCloudPilotAuthorizationSafety",
    "QCBoundedCloudPilotPlatformActionAuthorizationLoadResult",
    "QCBoundedCloudPilotPlatformActionAuthorizationPolicy",
    "QCBoundedCloudPilotPlatformActionContractError",
    "QCBoundedCloudPilotPreRunAuthorizationRecord",
    "QCBoundedCloudPilotProjectSourceArtifact",
    "build_qc_qqq_options_bounded_cloud_pilot_pre_run_record",
    "build_qc_qqq_options_bounded_cloud_pilot_project_source",
    "load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization",
]
