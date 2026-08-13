from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_collection_evidence_admission as admission_v1,
)
from ai_trading_system.qqq_options_research import (
    primary_window_derived_aggregate_run_proposal as proposal_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_evidence_lane_authorization_refresh_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/trading_2516_primary_window_evidence_lane_authorization_refresh_v1"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_PACKAGE_FILES = ("owner_decision_request.md", "package_manifest.json")
_ALLOWED_ACTIONS = (
    "QUANTCONNECT_LOGIN",
    "MODIFY_EXISTING_DEDICATED_PROJECT_ONCE",
    "RUN_ONE_ZERO_ORDER_CLOUD_BACKTEST",
    "EXPORT_SAFE_MANUAL_DOWNLOAD_RESULTS_COLLECTION",
)
_PROHIBITED_ACTIONS = (
    "API",
    "BROKER",
    "CLI",
    "HTTP",
    "INVESTMENT_INTERPRETATION",
    "LIVE",
    "OBJECT_STORE",
    "PAPER",
    "PRODUCTION",
    "PURCHASE_OR_SUBSCRIPTION",
    "RAW_OPTIONS_DATA_DOWNLOAD",
    "RAW_OPTION_ROW_LOGGING_OR_EXPORT",
    "SECOND_CLOUD_BACKTEST",
)
_OWNER_TOKEN_FIELD_ORDER = (
    "ordinary_pushed_main_sha",
    "refresh_policy_file_sha256",
    "refresh_policy_canonical_sha256",
    "refresh_package_manifest_file_sha256",
    "refresh_package_manifest_content_sha256",
    "proposal_content_sha256",
    "run_scope_content_sha256",
    "project_code_lf_sha256",
    "proposal_policy_file_sha256",
    "proposal_policy_canonical_sha256",
    "collector_policy_file_sha256",
    "collector_policy_canonical_sha256",
    "transport_map_sha256",
    "admission_policy_file_sha256",
    "admission_policy_canonical_sha256",
    "target_project_id",
    "requested_range",
    "expected_session_count",
    "maximum_project_mutations",
    "maximum_cloud_backtests",
    "maximum_orders",
    "maximum_fills",
    "collector",
    "independent_reviewer",
    "authorization_expires_at_utc",
    "authorization_single_use",
    "authorization_invalidates_after_evidence_collection",
)


class QCQQQOptionsEvidenceLaneAuthorizationRefreshError(ValueError):
    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode("utf-8")


def _duplicate_key_rejecting_json(raw: bytes) -> object:
    def pairs(values: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("invalid UTF-8 JSON") from exc


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
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field} must be a normalized repository-relative path")
    return value


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{field} must be timezone-aware UTC")
    return value.astimezone(UTC)


def _bound_path(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes project root") from exc
    if must_exist and not resolved.exists():
        raise ValueError(f"{field} does not exist")
    if candidate.is_symlink() or (must_exist and resolved.is_symlink()):
        raise ValueError(f"{field} must not be a symlink")
    return resolved


class RefreshUpstreamAuthority(_PolicyModel):
    proposal_package_root: str
    proposal_package_manifest_file_sha256: str
    proposal_package_manifest_content_sha256: str
    proposal_content_sha256: str
    run_scope_content_sha256: str
    project_code_lf_sha256: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    admission_policy_path: str
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str

    @field_validator("proposal_package_root", "admission_policy_path")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator(
        "proposal_package_manifest_file_sha256",
        "proposal_package_manifest_content_sha256",
        "proposal_content_sha256",
        "run_scope_content_sha256",
        "project_code_lf_sha256",
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class RefreshSafety(_PolicyModel):
    owner_token_observed: Literal[False]
    authorization_status: Literal["OWNER_AUTHORIZATION_NOT_PROVIDED"]
    evidence_status: Literal["EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    raw_options_data_export_allowed: Literal[False]
    paper_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_evidence_lane_authorization_refresh_policy.v1"
    ]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_VERSIONED_SUCCESSOR"]
    task_id: Literal[
        "TRADING-2516_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_V1"
    ]
    registration_base_repository_code_sha: str
    selected_evidence_lane: Literal[
        "QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"
    ]
    decision_token: str
    token_decision_date: date
    authorization_expires_after_hours: Literal[168]
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
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
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    collector_id: str
    independent_reviewer_id: str
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    upstream_authority: RefreshUpstreamAuthority
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    safety: RefreshSafety

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base_sha(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator("policy_id", "collector_id", "independent_reviewer_id")
    @classmethod
    def _ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _scope_and_token(self) -> Self:
        expected_token = (
            "owner_decision:TRADING-2516:2026-08-13:"
            "authorize_single_zero_order_primary_window_derived_aggregate_collection_v2"
        )
        if self.decision_token != expected_token:
            raise ValueError("fresh Owner token identity drifted")
        if self.token_decision_date != date(2026, 8, 13):
            raise ValueError("fresh Owner decision date drifted")
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ):
            raise ValueError("PRIMARY start must remain 2021-02-22")
        if (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("reviewed collection end must remain 2025-12-02")
        if (self.expected_first_session, self.expected_last_session) != (
            self.requested_start,
            self.requested_end,
        ):
            raise ValueError("session boundaries must match the reviewed range")
        if self.allowed_actions != _ALLOWED_ACTIONS:
            raise ValueError("allowed action inventory or order drifted")
        if self.prohibited_actions != _PROHIBITED_ACTIONS:
            raise ValueError("prohibited action inventory or order drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult:
    policy: QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicy
    policy_path: Path
    policy_file_sha256: str
    policy_canonical_sha256: str
    proposal_package: proposal_v1.BuiltQCQQQOptionsPrimaryWindowRunProposalPackage
    admission_policy: admission_v1.QCQQQOptionsCollectionEvidenceAdmissionPolicyLoadResult


class _SealedModel(_StrictModel):
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_hash(cls, value: str) -> str:
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
    def _seal(cls, **payload: Any) -> Self:
        draft = cls(content_sha256=_UNSEALED_SHA256, **payload)
        content_sha256 = hashlib.sha256(
            _canonical_json_bytes(draft.semantic_payload())
        ).hexdigest()
        return cls(content_sha256=content_sha256, **payload)

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        payload = _duplicate_key_rejecting_json(raw)
        if not isinstance(payload, dict):
            raise ValueError("sealed payload root must be a mapping")
        value = cls.model_validate(payload)
        expected = hashlib.sha256(
            _canonical_json_bytes(value.semantic_payload())
        ).hexdigest()
        if value.content_sha256 != expected or raw != value.canonical_bytes:
            raise ValueError("sealed payload is noncanonical or checksum-mismatched")
        return value


class RefreshPackageArtifact(_StrictModel):
    role: Literal["OWNER_DECISION_REQUEST"]
    relative_path: Literal["owner_decision_request.md"]
    byte_count: int
    sha256: str

    @field_validator("byte_count")
    @classmethod
    def _bytes(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("byte_count must be positive")
        return value

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "sha256")


class QCQQQOptionsEvidenceLaneAuthorizationRefreshPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package.v1"
    ]
    package_id: Literal[
        "TRADING_2516_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_V1"
    ]
    selected_evidence_lane: Literal[
        "QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE"
    ]
    registration_base_repository_code_sha: str
    refresh_policy_file_sha256: str
    refresh_policy_canonical_sha256: str
    proposal_package_manifest_file_sha256: str
    proposal_package_manifest_content_sha256: str
    admission_policy_file_sha256: str
    admission_policy_canonical_sha256: str
    decision_token: str
    token_decision_date: date
    authorization_expires_after_hours: Literal[168]
    target_project_id: Literal[34808569]
    requested_start: date
    requested_end: date
    expected_session_count: Literal[1202]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    authorization_status: Literal["OWNER_AUTHORIZATION_NOT_PROVIDED"]
    evidence_status: Literal["EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED"]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    artifacts: tuple[RefreshPackageArtifact, ...]

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _base_sha(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator(
        "refresh_policy_file_sha256",
        "refresh_policy_canonical_sha256",
        "proposal_package_manifest_file_sha256",
        "proposal_package_manifest_content_sha256",
        "admission_policy_file_sha256",
        "admission_policy_canonical_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @model_validator(mode="after")
    def _artifact_inventory(self) -> Self:
        if len(self.artifacts) != 1:
            raise ValueError("refresh package must contain one bound request artifact")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        return cls._seal(**payload)


class QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_authorization_refresh_owner_decision_candidate.v1"
    ]
    owner_decision_token: str
    owner_decision_file_sha256: str
    owner_decision_content_sha256: str
    ordinary_pushed_main_sha: str
    reviewed_at_utc: datetime
    expires_at_utc: datetime
    refresh_policy_file_sha256: str
    refresh_policy_canonical_sha256: str
    refresh_package_manifest_file_sha256: str
    refresh_package_manifest_content_sha256: str
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    authorization_consumed: Literal[False]
    decision: Literal["OWNER_AUTHORIZATION_REVIEWED_NOT_CONSUMED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("ordinary_pushed_main_sha")
    @classmethod
    def _main_sha(cls, value: str) -> str:
        return _git_sha(value, "ordinary_pushed_main_sha")

    @field_validator(
        "owner_decision_file_sha256",
        "owner_decision_content_sha256",
        "refresh_policy_file_sha256",
        "refresh_policy_canonical_sha256",
        "refresh_package_manifest_file_sha256",
        "refresh_package_manifest_content_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("reviewed_at_utc", "expires_at_utc")
    @classmethod
    def _timestamps(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        return cls._seal(**payload)


@dataclass(frozen=True)
class BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage:
    policy_load: QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult
    owner_decision_request_bytes: bytes
    manifest: QCQQQOptionsEvidenceLaneAuthorizationRefreshPackageManifest


def load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_policy(
    path: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_POLICY_PATH
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _bound_path(path, root=root, field="refresh policy", must_exist=True)
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("refresh policy root must be a mapping")
        policy = QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicy.model_validate(payload)
        proposal_package = (
            proposal_v1.load_qc_qqq_options_primary_window_run_proposal_package(
                project_root=root
            )
        )
        admission_policy = (
            admission_v1.load_qc_qqq_options_collection_evidence_admission_policy(
                project_root=root
            )
        )
        upstream = policy.upstream_authority
        declared_proposal_path = Path(upstream.proposal_package_root)
        if declared_proposal_path != (
            proposal_v1.DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT
        ):
            raise ValueError("2513 proposal package path drifted")
        proposal_manifest_path = root / declared_proposal_path / "package_manifest.json"
        if (
            hashlib.sha256(proposal_manifest_path.read_bytes()).hexdigest()
            != upstream.proposal_package_manifest_file_sha256
            or proposal_package.manifest.content_sha256
            != upstream.proposal_package_manifest_content_sha256
            or proposal_package.proposal.content_sha256 != upstream.proposal_content_sha256
            or proposal_package.run_scope.content_sha256
            != upstream.run_scope_content_sha256
            or proposal_package.manifest.project_code_lf_sha256
            != upstream.project_code_lf_sha256
            or proposal_package.policy_load.policy_file_sha256
            != upstream.proposal_policy_file_sha256
            or proposal_package.policy_load.policy_canonical_sha256
            != upstream.proposal_policy_canonical_sha256
            or proposal_package.proposal.collector_policy_file_sha256
            != upstream.collector_policy_file_sha256
            or proposal_package.proposal.collector_policy_canonical_sha256
            != upstream.collector_policy_canonical_sha256
            or proposal_package.proposal.transport_map_sha256
            != upstream.transport_map_sha256
        ):
            raise ValueError("2513 proposal authority drifted")
        if (
            admission_policy.policy_path.relative_to(root).as_posix()
            != upstream.admission_policy_path
            or admission_policy.policy_file_sha256
            != upstream.admission_policy_file_sha256
            or admission_policy.policy_canonical_sha256
            != upstream.admission_policy_canonical_sha256
        ):
            raise ValueError("2514 admission authority drifted")
    except QCQQQOptionsEvidenceLaneAuthorizationRefreshError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsEvidenceLaneAuthorizationRefreshError(
            "AUTHORIZATION_REFRESH_POLICY_REJECTED", str(exc)
        ) from exc
    return QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
        proposal_package=proposal_package,
        admission_policy=admission_policy,
    )


def _owner_decision_request_bytes(
    loaded: QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult,
) -> bytes:
    policy = loaded.policy
    proposal = loaded.proposal_package.proposal
    manifest_file_placeholder = "<REFRESH_PACKAGE_MANIFEST_FILE_SHA256>"
    manifest_content_placeholder = "<REFRESH_PACKAGE_MANIFEST_CONTENT_SHA256>"
    expiry = (
        "<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_"
        f"{policy.authorization_expires_after_hours}_HOURS>"
    )
    token_lines = (
        policy.decision_token,
        "ordinary_pushed_main_sha:<ORDINARY_PUSHED_2516_MAIN_SHA>",
        f"refresh_policy_file_sha256:{loaded.policy_file_sha256}",
        f"refresh_policy_canonical_sha256:{loaded.policy_canonical_sha256}",
        f"refresh_package_manifest_file_sha256:{manifest_file_placeholder}",
        f"refresh_package_manifest_content_sha256:{manifest_content_placeholder}",
        f"proposal_content_sha256:{proposal.content_sha256}",
        f"run_scope_content_sha256:{proposal.run_scope.content_sha256}",
        f"project_code_lf_sha256:{proposal.project_code_lf_sha256}",
        f"proposal_policy_file_sha256:{loaded.proposal_package.policy_load.policy_file_sha256}",
        f"proposal_policy_canonical_sha256:{loaded.proposal_package.policy_load.policy_canonical_sha256}",
        f"collector_policy_file_sha256:{proposal.collector_policy_file_sha256}",
        f"collector_policy_canonical_sha256:{proposal.collector_policy_canonical_sha256}",
        f"transport_map_sha256:{proposal.transport_map_sha256}",
        f"admission_policy_file_sha256:{loaded.admission_policy.policy_file_sha256}",
        f"admission_policy_canonical_sha256:{loaded.admission_policy.policy_canonical_sha256}",
        f"target_project_id:{policy.target_project_id}",
        f"requested_range:{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}",
        f"expected_session_count:{policy.expected_session_count}",
        f"maximum_project_mutations:{policy.maximum_project_mutations}",
        f"maximum_cloud_backtests:{policy.maximum_cloud_backtests}",
        f"maximum_orders:{policy.maximum_orders}",
        f"maximum_fills:{policy.maximum_fills}",
        f"collector:{policy.collector_id}",
        f"independent_reviewer:{policy.independent_reviewer_id}",
        f"authorization_expires_at_utc:{expiry}",
        "authorization_single_use:true",
        "authorization_invalidates_after_evidence_collection:true",
    )
    allowed = "\n".join(f"- `{item}`" for item in policy.allowed_actions)
    prohibited = "\n".join(f"- `{item}`" for item in policy.prohibited_actions)
    text = "\n".join(
        (
            "# TRADING-2516 Owner Decision Request",
            "",
            "状态：`OWNER_AUTHORIZATION_REQUIRED_FRESH_TOKEN`",
            "",
            (
                "本请求只刷新 QQQ Options PRIMARY 主窗口的 zero-order、export-safe "
                "derived aggregate collection 授权。"
            ),
            (
                "它不授权 policy values、selection、engine、订单、投资解释、"
                "paper/live/broker/production。"
            ),
            "",
            "## Exact scope",
            "",
            f"- selected lane：`{policy.selected_evidence_lane}`",
            f"- registration base：`{policy.registration_base_repository_code_sha}`",
            f"- target project id：`{policy.target_project_id}`",
            (
                "- requested/evaluated range："
                f"`{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}`"
            ),
            f"- XNYS session count：`{policy.expected_session_count}`",
            (
                "- maximum project mutations / cloud backtests："
                f"`{policy.maximum_project_mutations}` / "
                f"`{policy.maximum_cloud_backtests}`"
            ),
            f"- maximum orders / fills：`{policy.maximum_orders}` / `{policy.maximum_fills}`",
            "- result carrier：Owner manual `Download Results` JSON only",
            "",
            "## Allowed actions（仅在 Owner exact token 后）",
            "",
            allowed,
            "",
            "## Prohibited actions",
            "",
            prohibited,
            "",
            "## Review rules",
            "",
            "1. 使用 ordinary-pushed 2516 exact main 与 package manifest hashes 填充占位符。",
            "2. expiry 必须晚于 2026-08-13 且不超过 168 小时；不得使用 2513/2514 的旧 token 倒签。",
            "3. token single-use，并在 evidence collection 后失效。",
            (
                "4. 授权不等于 run/evidence/DQ/PIT PASS，也不解除 "
                "`KEEP_CLOSED + PREREGISTRATION_ONLY`。"
            ),
            "",
            "## Owner token template（未签署）",
            "",
            "```text",
            *token_lines,
            "```",
            "",
        )
    )
    return text.encode("utf-8")


def build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage:
    loaded = load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_policy(
        project_root=project_root
    )
    request = _owner_decision_request_bytes(loaded)
    policy = loaded.policy
    upstream = policy.upstream_authority
    artifact = RefreshPackageArtifact(
        role="OWNER_DECISION_REQUEST",
        relative_path="owner_decision_request.md",
        byte_count=len(request),
        sha256=hashlib.sha256(request).hexdigest(),
    )
    manifest = QCQQQOptionsEvidenceLaneAuthorizationRefreshPackageManifest.seal(
        schema_version="qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package.v1",
        package_id="TRADING_2516_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_V1",
        selected_evidence_lane=policy.selected_evidence_lane,
        registration_base_repository_code_sha=policy.registration_base_repository_code_sha,
        refresh_policy_file_sha256=loaded.policy_file_sha256,
        refresh_policy_canonical_sha256=loaded.policy_canonical_sha256,
        proposal_package_manifest_file_sha256=upstream.proposal_package_manifest_file_sha256,
        proposal_package_manifest_content_sha256=upstream.proposal_package_manifest_content_sha256,
        admission_policy_file_sha256=upstream.admission_policy_file_sha256,
        admission_policy_canonical_sha256=upstream.admission_policy_canonical_sha256,
        decision_token=policy.decision_token,
        token_decision_date=policy.token_decision_date,
        authorization_expires_after_hours=policy.authorization_expires_after_hours,
        target_project_id=policy.target_project_id,
        requested_start=policy.requested_start,
        requested_end=policy.requested_end,
        expected_session_count=policy.expected_session_count,
        maximum_project_mutations=policy.maximum_project_mutations,
        maximum_cloud_backtests=policy.maximum_cloud_backtests,
        maximum_orders=policy.maximum_orders,
        maximum_fills=policy.maximum_fills,
        authorization_status="OWNER_AUTHORIZATION_NOT_PROVIDED",
        evidence_status="EVIDENCE_NOT_ADMITTED_POLICY_BLOCKED",
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        production_effect="none",
        broker_action="none",
        artifacts=(artifact,),
    )
    return BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage(
        policy_load=loaded,
        owner_decision_request_bytes=request,
        manifest=manifest,
    )


def write_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage:
    root = project_root.resolve()
    built = build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
        project_root=root
    )
    package_root = _bound_path(
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT,
        root=root,
        field="refresh package root",
        must_exist=False,
    )
    package_root.mkdir(parents=True, exist_ok=True)
    write_bytes_atomic(
        package_root / "owner_decision_request.md", built.owner_decision_request_bytes
    )
    write_bytes_atomic(package_root / "package_manifest.json", built.manifest.canonical_bytes)
    return built


def load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage:
    root = project_root.resolve()
    try:
        package_root = _bound_path(
            DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT,
            root=root,
            field="refresh package root",
            must_exist=True,
        )
        if not package_root.is_dir():
            raise ValueError("refresh package root must be a directory")
        entries = tuple(sorted(path.name for path in package_root.iterdir()))
        if entries != _PACKAGE_FILES:
            raise ValueError("refresh package inventory drifted")
        for path in package_root.iterdir():
            if path.is_symlink() or not path.is_file():
                raise ValueError("refresh package entries must be regular non-symlink files")
        request = (package_root / "owner_decision_request.md").read_bytes()
        manifest_raw = (package_root / "package_manifest.json").read_bytes()
        manifest = QCQQQOptionsEvidenceLaneAuthorizationRefreshPackageManifest.from_json_bytes(
            manifest_raw
        )
        expected = build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
            project_root=root
        )
        artifact = manifest.artifacts[0]
        if (
            request != expected.owner_decision_request_bytes
            or manifest != expected.manifest
            or len(request) != artifact.byte_count
            or hashlib.sha256(request).hexdigest() != artifact.sha256
        ):
            raise ValueError("refresh package bytes or authority bindings drifted")
    except QCQQQOptionsEvidenceLaneAuthorizationRefreshError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsEvidenceLaneAuthorizationRefreshError(
            "AUTHORIZATION_REFRESH_PACKAGE_REJECTED", str(exc)
        ) from exc
    return BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage(
        policy_load=expected.policy_load,
        owner_decision_request_bytes=request,
        manifest=manifest,
    )


def validate_qc_qqq_options_authorization_refresh_owner_decision_candidate(
    *,
    owner_decision_bytes: bytes,
    expected_ordinary_pushed_main_sha: str,
    reviewed_at_utc: datetime,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate:
    expected_main = _git_sha(
        expected_ordinary_pushed_main_sha, "expected_ordinary_pushed_main_sha"
    )
    reviewed_at = _utc(reviewed_at_utc, "reviewed_at_utc")
    package = load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package(
        project_root=project_root
    )
    policy_load = package.policy_load
    policy = policy_load.policy
    try:
        text = owner_decision_bytes.decode("utf-8")
        if "\r" in text or not text.endswith("\n") or text.endswith("\n\n"):
            raise ValueError("Owner decision must be exact LF text with one final newline")
        lines = text[:-1].split("\n")
        if not lines or lines[0] != policy.decision_token:
            raise ValueError("fresh exact 2516 Owner decision token was not supplied")
        fields: dict[str, str] = {}
        order: list[str] = []
        for line in lines[1:]:
            if line.count(":") < 1:
                raise ValueError("Owner decision line lacks key/value separator")
            key, value = line.split(":", 1)
            if key in fields or not _IDENTIFIER.fullmatch(key):
                raise ValueError(f"invalid or duplicate Owner decision field: {key}")
            if not value or value != value.strip():
                raise ValueError(f"invalid Owner decision value: {key}")
            fields[key] = value
            order.append(key)
        if tuple(order) != _OWNER_TOKEN_FIELD_ORDER:
            raise ValueError("Owner decision field inventory/order drifted")
        expiry_text = fields["authorization_expires_at_utc"]
        expiry = datetime.strptime(expiry_text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
        decision_start = datetime.combine(policy.token_decision_date, datetime.min.time(), UTC)
        decision_limit = decision_start + timedelta(
            hours=policy.authorization_expires_after_hours
        )
        if not decision_start < expiry <= decision_limit:
            raise ValueError("authorization expiry is outside the reviewed <=168h window")
        if not decision_start <= reviewed_at <= expiry:
            raise ValueError("Owner review as-of is outside the authorization window")
        manifest_path = (
            project_root.resolve()
            / DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT
            / "package_manifest.json"
        )
        manifest_file_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        upstream = policy.upstream_authority
        expected = {
            "ordinary_pushed_main_sha": expected_main,
            "refresh_policy_file_sha256": policy_load.policy_file_sha256,
            "refresh_policy_canonical_sha256": policy_load.policy_canonical_sha256,
            "refresh_package_manifest_file_sha256": manifest_file_sha,
            "refresh_package_manifest_content_sha256": package.manifest.content_sha256,
            "proposal_content_sha256": upstream.proposal_content_sha256,
            "run_scope_content_sha256": upstream.run_scope_content_sha256,
            "project_code_lf_sha256": upstream.project_code_lf_sha256,
            "proposal_policy_file_sha256": upstream.proposal_policy_file_sha256,
            "proposal_policy_canonical_sha256": upstream.proposal_policy_canonical_sha256,
            "collector_policy_file_sha256": upstream.collector_policy_file_sha256,
            "collector_policy_canonical_sha256": upstream.collector_policy_canonical_sha256,
            "transport_map_sha256": upstream.transport_map_sha256,
            "admission_policy_file_sha256": upstream.admission_policy_file_sha256,
            "admission_policy_canonical_sha256": upstream.admission_policy_canonical_sha256,
            "target_project_id": str(policy.target_project_id),
            "requested_range": (
                f"{policy.requested_start.isoformat()}..{policy.requested_end.isoformat()}"
            ),
            "expected_session_count": str(policy.expected_session_count),
            "maximum_project_mutations": str(policy.maximum_project_mutations),
            "maximum_cloud_backtests": str(policy.maximum_cloud_backtests),
            "maximum_orders": "0",
            "maximum_fills": "0",
            "collector": policy.collector_id,
            "independent_reviewer": policy.independent_reviewer_id,
            "authorization_expires_at_utc": expiry_text,
            "authorization_single_use": "true",
            "authorization_invalidates_after_evidence_collection": "true",
        }
        if fields != expected:
            mismatches = tuple(
                sorted(key for key in expected if fields.get(key) != expected[key])
            )
            raise ValueError(f"Owner decision binding mismatch: {mismatches}")
    except (UnicodeDecodeError, OSError, ValueError) as exc:
        raise QCQQQOptionsEvidenceLaneAuthorizationRefreshError(
            "OWNER_AUTHORIZATION_REFRESH_CANDIDATE_REJECTED", str(exc)
        ) from exc
    semantic_sha = hashlib.sha256(
        _canonical_json_bytes({"owner_decision_token": lines[0], **fields})
    ).hexdigest()
    return QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate.seal(
        schema_version="qc_qqq_options_primary_window_authorization_refresh_owner_decision_candidate.v1",
        owner_decision_token=policy.decision_token,
        owner_decision_file_sha256=hashlib.sha256(owner_decision_bytes).hexdigest(),
        owner_decision_content_sha256=semantic_sha,
        ordinary_pushed_main_sha=expected_main,
        reviewed_at_utc=reviewed_at,
        expires_at_utc=expiry,
        refresh_policy_file_sha256=policy_load.policy_file_sha256,
        refresh_policy_canonical_sha256=policy_load.policy_canonical_sha256,
        refresh_package_manifest_file_sha256=manifest_file_sha,
        refresh_package_manifest_content_sha256=package.manifest.content_sha256,
        authorization_single_use=True,
        authorization_invalidates_after_evidence_collection=True,
        authorization_consumed=False,
        decision="OWNER_AUTHORIZATION_REVIEWED_NOT_CONSUMED",
        owner_policy_value_count=0,
        executable_policy_authorized=False,
        engine_status="POLICY_BLOCKED_CASH_PRESERVATION",
        selection_authorized=False,
        external_action_performed=False,
        investment_interpretation_generated=False,
        production_effect="none",
        broker_action="none",
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_PACKAGE_ROOT",
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE_LANE_AUTHORIZATION_REFRESH_POLICY_PATH",
    "BuiltQCQQQOptionsEvidenceLaneAuthorizationRefreshPackage",
    "QCQQQOptionsAuthorizationRefreshOwnerDecisionCandidate",
    "QCQQQOptionsEvidenceLaneAuthorizationRefreshError",
    "QCQQQOptionsEvidenceLaneAuthorizationRefreshPackageManifest",
    "QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicy",
    "QCQQQOptionsEvidenceLaneAuthorizationRefreshPolicyLoadResult",
    "RefreshPackageArtifact",
    "build_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package",
    "load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package",
    "load_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_policy",
    "validate_qc_qqq_options_authorization_refresh_owner_decision_candidate",
    "write_qc_qqq_options_primary_window_evidence_lane_authorization_refresh_package",
]
