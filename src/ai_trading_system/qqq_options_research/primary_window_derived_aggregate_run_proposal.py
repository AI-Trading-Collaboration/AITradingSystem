from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    primary_window_export_safe_derived_aggregate_collector as collector_v1,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_POLICY_PATH = Path(
    "config/research/qc_qqq_options_primary_window_derived_aggregate_run_proposal_v1.yaml"
)
DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/trading_2513_primary_window_derived_aggregate_run_proposal_v1"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_PACKAGE_FILES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "run_scope.json",
)
_MANIFEST_ARTIFACTS = (
    ("QC_COLLECTOR_PROJECT_CODE", "main.py"),
    ("OWNER_DECISION_REQUEST", "owner_decision_request.md"),
    ("COLLECTOR_PROPOSAL", "proposal.json"),
    ("COLLECTOR_RUN_SCOPE", "run_scope.json"),
)


class QCQQQOptionsPrimaryWindowRunProposalError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


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
            raw.decode("utf-8"), object_pairs_hook=hook, parse_constant=reject_constant
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("record is not UTF-8 JSON") from exc


def _sha256(value: str, field: str) -> str:
    if not _SHA256.fullmatch(value):
        raise ValueError(f"{field} must be a lowercase SHA-256")
    return value


def _git_sha(value: str, field: str) -> str:
    if not _GIT_SHA.fullmatch(value):
        raise ValueError(f"{field} must be a 40-character Git SHA")
    return value


def _relative_path(value: str, field: str) -> str:
    path = Path(value)
    if not value or path.is_absolute() or path.drive or ".." in path.parts:
        raise ValueError(f"{field} must be a bounded project-relative path")
    if path.as_posix() != value:
        raise ValueError(f"{field} must use normalized forward slashes")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None:
        raise ValueError(f"{field} must be timezone-aware")
    if offset.total_seconds() != 0:
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _bound_file(path: Path, *, root: Path, field: str, must_exist: bool) -> Path:
    resolved_root = root.resolve()
    candidate = path if path.is_absolute() else resolved_root / path
    try:
        relative = candidate.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(f"{field} escapes the project root") from exc
    current = resolved_root
    for part in relative.parts:
        current = current / part
        if current.exists() and current.is_symlink():
            raise ValueError(f"{field} cannot traverse a symlink")
    if must_exist and not candidate.is_file():
        raise ValueError(f"{field} must be a regular file")
    return candidate


class ProposalUpstreamCollector(_PolicyModel):
    policy_path: str
    policy_file_sha256: str
    policy_canonical_sha256: str
    transport_map_sha256: str
    implementation_path: str
    implementation_file_sha256: str

    @field_validator("policy_path", "implementation_path")
    @classmethod
    def _paths(cls, value: str, info: ValidationInfo) -> str:
        return _relative_path(value, str(info.field_name))

    @field_validator(
        "policy_file_sha256",
        "policy_canonical_sha256",
        "transport_map_sha256",
        "implementation_file_sha256",
    )
    @classmethod
    def _hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))


class ProposalSafety(_PolicyModel):
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_AUTHORIZATION_REQUIRED"]
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


class QCQQQOptionsPrimaryWindowRunProposalPolicy(_PolicyModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_derived_aggregate_run_proposal_policy.v1"
    ]
    policy_id: str
    policy_version: Literal["1.0.0"]
    policy_status: Literal["OWNER_REVIEW_REQUIRED_EXACT_PROPOSAL"]
    task_id: Literal[
        "TRADING-2513_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_OWNER_DECISION_PACK_V1"
    ]
    registration_base_repository_code_sha: str
    package_id: str
    package_root: str
    created_at_utc: datetime
    run_scope_id: str
    proposal_id: str
    target_project_id: int = Field(ge=1)
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    primary_research_role: Literal["PRIMARY"]
    exchange_calendar: Literal["XNYS"]
    expected_session_count: int = Field(ge=1)
    expected_first_session: date
    expected_last_session: date
    collector_id: str
    independent_reviewer_id: str
    authorization_expires_after_hours: int = Field(ge=1)
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    result_carrier: Literal["MANUAL_DOWNLOAD_RESULTS_JSON"]
    upstream_collector: ProposalUpstreamCollector
    safety: ProposalSafety

    @field_validator("registration_base_repository_code_sha")
    @classmethod
    def _repository_sha(cls, value: str) -> str:
        return _git_sha(value, "registration_base_repository_code_sha")

    @field_validator("package_root")
    @classmethod
    def _package_root(cls, value: str) -> str:
        return _relative_path(value, "package_root")

    @field_validator("created_at_utc")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("policy_id", "package_id", "run_scope_id", "proposal_id")
    @classmethod
    def _identifiers(cls, value: str, info: ValidationInfo) -> str:
        if not _IDENTIFIER.fullmatch(value):
            raise ValueError(f"{info.field_name} is not a stable identifier")
        return value

    @model_validator(mode="after")
    def _scope(self) -> Self:
        if (self.requested_start, self.evaluated_start) != (
            date(2021, 2, 22),
            date(2021, 2, 22),
        ):
            raise ValueError("PRIMARY start must remain 2021-02-22")
        if (self.requested_end, self.evaluated_end) != (
            date(2025, 12, 2),
            date(2025, 12, 2),
        ):
            raise ValueError("reviewed proposal end must remain 2025-12-02")
        if (
            self.expected_session_count != 1202
            or self.expected_first_session != self.requested_start
            or self.expected_last_session != self.requested_end
        ):
            raise ValueError("reviewed session inventory summary drifted")
        if self.package_root != (
            DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT.as_posix()
        ):
            raise ValueError("package root drifted")
        return self

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True)
class QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult:
    policy: QCQQQOptionsPrimaryWindowRunProposalPolicy
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
        if info.context and info.context.get("allow_unsealed") and self.content_sha256 == (
            _UNSEALED_SHA256
        ):
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
            raise QCQQQOptionsPrimaryWindowRunProposalError(
                "PROPOSAL_PACKAGE_PAYLOAD_INVALID", str(exc)
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
            raise QCQQQOptionsPrimaryWindowRunProposalError(
                "PROPOSAL_PACKAGE_RECORD_INVALID", str(exc)
            ) from exc


class ProposalPackageArtifact(_StrictModel):
    role: str
    relative_path: str
    sha256: str
    byte_count: int = Field(ge=1)

    @field_validator("relative_path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _relative_path(value, "artifact.relative_path")

    @field_validator("sha256")
    @classmethod
    def _hash(cls, value: str) -> str:
        return _sha256(value, "artifact.sha256")


class QCQQQOptionsPrimaryWindowRunProposalPackageManifest(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_primary_window_derived_aggregate_run_proposal_package.v1"
    ]
    package_id: str
    created_at_utc: datetime
    repository_code_sha: str
    proposal_policy_file_sha256: str
    proposal_policy_canonical_sha256: str
    collector_policy_file_sha256: str
    collector_policy_canonical_sha256: str
    transport_map_sha256: str
    collector_implementation_file_sha256: str
    run_scope_content_sha256: str
    run_scope_canonical_sha256: str
    proposal_content_sha256: str
    proposal_canonical_sha256: str
    project_code_lf_sha256: str
    project_code_lf_byte_count: int = Field(ge=1)
    target_project_id: int = Field(ge=1)
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    session_count: int = Field(ge=1)
    first_session: date
    last_session: date
    maximum_project_mutations: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_orders: Literal[0]
    maximum_fills: Literal[0]
    collector_id: str
    independent_reviewer_id: str
    authorization_expires_after_hours: int = Field(ge=1)
    authorization_single_use: Literal[True]
    authorization_invalidates_after_evidence_collection: Literal[True]
    authorization_status: Literal["NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"]
    decision: Literal["OWNER_AUTHORIZATION_REQUIRED"]
    owner_policy_value_count: Literal[0]
    executable_policy_authorized: Literal[False]
    engine_status: Literal["POLICY_BLOCKED_CASH_PRESERVATION"]
    selection_authorized: Literal[False]
    external_action_performed: Literal[False]
    investment_interpretation_generated: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]
    artifacts: tuple[ProposalPackageArtifact, ...]

    @field_validator("created_at_utc")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _utc(value, "created_at_utc")

    @field_validator("repository_code_sha")
    @classmethod
    def _repository(cls, value: str) -> str:
        return _git_sha(value, "repository_code_sha")

    @field_validator(
        "proposal_policy_file_sha256",
        "proposal_policy_canonical_sha256",
        "collector_policy_file_sha256",
        "collector_policy_canonical_sha256",
        "transport_map_sha256",
        "collector_implementation_file_sha256",
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
    def _inventory(self) -> Self:
        expected = tuple(role for role, _ in _MANIFEST_ARTIFACTS)
        if tuple(item.role for item in self.artifacts) != expected:
            raise ValueError("manifest artifact role inventory drifted")
        expected_paths = tuple(path for _, path in _MANIFEST_ARTIFACTS)
        if tuple(item.relative_path for item in self.artifacts) != expected_paths:
            raise ValueError("manifest artifact path inventory drifted")
        if len({item.relative_path for item in self.artifacts}) != len(self.artifacts):
            raise ValueError("manifest artifact paths must be unique")
        return self


@dataclass(frozen=True)
class BuiltQCQQQOptionsPrimaryWindowRunProposalPackage:
    policy_load: QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult
    run_scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal
    project_code_bytes: bytes
    owner_decision_request_bytes: bytes
    manifest: QCQQQOptionsPrimaryWindowRunProposalPackageManifest


def load_qc_qqq_options_primary_window_run_proposal_policy(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_POLICY_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult:
    root = project_root.resolve()
    try:
        policy_path = _bound_file(path, root=root, field="proposal policy", must_exist=True)
        raw = policy_path.read_bytes()
        payload = safe_load_yaml_path(policy_path)
        if not isinstance(payload, dict):
            raise TypeError("proposal policy root must be a mapping")
        policy = QCQQQOptionsPrimaryWindowRunProposalPolicy.model_validate(payload)
        upstream = (
            collector_v1.load_qc_qqq_options_primary_window_derived_aggregate_collector_policy(
                project_root=root
            )
        )
        declared = policy.upstream_collector
        implementation = _bound_file(
            Path(declared.implementation_path),
            root=root,
            field="collector implementation",
            must_exist=True,
        )
        if (
            declared.policy_path != upstream.policy_path.relative_to(root).as_posix()
            or declared.policy_file_sha256 != upstream.policy_file_sha256
            or declared.policy_canonical_sha256 != upstream.policy_canonical_sha256
            or declared.transport_map_sha256 != upstream.policy.transport.canonical_sha256
            or hashlib.sha256(implementation.read_bytes()).hexdigest()
            != declared.implementation_file_sha256
        ):
            raise ValueError("2512 collector authority drifted")
    except QCQQQOptionsPrimaryWindowRunProposalError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsPrimaryWindowRunProposalError(
            "PROPOSAL_POLICY_AUTHORITY_MISMATCH", str(exc)
        ) from exc
    return QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult(
        policy=policy,
        policy_path=policy_path,
        policy_file_sha256=hashlib.sha256(raw).hexdigest(),
        policy_canonical_sha256=policy.canonical_sha256,
    )


def _owner_decision_request_bytes(
    *,
    policy_load: QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult,
    scope: collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope,
    proposal: collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal,
) -> bytes:
    policy = policy_load.policy
    allowed = "\n".join(f"- `{item}`" for item in proposal.allowed_actions)
    prohibited = "\n".join(f"- `{item}`" for item in proposal.prohibited_actions)
    token_name = (
        "owner_decision:TRADING-2513:<YYYY-MM-DD>:"
        "authorize_single_zero_order_primary_window_derived_aggregate_collection_v1"
    )
    expiry_placeholder = (
        "<OWNER_SELECTED_EXPIRY_NOT_MORE_THAN_"
        f"{policy.authorization_expires_after_hours}_HOURS>"
    )
    token = "\n".join(
        (
            token_name,
            "ordinary_pushed_main_sha:<ORDINARY_PUSHED_MAIN_SHA>",
            f"repository_code_sha:{scope.repository_code_sha}",
            f"proposal_content_sha256:{proposal.content_sha256}",
            f"run_scope_content_sha256:{scope.content_sha256}",
            f"project_code_lf_sha256:{proposal.project_code_lf_sha256}",
            f"proposal_policy_file_sha256:{policy_load.policy_file_sha256}",
            (
                "proposal_policy_canonical_sha256:"
                f"{policy_load.policy_canonical_sha256}"
            ),
            (
                "collector_policy_file_sha256:"
                f"{proposal.collector_policy_file_sha256}"
            ),
            (
                "collector_policy_canonical_sha256:"
                f"{proposal.collector_policy_canonical_sha256}"
            ),
            f"transport_map_sha256:{proposal.transport_map_sha256}",
            f"target_project_id:{scope.target_project_id}",
            (
                f"requested_range:{scope.requested_start.isoformat()}.."
                f"{scope.requested_end.isoformat()}"
            ),
            f"expected_session_count:{len(scope.session_ids)}",
            "maximum_project_mutations:1",
            "maximum_cloud_backtests:1",
            "maximum_orders:0",
            "maximum_fills:0",
            f"collector:{policy.collector_id}",
            f"independent_reviewer:{policy.independent_reviewer_id}",
            f"authorization_expires_at_utc:{expiry_placeholder}",
            "authorization_single_use:true",
            "authorization_invalidates_after_evidence_collection:true",
        )
    )
    text = "\n".join(
        (
            "# TRADING-2513 Owner Decision Request",
            "",
            "状态：`OWNER_AUTHORIZATION_REQUIRED`",
            "",
            (
                "这是一份 zero-order、export-safe derived aggregate collection 授权请求，"
                "不是策略、阈值、交易或投资结论授权。"
            ),
            "",
            "## Exact run scope",
            "",
            f"- repository code SHA：`{scope.repository_code_sha}`",
            f"- target project id：`{scope.target_project_id}`",
            (
                "- requested/evaluated range："
                f"`{scope.requested_start.isoformat()}..{scope.requested_end.isoformat()}`"
            ),
            f"- XNYS session count：`{len(scope.session_ids)}`",
            (
                f"- first/last session：`{scope.session_ids[0].isoformat()}` / "
                f"`{scope.session_ids[-1].isoformat()}`"
            ),
            f"- project code LF SHA-256：`{proposal.project_code_lf_sha256}`",
            f"- proposal content SHA-256：`{proposal.content_sha256}`",
            f"- run scope content SHA-256：`{scope.content_sha256}`",
            (
                "- proposal policy file/canonical SHA-256："
                f"`{policy_load.policy_file_sha256}` / "
                f"`{policy_load.policy_canonical_sha256}`"
            ),
            (
                "- collector policy file/canonical SHA-256："
                f"`{proposal.collector_policy_file_sha256}` / "
                f"`{proposal.collector_policy_canonical_sha256}`"
            ),
            f"- transport map SHA-256：`{proposal.transport_map_sha256}`",
            "",
            "## Allowed actions（仅在 Owner 另行签署后）",
            "",
            allowed,
            "",
            "## Prohibited actions",
            "",
            prohibited,
            "",
            "## Review checklist",
            "",
            "1. 在已 ordinary-push 的 exact main 上复核本 package 五文件 inventory 与 hashes。",
            (
                "2. 复核 target project、1202 sessions、一次 project mutation / "
                "一次 cloud backtest / 零订单零成交上限。"
            ),
            (
                "3. 复核 `main.py` 不包含 threshold、order、raw-row logging/export、"
                "Object Store 或 network 行为。"
            ),
            (
                f"4. 选择不超过 {policy.authorization_expires_after_hours} 小时的 expiry，"
                "并确认 single-use 与 evidence collection 后失效。"
            ),
            (
                "5. 指定 collector 与 independent reviewer；reviewer 必须独立复核"
                "外部动作次数、结果下载与 prohibited-action absence。"
            ),
            (
                "6. 授权不等于 DQ PASS、policy reviewed、selection/engine enabled "
                "或 investment interpretation。"
            ),
            "",
            "## Owner token template（当前未签署）",
            "",
            "```text",
            token,
            "```",
            "",
        )
    )
    return text.encode("utf-8")


def _artifact(role: str, relative_path: str, raw: bytes) -> ProposalPackageArtifact:
    return ProposalPackageArtifact(
        role=role,
        relative_path=relative_path,
        sha256=hashlib.sha256(raw).hexdigest(),
        byte_count=len(raw),
    )


def build_qc_qqq_options_primary_window_run_proposal_package(
    *,
    project_root: Path = PROJECT_ROOT,
) -> BuiltQCQQQOptionsPrimaryWindowRunProposalPackage:
    root = project_root.resolve()
    loaded = load_qc_qqq_options_primary_window_run_proposal_policy(project_root=root)
    policy = loaded.policy
    scope = collector_v1.build_qc_qqq_options_derived_aggregate_collector_run_scope(
        run_scope_id=policy.run_scope_id,
        created_at_utc=policy.created_at_utc,
        repository_code_sha=policy.registration_base_repository_code_sha,
        target_project_id=policy.target_project_id,
        requested_end=policy.requested_end,
        project_root=root,
    )
    if (
        len(scope.session_ids) != policy.expected_session_count
        or scope.session_ids[0] != policy.expected_first_session
        or scope.session_ids[-1] != policy.expected_last_session
    ):
        raise QCQQQOptionsPrimaryWindowRunProposalError(
            "PROPOSAL_SESSION_INVENTORY_MISMATCH",
            "2512 XNYS session inventory differs from reviewed proposal policy",
        )
    proposal = collector_v1.build_qc_qqq_options_derived_aggregate_collector_proposal(
        proposal_id=policy.proposal_id,
        issued_at_utc=policy.created_at_utc,
        run_scope=scope,
        project_root=root,
    )
    rendered = (
        collector_v1.render_qc_qqq_options_primary_window_derived_aggregate_collector_project(
            run_scope=scope,
            project_root=root,
        )
    )
    request_bytes = _owner_decision_request_bytes(
        policy_load=loaded, scope=scope, proposal=proposal
    )
    raw_artifacts: Mapping[str, bytes] = {
        "main.py": rendered.code_bytes,
        "owner_decision_request.md": request_bytes,
        "proposal.json": proposal.canonical_bytes,
        "run_scope.json": scope.canonical_bytes,
    }
    artifacts = tuple(
        _artifact(role, path, raw_artifacts[path]) for role, path in _MANIFEST_ARTIFACTS
    )
    manifest = QCQQQOptionsPrimaryWindowRunProposalPackageManifest.seal(
        schema_version=(
            "qc_qqq_options_primary_window_derived_aggregate_run_proposal_package.v1"
        ),
        package_id=policy.package_id,
        created_at_utc=policy.created_at_utc,
        repository_code_sha=scope.repository_code_sha,
        proposal_policy_file_sha256=loaded.policy_file_sha256,
        proposal_policy_canonical_sha256=loaded.policy_canonical_sha256,
        collector_policy_file_sha256=proposal.collector_policy_file_sha256,
        collector_policy_canonical_sha256=proposal.collector_policy_canonical_sha256,
        transport_map_sha256=proposal.transport_map_sha256,
        collector_implementation_file_sha256=(
            policy.upstream_collector.implementation_file_sha256
        ),
        run_scope_content_sha256=scope.content_sha256,
        run_scope_canonical_sha256=scope.canonical_sha256,
        proposal_content_sha256=proposal.content_sha256,
        proposal_canonical_sha256=proposal.canonical_sha256,
        project_code_lf_sha256=rendered.code_lf_sha256,
        project_code_lf_byte_count=rendered.code_lf_byte_count,
        target_project_id=scope.target_project_id,
        requested_start=scope.requested_start,
        requested_end=scope.requested_end,
        evaluated_start=scope.evaluated_start,
        evaluated_end=scope.evaluated_end,
        session_count=len(scope.session_ids),
        first_session=scope.session_ids[0],
        last_session=scope.session_ids[-1],
        maximum_project_mutations=proposal.maximum_project_mutations,
        maximum_cloud_backtests=proposal.maximum_cloud_backtests,
        maximum_orders=proposal.maximum_orders,
        maximum_fills=proposal.maximum_fills,
        collector_id=policy.collector_id,
        independent_reviewer_id=policy.independent_reviewer_id,
        authorization_expires_after_hours=policy.authorization_expires_after_hours,
        authorization_single_use=policy.authorization_single_use,
        authorization_invalidates_after_evidence_collection=(
            policy.authorization_invalidates_after_evidence_collection
        ),
        authorization_status=proposal.authorization_status,
        decision=proposal.decision,
        owner_policy_value_count=proposal.owner_policy_value_count,
        executable_policy_authorized=proposal.executable_policy_authorized,
        engine_status=proposal.engine_status,
        selection_authorized=proposal.selection_authorized,
        external_action_performed=proposal.external_action_performed,
        investment_interpretation_generated=proposal.investment_interpretation_generated,
        production_effect=proposal.production_effect,
        broker_action=proposal.broker_action,
        artifacts=artifacts,
    )
    return BuiltQCQQQOptionsPrimaryWindowRunProposalPackage(
        policy_load=loaded,
        run_scope=scope,
        proposal=proposal,
        project_code_bytes=rendered.code_bytes,
        owner_decision_request_bytes=request_bytes,
        manifest=manifest,
    )


def write_qc_qqq_options_primary_window_run_proposal_package(
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsPrimaryWindowRunProposalPackageManifest:
    root = project_root.resolve()
    built = build_qc_qqq_options_primary_window_run_proposal_package(project_root=root)
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


def load_qc_qqq_options_primary_window_run_proposal_package(
    package_root: Path = (
        DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT
    ),
    *,
    project_root: Path = PROJECT_ROOT,
) -> BuiltQCQQQOptionsPrimaryWindowRunProposalPackage:
    root = project_root.resolve()
    expected = build_qc_qqq_options_primary_window_run_proposal_package(project_root=root)
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
        actual_inventory = tuple(sorted(path.name for path in actual_root.iterdir()))
        if actual_inventory != _PACKAGE_FILES:
            raise ValueError("package file inventory is not exact")
        if any(not path.is_file() or path.is_symlink() for path in actual_root.iterdir()):
            raise ValueError("package entries must be non-symlink regular files")
        raw = {name: (actual_root / name).read_bytes() for name in _PACKAGE_FILES}
        scope = collector_v1.QCQQQOptionsDerivedAggregateCollectorRunScope.from_json_bytes(
            raw["run_scope.json"]
        )
        proposal = collector_v1.QCQQQOptionsDerivedAggregateCollectorProposal.from_json_bytes(
            raw["proposal.json"]
        )
        manifest = QCQQQOptionsPrimaryWindowRunProposalPackageManifest.from_json_bytes(
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
        for item in manifest.artifacts:
            artifact_raw = raw[item.relative_path]
            if (
                hashlib.sha256(artifact_raw).hexdigest() != item.sha256
                or len(artifact_raw) != item.byte_count
            ):
                raise ValueError(f"artifact identity drifted: {item.relative_path}")
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsPrimaryWindowRunProposalError(
            "PROPOSAL_PACKAGE_ADMISSION_FAILED", str(exc)
        ) from exc
    return BuiltQCQQQOptionsPrimaryWindowRunProposalPackage(
        policy_load=expected.policy_load,
        run_scope=scope,
        proposal=proposal,
        project_code_bytes=raw["main.py"],
        owner_decision_request_bytes=raw["owner_decision_request.md"],
        manifest=manifest,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_PACKAGE_ROOT",
    "DEFAULT_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_PROPOSAL_POLICY_PATH",
    "BuiltQCQQQOptionsPrimaryWindowRunProposalPackage",
    "ProposalPackageArtifact",
    "QCQQQOptionsPrimaryWindowRunProposalError",
    "QCQQQOptionsPrimaryWindowRunProposalPackageManifest",
    "QCQQQOptionsPrimaryWindowRunProposalPolicy",
    "QCQQQOptionsPrimaryWindowRunProposalPolicyLoadResult",
    "build_qc_qqq_options_primary_window_run_proposal_package",
    "load_qc_qqq_options_primary_window_run_proposal_package",
    "load_qc_qqq_options_primary_window_run_proposal_policy",
    "write_qc_qqq_options_primary_window_run_proposal_package",
]
