from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

CAPABILITY_DISCOVERY_RESULT_SURFACES: tuple[str, ...] = (
    "CODE",
    "DOWNLOAD_RESULTS",
    "INSIGHTS",
    "LOGS",
    "ORDERS",
    "OVERVIEW",
    "REPORT",
    "TRADES",
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class QCCapabilityDiscoveryEvidenceContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _required_text(value: str, field: str) -> str:
    if not value or value != value.strip() or any(ord(char) < 32 for char in value):
        raise ValueError(f"{field} must be non-empty normalized text")
    return value


def _identifier(value: str, field: str) -> str:
    checked = _required_text(value, field)
    if not _IDENTIFIER_PATTERN.fullmatch(checked):
        raise ValueError(f"{field} must be a stable identifier")
    return checked


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _utc(value: datetime, field: str) -> datetime:
    offset = value.utcoffset()
    if value.tzinfo is None or offset is None or offset != timedelta(0):
        raise ValueError(f"{field} must use UTC")
    return value


def _canonical_json_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        ).encode("utf-8")
        + b"\n"
    )


class QCCapabilityDiscoveryResultSurface(_StrictModel):
    surface_id: str
    status: Literal["OBSERVED_AVAILABLE"]
    downloaded: Literal[False]

    @field_validator("surface_id")
    @classmethod
    def _validate_surface_id(cls, value: str) -> str:
        return _identifier(value, "surface_id")


class QCCapabilityDiscoveryEvidenceSafety(_StrictModel):
    research_only: Literal[True]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    raw_options_data_downloaded: Literal[False]
    raw_rows_logged: Literal[False]
    result_artifacts_downloaded: Literal[False]
    api_used: Literal[False]
    cli_used: Literal[False]
    direct_http_used: Literal[False]
    object_store_used: Literal[False]
    optimization_used: Literal[False]
    second_cloud_backtest_used: Literal[False]
    paper_shadow_used: Literal[False]
    live_used: Literal[False]
    broker_action_used: Literal[False]
    production_used: Literal[False]
    account_or_broker_identifiers_included: Literal[False]
    secrets_included: Literal[False]
    investment_metrics_included: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCCapabilityDiscoveryEvidence(_StrictModel):
    schema_version: Literal["qc_qqq_options_capability_discovery_evidence.v1"]
    evidence_id: str
    platform: Literal["QuantConnect"]
    owner_authorization_id: Literal[
        "owner_decision:TRADING-2480:2026-08-04:"
        "authorize_single_no_order_qc_capability_discovery_run_v1"
    ]
    authorization_policy_path: Literal[
        "config/research/qc_qqq_options_capability_discovery_authorization_v1.yaml"
    ]
    authorization_policy_sha256: str
    authorization_canonical_sha256: str
    authorization_terminal_state: Literal["EXPIRED_AFTER_FIRST_RUN_TERMINAL"]
    run_role: Literal["CAPABILITY_DISCOVERY_NO_ORDER_NOT_RESEARCH_CONCLUSION"]
    collector_id: Literal["codex_pilot_coordinator"]
    independent_reviewer_id: Literal["project_owner"]
    independent_review_status: Literal["PENDING_OWNER_REVIEW"]
    repository_code_sha: str
    project_code_sha256: str
    project_id: str
    project_name: str
    project_locator: str
    backtest_id: str
    backtest_name: str
    build_id: str
    lean_engine_identity: str
    cloud_compute: Literal["Community B-MICRO"]
    account_tier: Literal["FREE"]
    submitted_at_utc: datetime
    terminal_at_utc: datetime
    requested_start: date
    requested_end: date
    evaluated_start: date
    evaluated_end: date
    option_observation_at_new_york: datetime
    algorithm_runtime_seconds: Decimal
    deployment_seconds: Decimal
    data_point_count: int
    data_points_per_second: int
    option_chain_present: Literal[True]
    option_contract_count: int
    two_sided_quote_count: int
    open_interest_nonzero_count: int
    total_orders: Literal[0]
    fills: Literal[0]
    holdings: Literal[0]
    volume: Literal[0]
    total_fees_usd: Decimal
    start_equity_usd: Decimal
    end_equity_usd: Decimal
    portfolio_invested: Literal[False]
    result_surfaces: tuple[QCCapabilityDiscoveryResultSurface, ...]
    prior_admission_decision: Literal["CAPABILITY_OR_LICENSE_BLOCKED"]
    bounded_pilot_preparation_allowed: Literal[False]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    decision: Literal["CAPABILITY_DISCOVERY_EVIDENCE_COLLECTED_REVIEW_PENDING"]
    limitations: tuple[str, ...]
    safety: QCCapabilityDiscoveryEvidenceSafety
    content_sha256: str

    @field_validator(
        "evidence_id",
        "project_id",
        "backtest_id",
        "build_id",
    )
    @classmethod
    def _validate_ids(cls, value: str, info: ValidationInfo) -> str:
        return _identifier(value, str(info.field_name))

    @field_validator(
        "project_name",
        "project_locator",
        "backtest_name",
        "lean_engine_identity",
    )
    @classmethod
    def _validate_text(cls, value: str, info: ValidationInfo) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "authorization_policy_sha256",
        "authorization_canonical_sha256",
        "project_code_sha256",
        "content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("repository_code_sha")
    @classmethod
    def _validate_repository_sha(cls, value: str) -> str:
        if not _GIT_SHA_PATTERN.fullmatch(value):
            raise ValueError("repository_code_sha must be a 40-character Git SHA")
        return value

    @field_validator("submitted_at_utc", "terminal_at_utc")
    @classmethod
    def _validate_utc_times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _utc(value, str(info.field_name))

    @field_validator("limitations")
    @classmethod
    def _validate_limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = tuple(_required_text(item, "limitations") for item in value)
        if checked != tuple(sorted(checked)) or len(checked) != len(set(checked)):
            raise ValueError("limitations must be sorted and unique")
        return checked

    @model_validator(mode="after")
    def _validate_evidence(self, info: ValidationInfo) -> Self:
        # Runtime defense in depth keeps actor separation explicit even though
        # the two Literal field types already prove this to static analysis.
        if self.collector_id == self.independent_reviewer_id:  # type: ignore[comparison-overlap]
            raise ValueError("collector and independent reviewer must differ")
        if self.submitted_at_utc >= self.terminal_at_utc:
            raise ValueError("run terminal must occur after submission")
        exact_date = date(2025, 12, 2)
        if (
            self.requested_start,
            self.requested_end,
            self.evaluated_start,
            self.evaluated_end,
        ) != (exact_date, exact_date, exact_date, exact_date):
            raise ValueError("discovery evidence must retain the reviewed one-session range")
        observation_offset = self.option_observation_at_new_york.utcoffset()
        if (
            self.option_observation_at_new_york.date() != exact_date
            or observation_offset != timedelta(hours=-5)
        ):
            raise ValueError("option observation must use the reviewed New York session")
        if self.algorithm_runtime_seconds <= 0 or self.algorithm_runtime_seconds > Decimal(600):
            raise ValueError("algorithm runtime must stay within the ten-minute authorization")
        if self.deployment_seconds <= 0 or self.deployment_seconds > Decimal(600):
            raise ValueError("deployment duration must stay within the ten-minute authorization")
        if self.data_point_count <= 0 or self.data_points_per_second <= 0:
            raise ValueError("runtime telemetry must be positive")
        if self.option_contract_count <= 0:
            raise ValueError("QQQ option visibility requires at least one observed contract")
        if not (
            0 <= self.two_sided_quote_count <= self.option_contract_count
            and 0 <= self.open_interest_nonzero_count <= self.option_contract_count
        ):
            raise ValueError("derived option counts exceed the observed chain")
        if self.total_fees_usd != 0:
            raise ValueError("no-order discovery must have zero fees")
        if self.start_equity_usd != self.end_equity_usd:
            raise ValueError("no-order discovery must preserve starting equity")
        surfaces = tuple(item.surface_id for item in self.result_surfaces)
        if surfaces != CAPABILITY_DISCOVERY_RESULT_SURFACES:
            raise ValueError("result-surface inventory drifted")
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical semantics")
        return self

    def semantic_payload_without_hash(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"content_sha256"})

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(
            _canonical_json_bytes(self.semantic_payload_without_hash())
        ).hexdigest()

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        if "content_sha256" in payload:
            raise QCCapabilityDiscoveryEvidenceContractError(
                "QC_CAPABILITY_DISCOVERY_EVIDENCE_HASH_CALLER_SUPPLIED",
                "seal computes content_sha256 and rejects caller-supplied values",
            )
        provisional = cls.model_validate(
            {**payload, "content_sha256": _UNSEALED_SHA256},
            context={"allow_unsealed": True},
        )
        return cls.model_validate(
            {**payload, "content_sha256": provisional.compute_content_sha256()}
        )

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    @classmethod
    def from_json_bytes(cls, content: bytes) -> Self:
        try:
            record = cls.model_validate_json(content)
        except ValueError as exc:
            raise QCCapabilityDiscoveryEvidenceContractError(
                "QC_CAPABILITY_DISCOVERY_EVIDENCE_INVALID",
                str(exc),
            ) from exc
        if content != record.canonical_bytes:
            raise QCCapabilityDiscoveryEvidenceContractError(
                "QC_CAPABILITY_DISCOVERY_EVIDENCE_NOT_CANONICAL",
                "evidence bytes do not match canonical JSON encoding",
            )
        return record
