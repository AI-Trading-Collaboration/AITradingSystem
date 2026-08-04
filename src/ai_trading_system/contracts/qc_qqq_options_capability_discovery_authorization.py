from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

CAPABILITY_DISCOVERY_ALLOWED_ACTIONS: tuple[str, ...] = (
    "CAPTURE_EXPORT_SAFE_DERIVED_EVIDENCE",
    "CREATE_OR_MODIFY_ONE_ISOLATED_PROJECT",
    "INSPECT_DERIVED_QQQ_OPTION_VISIBILITY_AND_COUNTS",
    "INSPECT_PROJECT_BACKTEST_LEAN_IDENTITY",
    "INSPECT_RESOURCE_RUNTIME_TELEMETRY",
    "INSPECT_RESULT_ARTIFACT_PRESENCE",
    "RUN_ONE_FREE_CLOUD_BACKTEST",
)
CAPABILITY_DISCOVERY_PROHIBITED_ACTIONS: tuple[str, ...] = (
    "API_OR_CLI_OR_DIRECT_HTTP_OR_OBJECT_STORE",
    "INVESTMENT_INTERPRETATION_OR_RANGE_EXPANSION",
    "OPTIMIZATION",
    "ORDER_OR_FILL_OR_POSITION_OR_CASH_MUTATION",
    "PAID_OR_UPGRADE",
    "PAPER_OR_LIVE_OR_BROKER_OR_PRODUCTION",
    "RAW_OPTION_DATA_DOWNLOAD_OR_EXPORT_OR_LOGGING",
    "SECOND_CLOUD_BACKTEST",
)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_TEXT_PATTERN = re.compile(r"^[^\x00-\x1f\x7f]+$")


class QCCapabilityDiscoveryAuthorizationContractError(ValueError):
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


def _sorted_unique_ids(values: tuple[str, ...], field: str) -> tuple[str, ...]:
    normalized = tuple(sorted(_identifier(str(item), field) for item in values))
    if not normalized or len(normalized) != len(set(normalized)):
        raise ValueError(f"{field} must contain unique values")
    return normalized


class QCCapabilityDiscoveryScope(_StrictModel):
    requested_start: date
    requested_end: date
    maximum_runtime_minutes: Literal[10]
    maximum_projects: Literal[1]
    maximum_cloud_backtests: Literal[1]
    maximum_order_count: Literal[0]
    maximum_contract_quantity: Literal[0]
    cloud_compute_budget: Literal["FREE_TIER_ONLY_ABORT_BEFORE_PAID_OR_UPGRADE"]

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        if self.requested_start != self.requested_end:
            raise ValueError("capability discovery must use one exact exchange session")
        return self


class QCCapabilityDiscoveryActors(_StrictModel):
    collector_id: str
    independent_reviewer_id: str
    reviewer_attestation_timing: Literal["AFTER_EVIDENCE_BUNDLE_CLOSE"]
    collector_and_reviewer_must_differ: Literal[True]

    @field_validator("collector_id", "independent_reviewer_id")
    @classmethod
    def _validate_actor_id(cls, value: str, info: Any) -> str:
        return _identifier(value, str(info.field_name))

    @model_validator(mode="after")
    def _validate_actor_separation(self) -> Self:
        if self.collector_id == self.independent_reviewer_id:
            raise ValueError("collector and independent reviewer must differ")
        return self


class QCCapabilityDiscoverySafetyBoundary(_StrictModel):
    research_only: Literal[True]
    external_action_authorized: Literal[True]
    project_mutation_authorized: Literal[True]
    cloud_backtest_authorized: Literal[True]
    derived_qqq_option_counts_allowed: Literal[True]
    result_artifact_presence_inspection_allowed: Literal[True]
    api_allowed: Literal[False]
    cli_allowed: Literal[False]
    direct_http_allowed: Literal[False]
    object_store_allowed: Literal[False]
    raw_options_data_download_allowed: Literal[False]
    raw_rows_may_be_logged: Literal[False]
    strategy_execution_allowed: Literal[False]
    orders_allowed: Literal[False]
    fills_allowed: Literal[False]
    positions_allowed: Literal[False]
    optimization_allowed: Literal[False]
    investment_interpretation_allowed: Literal[False]
    range_expansion_allowed: Literal[False]
    paper_shadow_allowed: Literal[False]
    live_allowed: Literal[False]
    broker_action_allowed: Literal[False]
    production_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCCapabilityDiscoveryAuthorization(_StrictModel):
    schema_version: Literal["qc_qqq_options_capability_discovery_authorization.v1"]
    policy_id: Literal["qc_qqq_options_capability_discovery_authorization_v1"]
    policy_version: Literal["1.0.0"]
    status: Literal["OWNER_REVIEWED_ACTIVE"]
    owner_authorization_id: Literal[
        "owner_decision:TRADING-2480:2026-08-04:"
        "authorize_single_no_order_qc_capability_discovery_run_v1"
    ]
    platform: Literal["QuantConnect"]
    run_role: Literal["CAPABILITY_DISCOVERY_NO_ORDER_NOT_RESEARCH_CONCLUSION"]
    authorization_effective_date: date
    rationale: str
    intended_effect: str
    validation_plan: str
    review_condition: str
    expiry_condition: str
    revocation_condition: str
    prior_admission_policy_path: str
    prior_admission_policy_sha256: str
    prior_evidence_path: str
    prior_evidence_sha256: str
    prior_receipt_path: str
    prior_receipt_sha256: str
    prior_admission_decision: Literal["CAPABILITY_OR_LICENSE_BLOCKED"]
    prior_bounded_pilot_preparation_allowed: Literal[False]
    prior_confirmed_item_count: Literal[7]
    prior_required_item_count: Literal[21]
    prior_confirmed_field_count: Literal[3]
    prior_required_field_count: Literal[12]
    calendar_id: Literal["XNYS"]
    calendar_policy_path: str
    calendar_policy_sha256: str
    research_window_policy_path: str
    research_window_policy_sha256: str
    research_window_split_id: Literal["historical_seen_2025_sample"]
    research_window_start: date
    research_window_end: date
    primary_research_start: date
    scope: QCCapabilityDiscoveryScope
    actors: QCCapabilityDiscoveryActors
    allowed_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    safety: QCCapabilityDiscoverySafetyBoundary
    decision: Literal["CAPABILITY_DISCOVERY_AUTHORIZED_NO_ORDER"]

    @field_validator(
        "rationale",
        "intended_effect",
        "validation_plan",
        "review_condition",
        "expiry_condition",
        "revocation_condition",
        "prior_admission_policy_path",
        "prior_evidence_path",
        "prior_receipt_path",
        "calendar_policy_path",
        "research_window_policy_path",
    )
    @classmethod
    def _validate_authorization_text(cls, value: str, info: Any) -> str:
        return _required_text(value, str(info.field_name))

    @field_validator(
        "prior_admission_policy_sha256",
        "prior_evidence_sha256",
        "prior_receipt_sha256",
        "calendar_policy_sha256",
        "research_window_policy_sha256",
    )
    @classmethod
    def _validate_authorization_sha256(cls, value: str) -> str:
        if not _SHA256_PATTERN.fullmatch(value):
            raise ValueError("authorization bindings must be lowercase SHA-256")
        return value

    @field_validator("allowed_actions")
    @classmethod
    def _validate_allowed_actions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = _sorted_unique_ids(value, "allowed_actions")
        if checked != CAPABILITY_DISCOVERY_ALLOWED_ACTIONS:
            raise ValueError("capability discovery allowed-actions boundary mismatch")
        return checked

    @field_validator("prohibited_actions")
    @classmethod
    def _validate_prohibited_actions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        checked = _sorted_unique_ids(value, "prohibited_actions")
        if checked != CAPABILITY_DISCOVERY_PROHIBITED_ACTIONS:
            raise ValueError("capability discovery prohibited-actions boundary mismatch")
        return checked

    @model_validator(mode="after")
    def _validate_authorization(self) -> Self:
        if self.authorization_effective_date != date(2026, 8, 4):
            raise ValueError("authorization effective date mismatch")
        if (
            self.research_window_start != date(2025, 1, 2)
            or self.research_window_end != date(2025, 12, 31)
            or self.primary_research_start != date(2021, 2, 22)
        ):
            raise ValueError("reviewed research-window authority mismatch")
        if not (
            self.research_window_start
            <= self.scope.requested_start
            <= self.scope.requested_end
            <= self.research_window_end
        ):
            raise ValueError("requested discovery session is outside the reviewed window")
        return self

    def semantic_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.semantic_payload())

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


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
