from __future__ import annotations

import hashlib
import json
import re
from datetime import date
from decimal import Decimal
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, ValidationInfo, field_validator, model_validator

OWNER_ATTESTATION_ID = (
    "owner_attestation:TRADING-2480:2026-08-04:accept_qc_capability_discovery_evidence_v1"
)
POST_TERMINAL_RESULT_DOWNLOAD_EXCEPTION = (
    "post_terminal_reviewer_result_json_downloaded_no_raw_option_rows"
)

_UNSEALED_SHA256 = "0" * 64
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")


class QCCapabilityDiscoveryReviewContractError(ValueError):
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


class QCCapabilityDiscoveryReviewRawFieldScan(_StrictModel):
    scan_method: Literal["CASE_INSENSITIVE_JSON_TEXT_FIELD_NAME_SCAN_V1"]
    bid_price_occurrences: Literal[0]
    ask_price_occurrences: Literal[0]
    open_interest_occurrences: Literal[0]
    option_chain_occurrences: Literal[0]
    contract_count_occurrences: Literal[0]
    raw_option_rows_present: Literal[False]


class QCCapabilityDiscoveryReviewArtifactSummary(_StrictModel):
    artifact_kind: Literal["QC_AGGREGATE_BACKTEST_RESULT_JSON"]
    file_name: Literal["Hyper Active Red Galago.json"]
    file_sha256: str
    byte_count: Literal[17322]
    downloaded_after_terminal: Literal[True]
    copied_into_repository: Literal[False]
    backtest_name: Literal["Hyper Active Red Galago"]
    backtest_id: Literal["cc699b521d94b44e877b4fc18d514181"]
    result_status: Literal["Completed"]
    evaluated_start: date
    evaluated_end: date
    state_order_count: Literal[0]
    statistics_total_orders: Literal[0]
    closed_trade_count: Literal[0]
    profit_loss_record_count: Literal[0]
    start_equity_usd: Decimal
    end_equity_usd: Decimal
    fees_usd: Decimal
    holdings_usd: Decimal
    volume_usd: Decimal
    raw_field_scan: QCCapabilityDiscoveryReviewRawFieldScan

    @field_validator("file_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "file_sha256")

    @model_validator(mode="after")
    def _validate_summary(self) -> Self:
        exact_date = date(2025, 12, 2)
        if (self.evaluated_start, self.evaluated_end) != (exact_date, exact_date):
            raise ValueError("review artifact range must match the authorized session")
        if self.start_equity_usd != self.end_equity_usd:
            raise ValueError("review artifact must preserve cash")
        if any(value != 0 for value in (self.fees_usd, self.holdings_usd, self.volume_usd)):
            raise ValueError("review artifact must retain zero fees, holdings, and volume")
        return self


class QCCapabilityDiscoveryReviewPageAssertions(_StrictModel):
    project_id: Literal["34808569"]
    backtest_id: Literal["cc699b521d94b44e877b4fc18d514181"]
    account_tier: Literal["FREE"]
    cloud_compute: Literal["Community B-MICRO"]
    build_id: Literal["200b2b-8b7b28"]
    deployment_seconds: Decimal
    project_code_sha256: str
    free_tier_and_compute_reviewed: Literal[True]
    build_and_deployment_reviewed: Literal[True]

    @field_validator("project_code_sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return _sha256(value, "project_code_sha256")

    @model_validator(mode="after")
    def _validate_page_assertions(self) -> Self:
        if self.deployment_seconds != Decimal("16.393"):
            raise ValueError("reviewed deployment duration drifted")
        return self


class QCCapabilityDiscoveryReviewSafety(_StrictModel):
    original_evidence_rewritten: Literal[False]
    reviewer_result_artifact_downloaded_after_terminal: Literal[True]
    raw_options_data_downloaded: Literal[False]
    raw_option_rows_in_review_artifact: Literal[False]
    review_artifact_committed_to_repository: Literal[False]
    second_cloud_backtest_used: Literal[False]
    project_modified_after_terminal: Literal[False]
    api_cli_http_object_store_used: Literal[False]
    selection_or_pilot_activated: Literal[False]
    investment_interpretation_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]


class QCCapabilityDiscoveryReview(_StrictModel):
    schema_version: Literal["qc_qqq_options_capability_discovery_review.v1"]
    review_id: str
    owner_attestation_id: Literal[
        "owner_attestation:TRADING-2480:2026-08-04:accept_qc_capability_discovery_evidence_v1"
    ]
    attested_on: date
    reviewer_id: Literal["project_owner"]
    collector_id: Literal["codex_pilot_coordinator"]
    evidence_path: Literal[
        "inputs/external_validation/qc_qqq_options_capability_discovery_evidence_20260804.json"
    ]
    evidence_file_sha256: str
    evidence_semantic_sha256: str
    project_id: Literal["34808569"]
    backtest_id: Literal["cc699b521d94b44e877b4fc18d514181"]
    page_assertions: QCCapabilityDiscoveryReviewPageAssertions
    review_artifact: QCCapabilityDiscoveryReviewArtifactSummary
    exceptions: tuple[str, ...]
    review_decision: Literal["ACCEPTED_WITH_DISCLOSED_POST_TERMINAL_ARTIFACT_DOWNLOAD"]
    owner_authorization_state: Literal["EXPIRED_AFTER_FIRST_RUN_TERMINAL"]
    prior_admission_decision: Literal["CAPABILITY_OR_LICENSE_BLOCKED"]
    bounded_pilot_preparation_allowed: Literal[False]
    option_event_dq_status: Literal["NOT_EVALUATED"]
    option_event_pit_status: Literal["NOT_EVALUATED"]
    safety: QCCapabilityDiscoveryReviewSafety
    content_sha256: str

    @field_validator("review_id")
    @classmethod
    def _validate_review_id(cls, value: str) -> str:
        return _identifier(value, "review_id")

    @field_validator(
        "evidence_file_sha256",
        "evidence_semantic_sha256",
        "content_sha256",
    )
    @classmethod
    def _validate_hashes(cls, value: str, info: ValidationInfo) -> str:
        return _sha256(value, str(info.field_name))

    @field_validator("exceptions")
    @classmethod
    def _validate_exceptions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != (POST_TERMINAL_RESULT_DOWNLOAD_EXCEPTION,):
            raise ValueError("review must retain the exact disclosed exception")
        return value

    @model_validator(mode="after")
    def _validate_review(self, info: ValidationInfo) -> Self:
        # Literal typing proves this for normal inputs; retain an explicit
        # runtime invariant so actor separation remains visible in the contract.
        if self.reviewer_id == self.collector_id:  # type: ignore[comparison-overlap]
            raise ValueError("independent reviewer and collector must differ")
        if self.attested_on != date(2026, 8, 4):
            raise ValueError("attestation date drifted")
        if self.project_id != self.page_assertions.project_id:
            raise ValueError("reviewed project identity mismatch")
        if self.backtest_id not in {
            self.page_assertions.backtest_id,
            self.review_artifact.backtest_id,
        }:
            raise ValueError("reviewed backtest identity mismatch")
        allow_unsealed = bool(info.context and info.context.get("allow_unsealed") is True)
        if allow_unsealed and self.content_sha256 == _UNSEALED_SHA256:
            return self
        if self.content_sha256 != self.compute_content_sha256():
            raise ValueError("content SHA-256 does not match canonical review semantics")
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
            raise QCCapabilityDiscoveryReviewContractError(
                "QC_CAPABILITY_DISCOVERY_REVIEW_HASH_CALLER_SUPPLIED",
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
            raise QCCapabilityDiscoveryReviewContractError(
                "QC_CAPABILITY_DISCOVERY_REVIEW_INVALID",
                str(exc),
            ) from exc
        if content != record.canonical_bytes:
            raise QCCapabilityDiscoveryReviewContractError(
                "QC_CAPABILITY_DISCOVERY_REVIEW_NOT_CANONICAL",
                "review bytes do not match canonical JSON encoding",
            )
        return record
