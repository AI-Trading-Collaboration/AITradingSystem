from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.daily_capability_gate import EXPECTED_SESSIONS
from ai_trading_system.qqq_options_research.daily_capability_gate_retry import (
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_PATH,
    QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult,
    load_qc_qqq_options_daily_capability_gate_retry_evidence,
)

DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH = Path(
    "inputs/external_validation/"
    "qc_qqq_options_daily_capability_gate_retry_attestation_20260808.json"
)

OWNER_ATTESTATION_ID = (
    "owner_attestation:TRADING-2500:2026-08-08:"
    "accept_qc_daily_capability_retry_evidence_v1"
)
EXPECTED_EVIDENCE_FILE_SHA256 = (
    "829cd5de1d7691d98bfbf3554d27fabcda64598f3e26ce4747beddaf03f1c3b0"
)
EXPECTED_EVIDENCE_CONTENT_SHA256 = (
    "c19c2601e35fe6ee0495a041c1ddeafc52aa275a18856585b36ba2e6435fc609"
)
EXPECTED_RESULT_ARTIFACT_SHA256 = (
    "3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09"
)
EXPECTED_PROJECT_CODE_SHA256 = (
    "1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139"
)
EXPECTED_PROJECT_ID = 34808569
EXPECTED_BACKTEST_ID = "077252aa78ce2e0a7c3b9b4c38a554f7"
EXPECTED_RANGE = "2021-02-22..2021-02-26"

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]*$")
_UNSEALED_SHA256 = "0" * 64


class QCQQQOptionsDailyCapabilityGateRetryReviewContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _identifier(value: str, field: str) -> str:
    if not value or value != value.strip() or not _IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be a normalized portable identifier")
    return value


def _sha256(value: str, field: str) -> str:
    if not _SHA256_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return value


def _canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _canonical_sha256(payload: object) -> str:
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

    @model_validator(mode="after")
    def _validate_seal(self) -> Self:
        expected = _canonical_sha256(self.semantic_payload())
        if self.content_sha256 not in {_UNSEALED_SHA256, expected}:
            raise ValueError("content_sha256 does not match canonical semantic payload")
        return self

    @classmethod
    def seal(cls, **payload: Any) -> Self:
        candidate = cls.model_validate({**payload, "content_sha256": _UNSEALED_SHA256})
        return cls.model_validate(
            {**payload, "content_sha256": _canonical_sha256(candidate.semantic_payload())}
        )

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> Self:
        try:
            decoded = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("review record is not valid UTF-8 JSON") from exc
        if not isinstance(decoded, dict):
            raise ValueError("review record JSON root must be an object")
        record = cls.model_validate(decoded, strict=False)
        if record.content_sha256 == _UNSEALED_SHA256:
            raise ValueError("review record is unsealed")
        if record.canonical_bytes != raw:
            raise ValueError("review record bytes are not canonical")
        return record


class QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord(_SealedModel):
    schema_version: Literal[
        "qc_qqq_options_daily_capability_gate_retry_independent_review.v1"
    ]
    record_id: str
    owner_attestation_id: Literal[
        "owner_attestation:TRADING-2500:2026-08-08:"
        "accept_qc_daily_capability_retry_evidence_v1"
    ]
    owner_attestation_date: date
    evidence_path: Literal[
        "inputs/external_validation/"
        "qc_qqq_options_daily_capability_gate_retry_evidence_20260808.json"
    ]
    evidence_file_sha256: Literal[
        "829cd5de1d7691d98bfbf3554d27fabcda64598f3e26ce4747beddaf03f1c3b0"
    ]
    evidence_content_sha256: Literal[
        "c19c2601e35fe6ee0495a041c1ddeafc52aa275a18856585b36ba2e6435fc609"
    ]
    result_artifact_sha256: Literal[
        "3e3b41b529294ac31c9559a6d46a7c8ad777063304adde72a72437d240751a09"
    ]
    project_id: Literal[34808569]
    backtest_id: Literal["077252aa78ce2e0a7c3b9b4c38a554f7"]
    confirmed_account_tier_free: Literal[True]
    confirmed_project_code_sha256: Literal[
        "1da0d834d5509aabd7fb3baeeff9b8b3f56eed3d9ba095679f84fda926843139"
    ]
    confirmed_requested_and_evaluated_range: Literal["2021-02-22..2021-02-26"]
    confirmed_five_expected_sessions: Literal[True]
    confirmed_complete_daily_chain_quote_greeks_iv: Literal[True]
    confirmed_positive_open_interest_each_session: Literal[True]
    confirmed_orders_fills_zero: Literal[True]
    confirmed_no_project_mutation: Literal[True]
    confirmed_no_second_backtest: Literal[True]
    confirmed_no_raw_option_rows: Literal[True]
    confirmed_no_prohibited_action: Literal[True]
    accepted_candidate_gate_status: Literal["GO_FOR_DAILY_ENGINEERING_ONLY"]
    successor_scope: Literal["DAILY_ENGINEERING_ONLY"]
    independent_reviewer: Literal["project_owner"]
    successor_registration_authorized: Literal[True]
    further_external_action_authorized: Literal[False]
    selection_policy_activated: Literal[False]
    execution_policy_activated: Literal[False]
    investment_interpretation_allowed: Literal[False]
    production_effect: Literal["none"]
    broker_action: Literal["none"]

    @field_validator("record_id")
    @classmethod
    def _validate_record_id(cls, value: str) -> str:
        return _identifier(value, "record_id")

    @field_validator("owner_attestation_date")
    @classmethod
    def _validate_attestation_date(cls, value: date) -> date:
        if value != date(2026, 8, 8):
            raise ValueError("Owner attestation date drifted")
        return value


@dataclass(frozen=True)
class QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult:
    review: QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord
    review_path: Path
    review_file_sha256: str
    evidence: QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult


def _require_bound_regular_file(path: Path, *, project_root: Path, field: str) -> Path:
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
    return resolved


def _build_review_from_evidence(
    evidence: QCQQQOptionsDailyCapabilityGateRetryEvidenceLoadResult,
    *,
    record_id: str,
) -> QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord:
    facts = evidence.evidence
    complete_sessions = (
        facts.evaluated_sessions == EXPECTED_SESSIONS
        and tuple(item.session_date for item in facts.session_evidence)
        == EXPECTED_SESSIONS
    )
    complete_chain = all(
        item.option_chain_present
        and item.contract_count > 0
        and item.two_sided_quote_count == item.contract_count
        and item.finite_greeks_count == item.contract_count
        and item.finite_implied_volatility_count == item.contract_count
        for item in facts.session_evidence
    )
    positive_oi = all(
        item.positive_open_interest_count > 0 for item in facts.session_evidence
    )
    requested_evaluated_range = (
        f"{facts.requested_start.isoformat()}..{facts.requested_end.isoformat()}"
        if (facts.requested_start, facts.requested_end)
        == (facts.evaluated_start, facts.evaluated_end)
        else "MISMATCH"
    )
    return QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.seal(
        schema_version=(
            "qc_qqq_options_daily_capability_gate_retry_independent_review.v1"
        ),
        record_id=record_id,
        owner_attestation_id=OWNER_ATTESTATION_ID,
        owner_attestation_date=date(2026, 8, 8),
        evidence_path=DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_EVIDENCE_PATH.as_posix(),
        evidence_file_sha256=evidence.evidence_file_sha256,
        evidence_content_sha256=facts.content_sha256,
        result_artifact_sha256=facts.result_artifact_sha256,
        project_id=facts.project_id,
        backtest_id=facts.backtest_id,
        confirmed_account_tier_free=facts.account_tier == "FREE",
        confirmed_project_code_sha256=facts.project_code_lf_sha256,
        confirmed_requested_and_evaluated_range=requested_evaluated_range,
        confirmed_five_expected_sessions=complete_sessions,
        confirmed_complete_daily_chain_quote_greeks_iv=complete_chain,
        confirmed_positive_open_interest_each_session=positive_oi,
        confirmed_orders_fills_zero=(
            facts.total_orders == facts.result_order_count == facts.fills == 0
        ),
        confirmed_no_project_mutation=facts.project_mutation_count == 0,
        confirmed_no_second_backtest=not facts.second_cloud_backtest_used,
        confirmed_no_raw_option_rows=(
            not facts.raw_options_rows_present and not facts.raw_rows_logged
        ),
        confirmed_no_prohibited_action=not facts.prohibited_actions_observed,
        accepted_candidate_gate_status=facts.candidate_gate_status,
        successor_scope="DAILY_ENGINEERING_ONLY",
        independent_reviewer="project_owner",
        successor_registration_authorized=True,
        further_external_action_authorized=False,
        selection_policy_activated=False,
        execution_policy_activated=False,
        investment_interpretation_allowed=False,
        production_effect="none",
        broker_action="none",
    )


def build_qc_qqq_options_daily_capability_gate_retry_review(
    *,
    record_id: str = "qc_qqq_options_daily_capability_gate_retry_review_20260808_v1",
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord:
    try:
        evidence = load_qc_qqq_options_daily_capability_gate_retry_evidence(
            project_root=project_root
        )
        if evidence.evidence_file_sha256 != EXPECTED_EVIDENCE_FILE_SHA256:
            raise ValueError("evidence file SHA-256 drifted")
        if evidence.evidence.content_sha256 != EXPECTED_EVIDENCE_CONTENT_SHA256:
            raise ValueError("evidence content SHA-256 drifted")
        review = _build_review_from_evidence(evidence, record_id=record_id)
    except QCQQQOptionsDailyCapabilityGateRetryReviewContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateRetryReviewContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_REVIEW_INVALID", str(exc)
        ) from exc
    return review


def load_qc_qqq_options_daily_capability_gate_retry_review(
    path: Path = DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult:
    root = project_root.resolve()
    try:
        review_path = _require_bound_regular_file(
            path, project_root=root, field="daily capability retry review"
        )
        raw = review_path.read_bytes()
        review = (
            QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.from_json_bytes(raw)
        )
        evidence = load_qc_qqq_options_daily_capability_gate_retry_evidence(
            project_root=root
        )
        expected = _build_review_from_evidence(evidence, record_id=review.record_id)
        if review != expected:
            raise ValueError("review does not replay from canonical evidence facts")
        if evidence.evidence_file_sha256 != EXPECTED_EVIDENCE_FILE_SHA256:
            raise ValueError("evidence file SHA-256 drifted")
    except QCQQQOptionsDailyCapabilityGateRetryReviewContractError:
        raise
    except (OSError, TypeError, ValueError) as exc:
        raise QCQQQOptionsDailyCapabilityGateRetryReviewContractError(
            "QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_REVIEW_INVALID", str(exc)
        ) from exc
    return QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult(
        review=review,
        review_path=review_path,
        review_file_sha256=hashlib.sha256(raw).hexdigest(),
        evidence=evidence,
    )


__all__ = [
    "DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH",
    "OWNER_ATTESTATION_ID",
    "QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord",
    "QCQQQOptionsDailyCapabilityGateRetryReviewContractError",
    "QCQQQOptionsDailyCapabilityGateRetryReviewLoadResult",
    "build_qc_qqq_options_daily_capability_gate_retry_review",
    "load_qc_qqq_options_daily_capability_gate_retry_review",
]
