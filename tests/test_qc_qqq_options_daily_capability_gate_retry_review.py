from __future__ import annotations

import json
from dataclasses import replace

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    daily_capability_gate_retry_review as review_module,
)
from ai_trading_system.qqq_options_research.daily_capability_gate_retry import (
    load_qc_qqq_options_daily_capability_gate_retry_evidence,
)
from ai_trading_system.qqq_options_research.daily_capability_gate_retry_review import (
    DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH,
    EXPECTED_EVIDENCE_CONTENT_SHA256,
    EXPECTED_EVIDENCE_FILE_SHA256,
    EXPECTED_PROJECT_CODE_SHA256,
    EXPECTED_RESULT_ARTIFACT_SHA256,
    OWNER_ATTESTATION_ID,
    QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord,
    QCQQQOptionsDailyCapabilityGateRetryReviewContractError,
    build_qc_qqq_options_daily_capability_gate_retry_review,
    load_qc_qqq_options_daily_capability_gate_retry_review,
)


def test_tracked_owner_review_replays_canonical_evidence_facts() -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_review()
    review = loaded.review

    assert loaded.review_path == (
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH
    )
    assert review.owner_attestation_id == OWNER_ATTESTATION_ID
    assert review.evidence_file_sha256 == EXPECTED_EVIDENCE_FILE_SHA256
    assert review.evidence_content_sha256 == EXPECTED_EVIDENCE_CONTENT_SHA256
    assert review.result_artifact_sha256 == EXPECTED_RESULT_ARTIFACT_SHA256
    assert review.confirmed_project_code_sha256 == EXPECTED_PROJECT_CODE_SHA256
    assert loaded.evidence.evidence.content_sha256 == review.evidence_content_sha256
    assert loaded.evidence.evidence_file_sha256 == review.evidence_file_sha256


def test_review_promotes_only_daily_engineering_successor() -> None:
    review = load_qc_qqq_options_daily_capability_gate_retry_review().review

    assert review.accepted_candidate_gate_status == "GO_FOR_DAILY_ENGINEERING_ONLY"
    assert review.successor_scope == "DAILY_ENGINEERING_ONLY"
    assert review.successor_registration_authorized is True
    assert review.further_external_action_authorized is False
    assert review.selection_policy_activated is False
    assert review.execution_policy_activated is False
    assert review.investment_interpretation_allowed is False
    assert review.production_effect == review.broker_action == "none"


def test_review_confirmations_are_derived_from_evidence() -> None:
    review = build_qc_qqq_options_daily_capability_gate_retry_review()

    assert review.confirmed_account_tier_free is True
    assert review.confirmed_requested_and_evaluated_range == "2021-02-22..2021-02-26"
    assert review.confirmed_five_expected_sessions is True
    assert review.confirmed_complete_daily_chain_quote_greeks_iv is True
    assert review.confirmed_positive_open_interest_each_session is True
    assert review.confirmed_orders_fills_zero is True
    assert review.confirmed_no_project_mutation is True
    assert review.confirmed_no_second_backtest is True
    assert review.confirmed_no_raw_option_rows is True
    assert review.confirmed_no_prohibited_action is True


def test_review_record_canonical_round_trip() -> None:
    review = build_qc_qqq_options_daily_capability_gate_retry_review()
    assert (
        QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.from_json_bytes(
            review.canonical_bytes
        )
        == review
    )


def test_review_record_rejects_noncanonical_bytes() -> None:
    review = build_qc_qqq_options_daily_capability_gate_retry_review()
    noncanonical = json.dumps(review.model_dump(mode="json"), sort_keys=True).encode()
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.from_json_bytes(
            noncanonical
        )


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("owner_attestation_id", "owner_attestation:forged"),
        ("evidence_file_sha256", "a" * 64),
        ("evidence_content_sha256", "b" * 64),
        ("result_artifact_sha256", "c" * 64),
        ("project_id", 1),
        ("backtest_id", "forged"),
        ("confirmed_project_code_sha256", "d" * 64),
        ("confirmed_requested_and_evaluated_range", "2021-02-23..2021-02-26"),
        ("accepted_candidate_gate_status", "UNKNOWN_EVIDENCE_INCOMPLETE"),
        ("successor_scope", "UNBOUNDED"),
        ("independent_reviewer", "collector"),
    ],
)
def test_review_identity_or_scope_tamper_fails_closed(
    field: str, replacement: object
) -> None:
    payload = build_qc_qqq_options_daily_capability_gate_retry_review().model_dump(
        mode="python"
    )
    payload[field] = replacement
    payload["content_sha256"] = "0" * 64
    with pytest.raises(ValidationError):
        QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    [
        "confirmed_account_tier_free",
        "confirmed_five_expected_sessions",
        "confirmed_complete_daily_chain_quote_greeks_iv",
        "confirmed_positive_open_interest_each_session",
        "confirmed_orders_fills_zero",
        "confirmed_no_project_mutation",
        "confirmed_no_second_backtest",
        "confirmed_no_raw_option_rows",
        "confirmed_no_prohibited_action",
        "successor_registration_authorized",
    ],
)
def test_false_owner_confirmation_cannot_be_sealed(field: str) -> None:
    payload = build_qc_qqq_options_daily_capability_gate_retry_review().model_dump(
        mode="python"
    )
    payload[field] = False
    payload["content_sha256"] = "0" * 64
    with pytest.raises(ValidationError):
        QCQQQOptionsDailyCapabilityGateRetryIndependentReviewRecord.model_validate(payload)


def test_arbitrary_evidence_bytes_cannot_promote_review(tmp_path) -> None:
    path = tmp_path / DEFAULT_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_RETRY_ATTESTATION_PATH
    path.parent.mkdir(parents=True)
    path.write_bytes(b"{}")
    with pytest.raises(
        QCQQQOptionsDailyCapabilityGateRetryReviewContractError,
        match="REVIEW_INVALID",
    ):
        load_qc_qqq_options_daily_capability_gate_retry_review(
            path=path, project_root=tmp_path
        )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"total_orders": 1}, "literal_error"),
        ({"candidate_gate_status": "UNKNOWN_EVIDENCE_INCOMPLETE"}, "literal_error"),
        ({"project_mutation_count": 1}, "literal_error"),
        ({"prohibited_actions_observed": True}, "literal_error"),
    ],
)
def test_semantically_invalid_evidence_facts_cannot_promote(
    monkeypatch, mutation: dict[str, object], expected: str
) -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_evidence()
    forged = loaded.evidence.model_copy(update=mutation)
    monkeypatch.setattr(
        review_module,
        "load_qc_qqq_options_daily_capability_gate_retry_evidence",
        lambda **_kwargs: replace(loaded, evidence=forged),
    )
    with pytest.raises(
        QCQQQOptionsDailyCapabilityGateRetryReviewContractError, match=expected
    ):
        build_qc_qqq_options_daily_capability_gate_retry_review()


def test_evidence_file_hash_mismatch_cannot_promote(monkeypatch) -> None:
    loaded = load_qc_qqq_options_daily_capability_gate_retry_evidence()
    monkeypatch.setattr(
        review_module,
        "load_qc_qqq_options_daily_capability_gate_retry_evidence",
        lambda **_kwargs: replace(loaded, evidence_file_sha256="f" * 64),
    )
    with pytest.raises(
        QCQQQOptionsDailyCapabilityGateRetryReviewContractError,
        match="evidence file SHA-256 drifted",
    ):
        build_qc_qqq_options_daily_capability_gate_retry_review()
