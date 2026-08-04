from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_trading_system.contracts.qc_qqq_options_capability_discovery_review import (
    OWNER_ATTESTATION_ID,
    POST_TERMINAL_RESULT_DOWNLOAD_EXCEPTION,
    QCCapabilityDiscoveryReview,
    QCCapabilityDiscoveryReviewContractError,
)
from ai_trading_system.qqq_options_capability_discovery_review import (
    DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH,
    load_qc_qqq_options_capability_discovery_review,
)


def _tracked_payload() -> dict[str, object]:
    return json.loads(
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH.read_text(encoding="utf-8")
    )


def _canonical_bytes(payload: dict[str, object]) -> bytes:
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


def test_tracked_review_is_exact_canonical_and_independently_attested() -> None:
    first = load_qc_qqq_options_capability_discovery_review()
    second = load_qc_qqq_options_capability_discovery_review()
    review = first.review

    assert first.review_file_sha256 == (
        "a5c9b9357e2b50a7f69d2710b35f184829917414f0dc8e297709f2fbf14c4ca3"
    )
    assert first.review_canonical_sha256 == first.review_file_sha256
    assert first.review == second.review
    assert review.content_sha256 == (
        "fd13eabddc2cd0cd8ae2acd7b64756e24822469de4b5c55cce9008d60f147eda"
    )
    assert review.owner_attestation_id == OWNER_ATTESTATION_ID
    assert review.reviewer_id == "project_owner"
    assert review.collector_id == "codex_pilot_coordinator"
    assert review.reviewer_id != review.collector_id
    assert review.exceptions == (POST_TERMINAL_RESULT_DOWNLOAD_EXCEPTION,)


def test_review_replays_closed_evidence_without_rewriting_it() -> None:
    loaded = load_qc_qqq_options_capability_discovery_review()
    review = loaded.review
    evidence = loaded.evidence.evidence

    assert (
        loaded.evidence.evidence_file_sha256
        == review.evidence_file_sha256
        == ("2d4c14e23d8b8f824d5b4f93db257f6d4852af31a12966535d21cc5d26a4807a")
    )
    assert (
        evidence.content_sha256
        == review.evidence_semantic_sha256
        == ("bd00355e1609c591778f53f745ca2762b9da83542ee602f2faec58cc11662702")
    )
    assert evidence.independent_review_status == "PENDING_OWNER_REVIEW"
    assert review.safety.original_evidence_rewritten is False
    assert review.project_id == evidence.project_id
    assert review.backtest_id == evidence.backtest_id


def test_post_terminal_artifact_is_aggregate_zero_activity_evidence_only() -> None:
    artifact = load_qc_qqq_options_capability_discovery_review().review.review_artifact

    assert artifact.file_sha256 == (
        "e4b440e39d3402cec77c8b22264f870ccc05d935ab5c6d8b21c939bcba62f2d4"
    )
    assert artifact.byte_count == 17322
    assert artifact.artifact_kind == "QC_AGGREGATE_BACKTEST_RESULT_JSON"
    assert artifact.downloaded_after_terminal is True
    assert artifact.copied_into_repository is False
    assert artifact.state_order_count == artifact.statistics_total_orders == 0
    assert artifact.closed_trade_count == artifact.profit_loss_record_count == 0
    assert artifact.start_equity_usd == artifact.end_equity_usd
    assert artifact.fees_usd == artifact.holdings_usd == artifact.volume_usd == 0
    scan = artifact.raw_field_scan
    assert scan.raw_option_rows_present is False
    assert (
        scan.bid_price_occurrences
        + scan.ask_price_occurrences
        + scan.open_interest_occurrences
        + scan.option_chain_occurrences
        + scan.contract_count_occurrences
        == 0
    )


def test_review_acceptance_does_not_change_admission_dq_or_pilot_state() -> None:
    review = load_qc_qqq_options_capability_discovery_review().review

    assert review.review_decision == ("ACCEPTED_WITH_DISCLOSED_POST_TERMINAL_ARTIFACT_DOWNLOAD")
    assert review.owner_authorization_state == "EXPIRED_AFTER_FIRST_RUN_TERMINAL"
    assert review.prior_admission_decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert review.bounded_pilot_preparation_allowed is False
    assert review.option_event_dq_status == "NOT_EVALUATED"
    assert review.option_event_pit_status == "NOT_EVALUATED"
    assert review.safety.selection_or_pilot_activated is False
    assert review.safety.investment_interpretation_allowed is False


@pytest.mark.parametrize(
    ("container", "field", "value"),
    (
        ("root", "evidence_file_sha256", "f" * 64),
        ("root", "project_id", "99999999"),
        ("root", "backtest_id", "f" * 32),
        ("page_assertions", "build_id", "wrong-build"),
        ("page_assertions", "deployment_seconds", "16.394"),
        ("review_artifact", "file_sha256", "f" * 64),
        ("review_artifact", "byte_count", 17321),
        ("review_artifact", "statistics_total_orders", 1),
    ),
)
def test_identity_evidence_and_result_artifact_tamper_fail_closed(
    container: str,
    field: str,
    value: object,
) -> None:
    payload = _tracked_payload()
    target = payload if container == "root" else payload[container]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(QCCapabilityDiscoveryReviewContractError):
        QCCapabilityDiscoveryReview.from_json_bytes(_canonical_bytes(payload))


@pytest.mark.parametrize(
    ("container", "field", "value"),
    (
        ("root", "bounded_pilot_preparation_allowed", True),
        ("root", "option_event_dq_status", "PASS"),
        ("root", "option_event_pit_status", "PASS"),
        ("safety", "original_evidence_rewritten", True),
        ("safety", "raw_options_data_downloaded", True),
        ("safety", "raw_option_rows_in_review_artifact", True),
        ("safety", "review_artifact_committed_to_repository", True),
        ("safety", "second_cloud_backtest_used", True),
        ("safety", "selection_or_pilot_activated", True),
        ("safety", "investment_interpretation_allowed", True),
        ("raw_field_scan", "raw_option_rows_present", True),
        ("raw_field_scan", "bid_price_occurrences", 1),
    ),
)
def test_raw_data_mutation_and_activation_tamper_fail_closed(
    container: str,
    field: str,
    value: object,
) -> None:
    payload = _tracked_payload()
    if container == "root":
        target = payload
    elif container == "raw_field_scan":
        artifact = payload["review_artifact"]
        assert isinstance(artifact, dict)
        target = artifact["raw_field_scan"]
    else:
        target = payload[container]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(QCCapabilityDiscoveryReviewContractError):
        QCCapabilityDiscoveryReview.from_json_bytes(_canonical_bytes(payload))


def test_exception_actor_and_attestation_tamper_fail_closed() -> None:
    for field, value in (
        ("exceptions", []),
        ("reviewer_id", "codex_pilot_coordinator"),
        ("owner_attestation_id", "owner_attestation:wrong"),
    ):
        payload = _tracked_payload()
        payload[field] = value
        with pytest.raises(QCCapabilityDiscoveryReviewContractError):
            QCCapabilityDiscoveryReview.from_json_bytes(_canonical_bytes(payload))


def test_noncanonical_bytes_and_caller_supplied_hash_are_rejected() -> None:
    payload = _tracked_payload()
    with pytest.raises(QCCapabilityDiscoveryReviewContractError) as raised:
        QCCapabilityDiscoveryReview.from_json_bytes(
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
        )
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_REVIEW_NOT_CANONICAL"

    with pytest.raises(QCCapabilityDiscoveryReviewContractError) as raised:
        QCCapabilityDiscoveryReview.seal(**payload)
    assert raised.value.code == "QC_CAPABILITY_DISCOVERY_REVIEW_HASH_CALLER_SUPPLIED"


def test_review_default_path_is_project_relative() -> None:
    assert not DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH.is_absolute()
    assert ".." not in DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH.parts
    assert Path("inputs/external_validation") in (
        DEFAULT_QC_QQQ_OPTIONS_CAPABILITY_DISCOVERY_REVIEW_PATH.parents
    )
