from __future__ import annotations

import copy
import hashlib

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review,
)

review = growth_action_value_mandatory_veto_historical_pit_source_candidate_evidence_review


def _payload() -> dict[str, object]:
    loaded = review.load_historical_pit_source_candidate_evidence_review()
    return copy.deepcopy(loaded.policy.model_dump(mode="json"))


def test_review_replays_s11_and_exact_primary_window() -> None:
    loaded = review.load_historical_pit_source_candidate_evidence_review()

    assert loaded.file_sha256 == hashlib.sha256(loaded.path.read_bytes()).hexdigest()
    assert loaded.canonical_sha256 == loaded.policy.canonical_sha256
    assert loaded.s11_static_contract.policy.policy_id.endswith(
        "historical_pit_static_authority_receipt_contract_v1"
    )
    assert loaded.policy.primary_research_window.requested_start.isoformat() == "2021-02-22"
    assert loaded.policy.primary_research_window.requested_end.isoformat() == "2025-12-02"
    assert loaded.policy.primary_research_window.exact_session_count == 1202
    assert loaded.terminal == (
        "S12_OWNER_APPROVAL_PACKET_READY_EXACT_HISTORICAL_COVERAGE_UNPROVEN_0_OF_5"
    )


def test_review_preserves_s11_state_while_recommending_next_dispositions() -> None:
    rows = review.load_historical_pit_source_candidate_evidence_review().policy.source_rows

    assert tuple(row.predecessor_candidate_disposition for row in rows) == (
        "VENDOR_EVIDENCE_REQUIRED",
        "VENDOR_EVIDENCE_REQUIRED",
        "FREEZE_CANDIDATE",
        "INVENTORY_ONLY",
        "INVENTORY_ONLY",
    )
    assert tuple(row.recommendation for row in rows) == (
        "KEEP_VENDOR_EVIDENCE_REQUIRED",
        "KEEP_VENDOR_EVIDENCE_REQUIRED",
        "RETAIN_FROZEN_CANDIDATE_FAMILY",
        "PROMOTE_TO_OWNER_FREEZE_CANDIDATE_AFTER_EXACT_DOCUMENT_INVENTORY",
        "PROMOTE_TO_OWNER_FREEZE_CANDIDATE_AFTER_EXACT_DOCUMENT_INVENTORY",
    )
    assert [row.candidate_id for row in rows if row.candidate_source_approved_after_review] == [
        "federal_reserve_fomc_schedule_capture_v1"
    ]
    assert all(row.recommendation_requires_owner_approval for row in rows)
    assert all(row.exact_authority_identity is None for row in rows)
    assert all(row.exact_authority_identity_frozen is False for row in rows)
    assert all(row.historical_coverage_proven is False for row in rows)
    assert all(row.source_contract_admitted is False for row in rows)
    assert all(row.runtime_authorized is False for row in rows)
    assert all(row.blocker_remediated is False for row in rows)


def test_public_discovery_separates_nominal_scope_from_pit_coverage() -> None:
    rows = review.load_historical_pit_source_candidate_evidence_review().policy.source_rows

    assert all(row.coverage_assessment.nominal_2021_2025_scope_located for row in rows)
    assert all(
        row.coverage_assessment.exact_remote_bytes_and_digests_frozen is False for row in rows
    )
    assert all(
        row.coverage_assessment.per_row_historical_available_at_proven is False for row in rows
    )
    assert all(
        row.coverage_assessment.complete_revision_or_reissue_ledger_proven is False for row in rows
    )
    assert all(row.coverage_assessment.exact_1202_cutoff_coverage_proven is False for row in rows)
    assert all(
        locator.remote_payload_sha256 is None
        for row in rows
        for locator in row.public_evidence_locators
    )


def test_fee_evidence_does_not_turn_reference_prices_into_quotes() -> None:
    rows = review.load_historical_pit_source_candidate_evidence_review().policy.source_rows

    assert rows[0].fee_evidence.fee_state == (
        "PUBLISHED_SELF_SERVE_REFERENCE_NOT_REQUIRED_SCOPE_QUOTE"
    )
    assert "PREMIUM_USD_59_PER_MONTH_BILLED_ANNUALLY_PUBLIC_REFERENCE" in (
        rows[0].fee_evidence.published_reference_points
    )
    assert rows[1].fee_evidence.fee_state == (
        "PRODUCT_QUOTE_UNKNOWN_ALL_ACCESS_REFERENCE_NOT_SUBSTITUTE"
    )
    assert all(row.fee_evidence.exact_required_scope_quote_obtained is False for row in rows)
    assert all(row.fee_evidence.purchase_authorized is False for row in rows)


def test_official_schedule_recommendations_remain_owner_gated() -> None:
    policy = review.load_historical_pit_source_candidate_evidence_review().policy
    decisions = policy.owner_decision_requests

    assert tuple(request.request_id for request in decisions) == (
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
    )
    assert decisions[0].risk_tier == "R2_MATERIAL_EXTERNAL_CHANGE"
    assert decisions[1].risk_tier == "R1_BOUNDED_RESEARCH_SANDBOX"
    assert all(request.approval_granted_in_this_artifact is False for request in decisions)
    assert all(request.automatic_source_state_change_allowed is False for request in decisions)


def test_review_records_public_web_discovery_without_inventing_request_count() -> None:
    activity = (
        review.load_historical_pit_source_candidate_evidence_review()
        .policy.observed_external_activity
    )

    assert activity.public_documentation_web_discovery_performed is True
    assert activity.exact_browser_http_request_count_recorded is False
    assert activity.exact_browser_http_request_count is None
    zero_fields = activity.model_dump()
    zero_fields.pop("public_documentation_web_discovery_performed")
    zero_fields.pop("exact_browser_http_request_count_recorded")
    zero_fields.pop("exact_browser_http_request_count")
    assert set(zero_fields.values()) == {0}


def test_review_preserves_non_executable_boundary() -> None:
    policy = review.load_historical_pit_source_candidate_evidence_review().policy

    assert policy.safety.public_documentation_web_discovery_allowed is True
    assert policy.safety.provider_api_query_allowed is False
    assert policy.safety.vendor_contact_allowed is False
    assert policy.safety.paid_product_purchase_allowed is False
    assert policy.safety.official_document_payload_download_allowed is False
    assert policy.safety.real_market_payload_download_allowed is False
    assert policy.safety.source_inventory_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.real_dq_allowed is False
    assert policy.safety.backtest_allowed is False
    assert policy.safety.production_effect == "none"
    assert policy.safety.broker_action == "none"


def test_policy_rejects_missing_source_row() -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    rows.pop()

    with pytest.raises(ValidationError, match="five-source evidence review surface drifted"):
        review.HistoricalPITSourceCandidateEvidenceReview.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("candidate_source_approved_after_review", True),
        ("exact_authority_identity", "fabricated-authority"),
        ("exact_authority_identity_frozen", True),
        ("historical_coverage_proven", True),
        ("source_contract_admitted", True),
        ("runtime_authorized", True),
        ("blocker_remediated", True),
    ),
)
def test_policy_rejects_premature_source_state(field: str, value: object) -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    first[field] = value

    with pytest.raises(ValidationError):
        review.HistoricalPITSourceCandidateEvidenceReview.model_validate(payload)


def test_policy_rejects_remote_digest_without_exact_capture() -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    locators = first["public_evidence_locators"]
    assert isinstance(locators, list)
    locator = locators[0]
    assert isinstance(locator, dict)
    locator["remote_payload_sha256"] = "0" * 64

    with pytest.raises(ValidationError):
        review.HistoricalPITSourceCandidateEvidenceReview.model_validate(payload)


def test_policy_rejects_silent_owner_approval() -> None:
    payload = _payload()
    requests = payload["owner_decision_requests"]
    assert isinstance(requests, list)
    first = requests[0]
    assert isinstance(first, dict)
    first["approval_granted_in_this_artifact"] = True

    with pytest.raises(ValidationError):
        review.HistoricalPITSourceCandidateEvidenceReview.model_validate(payload)
