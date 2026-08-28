from __future__ import annotations

import copy
import hashlib

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_source_evidence_acquisition_receipt,
)

receipt = growth_action_value_mandatory_veto_historical_pit_source_evidence_acquisition_receipt


def _payload() -> dict[str, object]:
    loaded = receipt.load_historical_pit_source_evidence_acquisition_receipt()
    return copy.deepcopy(loaded.policy.model_dump(mode="json"))


def test_receipt_replays_s12_and_hashes_every_retained_document() -> None:
    loaded = receipt.load_historical_pit_source_evidence_acquisition_receipt()

    assert loaded.file_sha256 == hashlib.sha256(loaded.path.read_bytes()).hexdigest()
    assert loaded.canonical_sha256 == loaded.policy.canonical_sha256
    assert loaded.s12_evidence_review.file_sha256 == (
        "459de4560d569db917024f6efc3f2f46e5487f98ba9bf2dc580fcc0a25e10fb9"
    )
    assert loaded.s12_evidence_review.canonical_sha256 == (
        "4900d472ba79091fc17701d17fd2ed20f925d28b55927249af0d3665e97ef82d"
    )
    assert loaded.terminal == (
        "S13_EVIDENCE_RECEIPTS_READY_VENDOR_SEND_AND_BLS_EXACT_BYTES_BLOCKED"
    )


def test_owner_approvals_replay_both_s12_requests_without_promoting_sources() -> None:
    policy = receipt.load_historical_pit_source_evidence_acquisition_receipt().policy

    assert tuple(row.request_id for row in policy.owner_approvals) == (
        "APPROVE_VENDOR_EVIDENCE_INQUIRY_SEND",
        "APPROVE_OFFICIAL_DOCUMENT_EXACT_FREEZE_INVENTORY",
    )
    assert {row.authorization_state for row in policy.owner_approvals} == {
        "EXACT_PREAUTHORIZED"
    }
    assert all(not row.automatic_technical_state_change_allowed for row in policy.owner_approvals)
    assert policy.source_state.exact_authority_identity_frozen_count == 0
    assert policy.source_state.historical_coverage_proven_count == 0
    assert policy.source_state.source_contract_admitted_count == 0
    assert policy.source_state.runtime_authorized_count == 0


def test_vendor_packets_are_prepared_but_not_sent_without_identity() -> None:
    packet = (
        receipt.load_historical_pit_source_evidence_acquisition_receipt()
        .policy.vendor_inquiry_packet
    )

    assert packet.packet_state == "PREPARED_NOT_SENT"
    assert tuple(row.vendor for row in packet.vendor_rows) == ("FMP", "CBOE_DATASHOP")
    assert packet.vendor_rows[0].required_identity_fields == ("SENDER_EMAIL",)
    assert packet.vendor_rows[1].captcha_observed is True
    assert all(row.browser_action_time_confirmation_required for row in packet.vendor_rows)
    assert all(row.form_submission_attempted is False for row in packet.vendor_rows)
    assert all(row.message_sent is False for row in packet.vendor_rows)
    assert all(row.sent_receipt is None for row in packet.vendor_rows)


def test_exact_document_inventory_keeps_authority_and_locator_provenance() -> None:
    rows = (
        receipt.load_historical_pit_source_evidence_acquisition_receipt()
        .policy.official_document_receipts
    )

    assert len(rows) == 18
    assert sum(row.authority == "FED" for row in rows) == 6
    assert sum(row.authority == "BEA" for row in rows) == 12
    assert all(row.http_status == 200 for row in rows)
    assert all(row.byte_count > 0 for row in rows)
    assert all(len(row.sha256) == 64 for row in rows)
    assert sum(
        row.locator_disposition == "S12_LOCATOR_STALE_CORRECTED_OFFICIAL_PATH"
        for row in rows
    ) == 5
    assert sum(row.locator_disposition == "S13_SAME_FAMILY_ADDITION" for row in rows) == 3


def test_failed_attempts_preserve_bls_403_and_bea_stale_locator_404() -> None:
    attempts = (
        receipt.load_historical_pit_source_evidence_acquisition_receipt()
        .policy.failed_official_document_attempts
    )

    assert len(attempts) == 15
    assert sum(row.authority == "BLS" and row.http_status == 403 for row in attempts) == 10
    assert sum(row.authority == "BEA" and row.http_status == 404 for row in attempts) == 5
    assert all(row.retained_as_authority is False for row in attempts)
    assert any(row.transport_profile == "BROWSER_LIKE_USER_AGENT_RETRY" for row in attempts)


def test_revision_gap_assessment_does_not_confuse_current_bytes_with_pit_coverage() -> None:
    rows = (
        receipt.load_historical_pit_source_evidence_acquisition_receipt()
        .policy.revision_gap_assessment
    )

    assert tuple(row.authority for row in rows) == ("FED", "BLS", "BEA")
    assert tuple(row.exact_document_receipt_count for row in rows) == (6, 0, 12)
    assert all(row.nominal_2021_2025_scope_located for row in rows)
    assert all(row.complete_revision_or_reissue_ledger_proven is False for row in rows)
    assert all(row.historical_available_at_proven is False for row in rows)
    assert all(row.exact_1202_cutoff_coverage_proven is False for row in rows)


def test_activity_counts_authorization_separately_from_technical_results() -> None:
    policy = receipt.load_historical_pit_source_evidence_acquisition_receipt().policy
    activity = policy.observed_external_activity

    assert activity.official_metadata_http_request_attempt_count == 33
    assert activity.official_document_exact_bytes_retained_count == 18
    assert activity.official_document_failed_http_attempt_count == 15
    assert activity.vendor_inquiry_packet_prepared_count == 2
    assert activity.vendor_form_submission_attempt_count == 0
    assert activity.vendor_contact_count == 0
    assert activity.provider_api_query_attempt_count == 0
    assert activity.real_market_payload_download_count == 0
    assert activity.real_dq_run_count == 0
    assert activity.backtest_run_count == 0
    assert activity.orders == activity.fills == activity.positions == 0


def test_safety_surface_remains_non_executable() -> None:
    policy = receipt.load_historical_pit_source_evidence_acquisition_receipt().policy

    assert policy.safety.vendor_contact_allowed_with_exact_scope is True
    assert policy.safety.official_schedule_metadata_download_allowed is True
    assert policy.safety.provider_api_query_allowed is False
    assert policy.safety.paid_product_purchase_allowed is False
    assert policy.safety.source_inventory_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.real_dq_allowed is False
    assert policy.safety.backtest_allowed is False
    assert policy.safety.production_effect == "none"
    assert policy.safety.broker_action == "none"
    assert policy.temporary_workspace.cleanup_completed is True
    assert policy.temporary_workspace.removed_file_count == 43
    assert policy.temporary_workspace.released_byte_count == 36968
    assert policy.temporary_workspace.removed_staging_recoverable is False


def test_policy_rejects_missing_official_document_receipt() -> None:
    payload = _payload()
    rows = payload["official_document_receipts"]
    assert isinstance(rows, list)
    rows.pop()

    with pytest.raises(ValidationError, match="official document receipt inventory drifted"):
        receipt.HistoricalPITSourceEvidenceAcquisitionReceipt.model_validate(payload)


def test_policy_rejects_fabricated_vendor_send() -> None:
    payload = _payload()
    packet = payload["vendor_inquiry_packet"]
    assert isinstance(packet, dict)
    rows = packet["vendor_rows"]
    assert isinstance(rows, list)
    row = rows[0]
    assert isinstance(row, dict)
    row["message_sent"] = True

    with pytest.raises(ValidationError):
        receipt.HistoricalPITSourceEvidenceAcquisitionReceipt.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("source_state", "exact_authority_identity_frozen_count", 1),
        ("source_state", "historical_coverage_proven_count", 1),
        ("source_state", "source_contract_admitted_count", 1),
        ("source_state", "runtime_authorized_count", 1),
        ("observed_external_activity", "provider_api_query_attempt_count", 1),
        ("observed_external_activity", "real_market_payload_download_count", 1),
        ("observed_external_activity", "real_dq_run_count", 1),
        ("observed_external_activity", "backtest_run_count", 1),
        ("observed_external_activity", "orders", 1),
    ),
)
def test_policy_rejects_premature_admission_or_execution(
    section: str, field: str, value: object
) -> None:
    payload = _payload()
    section_payload = payload[section]
    assert isinstance(section_payload, dict)
    section_payload[field] = value

    with pytest.raises(ValidationError):
        receipt.HistoricalPITSourceEvidenceAcquisitionReceipt.model_validate(payload)


def test_policy_rejects_failed_attempt_claimed_as_authority() -> None:
    payload = _payload()
    attempts = payload["failed_official_document_attempts"]
    assert isinstance(attempts, list)
    row = attempts[0]
    assert isinstance(row, dict)
    row["retained_as_authority"] = True

    with pytest.raises(ValidationError):
        receipt.HistoricalPITSourceEvidenceAcquisitionReceipt.model_validate(payload)
