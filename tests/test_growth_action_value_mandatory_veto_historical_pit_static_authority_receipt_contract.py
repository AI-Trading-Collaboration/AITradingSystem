from __future__ import annotations

import copy
import hashlib

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract,
)

static_contract = (
    growth_action_value_mandatory_veto_historical_pit_static_authority_receipt_contract
)


def _payload() -> dict[str, object]:
    loaded = static_contract.load_historical_pit_static_authority_receipt_contract()
    return copy.deepcopy(loaded.policy.model_dump(mode="json"))


def test_static_contract_replays_s10_and_exact_primary_window() -> None:
    loaded = static_contract.load_historical_pit_static_authority_receipt_contract()

    assert loaded.file_sha256 == hashlib.sha256(loaded.path.read_bytes()).hexdigest()
    assert loaded.canonical_sha256 == loaded.policy.canonical_sha256
    assert loaded.s10_decision_pack.policy.policy_id.endswith(
        "historical_pit_receipt_authority_decision_pack_v1"
    )
    assert loaded.policy.primary_research_window.requested_start.isoformat() == "2021-02-22"
    assert loaded.policy.primary_research_window.requested_end.isoformat() == "2025-12-02"
    assert loaded.policy.primary_research_window.exact_session_count == 1202
    assert loaded.terminal == (
        "S11_STATIC_AUTHORITY_RECEIPT_CONTRACT_FROZEN_HISTORICAL_COVERAGE_UNPROVEN"
    )


def test_compatibility_closeout_is_parent_bound_and_does_not_change_blockers() -> None:
    closeout = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.compatibility_closeout
    )

    assert closeout.candidate_sha == "5635010dc2bf8e2fa2f68fc78723b5aff380c85d"
    assert closeout.passing_full_result == "9787_PASSED_3_SKIPPED"
    assert closeout.changes_source_blockers is False


def test_five_source_dispositions_are_frozen_without_exact_authority() -> None:
    policy = static_contract.load_historical_pit_static_authority_receipt_contract().policy
    rows = policy.source_rows

    assert tuple(row.ordinal for row in rows) == (1, 2, 3, 4, 5)
    assert tuple(row.candidate_disposition for row in rows) == (
        "VENDOR_EVIDENCE_REQUIRED",
        "VENDOR_EVIDENCE_REQUIRED",
        "FREEZE_CANDIDATE",
        "INVENTORY_ONLY",
        "INVENTORY_ONLY",
    )
    assert all(row.disposition_frozen for row in rows)
    assert [row.candidate_id for row in rows if row.candidate_source_approved] == [
        "federal_reserve_fomc_schedule_capture_v1"
    ]
    assert all(row.exact_authority_identity is None for row in rows)
    assert all(row.exact_authority_identity_frozen is False for row in rows)
    assert all(row.historical_coverage_proven is False for row in rows)
    assert all(row.source_contract_admitted is False for row in rows)
    assert all(row.runtime_authorized is False for row in rows)
    assert all(row.blocker_remediated is False for row in rows)


def test_fed_freezes_only_candidate_family_role_and_precedence() -> None:
    fed = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.source_rows[2]
    )

    assert fed.candidate_authority_family_frozen is True
    assert fed.candidate_source_approved is True
    assert fed.authority_roles == (
        "INITIAL_SCHEDULE_AUTHORITY",
        "TERMINAL_RECONCILIATION_ONLY",
    )
    assert fed.selected_authority_class is None
    assert fed.evidence_packet_state == "EXACT_URL_DIGEST_REVISION_INVENTORY_NOT_GENERATED"


def test_vendor_packets_are_prepare_only_and_not_sent() -> None:
    loaded = static_contract.load_historical_pit_static_authority_receipt_contract()
    rows = loaded.policy.source_rows

    assert tuple(row.evidence_packet_state for row in rows[:2]) == (
        "PREPARE_NOT_SENT",
        "PREPARE_NOT_SENT",
    )
    assert all(row.candidate_source_approved is False for row in rows[:2])
    assert "INTERNAL_RESEARCH_LICENSE_RIGHTS" in rows[0].required_next_evidence
    assert "CORRECTION_AND_REISSUE_HANDLING" in rows[1].required_next_evidence


def test_receipt_contract_rejects_inferred_available_at() -> None:
    contract = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.receipt_identity_contract
    )

    assert contract.downloaded_at_role == "AUDIT_ONLY_NOT_PIT"
    assert contract.date_only_intraday_cutoff_policy == (
        "INSUFFICIENT_UNLESS_OWNER_FROZEN_CONSERVATIVE_MAPPING"
    )
    assert "SCHEDULED_FOR_AS_AVAILABLE_AT" in contract.prohibited_available_at_inference
    assert "SESSION_PLUS_ONE_AS_SOURCE_PROVEN_AVAILABLE_AT" in (
        contract.prohibited_available_at_inference
    )


def test_schedule_ledger_is_append_only_and_final_calendar_is_reconciliation_only() -> None:
    ledger = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.schedule_revision_ledger_contract
    )

    assert ledger.append_only is True
    assert ledger.superseded_revisions_retained is True
    assert ledger.move_then_restore_records_two_revisions is True
    assert ledger.current_or_final_calendar_role == "TERMINAL_RECONCILIATION_ONLY"
    assert ledger.final_calendar_can_supply_historical_available_at is False


def test_event_taxonomy_does_not_expand_frozen_veto_semantics() -> None:
    taxonomy = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.event_taxonomy_contract
    )

    assert taxonomy.frozen_veto_event_type_ids == (
        "FEDERAL_RESERVE:FOMC_RATE_DECISION",
        "BLS:CPI",
        "BLS:NONFARM_PAYROLLS",
        "BEA:PCE_PRICE_INDEX",
        "BEA:GDP_ADVANCE_ESTIMATE",
    )
    assert taxonomy.special_or_emergency_default == "INVENTORY_ONLY_NOT_VETO_ELIGIBLE"
    assert taxonomy.notation_vote_default == "INVENTORY_ONLY_NOT_VETO_ELIGIBLE"
    assert taxonomy.unmapped_event_terminal == (
        "INSUFFICIENT_OWNER_EVENT_TAXONOMY_REQUIRED"
    )
    assert taxonomy.architecture_expansion_allowed is False


def test_cutoff_coverage_and_state_separation_remain_fail_closed() -> None:
    policy = static_contract.load_historical_pit_static_authority_receipt_contract().policy
    coverage = policy.cutoff_coverage_contract
    states = policy.state_separation_contract

    assert coverage.selected_revision_rule == (
        "LATEST_ADMITTED_REVISION_WITH_AVAILABLE_AT_LTE_CUTOFF"
    )
    assert coverage.required_coverage_through == "NEXT_ACTION_SESSION_CLOSE"
    assert coverage.later_revision_leakage_allowed is False
    assert coverage.target_row_count_alone_proves_coverage is False
    assert states.automatic_state_promotion_allowed is False
    assert states.partial_source_freeze_can_generate_series is False
    assert states.source_capability_implies_investment_value is False


def test_falsification_matrix_has_all_four_typed_outcomes() -> None:
    axes = (
        static_contract.load_historical_pit_static_authority_receipt_contract()
        .policy.falsification_axes
    )

    assert len(axes) == 10
    assert {axis.axis_id for axis in axes} >= {
        "HISTORICAL_AVAILABLE_AT",
        "EXACT_WINDOW_COVERAGE",
        "STATE_SEPARATION",
        "SAFETY_COUNTERS",
    }
    assert all(axis.pass_action for axis in axes)
    assert all(axis.fail_action for axis in axes)
    assert all(axis.insufficient_action for axis in axes)
    assert all(axis.invalid_action for axis in axes)


def test_static_contract_preserves_zero_effect_boundary() -> None:
    policy = static_contract.load_historical_pit_static_authority_receipt_contract().policy

    assert set(policy.actual_counters.model_dump().values()) == {0}
    assert policy.safety.vendor_contact_allowed is False
    assert policy.safety.paid_product_purchase_allowed is False
    assert policy.safety.provider_query_allowed is False
    assert policy.safety.source_inventory_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.real_dq_allowed is False
    assert policy.safety.backtest_allowed is False
    assert policy.safety.production_effect == "none"
    assert policy.safety.broker_action == "none"


def test_policy_rejects_missing_s10_source_row() -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    rows.pop()

    with pytest.raises(ValidationError, match="five-source candidate disposition surface drifted"):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


def test_policy_rejects_fmp_candidate_approval() -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    fmp = rows[0]
    assert isinstance(fmp, dict)
    fmp["candidate_source_approved"] = True

    with pytest.raises(ValidationError, match="five-source candidate disposition surface drifted"):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("selected_authority_class", "PROVIDER_NATIVE_VERSIONED_AS_OF_ARCHIVE"),
        ("exact_authority_identity", "fabricated-authority-v1"),
        ("exact_authority_identity_frozen", True),
        ("historical_coverage_proven", True),
        ("source_contract_admitted", True),
        ("runtime_authorized", True),
        ("blocker_remediated", True),
    ),
)
def test_policy_rejects_premature_authority_coverage_or_runtime_state(
    field: str, value: object
) -> None:
    payload = _payload()
    rows = payload["source_rows"]
    assert isinstance(rows, list)
    first = rows[0]
    assert isinstance(first, dict)
    first[field] = value

    with pytest.raises(ValidationError):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "field", "value", "match"),
    (
        (
            "receipt_identity_contract",
            "date_only_intraday_cutoff_policy",
            "DATE_ONLY_ASSUMED_PREMARKET",
            None,
        ),
        (
            "schedule_revision_ledger_contract",
            "append_only",
            False,
            None,
        ),
        (
            "schedule_revision_ledger_contract",
            "superseded_revisions_retained",
            False,
            None,
        ),
        (
            "schedule_revision_ledger_contract",
            "final_calendar_can_supply_historical_available_at",
            True,
            None,
        ),
        (
            "event_taxonomy_contract",
            "architecture_expansion_allowed",
            True,
            None,
        ),
        (
            "cutoff_coverage_contract",
            "later_revision_leakage_allowed",
            True,
            None,
        ),
        (
            "cutoff_coverage_contract",
            "target_row_count_alone_proves_coverage",
            True,
            None,
        ),
        (
            "state_separation_contract",
            "partial_source_freeze_can_generate_series",
            True,
            None,
        ),
        (
            "state_separation_contract",
            "source_capability_implies_investment_value",
            True,
            None,
        ),
    ),
)
def test_policy_rejects_fail_closed_contract_drift(
    section: str, field: str, value: object, match: str | None
) -> None:
    payload = _payload()
    target = payload[section]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(ValidationError, match=match):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


def test_policy_rejects_available_at_inference_removal() -> None:
    payload = _payload()
    receipt = payload["receipt_identity_contract"]
    assert isinstance(receipt, dict)
    prohibited = receipt["prohibited_available_at_inference"]
    assert isinstance(prohibited, list)
    prohibited.remove("DOWNLOADED_AT_AS_AVAILABLE_AT")

    with pytest.raises(ValidationError, match="available-at inference policy drifted"):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


def test_policy_rejects_primary_window_drift() -> None:
    payload = _payload()
    window = payload["primary_research_window"]
    assert isinstance(window, dict)
    window["requested_start"] = "2022-12-01"

    with pytest.raises(ValidationError, match="primary research window policy drifted"):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("actual_counters", "vendor_contact_count", 1),
        ("actual_counters", "paid_product_purchase_count", 1),
        ("actual_counters", "real_market_payload_download_count", 1),
        ("actual_counters", "real_dq_run_count", 1),
        ("actual_counters", "backtest_run_count", 1),
        ("safety", "vendor_contact_allowed", True),
        ("safety", "paid_product_purchase_allowed", True),
        ("safety", "provider_query_allowed", True),
        ("safety", "source_inventory_admission_allowed", True),
        ("safety", "veto_series_generation_allowed", True),
        ("safety", "real_dq_allowed", True),
        ("safety", "backtest_allowed", True),
    ),
)
def test_policy_rejects_external_data_or_execution_boundary_drift(
    section: str, field: str, value: object
) -> None:
    payload = _payload()
    target = payload[section]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(ValidationError):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)


def test_policy_rejects_unknown_fields() -> None:
    payload = _payload()
    payload["unexpected"] = "drift"

    with pytest.raises(ValidationError):
        static_contract.HistoricalPITStaticAuthorityReceiptContract.model_validate(payload)
