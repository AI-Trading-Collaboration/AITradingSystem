from __future__ import annotations

import copy
import hashlib

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack,
)

decision_pack = (
    growth_action_value_mandatory_veto_historical_pit_receipt_authority_decision_pack
)


def _payload() -> dict[str, object]:
    loaded = decision_pack.load_historical_pit_receipt_authority_decision_pack()
    return copy.deepcopy(loaded.policy.model_dump(mode="json"))


def test_decision_pack_replays_exact_s9_blockers_and_primary_window() -> None:
    loaded = decision_pack.load_historical_pit_receipt_authority_decision_pack()

    assert loaded.file_sha256 == hashlib.sha256(loaded.path.read_bytes()).hexdigest()
    assert loaded.canonical_sha256 == loaded.policy.canonical_sha256
    assert loaded.policy.predecessor_binding.policy_id.endswith("manifest_replay_gate_v1")
    assert loaded.s9_gate.policy.policy_id.endswith("manifest_replay_gate_v1")
    assert loaded.policy.primary_research_window.requested_start.isoformat() == "2021-02-22"
    assert loaded.policy.primary_research_window.requested_end.isoformat() == "2025-12-02"
    assert loaded.policy.primary_research_window.exact_session_count == 1202
    assert loaded.terminal == (
        "OWNER_EXACT_HISTORICAL_PIT_AUTHORITY_REQUIRED_0_OF_5_REMEDIATED"
    )


def test_decision_pack_preserves_five_ordered_typed_blockers() -> None:
    loaded = decision_pack.load_historical_pit_receipt_authority_decision_pack()
    rows = loaded.policy.decision_rows
    s9_rows = loaded.s9_gate.policy.source_capability_gate.rows

    assert tuple(row.ordinal for row in rows) == (1, 2, 3, 4, 5)
    assert tuple(row.candidate_id for row in rows) == tuple(
        row.candidate_id for row in s9_rows
    )
    assert tuple(row.blocker_reason_code for row in rows) == tuple(
        row.reason_code for row in s9_rows
    )
    assert tuple(row.current_endpoint for row in rows) == tuple(
        row.endpoint for row in s9_rows
    )


def test_forward_capture_is_not_historical_coverage_or_remediation() -> None:
    policy = decision_pack.load_historical_pit_receipt_authority_decision_pack().policy

    assert policy.authority_class_policy.forward_only_class == "FORWARD_ONLY_CAPTURE_LEDGER"
    assert policy.authority_class_policy.forward_only_counts_as_historical_coverage is False
    assert all(
        "DOES_NOT_UNBLOCK_PRIMARY_2021_02_22_WINDOW" in row.forward_only_impact
        for row in policy.decision_rows
    )
    assert all(row.exact_authority_identity is None for row in policy.decision_rows)
    assert all(row.historical_coverage_proven is False for row in policy.decision_rows)
    assert all(row.source_contract_admitted is False for row in policy.decision_rows)
    assert all(row.blocker_remediated is False for row in policy.decision_rows)


def test_decision_pack_requires_bea_schedule_authority_correction() -> None:
    policy = decision_pack.load_historical_pit_receipt_authority_decision_pack().policy
    bea = policy.decision_rows[-1]

    assert bea.current_endpoint == "https://apps.bea.gov/api/data"
    assert bea.endpoint_contract_correction_required is True
    assert "BEA_DATA_API_AS_RELEASE_SCHEDULE_AUTHORITY" in bea.rejected_substitutes
    assert bea.recommended_owner_action == (
        "APPROVE_CORRECTED_OFFICIAL_BEA_SCHEDULE_AUTHORITY_AND_VERSIONED_LEDGER"
    )


def test_decision_pack_preserves_zero_effect_boundary() -> None:
    policy = decision_pack.load_historical_pit_receipt_authority_decision_pack().policy

    assert set(policy.actual_counters.model_dump().values()) == {0}
    assert policy.safety.provider_query_allowed is False
    assert policy.safety.network_io_allowed is False
    assert policy.safety.source_inventory_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.real_dq_allowed is False
    assert policy.safety.backtest_allowed is False
    assert policy.safety.production_effect == "none"
    assert policy.safety.broker_action == "none"


def test_policy_rejects_missing_s9_blocker_row() -> None:
    payload = _payload()
    decision_rows = payload["decision_rows"]
    assert isinstance(decision_rows, list)
    decision_rows.pop()

    with pytest.raises(ValidationError, match="decision surface drifted"):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


def test_policy_rejects_forward_capture_as_historical_coverage() -> None:
    payload = _payload()
    authority_policy = payload["authority_class_policy"]
    assert isinstance(authority_policy, dict)
    authority_policy["forward_only_counts_as_historical_coverage"] = True

    with pytest.raises(ValidationError):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("exact_authority_identity", "fabricated-archive-v1"),
        ("historical_coverage_proven", True),
        ("source_contract_admitted", True),
        ("blocker_remediated", True),
    ),
)
def test_policy_rejects_premature_authority_or_admission(
    field: str, value: object
) -> None:
    payload = _payload()
    decision_rows = payload["decision_rows"]
    assert isinstance(decision_rows, list)
    first = decision_rows[0]
    assert isinstance(first, dict)
    first[field] = value

    with pytest.raises(ValidationError):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


def test_policy_rejects_minimum_receipt_contract_drift() -> None:
    payload = _payload()
    decision_rows = payload["decision_rows"]
    assert isinstance(decision_rows, list)
    first = decision_rows[0]
    assert isinstance(first, dict)
    fields = first["minimum_receipt_fields"]
    assert isinstance(fields, list)
    fields.remove("corporate_action_vintage")

    with pytest.raises(ValidationError, match="minimum receipt contract drifted"):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


def test_policy_rejects_bea_endpoint_correction_bypass() -> None:
    payload = _payload()
    decision_rows = payload["decision_rows"]
    assert isinstance(decision_rows, list)
    bea = decision_rows[-1]
    assert isinstance(bea, dict)
    bea["endpoint_contract_correction_required"] = False

    with pytest.raises(ValidationError, match="decision surface drifted"):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


def test_policy_rejects_primary_window_drift() -> None:
    payload = _payload()
    window = payload["primary_research_window"]
    assert isinstance(window, dict)
    window["requested_start"] = "2022-12-01"

    with pytest.raises(ValidationError, match="primary research window policy drifted"):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    (
        ("actual_counters", "provider_query_attempt_count", 1),
        ("actual_counters", "real_dq_run_count", 1),
        ("actual_counters", "backtest_run_count", 1),
        ("safety", "provider_query_allowed", True),
        ("safety", "veto_series_generation_allowed", True),
        ("safety", "real_dq_allowed", True),
        ("safety", "backtest_allowed", True),
    ),
)
def test_policy_rejects_external_or_execution_boundary_drift(
    section: str, field: str, value: object
) -> None:
    payload = _payload()
    target = payload[section]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(ValidationError):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)


def test_policy_rejects_unknown_fields() -> None:
    payload = _payload()
    payload["unexpected"] = "drift"

    with pytest.raises(ValidationError):
        decision_pack.HistoricalPITReceiptAuthorityDecisionPack.model_validate(payload)
