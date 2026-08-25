from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_producer_contract_draft as producer_draft,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH = (
    producer_draft.DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH
)
MandatoryVetoProducerContractDraft = producer_draft.MandatoryVetoProducerContractDraft
MandatoryVetoProducerContractDraftError = (
    producer_draft.MandatoryVetoProducerContractDraftError
)
load_mandatory_veto_producer_contract_draft = (
    producer_draft.load_mandatory_veto_producer_contract_draft
)

EXPECTED_FILE_SHA256 = "8bd9799b42a0d2f547afbb5bb8708775bef0de35d504197b117ed210e49a6baa"
EXPECTED_CANONICAL_SHA256 = (
    "a6e3ff096d5c5c6df6ec76756581bf0262be4988b696cb2cfb6457dd1b07f063"
)
EXPECTED_SOURCE_WAVE_FILE_SHA256 = (
    "76e38c969ee0849c77ac4012b72d0e65115f0a3448ecb276c9ca8cfef5faf8b5"
)
EXPECTED_SOURCE_WAVE_CANONICAL_SHA256 = (
    "0f8204170b4c8810cf2685e63dd5035801cef79788932b63cdf5691c1ba28e26"
)
EXPECTED_SESSION_INVENTORY_SHA256 = (
    "d43f2c34d7fc00d1f45b726b18cd21d21faa26fd56e1226bb1845b3bbc7d12c0"
)
EXPECTED_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)


def _payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_PRODUCER_CONTRACT_DRAFT_PATH
    loaded = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    assert isinstance(loaded, dict)
    return loaded


def test_loader_binds_source_wave_inventory_and_exact_draft_identity() -> None:
    result = load_mandatory_veto_producer_contract_draft()
    policy = result.policy

    assert result.terminal == "OWNER_EXACT_FREEZE_REQUIRED_0_OF_4_ADMITTED"
    assert result.file_sha256 == EXPECTED_FILE_SHA256
    assert result.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.source_wave.file_sha256 == EXPECTED_SOURCE_WAVE_FILE_SHA256
    assert result.source_wave.canonical_sha256 == EXPECTED_SOURCE_WAVE_CANONICAL_SHA256
    assert policy.source_wave_binding.file_sha256 == EXPECTED_SOURCE_WAVE_FILE_SHA256
    assert policy.source_wave_binding.canonical_sha256 == (
        EXPECTED_SOURCE_WAVE_CANONICAL_SHA256
    )
    assert policy.target_inventory.expected_session_count == 1202
    assert (
        policy.target_inventory.target_session_inventory_lf_sha256
        == EXPECTED_SESSION_INVENTORY_SHA256
    )


def test_four_producer_contracts_remain_unadmitted_and_non_generative() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy

    assert tuple(row.veto_id for row in policy.producer_contracts) == EXPECTED_VETO_IDS
    assert policy.aggregate_state.admitted_producer_contracts == ()
    assert policy.aggregate_state.unresolved_producer_contracts == EXPECTED_VETO_IDS
    assert policy.aggregate_state.source_wave_terminal_preserved == (
        "BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS"
    )
    for row in policy.producer_contracts:
        assert row.draft_status == "OWNER_EXACT_FREEZE_REQUIRED"
        assert row.producer_contract_admitted is False
        assert row.exact_1202_session_inventory_admitted is False
        assert row.series_generation_allowed is False
        assert row.structural_formula.exact_formula_frozen is False
        assert row.structural_formula.combination_rule == "OWNER_DECISION_REQUIRED"
        assert all(item.exact_value is None for item in row.threshold_decisions)
        assert all(
            item.owner_freeze_state == "PENDING_OWNER_EXACT_FREEZE"
            for item in row.threshold_decisions
        )


def test_candidate_producers_are_classified_without_false_admission() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy
    by_id = {row.veto_id: row for row in policy.producer_contracts}

    broad = by_id["broad_market_risk_off_veto"]
    assert broad.producer_state == "PLANNED_INDEPENDENT_PRODUCER_NOT_CALLABLE"
    assert broad.input_contract.allowed_input_fields == (
        "SPY.exchange_session",
        "SPY.adjusted_close",
        "SPY.available_at",
    )
    assert all(not field.startswith("QQQ.") for field in broad.input_contract.allowed_input_fields)

    volatility = by_id["realized_volatility_veto"]
    assert volatility.producer_state == "CALLABLE_CANDIDATE_NOT_SUCCESSOR_ADMITTED"
    assert any(
        item.candidate_provenance and "PILOT_ONLY" in item.candidate_provenance
        for item in volatility.threshold_decisions
    )
    assert all(item.exact_value is None for item in volatility.threshold_decisions)

    trend = by_id["underlying_trend_break_veto"]
    assert trend.producer_state == "PLANNED_DEDICATED_PRODUCER_NOT_CALLABLE"
    assert trend.input_contract.allowed_input_fields == (
        "QQQ.exchange_session",
        "QQQ.adjusted_close",
        "QQQ.available_at",
    )


def test_event_candidate_is_explicitly_pit_incomplete() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy
    event = next(
        row
        for row in policy.producer_contracts
        if row.veto_id == "scheduled_event_risk_veto"
    )

    assert event.producer_state == "CALLABLE_CANDIDATE_PIT_INCOMPLETE"
    assert event.pit_contract.published_at_required is True
    assert event.pit_contract.revision_identity_required is True
    assert event.pit_contract.event_date_can_substitute_for_published_at is False
    assert set(event.pit_contract.required_source_fields) >= {
        "event_authority",
        "event_type",
        "scheduled_for",
        "published_at",
        "revision_id",
        "source_identity",
    }
    assert "PUBLISHED_AT_AND_REVISION_SCHEMA_NOT_IMPLEMENTED" in event.blocker_codes


def test_timing_missing_and_malformed_semantics_fail_closed() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy

    for row in policy.producer_contracts:
        assert row.timing_contract.effective_session == (
            "NEXT_VALID_QQQ_EXCHANGE_SESSION"
        )
        assert row.timing_contract.same_session_action_allowed is False
        assert row.timing_contract.cross_date_fallback_allowed is False
        assert row.pit_contract.missing_terminal == "INSUFFICIENT"
        assert row.pit_contract.malformed_authority_terminal == "INVALID"


def test_option_result_and_runtime_boundaries_remain_closed() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy
    forbidden = set(policy.dependency_policy.forbidden_producer_inputs)

    assert "selected_call_contract_identity" in forbidden
    assert "selected_put_activity" in forbidden
    assert "growth_allowed" in forbidden
    assert "candidate_return" in forbidden
    assert all(
        forbidden.isdisjoint(row.input_contract.allowed_input_fields)
        for row in policy.producer_contracts
    )
    assert policy.dependency_policy.option_or_result_dependency_allowed is False
    assert policy.dependency_policy.event_date_may_substitute_for_published_at is False
    assert policy.dependency_policy.pilot_threshold_may_be_labeled_owner_frozen is False
    assert policy.safety.non_executable_data_research_only is True
    assert policy.safety.exact_formula_or_threshold_frozen is False
    assert policy.safety.source_contract_admission_allowed is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.r1_manifest_generation_allowed is False
    assert policy.safety.cache_read_authorized is False
    assert policy.safety.provider_query_authorized is False
    assert policy.safety.real_dq_authorized is False
    assert policy.safety.backtest_authorized is False
    assert policy.safety.orders_allowed is policy.safety.fills_allowed is False
    assert policy.safety.positions_allowed is False
    assert policy.safety.production_effect == policy.safety.broker_action == "none"


def test_candidate_evidence_hashes_are_exact_and_never_authority() -> None:
    policy = load_mandatory_veto_producer_contract_draft().policy

    for row in policy.producer_contracts:
        for evidence in row.candidate_evidence:
            path = PROJECT_ROOT / evidence.path
            assert hashlib.sha256(path.read_bytes()).hexdigest() == evidence.file_sha256
            assert evidence.admitted_as_producer_authority is False


def test_canonical_round_trip_contains_no_exact_threshold_or_series() -> None:
    result = load_mandatory_veto_producer_contract_draft()
    payload = json.loads(result.policy.canonical_bytes)
    replay = MandatoryVetoProducerContractDraft.model_validate(payload)

    assert replay.canonical_bytes == result.policy.canonical_bytes
    assert replay.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    text = result.policy.canonical_bytes.decode("utf-8")
    assert '"exact_value": null' in text
    assert '"veto_series_sha256"' not in text
    assert '"backtest_id"' not in text
    assert '"strategy_return"' not in text
    assert '"orders_allowed": true' not in text


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["producer_contracts"][0].__setitem__(
                "producer_contract_admitted", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["producer_contracts"][1]["structural_formula"].__setitem__(
                "exact_formula_frozen", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["producer_contracts"][1]["threshold_decisions"][0].__setitem__(
                "exact_value", 20
            ),
            "Input should be None",
        ),
        (
            lambda payload: payload["producer_contracts"][2]["pit_contract"].__setitem__(
                "event_date_can_substitute_for_published_at", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["producer_contracts"][0]["input_contract"][
                "allowed_input_fields"
            ].append("QQQ.adjusted_close"),
            "broad-market producer cannot read QQQ",
        ),
        (
            lambda payload: payload["producer_contracts"][3]["input_contract"][
                "allowed_input_fields"
            ].append("selected_call_activity"),
            "allowed inputs include a forbidden dependency",
        ),
        (
            lambda payload: payload["aggregate_state"][
                "admitted_producer_contracts"
            ].append("realized_volatility_veto"),
            "Tuple should have at most 0 items",
        ),
        (
            lambda payload: payload["source_wave_binding"].__setitem__(
                "canonical_sha256", "0" * 64
            ),
            "source-wave exact identity drifted",
        ),
    ],
)
def test_schema_rejects_false_freeze_admission_or_dependency_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_payload())
    mutate(payload)

    with pytest.raises(ValidationError, match=match):
        MandatoryVetoProducerContractDraft.model_validate(payload)


def test_loader_rejects_parent_traversal() -> None:
    path = Path(
        "../AITradingSystem/config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_producer_contract_draft_v1.yaml"
    )

    with pytest.raises(
        MandatoryVetoProducerContractDraftError, match="escapes project root"
    ):
        load_mandatory_veto_producer_contract_draft(path=path, project_root=PROJECT_ROOT)
