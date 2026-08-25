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
    growth_action_value_mandatory_veto_source_contract as source_contract,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_ARCHITECTURE_FREEZE_PATH = source_contract.DEFAULT_ARCHITECTURE_FREEZE_PATH
DEFAULT_SOURCE_CONTRACT_WAVE_PATH = source_contract.DEFAULT_SOURCE_CONTRACT_WAVE_PATH
ArchitectureFreezeAdmission = source_contract.ArchitectureFreezeAdmission
MandatoryVetoSourceContractError = source_contract.MandatoryVetoSourceContractError
MandatoryVetoSourceContractWave = source_contract.MandatoryVetoSourceContractWave
load_architecture_freeze_admission = source_contract.load_architecture_freeze_admission
load_mandatory_veto_source_contract_wave = source_contract.load_mandatory_veto_source_contract_wave

ARCHITECTURE_PATH = Path(
    "config/research/qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1.yaml"
)
COMPATIBILITY_PATH = Path(
    "config/research/qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1.yaml"
)


def _payload(path: Path) -> dict[str, Any]:
    loaded = load_strict_yaml_text(
        (PROJECT_ROOT / path).read_text(encoding="utf-8"), label=str(path)
    )
    assert isinstance(loaded, dict)
    return loaded


def _freeze_payload() -> dict[str, Any]:
    return _payload(DEFAULT_ARCHITECTURE_FREEZE_PATH)


def _wave_payload() -> dict[str, Any]:
    return _payload(DEFAULT_SOURCE_CONTRACT_WAVE_PATH)


def test_owner_freeze_admission_binds_exact_immutable_drafts() -> None:
    result = load_architecture_freeze_admission()

    assert result.terminal == "OWNER_FROZEN_ARCHITECTURE_SOURCE_WAVE_NON_EXECUTABLE"
    assert result.admission.status == (
        "OWNER_EXACT_FROZEN_NON_EXECUTABLE_SOURCE_CONTRACT_WAVE_ONLY"
    )
    bindings = result.admission.approved_bindings
    assert bindings[0].file_sha256 == (
        "9b4856614298d64b2c8b5897980735a9e2a19c46fecb6c2362cb750ae13b136d"
    )
    assert bindings[0].canonical_sha256 == (
        "88e1283b0333bafca24779c9c527d362acef40b65d4cff1a9d081ded07ac70e4"
    )
    assert bindings[1].file_sha256 == (
        "c5867551aec4f152256219e4fb19b7c52ec5a6b7f8d8c316961d33a75749679d"
    )
    assert bindings[1].canonical_sha256 == (
        "067a6b23daa1bfff22a6d4f4fcb773346a7d866e21cf2adb759acde75d04f524"
    )
    assert hashlib.sha256((PROJECT_ROOT / ARCHITECTURE_PATH).read_bytes()).hexdigest() == (
        bindings[0].file_sha256
    )
    assert hashlib.sha256((PROJECT_ROOT / COMPATIBILITY_PATH).read_bytes()).hexdigest() == (
        bindings[1].file_sha256
    )


def test_owner_freeze_does_not_authorize_concrete_values_or_execution() -> None:
    policy = load_architecture_freeze_admission().admission

    assert policy.owner_instruction.concrete_formula_or_threshold_exact_freeze_granted is False
    assert policy.frozen_semantics.concrete_source_contract_values_selected is False
    assert policy.safety.source_contract_drafting_allowed is True
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.r1_manifest_generation_allowed is False
    assert policy.safety.cache_read_authorized is False
    assert policy.safety.provider_query_authorized is False
    assert policy.safety.real_dq_authorized is False
    assert policy.safety.backtest_authorized is False
    assert policy.safety.orders_allowed is policy.safety.fills_allowed is False
    assert policy.safety.positions_allowed is False
    assert policy.safety.production_effect == policy.safety.broker_action == "none"


def test_source_wave_exposes_four_typed_blockers_before_r1() -> None:
    result = load_mandatory_veto_source_contract_wave()
    policy = result.policy

    assert result.terminal == ("BLOCKED_PRE_R1_MANIFEST_INCOMPLETE_MANDATORY_SOURCE_CONTRACTS")
    assert tuple(row.veto_id for row in policy.source_contracts) == (
        "broad_market_risk_off_veto",
        "realized_volatility_veto",
        "scheduled_event_risk_veto",
        "underlying_trend_break_veto",
    )
    assert policy.aggregate_state.admitted_source_contracts == ()
    assert policy.aggregate_state.unresolved_source_contracts == tuple(
        row.veto_id for row in policy.source_contracts
    )
    assert policy.aggregate_state.missing_required_source_outcome == (
        "INSUFFICIENT_EVIDENCE_TO_BUILD_R1_MANIFEST"
    )
    assert policy.aggregate_state.malformed_authority_outcome == ("PRE_RUN_AUTHORITY_INVALID")


def test_legacy_volatility_readiness_is_not_successor_admission() -> None:
    policy = load_mandatory_veto_source_contract_wave().policy
    by_id = {row.veto_id: row for row in policy.source_contracts}
    volatility = by_id["realized_volatility_veto"]

    assert volatility.architecture_readiness_state == ("SOURCE_CONTRACT_READY_SERIES_NOT_GENERATED")
    assert volatility.successor_admission_state == (
        "BLOCKED_SUCCESSOR_THRESHOLD_PRODUCER_AND_TIMING_NOT_FROZEN"
    )
    assert "LEGACY_RUNTIME_THRESHOLDS_NOT_SEPARATE_OWNER_AUTHORITY" in (volatility.blocker_codes)
    assert volatility.required_identity.threshold_policy_id is None
    assert volatility.series_generation_allowed is False


def test_all_source_rows_keep_identity_missing_and_fail_closed() -> None:
    policy = load_mandatory_veto_source_contract_wave().policy

    for row in policy.source_contracts:
        identity = row.required_identity
        assert identity.source_contract_sha256 is None
        assert identity.independent_producer_identity is None
        assert identity.formula_category is None
        assert identity.threshold_policy_id is None
        assert identity.decision_as_of is None
        assert identity.available_at is None
        assert identity.missing_terminal == "INSUFFICIENT"
        assert identity.malformed_authority_terminal == "INVALID"
        assert identity.exact_1202_session_inventory.required is True
        assert identity.exact_1202_session_inventory.admitted is False
        assert identity.exact_1202_session_inventory.observed_inventory_lf_sha256 is None
        assert row.option_alpha_input_allowed is False
        assert row.result_input_allowed is False
        assert row.series_generation_allowed is False


def test_candidate_evidence_is_hash_bound_but_never_admitted() -> None:
    policy = load_mandatory_veto_source_contract_wave().policy
    by_id = {row.veto_id: row for row in policy.source_contracts}

    assert all(
        evidence.admitted_as_successor_source is False
        for row in policy.source_contracts
        for evidence in row.candidate_evidence
    )
    event_roles = {item.role for item in by_id["scheduled_event_risk_veto"].candidate_evidence}
    assert "PIT_WARNING_DIAGNOSTIC_ONLY_AUTHORITY" in event_roles
    assert "PIT_PUBLISHED_AT_CONTRACT_CANDIDATE_NOT_ADMITTED" in event_roles
    risk_roles = {item.role for item in by_id["broad_market_risk_off_veto"].candidate_evidence}
    assert "REJECTED_GROWTH_ALLOWED_ALIAS_EVIDENCE" in risk_roles
    trend_roles = {item.role for item in by_id["underlying_trend_break_veto"].candidate_evidence}
    assert trend_roles == {"NO_CALLABLE_PRODUCER_BLOCKER_AUTHORITY"}


def test_dependency_and_runtime_safety_boundaries_remain_closed() -> None:
    policy = load_mandatory_veto_source_contract_wave().policy

    assert policy.dependency_policy.growth_allowed_alias_allowed is False
    assert policy.dependency_policy.selected_pair_or_activity_allowed is False
    assert policy.dependency_policy.result_dependent_formula_or_bucket_allowed is False
    assert "selected_call_activity" in policy.dependency_policy.forbidden_source_inputs
    assert "candidate_return" in policy.dependency_policy.forbidden_source_inputs
    assert policy.safety.concrete_formula_or_threshold_frozen is False
    assert policy.safety.veto_series_generation_allowed is False
    assert policy.safety.r1_manifest_generation_allowed is False
    assert policy.safety.constant_false_fill_allowed is False
    assert policy.safety.retained_series_truncation_allowed is False
    assert policy.safety.cross_date_fallback_allowed is False
    assert policy.safety.cache_read_authorized is policy.safety.provider_query_authorized is False
    assert policy.safety.real_dq_authorized is policy.safety.backtest_authorized is False
    assert policy.safety.orders_allowed is policy.safety.fills_allowed is False
    assert policy.safety.positions_allowed is False


def test_canonical_round_trip_contains_no_result_or_series_identity() -> None:
    result = load_mandatory_veto_source_contract_wave()
    payload = json.loads(result.policy.canonical_bytes)
    replay = MandatoryVetoSourceContractWave.model_validate(payload)

    assert replay.canonical_bytes == result.policy.canonical_bytes
    assert replay.canonical_sha256 == result.policy.canonical_sha256
    text = result.policy.canonical_bytes.decode("utf-8")
    assert "backtest_id" not in text
    assert "strategy_return" not in text
    assert "veto_series_sha256" not in text
    assert '"target_weight":' not in text
    assert '"0.55"' not in text
    assert '"0.25"' not in text


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["approved_bindings"][0].__setitem__("file_sha256", "0" * 64),
            "owner exact-freeze binding surface drifted",
        ),
        (
            lambda payload: payload["frozen_semantics"]["successor_market_veto_taxonomy"].reverse(),
            "frozen successor veto taxonomy drifted",
        ),
        (
            lambda payload: payload["owner_instruction"].__setitem__(
                "concrete_formula_or_threshold_exact_freeze_granted", True
            ),
            "Input should be False",
        ),
    ],
)
def test_freeze_schema_rejects_authority_or_scope_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_freeze_payload())
    mutate(payload)

    with pytest.raises(ValidationError, match=match):
        ArchitectureFreezeAdmission.model_validate(payload)


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["source_contracts"][0].__setitem__(
                "series_generation_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["source_contracts"][1]["required_identity"].__setitem__(
                "source_contract_sha256", "0" * 64
            ),
            "Input should be None",
        ),
        (
            lambda payload: payload["source_contracts"][2]["required_identity"].__setitem__(
                "missing_terminal", "PASS"
            ),
            "Input should be 'INSUFFICIENT'",
        ),
        (
            lambda payload: payload["source_contracts"][3].__setitem__("selected_call_activity", 1),
            "Extra inputs are not permitted",
        ),
        (
            lambda payload: payload["aggregate_state"]["admitted_source_contracts"].append(
                "realized_volatility_veto"
            ),
            "Tuple should have at most 0 items",
        ),
        (
            lambda payload: payload["dependency_policy"].__setitem__(
                "growth_allowed_alias_allowed", True
            ),
            "Input should be False",
        ),
    ],
)
def test_source_wave_rejects_false_readiness_or_dependency_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_wave_payload())
    mutate(payload)

    with pytest.raises(ValidationError, match=match):
        MandatoryVetoSourceContractWave.model_validate(payload)


@pytest.mark.parametrize(
    ("loader", "path"),
    [
        (
            load_architecture_freeze_admission,
            Path(
                "../AITradingSystem/config/research/"
                "qc_qqq_options_growth_action_value_veto_option_signal_architecture_freeze_v1.yaml"
            ),
        ),
        (
            load_mandatory_veto_source_contract_wave,
            Path(
                "../AITradingSystem/config/research/"
                "qc_qqq_options_growth_action_value_mandatory_veto_source_contract_wave_v1.yaml"
            ),
        ),
    ],
)
def test_loaders_reject_parent_traversal(loader: Callable[..., object], path: Path) -> None:
    with pytest.raises(MandatoryVetoSourceContractError, match="escapes project root"):
        loader(path=path, project_root=PROJECT_ROOT)
