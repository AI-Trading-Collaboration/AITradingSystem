from __future__ import annotations

import copy
import hashlib
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.research_quality import (
    frozen_signal_value_confirmation_preregistration_freeze_admission as admission,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

FrozenSignalValuePreregistrationFreezeAdmission = (
    admission.FrozenSignalValuePreregistrationFreezeAdmission
)
load_freeze_admission = (
    admission.load_frozen_signal_value_confirmation_preregistration_freeze_admission
)

_PATH = PROJECT_ROOT / (
    "config/research/frozen_signal_value_confirmation_preregistration_freeze_admission_v1.yaml"
)
_POLICY_PATH = PROJECT_ROOT / (
    "config/research/frozen_signal_value_confirmation_preregistration_v1.yaml"
)
_POLICY_FILE_SHA256 = "507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa"
_POLICY_CANONICAL_SHA256 = (
    "7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c"
)
_AUTHORITY_SET_SHA256 = (
    "45d508d563b46b0929d80687155213d265399a4f105da69f31810780a34c754f"
)


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def test_freeze_admission_replays_exact_approved_preregistration_identity() -> None:
    loaded = load_freeze_admission()

    assert loaded.terminal == (
        "OWNER_EXACT_PREREGISTRATION_FROZEN_NO_EMPIRICAL_RUN_AUTHORITY"
    )
    assert loaded.preregistration.policy_file_sha256 == hashlib.sha256(
        _POLICY_PATH.read_bytes()
    ).hexdigest()
    assert loaded.preregistration.policy_file_sha256 == _POLICY_FILE_SHA256
    assert loaded.preregistration.policy_canonical_sha256 == _POLICY_CANONICAL_SHA256
    assert loaded.preregistration.authority_set_sha256 == _AUTHORITY_SET_SHA256


def test_approved_draft_bytes_and_review_status_remain_immutable() -> None:
    loaded = load_freeze_admission()
    policy = loaded.preregistration.policy

    assert policy.policy_status == "DRAFT_OWNER_REVIEW_REQUIRED"
    assert policy.owner_review.decision_state == "OWNER_REVIEW_REQUIRED"
    assert policy.safety.policy_or_threshold_finally_approved is False
    assert loaded.admission.frozen_surface.owner_exact_frozen_via_separate_admission is True


def test_signal_comparator_accounting_cost_threshold_and_reducer_are_frozen() -> None:
    surface = load_freeze_admission().admission.frozen_surface

    assert surface.signal_session_count == 1202
    assert surface.return_interval_count == 1201
    assert surface.candidate_implementation_id == (
        "FROZEN_SIGNAL_FULLY_FUNDED_QQQ_ZERO_RETURN_CASH"
    )
    assert surface.primary_comparator_id == "EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH"
    assert surface.initial_capital_usd == 100000
    assert surface.price_field == "ADJUSTED_CLOSE"
    assert surface.one_way_cost_bps == 5.0
    assert surface.return_threshold_strictly_greater_than == 0.0
    assert surface.drawdown_threshold_less_than_or_equal_to == 0.0
    assert surface.reducer_precedence == ("INSUFFICIENT", "REJECT", "RETAIN")


def test_empirical_result_and_every_runtime_gate_remain_closed() -> None:
    policy = load_freeze_admission().admission
    state = policy.empirical_state
    safety = policy.safety

    assert state.signal_value_verdict == "UNRESOLVED"
    assert state.empirical_confirmation_completed is False
    assert state.successor_task_implicitly_created is False
    assert state.next_legal_action == (
        "OWNER_SEPARATE_EXACT_BOUNDED_RUN_AUTHORIZATION_REQUIRED"
    )
    closed_flags = (
        safety.predecessor_policy_mutation_allowed,
        safety.outcome_access_authorized,
        safety.market_data_read_authorized,
        safety.market_data_download_authorized,
        safety.dq_authorized,
        safety.confirmation_authorized,
        safety.backtest_authorized,
        safety.quantconnect_authorized,
        safety.provider_authorized,
        safety.cache_mutation_authorized,
        safety.option_data_use_authorized,
        safety.paper_allowed,
        safety.live_allowed,
        safety.production_allowed,
        safety.broker_allowed,
        safety.investment_conclusion_generated,
    )
    assert not any(closed_flags)
    assert (safety.orders, safety.fills, safety.positions) == (0, 0, 0)
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"


@pytest.mark.parametrize(
    "field",
    (
        "exact_preregistration_freeze_granted",
        "full_listed_surface_frozen",
        "predecessor_bytes_must_remain_immutable",
    ),
)
def test_owner_freeze_cannot_be_partially_admitted(field: str) -> None:
    mutated = copy.deepcopy(_payload())
    mutated["owner_decision"][field] = False

    with pytest.raises(ValidationError):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(mutated)


def test_instruction_identity_or_frozen_surface_drift_fails_closed() -> None:
    instruction_drift = copy.deepcopy(_payload())
    instruction_drift["owner_decision"]["approved_instruction"] += "并授权回测。"
    with pytest.raises(ValidationError, match="owner approved instruction drifted"):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(instruction_drift)

    hash_drift = copy.deepcopy(_payload())
    hash_drift["authority_binding"]["canonical_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="approved preregistration identity drifted"):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(hash_drift)

    surface_drift = copy.deepcopy(_payload())
    surface_drift["frozen_surface"]["frozen_sections"].pop()
    with pytest.raises(ValidationError, match="frozen section inventory drifted"):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(surface_drift)


@pytest.mark.parametrize(
    "field",
    (
        "predecessor_policy_mutation_allowed",
        "outcome_access_authorized",
        "market_data_read_authorized",
        "market_data_download_authorized",
        "dq_authorized",
        "confirmation_authorized",
        "backtest_authorized",
        "quantconnect_authorized",
        "provider_authorized",
        "cache_mutation_authorized",
        "option_data_use_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
        "investment_conclusion_generated",
    ),
)
def test_closed_gate_cannot_be_enabled(field: str) -> None:
    mutated = copy.deepcopy(_payload())
    mutated["safety"][field] = True

    with pytest.raises(ValidationError):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(mutated)


def test_unknown_field_fails_closed() -> None:
    payload = _payload()
    payload["safety"]["backdoor"] = True

    with pytest.raises(ValidationError):
        FrozenSignalValuePreregistrationFreezeAdmission.model_validate(payload)
