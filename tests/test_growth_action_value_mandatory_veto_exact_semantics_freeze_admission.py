from __future__ import annotations

import copy
import hashlib
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_exact_semantics_freeze_admission as admission,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

MandatoryVetoExactSemanticsFreezeAdmission = (
    admission.MandatoryVetoExactSemanticsFreezeAdmission
)
load_mandatory_veto_exact_semantics_freeze_admission = (
    admission.load_mandatory_veto_exact_semantics_freeze_admission
)

_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "exact_semantics_freeze_admission_v1.yaml"
)
_SEMANTICS_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_calculation_semantics_v1.yaml"
)
_V2_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "owner_freeze_decision_pack_draft_v2.yaml"
)
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def test_freeze_admission_replays_both_exact_s4a_authorities() -> None:
    loaded = load_mandatory_veto_exact_semantics_freeze_admission()

    assert loaded.terminal == "OWNER_EXACT_FROZEN_4_OF_4_PRODUCER_UNADMITTED_0_OF_4"
    assert loaded.v2_draft.file_sha256 == hashlib.sha256(_V2_PATH.read_bytes()).hexdigest()
    assert loaded.v2_draft.calculation_semantics.file_sha256 == hashlib.sha256(
        _SEMANTICS_PATH.read_bytes()
    ).hexdigest()
    assert loaded.v2_draft.file_sha256 == (
        "d08480c07047e636f8b4a8208ec60406acd5debdc60f30541411310e401b789f"
    )
    assert loaded.v2_draft.calculation_semantics.file_sha256 == (
        "813c2eb2bb0d4b4f7673048889b66fa843b739a48405cc2e87272d925dd7b0d0"
    )


def test_owner_freeze_and_producer_admission_are_mechanically_separate() -> None:
    policy = load_mandatory_veto_exact_semantics_freeze_admission().policy

    assert tuple(row.veto_id for row in policy.freeze_rows) == _VETO_IDS
    assert all(row.owner_exact_freeze_granted for row in policy.freeze_rows)
    assert not any(row.producer_contract_admitted for row in policy.freeze_rows)
    assert not any(row.producer_callable_conformance_admitted for row in policy.freeze_rows)
    assert not any(row.exact_1202_session_inventory_admitted for row in policy.freeze_rows)
    assert all(row.observed_inventory_lf_sha256 is None for row in policy.freeze_rows)
    assert not any(row.series_generation_allowed for row in policy.freeze_rows)
    assert policy.aggregate_state.admitted_producer_contracts == ()
    assert policy.aggregate_state.admitted_exact_1202_session_inventories == ()


def test_freeze_admission_preserves_non_executable_boundary() -> None:
    safety = load_mandatory_veto_exact_semantics_freeze_admission().policy.safety

    assert safety.non_executable_data_research_only
    assert safety.owner_exact_freeze_admission_only
    assert safety.synthetic_producer_followup_requires_new_exact_base
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"
    closed_flags = (
        safety.producer_implementation_allowed_in_this_wave,
        safety.source_contract_admission_allowed,
        safety.exact_inventory_admission_allowed,
        safety.veto_series_generation_allowed,
        safety.r1_manifest_generation_allowed,
        safety.cache_read_authorized,
        safety.provider_query_authorized,
        safety.real_dq_authorized,
        safety.backtest_authorized,
        safety.parameter_or_threshold_search_allowed,
        safety.constant_false_fill_allowed,
        safety.retained_series_truncation_allowed,
        safety.cross_date_fallback_allowed,
        safety.orders_allowed,
        safety.fills_allowed,
        safety.positions_allowed,
        safety.paper_allowed,
        safety.live_allowed,
    )
    assert not any(closed_flags)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("owner_exact_freeze_granted", False),
        ("producer_contract_admitted", True),
        ("producer_callable_conformance_admitted", True),
        ("exact_1202_session_inventory_admitted", True),
        ("series_generation_allowed", True),
    ),
)
def test_row_state_drift_fails_closed(field: str, value: bool) -> None:
    payload = _payload()
    mutated = copy.deepcopy(payload)
    mutated["freeze_rows"][0][field] = value

    with pytest.raises(ValidationError):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(mutated)


def test_partial_or_reordered_freeze_fails_closed() -> None:
    payload = _payload()
    partial = copy.deepcopy(payload)
    partial["freeze_rows"] = partial["freeze_rows"][:3]
    with pytest.raises(ValidationError):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(partial)

    reordered = copy.deepcopy(payload)
    rows = reordered["freeze_rows"]
    rows[0], rows[1] = rows[1], rows[0]
    with pytest.raises(ValidationError, match="freeze row surface drifted"):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(reordered)


def test_authority_or_recommendation_identity_drift_fails_closed() -> None:
    payload = _payload()
    binding_drift = copy.deepcopy(payload)
    binding_drift["authority_bindings"][1]["file_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="freeze authority binding surface drifted"):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(binding_drift)

    recommendation_drift = copy.deepcopy(payload)
    recommendation_drift["freeze_rows"][2]["recommendation_id"] = "unreviewed"
    with pytest.raises(ValidationError, match="freeze row surface drifted"):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(recommendation_drift)


@pytest.mark.parametrize(
    "field",
    (
        "provider_query_authorized",
        "real_dq_authorized",
        "backtest_authorized",
        "orders_allowed",
        "fills_allowed",
        "positions_allowed",
        "paper_allowed",
        "live_allowed",
    ),
)
def test_external_or_execution_flag_cannot_be_enabled(field: str) -> None:
    payload = _payload()
    mutated = copy.deepcopy(payload)
    mutated["safety"][field] = True

    with pytest.raises(ValidationError):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(mutated)


def test_observed_inventory_cannot_be_fabricated() -> None:
    payload = _payload()
    mutated = copy.deepcopy(payload)
    mutated["freeze_rows"][3]["observed_inventory_lf_sha256"] = "f" * 64

    with pytest.raises(ValidationError):
        MandatoryVetoExactSemanticsFreezeAdmission.model_validate(mutated)
