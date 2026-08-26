from __future__ import annotations

import copy
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_real_source_adapter_contract_freeze_admission as admission,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_contract_freeze_admission_v1.yaml"
)
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)
_CANDIDATE_IDS = (
    "fmp_spy_qqq_eod_adjusted_v1",
    "marketstack_spy_qqq_second_source_v1",
    "cboe_vix_index_daily_v1",
    "fred_vixcls_diagnostic_crosscheck_v1",
    "federal_reserve_fomc_schedule_capture_v1",
    "bls_release_schedule_capture_v1",
    "bea_release_schedule_capture_v1",
)


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def test_freeze_admission_replays_exact_s6_authority() -> None:
    loaded = admission.load_mandatory_veto_real_source_adapter_contract_freeze_admission()

    assert loaded.terminal == (
        "OWNER_ADAPTER_MANIFEST_CONTRACT_FROZEN_4_OF_4_REAL_SOURCE_UNADMITTED_0_OF_4"
    )
    assert loaded.review.file_sha256 == (
        "d0adae89a1faf7c160cf82edc9d51ede74fa2ea279fcc2526c009752a9a5b57e"
    )
    assert loaded.review.canonical_sha256 == (
        "be705f1b46431e432169b186db6d336bb68d51cf296ca08ca5d6cca465ffc6e3"
    )
    assert loaded.review.terminal == ("OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4")


def test_contract_freeze_and_real_source_admission_are_mechanically_separate() -> None:
    policy = admission.load_mandatory_veto_real_source_adapter_contract_freeze_admission().policy

    assert tuple(row.veto_id for row in policy.freeze_rows) == _VETO_IDS
    assert all(row.owner_adapter_manifest_contract_frozen for row in policy.freeze_rows)
    assert not any(row.adapter_implementation_admitted for row in policy.freeze_rows)
    assert not any(row.real_source_identity_admitted for row in policy.freeze_rows)
    assert not any(row.exact_1202_session_inventory_admitted for row in policy.freeze_rows)
    assert not any(row.manifest_replay_allowed for row in policy.freeze_rows)
    assert all(row.observed_inventory_lf_sha256 is None for row in policy.freeze_rows)
    assert all(row.observed_manifest_sha256 is None for row in policy.freeze_rows)
    assert policy.aggregate_state.admitted_adapter_implementations == ()
    assert policy.aggregate_state.admitted_real_source_identities == ()
    assert policy.aggregate_state.admitted_exact_1202_session_inventories == ()
    assert policy.aggregate_state.observed_manifest_replays == ()


def test_frozen_candidate_inventory_warmup_and_gate_surfaces_are_exact() -> None:
    policy = admission.load_mandatory_veto_real_source_adapter_contract_freeze_admission().policy
    surface = policy.frozen_contract_surface

    assert surface.source_candidate_ids == _CANDIDATE_IDS
    assert surface.review_veto_ids == _VETO_IDS
    assert surface.exact_inventory.target_start == "2021-02-22"
    assert surface.exact_inventory.target_session_count == 1202
    assert surface.exact_inventory.target_end is None
    assert tuple(
        row.minimum_pre_target_sessions for row in surface.exact_inventory.warmup_rows
    ) == (199, 19, 251, 199)
    assert tuple(
        row.continuous_state_replay_required for row in surface.exact_inventory.warmup_rows
    ) == (False, False, False, True)
    assert len(surface.manifest_replay_gates) == 14
    assert surface.manifest_replay_gates[-1] == "ZERO_EXECUTION_COUNTERS"


def test_freeze_admission_preserves_non_executable_boundary() -> None:
    safety = (
        admission.load_mandatory_veto_real_source_adapter_contract_freeze_admission().policy.safety
    )

    assert safety.non_executable_data_research_only
    assert safety.owner_contract_freeze_admission_only
    assert safety.adapter_implementation_followup_requires_new_exact_base
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"
    closed_flags = (
        safety.adapter_implementation_allowed_in_this_wave,
        safety.filesystem_market_data_read_allowed,
        safety.network_io_allowed,
        safety.provider_query_authorized,
        safety.cache_read_authorized,
        safety.real_data_read_authorized,
        safety.adapter_execution_authorized,
        safety.real_source_admission_allowed,
        safety.exact_inventory_admission_allowed,
        safety.manifest_replay_allowed,
        safety.veto_series_generation_allowed,
        safety.r1_manifest_generation_allowed,
        safety.real_dq_authorized,
        safety.backtest_authorized,
        safety.parameter_or_threshold_search_allowed,
        safety.constant_false_fill_allowed,
        safety.missing_as_clear_allowed,
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
        ("owner_adapter_manifest_contract_frozen", False),
        ("adapter_implementation_admitted", True),
        ("real_source_identity_admitted", True),
        ("exact_1202_session_inventory_admitted", True),
        ("manifest_replay_allowed", True),
    ),
)
def test_row_state_drift_fails_closed(field: str, value: bool) -> None:
    payload = _payload()
    payload["freeze_rows"][0][field] = value

    with pytest.raises(ValidationError):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)


def test_partial_or_reordered_freeze_fails_closed() -> None:
    payload = _payload()
    partial = copy.deepcopy(payload)
    partial["freeze_rows"] = partial["freeze_rows"][:3]
    with pytest.raises(ValidationError):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(partial)

    reordered = copy.deepcopy(payload)
    rows = reordered["freeze_rows"]
    rows[0], rows[1] = rows[1], rows[0]
    with pytest.raises(ValidationError, match="freeze row surface drifted"):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(reordered)


def test_review_authority_identity_drift_fails_closed() -> None:
    payload = _payload()
    payload["review_authority_binding"]["file_sha256"] = "0" * 64

    with pytest.raises(ValidationError, match="S6 review exact identity drifted"):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "field", "replacement", "expected"),
    (
        (
            "frozen_contract_surface",
            "source_candidate_ids",
            ["fmp_spy_qqq_eod_adjusted_v1"],
            "candidate inventory drifted",
        ),
        (
            "frozen_contract_surface",
            "review_veto_ids",
            list(reversed(_VETO_IDS)),
            "review veto inventory drifted",
        ),
        (
            "frozen_contract_surface",
            "manifest_replay_gates",
            ["ZERO_EXECUTION_COUNTERS"],
            "manifest replay gate surface drifted",
        ),
    ),
)
def test_candidate_veto_or_manifest_surface_drift_fails_closed(
    section: str,
    field: str,
    replacement: object,
    expected: str,
) -> None:
    payload = _payload()
    payload[section][field] = replacement

    with pytest.raises(ValidationError, match=expected):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)


def test_inventory_or_owner_decision_surface_drift_fails_closed() -> None:
    payload = _payload()
    inventory_drift = copy.deepcopy(payload)
    inventory_drift["frozen_contract_surface"]["exact_inventory"]["warmup_rows"][2][
        "minimum_pre_target_sessions"
    ] = 250
    with pytest.raises(ValidationError, match="warmup surface drifted"):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(
            inventory_drift
        )

    decision_drift = copy.deepcopy(payload)
    decision_drift["freeze_rows"][2]["predecessor_admission_decision"] = (
        "DEFER_UNTIL_EXACT_REPLAY_PASS"
    )
    with pytest.raises(ValidationError, match="freeze row surface drifted"):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(
            decision_drift
        )


@pytest.mark.parametrize(
    "field",
    (
        "provider_query_authorized",
        "cache_read_authorized",
        "real_data_read_authorized",
        "adapter_execution_authorized",
        "manifest_replay_allowed",
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
    payload["safety"][field] = True

    with pytest.raises(ValidationError):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("freeze_rows", 3, "observed_inventory_lf_sha256"), "f" * 64),
        (("freeze_rows", 1, "observed_manifest_sha256"), "e" * 64),
        (
            (
                "frozen_contract_surface",
                "exact_inventory",
                "target_session_list_lf_sha256",
            ),
            "d" * 64,
        ),
    ),
)
def test_observed_evidence_cannot_be_fabricated(path: tuple[str | int, ...], value: str) -> None:
    payload = _payload()
    target: Any = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)


def test_unknown_fields_fail_closed() -> None:
    payload = _payload()
    payload["unexpected"] = True

    with pytest.raises(ValidationError):
        admission.MandatoryVetoRealSourceAdapterContractFreezeAdmission.model_validate(payload)
