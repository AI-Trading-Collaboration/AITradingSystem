from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft as decision_pack,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

DEFAULT_PATH = decision_pack.DEFAULT_OWNER_FREEZE_DECISION_PACK_DRAFT_PATH
DecisionPack = decision_pack.MandatoryVetoOwnerFreezeDecisionPackDraft
DecisionPackError = decision_pack.MandatoryVetoOwnerFreezeDecisionPackDraftError
load_decision_pack = decision_pack.load_mandatory_veto_owner_freeze_decision_pack_draft

EXPECTED_FILE_SHA256 = "4f188c6e10758a32984bb92c3252507636686f97404c4491df014c1d22807479"
EXPECTED_CANONICAL_SHA256 = (
    "c8838a4baef788a6b936e4e098658413e2c563e169f1ec4a5da8ec7318c9e4af"
)
EXPECTED_PARENT_FILE_SHA256 = (
    "8bd9799b42a0d2f547afbb5bb8708775bef0de35d504197b117ed210e49a6baa"
)
EXPECTED_PARENT_CANONICAL_SHA256 = (
    "a6e3ff096d5c5c6df6ec76756581bf0262be4988b696cb2cfb6457dd1b07f063"
)
EXPECTED_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)


def _payload() -> dict[str, Any]:
    path = PROJECT_ROOT / DEFAULT_PATH
    loaded = load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))
    assert isinstance(loaded, dict)
    return loaded


def test_loader_replays_parent_and_exact_decision_pack_identity() -> None:
    result = load_decision_pack()

    assert result.terminal == "OWNER_EXACT_FREEZE_DECISION_REQUIRED_0_OF_4_ADMITTED"
    assert result.file_sha256 == EXPECTED_FILE_SHA256
    assert result.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.producer_draft.file_sha256 == EXPECTED_PARENT_FILE_SHA256
    assert result.producer_draft.canonical_sha256 == EXPECTED_PARENT_CANONICAL_SHA256
    assert result.policy.target_inventory.expected_session_count == 1202


def test_all_four_decision_objects_are_reviewable_but_unfrozen() -> None:
    policy = load_decision_pack().policy

    assert tuple(row.veto_id for row in policy.decision_rows) == EXPECTED_VETO_IDS
    assert policy.aggregate_state.decision_objects_ready_for_owner_review == EXPECTED_VETO_IDS
    assert policy.aggregate_state.owner_frozen_producer_contracts == ()
    assert policy.aggregate_state.admitted_producer_contracts == ()
    assert policy.aggregate_state.unresolved_producer_contracts == EXPECTED_VETO_IDS
    for row in policy.decision_rows:
        assert row.recommendation_state == "RECOMMENDED_PROPOSAL_NOT_OWNER_FROZEN"
        assert row.decision_object_complete_for_owner_review is True
        assert row.owner_exact_freeze_granted is False
        assert row.producer_contract_admitted is False
        assert row.exact_1202_session_inventory_admitted is False
        assert row.observed_inventory_lf_sha256 is None
        assert row.series_generation_allowed is False
        assert "OWNER_EXACT_FREEZE_NOT_GRANTED" in row.open_evidence_blockers


def test_broad_market_recommendation_is_spy_only_and_compatibility_anchored() -> None:
    broad = load_decision_pack().policy.decision_rows[0]

    assert broad.veto_id == "broad_market_risk_off_veto"
    assert broad.producer_decision.independent_input_universe == (
        "SPY.exchange_session",
        "SPY.adjusted_close",
        "SPY.available_at",
        "SPY.source_identity",
    )
    assert broad.formula_decision.window_inventory == {
        "moving_average_sessions": 200,
        "drawdown_sessions": 63,
    }
    assert broad.formula_decision.threshold_inventory == {
        "drawdown_fraction_lte": -0.10
    }
    assert broad.formula_decision.combination_rule == "OR"
    assert "growth_allowed inverse alias" in broad.rejected_alternatives


def test_realized_volatility_recommendation_keeps_legacy_values_as_proposal_only() -> None:
    volatility = load_decision_pack().policy.decision_rows[1]

    assert volatility.formula_decision.window_inventory == {
        "vix_percentile_sessions": 252,
        "realized_volatility_sessions": 20,
        "annualization_sessions": 252,
    }
    assert volatility.formula_decision.threshold_inventory == {
        "vix_percentile_gte": 0.75,
        "realized_volatility_fraction_gt": 0.25,
    }
    assert volatility.owner_exact_freeze_granted is False
    assert volatility.producer_contract_admitted is False
    assert "UNVALIDATED" in load_decision_pack().policy.review_policy.calibration_status


def test_event_recommendation_requires_official_pit_lineage() -> None:
    event = load_decision_pack().policy.decision_rows[2]

    assert event.pit_decision.published_at_required is True
    assert event.pit_decision.revision_identity_required is True
    assert set(event.pit_decision.required_source_fields) >= {
        "scheduled_for",
        "published_at",
        "revision_id",
    }
    assert tuple(item.authority for item in event.admitted_event_taxonomy) == (
        "FEDERAL_RESERVE",
        "BLS",
        "BEA",
    )
    assert event.formula_decision.window_inventory == {
        "pre_event_qqq_sessions": 1,
        "post_event_qqq_sessions": 0,
    }
    assert event.source_precedence == "EXACT_OFFICIAL_AUTHORITY_ONLY_NO_CROSS_PROVIDER_FILL"
    assert "event_date substituted for published_at" in event.rejected_alternatives


def test_trend_recommendation_is_qqq_only_with_explicit_recovery() -> None:
    trend = load_decision_pack().policy.decision_rows[3]

    assert all(
        field.startswith("QQQ.")
        for field in trend.producer_decision.independent_input_universe
    )
    assert trend.formula_decision.window_inventory == {
        "moving_average_sessions": 200,
        "drawdown_sessions": 63,
        "recovery_confirmation_sessions": 2,
    }
    assert trend.formula_decision.threshold_inventory == {
        "drawdown_fraction_lte": -0.12,
        "recovery_close_vs_sma200_gte": 0.0,
    }
    assert trend.formula_decision.recovery_rule == (
        "TWO_CONSECUTIVE_CLOSES_AT_OR_ABOVE_SMA200"
    )


def test_decision_pack_cannot_drive_runtime_or_external_actions() -> None:
    policy = load_decision_pack().policy

    assert policy.review_policy.recommendation_values_may_drive_runtime is False
    assert policy.review_policy.partial_owner_freeze_may_generate_series is False
    assert policy.safety.non_executable_data_research_only is True
    assert policy.safety.recommendation_values_are_runtime_policy is False
    assert policy.safety.exact_formula_or_threshold_owner_frozen is False
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


def test_canonical_round_trip_preserves_proposals_and_zero_admission() -> None:
    result = load_decision_pack()
    replay = DecisionPack.model_validate(json.loads(result.policy.canonical_bytes))

    assert replay.canonical_bytes == result.policy.canonical_bytes
    assert replay.canonical_sha256 == EXPECTED_CANONICAL_SHA256
    text = result.policy.canonical_bytes.decode("utf-8")
    assert '"owner_exact_freeze_granted": false' in text
    assert '"observed_inventory_lf_sha256": null' in text
    assert '"veto_series_sha256"' not in text
    assert '"backtest_id"' not in text
    assert '"orders_allowed": true' not in text


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["decision_rows"][0].__setitem__(
                "owner_exact_freeze_granted", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["decision_rows"][1].__setitem__(
                "producer_contract_admitted", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["decision_rows"][2]["pit_decision"].__setitem__(
                "published_at_required", False
            ),
            "scheduled-event recommendation requires published_at",
        ),
        (
            lambda payload: payload["decision_rows"][2].__setitem__(
                "source_precedence", "CONVENIENCE_PROVIDER_FILL"
            ),
            "scheduled-event source precedence drifted",
        ),
        (
            lambda payload: payload["decision_rows"][0]["producer_decision"][
                "independent_input_universe"
            ].append("QQQ.adjusted_close"),
            "broad-market recommendation cannot read QQQ",
        ),
        (
            lambda payload: payload["decision_rows"][3]["producer_decision"][
                "independent_input_universe"
            ].append("SPY.adjusted_close"),
            "underlying-trend recommendation must be QQQ-only",
        ),
        (
            lambda payload: payload["aggregate_state"]["admitted_producer_contracts"].append(
                "realized_volatility_veto"
            ),
            "Tuple should have at most 0 items",
        ),
        (
            lambda payload: payload["producer_draft_binding"].__setitem__(
                "canonical_sha256", "0" * 64
            ),
            "producer-draft exact identity drifted",
        ),
    ],
)
def test_schema_rejects_false_freeze_admission_or_identity_drift(
    mutate: Callable[[dict[str, Any]], None], match: str
) -> None:
    payload = deepcopy(_payload())
    mutate(payload)

    with pytest.raises(ValidationError, match=match):
        DecisionPack.model_validate(payload)


def test_loader_rejects_parent_traversal() -> None:
    path = Path(
        "../AITradingSystem/config/research/"
        "qc_qqq_options_growth_action_value_mandatory_veto_owner_freeze_decision_pack_draft_v1.yaml"
    )

    with pytest.raises(DecisionPackError, match="escapes project root"):
        load_decision_pack(path=path, project_root=PROJECT_ROOT)
