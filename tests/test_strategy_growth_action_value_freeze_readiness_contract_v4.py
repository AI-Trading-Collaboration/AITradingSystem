from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract import (
    EpisodeInterval,
)
from ai_trading_system.strategy_growth_action_value_freeze_readiness_contract_v4 import (
    CostReconciliationSessionV4,
    StrategyGrowthActionValueFreezeReadinessContractV4,
    active_episode_intervals_v4,
    load_strategy_growth_action_value_freeze_readiness_contract_v4,
    maximum_keyed_cost_reconciliation_residual_v4,
)

CONFIG_PATH = Path(
    "config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml"
)
EXPECTED_FILE_SHA256 = "c90c4cc22b8918e90641bf0553416a68458433bea750bd2064fcf98df7886215"
EXPECTED_CANONICAL_SHA256 = "00198bb84cd57f518d0370035b5a5a38b12c9804880d7bf1e475ddd80a77bfc2"


def test_loads_v4_with_immutable_v3_and_dq_v3_bindings() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract_v4()

    assert result.contract_file_sha256 == EXPECTED_FILE_SHA256
    assert result.contract_canonical_sha256 == EXPECTED_CANONICAL_SHA256
    assert result.contract.sheet_version == "4.0.0"
    assert result.contract.sheet_status == "OWNER_FROZEN_NON_EXECUTABLE_DATA_RESEARCH"
    assert result.predecessor.contract_file_sha256 == (
        "304b5de907bbc0858d2ca1f6786e9e325d5493572561b8e4cff71fa91ff05375"
    )
    assert result.predecessor.contract_canonical_sha256 == (
        "68acb53ce3a2c2656565f24a98fe2de5b700d0ed3b994b9b3b20477f7aa6edb0"
    )
    assert result.dq_successor.contract_file_sha256 == (
        "96eafe7525704a8e0e260c9ed344adf3420f7e1c977e877a557856258fee3144"
    )
    assert result.dq_successor.contract_canonical_sha256 == (
        "e8e180b147e1a88dad3776f886b8eb7398481b1518785b6a2243ae795f4a6ede"
    )


def test_six_axes_preserve_v3_semantics_except_owner_freeze_state() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract_v4()

    for index in (0, 1, 2, 3, 6, 7):
        current = result.contract.axis_contracts[index].model_dump(mode="json")
        predecessor = result.predecessor.contract.axis_contracts[index].model_dump(mode="json")
        assert current.pop("owner_review_state") == (
            "APPROVED_EXACTLY_AS_DRAFTED_NON_EXECUTABLE_DATA_RESEARCH"
        )
        assert predecessor.pop("owner_review_state") == "PENDING_SUCCESSOR_OWNER_APPROVAL"
        assert current == predecessor


def test_only_dq_and_sample_axes_are_versioned_successors() -> None:
    contract = load_strategy_growth_action_value_freeze_readiness_contract_v4().contract
    dq = contract.axis_contracts[4]
    sample = contract.axis_contracts[5]

    assert dq.dq_successor_contract_id == (  # type: ignore[union-attr]
        "strategy_growth_action_value_canonical_dq_pit_contract_v3"
    )
    assert dq.numeric_policy_state == (  # type: ignore[union-attr]
        "OWNER_FROZEN_NON_EXECUTABLE_DATA_RESEARCH"
    )
    assert dq.executable_evidence_disposition == (  # type: ignore[union-attr]
        "INSUFFICIENT_EVIDENCE_TO_APPROVE"
    )
    assert sample.episode_contract.right_censor_application_order == (  # type: ignore[union-attr]
        "TRANSITIVE_CLUSTER_MERGE_BEFORE_RIGHT_CENSOR_EXCLUSION"
    )
    assert sample.episode_contract.connected_right_censored_cluster_rule == (  # type: ignore[union-attr]
        "ANY_RIGHT_CENSORED_RAW_MEMBER_EXCLUDES_ENTIRE_CONNECTED_CLUSTER"
    )


def _record(session: date, *, residual: Decimal = Decimal("0")) -> CostReconciliationSessionV4:
    return CostReconciliationSessionV4(
        session_date=session,
        candidate_gross=Decimal("0.0100"),
        candidate_net=Decimal("0.0090") - residual,
        candidate_modeled_cost=Decimal("0.0010"),
        comparator_gross=Decimal("0.0050"),
        comparator_net=Decimal("0.0045"),
        comparator_modeled_cost=Decimal("0.0005"),
    )


def test_keyed_cost_reconciliation_is_order_independent() -> None:
    sessions = (date(2021, 2, 22), date(2021, 2, 23))
    first = _record(sessions[0])
    second = _record(sessions[1], residual=Decimal("0.0001"))

    forward = maximum_keyed_cost_reconciliation_residual_v4(
        (first, second), expected_sessions=sessions
    )
    reversed_result = maximum_keyed_cost_reconciliation_residual_v4(
        (second, first), expected_sessions=sessions
    )

    assert forward == Decimal("0.0001")
    assert reversed_result == forward


def test_keyed_cost_reconciliation_rejects_duplicate_or_missing_session() -> None:
    sessions = (date(2021, 2, 22), date(2021, 2, 23))
    first = _record(sessions[0])

    with pytest.raises(ValueError, match="unique"):
        maximum_keyed_cost_reconciliation_residual_v4(
            (first, first), expected_sessions=sessions
        )
    with pytest.raises(ValueError, match="key set mismatch"):
        maximum_keyed_cost_reconciliation_residual_v4(
            (first,), expected_sessions=sessions
        )


def test_connected_right_censored_tail_excludes_entire_cluster() -> None:
    result = active_episode_intervals_v4(
        (False, True, False, True, True), merge_distance=20
    )

    assert result.intervals == ()
    assert result.right_censored_count == 1


def test_disconnected_completed_episode_survives_right_censored_cluster() -> None:
    active = [False] * 30
    active[1] = True
    active[25:] = [True] * 5

    result = active_episode_intervals_v4(active, merge_distance=20)

    assert result.intervals == (EpisodeInterval(1, 1, 1),)
    assert result.right_censored_count == 1


def test_transitive_connected_tail_excludes_prior_chain_members() -> None:
    active = [False] * 25
    active[1] = True
    active[10] = True
    active[20:] = [True] * 5

    result = active_episode_intervals_v4(active, merge_distance=10)

    assert result.intervals == ()
    assert result.right_censored_count == 1


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["axis_contracts"][0].__setitem__(
                "minimum_non_beta_return_delta", "0.0101"
            ),
            "non-beta axis drifted",
        ),
        (
            lambda payload: payload["axis_contracts"][4].__setitem__(
                "numeric_policy_state", "NON_EXECUTABLE_PILOT_POLICY_PENDING_REVIEW"
            ),
            "numeric_policy_state",
        ),
        (
            lambda payload: payload["axis_contracts"][5]["episode_contract"].__setitem__(
                "right_censor_application_order", "DROP_BEFORE_MERGE"
            ),
            "right_censor_application_order",
        ),
    ],
)
def test_exact_values_and_outcomes_reject_tamper(mutate, match: str) -> None:
    payload = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))
    mutate(payload)

    with pytest.raises(ValueError, match=match):
        StrategyGrowthActionValueFreezeReadinessContractV4.model_validate(payload)


def test_owner_freeze_is_recorded_while_all_execution_paths_remain_closed() -> None:
    contract = load_strategy_growth_action_value_freeze_readiness_contract_v4().contract

    assert contract.decision_timing.exact_owner_approval_visible is True
    assert contract.decision_timing.new_dq_result_visible is False
    assert contract.decision_timing.new_strategy_result_visible is False
    assert {item.owner_review_state for item in contract.axis_contracts} == {
        "APPROVED_EXACTLY_AS_DRAFTED_NON_EXECUTABLE_DATA_RESEARCH"
    }
    assert contract.terminal.threshold_bundle_frozen is True
    assert contract.terminal.dq_successor_authorized is True
    assert contract.terminal.empirical_successor_authorized is False
    assert contract.safety.dq_run_authorized is False
    assert contract.safety.empirical_research_authorized is False
    assert contract.safety.backtest_authorized is False
    assert contract.safety.paper_allowed is False
    assert contract.safety.live_allowed is False
    assert contract.safety.broker_allowed is False
    assert contract.safety.production_effect == "none"
    assert contract.safety.broker_action == "none"


def test_canonical_replay_is_stable() -> None:
    result = load_strategy_growth_action_value_freeze_readiness_contract_v4()
    replay = StrategyGrowthActionValueFreezeReadinessContractV4.model_validate_json(
        result.contract.canonical_bytes
    )

    assert replay == result.contract
    assert replay.canonical_sha256 == EXPECTED_CANONICAL_SHA256
