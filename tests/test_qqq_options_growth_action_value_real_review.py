from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pytest

from ai_trading_system.qqq_options_research.growth_action_value_real_review import (
    DEFAULT_GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_PATH,
    GrowthActionValueRealReviewPolicy,
    load_growth_action_value_real_review_policy,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, object]:
    path = PROJECT_ROOT / DEFAULT_GROWTH_ACTION_VALUE_REAL_REVIEW_POLICY_PATH
    return load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))


def test_policy_loads_with_exact_authority_identity_and_non_dispatchable_terminal() -> None:
    result = load_growth_action_value_real_review_policy()

    assert result.terminal == (
        "DRAFT_READY_FOR_OWNER_REVIEW_WITH_EXPLICIT_VETO_INPUT_BLOCKER"
    )
    assert result.policy.status == "DRAFT_FOR_OWNER_EXACT_FREEZE"
    assert result.policy_file_sha256 == hashlib.sha256(
        result.policy_path.read_bytes()
    ).hexdigest()
    assert result.policy_canonical_sha256 == hashlib.sha256(
        result.policy.canonical_bytes
    ).hexdigest()
    assert result.policy.owner_process_authorization.real_run_dispatch_authorized is False
    assert result.policy.safety.policy_values_frozen is False
    assert result.policy.safety.manifest_generation_allowed is False
    assert result.policy.safety.provider_query_authorized is False
    assert result.policy.safety.order_generation_allowed is False
    assert result.policy.safety.production_effect == "none"
    assert result.policy.safety.broker_action == "none"


def test_policy_freezes_the_complete_owner_review_surface() -> None:
    policy = load_growth_action_value_real_review_policy().policy
    selection = policy.contributor_selection

    assert (selection.min_dte, selection.target_dte, selection.max_dte) == (7, 14, 21)
    assert selection.max_abs_moneyness_deviation == Decimal("0.05")
    assert (
        selection.min_abs_prior_day_delta,
        selection.target_abs_prior_day_delta,
        selection.max_abs_prior_day_delta,
    ) == (Decimal("0.30"), Decimal("0.40"), Decimal("0.55"))
    assert (
        selection.max_quote_age_seconds,
        selection.max_relative_spread,
        selection.min_prior_session_open_interest,
        selection.min_decision_as_of_cumulative_volume,
    ) == (120, Decimal("0.20"), 10, 1)
    assert selection.deterministic_rank_components[-1] == "STABLE_OPTION_SID"
    assert selection.total_expected_contributor_count_per_session == 2
    assert policy.growth_state_mapping.signal_lag_sessions == 1
    assert policy.growth_state_mapping.same_session_weight_application_allowed is False
    assert policy.action_sizing.baseline_research_weights.QQQ == Decimal("0.50")
    assert policy.action_sizing.growth_active_research_weights.QQQ == Decimal("0.60")
    assert policy.action_sizing.maximum_qqq_increment_over_baseline == Decimal("0.10")


def test_provider_adapter_separates_daily_availability_from_minute_quote_time() -> None:
    adapter = load_growth_action_value_real_review_policy().policy.provider_adapter

    assert adapter.catalog_and_open_interest.source_type == "OptionUniverse"
    assert adapter.catalog_and_open_interest.available_at_field == "EndTime"
    assert adapter.catalog_and_open_interest.current_session_open_interest_allowed is False
    assert adapter.quote.source_type == "MINUTE_QUOTE_BAR"
    assert adapter.quote.quote_end_field == "EndTime"
    assert adapter.quote.daily_option_universe_time_as_quote_end_allowed is False
    assert adapter.volume.source_type == "MINUTE_TRADE_BAR"
    assert adapter.volume.end_of_day_or_revised_volume_allowed is False


def test_external_scope_is_single_use_zero_order_and_zero_fill() -> None:
    scope = load_growth_action_value_real_review_policy().policy.external_scope

    assert scope.target_clone_project_id == 35444189
    assert scope.original_project_id == 34808569
    assert scope.maximum_existing_clone_mutations == 1
    assert scope.maximum_zero_order_backtests == 1
    assert scope.maximum_retries == 0
    assert scope.maximum_new_clones == 0
    assert scope.orders == scope.fills == scope.positions == 0
    assert scope.raw_option_rows_exported == 0
    assert scope.contract_identifiers_exported == 0


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda payload: payload["provider_adapter"]["quote"].__setitem__(
                "daily_option_universe_time_as_quote_end_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["growth_state_mapping"].__setitem__(
                "same_session_weight_application_allowed", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["contributor_selection"].__setitem__(
                "max_relative_spread", "0.21"
            ),
            "spread must inherit DQ/PIT V3",
        ),
        (
            lambda payload: payload["action_sizing"][
                "growth_active_research_weights"
            ].__setitem__("QQQ", "0.70"),
            "weights must be long-only and sum exactly to one",
        ),
        (
            lambda payload: payload["defensive_veto_input"].__setitem__(
                "missing_veto_interpreted_as_clear", True
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["safety"].__setitem__(
                "real_run_dispatch_authorized", True
            ),
            "Input should be False",
        ),
    ],
)
def test_policy_rejects_semantic_or_safety_drift(mutate: object, match: str) -> None:
    payload = deepcopy(_payload())
    mutate(payload)  # type: ignore[operator]

    with pytest.raises(ValueError, match=match):
        GrowthActionValueRealReviewPolicy.model_validate(payload)


def test_policy_rejects_rank_reordering_and_veto_omission() -> None:
    rank_payload = deepcopy(_payload())
    rank = rank_payload["contributor_selection"]["deterministic_rank_components"]
    rank_payload["contributor_selection"]["deterministic_rank_components"] = [
        rank[-1],
        *rank[:-1],
    ]
    with pytest.raises(ValueError, match="deterministic rank order drifted"):
        GrowthActionValueRealReviewPolicy.model_validate(rank_payload)

    veto_payload = deepcopy(_payload())
    veto_payload["defensive_veto_input"]["required_veto_types"].pop()
    with pytest.raises(ValueError, match="veto taxonomy drifted"):
        GrowthActionValueRealReviewPolicy.model_validate(veto_payload)


def test_canonical_round_trip_is_stable_and_contains_no_result() -> None:
    policy = load_growth_action_value_real_review_policy().policy
    payload = json.loads(policy.canonical_bytes)
    replay = GrowthActionValueRealReviewPolicy.model_validate(payload)

    assert replay.canonical_bytes == policy.canonical_bytes
    assert replay.canonical_sha256 == policy.canonical_sha256
    text = policy.canonical_bytes.decode("utf-8")
    assert "backtest_id" not in text
    assert "GLOBAL_PASS" not in text
    assert "strategy_return" not in text
