from __future__ import annotations

from typing import Any

import pytest
from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    Path,
    _assert_safety,
    _run_candidate_batch,
    _write_price_caches,
    run_controlled_strategy_batch_review,
    run_gbdt_action_utility_baseline,
    run_gbdt_pivot_direction_selection,
    run_gbdt_pivot_review,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_expansion_gate,
    run_regret_state_machine_controlled_prototype,
    run_simple_strategy_selector_pilot,
    run_value_surface_controlled_expansion,
)


@pytest.fixture(scope="module")
def candidate_price_caches(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, Path]:
    tmp_path = tmp_path_factory.mktemp("candidate_price_caches")
    return _write_price_caches(tmp_path)


@pytest.fixture(scope="module")
def state_machine_context(
    tmp_path_factory: pytest.TempPathFactory,
    candidate_price_caches: tuple[Path, Path, Path],
) -> tuple[dict[str, Any], Path]:
    prices_path, marketstack_path, rates_path = candidate_price_caches
    output_root = tmp_path_factory.mktemp("state_machine_payload") / "state_machine"
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    return payload, output_root


@pytest.fixture(scope="module")
def simple_selector_payload(
    tmp_path_factory: pytest.TempPathFactory,
    candidate_price_caches: tuple[Path, Path, Path],
) -> dict[str, Any]:
    prices_path, marketstack_path, rates_path = candidate_price_caches
    return run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path_factory.mktemp("simple_selector_payload") / "simple",
        as_of_date=TEST_AS_OF,
    )


@pytest.fixture(scope="module")
def gbdt_context(
    tmp_path_factory: pytest.TempPathFactory,
    candidate_price_caches: tuple[Path, Path, Path],
) -> tuple[dict[str, Any], Path]:
    prices_path, marketstack_path, rates_path = candidate_price_caches
    output_root = tmp_path_factory.mktemp("gbdt_payload") / "gbdt"
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=TEST_AS_OF,
    )
    return payload, output_root


@pytest.fixture(scope="module")
def gbdt_pivot_payload(
    tmp_path_factory: pytest.TempPathFactory,
    gbdt_context: tuple[dict[str, Any], Path],
) -> dict[str, Any]:
    gbdt, _output_root = gbdt_context
    return run_gbdt_pivot_review(
        gbdt_action_utility_path=Path(gbdt["artifact_paths"]["json_path"]),
        output_root=tmp_path_factory.mktemp("gbdt_pivot_payload") / "gbdt_pivot",
    )


@pytest.fixture(scope="module")
def gbdt_selection_payload(
    tmp_path_factory: pytest.TempPathFactory,
    gbdt_pivot_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_gbdt_pivot_direction_selection(
        gbdt_pivot_review_path=Path(gbdt_pivot_payload["artifact_paths"]["json_path"]),
        output_root=tmp_path_factory.mktemp("gbdt_selection_payload") / "gbdt_selection",
    )


@pytest.fixture(scope="module")
def value_expansion_payload(
    tmp_path_factory: pytest.TempPathFactory,
    candidate_price_caches: tuple[Path, Path, Path],
) -> dict[str, Any]:
    prices_path, marketstack_path, rates_path = candidate_price_caches
    return run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path_factory.mktemp("value_expansion_payload") / "value_expansion",
        as_of_date=TEST_AS_OF,
    )


@pytest.fixture(scope="module")
def regret_gate_payload(
    tmp_path_factory: pytest.TempPathFactory,
    state_machine_context: tuple[dict[str, Any], Path],
    value_expansion_payload: dict[str, Any],
) -> dict[str, Any]:
    state, state_output_root = state_machine_context
    return run_regret_casebook_expansion_gate(
        regret_state_machine_path=Path(state["artifact_paths"]["json_path"]),
        state_transition_casebook_path=state_output_root / "state_transition_casebook.json",
        value_surface_expansion_path=Path(value_expansion_payload["artifact_paths"]["json_path"]),
        output_root=tmp_path_factory.mktemp("regret_gate_payload") / "regret_gate",
    )


@pytest.fixture(scope="module")
def regret_activation_payload(
    tmp_path_factory: pytest.TempPathFactory,
    value_expansion_payload: dict[str, Any],
    regret_gate_payload: dict[str, Any],
) -> dict[str, Any]:
    return run_regret_activation_inputs_from_value_surface_failures(
        value_surface_expansion_path=Path(value_expansion_payload["artifact_paths"]["json_path"]),
        regret_casebook_expansion_gate_path=Path(regret_gate_payload["artifact_paths"]["json_path"]),
        output_root=tmp_path_factory.mktemp("regret_activation_payload") / "regret_inputs",
    )


@pytest.fixture(scope="module")
def candidate_batch_paths(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("candidate_batch_paths")
    return _run_candidate_batch(tmp_path)


@pytest.fixture(scope="module")
def batch_review_payload(
    tmp_path_factory: pytest.TempPathFactory,
    candidate_batch_paths: dict[str, Path],
) -> dict[str, Any]:
    return run_controlled_strategy_batch_review(
        value_surface_path=candidate_batch_paths["value_surface"],
        regret_state_machine_path=candidate_batch_paths["state_machine"],
        simple_selector_path=candidate_batch_paths["simple"],
        gbdt_action_utility_path=candidate_batch_paths["gbdt"],
        output_root=tmp_path_factory.mktemp("batch_review_payload") / "review",
    )


def test_regret_state_machine_schema(
    state_machine_context: tuple[dict[str, Any], Path],
) -> None:
    payload, output_root = state_machine_context

    _assert_safety(payload)
    assert payload["report_type"] == "regret_state_machine_controlled_prototype"
    assert payload["summary"]["state_transition_explainable"] is True
    assert payload["state_transition_table"]
    assert (output_root / "state_transition_casebook.json").exists()


def test_state_transition_has_explanation(
    state_machine_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = state_machine_context
    assert all(row["explanation"] for row in payload["state_transition_table"])


def test_regret_type_mapping_required(
    state_machine_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = state_machine_context
    assert payload["summary"]["regret_type_mapping_present"] is True
    assert payload["regret_type_coverage"]


def test_state_machine_turnover_guardrail_reported(
    state_machine_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = state_machine_context
    assert payload["summary"]["turnover_guardrail_reported"] is True
    assert "turnover_not_worse_than_baseline_guardrail" in payload
    assert payload["summary"]["whipsaw_report_present"] is True


def test_state_machine_promotion_false(
    state_machine_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = state_machine_context
    _assert_safety(payload)


def test_simple_strategy_selector_schema(simple_selector_payload: dict[str, Any]) -> None:
    payload = simple_selector_payload
    _assert_safety(payload)
    assert payload["report_type"] == "simple_strategy_selector_pilot"
    assert payload["summary"]["simple_strategy_count"] >= payload["summary"]["configured_minimum"]
    assert payload["summary"]["selector_rules_present"] is True


def test_selector_compares_to_best_simple_strategy(
    simple_selector_payload: dict[str, Any],
) -> None:
    payload = simple_selector_payload
    assert payload["summary"]["best_simple_benchmark_comparison_present"] is True
    assert payload["best_single_strategy_comparison"]["best_single_strategy"]


def test_selector_overfit_warning_present(simple_selector_payload: dict[str, Any]) -> None:
    payload = simple_selector_payload
    assert payload["summary"]["selector_overfit_warning_present"] is True
    assert payload["overfit_warning"]["not_validated_utility_boundary"] is True


def test_selector_promotion_false(simple_selector_payload: dict[str, Any]) -> None:
    payload = simple_selector_payload
    _assert_safety(payload)


def test_gbdt_action_utility_schema(
    gbdt_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = gbdt_context
    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_action_utility_baseline"
    assert payload["summary"]["model_run_complete"] is True
    assert payload["action_utility_prediction"]


def test_gbdt_no_future_features(
    gbdt_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = gbdt_context
    assert payload["summary"]["future_feature_violation_count"] == 0
    assert payload["future_feature_audit"]["future_outcome_role"] == "evaluation_label_only"
    assert payload["future_feature_audit"]["input_feature_policy"] == (
        "PIT_state_action_horizon_cost_only"
    )


def test_gbdt_negative_control_required(
    gbdt_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = gbdt_context
    assert payload["summary"]["negative_control_pass"] is True
    assert (
        payload["negative_control_result"]["random_label_check"]["random_label_promoted"] is False
    )


def test_gbdt_feature_importance_present(
    gbdt_context: tuple[dict[str, Any], Path],
) -> None:
    payload, output_root = gbdt_context
    assert payload["summary"]["feature_importance_report_present"] is True
    assert payload["feature_importance"]
    assert (output_root / "gbdt_feature_importance.json").exists()


def test_gbdt_promotion_false(
    gbdt_context: tuple[dict[str, Any], Path],
) -> None:
    payload, _output_root = gbdt_context
    _assert_safety(payload)


def test_gbdt_pivot_review_design_only(gbdt_pivot_payload: dict[str, Any]) -> None:
    payload = gbdt_pivot_payload
    _assert_safety(payload)
    assert payload["status"] == "PIVOT_REVIEW_READY"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["local_parameter_tuning_allowed"] is False
    assert payload["summary"]["pivot_option_count"] >= 4


def test_regret_casebook_expansion_gate_watchlist(
    regret_gate_payload: dict[str, Any],
) -> None:
    payload = regret_gate_payload
    _assert_safety(payload)
    assert payload["report_type"] == "regret_casebook_expansion_gate"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_casebook_expansion_allowed"] is False
    assert any(not row["passed"] for row in payload["activation_gate"])


def test_gbdt_pivot_direction_selection_no_training(
    gbdt_selection_payload: dict[str, Any],
) -> None:
    payload = gbdt_selection_payload
    _assert_safety(payload)
    assert payload["status"] == "PIVOT_DIRECTION_SELECTED"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["model_training_allowed"] is False
    assert payload["summary"]["selected_pivot_direction"] == "gbdt_value_surface_residual_model"
    assert all(row["minimum_viable_experiment"] for row in payload["candidate_directions"])


def test_regret_activation_inputs_stay_watchlist_when_incomplete(
    regret_activation_payload: dict[str, Any],
) -> None:
    payload = regret_activation_payload
    _assert_safety(payload)
    assert payload["report_type"] == "regret_activation_inputs_from_value_surface_failures"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_activation_ready"] is False
    assert payload["watchlist_decision"]["expand_state_machine_now"] is False


def test_controlled_strategy_batch_review_schema(batch_review_payload: dict[str, Any]) -> None:
    review = batch_review_payload
    _assert_safety(review)
    assert review["report_type"] == "controlled_strategy_batch_review"
    assert review["summary"]["all_candidates_have_decision"] is True
    assert review["status"] == "CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE"


def test_all_candidates_have_decision(batch_review_payload: dict[str, Any]) -> None:
    review = batch_review_payload
    decisions = {row["candidate_id"]: row["decision"] for row in review["candidate_decisions"]}
    assert decisions.keys() == {
        "value_surface",
        "regret_state_machine",
        "simple_strategy_selector",
        "gbdt_action_utility",
    }


def test_no_promotion_from_controlled_review(batch_review_payload: dict[str, Any]) -> None:
    review = batch_review_payload
    assert review["summary"]["no_candidate_promoted_without_policy"] is True
    assert all(row["promotion_gate_allowed"] is False for row in review["candidate_decisions"])


def test_kill_pause_pivot_enum(batch_review_payload: dict[str, Any]) -> None:
    review = batch_review_payload
    allowed = {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL", "PIVOT", "INFRA_REVIEW"}
    assert {row["decision"] for row in review["candidate_decisions"]} <= allowed
    assert review["summary"]["kill_pause_pivot_decisions_present"] is True


def test_next_batch_recommendation_present(batch_review_payload: dict[str, Any]) -> None:
    review = batch_review_payload
    assert review["summary"]["next_batch_recommendation_present"] is True
    assert review["next_batch_recommendation"]["promotion_gate_allowed"] is False
