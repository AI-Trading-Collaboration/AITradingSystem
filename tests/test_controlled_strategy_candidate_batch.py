from __future__ import annotations

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


def test_regret_state_machine_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_state_machine_controlled_prototype"
    assert payload["summary"]["state_transition_explainable"] is True
    assert payload["state_transition_table"]
    assert (tmp_path / "state_machine" / "state_transition_casebook.json").exists()


def test_state_transition_has_explanation(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert all(row["explanation"] for row in payload["state_transition_table"])


def test_regret_type_mapping_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["regret_type_mapping_present"] is True
    assert payload["regret_type_coverage"]


def test_state_machine_turnover_guardrail_reported(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["turnover_guardrail_reported"] is True
    assert "turnover_not_worse_than_baseline_guardrail" in payload
    assert payload["summary"]["whipsaw_report_present"] is True


def test_state_machine_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state_machine",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_simple_strategy_selector_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "simple_strategy_selector_pilot"
    assert payload["summary"]["simple_strategy_count"] >= payload["summary"]["configured_minimum"]
    assert payload["summary"]["selector_rules_present"] is True


def test_selector_compares_to_best_simple_strategy(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["best_simple_benchmark_comparison_present"] is True
    assert payload["best_single_strategy_comparison"]["best_single_strategy"]


def test_selector_overfit_warning_present(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["selector_overfit_warning_present"] is True
    assert payload["overfit_warning"]["not_validated_utility_boundary"] is True


def test_selector_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_gbdt_action_utility_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_action_utility_baseline"
    assert payload["summary"]["model_run_complete"] is True
    assert payload["action_utility_prediction"]


def test_gbdt_no_future_features(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["future_feature_violation_count"] == 0
    assert payload["future_feature_audit"]["future_outcome_role"] == "evaluation_label_only"
    assert payload["future_feature_audit"]["input_feature_policy"] == (
        "PIT_state_action_horizon_cost_only"
    )


def test_gbdt_negative_control_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["negative_control_pass"] is True
    assert (
        payload["negative_control_result"]["random_label_check"]["random_label_promoted"] is False
    )


def test_gbdt_feature_importance_present(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["feature_importance_report_present"] is True
    assert payload["feature_importance"]
    assert (tmp_path / "gbdt" / "gbdt_feature_importance.json").exists()


def test_gbdt_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)


def test_gbdt_pivot_review_design_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    payload = run_gbdt_pivot_review(
        gbdt_action_utility_path=Path(gbdt["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_pivot",
    )

    _assert_safety(payload)
    assert payload["status"] == "PIVOT_REVIEW_READY"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["local_parameter_tuning_allowed"] is False
    assert payload["summary"]["pivot_option_count"] >= 4


def test_regret_casebook_expansion_gate_watchlist(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    payload = run_regret_casebook_expansion_gate(
        regret_state_machine_path=Path(state["artifact_paths"]["json_path"]),
        state_transition_casebook_path=tmp_path / "state" / "state_transition_casebook.json",
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_gate",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_casebook_expansion_gate"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_casebook_expansion_allowed"] is False
    assert any(not row["passed"] for row in payload["activation_gate"])


def test_gbdt_pivot_direction_selection_no_training(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    pivot = run_gbdt_pivot_review(
        gbdt_action_utility_path=Path(gbdt["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_pivot",
    )
    payload = run_gbdt_pivot_direction_selection(
        gbdt_pivot_review_path=Path(pivot["artifact_paths"]["json_path"]),
        output_root=tmp_path / "gbdt_selection",
    )

    _assert_safety(payload)
    assert payload["status"] == "PIVOT_DIRECTION_SELECTED"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["summary"]["model_training_allowed"] is False
    assert payload["summary"]["selected_pivot_direction"] == "gbdt_value_surface_residual_model"
    assert all(row["minimum_viable_experiment"] for row in payload["candidate_directions"])


def test_regret_activation_inputs_stay_watchlist_when_incomplete(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    gate = run_regret_casebook_expansion_gate(
        regret_state_machine_path=Path(state["artifact_paths"]["json_path"]),
        state_transition_casebook_path=tmp_path / "state" / "state_transition_casebook.json",
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_gate",
    )
    payload = run_regret_activation_inputs_from_value_surface_failures(
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        regret_casebook_expansion_gate_path=Path(gate["artifact_paths"]["json_path"]),
        output_root=tmp_path / "regret_inputs",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_activation_inputs_from_value_surface_failures"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_activation_ready"] is False
    assert payload["watchlist_decision"]["expand_state_machine_now"] is False


def test_controlled_strategy_batch_review_schema(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    _assert_safety(review)
    assert review["report_type"] == "controlled_strategy_batch_review"
    assert review["summary"]["all_candidates_have_decision"] is True
    assert review["status"] == "CONTROLLED_STRATEGY_RESEARCH_BATCH_1_COMPLETE"


def test_all_candidates_have_decision(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    decisions = {row["candidate_id"]: row["decision"] for row in review["candidate_decisions"]}
    assert decisions.keys() == {
        "value_surface",
        "regret_state_machine",
        "simple_strategy_selector",
        "gbdt_action_utility",
    }


def test_no_promotion_from_controlled_review(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    assert review["summary"]["no_candidate_promoted_without_policy"] is True
    assert all(row["promotion_gate_allowed"] is False for row in review["candidate_decisions"])


def test_kill_pause_pivot_enum(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    allowed = {"CONTINUE", "WATCHLIST", "DATA_REQUIRED", "PAUSE", "KILL", "PIVOT", "INFRA_REVIEW"}
    assert {row["decision"] for row in review["candidate_decisions"]} <= allowed
    assert review["summary"]["kill_pause_pivot_decisions_present"] is True


def test_next_batch_recommendation_present(tmp_path: Path) -> None:
    paths = _run_candidate_batch(tmp_path)
    review = run_controlled_strategy_batch_review(
        value_surface_path=paths["value_surface"],
        regret_state_machine_path=paths["state_machine"],
        simple_selector_path=paths["simple"],
        gbdt_action_utility_path=paths["gbdt"],
        output_root=tmp_path / "review",
    )

    assert review["summary"]["next_batch_recommendation_present"] is True
    assert review["next_batch_recommendation"]["promotion_gate_allowed"] is False
