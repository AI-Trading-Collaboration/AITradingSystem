from __future__ import annotations

import pytest
from controlled_strategy_batch_helpers import (
    Path,
    _assert_safety,
    _run_regime_conditioning_inputs,
    run_ai_after_chatgpt_full_regime_attribution_review,
    run_cost_aware_horizon_hysteresis,
    run_cost_turnover_aware_regime_conditioned_value_surface,
    run_gbdt_residual_hypothesis_regime_conditioning,
    run_horizon_selector_controlled_prototype,
    run_horizon_selector_holdout_review,
    run_horizon_selector_problem_contract,
    run_long_horizon_quarantine_fallback_review,
    run_long_horizon_quarantine_selection_review,
    run_regime_conditioned_value_surface_controlled_review,
    run_regime_conditioned_value_surface_design,
    run_regime_conditioned_walk_forward_holdout,
    run_regime_horizon_loss_attribution_matrix,
    run_tail_loss_guardrail_fallback_policy,
    run_value_surface_v2_controlled_review,
)


@pytest.fixture(scope="module")
def regime_conditioning_inputs(
    tmp_path_factory: pytest.TempPathFactory,
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("regime_conditioning_inputs")
    return _run_regime_conditioning_inputs(tmp_path)


@pytest.fixture(scope="module")
def value_surface_v2_inputs(
    tmp_path_factory: pytest.TempPathFactory,
    regime_conditioning_inputs: dict[str, Path],
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("value_surface_v2_inputs")
    paths = dict(regime_conditioning_inputs)
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "v2_design",
    )
    guardrail = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_guardrail",
    )
    return {
        **paths,
        "design": Path(design["artifact_paths"]["json_path"]),
        "guardrail": Path(guardrail["artifact_paths"]["json_path"]),
    }


@pytest.fixture(scope="module")
def value_surface_v2_full_inputs(
    tmp_path_factory: pytest.TempPathFactory,
    value_surface_v2_inputs: dict[str, Path],
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("value_surface_v2_full_inputs")
    paths = dict(value_surface_v2_inputs)
    cost = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "v2_cost",
    )
    long_horizon = run_long_horizon_quarantine_selection_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        cost_turnover_path=Path(cost["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_long_horizon",
    )
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "v2_matrix",
    )
    ai_regime = run_ai_after_chatgpt_full_regime_attribution_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_ai_regime",
    )
    return {
        **paths,
        "cost_turnover": Path(cost["artifact_paths"]["json_path"]),
        "long_horizon": Path(long_horizon["artifact_paths"]["json_path"]),
        "loss_matrix": Path(matrix["artifact_paths"]["json_path"]),
        "ai_regime": Path(ai_regime["artifact_paths"]["json_path"]),
    }


@pytest.fixture(scope="module")
def horizon_selector_review_inputs(
    tmp_path_factory: pytest.TempPathFactory,
    value_surface_v2_full_inputs: dict[str, Path],
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("horizon_selector_review_inputs")
    paths = dict(value_surface_v2_full_inputs)
    holdout = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "horizon_v2_holdout",
    )
    v2_review = run_value_surface_v2_controlled_review(
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        holdout_path=Path(holdout["artifact_paths"]["json_path"]),
        output_root=tmp_path / "horizon_v2_review",
    )
    return {
        **paths,
        "v2_holdout": Path(holdout["artifact_paths"]["json_path"]),
        "v2_review": Path(v2_review["artifact_paths"]["json_path"]),
    }


@pytest.fixture(scope="module")
def horizon_selector_inputs(
    tmp_path_factory: pytest.TempPathFactory,
    horizon_selector_review_inputs: dict[str, Path],
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("horizon_selector_inputs")
    paths = dict(horizon_selector_review_inputs)
    contract = run_horizon_selector_problem_contract(
        v2_review_path=paths["v2_review"],
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_contract",
    )
    fallback = run_long_horizon_quarantine_fallback_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=Path(contract["artifact_paths"]["json_path"]),
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "horizon_fallback",
    )
    return {
        **paths,
        "contract": Path(contract["artifact_paths"]["json_path"]),
        "fallback_review": Path(fallback["artifact_paths"]["json_path"]),
    }


@pytest.fixture(scope="module")
def horizon_selector_full_inputs(
    tmp_path_factory: pytest.TempPathFactory,
    horizon_selector_inputs: dict[str, Path],
) -> dict[str, Path]:
    tmp_path = tmp_path_factory.mktemp("horizon_selector_full_inputs")
    paths = dict(horizon_selector_inputs)
    prototype = run_horizon_selector_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_prototype",
    )
    hysteresis = run_cost_aware_horizon_hysteresis(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        prototype_path=Path(prototype["artifact_paths"]["json_path"]),
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_hysteresis",
    )
    return {
        **paths,
        "prototype": Path(prototype["artifact_paths"]["json_path"]),
        "hysteresis": Path(hysteresis["artifact_paths"]["json_path"]),
    }


def test_regime_conditioned_value_surface_design_protocol(
    tmp_path: Path,
    regime_conditioning_inputs: dict[str, Path],
) -> None:
    paths = regime_conditioning_inputs
    payload = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "regime_design",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_value_surface_design"
    assert payload["summary"]["regime_variable_count"] > 0
    assert payload["regimes_keep_value_surface"]
    assert payload["regimes_fallback_to_benchmark"]
    assert payload["controlled_only_validation_plan"]


def test_tail_loss_guardrail_fallback_policy_compares_variants(
    tmp_path: Path,
    regime_conditioning_inputs: dict[str, Path],
) -> None:
    paths = regime_conditioning_inputs
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "guardrail",
    )
    payload = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "guardrail",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_loss_guardrail_fallback_policy"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert {
        "original_value_surface",
        "regime_conditioned_value_surface",
        "tail_loss_guarded_value_surface",
        "benchmark_fallback_value_surface",
    }.issubset(variant_ids)
    assert "mean_delta_vs_benchmark" in payload["variant_metrics"][0]
    assert payload["guardrail_diagnostic_boundary"]["retrospective_ablation_only"] is True


def test_regime_horizon_loss_attribution_matrix(
    tmp_path: Path,
    regime_conditioning_inputs: dict[str, Path],
) -> None:
    paths = regime_conditioning_inputs
    payload = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "loss_matrix",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_horizon_loss_attribution_matrix"
    assert payload["summary"]["losing_case_count"] >= 0
    assert "max_loss_concentration_group" in payload["summary"]
    assert payload["loss_by_regime"]["summary"]["promotion_gate_allowed"] is False
    assert payload["loss_by_utility_profile"]["groups"] is not None


def test_gbdt_residual_regime_conditioning_no_strategy(
    tmp_path: Path,
    regime_conditioning_inputs: dict[str, Path],
) -> None:
    paths = regime_conditioning_inputs
    payload = run_gbdt_residual_hypothesis_regime_conditioning(
        value_surface_expansion_path=paths["value_expansion"],
        residual_triage_path=paths["residual_triage"],
        output_root=tmp_path / "residual_regime",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_residual_hypothesis_regime_conditioning"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["top_residual_features"]
    assert payload["diagnostic_boundary"]["direct_action_policy_generated"] is False


def test_regime_conditioned_controlled_review_decision_enum(
    tmp_path: Path,
    regime_conditioning_inputs: dict[str, Path],
) -> None:
    paths = regime_conditioning_inputs
    design = run_regime_conditioned_value_surface_design(
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        residual_triage_path=paths["residual_triage"],
        direction_review_path=paths["direction"],
        output_root=tmp_path / "controlled_review",
    )
    guardrail = run_tail_loss_guardrail_fallback_policy(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=Path(design["artifact_paths"]["json_path"]),
        output_root=tmp_path / "controlled_review",
    )
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "controlled_review",
    )
    residual = run_gbdt_residual_hypothesis_regime_conditioning(
        value_surface_expansion_path=paths["value_expansion"],
        residual_triage_path=paths["residual_triage"],
        output_root=tmp_path / "controlled_review",
    )
    payload = run_regime_conditioned_value_surface_controlled_review(
        design_path=Path(design["artifact_paths"]["json_path"]),
        guardrail_policy_path=Path(guardrail["artifact_paths"]["json_path"]),
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        residual_regime_path=Path(residual["artifact_paths"]["json_path"]),
        output_root=tmp_path / "controlled_review",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_value_surface_controlled_review"
    assert payload["summary"]["controlled_review_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "KILL_CURRENT_VALUE_SURFACE",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_BENCHMARK_FALLBACK",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_cost_turnover_aware_regime_conditioned_value_surface(
    tmp_path: Path,
    value_surface_v2_inputs: dict[str, Path],
) -> None:
    paths = value_surface_v2_inputs
    payload = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "cost_turnover",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "cost_turnover_aware_regime_conditioned_value_surface"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "regime_conditioned_turnover_penalty" in variant_ids
    assert "regime_conditioned_action_hysteresis" in variant_ids
    assert "regime_conditioned_no_trade_band" in variant_ids
    assert payload["variant_metrics"][0]["action_flip_count"] >= 0
    assert "turnover_delta" in payload["variant_metrics"][1]
    assert payload["v2_score_policy"]["policy_source"].endswith(
        "cost_turnover_aware_regime_conditioned_value_surface"
    )
    assert payload["diagnostic_boundary"]["production_execution_rule"] is False


def test_long_horizon_quarantine_selection_review(
    tmp_path: Path,
    value_surface_v2_inputs: dict[str, Path],
) -> None:
    paths = value_surface_v2_inputs
    cost = run_cost_turnover_aware_regime_conditioned_value_surface(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        guardrail_policy_path=paths["guardrail"],
        output_root=tmp_path / "long_horizon",
    )
    payload = run_long_horizon_quarantine_selection_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        cost_turnover_path=Path(cost["artifact_paths"]["json_path"]),
        output_root=tmp_path / "long_horizon",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "long_horizon_quarantine_selection_review"
    assert {"20d", "60d"}.issubset(set(payload["reviewed_horizons"]))
    assert payload["horizon_loss_matrix"]["summary"]["promotion_gate_allowed"] is False
    assert payload["disable_vs_downgrade_comparison"]


def test_ai_after_chatgpt_full_regime_attribution_review(
    tmp_path: Path,
    value_surface_v2_inputs: dict[str, Path],
) -> None:
    paths = value_surface_v2_inputs
    matrix = run_regime_horizon_loss_attribution_matrix(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        output_root=tmp_path / "ai_regime",
    )
    payload = run_ai_after_chatgpt_full_regime_attribution_review(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        loss_matrix_path=Path(matrix["artifact_paths"]["json_path"]),
        output_root=tmp_path / "ai_regime",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "ai_after_chatgpt_full_regime_attribution_review"
    assert payload["summary"]["target_regime"] == "ai_after_chatgpt_full"
    assert "benchmark_more_stable" in payload["benchmark_stability"]
    assert payload["candidate_repairs"]


def test_regime_conditioned_walk_forward_holdout(
    tmp_path: Path,
    value_surface_v2_full_inputs: dict[str, Path],
) -> None:
    paths = value_surface_v2_full_inputs
    payload = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "holdout",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regime_conditioned_walk_forward_holdout"
    assert payload["summary"]["holdout_case_count"] >= 0
    assert "overfit_risk" in payload["summary"]
    assert payload["leave_one_horizon_out"] is not None


def test_value_surface_v2_controlled_review_decision_enum(
    tmp_path: Path,
    value_surface_v2_full_inputs: dict[str, Path],
) -> None:
    paths = value_surface_v2_full_inputs
    holdout = run_regime_conditioned_walk_forward_holdout(
        value_surface_expansion_path=paths["value_expansion"],
        failure_attribution_path=paths["failure"],
        horizon_stabilization_path=paths["horizon"],
        design_path=paths["design"],
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        output_root=tmp_path / "v2_review",
    )
    payload = run_value_surface_v2_controlled_review(
        cost_turnover_path=paths["cost_turnover"],
        horizon_quarantine_path=paths["long_horizon"],
        regime_attribution_path=paths["ai_regime"],
        holdout_path=Path(holdout["artifact_paths"]["json_path"]),
        output_root=tmp_path / "v2_review",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_v2_controlled_review"
    assert payload["summary"]["value_surface_v2_decision"] in {
        "CONTINUE_TO_LARGER_CONTROLLED_RESEARCH",
        "WATCHLIST",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_HORIZON_SELECTOR",
        "KILL_VALUE_SURFACE",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_horizon_selector_problem_contract(
    tmp_path: Path,
    horizon_selector_review_inputs: dict[str, Path],
) -> None:
    paths = horizon_selector_review_inputs
    payload = run_horizon_selector_problem_contract(
        v2_review_path=paths["v2_review"],
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_contract_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_problem_contract"
    assert {row["horizon"] for row in payload["candidate_horizons"]} == {
        "1d",
        "5d",
        "10d",
        "20d",
        "60d",
    }
    assert payload["target_horizon_is_holding_commitment"] is False
    assert payload["selector_output"]["review_interval"] == "daily"
    assert "invalidation_condition" in {row["field"] for row in payload["selector_output_schema"]}


def test_long_horizon_quarantine_fallback_review(
    tmp_path: Path,
    horizon_selector_review_inputs: dict[str, Path],
) -> None:
    paths = horizon_selector_review_inputs
    contract = run_horizon_selector_problem_contract(
        v2_review_path=paths["v2_review"],
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_fallback_check",
    )
    payload = run_long_horizon_quarantine_fallback_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=Path(contract["artifact_paths"]["json_path"]),
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "horizon_fallback_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "long_horizon_quarantine_fallback_review"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "disable_60d" in variant_ids
    assert "disable_20d_60d" in variant_ids
    assert "long_horizon_fallback_to_5d_10d" in variant_ids
    assert "holdout_pass_rate" in payload["variant_metrics"][0]
    assert payload["review_summary"]["promotion_gate_allowed"] is False


def test_horizon_selector_controlled_prototype(
    tmp_path: Path,
    horizon_selector_inputs: dict[str, Path],
) -> None:
    paths = horizon_selector_inputs
    payload = run_horizon_selector_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_prototype_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_controlled_prototype"
    assert payload["summary"]["model_run_executed"] is False
    assert payload["horizon_decision_by_date"]
    assert payload["prototype_summary"]["fallback_count"] >= 0
    assert payload["selector_metric"]["promotion_gate_allowed"] is False


def test_cost_aware_horizon_hysteresis(
    tmp_path: Path,
    horizon_selector_inputs: dict[str, Path],
) -> None:
    paths = horizon_selector_inputs
    prototype = run_horizon_selector_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_hysteresis_check",
    )
    payload = run_cost_aware_horizon_hysteresis(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        prototype_path=Path(prototype["artifact_paths"]["json_path"]),
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_hysteresis_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "cost_aware_horizon_hysteresis"
    assert "horizon_switch_count" in payload["summary"]
    assert "utility_lost_to_hysteresis" in payload["summary"]
    assert payload["hysteresis_metric"]["promotion_gate_allowed"] is False


def test_horizon_selector_holdout_review_decision_enum(
    tmp_path: Path,
    horizon_selector_full_inputs: dict[str, Path],
) -> None:
    paths = horizon_selector_full_inputs
    payload = run_horizon_selector_holdout_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        prototype_path=paths["prototype"],
        hysteresis_path=paths["hysteresis"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "horizon_holdout_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_selector_holdout_review"
    assert payload["summary"]["horizon_selector_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "PIVOT_TO_TAIL_RISK_POLICY",
        "PIVOT_TO_BENCHMARK_SELECTOR",
        "KILL_VALUE_SURFACE_AS_ACTION_POLICY",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False
