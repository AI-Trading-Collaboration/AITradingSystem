from __future__ import annotations

from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    Path,
    _assert_safety,
    _read_json,
    _run_direction_review_inputs,
    _run_next_stage_inputs,
    _write_forward_ledger,
    _write_json,
    _write_price_caches,
    run_forward_evidence_continuity_extension,
    run_forward_evidence_daily_continuity_maturity_tracker,
    run_forward_evidence_daily_continuity_review,
    run_forward_evidence_maturity_tracker,
    run_gbdt_residual_hypothesis_triage,
    run_gbdt_value_surface_residual_diagnostic_prototype,
    run_horizon_cliff_utility_ranking_stabilization_review,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_activation_recheck,
    run_utility_boundary_ranking_policy_audit,
    run_utility_ranking_robustness_pareto_audit,
    run_value_surface_controlled_expansion,
    run_value_surface_controlled_prototype,
    run_value_surface_controlled_walk_forward_expansion,
    run_value_surface_direction_review,
    run_value_surface_failure_attribution,
    run_value_surface_utility_pareto_ranking_review,
    run_value_surface_warning_triage_review,
)


def test_value_surface_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_prototype"
    assert payload["summary"]["value_surface_generated"] is True
    assert payload["summary"]["candidate_action_count"] >= payload["summary"]["configured_minimum"]
    assert payload["summary"]["horizon_count"] >= payload["summary"]["horizon_configured_minimum"]
    assert payload["summary"]["sample_quality_report_present"] is True
    assert (tmp_path / "value_surface" / "value_surface_controlled_prototype.json").exists()
    assert (tmp_path / "value_surface" / "value_surface_horizon_audit.json").exists()
    assert (tmp_path / "value_surface" / "value_surface_benchmark_comparison.json").exists()


def test_value_surface_no_future_horizon_selection(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )
    audit = _read_json(tmp_path / "value_surface" / "value_surface_horizon_audit.json")

    assert audit["summary"]["horizon_leakage_check_pass"] is True
    assert audit["summary"]["future_outcome_used_for_horizon_selection"] is False
    assert audit["future_outcome_policy"]["strategy_input_allowed"] is False


def test_value_surface_control_failure_blocks_recommendation(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    control_audit = _write_json(
        tmp_path / "control_audit_failed.json",
        {
            "summary": {
                "negative_control_promotion_count": 1,
                "future_leakage_trap_blocked": False,
            }
        },
    )
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        control_audit_path=control_audit,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    assert payload["status"] == "CONTROL_FAILED"
    assert payload["summary"]["value_surface_status"] == "CONTROL_FAILED"
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_value_surface_benchmark_comparison_required(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    assert payload["summary"]["benchmark_comparison_present"] is True
    assert payload["benchmark_comparison"]
    assert payload["summary"]["future_leakage_trap_blocked"] is True


def test_value_surface_promotion_false(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_surface",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_value_surface_controlled_expansion_schema(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    payload = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_expansion"
    assert payload["summary"]["value_surface_expansion_generated"] is True
    assert payload["summary"]["action_horizon_surface_present"] is True
    assert payload["summary"]["horizon_smoothness_audit_present"] is True
    assert payload["summary"]["horizon_leakage_check_pass"] is True
    assert payload["summary"]["by_cluster_breakdown_present"] is True
    assert payload["summary"]["gross_net_turnover_drawdown_present"] is True
    assert (tmp_path / "value_expansion" / "value_surface_controlled_expansion.json").exists()
    assert (
        tmp_path / "value_expansion" / "value_surface_expansion_horizon_smoothness_audit.json"
    ).exists()


def test_utility_boundary_audit_status_cap(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    payload = run_utility_boundary_ranking_policy_audit(
        value_surface_expansion_path=(
            tmp_path / "value_expansion" / "value_surface_controlled_expansion.json"
        ),
        output_root=tmp_path / "utility",
    )

    _assert_safety(payload)
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert payload["summary"]["profile_reversal_report_present"] is True
    assert payload["summary"]["pareto_frontier_present"] is True


def test_forward_evidence_maturity_tracker_append_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    ledger_path = _write_forward_ledger(tmp_path)
    payload = run_forward_evidence_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "maturity",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_maturity_tracker"
    assert payload["summary"]["forward_maturity_tracker_generated"] is True
    assert payload["summary"]["future_outcomes_appended_only"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert {row["horizon"] for row in payload["horizon_maturity_summary"]} >= {
        "1d",
        "5d",
        "10d",
        "20d",
        "60d",
    }


def test_value_surface_warning_triage_decision_enum(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "warning_triage",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_warning_triage_review"
    assert payload["summary"]["warning_taxonomy_present"] is True
    assert payload["summary"]["decision_date_breakdown_present"] is True
    assert payload["summary"]["sample_concentration_present"] is True
    assert payload["summary"]["controlled_expansion_review_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "DATA_REQUIRED",
        "PAUSE",
        "KILL",
    }


def test_utility_ranking_robustness_remains_sensitivity_tested(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_utility_ranking_robustness_pareto_audit(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "utility_robustness",
    )

    _assert_safety(payload)
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["pareto_frontier_stability_present"] is True
    assert payload["diagnostic_boundary_assessment"]["boundary_use"] == "diagnostic_only"


def test_forward_evidence_daily_continuity_tracker(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    prices_path, marketstack_path, rates_path = (
        paths["prices"],
        paths["marketstack"],
        paths["rates"],
    )
    payload = run_forward_evidence_daily_continuity_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=paths["ledger"],
        forward_maturity_path=paths["maturity"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_daily_continuity_maturity_tracker"
    assert payload["summary"]["daily_continuity_checked"] is True
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert payload["summary"]["output_coverage_present"] is True


def test_value_surface_controlled_walk_forward_expansion(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    warning = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "warning_triage",
    )
    payload = run_value_surface_controlled_walk_forward_expansion(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        warning_triage_path=Path(warning["artifact_paths"]["json_path"]),
        output_root=tmp_path / "walk_forward",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_controlled_walk_forward_expansion"
    assert payload["summary"]["walk_forward_window_count"] >= 1
    assert payload["summary"]["decision_date_count"] >= 1
    assert payload["summary"]["benchmark_comparison_present"] is True
    assert payload["summary"]["negative_control_promotion_count"] == 0
    assert payload["summary"]["future_leakage_trap_blocked"] is True
    assert payload["summary"]["controlled_walk_forward_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "DATA_REQUIRED",
        "PAUSE",
        "KILL",
    }


def test_value_surface_utility_pareto_ranking_review(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_value_surface_utility_pareto_ranking_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "utility_pareto",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_utility_pareto_ranking_review"
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["utility_profile_count"] >= 1
    assert payload["summary"]["pareto_candidate_count"] >= 1
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert "horizon_cliff_count" in payload["summary"]


def test_forward_evidence_daily_continuity_review(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_forward_evidence_daily_continuity_review(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity_review",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_daily_continuity_review"
    assert payload["summary"]["ledger_event_count"] == 1
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["summary"]["horizon_maturity_recorded"] is True
    assert payload["summary"]["output_coverage_present"] is True


def test_gbdt_value_surface_residual_diagnostic_no_strategy(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    payload = run_gbdt_value_surface_residual_diagnostic_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "gbdt_residual",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_value_surface_residual_diagnostic_prototype"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["feature_importance_present"] is True
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["diagnostic_boundary"]["direct_action_utility_prediction"] is False


def test_regret_casebook_activation_recheck_stays_watchlist(tmp_path: Path) -> None:
    paths = _run_next_stage_inputs(tmp_path)
    activation = run_regret_activation_inputs_from_value_surface_failures(
        value_surface_expansion_path=paths["value_expansion"],
        regret_casebook_expansion_gate_path=tmp_path / "missing_regret_gate.json",
        output_root=tmp_path / "regret",
    )
    payload = run_regret_casebook_activation_recheck(
        value_surface_expansion_path=paths["value_expansion"],
        regret_activation_inputs_path=Path(activation["artifact_paths"]["json_path"]),
        regret_casebook_expansion_gate_path=tmp_path / "missing_regret_gate.json",
        output_root=tmp_path / "regret",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "regret_casebook_activation_recheck"
    assert payload["status"] == "WATCHLIST_NOT_READY"
    assert payload["summary"]["regret_casebook_expansion_allowed"] is False
    assert payload["activation_recheck_decision"]["expansion_executed"] is False


def test_value_surface_failure_attribution_explains_loss_delta(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "failure_attribution",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_failure_attribution"
    assert "winning_case_average_delta" in payload["summary"]
    assert "losing_case_average_delta" in payload["summary"]
    assert payload["top_losing_cases"]
    assert payload["tail_loss_contribution"]["tail_loss_share"] >= 0
    assert payload["benchmark_relative_downside_attribution"]["promotion_gate_allowed"] is False


def test_horizon_cliff_stabilization_review_diagnostic_only(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "stabilization",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "horizon_cliff_utility_ranking_stabilization_review"
    assert payload["status"] == "SENSITIVITY_TESTED"
    assert payload["summary"]["validated_boundary_count"] == 0
    assert payload["summary"]["not_validated_utility_boundary"] is True
    assert "ranking_jump_count" in payload["summary"]


def test_gbdt_residual_hypothesis_triage_no_strategy(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "residual_triage",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "gbdt_residual_hypothesis_triage"
    assert payload["summary"]["residual_case_count"] > 0
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["repair_rule_candidates"]
    assert payload["diagnostic_boundary"]["direct_action_policy_generated"] is False


def test_forward_evidence_continuity_extension(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    payload = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "continuity_extension",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "forward_evidence_continuity_extension"
    assert payload["summary"]["ledger_event_count"] == 1
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["forward_evidence_scope"]["paper_shadow_ready"] is False


def test_value_surface_direction_review_does_not_default_continue(tmp_path: Path) -> None:
    paths = _run_direction_review_inputs(tmp_path)
    failure = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "direction",
        as_of_date=TEST_AS_OF,
    )
    horizon = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "direction",
    )
    residual = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "direction",
    )
    forward = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "direction",
        as_of_date=TEST_AS_OF,
    )
    payload = run_value_surface_direction_review(
        failure_attribution_path=Path(failure["artifact_paths"]["json_path"]),
        horizon_stabilization_path=Path(horizon["artifact_paths"]["json_path"]),
        residual_triage_path=Path(residual["artifact_paths"]["json_path"]),
        forward_continuity_extension_path=Path(forward["artifact_paths"]["json_path"]),
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "direction",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_direction_review"
    assert payload["summary"]["do_not_default_continue"] is True
    assert payload["summary"]["direction_decision"] in {
        "CONTINUE_LOCAL_FIX",
        "WATCHLIST",
        "PIVOT_TO_REGIME_CONDITIONED_VALUE_SURFACE",
        "PIVOT_TO_PARETO_FRONTIER_POLICY",
        "PIVOT_TO_TAIL_RISK_FILTER",
        "KILL_CURRENT_VALUE_SURFACE_VERSION",
    }
