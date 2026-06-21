from __future__ import annotations

from controlled_strategy_batch_helpers import (
    TEST_AS_OF,
    Path,
    _assert_safety,
    _run_tail_risk_policy_full_inputs,
    _run_tail_risk_policy_inputs,
    _run_tail_risk_review_board_inputs,
    _run_tail_risk_robustness_inputs,
    run_benchmark_fallback_drawdown_guard_controlled_prototype,
    run_benchmark_first_tail_risk_policy_contract,
    run_conservative_horizon_risk_filter,
    run_tail_loss_avoidance_classifier_prototype,
    run_tail_risk_benchmark_fallback_robustness_expansion,
    run_tail_risk_fallback_trigger_precision_recall_audit,
    run_tail_risk_forward_evidence_integration,
    run_tail_risk_opportunity_cost_upside_capture_review,
    run_tail_risk_policy_controlled_review_board,
    run_tail_risk_policy_family_controlled_review,
    run_value_surface_policy_kill_diagnostic_downgrade,
)


def test_value_surface_policy_kill_diagnostic_downgrade(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_value_surface_policy_kill_diagnostic_downgrade(
        horizon_selector_holdout_path=paths["horizon_holdout"],
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "tail_policy_kill_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "value_surface_policy_kill_diagnostic_downgrade"
    assert payload["summary"]["action_policy_allowed"] is False
    assert payload["summary"]["promotion_gate_allowed"] is False
    assert payload["policy_downgrade"]["allowed_uses"] == [
        "diagnostic",
        "residual_analysis",
        "tail_loss_attribution",
        "horizon_risk_signal",
        "benchmark_fallback_trigger",
    ]
    assert "direct_action_policy" in payload["policy_downgrade"]["disallowed_uses"]


def test_benchmark_first_tail_risk_policy_contract(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_benchmark_first_tail_risk_policy_contract(
        policy_kill_path=paths["policy_kill"],
        output_root=tmp_path / "tail_contract_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "benchmark_first_tail_risk_policy_contract"
    contract = payload["policy_contract"]
    assert contract["base_policy"] == "benchmark_or_simple_trend_static_allocation"
    assert "risk_downshift" in contract["allowed_deviation"]
    assert contract["fallback_policy"] == "benchmark_first"
    assert contract["review_interval"] == "daily"
    assert contract["direct_position_policy"] is False


def test_tail_loss_avoidance_classifier_prototype_is_gate_only(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_tail_loss_avoidance_classifier_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        policy_kill_path=paths["policy_kill"],
        output_root=tmp_path / "tail_classifier_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_loss_avoidance_classifier_prototype"
    assert payload["summary"]["strategy_signal_generated"] is False
    assert payload["gate_semantics"]["direct_position_policy"] is False
    assert payload["summary"]["large_loss_case_count"] >= 0
    assert payload["summary"]["tail_loss_case_count"] >= 0
    assert payload["summary"]["benchmark_underperformance_case_count"] >= 0
    assert payload["summary"]["long_horizon_failure_case_count"] >= 0


def test_conservative_horizon_risk_filter_status(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    payload = run_conservative_horizon_risk_filter(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        contract_path=paths["contract"],
        output_root=tmp_path / "horizon_filter_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "conservative_horizon_risk_filter"
    status = {row["horizon"]: row["status"] for row in payload["horizon_status"]}
    assert status["1d"] == "ALLOWED"
    assert status["5d"] == "ALLOWED"
    assert status["10d"] == "ALLOWED"
    assert status["20d"] == "QUARANTINED"
    assert status["60d"] == "FALLBACK_ONLY"
    assert payload["selector_mode"] == "risk_filter_not_optimal_horizon_selector"


def test_benchmark_fallback_drawdown_guard_controlled_prototype(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    payload = run_benchmark_fallback_drawdown_guard_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        horizon_filter_path=paths["horizon_filter"],
        contract_path=paths["contract"],
        output_root=tmp_path / "fallback_guard_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "benchmark_fallback_drawdown_guard_controlled_prototype"
    variant_ids = {row["variant_id"] for row in payload["variant_metrics"]}
    assert "benchmark_first_baseline" in variant_ids
    assert "tail_risk_benchmark_fallback" in variant_ids
    assert "drawdown_guard_cash_fallback" in variant_ids
    assert "mean_delta_vs_benchmark" in payload["variant_metrics"][0]
    assert "tail_loss_contribution" in payload["variant_metrics"][0]
    assert "max_drawdown" in payload["variant_metrics"][0]
    assert "beat_rate_retention" in payload["variant_metrics"][0]


def test_tail_risk_policy_family_controlled_review_decision_enum(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    payload = run_tail_risk_policy_family_controlled_review(
        policy_kill_path=paths["policy_kill"],
        contract_path=paths["contract"],
        classifier_path=paths["classifier"],
        horizon_filter_path=paths["horizon_filter"],
        fallback_path=paths["fallback"],
        output_root=tmp_path / "tail_family_review_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_policy_family_controlled_review"
    assert payload["summary"]["tail_risk_policy_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "KILL",
        "PIVOT",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False


def test_tail_risk_benchmark_fallback_robustness_expansion(tmp_path: Path) -> None:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    payload = run_tail_risk_benchmark_fallback_robustness_expansion(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        fallback_path=paths["fallback"],
        output_root=tmp_path / "tail_robustness_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_benchmark_fallback_robustness_expansion"
    assert payload["summary"]["robustness_decision"] in {
        "CONTINUE",
        "WATCHLIST",
        "KILL",
        "DATA_REQUIRED",
    }
    assert "fallback_trigger_count" in payload["summary"]
    assert "upside_capture" in payload["summary"]
    assert payload["by_asset"]
    assert payload["by_horizon"]
    assert payload["by_regime"]
    assert payload["by_cluster"]


def test_tail_risk_fallback_trigger_precision_recall_audit(tmp_path: Path) -> None:
    paths = _run_tail_risk_robustness_inputs(tmp_path)
    payload = run_tail_risk_fallback_trigger_precision_recall_audit(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_precision_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_fallback_trigger_precision_recall_audit"
    assert "fallback_precision" in payload["summary"]
    assert "fallback_recall" in payload["summary"]
    assert "false_positive" in payload["confusion_matrix"]
    assert "false_negative" in payload["confusion_matrix"]
    assert "tail_loss_from_false_negative" in payload["summary"]


def test_tail_risk_opportunity_cost_upside_capture_review(tmp_path: Path) -> None:
    paths = _run_tail_risk_robustness_inputs(tmp_path)
    payload = run_tail_risk_opportunity_cost_upside_capture_review(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_opportunity_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_opportunity_cost_upside_capture_review"
    assert "benchmark_upside_case_count" in payload["summary"]
    assert "upside_capture_ratio" in payload["summary"]
    assert "missed_upside_cost" in payload["summary"]
    assert "opportunity_cost_condition_met" in payload["summary"]
    assert (
        payload["missed_upside_concentration"]["by_regime"]["summary"]["promotion_gate_allowed"]
        is False
    )


def test_tail_risk_forward_evidence_integration_pending_only(tmp_path: Path) -> None:
    paths = _run_tail_risk_robustness_inputs(tmp_path)
    payload = run_tail_risk_forward_evidence_integration(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        ledger_path=tmp_path / "tail_forward" / "ledger.jsonl",
        output_root=tmp_path / "tail_forward",
        as_of_date=TEST_AS_OF,
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_forward_evidence_integration"
    assert payload["summary"]["future_outcome_status"] == "pending_maturity"
    assert payload["summary"]["append_only_integrity_pass"] is True
    assert payload["forward_records"]
    assert (
        payload["forward_records"][0]["actual_future_outcome_after_maturity"]["status"]
        == "pending_maturity"
    )
    assert (
        payload["forward_records"][0]["benchmark_output"]["realized_future_return_included"]
        is False
    )


def test_tail_risk_policy_controlled_review_board_decision_enum(tmp_path: Path) -> None:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    payload = run_tail_risk_policy_controlled_review_board(
        robustness_path=paths["robustness"],
        precision_recall_path=paths["precision"],
        opportunity_cost_path=paths["opportunity"],
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "tail_board_check",
    )

    _assert_safety(payload)
    assert payload["report_type"] == "tail_risk_policy_controlled_review_board"
    assert payload["summary"]["tail_risk_controlled_decision"] in {
        "CONTROLLED_RESEARCH_CONTINUE",
        "WATCHLIST_FORWARD_MATURITY",
        "WATCHLIST",
        "PIVOT_OVERCONSERVATIVE",
        "KILL",
        "DATA_REQUIRED",
    }
    assert payload["review_decision"]["promotion_gate_allowed"] is False
    assert payload["review_decision"]["paper_shadow_change_allowed"] is False
