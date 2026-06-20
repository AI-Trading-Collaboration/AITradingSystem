from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.indicator_research import (
    _matrix_based_horizon_recommendation,
    build_backtest_trace_bridge,
    build_component_level_historical_trace,
    build_daily_indicator_coverage_gap_report,
    build_daily_indicator_inventory,
    build_daily_indicator_weight_trace,
    build_dependency_graph,
    build_gate_availability_audit,
    build_historical_multi_stage_weight_trace_validation,
    build_indicator_research_gate,
    build_indicator_research_validation_rollup,
    build_lineage_manifest_repair_report,
    build_long_horizon_evidence_floor_calibration_audit,
    build_mapping_plan,
    build_masking_audit,
    build_masking_casebook,
    build_threshold_registry_audit,
    build_valuation_crowding_ablation_validation,
    build_valuation_crowding_masking_effectiveness_review,
    build_valuation_crowding_masking_robustness_review,
    build_valuation_crowding_outcome_availability_audit,
    build_valuation_crowding_pilot_audit,
    build_valuation_crowding_pilot_validation_report,
    load_indicator_registry,
    write_daily_indicator_weight_trace,
    write_indicator_framework_validation_pack,
    write_indicator_validation_pack_stability_report,
)
from ai_trading_system.research_campaign import validate_stage_adapter_contracts


def test_daily_indicator_inventory_flags_valuation_crowding_high_impact() -> None:
    payload = build_daily_indicator_inventory()

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["market_regime"] == "ai_after_chatgpt"
    assert payload["safety_boundary"]["official_target_weights"] is False

    records = {item["indicator_id"]: item for item in payload["inventory"]}
    valuation = records["valuation_crowding_indicator"]
    assert valuation["used_in_daily_report"] is True
    assert valuation["affects_signal"] is True
    assert valuation["affects_weight"] is True
    assert valuation["constraint_type"] == "RESEARCHABLE_STRATEGY_CONSTRAINT"
    assert valuation["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
    assert payload["reader_brief"]["valuation_crowding_status"] == "HIGH_IMPACT_UNVALIDATED"


def test_data_quality_gate_indicator_mapping_spec_is_complete() -> None:
    registry = load_indicator_registry()
    indicators = {item.indicator_id: item for item in registry.indicators}
    mappings = {item.indicator_id: item for item in registry.mappings}

    indicator = indicators["data_quality_gate_indicator"]
    mapping = mappings["data_quality_gate_indicator"]

    assert indicator.role == "IMMUTABLE_SAFETY_CONSTRAINT"
    assert indicator.constraint_type == "IMMUTABLE_SAFETY_CONSTRAINT"
    assert indicator.research_status == "RESEARCHED"
    assert mapping.mapping_version == "data_quality_gate_v1"
    assert mapping.family == "M5_HARD_CAP"
    assert "hard block" in mapping.output_range
    assert mapping.status == "immutable_safety_gate"
    assert mapping.review_requirement is not None
    assert "not a tunable strategy heuristic" in mapping.review_requirement


def test_dependency_graph_keeps_valuation_masking_edge_visible() -> None:
    payload = build_dependency_graph()

    edge_keys = {
        (edge["from_node"], edge["to_node"], edge["edge_type"])
        for edge in payload["graph"]["edges"]
    }
    assert (
        "valuation_crowding_indicator",
        "trend_strength_indicator",
        "MASKS",
    ) in edge_keys
    assert payload["circular_dependency_audit"]["status"] == "PASS"
    dominance = {
        item["indicator_id"]: item["dominance_status"] for item in payload["dominance_audit"]
    }
    assert (
        dominance["valuation_crowding_indicator"] == "DOMINANT_WEIGHT_DRIVER_EXPECTED_UNVALIDATED"
    )


def test_masking_audit_computes_ratio_from_multi_stage_trace(tmp_path: Path) -> None:
    trace_path = _write_trace(tmp_path)

    payload = build_masking_audit(
        indicator_id="trend_strength_indicator",
        trace_path=trace_path,
    )
    result = payload["masking_results"][0]

    assert payload["status"] == "PASS"
    assert result["upstream_indicator_id"] == "valuation_crowding_indicator"
    assert result["downstream_indicator_id"] == "trend_strength_indicator"
    assert result["masking_status"] == "HIGH_MASKING"
    assert result["conclusion_status"] == "B_EFFECT_MASKED_BY_A"
    assert round(result["masking_ratio"], 3) == 0.767


def test_daily_indicator_weight_trace_covers_required_stages_and_masking(
    tmp_path: Path,
) -> None:
    payload = build_daily_indicator_weight_trace(_fake_daily_score_report())
    trace_path = write_daily_indicator_weight_trace(
        payload,
        tmp_path / "daily_indicator_weight_trace.json",
    )

    assert payload["status"] == "PASS"
    assert payload["summary"]["component_row_count"] >= 2
    assert payload["summary"]["constraint_row_count"] >= 1
    assert payload["summary"]["missing_trace_field_record_count"] == 0

    trend_row = next(
        row for row in payload["rows"] if row["module_id"] == "trend_strength_indicator"
    )
    assert trend_row["raw_indicator_value"]
    assert trend_row["normalized_indicator_score"] == 80.0
    assert trend_row["mapped_signal_contribution"] > 0
    assert trend_row["pre_constraint_signal_weight"] == 0.80
    assert trend_row["post_constraint_signal_weight"] == 0.65
    assert trend_row["final_advisory_portfolio_facing_weight"] == 0.325

    masking = build_masking_audit(
        indicator_id="trend_strength_indicator",
        trace_path=trace_path,
    )
    result = masking["masking_results"][0]
    assert masking["summary"]["trace_required_pair_count"] == 0
    assert result["masking_status"] == "HIGH_MASKING"
    assert result["conclusion_status"] == "B_EFFECT_MASKED_BY_A"


def test_daily_indicator_coverage_gap_report_lists_required_buckets(
    tmp_path: Path,
) -> None:
    trace = build_daily_indicator_weight_trace(_fake_daily_score_report())
    trace_path = write_daily_indicator_weight_trace(
        trace,
        tmp_path / "daily_indicator_weight_trace.json",
    )

    payload = build_daily_indicator_coverage_gap_report(trace_path=trace_path)

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["registered_indicators"]
    assert isinstance(payload["unregistered_daily_indicators"], list)
    assert payload["registered_incomplete_indicators"]
    assert payload["high_impact_unvalidated"]
    assert {
        "source",
        "constraint",
        "mapping",
        "dependency",
        "trace_contract",
    } >= set().union(
        *[set(item["missing_fields"]) for item in payload["registered_incomplete_indicators"]]
    )
    incomplete_by_id = {
        item["indicator_id"]: item["missing_fields"]
        for item in payload["registered_incomplete_indicators"]
    }
    assert "mapping" not in incomplete_by_id.get("data_quality_gate_indicator", [])


def test_indicator_gate_conditions_downstream_signal_on_upstream_constraint(
    tmp_path: Path,
) -> None:
    trace_path = _write_trace(tmp_path)

    trend_gate = build_indicator_research_gate(
        indicator_id="trend_strength_indicator",
        trace_path=trace_path,
    )
    valuation_gate = build_indicator_research_gate(indicator_id="valuation_crowding_indicator")

    assert trend_gate["status"] == "EFFECT_MASKED_BY_UPSTREAM_CONSTRAINT"
    assert trend_gate["gate"]["allowed_for_weight_increase"] is False
    assert valuation_gate["status"] == "OWNER_REVIEW_REQUIRED"
    assert valuation_gate["gate"]["official_target_weights"] is False


def test_mapping_plan_does_not_default_high_impact_constraint_to_hard_cap() -> None:
    payload = build_mapping_plan(indicator_id="valuation_crowding_indicator")

    assert payload["summary"]["current_mapping_family"] == "M5_HARD_CAP"
    cards = {item["mapping_family"]: item for item in payload["hypothesis_cards"]}
    assert cards["M5_HARD_CAP"]["allowed_for_current_stage"] is False
    assert "MASKING_AND_DOMINANCE_AUDIT_PASS" in cards["M5_HARD_CAP"]["requires_before_backfill"]


def test_valuation_crowding_pilot_is_untraced_high_impact_by_default() -> None:
    payload = build_valuation_crowding_pilot_audit()

    assert payload["status"] == "VALUATION_CROWDING_UNTESTED_HIGH_IMPACT"
    assert payload["summary"]["parameter_mutation"] is False
    assert payload["inventory_record"]["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
    assert payload["masking_results"][0]["masking_status"] == "TRACE_DATA_REQUIRED"


def test_valuation_crowding_pilot_validation_explains_high_impact_masking(
    tmp_path: Path,
) -> None:
    trace = build_daily_indicator_weight_trace(_fake_daily_score_report())
    trace_path = write_daily_indicator_weight_trace(
        trace,
        tmp_path / "daily_indicator_weight_trace.json",
    )

    payload = build_valuation_crowding_pilot_validation_report(trace_path=trace_path)

    assert payload["status"] == "VALUATION_CROWDING_RESEARCH_COVERAGE_KNOWN"
    assert payload["mapping_to_signal"]["direction"] == "higher_valuation_or_crowding_is_bearish"
    assert (
        payload["effective_thresholds"]["position_gate_thresholds"][
            "expensive_or_crowded_max_position"
        ]
        == 0.70
    )
    assert payload["high_impact_unvalidated_reason"]["coverage_status"] == (
        "HIGH_IMPACT_UNVALIDATED"
    )
    assert payload["high_masking_reason"]["masking_status"] == "HIGH_MASKING"
    assert payload["trend_interaction"]["dependency_edge_type"] == "MASKS"
    assert payload["no_parameter_mutation"] is True


def test_masking_casebook_artifact_generation(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_short_outcome_prices(tmp_path)

    payload = build_masking_casebook(
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker="QQQ",
    )

    assert payload["status"] == "PASS"
    assert payload["summary"]["case_count"] == 2
    case = payload["casebook"][0]
    assert case["trend_raw_direction"] in {
        "trend_positive",
        "trend_positive_allocation_intent",
    }
    assert case["valuation_crowding_raw_direction"] == "valuation_crowding_risk_off"
    assert case["pre_mask_signal"] > case["post_mask_signal"]
    assert case["final_advisory_facing_weight"] > 0
    assert {"return_1d", "return_5d", "return_10d", "return_20d"} <= set(case["outcomes"])
    assert isinstance(case["drawdown_reduced"], bool)
    assert isinstance(case["missed_upside"], bool)
    assert isinstance(case["false_risk_off"], bool)
    assert case["promotion_gate_allowed"] is False
    assert case["allowed_uses"] == ["diagnostic", "ablation", "sensitivity_analysis"]


def test_masking_casebook_expands_asset_universe_sample_counts(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_short_outcome_prices(tmp_path)

    payload = build_masking_casebook(
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
    )

    assert payload["status"] == "PASS"
    assert payload["summary"]["date_count"] == 2
    assert payload["summary"]["asset_count"] == 5
    assert payload["summary"]["case_count"] == 10
    assert {case["asset"] for case in payload["casebook"]} == {
        "GOOGL",
        "MSFT",
        "QQQ",
        "SMH",
        "SPY",
    }


def test_valuation_crowding_ablation_result_schema(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)

    payload = build_valuation_crowding_ablation_validation(
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker="QQQ",
    )

    assert payload["status"] == "PASS"
    assert set(payload["scenarios"]) == {
        "baseline",
        "no_valuation_crowding_masking",
        "capped_masking",
    }
    required_fields = {
        "avg_return_1d",
        "avg_return_5d",
        "avg_return_10d",
        "avg_return_20d",
        "max_drawdown",
        "drawdown_preservation",
        "drawdown_reduced_count",
        "false_risk_off_count",
        "hit_rate_1d",
        "hit_rate_5d",
        "hit_rate_10d",
        "hit_rate_20d",
        "missed_upside_count",
        "sample_quality_breakdown",
        "turnover",
        "constraint_hit_count",
    }
    for scenario in payload["scenarios"].values():
        assert required_fields <= set(scenario)
    assert payload["summary"]["production_weight_logic_changed"] is False


def test_outcome_availability_audit_schema_and_join_key(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    bridge_root = _write_bridge_artifact_root(tmp_path)

    payload = build_valuation_crowding_outcome_availability_audit(
        trace_path=trace_path,
        prices_path=prices_path,
        bridge_artifact_root=bridge_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY",
    )

    assert payload["report_type"] == "valuation_crowding_outcome_availability_audit"
    assert payload["summary"]["total_cases"] > 0
    assert payload["summary"]["outcome_available_count"] > 0
    assert payload["summary"]["outcome_missing_count"] == 0
    assert payload["summary"]["1d_mature_case_count"] > 0
    assert payload["summary"]["5d_mature_case_count"] > 0
    assert payload["summary"]["20d_mature_case_count"] > 0
    assert "mature_case_count_by_horizon" in payload["mature_sample_quality"]
    assert "full_advisory_mature_count_by_horizon" in payload["mature_sample_quality"]
    assert set(payload["by_window"]) == {"10d", "1d", "20d", "5d"}
    assert payload["by_asset"]
    assert payload["by_date"]
    assert payload["summary"]["promotion_gate_allowed"] is False
    record = payload["records"][0]
    expected_key_fields = {
        "as_of_date",
        "decision_time",
        "asset",
        "scenario",
        "trace_source",
        "trace_contract_version",
    }
    assert expected_key_fields == set(payload["join_key_fields"])
    assert expected_key_fields <= set(record["outcome_join_key"])
    assert all(record["outcome_join_key"][field] for field in expected_key_fields)
    assert record["promotion_gate_allowed"] is False


def test_horizon_specific_outcome_maturity_and_cutoff(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_medium_outcome_prices(tmp_path)

    payload = build_valuation_crowding_outcome_availability_audit(
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker="QQQ",
    )

    assert payload["summary"]["1d_mature_case_count"] > 0
    assert payload["summary"]["5d_mature_case_count"] > 0
    assert payload["summary"]["10d_mature_case_count"] == 0
    assert payload["summary"]["20d_mature_case_count"] == 0
    assert payload["summary"]["10d_not_mature_count"] > 0
    assert payload["summary"]["20d_not_mature_count"] > 0
    assert payload["summary"]["outcome_missing_count"] == 0
    first = payload["records"][0]
    windows = {window["window"]: window for window in first["outcome_windows"]}
    assert windows["1d"]["evaluation_cutoff_met"] is True
    assert windows["5d"]["evaluation_cutoff_met"] is True
    assert windows["10d"]["evaluation_cutoff_met"] is False
    assert windows["20d"]["evaluation_cutoff_met"] is False
    assert windows["5d"]["target_date"] <= windows["5d"]["latest_available_price_date"]


def test_outcome_not_mature_is_not_hard_missing(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_short_outcome_prices(tmp_path)

    payload = build_valuation_crowding_outcome_availability_audit(
        trace_path=trace_path,
        prices_path=prices_path,
        outcome_ticker="QQQ",
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["outcome_not_mature_count"] > 0
    assert payload["summary"]["outcome_missing_count"] == 0
    assert payload["summary"]["missing_price_count"] == 0
    assert payload["by_window"]["20d"]["not_mature_count"] > 0


def test_twenty_day_not_mature_does_not_drop_short_horizon_review(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_medium_outcome_prices(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path)

    payload = build_valuation_crowding_masking_effectiveness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
    )

    by_horizon = {item["horizon"]: item for item in payload["by_horizon"]}
    assert by_horizon["1d"]["sample_quality"]["mature_case_count"] > 0
    assert by_horizon["5d"]["sample_quality"]["mature_case_count"] > 0
    assert by_horizon["20d"]["sample_quality"]["mature_case_count"] == 0
    assert by_horizon["1d"]["scenarios"]["baseline"]["avg_return"] is not None
    assert by_horizon["5d"]["scenarios"]["baseline"]["avg_return"] is not None
    assert by_horizon["20d"]["scenarios"]["baseline"]["avg_return"] is None
    assert payload["decision_recommendation"]["decision_recommendation"] == (
        "insufficient_evidence"
    )
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_effectiveness_review_requires_available_outcomes_for_recommendation(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_short_outcome_prices(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path)

    payload = build_valuation_crowding_masking_effectiveness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
    )

    assert payload["decision_recommendation"]["decision_recommendation"] == (
        "insufficient_evidence"
    )
    assert payload["decision_recommendation"]["promotion_gate_allowed"] is False
    assert payload["summary"]["promotion_gate_allowed"] is False
    assert payload["summary"]["outcome_not_mature_count"] > 0
    assert payload["outcome_availability_summary"]["outcome_missing_count"] == 0


def test_masking_effectiveness_review_outputs_required_layers_and_recommendation(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path)
    bridge_root = _write_bridge_artifact_root(tmp_path)

    payload = build_valuation_crowding_masking_effectiveness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        bridge_artifact_root=bridge_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-05",
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert set(payload["layers"]) == {
        "full_advisory_only",
        "component_only",
        "backtest_bridge",
    }
    full_quality = payload["layers"]["full_advisory_only"]["sample_quality"]
    assert full_quality["date_count"] == 2
    assert full_quality["asset_count"] == 5
    assert full_quality["case_count"] == 10
    assert full_quality["unique_regime_count"] == 1
    assert full_quality["correlated_asset_cluster_count"] == 3
    assert full_quality["outcome_available_count"] == 10
    assert full_quality["outcome_not_mature_count"] == 0
    assert payload["outcome_availability_summary"]["outcome_available_count"] > 0
    assert payload["layers"]["component_only"]["sample_quality"]["case_count"] == 0
    for layer in payload["layers"].values():
        for scenario in layer["scenarios"].values():
            assert {
                "avg_return_1d",
                "avg_return_5d",
                "avg_return_10d",
                "avg_return_20d",
                "hit_rate_1d",
                "hit_rate_5d",
                "hit_rate_10d",
                "hit_rate_20d",
                "max_drawdown",
                "drawdown_preservation",
                "missed_upside_count",
                "false_risk_off_count",
                "drawdown_reduced_count",
                "turnover",
                "constraint_hit_count",
            } <= set(scenario)
    assert payload["summary"]["decision_recommendation"] == "insufficient_evidence"
    assert payload["decision_recommendation"]["recommendation_scope"] == "validation_only"
    assert payload["decision_recommendation"]["promotion_gate_allowed"] is False
    assert payload["by_date"]
    assert payload["by_asset"]
    assert payload["by_regime"]
    assert payload["by_event_window"]


def test_horizon_effectiveness_conclusion_matrix_schema_and_long_horizon_flag(
    tmp_path: Path,
) -> None:
    trace_path = _write_many_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    gate_root = _write_many_gate_audit_root(tmp_path)

    payload = build_valuation_crowding_masking_effectiveness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-22",
    )

    assert payload["summary"]["promotion_gate_allowed"] is False
    assert payload["summary"]["decision_recommendation"] == ("preliminary_short_horizon_only")
    assert payload["decision_recommendation"]["promotion_gate_allowed"] is False
    matrix = payload["conclusion_matrix"]
    assert len(matrix) == 12
    rows = {(row["scenario_id"], row["horizon"]): row for row in matrix}
    baseline_1d = rows[("baseline", "1d")]
    required_fields = {
        "avg_return",
        "median_return",
        "hit_rate",
        "downside_capture",
        "max_drawdown",
        "drawdown_reduced_count",
        "missed_upside_count",
        "false_risk_off_count",
        "turnover",
        "constraint_hit_count",
        "sample_count",
        "full_advisory_sample_count",
        "component_only_sample_count",
        "backtest_bridge_sample_count",
        "mature_date_count",
        "mature_asset_count",
        "mature_case_count",
        "full_advisory_mature_case_count",
        "unique_regime_count",
        "correlated_asset_cluster_count",
        "sample_quality",
        "return_profile",
        "risk_profile",
        "recommendation_contribution",
    }
    assert required_fields <= set(baseline_1d)
    assert baseline_1d["sample_count"] == 200
    assert baseline_1d["full_advisory_sample_count"] == 100
    assert baseline_1d["component_only_sample_count"] == 0
    assert baseline_1d["backtest_bridge_sample_count"] == 100
    assert baseline_1d["mature_date_count"] == 20
    assert baseline_1d["mature_asset_count"] == 5
    assert baseline_1d["correlated_asset_cluster_count"] == 3
    assert rows[("baseline", "10d")]["full_advisory_sample_count"] >= 50
    assert payload["decision_recommendation"]["horizon_contributions"][10] == (
        "conflicting_horizon_signal"
    )
    twenty_day = rows[("baseline", "20d")]
    assert twenty_day["evidence_status"] == "insufficient_long_horizon_evidence"
    assert twenty_day["full_advisory_sample_count"] < 50
    assert payload["by_horizon"][-1]["insufficient_long_horizon_evidence"] is True
    assert payload["by_correlated_asset_cluster"]
    assert payload["layers"]["full_advisory_only"]["sample_quality"]["case_count"] == 100


def test_conflicting_horizon_contributions_remain_preliminary() -> None:
    result = _matrix_based_horizon_recommendation(
        [
            {
                "horizon_trading_days": 1,
                "recommendation_contribution": ("supports_prefer_capped_masking_candidate"),
            },
            {
                "horizon_trading_days": 5,
                "recommendation_contribution": ("supports_keep_baseline_masking_candidate"),
            },
            {
                "horizon_trading_days": 10,
                "recommendation_contribution": ("supports_prefer_capped_masking_candidate"),
            },
        ]
    )

    assert result["decision_recommendation"] == "preliminary_short_horizon_only"
    assert result["promotion_gate_allowed"] is False


def test_masking_robustness_review_delta_aggregation_and_gate_schema(
    tmp_path: Path,
) -> None:
    trace_path = _write_many_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    gate_root = _write_many_gate_audit_root(tmp_path)

    payload = build_valuation_crowding_masking_robustness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-22",
    )

    assert payload["report_type"] == "valuation_crowding_masking_robustness_review"
    assert payload["summary"]["promotion_gate_allowed"] is False
    assert payload["summary"]["production_weight_change_allowed"] is False
    assert payload["summary"]["paper_shadow_change_allowed"] is False
    assert payload["summary"]["scenario_delta_row_count"] == 12
    first_delta = payload["scenario_delta_matrix"][0]
    assert {
        "delta_avg_return",
        "delta_median_return",
        "delta_hit_rate",
        "delta_downside_capture",
        "delta_max_drawdown",
        "delta_missed_upside_count",
        "delta_false_risk_off_count",
        "delta_drawdown_reduced_count",
        "delta_turnover",
        "delta_constraint_hit_count",
    } <= set(first_delta)
    aggregation = payload["aggregation"]
    assert aggregation["equal_weight_by_date"]["group_count"] == 20
    assert aggregation["equal_weight_by_asset"]["group_count"] == 5
    assert aggregation["equal_weight_by_correlated_asset_cluster"]["group_count"] == 3
    assert aggregation["full_advisory_only"]["sample_quality"]["case_count"] == 100
    assert aggregation["all_validation_sources"]["horizon_results"]
    gate = payload["conservative_evidence_gate"]
    check_ids = {item["check_id"] for item in gate["checks"]}
    assert {
        "two_primary_horizons_consistent",
        "full_advisory_only_not_conflicting",
        "date_level_not_conflicting",
        "cluster_not_single_dominant",
        "missed_upside_false_risk_off_not_worse",
        "promotion_gate_allowed_false",
    } <= check_ids
    assert gate["final_validation_recommendation"] == ("keep_preliminary_short_horizon_only")
    assert payload["final_validation_recommendation"]["promotion_gate_allowed"] is False


def test_masking_robustness_review_support_attribution_and_maturity_tracker(
    tmp_path: Path,
) -> None:
    trace_path = _write_many_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    gate_root = _write_many_gate_audit_root(tmp_path)

    payload = build_valuation_crowding_masking_robustness_review(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-22",
    )

    attribution = payload["ten_day_baseline_support_attribution"]
    assert attribution["horizon"] == "10d"
    assert "wins_concentrated_in_few_dates" in attribution
    assert "semiconductor_ai_cluster_share" in attribution
    assert "component_or_bridge_driven" in attribution
    assert "full_advisory_only_still_holds" in attribution
    assert "single_extreme_event_driven" in attribution
    diagnostics = payload["case_diagnostics"]
    assert diagnostics["top_winning_cases"]
    assert diagnostics["by_asset"]
    assert diagnostics["by_regime"]
    assert diagnostics["by_event_window"]
    short = {item["horizon"]: item for item in payload["short_horizon_neutral_explanation"]}
    assert set(short) == {"1d", "5d"}
    assert "mature_sample_sufficient" in short["1d"]
    assert "scenario_difference_small" in short["1d"]
    assert "outcome_noise_high" in short["1d"]
    assert "short_horizon_whipsaw" in short["1d"]
    tracker = payload["pending_20d_maturity_tracker"]
    assert tracker["current_20d_mature_cases"] > 0
    assert tracker["pending_20d_cases"] > 0
    assert tracker["expected_maturity_dates"]
    assert tracker["by_asset"]
    assert tracker["by_date"]
    assert tracker["promotion_gate_allowed"] is False


def test_indicator_research_validation_rollup_schema_tracker_and_rerun_criteria(
    tmp_path: Path,
) -> None:
    trace_path = _write_many_casebook_trace(tmp_path, date_count=1)
    prices_path = _write_short_outcome_prices(tmp_path)
    gate_root = _write_many_gate_audit_root(tmp_path, date_count=1)

    payload = build_indicator_research_validation_rollup(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-03",
    )

    assert payload["report_type"] == "indicator_research_validation_rollup"
    assert payload["summary"]["framework_readiness"] == "READY_WITH_LIMITATIONS"
    assert payload["summary"]["outcome_maturity_status"] == (
        "SHORT_HORIZON_MATURE_LONG_HORIZON_PENDING"
    )
    assert payload["summary"]["valuation_crowding_final_validation_recommendation"] == (
        "keep_preliminary_short_horizon_only"
    )
    recommendation = payload["valuation_crowding_masking_current_recommendation"]
    assert recommendation["final_validation_recommendation"] == (
        "keep_preliminary_short_horizon_only"
    )
    assert recommendation["ten_day_conclusion"] == "supports_baseline_masking"
    assert recommendation["one_day_conclusion"] == "neutral_or_incomplete"
    assert recommendation["five_day_conclusion"] == "neutral_or_incomplete"
    assert recommendation["twenty_day_conclusion"] == ("insufficient_long_horizon_evidence")
    assert recommendation["promotion_gate_allowed"] is False
    assert recommendation["production_weight_change_allowed"] is False
    assert recommendation["paper_shadow_change_allowed"] is False

    indicator_status = payload["valuation_crowding_indicator_status"]
    assert indicator_status["coverage_status"] == "HIGH_IMPACT_UNVALIDATED"
    assert indicator_status["validation_status"] == "PRELIMINARY_SHORT_HORIZON_ONLY"
    assert indicator_status["promotion_status"] == "NO_PROMOTION_ALLOWED"

    tracker = payload["pending_maturity_tracker"]
    assert tracker["current_mature_cases_by_horizon"]["1d"] > 0
    assert tracker["current_mature_cases_by_horizon"]["20d"] == 0
    assert tracker["pending_20d_cases"] > 0
    assert tracker["expected_maturity_dates"]
    assert tracker["by_asset"]
    assert tracker["by_date"]
    assert tracker["by_cluster"]
    assert tracker["next_recommended_rerun_date"]
    assert tracker["promotion_gate_allowed"] is False

    criteria = payload["rerun_criteria"]
    criterion_ids = {item["criterion_id"] for item in criteria["criteria"]}
    assert {
        "20d_full_advisory_maturity_floor",
        "primary_horizon_consensus",
        "challenger_stable_advantage",
        "risk_flag_deterioration",
    } <= criterion_ids
    assert criteria["promotion_gate_allowed"] is False
    assert criteria["production_weight_change_allowed"] is False
    assert criteria["paper_shadow_change_allowed"] is False
    assert payload["remaining_limitations"]


def test_long_horizon_floor_calibration_audit_sensitivity_and_gate_schema(
    tmp_path: Path,
) -> None:
    trace_path = _write_many_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    gate_root = _write_many_gate_audit_root(tmp_path)

    payload = build_long_horizon_evidence_floor_calibration_audit(
        trace_path=trace_path,
        prices_path=prices_path,
        gate_audit_root=gate_root,
        outcome_ticker="QQQ",
        asset_universe="QQQ,SPY,SMH,MSFT,GOOGL",
        start_date="2023-01-03",
        end_date="2023-01-22",
    )

    assert payload["report_type"] == "long_horizon_evidence_floor_calibration_audit"
    floor = payload["floor_interpretation"]
    assert floor["floor_id"] == "heuristic_min_full_advisory_cases"
    assert floor["role"] == "conservative_guardrail"
    assert floor["calibration_status"] == "uncalibrated"
    assert floor["validated_statistical_threshold"] is False
    assert floor["promotion_gate_allowed"] is False

    sensitivity = payload["threshold_sensitivity"]
    assert sensitivity["floors"] == [20, 30, 50, 80, 100]
    rows = sensitivity["recommendation_by_floor"]
    assert {row["floor"] for row in rows} == {20, 30, 50, 80, 100}
    assert all("recommendation_by_floor" in row for row in rows)
    assert "first_floor_where_recommendation_stabilizes" in sensitivity
    assert sensitivity["twenty_day_conclusion_driver"] in {
        "sample_count_only",
        "robustness_failures",
        "sample_count_and_robustness_failures",
        "sample_count_and_robustness_passed",
    }

    sample = payload["effective_sample_size"]
    assert sample["raw_case_count"] > 0
    assert sample["unique_date_count"] > 0
    assert sample["unique_asset_count"] > 0
    assert sample["correlated_asset_cluster_count"] > 0
    assert sample["effective_date_count"] == sample["unique_date_count"]
    assert sample["effective_cluster_count"] == sample["correlated_asset_cluster_count"]

    gate = payload["robustness_based_gate"]
    assert {
        "leave_one_date_out_stable",
        "leave_one_asset_out_stable",
        "leave_one_cluster_out_stable",
        "full_advisory_only_all_sources_not_conflicting",
        "row_level_date_equal_weight_not_conflicting",
        "cluster_equal_weight_not_single_cluster_dominated",
    } <= set(gate)
    check_ids = {check["check_id"] for check in gate["checks"]}
    assert {
        "leave_one_date_out_stable",
        "leave_one_asset_out_stable",
        "leave_one_cluster_out_stable",
        "full_advisory_only_all_sources_not_conflicting",
        "row_level_date_equal_weight_not_conflicting",
        "cluster_equal_weight_not_single_cluster_dominated",
    } <= check_ids

    conclusion = payload["calibration_conclusion"]
    assert conclusion["calibration_conclusion"] in {
        "floor_50_retained_as_heuristic",
        "floor_50_adjusted_to_X",
        "replace_fixed_floor_with_evidence_bands",
        "insufficient_data_to_calibrate_floor",
    }
    assert conclusion["calibration_status"] == "uncalibrated"
    assert conclusion["promotion_gate_allowed"] is False
    assert conclusion["production_weight_change_allowed"] is False
    assert conclusion["paper_shadow_change_allowed"] is False
    assert payload["summary"]["promotion_gate_allowed"] is False


def test_threshold_registry_audit_summarizes_high_impact_defaults() -> None:
    payload = build_threshold_registry_audit()

    assert payload["report_type"] == "threshold_registry_audit"
    assert payload["status"] == "PASS_WITH_WARNINGS"
    summary = payload["summary"]
    assert summary["total_threshold_count"] >= 30
    assert summary["high_impact_threshold_count"] > 0
    assert summary["uncalibrated_high_impact_count"] == summary["high_impact_threshold_count"]
    assert summary["heuristic_guardrail_count"] > 0
    assert summary["calibrated_count"] == 0
    assert summary["thresholds_blocking_promotion_count"] == len(
        summary["thresholds_blocking_promotion"]
    )
    assert (
        "indicator_research.effectiveness_min_available_outcome_cases"
        in summary["thresholds_blocking_promotion"]
    )
    assert summary["production_weight_affecting_threshold_count"] == 0
    assert summary["production_weight_logic_changed"] is False
    assert summary["paper_shadow_change_allowed"] is False
    assert payload["calibration_backlog"]

    for threshold in payload["thresholds"]:
        if threshold["threshold_class"] == "A":
            assert threshold["calibration_status"] in {
                "UNCALIBRATED_DEFAULT",
                "HEURISTIC_GUARDRAIL",
            }
            assert threshold["calibration_required"] is True
            assert threshold["no_promotion_dependency_without_review"] is True
            assert threshold["production_weight_affecting"] is False


def test_historical_trace_validation_accepts_replay_style_trace(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)

    payload = build_historical_multi_stage_weight_trace_validation(trace_path=trace_path)

    assert payload["summary"]["date_count"] == 2
    assert payload["summary"]["masking_pair_result_count"] == 1
    assert payload["summary"]["missing_trace_field_record_count"] == 0
    assert payload["summary"]["production_equivalent_lineage_count"] == 2
    assert payload["historical_replay_lineage_manifest"]
    assert payload["date_level_summary"]
    assert payload["historical_masking_results"][0]["masking_status"] == "HIGH_MASKING"


def test_gate_availability_audit_splits_full_and_component_eligibility(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path)

    payload = build_gate_availability_audit(
        trace_path=trace_path,
        gate_audit_root=gate_root,
        start_date="2023-01-03",
        end_date="2023-01-05",
    )

    records = {item["date"]: item for item in payload["gate_availability"]}
    assert records["2023-01-03"]["full_advisory_trace_eligible"] is True
    fail_closed = records["2023-01-05"]
    assert fail_closed["full_advisory_trace_eligible"] is False
    assert fail_closed["component_validation_trace_eligible"] is True
    assert "feature_availability=FAIL" in fail_closed["reason_if_not_full_eligible"]
    assert fail_closed["blocked_gate"] == "feature_availability_gate"
    assert fail_closed["missing_or_late_feature"] == "sec_fundamentals_filing"
    assert fail_closed["decision_time"] == "2023-01-05"
    assert fail_closed["reason_class"] == "expected_pit_limitation"
    assert fail_closed["can_be_repaired_without_relaxing_production_gate"] is False
    assert fail_closed["trace_source"] == "component_level_validation_trace"
    assert fail_closed["confidence"] == "MEDIUM_COMPONENT_DIAGNOSTIC"
    assert fail_closed["promotion_gate_allowed"] is False
    assert payload["summary"]["partial_component_only_count"] == 1
    assert payload["summary"]["root_cause_case_count"] >= 1
    assert payload["gate_root_cause_analysis"][0]["promotion_gate_allowed"] is False


def test_gate_availability_requires_explicit_lineage_for_full_equivalence(
    tmp_path: Path,
) -> None:
    trace_path = _write_trace(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path, component_only_date="2023-01-06")

    payload = build_gate_availability_audit(
        trace_path=trace_path,
        gate_audit_root=gate_root,
        start_date="2023-01-03",
        end_date="2023-01-04",
    )

    records = {item["date"]: item for item in payload["gate_availability"]}
    assert records["2023-01-03"]["full_advisory_trace_eligible"] is False
    assert records["2023-01-03"]["component_validation_trace_eligible"] is True
    assert records["2023-01-03"]["blocked_gate"] == "historical_replay_lineage_manifest"
    assert records["2023-01-03"]["reason_class"] == "lineage_manifest_missing"
    assert records["2023-01-03"]["trace_source"] == "component_level_validation_trace"


def test_component_level_historical_trace_marks_non_promotion_source_confidence(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path, component_only_date="2023-01-04")

    payload = build_component_level_historical_trace(
        trace_path=trace_path,
        gate_audit_root=gate_root,
    )

    assert payload["status"] == "PASS"
    rows = payload["rows"]
    assert rows
    component_only = [row for row in rows if row["date"] == "2023-01-04"]
    assert component_only
    assert {row["trace_source"] for row in component_only} == {"component_level_validation_trace"}
    assert {row["confidence"] for row in component_only} == {"MEDIUM_COMPONENT_DIAGNOSTIC"}
    assert all(row["promotion_gate_allowed"] is False for row in rows)
    assert payload["summary"]["partial_component_only_count"] == 1


def test_backtest_trace_bridge_schema_and_non_promotion_marker(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)
    bridge_root = _write_bridge_artifact_root(tmp_path)

    payload = build_backtest_trace_bridge(
        trace_path=trace_path,
        prices_path=prices_path,
        bridge_artifact_root=bridge_root,
        outcome_ticker="QQQ",
    )

    assert payload["status"] == "PASS"
    assert payload["summary"]["bridge_record_count"] == 2
    assert payload["summary"]["source_artifact_count"] == 1
    assert payload["summary"]["date_count"] == 2
    assert payload["summary"]["case_count"] == 2
    record = payload["bridge_records"][0]
    assert record["trace_source"] == "backtest_trace_bridge"
    assert record["confidence"] == "MEDIUM_BACKTEST_BRIDGE_DIAGNOSTIC"
    assert record["promotion_gate_allowed"] is False
    assert record["allowed_uses"] == ["diagnostic", "ablation", "sensitivity_analysis"]
    assert {
        "as_of_date",
        "decision_time",
        "asset",
        "scenario",
        "trace_source",
        "trace_contract_version",
    } <= set(record["outcome_join_key"])
    assert {"return_1d", "return_5d", "return_10d", "return_20d"} <= set(record["outcomes"])
    source = payload["source_artifacts"][0]
    assert source["source_artifact_path"].endswith("historical_backtest_summary.json")
    assert source["as_of_date"] == "2023-01-04"
    assert source["promotion_gate_allowed"] is False


def test_lineage_manifest_repair_report_lists_missing_affected_artifacts(
    tmp_path: Path,
) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    gate_root = _write_gate_audit_root(tmp_path)

    payload = build_lineage_manifest_repair_report(
        trace_path=trace_path,
        gate_audit_root=gate_root,
        start_date="2023-01-03",
        end_date="2023-01-06",
        asset_universe="QQQ,SPY",
    )

    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["summary"]["affected_root_cause_case_count"] == 2
    assert payload["summary"]["affected_artifact_count"] == 1
    assert payload["summary"]["source_artifact_missing_count"] == 1
    assert payload["summary"]["lineage_manifest_missing_after_gate_audit"] == 2
    artifact = payload["affected_artifacts"][0]
    assert (
        artifact["source_artifact_path"]
        .replace("\\", "/")
        .endswith("gate_audit/2023-01-06/daily_indicator_weight_trace.json")
    )
    assert artifact["as_of_date"] == "2023-01-06"
    assert artifact["decision_time"] == "2023-01-06"
    assert artifact["production_equivalent"] is False
    assert artifact["manifest_validation_status"] == "SOURCE_ARTIFACT_MISSING"
    assert artifact["promotion_gate_allowed"] is False
    assert artifact["allowed_uses"] == ["diagnostic", "ablation", "sensitivity_analysis"]


def test_indicator_validation_pack_writes_expected_artifacts(tmp_path: Path) -> None:
    payload = write_indicator_framework_validation_pack(output_root=tmp_path)

    assert payload["status"] == "INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS"
    expected = {
        "daily_indicator_inventory",
        "daily_indicator_coverage_gap_report",
        "threshold_registry_audit",
        "indicator_dependency_graph",
        "multi_stage_weight_trace_contract",
        "constraint_attribution_report",
        "indicator_mapping_candidate_plan_valuation_crowding",
        "indicator_masking_and_dominance_audit_valuation_crowding",
        "valuation_crowding_pilot_validation_report",
        "indicator_masking_casebook_valuation_crowding_trend",
        "valuation_crowding_ablation_validation",
        "valuation_crowding_outcome_availability_audit",
        "valuation_crowding_masking_effectiveness_review",
        "valuation_crowding_masking_robustness_review",
        "indicator_research_validation_rollup",
        "long_horizon_evidence_floor_calibration_audit",
        "historical_multi_stage_weight_trace_validation",
        "historical_trace_gate_availability_audit",
        "component_level_historical_trace",
        "backtest_trace_bridge",
        "lineage_manifest_repair_report",
        "indicator_to_signal_research_framework_v1_validation_pack",
    }
    assert expected <= set(payload["artifacts"])
    threshold_summary = payload["summary"]["threshold_audit_summary"]
    assert threshold_summary["total_threshold_count"] >= 30
    assert threshold_summary["high_impact_threshold_count"] > 0
    assert threshold_summary["uncalibrated_high_impact_count"] > 0
    assert threshold_summary["heuristic_guardrail_count"] > 0
    assert threshold_summary["calibrated_count"] == 0
    assert threshold_summary["thresholds_blocking_promotion"]
    for paths in payload["artifacts"].values():
        assert Path(paths["json_path"]).exists()
        assert Path(paths["markdown_path"]).exists()

    pack_json = Path(
        payload["artifacts"]["indicator_to_signal_research_framework_v1_validation_pack"][
            "json_path"
        ]
    )
    written = json.loads(pack_json.read_text(encoding="utf-8"))
    assert "indicator_to_signal_research_framework_v1_validation_pack" in written["artifacts"]


def test_indicator_validation_pack_stability_report_is_stable(tmp_path: Path) -> None:
    trace = build_daily_indicator_weight_trace(_fake_daily_score_report())
    trace_path = write_daily_indicator_weight_trace(
        trace,
        tmp_path / "daily_indicator_weight_trace.json",
    )

    payload = write_indicator_validation_pack_stability_report(
        output_root=tmp_path,
        trace_path=trace_path,
    )

    assert payload["status"] == "PASS"
    assert payload["summary"]["stable"] is True
    assert payload["stable_fields"]["artifact_count"] is True
    assert payload["stable_fields"]["trace_fields_complete"] is True
    assert payload["stable_fields"]["coverage_gap_unregistered"] is True
    assert payload["stable_fields"]["high_impact_unvalidated"] is True
    assert payload["stable_fields"]["masking_diagnostics_repeatable"] is True
    assert payload["stable_fields"]["masking_casebook_repeatable"] is True
    assert payload["stable_fields"]["gate_availability_repeatable"] is True
    assert payload["stable_fields"]["component_trace_repeatable"] is True
    assert payload["stable_fields"]["backtest_trace_bridge_repeatable"] is True
    assert payload["stable_fields"]["outcome_availability_repeatable"] is True
    assert payload["stable_fields"]["masking_robustness_repeatable"] is True
    assert payload["stable_fields"]["validation_rollup_repeatable"] is True
    assert payload["stable_fields"]["floor_calibration_repeatable"] is True
    assert payload["stable_fields"]["threshold_registry_audit_repeatable"] is True
    assert (
        tmp_path
        / "control_plane_v1_validation"
        / "indicator_validation_pack_rerun_stability_report.json"
    ).exists()


def test_indicator_cli_inventory_and_validation_pack(tmp_path: Path) -> None:
    runner = CliRunner()
    inventory = runner.invoke(
        app,
        ["research", "indicators", "inventory", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    pack = runner.invoke(
        app,
        ["research", "indicators", "validation-pack", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    threshold_audit = runner.invoke(
        app,
        ["research", "indicators", "threshold-audit", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    coverage_gap = runner.invoke(
        app,
        ["research", "indicators", "coverage-gap", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    casebook = runner.invoke(
        app,
        ["research", "indicators", "masking-casebook", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    ablation = runner.invoke(
        app,
        ["research", "indicators", "ablation-validation", "--output-root", str(tmp_path)],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    effectiveness = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "masking-effectiveness-review",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    robustness = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "masking-robustness-review",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    validation_rollup = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "validation-rollup",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    floor_calibration = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "long-horizon-floor-calibration-audit",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    outcome_availability = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "outcome-availability-audit",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    historical_trace = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "historical-trace-validation",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    gate_availability = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "gate-availability-audit",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    lineage_repair = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "lineage-manifest-repair",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    component_trace = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "component-historical-trace",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    backtest_bridge = runner.invoke(
        app,
        [
            "research",
            "indicators",
            "backtest-trace-bridge",
            "--output-root",
            str(tmp_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert inventory.exit_code == 0, inventory.output
    assert pack.exit_code == 0, pack.output
    assert threshold_audit.exit_code == 0, threshold_audit.output
    assert coverage_gap.exit_code == 0, coverage_gap.output
    assert casebook.exit_code == 0, casebook.output
    assert ablation.exit_code == 0, ablation.output
    assert effectiveness.exit_code == 0, effectiveness.output
    assert robustness.exit_code == 0, robustness.output
    assert validation_rollup.exit_code == 0, validation_rollup.output
    assert floor_calibration.exit_code == 0, floor_calibration.output
    assert outcome_availability.exit_code == 0, outcome_availability.output
    assert historical_trace.exit_code == 0, historical_trace.output
    assert gate_availability.exit_code == 0, gate_availability.output
    assert lineage_repair.exit_code == 0, lineage_repair.output
    assert component_trace.exit_code == 0, component_trace.output
    assert backtest_bridge.exit_code == 0, backtest_bridge.output
    assert (tmp_path / "daily_indicator_inventory.json").exists()
    assert (tmp_path / "daily_indicator_coverage_gap_report.json").exists()
    assert (tmp_path / "threshold_registry_audit.json").exists()
    assert (tmp_path / "indicator_masking_casebook_valuation_crowding_trend.json").exists()
    assert (tmp_path / "valuation_crowding_ablation_validation.json").exists()
    assert (tmp_path / "valuation_crowding_outcome_availability_audit.json").exists()
    assert (tmp_path / "valuation_crowding_masking_effectiveness_review.json").exists()
    assert (tmp_path / "valuation_crowding_masking_robustness_review.json").exists()
    assert (tmp_path / "indicator_research_validation_rollup.json").exists()
    assert (tmp_path / "long_horizon_evidence_floor_calibration_audit.json").exists()
    assert (tmp_path / "historical_multi_stage_weight_trace_validation.json").exists()
    assert (tmp_path / "historical_trace_gate_availability_audit.json").exists()
    assert (tmp_path / "lineage_manifest_repair_report.json").exists()
    assert (tmp_path / "component_level_historical_trace.json").exists()
    assert (tmp_path / "backtest_trace_bridge.json").exists()
    assert (
        tmp_path
        / "control_plane_v1_validation"
        / "indicator_to_signal_research_framework_v1_validation_pack.json"
    ).exists()
    assert "Indicator validation pack" in pack.output


def test_research_campaign_adapter_contract_includes_indicator_framework() -> None:
    payload = validate_stage_adapter_contracts()

    assert payload["validation_status"] == "PASS"
    assert "indicator-research-framework-v1-adapter" in payload["adapter_ids"]
    assert (
        payload["adapter_run_modes"]["indicator-research-framework-v1-adapter"]
        == "VALIDATION_ONLY_MODE"
    )


def _write_trace(tmp_path: Path) -> Path:
    path = tmp_path / "multi_stage_weight_trace.json"
    path.write_text(
        json.dumps(
            {
                "rows": [
                    {
                        "date": "2023-01-03",
                        "asset": "QQQ",
                        "module_id": "valuation_crowding_indicator",
                        "weight_before": 0.80,
                        "weight_after": 0.65,
                        "upstream_indicator_id": "valuation_crowding_indicator",
                        "downstream_indicator_id": "trend_strength_indicator",
                        "b_intended_change": 0.20,
                        "a_suppressed_change": 0.15,
                    },
                    {
                        "date": "2023-01-04",
                        "asset": "SMH",
                        "module_id": "valuation_crowding_indicator",
                        "weight_before": 0.70,
                        "weight_after": 0.62,
                        "upstream_indicator_id": "valuation_crowding_indicator",
                        "downstream_indicator_id": "trend_strength_indicator",
                        "b_intended_change": 0.10,
                        "a_suppressed_change": 0.08,
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_casebook_trace(tmp_path: Path) -> Path:
    path = tmp_path / "casebook_trace.json"
    rows = []
    for row_date, before, after, final, intended, suppressed in (
        ("2023-01-03", 0.80, 0.65, 0.325, 0.20, 0.15),
        ("2023-01-04", 0.72, 0.62, 0.310, 0.18, 0.10),
    ):
        rows.extend(
            [
                _complete_trace_row(
                    row_date,
                    row_type="indicator_component",
                    module_id="trend_strength_indicator",
                    daily_component_id="trend",
                    mapping_version="scoring_rules_v1.trend",
                    raw_indicator_value=[
                        {
                            "subject": "QQQ",
                            "feature": "above_ma_200",
                            "value": 1.0,
                            "available": True,
                        }
                    ],
                    normalized_indicator_score=80.0,
                    mapped_signal_contribution=24.0,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=False,
                    upstream_indicator_id="trend_strength_indicator",
                    downstream_indicator_id="",
                    b_intended_change=None,
                    a_suppressed_change=None,
                    reason_code="component_score_contribution_trace",
                ),
                _complete_trace_row(
                    row_date,
                    row_type="indicator_component",
                    module_id="valuation_crowding_indicator",
                    daily_component_id="valuation",
                    mapping_version="scoring_rules_v1.valuation+position_gate_valuation_v1",
                    raw_indicator_value=[
                        {
                            "subject": "AI_CORE_MEDIAN",
                            "feature": "valuation_percentile",
                            "value": 0.92,
                            "available": True,
                        }
                    ],
                    normalized_indicator_score=35.0,
                    mapped_signal_contribution=3.5,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=False,
                    upstream_indicator_id="valuation_crowding_indicator",
                    downstream_indicator_id="",
                    b_intended_change=None,
                    a_suppressed_change=None,
                    reason_code="component_score_contribution_trace",
                ),
                _complete_trace_row(
                    row_date,
                    row_type="constraint_gate",
                    module_id="valuation_crowding_indicator",
                    daily_component_id="valuation",
                    mapping_version="scoring_rules_v1.valuation+position_gate_valuation_v1",
                    raw_indicator_value={
                        "gate_id": "valuation",
                        "label": "估值拥挤",
                        "reason": "fixture valuation/crowding cap",
                        "gate_max_position": after,
                    },
                    normalized_indicator_score=None,
                    mapped_signal_contribution=None,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=True,
                    upstream_indicator_id="valuation_crowding_indicator",
                    downstream_indicator_id="trend_strength_indicator",
                    b_intended_change=intended,
                    a_suppressed_change=suppressed,
                    reason_code="valuation_constraint_attribution",
                ),
            ]
        )
    lineage_manifests = [
        {
            "source_artifact_path": f"fixture/daily_indicator_weight_trace_{row_date}.json",
            "generated_at": f"{row_date}T21:30:00+00:00",
            "as_of_date": row_date,
            "decision_time": row_date,
            "config_hash": f"config-{row_date}",
            "input_snapshot_hash": f"inputs-{row_date}",
            "trace_contract_version": "multi_stage_weight_trace_contract_v1",
            "production_equivalent": True,
        }
        for row_date in ("2023-01-03", "2023-01-04")
    ]
    path.write_text(
        json.dumps(
            {"rows": rows, "lineage_manifests": lineage_manifests},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _write_many_casebook_trace(tmp_path: Path, *, date_count: int = 20) -> Path:
    path = tmp_path / "many_casebook_trace.json"
    rows = []
    lineage_manifests = []
    start = date(2023, 1, 3)
    for offset in range(date_count):
        row_date = (start + timedelta(days=offset)).isoformat()
        before = 0.80
        after = 0.65
        final = 0.325
        intended = 0.20
        suppressed = 0.15
        rows.extend(
            [
                _complete_trace_row(
                    row_date,
                    row_type="indicator_component",
                    module_id="trend_strength_indicator",
                    daily_component_id="trend",
                    mapping_version="scoring_rules_v1.trend",
                    raw_indicator_value=[
                        {
                            "subject": "QQQ",
                            "feature": "above_ma_200",
                            "value": 1.0,
                            "available": True,
                        }
                    ],
                    normalized_indicator_score=80.0,
                    mapped_signal_contribution=24.0,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=False,
                    upstream_indicator_id="trend_strength_indicator",
                    downstream_indicator_id="",
                    b_intended_change=None,
                    a_suppressed_change=None,
                    reason_code="component_score_contribution_trace",
                ),
                _complete_trace_row(
                    row_date,
                    row_type="indicator_component",
                    module_id="valuation_crowding_indicator",
                    daily_component_id="valuation",
                    mapping_version=("scoring_rules_v1.valuation+position_gate_valuation_v1"),
                    raw_indicator_value=[
                        {
                            "subject": "AI_CORE_MEDIAN",
                            "feature": "valuation_percentile",
                            "value": 0.92,
                            "available": True,
                        }
                    ],
                    normalized_indicator_score=35.0,
                    mapped_signal_contribution=3.5,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=False,
                    upstream_indicator_id="valuation_crowding_indicator",
                    downstream_indicator_id="",
                    b_intended_change=None,
                    a_suppressed_change=None,
                    reason_code="component_score_contribution_trace",
                ),
                _complete_trace_row(
                    row_date,
                    row_type="constraint_gate",
                    module_id="valuation_crowding_indicator",
                    daily_component_id="valuation",
                    mapping_version=("scoring_rules_v1.valuation+position_gate_valuation_v1"),
                    raw_indicator_value={
                        "gate_id": "valuation",
                        "label": "估值拥挤",
                        "reason": "fixture valuation/crowding cap",
                        "gate_max_position": after,
                    },
                    normalized_indicator_score=None,
                    mapped_signal_contribution=None,
                    pre_constraint_signal_weight=before,
                    post_constraint_signal_weight=after,
                    final_advisory_portfolio_facing_weight=final,
                    weight_before=before,
                    weight_after=after,
                    constraint_hit=True,
                    upstream_indicator_id="valuation_crowding_indicator",
                    downstream_indicator_id="trend_strength_indicator",
                    b_intended_change=intended,
                    a_suppressed_change=suppressed,
                    reason_code="valuation_constraint_attribution",
                ),
            ]
        )
        lineage_manifests.append(
            {
                "source_artifact_path": (f"fixture/daily_indicator_weight_trace_{row_date}.json"),
                "generated_at": f"{row_date}T21:30:00+00:00",
                "as_of_date": row_date,
                "decision_time": row_date,
                "config_hash": f"config-{row_date}",
                "input_snapshot_hash": f"inputs-{row_date}",
                "trace_contract_version": "multi_stage_weight_trace_contract_v1",
                "production_equivalent": True,
            }
        )
    path.write_text(
        json.dumps(
            {"rows": rows, "lineage_manifests": lineage_manifests},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _complete_trace_row(
    row_date: str,
    *,
    row_type: str,
    module_id: str,
    daily_component_id: str,
    mapping_version: str,
    raw_indicator_value: object,
    normalized_indicator_score: float | None,
    mapped_signal_contribution: float | None,
    pre_constraint_signal_weight: float,
    post_constraint_signal_weight: float,
    final_advisory_portfolio_facing_weight: float,
    weight_before: float,
    weight_after: float,
    constraint_hit: bool,
    upstream_indicator_id: str,
    downstream_indicator_id: str,
    b_intended_change: float | None,
    a_suppressed_change: float | None,
    reason_code: str,
) -> dict[str, object]:
    return {
        "date": row_date,
        "asset": "AI_RISK_ASSET_BASKET",
        "row_type": row_type,
        "module_id": module_id,
        "daily_component_id": daily_component_id,
        "mapping_version": mapping_version,
        "raw_indicator_value": raw_indicator_value,
        "normalized_indicator_score": normalized_indicator_score,
        "mapped_signal_contribution": mapped_signal_contribution,
        "pre_constraint_signal_weight": pre_constraint_signal_weight,
        "post_constraint_signal_weight": post_constraint_signal_weight,
        "final_advisory_portfolio_facing_weight": final_advisory_portfolio_facing_weight,
        "weight_before": weight_before,
        "weight_after": weight_after,
        "delta": weight_after - weight_before,
        "reason_code": reason_code,
        "constraint_hit": constraint_hit,
        "upstream_indicator_id": upstream_indicator_id,
        "downstream_indicator_id": downstream_indicator_id,
        "b_intended_change": b_intended_change,
        "a_suppressed_change": a_suppressed_change,
        "official_target_weights": False,
        "broker_order": False,
        "live_order": False,
        "production_position_mutation": False,
    }


def _write_outcome_prices(tmp_path: Path) -> Path:
    path = tmp_path / "prices_daily.csv"
    lines = ["date,ticker,adj_close"]
    prices = [
        100.0,
        98.0,
        99.0,
        97.0,
        96.0,
        95.0,
        97.0,
        100.0,
        103.0,
        106.0,
        110.0,
        108.0,
        111.0,
        113.0,
        114.0,
        116.0,
        117.0,
        118.0,
        119.0,
        120.0,
        115.0,
        116.0,
        117.0,
        118.0,
        119.0,
    ]
    start = date(2023, 1, 3)
    tickers = {
        "QQQ": 1.00,
        "SPY": 0.75,
        "SMH": 1.25,
        "MSFT": 2.00,
        "GOOGL": 1.50,
        "NVDA": 3.00,
        "AMD": 1.10,
        "TSM": 0.95,
    }
    for offset, price in enumerate(prices):
        row_date = start + timedelta(days=offset)
        for ticker, multiplier in tickers.items():
            lines.append(f"{row_date.isoformat()},{ticker},{price * multiplier:.4f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_short_outcome_prices(tmp_path: Path) -> Path:
    path = tmp_path / "prices_daily_short.csv"
    lines = ["date,ticker,adj_close"]
    start = date(2023, 1, 3)
    tickers = {
        "QQQ": 1.00,
        "SPY": 0.75,
        "SMH": 1.25,
        "MSFT": 2.00,
        "GOOGL": 1.50,
    }
    for offset, price in enumerate((100.0, 101.0, 102.0)):
        row_date = start + timedelta(days=offset)
        for ticker, multiplier in tickers.items():
            lines.append(f"{row_date.isoformat()},{ticker},{price * multiplier:.4f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_medium_outcome_prices(tmp_path: Path) -> Path:
    path = tmp_path / "prices_daily_medium.csv"
    lines = ["date,ticker,adj_close"]
    start = date(2023, 1, 3)
    tickers = {
        "QQQ": 1.00,
        "SPY": 0.75,
        "SMH": 1.25,
        "MSFT": 2.00,
        "GOOGL": 1.50,
    }
    prices = (100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0)
    for offset, price in enumerate(prices):
        row_date = start + timedelta(days=offset)
        for ticker, multiplier in tickers.items():
            lines.append(f"{row_date.isoformat()},{ticker},{price * multiplier:.4f}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_gate_audit_root(
    tmp_path: Path,
    *,
    component_only_date: str = "2023-01-05",
) -> Path:
    root = tmp_path / "gate_audit"
    for row_date in ("2023-01-03", "2023-01-04", component_only_date):
        date_root = root / row_date
        date_root.mkdir(parents=True, exist_ok=True)
        (date_root / "data_quality.md").write_text(
            "\n".join(
                [
                    "# 数据质量报告",
                    "",
                    "- 状态：PASS_WITH_WARNINGS",
                    f"- 评估日期：{row_date}",
                    "",
                    "## 问题",
                    "",
                    "| 级别 | 来源 | Code | 行数 | 说明 | 样例 |",
                    "|---|---|---|---:|---|---|",
                    "| 警告 | 下载审计清单 | download_manifest_missing |  | fixture |  |",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        feature_status = "FAIL" if row_date == component_only_date else "PASS"
        issue_rows = (
            [
                "| ERROR | feature_source_available_time_after_decision_time | "
                "sec_fundamentals_filing | sec_edgar_reconstructed_pit_features | "
                "12 行 available_time 晚于 decision_time。 |",
            ]
            if feature_status == "FAIL"
            else ["未发现 feature availability 问题。"]
        )
        (date_root / "feature_availability.md").write_text(
            "\n".join(
                [
                    "# PIT 特征可见时间报告",
                    "",
                    f"- 状态：{feature_status}",
                    f"- 评估日期：{row_date}",
                    "",
                    "## 覆盖检查",
                    "",
                    "| Severity | Code | Rule | Source | 说明 |",
                    "|---|---|---|---|---|",
                    *issue_rows,
                    "",
                ]
            ),
            encoding="utf-8",
        )
    return root


def _write_many_gate_audit_root(tmp_path: Path, *, date_count: int = 20) -> Path:
    root = tmp_path / "many_gate_audit"
    start = date(2023, 1, 3)
    for offset in range(date_count):
        row_date = (start + timedelta(days=offset)).isoformat()
        date_root = root / row_date
        date_root.mkdir(parents=True, exist_ok=True)
        (date_root / "data_quality.md").write_text(
            "\n".join(
                [
                    "# 数据质量报告",
                    "",
                    "- 状态：PASS",
                    f"- 评估日期：{row_date}",
                    "",
                    "未发现数据质量阻断问题。",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        (date_root / "feature_availability.md").write_text(
            "\n".join(
                [
                    "# PIT 特征可见时间报告",
                    "",
                    "- 状态：PASS",
                    f"- 评估日期：{row_date}",
                    "",
                    "未发现 feature availability 问题。",
                    "",
                ]
            ),
            encoding="utf-8",
        )
    return root


def _write_bridge_artifact_root(tmp_path: Path) -> Path:
    root = tmp_path / "bridge_artifacts"
    root.mkdir(parents=True, exist_ok=True)
    (root / "historical_backtest_summary.json").write_text(
        json.dumps(
            {
                "report_type": "historical_backtest_summary",
                "status": "PASS",
                "as_of": "2023-01-04",
                "summary": {"total_return": 0.02, "max_drawdown": -0.03},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return root


def _fake_daily_score_report() -> SimpleNamespace:
    model_band = SimpleNamespace(min_position=0.60, max_position=0.80, label="积极")
    final_band = SimpleNamespace(min_position=0.60, max_position=0.65, label="估值受限")
    total_asset_band = SimpleNamespace(min_position=0.30, max_position=0.325, label="估值受限")
    total_risk_band = SimpleNamespace(min_position=0.50, max_position=0.50, label="总风险资产")
    gates = (
        SimpleNamespace(
            gate_id="score_model",
            label="评分模型仓位",
            source="weighted_score_model",
            max_position=0.80,
            triggered=True,
            reason="score model",
            gate_class="score_mapping",
            target_effect="raw_position_mapping",
            execution_effect="base_signal_to_raw_position",
        ),
        SimpleNamespace(
            gate_id="valuation",
            label="估值拥挤",
            source="data/external/valuation_snapshots",
            max_position=0.65,
            triggered=True,
            reason="存在 EXPENSIVE_OR_CROWDED 估值或拥挤度信号。",
            gate_class="hard_cap",
            target_effect="max_position_cap",
            execution_effect="final_position_limit",
        ),
    )
    recommendation = SimpleNamespace(
        total_score=72.0,
        model_risk_asset_ai_band=model_band,
        risk_asset_ai_band=final_band,
        total_asset_ai_band=total_asset_band,
        total_risk_asset_band=total_risk_band,
        position_gates=gates,
    )
    return SimpleNamespace(
        as_of=date(2023, 1, 3),
        components=(
            _fake_component(
                "trend",
                score=80.0,
                weight=30.0,
                signal_subject="SPY",
                signal_feature="above_ma_200",
                value=1.0,
                earned_points=10.0,
            ),
            _fake_component(
                "valuation",
                score=40.0,
                weight=10.0,
                signal_subject="AI_CORE_MEDIAN",
                signal_feature="valuation_percentile",
                value=0.92,
                earned_points=4.0,
            ),
        ),
        recommendation=recommendation,
        confidence_assessment=SimpleNamespace(
            score=88.0,
            level="high",
            reasons=("hard data coverage",),
        ),
        data_quality_report=SimpleNamespace(status="PASS"),
        feature_set=SimpleNamespace(status="PASS", warnings=()),
        minimum_action_delta=0.02,
    )


def _fake_component(
    name: str,
    *,
    score: float,
    weight: float,
    signal_subject: str,
    signal_feature: str,
    value: float,
    earned_points: float,
) -> SimpleNamespace:
    signal = SimpleNamespace(
        subject=signal_subject,
        feature=signal_feature,
        value=value,
        points=10.0,
        earned_points=earned_points,
        available=True,
        reason="fixture",
    )
    return SimpleNamespace(
        name=name,
        score=score,
        weight=weight,
        source_type="hard_data",
        coverage=1.0,
        confidence=1.0,
        reason="fixture",
        signals=(signal,),
    )
