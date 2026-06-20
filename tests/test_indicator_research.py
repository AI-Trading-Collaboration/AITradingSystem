from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.indicator_research import (
    build_backtest_trace_bridge,
    build_component_level_historical_trace,
    build_daily_indicator_coverage_gap_report,
    build_daily_indicator_inventory,
    build_daily_indicator_weight_trace,
    build_dependency_graph,
    build_gate_availability_audit,
    build_historical_multi_stage_weight_trace_validation,
    build_indicator_research_gate,
    build_mapping_plan,
    build_masking_audit,
    build_masking_casebook,
    build_valuation_crowding_ablation_validation,
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
        dominance["valuation_crowding_indicator"]
        == "DOMINANT_WEIGHT_DRIVER_EXPECTED_UNVALIDATED"
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
    assert payload["effective_thresholds"]["position_gate_thresholds"][
        "expensive_or_crowded_max_position"
    ] == 0.70
    assert payload["high_impact_unvalidated_reason"]["coverage_status"] == (
        "HIGH_IMPACT_UNVALIDATED"
    )
    assert payload["high_masking_reason"]["masking_status"] == "HIGH_MASKING"
    assert payload["trend_interaction"]["dependency_edge_type"] == "MASKS"
    assert payload["no_parameter_mutation"] is True


def test_masking_casebook_artifact_generation(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)
    prices_path = _write_outcome_prices(tmp_path)

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
    assert {"return_1d", "return_5d", "return_10d", "return_20d"} <= set(
        case["outcomes"]
    )
    assert isinstance(case["drawdown_reduced"], bool)
    assert isinstance(case["missed_upside"], bool)
    assert isinstance(case["false_risk_off"], bool)


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
        "false_risk_off_count",
        "missed_upside_count",
        "turnover",
        "constraint_hit_count",
    }
    for scenario in payload["scenarios"].values():
        assert required_fields <= set(scenario)
    assert payload["summary"]["production_weight_logic_changed"] is False


def test_historical_trace_validation_accepts_replay_style_trace(tmp_path: Path) -> None:
    trace_path = _write_casebook_trace(tmp_path)

    payload = build_historical_multi_stage_weight_trace_validation(trace_path=trace_path)

    assert payload["summary"]["date_count"] == 2
    assert payload["summary"]["masking_pair_result_count"] == 1
    assert payload["summary"]["missing_trace_field_record_count"] == 0
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
    assert fail_closed["trace_source"] == "component_level_validation_trace"
    assert fail_closed["confidence"] == "MEDIUM_COMPONENT_DIAGNOSTIC"
    assert fail_closed["promotion_gate_allowed"] is False
    assert payload["summary"]["partial_component_only_count"] == 1


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
    assert {row["trace_source"] for row in component_only} == {
        "component_level_validation_trace"
    }
    assert {row["confidence"] for row in component_only} == {
        "MEDIUM_COMPONENT_DIAGNOSTIC"
    }
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
    record = payload["bridge_records"][0]
    assert record["trace_source"] == "backtest_trace_bridge"
    assert record["confidence"] == "MEDIUM_BACKTEST_BRIDGE_DIAGNOSTIC"
    assert record["promotion_gate_allowed"] is False
    assert record["allowed_uses"] == ["diagnostic", "ablation", "sensitivity_analysis"]
    assert {"return_1d", "return_5d", "return_10d", "return_20d"} <= set(
        record["outcomes"]
    )


def test_indicator_validation_pack_writes_expected_artifacts(tmp_path: Path) -> None:
    payload = write_indicator_framework_validation_pack(output_root=tmp_path)

    assert payload["status"] == "INDICATOR_TO_SIGNAL_RESEARCH_FRAMEWORK_V1_READY_WITH_LIMITATIONS"
    expected = {
        "daily_indicator_inventory",
        "daily_indicator_coverage_gap_report",
        "indicator_dependency_graph",
        "multi_stage_weight_trace_contract",
        "constraint_attribution_report",
        "indicator_mapping_candidate_plan_valuation_crowding",
        "indicator_masking_and_dominance_audit_valuation_crowding",
        "valuation_crowding_pilot_validation_report",
        "indicator_masking_casebook_valuation_crowding_trend",
        "valuation_crowding_ablation_validation",
        "historical_multi_stage_weight_trace_validation",
        "historical_trace_gate_availability_audit",
        "component_level_historical_trace",
        "backtest_trace_bridge",
        "indicator_to_signal_research_framework_v1_validation_pack",
    }
    assert expected <= set(payload["artifacts"])
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
    assert coverage_gap.exit_code == 0, coverage_gap.output
    assert casebook.exit_code == 0, casebook.output
    assert ablation.exit_code == 0, ablation.output
    assert historical_trace.exit_code == 0, historical_trace.output
    assert gate_availability.exit_code == 0, gate_availability.output
    assert component_trace.exit_code == 0, component_trace.output
    assert backtest_bridge.exit_code == 0, backtest_bridge.output
    assert (tmp_path / "daily_indicator_inventory.json").exists()
    assert (tmp_path / "daily_indicator_coverage_gap_report.json").exists()
    assert (tmp_path / "indicator_masking_casebook_valuation_crowding_trend.json").exists()
    assert (tmp_path / "valuation_crowding_ablation_validation.json").exists()
    assert (tmp_path / "historical_multi_stage_weight_trace_validation.json").exists()
    assert (tmp_path / "historical_trace_gate_availability_audit.json").exists()
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
    path.write_text(json.dumps({"rows": rows}, ensure_ascii=False), encoding="utf-8")
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
    for offset, price in enumerate(prices):
        row_date = start + timedelta(days=offset)
        lines.append(f"{row_date.isoformat()},QQQ,{price}")
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
