from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.layer1_meta_policy_readiness import (
    run_layer1_dataset_lineage_leakage_audit,
    run_layer1_naive_selector_baselines,
    run_layer1_objective_outcome_contract,
    run_layer1_policy_combiner_contract,
    run_layer1_purged_walk_forward_split_contract,
    run_layer1_reader_brief_safety_preview,
    run_layer1_research_dataset_builder,
    run_layer1_selector_cost_adjusted_evaluation,
    run_layer1_simple_rule_selector_search,
    run_layer2_best_component_label_builder,
)
from ai_trading_system.layer2_strategy_component_readiness import (
    run_layer2_anti_leakage_time_boundary_audit,
    run_layer2_common_robustness_validation,
    run_layer2_component_data_quality_check,
    run_layer2_component_definition_lock,
    run_layer2_component_distinctiveness_review,
    run_layer2_component_pool_freeze,
    run_layer2_component_readiness_matrix,
    run_layer2_component_readiness_reconciliation,
    run_layer2_forward_outcome_cube_build,
    run_layer2_historical_weight_path_build,
    run_layer2_return_cost_exposure_panel,
    run_layer2_selector_headroom_oracle_review,
    run_layer2_switching_constraint_contract,
    run_layer2_transition_cost_latency_review,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

LAYER2_REPORT_IDS = {
    "layer2_component_readiness_reconciliation",
    "layer2_component_pool_freeze",
    "layer2_component_definition_lock",
    "layer2_component_data_quality_check",
    "layer2_component_readiness_matrix",
    "layer2_historical_weight_path",
    "layer2_return_cost_exposure_panel",
    "layer2_forward_outcome_cube",
    "layer2_anti_leakage_time_boundary_audit",
    "layer2_common_robustness_validation",
    "layer2_transition_cost_latency_review",
    "layer2_component_distinctiveness_review",
    "layer2_selector_headroom_oracle_review",
    "layer2_switching_constraint_contract",
    "layer2_best_component_label_builder",
    "layer1_policy_combiner_contract",
    "layer1_objective_outcome_contract",
    "layer1_purged_walk_forward_split_contract",
    "layer1_research_dataset",
    "layer1_dataset_lineage_leakage_audit",
    "layer1_naive_selector_baselines",
    "layer1_simple_rule_selector_search",
    "layer1_selector_cost_adjusted_evaluation",
    "layer1_selector_regime_period_validation",
    "layer1_selector_failure_case_review",
    "layer1_historical_research_readiness_gate",
    "layer1_research_owner_decision_pack",
    "layer1_reader_brief_safety_preview",
    "layer1_meta_policy_master_review",
}


def test_layer2_component_readiness_builders_freeze_growth_as_inactive_reference(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer2_caches(tmp_path)
    simple_output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    growth_output_root = tmp_path / "outputs" / "research_strategies" / "qqq_plus_growth"
    output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    _write_source_artifacts(simple_output_root, growth_output_root)

    reconciliation = run_layer2_component_readiness_reconciliation(
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    pool = run_layer2_component_pool_freeze(
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    definitions = run_layer2_component_definition_lock(
        growth_output_root=growth_output_root,
        output_root=output_root,
    )
    data_quality = run_layer2_component_data_quality_check(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    matrix = run_layer2_component_readiness_matrix(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        simple_output_root=simple_output_root,
        growth_output_root=growth_output_root,
        as_of_date=as_of,
        output_root=output_root,
    )

    assert reconciliation["summary"]["growth_owner_decision"] == "KEEP_GROWTH_RESEARCH_ONLY"
    assert pool["status"] == "LAYER2_POOL_FROZEN_WITHOUT_GROWTH"
    assert pool["summary"]["growth_in_formal_pool"] is False
    assert definitions["status"] == "ALL_COMPONENT_DEFINITIONS_LOCKED"
    assert data_quality["status"] in {
        "LAYER2_DATA_QUALITY_PASS",
        "LAYER2_DATA_QUALITY_PASS_WITH_WARNINGS",
    }
    assert matrix["status"] in {
        "LAYER2_COMPONENT_READINESS_MATRIX_READY",
        "LAYER2_COMPONENT_READINESS_MATRIX_READY_WITH_WARNINGS",
    }
    assert matrix["layer1_historical_research_allowed"] is False
    assert matrix["paper_shadow_allowed"] is False
    assert matrix["production_allowed"] is False
    assert matrix["broker_action"] == "none"

    rows = {row["strategy_id"]: row for row in matrix["component_readiness_matrix"]}
    assert rows["equal_risk_qqq_sgov"]["selectable_by_layer1"] is True
    assert rows["100_qqq"]["selectable_by_layer1"] is True
    assert rows["qqq_50_sgov_50"]["reference_only"] is True
    growth = rows["qqq_plus_growth_research_candidate"]
    assert growth["formal_component_pool_member"] is False
    assert growth["selectable_by_layer1"] is False
    assert growth["readiness_status"] == "RESEARCH_ONLY_INACTIVE_REFERENCE"
    assert "owner_decision_keep_growth_research_only" in growth["blockers"]

    for payload in (reconciliation, pool, definitions, data_quality, matrix):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_layer2_component_readiness_cli_and_report_registry(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer2_caches(tmp_path)
    simple_output_root = tmp_path / "outputs" / "research_strategies" / "simple_baselines"
    growth_output_root = tmp_path / "outputs" / "research_strategies" / "qqq_plus_growth"
    output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    _write_source_artifacts(simple_output_root, growth_output_root)

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer2-component-readiness-matrix",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--simple-output-root",
            str(simple_output_root),
            "--growth-output-root",
            str(growth_output_root),
            "--output-root",
            str(output_root),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(
        (output_root / "layer2_component_readiness_matrix.json").read_text(encoding="utf-8")
    )
    assert payload["summary"]["inactive_growth_reference_count"] == 1
    assert payload["summary"]["broker_action"] == "none"

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    registered = {item["report_id"] for item in registry["reports"]}
    assert LAYER2_REPORT_IDS <= registered
    for report_id in LAYER2_REPORT_IDS:
        entry = next(item for item in registry["reports"] if item["report_id"] == report_id)
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"


def test_layer2_fact_and_outcome_builders_exclude_growth_and_preserve_safety(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer2_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"

    weight_path = run_layer2_historical_weight_path_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    return_panel = run_layer2_return_cost_exposure_panel(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    outcome_cube = run_layer2_forward_outcome_cube_build(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    leakage_audit = run_layer2_anti_leakage_time_boundary_audit(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    robustness = run_layer2_common_robustness_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )

    assert weight_path["status"] in {
        "LAYER2_WEIGHT_PATH_READY",
        "LAYER2_WEIGHT_PATH_DATA_WARN",
    }
    assert return_panel["status"] in {
        "LAYER2_RETURN_PANEL_READY",
        "LAYER2_RETURN_PANEL_WARN",
    }
    assert outcome_cube["status"] in {
        "FORWARD_OUTCOME_CUBE_READY",
        "FORWARD_OUTCOME_CUBE_PARTIAL",
    }
    assert leakage_audit["status"] in {
        "LAYER2_ANTI_LEAKAGE_PASS",
        "LAYER2_ANTI_LEAKAGE_WARN",
    }
    assert robustness["status"] in {
        "LAYER2_ROBUSTNESS_READY",
        "LAYER2_ROBUSTNESS_MIXED",
    }

    expected_components = {
        "equal_risk_qqq_sgov",
        "100_qqq",
        "qqq_50_sgov_50",
        "qqq_60_sgov_40",
    }
    weight_frame = pd.read_parquet(output_root / "layer2_historical_weight_path.parquet")
    panel_frame = pd.read_parquet(output_root / "layer2_return_cost_exposure_panel.parquet")
    cube_frame = pd.read_parquet(output_root / "layer2_forward_outcome_cube.parquet")

    assert set(weight_frame["strategy_id"].unique()) == expected_components
    assert "qqq_plus_growth_research_candidate" not in set(weight_frame["strategy_id"])
    assert set(panel_frame["strategy_id"].unique()) == expected_components
    assert {"net_return", "effective_qqq_beta", "transaction_cost", "slippage_cost"} <= set(
        panel_frame.columns
    )
    assert {"5d", "120d"} <= set(cube_frame["horizon"].unique())
    assert cube_frame["outcome_side_only"].all()
    assert (cube_frame["relative_return_vs_growth_candidate"].isna()).all()

    for payload in (weight_path, return_panel, outcome_cube, leakage_audit, robustness):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_layer2_selector_headroom_first_batch_preserves_research_boundary(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer2_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"

    transition = run_layer2_transition_cost_latency_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    distinctiveness = run_layer2_component_distinctiveness_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    headroom = run_layer2_selector_headroom_oracle_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )
    constraints = run_layer2_switching_constraint_contract(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        as_of_date=as_of,
        output_root=output_root,
    )

    assert transition["status"] in {
        "TRANSITION_COST_ACCEPTABLE",
        "TRANSITION_COST_MATERIAL",
        "TRANSITION_COST_TOO_HIGH",
    }
    assert {row["pair"] for row in transition["transition_cost_rows"]} == {
        "equal_risk_qqq_sgov ↔ 100_qqq",
        "equal_risk_qqq_sgov ↔ qqq_50_sgov_50",
        "equal_risk_qqq_sgov ↔ qqq_60_sgov_40",
        "100_qqq ↔ qqq_50_sgov_50",
        "100_qqq ↔ qqq_60_sgov_40",
    }
    assert {
        "avg_turnover",
        "median_turnover",
        "max_turnover",
        "one_day_execution_lag_impact",
        "two_day_execution_lag_impact",
        "monthly_switch_cost",
        "weekly_switch_cost",
        "threshold_switch_cost",
        "cost_adjusted_return_impact",
        "switching_cost_commentary",
    } <= set(transition["transition_cost_rows"][0])

    assert distinctiveness["status"] in {
        "COMPONENTS_DISTINCT",
        "COMPONENTS_PARTIALLY_DISTINCT",
        "COMPONENTS_REDUNDANT",
    }
    assert len(distinctiveness["required_answers"]) == 5
    assert {
        "weight_path_correlation",
        "return_correlation",
        "drawdown_correlation",
        "exposure_correlation",
        "regime_response_difference",
        "relative_performance_dispersion",
        "turnover_difference",
        "risk_budget_difference",
    } <= set(distinctiveness["distinctiveness_rows"][0])

    assert headroom["status"] in {
        "SELECTOR_HEADROOM_MATERIAL",
        "SELECTOR_HEADROOM_MODEST",
        "NO_SELECTOR_HEADROOM",
    }
    assert {row["oracle_variant"] for row in headroom["oracle_rows"]} == {
        "oracle_best_5d_component",
        "oracle_best_20d_component",
        "oracle_best_60d_component",
        "oracle_best_drawdown_reduction",
        "oracle_best_calmar_window",
        "cost_adjusted_oracle",
        "min_holding_20d_oracle",
        "min_holding_60d_oracle",
    }
    assert {
        "oracle_return",
        "oracle_max_drawdown",
        "oracle_sharpe",
        "oracle_calmar",
        "turnover",
        "cost_adjusted_oracle_return",
        "headroom_vs_best_static_component",
        "headroom_vs_equal_risk",
        "headroom_vs_100_qqq",
        "required_prediction_accuracy_to_break_even",
    } <= set(headroom["oracle_rows"][0])
    assert headroom["oracle_realizable_strategy"] is False

    assert constraints["status"] in {
        "SWITCHING_CONSTRAINT_READY",
        "SWITCHING_CONSTRAINT_NEEDS_OWNER_REVIEW",
    }
    assert constraints["selector_transition_rules"]["minimum_holding_period"] == ("20 trading days")
    assert constraints["selectable_component_ids"] == [
        "equal_risk_qqq_sgov",
        "100_qqq",
    ]
    assert "qqq_50_sgov_50" in constraints["reference_only_component_ids"]

    for payload in (transition, distinctiveness, headroom, constraints):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer2-selector-headroom-oracle-review",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_root / "layer2_selector_headroom_oracle_review.json").exists()


def test_layer1_meta_policy_readiness_builders_are_research_only(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer2_caches(tmp_path)
    layer2_output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"

    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "as_of_date": as_of,
        "output_root": output_root,
        "layer2_output_root": layer2_output_root,
    }
    labels = run_layer2_best_component_label_builder(**common)
    combiner = run_layer1_policy_combiner_contract(output_root=output_root)
    objective = run_layer1_objective_outcome_contract(output_root=output_root)
    split = run_layer1_purged_walk_forward_split_contract(**common)
    dataset = run_layer1_research_dataset_builder(**common)
    leakage = run_layer1_dataset_lineage_leakage_audit(**common)
    naive = run_layer1_naive_selector_baselines(**common)
    simple = run_layer1_simple_rule_selector_search(**common)
    cost_eval = run_layer1_selector_cost_adjusted_evaluation(**common)
    reader = run_layer1_reader_brief_safety_preview(output_root=output_root)

    assert labels["status"] in {"BEST_COMPONENT_LABELS_READY", "BEST_COMPONENT_LABELS_PARTIAL"}
    assert combiner["status"] == "POLICY_COMBINER_CONTRACT_READY"
    assert objective["status"] == "LAYER1_OBJECTIVE_READY"
    assert split["status"] in {
        "PURGED_WALK_FORWARD_CONTRACT_READY",
        "PURGED_WALK_FORWARD_NEEDS_REVIEW",
    }
    assert dataset["status"] in {
        "LAYER1_RESEARCH_DATASET_READY",
        "LAYER1_RESEARCH_DATASET_PARTIAL",
    }
    assert leakage["status"] in {
        "LAYER1_DATASET_LEAKAGE_PASS",
        "LAYER1_DATASET_LEAKAGE_WARN",
    }
    assert naive["status"] == "NAIVE_SELECTOR_BASELINES_READY"
    assert simple["status"] in {
        "SIMPLE_RULE_SELECTOR_SEARCH_READY",
        "SIMPLE_RULE_SELECTOR_NO_EDGE",
    }
    assert cost_eval["status"] == "SELECTOR_COST_EVAL_READY"
    assert reader["status"] == "LAYER1_READER_PREVIEW_SAFE"

    assert dataset["dataset_rows"]
    first_row = dataset["dataset_rows"][0]
    assert {
        "decision_date",
        "market_features_at_decision_time",
        "selectable_component_ids",
        "reference_component_ids",
        "component_target_weights",
        "component_definition_hashes",
        "component_forward_outcomes",
        "best_component_labels",
        "regret_vs_best_component",
        "data_quality_status",
        "split_id",
        "embargo_status",
    } <= set(first_row)
    assert first_row["selectable_component_ids"] == ["equal_risk_qqq_sgov", "100_qqq"]
    assert first_row["embargo_status"] == "MATURED_LABELS_ONLY"

    for payload in (
        labels,
        combiner,
        objective,
        split,
        dataset,
        leakage,
        naive,
        simple,
        cost_eval,
        reader,
    ):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-reader-brief-safety-preview",
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (output_root / "layer1_reader_brief_safety_preview.json").exists()


def _write_source_artifacts(simple_output_root: Path, growth_output_root: Path) -> None:
    simple_output_root.mkdir(parents=True, exist_ok=True)
    growth_output_root.mkdir(parents=True, exist_ok=True)
    for report_type, status in {
        "simple_baseline_master_review": "NARROW_TO_TOP_CANDIDATES",
        "simple_baseline_watchlist_owner_decision": "OWNER_DECISION_READY",
        "simple_baseline_forward_aging_master_review": "FORWARD_AGING_MASTER_REVIEW_READY",
    }.items():
        _write_json(
            simple_output_root / f"{report_type}.json",
            _safe_payload(
                report_type,
                status,
                {
                    "primary_candidate": "equal_risk_qqq_sgov",
                    "paper_shadow_allowed": False,
                    "production_allowed": False,
                    "broker_action": "none",
                },
            ),
        )
    for report_type, status in {
        "qqq_plus_growth_real_cli_suite_summary": "QQQ_PLUS_REAL_RUN_WARN",
        "qqq_plus_growth_candidate_result_summary": "GROWTH_CANDIDATES_SUMMARIZED",
        "growth_edge_vs_qqq_materiality_review": "GROWTH_EDGE_WEAK",
        "qqq_plus_beta_and_exposure_attribution": "ATTRIBUTION_READY",
    }.items():
        _write_json(
            growth_output_root / f"{report_type}.json",
            _safe_payload(report_type, status, {"data_quality_status": "PASS_WITH_WARNINGS"}),
        )
    _write_json(
        growth_output_root / "qqq_plus_growth_owner_decision_pack.json",
        _safe_payload(
            "qqq_plus_growth_owner_decision_pack",
            "QQQ_PLUS_GROWTH_OWNER_DECISION_PACK_READY",
            {
                "owner_recommendation": "KEEP_GROWTH_RESEARCH_ONLY",
                "watchlist_gate_status": "NO_GROWTH_WATCHLIST_CANDIDATE",
                "paper_shadow_allowed": False,
                "production_allowed": False,
                "broker_action": "none",
            },
            owner_recommendation="KEEP_GROWTH_RESEARCH_ONLY",
        ),
    )


def _safe_payload(
    report_type: str,
    status: str,
    summary: dict[str, object],
    **extra: object,
) -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "report_type": report_type,
        "status": status,
        "summary": summary,
        "production_effect": "none",
        "broker_action": "none",
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "manual_review_required": True,
        **extra,
    }


def _write_layer2_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 500)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0007 + 0.0015 * math.sin(day_index / 17.0)
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.0002
        levels["SGOV"] *= 1.0 + 0.00015
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{close:.4f},{1000000 + day_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0005:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path, dates[-1]


def _business_dates(start: date, count: int) -> list[date]:
    result = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current)
        current += timedelta(days=1)
    return result


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
