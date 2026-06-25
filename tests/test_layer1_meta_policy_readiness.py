from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.layer1_simple_rule_meta_policy import (
    REQUIRED_SELECTOR_IDS,
    run_layer1_combined_simple_rule_selector_search,
    run_layer1_drawdown_rule_selector_backtest,
    run_layer1_selector_buffered_200dma_variants,
    run_layer1_selector_cost_latency_stress,
    run_layer1_selector_drawdown_episode_review,
    run_layer1_selector_forward_aging_watchlist_final_review,
    run_layer1_selector_forward_aging_watchlist_gate,
    run_layer1_selector_hysteresis_review,
    run_layer1_selector_low_turnover_finalist_ranking,
    run_layer1_selector_low_turnover_owner_decision_pack,
    run_layer1_selector_low_turnover_ranking,
    run_layer1_selector_min_holding_cooldown_review,
    run_layer1_selector_minimum_holding_period_review,
    run_layer1_selector_monthly_only_review,
    run_layer1_selector_overfit_sensitivity_review,
    run_layer1_selector_owner_decision_pack,
    run_layer1_selector_pause_or_continue_owner_pack,
    run_layer1_selector_period_split_validation,
    run_layer1_selector_regret_attribution,
    run_layer1_selector_result_review_master,
    run_layer1_selector_soft_blend_constrained_search,
    run_layer1_selector_soft_blend_review,
    run_layer1_selector_switch_count_threshold_contract,
    run_layer1_selector_switch_quality_attribution,
    run_layer1_selector_turnover_source_diagnosis,
    run_layer1_selector_vs_component_baseline_ranking,
    run_layer1_selector_vs_simple_components_final_gate,
    run_layer1_simple_rule_selector_master_review,
    run_layer1_simple_rule_selector_registry_review,
    run_layer1_trend_rule_selector_backtest,
    run_layer1_volatility_rule_selector_backtest,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

NEW_REPORT_IDS = {
    "layer1_simple_rule_selector_registry_review",
    "layer1_trend_rule_selector_backtest",
    "layer1_volatility_rule_selector_backtest",
    "layer1_drawdown_rule_selector_backtest",
    "layer1_combined_simple_rule_selector_search",
    "layer1_selector_cost_latency_stress",
    "layer1_selector_period_split_validation",
    "layer1_selector_drawdown_episode_review",
    "layer1_selector_regret_attribution",
    "layer1_selector_vs_component_baseline_ranking",
    "layer1_selector_overfit_sensitivity_review",
    "layer1_selector_minimum_holding_period_review",
    "layer1_selector_forward_aging_watchlist_gate",
    "layer1_selector_owner_decision_pack",
    "layer1_simple_rule_selector_master_review",
}
RESULT_REVIEW_REPORT_IDS = {
    "layer1_selector_real_result_summary",
    "layer1_selector_history_coverage_gap_audit",
    "layer1_selector_recent_regime_risk_disclosure",
    "layer1_selector_owner_watchlist_review",
    "layer1_selector_forward_aging_dry_run",
    "layer1_selector_watchlist_blocker_report",
    "layer1_selector_reader_brief_preview",
    "layer1_selector_result_review_master",
}
LOW_TURNOVER_REPORT_IDS = {
    "layer1_selector_turnover_source_diagnosis",
    "layer1_selector_buffered_200dma_variants",
    "layer1_selector_min_holding_cooldown_review",
    "layer1_selector_soft_blend_review",
    "layer1_selector_low_turnover_ranking",
    "layer1_selector_low_turnover_owner_decision_pack",
}
LOW_TURNOVER_FINAL_GATE_REPORT_IDS = {
    "layer1_selector_switch_count_threshold_contract",
    "layer1_selector_soft_blend_constrained_search",
    "layer1_selector_monthly_only_review",
    "layer1_selector_hysteresis_review",
    "layer1_selector_switch_quality_attribution",
    "layer1_selector_low_turnover_finalist_ranking",
    "layer1_selector_vs_simple_components_final_gate",
    "layer1_selector_forward_aging_watchlist_final_review",
    "layer1_selector_pause_or_continue_owner_pack",
}


def test_layer1_simple_rule_selector_registry_and_report_registry(
    tmp_path: Path,
) -> None:
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"

    payload = run_layer1_simple_rule_selector_registry_review(output_root=output_root)

    assert payload["status"] == "SELECTOR_REGISTRY_READY"
    selector_ids = {row["selector_id"] for row in payload["selector_registry_rows"]}
    assert REQUIRED_SELECTOR_IDS <= selector_ids
    assert all(row["uses_future_data"] is False for row in payload["selector_registry_rows"])
    assert all(row["uses_ml"] is False for row in payload["selector_registry_rows"])
    assert all(row["uses_options"] is False for row in payload["selector_registry_rows"])
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["broker_action"] == "none"

    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-simple-rule-selector-registry-review",
            "--output-root",
            str(output_root),
        ],
    )
    assert result.exit_code == 0, result.output

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    registered = {item["report_id"] for item in registry["reports"]}
    expected = (
        NEW_REPORT_IDS
        | RESULT_REVIEW_REPORT_IDS
        | LOW_TURNOVER_REPORT_IDS
        | LOW_TURNOVER_FINAL_GATE_REPORT_IDS
    )
    assert expected <= registered
    for report_id in expected:
        entry = next(item for item in registry["reports"] if item["report_id"] == report_id)
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"


def test_layer1_simple_rule_selector_research_outputs_are_research_only(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer1_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"
    layer2_output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "as_of_date": as_of,
        "output_root": output_root,
        "layer2_output_root": layer2_output_root,
    }

    trend = run_layer1_trend_rule_selector_backtest(**common)
    vol = run_layer1_volatility_rule_selector_backtest(**common)
    drawdown = run_layer1_drawdown_rule_selector_backtest(**common)
    combined = run_layer1_combined_simple_rule_selector_search(**common)
    cost = run_layer1_selector_cost_latency_stress(**common)
    period = run_layer1_selector_period_split_validation(**common)
    episode = run_layer1_selector_drawdown_episode_review(**common)
    regret = run_layer1_selector_regret_attribution(**common)
    ranking = run_layer1_selector_vs_component_baseline_ranking(**common)
    sensitivity = run_layer1_selector_overfit_sensitivity_review(**common)
    holding = run_layer1_selector_minimum_holding_period_review(**common)
    gate = run_layer1_selector_forward_aging_watchlist_gate(**common)
    owner = run_layer1_selector_owner_decision_pack(**common)
    master = run_layer1_simple_rule_selector_master_review(**common)

    assert trend["status"] in {
        "TREND_SELECTOR_EDGE_FOUND",
        "TREND_SELECTOR_NO_EDGE",
        "TREND_SELECTOR_MIXED",
    }
    assert vol["status"] in {
        "VOL_SELECTOR_EDGE_FOUND",
        "VOL_SELECTOR_NO_EDGE",
        "VOL_SELECTOR_MIXED",
    }
    assert drawdown["status"] in {
        "DRAWDOWN_SELECTOR_EDGE_FOUND",
        "DRAWDOWN_SELECTOR_OVER_DEFENSIVE",
        "DRAWDOWN_SELECTOR_NO_EDGE",
    }
    assert combined["combined_selector_rows"]
    assert cost["cost_latency_stress_rows"]
    assert period["period_split_rows"]
    assert episode["episode_review_rows"]
    assert regret["regret_attribution_rows"]
    assert ranking["ranking_rows"][0]["rank"] == 1
    assert sensitivity["sensitivity_rows"]
    assert holding["minimum_holding_period_rows"]
    assert gate["watchlist_gate_rows"][0]["paper_shadow_allowed"] is False
    assert owner["owner_recommendation"] in {
        "ADD_SELECTOR_TO_FORWARD_AGING",
        "KEEP_SELECTOR_RESEARCH_ONLY",
        "NO_SELECTOR_EDGE",
        "BLOCKED",
    }
    assert master["status"] in {
        "LAYER1_SELECTOR_FORWARD_AGING_READY",
        "LAYER1_SELECTOR_RESEARCH_ONLY",
        "LAYER1_SELECTOR_NO_EDGE",
        "LAYER1_SELECTOR_BLOCKED",
    }

    expected_trend_fields = {
        "selector_id",
        "gross_return",
        "net_return_after_cost",
        "max_drawdown",
        "sharpe",
        "calmar",
        "turnover",
        "switch_count",
        "avg_holding_period",
        "cost_drag",
        "regret_vs_best_component",
        "relative_vs_equal_risk",
        "relative_vs_100_qqq",
    }
    assert expected_trend_fields <= set(trend["trend_selector_rows"][0])
    assert {
        "vol_window",
        "vol_threshold",
        "false_defensive_periods",
        "false_risk_on_periods",
    } <= set(vol["volatility_selector_rows"][0])
    assert {
        "drawdown_threshold",
        "recovery_rule",
        "drawdown_reduction_vs_100_qqq",
        "missed_rebound_cost",
        "late_risk_on_count",
        "late_risk_off_count",
    } <= set(drawdown["drawdown_selector_rows"][0])

    for payload in (
        trend,
        vol,
        drawdown,
        combined,
        cost,
        period,
        episode,
        regret,
        ranking,
        sensitivity,
        holding,
        gate,
        owner,
        master,
    ):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert payload["summary"]["data_quality_status"]
        actual_range = payload["summary"]["actual_requested_date_range"]
        assert actual_range["start"] == date(2022, 12, 1).isoformat()
        assert actual_range["end"] == as_of.isoformat()
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    cli_output_root = tmp_path / "cli_outputs" / "layer1_meta_policy"
    result = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-selector-vs-component-baseline-ranking",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(cli_output_root),
            "--layer2-output-root",
            str(tmp_path / "cli_outputs" / "layer2_components"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert (cli_output_root / "layer1_selector_vs_component_baseline_ranking.json").exists()


def test_layer1_selector_result_review_gate_outputs_are_research_only(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer1_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"
    layer2_output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    docs_root = tmp_path / "docs" / "research"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "as_of_date": as_of,
        "output_root": output_root,
        "layer2_output_root": layer2_output_root,
    }

    master = run_layer1_selector_result_review_master(
        **common,
        owner_doc_path=docs_root / "layer1_selector_owner_watchlist_review.md",
        master_doc_path=docs_root / "layer1_selector_result_review_master.md",
    )
    result = _read_json(output_root / "layer1_selector_real_result_summary.json")
    coverage = _read_json(output_root / "layer1_selector_history_coverage_gap_audit.json")
    recent = _read_json(output_root / "layer1_selector_recent_regime_risk_disclosure.json")
    owner = _read_json(output_root / "layer1_selector_owner_watchlist_review.json")
    dry_run = _read_json(output_root / "layer1_selector_forward_aging_dry_run.json")
    blocker = _read_json(output_root / "layer1_selector_watchlist_blocker_report.json")
    preview = _read_json(output_root / "layer1_selector_reader_brief_preview.json")

    assert result["status"] in {
        "LAYER1_SELECTOR_RESULT_SUMMARY_READY",
        "LAYER1_SELECTOR_RESULT_SUMMARY_INCONCLUSIVE",
    }
    assert {
        "top_selector_id",
        "top_selector_type",
        "net_return_after_cost",
        "max_drawdown",
        "sharpe",
        "calmar",
        "turnover",
        "switch_count",
        "avg_holding_period",
        "relative_vs_always_equal_risk",
        "relative_vs_always_100_qqq",
        "regret_vs_best_component",
        "period_split_status",
        "sensitivity_status",
        "watchlist_status",
        "owner_review_status",
    } <= set(result["summary"])
    assert coverage["status"] in {
        "FULL_HISTORY_AVAILABLE",
        "SHORT_HISTORY_ACCEPTABLE_FOR_RESEARCH",
        "RECENT_REGIME_ONLY_WARNING",
        "HISTORY_COVERAGE_BLOCKED",
    }
    assert coverage["summary"]["can_backfill_to_2012"] is False
    assert recent["status"] in {
        "RECENT_REGIME_RISK_DISCLOSED",
        "RECENT_REGIME_RISK_MATERIAL",
    }
    assert owner["status"] in {
        "ADD_SELECTOR_TO_RESEARCH_ONLY_FORWARD_AGING",
        "KEEP_SELECTOR_RESEARCH_ONLY",
        "NO_SELECTOR_EDGE",
        "NEED_HISTORY_BACKFILL_FIRST",
        "BLOCKED",
    }
    assert dry_run["status"] in {
        "LAYER1_SELECTOR_FORWARD_DRY_RUN_PASS",
        "LAYER1_SELECTOR_FORWARD_DRY_RUN_WARN",
        "LAYER1_SELECTOR_FORWARD_DRY_RUN_BLOCKED",
    }
    assert dry_run["summary"]["observation_written"] is False
    assert blocker["status"] == "WATCHLIST_BLOCKER_REPORT_READY"
    assert blocker["summary"]["paper_shadow_allowed"] is False
    assert preview["status"] == "LAYER1_SELECTOR_READER_PREVIEW_SAFE"
    assert preview["prohibited_phrase_hits"] == []
    assert master["status"] in {
        "LAYER1_SELECTOR_FORWARD_AGING_REVIEWABLE",
        "LAYER1_SELECTOR_DRY_RUN_ONLY",
        "LAYER1_SELECTOR_NEEDS_HISTORY_BACKFILL",
        "LAYER1_SELECTOR_RESEARCH_ONLY",
        "LAYER1_SELECTOR_BLOCKED",
    }
    assert (docs_root / "layer1_selector_owner_watchlist_review.md").exists()
    assert (docs_root / "layer1_selector_result_review_master.md").exists()

    for payload in (result, coverage, recent, owner, dry_run, blocker, preview, master):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    cli_output_root = tmp_path / "cli_outputs" / "layer1_meta_policy"
    result_cli = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-selector-real-result-summary",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(cli_output_root),
            "--layer2-output-root",
            str(tmp_path / "cli_outputs" / "layer2_components"),
        ],
    )
    assert result_cli.exit_code == 0, result_cli.output
    assert (cli_output_root / "layer1_selector_real_result_summary.json").exists()


def test_layer1_selector_low_turnover_refinement_outputs_are_research_only(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer1_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"
    layer2_output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    docs_root = tmp_path / "docs" / "research"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "as_of_date": as_of,
        "output_root": output_root,
        "layer2_output_root": layer2_output_root,
    }

    diagnosis = run_layer1_selector_turnover_source_diagnosis(**common)
    buffered = run_layer1_selector_buffered_200dma_variants(**common)
    holding = run_layer1_selector_min_holding_cooldown_review(**common)
    soft = run_layer1_selector_soft_blend_review(**common)
    ranking = run_layer1_selector_low_turnover_ranking(**common)
    owner = run_layer1_selector_low_turnover_owner_decision_pack(
        **common,
        owner_doc_path=docs_root / "layer1_selector_low_turnover_owner_decision_pack.md",
    )

    assert diagnosis["status"] in {
        "TURNOVER_SOURCE_DIAGNOSED",
        "TURNOVER_NOISE_DOMINANT",
    }
    assert {
        "switch_count",
        "helpful_switch_count",
        "noise_switch_count",
        "near_200dma_switch_count",
        "buffer_band_missing_likely",
        "trend_confirmation_insufficient_likely",
        "minimum_holding_period_too_short_likely",
    } <= set(diagnosis["summary"])
    if diagnosis["turnover_source_rows"]:
        assert {
            "switch_date",
            "from_component",
            "to_component",
            "market_state",
            "subsequent_20d_outcome",
            "switch_was_helpful",
            "switch_was_noise",
            "turnover_cost",
        } <= set(diagnosis["turnover_source_rows"][0])

    assert buffered["status"] == "BUFFERED_200DMA_VARIANTS_REVIEWED"
    assert len(buffered["buffered_200dma_variant_rows"]) == 8
    assert {
        "buffered_200dma_selector",
        "confirmed_200dma_selector",
    } <= {row["variant_family"] for row in buffered["buffered_200dma_variant_rows"]}
    assert holding["minimum_holding_cooldown_rows"]
    assert len(holding["minimum_holding_cooldown_rows"]) == 27
    assert soft["status"] == "SOFT_BLEND_REVIEWED"
    assert soft["blend_weight_path"]
    assert ranking["status"] == "LOW_TURNOVER_SELECTOR_RANKED"
    assert {
        "top_by_net_return",
        "top_by_calmar",
        "top_by_low_turnover",
        "top_by_regret_reduction",
        "dominated_variants",
        "recommended_low_turnover_candidate",
    } <= set(ranking["summary"])
    assert {
        "original_trend_200dma_selector",
        "always_equal_risk",
        "always_100_qqq",
    } <= {row["variant_id"] for row in ranking["low_turnover_ranking_rows"]}
    assert owner["status"] in {
        "LOW_TURNOVER_SELECTOR_REVIEWABLE",
        "KEEP_SELECTOR_DRY_RUN_ONLY",
        "NO_SELECTOR_EDGE",
        "BLOCKED",
    }
    assert (docs_root / "layer1_selector_low_turnover_owner_decision_pack.md").exists()

    for payload in (diagnosis, buffered, holding, soft, ranking, owner):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert payload["summary"]["data_quality_status"]
        actual_range = payload["summary"]["actual_requested_date_range"]
        assert actual_range["start"] == date(2022, 12, 1).isoformat()
        assert actual_range["end"] == as_of.isoformat()
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    cli_output_root = tmp_path / "cli_outputs" / "layer1_meta_policy"
    result_cli = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-selector-low-turnover-ranking",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(cli_output_root),
            "--layer2-output-root",
            str(tmp_path / "cli_outputs" / "layer2_components"),
        ],
    )
    assert result_cli.exit_code == 0, result_cli.output
    assert (cli_output_root / "layer1_selector_low_turnover_ranking.json").exists()


def test_layer1_selector_low_turnover_final_gate_outputs_are_research_only(
    tmp_path: Path,
) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_layer1_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "layer1_meta_policy"
    layer2_output_root = tmp_path / "outputs" / "research_strategies" / "layer2_components"
    docs_root = tmp_path / "docs" / "research"
    common = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "as_of_date": as_of,
        "output_root": output_root,
        "layer2_output_root": layer2_output_root,
    }

    contract = run_layer1_selector_switch_count_threshold_contract(**common)
    search = run_layer1_selector_soft_blend_constrained_search(**common)
    monthly = run_layer1_selector_monthly_only_review(**common)
    hysteresis = run_layer1_selector_hysteresis_review(**common)
    switch_quality = run_layer1_selector_switch_quality_attribution(**common)
    finalist = run_layer1_selector_low_turnover_finalist_ranking(**common)
    final_gate = run_layer1_selector_vs_simple_components_final_gate(**common)
    watchlist = run_layer1_selector_forward_aging_watchlist_final_review(**common)
    owner = run_layer1_selector_pause_or_continue_owner_pack(
        **common,
        owner_doc_path=docs_root / "layer1_selector_pause_or_continue_owner_pack.md",
    )

    assert contract["status"] == "SWITCH_COUNT_CONTRACT_READY"
    assert contract["summary"]["max_switches_per_year"] == 2
    assert contract["summary"]["max_switches_per_3y"] == 6
    assert contract["summary"]["max_turnover_per_year"] == 1.0
    assert contract["summary"]["min_avg_holding_period"] == 60
    assert contract["summary"]["allowed_exception_cases"]
    assert search["status"] in {
        "SOFT_BLEND_CONSTRAINED_ACCEPTABLE_FOUND",
        "SOFT_BLEND_CONSTRAINED_SEARCH_REVIEWED",
    }
    assert len(search["soft_blend_constrained_search_rows"]) == 243
    first_search_row = search["soft_blend_constrained_search_rows"][0]
    assert {
        "net_return_after_cost",
        "max_drawdown",
        "calmar",
        "sharpe",
        "switch_count",
        "turnover",
        "avg_holding_period",
        "missed_rebound_cost",
        "late_risk_off_cost",
        "switch_count_controlled",
    } <= set(first_search_row)
    assert len(monthly["monthly_only_rows"]) == 3
    assert {
        "daily_signal_monthly_execution",
        "monthly_signal_monthly_execution",
        "threshold_signal_monthly_execution",
    } == {row["variant_id"] for row in monthly["monthly_only_rows"]}
    assert hysteresis["status"] == "HYSTERESIS_REVIEWED"
    assert {
        "switch_count_reduction",
        "chop_reduction",
        "late_risk_off_cost",
        "missed_rebound_cost",
    } <= set(hysteresis["hysteresis_rows"][0])
    if switch_quality["switch_quality_rows"]:
        assert {
            "switch_date",
            "from_state",
            "to_state",
            "outcome_20d_after_switch",
            "outcome_60d_after_switch",
            "switch_benefit_vs_not_switch",
            "turnover_cost",
            "net_switch_value",
        } <= set(switch_quality["switch_quality_rows"][0])
    assert finalist["status"] in {
        "LOW_TURNOVER_FINALIST_FOUND",
        "LOW_TURNOVER_NO_ACCEPTABLE_SELECTOR",
        "LOW_TURNOVER_INCONCLUSIVE",
    }
    assert {
        "original_trend_200dma_selector",
        "soft_blend_200dma_three_state",
        "monthly_soft_blend",
        "hysteresis_soft_blend",
        "confirmed_soft_blend",
        "min_holding_soft_blend",
    } == {row["variant_id"] for row in finalist["low_turnover_finalist_rows"]}
    assert final_gate["status"] in {
        "SELECTOR_FINAL_GATE_PASS",
        "SELECTOR_FINAL_GATE_FAIL_KEEP_DRY_RUN",
    }
    assert {
        "always_equal_risk",
        "always_100_qqq",
        "qqq_50_sgov_50",
        "qqq_60_sgov_40",
    } <= {row["variant_id"] for row in final_gate["selector_vs_simple_component_rows"]}
    assert watchlist["status"] in {
        "RESEARCH_ONLY_FORWARD_AGING_WATCHLIST_REVIEWABLE",
        "KEEP_SELECTOR_DRY_RUN_ONLY",
    }
    assert owner["status"] == "LAYER1_SELECTOR_PAUSE_OR_CONTINUE_OWNER_PACK_READY"
    assert len(owner["owner_questions"]) == 6
    assert (docs_root / "layer1_selector_pause_or_continue_owner_pack.md").exists()

    for payload in (
        contract,
        search,
        monthly,
        hysteresis,
        switch_quality,
        finalist,
        final_gate,
        watchlist,
        owner,
    ):
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["manual_review_required"] is True
        assert payload["summary"]["data_quality_status"]
        actual_range = payload["summary"]["actual_requested_date_range"]
        assert actual_range["start"] == date(2022, 12, 1).isoformat()
        assert actual_range["end"] == as_of.isoformat()
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()

    cli_output_root = tmp_path / "cli_outputs" / "layer1_meta_policy"
    result_cli = CliRunner().invoke(
        app,
        [
            "research",
            "strategies",
            "layer1-selector-pause-or-continue-owner-pack",
            "--prices-path",
            str(prices_path),
            "--marketstack-prices-path",
            str(marketstack_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            as_of.isoformat(),
            "--output-root",
            str(cli_output_root),
            "--layer2-output-root",
            str(tmp_path / "cli_outputs" / "layer2_components"),
        ],
    )
    assert result_cli.exit_code == 0, result_cli.output
    assert (cli_output_root / "layer1_selector_pause_or_continue_owner_pack.json").exists()


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_layer1_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 360)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0007 + 0.0030 * math.sin(day_index / 19.0)
        if 110 <= day_index <= 145:
            qqq_return -= 0.004
        if 190 <= day_index <= 240:
            qqq_return += 0.0025
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
