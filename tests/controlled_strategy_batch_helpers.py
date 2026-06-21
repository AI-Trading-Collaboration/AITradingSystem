from __future__ import annotations

# ruff: noqa: F401
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.controlled_strategy_batch import (
    run_ai_after_chatgpt_full_regime_attribution_review,
    run_benchmark_fallback_drawdown_guard_controlled_prototype,
    run_benchmark_first_tail_risk_policy_contract,
    run_conservative_horizon_risk_filter,
    run_controlled_strategy_batch_review,
    run_cost_aware_horizon_hysteresis,
    run_cost_turnover_aware_regime_conditioned_value_surface,
    run_forward_evidence_continuity_extension,
    run_forward_evidence_daily_continuity_maturity_tracker,
    run_forward_evidence_daily_continuity_review,
    run_forward_evidence_maturity_tracker,
    run_gbdt_action_utility_baseline,
    run_gbdt_pivot_direction_selection,
    run_gbdt_pivot_review,
    run_gbdt_residual_hypothesis_regime_conditioning,
    run_gbdt_residual_hypothesis_triage,
    run_gbdt_value_surface_residual_diagnostic_prototype,
    run_horizon_cliff_utility_ranking_stabilization_review,
    run_horizon_selector_controlled_prototype,
    run_horizon_selector_holdout_review,
    run_horizon_selector_problem_contract,
    run_long_horizon_quarantine_fallback_review,
    run_long_horizon_quarantine_selection_review,
    run_regime_conditioned_value_surface_controlled_review,
    run_regime_conditioned_value_surface_design,
    run_regime_conditioned_walk_forward_holdout,
    run_regime_horizon_loss_attribution_matrix,
    run_regret_activation_inputs_from_value_surface_failures,
    run_regret_casebook_activation_recheck,
    run_regret_casebook_expansion_gate,
    run_regret_state_machine_controlled_prototype,
    run_simple_strategy_selector_pilot,
    run_tail_loss_avoidance_classifier_prototype,
    run_tail_loss_guardrail_fallback_policy,
    run_tail_risk_benchmark_fallback_robustness_expansion,
    run_tail_risk_fallback_anti_leakage_audit,
    run_tail_risk_fallback_audit_universe_reconciliation,
    run_tail_risk_fallback_forward_maturity_scoreboard,
    run_tail_risk_fallback_regime_segmented_robustness,
    run_tail_risk_fallback_threshold_sensitivity,
    run_tail_risk_fallback_trigger_precision_recall_audit,
    run_tail_risk_forward_evidence_integration,
    run_tail_risk_opportunity_cost_upside_capture_review,
    run_tail_risk_policy_controlled_review_board,
    run_tail_risk_policy_family_controlled_review,
    run_utility_boundary_ranking_policy_audit,
    run_utility_ranking_robustness_pareto_audit,
    run_value_surface_controlled_expansion,
    run_value_surface_controlled_prototype,
    run_value_surface_controlled_walk_forward_expansion,
    run_value_surface_direction_review,
    run_value_surface_failure_attribution,
    run_value_surface_policy_kill_diagnostic_downgrade,
    run_value_surface_utility_pareto_ranking_review,
    run_value_surface_v2_controlled_review,
    run_value_surface_warning_triage_review,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path
from scripts.run_validation_tier import TIER_SPECS

TEST_AS_OF = date(2023, 5, 17)


def _run_next_stage_inputs(tmp_path: Path) -> dict[str, Path]:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    expansion = run_value_surface_controlled_expansion(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value_expansion",
        as_of_date=TEST_AS_OF,
    )
    utility = run_utility_boundary_ranking_policy_audit(
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "utility",
    )
    ledger_path = _write_forward_ledger(tmp_path)
    maturity = run_forward_evidence_maturity_tracker(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        ledger_path=ledger_path,
        value_surface_expansion_path=Path(expansion["artifact_paths"]["json_path"]),
        output_root=tmp_path / "maturity",
        as_of_date=TEST_AS_OF,
    )
    return {
        "prices": prices_path,
        "marketstack": marketstack_path,
        "rates": rates_path,
        "ledger": ledger_path,
        "value_expansion": Path(expansion["artifact_paths"]["json_path"]),
        "utility": Path(utility["artifact_paths"]["json_path"]),
        "maturity": Path(maturity["artifact_paths"]["json_path"]),
    }


def _run_direction_review_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_next_stage_inputs(tmp_path)
    warning = run_value_surface_warning_triage_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        forward_maturity_path=paths["maturity"],
        output_root=tmp_path / "direction_warning",
    )
    walk_forward = run_value_surface_controlled_walk_forward_expansion(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        warning_triage_path=Path(warning["artifact_paths"]["json_path"]),
        output_root=tmp_path / "direction_walk_forward",
        as_of_date=TEST_AS_OF,
    )
    utility_pareto = run_value_surface_utility_pareto_ranking_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_boundary_audit_path=paths["utility"],
        output_root=tmp_path / "direction_utility",
    )
    residual = run_gbdt_value_surface_residual_diagnostic_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        gbdt_pivot_selection_path=tmp_path / "missing_gbdt_pivot_selection.json",
        output_root=tmp_path / "direction_gbdt",
    )
    return {
        **paths,
        "warning": Path(warning["artifact_paths"]["json_path"]),
        "walk_forward": Path(walk_forward["artifact_paths"]["json_path"]),
        "utility_pareto": Path(utility_pareto["artifact_paths"]["json_path"]),
        "residual_diagnostic": Path(residual["artifact_paths"]["json_path"]),
    }


def _run_regime_conditioning_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_direction_review_inputs(tmp_path)
    failure = run_value_surface_failure_attribution(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        value_surface_expansion_path=paths["value_expansion"],
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "regime_failure",
        as_of_date=TEST_AS_OF,
    )
    horizon = run_horizon_cliff_utility_ranking_stabilization_review(
        value_surface_expansion_path=paths["value_expansion"],
        utility_pareto_ranking_path=paths["utility_pareto"],
        output_root=tmp_path / "regime_horizon",
    )
    residual_triage = run_gbdt_residual_hypothesis_triage(
        value_surface_expansion_path=paths["value_expansion"],
        residual_diagnostic_path=paths["residual_diagnostic"],
        output_root=tmp_path / "regime_residual_triage",
    )
    forward = run_forward_evidence_continuity_extension(
        prices_path=paths["prices"],
        marketstack_prices_path=paths["marketstack"],
        rates_path=paths["rates"],
        ledger_path=paths["ledger"],
        value_surface_expansion_path=paths["value_expansion"],
        output_root=tmp_path / "regime_forward",
        as_of_date=TEST_AS_OF,
    )
    direction = run_value_surface_direction_review(
        failure_attribution_path=Path(failure["artifact_paths"]["json_path"]),
        horizon_stabilization_path=Path(horizon["artifact_paths"]["json_path"]),
        residual_triage_path=Path(residual_triage["artifact_paths"]["json_path"]),
        forward_continuity_extension_path=Path(forward["artifact_paths"]["json_path"]),
        walk_forward_path=paths["walk_forward"],
        output_root=tmp_path / "regime_direction",
    )
    return {
        **paths,
        "failure": Path(failure["artifact_paths"]["json_path"]),
        "horizon": Path(horizon["artifact_paths"]["json_path"]),
        "residual_triage": Path(residual_triage["artifact_paths"]["json_path"]),
        "forward": Path(forward["artifact_paths"]["json_path"]),
        "direction": Path(direction["artifact_paths"]["json_path"]),
    }


def _run_value_surface_v2_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_regime_conditioning_inputs(tmp_path)
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


def _run_value_surface_v2_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_value_surface_v2_inputs(tmp_path)
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


def _run_horizon_selector_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_value_surface_v2_full_inputs(tmp_path)
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
    contract = run_horizon_selector_problem_contract(
        v2_review_path=Path(v2_review["artifact_paths"]["json_path"]),
        long_horizon_review_path=paths["long_horizon"],
        output_root=tmp_path / "horizon_contract",
    )
    fallback = run_long_horizon_quarantine_fallback_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=Path(contract["artifact_paths"]["json_path"]),
        v2_review_path=Path(v2_review["artifact_paths"]["json_path"]),
        output_root=tmp_path / "horizon_fallback",
    )
    return {
        **paths,
        "v2_holdout": Path(holdout["artifact_paths"]["json_path"]),
        "v2_review": Path(v2_review["artifact_paths"]["json_path"]),
        "contract": Path(contract["artifact_paths"]["json_path"]),
        "fallback_review": Path(fallback["artifact_paths"]["json_path"]),
    }


def _run_horizon_selector_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_horizon_selector_inputs(tmp_path)
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


def _run_tail_risk_policy_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_horizon_selector_full_inputs(tmp_path)
    horizon_holdout = run_horizon_selector_holdout_review(
        value_surface_expansion_path=paths["value_expansion"],
        contract_path=paths["contract"],
        fallback_review_path=paths["fallback_review"],
        prototype_path=paths["prototype"],
        hysteresis_path=paths["hysteresis"],
        horizon_stabilization_path=paths["horizon"],
        output_root=tmp_path / "tail_horizon_holdout",
    )
    policy_kill = run_value_surface_policy_kill_diagnostic_downgrade(
        horizon_selector_holdout_path=Path(horizon_holdout["artifact_paths"]["json_path"]),
        v2_review_path=paths["v2_review"],
        output_root=tmp_path / "tail_policy_kill",
    )
    contract = run_benchmark_first_tail_risk_policy_contract(
        policy_kill_path=Path(policy_kill["artifact_paths"]["json_path"]),
        output_root=tmp_path / "tail_contract",
    )
    classifier = run_tail_loss_avoidance_classifier_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        policy_kill_path=Path(policy_kill["artifact_paths"]["json_path"]),
        output_root=tmp_path / "tail_classifier",
    )
    return {
        **paths,
        "horizon_contract": paths["contract"],
        "horizon_holdout": Path(horizon_holdout["artifact_paths"]["json_path"]),
        "policy_kill": Path(policy_kill["artifact_paths"]["json_path"]),
        "contract": Path(contract["artifact_paths"]["json_path"]),
        "classifier": Path(classifier["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_policy_full_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_tail_risk_policy_inputs(tmp_path)
    horizon_filter = run_conservative_horizon_risk_filter(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        contract_path=paths["contract"],
        output_root=tmp_path / "tail_horizon_filter",
    )
    fallback = run_benchmark_fallback_drawdown_guard_controlled_prototype(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        horizon_filter_path=Path(horizon_filter["artifact_paths"]["json_path"]),
        contract_path=paths["contract"],
        output_root=tmp_path / "tail_fallback",
    )
    return {
        **paths,
        "horizon_filter": Path(horizon_filter["artifact_paths"]["json_path"]),
        "fallback": Path(fallback["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_robustness_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_tail_risk_policy_full_inputs(tmp_path)
    robustness = run_tail_risk_benchmark_fallback_robustness_expansion(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        fallback_path=paths["fallback"],
        output_root=tmp_path / "tail_robustness",
    )
    return {
        **paths,
        "robustness": Path(robustness["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_review_board_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_tail_risk_robustness_inputs(tmp_path)
    precision = run_tail_risk_fallback_trigger_precision_recall_audit(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_precision",
    )
    opportunity = run_tail_risk_opportunity_cost_upside_capture_review(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_opportunity",
    )
    forward = run_tail_risk_forward_evidence_integration(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        ledger_path=tmp_path / "tail_forward" / "ledger.jsonl",
        output_root=tmp_path / "tail_forward",
        as_of_date=TEST_AS_OF,
    )
    return {
        **paths,
        "precision": Path(precision["artifact_paths"]["json_path"]),
        "opportunity": Path(opportunity["artifact_paths"]["json_path"]),
        "forward": Path(forward["artifact_paths"]["json_path"]),
    }


def _run_tail_risk_falsification_inputs(tmp_path: Path) -> dict[str, Path]:
    paths = _run_tail_risk_review_board_inputs(tmp_path)
    reconciliation = run_tail_risk_fallback_audit_universe_reconciliation(
        robustness_path=paths["robustness"],
        precision_recall_path=paths["precision"],
        opportunity_cost_path=paths["opportunity"],
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "tail_reconciliation",
    )
    anti_leakage = run_tail_risk_fallback_anti_leakage_audit(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_anti_leakage",
    )
    sensitivity = run_tail_risk_fallback_threshold_sensitivity(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_sensitivity",
    )
    regime = run_tail_risk_fallback_regime_segmented_robustness(
        value_surface_expansion_path=paths["value_expansion"],
        classifier_path=paths["classifier"],
        robustness_path=paths["robustness"],
        output_root=tmp_path / "tail_regime",
    )
    scoreboard = run_tail_risk_fallback_forward_maturity_scoreboard(
        forward_integration_path=paths["forward"],
        output_root=tmp_path / "tail_scoreboard",
        as_of_date=TEST_AS_OF,
    )
    return {
        **paths,
        "reconciliation": Path(reconciliation["artifact_paths"]["json_path"]),
        "anti_leakage": Path(anti_leakage["artifact_paths"]["json_path"]),
        "sensitivity": Path(sensitivity["artifact_paths"]["json_path"]),
        "regime": Path(regime["artifact_paths"]["json_path"]),
        "scoreboard": Path(scoreboard["artifact_paths"]["json_path"]),
    }


def _run_candidate_batch(tmp_path: Path) -> dict[str, Path]:
    prices_path, marketstack_path, rates_path = _write_price_caches(tmp_path)
    value = run_value_surface_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "value",
        as_of_date=TEST_AS_OF,
    )
    state = run_regret_state_machine_controlled_prototype(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "state",
        as_of_date=TEST_AS_OF,
    )
    simple = run_simple_strategy_selector_pilot(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "simple",
        as_of_date=TEST_AS_OF,
    )
    gbdt = run_gbdt_action_utility_baseline(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "gbdt",
        as_of_date=TEST_AS_OF,
    )
    _assert_safety(value)
    _assert_safety(state)
    _assert_safety(simple)
    _assert_safety(gbdt)
    return {
        "value_surface": tmp_path / "value" / "value_surface_controlled_prototype.json",
        "state_machine": tmp_path / "state" / "regret_state_machine_controlled_prototype.json",
        "simple": tmp_path / "simple" / "simple_strategy_selector_pilot.json",
        "gbdt": tmp_path / "gbdt" / "gbdt_action_utility_baseline.json",
    }


def _write_price_caches(tmp_path: Path) -> tuple[Path, Path, Path]:
    universe = ["SPY", "QQQ", "SMH", "MSFT", "GOOGL", "NVDA", "AMD", "TSM"]
    dates = _business_dates(date(2022, 12, 1), 120)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    for ticker_index, ticker in enumerate(universe):
        base = 100.0 + ticker_index * 7.0
        for day_index, row_date in enumerate(dates):
            trend = 1.0 + day_index * 0.0015
            cycle = (day_index % 9 - 4) * 0.001
            drawdown = -0.05 if 45 <= day_index <= 55 else 0.0
            close = base * (trend + cycle + drawdown)
            row = (
                f"{row_date.isoformat()},{ticker},{close - 0.5:.4f},{close + 0.5:.4f},"
                f"{close - 1.0:.4f},{close:.4f},{close:.4f},{1000000 + ticker_index}\n"
            )
            price_rows.append(row)
            secondary_rows.append(row)
    rate_rows = ["date,series,value\n"]
    for day_index, row_date in enumerate(dates):
        rate_rows.append(f"{row_date.isoformat()},DGS10,{3.5 + day_index * 0.001:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS2,{3.0 + day_index * 0.001:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DTWEXBGS,{120.0 + day_index * 0.01:.4f}\n")
    prices_path.write_text("".join(price_rows), encoding="utf-8")
    marketstack_path.write_text("".join(secondary_rows), encoding="utf-8")
    rates_path.write_text("".join(rate_rows), encoding="utf-8")
    return prices_path, marketstack_path, rates_path


def _business_dates(start: date, count: int) -> list[date]:
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values


def _write_forward_ledger(tmp_path: Path) -> Path:
    archive_path = tmp_path / "forward_evidence_dry_run_2023-02-01.json"
    _write_json(
        archive_path,
        {
            "report_type": "forward_evidence_daily_dry_run_archive",
            "status": "PASS_WITH_WARNINGS",
            "as_of": "2023-02-01",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
        },
    )
    ledger_path = tmp_path / "forward_evidence_dry_run_ledger.jsonl"
    rows = [
        {
            "archive_id": "forward_evidence_dry_run:2023-02-01",
            "archive_path": str(archive_path),
            "as_of": "2023-02-01",
            "outcome_append_only": True,
            "outcome_status": "pending",
            "production_effect": "none",
            "broker_action": "none",
            "promotion_gate_allowed": False,
            "paper_shadow_change_allowed": False,
            "production_weight_change_allowed": False,
        }
    ]
    ledger_path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n",
        encoding="utf-8",
    )
    return ledger_path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _assert_safety(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_gate_allowed"] is False
    assert payload["paper_shadow_change_allowed"] is False
    assert payload["production_weight_change_allowed"] is False
    assert payload["lookahead_violation_count"] == 0
