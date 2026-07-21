from __future__ import annotations

import math
from datetime import date, timedelta
from pathlib import Path

import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.execution_semantics import (
    DEFAULT_EXECUTION_POLICY_REGISTRY_PATH,
    EXECUTION_SEMANTICS_REPORT_SPECS,
    REQUIRED_EXECUTION_POLICY_FIELDS,
    run_dynamic_backtest_engine_contract_update,
    run_dynamic_strategy_execution_semantics_contract,
    run_dynamic_strategy_latency_execution_lag_review,
    run_dynamic_strategy_validity_period_audit,
    run_equal_risk_balanced_core_execution_policy_selection,
    run_execution_aware_forward_aging_observation_contract,
    run_execution_policy_cost_turnover_normalization,
    run_execution_policy_impact_on_prior_conclusions,
    run_execution_semantics_data_lineage_audit,
    run_execution_semantics_external_validation_update,
    run_execution_semantics_master_review,
    run_execution_semantics_reporting_update,
    run_implicit_monthly_rebalance_assumption_audit,
    run_reader_brief_execution_semantics_safe_preview,
    run_rebalance_assumption_owner_review_pack,
    run_rebalance_frequency_sensitivity_suite,
    run_rebalance_sensitive_candidate_recovery_review,
    run_roadmap_update_after_execution_semantics_review,
    run_signal_staleness_cost_review,
    run_strategy_execution_policy_registry_review,
    run_target_vs_actual_position_path_builder,
    run_threshold_hybrid_rebalance_review,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)


def test_execution_policy_registry_contract() -> None:
    registry = yaml.safe_load(DEFAULT_EXECUTION_POLICY_REGISTRY_PATH.read_text(encoding="utf-8"))
    policies = registry["policies"]
    policy_ids = {policy["execution_policy_id"] for policy in policies}

    assert registry["policy_id"] == "strategy_execution_policy_registry_v1"
    assert registry["safety_boundary"]["paper_shadow_allowed"] is False
    assert registry["safety_boundary"]["production_allowed"] is False
    assert registry["safety_boundary"]["broker_action"] == "none"
    assert registry["market_regime"]["default_backtest_start"] == date(2021, 2, 22)
    assert {
        "monthly_eom_v1",
        "monthly_bom_v1",
        "weekly_friday_v1",
        "daily_close_next_day_v1",
        "threshold_drift_5pct_v1",
        "threshold_drift_10pct_v1",
        "monthly_plus_threshold_5pct_v1",
        "monthly_plus_threshold_10pct_v1",
        "monthly_plus_vol_shock_v1",
        "monthly_plus_drawdown_shock_v1",
        "validity_5d_v1",
        "validity_10d_v1",
        "validity_20d_v1",
        "min_holding_20d_v1",
        "hysteresis_band_v1",
    } <= policy_ids
    for policy in policies:
        assert set(REQUIRED_EXECUTION_POLICY_FIELDS) <= set(policy)
        metadata = policy["policy_metadata"]
        assert metadata["owner"]
        assert metadata["rationale"]
        assert metadata["review_condition"]


def test_execution_semantics_builders_preserve_safety_and_paths(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "execution_semantics"
    docs_path = tmp_path / "docs" / "research" / "rebalance_assumption_owner_review_pack.md"
    data_kwargs = {
        "prices_path": prices_path,
        "marketstack_prices_path": marketstack_path,
        "rates_path": rates_path,
        "output_root": output_root,
        "as_of_date": as_of,
    }

    payloads = [
        run_dynamic_strategy_execution_semantics_contract(output_root=output_root),
        run_implicit_monthly_rebalance_assumption_audit(output_root=output_root),
        run_strategy_execution_policy_registry_review(output_root=output_root),
        run_dynamic_strategy_validity_period_audit(output_root=output_root),
        run_target_vs_actual_position_path_builder(**data_kwargs),
        run_rebalance_frequency_sensitivity_suite(**data_kwargs),
        run_threshold_hybrid_rebalance_review(**data_kwargs),
        run_signal_staleness_cost_review(**data_kwargs),
        run_dynamic_strategy_latency_execution_lag_review(**data_kwargs),
        run_execution_policy_impact_on_prior_conclusions(output_root=output_root),
        run_rebalance_sensitive_candidate_recovery_review(output_root=output_root),
        run_execution_semantics_data_lineage_audit(output_root=output_root),
        run_execution_policy_cost_turnover_normalization(**data_kwargs),
        run_execution_semantics_external_validation_update(output_root=output_root),
        run_execution_aware_forward_aging_observation_contract(output_root=output_root),
        run_equal_risk_balanced_core_execution_policy_selection(output_root=output_root),
        run_dynamic_backtest_engine_contract_update(output_root=output_root),
        run_execution_semantics_reporting_update(output_root=output_root),
        run_rebalance_assumption_owner_review_pack(
            output_root=output_root,
            docs_path=docs_path,
        ),
        run_execution_semantics_master_review(output_root=output_root),
        run_roadmap_update_after_execution_semantics_review(output_root=output_root),
        run_reader_brief_execution_semantics_safe_preview(output_root=output_root),
    ]

    target_actual = next(
        payload
        for payload in payloads
        if payload["report_type"] == "target_vs_actual_position_path_builder"
    )
    sensitivity = next(
        payload
        for payload in payloads
        if payload["report_type"] == "rebalance_frequency_sensitivity_suite"
    )
    registry_review = next(
        payload
        for payload in payloads
        if payload["report_type"] == "strategy_execution_policy_registry_review"
    )
    master = next(
        payload
        for payload in payloads
        if payload["report_type"] == "execution_semantics_master_review"
    )
    reader_preview = next(
        payload
        for payload in payloads
        if payload["report_type"] == "reader_brief_execution_semantics_safe_preview"
    )

    assert registry_review["status"] == "EXECUTION_POLICY_REGISTRY_READY"
    assert target_actual["status"] == "TARGET_ACTUAL_PATH_READY"
    assert target_actual["data_quality"]["passed"] is True
    assert target_actual["path_rows"]
    assert any(row["rebalance_executed"] for row in target_actual["path_rows"])
    first_row = target_actual["path_rows"][0]
    assert {
        "target_weight_qqq",
        "target_weight_tqqq",
        "target_weight_sgov",
        "actual_weight_qqq",
        "actual_weight_tqqq",
        "actual_weight_sgov",
        "execution_policy_id",
    } <= set(first_row)
    assert sensitivity["summary"]["strategy_count"] == 5
    assert sensitivity["summary"]["policy_count"] >= 10
    assert master["status"] == "EXECUTION_SEMANTICS_REQUIRES_REBACKTEST"
    assert reader_preview["status"] == "EXECUTION_READER_PREVIEW_SAFE"
    assert reader_preview["prohibited_phrase_hits"] == []
    assert docs_path.exists()

    for payload in payloads:
        _assert_execution_semantics_payload(payload)


def test_execution_semantics_cli_and_report_registry(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_execution_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "execution_semantics"
    runner = CliRunner()

    result = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "target-vs-actual-position-path-builder",
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
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert (output_root / "target_vs_actual_position_path_builder.json").exists()
    assert "paper_shadow_allowed=False" in result.output

    master = runner.invoke(
        app,
        [
            "research",
            "strategies",
            "execution-semantics-master-review",
            "--output-root",
            str(output_root),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert master.exit_code == 0, master.output
    assert (output_root / "execution_semantics_master_review.json").exists()

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    entries = {item["report_id"]: item for item in registry["reports"]}
    report_ids = {item["report_id"] for item in EXECUTION_SEMANTICS_REPORT_SPECS}
    assert report_ids <= set(entries)
    for report_id in report_ids:
        entry = entries[report_id]
        assert entry["command"].startswith("aits research strategies ")
        assert entry["artifact_selection_policy"] == "latest_available"
        assert entry["required_for_daily_reading"] is False
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"


def _assert_execution_semantics_payload(payload: dict[str, object]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["promotion_allowed"] is False
    assert payload["paper_shadow_allowed"] is False
    assert payload["production_allowed"] is False
    assert payload["manual_review_required"] is True
    assert payload["market_regime"] == "unified_primary_2021"
    assert Path(payload["artifact_paths"]["json_path"]).exists()
    assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def _write_execution_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 760)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}

    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0007 + 0.0018 * math.sin(day_index / 19.0)
        if 80 <= day_index <= 125:
            qqq_return -= 0.006
        if 126 <= day_index <= 190:
            qqq_return += 0.004
        if 430 <= day_index <= 470:
            qqq_return -= 0.004
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= max(0.01, 1.0 + qqq_return * 3.0 - 0.00025)
        levels["SGOV"] *= 1.0 + 0.00016
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
        rate_rows.append(f"{row_date.isoformat()},DGS2,{4.0 + day_index * 0.0004:.4f}\n")
        rate_rows.append(f"{row_date.isoformat()},DGS10,{4.2 + day_index * 0.0003:.4f}\n")
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
