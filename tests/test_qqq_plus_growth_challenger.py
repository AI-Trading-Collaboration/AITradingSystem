from __future__ import annotations

import json
import math
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.qqq_plus_growth_challenger import (
    run_controlled_tqqq_overlay_search,
    run_drawdown_guarded_growth_policy_search,
    run_growth_candidate_forward_aging_watchlist,
    run_growth_edge_significance_review,
    run_growth_vs_defensive_role_allocation_review,
    run_qqq_outperformance_drawdown_replay,
    run_qqq_outperformance_objective_contract,
    run_qqq_outperformance_owner_decision_pack,
    run_qqq_outperformance_period_split_validation,
    run_qqq_outperformance_ranking_report,
    run_qqq_plus_growth_candidate_registry,
    run_qqq_plus_risk_budget_review,
    run_trend_gated_leverage_policy_search,
    run_volatility_targeted_growth_policy_search,
)
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)

QQQ_PLUS_REPORT_IDS = {
    "qqq_outperformance_objective_contract",
    "qqq_plus_growth_candidate_registry",
    "controlled_tqqq_overlay_search",
    "trend_gated_leverage_policy_search",
    "volatility_targeted_growth_policy_search",
    "drawdown_guarded_growth_policy_search",
    "qqq_outperformance_ranking_report",
    "qqq_outperformance_period_split_validation",
    "qqq_outperformance_drawdown_replay",
    "growth_edge_significance_review",
    "growth_candidate_forward_aging_watchlist",
    "qqq_plus_risk_budget_review",
    "growth_vs_defensive_role_allocation_review",
    "qqq_outperformance_owner_decision_pack",
}


def test_qqq_plus_growth_builders_write_auditable_artifacts(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_qqq_growth_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "qqq_plus_growth"
    owner_docs_path = tmp_path / "docs" / "research" / "qqq_owner.md"

    objective = run_qqq_outperformance_objective_contract(output_root=output_root)
    registry = run_qqq_plus_growth_candidate_registry(output_root=output_root)
    controlled = run_controlled_tqqq_overlay_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    trend = run_trend_gated_leverage_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    volatility = run_volatility_targeted_growth_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    drawdown_guarded = run_drawdown_guarded_growth_policy_search(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    ranking = run_qqq_outperformance_ranking_report(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    period = run_qqq_outperformance_period_split_validation(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    replay = run_qqq_outperformance_drawdown_replay(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    edge = run_growth_edge_significance_review(output_root=output_root)
    watchlist = run_growth_candidate_forward_aging_watchlist(output_root=output_root)
    risk = run_qqq_plus_risk_budget_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=output_root,
        as_of_date=as_of,
    )
    role = run_growth_vs_defensive_role_allocation_review(output_root=output_root)
    owner = run_qqq_outperformance_owner_decision_pack(
        output_root=output_root,
        docs_path=owner_docs_path,
    )

    payloads = [
        objective,
        registry,
        controlled,
        trend,
        volatility,
        drawdown_guarded,
        ranking,
        period,
        replay,
        edge,
        watchlist,
        risk,
        role,
        owner,
    ]

    assert {payload["report_type"] for payload in payloads} == QQQ_PLUS_REPORT_IDS
    assert objective["status"] == "QQQ_OUTPERFORMANCE_CONTRACT_READY"
    assert registry["status"] == "QQQ_PLUS_GROWTH_REGISTRY_READY"
    safety_boundary = registry["registry_policy"]["safety_boundary"]
    assert safety_boundary["max_tqqq_weight"] == 0.40
    assert safety_boundary["max_effective_qqq_exposure"] == 1.80
    assert controlled["status"] == "CONTROLLED_TQQQ_OVERLAY_SEARCH_READY"
    assert trend["status"] == "TREND_GATED_LEVERAGE_SEARCH_READY"
    assert volatility["status"] == "VOL_TARGETED_GROWTH_SEARCH_READY"
    assert drawdown_guarded["status"] == "DRAWDOWN_GUARDED_GROWTH_SEARCH_READY"
    assert "100_qqq" in ranking["comparison_objects"]
    assert "equal_risk_qqq_sgov" in ranking["comparison_objects"]
    assert period["status"] in {
        "QQQ_OUTPERFORMANCE_PERIOD_SPLIT_PASS",
        "QQQ_OUTPERFORMANCE_NOT_STABLE",
        "REGIME_CONCENTRATED",
    }
    assert replay["status"] in {
        "QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_READY",
        "QQQ_OUTPERFORMANCE_DRAWDOWN_REPLAY_MIXED",
    }
    assert edge["status"] in {
        "GROWTH_EDGE_MATERIAL",
        "GROWTH_EDGE_REGIME_CONCENTRATED",
        "GROWTH_EDGE_WEAK",
    }
    assert watchlist["summary"]["paper_shadow_allowed"] is False
    assert risk["status"] in {"QQQ_PLUS_RISK_BUDGET_READY", "QQQ_PLUS_RISK_BUDGET_BETA_HEAVY"}
    assert role["summary"]["defensive_primary"] == "equal_risk_qqq_sgov"
    assert owner["production_allowed"] is False
    assert owner_docs_path.exists()

    data_dependent = [
        controlled,
        trend,
        volatility,
        drawdown_guarded,
        ranking,
        period,
        replay,
        risk,
    ]
    for payload in data_dependent:
        assert payload["data_quality"]["passed"] is True
        assert payload["summary"]["data_quality_status"] in {"PASS", "PASS_WITH_WARNINGS"}

    for payload in payloads:
        assert payload["production_effect"] == "none"
        assert payload["broker_action"] == "none"
        assert payload["promotion_allowed"] is False
        assert payload["paper_shadow_allowed"] is False
        assert payload["production_allowed"] is False
        assert payload["report_registry_entry"]["report_id"] == payload["report_type"]
        assert Path(payload["artifact_paths"]["json_path"]).exists()
        assert Path(payload["artifact_paths"]["markdown_path"]).exists()


def test_qqq_plus_growth_cli_smoke_and_report_registry(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_qqq_growth_caches(tmp_path)
    output_root = tmp_path / "outputs" / "research_strategies" / "qqq_plus_growth"
    docs_path = tmp_path / "docs" / "research" / "qqq_owner.md"
    runner = CliRunner()

    data_args = [
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
    ]
    commands = [
        [
            "research",
            "strategies",
            "qqq-outperformance-objective-contract",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "qqq-plus-growth-candidate-registry",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "controlled-tqqq-overlay-search", *data_args],
        ["research", "strategies", "trend-gated-leverage-policy-search", *data_args],
        ["research", "strategies", "volatility-targeted-growth-policy-search", *data_args],
        ["research", "strategies", "drawdown-guarded-growth-policy-search", *data_args],
        ["research", "strategies", "qqq-outperformance-ranking-report", *data_args],
        ["research", "strategies", "qqq-outperformance-period-split-validation", *data_args],
        ["research", "strategies", "qqq-outperformance-drawdown-replay", *data_args],
        [
            "research",
            "strategies",
            "growth-edge-significance-review",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "growth-candidate-forward-aging-watchlist",
            "--output-root",
            str(output_root),
        ],
        ["research", "strategies", "qqq-plus-risk-budget-review", *data_args],
        [
            "research",
            "strategies",
            "growth-vs-defensive-role-allocation-review",
            "--output-root",
            str(output_root),
        ],
        [
            "research",
            "strategies",
            "qqq-outperformance-owner-decision-pack",
            "--output-root",
            str(output_root),
            "--docs-path",
            str(docs_path),
        ],
    ]
    for command in commands:
        result = runner.invoke(app, command)
        assert result.exit_code == 0, result.output

    owner_payload = json.loads(
        (output_root / "qqq_outperformance_owner_decision_pack.json").read_text(
            encoding="utf-8"
        )
    )
    assert owner_payload["production_effect"] == "none"
    assert owner_payload["broker_action"] == "none"
    assert docs_path.exists()

    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    registered = {item["report_id"] for item in registry["reports"]}
    assert QQQ_PLUS_REPORT_IDS <= registered
    for report_id in QQQ_PLUS_REPORT_IDS:
        entry = next(item for item in registry["reports"] if item["report_id"] == report_id)
        assert entry["production_effect"] == "none"
        assert entry["broker_action"] == "none"
        assert entry["include_in_reader_brief"] is False
        assert entry["required_for_daily_reading"] is False


def _write_qqq_growth_caches(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = _business_dates(date(2022, 12, 1), 690)
    prices_path = tmp_path / "prices_daily.csv"
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    price_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    secondary_rows = ["date,ticker,open,high,low,close,adj_close,volume\n"]
    levels = {"QQQ": 280.0, "TQQQ": 22.0, "SGOV": 100.0}
    for day_index, row_date in enumerate(dates):
        qqq_return = 0.0009 + 0.0022 * math.sin(day_index / 19.0)
        if 40 <= day_index <= 65:
            qqq_return -= 0.006
        if 250 <= day_index <= 280:
            qqq_return -= 0.004
        if 430 <= day_index <= 455:
            qqq_return -= 0.003
        levels["QQQ"] *= 1.0 + qqq_return
        levels["TQQQ"] *= 1.0 + qqq_return * 3.0 - 0.00025
        levels["SGOV"] *= 1.0 + 0.00016
        for ticker in ("QQQ", "TQQQ", "SGOV"):
            close = levels[ticker]
            adj_close = close + (0.01 if ticker == "SGOV" else 0.0)
            row = (
                f"{row_date.isoformat()},{ticker},{close * 0.999:.4f},"
                f"{close * 1.002:.4f},{close * 0.998:.4f},{close:.4f},"
                f"{adj_close:.4f},{1000000 + day_index}\n"
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
    values: list[date] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current)
        current += timedelta(days=1)
    return values
