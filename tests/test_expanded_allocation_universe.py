from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.expanded_allocation_universe import (
    classify_monotonic_risk_profile,
    run_expanded_actual_path_rebacktest,
    run_expanded_universe_owner_review_pack,
    run_risk_bucket_representatives,
    run_state_portfolio_candidates,
    run_static_frontier_review,
    run_static_simplex_grid,
    run_tqqq_data_quality_blocking_review,
)


def test_static_simplex_grid_generates_frontier_and_bucket_summary(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_market_cache(tmp_path)

    payload = run_static_simplex_grid(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "static_grid",
        step=0.5,
        as_of_date=as_of,
    )

    assert payload["status"] == "STATIC_SIMPLEX_GRID_READY"
    assert payload["summary"]["static_grid_size"] == 6
    metrics = pd.read_csv(tmp_path / "static_grid" / "static_simplex_grid_metrics.csv")
    frontier = pd.read_csv(tmp_path / "static_grid" / "static_simplex_grid_pareto_frontier.csv")
    buckets = pd.read_csv(
        tmp_path / "static_grid" / "static_simplex_grid_risk_bucket_summary.csv"
    )
    assert {"QQQ", "SGOV", "TQQQ"} == set(payload["data_quality"]["expected_price_tickers"])
    assert metrics["risk_bucket"].notna().all()
    assert "qqq_equivalent_exposure" in metrics.columns
    assert not frontier.empty
    assert not buckets.empty

    review = run_static_frontier_review(
        static_grid_root=tmp_path / "static_grid",
        docs_path=tmp_path / "frontier.md",
        yaml_path=tmp_path / "frontier.yaml",
    )
    assert review["status"] == "STATIC_FRONTIER_REVIEW_READY"
    assert (tmp_path / "frontier.yaml").exists()


def test_representatives_state_candidates_and_monotonic_classifier(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_market_cache(tmp_path)
    run_static_simplex_grid(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=tmp_path / "static_grid",
        step=0.25,
        as_of_date=as_of,
    )

    reps = run_risk_bucket_representatives(
        static_grid_root=tmp_path / "static_grid",
        output_path=tmp_path / "risk_bucket_representatives.csv",
    )
    assert reps["status"] == "RISK_BUCKET_REPRESENTATIVES_READY"
    assert reps["summary"]["representative_count"] > 0

    candidates = run_state_portfolio_candidates(
        representatives_path=tmp_path / "risk_bucket_representatives.csv",
        output_path=tmp_path / "state_portfolio_candidates.json",
    )
    assert candidates["status"] == "STATE_PORTFOLIO_CANDIDATES_READY"
    assert candidates["summary"]["candidate_count"] > 0
    assert all(candidate["promotion_allowed"] is False for candidate in candidates["candidates"])

    classifier = classify_monotonic_risk_profile(
        {
            "risk_off": {"QQQ": 0.2, "SGOV": 0.8, "TQQQ": 0.0},
            "defensive": {"QQQ": 0.4, "SGOV": 0.6, "TQQQ": 0.0},
            "neutral": {"QQQ": 0.6, "SGOV": 0.4, "TQQQ": 0.0},
            "constructive": {"QQQ": 0.7, "SGOV": 0.25, "TQQQ": 0.05},
            "risk_on": {"QQQ": 0.7, "SGOV": 0.15, "TQQQ": 0.15},
        },
        states=("risk_off", "defensive", "neutral", "constructive", "risk_on"),
    )
    assert classifier["monotonic_risk_profile"] is True
    assert classifier["risk_exposure_by_state"]["risk_on"] > 1.0


def test_actual_path_rebacktest_and_owner_pack_keep_promotion_blocked(tmp_path: Path) -> None:
    prices_path, marketstack_path, rates_path, as_of = _write_market_cache(tmp_path)
    static_root = tmp_path / "static_grid"
    candidates_path = tmp_path / "state_portfolio_candidates.json"
    run_static_simplex_grid(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        output_root=static_root,
        step=0.25,
        as_of_date=as_of,
    )
    run_risk_bucket_representatives(
        static_grid_root=static_root,
        output_path=tmp_path / "risk_bucket_representatives.csv",
    )
    run_state_portfolio_candidates(
        representatives_path=tmp_path / "risk_bucket_representatives.csv",
        output_path=candidates_path,
    )
    tqqq_review = run_tqqq_data_quality_blocking_review(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        yaml_path=tmp_path / "tqqq_review.yaml",
        docs_path=tmp_path / "tqqq_review.md",
        as_of_date=as_of,
    )
    assert tqqq_review["status"] == "TQQQ_RESEARCH_ONLY_APPROVED"
    assert tqqq_review["promotion_universe_status"] == (
        "TQQQ_DATA_QUALITY_BLOCKING_REVIEW_REQUIRED"
    )

    actual = run_expanded_actual_path_rebacktest(
        prices_path=prices_path,
        marketstack_prices_path=marketstack_path,
        rates_path=rates_path,
        static_grid_root=static_root,
        candidates_path=candidates_path,
        output_root=tmp_path / "actual_path",
        as_of_date=as_of,
    )

    assert actual["status"] == "EXPANDED_UNIVERSE_RESEARCH_READY_PROMOTION_BLOCKED"
    assert actual["dynamic_promotion"]["final_status"] == "BLOCKED"
    leaderboard = pd.read_csv(tmp_path / "actual_path" / "leaderboard_actual_path.csv")
    target = pd.read_csv(tmp_path / "actual_path" / "leaderboard_target_path_diagnostic.csv")
    readiness = (tmp_path / "actual_path" / "expanded_universe_promotion_readiness.json")
    assert not leaderboard.empty
    assert not target.empty
    assert readiness.exists()
    assert "actual_path_annual_return" in leaderboard.columns
    assert "target_path_annual_return" in target.columns

    owner = run_expanded_universe_owner_review_pack(
        static_grid_root=static_root,
        actual_path_root=tmp_path / "actual_path",
        tqqq_review_yaml_path=tmp_path / "tqqq_review.yaml",
        owner_doc_path=tmp_path / "owner_pack.md",
        tqqq_attribution_doc_path=tmp_path / "tqqq_attr.md",
        tqqq_attribution_yaml_path=tmp_path / "tqqq_attr.yaml",
        same_risk_yaml_path=tmp_path / "same_risk.yaml",
        survival_doc_path=tmp_path / "survival.md",
        survival_yaml_path=tmp_path / "survival.yaml",
        walk_forward_doc_path=tmp_path / "walk_forward.md",
        walk_forward_yaml_path=tmp_path / "walk_forward.yaml",
        net_cost_doc_path=tmp_path / "net_cost.md",
        stress_doc_path=tmp_path / "stress.md",
    )
    assert owner["status"] == "EXPANDED_OWNER_REVIEW_PACK_READY_PROMOTION_BLOCKED"
    assert owner["dynamic_promotion"]["final_status"] == "BLOCKED"
    assert (tmp_path / "survival.yaml").exists()
    assert "promotion_status：`BLOCKED`" in (tmp_path / "owner_pack.md").read_text(
        encoding="utf-8"
    )


def test_expanded_universe_cli_is_registered() -> None:
    runner = CliRunner()
    result = runner.invoke(
        app,
        ["research", "strategies", "expanded-universe", "--help"],
        env={"COLUMNS": "180"},
        terminal_width=180,
    )
    assert result.exit_code == 0, result.output
    assert "static-simplex-grid" in result.output
    assert "actual-path-rebacktest" in result.output


def _write_market_cache(tmp_path: Path) -> tuple[Path, Path, Path, date]:
    dates = pd.bdate_range("2022-12-01", periods=320)
    levels = {"QQQ": 280.0, "SGOV": 100.0, "TQQQ": 22.0}
    rows = []
    for idx, day in enumerate(dates):
        qqq_return = 0.001 + (0.002 if idx % 41 == 0 else 0.0) - (0.003 if idx % 67 == 0 else 0.0)
        levels["QQQ"] *= 1.0 + qqq_return
        levels["SGOV"] *= 1.0 + 0.00015
        levels["TQQQ"] *= 1.0 + qqq_return * 2.6
        for ticker, close in levels.items():
            rows.append(
                {
                    "date": day.date().isoformat(),
                    "ticker": ticker,
                    "open": round(close * 0.999, 6),
                    "high": round(close * 1.002, 6),
                    "low": round(close * 0.998, 6),
                    "close": round(close, 6),
                    "adj_close": round(close, 6),
                    "volume": 1000000,
                }
            )
    prices_path = tmp_path / "prices_daily.csv"
    pd.DataFrame(rows).to_csv(prices_path, index=False)
    marketstack_path = tmp_path / "prices_marketstack_daily.csv"
    pd.DataFrame(rows).to_csv(marketstack_path, index=False)

    rate_rows = []
    for day in dates:
        for series, value in (("DGS2", 4.5), ("DGS10", 4.1), ("DTWEXBGS", 120.0)):
            rate_rows.append({"date": day.date().isoformat(), "series": series, "value": value})
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(rate_rows).to_csv(rates_path, index=False)
    return prices_path, marketstack_path, rates_path, dates[-1].date()
