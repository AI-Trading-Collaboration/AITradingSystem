from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.allocation import allocate_portfolio, weights_from_records
from ai_trading_system.etf_portfolio.backtest import (
    benchmark_registry,
    benchmark_weights_for_date,
    run_portfolio_backtest,
    toy_portfolio_return,
    write_backtest_run,
)
from ai_trading_system.etf_portfolio.data import standardize_price_frame, validate_price_data
from ai_trading_system.etf_portfolio.features import build_feature_store, select_features_for_date
from ai_trading_system.etf_portfolio.models import (
    ETFAllocationRecord,
    load_etf_config_bundle,
)
from ai_trading_system.etf_portfolio.p1 import (
    append_experiment_registry,
    append_experiment_run,
    build_confirmation_scores,
    build_experiment_comparison,
    build_governance_status,
    build_portfolio_attribution,
    build_relative_strength_table,
    evaluate_event_risk,
    evaluate_satellite_candidates,
)
from ai_trading_system.etf_portfolio.p2 import (
    build_advanced_risk_report,
    build_edgar_text_topic_audit,
    build_ensemble_candidates,
    build_holdings_lookthrough_report,
    build_live_interface_preflight,
    build_ml_ranking_candidates,
    build_news_theme_tracking_report,
    build_source_contract_report,
    build_walk_forward_readiness_report,
    build_weight_optimizer_candidates,
    derive_edgar_text_events_from_timeline,
    derive_options_iv_skew_from_vix,
    fetch_edgar_text_documents_from_timeline,
    import_p2_source,
    normalize_etf_holdings_source,
    normalize_news_theme_source,
    normalize_options_risk_source,
)
from ai_trading_system.etf_portfolio.regime import generate_regime_for_date
from ai_trading_system.etf_portfolio.reporting import render_daily_brief
from ai_trading_system.etf_portfolio.signals import generate_signals_for_date, signals_to_frame
from ai_trading_system.etf_portfolio.simulation import (
    evaluate_simulation_ledger,
    record_simulation_snapshot,
    render_simulation_report,
    summarize_simulation_for_brief,
)


def test_etf_config_loads_and_default_weights_sum_to_one() -> None:
    config = load_etf_config_bundle()

    assert "CASH" in config.assets.assets
    assert abs(sum(asset.default_weight for asset in config.assets.assets.values()) - 1.0) < 1e-6
    assert config.backtest.backtest.regime == "ai_after_chatgpt"
    assert config.backtest.backtest.start_date == date(2022, 12, 1)
    assert config.p2 is not None
    assert not config.p2.live_interface.broker_routing_allowed


def test_etf_price_validation_passes_standardized_toy_data() -> None:
    config = load_etf_config_bundle()
    raw = _make_prices(days=260, mode="up")
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name="fixture",
    )

    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )

    assert report.passed
    assert report.status == "PASS"
    assert "CASH" in report.symbols


def test_etf_price_validation_rejects_duplicates_and_negative_prices() -> None:
    config = load_etf_config_bundle()
    raw = _make_prices(days=260, mode="up")
    duplicate = raw.iloc[[0]].copy()
    raw = pd.concat([raw, duplicate], ignore_index=True)
    raw.loc[1, "close"] = -1.0
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name="fixture",
    )

    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )

    codes = {issue.code for issue in report.issues}
    assert not report.passed
    assert "prices_duplicate_date_symbol" in codes
    assert "prices_non_positive_or_missing" in codes


def test_feature_store_constant_price_has_zero_return_vol_and_drawdown() -> None:
    config = load_etf_config_bundle()
    prices, _ = standardize_price_frame(
        _make_prices(days=260, mode="constant"),
        assets=config.assets,
        source_name="fixture",
    )

    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    latest = select_features_for_date(features, date.fromisoformat(str(features["date"].max())))
    spy = latest.loc[latest["symbol"] == "SPY"].iloc[0]

    assert spy["ret_20d"] == 0
    assert spy["realized_vol_20d"] == 0
    assert spy["drawdown_63d"] == 0


def test_signals_are_scored_and_clamped() -> None:
    config = load_etf_config_bundle()
    prices, _ = standardize_price_frame(
        _make_prices(days=260, mode="up"),
        assets=config.assets,
        source_name="fixture",
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))

    records = generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)

    assert {record.symbol for record in records} == {"SPY", "QQQ", "SMH", "SOXX"}
    assert all(0 <= record.composite_score <= 100 for record in records)
    assert all(record.reason_codes for record in records)


def test_regime_detects_risk_off_fixture() -> None:
    config = load_etf_config_bundle()
    prices, _ = standardize_price_frame(
        _make_prices(days=260, mode="down"),
        assets=config.assets,
        source_name="fixture",
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))
    signals = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )

    regime = generate_regime_for_date(
        features,
        signals,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )

    assert regime.regime == "Risk-Off"


def test_risk_off_allocation_enforces_cash_min_and_sum() -> None:
    config = load_etf_config_bundle()
    prices, quality_report = standardize_price_frame(
        _make_prices(days=260, mode="down"),
        assets=config.assets,
        source_name="fixture",
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=quality_report,
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))
    signal_frame = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )

    allocation = allocate_portfolio(
        signal_frame,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-Off",
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=report,
    )
    weights = weights_from_records(allocation)

    assert abs(sum(weights.values()) - 1.0) < 1e-6
    assert weights["CASH"] >= 0.60
    assert weights["SMH"] + weights["SOXX"] <= 0.05 + 1e-6


def test_backtest_toy_portfolio_return() -> None:
    assert toy_portfolio_return(weight=0.50, asset_return=0.02) == 0.01


def test_backtest_runs_with_one_day_execution_lag(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    prices, metadata_issues = standardize_price_frame(
        _make_prices(days=320, mode="up"),
        assets=config.assets,
        source_name="fixture",
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )

    result = run_portfolio_backtest(
        prices,
        config=config,
        quality_report=report,
        start=date(2022, 12, 1),
        end=date.fromisoformat(str(prices["date"].max())),
        fast=True,
    )

    assert not result.daily.empty
    assert not result.weights.empty
    assert not result.trades.empty
    assert set(result.daily["signal_execution_lag_days"]) == {1}
    assert result.summary["data_quality_status"] == "PASS"
    assert "asset_returns_json" in result.daily.columns
    assert "asset_contributions_json" in result.daily.columns
    first_daily = result.daily.iloc[0]
    assert date.fromisoformat(str(first_daily["signal_date"])) < date.fromisoformat(
        str(first_daily["execution_date"])
    )
    assert date.fromisoformat(str(first_daily["execution_date"])) < date.fromisoformat(
        str(first_daily["return_date"])
    )
    contributions = json.loads(str(first_daily["asset_contributions_json"]))
    assert round(sum(float(value) for value in contributions.values()), 12) == round(
        float(first_daily["gross_return"]),
        12,
    )
    assert {
        "signal_date",
        "execution_date",
        "return_date",
        "symbol",
        "current_weight",
        "target_weight",
        "trade_delta",
        "model_version",
        "config_hash",
    }.issubset(result.weights.columns)
    assert set(result.summary["benchmark_metrics"]) == {
        "B001",
        "B002",
        "B003",
        "B004",
        "B005",
        "B006",
        "B007",
        "B008",
    }
    assert result.summary["benchmark_metrics"]["B007"]["benchmark_name"] == "ma_50_200_qqq"
    comparison = result.summary["benchmark_comparisons"][0]
    assert {
        "benchmark_name",
        "strategy_cagr",
        "benchmark_cagr",
        "excess_cagr",
        "strategy_max_drawdown",
        "benchmark_max_drawdown",
        "drawdown_reduction",
        "strategy_sharpe",
        "benchmark_sharpe",
        "strategy_calmar",
        "benchmark_calmar",
        "strategy_turnover",
        "benchmark_turnover",
    }.issubset(comparison)
    assert "annualized_volatility" in result.summary["strategy_extended_metrics"]

    paths = write_backtest_run(result, tmp_path)
    for path in paths:
        assert path.exists()
    assert (tmp_path / result.run_id / "weights.csv").exists()
    assert (tmp_path / result.run_id / "trades.csv").exists()
    assert (tmp_path / result.run_id / "metrics.json").exists()


def test_benchmark_registry_loads_required_ids_and_static_weights_sum_to_one() -> None:
    config = load_etf_config_bundle()
    registry = benchmark_registry(config)

    assert set(registry) == {"B001", "B002", "B003", "B004", "B005", "B006", "B007", "B008"}
    assert registry["B004"].symbol == "SOXX"
    assert registry["B005"].name == "static_growth_balanced"
    assert registry["B006"].name == "static_ai_growth"
    assert abs(sum(registry["B005"].weights.values()) - 1.0) < 1e-8
    assert abs(sum(registry["B006"].weights.values()) - 1.0) < 1e-8


def test_benchmark_weight_policies_are_deterministic_and_no_early_ma_trade() -> None:
    config = load_etf_config_bundle()
    raw = _make_prices(days=260, mode="up")
    prices, _ = standardize_price_frame(raw, assets=config.assets, source_name="fixture")
    dates = [item.date() for item in pd.bdate_range("2022-01-03", periods=260)]

    buy_hold = benchmark_weights_for_date(
        config=config,
        benchmark_id="B001",
        prices=prices,
        signal_date=dates[-1],
    )
    static_growth = benchmark_weights_for_date(
        config=config,
        benchmark_id="B005",
        prices=prices,
        signal_date=dates[-1],
    )
    early_ma = benchmark_weights_for_date(
        config=config,
        benchmark_id="B007",
        prices=prices,
        signal_date=dates[100],
    )

    assert buy_hold == {"SPY": 1.0}
    assert static_growth == {"SPY": 0.30, "QQQ": 0.50, "CASH": 0.20}
    assert early_ma == {"CASH": 1.0}


def test_risk_off_cash_switch_uses_signal_date_not_future_prices() -> None:
    config = load_etf_config_bundle()
    raw = _make_risk_switch_prices(days=230)
    prices, _ = standardize_price_frame(raw, assets=config.assets, source_name="fixture")
    dates = [item.date() for item in pd.bdate_range("2022-01-03", periods=230)]

    before_drop = benchmark_weights_for_date(
        config=config,
        benchmark_id="B008",
        prices=prices,
        signal_date=dates[209],
    )
    after_drop = benchmark_weights_for_date(
        config=config,
        benchmark_id="B008",
        prices=prices,
        signal_date=dates[220],
    )

    assert before_drop == {"QQQ": 1.0}
    assert after_drop == {"CASH": 1.0}


def test_no_lookahead_future_price_changes_do_not_change_signal_or_weights() -> None:
    config = load_etf_config_bundle()
    raw = _make_prices(days=320, mode="up")
    target_date = pd.bdate_range("2022-01-03", periods=260)[-1].date()
    prices, metadata_issues = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name="fixture",
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=target_date,
        extra_issues=metadata_issues,
    )
    first_weights = _weights_for_date(prices, config, report, target_date)

    modified_raw = raw.copy()
    modified_raw.loc[pd.to_datetime(modified_raw["date"]).dt.date > target_date, "adj_close"] *= 3
    modified_raw.loc[pd.to_datetime(modified_raw["date"]).dt.date > target_date, "close"] *= 3
    modified_prices, modified_issues = standardize_price_frame(
        modified_raw,
        assets=config.assets,
        source_name="fixture",
    )
    modified_report = validate_price_data(
        modified_prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=target_date,
        extra_issues=modified_issues,
    )
    second_weights = _weights_for_date(modified_prices, config, modified_report, target_date)

    assert first_weights == second_weights


def test_simulation_ledger_upserts_and_keeps_unavailable_forward_returns_null(
    tmp_path: Path,
) -> None:
    config = load_etf_config_bundle()
    run_date = date(2023, 1, 3)
    allocation = [
        ETFAllocationRecord(
            date=run_date,
            symbol="SPY",
            target_weight=0.5,
            previous_weight=None,
            trade_delta=None,
            composite_score=60.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        )
    ]
    ledger_path = tmp_path / "ledger.csv"
    prices, _ = standardize_price_frame(
        _make_prices(days=5, mode="up"),
        assets=config.assets,
        source_name="fixture",
    )

    record_simulation_snapshot(allocation_records=allocation, ledger_path=ledger_path)
    record_simulation_snapshot(allocation_records=allocation, ledger_path=ledger_path)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=run_date)
    ledger = pd.read_csv(ledger_path)

    assert len(ledger) == 1
    assert set(ledger["evaluation_only"]) == {True}
    assert pd.isna(ledger.iloc[0]["forward_return_20d"])
    assert pd.isna(ledger.iloc[0]["relative_return_vs_spy_20d"])
    assert pd.isna(ledger.iloc[0]["weight_contribution_20d"])


def test_simulation_record_cli_selects_latest_and_explicit_date(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    allocation_path = tmp_path / "target_weights.csv"
    ledger_path = tmp_path / "ledger.csv"
    rows = []
    for run_date, weight in ((date(2023, 1, 3), 0.4), (date(2023, 1, 4), 0.6)):
        rows.append(
            ETFAllocationRecord(
                date=run_date,
                symbol="SPY",
                target_weight=weight,
                previous_weight=None,
                trade_delta=None,
                composite_score=60.0,
                regime="Risk-On",
                reason_codes=("TEST",),
                constraints_applied=(),
                model_version=config.strategy.model.version,
                config_hash=config.config_hash,
                data_quality_status="PASS",
                created_at=datetime.now(UTC),
            ).to_record()
        )
    pd.DataFrame(rows).to_csv(allocation_path, index=False)
    runner = CliRunner()

    latest = runner.invoke(
        app,
        [
            "etf",
            "simulation",
            "record",
            "--allocation-path",
            str(allocation_path),
            "--ledger-path",
            str(ledger_path),
            "--date",
            "latest",
        ],
    )
    assert latest.exit_code == 0, latest.output
    ledger = pd.read_csv(ledger_path)
    assert set(ledger["date"]) == {"2023-01-04"}

    explicit = runner.invoke(
        app,
        [
            "simulation",
            "record",
            "--allocation-path",
            str(allocation_path),
            "--ledger-path",
            str(ledger_path),
            "--date",
            "2023-01-03",
        ],
    )
    assert explicit.exit_code == 0, explicit.output
    explicit_again = runner.invoke(
        app,
        [
            "simulation",
            "record",
            "--allocation-path",
            str(allocation_path),
            "--ledger-path",
            str(ledger_path),
            "--date",
            "2023-01-03",
        ],
    )
    assert explicit_again.exit_code == 0, explicit_again.output
    ledger = pd.read_csv(ledger_path)
    assert len(ledger) == 2
    assert set(ledger["date"]) == {"2023-01-03", "2023-01-04"}

    bad_allocation = tmp_path / "bad_target_weights.csv"
    pd.DataFrame([{"symbol": "SPY", "target_weight": 1.0}]).to_csv(
        bad_allocation,
        index=False,
    )
    bad = runner.invoke(
        app,
        [
            "etf",
            "simulation",
            "record",
            "--allocation-path",
            str(bad_allocation),
            "--ledger-path",
            str(tmp_path / "bad_ledger.csv"),
            "--date",
            "latest",
        ],
    )
    assert bad.exit_code != 0


def test_simulation_ledger_adds_benchmark_relative_and_weight_contribution(
    tmp_path: Path,
) -> None:
    config = load_etf_config_bundle()
    raw_prices = _make_prices(days=30, mode="up", symbols=["SPY", "QQQ", "SMH", "SOXX"])
    fixture_dates = [item.date() for item in pd.bdate_range("2022-01-03", periods=30)]
    run_date = fixture_dates[2]
    as_of = fixture_dates[25]
    allocation = [
        ETFAllocationRecord(
            date=run_date,
            symbol="SPY",
            target_weight=0.5,
            previous_weight=None,
            trade_delta=None,
            composite_score=60.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        ),
        ETFAllocationRecord(
            date=run_date,
            symbol="QQQ",
            target_weight=0.3,
            previous_weight=None,
            trade_delta=None,
            composite_score=62.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        ),
        ETFAllocationRecord(
            date=run_date,
            symbol="CASH",
            target_weight=0.2,
            previous_weight=None,
            trade_delta=None,
            composite_score=50.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        ),
    ]
    ledger_path = tmp_path / "ledger.csv"
    prices, _ = standardize_price_frame(raw_prices, assets=config.assets, source_name="fixture")

    record_simulation_snapshot(allocation_records=allocation, ledger_path=ledger_path)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=as_of)
    ledger = pd.read_csv(ledger_path)
    spy_row = ledger.loc[ledger["symbol"] == "SPY"].iloc[0]
    qqq_row = ledger.loc[ledger["symbol"] == "QQQ"].iloc[0]
    portfolio_return = float(ledger["portfolio_return_20d"].dropna().iloc[0])
    markdown = render_simulation_report(ledger_path)

    assert "relative_return_vs_spy_20d" in ledger.columns
    assert "relative_return_vs_qqq_20d" in ledger.columns
    assert "weight_contribution_20d" in ledger.columns
    assert set(ledger["evaluation_only"]) == {True}
    assert abs(float(spy_row["relative_return_vs_spy_20d"])) < 1e-12
    assert not pd.isna(qqq_row["relative_return_vs_spy_20d"])
    assert round(float(spy_row["weight_contribution_20d"]), 10) == round(
        0.5 * float(spy_row["forward_return_20d"]),
        10,
    )
    assert portfolio_return == float(ledger["weight_contribution_20d"].sum())
    assert "## Portfolio vs Benchmarks" in markdown
    assert "avg relative vs SPY" in markdown


def test_daily_brief_simulation_summary_reads_ledger(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    raw_prices = _make_prices(days=30, mode="up", symbols=["SPY", "QQQ", "SMH", "SOXX"])
    prices, _ = standardize_price_frame(raw_prices, assets=config.assets, source_name="fixture")
    fixture_dates = [item.date() for item in pd.bdate_range("2022-01-03", periods=30)]
    run_date = fixture_dates[2]
    as_of = fixture_dates[25]
    allocation = [
        ETFAllocationRecord(
            date=run_date,
            symbol="SPY",
            target_weight=0.6,
            previous_weight=None,
            trade_delta=None,
            composite_score=60.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        ),
        ETFAllocationRecord(
            date=run_date,
            symbol="QQQ",
            target_weight=0.4,
            previous_weight=None,
            trade_delta=None,
            composite_score=62.0,
            regime="Risk-On",
            reason_codes=("TEST",),
            constraints_applied=(),
            model_version=config.strategy.model.version,
            config_hash=config.config_hash,
            data_quality_status="PASS",
            created_at=datetime.now(UTC),
        ),
    ]
    ledger_path = tmp_path / "ledger.csv"
    record_simulation_snapshot(allocation_records=allocation, ledger_path=ledger_path)
    evaluate_simulation_ledger(ledger_path=ledger_path, prices=prices, as_of=as_of)

    summary = summarize_simulation_for_brief(ledger_path, as_of=as_of)

    assert "20d hit rate=" in summary
    assert "avg relative vs SPY" in summary
    assert "avg relative vs QQQ" in summary
    assert "未找到 ledger" in summarize_simulation_for_brief(tmp_path / "missing.csv", as_of=as_of)


def test_daily_report_contains_required_sections() -> None:
    config = load_etf_config_bundle()
    prices, metadata_issues = standardize_price_frame(
        _make_prices(days=260, mode="up"),
        assets=config.assets,
        source_name="fixture",
    )
    report = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))
    signals = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )
    regime = generate_regime_for_date(
        features,
        signals,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    allocation = allocate_portfolio(
        signals,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=regime.regime,
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=report,
    )

    markdown = render_daily_brief(
        run_date=run_date,
        config=config,
        quality_report=report,
        signals=signals,
        regime=pd.Series(regime.to_record()),
        allocation=pd.DataFrame([record.to_record() for record in allocation]),
    )

    assert "## 1. Executive Summary" in markdown
    assert "## 3. ETF Signal Dashboard" in markdown
    assert "## 4. Target Weights" in markdown
    assert "Data Quality: PASS" in markdown


def test_p1_relative_strength_includes_confirmation_and_satellite_pairs() -> None:
    config = load_etf_config_bundle()
    assert config.p1 is not None
    satellite_symbols = set(config.p1.satellite_stocks)
    raw = _make_prices(
        days=260,
        mode="up",
        symbols=["SPY", "QQQ", "SMH", "SOXX", *sorted(satellite_symbols)],
    )
    prices, _ = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name="fixture",
        extra_symbols=satellite_symbols,
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))

    table = build_relative_strength_table(features, config=config, run_date=run_date)

    pairs = set(table["pair"])
    assert {"MSFT/QQQ", "GOOGL/QQQ", "NVDA/SMH", "AVGO/SMH"}.issubset(pairs)
    assert "p1_mega_cap_confirmation" in set(table["meaning"])
    assert "RELATIVE_STRENGTH_PAIR_MISSING" not in " ".join(table["reason_codes"])


def test_p1_confirmation_scores_are_observe_only() -> None:
    config = load_etf_config_bundle()
    assert config.p1 is not None
    run_date = date(2026, 5, 29)
    relative_strength = pd.DataFrame(
        [
            {"pair": "SMH/QQQ", "rs_score": 70.0},
            {"pair": "SOXX/QQQ", "rs_score": 60.0},
            {"pair": "SMH/SPY", "rs_score": 65.0},
            {"pair": "NVDA/SMH", "rs_score": 80.0},
            {"pair": "AVGO/SMH", "rs_score": 75.0},
            {"pair": "MSFT/QQQ", "rs_score": 55.0},
            {"pair": "GOOGL/QQQ", "rs_score": 75.0},
        ]
    )

    scores = build_confirmation_scores(
        relative_strength,
        p1_config=config.p1,
        run_date=run_date,
    )

    assert set(scores["score_id"]) == {
        "AIConfirmationScore",
        "SemiconductorLeadershipScore",
        "MegaCapConfirmationScore",
    }
    assert set(scores["production_effect"]) == {"none"}
    assert scores.loc[
        scores["score_id"] == "AIConfirmationScore", "model_stage"
    ].iloc[0] == "observe_only"


def test_p1_satellite_candidates_do_not_change_production_weights() -> None:
    config = load_etf_config_bundle()
    assert config.p1 is not None
    satellite_symbols = set(config.p1.satellite_stocks)
    raw = _make_prices(
        days=260,
        mode="up",
        symbols=["SPY", "QQQ", "SMH", "SOXX", *sorted(satellite_symbols)],
    )
    prices, _ = standardize_price_frame(
        raw,
        assets=config.assets,
        source_name="fixture",
        extra_symbols=satellite_symbols,
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))
    signals = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )

    candidates = evaluate_satellite_candidates(
        features,
        signals,
        config=config,
        p1_config=config.p1,
        run_date=run_date,
        regime="Risk-On",
    )
    risk_off_candidates = evaluate_satellite_candidates(
        features,
        signals,
        config=config,
        p1_config=config.p1,
        run_date=run_date,
        regime="Risk-Off",
    )

    assert set(candidates["symbol"]) == satellite_symbols
    assert set(candidates["production_effect"]) == {"none"}
    assert candidates["suggested_substitution_weight"].max() <= (
        config.p1.satellite_rules.default_substitution_weight + 1e-9
    )
    assert not risk_off_candidates["benchmark_allowed_by_regime"].any()
    assert risk_off_candidates["suggested_substitution_weight"].max() == 0


def test_p1_attribution_event_registry_and_governance_are_observe_only(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p1 is not None
    run_date = date(2026, 5, 29)
    prices = pd.DataFrame(
        [
            {"date": "2026-05-28", "symbol": "SPY", "adj_close": 100.0},
            {"date": "2026-05-29", "symbol": "SPY", "adj_close": 110.0},
        ]
    )
    allocation = pd.DataFrame(
        [
            {
                "date": run_date.isoformat(),
                "symbol": "SPY",
                "target_weight": 0.50,
                "trade_delta": 0.10,
            },
            {
                "date": run_date.isoformat(),
                "symbol": "CASH",
                "target_weight": 0.50,
                "trade_delta": -0.10,
            },
        ]
    )

    attribution = build_portfolio_attribution(allocation, prices, run_date=run_date)
    spy_contribution = attribution.loc[
        attribution["symbol"] == "SPY", "weight_contribution_1d"
    ].iloc[0]
    flags = evaluate_event_risk(p1_config=config.p1, run_date=run_date)
    registry_path = append_experiment_registry(
        registry_path=tmp_path / "registry.jsonl",
        model_version=config.strategy.model.version,
        parent_model_version=config.strategy.model.version,
        config_hash=config.config_hash,
        parameter_diff={},
        metrics={},
        status="candidate",
        notes="unit test",
    )
    status = build_governance_status(config)
    record = json.loads(registry_path.read_text(encoding="utf-8").strip())

    assert round(spy_contribution, 6) == 0.05
    assert "sleeve_contribution_1d" in attribution.columns
    assert round(attribution["turnover_contribution"].sum(), 6) == 0.20
    assert flags.iloc[0]["event_id"] == "NO_CONFIGURED_EVENTS"
    assert bool(flags.iloc[0]["risk_flag"]) is False
    assert record["production_effect"] == "none"
    assert record["manual_review_required"] is True
    assert bool(status.iloc[0]["auto_promotion"]) is False
    assert status.iloc[0]["production_effect"] == "none"


def test_p1_experiment_run_and_compare_are_candidate_only(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    candidate_path = tmp_path / "strategy_candidate.yaml"
    candidate_path.write_text(
        "\n".join(
            [
                "policy_metadata:",
                "  version: etf_strategy_candidate_v0_2",
                "model:",
                "  version: 0.2.0-candidate",
                "allocation:",
                "  score_multipliers:",
                "    strong_up: 1.10",
            ]
        ),
        encoding="utf-8",
    )
    candidate_summary = tmp_path / "candidate_summary.json"
    candidate_summary.write_text(
        json.dumps(
            {
                "market_regime": "ai_after_chatgpt",
                "data_quality_status": "PASS",
                "strategy_metrics": {
                    "total_return": 0.20,
                    "cagr": 0.10,
                    "max_drawdown": -0.05,
                    "sharpe": 1.20,
                    "turnover": 2.0,
                },
            }
        ),
        encoding="utf-8",
    )
    baseline_summary = tmp_path / "baseline_summary.json"
    baseline_summary.write_text(
        json.dumps(
            {
                "strategy_metrics": {
                    "total_return": 0.15,
                    "cagr": 0.08,
                    "max_drawdown": -0.06,
                    "sharpe": 1.10,
                    "turnover": 1.5,
                },
            }
        ),
        encoding="utf-8",
    )
    registry_path = tmp_path / "registry.jsonl"

    append_experiment_run(
        registry_path=registry_path,
        candidate_config_path=candidate_path,
        baseline_config_path=Path("config/etf_portfolio/strategy.yaml"),
        config=config,
        metrics_path=candidate_summary,
        status="candidate",
        notes="unit candidate",
    )
    comparison = build_experiment_comparison(
        registry_path=registry_path,
        baseline_metrics_path=baseline_summary,
    )
    record = json.loads(registry_path.read_text(encoding="utf-8").strip())

    assert record["model_version"] == "0.2.0-candidate"
    assert record["candidate_only"] is True
    assert record["auto_promotion"] is False
    assert record["production_effect"] == "none"
    assert record["metrics"]["metric_status"] == "AVAILABLE"
    assert record["parameter_diff"]["changed_count"] > 0
    assert comparison.iloc[0]["comparison_status"] == "OBSERVE_ONLY"
    assert comparison.iloc[0]["metric_status"] == "AVAILABLE"
    assert comparison.iloc[0]["production_effect"] == "none"
    assert bool(comparison.iloc[0]["auto_promotion"]) is False
    assert round(float(comparison.iloc[0]["delta_total_return"]), 6) == 0.05


def test_etf_experiment_run_compare_cli_smoke(tmp_path: Path) -> None:
    candidate_path = tmp_path / "strategy_candidate.yaml"
    candidate_path.write_text(
        "policy_metadata:\n  version: cli_candidate\nmodel:\n  version: cli-candidate\n",
        encoding="utf-8",
    )
    registry_path = tmp_path / "registry.jsonl"
    output_dir = tmp_path / "reports"
    runner = CliRunner()

    run_result = runner.invoke(
        app,
        [
            "experiments",
            "run",
            "--config",
            str(candidate_path),
            "--registry-path",
            str(registry_path),
            "--notes",
            "cli smoke",
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    compare_result = runner.invoke(
        app,
        [
            "experiments",
            "compare",
            "--registry-path",
            str(registry_path),
            "--output-dir",
            str(output_dir),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert run_result.exit_code == 0, run_result.output
    assert compare_result.exit_code == 0, compare_result.output
    assert "production_effect=none" in run_result.output
    assert "production_effect=none" in compare_result.output
    assert registry_path.exists()
    assert list(output_dir.glob("*_experiment_compare.md"))


def test_p2_source_contract_reports_missing_input_without_silent_success(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["edgar_text"]

    report = build_source_contract_report(
        source_id="edgar_text",
        source=source,
        run_date=date(2026, 5, 29),
        input_path=tmp_path / "missing.csv",
    )

    assert report.iloc[0]["status"] == "MISSING_INPUT"
    assert report.iloc[0]["row_count"] == 0
    assert report.iloc[0]["production_effect"] == "none"


def test_p2_derives_edgar_text_events_from_sec_timeline(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["edgar_text"]
    timeline_path = tmp_path / "filing_timeline.csv"
    output_path = tmp_path / "edgar_text_events.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "accession_number": "0001045810-26-000001",
                "form": "10-Q",
                "filing_date": "2026-05-28",
                "acceptance_datetime_utc": "2026-05-28T21:15:00Z",
                "available_time_utc": "2026-05-28T21:15:00Z",
                "available_for_signal_date": "2026-05-29",
                "source_url": "https://www.sec.gov/Archives/example-nvda.htm",
                "raw_payload_sha256": "a" * 64,
            },
            {
                "ticker": "NVDA",
                "accession_number": "0001045810-26-000002",
                "form": "8-K",
                "filing_date": "2026-06-01",
                "acceptance_datetime_utc": "2026-06-01T13:00:00Z",
                "available_time_utc": "2026-06-01T13:00:00Z",
                "available_for_signal_date": "2026-06-02",
                "source_url": "https://www.sec.gov/Archives/future-nvda.htm",
                "raw_payload_sha256": "b" * 64,
            },
            {
                "ticker": "MSFT",
                "accession_number": "0000789019-26-000001",
                "form": "10-K",
                "filing_date": "2026-05-28",
                "acceptance_datetime_utc": "2026-05-28T22:00:00Z",
                "available_time_utc": "2026-05-28T22:00:00Z",
                "available_for_signal_date": "2026-05-29",
                "source_url": "https://www.sec.gov/Archives/example-msft.htm",
                "raw_payload_sha256": "c" * 64,
            },
        ]
    ).to_csv(timeline_path, index=False)

    report = derive_edgar_text_events_from_timeline(
        source=source,
        timeline_path=timeline_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=date(2026, 5, 29),
        symbols=["NVDA"],
        downloaded_at=datetime(2026, 5, 29, tzinfo=UTC),
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    contract = build_source_contract_report(
        source_id="edgar_text",
        source=source,
        run_date=date(2026, 5, 29),
        input_path=output_path,
    )

    assert report.iloc[0]["status"] == "DERIVED"
    assert list(canonical.columns) == source.required_columns
    assert len(canonical) == 1
    assert canonical.iloc[0]["symbol"] == "NVDA"
    assert canonical.iloc[0]["filing_type"] == "10-Q"
    assert canonical.iloc[0]["sentiment_score"] == 0.0
    assert "metadata only" in canonical.iloc[0]["summary"]
    assert len(canonical.iloc[0]["checksum"]) == 64
    assert manifest.iloc[0]["source_id"] == "edgar_text"
    assert manifest.iloc[0]["production_effect"] == "none"
    assert contract.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert contract.iloc[0]["rows_as_of"] == 1


def test_p2_fetches_edgar_text_documents_with_pit_gate(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["edgar_text"]
    filing_doc = tmp_path / "nvda_10q.htm"
    filing_doc.write_text(
        """
        <html>
          <head><title>NVDA 10-Q</title><script>ignore me</script></head>
          <body>
            <h1>NVIDIA 10-Q</h1>
            <p>Risk factors include export controls and supply constraints.</p>
          </body>
        </html>
        """,
        encoding="utf-8",
    )
    timeline_path = tmp_path / "filing_timeline.csv"
    output_path = tmp_path / "edgar_text_documents.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    document_dir = tmp_path / "text_cache"
    pd.DataFrame(
        [
            {
                "ticker": "NVDA",
                "accession_number": "0001045810-26-000001",
                "form": "10-Q",
                "filing_date": "2026-05-28",
                "acceptance_datetime_utc": "2026-05-28T21:15:00Z",
                "available_time_utc": "2026-05-28T21:15:00Z",
                "available_for_signal_date": "2026-05-29",
                "source_url": str(filing_doc),
            },
            {
                "ticker": "NVDA",
                "accession_number": "0001045810-26-000002",
                "form": "10-Q",
                "filing_date": "2026-06-01",
                "acceptance_datetime_utc": "2026-06-01T21:15:00Z",
                "available_time_utc": "2026-06-01T21:15:00Z",
                "available_for_signal_date": "2026-06-02",
                "source_url": str(filing_doc),
            },
        ]
    ).to_csv(timeline_path, index=False)

    report = fetch_edgar_text_documents_from_timeline(
        source=source,
        timeline_path=timeline_path,
        document_dir=document_dir,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=date(2026, 5, 29),
        symbols=["NVDA"],
        filing_types=["10-Q"],
        limit=5,
        downloaded_at=datetime(2026, 5, 29, tzinfo=UTC),
    )
    index = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    text_path = Path(index.iloc[0]["document_text_path"])

    assert report.iloc[0]["status"] == "FETCHED"
    assert report.iloc[0]["candidate_count"] == 1
    assert report.iloc[0]["fetched_document_count"] == 1
    assert len(index) == 1
    assert index.iloc[0]["fetch_status"] == "FETCHED"
    assert index.iloc[0]["symbol"] == "NVDA"
    assert index.iloc[0]["filing_type"] == "10-Q"
    assert index.iloc[0]["production_effect"] == "none"
    assert len(index.iloc[0]["document_text_checksum"]) == 64
    assert "Risk factors include export controls" in index.iloc[0]["text_excerpt"]
    assert "ignore me" not in text_path.read_text(encoding="utf-8")
    assert manifest.iloc[0]["source_id"] == "edgar_text_documents"
    assert manifest.iloc[0]["production_effect"] == "none"


def test_p2_edgar_text_topic_audit_is_observe_only(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    text_path = tmp_path / "nvda_10q.txt"
    text_path.write_text(
        (
            "NVIDIA discusses artificial intelligence and accelerated computing. "
            "Export controls and supply constraints may affect revenue. "
            "Capital expenditures include property and equipment."
        )
        * 20,
        encoding="utf-8",
    )
    document_index = pd.DataFrame(
        [
            {
                "as_of": "2026-05-21",
                "symbol": "NVDA",
                "source_url": "https://www.sec.gov/Archives/example.htm",
                "filing_type": "10-Q",
                "available_at": "2026-05-20T21:15:00Z",
                "accession_number": "0001045810-26-000052",
                "document_text_path": str(text_path),
                "fetch_status": "FETCHED",
                "production_effect": "none",
            },
            {
                "as_of": "2026-06-02",
                "symbol": "NVDA",
                "source_url": "https://www.sec.gov/Archives/future.htm",
                "filing_type": "10-Q",
                "available_at": "2026-06-01T21:15:00Z",
                "accession_number": "0001045810-26-000099",
                "document_text_path": str(text_path),
                "fetch_status": "FETCHED",
                "production_effect": "none",
            },
        ]
    )

    audit = build_edgar_text_topic_audit(
        document_index=document_index,
        p2_config=config.p2,
        run_date=date(2026, 5, 29),
    )

    assert set(audit["analysis_status"]) == {"COUNTED"}
    assert set(audit["topic"]) == set(config.p2.edgar_text_analysis.topic_keywords)
    assert audit["candidate_only"].all()
    assert not audit["auto_promotion"].any()
    assert set(audit["production_effect"]) == {"none"}
    assert audit.loc[audit["topic"] == "ai_demand", "keyword_count"].iloc[0] > 0
    assert "sentiment inference" in audit.iloc[0]["limitation"]
    assert set(audit["as_of"]) == {"2026-05-21"}


def test_p2_derives_options_risk_from_vix_proxy_with_explicit_limitations(
    tmp_path: Path,
) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["options_iv_skew"]
    prices_path = tmp_path / "prices.csv"
    output_path = tmp_path / "options_iv_skew.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    raw = _make_prices(
        days=80,
        mode="up",
        symbols=["SPY", "QQQ", "SMH", "SOXX", "^VIX"],
    )
    proxy_prices = raw[["date", "symbol", "close"]].rename(columns={"symbol": "ticker"})
    proxy_prices.to_csv(prices_path, index=False)
    run_date = date.fromisoformat(str(raw["date"].max()))

    report = derive_options_iv_skew_from_vix(
        source=source,
        p2_config=config.p2,
        prices=proxy_prices,
        prices_path=prices_path,
        output_path=output_path,
        manifest_path=manifest_path,
        run_date=run_date,
        symbols=["QQQ", "SMH"],
        data_quality_status="PASS",
        downloaded_at=datetime(2026, 5, 29, tzinfo=UTC),
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    contract = build_source_contract_report(
        source_id="options_iv_skew",
        source=source,
        run_date=run_date,
        input_path=output_path,
    )

    assert report.iloc[0]["status"] == "DERIVED"
    assert source.required_columns == list(canonical.columns[: len(source.required_columns)])
    assert set(canonical["symbol"]) == {"QQQ", "SMH"}
    assert canonical["iv_rank"].notna().all()
    assert canonical["skew_zscore"].isna().all()
    assert canonical["vxn_level"].isna().all()
    assert canonical["risk_flag"].str.contains("MISSING_VXN_SKEW").all()
    assert set(canonical["data_quality_status"]) == {"PASS"}
    assert set(canonical["vix_proxy_quality_status"]) == {"PASS"}
    assert manifest.iloc[0]["source_id"] == "options_iv_skew"
    assert manifest.iloc[0]["production_effect"] == "none"
    assert contract.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert contract.iloc[0]["rows_as_of"] == 2
    assert contract.iloc[0]["data_quality_status"] == "PASS"
    assert contract.iloc[0]["source_quality_status"] == "PASS"


def test_p2_normalizes_vendor_options_risk_source_with_vxn_and_skew(
    tmp_path: Path,
) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["options_iv_skew"]
    input_path = tmp_path / "vendor_options.csv"
    output_path = tmp_path / "options_iv_skew.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame(
        [
            {
                "Ticker": "QQQ",
                "As Of": "2026-05-29",
                "provider_available_at": "2026-05-29T21:00:00Z",
                "IV Rank": "85%",
                "Skew Z": 1.2,
                "VXN Level": 22.4,
                "url": "https://example.test/options/qqq",
            },
            {
                "Ticker": "SMH",
                "As Of": "2026-05-29",
                "provider_available_at": "2026-05-29T21:00:00Z",
                "IV Rank": 0.97,
                "Skew Z": -0.4,
                "VXN Level": 23.1,
                "url": "https://example.test/options/smh",
            },
            {
                "Ticker": "BAD",
                "As Of": "2026-05-29",
                "provider_available_at": "2026-05-29T21:00:00Z",
                "IV Rank": "not-a-rank",
                "Skew Z": 0.1,
                "VXN Level": 22.0,
            },
        ]
    ).to_csv(input_path, index=False)

    report = normalize_options_risk_source(
        source=source,
        p2_config=config.p2,
        input_path=input_path,
        provider="options_fixture",
        source_url="https://example.test/options-feed",
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=datetime(2026, 5, 29, 21, tzinfo=UTC),
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    contract = build_source_contract_report(
        source_id="options_iv_skew",
        source=source,
        run_date=date(2026, 5, 29),
        input_path=output_path,
    )

    assert report.iloc[0]["status"] == "NORMALIZED"
    assert report.iloc[0]["row_count"] == 2
    assert report.iloc[0]["invalid_row_count"] == 1
    assert source.required_columns == list(canonical.columns[: len(source.required_columns)])
    assert set(canonical["symbol"]) == {"QQQ", "SMH"}
    assert set(canonical["risk_flag"]) == {"VENDOR_OPTIONS_ELEVATED", "VENDOR_OPTIONS_STRESS"}
    assert canonical["iv_rank"].notna().all()
    assert canonical["skew_zscore"].notna().all()
    assert canonical["vxn_level"].notna().all()
    assert canonical["source_url"].str.startswith("https://example.test/options/").all()
    assert canonical["limitation"].str.contains("observe-only").all()
    assert manifest.iloc[0]["source_id"] == "options_iv_skew"
    assert manifest.iloc[0]["production_effect"] == "none"
    assert contract.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert contract.iloc[0]["rows_as_of"] == 2
    assert "skew_zscore" in contract.iloc[0]["numeric_summary"]
    assert "vxn_level" in contract.iloc[0]["numeric_summary"]


def test_p2_normalizes_news_theme_source_without_llm_inference(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["news_themes"]
    input_path = tmp_path / "news.csv"
    output_path = tmp_path / "news_theme_events.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame(
        [
            {
                "Ticker": "NVDA",
                "Topic": "AI accelerator demand",
                "Headline": "NVIDIA supplier commentary points to continued AI demand",
                "Published": "2026-05-29T13:00:00Z",
                "provider_available_at": "2026-05-29T13:05:00Z",
                "url": "https://example.test/nvda-ai-demand",
            },
            {
                "Ticker": "SMH",
                "Topic": "semiconductor cycle",
                "Headline": "Semiconductor ETF breadth improves",
                "Published": "2026-05-29T14:00:00Z",
                "provider_available_at": "2026-05-29T14:05:00Z",
                "url": "https://example.test/smh-breadth",
            },
        ]
    ).to_csv(input_path, index=False)

    report = normalize_news_theme_source(
        source=source,
        p2_config=config.p2,
        input_path=input_path,
        provider="news_fixture",
        source_url="https://example.test/news-feed",
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=datetime(2026, 5, 29, 15, tzinfo=UTC),
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    contract = build_source_contract_report(
        source_id="news_themes",
        source=source,
        run_date=date(2026, 5, 29),
        input_path=output_path,
    )

    assert report.iloc[0]["status"] == "NORMALIZED"
    assert bool(report.iloc[0]["used_default_sentiment"]) is True
    assert source.required_columns == list(canonical.columns[: len(source.required_columns)])
    assert set(canonical["symbol"]) == {"NVDA", "SMH"}
    assert set(canonical["sentiment_score"]) == {0.0}
    assert set(canonical["relevance_score"]) == {1.0}
    assert canonical["limitation"].str.contains("neutral_sentiment_default_used").all()
    assert manifest.iloc[0]["source_id"] == "news_themes"
    assert manifest.iloc[0]["production_effect"] == "none"
    assert contract.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert contract.iloc[0]["rows_as_of"] == 2


def test_p2_tracks_news_theme_events_with_pit_window(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["news_themes"]
    input_path = tmp_path / "news_theme_events.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-29",
                "symbol": "NVDA",
                "source_provider": "news_fixture",
                "source_url": "https://example.test/nvda-1",
                "published_at": "2026-05-29T13:00:00Z",
                "available_at": "2026-05-29T13:05:00Z",
                "theme": "AI demand",
                "sentiment_score": 0.4,
                "relevance_score": 0.8,
                "summary": "AI demand remains strong",
                "checksum": "a" * 64,
                "limitation": "",
            },
            {
                "as_of": "2026-05-29",
                "symbol": "NVDA",
                "source_provider": "news_fixture",
                "source_url": "https://example.test/nvda-2",
                "published_at": "2026-05-29T14:00:00Z",
                "available_at": "2026-05-29T14:05:00Z",
                "theme": "AI demand",
                "sentiment_score": -0.2,
                "relevance_score": 0.2,
                "summary": "Supply risk tempers AI demand",
                "checksum": "b" * 64,
                "limitation": "neutral_sentiment_default_used",
            },
            {
                "as_of": "2026-05-01",
                "symbol": "NVDA",
                "source_provider": "news_fixture",
                "source_url": "https://example.test/old",
                "published_at": "2026-05-01T14:00:00Z",
                "available_at": "2026-05-01T14:05:00Z",
                "theme": "AI demand",
                "sentiment_score": 1.0,
                "relevance_score": 1.0,
                "summary": "Old event outside tracking window",
                "checksum": "c" * 64,
                "limitation": "",
            },
        ]
    ).to_csv(input_path, index=False)

    report = build_news_theme_tracking_report(
        source=source,
        p2_config=config.p2,
        run_date=date(2026, 5, 29),
        input_path=input_path,
    )

    assert len(report) == 1
    row = report.iloc[0]
    assert row["status"] == "TRACKED"
    assert row["symbol"] == "NVDA"
    assert row["theme"] == "AI demand"
    assert row["event_count"] == 2
    assert abs(float(row["weighted_sentiment"]) - 0.28) < 1e-9
    assert row["latest_summary"] == "Supply risk tempers AI demand"
    assert row["source_limitation"] == "neutral_sentiment_default_used"
    assert bool(row["candidate_only"]) is True
    assert bool(row["auto_promotion"]) is False
    assert row["production_effect"] == "none"
    assert "no LLM inference" in row["limitation"]


def test_p2_holdings_lookthrough_uses_audited_input(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    holdings_path = tmp_path / "holdings.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-29",
                "etf_symbol": "SMH",
                "holding_symbol": "NVDA",
                "holding_weight": 0.25,
                "source_provider": "fixture",
                "source_url": "https://example.test/smh.csv",
                "downloaded_at": "2026-05-29T21:00:00Z",
                "checksum": "abc",
            },
            {
                "as_of": "2026-05-29",
                "etf_symbol": "SMH",
                "holding_symbol": "AVGO",
                "holding_weight": 0.10,
                "source_provider": "fixture",
                "source_url": "https://example.test/smh.csv",
                "downloaded_at": "2026-05-29T21:00:00Z",
                "checksum": "abc",
            },
        ]
    ).to_csv(holdings_path, index=False)

    report = build_holdings_lookthrough_report(
        source=config.p2.sources["etf_holdings"],
        run_date=date(2026, 5, 29),
        input_path=holdings_path,
    )

    assert report.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert report.iloc[0]["etf_symbol"] == "SMH"
    assert report.iloc[0]["top_holding"] == "NVDA"
    assert report.iloc[0]["production_effect"] == "none"


def test_p2_normalizes_issuer_holdings_source_and_updates_manifest(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    source = config.p2.sources["etf_holdings"]
    input_path = tmp_path / "issuer_holdings.csv"
    output_path = tmp_path / "etf_holdings.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame(
        [
            {"Ticker": "NVDA", "Name": "NVIDIA Corp", "Weight (%)": "25.0%"},
            {"Ticker": "AVGO", "Name": "Broadcom Inc", "Weight (%)": "10.5%"},
            {"Ticker": "", "Name": "invalid", "Weight (%)": "not-a-number"},
        ]
    ).to_csv(input_path, index=False)

    report = normalize_etf_holdings_source(
        source=source,
        input_path=input_path,
        etf_symbol="SMH",
        provider="issuer_fixture",
        source_url="https://example.test/smh-holdings.csv",
        as_of=date(2026, 5, 29),
        output_path=output_path,
        manifest_path=manifest_path,
        downloaded_at=datetime(2026, 5, 29, 21, tzinfo=UTC),
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)
    lookthrough = build_holdings_lookthrough_report(
        source=source,
        run_date=date(2026, 5, 29),
        input_path=output_path,
    )

    assert report.iloc[0]["status"] == "NORMALIZED"
    assert report.iloc[0]["row_count"] == 2
    assert list(canonical.columns) == source.required_columns
    assert set(canonical["holding_symbol"]) == {"NVDA", "AVGO"}
    assert canonical.loc[canonical["holding_symbol"] == "NVDA", "holding_weight"].iloc[0] == 0.25
    assert manifest.iloc[0]["source_id"] == "etf_holdings"
    assert manifest.iloc[0]["row_count"] == 2
    assert manifest.iloc[0]["production_effect"] == "none"
    assert lookthrough.iloc[0]["status"] == "PASS_WITH_LIMITATIONS"
    assert lookthrough.iloc[0]["top_holding"] == "NVDA"


def test_p2_import_source_writes_canonical_file_and_manifest(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    input_path = tmp_path / "holdings_input.csv"
    output_path = tmp_path / "canonical_holdings.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame(
        [
            {
                "as_of": "2026-05-29",
                "etf_symbol": "SMH",
                "holding_symbol": "NVDA",
                "holding_weight": 0.25,
                "source_provider": "issuer_fixture",
                "source_url": "https://example.test/smh.csv",
                "downloaded_at": "2026-05-29T21:00:00Z",
                "checksum": "abc",
                "ignored_column": "not persisted",
            },
        ]
    ).to_csv(input_path, index=False)

    report = import_p2_source(
        source_id="etf_holdings",
        source=config.p2.sources["etf_holdings"],
        input_path=input_path,
        output_path=output_path,
        manifest_path=manifest_path,
        provider="issuer_fixture",
        source_url="https://example.test/smh.csv",
        request_params={"fixture": True},
    )
    canonical = pd.read_csv(output_path)
    manifest = pd.read_csv(manifest_path)

    assert report.iloc[0]["status"] == "IMPORTED"
    assert "ignored_column" not in canonical.columns
    assert manifest.iloc[0]["source_id"] == "etf_holdings"
    assert manifest.iloc[0]["row_count"] == 1
    assert len(str(manifest.iloc[0]["checksum"])) == 64
    assert manifest.iloc[0]["production_effect"] == "none"


def test_p2_import_source_rejects_schema_without_writing_output(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    input_path = tmp_path / "bad_holdings.csv"
    output_path = tmp_path / "canonical_holdings.csv"
    manifest_path = tmp_path / "source_manifest.csv"
    pd.DataFrame([{"as_of": "2026-05-29"}]).to_csv(input_path, index=False)

    report = import_p2_source(
        source_id="etf_holdings",
        source=config.p2.sources["etf_holdings"],
        input_path=input_path,
        output_path=output_path,
        manifest_path=manifest_path,
        provider="issuer_fixture",
        source_url="https://example.test/smh.csv",
    )

    assert report.iloc[0]["status"] == "FAILED_SCHEMA"
    assert "etf_symbol" in str(report.iloc[0]["missing_columns"])
    assert not output_path.exists()
    assert not manifest_path.exists()


def test_p2_risk_ml_ensemble_and_live_preflight_are_observe_only(tmp_path: Path) -> None:
    config = load_etf_config_bundle()
    assert config.p2 is not None
    prices, metadata_issues = standardize_price_frame(
        _make_prices(days=260, mode="up"),
        assets=config.assets,
        source_name="fixture",
    )
    quality = validate_price_data(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        as_of=date.fromisoformat(str(prices["date"].max())),
        extra_issues=metadata_issues,
    )
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    run_date = date.fromisoformat(str(features["date"].max()))
    signals = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )
    allocation = pd.DataFrame(
        [
            {"date": run_date.isoformat(), "symbol": "SPY", "target_weight": 0.30},
            {"date": run_date.isoformat(), "symbol": "QQQ", "target_weight": 0.40},
            {"date": run_date.isoformat(), "symbol": "SMH", "target_weight": 0.15},
            {"date": run_date.isoformat(), "symbol": "SOXX", "target_weight": 0.00},
            {"date": run_date.isoformat(), "symbol": "CASH", "target_weight": 0.15},
        ]
    )

    risk = build_advanced_risk_report(
        allocation=allocation,
        prices=prices,
        config=config,
        quality_report=quality,
        run_date=run_date,
    )
    ml = build_ml_ranking_candidates(signals, p2_config=config.p2, run_date=run_date)
    optimizer = build_weight_optimizer_candidates(
        signals=signals,
        prices=prices,
        config=config,
        quality_report=quality,
        run_date=run_date,
    )
    ensemble = build_ensemble_candidates(signals, ml, p2_config=config.p2, run_date=run_date)
    preflight = build_live_interface_preflight(p2_config=config.p2, run_date=run_date)
    walk_forward = build_walk_forward_readiness_report(
        backtest_dir=tmp_path / "missing_backtests",
        p2_config=config.p2,
        run_date=run_date,
    )

    assert risk.iloc[0]["production_effect"] == "none"
    assert set(ml["model_stage"]) == {"candidate_only"}
    assert set(ml["production_effect"]) == {"none"}
    assert abs(float(optimizer["candidate_weight"].sum()) - 1.0) < 1e-8
    assert set(optimizer["model_stage"]) == {"candidate_only"}
    assert set(optimizer["production_effect"]) == {"none"}
    assert optimizer.loc[
        optimizer["symbol"] != "CASH",
        "candidate_weight",
    ].max() <= config.p2.weight_optimizer.max_candidate_weight + 1e-9
    assert optimizer.loc[
        optimizer["symbol"] == "CASH",
        "candidate_weight",
    ].iloc[0] >= config.p2.weight_optimizer.min_cash_weight - 1e-9
    assert set(ensemble["model_stage"]) == {"candidate_only"}
    assert preflight.iloc[0]["status"] == "BLOCKED_BY_POLICY"
    assert bool(preflight.iloc[0]["broker_order_route_called"]) is False
    assert walk_forward.iloc[0]["status"] == "NOT_READY"


def _weights_for_date(prices, config, report, run_date: date) -> dict[str, float]:
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    signal_frame = signals_to_frame(
        generate_signals_for_date(features, strategy=config.strategy, run_date=run_date)
    )
    regime = generate_regime_for_date(
        features,
        signal_frame,
        strategy=config.strategy,
        risk=config.risk,
        run_date=run_date,
    )
    allocation = allocate_portfolio(
        signal_frame,
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime=regime.regime,
        run_date=run_date,
        config_hash=config.config_hash,
        data_quality_report=report,
    )
    return weights_from_records(allocation)


def _make_prices(days: int, mode: str, symbols: list[str] | None = None) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=days)
    rows = []
    selected_symbols = symbols or ["SPY", "QQQ", "SMH", "SOXX"]
    for symbol_index, symbol in enumerate(selected_symbols):
        for index, current_date in enumerate(dates):
            if mode == "constant":
                price = 100.0 + symbol_index
            elif mode == "down":
                price = 200.0 - index * (80.0 / max(days - 1, 1)) + symbol_index
            else:
                price = 100.0 + index * (80.0 / max(days - 1, 1)) + symbol_index
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "fixture",
                    "created_at": "2026-05-31T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _make_risk_switch_prices(days: int) -> pd.DataFrame:
    dates = pd.bdate_range("2022-01-03", periods=days)
    rows = []
    for symbol in ["SPY", "QQQ", "SMH", "SOXX"]:
        for index, current_date in enumerate(dates):
            if symbol == "SPY" and index >= 215:
                price = 80.0
            else:
                price = 100.0 + index * 0.10
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000,
                    "source": "fixture",
                    "created_at": "2026-06-01T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)
