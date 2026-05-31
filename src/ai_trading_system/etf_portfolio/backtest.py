from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from math import sqrt
from pathlib import Path
from statistics import mean, pstdev

import pandas as pd

from ai_trading_system.backtest.engine import BacktestMetrics, summarize_long_only_backtest
from ai_trading_system.etf_portfolio.allocation import (
    allocate_portfolio,
    weights_from_records,
)
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import (
    ETFBenchmarkConfig,
    ETFConfigBundle,
    ETFQualityReport,
)
from ai_trading_system.etf_portfolio.no_lookahead import (
    raise_for_no_lookahead_violations,
    validate_no_lookahead_records,
)
from ai_trading_system.etf_portfolio.regime import generate_regime_for_date
from ai_trading_system.etf_portfolio.signals import generate_signals_for_date, signals_to_frame
from ai_trading_system.etf_portfolio.stability import (
    build_allocation_stability_diagnostics,
    write_allocation_stability_diagnostics,
)


@dataclass(frozen=True)
class ETFBacktestRun:
    run_id: str
    requested_start: date
    requested_end: date
    market_regime: str
    daily: pd.DataFrame
    weights: pd.DataFrame
    trades: pd.DataFrame
    summary: dict[str, object]


@dataclass(frozen=True)
class ETFBenchmarkResult:
    benchmark_id: str
    benchmark: ETFBenchmarkConfig
    metrics: BacktestMetrics


@dataclass(frozen=True)
class ETFAccountingStep:
    signal_date: date
    execution_date: date
    return_date: date
    period_returns: dict[str, float]
    asset_contributions: dict[str, float]
    turnover: float
    gross_return: float
    transaction_cost: float
    strategy_return: float
    ending_equity: float


def run_portfolio_backtest(
    prices: pd.DataFrame,
    *,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    start: date | None = None,
    end: date | None = None,
    fast: bool = False,
) -> ETFBacktestRun:
    settings = config.backtest.backtest
    run_start = start or settings.start_date
    run_end = end or _latest_date(prices)
    if run_start >= run_end:
        raise ValueError("ETF backtest start date must be before end date")
    feature_start = settings.warmup_start_date
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=feature_start,
        end=run_end,
    )
    close_pivot = _price_pivot(prices, settings.price_field)
    trading_dates = [
        item.date()
        for item in close_pivot.index
        if run_start <= item.date() <= run_end
    ]
    if fast:
        trading_dates = trading_dates[-90:]
    minimum_trading_dates = int(settings.signal_lag_days) + 2
    if len(trading_dates) < minimum_trading_dates:
        raise ValueError("ETF backtest has too few trading dates for configured signal lag")

    previous_weights: dict[str, float] | None = None
    previous_regime: str | None = None
    rows: list[dict[str, object]] = []
    weight_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    portfolio_equity = 1.0
    signal_lag_days = int(settings.signal_lag_days)
    for index, signal_date in enumerate(trading_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(trading_dates):
            break
        execution_date = trading_dates[execution_index]
        return_date = trading_dates[return_index]
        signal_records = generate_signals_for_date(
            features,
            strategy=config.strategy,
            run_date=signal_date,
        )
        signal_frame = signals_to_frame(signal_records)
        regime_record = generate_regime_for_date(
            features,
            signal_frame,
            strategy=config.strategy,
            risk=config.risk,
            run_date=signal_date,
            previous_regime=previous_regime,
        )
        allocation_records = allocate_portfolio(
            signal_frame,
            assets=config.assets,
            strategy=config.strategy,
            risk=config.risk,
            regime=regime_record.regime,
            run_date=signal_date,
            config_hash=config.config_hash,
            data_quality_report=quality_report,
            previous_weights=previous_weights,
        )
        target_weights = weights_from_records(allocation_records)
        accounting = calculate_portfolio_accounting_step(
            close_pivot,
            signal_date=signal_date,
            execution_date=execution_date,
            return_date=return_date,
            target_weights=target_weights,
            previous_weights=previous_weights,
            asset_symbols=tuple(config.assets.assets),
            total_cost_bps=_total_cost_bps(config),
            starting_equity=portfolio_equity,
        )
        portfolio_equity = accounting.ending_equity
        rows.append(
            {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "market_regime": settings.regime,
                "regime": regime_record.regime,
                "gross_return": accounting.gross_return,
                "turnover": accounting.turnover,
                "transaction_cost": accounting.transaction_cost,
                "strategy_return": accounting.strategy_return,
                "portfolio_value": settings.initial_capital * portfolio_equity,
                "asset_returns_json": json.dumps(
                    accounting.period_returns,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "asset_contributions_json": json.dumps(
                    accounting.asset_contributions,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "target_weights_json": json.dumps(
                    target_weights,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                "signal_execution_lag_days": settings.signal_lag_days,
                "data_quality_status": quality_report.status,
                "model_version": config.strategy.model.version,
                "config_hash": config.config_hash,
            }
        )
        before_weights = previous_weights or {"CASH": 1.0}
        for record in allocation_records:
            current_weight = before_weights.get(record.symbol, 0.0)
            trade_delta = target_weights.get(record.symbol, 0.0) - current_weight
            common = {
                "signal_date": signal_date.isoformat(),
                "execution_date": execution_date.isoformat(),
                "return_date": return_date.isoformat(),
                "symbol": record.symbol,
                "current_weight": current_weight,
                "target_weight": target_weights.get(record.symbol, 0.0),
                "trade_delta": trade_delta,
                "regime": regime_record.regime,
                "model_version": config.strategy.model.version,
                "config_hash": config.config_hash,
                "data_quality_status": quality_report.status,
            }
            weight_rows.append(
                {
                    **common,
                    "constraints_applied": json.dumps(
                        list(record.constraints_applied),
                        ensure_ascii=False,
                    ),
                }
            )
            trade_rows.append(
                {
                    **common,
                    "trade_required": abs(trade_delta)
                    >= config.strategy.model.min_rebalance_delta,
                    "turnover": accounting.turnover,
                    "transaction_cost": accounting.transaction_cost,
                }
            )
        previous_weights = target_weights
        previous_regime = regime_record.regime

    daily = pd.DataFrame(rows)
    weights = pd.DataFrame(weight_rows)
    trades = pd.DataFrame(trade_rows)
    raise_for_no_lookahead_violations(
        validate_no_lookahead_records(
            backtest_records=daily,
            allocation_records=weights,
            trade_records=trades,
        )
    )
    strategy_returns = [float(value) for value in daily["strategy_return"]]
    turnovers = [float(value) for value in daily["turnover"]]
    exposures = [
        1.0 - json.loads(str(weights_json)).get("CASH", 0.0)
        for weights_json in daily["target_weights_json"]
    ]
    strategy_metrics = summarize_long_only_backtest(strategy_returns, exposures, turnovers)
    benchmark_metrics = _benchmark_metrics(close_pivot, trading_dates, config, fast=fast)
    effective_start = date.fromisoformat(str(daily.iloc[0]["signal_date"]))
    effective_end = date.fromisoformat(str(daily.iloc[-1]["signal_date"]))
    allocation_stability_diagnostics = build_allocation_stability_diagnostics(
        daily,
        weights,
        max_daily_turnover=config.risk.portfolio_constraints.max_daily_turnover,
        max_rebalance_trade_weight=config.risk.portfolio_constraints.max_rebalance_trade_weight,
    )
    summary = _summary_payload(
        config=config,
        quality_report=quality_report,
        requested_start=run_start,
        requested_end=run_end,
        first_signal_date=effective_start,
        last_signal_date=effective_end,
        strategy_metrics=strategy_metrics,
        strategy_returns=strategy_returns,
        benchmark_metrics=benchmark_metrics,
        weights=weights,
        allocation_stability_diagnostics=allocation_stability_diagnostics,
        row_count=len(daily),
        fast=fast,
    )
    run_id = f"etf-backtest-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    return ETFBacktestRun(
        run_id=run_id,
        requested_start=run_start,
        requested_end=run_end,
        market_regime=settings.regime,
        daily=daily,
        weights=weights,
        trades=trades,
        summary=summary,
    )


def write_backtest_run(
    run: ETFBacktestRun,
    output_root: Path,
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path]:
    run_dir = output_root / run.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    daily_path = run_dir / "daily.csv"
    weights_path = run_dir / "weights.csv"
    trades_path = run_dir / "trades.csv"
    summary_json_path = run_dir / "summary.json"
    metrics_json_path = run_dir / "metrics.json"
    summary_md_path = run_dir / "summary.md"
    stability_json_path = run_dir / "stability_diagnostics.json"
    stability_md_path = run_dir / "stability_diagnostics.md"
    run.daily.to_csv(daily_path, index=False)
    run.weights.to_csv(weights_path, index=False)
    run.trades.to_csv(trades_path, index=False)
    summary_json_path.write_text(
        json.dumps(run.summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    metrics_json_path.write_text(
        json.dumps(_metrics_document(run.summary), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    summary_md_path.write_text(render_backtest_summary(run), encoding="utf-8")
    write_allocation_stability_diagnostics(
        dict(run.summary["allocation_stability_diagnostics"]),
        stability_json_path,
        stability_md_path,
    )
    return (
        daily_path,
        weights_path,
        trades_path,
        summary_json_path,
        metrics_json_path,
        summary_md_path,
        stability_json_path,
        stability_md_path,
    )


def benchmark_registry(config: ETFConfigBundle) -> dict[str, ETFBenchmarkConfig]:
    settings = config.backtest.backtest
    configured = dict(settings.benchmarks)
    if not configured:
        configured = _legacy_benchmark_registry(config)
    active = set(settings.baselines)
    return {
        benchmark_id: benchmark
        for benchmark_id, benchmark in configured.items()
        if benchmark_id in active or benchmark.name in active
    }


def benchmark_weights_for_date(
    *,
    config: ETFConfigBundle,
    benchmark_id: str,
    prices: pd.DataFrame,
    signal_date: date,
) -> dict[str, float]:
    registry = benchmark_registry(config)
    if benchmark_id not in registry:
        raise ValueError(f"unknown ETF benchmark id: {benchmark_id}")
    close_pivot = _price_pivot(prices, config.backtest.backtest.price_field)
    return _benchmark_weights_for_signal_date(registry[benchmark_id], close_pivot, signal_date)


def render_backtest_summary(run: ETFBacktestRun) -> str:
    metrics = run.summary["strategy_metrics"]
    benchmark_rows = run.summary["benchmark_metrics"]
    lines = [
        f"# ETF Portfolio Backtest - {run.run_id}",
        "",
        f"- Market Regime：{run.market_regime}",
        (
            f"- Requested Date Range：{run.requested_start.isoformat()} 至 "
            f"{run.requested_end.isoformat()}"
        ),
        (
            f"- Effective Signal Range：{run.summary['first_signal_date']} 至 "
            f"{run.summary['last_signal_date']}"
        ),
        f"- Fast Smoke Mode：{str(run.summary['fast']).lower()}",
        f"- Signal Execution Lag：{run.summary['signal_execution_lag_days']} trading day",
        f"- Data Quality：{run.summary['data_quality_status']}",
        f"- Model Version：{run.summary['model_version']}",
        f"- Config Hash：`{run.summary['config_hash']}`",
        "",
        "## Strategy Metrics",
        "",
        _metric_line(metrics),
        "",
        "## Strategy Extended Metrics",
        "",
        _extended_metric_line(run.summary["strategy_extended_metrics"]),
        "",
        "## Allocation Stability Diagnostics",
        "",
        *_stability_lines(run.summary["allocation_stability_diagnostics"]),
        "",
        "## Benchmarks",
        "",
        "| ID | Benchmark | Total Return | CAGR | Max Drawdown | Sharpe |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for benchmark_id, item in benchmark_rows.items():
        lines.append(
            "| "
            f"{benchmark_id} | "
            f"{item['benchmark_name']} | "
            f"{item['total_return']:.2%} | "
            f"{item['cagr']:.2%} | "
            f"{item['max_drawdown']:.2%} | "
            f"{_fmt_optional(item['sharpe'])} |"
        )
    lines.extend(
        [
            "",
            "## Generated Artifacts",
            "",
            (
                "- `daily.csv`：逐日 signal/execution/return date、组合收益、"
                "资产收益 JSON、资产收益贡献 JSON 和目标权重 JSON。"
            ),
            "- `weights.csv`：逐日目标权重历史。",
            "- `trades.csv`：逐日调仓 delta、turnover 和交易成本审计。",
            "- `metrics.json`：strategy / benchmark 指标摘要，便于实验 registry 读取。",
            "- `stability_diagnostics.json/md`：allocation stability 诊断摘要。",
            "",
            "## Correctness Notes",
            "",
            (
                "- 信号使用 signal_date 当日及之前可见数据，目标权重在下一交易日 "
                "execution_date 执行，收益窗口为 execution_date 到 return_date。"
            ),
            "- 交易成本按 turnover * (commission_bps + slippage_bps) 扣除。",
            "- 2022-12-01 前历史只作为 warm-up，不作为默认 AI regime 主结论窗口。",
        ]
    )
    return "\n".join(lines) + "\n"


def calculate_portfolio_accounting_step(
    close_pivot: pd.DataFrame,
    *,
    signal_date: date,
    execution_date: date,
    return_date: date,
    target_weights: dict[str, float],
    previous_weights: dict[str, float] | None,
    asset_symbols: tuple[str, ...],
    total_cost_bps: float,
    starting_equity: float = 1.0,
) -> ETFAccountingStep:
    if execution_date <= signal_date:
        raise ValueError("ETF execution_date must be after signal_date")
    if return_date <= execution_date:
        raise ValueError("ETF return_date must be after execution_date")
    _assert_accounting_weights_sum_to_one(target_weights)
    period_returns = _period_returns(close_pivot, execution_date, return_date)
    asset_contributions = {
        symbol: target_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
        for symbol in asset_symbols
    }
    turnover = _turnover(target_weights, previous_weights)
    gross_return = sum(asset_contributions.get(symbol, 0.0) for symbol in asset_symbols)
    transaction_cost = turnover * total_cost_bps / 10_000.0
    strategy_return = gross_return - transaction_cost
    return ETFAccountingStep(
        signal_date=signal_date,
        execution_date=execution_date,
        return_date=return_date,
        period_returns=period_returns,
        asset_contributions=asset_contributions,
        turnover=turnover,
        gross_return=gross_return,
        transaction_cost=transaction_cost,
        strategy_return=strategy_return,
        ending_equity=starting_equity * (1.0 + strategy_return),
    )


def toy_portfolio_return(weight: float, asset_return: float) -> float:
    return weight * asset_return


def _summary_payload(
    *,
    config: ETFConfigBundle,
    quality_report: ETFQualityReport,
    requested_start: date,
    requested_end: date,
    first_signal_date: date,
    last_signal_date: date,
    strategy_metrics: BacktestMetrics,
    benchmark_metrics: dict[str, ETFBenchmarkResult],
    strategy_returns: list[float],
    weights: pd.DataFrame,
    allocation_stability_diagnostics: dict[str, object],
    row_count: int,
    fast: bool,
) -> dict[str, object]:
    strategy_payload = _metrics_payload(strategy_metrics)
    benchmark_payload = _benchmark_metrics_payload(benchmark_metrics)
    return {
        "market_regime": config.backtest.backtest.regime,
        "requested_start": requested_start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "first_signal_date": first_signal_date.isoformat(),
        "last_signal_date": last_signal_date.isoformat(),
        "row_count": row_count,
        "fast": fast,
        "signal_execution_lag_days": config.backtest.backtest.signal_lag_days,
        "data_quality_status": quality_report.status,
        "model_version": config.strategy.model.version,
        "config_hash": config.config_hash,
        "strategy_metrics": strategy_payload,
        "strategy_extended_metrics": _extended_metrics(strategy_returns, weights),
        "allocation_stability_diagnostics": allocation_stability_diagnostics,
        "benchmark_metrics": benchmark_payload,
        "benchmark_relative_return": {
            name: strategy_payload["total_return"] - item["total_return"]
            for name, item in benchmark_payload.items()
        },
        "benchmark_comparisons": _benchmark_comparisons(strategy_payload, benchmark_payload),
    }


def _metrics_payload(metrics: BacktestMetrics) -> dict[str, float | None]:
    return {
        "total_return": metrics.total_return,
        "cagr": metrics.cagr,
        "max_drawdown": metrics.max_drawdown,
        "sharpe": metrics.sharpe,
        "sortino": metrics.sortino,
        "calmar": metrics.calmar,
        "time_in_market": metrics.time_in_market,
        "turnover": metrics.turnover,
    }


def _metrics_document(summary: dict[str, object]) -> dict[str, object]:
    return {
        "market_regime": summary["market_regime"],
        "requested_start": summary["requested_start"],
        "requested_end": summary["requested_end"],
        "first_signal_date": summary["first_signal_date"],
        "last_signal_date": summary["last_signal_date"],
        "data_quality_status": summary["data_quality_status"],
        "model_version": summary["model_version"],
        "config_hash": summary["config_hash"],
        "strategy_metrics": summary["strategy_metrics"],
        "strategy_extended_metrics": summary["strategy_extended_metrics"],
        "allocation_stability_diagnostics": summary["allocation_stability_diagnostics"],
        "benchmark_metrics": summary["benchmark_metrics"],
        "benchmark_relative_return": summary["benchmark_relative_return"],
        "benchmark_comparisons": summary["benchmark_comparisons"],
    }


def _benchmark_metrics_payload(
    benchmark_results: dict[str, ETFBenchmarkResult],
) -> dict[str, dict[str, object]]:
    payload: dict[str, dict[str, object]] = {}
    for benchmark_id, result in benchmark_results.items():
        metrics = _metrics_payload(result.metrics)
        payload[benchmark_id] = {
            "benchmark_id": benchmark_id,
            "benchmark_name": result.benchmark.name,
            "benchmark_type": result.benchmark.benchmark_type,
            **metrics,
        }
    return payload


def _benchmark_comparisons(
    strategy_payload: dict[str, float | None],
    benchmark_payload: dict[str, dict[str, object]],
) -> list[dict[str, object]]:
    comparisons: list[dict[str, object]] = []
    strategy_cagr = float(strategy_payload["cagr"] or 0.0)
    strategy_max_drawdown = float(strategy_payload["max_drawdown"] or 0.0)
    strategy_turnover = float(strategy_payload["turnover"] or 0.0)
    for benchmark_id, benchmark in benchmark_payload.items():
        benchmark_cagr = float(benchmark["cagr"] or 0.0)
        benchmark_max_drawdown = float(benchmark["max_drawdown"] or 0.0)
        comparisons.append(
            {
                "benchmark_id": benchmark_id,
                "benchmark_name": benchmark["benchmark_name"],
                "strategy_cagr": strategy_payload["cagr"],
                "benchmark_cagr": benchmark["cagr"],
                "excess_cagr": strategy_cagr - benchmark_cagr,
                "strategy_max_drawdown": strategy_payload["max_drawdown"],
                "benchmark_max_drawdown": benchmark["max_drawdown"],
                "drawdown_reduction": abs(benchmark_max_drawdown) - abs(strategy_max_drawdown),
                "strategy_sharpe": strategy_payload["sharpe"],
                "benchmark_sharpe": benchmark["sharpe"],
                "strategy_calmar": strategy_payload["calmar"],
                "benchmark_calmar": benchmark["calmar"],
                "strategy_turnover": strategy_payload["turnover"],
                "benchmark_turnover": benchmark["turnover"],
                "turnover_difference": strategy_turnover - float(benchmark["turnover"] or 0.0),
            }
        )
    return comparisons


def _extended_metrics(strategy_returns: list[float], weights: pd.DataFrame) -> dict[str, object]:
    equity_curve: list[float] = []
    running = 1.0
    for period_return in strategy_returns:
        running *= 1.0 + period_return
        equity_curve.append(running)
    drawdowns = _drawdown_series(equity_curve)
    exposure_by_asset: dict[str, float] = {}
    if not weights.empty:
        grouped = weights.groupby("symbol")["target_weight"].mean()
        exposure_by_asset = {str(symbol): float(value) for symbol, value in grouped.items()}
    return {
        "annualized_volatility": _annualized_volatility(strategy_returns),
        "win_rate_daily": mean(1.0 if item > 0 else 0.0 for item in strategy_returns),
        "average_drawdown": mean(drawdowns) if drawdowns else 0.0,
        "best_day": max(strategy_returns),
        "worst_day": min(strategy_returns),
        "exposure_by_asset": exposure_by_asset,
    }


def _benchmark_metrics(
    close_pivot: pd.DataFrame,
    trading_dates: list[date],
    config: ETFConfigBundle,
    *,
    fast: bool,
) -> dict[str, ETFBenchmarkResult]:
    metrics: dict[str, ETFBenchmarkResult] = {}
    benchmark_dates = trading_dates[-90:] if fast else trading_dates
    signal_lag_days = int(config.backtest.backtest.signal_lag_days)
    for benchmark_id, benchmark in benchmark_registry(config).items():
        returns, exposures, turnovers = _benchmark_return_series(
            benchmark,
            close_pivot,
            benchmark_dates,
            signal_lag_days=signal_lag_days,
        )
        if not returns:
            continue
        metrics[benchmark_id] = ETFBenchmarkResult(
            benchmark_id=benchmark_id,
            benchmark=benchmark,
            metrics=summarize_long_only_backtest(returns, exposures, turnovers),
        )
    return metrics


def _legacy_benchmark_registry(config: ETFConfigBundle) -> dict[str, ETFBenchmarkConfig]:
    registry: dict[str, ETFBenchmarkConfig] = {}
    for index, symbol in enumerate(config.backtest.backtest.benchmark_assets, start=1):
        registry[f"LEGACY{index:03d}"] = ETFBenchmarkConfig(
            name=f"buy_and_hold_{symbol.lower()}",
            benchmark_type="buy_and_hold",
            symbol=symbol,
        )
    if "static_default_portfolio" in config.backtest.backtest.baselines:
        registry["LEGACY_STATIC_DEFAULT"] = ETFBenchmarkConfig(
            name="static_default_portfolio",
            benchmark_type="static_portfolio",
            weights={
                symbol: asset.default_weight for symbol, asset in config.assets.assets.items()
            },
        )
    if "ma_50_200_qqq" in config.backtest.backtest.baselines:
        registry["LEGACY_MA_QQQ"] = ETFBenchmarkConfig(
            name="ma_50_200_qqq",
            benchmark_type="moving_average",
            symbol="QQQ",
            short_window=50,
            long_window=200,
        )
    return registry


def _benchmark_return_series(
    benchmark: ETFBenchmarkConfig,
    close_pivot: pd.DataFrame,
    benchmark_dates: list[date],
    *,
    signal_lag_days: int,
) -> tuple[list[float], list[float], list[float]]:
    returns: list[float] = []
    exposures: list[float] = []
    turnovers: list[float] = []
    previous_weights: dict[str, float] | None = None
    for index, signal_date in enumerate(benchmark_dates):
        execution_index = index + signal_lag_days
        return_index = execution_index + 1
        if return_index >= len(benchmark_dates):
            break
        execution_date = benchmark_dates[execution_index]
        return_date = benchmark_dates[return_index]
        weights = _benchmark_weights_for_signal_date(benchmark, close_pivot, signal_date)
        period_returns = _period_returns(close_pivot, execution_date, return_date)
        returns.append(
            sum(weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0) for symbol in weights)
        )
        exposures.append(1.0 - weights.get(benchmark.cash_symbol, 0.0))
        turnovers.append(0.0 if previous_weights is None else _turnover(weights, previous_weights))
        previous_weights = weights
    return returns, exposures, turnovers


def _benchmark_weights_for_signal_date(
    benchmark: ETFBenchmarkConfig,
    close_pivot: pd.DataFrame,
    signal_date: date,
) -> dict[str, float]:
    cash_symbol = benchmark.cash_symbol
    if benchmark.benchmark_type == "buy_and_hold":
        return {str(benchmark.symbol): 1.0}
    if benchmark.benchmark_type == "static_portfolio":
        return {symbol: float(weight) for symbol, weight in benchmark.weights.items()}
    if benchmark.benchmark_type == "moving_average":
        if _moving_average_signal_is_on(
            close_pivot,
            str(benchmark.symbol),
            signal_date,
            int(benchmark.short_window or 0),
            int(benchmark.long_window or 0),
        ):
            return {str(benchmark.symbol): 1.0}
        return {cash_symbol: 1.0}
    if benchmark.benchmark_type == "risk_off_cash_switch":
        if _price_above_moving_average(
            close_pivot,
            str(benchmark.signal_symbol),
            signal_date,
            int(benchmark.long_window or 0),
        ):
            return {str(benchmark.symbol): 1.0}
        return {cash_symbol: 1.0}
    raise ValueError(f"unsupported ETF benchmark type: {benchmark.benchmark_type}")


def _price_pivot(prices: pd.DataFrame, price_field: str) -> pd.DataFrame:
    frame = prices.copy()
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["_price"] = pd.to_numeric(frame[price_field], errors="coerce")
    pivot = frame.pivot(index="_date", columns="symbol", values="_price").sort_index()
    return pivot.dropna(how="all")


def _period_returns(close_pivot: pd.DataFrame, left: date, right: date) -> dict[str, float]:
    left_row = close_pivot.loc[pd.Timestamp(left)]
    right_row = close_pivot.loc[pd.Timestamp(right)]
    returns: dict[str, float] = {}
    for symbol in close_pivot.columns:
        left_value = left_row.get(symbol)
        right_value = right_row.get(symbol)
        if pd.isna(left_value) or pd.isna(right_value) or float(left_value) == 0:
            continue
        returns[str(symbol)] = float(right_value) / float(left_value) - 1.0
    returns["CASH"] = 0.0
    return returns


def _asset_contributions(
    target_weights: dict[str, float],
    period_returns: dict[str, float],
    config: ETFConfigBundle,
) -> dict[str, float]:
    return {
        symbol: target_weights.get(symbol, 0.0) * period_returns.get(symbol, 0.0)
        for symbol in config.assets.assets
    }


def _assert_accounting_weights_sum_to_one(target_weights: dict[str, float]) -> None:
    total = sum(float(value) for value in target_weights.values())
    if abs(total - 1.0) > 1e-6:
        raise ValueError(f"ETF accounting weights must sum to 1.0, got {total:.8f}")


def _ma_50_200_qqq_returns(
    close_pivot: pd.DataFrame,
    benchmark_dates: list[date],
) -> tuple[list[float], list[float]]:
    if "QQQ" not in close_pivot.columns:
        period_count = max(0, len(benchmark_dates) - 1)
        return [0.0] * period_count, [0.0] * period_count
    qqq = pd.to_numeric(close_pivot["QQQ"], errors="coerce")
    ma_50 = qqq.rolling(50, min_periods=50).mean()
    ma_200 = qqq.rolling(200, min_periods=200).mean()
    returns: list[float] = []
    exposures: list[float] = []
    for left, right in zip(benchmark_dates[:-1], benchmark_dates[1:], strict=True):
        left_ts = pd.Timestamp(left)
        invested = (
            left_ts in qqq.index
            and pd.notna(qqq.loc[left_ts])
            and pd.notna(ma_50.loc[left_ts])
            and pd.notna(ma_200.loc[left_ts])
            and float(qqq.loc[left_ts]) > float(ma_50.loc[left_ts])
            and float(qqq.loc[left_ts]) > float(ma_200.loc[left_ts])
        )
        exposure = 1.0 if invested else 0.0
        exposures.append(exposure)
        qqq_return = _period_returns(close_pivot, left, right).get("QQQ", 0.0)
        returns.append(qqq_return if invested else 0.0)
    return returns, exposures


def _moving_average_signal_is_on(
    close_pivot: pd.DataFrame,
    symbol: str,
    signal_date: date,
    short_window: int,
    long_window: int,
) -> bool:
    return _price_above_moving_average(
        close_pivot,
        symbol,
        signal_date,
        short_window,
    ) and _price_above_moving_average(
        close_pivot,
        symbol,
        signal_date,
        long_window,
    )


def _price_above_moving_average(
    close_pivot: pd.DataFrame,
    symbol: str,
    signal_date: date,
    window: int,
) -> bool:
    if symbol not in close_pivot.columns or window <= 0:
        return False
    series = pd.to_numeric(close_pivot[symbol], errors="coerce")
    moving_average = series.rolling(window, min_periods=window).mean()
    signal_ts = pd.Timestamp(signal_date)
    return (
        signal_ts in series.index
        and pd.notna(series.loc[signal_ts])
        and pd.notna(moving_average.loc[signal_ts])
        and float(series.loc[signal_ts]) > float(moving_average.loc[signal_ts])
    )


def _turnover(
    target_weights: dict[str, float],
    previous_weights: dict[str, float] | None,
) -> float:
    if previous_weights is None:
        return sum(abs(value) for key, value in target_weights.items() if key != "CASH")
    symbols = set(target_weights) | set(previous_weights)
    return sum(
        abs(target_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
        for symbol in symbols
    )


def _latest_date(prices: pd.DataFrame) -> date:
    parsed = pd.to_datetime(prices["date"], errors="coerce").dropna()
    if parsed.empty:
        raise ValueError("ETF backtest price data has no valid dates")
    return parsed.max().date()


def _total_cost_bps(config: ETFConfigBundle) -> float:
    costs = config.risk.transaction_costs
    return costs.commission_bps + costs.slippage_bps


def _metric_line(metrics: object) -> str:
    if isinstance(metrics, dict):
        payload = metrics
    else:
        payload = _metrics_payload(metrics)
    return (
        f"- Total Return：{payload['total_return']:.2%}；"
        f"CAGR：{payload['cagr']:.2%}；"
        f"Max Drawdown：{payload['max_drawdown']:.2%}；"
        f"Sharpe：{_fmt_optional(payload['sharpe'])}；"
        f"Turnover：{payload['turnover']:.2f}"
    )


def _extended_metric_line(metrics: object) -> str:
    if not isinstance(metrics, dict):
        return "- n/a"
    exposure = metrics.get("exposure_by_asset", {})
    if isinstance(exposure, dict):
        exposure_text = ", ".join(
            f"{symbol}={float(value):.1%}" for symbol, value in sorted(exposure.items())
        )
    else:
        exposure_text = "n/a"
    return (
        f"- Annualized Volatility：{float(metrics['annualized_volatility']):.2%}；"
        f"Daily Win Rate：{float(metrics['win_rate_daily']):.2%}；"
        f"Average Drawdown：{float(metrics['average_drawdown']):.2%}；"
        f"Best/Worst Day：{float(metrics['best_day']):.2%} / "
        f"{float(metrics['worst_day']):.2%}；"
        f"Average Exposure：{exposure_text}"
    )


def _stability_lines(payload: object) -> list[str]:
    if not isinstance(payload, dict):
        return ["- Allocation stability diagnostics unavailable."]
    return [
        f"- Stability Status：{payload.get('status', 'UNKNOWN')}",
        f"- Rebalance Frequency：{_fmt_optional_pct(payload.get('rebalance_frequency'))}",
        f"- Average Daily Turnover：{_fmt_optional_pct(payload.get('daily_turnover_average'))}",
        f"- Max Rebalance Turnover：{_fmt_optional_pct(payload.get('max_rebalance_turnover'))}",
        (
            "- Max Single-Day Weight Delta After Initial："
            f"{_fmt_optional_pct(payload.get('max_single_day_weight_delta_after_initial'))}"
        ),
        f"- Constraint Hit Rate：{_fmt_optional_pct(payload.get('constraint_hit_rate'))}",
        f"- Cash Weight Average：{_fmt_optional_pct(payload.get('cash_weight_average'))}",
        (
            "- Semiconductor Exposure Average："
            f"{_fmt_optional_pct(payload.get('semiconductor_exposure_average'))}"
        ),
    ]


def _annualized_volatility(returns: list[float], periods_per_year: int = 252) -> float:
    if len(returns) < 2:
        return 0.0
    return pstdev(returns) * sqrt(periods_per_year)


def _drawdown_series(equity_curve: list[float]) -> list[float]:
    if not equity_curve:
        return []
    peak = equity_curve[0]
    drawdowns: list[float] = []
    for value in equity_curve:
        peak = max(peak, value)
        drawdowns.append(value / peak - 1.0)
    return drawdowns


def _fmt_optional(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"


def _fmt_optional_pct(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2%}"
