from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

from ai_trading_system.config import (
    PROJECT_ROOT,
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_portfolio,
    load_universe,
)
from ai_trading_system.data.download import download_daily_data
from ai_trading_system.data.quality import (
    default_quality_report_path,
    validate_data_cache,
    write_data_quality_report,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.scoring.position_model import ModuleScore, WeightedScoreModel

app = typer.Typer(help="AI trading trend analysis toolkit.", no_args_is_help=True)
console = Console()


@app.callback()
def main() -> None:
    """AI trading trend analysis toolkit."""


@app.command("score-example")
def score_example() -> None:
    """Print an example position recommendation."""
    model = WeightedScoreModel()
    portfolio = load_portfolio()
    recommendation = model.recommend(
        [
            ModuleScore(
                "trend",
                score=72,
                weight=25,
                reason="SMH 和 QQQ 仍在长期均线上方",
            ),
            ModuleScore("fundamentals", score=60, weight=25, reason="MVP 阶段中性占位"),
            ModuleScore("macro_liquidity", score=55, weight=15, reason="利率环境不算友好"),
            ModuleScore("risk_sentiment", score=66, weight=15, reason="VIX 仍可控"),
            ModuleScore("valuation", score=50, weight=10, reason="MVP 阶段中性占位"),
            ModuleScore(
                "policy_geopolitics",
                score=60,
                weight=10,
                reason="MVP 阶段中性占位",
            ),
        ],
        total_risk_asset_min=portfolio.portfolio.total_risk_asset_min,
        total_risk_asset_max=portfolio.portfolio.total_risk_asset_max,
    )
    console.print(render_recommendation_markdown(recommendation))


@app.command("download-data")
def download_data(
    start: Annotated[
        str,
        typer.Option(help="Start date, inclusive, in YYYY-MM-DD format."),
    ] = "2018-01-01",
    end: Annotated[
        str | None,
        typer.Option(help="End date, inclusive, in YYYY-MM-DD format."),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Output cache directory."),
    ] = PROJECT_ROOT / "data" / "raw",
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="Include every configured AI-chain ticker, not only the core watchlist.",
        ),
    ] = False,
) -> None:
    """Download daily market prices and FRED rate data into local CSV cache."""
    universe = load_universe()
    start_date = _parse_date(start)
    end_date = _parse_date(end) if end else date.today()

    summary = download_daily_data(
        universe,
        start=start_date,
        end=end_date,
        output_dir=output_dir,
        include_full_ai_chain=full_universe,
    )

    console.print("[green]Data cache updated.[/green]")
    console.print(f"Prices: {summary.prices_path} ({summary.price_rows} rows)")
    console.print(f"Rates:  {summary.rates_path} ({summary.rate_rows} rows)")
    console.print(f"Price tickers: {', '.join(summary.price_tickers)}")
    console.print(f"Rate series: {', '.join(summary.rate_series)}")


@app.command("validate-data")
def validate_data(
    prices_path: Annotated[
        Path,
        typer.Option(help="Path to standardized daily prices CSV."),
    ] = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv",
    rates_path: Annotated[
        Path,
        typer.Option(help="Path to standardized daily rates CSV."),
    ] = PROJECT_ROOT / "data" / "raw" / "rates_daily.csv",
    as_of: Annotated[
        str | None,
        typer.Option(help="Validation date in YYYY-MM-DD format. Defaults to today."),
    ] = None,
    output_path: Annotated[
        Path | None,
        typer.Option(help="Markdown report output path."),
    ] = None,
    full_universe: Annotated[
        bool,
        typer.Option(
            "--full-universe",
            help="Validate every configured AI-chain ticker, not only the core watchlist.",
        ),
    ] = False,
) -> None:
    """Validate cached data and write a Markdown quality report."""
    universe = load_universe()
    quality_config = load_data_quality()
    validation_date = _parse_date(as_of) if as_of else date.today()
    report_path = output_path or default_quality_report_path(
        PROJECT_ROOT / "outputs" / "reports",
        validation_date,
    )

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(
            universe,
            include_full_ai_chain=full_universe,
        ),
        expected_rate_series=configured_rate_series(universe),
        quality_config=quality_config,
        as_of=validation_date,
    )
    write_data_quality_report(report, report_path)

    status_style = "green" if report.status == "PASS" else "yellow" if report.passed else "red"
    console.print(f"[{status_style}]Data quality status: {report.status}[/{status_style}]")
    console.print(f"Report: {report_path}")
    console.print(f"Errors: {report.error_count}; warnings: {report.warning_count}")

    if not report.passed:
        raise typer.Exit(code=1)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter("Date must use YYYY-MM-DD format.") from exc


if __name__ == "__main__":
    app()
