from __future__ import annotations

import typer
from rich.console import Console

from ai_trading_system.config import load_portfolio
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


if __name__ == "__main__":
    app()
