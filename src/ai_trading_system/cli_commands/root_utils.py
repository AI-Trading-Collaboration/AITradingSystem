from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

import typer
import yaml
from rich.console import Console

from ai_trading_system.config import ScoringRulesConfig, load_portfolio, load_scoring_rules
from ai_trading_system.explain import (
    DEFAULT_ARTIFACT_CATALOG_PATH,
    DEFAULT_CALCULATION_LOGIC_PATH,
    DEFAULT_FIELDS_PATH,
    explain_query,
    render_explain_result,
)
from ai_trading_system.reports.daily import render_recommendation_markdown
from ai_trading_system.scoring.position_model import (
    ModuleScore,
    PositionBandRule,
    WeightedScoreModel,
)

console = Console()


def register_root_utility_commands(app: typer.Typer) -> None:
    app.command("explain")(explain_command)
    app.command("score-example")(score_example)


def explain_command(
    query: Annotated[str, typer.Argument(help="要解释的字段、gate 或 artifact 名称。")],
    kind: Annotated[
        Literal["auto", "field", "artifact", "gate"],
        typer.Option(help="解释类型；auto 会依次查询字段、gate 和 artifact。"),
    ] = "auto",
    fields_path: Annotated[
        Path,
        typer.Option(help="字段字典 YAML 路径。"),
    ] = DEFAULT_FIELDS_PATH,
    artifact_catalog_path: Annotated[
        Path,
        typer.Option(help="artifact catalog Markdown 路径。"),
    ] = DEFAULT_ARTIFACT_CATALOG_PATH,
    calculation_logic_path: Annotated[
        Path,
        typer.Option(help="计算逻辑文档路径，用于 gate 解释来源标注。"),
    ] = DEFAULT_CALCULATION_LOGIC_PATH,
) -> None:
    """只读解释字段、gate 或 artifact 来源，不运行上游、不重算投资结论。"""
    try:
        result = explain_query(
            query,
            kind=kind,
            fields_path=fields_path,
            artifact_catalog_path=artifact_catalog_path,
            calculation_logic_path=calculation_logic_path,
        )
    except (OSError, ValueError, yaml.YAMLError) as exc:
        raise typer.BadParameter(str(exc)) from exc
    console.print(render_explain_result(result))
    if not result.get("found"):
        raise typer.Exit(code=1)


def score_example() -> None:
    """输出一份示例仓位建议。"""
    scoring_rules = load_scoring_rules()
    model = WeightedScoreModel(position_bands=_configured_position_band_rules(scoring_rules))
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


def _configured_position_band_rules(
    scoring_rules: ScoringRulesConfig,
) -> tuple[PositionBandRule, ...]:
    return tuple(
        PositionBandRule(
            min_score=band.min_score,
            min_position=band.min_position,
            max_position=band.max_position,
            label=band.label,
        )
        for band in scoring_rules.position_bands
    )
