from __future__ import annotations

from ai_trading_system.scoring.position_model import PositionRecommendation


def render_recommendation_markdown(recommendation: PositionRecommendation) -> str:
    lines = [
        "# AI 产业链每日仓位建议",
        "",
        f"- 总分：{recommendation.total_score:.1f}",
        f"- 状态：{recommendation.label}",
        (
            "- AI 仓位（股票风险资产内）："
            f"{recommendation.risk_asset_ai_band.min_position:.0%}-"
            f"{recommendation.risk_asset_ai_band.max_position:.0%}"
        ),
        (
            "- 股票/风险资产预算（总资产内）："
            f"{recommendation.total_risk_asset_band.min_position:.0%}-"
            f"{recommendation.total_risk_asset_band.max_position:.0%}"
        ),
        (
            "- AI 仓位（总资产内）："
            f"{recommendation.total_asset_ai_band.min_position:.0%}-"
            f"{recommendation.total_asset_ai_band.max_position:.0%}"
        ),
        "",
        "## 评分明细",
        "",
    ]
    for component in recommendation.components:
        lines.append(
            f"- {component.name}: score={component.score:.1f}, "
            f"weight={component.weight:.1f}, reason={component.reason}"
        )
    return "\n".join(lines)
