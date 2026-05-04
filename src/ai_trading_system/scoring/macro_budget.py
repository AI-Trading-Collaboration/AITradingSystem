from __future__ import annotations

from dataclasses import dataclass

from ai_trading_system.config import MacroRiskAssetBudgetConfig
from ai_trading_system.features.market import MarketFeatureSet
from ai_trading_system.scoring.position_model import PositionBand


@dataclass(frozen=True)
class MacroRiskAssetBudgetAdjustment:
    static_total_risk_asset_band: PositionBand
    adjusted_total_risk_asset_band: PositionBand
    level: str
    triggered: bool
    reasons: tuple[str, ...]
    source: str = "config/portfolio.yaml:macro_risk_asset_budget"


def build_macro_risk_asset_budget_adjustment(
    *,
    static_total_risk_asset_min: float,
    static_total_risk_asset_max: float,
    feature_set: MarketFeatureSet,
    config: MacroRiskAssetBudgetConfig | None,
) -> MacroRiskAssetBudgetAdjustment:
    static_band = PositionBand(
        static_total_risk_asset_min,
        static_total_risk_asset_max,
        "静态总风险资产预算",
    )
    if config is None:
        return MacroRiskAssetBudgetAdjustment(
            static_total_risk_asset_band=static_band,
            adjusted_total_risk_asset_band=PositionBand(
                static_total_risk_asset_min,
                static_total_risk_asset_max,
                "总风险资产预算",
            ),
            level="not_configured",
            triggered=False,
            reasons=("未传入 macro_risk_asset_budget 配置，沿用静态预算。",),
        )
    if not config.enabled:
        return MacroRiskAssetBudgetAdjustment(
            static_total_risk_asset_band=static_band,
            adjusted_total_risk_asset_band=PositionBand(
                static_total_risk_asset_min,
                static_total_risk_asset_max,
                "总风险资产预算",
            ),
            level="disabled",
            triggered=False,
            reasons=("macro_risk_asset_budget 未启用，沿用静态预算。",),
        )

    stress_reasons = _trigger_reasons(feature_set, config, level="stress")
    if stress_reasons:
        adjusted_band = _adjusted_band(
            static_min=static_total_risk_asset_min,
            static_max=static_total_risk_asset_max,
            configured_min=config.stress_total_risk_asset_min,
            configured_max=config.stress_total_risk_asset_max,
            label="宏观压力下调后的总风险资产预算",
        )
        return MacroRiskAssetBudgetAdjustment(
            static_total_risk_asset_band=static_band,
            adjusted_total_risk_asset_band=adjusted_band,
            level="stress",
            triggered=True,
            reasons=(
                "宏观流动性达到 stress 阈值："
                + "；".join(stress_reasons)
                + "。",
            ),
        )

    elevated_reasons = _trigger_reasons(feature_set, config, level="elevated")
    if elevated_reasons:
        adjusted_band = _adjusted_band(
            static_min=static_total_risk_asset_min,
            static_max=static_total_risk_asset_max,
            configured_min=config.elevated_total_risk_asset_min,
            configured_max=config.elevated_total_risk_asset_max,
            label="宏观偏紧下调后的总风险资产预算",
        )
        return MacroRiskAssetBudgetAdjustment(
            static_total_risk_asset_band=static_band,
            adjusted_total_risk_asset_band=adjusted_band,
            level="elevated",
            triggered=True,
            reasons=(
                "宏观流动性达到 elevated 阈值："
                + "；".join(elevated_reasons)
                + "。",
            ),
        )

    return MacroRiskAssetBudgetAdjustment(
        static_total_risk_asset_band=static_band,
        adjusted_total_risk_asset_band=PositionBand(
            static_total_risk_asset_min,
            static_total_risk_asset_max,
            "总风险资产预算",
        ),
        level="normal",
        triggered=False,
        reasons=("宏观预算信号未触发下调，沿用静态总风险资产预算。",),
    )


def _trigger_reasons(
    feature_set: MarketFeatureSet,
    config: MacroRiskAssetBudgetConfig,
    *,
    level: str,
) -> list[str]:
    if level == "stress":
        thresholds = (
            (config.vix_subject, config.vix_current_feature, config.stress_vix_current),
            (
                config.vix_subject,
                config.vix_percentile_feature,
                config.stress_vix_percentile,
            ),
            (config.rate_subject, config.rate_change_feature, config.stress_rate_change_20d),
            (
                config.dollar_subject,
                config.dollar_return_feature,
                config.stress_dollar_return_20d,
            ),
        )
    elif level == "elevated":
        thresholds = (
            (config.vix_subject, config.vix_current_feature, config.elevated_vix_current),
            (
                config.vix_subject,
                config.vix_percentile_feature,
                config.elevated_vix_percentile,
            ),
            (
                config.rate_subject,
                config.rate_change_feature,
                config.elevated_rate_change_20d,
            ),
            (
                config.dollar_subject,
                config.dollar_return_feature,
                config.elevated_dollar_return_20d,
            ),
        )
    else:
        raise ValueError(f"unknown macro budget level: {level}")

    reasons: list[str] = []
    for subject, feature, threshold in thresholds:
        value = _feature_value(feature_set, subject, feature)
        if value is not None and value >= threshold:
            reasons.append(
                f"{subject} {feature}={value:.4f} >= {threshold:.4f}"
            )
    return reasons


def _adjusted_band(
    *,
    static_min: float,
    static_max: float,
    configured_min: float,
    configured_max: float,
    label: str,
) -> PositionBand:
    adjusted_max = min(static_max, configured_max)
    adjusted_min = min(static_min, configured_min, adjusted_max)
    return PositionBand(adjusted_min, adjusted_max, label)


def _feature_value(
    feature_set: MarketFeatureSet,
    subject: str,
    feature: str,
) -> float | None:
    matches = [
        row.value
        for row in feature_set.rows
        if row.subject == subject and row.feature == feature
    ]
    if not matches:
        return None
    return matches[-1]
