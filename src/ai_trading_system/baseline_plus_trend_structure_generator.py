from __future__ import annotations

import pandas as pd

from ai_trading_system.regenerated_candidate_generator_common import (
    DRAWDOWN_SCORE_SCALE,
    MOVING_AVERAGE_SCORE_SCALE,
    RELATIVE_STRENGTH_SCORE_SCALE,
    RETURN_SCORE_SCALE,
    PriceDerivedRegeneratedCandidateGenerator,
    SignalComputation,
    clamp_score,
    confidence_for,
    history_count,
    missing_tickers,
    moving_average_gap,
    neutral_signal,
    risk_direction,
    rolling_drawdown,
    rolling_return,
    trend_direction,
)

BASELINE_PLUS_TREND_STRUCTURE_CANDIDATE_ID = "baseline_plus_trend_structure"

# Pilot blend for non-promotable regenerated candidate artifacts; TRADING-2285
# must validate these weights before any candidate can influence investment action.
TREND_LOOKBACK_DAYS = 20
TREND_RETURN_WEIGHT = 0.45
TREND_MOVING_AVERAGE_WEIGHT = 0.35
TREND_DRAWDOWN_WEIGHT = 0.2


class BaselinePlusTrendStructureGenerator(PriceDerivedRegeneratedCandidateGenerator):
    candidate_id = BASELINE_PLUS_TREND_STRUCTURE_CANDIDATE_ID
    generator_version = "baseline_plus_trend_structure_generator.v1"
    model_or_rule_version = "baseline_plus_trend_structure_rules.v1"
    required_inputs = (
        "target_asset_adjusted_close",
        "SPY_adjusted_close",
        "QQQ_adjusted_close",
        "SMH_adjusted_close",
        "moving_average_features",
        "rolling_return_features",
        "drawdown_features",
        "relative_strength_features",
    )
    output_signal_names = (
        "trend_structure_score",
        "trend_confirmation_score",
        "trend_weakening_score",
        "relative_strength_score",
    )
    signal_direction_mapping = {
        "trend_structure_score_positive": "trend_confirming",
        "trend_structure_score_near_zero": "neutral",
        "trend_structure_score_negative": "trend_weakening",
        "relative_strength_score_positive": "risk_on",
        "relative_strength_score_negative": "risk_off",
    }

    def compute_signals(
        self,
        price_matrix: pd.DataFrame,
        target_asset: str,
        current_ts: pd.Timestamp,
    ) -> list[SignalComputation]:
        missing = missing_tickers(price_matrix, (target_asset, "SPY"))
        if missing:
            return [
                neutral_signal(name, missing_inputs=missing)
                for name in self.output_signal_names
            ]

        current_history = history_count(price_matrix, current_ts)
        target_return = rolling_return(
            price_matrix, target_asset, current_ts, TREND_LOOKBACK_DAYS
        )
        spy_return = rolling_return(price_matrix, "SPY", current_ts, TREND_LOOKBACK_DAYS)
        ma_gap = moving_average_gap(
            price_matrix, target_asset, current_ts, TREND_LOOKBACK_DAYS
        )
        drawdown = rolling_drawdown(
            price_matrix, target_asset, current_ts, TREND_LOOKBACK_DAYS
        )
        if None in (target_return, spy_return, ma_gap, drawdown):
            missing_features = ("minimum_20_trading_day_history",)
            return [
                neutral_signal(name, missing_inputs=missing_features)
                for name in self.output_signal_names
            ]

        return_score = clamp_score(float(target_return) / RETURN_SCORE_SCALE)
        ma_score = clamp_score(float(ma_gap) / MOVING_AVERAGE_SCORE_SCALE)
        drawdown_score = clamp_score(float(drawdown) / DRAWDOWN_SCORE_SCALE)
        relative_strength = clamp_score(
            (float(target_return) - float(spy_return)) / RELATIVE_STRENGTH_SCORE_SCALE
        )
        trend_structure = clamp_score(
            (TREND_RETURN_WEIGHT * return_score)
            + (TREND_MOVING_AVERAGE_WEIGHT * ma_score)
            + (TREND_DRAWDOWN_WEIGHT * drawdown_score)
        )
        confirmation = clamp_score(max(0.0, trend_structure))
        weakening = clamp_score(min(0.0, trend_structure))
        confidence = confidence_for(history_count=current_history)
        return [
            SignalComputation(
                signal_name="trend_structure_score",
                signal_value=trend_structure,
                signal_direction=trend_direction(trend_structure),
                signal_confidence=confidence,
                source_state=trend_direction(trend_structure),
            ),
            SignalComputation(
                signal_name="trend_confirmation_score",
                signal_value=confirmation,
                signal_direction=trend_direction(confirmation),
                signal_confidence=confidence,
                source_state=trend_direction(confirmation),
            ),
            SignalComputation(
                signal_name="trend_weakening_score",
                signal_value=weakening,
                signal_direction=trend_direction(weakening),
                signal_confidence=confidence,
                source_state=trend_direction(weakening),
            ),
            SignalComputation(
                signal_name="relative_strength_score",
                signal_value=relative_strength,
                signal_direction=risk_direction(relative_strength),
                signal_confidence=confidence,
                source_state=risk_direction(relative_strength),
            ),
        ]
