from __future__ import annotations

import pandas as pd

from ai_trading_system.regenerated_candidate_generator_common import (
    DRAWDOWN_SCORE_SCALE,
    VIX_STRESS_LEVEL,
    VIX_STRESS_SCALE,
    VOLATILITY_EXPANSION_SCALE,
    VOLATILITY_SCORE_SCALE,
    PriceDerivedRegeneratedCandidateGenerator,
    SignalComputation,
    clamp_score,
    confidence_for,
    history_count,
    missing_tickers,
    neutral_signal,
    price_at,
    realized_volatility,
    risk_direction,
    rolling_drawdown,
    volatility_direction,
)

VOLATILITY_REGIME_CANDIDATE_ID = "volatility_regime"

# Pilot blend for non-promotable regenerated candidate artifacts; TRADING-2285
# must validate these weights before this can become a risk cap or veto input.
VOLATILITY_LOOKBACK_DAYS = 20
SHORT_VOLATILITY_LOOKBACK_DAYS = 5
VOLATILITY_LEVEL_REFERENCE = 0.25
VOLATILITY_REGIME_LEVEL_WEIGHT = 0.35
VOLATILITY_REGIME_EXPANSION_WEIGHT = 0.35
VOLATILITY_REGIME_DRAWDOWN_WEIGHT = 0.2
VOLATILITY_REGIME_VIX_WEIGHT = 0.1


class VolatilityRegimeCandidateGenerator(PriceDerivedRegeneratedCandidateGenerator):
    candidate_id = VOLATILITY_REGIME_CANDIDATE_ID
    generator_version = "volatility_regime_candidate_generator.v1"
    model_or_rule_version = "volatility_regime_rules.v1"
    pit_policy = "pit_approximation"
    required_inputs = (
        "target_asset_realized_volatility",
        "SPY_realized_volatility",
        "SMH_realized_volatility",
        "rolling_drawdown",
        "rolling_range_proxy",
    )
    optional_input_tickers = ("^VIX",)
    output_signal_names = (
        "volatility_regime_score",
        "volatility_expansion_score",
        "stress_regime_score",
        "volatility_compression_score",
    )
    signal_direction_mapping = {
        "volatility_regime_score_positive": "volatility_compression",
        "volatility_regime_score_negative": "volatility_expansion",
        "volatility_expansion_score_negative": "volatility_expansion",
        "stress_regime_score_negative": "risk_off",
        "volatility_compression_score_positive": "volatility_compression",
    }

    def compute_signals(
        self,
        price_matrix: pd.DataFrame,
        target_asset: str,
        current_ts: pd.Timestamp,
    ) -> list[SignalComputation]:
        core_missing = missing_tickers(price_matrix, (target_asset, "SPY", "SMH"))
        if core_missing:
            return [
                neutral_signal(name, missing_inputs=core_missing)
                for name in self.output_signal_names
            ]
        optional_missing = missing_tickers(price_matrix, self.optional_input_tickers)

        realized_vol = realized_volatility(
            price_matrix, target_asset, current_ts, VOLATILITY_LOOKBACK_DAYS
        )
        short_vol = realized_volatility(
            price_matrix, target_asset, current_ts, SHORT_VOLATILITY_LOOKBACK_DAYS
        )
        drawdown = rolling_drawdown(
            price_matrix, target_asset, current_ts, VOLATILITY_LOOKBACK_DAYS
        )
        spy_vol = realized_volatility(
            price_matrix, "SPY", current_ts, VOLATILITY_LOOKBACK_DAYS
        )
        smh_vol = realized_volatility(
            price_matrix, "SMH", current_ts, VOLATILITY_LOOKBACK_DAYS
        )
        if None in (realized_vol, short_vol, drawdown, spy_vol, smh_vol):
            missing_features = ("minimum_20_trading_day_volatility_history",)
            return [
                neutral_signal(name, missing_inputs=missing_features)
                for name in self.output_signal_names
            ]

        volatility_level_pressure = clamp_score(
            (float(realized_vol) - VOLATILITY_LEVEL_REFERENCE) / VOLATILITY_SCORE_SCALE
        )
        volatility_expansion_pressure = clamp_score(
            (float(short_vol) - float(realized_vol)) / VOLATILITY_EXPANSION_SCALE
        )
        drawdown_pressure = clamp_score(-float(drawdown) / DRAWDOWN_SCORE_SCALE)
        vix_level = price_at(price_matrix, "^VIX", current_ts)
        vix_available = vix_level is not None
        vix_pressure = (
            clamp_score((float(vix_level) - VIX_STRESS_LEVEL) / VIX_STRESS_SCALE)
            if vix_available
            else 0.0
        )
        stress_pressure = clamp_score(
            (VOLATILITY_REGIME_LEVEL_WEIGHT * volatility_level_pressure)
            + (VOLATILITY_REGIME_EXPANSION_WEIGHT * volatility_expansion_pressure)
            + (VOLATILITY_REGIME_DRAWDOWN_WEIGHT * drawdown_pressure)
            + (VOLATILITY_REGIME_VIX_WEIGHT * vix_pressure)
        )
        compression_pressure = clamp_score(
            (float(realized_vol) - float(short_vol)) / VOLATILITY_EXPANSION_SCALE
        )
        regime_score = clamp_score(compression_pressure - max(0.0, stress_pressure))
        expansion_score = clamp_score(-max(0.0, volatility_expansion_pressure))
        stress_score = clamp_score(-max(0.0, stress_pressure))
        compression_score = clamp_score(max(0.0, compression_pressure))
        confidence = confidence_for(
            history_count=history_count(price_matrix, current_ts),
            missing_inputs=optional_missing,
        )
        proxy_input_used = not vix_available
        proxy_reason = "VIX unavailable; using realized volatility only" if proxy_input_used else ""
        proxy_limitations = (
            "volatility_regime is a risk-control input, not a direct return prediction signal",
        )
        proxy_mode = (
            "realized_volatility_with_vix_overlay"
            if vix_available
            else "realized_volatility_only"
        )
        return [
            SignalComputation(
                signal_name="volatility_regime_score",
                signal_value=regime_score,
                signal_direction=volatility_direction(regime_score),
                signal_confidence=confidence,
                source_state=volatility_direction(regime_score),
                missing_inputs=optional_missing,
                proxy_input_used=proxy_input_used,
                proxy_input_reason=proxy_reason,
                proxy_limitations=proxy_limitations,
                vix_available=vix_available,
                volatility_proxy_mode=proxy_mode,
            ),
            SignalComputation(
                signal_name="volatility_expansion_score",
                signal_value=expansion_score,
                signal_direction=volatility_direction(expansion_score),
                signal_confidence=confidence,
                source_state=volatility_direction(expansion_score),
                missing_inputs=optional_missing,
                proxy_input_used=proxy_input_used,
                proxy_input_reason=proxy_reason,
                proxy_limitations=proxy_limitations,
                vix_available=vix_available,
                volatility_proxy_mode=proxy_mode,
            ),
            SignalComputation(
                signal_name="stress_regime_score",
                signal_value=stress_score,
                signal_direction=risk_direction(stress_score),
                signal_confidence=confidence,
                source_state=risk_direction(stress_score),
                missing_inputs=optional_missing,
                proxy_input_used=proxy_input_used,
                proxy_input_reason=proxy_reason,
                proxy_limitations=proxy_limitations,
                vix_available=vix_available,
                volatility_proxy_mode=proxy_mode,
            ),
            SignalComputation(
                signal_name="volatility_compression_score",
                signal_value=compression_score,
                signal_direction=volatility_direction(compression_score),
                signal_confidence=confidence,
                source_state=volatility_direction(compression_score),
                missing_inputs=optional_missing,
                proxy_input_used=proxy_input_used,
                proxy_input_reason=proxy_reason,
                proxy_limitations=proxy_limitations,
                vix_available=vix_available,
                volatility_proxy_mode=proxy_mode,
            ),
        ]
