from __future__ import annotations

import pandas as pd

from ai_trading_system.regenerated_candidate_generator_common import (
    DRAWDOWN_SCORE_SCALE,
    RELATIVE_STRENGTH_SCORE_SCALE,
    RETURN_SCORE_SCALE,
    PriceDerivedRegeneratedCandidateGenerator,
    SignalComputation,
    clamp_score,
    confidence_for,
    history_count,
    missing_tickers,
    neutral_signal,
    risk_direction,
    rolling_drawdown,
    rolling_return,
)

RISK_APPETITE_CANDIDATE_ID = "risk_appetite"

# Pilot blend for non-promotable regenerated candidate artifacts; TRADING-2285
# must validate these weights before this can become an exposure limiter.
RISK_APPETITE_LOOKBACK_DAYS = 20
TARGET_RELATIVE_STRENGTH_WEIGHT = 0.3
QQQ_RELATIVE_STRENGTH_WEIGHT = 0.25
SMH_RELATIVE_STRENGTH_WEIGHT = 0.25
DEFENSIVE_PROXY_WEIGHT = 0.1
DRAWDOWN_STATE_WEIGHT = 0.1
MARKET_DRAWDOWN_LOOKBACK_DAYS = 20


class RiskAppetiteCandidateGenerator(PriceDerivedRegeneratedCandidateGenerator):
    candidate_id = RISK_APPETITE_CANDIDATE_ID
    generator_version = "risk_appetite_candidate_generator.v1"
    model_or_rule_version = "risk_appetite_rules.v1"
    required_inputs = (
        "QQQ_adjusted_close",
        "SPY_adjusted_close",
        "SMH_adjusted_close",
        "target_asset_adjusted_close",
        "relative_strength_features",
        "market_drawdown_state",
    )
    optional_input_tickers = ("TLT", "UUP", "GLD")
    output_signal_names = (
        "risk_appetite_score",
        "risk_on_confirmation_score",
        "risk_off_pressure_score",
        "semiconductor_risk_appetite_score",
    )
    signal_direction_mapping = {
        "risk_appetite_score_high": "risk_on",
        "risk_appetite_score_medium": "neutral",
        "risk_appetite_score_low": "risk_off",
        "risk_off_pressure_score_negative": "risk_off",
        "semiconductor_risk_appetite_score_positive": "risk_on",
    }

    def compute_signals(
        self,
        price_matrix: pd.DataFrame,
        target_asset: str,
        current_ts: pd.Timestamp,
    ) -> list[SignalComputation]:
        core_missing = missing_tickers(price_matrix, (target_asset, "QQQ", "SPY", "SMH"))
        if core_missing:
            return [
                neutral_signal(name, missing_inputs=core_missing)
                for name in self.output_signal_names
            ]
        optional_missing = missing_tickers(price_matrix, self.optional_input_tickers)

        target_return = rolling_return(
            price_matrix, target_asset, current_ts, RISK_APPETITE_LOOKBACK_DAYS
        )
        qqq_return = rolling_return(
            price_matrix, "QQQ", current_ts, RISK_APPETITE_LOOKBACK_DAYS
        )
        spy_return = rolling_return(
            price_matrix, "SPY", current_ts, RISK_APPETITE_LOOKBACK_DAYS
        )
        smh_return = rolling_return(
            price_matrix, "SMH", current_ts, RISK_APPETITE_LOOKBACK_DAYS
        )
        market_drawdown = rolling_drawdown(
            price_matrix, "SPY", current_ts, MARKET_DRAWDOWN_LOOKBACK_DAYS
        )
        if None in (target_return, qqq_return, spy_return, smh_return, market_drawdown):
            missing_features = ("minimum_20_trading_day_history",)
            return [
                neutral_signal(name, missing_inputs=missing_features)
                for name in self.output_signal_names
            ]

        target_relative = clamp_score(
            (float(target_return) - float(spy_return)) / RELATIVE_STRENGTH_SCORE_SCALE
        )
        qqq_relative = clamp_score(
            (float(qqq_return) - float(spy_return)) / RELATIVE_STRENGTH_SCORE_SCALE
        )
        semiconductor_relative = clamp_score(
            (float(smh_return) - float(spy_return)) / RELATIVE_STRENGTH_SCORE_SCALE
        )
        tlt_return = rolling_return(
            price_matrix, "TLT", current_ts, RISK_APPETITE_LOOKBACK_DAYS
        )
        defensive_pressure = (
            clamp_score(-float(tlt_return) / RETURN_SCORE_SCALE)
            if tlt_return is not None
            else 0.0
        )
        drawdown_pressure = clamp_score(float(market_drawdown) / DRAWDOWN_SCORE_SCALE)
        risk_appetite = clamp_score(
            (TARGET_RELATIVE_STRENGTH_WEIGHT * target_relative)
            + (QQQ_RELATIVE_STRENGTH_WEIGHT * qqq_relative)
            + (SMH_RELATIVE_STRENGTH_WEIGHT * semiconductor_relative)
            + (DEFENSIVE_PROXY_WEIGHT * defensive_pressure)
            + (DRAWDOWN_STATE_WEIGHT * drawdown_pressure)
        )
        risk_on_confirmation = clamp_score(max(0.0, risk_appetite))
        risk_off_pressure = clamp_score(min(0.0, risk_appetite))
        confidence = confidence_for(
            history_count=history_count(price_matrix, current_ts),
            missing_inputs=optional_missing,
        )
        proxy_limitations = (
            "risk_appetite is a confirm/exposure-limiter input, not a standalone rebalance trigger",
        )
        return [
            SignalComputation(
                signal_name="risk_appetite_score",
                signal_value=risk_appetite,
                signal_direction=risk_direction(risk_appetite),
                signal_confidence=confidence,
                source_state=risk_direction(risk_appetite),
                missing_inputs=optional_missing,
                proxy_input_used=bool(optional_missing),
                proxy_input_reason="optional defensive ETF proxies unavailable"
                if optional_missing
                else "",
                proxy_limitations=proxy_limitations,
            ),
            SignalComputation(
                signal_name="risk_on_confirmation_score",
                signal_value=risk_on_confirmation,
                signal_direction=risk_direction(risk_on_confirmation),
                signal_confidence=confidence,
                source_state=risk_direction(risk_on_confirmation),
                missing_inputs=optional_missing,
                proxy_input_used=bool(optional_missing),
                proxy_input_reason="optional defensive ETF proxies unavailable"
                if optional_missing
                else "",
                proxy_limitations=proxy_limitations,
            ),
            SignalComputation(
                signal_name="risk_off_pressure_score",
                signal_value=risk_off_pressure,
                signal_direction=risk_direction(risk_off_pressure),
                signal_confidence=confidence,
                source_state=risk_direction(risk_off_pressure),
                missing_inputs=optional_missing,
                proxy_input_used=bool(optional_missing),
                proxy_input_reason="optional defensive ETF proxies unavailable"
                if optional_missing
                else "",
                proxy_limitations=proxy_limitations,
            ),
            SignalComputation(
                signal_name="semiconductor_risk_appetite_score",
                signal_value=semiconductor_relative,
                signal_direction=risk_direction(semiconductor_relative),
                signal_confidence=confidence,
                source_state=risk_direction(semiconductor_relative),
                missing_inputs=optional_missing,
                proxy_input_used=bool(optional_missing),
                proxy_input_reason="optional defensive ETF proxies unavailable"
                if optional_missing
                else "",
                proxy_limitations=proxy_limitations,
            ),
        ]
