from __future__ import annotations

import json
from datetime import date

import pandas as pd

from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_b3 import (
    build_b3_relative_tilt_signal,
    build_b3_target_path,
    load_b3_policies,
)
from ai_trading_system.etf_portfolio.weight_research_b4 import (
    build_b4_interaction_target_path,
)
from ai_trading_system.etf_portfolio.weight_research_execution import (
    simulate_target_path_execution,
)


def test_b4_target_path_combines_b2_exposure_with_b3_relative_mix() -> None:
    config = load_etf_config_bundle()
    prices, b2_target_path, b3_target_path = _component_target_paths()

    b4_target_path = build_b4_interaction_target_path(
        b2_target_path,
        b3_target_path,
        config=config,
    )

    assert not b4_target_path.empty
    first = b4_target_path.iloc[0]
    b2_weights = json.loads(str(first["b2_target_weights_json"]))
    b3_weights = json.loads(str(first["b3_target_weights_json"]))
    b4_weights = json.loads(str(first["target_weights_json"]))
    assert abs((1.0 - b4_weights["CASH"]) - (1.0 - b2_weights["CASH"])) < 1e-9
    assert abs(b4_weights["SMH"] / b4_weights["QQQ"] - b3_weights["SMH"] / b3_weights["QQQ"]) < 1e-9
    assert abs(sum(b4_weights.values()) - 1.0) < 1e-9
    assert prices is not None


def test_b4_target_path_can_run_naive_execution() -> None:
    config = load_etf_config_bundle()
    prices, b2_target_path, b3_target_path = _component_target_paths()
    b4_target_path = build_b4_interaction_target_path(
        b2_target_path,
        b3_target_path,
        config=config,
    )

    daily = simulate_target_path_execution(
        prices=prices,
        config=config,
        target_path=b4_target_path,
        mode="naive",
    )

    assert not daily.empty
    assert daily["official_target_weights"].eq(False).all()
    assert daily["production_effect"].eq("none").all()


def _component_target_paths() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    config = load_etf_config_bundle()
    b2_signal_policy, b2_target_policy = load_b2_policies()
    b3_signal_policy, b3_target_policy = load_b3_policies()
    prices = _price_fixture(periods=230)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=date(2023, 1, 3),
        end=date(2023, 11, 17),
    )
    selected = features.loc[pd.to_datetime(features["date"]) >= pd.Timestamp("2023-10-02")]
    b2_signal = build_b2_risk_signal(selected, config=config, policy=b2_signal_policy)
    b3_signal = build_b3_relative_tilt_signal(selected, config=config, policy=b3_signal_policy)
    b2_target_path = build_b2_target_path(
        b2_signal,
        prices=prices,
        config=config,
        mapping_policy=b2_target_policy,
        start=date(2023, 10, 2),
        end=date(2023, 11, 17),
    )
    b3_target_path = build_b3_target_path(
        b3_signal,
        prices=prices,
        config=config,
        mapping_policy=b3_target_policy,
        signal_policy=b3_signal_policy,
        start=date(2023, 10, 2),
        end=date(2023, 11, 17),
    )
    return prices, b2_target_path, b3_target_path


def _price_fixture(*, periods: int) -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-03", periods=periods)
    rows: list[dict[str, object]] = []
    base_prices = {"SPY": 100.0, "QQQ": 100.0, "SMH": 100.0, "SOXX": 100.0}
    daily_returns = {"SPY": 0.0003, "QQQ": 0.0009, "SMH": 0.0014, "SOXX": 0.0011}
    for index, current_date in enumerate(dates):
        for symbol, start_price in base_prices.items():
            price = start_price * ((1.0 + daily_returns[symbol]) ** index)
            rows.append(
                {
                    "date": current_date.date().isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1000,
                    "source": "fixture",
                    "created_at": "2026-06-19T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)
