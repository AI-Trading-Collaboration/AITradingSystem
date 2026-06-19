from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.weight_research_b2 import (
    build_b2_risk_signal,
    build_b2_target_path,
    load_b2_policies,
)
from ai_trading_system.etf_portfolio.weight_research_execution import (
    simulate_target_path_execution,
)


def test_b2_risk_signal_outputs_signal_only_fields() -> None:
    config = load_etf_config_bundle()
    risk_policy, _ = load_b2_policies()
    features = _feature_fixture()
    selected = features.loc[pd.to_datetime(features["date"]) >= pd.Timestamp("2023-10-02")]

    signal = build_b2_risk_signal(selected, config=config, policy=risk_policy)

    assert not signal.empty
    assert {"risk_score", "risk_state", "risk_confidence", "risk_coverage"} <= set(
        signal.columns
    )
    assert "target_weight" not in signal.columns
    assert signal["official_target_weights"].eq(False).all()
    assert signal["production_effect"].eq("none").all()


def test_b2_target_mapping_changes_total_exposure_not_relative_selection() -> None:
    config = load_etf_config_bundle()
    risk_policy, target_policy = load_b2_policies()
    prices = _price_fixture(periods=230)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=date(2023, 1, 3),
        end=date(2023, 11, 17),
    )
    selected = features.loc[pd.to_datetime(features["date"]) >= pd.Timestamp("2023-10-02")]
    signal = build_b2_risk_signal(selected, config=config, policy=risk_policy)

    target_path = build_b2_target_path(
        signal,
        prices=prices,
        config=config,
        mapping_policy=target_policy,
        start=date(2023, 10, 2),
        end=date(2023, 11, 17),
    )

    assert not target_path.empty
    weights = target_path["target_weights_json"].map(lambda value: __import__("json").loads(value))
    for row in weights:
        assert row["SPY"] / row["QQQ"] == 0.30 / 0.40
        assert row["SMH"] / row["QQQ"] == 0.15 / 0.40
        assert abs(sum(row.values()) - 1.0) < 1e-9


def test_b2_target_path_can_run_naive_execution() -> None:
    config = load_etf_config_bundle()
    risk_policy, target_policy = load_b2_policies()
    prices = _price_fixture(periods=230)
    features = build_feature_store(
        prices,
        assets=config.assets,
        strategy=config.strategy,
        start=date(2023, 1, 3),
        end=date(2023, 11, 17),
    )
    selected = features.loc[pd.to_datetime(features["date"]) >= pd.Timestamp("2023-10-02")]
    signal = build_b2_risk_signal(selected, config=config, policy=risk_policy)
    target_path = build_b2_target_path(
        signal,
        prices=prices,
        config=config,
        mapping_policy=target_policy,
        start=date(2023, 10, 2),
        end=date(2023, 11, 17),
    )

    daily = simulate_target_path_execution(
        prices=prices,
        config=config,
        target_path=target_path,
        mode="naive",
    )

    assert not daily.empty
    assert daily["official_target_weights"].eq(False).all()
    assert daily["production_effect"].eq("none").all()


def _feature_fixture() -> pd.DataFrame:
    config = load_etf_config_bundle()
    return build_feature_store(
        _price_fixture(periods=230),
        assets=config.assets,
        strategy=config.strategy,
        start=date(2023, 1, 3),
        end=date(2023, 11, 17),
    )


def _price_fixture(*, periods: int) -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-03", periods=periods)
    rows: list[dict[str, object]] = []
    base_prices = {"SPY": 100.0, "QQQ": 100.0, "SMH": 100.0, "SOXX": 100.0}
    daily_returns = {"SPY": 0.0005, "QQQ": 0.0008, "SMH": 0.001, "SOXX": 0.0009}
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
