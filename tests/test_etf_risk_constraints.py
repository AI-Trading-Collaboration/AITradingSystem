from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pandas as pd
import pytest

from ai_trading_system.etf_portfolio.allocation import allocate_portfolio, weights_from_records
from ai_trading_system.etf_portfolio.models import ETFQualityReport, load_etf_config_bundle
from ai_trading_system.etf_portfolio.signals import risk_score_for_row


def test_etf_allocation_records_structured_constraint_diagnostics() -> None:
    config = load_etf_config_bundle()
    config.assets.assets["QQQ"].max_weight = 0.35

    allocation = allocate_portfolio(
        _signals(QQQ=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-On",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
    )
    weights = weights_from_records(allocation)
    diagnostics = _diagnostics(allocation)
    serialized = json.loads(str(allocation[0].to_record()["constraint_diagnostics"]))

    assert weights["QQQ"] <= 0.35 + 1e-8
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "asset_weight_cap" in {item["constraint_id"] for item in diagnostics}
    assert {
        "constraint_id",
        "asset_or_sleeve",
        "before_weight",
        "after_weight",
        "reason",
        "severity",
    }.issubset(serialized[0])
    assert serialized[0]["severity"] == "info"


def test_etf_semiconductor_sleeve_cap_limits_smh_and_soxx() -> None:
    config = load_etf_config_bundle()
    config.assets.assets["SOXX"].default_weight = 0.20
    config.assets.assets["SOXX"].max_weight = 0.30
    config.risk.regime_constraints["Risk-On"].semiconductor_cap = 0.25

    allocation = allocate_portfolio(
        _signals(SMH=90.0, SOXX=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-On",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
    )
    weights = weights_from_records(allocation)
    diagnostics = _diagnostics(allocation)

    assert weights["SMH"] + weights["SOXX"] <= 0.25 + 1e-8
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "semiconductor_sleeve_cap" in {item["constraint_id"] for item in diagnostics}


def test_etf_risk_off_caps_equity_and_preserves_cash_floor() -> None:
    config = load_etf_config_bundle()

    allocation = allocate_portfolio(
        _signals(SPY=90.0, QQQ=90.0, SMH=90.0, SOXX=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-Off",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
    )
    weights = weights_from_records(allocation)
    diagnostics = _diagnostics(allocation)
    diagnostic_ids = {item["constraint_id"] for item in diagnostics}

    assert sum(weights[symbol] for symbol in weights if symbol != "CASH") <= 0.40 + 1e-8
    assert weights["CASH"] >= 0.60 - 1e-8
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "regime_equity_cap" in diagnostic_ids


def test_etf_cash_min_constraint_emits_diagnostic_when_it_is_binding() -> None:
    config = load_etf_config_bundle()
    config.risk.regime_constraints["Risk-Off"].equity_cap = 1.0

    allocation = allocate_portfolio(
        _signals(SPY=90.0, QQQ=90.0, SMH=90.0, SOXX=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-Off",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
    )
    weights = weights_from_records(allocation)
    diagnostic_ids = {item["constraint_id"] for item in _diagnostics(allocation)}

    assert weights["CASH"] >= 0.60 - 1e-8
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "cash_min_weight" in diagnostic_ids


def test_etf_max_rebalance_trade_weight_caps_single_asset_changes() -> None:
    config = load_etf_config_bundle()
    config.risk.portfolio_constraints.max_daily_turnover = 1.0
    previous_weights = {
        "SPY": 0.10,
        "QQQ": 0.10,
        "SMH": 0.00,
        "SOXX": 0.00,
        "CASH": 0.80,
    }

    allocation = allocate_portfolio(
        _signals(SPY=90.0, QQQ=90.0, SMH=90.0, SOXX=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-On",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
        previous_weights=previous_weights,
    )
    weights = weights_from_records(allocation)
    diagnostics = _diagnostics(allocation)

    for symbol, target in weights.items():
        if symbol == "CASH":
            continue
        assert abs(target - previous_weights[symbol]) <= 0.15 + 1e-8
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "max_rebalance_trade_weight" in {item["constraint_id"] for item in diagnostics}


def test_etf_max_daily_turnover_scales_rebalance_to_configured_limit() -> None:
    config = load_etf_config_bundle()
    config.risk.portfolio_constraints.max_rebalance_trade_weight = 1.0
    previous_weights = {
        "SPY": 0.10,
        "QQQ": 0.10,
        "SMH": 0.00,
        "SOXX": 0.00,
        "CASH": 0.80,
    }

    allocation = allocate_portfolio(
        _signals(SPY=90.0, QQQ=90.0, SMH=90.0, SOXX=90.0),
        assets=config.assets,
        strategy=config.strategy,
        risk=config.risk,
        regime="Risk-On",
        run_date=date(2026, 1, 2),
        config_hash=config.config_hash,
        data_quality_report=_quality_report(config),
        previous_weights=previous_weights,
    )
    weights = weights_from_records(allocation)
    diagnostics = _diagnostics(allocation)

    assert _turnover(weights, previous_weights) <= 0.30 + 1e-6
    assert sum(weights.values()) == pytest.approx(1.0)
    assert "max_daily_turnover" in {item["constraint_id"] for item in diagnostics}


def test_etf_signal_risk_score_applies_volatility_and_drawdown_penalties() -> None:
    config = load_etf_config_bundle()
    mapping = config.strategy.score_mapping

    stable_score, stable_reasons = risk_score_for_row(
        pd.Series(
            {
                "realized_vol_20d": mapping.vol_low,
                "drawdown_63d": mapping.drawdown_low,
                "above_ma_200": True,
            }
        ),
        config.strategy,
    )
    stressed_score, stressed_reasons = risk_score_for_row(
        pd.Series(
            {
                "realized_vol_20d": mapping.vol_high,
                "drawdown_63d": mapping.drawdown_high,
                "above_ma_200": False,
            }
        ),
        config.strategy,
    )

    assert stable_score == 100.0
    assert stressed_score == 100.0 - (
        mapping.vol_max_penalty
        + mapping.drawdown_max_penalty
        + mapping.below_ma_200_penalty
    )
    assert "VOLATILITY_STABLE" in stable_reasons
    assert "DRAWDOWN_CONTAINED" in stable_reasons
    assert "VOLATILITY_ELEVATED" in stressed_reasons
    assert "DRAWDOWN_EXTENDED" in stressed_reasons
    assert "PRICE_BELOW_200D_MA_RISK_PENALTY" in stressed_reasons


def _signals(**scores: float) -> pd.DataFrame:
    rows = [
        {"symbol": symbol, "composite_score": scores.get(symbol, 50.0)}
        for symbol in ("SPY", "QQQ", "SMH", "SOXX")
    ]
    return pd.DataFrame(rows)


def _diagnostics(records) -> tuple[dict[str, object], ...]:
    return records[0].constraint_diagnostics


def _turnover(
    target_weights: dict[str, float],
    previous_weights: dict[str, float],
) -> float:
    symbols = set(target_weights) | set(previous_weights)
    return sum(
        abs(target_weights.get(symbol, 0.0) - previous_weights.get(symbol, 0.0))
        for symbol in symbols
    )


def _quality_report(config) -> ETFQualityReport:
    return ETFQualityReport(
        checked_at=datetime.now(UTC),
        as_of=date(2026, 1, 2),
        status="PASS",
        row_count=1000,
        symbols=config.assets.symbols,
        min_date=date(2022, 12, 1),
        max_date=date(2026, 1, 2),
        checksum="risk-constraints-fixture",
    )
