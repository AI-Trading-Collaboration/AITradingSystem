from __future__ import annotations

import json
from copy import deepcopy
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio.ai_confirmation import (
    AIConfirmationUniverseConfig,
    ai_confirmation_breadth_records_to_frame,
    all_enabled_tickers,
    build_ai_confirmation_breadth_features,
    build_mega_cap_ai_confirmation_score,
    enabled_symbols_for_group,
    load_ai_confirmation_policy_config,
    load_ai_confirmation_universe_config,
    score_band,
    validate_ai_confirmation_data_availability,
)
from ai_trading_system.etf_portfolio.data import standardize_price_frame
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle


def test_ai_confirmation_universe_config_loads() -> None:
    config = load_ai_confirmation_universe_config()

    assert config.policy_metadata.version == "ai_confirmation_universe_v0_1"
    assert "mega_cap_ai" in config.ai_confirmation_universe
    assert "semiconductor_hardware" in config.ai_confirmation_universe
    assert config.safety.observe_only is True
    assert config.safety.candidate_only is True
    assert config.safety.production_effect == "none"
    assert config.safety.broker_action == "none"
    assert config.safety.manual_review_required is True
    assert config.config_hash


def test_ai_confirmation_symbols_have_required_metadata() -> None:
    config = load_ai_confirmation_universe_config()

    for group in config.ai_confirmation_universe.values():
        assert group.description
        assert group.default_weighting_method in {"equal_weight", "weight_cap"}
        assert group.benchmark in config.allowed_benchmarks
        assert group.required_data_level in {"strict", "warning", "optional"}
        for symbol in group.symbols:
            assert symbol.ticker == symbol.ticker.upper()
            assert symbol.name
            assert symbol.group == group.group_id
            assert symbol.role
            assert 0.0 <= symbol.weight_cap <= 1.0
            assert symbol.benchmark in config.allowed_benchmarks
            assert not (symbol.optional and symbol.data_required)


def test_ai_confirmation_duplicate_ticker_handling_is_deterministic() -> None:
    raw = _raw_config()
    duplicate = deepcopy(raw["ai_confirmation_universe"]["mega_cap_ai"]["symbols"][0])
    duplicate["role"] = "duplicate_optional_role"
    duplicate["data_required"] = False
    duplicate["optional"] = True
    raw["ai_confirmation_universe"]["mega_cap_ai"]["symbols"].append(duplicate)
    config = AIConfirmationUniverseConfig.model_validate(raw)

    first = enabled_symbols_for_group(config, "mega_cap_ai")
    second = enabled_symbols_for_group(config, "mega_cap_ai")

    assert [symbol.ticker for symbol in first] == [symbol.ticker for symbol in second]
    assert [symbol.ticker for symbol in first].count("NVDA") == 1
    assert next(symbol for symbol in first if symbol.ticker == "NVDA").data_required is True


def test_ai_confirmation_disabled_symbols_are_excluded_from_default_calculations() -> None:
    raw = _raw_config()
    raw["ai_confirmation_universe"]["mega_cap_ai"]["symbols"][0]["enabled"] = False
    config = AIConfirmationUniverseConfig.model_validate(raw)

    tickers = [symbol.ticker for symbol in enabled_symbols_for_group(config, "mega_cap_ai")]

    assert "NVDA" not in tickers


def test_ai_confirmation_unknown_group_fails() -> None:
    config = load_ai_confirmation_universe_config()

    with pytest.raises(KeyError, match="unknown AI confirmation group"):
        enabled_symbols_for_group(config, "missing_group")


def test_ai_confirmation_invalid_benchmark_reference_fails() -> None:
    raw = _raw_config()
    raw["ai_confirmation_universe"]["mega_cap_ai"]["symbols"][0]["benchmark"] = "BAD"

    with pytest.raises(ValueError, match="invalid benchmark"):
        AIConfirmationUniverseConfig.model_validate(raw)


def test_ai_confirmation_optional_symbols_can_be_missing_without_failing() -> None:
    config = load_ai_confirmation_universe_config()
    available = {
        "SPY",
        "QQQ",
        "SMH",
        "SOXX",
        "NVDA",
        "AVGO",
        "AMD",
        "TSM",
        "AMAT",
        "LRCX",
        "MU",
        "MRVL",
        "QCOM",
        "INTC",
        "MSFT",
        "GOOGL",
        "AMZN",
        "META",
        "AAPL",
    }

    report = validate_ai_confirmation_data_availability(config, available)

    assert report["status"] == "PASS_WITH_WARNINGS"
    assert any("missing_optional" in warning for warning in report["warnings"])
    assert not report["errors"]


def test_ai_confirmation_required_symbols_missing_fail_or_warn_by_group_policy() -> None:
    config = load_ai_confirmation_universe_config()
    available = set(all_enabled_tickers(config)) - {"NVDA", "AMAT"}

    report = validate_ai_confirmation_data_availability(config, available)

    assert report["status"] == "FAIL"
    assert "mega_cap_ai:missing_required:NVDA" in report["errors"]
    assert "semiconductor_hardware:missing_required:AMAT" in report["warnings"]


def test_ai_confirmation_breadth_features_from_toy_data() -> None:
    config = _single_group_config("mega_cap_ai", ["NVDA", "MSFT", "AMD"])
    prices = _make_ai_price_frame(
        {
            "NVDA": 1.0,
            "MSFT": 0.5,
            "AMD": -0.2,
        }
    )
    run_date = date.fromisoformat(str(prices["date"].max()))

    records = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )

    assert len(records) == 1
    record = records[0]
    values = record["feature_values"]
    assert record["observe_only"] is True
    assert record["candidate_only"] is True
    assert record["production_effect"] == "none"
    assert record["broker_action"] == "none"
    assert record["manual_review_required"] is True
    assert record["symbol_count"] == 3
    assert record["valid_symbol_count"] == 3
    assert record["data_coverage_ratio"] == 1.0
    assert values["percent_above_50d_ma"] == pytest.approx(2 / 3)
    assert values["percent_positive_20d_return"] == pytest.approx(2 / 3)
    assert values["median_20d_return"] > 0
    assert values["equal_weight_group_return_60d"] > 0
    assert values["group_drawdown_from_60d_high"] <= 0
    assert values["group_realized_vol_20d"] is not None
    assert values["advancing_declining_ratio"] == pytest.approx(2.0)


def test_ai_confirmation_breadth_disabled_symbols_are_excluded() -> None:
    raw = _raw_config()
    raw["ai_confirmation_universe"]["mega_cap_ai"]["symbols"][0]["enabled"] = False
    for group_id in raw["ai_confirmation_universe"]:
        if group_id != "mega_cap_ai":
            raw["ai_confirmation_universe"][group_id]["enabled"] = False
    config = AIConfirmationUniverseConfig.model_validate(raw)
    prices = _make_ai_price_frame({"MSFT": 0.5, "AMD": 0.4})
    run_date = date.fromisoformat(str(prices["date"].max()))

    records = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )

    assert records[0]["symbol_count"] == 8
    assert "mega_cap_ai:missing_required:NVDA" not in records[0]["warnings"]


def test_ai_confirmation_breadth_optional_missing_lowers_coverage_without_fail() -> None:
    config = _single_group_config(
        "semiconductor_hardware",
        ["NVDA", "AVGO", "AMD", "TSM", "AMAT", "LRCX", "MU", "MRVL", "QCOM", "INTC", "ASML"],
    )
    prices = _make_ai_price_frame(
        {
            "NVDA": 0.7,
            "AVGO": 0.5,
            "AMD": 0.4,
            "TSM": 0.2,
            "AMAT": 0.2,
            "LRCX": 0.2,
            "MU": 0.2,
            "MRVL": 0.2,
            "QCOM": 0.2,
            "INTC": 0.2,
        }
    )
    run_date = date.fromisoformat(str(prices["date"].max()))

    records = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["semiconductor_hardware"],
    )

    assert records[0]["valid_symbol_count"] == 10
    assert records[0]["symbol_count"] == 11
    assert records[0]["data_coverage_ratio"] == pytest.approx(10 / 11)
    assert "semiconductor_hardware:missing_optional:ASML" in records[0]["warnings"]


def test_ai_confirmation_breadth_required_missing_follows_group_policy() -> None:
    config = load_ai_confirmation_universe_config()
    available = set(all_enabled_tickers(config)) - {"NVDA", "AMAT"}

    strict_report = validate_ai_confirmation_data_availability(
        config,
        available,
        group_ids=["mega_cap_ai"],
    )
    warning_report = validate_ai_confirmation_data_availability(
        config,
        available,
        group_ids=["semiconductor_hardware"],
    )

    assert strict_report["status"] == "FAIL"
    assert "mega_cap_ai:missing_required:NVDA" in strict_report["errors"]
    assert warning_report["status"] == "PASS_WITH_WARNINGS"
    assert "semiconductor_hardware:missing_required:AMAT" in warning_report["warnings"]


def test_ai_confirmation_breadth_median_return_and_no_lookahead() -> None:
    config = _single_group_config("mega_cap_ai", ["NVDA", "MSFT"])
    prices = _make_ai_price_frame({"NVDA": 1.0, "MSFT": 0.5})
    run_date = date.fromisoformat(str(prices["date"].max()))
    future = prices.copy()
    future_rows = []
    for symbol in ["NVDA", "MSFT"]:
        future_rows.append(
            {
                "date": (run_date + timedelta(days=1)).isoformat(),
                "symbol": symbol,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 9999.0,
                "adj_close": 9999.0,
                "volume": 1_000_000,
                "source": "future_row",
                "created_at": "2026-01-01T00:00:00Z",
            }
        )
    future = pd.concat([future, pd.DataFrame(future_rows)], ignore_index=True)

    baseline = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )[0]
    with_future = build_ai_confirmation_breadth_features(
        future,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )[0]

    expected_returns = []
    for symbol in ["NVDA", "MSFT"]:
        series = prices.loc[prices["symbol"] == symbol, "adj_close"].reset_index(drop=True)
        expected_returns.append(series.iloc[-1] / series.iloc[-21] - 1.0)
    expected_median = sorted(expected_returns)[0] + (
        sorted(expected_returns)[1] - sorted(expected_returns)[0]
    ) / 2

    assert baseline["feature_values"]["median_20d_return"] == pytest.approx(expected_median)
    assert with_future["feature_values"] == baseline["feature_values"]


def test_ai_confirmation_breadth_records_flatten_for_report_output() -> None:
    config = _single_group_config("mega_cap_ai", ["NVDA", "MSFT"])
    prices = _make_ai_price_frame({"NVDA": 1.0, "MSFT": 0.5})
    run_date = date.fromisoformat(str(prices["date"].max()))

    records = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )
    frame = ai_confirmation_breadth_records_to_frame(records)

    assert "percent_above_20d_ma" in frame.columns
    assert "feature_values_json" in frame.columns
    assert json.loads(frame.iloc[0]["feature_values_json"])["percent_above_20d_ma"] == 1.0


def test_ai_confirmation_features_cli_writes_json_and_csv(tmp_path: Path) -> None:
    runner = CliRunner()
    etf_config = load_etf_config_bundle()
    symbols = [
        "SPY",
        "QQQ",
        "SMH",
        "SOXX",
        "NVDA",
        "AVGO",
        "AMD",
        "MSFT",
        "GOOGL",
        "AMZN",
        "META",
        "AAPL",
        "TSM",
    ]
    raw = _make_ai_price_frame({symbol: 0.5 for symbol in symbols}, days=230)
    prices, _ = standardize_price_frame(
        raw,
        assets=etf_config.assets,
        source_name="fixture",
        extra_symbols=set(symbols),
    )
    prices_path = tmp_path / "prices.csv"
    prices.to_csv(prices_path, index=False)
    output_dir = tmp_path / "features"
    universe_path = tmp_path / "ai_universe.yaml"
    universe_path.write_text(
        Path("config/etf_portfolio/ai_confirmation_universe.yaml").read_text(encoding="utf-8")
        .replace("required_data_level: strict", "required_data_level: warning", 1),
        encoding="utf-8",
    )

    result = runner.invoke(
        app,
        [
            "etf",
            "ai-confirmation",
            "features",
            "--prices-path",
            str(prices_path),
            "--date",
            str(prices["date"].max()),
            "--output-dir",
            str(output_dir),
            "--universe-path",
            str(universe_path),
        ],
    )

    assert result.exit_code == 0, result.output
    json_path = output_dir / f"ai_confirmation_features_{prices['date'].max()}.json"
    csv_path = output_dir / f"ai_confirmation_features_{prices['date'].max()}.csv"
    assert json_path.exists()
    assert csv_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["observe_only"] is True
    assert payload["candidate_only"] is True
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True
    assert payload["records"]


def test_ai_confirmation_policy_loads_and_score_bands_map() -> None:
    policy = load_ai_confirmation_policy_config()

    assert policy.policy_metadata.version == "ai_confirmation_policy_v0_1"
    assert policy.safety.observe_only is True
    assert policy.safety.candidate_only is True
    assert score_band(85.0, policy) == "strong_confirm"
    assert score_band(70.0, policy) == "confirm"
    assert score_band(50.0, policy) == "neutral"
    assert score_band(35.0, policy) == "weak"
    assert score_band(20.0, policy) == "negative"


def test_mega_cap_ai_score_increases_when_leaders_trend_above_ma() -> None:
    strong_score = _mega_score({"NVDA": 1.0, "MSFT": 0.8, "AMD": 0.7, "QQQ": 0.2, "SPY": 0.1})
    weak_score = _mega_score(
        {"NVDA": -0.6, "MSFT": -0.5, "AMD": -0.4, "QQQ": 0.2, "SPY": 0.1}
    )

    assert strong_score["score_value"] > weak_score["score_value"]
    assert strong_score["component_scores"]["mega_cap_trend_score"] > weak_score[
        "component_scores"
    ]["mega_cap_trend_score"]
    assert strong_score["component_scores"]["mega_cap_momentum_score"] > weak_score[
        "component_scores"
    ]["mega_cap_momentum_score"]


def test_mega_cap_ai_score_relative_strength_vs_qqq_affects_score() -> None:
    outperforming = _mega_score(
        {"NVDA": 1.0, "MSFT": 0.9, "AMD": 0.8, "QQQ": 0.1, "SPY": 0.1}
    )
    underperforming = _mega_score(
        {"NVDA": 0.1, "MSFT": 0.1, "AMD": 0.1, "QQQ": 1.0, "SPY": 0.1}
    )

    assert outperforming["component_scores"]["mega_cap_relative_strength_vs_QQQ"] > (
        underperforming["component_scores"]["mega_cap_relative_strength_vs_QQQ"]
    )
    assert outperforming["score_value"] > underperforming["score_value"]


def test_mega_cap_ai_score_drawdown_penalty_reduces_score() -> None:
    steady = _mega_score({"NVDA": 0.7, "MSFT": 0.7, "AMD": 0.7, "QQQ": 0.2, "SPY": 0.1})
    drawdown_prices = _make_drawdown_price_frame(["NVDA", "MSFT", "AMD", "QQQ", "SPY"])
    drawdown = _mega_score_from_prices(drawdown_prices)

    assert drawdown["component_scores"]["mega_cap_drawdown_penalty"] < steady[
        "component_scores"
    ]["mega_cap_drawdown_penalty"]
    assert drawdown["score_value"] < steady["score_value"]


def test_mega_cap_ai_score_data_coverage_penalty_and_safety_fields() -> None:
    score = _mega_score({"NVDA": 1.0, "MSFT": 1.0, "QQQ": 0.2, "SPY": 0.1})

    assert score["component_scores"]["data_coverage_penalty"] == pytest.approx(
        2 / 3 * 100,
        abs=0.01,
    )
    assert score["data_coverage_ratio"] == pytest.approx(2 / 3)
    assert any("low_data_coverage" in warning for warning in score["warnings"])
    assert score["observe_only"] is True
    assert score["candidate_only"] is True
    assert score["production_effect"] == "none"
    assert score["broker_action"] == "none"
    assert score["manual_review_required"] is True
    assert score["top_positive_drivers"]
    assert isinstance(score["top_negative_drivers"], list)


def _raw_config() -> dict[str, object]:
    return deepcopy(load_ai_confirmation_universe_config().model_dump(mode="json"))


def _single_group_config(group_id: str, tickers: list[str]) -> AIConfirmationUniverseConfig:
    raw = _raw_config()
    for current_group_id, group in raw["ai_confirmation_universe"].items():
        group["enabled"] = current_group_id == group_id
        if current_group_id == group_id:
            group["symbols"] = [
                symbol for symbol in group["symbols"] if symbol["ticker"] in set(tickers)
            ]
            group["required_data_level"] = "warning"
    return AIConfirmationUniverseConfig.model_validate(raw)


def _make_ai_price_frame(
    slopes: dict[str, float],
    *,
    days: int = 260,
    start: date = date(2025, 1, 1),
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for day_index in range(days):
        current_date = start + timedelta(days=day_index)
        for symbol, slope in slopes.items():
            base = 100.0 + (sum(ord(char) for char in symbol) % 17)
            price = max(1.0, base + slope * day_index)
            rows.append(
                {
                    "date": current_date.isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price * 1.01,
                    "low": price * 0.99,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000 + day_index,
                    "source": "fixture",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            )
    return pd.DataFrame(rows)


def _mega_score(slopes: dict[str, float]) -> dict[str, object]:
    return _mega_score_from_prices(_make_ai_price_frame(slopes))


def _mega_score_from_prices(prices: pd.DataFrame) -> dict[str, object]:
    config = _single_group_config("mega_cap_ai", ["NVDA", "MSFT", "AMD"])
    policy = load_ai_confirmation_policy_config()
    run_date = date.fromisoformat(str(prices["date"].max()))
    breadth_records = build_ai_confirmation_breadth_features(
        prices,
        config=config,
        run_date=run_date,
        group_ids=["mega_cap_ai"],
    )
    return build_mega_cap_ai_confirmation_score(
        prices,
        breadth_records=breadth_records,
        universe_config=config,
        policy_config=policy,
        run_date=run_date,
    )


def _make_drawdown_price_frame(symbols: list[str]) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    start = date(2025, 1, 1)
    for day_index in range(260):
        current_date = start + timedelta(days=day_index)
        for symbol in symbols:
            if symbol in {"QQQ", "SPY"}:
                price = 100.0 + 0.1 * day_index
            elif day_index < 200:
                price = 100.0 + 1.0 * day_index
            else:
                price = 300.0 - 2.0 * (day_index - 200)
            rows.append(
                {
                    "date": current_date.isoformat(),
                    "symbol": symbol,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "adj_close": price,
                    "volume": 1_000_000 + day_index,
                    "source": "fixture",
                    "created_at": "2026-01-01T00:00:00Z",
                }
            )
    return pd.DataFrame(rows)
