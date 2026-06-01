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
    build_ai_confirmation_composite_score,
    build_ai_confirmation_report,
    build_ai_event_risk_overlay,
    build_ai_semiconductor_relative_strength_score,
    build_mega_cap_ai_confirmation_score,
    enabled_symbols_for_group,
    event_risk_band,
    load_ai_confirmation_policy_config,
    load_ai_confirmation_universe_config,
    render_ai_confirmation_report_markdown,
    score_band,
    validate_ai_confirmation_data_availability,
    write_ai_confirmation_report,
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


def test_ai_semiconductor_relative_strength_score_rewards_smh_vs_qqq() -> None:
    strong = _relative_strength_score({"SPY": 0.2, "QQQ": 0.4, "SMH": 1.0, "SOXX": 0.9})
    weak = _relative_strength_score({"SPY": 0.2, "QQQ": 0.9, "SMH": 0.1, "SOXX": 0.1})

    assert strong["score_value"] > weak["score_value"]
    assert strong["component_scores"]["semiconductor_vs_growth"] > weak[
        "component_scores"
    ]["semiconductor_vs_growth"]


def test_ai_semiconductor_relative_strength_growth_vs_market_component() -> None:
    growth_leads = _relative_strength_score({"SPY": 0.1, "QQQ": 1.0, "SMH": 0.9, "SOXX": 0.8})
    growth_lags = _relative_strength_score({"SPY": 1.0, "QQQ": 0.1, "SMH": 0.2, "SOXX": 0.2})

    assert growth_leads["component_scores"]["growth_vs_market"] > growth_lags[
        "component_scores"
    ]["growth_vs_market"]


def test_ai_semiconductor_relative_strength_optional_pairs_do_not_block() -> None:
    score = _relative_strength_score({"SPY": 0.2, "QQQ": 0.4, "SMH": 0.8, "SOXX": 0.7})

    assert score["score_name"] == "AISemiconductorRelativeStrengthScore"
    assert score["score_value"] > 0
    assert any("optional_pair_missing" in warning for warning in score["warnings"])
    assert score["observe_only"] is True
    assert score["candidate_only"] is True
    assert score["production_effect"] == "none"
    assert score["broker_action"] == "none"


def test_ai_semiconductor_relative_strength_drawdown_penalty_works() -> None:
    steady = _relative_strength_score({"SPY": 0.2, "QQQ": 0.4, "SMH": 0.8, "SOXX": 0.8})
    drawdown_prices = _make_relative_drawdown_prices()
    drawdown = build_ai_semiconductor_relative_strength_score(
        drawdown_prices,
        policy_config=load_ai_confirmation_policy_config(),
        run_date=date.fromisoformat(str(drawdown_prices["date"].max())),
    )

    assert drawdown["component_scores"]["relative_drawdown_penalty"] < steady[
        "component_scores"
    ]["relative_drawdown_penalty"]


def test_ai_semiconductor_relative_strength_score_band_and_pair_schema() -> None:
    score = _relative_strength_score(
        {"SPY": 0.05, "QQQ": 1.0, "SMH": 1.5, "SOXX": 1.4, "XLK": 0.9, "IGV": 1.1}
    )

    assert score["score_band"] in {
        "strong_confirm",
        "confirm",
        "neutral",
        "weak",
        "negative",
    }
    required_pairs = {pair["pair"] for pair in score["pair_features"] if not pair["optional"]}
    assert {"QQQ/SPY", "SMH/QQQ", "SOXX/QQQ", "SMH/SPY", "SOXX/SPY"} <= required_pairs
    assert all("relative_return_60d" in pair for pair in score["pair_features"])


def test_ai_event_risk_inside_lookahead_window_increases_risk() -> None:
    overlay = _event_overlay(
        [
            _event(
                event_id="nvda_earnings",
                event_date="2026-06-03",
                event_type="NVDA earnings",
                related_symbols=["NVDA"],
                severity="high",
                lookahead_window_days=3,
            )
        ],
        run_date=date(2026, 6, 1),
    )

    assert overlay["event_risk_score"] >= 75
    assert overlay["risk_band"] == "high"
    assert overlay["active_events"]
    assert overlay["upcoming_events"]
    assert "mega_cap_ai" in overlay["affected_groups"]
    assert "semiconductor_hardware" in overlay["affected_groups"]


def test_ai_event_risk_outside_window_does_not_increase_risk() -> None:
    overlay = _event_overlay(
        [
            _event(
                event_id="old_cpi",
                event_date="2026-05-01",
                event_type="CPI",
                related_symbols=[],
                severity="critical",
                lookback_window_days=2,
                lookahead_window_days=2,
            )
        ],
        run_date=date(2026, 6, 1),
    )

    assert overlay["event_risk_score"] == 0
    assert overlay["risk_band"] == "low"
    assert overlay["active_events"] == []
    assert overlay["reason_codes"] == ["no_active_ai_event_risk"]


def test_ai_event_risk_multiple_events_aggregate_and_map_macro_groups() -> None:
    overlay = _event_overlay(
        [
            _event(
                event_id="fomc",
                event_date="2026-06-01",
                event_type="FOMC",
                related_symbols=[],
                severity="medium",
            ),
            _event(
                event_id="tsm_earnings",
                event_date="2026-06-02",
                event_type="TSM earnings",
                related_symbols=["TSM"],
                severity="medium",
                lookahead_window_days=2,
            ),
        ],
        run_date=date(2026, 6, 1),
    )

    assert overlay["event_risk_score"] == pytest.approx(50.0)
    assert overlay["risk_band"] == "high"
    assert {"mega_cap_ai", "semiconductor_hardware", "cloud_ai_platform"} <= set(
        overlay["affected_groups"]
    )


def test_ai_event_risk_optional_missing_source_is_safe() -> None:
    overlay = _event_overlay([], run_date=date(2026, 6, 1))

    assert overlay["event_risk_score"] == 0
    assert overlay["active_events"] == []
    assert overlay["observe_only"] is True
    assert overlay["candidate_only"] is True
    assert overlay["production_effect"] == "none"
    assert overlay["broker_action"] == "none"
    assert overlay["manual_review_required"] is True
    assert event_risk_band(100, load_ai_confirmation_policy_config()) == "critical"


def test_ai_confirmation_composite_combines_components_and_safety_fields() -> None:
    composite = _composite_score(
        mega_score=82.0,
        relative_score=74.0,
        event_risk=20.0,
        semiconductor_breadth=0.8,
        coverage=1.0,
    )

    assert composite["AIConfirmationScore"] > 70
    assert composite["score_band"] in {"confirm", "strong_confirm"}
    assert composite["action_hint"] in {
        "supports_ai_overweight_candidate",
        "supports_neutral_ai_exposure",
    }
    assert composite["component_scores"]["event_risk_adjustment"] == 80.0
    assert composite["observe_only"] is True
    assert composite["candidate_only"] is True
    assert composite["production_effect"] == "none"
    assert composite["broker_action"] == "none"
    assert composite["manual_review_required"] is True


def test_ai_confirmation_composite_event_risk_reduces_or_flags_score() -> None:
    low_event = _composite_score(
        mega_score=85.0,
        relative_score=80.0,
        event_risk=10.0,
        semiconductor_breadth=0.9,
        coverage=1.0,
    )
    high_event = _composite_score(
        mega_score=85.0,
        relative_score=80.0,
        event_risk=90.0,
        semiconductor_breadth=0.9,
        coverage=1.0,
    )

    assert high_event["AIConfirmationScore"] < low_event["AIConfirmationScore"]
    assert high_event["action_hint"] == "warns_against_ai_overweight"
    assert "high_event_risk" in high_event["reason_codes"]


def test_ai_confirmation_composite_low_coverage_is_insufficient_data() -> None:
    composite = _composite_score(
        mega_score=85.0,
        relative_score=80.0,
        event_risk=10.0,
        semiconductor_breadth=0.9,
        coverage=0.4,
    )

    assert composite["action_hint"] == "insufficient_data"
    assert "insufficient_data_coverage" in composite["reason_codes"]


def test_ai_confirmation_report_json_and_markdown_include_required_sections(
    tmp_path: Path,
) -> None:
    run_date = date(2026, 6, 1)
    prices = _standardized_report_prices()
    report = build_ai_confirmation_report(
        prices=prices,
        events=[
            _event(
                event_id="nvda_earnings",
                event_date=run_date + timedelta(days=1),
                event_type="NVDA earnings",
                related_symbols=["NVDA"],
                severity="medium",
                lookahead_window_days=3,
            )
        ],
        universe_config=load_ai_confirmation_universe_config(),
        policy_config=load_ai_confirmation_policy_config(),
        run_date=run_date,
        data_quality_status="PASS",
        data_quality_report="outputs/reports/data_quality_2026-06-01.md",
        market_regime="ai_after_chatgpt",
        requested_date_range={"start": "2025-10-15", "end": "2026-06-01"},
    )

    json_path = tmp_path / "ai_confirmation_report_2026-06-01.json"
    markdown_path = tmp_path / "ai_confirmation_report_2026-06-01.md"
    write_ai_confirmation_report(report, json_path=json_path, markdown_path=markdown_path)

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    markdown = markdown_path.read_text(encoding="utf-8")
    assert payload["schema_version"] == "ai_confirmation_report_v1"
    assert payload["AIConfirmationScore"]["score_name"] == "AIConfirmationScore"
    assert "semiconductor_breadth" in payload["component_scores"]
    assert payload["event_risk_overlay"]["risk_band"] in {"medium", "high"}
    assert payload["data_coverage"]["composite_data_coverage_ratio"] > 0
    assert payload["candidate_only_usage_note"]
    assert payload["observe_only"] is True
    assert payload["candidate_only"] is True
    assert payload["production_effect"] == "none"
    assert payload["broker_action"] == "none"
    assert payload["manual_review_required"] is True
    assert "# AI Confirmation Report" in markdown
    assert "## Component Scores" in markdown
    assert "## Event Risk Overlay" in markdown
    assert "candidate-only" in markdown
    assert "observe_only=true" in markdown


def test_ai_confirmation_report_markdown_is_stable() -> None:
    report = build_ai_confirmation_report(
        prices=_standardized_report_prices(),
        events=[],
        universe_config=load_ai_confirmation_universe_config(),
        policy_config=load_ai_confirmation_policy_config(),
        run_date=date(2026, 6, 1),
        data_quality_status="PASS",
        data_quality_report="outputs/reports/data_quality_2026-06-01.md",
        market_regime="ai_after_chatgpt",
        requested_date_range={"start": "2025-10-15", "end": "2026-06-01"},
    )

    markdown = render_ai_confirmation_report_markdown(report)

    assert "- AIConfirmationScore:" in markdown
    assert "| semiconductor_breadth |" in markdown
    assert "- Data Coverage:" in markdown
    assert "- Event Risk Score:" in markdown
    assert "Use AIConfirmationScore only as a bounded shadow overlay input" in markdown


def test_ai_confirmation_report_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    runner = CliRunner()
    prices = _standardized_report_prices()
    prices_path = tmp_path / "prices.csv"
    prices.to_csv(prices_path, index=False)
    output_dir = tmp_path / "reports"
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
            "report",
            "--prices-path",
            str(prices_path),
            "--date",
            "2026-06-01",
            "--output-dir",
            str(output_dir),
            "--universe-path",
            str(universe_path),
        ],
    )

    assert result.exit_code == 0, result.output
    json_path = output_dir / "ai_confirmation_report_2026-06-01.json"
    markdown_path = output_dir / "ai_confirmation_report_2026-06-01.md"
    assert json_path.exists()
    assert markdown_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["report_type"] == "ai_confirmation_report"
    assert payload["data_quality"]["status"] == "PASS"
    assert payload["AIConfirmationScore"]["candidate_only"] is True
    assert "production_effect=none" in markdown_path.read_text(encoding="utf-8")


def _raw_config() -> dict[str, object]:
    return deepcopy(load_ai_confirmation_universe_config().model_dump(mode="json"))


def _standardized_report_prices() -> pd.DataFrame:
    symbols = {
        "SPY": 0.20,
        "QQQ": 0.35,
        "SMH": 0.75,
        "SOXX": 0.70,
        "NVDA": 0.95,
        "AVGO": 0.80,
        "AMD": 0.65,
        "MSFT": 0.45,
        "GOOGL": 0.40,
        "AMZN": 0.42,
        "META": 0.50,
        "AAPL": 0.25,
        "TSM": 0.70,
        "ASML": 0.62,
        "AMAT": 0.58,
        "LRCX": 0.55,
        "MU": 0.50,
        "MRVL": 0.48,
        "ARM": 0.52,
        "QCOM": 0.28,
        "INTC": 0.10,
    }
    raw = _make_ai_price_frame(symbols, days=275, start=date(2025, 9, 1))
    etf_config = load_etf_config_bundle()
    prices, _ = standardize_price_frame(
        raw,
        assets=etf_config.assets,
        source_name="fixture",
        extra_symbols=set(symbols),
    )
    return prices


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


def _relative_strength_score(slopes: dict[str, float]) -> dict[str, object]:
    prices = _make_ai_price_frame(slopes, days=260)
    return build_ai_semiconductor_relative_strength_score(
        prices,
        policy_config=load_ai_confirmation_policy_config(),
        run_date=date.fromisoformat(str(prices["date"].max())),
    )


def _make_relative_drawdown_prices() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    start = date(2025, 1, 1)
    for day_index in range(260):
        current_date = start + timedelta(days=day_index)
        for symbol in ["SPY", "QQQ", "SMH", "SOXX"]:
            if symbol == "SPY":
                price = 100.0 + 0.1 * day_index
            elif symbol == "QQQ":
                price = 100.0 + 0.2 * day_index
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


def _event_overlay(events: list[dict[str, object]], *, run_date: date) -> dict[str, object]:
    return build_ai_event_risk_overlay(
        events,
        universe_config=load_ai_confirmation_universe_config(),
        policy_config=load_ai_confirmation_policy_config(),
        run_date=run_date,
    )


def _event(
    *,
    event_id: str,
    event_date: str,
    event_type: str,
    related_symbols: list[str],
    severity: str,
    lookback_window_days: int = 1,
    lookahead_window_days: int = 1,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_date": event_date,
        "event_type": event_type,
        "related_symbols": related_symbols,
        "severity": severity,
        "lookback_window_days": lookback_window_days,
        "lookahead_window_days": lookahead_window_days,
        "source": "fixture",
        "confidence": "high",
        "optional": True,
    }


def _composite_score(
    *,
    mega_score: float,
    relative_score: float,
    event_risk: float,
    semiconductor_breadth: float,
    coverage: float,
) -> dict[str, object]:
    run_date = date(2026, 6, 1)
    breadth_records = [
        {
            "date": run_date.isoformat(),
            "group_id": "semiconductor_hardware",
            "feature_values": {
                "percent_above_20d_ma": semiconductor_breadth,
                "percent_above_50d_ma": semiconductor_breadth,
                "percent_above_100d_ma": semiconductor_breadth,
                "percent_above_200d_ma": semiconductor_breadth,
                "percent_positive_20d_return": semiconductor_breadth,
                "percent_positive_60d_return": semiconductor_breadth,
            },
            "data_coverage_ratio": coverage,
            "warnings": [],
        }
    ]
    return build_ai_confirmation_composite_score(
        breadth_records=breadth_records,
        mega_cap_score={
            "score_value": mega_score,
            "data_coverage_ratio": coverage,
        },
        relative_strength_score={"score_value": relative_score},
        event_risk_overlay={"event_risk_score": event_risk},
        policy_config=load_ai_confirmation_policy_config(),
        run_date=run_date,
    )
