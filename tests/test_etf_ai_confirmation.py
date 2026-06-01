from __future__ import annotations

from copy import deepcopy

import pytest

from ai_trading_system.etf_portfolio.ai_confirmation import (
    AIConfirmationUniverseConfig,
    all_enabled_tickers,
    enabled_symbols_for_group,
    load_ai_confirmation_universe_config,
    validate_ai_confirmation_data_availability,
)


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


def _raw_config() -> dict[str, object]:
    return deepcopy(load_ai_confirmation_universe_config().model_dump(mode="json"))
