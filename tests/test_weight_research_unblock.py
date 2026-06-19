from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import weight_research_app
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.weight_research_unblock import (
    B1_FORBIDDEN_MECHANISMS,
    DEFAULT_SCOPE_FREEZE_PATH,
    build_b1_metric_semantics_audit,
    build_contract_validation,
    load_b1_execution_policy,
    simulate_b1_execution_control,
    simulate_static_baseline_path,
)


def test_weight_research_unblock_contract_validation_passes_for_b1_window() -> None:
    payload = build_contract_validation(
        layer_id="B1",
        run_start=date(2023, 1, 3),
        run_end=date(2023, 7, 31),
    )

    assert payload["status"] == "PASS"
    assert payload["safety_boundary"]["official_target_weights"] is False
    assert payload["safety_boundary"]["production_effect"] == "none"


def test_weight_research_unblock_validation_fails_on_early_holdout_use() -> None:
    payload = build_contract_validation(
        layer_id="B1",
        run_start=date(2026, 7, 1),
        run_end=date(2026, 7, 31),
    )

    assert payload["status"] == "FAIL"
    assert "run_window_does_not_use_holdout_early" in payload["blocking_checks"]


def test_weight_research_scope_rejects_b1_mixed_logic(tmp_path: Path) -> None:
    scope = json.loads(DEFAULT_SCOPE_FREEZE_PATH.read_text(encoding="utf-8"))
    broken = deepcopy(scope)
    for layer in broken["layers"]:
        if layer["layer_id"] == "B1":
            layer["forbidden_mechanisms"] = [
                item
                for item in layer["forbidden_mechanisms"]
                if item != "trend_signal"
            ]
    scope_path = tmp_path / "broken_scope.json"
    scope_path.write_text(json.dumps(broken), encoding="utf-8")

    payload = build_contract_validation(scope_path=scope_path, layer_id="B1")

    assert payload["status"] == "FAIL"
    assert "b1_forbidden_mixed_logic" in payload["blocking_checks"]


def test_b1_execution_control_simulation_uses_only_allowed_mechanism() -> None:
    config = load_etf_config_bundle()
    policy = load_b1_execution_policy()
    prices = _price_fixture()

    daily = simulate_b1_execution_control(
        prices=prices,
        config=config,
        policy=policy,
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
    )

    assert not daily.empty
    assert set(daily["added_mechanism"]) == {"execution_no_trade_turnover_control_only"}
    assert set(daily["forbidden_logic_check"]) == {
        "PASS_NO_SIGNAL_ALLOCATOR_REGIME_OR_CONFIDENCE_INPUTS"
    }
    assert daily["official_target_weights"].eq(False).all()
    assert daily["production_effect"].eq("none").all()
    assert B1_FORBIDDEN_MECHANISMS <= set(load_b1_execution_policy().forbidden_mechanisms)


def test_b1_metric_semantics_audit_marks_historical_b1_as_partial() -> None:
    payload = build_b1_metric_semantics_audit()

    assert payload["status"] == "B1_ATTRIBUTION_PARTIAL"
    assert payload["metric_contract"]["drawdown_reduction"]["positive_direction"] == (
        "candidate drawdown is smaller"
    )
    assert "pure execution-control attribution conclusion" in payload[
        "historical_b1_usage_scope"
    ]["forbidden"]


def test_static_baseline_family_separates_hold_and_rebalance_semantics() -> None:
    config = load_etf_config_bundle()
    prices = _price_fixture()

    b0h = simulate_static_baseline_path(
        prices=prices,
        config=config,
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
        variant_id="B0H",
    )
    b0r = simulate_static_baseline_path(
        prices=prices,
        config=config,
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
        variant_id="B0R",
    )

    assert not b0h.empty
    assert not b0r.empty
    assert b0h["turnover"].sum() == 0.0
    assert b0r["turnover"].sum() > 0.0
    assert set(b0r["decision"]) == {"REBALANCE_TO_STATIC_TARGET"}
    assert set(b0r["forbidden_logic_check"]) == {
        "PASS_NO_SIGNAL_ALLOCATOR_REGIME_OR_CONFIDENCE_INPUTS"
    }
    assert list(b0r["target_weights_json"]) == list(b0h["target_weights_json"])


def test_b1e_and_b0r_share_target_path_for_isolated_attribution() -> None:
    config = load_etf_config_bundle()
    policy = load_b1_execution_policy()
    prices = _price_fixture()

    b1e = simulate_b1_execution_control(
        prices=prices,
        config=config,
        policy=policy,
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
    )
    b0r = simulate_static_baseline_path(
        prices=prices,
        config=config,
        start=date(2023, 1, 3),
        end=date(2023, 1, 20),
        variant_id="B0R",
    )

    assert list(b1e["signal_date"]) == list(b0r["signal_date"])
    assert list(b1e["target_weights_json"]) == list(b0r["target_weights_json"])
    assert b1e["turnover"].sum() <= b0r["turnover"].sum()


def test_weight_research_validate_contracts_cli_writes_outputs(tmp_path: Path) -> None:
    runner = CliRunner()

    result = runner.invoke(
        weight_research_app,
        [
            "validate-contracts",
            "--from",
            "2023-01-03",
            "--to",
            "2023-07-31",
            "--output-dir",
            str(tmp_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "weight_research_contract_validation_status=PASS" in result.output
    assert list(tmp_path.glob("contract_validation_*.json"))
    assert list(tmp_path.glob("contract_validation_*.md"))


def _price_fixture() -> pd.DataFrame:
    dates = pd.bdate_range("2023-01-03", periods=20)
    rows: list[dict[str, object]] = []
    base_prices = {"SPY": 100.0, "QQQ": 100.0, "SMH": 100.0, "SOXX": 100.0}
    daily_returns = {"SPY": 0.001, "QQQ": 0.003, "SMH": -0.002, "SOXX": 0.002}
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
