from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest
import typer
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli_commands.etf_portfolio import etf_app
from ai_trading_system.etf_portfolio.features import build_feature_store
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.etf_portfolio.trend_calibration import (
    DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
    TREND_CALIBRATION_SAFETY,
    TrendCalibrationError,
    build_trend_calibration_report,
    build_trend_calibration_validation_report,
    build_trend_signal_dataset,
    compute_trend_scores,
    load_trend_calibration_policy_config,
    run_trend_signal_weight_search,
    write_trend_calibration_report,
)
from ai_trading_system.interfaces.cli.etf_portfolio import trend_calibration as trend_cli
from ai_trading_system.reports import reader_brief

GENERATED_AT = datetime(2026, 6, 5, 12, 0, tzinfo=UTC)


def test_trend_calibration_policy_loads_and_rejects_unsafe(tmp_path: Path) -> None:
    policy = load_trend_calibration_policy_config()

    assert policy.safety.model_dump(mode="json") == TREND_CALIBRATION_SAFETY
    assert policy.market_regime.regime_id == "unified_primary_2021"
    assert policy.search.preset_weight_sets

    raw = yaml.safe_load(DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["safety"]["production_effect"] = "mutate_config"
    unsafe_path = tmp_path / "unsafe_trend_calibration.yaml"
    unsafe_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(TrendCalibrationError):
        load_trend_calibration_policy_config(unsafe_path)

    raw = yaml.safe_load(DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH.read_text(encoding="utf-8"))
    raw["search"]["preset_weight_sets"][0]["weights"]["price_trend"] = 0.90
    invalid_path = tmp_path / "invalid_trend_weights.yaml"
    invalid_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    with pytest.raises(TrendCalibrationError):
        load_trend_calibration_policy_config(invalid_path)


def test_dataset_search_report_are_evaluation_only_and_candidate_only() -> None:
    dataset = _build_dataset()
    policy = load_trend_calibration_policy_config()
    weight_set = policy.search.preset_weight_sets[0].model_dump(mode="json")

    score_run = compute_trend_scores(
        dataset,
        weight_set=weight_set,
        policy=policy,
        generated_at=GENERATED_AT,
    )
    search = run_trend_signal_weight_search(dataset, policy=policy, generated_at=GENERATED_AT)
    report = build_trend_calibration_report(
        dataset=dataset,
        policy=policy,
        generated_at=GENERATED_AT,
    )

    assert dataset["market_regime"] == "unified_primary_2021"
    assert dataset["records"][0]["evaluation_only"] is True
    assert dataset["records"][0]["forward_return_windows"]
    assert score_run["scores"][0]["CompositeTrendScore"] >= 0
    assert search["top_configs"][0]["trend_signal_config_id"]
    assert report["summary"]["top_config"] == search["top_configs"][0]["trend_signal_config_id"]
    assert report["production_effect"] == "none"

    serialized = json.dumps(report, ensure_ascii=False)
    assert "target_weights" not in serialized
    assert "production_weights" not in serialized
    assert "broker_order" not in serialized


def test_trend_calibration_validation_cli_and_reader_brief(tmp_path: Path) -> None:
    runner = CliRunner()
    result = runner.invoke(
        etf_app,
        [
            "trend-calibration",
            "validate",
            "--output-dir",
            str(tmp_path / "validation"),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )
    assert result.exit_code == 0, result.output

    validation = build_trend_calibration_validation_report(generated_at=GENERATED_AT)
    assert validation["status"] == "PASS"

    report = build_trend_calibration_report(
        dataset=_build_dataset(),
        policy=load_trend_calibration_policy_config(),
        generated_at=GENERATED_AT,
    )
    report_paths = write_trend_calibration_report(report, output_dir=tmp_path / "reports")
    summary = reader_brief._etf_trend_calibration_summary(
        {"reports": [_report_record("etf_trend_calibration_report", report_paths["json"])]}
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["top_config"] == report["summary"]["top_config"]
    assert summary["evaluation_only"] is True
    assert summary["safety_status"].startswith("observe_only=true")

    missing = reader_brief._etf_trend_calibration_summary({"reports": []})
    assert missing["availability"] == "MISSING"


def test_trend_calibration_run_stops_before_price_features_when_cached_dq_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def fail_dq(**_: object) -> tuple[Path, object]:
        calls.append("dq")
        raise typer.Exit(code=1)

    def unexpected_prices(*_: object, **__: object) -> tuple[pd.DataFrame, object]:
        calls.append("prices")
        return _sample_prices(), SimpleNamespace(passed=True, status="PASS")

    monkeypatch.setattr(trend_cli, "run_cached_data_quality_gate_with_report", fail_dq)
    monkeypatch.setattr(trend_cli, "load_standard_prices", unexpected_prices)

    with pytest.raises(typer.Exit):
        trend_cli.trend_calibration_run_command(
            prices_path=tmp_path / "prices.csv",
            rates_path=tmp_path / "rates.csv",
            as_of="2024-03-29",
            start="2023-01-03",
            end="2024-03-29",
            top=1,
            config_path=DEFAULT_TREND_CALIBRATION_POLICY_CONFIG_PATH,
            data_quality_output_path=tmp_path / "dq.md",
            dataset_output_dir=tmp_path / "datasets",
            report_output_dir=tmp_path / "reports",
            registry_output_dir=tmp_path / "registry",
        )

    assert calls == ["dq"]
    assert not (tmp_path / "datasets").exists()


def _build_dataset() -> dict[str, object]:
    config = load_etf_config_bundle()
    prices = _sample_prices()
    features = build_feature_store(prices, assets=config.assets, strategy=config.strategy)
    return build_trend_signal_dataset(
        features=features,
        prices=prices,
        strategy=config.strategy,
        policy=load_trend_calibration_policy_config(),
        start=pd.Timestamp("2023-01-03").date(),
        end=pd.Timestamp("2024-03-29").date(),
        data_quality_status="PASS",
        data_quality_report="test_quality_report",
        price_source_path="test_prices",
        generated_at=GENERATED_AT,
    )


def _sample_prices() -> pd.DataFrame:
    trading_dates = pd.bdate_range("2022-01-03", "2024-05-31")
    rows: list[dict[str, object]] = []
    for symbol, start_price, drift in (
        ("SPY", 450.0, 0.00025),
        ("QQQ", 360.0, 0.00035),
        ("SMH", 260.0, 0.00045),
        ("SOXX", 520.0, 0.00040),
    ):
        for index, dt in enumerate(trading_dates):
            seasonal = np.sin(index / 18.0) * 0.01
            price = start_price * (1.0 + drift + seasonal / 10.0) ** index
            rows.append(
                {
                    "date": dt.date().isoformat(),
                    "symbol": symbol,
                    "open": price * 0.99,
                    "high": price * 1.01,
                    "low": price * 0.98,
                    "close": price,
                    "adj_close": price,
                    "volume": 1000000 + index,
                    "source": "test",
                    "created_at": "2026-06-05T00:00:00+00:00",
                }
            )
    return pd.DataFrame(rows)


def _report_record(report_id: str, path: Path) -> dict[str, object]:
    return {
        "report_id": report_id,
        "latest_artifact_path": str(path),
        "latest_artifact_name": path.name,
        "artifact_date": "2026-06-05",
        "freshness_status": "FRESH",
        "artifact_status": "PASS",
        "exists": True,
        "age_days": 0,
    }
