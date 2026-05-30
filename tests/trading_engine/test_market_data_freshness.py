from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.trading_engine.market_data_freshness import (
    load_market_data_freshness_config,
    run_market_data_freshness,
    validate_market_data_freshness_payload,
)


def test_default_market_data_freshness_config_loads() -> None:
    config = load_market_data_freshness_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert config["safety"]["production_write_allowed"] is False
    assert config["safety"]["data_download_allowed"] is False
    assert "QQQ" in config["assets"]["required"]


def test_tracking_date_equals_effective_data_date_is_ok(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [date(2026, 1, 5), as_of], "NVDA": [date(2026, 1, 5), as_of]},
        manifest_date=as_of,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )

    assert validate_market_data_freshness_payload(run.payload) == []
    assert run.payload["freshness"]["status"] == "OK"
    assert run.payload["tracking_readiness"]["can_track"] is True
    assert run.payload["metadata"]["production_effect"] == "none"


def test_one_trading_day_lag_before_ready_time_is_acceptable(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    previous = date(2026, 1, 5)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [previous], "NVDA": [previous]},
        manifest_date=previous,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 6, 17, 0, tzinfo=UTC),
    )

    assert run.payload["freshness"]["status"] == "ACCEPTABLE_LAG"
    assert run.payload["freshness"]["lag_trading_days"] == 1
    assert run.payload["tracking_readiness"]["tracking_status_recommendation"] == (
        "degraded_tracking"
    )


def test_weekend_uses_previous_trading_day(tmp_path: Path) -> None:
    saturday = date(2026, 1, 10)
    friday = date(2026, 1, 9)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [friday], "NVDA": [friday]},
        manifest_date=friday,
    )

    run = run_market_data_freshness(
        as_of=saturday,
        config_path=config_path,
        generated_at=datetime(2026, 1, 10, 16, 0, tzinfo=UTC),
    )

    assert run.payload["freshness"]["status"] == "NON_TRADING_DAY"
    assert run.payload["calendar"]["is_trading_day"] is False
    assert run.payload["data_dates"]["effective_data_date"] == friday.isoformat()
    assert run.payload["tracking_readiness"]["tracking_status_recommendation"] == (
        "active_tracking"
    )


def test_stale_data_after_ready_window(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    previous = date(2026, 1, 5)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [previous], "NVDA": [previous]},
        manifest_date=previous,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )

    assert run.payload["freshness"]["status"] == "STALE"
    assert run.payload["tracking_readiness"]["can_track"] is False
    assert run.payload["suggested_actions"][0]["action"] == "refresh_market_data_cache"


def test_missing_required_asset_is_missing(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [as_of]},
        manifest_date=as_of,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )

    assert run.payload["freshness"]["status"] == "MISSING"
    assert run.payload["asset_coverage"]["missing_effective_assets"] == ["NVDA"]


def test_unknown_market_calendar_is_reported(tmp_path: Path) -> None:
    as_of = date(2026, 1, 6)
    config_path = _freshness_fixture(
        tmp_path,
        price_dates={"QQQ": [as_of], "NVDA": [as_of]},
        manifest_date=as_of,
    )

    run = run_market_data_freshness(
        as_of=as_of,
        market="EU",
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 1, 0, tzinfo=UTC),
    )

    assert run.payload["freshness"]["status"] == "MARKET_CALENDAR_UNKNOWN"
    assert run.payload["tracking_readiness"]["can_track"] is False


def _freshness_fixture(
    tmp_path: Path,
    *,
    price_dates: dict[str, list[date]],
    manifest_date: date,
    required_assets: tuple[str, ...] = ("QQQ", "NVDA"),
) -> Path:
    prices_path = tmp_path / "data" / "raw" / "prices_daily.csv"
    _write_prices(prices_path, price_dates)
    _write_backtest_manifest(
        tmp_path,
        manifest_date=manifest_date,
        prices_path=prices_path,
        required_assets=required_assets,
    )
    return _write_market_data_freshness_config(
        tmp_path,
        prices_path=prices_path,
        required_assets=required_assets,
    )


def _write_prices(path: Path, price_dates: dict[str, list[date]]) -> None:
    rows: list[dict[str, object]] = []
    for symbol, dates in price_dates.items():
        for offset, current in enumerate(dates):
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": symbol,
                    "symbol": symbol,
                    "canonical_symbol": symbol,
                    "source_symbol": symbol,
                    "open": 100.0 + offset,
                    "high": 101.0 + offset,
                    "low": 99.0 + offset,
                    "close": 100.5 + offset,
                    "adj_close": 100.5 + offset,
                    "volume": 1_000_000,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_backtest_manifest(
    tmp_path: Path,
    *,
    manifest_date: date,
    prices_path: Path,
    required_assets: tuple[str, ...],
) -> Path:
    path = (
        tmp_path
        / "artifacts"
        / "backtest_snapshots"
        / manifest_date.isoformat()
        / "backtest_input_manifest.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "backtest_input_manifest",
                "snapshot_id": f"backtest-input-{manifest_date.isoformat()}",
                "generated_at": "2026-01-07T00:00:00+00:00",
                "status": "OK",
                "production_effect": "none",
                "assets": list(required_assets),
                "symbol_mapping": {},
                "date_range": {
                    "start": min(
                        date.fromisoformat(item)
                        for item in pd.read_csv(prices_path)["date"].astype(str)
                    ).isoformat(),
                    "end": manifest_date.isoformat(),
                },
                "price_data_files": [str(prices_path)],
                "signal_snapshot_files": [],
                "data_quality_report": "",
                "config_hash": "test",
                "code_version": "test",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_market_data_freshness_config(
    tmp_path: Path,
    *,
    prices_path: Path,
    required_assets: tuple[str, ...] = ("QQQ", "NVDA"),
) -> Path:
    config_path = tmp_path / "config" / "data" / "market_data_freshness.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "market-data-freshness-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test freshness",
                "intended_effect": "test freshness",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "market": {
                    "id": "US",
                    "timezone": "America/New_York",
                    "close_time": "16:00",
                    "expected_data_ready_after_close_minutes": 180,
                    "allow_previous_trading_day_before_ready_time": True,
                },
                "freshness": {
                    "max_acceptable_lag_trading_days": 1,
                    "max_acceptable_lag_calendar_days": 3,
                    "require_all_assets_same_effective_date": True,
                },
                "assets": {"required": list(required_assets)},
                "input": {
                    "prices_path": str(prices_path),
                    "backtest_snapshot_dir": str(tmp_path / "artifacts" / "backtest_snapshots"),
                    "price_cache_registry_path": str(
                        tmp_path / "artifacts" / "data_registry" / "price_cache_registry.json"
                    ),
                    "download_manifest_path": str(
                        tmp_path / "data" / "raw" / "download_manifest.csv"
                    ),
                },
                "output": {
                    "market_data_freshness_dir": str(tmp_path / "artifacts" / "data_freshness"),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                    "dry_run_dir": str(tmp_path / "outputs" / "dry_runs" / "data_freshness"),
                },
                "safety": {
                    "production_write_allowed": False,
                    "data_download_allowed": False,
                    "fake_price_rows_allowed": False,
                    "data_quality_gate_lowered": False,
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path
