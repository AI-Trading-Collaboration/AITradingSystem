from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.trading_engine import market_data_refresh
from ai_trading_system.trading_engine.market_data_refresh import (
    load_market_data_refresh_config,
    run_market_data_refresh,
    validate_market_data_refresh_payload,
)


def test_default_market_data_refresh_config_loads() -> None:
    config = load_market_data_refresh_config()

    assert config["production_effect"] == "none"
    assert config["manual_review_required"] is True
    assert config["auto_promotion"] is False
    assert config["safety"]["forbid_mock_prices"] is True
    assert config["safety"]["forbid_synthetic_latest_bar"] is True
    assert "BRK.B" in config["assets"]["required"]


def test_refresh_plan_generated_when_freshness_stale(tmp_path: Path) -> None:
    target = date(2026, 1, 6)
    previous = date(2026, 1, 5)
    prices_path = _write_prices(tmp_path, {"GOOGL": [previous], "BRK.B": [previous]})
    _write_freshness(
        tmp_path,
        target=target,
        effective=previous,
        status="STALE",
        missing_expected=("GOOGL", "BRK.B"),
    )
    config_path = _write_refresh_config(
        tmp_path,
        prices_path=prices_path,
        required_assets=("GOOGL", "BRK.B"),
    )
    before = prices_path.read_text(encoding="utf-8")

    run = run_market_data_refresh(
        as_of=target,
        config_path=config_path,
        dry_run=True,
        generated_at=datetime(2026, 1, 7, 2, 0, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] == "PLANNED"
    assert run.payload["refresh_actions"][0]["symbols"] == ["GOOGL", "BRK.B"]
    assert run.payload["symbol_mapping"]["BRK.B"]["source_symbol"] == "BRK-B"
    assert run.plan_path.exists()
    assert prices_path.read_text(encoding="utf-8") == before


def test_refresh_not_needed_when_freshness_ok(tmp_path: Path) -> None:
    target = date(2026, 1, 6)
    prices_path = _write_prices(tmp_path, {"GOOGL": [target], "BRK.B": [target]})
    _write_freshness(
        tmp_path,
        target=target,
        effective=target,
        status="OK",
        missing_expected=(),
    )
    config_path = _write_refresh_config(
        tmp_path,
        prices_path=prices_path,
        required_assets=("GOOGL", "BRK.B"),
    )

    run = run_market_data_refresh(
        as_of=target,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 2, 0, tzinfo=UTC),
    )

    assert validate_market_data_refresh_payload(run.payload) == []
    assert run.payload["metadata"]["status"] == "NOT_NEEDED"
    assert run.payload["actions"]["updated_price_cache"] is False
    assert run.payload["after"]["freshness_status"] == "OK"


def test_source_delayed_when_target_date_unavailable(tmp_path: Path, monkeypatch) -> None:
    target = date(2026, 1, 6)
    previous = date(2026, 1, 5)
    prices_path = _write_prices(tmp_path, {"GOOGL": [previous]})
    _write_freshness(
        tmp_path,
        target=target,
        effective=previous,
        status="STALE",
        missing_expected=("GOOGL",),
    )
    config_path = _write_refresh_config(
        tmp_path,
        prices_path=prices_path,
        required_assets=("GOOGL",),
        allow_external_fetch=False,
    )
    before = prices_path.read_text(encoding="utf-8")
    monkeypatch.setattr(
        market_data_refresh,
        "run_market_data_freshness",
        lambda **_: _Run(
            {
                "freshness": {"status": "STALE", "reason": "still stale"},
                "data_dates": {"effective_data_date": previous.isoformat()},
                "tracking_readiness": {"readiness": "cannot_track"},
                "output_artifacts": {},
            }
        ),
    )
    monkeypatch.setattr(
        market_data_refresh,
        "run_portfolio_candidate_tracking",
        lambda **_: _Run({"candidate": {"tracking_status": "tracking_blocked"}}),
    )

    run = run_market_data_refresh(
        as_of=target,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 2, 0, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] == "SOURCE_DELAYED"
    assert run.payload["actions"]["updated_price_cache"] is False
    assert "GOOGL" in run.payload["remaining_limitations"][0]
    assert prices_path.read_text(encoding="utf-8") == before


def test_audited_cache_refresh_registers_required_assets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    target = date(2026, 1, 6)
    previous = date(2026, 1, 5)
    required_assets = ("GOOGL", "BRK.B", "SGOV")
    prices_path = _write_prices(tmp_path, {symbol: [previous] for symbol in required_assets})
    _write_freshness(
        tmp_path,
        target=target,
        effective=previous,
        status="STALE",
        missing_expected=required_assets,
    )
    for symbol, source_symbol in {
        "GOOGL": "GOOGL",
        "BRK.B": "BRK-B",
        "SGOV": "SGOV",
    }.items():
        _write_audited_fmp_cache_row(
            tmp_path,
            canonical_symbol=symbol,
            source_symbol=source_symbol,
            target=target,
        )
    config_path = _write_refresh_config(
        tmp_path,
        prices_path=prices_path,
        required_assets=required_assets,
        allow_external_fetch=False,
    )
    monkeypatch.setattr(
        market_data_refresh,
        "refresh_backtest_manifest",
        lambda **_: _ManifestRun(
            {
                "target_manifest_date": target.isoformat(),
                "would_write_manifest": str(
                    tmp_path
                    / "artifacts"
                    / "backtest_snapshots"
                    / target.isoformat()
                    / "backtest_input_manifest.json"
                ),
            }
        ),
    )
    monkeypatch.setattr(
        market_data_refresh,
        "run_market_data_freshness",
        lambda **_: _Run(
            {
                "freshness": {"status": "OK", "reason": "recovered"},
                "data_dates": {"effective_data_date": target.isoformat()},
                "tracking_readiness": {"readiness": "can_track"},
                "output_artifacts": {"summary_json": "freshness.json"},
            }
        ),
    )
    monkeypatch.setattr(
        market_data_refresh,
        "run_portfolio_candidate_tracking",
        lambda **_: _Run({"candidate": {"tracking_status": "active_tracking"}}),
    )

    run = run_market_data_refresh(
        as_of=target,
        config_path=config_path,
        generated_at=datetime(2026, 1, 7, 2, 0, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] == "OK"
    assert set(run.payload["actions"]["fetched_assets"]) == set(required_assets)
    refreshed = pd.read_csv(prices_path)
    assert set(refreshed.loc[refreshed["date"] == target.isoformat(), "ticker"]) == set(
        required_assets
    )
    registry = json.loads(
        (tmp_path / "artifacts" / "data_registry" / "price_cache_registry.json").read_text(
            encoding="utf-8"
        )
    )
    assert registry["metadata"]["status"] == "OK"
    assert registry["assets"]["GOOGL"]["latest_date"] == target.isoformat()
    assert registry["assets"]["BRK.B"]["source_symbol"] == "BRK-B"
    assert registry["assets"]["SGOV"]["schema_status"] == "OK"
    assert run.payload["after"]["candidate_tracking_status"] == "active_tracking"
    assert run.payload["safety"]["synthetic_latest_bar_generated"] is False


@dataclass(frozen=True)
class _Run:
    payload: dict[str, object]


@dataclass(frozen=True)
class _ManifestRun:
    payload: dict[str, object]


def _write_prices(
    tmp_path: Path,
    price_dates: dict[str, list[date]],
) -> Path:
    path = tmp_path / "data" / "raw" / "prices_daily.csv"
    rows: list[dict[str, object]] = []
    for symbol, dates in price_dates.items():
        source_symbol = "BRK-B" if symbol == "BRK.B" else symbol
        for offset, current in enumerate(dates):
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": symbol,
                    "symbol": symbol,
                    "canonical_symbol": symbol,
                    "source_symbol": source_symbol,
                    "open": 100.0 + offset,
                    "high": 101.0 + offset,
                    "low": 99.0 + offset,
                    "close": 100.5 + offset,
                    "adj_close": 100.5 + offset,
                    "volume": 1_000_000,
                    "source": "existing_cache",
                    "updated_at": "2026-01-07T00:00:00+00:00",
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_freshness(
    tmp_path: Path,
    *,
    target: date,
    effective: date,
    status: str,
    missing_expected: tuple[str, ...],
) -> Path:
    path = (
        tmp_path
        / "artifacts"
        / "data_freshness"
        / target.isoformat()
        / "market_data_freshness_summary.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "market_data_freshness",
                "metadata": {
                    "run_id": f"market-data-freshness-{target.isoformat()}",
                    "generated_at": "2026-01-07T00:00:00+00:00",
                    "status": status,
                    "production_effect": "none",
                    "manual_review_required": True,
                    "auto_promotion": False,
                },
                "data_dates": {
                    "tracking_date": target.isoformat(),
                    "expected_data_date": target.isoformat(),
                    "effective_data_date": effective.isoformat(),
                },
                "freshness": {"status": status, "reason": "test"},
                "asset_coverage": {
                    "missing_expected_date_assets": list(missing_expected),
                },
                "tracking_readiness": {
                    "readiness": "can_track" if status == "OK" else "cannot_track",
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def _write_refresh_config(
    tmp_path: Path,
    *,
    prices_path: Path,
    required_assets: tuple[str, ...],
    allow_external_fetch: bool = True,
) -> Path:
    config_path = tmp_path / "config" / "data" / "market_data_refresh.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "version": "market-data-refresh-test",
                "owner": "tests",
                "status": "pilot",
                "production_effect": "none",
                "manual_review_required": True,
                "auto_promotion": False,
                "observe_only": True,
                "rationale": "test refresh",
                "intended_effect": "test refresh",
                "validation_evidence": "unit tests",
                "review_condition": "test review",
                "refresh": {
                    "enabled": True,
                    "mode": "required_assets_only",
                    "allow_external_fetch": allow_external_fetch,
                    "allow_audited_raw_cache": True,
                    "allow_partial_refresh": False,
                    "require_schema_validation": True,
                    "require_registry_update": True,
                    "require_manifest_refresh": True,
                },
                "sources": {
                    "preferred_order": ["audited_fmp_raw_cache", "fmp", "yahoo"],
                    "fmp_api_key_env": "AIT_TEST_FMP_TOKEN",
                },
                "assets": {"required": list(required_assets)},
                "input": {
                    "prices_path": str(prices_path),
                    "market_data_freshness_dir": str(
                        tmp_path / "artifacts" / "data_freshness"
                    ),
                    "market_data_freshness_config_path": str(
                        tmp_path / "config" / "data" / "market_data_freshness.yaml"
                    ),
                    "portfolio_candidate_tracking_config_path": str(
                        tmp_path
                        / "config"
                        / "portfolio"
                        / "portfolio_candidate_tracking.yaml"
                    ),
                    "external_request_cache_dir": str(
                        tmp_path / "data" / "raw" / "external_request_cache"
                    ),
                    "price_cache_registry_path": str(
                        tmp_path
                        / "artifacts"
                        / "data_registry"
                        / "price_cache_registry.json"
                    ),
                },
                "output": {
                    "market_data_refresh_dir": str(tmp_path / "artifacts" / "data_refresh"),
                    "report_alias_dir": str(tmp_path / "outputs" / "reports"),
                    "dry_run_dir": str(tmp_path / "outputs" / "dry_runs" / "data_refresh"),
                },
                "safety": {
                    "production_effect": "none",
                    "auto_promotion": False,
                    "manual_review_required": True,
                    "production_write_allowed": False,
                    "forbid_mock_prices": True,
                    "forbid_synthetic_latest_bar": True,
                    "data_quality_gate_lowered": False,
                },
            },
            sort_keys=False,
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    return config_path


def _write_audited_fmp_cache_row(
    tmp_path: Path,
    *,
    canonical_symbol: str,
    source_symbol: str,
    target: date,
) -> None:
    cache_dir = (
        tmp_path
        / "data"
        / "raw"
        / "external_request_cache"
        / "Financial_Modeling_Prep"
        / "eod_daily_prices"
        / source_symbol
        / "fixture"
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    body_path = cache_dir / "response.body"
    body_path.write_text(
        json.dumps(
            [
                {
                    "symbol": source_symbol,
                    "date": target.isoformat(),
                    "adjOpen": 101.0,
                    "adjHigh": 102.0,
                    "adjLow": 100.0,
                    "adjClose": 101.5,
                    "volume": 123456,
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (cache_dir / "metadata.json").write_text(
        json.dumps(
            {
                "status_code": 200,
                "created_at": "2026-01-07T00:00:00+00:00",
                "endpoint": "https://financialmodelingprep.com/stable/historical-price-eod",
                "body_path": str(body_path),
                "request_identity": {
                    "params": {
                        "symbol": source_symbol,
                        "from": target.isoformat(),
                        "to": target.isoformat(),
                    }
                },
                "canonical_symbol": canonical_symbol,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
