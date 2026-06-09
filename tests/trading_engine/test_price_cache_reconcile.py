from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path

import pandas as pd
import yaml

import ai_trading_system.trading_engine.price_cache_reconcile as reconcile_module
from ai_trading_system.trading_engine.price_cache_reconcile import run_price_cache_reconcile
from trading_engine.test_shadow_parameter_backtest import _write_shadow_backtest_fixture


def test_reconcile_dry_run_does_not_write_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch)
    prices_path = tmp_path / "data" / "prices_daily.csv"
    registry_path = fixture["output_dir"] / "data_registry" / "price_cache_registry.json"

    run = run_price_cache_reconcile(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        dry_run=True,
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    prices = pd.read_csv(prices_path)
    assert run.payload["metadata"]["status"] == "DRY_RUN"
    assert run.json_path is None
    assert not registry_path.exists()
    assert "GOOGL" not in set(prices["ticker"].astype(str))


def test_reconcile_registers_repaired_assets_and_writes_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch)
    prices_path = tmp_path / "data" / "prices_daily.csv"

    run = run_price_cache_reconcile(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    prices = pd.read_csv(prices_path)
    assert run.payload["metadata"]["status"] in {"OK", "LIMITED"}
    assert run.json_path is not None and run.json_path.exists()
    assert run.markdown_path is not None and run.markdown_path.exists()
    assert run.registry_path.exists()
    assert run.payload["safety"]["fake_price_rows_generated"] is False
    assert run.payload["safety"]["production_parameters_modified"] is False
    assert {
        "GOOGL",
        "BRK.B",
        "SGOV",
    }.issubset(set(prices["ticker"].astype(str)))
    brk = prices.loc[prices["ticker"].astype(str) == "BRK.B"].iloc[0]
    assert brk["source_symbol"] == "BRK-B"
    assert run.payload["after"]["latest_resolution"] == "OK"


def test_reconcile_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch)
    kwargs = {
        "as_of": fixture["as_of"],
        "config_path": fixture["config_path"],
        "output_root": fixture["output_dir"],
        "generated_at": datetime(2026, 1, 20, tzinfo=UTC),
    }

    first = run_price_cache_reconcile(**kwargs)
    row_count = len(pd.read_csv(tmp_path / "data" / "prices_daily.csv"))
    second = run_price_cache_reconcile(**kwargs)

    assert first.payload["metadata"]["status"] in {"OK", "LIMITED"}
    assert second.payload["metadata"]["status"] in {"OK", "LIMITED", "NOT_REQUIRED"}
    assert len(pd.read_csv(tmp_path / "data" / "prices_daily.csv")) == row_count
    assert "remains blocked" not in second.payload["impact_on_portfolio_sensitivity"]["summary"]
    assert "remains blocked" not in second.payload["impact_on_shadow_backtest"]["summary"]


def test_reconcile_fails_closed_when_repaired_artifact_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fixture = _reconcile_fixture(tmp_path, monkeypatch, write_external_cache=False)

    run = run_price_cache_reconcile(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_dir"],
        generated_at=datetime(2026, 1, 20, tzinfo=UTC),
    )

    assert run.payload["metadata"]["status"] == "FAILED"
    inspection = {
        item["canonical_symbol"]: item for item in run.payload["repaired_artifact_inspection"]
    }
    assert inspection["GOOGL"]["error_code"] == "REPAIRED_ARTIFACT_MISSING"
    assert run.payload["safety"]["fake_price_rows_generated"] is False


def _reconcile_fixture(
    tmp_path: Path,
    monkeypatch,
    *,
    write_external_cache: bool = True,
) -> dict[str, object]:
    fixture = _write_shadow_backtest_fixture(tmp_path, days=20, min_history_days=8)
    assets = ("QQQ", "NVDA", "GOOGL", "BRK.B", "SGOV")
    _write_baseline_assets(Path(fixture["baseline_path"]), assets)
    _write_backtest_manifest(
        output_root=Path(fixture["output_dir"]),
        as_of=fixture["as_of"],
        prices_path=tmp_path / "data" / "prices_daily.csv",
        assets=assets,
    )
    if write_external_cache:
        start = fixture["as_of"] - timedelta(days=9)
        for symbol, source_symbol in (
            ("GOOGL", "GOOGL"),
            ("BRK.B", "BRK-B"),
            ("SGOV", "SGOV"),
        ):
            _write_fmp_cache(
                tmp_path,
                canonical_symbol=symbol,
                source_symbol=source_symbol,
                start=start,
                end=fixture["as_of"],
            )
    monkeypatch.setattr(reconcile_module, "PROJECT_ROOT", tmp_path)
    return fixture


def _write_baseline_assets(path: Path, assets: tuple[str, ...]) -> None:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    payload["asset_universe"] = {"core": [*assets, "CASH"]}
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_backtest_manifest(
    *,
    output_root: Path,
    as_of: date,
    prices_path: Path,
    assets: tuple[str, ...],
) -> Path:
    manifest_path = (
        output_root / "backtest_snapshots" / as_of.isoformat() / "backtest_input_manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "report_type": "backtest_input_manifest",
        "snapshot_id": f"backtest-input-{as_of.isoformat()}",
        "generated_at": "2026-01-20T00:00:00+00:00",
        "status": "LIMITED",
        "production_effect": "none",
        "assets": list(assets),
        "symbol_mapping": {"BRK.B": {"canonical_symbol": "BRK.B", "source_symbol": "BRK-B"}},
        "date_range": {
            "start": (as_of - timedelta(days=9)).isoformat(),
            "end": as_of.isoformat(),
        },
        "price_data_files": [str(prices_path)],
        "signal_snapshot_files": [],
        "data_quality_report": str(
            output_root / "data_quality" / as_of.isoformat() / "backtest_input_diagnostics.json"
        ),
        "config_hash": "test",
        "code_version": "test",
    }
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path


def _write_fmp_cache(
    tmp_path: Path,
    *,
    canonical_symbol: str,
    source_symbol: str,
    start: date,
    end: date,
) -> None:
    rows = []
    current = start
    offset = 0
    while current <= end:
        rows.append(
            {
                "symbol": source_symbol,
                "date": current.isoformat(),
                "adjOpen": 100.0 + offset,
                "adjHigh": 101.0 + offset,
                "adjLow": 99.0 + offset,
                "adjClose": 100.5 + offset,
                "volume": 1_000_000 + offset,
            }
        )
        current += timedelta(days=1)
        offset += 1
    body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    key = sha256(f"{canonical_symbol}-{source_symbol}".encode()).hexdigest()
    cache_dir = (
        tmp_path
        / "data"
        / "raw"
        / "external_request_cache"
        / "Financial_Modeling_Prep"
        / "eod_daily_prices"
        / key[:2]
        / key
    )
    cache_dir.mkdir(parents=True, exist_ok=True)
    body_path = cache_dir / "response.body"
    body_path.write_bytes(body)
    metadata = {
        "api_family": "eod_daily_prices",
        "body_path": str(body_path),
        "body_sha256": sha256(body).hexdigest(),
        "created_at": "2026-01-20T00:00:00+00:00",
        "endpoint": "https://financialmodelingprep.com/stable/historical-price-eod/dividend-adjusted",
        "provider": "Financial Modeling Prep",
        "request_identity": {
            "params": {
                "symbol": source_symbol,
                "from": start.isoformat(),
                "to": end.isoformat(),
                "apikey": "***",
            }
        },
        "status_code": 200,
    }
    (cache_dir / "metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
