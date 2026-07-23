from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.data.market_data import PriceRequest
from ai_trading_system.trading_engine.data.price_history_repair import (
    repair_backtest_price_history,
    source_symbol_for_price_repair,
)

GENERATED_AT = datetime(2026, 5, 29, tzinfo=UTC)
SIGNALS = (
    "macro_liquidity",
    "trend_momentum",
    "sector_strength",
    "earnings_quality",
    "valuation_risk",
    "event_risk",
)


class FakePriceProvider:
    def __init__(
        self,
        *,
        failing_symbols: tuple[str, ...] = (),
        omit_last_for: tuple[str, ...] = (),
    ) -> None:
        self.failing_symbols = set(failing_symbols)
        self.omit_last_for = set(omit_last_for)
        self.requested_symbols: list[str] = []

    def download_prices(self, request: PriceRequest) -> pd.DataFrame:
        source_symbol = request.tickers[0]
        self.requested_symbols.append(source_symbol)
        if source_symbol in self.failing_symbols:
            raise RuntimeError(f"{source_symbol} provider failure")
        rows: list[dict[str, object]] = []
        day_count = (request.end - request.start).days + 1
        for offset in range(day_count):
            if source_symbol in self.omit_last_for and offset == day_count - 1:
                continue
            current = request.start + timedelta(days=offset)
            close = 100.0 + offset
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": source_symbol,
                    "open": close - 0.5,
                    "high": close + 0.5,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000 + offset,
                }
            )
        return pd.DataFrame(rows)


def test_repairs_missing_googl_price_history(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, required_assets=("QQQ", "GOOGL"), missing=("GOOGL",))
    provider = FakePriceProvider()

    run = repair_backtest_price_history(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        symbols=("GOOGL",),
        price_provider=provider,
        price_only=True,
        generated_at=GENERATED_AT,
    )

    assert run.status == "REPAIRED"
    assert run.asset_results[0].status == "REPAIRED"
    assert provider.requested_symbols == ["GOOGL"]
    prices = pd.read_csv(fixture["prices_path"])
    assert "GOOGL" in set(prices["ticker"])
    assert run.final_diagnostics.payload["summary"]["price_data_status"] == "OK"
    assert run.final_diagnostics.payload["summary"]["asset_coverage_status"] == "OK"


def test_repairs_brk_b_with_source_symbol_mapping(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, required_assets=("QQQ", "BRK.B"), missing=("BRK.B",))
    provider = FakePriceProvider()

    run = repair_backtest_price_history(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        symbols=("BRK.B",),
        price_provider=provider,
        price_only=True,
        generated_at=GENERATED_AT,
    )

    assert source_symbol_for_price_repair("BRK.B") == "BRK-B"
    assert provider.requested_symbols == ["BRK-B"]
    assert run.symbol_mapping == {"BRK.B": {"source_symbol": "BRK-B", "canonical_symbol": "BRK.B"}}
    prices = pd.read_csv(fixture["prices_path"])
    repaired_rows = prices.loc[prices["ticker"] == "BRK.B"]
    assert set(repaired_rows["source_symbol"]) == {"BRK-B"}
    manifest = pd.read_csv(fixture["manifest_path"])
    request_parameters = json.loads(str(manifest.iloc[-1]["request_parameters"]))
    assert request_parameters["symbol_mapping"] == run.symbol_mapping


def test_repairs_missing_sgov_price_history(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, required_assets=("QQQ", "SGOV"), missing=("SGOV",))
    provider = FakePriceProvider()

    run = repair_backtest_price_history(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        symbols=("SGOV",),
        price_provider=provider,
        price_only=True,
        generated_at=GENERATED_AT,
    )

    assert run.status == "REPAIRED"
    assert provider.requested_symbols == ["SGOV"]
    prices = pd.read_csv(fixture["prices_path"])
    assert "SGOV" in set(prices["ticker"])


def test_repair_is_idempotent_for_existing_symbol_dates(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, required_assets=("QQQ", "GOOGL"), missing=("GOOGL",))
    provider = FakePriceProvider()

    for _ in range(2):
        repair_backtest_price_history(
            as_of=fixture["as_of"],
            config_path=fixture["config_path"],
            output_root=fixture["output_root"],
            symbols=("GOOGL",),
            price_provider=provider,
            price_only=True,
            generated_at=GENERATED_AT,
        )

    prices = pd.read_csv(fixture["prices_path"])
    duplicate_count = prices.duplicated(subset=["date", "ticker"]).sum()
    googl_rows = prices.loc[prices["ticker"] == "GOOGL"]
    assert duplicate_count == 0
    assert len(googl_rows) == fixture["repair_day_count"]


def test_repair_reports_partial_failure_without_crashing(tmp_path: Path) -> None:
    fixture = _write_fixture(
        tmp_path,
        required_assets=("QQQ", "GOOGL", "BRK.B", "SGOV"),
        missing=("GOOGL", "BRK.B", "SGOV"),
    )
    provider = FakePriceProvider(failing_symbols=("SGOV",))

    run = repair_backtest_price_history(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        symbols=("GOOGL", "BRK.B", "SGOV"),
        price_provider=provider,
        price_only=True,
        generated_at=GENERATED_AT,
    )

    statuses = {result.symbol: result.status for result in run.asset_results}
    assert run.status == "PARTIAL"
    assert statuses == {"GOOGL": "REPAIRED", "BRK.B": "REPAIRED", "SGOV": "FAILED"}
    assert run.final_diagnostics.payload["summary"]["overall_status"] == "FAILED"


def test_repair_asset_result_reports_missing_dates(tmp_path: Path) -> None:
    fixture = _write_fixture(tmp_path, required_assets=("QQQ", "GOOGL"), missing=("GOOGL",))
    provider = FakePriceProvider(omit_last_for=("GOOGL",))

    run = repair_backtest_price_history(
        as_of=fixture["as_of"],
        config_path=fixture["config_path"],
        output_root=fixture["output_root"],
        symbols=("GOOGL",),
        price_provider=provider,
        price_only=True,
        generated_at=GENERATED_AT,
    )

    result = run.asset_results[0]
    assert result.status == "REPAIRED"
    assert result.missing_date_count == 1
    assert result.missing_dates_sample == (fixture["as_of"].isoformat(),)


def _write_fixture(
    tmp_path: Path,
    *,
    required_assets: tuple[str, ...],
    missing: tuple[str, ...],
    write_signal_snapshots: bool = True,
) -> dict[str, object]:
    as_of = date(2026, 5, 29)
    day_count = 8
    start = as_of - timedelta(days=day_count - 1)
    dates = tuple(start + timedelta(days=offset) for offset in range(day_count))
    data_dir = tmp_path / "data" / "raw"
    config_dir = tmp_path / "config"
    output_root = tmp_path / "artifacts"
    prices_path = data_dir / "prices_daily.csv"
    rates_path = data_dir / "rates_daily.csv"
    secondary_prices_path = data_dir / "prices_marketstack_daily.csv"
    manifest_path = data_dir / "download_manifest.csv"
    baseline_path = config_dir / "parameters" / "production" / "current.yaml"
    promotion_path = config_dir / "parameters" / "promotion" / "promotion_rules.yaml"
    config_path = config_dir / "parameters" / "shadow" / "shadow_backtest.yaml"
    signal_dir = output_root / "signal_snapshots" / as_of.isoformat()

    _write_prices(prices_path, dates, required_assets, missing)
    _write_prices(secondary_prices_path, dates, required_assets, missing)
    _write_rates(rates_path, dates)
    _write_manifest(manifest_path, [prices_path, rates_path, secondary_prices_path])
    _write_yaml(baseline_path, _baseline_payload(required_assets))
    _write_yaml(promotion_path, {"version": "promotion-test"})
    if write_signal_snapshots:
        signal_dir.mkdir(parents=True, exist_ok=True)
        for signal in SIGNALS:
            (signal_dir / f"{signal}.json").write_text("{}", encoding="utf-8")
    _write_yaml(
        config_path,
        _shadow_config_payload(
            prices_path=prices_path,
            rates_path=rates_path,
            secondary_prices_path=secondary_prices_path,
            manifest_path=manifest_path,
            baseline_path=baseline_path,
            promotion_path=promotion_path,
            output_root=output_root,
            signal_dir=signal_dir.parent,
            min_history_days=5,
        ),
    )
    return {
        "as_of": as_of,
        "config_path": config_path,
        "prices_path": prices_path,
        "manifest_path": manifest_path,
        "output_root": output_root,
        "day_count": day_count,
        "repair_day_count": 5,
    }


def _write_prices(
    path: Path,
    dates: tuple[date, ...],
    required_assets: tuple[str, ...],
    missing: tuple[str, ...],
) -> None:
    rows: list[dict[str, object]] = []
    for offset, current in enumerate(dates):
        for asset_index, asset in enumerate(required_assets):
            if asset in missing:
                continue
            close = 100.0 + asset_index + offset
            rows.append(
                {
                    "date": current.isoformat(),
                    "ticker": asset,
                    "open": close - 0.5,
                    "high": close + 0.5,
                    "low": close - 1.0,
                    "close": close,
                    "adj_close": close,
                    "volume": 1000 + offset,
                }
            )
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_rates(path: Path, dates: tuple[date, ...]) -> None:
    rows = [
        {"date": current.isoformat(), "series": series, "value": 4.0}
        for current in dates
        for series in ("DGS2", "DGS10", "DTWEXBGS")
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_manifest(path: Path, files: list[Path]) -> None:
    rows = [
        {
            "downloaded_at": GENERATED_AT.isoformat(),
            "source_id": file_path.stem,
            "provider": "unit_test",
            "endpoint": "fixture",
            "request_parameters": "{}",
            "output_path": str(file_path),
            "row_count": len(pd.read_csv(file_path)),
            "checksum_sha256": "test",
        }
        for file_path in files
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_yaml(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _baseline_payload(required_assets: tuple[str, ...]) -> dict[str, object]:
    return {
        "version": "production-test",
        "created_at": GENERATED_AT.isoformat(),
        "owner": "tests",
        "status": "pilot_baseline",
        "production_effect": "production",
        "rationale": "unit test baseline",
        "asset_universe": {"core": [*required_assets, "CASH"]},
        "decision_frequency": "daily",
        "rebalance_frequency": "weekly",
        "risk_profile": "balanced_growth",
        "weights": {
            "macro_liquidity": 0.20,
            "trend_momentum": 0.25,
            "sector_strength": 0.20,
            "earnings_quality": 0.15,
            "valuation_risk": 0.10,
            "event_risk": 0.10,
        },
        "position_limits": {
            "max_single_asset_weight": 0.30,
            "max_sector_weight": 0.60,
            "min_cash_weight": 0.05,
        },
    }


def _shadow_config_payload(
    *,
    prices_path: Path,
    rates_path: Path,
    secondary_prices_path: Path,
    manifest_path: Path,
    baseline_path: Path,
    promotion_path: Path,
    output_root: Path,
    signal_dir: Path,
    min_history_days: int,
) -> dict[str, object]:
    return {
        "version": "shadow-test",
        "owner": "tests",
        "status": "pilot",
        "production_effect": "none",
        "manual_review_required": True,
        "auto_promotion": False,
        "observe_only": True,
        "rationale": "test",
        "intended_effect": "test",
        "validation_evidence": "tests",
        "review_condition": "test",
        "market_regime": {
            "id": "ai_after_chatgpt",
            "anchor_event": "ChatGPT public launch",
            "anchor_date": "2022-11-30",
            "default_backtest_start": "2022-12-01",
        },
        "data": {
            "prices_path": str(prices_path),
            "rates_path": str(rates_path),
            "download_manifest_path": str(manifest_path),
            "secondary_prices_path": str(secondary_prices_path),
            "data_quality_report_dir": str(output_root / "reports"),
            "signal_snapshot_dir": str(signal_dir),
        },
        "baseline_parameters_path": str(baseline_path),
        "promotion_rules_path": str(promotion_path),
        "output": {
            "shadow_backtest_dir": str(output_root / "shadow_backtest"),
            "shadow_parameters_dir": str(output_root / "shadow_parameters"),
            "candidate_parameters_dir": str(output_root / "candidate_parameters"),
            "parameter_promotion_dir": str(output_root / "parameter_promotion"),
            "report_alias_dir": str(output_root / "reports"),
        },
        "walk_forward": {
            "train_window_days": 3,
            "validation_window_days": 2,
            "step_days": 2,
            "min_history_days": min_history_days,
        },
        "backtest_frequency": "daily",
        "rebalance_frequency": "weekly",
        "signal_evaluation_frequency": "daily",
        "transaction_cost": {
            "commission_bps": 1,
            "slippage_bps": 5,
            "fx_cost_bps": 0,
            "tax_model": "ignored_for_test",
        },
        "search": {
            "algorithm": "bounded_grid",
            "max_candidates": 8,
            "hard_gate_tuning": {"enabled": False, "reason": "test"},
            "search_space": {
                "macro_liquidity": {"min": 0.20, "max": 0.20, "step": 0.05},
                "trend_momentum": {"min": 0.25, "max": 0.25, "step": 0.05},
                "sector_strength": {"min": 0.20, "max": 0.20, "step": 0.05},
                "earnings_quality": {"min": 0.15, "max": 0.15, "step": 0.05},
                "valuation_risk": {"min": 0.10, "max": 0.10, "step": 0.05},
                "event_risk": {"min": 0.10, "max": 0.10, "step": 0.05},
            },
            "constraints": {
                "total_weight_sum": 1.0,
                "max_single_weight": 0.35,
                "min_single_weight": 0.05,
                "max_daily_parameter_delta": 0.05,
                "max_weekly_parameter_delta": 0.10,
            },
            "parameter_change_guardrails": {
                "max_abs_change_per_weight": 0.10,
                "max_total_l1_change": 0.30,
                "require_reason_for_each_change": True,
            },
        },
        "data_quality_rules": {
            "insufficient_history": {"min_days": min_history_days, "status": "INSUFFICIENT_DATA"},
            "missing_price_data": {"max_missing_ratio": 0.02, "status": "LIMITED"},
            "missing_required_asset": {"status": "FAILED"},
            "missing_signal_snapshot": {"status": "LIMITED"},
            "cache_freshness": {
                "max_age_days": {"price_data": 3, "signal_snapshot": 3, "macro_data": 7}
            },
        },
        "point_in_time_status": {
            "price_data": "OK",
            "fundamental_data": "LIMITED",
            "news_data": "NOT_AVAILABLE",
            "macro_data": "LIMITED",
        },
    }
