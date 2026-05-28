from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.market_panel import (
    MISSING_MARKET_PRICE_DATA,
    build_market_panel_payload,
    render_market_panel_markdown,
)


def test_market_panel_payload_covers_required_proxy_schema(tmp_path: Path) -> None:
    prices_path = _write_market_prices(tmp_path / "prices_daily.csv")
    rates_path = _write_rates(tmp_path / "rates_daily.csv")

    payload = build_market_panel_payload(
        as_of=date(2026, 5, 22),
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_status="PASS",
        data_quality_report_path=tmp_path / "data_quality_2026-05-22.md",
    )
    markdown = render_market_panel_markdown(payload)

    assert payload["report_type"] == "market_panel"
    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["data_quality"]["status"] == "PASS"
    assert payload["summary"]["available_proxy_count"] == 6
    assert "SPY 1D=" in payload["summary"]["market_movement_sentence"]
    for row in payload["proxies"]:
        assert {
            "symbol",
            "role",
            "last_price",
            "return_1d",
            "return_5d",
            "return_20d",
            "trend_label",
            "risk_interpretation",
            "data_status",
            "source_artifact",
            "production_effect",
        } <= set(row)
        assert row["production_effect"] == "none"
    smh = next(row for row in payload["proxies"] if row["symbol"] == "SMH")
    assert smh["role"] == "ai_sector_proxy"
    assert smh["data_status"] == "AVAILABLE"
    assert smh["return_1d"] > 0
    assert "production_effect" in markdown
    assert "不生成交易指令" in markdown


def test_market_panel_missing_data_degrades_without_fabricating_moves(tmp_path: Path) -> None:
    payload = build_market_panel_payload(
        as_of=date(2026, 5, 22),
        prices_path=tmp_path / "missing_prices_daily.csv",
        rates_path=tmp_path / "missing_rates_daily.csv",
    )

    assert payload["status"] == MISSING_MARKET_PRICE_DATA
    assert payload["summary"]["available_proxy_count"] == 0
    assert all(row["last_price"] is None for row in payload["proxies"])
    assert all(row["return_1d"] is None for row in payload["proxies"])
    assert all(row["data_status"] == MISSING_MARKET_PRICE_DATA for row in payload["proxies"])


def test_market_panel_skips_cached_reads_when_quality_gate_failed(tmp_path: Path) -> None:
    prices_path = _write_market_prices(tmp_path / "prices_daily.csv")
    rates_path = _write_rates(tmp_path / "rates_daily.csv")

    payload = build_market_panel_payload(
        as_of=date(2026, 5, 22),
        prices_path=prices_path,
        rates_path=rates_path,
        data_quality_status="FAIL",
        data_quality_report_path=tmp_path / "data_quality_2026-05-22.md",
        read_cached_data=False,
    )

    assert payload["status"] == MISSING_MARKET_PRICE_DATA
    assert payload["data_quality"]["status"] == "FAIL"
    assert all(row["last_price"] is None for row in payload["proxies"])
    assert all(row["return_1d"] is None for row in payload["proxies"])
    assert "market_panel_cache_not_read:data_quality_failed" in payload["warnings"]


def test_reports_market_panel_cli_writes_markdown_and_json(tmp_path: Path) -> None:
    prices_path = _write_market_prices(tmp_path / "prices_daily.csv")
    rates_path = _write_rates(tmp_path / "rates_daily.csv")
    markdown_path = tmp_path / "market_panel_2026-05-22.md"
    json_path = tmp_path / "market_panel_2026-05-22.json"
    data_quality_path = tmp_path / "data_quality_2026-05-22.md"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "market-panel",
            "--date",
            "2026-05-22",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--output-path",
            str(markdown_path),
            "--json-output-path",
            str(json_path),
            "--data-quality-output-path",
            str(data_quality_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Market panel：PASS" in result.output
    assert markdown_path.exists()
    assert json_path.exists()
    assert data_quality_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["production_effect"] == "none"
    assert payload["source_artifacts"]["prices_daily"]["exists"] is True


def test_reports_market_panel_cli_emits_degraded_artifact_on_quality_failure(
    tmp_path: Path,
) -> None:
    markdown_path = tmp_path / "market_panel_2026-05-22.md"
    json_path = tmp_path / "market_panel_2026-05-22.json"
    data_quality_path = tmp_path / "data_quality_2026-05-22.md"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "market-panel",
            "--date",
            "2026-05-22",
            "--prices-path",
            str(tmp_path / "missing_prices_daily.csv"),
            "--rates-path",
            str(tmp_path / "missing_rates_daily.csv"),
            "--output-path",
            str(markdown_path),
            "--json-output-path",
            str(json_path),
            "--data-quality-output-path",
            str(data_quality_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 1
    assert "Market panel 数据质量状态：FAIL" in result.output
    assert markdown_path.exists()
    assert json_path.exists()
    assert data_quality_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["status"] == MISSING_MARKET_PRICE_DATA
    assert payload["data_quality"]["status"] == "FAIL"
    assert all(row["return_1d"] is None for row in payload["proxies"])


def _write_market_prices(path: Path) -> Path:
    symbols = {
        "SPY": 100.0,
        "QQQ": 200.0,
        "SMH": 150.0,
        "SOXX": 120.0,
        "^VIX": 30.0,
    }
    rows = ["date,ticker,open,high,low,close,adj_close,volume"]
    start = date(2026, 5, 1)
    for offset in range(22):
        current_date = start + timedelta(days=offset)
        for symbol, base in symbols.items():
            value = base + offset if symbol != "^VIX" else base - (offset * 0.1)
            volume = "" if symbol == "^VIX" else "1000"
            rows.append(
                f"{current_date.isoformat()},{symbol},{value},{value + 1},"
                f"{value - 1},{value},{value},{volume}"
            )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _write_rates(path: Path) -> Path:
    rows = ["date,series,value"]
    start = date(2026, 5, 1)
    for offset in range(22):
        current_date = start + timedelta(days=offset)
        rows.append(f"{current_date.isoformat()},DGS10,{4.0 + offset * 0.01}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path
