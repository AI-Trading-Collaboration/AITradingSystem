from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands import data_cache as data_cache_cli
from ai_trading_system.config import PROJECT_ROOT as REAL_PROJECT_ROOT
from ai_trading_system.config import configured_price_tickers, load_data_quality, load_universe
from ai_trading_system.data.quality import (
    DataQualityReport,
    Severity,
    render_data_quality_report,
    validate_data_cache,
    write_data_quality_report,
)


def test_validate_data_cache_passes_clean_data(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert report.status == "PASS"
    assert report.error_count == 0
    assert report.price_summary.rows == 4
    assert report.price_summary.sha256 is not None


def test_validate_data_cache_fails_duplicate_price_keys(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    prices = pd.concat([prices, prices.iloc[[0]]], ignore_index=True)
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert _issue_codes(report) == {"prices_duplicate_keys"}


def test_validate_data_cache_fails_invalid_ohlc(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    prices.loc[0, "high"] = 90
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "prices_invalid_ohlc" in _issue_codes(report)


def test_validate_data_cache_fails_missing_expected_ticker(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA", "AMD"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "prices_missing_expected_values" in _issue_codes(report)


def test_validate_data_cache_flags_suspicious_price_move(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    prices.loc[(prices["ticker"] == "NVDA") & (prices["date"] == "2026-04-30"), "adj_close"] = 145
    prices.loc[(prices["ticker"] == "NVDA") & (prices["date"] == "2026-04-30"), "close"] = 145
    prices.loc[(prices["ticker"] == "NVDA") & (prices["date"] == "2026-04-30"), "high"] = 146
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert any(
        issue.severity is Severity.INFO and issue.code == "prices_suspicious_adj_close_move"
        for issue in report.issues
    )


def test_validate_data_cache_fails_extreme_stock_price_move(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    move = (prices["ticker"] == "NVDA") & (prices["date"] == "2026-04-30")
    prices.loc[move, ["open", "high", "low", "close", "adj_close"]] = [210, 212, 208, 210, 210]
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "prices_extreme_adj_close_move" in _issue_codes(report)


def test_validate_data_cache_ignores_price_moves_before_consistency_window(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    move = (prices["ticker"] == "NVDA") & (prices["date"] == "2026-04-30")
    prices.loc[move, ["open", "high", "low", "close", "adj_close"]] = [210, 212, 208, 210, 210]
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=_quality_config_with_consistency_start(date(2026, 5, 1)),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "prices_extreme_adj_close_move" not in _issue_codes(report)


def test_validate_data_cache_uses_ticker_return_threshold_overrides(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path, tickers=["^VIX"])
    prices = pd.read_csv(prices_path)
    move = (prices["ticker"] == "^VIX") & (prices["date"] == "2026-04-30")
    prices.loc[move, ["open", "high", "low", "close", "adj_close"]] = [220, 230, 215, 220, 220]
    prices.to_csv(prices_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["^VIX"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "prices_extreme_adj_close_move" not in _issue_codes(report)
    assert any(
        issue.severity is Severity.INFO and issue.code == "prices_suspicious_adj_close_move"
        for issue in report.issues
    )


def test_validate_data_cache_uses_fred_series_change_overrides(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    rates = pd.read_csv(rates_path)
    rates.loc[
        (rates["series"] == "DTWEXBGS") & (rates["date"] == "2026-04-30"),
        "value",
    ] = 122.5
    rates.to_csv(rates_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10", "DTWEXBGS"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "rates_extreme_daily_change" not in _issue_codes(report)
    assert "rates_suspicious_daily_change" in _issue_codes(report)


def test_validate_data_cache_ignores_rate_moves_before_consistency_window(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    rates = pd.read_csv(rates_path)
    rates.loc[
        (rates["series"] == "DTWEXBGS") & (rates["date"] == "2026-04-30"),
        "value",
    ] = 122.5
    rates.to_csv(rates_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10", "DTWEXBGS"],
        quality_config=_quality_config_with_consistency_start(date(2026, 5, 1)),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "rates_suspicious_daily_change" not in _issue_codes(report)


def test_validate_data_cache_fails_stale_data(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 6, 1),
    )

    assert report.passed is False
    assert "prices_stale" in _issue_codes(report)
    assert "rates_stale" in _issue_codes(report)


def test_validate_data_cache_uses_rate_series_staleness_override(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    prices["date"] = prices["date"].replace(
        {
            "2026-04-29": "2026-05-07",
            "2026-04-30": "2026-05-08",
        }
    )
    prices.to_csv(prices_path, index=False)

    rates = pd.read_csv(rates_path)
    rates["date"] = rates.apply(
        lambda row: _rate_date_for_series(str(row["series"]), str(row["date"])),
        axis=1,
    )
    rates.to_csv(rates_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10", "DTWEXBGS"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 9),
    )

    assert report.passed is True
    assert "rates_stale" not in _issue_codes(report)


def test_render_and_write_data_quality_report(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
    )

    markdown = render_data_quality_report(report)
    output_path = write_data_quality_report(report, tmp_path / "report.md")

    assert "- 状态：PASS" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_validate_data_cache_checks_download_manifest(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    manifest_path = tmp_path / "download_manifest.csv"
    pd.DataFrame(
        [
            _manifest_row("prices", prices_path),
            _manifest_row("rates", rates_path),
        ]
    ).to_csv(manifest_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
    )
    markdown = render_data_quality_report(report)

    assert report.status == "PASS"
    assert report.manifest_summary is not None
    assert report.manifest_summary.rows == 2
    assert "下载审计清单" in markdown
    assert "价格一致性检查起点：2021-02-22" in markdown
    assert "宏观变化检查起点：2021-02-22" in markdown


def test_validate_data_cache_ignores_stale_reconstructed_manifest_rows(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    old_prices_path = tmp_path / "old_prices_daily.csv"
    old_prices_path.write_text(
        "date,ticker,open,high,low,close,adj_close,volume\n",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "download_manifest.csv"
    reconstructed = _manifest_row("reconstructed_prices", old_prices_path)
    reconstructed.update(
        {
            "provider": "cache_rebuild_from_existing_file",
            "request_parameters": (
                '{"mode":"reconstruct_from_existing_cache",'
                '"provenance_status":"RECONSTRUCTED_MANIFEST"}'
            ),
        }
    )
    pd.DataFrame(
        [
            reconstructed,
            _manifest_row("prices", prices_path),
            _manifest_row("rates", rates_path),
        ]
    ).to_csv(manifest_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
    )

    assert report.status == "PASS"
    assert "download_manifest_provenance_reconstructed" not in _issue_codes(report)


def test_validate_data_cache_warns_when_current_manifest_row_is_reconstructed(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    manifest_path = tmp_path / "download_manifest.csv"
    reconstructed = _manifest_row("reconstructed_prices", prices_path)
    reconstructed.update(
        {
            "provider": "cache_rebuild_from_existing_file",
            "request_parameters": (
                '{"mode":"reconstruct_from_existing_cache",'
                '"provenance_status":"RECONSTRUCTED_MANIFEST"}'
            ),
        }
    )
    pd.DataFrame(
        [
            reconstructed,
            _manifest_row("rates", rates_path),
        ]
    ).to_csv(manifest_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        manifest_path=manifest_path,
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert "download_manifest_provenance_reconstructed" in _issue_codes(report)


def test_validate_data_cache_checks_secondary_price_source(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    pd.read_csv(prices_path).to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )
    markdown = render_data_quality_report(report)

    assert report.status == "PASS"
    assert report.secondary_price_summary is not None
    assert report.secondary_price_summary.rows == 4
    assert "第二行情源 Marketstack" in markdown


def test_validate_data_cache_fails_secondary_price_mismatch(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        ["open", "high", "low", "close", "adj_close"],
    ] = [1.0, 1.0, 1.0, 1.0, 1.0]
    secondary.to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    assert report.passed is False
    assert "secondary_prices_adj_close_mismatch" in _issue_codes(report)
    assert _issue_by_code(report, "secondary_prices_adj_close_mismatch").source == (
        "跨源核验：主价格源 vs Marketstack"
    )


def test_validate_data_cache_ignores_secondary_mismatch_before_consistency_window(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        ["open", "high", "low", "close", "adj_close"],
    ] = [1.0, 1.0, 1.0, 1.0, 1.0]
    secondary.to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=_quality_config_with_consistency_start(date(2026, 5, 1)),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    assert report.passed is True
    assert "secondary_prices_adj_close_mismatch" not in _issue_codes(report)


def test_validate_data_cache_labels_secondary_self_check_source(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        "close",
    ] = 0.0
    secondary.to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=_quality_config_with_secondary_overlap_threshold(0.50),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )
    issue = _issue_by_code(report, "prices_non_positive_close")
    markdown = render_data_quality_report(report)

    assert report.passed is True
    assert report.status == "PASS"
    assert issue.severity is Severity.INFO
    assert issue.source == "第二行情源 Marketstack"
    assert "| 信息 | 第二行情源 Marketstack | prices_non_positive_close |" in markdown


def test_validate_data_cache_can_fail_closed_on_secondary_self_check_when_configured(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = pd.read_csv(prices_path)
    secondary.loc[
        (secondary["ticker"] == "NVDA") & (secondary["date"] == "2026-04-30"),
        "close",
    ] = 0.0
    secondary.to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=_quality_config_with_secondary_fail_closed(),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )
    issue = _issue_by_code(report, "prices_non_positive_close")

    assert report.passed is False
    assert issue.severity is Severity.ERROR


def test_validate_data_cache_warns_when_secondary_adjustment_basis_differs(
    tmp_path: Path,
) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)
    prices = pd.read_csv(prices_path)
    prices["adj_close"] = prices["close"] * 0.90
    prices.to_csv(prices_path, index=False)

    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    secondary = prices.copy()
    secondary["adj_close"] = secondary["close"]
    secondary.to_csv(secondary_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    assert report.passed is True
    assert "secondary_prices_adjustment_basis_warning" in _issue_codes(report)
    assert "secondary_prices_adj_close_mismatch" not in _issue_codes(report)
    assert report.marketstack_reconciliation_records


def test_validate_data_cache_downgrades_known_split_raw_close_basis_diff(
    tmp_path: Path,
) -> None:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    secondary_path = tmp_path / "prices_marketstack_daily.csv"
    pd.DataFrame(
        [
            _price_row("2025-12-17", "NOW", 785.00, 809.65, 781.45, 782.40, 156.48, 11156780),
            _price_row("2025-12-18", "NOW", 156.51, 157.78, 151.15, 153.38, 153.38, 9644020),
        ]
    ).to_csv(prices_path, index=False)
    pd.DataFrame(
        [
            _price_row("2025-12-17", "NOW", 157.04, 161.93, 156.29, 156.48, 156.48, 11156780),
            _price_row("2025-12-18", "NOW", 156.51, 157.78, 151.15, 153.38, 153.38, 9644020),
        ]
    ).to_csv(secondary_path, index=False)
    pd.DataFrame(
        [
            {"date": "2025-12-17", "series": "DGS2", "value": 3.6},
            {"date": "2025-12-18", "series": "DGS2", "value": 3.6},
            {"date": "2025-12-17", "series": "DGS10", "value": 4.1},
            {"date": "2025-12-18", "series": "DGS10", "value": 4.1},
            {"date": "2025-12-17", "series": "DTWEXBGS", "value": 121.0},
            {"date": "2025-12-18", "series": "DTWEXBGS", "value": 121.1},
        ]
    ).to_csv(rates_path, index=False)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["NOW"],
        expected_rate_series=["DGS2", "DGS10", "DTWEXBGS"],
        quality_config=load_data_quality(),
        as_of=date(2025, 12, 20),
        secondary_prices_path=secondary_path,
        require_secondary_prices=True,
    )

    assert report.passed is True
    assert "secondary_prices_known_split_close_basis" in _issue_codes(report)
    assert "secondary_prices_close_mismatch" not in _issue_codes(report)
    assert any(
        record.classification == "known_split_raw_close_basis_difference"
        for record in report.marketstack_reconciliation_records
    )


def test_validate_data_cache_fails_missing_required_secondary_source(tmp_path: Path) -> None:
    prices_path, rates_path = _write_valid_cache(tmp_path)

    report = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["MSFT", "NVDA"],
        expected_rate_series=["DGS2", "DGS10"],
        quality_config=load_data_quality(),
        as_of=date(2026, 5, 2),
        secondary_prices_path=tmp_path / "prices_marketstack_daily.csv",
        require_secondary_prices=True,
    )

    assert report.passed is False
    assert "secondary_prices_file_missing" in _issue_codes(report)


def test_validate_data_cli_writes_report(tmp_path: Path, monkeypatch) -> None:
    prices_path, rates_path = _write_valid_cache(
        tmp_path,
        tickers=configured_price_tickers(load_universe()),
    )
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)
    output_path = tmp_path / "quality.md"

    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
        terminal_width=240,
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "数据质量状态：PASS" in result.output
    assert "Canonicalreceipt" in "".join(result.output.split())
    assert "DQ discovery pointer：未发布" in result.output
    assert list((tmp_path / "outputs/data_quality/executions").glob("*/receipt.json"))
    audit_path = next(
        (tmp_path / "artifacts/data_refresh_audit/validation").glob("validate_data_*.json")
    )
    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    assert audit["data_quality_execution"]["receipt_id"].startswith("dq_execution_")
    assert audit["data_quality_execution"]["projection_only"] is True


def test_validate_data_cli_explicit_as_of_auto_profile_does_not_publish_pointer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_daily_default_cli_cache(tmp_path)
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)

    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            "data/raw/prices_daily.csv",
            "--rates-path",
            "data/raw/rates_daily.csv",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(tmp_path / "manual_quality.md"),
        ],
        terminal_width=240,
    )

    assert result.exit_code == 0, result.output
    assert "DQ discovery pointer：未发布" in result.output
    assert not (
        tmp_path / "outputs/data_quality/executions/discovery/daily_default/2026-05-02/current.json"
    ).exists()


def test_validate_data_cli_explicit_daily_profile_uses_project_relative_inputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_daily_default_cli_cache(tmp_path)
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    decoy_cwd = tmp_path / "unrelated_cwd"
    decoy_raw = decoy_cwd / "data/raw"
    decoy_raw.mkdir(parents=True)
    (decoy_raw / "prices_daily.csv").write_text("invalid", encoding="utf-8")
    (decoy_raw / "rates_daily.csv").write_text("invalid", encoding="utf-8")
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.chdir(decoy_cwd)

    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            "data/raw/prices_daily.csv",
            "--rates-path",
            "data/raw/rates_daily.csv",
            "--as-of",
            "2026-05-02",
            "--execution-profile",
            "daily_default.v1",
            "--output-path",
            str(tmp_path / "daily_quality.md"),
        ],
        terminal_width=240,
    )

    pointer_path = (
        tmp_path / "outputs/data_quality/executions/discovery/daily_default/2026-05-02/current.json"
    )
    assert result.exit_code == 0, result.output
    assert "未发布（非 daily_default profile）" not in result.output
    assert pointer_path.is_file()


def test_validate_data_cli_daily_profile_cannot_use_cwd_short_path_bypass(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_daily_default_cli_cache(tmp_path)
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.chdir(prices_path.parent)

    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            "prices_daily.csv",
            "--rates-path",
            "rates_daily.csv",
            "--as-of",
            "2026-05-02",
            "--execution-profile",
            "daily_default.v1",
        ],
        terminal_width=240,
    )

    assert result.exit_code == 2
    assert "daily_default.v1" in result.output
    assert not (
        tmp_path / "outputs/data_quality/executions/discovery/daily_default/2026-05-02/current.json"
    ).exists()


def test_validate_data_cli_pointer_failure_does_not_publish_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_daily_default_cli_cache(tmp_path)
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)

    def _fail_pointer_publish(*args, **kwargs):
        raise data_cache_cli.DataQualityExecutionError(
            "DQ_RECEIPT_ID_MISMATCH",
            "injected pointer verification failure",
        )

    monkeypatch.setattr(
        data_cache_cli,
        "publish_default_data_quality_execution_discovery",
        _fail_pointer_publish,
    )
    compatibility_report = tmp_path / "pointer_failure_quality.md"
    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            "data/raw/prices_daily.csv",
            "--rates-path",
            "data/raw/rates_daily.csv",
            "--as-of",
            "2026-05-02",
            "--execution-profile",
            "daily_default.v1",
            "--output-path",
            str(compatibility_report),
        ],
        terminal_width=240,
    )

    audit_dir = tmp_path / "artifacts/data_refresh_audit/validation"
    assert result.exit_code == 1
    assert "DQ_RECEIPT_ID_MISMATCH" in result.output
    assert compatibility_report.is_file()
    assert not audit_dir.exists()


def test_validate_data_cli_tampered_execution_receipt_blocks_audit_projection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    prices_path, rates_path = _write_valid_cache(
        tmp_path,
        tickers=configured_price_tickers(load_universe()),
    )
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)
    write_audit_sidecar = data_cache_cli.write_validate_data_audit_sidecar

    def _tamper_then_write_audit(**kwargs):
        execution_result = kwargs["execution_result"]
        execution_result.receipt_path.write_bytes(b"{}")
        return write_audit_sidecar(**kwargs)

    monkeypatch.setattr(
        data_cache_cli,
        "write_validate_data_audit_sidecar",
        _tamper_then_write_audit,
    )
    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(tmp_path / "tampered_receipt_quality.md"),
        ],
        terminal_width=240,
    )

    audit_dir = tmp_path / "artifacts/data_refresh_audit/validation"
    assert result.exit_code == 1
    assert "DQ_RECEIPT_ID_MISMATCH" in result.output
    assert not audit_dir.exists()


def test_validate_data_cli_returns_nonzero_on_failure(tmp_path: Path, monkeypatch) -> None:
    prices_path, rates_path = _write_valid_cache(
        tmp_path,
        tickers=configured_price_tickers(load_universe()),
    )
    _prepare_canonical_cli_project(tmp_path, prices_path=prices_path, rates_path=rates_path)
    monkeypatch.setattr(data_cache_cli, "PROJECT_ROOT", tmp_path)
    output_path = tmp_path / "quality.md"

    result = CliRunner().invoke(
        app,
        [
            "validate-data",
            "--prices-path",
            str(prices_path),
            "--rates-path",
            str(rates_path),
            "--as-of",
            "2026-06-01",
            "--output-path",
            str(output_path),
        ],
        terminal_width=240,
    )

    assert result.exit_code == 1
    assert output_path.exists()
    assert "数据质量状态：FAIL" in result.output
    assert "Canonicalreceipt" in "".join(result.output.split())


def _write_valid_cache(
    tmp_path: Path,
    tickers: list[str] | None = None,
) -> tuple[Path, Path]:
    selected_tickers = tickers or ["MSFT", "NVDA"]
    price_rows: list[dict[str, object]] = []
    for index, ticker in enumerate(selected_tickers):
        base_price = 100.0 + index * 10.0
        price_rows.extend(
            [
                _price_row(
                    "2026-04-29",
                    ticker,
                    base_price,
                    base_price + 5,
                    base_price - 1,
                    base_price + 4,
                    base_price + 4,
                    1000 + index,
                ),
                _price_row(
                    "2026-04-30",
                    ticker,
                    base_price + 4,
                    base_price + 6,
                    base_price + 2,
                    base_price + 5,
                    base_price + 5,
                    1100 + index,
                ),
            ]
        )
    prices = pd.DataFrame(price_rows)
    rates = pd.DataFrame(
        [
            {"date": "2026-04-29", "series": "DGS2", "value": 4.1},
            {"date": "2026-04-30", "series": "DGS2", "value": 4.2},
            {"date": "2026-04-29", "series": "DGS10", "value": 4.4},
            {"date": "2026-04-30", "series": "DGS10", "value": 4.5},
            {"date": "2026-04-29", "series": "DTWEXBGS", "value": 120.0},
            {"date": "2026-04-30", "series": "DTWEXBGS", "value": 120.2},
        ]
    )
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    prices.to_csv(prices_path, index=False)
    rates.to_csv(rates_path, index=False)
    return prices_path, rates_path


def _write_daily_default_cli_cache(root: Path) -> tuple[Path, Path]:
    raw_dir = root / "data/raw"
    raw_dir.mkdir(parents=True)
    prices_path, rates_path = _write_valid_cache(
        raw_dir,
        tickers=configured_price_tickers(load_universe()),
    )
    pd.read_csv(prices_path).to_csv(raw_dir / "prices_marketstack_daily.csv", index=False)
    return prices_path, rates_path


def _prepare_canonical_cli_project(
    root: Path,
    *,
    prices_path: Path,
    rates_path: Path,
) -> None:
    policy_path = root / "config/data_quality.yaml"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_bytes((REAL_PROJECT_ROOT / "config/data_quality.yaml").read_bytes())
    (root / "config/universe.yaml").write_bytes(
        (REAL_PROJECT_ROOT / "config/universe.yaml").read_bytes()
    )
    for relative in (
        Path("src/ai_trading_system/data/immutable_publish.py"),
        Path("src/ai_trading_system/data/quality_execution.py"),
        Path("src/ai_trading_system/data/quality.py"),
    ):
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((REAL_PROJECT_ROOT / relative).read_bytes())
    manifest_rows = [
        {
            "downloaded_at": "2026-05-02T00:00:00+00:00",
            "source_id": "fixture_prices",
            "provider": "test",
            "endpoint": "test",
            "request_parameters": "{}",
            "output_path": prices_path.relative_to(root).as_posix(),
            "row_count": len(pd.read_csv(prices_path)),
            "checksum_sha256": _sha256_file(prices_path),
        },
        {
            "downloaded_at": "2026-05-02T00:00:00+00:00",
            "source_id": "fixture_rates",
            "provider": "test",
            "endpoint": "test",
            "request_parameters": "{}",
            "output_path": rates_path.relative_to(root).as_posix(),
            "row_count": len(pd.read_csv(rates_path)),
            "checksum_sha256": _sha256_file(rates_path),
        },
    ]
    secondary_prices_path = prices_path.parent / "prices_marketstack_daily.csv"
    if secondary_prices_path.is_file():
        manifest_rows.append(
            {
                "downloaded_at": "2026-05-02T00:00:00+00:00",
                "source_id": "fixture_marketstack_prices",
                "provider": "test",
                "endpoint": "test",
                "request_parameters": "{}",
                "output_path": secondary_prices_path.relative_to(root).as_posix(),
                "row_count": len(pd.read_csv(secondary_prices_path)),
                "checksum_sha256": _sha256_file(secondary_prices_path),
            }
        )
    manifest = pd.DataFrame(manifest_rows)
    manifest.to_csv(prices_path.parent / "download_manifest.csv", index=False)


def _price_row(
    row_date: str,
    ticker: str,
    open_price: float,
    high: float,
    low: float,
    close: float,
    adj_close: float,
    volume: int,
) -> dict[str, object]:
    return {
        "date": row_date,
        "ticker": ticker,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "adj_close": adj_close,
        "volume": volume,
    }


def _rate_date_for_series(series: str, row_date: str) -> str:
    if series == "DTWEXBGS":
        return {
            "2026-04-29": "2026-04-30",
            "2026-04-30": "2026-05-01",
        }[row_date]
    return {
        "2026-04-29": "2026-05-07",
        "2026-04-30": "2026-05-08",
    }[row_date]


def _manifest_row(source_id: str, output_path: Path) -> dict[str, object]:
    return {
        "downloaded_at": "2026-05-02T00:00:00+00:00",
        "source_id": source_id,
        "provider": "test",
        "endpoint": "test",
        "request_parameters": "{}",
        "output_path": str(output_path),
        "row_count": 1,
        "checksum_sha256": _sha256_file(output_path),
    }


def _sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _issue_codes(report: DataQualityReport) -> set[str]:
    return {issue.code for issue in report.issues}


def _issue_by_code(report: DataQualityReport, code: str):
    return next(issue for issue in report.issues if issue.code == code)


def _quality_config_with_consistency_start(start: date):
    config = load_data_quality()
    return config.model_copy(
        update={
            "prices": config.prices.model_copy(update={"consistency_start_date": start}),
            "rates": config.rates.model_copy(update={"consistency_start_date": start}),
        }
    )


def _quality_config_with_secondary_overlap_threshold(threshold: float):
    config = load_data_quality()
    return config.model_copy(
        update={
            "prices": config.prices.model_copy(
                update={"secondary_source_min_overlap_ratio": threshold}
            ),
        }
    )


def _quality_config_with_secondary_fail_closed():
    config = _quality_config_with_secondary_overlap_threshold(0.50)
    return config.model_copy(
        update={
            "prices": config.prices.model_copy(
                update={"secondary_source_self_check_fail_closed": True}
            ),
        }
    )
