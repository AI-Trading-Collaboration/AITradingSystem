from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_industry_chain, load_watchlist
from ai_trading_system.portfolio_exposure import (
    build_portfolio_exposure_report,
    render_portfolio_exposure_report,
)


def test_build_portfolio_exposure_report_decomposes_real_holdings(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "positions.csv"
    _write_positions_csv(input_path)

    report = build_portfolio_exposure_report(
        input_path=input_path,
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
    )
    markdown = render_portfolio_exposure_report(report)

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.snapshot_date == date(2026, 5, 4)
    assert report.total_market_value == 36_000
    assert report.ai_market_value == 15_600
    assert round(report.ai_exposure_pct_total, 4) == 0.4333
    assert report.etf_beta_coverage == 1.0
    assert report.ticker_exposures[0].name == "NVDA"
    assert any(bucket.name == "gpu_asic_demand" for bucket in report.node_exposures)
    assert any(bucket.name == "US" for bucket in report.region_exposures)
    assert any(
        issue.code == "portfolio_ai_position_missing_node_mapping"
        for issue in report.issues
    )
    assert "生产影响：none" in markdown
    assert "### 产业链节点暴露" in markdown


def test_missing_portfolio_positions_is_not_connected(tmp_path: Path) -> None:
    report = build_portfolio_exposure_report(
        input_path=tmp_path / "missing.csv",
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
    )
    markdown = render_portfolio_exposure_report(report)

    assert report.status == "NOT_CONNECTED"
    assert report.passed is True
    assert "不能把观察池或模型建议仓位当作账户持仓" in markdown


def test_invalid_portfolio_positions_fails_validation(tmp_path: Path) -> None:
    input_path = tmp_path / "positions.csv"
    input_path.write_text(
        "as_of,ticker,market_value\n2026-05-04,NVDA,10000\n",
        encoding="utf-8",
    )

    report = build_portfolio_exposure_report(
        input_path=input_path,
        as_of=date(2026, 5, 4),
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
    )

    assert report.status == "FAIL"
    assert report.passed is False
    assert "portfolio_positions_missing_columns" in {
        issue.code for issue in report.issues
    }


def test_portfolio_exposure_cli_writes_report(tmp_path: Path) -> None:
    input_path = tmp_path / "positions.csv"
    output_path = tmp_path / "portfolio_exposure.md"
    _write_positions_csv(input_path)

    result = CliRunner().invoke(
        app,
        [
            "portfolio",
            "exposure",
            "--input-path",
            str(input_path),
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "组合暴露状态：PASS_WITH_WARNINGS" in result.output
    assert output_path.exists()
    assert "AI 名义暴露：15600.00" in output_path.read_text(encoding="utf-8")


def _write_positions_csv(output_path: Path) -> None:
    output_path.write_text(
        "\n".join(
            [
                (
                    "as_of,ticker,instrument_type,quantity,market_value,currency,"
                    "ai_exposure_pct,region,customer_chain,factor_tags,"
                    "correlation_cluster,etf_beta_to_ai_proxy,notes"
                ),
                (
                    "2026-05-03,NVDA,single_stock,1,1000,USD,1.0,US,"
                    "hyperscaler_capex,growth,ai_semis,,older snapshot"
                ),
                (
                    "2026-05-04,NVDA,single_stock,10,10000,USD,1.0,US,"
                    "hyperscaler_capex,growth;semiconductor,ai_semis,,core position"
                ),
                (
                    "2026-05-04,MSFT,single_stock,12,6000,USD,0.35,US,"
                    "cloud_ai,growth;mega_cap,mega_cap_ai,,platform exposure"
                ),
                (
                    "2026-05-04,SMH,etf,20,5000,USD,0.70,US,"
                    "semiconductor_cycle,semiconductor;etf,ai_semis,0.85,ETF proxy"
                ),
                "2026-05-04,USD_CASH,cash,1,15000,USD,0,US,cash,cash,cash,,cash",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
