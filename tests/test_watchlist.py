from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import WatchlistConfig, load_universe, load_watchlist
from ai_trading_system.watchlist import (
    render_watchlist_validation_report,
    validate_watchlist_config,
    write_watchlist_validation_report,
)


def test_validate_watchlist_config_passes_default_watchlist() -> None:
    report = validate_watchlist_config(
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.error_count == 0
    assert report.active_count == 6


def test_validate_watchlist_config_fails_missing_core_ticker() -> None:
    watchlist = load_watchlist()
    broken = WatchlistConfig(
        items=[item for item in watchlist.items if item.ticker != "NVDA"],
    )

    report = validate_watchlist_config(
        watchlist=broken,
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "core_ticker_missing" in {issue.code for issue in report.issues}


def test_validate_watchlist_config_rejects_high_risk_without_thesis() -> None:
    watchlist = load_watchlist()
    items = [
        item.model_copy(update={"thesis_required": False}) if item.ticker == "AMD" else item
        for item in watchlist.items
    ]
    broken = WatchlistConfig(items=items)

    report = validate_watchlist_config(
        watchlist=broken,
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "high_risk_without_thesis" in {issue.code for issue in report.issues}


def test_render_and_write_watchlist_validation_report(tmp_path: Path) -> None:
    report = validate_watchlist_config(
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    markdown = render_watchlist_validation_report(report)
    output_path = write_watchlist_validation_report(report, tmp_path / "watchlist.md")

    assert "- 状态：PASS" in markdown
    assert "NVDA" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_watchlist_cli_validate_writes_report(tmp_path: Path) -> None:
    output_path = tmp_path / "watchlist_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "watchlist",
            "validate",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "观察池校验状态：PASS" in result.output


def test_watchlist_cli_list_outputs_core_tickers() -> None:
    result = CliRunner().invoke(app, ["watchlist", "list"])

    assert result.exit_code == 0
    assert "MSFT" in result.output
    assert "NVDA" in result.output
