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
from ai_trading_system.watchlist_lifecycle import (
    WatchlistLifecycleConfig,
    WatchlistLifecycleEntry,
    active_watchlist_tickers_as_of,
    load_watchlist_lifecycle,
    validate_watchlist_lifecycle,
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


def test_validate_watchlist_lifecycle_passes_default_config() -> None:
    report = validate_watchlist_lifecycle(
        lifecycle=load_watchlist_lifecycle(),
        input_path=Path("config/watchlist_lifecycle.yaml"),
        watchlist=load_watchlist(),
        universe=load_universe(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.entry_count == 6
    assert report.active_entry_count == 6


def test_active_watchlist_tickers_as_of_filters_future_and_node_mapping() -> None:
    lifecycle = WatchlistLifecycleConfig(
        entries=[
            WatchlistLifecycleEntry(
                ticker="NVDA",
                added_at=date(2026, 4, 1),
                reason="测试新增。",
                active_from=date(2026, 4, 1),
                competence_status="in_competence",
                node_mapping_valid_from=date(2026, 4, 1),
                thesis_required_from=date(2026, 4, 1),
                source="unit_test",
            ),
            WatchlistLifecycleEntry(
                ticker="AMD",
                added_at=date(2026, 4, 1),
                reason="节点映射稍后生效。",
                active_from=date(2026, 4, 1),
                competence_status="in_competence",
                node_mapping_valid_from=date(2026, 4, 15),
                source="unit_test",
            ),
            WatchlistLifecycleEntry(
                ticker="MSFT",
                added_at=date(2026, 5, 1),
                reason="未来才加入。",
                active_from=date(2026, 5, 1),
                competence_status="in_competence",
                node_mapping_valid_from=date(2026, 5, 1),
                source="unit_test",
            ),
        ]
    )

    assert active_watchlist_tickers_as_of(
        lifecycle=lifecycle,
        tickers=("NVDA", "AMD", "MSFT"),
        as_of=date(2026, 4, 10),
    ) == ["NVDA"]
    assert active_watchlist_tickers_as_of(
        lifecycle=lifecycle,
        tickers=("NVDA", "AMD", "MSFT"),
        as_of=date(2026, 4, 16),
    ) == ["NVDA", "AMD"]


def test_watchlist_cli_validate_lifecycle_writes_report(tmp_path: Path) -> None:
    output_path = tmp_path / "watchlist_lifecycle.md"

    result = CliRunner().invoke(
        app,
        [
            "watchlist",
            "validate-lifecycle",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "观察池 lifecycle 校验状态：PASS" in result.output


def test_watchlist_cli_list_outputs_core_tickers() -> None:
    result = CliRunner().invoke(app, ["watchlist", "list"])

    assert result.exit_code == 0
    assert "MSFT" in result.output
    assert "NVDA" in result.output
