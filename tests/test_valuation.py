from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_universe, load_watchlist
from ai_trading_system.valuation import (
    build_valuation_review_report,
    load_valuation_snapshot_store,
    render_valuation_review_report,
    render_valuation_validation_report,
    validate_valuation_snapshot_store,
    write_valuation_review_report,
    write_valuation_validation_report,
)


def test_validate_valuation_snapshot_store_passes_manual_snapshot(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "nvda.yaml"
    _write_valid_snapshot(snapshot_path)

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.snapshot_count == 1
    assert report.ticker_count == 1


def test_validate_valuation_snapshot_store_rejects_unknown_ticker(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "unknown.yaml"
    _write_valid_snapshot(snapshot_path, ticker="UNKNOWN")

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_ticker" in {issue.code for issue in report.issues}


def test_validate_valuation_snapshot_store_warns_public_convenience_source(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "nvda.yaml"
    _write_valid_snapshot(snapshot_path, source_type="public_convenience")

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert "public_convenience_source" in {issue.code for issue in report.issues}


def test_validate_valuation_snapshot_store_rejects_negative_multiple(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "nvda.yaml"
    _write_valid_snapshot(snapshot_path, forward_pe=-1.0)

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "negative_valuation_multiple" in {issue.code for issue in report.issues}


def test_render_and_write_valuation_reports(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "nvda.yaml"
    _write_valid_snapshot(snapshot_path)
    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_valuation_review_report(validation_report)

    validation_markdown = render_valuation_validation_report(validation_report)
    review_markdown = render_valuation_review_report(review_report)
    validation_path = write_valuation_validation_report(
        validation_report,
        tmp_path / "valuation_validation.md",
    )
    review_path = write_valuation_review_report(
        review_report,
        tmp_path / "valuation_review.md",
    )

    assert "- 状态：PASS" in validation_markdown
    assert "nvda_valuation_2026_05_01" in validation_markdown
    assert "偏贵或拥挤" in review_markdown
    assert validation_path.read_text(encoding="utf-8") == validation_markdown
    assert review_path.read_text(encoding="utf-8") == review_markdown


def test_valuation_cli_validate_review_and_list(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "valuation_snapshots"
    snapshot_dir.mkdir()
    _write_valid_snapshot(snapshot_dir / "nvda.yaml")
    validation_path = tmp_path / "valuation_validation.md"
    review_path = tmp_path / "valuation_review.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "valuation",
            "validate",
            "--input-path",
            str(snapshot_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(validation_path),
        ],
    )
    review_result = CliRunner().invoke(
        app,
        [
            "valuation",
            "review",
            "--input-path",
            str(snapshot_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(review_path),
        ],
    )
    list_result = CliRunner().invoke(
        app,
        [
            "valuation",
            "list",
            "--input-path",
            str(snapshot_dir),
        ],
    )

    assert validate_result.exit_code == 0
    assert review_result.exit_code == 0
    assert list_result.exit_code == 0
    assert validation_path.exists()
    assert review_path.exists()
    assert "估值快照校验状态：PASS" in validate_result.output
    assert "估值复核状态：PASS_WITH_WARNINGS" in review_result.output
    assert "NVDA" in list_result.output


def _write_valid_snapshot(
    path: Path,
    ticker: str = "NVDA",
    source_type: str = "manual_input",
    forward_pe: float = 36.0,
) -> None:
    path.write_text(
        f"""snapshot_id: nvda_valuation_2026_05_01
ticker: {ticker}
as_of: 2026-05-01
source_type: {source_type}
source_name: manual_valuation_sheet
source_url: ""
captured_at: 2026-05-01
valuation_metrics:
  - metric_id: forward_pe
    value: {forward_pe}
    unit: ratio
    period: next_12m
    source_field: manual_forward_pe
  - metric_id: ev_sales
    value: 18.0
    unit: ratio
    period: next_12m
    source_field: manual_ev_sales
expectation_metrics:
  - metric_id: revenue_growth_next_12m_pct
    value: 28.0
    unit: percent
    period: next_12m
    source_field: manual_revenue_growth
crowding_signals:
  - signal_id: valuation_percentile
    name: 估值历史分位
    status: elevated
    evidence_source: manual_valuation_sheet
    updated_at: 2026-05-01
valuation_percentile: 82
overall_assessment: expensive
notes: 测试用估值快照。
""",
        encoding="utf-8",
    )
