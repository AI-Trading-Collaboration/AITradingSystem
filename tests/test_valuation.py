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


def test_validate_valuation_snapshot_store_labels_backfilled_history_confidence(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "nvda_backfilled.yaml"
    _write_valid_snapshot(
        snapshot_path,
        point_in_time_class="backfilled_history_distribution",
        history_source_class="vendor_historical_endpoint",
        confidence_level="low",
        confidence_reason="FMP historical endpoint is not a point-in-time archive.",
        backtest_use="captured_at_forward_only",
    )

    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_valuation_review_report(validation_report)
    markdown = render_valuation_review_report(review_report)

    assert validation_report.status == "PASS_WITH_WARNINGS"
    assert "backfilled_valuation_not_strict_point_in_time" in {
        issue.code for issue in validation_report.issues
    }
    assert review_report.items[0].point_in_time_class == "backfilled_history_distribution"
    assert review_report.items[0].confidence_level == "low"
    assert review_report.items[0].backtest_use == "captured_at_forward_only"
    assert "回填历史分布" in markdown
    assert "低" in markdown


def test_validate_valuation_snapshot_store_rejects_low_confidence_strict_pit(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "nvda_low_confidence.yaml"
    _write_valid_snapshot(
        snapshot_path,
        confidence_level="low",
        backtest_use="strict_point_in_time",
    )

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_path),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "low_confidence_strict_backtest_use" in {issue.code for issue in report.issues}


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


def test_review_uses_latest_visible_snapshot_per_ticker(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "valuation_snapshots"
    snapshot_dir.mkdir()
    _write_valid_snapshot(
        snapshot_dir / "nvda_old.yaml",
        snapshot_id="nvda_old",
        as_of=date(2026, 4, 1),
        valuation_percentile=95.0,
    )
    _write_valid_snapshot(
        snapshot_dir / "nvda_latest.yaml",
        snapshot_id="nvda_latest",
        as_of=date(2026, 5, 1),
        valuation_percentile=25.0,
        overall_assessment="cheap",
    )

    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_valuation_review_report(validation_report)

    assert validation_report.snapshot_count == 2
    assert [item.snapshot_id for item in review_report.items] == ["nvda_latest"]
    assert review_report.items[0].valuation_percentile == 25.0


def test_validation_excludes_future_snapshots_for_historical_as_of(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "valuation_snapshots"
    snapshot_dir.mkdir()
    _write_valid_snapshot(
        snapshot_dir / "nvda_visible.yaml",
        snapshot_id="nvda_visible",
        as_of=date(2026, 5, 8),
        valuation_percentile=55.0,
    )
    _write_valid_snapshot(
        snapshot_dir / "nvda_future.yaml",
        snapshot_id="nvda_future",
        as_of=date(2026, 5, 10),
        valuation_percentile=95.0,
    )

    validation_report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 8),
    )
    review_report = build_valuation_review_report(validation_report)
    markdown = render_valuation_validation_report(validation_report)

    assert validation_report.status == "PASS_WITH_WARNINGS"
    assert validation_report.snapshot_count == 1
    assert [loaded.snapshot.snapshot_id for loaded in validation_report.snapshots] == [
        "nvda_visible"
    ]
    assert [item.snapshot_id for item in review_report.items] == ["nvda_visible"]
    assert "valuation_snapshot_excluded_future_as_of" in {
        issue.code for issue in validation_report.issues
    }
    assert "nvda_future" in markdown
    assert "nvda_future | NVDA | 2026-05-10" not in markdown


def test_validation_current_readiness_uses_latest_snapshot_per_ticker(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "valuation_snapshots"
    snapshot_dir.mkdir()
    _write_valid_snapshot(
        snapshot_dir / "nvda_old.yaml",
        snapshot_id="nvda_old",
        as_of=date(2026, 2, 1),
        valuation_percentile=None,
    )
    _write_valid_snapshot(
        snapshot_dir / "nvda_latest.yaml",
        snapshot_id="nvda_latest",
        as_of=date(2026, 5, 1),
        valuation_percentile=82.0,
        include_eps_revision=True,
    )

    report = validate_valuation_snapshot_store(
        store=load_valuation_snapshot_store(snapshot_dir),
        universe=load_universe(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )
    markdown = render_valuation_validation_report(report)

    assert report.status == "PASS"
    assert "valuation_snapshot_stale" not in {issue.code for issue in report.issues}
    assert "missing_valuation_percentile" not in {issue.code for issue in report.issues}
    assert "| valuation_percentile | 1/1 |" in markdown
    assert "| eps_revision_90d_pct | 1/1 |" in markdown


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
    snapshot_id: str = "nvda_valuation_2026_05_01",
    ticker: str = "NVDA",
    source_type: str = "manual_input",
    as_of: date = date(2026, 5, 1),
    captured_at: date | None = None,
    forward_pe: float = 36.0,
    valuation_percentile: float | None = 82.0,
    overall_assessment: str = "expensive",
    include_eps_revision: bool = False,
    point_in_time_class: str = "captured_snapshot",
    history_source_class: str = "unknown",
    confidence_level: str = "medium",
    confidence_reason: str = "",
    backtest_use: str = "captured_at_forward_only",
) -> None:
    captured_date = captured_at or as_of
    percentile_value = "null" if valuation_percentile is None else str(valuation_percentile)
    eps_revision = (
        """
  - metric_id: eps_revision_90d_pct
    value: 4.0
    unit: percent
    period: trailing_90d
    source_field: analyst-estimates.epsAvg
"""
        if include_eps_revision
        else ""
    )
    path.write_text(
        f"""snapshot_id: {snapshot_id}
ticker: {ticker}
as_of: {as_of.isoformat()}
source_type: {source_type}
source_name: manual_valuation_sheet
source_url: ""
captured_at: {captured_date.isoformat()}
point_in_time_class: {point_in_time_class}
history_source_class: {history_source_class}
confidence_level: {confidence_level}
confidence_reason: "{confidence_reason}"
backtest_use: {backtest_use}
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
{eps_revision.rstrip()}
crowding_signals:
  - signal_id: valuation_percentile
    name: 估值历史分位
    status: elevated
    evidence_source: manual_valuation_sheet
    updated_at: {as_of.isoformat()}
valuation_percentile: {percentile_value}
overall_assessment: {overall_assessment}
notes: 测试用估值快照。
""",
        encoding="utf-8",
    )
