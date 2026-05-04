from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.catalyst_calendar import (
    load_catalyst_calendar,
    lookup_catalyst,
    render_catalyst_calendar_report,
    validate_catalyst_calendar,
)
from ai_trading_system.cli import app
from ai_trading_system.config import load_industry_chain, load_risk_events, load_watchlist


def test_default_catalyst_calendar_validates_as_empty_baseline() -> None:
    report = _validate(load_catalyst_calendar(), as_of=date(2026, 5, 4))
    markdown = render_catalyst_calendar_report(report)

    assert report.passed is True
    assert report.status == "PASS"
    assert report.event_count == 0
    assert "baseline_empty" in markdown
    assert "暂无已登记 upcoming catalyst" in markdown


def test_catalyst_calendar_buckets_upcoming_events(tmp_path: Path) -> None:
    input_path = tmp_path / "catalyst_calendar.yaml"
    input_path.write_text(_calendar_yaml(), encoding="utf-8")

    report = _validate(load_catalyst_calendar(input_path), as_of=date(2026, 5, 4))
    markdown = render_catalyst_calendar_report(report)

    assert report.passed is True
    assert report.upcoming_count == 2
    assert [event.catalyst_id for event in report.upcoming_events(5)] == [
        "msft_earnings_2026q2"
    ]
    assert "msft_earnings_2026q2" in markdown
    assert "fomc_2026_05" in markdown
    assert "pre_event_review" in markdown


def test_catalyst_calendar_rejects_unknown_node(tmp_path: Path) -> None:
    input_path = tmp_path / "catalyst_calendar.yaml"
    input_path.write_text(
        _calendar_yaml(related_nodes="      - missing_node\n"),
        encoding="utf-8",
    )

    report = _validate(load_catalyst_calendar(input_path), as_of=date(2026, 5, 4))

    assert report.passed is False
    assert "unknown_related_node" in {issue.code for issue in report.issues}


def test_catalysts_cli_validates_upcoming_and_lookup(tmp_path: Path) -> None:
    input_path = tmp_path / "catalyst_calendar.yaml"
    report_path = tmp_path / "catalyst_calendar.md"
    input_path.write_text(_calendar_yaml(), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "catalysts",
            "validate",
            "--input-path",
            str(input_path),
            "--as-of",
            "2026-05-04",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "催化剂日历状态：PASS" in result.output
    assert report_path.exists()

    event = lookup_catalyst(input_path, "msft_earnings_2026q2")
    assert event.importance == "high"
    lookup = CliRunner().invoke(
        app,
        [
            "catalysts",
            "lookup",
            "--input-path",
            str(input_path),
            "--id",
            "msft_earnings_2026q2",
        ],
    )
    assert lookup.exit_code == 0
    assert "Microsoft earnings" in lookup.output
    assert "pre_event_review" in lookup.output


def _validate(store, *, as_of: date):  # type: ignore[no-untyped-def]
    return validate_catalyst_calendar(
        store,
        as_of=as_of,
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        risk_events=load_risk_events(),
    )


def _calendar_yaml(
    *,
    related_nodes: str = "      - cloud_capex\n",
) -> str:
    return f"""calendar_id: test_catalyst_calendar.v1
version: v1
status: active
owner: test
description: test catalyst calendar
source_policy: manual_review_required
last_reviewed_at: 2026-05-04
next_review_due: 2026-05-11
events:
  - catalyst_id: msft_earnings_2026q2
    title: Microsoft earnings and AI CapEx commentary
    event_type: earnings
    event_date: 2026-05-08
    status: scheduled
    importance: high
    related_tickers:
      - MSFT
    related_nodes:
{related_nodes}    linked_thesis_ids: []
    linked_risk_event_ids:
      - cloud_capex_cut
    pre_event_actions:
      - pre_event_review
      - no_new_position
    post_event_review_targets:
      - thesis
      - valuation
    source_name: manual test calendar
    source_type: manual_input
    source_url: https://example.com/msft
    captured_at: 2026-05-01T00:00:00Z
    reviewer: test_reviewer
    reviewed_at: 2026-05-04
    confidence: high
    notes: test event
  - catalyst_id: fomc_2026_05
    title: FOMC policy decision
    event_type: macro
    event_date: 2026-05-20
    status: tentative
    importance: medium
    related_tickers:
      - NVDA
      - AMD
    related_nodes:
      - cloud_capex
    linked_thesis_ids: []
    linked_risk_event_ids: []
    pre_event_actions:
      - manual_review_only
    post_event_review_targets:
      - valuation
    source_name: manual test calendar
    source_type: manual_input
    source_url: https://example.com/fomc
    captured_at: 2026-05-01T00:00:00Z
    reviewer: test_reviewer
    reviewed_at: 2026-05-04
    confidence: medium
    notes: test macro event
"""
