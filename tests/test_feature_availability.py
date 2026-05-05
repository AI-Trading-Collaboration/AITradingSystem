from __future__ import annotations

from datetime import date

import pandas as pd

from ai_trading_system.feature_availability import (
    DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
    build_feature_availability_report,
    build_feature_source_check,
    feature_availability_summary_record,
    render_feature_availability_report,
    render_feature_availability_section,
)


def test_feature_availability_catalog_covers_core_pit_sources() -> None:
    report = build_feature_availability_report(
        input_path=DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
        as_of=date(2026, 5, 5),
        observed_sources=("prices_daily", "rates_daily"),
        required_sources=(
            "prices_daily",
            "rates_daily",
            "sec_fundamental_features",
            "valuation_snapshots",
            "risk_event_occurrences",
            "watchlist_lifecycle",
        ),
    )

    assert report.passed
    assert report.status == "PASS"
    assert {rule.rule_id for rule in report.rules} >= {
        "market_prices_daily",
        "macro_rates_daily",
        "sec_fundamentals_filing",
        "valuation_snapshots",
        "risk_event_occurrences",
        "watchlist_lifecycle",
    }
    markdown = render_feature_availability_report(report)
    section = render_feature_availability_section(report)
    summary = feature_availability_summary_record(report)
    assert "# PIT 特征可见时间报告" in markdown
    assert "## PIT 特征可见时间" in section
    assert summary["status"] == "PASS"
    assert "prices_daily" in summary["observed_sources"]


def test_feature_availability_report_flags_uncataloged_sources() -> None:
    report = build_feature_availability_report(
        input_path=DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
        as_of=date(2026, 5, 5),
        observed_sources=("unregistered_vendor_backfill",),
        required_sources=("prices_daily",),
    )

    assert not report.passed
    assert report.status == "FAIL"
    assert any(
        issue.code == "observed_source_without_availability_rule"
        and issue.source == "unregistered_vendor_backfill"
        for issue in report.issues
    )


def test_feature_availability_source_check_blocks_future_available_time() -> None:
    source_check = build_feature_source_check(
        source="sec_fundamental_features",
        frame=pd.DataFrame(
            [
                {
                    "end_date": "2026-03-31",
                    "filed_date": "2026-05-06",
                    "value": 1.2,
                }
            ]
        ),
        decision_time=date(2026, 5, 5),
        event_time_columns=("end_date",),
        available_time_columns=("filed_date",),
    )
    report = build_feature_availability_report(
        input_path=DEFAULT_FEATURE_AVAILABILITY_CONFIG_PATH,
        as_of=date(2026, 5, 5),
        observed_sources=("sec_fundamental_features",),
        required_sources=("sec_fundamental_features",),
        source_checks=(source_check,),
    )

    assert not report.passed
    assert source_check.future_available_time_count == 1
    assert any(
        issue.code == "feature_source_available_time_after_decision_time"
        and issue.source == "sec_fundamental_features"
        for issue in report.issues
    )
    markdown = render_feature_availability_report(report)
    summary = feature_availability_summary_record(report)
    assert "字段级 Source 检查" in markdown
    assert summary["source_checks"][0]["future_available_time_count"] == 1
