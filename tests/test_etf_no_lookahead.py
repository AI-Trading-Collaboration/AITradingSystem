from __future__ import annotations

import json

from ai_trading_system.etf_portfolio.no_lookahead import (
    extract_daily_brief_decision_text,
    validate_daily_brief_no_lookahead,
    validate_no_lookahead_records,
)


def test_no_lookahead_contract_allows_valid_next_trading_day_execution() -> None:
    result = validate_no_lookahead_records(
        feature_snapshots=[
            {
                "signal_date": "2026-05-29",
                "feature_source_date": "2026-05-29",
                "raw_market_data_date": "2026-05-29",
            }
        ],
        signal_records=[
            {
                "signal_date": "2026-05-29",
                "decision_payload": {"composite_score": 65.0},
            }
        ],
        allocation_records=[
            {
                "signal_date": "2026-05-29",
                "date": "2026-05-29",
                "target_weight": 0.50,
            }
        ],
        trade_records=[
            {
                "signal_date": "2026-05-29",
                "execution_date": "2026-06-01",
                "return_date": "2026-06-02",
                "symbol": "QQQ",
            }
        ],
    )

    assert result.passed
    assert result.timing_contract["earliest_execution_date"] == "next trading date after t"


def test_no_lookahead_contract_rejects_same_day_execution() -> None:
    result = validate_no_lookahead_records(
        trade_records=[
            {
                "signal_date": "2026-05-29",
                "execution_date": "2026-05-29",
                "symbol": "QQQ",
            }
        ]
    )

    assert not result.passed
    assert "execution_date_not_after_signal_date" in _issue_codes(result)


def test_no_lookahead_contract_rejects_return_date_not_after_execution() -> None:
    result = validate_no_lookahead_records(
        trade_records=[
            {
                "signal_date": "2026-05-29",
                "execution_date": "2026-06-01",
                "return_date": "2026-06-01",
                "symbol": "QQQ",
            }
        ]
    )

    assert not result.passed
    assert "return_date_not_after_execution_date" in _issue_codes(result)


def test_no_lookahead_contract_rejects_feature_source_after_signal_date() -> None:
    result = validate_no_lookahead_records(
        feature_snapshots=[
            {
                "signal_date": "2026-05-29",
                "feature_source_date": "2026-06-01",
                "symbol": "SPY",
            }
        ]
    )

    assert not result.passed
    assert "feature_source_date_after_signal_date" in _issue_codes(result)


def test_no_lookahead_contract_rejects_future_field_in_decision_payload() -> None:
    result = validate_no_lookahead_records(
        signal_records=[
            {
                "signal_date": "2026-05-29",
                "decision_payload": json.dumps(
                    {"symbol": "QQQ", "forward_return_20d": 0.08},
                    sort_keys=True,
                ),
            }
        ]
    )

    assert not result.passed
    assert "future_field_in_decision_payload" in _issue_codes(result)


def test_simulation_future_evaluation_fields_require_evaluation_only_marker() -> None:
    unmarked = validate_no_lookahead_records(
        simulation_records=[
            {
                "date": "2026-05-29",
                "symbol": "QQQ",
                "forward_return_20d": 0.08,
                "evaluation_only": False,
            }
        ]
    )
    marked = validate_no_lookahead_records(
        simulation_records=[
            {
                "date": "2026-05-29",
                "symbol": "QQQ",
                "forward_return_20d": 0.08,
                "evaluation_only": True,
            }
        ]
    )

    assert not unmarked.passed
    assert "future_field_without_evaluation_only_marker" in _issue_codes(unmarked)
    assert marked.passed


def test_daily_brief_decision_sections_exclude_evaluation_only_fields() -> None:
    markdown = """# ETF Daily Brief

## 1. Executive Summary

- Suggested Action: Hold

## 7. Simulation Performance

- forward_return_20d: 8.0%

## 9. Action Checklist

- Rebalance required: no
"""
    leaking_markdown = markdown.replace(
        "- Suggested Action: Hold",
        "- Suggested Action: Hold\n- forward_return_20d: 8.0%",
    )

    assert "forward_return_20d" not in extract_daily_brief_decision_text(markdown)
    assert validate_daily_brief_no_lookahead(markdown).passed
    leaking = validate_daily_brief_no_lookahead(leaking_markdown)
    assert not leaking.passed
    assert "report_decision_contains_evaluation_only_field" in _issue_codes(leaking)


def _issue_codes(result: object) -> set[str]:
    return {issue.code for issue in result.issues}
