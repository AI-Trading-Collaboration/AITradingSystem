"""DATA-034-02: SOXX's 2026-11-05 three-for-one split is a reviewed corporate action.

After DATA-034-01 refetches SOXX's history on the split-adjusted basis, the adjustment ratio
(adj_close / raw close) jumps by 3 on 2026-11-05. config/data_quality.yaml registers the split
next to SOXX's 2024-03-07 one, so the jump is INFO, not an unresolved WARNING that score_daily
refuses. A broken adjusted close is still an ERROR: the registration exempts ratio jumps only.
The policy bytes move, so the rate review pack, its decision and the regime-label dependency are
re-bound (checked by tests/test_ops080_googl_known_split.py and the review pack tests).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.config import load_data_quality
from ai_trading_system.data.quality import (
    DataQualityIssue,
    DataQualityReport,
    Severity,
    validate_data_cache,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DQ_POLICY = REPO_ROOT / "config" / "data_quality.yaml"


def _row(value_date: date, ticker: str, close: float, adj_close: float) -> dict[str, object]:
    return {
        "date": value_date.isoformat(),
        "ticker": ticker,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "adj_close": adj_close,
        "volume": 1000,
    }


def test_soxx_2026_split_is_registered_next_to_the_2024_one() -> None:
    events = load_data_quality().prices.known_split_events

    assert sorted(events) == [
        "AMZN",
        "AVGO",
        "GOOG",
        "GOOGL",
        "LRCX",
        "NOW",
        "NVDA",
        "SMH",
        "SOXX",
        "TQQQ",
    ]
    assert sum(len(items) for items in events.values()) == 13
    soxx = events["SOXX"]
    assert [(event.effective_date, event.ratio) for event in soxx] == [
        (date(2024, 3, 7), 3),
        (date(2026, 11, 5), 3),
    ]
    new = soxx[1]
    assert new.status == "REVIEWED"
    assert new.owner == "data_platform_owner"
    assert new.reviewed_at == date(2026, 11, 10)
    assert new.review_condition
    assert new.source_name
    assert new.source_url.startswith("https://www.sec.gov/Archives/edgar/data/0001100663/")
    assert soxx[0].reviewed_at == date(2026, 7, 23)


def test_dq_thresholds_profile_and_policy_version_unchanged() -> None:
    raw = yaml.safe_load(DQ_POLICY.read_text(encoding="utf-8"))
    prices = raw["prices"]

    assert raw["governance"]["policy_version"] == "data_quality_cache_gate.v2"
    assert prices["extreme_daily_return_abs"] == 0.50
    assert prices["suspicious_daily_return_abs"] == 0.20
    assert prices["suspicious_adjustment_ratio_change_abs"] == 0.25
    assert prices["known_split_match_window_days"] == 5
    assert prices["known_split_ratio_tolerance_abs"] == 0.08


def _validate(tmp_path: Path, rows: list[dict[str, object]]) -> DataQualityReport:
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(rows).to_csv(prices_path, index=False)
    pd.DataFrame([{"date": "2026-11-09", "series": "DGS2", "value": 3.6}]).to_csv(
        rates_path, index=False
    )
    return validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=["SOXX"],
        expected_rate_series=["DGS2"],
        quality_config=load_data_quality(),
        as_of=date(2026, 11, 10),
    )


def _issue(report: DataQualityReport, code: str) -> DataQualityIssue | None:
    return next((issue for issue in report.issues if issue.code == code), None)


def test_refetched_soxx_history_passes_dq_with_the_split_as_info(tmp_path: Path) -> None:
    rows = [
        _row(date(2026, 11, 3), "SOXX", 465.0, 155.0),
        _row(date(2026, 11, 4), "SOXX", 465.0, 155.0),
        _row(date(2026, 11, 5), "SOXX", 155.0, 155.0),
        _row(date(2026, 11, 6), "SOXX", 155.0, 155.0),
        _row(date(2026, 11, 9), "SOXX", 155.0, 155.0),
    ]
    report = _validate(tmp_path, rows)

    known = _issue(report, "prices_known_split_adjustment_ratio_jump")
    assert known is not None and known.severity is Severity.INFO
    assert known.rows == 1
    assert "2026-11-05" in (known.sample or "")
    assert _issue(report, "prices_adjustment_ratio_jump") is None
    assert _issue(report, "prices_extreme_adj_close_move") is None
    assert not [issue for issue in report.issues if issue.severity is Severity.WARNING]
    assert report.passed is True


def test_stale_soxx_history_still_fails_dq_after_registration(tmp_path: Path) -> None:
    # The 2026-11-10 shape: registering the split classifies the ratio jumps, but the
    # adjusted-close break is still an error (no split exemption for adj_close moves).
    rows = [
        _row(date(2026, 11, 3), "SOXX", 465.0, 465.0),
        _row(date(2026, 11, 4), "SOXX", 465.0, 155.0),
        _row(date(2026, 11, 5), "SOXX", 155.0, 155.0),
        _row(date(2026, 11, 6), "SOXX", 155.0, 155.0),
        _row(date(2026, 11, 9), "SOXX", 155.0, 155.0),
    ]
    report = _validate(tmp_path, rows)

    extreme = _issue(report, "prices_extreme_adj_close_move")
    assert extreme is not None and extreme.severity is Severity.ERROR
    assert "2026-11-04" in (extreme.sample or "")
    assert _issue(report, "prices_adjustment_ratio_jump") is None
    assert report.passed is False
