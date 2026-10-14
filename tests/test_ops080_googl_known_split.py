"""OPS-080-02: GOOGL's 2022-07-18 split is a reviewed corporate action.

OPS-080 fetches the shadow-tracking ticker GOOGL from 1600 calendar days before the end date,
so its rows now cross Alphabet's 20-for-1 split. The split must be classified like GOOG's
(INFO), not left as an unresolved adjustment-ratio jump (WARNING), or score_daily refuses
the DQ receipt. The DQ policy change moves the policy hash, so the rate review pack, its
owner decision and the regime-label dependency are bound to the new bytes.
"""

from __future__ import annotations

import json
from datetime import date
from hashlib import sha256
from pathlib import Path

import pandas as pd
import yaml

from ai_trading_system.config import load_data_quality
from ai_trading_system.contracts.rate_data_quality_attribution import (
    RATE_ROW_ATTRIBUTION_REVIEW_PACK_ID,
    load_rate_row_issue_attribution_decision,
)
from ai_trading_system.data.quality import Severity, validate_data_cache

REPO_ROOT = Path(__file__).resolve().parents[1]
DQ_POLICY = REPO_ROOT / "config" / "data_quality.yaml"
RATE_PACK = REPO_ROOT / "inputs" / "data_quality" / "rate_issue_attribution_review_pack_v1.json"
RATE_DECISION = (
    REPO_ROOT / "config" / "data_quality" / "rate_row_issue_attribution_decision_v1.yaml"
)
REGIME_DEPENDENCY = (
    REPO_ROOT / "config" / "data_quality" / "regime_label_generator_dependency_v1.yaml"
)


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _split_basis_row(value_date: str, ticker: str, adjustment_ratio: float) -> dict[str, object]:
    # Unadjusted close on the pre-split basis, adjusted close continuous at 100.
    close = 100.0 / adjustment_ratio
    return {
        "date": value_date,
        "ticker": ticker,
        "open": close,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "adj_close": 100.0,
        "volume": 1000,
    }


def _validate(tmp_path: Path, rows: list[dict[str, object]], tickers: list[str]):
    prices_path = tmp_path / "prices_daily.csv"
    rates_path = tmp_path / "rates_daily.csv"
    pd.DataFrame(rows).to_csv(prices_path, index=False)
    pd.DataFrame([{"date": "2025-11-21", "series": "DGS2", "value": 3.6}]).to_csv(
        rates_path, index=False
    )
    return validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=tickers,
        expected_rate_series=["DGS2"],
        quality_config=load_data_quality(),
        as_of=date(2025, 11, 22),
    )


def _issue(report, code: str):
    return next((issue for issue in report.issues if issue.code == code), None)


def test_googl_split_event_is_reviewed_like_goog() -> None:
    events = load_data_quality().prices.known_split_events

    # GOOGL is the only ticker added; every other ticker keeps its events.
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
    assert sum(len(items) for items in events.values()) == 12
    assert len(events["GOOGL"]) == 1
    googl = events["GOOGL"][0]
    goog = events["GOOG"][0]
    assert googl.effective_date == date(2022, 7, 18)
    assert googl.ratio == 20
    assert googl.status == "REVIEWED"
    assert googl.owner == "data_platform_owner"
    assert googl.reviewed_at == date(2026, 10, 14)
    assert googl.review_condition
    assert googl.source_name == goog.source_name
    assert googl.source_url == goog.source_url
    # GOOG's own event is unchanged.
    assert (goog.effective_date, goog.ratio, goog.reviewed_at) == (
        date(2022, 7, 18),
        20,
        date(2026, 7, 25),
    )


def test_dq_thresholds_and_policy_version_unchanged() -> None:
    raw = yaml.safe_load(DQ_POLICY.read_text(encoding="utf-8"))
    prices = raw["prices"]

    assert raw["governance"]["policy_version"] == "data_quality_cache_gate.v2"
    assert prices["suspicious_adjustment_ratio_change_abs"] == 0.25
    assert prices["known_split_match_window_days"] == 5
    assert prices["known_split_ratio_tolerance_abs"] == 0.08


def test_googl_split_jump_is_info_and_unregistered_jumps_stay_warnings(tmp_path: Path) -> None:
    rows = [
        # The 2022-07-18 GOOGL split as the daily saw it (observed factor 20).
        _split_basis_row("2022-07-15", "GOOGL", 1.0 / 20.0),
        _split_basis_row("2022-07-18", "GOOGL", 1.0),
        # A GOOGL jump a year later is not the reviewed event.
        _split_basis_row("2023-07-17", "GOOGL", 1.0),
        _split_basis_row("2023-07-18", "GOOGL", 20.0),
        _split_basis_row("2025-11-21", "GOOGL", 20.0),
        _split_basis_row("2022-07-15", "GOOG", 1.0 / 20.0),
        _split_basis_row("2022-07-18", "GOOG", 1.0),
        _split_basis_row("2025-11-21", "GOOG", 1.0),
    ]
    report = _validate(tmp_path, rows, ["GOOG", "GOOGL"])

    known = _issue(report, "prices_known_split_adjustment_ratio_jump")
    unresolved = _issue(report, "prices_adjustment_ratio_jump")
    assert known is not None and known.severity is Severity.INFO
    assert known.rows == 2
    assert "2022-07-18" in (known.sample or "")
    assert unresolved is not None and unresolved.severity is Severity.WARNING
    assert unresolved.rows == 1
    assert "2023-07-18" in (unresolved.sample or "")
    assert "2022-07-18" not in (unresolved.sample or "")


def test_googl_split_alone_leaves_no_warning(tmp_path: Path) -> None:
    rows = [
        _split_basis_row("2022-07-15", "GOOGL", 1.0 / 20.0),
        _split_basis_row("2022-07-18", "GOOGL", 1.0),
        _split_basis_row("2025-11-21", "GOOGL", 1.0),
    ]
    report = _validate(tmp_path, rows, ["GOOGL"])

    assert _issue(report, "prices_adjustment_ratio_jump") is None
    assert not [issue for issue in report.issues if issue.severity is Severity.WARNING]
    assert report.passed is True


def test_rate_review_pack_and_decision_are_bound_to_the_new_policy_bytes() -> None:
    pack = json.loads(RATE_PACK.read_text(encoding="utf-8"))
    bindings = {item["path"]: item["sha256"] for item in pack["input_bindings"]}
    decision = load_rate_row_issue_attribution_decision()
    raw_decision = yaml.safe_load(RATE_DECISION.read_text(encoding="utf-8"))

    assert bindings["config/data_quality.yaml"] == _file_sha256(DQ_POLICY)
    assert decision.review_pack_sha256 == _file_sha256(RATE_PACK)
    assert decision.review_pack_id == pack["review_pack_id"]
    assert RATE_ROW_ATTRIBUTION_REVIEW_PACK_ID == pack["review_pack_id"]
    assert raw_decision["decision_version"] == "1.0.0"
    assert raw_decision["decision_id"] == (
        "owner_decision:DATA-GOV-002C3:2026-07-28:"
        "approve_rate_row_issue_attribution_contract_wave_v1"
    )


def test_regime_label_dependency_is_bound_to_the_new_policy_bytes() -> None:
    dependency = yaml.safe_load(REGIME_DEPENDENCY.read_text(encoding="utf-8"))

    assert dependency["data_quality_policy_path"] == "config/data_quality.yaml"
    assert dependency["data_quality_policy_sha256"] == _file_sha256(DQ_POLICY)
