from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from ai_trading_system.data.quality import DataFileSummary, DataQualityReport
from ai_trading_system.high_intensity_risk_cap_actual_path_outcome_binder import (
    build_high_intensity_actual_path_data_quality_report,
    build_high_intensity_outcome_coverage_report,
)


def _quality_report() -> DataQualityReport:
    return DataQualityReport(
        checked_at=datetime(2023, 3, 10, tzinfo=UTC),
        as_of=date(2023, 3, 10),
        price_summary=DataFileSummary(path=Path("prices.csv"), exists=True, rows=10, sha256="p"),
        rate_summary=DataFileSummary(path=Path("rates.csv"), exists=True, rows=1, sha256="r"),
        expected_price_tickers=("QQQ",),
        expected_rate_series=(),
    )


def test_outcome_coverage_statuses_full_partial_and_data_gaps() -> None:
    base = {
        "event_id": "evt1",
        "event_cluster_id": "cl1",
        "target_asset": "QQQ",
        "horizon": "1d",
    }
    price_matrix = pd.DataFrame({"QQQ": [100, 101]}, index=pd.bdate_range("2023-01-02", periods=2))

    full = build_high_intensity_outcome_coverage_report(
        event_log=[base],
        cluster_registry=[{"event_cluster_id": "cl1", "target_asset": "QQQ"}],
        pending_registry=[base],
        event_matrix=[{**base, "outcome_binding_status": "OUTCOME_BOUND"}],
        cluster_matrix=[{"cluster_outcome_binding_status": "OUTCOME_BOUND"}],
        latest_market_data_date=date(2023, 3, 10),
        price_matrix=price_matrix,
    )
    partial = build_high_intensity_outcome_coverage_report(
        event_log=[base],
        cluster_registry=[{"event_cluster_id": "cl1", "target_asset": "QQQ"}],
        pending_registry=[base],
        event_matrix=[{**base, "outcome_binding_status": "OUTCOME_NOT_DUE"}],
        cluster_matrix=[{"cluster_outcome_binding_status": "OUTCOME_NOT_DUE"}],
        latest_market_data_date=date(2023, 1, 3),
        price_matrix=price_matrix,
    )
    gap = build_high_intensity_outcome_coverage_report(
        event_log=[{**base, "target_asset": "MISSING"}],
        cluster_registry=[{"event_cluster_id": "cl1", "target_asset": "MISSING"}],
        pending_registry=[base],
        event_matrix=[{**base, "outcome_binding_status": "OUTCOME_BLOCKED_MARKET_DATA"}],
        cluster_matrix=[{"cluster_outcome_binding_status": "OUTCOME_BLOCKED_MARKET_DATA"}],
        latest_market_data_date=date(2023, 3, 10),
        price_matrix=price_matrix,
    )

    assert full["coverage_status"] == "FULL_COVERAGE"
    assert partial["coverage_status"] == "PARTIAL_COVERAGE_WITH_NOT_DUE_HORIZONS"
    assert gap["coverage_status"] == "PARTIAL_COVERAGE_WITH_DATA_GAPS"


def test_actual_path_data_quality_detects_duplicate_and_safety_violation() -> None:
    event_rows = [
        {
            "outcome_id": "dup",
            "target_asset": "QQQ",
            "horizon": "1d",
            "outcome_binding_status": "OUTCOME_BOUND",
            "promotion_allowed": False,
        },
        {
            "outcome_id": "dup",
            "target_asset": "QQQ",
            "horizon": "1d",
            "outcome_binding_status": "OUTCOME_BOUND",
            "promotion_allowed": True,
        },
    ]

    report = build_high_intensity_actual_path_data_quality_report(
        event_log=[{"target_asset": "QQQ"}],
        cluster_registry=[{"target_asset": "QQQ"}],
        pending_registry=[{}],
        outcome_schedule=[{}],
        event_matrix=event_rows,
        cluster_matrix=[],
        trigger_context=[],
        price_matrix=pd.DataFrame({"QQQ": [100]}),
        quality_report=_quality_report(),
        quality_report_path=Path("quality.md"),
    )

    assert report["duplicate_outcome_id_count"] == 1
    assert report["safety_violation_count"] == 1
    assert report["data_quality_status"] == "FAIL"
