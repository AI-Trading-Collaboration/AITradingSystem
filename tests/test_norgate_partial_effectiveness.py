from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.vendor_adapters import norgate_partial_effectiveness as partial
from ai_trading_system.vendor_adapters.norgate_connector import NorgateEnvironment
from ai_trading_system.vendor_adapters.norgate_partial_effectiveness import (
    run_norgate_trial_partial_effectiveness,
)


def test_norgate_partial_effectiveness_fails_closed_without_environment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    def fake_environment(self) -> NorgateEnvironment:  # noqa: ARG001
        return NorgateEnvironment(
            module_present=False,
            module_version="",
            database_available=False,
            database_names=(),
            status="NORGATE_ENV_MISSING_PACKAGE",
            warnings=("missing",),
            errors=("ModuleNotFoundError",),
        )

    monkeypatch.setattr(partial.NorgateConnector, "inspect_environment", fake_environment)

    final = run_norgate_trial_partial_effectiveness(
        output_root=tmp_path / "outputs",
        docs_root=tmp_path / "docs",
        inputs_root=tmp_path / "inputs",
        policy_path=_policy_path(),
    )

    assert final["status"] == "NORGATE_2Y_PARTIAL_EFFECTIVENESS_BLOCKED_OR_WEAK"
    assert final["source_engineering_useful"] is False
    assert final["primary_window_validated"] is False
    assert final["promotion_allowed"] is False
    assert final["paper_shadow_allowed"] is False
    assert final["production_allowed"] is False
    assert final["broker_action"] == "none"
    assert (
        tmp_path / "outputs" / "norgate_trial_partial_effectiveness_coverage_report.json"
    ).exists()


def test_partial_effectiveness_conclusion_separates_2y_evidence_from_primary_window() -> None:
    final = partial._build_conclusion_matrix(
        coverage={
            "engineering_validated": True,
            "earliest_price_date": "2024-06-28",
            "latest_price_date": "2026-06-26",
            "member_day_coverage_ratio": 0.99,
            "missing_price_ratio": 0.01,
            "failed_join_count": 3,
        },
        features={"feature_numeric_validated": True},
        local_signal={"local_signal_evidence": "moderate"},
        policy=_policy_payload(),
    )

    assert final["source_engineering_useful"] is True
    assert final["source_feature_useful_2y"] is True
    assert final["purchase_platinum_evidence_strength"] == "moderate"
    assert final["primary_window_validated"] is False
    assert final["model_ready_for_2021_primary_window"] is False
    assert final["reopen_gate_allowed"] is False
    assert final["promotion_allowed"] is False


def test_partial_effectiveness_coverage_is_summary_only() -> None:
    dates = pd.to_datetime(["2026-06-24", "2026-06-25", "2026-06-26"])
    qqq = pd.DataFrame({"Close": [100.0, 101.0, 102.0]}, index=dates)
    member_a = pd.DataFrame({"Close": [10.0, 11.0, 12.0]}, index=dates)
    member_b = pd.DataFrame({"Close": [20.0, 21.0]}, index=dates[:2])
    coverage = partial._build_coverage_report(
        environment=NorgateEnvironment(
            module_present=True,
            module_version="1.0.74",
            database_available=True,
            database_names=("US Equities",),
            status="NORGATE_ENV_READY",
            warnings=(),
            errors=(),
        ),
        index_id="$NDX",
        qqq_prices=qqq,
        membership_by_symbol={
            "AAA": {dates[0], dates[1], dates[2]},
            "BBB": {dates[0], dates[1], dates[2]},
        },
        price_frames={"AAA": member_a, "BBB": member_b},
        trading_dates=list(dates),
        membership_scan_symbol_count=2,
        membership_scan_failure_count=0,
        policy=_policy_payload(),
    )

    assert coverage["total_member_days"] == 6
    assert coverage["covered_member_days"] == 5
    assert coverage["failed_join_count"] == 1
    assert coverage["raw_member_symbols_committed"] is False
    assert "membership_by_symbol" not in coverage
    assert coverage["historical_member_symbols_hash"]


def test_local_signal_report_keeps_safety_boundary() -> None:
    dates = pd.bdate_range("2025-01-02", periods=140)
    qqq = pd.DataFrame({"Close": [100.0 + index for index in range(140)]}, index=dates)
    qqq["return_1d"] = qqq["Close"].pct_change()
    rows: list[dict[str, Any]] = []
    for index, day in enumerate(dates):
        rows.append(
            {
                "date": day.date().isoformat(),
                "pct_above_ma50": index / 139,
                "breadth_momentum": (index / 139) - ((index - 20) / 139)
                if index >= 20
                else None,
                "pct_above_ma20": index / 139,
                "pct_above_ma200": None,
            }
        )

    report = partial._build_local_signal_report(
        feature_rows=rows,
        qqq_prices=qqq,
        coverage={"feature_numeric_validated": True},
        policy=_policy_payload(),
    )

    assert report["status"] == "NORGATE_2Y_LOCAL_SIGNAL_REPORT_READY"
    assert report["primary_window_validated"] is False
    assert report["promotion_allowed"] is False
    assert len(report["breadth_bucket_vs_forward_return"]) == 3


def _policy_payload() -> dict[str, Any]:
    return {
        "policy_id": "norgate_trial_partial_effectiveness_policy_v1",
        "coverage_policy": {
            "min_member_day_coverage_ratio_for_feature_numeric_validated": 0.5,
            "min_feature_days_for_local_signal_review": 1,
        },
        "baseline_proxy_policy": {"qqq_ma_window": 20},
    }


def _policy_path() -> Path:
    return partial.DEFAULT_PARTIAL_EFFECTIVENESS_POLICY_PATH
