from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from ai_trading_system.fundamentals.sec_pit_baseline_coverage import (
    BASELINE_COVERAGE_BY_DATE_COLUMNS,
    BASELINE_COVERAGE_BY_TICKER_COLUMNS,
    BASELINE_GAP_COLUMNS,
    run_sec_pit_baseline_coverage_audit,
)


def test_sec_pit_baseline_coverage_writes_expected_artifacts(tmp_path: Path) -> None:
    feature_panel = _write_feature_panel(tmp_path)
    baseline = _write_baseline(tmp_path)

    artifacts = run_sec_pit_baseline_coverage_audit(
        start=date(2023, 1, 2),
        end=date(2023, 1, 3),
        baseline_score_path=baseline,
        feature_panel_path=feature_panel,
        output_dir=tmp_path / "outputs" / "sec_pit_baseline_coverage",
    )

    summary = json.loads(artifacts.summary_json_path.read_text(encoding="utf-8"))
    by_ticker = pd.read_csv(artifacts.by_ticker_path)
    by_date = pd.read_csv(artifacts.by_date_path)
    gaps = pd.read_csv(artifacts.gap_path)
    markdown = artifacts.summary_markdown_path.read_text(encoding="utf-8")

    assert artifacts.status == "OK"
    assert summary["report_type"] == "sec_pit_baseline_coverage"
    assert summary["coverage_status"] == "OK"
    assert summary["expected_rows"] == 4
    assert summary["actual_rows"] == 4
    assert summary["coverage_ratio"] == 1.0
    assert tuple(by_ticker.columns) == BASELINE_COVERAGE_BY_TICKER_COLUMNS
    assert tuple(by_date.columns) == BASELINE_COVERAGE_BY_DATE_COLUMNS
    assert tuple(gaps.columns) == BASELINE_GAP_COLUMNS
    assert gaps.empty
    assert "# SEC PIT Baseline Coverage Summary" in markdown


def test_sec_pit_baseline_coverage_reports_missing_and_degraded_rows(
    tmp_path: Path,
) -> None:
    feature_panel = _write_feature_panel(tmp_path)
    baseline = tmp_path / "scores_daily.csv"
    pd.DataFrame(
        [
            {
                "decision_date": "2023-01-02",
                "ticker": "NVDA",
                "baseline_score": 70.0,
                "baseline_rank": 1,
                "baseline_action": "WATCH",
                "score_completeness_ratio": 0.5,
            },
            {
                "decision_date": "2023-01-02",
                "ticker": "MSFT",
                "baseline_score": "",
                "baseline_rank": "",
                "baseline_action": "",
                "score_completeness_ratio": 1.0,
            },
        ]
    ).to_csv(baseline, index=False)

    artifacts = run_sec_pit_baseline_coverage_audit(
        start=date(2023, 1, 2),
        end=date(2023, 1, 3),
        baseline_score_path=baseline,
        feature_panel_path=feature_panel,
        output_dir=tmp_path / "coverage",
    )

    summary = json.loads(artifacts.summary_json_path.read_text(encoding="utf-8"))
    gaps = pd.read_csv(artifacts.gap_path)

    assert artifacts.status == "LIMITED_COVERAGE"
    assert summary["coverage_status"] == "LIMITED_COVERAGE"
    assert summary["missing_rows"] == 2
    assert set(gaps["gap_type"]) == {
        "LOW_COMPLETENESS",
        "MISSING_ACTION",
        "MISSING_BASELINE_RANK",
        "MISSING_BASELINE_SCORE",
        "MISSING_SCORE_ROW",
    }


def test_sec_pit_baseline_coverage_is_deterministic(tmp_path: Path) -> None:
    feature_panel = _write_feature_panel(tmp_path)
    baseline = _write_baseline(tmp_path)
    output_dir = tmp_path / "coverage"

    first = run_sec_pit_baseline_coverage_audit(
        start=date(2023, 1, 2),
        end=date(2023, 1, 3),
        baseline_score_path=baseline,
        feature_panel_path=feature_panel,
        output_dir=output_dir,
    )
    first_summary = first.summary_json_path.read_text(encoding="utf-8")
    first_gap = first.gap_path.read_text(encoding="utf-8")
    second = run_sec_pit_baseline_coverage_audit(
        start=date(2023, 1, 2),
        end=date(2023, 1, 3),
        baseline_score_path=baseline,
        feature_panel_path=feature_panel,
        output_dir=output_dir,
    )

    assert second.summary_json_path.read_text(encoding="utf-8") == first_summary
    assert second.gap_path.read_text(encoding="utf-8") == first_gap


def _write_feature_panel(tmp_path: Path) -> Path:
    path = tmp_path / "sec_pit_feature_panel.csv"
    rows = []
    for decision_date in ("2023-01-02", "2023-01-03"):
        for ticker in ("NVDA", "MSFT"):
            for feature_id in ("capex_intensity", "gross_margin"):
                rows.append(
                    {
                        "decision_date": decision_date,
                        "ticker": ticker,
                        "feature_id": feature_id,
                        "feature_value": 1.0,
                    }
                )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


def _write_baseline(tmp_path: Path) -> Path:
    path = tmp_path / "scores_daily.csv"
    rows = []
    for decision_date in ("2023-01-02", "2023-01-03"):
        for rank, ticker in enumerate(("NVDA", "MSFT"), start=1):
            rows.append(
                {
                    "decision_date": decision_date,
                    "ticker": ticker,
                    "baseline_score": 70.0 - rank,
                    "baseline_rank": rank,
                    "baseline_action": "WATCH",
                    "score_completeness_ratio": 1.0,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
