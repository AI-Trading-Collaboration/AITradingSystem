from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.reader_brief import (
    build_reader_brief_payload,
    render_reader_brief_html,
)


def test_reader_brief_trading_engine_validation_path(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "decision_snapshot_2026-05-04.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-05-04",
                "signal_date": "2026-05-04",
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "start_date": "2022-12-01",
                },
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 66.0,
                    "confidence_level": "medium",
                    "components": [
                        {
                            "component": "trend",
                            "score": 72.0,
                            "weight": 25.0,
                            "coverage": 1.0,
                            "confidence": 0.9,
                        }
                    ],
                },
                "positions": {
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.4,
                        "label": "受限中配",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                    },
                    "position_gates": [
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "max_position": 0.4,
                            "triggered": True,
                        }
                    ],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_reader_brief_payload(
        as_of=date(2026, 5, 4),
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
        calculation_explainers_path=tmp_path / "missing_calculation_explainers.json",
        daily_decision_summary_path=tmp_path / "missing_daily_decision_summary.json",
        evidence_dashboard_json_path=tmp_path / "missing_evidence_dashboard.json",
        daily_task_dashboard_json_path=tmp_path / "missing_daily_task_dashboard.json",
        daily_report_path=tmp_path / "missing_daily_score.md",
        trace_bundle_path=tmp_path / "missing_trace.json",
        score_change_attribution_path=tmp_path / "missing_score_change.json",
        market_panel_path=tmp_path / "missing_market_panel.json",
        research_governance_summary_path=tmp_path / "missing_research_governance.json",
        report_index_path=tmp_path / "missing_report_index.json",
        documentation_contract_path=tmp_path / "missing_documentation_contract.json",
    )

    assert payload["report_type"] == "reader_brief"
    assert payload["production_effect"] == "none"
    assert payload["report_navigation_groups"]["status"] == "AVAILABLE"


def test_reader_brief_displays_etf_backtest_summary(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(reader_brief, "PROJECT_ROOT", tmp_path)
    snapshot_path = _write_decision_snapshot(tmp_path)
    summary_dir = (
        tmp_path / "reports" / "etf_portfolio" / "backtests" / "etf-backtest-20260504T000000Z"
    )
    summary_dir.mkdir(parents=True)
    (summary_dir / "summary.json").write_text(
        json.dumps(
            {
                "data_quality_status": "PASS",
                "first_signal_date": "2026-01-02",
                "last_signal_date": "2026-03-31",
                "row_count": 60,
                "standardized_metrics": {
                    "primary_benchmark_id": "B001",
                    "start_date": "2026-01-02",
                    "end_date": "2026-03-31",
                    "trading_days": 60,
                    "total_return": 0.12,
                    "CAGR": 0.58,
                    "max_drawdown": -0.04,
                    "Sharpe": 1.2,
                    "benchmark_excess_return": 0.03,
                    "benchmark_drawdown_reduction": 0.02,
                    "metric_null_reasons": {},
                },
                "monthly_returns": [
                    {
                        "month": "2026-01",
                        "strategy_return": 0.03,
                        "benchmark_return": 0.02,
                        "excess_return": 0.01,
                        "max_drawdown_in_month": -0.01,
                        "average_equity_exposure": 0.8,
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = build_reader_brief_payload(
        as_of=date(2026, 5, 4),
        reports_dir=tmp_path,
        decision_snapshot_path=snapshot_path,
    )
    html = render_reader_brief_html(payload)

    assert payload["etf_backtest_summary"]["status"] == "AVAILABLE"
    assert payload["etf_backtest_summary"]["primary_benchmark_id"] == "B001"
    assert payload["etf_backtest_summary"]["benchmark_excess_return"] == "0.0300"
    assert "ETF Backtest Summary" in html
    assert "benchmark_excess_return" in html


def _write_decision_snapshot(tmp_path: Path) -> Path:
    snapshot_path = tmp_path / "decision_snapshot_2026-05-04.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-05-04",
                "signal_date": "2026-05-04",
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "start_date": "2022-12-01",
                },
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 66.0,
                    "confidence_level": "medium",
                    "components": [],
                },
                "positions": {
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.4,
                        "label": "受限中配",
                    },
                    "final_total_risk_asset_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                    },
                    "position_gates": [],
                },
                "quality": {"market_data_status": "PASS"},
                "manual_review": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return snapshot_path
