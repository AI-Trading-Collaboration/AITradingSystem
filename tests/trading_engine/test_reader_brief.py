from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from ai_trading_system.reports.reader_brief import build_reader_brief_payload


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
