from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.score_change_attribution import (
    build_score_change_attribution_payload,
    render_score_change_attribution_markdown,
)


def test_score_change_attribution_decomposes_snapshot_deltas(tmp_path: Path) -> None:
    previous_path = _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 3),
        overall_score=70.0,
        confidence_score=60.0,
        final_max=0.50,
        valuation_cap=0.50,
        trend_score=65.0,
        trend_weight=25.0,
        valuation_score=80.0,
        valuation_weight=25.0,
        market_data_status="PASS",
    )
    current_path = _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 4),
        overall_score=73.0,
        confidence_score=66.0,
        final_max=0.40,
        valuation_cap=0.40,
        trend_score=72.0,
        trend_weight=30.0,
        valuation_score=82.0,
        valuation_weight=20.0,
        market_data_status="PASS_WITH_WARNINGS",
    )

    payload = build_score_change_attribution_payload(
        as_of=date(2026, 5, 4),
        decision_snapshot_path=current_path,
        previous_decision_snapshot_path=previous_path,
    )
    markdown = render_score_change_attribution_markdown(payload)

    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["current_date"] == "2026-05-04"
    assert payload["previous_date"] == "2026-05-03"
    assert payload["comparison_window"]["previous_signal_date"] == "2026-05-03"
    assert payload["overall_score_current"] == 73.0
    assert payload["overall_score_previous"] == 70.0
    assert payload["overall_score_delta"]["delta"] == 3.0
    assert payload["final_position_current"]["max"] == 0.40
    assert payload["final_position_previous"]["max"] == 0.50
    assert payload["final_position_delta"]["max_delta"] == pytest.approx(-0.1)
    assert payload["binding_gate_current"]["gate_id"] == "valuation"
    assert payload["binding_gate_previous"]["gate_id"] == "valuation"
    assert payload["binding_gate_changed"] is False
    assert payload["manual_review_count_delta"] == 0
    trend = next(row for row in payload["component_attribution"] if row["component"] == "trend")
    assert trend["score_delta"] == 7.0
    assert trend["effective_weight_delta"] == pytest.approx(0.05)
    assert trend["score_delta_effect"] == pytest.approx(1.75)
    assert trend["weight_delta_effect"] == pytest.approx(3.25)
    assert trend["interaction_effect"] == pytest.approx(0.35)
    assert trend["contribution_delta"] == pytest.approx(5.35)
    valuation_gate = next(
        row for row in payload["gate_attribution"] if row["gate_id"] == "valuation"
    )
    assert valuation_gate["cap_delta"] == pytest.approx(-0.1)
    assert "CAP_CHANGED" in valuation_gate["change_flags"]
    assert payload["component_score_deltas"][0]["component"] == "trend"
    assert payload["component_contribution_deltas"][0]["component"] == "trend"
    assert payload["gate_state_changes"][0]["gate_id"] == "valuation"
    assert payload["data_quality_attribution"]["market_data_status_changed"] is True
    assert payload["data_quality_status_delta"]["market_data_status_changed"] is True
    assert payload["top_changes"]["positive_contribution_drivers"][0]["component"] == "trend"
    assert payload["top_positive_change_drivers"][0]["component"] == "trend"
    assert payload["top_negative_change_drivers"]
    assert "Score Change Attribution 2026-05-04" in markdown
    assert "production_effect=none" in markdown
    assert "## 读者解释" in markdown
    assert "今天变化主要来自" in markdown
    assert "## Component Attribution" in markdown


def test_reports_score_change_attribution_cli_writes_markdown_and_json(tmp_path: Path) -> None:
    _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 3),
        overall_score=70.0,
        confidence_score=60.0,
        final_max=0.50,
        valuation_cap=0.50,
        trend_score=65.0,
        trend_weight=25.0,
        valuation_score=80.0,
        valuation_weight=25.0,
        market_data_status="PASS",
    )
    _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 4),
        overall_score=73.0,
        confidence_score=66.0,
        final_max=0.40,
        valuation_cap=0.40,
        trend_score=72.0,
        trend_weight=30.0,
        valuation_score=82.0,
        valuation_weight=20.0,
        market_data_status="PASS",
    )
    markdown_path = tmp_path / "score_change_attribution_2026-05-04.md"
    json_path = tmp_path / "score_change_attribution_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "score-change-attribution",
            "--date",
            "2026-05-04",
            "--snapshot-dir",
            str(tmp_path),
            "--output-path",
            str(markdown_path),
            "--json-output-path",
            str(json_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    assert "Score change attribution：PASS" in result.output
    assert "不重算 score" in result.output
    assert markdown_path.exists()
    assert json_path.exists()
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["comparison_window"]["previous_signal_date"] == "2026-05-03"
    assert payload["source_inputs"]["previous_decision_snapshot"]["exists"] is True


def test_reports_score_change_attribution_cli_supports_latest(tmp_path: Path) -> None:
    _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 3),
        overall_score=70.0,
        confidence_score=60.0,
        final_max=0.50,
        valuation_cap=0.50,
        trend_score=65.0,
        trend_weight=25.0,
        valuation_score=80.0,
        valuation_weight=25.0,
        market_data_status="PASS",
    )
    _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 4),
        overall_score=73.0,
        confidence_score=66.0,
        final_max=0.40,
        valuation_cap=0.40,
        trend_score=72.0,
        trend_weight=30.0,
        valuation_score=82.0,
        valuation_weight=20.0,
        market_data_status="PASS",
    )
    json_path = tmp_path / "score_change_attribution_latest.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "score-change-attribution",
            "--latest",
            "--snapshot-dir",
            str(tmp_path),
            "--output-path",
            str(tmp_path / "score_change_attribution_latest.md"),
            "--json-output-path",
            str(json_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["current_date"] == "2026-05-04"
    assert payload["previous_date"] == "2026-05-03"


def test_score_change_attribution_missing_previous_is_insufficient_data(
    tmp_path: Path,
) -> None:
    current_path = _write_decision_snapshot(
        tmp_path,
        date(2026, 5, 4),
        overall_score=73.0,
        confidence_score=66.0,
        final_max=0.40,
        valuation_cap=0.40,
        trend_score=72.0,
        trend_weight=30.0,
        valuation_score=82.0,
        valuation_weight=20.0,
        market_data_status="PASS",
    )

    payload = build_score_change_attribution_payload(
        as_of=date(2026, 5, 4),
        decision_snapshot_path=current_path,
        snapshot_dir=tmp_path,
    )

    assert payload["status"] == "INSUFFICIENT_DATA"
    assert payload["component_attribution"] == []
    assert payload["component_score_deltas"] == []
    assert payload["previous_date"] is None
    assert payload["production_effect"] == "none"
    assert payload["warnings"] == ["previous_decision_snapshot_missing"]


def _write_decision_snapshot(
    tmp_path: Path,
    signal_date: date,
    *,
    overall_score: float,
    confidence_score: float,
    final_max: float,
    valuation_cap: float,
    trend_score: float,
    trend_weight: float,
    valuation_score: float,
    valuation_weight: float,
    market_data_status: str,
) -> Path:
    path = tmp_path / f"decision_snapshot_{signal_date.isoformat()}.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_id": f"decision_snapshot:{signal_date.isoformat()}",
                "signal_date": signal_date.isoformat(),
                "market_regime": {
                    "regime_id": "ai_after_chatgpt",
                    "anchor_date": "2022-11-30",
                    "start_date": "2022-12-01",
                },
                "scores": {
                    "overall_score": overall_score,
                    "confidence_score": confidence_score,
                    "confidence_level": "medium",
                    "components": [
                        {
                            "component": "trend",
                            "score": trend_score,
                            "weight": trend_weight,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.90,
                            "reason": "趋势变化。",
                        },
                        {
                            "component": "fundamentals",
                            "score": 68.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 0.8,
                            "confidence": 0.75,
                            "reason": "基本面稳定。",
                        },
                        {
                            "component": "macro_liquidity",
                            "score": 70.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.80,
                            "reason": "宏观中性。",
                        },
                        {
                            "component": "valuation",
                            "score": valuation_score,
                            "weight": valuation_weight,
                            "source_type": "manual_input",
                            "coverage": 0.7,
                            "confidence": 0.60,
                            "reason": "估值拥挤。",
                        },
                    ],
                },
                "positions": {
                    "model_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.6,
                        "label": "中高配",
                    },
                    "confidence_adjusted_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": 0.5,
                        "label": "置信度受限",
                    },
                    "final_risk_asset_ai_band": {
                        "min_position": 0.4,
                        "max_position": final_max,
                        "label": "gate 后仓位",
                    },
                    "position_gates": [
                        {
                            "gate_id": "score_model",
                            "label": "评分模型仓位",
                            "source": "weighted_score_model",
                            "max_position": 0.6,
                            "triggered": True,
                            "reason": "score band cap",
                        },
                        {
                            "gate_id": "valuation",
                            "label": "估值拥挤",
                            "source": "valuation_review",
                            "max_position": valuation_cap,
                            "triggered": True,
                            "reason": "估值分位过高。",
                        },
                    ],
                },
                "quality": {
                    "market_data_status": market_data_status,
                    "market_data_error_count": 0,
                    "market_data_warning_count": 1 if market_data_status != "PASS" else 0,
                    "feature_status": "PASS",
                    "sec_feature_status": "PASS",
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path
