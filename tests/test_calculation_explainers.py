from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports.calculation_explainers import (
    build_calculation_explainers_payload,
    load_metric_explainer_registry,
)


def test_calculation_explainers_payload_binds_formulas_to_snapshot_values(
    tmp_path: Path,
) -> None:
    snapshot_path = _write_decision_snapshot(tmp_path)
    scores_path = tmp_path / "scores_daily.csv"
    scores_path.write_text("as_of,component,score\n2026-05-04,overall,73\n", encoding="utf-8")

    payload = build_calculation_explainers_payload(
        as_of=date(2026, 5, 4),
        decision_snapshot_path=snapshot_path,
        scores_daily_path=scores_path,
    )

    assert payload["status"] == "PASS"
    assert payload["production_effect"] == "none"
    assert payload["source_inputs"]["decision_snapshot"]["exists"] is True
    assert payload["source_inputs"]["scores_daily"]["exists"] is True
    overall = payload["metrics"]["overall_score"]
    assert overall["value"] == 73.0
    assert overall["formula"] == "sum(component_score[module] * effective_weight[module])"
    assert overall["input_values"][0]["component"] == "trend"
    assert overall["input_values"][0]["effective_weight"] == 0.25
    assert overall["input_values"][0]["contribution_to_overall_score"] == 18.0
    final_max = payload["metrics"]["final_position_max"]
    assert final_max["value"] == 0.4
    assert final_max["input_values"]["binding_gate"]["gate_id"] == "valuation"
    assert payload["metrics"]["rank_ic"]["status"] == "DEFINITION_ONLY"
    assert payload["metrics"]["rank_ic"]["limitations"] == ["not_present_in_decision_snapshot"]


def test_reports_calculation_explainers_cli_writes_json(tmp_path: Path) -> None:
    snapshot_path = _write_decision_snapshot(tmp_path)
    output_path = tmp_path / "calculation_explainers_2026-05-04.json"

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "calculation-explainers",
            "--as-of",
            "2026-05-04",
            "--decision-snapshot-path",
            str(snapshot_path),
            "--scores-daily-path",
            str(tmp_path / "missing_scores.csv"),
            "--output-path",
            str(output_path),
        ],
        env={"COLUMNS": "160"},
        terminal_width=160,
    )

    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["status"] == "PASS_WITH_WARNINGS"
    assert payload["warnings"] == [f"scores_daily_missing:{tmp_path / 'missing_scores.csv'}"]
    assert payload["metrics"]["final_position_band"]["value"]["label"] == "受限中配"
    assert "计算解释：PASS_WITH_WARNINGS" in result.output


def test_metric_explainer_registry_requires_required_metrics(tmp_path: Path) -> None:
    registry_path = tmp_path / "metric_explainers.yaml"
    registry_path.write_text(
        """
schema_version: 1
metrics:
  overall_score:
    label: Overall Score
    audience_label: 综合分数
    formula: sum(component_score * effective_weight)
    input_fields:
      - component_score
    pit_policy: test
""",
        encoding="utf-8",
    )

    try:
        load_metric_explainer_registry(registry_path)
    except ValueError as exc:
        assert "missing required metrics" in str(exc)
        assert "component_score" in str(exc)
    else:
        raise AssertionError("registry validation should reject missing required metrics")


def _write_decision_snapshot(tmp_path: Path) -> Path:
    path = tmp_path / "decision_snapshot_2026-05-04.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": "decision_snapshot:2026-05-04",
                "signal_date": "2026-05-04",
                "scores": {
                    "overall_score": 73.0,
                    "confidence_score": 66.0,
                    "confidence_level": "medium",
                    "confidence_reasons": ["data coverage medium"],
                    "components": [
                        {
                            "component": "trend",
                            "score": 72.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.9,
                            "reason": "趋势支持。",
                        },
                        {
                            "component": "fundamentals",
                            "score": 68.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 0.8,
                            "confidence": 0.75,
                            "reason": "基本面支持。",
                        },
                        {
                            "component": "macro_liquidity",
                            "score": 70.0,
                            "weight": 25.0,
                            "source_type": "hard_data",
                            "coverage": 1.0,
                            "confidence": 0.8,
                            "reason": "宏观中性偏正。",
                        },
                        {
                            "component": "valuation",
                            "score": 82.0,
                            "weight": 25.0,
                            "source_type": "manual_input",
                            "coverage": 0.7,
                            "confidence": 0.6,
                            "reason": "估值偏贵。",
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
                        "max_position": 0.4,
                        "label": "受限中配",
                    },
                    "macro_risk_asset_budget": {
                        "level": "neutral",
                        "triggered": False,
                        "source": "portfolio_policy",
                        "reasons": ["宏观预算中性。"],
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
                            "max_position": 0.4,
                            "triggered": True,
                            "reason": "估值分位过高。",
                        },
                    ],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path
