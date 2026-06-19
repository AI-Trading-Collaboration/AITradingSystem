from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"


def test_b1_b4_diagnosis_batch_blocks_b5_b6_when_b4_is_inconclusive() -> None:
    payloads = _build_b1_b4_diagnosis_payloads(
        sources=_sources(),
        generated_at=datetime(2026, 6, 19, tzinfo=UTC),
        data_quality_gate=_data_quality_gate(),
    )

    attribution = payloads["b1_b4_component_result_attribution"]
    drilldown = payloads["b4_interaction_inconclusive_drilldown"]
    baseline = payloads["e0_e1_baseline_consistency_audit"]
    decision = payloads["b4_next_decision_checkpoint"]

    assert attribution["status"] == "B1_B4_COMPONENT_ATTRIBUTION_READY"
    assert {row["comparison_id"] for row in attribution["comparisons"]} == {
        "B1_vs_B0",
        "B2_vs_B0",
        "B3_vs_B0",
        "B4_vs_B0",
        "B4_vs_B2",
        "B4_vs_B3",
    }
    assert all("cost_delta" in row for row in attribution["comparisons"])
    assert all("benchmark_relative_delta" in row for row in attribution["comparisons"])
    assert drilldown["status"] == "B4_REQUIRES_MORE_WINDOWS"
    assert "sample window insufficient" in drilldown["primary_root_causes"]
    assert baseline["status"] == "E0_E1_BASELINE_CONSISTENCY_PASS_WITH_LIMITATIONS"
    assert decision["status"] == "RUN_MORE_B4_WINDOWS"
    assert decision["b5_allowed"] is False
    assert decision["b6_allowed"] is False


def test_b1_b4_attribution_marks_b3_and_b4_not_independently_useful() -> None:
    payloads = _build_b1_b4_diagnosis_payloads(
        sources=_sources(),
        generated_at=datetime(2026, 6, 19, tzinfo=UTC),
        data_quality_gate=_data_quality_gate(),
    )
    usefulness = {
        row["layer_id"]: row["independently_useful_status"]
        for row in payloads["b1_b4_component_result_attribution"]["module_usefulness"]
    }

    assert usefulness["B1"] == "CONDITIONAL_MIXED_USEFULNESS"
    assert usefulness["B2"] == "NOT_PROVEN_USEFUL_IN_CURRENT_WINDOW"
    assert usefulness["B3"] == "NOT_INDEPENDENTLY_USEFUL_IN_CURRENT_WINDOW"
    assert usefulness["B4"] == "INCONCLUSIVE_NOT_INDEPENDENTLY_USEFUL"


def _build_b1_b4_diagnosis_payloads(
    *,
    sources: dict[str, dict[str, object]],
    generated_at: datetime,
    data_quality_gate: dict[str, object],
) -> dict[str, dict[str, object]]:
    from ai_trading_system.etf_portfolio.weight_research_diagnosis import (
        build_b4_inconclusive_drilldown,
        build_b4_next_decision_checkpoint,
        build_component_result_attribution,
        build_e0_e1_baseline_consistency_audit,
    )

    attribution = build_component_result_attribution(
        sources=sources,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
    )
    drilldown = build_b4_inconclusive_drilldown(
        sources=sources,
        attribution=attribution,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
    )
    baseline = build_e0_e1_baseline_consistency_audit(
        sources=sources,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
    )
    decision = build_b4_next_decision_checkpoint(
        attribution=attribution,
        drilldown=drilldown,
        baseline_audit=baseline,
        generated_at=generated_at,
        data_quality_gate=data_quality_gate,
    )
    return {
        "b1_b4_component_result_attribution": attribution,
        "b4_interaction_inconclusive_drilldown": drilldown,
        "e0_e1_baseline_consistency_audit": baseline,
        "b4_next_decision_checkpoint": decision,
    }


def _sources() -> dict[str, dict[str, object]]:
    return {
        "b0": _read_json("b0_static_strategic_baseline_result.json"),
        "b1": _read_json("b1_isolated_attribution_result.json"),
        "b2": _read_json("b2_risk_scaler_research_result.json"),
        "b3": _read_json("b3_relative_tilt_research_result.json"),
        "b4": _read_json("b4_risk_tilt_interaction_result.json"),
    }


def _read_json(name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / name).read_text(encoding="utf-8"))


def _data_quality_gate() -> dict[str, object]:
    return {
        "required_command": "aits validate-data",
        "status": "PASS_WITH_WARNINGS",
        "passed": True,
        "error_count": 0,
        "warning_count": 1,
        "info_count": 12,
        "report_path": "outputs/reports/data_quality_2026-06-19.md",
    }
