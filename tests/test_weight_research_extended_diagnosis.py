from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"


def test_trading_525_to_529_outputs_block_b5_until_b4_is_admissible() -> None:
    b2 = _read_json("b2_risk_scaler_trigger_coverage_audit.json")
    b3 = _read_json("b3_slow_tilt_negative_contribution_attribution.json")
    multi = _read_json("b1_b4_multi_window_diagnostic_expansion.json")
    synthesis = _read_json("b4_interaction_evidence_synthesis.json")
    checkpoint = _read_json("b5_admission_checkpoint.json")

    assert b2["status"] == "B2_REQUIRES_RISK_HEAVY_WINDOWS"
    assert b2["risk_trigger_count"] > 0
    assert "slow_drawdown" in b2["recommended_risk_heavy_windows"]

    assert b3["status"] == "B3_NEGATIVE_DUE_TO_SIGNAL_DIRECTION"
    assert b3["wrong_tilt_dates"]
    assert "rapid_drawdown" in b3["negative_windows"]

    assert multi["status"] == "MULTI_WINDOW_DIAGNOSTIC_COMPLETE"
    assert len(multi["window_results"]) == 7
    assert synthesis["status"] == "B4_REDUNDANT"
    assert checkpoint["status"] == "B5_ADMISSION_BLOCKED_MORE_EVIDENCE"
    assert checkpoint["b5_allowed"] is False
    assert checkpoint["b6_allowed"] is False
    assert checkpoint["v3_allowed"] is False
    assert all(row["status"] == "PASS" for row in checkpoint["hard_rule_checks"])


def test_multi_window_diagnostic_has_required_windows_and_comparisons() -> None:
    payload = _read_json("b1_b4_multi_window_diagnostic_expansion.json")
    windows = {row["window"]["window_id"]: row for row in payload["window_results"]}

    assert set(windows) == {
        "normal_uptrend",
        "rapid_drawdown",
        "slow_drawdown",
        "high_volatility_sideways",
        "v_shaped_recovery",
        "semiconductor_correction",
        "false_risk_off_cluster",
    }
    for row in windows.values():
        assert row["window"]["allowed_stage"] == "diagnostic"
        assert row["window"]["holdout_allowed"] is False
        assert {comparison["comparison_id"] for comparison in row["comparisons"]} == {
            "B1_vs_B0",
            "B2_vs_B0",
            "B3_vs_B0",
            "B4_vs_B0",
            "B4_vs_B2",
            "B4_vs_B3",
        }
        assert all("cost_delta" in comparison for comparison in row["comparisons"])
        assert all("benchmark_relative_delta" in comparison for comparison in row["comparisons"])


def test_extended_diagnosis_artifacts_keep_research_only_safety_boundary() -> None:
    for name in (
        "b2_risk_scaler_trigger_coverage_audit.json",
        "b3_slow_tilt_negative_contribution_attribution.json",
        "b1_b4_multi_window_diagnostic_expansion.json",
        "b4_interaction_evidence_synthesis.json",
        "b5_admission_checkpoint.json",
    ):
        payload = _read_json(name)
        safety = payload["safety_boundary"]

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["holdout_accessed"] is False
        assert safety["research_only"] is True
        assert safety["manual_review_only"] is True
        assert safety["official_target_weights"] is False
        assert safety["paper_shadow_activation"] is False
        assert safety["broker_action_allowed"] is False
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
