from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

BRANCHING_ARTIFACTS = (
    "b2_risk_heavy_window_evaluation.json",
    "b3_slow_tilt_signal_direction_audit.json",
    "b1_execution_control_adoption_review.json",
    "ablation_path_branching_decision.json",
    "b2_only_research_candidate_checkpoint.json",
    "b3_redesign_hypothesis_pack.json",
    "research_program_checkpoint_after_branching.json",
)


def test_trading_530_to_536_branching_outputs_expected_statuses() -> None:
    b2 = _read_json("b2_risk_heavy_window_evaluation.json")
    b3 = _read_json("b3_slow_tilt_signal_direction_audit.json")
    b1 = _read_json("b1_execution_control_adoption_review.json")
    branch = _read_json("ablation_path_branching_decision.json")
    b2_only = _read_json("b2_only_research_candidate_checkpoint.json")
    b3_hypotheses = _read_json("b3_redesign_hypothesis_pack.json")
    program = _read_json("research_program_checkpoint_after_branching.json")

    assert b2["status"] == "B2_RISK_OVERLAY_MIXED"
    assert b3["status"] == "B3_REDESIGN_REQUIRED"
    assert b1["status"] == "B1_OPTIONAL_WRAPPER"
    assert branch["status"] == "CONTINUE_B2_ONLY_PATH"
    assert branch["selected_branch"] == "CONTINUE_B2_ONLY_PATH"
    assert b2_only["status"] == "B2_ONLY_NEEDS_MORE_EVIDENCE"
    assert b3_hypotheses["status"] == "B3_REDESIGN_HYPOTHESES_READY"
    assert program["status"] == "CONTINUE_B2_ONLY_RESEARCH"
    assert program["recommended_next_branch"] == "CONTINUE_B2_ONLY_PATH"


def test_b2_risk_heavy_window_evaluation_includes_required_metrics() -> None:
    payload = _read_json("b2_risk_heavy_window_evaluation.json")
    windows = {row["window_id"]: row for row in payload["window_evaluations"]}

    assert set(windows) == {
        "rapid_drawdown",
        "slow_drawdown",
        "high_volatility_sideways",
        "semiconductor_correction",
        "v_shaped_recovery",
        "false_risk_off_cluster",
    }
    for row in windows.values():
        assert {
            "risk_trigger_count",
            "risk_trigger_dates",
            "exposure_scaler_changes",
            "drawdown_delta_vs_b0",
            "return_delta_vs_b0",
            "turnover_delta_vs_b0",
            "cost_delta_vs_b0",
            "false_risk_off_count",
            "re_entry_lag_days",
            "v_shaped_recovery_opportunity_cost",
        } <= set(row)

    slow = windows["slow_drawdown"]
    assert slow["risk_trigger_count"] == 33
    assert slow["drawdown_delta_vs_b0"] > 0
    assert slow["return_delta_vs_b0"] < 0
    assert slow["turnover_delta_vs_b0"] > 0
    assert slow["cost_delta_vs_b0"] > 0
    assert slow["re_entry_lag_days"] == 14
    assert payload["aggregate"]["triggered_window_count"] == 1


def test_b3_signal_direction_audit_exposes_redesign_evidence() -> None:
    payload = _read_json("b3_slow_tilt_signal_direction_audit.json")

    assert payload["wrong_tilt_dates"]
    assert payload["overweighted_underperformers"]
    assert payload["underweighted_outperformers"]
    assert set(payload["negative_windows"]) >= {
        "rapid_drawdown",
        "slow_drawdown",
        "semiconductor_correction",
    }
    diagnosis = payload["signal_normalization_diagnosis"]
    assert diagnosis["lagged"] == "LIKELY_CONTRIBUTOR"
    assert diagnosis["overreactive"] == "LIKELY_CONTRIBUTOR"
    assert diagnosis["asset_mapping_issue"] == "NOT_PROVEN"


def test_b1_branching_and_program_keep_b5_b6_v3_blocked() -> None:
    b1 = _read_json("b1_execution_control_adoption_review.json")
    branch = _read_json("ablation_path_branching_decision.json")
    b2_only = _read_json("b2_only_research_candidate_checkpoint.json")
    program = _read_json("research_program_checkpoint_after_branching.json")

    assert b1["adoption_mode"] == "optional execution wrapper"
    assert b1["when_b1_helps"]
    assert b1["when_b1_hurts"]
    assert branch["blocked_modules"] == ["B3_CURRENT_FORM", "B4_CURRENT_COMBO", "B5", "B6", "v3"]
    assert {row["branch"] for row in branch["rejected_branches"]} == {
        "REDESIGN_B3_BEFORE_COMBO",
        "DROP_B3_CURRENT_FORM",
        "RETEST_B4_AFTER_B3_REDESIGN",
        "STOP_CURRENT_ABLATION_LINE",
    }
    assert b2_only["paper_shadow_allowed"] is False
    assert branch["b5_allowed"] is False
    assert branch["b6_allowed"] is False
    assert branch["v3_allowed"] is False
    assert program["b5_allowed"] is False
    assert program["b6_allowed"] is False
    assert program["v3_allowed"] is False


def test_branching_artifacts_keep_research_only_safety_boundary() -> None:
    for file_name in BRANCHING_ARTIFACTS:
        payload = _read_json(file_name)
        safety = payload["safety_boundary"]

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["holdout_accessed"] is False
        assert payload["forbidden_outputs_absent"] is True
        assert payload["data_quality_gate"]["required_command"] == "aits validate-data"
        assert payload["data_quality_gate"]["passed"] is True
        assert safety["research_only"] is True
        assert safety["manual_review_only"] is True
        assert safety["official_target_weights"] is False
        assert safety["paper_shadow_activation"] is False
        assert safety["extended_shadow_allowed"] is False
        assert safety["broker_action_allowed"] is False
        assert safety["order_ticket_generated"] is False
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
