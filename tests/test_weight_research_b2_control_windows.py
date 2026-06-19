from __future__ import annotations

import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RESEARCH_DIR = PROJECT_ROOT / "docs" / "research"

B2_CONTROL_ARTIFACTS = (
    "b2_control_window_rerun_contract.json",
    "b2_control_window_rerun.json",
    "b2_no_trigger_correctness_review.json",
    "b2_full_diagnostic_with_control_windows.json",
    "b2_only_research_gate_v3.json",
    "b2_path_decision_snapshot.json",
)


def test_trading_575_to_580_outputs_expected_statuses() -> None:
    expected = {
        "b2_control_window_rerun_contract.json": (
            "B2_CONTROL_WINDOW_RERUN_CONTRACT_READY"
        ),
        "b2_control_window_rerun.json": "B2_CONTROL_RERUN_COMPLETE",
        "b2_no_trigger_correctness_review.json": "B2_NO_TRIGGER_CORRECTNESS_PASS",
        "b2_full_diagnostic_with_control_windows.json": "B2_FULL_DIAGNOSTIC_COMPLETE",
        "b2_only_research_gate_v3.json": "B2_ONLY_NEEDS_MORE_EVIDENCE",
        "b2_path_decision_snapshot.json": "CONTINUE_B2_ONLY_RESEARCH",
    }

    for file_name, status in expected.items():
        assert _read_json(file_name)["status"] == status


def test_control_window_contract_requires_independent_b2_rerun_outputs() -> None:
    contract = _read_json("b2_control_window_rerun_contract.json")

    assert [row["window_id"] for row in contract["control_windows"]] == [
        "normal_uptrend_control",
        "calm_market_control",
    ]
    assert contract["expected_b2_behavior"] == [
        "no trigger or minimal trigger",
        "no unnecessary exposure reduction",
        "no false risk-off cluster",
        "no excess turnover",
    ]
    assert set(contract["required_outputs"]) == {
        "risk_signal_values",
        "trigger_count",
        "trigger_dates",
        "exposure_scaler_changes",
        "false_risk_off_count",
        "no_trigger_reference",
        "control_window_status",
    }
    assert "not by reusing generic market references" in contract["independence_requirement"]
    assert all(row["status"] == "PASS" for row in contract["validation"])


def test_control_window_rerun_is_independent_b2_only_and_inactive() -> None:
    rerun = _read_json("b2_control_window_rerun.json")

    assert rerun["b2_logic_only"] is True
    assert rerun["B3_used"] is False
    assert rerun["B4_B5_B6_v3_used"] is False
    assert rerun["untouched_holdout_used"] is False
    assert rerun["aggregate"] == {
        "false_risk_off_count": 0,
        "trigger_count": 0,
        "unnecessary_exposure_reduction_count": 0,
        "window_count": 2,
    }

    for row in rerun["window_results"]:
        assert row["independent_b2_rerun"] is True
        assert row["control_window_status"] == "B2_CONTROL_NO_TRIGGER_PASS"
        assert row["trigger_count"] == 0
        assert row["trigger_dates"] == []
        assert row["exposure_scaler_changes"] == []
        assert row["false_risk_off_count"] == 0
        assert row["risk_state_counts"] == {"NORMAL": 144}
        assert len(row["risk_signal_values"]) == 144
        assert {item["risk_state"] for item in row["risk_signal_values"]} == {"NORMAL"}
        assert row["no_trigger_reference"]["expected_exposure_scaler"] == 1.0
        assert row["no_trigger_reference"]["risk_off_score_max"] == 45.0
        assert "reports\\etf_portfolio\\weight_research" in row["component_artifacts"][
            "risk_signal_artifact"
        ]
        assert "reports\\etf_portfolio\\weight_research" in row["component_artifacts"][
            "target_path_artifact"
        ]
        assert len(row["signal_checksum"]) == 64
        assert len(row["target_path_checksum"]) == 64


def test_no_trigger_review_completes_full_diagnostic_but_gate_stays_conservative() -> None:
    review = _read_json("b2_no_trigger_correctness_review.json")
    full = _read_json("b2_full_diagnostic_with_control_windows.json")
    gate = _read_json("b2_only_research_gate_v3.json")

    assert review["aggregate"] == {
        "benchmark_opportunity_cost": 0.0,
        "false_risk_off_count": 0,
        "unnecessary_exposure_reduction": 0,
    }
    assert all(row["no_trigger_correct"] is True for row in review["review_rows"])
    assert full["previous_full_diagnostic_status"] == "B2_FULL_DIAGNOSTIC_PARTIAL"
    assert full["risk_heavy_evidence_present"] is True
    assert full["control_window_rerun_status"] == "B2_CONTROL_RERUN_COMPLETE"
    assert full["no_trigger_correctness_status"] == "B2_NO_TRIGGER_CORRECTNESS_PASS"
    assert full["partial_reason_resolved"] is True
    assert gate["input_statuses"] == {
        "b2_full_diagnostic_with_control_windows": "B2_FULL_DIAGNOSTIC_COMPLETE",
        "drawdown_protection": "B2_DRAWDOWN_PROTECTION_MIXED",
        "no_trigger_correctness": "B2_NO_TRIGGER_CORRECTNESS_PASS",
        "reentry_cost": "B2_REENTRY_LAG_HIGH",
        "signal_robustness": "B2_TRIGGER_STABILITY_WEAK",
        "utility": "B2_UTILITY_MIXED",
    }
    assert gate["B4_retest_allowed"] is False
    assert gate["b5_allowed"] is False
    assert gate["b6_allowed"] is False
    assert gate["v3_allowed"] is False


def test_b2_path_snapshot_keeps_blocked_modules_blocked() -> None:
    snapshot = _read_json("b2_path_decision_snapshot.json")

    assert snapshot["b2_full_diagnostic_status"] == "B2_FULL_DIAGNOSTIC_COMPLETE"
    assert snapshot["b2_research_gate_status"] == "B2_ONLY_NEEDS_MORE_EVIDENCE"
    assert snapshot["B4_retest_allowed"] is False
    assert snapshot["b5_allowed"] is False
    assert snapshot["b6_allowed"] is False
    assert snapshot["v3_allowed"] is False
    assert all(row["status"] == "PASS" for row in snapshot["hard_rules"])


def test_b2_control_artifacts_disclose_quality_sources_and_safety() -> None:
    expected_source_keys = {
        "b2_full_diagnostic_backfill",
        "b2_drawdown_protection_attribution",
        "b2_false_risk_off_reentry_cost_review",
        "b2_cost_benchmark_utility_review",
        "b2_signal_robustness_trigger_stability",
        "b2_only_full_diagnostic_gate",
        "b3_signal_precheck_resolution_plan",
        "b2_b3_branch_status_snapshot",
        "research_window_catalog",
    }

    for file_name in B2_CONTROL_ARTIFACTS:
        payload = _read_json(file_name)
        safety = payload["safety_boundary"]

        assert payload["schema_version"] == 1
        assert payload["market_regime"] == "ai_after_chatgpt"
        assert payload["requested_date_range"]["start_date"] == "2022-12-01"
        assert payload["holdout_accessed"] is False
        assert payload["forbidden_outputs_absent"] is True
        assert payload["data_quality_gate"]["required_command"] == "aits validate-data"
        assert payload["data_quality_gate"]["passed"] is True
        assert set(payload["source_artifacts"]) == expected_source_keys
        assert payload["reader_brief"]["key_result"] == payload["status"]
        assert safety["research_only"] is True
        assert safety["manual_review_only"] is True
        assert safety["official_target_weights"] is False
        assert safety["paper_shadow_activation"] is False
        assert safety["extended_shadow_allowed"] is False
        assert safety["live_trading_allowed"] is False
        assert safety["broker_action_allowed"] is False
        assert safety["order_ticket_generated"] is False
        assert safety["production_effect"] == "none"


def _read_json(file_name: str) -> dict[str, object]:
    return json.loads((RESEARCH_DIR / file_name).read_text(encoding="utf-8"))
