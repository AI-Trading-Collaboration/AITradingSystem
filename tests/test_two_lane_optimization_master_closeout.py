from __future__ import annotations

from pathlib import Path

from ai_trading_system.yaml_loader import safe_load_yaml_path

MASTER_MATRIX_PATH = Path("inputs/research_reviews/two_lane_optimization_master_final_matrix.yaml")
MASTER_DOC_PATH = Path("docs/research/two_lane_optimization_master_closeout.md")
REGISTRY_PATH = Path("config/report_registry.yaml")
CATALOG_PATH = Path("docs/artifact_catalog.md")
SYSTEM_FLOW_PATH = Path("docs/system_flow.md")

FINAL_STATUS = "TWO_LANE_OPTIMIZATION_DIAGNOSTIC_ONLY_INSUFFICIENT_EVIDENCE_PROMOTION_BLOCKED"


def test_master_closeout_records_full_attachment_gate_result() -> None:
    matrix = _load_yaml(MASTER_MATRIX_PATH)
    summary = matrix["summary"]

    assert matrix["status"] == FINAL_STATUS
    assert summary["final_status"] == FINAL_STATUS
    assert summary["phase_1_status"] == "LANE_SEPARATION_POLICY_READY"
    assert summary["phase_2_status"] == "DEFENSIVE_LANE_NO_MATERIAL_IMPROVEMENT"
    assert summary["phase_3_status"] == (
        "RETURN_SEEKING_DIAGNOSTIC_UPSIDE_DEPENDENT_DRAWDOWN_REGRESSED_PROMOTION_BLOCKED"
    )
    assert summary["phase_4_status"] == "BLOCKED_OWNER_INPUT"
    assert summary["phase_5_status"] == "BLOCKED_OWNER_INPUT"
    assert summary["final_recommendation"] == (
        "PAUSE_AUTOMATIC_FIRST_LAYER_TREND_STRATEGY_KEEP_DIAGNOSTIC_ONLY"
    )


def test_master_closeout_blocks_phase_4_and_phase_5_execution() -> None:
    matrix = _load_yaml(MASTER_MATRIX_PATH)
    phase_rows = {row["phase_id"]: row for row in matrix["phase_rows"]}
    blocked_actions = {row["action"] for row in matrix["blocked_actions"]}

    phase_4 = phase_rows["TRADING-1861_to_1875"]
    phase_5 = phase_rows["TRADING-1876_to_1885"]

    assert phase_4["gate_result"] == "NOT_ENTERED_PHASE_2_3_GATES_FAILED"
    assert phase_5["gate_result"] == "NOT_ENTERED_NO_LOCKED_CANDIDATE"
    assert "implement_two_lane_gated_overlay_actual_path" in blocked_actions
    assert "run_multi_window_candidate_validation" in blocked_actions
    assert matrix["summary"]["gated_integration_allowed"] is False
    assert matrix["summary"]["candidate_locked_for_validation"] is False
    assert matrix["final_decision"]["gated_integration_allowed_now"] is False
    assert matrix["final_decision"]["candidate_validation_allowed_now"] is False


def test_master_closeout_keeps_all_downstream_safety_disabled() -> None:
    matrix = _load_yaml(MASTER_MATRIX_PATH)
    summary = matrix["summary"]

    assert summary["owner_review_allowed"] is False
    assert summary["promotion_status"] == "blocked"
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert matrix["promotion_allowed"] is False
    assert matrix["paper_shadow_allowed"] is False
    assert matrix["production_allowed"] is False
    assert matrix["broker_action"] == "none"
    assert matrix["dynamic_promotion_status"] == "BLOCKED"


def test_master_closeout_evidence_paths_exist() -> None:
    matrix = _load_yaml(MASTER_MATRIX_PATH)

    for row in matrix["phase_rows"]:
        for evidence_path in row["evidence_paths"]:
            assert Path(evidence_path).exists(), evidence_path


def test_master_closeout_is_documented_in_governance_surfaces() -> None:
    registry = REGISTRY_PATH.read_text(encoding="utf-8")
    catalog = CATALOG_PATH.read_text(encoding="utf-8")
    system_flow = SYSTEM_FLOW_PATH.read_text(encoding="utf-8")
    closeout = MASTER_DOC_PATH.read_text(encoding="utf-8")

    assert "two_lane_optimization_master_closeout" in registry
    assert "two_lane_optimization_master_final_matrix.yaml" in catalog
    assert FINAL_STATUS in system_flow
    assert FINAL_STATUS in closeout
    assert "Phase 4/5" in closeout
    assert "gate-defined stop state" in closeout
    assert "不是待补实现项" in closeout


def _load_yaml(path: Path) -> dict[str, object]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
