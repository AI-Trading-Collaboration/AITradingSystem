from __future__ import annotations

from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import safe_load_yaml_path

PIT_COVERAGE_PATH = Path("inputs/research_reviews/indicator_family_pit_coverage_matrix.yaml")
SELECTION_PATH = Path("inputs/research_reviews/indicator_family_selection_matrix.yaml")
FEATURE_SET_PATH = Path("config/research/channel_specific_feature_set_v1.yaml")
ADD_RISK_PATH = Path("inputs/research_reviews/indicator_family_add_risk_matrix.yaml")
DEPENDENCE_2023_PATH = Path(
    "inputs/research_reviews/indicator_family_2023_plus_dependence_matrix.yaml"
)
BETA_TQQQ_PATH = Path(
    "inputs/research_reviews/indicator_family_beta_tqqq_dependency_matrix.yaml"
)
FINAL_PATH = Path("inputs/research_reviews/indicator_family_ablation_final_matrix.yaml")


def test_indicator_family_requires_pit_coverage() -> None:
    matrix = _load_yaml(PIT_COVERAGE_PATH)
    rows = _rows_by_family(matrix, "family_rows")

    assert matrix["status"] == "INDICATOR_FAMILY_PIT_COVERAGE_AUDIT_READY"
    assert len(rows) == 7
    assert rows["trend_persistence"]["coverage_status"] == "PIT_COVERAGE_PASS"
    assert rows["drawdown_recovery"]["has_2022_slice_coverage"] is True
    assert rows["breadth_participation"]["coverage_status"] == "PIT_BLOCKED"
    assert rows["event_risk"]["coverage_status"] == "PIT_BLOCKED"


def test_family_with_pit_blocker_cannot_be_selected() -> None:
    matrix = _load_yaml(SELECTION_PATH)
    rows = _rows_by_family(matrix, "family_rows")

    for family_name in ("breadth_participation", "event_risk"):
        row = rows[family_name]
        assert row["coverage_status"] == "PIT_BLOCKED"
        assert row["selected_for_next_model"] is False
        assert row["candidate_allowed"] is False
        assert row["can_emit_weights"] is False


def test_2023_plus_dependent_family_is_diagnostic_only() -> None:
    dependence = _rows_by_family(_load_yaml(DEPENDENCE_2023_PATH), "family_rows")
    selection = _rows_by_family(_load_yaml(SELECTION_PATH), "family_rows")

    for family_name in ("trend_persistence", "relative_strength"):
        assert dependence[family_name]["dependence_status"] == "FAMILY_2023_PLUS_DEPENDENT"
        assert dependence[family_name]["diagnostic_only"] is True
        assert selection[family_name]["diagnostic_only"] is True
        assert "add_risk" in selection[family_name]["blocked_channels"]


def test_beta_only_family_cannot_pass_add_risk_selection() -> None:
    beta = _rows_by_family(_load_yaml(BETA_TQQQ_PATH), "family_rows")
    add_risk = _rows_by_family(_load_yaml(ADD_RISK_PATH), "family_rows")

    assert beta["relative_strength"]["dependency_status"] == "FAMILY_TQQQ_BETA_DEPENDENT"
    assert beta["relative_strength"]["add_risk_allowed"] is False
    assert add_risk["relative_strength"]["selected_for_channel"] is False
    assert add_risk["relative_strength"]["verdict"] == "FAMILY_TQQQ_BETA_DEPENDENT"


def test_family_selection_matrix_has_allowed_and_blocked_channels() -> None:
    matrix = _load_yaml(SELECTION_PATH)
    rows = _rows_by_family(matrix, "family_rows")

    assert matrix["summary"]["allocation_candidate_count"] == 0
    assert rows["drawdown_recovery"]["selected_channels"] == ["do_not_de_risk"]
    assert rows["volatility_compression"]["selected_channels"] == ["risk_on_veto"]
    for row in rows.values():
        assert "allowed_channels" in row
        assert "blocked_channels" in row
        assert row["can_emit_weights"] is False


def test_channel_specific_feature_set_uses_selected_families_only() -> None:
    selection = _rows_by_family(_load_yaml(SELECTION_PATH), "family_rows")
    selected = {
        family_name
        for family_name, row in selection.items()
        if row["selected_for_next_model"] is True
    }
    feature_set = _load_yaml(FEATURE_SET_PATH)

    allowed_families: set[str] = set()
    for section in (
        "do_not_de_risk",
        "risk_on_veto",
        "add_risk",
        "return_seeking_diagnostic",
    ):
        allowed_families.update(feature_set[section]["allowed_families"])
    allowed_families.update(feature_set["defensive_channel"]["diagnostic_families"])

    assert allowed_families <= selected
    assert not ({"breadth_participation", "event_risk"} & allowed_families)
    assert feature_set["add_risk"]["allowed_families"] == []
    assert feature_set["safety_boundary"]["can_emit_weights"] is False


def test_indicator_family_ablation_final_matrix_keeps_promotion_blocked() -> None:
    final = _load_yaml(FINAL_PATH)

    assert final["status"] == "INDICATOR_FAMILY_ABLATION_EVIDENCE_READY"
    assert "DO_NOT_DERISK_FAMILY_FOUND" in final["status_flags"]
    assert "RISK_ON_VETO_FAMILY_FOUND" in final["status_flags"]
    assert "ADD_RISK_FAMILY_DIAGNOSTIC_ONLY" in final["status_flags"]
    assert final["phase_decision"]["candidate_count"] == 0
    assert final["promotion_allowed"] is False
    assert final["broker_action"] == "none"


def _rows_by_family(payload: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    rows = payload[key]
    assert isinstance(rows, list)
    return {str(row["family_name"]): row for row in rows if isinstance(row, dict)}


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw
