from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

CONTRACT_PATH = Path("config/research/risk_on_veto_diagnostic_contract.yaml")
METRIC_POLICY_PATH = Path("config/research/risk_on_veto_metric_policy.yaml")
FORWARD_LOG_PATH = Path("config/research/risk_on_veto_forward_log.yaml")
EPISODES_PATH = Path(
    "outputs/research_trends/risk_on_veto_diagnostic/risk_on_veto_episodes.csv"
)
DIAGNOSTIC_PATH = Path(
    "inputs/research_reviews/risk_on_veto_observe_only_diagnostic.yaml"
)
ARCHIVE_PATH = Path("inputs/research_reviews/do_not_de_risk_v3_archive.yaml")
TRADEOFF_PATH = Path("inputs/research_reviews/risk_on_veto_tradeoff_matrix.yaml")
COMPATIBILITY_PATH = Path(
    "inputs/research_reviews/risk_on_veto_return_seeking_diagnostic_compatibility.yaml"
)

FORBIDDEN_FIELDS = {
    "target_weights",
    "portfolio_weights",
    "trade_action",
    "paper_shadow_action",
    "broker_action",
    "recommended_allocation",
    "target_allocation",
    "qqq_weight",
    "sgov_weight",
    "tqqq_weight",
    "QQQ",
    "SGOV",
    "TQQQ",
}


def test_risk_on_veto_diagnostic_cannot_emit_weights() -> None:
    contract = _load_yaml(CONTRACT_PATH)
    diagnostic = contract["risk_on_veto_diagnostic_contract"]
    columns = set(_csv_columns(EPISODES_PATH))

    assert diagnostic["can_emit_weights"] is False
    assert diagnostic["can_enable_tqqq"] is False
    assert not (FORBIDDEN_FIELDS & columns)


def test_risk_on_veto_diagnostic_cannot_emit_trade_action() -> None:
    contract = _load_yaml(CONTRACT_PATH)
    forward_log = _load_yaml(FORWARD_LOG_PATH)["risk_on_veto_forward_log"]
    diagnostic = contract["risk_on_veto_diagnostic_contract"]

    assert diagnostic["can_emit_trade_advice"] is False
    assert "trade_action" in forward_log["blocked_fields"]
    assert "broker_action" in forward_log["blocked_fields"]
    assert "trade_action" not in _csv_columns(EPISODES_PATH)


def test_risk_on_veto_diagnostic_cannot_enable_promotion() -> None:
    contract = _load_yaml(CONTRACT_PATH)
    diagnostic = _load_yaml(DIAGNOSTIC_PATH)
    tradeoff = _load_yaml(TRADEOFF_PATH)
    compatibility = _load_yaml(COMPATIBILITY_PATH)

    assert contract["risk_on_veto_diagnostic_contract"]["owner_review_allowed"] is False
    for artifact in (diagnostic, tradeoff, compatibility):
        if "candidate_count" in artifact["summary"]:
            assert artifact["summary"]["candidate_count"] == 0
        assert artifact["promotion_allowed"] is False
        assert artifact["paper_shadow_allowed"] is False
        assert artifact["production_allowed"] is False
        assert artifact["broker_action"] == "none"
        assert artifact["dynamic_promotion_status"] == "BLOCKED"


def test_risk_on_veto_episode_log_has_no_allocation_fields() -> None:
    columns = set(_csv_columns(EPISODES_PATH))
    rows = _csv_rows(EPISODES_PATH)

    assert rows
    assert not (FORBIDDEN_FIELDS & columns)
    assert {"veto_active", "blocked_add_risk", "net_veto_benefit"} <= columns


def test_veto_metric_policy_distinguishes_raw_and_avoided_cost() -> None:
    policy = _load_yaml(METRIC_POLICY_PATH)["risk_on_veto_metric_policy"]
    diagnostic_summary = _load_yaml(DIAGNOSTIC_PATH)["summary"]

    assert "raw_false_add_risk_cost_when_veto_active" in policy
    assert "avoided_false_add_risk_cost_due_to_veto" in policy
    assert policy["raw_false_add_risk_cost_when_veto_active"]["filter"] != (
        policy["avoided_false_add_risk_cost_due_to_veto"]["filter"]
    )
    assert "raw_false_add_risk_cost_when_veto_active" in diagnostic_summary
    assert "avoided_false_add_risk_cost_due_to_veto_total" in diagnostic_summary


def test_do_not_de_risk_v3_is_archived() -> None:
    archive = _load_yaml(ARCHIVE_PATH)
    summary = archive["summary"]

    assert archive["status"] == "DO_NOT_DERISK_V3_ARCHIVED_NO_MATERIAL_IMPROVEMENT"
    assert summary["false_risk_off_reduction"] is False
    assert summary["2022_slice_not_worse"] is False
    assert summary["defensive_probe_regression_count"] == 8
    assert summary["next_model_allowed"] is False
    assert summary["owner_review_allowed"] is False


def test_risk_on_veto_diagnostic_artifacts_have_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    for path in (DIAGNOSTIC_PATH, ARCHIVE_PATH, TRADEOFF_PATH, COMPATIBILITY_PATH):
        artifact = _load_yaml(path)
        metadata = artifact["research_audit_metadata"]

        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "first_layer"
        assert metadata["modified_channel"] == "risk_veto"
        assert metadata["candidate_count"] == 0


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw


def _csv_columns(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames
        return list(reader.fieldnames)


def _csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))
