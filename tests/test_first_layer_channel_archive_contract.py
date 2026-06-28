from __future__ import annotations

from pathlib import Path
from typing import Any

from ai_trading_system.research_audit_metadata import (
    load_research_audit_metadata_schema,
    validate_research_audit_metadata,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

ARCHIVE_POLICY_PATH = Path("config/research/first_layer_channel_archive_policy.yaml")
FORWARD_PLAN_PATH = Path("config/research/first_layer_forward_diagnostic_minimal_plan.yaml")
MASTER_CLOSEOUT_PATH = Path("inputs/research_reviews/first_layer_channel_master_closeout.yaml")
STATUS_MATRIX_PATH = Path("inputs/research_reviews/first_layer_channel_status_matrix.yaml")
EVIDENCE_LABELING_PATH = Path(
    "inputs/research_reviews/first_layer_diagnostic_evidence_labeling.yaml"
)
REOPEN_CRITERIA_PATH = Path("inputs/research_reviews/first_layer_channel_reopen_criteria.yaml")
PIT_GAP_ROADMAP_PATH = Path("inputs/research_reviews/first_layer_pit_data_gap_roadmap.yaml")
REPORT_REGISTRY_PATH = Path("config/report_registry.yaml")

FORBIDDEN_OUTPUTS = {
    "weights",
    "portfolio_weights",
    "trade_advice",
    "target_allocation",
    "recommended_allocation",
    "paper_shadow_action",
    "broker_action",
}


def test_first_layer_channel_master_closeout_blocks_promotion_paths() -> None:
    closeout = _load_yaml(MASTER_CLOSEOUT_PATH)
    summary = closeout["summary"]

    assert closeout["status"] == "FIRST_LAYER_CHANNEL_RESEARCH_CLOSED_NO_CANDIDATE"
    assert summary["candidate_count"] == 0
    assert summary["owner_review_allowed"] is False
    assert summary["forward_watch_allowed"] is False
    assert summary["promotion_allowed"] is False
    assert summary["paper_shadow_allowed"] is False
    assert summary["production_allowed"] is False
    assert summary["broker_action"] == "none"
    assert closeout["dynamic_promotion_status"] == "BLOCKED"


def test_status_matrix_records_required_channel_archive_states() -> None:
    matrix = _load_yaml(STATUS_MATRIX_PATH)
    rows = _rows_by_name(matrix)

    expected = {
        "do_not_de_risk_v3": ("ARCHIVED", "NO_MATERIAL_IMPROVEMENT"),
        "risk_on_veto_v3": (
            "HISTORICAL_DIAGNOSTIC_ARCHIVE",
            "NET_VETO_BENEFIT_NEGATIVE",
        ),
        "add_risk": ("NOT_SUPPORTED", "NO_SELECTED_INDICATOR_FAMILY"),
        "return_seeking_diagnostic": (
            "HISTORICAL_DIAGNOSTIC_ONLY",
            "RETURN_SEEKING_EVIDENCE_WITH_BLOCKERS",
        ),
        "defensive_channel": ("CLOSED_NO_MATERIAL_IMPROVEMENT", "DEFENSIVE_USAGE_BLOCKED"),
        "risk_veto_channel": (
            "HISTORICAL_DIAGNOSTIC_ARCHIVE",
            "RISK_VETO_TOO_STRICT_CURRENT_EVIDENCE",
        ),
        "breadth_participation": (
            "PIT_BLOCKED",
            "CANNOT_ENTER_MODEL_WITHOUT_PIT_APPROVED_SOURCE",
        ),
        "event_risk": ("PIT_BLOCKED", "CANNOT_ENTER_MODEL_WITHOUT_PIT_APPROVED_SOURCE"),
    }

    assert set(rows) == set(expected)
    for channel_name, (status, verdict) in expected.items():
        row = rows[channel_name]

        assert row["status"] == status
        assert row["verdict"] == verdict
        assert row.get("candidate_allowed") is not True
        assert "promotion" in row["blocked_usage"]
        assert "production" in row["blocked_usage"]
        assert "broker" in row["blocked_usage"]


def test_risk_on_veto_negative_net_benefit_blocks_forward_watch() -> None:
    rows = _rows_by_name(_load_yaml(STATUS_MATRIX_PATH))
    risk_on_veto = rows["risk_on_veto_v3"]

    assert risk_on_veto["net_veto_benefit_total"] < 0
    assert risk_on_veto["compatibility_status"] == (
        "VETO_TOO_STRICT_FOR_RETURN_SEEKING_DIAGNOSTIC"
    )
    assert risk_on_veto["forward_watch_candidate"] is False
    assert risk_on_veto["can_emit_weights"] is False
    assert "forward_watch_candidate" in risk_on_veto["blocked_usage"]


def test_diagnostic_evidence_labels_are_not_owner_or_promotion_evidence() -> None:
    labeling = _load_yaml(EVIDENCE_LABELING_PATH)
    required_labels = {
        "ARCHIVED_RESEARCH_EVIDENCE",
        "HISTORICAL_DIAGNOSTIC_ONLY",
        "NOT_OWNER_REVIEWABLE",
        "NOT_PROMOTION_EVIDENCE",
        "NO_ALLOCATION_OUTPUT",
    }

    assert set(labeling["summary"]["labels"]) == required_labels
    assert labeling["summary"]["owner_reviewable"] is False
    assert labeling["summary"]["promotion_evidence"] is False
    assert labeling["summary"]["allocation_output"] is False
    for row in labeling["rows"]:
        assert set(row["labels"]) == required_labels


def test_reopen_criteria_requires_new_pit_or_forward_evidence_and_owner_approval() -> None:
    reopen = _load_yaml(REOPEN_CRITERIA_PATH)
    rows = {row["criteria_group"]: row for row in reopen["rows"]}

    assert reopen["summary"]["owner_approval_required"] is True
    assert reopen["summary"]["historical_rerun_alone_insufficient"] is True
    assert reopen["summary"]["2023_plus_only_result_can_reopen"] is False
    assert reopen["summary"]["target_path_only_result_can_reopen"] is False
    assert reopen["summary"]["diagnostic_only_signal_can_emit_weights"] is False
    assert "PIT-approved breadth / participation data" in rows["data_or_feature"][
        "allowed_reopen_trigger"
    ]
    assert "owner manual reopen approval" in rows["model_or_strategy"][
        "required_conditions"
    ]
    assert "diagnostic-only signal emits weights" in rows["prohibited_reopen_evidence"][
        "blocked_conditions"
    ]


def test_pit_data_gap_roadmap_keeps_blocked_families_out_of_model() -> None:
    roadmap = _load_yaml(PIT_GAP_ROADMAP_PATH)
    rows = {row["family_name"]: row for row in roadmap["rows"]}

    assert roadmap["summary"]["model_entry_allowed_before_pit_approval"] is False
    assert roadmap["summary"]["high_priority_families"] == [
        "breadth_participation",
        "event_risk",
    ]
    for family_name, row in rows.items():
        assert row["current_status"] == "PIT_BLOCKED"
        assert "known_at" in row["minimum_required_fields"]
        assert "PIT" in row["blocked_until"]
        if family_name in {"breadth_participation", "event_risk"}:
            assert row["expected_usage"]


def test_forward_diagnostic_minimal_plan_stays_disabled_and_no_allocation_output() -> None:
    plan = _load_yaml(FORWARD_PLAN_PATH)
    body = plan["first_layer_forward_diagnostic_minimal_plan"]
    safety = plan["safety_boundary"]

    assert plan["status"] == "disabled_until_owner_reopen"
    assert body["enabled"] is False
    assert body["requires_owner_approval"] is True
    assert FORBIDDEN_OUTPUTS <= set(body["blocked_outputs"])
    assert not (FORBIDDEN_OUTPUTS & set(body["allowed_outputs"]))
    assert safety["candidate_count"] == 0
    assert safety["owner_review_allowed"] is False
    assert safety["promotion_allowed"] is False
    assert safety["paper_shadow_allowed"] is False
    assert safety["production_allowed"] is False
    assert safety["broker_action"] == "none"


def test_archive_policy_matches_closeout_status_contract() -> None:
    policy = _load_yaml(ARCHIVE_POLICY_PATH)
    body = policy["first_layer_channel_archive_policy"]

    assert body["archived_channels"]["do_not_de_risk_v3"]["status"] == "ARCHIVED"
    assert body["archived_channels"]["risk_on_veto_v3"]["status"] == (
        "HISTORICAL_DIAGNOSTIC_ARCHIVE"
    )
    assert body["archived_channels"]["add_risk"]["status"] == "NOT_SUPPORTED"
    assert body["archived_channels"]["return_seeking_diagnostic"]["status"] == (
        "HISTORICAL_DIAGNOSTIC_ONLY"
    )
    assert body["reopen"]["owner_approval_required"] is True
    assert body["reopen"]["historical_rerun_alone_insufficient"] is True
    assert body["reopen"]["beta_or_tqqq_only_blocked"] is True
    assert body["reopen"]["2023_plus_only_blocked"] is True
    assert body["reopen"]["target_path_only_blocked"] is True


def test_first_layer_channel_closeout_artifacts_have_audit_metadata() -> None:
    schema = load_research_audit_metadata_schema()
    for path in (
        MASTER_CLOSEOUT_PATH,
        STATUS_MATRIX_PATH,
        EVIDENCE_LABELING_PATH,
        REOPEN_CRITERIA_PATH,
        PIT_GAP_ROADMAP_PATH,
    ):
        artifact = _load_yaml(path)
        metadata = artifact["research_audit_metadata"]

        assert validate_research_audit_metadata(artifact, schema)["status"] == "PASS"
        assert metadata["modified_layer"] == "validation_only"
        assert metadata["modified_channel"] == "first_layer_channel_closeout"
        assert metadata["candidate_count"] == 0
        assert artifact["promotion_allowed"] is False
        assert artifact["paper_shadow_allowed"] is False
        assert artifact["production_allowed"] is False
        assert artifact["broker_action"] == "none"


def test_report_registry_tracks_first_layer_channel_closeout_pack() -> None:
    registry = _load_yaml(REPORT_REGISTRY_PATH)
    reports = {report["report_id"]: report for report in registry["reports"]}
    report = reports["first_layer_channel_closeout_reopen_criteria"]

    assert report["command"] == "aits research trends first-layer-channel-closeout"
    assert "inputs/research_reviews/first_layer_channel_master_closeout.yaml" in report[
        "artifact_globs"
    ]
    assert "docs/research/first_layer_channel_closeout_owner_brief.md" in report[
        "artifact_globs"
    ]
    assert report["production_effect"] == "none"
    assert report["broker_action"] == "none"


def _load_yaml(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    assert isinstance(raw, dict)
    return raw


def _rows_by_name(artifact: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {row["channel_name"]: row for row in artifact["rows"]}
