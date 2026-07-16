from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import (
    run_research_direction_foundation_fixture,
)

from ai_trading_system.etf_portfolio import dynamic_v3_research_direction_foundation as direction
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as legacy
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


@pytest.fixture(scope="module")
def foundation_fixture(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("research_direction_foundation")
    with artifact_validation_session():
        yield {"root": root, **run_research_direction_foundation_fixture(root)}


def _direction_case(
    fixture: dict[str, Any],
) -> tuple[Path, str, Path]:
    artifact = fixture["next_direction"]
    return (
        Path(artifact["direction_dir"]),
        artifact["direction_id"],
        fixture["root"] / "next_research_direction",
    )


def _roadmap_case(
    fixture: dict[str, Any],
) -> tuple[Path, str, Path]:
    artifact = fixture["owner_roadmap"]
    return (
        Path(artifact["roadmap_dir"]),
        artifact["roadmap_id"],
        fixture["root"] / "owner_research_roadmap",
    )


def test_research_direction_foundation_preserves_insufficient_evidence_semantics(
    foundation_fixture: dict[str, Any],
) -> None:
    next_direction = foundation_fixture["next_direction"]
    roadmap = foundation_fixture["owner_roadmap"]
    decision = next_direction["next_research_direction_decision"]
    tasks = next_direction["next_task_plan"]["tasks"]
    summary = roadmap["owner_roadmap_summary"]

    assert next_direction["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert decision["source_evidence_status"] == "INSUFFICIENT_DATA"
    assert decision["source_failure_source"] == "INCONCLUSIVE"
    assert decision["source_recommended_shift"] == "DEFER_AND_BUILD_DATED_EVIDENCE"
    assert decision["decision"] == "DEFER_PARAMETER_SEARCH_AND_BUILD_DATED_EVIDENCE"
    assert decision["confidence"] == "LOW"
    assert decision["dated_evidence_required"] is True
    assert decision["research_direction_change_authorized"] is False
    assert decision["historical_downstream_task_ranges"] == [
        "TRADING-326_to_335",
        "TRADING-336_to_345",
    ]
    assert (
        decision["historical_downstream_evidence_role"]
        == "HISTORICAL_CONTEXT_ONLY_NOT_CURRENT_ATTRIBUTION_PROOF"
    )
    assert tasks and {task["status"] for task in tasks} == {"PROPOSED_OWNER_REVIEW"}
    assert all(task["implemented"] is False for task in tasks)
    assert all(task["auto_register"] is False for task in tasks)
    assert next_direction["next_task_plan"]["automatic_implementation_allowed"] is False
    assert next_direction["next_task_plan"]["task_state_mutation_allowed"] is False

    assert roadmap["manifest"]["status"] == "PASS_WITH_WARNINGS"
    assert summary["current_phase"] == "post_micro_search_v4_evidence_gap"
    assert summary["parameter_search_status"] == "DEFER"
    assert summary["best_current_observation_candidate"] == (
        "NOT_SELECTED_CURRENT_EVIDENCE_INSUFFICIENT"
    )
    assert summary["next_research_direction"] == ("DEFER_PARAMETER_SEARCH_AND_BUILD_DATED_EVIDENCE")
    assert summary["recommended_owner_action"] == (
        "register_and_review_dated_evidence_build_before_directional_research"
    )
    assert summary["roadmap_decision_status"] == "OWNER_REVIEW_REQUIRED"
    assert summary["task_state_mutation_allowed"] is False
    assert summary["automatic_implementation_allowed"] is False
    assert roadmap["manifest"]["production_effect"] == "none"
    assert "build PIT dated signal events" in roadmap["owner_roadmap_checklist"]
    assert "broker_action_allowed=false" in roadmap["owner_roadmap_checklist"]


def test_research_direction_foundation_snapshots_bind_exact_lineage(
    foundation_fixture: dict[str, Any],
) -> None:
    direction_root, direction_id, _ = _direction_case(foundation_fixture)
    roadmap_root, roadmap_id, _ = _roadmap_case(foundation_fixture)
    direction_snapshot = json.loads(
        (direction_root / "next_research_direction_input_snapshot.json").read_bytes()
    )
    roadmap_snapshot = json.loads(
        (roadmap_root / "owner_research_roadmap_input_snapshot.json").read_bytes()
    )

    assert direction_snapshot["schema_version"] == direction.DIRECTION_INPUT_SCHEMA
    assert roadmap_snapshot["schema_version"] == direction.ROADMAP_INPUT_SCHEMA
    assert direction_snapshot["direction_id"] == direction_id
    assert roadmap_snapshot["roadmap_id"] == roadmap_id
    assert direction_snapshot["source_recommended_shift"] == ("DEFER_AND_BUILD_DATED_EVIDENCE")
    assert roadmap_snapshot["direction_source"]["artifact_id"] == direction_id
    assert roadmap_snapshot["source_attribution_id"] == (
        direction_snapshot["attribution_source"]["artifact_id"]
    )
    assert (
        direction_snapshot["policy_source"]["sha256"] == roadmap_snapshot["policy_source"]["sha256"]
    )


def test_research_direction_foundation_rebuilds_all_canonical_bytes(
    foundation_fixture: dict[str, Any],
) -> None:
    _, direction_id, direction_output = _direction_case(foundation_fixture)
    _, roadmap_id, roadmap_output = _roadmap_case(foundation_fixture)
    with artifact_validation_session():
        direction_validation = direction.validate_next_research_direction_artifact(
            direction_id=direction_id, output_dir=direction_output
        )
        roadmap_validation = direction.validate_owner_research_roadmap_artifact(
            roadmap_id=roadmap_id, output_dir=roadmap_output
        )
    assert direction_validation["status"] == "PASS", direction_validation
    assert roadmap_validation["status"] == "PASS", roadmap_validation


def test_research_direction_foundation_rejects_unknown_source_shift() -> None:
    policy = direction._policy(direction.DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH)
    attribution = {
        "attribution_id": "fixture-attribution",
        "recommended_research_shift": {"recommended_shift": "UNKNOWN_SHIFT"},
        "failure_source_attribution": {
            "evidence_status": "INSUFFICIENT_DATA",
            "failure_source": "INCONCLUSIVE",
            "confidence": "LOW",
        },
    }

    with pytest.raises(
        direction.DynamicV3ResearchDirectionFoundationError,
        match="unmapped recommended shift",
    ):
        direction._direction_decision(attribution, policy)


def test_research_direction_foundation_rejects_directional_shift_without_dated_evidence() -> None:
    policy = direction._policy(direction.DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH)
    attribution = {
        "attribution_id": "fixture-attribution",
        "recommended_research_shift": {"recommended_shift": "SHIFT_TO_SIGNAL_FEATURE_DIAGNOSIS"},
        "failure_source_attribution": {
            "evidence_status": "INSUFFICIENT_DATA",
            "failure_source": "INCONCLUSIVE",
            "confidence": "LOW",
            "evidence": [],
        },
    }

    with pytest.raises(
        direction.DynamicV3ResearchDirectionFoundationError,
        match="insufficient evidence must defer",
    ):
        direction._direction_decision(attribution, policy)


def test_research_direction_foundation_maps_sufficient_evidence_without_authorizing() -> None:
    policy = direction._policy(direction.DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH)
    attribution = {
        "attribution_id": "fixture-attribution",
        "recommended_research_shift": {"recommended_shift": "REVIEW_GATE_POLICY"},
        "failure_source_attribution": {
            "evidence_status": "SUFFICIENT",
            "failure_source": "GATE_POLICY",
            "confidence": "MEDIUM",
            "evidence": ["validated gate-policy evidence"],
        },
    }

    decision = direction._direction_decision(attribution, policy)

    assert decision["decision"] == "REVIEW_GATE_POLICY"
    assert decision["dated_evidence_required"] is False
    assert decision["research_direction_change_authorized"] is False


def test_research_direction_foundation_rejects_dated_evidence_defer_when_sufficient() -> None:
    policy = direction._policy(direction.DEFAULT_RESEARCH_DIRECTION_FOUNDATION_POLICY_PATH)
    attribution = {
        "attribution_id": "fixture-attribution",
        "recommended_research_shift": {"recommended_shift": "DEFER_AND_BUILD_DATED_EVIDENCE"},
        "failure_source_attribution": {
            "evidence_status": "SUFFICIENT",
            "failure_source": "GATE_POLICY",
            "confidence": "MEDIUM",
            "evidence": [],
        },
    }

    with pytest.raises(
        direction.DynamicV3ResearchDirectionFoundationError,
        match="defer shift requires insufficient evidence",
    ):
        direction._direction_decision(attribution, policy)


def test_owner_research_roadmap_checklist_matches_sufficient_evidence_branch() -> None:
    summary = {
        "roadmap_id": "fixture-roadmap",
        "source_direction_id": "fixture-direction",
        "source_attribution_id": "fixture-attribution",
        "source_evidence_status": "SUFFICIENT",
        "next_research_direction": "REVIEW_GATE_POLICY",
        "dated_evidence_required": False,
    }
    source = {"next_research_direction_decision": {"confidence": "MEDIUM"}}

    checklist = direction._render_roadmap_checklist(summary, source)

    assert "build PIT dated signal events" not in checklist
    assert "review the validated direction and proposed task plan" in checklist
    assert "separately registered owner review" in checklist


@pytest.mark.parametrize("view_name", direction.DIRECTION_VIEWS)
def test_next_research_direction_rejects_every_output_tamper(
    foundation_fixture: dict[str, Any], view_name: str
) -> None:
    artifact_root, artifact_id, output_dir = _direction_case(foundation_fixture)
    view = artifact_root / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(original + b"\nTAMPER")
        validation = direction.validate_next_research_direction_artifact(
            direction_id=artifact_id, output_dir=output_dir
        )
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


@pytest.mark.parametrize("view_name", direction.ROADMAP_VIEWS)
def test_owner_research_roadmap_rejects_every_output_tamper(
    foundation_fixture: dict[str, Any], view_name: str
) -> None:
    artifact_root, artifact_id, output_dir = _roadmap_case(foundation_fixture)
    view = artifact_root / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(original + b"\nTAMPER")
        validation = direction.validate_owner_research_roadmap_artifact(
            roadmap_id=artifact_id, output_dir=output_dir
        )
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


@pytest.mark.parametrize(
    ("case_name", "snapshot_name", "validator_name", "id_key"),
    [
        (
            "direction",
            "next_research_direction_input_snapshot.json",
            "validate_next_research_direction_artifact",
            "direction_id",
        ),
        (
            "roadmap",
            "owner_research_roadmap_input_snapshot.json",
            "validate_owner_research_roadmap_artifact",
            "roadmap_id",
        ),
    ],
)
def test_research_direction_foundation_rejects_policy_binding_tamper(
    foundation_fixture: dict[str, Any],
    case_name: str,
    snapshot_name: str,
    validator_name: str,
    id_key: str,
) -> None:
    case = (
        _direction_case(foundation_fixture)
        if case_name == "direction"
        else _roadmap_case(foundation_fixture)
    )
    artifact_root, artifact_id, output_dir = case
    snapshot_path = artifact_root / snapshot_name
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["policy_source"]["sha256"] = "0" * 64
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = getattr(direction, validator_name)(
            **{id_key: artifact_id, "output_dir": output_dir}
        )
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


@pytest.mark.parametrize(
    ("case_name", "snapshot_name", "validator_name", "id_key"),
    [
        (
            "direction",
            "next_research_direction_input_snapshot.json",
            "validate_next_research_direction_artifact",
            "direction_id",
        ),
        (
            "roadmap",
            "owner_research_roadmap_input_snapshot.json",
            "validate_owner_research_roadmap_artifact",
            "roadmap_id",
        ),
    ],
)
def test_research_direction_foundation_rejects_snapshot_safety_tamper(
    foundation_fixture: dict[str, Any],
    case_name: str,
    snapshot_name: str,
    validator_name: str,
    id_key: str,
) -> None:
    case = (
        _direction_case(foundation_fixture)
        if case_name == "direction"
        else _roadmap_case(foundation_fixture)
    )
    artifact_root, artifact_id, output_dir = case
    snapshot_path = artifact_root / snapshot_name
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["broker_action_allowed"] = True
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = getattr(direction, validator_name)(
            **{id_key: artifact_id, "output_dir": output_dir}
        )
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


@pytest.mark.parametrize(
    ("case_name", "snapshot_name", "source_key", "validator_name", "id_key"),
    [
        (
            "direction",
            "next_research_direction_input_snapshot.json",
            "attribution_source",
            "validate_next_research_direction_artifact",
            "direction_id",
        ),
        (
            "roadmap",
            "owner_research_roadmap_input_snapshot.json",
            "direction_source",
            "validate_owner_research_roadmap_artifact",
            "roadmap_id",
        ),
    ],
)
def test_research_direction_foundation_rejects_unbounded_source_binding(
    foundation_fixture: dict[str, Any],
    case_name: str,
    snapshot_name: str,
    source_key: str,
    validator_name: str,
    id_key: str,
) -> None:
    case = (
        _direction_case(foundation_fixture)
        if case_name == "direction"
        else _roadmap_case(foundation_fixture)
    )
    artifact_root, artifact_id, output_dir = case
    snapshot_path = artifact_root / snapshot_name
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        files = snapshot[source_key]["files"]
        files.pop(next(iter(files)))
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = getattr(direction, validator_name)(
            **{id_key: artifact_id, "output_dir": output_dir}
        )
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


@pytest.mark.parametrize(
    ("case_name", "snapshot_name", "source_key", "validator_name", "id_key"),
    [
        (
            "direction",
            "next_research_direction_input_snapshot.json",
            "attribution_source",
            "validate_next_research_direction_artifact",
            "direction_id",
        ),
        (
            "roadmap",
            "owner_research_roadmap_input_snapshot.json",
            "direction_source",
            "validate_owner_research_roadmap_artifact",
            "roadmap_id",
        ),
    ],
)
def test_research_direction_foundation_rejects_source_binding_path_alias(
    foundation_fixture: dict[str, Any],
    case_name: str,
    snapshot_name: str,
    source_key: str,
    validator_name: str,
    id_key: str,
) -> None:
    case = (
        _direction_case(foundation_fixture)
        if case_name == "direction"
        else _roadmap_case(foundation_fixture)
    )
    artifact_root, artifact_id, output_dir = case
    snapshot_path = artifact_root / snapshot_name
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        files = snapshot[source_key]["files"]
        first, second = list(files)[:2]
        files[first] = dict(files[second])
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = getattr(direction, validator_name)(
            **{id_key: artifact_id, "output_dir": output_dir}
        )
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_owner_research_roadmap_rejects_cross_artifact_lineage_tamper(
    foundation_fixture: dict[str, Any],
) -> None:
    artifact_root, artifact_id, output_dir = _roadmap_case(foundation_fixture)
    snapshot_path = artifact_root / "owner_research_roadmap_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["source_attribution_id"] = "wrong-attribution-id"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = direction.validate_owner_research_roadmap_artifact(
            roadmap_id=artifact_id, output_dir=output_dir
        )
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_legacy_public_api_forwards_to_canonical_readers(
    foundation_fixture: dict[str, Any],
) -> None:
    next_direction = foundation_fixture["next_direction"]
    roadmap = foundation_fixture["owner_roadmap"]
    direction_output = foundation_fixture["root"] / "next_research_direction"
    roadmap_output = foundation_fixture["root"] / "owner_research_roadmap"

    assert legacy.next_research_direction_report_payload(
        direction_id=next_direction["direction_id"], output_dir=direction_output
    ) == direction.next_research_direction_report_payload(
        direction_id=next_direction["direction_id"], output_dir=direction_output
    )
    assert legacy.owner_research_roadmap_report_payload(
        roadmap_id=roadmap["roadmap_id"], output_dir=roadmap_output
    ) == direction.owner_research_roadmap_report_payload(
        roadmap_id=roadmap["roadmap_id"], output_dir=roadmap_output
    )
