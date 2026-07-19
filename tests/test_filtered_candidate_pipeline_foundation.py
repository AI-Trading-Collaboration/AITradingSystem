from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_owner_signal_roadmap_fixture
from typer.testing import CliRunner

from ai_trading_system.etf_portfolio import (
    dynamic_v3_filtered_candidate_pipeline as pipeline,
)
from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as legacy
from ai_trading_system.interfaces.cli.etf_portfolio import etf_app
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


@pytest.fixture(scope="module")
def pipeline_fixture(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    root = tmp_path_factory.mktemp("filtered_candidate_pipeline")
    with artifact_validation_session():
        yield {"root": root, **run_owner_signal_roadmap_fixture(root)}


def _artifact_cases(
    fixture: dict[str, Any],
) -> list[tuple[str, Path, str, Callable[..., dict[str, Any]], str, Path, tuple[str, ...]]]:
    root = fixture["root"]
    return [
        (
            "backfill",
            Path(fixture["filtered_candidate_backfill"]["filtered_backfill_dir"]),
            fixture["filtered_candidate_backfill"]["filtered_backfill_id"],
            pipeline.validate_filtered_candidate_backfill_artifact,
            "filtered_backfill_id",
            root / "filtered_candidate_backfill",
            pipeline.BACKFILL_VIEWS,
        ),
        (
            "comparison",
            Path(fixture["filtered_vs_original_comparison"]["comparison_dir"]),
            fixture["filtered_vs_original_comparison"]["comparison_id"],
            pipeline.validate_filtered_vs_original_comparison_artifact,
            "comparison_id",
            root / "filtered_vs_original_comparison",
            pipeline.COMPARISON_VIEWS,
        ),
        (
            "gate",
            Path(fixture["signal_gate_experiment"]["signal_gate_experiment_dir"]),
            fixture["signal_gate_experiment"]["signal_gate_experiment_id"],
            pipeline.validate_signal_gate_experiment_artifact,
            "signal_gate_experiment_id",
            root / "signal_gate_experiment",
            pipeline.GATE_VIEWS,
        ),
        (
            "review",
            Path(fixture["filtered_candidate_promotion_review"]["filtered_review_dir"]),
            fixture["filtered_candidate_promotion_review"]["filtered_review_id"],
            pipeline.validate_filtered_candidate_promotion_review_artifact,
            "filtered_review_id",
            root / "filtered_candidate_promotion_review",
            pipeline.REVIEW_VIEWS,
        ),
        (
            "roadmap",
            Path(fixture["owner_signal_roadmap"]["owner_signal_roadmap_dir"]),
            fixture["owner_signal_roadmap"]["owner_signal_roadmap_id"],
            pipeline.validate_owner_signal_roadmap_artifact,
            "owner_signal_roadmap_id",
            root / "owner_signal_roadmap",
            pipeline.ROADMAP_VIEWS,
        ),
    ]


def _validate_case(
    validator: Callable[..., dict[str, Any]],
    id_key: str,
    artifact_id: str,
    output_dir: Path,
) -> dict[str, Any]:
    return validator(**{id_key: artifact_id, "output_dir": output_dir})


def test_filtered_candidate_pipeline_preserves_missing_evidence(
    pipeline_fixture: dict[str, Any],
) -> None:
    backfill = pipeline_fixture["filtered_candidate_backfill"]
    comparison = pipeline_fixture["filtered_vs_original_comparison"]
    gate = pipeline_fixture["signal_gate_experiment"]
    review = pipeline_fixture["filtered_candidate_promotion_review"]
    roadmap = pipeline_fixture["owner_signal_roadmap"]

    assert backfill["manifest"]["evidence_status"] == "INSUFFICIENT_DATA"
    assert backfill["filtered_variant_specs"] == []
    assert backfill["filtered_variant_performance"] == []
    assert backfill["filtered_variant_signal_metrics"] == []

    comparison_summary = comparison["filtered_improvement_summary"]
    assert comparison["filtered_comparison_matrix"] == []
    assert comparison_summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert comparison_summary["best_filtered_variant"] is None
    assert comparison_summary["recommendation"] == "INSUFFICIENT_DATA"

    gate_summary = gate["signal_gate_experiment_summary"]
    assert gate["signal_gate_experiment_results"] == []
    assert gate_summary["tested_gate_count"] == 0
    assert gate_summary["promising_gate_count"] is None
    assert gate_summary["formalization_ready"] is False

    decision = review["filtered_promotion_decision"]
    assert decision["decision"] == "INSUFFICIENT_DATA"
    assert decision["confidence"] is None
    assert review["filtered_candidate_specs"]["candidate_variant"] is None

    roadmap_summary = roadmap["owner_signal_roadmap_summary"]
    assert roadmap_summary["evidence_status"] == "INSUFFICIENT_DATA"
    assert roadmap_summary["candidate_available"] is False
    assert roadmap_summary["next_task_family"] == "dated_signal_evidence"


def test_filtered_candidate_pipeline_snapshots_bind_exact_lineage(
    pipeline_fixture: dict[str, Any],
) -> None:
    cases = {case[0]: case for case in _artifact_cases(pipeline_fixture)}
    snapshots = {
        "backfill": json.loads(
            (cases["backfill"][1] / "filtered_candidate_backfill_input_snapshot.json")
            .read_bytes()
        ),
        "comparison": json.loads(
            (
                cases["comparison"][1]
                / "filtered_vs_original_comparison_input_snapshot.json"
            ).read_bytes()
        ),
        "gate": json.loads(
            (cases["gate"][1] / "signal_gate_experiment_input_snapshot.json").read_bytes()
        ),
        "review": json.loads(
            (
                cases["review"][1]
                / "filtered_candidate_promotion_review_input_snapshot.json"
            ).read_bytes()
        ),
        "roadmap": json.loads(
            (cases["roadmap"][1] / "owner_signal_roadmap_input_snapshot.json").read_bytes()
        ),
    }

    assert snapshots["backfill"]["schema_version"] == pipeline.BACKFILL_INPUT_SCHEMA
    assert snapshots["comparison"]["schema_version"] == pipeline.COMPARISON_INPUT_SCHEMA
    assert snapshots["gate"]["schema_version"] == pipeline.GATE_INPUT_SCHEMA
    assert snapshots["review"]["schema_version"] == pipeline.REVIEW_INPUT_SCHEMA
    assert snapshots["roadmap"]["schema_version"] == pipeline.ROADMAP_INPUT_SCHEMA
    assert (
        snapshots["backfill"]["filter_design_source"]["artifact_id"]
        == pipeline_fixture["candidate_quality_filter_design"]["filter_design_id"]
    )
    assert snapshots["comparison"]["backfill_source"]["artifact_id"] == cases["backfill"][2]
    assert snapshots["gate"]["ledger_source"]["artifact_id"] == pipeline_fixture[
        "candidate_signal_ledger"
    ]["ledger_id"]
    assert snapshots["review"]["comparison_source"]["artifact_id"] == cases["comparison"][2]
    assert snapshots["review"]["gate_source"]["artifact_id"] == cases["gate"][2]
    assert snapshots["roadmap"]["review_source"]["artifact_id"] == cases["review"][2]
    assert len({item["policy_source"]["sha256"] for item in snapshots.values()}) == 1


def test_filtered_candidate_pipeline_rebuilds_every_canonical_view(
    pipeline_fixture: dict[str, Any],
) -> None:
    for _, _, artifact_id, validator, id_key, output_dir, _ in _artifact_cases(
        pipeline_fixture
    ):
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "PASS", validation


@pytest.mark.parametrize(
    ("case_name", "view_name"),
    [
        ("backfill", view) for view in pipeline.BACKFILL_VIEWS
    ]
    + [("comparison", view) for view in pipeline.COMPARISON_VIEWS]
    + [("gate", view) for view in pipeline.GATE_VIEWS]
    + [("review", view) for view in pipeline.REVIEW_VIEWS]
    + [("roadmap", view) for view in pipeline.ROADMAP_VIEWS],
)
def test_filtered_candidate_pipeline_rejects_every_output_tamper(
    pipeline_fixture: dict[str, Any], case_name: str, view_name: str
) -> None:
    case = {item[0]: item for item in _artifact_cases(pipeline_fixture)}[case_name]
    _, artifact_root, artifact_id, validator, id_key, output_dir, _ = case
    view = artifact_root / view_name
    original = view.read_bytes()
    try:
        view.write_bytes(original + b"\nTAMPER")
        validation = _validate_case(validator, id_key, artifact_id, output_dir)
        assert validation["status"] == "FAIL"
    finally:
        view.write_bytes(original)


def test_filtered_candidate_pipeline_rejects_live_source_tamper(
    pipeline_fixture: dict[str, Any],
) -> None:
    design_root = Path(pipeline_fixture["candidate_quality_filter_design"]["filter_design_dir"])
    design_manifest = design_root / "candidate_quality_filter_manifest.json"
    original = design_manifest.read_bytes()
    try:
        design_manifest.write_bytes(original + b"\n")
        backfill = {item[0]: item for item in _artifact_cases(pipeline_fixture)}["backfill"]
        validation = _validate_case(backfill[3], backfill[4], backfill[2], backfill[5])
        assert validation["status"] == "FAIL"
    finally:
        design_manifest.write_bytes(original)


def test_filtered_candidate_pipeline_rejects_cross_lineage_tamper(
    pipeline_fixture: dict[str, Any],
) -> None:
    case = {item[0]: item for item in _artifact_cases(pipeline_fixture)}["comparison"]
    snapshot_path = case[1] / "filtered_vs_original_comparison_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["backfill_source"]["artifact_id"] = "wrong-backfill"
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(case[3], case[4], case[2], case[5])
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_filtered_candidate_pipeline_rejects_chronology_tamper(
    pipeline_fixture: dict[str, Any],
) -> None:
    case = {item[0]: item for item in _artifact_cases(pipeline_fixture)}["roadmap"]
    snapshot_path = case[1] / "owner_signal_roadmap_input_snapshot.json"
    original = snapshot_path.read_bytes()
    try:
        snapshot = json.loads(original)
        snapshot["generated_at"] = datetime(2000, 1, 1, tzinfo=UTC).isoformat()
        snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        validation = _validate_case(case[3], case[4], case[2], case[5])
        assert validation["status"] == "FAIL"
    finally:
        snapshot_path.write_bytes(original)


def test_filtered_candidate_report_readers_preserve_pre_eb2_artifacts(
    pipeline_fixture: dict[str, Any],
) -> None:
    readers = {
        "backfill": (
            pipeline.filtered_candidate_backfill_report_payload,
            "filtered_backfill_id",
            "filtered_candidate_backfill_input_snapshot.json",
        ),
        "comparison": (
            pipeline.filtered_vs_original_comparison_report_payload,
            "comparison_id",
            "filtered_vs_original_comparison_input_snapshot.json",
        ),
        "gate": (
            pipeline.signal_gate_experiment_report_payload,
            "signal_gate_experiment_id",
            "signal_gate_experiment_input_snapshot.json",
        ),
        "review": (
            pipeline.filtered_candidate_promotion_review_report_payload,
            "filtered_review_id",
            "filtered_candidate_promotion_review_input_snapshot.json",
        ),
        "roadmap": (
            pipeline.owner_signal_roadmap_report_payload,
            "owner_signal_roadmap_id",
            "owner_signal_roadmap_input_snapshot.json",
        ),
    }
    for case_name, artifact_root, artifact_id, _, _, output_dir, _ in _artifact_cases(
        pipeline_fixture
    ):
        reader, id_key, snapshot_name = readers[case_name]
        snapshot_path = artifact_root / snapshot_name
        original = snapshot_path.read_bytes()
        try:
            snapshot_path.unlink()
            payload = reader(**{id_key: artifact_id, "output_dir": output_dir})
            assert "input_snapshot" not in payload
        finally:
            snapshot_path.write_bytes(original)


def test_filtered_candidate_legacy_api_and_cli_use_canonical_owner(
    pipeline_fixture: dict[str, Any],
) -> None:
    roadmap = pipeline_fixture["owner_signal_roadmap"]
    output_dir = pipeline_fixture["root"] / "owner_signal_roadmap"
    canonical = pipeline.owner_signal_roadmap_report_payload(
        owner_signal_roadmap_id=roadmap["owner_signal_roadmap_id"],
        output_dir=output_dir,
    )
    compatibility = legacy.owner_signal_roadmap_report_payload(
        owner_signal_roadmap_id=roadmap["owner_signal_roadmap_id"],
        output_dir=output_dir,
    )
    assert compatibility == canonical

    result = CliRunner().invoke(
        etf_app,
        [
            "dynamic-v3-rescue",
            "owner-signal-roadmap",
            "report",
            "--owner-signal-roadmap-id",
            roadmap["owner_signal_roadmap_id"],
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"owner_signal_roadmap_id={roadmap['owner_signal_roadmap_id']}" in result.output
