from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.core import (
    ArtifactRef,
    ProductionEffect,
    WorkflowStep,
    WorkflowStepResult,
)


def test_artifact_ref_matches_run_manifest_record_contract(tmp_path: Path) -> None:
    artifact_path = tmp_path / "reports" / "daily_ops_run_2026-05-06.md"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_text("# run\n", encoding="utf-8")

    record = ArtifactRef.from_path(artifact_path).to_manifest_record()
    artifact_bytes = artifact_path.read_bytes()

    assert record == {
        "path": str(artifact_path),
        "exists": True,
        "artifact_type": "md",
        "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
        "size_bytes": len(artifact_bytes),
        "file_count": None,
    }


def test_artifact_ref_records_directories_and_missing_paths(tmp_path: Path) -> None:
    directory = tmp_path / "bundle"
    directory.mkdir()
    (directory / "manifest.json").write_text("{}\n", encoding="utf-8")
    missing = tmp_path / "missing.csv"

    directory_record = ArtifactRef.from_path(directory).to_manifest_record()
    missing_record = ArtifactRef.from_path(missing).to_manifest_record()

    assert directory_record["artifact_type"] == "directory"
    assert directory_record["file_count"] == 1
    assert directory_record["sha256"] is None
    assert missing_record == {
        "path": str(missing),
        "exists": False,
        "artifact_type": "csv",
        "sha256": None,
        "size_bytes": None,
        "file_count": None,
    }


def test_production_effect_parse_and_boundary_flags() -> None:
    assert ProductionEffect.parse(" none ") is ProductionEffect.NONE
    assert ProductionEffect.parse(None, default=ProductionEffect.NONE) is ProductionEffect.NONE
    assert ProductionEffect.PRODUCTION.affects_production is True
    assert ProductionEffect.ADVISORY.affects_production is True
    assert ProductionEffect.VALIDATION_ONLY.affects_production is False

    with pytest.raises(ValueError, match="unknown production_effect"):
        ProductionEffect.parse("report_only")


def test_workflow_contract_records_step_boundaries(tmp_path: Path) -> None:
    output = ArtifactRef.from_path(tmp_path / "daily_ops_run_2026-05-06.md")
    step = WorkflowStep(
        step_id="daily_ops_run",
        name="Daily ops run",
        command_name="aits ops daily-run",
        command=("aits", "ops", "daily-run", "--as-of", "2026-05-06"),
        expected_outputs=(output,),
        blocking=True,
    )
    result = WorkflowStepResult(
        step_id=step.step_id,
        status="PASS",
        started_at=datetime(2026, 5, 6, 21, 0, tzinfo=UTC),
        finished_at=datetime(2026, 5, 6, 21, 1, tzinfo=UTC),
        artifacts=step.expected_outputs,
        key_conclusions=("manifest contract unchanged",),
    )

    assert step.production_effect is ProductionEffect.NONE
    assert result.production_effect is ProductionEffect.NONE
    assert result.artifacts[0].artifact_type == "md"
