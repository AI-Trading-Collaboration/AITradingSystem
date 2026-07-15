from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_weight_batch_backfill_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_batch_search as weight_search


def _tamper_bytes_and_expect_fail(path: Path, validator: Callable[[], dict[str, Any]]) -> None:
    original = path.read_bytes()
    try:
        path.write_bytes(original + b" ")
        assert validator()["status"] == "FAIL", path.name
    finally:
        path.write_bytes(original)


def _tamper_snapshot_schema_and_expect_fail(
    path: Path, validator: Callable[[], dict[str, Any]]
) -> None:
    original = path.read_bytes()
    try:
        payload = json.loads(original)
        payload["schema_version"] = "tampered.schema"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        assert validator()["status"] == "FAIL", path.name
    finally:
        path.write_bytes(original)


def test_weight_search_foundation_chain_rebuilds_views_and_rejects_tamper(tmp_path) -> None:
    fixture = run_weight_batch_backfill_fixture(tmp_path)
    search = fixture["search_space"]
    matrix = fixture["matrix"]
    backfill = fixture["weight_backfill"]
    search_root = Path(search["search_space_dir"])
    matrix_root = Path(matrix["matrix_dir"])
    backfill_root = Path(backfill["backfill_dir"])

    def validate_search() -> dict[str, Any]:
        return weight_search.validate_weight_search_space_artifact(
            search_space_id=search["search_space_id"], output_dir=search_root.parent
        )

    def validate_matrix() -> dict[str, Any]:
        return weight_search.validate_weight_experiment_batch2_artifact(
            matrix_id=matrix["matrix_id"], output_dir=matrix_root.parent
        )

    def validate_backfill() -> dict[str, Any]:
        return weight_search.validate_weight_batch_backfill_artifact(
            backfill_id=backfill["batch_backfill_id"], output_dir=backfill_root.parent
        )

    assert validate_search()["status"] == "PASS"
    assert validate_matrix()["status"] == "PASS"
    assert validate_backfill()["status"] == "PASS"

    for name in (
        "weight_search_space_manifest.json",
        "normalized_search_space.yaml",
        "search_family_inventory.json",
        "weight_search_space_report.md",
    ):
        _tamper_bytes_and_expect_fail(search_root / name, validate_search)
    _tamper_snapshot_schema_and_expect_fail(
        search_root / "weight_search_space_input_snapshot.json", validate_search
    )

    for name in (
        "batch2_matrix_manifest.json",
        "batch2_variant_specs.jsonl",
        "batch2_family_coverage.json",
        "batch2_matrix_report.md",
    ):
        _tamper_bytes_and_expect_fail(matrix_root / name, validate_matrix)
    _tamper_snapshot_schema_and_expect_fail(
        matrix_root / "weight_experiment_batch2_input_snapshot.json", validate_matrix
    )

    for name in (
        "batch_backfill_manifest.json",
        "batch_backfill_progress.json",
        "validate_data_quality_report.md",
        "variant_weight_paths.jsonl",
        "variant_performance_metrics.jsonl",
        "variant_regime_metrics.jsonl",
        "variant_stability_metrics.jsonl",
        "variant_churn_metrics.jsonl",
        "variant_lag_metrics.jsonl",
        "batch_backfill_report.md",
    ):
        _tamper_bytes_and_expect_fail(backfill_root / name, validate_backfill)
    _tamper_snapshot_schema_and_expect_fail(
        backfill_root / "weight_batch_backfill_input_snapshot.json", validate_backfill
    )

    config_path = Path(fixture["weight_search_config_path"])
    original_config = config_path.read_bytes()
    try:
        config_path.write_bytes(original_config + b"\n# tampered\n")
        assert validate_search()["status"] == "FAIL"
    finally:
        config_path.write_bytes(original_config)

    matrix_snapshot_path = matrix_root / "weight_experiment_batch2_input_snapshot.json"
    original_matrix_snapshot = matrix_snapshot_path.read_bytes()
    try:
        payload = json.loads(original_matrix_snapshot)
        payload["search_source"]["artifact_id"] = "cross-lineage-search-space"
        matrix_snapshot_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        assert validate_matrix()["status"] == "FAIL"
    finally:
        matrix_snapshot_path.write_bytes(original_matrix_snapshot)

    progress_path = backfill_root / "batch_backfill_progress.json"
    original_progress = progress_path.read_bytes()
    try:
        progress_path.write_bytes(original_progress + b" ")
        with pytest.raises(ValueError, match="backfill validation failed before resume"):
            weight_search.resume_weight_batch_backfill(
                backfill_id=backfill["batch_backfill_id"], output_dir=backfill_root.parent
            )
    finally:
        progress_path.write_bytes(original_progress)
