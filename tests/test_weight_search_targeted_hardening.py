from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_near_miss_ab_comparison_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_targeted as targeted


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _assert_fail(validator: Callable[[], dict[str, Any]]) -> None:
    payload = validator()
    assert payload["status"] == "FAIL"
    assert payload["failed_check_count"] > 0


def _tamper_snapshot_field(
    path: Path,
    validator: Callable[[], dict[str, Any]],
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    original = path.read_bytes()
    try:
        payload = json.loads(original.decode("utf-8"))
        mutate(payload)
        _write_json(path, payload)
        _assert_fail(validator)
    finally:
        path.write_bytes(original)


def test_targeted_chain_rebuilds_all_views_and_fails_closed(tmp_path: Path) -> None:
    fixture = run_near_miss_ab_comparison_fixture(tmp_path)
    matrix = fixture["targeted_v3"]
    backfill = fixture["targeted_v3_backfill"]
    comparison = fixture["near_miss_ab"]

    matrix_root = Path(matrix["v3_matrix_dir"])
    backfill_root = Path(backfill["v3_backfill_dir"])
    ab_root = Path(comparison["ab_dir"])

    def validate_matrix() -> dict[str, Any]:
        return targeted.validate_targeted_search_v3_artifact(
            v3_matrix_id=matrix["v3_matrix_id"],
            output_dir=tmp_path / "targeted_search_v3",
        )

    def validate_backfill() -> dict[str, Any]:
        return targeted.validate_targeted_v3_backfill_artifact(
            v3_backfill_id=backfill["v3_backfill_id"],
            output_dir=tmp_path / "targeted_v3_backfill",
        )

    def validate_ab() -> dict[str, Any]:
        return targeted.validate_near_miss_ab_comparison_artifact(
            ab_id=comparison["ab_id"],
            output_dir=tmp_path / "near_miss_ab_comparison",
        )

    stages = (
        (
            matrix_root,
            "targeted_search_v3_input_snapshot.json",
            targeted.MATRIX_INPUT_SCHEMA,
            targeted.MATRIX_VIEWS,
            validate_matrix,
        ),
        (
            backfill_root,
            "targeted_v3_backfill_input_snapshot.json",
            targeted.BACKFILL_INPUT_SCHEMA,
            targeted.BACKFILL_VIEWS,
            validate_backfill,
        ),
        (
            ab_root,
            "near_miss_ab_comparison_input_snapshot.json",
            targeted.AB_INPUT_SCHEMA,
            targeted.AB_VIEWS,
            validate_ab,
        ),
    )

    assert sum(len(view_names) for _, _, _, view_names, _ in stages) == 16
    for root, snapshot_name, expected_schema, view_names, validator in stages:
        snapshot_path = root / snapshot_name
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        assert snapshot["schema_version"] == expected_schema
        assert set(snapshot["view_hashes"]) == set(view_names)

        for view_name in view_names:
            view_path = root / view_name
            original_view = view_path.read_bytes()
            try:
                view_path.write_bytes(original_view + b"\n")
                _assert_fail(validator)
            finally:
                view_path.write_bytes(original_view)

        _tamper_snapshot_field(
            snapshot_path,
            validator,
            lambda payload: payload.__setitem__("schema_version", "tampered.schema"),
        )

    lineage_cases = (
        (
            matrix_root / "targeted_search_v3_input_snapshot.json",
            "coverage_source",
            validate_matrix,
        ),
        (
            backfill_root / "targeted_v3_backfill_input_snapshot.json",
            "matrix_source",
            validate_backfill,
        ),
        (
            ab_root / "near_miss_ab_comparison_input_snapshot.json",
            "backfill_source",
            validate_ab,
        ),
    )
    for snapshot_path, binding_name, validator in lineage_cases:
        _tamper_snapshot_field(
            snapshot_path,
            validator,
            lambda payload, binding_name=binding_name: payload[binding_name].__setitem__(
                "artifact_id", "cross-lineage-artifact"
            ),
        )

    matrix_snapshot_path = matrix_root / "targeted_search_v3_input_snapshot.json"
    _tamper_snapshot_field(
        matrix_snapshot_path,
        validate_matrix,
        lambda payload: payload["policy_source"].__setitem__("sha256", "0" * 64),
    )

    backfill_snapshot_path = backfill_root / "targeted_v3_backfill_input_snapshot.json"
    for binding_name in ("price_source", "rates_source"):
        _tamper_snapshot_field(
            backfill_snapshot_path,
            validate_backfill,
            lambda payload, binding_name=binding_name: payload[binding_name].__setitem__(
                "sha256", "0" * 64
            ),
        )

    progress_path = backfill_root / targeted.BACKFILL_VIEWS[1]
    original_progress = progress_path.read_bytes()
    try:
        progress_path.write_bytes(original_progress + b"\n")
        with pytest.raises(ValueError, match="validation failed before resume"):
            targeted.resume_targeted_v3_backfill(
                v3_backfill_id=backfill["v3_backfill_id"],
                output_dir=tmp_path / "targeted_v3_backfill",
            )
    finally:
        progress_path.write_bytes(original_progress)

    matrix_manifest = matrix_root / targeted.MATRIX_VIEWS[0]
    held_manifest = matrix_manifest.with_suffix(".held")
    matrix_manifest.rename(held_manifest)
    try:
        with pytest.raises(ValueError, match="validation failed before resume"):
            targeted.resume_targeted_v3_backfill(
                v3_backfill_id=backfill["v3_backfill_id"],
                output_dir=tmp_path / "targeted_v3_backfill",
            )
    finally:
        held_manifest.rename(matrix_manifest)

    early_output = tmp_path / "chronology_must_not_materialize"
    with pytest.raises(ValueError, match="source chronology invalid"):
        targeted.build_targeted_search_v3(
            coverage_gap_id=fixture["coverage_gap"]["coverage_gap_id"],
            coverage_gap_dir=tmp_path / "search_coverage_gap",
            near_miss_dir=tmp_path / "near_miss_candidates",
            output_dir=early_output,
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    assert not early_output.exists()
