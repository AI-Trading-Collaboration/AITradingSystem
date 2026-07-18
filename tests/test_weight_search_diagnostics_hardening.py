from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_search_coverage_gap_fixture

from ai_trading_system.etf_portfolio import (
    dynamic_v3_weight_search_diagnostics as diagnostics,
)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _assert_fail(validator: Callable[[], dict[str, Any]]) -> None:
    payload = validator()
    assert payload["status"] == "FAIL"
    assert payload["failed_check_count"] > 0


def test_diagnostics_chain_rebuilds_every_view_and_fails_closed(tmp_path: Path) -> None:
    fixture = run_search_coverage_gap_fixture(tmp_path, compact_test_matrix=True)
    review = fixture["no_promotion_review"]
    near_miss = fixture["near_miss"]
    attribution = fixture["cash_buffer_attribution"]
    coverage = fixture["coverage_gap"]

    stages = (
        (
            Path(review["review_dir"]),
            "no_promotion_review_input_snapshot.json",
            diagnostics.REVIEW_INPUT_SCHEMA,
            lambda: diagnostics.validate_no_promotion_review_artifact(
                review_id=review["review_id"],
                output_dir=tmp_path / "no_promotion_review",
            ),
        ),
        (
            Path(near_miss["near_miss_dir"]),
            "near_miss_candidates_input_snapshot.json",
            diagnostics.NEAR_MISS_INPUT_SCHEMA,
            lambda: diagnostics.validate_near_miss_candidates_artifact(
                near_miss_id=near_miss["near_miss_id"],
                output_dir=tmp_path / "near_miss_candidates",
            ),
        ),
        (
            Path(attribution["attribution_dir"]),
            "cash_buffer_attribution_input_snapshot.json",
            diagnostics.CASH_INPUT_SCHEMA,
            lambda: diagnostics.validate_cash_buffer_attribution_artifact(
                attribution_id=attribution["attribution_id"],
                output_dir=tmp_path / "cash_buffer_attribution",
            ),
        ),
        (
            Path(coverage["coverage_gap_dir"]),
            "search_coverage_gap_input_snapshot.json",
            diagnostics.COVERAGE_INPUT_SCHEMA,
            lambda: diagnostics.validate_search_coverage_gap_artifact(
                coverage_gap_id=coverage["coverage_gap_id"],
                output_dir=tmp_path / "search_coverage_gap",
            ),
        ),
    )

    assert (
        sum(
            len(json.loads((root / snapshot).read_text(encoding="utf-8"))["view_hashes"])
            for root, snapshot, _, _ in stages
        )
        == 21
    )

    for root, snapshot_name, expected_schema, validator in stages:
        assert validator()["status"] == "PASS"
        snapshot_path = root / snapshot_name
        original_snapshot = snapshot_path.read_bytes()
        snapshot = json.loads(original_snapshot)
        assert snapshot["schema_version"] == expected_schema

        for view_name in snapshot["view_hashes"]:
            view_path = root / view_name
            original_view = view_path.read_bytes()
            view_path.write_bytes(original_view + b"\n")
            _assert_fail(validator)
            view_path.write_bytes(original_view)

        snapshot["schema_version"] = "tampered.schema"
        _write_json(snapshot_path, snapshot)
        _assert_fail(validator)
        snapshot_path.write_bytes(original_snapshot)
        assert validator()["status"] == "PASS"

    tamper_cases = (
        (
            Path(near_miss["near_miss_dir"]) / "near_miss_candidates_input_snapshot.json",
            "review_source",
            stages[1][3],
        ),
        (
            Path(attribution["attribution_dir"]) / "cash_buffer_attribution_input_snapshot.json",
            "near_miss_source",
            stages[2][3],
        ),
        (
            Path(coverage["coverage_gap_dir"]) / "search_coverage_gap_input_snapshot.json",
            "attribution_source",
            stages[3][3],
        ),
    )
    for snapshot_path, binding_name, validator in tamper_cases:
        original = snapshot_path.read_bytes()
        snapshot = json.loads(original)
        snapshot[binding_name]["artifact_id"] = "cross-lineage-artifact"
        _write_json(snapshot_path, snapshot)
        _assert_fail(validator)
        snapshot_path.write_bytes(original)

    review_snapshot_path = Path(review["review_dir"]) / "no_promotion_review_input_snapshot.json"
    original = review_snapshot_path.read_bytes()
    snapshot = json.loads(original)
    snapshot["policy_source"]["sha256"] = "0" * 64
    _write_json(review_snapshot_path, snapshot)
    _assert_fail(stages[0][3])
    review_snapshot_path.write_bytes(original)

    early_output = tmp_path / "chronology_must_not_materialize"
    with pytest.raises(diagnostics.DynamicV3WeightSearchDiagnosticsError):
        diagnostics.run_no_promotion_review(
            scorecard_id=fixture["scorecard"]["scorecard_id"],
            scorecard_dir=tmp_path / "weight_scorecard",
            output_dir=early_output,
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    assert not early_output.exists()
