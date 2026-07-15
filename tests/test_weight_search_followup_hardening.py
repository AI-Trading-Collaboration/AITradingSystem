from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_weight_batch_search_helpers import run_next_formal_or_search_plan_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_weight_search_followup as followup
from ai_trading_system.platform.artifacts.validation_session import artifact_validation_session


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


def test_followup_chain_rebuilds_all_views_and_invalidates_cached_pass(
    tmp_path: Path,
) -> None:
    fixture = run_next_formal_or_search_plan_fixture(tmp_path)
    sensitivity = fixture["sensitivity"]
    promotion = fixture["promotion_v2"]
    next_plan = fixture["next_plan"]

    sensitivity_root = Path(sensitivity["sensitivity_dir"])
    promotion_root = Path(promotion["promotion_v2_dir"])
    next_plan_root = Path(next_plan["plan_dir"])

    def validate_sensitivity() -> dict[str, Any]:
        return followup.validate_promotion_threshold_sensitivity_artifact(
            sensitivity_id=sensitivity["sensitivity_id"],
            output_dir=tmp_path / "promotion_threshold_sensitivity",
        )

    def validate_promotion() -> dict[str, Any]:
        return followup.validate_candidate_promotion_v2_artifact(
            promotion_v2_id=promotion["promotion_v2_id"],
            output_dir=tmp_path / "candidate_promotion_v2",
        )

    def validate_next_plan() -> dict[str, Any]:
        return followup.validate_next_formal_or_search_plan_artifact(
            plan_id=next_plan["plan_id"],
            output_dir=tmp_path / "next_formal_or_search_plan",
        )

    stages = (
        (
            sensitivity_root,
            "promotion_threshold_sensitivity_input_snapshot.json",
            followup.SENSITIVITY_INPUT_SCHEMA,
            followup.SENSITIVITY_VIEWS,
            validate_sensitivity,
        ),
        (
            promotion_root,
            "candidate_promotion_v2_input_snapshot.json",
            followup.PROMOTION_INPUT_SCHEMA,
            followup.PROMOTION_VIEWS,
            validate_promotion,
        ),
        (
            next_plan_root,
            "next_formal_or_search_plan_input_snapshot.json",
            followup.NEXT_PLAN_INPUT_SCHEMA,
            followup.NEXT_PLAN_VIEWS,
            validate_next_plan,
        ),
    )

    assert sum(len(view_names) for _, _, _, view_names, _ in stages) == 18
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
            sensitivity_root / "promotion_threshold_sensitivity_input_snapshot.json",
            "ab_source",
            validate_sensitivity,
        ),
        (
            promotion_root / "candidate_promotion_v2_input_snapshot.json",
            "sensitivity_source",
            validate_promotion,
        ),
        (
            next_plan_root / "next_formal_or_search_plan_input_snapshot.json",
            "promotion_source",
            validate_next_plan,
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

    _tamper_snapshot_field(
        sensitivity_root / "promotion_threshold_sensitivity_input_snapshot.json",
        validate_sensitivity,
        lambda payload: payload["policy_source"].__setitem__("sha256", "0" * 64),
    )

    price_path = fixture["prices_path"]
    original_prices = price_path.read_bytes()
    with artifact_validation_session():
        assert validate_next_plan()["status"] == "PASS"
        try:
            price_path.write_bytes(original_prices + b"\n")
            _assert_fail(validate_next_plan)
        finally:
            price_path.write_bytes(original_prices)
        assert validate_next_plan()["status"] == "PASS"

    early_outputs = (
        tmp_path / "early_sensitivity",
        tmp_path / "early_promotion",
        tmp_path / "early_next_plan",
    )
    with pytest.raises(ValueError, match="source chronology invalid"):
        followup.run_promotion_threshold_sensitivity(
            v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
            ab_id=fixture["near_miss_ab"]["ab_id"],
            v3_backfill_dir=tmp_path / "targeted_v3_backfill",
            v3_matrix_dir=tmp_path / "targeted_search_v3",
            ab_dir=tmp_path / "near_miss_ab_comparison",
            output_dir=early_outputs[0],
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="source chronology invalid"):
        followup.run_candidate_promotion_v2(
            v3_backfill_id=fixture["targeted_v3_backfill"]["v3_backfill_id"],
            ab_id=fixture["near_miss_ab"]["ab_id"],
            sensitivity_id=sensitivity["sensitivity_id"],
            v3_backfill_dir=tmp_path / "targeted_v3_backfill",
            v3_matrix_dir=tmp_path / "targeted_search_v3",
            ab_dir=tmp_path / "near_miss_ab_comparison",
            sensitivity_dir=tmp_path / "promotion_threshold_sensitivity",
            output_dir=early_outputs[1],
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    with pytest.raises(ValueError, match="source chronology invalid"):
        followup.run_next_formal_or_search_plan(
            promotion_v2_id=promotion["promotion_v2_id"],
            promotion_v2_dir=tmp_path / "candidate_promotion_v2",
            output_dir=early_outputs[2],
            generated_at=datetime(2000, 1, 1, tzinfo=UTC),
        )
    assert all(not path.exists() for path in early_outputs)
