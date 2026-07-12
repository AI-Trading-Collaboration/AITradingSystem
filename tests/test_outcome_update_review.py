from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from dynamic_v3_outcome_loop_helpers import build_ready_outcome_update_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_outcome_accumulation as accumulation


def test_outcome_update_review_builds_ready_pack_without_future_decision_data(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    fixture = build_ready_outcome_update_fixture(tmp_path, monkeypatch)
    result = fixture["update_review"]
    safety = result["update_safety_checks"]
    impact = result["update_impact_preview"]
    rows = result["update_ready_review_matrix"]

    assert safety["ready_to_update_count"] == 1
    assert safety["blocked_count"] == 0
    assert safety["future_data_used_in_decision"] is False
    assert impact["expected_forward_available_delta"] == 1
    assert any(row["review_status"] == "READY_TO_UPDATE" for row in rows)
    assert all(row["future_data_used_in_decision"] is False for row in rows)
    assert (
        accumulation.validate_outcome_update_review_artifact(
            review_id=result["update_review_id"],
            output_dir=tmp_path / "outcome_update_review",
        )["status"]
        == "PASS"
    )

    matrix_path = Path(result["review_dir"]) / "update_ready_review_matrix.jsonl"
    original = matrix_path.read_text(encoding="utf-8")
    matrix_path.write_text(original.replace("READY_TO_UPDATE", "BLOCKED"), encoding="utf-8")
    assert (
        accumulation.validate_outcome_update_review_artifact(
            review_id=result["update_review_id"],
            output_dir=tmp_path / "outcome_update_review",
        )["status"]
        == "FAIL"
    )
    matrix_path.write_text(original, encoding="utf-8")


def test_outcome_update_review_empty_is_valid_insufficient_and_duplicate_fails() -> None:
    snapshot = {
        "due_id": "due-1",
        "as_of": "2026-06-10",
        "generated_at": "2026-06-10T12:00:00+00:00",
        "generated_cutoff": "2026-06-10T12:00:00+00:00",
        "due_source_bundle": {
            "files": {
                "outcome_due_manifest.json": {
                    "content": {"due_id": "due-1", "as_of": "2026-06-10"}
                },
                "due_window_inventory.jsonl": {"content": []},
            }
        },
    }
    rows, safety, impact, status = accumulation._outcome_update_review_views(snapshot)
    assert rows == []
    assert safety["ready_to_update_count"] == 0
    assert impact["expected_forward_available_delta"] == 0
    assert status == "INSUFFICIENT_DATA"

    source_row = {
        "outcome_id": "outcome-1",
        "window_days": 5,
        "due_status": "DUE",
        "current_outcome_status": "PENDING",
        "latest_price_date": "2026-06-10",
        "can_update": True,
    }
    snapshot["due_source_bundle"]["files"]["due_window_inventory.jsonl"]["content"] = [
        source_row,
        dict(source_row),
    ]
    with pytest.raises(accumulation.DynamicV3OutcomeAccumulationError):
        accumulation._outcome_update_review_views(snapshot)
