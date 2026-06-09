from __future__ import annotations

from pathlib import Path
from typing import Any

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
