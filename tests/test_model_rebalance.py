from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from dynamic_v3_system_target_helpers import build_rebalanced_shadow_fixture

from ai_trading_system.etf_portfolio import dynamic_v3_system_target as system_target


def test_model_rebalance_emits_append_only_paper_shadow_post_state(tmp_path) -> None:
    fixture = build_rebalanced_shadow_fixture(tmp_path)
    rebalance = fixture["rebalance"]
    summary = rebalance["rebalance_turnover_summary"]

    assert summary["paper_shadow_only"] is True
    assert summary["broker_action_taken"] is False
    assert summary["skipped_methods"] == []
    assert summary["total_turnover"] > 0
    assert set(summary["applied_methods"]) == set(system_target.TARGET_METHODS)

    state_path = fixture["paper"]["paper_shadow_dir"] / "paper_shadow_state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["state_status"] == "INITIALIZED"

    post_state_path = rebalance["rebalance_dir"] / "paper_shadow_state_after.json"
    post_state = json.loads(post_state_path.read_text(encoding="utf-8"))
    assert post_state["state_status"] == "REBALANCED_TO_MODEL_TARGET"
    assert post_state["source_model_target_id"] == fixture["target"]["target_id"]
    assert all(row["broker_action_taken"] is False for row in post_state["method_states"])

    validation = system_target.validate_model_rebalance_artifact(
        rebalance_id=rebalance["rebalance_id"],
        output_dir=tmp_path / "model_rebalance",
    )
    assert validation["status"] == "PASS"


def test_model_rebalance_rejects_duplicate_pair_and_time_rollback(tmp_path) -> None:
    fixture = build_rebalanced_shadow_fixture(tmp_path)
    paper_id = fixture["paper"]["paper_shadow_id"]
    target_id = fixture["target"]["target_id"]

    with pytest.raises(ValueError, match="already exists"):
        system_target.simulate_model_rebalance(
            paper_shadow_id=paper_id,
            target_id=target_id,
            paper_shadow_dir=tmp_path / "paper_shadow",
            model_target_dir=tmp_path / "model_target",
            output_dir=tmp_path / "model_rebalance",
            generated_at=datetime(2026, 1, 5, 3, tzinfo=UTC),
        )

    with pytest.raises(ValueError, match="source after cutoff"):
        system_target.simulate_model_rebalance(
            paper_shadow_id=paper_id,
            target_id=target_id,
            paper_shadow_dir=tmp_path / "paper_shadow",
            model_target_dir=tmp_path / "model_target",
            output_dir=tmp_path / "rollback_rebalance",
            generated_at=datetime(2026, 1, 4, tzinfo=UTC),
        )
