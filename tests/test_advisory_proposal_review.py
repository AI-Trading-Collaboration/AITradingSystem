from __future__ import annotations

from pathlib import Path
from typing import Any

from dynamic_v3_backtest_sim_helpers import run_advisory_proposal_review_fixture

from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    PROPOSAL_REVIEW_DECISIONS,
    validate_advisory_proposal_review_artifact,
)


def test_advisory_proposal_review_requires_owner_and_no_auto_apply(
    tmp_path: Path, monkeypatch: Any
) -> None:
    fixture = run_advisory_proposal_review_fixture(tmp_path, monkeypatch)
    review = fixture["proposal_review"]
    manifest = review["manifest"]
    matrix = review["proposal_decision_matrix"]

    assert matrix["auto_apply"] is False
    assert matrix["owner_approval_required"] is True
    assert matrix["position_advisory_config_mutated"] is False
    assert all(row["decision"] in PROPOSAL_REVIEW_DECISIONS for row in matrix["proposals"])
    assert all(row["auto_apply"] is False for row in matrix["proposals"])
    assert manifest["owner_approval_required"] is True
    assert manifest["broker_action_allowed"] is False
    assert (review["proposal_review_dir"] / "owner_approval_checklist.md").exists()
    assert "no production" in review["reader_brief_section"].lower()

    validation = validate_advisory_proposal_review_artifact(
        proposal_review_id=review["proposal_review_id"],
        output_dir=fixture["proposal_review_dir"],
    )
    assert validation["status"] == "PASS"
