from __future__ import annotations

from datetime import datetime, timedelta
from shutil import copyfile

import pytest
from dynamic_v3_defensive_evidence_helpers import (
    run_defensive_deep_dive_fixture,
    run_label_review_fixture,
)

from ai_trading_system.etf_portfolio.dynamic_v3_defensive_research import (
    DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH,
    DynamicV3DefensiveResearchError,
    run_defensive_label_review,
    validate_defensive_label_review_artifact,
)


def test_defensive_label_review_keeps_label_change_owner_gated(tmp_path):
    fixture = run_label_review_fixture(tmp_path)
    review = fixture["defensive_label_review"]
    matrix = review["label_decision_matrix"]

    assert matrix["label_status"] == "POTENTIALLY_MISLEADING"
    assert matrix["recommended_label"] == "risk_aware_limited_adjustment"
    assert matrix["auto_rename"] is False
    assert matrix["config_change_allowed"] is False
    assert matrix["policy_change_allowed"] is False
    assert matrix["broker_action_allowed"] is False
    assert matrix["production_effect"] == "none"

    validation = validate_defensive_label_review_artifact(
        label_review_id=review["label_review_id"],
        output_dir=fixture["defensive_label_review_dir"],
    )

    assert validation["status"] == "PASS"
    assert validation["failed_check_count"] == 0


def test_defensive_label_review_rejects_naive_or_pre_source_generated_time(tmp_path):
    fixture = run_label_review_fixture(tmp_path)
    deep = fixture["defensive_hypothesis_deep_dive"]
    with pytest.raises(DynamicV3DefensiveResearchError, match="timezone-aware"):
        run_defensive_label_review(
            deep_dive_id=deep["deep_dive_id"],
            deep_dive_dir=fixture["defensive_hypothesis_deep_dive_dir"],
            output_dir=tmp_path / "naive-label",
            generated_at=datetime.fromisoformat(deep["input_snapshot"]["generated_at"]).replace(
                tzinfo=None
            ),
        )
    source_time = datetime.fromisoformat(deep["input_snapshot"]["generated_at"])
    with pytest.raises(DynamicV3DefensiveResearchError, match="generated after cutoff"):
        run_defensive_label_review(
            deep_dive_id=deep["deep_dive_id"],
            deep_dive_dir=fixture["defensive_hypothesis_deep_dive_dir"],
            output_dir=tmp_path / "early-label",
            generated_at=source_time - timedelta(seconds=1),
        )


def test_defensive_label_review_validator_rejects_live_policy_drift(tmp_path):
    fixture = run_defensive_deep_dive_fixture(tmp_path)
    policy_path = tmp_path / "defensive_research_policy.yaml"
    copyfile(DEFAULT_DEFENSIVE_RESEARCH_POLICY_PATH, policy_path)
    review = run_defensive_label_review(
        deep_dive_id=fixture["defensive_hypothesis_deep_dive"]["deep_dive_id"],
        deep_dive_dir=fixture["defensive_hypothesis_deep_dive_dir"],
        output_dir=tmp_path / "label-policy-drift",
        policy_path=policy_path,
        generated_at=datetime.fromisoformat(
            fixture["defensive_hypothesis_deep_dive"]["input_snapshot"]["generated_at"]
        ),
    )
    policy_path.write_text(
        policy_path.read_text(encoding="utf-8").replace(
            "version: defensive_research_synthesis_v1",
            "version: defensive_research_synthesis_v1_tampered",
        ),
        encoding="utf-8",
    )

    validation = validate_defensive_label_review_artifact(
        label_review_id=review["label_review_id"],
        output_dir=tmp_path / "label-policy-drift",
    )
    assert validation["status"] == "FAIL"
