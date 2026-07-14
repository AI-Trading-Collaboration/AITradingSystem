from __future__ import annotations

import copy

import pytest
import yaml

from ai_trading_system.etf_portfolio import (
    dynamic_v3_system_target_smoothed_readiness as readiness,
)


def _policy() -> dict[str, object]:
    config = yaml.safe_load(
        readiness.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    return readiness._readiness_policy(config)


def test_readiness_policy_never_scores_missing_or_not_registered_evidence() -> None:
    policy = _policy()

    assert policy["missing_component_score"] is None
    assert "INSUFFICIENT_DATA" not in policy["status_scores"]
    assert "INSUFFICIENT_EVIDENCE" not in policy["status_scores"]
    assert "NOT_REGISTERED" not in policy["forward_status_scores"]
    assert readiness._status_score("INSUFFICIENT_DATA", policy) is None


def test_churn_delta_rejects_nonzero_turnover_without_explicit_deltas() -> None:
    with pytest.raises(
        readiness.DynamicV3SmoothedReadinessError,
        match="missing deltas for nonzero turnover",
    ):
        readiness._ledger_delta(
            {"turnover": 0.1, "deltas": {}},
            "smooth_weights_3d_limited_adjustment",
        )


def test_churn_delta_requires_turnover_identity() -> None:
    with pytest.raises(
        readiness.DynamicV3SmoothedReadinessError,
        match="turnover/delta identity mismatch",
    ):
        readiness._ledger_delta(
            {
                "turnover": 0.1,
                "deltas": {"QQQ": 0.05, "CASH": -0.05},
            },
            "smooth_weights_5d_limited_adjustment",
        )


def test_candidate_less_readiness_cannot_promote_even_with_high_diagnostic_scores() -> None:
    scorecard = {
        "candidate_method": None,
        "methods": [
            {
                "method": method_name,
                "overall_readiness_score": 1.0,
                "readiness_status": "PROMOTE_FOR_REVIEW",
            }
            for method_name in readiness.SMOOTHED_METHOD_TO_VARIANT
        ],
    }
    confirmation = {
        "smoothed_confirmation_targets": {
            "candidate_method": None,
            "targets": [],
        }
    }

    result = readiness._readiness_decision(scorecard, confirmation)

    assert result["recommended_method"] is None
    assert result["secondary_method"] is None
    assert result["decision"] == "CONTINUE_OBSERVATION"
    assert result["evidence_status"] == "INSUFFICIENT_EVIDENCE"
    assert "readiness_score_cannot_create_candidate" in result["blocking_reasons"]


def test_readiness_follows_confirmation_candidate_instead_of_fixed_3d() -> None:
    candidate = "smooth_weights_5d_limited_adjustment"
    scorecard = {
        "candidate_method": candidate,
        "methods": [
            {
                "method": candidate,
                "overall_readiness_score": 0.8,
                "readiness_status": "PROMOTE_FOR_REVIEW",
                "missing_score_components": [],
                "hard_block_reasons": [],
            }
        ],
    }
    confirmation = {
        "smoothed_confirmation_targets": {
            "candidate_method": candidate,
            "targets": [
                {
                    "target_id": "5d-forward",
                    "method": candidate,
                    "status": "PASS",
                }
            ],
        }
    }

    result = readiness._readiness_decision(scorecard, confirmation)

    assert result["recommended_method"] == candidate
    assert result["secondary_method"] is None
    assert result["decision"] == "PROMOTE_FOR_REVIEW"


def test_owner_options_keep_promotion_unrecommended_without_candidate() -> None:
    scorecard = {
        "promotion_readiness_decision": {
            "recommended_method": None,
            "secondary_method": None,
            "decision": "CONTINUE_OBSERVATION",
            "evidence_status": "INSUFFICIENT_EVIDENCE",
            "blocking_reasons": ["no_eligible_candidate"],
        }
    }
    watch = {
        "smoothed_watch_summary": {
            "candidate_method": None,
            "current_decision": "CONTINUE_OBSERVATION",
            "forward_confirmation_status": "NOT_REGISTERED",
        }
    }

    result = readiness._owner_options("owner-1", scorecard, watch)
    promotion = next(
        row
        for row in result["owner_decision_options"]
        if row["decision"] == "promote_for_research_review"
    )

    assert result["recommended_owner_action"] == "request_additional_evidence"
    assert promotion["recommended"] is False


def test_readiness_policy_rejects_non_null_missing_component_score() -> None:
    config = yaml.safe_load(
        readiness.DEFAULT_SMOOTHED_LIMITED_CONFIG_PATH.read_text(encoding="utf-8")
    )
    changed = copy.deepcopy(config)
    changed["readiness_policy"]["missing_component_score"] = 0.25

    with pytest.raises(
        readiness.DynamicV3SmoothedReadinessError,
        match="missing_component_score must be null",
    ):
        readiness._readiness_policy(changed)
