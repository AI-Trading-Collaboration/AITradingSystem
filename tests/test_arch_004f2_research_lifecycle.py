from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ai_trading_system.contracts import (
    CanonicalStatus,
    ResearchLifecycleError,
    ResearchLifecycleRecord,
    ResearchLifecycleStage,
    ResearchOwnerDecision,
    ResearchPreregistration,
    ResearchReviewDecision,
    ResultVisibility,
    apply_periodic_research_review,
)
from ai_trading_system.legacy import (
    LegacyCampaignLifecycleBinding,
    assess_legacy_campaign_lifecycle,
)
from ai_trading_system.research_campaign import load_campaign_spec

AT = datetime(2026, 7, 11, 1, 0, tzinfo=UTC)
CAMPAIGN_SPEC = Path("docs/examples/research_campaigns/b2_risk_overlay_current_form.yaml")


def test_canonical_research_lifecycle_requires_preregistration_validation_and_owner() -> None:
    record = ResearchLifecycleRecord.initialize(
        lifecycle_id="research:example",
        owner="research_owner",
        observation_ref="artifact:observation",
        at=AT,
        actor="review_scheduler",
    )
    record = record.attach_evidence(
        evidence_refs=("artifact:evidence",),
        at=AT + timedelta(minutes=1),
        actor="research_owner",
    )
    record = record.record_review(
        decision=ResearchReviewDecision.OPEN_RESEARCH,
        at=AT + timedelta(minutes=2),
        actor="research_owner",
        reason_codes=("INDEPENDENT_EVIDENCE_CHANGED",),
    )
    preregistration = _preregistration(AT + timedelta(minutes=3))
    record = record.freeze_change_proposal(
        preregistration=preregistration,
        at=preregistration.frozen_at,
        actor="research_owner",
    )
    record = record.record_validation(
        status=CanonicalStatus.PASS,
        evidence_refs=("artifact:holdout", "artifact:cost_stress"),
        at=AT + timedelta(minutes=4),
        actor="validation_runner",
        reason_codes=("ALL_PREREGISTERED_GATES_PASS",),
    )
    record = record.record_owner_decision(
        decision=ResearchOwnerDecision.ADOPT,
        at=AT + timedelta(minutes=5),
        actor="project_owner",
        reason_codes=("OWNER_APPROVED_AFTER_VALIDATION",),
    )

    assert record.stage is ResearchLifecycleStage.ADOPTED
    assert record.preregistration == preregistration
    assert record.validation_status is CanonicalStatus.PASS
    assert record.owner_decision is ResearchOwnerDecision.ADOPT
    assert [event.sequence for event in record.events] == list(range(1, 7))
    assert record.to_dict()["production_effect"] == "none"
    assert ResearchPreregistration.from_dict(preregistration.to_dict()) == preregistration
    assert ResearchLifecycleRecord.from_dict(record.to_dict()) == record
    assert record.record_id == record.record_id


@pytest.mark.parametrize(
    ("decision", "expected_stage"),
    [
        (ResearchReviewDecision.KEEP, ResearchLifecycleStage.CLOSED_KEEP),
        (ResearchReviewDecision.INVESTIGATE, ResearchLifecycleStage.INVESTIGATING),
        (ResearchReviewDecision.RETIRE, ResearchLifecycleStage.RETIRED),
        (
            ResearchReviewDecision.OPEN_RESEARCH,
            ResearchLifecycleStage.CHANGE_PROPOSAL_PENDING,
        ),
    ],
)
def test_periodic_review_stops_before_preregistration_or_adoption(
    decision: ResearchReviewDecision,
    expected_stage: ResearchLifecycleStage,
) -> None:
    record = apply_periodic_research_review(
        lifecycle_id=f"periodic:{decision.value.lower()}",
        owner="research_owner",
        observation_ref="artifact:periodic_observation",
        evidence_refs=("artifact:weekly_evidence",),
        decision=decision,
        at=AT,
        actor="periodic_review_runner",
        reason_codes=("PERIODIC_REVIEW_DECISION",),
    )

    assert record.stage is expected_stage
    assert record.preregistration is None
    assert record.validation_status is None
    assert record.owner_decision is None


def test_lifecycle_fails_closed_on_visible_results_and_premature_owner_decision() -> None:
    with pytest.raises(ResearchLifecycleError, match="PREREGISTRATION_AFTER_RESULT_VISIBILITY"):
        _preregistration(AT, visibility=ResultVisibility.FULL)

    record = ResearchLifecycleRecord.initialize(
        lifecycle_id="research:premature",
        owner="research_owner",
        observation_ref="artifact:observation",
        at=AT,
        actor="research_owner",
    )
    with pytest.raises(ResearchLifecycleError, match="OWNER_DECISION_STAGE_INVALID"):
        record.record_owner_decision(
            decision=ResearchOwnerDecision.ADOPT,
            at=AT,
            actor="project_owner",
            reason_codes=("PREMATURE",),
        )


def test_legacy_campaign_requires_explicit_lifecycle_binding_without_inference() -> None:
    campaign = load_campaign_spec(CAMPAIGN_SPEC)

    blocked = assess_legacy_campaign_lifecycle(campaign)

    assert blocked.status == "BLOCKED_MISSING_CANONICAL_LIFECYCLE_BINDING"
    assert blocked.preregistration is None
    assert blocked.inferred_fields == ()
    assert set(blocked.blocker_codes) == {
        "PREREGISTRATION_FROZEN_AT_MISSING",
        "RESEARCH_CONTEXT_ID_MISSING",
        "RESULT_VISIBILITY_NOT_DECLARED",
        "SELECTION_RULE_CHECKSUM_MISSING",
        "VERSIONED_POLICY_REFS_MISSING",
    }

    compatible = assess_legacy_campaign_lifecycle(
        campaign,
        binding=LegacyCampaignLifecycleBinding(
            owner="research_owner",
            candidate_id="risk-overlay-b2",
            research_context_id="research_context_example",
            selection_rule_id="b2-selection-v1",
            selection_rule_sha256="a" * 64,
            metric_ids=("drawdown_reduction", "turnover_cost"),
            policy_ref_ids=("research_gate_policies_v1", "research_window_policy_v1"),
            validation_plan_ids=("holdout", "cost_stress", "robustness"),
            frozen_at=AT,
            result_visibility=ResultVisibility.NONE,
        ),
    )

    assert compatible.status == "COMPATIBLE_EXPLICIT_BINDING"
    assert compatible.blocker_codes == ()
    assert compatible.inferred_fields == ()
    assert compatible.preregistration is not None
    assert compatible.preregistration.baseline_id == "b0-static"


def _preregistration(
    frozen_at: datetime,
    *,
    visibility: ResultVisibility = ResultVisibility.NONE,
) -> ResearchPreregistration:
    return ResearchPreregistration(
        hypothesis_id="hypothesis:example",
        hypothesis_statement="Candidate improves risk-adjusted outcome under frozen rules.",
        owner="research_owner",
        baseline_id="baseline:v1",
        candidate_id="candidate:v1",
        research_context_id="research_context_example",
        selection_rule_id="selection:v1",
        selection_rule_sha256="a" * 64,
        metric_ids=("total_return", "max_drawdown", "turnover"),
        policy_ref_ids=("window:v1", "cost:v1", "threshold:v1"),
        validation_plan_ids=("pit_replay", "holdout", "robustness"),
        frozen_at=frozen_at,
        result_visibility=visibility,
    )
