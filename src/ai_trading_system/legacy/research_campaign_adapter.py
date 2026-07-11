from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ai_trading_system.contracts import ResearchPreregistration, ResultVisibility


@dataclass(frozen=True)
class LegacyCampaignLifecycleBinding:
    owner: str
    candidate_id: str
    research_context_id: str
    selection_rule_id: str
    selection_rule_sha256: str
    metric_ids: tuple[str, ...]
    policy_ref_ids: tuple[str, ...]
    validation_plan_ids: tuple[str, ...]
    frozen_at: datetime
    result_visibility: ResultVisibility


@dataclass(frozen=True)
class ResearchCampaignCompatibilityAssessment:
    campaign_id: str
    status: str
    blocker_codes: tuple[str, ...]
    preregistration: ResearchPreregistration | None
    inferred_fields: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "research_campaign_lifecycle_compatibility.v1",
            "campaign_id": self.campaign_id,
            "status": self.status,
            "blocker_codes": list(self.blocker_codes),
            "preregistration": (
                None if self.preregistration is None else self.preregistration.to_dict()
            ),
            "inferred_fields": list(self.inferred_fields),
            "production_effect": "none",
        }


def assess_legacy_campaign_lifecycle(
    campaign_spec: Any,
    *,
    binding: LegacyCampaignLifecycleBinding | None = None,
) -> ResearchCampaignCompatibilityAssessment:
    """Assess an existing CampaignSpec without guessing missing lifecycle semantics."""
    campaign_id = str(getattr(campaign_spec, "campaign_id", ""))
    if binding is None:
        return ResearchCampaignCompatibilityAssessment(
            campaign_id=campaign_id,
            status="BLOCKED_MISSING_CANONICAL_LIFECYCLE_BINDING",
            blocker_codes=(
                "PREREGISTRATION_FROZEN_AT_MISSING",
                "RESEARCH_CONTEXT_ID_MISSING",
                "RESULT_VISIBILITY_NOT_DECLARED",
                "SELECTION_RULE_CHECKSUM_MISSING",
                "VERSIONED_POLICY_REFS_MISSING",
            ),
            preregistration=None,
        )

    hypothesis = getattr(campaign_spec, "hypothesis", None)
    module_graph = getattr(campaign_spec, "module_graph", None)
    preregistration = ResearchPreregistration(
        hypothesis_id=f"campaign:{campaign_id}",
        hypothesis_statement=str(getattr(hypothesis, "statement", "")),
        owner=binding.owner,
        baseline_id=str(getattr(module_graph, "baseline", "")),
        candidate_id=binding.candidate_id,
        research_context_id=binding.research_context_id,
        selection_rule_id=binding.selection_rule_id,
        selection_rule_sha256=binding.selection_rule_sha256,
        metric_ids=binding.metric_ids,
        policy_ref_ids=binding.policy_ref_ids,
        validation_plan_ids=binding.validation_plan_ids,
        frozen_at=binding.frozen_at,
        result_visibility=binding.result_visibility,
    )
    return ResearchCampaignCompatibilityAssessment(
        campaign_id=campaign_id,
        status="COMPATIBLE_EXPLICIT_BINDING",
        blocker_codes=(),
        preregistration=preregistration,
        inferred_fields=(),
    )


__all__ = [
    "LegacyCampaignLifecycleBinding",
    "ResearchCampaignCompatibilityAssessment",
    "assess_legacy_campaign_lifecycle",
]
