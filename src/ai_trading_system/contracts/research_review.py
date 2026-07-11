from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import ClassVar

from ai_trading_system.contracts.research_lifecycle import (
    ResearchLifecycleStage,
    ResearchOwnerDecision,
    ResearchReviewDecision,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect


class ResearchReviewContractError(ValueError):
    pass


@dataclass(frozen=True)
class ResearchReviewItem:
    lifecycle_id: str
    owner: str
    stage: ResearchLifecycleStage
    updated_at: datetime
    observation_ref: str
    evidence_refs: tuple[str, ...]
    status: CanonicalStatus
    review_decision: ResearchReviewDecision | None = None
    preregistration_ref: str | None = None
    validation_status: CanonicalStatus | None = None
    owner_decision: ResearchOwnerDecision | None = None
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.lifecycle_id.strip() or not self.owner.strip():
            raise ResearchReviewContractError("RESEARCH_REVIEW_REQUIRED_ID_OR_OWNER")
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ResearchReviewContractError("RESEARCH_REVIEW_TIMEZONE_REQUIRED")
        if self.stage is ResearchLifecycleStage.ADOPTED and not self.adoption_recorded:
            raise ResearchReviewContractError("RESEARCH_REVIEW_ADOPTION_EVIDENCE_INCOMPLETE")
        if self.owner_decision is ResearchOwnerDecision.ADOPT and not self.adoption_recorded:
            raise ResearchReviewContractError("RESEARCH_REVIEW_PREMATURE_ADOPTION")

    @property
    def adoption_recorded(self) -> bool:
        return (
            self.stage is ResearchLifecycleStage.ADOPTED
            and self.preregistration_ref is not None
            and self.validation_status is CanonicalStatus.PASS
            and self.owner_decision is ResearchOwnerDecision.ADOPT
        )

    @property
    def proposal_is_adoption(self) -> bool:
        return False

    def to_dict(self) -> dict[str, object]:
        return {
            "lifecycle_id": self.lifecycle_id,
            "owner": self.owner,
            "stage": self.stage.value,
            "updated_at": self.updated_at.isoformat(),
            "observation_ref": self.observation_ref,
            "evidence_refs": list(self.evidence_refs),
            "status": self.status.value,
            "review_decision": (
                None if self.review_decision is None else self.review_decision.value
            ),
            "preregistration_ref": self.preregistration_ref,
            "validation_status": (
                None if self.validation_status is None else self.validation_status.value
            ),
            "owner_decision": (
                None if self.owner_decision is None else self.owner_decision.value
            ),
            "adoption_recorded": self.adoption_recorded,
            "proposal_is_adoption": self.proposal_is_adoption,
            "blocker_codes": list(self.blocker_codes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchReviewItem:
        review = payload.get("review_decision")
        validation = payload.get("validation_status")
        owner_decision = payload.get("owner_decision")
        return cls(
            lifecycle_id=str(payload.get("lifecycle_id", "")),
            owner=str(payload.get("owner", "")),
            stage=ResearchLifecycleStage(str(payload.get("stage", ""))),
            updated_at=datetime.fromisoformat(
                str(payload.get("updated_at", "")).replace("Z", "+00:00")
            ),
            observation_ref=str(payload.get("observation_ref", "")),
            evidence_refs=_string_tuple(payload.get("evidence_refs"), "evidence_refs"),
            status=CanonicalStatus(str(payload.get("status", ""))),
            review_decision=(
                None if review is None else ResearchReviewDecision(str(review))
            ),
            preregistration_ref=(
                None
                if payload.get("preregistration_ref") is None
                else str(payload["preregistration_ref"])
            ),
            validation_status=(
                None if validation is None else CanonicalStatus(str(validation))
            ),
            owner_decision=(
                None
                if owner_decision is None
                else ResearchOwnerDecision(str(owner_decision))
            ),
            blocker_codes=_string_tuple(payload.get("blocker_codes"), "blocker_codes"),
        )


@dataclass(frozen=True)
class ResearchReviewPackViewModel:
    schema_version: ClassVar[str] = "research_review_pack_view_model.v1"

    policy_id: str
    as_of: date
    generated_at: datetime
    status: CanonicalStatus
    items: tuple[ResearchReviewItem, ...]
    auto_tune_allowed: bool = False
    proposal_may_equal_adoption: bool = False
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        if not self.policy_id.strip():
            raise ResearchReviewContractError("RESEARCH_REVIEW_POLICY_REQUIRED")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ResearchReviewContractError("RESEARCH_REVIEW_TIMEZONE_REQUIRED")
        if not self.items:
            raise ResearchReviewContractError("RESEARCH_REVIEW_ITEMS_REQUIRED")
        if len({item.lifecycle_id for item in self.items}) != len(self.items):
            raise ResearchReviewContractError("RESEARCH_REVIEW_DUPLICATE_LIFECYCLE")
        if self.auto_tune_allowed or self.proposal_may_equal_adoption:
            raise ResearchReviewContractError("RESEARCH_REVIEW_AUTOMATION_FORBIDDEN")
        if self.production_effect is not ProductionEffect.NONE:
            raise ResearchReviewContractError("RESEARCH_REVIEW_PRODUCTION_EFFECT_FORBIDDEN")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "items": [item.to_dict() for item in self.items],
            "auto_tune_allowed": self.auto_tune_allowed,
            "proposal_may_equal_adoption": self.proposal_may_equal_adoption,
            "production_effect": self.production_effect.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchReviewPackViewModel:
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raise ResearchReviewContractError("RESEARCH_REVIEW_ITEMS_MUST_BE_LIST")
        return cls(
            policy_id=str(payload.get("policy_id", "")),
            as_of=date.fromisoformat(str(payload.get("as_of", ""))),
            generated_at=datetime.fromisoformat(
                str(payload.get("generated_at", "")).replace("Z", "+00:00")
            ),
            status=CanonicalStatus(str(payload.get("status", ""))),
            items=tuple(
                ResearchReviewItem.from_dict(_mapping(item)) for item in raw_items
            ),
            auto_tune_allowed=payload.get("auto_tune_allowed") is True,
            proposal_may_equal_adoption=(
                payload.get("proposal_may_equal_adoption") is True
            ),
            production_effect=ProductionEffect.parse(
                str(payload.get("production_effect", ""))
            ),
        )


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ResearchReviewContractError(f"{field} must be list")
    return tuple(str(item) for item in value)


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ResearchReviewContractError("research review item must be mapping")
    return value


__all__ = [
    "ResearchReviewContractError",
    "ResearchReviewItem",
    "ResearchReviewPackViewModel",
]
