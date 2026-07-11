from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_VALIDATION_STATUSES = frozenset(
    {CanonicalStatus.PASS, CanonicalStatus.BLOCKED, CanonicalStatus.FAILED}
)


class ResearchLifecycleError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class ResearchLifecycleStage(StrEnum):
    OBSERVATION = "OBSERVATION"
    EVIDENCE_SNAPSHOT = "EVIDENCE_SNAPSHOT"
    CHANGE_PROPOSAL_PENDING = "CHANGE_PROPOSAL_PENDING"
    CHANGE_PROPOSAL_FROZEN = "CHANGE_PROPOSAL_FROZEN"
    VALIDATION_PASSED = "VALIDATION_PASSED"
    VALIDATION_BLOCKED = "VALIDATION_BLOCKED"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    INVESTIGATING = "INVESTIGATING"
    CLOSED_KEEP = "CLOSED_KEEP"
    RETIRED = "RETIRED"
    ADOPTED = "ADOPTED"
    REJECTED = "REJECTED"


class ResearchReviewDecision(StrEnum):
    KEEP = "KEEP"
    INVESTIGATE = "INVESTIGATE"
    RETIRE = "RETIRE"
    OPEN_RESEARCH = "OPEN_RESEARCH"


class ResearchOwnerDecision(StrEnum):
    ADOPT = "ADOPT"
    REJECT = "REJECT"
    CONTINUE_RESEARCH = "CONTINUE_RESEARCH"


class ResultVisibility(StrEnum):
    NONE = "NONE"
    PARTIAL = "PARTIAL"
    FULL = "FULL"


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise ResearchLifecycleError("REQUIRED_LIFECYCLE_FIELD_EMPTY", field)


def _aware(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ResearchLifecycleError("LIFECYCLE_DATETIME_TZ_REQUIRED", field)


def _unique_text(values: tuple[str, ...], field: str, *, required: bool = False) -> tuple[str, ...]:
    normalized = tuple(sorted({value.strip() for value in values if value.strip()}))
    if required and not normalized:
        raise ResearchLifecycleError("REQUIRED_LIFECYCLE_COLLECTION_EMPTY", field)
    return normalized


def _semantic_id(prefix: str, payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(encoded).hexdigest()[:20]}"


@dataclass(frozen=True)
class ResearchPreregistration:
    schema_version: ClassVar[str] = "research_preregistration.v1"

    hypothesis_id: str
    hypothesis_statement: str
    owner: str
    baseline_id: str
    candidate_id: str
    research_context_id: str
    selection_rule_id: str
    selection_rule_sha256: str
    metric_ids: tuple[str, ...]
    policy_ref_ids: tuple[str, ...]
    validation_plan_ids: tuple[str, ...]
    frozen_at: datetime
    result_visibility: ResultVisibility = ResultVisibility.NONE
    manual_review_required: bool = True
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.hypothesis_id, "hypothesis_id"),
            (self.hypothesis_statement, "hypothesis_statement"),
            (self.owner, "owner"),
            (self.baseline_id, "baseline_id"),
            (self.candidate_id, "candidate_id"),
            (self.research_context_id, "research_context_id"),
            (self.selection_rule_id, "selection_rule_id"),
        ):
            _require_text(value, field)
        _aware(self.frozen_at, "frozen_at")
        if not _SHA256_PATTERN.fullmatch(self.selection_rule_sha256):
            raise ResearchLifecycleError(
                "INVALID_SELECTION_RULE_CHECKSUM", self.selection_rule_id
            )
        if self.result_visibility is not ResultVisibility.NONE:
            raise ResearchLifecycleError(
                "PREREGISTRATION_AFTER_RESULT_VISIBILITY", self.result_visibility.value
            )
        if not self.manual_review_required:
            raise ResearchLifecycleError(
                "MANUAL_REVIEW_REQUIRED", self.hypothesis_id
            )
        if self.production_effect is not ProductionEffect.NONE:
            raise ResearchLifecycleError(
                "PREREGISTRATION_PRODUCTION_EFFECT_FORBIDDEN",
                self.production_effect.value,
            )
        object.__setattr__(
            self,
            "metric_ids",
            _unique_text(self.metric_ids, "metric_ids", required=True),
        )
        object.__setattr__(
            self,
            "policy_ref_ids",
            _unique_text(self.policy_ref_ids, "policy_ref_ids", required=True),
        )
        object.__setattr__(
            self,
            "validation_plan_ids",
            _unique_text(self.validation_plan_ids, "validation_plan_ids", required=True),
        )

    @property
    def preregistration_id(self) -> str:
        return _semantic_id("research_preregistration", self._semantic_payload())

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "hypothesis_id": self.hypothesis_id,
            "hypothesis_statement": self.hypothesis_statement,
            "owner": self.owner,
            "baseline_id": self.baseline_id,
            "candidate_id": self.candidate_id,
            "research_context_id": self.research_context_id,
            "selection_rule_id": self.selection_rule_id,
            "selection_rule_sha256": self.selection_rule_sha256,
            "metric_ids": list(self.metric_ids),
            "policy_ref_ids": list(self.policy_ref_ids),
            "validation_plan_ids": list(self.validation_plan_ids),
            "frozen_at": self.frozen_at.isoformat(),
            "result_visibility": self.result_visibility.value,
            "manual_review_required": self.manual_review_required,
            "production_effect": self.production_effect.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"preregistration_id": self.preregistration_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchPreregistration:
        preregistration = cls(
            hypothesis_id=str(payload.get("hypothesis_id", "")),
            hypothesis_statement=str(payload.get("hypothesis_statement", "")),
            owner=str(payload.get("owner", "")),
            baseline_id=str(payload.get("baseline_id", "")),
            candidate_id=str(payload.get("candidate_id", "")),
            research_context_id=str(payload.get("research_context_id", "")),
            selection_rule_id=str(payload.get("selection_rule_id", "")),
            selection_rule_sha256=str(payload.get("selection_rule_sha256", "")),
            metric_ids=_string_tuple(payload.get("metric_ids"), "metric_ids"),
            policy_ref_ids=_string_tuple(payload.get("policy_ref_ids"), "policy_ref_ids"),
            validation_plan_ids=_string_tuple(
                payload.get("validation_plan_ids"), "validation_plan_ids"
            ),
            frozen_at=_datetime_value(payload.get("frozen_at"), "frozen_at"),
            result_visibility=ResultVisibility(str(payload.get("result_visibility", ""))),
            manual_review_required=payload.get("manual_review_required") is True,
            production_effect=ProductionEffect.parse(
                str(payload.get("production_effect", ProductionEffect.NONE.value))
            ),
        )
        supplied_id = payload.get("preregistration_id")
        if supplied_id is not None and str(supplied_id) != preregistration.preregistration_id:
            raise ResearchLifecycleError(
                "PREREGISTRATION_ID_MISMATCH",
                f"supplied={supplied_id} actual={preregistration.preregistration_id}",
            )
        return preregistration


@dataclass(frozen=True)
class ResearchLifecycleEvent:
    sequence: int
    from_stage: ResearchLifecycleStage | None
    to_stage: ResearchLifecycleStage
    at: datetime
    actor: str
    reason_codes: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if isinstance(self.sequence, bool) or self.sequence < 1:
            raise ResearchLifecycleError("INVALID_LIFECYCLE_SEQUENCE", str(self.sequence))
        _aware(self.at, "event.at")
        _require_text(self.actor, "event.actor")
        object.__setattr__(
            self,
            "reason_codes",
            _unique_text(self.reason_codes, "event.reason_codes"),
        )
        object.__setattr__(
            self,
            "artifact_refs",
            _unique_text(self.artifact_refs, "event.artifact_refs"),
        )

    @property
    def event_id(self) -> str:
        return _semantic_id("research_lifecycle_event", self.to_dict(include_id=False))

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "sequence": self.sequence,
            "from_stage": None if self.from_stage is None else self.from_stage.value,
            "to_stage": self.to_stage.value,
            "at": self.at.isoformat(),
            "actor": self.actor,
            "reason_codes": list(self.reason_codes),
            "artifact_refs": list(self.artifact_refs),
        }
        return {"event_id": self.event_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchLifecycleEvent:
        from_stage_raw = payload.get("from_stage")
        event = cls(
            sequence=_int_value(payload.get("sequence"), "event.sequence"),
            from_stage=(
                None
                if from_stage_raw is None
                else ResearchLifecycleStage(str(from_stage_raw))
            ),
            to_stage=ResearchLifecycleStage(str(payload.get("to_stage", ""))),
            at=_datetime_value(payload.get("at"), "event.at"),
            actor=str(payload.get("actor", "")),
            reason_codes=_string_tuple(payload.get("reason_codes", []), "reason_codes"),
            artifact_refs=_string_tuple(payload.get("artifact_refs", []), "artifact_refs"),
        )
        supplied_id = payload.get("event_id")
        if supplied_id is not None and str(supplied_id) != event.event_id:
            raise ResearchLifecycleError(
                "LIFECYCLE_EVENT_ID_MISMATCH",
                f"supplied={supplied_id} actual={event.event_id}",
            )
        return event


@dataclass(frozen=True)
class ResearchLifecycleRecord:
    schema_version: ClassVar[str] = "research_lifecycle.v1"

    lifecycle_id: str
    owner: str
    stage: ResearchLifecycleStage
    created_at: datetime
    updated_at: datetime
    observation_ref: str
    evidence_refs: tuple[str, ...] = ()
    preregistration: ResearchPreregistration | None = None
    review_decision: ResearchReviewDecision | None = None
    validation_status: CanonicalStatus | None = None
    owner_decision: ResearchOwnerDecision | None = None
    events: tuple[ResearchLifecycleEvent, ...] = ()
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.lifecycle_id, "lifecycle_id"),
            (self.owner, "owner"),
            (self.observation_ref, "observation_ref"),
        ):
            _require_text(value, field)
        _aware(self.created_at, "created_at")
        _aware(self.updated_at, "updated_at")
        if self.updated_at < self.created_at:
            raise ResearchLifecycleError("LIFECYCLE_TIME_REVERSED", self.lifecycle_id)
        if self.production_effect is not ProductionEffect.NONE:
            raise ResearchLifecycleError(
                "LIFECYCLE_PRODUCTION_EFFECT_FORBIDDEN", self.production_effect.value
            )
        evidence_refs = _unique_text(self.evidence_refs, "evidence_refs")
        object.__setattr__(self, "evidence_refs", evidence_refs)
        if tuple(event.sequence for event in self.events) != tuple(
            range(1, len(self.events) + 1)
        ):
            raise ResearchLifecycleError("LIFECYCLE_EVENT_SEQUENCE_GAP", self.lifecycle_id)
        if self.events and self.events[-1].to_stage is not self.stage:
            raise ResearchLifecycleError("LIFECYCLE_STAGE_EVENT_MISMATCH", self.lifecycle_id)

    @classmethod
    def initialize(
        cls,
        *,
        lifecycle_id: str,
        owner: str,
        observation_ref: str,
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...] = (),
    ) -> ResearchLifecycleRecord:
        event = ResearchLifecycleEvent(
            sequence=1,
            from_stage=None,
            to_stage=ResearchLifecycleStage.OBSERVATION,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            artifact_refs=(observation_ref,),
        )
        return cls(
            lifecycle_id=lifecycle_id,
            owner=owner,
            stage=ResearchLifecycleStage.OBSERVATION,
            created_at=at,
            updated_at=at,
            observation_ref=observation_ref,
            events=(event,),
        )

    @property
    def record_id(self) -> str:
        return _semantic_id("research_lifecycle", self._semantic_payload())

    def attach_evidence(
        self,
        *,
        evidence_refs: tuple[str, ...],
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...] = (),
    ) -> ResearchLifecycleRecord:
        if self.stage not in {
            ResearchLifecycleStage.OBSERVATION,
            ResearchLifecycleStage.INVESTIGATING,
        }:
            raise ResearchLifecycleError("EVIDENCE_STAGE_INVALID", self.stage.value)
        refs = _unique_text(evidence_refs, "evidence_refs", required=True)
        return self._transition(
            ResearchLifecycleStage.EVIDENCE_SNAPSHOT,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            artifact_refs=refs,
            evidence_refs=tuple(sorted(set(self.evidence_refs) | set(refs))),
        )

    def record_review(
        self,
        *,
        decision: ResearchReviewDecision,
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...],
    ) -> ResearchLifecycleRecord:
        if self.stage is not ResearchLifecycleStage.EVIDENCE_SNAPSHOT:
            raise ResearchLifecycleError("REVIEW_STAGE_INVALID", self.stage.value)
        if not reason_codes:
            raise ResearchLifecycleError("REVIEW_REASON_REQUIRED", decision.value)
        target = {
            ResearchReviewDecision.KEEP: ResearchLifecycleStage.CLOSED_KEEP,
            ResearchReviewDecision.INVESTIGATE: ResearchLifecycleStage.INVESTIGATING,
            ResearchReviewDecision.RETIRE: ResearchLifecycleStage.RETIRED,
            ResearchReviewDecision.OPEN_RESEARCH: (
                ResearchLifecycleStage.CHANGE_PROPOSAL_PENDING
            ),
        }[decision]
        return self._transition(
            target,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            review_decision=decision,
        )

    def freeze_change_proposal(
        self,
        *,
        preregistration: ResearchPreregistration,
        at: datetime,
        actor: str,
    ) -> ResearchLifecycleRecord:
        if self.stage is not ResearchLifecycleStage.CHANGE_PROPOSAL_PENDING:
            raise ResearchLifecycleError("CHANGE_PROPOSAL_STAGE_INVALID", self.stage.value)
        if preregistration.owner != self.owner:
            raise ResearchLifecycleError(
                "PREREGISTRATION_OWNER_MISMATCH",
                f"record={self.owner} preregistration={preregistration.owner}",
            )
        if preregistration.frozen_at != at:
            raise ResearchLifecycleError(
                "PREREGISTRATION_FREEZE_TIME_MISMATCH", self.lifecycle_id
            )
        return self._transition(
            ResearchLifecycleStage.CHANGE_PROPOSAL_FROZEN,
            at=at,
            actor=actor,
            reason_codes=("PREREGISTRATION_FROZEN_BEFORE_RESULTS",),
            artifact_refs=(preregistration.preregistration_id,),
            preregistration=preregistration,
        )

    def record_validation(
        self,
        *,
        status: CanonicalStatus,
        evidence_refs: tuple[str, ...],
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...],
    ) -> ResearchLifecycleRecord:
        if self.stage is not ResearchLifecycleStage.CHANGE_PROPOSAL_FROZEN:
            raise ResearchLifecycleError("VALIDATION_STAGE_INVALID", self.stage.value)
        if status not in _VALIDATION_STATUSES:
            raise ResearchLifecycleError("VALIDATION_STATUS_INVALID", status.value)
        refs = _unique_text(evidence_refs, "validation.evidence_refs", required=True)
        if not reason_codes:
            raise ResearchLifecycleError("VALIDATION_REASON_REQUIRED", status.value)
        target = {
            CanonicalStatus.PASS: ResearchLifecycleStage.VALIDATION_PASSED,
            CanonicalStatus.BLOCKED: ResearchLifecycleStage.VALIDATION_BLOCKED,
            CanonicalStatus.FAILED: ResearchLifecycleStage.VALIDATION_FAILED,
        }[status]
        return self._transition(
            target,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            artifact_refs=refs,
            evidence_refs=tuple(sorted(set(self.evidence_refs) | set(refs))),
            validation_status=status,
        )

    def record_owner_decision(
        self,
        *,
        decision: ResearchOwnerDecision,
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...],
    ) -> ResearchLifecycleRecord:
        if self.stage is not ResearchLifecycleStage.VALIDATION_PASSED:
            raise ResearchLifecycleError("OWNER_DECISION_STAGE_INVALID", self.stage.value)
        if not reason_codes:
            raise ResearchLifecycleError("OWNER_DECISION_REASON_REQUIRED", decision.value)
        target = {
            ResearchOwnerDecision.ADOPT: ResearchLifecycleStage.ADOPTED,
            ResearchOwnerDecision.REJECT: ResearchLifecycleStage.REJECTED,
            ResearchOwnerDecision.CONTINUE_RESEARCH: ResearchLifecycleStage.INVESTIGATING,
        }[decision]
        return self._transition(
            target,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            owner_decision=decision,
        )

    def _transition(
        self,
        target: ResearchLifecycleStage,
        *,
        at: datetime,
        actor: str,
        reason_codes: tuple[str, ...] = (),
        artifact_refs: tuple[str, ...] = (),
        evidence_refs: tuple[str, ...] | None = None,
        preregistration: ResearchPreregistration | None = None,
        review_decision: ResearchReviewDecision | None = None,
        validation_status: CanonicalStatus | None = None,
        owner_decision: ResearchOwnerDecision | None = None,
    ) -> ResearchLifecycleRecord:
        _aware(at, "transition.at")
        if at < self.updated_at:
            raise ResearchLifecycleError("LIFECYCLE_TRANSITION_TIME_REVERSED", target.value)
        event = ResearchLifecycleEvent(
            sequence=len(self.events) + 1,
            from_stage=self.stage,
            to_stage=target,
            at=at,
            actor=actor,
            reason_codes=reason_codes,
            artifact_refs=artifact_refs,
        )
        return replace(
            self,
            stage=target,
            updated_at=at,
            events=(*self.events, event),
            evidence_refs=self.evidence_refs if evidence_refs is None else evidence_refs,
            preregistration=(
                self.preregistration if preregistration is None else preregistration
            ),
            review_decision=(
                self.review_decision if review_decision is None else review_decision
            ),
            validation_status=(
                self.validation_status if validation_status is None else validation_status
            ),
            owner_decision=(
                self.owner_decision if owner_decision is None else owner_decision
            ),
        )

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "lifecycle_id": self.lifecycle_id,
            "owner": self.owner,
            "stage": self.stage.value,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "observation_ref": self.observation_ref,
            "evidence_refs": list(self.evidence_refs),
            "preregistration": (
                None if self.preregistration is None else self.preregistration.to_dict()
            ),
            "review_decision": (
                None if self.review_decision is None else self.review_decision.value
            ),
            "validation_status": (
                None if self.validation_status is None else self.validation_status.value
            ),
            "owner_decision": (
                None if self.owner_decision is None else self.owner_decision.value
            ),
            "events": [event.to_dict() for event in self.events],
            "production_effect": self.production_effect.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"record_id": self.record_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchLifecycleRecord:
        events_raw = payload.get("events", [])
        preregistration_raw = payload.get("preregistration")
        if not isinstance(events_raw, list):
            raise ResearchLifecycleError("INVALID_LIFECYCLE_PAYLOAD", "events must be list")
        if preregistration_raw is not None and not isinstance(preregistration_raw, Mapping):
            raise ResearchLifecycleError(
                "INVALID_LIFECYCLE_PAYLOAD", "preregistration must be mapping or null"
            )
        review_raw = payload.get("review_decision")
        validation_raw = payload.get("validation_status")
        owner_raw = payload.get("owner_decision")
        record = cls(
            lifecycle_id=str(payload.get("lifecycle_id", "")),
            owner=str(payload.get("owner", "")),
            stage=ResearchLifecycleStage(str(payload.get("stage", ""))),
            created_at=_datetime_value(payload.get("created_at"), "created_at"),
            updated_at=_datetime_value(payload.get("updated_at"), "updated_at"),
            observation_ref=str(payload.get("observation_ref", "")),
            evidence_refs=_string_tuple(payload.get("evidence_refs", []), "evidence_refs"),
            preregistration=(
                None
                if preregistration_raw is None
                else ResearchPreregistration.from_dict(preregistration_raw)
            ),
            review_decision=(
                None if review_raw is None else ResearchReviewDecision(str(review_raw))
            ),
            validation_status=(
                None if validation_raw is None else CanonicalStatus(str(validation_raw))
            ),
            owner_decision=(
                None if owner_raw is None else ResearchOwnerDecision(str(owner_raw))
            ),
            events=tuple(
                ResearchLifecycleEvent.from_dict(item)
                for item in events_raw
                if isinstance(item, Mapping)
            ),
            production_effect=ProductionEffect.parse(
                str(payload.get("production_effect", ProductionEffect.NONE.value))
            ),
        )
        if len(record.events) != len(events_raw):
            raise ResearchLifecycleError(
                "INVALID_LIFECYCLE_PAYLOAD", "every event must be mapping"
            )
        supplied_id = payload.get("record_id")
        if supplied_id is not None and str(supplied_id) != record.record_id:
            raise ResearchLifecycleError(
                "LIFECYCLE_RECORD_ID_MISMATCH",
                f"supplied={supplied_id} actual={record.record_id}",
            )
        return record


def apply_periodic_research_review(
    *,
    lifecycle_id: str,
    owner: str,
    observation_ref: str,
    evidence_refs: tuple[str, ...],
    decision: ResearchReviewDecision,
    at: datetime,
    actor: str,
    reason_codes: tuple[str, ...],
) -> ResearchLifecycleRecord:
    """Create only observation/evidence/review state; never preregister or adopt."""
    record = ResearchLifecycleRecord.initialize(
        lifecycle_id=lifecycle_id,
        owner=owner,
        observation_ref=observation_ref,
        at=at,
        actor=actor,
        reason_codes=("PERIODIC_REVIEW_OBSERVATION",),
    )
    record = record.attach_evidence(
        evidence_refs=evidence_refs,
        at=at,
        actor=actor,
        reason_codes=("PERIODIC_REVIEW_EVIDENCE_SNAPSHOT",),
    )
    return record.record_review(
        decision=decision,
        at=at,
        actor=actor,
        reason_codes=reason_codes,
    )


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ResearchLifecycleError(
                "INVALID_LIFECYCLE_DATETIME", f"{field}={value!r}"
            ) from exc
    else:
        raise ResearchLifecycleError("INVALID_LIFECYCLE_DATETIME", f"{field}={value!r}")
    _aware(parsed, field)
    return parsed


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise ResearchLifecycleError("INVALID_LIFECYCLE_PAYLOAD", f"{field} must be list")
    return tuple(str(item) for item in value)


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ResearchLifecycleError("INVALID_LIFECYCLE_PAYLOAD", f"{field} must be integer")
    return value


__all__ = [
    "ResearchLifecycleError",
    "ResearchLifecycleEvent",
    "ResearchLifecycleRecord",
    "ResearchLifecycleStage",
    "ResearchOwnerDecision",
    "ResearchPreregistration",
    "ResearchReviewDecision",
    "ResultVisibility",
    "apply_periodic_research_review",
]
