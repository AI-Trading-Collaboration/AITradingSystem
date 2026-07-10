from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.data_quality import DataQualityEvidence
from ai_trading_system.contracts.research_context import PolicyRef, ResearchEvaluationContext
from ai_trading_system.contracts.status import CanonicalStatus, ContextResolutionStatus
from ai_trading_system.core.production_effect import ProductionEffect

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SUPPORTED_CONCLUSION_STATUSES = frozenset({CanonicalStatus.PASS, CanonicalStatus.LIMITED})


class ArtifactEnvelopeError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class ArtifactLifecycle(StrEnum):
    CURRENT = "CURRENT"
    SUPERSEDED = "SUPERSEDED"
    DEPRECATED = "DEPRECATED"
    ARCHIVED = "ARCHIVED"


class ArtifactVisibility(StrEnum):
    OWNER = "OWNER"
    OPERATOR = "OPERATOR"
    RESEARCH = "RESEARCH"
    AUDIT = "AUDIT"


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise ArtifactEnvelopeError("REQUIRED_ARTIFACT_FIELD_EMPTY", f"{field} is required")


def _date_value(value: object, field: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise ArtifactEnvelopeError("INVALID_ARTIFACT_DATE", f"{field}={value!r}") from exc
    raise ArtifactEnvelopeError("INVALID_ARTIFACT_DATE", f"{field}={value!r}")


def _datetime_value(value: object, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ArtifactEnvelopeError("INVALID_ARTIFACT_DATETIME", f"{field}={value!r}") from exc
    else:
        raise ArtifactEnvelopeError("INVALID_ARTIFACT_DATETIME", f"{field}={value!r}")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ArtifactEnvelopeError("ARTIFACT_DATETIME_TZ_REQUIRED", field)
    return parsed


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, (list, tuple)):
        raise ArtifactEnvelopeError("INVALID_ARTIFACT_PAYLOAD", f"{field} must be a list")
    return tuple(str(item) for item in value)


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ArtifactEnvelopeError("INVALID_ARTIFACT_PAYLOAD", f"{field} must be an integer")
    return value


@dataclass(frozen=True)
class ArtifactPointer:
    path: str
    artifact_type: str
    sha256: str
    size_bytes: int
    schema_version: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.path, "artifact.path"),
            (self.artifact_type, "artifact.artifact_type"),
            (self.schema_version, "artifact.schema_version"),
        ):
            _require_text(value, field)
        if not _SHA256_PATTERN.fullmatch(self.sha256):
            raise ArtifactEnvelopeError("INVALID_ARTIFACT_CHECKSUM", self.path)
        if isinstance(self.size_bytes, bool) or self.size_bytes < 0:
            raise ArtifactEnvelopeError("INVALID_ARTIFACT_SIZE", self.path)

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "artifact_type": self.artifact_type,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ArtifactPointer:
        return cls(
            path=str(payload.get("path", "")),
            artifact_type=str(payload.get("artifact_type", "")),
            sha256=str(payload.get("sha256", "")),
            size_bytes=_int_value(payload.get("size_bytes", -1), "size_bytes"),
            schema_version=str(payload.get("schema_version", "")),
        )


@dataclass(frozen=True)
class ArtifactEnvelope:
    schema_version: ClassVar[str] = "artifact_envelope.v1"

    artifact_id: str
    producer: str
    run_id: str
    generated_at: datetime
    as_of: date
    status: CanonicalStatus
    production_effect: ProductionEffect
    payload: ArtifactPointer
    owner: str
    lifecycle: ArtifactLifecycle = ArtifactLifecycle.CURRENT
    visibility: ArtifactVisibility = ArtifactVisibility.AUDIT
    retention_until: date | None = None
    investment_facing: bool = False
    data_quality_required: bool = False
    data_quality: DataQualityEvidence | None = None
    research_context: ResearchEvaluationContext | None = None
    input_artifacts: tuple[ArtifactPointer, ...] = ()
    policy_refs: tuple[PolicyRef, ...] = ()
    limitations: tuple[str, ...] = ()
    next_actions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.artifact_id, "artifact_id"),
            (self.producer, "producer"),
            (self.run_id, "run_id"),
            (self.owner, "owner"),
        ):
            _require_text(value, field)
        _datetime_value(self.generated_at, "generated_at")
        if self.generated_at.date() < self.as_of:
            raise ArtifactEnvelopeError(
                "ARTIFACT_GENERATED_BEFORE_AS_OF", "generated_at date must not precede as_of"
            )
        if self.retention_until is not None and self.retention_until < self.as_of:
            raise ArtifactEnvelopeError(
                "ARTIFACT_RETENTION_BEFORE_AS_OF", self.retention_until.isoformat()
            )
        inputs = tuple(sorted(self.input_artifacts, key=lambda item: item.path))
        if len({item.path for item in inputs}) != len(inputs):
            raise ArtifactEnvelopeError(
                "DUPLICATE_INPUT_ARTIFACT", "input artifact paths must be unique"
            )
        policies = tuple(
            sorted(self.policy_refs, key=lambda item: (item.role.value, item.policy_id))
        )
        if len({item.policy_id for item in policies}) != len(policies):
            raise ArtifactEnvelopeError("DUPLICATE_ENVELOPE_POLICY", self.artifact_id)
        object.__setattr__(self, "input_artifacts", inputs)
        object.__setattr__(self, "policy_refs", policies)
        object.__setattr__(self, "limitations", tuple(sorted(set(self.limitations))))
        object.__setattr__(self, "next_actions", tuple(sorted(set(self.next_actions))))

        requires_quality = self.data_quality_required or self.investment_facing
        if requires_quality and self.data_quality is None:
            raise ArtifactEnvelopeError("DATA_QUALITY_EVIDENCE_REQUIRED", self.artifact_id)
        if self.data_quality is not None and self.data_quality.as_of != self.as_of:
            raise ArtifactEnvelopeError("ENVELOPE_DQ_AS_OF_MISMATCH", self.artifact_id)
        if self.investment_facing and self.research_context is None:
            raise ArtifactEnvelopeError("RESEARCH_CONTEXT_REQUIRED", self.artifact_id)
        if self.research_context is not None and self.research_context.as_of != self.as_of:
            raise ArtifactEnvelopeError("ENVELOPE_CONTEXT_AS_OF_MISMATCH", self.artifact_id)

        if self.status in _SUPPORTED_CONCLUSION_STATUSES and requires_quality:
            assert self.data_quality is not None
            try:
                self.data_quality.assert_ready()
            except ValueError as exc:
                raise ArtifactEnvelopeError("ARTIFACT_DQ_NOT_READY", str(exc)) from exc
        if self.status in _SUPPORTED_CONCLUSION_STATUSES and self.investment_facing:
            assert self.research_context is not None
            if self.research_context.status is not ContextResolutionStatus.COMPLETE:
                raise ArtifactEnvelopeError(
                    "ARTIFACT_CONTEXT_NOT_COMPLETE", self.research_context.context_id
                )
            self.research_context.assert_complete()

    @property
    def envelope_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"artifact_envelope_{hashlib.sha256(material).hexdigest()[:20]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "artifact_id": self.artifact_id,
            "producer": self.producer,
            "run_id": self.run_id,
            "generated_at": self.generated_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "status": self.status.value,
            "production_effect": self.production_effect.value,
            "payload": self.payload.to_dict(),
            "owner": self.owner,
            "lifecycle": self.lifecycle.value,
            "visibility": self.visibility.value,
            "retention_until": (
                None if self.retention_until is None else self.retention_until.isoformat()
            ),
            "investment_facing": self.investment_facing,
            "data_quality_required": self.data_quality_required,
            "data_quality": None if self.data_quality is None else self.data_quality.to_dict(),
            "research_context": (
                None if self.research_context is None else self.research_context.to_dict()
            ),
            "input_artifacts": [item.to_dict() for item in self.input_artifacts],
            "policy_refs": [item.to_dict() for item in self.policy_refs],
            "limitations": list(self.limitations),
            "next_actions": list(self.next_actions),
        }

    def to_dict(self) -> dict[str, object]:
        return {"envelope_id": self.envelope_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ArtifactEnvelope:
        payload_ref = payload.get("payload")
        input_refs = payload.get("input_artifacts", [])
        policy_refs = payload.get("policy_refs", [])
        quality = payload.get("data_quality")
        context = payload.get("research_context")
        if not isinstance(payload_ref, Mapping):
            raise ArtifactEnvelopeError("INVALID_ARTIFACT_PAYLOAD", "payload must be mapping")
        if not isinstance(input_refs, list) or not isinstance(policy_refs, list):
            raise ArtifactEnvelopeError(
                "INVALID_ARTIFACT_PAYLOAD", "input_artifacts/policy_refs must be lists"
            )
        envelope = cls(
            artifact_id=str(payload.get("artifact_id", "")),
            producer=str(payload.get("producer", "")),
            run_id=str(payload.get("run_id", "")),
            generated_at=_datetime_value(payload.get("generated_at"), "generated_at"),
            as_of=_date_value(payload.get("as_of"), "as_of"),
            status=CanonicalStatus(str(payload.get("status", ""))),
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
            payload=ArtifactPointer.from_dict(payload_ref),
            owner=str(payload.get("owner", "")),
            lifecycle=ArtifactLifecycle(str(payload.get("lifecycle", ""))),
            visibility=ArtifactVisibility(str(payload.get("visibility", ""))),
            retention_until=(
                None
                if payload.get("retention_until") is None
                else _date_value(payload.get("retention_until"), "retention_until")
            ),
            investment_facing=payload.get("investment_facing") is True,
            data_quality_required=payload.get("data_quality_required") is True,
            data_quality=(
                None
                if quality is None
                else DataQualityEvidence.from_dict(_mapping(quality, "data_quality"))
            ),
            research_context=(
                None
                if context is None
                else ResearchEvaluationContext.from_dict(_mapping(context, "research_context"))
            ),
            input_artifacts=tuple(
                ArtifactPointer.from_dict(_mapping(item, "input_artifact")) for item in input_refs
            ),
            policy_refs=tuple(
                PolicyRef.from_dict(_mapping(item, "policy_ref")) for item in policy_refs
            ),
            limitations=_string_tuple(payload.get("limitations"), "limitations"),
            next_actions=_string_tuple(payload.get("next_actions"), "next_actions"),
        )
        supplied_id = payload.get("envelope_id")
        if supplied_id is not None and str(supplied_id) != envelope.envelope_id:
            raise ArtifactEnvelopeError(
                "ARTIFACT_ENVELOPE_ID_MISMATCH",
                f"supplied={supplied_id} actual={envelope.envelope_id}",
            )
        return envelope


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ArtifactEnvelopeError("INVALID_ARTIFACT_PAYLOAD", f"{field} must be mapping")
    return value
