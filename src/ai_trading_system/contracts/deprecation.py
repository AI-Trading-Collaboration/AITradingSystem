from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect


class DeprecationContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class SurfaceLifecycle(StrEnum):
    EXPERIMENTAL = "EXPERIMENTAL"
    ACTIVE = "ACTIVE"
    DEPRECATED = "DEPRECATED"
    FROZEN = "FROZEN"
    REMOVED = "REMOVED"


_NEXT_STATE = {
    SurfaceLifecycle.EXPERIMENTAL: SurfaceLifecycle.ACTIVE,
    SurfaceLifecycle.ACTIVE: SurfaceLifecycle.DEPRECATED,
    SurfaceLifecycle.DEPRECATED: SurfaceLifecycle.FROZEN,
    SurfaceLifecycle.FROZEN: SurfaceLifecycle.REMOVED,
}


@dataclass(frozen=True)
class RemovalGateEvidence:
    gate_id: str
    status: CanonicalStatus
    evidence_refs: tuple[str, ...]
    checked_at: datetime

    def __post_init__(self) -> None:
        if not self.gate_id.strip():
            raise DeprecationContractError("REMOVAL_GATE_ID_REQUIRED", "gate_id")
        if self.status not in {CanonicalStatus.PASS, CanonicalStatus.BLOCKED}:
            raise DeprecationContractError(
                "REMOVAL_GATE_STATUS_INVALID", f"{self.gate_id}={self.status.value}"
            )
        if not self.evidence_refs or any(not item.strip() for item in self.evidence_refs):
            raise DeprecationContractError(
                "REMOVAL_GATE_EVIDENCE_REQUIRED", self.gate_id
            )
        if self.checked_at.tzinfo is None or self.checked_at.utcoffset() is None:
            raise DeprecationContractError(
                "REMOVAL_GATE_TIMEZONE_REQUIRED", self.gate_id
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "gate_id": self.gate_id,
            "status": self.status.value,
            "evidence_refs": list(self.evidence_refs),
            "checked_at": self.checked_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RemovalGateEvidence:
        return cls(
            gate_id=str(payload.get("gate_id", "")),
            status=CanonicalStatus(str(payload.get("status", ""))),
            evidence_refs=_string_tuple(payload.get("evidence_refs"), "evidence_refs"),
            checked_at=_datetime(payload.get("checked_at"), "checked_at"),
        )


@dataclass(frozen=True)
class DeprecationRecord:
    schema_version: ClassVar[str] = "deprecation_record.v1"

    surface_id: str
    owner: str
    source_path: str
    replacement: str
    lifecycle: SurfaceLifecycle
    compatibility_window: str
    sunset_condition: str
    usage_evidence_ref: str
    required_gate_ids: tuple[str, ...]
    gate_evidence: tuple[RemovalGateEvidence, ...] = ()
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.surface_id, "surface_id"),
            (self.owner, "owner"),
            (self.source_path, "source_path"),
            (self.replacement, "replacement"),
            (self.compatibility_window, "compatibility_window"),
            (self.sunset_condition, "sunset_condition"),
            (self.usage_evidence_ref, "usage_evidence_ref"),
        ):
            if not value.strip():
                raise DeprecationContractError("DEPRECATION_FIELD_REQUIRED", field)
        if not self.required_gate_ids:
            raise DeprecationContractError(
                "DEPRECATION_REMOVAL_GATES_REQUIRED", self.surface_id
            )
        if len(set(self.required_gate_ids)) != len(self.required_gate_ids):
            raise DeprecationContractError(
                "DEPRECATION_REMOVAL_GATE_DUPLICATE", self.surface_id
            )
        evidence_ids = tuple(item.gate_id for item in self.gate_evidence)
        if len(set(evidence_ids)) != len(evidence_ids):
            raise DeprecationContractError(
                "DEPRECATION_GATE_EVIDENCE_DUPLICATE", self.surface_id
            )
        if any(item not in self.required_gate_ids for item in evidence_ids):
            raise DeprecationContractError(
                "DEPRECATION_UNKNOWN_GATE_EVIDENCE", self.surface_id
            )
        if self.lifecycle is SurfaceLifecycle.REMOVED and not self.removal_ready:
            raise DeprecationContractError(
                "DEPRECATION_REMOVED_WITH_OPEN_GATES", self.surface_id
            )
        if self.production_effect is not ProductionEffect.NONE:
            raise DeprecationContractError(
                "DEPRECATION_PRODUCTION_EFFECT_FORBIDDEN", self.surface_id
            )

    @property
    def removal_ready(self) -> bool:
        statuses = {item.gate_id: item.status for item in self.gate_evidence}
        return all(
            statuses.get(gate_id) is CanonicalStatus.PASS
            for gate_id in self.required_gate_ids
        )

    @property
    def open_gate_ids(self) -> tuple[str, ...]:
        statuses = {item.gate_id: item.status for item in self.gate_evidence}
        return tuple(
            gate_id
            for gate_id in self.required_gate_ids
            if statuses.get(gate_id) is not CanonicalStatus.PASS
        )

    def transition(
        self,
        target: SurfaceLifecycle,
        *,
        gate_evidence: tuple[RemovalGateEvidence, ...] | None = None,
    ) -> DeprecationRecord:
        expected = _NEXT_STATE.get(self.lifecycle)
        if expected is not target:
            raise DeprecationContractError(
                "DEPRECATION_TRANSITION_INVALID",
                f"{self.lifecycle.value}->{target.value}",
            )
        candidate = replace(
            self,
            lifecycle=target,
            gate_evidence=(
                self.gate_evidence if gate_evidence is None else gate_evidence
            ),
        )
        if target is SurfaceLifecycle.REMOVED and not candidate.removal_ready:
            raise DeprecationContractError(
                "DEPRECATION_REMOVAL_GATES_OPEN",
                ",".join(candidate.open_gate_ids),
            )
        return candidate

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "surface_id": self.surface_id,
            "owner": self.owner,
            "source_path": self.source_path,
            "replacement": self.replacement,
            "lifecycle": self.lifecycle.value,
            "compatibility_window": self.compatibility_window,
            "sunset_condition": self.sunset_condition,
            "usage_evidence_ref": self.usage_evidence_ref,
            "required_gate_ids": list(self.required_gate_ids),
            "gate_evidence": [item.to_dict() for item in self.gate_evidence],
            "removal_ready": self.removal_ready,
            "open_gate_ids": list(self.open_gate_ids),
            "production_effect": self.production_effect.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> DeprecationRecord:
        raw_evidence = payload.get("gate_evidence", [])
        if not isinstance(raw_evidence, list):
            raise DeprecationContractError(
                "DEPRECATION_GATE_EVIDENCE_LIST_REQUIRED", "gate_evidence"
            )
        record = cls(
            surface_id=str(payload.get("surface_id", "")),
            owner=str(payload.get("owner", "")),
            source_path=str(payload.get("source_path", "")),
            replacement=str(payload.get("replacement", "")),
            lifecycle=SurfaceLifecycle(str(payload.get("lifecycle", ""))),
            compatibility_window=str(payload.get("compatibility_window", "")),
            sunset_condition=str(payload.get("sunset_condition", "")),
            usage_evidence_ref=str(payload.get("usage_evidence_ref", "")),
            required_gate_ids=_string_tuple(
                payload.get("required_gate_ids"), "required_gate_ids"
            ),
            gate_evidence=tuple(
                RemovalGateEvidence.from_dict(_mapping(item))
                for item in raw_evidence
            ),
            production_effect=ProductionEffect.parse(
                str(payload.get("production_effect", "none"))
            ),
        )
        if payload.get("removal_ready") not in {None, record.removal_ready}:
            raise DeprecationContractError(
                "DEPRECATION_DERIVED_STATUS_MISMATCH", record.surface_id
            )
        return record


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise DeprecationContractError(
            "DEPRECATION_MAPPING_REQUIRED", type(value).__name__
        )
    return value


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise DeprecationContractError("DEPRECATION_LIST_REQUIRED", field)
    return tuple(str(item) for item in value)


def _datetime(value: object, field: str) -> datetime:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeprecationContractError(
            "DEPRECATION_DATETIME_INVALID", field
        ) from exc


__all__ = [
    "DeprecationContractError",
    "DeprecationRecord",
    "RemovalGateEvidence",
    "SurfaceLifecycle",
]
