from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.artifact_envelope import ArtifactLifecycle
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import EntrypointRef, WorkflowCadence
from ai_trading_system.core.production_effect import ProductionEffect


class ReportContractError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class ReportAudience(StrEnum):
    OWNER = "owner"
    INVESTOR = "investor"
    OPERATOR = "operator"
    RESEARCHER = "researcher"
    AUDITOR = "auditor"


class ReaderTier(StrEnum):
    OWNER_DAILY_BRIEF = "owner_daily_brief"
    RESEARCH_REVIEW_PACK = "research_review_pack"
    AUDIT_INDEX = "audit_index"


class ReportCatalogDisposition(StrEnum):
    TYPED = "TYPED"
    AUDIT_INDEX_LIMITED_UNCLASSIFIED = "AUDIT_INDEX_LIMITED_UNCLASSIFIED"
    BLOCKED_INVALID_TYPED = "BLOCKED_INVALID_TYPED"


def _require_text(value: str, field: str) -> None:
    if not value.strip():
        raise ReportContractError("REQUIRED_REPORT_FIELD_EMPTY", f"{field} is required")


def _int_value(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ReportContractError("INVALID_REPORT_PAYLOAD", f"{field} must be integer")
    return value


@dataclass(frozen=True)
class ReportSpec:
    schema_version: ClassVar[str] = "report_spec.v1"

    report_id: str
    title: str
    owner: str
    audience: ReportAudience
    reader_tier: ReaderTier
    cadence: WorkflowCadence
    canonical_source: EntrypointRef
    section_provider: EntrypointRef
    view_model: EntrypointRef
    renderer: EntrypointRef
    artifact_globs: tuple[str, ...]
    freshness_sla_days: int
    owner_action: str
    actionable: bool
    lifecycle: ArtifactLifecycle = ArtifactLifecycle.CURRENT
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.report_id, "report_id"),
            (self.title, "title"),
            (self.owner, "owner"),
            (self.owner_action, "owner_action"),
        ):
            _require_text(value, field)
        globs = tuple(sorted(set(self.artifact_globs)))
        if not globs or any(not item.strip() for item in globs):
            raise ReportContractError("REPORT_ARTIFACT_GLOB_REQUIRED", self.report_id)
        object.__setattr__(self, "artifact_globs", globs)
        if isinstance(self.freshness_sla_days, bool) or self.freshness_sla_days < 0:
            raise ReportContractError("INVALID_REPORT_FRESHNESS_SLA", self.report_id)
        if self.canonical_source == self.renderer:
            raise ReportContractError(
                "REPORT_SOURCE_RENDERER_COLLISION", "report renderer cannot be canonical source"
            )
        if not self.actionable and self.reader_tier is ReaderTier.OWNER_DAILY_BRIEF:
            raise ReportContractError("NON_ACTIONABLE_OWNER_DAILY_REPORT", self.report_id)

    @property
    def spec_id(self) -> str:
        material = json.dumps(
            self._semantic_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return f"report_spec_{hashlib.sha256(material).hexdigest()[:20]}"

    def _semantic_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "report_id": self.report_id,
            "title": self.title,
            "owner": self.owner,
            "audience": self.audience.value,
            "reader_tier": self.reader_tier.value,
            "cadence": self.cadence.value,
            "canonical_source": self.canonical_source.to_dict(),
            "section_provider": self.section_provider.to_dict(),
            "view_model": self.view_model.to_dict(),
            "renderer": self.renderer.to_dict(),
            "artifact_globs": list(self.artifact_globs),
            "freshness_sla_days": self.freshness_sla_days,
            "owner_action": self.owner_action,
            "actionable": self.actionable,
            "lifecycle": self.lifecycle.value,
            "production_effect": self.production_effect.value,
        }

    def to_dict(self) -> dict[str, object]:
        return {"spec_id": self.spec_id, **self._semantic_payload()}

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReportSpec:
        spec = cls(
            report_id=str(payload.get("report_id", "")),
            title=str(payload.get("title", "")),
            owner=str(payload.get("owner", "")),
            audience=ReportAudience(str(payload.get("audience", ""))),
            reader_tier=ReaderTier(str(payload.get("reader_tier", ""))),
            cadence=WorkflowCadence(str(payload.get("cadence", ""))),
            canonical_source=EntrypointRef.from_dict(_mapping(payload, "canonical_source")),
            section_provider=EntrypointRef.from_dict(_mapping(payload, "section_provider")),
            view_model=EntrypointRef.from_dict(_mapping(payload, "view_model")),
            renderer=EntrypointRef.from_dict(_mapping(payload, "renderer")),
            artifact_globs=tuple(str(item) for item in _list(payload, "artifact_globs")),
            freshness_sla_days=_int_value(
                payload.get("freshness_sla_days", -1), "freshness_sla_days"
            ),
            owner_action=str(payload.get("owner_action", "")),
            actionable=payload.get("actionable") is True,
            lifecycle=ArtifactLifecycle(str(payload.get("lifecycle", ""))),
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
        )
        supplied_id = payload.get("spec_id")
        if supplied_id is not None and str(supplied_id) != spec.spec_id:
            raise ReportContractError("REPORT_SPEC_ID_MISMATCH", spec.report_id)
        return spec


@dataclass(frozen=True)
class ReportSectionSpec:
    schema_version: ClassVar[str] = "report_section_spec.v1"

    section_id: str
    title: str
    owner: str
    reader_tier: ReaderTier
    provider: EntrypointRef
    provider_version: str
    source_keys: tuple[str, ...]
    core_order: int | None = None
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.section_id, "section_id"),
            (self.title, "title"),
            (self.owner, "owner"),
            (self.provider_version, "provider_version"),
        ):
            _require_text(value, field)
        keys = tuple(dict.fromkeys(self.source_keys))
        if not keys or any(not item.strip() for item in keys):
            raise ReportContractError("REPORT_SECTION_SOURCE_KEYS_REQUIRED", self.section_id)
        object.__setattr__(self, "source_keys", keys)
        if self.core_order is not None:
            if self.reader_tier is not ReaderTier.OWNER_DAILY_BRIEF:
                raise ReportContractError("REPORT_CORE_SECTION_TIER_INVALID", self.section_id)
            if isinstance(self.core_order, bool) or not 1 <= self.core_order <= 10:
                raise ReportContractError("REPORT_CORE_SECTION_ORDER_INVALID", self.section_id)
        if self.production_effect is not ProductionEffect.NONE:
            raise ReportContractError("REPORT_SECTION_PRODUCTION_EFFECT_FORBIDDEN", self.section_id)

    @property
    def spec_id(self) -> str:
        material = json.dumps(
            self.to_dict(include_id=False),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return f"report_section_spec_{hashlib.sha256(material).hexdigest()[:20]}"

    def to_dict(self, *, include_id: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "section_id": self.section_id,
            "title": self.title,
            "owner": self.owner,
            "reader_tier": self.reader_tier.value,
            "provider": self.provider.to_dict(),
            "provider_version": self.provider_version,
            "source_keys": list(self.source_keys),
            "core_order": self.core_order,
            "production_effect": self.production_effect.value,
        }
        return {"spec_id": self.spec_id, **payload} if include_id else payload

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReportSectionSpec:
        spec = cls(
            section_id=str(payload.get("section_id", "")),
            title=str(payload.get("title", "")),
            owner=str(payload.get("owner", "")),
            reader_tier=ReaderTier(str(payload.get("reader_tier", ""))),
            provider=EntrypointRef.from_dict(_mapping(payload, "provider")),
            provider_version=str(payload.get("provider_version", "")),
            source_keys=tuple(str(item) for item in _list(payload, "source_keys")),
            core_order=(
                None
                if payload.get("core_order") is None
                else _int_value(payload.get("core_order"), "core_order")
            ),
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
        )
        supplied = payload.get("spec_id")
        if supplied is not None and str(supplied) != spec.spec_id:
            raise ReportContractError("REPORT_SECTION_SPEC_ID_MISMATCH", spec.section_id)
        return spec


@dataclass(frozen=True)
class ReportSectionViewModel:
    schema_version: ClassVar[str] = "report_section_view_model.v1"

    section_spec_id: str
    section_id: str
    title: str
    reader_tier: ReaderTier
    status: CanonicalStatus
    summary: str
    facts: tuple[tuple[str, str], ...]
    source_keys: tuple[str, ...]
    caveats: tuple[str, ...] = ()
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        for value, field in (
            (self.section_spec_id, "section_spec_id"),
            (self.section_id, "section_id"),
            (self.title, "title"),
            (self.summary, "summary"),
        ):
            _require_text(value, field)
        if self.status not in {
            CanonicalStatus.PASS,
            CanonicalStatus.LIMITED,
            CanonicalStatus.BLOCKED,
            CanonicalStatus.NOT_DUE,
        }:
            raise ReportContractError("REPORT_SECTION_STATUS_INVALID", self.section_id)
        fact_keys = [key for key, _ in self.facts]
        if len(fact_keys) != len(set(fact_keys)) or any(not key.strip() for key in fact_keys):
            raise ReportContractError("REPORT_SECTION_FACTS_INVALID", self.section_id)
        if not self.source_keys or any(not item.strip() for item in self.source_keys):
            raise ReportContractError("REPORT_SECTION_SOURCE_KEYS_REQUIRED", self.section_id)
        if self.production_effect is not ProductionEffect.NONE:
            raise ReportContractError("REPORT_SECTION_PRODUCTION_EFFECT_FORBIDDEN", self.section_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "section_spec_id": self.section_spec_id,
            "section_id": self.section_id,
            "title": self.title,
            "reader_tier": self.reader_tier.value,
            "status": self.status.value,
            "summary": self.summary,
            "facts": [{"key": key, "value": value} for key, value in self.facts],
            "source_keys": list(self.source_keys),
            "caveats": list(self.caveats),
            "production_effect": self.production_effect.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReportSectionViewModel:
        facts: list[tuple[str, str]] = []
        for item in _list(payload, "facts"):
            mapped = _mapping_value(item, "fact")
            facts.append((str(mapped.get("key", "")), str(mapped.get("value", ""))))
        return cls(
            section_spec_id=str(payload.get("section_spec_id", "")),
            section_id=str(payload.get("section_id", "")),
            title=str(payload.get("title", "")),
            reader_tier=ReaderTier(str(payload.get("reader_tier", ""))),
            status=CanonicalStatus(str(payload.get("status", ""))),
            summary=str(payload.get("summary", "")),
            facts=tuple(facts),
            source_keys=tuple(str(item) for item in _list(payload, "source_keys")),
            caveats=tuple(str(item) for item in _list(payload, "caveats")),
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
        )


@dataclass(frozen=True)
class OwnerActionItem:
    action_id: str
    title: str
    owner_action: str
    due_status: CanonicalStatus
    actionable: bool
    priority: int
    source_artifact_ids: tuple[str, ...]
    blocker_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.action_id, "action_id"),
            (self.title, "title"),
            (self.owner_action, "owner_action"),
        ):
            _require_text(value, field)
        if self.due_status not in {
            CanonicalStatus.DUE,
            CanonicalStatus.NOT_DUE,
            CanonicalStatus.BLOCKED,
        }:
            raise ReportContractError("OWNER_ACTION_DUE_STATUS_INVALID", self.action_id)
        if isinstance(self.priority, bool) or self.priority < 1:
            raise ReportContractError("OWNER_ACTION_PRIORITY_INVALID", self.action_id)
        if not self.source_artifact_ids or any(
            not item.strip() for item in self.source_artifact_ids
        ):
            raise ReportContractError("OWNER_ACTION_SOURCE_REQUIRED", self.action_id)

    @property
    def eligible_for_owner_queue(self) -> bool:
        return self.due_status is CanonicalStatus.DUE and self.actionable

    def to_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "title": self.title,
            "owner_action": self.owner_action,
            "due_status": self.due_status.value,
            "actionable": self.actionable,
            "priority": self.priority,
            "source_artifact_ids": list(self.source_artifact_ids),
            "blocker_codes": list(self.blocker_codes),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OwnerActionItem:
        return cls(
            action_id=str(payload.get("action_id", "")),
            title=str(payload.get("title", "")),
            owner_action=str(payload.get("owner_action", "")),
            due_status=CanonicalStatus(str(payload.get("due_status", ""))),
            actionable=payload.get("actionable") is True,
            priority=_int_value(payload.get("priority"), "priority"),
            source_artifact_ids=tuple(str(item) for item in _list(payload, "source_artifact_ids")),
            blocker_codes=tuple(
                str(item) for item in _optional_list(payload, "blocker_codes")
            ),
        )


@dataclass(frozen=True)
class OwnerDailyBriefViewModel:
    schema_version: ClassVar[str] = "owner_daily_brief_view_model.v1"

    policy_id: str
    as_of: date
    generated_at: datetime
    status: CanonicalStatus
    sections: tuple[ReportSectionViewModel, ...]
    owner_queue: tuple[OwnerActionItem, ...]
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        _require_text(self.policy_id, "policy_id")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ReportContractError("OWNER_BRIEF_TIMEZONE_REQUIRED", self.policy_id)
        if not self.sections or len(self.sections) > 10:
            raise ReportContractError("OWNER_BRIEF_SECTION_LIMIT_INVALID", self.policy_id)
        section_ids = [item.section_id for item in self.sections]
        if len(section_ids) != len(set(section_ids)):
            raise ReportContractError("OWNER_BRIEF_SECTION_DUPLICATE", self.policy_id)
        if any(item.reader_tier is not ReaderTier.OWNER_DAILY_BRIEF for item in self.sections):
            raise ReportContractError("OWNER_BRIEF_SECTION_TIER_INVALID", self.policy_id)
        if any(not item.eligible_for_owner_queue for item in self.owner_queue):
            raise ReportContractError("OWNER_BRIEF_QUEUE_ITEM_INELIGIBLE", self.policy_id)
        priorities = [item.priority for item in self.owner_queue]
        if priorities != sorted(priorities):
            raise ReportContractError("OWNER_BRIEF_QUEUE_ORDER_INVALID", self.policy_id)
        if self.production_effect is not ProductionEffect.NONE:
            raise ReportContractError("OWNER_BRIEF_PRODUCTION_EFFECT_FORBIDDEN", self.policy_id)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "as_of": self.as_of.isoformat(),
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "sections": [item.to_dict() for item in self.sections],
            "owner_queue": [item.to_dict() for item in self.owner_queue],
            "production_effect": self.production_effect.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> OwnerDailyBriefViewModel:
        return cls(
            policy_id=str(payload.get("policy_id", "")),
            as_of=date.fromisoformat(str(payload.get("as_of", ""))),
            generated_at=datetime.fromisoformat(
                str(payload.get("generated_at", "")).replace("Z", "+00:00")
            ),
            status=CanonicalStatus(str(payload.get("status", ""))),
            sections=tuple(
                ReportSectionViewModel.from_dict(_mapping_value(item, "section"))
                for item in _list(payload, "sections")
            ),
            owner_queue=tuple(
                OwnerActionItem.from_dict(_mapping_value(item, "owner_queue"))
                for item in _list(payload, "owner_queue")
            ),
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
        )


@dataclass(frozen=True)
class ReportCatalogEntryAssessment:
    report_id: str
    status: CanonicalStatus
    disposition: ReportCatalogDisposition
    missing_semantics: tuple[str, ...]
    report_spec: ReportSpec | None = None

    def __post_init__(self) -> None:
        _require_text(self.report_id, "report_id")
        if self.report_spec is None and not self.missing_semantics:
            raise ReportContractError("REPORT_CATALOG_MISSING_SEMANTICS_REQUIRED", self.report_id)
        if self.report_spec is not None:
            if self.status is not CanonicalStatus.PASS:
                raise ReportContractError("REPORT_CATALOG_TYPED_STATUS_INVALID", self.report_id)
            if self.disposition is not ReportCatalogDisposition.TYPED:
                raise ReportContractError(
                    "REPORT_CATALOG_TYPED_DISPOSITION_INVALID", self.report_id
                )

    def to_dict(self) -> dict[str, object]:
        return {
            "report_id": self.report_id,
            "status": self.status.value,
            "disposition": self.disposition.value,
            "missing_semantics": list(self.missing_semantics),
            "report_spec": None if self.report_spec is None else self.report_spec.to_dict(),
        }


@dataclass(frozen=True)
class ReportCatalogAssessment:
    schema_version: ClassVar[str] = "report_catalog_assessment.v1"

    source_path: str
    source_sha256: str
    entries: tuple[ReportCatalogEntryAssessment, ...]

    def __post_init__(self) -> None:
        _require_text(self.source_path, "source_path")
        _require_text(self.source_sha256, "source_sha256")
        if not self.entries:
            raise ReportContractError("REPORT_CATALOG_ENTRIES_EMPTY", self.source_path)
        report_ids = [item.report_id for item in self.entries]
        if len(report_ids) != len(set(report_ids)):
            raise ReportContractError("REPORT_CATALOG_DUPLICATE_REPORT", self.source_path)

    @property
    def status(self) -> CanonicalStatus:
        if any(item.status is CanonicalStatus.BLOCKED for item in self.entries):
            return CanonicalStatus.BLOCKED
        if any(item.status is CanonicalStatus.LIMITED for item in self.entries):
            return CanonicalStatus.LIMITED
        return CanonicalStatus.PASS

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_path": self.source_path,
            "source_sha256": self.source_sha256,
            "status": self.status.value,
            "entry_count": len(self.entries),
            "typed_count": sum(item.report_spec is not None for item in self.entries),
            "limited_count": sum(item.status is CanonicalStatus.LIMITED for item in self.entries),
            "blocked_count": sum(item.status is CanonicalStatus.BLOCKED for item in self.entries),
            "entries": [item.to_dict() for item in self.entries],
        }


def _mapping(payload: Mapping[str, object], field: str) -> Mapping[str, object]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise ReportContractError("INVALID_REPORT_PAYLOAD", f"{field} must be mapping")
    return value


def _list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise ReportContractError("INVALID_REPORT_PAYLOAD", f"{field} must be list")
    return value


def _optional_list(payload: Mapping[str, object], field: str) -> list[object]:
    value = payload.get(field, [])
    if not isinstance(value, list):
        raise ReportContractError("INVALID_REPORT_PAYLOAD", f"{field} must be list")
    return value


def _mapping_value(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ReportContractError("INVALID_REPORT_PAYLOAD", f"{field} must be mapping")
    return value
