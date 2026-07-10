from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.artifact_envelope import ArtifactLifecycle
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
