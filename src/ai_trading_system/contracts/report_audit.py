from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from ai_trading_system.contracts.report_spec import ReportCatalogAssessment
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect


class ReportAuditContractError(ValueError):
    pass


@dataclass(frozen=True)
class ReportAuditIndexViewModel:
    schema_version: ClassVar[str] = "report_audit_index_view_model.v1"

    policy_id: str
    generated_at: datetime
    catalog: ReportCatalogAssessment
    include_all_registry_entries: bool = True
    include_legacy_unclassified: bool = True
    production_effect: ProductionEffect = ProductionEffect.NONE

    def __post_init__(self) -> None:
        if not self.policy_id.strip():
            raise ReportAuditContractError("REPORT_AUDIT_POLICY_REQUIRED")
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ReportAuditContractError("REPORT_AUDIT_TIMEZONE_REQUIRED")
        if not self.include_all_registry_entries or not self.include_legacy_unclassified:
            raise ReportAuditContractError("REPORT_AUDIT_FULL_COVERAGE_REQUIRED")
        if self.production_effect is not ProductionEffect.NONE:
            raise ReportAuditContractError("REPORT_AUDIT_PRODUCTION_EFFECT_FORBIDDEN")

    @property
    def status(self) -> CanonicalStatus:
        return self.catalog.status

    def to_dict(self) -> dict[str, object]:
        catalog = self.catalog.to_dict()
        return {
            "schema_version": self.schema_version,
            "policy_id": self.policy_id,
            "generated_at": self.generated_at.isoformat(),
            "status": self.status.value,
            "source_path": self.catalog.source_path,
            "source_sha256": self.catalog.source_sha256,
            "entry_count": catalog["entry_count"],
            "typed_count": catalog["typed_count"],
            "limited_count": catalog["limited_count"],
            "blocked_count": catalog["blocked_count"],
            "include_all_registry_entries": self.include_all_registry_entries,
            "include_legacy_unclassified": self.include_legacy_unclassified,
            "entries": catalog["entries"],
            "production_effect": self.production_effect.value,
        }


__all__ = ["ReportAuditContractError", "ReportAuditIndexViewModel"]
