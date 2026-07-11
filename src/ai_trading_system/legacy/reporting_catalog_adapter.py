from __future__ import annotations

import hashlib
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from ai_trading_system.contracts.report_spec import (
    ReaderTier,
    ReportCatalogAssessment,
    ReportCatalogDisposition,
    ReportCatalogEntryAssessment,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.workflow import EntrypointRef
from ai_trading_system.legacy.platform_contract_adapters import (
    PlatformContractAdapterError,
    report_registry_entry_to_spec,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path


class ReportingCatalogPolicy(Protocol):
    @property
    def legacy_unclassified_disposition(self) -> str: ...

_REQUIRED_TYPED_FIELDS = (
    "production_effect",
    "reader_tier",
    "actionable",
    "canonical_source",
    "section_provider",
    "view_model",
    "renderer",
)


def assess_report_registry_catalog(
    path: Path,
    *,
    policy: ReportingCatalogPolicy,
) -> ReportCatalogAssessment:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, Mapping):
        raise PlatformContractAdapterError("INVALID_REPORT_REGISTRY", str(path))
    raw_entries = payload.get("reports")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise PlatformContractAdapterError("INVALID_REPORT_REGISTRY_ENTRIES", str(path))
    entries: list[ReportCatalogEntryAssessment] = []
    for raw in raw_entries:
        if not isinstance(raw, Mapping):
            raise PlatformContractAdapterError("INVALID_REPORT_REGISTRY_ENTRY", str(path))
        report_id = str(raw.get("report_id", "")).strip()
        if not report_id:
            raise PlatformContractAdapterError("REQUIRED_LEGACY_FIELD_EMPTY", "report_id")
        missing = tuple(field for field in _REQUIRED_TYPED_FIELDS if field not in raw)
        if missing:
            entries.append(
                ReportCatalogEntryAssessment(
                    report_id=report_id,
                    status=CanonicalStatus.LIMITED,
                    disposition=ReportCatalogDisposition(policy.legacy_unclassified_disposition),
                    missing_semantics=missing,
                )
            )
            continue
        try:
            spec = report_registry_entry_to_spec(
                raw,
                canonical_source=EntrypointRef.from_dict(_mapping(raw, "canonical_source")),
                section_provider=EntrypointRef.from_dict(_mapping(raw, "section_provider")),
                view_model=EntrypointRef.from_dict(_mapping(raw, "view_model")),
                renderer=EntrypointRef.from_dict(_mapping(raw, "renderer")),
                reader_tier=ReaderTier(str(raw.get("reader_tier", ""))),
                actionable=raw.get("actionable") is True,
            )
        except (ValueError, PlatformContractAdapterError) as exc:
            entries.append(
                ReportCatalogEntryAssessment(
                    report_id=report_id,
                    status=CanonicalStatus.BLOCKED,
                    disposition=ReportCatalogDisposition.BLOCKED_INVALID_TYPED,
                    missing_semantics=(f"INVALID_TYPED_SEMANTICS:{exc}",),
                )
            )
            continue
        entries.append(
            ReportCatalogEntryAssessment(
                report_id=report_id,
                status=CanonicalStatus.PASS,
                disposition=ReportCatalogDisposition.TYPED,
                missing_semantics=(),
                report_spec=spec,
            )
        )
    return ReportCatalogAssessment(
        source_path=str(path),
        source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
        entries=tuple(entries),
    )


def _mapping(payload: Mapping[str, object], field: str) -> Mapping[str, object]:
    value = payload.get(field)
    if not isinstance(value, Mapping):
        raise PlatformContractAdapterError("INVALID_TYPED_REPORT_FIELD", field)
    return value
