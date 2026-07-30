from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

import yaml

from ai_trading_system.contracts.strategy_research_explorer import (
    ExplorerSourceKind,
    ExplorerSourceRef,
)

ATLAS_SOURCE_REGISTRY_SCHEMA_VERSION = "atlas_source_registry.v1"
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_ALLOWED_SOURCE_SUFFIXES = {".json", ".md", ".yaml", ".yml"}


class AtlasSourceProjectionError(ValueError):
    pass


@dataclass(frozen=True)
class AtlasGlossaryEntry:
    term: str
    plain_language: str


@dataclass(frozen=True)
class AtlasSourceRegistry:
    schema_version: str
    registry_id: str
    title: str
    as_of: datetime
    primary_research_start: date
    reader_notice: str
    source_payloads: tuple[Mapping[str, object], ...]
    node_payloads: tuple[Mapping[str, object], ...]
    edge_payloads: tuple[Mapping[str, object], ...]
    result_payloads: tuple[Mapping[str, object], ...]
    attribution_payloads: tuple[Mapping[str, object], ...]
    glossary: tuple[AtlasGlossaryEntry, ...]


def load_source_registry(path: Path) -> AtlasSourceRegistry:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise AtlasSourceProjectionError("ATLAS_REGISTRY_MAPPING_REQUIRED")
    schema_version = str(payload.get("schema_version", ""))
    if schema_version != ATLAS_SOURCE_REGISTRY_SCHEMA_VERSION:
        raise AtlasSourceProjectionError("ATLAS_REGISTRY_SCHEMA_VERSION_MISMATCH")
    glossary = tuple(
        AtlasGlossaryEntry(
            term=_required_text(item, "term"),
            plain_language=_required_text(item, "plain_language"),
        )
        for item in _mapping_list(payload.get("glossary"), "glossary")
    )
    registry = AtlasSourceRegistry(
        schema_version=schema_version,
        registry_id=_required_text(payload, "registry_id"),
        title=_required_text(payload, "title"),
        as_of=_aware_datetime(payload.get("as_of"), "as_of"),
        primary_research_start=_date_value(
            payload.get("primary_research_start"), "primary_research_start"
        ),
        reader_notice=_required_text(payload, "reader_notice"),
        source_payloads=_mapping_list(payload.get("sources"), "sources"),
        node_payloads=_mapping_list(payload.get("nodes"), "nodes"),
        edge_payloads=_mapping_list(payload.get("edges"), "edges"),
        result_payloads=_mapping_list(payload.get("results"), "results"),
        attribution_payloads=_mapping_list(payload.get("attributions"), "attributions"),
        glossary=glossary,
    )
    if not registry.source_payloads or not registry.node_payloads:
        raise AtlasSourceProjectionError("ATLAS_REGISTRY_SOURCES_AND_NODES_REQUIRED")
    if len({item.term for item in glossary}) != len(glossary):
        raise AtlasSourceProjectionError("ATLAS_REGISTRY_DUPLICATE_GLOSSARY_TERM")
    return registry


def project_source_refs(
    *,
    repository_root: Path,
    registry: AtlasSourceRegistry,
    exact_commit: str,
) -> tuple[ExplorerSourceRef, ...]:
    if not _COMMIT_PATTERN.fullmatch(exact_commit):
        raise AtlasSourceProjectionError("ATLAS_EXACT_COMMIT_INVALID")
    root = repository_root.resolve()
    projected: list[ExplorerSourceRef] = []
    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    for payload in registry.source_payloads:
        source_ref_id = _required_text(payload, "source_ref_id")
        source_path = _safe_source_path(_required_text(payload, "source_path"))
        if source_ref_id in seen_ids:
            raise AtlasSourceProjectionError(f"ATLAS_DUPLICATE_SOURCE_REF_ID:{source_ref_id}")
        if source_path in seen_paths:
            raise AtlasSourceProjectionError(f"ATLAS_DUPLICATE_SOURCE_PATH:{source_path}")
        seen_ids.add(source_ref_id)
        seen_paths.add(source_path)
        absolute_path = (root / Path(source_path)).resolve()
        try:
            absolute_path.relative_to(root)
        except ValueError as exc:
            raise AtlasSourceProjectionError(
                f"ATLAS_SOURCE_OUTSIDE_REPOSITORY:{source_path}"
            ) from exc
        if not absolute_path.is_file():
            raise AtlasSourceProjectionError(f"ATLAS_SOURCE_MISSING:{source_path}")
        source_bytes = absolute_path.read_bytes()
        projected.append(
            ExplorerSourceRef(
                source_ref_id=source_ref_id,
                source_kind=ExplorerSourceKind(_required_text(payload, "source_kind")),
                exact_commit=exact_commit,
                source_path=source_path,
                content_sha256=hashlib.sha256(source_bytes).hexdigest(),
                artifact_identity=_required_text(payload, "artifact_identity"),
                as_of=registry.as_of,
                requested_start=_optional_date(payload.get("requested_start")),
                requested_end=_optional_date(payload.get("requested_end")),
                evaluated_start=_optional_date(payload.get("evaluated_start")),
                evaluated_end=_optional_date(payload.get("evaluated_end")),
                known_at=_optional_datetime(payload.get("known_at"), "known_at"),
                available_at=_optional_datetime(payload.get("available_at"), "available_at"),
                research_context_complete=(payload.get("research_context_complete") is True),
                data_quality_ready=payload.get("data_quality_ready") is True,
                legacy_history_partial=(payload.get("legacy_history_partial") is True),
                limitation=str(payload.get("limitation", "")),
            )
        )
    return tuple(projected)


def _safe_source_path(value: str) -> str:
    if "\\" in value:
        raise AtlasSourceProjectionError(f"ATLAS_SOURCE_PATH_INVALID:{value}")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or "." in path.parts:
        raise AtlasSourceProjectionError(f"ATLAS_SOURCE_PATH_INVALID:{value}")
    if path.suffix.lower() not in _ALLOWED_SOURCE_SUFFIXES:
        raise AtlasSourceProjectionError(f"ATLAS_SOURCE_SUFFIX_NOT_ALLOWED:{value}")
    return path.as_posix()


def _mapping_list(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise AtlasSourceProjectionError(f"ATLAS_REGISTRY_LIST_REQUIRED:{field}")
    mappings: list[Mapping[str, object]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise AtlasSourceProjectionError(f"ATLAS_REGISTRY_MAPPING_REQUIRED:{field}")
        mappings.append(item)
    return tuple(mappings)


def _required_text(payload: Mapping[str, Any], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise AtlasSourceProjectionError(f"ATLAS_REGISTRY_TEXT_REQUIRED:{field}")
    return value


def _aware_datetime(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise AtlasSourceProjectionError(f"ATLAS_DATETIME_INVALID:{field}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise AtlasSourceProjectionError(f"ATLAS_TIMEZONE_REQUIRED:{field}")
    return parsed


def _optional_datetime(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    return _aware_datetime(value, field)


def _date_value(value: object, field: str) -> date:
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise AtlasSourceProjectionError(f"ATLAS_DATE_INVALID:{field}") from exc


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    return _date_value(value, "optional_date")


__all__ = [
    "ATLAS_SOURCE_REGISTRY_SCHEMA_VERSION",
    "AtlasGlossaryEntry",
    "AtlasSourceProjectionError",
    "AtlasSourceRegistry",
    "load_source_registry",
    "project_source_refs",
]
