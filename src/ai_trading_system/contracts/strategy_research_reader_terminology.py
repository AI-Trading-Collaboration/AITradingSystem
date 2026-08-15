from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_STABLE_ID = re.compile(r"^[a-z][a-z0-9_]*$")


class ReaderTerminologyContractError(ValueError):
    pass


class ReaderTermClassification(StrEnum):
    COMMON_LANGUAGE = "COMMON_LANGUAGE"
    INLINE_EXPLANATION = "INLINE_EXPLANATION"
    ACCESSIBLE_DISCLOSURE = "ACCESSIBLE_DISCLOSURE"
    GLOSSARY = "GLOSSARY"
    AUDIT_ONLY = "AUDIT_ONLY"
    PROHIBITED_UNEXPLAINED = "PROHIBITED_UNEXPLAINED"


class ReaderTextSurface(StrEnum):
    VISIBLE_TEXT = "VISIBLE_TEXT"
    ARIA_LABEL = "ARIA_LABEL"
    ARIA_DESCRIPTION = "ARIA_DESCRIPTION"
    TITLE = "TITLE"


class ReaderInteractionState(StrEnum):
    DEFAULT = "DEFAULT"
    EXPANDED_DISCLOSURE = "EXPANDED_DISCLOSURE"
    ATTRIBUTE = "ATTRIBUTE"
    AUDIT_DISCLOSURE = "AUDIT_DISCLOSURE"


class ReaderTextLayer(StrEnum):
    READER = "READER"
    AUDIT = "AUDIT"


def canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _required(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ReaderTerminologyContractError(f"READER_TERMINOLOGY_REQUIRED:{field}")
    return normalized


def _exact_keys(payload: Mapping[str, object], expected: set[str], field: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise ReaderTerminologyContractError(
            f"READER_TERMINOLOGY_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ReaderTerminologyContractError(f"READER_TERMINOLOGY_LIST_REQUIRED:{field}")
    result = tuple(_required(str(item), field) for item in value)
    if len(result) != len(set(result)):
        raise ReaderTerminologyContractError(f"READER_TERMINOLOGY_DUPLICATE:{field}")
    return result


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise ReaderTerminologyContractError(f"READER_TERMINOLOGY_LIST_REQUIRED:{field}")
    if not all(isinstance(item, Mapping) for item in value):
        raise ReaderTerminologyContractError(f"READER_TERMINOLOGY_MAPPING_REQUIRED:{field}")
    return tuple(item for item in value if isinstance(item, Mapping))


@dataclass(frozen=True)
class ReaderProfile:
    profile_id: str
    profile_version: str
    audience_zh: str
    assumed_knowledge_zh: tuple[str, ...]
    not_assumed_knowledge_zh: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _STABLE_ID.fullmatch(self.profile_id):
            raise ReaderTerminologyContractError("READER_PROFILE_ID_INVALID")
        _required(self.profile_version, "profile.version")
        _required(self.audience_zh, "profile.audience_zh")
        if not self.assumed_knowledge_zh or not self.not_assumed_knowledge_zh:
            raise ReaderTerminologyContractError("READER_PROFILE_KNOWLEDGE_BOUNDARY_REQUIRED")
        if len(self.assumed_knowledge_zh) != len(set(self.assumed_knowledge_zh)):
            raise ReaderTerminologyContractError("READER_PROFILE_ASSUMPTION_DUPLICATE")
        if len(self.not_assumed_knowledge_zh) != len(set(self.not_assumed_knowledge_zh)):
            raise ReaderTerminologyContractError("READER_PROFILE_NON_ASSUMPTION_DUPLICATE")


@dataclass(frozen=True)
class ReaderTermDefinition:
    term_id: str
    display_name_zh: str
    aliases: tuple[str, ...]
    classification: ReaderTermClassification
    plain_definition_zh: str
    why_needed_zh: str

    def __post_init__(self) -> None:
        if not _STABLE_ID.fullmatch(self.term_id):
            raise ReaderTerminologyContractError(
                f"READER_TERM_ID_INVALID:{self.term_id}"
            )
        _required(self.display_name_zh, f"term:{self.term_id}.display_name_zh")
        if not self.aliases:
            raise ReaderTerminologyContractError(
                f"READER_TERM_ALIAS_REQUIRED:{self.term_id}"
            )
        if len(self.aliases) != len(set(self.aliases)):
            raise ReaderTerminologyContractError(
                f"READER_TERM_ALIAS_DUPLICATE:{self.term_id}"
            )
        for alias in self.aliases:
            _required(alias, f"term:{self.term_id}.alias")
        if self.classification is ReaderTermClassification.PROHIBITED_UNEXPLAINED:
            raise ReaderTerminologyContractError(
                f"READER_TERM_PROHIBITED_CANNOT_BE_AUTHORIZED:{self.term_id}"
            )
        _required(self.plain_definition_zh, f"term:{self.term_id}.plain_definition_zh")
        _required(self.why_needed_zh, f"term:{self.term_id}.why_needed_zh")

    def to_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "display_name_zh": self.display_name_zh,
            "aliases": list(self.aliases),
            "classification": self.classification.value,
            "plain_definition_zh": self.plain_definition_zh,
            "why_needed_zh": self.why_needed_zh,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderTermDefinition:
        expected = {
            "term_id",
            "display_name_zh",
            "aliases",
            "classification",
            "plain_definition_zh",
            "why_needed_zh",
        }
        _exact_keys(payload, expected, "term")
        return cls(
            term_id=str(payload["term_id"]),
            display_name_zh=str(payload["display_name_zh"]),
            aliases=_string_tuple(payload["aliases"], "term.aliases"),
            classification=ReaderTermClassification(str(payload["classification"])),
            plain_definition_zh=str(payload["plain_definition_zh"]),
            why_needed_zh=str(payload["why_needed_zh"]),
        )


@dataclass(frozen=True)
class RenderedTermOccurrence:
    term_id: str
    matched_text: str
    ordinal: int
    dom_locator: str
    surface: ReaderTextSurface
    interaction_state: ReaderInteractionState
    reader_layer: ReaderTextLayer

    def __post_init__(self) -> None:
        if not _STABLE_ID.fullmatch(self.term_id):
            raise ReaderTerminologyContractError("RENDERED_TERM_ID_INVALID")
        _required(self.matched_text, "occurrence.matched_text")
        if self.ordinal <= 0:
            raise ReaderTerminologyContractError("RENDERED_TERM_ORDINAL_INVALID")
        _required(self.dom_locator, "occurrence.dom_locator")

    def to_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "matched_text": self.matched_text,
            "ordinal": self.ordinal,
            "dom_locator": self.dom_locator,
            "surface": self.surface.value,
            "interaction_state": self.interaction_state.value,
            "reader_layer": self.reader_layer.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RenderedTermOccurrence:
        expected = {
            "term_id",
            "matched_text",
            "ordinal",
            "dom_locator",
            "surface",
            "interaction_state",
            "reader_layer",
        }
        _exact_keys(payload, expected, "occurrence")
        return cls(
            term_id=str(payload["term_id"]),
            matched_text=str(payload["matched_text"]),
            ordinal=int(str(payload["ordinal"])),
            dom_locator=str(payload["dom_locator"]),
            surface=ReaderTextSurface(str(payload["surface"])),
            interaction_state=ReaderInteractionState(str(payload["interaction_state"])),
            reader_layer=ReaderTextLayer(str(payload["reader_layer"])),
        )


@dataclass(frozen=True)
class RenderedReaderSurface:
    ordinal: int
    normalized_text: str
    dom_locator: str
    surface: ReaderTextSurface
    interaction_state: ReaderInteractionState
    reader_layer: ReaderTextLayer
    term_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.ordinal <= 0:
            raise ReaderTerminologyContractError("RENDERED_SURFACE_ORDINAL_INVALID")
        _required(self.normalized_text, "surface.normalized_text")
        _required(self.dom_locator, "surface.dom_locator")
        if len(self.term_ids) != len(set(self.term_ids)):
            raise ReaderTerminologyContractError("RENDERED_SURFACE_TERM_IDS_DUPLICATE")
        if any(not _STABLE_ID.fullmatch(item) for item in self.term_ids):
            raise ReaderTerminologyContractError("RENDERED_SURFACE_TERM_ID_INVALID")

    def to_dict(self) -> dict[str, object]:
        return {
            "ordinal": self.ordinal,
            "normalized_text": self.normalized_text,
            "dom_locator": self.dom_locator,
            "surface": self.surface.value,
            "interaction_state": self.interaction_state.value,
            "reader_layer": self.reader_layer.value,
            "term_ids": list(self.term_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RenderedReaderSurface:
        expected = {
            "ordinal",
            "normalized_text",
            "dom_locator",
            "surface",
            "interaction_state",
            "reader_layer",
            "term_ids",
        }
        _exact_keys(payload, expected, "surface")
        return cls(
            ordinal=int(str(payload["ordinal"])),
            normalized_text=str(payload["normalized_text"]),
            dom_locator=str(payload["dom_locator"]),
            surface=ReaderTextSurface(str(payload["surface"])),
            interaction_state=ReaderInteractionState(str(payload["interaction_state"])),
            reader_layer=ReaderTextLayer(str(payload["reader_layer"])),
            term_ids=_string_tuple(payload["term_ids"], "surface.term_ids"),
        )


@dataclass(frozen=True)
class RenderedAuditIdentifier:
    surface_ordinal: int
    pattern_id: str
    identifier: str
    dom_locator: str

    def __post_init__(self) -> None:
        if self.surface_ordinal <= 0:
            raise ReaderTerminologyContractError("RENDERED_AUDIT_ORDINAL_INVALID")
        _required(self.pattern_id, "audit_identifier.pattern_id")
        _required(self.identifier, "audit_identifier.identifier")
        _required(self.dom_locator, "audit_identifier.dom_locator")

    def to_dict(self) -> dict[str, object]:
        return {
            "surface_ordinal": self.surface_ordinal,
            "pattern_id": self.pattern_id,
            "identifier": self.identifier,
            "dom_locator": self.dom_locator,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RenderedAuditIdentifier:
        expected = {"surface_ordinal", "pattern_id", "identifier", "dom_locator"}
        _exact_keys(payload, expected, "audit_identifier")
        return cls(
            surface_ordinal=int(str(payload["surface_ordinal"])),
            pattern_id=str(payload["pattern_id"]),
            identifier=str(payload["identifier"]),
            dom_locator=str(payload["dom_locator"]),
        )


@dataclass(frozen=True)
class RenderedTermResolution:
    term_id: str
    classification: ReaderTermClassification
    first_occurrence_ordinal: int
    first_occurrence_locator: str
    explanation_ordinal: int | None
    explanation_locator: str | None

    def __post_init__(self) -> None:
        if not _STABLE_ID.fullmatch(self.term_id):
            raise ReaderTerminologyContractError("RENDERED_TERM_RESOLUTION_ID_INVALID")
        if self.first_occurrence_ordinal <= 0:
            raise ReaderTerminologyContractError(
                "RENDERED_TERM_FIRST_OCCURRENCE_ORDINAL_INVALID"
            )
        _required(self.first_occurrence_locator, "resolution.first_occurrence_locator")
        explanation_required = self.classification not in {
            ReaderTermClassification.COMMON_LANGUAGE,
            ReaderTermClassification.AUDIT_ONLY,
        }
        if explanation_required:
            if self.explanation_ordinal is None or self.explanation_locator is None:
                raise ReaderTerminologyContractError(
                    f"RENDERED_TERM_EXPLANATION_REQUIRED:{self.term_id}"
                )
            if self.explanation_ordinal > self.first_occurrence_ordinal:
                raise ReaderTerminologyContractError(
                    f"RENDERED_TERM_EXPLANATION_AFTER_FIRST_USE:{self.term_id}"
                )
        elif self.explanation_ordinal is not None or self.explanation_locator is not None:
            raise ReaderTerminologyContractError(
                f"RENDERED_TERM_EXPLANATION_UNEXPECTED:{self.term_id}"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "term_id": self.term_id,
            "classification": self.classification.value,
            "first_occurrence_ordinal": self.first_occurrence_ordinal,
            "first_occurrence_locator": self.first_occurrence_locator,
            "explanation_ordinal": self.explanation_ordinal,
            "explanation_locator": self.explanation_locator,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RenderedTermResolution:
        expected = {
            "term_id",
            "classification",
            "first_occurrence_ordinal",
            "first_occurrence_locator",
            "explanation_ordinal",
            "explanation_locator",
        }
        _exact_keys(payload, expected, "resolution")
        return cls(
            term_id=str(payload["term_id"]),
            classification=ReaderTermClassification(str(payload["classification"])),
            first_occurrence_ordinal=int(str(payload["first_occurrence_ordinal"])),
            first_occurrence_locator=str(payload["first_occurrence_locator"]),
            explanation_ordinal=(
                None
                if payload["explanation_ordinal"] is None
                else int(str(payload["explanation_ordinal"]))
            ),
            explanation_locator=(
                None
                if payload["explanation_locator"] is None
                else str(payload["explanation_locator"])
            ),
        )


@dataclass(frozen=True)
class RenderedTermInventory:
    schema_version: ClassVar[str] = "atlas_rendered_term_inventory.v1"

    html_sha256: str
    reader_profile_id: str
    reader_profile_sha256: str
    terminology_policy_id: str
    terminology_policy_sha256: str
    scanned_surface_count: int
    surfaces: tuple[RenderedReaderSurface, ...]
    occurrences: tuple[RenderedTermOccurrence, ...]
    resolutions: tuple[RenderedTermResolution, ...]
    audit_identifiers: tuple[RenderedAuditIdentifier, ...]
    audit_identifier_count: int
    excluded_non_reader_regions: tuple[str, ...]
    content_sha256: str

    def __post_init__(self) -> None:
        for field, value in (
            ("html_sha256", self.html_sha256),
            ("reader_profile_sha256", self.reader_profile_sha256),
            ("terminology_policy_sha256", self.terminology_policy_sha256),
            ("content_sha256", self.content_sha256),
        ):
            if not _SHA256.fullmatch(value):
                raise ReaderTerminologyContractError(
                    f"RENDERED_TERM_SHA256_INVALID:{field}"
                )
        if not _STABLE_ID.fullmatch(self.reader_profile_id):
            raise ReaderTerminologyContractError("RENDERED_TERM_PROFILE_ID_INVALID")
        if not _STABLE_ID.fullmatch(self.terminology_policy_id):
            raise ReaderTerminologyContractError("RENDERED_TERM_POLICY_ID_INVALID")
        if self.scanned_surface_count <= 0:
            raise ReaderTerminologyContractError("RENDERED_TERM_SURFACE_COUNT_INVALID")
        surface_ordinals = tuple(item.ordinal for item in self.surfaces)
        if surface_ordinals != tuple(range(1, self.scanned_surface_count + 1)):
            raise ReaderTerminologyContractError("RENDERED_TERM_SURFACE_SET_INVALID")
        if self.audit_identifier_count != len(self.audit_identifiers):
            raise ReaderTerminologyContractError("RENDERED_TERM_AUDIT_COUNT_INVALID")
        if any(
            item.surface_ordinal > self.scanned_surface_count
            for item in self.audit_identifiers
        ):
            raise ReaderTerminologyContractError("RENDERED_TERM_AUDIT_SURFACE_INVALID")
        surfaces_by_ordinal = {item.ordinal: item for item in self.surfaces}
        if any(
            surfaces_by_ordinal[item.surface_ordinal].reader_layer
            is not ReaderTextLayer.AUDIT
            for item in self.audit_identifiers
        ):
            raise ReaderTerminologyContractError("RENDERED_TERM_AUDIT_LAYER_INVALID")
        term_ids = tuple(item.term_id for item in self.resolutions)
        if not term_ids or len(term_ids) != len(set(term_ids)):
            raise ReaderTerminologyContractError("RENDERED_TERM_RESOLUTION_SET_INVALID")
        if tuple(sorted(term_ids)) != term_ids:
            raise ReaderTerminologyContractError("RENDERED_TERM_RESOLUTION_ORDER_INVALID")
        if tuple(item.ordinal for item in self.occurrences) != tuple(
            sorted(item.ordinal for item in self.occurrences)
        ):
            raise ReaderTerminologyContractError("RENDERED_TERM_OCCURRENCE_ORDER_INVALID")
        occurrences_by_surface: dict[int, set[str]] = {
            item.ordinal: set() for item in self.surfaces
        }
        for occurrence in self.occurrences:
            surface = surfaces_by_ordinal.get(occurrence.ordinal)
            if surface is None or (
                occurrence.dom_locator,
                occurrence.surface,
                occurrence.interaction_state,
                occurrence.reader_layer,
            ) != (
                surface.dom_locator,
                surface.surface,
                surface.interaction_state,
                surface.reader_layer,
            ):
                raise ReaderTerminologyContractError(
                    "RENDERED_TERM_OCCURRENCE_SURFACE_MISMATCH"
                )
            occurrences_by_surface[occurrence.ordinal].add(occurrence.term_id)
        if any(
            set(surface.term_ids) != occurrences_by_surface[surface.ordinal]
            for surface in self.surfaces
        ):
            raise ReaderTerminologyContractError("RENDERED_TERM_SURFACE_MAPPING_MISMATCH")
        expected_hash = hashlib.sha256(
            canonical_json_bytes(self._payload(include_hash=False))
        ).hexdigest()
        if self.content_sha256 != expected_hash:
            raise ReaderTerminologyContractError("RENDERED_TERM_CONTENT_SHA_MISMATCH")

    def _payload(self, *, include_hash: bool) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "html_sha256": self.html_sha256,
            "reader_profile_id": self.reader_profile_id,
            "reader_profile_sha256": self.reader_profile_sha256,
            "terminology_policy_id": self.terminology_policy_id,
            "terminology_policy_sha256": self.terminology_policy_sha256,
            "scanned_surface_count": self.scanned_surface_count,
            "surfaces": [item.to_dict() for item in self.surfaces],
            "occurrences": [item.to_dict() for item in self.occurrences],
            "resolutions": [item.to_dict() for item in self.resolutions],
            "audit_identifiers": [item.to_dict() for item in self.audit_identifiers],
            "audit_identifier_count": self.audit_identifier_count,
            "excluded_non_reader_regions": list(self.excluded_non_reader_regions),
        }
        if include_hash:
            payload["content_sha256"] = self.content_sha256
        return payload

    def to_dict(self) -> dict[str, object]:
        return self._payload(include_hash=True)

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @classmethod
    def seal(
        cls,
        *,
        html_sha256: str,
        reader_profile_id: str,
        reader_profile_sha256: str,
        terminology_policy_id: str,
        terminology_policy_sha256: str,
        scanned_surface_count: int,
        surfaces: Sequence[RenderedReaderSurface],
        occurrences: Sequence[RenderedTermOccurrence],
        resolutions: Sequence[RenderedTermResolution],
        audit_identifiers: Sequence[RenderedAuditIdentifier],
        excluded_non_reader_regions: Sequence[str],
    ) -> RenderedTermInventory:
        base: dict[str, object] = {
            "schema_version": cls.schema_version,
            "html_sha256": html_sha256,
            "reader_profile_id": reader_profile_id,
            "reader_profile_sha256": reader_profile_sha256,
            "terminology_policy_id": terminology_policy_id,
            "terminology_policy_sha256": terminology_policy_sha256,
            "scanned_surface_count": scanned_surface_count,
            "surfaces": [item.to_dict() for item in surfaces],
            "occurrences": [item.to_dict() for item in occurrences],
            "resolutions": [item.to_dict() for item in resolutions],
            "audit_identifiers": [item.to_dict() for item in audit_identifiers],
            "audit_identifier_count": len(audit_identifiers),
            "excluded_non_reader_regions": list(excluded_non_reader_regions),
        }
        return cls.from_dict(
            {**base, "content_sha256": hashlib.sha256(canonical_json_bytes(base)).hexdigest()}
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RenderedTermInventory:
        expected = {
            "schema_version",
            "html_sha256",
            "reader_profile_id",
            "reader_profile_sha256",
            "terminology_policy_id",
            "terminology_policy_sha256",
            "scanned_surface_count",
            "surfaces",
            "occurrences",
            "resolutions",
            "audit_identifiers",
            "audit_identifier_count",
            "excluded_non_reader_regions",
            "content_sha256",
        }
        _exact_keys(payload, expected, "inventory")
        if payload["schema_version"] != cls.schema_version:
            raise ReaderTerminologyContractError("RENDERED_TERM_SCHEMA_INVALID")
        return cls(
            html_sha256=str(payload["html_sha256"]),
            reader_profile_id=str(payload["reader_profile_id"]),
            reader_profile_sha256=str(payload["reader_profile_sha256"]),
            terminology_policy_id=str(payload["terminology_policy_id"]),
            terminology_policy_sha256=str(payload["terminology_policy_sha256"]),
            scanned_surface_count=int(str(payload["scanned_surface_count"])),
            surfaces=tuple(
                RenderedReaderSurface.from_dict(item)
                for item in _mapping_tuple(payload["surfaces"], "inventory.surfaces")
            ),
            occurrences=tuple(
                RenderedTermOccurrence.from_dict(item)
                for item in _mapping_tuple(payload["occurrences"], "inventory.occurrences")
            ),
            resolutions=tuple(
                RenderedTermResolution.from_dict(item)
                for item in _mapping_tuple(payload["resolutions"], "inventory.resolutions")
            ),
            audit_identifiers=tuple(
                RenderedAuditIdentifier.from_dict(item)
                for item in _mapping_tuple(
                    payload["audit_identifiers"], "inventory.audit_identifiers"
                )
            ),
            audit_identifier_count=int(str(payload["audit_identifier_count"])),
            excluded_non_reader_regions=_string_tuple(
                payload["excluded_non_reader_regions"],
                "inventory.excluded_non_reader_regions",
            ),
            content_sha256=str(payload["content_sha256"]),
        )

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> RenderedTermInventory:
        try:
            decoded = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReaderTerminologyContractError("RENDERED_TERM_JSON_INVALID") from exc
        if not isinstance(decoded, Mapping):
            raise ReaderTerminologyContractError("RENDERED_TERM_JSON_OBJECT_REQUIRED")
        result = cls.from_dict(decoded)
        if result.canonical_bytes != payload:
            raise ReaderTerminologyContractError("RENDERED_TERM_NON_CANONICAL_BYTES")
        return result


__all__ = [
    "ReaderInteractionState",
    "ReaderProfile",
    "ReaderTermClassification",
    "ReaderTermDefinition",
    "ReaderTerminologyContractError",
    "ReaderTextLayer",
    "ReaderTextSurface",
    "RenderedAuditIdentifier",
    "RenderedReaderSurface",
    "RenderedTermInventory",
    "RenderedTermOccurrence",
    "RenderedTermResolution",
    "canonical_json_bytes",
]
