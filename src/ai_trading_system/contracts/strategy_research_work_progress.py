from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.strategy_research_status_explanation import (
    ATLAS_STATUS_EXPLANATION_STAGE_IDS,
)

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_CONCEPT_ID_PATTERN = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_INTERNAL_IDENTIFIER_PATTERN = re.compile(r"\b[A-Z][A-Z0-9]+_[A-Z0-9_]+\b")
_HASH_LIKE_PATTERN = re.compile(r"\b[0-9a-f]{40,}\b", re.IGNORECASE)
_PATH_LIKE_PATTERN = re.compile(r"\b(?:src|config|docs|outputs)/", re.IGNORECASE)
_READER_FIRST_FORBIDDEN_TERMS = re.compile(
    r"\b(?:diff|source lineage|validator|schema|sha-?256|canonical)\b",
    re.IGNORECASE,
)


class StrategyResearchWorkProgressContractError(ValueError):
    pass


class CapabilityProgress(StrEnum):
    AVAILABLE = "AVAILABLE"
    IN_PROGRESS = "IN_PROGRESS"
    BLOCKED = "BLOCKED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class ResearchEffect(StrEnum):
    NO_NEW_RESEARCH_EVIDENCE = "NO_NEW_RESEARCH_EVIDENCE"
    LIMITED_RESEARCH_EVIDENCE = "LIMITED_RESEARCH_EVIDENCE"
    OWNER_DECISION_ONLY = "OWNER_DECISION_ONLY"


@dataclass(frozen=True)
class StrategyResearchProgressMatrix:
    stage_count: int
    capability_available: int
    capability_in_progress: int
    capability_blocked: int
    capability_not_applicable: int
    research_no_new_evidence: int
    research_limited_evidence: int
    research_owner_decision_only: int

    def __post_init__(self) -> None:
        counts = (
            self.capability_available,
            self.capability_in_progress,
            self.capability_blocked,
            self.capability_not_applicable,
            self.research_no_new_evidence,
            self.research_limited_evidence,
            self.research_owner_decision_only,
        )
        if self.stage_count != len(ATLAS_STATUS_EXPLANATION_STAGE_IDS):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_MATRIX_STAGE_COUNT_INVALID"
            )
        if any(value < 0 for value in counts):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_MATRIX_NEGATIVE_COUNT"
            )
        if (
            self.capability_available
            + self.capability_in_progress
            + self.capability_blocked
            + self.capability_not_applicable
            != self.stage_count
        ):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_MATRIX_CAPABILITY_TOTAL_INVALID"
            )
        if (
            self.research_no_new_evidence
            + self.research_limited_evidence
            + self.research_owner_decision_only
            != self.stage_count
        ):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_MATRIX_RESEARCH_TOTAL_INVALID"
            )


def build_strategy_research_progress_matrix(
    stage_records: Sequence[StageWorkProgressRecord],
) -> StrategyResearchProgressMatrix:
    stage_ids = tuple(item.stage_id for item in stage_records)
    if stage_ids != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
        raise StrategyResearchWorkProgressContractError(
            "WORK_PROGRESS_MATRIX_STAGE_SET_OR_ORDER_INVALID"
        )
    return StrategyResearchProgressMatrix(
        stage_count=len(stage_records),
        capability_available=sum(
            item.capability_progress is CapabilityProgress.AVAILABLE
            for item in stage_records
        ),
        capability_in_progress=sum(
            item.capability_progress is CapabilityProgress.IN_PROGRESS
            for item in stage_records
        ),
        capability_blocked=sum(
            item.capability_progress is CapabilityProgress.BLOCKED
            for item in stage_records
        ),
        capability_not_applicable=sum(
            item.capability_progress is CapabilityProgress.NOT_APPLICABLE
            for item in stage_records
        ),
        research_no_new_evidence=sum(
            item.research_effect is ResearchEffect.NO_NEW_RESEARCH_EVIDENCE
            for item in stage_records
        ),
        research_limited_evidence=sum(
            item.research_effect is ResearchEffect.LIMITED_RESEARCH_EVIDENCE
            for item in stage_records
        ),
        research_owner_decision_only=sum(
            item.research_effect is ResearchEffect.OWNER_DECISION_ONLY
            for item in stage_records
        ),
    )


def _required_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_REQUIRED_TEXT:{field}"
        )


def _reader_first_text(value: str, field: str) -> None:
    _required_text(value, field)
    if (
        "`" in value
        or _INTERNAL_IDENTIFIER_PATTERN.search(value)
        or _HASH_LIKE_PATTERN.search(value)
        or _PATH_LIKE_PATTERN.search(value)
        or _READER_FIRST_FORBIDDEN_TERMS.search(value)
    ):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_READER_FIRST_INTERNAL_TERM:{field}"
        )


def _require_unique(values: Sequence[str], field: str) -> None:
    if len(set(values)) != len(values):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_DUPLICATE_VALUE:{field}"
        )


def _require_exact_keys(
    payload: Mapping[str, object],
    expected: set[str],
    field: str,
) -> None:
    actual = set(payload)
    if actual != expected:
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_SCHEMA_KEYS_INVALID:{field}:"
            f"missing={sorted(expected - actual)}:extra={sorted(actual - expected)}"
        )


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_MAPPING_REQUIRED:{field}"
        )
    return value


def _mapping_tuple(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_LIST_REQUIRED:{field}"
        )
    return tuple(_mapping(item, field) for item in value)


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_LIST_REQUIRED:{field}"
        )
    result = tuple(str(item) for item in value)
    if not result or any(not item.strip() for item in result):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_TEXT_LIST_INVALID:{field}"
        )
    return result


def _optional_string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_LIST_REQUIRED:{field}"
        )
    result = tuple(str(item) for item in value)
    if any(not item.strip() for item in result):
        raise StrategyResearchWorkProgressContractError(
            f"WORK_PROGRESS_TEXT_LIST_INVALID:{field}"
        )
    return result


def _canonical_json_bytes(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


@dataclass(frozen=True)
class ReaderConcept:
    concept_id: str
    display_name_zh: str
    plain_definition_zh: str
    why_needed_zh: str
    example_zh: str
    related_concept_ids: tuple[str, ...]
    source_ref_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if not _CONCEPT_ID_PATTERN.fullmatch(self.concept_id):
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_CONCEPT_ID_INVALID:{self.concept_id}"
            )
        for value, field in (
            (self.display_name_zh, "display_name_zh"),
            (self.plain_definition_zh, "plain_definition_zh"),
            (self.why_needed_zh, "why_needed_zh"),
            (self.example_zh, "example_zh"),
        ):
            _reader_first_text(value, f"concept:{self.concept_id}.{field}")
        _require_unique(
            self.related_concept_ids,
            f"concept:{self.concept_id}.related_concept_ids",
        )
        if self.concept_id in self.related_concept_ids:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_CONCEPT_SELF_REFERENCE:{self.concept_id}"
            )
        if not self.source_ref_ids:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_CONCEPT_SOURCE_REQUIRED:{self.concept_id}"
            )
        _require_unique(self.source_ref_ids, f"concept:{self.concept_id}.source_ref_ids")

    def to_dict(self) -> dict[str, object]:
        return {
            "concept_id": self.concept_id,
            "display_name_zh": self.display_name_zh,
            "plain_definition_zh": self.plain_definition_zh,
            "why_needed_zh": self.why_needed_zh,
            "example_zh": self.example_zh,
            "related_concept_ids": list(self.related_concept_ids),
            "source_ref_ids": list(self.source_ref_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ReaderConcept:
        _require_exact_keys(
            payload,
            {
                "concept_id",
                "display_name_zh",
                "plain_definition_zh",
                "why_needed_zh",
                "example_zh",
                "related_concept_ids",
                "source_ref_ids",
            },
            "concept",
        )
        return cls(
            concept_id=str(payload["concept_id"]),
            display_name_zh=str(payload["display_name_zh"]),
            plain_definition_zh=str(payload["plain_definition_zh"]),
            why_needed_zh=str(payload["why_needed_zh"]),
            example_zh=str(payload["example_zh"]),
            related_concept_ids=_optional_string_tuple(
                payload["related_concept_ids"],
                "concept.related_concept_ids",
            ),
            source_ref_ids=_string_tuple(payload["source_ref_ids"], "concept.source_ref_ids"),
        )


@dataclass(frozen=True)
class StageWorkProgressRecord:
    schema_version: ClassVar[str] = "strategy_research_stage_work_progress.v1"

    stage_id: str
    display_title_zh: str
    why_needed_zh: str
    work_items_zh: tuple[str, ...]
    capability_progress: CapabilityProgress
    capability_progress_zh: str
    latest_execution_status: str
    latest_execution_summary_zh: str
    research_effect: ResearchEffect
    research_effect_zh: str
    expected_outputs_zh: tuple[str, ...]
    downstream_use_zh: str
    boundary_zh: str
    next_trigger_zh: str
    concept_ids: tuple[str, ...]
    source_ref_ids: tuple[str, ...]
    template_version: str

    def __post_init__(self) -> None:
        if self.stage_id not in ATLAS_STATUS_EXPLANATION_STAGE_IDS:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_STAGE_ID_INVALID:{self.stage_id}"
            )
        for value, field in (
            (self.display_title_zh, "display_title_zh"),
            (self.why_needed_zh, "why_needed_zh"),
            (self.capability_progress_zh, "capability_progress_zh"),
            (self.latest_execution_summary_zh, "latest_execution_summary_zh"),
            (self.research_effect_zh, "research_effect_zh"),
            (self.downstream_use_zh, "downstream_use_zh"),
            (self.boundary_zh, "boundary_zh"),
            (self.next_trigger_zh, "next_trigger_zh"),
        ):
            _reader_first_text(value, f"stage:{self.stage_id}.{field}")
        _required_text(self.latest_execution_status, "latest_execution_status")
        _required_text(self.template_version, "template_version")
        for values, field in (
            (self.work_items_zh, "work_items_zh"),
            (self.expected_outputs_zh, "expected_outputs_zh"),
        ):
            if not values:
                raise StrategyResearchWorkProgressContractError(
                    f"WORK_PROGRESS_STAGE_LIST_REQUIRED:{self.stage_id}:{field}"
                )
            _require_unique(values, f"stage:{self.stage_id}.{field}")
            for index, value in enumerate(values):
                _reader_first_text(value, f"stage:{self.stage_id}.{field}[{index}]")
        if not self.concept_ids:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_STAGE_CONCEPT_REQUIRED:{self.stage_id}"
            )
        _require_unique(self.concept_ids, f"stage:{self.stage_id}.concept_ids")
        if not self.source_ref_ids:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_STAGE_SOURCE_REQUIRED:{self.stage_id}"
            )
        _require_unique(self.source_ref_ids, f"stage:{self.stage_id}.source_ref_ids")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "stage_id": self.stage_id,
            "display_title_zh": self.display_title_zh,
            "why_needed_zh": self.why_needed_zh,
            "work_items_zh": list(self.work_items_zh),
            "capability_progress": self.capability_progress.value,
            "capability_progress_zh": self.capability_progress_zh,
            "latest_execution_status": self.latest_execution_status,
            "latest_execution_summary_zh": self.latest_execution_summary_zh,
            "research_effect": self.research_effect.value,
            "research_effect_zh": self.research_effect_zh,
            "expected_outputs_zh": list(self.expected_outputs_zh),
            "downstream_use_zh": self.downstream_use_zh,
            "boundary_zh": self.boundary_zh,
            "next_trigger_zh": self.next_trigger_zh,
            "concept_ids": list(self.concept_ids),
            "source_ref_ids": list(self.source_ref_ids),
            "template_version": self.template_version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> StageWorkProgressRecord:
        _require_exact_keys(
            payload,
            {
                "schema_version",
                "stage_id",
                "display_title_zh",
                "why_needed_zh",
                "work_items_zh",
                "capability_progress",
                "capability_progress_zh",
                "latest_execution_status",
                "latest_execution_summary_zh",
                "research_effect",
                "research_effect_zh",
                "expected_outputs_zh",
                "downstream_use_zh",
                "boundary_zh",
                "next_trigger_zh",
                "concept_ids",
                "source_ref_ids",
                "template_version",
            },
            "stage_record",
        )
        if payload["schema_version"] != cls.schema_version:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_STAGE_SCHEMA_VERSION_INVALID"
            )
        return cls(
            stage_id=str(payload["stage_id"]),
            display_title_zh=str(payload["display_title_zh"]),
            why_needed_zh=str(payload["why_needed_zh"]),
            work_items_zh=_string_tuple(payload["work_items_zh"], "stage.work_items_zh"),
            capability_progress=CapabilityProgress(str(payload["capability_progress"])),
            capability_progress_zh=str(payload["capability_progress_zh"]),
            latest_execution_status=str(payload["latest_execution_status"]),
            latest_execution_summary_zh=str(payload["latest_execution_summary_zh"]),
            research_effect=ResearchEffect(str(payload["research_effect"])),
            research_effect_zh=str(payload["research_effect_zh"]),
            expected_outputs_zh=_string_tuple(
                payload["expected_outputs_zh"],
                "stage.expected_outputs_zh",
            ),
            downstream_use_zh=str(payload["downstream_use_zh"]),
            boundary_zh=str(payload["boundary_zh"]),
            next_trigger_zh=str(payload["next_trigger_zh"]),
            concept_ids=_string_tuple(payload["concept_ids"], "stage.concept_ids"),
            source_ref_ids=_string_tuple(payload["source_ref_ids"], "stage.source_ref_ids"),
            template_version=str(payload["template_version"]),
        )


@dataclass(frozen=True)
class StrategyResearchWorkProgressBundle:
    schema_id: ClassVar[str] = "strategy_research_work_progress.v1"
    schema_version: ClassVar[str] = "1.0.0"

    snapshot_id: str
    primary_research_start: str
    stage_records: tuple[StageWorkProgressRecord, ...]
    concepts: tuple[ReaderConcept, ...]
    policy_id: str
    policy_version: str
    policy_sha256: str
    content_sha256: str

    def __post_init__(self) -> None:
        if not _SHA256_PATTERN.fullmatch(self.snapshot_id):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_SNAPSHOT_ID_INVALID"
            )
        for value, field in (
            (self.primary_research_start, "primary_research_start"),
            (self.policy_id, "policy_id"),
            (self.policy_version, "policy_version"),
        ):
            _required_text(value, field)
        if not _SHA256_PATTERN.fullmatch(self.policy_sha256):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_POLICY_SHA256_INVALID"
            )
        if not _SHA256_PATTERN.fullmatch(self.content_sha256):
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CONTENT_SHA256_INVALID"
            )
        stage_ids = tuple(item.stage_id for item in self.stage_records)
        if stage_ids != ATLAS_STATUS_EXPLANATION_STAGE_IDS:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_STAGE_SET_OR_ORDER_INVALID"
            )
        concept_ids = tuple(item.concept_id for item in self.concepts)
        if not concept_ids:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CONCEPT_SET_REQUIRED"
            )
        _require_unique(concept_ids, "bundle.concept_ids")
        concept_set = set(concept_ids)
        referenced = {
            concept_id
            for record in self.stage_records
            for concept_id in record.concept_ids
        }
        related = {
            related_id
            for concept in self.concepts
            for related_id in concept.related_concept_ids
        }
        missing = sorted((referenced | related) - concept_set)
        if missing:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CONCEPT_REFERENCE_UNKNOWN:" + ",".join(missing)
            )
        reachable = _reachable_concepts(
            roots=referenced,
            concepts={item.concept_id: item for item in self.concepts},
        )
        orphaned = sorted(concept_set - reachable)
        if orphaned:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CONCEPT_ORPHANED:" + ",".join(orphaned)
            )
        _validate_acyclic_concepts({item.concept_id: item for item in self.concepts})
        if self.compute_content_sha256() != self.content_sha256:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CONTENT_SHA256_MISMATCH"
            )

    @classmethod
    def seal(
        cls,
        *,
        snapshot_id: str,
        primary_research_start: str,
        stage_records: tuple[StageWorkProgressRecord, ...],
        concepts: tuple[ReaderConcept, ...],
        policy_id: str,
        policy_version: str,
        policy_sha256: str,
    ) -> StrategyResearchWorkProgressBundle:
        provisional = object.__new__(cls)
        object.__setattr__(provisional, "snapshot_id", snapshot_id)
        object.__setattr__(provisional, "primary_research_start", primary_research_start)
        object.__setattr__(provisional, "stage_records", stage_records)
        object.__setattr__(provisional, "concepts", concepts)
        object.__setattr__(provisional, "policy_id", policy_id)
        object.__setattr__(provisional, "policy_version", policy_version)
        object.__setattr__(provisional, "policy_sha256", policy_sha256)
        object.__setattr__(provisional, "content_sha256", "0" * 64)
        return cls(
            snapshot_id=snapshot_id,
            primary_research_start=primary_research_start,
            stage_records=stage_records,
            concepts=concepts,
            policy_id=policy_id,
            policy_version=policy_version,
            policy_sha256=policy_sha256,
            content_sha256=provisional.compute_content_sha256(),
        )

    def _content_payload(self) -> dict[str, object]:
        return {
            "schema_id": self.schema_id,
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "primary_research_start": self.primary_research_start,
            "stage_records": [item.to_dict() for item in self.stage_records],
            "concepts": [item.to_dict() for item in self.concepts],
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "policy_sha256": self.policy_sha256,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._content_payload(), "content_sha256": self.content_sha256}

    def compute_content_sha256(self) -> str:
        return hashlib.sha256(_canonical_json_bytes(self._content_payload())).hexdigest()

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_json_bytes(self.to_dict())

    @classmethod
    def from_json_bytes(cls, payload: bytes) -> StrategyResearchWorkProgressBundle:
        try:
            raw = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_JSON_INVALID"
            ) from exc
        mapping = _mapping(raw, "bundle")
        _require_exact_keys(
            mapping,
            {
                "schema_id",
                "schema_version",
                "snapshot_id",
                "primary_research_start",
                "stage_records",
                "concepts",
                "policy_id",
                "policy_version",
                "policy_sha256",
                "content_sha256",
            },
            "bundle",
        )
        if mapping["schema_id"] != cls.schema_id or mapping["schema_version"] != cls.schema_version:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_BUNDLE_SCHEMA_INVALID"
            )
        bundle = cls(
            snapshot_id=str(mapping["snapshot_id"]),
            primary_research_start=str(mapping["primary_research_start"]),
            stage_records=tuple(
                StageWorkProgressRecord.from_dict(item)
                for item in _mapping_tuple(mapping["stage_records"], "bundle.stage_records")
            ),
            concepts=tuple(
                ReaderConcept.from_dict(item)
                for item in _mapping_tuple(mapping["concepts"], "bundle.concepts")
            ),
            policy_id=str(mapping["policy_id"]),
            policy_version=str(mapping["policy_version"]),
            policy_sha256=str(mapping["policy_sha256"]),
            content_sha256=str(mapping["content_sha256"]),
        )
        if payload != bundle.canonical_bytes:
            raise StrategyResearchWorkProgressContractError(
                "WORK_PROGRESS_CANONICAL_BYTES_REQUIRED"
            )
        return bundle


def _reachable_concepts(
    *,
    roots: set[str],
    concepts: Mapping[str, ReaderConcept],
) -> set[str]:
    pending = list(roots)
    visited: set[str] = set()
    while pending:
        concept_id = pending.pop()
        if concept_id in visited:
            continue
        visited.add(concept_id)
        concept = concepts.get(concept_id)
        if concept is not None:
            pending.extend(concept.related_concept_ids)
    return visited


def _validate_acyclic_concepts(concepts: Mapping[str, ReaderConcept]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(concept_id: str) -> None:
        if concept_id in visiting:
            raise StrategyResearchWorkProgressContractError(
                f"WORK_PROGRESS_CONCEPT_CYCLE:{concept_id}"
            )
        if concept_id in visited:
            return
        visiting.add(concept_id)
        for related_id in concepts[concept_id].related_concept_ids:
            visit(related_id)
        visiting.remove(concept_id)
        visited.add(concept_id)

    for concept_id in concepts:
        visit(concept_id)


__all__ = [
    "CapabilityProgress",
    "ReaderConcept",
    "ResearchEffect",
    "StageWorkProgressRecord",
    "StrategyResearchProgressMatrix",
    "StrategyResearchWorkProgressBundle",
    "StrategyResearchWorkProgressContractError",
    "build_strategy_research_progress_matrix",
]
