from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ReaderStateContractError(ValueError):
    pass


class ReaderStateKind(StrEnum):
    UNKNOWN = "UNKNOWN"
    LIMITED = "LIMITED"
    BLOCKED = "BLOCKED"
    NOT_EVALUATED = "NOT_EVALUATED"
    NOT_STRATEGY_VALID = "NOT_STRATEGY_VALID"
    PASS = "PASS"


class ReaderChangeKind(StrEnum):
    CHANGED = "CHANGED"
    UNCHANGED = "UNCHANGED"
    UNKNOWN = "UNKNOWN"
    NOT_COMPARABLE = "NOT_COMPARABLE"


def canonical_json_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _required(value: str, field: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ReaderStateContractError(f"READER_STATE_REQUIRED:{field}")
    return normalized


def _portable_ref(value: str, field: str) -> str:
    normalized = _required(value.replace("\\", "/"), field)
    if normalized.startswith("/") or ".." in normalized.split("/"):
        raise ReaderStateContractError(f"READER_STATE_SOURCE_REF_INVALID:{field}")
    return normalized


def _temporal(value: str | None, field: str) -> str | None:
    if value is None:
        return None
    normalized = _required(value, field)
    try:
        if "T" in normalized:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("timezone required")
        else:
            date.fromisoformat(normalized)
    except ValueError as exc:
        raise ReaderStateContractError(f"READER_STATE_DATE_INVALID:{field}") from exc
    return normalized


@dataclass(frozen=True)
class ReaderDateContext:
    research_state_as_of: str
    evidence_evaluated_at: str | None
    page_source_commit_at: str

    def __post_init__(self) -> None:
        _temporal(self.research_state_as_of, "date.research_state_as_of")
        _temporal(self.evidence_evaluated_at, "date.evidence_evaluated_at")
        _temporal(self.page_source_commit_at, "date.page_source_commit_at")

    def to_dict(self) -> dict[str, object]:
        return {
            "research_state_as_of": self.research_state_as_of,
            "evidence_evaluated_at": self.evidence_evaluated_at,
            "page_source_commit_at": self.page_source_commit_at,
        }


@dataclass(frozen=True)
class ReaderChangeContext:
    change_kind: ReaderChangeKind
    comparison_base_id: str | None
    comparison_base_date: str | None
    explanation_zh: str

    def __post_init__(self) -> None:
        _required(self.explanation_zh, "change.explanation_zh")
        comparable = self.change_kind in {
            ReaderChangeKind.CHANGED,
            ReaderChangeKind.UNCHANGED,
        }
        if comparable:
            _required(self.comparison_base_id or "", "change.comparison_base_id")
            _temporal(self.comparison_base_date, "change.comparison_base_date")
        elif self.comparison_base_id is not None or self.comparison_base_date is not None:
            raise ReaderStateContractError("READER_STATE_NON_COMPARABLE_BASE_UNEXPECTED")

    def to_dict(self) -> dict[str, object]:
        return {
            "change_kind": self.change_kind.value,
            "comparison_base_id": self.comparison_base_id,
            "comparison_base_date": self.comparison_base_date,
            "explanation_zh": self.explanation_zh,
        }


@dataclass(frozen=True)
class ReaderStateProjection:
    schema_version = "atlas_reader_state_projection.v2"

    status_object_zh: str
    raw_status: str
    reader_state: ReaderStateKind
    reader_label_zh: str
    reason_zh: str
    dates: ReaderDateContext
    next_legal_action_zh: str
    prohibited_inference_zh: str
    change: ReaderChangeContext
    source_refs: tuple[str, ...]
    strategy_validity_supported: bool = False

    def __post_init__(self) -> None:
        for field in (
            "status_object_zh",
            "raw_status",
            "reader_label_zh",
            "reason_zh",
            "next_legal_action_zh",
            "prohibited_inference_zh",
        ):
            _required(str(getattr(self, field)), field)
        if not self.source_refs or len(self.source_refs) != len(set(self.source_refs)):
            raise ReaderStateContractError("READER_STATE_SOURCE_REF_SET_INVALID")
        for ref in self.source_refs:
            _portable_ref(ref, "source_ref")
        if self.strategy_validity_supported:
            raise ReaderStateContractError("READER_STATE_STRATEGY_UPGRADE_FORBIDDEN")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status_object_zh": self.status_object_zh,
            "raw_status": self.raw_status,
            "reader_state": self.reader_state.value,
            "reader_label_zh": self.reader_label_zh,
            "reason_zh": self.reason_zh,
            "dates": self.dates.to_dict(),
            "next_legal_action_zh": self.next_legal_action_zh,
            "prohibited_inference_zh": self.prohibited_inference_zh,
            "change": self.change.to_dict(),
            "source_refs": list(self.source_refs),
            "strategy_validity_supported": self.strategy_validity_supported,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @property
    def content_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


__all__ = [
    "ReaderChangeContext",
    "ReaderChangeKind",
    "ReaderDateContext",
    "ReaderStateContractError",
    "ReaderStateKind",
    "ReaderStateProjection",
    "canonical_json_bytes",
]
