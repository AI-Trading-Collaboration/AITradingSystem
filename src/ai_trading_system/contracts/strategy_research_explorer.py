from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import ClassVar

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.core.production_effect import ProductionEffect

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class StrategyResearchExplorerContractError(ValueError):
    pass


class AssertionKind(StrEnum):
    DATA_FACT = "DATA_FACT"
    RULE_JUDGMENT = "RULE_JUDGMENT"
    MODEL_RESULT = "MODEL_RESULT"
    RESEARCHER_INTERPRETATION = "RESEARCHER_INTERPRETATION"
    OWNER_DECISION = "OWNER_DECISION"


class ExplorerSourceKind(StrEnum):
    GIT_AUTHORITY = "GIT_AUTHORITY"
    PUBLISHED_ARTIFACT = "PUBLISHED_ARTIFACT"
    UNVERIFIED_CONTEXT = "UNVERIFIED_CONTEXT"


class ResearchNodeKind(StrEnum):
    PROGRAM = "PROGRAM"
    CAMPAIGN = "CAMPAIGN"
    HYPOTHESIS = "HYPOTHESIS"
    STAGE = "STAGE"
    EXPERIMENT = "EXPERIMENT"
    EVIDENCE = "EVIDENCE"
    RESULT = "RESULT"
    DECISION = "DECISION"
    STOP = "STOP"
    BLOCKER = "BLOCKER"
    ARTIFACT = "ARTIFACT"


class ResearchEdgeKind(StrEnum):
    CONTAINS = "CONTAINS"
    TESTS = "TESTS"
    PRODUCED = "PRODUCED"
    SUPPORTS = "SUPPORTS"
    CONTRADICTS = "CONTRADICTS"
    BLOCKS = "BLOCKS"
    SUPERSEDES = "SUPERSEDES"
    DECIDED_BY = "DECIDED_BY"
    NEXT_STEP = "NEXT_STEP"


class AttributionDirection(StrEnum):
    SUPPORTS = "SUPPORTS"
    CONTRADICTS = "CONTRADICTS"
    MIXED = "MIXED"
    NEUTRAL = "NEUTRAL"
    UNKNOWN = "UNKNOWN"


def _required_text(value: str, field: str) -> None:
    if not value.strip():
        raise StrategyResearchExplorerContractError(f"STRATEGY_EXPLORER_REQUIRED_FIELD:{field}")


def _aware_datetime(value: datetime, field: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise StrategyResearchExplorerContractError(f"STRATEGY_EXPLORER_TIMEZONE_REQUIRED:{field}")


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise StrategyResearchExplorerContractError(f"STRATEGY_EXPLORER_MAPPING_REQUIRED:{field}")
    return value


def _mapping_list(value: object, field: str) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise StrategyResearchExplorerContractError(f"STRATEGY_EXPLORER_LIST_REQUIRED:{field}")
    return tuple(_mapping(item, field) for item in value)


def _string_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise StrategyResearchExplorerContractError(f"STRATEGY_EXPLORER_LIST_REQUIRED:{field}")
    return tuple(str(item) for item in value)


def _parse_datetime(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise StrategyResearchExplorerContractError(
            f"STRATEGY_EXPLORER_DATETIME_INVALID:{field}"
        ) from exc
    _aware_datetime(parsed, field)
    return parsed


@dataclass(frozen=True)
class ExplorerSourceRef:
    schema_version: ClassVar[str] = "strategy_research_explorer_source_ref.v1"

    source_ref_id: str
    source_kind: ExplorerSourceKind
    exact_commit: str
    source_path: str
    content_sha256: str
    artifact_identity: str
    as_of: datetime
    requested_start: date | None = None
    requested_end: date | None = None
    evaluated_start: date | None = None
    evaluated_end: date | None = None
    known_at: datetime | None = None
    available_at: datetime | None = None
    research_context_complete: bool = False
    data_quality_ready: bool = False
    legacy_history_partial: bool = False
    limitation: str = ""

    def __post_init__(self) -> None:
        for value, field in (
            (self.source_ref_id, "source_ref_id"),
            (self.source_path, "source_path"),
            (self.artifact_identity, "artifact_identity"),
        ):
            _required_text(value, field)
        if not _GIT_COMMIT_PATTERN.fullmatch(self.exact_commit):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_EXACT_COMMIT_INVALID")
        if not _SHA256_PATTERN.fullmatch(self.content_sha256):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_CONTENT_SHA256_INVALID")
        _aware_datetime(self.as_of, "as_of")
        for value, field in (
            (self.known_at, "known_at"),
            (self.available_at, "available_at"),
        ):
            if value is not None:
                _aware_datetime(value, field)
        if (self.requested_start is None) != (self.requested_end is None):
            raise StrategyResearchExplorerContractError(
                "STRATEGY_EXPLORER_REQUESTED_RANGE_INCOMPLETE"
            )
        if (self.evaluated_start is None) != (self.evaluated_end is None):
            raise StrategyResearchExplorerContractError(
                "STRATEGY_EXPLORER_EVALUATED_RANGE_INCOMPLETE"
            )
        if (
            self.requested_start is not None
            and self.requested_end is not None
            and self.requested_start > self.requested_end
        ):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_REQUESTED_RANGE_INVALID")
        if (
            self.evaluated_start is not None
            and self.evaluated_end is not None
            and self.evaluated_start > self.evaluated_end
        ):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_EVALUATED_RANGE_INVALID")
        if self.legacy_history_partial and not self.limitation.strip():
            raise StrategyResearchExplorerContractError(
                "STRATEGY_EXPLORER_LEGACY_LIMITATION_REQUIRED"
            )

    @property
    def investment_claim_ready(self) -> bool:
        return (
            self.source_kind is not ExplorerSourceKind.UNVERIFIED_CONTEXT
            and self.research_context_complete
            and self.data_quality_ready
            and not self.legacy_history_partial
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "source_ref_id": self.source_ref_id,
            "source_kind": self.source_kind.value,
            "exact_commit": self.exact_commit,
            "source_path": self.source_path,
            "content_sha256": self.content_sha256,
            "artifact_identity": self.artifact_identity,
            "as_of": self.as_of.isoformat(),
            "requested_start": (
                None if self.requested_start is None else self.requested_start.isoformat()
            ),
            "requested_end": (
                None if self.requested_end is None else self.requested_end.isoformat()
            ),
            "evaluated_start": (
                None if self.evaluated_start is None else self.evaluated_start.isoformat()
            ),
            "evaluated_end": (
                None if self.evaluated_end is None else self.evaluated_end.isoformat()
            ),
            "known_at": None if self.known_at is None else self.known_at.isoformat(),
            "available_at": (None if self.available_at is None else self.available_at.isoformat()),
            "research_context_complete": self.research_context_complete,
            "data_quality_ready": self.data_quality_ready,
            "legacy_history_partial": self.legacy_history_partial,
            "limitation": self.limitation,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ExplorerSourceRef:
        return cls(
            source_ref_id=str(payload.get("source_ref_id", "")),
            source_kind=ExplorerSourceKind(str(payload.get("source_kind", ""))),
            exact_commit=str(payload.get("exact_commit", "")),
            source_path=str(payload.get("source_path", "")),
            content_sha256=str(payload.get("content_sha256", "")),
            artifact_identity=str(payload.get("artifact_identity", "")),
            as_of=_parse_datetime(payload.get("as_of"), "as_of"),
            requested_start=_optional_date(payload.get("requested_start")),
            requested_end=_optional_date(payload.get("requested_end")),
            evaluated_start=_optional_date(payload.get("evaluated_start")),
            evaluated_end=_optional_date(payload.get("evaluated_end")),
            known_at=_optional_datetime(payload.get("known_at"), "known_at"),
            available_at=_optional_datetime(payload.get("available_at"), "available_at"),
            research_context_complete=(payload.get("research_context_complete") is True),
            data_quality_ready=payload.get("data_quality_ready") is True,
            legacy_history_partial=payload.get("legacy_history_partial") is True,
            limitation=str(payload.get("limitation", "")),
        )


@dataclass(frozen=True)
class ResearchPathNode:
    schema_version: ClassVar[str] = "strategy_research_path_node.v1"

    node_id: str
    node_kind: ResearchNodeKind
    title: str
    summary: str
    assertion_kind: AssertionKind
    source_ref_ids: tuple[str, ...]
    raw_status: CanonicalStatus

    def __post_init__(self) -> None:
        for value, field in (
            (self.node_id, "node_id"),
            (self.title, "node.title"),
            (self.summary, "node.summary"),
        ):
            _required_text(value, field)
        _require_source_ids(self.source_ref_ids, f"node:{self.node_id}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "node_id": self.node_id,
            "node_kind": self.node_kind.value,
            "title": self.title,
            "summary": self.summary,
            "assertion_kind": self.assertion_kind.value,
            "source_ref_ids": list(self.source_ref_ids),
            "raw_status": self.raw_status.value,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchPathNode:
        return cls(
            node_id=str(payload.get("node_id", "")),
            node_kind=ResearchNodeKind(str(payload.get("node_kind", ""))),
            title=str(payload.get("title", "")),
            summary=str(payload.get("summary", "")),
            assertion_kind=AssertionKind(str(payload.get("assertion_kind", ""))),
            source_ref_ids=_string_tuple(payload.get("source_ref_ids"), "node.source_ref_ids"),
            raw_status=CanonicalStatus(str(payload.get("raw_status", ""))),
        )


@dataclass(frozen=True)
class ResearchPathEdge:
    schema_version: ClassVar[str] = "strategy_research_path_edge.v1"

    edge_id: str
    edge_kind: ResearchEdgeKind
    from_node_id: str
    to_node_id: str
    label: str

    def __post_init__(self) -> None:
        for value, field in (
            (self.edge_id, "edge_id"),
            (self.from_node_id, "edge.from_node_id"),
            (self.to_node_id, "edge.to_node_id"),
            (self.label, "edge.label"),
        ):
            _required_text(value, field)
        if self.from_node_id == self.to_node_id:
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_SELF_EDGE_FORBIDDEN")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "edge_id": self.edge_id,
            "edge_kind": self.edge_kind.value,
            "from_node_id": self.from_node_id,
            "to_node_id": self.to_node_id,
            "label": self.label,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchPathEdge:
        return cls(
            edge_id=str(payload.get("edge_id", "")),
            edge_kind=ResearchEdgeKind(str(payload.get("edge_kind", ""))),
            from_node_id=str(payload.get("from_node_id", "")),
            to_node_id=str(payload.get("to_node_id", "")),
            label=str(payload.get("label", "")),
        )


@dataclass(frozen=True)
class ResearchResultCard:
    schema_version: ClassVar[str] = "strategy_research_result_card.v1"

    result_id: str
    node_id: str
    title: str
    raw_status: CanonicalStatus
    display_status: CanonicalStatus
    reader_summary: str
    assertion_kind: AssertionKind
    source_ref_ids: tuple[str, ...]
    investment_facing: bool = False
    limitations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value, field in (
            (self.result_id, "result_id"),
            (self.node_id, "result.node_id"),
            (self.title, "result.title"),
            (self.reader_summary, "result.reader_summary"),
        ):
            _required_text(value, field)
        _require_source_ids(self.source_ref_ids, f"result:{self.result_id}")
        if any(not item.strip() for item in self.limitations):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_EMPTY_LIMITATION")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "result_id": self.result_id,
            "node_id": self.node_id,
            "title": self.title,
            "raw_status": self.raw_status.value,
            "display_status": self.display_status.value,
            "reader_summary": self.reader_summary,
            "assertion_kind": self.assertion_kind.value,
            "source_ref_ids": list(self.source_ref_ids),
            "investment_facing": self.investment_facing,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchResultCard:
        return cls(
            result_id=str(payload.get("result_id", "")),
            node_id=str(payload.get("node_id", "")),
            title=str(payload.get("title", "")),
            raw_status=CanonicalStatus(str(payload.get("raw_status", ""))),
            display_status=CanonicalStatus(str(payload.get("display_status", ""))),
            reader_summary=str(payload.get("reader_summary", "")),
            assertion_kind=AssertionKind(str(payload.get("assertion_kind", ""))),
            source_ref_ids=_string_tuple(payload.get("source_ref_ids"), "result.source_ref_ids"),
            investment_facing=payload.get("investment_facing") is True,
            limitations=_string_tuple(payload.get("limitations"), "result.limitations"),
        )


@dataclass(frozen=True)
class ResearchAttribution:
    schema_version: ClassVar[str] = "strategy_research_attribution.v1"

    attribution_id: str
    result_id: str
    source_node_id: str
    direction: AttributionDirection
    explanation: str
    assertion_kind: AssertionKind
    source_ref_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        for value, field in (
            (self.attribution_id, "attribution_id"),
            (self.result_id, "attribution.result_id"),
            (self.source_node_id, "attribution.source_node_id"),
            (self.explanation, "attribution.explanation"),
        ):
            _required_text(value, field)
        _require_source_ids(self.source_ref_ids, f"attribution:{self.attribution_id}")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "attribution_id": self.attribution_id,
            "result_id": self.result_id,
            "source_node_id": self.source_node_id,
            "direction": self.direction.value,
            "explanation": self.explanation,
            "assertion_kind": self.assertion_kind.value,
            "source_ref_ids": list(self.source_ref_ids),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> ResearchAttribution:
        return cls(
            attribution_id=str(payload.get("attribution_id", "")),
            result_id=str(payload.get("result_id", "")),
            source_node_id=str(payload.get("source_node_id", "")),
            direction=AttributionDirection(str(payload.get("direction", ""))),
            explanation=str(payload.get("explanation", "")),
            assertion_kind=AssertionKind(str(payload.get("assertion_kind", ""))),
            source_ref_ids=_string_tuple(
                payload.get("source_ref_ids"), "attribution.source_ref_ids"
            ),
        )


@dataclass(frozen=True)
class StrategyResearchExplorerSnapshot:
    schema_version: ClassVar[str] = "strategy_research_explorer_snapshot.v1"

    snapshot_id: str
    title: str
    generated_at: datetime
    sources: tuple[ExplorerSourceRef, ...]
    nodes: tuple[ResearchPathNode, ...]
    edges: tuple[ResearchPathEdge, ...]
    results: tuple[ResearchResultCard, ...]
    attributions: tuple[ResearchAttribution, ...]
    manual_review_only: bool = True
    commands_executed: bool = False
    source_state_mutated: bool = False
    production_effect: ProductionEffect = ProductionEffect.NONE
    broker_action: str = "none"

    def __post_init__(self) -> None:
        _required_text(self.snapshot_id, "snapshot_id")
        _required_text(self.title, "snapshot.title")
        _aware_datetime(self.generated_at, "generated_at")
        if not _SHA256_PATTERN.fullmatch(self.snapshot_id):
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_SNAPSHOT_ID_INVALID")
        if not self.sources or not self.nodes:
            raise StrategyResearchExplorerContractError(
                "STRATEGY_EXPLORER_SOURCE_AND_NODE_REQUIRED"
            )
        if (
            not self.manual_review_only
            or self.commands_executed
            or self.source_state_mutated
            or self.production_effect is not ProductionEffect.NONE
            or self.broker_action != "none"
        ):
            raise StrategyResearchExplorerContractError(
                "STRATEGY_EXPLORER_READ_ONLY_BOUNDARY_VIOLATION"
            )
        self._validate_graph_and_lineage()
        if self.snapshot_id != self.compute_snapshot_id():
            raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_SNAPSHOT_ID_MISMATCH")

    def _validate_graph_and_lineage(self) -> None:
        source_map = _unique_by(self.sources, lambda item: item.source_ref_id, "source_ref_id")
        node_map = _unique_by(self.nodes, lambda item: item.node_id, "node_id")
        edge_map = _unique_by(self.edges, lambda item: item.edge_id, "edge_id")
        result_map = _unique_by(self.results, lambda item: item.result_id, "result_id")
        _unique_by(
            self.attributions,
            lambda item: item.attribution_id,
            "attribution_id",
        )
        del edge_map
        for edge in self.edges:
            if edge.from_node_id not in node_map or edge.to_node_id not in node_map:
                raise StrategyResearchExplorerContractError(
                    f"STRATEGY_EXPLORER_EDGE_ENDPOINT_MISSING:{edge.edge_id}"
                )
        for item in (*self.nodes, *self.results, *self.attributions):
            for source_ref_id in item.source_ref_ids:
                if source_ref_id not in source_map:
                    raise StrategyResearchExplorerContractError(
                        f"STRATEGY_EXPLORER_SOURCE_REF_MISSING:{source_ref_id}"
                    )
        for result in self.results:
            if result.node_id not in node_map:
                raise StrategyResearchExplorerContractError(
                    f"STRATEGY_EXPLORER_RESULT_NODE_MISSING:{result.result_id}"
                )
            bound_sources = tuple(source_map[item] for item in result.source_ref_ids)
            ready = all(item.investment_claim_ready for item in bound_sources)
            if result.investment_facing and result.display_status is CanonicalStatus.PASS:
                if result.raw_status is not CanonicalStatus.PASS or not ready:
                    raise StrategyResearchExplorerContractError(
                        f"STRATEGY_EXPLORER_PREMATURE_INVESTMENT_PASS:{result.result_id}"
                    )
            if (
                result.investment_facing
                and result.raw_status is CanonicalStatus.PASS
                and not ready
                and result.display_status not in (CanonicalStatus.LIMITED, CanonicalStatus.BLOCKED)
            ):
                raise StrategyResearchExplorerContractError(
                    f"STRATEGY_EXPLORER_INCOMPLETE_PASS_NOT_DOWNGRADED:{result.result_id}"
                )
        for attribution in self.attributions:
            if attribution.result_id not in result_map:
                raise StrategyResearchExplorerContractError(
                    f"STRATEGY_EXPLORER_ATTRIBUTION_RESULT_MISSING:{attribution.attribution_id}"
                )
            if attribution.source_node_id not in node_map:
                raise StrategyResearchExplorerContractError(
                    f"STRATEGY_EXPLORER_ATTRIBUTION_NODE_MISSING:{attribution.attribution_id}"
                )

    @classmethod
    def build(
        cls,
        *,
        title: str,
        generated_at: datetime,
        sources: Sequence[ExplorerSourceRef],
        nodes: Sequence[ResearchPathNode],
        edges: Sequence[ResearchPathEdge],
        results: Sequence[ResearchResultCard],
        attributions: Sequence[ResearchAttribution],
    ) -> StrategyResearchExplorerSnapshot:
        provisional = object.__new__(cls)
        object.__setattr__(provisional, "snapshot_id", "0" * 64)
        object.__setattr__(provisional, "title", title)
        object.__setattr__(provisional, "generated_at", generated_at)
        object.__setattr__(provisional, "sources", tuple(sources))
        object.__setattr__(provisional, "nodes", tuple(nodes))
        object.__setattr__(provisional, "edges", tuple(edges))
        object.__setattr__(provisional, "results", tuple(results))
        object.__setattr__(provisional, "attributions", tuple(attributions))
        object.__setattr__(provisional, "manual_review_only", True)
        object.__setattr__(provisional, "commands_executed", False)
        object.__setattr__(provisional, "source_state_mutated", False)
        object.__setattr__(provisional, "production_effect", ProductionEffect.NONE)
        object.__setattr__(provisional, "broker_action", "none")
        snapshot_id = provisional.compute_snapshot_id()
        return cls(
            snapshot_id=snapshot_id,
            title=title,
            generated_at=generated_at,
            sources=tuple(sources),
            nodes=tuple(nodes),
            edges=tuple(edges),
            results=tuple(results),
            attributions=tuple(attributions),
        )

    def _payload_without_snapshot_id(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "title": self.title,
            "generated_at": self.generated_at.isoformat(),
            "sources": [
                item.to_dict() for item in sorted(self.sources, key=lambda item: item.source_ref_id)
            ],
            "nodes": [item.to_dict() for item in sorted(self.nodes, key=lambda item: item.node_id)],
            "edges": [item.to_dict() for item in sorted(self.edges, key=lambda item: item.edge_id)],
            "results": [
                item.to_dict() for item in sorted(self.results, key=lambda item: item.result_id)
            ],
            "attributions": [
                item.to_dict()
                for item in sorted(self.attributions, key=lambda item: item.attribution_id)
            ],
            "manual_review_only": self.manual_review_only,
            "commands_executed": self.commands_executed,
            "source_state_mutated": self.source_state_mutated,
            "production_effect": self.production_effect.value,
            "broker_action": self.broker_action,
        }

    def compute_snapshot_id(self) -> str:
        return hashlib.sha256(
            json.dumps(
                self._payload_without_snapshot_id(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "snapshot_id": self.snapshot_id,
            **self._payload_without_snapshot_id(),
        }

    def canonical_json_bytes(self) -> bytes:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> StrategyResearchExplorerSnapshot:
        return cls(
            snapshot_id=str(payload.get("snapshot_id", "")),
            title=str(payload.get("title", "")),
            generated_at=_parse_datetime(payload.get("generated_at"), "generated_at"),
            sources=tuple(
                ExplorerSourceRef.from_dict(item)
                for item in _mapping_list(payload.get("sources"), "sources")
            ),
            nodes=tuple(
                ResearchPathNode.from_dict(item)
                for item in _mapping_list(payload.get("nodes"), "nodes")
            ),
            edges=tuple(
                ResearchPathEdge.from_dict(item)
                for item in _mapping_list(payload.get("edges"), "edges")
            ),
            results=tuple(
                ResearchResultCard.from_dict(item)
                for item in _mapping_list(payload.get("results"), "results")
            ),
            attributions=tuple(
                ResearchAttribution.from_dict(item)
                for item in _mapping_list(payload.get("attributions"), "attributions")
            ),
            manual_review_only=payload.get("manual_review_only") is True,
            commands_executed=payload.get("commands_executed") is True,
            source_state_mutated=payload.get("source_state_mutated") is True,
            production_effect=ProductionEffect.parse(str(payload.get("production_effect", ""))),
            broker_action=str(payload.get("broker_action", "")),
        )


def _require_source_ids(values: tuple[str, ...], owner: str) -> None:
    if not values or any(not item.strip() for item in values):
        raise StrategyResearchExplorerContractError(
            f"STRATEGY_EXPLORER_SOURCE_REF_REQUIRED:{owner}"
        )
    if len(set(values)) != len(values):
        raise StrategyResearchExplorerContractError(
            f"STRATEGY_EXPLORER_DUPLICATE_SOURCE_REF:{owner}"
        )


def _unique_by(
    values: Sequence[object],
    key,
    field: str,
) -> dict[str, object]:
    result: dict[str, object] = {}
    for item in values:
        identity = str(key(item))
        if identity in result:
            raise StrategyResearchExplorerContractError(
                f"STRATEGY_EXPLORER_DUPLICATE_ID:{field}:{identity}"
            )
        result[identity] = item
    return result


def _optional_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise StrategyResearchExplorerContractError("STRATEGY_EXPLORER_DATE_INVALID") from exc


def _optional_datetime(value: object, field: str) -> datetime | None:
    if value is None:
        return None
    return _parse_datetime(value, field)


__all__ = [
    "AssertionKind",
    "AttributionDirection",
    "ExplorerSourceKind",
    "ExplorerSourceRef",
    "ResearchAttribution",
    "ResearchEdgeKind",
    "ResearchNodeKind",
    "ResearchPathEdge",
    "ResearchPathNode",
    "ResearchResultCard",
    "StrategyResearchExplorerContractError",
    "StrategyResearchExplorerSnapshot",
]
