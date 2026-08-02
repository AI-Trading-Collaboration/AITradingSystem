from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.strategy_research_explorer import (
    AssertionKind,
    AttributionDirection,
    ResearchAttribution,
    ResearchEdgeKind,
    ResearchNodeKind,
    ResearchPathEdge,
    ResearchPathNode,
    ResearchResultCard,
    StrategyResearchExplorerSnapshot,
)

from .historical_canonical_projection import (
    apply_historical_canonical_projection,
    build_historical_canonical_projection,
)
from .source_projection import (
    AtlasGlossaryEntry,
    AtlasSourceProjectionError,
    load_source_registry,
    project_source_refs,
)


@dataclass(frozen=True)
class AtlasExplorerBundle:
    registry_id: str
    reader_notice: str
    primary_research_start: str
    glossary: tuple[AtlasGlossaryEntry, ...]
    snapshot: StrategyResearchExplorerSnapshot


def build_atlas_bundle(
    *,
    repository_root: Path,
    exact_commit: str,
    registry_path: Path | None = None,
    include_historical_projection: bool = True,
    historical_projection_path: Path | None = None,
) -> AtlasExplorerBundle:
    root = repository_root.resolve()
    selected_registry_path = (
        registry_path
        if registry_path is not None
        else root / "config" / "atlas" / "source_registry.yaml"
    )
    registry = load_source_registry(selected_registry_path)
    if include_historical_projection:
        projection = build_historical_canonical_projection(
            repository_root=root,
            policy_path=historical_projection_path,
        )
        registry = apply_historical_canonical_projection(registry, projection)
    sources = project_source_refs(
        repository_root=root,
        registry=registry,
        exact_commit=exact_commit,
    )
    snapshot = StrategyResearchExplorerSnapshot.build(
        title=registry.title,
        generated_at=registry.as_of,
        sources=sources,
        nodes=tuple(_node(item) for item in registry.node_payloads),
        edges=tuple(_edge(item) for item in registry.edge_payloads),
        results=tuple(_result(item) for item in registry.result_payloads),
        attributions=tuple(_attribution(item) for item in registry.attribution_payloads),
    )
    return AtlasExplorerBundle(
        registry_id=registry.registry_id,
        reader_notice=registry.reader_notice,
        primary_research_start=registry.primary_research_start.isoformat(),
        glossary=registry.glossary,
        snapshot=snapshot,
    )


def _node(payload: Mapping[str, object]) -> ResearchPathNode:
    return ResearchPathNode(
        node_id=_text(payload, "node_id"),
        node_kind=ResearchNodeKind(_text(payload, "node_kind")),
        title=_text(payload, "title"),
        summary=_text(payload, "summary"),
        assertion_kind=AssertionKind(_text(payload, "assertion_kind")),
        source_ref_ids=_string_tuple(payload, "source_ref_ids"),
        raw_status=CanonicalStatus(_text(payload, "raw_status")),
    )


def _edge(payload: Mapping[str, object]) -> ResearchPathEdge:
    return ResearchPathEdge(
        edge_id=_text(payload, "edge_id"),
        edge_kind=ResearchEdgeKind(_text(payload, "edge_kind")),
        from_node_id=_text(payload, "from_node_id"),
        to_node_id=_text(payload, "to_node_id"),
        label=_text(payload, "label"),
    )


def _result(payload: Mapping[str, object]) -> ResearchResultCard:
    return ResearchResultCard(
        result_id=_text(payload, "result_id"),
        node_id=_text(payload, "node_id"),
        title=_text(payload, "title"),
        raw_status=CanonicalStatus(_text(payload, "raw_status")),
        display_status=CanonicalStatus(_text(payload, "display_status")),
        reader_summary=_text(payload, "reader_summary"),
        assertion_kind=AssertionKind(_text(payload, "assertion_kind")),
        source_ref_ids=_string_tuple(payload, "source_ref_ids"),
        investment_facing=payload.get("investment_facing") is True,
        limitations=_string_tuple(payload, "limitations"),
        source_original_status=(
            None
            if payload.get("source_original_status") is None
            else _text(payload, "source_original_status")
        ),
        status_mapping_rationale=(
            None
            if payload.get("status_mapping_rationale") is None
            else _text(payload, "status_mapping_rationale")
        ),
    )


def _attribution(payload: Mapping[str, object]) -> ResearchAttribution:
    return ResearchAttribution(
        attribution_id=_text(payload, "attribution_id"),
        result_id=_text(payload, "result_id"),
        source_node_id=_text(payload, "source_node_id"),
        direction=AttributionDirection(_text(payload, "direction")),
        explanation=_text(payload, "explanation"),
        assertion_kind=AssertionKind(_text(payload, "assertion_kind")),
        source_ref_ids=_string_tuple(payload, "source_ref_ids"),
    )


def _text(payload: Mapping[str, object], field: str) -> str:
    value = str(payload.get(field, "")).strip()
    if not value:
        raise AtlasSourceProjectionError(f"ATLAS_GRAPH_TEXT_REQUIRED:{field}")
    return value


def _string_tuple(payload: Mapping[str, object], field: str) -> tuple[str, ...]:
    value = payload.get(field)
    if not isinstance(value, list):
        raise AtlasSourceProjectionError(f"ATLAS_GRAPH_LIST_REQUIRED:{field}")
    result = tuple(str(item).strip() for item in value)
    if not result or any(not item for item in result):
        raise AtlasSourceProjectionError(f"ATLAS_GRAPH_LIST_INVALID:{field}")
    return result


__all__ = ["AtlasExplorerBundle", "build_atlas_bundle"]
