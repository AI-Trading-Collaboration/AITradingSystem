from __future__ import annotations

import copy
from datetime import UTC, date, datetime

import pytest

from ai_trading_system.contracts import (
    AssertionKind,
    AttributionDirection,
    CanonicalStatus,
    ExplorerSourceKind,
    ExplorerSourceRef,
    ResearchAttribution,
    ResearchEdgeKind,
    ResearchNodeKind,
    ResearchPathEdge,
    ResearchPathNode,
    ResearchResultCard,
    StrategyResearchExplorerContractError,
    StrategyResearchExplorerSnapshot,
)

COMMIT = "1" * 40
SHA256 = "2" * 64
GENERATED_AT = datetime(2026, 7, 30, 10, 0, tzinfo=UTC)


def _source(
    source_ref_id: str = "source-1",
    *,
    source_kind: ExplorerSourceKind = ExplorerSourceKind.PUBLISHED_ARTIFACT,
    ready: bool = True,
    legacy_history_partial: bool = False,
) -> ExplorerSourceRef:
    return ExplorerSourceRef(
        source_ref_id=source_ref_id,
        source_kind=source_kind,
        exact_commit=COMMIT,
        source_path=f"reports/{source_ref_id}.json",
        content_sha256=SHA256,
        artifact_identity=f"report:{source_ref_id}",
        as_of=GENERATED_AT,
        requested_start=date(2021, 2, 22),
        requested_end=date(2026, 7, 30),
        evaluated_start=date(2021, 2, 22),
        evaluated_end=date(2026, 7, 29),
        known_at=GENERATED_AT,
        available_at=GENERATED_AT,
        research_context_complete=ready,
        data_quality_ready=ready,
        legacy_history_partial=legacy_history_partial,
        limitation="历史时间线不完整" if legacy_history_partial else "",
    )


def _node(node_id: str, source_ref_id: str = "source-1") -> ResearchPathNode:
    return ResearchPathNode(
        node_id=node_id,
        node_kind=ResearchNodeKind.RESULT,
        title=f"节点 {node_id}",
        summary="只展示已经发布的研究事实。",
        assertion_kind=AssertionKind.DATA_FACT,
        source_ref_ids=(source_ref_id,),
        raw_status=CanonicalStatus.PASS,
    )


def _result(
    *,
    display_status: CanonicalStatus = CanonicalStatus.PASS,
    source_ref_id: str = "source-1",
) -> ResearchResultCard:
    return ResearchResultCard(
        result_id="result-1",
        node_id="node-2",
        title="研究结果",
        raw_status=CanonicalStatus.PASS,
        display_status=display_status,
        reader_summary="原始结果与面向读者的展示状态分别保存。",
        assertion_kind=AssertionKind.MODEL_RESULT,
        source_ref_ids=(source_ref_id,),
        investment_facing=True,
        limitations=(),
    )


def _snapshot(
    *,
    source: ExplorerSourceRef | None = None,
    result: ResearchResultCard | None = None,
    edges: tuple[ResearchPathEdge, ...] | None = None,
) -> StrategyResearchExplorerSnapshot:
    actual_source = source or _source()
    actual_edges = edges or (
        ResearchPathEdge(
            edge_id="edge-1",
            edge_kind=ResearchEdgeKind.PRODUCED,
            from_node_id="node-1",
            to_node_id="node-2",
            label="产生",
        ),
    )
    actual_result = result or _result(source_ref_id=actual_source.source_ref_id)
    return StrategyResearchExplorerSnapshot.build(
        title="Atlas Strategy Research Explorer",
        generated_at=GENERATED_AT,
        sources=(actual_source,),
        nodes=(
            _node("node-1", actual_source.source_ref_id),
            _node("node-2", actual_source.source_ref_id),
        ),
        edges=actual_edges,
        results=(actual_result,),
        attributions=(
            ResearchAttribution(
                attribution_id="attribution-1",
                result_id="result-1",
                source_node_id="node-1",
                direction=AttributionDirection.SUPPORTS,
                explanation="节点一提供了支持该结果的已发布 evidence。",
                assertion_kind=AssertionKind.RESEARCHER_INTERPRETATION,
                source_ref_ids=(actual_source.source_ref_id,),
            ),
        ),
    )


def test_snapshot_round_trip_is_byte_deterministic() -> None:
    snapshot = _snapshot()
    rebuilt = StrategyResearchExplorerSnapshot.from_dict(snapshot.to_dict())
    assert rebuilt == snapshot
    assert rebuilt.canonical_json_bytes() == snapshot.canonical_json_bytes()
    assert rebuilt.snapshot_id == snapshot.compute_snapshot_id()


def test_all_serialized_objects_expose_schema_version() -> None:
    payload = _snapshot().to_dict()
    assert payload["schema_version"] == "strategy_research_explorer_snapshot.v1"
    for collection in ("sources", "nodes", "edges", "results", "attributions"):
        assert all("schema_version" in item for item in payload[collection])


def test_snapshot_rejects_tampered_identity() -> None:
    payload = copy.deepcopy(_snapshot().to_dict())
    payload["title"] = "被篡改"
    with pytest.raises(
        StrategyResearchExplorerContractError,
        match="STRATEGY_EXPLORER_SNAPSHOT_ID_MISMATCH",
    ):
        StrategyResearchExplorerSnapshot.from_dict(payload)


def test_snapshot_rejects_missing_graph_endpoint() -> None:
    bad_edge = ResearchPathEdge(
        edge_id="edge-bad",
        edge_kind=ResearchEdgeKind.BLOCKS,
        from_node_id="node-1",
        to_node_id="missing-node",
        label="阻断",
    )
    with pytest.raises(
        StrategyResearchExplorerContractError,
        match="STRATEGY_EXPLORER_EDGE_ENDPOINT_MISSING",
    ):
        _snapshot(edges=(bad_edge,))


@pytest.mark.parametrize(
    "source",
    [
        _source(source_kind=ExplorerSourceKind.UNVERIFIED_CONTEXT),
        _source(ready=False),
        _source(ready=False, legacy_history_partial=True),
    ],
)
def test_investment_facing_pass_requires_complete_verified_lineage(
    source: ExplorerSourceRef,
) -> None:
    with pytest.raises(
        StrategyResearchExplorerContractError,
        match="STRATEGY_EXPLORER_PREMATURE_INVESTMENT_PASS",
    ):
        _snapshot(source=source, result=_result(source_ref_id=source.source_ref_id))


def test_incomplete_raw_pass_may_only_display_limited_or_blocked() -> None:
    source = _source(ready=False, legacy_history_partial=True)
    limited = _snapshot(
        source=source,
        result=_result(
            display_status=CanonicalStatus.LIMITED,
            source_ref_id=source.source_ref_id,
        ),
    )
    assert limited.results[0].raw_status is CanonicalStatus.PASS
    assert limited.results[0].display_status is CanonicalStatus.LIMITED


def test_read_only_boundary_fails_closed() -> None:
    payload = _snapshot().to_dict()
    payload["commands_executed"] = True
    payload_without_id = copy.deepcopy(payload)
    payload_without_id["snapshot_id"] = "0" * 64
    provisional = object.__new__(StrategyResearchExplorerSnapshot)
    for key, value in StrategyResearchExplorerSnapshot.from_dict(
        {**payload, "commands_executed": False}
    ).__dict__.items():
        object.__setattr__(provisional, key, value)
    object.__setattr__(provisional, "commands_executed", True)
    payload["snapshot_id"] = provisional.compute_snapshot_id()
    with pytest.raises(
        StrategyResearchExplorerContractError,
        match="STRATEGY_EXPLORER_READ_ONLY_BOUNDARY_VIOLATION",
    ):
        StrategyResearchExplorerSnapshot.from_dict(payload)
