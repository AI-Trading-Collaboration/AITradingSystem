from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

from ai_trading_system.atlas.cited_query_validation import (
    AtlasCitedQueryInputError,
    load_validated_diff_payloads,
    load_validated_snapshot_payload,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.strategy_research_cited_query import (
    CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE,
    CitedQueryAnswerStatus,
    CitedQueryCitation,
    CitedQueryClaim,
    CitedQueryInputKind,
    CitedQueryTargetKind,
    StrategyResearchCitedQueryRequest,
    StrategyResearchCitedQueryResponse,
)
from ai_trading_system.contracts.strategy_research_explorer import (
    ExplorerSourceRef,
    ResearchAttribution,
    ResearchPathNode,
    ResearchResultCard,
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffChangeKind,
    ExplorerDiffEntityKind,
    ExplorerDiffSignificance,
    ExplorerEntityChange,
    StrategyResearchExplorerDiff,
)

_TARGET_COLLECTIONS = {
    CitedQueryTargetKind.SOURCE: ("sources", "source_ref_id"),
    CitedQueryTargetKind.NODE: ("nodes", "node_id"),
    CitedQueryTargetKind.RESULT: ("results", "result_id"),
    CitedQueryTargetKind.ATTRIBUTION: ("attributions", "attribution_id"),
}
_DIFF_COLLECTIONS = {
    ExplorerDiffEntityKind.SOURCE: ("sources", "source_ref_id"),
    ExplorerDiffEntityKind.NODE: ("nodes", "node_id"),
    ExplorerDiffEntityKind.EDGE: ("edges", "edge_id"),
    ExplorerDiffEntityKind.RESULT: ("results", "result_id"),
    ExplorerDiffEntityKind.ATTRIBUTION: ("attributions", "attribution_id"),
}
_STATUS_ZH = {
    CanonicalStatus.NOT_DUE: "尚未到期",
    CanonicalStatus.DUE: "已到期",
    CanonicalStatus.RUNNING: "进行中",
    CanonicalStatus.PASS: "已通过既定检查",
    CanonicalStatus.LIMITED: "证据有限",
    CanonicalStatus.SKIPPED: "已跳过",
    CanonicalStatus.BLOCKED: "受阻",
    CanonicalStatus.FAILED: "未通过既定检查",
}
_CHANGE_KIND_ZH = {
    ExplorerDiffChangeKind.ADDED: "新增",
    ExplorerDiffChangeKind.REMOVED: "移除",
    ExplorerDiffChangeKind.CHANGED: "修改",
}
_SIGNIFICANCE_ZH = {
    ExplorerDiffSignificance.SEMANTIC: "研究含义发生变化",
    ExplorerDiffSignificance.LINEAGE_ONLY: "仅证据时点或版本发生变化",
    ExplorerDiffSignificance.STRUCTURAL: "研究结构发生变化",
}


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _entity_sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json_bytes(value.to_dict())).hexdigest()


def _blocked_response(
    request: StrategyResearchCitedQueryRequest,
    *,
    limitation: str,
    reason_code: str,
) -> StrategyResearchCitedQueryResponse:
    return StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=CitedQueryAnswerStatus.BLOCKED,
        limitations=(limitation,),
        reason_codes=(reason_code,),
    )


def _find_snapshot_entity(
    snapshot: StrategyResearchExplorerSnapshot,
    request: StrategyResearchCitedQueryRequest,
) -> object | None:
    collection_name, id_field = _TARGET_COLLECTIONS[request.target_kind]
    return next(
        (
            item
            for item in getattr(snapshot, collection_name)
            if str(getattr(item, id_field)) == request.target_id
        ),
        None,
    )


def _snapshot_sources(
    snapshot: StrategyResearchExplorerSnapshot,
    request: StrategyResearchCitedQueryRequest,
    entity: object,
) -> tuple[ExplorerSourceRef, ...]:
    if request.target_kind is CitedQueryTargetKind.SOURCE:
        return (entity,)  # type: ignore[return-value]
    source_map = {item.source_ref_id: item for item in snapshot.sources}
    return tuple(
        source_map[source_ref_id]
        for source_ref_id in tuple(entity.source_ref_ids)  # type: ignore[attr-defined]
    )


def _find_diff_entity(
    snapshot: StrategyResearchExplorerSnapshot,
    change: ExplorerEntityChange,
) -> object | None:
    collection_name, id_field = _DIFF_COLLECTIONS[change.entity_kind]
    return next(
        (
            item
            for item in getattr(snapshot, collection_name)
            if str(getattr(item, id_field)) == change.entity_id
        ),
        None,
    )


def _diff_sources(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    change: ExplorerEntityChange,
    entity: object,
) -> tuple[ExplorerSourceRef, ...]:
    if change.entity_kind is ExplorerDiffEntityKind.EDGE:
        return ()
    if change.entity_kind is ExplorerDiffEntityKind.SOURCE:
        return (entity,)  # type: ignore[return-value]
    source_map = {item.source_ref_id: item for item in snapshot.sources}
    return tuple(
        source_map[source_ref_id]
        for source_ref_id in tuple(entity.source_ref_ids)  # type: ignore[attr-defined]
    )


def _source_key(source: ExplorerSourceRef) -> tuple[object, ...]:
    return (
        source.source_ref_id,
        source.source_path,
        source.exact_commit,
        source.content_sha256,
        source.as_of,
        source.known_at,
        source.available_at,
    )


def _limitations_and_reasons(
    sources: tuple[ExplorerSourceRef, ...],
    entity: object,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    limitations: set[str] = set()
    reasons: set[str] = set()
    if any(
        source.known_at is None or source.available_at is None for source in sources
    ):
        limitations.add(
            "来源未完整记录 known_at/available_at，无法确认信息在当时何时可知。"
        )
        reasons.add(CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE)
    if any(not source.research_context_complete for source in sources):
        limitations.add("来源尚未声明完整研究上下文。")
        reasons.add("SOURCE_RESEARCH_CONTEXT_INCOMPLETE")
    if any(not source.data_quality_ready for source in sources):
        limitations.add("来源尚未声明 data quality ready。")
        reasons.add("SOURCE_DATA_QUALITY_NOT_READY")
    if any(source.legacy_history_partial for source in sources):
        limitations.add("来源包含不完整的 legacy history。")
        reasons.add("LEGACY_HISTORY_PARTIAL")
    source_limitations = {
        source.limitation.strip()
        for source in sources
        if source.limitation.strip()
    }
    if source_limitations:
        limitations.update(source_limitations)
        reasons.add("SOURCE_LIMITATION_PRESENT")
    entity_limitations = {
        str(item).strip()
        for item in tuple(getattr(entity, "limitations", ()))
        if str(item).strip()
    }
    if entity_limitations:
        limitations.update(entity_limitations)
        reasons.add("ENTITY_LIMITATIONS_PRESENT")
    return (
        tuple(sorted(limitations, key=str.casefold)),
        tuple(sorted(reasons, key=str.casefold)),
    )


def _snapshot_claim_text(
    request: StrategyResearchCitedQueryRequest,
    entity: object,
) -> str:
    if request.target_kind is CitedQueryTargetKind.NODE:
        node: ResearchPathNode = entity  # type: ignore[assignment]
        return (
            f"“{node.title}”这条研究主线当前为“{_STATUS_ZH[node.raw_status]}”。"
            f"{node.summary}"
        )
    if request.target_kind is CitedQueryTargetKind.RESULT:
        result: ResearchResultCard = entity  # type: ignore[assignment]
        return (
            f"“{result.title}”的实际记录是：{result.reader_summary}"
            f" 当前展示状态为“{_STATUS_ZH[result.display_status]}”，"
            "这不代表策略已获投资或生产批准。"
        )
    if request.target_kind is CitedQueryTargetKind.ATTRIBUTION:
        attribution: ResearchAttribution = entity  # type: ignore[assignment]
        return (
            f"这条归因对结果“{attribution.result_id}”的方向是"
            f"“{attribution.direction.value}”：{attribution.explanation}"
        )
    source: ExplorerSourceRef = entity  # type: ignore[assignment]
    known_at = (
        "未记录" if source.known_at is None else source.known_at.isoformat()
    )
    available_at = (
        "未记录"
        if source.available_at is None
        else source.available_at.isoformat()
    )
    return (
        f"来源位于 {source.source_path}，绑定 commit {source.exact_commit}，"
        f"as_of={source.as_of.isoformat()}，known_at={known_at}，"
        f"available_at={available_at}。"
    )


def _build_snapshot_citations(
    *,
    request: StrategyResearchCitedQueryRequest,
    entity: object,
    sources: tuple[ExplorerSourceRef, ...],
) -> tuple[CitedQueryCitation, ...]:
    entity_sha256 = _entity_sha256(entity)
    return tuple(
        CitedQueryCitation.build(
            target_kind=request.target_kind,
            target_id=request.target_id,
            entity_sha256=entity_sha256,
            source_ref_id=source.source_ref_id,
            source_path=source.source_path,
            exact_commit=source.exact_commit,
            source_sha256=source.content_sha256,
            as_of=source.as_of,
            known_at=source.known_at,
            available_at=source.available_at,
            snapshot_id=request.snapshot_id,
        )
        for source in sources
    )


def _answer_snapshot(
    request: StrategyResearchCitedQueryRequest,
    snapshot: StrategyResearchExplorerSnapshot,
) -> StrategyResearchCitedQueryResponse:
    if request.snapshot_id != snapshot.snapshot_id:
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_REQUEST_SNAPSHOT_ID_MISMATCH"
        )
    entity = _find_snapshot_entity(snapshot, request)
    if entity is None:
        return _blocked_response(
            request,
            limitation="指定的 stable entity id 不存在，系统不会猜测相近对象。",
            reason_code="TARGET_NOT_FOUND",
        )
    sources = _snapshot_sources(snapshot, request, entity)
    citations = _build_snapshot_citations(
        request=request,
        entity=entity,
        sources=sources,
    )
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh=_snapshot_claim_text(request, entity),
        citation_ids=tuple(item.citation_id for item in citations),
    )
    limitations, reason_codes = _limitations_and_reasons(sources, entity)
    status = (
        CitedQueryAnswerStatus.LIMITED
        if reason_codes
        else CitedQueryAnswerStatus.ANSWERED
    )
    return StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=status,
        claims=(claim,),
        citations=citations,
        limitations=limitations,
        reason_codes=reason_codes,
    )


def _diff_claim_text(change: ExplorerEntityChange) -> str:
    changed_fields = (
        "；变更字段：" + "、".join(change.changed_fields)
        if change.changed_fields
        else ""
    )
    return (
        f"{change.entity_kind.value}“{change.entity_id}”被"
        f"{_CHANGE_KIND_ZH[change.change_kind]}；"
        f"{_SIGNIFICANCE_ZH[change.significance]}{changed_fields}。"
        "这只是研究记录变化，不是策略好坏或投资建议。"
    )


def _build_diff_citations(
    *,
    request: StrategyResearchCitedQueryRequest,
    change: ExplorerEntityChange,
    sources: tuple[ExplorerSourceRef, ...],
) -> tuple[CitedQueryCitation, ...]:
    return tuple(
        CitedQueryCitation.build(
            target_kind=request.target_kind,
            target_id=request.target_id,
            before_entity_sha256=change.before_sha256,
            after_entity_sha256=change.after_sha256,
            source_ref_id=source.source_ref_id,
            source_path=source.source_path,
            exact_commit=source.exact_commit,
            source_sha256=source.content_sha256,
            as_of=source.as_of,
            known_at=source.known_at,
            available_at=source.available_at,
            diff_id=request.diff_id,
        )
        for source in sources
    )


def _answer_diff(
    request: StrategyResearchCitedQueryRequest,
    *,
    before: StrategyResearchExplorerSnapshot,
    after: StrategyResearchExplorerSnapshot,
    diff: StrategyResearchExplorerDiff,
) -> StrategyResearchCitedQueryResponse:
    if (
        request.diff_id != diff.diff_id
        or request.before_snapshot_id != before.snapshot_id
        or request.after_snapshot_id != after.snapshot_id
    ):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_REQUEST_DIFF_IDENTITY_MISMATCH"
        )
    change = next(
        (item for item in diff.changes if item.change_id == request.target_id),
        None,
    )
    if change is None:
        return _blocked_response(
            request,
            limitation="指定的 stable change id 不存在，系统不会推断 rename 或相近变化。",
            reason_code="TARGET_NOT_FOUND",
        )
    sources_by_key: dict[tuple[object, ...], ExplorerSourceRef] = {}
    entities: list[object] = []
    for snapshot, expected_hash in (
        (before, change.before_sha256),
        (after, change.after_sha256),
    ):
        if expected_hash is None:
            continue
        entity = _find_diff_entity(snapshot, change)
        if entity is None:
            raise AtlasCitedQueryInputError(
                "ATLAS_CITED_QUERY_DIFF_ENTITY_MISSING"
            )
        entities.append(entity)
        for source in _diff_sources(
            snapshot=snapshot,
            change=change,
            entity=entity,
        ):
            sources_by_key[_source_key(source)] = source
    if not sources_by_key:
        return _blocked_response(
            request,
            limitation="该变化没有可闭包到 source_ref 的直接证据，系统不生成未引用解释。",
            reason_code="TARGET_SOURCE_LINEAGE_UNAVAILABLE",
        )
    sources = tuple(sources_by_key.values())
    citations = _build_diff_citations(
        request=request,
        change=change,
        sources=sources,
    )
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh=_diff_claim_text(change),
        citation_ids=tuple(item.citation_id for item in citations),
    )
    entity = entities[-1]
    limitations, reason_codes = _limitations_and_reasons(sources, entity)
    status = (
        CitedQueryAnswerStatus.LIMITED
        if reason_codes
        else CitedQueryAnswerStatus.ANSWERED
    )
    return StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=status,
        claims=(claim,),
        citations=citations,
        limitations=limitations,
        reason_codes=reason_codes,
    )


def answer_cited_query(
    request: StrategyResearchCitedQueryRequest,
    *,
    snapshot_payload: Mapping[str, object] | None = None,
    before_payload: Mapping[str, object] | None = None,
    after_payload: Mapping[str, object] | None = None,
    diff_payload: Mapping[str, object] | None = None,
) -> StrategyResearchCitedQueryResponse:
    if request.input_kind is CitedQueryInputKind.SNAPSHOT:
        if (
            snapshot_payload is None
            or before_payload is not None
            or after_payload is not None
            or diff_payload is not None
        ):
            raise AtlasCitedQueryInputError(
                "ATLAS_CITED_QUERY_SNAPSHOT_INPUT_MATRIX_INVALID"
            )
        snapshot = load_validated_snapshot_payload(snapshot_payload)
        return _answer_snapshot(request, snapshot)
    if (
        snapshot_payload is not None
        or before_payload is None
        or after_payload is None
        or diff_payload is None
    ):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_DIFF_INPUT_MATRIX_INVALID"
        )
    before, after, diff = load_validated_diff_payloads(
        before_payload=before_payload,
        after_payload=after_payload,
        diff_payload=diff_payload,
    )
    return _answer_diff(
        request,
        before=before,
        after=after,
        diff=diff,
    )


__all__ = [
    "answer_cited_query",
]
