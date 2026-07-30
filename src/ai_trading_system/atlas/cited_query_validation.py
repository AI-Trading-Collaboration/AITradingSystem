from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass

from ai_trading_system.atlas.diff_validation import (
    validate_serialized_snapshot_diff,
)
from ai_trading_system.contracts.strategy_research_cited_query import (
    CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE,
    CitedQueryAnswerStatus,
    CitedQueryCitation,
    CitedQueryInputKind,
    CitedQueryTargetKind,
    StrategyResearchCitedQueryResponse,
)
from ai_trading_system.contracts.strategy_research_explorer import (
    ExplorerSourceKind,
    ExplorerSourceRef,
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_explorer_diff import (
    ExplorerDiffEntityKind,
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


class AtlasCitedQueryInputError(ValueError):
    def __init__(self, code: str, details: tuple[str, ...] = ()) -> None:
        self.code = code
        self.details = details
        suffix = "" if not details else ":" + ",".join(details)
        super().__init__(code + suffix)


@dataclass(frozen=True)
class CitedQueryValidationResult:
    schema_version: str
    status: str
    request_id: str
    response_id: str
    claim_count: int
    citation_count: int
    error_count: int
    errors: tuple[str, ...]
    production_effect: str = "none"
    broker_action: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "request_id": self.request_id,
            "response_id": self.response_id,
            "claim_count": self.claim_count,
            "citation_count": self.citation_count,
            "error_count": self.error_count,
            "errors": list(self.errors),
            "production_effect": self.production_effect,
            "broker_action": self.broker_action,
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
    payload = value.to_dict()
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def load_validated_snapshot_payload(
    payload: Mapping[str, object],
) -> StrategyResearchExplorerSnapshot:
    try:
        snapshot = StrategyResearchExplorerSnapshot.from_dict(payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_SNAPSHOT_CONTRACT_INVALID",
            (type(exc).__name__,),
        ) from exc
    if snapshot.canonical_json_bytes() != _canonical_json_bytes(payload):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_SNAPSHOT_CANONICAL_ROUND_TRIP_MISMATCH"
        )
    if any(
        source.source_kind is ExplorerSourceKind.UNVERIFIED_CONTEXT
        for source in snapshot.sources
    ):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_UNVERIFIED_CONTEXT_SOURCE_FORBIDDEN"
        )
    if any(result.investment_facing for result in snapshot.results):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_INVESTMENT_FACING_INPUT_FORBIDDEN"
        )
    return snapshot


def load_validated_diff_payloads(
    *,
    before_payload: Mapping[str, object],
    after_payload: Mapping[str, object],
    diff_payload: Mapping[str, object],
) -> tuple[
    StrategyResearchExplorerSnapshot,
    StrategyResearchExplorerSnapshot,
    StrategyResearchExplorerDiff,
]:
    result = validate_serialized_snapshot_diff(
        before_payload=before_payload,
        after_payload=after_payload,
        diff_payload=diff_payload,
    )
    if result.status != "PASS":
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_DIFF_VALIDATION_FAILED",
            result.errors,
        )
    before = load_validated_snapshot_payload(before_payload)
    after = load_validated_snapshot_payload(after_payload)
    try:
        diff = StrategyResearchExplorerDiff.from_dict(diff_payload)
    except (KeyError, TypeError, ValueError) as exc:
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_DIFF_CONTRACT_INVALID",
            (type(exc).__name__,),
        ) from exc
    if diff.canonical_json_bytes() != _canonical_json_bytes(diff_payload):
        raise AtlasCitedQueryInputError(
            "ATLAS_CITED_QUERY_DIFF_CANONICAL_ROUND_TRIP_MISMATCH"
        )
    return before, after, diff


def _find_snapshot_entity(
    snapshot: StrategyResearchExplorerSnapshot,
    target_kind: CitedQueryTargetKind,
    target_id: str,
) -> object | None:
    collection_name, id_field = _TARGET_COLLECTIONS[target_kind]
    return next(
        (
            item
            for item in getattr(snapshot, collection_name)
            if str(getattr(item, id_field)) == target_id
        ),
        None,
    )


def _source_refs_for_snapshot_entity(
    *,
    snapshot: StrategyResearchExplorerSnapshot,
    target_kind: CitedQueryTargetKind,
    entity: object,
) -> tuple[ExplorerSourceRef, ...]:
    source_map = {item.source_ref_id: item for item in snapshot.sources}
    if target_kind is CitedQueryTargetKind.SOURCE:
        return (entity,)  # type: ignore[return-value]
    source_ref_ids = tuple(entity.source_ref_ids)  # type: ignore[attr-defined]
    return tuple(source_map[source_ref_id] for source_ref_id in source_ref_ids)


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


def _source_refs_for_diff_entity(
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


def _citation_source_key(citation: CitedQueryCitation) -> tuple[object, ...]:
    return (
        citation.source_ref_id,
        citation.source_path,
        citation.exact_commit,
        citation.source_sha256,
        citation.as_of,
        citation.known_at,
        citation.available_at,
    )


def _required_limitation_reasons(
    sources: tuple[ExplorerSourceRef, ...],
    entity: object,
) -> frozenset[str]:
    reasons: set[str] = set()
    if any(
        source.known_at is None or source.available_at is None for source in sources
    ):
        reasons.add(CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE)
    if any(not source.research_context_complete for source in sources):
        reasons.add("SOURCE_RESEARCH_CONTEXT_INCOMPLETE")
    if any(not source.data_quality_ready for source in sources):
        reasons.add("SOURCE_DATA_QUALITY_NOT_READY")
    if any(source.legacy_history_partial for source in sources):
        reasons.add("LEGACY_HISTORY_PARTIAL")
    if any(source.limitation.strip() for source in sources):
        reasons.add("SOURCE_LIMITATION_PRESENT")
    if tuple(getattr(entity, "limitations", ())):
        reasons.add("ENTITY_LIMITATIONS_PRESENT")
    return frozenset(reasons)


def _validate_status_and_sources(
    *,
    response: StrategyResearchCitedQueryResponse,
    entity: object,
    sources: tuple[ExplorerSourceRef, ...],
    expected_entity_sha256: str | None,
    expected_before_sha256: str | None,
    expected_after_sha256: str | None,
    errors: list[str],
) -> None:
    expected_keys = {_source_key(source) for source in sources}
    observed_keys = {_citation_source_key(item) for item in response.citations}
    if expected_keys != observed_keys:
        errors.append("ATLAS_CITED_QUERY_SOURCE_COVERAGE_MISMATCH")
    for citation in response.citations:
        if citation.entity_sha256 != expected_entity_sha256:
            errors.append("ATLAS_CITED_QUERY_ENTITY_HASH_MISMATCH")
        if citation.before_entity_sha256 != expected_before_sha256:
            errors.append("ATLAS_CITED_QUERY_BEFORE_ENTITY_HASH_MISMATCH")
        if citation.after_entity_sha256 != expected_after_sha256:
            errors.append("ATLAS_CITED_QUERY_AFTER_ENTITY_HASH_MISMATCH")
    required_reasons = _required_limitation_reasons(sources, entity)
    if required_reasons:
        if response.answer_status is not CitedQueryAnswerStatus.LIMITED:
            errors.append("ATLAS_CITED_QUERY_REQUIRED_LIMITED_STATUS_MISSING")
        if not required_reasons <= set(response.reason_codes):
            errors.append("ATLAS_CITED_QUERY_REQUIRED_LIMITATION_REASON_MISSING")


def _validate_snapshot_response(
    *,
    response: StrategyResearchCitedQueryResponse,
    snapshot: StrategyResearchExplorerSnapshot,
    errors: list[str],
) -> None:
    request = response.request
    if request.snapshot_id != snapshot.snapshot_id:
        errors.append("ATLAS_CITED_QUERY_SNAPSHOT_ID_MISMATCH")
        return
    entity = _find_snapshot_entity(
        snapshot,
        request.target_kind,
        request.target_id,
    )
    if entity is None:
        if (
            response.answer_status is not CitedQueryAnswerStatus.BLOCKED
            or "TARGET_NOT_FOUND" not in response.reason_codes
        ):
            errors.append("ATLAS_CITED_QUERY_UNKNOWN_TARGET_NOT_BLOCKED")
        return
    if response.answer_status is CitedQueryAnswerStatus.BLOCKED:
        errors.append("ATLAS_CITED_QUERY_KNOWN_TARGET_UNEXPECTEDLY_BLOCKED")
        return
    sources = _source_refs_for_snapshot_entity(
        snapshot=snapshot,
        target_kind=request.target_kind,
        entity=entity,
    )
    _validate_status_and_sources(
        response=response,
        entity=entity,
        sources=sources,
        expected_entity_sha256=_entity_sha256(entity),
        expected_before_sha256=None,
        expected_after_sha256=None,
        errors=errors,
    )


def _validate_diff_response(
    *,
    response: StrategyResearchCitedQueryResponse,
    before: StrategyResearchExplorerSnapshot,
    after: StrategyResearchExplorerSnapshot,
    diff: StrategyResearchExplorerDiff,
    errors: list[str],
) -> None:
    request = response.request
    if (
        request.diff_id != diff.diff_id
        or request.before_snapshot_id != before.snapshot_id
        or request.after_snapshot_id != after.snapshot_id
    ):
        errors.append("ATLAS_CITED_QUERY_DIFF_IDENTITY_MISMATCH")
        return
    change = next(
        (item for item in diff.changes if item.change_id == request.target_id),
        None,
    )
    if change is None:
        if (
            response.answer_status is not CitedQueryAnswerStatus.BLOCKED
            or "TARGET_NOT_FOUND" not in response.reason_codes
        ):
            errors.append("ATLAS_CITED_QUERY_UNKNOWN_TARGET_NOT_BLOCKED")
        return
    side_entities = tuple(
        (snapshot, _find_diff_entity(snapshot, change))
        for snapshot, expected_hash in (
            (before, change.before_sha256),
            (after, change.after_sha256),
        )
        if expected_hash is not None
    )
    sources_by_key: dict[tuple[object, ...], ExplorerSourceRef] = {}
    entities: list[object] = []
    for snapshot, entity in side_entities:
        if entity is None:
            errors.append("ATLAS_CITED_QUERY_DIFF_ENTITY_MISSING")
            continue
        entities.append(entity)
        for source in _source_refs_for_diff_entity(
            snapshot=snapshot,
            change=change,
            entity=entity,
        ):
            sources_by_key[_source_key(source)] = source
    if not sources_by_key:
        if (
            response.answer_status is not CitedQueryAnswerStatus.BLOCKED
            or "TARGET_SOURCE_LINEAGE_UNAVAILABLE" not in response.reason_codes
        ):
            errors.append("ATLAS_CITED_QUERY_UNCITEABLE_CHANGE_NOT_BLOCKED")
        return
    if response.answer_status is CitedQueryAnswerStatus.BLOCKED:
        errors.append("ATLAS_CITED_QUERY_CITEABLE_CHANGE_UNEXPECTEDLY_BLOCKED")
        return
    entity = entities[-1]
    _validate_status_and_sources(
        response=response,
        entity=entity,
        sources=tuple(sources_by_key.values()),
        expected_entity_sha256=None,
        expected_before_sha256=change.before_sha256,
        expected_after_sha256=change.after_sha256,
        errors=errors,
    )


def validate_serialized_cited_query_response(
    *,
    response_payload: Mapping[str, object],
    snapshot_payload: Mapping[str, object] | None = None,
    before_payload: Mapping[str, object] | None = None,
    after_payload: Mapping[str, object] | None = None,
    diff_payload: Mapping[str, object] | None = None,
) -> CitedQueryValidationResult:
    errors: list[str] = []
    request_id = ""
    response_id = str(response_payload.get("response_id", ""))
    claim_count = 0
    citation_count = 0
    try:
        response = StrategyResearchCitedQueryResponse.from_dict(response_payload)
        request_id = response.request.request_id
        claim_count = len(response.claims)
        citation_count = len(response.citations)
        if response.canonical_json_bytes() != _canonical_json_bytes(response_payload):
            errors.append("ATLAS_CITED_QUERY_RESPONSE_CANONICAL_ROUND_TRIP_MISMATCH")
        if response.request.input_kind is CitedQueryInputKind.SNAPSHOT:
            if (
                snapshot_payload is None
                or before_payload is not None
                or after_payload is not None
                or diff_payload is not None
            ):
                errors.append("ATLAS_CITED_QUERY_SNAPSHOT_INPUT_MATRIX_INVALID")
            else:
                snapshot = load_validated_snapshot_payload(snapshot_payload)
                _validate_snapshot_response(
                    response=response,
                    snapshot=snapshot,
                    errors=errors,
                )
        elif (
            snapshot_payload is not None
            or before_payload is None
            or after_payload is None
            or diff_payload is None
        ):
            errors.append("ATLAS_CITED_QUERY_DIFF_INPUT_MATRIX_INVALID")
        else:
            before, after, diff = load_validated_diff_payloads(
                before_payload=before_payload,
                after_payload=after_payload,
                diff_payload=diff_payload,
            )
            _validate_diff_response(
                response=response,
                before=before,
                after=after,
                diff=diff,
                errors=errors,
            )
    except (AtlasCitedQueryInputError, KeyError, TypeError, ValueError) as exc:
        errors.append(f"ATLAS_CITED_QUERY_VALIDATION_FAILED:{type(exc).__name__}")
    errors_tuple = tuple(sorted(set(errors)))
    return CitedQueryValidationResult(
        schema_version="atlas_cited_query_validation.v1",
        status="PASS" if not errors_tuple else "FAIL",
        request_id=request_id,
        response_id=response_id,
        claim_count=claim_count,
        citation_count=citation_count,
        error_count=len(errors_tuple),
        errors=errors_tuple,
    )


def cited_query_validation_json_bytes(
    result: CitedQueryValidationResult,
) -> bytes:
    return _canonical_json_bytes(result.to_dict())


__all__ = [
    "AtlasCitedQueryInputError",
    "CitedQueryValidationResult",
    "cited_query_validation_json_bytes",
    "load_validated_diff_payloads",
    "load_validated_snapshot_payload",
    "validate_serialized_cited_query_response",
]
