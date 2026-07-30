from __future__ import annotations

from pathlib import Path

from ai_trading_system.atlas.cited_query import answer_cited_query
from ai_trading_system.atlas.cited_query_validation import (
    cited_query_validation_json_bytes,
    validate_serialized_cited_query_response,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.contracts import (
    CitedQueryAnswerStatus,
    CitedQueryCitation,
    CitedQueryClaim,
    CitedQueryQuestionId,
    StrategyResearchCitedQueryRequest,
    StrategyResearchCitedQueryResponse,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _snapshot_and_response():
    snapshot = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY,
        target_id=snapshot.nodes[0].node_id,
        snapshot_id=snapshot.snapshot_id,
    )
    response = answer_cited_query(
        request,
        snapshot_payload=snapshot.to_dict(),
    )
    return snapshot, response


def test_validator_rebuilds_entity_hash_without_calling_query_engine(
    monkeypatch,
) -> None:
    snapshot, response = _snapshot_and_response()

    def forbidden_query(*args, **kwargs):
        del args, kwargs
        raise AssertionError("independent validator must not call query engine")

    monkeypatch.setattr(
        "ai_trading_system.atlas.cited_query.answer_cited_query",
        forbidden_query,
    )
    result = validate_serialized_cited_query_response(
        response_payload=response.to_dict(),
        snapshot_payload=snapshot.to_dict(),
    )
    assert result.status == "PASS"
    assert result.error_count == 0
    assert cited_query_validation_json_bytes(result).endswith(b"\n")


def test_validator_rejects_validly_reidentified_wrong_entity_hash() -> None:
    snapshot, response = _snapshot_and_response()
    original = response.citations[0]
    wrong = CitedQueryCitation.build(
        target_kind=original.target_kind,
        target_id=original.target_id,
        entity_sha256="0" * 64,
        source_ref_id=original.source_ref_id,
        source_path=original.source_path,
        exact_commit=original.exact_commit,
        source_sha256=original.source_sha256,
        as_of=original.as_of,
        known_at=original.known_at,
        available_at=original.available_at,
        snapshot_id=original.snapshot_id,
    )
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh=response.claims[0].text_zh,
        citation_ids=(wrong.citation_id,),
    )
    reidentified = StrategyResearchCitedQueryResponse.build(
        request=response.request,
        answer_status=CitedQueryAnswerStatus.LIMITED,
        claims=(claim,),
        citations=(wrong,),
        limitations=response.limitations,
        reason_codes=response.reason_codes,
    )
    result = validate_serialized_cited_query_response(
        response_payload=reidentified.to_dict(),
        snapshot_payload=snapshot.to_dict(),
    )
    assert result.status == "FAIL"
    assert "ATLAS_CITED_QUERY_ENTITY_HASH_MISMATCH" in result.errors


def test_validator_rejects_missing_canonical_source_coverage() -> None:
    snapshot = build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit="f" * 40,
    ).snapshot
    result_card = next(
        item for item in snapshot.results if len(item.source_ref_ids) > 1
    )
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.RESULT_AND_STATUS,
        target_id=result_card.result_id,
        snapshot_id=snapshot.snapshot_id,
    )
    response = answer_cited_query(
        request,
        snapshot_payload=snapshot.to_dict(),
    )
    retained = response.citations[:1]
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh=response.claims[0].text_zh,
        citation_ids=(retained[0].citation_id,),
    )
    incomplete = StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=CitedQueryAnswerStatus.LIMITED,
        claims=(claim,),
        citations=retained,
        limitations=response.limitations,
        reason_codes=response.reason_codes,
    )
    validation = validate_serialized_cited_query_response(
        response_payload=incomplete.to_dict(),
        snapshot_payload=snapshot.to_dict(),
    )
    assert validation.status == "FAIL"
    assert "ATLAS_CITED_QUERY_SOURCE_COVERAGE_MISMATCH" in validation.errors
