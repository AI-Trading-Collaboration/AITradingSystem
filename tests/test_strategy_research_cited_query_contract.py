from __future__ import annotations

import copy
from datetime import UTC, datetime

import pytest

from ai_trading_system.contracts.strategy_research_cited_query import (
    CITED_QUERY_QUESTION_CATALOG,
    CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE,
    CitedQueryAnswerStatus,
    CitedQueryCitation,
    CitedQueryClaim,
    CitedQueryInputKind,
    CitedQueryQuestionId,
    CitedQueryReaderProfile,
    CitedQueryTargetKind,
    StrategyResearchCitedQueryContractError,
    StrategyResearchCitedQueryRequest,
    StrategyResearchCitedQueryResponse,
)

SNAPSHOT_ID = "1" * 64
DIFF_ID = "2" * 64
BEFORE_SNAPSHOT_ID = "3" * 64
AFTER_SNAPSHOT_ID = "4" * 64
ENTITY_SHA = "5" * 64
BEFORE_ENTITY_SHA = "6" * 64
AFTER_ENTITY_SHA = "7" * 64
SOURCE_SHA = "8" * 64
EXACT_COMMIT = "9" * 40
AT = datetime(2026, 7, 30, 0, 0, tzinfo=UTC)


def _request(
    question_id: CitedQueryQuestionId = CitedQueryQuestionId.RESULT_AND_STATUS,
) -> StrategyResearchCitedQueryRequest:
    if question_id is CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION:
        return StrategyResearchCitedQueryRequest.build(
            question_id=question_id,
            target_id="change-1",
            diff_id=DIFF_ID,
            before_snapshot_id=BEFORE_SNAPSHOT_ID,
            after_snapshot_id=AFTER_SNAPSHOT_ID,
        )
    target_id = {
        CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY: "node-1",
        CitedQueryQuestionId.RESULT_AND_STATUS: "result-1",
        CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS: "attribution-1",
        CitedQueryQuestionId.SOURCE_LINEAGE: "source-1",
    }[question_id]
    return StrategyResearchCitedQueryRequest.build(
        question_id=question_id,
        target_id=target_id,
        snapshot_id=SNAPSHOT_ID,
    )


def _citation(
    request: StrategyResearchCitedQueryRequest,
) -> CitedQueryCitation:
    common = {
        "target_kind": request.target_kind,
        "target_id": request.target_id,
        "source_ref_id": "source-1",
        "source_path": "docs/requirements/example.md",
        "exact_commit": EXACT_COMMIT,
        "source_sha256": SOURCE_SHA,
        "as_of": AT,
        "known_at": AT,
        "available_at": AT,
    }
    if request.input_kind is CitedQueryInputKind.DIFF:
        return CitedQueryCitation.build(
            **common,
            before_entity_sha256=BEFORE_ENTITY_SHA,
            after_entity_sha256=AFTER_ENTITY_SHA,
            diff_id=request.diff_id,
        )
    return CitedQueryCitation.build(
        **common,
        entity_sha256=ENTITY_SHA,
        snapshot_id=request.snapshot_id,
    )


def _answered_response(
    request: StrategyResearchCitedQueryRequest | None = None,
) -> StrategyResearchCitedQueryResponse:
    actual_request = request or _request()
    citation = _citation(actual_request)
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh="当前证据记录了研究结果，但不代表策略已获批准。",
        citation_ids=(citation.citation_id,),
    )
    return StrategyResearchCitedQueryResponse.build(
        request=actual_request,
        answer_status=CitedQueryAnswerStatus.ANSWERED,
        claims=(claim,),
        citations=(citation,),
    )


def test_question_catalog_is_complete_stable_and_reader_facing() -> None:
    assert tuple(item.question_id for item in CITED_QUERY_QUESTION_CATALOG) == tuple(
        CitedQueryQuestionId
    )
    assert {item.reader_prompt_zh for item in CITED_QUERY_QUESTION_CATALOG}
    assert len({item.reader_prompt_zh for item in CITED_QUERY_QUESTION_CATALOG}) == 5
    assert all("？" in item.reader_prompt_zh for item in CITED_QUERY_QUESTION_CATALOG)


@pytest.mark.parametrize("question_id", list(CitedQueryQuestionId))
def test_request_round_trip_and_identity_are_deterministic(
    question_id: CitedQueryQuestionId,
) -> None:
    request = _request(question_id)
    rebuilt = StrategyResearchCitedQueryRequest.from_dict(request.to_dict())
    assert rebuilt == request
    assert rebuilt.request_id == rebuilt.compute_request_id()
    assert rebuilt.reader_profile is CitedQueryReaderProfile.LOW_FINANCE_KNOWLEDGE


def test_question_target_and_input_matrix_fails_closed() -> None:
    payload = _request().to_dict()
    payload["target_kind"] = CitedQueryTargetKind.SOURCE.value
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_QUESTION_TARGET_UNSUPPORTED",
    ):
        StrategyResearchCitedQueryRequest.from_dict(payload)

    payload = _request().to_dict()
    payload["diff_id"] = DIFF_ID
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_SNAPSHOT_IDENTITY_INVALID",
    ):
        StrategyResearchCitedQueryRequest.from_dict(payload)


def test_response_round_trip_is_byte_deterministic_and_citation_closed() -> None:
    response = _answered_response()
    rebuilt = StrategyResearchCitedQueryResponse.from_dict(response.to_dict())
    assert rebuilt == response
    assert rebuilt.response_id == rebuilt.compute_response_id()
    assert rebuilt.canonical_json_bytes() == response.canonical_json_bytes()
    assert len(rebuilt.claims) == 1
    assert len(rebuilt.citations) == 1


def test_tampered_request_claim_citation_and_response_ids_fail_closed() -> None:
    response = _answered_response()
    payload = copy.deepcopy(response.to_dict())
    payload["request"]["target_id"] = "result-tampered"
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_REQUEST_ID_MISMATCH",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)

    payload = copy.deepcopy(response.to_dict())
    payload["claims"][0]["text_zh"] = "被篡改的回答"
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CLAIM_ID_MISMATCH",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)

    payload = copy.deepcopy(response.to_dict())
    payload["citations"][0]["source_sha256"] = "a" * 64
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CITATION_ID_MISMATCH",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)

    payload = copy.deepcopy(response.to_dict())
    payload["answer_status"] = CitedQueryAnswerStatus.LIMITED.value
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_ANSWER_STATUS_INVALID",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)


def test_answered_limited_and_blocked_status_contracts_are_explicit() -> None:
    request = _request()
    citation = _citation(request)
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh="已有证据只覆盖部分归因。",
        citation_ids=(citation.citation_id,),
    )
    limited = StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=CitedQueryAnswerStatus.LIMITED,
        claims=(claim,),
        citations=(citation,),
        limitations=("缺少完整研究上下文。",),
        reason_codes=("EVIDENCE_COVERAGE_INCOMPLETE",),
    )
    assert limited.answer_status is CitedQueryAnswerStatus.LIMITED

    blocked = StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=CitedQueryAnswerStatus.BLOCKED,
        limitations=("引用闭包无法验证。",),
        reason_codes=("CITATION_CLOSURE_INVALID",),
    )
    assert blocked.claims == ()
    assert blocked.citations == ()


def test_missing_or_orphan_citation_fails_closed() -> None:
    response = _answered_response()
    payload = copy.deepcopy(response.to_dict())
    payload["citations"] = []
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CITATION_CLOSURE_INVALID",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)

    request = _request()
    first = _citation(request)
    second = CitedQueryCitation.build(
        target_kind=request.target_kind,
        target_id=request.target_id,
        entity_sha256="a" * 64,
        source_ref_id="source-2",
        source_path="docs/requirements/second.md",
        exact_commit="b" * 40,
        source_sha256="c" * 64,
        as_of=AT,
        known_at=AT,
        available_at=AT,
        snapshot_id=request.snapshot_id,
    )
    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh="只引用第一条证据。",
        citation_ids=(first.citation_id,),
    )
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CITATION_CLOSURE_INVALID",
    ):
        StrategyResearchCitedQueryResponse.build(
            request=request,
            answer_status=CitedQueryAnswerStatus.ANSWERED,
            claims=(claim,),
            citations=(first, second),
        )


def test_citation_path_time_and_input_identity_are_strict() -> None:
    request = _request()
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_SOURCE_PATH_INVALID",
    ):
        CitedQueryCitation.build(
            target_kind=request.target_kind,
            target_id=request.target_id,
            entity_sha256=ENTITY_SHA,
            source_ref_id="source-1",
            source_path="../outside.md",
            exact_commit=EXACT_COMMIT,
            source_sha256=SOURCE_SHA,
            as_of=AT,
            known_at=AT,
            available_at=AT,
            snapshot_id=request.snapshot_id,
        )

    payload = _citation(request).to_dict()
    payload["known_at"] = "2026-07-29T00:00:00+00:00"
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CITATION_TIME_ORDER_INVALID",
    ):
        CitedQueryCitation.from_dict(payload)

    payload = _citation(request).to_dict()
    payload["known_at"] = None
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_CITATION_TIME_ORDER_INVALID",
    ):
        CitedQueryCitation.from_dict(payload)


def test_missing_source_times_are_preserved_and_require_limited_status() -> None:
    request = _request()
    citation = CitedQueryCitation.build(
        target_kind=request.target_kind,
        target_id=request.target_id,
        entity_sha256=ENTITY_SHA,
        source_ref_id="source-1",
        source_path="docs/requirements/example.md",
        exact_commit=EXACT_COMMIT,
        source_sha256=SOURCE_SHA,
        as_of=AT,
        snapshot_id=request.snapshot_id,
    )
    rebuilt = CitedQueryCitation.from_dict(citation.to_dict())
    assert rebuilt.known_at is None
    assert rebuilt.available_at is None
    assert rebuilt.to_dict()["known_at"] is None
    assert rebuilt.to_dict()["available_at"] is None

    claim = CitedQueryClaim.build(
        ordinal=1,
        text_zh="来源内容可验证，但可知时间上下文不完整。",
        citation_ids=(citation.citation_id,),
    )
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_SOURCE_TIME_CONTEXT_STATUS_INVALID",
    ):
        StrategyResearchCitedQueryResponse.build(
            request=request,
            answer_status=CitedQueryAnswerStatus.ANSWERED,
            claims=(claim,),
            citations=(citation,),
        )
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_SOURCE_TIME_CONTEXT_STATUS_INVALID",
    ):
        StrategyResearchCitedQueryResponse.build(
            request=request,
            answer_status=CitedQueryAnswerStatus.LIMITED,
            claims=(claim,),
            citations=(citation,),
            limitations=("来源可知时间上下文不完整。",),
            reason_codes=("EVIDENCE_COVERAGE_INCOMPLETE",),
        )

    limited = StrategyResearchCitedQueryResponse.build(
        request=request,
        answer_status=CitedQueryAnswerStatus.LIMITED,
        claims=(claim,),
        citations=(citation,),
        limitations=("来源可知时间上下文不完整。",),
        reason_codes=(CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE,),
    )
    assert StrategyResearchCitedQueryResponse.from_dict(limited.to_dict()) == limited


def test_read_only_boundary_fails_closed() -> None:
    payload = copy.deepcopy(_answered_response().to_dict())
    payload["investment_advice_generated"] = True
    with pytest.raises(
        StrategyResearchCitedQueryContractError,
        match="STRATEGY_CITED_QUERY_READ_ONLY_BOUNDARY_VIOLATION",
    ):
        StrategyResearchCitedQueryResponse.from_dict(payload)
