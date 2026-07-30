from __future__ import annotations

import copy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from ai_trading_system.atlas.cited_query import answer_cited_query
from ai_trading_system.atlas.cited_query_validation import (
    AtlasCitedQueryInputError,
    validate_serialized_cited_query_response,
)
from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.snapshot_diff import build_snapshot_diff
from ai_trading_system.contracts import (
    CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE,
    CitedQueryAnswerStatus,
    CitedQueryQuestionId,
    StrategyResearchCitedQueryRequest,
    StrategyResearchExplorerSnapshot,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "f" * 40


def _snapshot() -> StrategyResearchExplorerSnapshot:
    return build_atlas_bundle(
        repository_root=PROJECT_ROOT,
        exact_commit=EXACT_COMMIT,
    ).snapshot


@pytest.mark.parametrize(
    ("question_id", "collection_name", "id_field"),
    [
        (CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY, "nodes", "node_id"),
        (CitedQueryQuestionId.RESULT_AND_STATUS, "results", "result_id"),
        (
            CitedQueryQuestionId.ATTRIBUTION_AND_LIMITATIONS,
            "attributions",
            "attribution_id",
        ),
        (CitedQueryQuestionId.SOURCE_LINEAGE, "sources", "source_ref_id"),
    ],
)
def test_snapshot_questions_are_cited_limited_and_independently_validated(
    question_id: CitedQueryQuestionId,
    collection_name: str,
    id_field: str,
) -> None:
    snapshot = _snapshot()
    entity = getattr(snapshot, collection_name)[0]
    request = StrategyResearchCitedQueryRequest.build(
        question_id=question_id,
        target_id=str(getattr(entity, id_field)),
        snapshot_id=snapshot.snapshot_id,
    )
    first = answer_cited_query(request, snapshot_payload=snapshot.to_dict())
    second = answer_cited_query(request, snapshot_payload=snapshot.to_dict())

    assert first.canonical_json_bytes() == second.canonical_json_bytes()
    assert first.answer_status is CitedQueryAnswerStatus.LIMITED
    assert (
        CITED_QUERY_SOURCE_TIME_CONTEXT_INCOMPLETE_REASON_CODE
        in first.reason_codes
    )
    assert first.claims
    assert first.citations
    assert all(item.known_at is None for item in first.citations)
    assert all(item.available_at is None for item in first.citations)
    validation = validate_serialized_cited_query_response(
        response_payload=first.to_dict(),
        snapshot_payload=snapshot.to_dict(),
    )
    assert validation.status == "PASS"
    assert validation.errors == ()


def test_complete_source_context_can_answer_without_artificial_limitation() -> None:
    snapshot = _snapshot()
    sources = tuple(
        replace(
            source,
            known_at=source.as_of,
            available_at=source.as_of,
            research_context_complete=True,
            data_quality_ready=True,
            legacy_history_partial=False,
            limitation="",
        )
        for source in snapshot.sources
    )
    complete = StrategyResearchExplorerSnapshot.build(
        title=snapshot.title,
        generated_at=snapshot.generated_at,
        sources=sources,
        nodes=snapshot.nodes,
        edges=snapshot.edges,
        results=snapshot.results,
        attributions=snapshot.attributions,
    )
    node = complete.nodes[0]
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY,
        target_id=node.node_id,
        snapshot_id=complete.snapshot_id,
    )
    response = answer_cited_query(
        request,
        snapshot_payload=complete.to_dict(),
    )
    assert response.answer_status is CitedQueryAnswerStatus.ANSWERED
    assert response.limitations == ()
    assert response.reason_codes == ()


def test_unknown_stable_target_is_blocked_without_guessing() -> None:
    snapshot = _snapshot()
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.RESULT_AND_STATUS,
        target_id="unknown-result",
        snapshot_id=snapshot.snapshot_id,
    )
    response = answer_cited_query(
        request,
        snapshot_payload=snapshot.to_dict(),
    )
    assert response.answer_status is CitedQueryAnswerStatus.BLOCKED
    assert response.reason_codes == ("TARGET_NOT_FOUND",)
    assert response.claims == ()
    assert response.citations == ()
    assert (
        validate_serialized_cited_query_response(
            response_payload=response.to_dict(),
            snapshot_payload=snapshot.to_dict(),
        ).status
        == "PASS"
    )


def test_tampered_snapshot_fails_closed_before_answer_generation() -> None:
    snapshot = _snapshot()
    payload = copy.deepcopy(snapshot.to_dict())
    payload["title"] = "tampered without rebuilding snapshot id"
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.RESEARCH_MAINLINE_SUMMARY,
        target_id=snapshot.nodes[0].node_id,
        snapshot_id=snapshot.snapshot_id,
    )
    with pytest.raises(
        AtlasCitedQueryInputError,
        match="ATLAS_CITED_QUERY_SNAPSHOT_CONTRACT_INVALID",
    ):
        answer_cited_query(request, snapshot_payload=payload)


def _diff_pair() -> tuple[
    StrategyResearchExplorerSnapshot,
    StrategyResearchExplorerSnapshot,
]:
    before = _snapshot()
    after = StrategyResearchExplorerSnapshot.build(
        title=before.title + " next",
        generated_at=before.generated_at + timedelta(days=1),
        sources=before.sources,
        nodes=(
            replace(
                before.nodes[0],
                summary=before.nodes[0].summary + " 已补充 citation-first 入口。",
            ),
            *before.nodes[1:],
        ),
        edges=before.edges,
        results=before.results,
        attributions=before.attributions,
    )
    return before, after


def test_diff_question_binds_change_hashes_and_source_lineage() -> None:
    before, after = _diff_pair()
    diff = build_snapshot_diff(before, after)
    change = diff.changes[0]
    request = StrategyResearchCitedQueryRequest.build(
        question_id=CitedQueryQuestionId.SNAPSHOT_CHANGE_EXPLANATION,
        target_id=change.change_id,
        diff_id=diff.diff_id,
        before_snapshot_id=before.snapshot_id,
        after_snapshot_id=after.snapshot_id,
    )
    response = answer_cited_query(
        request,
        before_payload=before.to_dict(),
        after_payload=after.to_dict(),
        diff_payload=diff.to_dict(),
    )
    assert response.answer_status is CitedQueryAnswerStatus.LIMITED
    assert all(
        citation.before_entity_sha256 == change.before_sha256
        and citation.after_entity_sha256 == change.after_sha256
        for citation in response.citations
    )
    validation = validate_serialized_cited_query_response(
        response_payload=response.to_dict(),
        before_payload=before.to_dict(),
        after_payload=after.to_dict(),
        diff_payload=diff.to_dict(),
    )
    assert validation.status == "PASS"

