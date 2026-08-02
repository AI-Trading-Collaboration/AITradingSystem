from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.status_explanation_projection import (
    StatusExplanationProjectionError,
    build_status_explanation_bundle,
    load_status_explanation_authority_policy,
    project_status_explanations,
    validate_status_explanation_bundle,
)
from ai_trading_system.contracts.status import CanonicalStatus
from ai_trading_system.contracts.strategy_research_explorer import (
    StrategyResearchExplorerSnapshot,
)
from ai_trading_system.contracts.strategy_research_status_explanation import (
    ExplanationFactKind,
    ExplanationValidationSummary,
)

ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "13292726540dc78039a85f17a39f64ddbee956d1"


def _atlas_bundle():
    return build_atlas_bundle(repository_root=ROOT, exact_commit=EXACT_COMMIT)


def _record(bundle, stage_id: str):
    return next(item for item in bundle.explanation_records if item.stage_id == stage_id)


def test_policy_projection_and_validation_bind_exact_snapshot() -> None:
    atlas = _atlas_bundle()
    policy = load_status_explanation_authority_policy(repository_root=ROOT)

    bundle = project_status_explanations(
        snapshot=atlas.snapshot,
        primary_research_start=atlas.primary_research_start,
        policy=policy,
    )
    validation = validate_status_explanation_bundle(
        snapshot=atlas.snapshot,
        bundle=bundle,
        policy=policy,
    )

    assert validation.status == "PASS"
    assert validation.snapshot_id == atlas.snapshot.snapshot_id
    assert bundle.validation_summary is ExplanationValidationSummary.INSUFFICIENT_AUTHORITY
    assert bundle.policy_sha256 == policy.policy_sha256


def test_double_build_is_byte_identical() -> None:
    first = build_status_explanation_bundle(
        repository_root=ROOT,
        exact_commit=EXACT_COMMIT,
    )
    second = build_status_explanation_bundle(
        repository_root=ROOT,
        exact_commit=EXACT_COMMIT,
    )

    assert first.canonical_bytes == second.canonical_bytes
    assert first.content_sha256 == second.content_sha256


def test_research_and_result_explanations_add_independent_facts() -> None:
    bundle = build_status_explanation_bundle(
        repository_root=ROOT,
        exact_commit=EXACT_COMMIT,
    )
    research = _record(bundle, "RESEARCH_MAINLINE")
    backtest = _record(bundle, "BACKTEST_AND_EVALUATION")

    assert research.status_code == "RUNNING"
    assert "具体正在执行的研究子任务" in research.plain_summary
    assert any(
        item.fact_kind is ExplanationFactKind.COMPLETED_MILESTONE and "R0～R2" in item.text_zh
        for item in research.facts
    )
    assert backtest.status_code == "LIMITED"
    assert any(
        item.fact_kind is ExplanationFactKind.EVIDENCE_GAP and "20/60 日" in item.text_zh
        for item in backtest.facts
    )
    assert "display_status 为 LIMITED" not in backtest.plain_summary


def test_free_text_keywords_do_not_create_new_typed_reasons() -> None:
    atlas = _atlas_bundle()
    policy = load_status_explanation_authority_policy(repository_root=ROOT)
    original = project_status_explanations(
        snapshot=atlas.snapshot,
        primary_research_start=atlas.primary_research_start,
        policy=policy,
    )
    noisy_nodes = tuple(
        replace(item, summary=item.summary + " OOS DQ Owner threshold sample")
        for item in atlas.snapshot.nodes
    )
    noisy_results = tuple(
        replace(item, reader_summary=item.reader_summary + " OOS DQ Owner threshold sample")
        for item in atlas.snapshot.results
    )
    noisy_snapshot = StrategyResearchExplorerSnapshot.build(
        title=atlas.snapshot.title,
        generated_at=atlas.snapshot.generated_at,
        sources=atlas.snapshot.sources,
        nodes=noisy_nodes,
        edges=atlas.snapshot.edges,
        results=noisy_results,
        attributions=atlas.snapshot.attributions,
    )

    noisy = project_status_explanations(
        snapshot=noisy_snapshot,
        primary_research_start=atlas.primary_research_start,
        policy=policy,
    )

    assert [item.to_dict() for item in noisy.explanation_records] == [
        item.to_dict() for item in original.explanation_records
    ]


def test_node_status_drift_fails_closed() -> None:
    atlas = _atlas_bundle()
    policy = load_status_explanation_authority_policy(repository_root=ROOT)
    nodes = tuple(
        replace(item, raw_status=CanonicalStatus.PASS)
        if item.node_id == "program-strategy-research"
        else item
        for item in atlas.snapshot.nodes
    )
    snapshot = StrategyResearchExplorerSnapshot.build(
        title=atlas.snapshot.title,
        generated_at=atlas.snapshot.generated_at,
        sources=atlas.snapshot.sources,
        nodes=nodes,
        edges=atlas.snapshot.edges,
        results=atlas.snapshot.results,
        attributions=atlas.snapshot.attributions,
    )

    with pytest.raises(StatusExplanationProjectionError, match="NODE_STATUS_DRIFT"):
        project_status_explanations(
            snapshot=snapshot,
            primary_research_start=atlas.primary_research_start,
            policy=policy,
        )


def test_result_status_drift_fails_closed() -> None:
    atlas = _atlas_bundle()
    policy = load_status_explanation_authority_policy(repository_root=ROOT)
    results = tuple(
        replace(item, display_status=CanonicalStatus.PASS)
        if item.result_id == "result-restart-r2"
        else item
        for item in atlas.snapshot.results
    )
    snapshot = StrategyResearchExplorerSnapshot.build(
        title=atlas.snapshot.title,
        generated_at=atlas.snapshot.generated_at,
        sources=atlas.snapshot.sources,
        nodes=atlas.snapshot.nodes,
        edges=atlas.snapshot.edges,
        results=results,
        attributions=atlas.snapshot.attributions,
    )

    with pytest.raises(StatusExplanationProjectionError, match="RESULT_STATUS_DRIFT"):
        project_status_explanations(
            snapshot=snapshot,
            primary_research_start=atlas.primary_research_start,
            policy=policy,
        )


def test_unknown_source_ref_and_excluded_task_authority_fail_closed() -> None:
    atlas = _atlas_bundle()
    policy = load_status_explanation_authority_policy(repository_root=ROOT)
    record = policy.stage_records[2]
    fact = replace(record.facts[1], source_ref_ids=("not-registered",))
    bad_source_record = replace(record, facts=(record.facts[0], fact, *record.facts[2:]))
    bad_source_policy = replace(
        policy,
        stage_records=(*policy.stage_records[:2], bad_source_record, *policy.stage_records[3:]),
    )

    with pytest.raises(StatusExplanationProjectionError, match="SOURCE_REF_MISSING"):
        project_status_explanations(
            snapshot=atlas.snapshot,
            primary_research_start=atlas.primary_research_start,
            policy=bad_source_policy,
        )

    leaked_fact = replace(record.facts[0], text_zh="TRADING-2481 不得进入解释 lineage。")
    leaked_record = replace(record, facts=(leaked_fact, *record.facts[1:]))
    leaked_policy = replace(
        policy,
        stage_records=(*policy.stage_records[:2], leaked_record, *policy.stage_records[3:]),
    )

    with pytest.raises(StatusExplanationProjectionError, match="EXCLUDED_TASK_AUTHORITY_LEAK"):
        project_status_explanations(
            snapshot=atlas.snapshot,
            primary_research_start=atlas.primary_research_start,
            policy=leaked_policy,
        )


def test_policy_path_must_stay_inside_repository(tmp_path: Path) -> None:
    policy_path = tmp_path / "policy.yaml"
    policy_path.write_text("schema_version: invalid\n", encoding="utf-8")

    with pytest.raises(StatusExplanationProjectionError, match="PATH_OUTSIDE_REPOSITORY"):
        load_status_explanation_authority_policy(
            repository_root=ROOT,
            policy_path=policy_path,
        )
