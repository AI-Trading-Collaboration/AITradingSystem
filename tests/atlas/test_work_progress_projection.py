from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas.snapshot_builder import build_atlas_bundle
from ai_trading_system.atlas.status_explanation_projection import (
    load_status_explanation_authority_policy,
    project_status_explanations,
)
from ai_trading_system.atlas.work_progress_projection import (
    WorkProgressProjectionError,
    load_work_progress_authority_policy,
    project_work_progress,
    validate_work_progress_bundle,
)
from ai_trading_system.contracts.strategy_research_work_progress import (
    CapabilityProgress,
    StrategyResearchWorkProgressBundle,
    StrategyResearchWorkProgressContractError,
)

ROOT = Path(__file__).resolve().parents[2]
EXACT_COMMIT = "97421591119f68e46a091d0aca47c2a48aa9d317"


def _inputs():
    atlas = build_atlas_bundle(repository_root=ROOT, exact_commit=EXACT_COMMIT)
    status_policy = load_status_explanation_authority_policy(repository_root=ROOT)
    status_bundle = project_status_explanations(
        snapshot=atlas.snapshot,
        primary_research_start=atlas.primary_research_start,
        policy=status_policy,
    )
    work_policy = load_work_progress_authority_policy(repository_root=ROOT)
    return atlas, status_bundle, work_policy


def test_policy_projection_separates_progress_execution_and_research_effect() -> None:
    atlas, status_bundle, policy = _inputs()

    bundle = project_work_progress(
        snapshot=atlas.snapshot,
        status_explanations=status_bundle,
        policy=policy,
    )
    validation = validate_work_progress_bundle(
        snapshot=atlas.snapshot,
        status_explanations=status_bundle,
        bundle=bundle,
        policy=policy,
    )
    snapshot_stage = next(
        item for item in bundle.stage_records if item.stage_id == "ATLAS_SNAPSHOT_DIFF"
    )

    assert validation.status == "PASS"
    assert snapshot_stage.display_title_zh == "检查页面是否仍代表最新研究状态"
    assert snapshot_stage.capability_progress is CapabilityProgress.AVAILABLE
    assert snapshot_stage.latest_execution_status == "VALIDATED"
    assert "没有产生新的策略证据" in snapshot_stage.research_effect_zh
    assert "旧页面即使还能打开" in snapshot_stage.why_needed_zh
    assert any("变化清单" in item for item in snapshot_stage.expected_outputs_zh)


def test_bundle_is_canonical_and_deterministic() -> None:
    atlas, status_bundle, policy = _inputs()
    first = project_work_progress(
        snapshot=atlas.snapshot,
        status_explanations=status_bundle,
        policy=policy,
    )
    second = project_work_progress(
        snapshot=atlas.snapshot,
        status_explanations=status_bundle,
        policy=policy,
    )

    assert first.canonical_bytes == second.canonical_bytes
    assert StrategyResearchWorkProgressBundle.from_json_bytes(first.canonical_bytes) == first
    pretty = (json.dumps(first.to_dict(), ensure_ascii=False, indent=2) + "\n").encode()
    with pytest.raises(
        StrategyResearchWorkProgressContractError,
        match="CANONICAL_BYTES_REQUIRED",
    ):
        StrategyResearchWorkProgressBundle.from_json_bytes(pretty)


def test_every_stage_concept_resolves_and_graph_is_acyclic() -> None:
    atlas, status_bundle, policy = _inputs()
    bundle = project_work_progress(
        snapshot=atlas.snapshot,
        status_explanations=status_bundle,
        policy=policy,
    )
    concept_ids = {item.concept_id for item in bundle.concepts}

    assert all(set(record.concept_ids) <= concept_ids for record in bundle.stage_records)
    assert "page-snapshot" in concept_ids
    assert "source-relationship" in concept_ids
    assert "citation-chain" in concept_ids


def test_unknown_concept_and_cycle_fail_closed() -> None:
    atlas, status_bundle, policy = _inputs()
    snapshot_stage = policy.stage_records[5]
    unknown_stage = replace(snapshot_stage, concept_ids=("unknown-concept",))
    unknown_policy = replace(
        policy,
        stage_records=(
            *policy.stage_records[:5],
            unknown_stage,
            *policy.stage_records[6:],
        ),
    )
    with pytest.raises(
        StrategyResearchWorkProgressContractError,
        match="CONCEPT_REFERENCE_UNKNOWN",
    ):
        project_work_progress(
            snapshot=atlas.snapshot,
            status_explanations=status_bundle,
            policy=unknown_policy,
        )

    concepts = {item.concept_id: item for item in policy.concepts}
    change_list = replace(
        concepts["change-list"],
        related_concept_ids=("page-snapshot",),
    )
    cycle_policy = replace(
        policy,
        concepts=tuple(
            change_list if item.concept_id == "change-list" else item
            for item in policy.concepts
        ),
    )
    with pytest.raises(
        StrategyResearchWorkProgressContractError,
        match="CONCEPT_CYCLE",
    ):
        project_work_progress(
            snapshot=atlas.snapshot,
            status_explanations=status_bundle,
            policy=cycle_policy,
        )


def test_internal_identifier_in_reader_first_text_fails_closed() -> None:
    _, _, policy = _inputs()

    with pytest.raises(
        StrategyResearchWorkProgressContractError,
        match="READER_FIRST_INTERNAL_TERM",
    ):
        replace(
            policy.stage_records[5],
            why_needed_zh="直接阅读 ATLAS_SNAPSHOT_DIFF 的 validator 输出。",
        )


def test_latest_execution_status_and_source_refs_are_bound() -> None:
    atlas, status_bundle, policy = _inputs()
    bad_status = replace(policy.stage_records[5], latest_execution_status="PASS")
    status_policy = replace(
        policy,
        stage_records=(
            *policy.stage_records[:5],
            bad_status,
            *policy.stage_records[6:],
        ),
    )
    with pytest.raises(WorkProgressProjectionError, match="LATEST_STATUS_DRIFT"):
        project_work_progress(
            snapshot=atlas.snapshot,
            status_explanations=status_bundle,
            policy=status_policy,
        )

    bad_source = replace(policy.stage_records[5], source_ref_ids=("missing-source",))
    source_policy = replace(
        policy,
        stage_records=(
            *policy.stage_records[:5],
            bad_source,
            *policy.stage_records[6:],
        ),
    )
    with pytest.raises(WorkProgressProjectionError, match="SOURCE_REF_MISSING"):
        project_work_progress(
            snapshot=atlas.snapshot,
            status_explanations=status_bundle,
            policy=source_policy,
        )
