from __future__ import annotations

import hashlib
from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas.page_effectiveness import (
    PageEffectivenessError,
    build_page_effectiveness_manifest,
    load_page_effectiveness_policy,
    validate_page_effectiveness_manifest,
)
from ai_trading_system.contracts.strategy_research_page_effectiveness import (
    PageAcceptanceRecord,
    PageAcceptanceStatus,
    PageAcceptanceTrack,
    PageArtifactIdentity,
    PageEffectivenessContractError,
    PageFreshnessStatus,
    StrategyResearchPageEffectivenessManifest,
)

ROOT = Path(__file__).resolve().parents[2]
PAGE_LOCATOR = "outputs/atlas/strategy_research_cited_query/trading_2470_v1/index.html"


def _rendered(
    payload: bytes = b"<!doctype html><title>Atlas</title>\n",
) -> tuple[tuple[PageArtifactIdentity, ...], dict[str, bytes]]:
    identity = PageArtifactIdentity(
        role="ATLAS_PAGE_INDEX_HTML",
        locator=PAGE_LOCATOR,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_count=len(payload),
    )
    return (identity,), {"index.html": payload}


def test_policy_freezes_reader_questions_and_thirty_four_task_sources() -> None:
    policy = load_page_effectiveness_policy(repository_root=ROOT)
    assert policy.primary_research_start == "2021-02-22"
    assert len(policy.task_sources) == 34
    assert [item.task_id.split("_", 1)[0] for item in policy.task_sources] == [
        f"TRADING-{number}" for number in (*range(2481, 2505), *range(2506, 2516))
    ]
    assert policy.reader_questions == (
        "CURRENT_RESEARCH_MAINLINE",
        "LARGEST_CURRENT_BLOCKER",
        "ENGINEERING_VS_RESEARCH_EVIDENCE",
        "PROHIBITED_INFERENCES",
        "NEXT_OWNER_AND_ACTION",
        "INVESTMENT_ORDER_ENGINE_AUTHORITY",
    )
    assert policy.safety["investment_conclusion_generated"] is False
    assert policy.safety["order_authorized"] is False
    assert policy.safety["real_engine_authorized"] is False


def test_manifest_binds_current_sources_tasks_and_independent_reviews() -> None:
    rendered, _ = _rendered()
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
    )
    assert manifest.freshness_status is PageFreshnessStatus.CURRENT
    assert (
        manifest.freshness_status is not PageFreshnessStatus.UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED
    )
    assert len(manifest.task_coverage) == 34
    assert [item.task_id.split("_", 1)[0] for item in manifest.task_coverage] == [
        f"TRADING-{number}" for number in (*range(2481, 2505), *range(2506, 2516))
    ]
    assert manifest.task_coverage[-7].coverage == "DISCLOSED_VERSIONED_SUCCESSOR_CONTRACT_ONLY"
    assert (
        manifest.task_coverage[-6].coverage
        == "DISCLOSED_PRIMARY_WINDOW_CALIBRATION_EVIDENCE_NOT_PROVIDED"
    )
    assert (
        manifest.task_coverage[-5].coverage == "DISCLOSED_DERIVED_EVIDENCE_GENERATOR_SOURCE_BLOCKED"
    )
    assert (
        manifest.task_coverage[-4].coverage
        == "DISCLOSED_EXPORT_SAFE_COLLECTOR_CONTRACT_RUN_NOT_AUTHORIZED"
    )
    assert (
        manifest.task_coverage[-3].coverage
        == "DISCLOSED_EXACT_RUN_PROPOSAL_OWNER_AUTHORIZATION_REQUIRED"
    )
    assert (
        manifest.task_coverage[-2].coverage
        == "DISCLOSED_EVIDENCE_ADMISSION_CONTRACT_OWNER_TOKEN_NOT_PROVIDED"
    )
    assert (
        manifest.task_coverage[-1].coverage
        == "DISCLOSED_STRATEGY_RESEARCH_REOPEN_READINESS_KEEP_CLOSED"
    )
    assert len(manifest.source_artifacts) == 15
    assert [item.track for item in manifest.acceptance] == list(PageAcceptanceTrack)
    assert [item.status for item in manifest.acceptance] == [
        PageAcceptanceStatus.NOT_EXECUTED,
        PageAcceptanceStatus.PENDING_REVIEW,
        PageAcceptanceStatus.PENDING_REVIEW,
    ]
    replay = StrategyResearchPageEffectivenessManifest.from_json_bytes(manifest.canonical_bytes)
    assert replay == manifest
    assert replay.content_sha256 == manifest.content_sha256


def test_engineering_pass_requires_evidence_and_cannot_sign_human_review() -> None:
    with pytest.raises(
        PageEffectivenessContractError,
        match="PAGE_EFFECTIVENESS_PASS_EVIDENCE_REQUIRED",
    ):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.ENGINEERING_VALIDATION,
            status=PageAcceptanceStatus.PASS,
            evidence_refs=(),
        )
    with pytest.raises(
        PageEffectivenessContractError,
        match="ENGINEERING_CANNOT_IMPERSONATE_HUMAN_REVIEW",
    ):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.ENGINEERING_VALIDATION,
            status=PageAcceptanceStatus.NOT_EXECUTED,
            evidence_refs=(),
            reviewer_id="project-owner",
        )
    with pytest.raises(PageEffectivenessContractError, match="PASS_EVIDENCE_REQUIRED"):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.OWNER_VISUAL_REVIEW,
            status=PageAcceptanceStatus.PASS,
            evidence_refs=(),
            reviewer_id="project-owner",
            reviewed_at="2026-08-10T00:00:00+09:00",
            decision_id="owner-decision",
        )


def test_explicit_reader_review_survives_manifest_build_and_validation() -> None:
    rendered, payloads = _rendered()
    reader_review = PageAcceptanceRecord(
        track=PageAcceptanceTrack.READER_COMPREHENSION_REVIEW,
        status=PageAcceptanceStatus.PASS,
        evidence_refs=(
            "docs/requirements/TRADING-2506_Atlas_Work_Progress_Recursive_Explanation_V1.md",
        ),
        reviewer_id="project-owner",
        reviewed_at="2026-08-10T10:17:40Z",
        decision_id="trading-2506-reader-comprehension-pass-20260810-v1",
    )
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
        reader_comprehension_review=reader_review,
    )
    assert manifest.acceptance[1].status is PageAcceptanceStatus.PENDING_REVIEW
    assert manifest.acceptance[2] == reader_review

    validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=manifest,
        rendered_payloads=payloads,
    )
    assert validation.status == "PASS"
    assert "HUMAN_REVIEW_EXPLICITLY_ATTESTED" in validation.checks


def test_human_review_track_mismatch_fails_closed() -> None:
    wrong_track = PageAcceptanceRecord(
        track=PageAcceptanceTrack.OWNER_VISUAL_REVIEW,
        status=PageAcceptanceStatus.PENDING_REVIEW,
        evidence_refs=(),
    )
    with pytest.raises(PageEffectivenessError, match="HUMAN_REVIEW_TRACK_INVALID"):
        build_page_effectiveness_manifest(
            repository_root=ROOT,
            reader_comprehension_review=wrong_track,
        )


def test_manifest_rejects_noncanonical_and_path_escape() -> None:
    manifest = build_page_effectiveness_manifest(repository_root=ROOT)
    with pytest.raises(PageEffectivenessContractError, match="NON_CANONICAL_BYTES"):
        StrategyResearchPageEffectivenessManifest.from_json_bytes(
            manifest.canonical_bytes.replace(b'"page_id":', b'"page_id" :', 1)
        )
    with pytest.raises(PageEffectivenessContractError, match="PATH_INVALID"):
        PageArtifactIdentity(
            role="BROWSER_EVIDENCE",
            locator="../outside.png",
            sha256="a" * 64,
            byte_count=1,
        )


def test_validation_passes_exact_payload_and_marks_source_drift_stale() -> None:
    rendered, payloads = _rendered()
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
    )
    validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=manifest,
        rendered_payloads=payloads,
    )
    assert validation.status == "PASS"
    assert validation.freshness_status is PageFreshnessStatus.CURRENT
    assert "TWENTY_FOUR_TASKS_COVERED" in validation.checks
    assert "THREE_ACCEPTANCE_TRACKS_INDEPENDENT" in validation.checks

    wrong_sources = (
        replace(manifest.source_artifacts[0], sha256="f" * 64),
        *manifest.source_artifacts[1:],
    )
    stale = StrategyResearchPageEffectivenessManifest.seal(
        page_id=manifest.page_id,
        repository_commit=manifest.repository_commit,
        source_snapshot_commit=manifest.source_snapshot_commit,
        policy_id=manifest.policy_id,
        policy_version=manifest.policy_version,
        policy_sha256=manifest.policy_sha256,
        primary_research_start=manifest.primary_research_start,
        freshness_status=manifest.freshness_status,
        reader_questions=manifest.reader_questions,
        task_coverage=manifest.task_coverage,
        source_artifacts=wrong_sources,
        rendered_artifacts=manifest.rendered_artifacts,
        acceptance=manifest.acceptance,
    )
    stale_validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=stale,
        rendered_payloads=payloads,
    )
    assert stale_validation.status == "FAIL"
    assert stale_validation.freshness_status is PageFreshnessStatus.STALE_REBUILD_REQUIRED
    assert "SEMANTIC_SOURCE_DRIFT" in stale_validation.errors


def test_unknown_or_invalid_policy_fails_closed(tmp_path: Path) -> None:
    outside = tmp_path / "policy.yaml"
    outside.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(PageEffectivenessError, match="POLICY_OUTSIDE_REPOSITORY"):
        load_page_effectiveness_policy(repository_root=ROOT, policy_path=outside)


def test_repository_ahead_without_relevant_drift_is_not_called_current() -> None:
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        repository_commit="b" * 40,
        source_snapshot_commit="a" * 40,
    )
    assert manifest.freshness_status is PageFreshnessStatus.REPOSITORY_AHEAD_NO_RELEVANT_DRIFT
