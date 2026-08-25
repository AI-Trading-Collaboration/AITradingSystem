from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from functools import lru_cache
from pathlib import Path

import pytest

from ai_trading_system.atlas.cited_query_renderer import (
    build_cited_query_showcase,
    render_cited_query_html,
)
from ai_trading_system.atlas.live_snapshot import build_live_snapshot_bundle
from ai_trading_system.atlas.page_effectiveness import (
    PageEffectivenessError,
    build_page_effectiveness_manifest,
    load_page_effectiveness_policy,
    repository_head,
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
    page_task_identity_sort_key,
)

ROOT = Path(__file__).resolve().parents[2]
PAGE_LOCATOR = "outputs/atlas/strategy_research_cited_query/trading_2470_v1/index.html"
PAGE_PREFIX = "outputs/atlas/strategy_research_cited_query/trading_2470_v1"


@lru_cache(maxsize=1)
def _live_sidecar_payloads() -> dict[str, bytes]:
    head = repository_head(ROOT)
    bundle = build_live_snapshot_bundle(repository_root=ROOT, exact_commit=head)
    showcase = build_cited_query_showcase(
        target_ids=bundle.target_ids,
        snapshot_payload=bundle.current_snapshot.to_dict(),
        before_payload=bundle.comparison_snapshot.to_dict(),
        after_payload=bundle.current_snapshot.to_dict(),
        diff_payload=bundle.current_diff.to_dict(),
        repository_root=ROOT,
    )
    return {
        "comparison_snapshot.json": bundle.comparison_snapshot.canonical_json_bytes(),
        "current_snapshot.json": bundle.current_snapshot.canonical_json_bytes(),
        "current_diff.json": bundle.current_diff.canonical_json_bytes(),
        "reader_state.json": showcase.reader_state.canonical_bytes,
        "index.html": render_cited_query_html(showcase).encode("utf-8"),
    }


def _rendered(
    payload: bytes | None = None,
) -> tuple[tuple[PageArtifactIdentity, ...], dict[str, bytes]]:
    live_payloads = _live_sidecar_payloads()
    payloads = {
        "index.html": live_payloads["index.html"] if payload is None else payload,
        **{name: raw for name, raw in live_payloads.items() if name != "index.html"},
    }
    identities = tuple(
        PageArtifactIdentity(
            role="ATLAS_PAGE_" + name.upper().replace(".", "_"),
            locator=f"{PAGE_PREFIX}/{name}",
            sha256=hashlib.sha256(raw).hexdigest(),
            byte_count=len(raw),
        )
        for name, raw in payloads.items()
    )
    return identities, payloads


def test_policy_freezes_reader_questions_and_suffix_aware_task_sources() -> None:
    policy = load_page_effectiveness_policy(repository_root=ROOT)
    assert policy.primary_research_start == "2021-02-22"
    assert len(policy.task_sources) == 73
    assert [item.task_id.split("_", 1)[0] for item in policy.task_sources] == [
        *[f"TRADING-{number}" for number in (*range(2481, 2505), *range(2506, 2524))],
        "TRADING-2523A",
        "TRADING-2523B",
        *[f"TRADING-{number}" for number in range(2524, 2543)],
        "TRADING-2542A",
        "TRADING-2542B",
        "TRADING-2542C",
        "TRADING-2542D",
        "TRADING-2542E",
        "TRADING-2542F",
        "TRADING-2542G",
        "TRADING-2543",
        "TRADING-2544",
        "TRADING-2545",
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
    assert manifest.schema_version == "strategy_research_page_effectiveness.v3"
    assert len(manifest.task_coverage) == 73
    assert [item.task_id.split("_", 1)[0] for item in manifest.task_coverage] == [
        *[f"TRADING-{number}" for number in (*range(2481, 2505), *range(2506, 2524))],
        "TRADING-2523A",
        "TRADING-2523B",
        *[f"TRADING-{number}" for number in range(2524, 2543)],
        "TRADING-2542A",
        "TRADING-2542B",
        "TRADING-2542C",
        "TRADING-2542D",
        "TRADING-2542E",
        "TRADING-2542F",
        "TRADING-2542G",
        "TRADING-2543",
        "TRADING-2544",
        "TRADING-2545",
    ]
    coverage_by_task = {
        item.task_id.split("_", 1)[0]: item.coverage for item in manifest.task_coverage
    }
    assert coverage_by_task["TRADING-2509"] == "DISCLOSED_VERSIONED_SUCCESSOR_CONTRACT_ONLY"
    assert coverage_by_task["TRADING-2510"] == (
        "DISCLOSED_PRIMARY_WINDOW_CALIBRATION_EVIDENCE_NOT_PROVIDED"
    )
    assert coverage_by_task["TRADING-2511"] == (
        "DISCLOSED_DERIVED_EVIDENCE_GENERATOR_SOURCE_BLOCKED"
    )
    assert coverage_by_task["TRADING-2512"] == (
        "DISCLOSED_EXPORT_SAFE_COLLECTOR_CONTRACT_RUN_NOT_AUTHORIZED"
    )
    assert coverage_by_task["TRADING-2513"] == (
        "DISCLOSED_EXACT_RUN_PROPOSAL_OWNER_AUTHORIZATION_REQUIRED"
    )
    assert coverage_by_task["TRADING-2514"] == (
        "DISCLOSED_EVIDENCE_ADMISSION_CONTRACT_OWNER_TOKEN_NOT_PROVIDED"
    )
    assert coverage_by_task["TRADING-2515"] == (
        "DISCLOSED_STRATEGY_RESEARCH_REOPEN_READINESS_KEEP_CLOSED"
    )
    assert coverage_by_task["TRADING-2516"] == (
        "DISCLOSED_QQQ_OPTIONS_EVIDENCE_LANE_TOKEN_CONSUMED_RUN_FAILED"
    )
    assert coverage_by_task["TRADING-2517"] == (
        "DISCLOSED_REFRESH_AUTHORIZATION_CONSUMED_FAILED_RUN_NO_EVIDENCE"
    )
    assert coverage_by_task["TRADING-2518"] == (
        "DISCLOSED_COLLECTOR_FILTER_FIXED_REAUTHORIZATION_REQUIRED"
    )
    assert coverage_by_task["TRADING-2519"] == (
        "DISCLOSED_V3_RUN_INVALID_DAILY_SLICE_FAILURE_FIX_BASELINE"
    )
    assert coverage_by_task["TRADING-2520"] == (
        "DISCLOSED_DAILY_SLICE_ACCESSOR_FIX_REVALIDATION_OWNER_TOKEN_REQUIRED"
    )
    assert coverage_by_task["TRADING-2521"] == (
        "DISCLOSED_V4_AUTHORIZATION_ADMITTED_AND_CONSUMED_BY_2522_RUN"
    )
    assert coverage_by_task["TRADING-2522"] == (
        "DISCLOSED_V4_RUN_INVALID_ALL_CHAIN_SESSIONS_TRANSPORT_REJECTED_AXIS_UNRESOLVED"
    )
    assert coverage_by_task["TRADING-2523"] == ("INCLUDED_READER_TERMINOLOGY_FIRST_USE_CONTRACT")
    assert coverage_by_task["TRADING-2523A"] == ("COMPLETED_BASE_DRIFT_INTEGRATION_CORRECTION")
    assert coverage_by_task["TRADING-2523B"] == (
        "COMPLETED_PAGE_EFFECTIVENESS_V2_SERIAL_CONTRACT_WAVE"
    )
    assert coverage_by_task["TRADING-2524"] == (
        "INTEGRATED_WHY_FIRST_DECISION_PATH_AND_PROGRESSIVE_DISCLOSURE"
    )
    assert coverage_by_task["TRADING-2525"] == (
        "INTEGRATED_OBJECT_QUALIFIED_STATE_DATE_CHANGE_PROJECTION"
    )
    assert coverage_by_task["TRADING-2526"] == (
        "INTEGRATED_AUTOMATED_ACCESSIBILITY_HARNESS_FINAL_HUMAN_REVIEW_PENDING"
    )
    assert coverage_by_task["TRADING-2527"] == (
        "PROTOCOL_PREPARED_OWNER_POLICY_AND_HUMAN_PILOT_PENDING"
    )
    assert coverage_by_task["TRADING-2528"] == (
        "DISCLOSED_OFFLINE_PER_AXIS_DIAGNOSTIC_IMPLEMENTED_ROOT_CAUSE_UNRESOLVED"
    )
    assert coverage_by_task["TRADING-2529"] == (
        "DISCLOSED_EXPORT_SAFE_PER_AXIS_PROPOSAL_READY_OWNER_FINAL_TOKEN_PENDING"
    )
    assert coverage_by_task["TRADING-2530"] == (
        "DISCLOSED_SINGLE_ZERO_ORDER_COLLECTION_COMPLETE_PER_AXIS_TRANSPORT_LIMITED"
    )
    assert coverage_by_task["TRADING-2531"] == (
        "DISCLOSED_OFFLINE_SESSION_FINALIZATION_AND_UNDERLYING_SOURCE_FIX_"
        "EXTERNAL_VALIDATION_PENDING"
    )
    assert coverage_by_task["TRADING-2532"] == (
        "DISCLOSED_SINGLE_V2_VALIDATION_COMPLETE_COLLECTOR_CONFOUNDERS_RESOLVED_DQ_PIT_BLOCKED"
    )
    assert coverage_by_task["TRADING-2533"] == (
        "DISCLOSED_OFFLINE_DQ_PIT_ADMISSION_FAIL_CLOSED_NEXT_EVIDENCE_GAPS_EXPLICIT"
    )
    assert coverage_by_task["TRADING-2534"] == (
        "DISCLOSED_STAGED_READINESS_AUTHORITY_IMPLEMENTED_CURRENT_EVIDENCE_STILL_BLOCKED"
    )
    assert coverage_by_task["TRADING-2535"] == (
        "DISCLOSED_EXPORT_SAFE_PROVIDER_TRANSPORT_ATTRIBUTION_PROPOSAL_READY_"
        "EXTERNAL_COLLECTION_UNEXECUTED"
    )
    assert coverage_by_task["TRADING-2536"] == (
        "DISCLOSED_ATLAS_SUCCESSOR_CLASSIFICATION_SERIAL_CONTRACT_WAVE"
    )
    assert coverage_by_task["TRADING-2537"] == (
        "DISCLOSED_SOURCE_TIME_V2_EXECUTION_ATTRIBUTION_RESOLVED_SUBSCRIPTION_REPAIR_PENDING"
    )
    assert coverage_by_task["TRADING-2538"] == (
        "DISCLOSED_FIRST_MUTATION_ATTEMPT_FAILED_AUTHORIZATION_CONSUMED_"
        "NO_CLOUD_RUN"
    )
    assert coverage_by_task["TRADING-2539"] == (
        "DISCLOSED_EXISTING_CLONE_V1_HISTORY_PRESERVED_V2_SUCCESSOR_EVIDENCE_RESOLVED"
    )
    assert coverage_by_task["TRADING-2540"] == (
        "DISCLOSED_QQQ_OPTIONS_LANE_RETAINED_PREREGISTRATION_BASELINE_DONE_"
        "THRESHOLD_POLICY_BLOCKED"
    )
    assert coverage_by_task["TRADING-2541"] == (
        "DISCLOSED_EXACT_DATE_SUBSCRIPTION_RECOVERY_CLOUD_VALIDATED_COMPLETE"
    )
    assert coverage_by_task["TRADING-2542"] == (
        "DISCLOSED_V1_REJECTED_V2_MEASUREMENT_DRAFT_OWNER_AND_DQ_REVIEW_REQUIRED"
    )
    assert coverage_by_task["TRADING-2542A"] == (
        "DISCLOSED_V2_EXACT_MEASUREMENT_DRAFT_OWNER_AND_DQ_REVIEW_REQUIRED"
    )
    assert coverage_by_task["TRADING-2542B"] == (
        "DISCLOSED_CANONICAL_DQ_PIT_SERIAL_CONTRACT_DRAFT_"
        "OWNER_AND_INDEPENDENT_REVIEW_REQUIRED"
    )
    assert coverage_by_task["TRADING-2542D"] == (
        "DISCLOSED_DQ_PIT_V3_AND_EXACT_SHEET_V4_OWNER_FROZEN_"
        "NON_EXECUTABLE_DATA_RESEARCH"
    )
    assert coverage_by_task["TRADING-2542E"] == (
        "DISCLOSED_EXACT_POLICY_FREEZE_ACCEPTED_"
        "VETO_SOURCE_CONTRACT_BLOCKED"
    )
    assert coverage_by_task["TRADING-2542F"] == (
        "DISCLOSED_RESULT_BLIND_VETO_OPTION_ARCHITECTURE_OWNER_EXACT_FROZEN"
    )
    assert coverage_by_task["TRADING-2542G"] == (
        "DISCLOSED_MANDATORY_VETO_EXACT_SEMANTICS_V2_DRAFT_"
        "0_OF_4_ADMITTED"
    )
    assert coverage_by_task["TRADING-2543"] == (
        "LIVE_CANONICAL_SNAPSHOT_DATE_AND_FRESHNESS_REPAIR_COMPLETE"
    )
    assert coverage_by_task["TRADING-2544"] == (
        "DISCLOSED_DIRECTION_ONLY_PROPOSED_OWNER_AUTHORIZATION_REQUIRED"
    )
    assert len(manifest.source_artifacts) == len(
        load_page_effectiveness_policy(repository_root=ROOT).relevant_source_paths
    )
    assert all(item.task_event_id for item in manifest.task_coverage)
    assert all(item.task_fragment_sha256 for item in manifest.task_coverage)
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
            reviewed_page_sha256="a" * 64,
        )


def test_human_review_page_identity_is_required_only_for_pass() -> None:
    with pytest.raises(PageEffectivenessContractError, match="REQUIRED:acceptance.reviewed_page"):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.OWNER_VISUAL_REVIEW,
            status=PageAcceptanceStatus.PASS,
            evidence_refs=("docs/requirements/review.md",),
            reviewer_id="project-owner",
            reviewed_at="2026-08-15T10:00:00+09:00",
            decision_id="owner-review-v1",
        )
    with pytest.raises(PageEffectivenessContractError, match="REVIEWED_PAGE_SHA256_INVALID"):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.OWNER_VISUAL_REVIEW,
            status=PageAcceptanceStatus.PASS,
            evidence_refs=("docs/requirements/review.md",),
            reviewer_id="project-owner",
            reviewed_at="2026-08-15T10:00:00+09:00",
            decision_id="owner-review-v1",
            reviewed_page_sha256="not-a-sha",
        )
    with pytest.raises(PageEffectivenessContractError, match="NON_PASS_CANNOT_BIND"):
        PageAcceptanceRecord(
            track=PageAcceptanceTrack.READER_COMPREHENSION_REVIEW,
            status=PageAcceptanceStatus.PENDING_REVIEW,
            evidence_refs=(),
            reviewed_page_sha256="a" * 64,
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
        reviewed_page_sha256=rendered[0].sha256,
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


def test_human_pass_for_different_page_identity_fails_validation() -> None:
    rendered, payloads = _rendered()
    reader_review = PageAcceptanceRecord(
        track=PageAcceptanceTrack.READER_COMPREHENSION_REVIEW,
        status=PageAcceptanceStatus.PASS,
        evidence_refs=("docs/requirements/review.md",),
        reviewer_id="project-owner",
        reviewed_at="2026-08-15T10:00:00+09:00",
        decision_id="reader-review-v1",
        reviewed_page_sha256="b" * 64,
    )
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
        reader_comprehension_review=reader_review,
    )

    validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=manifest,
        rendered_payloads=payloads,
    )

    assert validation.status == "FAIL"
    assert "HUMAN_REVIEW_PAGE_IDENTITY_MISMATCH:READER_COMPREHENSION_REVIEW" in validation.errors


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
    legacy = manifest.to_dict()
    legacy["schema_version"] = "strategy_research_page_effectiveness.v1"
    with pytest.raises(PageEffectivenessContractError, match="SCHEMA_INVALID"):
        StrategyResearchPageEffectivenessManifest.from_dict(legacy)
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
    assert "TASK_COVERAGE_EXACT_SET_MATCH" in validation.checks
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


def test_validation_rejects_visible_reader_decision_drift() -> None:
    rendered, payloads = _rendered()
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
    )
    tampered_html = payloads["index.html"].replace(
        "合计 1202/1202。".encode(),
        "合计 1201/1202。".encode(),
        1,
    )
    assert tampered_html != payloads["index.html"]

    validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=manifest,
        rendered_payloads={**payloads, "index.html": tampered_html},
    )

    assert validation.status == "FAIL"
    assert (
        "READER_DECISION_HTML_TEXT_DRIFT:reader_cards:WHY_PAUSED"
        in validation.errors
    )


def test_validation_rejects_live_snapshot_and_reader_date_substitution() -> None:
    rendered, payloads = _rendered()
    manifest = build_page_effectiveness_manifest(
        repository_root=ROOT,
        rendered_artifacts=rendered,
    )
    reader_state = json.loads(payloads["reader_state.json"])
    reader_state["dates"]["evidence_evaluated_at"] = reader_state["dates"][
        "research_state_as_of"
    ]
    tampered = {
        **payloads,
        "reader_state.json": (
            json.dumps(reader_state, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8"),
        "current_snapshot.json": payloads["current_snapshot.json"].replace(
            b'"title":"', b'"title":"tampered ', 1
        ),
    }

    validation = validate_page_effectiveness_manifest(
        repository_root=ROOT,
        manifest=manifest,
        rendered_payloads=tampered,
    )

    assert validation.status == "FAIL"
    assert any("reader_state.json" in item for item in validation.errors)
    assert any("current_snapshot.json" in item for item in validation.errors)


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


def test_task_identity_accepts_suffixes_and_rejects_ambiguous_ordering() -> None:
    assert page_task_identity_sort_key("TRADING-2523_ATLAS_TASK_V1") == (2523, "")
    assert page_task_identity_sort_key("TRADING-2523A_ATLAS_TASK_V1") == (2523, "A")
    assert page_task_identity_sort_key("TRADING-2523B_ATLAS_TASK_V1") == (2523, "B")
    with pytest.raises(PageEffectivenessContractError, match="TASK_ID_INVALID"):
        page_task_identity_sort_key("TRADING-2523b_ATLAS_TASK_V1")

    manifest = build_page_effectiveness_manifest(repository_root=ROOT)
    with pytest.raises(PageEffectivenessContractError, match="TASK_COVERAGE_ORDER_INVALID"):
        replace(manifest, task_coverage=tuple(reversed(manifest.task_coverage)))

    duplicate_identity = (
        *manifest.task_coverage[:-1],
        replace(
            manifest.task_coverage[-1],
            task_id="TRADING-2523B_DUPLICATE_IDENTITY_V1",
        ),
    )
    with pytest.raises(PageEffectivenessContractError, match="TASK_IDENTITY_DUPLICATE"):
        replace(manifest, task_coverage=duplicate_identity)
