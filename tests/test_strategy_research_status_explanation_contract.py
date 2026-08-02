from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from ai_trading_system.atlas.status_explanation_projection import (
    build_status_explanation_bundle,
)
from ai_trading_system.contracts.strategy_research_status_explanation import (
    ATLAS_STATUS_EXPLANATION_STAGE_IDS,
    CitedExplanationFact,
    ExplanationAuthorityKind,
    ExplanationFactKind,
    ExplanationTransitionCondition,
    ExplanationValidationSummary,
    ExplanationValueState,
    StrategyResearchStatusExplanationBundle,
    StrategyResearchStatusExplanationContractError,
)

ROOT = Path(__file__).resolve().parents[1]
EXACT_COMMIT = "13292726540dc78039a85f17a39f64ddbee956d1"


@pytest.fixture(scope="module")
def bundle() -> StrategyResearchStatusExplanationBundle:
    return build_status_explanation_bundle(
        repository_root=ROOT,
        exact_commit=EXACT_COMMIT,
    )


def test_status_explanation_bundle_has_stable_stage_contract(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    assert tuple(item.stage_id for item in bundle.explanation_records) == (
        ATLAS_STATUS_EXPLANATION_STAGE_IDS
    )
    assert bundle.primary_research_start == "2021-02-22"
    assert bundle.excluded_task_ids == tuple(f"TRADING-{task_id}" for task_id in range(2481, 2494))
    assert bundle.validation_summary is ExplanationValidationSummary.INSUFFICIENT_AUTHORITY
    assert bundle.snapshot_fingerprint == bundle.snapshot_id


def test_status_explanation_bundle_seal_and_replay_are_canonical(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    replay = StrategyResearchStatusExplanationBundle.from_json_bytes(bundle.canonical_bytes)

    assert replay == bundle
    assert replay.compute_content_sha256() == bundle.content_sha256
    assert replay.canonical_bytes == bundle.canonical_bytes


def test_status_explanation_bundle_rejects_extra_field(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    payload = bundle.to_dict()
    payload["fabricated_reason"] = "OOS 不足"

    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_SCHEMA_KEYS_INVALID:bundle",
    ):
        StrategyResearchStatusExplanationBundle.from_json_bytes(
            (
                json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")
        )


def test_status_explanation_bundle_rejects_content_tamper(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    payload = bundle.to_dict()
    payload["primary_research_start"] = "2022-12-01"

    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_CONTENT_SHA256_MISMATCH",
    ):
        StrategyResearchStatusExplanationBundle.from_json_bytes(
            (
                json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                + "\n"
            ).encode("utf-8")
        )


def test_present_fact_requires_exact_source_ref() -> None:
    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_PRESENT_FACT_UNCITED",
    ):
        CitedExplanationFact(
            fact_id="fact-without-source",
            fact_kind=ExplanationFactKind.EVIDENCE_GAP,
            value_state=ExplanationValueState.PRESENT,
            text_zh="不能凭空生成具体证据缺口。",
            authority_kind=ExplanationAuthorityKind.CANONICAL_RESULT,
            authority_id="result-restart-r2",
            source_ref_ids=(),
        )


def test_non_present_transition_cannot_hide_specific_reason() -> None:
    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_NON_PRESENT_TRANSITION_HAS_HIDDEN_FACTS",
    ):
        ExplanationTransitionCondition(
            condition_id="hidden-transition",
            value_state=ExplanationValueState.NOT_RECORDED,
            description_zh="状态转变条件尚未记录。",
            current_state=None,
            observable_event="OOS_PASS",
            deciding_authority_kind=None,
            deciding_authority_id=None,
            target_status=None,
            source_ref_ids=(),
        )


def test_validation_summary_cannot_claim_pass_when_authority_is_missing(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_VALIDATION_SUMMARY_INVALID",
    ):
        replace(bundle, validation_summary=ExplanationValidationSummary.PASS)


def test_record_summary_only_uses_declared_fact_ids(
    bundle: StrategyResearchStatusExplanationBundle,
) -> None:
    record = bundle.explanation_records[2]

    with pytest.raises(
        StrategyResearchStatusExplanationContractError,
        match="STATUS_EXPLANATION_SUMMARY_FACT_UNKNOWN",
    ):
        replace(record, derived_from_fact_ids=("not-a-fact",))
