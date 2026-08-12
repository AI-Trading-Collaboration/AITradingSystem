from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.strategy_research_reopen_readiness_decision import (
    DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH,
    DataEvidenceLane,
    PermittedResearchStage,
    ReadinessActionRequest,
    ReadinessSourceStatus,
    StrategyResearchReopenReadinessDecision,
    StrategyResearchReopenReadinessError,
    build_strategy_research_reopen_readiness_decision,
    load_strategy_research_reopen_readiness_policy,
)

POLICY_FILE_SHA256 = "6f4688c245b512cef315128d721f76012725f37b7e232e197f107b1b4d27e223"
POLICY_CANONICAL_SHA256 = "ccde97a297ff9334dc8b93a937ecf43efadaac959d3e01dadc1a2980d438a637"
AUTHORITY_SET_SHA256 = "16d18eb1ad3c1052eac533979374cff2d1f118a0a021008533d5aed10c2b9269"

AUTHORITY_PATHS = (
    "config/research/dynamic_v3_clean_selection_preregistration_policy.yaml",
    "docs/requirements/TRADING-2451_Dynamic_V3_Clean_Selection_S1_Preregistration.md",
    "docs/requirements/TRADING-2463_S4_O1_Relative_Opportunity_Spread_Preregistration_Freeze.md",
    "config/research/o1_relative_opportunity_blind_calendar_reentry_policy_v1.yaml",
    "config/research/qqq_options_primary_window_policy_calibration_v1.yaml",
    "config/research/qqq_options_primary_window_derived_calibration_evidence_generator_v1.yaml",
    "config/research/qc_qqq_options_primary_window_export_safe_derived_aggregate_collector_v1.yaml",
    "config/research/qc_qqq_options_primary_window_derived_aggregate_run_proposal_v1.yaml",
    "config/research/qc_qqq_options_primary_window_derived_aggregate_collection_evidence_admission_v1.yaml",
)


def _copy_policy_authorities(tmp_path: Path) -> Path:
    relative_paths = (
        DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH.as_posix(),
        *AUTHORITY_PATHS,
    )
    for relative in relative_paths:
        source = PROJECT_ROOT / relative
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return tmp_path


def _replace(path: Path, old: str, new: str) -> None:
    content = path.read_text(encoding="utf-8")
    assert old in content
    path.write_text(content.replace(old, new, 1), encoding="utf-8", newline="\n")


def _rewrite_binding_hash(policy_path: Path, old_hash: str, authority_path: Path) -> None:
    new_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(policy_path, old_hash, new_hash)


def _build() -> StrategyResearchReopenReadinessDecision:
    return build_strategy_research_reopen_readiness_decision(
        decision_id="strategy-research-reopen-readiness-20260813",
        evaluated_at_utc=datetime(2026, 8, 13, 1, 30, tzinfo=UTC),
    )


def test_policy_loads_exact_authority_identity_and_semantics() -> None:
    loaded = load_strategy_research_reopen_readiness_policy()

    assert loaded.policy_file_sha256 == POLICY_FILE_SHA256
    assert loaded.policy_canonical_sha256 == POLICY_CANONICAL_SHA256
    assert loaded.authority_set_sha256 == AUTHORITY_SET_SHA256
    assert len(loaded.authority_observations) == 9
    assert all(item.identity_verified for item in loaded.authority_observations)
    assert all(item.semantics_verified for item in loaded.authority_observations)
    assert sum(item.semantic_fact_count for item in loaded.authority_observations) == 39
    assert sum(item.required_snippet_count for item in loaded.authority_observations) == 7


def test_default_decision_keeps_empirical_research_closed() -> None:
    decision = _build()

    assert decision.reopen_decision == "KEEP_CLOSED"
    assert decision.permitted_stage == "PREREGISTRATION_ONLY"
    assert decision.selected_data_lane is None
    assert decision.recommended_data_lane == "QLD_CANONICAL_FULL_CACHE_DQ"
    assert decision.recommendation_status == "RECOMMENDATION_ONLY_NOT_SELECTED_OR_AUTHORIZED"
    assert decision.primary_research_start.isoformat() == "2021-02-22"
    assert decision.prohibited_default_start.isoformat() == "2022-12-01"
    assert decision.data_lane_selected is False
    assert decision.empirical_research_authorized is False
    assert decision.candidate_search_authorized is False
    assert decision.parameter_search_authorized is False
    assert decision.backtest_authorized is False
    assert decision.holdout_access_authorized is False
    assert decision.investment_conclusion_authorized is False
    assert decision.external_action_authorized is False
    assert decision.production_effect == "none"
    assert decision.broker_action == "none"


def test_decision_is_canonical_sealed_and_replayable() -> None:
    decision = _build()

    replay = StrategyResearchReopenReadinessDecision.from_json_bytes(decision.canonical_bytes)

    assert replay == decision
    assert replay.canonical_bytes == decision.canonical_bytes
    assert replay.canonical_sha256 == decision.canonical_sha256
    assert decision.canonical_sha256 == (
        "b1ec45e722bc63eaa354ab46c0d518e9dee4854fd00223207388ea275fa10292"
    )
    assert replay.content_sha256 == decision.compute_content_sha256()


def test_source_declaration_input_order_does_not_change_identity() -> None:
    loaded = load_strategy_research_reopen_readiness_policy()
    forward = {item.source_id: item.status for item in loaded.policy.source_facts}
    reverse = dict(reversed(tuple(forward.items())))

    first = build_strategy_research_reopen_readiness_decision(
        decision_id="stable-order",
        evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
        source_status_declarations=forward,
    )
    second = build_strategy_research_reopen_readiness_decision(
        decision_id="stable-order",
        evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
        source_status_declarations=reverse,
    )

    assert first.canonical_bytes == second.canonical_bytes


@pytest.mark.parametrize(
    "forged_status",
    [
        ReadinessSourceStatus.NOT_EVALUATED,
        ReadinessSourceStatus.ELIGIBLE_FOR_OWNER_AUTHORIZED_CLEAN_RUN,
    ],
)
def test_caller_cannot_forge_legacy_source_pass_or_unknown(
    forged_status: ReadinessSourceStatus,
) -> None:
    loaded = load_strategy_research_reopen_readiness_policy()
    declarations = {item.source_id: item.status for item in loaded.policy.source_facts}
    declarations["LEGACY_DYNAMIC_V3_SELECTION"] = forged_status

    with pytest.raises(
        StrategyResearchReopenReadinessError,
        match="READINESS_SOURCE_DECLARATION_MISMATCH",
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id="forged-source",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            source_status_declarations=declarations,
        )


def test_caller_cannot_forge_qqq_options_dq_pass() -> None:
    loaded = load_strategy_research_reopen_readiness_policy()
    declarations: dict[str, ReadinessSourceStatus | str] = {
        item.source_id: item.status for item in loaded.policy.source_facts
    }
    declarations["QQQ_OPTIONS_DQ_PIT"] = "PASS"

    with pytest.raises(
        StrategyResearchReopenReadinessError,
        match="READINESS_SOURCE_DECLARATION_MISMATCH",
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id="forged-dq-pass",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            source_status_declarations=declarations,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "stage",
    [
        PermittedResearchStage.SINGLE_DATA_EVIDENCE_LANE_ONLY,
        PermittedResearchStage.READY_FOR_OWNER_REOPEN_REVIEW,
    ],
)
def test_stage_escalation_is_rejected(stage: PermittedResearchStage) -> None:
    with pytest.raises(
        StrategyResearchReopenReadinessError, match="READINESS_STAGE_NOT_AUTHORIZED"
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id="stage-escalation",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            requested_stage=stage,
        )


def test_single_lane_is_not_selected_by_recommendation() -> None:
    with pytest.raises(
        StrategyResearchReopenReadinessError, match="DATA_EVIDENCE_LANE_NOT_OWNER_SELECTED"
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id="unauthorized-lane",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            selected_data_lanes=(DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ,),
        )


def test_parallel_heavy_data_lanes_are_rejected() -> None:
    with pytest.raises(
        StrategyResearchReopenReadinessError, match="MULTIPLE_DATA_EVIDENCE_LANES_PROHIBITED"
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id="dual-lane",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            selected_data_lanes=(
                DataEvidenceLane.QLD_CANONICAL_FULL_CACHE_DQ,
                DataEvidenceLane.QQQ_OPTIONS_PRIMARY_WINDOW_EVIDENCE,
            ),
        )


@pytest.mark.parametrize(
    "action_field",
    [
        "empirical_research",
        "candidate_search",
        "parameter_search",
        "backtest",
        "holdout_access",
        "cache_mutation",
        "external_action",
        "investment_conclusion",
        "paper",
        "live",
        "broker",
        "production",
    ],
)
def test_unauthorized_empirical_external_and_production_actions_fail_closed(
    action_field: str,
) -> None:
    request = ReadinessActionRequest.model_validate({action_field: True})

    with pytest.raises(
        StrategyResearchReopenReadinessError,
        match="EMPIRICAL_OR_EXTERNAL_ACTION_NOT_AUTHORIZED",
    ):
        build_strategy_research_reopen_readiness_decision(
            decision_id=f"unauthorized-{action_field}",
            evaluated_at_utc=datetime(2026, 8, 13, tzinfo=UTC),
            action_request=request,
        )


def test_authority_file_hash_drift_fails_closed(tmp_path: Path) -> None:
    root = _copy_policy_authorities(tmp_path)
    authority_path = root / AUTHORITY_PATHS[-1]
    authority_path.write_bytes(authority_path.read_bytes() + b"\n")

    with pytest.raises(
        StrategyResearchReopenReadinessError, match="authority file SHA-256 mismatch"
    ):
        load_strategy_research_reopen_readiness_policy(project_root=root)


def test_authority_semantic_drift_fails_even_when_binding_hash_is_updated(tmp_path: Path) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH
    authority_path = root / AUTHORITY_PATHS[-1]
    old_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(authority_path, "owner_token_observed: false", "owner_token_observed: true")
    _rewrite_binding_hash(policy_path, old_hash, authority_path)

    with pytest.raises(StrategyResearchReopenReadinessError, match="semantic fact mismatch"):
        load_strategy_research_reopen_readiness_policy(project_root=root)


def test_text_authority_semantic_drift_fails_even_when_binding_hash_is_updated(
    tmp_path: Path,
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH
    authority_path = root / AUTHORITY_PATHS[1]
    old_hash = hashlib.sha256(authority_path.read_bytes()).hexdigest()
    _replace(authority_path, "historical_seen_protocol_replay", "historical_protocol_removed")
    _rewrite_binding_hash(policy_path, old_hash, authority_path)

    with pytest.raises(StrategyResearchReopenReadinessError, match="required text is missing"):
        load_strategy_research_reopen_readiness_policy(project_root=root)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "primary_research_start: '2021-02-22'",
            "primary_research_start: '2022-12-01'",
            "PRIMARY research start",
        ),
        (
            "prohibited_default_start: '2022-12-01'",
            "prohibited_default_start: '2021-02-22'",
            "historical 2022-12-01 boundary",
        ),
        (
            "permitted_stage: PREREGISTRATION_ONLY",
            "permitted_stage: SINGLE_DATA_EVIDENCE_LANE_ONLY",
            "permitted_stage",
        ),
        (
            "selected_data_lane: null",
            "selected_data_lane: QLD_CANONICAL_FULL_CACHE_DQ",
            "selected_data_lane",
        ),
    ],
)
def test_readiness_policy_cannot_promote_window_stage_or_lane(
    tmp_path: Path, old: str, new: str, message: str
) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH
    _replace(policy_path, old, new)

    with pytest.raises(StrategyResearchReopenReadinessError, match=message):
        load_strategy_research_reopen_readiness_policy(project_root=root)


def test_duplicate_policy_key_is_rejected(tmp_path: Path) -> None:
    root = _copy_policy_authorities(tmp_path)
    policy_path = root / DEFAULT_STRATEGY_RESEARCH_REOPEN_READINESS_DECISION_POLICY_PATH
    _replace(
        policy_path,
        "policy_version: 1.0.0",
        "policy_version: 1.0.0\npolicy_version: 1.0.0",
    )

    with pytest.raises(StrategyResearchReopenReadinessError, match="DUPLICATE_KEY"):
        load_strategy_research_reopen_readiness_policy(project_root=root)


def test_decision_tamper_is_rejected() -> None:
    decision = _build()
    payload = json.loads(decision.canonical_bytes)
    payload["backtest_authorized"] = True
    tampered = (json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode()

    with pytest.raises(
        StrategyResearchReopenReadinessError, match="READINESS_DECISION_RECORD_INVALID"
    ):
        StrategyResearchReopenReadinessDecision.from_json_bytes(tampered)


def test_noncanonical_and_duplicate_json_are_rejected() -> None:
    decision = _build()
    noncanonical = json.dumps(json.loads(decision.canonical_bytes)).encode()
    duplicate = decision.canonical_bytes.replace(
        b'{\n  "authority_observations"',
        b'{\n  "authority_set_sha256": "'
        + decision.authority_set_sha256.encode()
        + b'",\n  "authority_observations"',
        1,
    )

    with pytest.raises(StrategyResearchReopenReadinessError, match="not canonical JSON bytes"):
        StrategyResearchReopenReadinessDecision.from_json_bytes(noncanonical)
    with pytest.raises(StrategyResearchReopenReadinessError, match="duplicate JSON key"):
        StrategyResearchReopenReadinessDecision.from_json_bytes(duplicate)
