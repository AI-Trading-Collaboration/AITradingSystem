from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from ai_trading_system.contracts.research_context import (
    CoverageInterval,
    DataQualityContractRef,
    DateRange,
    EffectiveCoverage,
    MarketRegimeSpec,
    PolicyRef,
    ResearchWindowSpec,
    resolve_complete_research_context,
)
from ai_trading_system.contracts.research_lifecycle import (
    ResearchPreregistration,
    ResultVisibility,
)
from ai_trading_system.contracts.research_selection_protocol import (
    CandidateUniverseCommitment,
    MarketOutcomeVisibility,
    SelectionDataBinding,
    SelectionDataRole,
    SelectionPolicyBinding,
    SelectionPolicyRole,
    SelectionProtocolFoundationPolicy,
    SelectionProtocolFoundationStatus,
    SelectionProtocolSafety,
    UncontaminatedSelectionProtocol,
    validate_uncontaminated_selection_protocol,
    validate_uncontaminated_selection_protocol_payload,
)
from ai_trading_system.contracts.status import (
    EvidenceRole,
    PolicyRole,
    ResearchWindowRole,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

POLICY_PATH = Path("config/research/uncontaminated_selection_protocol_policy.yaml")
FROZEN_AT = datetime(2026, 7, 21, 9, 0, tzinfo=UTC)
SELECTION_POLICY_SHA = "d" * 64


def load_selection_protocol_foundation_policy(
    path: Path,
) -> SelectionProtocolFoundationPolicy:
    payload = safe_load_yaml_path(path)
    assert isinstance(payload, dict)
    return SelectionProtocolFoundationPolicy.from_dict(payload)


def test_foundation_policy_and_valid_protocol_are_deterministic_and_non_executing() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)

    validation = validate_uncontaminated_selection_protocol(protocol, policy=policy)
    reordered = replace(
        protocol,
        data_bindings=tuple(reversed(protocol.data_bindings)),
        policy_bindings=tuple(reversed(protocol.policy_bindings)),
    )

    assert policy.owner == "research_governance"
    assert set(policy.required_data_roles) == set(SelectionDataRole)
    assert validation["status"] == "PASS"
    assert validation["foundation_status"] == "FOUNDATION_ONLY"
    assert validation["admission_status"] == "READY_FOR_OWNER_PROTOCOL_AUTHORING"
    assert validation["execution_unblocked"] is False
    assert validation["prospective_holdout_access_allowed"] is False
    assert protocol.protocol_id == reordered.protocol_id
    assert UncontaminatedSelectionProtocol.from_dict(protocol.to_dict()) == protocol


def test_full_period_top_n_candidate_origin_is_blocked() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    contaminated = replace(
        protocol,
        candidate_universe=replace(
            protocol.candidate_universe,
            origin="full_period_source_leaderboard_top_n",
            derived_from_full_period_ranking=True,
        ),
    )

    validation = validate_uncontaminated_selection_protocol(contaminated, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "candidate_universe_not_result_ranked")["status"] == "FAIL"


def test_selection_result_visible_at_or_before_freeze_is_blocked() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    train = _binding(protocol, SelectionDataRole.TRAIN_SELECTION)
    contaminated = _replace_binding(
        protocol,
        replace(
            train,
            evaluation_result_visibility_at_freeze=ResultVisibility.FULL,
            first_evaluation_result_visible_at=FROZEN_AT,
        ),
    )

    validation = validate_uncontaminated_selection_protocol(contaminated, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "results_unseen_at_freeze")["status"] == "FAIL"


def test_forbidden_role_window_overlap_is_blocked() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    prospective = _binding(protocol, SelectionDataRole.PROSPECTIVE_UNTOUCHED)
    overlapped = _replace_binding(
        protocol,
        replace(prospective, date_range=DateRange(date(2025, 12, 1), date(2027, 7, 21))),
    )

    validation = validate_uncontaminated_selection_protocol(overlapped, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "role_windows_do_not_overlap")["status"] == "FAIL"


def test_any_prospective_access_is_blocked() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    prospective = _binding(protocol, SelectionDataRole.PROSPECTIVE_UNTOUCHED)
    accessed = _replace_binding(protocol, replace(prospective, accessed=True))

    validation = validate_uncontaminated_selection_protocol(accessed, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "prospective_holdout_untouched")["status"] == "FAIL"


@pytest.mark.parametrize(
    "intended_role",
    [SelectionPolicyRole.OBSERVE_ONLY_ENABLEMENT, SelectionPolicyRole.REPORTING_ONLY],
)
def test_observe_or_reporting_policy_cannot_be_consumed_as_hard_eligibility(
    intended_role: SelectionPolicyRole,
) -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    selection = protocol.policy_bindings[0]
    mismatched = replace(
        protocol,
        policy_bindings=(
            replace(
                selection,
                intended_roles=(intended_role,),
                consumed_roles=(SelectionPolicyRole.HARD_ELIGIBILITY,),
            ),
        ),
    )

    validation = validate_uncontaminated_selection_protocol(mismatched, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "selection_policy_roles_compatible")["status"] == "FAIL"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("owner", ""),
        ("version", ""),
        ("rationale", ""),
        ("review_condition", ""),
        ("source_sha256", ""),
    ],
)
def test_missing_policy_governance_metadata_is_blocked(field: str, value: str) -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    binding = replace(protocol.policy_bindings[0], **{field: value})
    incomplete = replace(protocol, policy_bindings=(binding,))

    validation = validate_uncontaminated_selection_protocol(incomplete, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "selection_policy_governance_complete")["status"] == "FAIL"


def test_candidate_universe_cannot_derive_from_validation_source() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    historical_validation = _binding(protocol, SelectionDataRole.HISTORICAL_SEEN_VALIDATION)
    contaminated = replace(
        protocol,
        candidate_universe=replace(
            protocol.candidate_universe,
            derivation_source_ids=(historical_validation.source_id,),
        ),
    )

    validation = validate_uncontaminated_selection_protocol(contaminated, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "candidate_universe_uses_discovery_sources_only")["status"] == "FAIL"


def test_retired_2022_window_cannot_be_active_primary_context() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    legacy_context = _context(legacy=True)
    legacy = replace(
        protocol,
        research_context=legacy_context,
        preregistration=replace(
            protocol.preregistration,
            research_context_id=legacy_context.context_id,
        ),
    )

    validation = validate_uncontaminated_selection_protocol(legacy, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "active_primary_context")["status"] == "FAIL"


def test_safety_flags_fail_closed() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    unsafe = replace(
        protocol,
        safety=replace(protocol.safety, parameter_search_allowed=True),
    )

    validation = validate_uncontaminated_selection_protocol(unsafe, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "foundation_safety_closed")["status"] == "FAIL"


def test_content_derived_identity_detects_nested_tamper() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    payload = protocol.to_dict()
    payload["candidate_universe"]["origin"] = "tampered-origin"

    validation = validate_uncontaminated_selection_protocol_payload(payload, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert validation["admission_status"] == "BLOCKED_PROTOCOL_TAMPER_OR_SCHEMA"
    assert validation["checks"][0]["check_id"] == "protocol_content_recomputed"


def test_foundation_policy_semantic_hash_drift_is_blocking() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    changed_policy = replace(policy, rationale=f"{policy.rationale} changed")

    validation = validate_uncontaminated_selection_protocol(
        protocol,
        policy=changed_policy,
    )

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "foundation_policy_binding_fresh")["status"] == "FAIL"


def test_not_preregistered_status_is_typed_but_not_ready_for_authoring() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = replace(
        _protocol(policy),
        foundation_status=SelectionProtocolFoundationStatus.NOT_PREREGISTERED,
    )

    validation = validate_uncontaminated_selection_protocol(protocol, policy=policy)

    assert validation["status"] == "BLOCKED"
    assert validation["foundation_status"] == "NOT_PREREGISTERED"
    assert validation["admission_status"] == "BLOCKED_PROTOCOL_CONTRACT"
    assert _check(validation, "foundation_status_ready_for_authoring")["status"] == "FAIL"


def test_unreviewed_foundation_policy_status_is_blocked() -> None:
    policy = load_selection_protocol_foundation_policy(POLICY_PATH)
    protocol = _protocol(policy)
    unreviewed = replace(policy, status="draft")
    rebound = replace(protocol, foundation_policy_sha256=unreviewed.semantic_sha256)

    validation = validate_uncontaminated_selection_protocol(rebound, policy=unreviewed)

    assert validation["status"] == "BLOCKED"
    assert _check(validation, "foundation_policy_governance_complete")["status"] == "FAIL"


def _protocol(policy) -> UncontaminatedSelectionProtocol:
    context = _context()
    universe = CandidateUniverseCommitment(
        universe_id="dynamic-v3-future-universe-v1",
        origin="preregistered_generator_cartesian_product",
        generator_id="dynamic-v3-generator",
        generator_version="v1",
        generator_sha256="a" * 64,
        universe_sha256="b" * 64,
        derivation_source_ids=("historical-discovery-snapshot",),
        derived_from_full_period_ranking=False,
    )
    selection_policy = SelectionPolicyBinding(
        policy_id="future-selection-hard-gate-v1",
        version="v1",
        status="owner_reviewed",
        owner="research_governance",
        rationale="Use only a reviewed hard-eligibility rule for train-time admission.",
        review_condition="Review before changing role, threshold, or consumer.",
        source_sha256=SELECTION_POLICY_SHA,
        intended_roles=(SelectionPolicyRole.HARD_ELIGIBILITY,),
        consumed_roles=(SelectionPolicyRole.HARD_ELIGIBILITY,),
    )
    preregistration = ResearchPreregistration(
        hypothesis_id="future-selection-foundation",
        hypothesis_statement="A frozen candidate generator is evaluated by disjoint roles.",
        owner="research_owner",
        baseline_id="b0-static",
        candidate_id=universe.universe_id,
        research_context_id=context.context_id,
        selection_rule_id=selection_policy.policy_id,
        selection_rule_sha256=selection_policy.source_sha256,
        metric_ids=("annualized_return", "max_drawdown", "turnover"),
        policy_ref_ids=(policy.policy_id, selection_policy.policy_id),
        validation_plan_ids=("historical-seen-validation", "prospective-single-access"),
        frozen_at=FROZEN_AT,
    )
    return UncontaminatedSelectionProtocol(
        protocol_version="v1",
        foundation_status=SelectionProtocolFoundationStatus.FOUNDATION_ONLY,
        owner="research_owner",
        rationale="Freeze role separation before any new selection results are visible.",
        review_condition="Review before any role, source, policy, or access change.",
        foundation_policy_id=policy.policy_id,
        foundation_policy_sha256=policy.semantic_sha256,
        research_context=context,
        preregistration=preregistration,
        candidate_universe=universe,
        data_bindings=(
            SelectionDataBinding(
                role=SelectionDataRole.DISCOVERY_HISTORICAL_KNOWN,
                window_id="discovery-2021",
                date_range=DateRange(date(2021, 2, 22), date(2021, 12, 31)),
                source_id="historical-discovery-snapshot",
                source_sha256="1" * 64,
                market_outcome_visibility=MarketOutcomeVisibility.KNOWN,
                evaluation_result_visibility_at_freeze=ResultVisibility.FULL,
                first_evaluation_result_visible_at=datetime(2022, 1, 1, tzinfo=UTC),
                accessed=True,
                candidate_generation_input=True,
                selection_input=False,
            ),
            SelectionDataBinding(
                role=SelectionDataRole.TRAIN_SELECTION,
                window_id="train-2022-2023",
                date_range=DateRange(date(2022, 1, 3), date(2023, 12, 29)),
                source_id="frozen-train-snapshot",
                source_sha256="2" * 64,
                market_outcome_visibility=MarketOutcomeVisibility.KNOWN,
                evaluation_result_visibility_at_freeze=ResultVisibility.NONE,
                first_evaluation_result_visible_at=None,
                accessed=True,
                candidate_generation_input=False,
                selection_input=True,
            ),
            SelectionDataBinding(
                role=SelectionDataRole.HISTORICAL_SEEN_VALIDATION,
                window_id="historical-seen-2024-2025",
                date_range=DateRange(date(2024, 1, 2), date(2025, 12, 31)),
                source_id="frozen-historical-validation-snapshot",
                source_sha256="3" * 64,
                market_outcome_visibility=MarketOutcomeVisibility.KNOWN,
                evaluation_result_visibility_at_freeze=ResultVisibility.NONE,
                first_evaluation_result_visible_at=None,
                accessed=True,
                candidate_generation_input=False,
                selection_input=False,
            ),
            SelectionDataBinding(
                role=SelectionDataRole.PROSPECTIVE_UNTOUCHED,
                window_id="prospective-2026-2027",
                date_range=DateRange(date(2026, 7, 22), date(2027, 7, 21)),
                source_id="prospective-single-access-slot",
                source_sha256="4" * 64,
                market_outcome_visibility=MarketOutcomeVisibility.UNTOUCHED,
                evaluation_result_visibility_at_freeze=ResultVisibility.NONE,
                first_evaluation_result_visible_at=None,
                accessed=False,
                candidate_generation_input=False,
                selection_input=False,
            ),
        ),
        policy_bindings=(selection_policy,),
        safety=SelectionProtocolSafety(),
    )


def _context(*, legacy: bool = False):
    policy_refs = _policy_refs()
    start = date(2022, 12, 1) if legacy else date(2021, 2, 22)
    regime = MarketRegimeSpec(
        regime_id="ai_after_chatgpt" if legacy else "unified_primary_2021",
        anchor_date=date(2022, 11, 30) if legacy else start,
        start_date=start,
    )
    window = ResearchWindowSpec(
        window_id=("legacy_research_window_2022_12" if legacy else "exact_three_asset_validated"),
        start_date=start,
        role=(
            ResearchWindowRole.LEGACY_COMPARISON if legacy else ResearchWindowRole.PRIMARY_VALIDATED
        ),
        evidence_role=(
            EvidenceRole.LEGACY_COMPARISON_EVIDENCE
            if legacy
            else EvidenceRole.PRIMARY_DECISION_EVIDENCE
        ),
    )
    requested = DateRange(start, date(2025, 12, 31))
    return resolve_complete_research_context(
        regime=regime,
        window=window,
        requested_range=requested,
        actual_data_range=requested,
        effective_feature_start=start,
        effective_prediction_start=start,
        actual_portfolio_start=start,
        evaluation_range=requested,
        as_of=date(2025, 12, 31),
        trading_calendar="XNYS",
        effective_coverage=EffectiveCoverage(
            (CoverageInterval(source_id="fixture-prices", date_range=requested),)
        ),
        data_quality=DataQualityContractRef(
            contract_id="fixture-dq",
            status="PASS",
            passed=True,
            as_of=date(2025, 12, 31),
            policy_ref_id="fixture-data-quality",
        ),
        policy_refs=policy_refs,
    )


def _policy_refs() -> tuple[PolicyRef, ...]:
    return (
        PolicyRef(
            policy_id="fixture-market-regime",
            role=PolicyRole.MARKET_REGIME,
            version="v1",
            status="fixture",
            path="config/market_regimes.yaml",
            sha256="5" * 64,
        ),
        PolicyRef(
            policy_id="fixture-research-window",
            role=PolicyRole.RESEARCH_WINDOW,
            version="v1",
            status="fixture",
            path="config/research/primary_research_window_policy.yaml",
            sha256="6" * 64,
        ),
        PolicyRef(
            policy_id="fixture-data-quality",
            role=PolicyRole.DATA_QUALITY,
            version="v1",
            status="fixture",
            path="config/etf_portfolio/data_quality.yaml",
            sha256="7" * 64,
        ),
    )


def _binding(
    protocol: UncontaminatedSelectionProtocol,
    role: SelectionDataRole,
) -> SelectionDataBinding:
    return next(item for item in protocol.data_bindings if item.role is role)


def _replace_binding(
    protocol: UncontaminatedSelectionProtocol,
    replacement: SelectionDataBinding,
) -> UncontaminatedSelectionProtocol:
    return replace(
        protocol,
        data_bindings=tuple(
            replacement if item.role is replacement.role else item
            for item in protocol.data_bindings
        ),
    )


def _check(validation: dict[str, object], check_id: str) -> dict[str, object]:
    return next(item for item in validation["checks"] if item["check_id"] == check_id)
