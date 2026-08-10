from __future__ import annotations

import json
from datetime import UTC, date, datetime

import pytest

from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionAction,
    OwnerDecisionCanonicalGroup,
    OwnerDecisionGroupChoice,
    OwnerDecisionGroupMode,
    OwnerDecisionSlotChoice,
    load_qqq_options_owner_decision_manifest_policy,
)
from ai_trading_system.qqq_options_research.owner_policy_adoption_contract import (
    OwnerDecisionAttestationError,
    OwnerDecisionAttestationRecord,
    QQQOptionsPolicyAdoptionPlan,
    QQQOptionsPolicyAdoptionResolution,
    SlotCatalogAmendmentDisposition,
    SlotCatalogAmendmentResolution,
    build_policy_adoption_plan,
    load_owner_decision_attestation,
    load_qqq_options_owner_policy_adoption_contract,
    resolve_policy_adoption_plan,
)

_BASE_SHA = "c980c79e856fa3d91855154d45d8f6fb804de9e5"
_CREATED_AT = datetime(2026, 8, 10, 5, 0, tzinfo=UTC)
_DECISION_DATE = date(2026, 8, 10)
_OWNER_DECISION_ID = (
    "owner_decision:TRADING-2502:2026-08-10:review_qqq_options_backtest_policy_decision_pack_v1"
)


def _group_choices(
    *,
    selection: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G1,
) -> tuple[OwnerDecisionGroupChoice, ...]:
    modes = {
        OwnerDecisionCanonicalGroup.SELECTION: selection,
        OwnerDecisionCanonicalGroup.EXECUTION: OwnerDecisionGroupMode.G1,
        OwnerDecisionCanonicalGroup.ACCOUNTING: OwnerDecisionGroupMode.G1,
        OwnerDecisionCanonicalGroup.LIFECYCLE: OwnerDecisionGroupMode.G1,
        OwnerDecisionCanonicalGroup.ACCEPTANCE: OwnerDecisionGroupMode.G1,
    }
    return tuple(
        OwnerDecisionGroupChoice(canonical_group=group, mode=mode) for group, mode in modes.items()
    )


def _amendments() -> tuple[SlotCatalogAmendmentResolution, ...]:
    policy = load_qqq_options_owner_policy_adoption_contract().policy
    return tuple(
        SlotCatalogAmendmentResolution(
            amendment_id=item.amendment_id,
            disposition=SlotCatalogAmendmentDisposition.OWNER_REVIEW_REQUIRED,
            rationale=(
                "Synthetic contract fixture: Owner review remains required and the "
                "frozen v1 catalog remains authoritative."
            ),
            successor_catalog_version=None,
        )
        for item in policy.slot_catalog_amendments
    )


def _attestation(
    *,
    repository_code_sha: str = _BASE_SHA,
    owner_id: str = "project_owner",
    independent_reviewer_id: str = "project_owner",
    pack_requirement_lf_sha256: str | None = None,
    authority_set_sha256: str | None = None,
    manifest_policy_file_sha256: str | None = None,
    manifest_policy_canonical_sha256: str | None = None,
    slot_catalog_sha256: str | None = None,
    group_choices: tuple[OwnerDecisionGroupChoice, ...] | None = None,
    slot_choices: tuple[OwnerDecisionSlotChoice, ...] = (),
    amendment_resolutions: tuple[SlotCatalogAmendmentResolution, ...] | None = None,
) -> OwnerDecisionAttestationRecord:
    policy = load_qqq_options_owner_policy_adoption_contract().policy
    authority = policy.upstream_authority
    return OwnerDecisionAttestationRecord.seal(
        schema_version="qqq_options_owner_decision_attestation.v1",
        record_id="synthetic_owner_attestation_contract_fixture_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha=repository_code_sha,
        owner_decision_id=_OWNER_DECISION_ID,
        decision_date=_DECISION_DATE,
        owner_id=owner_id,
        independent_reviewer_id=independent_reviewer_id,
        pack_requirement_lf_sha256=(
            pack_requirement_lf_sha256 or authority.pack_requirement_lf_sha256
        ),
        authority_set_sha256=(authority_set_sha256 or authority.authority_set_sha256),
        manifest_policy_file_sha256=(
            manifest_policy_file_sha256 or authority.manifest_policy_file_sha256
        ),
        manifest_policy_canonical_sha256=(
            manifest_policy_canonical_sha256 or authority.manifest_policy_canonical_sha256
        ),
        slot_catalog_sha256=(slot_catalog_sha256 or authority.slot_catalog_sha256),
        group_choices=group_choices or _group_choices(),
        slot_choices=slot_choices,
        owner_policy_values=(),
        not_applicable_rationales=(),
        amendment_resolutions=(
            amendment_resolutions if amendment_resolutions is not None else _amendments()
        ),
        confirmed_no_engine_activation=True,
        confirmed_no_external_action=True,
    )


def test_policy_binds_exact_2504_authority_and_versioned_amendments() -> None:
    loaded = load_qqq_options_owner_policy_adoption_contract()
    upstream = load_qqq_options_owner_decision_manifest_policy()

    assert loaded.policy.frozen_slot_count == 28
    assert len(loaded.policy.slot_catalog_amendments) == 11
    assert loaded.policy.primary_research_start == date(2021, 2, 22)
    assert loaded.policy.upstream_authority.manifest_policy_file_sha256 == (
        upstream.policy_file_sha256
    )
    assert loaded.policy.upstream_authority.manifest_policy_canonical_sha256 == (
        upstream.policy_canonical_sha256
    )
    assert loaded.policy.upstream_authority.slot_catalog_sha256 == (upstream.slot_catalog_sha256)
    assert loaded.policy.safety.selection_authorized is False
    assert loaded.policy.safety.orders == loaded.policy.safety.fills == 0


def test_missing_attestation_stops_at_first_admission_gate() -> None:
    for raw in (None, b""):
        with pytest.raises(
            OwnerDecisionAttestationError,
            match="OWNER_ATTESTATION_MISSING",
        ):
            load_owner_decision_attestation(
                raw,
                expected_repository_code_sha=_BASE_SHA,
            )


def test_canonical_all_g1_attestation_derives_manifest_and_28_slot_plan() -> None:
    record = _attestation()
    admission = load_owner_decision_attestation(
        record.canonical_bytes,
        expected_repository_code_sha=_BASE_SHA,
    )
    plan = build_policy_adoption_plan(admission)

    assert admission.raw_byte_sha256 == record.canonical_sha256
    assert admission.semantic_content_sha256 == record.content_sha256
    assert len(admission.manifest.materialized_decisions) == 28
    assert len(plan.slot_plans) == 28
    assert {item.adoption_state.value for item in plan.slot_plans} == {"UNRESOLVED_BLOCKED"}
    assert plan.maximum_adoption_status == "VALID_POLICY_ADOPTION_CONTRACT_ONLY"
    assert plan.executable_policy_authorized is False
    assert plan.dq_pit_status == "NOT_EVALUATED_BY_THIS_CONTRACT"
    assert plan.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert plan.selection_authorized is False
    assert plan.orders == plan.fills == 0
    assert QQQOptionsPolicyAdoptionPlan.from_json_bytes(plan.canonical_bytes) == plan


def test_attestation_seal_is_permutation_invariant() -> None:
    first = _attestation()
    second = _attestation(
        group_choices=tuple(reversed(_group_choices())),
        amendment_resolutions=tuple(reversed(_amendments())),
    )

    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.canonical_sha256 == second.canonical_sha256


@pytest.mark.parametrize(
    "owner_id,reviewer_id",
    [
        ("different_owner", "project_owner"),
        ("project_owner", "different_reviewer"),
    ],
)
def test_owner_and_reviewer_identity_mismatch_is_typed(
    owner_id: str,
    reviewer_id: str,
) -> None:
    record = _attestation(
        owner_id=owner_id,
        independent_reviewer_id=reviewer_id,
    )
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="OWNER_IDENTITY_NOT_BOUND",
    ):
        load_owner_decision_attestation(
            record.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


@pytest.mark.parametrize(
    "field",
    [
        "repository_code_sha",
        "pack_requirement_lf_sha256",
        "authority_set_sha256",
        "manifest_policy_file_sha256",
        "manifest_policy_canonical_sha256",
        "slot_catalog_sha256",
    ],
)
def test_wrong_repository_or_authority_hash_fails_closed(field: str) -> None:
    value = "0" * (40 if field == "repository_code_sha" else 64)
    record = _attestation(
        repository_code_sha=(value if field == "repository_code_sha" else _BASE_SHA),
        pack_requirement_lf_sha256=(value if field == "pack_requirement_lf_sha256" else None),
        authority_set_sha256=(value if field == "authority_set_sha256" else None),
        manifest_policy_file_sha256=(value if field == "manifest_policy_file_sha256" else None),
        manifest_policy_canonical_sha256=(
            value if field == "manifest_policy_canonical_sha256" else None
        ),
        slot_catalog_sha256=(value if field == "slot_catalog_sha256" else None),
    )
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="AUTHORITY_BINDING_MISMATCH",
    ):
        load_owner_decision_attestation(
            record.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


def test_missing_or_extra_amendment_resolution_fails_closed() -> None:
    missing = _attestation(amendment_resolutions=_amendments()[:-1])
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="CATALOG_AMENDMENT_REQUIRED",
    ):
        load_owner_decision_attestation(
            missing.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )

    extra = _amendments() + (
        SlotCatalogAmendmentResolution(
            amendment_id="UNKNOWN_VERSIONED_AMENDMENT",
            disposition=SlotCatalogAmendmentDisposition.OWNER_REVIEW_REQUIRED,
            rationale="Synthetic unknown amendment must fail closed.",
            successor_catalog_version=None,
        ),
    )
    record = _attestation(amendment_resolutions=extra)
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="CATALOG_AMENDMENT_REQUIRED",
    ):
        load_owner_decision_attestation(
            record.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


@pytest.mark.parametrize(
    "action,expected_code",
    [
        (OwnerDecisionAction.G2, "G2_METADATA_INCOMPLETE"),
        (OwnerDecisionAction.G5, "G5_RATIONALE_INCOMPLETE"),
    ],
)
def test_g2_and_g5_cannot_omit_required_owner_payload(
    action: OwnerDecisionAction,
    expected_code: str,
) -> None:
    upstream = load_qqq_options_owner_decision_manifest_policy().policy
    selection_slots = tuple(
        slot.slot_id
        for slot in upstream.slots
        if slot.canonical_group is OwnerDecisionCanonicalGroup.SELECTION
    )
    choices = tuple(
        OwnerDecisionSlotChoice(
            slot_id=slot_id,
            action=action if index == 0 else OwnerDecisionAction.G1,
        )
        for index, slot_id in enumerate(selection_slots)
    )
    record = _attestation(
        group_choices=_group_choices(selection=OwnerDecisionGroupMode.PER_SLOT),
        slot_choices=choices,
    )
    with pytest.raises(OwnerDecisionAttestationError, match=expected_code):
        load_owner_decision_attestation(
            record.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


def test_unknown_slot_and_incomplete_per_slot_inventory_are_typed() -> None:
    record = _attestation(
        group_choices=_group_choices(selection=OwnerDecisionGroupMode.PER_SLOT),
        slot_choices=(
            OwnerDecisionSlotChoice(
                slot_id="UNKNOWN_SLOT",
                action=OwnerDecisionAction.G1,
            ),
        ),
    )
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="SLOT_INVENTORY_INVALID",
    ):
        load_owner_decision_attestation(
            record.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


def test_duplicate_key_noncanonical_unknown_field_and_tamper_fail_closed() -> None:
    record = _attestation()
    duplicate = record.canonical_bytes.replace(
        b"{\n",
        b'{\n  "record_id": "duplicate_record",\n',
        1,
    )
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="OWNER_ATTESTATION_PAYLOAD_MISMATCH.*duplicate JSON key",
    ):
        load_owner_decision_attestation(
            duplicate,
            expected_repository_code_sha=_BASE_SHA,
        )

    decoded = json.loads(record.canonical_bytes)
    noncanonical = json.dumps(decoded, sort_keys=False).encode("utf-8")
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="OWNER_ATTESTATION_NOT_CANONICAL",
    ):
        load_owner_decision_attestation(
            noncanonical,
            expected_repository_code_sha=_BASE_SHA,
        )

    decoded["unknown"] = True
    unknown = (json.dumps(decoded, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="OWNER_ATTESTATION_PAYLOAD_MISMATCH",
    ):
        load_owner_decision_attestation(
            unknown,
            expected_repository_code_sha=_BASE_SHA,
        )

    decoded.pop("unknown")
    decoded["content_sha256"] = "0" * 64
    tampered = (json.dumps(decoded, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="semantic content SHA-256 mismatch",
    ):
        load_owner_decision_attestation(
            tampered,
            expected_repository_code_sha=_BASE_SHA,
        )


def test_plan_replay_binds_attestation_policy_and_cash_preservation() -> None:
    record = _attestation()
    admission = load_owner_decision_attestation(
        record.canonical_bytes,
        expected_repository_code_sha=_BASE_SHA,
    )
    plan = build_policy_adoption_plan(admission)
    result = resolve_policy_adoption_plan(
        plan.canonical_bytes,
        expected_repository_code_sha=_BASE_SHA,
        expected_attestation_raw_byte_sha256=record.canonical_sha256,
    )

    assert result.validation_status == "VALID_POLICY_ADOPTION_CONTRACT_ONLY"
    assert result.executable_policy_authorized is False
    assert result.owner_input_blocker_cleared is False
    assert result.selection_authorized is False
    assert result.orders == result.fills == 0
    assert result.production_effect == result.broker_action == "none"
    assert QQQOptionsPolicyAdoptionResolution.from_json_bytes(result.canonical_bytes) == result

    with pytest.raises(
        OwnerDecisionAttestationError,
        match="AUTHORITY_BINDING_MISMATCH",
    ):
        resolve_policy_adoption_plan(
            plan.canonical_bytes,
            expected_repository_code_sha="0" * 40,
            expected_attestation_raw_byte_sha256=record.canonical_sha256,
        )
    with pytest.raises(
        OwnerDecisionAttestationError,
        match="AUTHORITY_BINDING_MISMATCH",
    ):
        resolve_policy_adoption_plan(
            plan.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
            expected_attestation_raw_byte_sha256="0" * 64,
        )


def test_no_real_owner_attestation_or_engine_activation_is_embedded() -> None:
    policy = load_qqq_options_owner_policy_adoption_contract().policy
    assert policy.policy_status == "CONTRACT_ONLY_BLOCKED_OWNER_INPUT"
    assert policy.safety.maximum_adoption_status == ("VALID_POLICY_ADOPTION_CONTRACT_ONLY")
    assert policy.safety.investment_interpretation_allowed is False
    assert policy.safety.paper_allowed is False
    assert policy.safety.live_allowed is False
    assert policy.safety.broker_allowed is False
