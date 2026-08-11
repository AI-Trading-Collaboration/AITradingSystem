from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    OwnerDecisionAction,
    OwnerDecisionCanonicalGroup,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest_v2 import (
    DEFAULT_QQQ_OPTIONS_OWNER_DECISION_ATTESTATION_PATH,
    DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH,
    CatalogMutationKind,
    EvidenceDQStatus,
    OwnerPolicyEvidenceReference,
    QQQOptionsOwnerDecisionCatalogV2MigrationReceipt,
    QQQOptionsOwnerDecisionManifestV2ContractError,
    ReviewFindingDisposition,
    SuccessorPolicyState,
    build_qqq_options_owner_decision_catalog_v2_migration_receipt,
    load_qqq_options_owner_decision_manifest_v2_policy,
    resolve_qqq_options_owner_decision_catalog_v2_migration_receipt,
)
from ai_trading_system.qqq_options_research.owner_policy_adoption_contract import (
    OwnerDecisionAttestationRecord,
)

_BASE_SHA = "1d7de7ff08e7253985760eb7e2257f117679b32c"
_REVIEWED_SHA = "e08bca3d22c1174e3dc31c14e2a4416ea809c440"
_ATTESTATION_SHA = "8345a55a73df022ef70cb57d6d8df4d6c498cafb091647ef8e27c835cde6fccc"
_ATTESTATION_CONTENT_SHA = (
    "2777768003bb81bdeadc72929edeae9db6f1a1d970b25ebf9e050adafa30b57c"
)
_POLICY_FILE_SHA = "d4f7fb3ffb196ce65000ec24fc302c44395a9d3c4dad3e2e5554683639f9ca79"
_POLICY_CANONICAL_SHA = (
    "9ac542b464ba4417d67fb626dc820d2e7e331c3c154951590fdf7a409ab67272"
)
_ISSUED_AT = datetime(2026, 8, 12, 0, 0, tzinfo=UTC)

_AUTHORITY_PATHS = (
    DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH,
    DEFAULT_QQQ_OPTIONS_OWNER_DECISION_ATTESTATION_PATH,
    Path("config/research/qqq_options_owner_decision_manifest_v1.yaml"),
    Path("config/research/qqq_options_owner_policy_adoption_contract_v1.yaml"),
    Path(
        "docs/requirements/"
        "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md"
    ),
)


def _copy_authority(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    for relative in _AUTHORITY_PATHS:
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(PROJECT_ROOT / relative, target)
    return root


def _receipt(
    *, project_root: Path = PROJECT_ROOT
) -> QQQOptionsOwnerDecisionCatalogV2MigrationReceipt:
    return build_qqq_options_owner_decision_catalog_v2_migration_receipt(
        record_id="trading_2509_owner_decision_catalog_v2_migration_20260812_v1",
        issued_at_utc=_ISSUED_AT,
        implementation_repository_code_sha=_BASE_SHA,
        project_root=project_root,
    )


def test_tracked_attestation_is_canonical_and_exactly_bound() -> None:
    raw = (PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_ATTESTATION_PATH).read_bytes()
    record = OwnerDecisionAttestationRecord.from_json_bytes(raw)

    assert hashlib.sha256(raw).hexdigest() == record.canonical_sha256 == _ATTESTATION_SHA
    assert record.content_sha256 == _ATTESTATION_CONTENT_SHA
    assert record.repository_code_sha == _REVIEWED_SHA
    assert record.owner_id == record.independent_reviewer_id == "project_owner"
    assert record.owner_policy_values == ()
    assert record.not_applicable_rationales == ()


def test_v2_policy_loads_exact_authority() -> None:
    loaded = load_qqq_options_owner_decision_manifest_v2_policy()

    assert loaded.policy_file_sha256 == _POLICY_FILE_SHA
    assert loaded.policy_canonical_sha256 == _POLICY_CANONICAL_SHA
    assert loaded.policy.policy_version == "2.0.0"
    assert loaded.policy.predecessor.reviewed_main_sha == _REVIEWED_SHA
    assert loaded.policy.primary_research_start == date(2021, 2, 22)


def test_v2_receipt_has_exact_37_slot_inventory() -> None:
    receipt = _receipt()

    assert len(receipt.successor_slots) == 37
    assert len({item.successor_slot_id for item in receipt.successor_slots}) == 37
    counts = {
        group: sum(item.canonical_group is group for item in receipt.successor_slots)
        for group in OwnerDecisionCanonicalGroup
    }
    assert counts == {
        OwnerDecisionCanonicalGroup.SELECTION: 8,
        OwnerDecisionCanonicalGroup.EXECUTION: 7,
        OwnerDecisionCanonicalGroup.ACCOUNTING: 9,
        OwnerDecisionCanonicalGroup.LIFECYCLE: 7,
        OwnerDecisionCanonicalGroup.ACCEPTANCE: 6,
    }


def test_v2_mutation_inventory_is_exact() -> None:
    receipt = _receipt()
    counts = {
        kind: sum(item.mutation_kind is kind for item in receipt.successor_slots)
        for kind in CatalogMutationKind
    }
    assert counts == {
        CatalogMutationKind.UNCHANGED: 24,
        CatalogMutationKind.SPLIT_SUCCESSOR: 8,
        CatalogMutationKind.ADDED_AXIS: 5,
    }


def test_split_successors_inherit_blocked_source_actions() -> None:
    split = {
        item.successor_slot_id: item
        for item in _receipt().successor_slots
        if item.mutation_kind is CatalogMutationKind.SPLIT_SUCCESSOR
    }
    assert set(split) == {
        "ACC_COST_BASIS_CONVENTION",
        "ACC_EVENT_IDENTITY_INVARIANT",
        "ACC_ROUNDING_RECONCILIATION",
        "ACC_SETTLEMENT_TIMING",
        "LIFE_ASSIGNMENT_POLICY",
        "LIFE_CLOSE_HOLD_POLICY",
        "LIFE_EXERCISE_POLICY",
        "LIFE_ROLL_AUTHORIZATION",
    }
    assert {item.inherited_owner_action for item in split.values()} == {
        OwnerDecisionAction.G1
    }
    assert {item.successor_policy_state for item in split.values()} == {
        SuccessorPolicyState.INHERITED_BLOCKED_ACTION
    }


def test_added_axes_do_not_receive_an_implicit_owner_action() -> None:
    added = [
        item
        for item in _receipt().successor_slots
        if item.mutation_kind is CatalogMutationKind.ADDED_AXIS
    ]
    assert {item.successor_slot_id for item in added} == {
        "ACC_CASH_CARRY_BENCHMARK",
        "ACC_METRIC_BENCHMARK_IDENTITY",
        "ACC_RESEARCH_MULTIPLICITY_CONTROL",
        "EXE_EXECUTION_OBSERVATION_SOURCE",
        "LIFE_POSITION_STATE_TRANSITION",
    }
    assert all(item.inherited_owner_action is None for item in added)
    assert all(
        item.successor_policy_state is SuccessorPolicyState.OWNER_ACTION_UNRESOLVED
        for item in added
    )


def test_quote_observation_identities_are_separate() -> None:
    slots = {item.successor_slot_id: item for item in _receipt().successor_slots}
    assert (
        slots["SEL_QUOTE_FRESHNESS"].quote_observation_identity
        == "SELECTION_QUOTE_OBSERVATION"
    )
    assert (
        slots["EXE_QUOTE_DISPOSITION"].quote_observation_identity
        == "EXECUTION_QUOTE_OBSERVATION"
    )
    assert (
        slots["SEL_QUOTE_FRESHNESS"].quote_observation_identity
        != slots["EXE_QUOTE_DISPOSITION"].quote_observation_identity
    )


def test_terminal_valuation_is_an_explicit_result_inclusion_dependency() -> None:
    slots = {item.successor_slot_id: item for item in _receipt().successor_slots}
    assert "LIFE_TERMINAL_VALUATION" in slots["ACC_RESULT_INCLUSION"].requires


def test_web_pro_additional_findings_are_not_misreported_as_accepted() -> None:
    receipt = _receipt()
    finding_ids = tuple(item.finding_id for item in receipt.review_findings)

    assert finding_ids == (
        "SPLIT_EXE_CANCEL_REJECT_NO_FILL",
        "SPLIT_ACC_DQ_PIT_REPRO_INVARIANT_FROM_OWNER_POLICY",
    )
    assert all(
        item.disposition
        is ReviewFindingDisposition.OWNER_REVIEW_REQUIRED_NOT_IN_ATTESTED_INVENTORY
        for item in receipt.review_findings
    )
    assert not set(finding_ids).intersection(receipt.accepted_amendment_ids)
    slot_ids = {item.successor_slot_id for item in receipt.successor_slots}
    assert {"EXE_CANCEL_REJECT_NO_FILL", "ACC_DQ_PIT_REPRO"}.issubset(slot_ids)


def test_migration_round_trip_is_canonical_and_contract_only() -> None:
    receipt = _receipt()
    resolution = resolve_qqq_options_owner_decision_catalog_v2_migration_receipt(
        receipt.canonical_bytes,
        expected_implementation_repository_code_sha=_BASE_SHA,
    )

    assert resolution.validation_status == "VALID_VERSIONED_SUCCESSOR_CONTRACT_ONLY"
    assert resolution.owner_policy_value_count == 0
    assert resolution.executable_policy_authorized is False
    assert resolution.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert resolution.selection_authorized is False
    assert resolution.orders == resolution.fills == 0


def test_repeated_builds_are_byte_identical() -> None:
    expected = _receipt().canonical_bytes
    assert all(_receipt().canonical_bytes == expected for _ in range(10))


def test_receipt_repository_mismatch_fails_closed() -> None:
    receipt = _receipt()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="repository identity",
    ):
        resolve_qqq_options_owner_decision_catalog_v2_migration_receipt(
            receipt.canonical_bytes,
            expected_implementation_repository_code_sha="0" * 40,
        )


def test_noncanonical_receipt_bytes_fail_closed() -> None:
    raw = _receipt().canonical_bytes.replace(b"\n", b"\r\n")
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="V2_RECORD_NOT_CANONICAL",
    ):
        QQQOptionsOwnerDecisionCatalogV2MigrationReceipt.from_json_bytes(raw)


def test_duplicate_receipt_json_key_fails_closed() -> None:
    raw = _receipt().canonical_bytes
    duplicated = raw.replace(
        b'{\n  "accepted_amendment_ids"',
        b'{\n  "accepted_amendment_ids": [],\n  "accepted_amendment_ids"',
        1,
    )
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="duplicate JSON key",
    ):
        QQQOptionsOwnerDecisionCatalogV2MigrationReceipt.from_json_bytes(duplicated)


def test_attestation_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    path = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_ATTESTATION_PATH
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["record_id"] = "tampered_record"
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="V2_AUTHORITY_BINDING_MISMATCH",
    ):
        load_qqq_options_owner_decision_manifest_v2_policy(project_root=root)


def test_unknown_split_source_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    path = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace(
            "source_slot_id: ACC_IDENTITY_ROUNDING",
            "source_slot_id: UNKNOWN_SOURCE_SLOT",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="unknown v1 slot",
    ):
        load_qqq_options_owner_decision_manifest_v2_policy(project_root=root)


def test_stale_unchanged_inventory_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    path = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace("  - ACC_CASH_RESERVATION\n", "", 1),
        encoding="utf-8",
    )

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestV2ContractError,
        match="unchanged v1 slot inventory drifted",
    ):
        load_qqq_options_owner_decision_manifest_v2_policy(project_root=root)


def test_dependency_cycle_fails_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    path = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_V2_POLICY_PATH
    text = path.read_text(encoding="utf-8")
    path.write_text(
        text.replace(
            "upstream_slot_id: LIFE_TERMINAL_VALUATION",
            "upstream_slot_id: ACC_RESULT_INCLUSION",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="dependency cycle"):
        build_qqq_options_owner_decision_catalog_v2_migration_receipt(
            record_id="cycle_test",
            issued_at_utc=_ISSUED_AT,
            implementation_repository_code_sha=_BASE_SHA,
            project_root=root,
        )


def test_unknown_dq_evidence_never_passes() -> None:
    with pytest.raises(ValidationError, match="only canonical DQ PASS"):
        OwnerPolicyEvidenceReference(
            relative_path="outputs/evidence.json",
            schema_version="evidence.v1",
            file_sha256="1" * 64,
            content_sha256="2" * 64,
            requested_start=date(2021, 2, 22),
            requested_end=date(2021, 2, 26),
            evaluated_start=date(2021, 2, 22),
            evaluated_end=date(2021, 2, 26),
            as_of_session=date(2021, 2, 26),
            dq_status=EvidenceDQStatus.UNKNOWN,
        )


def test_pre_window_primary_evidence_never_passes() -> None:
    with pytest.raises(ValidationError, match="requested_start"):
        OwnerPolicyEvidenceReference(
            relative_path="outputs/evidence.json",
            schema_version="evidence.v1",
            file_sha256="1" * 64,
            content_sha256="2" * 64,
            requested_start=date(2021, 2, 19),
            requested_end=date(2021, 2, 26),
            evaluated_start=date(2021, 2, 22),
            evaluated_end=date(2021, 2, 26),
            as_of_session=date(2021, 2, 26),
            dq_status=EvidenceDQStatus.PASS,
        )


def test_valid_primary_evidence_reference_is_typed_but_not_adopted() -> None:
    evidence = OwnerPolicyEvidenceReference(
        relative_path="outputs/evidence.json",
        schema_version="evidence.v1",
        file_sha256="1" * 64,
        content_sha256="2" * 64,
        requested_start=date(2021, 2, 22),
        requested_end=date(2021, 2, 26),
        evaluated_start=date(2021, 2, 22),
        evaluated_end=date(2021, 2, 26),
        as_of_session=date(2021, 2, 26),
        dq_status=EvidenceDQStatus.PASS,
    )
    assert evidence.dq_status is EvidenceDQStatus.PASS
    assert _receipt().policy_evidence == ()


def test_v1_authority_bytes_remain_immutable() -> None:
    policy = PROJECT_ROOT / "config/research/qqq_options_owner_decision_manifest_v1.yaml"
    assert hashlib.sha256(policy.read_bytes()).hexdigest() == (
        "55fb29bb2e4347959920cd3f5d72cbc5fc94c2aac5794f301e1c41f9a31547de"
    )


def test_attestation_exact_owner_actions_are_preserved() -> None:
    loaded = load_qqq_options_owner_decision_manifest_v2_policy()
    actions = {item.slot_id: item.action for item in loaded.attestation.slot_choices}
    assert actions["ACC_INITIAL_CASH"] is OwnerDecisionAction.G4
    assert actions["EXE_SLIPPAGE"] is OwnerDecisionAction.G4
    assert actions["EXE_MARKETABLE_LIMIT"] is OwnerDecisionAction.G3
    assert actions["LIFE_CLOSE_HOLD_ROLL"] is OwnerDecisionAction.G1
    assert len(actions) == 20


def test_safety_boundary_never_authorizes_execution_or_external_action() -> None:
    receipt = _receipt()
    assert receipt.executable_policy_authorized is False
    assert receipt.dq_pit_status == "NOT_EVALUATED_BY_THIS_CONTRACT"
    assert receipt.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert receipt.selection_authorized is False
    assert receipt.orders == receipt.fills == 0
    assert receipt.external_action_authorized is False
    assert receipt.investment_interpretation_allowed is False
    assert receipt.paper_allowed is receipt.live_allowed is receipt.broker_allowed is False
    assert receipt.production_effect == receipt.broker_action == "none"
