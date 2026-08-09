from __future__ import annotations

import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.owner_decision_manifest import (
    DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH,
    OwnerDecisionAction,
    OwnerDecisionCanonicalGroup,
    OwnerDecisionGroupChoice,
    OwnerDecisionGroupMode,
    OwnerDecisionNotApplicableRationale,
    OwnerDecisionSlotChoice,
    OwnerDecisionValueKind,
    OwnerReviewedPolicyValue,
    QQQOptionsOwnerDecisionManifest,
    QQQOptionsOwnerDecisionManifestContractError,
    QQQOptionsOwnerDecisionResolutionResult,
    build_qqq_options_owner_decision_manifest,
    load_qqq_options_owner_decision_manifest_policy,
    resolve_qqq_options_owner_decision_manifest,
)

_BASE_SHA = "2da20cf05ec6d31c2c4cb9d7c6ce797c9128f301"
_DECISION_DATE = date(2026, 8, 9)
_CREATED_AT = datetime(2026, 8, 9, 3, 0, tzinfo=UTC)
_OWNER_DECISION_ID = (
    "owner_decision:TRADING-2502:2026-08-09:"
    "review_qqq_options_backtest_policy_decision_pack_v1"
)
_GROUPS = (
    OwnerDecisionCanonicalGroup.SELECTION,
    OwnerDecisionCanonicalGroup.EXECUTION,
    OwnerDecisionCanonicalGroup.ACCOUNTING,
    OwnerDecisionCanonicalGroup.LIFECYCLE,
    OwnerDecisionCanonicalGroup.ACCEPTANCE,
)
_SELECTION_SLOTS = (
    "SEL_DTE_WINDOW",
    "SEL_MONEYNESS_RANGE",
    "SEL_DELTA_SOURCE_RANGE",
    "SEL_SPREAD_LIMIT",
    "SEL_OPEN_INTEREST_FLOOR",
    "SEL_VOLUME_FLOOR",
    "SEL_QUOTE_FRESHNESS",
    "SEL_RANK_PRIORITY",
)


def _group_choices(
    *,
    selection: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G3,
    execution: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G3,
    accounting: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G3,
    lifecycle: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G3,
    acceptance: OwnerDecisionGroupMode = OwnerDecisionGroupMode.G3,
) -> tuple[OwnerDecisionGroupChoice, ...]:
    modes = (selection, execution, accounting, lifecycle, acceptance)
    return tuple(
        OwnerDecisionGroupChoice(canonical_group=group, mode=mode)
        for group, mode in zip(_GROUPS, modes, strict=True)
    )


def _g2_value() -> OwnerReviewedPolicyValue:
    return OwnerReviewedPolicyValue(
        slot_id="SEL_DTE_WINDOW",
        value_schema_id="dte_window_policy_value.v1",
        value_kind=OwnerDecisionValueKind.RANGE_RULE,
        payload={
            "rule": "TEST_ONLY_OWNER_REVIEWED_RULE",
            "unit": "TEST_ONLY_UNIT",
        },
        owner="project_owner",
        policy_id="test_only_dte_policy",
        policy_version="1.0.0",
        policy_status="OWNER_REVIEWED",
        rationale="Contract fixture only; this is not a strategy threshold.",
        intended_effect="Exercise typed G2 validation without authorizing an engine.",
        evidence_refs=("TEST_ONLY_EVIDENCE",),
        reviewed_at_utc=_CREATED_AT,
        review_condition="Replace fixture with an exact Owner-reviewed successor policy.",
        expires_at_utc=None,
        reviewed_no_expiry_rationale="Test fixture has no operational lifetime.",
    )


def _g5_rationale() -> OwnerDecisionNotApplicableRationale:
    return OwnerDecisionNotApplicableRationale(
        slot_id="SEL_MONEYNESS_RANGE",
        rationale="Contract fixture proves that an explicit rationale is identity-bound.",
        impact_scope="No implicit default and no engine activation are permitted.",
    )


def _mixed_inputs() -> tuple[
    tuple[OwnerDecisionGroupChoice, ...],
    tuple[OwnerDecisionSlotChoice, ...],
    tuple[OwnerReviewedPolicyValue, ...],
    tuple[OwnerDecisionNotApplicableRationale, ...],
]:
    actions = (
        OwnerDecisionAction.G2,
        OwnerDecisionAction.G5,
        OwnerDecisionAction.G1,
        OwnerDecisionAction.G3,
        OwnerDecisionAction.G4,
        OwnerDecisionAction.G1,
        OwnerDecisionAction.G3,
        OwnerDecisionAction.G4,
    )
    slot_choices = tuple(
        OwnerDecisionSlotChoice(slot_id=slot_id, action=action)
        for slot_id, action in zip(_SELECTION_SLOTS, actions, strict=True)
    )
    return (
        _group_choices(selection=OwnerDecisionGroupMode.PER_SLOT),
        slot_choices,
        (_g2_value(),),
        (_g5_rationale(),),
    )


def _build(
    *,
    group_choices: tuple[OwnerDecisionGroupChoice, ...] | None = None,
    slot_choices: tuple[OwnerDecisionSlotChoice, ...] = (),
    owner_policy_values: tuple[OwnerReviewedPolicyValue, ...] = (),
    not_applicable_rationales: tuple[OwnerDecisionNotApplicableRationale, ...] = (),
    project_root: Path = PROJECT_ROOT,
) -> QQQOptionsOwnerDecisionManifest:
    return build_qqq_options_owner_decision_manifest(
        record_id="qqq_options_owner_decision_manifest_test_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha=_BASE_SHA,
        owner_decision_id=_OWNER_DECISION_ID,
        decision_date=_DECISION_DATE,
        independent_reviewer_id="project_owner",
        group_choices=group_choices or _group_choices(),
        slot_choices=slot_choices,
        owner_policy_values=owner_policy_values,
        not_applicable_rationales=not_applicable_rationales,
        project_root=project_root,
    )


def _copy_authority(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    policy = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH
    pack = (
        root
        / "docs/requirements/"
        "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md"
    )
    policy.parent.mkdir(parents=True, exist_ok=True)
    pack.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(PROJECT_ROOT / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH, policy)
    shutil.copy2(
        PROJECT_ROOT
        / "docs/requirements/"
        "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md",
        pack,
    )
    return root


def test_policy_loads_exact_28_slot_catalog_and_real_g_semantics() -> None:
    loaded = load_qqq_options_owner_decision_manifest_policy()

    assert len(loaded.policy.slots) == 28
    assert tuple(item.action.value for item in loaded.policy.action_semantics) == (
        "G1",
        "G2",
        "G3",
        "G4",
        "G5",
    )
    assert tuple(item.semantic for item in loaded.policy.action_semantics) == (
        "KEEP_UNRESOLVED_BLOCKED",
        "OWNER_SUPPLIED_REVIEWED_POLICY",
        "EVIDENCE_CALIBRATION_REQUIRED",
        "SENSITIVITY_ONLY_NOT_REALITY_BASELINE",
        "NOT_APPLICABLE_WITH_REVIEWED_RATIONALE",
    )
    assert loaded.policy_file_sha256 == (
        "55fb29bb2e4347959920cd3f5d72cbc5fc94c2aac5794f301e1c41f9a31547de"
    )
    assert loaded.policy_canonical_sha256 == (
        "c872e9aee37cf2ea36b201d81c48c98603ca4daa96c53d900cdaa5997e13f0db"
    )
    assert loaded.slot_catalog_sha256 == (
        "a1492e27ea8599d453249e5d29280d6fcb882ea0376ce531b949eae7b7621ad6"
    )


def test_acc_prefix_is_not_group_authority() -> None:
    policy = load_qqq_options_owner_decision_manifest_policy().policy
    mapping = {slot.slot_id: slot.canonical_group for slot in policy.slots}

    assert mapping["ACC_FEE_SCHEDULE"] is OwnerDecisionCanonicalGroup.ACCOUNTING
    assert mapping["ACC_RESULT_INCLUSION"] is OwnerDecisionCanonicalGroup.ACCEPTANCE
    assert mapping["ACC_DQ_PIT_REPRO"] is OwnerDecisionCanonicalGroup.ACCEPTANCE


@pytest.mark.parametrize(
    "mode,action",
    [
        (OwnerDecisionGroupMode.G1, OwnerDecisionAction.G1),
        (OwnerDecisionGroupMode.G3, OwnerDecisionAction.G3),
        (OwnerDecisionGroupMode.G4, OwnerDecisionAction.G4),
    ],
)
def test_group_level_modes_materialize_deterministically(
    mode: OwnerDecisionGroupMode, action: OwnerDecisionAction
) -> None:
    manifest = _build(
        group_choices=_group_choices(
            selection=mode,
            execution=mode,
            accounting=mode,
            lifecycle=mode,
            acceptance=mode,
        )
    )

    assert len(manifest.materialized_decisions) == 28
    assert {item.action for item in manifest.materialized_decisions} == {action}
    assert QQQOptionsOwnerDecisionManifest.from_json_bytes(manifest.canonical_bytes) == manifest


def test_mixed_per_slot_manifest_is_total_typed_and_permutation_invariant() -> None:
    groups, slots, values, rationales = _mixed_inputs()
    first = _build(
        group_choices=groups,
        slot_choices=slots,
        owner_policy_values=values,
        not_applicable_rationales=rationales,
    )
    second = _build(
        group_choices=tuple(reversed(groups)),
        slot_choices=tuple(reversed(slots)),
        owner_policy_values=(
            values[0].model_copy(
                update={"payload": dict(reversed(tuple(values[0].payload.items())))}
            ),
        ),
        not_applicable_rationales=tuple(reversed(rationales)),
    )

    assert first == second
    assert first.canonical_bytes == second.canonical_bytes
    assert first.canonical_sha256 == second.canonical_sha256
    assert tuple(item.slot_id for item in first.owner_policy_values) == ("SEL_DTE_WINDOW",)
    assert tuple(item.slot_id for item in first.not_applicable_rationales) == (
        "SEL_MONEYNESS_RANGE",
    )


def test_group_choice_plus_override_fails_closed() -> None:
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="group-level choice cannot have slot overrides",
    ):
        _build(
            slot_choices=(
                OwnerDecisionSlotChoice(
                    slot_id="SEL_DTE_WINDOW", action=OwnerDecisionAction.G1
                ),
            )
        )


def test_per_slot_missing_fails_closed() -> None:
    groups, slots, values, rationales = _mixed_inputs()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="PER_SLOT missing"
    ):
        _build(
            group_choices=groups,
            slot_choices=slots[:-1],
            owner_policy_values=values,
            not_applicable_rationales=rationales,
        )


def test_per_slot_duplicate_fails_closed() -> None:
    groups, slots, values, rationales = _mixed_inputs()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="repeats SEL_DTE_WINDOW"
    ):
        _build(
            group_choices=groups,
            slot_choices=slots + (slots[0],),
            owner_policy_values=values,
            not_applicable_rationales=rationales,
        )


def test_unknown_and_cross_group_slots_fail_closed() -> None:
    groups, slots, values, rationales = _mixed_inputs()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="unknown ids"
    ):
        _build(
            group_choices=groups,
            slot_choices=slots
            + (
                OwnerDecisionSlotChoice(
                    slot_id="SEL_UNKNOWN_SLOT", action=OwnerDecisionAction.G1
                ),
            ),
            owner_policy_values=values,
            not_applicable_rationales=rationales,
        )

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="PER_SLOT missing"
    ):
        _build(
            group_choices=groups,
            slot_choices=slots[:-1]
            + (
                OwnerDecisionSlotChoice(
                    slot_id="EXE_MARKETABLE_LIMIT", action=OwnerDecisionAction.G1
                ),
            ),
            owner_policy_values=values,
            not_applicable_rationales=rationales,
        )


def test_g2_requires_exactly_one_typed_value() -> None:
    groups, slots, _, rationales = _mixed_inputs()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="G2 values expected",
    ):
        _build(
            group_choices=groups,
            slot_choices=slots,
            not_applicable_rationales=rationales,
        )

    value = _g2_value().model_copy(update={"value_kind": OwnerDecisionValueKind.LIMIT_RULE})
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="schema or kind differs",
    ):
        _build(
            group_choices=groups,
            slot_choices=slots,
            owner_policy_values=(value,),
            not_applicable_rationales=rationales,
        )


def test_g2_payload_fields_and_review_expiry_are_strict() -> None:
    groups, slots, _, rationales = _mixed_inputs()
    value = _g2_value().model_copy(update={"payload": {"rule": "TEST_ONLY"}})
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="requires payload fields",
    ):
        _build(
            group_choices=groups,
            slot_choices=slots,
            owner_policy_values=(value,),
            not_applicable_rationales=rationales,
        )

    payload = _g2_value().model_dump(mode="python")
    payload["reviewed_no_expiry_rationale"] = None
    with pytest.raises(ValidationError, match="exactly one"):
        OwnerReviewedPolicyValue.model_validate(payload)


def test_non_g2_value_and_non_g5_rationale_fail_closed() -> None:
    value = _g2_value().model_copy(update={"slot_id": "SEL_DTE_WINDOW"})
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match=r"G2 values expected=\[\]"
    ):
        _build(owner_policy_values=(value,))

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match=r"G5 rationale expected=\[\]"
    ):
        _build(not_applicable_rationales=(_g5_rationale(),))


def test_g5_requires_exactly_one_rationale_and_impact() -> None:
    groups, slots, values, _ = _mixed_inputs()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="G5 rationale expected",
    ):
        _build(
            group_choices=groups,
            slot_choices=slots,
            owner_policy_values=values,
        )

    with pytest.raises(ValidationError, match="non-empty normalized text"):
        OwnerDecisionNotApplicableRationale(
            slot_id="SEL_MONEYNESS_RANGE",
            rationale="valid rationale",
            impact_scope="",
        )


def test_manifest_rejects_duplicate_json_key_noncanonical_bytes_and_tamper() -> None:
    manifest = _build()
    duplicate = manifest.canonical_bytes.replace(
        b"{\n", b'{\n  "record_id": "duplicate_record",\n', 1
    )
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="duplicate JSON key"
    ):
        QQQOptionsOwnerDecisionManifest.from_json_bytes(duplicate)

    decoded = json.loads(manifest.canonical_bytes)
    noncanonical = json.dumps(decoded, sort_keys=False).encode("utf-8")
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="NOT_CANONICAL"
    ):
        QQQOptionsOwnerDecisionManifest.from_json_bytes(noncanonical)

    decoded["content_sha256"] = "0" * 64
    tampered = (json.dumps(decoded, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="semantic content SHA-256"
    ):
        QQQOptionsOwnerDecisionManifest.from_json_bytes(tampered)


def test_manifest_rejects_unknown_field_primary_window_and_token_date_drift() -> None:
    manifest = _build()
    payload = manifest.model_dump(mode="python")
    payload["unknown"] = True
    with pytest.raises(ValidationError, match="extra_forbidden"):
        QQQOptionsOwnerDecisionManifest.model_validate(payload)

    payload = {
        field: getattr(manifest, field)
        for field in QQQOptionsOwnerDecisionManifest.model_fields
        if field != "content_sha256"
    }
    payload["requested_start"] = date(2022, 12, 1)
    with pytest.raises(ValidationError, match="2021-02-22"):
        QQQOptionsOwnerDecisionManifest.seal(**payload)

    payload = {
        field: getattr(manifest, field)
        for field in QQQOptionsOwnerDecisionManifest.model_fields
        if field != "content_sha256"
    }
    payload["decision_date"] = date(2026, 8, 8)
    with pytest.raises(ValidationError, match="token and decision_date"):
        QQQOptionsOwnerDecisionManifest.seal(**payload)


def test_resolver_uses_canonical_fact_and_preserves_cash() -> None:
    manifest = _build()
    result = resolve_qqq_options_owner_decision_manifest(
        manifest.canonical_bytes,
        expected_repository_code_sha=_BASE_SHA,
    )

    assert result.validation_status == "VALID_CONTRACT_ONLY_OWNER_DECISION"
    assert result.calibration_required_slot_ids == tuple(
        slot.slot_id
        for slot in load_qqq_options_owner_decision_manifest_policy().policy.slots
    )
    assert result.dq_pit_status == "NOT_EVALUATED_BY_THIS_CONTRACT"
    assert result.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert result.selection_authorized is False
    assert result.orders == result.fills == 0
    assert result.production_effect == result.broker_action == "none"
    assert QQQOptionsOwnerDecisionResolutionResult.from_json_bytes(
        result.canonical_bytes
    ) == result


def test_resolver_rejects_repository_and_policy_binding_drift() -> None:
    manifest = _build()
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="repository_code_sha",
    ):
        resolve_qqq_options_owner_decision_manifest(
            manifest.canonical_bytes,
            expected_repository_code_sha="0" * 40,
        )

    payload = {
        field: getattr(manifest, field)
        for field in QQQOptionsOwnerDecisionManifest.model_fields
        if field != "content_sha256"
    }
    payload["slot_catalog_sha256"] = "0" * 64
    rebound = QQQOptionsOwnerDecisionManifest.seal(**payload)
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="slot_catalog_sha256",
    ):
        resolve_qqq_options_owner_decision_manifest(
            rebound.canonical_bytes,
            expected_repository_code_sha=_BASE_SHA,
        )


def test_dependency_dag_and_corporate_action_hard_stop_remain_blocked() -> None:
    loaded = load_qqq_options_owner_decision_manifest_policy()
    manifest = _build()
    result = resolve_qqq_options_owner_decision_manifest(
        manifest.canonical_bytes,
        expected_repository_code_sha=_BASE_SHA,
    )

    assert loaded.policy.corporate_action_hard_stop.bypass_allowed is False
    assert loaded.policy.corporate_action_hard_stop.authority_id not in {
        slot.slot_id for slot in loaded.policy.slots
    }
    assert result.corporate_action_status == "HARD_STOP_NOT_A_DECISION_SLOT"
    assert all(
        item.status
        in {
            "NOT_EVALUATED_BY_THIS_CONTRACT",
            "BLOCKED_BY_UNRESOLVED_UPSTREAM",
        }
        for item in result.dependency_audit
    )


def test_policy_pack_hash_drift_and_extra_field_fail_closed(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    pack = (
        root
        / "docs/requirements/"
        "TRADING-2502_QQQ_Options_Owner_Reviewed_Backtest_Policy_Decision_Pack_V1.md"
    )
    pack.write_bytes(pack.read_bytes() + b"drift")
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError,
        match="decision pack LF SHA-256 mismatch",
    ):
        load_qqq_options_owner_decision_manifest_policy(project_root=root)

    root = _copy_authority(tmp_path / "extra")
    policy = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH
    policy.write_text(
        policy.read_text(encoding="utf-8") + "unexpected: true\n",
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="extra_forbidden"
    ):
        load_qqq_options_owner_decision_manifest_policy(project_root=root)


def test_policy_symlink_fails_closed_when_supported(tmp_path: Path) -> None:
    root = _copy_authority(tmp_path)
    policy = root / DEFAULT_QQQ_OPTIONS_OWNER_DECISION_MANIFEST_POLICY_PATH
    target = policy.with_name("policy_target.yaml")
    policy.replace(target)
    try:
        policy.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(
        QQQOptionsOwnerDecisionManifestContractError, match="cannot use a symlink"
    ):
        load_qqq_options_owner_decision_manifest_policy(project_root=root)
