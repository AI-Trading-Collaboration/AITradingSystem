from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.owner_stage_gate_signoff import (
    DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH,
    DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH,
    DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH,
    DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_SIGNOFF_PATH,
    EXPECTED_AXIS_DECISIONS,
    EXPECTED_UNKNOWN_IDS,
    OWNER_STAGE_GATE_DECISION_ID,
    OwnerStageGateAxis,
    OwnerStageGateDecision,
    QCQQQOptionsOwnerStageGateContractError,
    QCQQQOptionsOwnerStageGateOwnerAttestationRecord,
    QCQQQOptionsOwnerStageGateProposalRecord,
    build_qc_qqq_options_owner_stage_gate_proposal,
    build_qc_qqq_options_owner_stage_gate_signoff,
    load_qc_qqq_options_owner_stage_gate_policy,
)

_BASE_SHA = "c7c087388c0309d4e41f826d2d8aa29f3fb0e5e4"
_CREATED_AT = datetime(2026, 8, 6, 0, 0, tzinfo=UTC)
_AUTHORITY_PATHS = (
    "src/ai_trading_system/qqq_options_research/bounded_cloud_pilot_platform_action.py",
    "config/research/qc_qqq_options_bounded_cloud_pilot_platform_action_authorization_v1.yaml",
    "inputs/external_validation/qc_qqq_options_bounded_cloud_pilot_evidence_20260805.json",
    "inputs/external_validation/qc_qqq_options_bounded_cloud_pilot_review_20260805.json",
    "inputs/external_validation/qc_qqq_options_bounded_cloud_pilot_owner_attestation_20260805.json",
)


def _proposal(*, project_root: Path = PROJECT_ROOT) -> QCQQQOptionsOwnerStageGateProposalRecord:
    return build_qc_qqq_options_owner_stage_gate_proposal(
        record_id="qc_qqq_options_owner_stage_gate_proposal_20260806_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha=_BASE_SHA,
        project_root=project_root,
    )


def _copy_project_authority(tmp_path: Path) -> Path:
    root = tmp_path / "project"
    paths = (DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH.as_posix(),) + (_AUTHORITY_PATHS)
    for relative in paths:
        source = PROJECT_ROOT / relative
        destination = root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return root


def _attestation(
    proposal: QCQQQOptionsOwnerStageGateProposalRecord,
    *,
    proposal_sha256: str,
) -> QCQQQOptionsOwnerStageGateOwnerAttestationRecord:
    return QCQQQOptionsOwnerStageGateOwnerAttestationRecord.seal(
        schema_version="qc_qqq_options_owner_stage_gate_owner_attestation.v1",
        record_id="qc_qqq_options_owner_stage_gate_owner_attestation_20260806_v1",
        owner_decision_id=OWNER_STAGE_GATE_DECISION_ID,
        decision_date=date(2026, 8, 6),
        proposal_path=DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH.as_posix(),
        proposal_file_sha256=proposal_sha256,
        proposal_content_sha256=proposal.content_sha256,
        policy_file_sha256=proposal.policy_file_sha256,
        policy_canonical_sha256=proposal.policy_canonical_sha256,
        authority_set_sha256=proposal.authority_set_sha256,
        signer_id="project_owner",
        independent_reviewer_id="project_owner",
        accepted_axis_decisions=tuple(
            f"{axis.value}={decision.value}" for axis, decision in EXPECTED_AXIS_DECISIONS
        ),
        accepted_aggregate_recommendation="NO_GO_KEEP_BLOCKED",
        confirmed_scope_violation=True,
        confirmed_shared_2489_2490_blocked=True,
        confirmed_no_range_expansion=True,
        confirmed_no_further_cloud_action=True,
        confirmed_no_paid_upgrade_authorization=True,
        confirmed_no_investment_interpretation=True,
        confirmed_no_external_action=True,
    )


def test_policy_loads_and_exact_binds_predecessor_authority() -> None:
    loaded = load_qc_qqq_options_owner_stage_gate_policy()

    assert loaded.policy_sha256 == (
        "5bcfe8d29a70e79f110972d5b1df4fd6f013b0de5b6cb706220e40e09d8b51ff"
    )
    assert loaded.policy_canonical_sha256 == (
        "7e637c0eb6070e07e70d8b3d72789a37d646965c323cf95267c6f4799cfac238"
    )
    assert loaded.authority_set_sha256 == (
        "0659a92c7de22202a1cba493c74cedaa86ea9e9bccf1238275b51ce18fc118fe"
    )
    assert tuple(item.authority_id for item in loaded.policy.authority_bindings) == (
        "TRADING_2492_PLATFORM_ACTION_MODULE",
        "TRADING_2492_PLATFORM_AUTHORIZATION_POLICY",
        "TRADING_2492_EXECUTION_EVIDENCE",
        "TRADING_2492_REVIEW_REQUEST",
        "TRADING_2492_OWNER_REVIEW",
    )


def test_proposal_derives_exact_axis_decisions_and_unknown_exit_conditions() -> None:
    proposal = _proposal()

    assert tuple((item.axis_id, item.decision) for item in proposal.axis_decisions) == (
        EXPECTED_AXIS_DECISIONS
    )
    assert tuple(item.unknown_id for item in proposal.unknowns) == EXPECTED_UNKNOWN_IDS
    assert all(item.status == "UNKNOWN_BLOCKS_GO" for item in proposal.unknowns)
    assert all(item.owner and item.exit_condition for item in proposal.unknowns)
    assert proposal.aggregate_recommendation == "NO_GO_KEEP_BLOCKED"
    assert proposal.owner_signoff_status == "PENDING_OWNER_SIGNATURE"
    assert proposal.owner_signoff_completed is False
    assert proposal.terminal_stage_gate_issued is False


def test_proposal_preserves_scope_dq_pit_and_shared_blockers() -> None:
    proposal = _proposal()

    assert proposal.confirmed_processed_data_points == 734127
    assert proposal.confirmed_reviewed_cap == 250000
    assert proposal.confirmed_scope_violation is True
    assert proposal.option_event_dq_status == "PASS_PLATFORM_LOG_ONLY"
    assert proposal.option_event_pit_status == "PASS_PLATFORM_LOG_ONLY"
    assert proposal.confirmed_shared_2489_2490_blocked is True
    decisions = {item.axis_id: item.decision for item in proposal.axis_decisions}
    assert decisions[OwnerStageGateAxis.DQ_PIT] == OwnerStageGateDecision.NO_GO
    assert decisions[OwnerStageGateAxis.RESOURCE_BUDGET] == OwnerStageGateDecision.NO_GO
    assert decisions[OwnerStageGateAxis.SHARED_RECONCILIATION] == (OwnerStageGateDecision.NO_GO)


def test_conditional_capability_cannot_activate_range_paid_or_external_actions() -> None:
    proposal = _proposal()
    decisions = {item.axis_id: item.decision for item in proposal.axis_decisions}

    assert decisions[OwnerStageGateAxis.PLATFORM_CAPABILITY] == (
        OwnerStageGateDecision.CONDITIONAL_GO
    )
    assert decisions[OwnerStageGateAxis.TECHNICAL_CORRECTNESS] == (
        OwnerStageGateDecision.CONDITIONAL_GO
    )
    assert decisions[OwnerStageGateAxis.RANGE_EXPANSION] == OwnerStageGateDecision.NO_GO
    assert decisions[OwnerStageGateAxis.PAID_TIER_UPGRADE] == OwnerStageGateDecision.NO_GO
    assert proposal.safety.range_expansion_allowed is False
    assert proposal.safety.further_cloud_action_authorized is False
    assert proposal.safety.paid_tier_upgrade_authorized is False
    assert proposal.safety.investment_interpretation_allowed is False
    assert proposal.safety.broker_action == "none"


def test_proposal_is_deterministic_canonical_and_golden() -> None:
    first = _proposal()
    second = _proposal()

    assert first.canonical_bytes == second.canonical_bytes
    assert first.canonical_sha256 == (
        "19f964e09f613b680eb5be65423c45a56271a5dc00c9ce196e97c3718d0ab611"
    )
    assert first.content_sha256 == (
        "1129fa54c35f7bb137e6d3ab605e0cf670383246a0e91888862edbb3e9ebe947"
    )
    assert len(first.canonical_bytes) == 8836
    assert QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(first.canonical_bytes) == first


def test_tracked_proposal_exactly_replays_implementation_commit() -> None:
    raw = (PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH).read_bytes()
    tracked = QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(raw)
    expected = build_qc_qqq_options_owner_stage_gate_proposal(
        record_id="qc_qqq_options_owner_stage_gate_proposal_20260806_v1",
        created_at_utc=_CREATED_AT,
        repository_code_sha="3e56172cbc09bc9dabef9cc77cd40edf18c83b9b",
    )

    assert tracked == expected
    assert tracked.canonical_bytes == raw
    assert tracked.canonical_sha256 == (
        "c638f0e75ad8faaa27df91e8c0710a4202e4e29229e1f13202de6514b184d3ef"
    )
    assert tracked.content_sha256 == (
        "1a8da99b91245cf88d568d51d313d59986d5618a3e52b6a41d1cb562c7c58d89"
    )


def test_proposal_rejects_noncanonical_and_semantic_hash_tamper() -> None:
    proposal = _proposal()
    decoded = json.loads(proposal.canonical_bytes)
    noncanonical = json.dumps(decoded, sort_keys=False).encode("utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(noncanonical)

    decoded["content_sha256"] = "0" * 64
    tampered = (json.dumps(decoded, indent=2, sort_keys=True) + "\n").encode("utf-8")
    with pytest.raises(ValueError, match="semantic content SHA-256 mismatch"):
        QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(tampered)


def test_proposal_model_rejects_forged_go_axis() -> None:
    payload = _proposal().model_dump(mode="json")
    payload["axis_decisions"][0]["decision"] = "GO"

    with pytest.raises(ValidationError, match="proposal axis decisions drifted"):
        QCQQQOptionsOwnerStageGateProposalRecord.model_validate(payload)


def test_policy_hash_tamper_fails_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    evidence = root / _AUTHORITY_PATHS[2]
    evidence.write_bytes(evidence.read_bytes() + b" ")

    with pytest.raises(
        QCQQQOptionsOwnerStageGateContractError,
        match="TRADING_2492_EXECUTION_EVIDENCE SHA-256 mismatch",
    ):
        load_qc_qqq_options_owner_stage_gate_policy(project_root=root)


def test_policy_path_escape_and_extra_field_fail_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    policy_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_POLICY_PATH
    original = policy_path.read_text(encoding="utf-8")
    escaped = original.replace(
        "src/ai_trading_system/qqq_options_research/bounded_cloud_pilot_platform_action.py",
        "../outside.py",
    )
    policy_path.write_text(escaped, encoding="utf-8", newline="\n")
    with pytest.raises(QCQQQOptionsOwnerStageGateContractError, match="normalized"):
        load_qc_qqq_options_owner_stage_gate_policy(project_root=root)

    policy_path.write_text(original + "unexpected: true\n", encoding="utf-8", newline="\n")
    with pytest.raises(QCQQQOptionsOwnerStageGateContractError, match="extra_forbidden"):
        load_qc_qqq_options_owner_stage_gate_policy(project_root=root)


def test_symlink_authority_fails_closed_when_supported(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    evidence = root / _AUTHORITY_PATHS[2]
    target = evidence.with_name("evidence_target.json")
    evidence.replace(target)
    try:
        evidence.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")

    with pytest.raises(QCQQQOptionsOwnerStageGateContractError, match="cannot use a symlink"):
        load_qc_qqq_options_owner_stage_gate_policy(project_root=root)


def test_terminal_signoff_requires_tracked_owner_attestation(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    with pytest.raises(
        QCQQQOptionsOwnerStageGateContractError,
        match="QC_QQQ_OPTIONS_OWNER_STAGE_GATE_ATTESTATION_REQUIRED",
    ):
        build_qc_qqq_options_owner_stage_gate_signoff(
            record_id="qc_qqq_options_owner_stage_gate_signoff_20260806_v1",
            issued_at_utc=_CREATED_AT,
            repository_code_sha=_BASE_SHA,
            project_root=root,
        )


def test_owner_attestation_rejects_wrong_decision_or_axis() -> None:
    proposal = _proposal()
    proposal_sha = hashlib.sha256(proposal.canonical_bytes).hexdigest()
    payload = _attestation(proposal, proposal_sha256=proposal_sha).model_dump(mode="json")
    payload["accepted_axis_decisions"][0] = "PLATFORM_CAPABILITY=GO"
    with pytest.raises(ValidationError, match="axis decisions drifted"):
        QCQQQOptionsOwnerStageGateOwnerAttestationRecord.model_validate(payload)

    payload = _attestation(proposal, proposal_sha256=proposal_sha).model_dump(mode="json")
    payload["owner_decision_id"] = (
        "owner_decision:TRADING-2493:2026-08-06:accept_range_expansion_v1"
    )
    with pytest.raises(ValidationError, match="literal_error"):
        QCQQQOptionsOwnerStageGateOwnerAttestationRecord.model_validate(payload)


def test_exact_owner_attestation_builds_signed_no_go_record(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    proposal = _proposal(project_root=root)
    proposal_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH
    proposal_path.parent.mkdir(parents=True, exist_ok=True)
    proposal_path.write_bytes(proposal.canonical_bytes)
    proposal_sha = hashlib.sha256(proposal.canonical_bytes).hexdigest()
    attestation = _attestation(proposal, proposal_sha256=proposal_sha)
    attestation_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH
    attestation_path.write_bytes(attestation.canonical_bytes)

    signoff = build_qc_qqq_options_owner_stage_gate_signoff(
        record_id="qc_qqq_options_owner_stage_gate_signoff_20260806_v1",
        issued_at_utc=_CREATED_AT,
        repository_code_sha=_BASE_SHA,
        project_root=root,
    )

    assert signoff.owner_decision_id == OWNER_STAGE_GATE_DECISION_ID
    assert signoff.aggregate_decision == "NO_GO_KEEP_BLOCKED"
    assert signoff.signoff_status == "SIGNED_NO_GO"
    assert signoff.source_pilot_disposition == "PILOT_NO_GO_LICENSE_OR_EVIDENCE"
    assert signoff.authority_set_sha256 == proposal.authority_set_sha256
    assert signoff.safety.further_cloud_action_authorized is False


def test_owner_attestation_binding_mismatch_fails_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    proposal = _proposal(project_root=root)
    proposal_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH
    proposal_path.parent.mkdir(parents=True, exist_ok=True)
    proposal_path.write_bytes(proposal.canonical_bytes)
    attestation = _attestation(proposal, proposal_sha256="0" * 64)
    attestation_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH
    attestation_path.write_bytes(attestation.canonical_bytes)

    with pytest.raises(
        QCQQQOptionsOwnerStageGateContractError,
        match="ATTESTATION_BINDING_MISMATCH",
    ):
        build_qc_qqq_options_owner_stage_gate_signoff(
            record_id="qc_qqq_options_owner_stage_gate_signoff_20260806_v1",
            issued_at_utc=_CREATED_AT,
            repository_code_sha=_BASE_SHA,
            project_root=root,
        )


def test_owner_attestation_authority_set_mismatch_fails_closed(tmp_path: Path) -> None:
    root = _copy_project_authority(tmp_path)
    proposal = _proposal(project_root=root)
    proposal_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH
    proposal_path.parent.mkdir(parents=True, exist_ok=True)
    proposal_path.write_bytes(proposal.canonical_bytes)
    proposal_sha = hashlib.sha256(proposal.canonical_bytes).hexdigest()
    payload = _attestation(proposal, proposal_sha256=proposal_sha).model_dump(mode="json")
    payload["authority_set_sha256"] = "0" * 64
    attestation = QCQQQOptionsOwnerStageGateOwnerAttestationRecord.seal(**payload)
    attestation_path = root / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH
    attestation_path.write_bytes(attestation.canonical_bytes)

    with pytest.raises(
        QCQQQOptionsOwnerStageGateContractError,
        match="ATTESTATION_BINDING_MISMATCH",
    ):
        build_qc_qqq_options_owner_stage_gate_signoff(
            record_id="qc_qqq_options_owner_stage_gate_signoff_20260806_v1",
            issued_at_utc=_CREATED_AT,
            repository_code_sha=_BASE_SHA,
            project_root=root,
        )


def test_tracked_owner_attestation_and_signoff_are_canonical_and_bound() -> None:
    proposal_raw = (
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_PROPOSAL_PATH
    ).read_bytes()
    proposal = QCQQQOptionsOwnerStageGateProposalRecord.from_json_bytes(proposal_raw)
    attestation_raw = (
        PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_OWNER_ATTESTATION_PATH
    ).read_bytes()
    attestation = QCQQQOptionsOwnerStageGateOwnerAttestationRecord.from_json_bytes(attestation_raw)
    signoff_path = PROJECT_ROOT / DEFAULT_QC_QQQ_OPTIONS_OWNER_STAGE_GATE_SIGNOFF_PATH
    signoff_raw = signoff_path.read_bytes()

    expected = build_qc_qqq_options_owner_stage_gate_signoff(
        record_id="qc_qqq_options_owner_stage_gate_signoff_20260806_v1",
        issued_at_utc=datetime(2026, 8, 6, 13, 57, 58, tzinfo=UTC),
        repository_code_sha="afe58d615c09b10a43cc27547848122aa400a258",
    )
    assert attestation.proposal_file_sha256 == hashlib.sha256(proposal_raw).hexdigest()
    assert attestation.proposal_content_sha256 == proposal.content_sha256
    assert attestation.authority_set_sha256 == proposal.authority_set_sha256
    assert expected.canonical_bytes == signoff_raw
    assert expected.canonical_sha256 == hashlib.sha256(signoff_raw).hexdigest()
