from __future__ import annotations

import copy
import hashlib
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    exact_signal_implementation_policy_freeze_admission as admission,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

ExactSignalImplementationPolicyFreezeAdmission = (
    admission.ExactSignalImplementationPolicyFreezeAdmission
)
load_exact_signal_implementation_policy_freeze_admission = (
    admission.load_exact_signal_implementation_policy_freeze_admission
)

_PATH = PROJECT_ROOT / (
    "config/research/qc_qqq_options_exact_signal_implementation_policy_freeze_admission_v1.yaml"
)
_DRAFT_PATH = PROJECT_ROOT / (
    "config/research/qc_qqq_options_exact_signal_implementation_policy_draft_v1.yaml"
)
_DRAFT_FILE_SHA256 = "22335aa324ffb13c9917b65ad57f51916831ecd95c05fe357f7faa13f74b57d0"
_DRAFT_CANONICAL_SHA256 = "45c247010f47ad3172215f90aa7c9cd40044b5332284e1789095d230075a5d83"


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def test_freeze_admission_replays_exact_approved_draft_identity() -> None:
    loaded = load_exact_signal_implementation_policy_freeze_admission()

    assert loaded.terminal == ("OWNER_EXACT_POLICY_FROZEN_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST")
    assert loaded.draft.file_sha256 == hashlib.sha256(_DRAFT_PATH.read_bytes()).hexdigest()
    assert loaded.draft.file_sha256 == _DRAFT_FILE_SHA256
    assert loaded.draft.canonical_sha256 == _DRAFT_CANONICAL_SHA256
    assert loaded.policy.authority_binding.file_sha256 == _DRAFT_FILE_SHA256
    assert loaded.policy.authority_binding.canonical_sha256 == _DRAFT_CANONICAL_SHA256


def test_whole_draft_mapping_and_all_37_slots_are_exact_frozen() -> None:
    loaded = load_exact_signal_implementation_policy_freeze_admission()
    policy = loaded.policy

    assert policy.owner_decision.exact_draft_freeze_granted
    assert policy.owner_decision.whole_draft_surface_frozen
    assert policy.owner_decision.all_37_successor_slots_frozen
    assert policy.frozen_surface.frozen_slot_count == 37
    assert len(policy.frozen_surface.frozen_slot_ids) == 37
    assert policy.frozen_surface.frozen_slot_ids == tuple(
        row.slot_id for row in loaded.draft.draft.slot_proposals
    )
    assert tuple(
        (row.source_state, row.option_action) for row in policy.frozen_surface.signal_mapping_rows
    ) == (
        ("risk_on", "LONG_CALL"),
        ("constructive", "LONG_CALL"),
        ("neutral", "FLAT"),
        ("defensive", "FLAT"),
        ("risk_off", "FLAT"),
    )
    assert not policy.frozen_surface.long_put_baseline_allowed


def test_approved_draft_bytes_remain_review_draft_and_are_not_rewritten() -> None:
    loaded = load_exact_signal_implementation_policy_freeze_admission()
    draft = loaded.draft.draft

    assert draft.status == "OWNER_REVIEW_DRAFT_NON_EXECUTABLE"
    assert not draft.safety.owner_exact_freeze
    assert not any(row.owner_frozen for row in draft.slot_proposals)
    assert loaded.policy.frozen_surface.draft_status_preserved == draft.status


def test_signal_package_and_every_external_or_execution_gate_remain_closed() -> None:
    policy = load_exact_signal_implementation_policy_freeze_admission().policy
    package = policy.signal_package_state
    safety = policy.safety

    assert not package.exact_1202_session_package_present
    assert not package.exact_1202_session_package_admitted
    assert package.observed_package_sha256 is None
    assert not package.manifest_replay_executed
    assert package.blockers == (
        "SOURCE_PRIMARY_START_NOT_COVERED",
        "EXACT_1202_SESSION_PACKAGE_MISSING",
        "CODE_CONFIG_INPUT_DQ_PIT_IDENTITY_MISSING",
        "REAL_DQ_NOT_AUTHORIZED",
    )
    assert safety.signal_package_preparation_authorized
    closed_flags = (
        safety.signal_package_generation_authorized,
        safety.exact_signal_package_admission_allowed,
        safety.executable_policy_authorized,
        safety.r1_manifest_generation_authorized,
        safety.real_dq_authorized,
        safety.qc_backtest_authorized,
        safety.qc_project_mutation_authorized,
        safety.provider_query_authorized,
        safety.raw_option_payload_download_or_export_allowed,
        safety.parameter_or_threshold_search_allowed,
        safety.paper_allowed,
        safety.live_allowed,
    )
    assert not any(closed_flags)
    assert (safety.orders, safety.fills, safety.positions) == (0, 0, 0)
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"


@pytest.mark.parametrize(
    "field",
    (
        "exact_draft_freeze_granted",
        "five_state_mapping_frozen",
        "all_37_successor_slots_frozen",
        "whole_draft_surface_frozen",
        "predecessor_bytes_must_remain_immutable",
    ),
)
def test_owner_freeze_cannot_be_partially_admitted(field: str) -> None:
    payload = _payload()
    mutated = copy.deepcopy(payload)
    mutated["owner_decision"][field] = False

    with pytest.raises(ValidationError):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(mutated)


def test_mapping_or_slot_inventory_drift_fails_closed() -> None:
    payload = _payload()
    mapping_drift = copy.deepcopy(payload)
    mapping_drift["frozen_surface"]["signal_mapping_rows"][3]["option_action"] = "LONG_CALL"
    with pytest.raises(ValidationError, match="five-state frozen mapping drifted"):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(mapping_drift)

    missing_slot = copy.deepcopy(payload)
    missing_slot["frozen_surface"]["frozen_slot_ids"].pop()
    with pytest.raises(ValidationError, match="all 37 successor slots"):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(missing_slot)

    duplicate_slot = copy.deepcopy(payload)
    duplicate_slot["frozen_surface"]["frozen_slot_ids"][-1] = duplicate_slot["frozen_surface"][
        "frozen_slot_ids"
    ][0]
    with pytest.raises(ValidationError, match="duplicates"):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(duplicate_slot)


def test_draft_hash_or_source_readiness_drift_fails_closed() -> None:
    payload = _payload()
    hash_drift = copy.deepcopy(payload)
    hash_drift["authority_binding"]["canonical_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="approved draft identity drifted"):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(hash_drift)

    source_drift = copy.deepcopy(payload)
    source_drift["signal_package_state"]["documented_source_start"] = "2021-02-22"
    with pytest.raises(ValidationError, match="exact signal readiness facts drifted"):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(source_drift)


@pytest.mark.parametrize(
    "field",
    (
        "signal_package_generation_authorized",
        "exact_signal_package_admission_allowed",
        "executable_policy_authorized",
        "r1_manifest_generation_authorized",
        "real_dq_authorized",
        "qc_backtest_authorized",
        "qc_project_mutation_authorized",
        "provider_query_authorized",
        "raw_option_payload_download_or_export_allowed",
        "parameter_or_threshold_search_allowed",
        "paper_allowed",
        "live_allowed",
    ),
)
def test_closed_gate_cannot_be_enabled(field: str) -> None:
    payload = _payload()
    mutated = copy.deepcopy(payload)
    mutated["safety"][field] = True

    with pytest.raises(ValidationError):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(mutated)


def test_unknown_field_fails_closed() -> None:
    payload = _payload()
    payload["safety"]["backdoor"] = True

    with pytest.raises(ValidationError):
        ExactSignalImplementationPolicyFreezeAdmission.model_validate(payload)
