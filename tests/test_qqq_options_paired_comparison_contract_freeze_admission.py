from __future__ import annotations

import copy
import hashlib
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    paired_comparison_contract_freeze_admission as admission,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PairedComparisonContractFreezeAdmission = admission.PairedComparisonContractFreezeAdmission
load_paired_comparison_contract_freeze_admission = (
    admission.load_paired_comparison_contract_freeze_admission
)

_PATH = PROJECT_ROOT / (
    "config/research/qc_qqq_options_paired_comparison_contract_freeze_admission_v1.yaml"
)
_CONTRACT_PATH = PROJECT_ROOT / (
    "config/research/qc_qqq_options_paired_comparison_contract_v1.yaml"
)
_CONTRACT_FILE_SHA256 = "8c748634f6869eb4d4e9dfb14493acd072d146074ce7e86462eec0adae15714a"
_CONTRACT_CANONICAL_SHA256 = (
    "6f77cf17af6e435799a2e86e1fb6a81936368e053b2367efb3a8e2be13412267"
)


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def test_freeze_admission_replays_exact_approved_contract_identity() -> None:
    loaded = load_paired_comparison_contract_freeze_admission()

    assert loaded.terminal == (
        "OWNER_PAIRED_COMPARATOR_CONTRACT_EXACT_FROZEN_NO_SUCCESSOR_AUTHORITY"
    )
    assert loaded.contract.file_sha256 == hashlib.sha256(_CONTRACT_PATH.read_bytes()).hexdigest()
    assert loaded.contract.file_sha256 == _CONTRACT_FILE_SHA256
    assert loaded.contract.canonical_sha256 == _CONTRACT_CANONICAL_SHA256
    assert loaded.admission.authority_binding.file_sha256 == _CONTRACT_FILE_SHA256
    assert loaded.admission.authority_binding.canonical_sha256 == _CONTRACT_CANONICAL_SHA256


def test_whole_contract_comparator_estimands_and_axes_are_exact_frozen() -> None:
    loaded = load_paired_comparison_contract_freeze_admission()
    surface = loaded.admission.frozen_surface

    assert loaded.admission.owner_decision.exact_contract_freeze_granted
    assert loaded.admission.owner_decision.full_contract_surface_frozen
    assert surface.whole_contract_frozen
    assert surface.primary_comparator_method == "SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT"
    assert surface.primary_estimand_view == "COMMON_CAPITAL_ACCOUNT_VIEW"
    assert surface.secondary_view == "CAPITAL_AT_RISK_TIME_VIEW"
    assert surface.named_diagnostic_ids == ("SGOV_CARRY_COMPARATOR", "QQQ_BUY_AND_HOLD")
    assert len(surface.calendar_partition_ids) == 5
    assert len(surface.falsification_axis_ids) == 16
    assert surface.falsification_axis_ids[-1] == "EXTERNAL_AUTHORIZATION"


def test_approved_contract_bytes_remain_draft_and_are_not_rewritten() -> None:
    loaded = load_paired_comparison_contract_freeze_admission()
    contract = loaded.contract.contract

    assert contract.status == "STATIC_CONTRACT_READY_OWNER_EXACT_FREEZE_REQUIRED"
    assert contract.safety.owner_exact_frozen is False
    assert loaded.admission.frozen_surface.draft_owner_exact_frozen_flag_preserved is False
    assert loaded.admission.frozen_surface.owner_exact_frozen_via_separate_admission is True


def test_existing_result_and_every_successor_or_external_gate_remain_closed() -> None:
    policy = load_paired_comparison_contract_freeze_admission().admission
    successor = policy.successor_state
    safety = policy.safety

    assert successor.paired_comparator_outcome == "INSUFFICIENT_PLATFORM_EVIDENCE"
    assert successor.empirical_comparison_completed is False
    assert successor.successor_task_implicitly_created is False
    assert successor.next_legal_action == (
        "OWNER_SEPARATE_SUCCESSOR_SCOPE_REQUIRED_NO_AUTOMATIC_FOLLOW_ON"
    )
    closed_flags = (
        safety.comparator_contract_mutation_allowed,
        safety.qc_exporter_implementation_authorized,
        safety.local_result_admission_implementation_authorized,
        safety.run_manifest_generation_authorized,
        safety.real_dq_authorized,
        safety.quantconnect_save_authorized,
        safety.quantconnect_build_authorized,
        safety.quantconnect_backtest_authorized,
        safety.quantconnect_retry_authorized,
        safety.provider_query_or_purchase_authorized,
        safety.raw_option_payload_download_or_export_allowed,
        safety.object_store_write_allowed,
        safety.public_share_allowed,
        safety.parameter_or_threshold_search_allowed,
        safety.investment_conclusion_generated,
        safety.paper_allowed,
        safety.live_allowed,
        safety.production_allowed,
        safety.broker_allowed,
    )
    assert not any(closed_flags)
    assert (
        safety.orders_outside_qc_simulation,
        safety.fills_outside_qc_simulation,
        safety.positions_outside_qc_simulation,
    ) == (0, 0, 0)
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"


@pytest.mark.parametrize(
    "field",
    (
        "exact_contract_freeze_granted",
        "full_contract_surface_frozen",
        "predecessor_bytes_must_remain_immutable",
    ),
)
def test_owner_freeze_cannot_be_partially_admitted(field: str) -> None:
    mutated = copy.deepcopy(_payload())
    mutated["owner_decision"][field] = False

    with pytest.raises(ValidationError):
        PairedComparisonContractFreezeAdmission.model_validate(mutated)


def test_instruction_hash_or_frozen_surface_drift_fails_closed() -> None:
    instruction_drift = copy.deepcopy(_payload())
    instruction_drift["owner_decision"]["approved_instruction"] += "继续运行回测。"
    with pytest.raises(ValidationError, match="owner approved instruction drifted"):
        PairedComparisonContractFreezeAdmission.model_validate(instruction_drift)

    hash_drift = copy.deepcopy(_payload())
    hash_drift["authority_binding"]["canonical_sha256"] = "0" * 64
    with pytest.raises(ValidationError, match="approved comparator contract identity drifted"):
        PairedComparisonContractFreezeAdmission.model_validate(hash_drift)

    axis_drift = copy.deepcopy(_payload())
    axis_drift["frozen_surface"]["falsification_axis_ids"].pop()
    with pytest.raises(ValidationError, match="falsification axis freeze inventory drifted"):
        PairedComparisonContractFreezeAdmission.model_validate(axis_drift)


@pytest.mark.parametrize(
    "field",
    (
        "comparator_contract_mutation_allowed",
        "qc_exporter_implementation_authorized",
        "local_result_admission_implementation_authorized",
        "run_manifest_generation_authorized",
        "real_dq_authorized",
        "quantconnect_save_authorized",
        "quantconnect_build_authorized",
        "quantconnect_backtest_authorized",
        "quantconnect_retry_authorized",
        "provider_query_or_purchase_authorized",
        "raw_option_payload_download_or_export_allowed",
        "object_store_write_allowed",
        "public_share_allowed",
        "parameter_or_threshold_search_allowed",
        "investment_conclusion_generated",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    ),
)
def test_closed_gate_cannot_be_enabled(field: str) -> None:
    mutated = copy.deepcopy(_payload())
    mutated["safety"][field] = True

    with pytest.raises(ValidationError):
        PairedComparisonContractFreezeAdmission.model_validate(mutated)


def test_unknown_field_fails_closed() -> None:
    payload = _payload()
    payload["safety"]["backdoor"] = True

    with pytest.raises(ValidationError):
        PairedComparisonContractFreezeAdmission.model_validate(payload)
