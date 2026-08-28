from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from ai_trading_system.qqq_options_research.exact_signal_implementation_policy_draft import (
    DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_PATH,
    ExactSignalImplementationPolicyDraft,
    ExactSignalImplementationPolicyDraftError,
    load_exact_signal_implementation_policy_draft,
)
from ai_trading_system.qqq_options_research.owner_decision_manifest_v2 import (
    build_qqq_options_owner_decision_catalog_v2_migration_receipt,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict[str, object]:
    path = PROJECT_ROOT / DEFAULT_EXACT_SIGNAL_IMPLEMENTATION_POLICY_DRAFT_PATH
    return load_strict_yaml_text(path.read_text(encoding="utf-8"), label=str(path))


def _v2_slot_ids() -> tuple[str, ...]:
    receipt = build_qqq_options_owner_decision_catalog_v2_migration_receipt(
        record_id="trading_2509_owner_decision_catalog_v2_migration_20260812_v1",
        issued_at_utc=datetime(2026, 8, 12, tzinfo=UTC),
        implementation_repository_code_sha="1d7de7ff08e7253985760eb7e2257f117679b32c",
    )
    return tuple(item.successor_slot_id for item in receipt.successor_slots)


def test_draft_loads_with_owner_freeze_and_backtest_blocked_terminal() -> None:
    result = load_exact_signal_implementation_policy_draft()

    assert result.terminal == "OWNER_EXACT_FREEZE_AND_SIGNAL_PACKAGE_REQUIRED_NO_BACKTEST"
    assert result.file_sha256 == hashlib.sha256(result.path.read_bytes()).hexdigest()
    assert result.canonical_sha256 == result.draft.canonical_sha256
    assert result.draft.status == "OWNER_REVIEW_DRAFT_NON_EXECUTABLE"
    assert result.draft.scope == "NON_EXECUTABLE_DATA_RESEARCH"


def test_source_candidate_is_semantic_only_and_does_not_claim_primary_coverage() -> None:
    source = load_exact_signal_implementation_policy_draft().draft.source_candidate

    assert source.producer_id == "first_layer_composer_v2"
    assert source.source_enum == (
        "risk_on",
        "constructive",
        "neutral",
        "defensive",
        "risk_off",
    )
    assert str(source.documented_actual_start) == "2023-02-22"
    assert source.primary_start_covered is False
    assert source.retained_exact_1202_session_package_present is False
    assert source.poc_rewrap_admissible_as_exact_package is False


def test_mapping_preserves_existing_de_risking_semantics_without_put_alpha() -> None:
    mapping = load_exact_signal_implementation_policy_draft().draft.signal_mapping
    observed = {row.source_state: row.option_action for row in mapping.rows}

    assert observed == {
        "risk_on": "LONG_CALL",
        "constructive": "LONG_CALL",
        "neutral": "FLAT",
        "defensive": "FLAT",
        "risk_off": "FLAT",
    }
    assert mapping.baseline_actions == ("LONG_CALL", "FLAT")
    assert mapping.long_put_baseline_allowed is False
    assert mapping.long_put_role == "SEPARATE_SENSITIVITY_ONLY"
    assert mapping.option_or_result_input_allowed is False


def test_primary_window_is_exactly_the_validated_1202_session_window() -> None:
    window = load_exact_signal_implementation_policy_draft().draft.research_window

    assert str(window.requested_start) == "2021-02-22"
    assert str(window.requested_end) == "2025-12-02"
    assert window.requested_start == window.evaluated_start
    assert window.requested_end == window.evaluated_end
    assert window.expected_session_count == 1202


def test_selection_proposal_is_result_blind_and_never_relaxes_no_contract() -> None:
    selection = load_exact_signal_implementation_policy_draft().draft.selection_proposal

    assert (selection.min_dte_inclusive, selection.target_dte, selection.max_dte_inclusive) == (
        30,
        35,
        45,
    )
    assert str(selection.target_abs_delta) == "0.5"
    assert selection.rank_components[-1] == "SID"
    assert selection.volume_floor_disposition.startswith("PROPOSE_G5_NOT_APPLICABLE")
    assert selection.no_contract_disposition.endswith("NO_RELAXATION")


def test_execution_proposal_keeps_independent_minute_and_nonzero_costs() -> None:
    execution = load_exact_signal_implementation_policy_draft().draft.execution_proposal

    assert execution.submit_time_rule == "NEXT_INDEPENDENT_MINUTE_AFTER_SELECTION"
    assert execution.entry_order == "ASK_SIDE_MARKETABLE_LIMIT"
    assert execution.exit_order == "BID_SIDE_MARKETABLE_LIMIT"
    assert execution.adverse_price_adjustment_per_share_usd > 0
    assert execution.fee_per_contract_per_side_usd > 0
    assert execution.same_session_retry_allowed is False
    assert execution.same_bar_fill_allowed is False


def test_accounting_proposal_caps_premium_and_discloses_idle_cash_comparator() -> None:
    accounting = load_exact_signal_implementation_policy_draft().draft.accounting_proposal

    assert accounting.account_type == "CASH"
    assert accounting.initial_cash_usd == 100000
    assert accounting.premium_budget_fraction_of_pretrade_nav == Decimal("0.02")
    assert accounting.max_open_contracts == 1
    assert accounting.required_platform_multiplier == 100
    assert accounting.idle_cash_carry == "ZERO_RETURN_BASELINE"
    assert accounting.cash_carry_comparator == "SEPARATE_SGOV_CARRY_COMPARATOR"
    assert accounting.margin_allowed is False


def test_lifecycle_proposal_prevents_exercise_assignment_and_atomic_roll() -> None:
    lifecycle = load_exact_signal_implementation_policy_draft().draft.lifecycle_proposal

    assert lifecycle.pre_expiry_guard_xnys_sessions == 7
    assert lifecycle.atomic_or_same_session_roll_allowed is False
    assert lifecycle.fresh_next_session_reentry_allowed is True
    assert lifecycle.exercise_allowed is False
    assert lifecycle.assignment_allowed is False
    assert lifecycle.share_delivery_allowed is False
    assert lifecycle.missing_mark_disposition == "INSUFFICIENT_PLATFORM_EVIDENCE"


def test_all_37_v2_slots_are_present_once_in_canonical_identity_order() -> None:
    slots = load_exact_signal_implementation_policy_draft().draft.slot_proposals
    slot_ids = tuple(item.slot_id for item in slots)

    assert len(slot_ids) == len(set(slot_ids)) == 37
    assert slot_ids == _v2_slot_ids()


def test_only_reviewed_not_applicable_proposals_use_g5() -> None:
    slots = load_exact_signal_implementation_policy_draft().draft.slot_proposals
    g5_ids = {
        item.slot_id
        for item in slots
        if item.proposal_action == "PROPOSE_G5_NOT_APPLICABLE"
    }

    assert g5_ids == {
        "ACC_INVESTMENT_PROMOTION",
        "EXE_PARTIAL_FILL",
        "SEL_VOLUME_FLOOR",
    }
    assert all(item.owner_frozen is False for item in slots)


def test_result_admission_keeps_cash_facts_and_rejects_result_selection() -> None:
    admission = load_exact_signal_implementation_policy_draft().draft.result_admission

    assert admission.no_contract_no_fill_cancel_retained_as_cash_facts is True
    assert admission.invalid_run_in_aggregate_allowed is False
    assert admission.preregistered_baseline_count == 1
    assert admission.sensitivity_result_selection_allowed is False
    assert admission.same_signal_paired_comparator_required is True
    assert admission.maximum_interpretation == "RESEARCH_COMPARISON_ONLY"


def test_safety_surface_authorizes_only_the_static_draft() -> None:
    safety = load_exact_signal_implementation_policy_draft().draft.safety

    assert safety.draft_authorized is True
    assert safety.owner_exact_freeze is False
    assert safety.exact_signal_package_present is False
    assert safety.executable_policy_authorized is False
    assert safety.manifest_generation_authorized is False
    assert safety.real_dq_authorized is False
    assert safety.qc_backtest_authorized is False
    assert safety.qc_project_mutation_authorized is False
    assert safety.provider_query_authorized is False
    assert safety.orders == safety.fills == safety.positions == 0
    assert safety.paper_allowed is safety.live_allowed is safety.broker_allowed is False


def test_model_rejects_long_put_in_baseline_mapping() -> None:
    payload = deepcopy(_payload())
    payload["signal_mapping"]["rows"][4]["option_action"] = "LONG_PUT"

    with pytest.raises(ValueError, match="call-or-flat mapping drifted"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)


def test_model_rejects_missing_duplicate_or_reordered_slots() -> None:
    payload = deepcopy(_payload())
    payload["slot_proposals"].pop()
    with pytest.raises(ValueError, match="37-slot successor inventory"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)

    payload = deepcopy(_payload())
    payload["slot_proposals"][1] = deepcopy(payload["slot_proposals"][0])
    with pytest.raises(ValueError, match="37-slot successor inventory"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)

    payload = deepcopy(_payload())
    payload["slot_proposals"][0], payload["slot_proposals"][1] = (
        payload["slot_proposals"][1],
        payload["slot_proposals"][0],
    )
    with pytest.raises(ValueError, match="37-slot successor inventory"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)


def test_model_rejects_wrong_slot_action_reference_and_executable_flag() -> None:
    payload = deepcopy(_payload())
    payload["slot_proposals"][0]["proposal_action"] = "PROPOSE_G5_NOT_APPLICABLE"
    with pytest.raises(ValueError, match="slot action drifted"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)

    payload = deepcopy(_payload())
    payload["slot_proposals"][0]["proposal_ref"] = "engine.default"
    with pytest.raises(ValueError, match="slot proposal reference drifted"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)

    payload = deepcopy(_payload())
    payload["safety"]["executable_policy_authorized"] = True
    with pytest.raises(ValueError, match="Input should be False"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)


def test_loader_rejects_authority_hash_drift() -> None:
    payload = deepcopy(_payload())
    payload["authority_bindings"][0]["file_sha256"] = "0" * 64

    with TemporaryDirectory(dir=PROJECT_ROOT / "outputs") as directory:
        path = Path(directory) / "draft.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with pytest.raises(
            ExactSignalImplementationPolicyDraftError,
            match="file SHA-256 mismatch",
        ):
            load_exact_signal_implementation_policy_draft(path=path)


def test_model_rejects_extra_fields_and_loader_rejects_path_escape(tmp_path: Path) -> None:
    payload = deepcopy(_payload())
    payload["unexpected"] = True
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        ExactSignalImplementationPolicyDraft.model_validate(payload)

    outside = tmp_path / "outside.yaml"
    outside.write_text("schema_version: invalid\n", encoding="utf-8")
    with pytest.raises(
        ExactSignalImplementationPolicyDraftError,
        match="escapes project root",
    ):
        load_exact_signal_implementation_policy_draft(path=outside)


def test_canonical_round_trip_preserves_non_executable_owner_review_state() -> None:
    draft = load_exact_signal_implementation_policy_draft().draft
    replay = ExactSignalImplementationPolicyDraft.model_validate(
        json.loads(draft.canonical_bytes)
    )

    assert replay.canonical_bytes == draft.canonical_bytes
    assert replay.canonical_sha256 == draft.canonical_sha256
    text = draft.canonical_bytes.decode("utf-8")
    assert '"owner_exact_freeze": false' in text
    assert '"qc_backtest_authorized": false' in text
    assert '"orders": 0' in text
