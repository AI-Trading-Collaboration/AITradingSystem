from __future__ import annotations

import hashlib
import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    bounded_cloud_pilot_owner_review as owner_review_module,
)
from ai_trading_system.qqq_options_research.bounded_cloud_pilot import (
    load_qc_qqq_options_bounded_cloud_pilot_policy,
)
from ai_trading_system.qqq_options_research.bounded_cloud_pilot_owner_review import (
    DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH,
    QQQOptionsBoundedPilotOwnerReviewContractError,
    QQQOptionsBoundedPilotOwnerReviewPack,
    QQQOptionsBoundedPilotOwnerReviewProposal,
    build_qc_qqq_options_bounded_cloud_pilot_owner_review_pack,
    load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

ROOT = Path(__file__).resolve().parents[1]
AT = datetime(2026, 8, 4, tzinfo=UTC)
BASE_SHA = "d1c45decf8d41fb0ef47b0db8f9868263f2e7c45"
PROPOSAL_SHA256 = "9b3e50731663871e01626f0360c717ecdd14278c63f81e74ed79c4c2fd4041de"
AUTHORITY_SET_SHA256 = "69578c198823b95ba16b5f6c2780c3a7e24104babe2c6cc1fed8cd740c446bea"
BLOCKED_POLICY_SHA256 = "60ed5237fc37e4d44737fe295f4d341a58d318ecad59f8cdf753a0486609f66e"
REVIEW_FILE_SHA256 = "a5c9b9357e2b50a7f69d2710b35f184829917414f0dc8e297709f2fbf14c4ca3"
EVIDENCE_FILE_SHA256 = "2d4c14e23d8b8f824d5b4f93db257f6d4852af31a12966535d21cc5d26a4807a"
BLOCKERS = (
    "OWNER_AUTHORIZATION_NOT_GRANTED",
    "PROPOSAL_REVIEW_NOT_COMPLETED",
    "PRIOR_CAPABILITY_ADMISSION_REMAINS_BLOCKED",
    "OPTION_EVENT_DQ_PIT_NOT_EVALUATED",
)


def _loaded():
    return load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
        project_root=ROOT
    )


def _pack(**overrides: object) -> QQQOptionsBoundedPilotOwnerReviewPack:
    payload: dict[str, object] = {
        "pack_id": "trading_2492_owner_review_pack_v1",
        "created_at_utc": AT,
        "repository_code_sha": BASE_SHA,
        "project_root": ROOT,
    }
    payload.update(overrides)
    return build_qc_qqq_options_bounded_cloud_pilot_owner_review_pack(**payload)


def _policy_payload() -> dict[str, object]:
    path = ROOT / DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH
    payload = load_strict_yaml_text(path.read_text(encoding="utf-8"), label="test")
    assert isinstance(payload, dict)
    return payload


def test_proposal_exactly_freezes_owner_review_only_boundary_and_hashes() -> None:
    loaded = _loaded()
    proposal = loaded.proposal

    assert loaded.proposal_policy_sha256 == PROPOSAL_SHA256
    assert loaded.authority_set_sha256 == AUTHORITY_SET_SHA256
    assert proposal.status == "PROPOSED_OWNER_REVIEW_REQUIRED"
    assert proposal.owner_instruction == "OWNER_ALLOWED_COORDINATOR_TO_SELECT_PROPOSED_VALUES"
    assert proposal.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert proposal.decision == "OWNER_REVIEW_REQUIRED_NO_EXTERNAL_ACTION"
    assert proposal.proposal_expires_on.isoformat() == "2026-08-12"
    assert proposal.safety.proposal_only is True
    assert proposal.safety.pilot_authorized is False
    assert proposal.safety.external_platform_action_allowed is False
    assert proposal.safety.project_mutation_allowed is False
    assert proposal.safety.cloud_run_allowed is False
    assert proposal.safety.order_creation_allowed is False
    assert proposal.safety.fill_creation_allowed is False
    assert proposal.safety.production_effect == "none"
    assert proposal.safety.broker_action == "none"


def test_proposal_binds_exact_2480_review_evidence_and_frozen_2492_authority() -> None:
    loaded = _loaded()
    bindings = loaded.proposal.authority_bindings

    assert len(bindings) == 8
    assert tuple(item.authority_id for item in bindings) == tuple(
        sorted(item.authority_id for item in bindings)
    )
    for binding in bindings:
        content = (ROOT / binding.path).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == binding.sha256
    assert loaded.blocked_policy.policy_sha256 == BLOCKED_POLICY_SHA256
    assert loaded.capability_review.review_file_sha256 == REVIEW_FILE_SHA256
    assert loaded.capability_review.review.evidence_file_sha256 == EVIDENCE_FILE_SHA256


def test_accepted_discovery_review_is_not_reinterpreted_as_pilot_admission() -> None:
    loaded = _loaded()
    review = loaded.capability_review.review
    blocked = loaded.blocked_policy.policy

    assert review.review_decision == (
        "ACCEPTED_WITH_DISCLOSED_POST_TERMINAL_ARTIFACT_DOWNLOAD"
    )
    assert review.prior_admission_decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert review.bounded_pilot_preparation_allowed is False
    assert review.option_event_dq_status == "NOT_EVALUATED"
    assert review.option_event_pit_status == "NOT_EVALUATED"
    assert review.safety.selection_or_pilot_activated is False
    assert blocked.status == "BLOCKED_OWNER_INPUT"
    assert blocked.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"


def test_window_is_single_confirmed_smoke_session_and_not_primary_default() -> None:
    window = _loaded().proposal.research_window

    assert window.primary_research_start.isoformat() == "2021-02-22"
    assert window.requested_start.isoformat() == "2025-12-02"
    assert window.requested_end == window.requested_start
    assert window.expected_evaluated_start == window.requested_start
    assert window.expected_evaluated_end == window.requested_end
    assert window.role == "BOUNDED_PLATFORM_SMOKE_NOT_RESEARCH_CONCLUSION"
    assert window.dq_caveat == "SINGLE_CONFIRMED_ENTITLEMENT_SESSION_NOT_RESEARCH_WINDOW"
    assert window.legacy_non_default_start.isoformat() == "2022-12-01"
    assert window.legacy_non_default_start_is_default is False


def test_platform_selection_and_order_bounds_are_deliberately_small() -> None:
    proposal = _loaded().proposal
    platform = proposal.platform_scope
    selection = proposal.selection_scope

    assert platform.account_tier == "FREE"
    assert platform.cloud_compute == "Community B-MICRO"
    assert platform.maximum_project_mutation_count == 1
    assert platform.maximum_cloud_backtest_count == 1
    assert platform.maximum_runtime_seconds == 300
    assert platform.maximum_processed_data_points == 250000
    assert platform.maximum_order_count == 1
    assert platform.maximum_contract_quantity == 1
    assert selection.technical_direction == "LONG_CALL"
    assert (selection.minimum_dte, selection.target_dte, selection.maximum_dte) == (
        7,
        14,
        21,
    )
    assert selection.minimum_absolute_delta == "0.30"
    assert selection.target_absolute_delta == "0.40"
    assert selection.maximum_absolute_delta == "0.55"
    assert selection.maximum_relative_spread == "0.20"
    assert selection.minimum_open_interest == 10
    assert selection.minimum_volume == 0
    assert selection.rank_components[-1] == "STABLE_SID"


def test_execution_is_next_independent_minute_and_zero_slippage_is_only_sensitivity() -> None:
    execution = _loaded().proposal.execution_scope

    assert execution.submission_timing == "NEXT_INDEPENDENT_MINUTE_AFTER_INTENT"
    assert execution.fill_timing == "NEXT_INDEPENDENT_MINUTE_AFTER_SUBMISSION"
    assert execution.submission_latency_ms == 60000
    assert execution.fill_latency_ms == 60000
    assert execution.reality_slippage_per_share_usd == "0.01"
    assert execution.zero_slippage_isolation_sensitivity_per_share_usd == "0.00"
    assert execution.zero_slippage_is_reality_baseline is False
    assert execution.partial_fill_policy == (
        "PRESERVE_PARTIAL_AND_CANCEL_REMAINDER_AFTER_TIMEOUT"
    )
    assert execution.stale_missing_crossed_quote_disposition == (
        "NO_FILL_CANCEL_CASH_PRESERVATION"
    )


def test_accounting_lifecycle_and_reconciliation_are_explicit_temporary_values() -> None:
    proposal = _loaded().proposal

    assert proposal.accounting_scope.approved_initial_cash_usd == "100000.00"
    assert proposal.accounting_scope.premium_budget_usd == "2000.00"
    assert proposal.accounting_scope.maximum_contracts_per_order == 1
    assert proposal.accounting_scope.cost_basis_method == "FIFO"
    assert proposal.accounting_scope.include_fees_in_cost_basis is True
    assert proposal.accounting_scope.rounding_mode == "ROUND_HALF_EVEN"
    assert proposal.lifecycle_scope.pre_expiry_guard_sessions == 2
    assert proposal.lifecycle_scope.scope_violation_disposition == (
        "INVALIDATE_RUN_CASH_PRESERVATION"
    )
    assert proposal.reconciliation_scope.monetary_tolerance_usd == "0.01"
    assert proposal.reconciliation_scope.price_tolerance_usd == "0.01"
    assert proposal.reconciliation_scope.timestamp_tolerance_seconds == 60


def test_evidence_boundary_requires_distinct_reviewer_and_no_raw_rows() -> None:
    evidence = _loaded().proposal.evidence_scope

    assert evidence.collector_id == "codex_pilot_coordinator"
    assert evidence.independent_reviewer_id == "project_owner"
    assert evidence.collector_id != evidence.independent_reviewer_id
    assert evidence.two_person_attestation_required is True
    assert evidence.manual_bundle_required is True
    assert evidence.result_mapping_ids == (
        "logs",
        "orders_csv",
        "project_files",
        "report_pdf",
        "results_json",
        "trades_csv",
    )
    assert evidence.aggregate_export_safe_artifacts_only is True
    assert evidence.raw_option_rows_allowed is False


def test_owner_review_pack_is_canonical_cash_preserving_and_no_action() -> None:
    pack = _pack()

    assert pack.proposal_policy_sha256 == PROPOSAL_SHA256
    assert pack.proposal_authority_set_sha256 == AUTHORITY_SET_SHA256
    assert pack.blocked_2492_policy_sha256 == BLOCKED_POLICY_SHA256
    assert pack.capability_review_file_sha256 == REVIEW_FILE_SHA256
    assert pack.capability_evidence_file_sha256 == EVIDENCE_FILE_SHA256
    assert pack.bounded_pilot_preparation_allowed_by_capability_review is False
    assert pack.prior_capability_admission_decision == "CAPABILITY_OR_LICENSE_BLOCKED"
    assert pack.option_event_dq_status == "NOT_EVALUATED"
    assert pack.option_event_pit_status == "NOT_EVALUATED"
    assert pack.owner_authorization_token == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    assert pack.decision == "OWNER_REVIEW_REQUIRED_NO_EXTERNAL_ACTION"
    assert pack.blocking_reason_codes == BLOCKERS
    assert pack.cash_preservation_required is True
    assert pack.order_count == 0
    assert pack.fill_count == 0
    assert pack.external_action_executed is False
    assert pack.pilot_authorized is False
    assert pack.range_expansion_allowed is False


def test_owner_review_pack_round_trips_only_from_canonical_bytes() -> None:
    pack = _pack()

    assert QQQOptionsBoundedPilotOwnerReviewPack.from_json_bytes(pack.canonical_bytes()) == pack
    pretty = json.dumps(json.loads(pack.canonical_bytes()), indent=2, sort_keys=True).encode()
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="NONCANONICAL",
    ):
        QQQOptionsBoundedPilotOwnerReviewPack.from_json_bytes(pretty)

    payload = json.loads(pack.canonical_bytes())
    payload["pilot_authorized"] = True
    tampered = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="RECORD_INVALID",
    ):
        QQQOptionsBoundedPilotOwnerReviewPack.from_json_bytes(tampered)


def test_builder_has_no_caller_authorization_or_activation_input() -> None:
    parameters = inspect.signature(
        build_qc_qqq_options_bounded_cloud_pilot_owner_review_pack
    ).parameters

    assert "owner_authorization_token" not in parameters
    assert "pilot_authorized" not in parameters
    assert "external_action_executed" not in parameters
    assert "order_count" not in parameters
    assert "fill_count" not in parameters


def test_invalid_repository_sha_and_timestamps_fail_closed() -> None:
    with pytest.raises(ValidationError, match="Git SHA"):
        _pack(repository_code_sha="not-a-sha")
    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        _pack(created_at_utc=datetime(2026, 8, 4))
    with pytest.raises(ValidationError, match="future"):
        _pack(created_at_utc=datetime.now(UTC) + timedelta(days=1))


@pytest.mark.parametrize(
    ("section", "field", "value", "message"),
    [
        (None, "status", "OWNER_REVIEWED", "literal_error"),
        (None, "owner_authorization_token", "FORGED_TOKEN", "literal_error"),
        ("research_window", "primary_research_start", "2022-12-01", "2021-02-22"),
        ("research_window", "requested_start", "2025-12-03", "2025-12-02"),
        ("platform_scope", "maximum_cloud_backtest_count", 2, "literal_error"),
        ("platform_scope", "maximum_order_count", 2, "literal_error"),
        ("selection_scope", "technical_direction", "SHORT_CALL", "literal_error"),
        ("selection_scope", "minimum_dte", 0, "literal_error"),
        ("execution_scope", "fill_timing", "SAME_BAR", "literal_error"),
        ("execution_scope", "zero_slippage_is_reality_baseline", True, "literal_error"),
        ("safety", "cloud_run_allowed", True, "literal_error"),
        ("safety", "order_creation_allowed", True, "literal_error"),
        ("safety", "raw_options_data_export_allowed", True, "literal_error"),
    ],
)
def test_unreviewed_activation_window_and_numeric_changes_fail_closed(
    section: str | None,
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _policy_payload()
    target = payload if section is None else payload[section]
    assert isinstance(target, dict)
    target[field] = value

    with pytest.raises(ValidationError, match=message):
        QQQOptionsBoundedPilotOwnerReviewProposal.model_validate(payload, strict=False)


def test_rank_permutation_result_inventory_and_same_reviewer_fail_closed() -> None:
    payload = _policy_payload()
    selection = payload["selection_scope"]
    evidence = payload["evidence_scope"]
    assert isinstance(selection, dict)
    assert isinstance(evidence, dict)

    rank_components = selection["rank_components"]
    assert isinstance(rank_components, list)
    selection["rank_components"] = list(reversed(rank_components))
    with pytest.raises(ValidationError, match="rank components"):
        QQQOptionsBoundedPilotOwnerReviewProposal.model_validate(payload, strict=False)

    payload = _policy_payload()
    evidence = payload["evidence_scope"]
    assert isinstance(evidence, dict)
    evidence["result_mapping_ids"] = ["results_json"]
    with pytest.raises(ValidationError, match="result mapping inventory"):
        QQQOptionsBoundedPilotOwnerReviewProposal.model_validate(payload, strict=False)

    payload = _policy_payload()
    evidence = payload["evidence_scope"]
    assert isinstance(evidence, dict)
    evidence["independent_reviewer_id"] = evidence["collector_id"]
    with pytest.raises(ValidationError, match="must differ"):
        QQQOptionsBoundedPilotOwnerReviewProposal.model_validate(payload, strict=False)


def test_authority_hash_drift_and_path_escape_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = owner_review_module._lf_sha256_path
    target = ROOT / "src/ai_trading_system/contracts/qc_qqq_options_capability_discovery_review.py"

    def drifted(path: Path) -> str:
        if path == target:
            return "f" * 64
        return original(path)

    monkeypatch.setattr(owner_review_module, "_lf_sha256_path", drifted)
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="authority hash drifted",
    ):
        _loaded()

    monkeypatch.setattr(owner_review_module, "_lf_sha256_path", original)
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="escapes the project root",
    ):
        load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
            Path("../outside.yaml"),
            project_root=ROOT,
        )


def test_missing_and_symlink_policy_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="must be a regular file",
    ):
        load_qc_qqq_options_bounded_cloud_pilot_owner_review_proposal(
            Path("config/research/not-present.yaml"),
            project_root=ROOT,
        )

    target = ROOT / DEFAULT_QC_QQQ_OPTIONS_BOUNDED_CLOUD_PILOT_OWNER_REVIEW_PROPOSAL_PATH
    original = Path.is_symlink

    def fake_is_symlink(path: Path) -> bool:
        return path == target or original(path)

    monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
    with pytest.raises(
        QQQOptionsBoundedPilotOwnerReviewContractError,
        match="cannot use a symlink",
    ):
        _loaded()


def test_same_inputs_have_stable_policy_scope_and_pack_identity() -> None:
    first = _pack()
    second = _pack()

    assert first.canonical_bytes() == second.canonical_bytes()
    assert first.proposal_scope_sha256 == second.proposal_scope_sha256
    assert first.proposal_authority_set_sha256 == second.proposal_authority_set_sha256
    assert first.content_sha256 == second.content_sha256
    assert load_qc_qqq_options_bounded_cloud_pilot_policy(
        project_root=ROOT
    ).policy_sha256 == BLOCKED_POLICY_SHA256
