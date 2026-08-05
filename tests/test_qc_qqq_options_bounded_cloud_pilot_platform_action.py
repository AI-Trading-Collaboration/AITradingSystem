from __future__ import annotations

import copy
import hashlib
import inspect
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.bounded_cloud_pilot_platform_action import (
    ALLOWED_ACTIONS,
    AUTHORIZATION_TASK_ID,
    EVIDENCE_SCOPE_CHECK_IDS,
    EXPECTED_EXECUTION_EVIDENCE_RECORD_SHA256,
    EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256,
    EXPECTED_PROPOSAL_POLICY_SHA256,
    EXPECTED_RESULT_ARTIFACT_SHA256,
    EXPECTED_REVIEW_REQUEST_RECORD_SHA256,
    OWNER_AUTHORIZATION_ID,
    OWNER_EVIDENCE_ATTESTATION_ID,
    OWNER_REVIEW_REQUEST_ITEMS,
    PROHIBITED_ACTIONS,
    QCBoundedCloudPilotEvidenceScopeCheck,
    QCBoundedCloudPilotExecutionEvidenceRecord,
    QCBoundedCloudPilotIndependentReviewRecord,
    QCBoundedCloudPilotIndependentReviewRequestRecord,
    QCBoundedCloudPilotPlatformActionAuthorizationPolicy,
    QCBoundedCloudPilotPlatformActionContractError,
    QCBoundedCloudPilotPreRunAuthorizationRecord,
    build_qc_qqq_options_bounded_cloud_pilot_independent_review_record,
    build_qc_qqq_options_bounded_cloud_pilot_pre_run_record,
    build_qc_qqq_options_bounded_cloud_pilot_project_source,
    load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

ROOT = PROJECT_ROOT
REPOSITORY_SHA = "5dc32d240a9fe440e3d7b8fe6a5651a0461849f9"
CREATED_AT = datetime(2026, 8, 5, 2, 0, tzinfo=UTC)


def _loaded():
    return load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
        project_root=ROOT
    )


def _record() -> QCBoundedCloudPilotPreRunAuthorizationRecord:
    return build_qc_qqq_options_bounded_cloud_pilot_pre_run_record(
        record_id="qc_bounded_pilot_pre_run_20260805_v1",
        created_at_utc=CREATED_AT,
        repository_code_sha=REPOSITORY_SHA,
        project_root=ROOT,
    )


def _policy_payload() -> dict[str, object]:
    payload = safe_load_yaml_path(
        ROOT
        / "config/research/"
        "qc_qqq_options_bounded_cloud_pilot_platform_action_authorization_v1.yaml"
    )
    assert isinstance(payload, dict)
    return copy.deepcopy(payload)


def _evidence_scope_checks() -> tuple[QCBoundedCloudPilotEvidenceScopeCheck, ...]:
    facts = {
        "ACCOUNT_TIER": ("FREE", "FREE", "ACCOUNT_TIER_CONFIRMED"),
        "CLOUD_COMPUTE": (
            "Community B-MICRO",
            "Community B-MICRO",
            "CLOUD_COMPUTE_CONFIRMED",
        ),
        "REQUESTED_EVALUATED_RANGE": (
            "2025-12-02..2025-12-02",
            "2025-12-02..2025-12-02",
            "RANGE_CONFIRMED",
        ),
        "PROJECT_MUTATION_COUNT": ("<=1", "1", "PROJECT_MUTATION_CAP_MET"),
        "CLOUD_BACKTEST_COUNT": ("<=1", "1", "CLOUD_BACKTEST_CAP_MET"),
        "RUNTIME_SECONDS": ("<=300", "6.17", "RUNTIME_CAP_MET"),
        "PROCESSED_DATA_POINTS": (
            "<=250000",
            "734127",
            "MAXIMUM_PROCESSED_DATA_POINTS_EXCEEDED",
        ),
        "ORDER_COUNT": ("<=1", "1", "ORDER_CAP_MET"),
        "CONTRACT_QUANTITY": ("<=1", "1", "CONTRACT_CAP_MET"),
        "INTENT_SUBMIT_CHRONOLOGY": (
            "60_SECONDS",
            "60_SECONDS",
            "NEXT_INDEPENDENT_MINUTE_CONFIRMED",
        ),
        "SUBMIT_FILL_CHRONOLOGY": (
            "60_SECONDS",
            "60_SECONDS",
            "NEXT_INDEPENDENT_MINUTE_CONFIRMED",
        ),
        "FEE_PER_CONTRACT": ("0.65_USD", "0.65_USD", "FEE_CONFIRMED"),
        "SOURCE_AUTHORITY": (
            "97691704E1ED5D54071A8EC77B0DA895655DD4C6C7EEC8D9723D44CA22465A89",
            "97691704E1ED5D54071A8EC77B0DA895655DD4C6C7EEC8D9723D44CA22465A89",
            "SOURCE_AUTHORITY_CONFIRMED",
        ),
        "RESULT_TERMINAL": ("COMPLETED", "COMPLETED", "RESULT_TERMINAL_CONFIRMED"),
        "RAW_OPTIONS_ROWS_ABSENT": (
            "ABSENT",
            "ABSENT",
            "NO_RAW_OPTION_ROWS_CONFIRMED",
        ),
        "PROHIBITED_ACTIONS_ABSENT": (
            "ABSENT",
            "ABSENT",
            "PROHIBITED_ACTIONS_NOT_OBSERVED",
        ),
    }
    return tuple(
        QCBoundedCloudPilotEvidenceScopeCheck(
            check_id=check_id,
            status="FAIL" if check_id == "PROCESSED_DATA_POINTS" else "PASS",
            expected=facts[check_id][0],
            observed=facts[check_id][1],
            reason_code=facts[check_id][2],
        )
        for check_id in EVIDENCE_SCOPE_CHECK_IDS
    )


def _execution_evidence() -> QCBoundedCloudPilotExecutionEvidenceRecord:
    return QCBoundedCloudPilotExecutionEvidenceRecord.seal(
        schema_version="qc_qqq_options_bounded_cloud_pilot_execution_evidence_record.v1",
        record_id="qc_bounded_cloud_pilot_execution_evidence_20260805_v1",
        collected_at_utc=datetime(2026, 8, 5, 2, 20, tzinfo=UTC),
        repository_source_authority_sha="ce724ed7b09b8dacd66255e8d791d56dce5c4293",
        pre_run_authorization_record_sha256="d1fc79c209415260ebbc34b5ce231d67e4afa2097a87fdeebd92fb01d8c33d1e",
        owner_authorization_id=OWNER_AUTHORIZATION_ID,
        authorization_policy_sha256="2934ec3e43a9fb7db7357fa6d0fdc518098724eaed3ce14f46c93b7adf3747a7",
        authorization_policy_canonical_sha256="cc61e318ea2cd1bce32c93bdc51a2b0a135d20d33ac2a0849918c8c20c8d3823",
        proposal_policy_sha256=EXPECTED_PROPOSAL_POLICY_SHA256,
        proposal_authority_set_sha256=EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256,
        project_id="34808569",
        project_name="Sleepy Yellow-Green Shark",
        backtest_id="6e70793600035ddc3d7f856319a352db",
        backtest_name="Well Dressed Yellow Green Leopard",
        account_tier="FREE",
        cloud_compute="Community B-MICRO",
        engine_version="LEAN Engine v2.5.0.0.17970",
        lean_version="master v17970",
        project_source_sha256="97691704e1ed5d54071a8ec77b0da895655dd4c6c7eec8d9723d44ca22465a89",
        project_source_byte_count=9876,
        project_source_editor_line_endings="CRLF_CLIPBOARD_LF_CANONICAL",
        requested_start="2025-12-02",
        requested_end="2025-12-02",
        evaluated_start="2025-12-02",
        evaluated_end="2025-12-02",
        project_mutation_count=1,
        cloud_backtest_count=1,
        runtime_seconds="6.17",
        maximum_runtime_seconds=300,
        processed_data_points=734127,
        maximum_processed_data_points=250000,
        data_points_per_second=119000,
        order_count=1,
        fill_event_count=1,
        filled_quantity=1,
        maximum_contract_quantity=1,
        selected_contract_sid="QQQ YYBCLDUTHNL2|QQQ RIWIV7K5Z9LX",
        selected_contract_display="QQQ 251215C00625000",
        intent_time_utc="2025-12-02T14:31:00Z",
        submit_time_utc="2025-12-02T14:32:00Z",
        fill_time_utc="2025-12-02T14:33:00Z",
        order_type="BUY_LIMIT",
        order_status="FILLED",
        limit_price_usd="6.44",
        fill_price_usd="6.44",
        fee_usd="0.65",
        start_equity_usd="100000.00",
        end_equity_usd="100088.35",
        holdings_value_usd="733.00",
        runtime_unrealized_usd="83.35",
        result_state="Completed",
        result_artifact_sha256="fdd11ab6ce0791cc3ebd952269f670ba65a1b9747e663628ae462b52ff166ead",
        result_artifact_byte_count=17356,
        result_top_level_keys=(
            "algorithmConfiguration",
            "analysis",
            "charts",
            "orders",
            "profitLoss",
            "rollingWindow",
            "runtimeStatistics",
            "state",
            "statistics",
            "totalPerformance",
        ),
        raw_options_rows_present=False,
        order_submission_snapshot_present=True,
        broker_identifier_retained_in_tracked_evidence=False,
        editor_warning_count=4,
        editor_blocking_error_count=0,
        option_event_dq_status="PASS_PLATFORM_LOG_ONLY",
        option_event_pit_status="PASS_PLATFORM_LOG_ONLY",
        shared_2489_bundle_status="BLOCKED_SHARED_POLICY_NOT_AUTHORIZED",
        shared_2490_reconciliation_status="BLOCKED_SHARED_POLICY_NOT_AUTHORIZED",
        prior_capability_admission="CAPABILITY_OR_LICENSE_BLOCKED",
        scope_checks=_evidence_scope_checks(),
        failed_scope_check_ids=("PROCESSED_DATA_POINTS",),
        authorization_state="INVALIDATED_AFTER_EVIDENCE_COLLECTION_AND_SCOPE_VIOLATION",
        independent_review_status="PENDING_PROJECT_OWNER_REVIEW",
        final_disposition="NOT_ISSUED",
        decision="PILOT_EVIDENCE_COLLECTED_SCOPE_VIOLATION_REVIEW_REQUIRED",
        range_expansion_allowed=False,
        investment_interpretation_allowed=False,
        production_effect="none",
        broker_action="none",
    )


def test_authorization_loads_exact_owner_scope_and_live_proposal() -> None:
    loaded = _loaded()

    assert loaded.policy.owner_authorization_id == OWNER_AUTHORIZATION_ID
    assert loaded.policy.authorization_task_id == AUTHORIZATION_TASK_ID
    assert loaded.policy.proposal_policy_sha256 == EXPECTED_PROPOSAL_POLICY_SHA256
    assert (
        loaded.policy.proposal_authority_set_sha256
        == EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256
    )
    assert loaded.proposal.proposal_policy_sha256 == EXPECTED_PROPOSAL_POLICY_SHA256
    assert (
        loaded.proposal.authority_set_sha256
        == EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256
    )
    assert loaded.policy.allowed_actions == ALLOWED_ACTIONS
    assert loaded.policy.prohibited_actions == PROHIBITED_ACTIONS


def test_authorization_preserves_prior_blocked_admission_and_proposal_bytes() -> None:
    loaded = _loaded()

    assert loaded.proposal.blocked_policy.policy.status == "BLOCKED_OWNER_INPUT"
    assert (
        loaded.proposal.blocked_policy.policy.owner_authorization_token
        == "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS"
    )
    assert loaded.proposal.capability_review.review.prior_admission_decision == (
        "CAPABILITY_OR_LICENSE_BLOCKED"
    )
    assert loaded.proposal.capability_review.review.bounded_pilot_preparation_allowed is False
    assert loaded.proposal.proposal.safety.proposal_only is True
    assert loaded.proposal.proposal.safety.pilot_authorized is False


def test_project_source_is_deterministic_compilable_and_within_free_file_boundary() -> None:
    first = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    )
    second = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    )

    assert first == second
    assert first.file_name == "main.py"
    assert first.algorithm_class == "QQQOptionsBoundedPilot"
    assert first.byte_count == len(first.source_bytes)
    assert 0 < first.byte_count <= 32768
    compile(first.source_bytes, first.file_name, "exec")


def test_project_source_exact_binds_lineage_and_reviewed_runtime_values() -> None:
    source = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    ).source_bytes.decode("utf-8")

    loaded = _loaded()
    assert OWNER_AUTHORIZATION_ID in source
    assert loaded.policy_sha256 in source
    assert EXPECTED_PROPOSAL_POLICY_SHA256 in source
    assert REPOSITORY_SHA in source
    assert "self.set_start_date(2025, 12, 2)" in source
    assert "self.set_end_date(2025, 12, 2)" in source
    assert "self.set_cash(100000)" in source
    assert "ConstantSlippageModel(0.01)" in source
    assert "0.65 * abs(float(parameters.order.quantity))" in source


def test_project_source_enforces_independent_minute_chronology_and_one_order() -> None:
    source = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    ).source_bytes.decode("utf-8")

    assert "self.intent_time + timedelta(minutes=1)" in source
    assert "self.submit_time + timedelta(minutes=1)" in source
    assert "earliest_fill = self.submit_time + timedelta(minutes=1)" in source
    assert "self.limit_order(" in source
    assert "self.order_count = 1" in source
    assert source.count("self.limit_order(") == 1
    assert "Resolution.DAILY" not in source
    assert "market_order(" not in source


def test_project_source_rejects_raw_or_broker_capability_paths() -> None:
    source = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    ).source_bytes.decode("utf-8")

    assert "raw_rows_logged=false" in source
    assert "broker_action=false" in source
    assert "requests." not in source
    assert "urllib" not in source
    assert "object_store" not in source.lower()
    assert "set_brokerage_model" not in source
    assert "set_live_mode" not in source


def test_pre_run_record_is_sealed_canonical_and_cash_preserving() -> None:
    record = _record()

    assert record.authorization_state == "ACTIVE_PRE_RUN_NOT_CONSUMED"
    assert record.project_mutation_count == 0
    assert record.cloud_backtest_count == 0
    assert record.order_count == 0
    assert record.fill_count == 0
    assert record.external_action_executed is False
    assert record.option_event_dq_status == "NOT_EVALUATED_PRE_RUN"
    assert record.option_event_pit_status == "NOT_EVALUATED_PRE_RUN"
    assert record.production_effect == "none"
    assert record.broker_action == "none"
    assert (
        QCBoundedCloudPilotPreRunAuthorizationRecord.from_json_bytes(
            record.canonical_bytes
        )
        == record
    )


def test_tracked_pre_run_record_replays_exact_project_source_authority() -> None:
    raw = (
        ROOT
        / "inputs/external_validation/"
        "qc_qqq_options_bounded_cloud_pilot_authorization_20260805.json"
    ).read_bytes()
    record = QCBoundedCloudPilotPreRunAuthorizationRecord.from_json_bytes(raw)
    project = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=record.repository_code_sha,
        project_root=ROOT,
    )

    assert record.repository_code_sha == (
        "ce724ed7b09b8dacd66255e8d791d56dce5c4293"
    )
    assert record.project_source_sha256 == project.source_sha256
    assert record.project_source_byte_count == project.byte_count
    assert record.authorization_policy_sha256 == _loaded().policy_sha256


def test_pre_run_record_rejects_noncanonical_or_tampered_json() -> None:
    record = _record()
    payload = json.loads(record.canonical_bytes)
    reordered = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        QCBoundedCloudPilotPreRunAuthorizationRecord.from_json_bytes(reordered)

    payload["project_mutation_count"] = 1
    tampered = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    with pytest.raises(ValueError):
        QCBoundedCloudPilotPreRunAuthorizationRecord.from_json_bytes(tampered)


def test_execution_evidence_is_sealed_and_derives_scope_violation() -> None:
    evidence = _execution_evidence()

    assert evidence.order_count == 1
    assert evidence.fill_event_count == 1
    assert evidence.filled_quantity == 1
    assert evidence.processed_data_points == 734127
    assert evidence.maximum_processed_data_points == 250000
    assert evidence.failed_scope_check_ids == ("PROCESSED_DATA_POINTS",)
    assert evidence.authorization_state == (
        "INVALIDATED_AFTER_EVIDENCE_COLLECTION_AND_SCOPE_VIOLATION"
    )
    assert evidence.shared_2489_bundle_status == (
        "BLOCKED_SHARED_POLICY_NOT_AUTHORIZED"
    )
    assert evidence.shared_2490_reconciliation_status == (
        "BLOCKED_SHARED_POLICY_NOT_AUTHORIZED"
    )
    assert evidence.final_disposition == "NOT_ISSUED"
    assert (
        QCBoundedCloudPilotExecutionEvidenceRecord.from_json_bytes(
            evidence.canonical_bytes
        )
        == evidence
    )


def test_execution_evidence_rejects_hidden_data_point_breach() -> None:
    evidence = _execution_evidence()
    payload = evidence.model_dump(mode="json")
    payload["processed_data_points"] = 250000
    with pytest.raises(ValueError, match="observed data-point breach"):
        QCBoundedCloudPilotExecutionEvidenceRecord.model_validate(payload)

    payload = evidence.model_dump(mode="json")
    checks = payload["scope_checks"]
    assert isinstance(checks, list)
    checks[6]["status"] = "PASS"
    with pytest.raises(ValueError, match="scope failure taxonomy"):
        QCBoundedCloudPilotExecutionEvidenceRecord.model_validate(payload)


def test_independent_review_request_preserves_pending_owner_boundary() -> None:
    evidence = _execution_evidence()
    request = QCBoundedCloudPilotIndependentReviewRequestRecord.seal(
        schema_version="qc_qqq_options_bounded_cloud_pilot_independent_review_request.v1",
        record_id="qc_bounded_cloud_pilot_owner_review_request_20260805_v1",
        created_at_utc=datetime(2026, 8, 5, 2, 22, tzinfo=UTC),
        evidence_record_path=(
            "inputs/external_validation/"
            "qc_qqq_options_bounded_cloud_pilot_evidence_20260805.json"
        ),
        evidence_record_sha256=evidence.canonical_sha256,
        result_artifact_sha256=evidence.result_artifact_sha256,
        project_id="34808569",
        backtest_id="6e70793600035ddc3d7f856319a352db",
        collector_id="codex_pilot_coordinator",
        independent_reviewer_id="project_owner",
        required_review_items=OWNER_REVIEW_REQUEST_ITEMS,
        scope_violation_ids=("PROCESSED_DATA_POINTS",),
        review_status="PENDING_PROJECT_OWNER_REVIEW",
        independent_review_completed=False,
        final_disposition="NOT_ISSUED",
        range_expansion_allowed=False,
        production_effect="none",
        broker_action="none",
    )

    assert request.independent_review_completed is False
    assert request.review_status == "PENDING_PROJECT_OWNER_REVIEW"
    assert request.final_disposition == "NOT_ISSUED"
    assert (
        QCBoundedCloudPilotIndependentReviewRequestRecord.from_json_bytes(
            request.canonical_bytes
        )
        == request
    )


def test_tracked_execution_evidence_and_review_request_cross_bind() -> None:
    evidence = QCBoundedCloudPilotExecutionEvidenceRecord.from_json_bytes(
        (
            ROOT
            / "inputs/external_validation/"
            "qc_qqq_options_bounded_cloud_pilot_evidence_20260805.json"
        ).read_bytes()
    )
    review = QCBoundedCloudPilotIndependentReviewRequestRecord.from_json_bytes(
        (
            ROOT
            / "inputs/external_validation/"
            "qc_qqq_options_bounded_cloud_pilot_review_20260805.json"
        ).read_bytes()
    )

    assert review.evidence_record_sha256 == evidence.canonical_sha256
    assert review.result_artifact_sha256 == evidence.result_artifact_sha256
    assert review.project_id == evidence.project_id
    assert review.backtest_id == evidence.backtest_id
    assert review.scope_violation_ids == evidence.failed_scope_check_ids
    assert review.independent_review_completed is False


def test_independent_review_record_is_derived_from_canonical_predecessors() -> None:
    review = build_qc_qqq_options_bounded_cloud_pilot_independent_review_record(
        project_root=ROOT
    )

    assert review.owner_attestation_id == OWNER_EVIDENCE_ATTESTATION_ID
    assert review.evidence_record_sha256 == (
        EXPECTED_EXECUTION_EVIDENCE_RECORD_SHA256
    )
    assert review.review_request_sha256 == EXPECTED_REVIEW_REQUEST_RECORD_SHA256
    assert review.result_artifact_sha256 == EXPECTED_RESULT_ARTIFACT_SHA256
    assert review.confirmed_processed_data_points == 734127
    assert review.confirmed_reviewed_cap == 250000
    assert review.confirmed_scope_violation is True
    assert review.confirmed_no_raw_option_rows is True
    assert review.confirmed_shared_2489_2490_blocked is True
    assert review.disposition == "PILOT_NO_GO_LICENSE_OR_EVIDENCE"
    assert review.range_expansion_allowed is False
    assert review.further_cloud_action_authorized is False
    assert review.investment_interpretation_allowed is False


def test_tracked_independent_review_record_replays_exact_authority() -> None:
    raw = (
        ROOT
        / "inputs/external_validation/"
        "qc_qqq_options_bounded_cloud_pilot_owner_attestation_20260805.json"
    ).read_bytes()
    tracked = QCBoundedCloudPilotIndependentReviewRecord.from_json_bytes(raw)
    rebuilt = build_qc_qqq_options_bounded_cloud_pilot_independent_review_record(
        project_root=ROOT
    )

    assert tracked == rebuilt
    assert tracked.canonical_bytes == raw
    assert tracked.independent_review_completed is True
    assert tracked.evidence_acceptance == "ACCEPTED_WITH_SCOPE_VIOLATION"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("evidence_record_sha256", "0" * 64),
        ("review_request_sha256", "0" * 64),
        ("result_artifact_sha256", "0" * 64),
        ("project_id", "forged-project"),
        ("backtest_id", "forged-backtest"),
        ("confirmed_processed_data_points", 250000),
        ("confirmed_scope_violation", False),
        ("confirmed_no_raw_option_rows", False),
        ("confirmed_shared_2489_2490_blocked", False),
        ("disposition", "BOUNDED_PILOT_ACCEPTED_FOR_RANGE_EXPANSION"),
        ("range_expansion_allowed", True),
        ("further_cloud_action_authorized", True),
    ],
)
def test_independent_review_rejects_forged_acceptance_or_identity(
    field: str, value: object
) -> None:
    payload = build_qc_qqq_options_bounded_cloud_pilot_independent_review_record(
        project_root=ROOT
    ).model_dump(mode="json")
    payload[field] = value

    with pytest.raises(ValueError):
        QCBoundedCloudPilotIndependentReviewRecord.model_validate(payload)


def test_independent_review_builder_rejects_tampered_predecessor(
    tmp_path: Path,
) -> None:
    target = tmp_path / "inputs" / "external_validation"
    target.mkdir(parents=True)
    evidence_source = (
        ROOT
        / "inputs/external_validation/"
        "qc_qqq_options_bounded_cloud_pilot_evidence_20260805.json"
    )
    request_source = (
        ROOT
        / "inputs/external_validation/"
        "qc_qqq_options_bounded_cloud_pilot_review_20260805.json"
    )
    evidence_bytes = evidence_source.read_bytes()
    assert (
        hashlib.sha256(evidence_bytes).hexdigest()
        == EXPECTED_EXECUTION_EVIDENCE_RECORD_SHA256
    )
    (target / evidence_source.name).write_bytes(evidence_bytes.replace(b"734127", b"734128"))
    (target / request_source.name).write_bytes(request_source.read_bytes())

    with pytest.raises(
        QCBoundedCloudPilotPlatformActionContractError,
        match="QC_BOUNDED_CLOUD_PILOT_INDEPENDENT_REVIEW_INVALID",
    ):
        build_qc_qqq_options_bounded_cloud_pilot_independent_review_record(
            project_root=tmp_path
        )


def test_pre_run_builder_rejects_expired_authorization() -> None:
    with pytest.raises(
        QCBoundedCloudPilotPlatformActionContractError,
        match="QC_BOUNDED_CLOUD_PILOT_AUTHORIZATION_EXPIRED",
    ):
        build_qc_qqq_options_bounded_cloud_pilot_pre_run_record(
            record_id="expired",
            created_at_utc=datetime(2026, 8, 12, 0, 0, 1, tzinfo=UTC),
            repository_code_sha=REPOSITORY_SHA,
            project_root=ROOT,
        )


def test_pre_run_builder_uses_owner_decision_tokyo_effective_date() -> None:
    first_tokyo_minute = datetime(2026, 8, 4, 15, 0, tzinfo=UTC)
    record = build_qc_qqq_options_bounded_cloud_pilot_pre_run_record(
        record_id="tokyo_effective_boundary",
        created_at_utc=first_tokyo_minute,
        repository_code_sha=REPOSITORY_SHA,
        project_root=ROOT,
    )
    assert record.created_at_utc == first_tokyo_minute

    with pytest.raises(
        QCBoundedCloudPilotPlatformActionContractError,
        match="QC_BOUNDED_CLOUD_PILOT_AUTHORIZATION_NOT_YET_EFFECTIVE",
    ):
        build_qc_qqq_options_bounded_cloud_pilot_pre_run_record(
            record_id="tokyo_pre_effective_boundary",
            created_at_utc=datetime(2026, 8, 4, 14, 59, 59, tzinfo=UTC),
            repository_code_sha=REPOSITORY_SHA,
            project_root=ROOT,
        )


def test_project_builder_rejects_invalid_repository_sha() -> None:
    with pytest.raises(ValueError, match="Git SHA"):
        build_qc_qqq_options_bounded_cloud_pilot_project_source(
            repository_code_sha="not-a-sha", project_root=ROOT
        )


def test_project_source_identity_changes_with_repository_authority() -> None:
    first = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha=REPOSITORY_SHA, project_root=ROOT
    )
    second = build_qc_qqq_options_bounded_cloud_pilot_project_source(
        repository_code_sha="0" * 40, project_root=ROOT
    )

    assert first.source_sha256 != second.source_sha256
    assert first.source_bytes != second.source_bytes


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload: payload.__setitem__(
                "owner_authorization_id", "owner_decision:TRADING-2492:forged"
            ),
            "owner_authorization_id",
        ),
        (
            lambda payload: payload.__setitem__(
                "proposal_policy_sha256", "0" * 64
            ),
            "proposal policy hash",
        ),
        (
            lambda payload: payload.__setitem__(
                "proposal_authority_set_sha256", "0" * 64
            ),
            "proposal authority-set hash",
        ),
        (
            lambda payload: payload["platform_scope"].__setitem__(  # type: ignore[index,union-attr]
                "maximum_order_count", 2
            ),
            "maximum_order_count",
        ),
        (
            lambda payload: payload["research_window"].__setitem__(  # type: ignore[index,union-attr]
                "requested_start", "2022-12-01"
            ),
            "confirmed 2025-12-02 session",
        ),
        (
            lambda payload: payload["actors"].__setitem__(  # type: ignore[index,union-attr]
                "independent_reviewer_id", "codex_pilot_coordinator"
            ),
            "collector and independent reviewer",
        ),
        (
            lambda payload: payload["safety"].__setitem__(  # type: ignore[index,union-attr]
                "api_allowed", True
            ),
            "api_allowed",
        ),
        (
            lambda payload: payload.__setitem__(
                "authorization_expires_at_utc", "2026-08-13T00:00:00Z"
            ),
            "authorization expiry",
        ),
    ],
)
def test_authorization_model_rejects_scope_or_authority_drift(
    mutate, message: str
) -> None:
    payload = _policy_payload()
    mutate(payload)

    with pytest.raises(ValueError, match=message):
        QCBoundedCloudPilotPlatformActionAuthorizationPolicy.model_validate(payload)


def test_loader_rejects_missing_or_escaping_policy() -> None:
    with pytest.raises(
        QCBoundedCloudPilotPlatformActionContractError,
        match="must be a regular file",
    ):
        load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
            Path("config/research/missing-2492-authorization.yaml"),
            project_root=ROOT,
        )
    with pytest.raises(
        QCBoundedCloudPilotPlatformActionContractError,
        match="escapes the project root",
    ):
        load_qc_qqq_options_bounded_cloud_pilot_platform_action_authorization(
            Path("../outside.yaml"), project_root=ROOT
        )


def test_public_builders_have_no_caller_scope_or_activation_arguments() -> None:
    source_parameters = set(
        inspect.signature(
            build_qc_qqq_options_bounded_cloud_pilot_project_source
        ).parameters
    )
    record_parameters = set(
        inspect.signature(
            build_qc_qqq_options_bounded_cloud_pilot_pre_run_record
        ).parameters
    )

    forbidden = {
        "owner_authorization_id",
        "requested_start",
        "requested_end",
        "maximum_order_count",
        "maximum_contract_quantity",
        "pilot_authorized",
        "cloud_backtest_allowed",
    }
    assert source_parameters.isdisjoint(forbidden)
    assert record_parameters.isdisjoint(forbidden)
