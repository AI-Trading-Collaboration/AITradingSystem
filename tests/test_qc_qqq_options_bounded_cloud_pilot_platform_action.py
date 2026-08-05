from __future__ import annotations

import copy
import inspect
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.bounded_cloud_pilot_platform_action import (
    ALLOWED_ACTIONS,
    AUTHORIZATION_TASK_ID,
    EXPECTED_PROPOSAL_AUTHORITY_SET_SHA256,
    EXPECTED_PROPOSAL_POLICY_SHA256,
    OWNER_AUTHORIZATION_ID,
    PROHIBITED_ACTIONS,
    QCBoundedCloudPilotPlatformActionAuthorizationPolicy,
    QCBoundedCloudPilotPlatformActionContractError,
    QCBoundedCloudPilotPreRunAuthorizationRecord,
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
