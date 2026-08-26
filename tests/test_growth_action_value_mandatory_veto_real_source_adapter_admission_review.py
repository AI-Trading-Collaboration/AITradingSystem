from __future__ import annotations

import copy
from pathlib import Path
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_real_source_adapter_admission_review as review,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

_CONFIG_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "real_source_adapter_admission_review_v1.yaml"
)
_VETO_IDS = (
    "broad_market_risk_off_veto",
    "realized_volatility_veto",
    "scheduled_event_risk_veto",
    "underlying_trend_break_veto",
)


def _policy_payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(
            _CONFIG_PATH.read_text(encoding="utf-8"), label=str(_CONFIG_PATH)
        ),
    )


def _planning_receipt(
    loaded: review.MandatoryVetoRealSourceAdmissionReviewLoadResult,
) -> dict[str, Any]:
    return {
        "schema_version": (
            "growth_action_value_mandatory_veto_"
            "real_source_admission_planning_receipt.v1"
        ),
        "review_policy_file_sha256": loaded.file_sha256,
        "review_policy_canonical_sha256": loaded.canonical_sha256,
        "status": "REVIEW_READY_NOT_ADMITTED",
        "veto_rows": [
            {
                "veto_id": veto_id,
                "candidate_contract_complete": True,
                "remaining_blockers_acknowledged": True,
                "provider_query_count": 0,
                "cache_read_count": 0,
                "adapter_execution_count": 0,
                "real_source_identity_admitted": False,
                "exact_1202_session_inventory_admitted": False,
                "observed_inventory_lf_sha256": None,
                "observed_target_end": None,
                "observed_row_count": None,
            }
            for veto_id in _VETO_IDS
        ],
        "manifest": {
            "calendar_authority_id": "qqq_exact_1202_session_sheet_v4",
            "target_start": "2021-02-22",
            "expected_target_session_count": 1202,
            "observed_target_session_count": None,
            "target_end": None,
            "target_session_list_lf_sha256": None,
            "source_snapshot_sha256": None,
            "manifest_sha256": None,
            "dq_report_sha256": None,
            "veto_series_sha256": None,
            "event_authority_coverage_receipts": [],
            "trend_target_start_checkpoint_sha256": None,
            "manifest_replay_executed": False,
            "manifest_replay_status": "NOT_RUN_NOT_AUTHORIZED",
        },
        "execution_counters": {
            "orders": 0,
            "fills": 0,
            "positions": 0,
            "paper_actions": 0,
            "live_actions": 0,
            "production_effects": 0,
            "broker_actions": 0,
        },
    }


def test_review_replays_s5_and_keeps_source_and_inventory_unadmitted() -> None:
    loaded = review.load_mandatory_veto_real_source_admission_review()

    assert loaded.terminal == (
        "OWNER_REVIEW_READY_REAL_SOURCE_ADMISSION_NOT_GRANTED_0_OF_4"
    )
    assert loaded.synthetic_producer_contract.file_sha256 == (
        "14a8995e0bcb5cdc1a5fccb67d6389c5e72fb65ce1efdb926d1f9520e1d4d314"
    )
    assert tuple(row.veto_id for row in loaded.policy.review_rows) == _VETO_IDS
    assert all(row.candidate_ready_for_review for row in loaded.policy.review_rows)
    assert not any(row.real_source_identity_admitted for row in loaded.policy.review_rows)
    assert not any(
        row.exact_1202_session_inventory_admitted for row in loaded.policy.review_rows
    )
    assert loaded.policy.aggregate_state.admitted_real_source_identities == ()
    assert loaded.policy.aggregate_state.observed_manifest_replays == ()


def test_candidate_roles_preserve_primary_diagnostic_and_planned_boundaries() -> None:
    loaded = review.load_mandatory_veto_real_source_admission_review()
    candidates = {row.candidate_id: row for row in loaded.policy.source_candidates}

    assert candidates["fmp_spy_qqq_eod_adjusted_v1"].adapter_id == "FmpPriceProvider"
    assert candidates["cboe_vix_index_daily_v1"].official_source
    assert candidates["cboe_vix_index_daily_v1"].permitted_role == (
        "PRIMARY_VIX_INDEX_CANDIDATE"
    )
    assert candidates["marketstack_spy_qqq_second_source_v1"].permitted_role == (
        "SECOND_SOURCE_RECONCILIATION_ONLY"
    )
    assert candidates["fred_vixcls_diagnostic_crosscheck_v1"].permitted_role == (
        "DIAGNOSTIC_CROSSCHECK_ONLY"
    )
    event_candidates = [
        row
        for row in candidates.values()
        if row.permitted_role == "OFFICIAL_EVENT_COVERAGE_CANDIDATE"
    ]
    assert len(event_candidates) == 3
    assert all(
        row.implementation_state == "PLANNED_CAPTURE_ADAPTER_NOT_IMPLEMENTED"
        for row in event_candidates
    )
    assert not any(row.live_probe_performed or row.admitted for row in candidates.values())


def test_exact_inventory_separates_warmup_and_state_lineage_from_target() -> None:
    loaded = review.load_mandatory_veto_real_source_admission_review()
    inventory = loaded.policy.exact_inventory_plan

    assert inventory.target_start == "2021-02-22"
    assert inventory.target_session_count == 1202
    assert inventory.target_end is None
    assert inventory.target_session_list_lf_sha256 is None
    assert inventory.observed_target_session_count is None
    assert inventory.warmup_separate_from_target
    assert tuple(row.minimum_pre_target_sessions for row in inventory.warmup_rows) == (
        199,
        19,
        251,
        199,
    )
    assert tuple(row.continuous_state_replay_required for row in inventory.warmup_rows) == (
        False,
        False,
        False,
        True,
    )


def test_policy_rejects_fabricated_admission_execution_or_live_probe() -> None:
    payload = _policy_payload()

    admitted = copy.deepcopy(payload)
    admitted["review_rows"][0]["real_source_identity_admitted"] = True
    with pytest.raises(ValidationError):
        review.MandatoryVetoRealSourceAdmissionReview.model_validate(admitted)

    executable = copy.deepcopy(payload)
    executable["safety"]["adapter_execution_authorized"] = True
    with pytest.raises(ValidationError):
        review.MandatoryVetoRealSourceAdmissionReview.model_validate(executable)

    probed = copy.deepcopy(payload)
    probed["source_candidates"][0]["live_probe_performed"] = True
    with pytest.raises(ValidationError):
        review.MandatoryVetoRealSourceAdmissionReview.model_validate(probed)


@pytest.mark.parametrize(
    ("section", "index", "field", "replacement"),
    [
        ("source_candidates", 2, "endpoint", "https://example.invalid/vix.csv"),
        ("review_rows", 0, "required_receipt_fields", ["checksum"]),
        ("exact_inventory_plan", None, "target_session_count", 1201),
        ("manifest_replay_plan", None, "required_gates", ["ZERO_EXECUTION_COUNTERS"]),
    ],
)
def test_policy_rejects_source_inventory_and_manifest_surface_drift(
    section: str,
    index: int | None,
    field: str,
    replacement: object,
) -> None:
    payload = _policy_payload()
    target = payload[section] if index is None else payload[section][index]
    target[field] = replacement

    with pytest.raises(ValidationError):
        review.MandatoryVetoRealSourceAdmissionReview.model_validate(payload)


def test_in_memory_planning_receipt_accepts_review_only_state_without_io(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded = review.load_mandatory_veto_real_source_admission_review()
    payload = _planning_receipt(loaded)

    def forbidden_read_bytes(_path: Path) -> bytes:
        raise AssertionError("planning receipt validator attempted filesystem I/O")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    receipt = review.validate_mandatory_veto_real_source_planning_receipt(
        payload, review=loaded
    )

    assert receipt.status == "REVIEW_READY_NOT_ADMITTED"
    assert receipt.manifest.manifest_replay_status == "NOT_RUN_NOT_AUTHORIZED"
    assert all(row.provider_query_count == 0 for row in receipt.veto_rows)
    assert receipt.execution_counters.orders == 0
    assert receipt.execution_counters.broker_actions == 0


@pytest.mark.parametrize(
    ("mutator", "expected_fragment"),
    [
        (
            lambda payload: payload.update(
                {"review_policy_file_sha256": "0" * 64}
            ),
            "identity mismatch",
        ),
        (
            lambda payload: payload["veto_rows"][0].update(
                {"provider_query_count": 1}
            ),
            "Input should be 0",
        ),
        (
            lambda payload: payload["veto_rows"][1].update(
                {"observed_inventory_lf_sha256": "a" * 64}
            ),
            "Input should be None",
        ),
        (
            lambda payload: payload["manifest"].update(
                {"manifest_replay_executed": True}
            ),
            "Input should be False",
        ),
        (
            lambda payload: payload["execution_counters"].update({"fills": 1}),
            "Input should be 0",
        ),
    ],
)
def test_planning_receipt_fails_closed_on_observed_or_execution_claims(
    mutator: Any,
    expected_fragment: str,
) -> None:
    loaded = review.load_mandatory_veto_real_source_admission_review()
    payload = _planning_receipt(loaded)
    mutator(payload)

    with pytest.raises(
        review.MandatoryVetoRealSourcePlanningReceiptError,
        match=expected_fragment,
    ):
        review.validate_mandatory_veto_real_source_planning_receipt(
            payload, review=loaded
        )


def test_unknown_policy_or_receipt_fields_fail_closed() -> None:
    payload = _policy_payload()
    payload["unexpected"] = True
    with pytest.raises(ValidationError):
        review.MandatoryVetoRealSourceAdmissionReview.model_validate(payload)

    loaded = review.load_mandatory_veto_real_source_admission_review()
    receipt_payload = _planning_receipt(loaded)
    receipt_payload["unexpected"] = True
    with pytest.raises(review.MandatoryVetoRealSourcePlanningReceiptError):
        review.validate_mandatory_veto_real_source_planning_receipt(
            receipt_payload, review=loaded
        )
