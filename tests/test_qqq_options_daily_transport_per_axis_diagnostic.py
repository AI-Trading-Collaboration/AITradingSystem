from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from ai_trading_system.qqq_options_research import (
    daily_slice_revalidation_execution_evidence as source_v1,
)
from ai_trading_system.qqq_options_research.daily_transport_per_axis_diagnostic import (
    AxisDiagnosticReason,
    AxisDiagnosticRecord,
    AxisDiagnosticStatus,
    AxisRejectScope,
    DailyTransportAxis,
    DailyTransportPerAxisDiagnosticEnvelope,
    DiagnosticSourceBinding,
    DiagnosticSourceId,
    build_daily_transport_per_axis_diagnostic,
    build_repository_daily_transport_per_axis_diagnostic,
    classify_axis_rejection,
    load_daily_transport_axis_diagnostic_policy,
)

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_POLICY_FILE_SHA256 = "d98b94d783ae3405f50e5a470d7d720e1ca187cd48bef186fb09ea87c8e303e0"
EXPECTED_POLICY_CANONICAL_SHA256 = (
    "f051f5e50458add258f2915c00222a56f942576be6f42bd4b8c42aa3ab971f70"
)
EXPECTED_DIAGNOSTIC_CONTENT_SHA256 = (
    "e8125e165f8acf6147f15fbd64701832ba6f602bbc98d69863d65ae942b8b7aa"
)
EXPECTED_DIAGNOSTIC_CANONICAL_SHA256 = (
    "b2382b928a860685412add5ac091ac458d08ab9d246351a4a5a516d050eca9ac"
)


def _canonical_bytes(payload: object) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    ).encode("utf-8")


@pytest.fixture(scope="module")
def repository_diagnostic() -> DailyTransportPerAxisDiagnosticEnvelope:
    return build_repository_daily_transport_per_axis_diagnostic(project_root=ROOT)


@pytest.fixture(scope="module")
def source_package() -> source_v1.BuiltDailySliceExecutionEvidencePackage:
    return source_v1.load_daily_slice_execution_evidence_package(project_root=ROOT)


def _not_evaluated(axis: DailyTransportAxis) -> AxisDiagnosticRecord:
    return AxisDiagnosticRecord(
        axis=axis,
        status=AxisDiagnosticStatus.NOT_EVALUATED,
        reason_codes=(AxisDiagnosticReason.AXIS_COUNTER_NOT_EXPORTED,),
        source_fields=("runtime_diagnostic:no_per_axis_counters",),
    )


def test_policy_freezes_exact_source_axes_and_safety_boundary() -> None:
    loaded = load_daily_transport_axis_diagnostic_policy()
    policy = loaded.policy

    assert tuple(policy.axis_order) == tuple(DailyTransportAxis)
    assert tuple(policy.source_binding_order) == tuple(DiagnosticSourceId)
    assert policy.diagnostic_repository_base_commit == (
        "06b0b29fac5d77e011d5dbe0151f566c8c030d0d"
    )
    assert policy.source_repository_commit == "54e43a1aa9787c52d4b0cb363e30e5a4bf79aed9"
    assert (policy.expected_session_count, policy.chain_session_count) == (1202, 1201)
    assert (policy.valid_candidate_session_count, policy.transport_rejected_session_count) == (
        0,
        1201,
    )
    assert policy.unknown_input_maps_to == "NOT_EVALUATED"
    assert policy.caller_asserted_pass_accepted is False
    assert policy.raw_option_rows_allowed is False
    assert policy.new_external_action_authorized is False
    assert policy.further_cloud_run_authorized is False
    assert policy.selection_authorized is False
    assert policy.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert loaded.file_sha256 == EXPECTED_POLICY_FILE_SHA256
    assert loaded.canonical_sha256 == EXPECTED_POLICY_CANONICAL_SHA256


def test_repository_diagnostic_is_canonical_unresolved_and_cash_preserving(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
) -> None:
    diagnostic = repository_diagnostic
    replay = DailyTransportPerAxisDiagnosticEnvelope.from_json_bytes(
        diagnostic.canonical_bytes
    )

    assert replay == diagnostic
    assert replay.content_sha256 == replay.compute_content_sha256()
    assert replay.content_sha256 == EXPECTED_DIAGNOSTIC_CONTENT_SHA256
    assert replay.canonical_sha256 == EXPECTED_DIAGNOSTIC_CANONICAL_SHA256
    assert replay.reject_scope is AxisRejectScope.UNRESOLVED_COMBINATION
    assert replay.reject_reason_codes == (
        AxisDiagnosticReason.ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE,
        AxisDiagnosticReason.ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS,
    )
    assert replay.axes[0].axis is DailyTransportAxis.OPTION_CHAIN_PRESENCE
    assert replay.axes[0].status is AxisDiagnosticStatus.PRESENT
    assert replay.axes[0].observed_session_count == 1201
    assert all(item.status is AxisDiagnosticStatus.NOT_EVALUATED for item in replay.axes[1:])
    assert all(item.observed_session_count is None for item in replay.axes[1:])
    assert all(item.rejected_session_count is None for item in replay.axes[1:])
    assert replay.source_evidence_admission_status == "FAIL"
    assert replay.local_derived_aggregate_dq_status == "NOT_EVALUATED"
    assert replay.option_event_dq_status == "NOT_EVALUATED"
    assert replay.further_cloud_run_authorized is False
    assert replay.raw_option_rows_consumed is False
    assert replay.raw_option_rows_reconstructed is False
    assert replay.external_action == "none"
    assert replay.selection_authorized is False
    assert replay.engine_status == "POLICY_BLOCKED_CASH_PRESERVATION"
    assert replay.investment_interpretation_generated is False
    assert replay.production_effect == replay.broker_action == "none"


def test_source_binding_input_order_is_invariant(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
    source_package: source_v1.BuiltDailySliceExecutionEvidencePackage,
) -> None:
    rebuilt = build_daily_transport_per_axis_diagnostic(
        policy_load=load_daily_transport_axis_diagnostic_policy(),
        failure_receipt=source_package.failure_receipt,
        package_manifest=source_package.manifest,
        source_bindings=tuple(reversed(repository_diagnostic.source_bindings)),
    )

    assert rebuilt.canonical_bytes == repository_diagnostic.canonical_bytes


def test_single_and_cross_axis_rejects_are_distinct_from_unresolved_combination() -> None:
    chain = AxisDiagnosticRecord(
        axis=DailyTransportAxis.OPTION_CHAIN_PRESENCE,
        status=AxisDiagnosticStatus.PRESENT,
        observed_session_count=10,
        rejected_session_count=0,
        reason_codes=(AxisDiagnosticReason.CHAIN_SESSIONS_PRESENT,),
        source_fields=("daily_slice_chain_session_count",),
    )
    missing = AxisDiagnosticRecord(
        axis=DailyTransportAxis.UNDERLYING_PRICE,
        status=AxisDiagnosticStatus.MISSING,
        observed_session_count=10,
        rejected_session_count=4,
        reason_codes=(AxisDiagnosticReason.SINGLE_AXIS_MISSING,),
        source_fields=("underlying_missing_session_count",),
    )
    invalid = AxisDiagnosticRecord(
        axis=DailyTransportAxis.BID_ASK_QUOTE,
        status=AxisDiagnosticStatus.INVALID,
        observed_session_count=10,
        rejected_session_count=3,
        reason_codes=(AxisDiagnosticReason.SINGLE_AXIS_INVALID,),
        source_fields=("bid_ask_invalid_session_count",),
    )
    unresolved = (chain,) + tuple(_not_evaluated(axis) for axis in tuple(DailyTransportAxis)[1:])

    assert classify_axis_rejection((chain, missing), combined_rejected_session_count=4) == (
        AxisRejectScope.SINGLE_AXIS,
        (AxisDiagnosticReason.SINGLE_AXIS_REJECT_IDENTIFIED,),
    )
    assert classify_axis_rejection(
        (chain, missing, invalid), combined_rejected_session_count=7
    ) == (
        AxisRejectScope.CROSS_AXIS,
        (AxisDiagnosticReason.MULTIPLE_AXIS_REJECTS_IDENTIFIED,),
    )
    assert classify_axis_rejection(unresolved, combined_rejected_session_count=10) == (
        AxisRejectScope.UNRESOLVED_COMBINATION,
        (
            AxisDiagnosticReason.ALL_CHAIN_SESSIONS_REJECTED_BY_COMBINED_GATE,
            AxisDiagnosticReason.ROOT_CAUSE_UNRESOLVED_WITHOUT_PER_AXIS_COUNTS,
        ),
    )


@pytest.mark.parametrize("forged_status", ["PASS", "UNKNOWN"])
def test_forged_pass_or_unknown_status_cannot_enter_canonical_envelope(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
    forged_status: str,
) -> None:
    payload = json.loads(repository_diagnostic.canonical_bytes)
    payload["axes"][1]["status"] = forged_status
    payload["content_sha256"] = "0" * 64

    with pytest.raises(ValidationError):
        DailyTransportPerAxisDiagnosticEnvelope.from_json_bytes(_canonical_bytes(payload))


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "reordered", "extra"])
def test_axis_set_must_be_exact_unique_and_ordered(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
    mutation: str,
) -> None:
    payload = json.loads(repository_diagnostic.canonical_bytes)
    axes = payload["axes"]
    if mutation == "missing":
        axes.pop()
    elif mutation == "duplicate":
        axes[-1] = axes[-2]
    elif mutation == "reordered":
        axes[1], axes[2] = axes[2], axes[1]
    else:
        axes.append({**axes[-1], "axis": "FORGED_EXTRA_AXIS"})
    payload["content_sha256"] = "0" * 64

    with pytest.raises((ValidationError, ValueError)):
        DailyTransportPerAxisDiagnosticEnvelope.from_json_bytes(_canonical_bytes(payload))


def test_source_hash_missing_duplicate_and_mismatch_fail_closed(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
    source_package: source_v1.BuiltDailySliceExecutionEvidencePackage,
) -> None:
    policy = load_daily_transport_axis_diagnostic_policy()
    bindings = repository_diagnostic.source_bindings
    mismatched = bindings[:-1] + (
        DiagnosticSourceBinding(
            source_id=bindings[-1].source_id,
            sha256="f" * 64,
        ),
    )
    cases = (bindings[:-1], bindings + (bindings[-1],), mismatched)

    for candidate in cases:
        with pytest.raises(ValueError):
            build_daily_transport_per_axis_diagnostic(
                policy_load=policy,
                failure_receipt=source_package.failure_receipt,
                package_manifest=source_package.manifest,
                source_bindings=candidate,
            )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("expected_session_count", 1201),
        ("daily_slice_chain_session_count", 1200),
        ("transport_rejected_session_count", 1200),
        ("requested_start", date(2022, 12, 1)),
        ("backtest_id", "0" * 32),
    ],
)
def test_source_range_session_count_and_backtest_drift_fail_closed(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
    source_package: source_v1.BuiltDailySliceExecutionEvidencePackage,
    field: str,
    value: object,
) -> None:
    receipt = source_package.failure_receipt.model_copy(update={field: value})

    with pytest.raises(ValueError):
        build_daily_transport_per_axis_diagnostic(
            policy_load=load_daily_transport_axis_diagnostic_policy(),
            failure_receipt=receipt,
            package_manifest=source_package.manifest,
            source_bindings=repository_diagnostic.source_bindings,
        )


def test_raw_row_source_or_field_is_rejected() -> None:
    with pytest.raises(ValidationError):
        DiagnosticSourceBinding.model_validate(
            {"source_id": "RAW_OPTION_ROWS", "sha256": "a" * 64}, strict=False
        )
    with pytest.raises(ValidationError):
        AxisDiagnosticRecord(
            axis=DailyTransportAxis.UNDERLYING_PRICE,
            status=AxisDiagnosticStatus.NOT_EVALUATED,
            reason_codes=(AxisDiagnosticReason.AXIS_COUNTER_NOT_EXPORTED,),
            source_fields=("raw_option_rows",),
        )


def test_not_evaluated_cannot_be_promoted_by_changing_only_status() -> None:
    with pytest.raises(ValidationError):
        AxisDiagnosticRecord(
            axis=DailyTransportAxis.GREEKS,
            status=AxisDiagnosticStatus.PRESENT,
            reason_codes=(AxisDiagnosticReason.AXIS_COUNTER_NOT_EXPORTED,),
            source_fields=("runtime_diagnostic:no_per_axis_counters",),
        )


def test_noncanonical_or_duplicate_key_json_is_rejected(
    repository_diagnostic: DailyTransportPerAxisDiagnosticEnvelope,
) -> None:
    with pytest.raises(ValueError, match="canonical"):
        DailyTransportPerAxisDiagnosticEnvelope.from_json_bytes(
            json.dumps(repository_diagnostic.model_dump(mode="json")).encode("utf-8")
        )
    raw = repository_diagnostic.canonical_bytes
    duplicate = raw.replace(b'{\n  "', b'{\n  "task_id": "forged",\n  "', 1)
    with pytest.raises(ValueError, match="duplicate JSON key"):
        DailyTransportPerAxisDiagnosticEnvelope.from_json_bytes(duplicate)
