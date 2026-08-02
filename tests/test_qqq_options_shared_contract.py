from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from ai_trading_system.qqq_options_research.contracts import (
    QQQ_OPTIONS_CONTRACT_SHA256,
    QQQ_OPTIONS_RECORD_TYPES,
    QQQ_OPTIONS_SCHEMA_NAMES,
    ContractCandidateSnapshotRecord,
    DailySignalRecord,
    DQReportRecord,
    FillEventRecord,
    OrderEventRecord,
    OrderIntentRecord,
    PlatformEvidenceManifestRecord,
    PortfolioSnapshotRecord,
    PositionLifecycleEventRecord,
    QQQOptionsContractError,
    QQQOptionsRecordEnvelope,
    QQQOptionsSafetyBoundary,
    ReconciliationReportRecord,
    RunManifestRecord,
    SelectionDecisionRecord,
)
from ai_trading_system.qqq_options_research.policy import (
    DEFAULT_QQQ_OPTIONS_SHARED_CONTRACT_POLICY_PATH,
    load_qqq_options_shared_contract_policy,
)

_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64
_REPOSITORY_SHA = "d" * 40
_CREATED_AT = datetime(2021, 2, 24, 21, 0, tzinfo=UTC)


def _safety() -> dict[str, object]:
    return {
        "research_only": True,
        "promotion_allowed": False,
        "paper_shadow_allowed": False,
        "production_allowed": False,
        "raw_options_data_export_allowed": False,
        "strategy_execution_allowed": False,
        "bounded_cloud_pilot_authorized": False,
        "production_effect": "none",
        "broker_action": "none",
    }


def _envelope(schema_name: str, record_id: str) -> dict[str, Any]:
    return {
        "schema_name": schema_name,
        "schema_version": "1.0.0",
        "run_id": "run-20210224",
        "record_id": record_id,
        "created_at_utc": _CREATED_AT,
        "producer_version": "test.v1",
        "repository_code_sha": _REPOSITORY_SHA,
        "policy_id": "qqq_options_shared_contract_v1",
        "policy_version": "1.0.0",
        "policy_sha256": _SHA_A,
        "contract_schema_sha256": QQQ_OPTIONS_CONTRACT_SHA256,
        "source_ids": ("qqq.options.minute", "qqq.signal.daily"),
        "source_checksums": (_SHA_B, _SHA_C),
        "requested_start": date(2021, 2, 22),
        "requested_end": date(2021, 2, 26),
        "evaluated_start": date(2021, 2, 22),
        "evaluated_end": date(2021, 2, 24),
        "storage_timezone": "UTC",
        "exchange_timezone": "America/New_York",
        "dq_status": "PASS",
        "pit_status": "PASS",
        "export_classification": "EXPORT_ALLOWED_DERIVED",
        "lineage_id": "lineage-20210224",
        "safety": _safety(),
    }


def _record_cases() -> tuple[tuple[type[QQQOptionsRecordEnvelope], dict[str, Any]], ...]:
    return (
        (
            RunManifestRecord,
            {
                **_envelope("run_manifest", "manifest-1"),
                "underlying": "QQQ",
                "initial_cash_usd": Decimal("100000"),
                "account_currency": "USD",
                "account_type": "CASH",
                "signal_resolution": "DAILY",
                "execution_resolution": "MINUTE",
                "signal_artifact_sha256": _SHA_A,
                "engine_identity_status": "UNKNOWN",
                "engine_identity": None,
                "evidence_admission_decision": "CAPABILITY_OR_LICENSE_BLOCKED",
            },
        ),
        (
            DailySignalRecord,
            {
                **_envelope("daily_signal", "signal-1"),
                "signal_session": date(2021, 2, 22),
                "signal_as_of_utc": datetime(2021, 2, 22, 21, 0, tzinfo=UTC),
                "generated_at_utc": datetime(2021, 2, 23, 1, 0, tzinfo=UTC),
                "earliest_effective_session": date(2021, 2, 23),
                "signal": "LONG_CALL",
                "signal_source_sha256": _SHA_B,
            },
        ),
        (
            ContractCandidateSnapshotRecord,
            {
                **_envelope("contract_candidate_snapshot", "candidate-1"),
                "selection_snapshot_utc": datetime(2021, 2, 24, 15, 32, tzinfo=UTC),
                "option_sid": "QQQ-20210319-C-325",
                "right": "CALL",
                "expiry": date(2021, 3, 19),
                "strike_usd_per_share": Decimal("325"),
                "contract_multiplier": 100,
                "dte": 23,
                "moneyness": Decimal("0.998"),
                "prior_day_model_as_of_session": date(2021, 2, 23),
                "open_interest_as_of_session": date(2021, 2, 23),
                "quote_bid_per_share": Decimal("5.10"),
                "quote_ask_per_share": Decimal("5.20"),
                "quote_end_utc": datetime(2021, 2, 24, 15, 31, tzinfo=UTC),
                "quote_validity": "VALID",
                "eligible": True,
                "field_export_classification": "QC_ONLY_NOT_EXPORTED",
            },
        ),
        (
            SelectionDecisionRecord,
            {
                **_envelope("selection_decision", "decision-1"),
                "decision_id": "decision-1",
                "selection_snapshot_utc": datetime(2021, 2, 24, 15, 32, tzinfo=UTC),
                "selected_option_sid": "QQQ-20210319-C-325",
                "no_contract_reason": None,
                "candidate_set_sha256": _SHA_A,
                "stable_rank_components": ("dte_distance", "option_sid"),
                "rejected_counts": (
                    {"reason_code": "invalid_quote", "count": 0},
                    {"reason_code": "stale_quote", "count": 1},
                ),
            },
        ),
        (
            OrderIntentRecord,
            {
                **_envelope("order_intent", "intent-1"),
                "intent_id": "intent-1",
                "decision_id": "decision-1",
                "option_sid": "QQQ-20210319-C-325",
                "side": "BUY_TO_OPEN",
                "contracts": 1,
                "order_type": "MARKETABLE_LIMIT",
                "limit_price_per_share": Decimal("5.20"),
                "reserved_cash_usd": Decimal("520"),
                "not_before_utc": datetime(2021, 2, 25, 14, 31, tzinfo=UTC),
            },
        ),
        (
            OrderEventRecord,
            {
                **_envelope("order_event", "order-event-1"),
                "platform_order_id": "qc-order-1",
                "event_sequence": 0,
                "event_type": "SUBMITTED",
                "event_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                "side": "BUY_TO_OPEN",
                "order_contracts": 1,
                "filled_contracts_total": 0,
                "limit_price_per_share": Decimal("5.20"),
                "reason_code": None,
            },
        ),
        (
            FillEventRecord,
            {
                **_envelope("fill_event", "fill-1"),
                "platform_order_id": "qc-order-1",
                "fill_sequence": 1,
                "fill_at_utc": datetime(2021, 2, 24, 20, 1, tzinfo=UTC),
                "quote_end_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                "side": "BUY_TO_OPEN",
                "filled_contracts": 1,
                "fill_price_per_share": Decimal("5.20"),
                "contract_multiplier": 100,
                "fee_usd": Decimal("0.65"),
                "settlement_currency": "USD",
                "quote_side": "ASK",
                "gross_cash_delta_usd": Decimal("-520"),
            },
        ),
        (
            PositionLifecycleEventRecord,
            {
                **_envelope("position_lifecycle_event", "position-event-1"),
                "position_id": "position-1",
                "event_sequence": 0,
                "occurred_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                "prior_state": "FLAT",
                "next_state": "INTENT_PENDING",
                "quantity_delta_contracts": 0,
                "cash_delta_usd": Decimal("0"),
                "reason_code": "entry_intent_created",
            },
        ),
        (
            PortfolioSnapshotRecord,
            {
                **_envelope("portfolio_snapshot", "portfolio-1"),
                "snapshot_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                "currency": "USD",
                "settled_cash_usd": Decimal("99479.35"),
                "unsettled_cash_usd": Decimal("0"),
                "reserved_cash_usd": Decimal("0"),
                "option_market_value_usd": Decimal("520"),
                "fees_paid_usd": Decimal("0.65"),
                "realized_pnl_usd": Decimal("0"),
                "unrealized_pnl_usd": Decimal("0"),
            },
        ),
        (
            DQReportRecord,
            {
                **_envelope("dq_report", "dq-report-1"),
                "scope": "qqq_options_run",
                "report_version": "1.0.0",
                "generated_at_utc": datetime(2021, 2, 24, 20, 1, tzinfo=UTC),
                "checks": (
                    {
                        "check_id": "quote_chronology",
                        "status": "PASS",
                        "reason_code": None,
                        "observed_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                    },
                    {
                        "check_id": "source_checksums",
                        "status": "PASS",
                        "reason_code": None,
                        "observed_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                    },
                ),
            },
        ),
        (
            PlatformEvidenceManifestRecord,
            {
                **_envelope("platform_evidence_manifest", "bundle-1"),
                "bundle_id": "bundle-1",
                "platform": "QuantConnect",
                "backtest_id": None,
                "tier_status": "UNKNOWN",
                "engine_identity_status": "UNKNOWN",
                "collected_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
                "collected_by": "offline_test",
                "artifacts": (
                    {
                        "artifact_id": "summary",
                        "locator": "outputs/summary.json",
                        "sha256": _SHA_A,
                        "byte_count": 12,
                        "export_classification": "EXPORT_ALLOWED_DERIVED",
                    },
                ),
                "limitations": ("No external platform action performed",),
                "raw_option_rows_included": False,
                "account_or_broker_identifiers_included": False,
            },
        ),
        (
            ReconciliationReportRecord,
            {
                **_envelope("reconciliation_report", "recon-1"),
                "check_id": "recon-1",
                "status": "EXPLAINED_DIFFERENCE",
                "difference_class": "fee_model",
                "local_value": Decimal("1.00"),
                "platform_value": Decimal("0.90"),
                "delta": Decimal("0.10"),
                "unit": "USD",
                "tolerance_policy_id": "reconciliation_tolerance_v1",
                "tolerance_policy_version": "1.0.0",
                "tolerance_policy_sha256": _SHA_B,
                "explanation": "Fee model difference is classified but not accepted for promotion.",
                "evaluated_at_utc": datetime(2021, 2, 24, 20, 0, tzinfo=UTC),
            },
        ),
    )


def _case(model: type[QQQOptionsRecordEnvelope]) -> dict[str, Any]:
    return deepcopy(
        dict(next(payload for candidate, payload in _record_cases() if candidate is model))
    )


def test_policy_manifest_exactly_freezes_schema_enums_and_safety() -> None:
    loaded = load_qqq_options_shared_contract_policy()

    assert loaded.policy_sha256 == hashlib.sha256(loaded.policy_path.read_bytes()).hexdigest()
    assert tuple(item.schema_name for item in loaded.policy.supported_schemas) == tuple(
        sorted(QQQ_OPTIONS_SCHEMA_NAMES)
    )
    assert loaded.policy.investment_thresholds_frozen is False
    assert loaded.policy.contract_schema_sha256 == QQQ_OPTIONS_CONTRACT_SHA256
    assert loaded.policy.safety == QQQOptionsSafetyBoundary(**_safety())
    assert loaded.policy.raw_field_classifications == (
        "EXPORT_PROHIBITED",
        "QC_ONLY_NOT_EXPORTED",
    )


@pytest.mark.parametrize(("model", "payload"), _record_cases())
def test_every_record_seals_and_replays_canonical_bytes(
    model: type[QQQOptionsRecordEnvelope], payload: dict[str, Any]
) -> None:
    sealed = model.seal(**payload)

    assert len(sealed.content_sha256) == 64
    assert sealed.content_sha256 == sealed.compute_content_sha256()
    assert model.from_json_bytes(sealed.canonical_bytes) == sealed
    assert sealed.canonical_bytes.endswith(b"\n")
    assert b'"content_sha256"' in sealed.canonical_bytes


def test_record_rejects_noncanonical_or_semantically_tampered_replay() -> None:
    sealed = RunManifestRecord.seal(**_case(RunManifestRecord))
    noncanonical = sealed.canonical_bytes.replace(
        b'"run_id": "run-20210224"', b'"run_id":"run-20210224"'
    )
    tampered = sealed.canonical_bytes.replace(
        b'"initial_cash_usd": "100000"', b'"initial_cash_usd": "1"'
    )

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_RECORD_NOT_CANONICAL"):
        RunManifestRecord.from_json_bytes(noncanonical)
    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_RECORD_INVALID"):
        RunManifestRecord.from_json_bytes(tampered)


def test_record_rejects_floats_unsafe_boundary_and_unknown_fields() -> None:
    float_payload = _case(RunManifestRecord)
    float_payload["initial_cash_usd"] = 100000.0
    unsafe_payload = _case(RunManifestRecord)
    unsafe_payload["safety"]["paper_shadow_allowed"] = True
    extra_payload = _case(RunManifestRecord)
    extra_payload["undocumented_field"] = "drift"

    with pytest.raises(ValidationError):
        RunManifestRecord.seal(**float_payload)
    with pytest.raises(ValidationError):
        RunManifestRecord.seal(**unsafe_payload)
    with pytest.raises(ValidationError):
        RunManifestRecord.seal(**extra_payload)


@pytest.mark.parametrize(
    ("mutation", "expected"),
    (
        ({"initial_cash_usd": Decimal("NaN")}, "finite"),
        ({"account_currency": "EUR"}, "USD"),
        ({"repository_code_sha": "A" * 40}, "Git object SHA"),
        ({"source_ids": ("qqq.signal.daily", "qqq.signal.daily")}, "unique"),
        ({"created_at_utc": datetime(2021, 2, 24, 21, 0)}, "timezone-aware"),
        (
            {
                "evaluated_start": date(2021, 2, 21),
                "evaluated_end": date(2021, 2, 24),
            },
            "contained",
        ),
    ),
)
def test_envelope_rejects_numeric_unit_hash_lineage_and_time_drift(
    mutation: dict[str, Any], expected: str
) -> None:
    payload = _case(RunManifestRecord)
    payload.update(mutation)

    with pytest.raises(ValidationError, match=expected):
        RunManifestRecord.seal(**payload)


def test_candidate_allows_valid_but_ineligible_and_rejects_invalid_eligible_quote() -> None:
    valid_but_ineligible = _case(ContractCandidateSnapshotRecord)
    valid_but_ineligible["eligible"] = False
    invalid_but_eligible = _case(ContractCandidateSnapshotRecord)
    invalid_but_eligible["quote_validity"] = "STALE"

    assert ContractCandidateSnapshotRecord.seal(**valid_but_ineligible).eligible is False
    with pytest.raises(ValidationError, match="eligible candidate"):
        ContractCandidateSnapshotRecord.seal(**invalid_but_eligible)


def test_fill_requires_side_aware_quote_and_exact_multiplier_cash_delta() -> None:
    wrong_side = _case(FillEventRecord)
    wrong_side["quote_side"] = "BID"
    wrong_delta = _case(FillEventRecord)
    wrong_delta["gross_cash_delta_usd"] = Decimal("-5.20")

    with pytest.raises(ValidationError, match="quote side"):
        FillEventRecord.seal(**wrong_side)
    with pytest.raises(ValidationError, match="gross cash delta"):
        FillEventRecord.seal(**wrong_delta)


def test_position_lifecycle_rejects_unreviewed_transition() -> None:
    payload = _case(PositionLifecycleEventRecord)
    payload["next_state"] = "CLOSED"

    with pytest.raises(ValidationError, match="illegal position lifecycle transition"):
        PositionLifecycleEventRecord.seal(**payload)


def test_selection_requires_exactly_one_contract_or_no_contract_reason() -> None:
    payload = _case(SelectionDecisionRecord)
    payload["no_contract_reason"] = "no_valid_contract"

    with pytest.raises(ValidationError, match="exactly one"):
        SelectionDecisionRecord.seal(**payload)


def test_evidence_manifest_rejects_raw_rows_and_account_identifiers() -> None:
    payload = _case(PlatformEvidenceManifestRecord)
    payload["raw_option_rows_included"] = True

    with pytest.raises(ValidationError, match="prohibited content"):
        PlatformEvidenceManifestRecord.seal(**payload)


def test_policy_loader_fails_closed_on_schema_or_safety_drift(tmp_path: Path) -> None:
    source = DEFAULT_QQQ_OPTIONS_SHARED_CONTRACT_POLICY_PATH.read_text(encoding="utf-8")
    payload = yaml.safe_load(source)
    payload["supported_schemas"] = payload["supported_schemas"][:-1]
    drifted_schema = tmp_path / "schema-drift.yaml"
    drifted_schema.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SHARED_POLICY_INVALID"):
        load_qqq_options_shared_contract_policy(drifted_schema)

    payload = yaml.safe_load(source)
    payload["safety"]["production_allowed"] = True
    drifted_safety = tmp_path / "safety-drift.yaml"
    drifted_safety.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SHARED_POLICY_INVALID"):
        load_qqq_options_shared_contract_policy(drifted_safety)

    payload = yaml.safe_load(source)
    payload["contract_schema_sha256"] = _SHA_C
    drifted_contract_hash = tmp_path / "contract-hash-drift.yaml"
    drifted_contract_hash.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    with pytest.raises(QQQOptionsContractError, match="QQQ_OPTIONS_SHARED_POLICY_INVALID"):
        load_qqq_options_shared_contract_policy(drifted_contract_hash)


def test_public_record_registry_has_exactly_twelve_unique_models() -> None:
    assert len(QQQ_OPTIONS_RECORD_TYPES) == 12
    assert len(QQQ_OPTIONS_SCHEMA_NAMES) == 12
    assert len(set(QQQ_OPTIONS_SCHEMA_NAMES)) == 12
