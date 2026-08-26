from __future__ import annotations

import ast
import copy
import inspect
from datetime import datetime
from typing import Any, cast

import pytest
from pydantic import ValidationError

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_pit_receipt_adapter_contract as contract,
)
from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_pit_receipt_adapters as adapters,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

_PATH = PROJECT_ROOT / (
    "config/research/"
    "qc_qqq_options_growth_action_value_mandatory_veto_"
    "pit_receipt_adapter_contract_v1.yaml"
)
_SESSIONS = ("2021-02-22", "2021-02-23")
_DECISION_AS_OF = datetime.fromisoformat("2026-08-26T12:00:00+00:00")
_REQUIRED_EVENT_COVERAGE = datetime.fromisoformat("2026-08-28T20:00:00-04:00")


def _payload() -> dict[str, Any]:
    return cast(
        dict[str, Any],
        load_strict_yaml_text(_PATH.read_text(encoding="utf-8"), label=str(_PATH)),
    )


def _policy() -> contract.MandatoryVetoPITReceiptAdapterContract:
    return contract.load_mandatory_veto_pit_receipt_adapter_contract().policy


def _spec(adapter_id: str) -> contract.AdapterSpec:
    return _policy().adapter_spec(adapter_id)


def _price_rows(ticker: str) -> list[dict[str, object]]:
    return [
        {
            "session": "2021-02-22",
            "ticker": ticker,
            "provider_symbol_alias": ticker,
            "raw_close": 100.0,
            "dividend_adjusted_close": 99.5,
            "available_at": "2021-02-22T21:30:00-05:00",
        },
        {
            "session": "2021-02-23",
            "ticker": ticker,
            "provider_symbol_alias": ticker,
            "raw_close": 101.0,
            "dividend_adjusted_close": 100.5,
            "available_at": "2021-02-23T21:30:00-05:00",
        },
    ]


def _fmp_fixture(
    ticker: str,
) -> tuple[contract.AdapterSpec, dict[str, object], list[dict[str, object]]]:
    spec = _spec("FmpPricePITReceiptAdapter")
    rows = _price_rows(ticker)
    receipt: dict[str, object] = {
        "schema_version": "pit_price_source_receipt.v1",
        "candidate_id": spec.candidate_id,
        "provider": spec.provider_or_authority,
        "source_id": spec.source_id,
        "endpoint": spec.endpoint,
        "request_parameters": {
            "symbol": ticker,
            "from": _SESSIONS[0],
            "to": _SESSIONS[-1],
            "interval": "daily",
            "raw_price_mode": "non-split-adjusted",
            "adjusted_price_mode": "dividend-adjusted",
        },
        "ticker": ticker,
        "provider_symbol_alias": ticker,
        "adjustment_basis": "FMP_NON_SPLIT_RAW_PLUS_DIVIDEND_ADJUSTED_CLOSE",
        "adjustment_vintage": "2026-08-26T09:00:00+00:00",
        "session_timezone": "America/New_York",
        "available_at": "2026-08-26T09:30:00+00:00",
        "downloaded_at": "2026-08-26T10:00:00+00:00",
        "row_count": len(rows),
        "checksum": adapters.canonical_payload_sha256(rows),
    }
    return spec, receipt, rows


def _vix_fixture() -> tuple[contract.AdapterSpec, dict[str, object], list[dict[str, object]]]:
    spec = _spec("CboeVixPITReceiptAdapter")
    rows: list[dict[str, object]] = [
        {
            "session": "2021-02-22",
            "ticker": "VIX",
            "close": 23.45,
            "adjusted_close": 23.45,
            "available_at": "2021-02-22T16:30:00-06:00",
        },
        {
            "session": "2021-02-23",
            "ticker": "VIX",
            "close": 22.31,
            "adjusted_close": 22.31,
            "available_at": "2021-02-23T16:30:00-06:00",
        },
    ]
    receipt: dict[str, object] = {
        "schema_version": "pit_vix_source_receipt.v1",
        "candidate_id": spec.candidate_id,
        "provider": spec.provider_or_authority,
        "source_id": spec.source_id,
        "endpoint": spec.endpoint,
        "request_parameters": {
            "ticker": "VIX",
            "from": _SESSIONS[0],
            "to": _SESSIONS[-1],
            "interval": "daily",
            "content": "full_history_csv",
        },
        "ticker": "VIX",
        "adjustment_basis": "CBOE_VIX_INDEX_LEVEL_UNADJUSTED_CLOSE_EQUALS_ADJUSTED_CLOSE",
        "session_timezone": "America/Chicago",
        "level_definition": "CBOE_VIX_OFFICIAL_DAILY_CLOSE",
        "revision_policy": "IMMUTABLE_CAPTURE_SNAPSHOT_NO_FILL_OR_OVERRIDE",
        "available_at": "2026-08-26T09:30:00+00:00",
        "downloaded_at": "2026-08-26T10:00:00+00:00",
        "row_count": len(rows),
        "checksum": adapters.canonical_payload_sha256(rows),
    }
    return spec, receipt, rows


def _event_rows(authority: str) -> list[dict[str, object]]:
    if authority == "FEDERAL_RESERVE":
        return [
            {
                "stable_event_key": "fed:fomc:2026-08-27",
                "event_type": "FOMC_RATE_DECISION",
                "revision_id": "r1",
                "revision_action": "UPSERT",
                "scheduled_for": "2026-08-27T14:00:00-04:00",
                "source_published_at": "2026-08-20T09:00:00-04:00",
                "captured_at": "2026-08-25T10:00:00-04:00",
                "available_at": "2026-08-25T10:01:00-04:00",
            }
        ]
    if authority == "BLS":
        return [
            {
                "stable_event_key": "bls:cpi:2026-08-28",
                "event_type": "CPI",
                "revision_id": "r1",
                "revision_action": "UPSERT",
                "scheduled_for": "2026-08-28T08:30:00-04:00",
                "source_published_at": "2026-08-19T09:00:00-04:00",
                "captured_at": "2026-08-25T10:00:00-04:00",
                "available_at": "2026-08-25T10:01:00-04:00",
            },
            {
                "stable_event_key": "bls:nfp:2026-09-04",
                "event_type": "NONFARM_PAYROLLS",
                "revision_id": "r1",
                "revision_action": "UPSERT",
                "scheduled_for": "2026-09-04T08:30:00-04:00",
                "source_published_at": "2026-08-19T09:05:00-04:00",
                "captured_at": "2026-08-25T10:00:00-04:00",
                "available_at": "2026-08-25T10:01:00-04:00",
            },
        ]
    return []


def _event_fixture(
    adapter_id: str,
) -> tuple[contract.AdapterSpec, dict[str, object], list[dict[str, object]]]:
    authority_by_adapter = {
        "FederalReserveFomcSchedulePITReceiptAdapter": "FEDERAL_RESERVE",
        "BlsReleaseSchedulePITReceiptAdapter": "BLS",
        "BeaReleaseSchedulePITReceiptAdapter": "BEA",
    }
    spec = _spec(adapter_id)
    authority = authority_by_adapter[adapter_id]
    rows = _event_rows(authority)
    receipt: dict[str, object] = {
        "schema_version": "pit_official_event_capture_receipt.v1",
        "candidate_id": spec.candidate_id,
        "authority": authority,
        "source_id": spec.source_id,
        "endpoint": spec.endpoint,
        "request_parameters": {
            "event_types": list(spec.event_taxonomy),
            "capture_mode": "official_schedule",
            "coverage_start": "2026-08-01",
            "coverage_end": "2026-08-31",
        },
        "session_timezone": "America/New_York",
        "captured_at": "2026-08-25T10:00:00-04:00",
        "available_at": "2026-08-25T10:01:00-04:00",
        "coverage_through": "2026-08-31T23:59:00-04:00",
        "row_count": len(rows),
        "checksum": adapters.canonical_payload_sha256(rows),
    }
    return spec, receipt, rows


def _adapt_event(adapter_id: str) -> adapters.NormalizedEventReceipt:
    spec, receipt, rows = _event_fixture(adapter_id)
    return adapters.adapt_official_event_capture_receipt(
        spec=spec,
        receipt=receipt,
        rows=rows,
        coverage_start="2026-08-01",
        required_coverage_through=_REQUIRED_EVENT_COVERAGE,
        decision_as_of=_DECISION_AS_OF,
    )


def test_contract_replays_exact_s7_and_keeps_admission_closed() -> None:
    loaded = contract.load_mandatory_veto_pit_receipt_adapter_contract()

    assert loaded.s7.file_sha256 == (
        "d4e431350c0220934d48482e1cfd02287b06f291f8903f58901d75735d8b1636"
    )
    assert loaded.s7.canonical_sha256 == (
        "3344d14fd7b94b6951a8f676e77674c50b1dbe38820f83b6c45f96d4727a8405"
    )
    assert loaded.terminal == (
        "SYNTHETIC_PIT_RECEIPT_ADAPTER_CONFORMANCE_READY_4_OF_4_"
        "REAL_SOURCE_UNADMITTED_0_OF_4"
    )
    assert tuple(row.veto_id for row in loaded.policy.veto_bindings) == (
        "broad_market_risk_off_veto",
        "realized_volatility_veto",
        "scheduled_event_risk_veto",
        "underlying_trend_break_veto",
    )
    assert all(row.synthetic_adapter_conformance_ready for row in loaded.policy.veto_bindings)
    assert not any(row.adapter_implementation_admitted for row in loaded.policy.veto_bindings)
    assert not any(row.real_source_identity_admitted for row in loaded.policy.veto_bindings)
    assert not any(
        row.exact_1202_session_inventory_admitted for row in loaded.policy.veto_bindings
    )
    assert all(row.observed_inventory_lf_sha256 is None for row in loaded.policy.veto_bindings)
    assert all(row.observed_manifest_sha256 is None for row in loaded.policy.veto_bindings)


def test_pure_adapter_source_has_no_network_cache_provider_or_filesystem_io() -> None:
    tree = ast.parse(inspect.getsource(adapters))
    imported_roots: set[str] = set()
    called_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".")[0])
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            called_names.add(node.func.id)

    assert imported_roots.isdisjoint(
        {"pathlib", "requests", "urllib", "httpx", "aiohttp", "socket", "pandas"}
    )
    assert "open" not in called_names


def test_safety_surface_allows_only_injected_synthetic_adapter_execution() -> None:
    safety = _policy().safety

    assert safety.non_executable_data_research_only
    assert safety.pure_adapter_execution_on_injected_synthetic_payload_allowed
    assert safety.production_effect == "none"
    assert safety.broker_action == "none"
    closed_flags = (
        safety.filesystem_market_data_read_allowed,
        safety.network_io_allowed,
        safety.provider_query_authorized,
        safety.cache_read_authorized,
        safety.real_data_read_authorized,
        safety.real_payload_adapter_execution_authorized,
        safety.real_source_admission_allowed,
        safety.exact_inventory_admission_allowed,
        safety.manifest_replay_allowed,
        safety.veto_series_generation_allowed,
        safety.r1_manifest_generation_allowed,
        safety.real_dq_authorized,
        safety.backtest_authorized,
        safety.parameter_or_threshold_search_allowed,
        safety.constant_false_fill_allowed,
        safety.missing_as_clear_allowed,
        safety.cross_date_fallback_allowed,
        safety.orders_allowed,
        safety.fills_allowed,
        safety.positions_allowed,
        safety.paper_allowed,
        safety.live_allowed,
    )
    assert not any(closed_flags)


@pytest.mark.parametrize("ticker", ["SPY", "QQQ"])
def test_fmp_price_receipt_normalizes_exact_injected_sessions(ticker: str) -> None:
    spec, receipt, rows = _fmp_fixture(ticker)

    normalized = adapters.adapt_fmp_price_receipt(
        spec=spec,
        receipt=receipt,
        rows=rows,
        expected_sessions=_SESSIONS,
        decision_as_of=_DECISION_AS_OF,
    )

    assert normalized.ticker == ticker
    assert normalized.checksum == adapters.canonical_payload_sha256(rows)
    assert tuple(row.session.isoformat() for row in normalized.rows) == _SESSIONS
    assert normalized.rows[-1].dividend_adjusted_close == 100.5


@pytest.mark.parametrize(
    ("mutation", "reason_code"),
    [
        ("unknown_receipt_field", "PIT_RECEIPT_SCHEMA_INVALID"),
        ("checksum_drift", "PIT_RECEIPT_CHECKSUM_MISMATCH"),
        ("naive_downloaded_at", "PIT_RECEIPT_TIMESTAMP_INVALID"),
        ("session_gap", "PIT_RECEIPT_SESSION_INVENTORY_MISMATCH"),
        ("nan_close", "PIT_RECEIPT_PAYLOAD_INVALID"),
    ],
)
def test_fmp_price_receipt_fails_closed(mutation: str, reason_code: str) -> None:
    spec, receipt, rows = _fmp_fixture("SPY")
    if mutation == "unknown_receipt_field":
        receipt["unexpected"] = "forbidden"
    elif mutation == "checksum_drift":
        receipt["checksum"] = "0" * 64
    elif mutation == "naive_downloaded_at":
        receipt["downloaded_at"] = "2026-08-26T10:00:00"
    elif mutation == "session_gap":
        rows[1]["session"] = "2021-02-24"
        rows[1]["available_at"] = "2021-02-24T21:30:00-05:00"
        receipt["checksum"] = adapters.canonical_payload_sha256(rows)
    elif mutation == "nan_close":
        rows[0]["raw_close"] = float("nan")

    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as error:
        adapters.adapt_fmp_price_receipt(
            spec=spec,
            receipt=receipt,
            rows=rows,
            expected_sessions=_SESSIONS,
            decision_as_of=_DECISION_AS_OF,
        )

    assert error.value.reason_code == reason_code


def test_cboe_vix_receipt_preserves_exact_session_join_and_close_identity() -> None:
    spec, receipt, rows = _vix_fixture()

    normalized = adapters.adapt_cboe_vix_receipt(
        spec=spec,
        receipt=receipt,
        rows=rows,
        expected_sessions=_SESSIONS,
        decision_as_of=_DECISION_AS_OF,
    )

    assert tuple(row.session.isoformat() for row in normalized.rows) == _SESSIONS
    assert tuple(row.close for row in normalized.rows) == (23.45, 22.31)


def test_cboe_vix_receipt_rejects_adjusted_close_or_cross_date_fill() -> None:
    spec, receipt, rows = _vix_fixture()
    rows[0]["adjusted_close"] = 23.44
    receipt["checksum"] = adapters.canonical_payload_sha256(rows)

    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as error:
        adapters.adapt_cboe_vix_receipt(
            spec=spec,
            receipt=receipt,
            rows=rows,
            expected_sessions=_SESSIONS,
            decision_as_of=_DECISION_AS_OF,
        )

    assert error.value.reason_code == "PIT_RECEIPT_ADJUSTMENT_INVALID"


def test_three_official_event_receipts_bind_with_empty_bea_rows_as_coverage_proof() -> None:
    receipts = tuple(
        _adapt_event(adapter_id)
        for adapter_id in (
            "FederalReserveFomcSchedulePITReceiptAdapter",
            "BlsReleaseSchedulePITReceiptAdapter",
            "BeaReleaseSchedulePITReceiptAdapter",
        )
    )

    bundle = adapters.bind_official_event_receipt_bundle(
        receipts, required_coverage_through=_REQUIRED_EVENT_COVERAGE
    )

    assert tuple(receipt.authority for receipt in bundle.receipts) == (
        "FEDERAL_RESERVE",
        "BLS",
        "BEA",
    )
    assert bundle.receipts[-1].revisions == ()
    assert bundle.coverage_through >= _REQUIRED_EVENT_COVERAGE


def test_event_receipt_rejects_unknown_taxonomy_and_same_published_at_conflict() -> None:
    spec, receipt, rows = _event_fixture("FederalReserveFomcSchedulePITReceiptAdapter")
    rows[0]["event_type"] = "UNSCHEDULED_INTERVENTION"
    receipt["checksum"] = adapters.canonical_payload_sha256(rows)
    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as taxonomy_error:
        adapters.adapt_official_event_capture_receipt(
            spec=spec,
            receipt=receipt,
            rows=rows,
            coverage_start="2026-08-01",
            required_coverage_through=_REQUIRED_EVENT_COVERAGE,
            decision_as_of=_DECISION_AS_OF,
        )
    assert taxonomy_error.value.reason_code == "PIT_RECEIPT_EVENT_TAXONOMY_INVALID"

    spec, receipt, rows = _event_fixture("FederalReserveFomcSchedulePITReceiptAdapter")
    conflicting = copy.deepcopy(rows[0])
    conflicting["revision_id"] = "r2"
    conflicting["revision_action"] = "RESCHEDULE"
    conflicting["scheduled_for"] = "2026-08-27T15:00:00-04:00"
    rows.append(conflicting)
    receipt["row_count"] = len(rows)
    receipt["checksum"] = adapters.canonical_payload_sha256(rows)
    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as conflict_error:
        adapters.adapt_official_event_capture_receipt(
            spec=spec,
            receipt=receipt,
            rows=rows,
            coverage_start="2026-08-01",
            required_coverage_through=_REQUIRED_EVENT_COVERAGE,
            decision_as_of=_DECISION_AS_OF,
        )
    assert conflict_error.value.reason_code == (
        "PIT_RECEIPT_EVENT_SAME_PUBLISHED_AT_CONFLICT"
    )


def test_event_bundle_rejects_missing_authority_receipt() -> None:
    receipts = (
        _adapt_event("FederalReserveFomcSchedulePITReceiptAdapter"),
        _adapt_event("BlsReleaseSchedulePITReceiptAdapter"),
    )

    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as error:
        adapters.bind_official_event_receipt_bundle(
            receipts, required_coverage_through=_REQUIRED_EVENT_COVERAGE
        )

    assert error.value.reason_code == "PIT_RECEIPT_EVENT_AUTHORITY_INCOMPLETE"


def test_trend_binding_reuses_qqq_receipt_without_admitting_observed_checkpoint() -> None:
    spec, receipt, rows = _fmp_fixture("QQQ")
    qqq_receipt = adapters.adapt_fmp_price_receipt(
        spec=spec,
        receipt=receipt,
        rows=rows,
        expected_sessions=_SESSIONS,
        decision_as_of=_DECISION_AS_OF,
    )
    binding: dict[str, object] = {
        "schema_version": "pit_trend_consumer_binding.v1",
        "veto_id": "underlying_trend_break_veto",
        "consumer_binding_id": "QQQ_TREND_STATE_CHECKPOINT",
        "source_receipt_checksum": qqq_receipt.checksum,
        "replay_start": "2021-02-01",
        "initial_checkpoint_sha256": "1" * 64,
        "target_start_checkpoint_sha256": "2" * 64,
        "state_transition_lineage_sha256": "3" * 64,
        "synthetic_fixture_only": True,
        "adapter_implementation_admitted": False,
    }

    normalized = adapters.bind_trend_consumer_receipt(binding, qqq_receipt=qqq_receipt)

    assert normalized.source_receipt_checksum == qqq_receipt.checksum
    assert normalized.replay_start.isoformat() == "2021-02-01"

    binding["adapter_implementation_admitted"] = True
    with pytest.raises(adapters.MandatoryVetoPITReceiptAdapterError) as error:
        adapters.bind_trend_consumer_receipt(binding, qqq_receipt=qqq_receipt)
    assert error.value.reason_code == "PIT_RECEIPT_ADMISSION_FORBIDDEN"


def test_contract_rejects_partial_admission_and_unknown_surface() -> None:
    payload = _payload()
    payload["veto_bindings"][0]["adapter_implementation_admitted"] = True
    with pytest.raises(ValidationError):
        contract.MandatoryVetoPITReceiptAdapterContract.model_validate(payload)

    payload = _payload()
    payload["adapter_specs"][0]["network_client"] = "forbidden"
    with pytest.raises(ValidationError):
        contract.MandatoryVetoPITReceiptAdapterContract.model_validate(payload)
