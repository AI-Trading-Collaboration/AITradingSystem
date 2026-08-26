from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from typing import Final

from ai_trading_system.qqq_options_research import (
    growth_action_value_mandatory_veto_pit_receipt_adapter_contract as adapter_contract,
)

_SHA256: Final = re.compile(r"^[0-9a-f]{64}$")
_EVENT_AUTHORITY_BY_ADAPTER: Final = {
    "FederalReserveFomcSchedulePITReceiptAdapter": "FEDERAL_RESERVE",
    "BlsReleaseSchedulePITReceiptAdapter": "BLS",
    "BeaReleaseSchedulePITReceiptAdapter": "BEA",
}
_EVENT_AUTHORITY_ORDER: Final = ("FEDERAL_RESERVE", "BLS", "BEA")
_FMP_ADJUSTMENT_BASIS: Final = (
    "FMP_NON_SPLIT_RAW_PLUS_DIVIDEND_ADJUSTED_CLOSE"
)
_CBOE_ADJUSTMENT_BASIS: Final = (
    "CBOE_VIX_INDEX_LEVEL_UNADJUSTED_CLOSE_EQUALS_ADJUSTED_CLOSE"
)
_CBOE_LEVEL_DEFINITION: Final = "CBOE_VIX_OFFICIAL_DAILY_CLOSE"
_CBOE_REVISION_POLICY: Final = "IMMUTABLE_CAPTURE_SNAPSHOT_NO_FILL_OR_OVERRIDE"


class MandatoryVetoPITReceiptAdapterError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class EventRevisionAction(StrEnum):
    UPSERT = "UPSERT"
    RESCHEDULE = "RESCHEDULE"
    CANCEL = "CANCEL"


@dataclass(frozen=True)
class NormalizedPriceRow:
    session: date
    ticker: str
    provider_symbol_alias: str
    raw_close: float
    dividend_adjusted_close: float
    available_at: datetime


@dataclass(frozen=True)
class NormalizedPriceReceipt:
    candidate_id: str
    ticker: str
    source_id: str
    adjustment_vintage: datetime
    available_at: datetime
    downloaded_at: datetime
    checksum: str
    rows: tuple[NormalizedPriceRow, ...]


@dataclass(frozen=True)
class NormalizedVixRow:
    session: date
    close: float
    available_at: datetime


@dataclass(frozen=True)
class NormalizedVixReceipt:
    candidate_id: str
    source_id: str
    available_at: datetime
    downloaded_at: datetime
    checksum: str
    rows: tuple[NormalizedVixRow, ...]


@dataclass(frozen=True)
class NormalizedEventRevision:
    authority: str
    source_id: str
    stable_event_key: str
    event_type: str
    revision_id: str
    revision_action: EventRevisionAction
    scheduled_for: datetime
    source_published_at: datetime
    captured_at: datetime
    available_at: datetime


@dataclass(frozen=True)
class NormalizedEventReceipt:
    candidate_id: str
    authority: str
    source_id: str
    captured_at: datetime
    available_at: datetime
    coverage_through: datetime
    checksum: str
    revisions: tuple[NormalizedEventRevision, ...]


@dataclass(frozen=True)
class NormalizedEventReceiptBundle:
    receipts: tuple[NormalizedEventReceipt, NormalizedEventReceipt, NormalizedEventReceipt]
    coverage_through: datetime


@dataclass(frozen=True)
class NormalizedTrendConsumerBinding:
    source_receipt_checksum: str
    replay_start: date
    initial_checkpoint_sha256: str
    target_start_checkpoint_sha256: str
    state_transition_lineage_sha256: str


def _canonical_payload_bytes(rows: Sequence[Mapping[str, object]]) -> bytes:
    serializable: list[dict[str, object]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_PAYLOAD_INVALID", f"row[{index}] must be a mapping"
            )
        if not all(isinstance(key, str) for key in row):
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_PAYLOAD_INVALID", f"row[{index}] keys must be strings"
            )
        serializable.append(dict(row))
    try:
        return (
            json.dumps(
                serializable,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_PAYLOAD_INVALID", str(exc)
        ) from exc


def canonical_payload_sha256(rows: Sequence[Mapping[str, object]]) -> str:
    """Return the deterministic checksum used by injected receipt fixtures."""

    return hashlib.sha256(_canonical_payload_bytes(rows)).hexdigest()


def _strict_mapping(
    value: object, *, expected_keys: Sequence[str], label: str
) -> dict[str, object]:
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    result = dict(value)
    expected = set(expected_keys)
    observed = set(result)
    if observed != expected:
        missing = ",".join(sorted(expected - observed)) or "none"
        unknown = ",".join(sorted(observed - expected)) or "none"
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SCHEMA_INVALID",
            f"{label} keys drifted; missing={missing}; unknown={unknown}",
        )
    return result


def _string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_FIELD_INVALID", f"{field} must be a non-empty string"
        )
    return value


def _integer(value: object, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_FIELD_INVALID", f"{field} must be a non-negative integer"
        )
    return value


def _boolean(value: object, *, field: str) -> bool:
    if not isinstance(value, bool):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_FIELD_INVALID", f"{field} must be boolean"
        )
    return value


def _positive_number(value: object, *, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_VALUE_INVALID", f"{field} must be numeric"
        )
    converted = float(value)
    if not math.isfinite(converted) or converted <= 0:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_VALUE_INVALID", f"{field} must be finite and positive"
        )
    return converted


def _timestamp(value: object, *, field: str) -> datetime:
    text = _string(value, field=field)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_INVALID", f"{field} is not ISO-8601"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_INVALID", f"{field} must be timezone-aware"
        )
    return parsed


def _aware_clock(value: datetime, *, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_INVALID", f"{field} must be timezone-aware"
        )
    return value


def _session(value: object, *, field: str) -> date:
    text = _string(value, field=field)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SESSION_INVALID", f"{field} must be YYYY-MM-DD"
        ) from exc


def _sha256(value: object, *, field: str) -> str:
    text = _string(value, field=field)
    if not _SHA256.fullmatch(text):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_CHECKSUM_INVALID", f"{field} must be lowercase SHA-256"
        )
    return text


def _expected_sessions(values: Sequence[str]) -> tuple[date, ...]:
    if isinstance(values, (str, bytes)) or not values:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SESSION_INVALID", "expected_sessions must not be empty"
        )
    sessions = tuple(
        _session(value, field=f"expected_sessions[{index}]")
        for index, value in enumerate(values)
    )
    if sessions != tuple(sorted(set(sessions))):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SESSION_INVALID",
            "expected_sessions must be unique and strictly increasing",
        )
    return sessions


def _identity(receipt: Mapping[str, object], spec: adapter_contract.AdapterSpec) -> None:
    expected_pairs = {
        "candidate_id": spec.candidate_id,
        "source_id": spec.source_id,
        "endpoint": spec.endpoint,
    }
    for field, expected in expected_pairs.items():
        if _string(receipt[field], field=field) != expected:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_IDENTITY_DRIFT", f"{field} does not match frozen contract"
            )


def _request_parameters(
    value: object,
    *,
    spec: adapter_contract.AdapterSpec,
    dynamic_expected: Mapping[str, object],
) -> dict[str, object]:
    params = _strict_mapping(
        value,
        expected_keys=spec.request_parameter_keys,
        label="request_parameters",
    )
    expected: dict[str, object] = dict(spec.fixed_request_parameters)
    expected.update(dynamic_expected)
    if params != expected:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_REQUEST_DRIFT", "request_parameters do not match frozen contract"
        )
    return params


def _receipt_checksum(
    receipt: Mapping[str, object], rows: Sequence[Mapping[str, object]]
) -> str:
    expected = _sha256(receipt["checksum"], field="checksum")
    actual = canonical_payload_sha256(rows)
    if actual != expected:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_CHECKSUM_MISMATCH", f"expected={expected}; actual={actual}"
        )
    return actual


def adapt_fmp_price_receipt(
    *,
    spec: adapter_contract.AdapterSpec,
    receipt: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    expected_sessions: Sequence[str],
    decision_as_of: datetime,
) -> NormalizedPriceReceipt:
    if spec.adapter_id != "FmpPricePITReceiptAdapter" or spec.adapter_kind != "PRICE":
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADAPTER_SPEC_INVALID", "FMP adapter spec is not frozen"
        )
    clock = _aware_clock(decision_as_of, field="decision_as_of")
    expected = _expected_sessions(expected_sessions)
    payload = _strict_mapping(
        receipt, expected_keys=spec.receipt_field_names, label="FMP receipt"
    )
    _identity(payload, spec)
    if _string(payload["schema_version"], field="schema_version") != "pit_price_source_receipt.v1":
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SCHEMA_INVALID", "FMP schema_version drifted"
        )
    if _string(payload["provider"], field="provider") != spec.provider_or_authority:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_IDENTITY_DRIFT", "FMP provider drifted"
        )
    ticker = _string(payload["ticker"], field="ticker")
    symbol = _string(payload["provider_symbol_alias"], field="provider_symbol_alias")
    if ticker not in spec.allowed_tickers or symbol != ticker:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TICKER_INVALID", "FMP ticker or provider alias is not frozen"
        )
    if _string(payload["adjustment_basis"], field="adjustment_basis") != _FMP_ADJUSTMENT_BASIS:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADJUSTMENT_INVALID", "FMP adjustment basis drifted"
        )
    if _string(payload["session_timezone"], field="session_timezone") != spec.timestamp_timezone:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMEZONE_INVALID", "FMP session timezone drifted"
        )
    _request_parameters(
        payload["request_parameters"],
        spec=spec,
        dynamic_expected={
            "symbol": symbol,
            "from": expected[0].isoformat(),
            "to": expected[-1].isoformat(),
        },
    )
    adjustment_vintage = _timestamp(payload["adjustment_vintage"], field="adjustment_vintage")
    available_at = _timestamp(payload["available_at"], field="available_at")
    downloaded_at = _timestamp(payload["downloaded_at"], field="downloaded_at")
    if not (adjustment_vintage <= downloaded_at and available_at <= downloaded_at <= clock):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
            "FMP adjustment/availability/download/decision order is invalid",
        )
    if _integer(payload["row_count"], field="row_count") != len(rows):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ROW_COUNT_MISMATCH", "FMP row_count does not match payload"
        )
    checksum = _receipt_checksum(payload, rows)
    normalized: list[NormalizedPriceRow] = []
    for index, raw_row in enumerate(rows):
        row = _strict_mapping(
            raw_row, expected_keys=spec.row_field_names, label=f"FMP row[{index}]"
        )
        session = _session(row["session"], field=f"row[{index}].session")
        row_ticker = _string(row["ticker"], field=f"row[{index}].ticker")
        row_symbol = _string(
            row["provider_symbol_alias"], field=f"row[{index}].provider_symbol_alias"
        )
        row_available = _timestamp(
            row["available_at"], field=f"row[{index}].available_at"
        )
        if row_ticker != ticker or row_symbol != symbol:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TICKER_INVALID", f"FMP row[{index}] ticker drifted"
            )
        if row_available > available_at or row_available.date() < session:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
                f"FMP row[{index}] availability is not PIT-valid",
            )
        normalized.append(
            NormalizedPriceRow(
                session=session,
                ticker=ticker,
                provider_symbol_alias=symbol,
                raw_close=_positive_number(
                    row["raw_close"], field=f"row[{index}].raw_close"
                ),
                dividend_adjusted_close=_positive_number(
                    row["dividend_adjusted_close"],
                    field=f"row[{index}].dividend_adjusted_close",
                ),
                available_at=row_available,
            )
        )
    if tuple(row.session for row in normalized) != expected:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SESSION_INVENTORY_MISMATCH",
            "FMP rows contain a duplicate, gap, conflict, or unexpected order",
        )
    return NormalizedPriceReceipt(
        candidate_id=spec.candidate_id,
        ticker=ticker,
        source_id=spec.source_id,
        adjustment_vintage=adjustment_vintage,
        available_at=available_at,
        downloaded_at=downloaded_at,
        checksum=checksum,
        rows=tuple(normalized),
    )


def adapt_cboe_vix_receipt(
    *,
    spec: adapter_contract.AdapterSpec,
    receipt: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    expected_sessions: Sequence[str],
    decision_as_of: datetime,
) -> NormalizedVixReceipt:
    if spec.adapter_id != "CboeVixPITReceiptAdapter" or spec.adapter_kind != "VIX":
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADAPTER_SPEC_INVALID", "Cboe adapter spec is not frozen"
        )
    clock = _aware_clock(decision_as_of, field="decision_as_of")
    expected = _expected_sessions(expected_sessions)
    payload = _strict_mapping(
        receipt, expected_keys=spec.receipt_field_names, label="Cboe receipt"
    )
    _identity(payload, spec)
    fixed_identity = {
        "schema_version": "pit_vix_source_receipt.v1",
        "provider": spec.provider_or_authority,
        "ticker": "VIX",
        "adjustment_basis": _CBOE_ADJUSTMENT_BASIS,
        "session_timezone": spec.timestamp_timezone,
        "level_definition": _CBOE_LEVEL_DEFINITION,
        "revision_policy": _CBOE_REVISION_POLICY,
    }
    for field, expected_value in fixed_identity.items():
        if _string(payload[field], field=field) != expected_value:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_IDENTITY_DRIFT", f"Cboe {field} drifted"
            )
    _request_parameters(
        payload["request_parameters"],
        spec=spec,
        dynamic_expected={
            "ticker": "VIX",
            "from": expected[0].isoformat(),
            "to": expected[-1].isoformat(),
        },
    )
    available_at = _timestamp(payload["available_at"], field="available_at")
    downloaded_at = _timestamp(payload["downloaded_at"], field="downloaded_at")
    if not (available_at <= downloaded_at <= clock):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
            "Cboe availability/download/decision order is invalid",
        )
    if _integer(payload["row_count"], field="row_count") != len(rows):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ROW_COUNT_MISMATCH", "Cboe row_count does not match payload"
        )
    checksum = _receipt_checksum(payload, rows)
    normalized: list[NormalizedVixRow] = []
    for index, raw_row in enumerate(rows):
        row = _strict_mapping(
            raw_row, expected_keys=spec.row_field_names, label=f"Cboe row[{index}]"
        )
        session = _session(row["session"], field=f"row[{index}].session")
        if _string(row["ticker"], field=f"row[{index}].ticker") != "VIX":
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TICKER_INVALID", f"Cboe row[{index}] ticker drifted"
            )
        close = _positive_number(row["close"], field=f"row[{index}].close")
        adjusted = _positive_number(
            row["adjusted_close"], field=f"row[{index}].adjusted_close"
        )
        if close != adjusted:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_ADJUSTMENT_INVALID",
                f"Cboe row[{index}] close must equal adjusted_close",
            )
        row_available = _timestamp(
            row["available_at"], field=f"row[{index}].available_at"
        )
        if row_available > available_at or row_available.date() < session:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
                f"Cboe row[{index}] availability is not PIT-valid",
            )
        normalized.append(
            NormalizedVixRow(session=session, close=close, available_at=row_available)
        )
    if tuple(row.session for row in normalized) != expected:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_SESSION_INVENTORY_MISMATCH",
            "Cboe rows contain a duplicate, gap, conflict, or cross-date mapping",
        )
    return NormalizedVixReceipt(
        candidate_id=spec.candidate_id,
        source_id=spec.source_id,
        available_at=available_at,
        downloaded_at=downloaded_at,
        checksum=checksum,
        rows=tuple(normalized),
    )


def adapt_official_event_capture_receipt(
    *,
    spec: adapter_contract.AdapterSpec,
    receipt: Mapping[str, object],
    rows: Sequence[Mapping[str, object]],
    coverage_start: str,
    required_coverage_through: datetime,
    decision_as_of: datetime,
) -> NormalizedEventReceipt:
    if spec.adapter_kind != "EVENT" or spec.adapter_id not in _EVENT_AUTHORITY_BY_ADAPTER:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADAPTER_SPEC_INVALID", "event adapter spec is not frozen"
        )
    clock = _aware_clock(decision_as_of, field="decision_as_of")
    required_coverage = _aware_clock(
        required_coverage_through, field="required_coverage_through"
    )
    coverage_start_date = _session(coverage_start, field="coverage_start")
    payload = _strict_mapping(
        receipt, expected_keys=spec.receipt_field_names, label=f"{spec.adapter_id} receipt"
    )
    _identity(payload, spec)
    authority = _EVENT_AUTHORITY_BY_ADAPTER[spec.adapter_id]
    fixed_identity = {
        "schema_version": "pit_official_event_capture_receipt.v1",
        "authority": authority,
        "session_timezone": spec.timestamp_timezone,
    }
    for field, expected_value in fixed_identity.items():
        if _string(payload[field], field=field) != expected_value:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_IDENTITY_DRIFT", f"event {field} drifted"
            )
    captured_at = _timestamp(payload["captured_at"], field="captured_at")
    available_at = _timestamp(payload["available_at"], field="available_at")
    coverage_through = _timestamp(payload["coverage_through"], field="coverage_through")
    if not (captured_at <= available_at <= clock):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
            "event capture/availability/decision order is invalid",
        )
    if coverage_through < required_coverage:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_COVERAGE_INSUFFICIENT", f"{authority} coverage is incomplete"
        )
    if coverage_start_date > coverage_through.date():
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_COVERAGE_INSUFFICIENT", "coverage_start is after coverage_end"
        )
    _request_parameters(
        payload["request_parameters"],
        spec=spec,
        dynamic_expected={
            "event_types": list(spec.event_taxonomy),
            "coverage_start": coverage_start_date.isoformat(),
            "coverage_end": coverage_through.date().isoformat(),
        },
    )
    if _integer(payload["row_count"], field="row_count") != len(rows):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ROW_COUNT_MISMATCH", "event row_count does not match payload"
        )
    checksum = _receipt_checksum(payload, rows)
    normalized: list[NormalizedEventRevision] = []
    for index, raw_row in enumerate(rows):
        row = _strict_mapping(
            raw_row,
            expected_keys=spec.row_field_names,
            label=f"{authority} row[{index}]",
        )
        event_type = _string(row["event_type"], field=f"row[{index}].event_type")
        if event_type not in spec.event_taxonomy:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_TAXONOMY_INVALID",
                f"{authority} row[{index}] event_type is not frozen",
            )
        try:
            action = EventRevisionAction(
                _string(row["revision_action"], field=f"row[{index}].revision_action")
            )
        except ValueError as exc:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_REVISION_INVALID",
                f"{authority} row[{index}] revision_action is invalid",
            ) from exc
        source_published_at = _timestamp(
            row["source_published_at"], field=f"row[{index}].source_published_at"
        )
        row_captured_at = _timestamp(
            row["captured_at"], field=f"row[{index}].captured_at"
        )
        row_available_at = _timestamp(
            row["available_at"], field=f"row[{index}].available_at"
        )
        if not (
            source_published_at <= row_captured_at == captured_at
            and row_available_at == available_at
            and available_at <= clock
        ):
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TIMESTAMP_ORDER_INVALID",
                f"{authority} row[{index}] publication/capture/availability order is invalid",
            )
        normalized.append(
            NormalizedEventRevision(
                authority=authority,
                source_id=spec.source_id,
                stable_event_key=_string(
                    row["stable_event_key"], field=f"row[{index}].stable_event_key"
                ),
                event_type=event_type,
                revision_id=_string(
                    row["revision_id"], field=f"row[{index}].revision_id"
                ),
                revision_action=action,
                scheduled_for=_timestamp(
                    row["scheduled_for"], field=f"row[{index}].scheduled_for"
                ),
                source_published_at=source_published_at,
                captured_at=row_captured_at,
                available_at=row_available_at,
            )
        )
    expected_order = tuple(
        sorted(
            normalized,
            key=lambda row: (
                row.stable_event_key,
                row.source_published_at,
                row.revision_id,
            ),
        )
    )
    if tuple(normalized) != expected_order:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_EVENT_ORDER_INVALID",
            f"{authority} rows are not in deterministic revision order",
        )
    by_key: dict[str, list[NormalizedEventRevision]] = {}
    for revision in normalized:
        by_key.setdefault(revision.stable_event_key, []).append(revision)
    for stable_key, revisions in by_key.items():
        if revisions[0].revision_action is not EventRevisionAction.UPSERT:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_REVISION_INVALID",
                f"{stable_key} must begin with UPSERT",
            )
        if len({row.revision_id for row in revisions}) != len(revisions):
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_REVISION_INVALID",
                f"{stable_key} contains duplicate revision_id",
            )
        if len({row.source_published_at for row in revisions}) != len(revisions):
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_SAME_PUBLISHED_AT_CONFLICT",
                f"{stable_key} contains a same-published-at conflict",
            )
        if len({row.event_type for row in revisions}) != 1:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_EVENT_REVISION_INVALID",
                f"{stable_key} changes event_type across revisions",
            )
        cancelled = False
        previous_scheduled = revisions[0].scheduled_for
        for revision in revisions[1:]:
            if cancelled:
                raise MandatoryVetoPITReceiptAdapterError(
                    "PIT_RECEIPT_EVENT_REVISION_INVALID",
                    f"{stable_key} has a revision after CANCEL",
                )
            if (
                revision.revision_action is EventRevisionAction.RESCHEDULE
                and revision.scheduled_for == previous_scheduled
            ):
                raise MandatoryVetoPITReceiptAdapterError(
                    "PIT_RECEIPT_EVENT_REVISION_INVALID",
                    f"{stable_key} RESCHEDULE does not change scheduled_for",
                )
            cancelled = revision.revision_action is EventRevisionAction.CANCEL
            previous_scheduled = revision.scheduled_for
    return NormalizedEventReceipt(
        candidate_id=spec.candidate_id,
        authority=authority,
        source_id=spec.source_id,
        captured_at=captured_at,
        available_at=available_at,
        coverage_through=coverage_through,
        checksum=checksum,
        revisions=tuple(normalized),
    )


def bind_official_event_receipt_bundle(
    receipts: Sequence[NormalizedEventReceipt],
    *,
    required_coverage_through: datetime,
) -> NormalizedEventReceiptBundle:
    required = _aware_clock(required_coverage_through, field="required_coverage_through")
    if tuple(receipt.authority for receipt in receipts) != _EVENT_AUTHORITY_ORDER:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_EVENT_AUTHORITY_INCOMPLETE",
            "event receipt bundle must contain FEDERAL_RESERVE, BLS, and BEA exactly once",
        )
    if any(receipt.coverage_through < required for receipt in receipts):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_COVERAGE_INSUFFICIENT",
            "event receipt bundle does not cover the required action cutoff",
        )
    return NormalizedEventReceiptBundle(
        receipts=(receipts[0], receipts[1], receipts[2]),
        coverage_through=min(receipt.coverage_through for receipt in receipts),
    )


def bind_trend_consumer_receipt(
    binding: Mapping[str, object],
    *,
    qqq_receipt: NormalizedPriceReceipt,
) -> NormalizedTrendConsumerBinding:
    if qqq_receipt.ticker != "QQQ" or not qqq_receipt.rows:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TREND_BINDING_INVALID", "trend binding requires a QQQ receipt"
        )
    expected_fields = (
        "schema_version",
        "veto_id",
        "consumer_binding_id",
        "source_receipt_checksum",
        "replay_start",
        "initial_checkpoint_sha256",
        "target_start_checkpoint_sha256",
        "state_transition_lineage_sha256",
        "synthetic_fixture_only",
        "adapter_implementation_admitted",
    )
    payload = _strict_mapping(
        binding, expected_keys=expected_fields, label="trend consumer binding"
    )
    fixed_identity = {
        "schema_version": "pit_trend_consumer_binding.v1",
        "veto_id": "underlying_trend_break_veto",
        "consumer_binding_id": "QQQ_TREND_STATE_CHECKPOINT",
    }
    for field, expected in fixed_identity.items():
        if _string(payload[field], field=field) != expected:
            raise MandatoryVetoPITReceiptAdapterError(
                "PIT_RECEIPT_TREND_BINDING_INVALID", f"trend {field} drifted"
            )
    source_checksum = _sha256(
        payload["source_receipt_checksum"], field="source_receipt_checksum"
    )
    if source_checksum != qqq_receipt.checksum:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TREND_BINDING_INVALID",
            "trend binding does not reference the QQQ receipt checksum",
        )
    replay_start = _session(payload["replay_start"], field="replay_start")
    if replay_start > qqq_receipt.rows[0].session:
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_TREND_BINDING_INVALID",
            "trend replay_start is after the first injected source session",
        )
    if not _boolean(payload["synthetic_fixture_only"], field="synthetic_fixture_only"):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADMISSION_FORBIDDEN",
            "trend binding must remain synthetic_fixture_only",
        )
    if _boolean(
        payload["adapter_implementation_admitted"],
        field="adapter_implementation_admitted",
    ):
        raise MandatoryVetoPITReceiptAdapterError(
            "PIT_RECEIPT_ADMISSION_FORBIDDEN",
            "synthetic trend binding cannot admit an adapter implementation",
        )
    return NormalizedTrendConsumerBinding(
        source_receipt_checksum=source_checksum,
        replay_start=replay_start,
        initial_checkpoint_sha256=_sha256(
            payload["initial_checkpoint_sha256"], field="initial_checkpoint_sha256"
        ),
        target_start_checkpoint_sha256=_sha256(
            payload["target_start_checkpoint_sha256"],
            field="target_start_checkpoint_sha256",
        ),
        state_transition_lineage_sha256=_sha256(
            payload["state_transition_lineage_sha256"],
            field="state_transition_lineage_sha256",
        ),
    )


__all__ = [
    "EventRevisionAction",
    "MandatoryVetoPITReceiptAdapterError",
    "NormalizedEventReceipt",
    "NormalizedEventReceiptBundle",
    "NormalizedEventRevision",
    "NormalizedPriceReceipt",
    "NormalizedPriceRow",
    "NormalizedTrendConsumerBinding",
    "NormalizedVixReceipt",
    "NormalizedVixRow",
    "adapt_cboe_vix_receipt",
    "adapt_fmp_price_receipt",
    "adapt_official_event_capture_receipt",
    "bind_official_event_receipt_bundle",
    "bind_trend_consumer_receipt",
    "canonical_payload_sha256",
]
