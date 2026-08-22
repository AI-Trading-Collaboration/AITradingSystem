"""Offline same-date subscription recovery contract for TRADING-2541.

The module admits a provider-history record only after full-session subscription
absence has been finalized.  It also builds a zero-order QuantConnect candidate,
but it has no browser, network, Cloud, project-mutation, order, or broker path.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Final

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research import (
    exact_date_provider_catalog_attribution_correction as attribution_correction,
)
from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    Axis,
)
from ai_trading_system.qqq_options_research.daily_transport_session_finalization import (
    AxisSignal,
    SessionSliceObservation,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_exact_date_subscription_recovery_v1.yaml"
)
DEFAULT_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/trading_2541_exact_date_subscription_recovery_v1"
)
TASK_ID: Final = (
    "TRADING-2541_QC_QQQ_OPTIONS_EXACT_DATE_SUBSCRIPTION_MISSING_REMEDIATION_V1"
)
_UNSEALED_SHA256 = "0" * 64
_CHAIN_AXES = (
    Axis.BID_ASK_QUOTE,
    Axis.GREEKS,
    Axis.IMPLIED_VOLATILITY,
    Axis.OPEN_INTEREST,
    Axis.VOLUME,
)
_PACKAGE_FILES = (
    "main.py",
    "package_manifest.json",
    "proposal.json",
    "recovery_contract.json",
)


class ExactDateSubscriptionRecoveryError(ValueError):
    """Typed fail-closed recovery or package error."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


class DeliveryPath(StrEnum):
    SUBSCRIBED_SLICE = "SUBSCRIBED_SLICE"
    EXACT_DATE_PROVIDER_HISTORY_RECOVERY = "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"


class RecoveryStatus(StrEnum):
    NOT_REQUIRED = "NOT_REQUIRED"
    QUERY_PLANNED = "QUERY_PLANNED"
    ACCEPTED = "ACCEPTED"


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _seal(payload: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(payload)
    if "content_sha256" in body:
        raise ValueError("payload already contains content_sha256")
    body["content_sha256"] = _sha256(_canonical_json_bytes(body))
    return body


def _verify_seal(payload: Mapping[str, Any]) -> None:
    body = dict(payload)
    observed = body.pop("content_sha256", None)
    if observed != _sha256(_canonical_json_bytes(body)):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SEAL_INVALID", "content_sha256")


def _positive_finite(value: object) -> bool:
    if isinstance(value, bool):
        return False
    try:
        parsed = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False
    return math.isfinite(parsed) and parsed > 0


def _require_sha256(value: object, field: str) -> str:
    text = str(value)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_INVALID", field)
    return text


def _require_repo_path(value: object, field: str) -> str:
    text = str(value)
    path = Path(text)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != text:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_INVALID", field)
    return text


@dataclass(frozen=True)
class SubscriptionSessionSummary:
    """Finalized subscription facts needed before planning a provider query."""

    session_id: date
    session_finalized: bool
    equity_close: object
    subscribed_chain_event_count: int
    subscribed_contract_count: int

    def __post_init__(self) -> None:
        if self.subscribed_chain_event_count < 0 or self.subscribed_contract_count < 0:
            raise ValueError("subscription counts must be non-negative")
        if self.subscribed_chain_event_count == 0 and self.subscribed_contract_count != 0:
            raise ValueError("contract count requires a subscribed chain event")
        if self.subscribed_chain_event_count > 0 and self.subscribed_contract_count == 0:
            raise ValueError("subscribed chain event requires contracts")


@dataclass(frozen=True)
class RecoveryQueryPlan:
    target_source_date: date
    query_start_date: date
    query_end_date_exclusive: date
    equity_close: object
    maximum_query_attempts: int = 1
    delivery_path: DeliveryPath = DeliveryPath.EXACT_DATE_PROVIDER_HISTORY_RECOVERY
    status: RecoveryStatus = RecoveryStatus.QUERY_PLANNED


@dataclass(frozen=True)
class ProviderHistoryRecord:
    """Identifier-free aggregate view of one OptionUniverse history record."""

    source_date: date
    availability_date: date
    contract_count: int
    chain_axis_signals: Mapping[Axis, AxisSignal]
    cross_fields_without_underlying_valid: bool = False
    contract_underlying_zero_observed: bool = False

    def __post_init__(self) -> None:
        if self.contract_count < 0:
            raise ValueError("contract_count must be non-negative")
        actual_axes = set(self.chain_axis_signals)
        if self.contract_count == 0 and actual_axes:
            raise ValueError("empty provider record cannot carry axis signals")
        if self.contract_count > 0 and actual_axes != set(_CHAIN_AXES):
            raise ValueError("non-empty provider record requires the exact chain axes")


@dataclass(frozen=True)
class RecoveredSessionDelivery:
    delivery_path: DeliveryPath
    status: RecoveryStatus
    provider_record_count: int
    provider_contract_count: int
    session_observation: SessionSliceObservation


def plan_exact_date_recovery(
    summary: SubscriptionSessionSummary,
) -> RecoveryQueryPlan | None:
    """Return one exact-date plan only for a finalized, valid missing session."""

    if not summary.session_finalized:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_BEFORE_SESSION_FINALIZATION", summary.session_id.isoformat()
        )
    if summary.subscribed_chain_event_count > 0:
        return None
    if not _positive_finite(summary.equity_close):
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_EQUITY_SESSION_INVALID", summary.session_id.isoformat()
        )
    return RecoveryQueryPlan(
        target_source_date=summary.session_id,
        query_start_date=summary.session_id,
        query_end_date_exclusive=summary.session_id + timedelta(days=1),
        equity_close=summary.equity_close,
    )


def admit_exact_date_provider_history(
    plan: RecoveryQueryPlan,
    records: tuple[ProviderHistoryRecord, ...],
) -> RecoveredSessionDelivery:
    """Admit one exact source-date record into the existing session schema."""

    if plan.maximum_query_attempts != 1:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_QUERY_LIMIT_INVALID", "attempts")
    if any(item.source_date != plan.target_source_date for item in records):
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_CROSS_DATE_FALLBACK", plan.target_source_date.isoformat()
        )
    if not records:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_EXACT_DATE_RECORD_MISSING", plan.target_source_date.isoformat()
        )
    if len(records) != 1:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_EXACT_DATE_RECORD_DUPLICATE", str(len(records))
        )
    record = records[0]
    if record.availability_date != record.source_date + timedelta(days=1):
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_AVAILABILITY_IDENTITY_INVALID", record.availability_date.isoformat()
        )
    if record.contract_count == 0:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_EXACT_DATE_RECORD_EMPTY", record.source_date.isoformat()
        )
    observation = SessionSliceObservation(
        session_id=plan.target_source_date,
        chain_contract_count=record.contract_count,
        equity_close=plan.equity_close,
        chain_axis_signals=record.chain_axis_signals,
        cross_fields_without_underlying_valid=record.cross_fields_without_underlying_valid,
        contract_underlying_zero_observed=record.contract_underlying_zero_observed,
    )
    return RecoveredSessionDelivery(
        delivery_path=DeliveryPath.EXACT_DATE_PROVIDER_HISTORY_RECOVERY,
        status=RecoveryStatus.ACCEPTED,
        provider_record_count=1,
        provider_contract_count=record.contract_count,
        session_observation=observation,
    )


@dataclass(frozen=True)
class LoadedExactDateSubscriptionRecoveryPolicy:
    payload: Mapping[str, Any]
    file_sha256: str


@dataclass(frozen=True)
class BuiltExactDateSubscriptionRecoveryPackage:
    policy: LoadedExactDateSubscriptionRecoveryPolicy
    recovery_contract: Mapping[str, Any]
    proposal: Mapping[str, Any]
    manifest: Mapping[str, Any]
    project_code_bytes: bytes


_POLICY_KEYS = {
    "schema_version",
    "policy_id",
    "policy_version",
    "policy_status",
    "task_id",
    "registration_base_repository_code_sha",
    "package_root",
    "requested_start",
    "requested_end",
    "exchange_calendar",
    "expected_session_count",
    "target_source_date",
    "predecessor_execution_manifest_path",
    "predecessor_execution_manifest_file_sha256",
    "predecessor_terminal_evidence_path",
    "predecessor_terminal_evidence_file_sha256",
    "predecessor_v2_package_manifest_path",
    "predecessor_v2_package_manifest_file_sha256",
    "predecessor_attribution",
    "predecessor_attribution_terminal",
    "predecessor_exact_date_record_count",
    "predecessor_exact_date_contract_count",
    "normal_delivery_path",
    "recovery_delivery_path",
    "normal_slice_precedence_required",
    "session_absence_finalization",
    "recovery_trigger",
    "provider_query",
    "maximum_provider_queries_per_missing_session",
    "source_date_field",
    "availability_rule",
    "exact_source_date_match_required",
    "cross_date_fallback_allowed",
    "duplicate_record_allowed",
    "empty_record_allowed",
    "same_axis_schema_required",
    "delivery_lineage_required",
    "expected_axis_ids",
    "maximum_orders",
    "maximum_fills",
    "external_action_authorized",
    "cloud_validation_status",
    "data_quality_status",
    "point_in_time_status",
    "engine_status",
    "production_effect",
    "broker_action",
}


def load_exact_date_subscription_recovery_policy(
    *,
    policy_path: Path = DEFAULT_POLICY_PATH,
    project_root: Path = PROJECT_ROOT,
) -> LoadedExactDateSubscriptionRecoveryPolicy:
    root = project_root.resolve()
    path = policy_path if policy_path.is_absolute() else root / policy_path
    path = path.resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_PATH_ESCAPE", str(path)) from exc
    if not path.is_file() or path.is_symlink():
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_MISSING", str(path))
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict) or set(payload) != _POLICY_KEYS:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_INVALID", "key inventory")
    expected = {
        "schema_version": "qc_qqq_options_exact_date_subscription_recovery_policy.v1",
        "policy_id": "qc_qqq_options_exact_date_subscription_recovery_v1",
        "policy_version": "1.0.0",
        "policy_status": "OFFLINE_IMPLEMENTATION_CLOUD_VALIDATION_REQUIRED",
        "task_id": TASK_ID,
        "requested_start": "2021-02-22",
        "requested_end": "2025-12-02",
        "exchange_calendar": "XNYS",
        "expected_session_count": 1202,
        "target_source_date": "2022-08-26",
        "predecessor_attribution": "EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING",
        "predecessor_attribution_terminal": "RESOLVED",
        "predecessor_exact_date_record_count": 1,
        "predecessor_exact_date_contract_count": 6496,
        "normal_delivery_path": DeliveryPath.SUBSCRIBED_SLICE.value,
        "recovery_delivery_path": DeliveryPath.EXACT_DATE_PROVIDER_HISTORY_RECOVERY.value,
        "normal_slice_precedence_required": True,
        "session_absence_finalization": "END_OF_ALGORITHM_ONLY",
        "recovery_trigger": (
            "FULL_SESSION_SUBSCRIBED_CHAIN_EVENT_COUNT_ZERO_AND_VALID_EQUITY_SESSION"
        ),
        "provider_query": (
            "HISTORY_OPTION_UNIVERSE_TARGET_DATE_TO_NEXT_CALENDAR_DATE_END_EXCLUSIVE"
        ),
        "maximum_provider_queries_per_missing_session": 1,
        "source_date_field": "OPTION_UNIVERSE_TIME_DATE",
        "availability_rule": (
            "OPTION_UNIVERSE_END_TIME_DATE_EQUALS_TIME_DATE_PLUS_ONE_DAY"
        ),
        "exact_source_date_match_required": True,
        "cross_date_fallback_allowed": False,
        "duplicate_record_allowed": False,
        "empty_record_allowed": False,
        "same_axis_schema_required": True,
        "delivery_lineage_required": True,
        "maximum_orders": 0,
        "maximum_fills": 0,
        "external_action_authorized": False,
        "cloud_validation_status": "NOT_EXECUTED",
        "data_quality_status": "FAIL",
        "point_in_time_status": "NOT_EVALUATED",
        "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
        "production_effect": "none",
        "broker_action": "none",
    }
    for key, value in expected.items():
        if payload.get(key) != value:
            raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_INVALID", key)
    if tuple(payload["expected_axis_ids"]) != tuple(axis.value for axis in Axis):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_POLICY_INVALID", "expected_axis_ids")
    for key in (
        "predecessor_execution_manifest_path",
        "predecessor_terminal_evidence_path",
        "predecessor_v2_package_manifest_path",
        "package_root",
    ):
        _require_repo_path(payload[key], key)
    for key in (
        "predecessor_execution_manifest_file_sha256",
        "predecessor_terminal_evidence_file_sha256",
        "predecessor_v2_package_manifest_file_sha256",
    ):
        _require_sha256(payload[key], key)
    return LoadedExactDateSubscriptionRecoveryPolicy(
        payload=payload,
        file_sha256=_sha256(path.read_bytes()),
    )


def _bound_json(
    root: Path,
    relative_path: str,
    expected_sha256: str,
) -> dict[str, Any]:
    path = (root / relative_path).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_SOURCE_PATH_ESCAPE", relative_path
        ) from exc
    if not path.is_file() or path.is_symlink():
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SOURCE_MISSING", relative_path)
    raw = path.read_bytes()
    if _sha256(raw) != expected_sha256:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SOURCE_DRIFT", relative_path)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SOURCE_INVALID", relative_path) from exc
    if not isinstance(payload, dict):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SOURCE_INVALID", relative_path)
    return payload


def _render_project_code(
    *, session_ids: tuple[date, ...], target_source_date: date, identity: str
) -> bytes:
    sessions = repr(tuple(item.isoformat() for item in session_ids))
    template = '''from AlgorithmImports import *
from datetime import datetime, timedelta
import math

# TRADING-2541 offline candidate only. No external execution is authorized here.
# Zero orders; bounded aggregate runtime statistics; no logs or Object Store.
EXPECTED_SESSIONS = __SESSIONS__
TARGET_SOURCE_DATE = "__TARGET__"
AXES = __AXES__
STATUSES = ("PRESENT", "MISSING", "INVALID", "NOT_EVALUATED")
IDENTITY = __IDENTITY__
REQUESTED_RANGE = "2021-02-22..2025-12-02"
EVALUATED_RANGE = "2021-02-22..2025-12-02"


class QQQOptionsExactDateSubscriptionRecovery(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021, 2, 22)
        self.set_end_date(2025, 12, 2)
        self.set_cash(100000)
        self.settings.daily_precise_end_time = True
        self.universe_settings.asynchronous = False
        self.set_time_zone(TimeZones.NEW_YORK)
        self._equity = self.add_equity(
            "QQQ", Resolution.DAILY, data_normalization_mode=DataNormalizationMode.RAW
        ).symbol
        option = self.add_option("QQQ", Resolution.DAILY)
        option.set_filter(
            lambda universe: universe.contracts(
                lambda contracts: [contract.symbol for contract in contracts]
            )
        )
        self._option = option.symbol
        self._states = {session: self._new_state() for session in EXPECTED_SESSIONS}
        self._order_event_count = 0

    @staticmethod
    def _new_state():
        return {
            "slice_events": 0,
            "subscribed_chain_events": 0,
            "chain_events": 0,
            "equity_observed": False,
            "equity_valid": False,
            "equity_invalid": False,
            "contract_zero_observed": False,
            "cross_without_underlying_valid": False,
            "axis_observed": {axis: False for axis in AXES[2:7]},
            "axis_valid": {axis: False for axis in AXES[2:7]},
            "delivery_path": "UNRESOLVED",
        }

    @staticmethod
    def _finite(value):
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
        return result if math.isfinite(result) else None

    @staticmethod
    def _attribute(item, *names):
        for name in names:
            if hasattr(item, name):
                return getattr(item, name)
        return None

    @staticmethod
    def _merge_axis(state, axis, values, valid):
        observed = [value for value in values if value is not None]
        state["axis_observed"][axis] |= bool(observed)
        state["axis_valid"][axis] |= any(valid(value) for value in observed)

    def _merge_contracts(self, state, contracts, session):
        contract_underlyings = [
            self._finite(self._attribute(item, "underlying_last_price"))
            for item in contracts
        ]
        state["contract_zero_observed"] |= any(
            value == 0 for value in contract_underlyings
        )
        bids = [
            self._finite(self._attribute(item, "bid_price", "bidprice"))
            for item in contracts
        ]
        asks = [
            self._finite(self._attribute(item, "ask_price", "askprice"))
            for item in contracts
        ]
        ivs = [
            self._finite(self._attribute(item, "implied_volatility"))
            for item in contracts
        ]
        ois = [
            self._finite(self._attribute(item, "open_interest", "openinterest"))
            for item in contracts
        ]
        volumes = [self._finite(self._attribute(item, "volume")) for item in contracts]
        quote_pairs = [
            pair for pair in zip(bids, asks)
            if pair[0] is not None or pair[1] is not None
        ]
        self._merge_axis(
            state, "BID_ASK_QUOTE", quote_pairs,
            lambda pair: pair[0] is not None and pair[1] is not None
            and pair[0] >= 0 and pair[1] > 0 and pair[1] >= pair[0],
        )
        greek_vectors = []
        for item in contracts:
            greeks = self._attribute(item, "greeks")
            greek_vectors.append(None if greeks is None else tuple(
                self._finite(self._attribute(greeks, name))
                for name in ("delta", "gamma", "vega", "theta", "rho")
            ))
        self._merge_axis(
            state, "GREEKS", greek_vectors,
            lambda vector: all(value is not None for value in vector),
        )
        self._merge_axis(state, "IMPLIED_VOLATILITY", ivs, lambda value: value >= 0)
        self._merge_axis(state, "OPEN_INTEREST", ois, lambda value: value >= 0)
        self._merge_axis(state, "VOLUME", volumes, lambda value: value >= 0)
        for index, item in enumerate(contracts):
            strike = self._finite(self._attribute(item, "strike"))
            if strike is None and hasattr(item, "symbol"):
                strike = self._finite(item.symbol.id.strike_price)
            expiry = self._attribute(item, "expiry")
            if expiry is None and hasattr(item, "symbol"):
                expiry = item.symbol.id.date
            expiry_date = expiry.date() if hasattr(expiry, "date") else expiry
            state["cross_without_underlying_valid"] |= (
                strike is not None and strike > 0
                and hasattr(expiry_date, "year") and expiry_date >= session
                and bids[index] is not None and asks[index] is not None
                and bids[index] >= 0 and asks[index] > 0 and asks[index] >= bids[index]
            )

    def on_data(self, data: Slice):
        session_id = data.time.date().isoformat()
        state = self._states.get(session_id)
        if state is None:
            return
        state["slice_events"] += 1
        bar = data.bars.get(self._equity)
        if bar is not None:
            equity_close = self._finite(bar.close)
            if equity_close is not None:
                state["equity_observed"] = True
                if equity_close > 0:
                    state["equity_valid"] = True
                else:
                    state["equity_invalid"] = True
        chain = data.option_chains.get(self._option)
        if chain is None or len(chain) == 0:
            return
        state["subscribed_chain_events"] += 1
        state["chain_events"] += 1
        state["delivery_path"] = "SUBSCRIBED_SLICE"
        self._merge_contracts(state, list(chain), data.time.date())

    def _recover_target(self, target_date, state):
        result = {
            "status": "QUERY_ERROR",
            "provider_query_attempt_count": 1,
            "exact_date_record_count": 0,
            "exact_date_contract_count": 0,
            "non_target_record_count": 0,
            "invalid_availability_record_count": 0,
            "source_date": "NOT_AVAILABLE",
            "availability_date": "NOT_AVAILABLE",
        }
        start_time = datetime.strptime(target_date, "%Y-%m-%d")
        end_time = start_time + timedelta(days=1)
        try:
            history = self.history[OptionUniverse](self._option, start_time, end_time)
            records = list(history)
        except Exception:
            return result
        exact_records = []
        for record in records:
            source_date = record.time.date().isoformat()
            availability_date = record.end_time.date()
            expected_availability = record.time.date() + timedelta(days=1)
            if source_date != target_date:
                result["non_target_record_count"] += 1
            elif availability_date != expected_availability:
                result["invalid_availability_record_count"] += 1
            else:
                exact_records.append(record)
        result["exact_date_record_count"] = len(exact_records)
        if result["non_target_record_count"] > 0:
            result["status"] = "CROSS_DATE_FALLBACK_REJECTED"
            return result
        if result["invalid_availability_record_count"] > 0:
            result["status"] = "INVALID_AVAILABILITY_REJECTED"
            return result
        if len(exact_records) == 0:
            result["status"] = "EXACT_DATE_RECORD_MISSING"
            return result
        if len(exact_records) != 1:
            result["status"] = "EXACT_DATE_RECORD_DUPLICATE"
            return result
        record = exact_records[0]
        contracts = list(record)
        result["exact_date_contract_count"] = len(contracts)
        result["source_date"] = record.time.date().isoformat()
        result["availability_date"] = record.end_time.date().isoformat()
        if not contracts:
            result["status"] = "EXACT_DATE_RECORD_EMPTY"
            return result
        state["chain_events"] += 1
        state["delivery_path"] = "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
        self._merge_contracts(state, contracts, record.time.date())
        result["status"] = "ACCEPTED"
        return result

    def on_order_event(self, order_event):
        self._order_event_count += 1

    def on_end_of_algorithm(self):
        observed_sessions = sum(
            int(state["slice_events"] > 0) for state in self._states.values()
        )
        missing = [
            (session, state)
            for session, state in self._states.items()
            if state["chain_events"] == 0
        ]
        target_date = "NOT_AVAILABLE"
        recovery = {
            "status": "NOT_EVALUATED",
            "provider_query_attempt_count": 0,
            "exact_date_record_count": 0,
            "exact_date_contract_count": 0,
            "non_target_record_count": 0,
            "invalid_availability_record_count": 0,
            "source_date": "NOT_AVAILABLE",
            "availability_date": "NOT_AVAILABLE",
        }
        if observed_sessions == len(EXPECTED_SESSIONS) and len(missing) == 1:
            target_date, target_state = missing[0]
            if target_date != TARGET_SOURCE_DATE:
                recovery["status"] = "UNEXPECTED_MISSING_SESSION"
            elif not target_state["equity_valid"]:
                recovery["status"] = "EQUITY_SESSION_INVALID"
            else:
                recovery = self._recover_target(target_date, target_state)

        counts = {axis: {status: 0 for status in STATUSES} for axis in AXES}
        normal_slice_sessions = 0
        recovered_sessions = 0
        unresolved_sessions = 0
        for state in self._states.values():
            if state["chain_events"] == 0:
                unresolved_sessions += 1
                counts["OPTION_CHAIN_PRESENCE"]["MISSING"] += 1
                for axis in AXES[1:]:
                    counts[axis]["NOT_EVALUATED"] += 1
                continue
            counts["OPTION_CHAIN_PRESENCE"]["PRESENT"] += 1
            normal_slice_sessions += int(state["delivery_path"] == "SUBSCRIBED_SLICE")
            recovered_sessions += int(
                state["delivery_path"] == "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
            )
            if state["equity_valid"]:
                underlying_status = "PRESENT"
            elif state["equity_observed"] or state["equity_invalid"]:
                underlying_status = "INVALID"
            else:
                underlying_status = "MISSING"
            counts["UNDERLYING_PRICE"][underlying_status] += 1
            for axis in AXES[2:7]:
                status = "MISSING" if not state["axis_observed"][axis] else (
                    "PRESENT" if state["axis_valid"][axis] else "INVALID"
                )
                counts[axis][status] += 1
            cross_status = (
                "PRESENT"
                if underlying_status == "PRESENT"
                and state["cross_without_underlying_valid"]
                else "INVALID"
            )
            counts["CROSS_FIELD_CONSISTENCY"][cross_status] += 1

        partitions_valid = all(
            sum(counts[axis].values()) == len(EXPECTED_SESSIONS) for axis in AXES
        )
        valid = (
            partitions_valid
            and observed_sessions == len(EXPECTED_SESSIONS)
            and len(missing) == 1
            and target_date == TARGET_SOURCE_DATE
            and recovery["status"] == "ACCEPTED"
            and recovery["provider_query_attempt_count"] == 1
            and normal_slice_sessions == len(EXPECTED_SESSIONS) - 1
            and recovered_sessions == 1
            and unresolved_sessions == 0
            and self._order_event_count == 0
            and not self.portfolio.invested
        )
        for axis in AXES:
            for status in STATUSES:
                self.set_runtime_statistic(
                    "TRADING2541_" + axis + "_" + status + "_SESSIONS",
                    str(counts[axis][status]),
                )
        statistics = {
            "TARGET_SOURCE_DATE": target_date,
            "RECOVERY_STATUS": recovery["status"],
            "DELIVERY_PATH": "EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
                if recovered_sessions == 1 else "UNRESOLVED",
            "PROVIDER_QUERY_ATTEMPT_COUNT": recovery["provider_query_attempt_count"],
            "EXACT_DATE_RECORD_COUNT": recovery["exact_date_record_count"],
            "EXACT_DATE_CONTRACT_COUNT": recovery["exact_date_contract_count"],
            "NON_TARGET_RECORD_COUNT": recovery["non_target_record_count"],
            "INVALID_AVAILABILITY_RECORD_COUNT": recovery[
                "invalid_availability_record_count"
            ],
            "RECOVERY_SOURCE_DATE": recovery["source_date"],
            "RECOVERY_AVAILABILITY_DATE": recovery["availability_date"],
            "NORMAL_SLICE_SESSION_COUNT": normal_slice_sessions,
            "RECOVERED_SESSION_COUNT": recovered_sessions,
            "UNRESOLVED_SESSION_COUNT": unresolved_sessions,
        }
        for key, value in statistics.items():
            self.set_runtime_statistic("TRADING2541_" + key, str(value))
        self.set_runtime_statistic("TRADING2541_IDENTITY", IDENTITY)
        self.set_runtime_statistic(
            "TRADING2541_EXECUTION_TERMINAL",
            "status=" + ("COMPLETE" if valid else "INVALID")
            + "|expected_sessions=" + str(len(EXPECTED_SESSIONS))
            + "|observed_sessions=" + str(observed_sessions)
            + "|requested_range=" + REQUESTED_RANGE
            + "|evaluated_range=" + EVALUATED_RANGE
            + "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
            + "|contract_identifiers_exported=false|individual_fields_exported=false"
            + "|logs_as_data=false|object_store=false|dq_pit_promoted=false",
        )
'''
    rendered = (
        template.replace("__SESSIONS__", sessions)
        .replace("__TARGET__", target_source_date.isoformat())
        .replace("__AXES__", repr(tuple(axis.value for axis in Axis)))
        .replace("__IDENTITY__", repr(identity))
    )
    return rendered.replace("\r\n", "\n").encode("utf-8")


def build_exact_date_subscription_recovery_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltExactDateSubscriptionRecoveryPackage:
    root = project_root.resolve()
    loaded_policy = load_exact_date_subscription_recovery_policy(project_root=root)
    policy = loaded_policy.payload
    execution_manifest = _bound_json(
        root,
        str(policy["predecessor_execution_manifest_path"]),
        str(policy["predecessor_execution_manifest_file_sha256"]),
    )
    terminal = _bound_json(
        root,
        str(policy["predecessor_terminal_evidence_path"]),
        str(policy["predecessor_terminal_evidence_file_sha256"]),
    )
    _bound_json(
        root,
        str(policy["predecessor_v2_package_manifest_path"]),
        str(policy["predecessor_v2_package_manifest_file_sha256"]),
    )
    if (
        execution_manifest.get("status") != "EXECUTION_EVIDENCE_COMPLETE"
        or execution_manifest.get("technical_validation_state") != "PASS"
        or terminal.get("technical_validation_state") != "PASS"
        or terminal.get("target_session_date") != policy["target_source_date"]
        or terminal.get("exact_date_record_count")
        != policy["predecessor_exact_date_record_count"]
        or terminal.get("exact_date_contract_count")
        != policy["predecessor_exact_date_contract_count"]
        or terminal.get("attribution") != policy["predecessor_attribution"]
        or terminal.get("attribution_terminal") != policy["predecessor_attribution_terminal"]
        or terminal.get("target_subscribed_chain_event_count") != 0
        or terminal.get("cross_date_fallback_detected") is not False
        or terminal.get("orders") != 0
        or terminal.get("fills") != 0
    ):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_PREDECESSOR_INVALID", "V2")
    source_v2 = attribution_correction.load_attribution_proposal_package(
        project_root=root,
        policy_path=(
            root
            / attribution_correction.SOURCE_TIME_POLICY_PATH.relative_to(
                attribution_correction.PROJECT_ROOT
            )
        ),
    )
    session_ids = source_v2.run_scope.session_ids
    if (
        len(session_ids) != policy["expected_session_count"]
        or session_ids[0].isoformat() != policy["requested_start"]
        or session_ids[-1].isoformat() != policy["requested_end"]
    ):
        raise ExactDateSubscriptionRecoveryError("RECOVERY_SESSION_SCOPE_INVALID", "V2")
    session_inventory_sha256 = _sha256(
        ("\n".join(item.isoformat() for item in session_ids) + "\n").encode("ascii")
    )
    recovery_contract = _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_subscription_recovery_contract.v1",
            "contract_id": "TRADING_2541_EXACT_DATE_SUBSCRIPTION_RECOVERY_V1",
            "task_id": TASK_ID,
            "requested_range": "2021-02-22..2025-12-02",
            "exchange_calendar": "XNYS",
            "expected_session_count": len(session_ids),
            "session_inventory_lf_sha256": session_inventory_sha256,
            "target_source_date": str(policy["target_source_date"]),
            "normal_delivery_path": DeliveryPath.SUBSCRIBED_SLICE.value,
            "recovery_delivery_path": (
                DeliveryPath.EXACT_DATE_PROVIDER_HISTORY_RECOVERY.value
            ),
            "normal_slice_precedence": "ABSOLUTE_NO_PROVIDER_QUERY_WHEN_CHAIN_PRESENT",
            "session_absence_finalization": "END_OF_ALGORITHM_ONLY",
            "recovery_query_limit": 1,
            "source_date_identity": "OPTION_UNIVERSE_TIME_DATE_EQUALS_TARGET",
            "availability_identity": "OPTION_UNIVERSE_END_TIME_EQUALS_TIME_PLUS_ONE_DAY",
            "fail_closed_rejections": [
                "RECOVERY_BEFORE_SESSION_FINALIZATION",
                "RECOVERY_EQUITY_SESSION_INVALID",
                "RECOVERY_CROSS_DATE_FALLBACK",
                "RECOVERY_EXACT_DATE_RECORD_MISSING",
                "RECOVERY_EXACT_DATE_RECORD_DUPLICATE",
                "RECOVERY_AVAILABILITY_IDENTITY_INVALID",
                "RECOVERY_EXACT_DATE_RECORD_EMPTY",
            ],
            "axis_schema": [axis.value for axis in Axis],
            "recovered_record_adapter": "SESSION_SLICE_OBSERVATION_V2_SAME_AXIS_REDUCER",
            "predecessor_execution_manifest_file_sha256": str(
                policy["predecessor_execution_manifest_file_sha256"]
            ),
            "predecessor_terminal_evidence_file_sha256": str(
                policy["predecessor_terminal_evidence_file_sha256"]
            ),
            "predecessor_observed_exact_date_contract_count": int(
                policy["predecessor_exact_date_contract_count"]
            ),
            "observed_count_is_acceptance_threshold": False,
            "cloud_validation_status": "NOT_EXECUTED",
            "dq_status": "FAIL",
            "pit_status": "NOT_EVALUATED",
            "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
            "orders": 0,
            "fills": 0,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    identity = (
        "schema=qc_qqq_options_daily_transport_per_axis_runtime.v3"
        f"|recovery_contract={recovery_contract['content_sha256']}"
        "|normal_path=SUBSCRIBED_SLICE"
        "|recovery_path=EXACT_DATE_PROVIDER_HISTORY_RECOVERY"
        "|source_date=OptionUniverse.Time"
        "|availability=OptionUniverse.EndTime=Time+1day"
    )
    project_code = _render_project_code(
        session_ids=session_ids,
        target_source_date=date.fromisoformat(str(policy["target_source_date"])),
        identity=identity,
    )
    proposal = _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_subscription_recovery_proposal.v1",
            "task_id": TASK_ID,
            "recovery_contract_content_sha256": recovery_contract["content_sha256"],
            "project_code_lf_byte_count": len(project_code),
            "project_code_lf_sha256": _sha256(project_code),
            "implementation_status": "OFFLINE_IMPLEMENTED",
            "cloud_validation_status": "NOT_EXECUTED",
            "maximum_future_provider_queries": 1,
            "maximum_future_cloud_backtests": 1,
            "maximum_orders": 0,
            "maximum_fills": 0,
            "external_action_authorized": False,
            "data_quality_status": "FAIL",
            "point_in_time_status": "NOT_EVALUATED",
            "engine_status": "POLICY_BLOCKED_CASH_PRESERVATION",
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    artifacts = []
    for kind, name, raw in (
        ("PROJECT_CODE", "main.py", project_code),
        ("PROPOSAL", "proposal.json", _canonical_json_bytes(proposal)),
        ("RECOVERY_CONTRACT", "recovery_contract.json", _canonical_json_bytes(recovery_contract)),
    ):
        artifacts.append(
            {
                "kind": kind,
                "relative_path": name,
                "byte_count": len(raw),
                "sha256": _sha256(raw),
            }
        )
    manifest = _seal(
        {
            "schema_version": "qc_qqq_options_exact_date_subscription_recovery_package.v1",
            "task_id": TASK_ID,
            "policy_file_sha256": loaded_policy.file_sha256,
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
            "external_action_authorized": False,
            "cloud_validation_status": "NOT_EXECUTED",
            "orders": 0,
            "fills": 0,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return BuiltExactDateSubscriptionRecoveryPackage(
        policy=loaded_policy,
        recovery_contract=recovery_contract,
        proposal=proposal,
        manifest=manifest,
        project_code_bytes=project_code,
    )


def write_exact_date_subscription_recovery_package(
    *,
    output_root: Path = DEFAULT_PACKAGE_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> BuiltExactDateSubscriptionRecoveryPackage:
    root = project_root.resolve()
    if output_root.is_absolute():
        target = output_root.resolve()
    else:
        if ".." in output_root.parts:
            raise ExactDateSubscriptionRecoveryError(
                "RECOVERY_PACKAGE_PATH_ESCAPE", str(output_root)
            )
        target = (root / output_root).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise ExactDateSubscriptionRecoveryError(
                "RECOVERY_PACKAGE_PATH_ESCAPE", str(target)
            ) from exc
    built = build_exact_date_subscription_recovery_package(project_root=root)
    target.mkdir(parents=True, exist_ok=True)
    payloads = {
        "main.py": built.project_code_bytes,
        "package_manifest.json": _canonical_json_bytes(built.manifest),
        "proposal.json": _canonical_json_bytes(built.proposal),
        "recovery_contract.json": _canonical_json_bytes(built.recovery_contract),
    }
    for name, raw in payloads.items():
        write_bytes_atomic(target / name, raw)
    return built


def validate_exact_date_subscription_recovery_package(
    *,
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    project_root: Path = PROJECT_ROOT,
) -> BuiltExactDateSubscriptionRecoveryPackage:
    root = project_root.resolve()
    if package_root.is_absolute():
        target = package_root.resolve()
    else:
        if ".." in package_root.parts:
            raise ExactDateSubscriptionRecoveryError(
                "RECOVERY_PACKAGE_PATH_ESCAPE", str(package_root)
            )
        target = (root / package_root).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise ExactDateSubscriptionRecoveryError(
                "RECOVERY_PACKAGE_PATH_ESCAPE", str(target)
            ) from exc
    if not target.is_dir() or target.is_symlink():
        raise ExactDateSubscriptionRecoveryError("RECOVERY_PACKAGE_MISSING", str(target))
    inventory = tuple(sorted(item.name for item in target.iterdir()))
    if inventory != _PACKAGE_FILES:
        raise ExactDateSubscriptionRecoveryError(
            "RECOVERY_PACKAGE_INVENTORY_INVALID", repr(inventory)
        )
    expected = build_exact_date_subscription_recovery_package(project_root=root)
    expected_payloads = {
        "main.py": expected.project_code_bytes,
        "package_manifest.json": _canonical_json_bytes(expected.manifest),
        "proposal.json": _canonical_json_bytes(expected.proposal),
        "recovery_contract.json": _canonical_json_bytes(expected.recovery_contract),
    }
    for name, expected_raw in expected_payloads.items():
        observed = (target / name).read_bytes()
        if observed != expected_raw:
            raise ExactDateSubscriptionRecoveryError("RECOVERY_PACKAGE_REPLAY_MISMATCH", name)
    manifest = json.loads((target / "package_manifest.json").read_bytes())
    _verify_seal(manifest)
    for artifact in manifest["artifacts"]:
        raw = (target / artifact["relative_path"]).read_bytes()
        if len(raw) != artifact["byte_count"] or _sha256(raw) != artifact["sha256"]:
            raise ExactDateSubscriptionRecoveryError(
                "RECOVERY_PACKAGE_ARTIFACT_DRIFT", artifact["relative_path"]
            )
    return expected


__all__ = [
    "BuiltExactDateSubscriptionRecoveryPackage",
    "DeliveryPath",
    "ExactDateSubscriptionRecoveryError",
    "ProviderHistoryRecord",
    "RecoveredSessionDelivery",
    "RecoveryQueryPlan",
    "RecoveryStatus",
    "SubscriptionSessionSummary",
    "admit_exact_date_provider_history",
    "build_exact_date_subscription_recovery_package",
    "load_exact_date_subscription_recovery_policy",
    "plan_exact_date_recovery",
    "validate_exact_date_subscription_recovery_package",
    "write_exact_date_subscription_recovery_package",
]
