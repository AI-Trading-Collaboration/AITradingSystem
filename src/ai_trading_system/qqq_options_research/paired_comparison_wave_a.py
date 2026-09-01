from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_FLOOR, Decimal
from enum import StrEnum
from pathlib import Path

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.qqq_options_research.paired_comparison_contract_freeze_admission import (
    DEFAULT_PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_PATH,
    PairedComparisonContractFreezeAdmissionLoadResult,
    load_paired_comparison_contract_freeze_admission,
)

EXPECTED_SIGNAL_SESSION_COUNT = 1202
EXPECTED_FROZEN_SLOT_COUNT = 37
COMPARATOR_ID = "UNDERLYING_IMPLEMENTATION"
COMPARATOR_METHOD = "SAME_SIGNAL_FULLY_FUNDED_QQQ_CASH_ACCOUNT"
COMPARATOR_VERSION = "1.0.0-draft.1"
INITIAL_CAPITAL_USD = Decimal("100000.00")
# Protocol safety bound only; this does not change investment interpretation.
MAX_AGGREGATE_COUNT = 2**63 - 1

_RAW_OPTION_KEY_TOKENS = (
    "ASK_HISTORY",
    "BID_HISTORY",
    "CHAIN_ROW",
    "COMPLETE_CHAIN",
    "CONTRACT_ROW",
    "CONTRACT_SID",
    "CONTRACT_SYMBOL",
    "OPTION_ROW",
    "OPTION_SID",
    "OPTION_SYMBOL",
    "QUOTE_HISTORY",
    "RAW_OPTION",
    "SECURITY_IDENTIFIER",
)
_SHA256_FIELDS = (
    "QC_CODE_FILE_SHA256",
    "POLICY_FILE_SHA256",
    "POLICY_CANONICAL_SHA256",
    "FREEZE_ADMISSION_FILE_SHA256",
    "COMPARATOR_CONTRACT_FILE_SHA256",
    "COMPARATOR_CONTRACT_CANONICAL_SHA256",
    "SIGNAL_PACKAGE_RECEIPT_SHA256",
    "SIGNAL_INDEX_SHA256",
    "NORMALIZED_SIGNAL_SOURCE_SHA256",
    "RUN_MANIFEST_SHA256",
    "COMPARATOR_CONTRACT_SHA256",
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")


class PairedComparisonWaveAError(ValueError):
    def __init__(self, reason_code: str, detail: str) -> None:
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail


class ComparatorAction(StrEnum):
    LONG_CALL = "LONG_CALL"
    FLAT = "FLAT"


class AxisStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    INSUFFICIENT = "INSUFFICIENT"
    INVALID = "INVALID"


class AuthorizationEvidence(StrEnum):
    GRANTED = "GRANTED"
    DENIED = "DENIED"
    MISSING = "MISSING"
    UNAUTHORIZED_ACTION = "UNAUTHORIZED_ACTION"


@dataclass(frozen=True)
class QQQQuote:
    bid: Decimal
    ask: Decimal

    def __post_init__(self) -> None:
        if not self.bid.is_finite() or not self.ask.is_finite():
            raise PairedComparisonWaveAError("INVALID_QUOTE", "quote must be finite")
        if self.bid <= 0 or self.ask <= 0 or self.bid > self.ask:
            raise PairedComparisonWaveAError("INVALID_QUOTE", "require 0 < bid <= ask")


@dataclass(frozen=True)
class ComparatorEvent:
    event_id: str
    occurred_at: datetime
    action: ComparatorAction
    quote: QQQQuote
    fee_usd: Decimal
    option_contract_eligible: bool

    def __post_init__(self) -> None:
        if not self.event_id.strip():
            raise PairedComparisonWaveAError("INVALID_EVENT", "event_id is required")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise PairedComparisonWaveAError("INVALID_EVENT", "occurred_at must be timezone-aware")
        if not self.fee_usd.is_finite() or self.fee_usd < 0:
            raise PairedComparisonWaveAError(
                "INVALID_EVENT", "fee_usd must be explicit, finite, and nonnegative"
            )


@dataclass(frozen=True)
class ComparatorObservation:
    occurred_at: datetime
    quote: QQQQuote

    def __post_init__(self) -> None:
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise PairedComparisonWaveAError(
                "INVALID_OBSERVATION", "occurred_at must be timezone-aware"
            )


@dataclass(frozen=True)
class ComparatorLedgerAggregate:
    start_equity_usd: Decimal
    end_equity_usd: Decimal
    net_pnl_usd: Decimal
    net_return: Decimal
    fees_usd: Decimal
    spread_slippage_cost_usd: Decimal
    min_cash_usd: Decimal
    ending_cash_usd: Decimal
    peak_equity_usd: Decimal
    max_drawdown: Decimal
    time_in_market_minutes: Decimal
    long_episode_count: int
    flat_episode_count: int
    effective_event_alignment_count: int
    effective_event_mismatch_count: int
    entry_quote_available_count: int
    entry_quote_missing_count: int
    exit_quote_available_count: int
    exit_quote_missing_count: int
    deployed_capital_holding_time: Decimal
    ending_share_count: int
    event_ids: tuple[str, ...]


class FullyFundedQQQCashLedger:
    """Pure virtual comparator ledger; it has no broker or QuantConnect side effects."""

    def __init__(self, *, initial_cash_usd: Decimal = INITIAL_CAPITAL_USD) -> None:
        if initial_cash_usd != INITIAL_CAPITAL_USD:
            raise PairedComparisonWaveAError(
                "CAPITAL_NORMALIZATION_DRIFT", "initial cash must equal USD 100,000.00"
            )
        self._cash = initial_cash_usd
        self._shares = 0
        self._fees = Decimal("0")
        self._spread_cost = Decimal("0")
        self._min_cash = initial_cash_usd
        self._peak_equity = initial_cash_usd
        self._max_drawdown = Decimal("0")
        self._last_at: datetime | None = None
        self._last_quote: QQQQuote | None = None
        self._time_in_market_minutes = Decimal("0")
        self._deployed_capital_holding_time = Decimal("0")
        self._entry_deployed_capital = Decimal("0")
        self._long_episode_count = 0
        self._flat_episode_count = 0
        self._event_ids: list[str] = []
        self._entry_quote_available_count = 0
        self._exit_quote_available_count = 0

    @property
    def cash(self) -> Decimal:
        return self._cash

    @property
    def shares(self) -> int:
        return self._shares

    def _advance_clock(self, occurred_at: datetime) -> None:
        if occurred_at.tzinfo is None or occurred_at.utcoffset() is None:
            raise PairedComparisonWaveAError(
                "INVALID_EVENT_CLOCK", "event clock must be timezone-aware"
            )
        normalized = occurred_at.astimezone(UTC)
        if self._last_at is not None and normalized < self._last_at:
            raise PairedComparisonWaveAError(
                "EVENT_CLOCK_DRIFT", "events and observations must be chronological"
            )
        if self._last_at is not None and self._shares > 0:
            elapsed_minutes = Decimal(str((normalized - self._last_at).total_seconds())) / Decimal(
                "60"
            )
            self._time_in_market_minutes += elapsed_minutes
            self._deployed_capital_holding_time += self._entry_deployed_capital * elapsed_minutes
        self._last_at = normalized

    def _observe_equity(self, *, quote: QQQQuote) -> None:
        equity = self._cash + Decimal(self._shares) * quote.bid
        if equity > self._peak_equity:
            self._peak_equity = equity
        if self._peak_equity > 0:
            drawdown = (self._peak_equity - equity) / self._peak_equity
            if drawdown > self._max_drawdown:
                self._max_drawdown = drawdown
        self._last_quote = quote

    def observe(self, observation: ComparatorObservation) -> None:
        self._advance_clock(observation.occurred_at)
        self._observe_equity(quote=observation.quote)

    def apply_event(self, event: ComparatorEvent) -> None:
        if event.event_id in self._event_ids:
            raise PairedComparisonWaveAError(
                "DUPLICATE_EVENT", f"duplicate event_id {event.event_id!r}"
            )
        self._advance_clock(event.occurred_at)
        self._observe_equity(quote=event.quote)
        if event.action is ComparatorAction.LONG_CALL:
            if self._shares != 0:
                raise PairedComparisonWaveAError(
                    "INVALID_LEDGER_TRANSITION", "LONG_CALL requires a flat comparator ledger"
                )
            available = self._cash - event.fee_usd
            shares = int((available / event.quote.ask).to_integral_value(rounding=ROUND_FLOOR))
            if shares <= 0:
                raise PairedComparisonWaveAError(
                    "INSUFFICIENT_COMPARATOR_CAPITAL", "cash cannot fund one QQQ share and fee"
                )
            entry_notional = Decimal(shares) * event.quote.ask
            self._cash -= entry_notional + event.fee_usd
            if self._cash < 0:
                raise PairedComparisonWaveAError(
                    "NEGATIVE_CASH", "fully funded comparator cannot use negative cash"
                )
            self._shares = shares
            self._entry_deployed_capital = entry_notional
            self._fees += event.fee_usd
            self._spread_cost += (
                Decimal(shares) * (event.quote.ask - event.quote.bid) / Decimal("2")
            )
            self._long_episode_count += 1
            self._entry_quote_available_count += 1
        elif event.action is ComparatorAction.FLAT:
            if self._shares <= 0:
                raise PairedComparisonWaveAError(
                    "INVALID_LEDGER_TRANSITION", "FLAT requires a long comparator ledger"
                )
            shares = self._shares
            exit_notional = Decimal(shares) * event.quote.bid
            self._cash += exit_notional - event.fee_usd
            if self._cash < 0:
                raise PairedComparisonWaveAError(
                    "NEGATIVE_CASH", "exit fee cannot create negative cash"
                )
            self._fees += event.fee_usd
            self._spread_cost += (
                Decimal(shares) * (event.quote.ask - event.quote.bid) / Decimal("2")
            )
            self._shares = 0
            self._entry_deployed_capital = Decimal("0")
            self._flat_episode_count += 1
            self._exit_quote_available_count += 1
        else:  # pragma: no cover - StrEnum construction rejects this earlier.
            raise PairedComparisonWaveAError("INVALID_ACTION", str(event.action))
        self._event_ids.append(event.event_id)
        self._min_cash = min(self._min_cash, self._cash)
        self._observe_equity(quote=event.quote)

    def aggregate(
        self, *, final_observation: ComparatorObservation | None = None
    ) -> ComparatorLedgerAggregate:
        if final_observation is not None:
            self.observe(final_observation)
        if self._last_quote is None:
            end_equity = self._cash
        else:
            end_equity = self._cash + Decimal(self._shares) * self._last_quote.bid
        net_pnl = end_equity - INITIAL_CAPITAL_USD
        return ComparatorLedgerAggregate(
            start_equity_usd=INITIAL_CAPITAL_USD,
            end_equity_usd=end_equity,
            net_pnl_usd=net_pnl,
            net_return=net_pnl / INITIAL_CAPITAL_USD,
            fees_usd=self._fees,
            spread_slippage_cost_usd=self._spread_cost,
            min_cash_usd=self._min_cash,
            ending_cash_usd=self._cash,
            peak_equity_usd=self._peak_equity,
            max_drawdown=self._max_drawdown,
            time_in_market_minutes=self._time_in_market_minutes,
            long_episode_count=self._long_episode_count,
            flat_episode_count=self._flat_episode_count,
            effective_event_alignment_count=len(self._event_ids),
            effective_event_mismatch_count=0,
            entry_quote_available_count=self._entry_quote_available_count,
            entry_quote_missing_count=0,
            exit_quote_available_count=self._exit_quote_available_count,
            exit_quote_missing_count=0,
            deployed_capital_holding_time=self._deployed_capital_holding_time,
            ending_share_count=self._shares,
            event_ids=tuple(self._event_ids),
        )


def load_wave_a_authority(
    *,
    path: Path = DEFAULT_PAIRED_COMPARISON_CONTRACT_FREEZE_ADMISSION_PATH,
    project_root: Path = PROJECT_ROOT,
) -> PairedComparisonContractFreezeAdmissionLoadResult:
    return load_paired_comparison_contract_freeze_admission(path=path, project_root=project_root)


def export_safe_field_inventory(
    authority: PairedComparisonContractFreezeAdmissionLoadResult,
) -> tuple[str, ...]:
    fields = authority.contract.contract.export_safe_fields
    return (
        *fields.identity,
        *fields.dq_signal,
        *fields.events,
        *fields.accounts,
        *fields.risk,
        *fields.comparator,
    )


def _canonical_value(value: object, *, path: str) -> object:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise PairedComparisonWaveAError("NONFINITE_AGGREGATE", path)
        return format(value, "f")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PairedComparisonWaveAError("NONFINITE_AGGREGATE", path)
        return value
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        output: dict[str, object] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise PairedComparisonWaveAError(
                    "NON_JSON_AGGREGATE", f"{path}: mapping keys must be strings"
                )
            normalized_key = key.upper()
            if any(token in normalized_key for token in _RAW_OPTION_KEY_TOKENS):
                raise PairedComparisonWaveAError("RAW_OPTION_EXPORT_REJECTED", f"{path}.{key}")
            output[key] = _canonical_value(child, path=f"{path}.{key}")
        return output
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [
            _canonical_value(child, path=f"{path}[{index}]") for index, child in enumerate(value)
        ]
    raise PairedComparisonWaveAError(
        "NON_JSON_AGGREGATE", f"{path}: unsupported type {type(value).__name__}"
    )


def canonical_aggregate_json(payload: Mapping[str, object]) -> bytes:
    canonical = _canonical_value(payload, path="aggregate")
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


@dataclass(frozen=True)
class AggregateAdmissionReceipt:
    payload_sha256: str
    field_count: int
    field_inventory: tuple[str, ...]
    comparator_contract_file_sha256: str
    comparator_contract_canonical_sha256: str
    aggregate_only: bool = True
    raw_option_rows: int = 0
    raw_option_exports: int = 0


def admit_export_safe_aggregate(
    payload: Mapping[str, object],
    *,
    authority: PairedComparisonContractFreezeAdmissionLoadResult | None = None,
) -> AggregateAdmissionReceipt:
    loaded = authority or load_wave_a_authority()
    inventory = export_safe_field_inventory(loaded)
    expected = set(inventory)
    observed = set(payload)
    missing = tuple(sorted(expected - observed))
    extra = tuple(sorted(observed - expected))
    if missing or extra or len(payload) != len(inventory):
        raise PairedComparisonWaveAError(
            "EXPORT_FIELD_INVENTORY_MISMATCH",
            f"missing={missing!r}; extra={extra!r}; expected_count={len(inventory)}",
        )
    for field in _SHA256_FIELDS:
        value = payload.get(field)
        if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
            raise PairedComparisonWaveAError(
                "AGGREGATE_IDENTITY_INVALID", f"{field} must be a lowercase SHA-256"
            )
    repository_commit = payload.get("REPOSITORY_EXACT_COMMIT")
    if not isinstance(repository_commit, str) or _GIT_COMMIT.fullmatch(repository_commit) is None:
        raise PairedComparisonWaveAError(
            "AGGREGATE_IDENTITY_INVALID",
            "REPOSITORY_EXACT_COMMIT must be a lowercase 40-character Git commit",
        )
    count_fields = tuple(
        field
        for field in inventory
        if field.endswith("_COUNT") or field in {"SESSION_COUNT", "TIME_IN_MARKET_SESSIONS"}
    )
    for field in count_fields:
        value = payload.get(field)
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > MAX_AGGREGATE_COUNT
        ):
            raise PairedComparisonWaveAError(
                "AGGREGATE_COUNT_INVALID",
                f"{field} must be an integer in [0, {MAX_AGGREGATE_COUNT}]",
            )
    canonical = canonical_aggregate_json(payload)
    contract = loaded.contract
    exact_bindings = {
        "FREEZE_ADMISSION_FILE_SHA256": loaded.file_sha256,
        "COMPARATOR_CONTRACT_FILE_SHA256": contract.file_sha256,
        "COMPARATOR_CONTRACT_CANONICAL_SHA256": contract.canonical_sha256,
        "COMPARATOR_CONTRACT_SHA256": contract.canonical_sha256,
    }
    for field, expected_value in exact_bindings.items():
        if payload[field] != expected_value:
            raise PairedComparisonWaveAError(
                "AGGREGATE_AUTHORITY_DRIFT",
                f"{field} does not match the exact frozen Wave A authority",
            )
    return AggregateAdmissionReceipt(
        payload_sha256=hashlib.sha256(canonical).hexdigest(),
        field_count=len(inventory),
        field_inventory=inventory,
        comparator_contract_file_sha256=contract.file_sha256,
        comparator_contract_canonical_sha256=contract.canonical_sha256,
    )


@dataclass(frozen=True)
class CalendarPartitionEvidence:
    partition_id: str
    start: date
    end: date
    included_exactly_once: bool


@dataclass(frozen=True)
class ReplayContext:
    signal_hashes_exact: bool | None
    signal_semantics_match: bool | None
    option_alpha_input_detected: bool | None
    window_and_calendar_unchanged: bool | None
    dq_receipt_and_manifest_identity_exact: bool | None
    frozen_slot_count: int | None
    frozen_slots_exact: bool | None
    engine_default_substituted: bool | None
    signal_input_lineage_complete: bool | None
    comparator_platform_evidence_complete: bool | None
    comparator_changed_after_freeze: bool | None
    comparator_order_submission_count: int | None
    accounting_reconciled: bool | None
    local_option_repricing_used: bool | None
    risk_fields_reconciled: bool | None
    local_greek_reconstruction_used: bool | None
    export_inventory_evidence_complete: bool | None
    raw_option_export_detected: bool | None
    platform_identity_complete: bool | None
    platform_identity_drift: bool | None
    platform_identity_supported: bool | None
    calendar_partitions: tuple[CalendarPartitionEvidence, ...] | None
    diagnostic_inventory_complete: bool | None
    post_result_diagnostic_added: bool | None
    result_used_for_selection: bool | None
    authorization: AuthorizationEvidence


@dataclass(frozen=True)
class AxisFinding:
    axis_id: str
    status: AxisStatus
    reason: str


@dataclass(frozen=True)
class PairedComparisonReplayReceipt:
    aggregate_payload_sha256: str
    axis_findings: tuple[AxisFinding, ...]
    terminal_status: AxisStatus
    terminal_precedence: tuple[str, ...]
    export_safe_field_count: int
    external_actions_executed: int = 0
    backtests_executed: int = 0
    orders: int = 0
    fills: int = 0
    positions: int = 0

    @property
    def canonical_json_bytes(self) -> bytes:
        payload = {
            "aggregate_payload_sha256": self.aggregate_payload_sha256,
            "axis_findings": [
                {
                    "axis_id": finding.axis_id,
                    "reason": finding.reason,
                    "status": finding.status.value,
                }
                for finding in self.axis_findings
            ],
            "backtests_executed": self.backtests_executed,
            "export_safe_field_count": self.export_safe_field_count,
            "external_actions_executed": self.external_actions_executed,
            "fills": self.fills,
            "orders": self.orders,
            "positions": self.positions,
            "terminal_precedence": list(self.terminal_precedence),
            "terminal_status": self.terminal_status.value,
        }
        return json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @property
    def canonical_sha256(self) -> str:
        return hashlib.sha256(self.canonical_json_bytes).hexdigest()


def _finding(axis_id: str, status: AxisStatus, reason: str) -> AxisFinding:
    return AxisFinding(axis_id=axis_id, status=status, reason=reason)


def _missing(value: object) -> bool:
    return value is None or value == ""


def _to_decimal(payload: Mapping[str, object], field: str) -> Decimal | None:
    value = payload.get(field)
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001 - invalid untrusted aggregate scalar.
        return None
    return parsed if parsed.is_finite() else None


def _to_int(payload: Mapping[str, object], field: str) -> int | None:
    value = payload.get(field)
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value) is not None:
        return int(value)
    return None


def _status_triplet(payload: Mapping[str, object], fields: tuple[str, ...]) -> AxisFinding | None:
    values = tuple(payload.get(field) for field in fields)
    if any(_missing(value) for value in values):
        return _finding(
            "DQ_PIT_MANIFEST", AxisStatus.INSUFFICIENT, "DQ/PIT/replay evidence missing"
        )
    if values == ("PASS", "PASS", "PASS"):
        return None
    return _finding("DQ_PIT_MANIFEST", AxisStatus.FAIL, "DQ, PIT, or manifest replay failed")


def _accounting_values(payload: Mapping[str, object], prefix: str) -> tuple[Decimal, ...] | None:
    fields = (
        f"{prefix}_START_EQUITY_USD",
        f"{prefix}_END_EQUITY_USD",
        f"{prefix}_NET_PNL_USD",
        f"{prefix}_NET_RETURN",
        f"{prefix}_FEES_USD",
        f"{prefix}_SPREAD_SLIPPAGE_COST_USD",
        f"{prefix}_MIN_CASH_USD",
        f"{prefix}_ENDING_CASH_USD",
        f"{prefix}_PEAK_EQUITY_USD",
        f"{prefix}_MAX_DRAWDOWN",
        f"{prefix}_TIME_IN_MARKET",
    )
    values = tuple(_to_decimal(payload, field) for field in fields)
    if any(value is None for value in values):
        return None
    return tuple(value for value in values if value is not None)


def _accounting_reconciles(payload: Mapping[str, object]) -> bool | None:
    optionized = _accounting_values(payload, "OPTIONIZED")
    underlying = _accounting_values(payload, "UNDERLYING")
    return_delta = _to_decimal(payload, "PRIMARY_RETURN_DELTA")
    drawdown_delta = _to_decimal(payload, "PRIMARY_DRAWDOWN_DELTA")
    if optionized is None or underlying is None or return_delta is None or drawdown_delta is None:
        return None
    for values in (optionized, underlying):
        start, end, pnl, net_return, fees, spread_cost, min_cash, ending_cash, *_ = values
        if start <= 0 or fees < 0 or spread_cost < 0 or min_cash < 0 or ending_cash < 0:
            return False
        if end - start != pnl or pnl / start != net_return:
            return False
    if return_delta != optionized[3] - underlying[3]:
        return False
    if drawdown_delta != optionized[9] - underlying[9]:
        return False
    return True


def _calendar_finding(
    context: ReplayContext,
    authority: PairedComparisonContractFreezeAdmissionLoadResult,
) -> AxisFinding:
    axis = "CALENDAR_SUBPERIOD_COMPLETENESS"
    if context.calendar_partitions is None:
        return _finding(axis, AxisStatus.INSUFFICIENT, "subperiod aggregate evidence missing")
    expected = tuple(
        (row.partition_id, row.start, row.end)
        for row in authority.contract.contract.calendar_diagnostics.partitions
    )
    observed = tuple((row.partition_id, row.start, row.end) for row in context.calendar_partitions)
    if observed != expected:
        return _finding(axis, AxisStatus.INVALID, "fixed partition identity drifted")
    if not all(row.included_exactly_once for row in context.calendar_partitions):
        return _finding(axis, AxisStatus.FAIL, "partition gap, overlap, or omitted zero-event year")
    return _finding(axis, AxisStatus.PASS, "five frozen partitions cover the window exactly once")


def evaluate_falsification_axes(
    payload: Mapping[str, object],
    *,
    context: ReplayContext,
    admission: AggregateAdmissionReceipt,
    authority: PairedComparisonContractFreezeAdmissionLoadResult | None = None,
) -> tuple[AxisFinding, ...]:
    loaded = authority or load_wave_a_authority()
    findings: list[AxisFinding] = []

    if context.option_alpha_input_detected is True or context.signal_hashes_exact is False:
        findings.append(
            _finding(
                "FROZEN_SIGNAL_IDENTITY",
                AxisStatus.INVALID,
                "signal hash drift or option-alpha input",
            )
        )
    elif context.signal_hashes_exact is None or context.signal_semantics_match is None:
        findings.append(
            _finding(
                "FROZEN_SIGNAL_IDENTITY",
                AxisStatus.INSUFFICIENT,
                "signal identity evidence missing",
            )
        )
    elif context.signal_semantics_match is False:
        findings.append(
            _finding("FROZEN_SIGNAL_IDENTITY", AxisStatus.FAIL, "validated signal semantics differ")
        )
    else:
        findings.append(
            _finding(
                "FROZEN_SIGNAL_IDENTITY",
                AxisStatus.PASS,
                "signal identities and semantics are exact",
            )
        )

    expected_sessions = _to_int(payload, "EXPECTED_SIGNAL_SESSION_COUNT")
    observed_sessions = _to_int(payload, "OBSERVED_SIGNAL_SESSION_COUNT")
    missing_sessions = _to_int(payload, "MISSING_SIGNAL_SESSION_COUNT")
    duplicate_sessions = _to_int(payload, "DUPLICATE_SIGNAL_SESSION_COUNT")
    unknown_sessions = _to_int(payload, "UNKNOWN_SIGNAL_COUNT")
    session_values = (
        expected_sessions,
        observed_sessions,
        missing_sessions,
        duplicate_sessions,
        unknown_sessions,
    )
    if context.window_and_calendar_unchanged is False:
        findings.append(
            _finding(
                "SESSION_COVERAGE", AxisStatus.INVALID, "window or calendar changed after freeze"
            )
        )
    elif any(value is None for value in session_values):
        findings.append(
            _finding("SESSION_COVERAGE", AxisStatus.INSUFFICIENT, "session inventory missing")
        )
    elif session_values != (EXPECTED_SIGNAL_SESSION_COUNT, EXPECTED_SIGNAL_SESSION_COUNT, 0, 0, 0):
        findings.append(
            _finding(
                "SESSION_COVERAGE",
                AxisStatus.FAIL,
                "session gap, duplicate, unknown, or count mismatch",
            )
        )
    elif context.window_and_calendar_unchanged is None:
        findings.append(
            _finding(
                "SESSION_COVERAGE",
                AxisStatus.INSUFFICIENT,
                "window/calendar freeze evidence missing",
            )
        )
    else:
        findings.append(
            _finding("SESSION_COVERAGE", AxisStatus.PASS, "1202 unique frozen sessions reconcile")
        )

    dq = _status_triplet(
        payload, ("DATA_QUALITY_STATUS", "POINT_IN_TIME_STATUS", "MANIFEST_REPLAY_STATUS")
    )
    if context.dq_receipt_and_manifest_identity_exact is False:
        findings.append(
            _finding("DQ_PIT_MANIFEST", AxisStatus.INVALID, "DQ receipt or manifest identity drift")
        )
    elif dq is not None:
        findings.append(dq)
    elif context.dq_receipt_and_manifest_identity_exact is None:
        findings.append(
            _finding(
                "DQ_PIT_MANIFEST", AxisStatus.INSUFFICIENT, "DQ/manifest identity evidence missing"
            )
        )
    else:
        findings.append(
            _finding("DQ_PIT_MANIFEST", AxisStatus.PASS, "DQ, PIT, and manifest replay pass")
        )

    if context.frozen_slots_exact is False or context.engine_default_substituted is True:
        findings.append(
            _finding(
                "FROZEN_37_SLOT_POLICY",
                AxisStatus.INVALID,
                "slot changed or engine default substituted",
            )
        )
    elif (
        context.frozen_slot_count is None
        or context.frozen_slots_exact is None
        or context.engine_default_substituted is None
    ):
        findings.append(
            _finding(
                "FROZEN_37_SLOT_POLICY", AxisStatus.INSUFFICIENT, "37-slot replay evidence missing"
            )
        )
    elif context.frozen_slot_count != EXPECTED_FROZEN_SLOT_COUNT:
        findings.append(
            _finding(
                "FROZEN_37_SLOT_POLICY", AxisStatus.FAIL, "platform cannot implement all 37 slots"
            )
        )
    else:
        findings.append(
            _finding("FROZEN_37_SLOT_POLICY", AxisStatus.PASS, "all 37 frozen slots are exact")
        )

    if context.option_alpha_input_detected is True:
        findings.append(
            _finding(
                "OPTION_ALPHA_ISOLATION", AxisStatus.FAIL, "direction signal depends on option data"
            )
        )
    elif (
        context.signal_input_lineage_complete is None or context.option_alpha_input_detected is None
    ):
        findings.append(
            _finding(
                "OPTION_ALPHA_ISOLATION", AxisStatus.INSUFFICIENT, "signal input lineage incomplete"
            )
        )
    elif context.signal_input_lineage_complete is False:
        findings.append(
            _finding(
                "OPTION_ALPHA_ISOLATION", AxisStatus.INSUFFICIENT, "signal input lineage incomplete"
            )
        )
    else:
        findings.append(
            _finding(
                "OPTION_ALPHA_ISOLATION",
                AxisStatus.PASS,
                "option data does not influence direction",
            )
        )

    comparator_identity = (
        payload.get("COMPARATOR_ID"),
        payload.get("COMPARATOR_VERSION"),
        payload.get("SIGNAL_IDENTITY_MATCH"),
    )
    if (
        context.comparator_changed_after_freeze is True
        or (context.comparator_order_submission_count or 0) > 0
    ):
        findings.append(
            _finding(
                "COMPARATOR_CONTRACT", AxisStatus.INVALID, "comparator drift or order submission"
            )
        )
    elif context.comparator_platform_evidence_complete is None or any(
        _missing(value) for value in comparator_identity
    ):
        findings.append(
            _finding(
                "COMPARATOR_CONTRACT",
                AxisStatus.INSUFFICIENT,
                "comparator platform evidence missing",
            )
        )
    elif context.comparator_platform_evidence_complete is False:
        findings.append(
            _finding(
                "COMPARATOR_CONTRACT", AxisStatus.FAIL, "platform cannot maintain required ledger"
            )
        )
    elif comparator_identity != (COMPARATOR_ID, COMPARATOR_VERSION, True):
        findings.append(
            _finding(
                "COMPARATOR_CONTRACT", AxisStatus.INVALID, "frozen comparator identity mismatch"
            )
        )
    else:
        findings.append(
            _finding(
                "COMPARATOR_CONTRACT", AxisStatus.PASS, "one signal-matched primary comparator"
            )
        )

    option_start = _to_decimal(payload, "OPTIONIZED_START_EQUITY_USD")
    underlying_start = _to_decimal(payload, "UNDERLYING_START_EQUITY_USD")
    min_cash = _to_decimal(payload, "UNDERLYING_MIN_CASH_USD")
    if option_start is None or underlying_start is None or min_cash is None:
        findings.append(
            _finding(
                "CAPITAL_NORMALIZATION",
                AxisStatus.INSUFFICIENT,
                "start capital or ledger evidence missing",
            )
        )
    elif min_cash < 0:
        findings.append(
            _finding(
                "CAPITAL_NORMALIZATION",
                AxisStatus.INVALID,
                "negative cash or leverage-like funding",
            )
        )
    elif (option_start, underlying_start) != (INITIAL_CAPITAL_USD, INITIAL_CAPITAL_USD):
        findings.append(
            _finding("CAPITAL_NORMALIZATION", AxisStatus.FAIL, "noncomparable capital base")
        )
    else:
        findings.append(
            _finding(
                "CAPITAL_NORMALIZATION", AxisStatus.PASS, "both accounts start with USD 100,000"
            )
        )

    mismatch_count = _to_int(payload, "EFFECTIVE_EVENT_MISMATCH_COUNT")
    alignment_count = _to_int(payload, "EFFECTIVE_EVENT_ALIGNMENT_COUNT")
    expected_transitions = _to_int(payload, "EXPECTED_TRANSITION_COUNT")
    observed_transitions = _to_int(payload, "OBSERVED_TRANSITION_COUNT")
    if any(
        value is None
        for value in (
            mismatch_count,
            alignment_count,
            expected_transitions,
            observed_transitions,
        )
    ):
        findings.append(
            _finding("EVENT_ALIGNMENT", AxisStatus.INSUFFICIENT, "event alignment counts missing")
        )
    elif (
        mismatch_count != 0
        or expected_transitions != 83
        or observed_transitions != expected_transitions
        or alignment_count != observed_transitions
    ):
        findings.append(_finding("EVENT_ALIGNMENT", AxisStatus.FAIL, "nonzero event mismatch"))
    else:
        findings.append(_finding("EVENT_ALIGNMENT", AxisStatus.PASS, "effective events reconcile"))

    accounting_reconciles = _accounting_reconciles(payload)
    if context.local_option_repricing_used is True:
        findings.append(
            _finding("ACCOUNTING", AxisStatus.INVALID, "local option repricing substitute used")
        )
    elif accounting_reconciles is None or context.accounting_reconciled is None:
        findings.append(
            _finding(
                "ACCOUNTING", AxisStatus.INSUFFICIENT, "ledger reconciliation evidence missing"
            )
        )
    elif accounting_reconciles is False or context.accounting_reconciled is False:
        findings.append(_finding("ACCOUNTING", AxisStatus.FAIL, "ledger reconciliation failed"))
    else:
        findings.append(_finding("ACCOUNTING", AxisStatus.PASS, "both ledgers reconcile"))

    risk_fields = loaded.contract.contract.export_safe_fields.risk
    risk_values = tuple(_to_decimal(payload, field) for field in risk_fields)
    if context.local_greek_reconstruction_used is True:
        findings.append(
            _finding("RISK_FIELDS", AxisStatus.INVALID, "local Greek reconstruction used")
        )
    elif any(value is None for value in risk_values) or context.risk_fields_reconciled is None:
        findings.append(
            _finding("RISK_FIELDS", AxisStatus.INSUFFICIENT, "mandatory risk evidence missing")
        )
    elif context.risk_fields_reconciled is False:
        findings.append(_finding("RISK_FIELDS", AxisStatus.FAIL, "risk reconciliation failed"))
    else:
        findings.append(
            _finding(
                "RISK_FIELDS", AxisStatus.PASS, "mandatory risk fields are finite and reconciled"
            )
        )

    if context.raw_option_export_detected is True:
        findings.append(
            _finding("EXPORT_SAFETY", AxisStatus.INVALID, "raw option surface exported")
        )
    elif context.export_inventory_evidence_complete is None:
        findings.append(
            _finding("EXPORT_SAFETY", AxisStatus.INSUFFICIENT, "export inventory evidence missing")
        )
    elif context.export_inventory_evidence_complete is False:
        findings.append(_finding("EXPORT_SAFETY", AxisStatus.FAIL, "required export field omitted"))
    else:
        findings.append(
            _finding(
                "EXPORT_SAFETY",
                AxisStatus.PASS,
                f"exact {admission.field_count}-field aggregate inventory",
            )
        )

    platform_fields = (
        "RUN_ID",
        "PROJECT_ID",
        "BACKTEST_ID",
        "LEAN_VERSION",
        "PLATFORM_VERSION",
        "CLOUD_BUILD_IDENTITIES",
    )
    if context.platform_identity_drift is True:
        findings.append(
            _finding(
                "PLATFORM_IDENTITY", AxisStatus.INVALID, "project/code/policy/build identity drift"
            )
        )
    elif context.platform_identity_complete is None or any(
        _missing(payload.get(field)) for field in platform_fields
    ):
        findings.append(
            _finding("PLATFORM_IDENTITY", AxisStatus.INSUFFICIENT, "platform identity missing")
        )
    elif context.platform_identity_supported is False:
        findings.append(
            _finding("PLATFORM_IDENTITY", AxisStatus.FAIL, "platform identity unsupported")
        )
    elif context.platform_identity_complete is False or context.platform_identity_supported is None:
        findings.append(
            _finding(
                "PLATFORM_IDENTITY",
                AxisStatus.INSUFFICIENT,
                "platform identity evidence incomplete",
            )
        )
    else:
        findings.append(
            _finding(
                "PLATFORM_IDENTITY", AxisStatus.PASS, "run/project/code/Lean/build identities exact"
            )
        )

    findings.append(_calendar_finding(context, loaded))

    diagnostics = payload.get("PREREGISTERED_NAMED_DIAGNOSTIC_RESULTS")
    diagnostic_count = len(diagnostics) if isinstance(diagnostics, (list, tuple, dict)) else None
    if context.post_result_diagnostic_added is True:
        findings.append(
            _finding("MULTIPLICITY", AxisStatus.INVALID, "post-result diagnostic added")
        )
    elif context.diagnostic_inventory_complete is None or diagnostic_count is None:
        findings.append(
            _finding("MULTIPLICITY", AxisStatus.INSUFFICIENT, "diagnostic inventory missing")
        )
    elif diagnostic_count > 2:
        findings.append(
            _finding("MULTIPLICITY", AxisStatus.INVALID, "more than two named diagnostics")
        )
    elif context.diagnostic_inventory_complete is False:
        findings.append(
            _finding("MULTIPLICITY", AxisStatus.FAIL, "preregistered diagnostic unavailable")
        )
    else:
        findings.append(
            _finding(
                "MULTIPLICITY", AxisStatus.PASS, "one primary and at most two named diagnostics"
            )
        )

    return_delta = _to_decimal(payload, "PRIMARY_RETURN_DELTA")
    if context.result_used_for_selection is True:
        findings.append(
            _finding(
                "PRIMARY_IMPLEMENTATION_ESTIMAND",
                AxisStatus.INVALID,
                "result used to select normalization or baseline",
            )
        )
    elif return_delta is None:
        findings.append(
            _finding(
                "PRIMARY_IMPLEMENTATION_ESTIMAND",
                AxisStatus.INSUFFICIENT,
                "paired return delta not computable",
            )
        )
    elif return_delta > 0:
        findings.append(
            _finding(
                "PRIMARY_IMPLEMENTATION_ESTIMAND",
                AxisStatus.PASS,
                "paired return delta strictly positive",
            )
        )
    else:
        findings.append(
            _finding(
                "PRIMARY_IMPLEMENTATION_ESTIMAND",
                AxisStatus.FAIL,
                "paired return delta nonpositive",
            )
        )

    if context.authorization is AuthorizationEvidence.UNAUTHORIZED_ACTION:
        findings.append(
            _finding("EXTERNAL_AUTHORIZATION", AxisStatus.INVALID, "unauthorized external action")
        )
    elif context.authorization is AuthorizationEvidence.DENIED:
        findings.append(
            _finding(
                "EXTERNAL_AUTHORIZATION", AxisStatus.FAIL, "owner denied or expired run authority"
            )
        )
    elif context.authorization is AuthorizationEvidence.MISSING:
        findings.append(
            _finding(
                "EXTERNAL_AUTHORIZATION",
                AxisStatus.INSUFFICIENT,
                "separate run authority not granted",
            )
        )
    else:
        findings.append(
            _finding(
                "EXTERNAL_AUTHORIZATION",
                AxisStatus.PASS,
                "separate run authority and maxima satisfied",
            )
        )

    expected_axis_ids = tuple(row.axis_id for row in loaded.contract.contract.falsification.axes)
    observed_axis_ids = tuple(row.axis_id for row in findings)
    if observed_axis_ids != expected_axis_ids:
        raise PairedComparisonWaveAError(
            "FALSIFICATION_AXIS_DRIFT",
            f"expected={expected_axis_ids!r}; observed={observed_axis_ids!r}",
        )
    return tuple(findings)


def reduce_axis_findings(
    findings: Sequence[AxisFinding],
    *,
    terminal_precedence: Sequence[str] = ("INVALID", "FAIL", "INSUFFICIENT", "PASS"),
) -> AxisStatus:
    if tuple(terminal_precedence) != ("INVALID", "FAIL", "INSUFFICIENT", "PASS"):
        raise PairedComparisonWaveAError(
            "TERMINAL_PRECEDENCE_DRIFT", repr(tuple(terminal_precedence))
        )
    if not findings:
        raise PairedComparisonWaveAError("EMPTY_FALSIFICATION_MATRIX", "no findings")
    statuses = {finding.status for finding in findings}
    for candidate in (
        AxisStatus.INVALID,
        AxisStatus.FAIL,
        AxisStatus.INSUFFICIENT,
        AxisStatus.PASS,
    ):
        if candidate in statuses:
            return candidate
    raise PairedComparisonWaveAError("UNKNOWN_AXIS_STATUS", repr(statuses))


def replay_export_safe_aggregate(
    payload: Mapping[str, object],
    *,
    context: ReplayContext,
    authority: PairedComparisonContractFreezeAdmissionLoadResult | None = None,
) -> PairedComparisonReplayReceipt:
    loaded = authority or load_wave_a_authority()
    admission = admit_export_safe_aggregate(payload, authority=loaded)
    findings = evaluate_falsification_axes(
        payload, context=context, admission=admission, authority=loaded
    )
    precedence = loaded.contract.contract.falsification.terminal_precedence
    return PairedComparisonReplayReceipt(
        aggregate_payload_sha256=admission.payload_sha256,
        axis_findings=findings,
        terminal_status=reduce_axis_findings(findings, terminal_precedence=precedence),
        terminal_precedence=precedence,
        export_safe_field_count=admission.field_count,
    )


def render_quantconnect_comparator_helper_fragment() -> bytes:
    """Return deterministic LF-only helper bytes, intentionally not a runnable main.py."""

    inventory = export_safe_field_inventory(load_wave_a_authority())
    inventory_source = "".join(f'        "{field}",\n' for field in inventory)
    source = (
        '''\
from decimal import Decimal, ROUND_FLOOR


class FrozenFullyFundedQQQComparator:
    """Virtual same-signal comparator; never calls MarketOrder or submits orders."""

    INITIAL_CASH_USD = Decimal("100000.00")
    EXPORT_SAFE_FIELDS = (
'''
        + inventory_source
        + """\
    )

    def __init__(self):
        self.cash = self.INITIAL_CASH_USD
        self.shares = 0
        self.fees = Decimal("0")

    def enter_at_ask(self, ask, fee_usd):
        ask = Decimal(str(ask))
        fee_usd = Decimal(str(fee_usd))
        if self.shares != 0 or ask <= 0 or fee_usd < 0:
            raise ValueError("invalid frozen comparator entry")
        shares = int(((self.cash - fee_usd) / ask).to_integral_value(rounding=ROUND_FLOOR))
        if shares <= 0:
            raise ValueError("insufficient fully funded comparator cash")
        self.cash -= Decimal(shares) * ask + fee_usd
        if self.cash < 0:
            raise ValueError("negative cash forbidden")
        self.shares = shares
        self.fees += fee_usd

    def exit_at_bid(self, bid, fee_usd):
        bid = Decimal(str(bid))
        fee_usd = Decimal(str(fee_usd))
        if self.shares <= 0 or bid <= 0 or fee_usd < 0:
            raise ValueError("invalid frozen comparator exit")
        self.cash += Decimal(self.shares) * bid - fee_usd
        if self.cash < 0:
            raise ValueError("negative cash forbidden")
        self.shares = 0
        self.fees += fee_usd

    def mark_to_bid(self, bid):
        return self.cash + Decimal(self.shares) * Decimal(str(bid))

    def validate_export_field_inventory(self, payload):
        if len(payload) != len(self.EXPORT_SAFE_FIELDS):
            raise ValueError("frozen export field count mismatch")
        if set(payload) != set(self.EXPORT_SAFE_FIELDS):
            raise ValueError("frozen export field inventory mismatch")
        return payload
"""
    )
    encoded = source.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")
    if b"QCAlgorithm" in encoded or b"MarketOrder(" in encoded:
        raise PairedComparisonWaveAError(
            "EXECUTABLE_QC_SURFACE_REJECTED", "helper fragment contains executable surface"
        )
    return encoded


def quantconnect_comparator_helper_sha256() -> str:
    return hashlib.sha256(render_quantconnect_comparator_helper_fragment()).hexdigest()


__all__ = [
    "AggregateAdmissionReceipt",
    "AuthorizationEvidence",
    "AxisFinding",
    "AxisStatus",
    "CalendarPartitionEvidence",
    "ComparatorAction",
    "ComparatorEvent",
    "ComparatorLedgerAggregate",
    "ComparatorObservation",
    "FullyFundedQQQCashLedger",
    "PairedComparisonReplayReceipt",
    "PairedComparisonWaveAError",
    "QQQQuote",
    "ReplayContext",
    "admit_export_safe_aggregate",
    "canonical_aggregate_json",
    "evaluate_falsification_axes",
    "export_safe_field_inventory",
    "load_wave_a_authority",
    "quantconnect_comparator_helper_sha256",
    "reduce_axis_findings",
    "render_quantconnect_comparator_helper_fragment",
    "replay_export_safe_aggregate",
]
