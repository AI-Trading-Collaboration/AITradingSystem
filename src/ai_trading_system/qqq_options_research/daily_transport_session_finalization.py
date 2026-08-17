"""Offline v2 session-finalization contract and zero-order proposal for TRADING-2531.

The pure reducer makes same-session event ordering explicit.  Package construction is
local-only: this module has no QuantConnect, browser, network, broker, or execution path.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Final

from ai_trading_system.platform.artifacts import write_bytes_atomic
from ai_trading_system.qqq_options_research.daily_transport_per_axis_collection_proposal import (
    Axis,
    AxisStatus,
    build_per_axis_collection_proposal_package,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICY_PATH = Path(
    "config/research/qc_qqq_options_daily_transport_session_finalization_v2.yaml"
)
DEFAULT_PACKAGE_ROOT = Path(
    "inputs/research/qqq_options/trading_2531_daily_transport_session_finalization_v2"
)
TASK_ID: Final = (
    "TRADING-2531_QC_QQQ_OPTIONS_DAILY_TRANSPORT_SESSION_FINALIZATION_AND_"
    "UNDERLYING_PRICE_SOURCE_CONTRACT_FIX_V1"
)
_UNSEALED_HASH = "0" * 64
_AXES = tuple(Axis)
_STATUSES = tuple(AxisStatus)
_CHAIN_AXES = (
    Axis.BID_ASK_QUOTE,
    Axis.GREEKS,
    Axis.IMPLIED_VOLATILITY,
    Axis.OPEN_INTEREST,
    Axis.VOLUME,
)
_DIAGNOSTIC_KEYS = (
    "CHAINLESS_SLICE_EVENTS",
    "SESSIONS_WITH_CHAINLESS_SLICE",
    "SESSIONS_RECOVERED_AFTER_CHAINLESS",
    "SESSIONS_NEVER_CHAIN",
    "SESSIONS_WITH_CANONICAL_EQUITY_PRESENT",
    "SESSIONS_WITH_CANONICAL_EQUITY_MISSING",
    "SESSIONS_WITH_CANONICAL_EQUITY_INVALID",
    "SESSIONS_WITH_CONTRACT_ZERO_IGNORED",
    "SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS",
)
_PACKAGE_FILES = (
    "main.py",
    "owner_decision_request.md",
    "package_manifest.json",
    "proposal.json",
    "session_finalization_contract.json",
)


class SessionFinalizationError(ValueError):
    """Typed fail-closed error for the v2 contract and package."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail


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
        raise ValueError("content_sha256 mismatch")


def _verify_compact_predecessor_seal(payload: Mapping[str, Any]) -> None:
    body = dict(payload)
    observed = body.pop("content_sha256", None)
    raw = (
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    if observed != _sha256(raw):
        raise ValueError("predecessor content_sha256 mismatch")


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


@dataclass(frozen=True)
class AxisSignal:
    """One Slice's aggregate-only evidence for an axis."""

    observed: bool
    valid: bool

    def __post_init__(self) -> None:
        if self.valid and not self.observed:
            raise ValueError("valid axis signal must be observed")


@dataclass(frozen=True)
class SessionSliceObservation:
    """No-row, no-identifier observation passed to the deterministic reducer."""

    session_id: date
    chain_contract_count: int
    equity_close: object = None
    chain_axis_signals: Mapping[Axis, AxisSignal] = field(default_factory=dict)
    cross_fields_without_underlying_valid: bool = False
    contract_underlying_zero_observed: bool = False

    def __post_init__(self) -> None:
        if self.chain_contract_count < 0:
            raise ValueError("chain_contract_count must be non-negative")
        actual = set(self.chain_axis_signals)
        if self.chain_contract_count == 0 and actual:
            raise ValueError("chain-less Slice must not carry chain-axis signals")
        if self.chain_contract_count > 0 and actual != set(_CHAIN_AXES):
            raise ValueError("non-empty chain requires the exact chain-axis signal set")


@dataclass
class _SessionState:
    slice_events: int = 0
    chainless_slice_events: int = 0
    chainless_before_first_chain: bool = False
    chain_events: int = 0
    equity_observed: bool = False
    equity_valid: bool = False
    equity_invalid: bool = False
    contract_zero_observed: bool = False
    cross_without_underlying_valid: bool = False
    axis_observed: dict[Axis, bool] = field(
        default_factory=lambda: {axis: False for axis in _CHAIN_AXES}
    )
    axis_valid: dict[Axis, bool] = field(
        default_factory=lambda: {axis: False for axis in _CHAIN_AXES}
    )


@dataclass(frozen=True)
class SessionReductionResult:
    expected_session_count: int
    observed_session_count: int
    per_axis_status_session_counts: Mapping[str, int]
    diagnostic_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        for axis in _AXES:
            total = sum(
                self.per_axis_status_session_counts[
                    f"TRADING2531_{axis.value}_{status.value}_SESSIONS"
                ]
                for status in _STATUSES
            )
            if total != self.expected_session_count:
                raise ValueError(f"axis total mismatch: {axis.value}")
        if (
            self.diagnostic_counts["SESSIONS_NEVER_CHAIN"]
            + self.per_axis_status_session_counts[
                "TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"
            ]
            != self.expected_session_count
        ):
            raise ValueError("chain session partition mismatch")
        equity_total = sum(
            self.diagnostic_counts[key]
            for key in (
                "SESSIONS_WITH_CANONICAL_EQUITY_PRESENT",
                "SESSIONS_WITH_CANONICAL_EQUITY_MISSING",
                "SESSIONS_WITH_CANONICAL_EQUITY_INVALID",
            )
        )
        chain_present = self.per_axis_status_session_counts[
            "TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"
        ]
        if equity_total != chain_present:
            raise ValueError("canonical equity provenance partition mismatch")


class DailyTransportSessionReducer:
    """Commutative per-session reducer; absence is terminal only at finalize()."""

    def __init__(self, expected_sessions: Sequence[date]) -> None:
        ordered = tuple(expected_sessions)
        if not ordered or tuple(sorted(set(ordered))) != ordered:
            raise ValueError("expected_sessions must be sorted and unique")
        self._expected = ordered
        self._states = {session: _SessionState() for session in ordered}
        self._finalized = False

    def observe(self, observation: SessionSliceObservation) -> None:
        if self._finalized:
            raise ValueError("reducer already finalized")
        if observation.session_id not in self._states:
            raise ValueError("observation outside expected sessions")
        state = self._states[observation.session_id]
        state.slice_events += 1
        equity = _finite(observation.equity_close)
        if equity is not None:
            state.equity_observed = True
            if equity > 0:
                state.equity_valid = True
            else:
                state.equity_invalid = True
        if observation.chain_contract_count == 0:
            state.chainless_slice_events += 1
            if state.chain_events == 0:
                state.chainless_before_first_chain = True
            return
        state.chain_events += 1
        state.contract_zero_observed |= observation.contract_underlying_zero_observed
        state.cross_without_underlying_valid |= (
            observation.cross_fields_without_underlying_valid
        )
        for axis in _CHAIN_AXES:
            signal = observation.chain_axis_signals[axis]
            state.axis_observed[axis] |= signal.observed
            state.axis_valid[axis] |= signal.valid

    @staticmethod
    def _axis_status(observed: bool, valid: bool) -> AxisStatus:
        if not observed:
            return AxisStatus.MISSING
        return AxisStatus.PRESENT if valid else AxisStatus.INVALID

    def finalize(self) -> SessionReductionResult:
        if self._finalized:
            raise ValueError("reducer already finalized")
        self._finalized = True
        counts = {
            f"TRADING2531_{axis.value}_{status.value}_SESSIONS": 0
            for axis in _AXES
            for status in _STATUSES
        }
        diagnostics = {key: 0 for key in _DIAGNOSTIC_KEYS}
        observed_sessions = 0
        for state in self._states.values():
            if state.slice_events:
                observed_sessions += 1
            diagnostics["CHAINLESS_SLICE_EVENTS"] += state.chainless_slice_events
            if state.chainless_slice_events:
                diagnostics["SESSIONS_WITH_CHAINLESS_SLICE"] += 1
            if state.chain_events == 0:
                diagnostics["SESSIONS_NEVER_CHAIN"] += 1
                counts["TRADING2531_OPTION_CHAIN_PRESENCE_MISSING_SESSIONS"] += 1
                for axis in _AXES[1:]:
                    counts[
                        f"TRADING2531_{axis.value}_NOT_EVALUATED_SESSIONS"
                    ] += 1
                continue
            counts["TRADING2531_OPTION_CHAIN_PRESENCE_PRESENT_SESSIONS"] += 1
            if state.chainless_before_first_chain:
                diagnostics["SESSIONS_RECOVERED_AFTER_CHAINLESS"] += 1
            if state.chain_events > 1:
                diagnostics["SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS"] += 1
            if state.contract_zero_observed:
                diagnostics["SESSIONS_WITH_CONTRACT_ZERO_IGNORED"] += 1
            if state.equity_valid:
                underlying_status = AxisStatus.PRESENT
                diagnostics["SESSIONS_WITH_CANONICAL_EQUITY_PRESENT"] += 1
            elif state.equity_observed or state.equity_invalid:
                underlying_status = AxisStatus.INVALID
                diagnostics["SESSIONS_WITH_CANONICAL_EQUITY_INVALID"] += 1
            else:
                underlying_status = AxisStatus.MISSING
                diagnostics["SESSIONS_WITH_CANONICAL_EQUITY_MISSING"] += 1
            counts[
                f"TRADING2531_UNDERLYING_PRICE_{underlying_status.value}_SESSIONS"
            ] += 1
            for axis in _CHAIN_AXES:
                status = self._axis_status(
                    state.axis_observed[axis], state.axis_valid[axis]
                )
                counts[f"TRADING2531_{axis.value}_{status.value}_SESSIONS"] += 1
            cross = (
                AxisStatus.PRESENT
                if underlying_status is AxisStatus.PRESENT
                and state.cross_without_underlying_valid
                else AxisStatus.INVALID
            )
            counts[
                f"TRADING2531_CROSS_FIELD_CONSISTENCY_{cross.value}_SESSIONS"
            ] += 1
        return SessionReductionResult(
            expected_session_count=len(self._expected),
            observed_session_count=observed_sessions,
            per_axis_status_session_counts=counts,
            diagnostic_counts=diagnostics,
        )


@dataclass(frozen=True)
class BuiltSessionFinalizationPackage:
    policy: Mapping[str, Any]
    policy_file_sha256: str
    policy_canonical_sha256: str
    contract: Mapping[str, Any]
    proposal: Mapping[str, Any]
    project_code_bytes: bytes
    owner_decision_request_bytes: bytes
    manifest: Mapping[str, Any]


def _load_policy(project_root: Path) -> tuple[dict[str, Any], bytes]:
    path = (project_root / DEFAULT_POLICY_PATH).resolve()
    raw = path.read_bytes()
    value = safe_load_yaml_path(path)
    if not isinstance(value, dict):
        raise SessionFinalizationError("SESSION_FINALIZATION_POLICY_INVALID", "root")
    required = {
        "schema_version",
        "policy_id",
        "policy_version",
        "policy_status",
        "task_id",
        "registration_base_repository_code_sha",
        "created_at_utc",
        "package_root",
        "target_project_id",
        "requested_start",
        "requested_end",
        "exchange_calendar",
        "expected_session_count",
        "predecessor_evidence_path",
        "predecessor_evidence_content_sha256",
        "predecessor_source_result_sha256",
        "predecessor_project_code_lf_sha256",
        "predecessor_run_scope_content_sha256",
        "current_external_project_mutations",
        "current_external_cloud_backtests",
        "current_external_orders",
        "current_external_fills",
        "external_action_authorized",
        "maximum_external_project_mutations_in_this_task",
        "maximum_external_cloud_backtests_in_this_task",
        "maximum_orders",
        "maximum_fills",
        "raw_option_rows_allowed",
        "logs_as_data_allowed",
        "object_store_allowed",
        "api_cli_http_browser_allowed",
        "stale_underlying_fallback_allowed",
        "production_effect",
        "broker_action",
    }
    if set(value) != required:
        raise SessionFinalizationError("SESSION_FINALIZATION_POLICY_INVALID", "keyset")
    expected = {
        "schema_version": "qc_qqq_options_daily_transport_session_finalization_policy.v2",
        "policy_id": "qc_qqq_options_daily_transport_session_finalization_v2",
        "policy_version": "2.0.0",
        "policy_status": "OFFLINE_CONTRACT_FIX_OWNER_REVIEW_REQUIRED",
        "task_id": TASK_ID,
        "registration_base_repository_code_sha": "4c4c108bb0af990833b325ca11cce5d21d8505c9",
        "package_root": DEFAULT_PACKAGE_ROOT.as_posix(),
        "target_project_id": 34808569,
        "requested_start": "2021-02-22",
        "requested_end": "2025-12-02",
        "exchange_calendar": "XNYS",
        "expected_session_count": 1202,
        "predecessor_evidence_content_sha256": (
            "d47f3234f58e1a7114984a7a79a5090082f923b7e02c65a66dfa8b761321f792"
        ),
        "predecessor_source_result_sha256": (
            "2233b20a900c76cbb6938a96c635c5dabc5855349ac74ff684c8f1c657b752b7"
        ),
        "current_external_project_mutations": 1,
        "current_external_cloud_backtests": 1,
        "current_external_orders": 0,
        "current_external_fills": 0,
        "external_action_authorized": False,
        "maximum_external_project_mutations_in_this_task": 0,
        "maximum_external_cloud_backtests_in_this_task": 0,
        "maximum_orders": 0,
        "maximum_fills": 0,
        "raw_option_rows_allowed": False,
        "logs_as_data_allowed": False,
        "object_store_allowed": False,
        "api_cli_http_browser_allowed": False,
        "stale_underlying_fallback_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    for key, item in expected.items():
        if value.get(key) != item:
            raise SessionFinalizationError("SESSION_FINALIZATION_POLICY_INVALID", key)
    return value, raw


def _load_predecessor_evidence(project_root: Path, policy: Mapping[str, Any]) -> dict[str, Any]:
    relative = Path(str(policy["predecessor_evidence_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise SessionFinalizationError("SESSION_FINALIZATION_PREDECESSOR_INVALID", "path")
    raw = (project_root / relative).read_bytes()
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise SessionFinalizationError("SESSION_FINALIZATION_PREDECESSOR_INVALID", "canonical")
    compact = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")
    if raw != compact:
        raise SessionFinalizationError("SESSION_FINALIZATION_PREDECESSOR_INVALID", "canonical")
    _verify_compact_predecessor_seal(payload)
    if (
        payload.get("content_sha256")
        != policy["predecessor_evidence_content_sha256"]
        or payload.get("source_result_file_sha256")
        != policy["predecessor_source_result_sha256"]
        or payload.get("project_code_lf_sha256")
        != policy["predecessor_project_code_lf_sha256"]
        or payload.get("observed_session_count") != 1202
        or payload.get("orders") != 0
        or payload.get("fills") != 0
    ):
        raise SessionFinalizationError("SESSION_FINALIZATION_PREDECESSOR_INVALID", "binding")
    return payload


def _render_project_code(session_ids: Sequence[date], contract_hash: str) -> bytes:
    sessions = repr(tuple(item.isoformat() for item in session_ids))
    template = '''from AlgorithmImports import *
import math

# TRADING-2531 future candidate only. No external execution is authorized here.
# Zero orders; aggregate-only runtime statistics; no logs or Object Store.
EXPECTED_SESSIONS = __SESSIONS__
AXES = __AXES__
STATUSES = ("PRESENT", "MISSING", "INVALID", "NOT_EVALUATED")
DIAGNOSTICS = __DIAGNOSTICS__
IDENTITY = "schema=qc_qqq_options_daily_transport_per_axis_runtime.v2|contract=__CONTRACT__"


class QQQOptionsDailyTransportSessionFinalizationCollector(QCAlgorithm):
    def initialize(self):
        self.set_start_date(2021, 2, 22)
        self.set_end_date(2025, 12, 2)
        self.set_cash(100000)
        self.settings.daily_precise_end_time = True
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
            "chainless_slice_events": 0,
            "chainless_before_first_chain": False,
            "chain_events": 0,
            "equity_observed": False,
            "equity_valid": False,
            "equity_invalid": False,
            "contract_zero_observed": False,
            "cross_without_underlying_valid": False,
            "axis_observed": {axis: False for axis in AXES[2:7]},
            "axis_valid": {axis: False for axis in AXES[2:7]},
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
            state["chainless_slice_events"] += 1
            if state["chain_events"] == 0:
                state["chainless_before_first_chain"] = True
            return
        state["chain_events"] += 1
        contracts = list(chain)
        contract_underlyings = [
            self._finite(self._attribute(item, "underlying_last_price"))
            for item in contracts
        ]
        state["contract_zero_observed"] |= any(value == 0 for value in contract_underlyings)
        bids = [self._finite(self._attribute(item, "bid_price", "bidprice")) for item in contracts]
        asks = [self._finite(self._attribute(item, "ask_price", "askprice")) for item in contracts]
        ivs = [self._finite(self._attribute(item, "implied_volatility")) for item in contracts]
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
        session = data.time.date()
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

    def on_order_event(self, order_event):
        self._order_event_count += 1

    def on_end_of_algorithm(self):
        counts = {axis: {status: 0 for status in STATUSES} for axis in AXES}
        diagnostics = {key: 0 for key in DIAGNOSTICS}
        observed_sessions = 0
        for state in self._states.values():
            observed_sessions += int(state["slice_events"] > 0)
            diagnostics["CHAINLESS_SLICE_EVENTS"] += state["chainless_slice_events"]
            diagnostics["SESSIONS_WITH_CHAINLESS_SLICE"] += int(state["chainless_slice_events"] > 0)
            if state["chain_events"] == 0:
                diagnostics["SESSIONS_NEVER_CHAIN"] += 1
                counts["OPTION_CHAIN_PRESENCE"]["MISSING"] += 1
                for axis in AXES[1:]:
                    counts[axis]["NOT_EVALUATED"] += 1
                continue
            counts["OPTION_CHAIN_PRESENCE"]["PRESENT"] += 1
            diagnostics["SESSIONS_RECOVERED_AFTER_CHAINLESS"] += int(
                state["chainless_before_first_chain"]
            )
            diagnostics["SESSIONS_WITH_MULTIPLE_CHAIN_EVENTS"] += int(state["chain_events"] > 1)
            diagnostics["SESSIONS_WITH_CONTRACT_ZERO_IGNORED"] += int(
                state["contract_zero_observed"]
            )
            if state["equity_valid"]:
                underlying_status = "PRESENT"
            elif state["equity_observed"] or state["equity_invalid"]:
                underlying_status = "INVALID"
            else:
                underlying_status = "MISSING"
            diagnostics["SESSIONS_WITH_CANONICAL_EQUITY_" + underlying_status] += 1
            counts["UNDERLYING_PRICE"][underlying_status] += 1
            for axis in AXES[2:7]:
                status = "MISSING" if not state["axis_observed"][axis] else (
                    "PRESENT" if state["axis_valid"][axis] else "INVALID"
                )
                counts[axis][status] += 1
            cross = (
                "PRESENT"
                if underlying_status == "PRESENT"
                and state["cross_without_underlying_valid"]
                else "INVALID"
            )
            counts["CROSS_FIELD_CONSISTENCY"][cross] += 1
        valid = all(sum(counts[axis].values()) == len(EXPECTED_SESSIONS) for axis in AXES)
        valid &= (
            diagnostics["SESSIONS_NEVER_CHAIN"]
            + counts["OPTION_CHAIN_PRESENCE"]["PRESENT"]
            == len(EXPECTED_SESSIONS)
        )
        for axis in AXES:
            for status in STATUSES:
                self.set_runtime_statistic(
                    "TRADING2531_" + axis + "_" + status + "_SESSIONS",
                    str(counts[axis][status]),
                )
        for key in DIAGNOSTICS:
            self.set_runtime_statistic("TRADING2531_" + key, str(diagnostics[key]))
        self.set_runtime_statistic("TRADING2531_IDENTITY", IDENTITY)
        terminal = (
            "COMPLETE"
            if valid and self._order_event_count == 0 and not self.portfolio.invested
            else "INVALID"
        )
        self.set_runtime_statistic(
            "TRADING2531_TERMINAL",
            "status=" + terminal + "|expected_sessions=" + str(len(EXPECTED_SESSIONS))
            + "|observed_sessions=" + str(observed_sessions)
            + "|orders=0|fills=0|portfolio_invested=false|raw_rows=false"
            + "|logs_as_data=false|object_store=false|stale_underlying_fallback=false",
        )
'''
    rendered = (
        template.replace("__SESSIONS__", sessions)
        .replace("__AXES__", repr(tuple(axis.value for axis in _AXES)))
        .replace("__DIAGNOSTICS__", repr(_DIAGNOSTIC_KEYS))
        .replace("__CONTRACT__", contract_hash)
    )
    return rendered.replace("\r\n", "\n").encode("utf-8")


def build_session_finalization_package(
    *, project_root: Path = PROJECT_ROOT
) -> BuiltSessionFinalizationPackage:
    root = project_root.resolve()
    policy, policy_raw = _load_policy(root)
    predecessor = _load_predecessor_evidence(root, policy)
    v1 = build_per_axis_collection_proposal_package(project_root=root)
    sessions = v1.run_scope.session_ids
    if (
        len(sessions) != policy["expected_session_count"]
        or v1.run_scope.content_sha256 != policy["predecessor_run_scope_content_sha256"]
    ):
        raise SessionFinalizationError("SESSION_FINALIZATION_SESSION_SCOPE_INVALID", "v1")
    session_inventory_sha = _sha256(
        ("\n".join(item.isoformat() for item in sessions) + "\n").encode("ascii")
    )
    axis_keys = tuple(
        f"TRADING2531_{axis.value}_{status.value}_SESSIONS"
        for axis in _AXES
        for status in _STATUSES
    )
    diagnostic_keys = tuple(f"TRADING2531_{key}" for key in _DIAGNOSTIC_KEYS)
    contract = _seal(
        {
            "schema_version": "qc_qqq_options_daily_transport_session_finalization_contract.v2",
            "contract_id": "TRADING_2531_DAILY_TRANSPORT_SESSION_FINALIZATION_V2",
            "task_id": TASK_ID,
            "created_at_utc": str(policy["created_at_utc"]),
            "predecessor_evidence_content_sha256": predecessor["content_sha256"],
            "predecessor_source_result_sha256": predecessor["source_result_file_sha256"],
            "requested_range": "2021-02-22..2025-12-02",
            "exchange_calendar": "XNYS",
            "expected_session_count": len(sessions),
            "session_inventory_lf_sha256": session_inventory_sha,
            "axis_output_keys": axis_keys,
            "diagnostic_output_keys": diagnostic_keys,
            "session_absence_finalization": "END_OF_ALGORITHM_ONLY",
            "same_session_event_merge": (
                "ORDER_INDEPENDENT_CLASSIFICATION_WITH_ORDERED_RECOVERY_DIAGNOSTIC"
            ),
            "canonical_underlying_source": "SAME_SESSION_RAW_QQQ_EQUITY_TRADEBAR_CLOSE",
            "contract_underlying_zero_policy": "MEASURE_AND_IGNORE_AS_CANONICAL_SOURCE",
            "stale_underlying_fallback_allowed": False,
            "raw_option_rows_allowed": False,
            "logs_as_data_allowed": False,
            "object_store_allowed": False,
            "external_action_authorized": False,
            "maximum_orders": 0,
            "maximum_fills": 0,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    project_code = _render_project_code(sessions, str(contract["content_sha256"]))
    policy_canonical = _canonical_json_bytes(policy)
    proposal = _seal(
        {
            "schema_version": "qc_qqq_options_daily_transport_session_finalization_proposal.v2",
            "proposal_id": "TRADING_2531_DAILY_TRANSPORT_SESSION_FINALIZATION_PROPOSAL_V2",
            "task_id": TASK_ID,
            "issued_at_utc": str(policy["created_at_utc"]),
            "target_project_id": 34808569,
            "requested_range": "2021-02-22..2025-12-02",
            "expected_session_count": 1202,
            "policy_file_sha256": _sha256(policy_raw),
            "policy_canonical_sha256": _sha256(policy_canonical),
            "contract_content_sha256": contract["content_sha256"],
            "contract_canonical_sha256": _sha256(_canonical_json_bytes(contract)),
            "project_code_lf_sha256": _sha256(project_code),
            "project_code_lf_byte_count": len(project_code),
            "predecessor_evidence_content_sha256": predecessor["content_sha256"],
            "predecessor_source_result_sha256": predecessor["source_result_file_sha256"],
            "current_external_counters": {
                "project_mutations": 1,
                "cloud_backtests": 1,
                "orders": 0,
                "fills": 0,
            },
            "requested_future_limits_after_new_owner_authority": {
                "project_mutations": 1,
                "cloud_backtests": 1,
                "orders": 0,
                "fills": 0,
            },
            "authorization_status": "NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS",
            "external_action_performed": False,
            "selection_authorized": False,
            "investment_conclusion_authorized": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    request = (
        "# TRADING-2531 v2 external validation request (not authorization)\n\n"
        "当前任务只修复离线合同，不授权 QuantConnect 动作。"
        "未来若 Owner 决定验证，必须新建 governed execution task，"
        "并绑定以下 exact hashes：\n\n"
        f"- policy file SHA-256: `{_sha256(policy_raw)}`\n"
        f"- policy canonical SHA-256: `{_sha256(policy_canonical)}`\n"
        f"- contract content SHA-256: `{contract['content_sha256']}`\n"
        f"- contract canonical SHA-256: `{_sha256(_canonical_json_bytes(contract))}`\n"
        f"- project code LF SHA-256: `{_sha256(project_code)}`\n"
        f"- predecessor evidence: `{predecessor['content_sha256']}`\n"
        f"- predecessor Results SHA-256: `{predecessor['source_result_file_sha256']}`\n\n"
        "边界：最多一次 project mutation、一次 zero-order Cloud backtest、0 orders、0 fills；"
        "仍禁止 raw rows、logs-as-data、Object Store、DQ/PIT/strategy/trading conclusion。\n"
    ).encode()
    raw_artifacts = {
        "main.py": project_code,
        "owner_decision_request.md": request,
        "proposal.json": _canonical_json_bytes(proposal),
        "session_finalization_contract.json": _canonical_json_bytes(contract),
    }
    artifacts = tuple(
        {
            "relative_path": path,
            "sha256": _sha256(raw_artifacts[path]),
            "byte_count": len(raw_artifacts[path]),
        }
        for path in (
            "main.py",
            "owner_decision_request.md",
            "proposal.json",
            "session_finalization_contract.json",
        )
    )
    manifest = _seal(
        {
            "schema_version": "qc_qqq_options_daily_transport_session_finalization_package.v2",
            "package_id": "TRADING_2531_DAILY_TRANSPORT_SESSION_FINALIZATION_PACKAGE_V2",
            "task_id": TASK_ID,
            "created_at_utc": str(policy["created_at_utc"]),
            "repository_code_sha": policy["registration_base_repository_code_sha"],
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
            "external_action_authorized": False,
            "external_action_performed": False,
            "production_effect": "none",
            "broker_action": "none",
        }
    )
    return BuiltSessionFinalizationPackage(
        policy=policy,
        policy_file_sha256=_sha256(policy_raw),
        policy_canonical_sha256=_sha256(policy_canonical),
        contract=contract,
        proposal=proposal,
        project_code_bytes=project_code,
        owner_decision_request_bytes=request,
        manifest=manifest,
    )


def write_session_finalization_package(
    *, project_root: Path = PROJECT_ROOT
) -> Mapping[str, Any]:
    root = project_root.resolve()
    built = build_session_finalization_package(project_root=root)
    package_root = (root / str(built.policy["package_root"])).resolve()
    if package_root.relative_to(root).as_posix() != built.policy["package_root"]:
        raise SessionFinalizationError("SESSION_FINALIZATION_PACKAGE_INVALID", "path")
    package_root.mkdir(parents=True, exist_ok=True)
    payloads = {
        "main.py": built.project_code_bytes,
        "owner_decision_request.md": built.owner_decision_request_bytes,
        "package_manifest.json": _canonical_json_bytes(built.manifest),
        "proposal.json": _canonical_json_bytes(built.proposal),
        "session_finalization_contract.json": _canonical_json_bytes(built.contract),
    }
    for name in _PACKAGE_FILES:
        write_bytes_atomic(package_root / name, payloads[name])
    return built.manifest


def validate_session_finalization_package(
    package_root: Path = DEFAULT_PACKAGE_ROOT,
    *,
    project_root: Path = PROJECT_ROOT,
) -> BuiltSessionFinalizationPackage:
    root = project_root.resolve()
    expected = build_session_finalization_package(project_root=root)
    actual_root = (package_root if package_root.is_absolute() else root / package_root).resolve()
    try:
        inventory = tuple(sorted(path.name for path in actual_root.iterdir()))
        if inventory != _PACKAGE_FILES:
            raise ValueError("package file inventory is not exact")
        if any(not path.is_file() or path.is_symlink() for path in actual_root.iterdir()):
            raise ValueError("package entries must be regular non-symlink files")
        raw = {name: (actual_root / name).read_bytes() for name in _PACKAGE_FILES}
        expected_raw = {
            "main.py": expected.project_code_bytes,
            "owner_decision_request.md": expected.owner_decision_request_bytes,
            "package_manifest.json": _canonical_json_bytes(expected.manifest),
            "proposal.json": _canonical_json_bytes(expected.proposal),
            "session_finalization_contract.json": _canonical_json_bytes(expected.contract),
        }
        if raw != expected_raw:
            raise ValueError("package bytes drifted")
        for key in ("package_manifest.json", "proposal.json", "session_finalization_contract.json"):
            parsed = json.loads(raw[key])
            if not isinstance(parsed, dict) or raw[key] != _canonical_json_bytes(parsed):
                raise ValueError(f"non-canonical artifact: {key}")
            _verify_seal(parsed)
        for artifact in expected.manifest["artifacts"]:
            item = raw[str(artifact["relative_path"])]
            if _sha256(item) != artifact["sha256"] or len(item) != artifact["byte_count"]:
                raise ValueError("artifact identity mismatch")
    except (OSError, TypeError, ValueError) as exc:
        raise SessionFinalizationError(
            "SESSION_FINALIZATION_PACKAGE_ADMISSION_FAILED", str(exc)
        ) from exc
    return expected


__all__ = [
    "DEFAULT_PACKAGE_ROOT",
    "DEFAULT_POLICY_PATH",
    "AxisSignal",
    "BuiltSessionFinalizationPackage",
    "DailyTransportSessionReducer",
    "SessionFinalizationError",
    "SessionReductionResult",
    "SessionSliceObservation",
    "build_session_finalization_package",
    "validate_session_finalization_package",
    "write_session_finalization_package",
]
