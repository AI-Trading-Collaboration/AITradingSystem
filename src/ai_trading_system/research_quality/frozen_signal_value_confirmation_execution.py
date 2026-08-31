"""Bounded local confirmation for the frozen first-layer signal-value question.

The runner is deliberately narrow: it replays one committed manifest, invokes
the canonical DQ path once, computes the preregistered QQQ/cash comparison once,
and performs one independent arithmetic replay.  It never downloads data,
touches QuantConnect, or emits row-level market/signal payloads.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    run_canonical_data_quality_execution,
)
from ai_trading_system.research_quality import (
    frozen_signal_value_confirmation_preregistration_freeze_admission as freeze_admission,
)
from ai_trading_system.trading_calendar import is_us_equity_trading_day
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AUTHORIZATION_PATH = Path(
    "config/research/frozen_signal_value_confirmation_run_authorization_v1.yaml"
)
DEFAULT_MANIFEST_PATH = Path(
    "inputs/research/frozen_signal_value_confirmation_v1/execution_manifest.json"
)

_TASK_ID = "TRADING-2550_FROZEN_SIGNAL_VALUE_CONFIRMATION_V1"
_AUTHORIZATION_STATUS = "OWNER_EXACT_BOUNDED_CONFIRMATION_AUTHORIZED"
_AUTHORIZATION_DECISION = (
    "owner_decision:TRADING-2550:2026-09-01:authorize_bounded_signal_value_confirmation_v1"
)
_PREREGISTRATION_FILE_SHA256 = "507ab3dd3610971c0962fa093cec0c7f09e1b816f694b7dd946c4b9703013dfa"
_PREREGISTRATION_CANONICAL_SHA256 = (
    "7d12dd62127cb02676d4e18510c06fddc9e2a0afa03ec2f0e758ba6143bed88c"
)
_FREEZE_ADMISSION_FILE_SHA256 = "17e068eacd9b7972b2fb7562dacd7c4a7ad31dc0b0f18159e2d69e4286ec05e2"
_EXPECTED_COUNTERS = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_signal_value_confirmations": 1,
    "independent_replays": 1,
    "data_downloads": 0,
    "cache_mutations": 0,
    "quantconnect_actions": 0,
    "option_backtests": 0,
    "external_provider_actions": 0,
    "orders": 0,
    "fills": 0,
    "positions": 0,
}
_REQUIRED_INPUT_ROLES = (
    "signal_index",
    "real_dq_materialization_receipt",
    "signal_package_manifest_replay_receipt",
    "canonical_prices",
    "canonical_rates",
    "canonical_secondary_prices",
    "canonical_download_manifest",
    "data_quality_policy",
    "us_equity_calendar_policy",
)
_ALLOWED_VERDICTS = ("RETAIN", "REJECT", "INSUFFICIENT")
_COST_RATE = 5.0 / 10_000.0
_INITIAL_CAPITAL = 100_000.0
_EXPECTED_SESSION_COUNT = 1202
_EXPECTED_INTERVAL_COUNT = 1201
_REQUESTED_START = date(2021, 2, 22)
_REQUESTED_END = date(2025, 12, 2)
_RECONCILIATION_TOLERANCE = 1e-8


class FrozenSignalValueConfirmationExecutionError(ValueError):
    """Stable fail-closed execution error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    ).encode("utf-8")


def _sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _require_keys(value: Mapping[str, object], expected: set[str], label: str) -> None:
    actual = set(value)
    if actual != expected:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_SCHEMA_INVALID",
            f"{label} keys differ: missing={sorted(expected - actual)} "
            f"extra={sorted(actual - expected)}",
        )


def _expect(value: object, expected: object, label: str) -> None:
    if value != expected:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_IDENTITY_MISMATCH",
            f"{label}: expected={expected!r} actual={value!r}",
        )


def _bound_file(path_value: str | Path, *, root: Path, label: str) -> Path:
    path = Path(path_value)
    if path.is_absolute() or ".." in path.parts:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_PATH_INVALID", f"{label} must be a repository-relative path"
        )
    resolved_root = root.resolve()
    resolved = (resolved_root / path).resolve(strict=False)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_PATH_INVALID", f"{label} escapes the repository"
        ) from exc
    if not resolved.is_file():
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_INPUT_MISSING", f"{label}: {path.as_posix()}"
        )
    return resolved


@dataclass(frozen=True)
class LoadedRunAuthorization:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class ManifestInputBinding:
    role: str
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class LoadedExecutionManifest:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str
    inputs: tuple[ManifestInputBinding, ...]


@dataclass(frozen=True)
class SignalPlan:
    sessions: tuple[date, ...]
    prices: tuple[float, ...]
    interval_targets: tuple[float, ...]
    long_interval_count: int
    comparator_weight: float
    action_counts: Mapping[str, int]


@dataclass(frozen=True)
class PortfolioMetrics:
    final_value: float
    net_total_return_pct: float
    max_drawdown_magnitude_pct: float
    total_cost_usd: float
    traded_notional_usd: float


def load_run_authorization(
    path: Path = DEFAULT_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedRunAuthorization:
    resolved = _bound_file(path, root=project_root, label="run_authorization")
    raw = resolved.read_bytes()
    payload = _require_mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()),
        "run_authorization",
    )
    _expect(
        payload.get("schema_version"),
        "frozen_signal_value_confirmation_run_authorization.v1",
        "authorization.schema_version",
    )
    _expect(
        payload.get("authorization_id"),
        "frozen_signal_value_confirmation_run_authorization_v1",
        "authorization.authorization_id",
    )
    _expect(payload.get("authorization_version"), "1.0.0", "authorization.authorization_version")
    _expect(payload.get("status"), _AUTHORIZATION_STATUS, "authorization.status")
    _expect(payload.get("task_id"), _TASK_ID, "authorization.task_id")
    _expect(payload.get("scope"), "BOUNDED_DATA_RESEARCH_CONFIRMATION", "authorization.scope")

    owner = _require_mapping(payload.get("owner_decision"), "owner_decision")
    _expect(owner.get("decision_ref"), _AUTHORIZATION_DECISION, "owner_decision.decision_ref")
    _expect(
        owner.get("authorization_state"),
        "EXACT_PREAUTHORIZED",
        "owner_decision.authorization_state",
    )
    _expect(
        owner.get("exact_bounded_run_granted"), True, "owner_decision.exact_bounded_run_granted"
    )

    prereg = _require_mapping(payload.get("preregistration_binding"), "preregistration_binding")
    _expect(
        prereg.get("file_sha256"),
        _PREREGISTRATION_FILE_SHA256,
        "preregistration_binding.file_sha256",
    )
    _expect(
        prereg.get("canonical_sha256"),
        _PREREGISTRATION_CANONICAL_SHA256,
        "preregistration_binding.canonical_sha256",
    )
    freeze = _require_mapping(payload.get("freeze_admission_binding"), "freeze_admission_binding")
    _expect(
        freeze.get("file_sha256"),
        _FREEZE_ADMISSION_FILE_SHA256,
        "freeze_admission_binding.file_sha256",
    )

    envelope = _require_mapping(payload.get("run_envelope"), "run_envelope")
    _require_keys(envelope, set(_EXPECTED_COUNTERS), "run_envelope")
    _expect(envelope, _EXPECTED_COUNTERS, "run_envelope")
    result_boundary = _require_mapping(payload.get("result_boundary"), "result_boundary")
    _expect(
        tuple(result_boundary.get("allowed_verdicts", ())),
        _ALLOWED_VERDICTS,
        "result_boundary.allowed_verdicts",
    )
    _expect(
        result_boundary.get("aggregate_result_only"), True, "result_boundary.aggregate_result_only"
    )
    _expect(
        result_boundary.get("raw_market_payload_export_allowed"),
        False,
        "result_boundary.raw_market_payload_export_allowed",
    )
    _expect(
        result_boundary.get("raw_option_export_allowed"),
        False,
        "result_boundary.raw_option_export_allowed",
    )

    safety = _require_mapping(payload.get("safety"), "safety")
    required_true = {
        "outcome_access_authorized",
        "market_data_read_authorized",
        "manifest_replay_authorized",
        "canonical_dq_authorized",
        "local_signal_value_confirmation_authorized",
        "independent_replay_authorized",
        "local_backtest_authorized_only_as_frozen_confirmation",
    }
    required_false = {
        "data_download_authorized",
        "cache_mutation_authorized",
        "quantconnect_authorized",
        "option_data_use_authorized",
        "option_backtest_authorized",
        "provider_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    }
    for field in required_true:
        _expect(safety.get(field), True, f"safety.{field}")
    for field in required_false:
        _expect(safety.get(field), False, f"safety.{field}")
    _expect(safety.get("production_effect"), "none", "safety.production_effect")
    _expect(safety.get("broker_action"), "none", "safety.broker_action")
    return LoadedRunAuthorization(
        payload=payload,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_json_bytes(payload)),
    )


def load_execution_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    authorization: LoadedRunAuthorization | None = None,
) -> LoadedExecutionManifest:
    loaded_authorization = authorization or load_run_authorization(project_root=project_root)
    resolved = _bound_file(path, root=project_root, label="execution_manifest")
    raw = resolved.read_bytes()
    payload = _require_mapping(json.loads(raw), "execution_manifest")
    _expect(
        payload.get("schema_version"),
        "frozen_signal_value_confirmation_execution_manifest.v1",
        "manifest.schema_version",
    )
    _expect(
        payload.get("manifest_id"),
        "frozen_signal_value_confirmation_execution_manifest_v1",
        "manifest.manifest_id",
    )
    _expect(payload.get("task_id"), _TASK_ID, "manifest.task_id")
    _expect(payload.get("status"), "FROZEN_READY_FOR_SINGLE_DISPATCH", "manifest.status")
    authorization_binding = _require_mapping(
        payload.get("authorization_binding"), "manifest.authorization_binding"
    )
    _expect(
        authorization_binding.get("path"),
        DEFAULT_AUTHORIZATION_PATH.as_posix(),
        "manifest.authorization_binding.path",
    )
    _expect(
        authorization_binding.get("file_sha256"),
        loaded_authorization.file_sha256,
        "manifest.authorization_binding.file_sha256",
    )
    _expect(
        authorization_binding.get("canonical_sha256"),
        loaded_authorization.canonical_sha256,
        "manifest.authorization_binding.canonical_sha256",
    )
    _expect(
        _require_mapping(payload.get("run_envelope"), "manifest.run_envelope"),
        _EXPECTED_COUNTERS,
        "manifest.run_envelope",
    )
    _expect(
        payload.get("requested_start"), _REQUESTED_START.isoformat(), "manifest.requested_start"
    )
    _expect(payload.get("requested_end"), _REQUESTED_END.isoformat(), "manifest.requested_end")
    _expect(
        payload.get("evaluated_start"), _REQUESTED_START.isoformat(), "manifest.evaluated_start"
    )
    _expect(payload.get("evaluated_end"), _REQUESTED_END.isoformat(), "manifest.evaluated_end")
    _expect(
        payload.get("expected_signal_sessions"),
        _EXPECTED_SESSION_COUNT,
        "manifest.expected_signal_sessions",
    )
    _expect(
        payload.get("expected_return_intervals"),
        _EXPECTED_INTERVAL_COUNT,
        "manifest.expected_return_intervals",
    )

    code = _require_mapping(payload.get("code_binding"), "manifest.code_binding")
    module_path = str(code.get("module_path", ""))
    module = _bound_file(module_path, root=project_root, label="manifest.code_binding.module_path")
    _expect(_sha256_path(module), code.get("module_sha256"), "manifest.code_binding.module_sha256")
    implementation_commit = str(code.get("implementation_commit_sha", ""))
    if len(implementation_commit) != 40:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_IDENTITY_MISMATCH", "implementation_commit_sha must be a full Git SHA"
        )

    raw_inputs = payload.get("input_bindings")
    if not isinstance(raw_inputs, list):
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_SCHEMA_INVALID", "manifest.input_bindings must be a list"
        )
    inputs: list[ManifestInputBinding] = []
    for item in raw_inputs:
        binding = _require_mapping(item, "manifest.input_binding")
        _require_keys(binding, {"role", "path", "sha256", "size_bytes"}, "manifest.input_binding")
        role = str(binding["role"])
        sha256 = str(binding["sha256"])
        if len(sha256) != 64:
            raise FrozenSignalValueConfirmationExecutionError(
                "EXECUTION_SCHEMA_INVALID", f"invalid SHA-256 for role={role}"
            )
        inputs.append(
            ManifestInputBinding(
                role=role,
                path=str(binding["path"]),
                sha256=sha256,
                size_bytes=int(binding["size_bytes"]),
            )
        )
    if tuple(item.role for item in inputs) != _REQUIRED_INPUT_ROLES:
        raise FrozenSignalValueConfirmationExecutionError(
            "EXECUTION_SCHEMA_INVALID", "manifest input role order/inventory drifted"
        )
    return LoadedExecutionManifest(
        payload=payload,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_json_bytes(payload)),
        inputs=tuple(inputs),
    )


def replay_execution_manifest(
    manifest: LoadedExecutionManifest,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, Any]:
    observations: list[dict[str, object]] = []
    for binding in manifest.inputs:
        path = _bound_file(binding.path, root=project_root, label=f"input:{binding.role}")
        actual_size = path.stat().st_size
        actual_sha256 = _sha256_path(path)
        if (actual_sha256, actual_size) != (binding.sha256, binding.size_bytes):
            raise FrozenSignalValueConfirmationExecutionError(
                "MANIFEST_REPLAY_INPUT_MISMATCH", binding.role
            )
        observations.append(
            {
                "role": binding.role,
                "path": binding.path,
                "sha256": actual_sha256,
                "size_bytes": actual_size,
                "status": "PASS",
            }
        )
    freeze = (
        freeze_admission.load_frozen_signal_value_confirmation_preregistration_freeze_admission(
            project_root=project_root
        )
    )
    if freeze.file_sha256 != _FREEZE_ADMISSION_FILE_SHA256:
        raise FrozenSignalValueConfirmationExecutionError(
            "MANIFEST_REPLAY_POLICY_MISMATCH", "freeze admission file identity changed"
        )
    if freeze.preregistration.policy_file_sha256 != _PREREGISTRATION_FILE_SHA256:
        raise FrozenSignalValueConfirmationExecutionError(
            "MANIFEST_REPLAY_POLICY_MISMATCH", "preregistration file identity changed"
        )
    return {
        "schema_version": "frozen_signal_value_confirmation_manifest_replay_receipt.v1",
        "status": "PASS",
        "manifest_file_sha256": manifest.file_sha256,
        "manifest_canonical_sha256": manifest.canonical_sha256,
        "preregistration_file_sha256": freeze.preregistration.policy_file_sha256,
        "preregistration_canonical_sha256": freeze.preregistration.policy_canonical_sha256,
        "freeze_admission_file_sha256": freeze.file_sha256,
        "input_observations": observations,
        "production_effect": "none",
        "broker_action": "none",
    }


def _next_xnys_session(value: date) -> date:
    candidate = value + timedelta(days=1)
    while not is_us_equity_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def _binding_by_role(manifest: LoadedExecutionManifest) -> dict[str, ManifestInputBinding]:
    return {binding.role: binding for binding in manifest.inputs}


def load_signal_plan(
    manifest: LoadedExecutionManifest,
    *,
    project_root: Path = PROJECT_ROOT,
) -> SignalPlan:
    bindings = _binding_by_role(manifest)
    index_path = _bound_file(bindings["signal_index"].path, root=project_root, label="signal_index")
    index = _require_mapping(json.loads(index_path.read_bytes()), "signal_index")
    _expect(
        index.get("schema_version"), "qqq_options_signal_index.v1", "signal_index.schema_version"
    )
    artifacts = index.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != _EXPECTED_SESSION_COUNT:
        raise FrozenSignalValueConfirmationExecutionError(
            "SIGNAL_SESSION_COUNT_NOT_1202",
            str(len(artifacts) if isinstance(artifacts, list) else None),
        )
    package_root = index_path.parent
    sessions: list[date] = []
    actions: list[str] = []
    action_counts = {"FLAT": 0, "LONG_CALL": 0}
    for position, raw_artifact in enumerate(artifacts):
        artifact = _require_mapping(raw_artifact, "signal_index.artifact")
        relative = Path(str(artifact.get("relative_path", "")))
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or relative.parts[:1] != ("daily_signals",)
        ):
            raise FrozenSignalValueConfirmationExecutionError(
                "SIGNAL_PACKAGE_OR_INDEX_IDENTITY_MISMATCH", str(relative)
            )
        signal_path = (package_root / relative).resolve(strict=False)
        try:
            signal_path.relative_to(package_root.resolve())
        except ValueError as exc:
            raise FrozenSignalValueConfirmationExecutionError(
                "SIGNAL_PACKAGE_OR_INDEX_IDENTITY_MISMATCH", str(relative)
            ) from exc
        raw = signal_path.read_bytes()
        if _sha256_bytes(raw) != artifact.get("sha256") or len(raw) != artifact.get("byte_count"):
            raise FrozenSignalValueConfirmationExecutionError(
                "SIGNAL_PACKAGE_OR_INDEX_IDENTITY_MISMATCH", str(relative)
            )
        record = _require_mapping(json.loads(raw), "daily_signal")
        session = date.fromisoformat(str(record.get("signal_session")))
        expected_filename = f"daily_signals/{session.isoformat()}.json"
        _expect(relative.as_posix(), expected_filename, "daily_signal.relative_path")
        _expect(
            record.get("requested_start"),
            _REQUESTED_START.isoformat(),
            "daily_signal.requested_start",
        )
        _expect(
            record.get("requested_end"), _REQUESTED_END.isoformat(), "daily_signal.requested_end"
        )
        _expect(
            record.get("evaluated_start"),
            _REQUESTED_START.isoformat(),
            "daily_signal.evaluated_start",
        )
        _expect(
            record.get("evaluated_end"), _REQUESTED_END.isoformat(), "daily_signal.evaluated_end"
        )
        _expect(record.get("dq_status"), "PASS", "daily_signal.dq_status")
        _expect(record.get("pit_status"), "PASS", "daily_signal.pit_status")
        _expect(
            record.get("earliest_effective_session"),
            _next_xnys_session(session).isoformat(),
            "daily_signal.earliest_effective_session",
        )
        action = str(record.get("signal"))
        if action not in action_counts:
            raise FrozenSignalValueConfirmationExecutionError(
                "MISSING_DUPLICATE_UNKNOWN_OR_IMPUTED_INPUT", f"unknown action={action!r}"
            )
        if position and session <= sessions[-1]:
            raise FrozenSignalValueConfirmationExecutionError(
                "MISSING_DUPLICATE_UNKNOWN_OR_IMPUTED_INPUT",
                "signal sessions not strictly increasing",
            )
        sessions.append(session)
        actions.append(action)
        action_counts[action] += 1

    if (sessions[0], sessions[-1]) != (_REQUESTED_START, _REQUESTED_END):
        raise FrozenSignalValueConfirmationExecutionError(
            "REQUESTED_OR_EVALUATED_RANGE_MISMATCH", "signal boundary drifted"
        )
    for left, right in zip(sessions, sessions[1:], strict=False):
        if _next_xnys_session(left) != right:
            raise FrozenSignalValueConfirmationExecutionError(
                "MISSING_DUPLICATE_UNKNOWN_OR_IMPUTED_INPUT", "signal XNYS inventory has a gap"
            )

    prices_path = _bound_file(
        bindings["canonical_prices"].path, root=project_root, label="canonical_prices"
    )
    frame = pd.read_csv(
        prices_path,
        usecols=["date", "ticker", "adj_close"],
        dtype={"date": "string", "ticker": "string", "adj_close": "float64"},
    )
    qqq = frame.loc[frame["ticker"] == "QQQ", ["date", "adj_close"]].copy()
    if len(qqq) != _EXPECTED_SESSION_COUNT or qqq["date"].duplicated().any():
        raise FrozenSignalValueConfirmationExecutionError(
            "MISSING_DUPLICATE_UNKNOWN_OR_IMPUTED_INPUT", "QQQ row count/uniqueness mismatch"
        )
    qqq_sessions = tuple(date.fromisoformat(str(value)) for value in qqq["date"])
    if qqq_sessions != tuple(sessions):
        raise FrozenSignalValueConfirmationExecutionError(
            "REQUESTED_OR_EVALUATED_RANGE_MISMATCH", "QQQ sessions differ from signal sessions"
        )
    prices = tuple(float(value) for value in qqq["adj_close"])
    if any(not math.isfinite(value) or value <= 0.0 for value in prices):
        raise FrozenSignalValueConfirmationExecutionError(
            "MISSING_DUPLICATE_UNKNOWN_OR_IMPUTED_INPUT",
            "QQQ adjusted close is missing/non-positive",
        )

    targets = [0.0] * _EXPECTED_INTERVAL_COUNT
    # No pre-window signal is imported.  Signal i becomes effective on session
    # i+1 and therefore controls the close(i+1)->close(i+2) return interval.
    for signal_position in range(_EXPECTED_SESSION_COUNT - 2):
        targets[signal_position + 1] = 1.0 if actions[signal_position] == "LONG_CALL" else 0.0
    long_count = sum(1 for value in targets if value == 1.0)
    return SignalPlan(
        sessions=tuple(sessions),
        prices=prices,
        interval_targets=tuple(targets),
        long_interval_count=long_count,
        comparator_weight=long_count / _EXPECTED_INTERVAL_COUNT,
        action_counts=action_counts,
    )


def _max_drawdown_magnitude(values: Sequence[float]) -> float:
    peak = values[0]
    maximum = 0.0
    for value in values:
        peak = max(peak, value)
        maximum = max(maximum, 1.0 - value / peak)
    return maximum


def calculate_candidate_primary(
    prices: Sequence[float], targets: Sequence[float]
) -> PortfolioMetrics:
    if len(prices) != len(targets) + 1:
        raise FrozenSignalValueConfirmationExecutionError(
            "RETURN_INTERVAL_COUNT_NOT_1201", "price/target length mismatch"
        )
    cash = _INITIAL_CAPITAL
    shares = 0.0
    position = 0.0
    total_cost = 0.0
    traded = 0.0
    curve = [_INITIAL_CAPITAL]
    for position_index, target in enumerate(targets):
        price = float(prices[position_index])
        if target not in (0.0, 1.0):
            raise FrozenSignalValueConfirmationExecutionError(
                "ACCOUNTING_OR_COST_RECONCILIATION_NOT_PASS", "candidate target is not binary"
            )
        if target != position:
            if target == 1.0:
                notional = cash / (1.0 + _COST_RATE)
                cost = notional * _COST_RATE
                shares = notional / price
                cash -= notional + cost
            else:
                notional = shares * price
                cost = notional * _COST_RATE
                cash += notional - cost
                shares = 0.0
            total_cost += cost
            traded += notional
            position = target
        if cash < -_RECONCILIATION_TOLERANCE or shares < -_RECONCILIATION_TOLERANCE:
            raise FrozenSignalValueConfirmationExecutionError(
                "ACCOUNTING_OR_COST_RECONCILIATION_NOT_PASS", "negative cash or shares"
            )
        curve.append(cash + shares * price)
        curve.append(cash + shares * float(prices[position_index + 1]))
    if shares:
        terminal_notional = shares * float(prices[-1])
        terminal_cost = terminal_notional * _COST_RATE
        cash += terminal_notional - terminal_cost
        traded += terminal_notional
        total_cost += terminal_cost
        shares = 0.0
        curve.append(cash)
    final_value = cash
    return PortfolioMetrics(
        final_value=final_value,
        net_total_return_pct=(final_value / _INITIAL_CAPITAL - 1.0) * 100.0,
        max_drawdown_magnitude_pct=_max_drawdown_magnitude(curve) * 100.0,
        total_cost_usd=total_cost,
        traded_notional_usd=traded,
    )


def calculate_static_comparator_primary(
    prices: Sequence[float], target_weight: float
) -> PortfolioMetrics:
    if not 0.0 <= target_weight <= 1.0 or len(prices) < 2:
        raise FrozenSignalValueConfirmationExecutionError(
            "ACCOUNTING_OR_COST_RECONCILIATION_NOT_PASS", "invalid comparator input"
        )
    post_cost_equity = _INITIAL_CAPITAL / (1.0 + _COST_RATE * target_weight)
    initial_notional = target_weight * post_cost_equity
    initial_cost = initial_notional * _COST_RATE
    cash = (1.0 - target_weight) * post_cost_equity
    shares = initial_notional / float(prices[0])
    curve = [_INITIAL_CAPITAL, post_cost_equity]
    curve.extend(cash + shares * float(price) for price in prices[1:])
    terminal_notional = shares * float(prices[-1])
    terminal_cost = terminal_notional * _COST_RATE
    final_value = cash + terminal_notional - terminal_cost
    curve.append(final_value)
    return PortfolioMetrics(
        final_value=final_value,
        net_total_return_pct=(final_value / _INITIAL_CAPITAL - 1.0) * 100.0,
        max_drawdown_magnitude_pct=_max_drawdown_magnitude(curve) * 100.0,
        total_cost_usd=initial_cost + terminal_cost,
        traded_notional_usd=initial_notional + terminal_notional,
    )


def calculate_independent_replay(
    prices: Sequence[float], targets: Sequence[float], comparator_weight: float
) -> Mapping[str, float]:
    wealth = _INITIAL_CAPITAL
    position = 0.0
    candidate_curve = [wealth]
    for index, target in enumerate(targets):
        if target != position:
            wealth *= 1.0 / (1.0 + _COST_RATE) if target == 1.0 else 1.0 - _COST_RATE
            position = target
        candidate_curve.append(wealth)
        if position == 1.0:
            wealth *= float(prices[index + 1]) / float(prices[index])
        candidate_curve.append(wealth)
    if position == 1.0:
        wealth *= 1.0 - _COST_RATE
        candidate_curve.append(wealth)

    comparator_post = _INITIAL_CAPITAL / (1.0 + _COST_RATE * comparator_weight)
    comparator_qqq = comparator_weight * comparator_post
    comparator_cash = (1.0 - comparator_weight) * comparator_post
    comparator_curve = [_INITIAL_CAPITAL, comparator_post]
    for price in prices[1:]:
        comparator_curve.append(comparator_cash + comparator_qqq * float(price) / float(prices[0]))
    comparator_final_before_sale = comparator_curve[-1]
    comparator_terminal_notional = comparator_qqq * float(prices[-1]) / float(prices[0])
    comparator_final = comparator_final_before_sale - _COST_RATE * comparator_terminal_notional
    comparator_curve.append(comparator_final)
    return {
        "candidate_final_value": wealth,
        "candidate_net_total_return_pct": (wealth / _INITIAL_CAPITAL - 1.0) * 100.0,
        "candidate_max_drawdown_magnitude_pct": _max_drawdown_magnitude(candidate_curve) * 100.0,
        "comparator_final_value": comparator_final,
        "comparator_net_total_return_pct": (comparator_final / _INITIAL_CAPITAL - 1.0) * 100.0,
        "comparator_max_drawdown_magnitude_pct": _max_drawdown_magnitude(comparator_curve) * 100.0,
    }


def reduce_verdict(
    *,
    gates_passed: bool,
    primary_metric_pp: float | None,
    drawdown_delta_pp: float | None,
) -> Literal["RETAIN", "REJECT", "INSUFFICIENT"]:
    if not gates_passed or primary_metric_pp is None or drawdown_delta_pp is None:
        return "INSUFFICIENT"
    if primary_metric_pp <= 0.0 or drawdown_delta_pp > 0.0:
        return "REJECT"
    return "RETAIN"


def _year_concentration(plan: SignalPlan) -> list[dict[str, object]]:
    buckets: dict[int, list[float]] = {}
    for session, target in zip(plan.sessions[:-1], plan.interval_targets, strict=True):
        buckets.setdefault(session.year, []).append(target)
    return [
        {
            "year": year,
            "return_interval_count": len(values),
            "long_interval_count": sum(1 for value in values if value == 1.0),
            "long_exposure_fraction": sum(values) / len(values),
        }
        for year, values in sorted(buckets.items())
    ]


def _write_once(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = _canonical_json_bytes(payload)
    try:
        with path.open("xb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FrozenSignalValueConfirmationExecutionError(
            "RUN_ATTEMPT_ALREADY_CONSUMED", path.as_posix()
        ) from exc


def _git_head(project_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _aggregate_failure_result(
    *,
    manifest_file_sha256: str | None,
    counters: Mapping[str, int],
    code: str,
    message: str,
    git_head: str,
) -> Mapping[str, object]:
    return {
        "schema_version": "frozen_signal_value_confirmation_result.v1",
        "task_id": _TASK_ID,
        "status": "TERMINAL",
        "verdict": "INSUFFICIENT",
        "stop_action": "COLLECT_ONLY_EXPLICITLY_IDENTIFIED_PROSPECTIVE_EVIDENCE",
        "requested_range": {
            "start": _REQUESTED_START.isoformat(),
            "end": _REQUESTED_END.isoformat(),
        },
        "evaluated_range": None,
        "manifest_file_sha256": manifest_file_sha256,
        "runtime_git_head": git_head,
        "gate_status": "FAIL_CLOSED",
        "failure": {"code": code, "message": message},
        "actual_counters": dict(counters),
        "aggregate_result_only": True,
        "raw_market_payload_exported": False,
        "raw_option_payload_exported": False,
        "production_effect": "none",
        "broker_action": "none",
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def execute_bounded_confirmation(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, object]:
    root = project_root.resolve()
    output_dir = (root / manifest_path).resolve().parent
    attempt_path = output_dir / "run_attempt_consumption_receipt.json"
    result_path = output_dir / "aggregate_result.json"
    if attempt_path.exists() or result_path.exists():
        raise FrozenSignalValueConfirmationExecutionError(
            "RUN_ATTEMPT_ALREADY_CONSUMED", output_dir.as_posix()
        )
    counters = {key: 0 for key in _EXPECTED_COUNTERS}
    git_head = _git_head(root)
    attempt = {
        "schema_version": "frozen_signal_value_confirmation_run_attempt_consumption.v1",
        "task_id": _TASK_ID,
        "status": "DISPATCHED_SINGLE_ATTEMPT_RESERVED",
        "authorized_maxima": _EXPECTED_COUNTERS,
        "runtime_git_head": git_head,
        "data_downloads": 0,
        "cache_mutations": 0,
        "quantconnect_actions": 0,
        "option_backtests": 0,
        "external_provider_actions": 0,
        "orders": 0,
        "fills": 0,
        "positions": 0,
        "production_effect": "none",
        "broker_action": "none",
    }
    _write_once(attempt_path, attempt)
    manifest: LoadedExecutionManifest | None = None
    try:
        authorization = load_run_authorization(project_root=root)
        manifest = load_execution_manifest(
            manifest_path, project_root=root, authorization=authorization
        )
        counters["manifest_replays"] = 1
        manifest_receipt = replay_execution_manifest(manifest, project_root=root)
        _write_once(output_dir / "manifest_replay_receipt.json", manifest_receipt)

        bindings = _binding_by_role(manifest)
        counters["canonical_dq_runs"] = 1
        dq = run_canonical_data_quality_execution(
            CanonicalDataQualityExecutionRequest(
                as_of=_REQUESTED_END,
                requested_window=DataQualityDateWindow(start=_REQUESTED_START, end=_REQUESTED_END),
                evaluated_window=DataQualityDateWindow(start=_REQUESTED_START, end=_REQUESTED_END),
                prices_path=Path(bindings["canonical_prices"].path),
                rates_path=Path(bindings["canonical_rates"].path),
                manifest_path=Path(bindings["canonical_download_manifest"].path),
                secondary_prices_path=Path(bindings["canonical_secondary_prices"].path),
                require_secondary_prices=True,
                expected_price_tickers=("QQQ", "SGOV", "TQQQ"),
                expected_rate_series=("DGS10", "DGS2", "DTWEXBGS"),
                policy_path=Path(bindings["data_quality_policy"].path),
            ),
            project_root=root,
        )
        dq_receipt = {
            "schema_version": "frozen_signal_value_confirmation_dq_receipt.v1",
            "status": dq.report.status,
            "canonical_dq_receipt_path": dq.receipt_path.relative_to(root).as_posix(),
            "canonical_dq_receipt_sha256": _sha256_path(dq.receipt_path),
            "canonical_dq_report_path": dq.report_path.relative_to(root).as_posix(),
            "canonical_dq_report_sha256": _sha256_path(dq.report_path),
            "requested_start": _REQUESTED_START.isoformat(),
            "requested_end": _REQUESTED_END.isoformat(),
            "evaluated_start": dq.receipt.evaluated_window.start.isoformat(),
            "evaluated_end": dq.receipt.evaluated_window.end.isoformat(),
            "error_count": dq.report.error_count,
            "warning_count": dq.report.warning_count,
            "production_effect": "none",
            "broker_action": "none",
        }
        _write_once(output_dir / "canonical_dq_receipt.json", dq_receipt)
        if dq.report.status != "PASS":
            raise FrozenSignalValueConfirmationExecutionError(
                "CANONICAL_DQ_PIT_NOT_PASS", dq.report.status
            )

        counters["local_signal_value_confirmations"] = 1
        plan = load_signal_plan(manifest, project_root=root)
        candidate = calculate_candidate_primary(plan.prices, plan.interval_targets)
        comparator = calculate_static_comparator_primary(plan.prices, plan.comparator_weight)
        buy_hold = calculate_static_comparator_primary(plan.prices, 1.0)
        primary_metric = candidate.net_total_return_pct - comparator.net_total_return_pct
        drawdown_delta = (
            candidate.max_drawdown_magnitude_pct - comparator.max_drawdown_magnitude_pct
        )

        counters["independent_replays"] = 1
        independent = calculate_independent_replay(
            plan.prices, plan.interval_targets, plan.comparator_weight
        )
        reconciliation = {
            "candidate_final_value_abs_diff": abs(
                candidate.final_value - independent["candidate_final_value"]
            ),
            "candidate_drawdown_abs_diff_pp": abs(
                candidate.max_drawdown_magnitude_pct
                - independent["candidate_max_drawdown_magnitude_pct"]
            ),
            "comparator_final_value_abs_diff": abs(
                comparator.final_value - independent["comparator_final_value"]
            ),
            "comparator_drawdown_abs_diff_pp": abs(
                comparator.max_drawdown_magnitude_pct
                - independent["comparator_max_drawdown_magnitude_pct"]
            ),
        }
        if any(value > _RECONCILIATION_TOLERANCE for value in reconciliation.values()):
            raise FrozenSignalValueConfirmationExecutionError(
                "INDEPENDENT_REPLAY_NOT_PASS", json.dumps(reconciliation, sort_keys=True)
            )
        independent_receipt = {
            "schema_version": "frozen_signal_value_confirmation_independent_replay.v1",
            "status": "PASS",
            "tolerance": _RECONCILIATION_TOLERANCE,
            "reconciliation": reconciliation,
            "production_effect": "none",
            "broker_action": "none",
        }
        _write_once(output_dir / "independent_replay_receipt.json", independent_receipt)
        verdict = reduce_verdict(
            gates_passed=True,
            primary_metric_pp=primary_metric,
            drawdown_delta_pp=drawdown_delta,
        )
        stop_actions = {
            "RETAIN": "OPEN_OWNER_REVIEW_FOR_CONDITIONAL_OPTIONS_PAIRED_COMPARISON",
            "REJECT": "CLOSE_OPTIONS_IMPLEMENTATION_P0_ROUTE_NO_PARAMETER_RESCUE",
            "INSUFFICIENT": "COLLECT_ONLY_EXPLICITLY_IDENTIFIED_PROSPECTIVE_EVIDENCE",
        }
        result: Mapping[str, object] = {
            "schema_version": "frozen_signal_value_confirmation_result.v1",
            "task_id": _TASK_ID,
            "status": "TERMINAL",
            "verdict": verdict,
            "stop_action": stop_actions[verdict],
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "requested_range": {
                "start": _REQUESTED_START.isoformat(),
                "end": _REQUESTED_END.isoformat(),
            },
            "evaluated_range": {
                "start": plan.sessions[0].isoformat(),
                "end": plan.sessions[-1].isoformat(),
            },
            "signal_session_count": len(plan.sessions),
            "return_interval_count": len(plan.interval_targets),
            "first_interval_policy": "ZERO_RETURN_CASH_NO_PREWINDOW_SIGNAL_IMPORT",
            "long_interval_count": plan.long_interval_count,
            "exposure_matched_comparator_weight": plan.comparator_weight,
            "action_counts": dict(plan.action_counts),
            "candidate": {
                "final_value_usd": candidate.final_value,
                "net_total_return_pct": candidate.net_total_return_pct,
                "max_drawdown_magnitude_pct": candidate.max_drawdown_magnitude_pct,
                "total_cost_usd": candidate.total_cost_usd,
                "traded_notional_usd": candidate.traded_notional_usd,
            },
            "primary_comparator": {
                "final_value_usd": comparator.final_value,
                "net_total_return_pct": comparator.net_total_return_pct,
                "max_drawdown_magnitude_pct": comparator.max_drawdown_magnitude_pct,
                "total_cost_usd": comparator.total_cost_usd,
                "traded_notional_usd": comparator.traded_notional_usd,
            },
            "primary_estimand": {
                "net_total_return_difference_percentage_points": primary_metric,
                "retain_threshold_strictly_greater_than": 0.0,
            },
            "falsification_guard": {
                "max_drawdown_magnitude_delta_percentage_points": drawdown_delta,
                "retain_threshold_less_than_or_equal_to": 0.0,
            },
            "diagnostics": {
                "qqq_buy_and_hold_net_total_return_pct": buy_hold.net_total_return_pct,
                "calendar_year_exposure_concentration": _year_concentration(plan),
                "pre_2023_long_interval_count": sum(
                    1
                    for session, target in zip(
                        plan.sessions[:-1], plan.interval_targets, strict=True
                    )
                    if session.year < 2023 and target == 1.0
                ),
                "post_2022_long_interval_count": sum(
                    1
                    for session, target in zip(
                        plan.sessions[:-1], plan.interval_targets, strict=True
                    )
                    if session.year >= 2023 and target == 1.0
                ),
            },
            "gate_status": "PASS",
            "manifest_file_sha256": manifest.file_sha256,
            "manifest_canonical_sha256": manifest.canonical_sha256,
            "authorization_file_sha256": authorization.file_sha256,
            "authorization_canonical_sha256": authorization.canonical_sha256,
            "canonical_dq": dq_receipt,
            "independent_replay": independent_receipt,
            "runtime_git_head": git_head,
            "actual_counters": counters,
            "aggregate_result_only": True,
            "raw_market_payload_exported": False,
            "raw_option_payload_exported": False,
            "data_downloads": 0,
            "cache_mutations": 0,
            "quantconnect_actions": 0,
            "option_backtests": 0,
            "external_provider_actions": 0,
            "production_effect": "none",
            "broker_action": "none",
            "orders": 0,
            "fills": 0,
            "positions": 0,
        }
        _write_once(result_path, result)
        return result
    except Exception as exc:
        if isinstance(exc, FrozenSignalValueConfirmationExecutionError):
            code, message = exc.code, exc.message
        else:
            code, message = "UNEXPECTED_CONFIRMATION_FAILURE", str(exc)
        failure = _aggregate_failure_result(
            manifest_file_sha256=None if manifest is None else manifest.file_sha256,
            counters=counters,
            code=code,
            message=message,
            git_head=git_head,
        )
        _write_once(output_dir / "failure_receipt.json", failure)
        _write_once(result_path, failure)
        return failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute_bounded_confirmation(args.manifest, project_root=args.project_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("verdict") in _ALLOWED_VERDICTS else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "DEFAULT_AUTHORIZATION_PATH",
    "DEFAULT_MANIFEST_PATH",
    "FrozenSignalValueConfirmationExecutionError",
    "LoadedExecutionManifest",
    "LoadedRunAuthorization",
    "ManifestInputBinding",
    "PortfolioMetrics",
    "SignalPlan",
    "calculate_candidate_primary",
    "calculate_independent_replay",
    "calculate_static_comparator_primary",
    "execute_bounded_confirmation",
    "load_execution_manifest",
    "load_run_authorization",
    "load_signal_plan",
    "reduce_verdict",
    "replay_execution_manifest",
]
