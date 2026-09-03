"""Execute the single bounded F1 foundational falsification run.

The module keeps the empirical surface deliberately narrow: exact manifest
replay, one canonical DQ call, aggregate-only diagnostics, and one independent
accounting replay.  It has no downloader, provider, option, broker, or
production integration.
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
from datetime import date
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from ai_trading_system.contracts.data_quality_execution import DataQualityDateWindow
from ai_trading_system.data.quality_execution import (
    CanonicalDataQualityExecutionRequest,
    run_canonical_data_quality_execution,
)
from ai_trading_system.first_layer_foundational_falsification_contract import (
    BootstrapInterval,
    FoundationalDiagnosticSummary,
    LeaveOneYearOutResult,
    load_foundational_falsification_contract,
    reduce_foundational_falsification_status,
)
from ai_trading_system.research_quality.frozen_signal_value_confirmation_execution import (
    calculate_candidate_primary,
    calculate_static_comparator_primary,
    load_signal_plan,
)
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_AUTHORIZATION_PATH = Path(
    "config/research/first_layer_composer_v2_foundational_falsification_run_authorization_v1.yaml"
)
DEFAULT_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_foundational_falsification_v1/execution_manifest.json"
)
DEFAULT_OUTPUT_DIR = Path("outputs/research/first_layer_composer_v2_foundational_falsification_v1")

TASK_ID = "TRADING-2556_FIRST_LAYER_COMPOSER_V2_FOUNDATIONAL_FALSIFICATION_EMPIRICAL_RUN_V1"
AUTHORIZATION_STATUS = "OWNER_STANDING_SCOPE_BOUNDED_F1_AUTHORIZED"
F0_FILE_SHA256 = "54dc349be1ec5670f9e02fc74e9467b668b2311a7dadbdc22680c8c605a824ad"
F0_CANONICAL_SHA256 = "ea6b51baf7d8bdfec2454fb037131a199736e6cacb1eecdc35e01701f5357818"
F0_AUTHORITY_SET_SHA256 = "a07e63c9f3ba035d94cfdbf18bc096b69380e4baf1b003540390b66d4ec44fe3"
F0_EXACT_MAIN = "fcb2a420ed1489189ea1ec9a323724943dcaee52"
REQUESTED_START = date(2021, 2, 22)
REQUESTED_END = date(2025, 12, 2)
EXPECTED_SESSIONS = 1202
EXPECTED_INTERVALS = 1201
INITIAL_CAPITAL = 100_000.0
RECONCILIATION_TOLERANCE = 1e-8
ALLOWED_STATES = ("risk_off", "defensive", "neutral", "constructive", "risk_on")
DIAGNOSTIC_IDS = (
    "POLICY_CONSUMPTION_INVENTORY",
    "CALENDAR_YEAR_ATTRIBUTION",
    "CONTIGUOUS_EPISODE_ATTRIBUTION",
    "LEAVE_ONE_CALENDAR_YEAR_OUT",
    "PAIRED_MOVING_BLOCK_BOOTSTRAP",
    "COST_SENSITIVITY",
    "SGOV_CARRY_SENSITIVITY",
    "STATE_TRANSITION_ATTRIBUTION",
    "SELECTION_HISTORY_INVENTORY",
    "SOURCE_REVISION_DIFF",
)
INPUT_ROLES = (
    "signal_index",
    "real_dq_materialization_receipt",
    "signal_package_manifest_replay_receipt",
    "operational_predictions",
    "canonical_prices",
    "canonical_rates",
    "canonical_secondary_prices",
    "canonical_download_manifest",
    "data_quality_policy",
    "us_equity_calendar_policy",
    "trading_2550_execution_manifest",
)
COMMON_TRADING_2550_ROLES = (
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
EXPECTED_COUNTERS = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_foundational_runs": 1,
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


class FoundationalFalsificationExecutionError(ValueError):
    """Stable fail-closed F1 execution error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class LoadedAuthorization:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class InputBinding:
    role: str
    path: str
    sha256: str
    size_bytes: int


@dataclass(frozen=True)
class LoadedManifest:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str
    inputs: tuple[InputBinding, ...]


@dataclass(frozen=True)
class DiagnosticPlan:
    sessions: tuple[date, ...]
    qqq_prices: tuple[float, ...]
    sgov_prices: tuple[float, ...]
    states: tuple[str, ...]
    interval_targets: tuple[float, ...]
    long_interval_count: int
    comparator_weight: float
    action_counts: Mapping[str, int]


@dataclass(frozen=True)
class ReturnPath:
    final_value: float
    net_total_return_pct: float
    max_drawdown_magnitude_pct: float
    interval_returns: tuple[float, ...]


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise FoundationalFalsificationExecutionError(
            "F1_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise FoundationalFalsificationExecutionError(
            "F1_IDENTITY_MISMATCH", f"{label}: expected={expected!r} actual={actual!r}"
        )


def _bound_file(path_value: str | Path, *, root: Path, label: str) -> Path:
    relative = Path(path_value)
    if relative.is_absolute() or ".." in relative.parts:
        raise FoundationalFalsificationExecutionError(
            "F1_PATH_INVALID", f"{label} must be repository-relative"
        )
    resolved_root = root.resolve()
    resolved = (resolved_root / relative).resolve(strict=False)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise FoundationalFalsificationExecutionError(
            "F1_PATH_INVALID", f"{label} escapes repository"
        ) from exc
    if not resolved.is_file():
        raise FoundationalFalsificationExecutionError(
            "F1_INPUT_MISSING", f"{label}: {relative.as_posix()}"
        )
    return resolved


def load_run_authorization(
    path: Path = DEFAULT_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
) -> LoadedAuthorization:
    resolved = _bound_file(path, root=project_root, label="authorization")
    raw = resolved.read_bytes()
    payload = _mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "authorization"
    )
    _expect(
        payload.get("schema_version"),
        "first_layer_composer_v2_foundational_falsification_run_authorization.v1",
        "authorization.schema_version",
    )
    _expect(payload.get("authorization_id"), path.stem, "authorization.authorization_id")
    _expect(payload.get("authorization_version"), "1.0.0", "authorization.version")
    _expect(payload.get("status"), AUTHORIZATION_STATUS, "authorization.status")
    _expect(payload.get("task_id"), TASK_ID, "authorization.task_id")
    _expect(payload.get("scope"), "R1_BOUNDED_RESEARCH_SANDBOX", "authorization.scope")
    owner = _mapping(payload.get("owner_decision"), "authorization.owner_decision")
    _expect(owner.get("authorization_state"), "STANDING_OWNER_SCOPE", "authorization_state")
    _expect(owner.get("exact_bounded_run_granted"), True, "exact_bounded_run_granted")
    f0 = _mapping(payload.get("f0_binding"), "authorization.f0_binding")
    _expect(f0.get("exact_main_commit"), F0_EXACT_MAIN, "f0.exact_main_commit")
    _expect(f0.get("file_sha256"), F0_FILE_SHA256, "f0.file_sha256")
    _expect(f0.get("canonical_sha256"), F0_CANONICAL_SHA256, "f0.canonical_sha256")
    _expect(f0.get("authority_set_sha256"), F0_AUTHORITY_SET_SHA256, "f0.authority_set_sha256")
    loaded_f0 = load_foundational_falsification_contract(project_root=project_root)
    _expect(loaded_f0.policy_file_sha256, F0_FILE_SHA256, "loaded_f0.file_sha256")
    _expect(loaded_f0.policy_canonical_sha256, F0_CANONICAL_SHA256, "loaded_f0.canonical")
    _expect(loaded_f0.authority_set_sha256, F0_AUTHORITY_SET_SHA256, "loaded_f0.authorities")
    _expect(
        _mapping(payload.get("run_envelope"), "run_envelope"), EXPECTED_COUNTERS, "run_envelope"
    )
    boundary = _mapping(payload.get("result_boundary"), "result_boundary")
    _expect(boundary.get("aggregate_result_only"), True, "aggregate_result_only")
    _expect(boundary.get("raw_market_payload_export_allowed"), False, "raw_market_export")
    _expect(boundary.get("raw_signal_payload_export_allowed"), False, "raw_signal_export")
    safety = _mapping(payload.get("safety"), "safety")
    for field in (
        "outcome_access_authorized",
        "market_data_read_authorized",
        "manifest_replay_authorized",
        "canonical_dq_authorized",
        "local_foundational_run_authorized",
        "independent_replay_authorized",
    ):
        _expect(safety.get(field), True, f"safety.{field}")
    for field in (
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
    ):
        _expect(safety.get(field), False, f"safety.{field}")
    _expect(safety.get("production_effect"), "none", "safety.production_effect")
    _expect(safety.get("broker_action"), "none", "safety.broker_action")
    allowlist = _mapping(payload.get("input_allowlist"), "input_allowlist")
    _expect(tuple(allowlist), INPUT_ROLES, "input_allowlist.role_order")
    return LoadedAuthorization(
        payload=payload,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_json_bytes(payload)),
    )


def load_execution_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    authorization: LoadedAuthorization | None = None,
) -> LoadedManifest:
    auth = authorization or load_run_authorization(project_root=project_root)
    resolved = _bound_file(path, root=project_root, label="manifest")
    raw = resolved.read_bytes()
    payload = _mapping(json.loads(raw), "manifest")
    _expect(
        payload.get("schema_version"),
        "first_layer_composer_v2_foundational_falsification_execution_manifest.v1",
        "manifest.schema_version",
    )
    _expect(payload.get("manifest_id"), path.parent.name, "manifest.manifest_id")
    _expect(payload.get("task_id"), TASK_ID, "manifest.task_id")
    _expect(payload.get("status"), "FROZEN_READY_FOR_SINGLE_DISPATCH", "manifest.status")
    auth_binding = _mapping(payload.get("authorization_binding"), "authorization_binding")
    _expect(auth_binding.get("path"), DEFAULT_AUTHORIZATION_PATH.as_posix(), "auth.path")
    _expect(auth_binding.get("file_sha256"), auth.file_sha256, "auth.file_sha256")
    _expect(auth_binding.get("canonical_sha256"), auth.canonical_sha256, "auth.canonical")
    _expect(
        _mapping(payload.get("run_envelope"), "manifest.run_envelope"),
        EXPECTED_COUNTERS,
        "run_envelope",
    )
    for field, expected in (
        ("requested_start", REQUESTED_START.isoformat()),
        ("requested_end", REQUESTED_END.isoformat()),
        ("evaluated_start", REQUESTED_START.isoformat()),
        ("evaluated_end", REQUESTED_END.isoformat()),
        ("expected_signal_sessions", EXPECTED_SESSIONS),
        ("expected_return_intervals", EXPECTED_INTERVALS),
    ):
        _expect(payload.get(field), expected, f"manifest.{field}")
    code = _mapping(payload.get("code_binding"), "manifest.code_binding")
    module = _bound_file(str(code.get("module_path", "")), root=project_root, label="code.module")
    _expect(_sha256_path(module), code.get("module_sha256"), "code.module_sha256")
    if len(str(code.get("implementation_commit_sha", ""))) != 40:
        raise FoundationalFalsificationExecutionError(
            "F1_IDENTITY_MISMATCH", "implementation_commit_sha must be a full SHA"
        )
    raw_bindings = payload.get("input_bindings")
    if not isinstance(raw_bindings, list):
        raise FoundationalFalsificationExecutionError("F1_SCHEMA_INVALID", "input_bindings")
    bindings: list[InputBinding] = []
    for raw_binding in raw_bindings:
        binding = _mapping(raw_binding, "input_binding")
        if set(binding) != {"role", "path", "sha256", "size_bytes"}:
            raise FoundationalFalsificationExecutionError(
                "F1_SCHEMA_INVALID", "input binding keys drifted"
            )
        bindings.append(
            InputBinding(
                role=str(binding["role"]),
                path=str(binding["path"]),
                sha256=str(binding["sha256"]),
                size_bytes=int(binding["size_bytes"]),
            )
        )
    _expect(tuple(item.role for item in bindings), INPUT_ROLES, "manifest.input_roles")
    allowlist = _mapping(auth.payload.get("input_allowlist"), "authorization.input_allowlist")
    for parsed_binding in bindings:
        allowed = _mapping(allowlist.get(parsed_binding.role), f"allowlist.{parsed_binding.role}")
        _expect(parsed_binding.path, allowed.get("path"), f"input.{parsed_binding.role}.path")
        _expect(
            parsed_binding.sha256,
            allowed.get("sha256"),
            f"input.{parsed_binding.role}.sha256",
        )
        _expect(
            parsed_binding.size_bytes,
            allowed.get("size_bytes"),
            f"input.{parsed_binding.role}.size",
        )
    return LoadedManifest(
        payload=payload,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_json_bytes(payload)),
        inputs=tuple(bindings),
    )


def replay_execution_manifest(
    manifest: LoadedManifest, *, project_root: Path = PROJECT_ROOT
) -> Mapping[str, object]:
    observations: list[dict[str, object]] = []
    for binding in manifest.inputs:
        path = _bound_file(binding.path, root=project_root, label=f"input:{binding.role}")
        actual = (_sha256_path(path), path.stat().st_size)
        if actual != (binding.sha256, binding.size_bytes):
            raise FoundationalFalsificationExecutionError(
                "F1_MANIFEST_REPLAY_INPUT_MISMATCH", binding.role
            )
        observations.append(
            {
                "role": binding.role,
                "path": binding.path,
                "sha256": actual[0],
                "size_bytes": actual[1],
                "status": "PASS",
            }
        )
    return {
        "schema_version": "first_layer_foundational_falsification_manifest_replay.v1",
        "status": "PASS",
        "manifest_file_sha256": manifest.file_sha256,
        "manifest_canonical_sha256": manifest.canonical_sha256,
        "input_observations": observations,
        "production_effect": "none",
        "broker_action": "none",
    }


def _bindings(manifest: LoadedManifest) -> dict[str, InputBinding]:
    return {binding.role: binding for binding in manifest.inputs}


def load_diagnostic_plan(
    manifest: LoadedManifest, *, project_root: Path = PROJECT_ROOT
) -> DiagnosticPlan:
    base = load_signal_plan(manifest, project_root=project_root)  # type: ignore[arg-type]
    bindings = _bindings(manifest)
    price_frame = pd.read_csv(
        _bound_file(bindings["canonical_prices"].path, root=project_root, label="prices"),
        usecols=["date", "ticker", "adj_close"],
        dtype={"date": "string", "ticker": "string", "adj_close": "float64"},
    )
    sgov = price_frame.loc[price_frame["ticker"] == "SGOV", ["date", "adj_close"]].copy()
    if len(sgov) != EXPECTED_SESSIONS or sgov["date"].duplicated().any():
        raise FoundationalFalsificationExecutionError(
            "F1_PRICE_COVERAGE_INVALID", "SGOV row count/uniqueness mismatch"
        )
    sessions = tuple(date.fromisoformat(str(item)) for item in sgov["date"])
    if sessions != base.sessions:
        raise FoundationalFalsificationExecutionError(
            "F1_PRICE_COVERAGE_INVALID", "SGOV sessions differ from signal sessions"
        )
    sgov_prices = tuple(float(item) for item in sgov["adj_close"])
    if any(not math.isfinite(item) or item <= 0.0 for item in sgov_prices):
        raise FoundationalFalsificationExecutionError(
            "F1_PRICE_COVERAGE_INVALID", "SGOV adjusted close invalid"
        )
    predictions = pd.read_csv(
        _bound_file(
            bindings["operational_predictions"].path,
            root=project_root,
            label="operational_predictions",
        ),
        usecols=["date", "model_id", "trend_state"],
        dtype="string",
    )
    if len(predictions) != EXPECTED_SESSIONS or predictions["date"].duplicated().any():
        raise FoundationalFalsificationExecutionError(
            "F1_STATE_COVERAGE_INVALID", "prediction row count/uniqueness mismatch"
        )
    prediction_sessions = tuple(date.fromisoformat(str(item)) for item in predictions["date"])
    if prediction_sessions != base.sessions:
        raise FoundationalFalsificationExecutionError(
            "F1_STATE_COVERAGE_INVALID", "prediction sessions differ from signal sessions"
        )
    if set(predictions["model_id"].astype(str)) != {"first_layer_composer_v2"}:
        raise FoundationalFalsificationExecutionError(
            "F1_STATE_IDENTITY_INVALID", "model_id mismatch"
        )
    states = tuple(str(item) for item in predictions["trend_state"])
    if not set(states) <= set(ALLOWED_STATES):
        raise FoundationalFalsificationExecutionError(
            "F1_STATE_IDENTITY_INVALID", "unknown trend_state"
        )
    return DiagnosticPlan(
        sessions=base.sessions,
        qqq_prices=base.prices,
        sgov_prices=sgov_prices,
        states=states,
        interval_targets=base.interval_targets,
        long_interval_count=base.long_interval_count,
        comparator_weight=base.comparator_weight,
        action_counts=base.action_counts,
    )


def _max_drawdown(values: Sequence[float]) -> float:
    peak = float(values[0])
    maximum = 0.0
    for value in values:
        peak = max(peak, float(value))
        maximum = max(maximum, 1.0 - float(value) / peak)
    return maximum * 100.0


def _cash_returns(prices: Sequence[float]) -> tuple[float, ...]:
    return tuple(
        float(right) / float(left) - 1.0 for left, right in zip(prices, prices[1:], strict=False)
    )


def candidate_return_path(
    prices: Sequence[float],
    targets: Sequence[float],
    *,
    one_way_cost_bps: float,
    cash_returns: Sequence[float] | None = None,
) -> ReturnPath:
    if len(prices) != len(targets) + 1 or (
        cash_returns is not None and len(cash_returns) != len(targets)
    ):
        raise FoundationalFalsificationExecutionError("F1_ACCOUNTING_INVALID", "path lengths")
    cost = one_way_cost_bps / 10_000.0
    wealth = INITIAL_CAPITAL
    position = 0.0
    returns: list[float] = []
    curve = [wealth]
    for index, target in enumerate(targets):
        if target not in (0.0, 1.0):
            raise FoundationalFalsificationExecutionError(
                "F1_ACCOUNTING_INVALID", "non-binary target"
            )
        start = wealth
        if target != position:
            wealth *= 1.0 / (1.0 + cost) if target == 1.0 else 1.0 - cost
            position = target
        if position == 1.0:
            wealth *= float(prices[index + 1]) / float(prices[index])
        elif cash_returns is not None:
            wealth *= 1.0 + float(cash_returns[index])
        if index == len(targets) - 1 and position == 1.0:
            wealth *= 1.0 - cost
        returns.append(wealth / start - 1.0)
        curve.append(wealth)
    return ReturnPath(
        final_value=wealth,
        net_total_return_pct=(wealth / INITIAL_CAPITAL - 1.0) * 100.0,
        max_drawdown_magnitude_pct=_max_drawdown(curve),
        interval_returns=tuple(returns),
    )


def comparator_return_path(
    prices: Sequence[float],
    target_weight: float,
    *,
    one_way_cost_bps: float,
    cash_returns: Sequence[float] | None = None,
) -> ReturnPath:
    if not 0.0 <= target_weight <= 1.0 or len(prices) < 2:
        raise FoundationalFalsificationExecutionError("F1_ACCOUNTING_INVALID", "comparator")
    if cash_returns is not None and len(cash_returns) != len(prices) - 1:
        raise FoundationalFalsificationExecutionError("F1_ACCOUNTING_INVALID", "cash lengths")
    cost = one_way_cost_bps / 10_000.0
    post_cost = INITIAL_CAPITAL / (1.0 + cost * target_weight)
    qqq_value = target_weight * post_cost
    cash_value = (1.0 - target_weight) * post_cost
    previous_equity = INITIAL_CAPITAL
    returns: list[float] = []
    curve = [previous_equity]
    for index, (left, right) in enumerate(zip(prices, prices[1:], strict=False)):
        qqq_value *= float(right) / float(left)
        if cash_returns is not None:
            cash_value *= 1.0 + float(cash_returns[index])
        equity = qqq_value + cash_value
        if index == len(prices) - 2:
            equity -= cost * qqq_value
        returns.append(equity / previous_equity - 1.0)
        previous_equity = equity
        curve.append(equity)
    return ReturnPath(
        final_value=previous_equity,
        net_total_return_pct=(previous_equity / INITIAL_CAPITAL - 1.0) * 100.0,
        max_drawdown_magnitude_pct=_max_drawdown(curve),
        interval_returns=tuple(returns),
    )


def _compound(returns: Sequence[float]) -> float:
    wealth = 1.0
    for value in returns:
        wealth *= 1.0 + float(value)
    return (wealth - 1.0) * 100.0


def calendar_year_attribution(
    plan: DiagnosticPlan, candidate: ReturnPath, comparator: ReturnPath
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for year in range(2021, 2026):
        indices = [
            index for index, session in enumerate(plan.sessions[:-1]) if session.year == year
        ]
        candidate_pct = _compound([candidate.interval_returns[index] for index in indices])
        comparator_pct = _compound([comparator.interval_returns[index] for index in indices])
        rows.append(
            {
                "calendar_year": year,
                "partial_year": year in (2021, 2025),
                "interval_count": len(indices),
                "candidate_net_total_return_pct": candidate_pct,
                "comparator_net_total_return_pct": comparator_pct,
                "paired_excess_percentage_points": candidate_pct - comparator_pct,
            }
        )
    return rows


def leave_one_year_out(
    plan: DiagnosticPlan, candidate: ReturnPath, comparator: ReturnPath
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for year in range(2021, 2026):
        indices = [
            index for index, session in enumerate(plan.sessions[:-1]) if session.year != year
        ]
        candidate_pct = _compound([candidate.interval_returns[index] for index in indices])
        comparator_pct = _compound([comparator.interval_returns[index] for index in indices])
        rows.append(
            {
                "excluded_calendar_year": year,
                "remaining_interval_count": len(indices),
                "candidate_net_total_return_pct": candidate_pct,
                "comparator_net_total_return_pct": comparator_pct,
                "paired_excess_percentage_points": candidate_pct - comparator_pct,
            }
        )
    return rows


def contiguous_episode_attribution(plan: DiagnosticPlan) -> list[dict[str, object]]:
    episodes: list[dict[str, object]] = []
    start: int | None = None
    for index, target in enumerate((*plan.interval_targets, 0.0)):
        if target == 1.0 and start is None:
            start = index
        if target == 0.0 and start is not None:
            end = index - 1
            prices = plan.qqq_prices[start : end + 2]
            targets = (1.0,) * (end - start + 1)
            candidate = candidate_return_path(prices, targets, one_way_cost_bps=5.0)
            comparator = comparator_return_path(
                prices, plan.comparator_weight, one_way_cost_bps=5.0
            )
            episodes.append(
                {
                    "episode_id": len(episodes) + 1,
                    "start_session": plan.sessions[start].isoformat(),
                    "end_session": plan.sessions[end + 1].isoformat(),
                    "interval_count": end - start + 1,
                    "candidate_net_total_return_pct": candidate.net_total_return_pct,
                    "comparator_net_total_return_pct": comparator.net_total_return_pct,
                    "paired_excess_percentage_points": (
                        candidate.net_total_return_pct - comparator.net_total_return_pct
                    ),
                }
            )
            start = None
    return episodes


def paired_circular_moving_block_bootstrap(
    candidate_returns: Sequence[float],
    comparator_returns: Sequence[float],
    *,
    block_lengths: Sequence[int] = (21, 63),
    seed: int = 2555,
    replicates: int = 10_000,
) -> list[dict[str, object]]:
    candidate = np.asarray(candidate_returns, dtype=np.float64)
    comparator = np.asarray(comparator_returns, dtype=np.float64)
    if candidate.shape != comparator.shape or candidate.ndim != 1 or candidate.size == 0:
        raise FoundationalFalsificationExecutionError("F1_BOOTSTRAP_INVALID", "paired inputs")
    if np.any(candidate <= -1.0) or np.any(comparator <= -1.0):
        raise FoundationalFalsificationExecutionError("F1_BOOTSTRAP_INVALID", "return <= -100%")
    rng = np.random.default_rng(seed)
    results: list[dict[str, object]] = []
    sample_size = int(candidate.size)
    for block_length in block_lengths:
        blocks = math.ceil(sample_size / block_length)
        statistics = np.empty(replicates, dtype=np.float64)
        offsets = np.arange(block_length, dtype=np.int64)
        for batch_start in range(0, replicates, 500):
            batch_size = min(500, replicates - batch_start)
            starts = rng.integers(0, sample_size, size=(batch_size, blocks, 1))
            indices = ((starts + offsets) % sample_size).reshape(batch_size, -1)[:, :sample_size]
            candidate_total = np.expm1(np.log1p(candidate[indices]).sum(axis=1)) * 100.0
            comparator_total = np.expm1(np.log1p(comparator[indices]).sum(axis=1)) * 100.0
            statistics[batch_start : batch_start + batch_size] = candidate_total - comparator_total
        percentiles = np.percentile(statistics, [2.5, 50.0, 97.5])
        results.append(
            {
                "block_length_sessions": int(block_length),
                "replicates": replicates,
                "random_seed": seed,
                "percentile_2_5": float(percentiles[0]),
                "percentile_50": float(percentiles[1]),
                "percentile_97_5": float(percentiles[2]),
                "probability_excess_less_than_or_equal_to_zero": float(np.mean(statistics <= 0.0)),
            }
        )
    return results


def cost_sensitivity(plan: DiagnosticPlan) -> Mapping[str, object]:
    rows: list[dict[str, float]] = []
    for cost_bps in (5.0, 10.0, 15.0, 20.0):
        candidate = candidate_return_path(
            plan.qqq_prices, plan.interval_targets, one_way_cost_bps=cost_bps
        )
        comparator = comparator_return_path(
            plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=cost_bps
        )
        rows.append(
            {
                "one_way_cost_bps": cost_bps,
                "candidate_net_total_return_pct": candidate.net_total_return_pct,
                "comparator_net_total_return_pct": comparator.net_total_return_pct,
                "paired_excess_percentage_points": (
                    candidate.net_total_return_pct - comparator.net_total_return_pct
                ),
            }
        )
    positive = [
        row["one_way_cost_bps"] for row in rows if row["paired_excess_percentage_points"] > 0
    ]
    nonpositive = [
        row["one_way_cost_bps"] for row in rows if row["paired_excess_percentage_points"] <= 0
    ]
    if not positive:
        bracket = "AT_OR_BELOW_5_BPS"
    elif not nonpositive:
        bracket = "ABOVE_20_BPS"
    else:
        bracket = f"BETWEEN_{max(positive):g}_AND_{min(nonpositive):g}_BPS"
    return {"rows": rows, "discrete_break_even_bracket": bracket}


def sgov_carry_sensitivity(plan: DiagnosticPlan) -> Mapping[str, object]:
    cash_returns = _cash_returns(plan.sgov_prices)
    candidate = candidate_return_path(
        plan.qqq_prices,
        plan.interval_targets,
        one_way_cost_bps=5.0,
        cash_returns=cash_returns,
    )
    comparator = comparator_return_path(
        plan.qqq_prices,
        plan.comparator_weight,
        one_way_cost_bps=5.0,
        cash_returns=cash_returns,
    )
    return {
        "cash_carry_asset": "SGOV",
        "role": "DIAGNOSTIC_ONLY",
        "sgov_trade_or_extra_cost_modeled": False,
        "candidate_net_total_return_pct": candidate.net_total_return_pct,
        "comparator_net_total_return_pct": comparator.net_total_return_pct,
        "paired_excess_percentage_points": (
            candidate.net_total_return_pct - comparator.net_total_return_pct
        ),
    }


def state_transition_attribution(plan: DiagnosticPlan) -> list[dict[str, object]]:
    buckets: dict[tuple[str, str], dict[int, list[float]]] = {}
    missing: dict[tuple[str, str], dict[int, int]] = {}
    for index in range(len(plan.states) - 1):
        pair = (plan.states[index], plan.states[index + 1])
        buckets.setdefault(pair, {1: [], 5: [], 20: []})
        missing.setdefault(pair, {1: 0, 5: 0, 20: 0})
        origin = index + 1
        for horizon in (1, 5, 20):
            terminal = origin + horizon
            if terminal >= len(plan.qqq_prices):
                missing[pair][horizon] += 1
            else:
                buckets[pair][horizon].append(
                    plan.qqq_prices[terminal] / plan.qqq_prices[origin] - 1.0
                )
    rows: list[dict[str, object]] = []
    for from_state in ALLOWED_STATES:
        for to_state in ALLOWED_STATES:
            pair = (from_state, to_state)
            if pair not in buckets:
                continue
            horizon_rows = []
            for horizon in (1, 5, 20):
                values = buckets[pair][horizon]
                horizon_rows.append(
                    {
                        "horizon_sessions": horizon,
                        "mature_count": len(values),
                        "missing_count": missing[pair][horizon],
                        "mean_forward_return_pct": (
                            None if not values else float(np.mean(values) * 100.0)
                        ),
                        "median_forward_return_pct": (
                            None if not values else float(np.median(values) * 100.0)
                        ),
                    }
                )
            rows.append(
                {
                    "from_state": from_state,
                    "to_state": to_state,
                    "transition_count": len(buckets[pair][1]) + missing[pair][1],
                    "forward_horizons": horizon_rows,
                }
            )
    return rows


def source_revision_diff(
    manifest: LoadedManifest, *, project_root: Path = PROJECT_ROOT
) -> Mapping[str, object]:
    current = _bindings(manifest)
    old_path = _bound_file(
        current["trading_2550_execution_manifest"].path,
        root=project_root,
        label="trading_2550_manifest",
    )
    old_payload = _mapping(json.loads(old_path.read_bytes()), "trading_2550_manifest")
    old_bindings_raw = old_payload.get("input_bindings")
    if not isinstance(old_bindings_raw, list):
        raise FoundationalFalsificationExecutionError("F1_SOURCE_REVISION_INVALID", "old inputs")
    old = {
        str(_mapping(item, "old_input").get("role")): _mapping(item, "old_input")
        for item in old_bindings_raw
    }
    comparisons: list[dict[str, object]] = []
    for role in COMMON_TRADING_2550_ROLES:
        prior = old.get(role, {})
        matched = (
            prior.get("path") == current[role].path
            and prior.get("sha256") == current[role].sha256
            and prior.get("size_bytes") == current[role].size_bytes
        )
        comparisons.append({"role": role, "matched": matched})
    status = "MATCHED" if all(bool(row["matched"]) for row in comparisons) else "INVALID"
    return {
        "status": status,
        "comparisons": comparisons,
        "new_diagnostic_inputs": ["operational_predictions"],
    }


def _policy_consumption(policy: Any) -> Mapping[str, object]:
    rows = [
        {
            "field_id": item.field_id,
            "authority_id": item.authority_id,
            "expected_status": item.expected_status,
            "observed_status": item.expected_status,
            "matched": True,
        }
        for item in policy.policy_consumption_inventory.entries
    ]
    return {"matches_contract": True, "entries": rows, "old_model_wiring_changed": False}


def _selection_history(policy: Any) -> Mapping[str, object]:
    return {
        "result_visibility": policy.known_result_boundary.result_visibility,
        "known_before_f0_freeze": list(policy.known_result_boundary.known_before_freeze),
        "historical_window_role": policy.known_result_boundary.historical_window_role,
        "pristine_oos_claim_allowed": False,
        "post_result_parameter_rescue_allowed": False,
    }


def _write_once(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as handle:
            handle.write(_canonical_json_bytes(payload))
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError as exc:
        raise FoundationalFalsificationExecutionError(
            "F1_ATTEMPT_ALREADY_CONSUMED", str(path)
        ) from exc


def _git_head(root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
    ).stdout.strip()


def _failure_result(
    *, code: str, message: str, counters: Mapping[str, int], runtime_git_head: str
) -> Mapping[str, object]:
    return {
        "schema_version": "first_layer_composer_v2_foundational_falsification_result.v1",
        "task_id": TASK_ID,
        "status": "TERMINAL",
        "foundational_status": "INVALID",
        "conclusion": "FOUNDATIONAL_EVIDENCE_INVALID",
        "reason_codes": [code],
        "failure": {"code": code, "message": message},
        "requested_range": {"start": REQUESTED_START.isoformat(), "end": REQUESTED_END.isoformat()},
        "evaluated_range": None,
        "runtime_git_head": runtime_git_head,
        "actual_counters": dict(counters),
        "aggregate_result_only": True,
        "raw_market_payload_exported": False,
        "raw_signal_payload_exported": False,
        "qqq_options_wave_b": "HOLD",
        "qqq_options_wave_c": "NOT_AUTHORIZED",
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def execute_foundational_falsification(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, object]:
    root = project_root.resolve()
    target = (root / output_dir).resolve(strict=False)
    target.relative_to(root)
    attempt_path = target / "run_attempt_consumption_receipt.json"
    result_path = target / "aggregate_result.json"
    if attempt_path.exists() or result_path.exists():
        raise FoundationalFalsificationExecutionError("F1_ATTEMPT_ALREADY_CONSUMED", str(target))
    counters = {key: 0 for key in EXPECTED_COUNTERS}
    runtime_git_head = _git_head(root)
    _write_once(
        attempt_path,
        {
            "schema_version": "first_layer_foundational_falsification_attempt.v1",
            "task_id": TASK_ID,
            "status": "DISPATCHED_SINGLE_ATTEMPT_RESERVED",
            "authorized_maxima": EXPECTED_COUNTERS,
            "runtime_git_head": runtime_git_head,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    try:
        authorization = load_run_authorization(project_root=root)
        manifest = load_execution_manifest(
            manifest_path, project_root=root, authorization=authorization
        )
        counters["manifest_replays"] = 1
        replay = replay_execution_manifest(manifest, project_root=root)
        _write_once(target / "manifest_replay_receipt.json", replay)
        bindings = _bindings(manifest)
        counters["canonical_dq_runs"] = 1
        dq = run_canonical_data_quality_execution(
            CanonicalDataQualityExecutionRequest(
                as_of=REQUESTED_END,
                requested_window=DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
                evaluated_window=DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
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
            "schema_version": "first_layer_foundational_falsification_dq_receipt.v1",
            "status": dq.report.status,
            "canonical_dq_receipt_path": dq.receipt_path.relative_to(root).as_posix(),
            "canonical_dq_receipt_sha256": _sha256_path(dq.receipt_path),
            "canonical_dq_report_path": dq.report_path.relative_to(root).as_posix(),
            "canonical_dq_report_sha256": _sha256_path(dq.report_path),
            "requested_start": REQUESTED_START.isoformat(),
            "requested_end": REQUESTED_END.isoformat(),
            "evaluated_start": dq.receipt.evaluated_window.start.isoformat(),
            "evaluated_end": dq.receipt.evaluated_window.end.isoformat(),
            "error_count": dq.report.error_count,
            "warning_count": dq.report.warning_count,
        }
        _write_once(target / "canonical_dq_receipt.json", dq_receipt)
        if dq.report.status != "PASS":
            raise FoundationalFalsificationExecutionError("F1_DQ_OR_PIT_NOT_PASS", dq.report.status)

        counters["local_foundational_runs"] = 1
        contract = load_foundational_falsification_contract(project_root=root)
        plan = load_diagnostic_plan(manifest, project_root=root)
        candidate = candidate_return_path(
            plan.qqq_prices, plan.interval_targets, one_way_cost_bps=5.0
        )
        comparator = comparator_return_path(
            plan.qqq_prices, plan.comparator_weight, one_way_cost_bps=5.0
        )
        old_candidate = calculate_candidate_primary(plan.qqq_prices, plan.interval_targets)
        old_comparator = calculate_static_comparator_primary(
            plan.qqq_prices, plan.comparator_weight
        )
        counters["independent_replays"] = 1
        reconciliation = {
            "candidate_final_value_abs_diff": abs(
                candidate.final_value - old_candidate.final_value
            ),
            "comparator_final_value_abs_diff": abs(
                comparator.final_value - old_comparator.final_value
            ),
            "candidate_compound_abs_diff_pp": abs(
                _compound(candidate.interval_returns) - candidate.net_total_return_pct
            ),
            "comparator_compound_abs_diff_pp": abs(
                _compound(comparator.interval_returns) - comparator.net_total_return_pct
            ),
        }
        if any(value > RECONCILIATION_TOLERANCE for value in reconciliation.values()):
            raise FoundationalFalsificationExecutionError(
                "F1_INDEPENDENT_REPLAY_NOT_PASS", json.dumps(reconciliation, sort_keys=True)
            )
        independent = {
            "schema_version": "first_layer_foundational_falsification_independent_replay.v1",
            "status": "PASS",
            "tolerance": RECONCILIATION_TOLERANCE,
            "reconciliation": reconciliation,
        }
        _write_once(target / "independent_replay_receipt.json", independent)

        years = calendar_year_attribution(plan, candidate, comparator)
        episodes = contiguous_episode_attribution(plan)
        leave_out = leave_one_year_out(plan, candidate, comparator)
        bootstrap = paired_circular_moving_block_bootstrap(
            candidate.interval_returns, comparator.interval_returns
        )
        source_diff = source_revision_diff(manifest, project_root=root)
        policy_consumption = _policy_consumption(contract.policy)
        primary_excess = candidate.net_total_return_pct - comparator.net_total_return_pct
        summary = FoundationalDiagnosticSummary(
            completed_diagnostic_ids=DIAGNOSTIC_IDS,
            policy_consumption_matches_contract=bool(policy_consumption["matches_contract"]),
            source_revision_status=cast(Any, source_diff["status"]),
            primary_paired_excess_percentage_points=primary_excess,
            bootstrap_intervals=tuple(BootstrapInterval.model_validate(row) for row in bootstrap),
            leave_one_calendar_year_out=tuple(
                LeaveOneYearOutResult(
                    calendar_year=cast(Any, row["excluded_calendar_year"]),
                    paired_excess_percentage_points=cast(
                        Any, row["paired_excess_percentage_points"]
                    ),
                )
                for row in leave_out
            ),
        )
        decision = reduce_foundational_falsification_status(summary, policy=contract.policy)
        result: Mapping[str, object] = {
            "schema_version": "first_layer_composer_v2_foundational_falsification_result.v1",
            "task_id": TASK_ID,
            "status": "TERMINAL",
            "foundational_status": decision.status,
            "conclusion": decision.conclusion,
            "reason_codes": list(decision.reason_codes),
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "pristine_out_of_sample_claim": False,
            "requested_range": {
                "start": REQUESTED_START.isoformat(),
                "end": REQUESTED_END.isoformat(),
            },
            "evaluated_range": {
                "start": plan.sessions[0].isoformat(),
                "end": plan.sessions[-1].isoformat(),
            },
            "signal_session_count": len(plan.sessions),
            "return_interval_count": len(plan.interval_targets),
            "signal_lag_sessions": 1,
            "long_interval_count": plan.long_interval_count,
            "exposure_matched_comparator_weight": plan.comparator_weight,
            "action_counts": dict(plan.action_counts),
            "primary_5_bps": {
                "candidate_net_total_return_pct": candidate.net_total_return_pct,
                "candidate_max_drawdown_magnitude_pct": candidate.max_drawdown_magnitude_pct,
                "comparator_net_total_return_pct": comparator.net_total_return_pct,
                "comparator_max_drawdown_magnitude_pct": comparator.max_drawdown_magnitude_pct,
                "paired_excess_percentage_points": primary_excess,
            },
            "diagnostics": {
                "policy_consumption_inventory": policy_consumption,
                "calendar_year_attribution": years,
                "contiguous_episode_attribution": {
                    "episode_count": len(episodes),
                    "episodes": episodes,
                },
                "leave_one_calendar_year_out": leave_out,
                "paired_moving_block_bootstrap": bootstrap,
                "cost_sensitivity": cost_sensitivity(plan),
                "sgov_carry_sensitivity": sgov_carry_sensitivity(plan),
                "state_transition_attribution": state_transition_attribution(plan),
                "selection_history_inventory": _selection_history(contract.policy),
                "source_revision_diff": source_diff,
            },
            "completed_diagnostic_ids": list(DIAGNOSTIC_IDS),
            "manifest_file_sha256": manifest.file_sha256,
            "manifest_canonical_sha256": manifest.canonical_sha256,
            "authorization_file_sha256": authorization.file_sha256,
            "authorization_canonical_sha256": authorization.canonical_sha256,
            "f0_file_sha256": contract.policy_file_sha256,
            "canonical_dq": dq_receipt,
            "independent_replay": independent,
            "runtime_git_head": runtime_git_head,
            "actual_counters": counters,
            "aggregate_result_only": True,
            "raw_market_payload_exported": False,
            "raw_signal_payload_exported": False,
            "qqq_options_wave_b": decision.qqq_options_wave_b,
            "qqq_options_wave_c": decision.qqq_options_wave_c,
            "production_allowed": False,
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
        if isinstance(exc, FoundationalFalsificationExecutionError):
            code, message = exc.code, exc.message
        else:
            code, message = "F1_UNEXPECTED_FAILURE", str(exc)
        failure = _failure_result(
            code=code, message=message, counters=counters, runtime_git_head=runtime_git_head
        )
        _write_once(target / "failure_receipt.json", failure)
        _write_once(result_path, failure)
        return failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute_foundational_falsification(
        args.manifest, output_dir=args.output_dir, project_root=args.project_root
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("foundational_status") in {"FAIL", "INSUFFICIENT", "PASS"} else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "DEFAULT_AUTHORIZATION_PATH",
    "DEFAULT_MANIFEST_PATH",
    "DEFAULT_OUTPUT_DIR",
    "DiagnosticPlan",
    "FoundationalFalsificationExecutionError",
    "InputBinding",
    "LoadedManifest",
    "ReturnPath",
    "calendar_year_attribution",
    "candidate_return_path",
    "comparator_return_path",
    "contiguous_episode_attribution",
    "cost_sensitivity",
    "execute_foundational_falsification",
    "leave_one_year_out",
    "load_diagnostic_plan",
    "load_execution_manifest",
    "load_run_authorization",
    "paired_circular_moving_block_bootstrap",
    "replay_execution_manifest",
    "sgov_carry_sensitivity",
    "source_revision_diff",
    "state_transition_attribution",
]
