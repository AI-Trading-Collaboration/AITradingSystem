"""Run the bounded matched-placebo timing falsification for first_layer_composer_v2.

The empirical surface is intentionally narrow: exact manifest replay, one
canonical DQ call, one deterministic 10,000-draw run, aggregate-only output,
and one independent deterministic replay.  It has no downloader, provider,
option, broker, production, or trading integration.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system import first_layer_foundational_falsification_execution as f1
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = f1.PROJECT_ROOT
DEFAULT_POLICY_PATH = Path(
    "config/research/first_layer_composer_v2_matched_placebo_preregistration_v1.yaml"
)
DEFAULT_AUTHORIZATION_PATH = Path(
    "config/research/first_layer_composer_v2_matched_placebo_run_authorization_v1.yaml"
)
DEFAULT_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_matched_placebo_v1/execution_manifest.json"
)
DEFAULT_OUTPUT_DIR = Path("outputs/research/first_layer_composer_v2_matched_placebo_v1")

TASK_ID = "TRADING-2558_FIRST_LAYER_COMPOSER_V2_MATCHED_PLACEBO_FALSIFICATION_V1"
AUTHORIZATION_STATUS = "OWNER_STANDING_SCOPE_BOUNDED_MATCHED_PLACEBO_AUTHORIZED"
AUTHORIZATION_STATE = "STANDING_OWNER_SCOPE"
OWNER_DECISION_REF = "owner_instruction:TRADING-2558:2026-09-03:continue_low_cost_validation"
REQUESTED_START = f1.REQUESTED_START
REQUESTED_END = f1.REQUESTED_END
EXPECTED_SESSIONS = f1.EXPECTED_SESSIONS
EXPECTED_INTERVALS = f1.EXPECTED_INTERVALS
EXPECTED_LONG_INTERVALS = 385
EXPECTED_LONG_EPISODES = 41
ONE_WAY_COST_BPS = 5.0
RANDOM_SEED = 2558
PLACEBO_DRAWS = 10_000
PILOT_ALPHA = 0.05
RECONCILIATION_TOLERANCE = 1e-10

TRADING_2557_BINDINGS = {
    "trading_2557_module": (
        Path("src/ai_trading_system/first_layer_foundational_falsification_execution_v2.py"),
        "00b75a97617ba0292b9fcb93af77b181e5db81166c9583c75e50fce68123b62c",
        28_644,
    ),
    "trading_2557_authorization": (
        Path(
            "config/research/first_layer_composer_v2_foundational_falsification_"
            "failure_fix_run_authorization_v1.yaml"
        ),
        "0c980179b23bc133433eaea588155cb5368bcccb2d127e2830c089700b248137",
        6_170,
    ),
    "trading_2557_manifest": (
        Path(
            "inputs/research/first_layer_composer_v2_foundational_falsification_"
            "failure_fix_v1/execution_manifest.json"
        ),
        "cce50b9fbfb5902e2b63168b380c1a4e3dc67d90dee737fa80e047cbff54b3bf",
        6_140,
    ),
    "trading_2557_aggregate_result": (
        Path(
            "outputs/research/first_layer_composer_v2_foundational_falsification_"
            "failure_fix_v1/aggregate_result.json"
        ),
        "76024a73b47414a40f35f6ebb72d41a0dd1fc80421f80ec168a37b6b9f429d2e",
        35_802,
    ),
    "trading_2557_result_admission": (
        Path(
            "config/research/first_layer_composer_v2_foundational_falsification_"
            "failure_fix_result_admission_v1.yaml"
        ),
        "e0d42e6a36f5bc65981d040c55684aa7b852c7d69c54f19f65a4461396f87c60",
        6_202,
    ),
}

INPUT_ROLES = (
    "matched_placebo_preregistration",
    *f1.INPUT_ROLES,
    *TRADING_2557_BINDINGS,
)
EXPECTED_COUNTERS = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_matched_placebo_runs": 1,
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


class MatchedPlaceboExecutionError(ValueError):
    """Stable fail-closed error for the matched-placebo execution."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ExposureShape:
    interval_count: int
    leading_flat_count: int
    trailing_flat_count: int
    long_run_lengths: tuple[int, ...]
    interior_flat_gap_lengths: tuple[int, ...]
    long_interval_count: int
    accounting_trade_event_count: int


@dataclass(frozen=True)
class LoadedPolicy:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class PlaceboDistribution:
    observed_excess_percentage_points: float
    observed_max_drawdown_magnitude_pct: float
    comparator_net_total_return_pct: float
    placebo_excess_percentage_points: tuple[float, ...]
    placebo_max_drawdown_magnitude_pct: tuple[float, ...]
    target_inventory_sha256: str


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise MatchedPlaceboExecutionError(
            "MPF_IDENTITY_MISMATCH", f"{label}: expected={expected!r} actual={actual!r}"
        )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise MatchedPlaceboExecutionError(
            "MPF_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _sha256_path(path: Path) -> str:
    return f1._sha256_path(path)


def _canonical_sha256(value: object) -> str:
    return f1._sha256_bytes(f1._canonical_json_bytes(value))


def load_preregistration(
    path: Path = DEFAULT_POLICY_PATH, *, project_root: Path = PROJECT_ROOT
) -> LoadedPolicy:
    resolved = f1._bound_file(path, root=project_root, label="matched_placebo_policy")
    raw = resolved.read_bytes()
    payload = _mapping(load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "policy")
    _expect(
        tuple(payload),
        (
            "schema_version",
            "policy_id",
            "policy_version",
            "policy_status",
            "task_id",
            "owner",
            "approval_ref",
            "known_result_boundary",
            "primary_identity",
            "matched_placebo_contract",
            "statistics",
            "pilot_reducer",
            "run_envelope",
            "safety",
        ),
        "policy.root_fields",
    )
    _expect(
        payload["schema_version"],
        "first_layer_composer_v2_matched_placebo_preregistration.v1",
        "policy.schema_version",
    )
    _expect(payload["policy_id"], path.stem, "policy.policy_id")
    _expect(payload["policy_version"], "1.0.0", "policy.policy_version")
    _expect(
        payload["policy_status"],
        "OWNER_DIRECTED_PARTIAL_PRIOR_VISIBILITY_CONTRACT",
        "policy.policy_status",
    )
    _expect(payload["task_id"], TASK_ID, "policy.task_id")
    _expect(payload["approval_ref"], OWNER_DECISION_REF, "policy.approval_ref")
    _expect(
        _mapping(payload["known_result_boundary"], "policy.known_result_boundary"),
        {
            "result_visibility": "PARTIAL_PRIOR_VISIBILITY",
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "pristine_out_of_sample_claim_allowed": False,
            "matched_placebo_distribution_visible_before_freeze": False,
            "post_result_parameter_rescue_allowed": False,
        },
        "policy.known_result_boundary",
    )
    identity = _mapping(payload["primary_identity"], "policy.primary_identity")
    expected_identity = {
        "producer_id": "first_layer_composer_v2",
        "calendar": "XNYS",
        "requested_start": REQUESTED_START.isoformat(),
        "requested_end": REQUESTED_END.isoformat(),
        "evaluated_start": REQUESTED_START.isoformat(),
        "evaluated_end": REQUESTED_END.isoformat(),
        "expected_signal_sessions": EXPECTED_SESSIONS,
        "expected_return_intervals": EXPECTED_INTERVALS,
        "expected_long_intervals": EXPECTED_LONG_INTERVALS,
        "expected_long_episodes": EXPECTED_LONG_EPISODES,
        "signal_lag_sessions": 1,
        "candidate_id": "FROZEN_SIGNAL_FULLY_FUNDED_QQQ_ZERO_RETURN_CASH",
        "comparator_id": "EXPOSURE_MATCHED_STATIC_QQQ_ZERO_RETURN_CASH",
        "comparator_weight_formula": ("LONG_EXPOSURE_RETURN_INTERVAL_COUNT_DIVIDED_BY_1201"),
        "primary_one_way_cost_bps": ONE_WAY_COST_BPS,
        "primary_idle_cash_asset": "ZERO_RETURN_CASH",
    }
    _expect(identity, expected_identity, "policy.primary_identity")
    placebo = _mapping(payload["matched_placebo_contract"], "policy.placebo")
    _expect(
        placebo,
        {
            "construction": (
                "PERMUTE_LONG_RUN_AND_INTERIOR_FLAT_GAP_LENGTH_MULTISETS_" "INDEPENDENTLY"
            ),
            "leading_flat_boundary_gap": "FIXED",
            "trailing_flat_boundary_gap": "FIXED",
            "long_interval_count": "EXACT_MATCH_REQUIRED",
            "long_episode_count": "EXACT_MATCH_REQUIRED",
            "long_run_length_multiset": "EXACT_MATCH_REQUIRED",
            "interior_flat_gap_length_multiset": "EXACT_MATCH_REQUIRED",
            "boundary_states": "EXACT_MATCH_REQUIRED",
            "accounting_trade_event_count": "EXACT_MATCH_REQUIRED",
            "random_seed": RANDOM_SEED,
            "draws": PLACEBO_DRAWS,
            "permutation_key": "SHA256_SEED_DRAW_STREAM_INDEX_ASCENDING",
            "parameter_search_allowed": False,
        },
        "policy.placebo",
    )
    statistics = _mapping(payload["statistics"], "policy.statistics")
    _expect(
        statistics,
        {
            "primary_statistic": "OBSERVED_5_BPS_PAIRED_EXCESS_VS_MATCHED_PLACEBO_EXCESS",
            "one_sided_p_value": (
                "ONE_PLUS_PLACEBO_GREATER_THAN_OR_EQUAL_OBSERVED_DIVIDED_BY_DRAWS_PLUS_ONE"
            ),
            "observed_percentile": "EMPIRICAL_CDF_LESS_THAN_OR_EQUAL",
            "quantile_method": "LINEAR_TYPE_7",
            "placebo_percentiles": [2.5, 50.0, 97.5],
            "max_drawdown_role": "DESCRIPTIVE_ONLY",
        },
        "policy.statistics",
    )
    reducer = _mapping(payload["pilot_reducer"], "policy.reducer")
    _expect(
        reducer,
        {
            "alpha": PILOT_ALPHA,
            "status": "TEMPORARY_PILOT_BASELINE",
            "owner_reviewer": "Project Owner / strategy research",
            "rationale": (
                "Common one-sided randomization-test research threshold; not an " "investment gate."
            ),
            "review_condition": "Review after the write-once result is available.",
            "exit_condition": ("Replace with prospective OOS policy or archive the candidate."),
            "precedence": [
                "INVALID",
                "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO",
                "TIMING_DISTINGUISHED_DIAGNOSTIC_ONLY",
            ],
            "not_distinguished_if": "p_value_strictly_greater_than_0_05",
            "distinguished_diagnostic_only_if": ("p_value_less_than_or_equal_to_0_05"),
            "trading_2557_status_must_remain": "INSUFFICIENT",
            "qqq_options_wave_b": "HOLD",
            "qqq_options_wave_c": "NOT_AUTHORIZED",
            "production_allowed": False,
        },
        "policy.reducer",
    )
    _expect(
        _mapping(payload["run_envelope"], "policy.run_envelope"),
        EXPECTED_COUNTERS,
        "policy.run_envelope",
    )
    safety = _mapping(payload["safety"], "policy.safety")
    _expect(
        set(safety),
        {
            "aggregate_result_only",
            "raw_market_payload_export_allowed",
            "raw_signal_payload_export_allowed",
            "placebo_path_export_allowed",
            "data_download_authorized",
            "cache_mutation_authorized",
            "quantconnect_authorized",
            "option_backtest_authorized",
            "provider_authorized",
            "paper_allowed",
            "live_allowed",
            "production_allowed",
            "broker_allowed",
            "production_effect",
            "broker_action",
        },
        "policy.safety_fields",
    )
    for field in (
        "raw_market_payload_export_allowed",
        "raw_signal_payload_export_allowed",
        "placebo_path_export_allowed",
        "data_download_authorized",
        "cache_mutation_authorized",
        "quantconnect_authorized",
        "option_backtest_authorized",
        "provider_authorized",
        "paper_allowed",
        "live_allowed",
        "production_allowed",
        "broker_allowed",
    ):
        _expect(safety[field], False, f"policy.safety.{field}")
    _expect(safety["aggregate_result_only"], True, "policy.aggregate_result_only")
    _expect(safety["production_effect"], "none", "policy.production_effect")
    _expect(safety["broker_action"], "none", "policy.broker_action")
    return LoadedPolicy(
        payload=payload,
        path=resolved,
        file_sha256=f1._sha256_bytes(raw),
        canonical_sha256=_canonical_sha256(payload),
    )


def _trade_event_count(targets: Sequence[float]) -> int:
    position = 0.0
    count = 0
    for target in targets:
        if target != position:
            count += 1
            position = float(target)
    if position == 1.0:
        count += 1
    return count


def extract_exposure_shape(targets: Sequence[float]) -> ExposureShape:
    values = tuple(float(value) for value in targets)
    if not values or any(value not in (0.0, 1.0) for value in values):
        raise MatchedPlaceboExecutionError("MPF_SHAPE_INVALID", "targets must be binary")
    first_long = next((index for index, value in enumerate(values) if value == 1.0), None)
    if first_long is None:
        raise MatchedPlaceboExecutionError("MPF_SHAPE_INVALID", "no LONG interval")
    last_long = (
        len(values)
        - 1
        - next(index for index, value in enumerate(reversed(values)) if value == 1.0)
    )
    long_runs: list[int] = []
    flat_gaps: list[int] = []
    index = first_long
    while index <= last_long:
        run_start = index
        while index <= last_long and values[index] == 1.0:
            index += 1
        long_runs.append(index - run_start)
        if index <= last_long:
            gap_start = index
            while index <= last_long and values[index] == 0.0:
                index += 1
            gap = index - gap_start
            if gap <= 0:
                raise MatchedPlaceboExecutionError("MPF_SHAPE_INVALID", "empty interior gap")
            flat_gaps.append(gap)
    if len(flat_gaps) != len(long_runs) - 1:
        raise MatchedPlaceboExecutionError("MPF_SHAPE_INVALID", "run/gap topology")
    return ExposureShape(
        interval_count=len(values),
        leading_flat_count=first_long,
        trailing_flat_count=len(values) - 1 - last_long,
        long_run_lengths=tuple(long_runs),
        interior_flat_gap_lengths=tuple(flat_gaps),
        long_interval_count=sum(long_runs),
        accounting_trade_event_count=_trade_event_count(values),
    )


def _permutation_indices(count: int, *, seed: int, draw: int, stream: str) -> tuple[int, ...]:
    keyed = []
    for index in range(count):
        key = hashlib.sha256(f"{seed}:{draw}:{stream}:{index}".encode("ascii")).digest()
        keyed.append((key, index))
    return tuple(index for _, index in sorted(keyed))


def reconstruct_placebo_targets(shape: ExposureShape, *, seed: int, draw: int) -> tuple[float, ...]:
    run_order = _permutation_indices(
        len(shape.long_run_lengths), seed=seed, draw=draw, stream="LONG_RUN"
    )
    gap_order = _permutation_indices(
        len(shape.interior_flat_gap_lengths), seed=seed, draw=draw, stream="FLAT_GAP"
    )
    runs = tuple(shape.long_run_lengths[index] for index in run_order)
    gaps = tuple(shape.interior_flat_gap_lengths[index] for index in gap_order)
    values: list[float] = [0.0] * shape.leading_flat_count
    for index, run in enumerate(runs):
        values.extend([1.0] * run)
        if index < len(gaps):
            values.extend([0.0] * gaps[index])
    values.extend([0.0] * shape.trailing_flat_count)
    targets = tuple(values)
    _validate_placebo_targets(targets, shape)
    return targets


def _validate_placebo_targets(targets: Sequence[float], expected: ExposureShape) -> None:
    actual = extract_exposure_shape(targets)
    if (
        actual.interval_count != expected.interval_count
        or actual.leading_flat_count != expected.leading_flat_count
        or actual.trailing_flat_count != expected.trailing_flat_count
        or actual.long_interval_count != expected.long_interval_count
        or len(actual.long_run_lengths) != len(expected.long_run_lengths)
        or sorted(actual.long_run_lengths) != sorted(expected.long_run_lengths)
        or sorted(actual.interior_flat_gap_lengths) != sorted(expected.interior_flat_gap_lengths)
        or actual.accounting_trade_event_count != expected.accounting_trade_event_count
    ):
        raise MatchedPlaceboExecutionError("MPF_PLACEBO_INVARIANT_DRIFT", str(actual))


def _update_target_digest(digest: Any, targets: Sequence[float]) -> None:
    digest.update(bytes(int(value) for value in targets))


def build_placebo_distribution(
    prices: Sequence[float],
    observed_targets: Sequence[float],
    comparator_weight: float,
    *,
    seed: int = RANDOM_SEED,
    draws: int = PLACEBO_DRAWS,
) -> PlaceboDistribution:
    shape = extract_exposure_shape(observed_targets)
    observed = f1.candidate_return_path(prices, observed_targets, one_way_cost_bps=ONE_WAY_COST_BPS)
    comparator = f1.comparator_return_path(
        prices, comparator_weight, one_way_cost_bps=ONE_WAY_COST_BPS
    )
    excesses: list[float] = []
    drawdowns: list[float] = []
    digest = hashlib.sha256()
    for draw in range(draws):
        targets = reconstruct_placebo_targets(shape, seed=seed, draw=draw)
        _update_target_digest(digest, targets)
        path = f1.candidate_return_path(prices, targets, one_way_cost_bps=ONE_WAY_COST_BPS)
        excesses.append(path.net_total_return_pct - comparator.net_total_return_pct)
        drawdowns.append(path.max_drawdown_magnitude_pct)
    return PlaceboDistribution(
        observed_excess_percentage_points=(
            observed.net_total_return_pct - comparator.net_total_return_pct
        ),
        observed_max_drawdown_magnitude_pct=observed.max_drawdown_magnitude_pct,
        comparator_net_total_return_pct=comparator.net_total_return_pct,
        placebo_excess_percentage_points=tuple(excesses),
        placebo_max_drawdown_magnitude_pct=tuple(drawdowns),
        target_inventory_sha256=digest.hexdigest(),
    )


def _linear_type_7(values: Sequence[float], percentile: float) -> float:
    if not values or not 0.0 <= percentile <= 100.0:
        raise MatchedPlaceboExecutionError("MPF_STATISTIC_INVALID", "percentile")
    ordered = sorted(float(value) for value in values)
    position = (len(ordered) - 1) * percentile / 100.0
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def summarize_distribution(distribution: PlaceboDistribution) -> Mapping[str, object]:
    observed = distribution.observed_excess_percentage_points
    excesses = distribution.placebo_excess_percentage_points
    drawdowns = distribution.placebo_max_drawdown_magnitude_pct
    if len(excesses) != PLACEBO_DRAWS or len(drawdowns) != PLACEBO_DRAWS:
        raise MatchedPlaceboExecutionError("MPF_DRAW_COUNT_INVALID", str(len(excesses)))
    exceedance_count = sum(value >= observed for value in excesses)
    p_value = (1.0 + exceedance_count) / (PLACEBO_DRAWS + 1.0)
    status = (
        "TIMING_DISTINGUISHED_DIAGNOSTIC_ONLY"
        if p_value <= PILOT_ALPHA
        else "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO"
    )
    return {
        "reducer_status": status,
        "pilot_alpha": PILOT_ALPHA,
        "one_sided_p_value": p_value,
        "placebo_excess_greater_than_or_equal_observed_count": exceedance_count,
        "observed_excess_percentile": (
            100.0 * sum(value <= observed for value in excesses) / PLACEBO_DRAWS
        ),
        "placebo_excess_percentage_points": {
            "percentile_2_5": _linear_type_7(excesses, 2.5),
            "percentile_50": _linear_type_7(excesses, 50.0),
            "percentile_97_5": _linear_type_7(excesses, 97.5),
        },
        "observed_max_drawdown_magnitude_pct": (distribution.observed_max_drawdown_magnitude_pct),
        "observed_max_drawdown_empirical_cdf_percentile": (
            100.0
            * sum(value <= distribution.observed_max_drawdown_magnitude_pct for value in drawdowns)
            / PLACEBO_DRAWS
        ),
        "placebo_max_drawdown_magnitude_pct": {
            "percentile_2_5": _linear_type_7(drawdowns, 2.5),
            "percentile_50": _linear_type_7(drawdowns, 50.0),
            "percentile_97_5": _linear_type_7(drawdowns, 97.5),
        },
        "target_inventory_sha256": distribution.target_inventory_sha256,
    }


def _independent_candidate_metrics(
    prices: Sequence[float], targets: Sequence[float]
) -> tuple[float, float]:
    cost = ONE_WAY_COST_BPS / 10_000.0
    wealth = f1.INITIAL_CAPITAL
    position = 0.0
    peak = wealth
    drawdown = 0.0
    for index, target in enumerate(targets):
        if target != position:
            wealth *= 1.0 / (1.0 + cost) if target == 1.0 else 1.0 - cost
            position = float(target)
        if position == 1.0:
            wealth *= float(prices[index + 1]) / float(prices[index])
        if index == len(targets) - 1 and position == 1.0:
            wealth *= 1.0 - cost
        peak = max(peak, wealth)
        drawdown = max(drawdown, 1.0 - wealth / peak)
    return (wealth / f1.INITIAL_CAPITAL - 1.0) * 100.0, drawdown * 100.0


def independently_replay_distribution(
    prices: Sequence[float],
    observed_targets: Sequence[float],
    comparator_weight: float,
    primary: PlaceboDistribution,
) -> Mapping[str, object]:
    shape = extract_exposure_shape(observed_targets)
    comparator = f1.comparator_return_path(
        prices, comparator_weight, one_way_cost_bps=ONE_WAY_COST_BPS
    )
    digest = hashlib.sha256()
    max_excess_diff = 0.0
    max_drawdown_diff = 0.0
    replay_excesses: list[float] = []
    replay_drawdowns: list[float] = []
    for draw in range(PLACEBO_DRAWS):
        targets = reconstruct_placebo_targets(shape, seed=RANDOM_SEED, draw=draw)
        _update_target_digest(digest, targets)
        total_return, drawdown = _independent_candidate_metrics(prices, targets)
        excess = total_return - comparator.net_total_return_pct
        max_excess_diff = max(
            max_excess_diff, abs(excess - primary.placebo_excess_percentage_points[draw])
        )
        max_drawdown_diff = max(
            max_drawdown_diff,
            abs(drawdown - primary.placebo_max_drawdown_magnitude_pct[draw]),
        )
        replay_excesses.append(excess)
        replay_drawdowns.append(drawdown)
    if digest.hexdigest() != primary.target_inventory_sha256:
        raise MatchedPlaceboExecutionError("MPF_INDEPENDENT_REPLAY_FAILED", "target digest")
    if max(max_excess_diff, max_drawdown_diff) > RECONCILIATION_TOLERANCE:
        raise MatchedPlaceboExecutionError(
            "MPF_INDEPENDENT_REPLAY_FAILED",
            f"excess={max_excess_diff} drawdown={max_drawdown_diff}",
        )
    replay = PlaceboDistribution(
        observed_excess_percentage_points=primary.observed_excess_percentage_points,
        observed_max_drawdown_magnitude_pct=primary.observed_max_drawdown_magnitude_pct,
        comparator_net_total_return_pct=primary.comparator_net_total_return_pct,
        placebo_excess_percentage_points=tuple(replay_excesses),
        placebo_max_drawdown_magnitude_pct=tuple(replay_drawdowns),
        target_inventory_sha256=digest.hexdigest(),
    )
    primary_summary = summarize_distribution(primary)
    replay_summary = summarize_distribution(replay)
    if primary_summary != replay_summary:
        raise MatchedPlaceboExecutionError("MPF_INDEPENDENT_REPLAY_FAILED", "summary drift")
    return {
        "schema_version": "first_layer_matched_placebo_independent_replay.v1",
        "status": "PASS",
        "tolerance": RECONCILIATION_TOLERANCE,
        "maximum_placebo_excess_abs_diff": max_excess_diff,
        "maximum_placebo_drawdown_abs_diff": max_drawdown_diff,
        "target_inventory_sha256": digest.hexdigest(),
        "one_sided_p_value": primary_summary["one_sided_p_value"],
        "reducer_status": primary_summary["reducer_status"],
    }


def _validate_trading_2557_bindings(*, project_root: Path) -> None:
    for role, (path, sha256, size_bytes) in TRADING_2557_BINDINGS.items():
        bound = f1._bound_file(path, root=project_root, label=role)
        _expect(_sha256_path(bound), sha256, f"{role}.sha256")
        _expect(bound.stat().st_size, size_bytes, f"{role}.size_bytes")
    result_path = project_root / TRADING_2557_BINDINGS["trading_2557_aggregate_result"][0]
    result = _mapping(json.loads(result_path.read_bytes()), "trading_2557_result")
    _expect(result["foundational_status"], "INSUFFICIENT", "trading_2557.status")
    _expect(result["qqq_options_wave_b"], "HOLD", "trading_2557.wave_b")
    _expect(result["qqq_options_wave_c"], "NOT_AUTHORIZED", "trading_2557.wave_c")


def _trading_2557_primary_result(*, project_root: Path) -> Mapping[str, Any]:
    result_path = project_root / TRADING_2557_BINDINGS["trading_2557_aggregate_result"][0]
    result = _mapping(json.loads(result_path.read_bytes()), "trading_2557_result")
    primary = _mapping(result["primary_5_bps"], "trading_2557.primary_5_bps")
    _expect(result["signal_session_count"], EXPECTED_SESSIONS, "trading_2557.sessions")
    _expect(result["return_interval_count"], EXPECTED_INTERVALS, "trading_2557.intervals")
    _expect(result["long_interval_count"], EXPECTED_LONG_INTERVALS, "trading_2557.long_count")
    _expect(
        result["exposure_matched_comparator_weight"],
        EXPECTED_LONG_INTERVALS / EXPECTED_INTERVALS,
        "trading_2557.comparator_weight",
    )
    return primary


def load_run_authorization(
    path: Path = DEFAULT_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    policy: LoadedPolicy | None = None,
) -> f1.LoadedAuthorization:
    loaded_policy = policy or load_preregistration(project_root=project_root)
    resolved = f1._bound_file(path, root=project_root, label="authorization")
    raw = resolved.read_bytes()
    payload = _mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "authorization"
    )
    _expect(
        payload["schema_version"],
        "first_layer_composer_v2_matched_placebo_run_authorization.v1",
        "authorization.schema_version",
    )
    _expect(payload["authorization_id"], path.stem, "authorization.authorization_id")
    _expect(payload["authorization_version"], "1.0.0", "authorization.version")
    _expect(payload["status"], AUTHORIZATION_STATUS, "authorization.status")
    _expect(payload["task_id"], TASK_ID, "authorization.task_id")
    _expect(payload["scope"], "R1_BOUNDED_RESEARCH_SANDBOX", "authorization.scope")
    owner = _mapping(payload["owner_decision"], "authorization.owner_decision")
    _expect(owner["decision_ref"], OWNER_DECISION_REF, "authorization.decision_ref")
    _expect(owner["authorization_state"], AUTHORIZATION_STATE, "authorization.state")
    _expect(owner["exact_bounded_run_granted"], True, "authorization.bounded_run")
    policy_binding = _mapping(payload["policy_binding"], "authorization.policy_binding")
    _expect(policy_binding["path"], DEFAULT_POLICY_PATH.as_posix(), "policy_binding.path")
    _expect(policy_binding["file_sha256"], loaded_policy.file_sha256, "policy_binding.file")
    _expect(
        policy_binding["canonical_sha256"],
        loaded_policy.canonical_sha256,
        "policy_binding.canonical",
    )
    _expect(
        _mapping(payload["run_envelope"], "authorization.run_envelope"),
        EXPECTED_COUNTERS,
        "authorization.run_envelope",
    )
    allowlist = _mapping(payload["input_allowlist"], "authorization.input_allowlist")
    _expect(tuple(allowlist), INPUT_ROLES, "authorization.input_roles")
    safety = _mapping(payload["safety"], "authorization.safety")
    for field in (
        "outcome_access_authorized",
        "market_data_read_authorized",
        "manifest_replay_authorized",
        "canonical_dq_authorized",
        "local_matched_placebo_run_authorized",
        "independent_replay_authorized",
    ):
        _expect(safety[field], True, f"authorization.safety.{field}")
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
        _expect(safety[field], False, f"authorization.safety.{field}")
    _expect(safety["production_effect"], "none", "authorization.production_effect")
    _expect(safety["broker_action"], "none", "authorization.broker_action")
    _validate_trading_2557_bindings(project_root=project_root)
    return f1.LoadedAuthorization(
        payload=payload,
        path=resolved,
        file_sha256=f1._sha256_bytes(raw),
        canonical_sha256=_canonical_sha256(payload),
    )


def load_execution_manifest(
    path: Path = DEFAULT_MANIFEST_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    authorization: f1.LoadedAuthorization | None = None,
) -> f1.LoadedManifest:
    auth = authorization or load_run_authorization(project_root=project_root)
    resolved = f1._bound_file(path, root=project_root, label="manifest")
    raw = resolved.read_bytes()
    payload = _mapping(json.loads(raw), "manifest")
    _expect(
        payload["schema_version"],
        "first_layer_composer_v2_matched_placebo_execution_manifest.v1",
        "manifest.schema_version",
    )
    _expect(payload["manifest_id"], path.parent.name, "manifest.manifest_id")
    _expect(payload["task_id"], TASK_ID, "manifest.task_id")
    _expect(payload["status"], "FROZEN_READY_FOR_SINGLE_DISPATCH", "manifest.status")
    auth_binding = _mapping(payload["authorization_binding"], "manifest.authorization")
    _expect(auth_binding["path"], DEFAULT_AUTHORIZATION_PATH.as_posix(), "auth.path")
    _expect(auth_binding["file_sha256"], auth.file_sha256, "auth.file_sha256")
    _expect(auth_binding["canonical_sha256"], auth.canonical_sha256, "auth.canonical")
    _expect(
        _mapping(payload["run_envelope"], "manifest.run_envelope"),
        EXPECTED_COUNTERS,
        "manifest.run_envelope",
    )
    for field, expected in (
        ("requested_start", REQUESTED_START.isoformat()),
        ("requested_end", REQUESTED_END.isoformat()),
        ("evaluated_start", REQUESTED_START.isoformat()),
        ("evaluated_end", REQUESTED_END.isoformat()),
        ("expected_signal_sessions", EXPECTED_SESSIONS),
        ("expected_return_intervals", EXPECTED_INTERVALS),
        ("expected_long_intervals", EXPECTED_LONG_INTERVALS),
        ("expected_long_episodes", EXPECTED_LONG_EPISODES),
        ("random_seed", RANDOM_SEED),
        ("placebo_draws", PLACEBO_DRAWS),
    ):
        _expect(payload[field], expected, f"manifest.{field}")
    code = _mapping(payload["code_binding"], "manifest.code_binding")
    module = f1._bound_file(str(code["module_path"]), root=project_root, label="module")
    _expect(_sha256_path(module), code["module_sha256"], "code.module_sha256")
    if len(str(code["implementation_commit_sha"])) != 40:
        raise MatchedPlaceboExecutionError("MPF_IDENTITY_MISMATCH", "implementation SHA")
    raw_bindings = payload["input_bindings"]
    if not isinstance(raw_bindings, list):
        raise MatchedPlaceboExecutionError("MPF_SCHEMA_INVALID", "input_bindings")
    bindings: list[f1.InputBinding] = []
    allowlist = _mapping(auth.payload["input_allowlist"], "authorization.input_allowlist")
    for raw_binding in raw_bindings:
        binding = _mapping(raw_binding, "input_binding")
        _expect(set(binding), {"role", "path", "sha256", "size_bytes"}, "binding.fields")
        parsed = f1.InputBinding(
            role=str(binding["role"]),
            path=str(binding["path"]),
            sha256=str(binding["sha256"]),
            size_bytes=int(binding["size_bytes"]),
        )
        allowed = _mapping(allowlist[parsed.role], f"allowlist.{parsed.role}")
        for field in ("path", "sha256", "size_bytes"):
            _expect(getattr(parsed, field), allowed[field], f"input.{parsed.role}.{field}")
        bindings.append(parsed)
    _expect(tuple(binding.role for binding in bindings), INPUT_ROLES, "manifest.input_roles")
    return f1.LoadedManifest(
        payload=payload,
        path=resolved,
        file_sha256=f1._sha256_bytes(raw),
        canonical_sha256=_canonical_sha256(payload),
        inputs=tuple(bindings),
    )


def replay_execution_manifest(
    manifest: f1.LoadedManifest, *, project_root: Path = PROJECT_ROOT
) -> Mapping[str, object]:
    replay = dict(f1.replay_execution_manifest(manifest, project_root=project_root))
    replay["schema_version"] = "first_layer_matched_placebo_manifest_replay.v1"
    return replay


def _failure_result(
    *, code: str, message: str, counters: Mapping[str, int], runtime_git_head: str
) -> Mapping[str, object]:
    return {
        "schema_version": "first_layer_composer_v2_matched_placebo_result.v1",
        "task_id": TASK_ID,
        "status": "TERMINAL",
        "technical_validation_state": "INVALID",
        "reducer_status": "INVALID",
        "reason_codes": [code],
        "failure": {"code": code, "message": message},
        "requested_range": {
            "start": REQUESTED_START.isoformat(),
            "end": REQUESTED_END.isoformat(),
        },
        "evaluated_range": None,
        "runtime_git_head": runtime_git_head,
        "actual_counters": dict(counters),
        "authorization_state": AUTHORIZATION_STATE,
        "aggregate_result_only": True,
        "raw_market_payload_exported": False,
        "raw_signal_payload_exported": False,
        "placebo_paths_exported": False,
        "trading_2557_status": "INSUFFICIENT",
        "qqq_options_wave_b": "HOLD",
        "qqq_options_wave_c": "NOT_AUTHORIZED",
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def execute_matched_placebo_falsification(
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
        raise MatchedPlaceboExecutionError("MPF_ATTEMPT_ALREADY_CONSUMED", str(target))
    counters = {key: 0 for key in EXPECTED_COUNTERS}
    runtime_git_head = f1._git_head(root)
    f1._write_once(
        attempt_path,
        {
            "schema_version": "first_layer_matched_placebo_attempt.v1",
            "task_id": TASK_ID,
            "status": "DISPATCHED_SINGLE_ATTEMPT_RESERVED",
            "authorization_state": AUTHORIZATION_STATE,
            "authorized_maxima": EXPECTED_COUNTERS,
            "runtime_git_head": runtime_git_head,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    try:
        policy = load_preregistration(project_root=root)
        authorization = load_run_authorization(project_root=root, policy=policy)
        manifest = load_execution_manifest(
            manifest_path, project_root=root, authorization=authorization
        )
        counters["manifest_replays"] = 1
        replay = replay_execution_manifest(manifest, project_root=root)
        f1._write_once(target / "manifest_replay_receipt.json", replay)
        bindings = f1._bindings(manifest)
        counters["canonical_dq_runs"] = 1
        dq = f1.run_canonical_data_quality_execution(
            f1.CanonicalDataQualityExecutionRequest(
                as_of=REQUESTED_END,
                requested_window=f1.DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
                evaluated_window=f1.DataQualityDateWindow(start=REQUESTED_START, end=REQUESTED_END),
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
            "schema_version": "first_layer_matched_placebo_dq_receipt.v1",
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
        f1._write_once(target / "canonical_dq_receipt.json", dq_receipt)
        if dq.report.status != "PASS":
            raise MatchedPlaceboExecutionError("MPF_DQ_OR_PIT_NOT_PASS", dq.report.status)

        counters["local_matched_placebo_runs"] = 1
        plan = f1.load_diagnostic_plan(manifest, project_root=root)
        shape = extract_exposure_shape(plan.interval_targets)
        _expect(len(plan.sessions), EXPECTED_SESSIONS, "plan.signal_sessions")
        _expect(shape.interval_count, EXPECTED_INTERVALS, "plan.return_intervals")
        _expect(shape.long_interval_count, EXPECTED_LONG_INTERVALS, "plan.long_intervals")
        _expect(len(shape.long_run_lengths), EXPECTED_LONG_EPISODES, "plan.long_episodes")
        _expect(
            plan.comparator_weight,
            EXPECTED_LONG_INTERVALS / EXPECTED_INTERVALS,
            "plan.comparator_weight",
        )
        distribution = build_placebo_distribution(
            plan.qqq_prices,
            plan.interval_targets,
            plan.comparator_weight,
            seed=RANDOM_SEED,
            draws=PLACEBO_DRAWS,
        )
        prior_primary = _trading_2557_primary_result(project_root=root)
        accounting_reconciliation = {
            "observed_candidate_net_total_return_abs_diff": abs(
                distribution.observed_excess_percentage_points
                + distribution.comparator_net_total_return_pct
                - float(prior_primary["candidate_net_total_return_pct"])
            ),
            "observed_candidate_max_drawdown_abs_diff": abs(
                distribution.observed_max_drawdown_magnitude_pct
                - float(prior_primary["candidate_max_drawdown_magnitude_pct"])
            ),
            "comparator_net_total_return_abs_diff": abs(
                distribution.comparator_net_total_return_pct
                - float(prior_primary["comparator_net_total_return_pct"])
            ),
            "observed_paired_excess_abs_diff": abs(
                distribution.observed_excess_percentage_points
                - float(prior_primary["paired_excess_percentage_points"])
            ),
        }
        if any(value > f1.RECONCILIATION_TOLERANCE for value in accounting_reconciliation.values()):
            raise MatchedPlaceboExecutionError(
                "MPF_TRADING_2557_ACCOUNTING_DRIFT",
                json.dumps(accounting_reconciliation, sort_keys=True),
            )
        summary = summarize_distribution(distribution)

        counters["independent_replays"] = 1
        independent = independently_replay_distribution(
            plan.qqq_prices,
            plan.interval_targets,
            plan.comparator_weight,
            distribution,
        )
        f1._write_once(target / "independent_replay_receipt.json", independent)
        result: Mapping[str, object] = {
            "schema_version": "first_layer_composer_v2_matched_placebo_result.v1",
            "task_id": TASK_ID,
            "status": "TERMINAL",
            "technical_validation_state": "PASS",
            "reducer_status": summary["reducer_status"],
            "authorization_state": AUTHORIZATION_STATE,
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "prior_visibility": "PARTIAL_PRIOR_VISIBILITY",
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
            "return_interval_count": shape.interval_count,
            "signal_lag_sessions": 1,
            "long_interval_count": shape.long_interval_count,
            "long_episode_count": len(shape.long_run_lengths),
            "leading_flat_boundary_count": shape.leading_flat_count,
            "trailing_flat_boundary_count": shape.trailing_flat_count,
            "accounting_trade_event_count": shape.accounting_trade_event_count,
            "long_run_length_multiset": sorted(shape.long_run_lengths),
            "interior_flat_gap_length_multiset": sorted(shape.interior_flat_gap_lengths),
            "exposure_matched_comparator_weight": plan.comparator_weight,
            "primary_one_way_cost_bps": ONE_WAY_COST_BPS,
            "random_seed": RANDOM_SEED,
            "placebo_draws": PLACEBO_DRAWS,
            "observed_5_bps_paired_excess_percentage_points": (
                distribution.observed_excess_percentage_points
            ),
            "observed_candidate_net_total_return_pct": (
                distribution.observed_excess_percentage_points
                + distribution.comparator_net_total_return_pct
            ),
            "comparator_net_total_return_pct": (distribution.comparator_net_total_return_pct),
            "trading_2557_accounting_reconciliation": accounting_reconciliation,
            "matched_placebo": summary,
            "manifest_file_sha256": manifest.file_sha256,
            "manifest_canonical_sha256": manifest.canonical_sha256,
            "authorization_file_sha256": authorization.file_sha256,
            "authorization_canonical_sha256": authorization.canonical_sha256,
            "policy_file_sha256": policy.file_sha256,
            "policy_canonical_sha256": policy.canonical_sha256,
            "canonical_dq": dq_receipt,
            "independent_replay": independent,
            "runtime_git_head": runtime_git_head,
            "actual_counters": counters,
            "aggregate_result_only": True,
            "raw_market_payload_exported": False,
            "raw_signal_payload_exported": False,
            "placebo_paths_exported": False,
            "trading_2557_status": "INSUFFICIENT",
            "qqq_options_wave_b": "HOLD",
            "qqq_options_wave_c": "NOT_AUTHORIZED",
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
        f1._write_once(result_path, result)
        return result
    except Exception as exc:
        if isinstance(exc, MatchedPlaceboExecutionError):
            code, message = exc.code, exc.message
        elif isinstance(exc, f1.FoundationalFalsificationExecutionError):
            code, message = exc.code, exc.message
        else:
            code, message = "MPF_UNEXPECTED_FAILURE", str(exc)
        failure = _failure_result(
            code=code,
            message=message,
            counters=counters,
            runtime_git_head=runtime_git_head,
        )
        f1._write_once(target / "failure_receipt.json", failure)
        f1._write_once(result_path, failure)
        return failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute_matched_placebo_falsification(
        args.manifest, output_dir=args.output_dir, project_root=args.project_root
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if result.get("technical_validation_state") == "PASS" else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


__all__ = [
    "DEFAULT_AUTHORIZATION_PATH",
    "DEFAULT_MANIFEST_PATH",
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_POLICY_PATH",
    "ExposureShape",
    "MatchedPlaceboExecutionError",
    "PlaceboDistribution",
    "build_placebo_distribution",
    "execute_matched_placebo_falsification",
    "extract_exposure_shape",
    "independently_replay_distribution",
    "load_execution_manifest",
    "load_preregistration",
    "load_run_authorization",
    "reconstruct_placebo_targets",
    "summarize_distribution",
]
