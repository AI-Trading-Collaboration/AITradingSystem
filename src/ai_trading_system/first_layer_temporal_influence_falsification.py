"""Run bounded temporal-displacement and episode-influence falsification.

The runner is deliberately local and aggregate-only.  It reuses the frozen
first-layer signal and canonical inputs, performs one canonical DQ execution,
one deterministic diagnostic run, and one independent accounting replay.  It
has no downloader, provider, option, broker, production, or trading surface.
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
from ai_trading_system import first_layer_matched_placebo_falsification as mp
from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = f1.PROJECT_ROOT
DEFAULT_POLICY_PATH = Path(
    "config/research/first_layer_composer_v2_temporal_influence_preregistration_v1.yaml"
)
DEFAULT_AUTHORIZATION_PATH = Path(
    "config/research/first_layer_composer_v2_temporal_influence_run_authorization_v1.yaml"
)
FAILURE_FIX_AUTHORIZATION_PATH = Path(
    "config/research/first_layer_composer_v2_temporal_influence_"
    "failure_fix_run_authorization_v1.yaml"
)
DEFAULT_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_temporal_influence_v1/execution_manifest.json"
)
FAILURE_FIX_MANIFEST_PATH = Path(
    "inputs/research/first_layer_composer_v2_temporal_influence_"
    "failure_fix_v1/execution_manifest.json"
)
DEFAULT_OUTPUT_DIR = Path(
    "outputs/research/first_layer_composer_v2_temporal_influence_v1"
)
FAILURE_FIX_OUTPUT_DIR = Path(
    "outputs/research/first_layer_composer_v2_temporal_influence_failure_fix_v1"
)

TASK_ID = "TRADING-2559_FIRST_LAYER_COMPOSER_V2_TEMPORAL_INFLUENCE_FALSIFICATION_V1"
AUTHORIZATION_STATUS = "OWNER_STANDING_SCOPE_BOUNDED_TEMPORAL_INFLUENCE_AUTHORIZED"
AUTHORIZATION_STATE = "STANDING_OWNER_SCOPE"
OWNER_DECISION_REF = "owner_instruction:TRADING-2559:2026-09-03:continue_low_cost_validation"
FAILURE_FIX_AUTHORIZATION_STATUS = (
    "OWNER_EXACT_PREAUTHORIZED_TEMPORAL_INFLUENCE_FAILURE_FIX"
)
FAILURE_FIX_AUTHORIZATION_STATE = "EXACT_PREAUTHORIZED"
FAILURE_FIX_OWNER_DECISION_REF = (
    "owner_instruction:TRADING-2559:2026-09-03:failure_fix_exact_approved"
)
REQUESTED_START = f1.REQUESTED_START
REQUESTED_END = f1.REQUESTED_END
EXPECTED_SESSIONS = f1.EXPECTED_SESSIONS
EXPECTED_INTERVALS = f1.EXPECTED_INTERVALS
EXPECTED_LONG_INTERVALS = mp.EXPECTED_LONG_INTERVALS
EXPECTED_LONG_EPISODES = mp.EXPECTED_LONG_EPISODES
ONE_WAY_COST_BPS = 5.0
EXPECTED_PRIMARY_EXCESS_PP = 13.745976956735628
SHIFT_SESSIONS = (-10, -5, -2, -1, 0, 1, 2, 5, 10)
MAX_SHIFT = 10
COMMON_START = MAX_SHIFT
COMMON_END_EXCLUSIVE = EXPECTED_INTERVALS - MAX_SHIFT
EXPECTED_COMMON_INTERVALS = EXPECTED_INTERVALS - 2 * MAX_SHIFT
RECONCILIATION_TOLERANCE = 1e-10
TIE_TOLERANCE = 1e-12

TRADING_2558_PATHS = {
    "trading_2558_module": Path(
        "src/ai_trading_system/first_layer_matched_placebo_falsification.py"
    ),
    "trading_2558_authorization": Path(
        "config/research/first_layer_composer_v2_matched_placebo_run_authorization_v1.yaml"
    ),
    "trading_2558_manifest": Path(
        "inputs/research/first_layer_composer_v2_matched_placebo_v1/execution_manifest.json"
    ),
    "trading_2558_aggregate_result": Path(
        "outputs/research/first_layer_composer_v2_matched_placebo_v1/aggregate_result.json"
    ),
    "trading_2558_result_admission": Path(
        "config/research/first_layer_composer_v2_matched_placebo_result_admission_v1.yaml"
    ),
}

INPUT_ROLES = (
    "temporal_influence_preregistration",
    *mp.INPUT_ROLES,
    *TRADING_2558_PATHS,
)

EXPECTED_COUNTERS = {
    "manifest_replays": 1,
    "canonical_dq_runs": 1,
    "local_temporal_influence_runs": 1,
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


class TemporalInfluenceExecutionError(ValueError):
    """Stable fail-closed error for the temporal/influence execution."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


@dataclass(frozen=True)
class LoadedPolicy:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise TemporalInfluenceExecutionError(
            "TIF_IDENTITY_MISMATCH",
            f"{label}: expected={expected!r} actual={actual!r}",
        )


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise TemporalInfluenceExecutionError(
            "TIF_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _canonical_sha256(value: object) -> str:
    return f1._sha256_bytes(f1._canonical_json_bytes(value))


def _trade_event_count(targets: Sequence[float]) -> int:
    position = 0.0
    count = 0
    for target in targets:
        if target not in (0.0, 1.0):
            raise TemporalInfluenceExecutionError("TIF_TARGET_INVALID", str(target))
        if float(target) != position:
            count += 1
            position = float(target)
    if position == 1.0:
        count += 1
    return count


def load_preregistration(
    path: Path = DEFAULT_POLICY_PATH, *, project_root: Path = PROJECT_ROOT
) -> LoadedPolicy:
    resolved = f1._bound_file(path, root=project_root, label="temporal_influence_policy")
    raw = resolved.read_bytes()
    payload = _mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "policy"
    )
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
            "temporal_displacement_contract",
            "episode_influence_contract",
            "pilot_reducer",
            "run_envelope",
            "safety",
        ),
        "policy.root_fields",
    )
    _expect(
        payload["schema_version"],
        "first_layer_composer_v2_temporal_influence_preregistration.v1",
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
    _expect(payload["owner"], "project_owner", "policy.owner")
    _expect(payload["approval_ref"], OWNER_DECISION_REF, "policy.approval_ref")
    _expect(
        _mapping(payload["known_result_boundary"], "policy.known_result_boundary"),
        {
            "result_visibility": "PARTIAL_PRIOR_VISIBILITY",
            "historical_window_role": "REUSED_DEVELOPMENT_CONFIRMATION",
            "pristine_out_of_sample_claim_allowed": False,
            "trading_2557_result_known": True,
            "trading_2558_result_known": True,
            "temporal_and_influence_results_visible_before_freeze": False,
            "post_result_parameter_rescue_allowed": False,
        },
        "policy.known_result_boundary",
    )
    _expect(
        _mapping(payload["primary_identity"], "policy.primary_identity"),
        {
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
            "primary_one_way_cost_bps": ONE_WAY_COST_BPS,
            "primary_idle_cash_asset": "ZERO_RETURN_CASH",
            "expected_primary_paired_excess_percentage_points": EXPECTED_PRIMARY_EXCESS_PP,
        },
        "policy.primary_identity",
    )
    _expect(
        _mapping(
            payload["temporal_displacement_contract"],
            "policy.temporal_displacement_contract",
        ),
        {
            "shift_sessions": list(SHIFT_SESSIONS),
            "maximum_absolute_shift_sessions": MAX_SHIFT,
            "common_interval_index_start_inclusive": COMMON_START,
            "common_interval_index_end_exclusive": COMMON_END_EXCLUSIVE,
            "expected_common_return_intervals": EXPECTED_COMMON_INTERVALS,
            "target_formula": (
                "SHIFTED_TARGET_AT_I_EQUALS_FROZEN_TARGET_AT_I_MINUS_SHIFT"
            ),
            "negative_shift_role": "NON_CAUSAL_ANTICIPATORY_CONTROL_ONLY",
            "positive_shift_role": (
                "ADDITIONAL_EXECUTION_DELAY_AFTER_EXISTING_ONE_SESSION_LAG"
            ),
            "comparator_weight_formula": "SHIFT_PATH_LONG_COUNT_DIVIDED_BY_1181",
            "candidate_and_comparator_cost_bps": ONE_WAY_COST_BPS,
            "best_shift_tie_break": "SMALLEST_NUMERIC_SHIFT",
            "parameter_search_allowed": False,
        },
        "policy.temporal_displacement_contract",
    )
    _expect(
        _mapping(
            payload["episode_influence_contract"],
            "policy.episode_influence_contract",
        ),
        {
            "construction": "SET_ONE_CONTIGUOUS_LONG_EPISODE_TO_FLAT",
            "expected_episode_count": EXPECTED_LONG_EPISODES,
            "all_other_targets_unchanged": True,
            "comparator_weight_formula": "REMAINING_LONG_COUNT_DIVIDED_BY_1201",
            "candidate_and_comparator_cost_bps": ONE_WAY_COST_BPS,
            "original_accounting_reconciliation_tolerance": RECONCILIATION_TOLERANCE,
            "independent_replay_tolerance": RECONCILIATION_TOLERANCE,
            "concentration_boundary": (
                "ANY_REMATCHED_LEAVE_ONE_EPISODE_PAIRED_EXCESS_"
                "LESS_THAN_OR_EQUAL_TO_ZERO"
            ),
            "parameter_search_allowed": False,
        },
        "policy.episode_influence_contract",
    )
    _expect(
        _mapping(payload["pilot_reducer"], "policy.pilot_reducer"),
        {
            "status": "TEMPORARY_PILOT_BASELINE",
            "owner_reviewer": "Project Owner / strategy research",
            "rationale": (
                "Natural zero-excess falsification boundaries and a floating-point-only "
                "tie tolerance; not an investment gate."
            ),
            "review_condition": "Review after the write-once result is available.",
            "exit_condition": (
                "Proceed to separately preregistered feature ablation only if useful, "
                "or wait for prospective OOS."
            ),
            "precedence": [
                "INVALID",
                "SINGLE_EPISODE_DEPENDENT",
                "ONE_SESSION_DELAY_FRAGILE",
                "ANTICIPATORY_ALIGNMENT_DOMINATES",
                "LOW_COST_ROBUSTNESS_NOT_DISCONFIRMED_DIAGNOSTIC_ONLY",
            ],
            "single_episode_dependent_if": (
                "any_rematched_leave_one_episode_paired_excess_"
                "less_than_or_equal_to_zero"
            ),
            "one_session_delay_fragile_if": (
                "shift_plus_one_paired_excess_less_than_or_equal_to_zero"
            ),
            "anticipatory_alignment_dominates_if": (
                "best_shift_is_negative_and_exceeds_shift_zero_by_more_than_1e_minus_12"
            ),
            "prior_trading_2557_status_must_remain": "INSUFFICIENT",
            "prior_trading_2558_status_must_remain": (
                "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO"
            ),
            "research_priority_increase_allowed": False,
            "qqq_options_wave_b": "HOLD",
            "qqq_options_wave_c": "NOT_AUTHORIZED",
            "production_allowed": False,
        },
        "policy.pilot_reducer",
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
            "shifted_target_payload_export_allowed",
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
        "shifted_target_payload_export_allowed",
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


def _validate_prior_result(*, authorization: f1.LoadedAuthorization, project_root: Path) -> None:
    allowlist = _mapping(authorization.payload["input_allowlist"], "authorization.input_allowlist")
    for role, expected_path in TRADING_2558_PATHS.items():
        row = _mapping(allowlist[role], f"allowlist.{role}")
        _expect(row["path"], expected_path.as_posix(), f"allowlist.{role}.path")
        path = f1._bound_file(expected_path, root=project_root, label=role)
        _expect(f1._sha256_path(path), row["sha256"], f"allowlist.{role}.sha256")
        _expect(path.stat().st_size, row["size_bytes"], f"allowlist.{role}.size")
    result_path = project_root / TRADING_2558_PATHS["trading_2558_aggregate_result"]
    result = _mapping(json.loads(result_path.read_bytes()), "trading_2558_result")
    _expect(result["task_id"], mp.TASK_ID, "trading_2558.task_id")
    _expect(result["technical_validation_state"], "PASS", "trading_2558.validation")
    _expect(
        result["reducer_status"],
        "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO",
        "trading_2558.reducer",
    )
    _expect(
        result["observed_5_bps_paired_excess_percentage_points"],
        EXPECTED_PRIMARY_EXCESS_PP,
        "trading_2558.primary_excess",
    )
    _expect(result["actual_counters"], mp.EXPECTED_COUNTERS, "trading_2558.counters")
    _expect(result["production_effect"], "none", "trading_2558.production_effect")
    _expect(result["broker_action"], "none", "trading_2558.broker_action")


def _trading_2558_observed_max_drawdown(result: Mapping[str, object]) -> float:
    matched = _mapping(result["matched_placebo"], "trading_2558.matched_placebo")
    return float(matched["observed_max_drawdown_magnitude_pct"])


def _authorization_profile(path: Path) -> tuple[str, str, str, str]:
    if path == DEFAULT_AUTHORIZATION_PATH:
        return (
            "first_layer_composer_v2_temporal_influence_run_authorization.v1",
            AUTHORIZATION_STATUS,
            AUTHORIZATION_STATE,
            OWNER_DECISION_REF,
        )
    if path == FAILURE_FIX_AUTHORIZATION_PATH:
        return (
            "first_layer_composer_v2_temporal_influence_failure_fix_run_authorization.v1",
            FAILURE_FIX_AUTHORIZATION_STATUS,
            FAILURE_FIX_AUTHORIZATION_STATE,
            FAILURE_FIX_OWNER_DECISION_REF,
        )
    raise TemporalInfluenceExecutionError(
        "TIF_AUTHORIZATION_PATH_INVALID", path.as_posix()
    )


def load_run_authorization(
    path: Path = DEFAULT_AUTHORIZATION_PATH,
    *,
    project_root: Path = PROJECT_ROOT,
    policy: LoadedPolicy | None = None,
) -> f1.LoadedAuthorization:
    loaded_policy = policy or load_preregistration(project_root=project_root)
    expected_schema, expected_status, expected_state, expected_decision_ref = (
        _authorization_profile(path)
    )
    resolved = f1._bound_file(path, root=project_root, label="authorization")
    raw = resolved.read_bytes()
    payload = _mapping(
        load_strict_yaml_text(raw.decode("utf-8"), label=path.as_posix()), "authorization"
    )
    _expect(
        payload["schema_version"],
        expected_schema,
        "authorization.schema_version",
    )
    _expect(payload["authorization_id"], path.stem, "authorization.authorization_id")
    _expect(payload["authorization_version"], "1.0.0", "authorization.version")
    _expect(payload["status"], expected_status, "authorization.status")
    _expect(payload["task_id"], TASK_ID, "authorization.task_id")
    _expect(payload["scope"], "R1_BOUNDED_RESEARCH_SANDBOX", "authorization.scope")
    owner = _mapping(payload["owner_decision"], "authorization.owner_decision")
    _expect(owner["decision_ref"], expected_decision_ref, "authorization.decision_ref")
    _expect(owner["authorization_state"], expected_state, "authorization.state")
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
        "local_temporal_influence_run_authorized",
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
    loaded = f1.LoadedAuthorization(
        payload=payload,
        path=resolved,
        file_sha256=f1._sha256_bytes(raw),
        canonical_sha256=_canonical_sha256(payload),
    )
    _validate_prior_result(authorization=loaded, project_root=project_root)
    return loaded


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
        "first_layer_composer_v2_temporal_influence_execution_manifest.v1",
        "manifest.schema_version",
    )
    _expect(payload["manifest_id"], path.parent.name, "manifest.manifest_id")
    _expect(payload["task_id"], TASK_ID, "manifest.task_id")
    _expect(payload["status"], "FROZEN_READY_FOR_SINGLE_DISPATCH", "manifest.status")
    auth_binding = _mapping(payload["authorization_binding"], "manifest.authorization")
    _expect(
        auth_binding["path"],
        auth.path.relative_to(project_root.resolve()).as_posix(),
        "auth.path",
    )
    _expect(auth_binding["file_sha256"], auth.file_sha256, "auth.file_sha256")
    _expect(auth_binding["canonical_sha256"], auth.canonical_sha256, "auth.canonical")
    _expect(
        _mapping(payload["run_envelope"], "manifest.run_envelope"),
        EXPECTED_COUNTERS,
        "manifest.run_envelope",
    )
    expected_scalars = (
        ("requested_start", REQUESTED_START.isoformat()),
        ("requested_end", REQUESTED_END.isoformat()),
        ("evaluated_start", REQUESTED_START.isoformat()),
        ("evaluated_end", REQUESTED_END.isoformat()),
        ("expected_signal_sessions", EXPECTED_SESSIONS),
        ("expected_return_intervals", EXPECTED_INTERVALS),
        ("expected_long_intervals", EXPECTED_LONG_INTERVALS),
        ("expected_long_episodes", EXPECTED_LONG_EPISODES),
        ("common_start_index", COMMON_START),
        ("common_end_exclusive", COMMON_END_EXCLUSIVE),
        ("expected_common_return_intervals", EXPECTED_COMMON_INTERVALS),
    )
    for field, expected in expected_scalars:
        _expect(payload[field], expected, f"manifest.{field}")
    _expect(payload["shift_sessions"], list(SHIFT_SESSIONS), "manifest.shift_sessions")
    code = _mapping(payload["code_binding"], "manifest.code_binding")
    module = f1._bound_file(str(code["module_path"]), root=project_root, label="module")
    _expect(f1._sha256_path(module), code["module_sha256"], "code.module_sha256")
    if len(str(code["implementation_commit_sha"])) != 40:
        raise TemporalInfluenceExecutionError("TIF_IDENTITY_MISMATCH", "implementation SHA")
    raw_bindings = payload["input_bindings"]
    if not isinstance(raw_bindings, list):
        raise TemporalInfluenceExecutionError("TIF_SCHEMA_INVALID", "input_bindings")
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
    replay["schema_version"] = "first_layer_temporal_influence_manifest_replay.v1"
    return replay


def _path_metrics(prices: Sequence[float], targets: Sequence[float]) -> dict[str, object]:
    values = tuple(float(value) for value in targets)
    if not values or len(prices) != len(values) + 1:
        raise TemporalInfluenceExecutionError("TIF_PATH_INVALID", "length mismatch")
    if any(value not in (0.0, 1.0) for value in values):
        raise TemporalInfluenceExecutionError("TIF_TARGET_INVALID", "non-binary target")
    weight = math.fsum(values) / len(values)
    candidate = f1.candidate_return_path(
        prices, values, one_way_cost_bps=ONE_WAY_COST_BPS
    )
    comparator = f1.comparator_return_path(
        prices, weight, one_way_cost_bps=ONE_WAY_COST_BPS
    )
    return {
        "interval_count": len(values),
        "long_interval_count": int(math.fsum(values)),
        "trade_event_count": _trade_event_count(values),
        "exposure_matched_comparator_weight": weight,
        "candidate_net_total_return_pct": candidate.net_total_return_pct,
        "comparator_net_total_return_pct": comparator.net_total_return_pct,
        "paired_excess_percentage_points": (
            candidate.net_total_return_pct - comparator.net_total_return_pct
        ),
        "candidate_max_drawdown_magnitude_pct": candidate.max_drawdown_magnitude_pct,
        "comparator_max_drawdown_magnitude_pct": comparator.max_drawdown_magnitude_pct,
    }


def _episode_bounds(targets: Sequence[float]) -> tuple[tuple[int, int], ...]:
    values = tuple(float(value) for value in targets)
    bounds: list[tuple[int, int]] = []
    start: int | None = None
    for index, target in enumerate((*values, 0.0)):
        if target == 1.0 and start is None:
            start = index
        elif target == 0.0 and start is not None:
            bounds.append((start, index - 1))
            start = None
    if len(bounds) != EXPECTED_LONG_EPISODES:
        raise TemporalInfluenceExecutionError(
            "TIF_EPISODE_INVENTORY_INVALID", f"episodes={len(bounds)}"
        )
    return tuple(bounds)


def build_temporal_displacement(
    plan: f1.DiagnosticPlan,
) -> tuple[tuple[dict[str, object], ...], str]:
    if len(plan.interval_targets) != EXPECTED_INTERVALS:
        raise TemporalInfluenceExecutionError("TIF_INTERVAL_COUNT_INVALID", "temporal")
    prices = plan.qqq_prices[COMMON_START : COMMON_END_EXCLUSIVE + 1]
    rows: list[dict[str, object]] = []
    digest = hashlib.sha256()
    for shift in SHIFT_SESSIONS:
        targets = tuple(
            plan.interval_targets[index - shift]
            for index in range(COMMON_START, COMMON_END_EXCLUSIVE)
        )
        if len(targets) != EXPECTED_COMMON_INTERVALS:
            raise TemporalInfluenceExecutionError("TIF_COMMON_WINDOW_INVALID", str(shift))
        digest.update(
            f1._canonical_json_bytes(
                {"shift_sessions": shift, "targets": [int(value) for value in targets]}
            )
        )
        role = (
            "NON_CAUSAL_ANTICIPATORY_CONTROL_ONLY"
            if shift < 0
            else (
                "FROZEN_EXECUTABLE_ALIGNMENT"
                if shift == 0
                else "ADDITIONAL_EXECUTION_DELAY"
            )
        )
        rows.append({"shift_sessions": shift, "role": role, **_path_metrics(prices, targets)})
    return tuple(rows), digest.hexdigest()


def build_episode_influence(
    plan: f1.DiagnosticPlan,
) -> tuple[dict[str, object], tuple[dict[str, object], ...], str]:
    baseline = _path_metrics(plan.qqq_prices, plan.interval_targets)
    rows: list[dict[str, object]] = []
    digest = hashlib.sha256()
    for episode_id, (start, end) in enumerate(_episode_bounds(plan.interval_targets), start=1):
        targets = list(plan.interval_targets)
        targets[start : end + 1] = [0.0] * (end - start + 1)
        digest.update(
            f1._canonical_json_bytes(
                {"episode_id": episode_id, "targets": [int(value) for value in targets]}
            )
        )
        metrics = _path_metrics(plan.qqq_prices, targets)
        excess = float(metrics["paired_excess_percentage_points"])
        baseline_excess = float(baseline["paired_excess_percentage_points"])
        rows.append(
            {
                "episode_id": episode_id,
                "start_session": plan.sessions[start].isoformat(),
                "end_session": plan.sessions[end + 1].isoformat(),
                "removed_interval_count": end - start + 1,
                **metrics,
                "paired_excess_change_from_original_percentage_points": (
                    excess - baseline_excess
                ),
                "paired_excess_drop_from_original_percentage_points": (
                    baseline_excess - excess
                ),
            }
        )
    return baseline, tuple(rows), digest.hexdigest()


def summarize_diagnostics(
    temporal_rows: Sequence[Mapping[str, object]],
    episode_rows: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    if tuple(int(row["shift_sessions"]) for row in temporal_rows) != SHIFT_SESSIONS:
        raise TemporalInfluenceExecutionError("TIF_SHIFT_INVENTORY_INVALID", "order")
    if len(episode_rows) != EXPECTED_LONG_EPISODES:
        raise TemporalInfluenceExecutionError("TIF_EPISODE_INVENTORY_INVALID", "summary")
    by_shift = {int(row["shift_sessions"]): row for row in temporal_rows}
    best = max(
        temporal_rows,
        key=lambda row: (
            float(row["paired_excess_percentage_points"]),
            -int(row["shift_sessions"]),
        ),
    )
    baseline = by_shift[0]
    delayed_one = by_shift[1]
    nonpositive = [
        row
        for row in episode_rows
        if float(row["paired_excess_percentage_points"]) <= 0.0
    ]
    reason_codes: list[str] = []
    if nonpositive:
        reason_codes.append("SINGLE_EPISODE_DEPENDENT")
    if float(delayed_one["paired_excess_percentage_points"]) <= 0.0:
        reason_codes.append("ONE_SESSION_DELAY_FRAGILE")
    anticipatory_dominates = (
        int(best["shift_sessions"]) < 0
        and float(best["paired_excess_percentage_points"])
        - float(baseline["paired_excess_percentage_points"])
        > TIE_TOLERANCE
    )
    if anticipatory_dominates:
        reason_codes.append("ANTICIPATORY_ALIGNMENT_DOMINATES")
    if "SINGLE_EPISODE_DEPENDENT" in reason_codes:
        reducer = "SINGLE_EPISODE_DEPENDENT"
    elif "ONE_SESSION_DELAY_FRAGILE" in reason_codes:
        reducer = "ONE_SESSION_DELAY_FRAGILE"
    elif "ANTICIPATORY_ALIGNMENT_DOMINATES" in reason_codes:
        reducer = "ANTICIPATORY_ALIGNMENT_DOMINATES"
    else:
        reducer = "LOW_COST_ROBUSTNESS_NOT_DISCONFIRMED_DIAGNOSTIC_ONLY"
        reason_codes.append(reducer)
    largest_drop = max(
        episode_rows,
        key=lambda row: (
            float(row["paired_excess_drop_from_original_percentage_points"]),
            -int(row["episode_id"]),
        ),
    )
    minimum_episode = min(
        episode_rows,
        key=lambda row: (
            float(row["paired_excess_percentage_points"]),
            int(row["episode_id"]),
        ),
    )
    return {
        "reducer_status": reducer,
        "reason_codes": reason_codes,
        "best_shift_sessions": int(best["shift_sessions"]),
        "best_shift_paired_excess_percentage_points": float(
            best["paired_excess_percentage_points"]
        ),
        "shift_zero_paired_excess_percentage_points": float(
            baseline["paired_excess_percentage_points"]
        ),
        "shift_plus_one_paired_excess_percentage_points": float(
            delayed_one["paired_excess_percentage_points"]
        ),
        "anticipatory_alignment_dominates": anticipatory_dominates,
        "nonpositive_leave_one_episode_count": len(nonpositive),
        "nonpositive_leave_one_episode_ids": [int(row["episode_id"]) for row in nonpositive],
        "minimum_leave_one_episode_id": int(minimum_episode["episode_id"]),
        "minimum_leave_one_episode_paired_excess_percentage_points": float(
            minimum_episode["paired_excess_percentage_points"]
        ),
        "largest_excess_drop_episode_id": int(largest_drop["episode_id"]),
        "largest_excess_drop_percentage_points": float(
            largest_drop["paired_excess_drop_from_original_percentage_points"]
        ),
    }


def _independent_max_drawdown(curve: Sequence[float]) -> float:
    high = float(curve[0])
    worst = 0.0
    for value in curve:
        high = max(high, float(value))
        worst = max(worst, (high - float(value)) / high)
    return worst * 100.0


def _independent_metrics(
    prices: Sequence[float], targets: Sequence[float]
) -> dict[str, object]:
    cost = ONE_WAY_COST_BPS / 10_000.0
    values = tuple(float(value) for value in targets)
    wealth = f1.INITIAL_CAPITAL
    position = 0.0
    candidate_curve = [wealth]
    for index in range(len(values)):
        target = values[index]
        if target != position:
            wealth = wealth / (1.0 + cost) if target == 1.0 else wealth * (1.0 - cost)
            position = target
        if position == 1.0:
            wealth *= float(prices[index + 1]) / float(prices[index])
        if index == len(values) - 1 and position == 1.0:
            wealth *= 1.0 - cost
        candidate_curve.append(wealth)
    weight = math.fsum(values) / len(values)
    after_entry = f1.INITIAL_CAPITAL / (1.0 + cost * weight)
    qqq_value = after_entry * weight
    cash_value = after_entry * (1.0 - weight)
    comparator_curve = [f1.INITIAL_CAPITAL]
    for index in range(len(values)):
        qqq_value *= float(prices[index + 1]) / float(prices[index])
        equity = qqq_value + cash_value
        if index == len(values) - 1:
            equity -= cost * qqq_value
        comparator_curve.append(equity)
    candidate_return = (candidate_curve[-1] / f1.INITIAL_CAPITAL - 1.0) * 100.0
    comparator_return = (comparator_curve[-1] / f1.INITIAL_CAPITAL - 1.0) * 100.0
    return {
        "interval_count": len(values),
        "long_interval_count": int(math.fsum(values)),
        "trade_event_count": _trade_event_count(values),
        "exposure_matched_comparator_weight": weight,
        "candidate_net_total_return_pct": candidate_return,
        "comparator_net_total_return_pct": comparator_return,
        "paired_excess_percentage_points": candidate_return - comparator_return,
        "candidate_max_drawdown_magnitude_pct": _independent_max_drawdown(
            candidate_curve
        ),
        "comparator_max_drawdown_magnitude_pct": _independent_max_drawdown(
            comparator_curve
        ),
    }


def independently_replay_diagnostics(
    plan: f1.DiagnosticPlan,
    *,
    primary_baseline: Mapping[str, object],
    primary_temporal: Sequence[Mapping[str, object]],
    primary_episodes: Sequence[Mapping[str, object]],
    primary_summary: Mapping[str, object],
    primary_temporal_digest: str,
    primary_episode_digest: str,
) -> dict[str, object]:
    temporal_rows: list[dict[str, object]] = []
    temporal_digest = hashlib.sha256()
    common_prices = tuple(
        plan.qqq_prices[index]
        for index in range(COMMON_START, COMMON_END_EXCLUSIVE + 1)
    )
    for shift in SHIFT_SESSIONS:
        shifted = tuple(
            float(plan.interval_targets[index - shift])
            for index in range(COMMON_START, COMMON_END_EXCLUSIVE)
        )
        temporal_digest.update(
            f1._canonical_json_bytes(
                {"shift_sessions": shift, "targets": [int(value) for value in shifted]}
            )
        )
        temporal_rows.append(
            {"shift_sessions": shift, **_independent_metrics(common_prices, shifted)}
        )

    episode_rows: list[dict[str, object]] = []
    episode_digest = hashlib.sha256()
    values = tuple(float(value) for value in plan.interval_targets)
    bounds: list[tuple[int, int]] = []
    cursor = 0
    while cursor < len(values):
        if values[cursor] == 0.0:
            cursor += 1
            continue
        start = cursor
        while cursor < len(values) and values[cursor] == 1.0:
            cursor += 1
        bounds.append((start, cursor - 1))
    independent_baseline = _independent_metrics(plan.qqq_prices, values)
    for episode_id, (start, end) in enumerate(bounds, start=1):
        removed = tuple(
            0.0 if start <= index <= end else value
            for index, value in enumerate(values)
        )
        episode_digest.update(
            f1._canonical_json_bytes(
                {"episode_id": episode_id, "targets": [int(value) for value in removed]}
            )
        )
        metrics = _independent_metrics(plan.qqq_prices, removed)
        excess = float(metrics["paired_excess_percentage_points"])
        original = float(independent_baseline["paired_excess_percentage_points"])
        episode_rows.append(
            {
                "episode_id": episode_id,
                **metrics,
                "paired_excess_change_from_original_percentage_points": excess - original,
                "paired_excess_drop_from_original_percentage_points": original - excess,
            }
        )

    metric_fields = (
        "exposure_matched_comparator_weight",
        "candidate_net_total_return_pct",
        "comparator_net_total_return_pct",
        "paired_excess_percentage_points",
        "candidate_max_drawdown_magnitude_pct",
        "comparator_max_drawdown_magnitude_pct",
    )
    baseline_diff = max(
        abs(float(primary_baseline[field]) - float(independent_baseline[field]))
        for field in metric_fields
    )
    temporal_diff = 0.0
    for primary, independent in zip(primary_temporal, temporal_rows, strict=True):
        _expect(primary["shift_sessions"], independent["shift_sessions"], "replay.shift")
        for field in metric_fields:
            temporal_diff = max(
                temporal_diff,
                abs(float(primary[field]) - float(independent[field])),
            )
        for field in ("interval_count", "long_interval_count", "trade_event_count"):
            _expect(primary[field], independent[field], f"replay.temporal.{field}")
    episode_diff = 0.0
    episode_metric_fields = (
        *metric_fields,
        "paired_excess_change_from_original_percentage_points",
        "paired_excess_drop_from_original_percentage_points",
    )
    for primary, independent in zip(primary_episodes, episode_rows, strict=True):
        _expect(primary["episode_id"], independent["episode_id"], "replay.episode")
        for field in episode_metric_fields:
            episode_diff = max(
                episode_diff,
                abs(float(primary[field]) - float(independent[field])),
            )
        for field in ("interval_count", "long_interval_count", "trade_event_count"):
            _expect(primary[field], independent[field], f"replay.episode.{field}")
    independent_summary = summarize_diagnostics(temporal_rows, episode_rows)
    _expect(
        independent_summary["reducer_status"],
        primary_summary["reducer_status"],
        "replay.reducer",
    )
    _expect(temporal_digest.hexdigest(), primary_temporal_digest, "replay.temporal_digest")
    _expect(episode_digest.hexdigest(), primary_episode_digest, "replay.episode_digest")
    maximum = max(baseline_diff, temporal_diff, episode_diff)
    if maximum > RECONCILIATION_TOLERANCE:
        raise TemporalInfluenceExecutionError(
            "TIF_INDEPENDENT_REPLAY_MISMATCH", f"maximum_abs_diff={maximum}"
        )
    return {
        "schema_version": "first_layer_temporal_influence_independent_replay.v1",
        "status": "PASS",
        "maximum_baseline_metric_abs_diff": baseline_diff,
        "maximum_temporal_metric_abs_diff": temporal_diff,
        "maximum_episode_metric_abs_diff": episode_diff,
        "maximum_metric_abs_diff": maximum,
        "temporal_target_inventory_sha256": temporal_digest.hexdigest(),
        "episode_target_inventory_sha256": episode_digest.hexdigest(),
        "reducer_status": independent_summary["reducer_status"],
        "tolerance": RECONCILIATION_TOLERANCE,
    }


def _failure_result(
    *,
    code: str,
    message: str,
    counters: Mapping[str, int],
    runtime_git_head: str,
    authorization_state: str,
) -> Mapping[str, object]:
    return {
        "schema_version": "first_layer_composer_v2_temporal_influence_result.v1",
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
        "temporal_common_evaluated_range": None,
        "runtime_git_head": runtime_git_head,
        "actual_counters": dict(counters),
        "authorization_state": authorization_state,
        "aggregate_result_only": True,
        "raw_market_payload_exported": False,
        "raw_signal_payload_exported": False,
        "shifted_target_payload_exported": False,
        "trading_2557_status": "INSUFFICIENT",
        "trading_2558_status": "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO",
        "research_priority_increase_allowed": False,
        "qqq_options_wave_b": "HOLD",
        "qqq_options_wave_c": "NOT_AUTHORIZED",
        "production_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
        "orders": 0,
        "fills": 0,
        "positions": 0,
    }


def execute_temporal_influence_falsification(
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    *,
    authorization_path: Path = DEFAULT_AUTHORIZATION_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    project_root: Path = PROJECT_ROOT,
) -> Mapping[str, object]:
    root = project_root.resolve()
    target = (root / output_dir).resolve(strict=False)
    target.relative_to(root)
    attempt_path = target / "run_attempt_consumption_receipt.json"
    result_path = target / "aggregate_result.json"
    if attempt_path.exists() or result_path.exists():
        raise TemporalInfluenceExecutionError("TIF_ATTEMPT_ALREADY_CONSUMED", str(target))
    _, _, authorization_state, _ = _authorization_profile(authorization_path)
    counters = {key: 0 for key in EXPECTED_COUNTERS}
    runtime_git_head = f1._git_head(root)
    f1._write_once(
        attempt_path,
        {
            "schema_version": "first_layer_temporal_influence_attempt.v1",
            "task_id": TASK_ID,
            "status": "DISPATCHED_SINGLE_ATTEMPT_RESERVED",
            "authorization_state": authorization_state,
            "authorized_maxima": EXPECTED_COUNTERS,
            "runtime_git_head": runtime_git_head,
            "production_effect": "none",
            "broker_action": "none",
        },
    )
    try:
        policy = load_preregistration(project_root=root)
        authorization = load_run_authorization(
            authorization_path, project_root=root, policy=policy
        )
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
                requested_window=f1.DataQualityDateWindow(
                    start=REQUESTED_START, end=REQUESTED_END
                ),
                evaluated_window=f1.DataQualityDateWindow(
                    start=REQUESTED_START, end=REQUESTED_END
                ),
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
            "schema_version": "first_layer_temporal_influence_dq_receipt.v1",
            "status": dq.report.status,
            "canonical_dq_receipt_path": dq.receipt_path.relative_to(root).as_posix(),
            "canonical_dq_receipt_sha256": f1._sha256_path(dq.receipt_path),
            "canonical_dq_report_path": dq.report_path.relative_to(root).as_posix(),
            "canonical_dq_report_sha256": f1._sha256_path(dq.report_path),
            "requested_start": REQUESTED_START.isoformat(),
            "requested_end": REQUESTED_END.isoformat(),
            "evaluated_start": dq.receipt.evaluated_window.start.isoformat(),
            "evaluated_end": dq.receipt.evaluated_window.end.isoformat(),
            "error_count": dq.report.error_count,
            "warning_count": dq.report.warning_count,
        }
        f1._write_once(target / "canonical_dq_receipt.json", dq_receipt)
        if dq.report.status != "PASS":
            raise TemporalInfluenceExecutionError("TIF_DQ_OR_PIT_NOT_PASS", dq.report.status)

        counters["local_temporal_influence_runs"] = 1
        plan = f1.load_diagnostic_plan(manifest, project_root=root)
        shape = mp.extract_exposure_shape(plan.interval_targets)
        _expect(len(plan.sessions), EXPECTED_SESSIONS, "plan.signal_sessions")
        _expect(shape.interval_count, EXPECTED_INTERVALS, "plan.return_intervals")
        _expect(shape.long_interval_count, EXPECTED_LONG_INTERVALS, "plan.long_intervals")
        _expect(len(shape.long_run_lengths), EXPECTED_LONG_EPISODES, "plan.long_episodes")
        baseline, episodes, episode_digest = build_episode_influence(plan)
        temporal, temporal_digest = build_temporal_displacement(plan)

        prior_path = root / TRADING_2558_PATHS["trading_2558_aggregate_result"]
        prior = _mapping(json.loads(prior_path.read_bytes()), "trading_2558_result")
        reconciliation = {
            "candidate_net_total_return_abs_diff": abs(
                float(baseline["candidate_net_total_return_pct"])
                - float(prior["observed_candidate_net_total_return_pct"])
            ),
            "comparator_net_total_return_abs_diff": abs(
                float(baseline["comparator_net_total_return_pct"])
                - float(prior["comparator_net_total_return_pct"])
            ),
            "paired_excess_abs_diff": abs(
                float(baseline["paired_excess_percentage_points"])
                - float(prior["observed_5_bps_paired_excess_percentage_points"])
            ),
            "candidate_max_drawdown_abs_diff": abs(
                float(baseline["candidate_max_drawdown_magnitude_pct"])
                - _trading_2558_observed_max_drawdown(prior)
            ),
            "comparator_weight_abs_diff": abs(
                float(baseline["exposure_matched_comparator_weight"])
                - float(prior["exposure_matched_comparator_weight"])
            ),
        }
        if any(value > RECONCILIATION_TOLERANCE for value in reconciliation.values()):
            raise TemporalInfluenceExecutionError(
                "TIF_TRADING_2558_ACCOUNTING_DRIFT",
                json.dumps(reconciliation, sort_keys=True),
            )
        summary = summarize_diagnostics(temporal, episodes)

        counters["independent_replays"] = 1
        independent = independently_replay_diagnostics(
            plan,
            primary_baseline=baseline,
            primary_temporal=temporal,
            primary_episodes=episodes,
            primary_summary=summary,
            primary_temporal_digest=temporal_digest,
            primary_episode_digest=episode_digest,
        )
        f1._write_once(target / "independent_replay_receipt.json", independent)
        result: Mapping[str, object] = {
            "schema_version": "first_layer_composer_v2_temporal_influence_result.v1",
            "task_id": TASK_ID,
            "status": "TERMINAL",
            "technical_validation_state": "PASS",
            "reducer_status": summary["reducer_status"],
            "reason_codes": summary["reason_codes"],
            "authorization_state": authorization_state,
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
            "temporal_common_evaluated_range": {
                "start": plan.sessions[COMMON_START].isoformat(),
                "end": plan.sessions[COMMON_END_EXCLUSIVE].isoformat(),
            },
            "signal_session_count": len(plan.sessions),
            "return_interval_count": len(plan.interval_targets),
            "temporal_common_return_interval_count": EXPECTED_COMMON_INTERVALS,
            "signal_lag_sessions": 1,
            "shift_sessions": list(SHIFT_SESSIONS),
            "long_interval_count": shape.long_interval_count,
            "long_episode_count": len(shape.long_run_lengths),
            "primary_one_way_cost_bps": ONE_WAY_COST_BPS,
            "baseline_full_window": baseline,
            "trading_2558_accounting_reconciliation": reconciliation,
            "temporal_displacement": {
                "rows": temporal,
                "target_inventory_sha256": temporal_digest,
            },
            "leave_one_episode_influence": {
                "rows": episodes,
                "target_inventory_sha256": episode_digest,
            },
            "summary": summary,
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
            "shifted_target_payload_exported": False,
            "trading_2557_status": "INSUFFICIENT",
            "trading_2558_status": "TIMING_NOT_DISTINGUISHED_FROM_MATCHED_PLACEBO",
            "research_priority_increase_allowed": False,
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
        if isinstance(exc, TemporalInfluenceExecutionError):
            code, message = exc.code, exc.message
        elif isinstance(
            exc,
            (
                f1.FoundationalFalsificationExecutionError,
                mp.MatchedPlaceboExecutionError,
            ),
        ):
            code, message = exc.code, exc.message
        else:
            code, message = "TIF_UNEXPECTED_FAILURE", str(exc)
        failure = _failure_result(
            code=code,
            message=message,
            counters=counters,
            runtime_git_head=runtime_git_head,
            authorization_state=authorization_state,
        )
        f1._write_once(target / "failure_receipt.json", failure)
        f1._write_once(result_path, failure)
        return failure


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--authorization", type=Path, default=DEFAULT_AUTHORIZATION_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = execute_temporal_influence_falsification(
        args.manifest,
        authorization_path=args.authorization,
        output_dir=args.output_dir,
        project_root=args.project_root,
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
    "FAILURE_FIX_AUTHORIZATION_PATH",
    "FAILURE_FIX_MANIFEST_PATH",
    "FAILURE_FIX_OUTPUT_DIR",
    "TemporalInfluenceExecutionError",
    "build_episode_influence",
    "build_temporal_displacement",
    "execute_temporal_influence_falsification",
    "independently_replay_diagnostics",
    "load_execution_manifest",
    "load_preregistration",
    "load_run_authorization",
    "summarize_diagnostics",
]
