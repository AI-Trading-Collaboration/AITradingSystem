"""Deterministic retained-evidence routing for structurally distinct strategies.

The screen reads only exact-bound tracked configuration.  It performs no market
data access, performance ranking, prospective outcome read, or artifact write.
Structural orthogonality is deliberately kept separate from empirical return
independence.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = Path("config/research/orthogonal_strategy_retained_evidence_screen_v1.yaml")
TASK_ID = "TRADING-2561_ORTHOGONAL_STRATEGY_RETAINED_EVIDENCE_SCREEN_V1"
POLICY_SCHEMA = "orthogonal_strategy_retained_evidence_screen.v1"
RESULT_SCHEMA = "orthogonal_strategy_retained_evidence_screen_result.v1"
MECHANISM_FAMILIES = (
    "STATIC_ALLOCATION",
    "VOLATILITY_ALLOCATION",
    "TREND_TIMING",
    "LEVERAGED_GROWTH",
    "OPTIONS_IMPLEMENTATION",
)
ORTHOGONALITY_VALUES = ("DISTINCT", "PARTIAL", "OVERLAPPING")
ROUTES = (
    "CONTINUE_EXISTING_FORWARD_AGING",
    "PREREGISTER_NEW_EXPERIMENT",
    "HOLD_REFERENCE",
    "EXCLUDE",
)
EXPECTED_SOURCE_ROLES = (
    "simple_baseline_registry",
    "layer2_component_pool",
    "growth_candidate_registry",
    "evidence_first_portfolio",
    "composer_terminal_admission",
)
ZERO_ACTION_COUNTERS = {
    "tracked_config_reads": 5,
    "market_data_reads": 0,
    "prospective_outcome_reads": 0,
    "canonical_dq_runs": 0,
    "backtests": 0,
    "data_downloads": 0,
    "cache_mutations": 0,
    "external_provider_actions": 0,
    "quantconnect_actions": 0,
    "option_backtests": 0,
    "orders": 0,
    "fills": 0,
    "positions": 0,
}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class OrthogonalStrategyScreenError(ValueError):
    """Stable fail-closed screen error."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class LoadedScreenPolicy:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _list(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_SCHEMA_INVALID", f"{label} must be a list"
        )
    return value


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_IDENTITY_MISMATCH",
            f"{label}: expected={expected!r} actual={actual!r}",
        )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bound_file(path: Path, *, project_root: Path, label: str) -> Path:
    root = project_root.resolve()
    resolved = (path if path.is_absolute() else root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_PATH_OUTSIDE_ROOT", f"{label}: {path}"
        ) from exc
    if not resolved.is_file():
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_INPUT_MISSING", f"{label}: {path.as_posix()}"
        )
    return resolved


def _load_yaml(path: Path, *, project_root: Path, label: str) -> dict[str, Any]:
    resolved = _bound_file(path, project_root=project_root, label=label)
    return _mapping(load_strict_yaml_text(resolved.read_text(encoding="utf-8"), label=label), label)


def load_screen_policy(
    path: Path = DEFAULT_POLICY_PATH, *, project_root: Path = PROJECT_ROOT
) -> LoadedScreenPolicy:
    resolved = _bound_file(path, project_root=project_root, label="screen policy")
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
            "source_bindings",
            "screen_contract",
            "candidates",
            "result_contract",
            "run_envelope",
            "safety",
        ),
        "policy.root_fields",
    )
    _expect(payload["schema_version"], POLICY_SCHEMA, "policy.schema_version")
    _expect(payload["policy_id"], path.stem, "policy.policy_id")
    _expect(payload["policy_version"], "1.0.0", "policy.policy_version")
    _expect(payload["task_id"], TASK_ID, "policy.task_id")
    boundary = _mapping(payload["known_result_boundary"], "known_result_boundary")
    _expect(boundary["current_composer_terminal_verdict"], "INSUFFICIENT_HOLD", "verdict")
    _expect(boundary["prospective_outcome_access_allowed"], False, "prospective access")
    _expect(boundary["new_backtest_allowed"], False, "new backtest")
    contract = _mapping(payload["screen_contract"], "screen_contract")
    _expect(tuple(contract["mechanism_families"]), MECHANISM_FAMILIES, "mechanisms")
    _expect(
        tuple(contract["structural_orthogonality_values"]),
        ORTHOGONALITY_VALUES,
        "orthogonality values",
    )
    _expect(contract["empirical_independence_value"], "NOT_ESTABLISHED", "independence")
    _expect(tuple(contract["routes"]), ROUTES, "routes")
    _expect(
        _mapping(payload["run_envelope"], "run_envelope"),
        ZERO_ACTION_COUNTERS,
        "run envelope",
    )
    safety = _mapping(payload["safety"], "safety")
    _assert_safe_boundary(safety, "policy.safety")
    bindings = _list(payload["source_bindings"], "source_bindings")
    roles: list[str] = []
    for index, raw_binding in enumerate(bindings):
        binding = _mapping(raw_binding, f"source_bindings[{index}]")
        _expect(tuple(binding), ("role", "path", "sha256"), f"binding[{index}].fields")
        role = str(binding["role"])
        roles.append(role)
        expected = str(binding["sha256"])
        if _SHA256.fullmatch(expected) is None:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_SHA256_INVALID", f"{role}: {expected!r}"
            )
        source = _bound_file(Path(str(binding["path"])), project_root=project_root, label=role)
        _expect(_sha256(source.read_bytes()), expected, f"{role}.sha256")
    _expect(tuple(roles), EXPECTED_SOURCE_ROLES, "source roles")
    candidates = _list(payload["candidates"], "candidates")
    ids: set[str] = set()
    for index, raw_candidate in enumerate(candidates):
        candidate = _mapping(raw_candidate, f"candidates[{index}]")
        _expect(
            tuple(candidate),
            (
                "candidate_id",
                "source_role",
                "source_section",
                "lookup_field",
                "lookup_value",
                "mechanism_family",
                "input_overlap",
                "action_overlap",
                "structural_orthogonality",
                "route",
                "reason_codes",
            ),
            f"candidate[{index}].fields",
        )
        candidate_id = str(candidate["candidate_id"])
        if candidate_id in ids:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_CANDIDATE_DUPLICATE", candidate_id
            )
        ids.add(candidate_id)
        if candidate["source_role"] not in roles:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_SOURCE_ROLE_UNKNOWN", str(candidate["source_role"])
            )
        if candidate["mechanism_family"] not in MECHANISM_FAMILIES:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_MECHANISM_INVALID", str(candidate["mechanism_family"])
            )
        if candidate["structural_orthogonality"] not in ORTHOGONALITY_VALUES:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_ORTHOGONALITY_INVALID",
                str(candidate["structural_orthogonality"]),
            )
        if candidate["route"] not in ROUTES:
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_ROUTE_INVALID", str(candidate["route"])
            )
        reasons = _list(candidate["reason_codes"], f"candidate[{index}].reason_codes")
        if not reasons or any(not isinstance(item, str) or not item for item in reasons):
            raise OrthogonalStrategyScreenError(
                "ORTHOGONAL_SCREEN_REASON_CODES_INVALID", candidate_id
            )
    return LoadedScreenPolicy(
        payload=payload,
        path=resolved,
        file_sha256=_sha256(raw),
        canonical_sha256=_sha256(_canonical_bytes(payload)),
    )


def _assert_safe_boundary(payload: Mapping[str, Any], label: str) -> None:
    if payload.get("production_effect") != "none" or payload.get("broker_action") != "none":
        raise OrthogonalStrategyScreenError("ORTHOGONAL_SCREEN_UNSAFE_SOURCE", label)
    if payload.get("production_allowed") is True or payload.get("paper_shadow_allowed") is True:
        raise OrthogonalStrategyScreenError("ORTHOGONAL_SCREEN_UNSAFE_SOURCE", label)


def _binding_map(policy: Mapping[str, Any]) -> dict[str, Path]:
    return {
        str(item["role"]): Path(str(item["path"]))
        for item in _list(policy["source_bindings"], "source_bindings")
    }


def _find_source_entry(
    source: Mapping[str, Any], *, section: str, field: str, value: str
) -> dict[str, Any]:
    rows = _list(source.get(section), f"source.{section}")
    matches = [
        _mapping(row, f"source.{section}[]")
        for row in rows
        if isinstance(row, dict) and row.get(field) == value
    ]
    if len(matches) != 1:
        raise OrthogonalStrategyScreenError(
            "ORTHOGONAL_SCREEN_SOURCE_ENTRY_INVALID",
            f"{section}.{field}={value}: matches={len(matches)}",
        )
    return matches[0]


def run_retained_evidence_screen(
    policy: LoadedScreenPolicy | None = None, *, project_root: Path = PROJECT_ROOT
) -> dict[str, Any]:
    loaded = policy or load_screen_policy(project_root=project_root)
    bindings = _binding_map(loaded.payload)
    sources = {
        role: _load_yaml(path, project_root=project_root, label=role)
        for role, path in bindings.items()
    }
    for role in (
        "simple_baseline_registry",
        "layer2_component_pool",
        "growth_candidate_registry",
    ):
        _assert_safe_boundary(
            _mapping(sources[role].get("safety_boundary"), f"{role}.safety_boundary"),
            role,
        )
    _assert_safe_boundary(
        _mapping(sources["evidence_first_portfolio"].get("safety"), "portfolio.safety"),
        "evidence_first_portfolio",
    )
    terminal = sources["composer_terminal_admission"]
    _expect(
        terminal.get("status"),
        "ADMITTED_DIAGNOSTIC_SINGLE_EPISODE_DEPENDENT",
        "composer terminal admission",
    )
    portfolio_question = _mapping(
        sources["evidence_first_portfolio"].get("primary_evidence_question"),
        "portfolio.primary_evidence_question",
    )
    _expect(
        portfolio_question.get("question_id"),
        "SIGNAL_VALUE_FIRST_LAYER_COMPOSER_V2",
        "portfolio question",
    )
    rows: list[dict[str, Any]] = []
    for raw_candidate in _list(loaded.payload["candidates"], "candidates"):
        candidate = _mapping(raw_candidate, "candidate")
        source_role = str(candidate["source_role"])
        source_entry = _find_source_entry(
            sources[source_role],
            section=str(candidate["source_section"]),
            field=str(candidate["lookup_field"]),
            value=str(candidate["lookup_value"]),
        )
        rows.append(
            {
                "candidate_id": candidate["candidate_id"],
                "mechanism_family": candidate["mechanism_family"],
                "input_overlap": candidate["input_overlap"],
                "action_overlap": candidate["action_overlap"],
                "structural_orthogonality": candidate["structural_orthogonality"],
                "empirical_independence_claim": "NOT_ESTABLISHED",
                "route": candidate["route"],
                "reason_codes": list(candidate["reason_codes"]),
                "source_role": source_role,
                "source_section": candidate["source_section"],
                "source_entry_sha256": _sha256(_canonical_bytes(source_entry)),
            }
        )
    rows.sort(key=lambda item: str(item["candidate_id"]))
    result_contract = _mapping(loaded.payload["result_contract"], "result_contract")
    result = {
        "schema_version": RESULT_SCHEMA,
        "task_id": TASK_ID,
        "policy_sha256": loaded.file_sha256,
        "status": result_contract["status"],
        "selected_continuation_candidate": result_contract["selected_continuation_candidate"],
        "selected_new_experiment_candidate": result_contract["selected_new_experiment_candidate"],
        "source_consistency": {
            "historical_portfolio_snapshot_verdict": portfolio_question.get("current_verdict"),
            "terminal_composer_verdict": "INSUFFICIENT_HOLD",
            "warning": result_contract["portfolio_snapshot_warning"],
            "terminal_evidence_precedence_applied": True,
        },
        "candidates": rows,
        "run_counters": ZERO_ACTION_COUNTERS,
        "automatic_promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
    return {**result, "result_sha256": _sha256(_canonical_bytes(result))}
