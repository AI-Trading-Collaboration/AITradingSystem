"""Result-blind prospective observation contract for first-layer composer v2.

This module deliberately has no market-data, scheduler, provider, broker, or
output-writing surface.  It validates the frozen policy and supplies pure
append-only ledger operations that a separately authorized capture runner may
consume after an exact freeze commit and first XNYS session are selected.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ai_trading_system.yaml_loader import load_strict_yaml_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = Path(
    "config/research/first_layer_composer_v2_prospective_oos_preregistration_v1.yaml"
)
TASK_ID = "TRADING-2560_FIRST_LAYER_COMPOSER_V2_PROSPECTIVE_OOS_OBSERVATION_V1"
POLICY_SCHEMA = "first_layer_composer_v2_prospective_oos_preregistration.v1"
OBSERVATION_SCHEMA = "first_layer_composer_v2_prospective_observation.v1"
LEDGER_SCHEMA = "first_layer_composer_v2_prospective_ledger.v1"
HORIZONS = (1, 5, 20)
PRIMARY_HORIZON = 20
ONE_WAY_COST_BPS = 5.0
HISTORICAL_EVALUATED_END = date(2025, 12, 2)
ALLOWED_STATES = ("risk_off", "defensive", "neutral", "constructive", "risk_on")
LONG_STATES = frozenset({"constructive", "risk_on"})
REQUIRED_IDENTITIES = (
    "feature_snapshot_sha256",
    "signal_sha256",
    "model_sha256",
    "policy_sha256",
    "dq_receipt_sha256",
    "source_sha256",
)
ZERO_RUN_ENVELOPE = {
    "market_data_reads": 0,
    "canonical_dq_runs": 0,
    "prospective_captures": 0,
    "maturity_updates": 0,
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
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")


class ProspectiveObservationError(ValueError):
    """Stable fail-closed error for prospective observation contracts."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass(frozen=True)
class LoadedProspectivePolicy:
    payload: Mapping[str, Any]
    path: Path
    file_sha256: str
    canonical_sha256: str


@dataclass(frozen=True)
class ActivatedObservationContract:
    policy_id: str
    policy_sha256: str
    freeze_commit: str
    prospective_start: date


def _mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ProspectiveObservationError(
            "PROSPECTIVE_SCHEMA_INVALID", f"{label} must be a string-keyed mapping"
        )
    return value


def _sequence(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ProspectiveObservationError("PROSPECTIVE_SCHEMA_INVALID", f"{label} must be a list")
    return value


def _expect(actual: object, expected: object, label: str) -> None:
    if actual != expected:
        raise ProspectiveObservationError(
            "PROSPECTIVE_IDENTITY_MISMATCH",
            f"{label}: expected={expected!r} actual={actual!r}",
        )


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _bound_path(path: Path, *, project_root: Path, label: str) -> Path:
    root = project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    resolved = candidate.resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise ProspectiveObservationError(
            "PROSPECTIVE_PATH_OUTSIDE_ROOT", f"{label}: {path}"
        ) from exc
    if not resolved.is_file():
        raise ProspectiveObservationError(
            "PROSPECTIVE_INPUT_MISSING", f"{label}: {path.as_posix()}"
        )
    return resolved


def _require_sha256(value: object, label: str) -> str:
    text = str(value)
    if _SHA256.fullmatch(text) is None:
        raise ProspectiveObservationError("PROSPECTIVE_SHA256_INVALID", f"{label}: {text!r}")
    return text


def load_preregistration(
    path: Path = DEFAULT_POLICY_PATH, *, project_root: Path = PROJECT_ROOT
) -> LoadedProspectivePolicy:
    resolved = _bound_path(path, project_root=project_root, label="policy")
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
            "freeze_contract",
            "definition_bindings",
            "observation_contract",
            "maturity_contract",
            "scoreboard_contract",
            "run_envelope",
            "safety",
        ),
        "policy.root_fields",
    )
    _expect(payload["schema_version"], POLICY_SCHEMA, "policy.schema_version")
    _expect(payload["policy_id"], path.stem, "policy.policy_id")
    _expect(payload["policy_version"], "1.0.0", "policy.policy_version")
    _expect(
        payload["policy_status"],
        "RESULT_BLIND_CONTRACT_NOT_YET_CAPTURING",
        "policy.policy_status",
    )
    _expect(payload["task_id"], TASK_ID, "policy.task_id")
    known = _mapping(payload["known_result_boundary"], "known_result_boundary")
    _expect(known["historical_evaluated_end"], "2025-12-02", "historical end")
    _expect(known["current_verdict"], "INSUFFICIENT_HOLD", "current verdict")
    _expect(
        known["prospective_outcome_accessed_by_this_contract"],
        False,
        "prospective outcome access",
    )
    freeze = _mapping(payload["freeze_contract"], "freeze_contract")
    _expect(tuple(freeze["state_order"]), ALLOWED_STATES, "state_order")
    _expect(frozenset(freeze["long_states"]), LONG_STATES, "long_states")
    _expect(freeze["execution_lag_sessions"], 1, "execution_lag_sessions")
    _expect(freeze["primary_one_way_cost_bps"], ONE_WAY_COST_BPS, "cost")
    _expect(freeze["prospective_start_date"], None, "prospective_start_date")
    _expect(freeze["backfill_allowed"], False, "backfill_allowed")
    observation = _mapping(payload["observation_contract"], "observation_contract")
    _expect(observation["schema_version"], OBSERVATION_SCHEMA, "observation schema")
    _expect(
        tuple(observation["required_identity_fields"]),
        REQUIRED_IDENTITIES,
        "required identities",
    )
    maturity = _mapping(payload["maturity_contract"], "maturity_contract")
    _expect(tuple(maturity["horizon_sessions"]), HORIZONS, "horizons")
    _expect(maturity["primary_horizon_sessions"], PRIMARY_HORIZON, "primary horizon")
    _expect(
        _mapping(payload["run_envelope"], "run_envelope"),
        ZERO_RUN_ENVELOPE,
        "run envelope",
    )
    safety = _mapping(payload["safety"], "safety")
    _expect(safety["paper_shadow_allowed"], False, "paper_shadow_allowed")
    _expect(safety["production_allowed"], False, "production_allowed")
    _expect(safety["production_effect"], "none", "production_effect")
    _expect(safety["broker_action"], "none", "broker_action")
    bindings = _sequence(payload["definition_bindings"], "definition_bindings")
    roles: set[str] = set()
    for index, item in enumerate(bindings):
        binding = _mapping(item, f"definition_bindings[{index}]")
        _expect(tuple(binding), ("role", "path", "sha256"), f"binding[{index}].fields")
        role = str(binding["role"])
        if role in roles:
            raise ProspectiveObservationError("PROSPECTIVE_BINDING_DUPLICATE", f"role={role}")
        roles.add(role)
        relative = Path(str(binding["path"]))
        source = _bound_path(relative, project_root=project_root, label=role)
        expected_sha = _require_sha256(binding["sha256"], f"{role}.sha256")
        _expect(_sha256_bytes(source.read_bytes()), expected_sha, f"{role}.sha256")
    expected_roles = {
        "composer_policy",
        "foundational_preregistration",
        "foundational_result_admission",
        "matched_placebo_result_admission",
        "temporal_influence_result_admission",
    }
    _expect(roles, expected_roles, "definition binding roles")
    return LoadedProspectivePolicy(
        payload=payload,
        path=resolved,
        file_sha256=_sha256_bytes(raw),
        canonical_sha256=_sha256_bytes(_canonical_bytes(payload)),
    )


def activate_contract(
    policy: LoadedProspectivePolicy,
    *,
    freeze_commit: str,
    prospective_start: date,
) -> ActivatedObservationContract:
    if _GIT_SHA.fullmatch(freeze_commit) is None:
        raise ProspectiveObservationError("PROSPECTIVE_FREEZE_COMMIT_INVALID", freeze_commit)
    if prospective_start <= HISTORICAL_EVALUATED_END:
        raise ProspectiveObservationError(
            "PROSPECTIVE_BACKFILL_FORBIDDEN", prospective_start.isoformat()
        )
    return ActivatedObservationContract(
        policy_id=str(policy.payload["policy_id"]),
        policy_sha256=policy.file_sha256,
        freeze_commit=freeze_commit,
        prospective_start=prospective_start,
    )


def new_ledger(contract: ActivatedObservationContract) -> dict[str, Any]:
    return {
        "schema_version": LEDGER_SCHEMA,
        "policy_id": contract.policy_id,
        "policy_sha256": contract.policy_sha256,
        "freeze_commit": contract.freeze_commit,
        "prospective_start": contract.prospective_start.isoformat(),
        "observations": [],
        "safety": {
            "research_only": True,
            "paper_shadow_allowed": False,
            "production_allowed": False,
            "production_effect": "none",
            "broker_action": "none",
        },
    }


def create_observation(
    contract: ActivatedObservationContract,
    *,
    decision_date: date,
    trend_state: str,
    identities: Mapping[str, str],
    dq_status: str,
) -> dict[str, Any]:
    if decision_date < contract.prospective_start:
        raise ProspectiveObservationError(
            "PROSPECTIVE_BACKFILL_FORBIDDEN", decision_date.isoformat()
        )
    if trend_state not in ALLOWED_STATES:
        raise ProspectiveObservationError("PROSPECTIVE_STATE_INVALID", trend_state)
    if dq_status != "PASS":
        raise ProspectiveObservationError("PROSPECTIVE_DQ_NOT_PASS", dq_status)
    _expect(tuple(sorted(identities)), tuple(sorted(REQUIRED_IDENTITIES)), "identity fields")
    normalized_identities = {
        key: _require_sha256(identities[key], key) for key in REQUIRED_IDENTITIES
    }
    action = "LONG_QQQ" if trend_state in LONG_STATES else "FLAT_CASH"
    core = {
        "schema_version": OBSERVATION_SCHEMA,
        "decision_date": decision_date.isoformat(),
        "trend_state": trend_state,
        "action": action,
        "execution_lag_sessions": 1,
        "primary_one_way_cost_bps": ONE_WAY_COST_BPS,
        "identities": normalized_identities,
        "dq_status": dq_status,
        "matured_outcomes": {},
    }
    return {**core, "observation_id": _sha256_bytes(_canonical_bytes(core))}


def _validate_observation(observation: Mapping[str, Any]) -> dict[str, Any]:
    candidate = _mapping(dict(observation), "observation")
    _expect(
        set(candidate),
        {
            "schema_version",
            "decision_date",
            "trend_state",
            "action",
            "execution_lag_sessions",
            "primary_one_way_cost_bps",
            "identities",
            "dq_status",
            "matured_outcomes",
            "observation_id",
        },
        "observation.fields",
    )
    _expect(candidate["schema_version"], OBSERVATION_SCHEMA, "observation schema")
    date.fromisoformat(str(candidate["decision_date"]))
    state = str(candidate["trend_state"])
    if state not in ALLOWED_STATES:
        raise ProspectiveObservationError("PROSPECTIVE_STATE_INVALID", state)
    expected_action = "LONG_QQQ" if state in LONG_STATES else "FLAT_CASH"
    _expect(candidate["action"], expected_action, "observation.action")
    _expect(candidate["execution_lag_sessions"], 1, "observation.execution_lag")
    _expect(
        float(candidate["primary_one_way_cost_bps"]),
        ONE_WAY_COST_BPS,
        "observation.cost",
    )
    _expect(candidate["dq_status"], "PASS", "observation.dq_status")
    identities = _mapping(candidate["identities"], "observation.identities")
    _expect(set(identities), set(REQUIRED_IDENTITIES), "observation.identity fields")
    for key in REQUIRED_IDENTITIES:
        _require_sha256(identities[key], f"observation.{key}")
    _mapping(candidate["matured_outcomes"], "observation.matured_outcomes")
    core = {
        key: ({} if key == "matured_outcomes" else value)
        for key, value in candidate.items()
        if key != "observation_id"
    }
    _expect(
        candidate["observation_id"],
        _sha256_bytes(_canonical_bytes(core)),
        "observation.observation_id",
    )
    return candidate


def _validate_ledger(ledger: Mapping[str, Any]) -> dict[str, Any]:
    result = deepcopy(_mapping(dict(ledger), "ledger"))
    _expect(
        set(result),
        {
            "schema_version",
            "policy_id",
            "policy_sha256",
            "freeze_commit",
            "prospective_start",
            "observations",
            "safety",
        },
        "ledger.fields",
    )
    _expect(result["schema_version"], LEDGER_SCHEMA, "ledger.schema_version")
    _require_sha256(result["policy_sha256"], "ledger.policy_sha256")
    if _GIT_SHA.fullmatch(str(result["freeze_commit"])) is None:
        raise ProspectiveObservationError(
            "PROSPECTIVE_FREEZE_COMMIT_INVALID", str(result["freeze_commit"])
        )
    date.fromisoformat(str(result["prospective_start"]))
    safety = _mapping(result["safety"], "ledger.safety")
    _expect(safety.get("paper_shadow_allowed"), False, "ledger.paper_shadow_allowed")
    _expect(safety.get("production_allowed"), False, "ledger.production_allowed")
    _expect(safety.get("production_effect"), "none", "ledger.production_effect")
    _expect(safety.get("broker_action"), "none", "ledger.broker_action")
    observations = _sequence(result["observations"], "ledger.observations")
    validated = [_validate_observation(item) for item in observations]
    dates = [date.fromisoformat(str(item["decision_date"])) for item in validated]
    if any(left >= right for left, right in zip(dates, dates[1:], strict=False)):
        raise ProspectiveObservationError(
            "PROSPECTIVE_LEDGER_ORDER_INVALID", "decision dates must strictly increase"
        )
    result["observations"] = validated
    return result


def append_observation(
    ledger: Mapping[str, Any], observation: Mapping[str, Any]
) -> tuple[dict[str, Any], str]:
    result = _validate_ledger(ledger)
    observations = _sequence(result.get("observations"), "ledger.observations")
    candidate = _validate_observation(observation)
    decision = date.fromisoformat(str(candidate.get("decision_date")))
    start = date.fromisoformat(str(result.get("prospective_start")))
    if decision < start:
        raise ProspectiveObservationError("PROSPECTIVE_BACKFILL_FORBIDDEN", decision.isoformat())
    for existing_raw in observations:
        existing = _mapping(existing_raw, "existing observation")
        if existing.get("decision_date") != candidate.get("decision_date"):
            continue
        if existing.get("observation_id") == candidate.get("observation_id"):
            return result, "OBSERVATION_ALREADY_EXISTS"
        raise ProspectiveObservationError(
            "PROSPECTIVE_SAME_DATE_IDENTITY_DRIFT", decision.isoformat()
        )
    if observations:
        last = date.fromisoformat(str(_mapping(observations[-1], "last")["decision_date"]))
        if decision <= last:
            raise ProspectiveObservationError("PROSPECTIVE_NON_APPEND_WRITE", decision.isoformat())
    observations.append(candidate)
    result["observations"] = observations
    return result, "OBSERVATION_WRITTEN"


def append_matured_outcomes(
    ledger: Mapping[str, Any],
    *,
    decision_date: date,
    as_of_date: date,
    session_dates: Sequence[date],
    outcomes: Mapping[int, Mapping[str, float]],
) -> tuple[dict[str, Any], str]:
    sessions = tuple(session_dates)
    if (
        not sessions
        or sessions[0] != decision_date
        or any(left >= right for left, right in zip(sessions, sessions[1:], strict=False))
    ):
        raise ProspectiveObservationError(
            "PROSPECTIVE_SESSION_SEQUENCE_INVALID", decision_date.isoformat()
        )
    result = _validate_ledger(ledger)
    observations = _sequence(result.get("observations"), "ledger.observations")
    target: dict[str, Any] | None = None
    for item in observations:
        row = _mapping(item, "observation")
        if row.get("decision_date") == decision_date.isoformat():
            target = row
            break
    if target is None:
        raise ProspectiveObservationError(
            "PROSPECTIVE_OBSERVATION_MISSING", decision_date.isoformat()
        )
    matured = _mapping(target.get("matured_outcomes"), "matured_outcomes")
    changed = False
    for horizon, metrics_raw in sorted(outcomes.items()):
        if horizon not in HORIZONS:
            raise ProspectiveObservationError("PROSPECTIVE_HORIZON_INVALID", str(horizon))
        if len(sessions) <= horizon:
            raise ProspectiveObservationError("PROSPECTIVE_SESSION_COVERAGE_MISSING", str(horizon))
        end_session = sessions[horizon]
        if as_of_date < end_session:
            raise ProspectiveObservationError(
                "PROSPECTIVE_OUTCOME_IMMATURE", f"{horizon}:{end_session.isoformat()}"
            )
        metrics = _mapping(dict(metrics_raw), f"outcomes[{horizon}]")
        _expect(
            tuple(metrics),
            (
                "candidate_net_return_pct",
                "comparator_net_return_pct",
                "paired_excess_percentage_points",
                "one_way_cost_bps",
            ),
            f"outcomes[{horizon}].fields",
        )
        candidate_return = float(metrics["candidate_net_return_pct"])
        comparator_return = float(metrics["comparator_net_return_pct"])
        paired = float(metrics["paired_excess_percentage_points"])
        if abs((candidate_return - comparator_return) - paired) > 1e-10:
            raise ProspectiveObservationError(
                "PROSPECTIVE_OUTCOME_ACCOUNTING_INVALID", str(horizon)
            )
        _expect(float(metrics["one_way_cost_bps"]), ONE_WAY_COST_BPS, "outcome cost")
        immutable_entry = {
            "horizon_sessions": horizon,
            "end_session": end_session.isoformat(),
            **{key: float(value) for key, value in metrics.items()},
        }
        key = str(horizon)
        if key in matured:
            existing = _mapping(matured[key], f"matured_outcomes[{key}]")
            existing_immutable = {
                field: value for field, value in existing.items() if field != "matured_as_of"
            }
            if existing_immutable != immutable_entry:
                raise ProspectiveObservationError(
                    "PROSPECTIVE_MATURED_OUTCOME_IMMUTABLE", str(horizon)
                )
            continue
        matured[key] = {
            **immutable_entry,
            "matured_as_of": as_of_date.isoformat(),
        }
        changed = True
    target["matured_outcomes"] = matured
    return result, "MATURITY_UPDATED" if changed else "MATURITY_ALREADY_RECORDED"


def scoreboard_state(ledger: Mapping[str, Any]) -> dict[str, Any]:
    observations = _sequence(_validate_ledger(ledger).get("observations"), "ledger.observations")
    counts = {str(horizon): 0 for horizon in HORIZONS}
    for item in observations:
        matured = _mapping(_mapping(item, "observation").get("matured_outcomes"), "matured")
        for horizon in HORIZONS:
            counts[str(horizon)] += int(str(horizon) in matured)
    return {
        "status": "EVIDENCE_INSUFFICIENT",
        "reason_codes": ["OWNER_REVIEWED_SAMPLE_EPISODE_GATE_NOT_FROZEN"],
        "observation_count": len(observations),
        "matured_counts": counts,
        "automatic_promotion_allowed": False,
        "production_effect": "none",
        "broker_action": "none",
    }
