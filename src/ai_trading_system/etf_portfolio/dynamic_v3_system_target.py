from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import PROJECT_ROOT, load_data_quality
from ai_trading_system.data.quality import DataQualityReport, validate_data_cache
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import DEFAULT_RATES_CACHE_PATH
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
TARGET_METHODS = (
    "static_baseline",
    "no_trade_baseline",
    "consensus_target",
    "limited_adjustment",
    "defensive_limited_adjustment",
    "equal_weight_shadow_candidates",
    "selected_top_candidate",
)
DEFAULT_MODEL_TARGET_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "model_target_portfolio_v1.yaml"
)
DEFAULT_PAPER_SHADOW_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "etf_portfolio" / "dynamic_v3_rescue" / "paper_shadow_account_v1.yaml"
)
DEFAULT_PRICE_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MODEL_TARGET_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_target"
DEFAULT_PAPER_SHADOW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow"
DEFAULT_MODEL_REBALANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_rebalance"
DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_performance"
DEFAULT_SYSTEM_TARGET_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "system_target_review"

# Reporting bucket boundary, not an approval or allocation rule. The 2% daily
# loss level is intentionally named so future calibration work can audit it.
PRESSURE_RETURN_THRESHOLD = -0.02

SYSTEM_TARGET_SAFETY: dict[str, Any] = {
    "research_target_only": True,
    "paper_shadow_only": True,
    "not_official_target_weights": True,
    "broker_action_allowed": False,
    "broker_action_taken": False,
    "order_ticket_generated": False,
    "owner_approval_required": True,
    "production_effect": PRODUCTION_EFFECT,
    "production_state_mutated": False,
    "baseline_config_mutated": False,
    "official_target_weights_mutated": False,
    "production_candidate_generated": False,
    "automatic_candidate_promotion": False,
    "auto_apply": False,
}


class DynamicV3SystemTargetError(ValueError):
    """Raised when system target or paper shadow artifacts fail closed."""


def load_model_target_config(path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_model_target_config_safe(payload)
    return payload


def validate_model_target_config(path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_model_target_config(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    else:
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check(
                    "research_target_only",
                    _mapping(payload.get("model_target")).get("mode") == "research_target_only",
                    "",
                ),
                _check(
                    "not_official_target_weights",
                    _mapping(payload.get("model_target")).get("not_official_target_weights")
                    is True,
                    "",
                ),
                _check(
                    "paper_shadow_only",
                    _mapping(payload.get("model_target")).get("paper_shadow_only") is True,
                    "",
                ),
                _check("target_methods_present", bool(_enabled_methods(payload)), ""),
                _check(
                    "required_methods_enabled",
                    {
                        "static_baseline",
                        "consensus_target",
                        "limited_adjustment",
                        "defensive_limited_adjustment",
                    }.issubset(set(_enabled_methods(payload))),
                    ",".join(_enabled_methods(payload)),
                ),
                _check("baseline_weights_valid", bool(_config_baseline_weights(payload)), ""),
                _check("constraints_valid", bool(_constraints(payload)), ""),
                _check("safety_locked", _safety_config_locked(_mapping(payload.get("safety"))), ""),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_model_target_config_validation",
        "model_target_config",
        checks,
        extra={"config_path": str(path)},
    )


def generate_model_target(
    *,
    config_path: Path = DEFAULT_MODEL_TARGET_CONFIG_PATH,
    as_of: date | None = None,
    output_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    position_advisory_daily_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    shadow_monitor_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    config = load_model_target_config(config_path)
    generated = generated_at or datetime.now(UTC)
    target_date = as_of or generated.date()
    enabled = _enabled_methods(config)
    baseline = _config_baseline_weights(config)
    source = _latest_target_source(
        position_advisory_daily_dir=position_advisory_daily_dir,
        shadow_monitor_dir=shadow_monitor_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        consensus_drift_dir=consensus_drift_dir,
    )
    candidates = source["candidate_targets"]
    consensus = _normalize_weights(
        source.get("consensus_weights") or _average_candidate_weights(candidates) or baseline
    )
    top_candidate = _normalize_weights(
        _first_candidate_weights(candidates) or consensus or baseline
    )
    equal_weight = _normalize_weights(_average_candidate_weights(candidates) or consensus)
    advisory_limits = _load_advisory_limits(
        _mapping(config.get("source")).get("position_advisory_config")
    )
    if not advisory_limits:
        advisory_limits = _load_advisory_limits(DEFAULT_POSITION_ADVISORY_CONFIG_PATH)
    if not advisory_limits:
        raise DynamicV3SystemTargetError("position advisory adjustment limits are required")
    limited = _limited_adjustment(
        baseline=baseline,
        target=consensus,
        max_total_adjustment=_float(advisory_limits.get("max_single_day_total_adjustment")),
        max_symbol_adjustment=_float(advisory_limits.get("max_single_symbol_adjustment")),
    )
    defensive = _defensive_adjustment(
        limited,
        _mapping(_mapping(config.get("method_policy")).get("defensive_limited_adjustment")),
    )
    method_weights = {
        "static_baseline": baseline,
        "no_trade_baseline": baseline,
        "consensus_target": consensus,
        "limited_adjustment": limited,
        "defensive_limited_adjustment": defensive,
        "equal_weight_shadow_candidates": equal_weight,
        "selected_top_candidate": top_candidate,
    }
    rows = [
        {
            "target_id": "",
            "as_of": target_date.isoformat(),
            "target_method": method,
            "weights": method_weights[method],
            "source_candidates": [row.get("candidate_id") for row in candidates],
            "source_shadow_shortlist_id": source.get("shadow_shortlist_id", ""),
            "source_shadow_monitor_run_id": source.get("shadow_monitor_run_id", ""),
            "source_consensus_drift_id": source.get("consensus_drift_id", ""),
            **SYSTEM_TARGET_SAFETY,
        }
        for method in enabled
        if method in method_weights
    ]
    target_id = _stable_id(
        "model-target",
        target_date.isoformat(),
        config_path,
        [(row["target_method"], row["weights"]) for row in rows],
    )
    target_dir = _unique_dir(output_dir / target_id)
    target_dir.mkdir(parents=True, exist_ok=False)
    for row in rows:
        row["target_id"] = target_dir.name
    constraint_checks = _constraint_checks(
        target_id=target_dir.name,
        rows=rows,
        constraints=_constraints(config),
    )
    selected = _select_model_target_weights(rows, preferred_method="limited_adjustment")
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_model_target_manifest",
        "target_id": target_dir.name,
        "as_of": target_date.isoformat(),
        "generated_at": generated.isoformat(),
        "status": constraint_checks["overall_status"],
        "config_path": str(config_path),
        "generated_methods": [row["target_method"] for row in rows],
        "recommended_research_method": selected["target_method"],
        "model_target_manifest_path": str(target_dir / "model_target_manifest.json"),
        "model_target_weights_path": str(target_dir / "model_target_weights.json"),
        "method_target_weights_path": str(target_dir / "method_target_weights.jsonl"),
        "target_constraint_checks_path": str(target_dir / "target_constraint_checks.json"),
        "model_target_report_path": str(target_dir / "model_target_report.md"),
        "source_summary": source["summary"],
        "warnings": source["warnings"],
        "method_policy": _mapping(config.get("method_policy")),
        **SYSTEM_TARGET_SAFETY,
    }
    model_target_weights = {
        "schema_version": SCHEMA_VERSION,
        "target_id": target_dir.name,
        "as_of": target_date.isoformat(),
        "recommended_research_method": selected["target_method"],
        "weights": selected["weights"],
        "method_count": len(rows),
        "method_weights": {row["target_method"]: row["weights"] for row in rows},
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(target_dir / "model_target_manifest.json", manifest)
    _write_json(target_dir / "model_target_weights.json", model_target_weights)
    _write_jsonl(target_dir / "method_target_weights.jsonl", rows)
    _write_json(target_dir / "target_constraint_checks.json", constraint_checks)
    _write_text(
        target_dir / "model_target_report.md",
        render_model_target_report(manifest, rows, constraint_checks),
    )
    _write_latest_pointer(
        "latest_model_target", target_dir.name, target_dir / "model_target_manifest.json"
    )
    return {
        "target_id": target_dir.name,
        "target_dir": target_dir,
        "manifest": manifest,
        "model_target_weights": model_target_weights,
        "method_target_weights": rows,
        "target_constraint_checks": constraint_checks,
    }


def model_target_report_payload(
    *,
    target_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MODEL_TARGET_DIR,
) -> dict[str, Any]:
    target_dir = _artifact_dir(
        artifact_id=target_id,
        latest_pointer="latest_model_target",
        latest=latest,
        output_dir=output_dir,
        required_name="model_target_manifest.json",
    )
    return {
        **_read_json(target_dir / "model_target_manifest.json"),
        "model_target_weights": _read_json(target_dir / "model_target_weights.json"),
        "method_target_weights": _read_jsonl(target_dir / "method_target_weights.jsonl"),
        "target_constraint_checks": _read_json(target_dir / "target_constraint_checks.json"),
        "target_dir": str(target_dir),
    }


def validate_model_target_artifact(
    *,
    target_id: str,
    output_dir: Path = DEFAULT_MODEL_TARGET_DIR,
) -> dict[str, Any]:
    target_dir = output_dir / target_id
    manifest = _read_optional_json(target_dir / "model_target_manifest.json") or {}
    weights = _read_optional_json(target_dir / "model_target_weights.json") or {}
    rows = _read_jsonl(target_dir / "method_target_weights.jsonl")
    checks_payload = _read_optional_json(target_dir / "target_constraint_checks.json") or {}
    checks = _required_file_checks(
        target_dir,
        (
            "model_target_manifest.json",
            "model_target_weights.json",
            "method_target_weights.jsonl",
            "target_constraint_checks.json",
            "model_target_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "target_id_matches",
                manifest.get("target_id") == target_id
                and weights.get("target_id") == target_id
                and all(row.get("target_id") == target_id for row in rows),
                target_id,
            ),
            _check(
                "required_methods_present",
                {
                    "static_baseline",
                    "consensus_target",
                    "limited_adjustment",
                    "defensive_limited_adjustment",
                }.issubset({str(row.get("target_method")) for row in rows}),
                "",
            ),
            _check(
                "weights_sum_to_one",
                all(_weights_sum_to_one(row.get("weights")) for row in rows),
                "",
            ),
            _check("all_rows_research_only", all(_payload_safe(row) for row in rows), ""),
            _check(
                "not_official_target_weights",
                manifest.get("not_official_target_weights") is True
                and weights.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "constraint_status_valid",
                checks_payload.get("overall_status") in {"PASS", "PASS_WITH_WARNINGS", "FAIL"},
                _text(checks_payload.get("overall_status")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, weights, checks_payload), ""),
        ]
    )
    return _validation_payload("etf_dynamic_v3_model_target_validation", target_id, checks)


def load_paper_shadow_config(path: Path = DEFAULT_PAPER_SHADOW_CONFIG_PATH) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_paper_shadow_config_safe(payload)
    return payload


def init_paper_shadow_account(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    config = load_paper_shadow_config(config_path)
    generated = generated_at or datetime.now(UTC)
    account = _mapping(config.get("paper_shadow_account"))
    tracked_methods = [
        str(item) for item in _mapping(config.get("tracking")).get("target_methods", [])
    ]
    if not tracked_methods:
        raise DynamicV3SystemTargetError("paper shadow account must track target methods")
    target_payload = _optional_latest_model_target_payload(model_target_dir)
    baseline_weights = _paper_initial_weights(config, target_payload)
    paper_shadow_id = _stable_id(
        "paper-shadow",
        config_path,
        account.get("name"),
        account.get("start_date"),
        tracked_methods,
        generated.isoformat(),
    )
    shadow_dir = _unique_dir(output_dir / paper_shadow_id)
    shadow_dir.mkdir(parents=True, exist_ok=False)
    method_rows = [
        {
            "paper_shadow_id": shadow_dir.name,
            "target_method": method,
            "as_of": _text(account.get("start_date"), generated.date().isoformat()),
            "portfolio_value": _float(account.get("initial_equity"), 100000.0),
            "weights": dict(baseline_weights),
            "cash_weight": _float(baseline_weights.get("CASH")),
            "turnover_to_date": 0.0,
            "last_rebalance_date": None,
            "broker_action_taken": False,
            "production_effect": PRODUCTION_EFFECT,
            **SYSTEM_TARGET_SAFETY,
        }
        for method in tracked_methods
    ]
    state = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_state",
        "paper_shadow_id": shadow_dir.name,
        "state_status": "INITIALIZED",
        "as_of": _text(account.get("start_date"), generated.date().isoformat()),
        "base_currency": _text(account.get("base_currency"), "USD"),
        "initial_equity": _float(account.get("initial_equity"), 100000.0),
        "tracked_methods": tracked_methods,
        "method_states": method_rows,
        "source_model_target_id": _text(
            _mapping(target_payload.get("model_target_weights")).get("target_id")
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_manifest",
        "paper_shadow_id": shadow_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "config_path": str(config_path),
        "paper_shadow_manifest_path": str(shadow_dir / "paper_shadow_manifest.json"),
        "paper_shadow_state_path": str(shadow_dir / "paper_shadow_state.json"),
        "paper_shadow_method_states_path": str(shadow_dir / "paper_shadow_method_states.jsonl"),
        "paper_shadow_report_path": str(shadow_dir / "paper_shadow_report.md"),
        "tracked_methods": tracked_methods,
        "source_model_target_id": state["source_model_target_id"],
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(shadow_dir / "paper_shadow_manifest.json", manifest)
    _write_json(shadow_dir / "paper_shadow_state.json", state)
    _write_jsonl(shadow_dir / "paper_shadow_method_states.jsonl", method_rows)
    _write_text(shadow_dir / "paper_shadow_report.md", render_paper_shadow_report(manifest, state))
    _write_latest_pointer(
        "latest_paper_shadow", shadow_dir.name, shadow_dir / "paper_shadow_manifest.json"
    )
    return {
        "paper_shadow_id": shadow_dir.name,
        "paper_shadow_dir": shadow_dir,
        "manifest": manifest,
        "state": state,
        "method_states": method_rows,
    }


def paper_shadow_state_payload(
    *,
    paper_shadow_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
) -> dict[str, Any]:
    shadow_dir = _artifact_dir(
        artifact_id=paper_shadow_id,
        latest_pointer="latest_paper_shadow",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_manifest.json",
    )
    return {
        **_read_json(shadow_dir / "paper_shadow_state.json"),
        "paper_shadow_dir": str(shadow_dir),
    }


def paper_shadow_report_payload(
    *,
    paper_shadow_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
) -> dict[str, Any]:
    shadow_dir = _artifact_dir(
        artifact_id=paper_shadow_id,
        latest_pointer="latest_paper_shadow",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_manifest.json",
    )
    return {
        **_read_json(shadow_dir / "paper_shadow_manifest.json"),
        "paper_shadow_state": _read_json(shadow_dir / "paper_shadow_state.json"),
        "paper_shadow_method_states": _read_jsonl(shadow_dir / "paper_shadow_method_states.jsonl"),
        "paper_shadow_dir": str(shadow_dir),
    }


def validate_paper_shadow_artifact(
    *,
    paper_shadow_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
) -> dict[str, Any]:
    shadow_dir = output_dir / paper_shadow_id
    manifest = _read_optional_json(shadow_dir / "paper_shadow_manifest.json") or {}
    state = _read_optional_json(shadow_dir / "paper_shadow_state.json") or {}
    rows = _read_jsonl(shadow_dir / "paper_shadow_method_states.jsonl")
    checks = _required_file_checks(
        shadow_dir,
        (
            "paper_shadow_manifest.json",
            "paper_shadow_state.json",
            "paper_shadow_method_states.jsonl",
            "paper_shadow_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "paper_shadow_id_matches",
                manifest.get("paper_shadow_id") == paper_shadow_id
                and state.get("paper_shadow_id") == paper_shadow_id
                and all(row.get("paper_shadow_id") == paper_shadow_id for row in rows),
                paper_shadow_id,
            ),
            _check("method_states_present", bool(rows), ""),
            _check(
                "each_method_has_state",
                len(rows) == len(set(row.get("target_method") for row in rows)),
                "",
            ),
            _check(
                "weights_sum_to_one",
                all(_weights_sum_to_one(row.get("weights")) for row in rows),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, state, *rows), ""),
        ]
    )
    return _validation_payload("etf_dynamic_v3_paper_shadow_validation", paper_shadow_id, checks)


def simulate_model_rebalance(
    *,
    paper_shadow_id: str,
    target_id: str,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    shadow_root = paper_shadow_dir / paper_shadow_id
    target_root = model_target_dir / target_id
    state = _read_json(shadow_root / "paper_shadow_state.json")
    manifest_target = _read_json(target_root / "model_target_manifest.json")
    method_targets = {
        row["target_method"]: row
        for row in _read_jsonl(target_root / "method_target_weights.jsonl")
    }
    constraint_rows = {
        row.get("target_method"): row
        for row in _records(_read_json(target_root / "target_constraint_checks.json").get("checks"))
    }
    current_rows = {
        row["target_method"]: dict(row)
        for row in _records(state.get("method_states"))
        if row.get("target_method")
    }
    events: list[dict[str, Any]] = []
    history: list[dict[str, Any]] = []
    next_rows: list[dict[str, Any]] = []
    for method in _texts(state.get("tracked_methods")):
        before = _normalize_weights(_mapping(current_rows.get(method, {}).get("weights")))
        target_row = method_targets.get(method)
        check = constraint_rows.get(method, {})
        if target_row is None:
            status = "INSUFFICIENT_DATA"
            target_weights: dict[str, float] = {}
            after = before
            turnover = 0.0
        elif check.get("status") == "FAIL":
            status = "SKIPPED"
            target_weights = _normalize_weights(_mapping(target_row.get("weights")))
            after = before
            turnover = 0.0
        else:
            status = "APPLIED_TO_PAPER"
            target_weights = _normalize_weights(_mapping(target_row.get("weights")))
            after = target_weights
            turnover = _turnover(before, target_weights)
        row = dict(current_rows.get(method, {}))
        row.update(
            {
                "paper_shadow_id": paper_shadow_id,
                "target_method": method,
                "as_of": manifest_target.get("as_of"),
                "weights": after,
                "cash_weight": _float(after.get("CASH")),
                "turnover_to_date": round(_float(row.get("turnover_to_date")) + turnover, 10),
                "last_rebalance_date": manifest_target.get("as_of")
                if status == "APPLIED_TO_PAPER"
                else row.get("last_rebalance_date"),
                "broker_action_taken": False,
                "production_effect": PRODUCTION_EFFECT,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        next_rows.append(row)
        events.append(
            {
                "rebalance_id": "",
                "date": manifest_target.get("as_of"),
                "target_method": method,
                "before_weights": before,
                "target_weights": target_weights,
                "after_weights": after,
                "deltas": _weight_deltas(before, target_weights or before),
                "turnover": turnover,
                "rebalance_status": status,
                "broker_action_taken": False,
                "production_effect": PRODUCTION_EFFECT,
                **SYSTEM_TARGET_SAFETY,
            }
        )
        history.append(
            {
                "date": manifest_target.get("as_of"),
                "target_method": method,
                "weights": after,
                "portfolio_value": _float(
                    row.get("portfolio_value"), _float(state.get("initial_equity"), 100000.0)
                ),
                "daily_return": 0.0,
                "drawdown": 0.0,
                "turnover": turnover,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    rebalance_id = _stable_id("model-rebalance", paper_shadow_id, target_id, generated.isoformat())
    rebalance_dir = _unique_dir(output_dir / rebalance_id)
    rebalance_dir.mkdir(parents=True, exist_ok=False)
    for event in events:
        event["rebalance_id"] = rebalance_dir.name
    turnover_summary = {
        "schema_version": SCHEMA_VERSION,
        "rebalance_id": rebalance_dir.name,
        "paper_shadow_id": paper_shadow_id,
        "target_id": target_id,
        "total_turnover": round(sum(_float(event.get("turnover")) for event in events), 10),
        "applied_methods": [
            event["target_method"]
            for event in events
            if event["rebalance_status"] == "APPLIED_TO_PAPER"
        ],
        "skipped_methods": [
            event["target_method"] for event in events if event["rebalance_status"] == "SKIPPED"
        ],
        "insufficient_data_methods": [
            event["target_method"]
            for event in events
            if event["rebalance_status"] == "INSUFFICIENT_DATA"
        ],
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_model_rebalance_manifest",
        "rebalance_id": rebalance_dir.name,
        "paper_shadow_id": paper_shadow_id,
        "target_id": target_id,
        "generated_at": generated.isoformat(),
        "status": "PASS"
        if not turnover_summary["insufficient_data_methods"]
        else "PASS_WITH_WARNINGS",
        "model_rebalance_manifest_path": str(rebalance_dir / "model_rebalance_manifest.json"),
        "rebalance_events_path": str(rebalance_dir / "rebalance_events.jsonl"),
        "method_weight_history_path": str(rebalance_dir / "method_weight_history.jsonl"),
        "rebalance_turnover_summary_path": str(rebalance_dir / "rebalance_turnover_summary.json"),
        "model_rebalance_report_path": str(rebalance_dir / "model_rebalance_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    state["method_states"] = next_rows
    state["as_of"] = manifest_target.get("as_of")
    state["state_status"] = "REBALANCED_TO_MODEL_TARGET"
    state["source_model_target_id"] = target_id
    state.update(SYSTEM_TARGET_SAFETY)
    _write_json(shadow_root / "paper_shadow_state.json", state)
    _write_jsonl(shadow_root / "paper_shadow_method_states.jsonl", next_rows)
    _write_json(rebalance_dir / "model_rebalance_manifest.json", manifest)
    _write_jsonl(rebalance_dir / "rebalance_events.jsonl", events)
    _write_jsonl(rebalance_dir / "method_weight_history.jsonl", history)
    _write_json(rebalance_dir / "rebalance_turnover_summary.json", turnover_summary)
    _write_text(
        rebalance_dir / "model_rebalance_report.md",
        render_model_rebalance_report(manifest, turnover_summary, events),
    )
    _write_latest_pointer(
        "latest_model_rebalance",
        rebalance_dir.name,
        rebalance_dir / "model_rebalance_manifest.json",
    )
    return {
        "rebalance_id": rebalance_dir.name,
        "rebalance_dir": rebalance_dir,
        "manifest": manifest,
        "rebalance_events": events,
        "method_weight_history": history,
        "rebalance_turnover_summary": turnover_summary,
    }


def model_rebalance_report_payload(
    *,
    rebalance_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
) -> dict[str, Any]:
    rebalance_dir = _artifact_dir(
        artifact_id=rebalance_id,
        latest_pointer="latest_model_rebalance",
        latest=latest,
        output_dir=output_dir,
        required_name="model_rebalance_manifest.json",
    )
    return {
        **_read_json(rebalance_dir / "model_rebalance_manifest.json"),
        "rebalance_events": _read_jsonl(rebalance_dir / "rebalance_events.jsonl"),
        "method_weight_history": _read_jsonl(rebalance_dir / "method_weight_history.jsonl"),
        "rebalance_turnover_summary": _read_json(rebalance_dir / "rebalance_turnover_summary.json"),
        "rebalance_dir": str(rebalance_dir),
    }


def validate_model_rebalance_artifact(
    *,
    rebalance_id: str,
    output_dir: Path = DEFAULT_MODEL_REBALANCE_DIR,
) -> dict[str, Any]:
    root = output_dir / rebalance_id
    manifest = _read_optional_json(root / "model_rebalance_manifest.json") or {}
    events = _read_jsonl(root / "rebalance_events.jsonl")
    summary = _read_optional_json(root / "rebalance_turnover_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "model_rebalance_manifest.json",
            "rebalance_events.jsonl",
            "method_weight_history.jsonl",
            "rebalance_turnover_summary.json",
            "model_rebalance_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "rebalance_id_matches",
                manifest.get("rebalance_id") == rebalance_id
                and summary.get("rebalance_id") == rebalance_id,
                "",
            ),
            _check("events_present", bool(events), ""),
            _check(
                "statuses_valid",
                all(
                    event.get("rebalance_status")
                    in {"APPLIED_TO_PAPER", "SKIPPED", "INSUFFICIENT_DATA"}
                    for event in events
                ),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, summary, *events), ""),
        ]
    )
    return _validation_payload("etf_dynamic_v3_model_rebalance_validation", rebalance_id, checks)


def run_paper_shadow_performance(
    *,
    paper_shadow_id: str,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
    price_cache_path: Path = DEFAULT_PRICE_CACHE_PATH,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    as_of: date | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    shadow_root = paper_shadow_dir / paper_shadow_id
    state = _read_json(shadow_root / "paper_shadow_state.json")
    method_rows = _records(state.get("method_states"))
    symbols = sorted(
        {
            symbol
            for row in method_rows
            for symbol in _mapping(row.get("weights"))
            if symbol != "CASH"
        }
    )
    performance_start = _coerce_date(state.get("as_of"), generated.date())
    evaluation_as_of = as_of or generated.date()
    if evaluation_as_of < performance_start:
        raise DynamicV3SystemTargetError(
            "performance as-of cannot predate paper shadow state as_of"
        )
    quality = _run_data_quality_gate(
        price_cache_path=price_cache_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=evaluation_as_of,
    )
    prices = _load_price_returns(price_cache_path, symbols, performance_start)
    performance_rows = [
        _method_performance(row, prices, _float(row.get("turnover_to_date"))) for row in method_rows
    ]
    static_return = _metric_for(performance_rows, "static_baseline", "total_return")
    no_trade_return = _metric_for(performance_rows, "no_trade_baseline", "total_return")
    for row in performance_rows:
        row["relative_to_static_baseline"] = round(
            _float(row.get("total_return")) - static_return, 10
        )
        row["relative_to_no_trade"] = round(_float(row.get("total_return")) - no_trade_return, 10)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "methods": performance_rows,
        "best_return_method": _best_method(performance_rows, "total_return", high=True),
        "best_drawdown_method": _best_method(performance_rows, "max_drawdown", high=True),
        "best_risk_adjusted_method": _best_method(
            performance_rows, "risk_adjusted_return_to_volatility", high=True
        ),
        "data_quality_status": quality.status,
        "data_quality_checked_at": quality.checked_at.isoformat(),
        "performance_start_date": performance_start.isoformat(),
        "evaluation_as_of": evaluation_as_of.isoformat(),
        "return_observation_count": len(prices),
        **SYSTEM_TARGET_SAFETY,
    }
    pairwise = _pairwise_comparisons(performance_rows)
    regime = _regime_breakdown(method_rows, prices)
    performance_id = _stable_id("paper-shadow-performance", paper_shadow_id, generated.isoformat())
    perf_dir = _unique_dir(output_dir / performance_id)
    perf_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_performance_manifest",
        "performance_id": perf_dir.name,
        "paper_shadow_id": paper_shadow_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if quality.passed else "FAIL",
        "performance_start_date": performance_start.isoformat(),
        "evaluation_as_of": evaluation_as_of.isoformat(),
        "paper_shadow_performance_manifest_path": str(
            perf_dir / "paper_shadow_performance_manifest.json"
        ),
        "method_performance_summary_path": str(perf_dir / "method_performance_summary.json"),
        "method_pairwise_comparison_path": str(perf_dir / "method_pairwise_comparison.json"),
        "regime_performance_breakdown_path": str(perf_dir / "regime_performance_breakdown.json"),
        "paper_shadow_performance_report_path": str(
            perf_dir / "paper_shadow_performance_report.md"
        ),
        "reader_brief_section_path": str(perf_dir / "reader_brief_section.md"),
        "data_quality_status": quality.status,
        "data_quality_report_visible": True,
        **SYSTEM_TARGET_SAFETY,
    }
    if not quality.passed:
        raise DynamicV3SystemTargetError(f"data quality gate failed: {quality.status}")
    _write_json(perf_dir / "paper_shadow_performance_manifest.json", manifest)
    _write_json(perf_dir / "method_performance_summary.json", summary)
    _write_json(perf_dir / "method_pairwise_comparison.json", pairwise)
    _write_json(perf_dir / "regime_performance_breakdown.json", regime)
    _write_text(
        perf_dir / "paper_shadow_performance_report.md",
        render_paper_shadow_performance_report(manifest, summary, pairwise, regime),
    )
    _write_text(perf_dir / "reader_brief_section.md", render_performance_reader_brief(summary))
    _write_latest_pointer(
        "latest_paper_shadow_performance",
        perf_dir.name,
        perf_dir / "paper_shadow_performance_manifest.json",
    )
    return {
        "performance_id": perf_dir.name,
        "performance_dir": perf_dir,
        "manifest": manifest,
        "method_performance_summary": summary,
        "method_pairwise_comparison": pairwise,
        "regime_performance_breakdown": regime,
    }


def paper_shadow_performance_report_payload(
    *,
    performance_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> dict[str, Any]:
    perf_dir = _artifact_dir(
        artifact_id=performance_id,
        latest_pointer="latest_paper_shadow_performance",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_performance_manifest.json",
    )
    return {
        **_read_json(perf_dir / "paper_shadow_performance_manifest.json"),
        "method_performance_summary": _read_json(perf_dir / "method_performance_summary.json"),
        "method_pairwise_comparison": _read_json(perf_dir / "method_pairwise_comparison.json"),
        "regime_performance_breakdown": _read_json(perf_dir / "regime_performance_breakdown.json"),
        "performance_dir": str(perf_dir),
    }


def validate_paper_shadow_performance_artifact(
    *,
    performance_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
) -> dict[str, Any]:
    root = output_dir / performance_id
    manifest = _read_optional_json(root / "paper_shadow_performance_manifest.json") or {}
    summary = _read_optional_json(root / "method_performance_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_performance_manifest.json",
            "method_performance_summary.json",
            "method_pairwise_comparison.json",
            "regime_performance_breakdown.json",
            "paper_shadow_performance_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check("performance_id_matches", manifest.get("performance_id") == performance_id, ""),
            _check("method_summary_present", bool(_records(summary.get("methods"))), ""),
            _check(
                "best_methods_present",
                bool(
                    summary.get("best_return_method")
                    and summary.get("best_drawdown_method")
                    and summary.get("best_risk_adjusted_method")
                ),
                "",
            ),
            _check("data_quality_visible", bool(manifest.get("data_quality_status")), ""),
            _check("broker_forbidden", _payload_safe(manifest, summary), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_performance_validation", performance_id, checks
    )


def build_system_target_review_pack(
    *,
    target_id: str,
    paper_shadow_id: str,
    performance_id: str,
    model_target_dir: Path = DEFAULT_MODEL_TARGET_DIR,
    paper_shadow_dir: Path = DEFAULT_PAPER_SHADOW_DIR,
    performance_dir: Path = DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    target = model_target_report_payload(target_id=target_id, output_dir=model_target_dir)
    paper = paper_shadow_report_payload(
        paper_shadow_id=paper_shadow_id, output_dir=paper_shadow_dir
    )
    performance = paper_shadow_performance_report_payload(
        performance_id=performance_id, output_dir=performance_dir
    )
    summary = _mapping(performance.get("method_performance_summary"))
    method_rows = _records(summary.get("methods"))
    review_policy = _mapping(_mapping(target.get("method_policy")).get("review_policy"))
    recommended = _recommended_research_method(
        method_rows,
        summary,
        preferred_order=_texts(review_policy.get("preferred_method_order")),
    )
    alternatives = [
        method
        for method in (
            "defensive_limited_adjustment",
            "consensus_target",
            "equal_weight_shadow_candidates",
            "selected_top_candidate",
        )
        if method != recommended and any(row.get("target_method") == method for row in method_rows)
    ][:3]
    decision_status = "INSUFFICIENT_DATA" if not method_rows else "CONTINUE_OBSERVATION"
    decision = {
        "review_id": "",
        "recommended_research_method": recommended,
        "alternative_methods": alternatives,
        "not_recommended_methods": [
            row.get("target_method")
            for row in method_rows
            if row.get("performance_status") == "INSUFFICIENT_DATA"
        ],
        "decision_status": decision_status,
        "reason": _review_reason(recommended, summary),
        **SYSTEM_TARGET_SAFETY,
    }
    review_id = _stable_id(
        "system-target-review", target_id, paper_shadow_id, performance_id, generated.isoformat()
    )
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    decision["review_id"] = review_dir.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_system_target_review_manifest",
        "review_id": review_dir.name,
        "system_target_review_id": review_dir.name,
        "target_id": target_id,
        "paper_shadow_id": paper_shadow_id,
        "performance_id": performance_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "recommended_research_method": recommended,
        "decision_status": decision_status,
        "system_target_review_manifest_path": str(
            review_dir / "system_target_review_manifest.json"
        ),
        "system_target_decision_path": str(review_dir / "system_target_decision.json"),
        "owner_research_review_checklist_path": str(
            review_dir / "owner_research_review_checklist.md"
        ),
        "system_target_review_report_path": str(review_dir / "system_target_review_report.md"),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    checklist = render_owner_research_review_checklist(decision)
    _write_json(review_dir / "system_target_review_manifest.json", manifest)
    _write_json(review_dir / "system_target_decision.json", decision)
    _write_text(review_dir / "owner_research_review_checklist.md", checklist)
    _write_text(
        review_dir / "system_target_review_report.md",
        render_system_target_review_report(manifest, decision, target, paper, performance),
    )
    _write_text(
        review_dir / "reader_brief_section.md", render_system_target_reader_brief(decision, summary)
    )
    _write_latest_pointer(
        "latest_system_target_review",
        review_dir.name,
        review_dir / "system_target_review_manifest.json",
    )
    return {
        "review_id": review_dir.name,
        "system_target_review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "system_target_decision": decision,
    }


def system_target_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir(
        artifact_id=review_id,
        latest_pointer="latest_system_target_review",
        latest=latest,
        output_dir=output_dir,
        required_name="system_target_review_manifest.json",
    )
    return {
        **_read_json(review_dir / "system_target_review_manifest.json"),
        "system_target_decision": _read_json(review_dir / "system_target_decision.json"),
        "review_dir": str(review_dir),
    }


def validate_system_target_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / review_id
    manifest = _read_optional_json(root / "system_target_review_manifest.json") or {}
    decision = _read_optional_json(root / "system_target_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "system_target_review_manifest.json",
            "system_target_decision.json",
            "owner_research_review_checklist.md",
            "system_target_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "review_id_matches",
                manifest.get("review_id") == review_id and decision.get("review_id") == review_id,
                "",
            ),
            _check(
                "recommended_method_present", bool(decision.get("recommended_research_method")), ""
            ),
            _check(
                "decision_status_valid",
                decision.get("decision_status")
                in {"CONTINUE_OBSERVATION", "REVIEW_REQUIRED", "INSUFFICIENT_DATA"},
                "",
            ),
            _check("research_target_only", decision.get("research_target_only") is True, ""),
            _check(
                "not_official_target_weights",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, decision), ""),
        ]
    )
    return _validation_payload("etf_dynamic_v3_system_target_review_validation", review_id, checks)


def render_model_target_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    checks: Mapping[str, Any],
) -> str:
    lines = [
        f"# Research Model Target Portfolio {manifest.get('target_id')}",
        "",
        f"- as_of: {manifest.get('as_of')}",
        f"- generated_methods: {', '.join(_texts(manifest.get('generated_methods')))}",
        f"- recommended_research_method: {manifest.get('recommended_research_method')}",
        f"- constraint_status: {checks.get('overall_status')}",
        "- official_target_weights_written: false",
        "- broker_action_allowed: false",
        "- research_target_only: true",
        "",
        "## Method Weights",
        "",
    ]
    for row in rows:
        lines.append(
            f"- {row.get('target_method')}: {json.dumps(row.get('weights'), sort_keys=True)}"
        )
    lines.extend(
        [
            "",
            "该报告只生成 research model target weights，"
            "不写 official target weights，不触发 broker。",
            "",
        ]
    )
    return "\n".join(lines)


def render_paper_shadow_report(manifest: Mapping[str, Any], state: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Account {manifest.get('paper_shadow_id')}",
            "",
            f"- state_status: {state.get('state_status')}",
            f"- initial_equity: {state.get('initial_equity')}",
            f"- tracked_methods: {', '.join(_texts(state.get('tracked_methods')))}",
            "- broker_connected: false",
            "- real_trade_triggered: false",
            "- paper_shadow_only: true",
            "",
        ]
    )


def render_model_rebalance_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            f"# Model Target Paper Rebalance {manifest.get('rebalance_id')}",
            "",
            f"- paper_shadow_id: {manifest.get('paper_shadow_id')}",
            f"- target_id: {manifest.get('target_id')}",
            f"- total_turnover: {summary.get('total_turnover')}",
            f"- applied_methods: {', '.join(_texts(summary.get('applied_methods')))}",
            f"- skipped_methods: {', '.join(_texts(summary.get('skipped_methods')))}",
            "- insufficient_data_methods: "
            f"{', '.join(_texts(summary.get('insufficient_data_methods')))}",
            "- broker_action_taken: false",
            "",
            "## Events",
            "",
            *[
                f"- {event.get('target_method')}: {event.get('rebalance_status')} "
                f"turnover={event.get('turnover')}"
                for event in events
            ],
            "",
        ]
    )


def render_paper_shadow_performance_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    pairwise: Mapping[str, Any],
    regime: Mapping[str, Any],
) -> str:
    limited = _find_method(_records(summary.get("methods")), "limited_adjustment")
    consensus = _find_method(_records(summary.get("methods")), "consensus_target")
    defensive = _find_method(_records(summary.get("methods")), "defensive_limited_adjustment")
    return "\n".join(
        [
            f"# Paper Shadow Performance {manifest.get('performance_id')}",
            "",
            f"- data_quality_status: {manifest.get('data_quality_status')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- best_risk_adjusted_method: {summary.get('best_risk_adjusted_method')}",
            "- limited_adjustment_vs_static_baseline: "
            f"{limited.get('relative_to_static_baseline')}",
            f"- defensive_limited_adjustment_max_drawdown: {defensive.get('max_drawdown')}",
            f"- consensus_target_max_drawdown: {consensus.get('max_drawdown')}",
            f"- pairwise_count: {len(_records(pairwise.get('comparisons')))}",
            f"- regime_count: {len(_records(regime.get('regimes')))}",
            "- broker_action_taken: false",
            "",
        ]
    )


def render_performance_reader_brief(summary: Mapping[str, Any]) -> str:
    limited = _find_method(_records(summary.get("methods")), "limited_adjustment")
    return "\n".join(
        [
            "## Dynamic Rescue System Target Portfolio",
            "",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- best_risk_adjusted_method: {summary.get('best_risk_adjusted_method')}",
            "- limited_adjustment_vs_static_baseline: "
            f"{limited.get('relative_to_static_baseline')}",
            "- research_target_only: true",
            "- broker_action_allowed: false",
            "",
        ]
    )


def render_owner_research_review_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Review Checklist {decision.get('review_id')}",
            "",
            "- 是否接受 limited_adjustment 继续作为主要 research target？",
            "- 是否接受 consensus_target 只作为 upper-bound reference？",
            "- 是否接受 defensive_limited_adjustment 仍为 research-only？",
            "- 是否继续运行 paper shadow account？",
            "- 是否需要调整 simulation / forward confirmation 重点？",
            "- 是否确认禁止 broker / production / order ticket？必须确认。",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_system_target_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    target: Mapping[str, Any],
    paper: Mapping[str, Any],
    performance: Mapping[str, Any],
) -> str:
    summary = _mapping(performance.get("method_performance_summary"))
    return "\n".join(
        [
            f"# System Target Portfolio Review {manifest.get('review_id')}",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            f"- target_id: {target.get('target_id')}",
            f"- paper_shadow_id: {paper.get('paper_shadow_id')}",
            f"- performance_id: {performance.get('performance_id')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            "",
            "收益最高 method 不会自动采用；review pack 只建议继续观察并等待 forward confirmation。",
            "consensus_target 仍只是 upper-bound reference；"
            "defensive_limited_adjustment 仍未批准为 production rule。",
            "当前输出不写 official target weights，不触发 broker，不生成 order ticket。",
            "",
        ]
    )


def render_system_target_reader_brief(
    decision: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue System Target Portfolio",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- best_return_method: {summary.get('best_return_method')}",
            f"- best_drawdown_method: {summary.get('best_drawdown_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- research_target_only: true",
            "- broker_action_allowed: false",
            "- next_action: continue_paper_shadow_observation",
            "",
        ]
    )


def _latest_target_source(
    *,
    position_advisory_daily_dir: Path,
    shadow_monitor_dir: Path,
    shadow_shortlist_dir: Path,
    consensus_drift_dir: Path,
) -> dict[str, Any]:
    warnings: list[str] = []
    candidate_rows: list[dict[str, Any]] = []
    consensus_weights: dict[str, float] = {}
    summary: dict[str, Any] = {}
    daily_dir = _latest_child_dir_with(position_advisory_daily_dir, "daily_candidate_targets.jsonl")
    if daily_dir is not None:
        candidate_rows = _read_jsonl(daily_dir / "daily_candidate_targets.jsonl")
        consensus_weights = _read_consensus_weights_csv(daily_dir / "daily_consensus_weights.csv")
        actions = _read_optional_json(daily_dir / "daily_advisory_actions.json") or {}
        summary.update(
            {
                "source_daily_advisory_id": actions.get("daily_advisory_id", daily_dir.name),
                "source_daily_advisory_dir": str(daily_dir),
                "consensus_status": actions.get("consensus_status", ""),
            }
        )
    else:
        warnings.append("latest_position_advisory_daily_missing")
    monitor_dir = _latest_child_dir_with(shadow_monitor_dir, "shadow_candidate_daily_results.jsonl")
    monitor_manifest: dict[str, Any] = {}
    if monitor_dir is not None:
        monitor_manifest = _read_optional_json(monitor_dir / "shadow_monitor_manifest.json") or {}
        if not candidate_rows:
            candidate_rows = _read_jsonl(monitor_dir / "shadow_candidate_daily_results.jsonl")
        summary.update(
            {
                "source_shadow_monitor_run_id": monitor_manifest.get(
                    "monitor_run_id", monitor_dir.name
                ),
                "source_shadow_monitor_dir": str(monitor_dir),
            }
        )
    else:
        warnings.append("latest_shadow_monitor_missing")
    shortlist_dir = _latest_child_dir_with(shadow_shortlist_dir, "shadow_shortlist_manifest.json")
    shortlist_manifest: dict[str, Any] = {}
    if shortlist_dir is not None:
        shortlist_manifest = (
            _read_optional_json(shortlist_dir / "shadow_shortlist_manifest.json") or {}
        )
        summary.update(
            {
                "source_shadow_shortlist_id": shortlist_manifest.get(
                    "shadow_shortlist_id", shortlist_dir.name
                ),
                "source_shadow_shortlist_dir": str(shortlist_dir),
            }
        )
    else:
        warnings.append("latest_shadow_shortlist_missing")
    drift_dir = _latest_child_dir_with(consensus_drift_dir, "consensus_drift_summary.json")
    drift_summary: dict[str, Any] = {}
    if drift_dir is not None:
        drift_summary = _read_optional_json(drift_dir / "consensus_drift_summary.json") or {}
        summary.update(
            {
                "source_consensus_drift_id": drift_summary.get("drift_id", drift_dir.name),
                "disagreement_status": drift_summary.get("disagreement_status", ""),
            }
        )
    else:
        warnings.append("latest_consensus_drift_missing")
    if not candidate_rows:
        warnings.append("candidate_target_weights_missing")
    return {
        "candidate_targets": candidate_rows,
        "consensus_weights": consensus_weights,
        "shadow_shortlist_id": _text(shortlist_manifest.get("shadow_shortlist_id")),
        "shadow_monitor_run_id": _text(monitor_manifest.get("monitor_run_id")),
        "consensus_drift_id": _text(drift_summary.get("drift_id")),
        "summary": summary,
        "warnings": warnings,
    }


def _read_consensus_weights_csv(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    frame = pd.read_csv(path)
    if not {"symbol", "mean_target_weight"}.issubset(frame.columns):
        return {}
    return {
        str(row["symbol"]).strip().upper(): _float(row["mean_target_weight"])
        for _, row in frame.iterrows()
        if str(row["symbol"]).strip()
    }


def _average_candidate_weights(candidates: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    rows = [
        _mapping(row.get("target_weights"))
        for row in candidates
        if _mapping(row.get("target_weights"))
    ]
    if not rows:
        return {}
    symbols = sorted({symbol for row in rows for symbol in row})
    return _normalize_weights(
        {symbol: sum(_float(row.get(symbol)) for row in rows) / len(rows) for symbol in symbols}
    )


def _first_candidate_weights(candidates: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    if not candidates:
        return {}
    sorted_rows = sorted(
        candidates,
        key=lambda row: (
            _float(row.get("shortlist_rank"), 9999),
            -_float(row.get("shortlist_score")),
            str(row.get("candidate_id")),
        ),
    )
    return _mapping(sorted_rows[0].get("target_weights"))


def _limited_adjustment(
    *,
    baseline: Mapping[str, float],
    target: Mapping[str, float],
    max_total_adjustment: float,
    max_symbol_adjustment: float,
) -> dict[str, float]:
    base = _normalize_weights(baseline)
    desired = _normalize_weights(target)
    symbols = sorted(set(base) | set(desired))
    deltas = {
        symbol: max(
            -max_symbol_adjustment,
            min(max_symbol_adjustment, _float(desired.get(symbol)) - _float(base.get(symbol))),
        )
        for symbol in symbols
    }
    total_abs = sum(abs(value) for value in deltas.values())
    if total_abs > max_total_adjustment and total_abs > 0:
        scale = max_total_adjustment / total_abs
        deltas = {symbol: value * scale for symbol, value in deltas.items()}
    adjusted = {symbol: _float(base.get(symbol)) + deltas.get(symbol, 0.0) for symbol in symbols}
    return _normalize_weights(adjusted)


def _defensive_adjustment(
    weights: Mapping[str, float], policy: Mapping[str, Any]
) -> dict[str, float]:
    result = dict(_normalize_weights(weights))
    required = {
        "semiconductor_symbols",
        "growth_symbols",
        "semiconductor_reduction",
        "growth_reduction",
        "max_cash_weight",
    }
    if not required.issubset(policy):
        raise DynamicV3SystemTargetError("defensive_limited_adjustment policy is incomplete")
    semis = [str(item).upper() for item in policy.get("semiconductor_symbols", [])]
    growth_symbols = [str(item).upper() for item in policy.get("growth_symbols", [])]
    semiconductor_reduction = _float(policy.get("semiconductor_reduction"))
    growth_reduction = _float(policy.get("growth_reduction"))
    cash_room = max(0.0, _float(policy.get("max_cash_weight")) - _float(result.get("CASH")))
    moved = _reduce_symbols(result, semis, min(semiconductor_reduction, cash_room))
    cash_room -= moved
    moved += _reduce_symbols(result, growth_symbols, min(growth_reduction, cash_room))
    result["CASH"] = _float(result.get("CASH")) + moved
    return _normalize_weights(result)


def _reduce_symbols(weights: dict[str, float], symbols: Sequence[str], amount: float) -> float:
    if amount <= 0:
        return 0.0
    available = sum(max(0.0, _float(weights.get(symbol))) for symbol in symbols)
    if available <= 0:
        return 0.0
    moved = min(amount, available)
    for symbol in symbols:
        share = max(0.0, _float(weights.get(symbol))) / available
        weights[symbol] = max(0.0, _float(weights.get(symbol)) - moved * share)
    return moved


def _constraint_checks(
    *,
    target_id: str,
    rows: Sequence[Mapping[str, Any]],
    constraints: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    for row in rows:
        weights = _normalize_weights(_mapping(row.get("weights")))
        violations: list[str] = []
        warnings: list[str] = []
        max_single = _float(constraints.get("max_single_symbol_weight"), 1.0)
        max_semi = _float(constraints.get("max_semiconductor_weight"), 1.0)
        min_cash = _float(constraints.get("min_cash_weight"), 0.0)
        max_risk = _float(constraints.get("max_total_risk_asset_weight"), 1.0)
        semiconductor_symbols = _texts(constraints.get("semiconductor_symbols")) or ["SMH", "SOXX"]
        defensive_symbols = set(_texts(constraints.get("defensive_symbols")) or ["CASH", "TLT"])
        if max(weights.values(), default=0.0) > max_single:
            violations.append("max_single_symbol_weight_exceeded")
        if sum(_float(weights.get(symbol)) for symbol in semiconductor_symbols) > max_semi:
            violations.append("max_semiconductor_weight_exceeded")
        if _float(weights.get("CASH")) < min_cash:
            violations.append("min_cash_weight_not_met")
        risk_weight = sum(
            value for symbol, value in weights.items() if symbol not in defensive_symbols
        )
        if risk_weight > max_risk:
            violations.append("max_total_risk_asset_weight_exceeded")
        if _float(weights.get("CASH")) < min_cash + 0.02:
            warnings.append("cash_near_minimum")
        checks.append(
            {
                "target_method": row.get("target_method"),
                "status": "FAIL" if violations else "PASS_WITH_WARNINGS" if warnings else "PASS",
                "warnings": warnings,
                "violations": violations,
            }
        )
    overall = (
        "FAIL"
        if any(row["status"] == "FAIL" for row in checks)
        else "PASS_WITH_WARNINGS"
        if any(row["status"] == "PASS_WITH_WARNINGS" for row in checks)
        else "PASS"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "target_id": target_id,
        "checks": checks,
        "overall_status": overall,
        **SYSTEM_TARGET_SAFETY,
    }


def _select_model_target_weights(
    rows: Sequence[Mapping[str, Any]],
    *,
    preferred_method: str,
) -> dict[str, Any]:
    for row in rows:
        if row.get("target_method") == preferred_method:
            return dict(row)
    return dict(rows[0]) if rows else {"target_method": "", "weights": {}}


def _run_data_quality_gate(
    *,
    price_cache_path: Path,
    rates_cache_path: Path,
    expected_symbols: Sequence[str],
    as_of: date,
) -> DataQualityReport:
    return validate_data_cache(
        prices_path=price_cache_path,
        rates_path=rates_cache_path,
        expected_price_tickers=list(expected_symbols),
        expected_rate_series=[],
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=price_cache_path.parent / "download_manifest.csv",
        secondary_prices_path=price_cache_path.parent / "prices_marketstack_daily.csv",
        require_secondary_prices=False,
    )


def _load_price_returns(path: Path, symbols: Sequence[str], start: date) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if not {"date", "ticker", "adj_close"}.issubset(frame.columns):
        raise DynamicV3SystemTargetError("price cache must contain date,ticker,adj_close")
    frame = frame.loc[frame["ticker"].astype(str).isin(symbols)].copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")
    frame = frame.loc[frame["date"].notna() & frame["adj_close"].notna()]
    frame = frame.loc[frame["date"].dt.date >= start]
    pivot = frame.pivot_table(
        index="date", columns="ticker", values="adj_close", aggfunc="last"
    ).sort_index()
    returns = pivot.pct_change().dropna(how="all").fillna(0.0)
    return returns


def _method_performance(
    row: Mapping[str, Any],
    returns: pd.DataFrame,
    turnover: float,
) -> dict[str, Any]:
    weights = _normalize_weights(_mapping(row.get("weights")))
    if returns.empty:
        return {
            "target_method": row.get("target_method"),
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": turnover,
            "relative_to_static_baseline": 0.0,
            "relative_to_no_trade": 0.0,
            "risk_adjusted_return_to_volatility": 0.0,
            "performance_status": "INSUFFICIENT_DATA",
        }
    series = pd.Series(0.0, index=returns.index)
    for symbol, weight in weights.items():
        if symbol == "CASH":
            continue
        if symbol in returns.columns:
            series = series + returns[symbol].fillna(0.0) * weight
    equity = (1.0 + series).cumprod()
    total_return = float(equity.iloc[-1] - 1.0)
    periods = max(1, len(series))
    annualized = (
        float((1.0 + total_return) ** (252.0 / periods) - 1.0) if total_return > -1 else -1.0
    )
    volatility = float(series.std(ddof=0) * math.sqrt(252.0)) if periods > 1 else 0.0
    drawdown = equity / equity.cummax() - 1.0
    max_drawdown = float(drawdown.min())
    risk_adjusted = annualized / volatility if volatility > 0 else annualized
    return {
        "target_method": row.get("target_method"),
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(max_drawdown, 10),
        "realized_volatility": round(volatility, 10),
        "turnover": round(turnover, 10),
        "relative_to_static_baseline": 0.0,
        "relative_to_no_trade": 0.0,
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "performance_status": "PASS",
    }


def _pairwise_comparisons(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            return_delta = _float(left.get("total_return")) - _float(right.get("total_return"))
            drawdown_delta = _float(left.get("max_drawdown")) - _float(right.get("max_drawdown"))
            turnover_delta = _float(left.get("turnover")) - _float(right.get("turnover"))
            if (
                left.get("performance_status") == "INSUFFICIENT_DATA"
                or right.get("performance_status") == "INSUFFICIENT_DATA"
            ):
                conclusion = "insufficient_data"
            elif return_delta > 0 and drawdown_delta >= 0:
                conclusion = "method_a_better"
            elif return_delta < 0 and drawdown_delta <= 0:
                conclusion = "method_b_better"
            else:
                conclusion = "mixed"
            comparisons.append(
                {
                    "method_a": left.get("target_method"),
                    "method_b": right.get("target_method"),
                    "return_delta": round(return_delta, 10),
                    "drawdown_delta": round(drawdown_delta, 10),
                    "turnover_delta": round(turnover_delta, 10),
                    "conclusion": conclusion,
                }
            )
    return {"schema_version": SCHEMA_VERSION, "comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _regime_breakdown(
    method_rows: Sequence[Mapping[str, Any]], returns: pd.DataFrame
) -> dict[str, Any]:
    regimes = []
    if returns.empty:
        return {"schema_version": SCHEMA_VERSION, "regimes": [], **SYSTEM_TARGET_SAFETY}
    labels = pd.Series("normal", index=returns.index)
    if "QQQ" in returns.columns:
        labels.loc[returns["QQQ"] <= PRESSURE_RETURN_THRESHOLD] = "tech_drawdown"
    if "SMH" in returns.columns:
        labels.loc[returns["SMH"] <= PRESSURE_RETURN_THRESHOLD] = "semiconductor_pressure"
    for regime in ("tech_drawdown", "semiconductor_pressure", "normal"):
        selected = returns.loc[labels == regime]
        methods = []
        for row in method_rows:
            perf = _method_performance(row, selected, 0.0)
            methods.append(
                {
                    "target_method": row.get("target_method"),
                    "return": perf["total_return"],
                    "max_drawdown": perf["max_drawdown"],
                    "relative_to_no_trade": 0.0,
                    "status": perf["performance_status"],
                }
            )
        no_trade = _find_method(methods, "no_trade_baseline").get("return", 0.0)
        for item in methods:
            item["relative_to_no_trade"] = round(_float(item.get("return")) - _float(no_trade), 10)
        regimes.append({"regime": regime, "methods": methods})
    return {"schema_version": SCHEMA_VERSION, "regimes": regimes, **SYSTEM_TARGET_SAFETY}


def _recommended_research_method(
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    *,
    preferred_order: Sequence[str] | None = None,
) -> str:
    available = {
        row.get("target_method")
        for row in rows
        if row.get("performance_status") != "INSUFFICIENT_DATA"
    }
    configured_order = preferred_order or (
        "limited_adjustment",
        "defensive_limited_adjustment",
        "consensus_target",
    )
    for method in configured_order:
        if method in available:
            return method
    observed_methods = {row.get("target_method") for row in rows}
    for method in configured_order:
        if method in observed_methods:
            return method
    best = _text(summary.get("best_risk_adjusted_method"))
    return best if best and best != "INSUFFICIENT_DATA" else "limited_adjustment"


def _review_reason(recommended: str, summary: Mapping[str, Any]) -> str:
    return (
        f"{recommended} is selected for continued research observation. "
        f"Best return method is {summary.get('best_return_method')}, but return alone does not "
        "approve official target weights or broker action."
    )


def _metric_for(rows: Sequence[Mapping[str, Any]], method: str, field: str) -> float:
    return _float(_find_method(rows, method).get(field))


def _find_method(rows: Sequence[Mapping[str, Any]], method: str) -> dict[str, Any]:
    for row in rows:
        if row.get("target_method") == method:
            return dict(row)
    return {}


def _best_method(rows: Sequence[Mapping[str, Any]], field: str, *, high: bool) -> str:
    valid = [row for row in rows if row.get("performance_status") != "INSUFFICIENT_DATA"]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        max(valid, key=lambda row: _float(row.get(field))).get("target_method")
        if high
        else min(valid, key=lambda row: _float(row.get(field))).get("target_method")
    )


def _paper_initial_weights(
    config: Mapping[str, Any], target_payload: Mapping[str, Any]
) -> dict[str, float]:
    target_weights = _mapping(
        _mapping(target_payload.get("model_target_weights")).get("method_weights")
    )
    initial_method = _text(
        _mapping(config.get("paper_shadow_account")).get("initial_method"), "static_baseline"
    )
    if initial_method in target_weights:
        return _normalize_weights(_mapping(target_weights[initial_method]))
    baseline = _mapping(config.get("baseline")).get("static_weights")
    if isinstance(baseline, Mapping):
        return _normalize_weights(baseline)
    return {"QQQ": 0.50, "SMH": 0.20, "TLT": 0.10, "CASH": 0.20}


def _optional_latest_model_target_payload(output_dir: Path) -> dict[str, Any]:
    try:
        return model_target_report_payload(latest=True, output_dir=output_dir)
    except DynamicV3SystemTargetError:
        return {}


def _load_advisory_limits(path_or_value: object) -> dict[str, Any]:
    path = Path(str(path_or_value)) if path_or_value else DEFAULT_POSITION_ADVISORY_CONFIG_PATH
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return {}
    payload = _load_yaml_mapping(path)
    return _mapping(payload.get("advisory_limits"))


def _assert_model_target_config_safe(payload: Mapping[str, Any]) -> None:
    model_target = _mapping(payload.get("model_target"))
    if model_target.get("mode") != "research_target_only":
        raise DynamicV3SystemTargetError("model target config must use research_target_only mode")
    if model_target.get("not_official_target_weights") is not True:
        raise DynamicV3SystemTargetError(
            "model target config must mark not_official_target_weights"
        )
    if model_target.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("model target config must mark paper_shadow_only")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("model target safety fields are unsafe")
    unknown = set(_enabled_methods(payload)) - set(TARGET_METHODS)
    if unknown:
        raise DynamicV3SystemTargetError(f"unknown target methods: {sorted(unknown)}")


def _assert_paper_shadow_config_safe(payload: Mapping[str, Any]) -> None:
    account = _mapping(payload.get("paper_shadow_account"))
    if account.get("mode") != "paper_shadow_only":
        raise DynamicV3SystemTargetError("paper shadow config must use paper_shadow_only mode")
    if _coerce_date(account.get("start_date"), date(1970, 1, 1)) < date(2022, 12, 1):
        raise DynamicV3SystemTargetError("paper shadow start_date cannot predate 2022-12-01")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("paper shadow safety fields are unsafe")


def _safety_config_locked(safety: Mapping[str, Any]) -> bool:
    return (
        safety.get("broker_action_allowed") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("production_effect") == PRODUCTION_EFFECT
        and safety.get("auto_apply", False) is False
    )


def _payload_safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("research_target_only", True) is True
        and payload.get("paper_shadow_only", True) is True
        and payload.get("not_official_target_weights", True) is True
        and payload.get("broker_action_allowed") is not True
        and payload.get("broker_action_taken") is not True
        and payload.get("order_ticket_generated") is not True
        and payload.get("production_state_mutated") is not True
        and payload.get("baseline_config_mutated") is not True
        and payload.get("official_target_weights_mutated") is not True
        and payload.get("production_candidate_generated") is not True
        and payload.get("automatic_candidate_promotion") is not True
        and payload.get("auto_apply") is not True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        for payload in payloads
        if payload
    )


def _enabled_methods(config: Mapping[str, Any]) -> list[str]:
    methods = _mapping(_mapping(config.get("target_methods")).get("enabled"))
    if methods:
        return [str(item) for item in methods.values()]
    raw = _mapping(config.get("target_methods")).get("enabled", [])
    if isinstance(raw, Sequence) and not isinstance(raw, str | bytes):
        return [str(item) for item in raw]
    return []


def _config_baseline_weights(config: Mapping[str, Any]) -> dict[str, float]:
    return _normalize_weights(_mapping(_mapping(config.get("baseline")).get("static_weights")))


def _constraints(config: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(config.get("constraints"))


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    cleaned = {
        str(symbol).strip().upper(): max(0.0, _float(value)) for symbol, value in weights.items()
    }
    cleaned = {symbol: value for symbol, value in cleaned.items() if symbol and value > 0}
    total = sum(cleaned.values())
    if total <= 0:
        raise DynamicV3SystemTargetError("weights must contain positive values")
    normalized = {symbol: round(value / total, 10) for symbol, value in sorted(cleaned.items())}
    residual = round(1.0 - sum(normalized.values()), 10)
    cash_symbol = "CASH" if "CASH" in normalized else sorted(normalized)[0]
    normalized[cash_symbol] = round(normalized[cash_symbol] + residual, 10)
    return normalized


def _weights_sum_to_one(value: object) -> bool:
    try:
        weights = _normalize_weights(_mapping(value))
    except DynamicV3SystemTargetError:
        return False
    return abs(sum(weights.values()) - 1.0) <= 0.000001


def _turnover(before: Mapping[str, float], after: Mapping[str, float]) -> float:
    symbols = set(before) | set(after)
    return round(
        sum(abs(_float(after.get(symbol)) - _float(before.get(symbol))) for symbol in symbols)
        / 2.0,
        10,
    )


def _weight_deltas(before: Mapping[str, float], after: Mapping[str, float]) -> dict[str, float]:
    return {
        symbol: round(_float(after.get(symbol)) - _float(before.get(symbol)), 10)
        for symbol in sorted(set(before) | set(after))
    }


def _validation_payload(
    report_type: str,
    artifact_id: str,
    checks: Sequence[Mapping[str, Any]],
    *,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    status = "PASS" if all(check.get("passed") is True for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "artifact_id": artifact_id,
        "status": status,
        "checks": list(checks),
        "failed_check_count": sum(1 for check in checks if check.get("passed") is not True),
        **dict(extra or {}),
        **SYSTEM_TARGET_SAFETY,
    }


def _required_file_checks(root: Path, names: Sequence[str]) -> list[dict[str, Any]]:
    return [
        _check(f"artifact_exists:{name}", (root / name).exists(), str(root / name))
        for name in names
    ]


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}


def _artifact_dir(
    *,
    artifact_id: str | None,
    latest_pointer: str,
    latest: bool,
    output_dir: Path,
    required_name: str,
) -> Path:
    resolved_id = artifact_id
    if latest:
        resolved_id = _latest_pointer_artifact_id(latest_pointer)
    if not resolved_id:
        raise DynamicV3SystemTargetError(
            f"--{latest_pointer.removeprefix('latest_')}-id or --latest is required"
        )
    root = output_dir / resolved_id
    if not (root / required_name).exists():
        raise DynamicV3SystemTargetError(f"artifact not found: {root / required_name}")
    return root


def _latest_pointer_artifact_id(name: str) -> str:
    payload = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{name}.json") or {}
    return _text(payload.get("artifact_id"))


def _write_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    if not _is_default_dynamic_v3_research_artifact(path):
        return
    DEFAULT_LATEST_POINTER_DIR.mkdir(parents=True, exist_ok=True)
    _write_json(
        DEFAULT_LATEST_POINTER_DIR / f"{name}.json",
        {
            "schema_version": SCHEMA_VERSION,
            "artifact_type": name.removeprefix("latest_"),
            "artifact_id": artifact_id,
            "path": str(path),
            "updated_at": datetime.now(UTC).isoformat(),
            "exists": path.exists(),
        },
    )


def _latest_child_dir_with(root: Path, filename: str) -> Path | None:
    if not root.exists():
        return None
    candidates = [path.parent for path in root.glob(f"*/{filename}") if path.is_file()]
    return (
        max(candidates, key=lambda path: (path / filename).stat().st_mtime) if candidates else None
    )


def _is_default_dynamic_v3_research_artifact(path: Path) -> bool:
    try:
        path.resolve().relative_to(DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve())
    except ValueError:
        return False
    return True


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.name}-{index:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3SystemTargetError(f"unable to allocate unique artifact dir under {path.parent}")


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, Mapping):
        raise DynamicV3SystemTargetError(f"YAML root must be mapping: {path}")
    return dict(raw)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3SystemTargetError(f"required JSON artifact not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise DynamicV3SystemTargetError(f"JSON artifact must be object: {path}")
    return payload


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _stable_id(prefix: str, *parts: object) -> str:
    digest = sha256(
        json.dumps(parts, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()
    return f"{prefix}_{digest[:16]}"


def _coerce_date(value: object, default: date) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return default


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: object) -> list[str]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str) and value:
        return [value]
    return []


def _text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
