from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
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
DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "paper_shadow_backfill_v1.yaml"
)
DEFAULT_PRICE_CACHE_PATH = PROJECT_ROOT / "data" / "raw" / "prices_daily.csv"
DEFAULT_MODEL_TARGET_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_target"
DEFAULT_PAPER_SHADOW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow"
DEFAULT_MODEL_REBALANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "model_rebalance"
DEFAULT_PAPER_SHADOW_PERFORMANCE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_performance"
DEFAULT_SYSTEM_TARGET_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "system_target_review"
DEFAULT_PAPER_SHADOW_BACKFILL_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_backfill"
DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_rolling_eval"
)
DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_regime_review"
)
DEFAULT_PAPER_SHADOW_STABILITY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_shadow_stability"
DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "system_target_selection_review"
)
DEFAULT_SELECTION_ATTRIBUTION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "selection_attribution"
DEFAULT_LIMITED_LONG_RISK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_long_risk"
DEFAULT_LIMITED_CONSISTENCY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_consistency"
DEFAULT_DATA_WARNING_IMPACT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "data_warning_impact"
DEFAULT_RESEARCH_METHOD_HARDENING_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "research_method_hardening"
)

AI_AFTER_CHATGPT_START = date(2022, 12, 1)

# Reporting bucket boundary, not an approval or allocation rule. The 2% daily
# loss level is intentionally named so future calibration work can audit it.
PRESSURE_RETURN_THRESHOLD = -0.02

# Reporting sufficiency floor used only to label thin diagnostic windows.
DEFAULT_MIN_EVAL_OBSERVATIONS = 20

# Reporting-only data quality penalties used in attribution explanations. These
# do not recompute or replace the original system target selection score.
DATA_QUALITY_WARNING_ATTRIBUTION_PENALTY = 0.05
DATA_QUALITY_FAIL_ATTRIBUTION_PENALTY = 0.25

# Reporting confidence floors for long-window review labels. They only affect
# confidence text, not recommendation or hardening decisions.
LONG_WINDOW_HIGH_CONFIDENCE_OBSERVATIONS = 504
LONG_WINDOW_MEDIUM_CONFIDENCE_OBSERVATIONS = 252

# Exposure interpretation tolerance for reporting whether limited_adjustment
# materially changes risk-asset weight relative to static baseline.
EXPOSURE_SIMILARITY_TOLERANCE = 0.02

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


def load_paper_shadow_backfill_config(
    path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
) -> dict[str, Any]:
    payload = _load_yaml_mapping(path)
    _assert_paper_shadow_backfill_config_safe(payload)
    return payload


def validate_paper_shadow_backfill_config(
    path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    payload: dict[str, Any] = {}
    try:
        payload = load_paper_shadow_backfill_config(path)
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("config_loads", False, str(exc)))
    else:
        backfill = _mapping(payload.get("backfill"))
        date_range = _mapping(payload.get("date_range"))
        source = _mapping(payload.get("source"))
        checks.extend(
            [
                _check("schema_version", payload.get("schema_version") == SCHEMA_VERSION, ""),
                _check(
                    "mode_backtest_simulation", backfill.get("mode") == "BACKTEST_SIMULATION", ""
                ),
                _check("not_pit_safe_visible", backfill.get("not_pit_safe") is True, ""),
                _check("research_target_only", backfill.get("research_target_only") is True, ""),
                _check("paper_shadow_only", backfill.get("paper_shadow_only") is True, ""),
                _check(
                    "date_start_ai_regime",
                    _coerce_date(date_range.get("start"), date(1970, 1, 1))
                    >= AI_AFTER_CHATGPT_START,
                    _text(date_range.get("start")),
                ),
                _check("target_methods_present", bool(_enabled_methods(payload)), ""),
                _check(
                    "required_source_configs_present",
                    bool(source.get("model_target_config") and source.get("paper_shadow_config")),
                    "",
                ),
                _check("safety_locked", _safety_config_locked(_mapping(payload.get("safety"))), ""),
            ]
        )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_backfill_config_validation",
        "paper_shadow_backfill_config",
        checks,
        extra={
            "config_path": str(path),
            "mode": _text(backfill.get("mode")),
            "not_pit_safe": backfill.get("not_pit_safe") is True,
        },
    )


def run_paper_shadow_backfill(
    *,
    config_path: Path = DEFAULT_PAPER_SHADOW_BACKFILL_CONFIG_PATH,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    price_cache_path: Path | None = None,
    rates_cache_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    config = load_paper_shadow_backfill_config(config_path)
    generated = generated_at or datetime.now(UTC)
    source = _mapping(config.get("source"))
    date_range = _mapping(config.get("date_range"))
    start = _coerce_date(date_range.get("start"), AI_AFTER_CHATGPT_START)
    enabled = _enabled_methods(config) or list(TARGET_METHODS)
    prices_path = price_cache_path or _resolve_project_path(
        source.get("price_cache_path"), DEFAULT_PRICE_CACHE_PATH
    )
    target_weights = _backfill_target_method_weights(config)
    target_weights = {
        method: target_weights[method] for method in enabled if method in target_weights
    }
    if not target_weights:
        raise DynamicV3SystemTargetError("backfill has no enabled target method weights")
    symbols = sorted(
        {symbol for weights in target_weights.values() for symbol in weights if symbol != "CASH"}
    )
    pivot = _load_price_pivot(prices_path, symbols, start)
    configured_end = _text(date_range.get("end"), "latest_available")
    end = (
        _coerce_date(configured_end, _latest_price_date(pivot))
        if configured_end
        else _latest_price_date(pivot)
    )
    if configured_end == "latest_available":
        end = _latest_price_date(pivot)
    pivot = pivot.loc[pivot.index.date <= end]
    if pivot.empty:
        raise DynamicV3SystemTargetError("backfill price window is empty")
    actual_start = pivot.index[0].date()
    actual_end = pivot.index[-1].date()
    quality = _run_data_quality_gate(
        price_cache_path=prices_path,
        rates_cache_path=rates_cache_path,
        expected_symbols=symbols,
        as_of=actual_end,
    )
    if not quality.passed:
        raise DynamicV3SystemTargetError(f"data quality gate failed: {quality.status}")
    returns = pivot.pct_change().fillna(0.0)
    trading_dates = [idx.date() for idx in returns.index]
    rebalance_dates = _backfill_rebalance_dates(
        trading_dates,
        frequency=_text(date_range.get("rebalance_frequency"), "weekly"),
        rebalance_day=_text(date_range.get("rebalance_day"), "MON"),
        min_history_days=int(_float(date_range.get("min_history_days_before_first_rebalance"), 0)),
    )
    initial_weights = _backfill_initial_weights(config)
    states: list[dict[str, Any]] = []
    ledger: list[dict[str, Any]] = []
    method_state = {
        method: {
            "weights": dict(initial_weights),
            "portfolio_value": 1.0,
            "peak_value": 1.0,
        }
        for method in target_weights
    }
    for timestamp, return_row in returns.iterrows():
        current_date = timestamp.date()
        is_calendar_rebalance = current_date in rebalance_dates
        for method, target in target_weights.items():
            state = method_state[method]
            before_return_weights = _normalize_weights(_mapping(state["weights"]))
            daily_return = _portfolio_return(before_return_weights, return_row)
            portfolio_value = _float(state["portfolio_value"]) * (1.0 + daily_return)
            drifted = _drift_weights(before_return_weights, return_row, daily_return)
            turnover = 0.0
            rebalance_event = False
            before_trade = dict(drifted)
            after_weights = dict(drifted)
            if is_calendar_rebalance and method != "no_trade_baseline":
                after_weights = _normalize_weights(target)
                turnover = _turnover(before_trade, after_weights)
                rebalance_event = True
                ledger.append(
                    {
                        "date": current_date.isoformat(),
                        "target_method": method,
                        "before_weights": before_trade,
                        "target_weights": after_weights,
                        "after_weights": after_weights,
                        "deltas": _weight_deltas(before_trade, after_weights),
                        "turnover": turnover,
                        "trade_type": "paper_rebalance",
                        "broker_action_taken": False,
                        "order_ticket_generated": False,
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            elif is_calendar_rebalance and method == "no_trade_baseline":
                ledger.append(
                    {
                        "date": current_date.isoformat(),
                        "target_method": method,
                        "before_weights": before_trade,
                        "target_weights": before_trade,
                        "after_weights": before_trade,
                        "deltas": {},
                        "turnover": 0.0,
                        "trade_type": "paper_no_trade",
                        "broker_action_taken": False,
                        "order_ticket_generated": False,
                        **SYSTEM_TARGET_SAFETY,
                    }
                )
            peak = max(_float(state["peak_value"]), portfolio_value)
            drawdown = portfolio_value / peak - 1.0 if peak > 0 else 0.0
            state["weights"] = after_weights
            state["portfolio_value"] = portfolio_value
            state["peak_value"] = peak
            states.append(
                {
                    "date": current_date.isoformat(),
                    "target_method": method,
                    "weights": after_weights,
                    "portfolio_value": round(portfolio_value, 10),
                    "daily_return": round(daily_return, 10),
                    "drawdown": round(drawdown, 10),
                    "turnover": round(turnover, 10),
                    "rebalance_event": rebalance_event,
                    "research_target_only": True,
                    "not_official_target_weights": True,
                    "broker_action_taken": False,
                    **SYSTEM_TARGET_SAFETY,
                }
            )
    backfill_id = _stable_id(
        "paper-shadow-backfill",
        config_path,
        actual_start.isoformat(),
        actual_end.isoformat(),
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / backfill_id)
    root.mkdir(parents=True, exist_ok=False)
    calendar = {
        "schema_version": SCHEMA_VERSION,
        "backfill_id": root.name,
        "rebalance_frequency": _text(date_range.get("rebalance_frequency"), "weekly"),
        "rebalance_day": _text(date_range.get("rebalance_day"), "MON"),
        "rebalance_dates": [item.isoformat() for item in sorted(rebalance_dates)],
        "rebalance_count": len(rebalance_dates),
        **SYSTEM_TARGET_SAFETY,
    }
    data_quality = _backfill_data_quality_payload(
        backfill_id=root.name,
        start=actual_start,
        end=actual_end,
        pivot=pivot,
        symbols=symbols,
        quality=quality,
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_backfill_manifest",
        "backfill_id": root.name,
        "generated_at": generated.isoformat(),
        "status": "PASS" if quality.passed else "FAIL",
        "market_regime": "ai_after_chatgpt",
        "requested_start_date": start.isoformat(),
        "requested_end_date": configured_end,
        "date_start": actual_start.isoformat(),
        "date_end": actual_end.isoformat(),
        "rebalance_count": len(rebalance_dates),
        "tracked_methods": list(target_weights),
        "data_quality_status": quality.status,
        "mode": "BACKTEST_SIMULATION",
        "not_pit_safe": True,
        "config_path": str(config_path),
        "paper_shadow_backfill_manifest_path": str(root / "paper_shadow_backfill_manifest.json"),
        "backfill_rebalance_calendar_path": str(root / "backfill_rebalance_calendar.json"),
        "backfill_method_states_path": str(root / "backfill_method_states.jsonl"),
        "backfill_trade_ledger_path": str(root / "backfill_trade_ledger.jsonl"),
        "backfill_data_quality_path": str(root / "backfill_data_quality.json"),
        "paper_shadow_backfill_report_path": str(root / "paper_shadow_backfill_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "paper_shadow_backfill_manifest.json", manifest)
    _write_json(root / "backfill_rebalance_calendar.json", calendar)
    _write_jsonl(root / "backfill_method_states.jsonl", states)
    _write_jsonl(root / "backfill_trade_ledger.jsonl", ledger)
    _write_json(root / "backfill_data_quality.json", data_quality)
    _write_text(
        root / "paper_shadow_backfill_report.md",
        render_paper_shadow_backfill_report(manifest, calendar, data_quality),
    )
    _write_latest_pointer(
        "latest_paper_shadow_backfill", root.name, root / "paper_shadow_backfill_manifest.json"
    )
    return {
        "backfill_id": root.name,
        "backfill_dir": root,
        "manifest": manifest,
        "backfill_rebalance_calendar": calendar,
        "backfill_method_states": states,
        "backfill_trade_ledger": ledger,
        "backfill_data_quality": data_quality,
    }


def paper_shadow_backfill_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=backfill_id,
        latest_pointer="latest_paper_shadow_backfill",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_backfill_manifest.json",
    )
    return {
        **_read_json(root / "paper_shadow_backfill_manifest.json"),
        "backfill_rebalance_calendar": _read_json(root / "backfill_rebalance_calendar.json"),
        "backfill_method_states": _read_jsonl(root / "backfill_method_states.jsonl"),
        "backfill_trade_ledger": _read_jsonl(root / "backfill_trade_ledger.jsonl"),
        "backfill_data_quality": _read_json(root / "backfill_data_quality.json"),
        "backfill_dir": str(root),
    }


def validate_paper_shadow_backfill_artifact(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
) -> dict[str, Any]:
    root = output_dir / backfill_id
    manifest = _read_optional_json(root / "paper_shadow_backfill_manifest.json") or {}
    calendar = _read_optional_json(root / "backfill_rebalance_calendar.json") or {}
    data_quality = _read_optional_json(root / "backfill_data_quality.json") or {}
    states = _read_jsonl(root / "backfill_method_states.jsonl")
    ledger = _read_jsonl(root / "backfill_trade_ledger.jsonl")
    tracked = set(_texts(manifest.get("tracked_methods")))
    state_methods = {str(row.get("target_method")) for row in states}
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_backfill_manifest.json",
            "backfill_rebalance_calendar.json",
            "backfill_method_states.jsonl",
            "backfill_trade_ledger.jsonl",
            "backfill_data_quality.json",
            "paper_shadow_backfill_report.md",
        ),
    )
    checks.extend(
        [
            _check("backfill_id_matches", manifest.get("backfill_id") == backfill_id, ""),
            _check(
                "market_regime_visible", manifest.get("market_regime") == "ai_after_chatgpt", ""
            ),
            _check("not_pit_safe_visible", manifest.get("not_pit_safe") is True, ""),
            _check("state_history_present", bool(states), ""),
            _check(
                "all_methods_have_states", tracked.issubset(state_methods), ",".join(state_methods)
            ),
            _check("rebalance_calendar_present", bool(calendar.get("rebalance_dates")), ""),
            _check(
                "trade_ledger_broker_false",
                all(row.get("broker_action_taken") is not True for row in ledger),
                "",
            ),
            _check(
                "data_quality_visible",
                data_quality.get("data_quality") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(data_quality.get("data_quality")),
            ),
            _check(
                "broker_forbidden", _payload_safe(manifest, calendar, data_quality, *states), ""
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_backfill_validation", backfill_id, checks
    )


def run_paper_shadow_rolling_eval(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    states = _records(backfill.get("backfill_method_states"))
    config = _load_backfill_config_from_manifest(backfill)
    min_observations = _config_int(
        config, ("evaluation", "min_observations_per_window"), DEFAULT_MIN_EVAL_OBSERVATIONS
    )
    windows = _rolling_window_inventory(states, min_observations=min_observations)
    metrics: list[dict[str, Any]] = []
    for window in windows:
        metrics.extend(_rolling_metrics_for_window(states, window, min_observations))
    _rank_rolling_metrics(metrics)
    stability = _rolling_rank_stability(metrics)
    rolling_eval_id = _stable_id("paper-shadow-rolling-eval", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / rolling_eval_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_rolling_eval_manifest",
        "rolling_eval_id": root.name,
        "backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if metrics else "FAIL",
        "window_count": len(windows),
        "metric_row_count": len(metrics),
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "rolling_eval_manifest_path": str(root / "rolling_eval_manifest.json"),
        "rolling_window_inventory_path": str(root / "rolling_window_inventory.json"),
        "rolling_method_metrics_path": str(root / "rolling_method_metrics.jsonl"),
        "rolling_rank_stability_path": str(root / "rolling_rank_stability.json"),
        "paper_shadow_rolling_eval_report_path": str(root / "paper_shadow_rolling_eval_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    inventory = {
        "schema_version": SCHEMA_VERSION,
        "rolling_eval_id": root.name,
        "windows": windows,
        "window_count": len(windows),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "rolling_eval_manifest.json", manifest)
    _write_json(root / "rolling_window_inventory.json", inventory)
    _write_jsonl(root / "rolling_method_metrics.jsonl", metrics)
    _write_json(root / "rolling_rank_stability.json", stability)
    _write_text(
        root / "paper_shadow_rolling_eval_report.md",
        render_paper_shadow_rolling_eval_report(manifest, stability, metrics),
    )
    _write_latest_pointer(
        "latest_paper_shadow_rolling_eval", root.name, root / "rolling_eval_manifest.json"
    )
    return {
        "rolling_eval_id": root.name,
        "rolling_eval_dir": root,
        "manifest": manifest,
        "rolling_window_inventory": inventory,
        "rolling_method_metrics": metrics,
        "rolling_rank_stability": stability,
    }


def paper_shadow_rolling_eval_report_payload(
    *,
    rolling_eval_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=rolling_eval_id,
        latest_pointer="latest_paper_shadow_rolling_eval",
        latest=latest,
        output_dir=output_dir,
        required_name="rolling_eval_manifest.json",
    )
    return {
        **_read_json(root / "rolling_eval_manifest.json"),
        "rolling_window_inventory": _read_json(root / "rolling_window_inventory.json"),
        "rolling_method_metrics": _read_jsonl(root / "rolling_method_metrics.jsonl"),
        "rolling_rank_stability": _read_json(root / "rolling_rank_stability.json"),
        "rolling_eval_dir": str(root),
    }


def validate_paper_shadow_rolling_eval_artifact(
    *,
    rolling_eval_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
) -> dict[str, Any]:
    root = output_dir / rolling_eval_id
    manifest = _read_optional_json(root / "rolling_eval_manifest.json") or {}
    inventory = _read_optional_json(root / "rolling_window_inventory.json") or {}
    stability = _read_optional_json(root / "rolling_rank_stability.json") or {}
    metrics = _read_jsonl(root / "rolling_method_metrics.jsonl")
    window_types = {str(row.get("window_type")) for row in _records(inventory.get("windows"))}
    checks = _required_file_checks(
        root,
        (
            "rolling_eval_manifest.json",
            "rolling_window_inventory.json",
            "rolling_method_metrics.jsonl",
            "rolling_rank_stability.json",
            "paper_shadow_rolling_eval_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "rolling_eval_id_matches", manifest.get("rolling_eval_id") == rolling_eval_id, ""
            ),
            _check("metrics_present", bool(metrics), ""),
            _check(
                "required_window_types_present",
                {"full", "yearly", "rolling_3m", "rolling_6m", "rolling_12m"}.issubset(
                    window_types
                ),
                ",".join(sorted(window_types)),
            ),
            _check("rank_stability_present", bool(_records(stability.get("methods"))), ""),
            _check("broker_forbidden", _payload_safe(manifest, inventory, stability, *metrics), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_rolling_eval_validation", rolling_eval_id, checks
    )


def run_paper_shadow_regime_review(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    states = _records(backfill.get("backfill_method_states"))
    config = _load_backfill_config_from_manifest(backfill)
    min_sample = _config_int(config, ("regime_policy", "min_sample_count"), 5)
    labels = _regime_labels_from_states(states, config)
    metrics = _regime_method_metrics(states, labels, min_sample)
    summary = _regime_method_summary(metrics)
    regime_review_id = _stable_id("paper-shadow-regime-review", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / regime_review_id)
    root.mkdir(parents=True, exist_ok=False)
    inventory = {
        "schema_version": SCHEMA_VERSION,
        "regime_review_id": root.name,
        "regimes": [
            {"regime": regime, "sample_count": sum(1 for item in labels.values() if item == regime)}
            for regime in _configured_regimes()
        ],
        **SYSTEM_TARGET_SAFETY,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_regime_review_manifest",
        "regime_review_id": root.name,
        "backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if metrics else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "paper_shadow_regime_manifest_path": str(root / "paper_shadow_regime_manifest.json"),
        "regime_window_inventory_path": str(root / "regime_window_inventory.json"),
        "method_regime_metrics_path": str(root / "method_regime_metrics.jsonl"),
        "regime_method_summary_path": str(root / "regime_method_summary.json"),
        "paper_shadow_regime_review_report_path": str(
            root / "paper_shadow_regime_review_report.md"
        ),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "paper_shadow_regime_manifest.json", manifest)
    _write_json(root / "regime_window_inventory.json", inventory)
    _write_jsonl(root / "method_regime_metrics.jsonl", metrics)
    _write_json(root / "regime_method_summary.json", summary)
    _write_text(
        root / "paper_shadow_regime_review_report.md",
        render_paper_shadow_regime_review_report(manifest, summary),
    )
    _write_latest_pointer(
        "latest_paper_shadow_regime_review",
        root.name,
        root / "paper_shadow_regime_manifest.json",
    )
    return {
        "regime_review_id": root.name,
        "regime_review_dir": root,
        "manifest": manifest,
        "regime_window_inventory": inventory,
        "method_regime_metrics": metrics,
        "regime_method_summary": summary,
    }


def paper_shadow_regime_review_report_payload(
    *,
    regime_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=regime_review_id,
        latest_pointer="latest_paper_shadow_regime_review",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_regime_manifest.json",
    )
    return {
        **_read_json(root / "paper_shadow_regime_manifest.json"),
        "regime_window_inventory": _read_json(root / "regime_window_inventory.json"),
        "method_regime_metrics": _read_jsonl(root / "method_regime_metrics.jsonl"),
        "regime_method_summary": _read_json(root / "regime_method_summary.json"),
        "regime_review_dir": str(root),
    }


def validate_paper_shadow_regime_review_artifact(
    *,
    regime_review_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / regime_review_id
    manifest = _read_optional_json(root / "paper_shadow_regime_manifest.json") or {}
    summary = _read_optional_json(root / "regime_method_summary.json") or {}
    metrics = _read_jsonl(root / "method_regime_metrics.jsonl")
    regimes = {str(row.get("regime")) for row in metrics}
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_regime_manifest.json",
            "regime_window_inventory.json",
            "method_regime_metrics.jsonl",
            "regime_method_summary.json",
            "paper_shadow_regime_review_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "regime_review_id_matches",
                manifest.get("regime_review_id") == regime_review_id,
                "",
            ),
            _check("metrics_present", bool(metrics), ""),
            _check("configured_regimes_present", set(_configured_regimes()).issubset(regimes), ""),
            _check("summary_present", bool(_records(summary.get("regimes"))), ""),
            _check("broker_forbidden", _payload_safe(manifest, summary, *metrics), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_regime_review_validation", regime_review_id, checks
    )


def run_paper_shadow_stability(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    states = _records(backfill.get("backfill_method_states"))
    config = _load_backfill_config_from_manifest(backfill)
    metrics, jumps, turnover = _stability_diagnostics(states, config)
    stability_id = _stable_id("paper-shadow-stability", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / stability_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_shadow_stability_manifest",
        "stability_id": root.name,
        "backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if metrics else "FAIL",
        "paper_shadow_stability_manifest_path": str(root / "paper_shadow_stability_manifest.json"),
        "method_stability_metrics_path": str(root / "method_stability_metrics.jsonl"),
        "weight_path_jump_events_path": str(root / "weight_path_jump_events.jsonl"),
        "turnover_diagnostics_path": str(root / "turnover_diagnostics.json"),
        "paper_shadow_stability_report_path": str(root / "paper_shadow_stability_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "paper_shadow_stability_manifest.json", manifest)
    _write_jsonl(root / "method_stability_metrics.jsonl", metrics)
    _write_jsonl(root / "weight_path_jump_events.jsonl", jumps)
    _write_json(root / "turnover_diagnostics.json", turnover)
    _write_text(
        root / "paper_shadow_stability_report.md",
        render_paper_shadow_stability_report(manifest, metrics, turnover),
    )
    _write_latest_pointer(
        "latest_paper_shadow_stability", root.name, root / "paper_shadow_stability_manifest.json"
    )
    return {
        "stability_id": root.name,
        "stability_dir": root,
        "manifest": manifest,
        "method_stability_metrics": metrics,
        "weight_path_jump_events": jumps,
        "turnover_diagnostics": turnover,
    }


def paper_shadow_stability_report_payload(
    *,
    stability_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=stability_id,
        latest_pointer="latest_paper_shadow_stability",
        latest=latest,
        output_dir=output_dir,
        required_name="paper_shadow_stability_manifest.json",
    )
    return {
        **_read_json(root / "paper_shadow_stability_manifest.json"),
        "method_stability_metrics": _read_jsonl(root / "method_stability_metrics.jsonl"),
        "weight_path_jump_events": _read_jsonl(root / "weight_path_jump_events.jsonl"),
        "turnover_diagnostics": _read_json(root / "turnover_diagnostics.json"),
        "stability_dir": str(root),
    }


def validate_paper_shadow_stability_artifact(
    *,
    stability_id: str,
    output_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
) -> dict[str, Any]:
    root = output_dir / stability_id
    manifest = _read_optional_json(root / "paper_shadow_stability_manifest.json") or {}
    turnover = _read_optional_json(root / "turnover_diagnostics.json") or {}
    metrics = _read_jsonl(root / "method_stability_metrics.jsonl")
    jumps = _read_jsonl(root / "weight_path_jump_events.jsonl")
    checks = _required_file_checks(
        root,
        (
            "paper_shadow_stability_manifest.json",
            "method_stability_metrics.jsonl",
            "weight_path_jump_events.jsonl",
            "turnover_diagnostics.json",
            "paper_shadow_stability_report.md",
        ),
    )
    checks.extend(
        [
            _check("stability_id_matches", manifest.get("stability_id") == stability_id, ""),
            _check("metrics_present", bool(metrics), ""),
            _check("turnover_diagnostics_present", bool(_records(turnover.get("methods"))), ""),
            _check(
                "jump_events_broker_false",
                all(row.get("broker_action_taken") is not True for row in jumps),
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, turnover, *metrics, *jumps), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_paper_shadow_stability_validation", stability_id, checks
    )


def run_system_target_selection_review(
    *,
    backfill_id: str,
    rolling_eval_id: str,
    regime_review_id: str,
    stability_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    rolling_eval_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    regime_review_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    stability_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id, output_dir=backfill_dir
    )
    rolling = paper_shadow_rolling_eval_report_payload(
        rolling_eval_id=rolling_eval_id, output_dir=rolling_eval_dir
    )
    regime = paper_shadow_regime_review_report_payload(
        regime_review_id=regime_review_id, output_dir=regime_review_dir
    )
    stability = paper_shadow_stability_report_payload(
        stability_id=stability_id, output_dir=stability_dir
    )
    config = _load_backfill_config_from_manifest(backfill)
    scorecard = _selection_scorecard(rolling, regime, stability, config)
    decision = _selection_decision(scorecard, config)
    selection_review_id = _stable_id(
        "system-target-selection-review",
        backfill_id,
        rolling_eval_id,
        regime_review_id,
        stability_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / selection_review_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["selection_review_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_system_target_selection_manifest",
        "selection_review_id": root.name,
        "backfill_id": backfill_id,
        "rolling_eval_id": rolling_eval_id,
        "regime_review_id": regime_review_id,
        "stability_id": stability_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "system_target_selection_manifest_path": str(
            root / "system_target_selection_manifest.json"
        ),
        "target_method_scorecard_path": str(root / "target_method_scorecard.json"),
        "selection_decision_path": str(root / "selection_decision.json"),
        "owner_research_checklist_path": str(root / "owner_research_checklist.md"),
        "system_target_selection_review_report_path": str(
            root / "system_target_selection_review_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "system_target_selection_manifest.json", manifest)
    _write_json(root / "target_method_scorecard.json", scorecard)
    _write_json(root / "selection_decision.json", decision)
    _write_text(root / "owner_research_checklist.md", render_selection_owner_checklist(decision))
    _write_text(
        root / "system_target_selection_review_report.md",
        render_system_target_selection_review_report(manifest, scorecard, decision),
    )
    _write_text(root / "reader_brief_section.md", render_selection_reader_brief(decision))
    _write_latest_pointer(
        "latest_system_target_selection_review",
        root.name,
        root / "system_target_selection_manifest.json",
    )
    return {
        "selection_review_id": root.name,
        "selection_review_dir": root,
        "manifest": manifest,
        "target_method_scorecard": scorecard,
        "selection_decision": decision,
    }


def system_target_selection_review_report_payload(
    *,
    selection_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=selection_review_id,
        latest_pointer="latest_system_target_selection_review",
        latest=latest,
        output_dir=output_dir,
        required_name="system_target_selection_manifest.json",
    )
    return {
        **_read_json(root / "system_target_selection_manifest.json"),
        "target_method_scorecard": _read_json(root / "target_method_scorecard.json"),
        "selection_decision": _read_json(root / "selection_decision.json"),
        "selection_review_dir": str(root),
    }


def validate_system_target_selection_review_artifact(
    *,
    selection_review_id: str,
    output_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
) -> dict[str, Any]:
    root = output_dir / selection_review_id
    manifest = _read_optional_json(root / "system_target_selection_manifest.json") or {}
    scorecard = _read_optional_json(root / "target_method_scorecard.json") or {}
    decision = _read_optional_json(root / "selection_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "system_target_selection_manifest.json",
            "target_method_scorecard.json",
            "selection_decision.json",
            "owner_research_checklist.md",
            "system_target_selection_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "selection_review_id_matches",
                manifest.get("selection_review_id") == selection_review_id
                and decision.get("selection_review_id") == selection_review_id,
                "",
            ),
            _check("scorecard_present", bool(_records(scorecard.get("methods"))), ""),
            _check(
                "recommended_method_present",
                bool(decision.get("recommended_research_method")),
                "",
            ),
            _check(
                "decision_status_valid",
                decision.get("decision_status")
                in {"CONTINUE_OBSERVATION", "REVIEW_REQUIRED", "INSUFFICIENT_DATA"},
                "",
            ),
            _check(
                "not_official_target_weights",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            _check("broker_forbidden", _payload_safe(manifest, scorecard, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_system_target_selection_review_validation",
        selection_review_id,
        checks,
    )


def run_selection_attribution(
    *,
    selection_review_id: str,
    selection_review_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    selection = system_target_selection_review_report_payload(
        selection_review_id=selection_review_id,
        output_dir=selection_review_dir,
    )
    scorecard = _mapping(selection.get("target_method_scorecard"))
    decision = _mapping(selection.get("selection_decision"))
    rows = _selection_attribution_rows(scorecard, decision, selection)
    recommendation = _recommendation_reason_breakdown(rows, decision)
    review_required = _review_required_reason_breakdown(selection, rows, decision)
    attribution_id = _stable_id(
        "selection-attribution",
        selection_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / attribution_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_selection_attribution_manifest",
        "attribution_id": root.name,
        "selection_review_id": selection_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "market_regime": selection.get("market_regime", "ai_after_chatgpt"),
        "date_start": selection.get("date_start"),
        "date_end": selection.get("date_end"),
        "data_quality_status": selection.get("data_quality_status"),
        "selection_attribution_manifest_path": str(
            root / "selection_attribution_manifest.json"
        ),
        "method_score_attribution_path": str(root / "method_score_attribution.jsonl"),
        "recommendation_reason_breakdown_path": str(
            root / "recommendation_reason_breakdown.json"
        ),
        "review_required_reason_breakdown_path": str(
            root / "review_required_reason_breakdown.json"
        ),
        "selection_attribution_report_path": str(root / "selection_attribution_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "selection_attribution_manifest.json", manifest)
    _write_jsonl(root / "method_score_attribution.jsonl", rows)
    _write_json(root / "recommendation_reason_breakdown.json", recommendation)
    _write_json(root / "review_required_reason_breakdown.json", review_required)
    _write_text(
        root / "selection_attribution_report.md",
        render_selection_attribution_report(manifest, rows, recommendation, review_required),
    )
    _write_latest_pointer(
        "latest_selection_attribution", root.name, root / "selection_attribution_manifest.json"
    )
    return {
        "attribution_id": root.name,
        "attribution_dir": root,
        "manifest": manifest,
        "method_score_attribution": rows,
        "recommendation_reason_breakdown": recommendation,
        "review_required_reason_breakdown": review_required,
    }


def selection_attribution_report_payload(
    *,
    attribution_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=attribution_id,
        latest_pointer="latest_selection_attribution",
        latest=latest,
        output_dir=output_dir,
        required_name="selection_attribution_manifest.json",
    )
    return {
        **_read_json(root / "selection_attribution_manifest.json"),
        "method_score_attribution": _read_jsonl(root / "method_score_attribution.jsonl"),
        "recommendation_reason_breakdown": _read_json(
            root / "recommendation_reason_breakdown.json"
        ),
        "review_required_reason_breakdown": _read_json(
            root / "review_required_reason_breakdown.json"
        ),
        "attribution_dir": str(root),
    }


def validate_selection_attribution_artifact(
    *,
    attribution_id: str,
    output_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
) -> dict[str, Any]:
    root = output_dir / attribution_id
    manifest = _read_optional_json(root / "selection_attribution_manifest.json") or {}
    rows = _read_jsonl(root / "method_score_attribution.jsonl")
    recommendation = _read_optional_json(root / "recommendation_reason_breakdown.json") or {}
    review_required = _read_optional_json(root / "review_required_reason_breakdown.json") or {}
    checks = _required_file_checks(
        root,
        (
            "selection_attribution_manifest.json",
            "method_score_attribution.jsonl",
            "recommendation_reason_breakdown.json",
            "review_required_reason_breakdown.json",
            "selection_attribution_report.md",
        ),
    )
    checks.extend(
        [
            _check("attribution_id_matches", manifest.get("attribution_id") == attribution_id, ""),
            _check("method_rows_present", bool(rows), ""),
            _check(
                "recommended_method_visible",
                bool(recommendation.get("recommended_research_method")),
                "",
            ),
            _check(
                "review_required_reasons_visible",
                bool(_records(review_required.get("review_required_reasons"))),
                "",
            ),
            _check(
                "can_trigger_production_false",
                review_required.get("can_trigger_production") is False,
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, recommendation, review_required, *rows),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_selection_attribution_validation", attribution_id, checks
    )


def run_limited_long_risk_review(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )
    states = _records(backfill.get("backfill_method_states"))
    long_window = _limited_long_window_risk_return(backfill, states)
    baseline_breakdown = _limited_vs_baseline_breakdown(states)
    exposure = _limited_exposure_path_analysis(states)
    risk_review_id = _stable_id("limited-long-risk", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / risk_review_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_long_risk_manifest",
        "risk_review_id": root.name,
        "backfill_id": backfill_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if long_window.get("metrics") else "FAIL",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "limited_long_risk_manifest_path": str(root / "limited_long_risk_manifest.json"),
        "long_window_risk_return_path": str(root / "long_window_risk_return.json"),
        "limited_vs_baseline_breakdown_path": str(root / "limited_vs_baseline_breakdown.json"),
        "exposure_path_analysis_path": str(root / "exposure_path_analysis.json"),
        "limited_long_risk_report_path": str(root / "limited_long_risk_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "limited_long_risk_manifest.json", manifest)
    _write_json(root / "long_window_risk_return.json", long_window)
    _write_json(root / "limited_vs_baseline_breakdown.json", baseline_breakdown)
    _write_json(root / "exposure_path_analysis.json", exposure)
    _write_text(
        root / "limited_long_risk_report.md",
        render_limited_long_risk_report(manifest, long_window, baseline_breakdown, exposure),
    )
    _write_latest_pointer(
        "latest_limited_long_risk", root.name, root / "limited_long_risk_manifest.json"
    )
    return {
        "risk_review_id": root.name,
        "risk_review_dir": root,
        "manifest": manifest,
        "long_window_risk_return": long_window,
        "limited_vs_baseline_breakdown": baseline_breakdown,
        "exposure_path_analysis": exposure,
    }


def limited_long_risk_report_payload(
    *,
    risk_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=risk_review_id,
        latest_pointer="latest_limited_long_risk",
        latest=latest,
        output_dir=output_dir,
        required_name="limited_long_risk_manifest.json",
    )
    return {
        **_read_json(root / "limited_long_risk_manifest.json"),
        "long_window_risk_return": _read_json(root / "long_window_risk_return.json"),
        "limited_vs_baseline_breakdown": _read_json(root / "limited_vs_baseline_breakdown.json"),
        "exposure_path_analysis": _read_json(root / "exposure_path_analysis.json"),
        "risk_review_dir": str(root),
    }


def validate_limited_long_risk_artifact(
    *,
    risk_review_id: str,
    output_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
) -> dict[str, Any]:
    root = output_dir / risk_review_id
    manifest = _read_optional_json(root / "limited_long_risk_manifest.json") or {}
    long_window = _read_optional_json(root / "long_window_risk_return.json") or {}
    baseline = _read_optional_json(root / "limited_vs_baseline_breakdown.json") or {}
    exposure = _read_optional_json(root / "exposure_path_analysis.json") or {}
    checks = _required_file_checks(
        root,
        (
            "limited_long_risk_manifest.json",
            "long_window_risk_return.json",
            "limited_vs_baseline_breakdown.json",
            "exposure_path_analysis.json",
            "limited_long_risk_report.md",
        ),
    )
    checks.extend(
        [
            _check("risk_review_id_matches", manifest.get("risk_review_id") == risk_review_id, ""),
            _check(
                "target_method_limited",
                long_window.get("target_method") == "limited_adjustment",
                "",
            ),
            _check(
                "risk_return_status_valid",
                long_window.get("risk_return_status")
                in {
                    "RETURN_IMPROVES_RISK_IMPROVES",
                    "RETURN_IMPROVES_RISK_WORSENS",
                    "RETURN_WORSE_RISK_IMPROVES",
                    "RETURN_WORSE_RISK_WORSE",
                    "INSUFFICIENT_DATA",
                },
                _text(long_window.get("risk_return_status")),
            ),
            _check("baseline_comparisons_present", bool(_records(baseline.get("comparisons"))), ""),
            _check(
                "exposure_summary_present",
                exposure.get("target_method") == "limited_adjustment",
                "",
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, long_window, baseline, exposure),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_limited_long_risk_validation", risk_review_id, checks
    )


def run_limited_consistency_check(
    *,
    backfill_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    rolling_eval_dir: Path = DEFAULT_PAPER_SHADOW_ROLLING_EVAL_DIR,
    regime_review_dir: Path = DEFAULT_PAPER_SHADOW_REGIME_REVIEW_DIR,
    stability_dir: Path = DEFAULT_PAPER_SHADOW_STABILITY_DIR,
    output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )
    rolling = _latest_or_run_rolling_for_backfill(
        backfill_id,
        backfill_dir=backfill_dir,
        rolling_eval_dir=rolling_eval_dir,
    )
    regime = _latest_or_run_regime_for_backfill(
        backfill_id,
        backfill_dir=backfill_dir,
        regime_review_dir=regime_review_dir,
    )
    stability = _latest_or_run_stability_for_backfill(
        backfill_id,
        backfill_dir=backfill_dir,
        stability_dir=stability_dir,
    )
    rolling_summary = _limited_rolling_consistency_summary(rolling)
    regime_summary = _limited_regime_consistency_summary(regime)
    stability_summary = _limited_stability_consistency_summary(stability)
    consistency_id = _stable_id("limited-consistency", backfill_id, generated.isoformat())
    root = _unique_dir(output_dir / consistency_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_consistency_manifest",
        "consistency_id": root.name,
        "backfill_id": backfill_id,
        "rolling_eval_id": rolling.get("rolling_eval_id"),
        "regime_review_id": regime.get("regime_review_id"),
        "stability_id": stability.get("stability_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "limited_consistency_manifest_path": str(root / "limited_consistency_manifest.json"),
        "rolling_consistency_summary_path": str(root / "rolling_consistency_summary.json"),
        "regime_consistency_summary_path": str(root / "regime_consistency_summary.json"),
        "stability_consistency_summary_path": str(root / "stability_consistency_summary.json"),
        "limited_consistency_report_path": str(root / "limited_consistency_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "limited_consistency_manifest.json", manifest)
    _write_json(root / "rolling_consistency_summary.json", rolling_summary)
    _write_json(root / "regime_consistency_summary.json", regime_summary)
    _write_json(root / "stability_consistency_summary.json", stability_summary)
    _write_text(
        root / "limited_consistency_report.md",
        render_limited_consistency_report(
            manifest,
            rolling_summary,
            regime_summary,
            stability_summary,
        ),
    )
    _write_latest_pointer(
        "latest_limited_consistency", root.name, root / "limited_consistency_manifest.json"
    )
    return {
        "consistency_id": root.name,
        "consistency_dir": root,
        "manifest": manifest,
        "rolling_consistency_summary": rolling_summary,
        "regime_consistency_summary": regime_summary,
        "stability_consistency_summary": stability_summary,
    }


def limited_consistency_report_payload(
    *,
    consistency_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=consistency_id,
        latest_pointer="latest_limited_consistency",
        latest=latest,
        output_dir=output_dir,
        required_name="limited_consistency_manifest.json",
    )
    return {
        **_read_json(root / "limited_consistency_manifest.json"),
        "rolling_consistency_summary": _read_json(root / "rolling_consistency_summary.json"),
        "regime_consistency_summary": _read_json(root / "regime_consistency_summary.json"),
        "stability_consistency_summary": _read_json(root / "stability_consistency_summary.json"),
        "consistency_dir": str(root),
    }


def validate_limited_consistency_artifact(
    *,
    consistency_id: str,
    output_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
) -> dict[str, Any]:
    root = output_dir / consistency_id
    manifest = _read_optional_json(root / "limited_consistency_manifest.json") or {}
    rolling = _read_optional_json(root / "rolling_consistency_summary.json") or {}
    regime = _read_optional_json(root / "regime_consistency_summary.json") or {}
    stability = _read_optional_json(root / "stability_consistency_summary.json") or {}
    checks = _required_file_checks(
        root,
        (
            "limited_consistency_manifest.json",
            "rolling_consistency_summary.json",
            "regime_consistency_summary.json",
            "stability_consistency_summary.json",
            "limited_consistency_report.md",
        ),
    )
    checks.extend(
        [
            _check("consistency_id_matches", manifest.get("consistency_id") == consistency_id, ""),
            _check(
                "rolling_status_valid",
                rolling.get("rolling_consistency_status")
                in {"STABLE", "MIXED", "UNSTABLE", "INSUFFICIENT_DATA"},
                _text(rolling.get("rolling_consistency_status")),
            ),
            _check(
                "regime_status_valid",
                regime.get("regime_consistency_status")
                in {
                    "BROADLY_CONSISTENT",
                    "REGIME_DEPENDENT",
                    "WEAK_IN_PRESSURE",
                    "INSUFFICIENT_DATA",
                },
                _text(regime.get("regime_consistency_status")),
            ),
            _check(
                "stability_status_valid",
                stability.get("stability_status")
                in {"STABLE", "MODERATE", "UNSTABLE", "INSUFFICIENT_DATA"},
                _text(stability.get("stability_status")),
            ),
            _check("broker_forbidden", _payload_safe(manifest, rolling, regime, stability), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_limited_consistency_validation", consistency_id, checks
    )


def run_data_warning_impact_review(
    *,
    backfill_id: str,
    selection_review_id: str,
    backfill_dir: Path = DEFAULT_PAPER_SHADOW_BACKFILL_DIR,
    selection_review_dir: Path = DEFAULT_SYSTEM_TARGET_SELECTION_REVIEW_DIR,
    output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    backfill = paper_shadow_backfill_report_payload(
        backfill_id=backfill_id,
        output_dir=backfill_dir,
    )
    selection = system_target_selection_review_report_payload(
        selection_review_id=selection_review_id,
        output_dir=selection_review_dir,
    )
    inventory = _data_warning_inventory(backfill)
    affected_metrics = _affected_metrics_from_warnings(inventory)
    sensitivity = _recommendation_sensitivity_to_warnings(selection, inventory, affected_metrics)
    impact_id = _stable_id(
        "data-warning-impact",
        backfill_id,
        selection_review_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / impact_id)
    root.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_data_warning_impact_manifest",
        "impact_id": root.name,
        "backfill_id": backfill_id,
        "selection_review_id": selection_review_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": backfill.get("market_regime", "ai_after_chatgpt"),
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "data_quality_status": backfill.get("data_quality_status"),
        "data_warning_impact_manifest_path": str(root / "data_warning_impact_manifest.json"),
        "data_warning_inventory_path": str(root / "data_warning_inventory.json"),
        "affected_metrics_path": str(root / "affected_metrics.json"),
        "recommendation_sensitivity_to_warnings_path": str(
            root / "recommendation_sensitivity_to_warnings.json"
        ),
        "data_warning_impact_report_path": str(root / "data_warning_impact_report.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "data_warning_impact_manifest.json", manifest)
    _write_json(root / "data_warning_inventory.json", inventory)
    _write_json(root / "affected_metrics.json", affected_metrics)
    _write_json(root / "recommendation_sensitivity_to_warnings.json", sensitivity)
    _write_text(
        root / "data_warning_impact_report.md",
        render_data_warning_impact_report(manifest, inventory, affected_metrics, sensitivity),
    )
    _write_latest_pointer(
        "latest_data_warning_impact", root.name, root / "data_warning_impact_manifest.json"
    )
    return {
        "impact_id": root.name,
        "impact_dir": root,
        "manifest": manifest,
        "data_warning_inventory": inventory,
        "affected_metrics": affected_metrics,
        "recommendation_sensitivity_to_warnings": sensitivity,
    }


def data_warning_impact_report_payload(
    *,
    impact_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=impact_id,
        latest_pointer="latest_data_warning_impact",
        latest=latest,
        output_dir=output_dir,
        required_name="data_warning_impact_manifest.json",
    )
    return {
        **_read_json(root / "data_warning_impact_manifest.json"),
        "data_warning_inventory": _read_json(root / "data_warning_inventory.json"),
        "affected_metrics": _read_json(root / "affected_metrics.json"),
        "recommendation_sensitivity_to_warnings": _read_json(
            root / "recommendation_sensitivity_to_warnings.json"
        ),
        "impact_dir": str(root),
    }


def validate_data_warning_impact_artifact(
    *,
    impact_id: str,
    output_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
) -> dict[str, Any]:
    root = output_dir / impact_id
    manifest = _read_optional_json(root / "data_warning_impact_manifest.json") or {}
    inventory = _read_optional_json(root / "data_warning_inventory.json") or {}
    affected = _read_optional_json(root / "affected_metrics.json") or {}
    sensitivity = _read_optional_json(root / "recommendation_sensitivity_to_warnings.json") or {}
    checks = _required_file_checks(
        root,
        (
            "data_warning_impact_manifest.json",
            "data_warning_inventory.json",
            "affected_metrics.json",
            "recommendation_sensitivity_to_warnings.json",
            "data_warning_impact_report.md",
        ),
    )
    checks.extend(
        [
            _check("impact_id_matches", manifest.get("impact_id") == impact_id, ""),
            _check(
                "data_quality_visible",
                inventory.get("data_quality") in {"PASS", "PASS_WITH_WARNINGS", "FAIL"},
                _text(inventory.get("data_quality")),
            ),
            _check("affected_metrics_present", bool(_records(affected.get("metrics"))), ""),
            _check(
                "recommendation_stability_valid",
                sensitivity.get("recommendation_stability")
                in {"STABLE", "REVIEW_REQUIRED", "UNSTABLE", "UNKNOWN"},
                _text(sensitivity.get("recommendation_stability")),
            ),
            _check(
                "data_quality_decision_valid",
                sensitivity.get("data_quality_decision")
                in {"ACCEPT_FOR_RESEARCH", "REVIEW_REQUIRED", "BLOCKED"},
                _text(sensitivity.get("data_quality_decision")),
            ),
            _check(
                "broker_forbidden",
                _payload_safe(manifest, inventory, affected, sensitivity),
                "",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_data_warning_impact_validation", impact_id, checks
    )


def run_research_method_hardening_pack(
    *,
    selection_attribution_id: str,
    risk_review_id: str,
    consistency_id: str,
    data_warning_impact_id: str,
    selection_attribution_dir: Path = DEFAULT_SELECTION_ATTRIBUTION_DIR,
    risk_review_dir: Path = DEFAULT_LIMITED_LONG_RISK_DIR,
    consistency_dir: Path = DEFAULT_LIMITED_CONSISTENCY_DIR,
    data_warning_impact_dir: Path = DEFAULT_DATA_WARNING_IMPACT_DIR,
    output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    attribution = selection_attribution_report_payload(
        attribution_id=selection_attribution_id,
        output_dir=selection_attribution_dir,
    )
    risk = limited_long_risk_report_payload(
        risk_review_id=risk_review_id,
        output_dir=risk_review_dir,
    )
    consistency = limited_consistency_report_payload(
        consistency_id=consistency_id,
        output_dir=consistency_dir,
    )
    data_warning = data_warning_impact_report_payload(
        impact_id=data_warning_impact_id,
        output_dir=data_warning_impact_dir,
    )
    decision = _research_method_hardening_decision(attribution, risk, consistency, data_warning)
    hardening_id = _stable_id(
        "research-method-hardening",
        selection_attribution_id,
        risk_review_id,
        consistency_id,
        data_warning_impact_id,
        generated.isoformat(),
    )
    root = _unique_dir(output_dir / hardening_id)
    root.mkdir(parents=True, exist_ok=False)
    decision["hardening_id"] = root.name
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_research_method_hardening_manifest",
        "hardening_id": root.name,
        "selection_attribution_id": selection_attribution_id,
        "risk_review_id": risk_review_id,
        "consistency_id": consistency_id,
        "data_warning_impact_id": data_warning_impact_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "candidate_method": decision.get("candidate_method"),
        "hardening_decision": decision.get("hardening_decision"),
        "decision_confidence": decision.get("decision_confidence"),
        "research_method_hardening_manifest_path": str(
            root / "research_method_hardening_manifest.json"
        ),
        "hardening_decision_path": str(root / "hardening_decision.json"),
        "owner_research_method_checklist_path": str(
            root / "owner_research_method_checklist.md"
        ),
        "research_method_hardening_report_path": str(
            root / "research_method_hardening_report.md"
        ),
        "reader_brief_section_path": str(root / "reader_brief_section.md"),
        **SYSTEM_TARGET_SAFETY,
    }
    _write_json(root / "research_method_hardening_manifest.json", manifest)
    _write_json(root / "hardening_decision.json", decision)
    _write_text(
        root / "owner_research_method_checklist.md",
        render_hardening_owner_checklist(decision),
    )
    _write_text(
        root / "research_method_hardening_report.md",
        render_research_method_hardening_report(
            manifest,
            decision,
            attribution,
            risk,
            consistency,
            data_warning,
        ),
    )
    _write_text(root / "reader_brief_section.md", render_hardening_reader_brief(decision))
    _write_latest_pointer(
        "latest_research_method_hardening",
        root.name,
        root / "research_method_hardening_manifest.json",
    )
    return {
        "hardening_id": root.name,
        "hardening_dir": root,
        "manifest": manifest,
        "hardening_decision": decision,
    }


def research_method_hardening_report_payload(
    *,
    hardening_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> dict[str, Any]:
    root = _artifact_dir(
        artifact_id=hardening_id,
        latest_pointer="latest_research_method_hardening",
        latest=latest,
        output_dir=output_dir,
        required_name="research_method_hardening_manifest.json",
    )
    return {
        **_read_json(root / "research_method_hardening_manifest.json"),
        "hardening_decision_payload": _read_json(root / "hardening_decision.json"),
        "hardening_dir": str(root),
    }


def validate_research_method_hardening_artifact(
    *,
    hardening_id: str,
    output_dir: Path = DEFAULT_RESEARCH_METHOD_HARDENING_DIR,
) -> dict[str, Any]:
    root = output_dir / hardening_id
    manifest = _read_optional_json(root / "research_method_hardening_manifest.json") or {}
    decision = _read_optional_json(root / "hardening_decision.json") or {}
    checks = _required_file_checks(
        root,
        (
            "research_method_hardening_manifest.json",
            "hardening_decision.json",
            "owner_research_method_checklist.md",
            "research_method_hardening_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "hardening_id_matches",
                manifest.get("hardening_id") == hardening_id
                and decision.get("hardening_id") == hardening_id,
                "",
            ),
            _check(
                "candidate_method_limited",
                decision.get("candidate_method") == "limited_adjustment",
                _text(decision.get("candidate_method")),
            ),
            _check(
                "hardening_decision_valid",
                decision.get("hardening_decision")
                in {
                    "HARDEN_AS_PRIMARY_RESEARCH",
                    "CONTINUE_OBSERVATION",
                    "REVIEW_REQUIRED",
                    "REJECT",
                },
                _text(decision.get("hardening_decision")),
            ),
            _check(
                "not_official_target_weights",
                decision.get("not_official_target_weights") is True,
                "",
            ),
            _check(
                "broker_action_allowed_false",
                decision.get("broker_action_allowed") is False,
                "",
            ),
            _check("production_effect_none", decision.get("production_effect") == "none", ""),
            _check("broker_forbidden", _payload_safe(manifest, decision), ""),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_research_method_hardening_validation", hardening_id, checks
    )


def render_paper_shadow_backfill_report(
    manifest: Mapping[str, Any],
    calendar: Mapping[str, Any],
    data_quality: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Paper Shadow Historical Backfill {manifest.get('backfill_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- requested_date_range: {manifest.get('requested_start_date')} to "
            f"{manifest.get('requested_end_date')}",
            f"- actual_date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- rebalance_events: {calendar.get('rebalance_count')}",
            f"- tracked_methods: {', '.join(_texts(manifest.get('tracked_methods')))}",
            f"- data_quality: {data_quality.get('data_quality')}",
            f"- missing_symbols: {', '.join(_texts(data_quality.get('missing_symbols')))}",
            "- mode: BACKTEST_SIMULATION",
            "- not_pit_safe: true",
            "- research_target_only: true",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- broker_action_taken: false",
            "- production_effect: none",
            "",
            "该报告是 paper shadow research backfill，不是 PIT-safe production backtest，"
            "不能批准 official target weights 或 broker action。",
            "",
        ]
    )


def render_paper_shadow_rolling_eval_report(
    manifest: Mapping[str, Any],
    stability: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
) -> str:
    rows = _records(stability.get("methods"))
    best_average = _best_rank_stability_method(rows)
    stable = [row for row in rows if row.get("rank_stability_status") == "STABLE"]
    limited = _find_method(rows, "limited_adjustment")
    defensive = _find_method(rows, "defensive_limited_adjustment")
    consensus = _find_method(rows, "consensus_target")
    return "\n".join(
        [
            f"# Paper Shadow Rolling Evaluation {manifest.get('rolling_eval_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- window_count: {manifest.get('window_count')}",
            f"- metric_row_count: {len(metrics)}",
            f"- best_average_rank_method: {best_average}",
            f"- stable_methods: {', '.join(_texts([row.get('target_method') for row in stable]))}",
            f"- limited_adjustment_stability: {limited.get('rank_stability_status', 'MISSING')}",
            f"- defensive_limited_adjustment_stability: "
            f"{defensive.get('rank_stability_status', 'MISSING')}",
            f"- consensus_target_stability: {consensus.get('rank_stability_status', 'MISSING')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_regime_review_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    regimes = _records(summary.get("regimes"))
    by_name = {row.get("regime"): row for row in regimes}
    semiconductor_best = _mapping(by_name.get("semiconductor_pullback")).get(
        "best_return_method",
        "MISSING",
    )
    return "\n".join(
        [
            f"# Paper Shadow Regime Review {manifest.get('regime_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- ai_trend_best_return_method: "
            f"{_mapping(by_name.get('ai_trend')).get('best_return_method', 'MISSING')}",
            f"- tech_drawdown_best_return_method: "
            f"{_mapping(by_name.get('tech_drawdown')).get('best_return_method', 'MISSING')}",
            f"- semiconductor_pullback_best_return_method: {semiconductor_best}",
            f"- defensive_limited_adjustment_status: "
            f"{summary.get('defensive_limited_adjustment_status')}",
            "- no_auto_defensive_rule_approval: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_paper_shadow_stability_report(
    manifest: Mapping[str, Any],
    metrics: Sequence[Mapping[str, Any]],
    turnover: Mapping[str, Any],
) -> str:
    methods = list(metrics)
    most_stable = _best_status_method(methods, "stability_status")
    highest_turnover = _max_field_method(_records(turnover.get("methods")), "annualized_turnover")
    jump_count = sum(int(_float(row.get("large_jump_count"))) for row in methods)
    consensus = _find_method(methods, "consensus_target")
    selected = _find_method(methods, "selected_top_candidate")
    return "\n".join(
        [
            f"# Paper Shadow Stability Diagnostics {manifest.get('stability_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- most_stable_method: {most_stable}",
            f"- highest_turnover_method: {highest_turnover}",
            f"- large_jump_count: {jump_count}",
            f"- consensus_target_stability: {consensus.get('stability_status', 'MISSING')}",
            f"- selected_top_candidate_stability: {selected.get('stability_status', 'MISSING')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_selection_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Checklist {decision.get('selection_review_id')}",
            "",
            "- 是否继续将 limited_adjustment 作为主 research target？",
            "- 是否保留 defensive_limited_adjustment 作为 secondary research method？",
            "- 是否将 consensus_target 保持为 reference-only？",
            "- 是否需要减少 target methods 数量？",
            "- 是否继续运行 paper shadow account？",
            "- 是否仍然禁止 broker / production？",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- decision_status: {decision.get('decision_status')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_system_target_selection_review_report(
    manifest: Mapping[str, Any],
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    methods = _records(scorecard.get("methods"))
    best_return = _max_field_method(methods, "return_score")
    best_drawdown = _max_field_method(methods, "drawdown_score")
    best_stability = _max_field_method(methods, "stability_score")
    best_regime = _max_field_method(methods, "regime_score")
    secondary_methods = ", ".join(_texts(decision.get("secondary_research_methods")))
    reference_only_methods = ", ".join(_texts(decision.get("reference_only_methods")))
    return "\n".join(
        [
            f"# System Target Method Selection Review {manifest.get('selection_review_id')}",
            "",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status: {manifest.get('data_quality_status')}",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- secondary_methods: {secondary_methods}",
            f"- reference_only_methods: {reference_only_methods}",
            f"- decision_status: {decision.get('decision_status')}",
            f"- best_return_score_method: {best_return}",
            f"- best_drawdown_score_method: {best_drawdown}",
            f"- best_stability_score_method: {best_stability}",
            f"- best_regime_score_method: {best_regime}",
            "- official_target_weights_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            _text(decision.get("reason")),
            "",
        ]
    )


def render_selection_reader_brief(decision: Mapping[str, Any]) -> str:
    secondary_methods = ", ".join(_texts(decision.get("secondary_research_methods")))
    reference_only_methods = ", ".join(_texts(decision.get("reference_only_methods")))
    return "\n".join(
        [
            "## Dynamic Rescue System Target Selection Review",
            "",
            f"- recommended_research_method: {decision.get('recommended_research_method')}",
            f"- secondary_methods: {secondary_methods}",
            f"- reference_only_methods: {reference_only_methods}",
            f"- decision_status: {decision.get('decision_status')}",
            "- research_target_only: true",
            f"- next_action: {decision.get('next_action')}",
            "",
        ]
    )


def render_selection_attribution_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    recommendation: Mapping[str, Any],
    review_required: Mapping[str, Any],
) -> str:
    recommended = _text(recommendation.get("recommended_research_method"))
    recommended_row = _find_method(rows, recommended)
    top_reasons = [
        _text(item.get("reason"))
        for item in _records(recommendation.get("primary_reasons"))
        if item.get("reason")
    ]
    blockers = [
        _text(item.get("reason"))
        for item in _records(review_required.get("review_required_reasons"))
        if item.get("blocking") is True
    ]
    return "\n".join(
        [
            f"# Selection Attribution {manifest.get('attribution_id')}",
            "",
            f"- selection_review_id: {manifest.get('selection_review_id')}",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {manifest.get('date_start')} to {manifest.get('date_end')}",
            f"- data_quality_status: {manifest.get('data_quality_status')}",
            f"- recommended_research_method: {recommended}",
            f"- recommended_overall_score: {recommended_row.get('overall_score', 'MISSING')}",
            f"- top_reasons: {', '.join(top_reasons)}",
            f"- decision_status: {review_required.get('decision_status')}",
            f"- blocking_reasons: {', '.join(blockers) if blockers else 'none'}",
            f"- can_harden_research_method: {review_required.get('can_harden_research_method')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "该 attribution 只解释既有 selection review，不重新选择 target method，"
            "也不允许 official target weights 或 broker action。",
            "",
            "## Method Components",
            "",
            *[
                "- "
                f"{row.get('rank')}. {row.get('target_method')}: "
                f"overall={row.get('overall_score')}, "
                f"return={_mapping(row.get('score_components')).get('return_score')}, "
                f"drawdown={_mapping(row.get('score_components')).get('drawdown_score')}, "
                "risk_adjusted="
                f"{_mapping(row.get('score_components')).get('risk_adjusted_score')}, "
                f"regime={_mapping(row.get('score_components')).get('regime_score')}, "
                f"stability={_mapping(row.get('score_components')).get('stability_score')}, "
                "turnover_penalty="
                f"{_mapping(row.get('score_components')).get('turnover_penalty')}, "
                f"data_quality_penalty="
                f"{_mapping(row.get('score_components')).get('data_quality_penalty')}"
                for row in rows
            ],
            "",
        ]
    )


def render_limited_long_risk_report(
    manifest: Mapping[str, Any],
    long_window: Mapping[str, Any],
    baseline_breakdown: Mapping[str, Any],
    exposure: Mapping[str, Any],
) -> str:
    metrics = _mapping(long_window.get("metrics"))
    comparisons = _records(baseline_breakdown.get("comparisons"))
    return "\n".join(
        [
            f"# Limited Adjustment Long-window Risk Review {manifest.get('risk_review_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- date_range: {long_window.get('date_start')} to {long_window.get('date_end')}",
            f"- total_return: {metrics.get('total_return')}",
            f"- annualized_return: {metrics.get('annualized_return')}",
            f"- max_drawdown: {metrics.get('max_drawdown')}",
            f"- realized_volatility: {metrics.get('realized_volatility')}",
            f"- turnover: {metrics.get('turnover')}",
            f"- risk_return_status: {long_window.get('risk_return_status')}",
            f"- confidence: {long_window.get('confidence')}",
            f"- risk_exposure_interpretation: "
            f"{exposure.get('risk_exposure_interpretation')}",
            f"- avg_risk_asset_weight: {exposure.get('avg_risk_asset_weight')}",
            f"- avg_semiconductor_weight: {exposure.get('avg_semiconductor_weight')}",
            f"- avg_cash_weight: {exposure.get('avg_cash_weight')}",
            "- official_target_weights_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Baseline Comparisons",
            "",
            *[
                "- "
                f"{row.get('baseline')}: return_delta={row.get('return_delta')}, "
                f"drawdown_delta={row.get('drawdown_delta')}, "
                f"volatility_delta={row.get('volatility_delta')}, "
                f"turnover_delta={row.get('turnover_delta')}, "
                f"conclusion={row.get('conclusion')}"
                for row in comparisons
            ],
            "",
            "收益改善如存在，仍需同时阅读回撤、波动、换手和 exposure path；"
            "该报告不能升级 official target weights。",
            "",
        ]
    )


def render_limited_consistency_report(
    manifest: Mapping[str, Any],
    rolling: Mapping[str, Any],
    regime: Mapping[str, Any],
    stability: Mapping[str, Any],
) -> str:
    pressure_failures = [
        _text(row.get("regime"))
        for row in _records(regime.get("regimes"))
        if row.get("status") == "FAIL"
        and row.get("regime") in {"tech_drawdown", "semiconductor_pullback", "risk_off"}
    ]
    pressure_summary = ", ".join(pressure_failures) if pressure_failures else "none"
    return "\n".join(
        [
            f"# Limited Adjustment Consistency Check {manifest.get('consistency_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- rolling_eval_id: {manifest.get('rolling_eval_id')}",
            f"- regime_review_id: {manifest.get('regime_review_id')}",
            f"- stability_id: {manifest.get('stability_id')}",
            f"- rolling_consistency_status: {rolling.get('rolling_consistency_status')}",
            f"- top_3_frequency_by_return: {rolling.get('top_3_frequency_by_return')}",
            f"- top_3_frequency_by_risk_adjusted: "
            f"{rolling.get('top_3_frequency_by_risk_adjusted')}",
            f"- bottom_3_frequency: {rolling.get('bottom_3_frequency')}",
            f"- regime_consistency_status: {regime.get('regime_consistency_status')}",
            f"- pressure_regime_failures: {pressure_summary}",
            f"- stability_status: {stability.get('stability_status')}",
            f"- turnover_status: {stability.get('turnover_status')}",
            f"- large_jump_count: {stability.get('large_jump_count')}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Consistency evidence 只用于 research hardening，不产生 policy auto-apply、"
            "official target weights 或 broker action。",
            "",
        ]
    )


def render_data_warning_impact_report(
    manifest: Mapping[str, Any],
    inventory: Mapping[str, Any],
    affected_metrics: Mapping[str, Any],
    sensitivity: Mapping[str, Any],
) -> str:
    warnings = _records(inventory.get("warnings"))
    metrics = _records(affected_metrics.get("metrics"))
    return "\n".join(
        [
            f"# Data Warning Impact Review {manifest.get('impact_id')}",
            "",
            f"- backfill_id: {manifest.get('backfill_id')}",
            f"- selection_review_id: {manifest.get('selection_review_id')}",
            f"- data_quality: {inventory.get('data_quality')}",
            f"- warning_ids: {', '.join(_texts([row.get('warning_id') for row in warnings]))}",
            f"- recommendation_stability: {sensitivity.get('recommendation_stability')}",
            f"- data_quality_decision: {sensitivity.get('data_quality_decision')}",
            f"- would_change_if_warnings_excluded: "
            f"{sensitivity.get('would_change_if_warnings_excluded')}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "## Affected Metrics",
            "",
            *[
                "- "
                f"{row.get('metric')}: affected={row.get('affected')}, "
                f"impact_level={row.get('impact_level')}, reason={row.get('reason')}"
                for row in metrics
            ],
            "",
            "如果 warning 明细缺失，本报告保持 UNKNOWN / REVIEW_REQUIRED，"
            "不把 warning 静默解释为无影响。",
            "",
        ]
    )


def render_hardening_owner_checklist(decision: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            f"# Owner Research Method Checklist {decision.get('hardening_id')}",
            "",
            "- 是否接受 limited_adjustment 作为 primary research method？",
            "- 是否接受它仍需 forward confirmation？",
            "- 是否接受它不是 official target weights？",
            "- 是否接受它不触发 broker / production？",
            "- 是否继续将 consensus_target 作为 reference-only？",
            "- 是否继续将 defensive_limited_adjustment 作为 secondary / research-only？",
            "- 是否需要重新跑 backfill 或修复 data warnings？",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- blocking_issues: {', '.join(_texts(decision.get('blocking_issues')))}",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


def render_research_method_hardening_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
    attribution: Mapping[str, Any],
    risk: Mapping[str, Any],
    consistency: Mapping[str, Any],
    data_warning: Mapping[str, Any],
) -> str:
    risk_return = _mapping(risk.get("long_window_risk_return"))
    risk_metrics = _mapping(risk_return.get("metrics"))
    rolling = _mapping(consistency.get("rolling_consistency_summary"))
    regime = _mapping(consistency.get("regime_consistency_summary"))
    stability = _mapping(consistency.get("stability_consistency_summary"))
    warning_sensitivity = _mapping(data_warning.get("recommendation_sensitivity_to_warnings"))
    return "\n".join(
        [
            f"# Research Method Hardening {manifest.get('hardening_id')}",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- current_status: {decision.get('current_status')}",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- reasons: {', '.join(_texts(decision.get('reasons')))}",
            f"- blocking_issues: {', '.join(_texts(decision.get('blocking_issues')))}",
            f"- warnings: {', '.join(_texts(decision.get('warnings')))}",
            f"- total_return: {risk_metrics.get('total_return')}",
            f"- max_drawdown: {risk_metrics.get('max_drawdown')}",
            f"- turnover: {risk_metrics.get('turnover')}",
            f"- risk_return_status: {risk_return.get('risk_return_status')}",
            f"- rolling_consistency_status: {rolling.get('rolling_consistency_status')}",
            f"- regime_consistency_status: {regime.get('regime_consistency_status')}",
            f"- stability_status: {stability.get('stability_status')}",
            f"- data_quality_decision: {warning_sensitivity.get('data_quality_decision')}",
            f"- requires_forward_confirmation: {decision.get('requires_forward_confirmation')}",
            "- official_target_weights_allowed: false",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
            "Hardening pack 只定义 research method 观察口径；不修改 "
            "`position_advisory_v1.yaml`、`model_target_portfolio_v1.yaml`、"
            "official target weights、portfolio、broker 或 production state。",
            "",
            f"- selection_attribution_id: {manifest.get('selection_attribution_id')}",
            f"- risk_review_id: {manifest.get('risk_review_id')}",
            f"- consistency_id: {manifest.get('consistency_id')}",
            f"- data_warning_impact_id: {manifest.get('data_warning_impact_id')}",
            "",
            "## Attribution Summary",
            "",
            f"- recommended_research_method: "
            f"{_mapping(attribution.get('recommendation_reason_breakdown')).get('recommended_research_method')}",
            f"- decision_status: "
            f"{_mapping(attribution.get('review_required_reason_breakdown')).get('decision_status')}",
            "",
        ]
    )


def render_hardening_reader_brief(decision: Mapping[str, Any]) -> str:
    next_action = (
        "owner_review_required"
        if decision.get("hardening_decision") == "REVIEW_REQUIRED"
        else "continue_paper_shadow_observation"
    )
    return "\n".join(
        [
            "## Dynamic Rescue Research Method Hardening",
            "",
            f"- candidate_method: {decision.get('candidate_method')}",
            f"- hardening_decision: {decision.get('hardening_decision')}",
            f"- decision_confidence: {decision.get('decision_confidence')}",
            f"- research_target_only: {str(decision.get('research_target_only') is True).lower()}",
            f"- requires_forward_confirmation: "
            f"{str(decision.get('requires_forward_confirmation') is True).lower()}",
            f"- next_action: {next_action}",
            "- not_official_target_weights: true",
            "- broker_action_allowed: false",
            "- production_effect: none",
            "",
        ]
    )


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


def _backfill_target_method_weights(config: Mapping[str, Any]) -> dict[str, dict[str, float]]:
    source = _mapping(config.get("source"))
    model_config_path = _resolve_project_path(
        source.get("model_target_config"), DEFAULT_MODEL_TARGET_CONFIG_PATH
    )
    model_config = load_model_target_config(model_config_path)
    baseline = _config_baseline_weights(model_config)
    source_payload = _latest_target_source(
        position_advisory_daily_dir=_resolve_project_path(
            source.get("position_advisory_daily_dir"), DEFAULT_POSITION_ADVISORY_DAILY_DIR
        ),
        shadow_monitor_dir=_resolve_project_path(
            source.get("shadow_monitor_dir"), DEFAULT_SHADOW_MONITOR_RUN_DIR
        ),
        shadow_shortlist_dir=_resolve_project_path(
            source.get("shadow_shortlist_dir"), DEFAULT_SHADOW_SHORTLIST_DIR
        ),
        consensus_drift_dir=_resolve_project_path(
            source.get("consensus_drift_dir"), DEFAULT_CONSENSUS_DRIFT_DIR
        ),
    )
    candidates = source_payload["candidate_targets"]
    consensus = _normalize_weights(
        source_payload.get("consensus_weights")
        or _average_candidate_weights(candidates)
        or baseline
    )
    top_candidate = _normalize_weights(_first_candidate_weights(candidates) or consensus)
    equal_weight = _normalize_weights(_average_candidate_weights(candidates) or consensus)
    advisory_limits = _load_advisory_limits(
        _mapping(model_config.get("source")).get("position_advisory_config")
    )
    if not advisory_limits:
        advisory_limits = _load_advisory_limits(DEFAULT_POSITION_ADVISORY_CONFIG_PATH)
    limited = _limited_adjustment(
        baseline=baseline,
        target=consensus,
        max_total_adjustment=_float(advisory_limits.get("max_single_day_total_adjustment")),
        max_symbol_adjustment=_float(advisory_limits.get("max_single_symbol_adjustment")),
    )
    defensive = _defensive_adjustment(
        limited,
        _mapping(_mapping(model_config.get("method_policy")).get("defensive_limited_adjustment")),
    )
    return {
        "static_baseline": baseline,
        "no_trade_baseline": baseline,
        "consensus_target": consensus,
        "limited_adjustment": limited,
        "defensive_limited_adjustment": defensive,
        "equal_weight_shadow_candidates": equal_weight,
        "selected_top_candidate": top_candidate,
    }


def _backfill_initial_weights(config: Mapping[str, Any]) -> dict[str, float]:
    source = _mapping(config.get("source"))
    paper_config_path = _resolve_project_path(
        source.get("paper_shadow_config"), DEFAULT_PAPER_SHADOW_CONFIG_PATH
    )
    paper_config = load_paper_shadow_config(paper_config_path)
    baseline = _mapping(paper_config.get("baseline")).get("static_weights")
    if isinstance(baseline, Mapping):
        return _normalize_weights(baseline)
    return _config_baseline_weights(
        load_model_target_config(
            _resolve_project_path(
                source.get("model_target_config"), DEFAULT_MODEL_TARGET_CONFIG_PATH
            )
        )
    )


def _load_price_pivot(path: Path, symbols: Sequence[str], start: date) -> pd.DataFrame:
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
    return pivot.dropna(how="all")


def _latest_price_date(pivot: pd.DataFrame) -> date:
    if pivot.empty:
        raise DynamicV3SystemTargetError("price cache has no rows for requested symbols")
    return pivot.index[-1].date()


def _backfill_rebalance_dates(
    trading_dates: Sequence[date],
    *,
    frequency: str,
    rebalance_day: str,
    min_history_days: int,
) -> set[date]:
    if frequency.lower() != "weekly":
        raise DynamicV3SystemTargetError("paper shadow backfill only supports weekly rebalance")
    if not trading_dates:
        return set()
    weekday_map = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4}
    target_weekday = weekday_map.get(rebalance_day.upper(), 0)
    first_allowed = min(trading_dates) + timedelta(days=min_history_days)
    selected: set[date] = set()
    by_week: dict[tuple[int, int], list[date]] = {}
    for item in trading_dates:
        if item < first_allowed:
            continue
        iso = item.isocalendar()
        by_week.setdefault((iso.year, iso.week), []).append(item)
    for dates in by_week.values():
        on_or_after = [item for item in dates if item.weekday() >= target_weekday]
        selected.add(min(on_or_after or dates))
    return selected


def _portfolio_return(weights: Mapping[str, float], return_row: Mapping[str, Any]) -> float:
    value = 0.0
    for symbol, weight in weights.items():
        if symbol == "CASH":
            continue
        value += _float(weight) * _float(return_row.get(symbol))
    return value


def _drift_weights(
    weights: Mapping[str, float],
    return_row: Mapping[str, Any],
    portfolio_return: float,
) -> dict[str, float]:
    denominator = 1.0 + portfolio_return
    if denominator <= 0:
        return _normalize_weights(weights)
    drifted = {}
    for symbol, weight in weights.items():
        asset_return = 0.0 if symbol == "CASH" else _float(return_row.get(symbol))
        drifted[symbol] = _float(weight) * (1.0 + asset_return) / denominator
    return _normalize_weights(drifted)


def _backfill_data_quality_payload(
    *,
    backfill_id: str,
    start: date,
    end: date,
    pivot: pd.DataFrame,
    symbols: Sequence[str],
    quality: DataQualityReport,
) -> dict[str, Any]:
    missing_symbols = [symbol for symbol in symbols if symbol not in pivot.columns]
    missing_dates = [
        idx.date().isoformat()
        for idx, row in pivot.iterrows()
        if any(pd.isna(row.get(symbol)) for symbol in symbols if symbol in pivot.columns)
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "backfill_id": backfill_id,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
        "price_source_status": quality.status,
        "missing_price_dates": missing_dates,
        "missing_symbols": missing_symbols,
        "data_quality": quality.status,
        "data_quality_checked_at": quality.checked_at.isoformat(),
        **SYSTEM_TARGET_SAFETY,
    }


def _rolling_window_inventory(
    states: Sequence[Mapping[str, Any]],
    *,
    min_observations: int,
) -> list[dict[str, Any]]:
    dates = sorted({_coerce_date(row.get("date"), date(1970, 1, 1)) for row in states})
    dates = [item for item in dates if item >= AI_AFTER_CHATGPT_START]
    if not dates:
        return []
    windows: list[dict[str, Any]] = [
        {
            "window_id": f"full_{dates[0].isoformat()}_{dates[-1].isoformat()}",
            "window_type": "full",
            "start_date": dates[0].isoformat(),
            "end_date": dates[-1].isoformat(),
            "observation_count": len(dates),
            "status": "PASS" if len(dates) >= min_observations else "INSUFFICIENT_DATA",
        }
    ]
    for year in sorted({item.year for item in dates}):
        year_dates = [item for item in dates if item.year == year]
        windows.append(
            {
                "window_id": f"yearly_{year}",
                "window_type": "yearly",
                "start_date": year_dates[0].isoformat(),
                "end_date": year_dates[-1].isoformat(),
                "observation_count": len(year_dates),
                "status": "PASS" if len(year_dates) >= min_observations else "INSUFFICIENT_DATA",
            }
        )
    date_index = pd.to_datetime([item.isoformat() for item in dates])
    month_starts = sorted({date(item.year, item.month, 1) for item in dates})
    for months in (3, 6, 12):
        for month_start in month_starts:
            window_end_ts = (
                pd.Timestamp(month_start) + pd.DateOffset(months=months) - pd.Timedelta(days=1)
            )
            selected = [
                item for item in date_index if pd.Timestamp(month_start) <= item <= window_end_ts
            ]
            if not selected:
                continue
            window_dates = [item.date() for item in selected]
            if window_dates[-1] > dates[-1]:
                continue
            windows.append(
                {
                    "window_id": (
                        f"rolling_{months}m_{window_dates[0].strftime('%Y_%m')}_"
                        f"{window_dates[-1].strftime('%Y_%m')}"
                    ),
                    "window_type": f"rolling_{months}m",
                    "start_date": window_dates[0].isoformat(),
                    "end_date": window_dates[-1].isoformat(),
                    "observation_count": len(window_dates),
                    "status": (
                        "PASS" if len(window_dates) >= min_observations else "INSUFFICIENT_DATA"
                    ),
                }
            )
    return windows


def _rolling_metrics_for_window(
    states: Sequence[Mapping[str, Any]],
    window: Mapping[str, Any],
    min_observations: int,
) -> list[dict[str, Any]]:
    start = _coerce_date(window.get("start_date"), date(1970, 1, 1))
    end = _coerce_date(window.get("end_date"), date(1970, 1, 1))
    rows = [
        row for row in states if start <= _coerce_date(row.get("date"), date(1970, 1, 1)) <= end
    ]
    results = []
    for method in sorted({str(row.get("target_method")) for row in rows}):
        method_rows = [row for row in rows if row.get("target_method") == method]
        metrics = _state_path_metrics(method_rows, min_observations=min_observations)
        results.append(
            {
                "window_id": window.get("window_id"),
                "window_type": window.get("window_type"),
                "start_date": window.get("start_date"),
                "end_date": window.get("end_date"),
                "target_method": method,
                **metrics,
                "relative_to_static_baseline": 0.0,
                "relative_to_no_trade_baseline": 0.0,
                "rank_by_return": 0,
                "rank_by_drawdown": 0,
                "rank_by_risk_adjusted": 0,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    static_return = _metric_for(results, "static_baseline", "total_return")
    no_trade_return = _metric_for(results, "no_trade_baseline", "total_return")
    for row in results:
        row["relative_to_static_baseline"] = round(
            _float(row.get("total_return")) - static_return, 10
        )
        row["relative_to_no_trade_baseline"] = round(
            _float(row.get("total_return")) - no_trade_return, 10
        )
    return results


def _state_path_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_observations: int,
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: _text(row.get("date")))
    if len(ordered) < min_observations:
        status = "INSUFFICIENT_DATA"
    else:
        status = "PASS"
    if len(ordered) < 2:
        return {
            "total_return": 0.0,
            "annualized_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": round(sum(_float(row.get("turnover")) for row in ordered), 10),
            "risk_adjusted_return_to_volatility": 0.0,
            "status": status,
        }
    start_value = _float(ordered[0].get("portfolio_value"), 1.0)
    end_value = _float(ordered[-1].get("portfolio_value"), start_value)
    total_return = end_value / start_value - 1.0 if start_value > 0 else 0.0
    daily_returns = [_float(row.get("daily_return")) for row in ordered]
    volatility = _stddev(daily_returns) * math.sqrt(252.0) if len(daily_returns) > 1 else 0.0
    annualized = _annualized_return(total_return, len(daily_returns))
    values = [_float(row.get("portfolio_value")) for row in ordered]
    peak = values[0] if values else 1.0
    drawdowns = []
    for value in values:
        peak = max(peak, value)
        drawdowns.append(value / peak - 1.0 if peak > 0 else 0.0)
    risk_adjusted = annualized / volatility if volatility > 0 else annualized
    return {
        "total_return": round(total_return, 10),
        "annualized_return": round(annualized, 10),
        "max_drawdown": round(min(drawdowns or [0.0]), 10),
        "realized_volatility": round(volatility, 10),
        "turnover": round(sum(_float(row.get("turnover")) for row in ordered), 10),
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "status": status,
    }


def _rank_rolling_metrics(metrics: list[dict[str, Any]]) -> None:
    for window_id in sorted({str(row.get("window_id")) for row in metrics}):
        rows = [
            row
            for row in metrics
            if row.get("window_id") == window_id and row.get("status") != "INSUFFICIENT_DATA"
        ]
        _assign_rank(rows, "total_return", "rank_by_return", high=True)
        _assign_rank(rows, "max_drawdown", "rank_by_drawdown", high=True)
        _assign_rank(
            rows,
            "risk_adjusted_return_to_volatility",
            "rank_by_risk_adjusted",
            high=True,
        )


def _assign_rank(
    rows: Sequence[dict[str, Any]], field: str, rank_field: str, *, high: bool
) -> None:
    ordered = sorted(rows, key=lambda row: _float(row.get(field)), reverse=high)
    for rank, row in enumerate(ordered, start=1):
        row[rank_field] = rank


def _rolling_rank_stability(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    methods = sorted({str(row.get("target_method")) for row in metrics if row.get("target_method")})
    method_count = max(1, len(methods))
    rows = []
    for method in methods:
        selected = [
            row
            for row in metrics
            if row.get("target_method") == method and _float(row.get("rank_by_return")) > 0
        ]
        if not selected:
            rows.append(
                {
                    "target_method": method,
                    "avg_rank_return": 0.0,
                    "avg_rank_drawdown": 0.0,
                    "avg_rank_risk_adjusted": 0.0,
                    "top_3_frequency": 0.0,
                    "bottom_3_frequency": 0.0,
                    "rank_stability_status": "INSUFFICIENT_DATA",
                }
            )
            continue
        avg_return = sum(_float(row.get("rank_by_return")) for row in selected) / len(selected)
        avg_drawdown = sum(_float(row.get("rank_by_drawdown")) for row in selected) / len(selected)
        avg_risk = sum(_float(row.get("rank_by_risk_adjusted")) for row in selected) / len(selected)
        top_3 = sum(1 for row in selected if _float(row.get("rank_by_return")) <= 3) / len(selected)
        bottom_3 = sum(
            1 for row in selected if _float(row.get("rank_by_return")) > method_count - 3
        ) / len(selected)
        if top_3 >= 0.6 and bottom_3 <= 0.2:
            status = "STABLE"
        elif bottom_3 >= 0.5:
            status = "UNSTABLE"
        else:
            status = "MIXED"
        rows.append(
            {
                "target_method": method,
                "avg_rank_return": round(avg_return, 6),
                "avg_rank_drawdown": round(avg_drawdown, 6),
                "avg_rank_risk_adjusted": round(avg_risk, 6),
                "top_3_frequency": round(top_3, 6),
                "bottom_3_frequency": round(bottom_3, 6),
                "rank_stability_status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "methods": rows, **SYSTEM_TARGET_SAFETY}


def _configured_regimes() -> tuple[str, ...]:
    return (
        "ai_trend",
        "tech_drawdown",
        "semiconductor_pullback",
        "risk_off",
        "sideways_choppy",
        "strong_recovery",
    )


def _regime_labels_from_states(
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> dict[str, str]:
    static_rows = [row for row in states if row.get("target_method") == "static_baseline"]
    by_date = {str(row.get("date")): _float(row.get("daily_return")) for row in static_rows}
    policy = _mapping(config.get("regime_policy"))
    risk_off = _float(policy.get("risk_off_return_threshold"), -0.015)
    drawdown = _float(policy.get("tech_drawdown_return_threshold"), -0.01)
    semi = _float(policy.get("semiconductor_pullback_return_threshold"), -0.012)
    trend = _float(policy.get("ai_trend_return_threshold"), 0.008)
    recovery = _float(policy.get("strong_recovery_return_threshold"), 0.012)
    labels = {}
    for date_text, value in by_date.items():
        if value <= risk_off:
            label = "risk_off"
        elif value <= semi:
            label = "semiconductor_pullback"
        elif value <= drawdown:
            label = "tech_drawdown"
        elif value >= recovery:
            label = "strong_recovery"
        elif value >= trend:
            label = "ai_trend"
        else:
            label = "sideways_choppy"
        labels[date_text] = label
    return labels


def _regime_method_metrics(
    states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    min_sample: int,
) -> list[dict[str, Any]]:
    rows = []
    methods = sorted({str(row.get("target_method")) for row in states if row.get("target_method")})
    no_trade_by_regime: dict[str, dict[str, Any]] = {}
    for regime in _configured_regimes():
        date_set = {date_text for date_text, label in labels.items() if label == regime}
        for method in methods:
            selected = [
                row
                for row in states
                if row.get("target_method") == method and row.get("date") in date_set
            ]
            metrics = _sample_return_metrics(selected, min_sample=min_sample)
            item = {
                "regime": regime,
                "target_method": method,
                "sample_count": len(selected),
                "total_return": metrics["total_return"],
                "avg_return": metrics["avg_return"],
                "max_drawdown": metrics["max_drawdown"],
                "realized_volatility": metrics["realized_volatility"],
                "turnover": metrics["turnover"],
                "relative_to_static_baseline": 0.0,
                "relative_to_no_trade_baseline": 0.0,
                "win_rate_vs_no_trade": 0.0,
                "risk_adjusted_return_to_volatility": metrics["risk_adjusted_return_to_volatility"],
                "status": metrics["status"],
                **SYSTEM_TARGET_SAFETY,
            }
            rows.append(item)
            if method == "no_trade_baseline":
                no_trade_by_regime[regime] = item
    static_by_regime = {
        row["regime"]: row for row in rows if row.get("target_method") == "static_baseline"
    }
    for row in rows:
        regime = str(row.get("regime"))
        static = static_by_regime.get(regime, {})
        no_trade = no_trade_by_regime.get(regime, {})
        row["relative_to_static_baseline"] = round(
            _float(row.get("total_return")) - _float(static.get("total_return")),
            10,
        )
        row["relative_to_no_trade_baseline"] = round(
            _float(row.get("total_return")) - _float(no_trade.get("total_return")),
            10,
        )
        row["win_rate_vs_no_trade"] = _win_rate_vs_method(states, labels, row, "no_trade_baseline")
    return rows


def _sample_return_metrics(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_sample: int,
) -> dict[str, Any]:
    daily = [_float(row.get("daily_return")) for row in rows]
    if not daily:
        return {
            "total_return": 0.0,
            "avg_return": 0.0,
            "max_drawdown": 0.0,
            "realized_volatility": 0.0,
            "turnover": 0.0,
            "risk_adjusted_return_to_volatility": 0.0,
            "status": "INSUFFICIENT_DATA",
        }
    equity = 1.0
    peak = 1.0
    drawdowns = []
    for value in daily:
        equity *= 1.0 + value
        peak = max(peak, equity)
        drawdowns.append(equity / peak - 1.0)
    total = equity - 1.0
    vol = _stddev(daily) * math.sqrt(252.0) if len(daily) > 1 else 0.0
    annualized = _annualized_return(total, len(daily))
    risk_adjusted = annualized / vol if vol > 0 else annualized
    return {
        "total_return": round(total, 10),
        "avg_return": round(sum(daily) / len(daily), 10),
        "max_drawdown": round(min(drawdowns or [0.0]), 10),
        "realized_volatility": round(vol, 10),
        "turnover": round(sum(_float(row.get("turnover")) for row in rows), 10),
        "risk_adjusted_return_to_volatility": round(risk_adjusted, 10),
        "status": "PASS" if len(daily) >= min_sample else "INSUFFICIENT_DATA",
    }


def _win_rate_vs_method(
    states: Sequence[Mapping[str, Any]],
    labels: Mapping[str, str],
    row: Mapping[str, Any],
    benchmark_method: str,
) -> float:
    regime = str(row.get("regime"))
    method = str(row.get("target_method"))
    date_set = {date_text for date_text, label in labels.items() if label == regime}
    method_returns = {
        str(item.get("date")): _float(item.get("daily_return"))
        for item in states
        if item.get("target_method") == method and item.get("date") in date_set
    }
    benchmark_returns = {
        str(item.get("date")): _float(item.get("daily_return"))
        for item in states
        if item.get("target_method") == benchmark_method and item.get("date") in date_set
    }
    shared = sorted(set(method_returns) & set(benchmark_returns))
    if not shared:
        return 0.0
    wins = sum(
        1 for date_text in shared if method_returns[date_text] > benchmark_returns[date_text]
    )
    return round(wins / len(shared), 6)


def _regime_method_summary(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regimes = []
    defensive_statuses = []
    for regime in _configured_regimes():
        rows = [row for row in metrics if row.get("regime") == regime]
        valid = [row for row in rows if row.get("status") != "INSUFFICIENT_DATA"]
        sample_count = max((int(_float(row.get("sample_count"))) for row in rows), default=0)
        if valid:
            best_return = _best_metric_method(valid, "total_return", high=True)
            best_drawdown = _best_metric_method(valid, "max_drawdown", high=True)
            best_risk = _best_metric_method(
                valid,
                "risk_adjusted_return_to_volatility",
                high=True,
            )
        else:
            best_return = best_drawdown = best_risk = "INSUFFICIENT_DATA"
        defensive_status = _defensive_regime_status(rows)
        defensive_statuses.append(defensive_status)
        regimes.append(
            {
                "regime": regime,
                "best_return_method": best_return,
                "best_drawdown_method": best_drawdown,
                "best_risk_adjusted_method": best_risk,
                "defensive_limited_adjustment_status": defensive_status,
                "sample_count": sample_count,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    if all(status == "INSUFFICIENT_DATA" for status in defensive_statuses):
        overall_defensive = "INSUFFICIENT_DATA"
    elif "FAIL" in defensive_statuses:
        overall_defensive = "MIXED"
    elif "MIXED" in defensive_statuses:
        overall_defensive = "MIXED"
    else:
        overall_defensive = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "regimes": regimes,
        "defensive_limited_adjustment_status": overall_defensive,
        **SYSTEM_TARGET_SAFETY,
    }


def _defensive_regime_status(rows: Sequence[Mapping[str, Any]]) -> str:
    defensive = _find_method(rows, "defensive_limited_adjustment")
    no_trade = _find_method(rows, "no_trade_baseline")
    if not defensive or defensive.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    better_return = _float(defensive.get("total_return")) >= _float(no_trade.get("total_return"))
    better_drawdown = _float(defensive.get("max_drawdown")) >= _float(no_trade.get("max_drawdown"))
    if better_return and better_drawdown:
        return "PASS"
    if better_return or better_drawdown:
        return "MIXED"
    return "FAIL"


def _stability_diagnostics(
    states: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    policy = _mapping(config.get("stability_policy"))
    large_jump = _float(policy.get("large_jump_threshold"), 0.10)
    high_jump = _float(policy.get("high_jump_threshold"), 0.20)
    stable_max = _float(policy.get("stable_max_daily_weight_change"), 0.08)
    unstable_max = _float(policy.get("unstable_max_daily_weight_change"), 0.18)
    turnover_high = _float(policy.get("high_annualized_turnover"), 4.0)
    turnover_moderate = _float(policy.get("moderate_annualized_turnover"), 1.5)
    metrics: list[dict[str, Any]] = []
    jumps: list[dict[str, Any]] = []
    turnover_rows: list[dict[str, Any]] = []
    for method in sorted(
        {str(row.get("target_method")) for row in states if row.get("target_method")}
    ):
        rows = sorted(
            [row for row in states if row.get("target_method") == method],
            key=lambda row: _text(row.get("date")),
        )
        changes = []
        cash_weights = []
        risk_weights = []
        rebalance_turnovers = []
        previous: Mapping[str, Any] | None = None
        for row in rows:
            weights = _normalize_weights(_mapping(row.get("weights")))
            cash_weights.append(_float(weights.get("CASH")))
            risk_weights.append(sum(value for symbol, value in weights.items() if symbol != "CASH"))
            if row.get("rebalance_event") is True:
                rebalance_turnovers.append(_float(row.get("turnover")))
            if previous is not None:
                previous_weights = _normalize_weights(_mapping(previous.get("weights")))
                deltas = _weight_deltas(previous_weights, weights)
                total_abs = sum(abs(value) for value in deltas.values())
                changes.append(total_abs)
                if total_abs >= large_jump:
                    symbol, delta = max(deltas.items(), key=lambda item: abs(item[1]))
                    jumps.append(
                        {
                            "date": row.get("date"),
                            "target_method": method,
                            "total_abs_weight_change": round(total_abs, 10),
                            "largest_symbol_delta": {"symbol": symbol, "delta": round(delta, 10)},
                            "jump_reason": (
                                "target_method_rebalance"
                                if row.get("rebalance_event") is True
                                else "weight_drift"
                            ),
                            "severity": (
                                "HIGH"
                                if total_abs >= high_jump
                                else "MEDIUM"
                                if total_abs >= large_jump
                                else "LOW"
                            ),
                            "broker_action_taken": False,
                            **SYSTEM_TARGET_SAFETY,
                        }
                    )
            previous = row
        avg_change = sum(changes) / len(changes) if changes else 0.0
        max_change = max(changes or [0.0])
        if not rows:
            status = "INSUFFICIENT_DATA"
        elif (
            max_change <= stable_max and len([item for item in changes if item >= large_jump]) == 0
        ):
            status = "STABLE"
        elif max_change >= unstable_max:
            status = "UNSTABLE"
        else:
            status = "MODERATE"
        total_turnover = sum(rebalance_turnovers)
        years = max(1.0 / 252.0, len(rows) / 252.0)
        annualized_turnover = total_turnover / years
        if not rows:
            turnover_status = "INSUFFICIENT_DATA"
        elif annualized_turnover >= turnover_high:
            turnover_status = "HIGH"
        elif annualized_turnover >= turnover_moderate:
            turnover_status = "MODERATE"
        else:
            turnover_status = "LOW"
        method_metric = {
            "target_method": method,
            "avg_daily_weight_change": round(avg_change, 10),
            "max_daily_weight_change": round(max_change, 10),
            "avg_rebalance_turnover": round(
                sum(rebalance_turnovers) / len(rebalance_turnovers) if rebalance_turnovers else 0.0,
                10,
            ),
            "max_rebalance_turnover": round(max(rebalance_turnovers or [0.0]), 10),
            "rebalance_count": len(rebalance_turnovers),
            "large_jump_count": len([item for item in changes if item >= large_jump]),
            "cash_weight_volatility": round(_stddev(cash_weights), 10),
            "risk_asset_weight_volatility": round(_stddev(risk_weights), 10),
            "stability_status": status,
            **SYSTEM_TARGET_SAFETY,
        }
        metrics.append(method_metric)
        turnover_rows.append(
            {
                "target_method": method,
                "total_turnover": round(total_turnover, 10),
                "annualized_turnover": round(annualized_turnover, 10),
                "turnover_status": turnover_status,
                "warning": ["high_turnover"] if turnover_status == "HIGH" else [],
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return (
        metrics,
        jumps,
        {"schema_version": SCHEMA_VERSION, "methods": turnover_rows, **SYSTEM_TARGET_SAFETY},
    )


def _selection_scorecard(
    rolling: Mapping[str, Any],
    regime: Mapping[str, Any],
    stability: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    rank_rows = _records(_mapping(rolling.get("rolling_rank_stability")).get("methods"))
    regime_summary = _records(_mapping(regime.get("regime_method_summary")).get("regimes"))
    stability_rows = _records(stability.get("method_stability_metrics"))
    turnover_rows = _records(_mapping(stability.get("turnover_diagnostics")).get("methods"))
    methods = sorted(
        {
            str(row.get("target_method"))
            for row in [*rank_rows, *stability_rows, *turnover_rows]
            if row.get("target_method")
        }
    )
    method_count = max(1, len(methods))
    policy = _mapping(config.get("selection_policy"))
    weights = _mapping(policy.get("score_weights"))
    return_weight = _float(weights.get("return"), 0.25)
    drawdown_weight = _float(weights.get("drawdown"), 0.25)
    risk_weight = _float(weights.get("risk_adjusted"), 0.20)
    regime_weight = _float(weights.get("regime"), 0.15)
    stability_weight = _float(weights.get("stability"), 0.15)
    turnover_high = _float(
        _mapping(config.get("stability_policy")).get("high_annualized_turnover"),
        4.0,
    )
    rows = []
    for method in methods:
        rank = _find_method(rank_rows, method)
        stability_row = _find_method(stability_rows, method)
        turnover = _find_method(turnover_rows, method)
        return_score = _rank_score(_float(rank.get("avg_rank_return")), method_count)
        drawdown_score = _rank_score(_float(rank.get("avg_rank_drawdown")), method_count)
        risk_score = _rank_score(_float(rank.get("avg_rank_risk_adjusted")), method_count)
        regime_score = _regime_score(regime_summary, method)
        stability_score = _stability_status_score(_text(stability_row.get("stability_status")))
        turnover_penalty = min(1.0, _float(turnover.get("annualized_turnover")) / turnover_high)
        overall = (
            return_score * return_weight
            + drawdown_score * drawdown_weight
            + risk_score * risk_weight
            + regime_score * regime_weight
            + stability_score * stability_weight
            - turnover_penalty * _float(weights.get("turnover_penalty"), 0.10)
        )
        if rank.get("rank_stability_status") == "INSUFFICIENT_DATA":
            status = "INSUFFICIENT_DATA"
        elif overall >= _float(policy.get("continue_observation_score"), 0.55):
            status = "CONTINUE_OBSERVATION"
        elif overall >= _float(policy.get("review_required_score"), 0.35):
            status = "REVIEW_REQUIRED"
        else:
            status = "NOT_RECOMMENDED"
        rows.append(
            {
                "target_method": method,
                "return_score": round(return_score, 6),
                "drawdown_score": round(drawdown_score, 6),
                "risk_adjusted_score": round(risk_score, 6),
                "regime_score": round(regime_score, 6),
                "stability_score": round(stability_score, 6),
                "turnover_penalty": round(turnover_penalty, 6),
                "overall_score": round(max(0.0, overall), 6),
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return {"schema_version": SCHEMA_VERSION, "methods": rows, **SYSTEM_TARGET_SAFETY}


def _selection_decision(
    scorecard: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, Any]:
    rows = _records(scorecard.get("methods"))
    policy = _mapping(config.get("selection_policy"))
    preferred_order = _texts(policy.get("preferred_method_order")) or [
        "limited_adjustment",
        "defensive_limited_adjustment",
        "equal_weight_shadow_candidates",
        "consensus_target",
    ]
    eligible = [row for row in rows if row.get("status") != "INSUFFICIENT_DATA"]
    if not eligible:
        recommended = "INSUFFICIENT_DATA"
        decision_status = "INSUFFICIENT_DATA"
    else:
        best_score = max(_float(row.get("overall_score")) for row in eligible)
        tolerance = _float(policy.get("preferred_method_score_tolerance"), 0.10)
        preferred_candidates = [
            row
            for method in preferred_order
            for row in eligible
            if row.get("target_method") == method
            and _float(row.get("overall_score")) >= best_score - tolerance
            and row.get("status") != "NOT_RECOMMENDED"
        ]
        recommended_row = (
            preferred_candidates[0]
            if preferred_candidates
            else max(
                eligible,
                key=lambda row: _float(row.get("overall_score")),
            )
        )
        recommended = _text(recommended_row.get("target_method"))
        decision_status = (
            "CONTINUE_OBSERVATION"
            if recommended_row.get("status") == "CONTINUE_OBSERVATION"
            else "REVIEW_REQUIRED"
        )
    secondary = [
        _text(row.get("target_method"))
        for row in sorted(
            eligible, key=lambda item: _float(item.get("overall_score")), reverse=True
        )
        if row.get("target_method") != recommended and row.get("status") != "NOT_RECOMMENDED"
    ][:2]
    reference_only = _texts(policy.get("reference_only_methods")) or ["consensus_target"]
    reference_only = [method for method in reference_only if method != recommended]
    not_recommended = [
        _text(row.get("target_method")) for row in rows if row.get("status") == "NOT_RECOMMENDED"
    ]
    reason = (
        f"{recommended} is selected for continued research observation based on rolling rank, "
        "regime behavior, stability, and turnover diagnostics. The decision remains "
        "research-only and does not allow official target weights or broker action."
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "selection_review_id": "",
        "recommended_research_method": recommended,
        "secondary_research_methods": secondary,
        "reference_only_methods": reference_only,
        "not_recommended_methods": not_recommended,
        "decision_status": decision_status,
        "reason": reason,
        "next_action": "continue_paper_shadow_observation",
        **SYSTEM_TARGET_SAFETY,
    }


def _selection_attribution_rows(
    scorecard: Mapping[str, Any],
    decision: Mapping[str, Any],
    selection: Mapping[str, Any],
) -> list[dict[str, Any]]:
    source_rows = _records(scorecard.get("methods"))
    ordered = sorted(source_rows, key=lambda row: _float(row.get("overall_score")), reverse=True)
    recommended = _text(decision.get("recommended_research_method"))
    secondary = set(_texts(decision.get("secondary_research_methods")))
    reference_only = set(_texts(decision.get("reference_only_methods")))
    not_recommended = set(_texts(decision.get("not_recommended_methods")))
    component_fields = (
        "return_score",
        "drawdown_score",
        "risk_adjusted_score",
        "regime_score",
        "stability_score",
    )
    best_by_component = {
        field: _max_field_method(source_rows, field) for field in component_fields
    }
    data_quality_penalty = _data_quality_attribution_penalty(
        _text(selection.get("data_quality_status"))
    )
    rows: list[dict[str, Any]] = []
    for rank, row in enumerate(ordered, start=1):
        method = _text(row.get("target_method"))
        score_components = {
            "return_score": round(_float(row.get("return_score")), 6),
            "drawdown_score": round(_float(row.get("drawdown_score")), 6),
            "risk_adjusted_score": round(_float(row.get("risk_adjusted_score")), 6),
            "regime_score": round(_float(row.get("regime_score")), 6),
            "stability_score": round(_float(row.get("stability_score")), 6),
            "turnover_penalty": round(_float(row.get("turnover_penalty")), 6),
            "data_quality_penalty": round(data_quality_penalty, 6),
        }
        if method == recommended:
            selection_status = "recommended_research_method"
        elif method in secondary:
            selection_status = "secondary_research_method"
        elif method in reference_only:
            selection_status = "reference_only"
        elif method in not_recommended or row.get("status") == "NOT_RECOMMENDED":
            selection_status = "not_recommended"
        else:
            selection_status = "observed_method"
        selection_reasons = _selection_component_reasons(
            method=method,
            row=row,
            recommended=recommended,
            best_by_component=best_by_component,
            decision=decision,
            rank=rank,
        )
        rows.append(
            {
                "target_method": method,
                "overall_score": round(_float(row.get("overall_score")), 6),
                "score_components": score_components,
                "rank": rank,
                "selection_status": selection_status,
                "selection_reasons": selection_reasons,
                "weaknesses": _selection_component_weaknesses(row),
                "review_required_reasons": _selection_row_review_reasons(
                    method=method,
                    row=row,
                    decision=decision,
                    data_quality_status=_text(selection.get("data_quality_status")),
                ),
                **SYSTEM_TARGET_SAFETY,
            }
        )
    return rows


def _selection_component_reasons(
    *,
    method: str,
    row: Mapping[str, Any],
    recommended: str,
    best_by_component: Mapping[str, str],
    decision: Mapping[str, Any],
    rank: int,
) -> list[str]:
    reasons: list[str] = []
    if method == recommended:
        reasons.append("selected_by_research_method_policy")
        if rank > 1:
            reasons.append("preferred_method_within_selection_tolerance")
    for field, best_method in best_by_component.items():
        if best_method == method:
            reasons.append(f"best_{field}")
    if _float(row.get("turnover_penalty")) <= 0.0:
        reasons.append("no_turnover_penalty")
    if row.get("status") == "CONTINUE_OBSERVATION":
        reasons.append("score_status_continue_observation")
    elif row.get("status") == "REVIEW_REQUIRED":
        reasons.append("score_status_review_required")
    if method in set(_texts(decision.get("reference_only_methods"))):
        reasons.append("reference_only_policy")
    return reasons


def _selection_component_weaknesses(row: Mapping[str, Any]) -> list[str]:
    weaknesses: list[str] = []
    for field in (
        "return_score",
        "drawdown_score",
        "risk_adjusted_score",
        "regime_score",
        "stability_score",
    ):
        if _float(row.get(field)) < 0.5:
            weaknesses.append(f"{field}_below_midpoint")
    if _float(row.get("turnover_penalty")) >= 0.5:
        weaknesses.append("turnover_penalty_high")
    if row.get("status") == "NOT_RECOMMENDED":
        weaknesses.append("selection_score_not_recommended")
    return weaknesses


def _selection_row_review_reasons(
    *,
    method: str,
    row: Mapping[str, Any],
    decision: Mapping[str, Any],
    data_quality_status: str,
) -> list[str]:
    reasons: list[str] = []
    if data_quality_status == "PASS_WITH_WARNINGS":
        reasons.append("data_quality_pass_with_warnings")
    if data_quality_status == "FAIL":
        reasons.append("data_quality_failed")
    if row.get("status") == "REVIEW_REQUIRED":
        reasons.append("method_score_review_required")
    if method == decision.get("recommended_research_method") and (
        decision.get("decision_status") == "REVIEW_REQUIRED"
    ):
        reasons.append("forward_confirmation_missing")
    return reasons


def _data_quality_attribution_penalty(status: str) -> float:
    if status == "PASS_WITH_WARNINGS":
        return DATA_QUALITY_WARNING_ATTRIBUTION_PENALTY
    if status == "FAIL":
        return DATA_QUALITY_FAIL_ATTRIBUTION_PENALTY
    return 0.0


def _recommendation_reason_breakdown(
    rows: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    recommended = _text(decision.get("recommended_research_method"))
    recommended_row = _find_method(rows, recommended)
    top_method = _text(rows[0].get("target_method")) if rows else "INSUFFICIENT_DATA"
    primary_reason = (
        "top_overall_score"
        if recommended == top_method
        else "preferred_research_method_within_selection_tolerance"
    )
    evidence = [
        f"recommended={recommended}",
        f"recommended_rank={recommended_row.get('rank', 'MISSING')}",
        f"recommended_overall_score={recommended_row.get('overall_score', 'MISSING')}",
        f"top_overall_score_method={top_method}",
    ]
    primary = [
        {
            "reason": primary_reason,
            "evidence": evidence,
            "confidence": "MEDIUM" if recommended_row else "LOW",
        }
    ]
    if recommended_row:
        components = _mapping(recommended_row.get("score_components"))
        primary.append(
            {
                "reason": "balanced_return_risk_stability_review_candidate",
                "evidence": [
                    f"return_score={components.get('return_score')}",
                    f"drawdown_score={components.get('drawdown_score')}",
                    f"risk_adjusted_score={components.get('risk_adjusted_score')}",
                    f"stability_score={components.get('stability_score')}",
                    f"turnover_penalty={components.get('turnover_penalty')}",
                ],
                "confidence": "MEDIUM",
            }
        )
    return {
        "recommended_research_method": recommended,
        "primary_reasons": primary,
        "secondary_reasons": [
            {
                "reason": "research_only_safety_boundary_preserved",
                "evidence": [
                    "not_official_target_weights=true",
                    "broker_action_allowed=false",
                    "production_effect=none",
                ],
                "confidence": "HIGH",
            }
        ],
        "why_not_consensus_target": _why_not_method(rows, recommended, "consensus_target"),
        "why_not_defensive_limited_adjustment": _why_not_method(
            rows, recommended, "defensive_limited_adjustment"
        ),
        "why_not_static_baseline": _why_not_method(rows, recommended, "static_baseline"),
        "why_not_selected_top_candidate": _why_not_method(
            rows, recommended, "selected_top_candidate"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _why_not_method(
    rows: Sequence[Mapping[str, Any]],
    recommended: str,
    method: str,
) -> list[str]:
    row = _find_method(rows, method)
    recommended_row = _find_method(rows, recommended)
    if not row:
        return [f"{method}_not_available"]
    reasons: list[str] = []
    if row.get("selection_status") == "reference_only":
        reasons.append(f"{method}_configured_reference_only")
    if row.get("selection_status") == "not_recommended":
        reasons.append(f"{method}_selection_status_not_recommended")
    if _float(row.get("overall_score")) > _float(recommended_row.get("overall_score")):
        reasons.append("higher_overall_score_but_not_preferred_research_method")
    weaknesses = _texts(row.get("weaknesses"))
    if weaknesses:
        reasons.extend(weaknesses[:3])
    if not reasons:
        reasons.append(f"{method}_not_selected_by_research_policy")
    return reasons


def _review_required_reason_breakdown(
    selection: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    decision_status = _text(decision.get("decision_status"), "REVIEW_REQUIRED")
    data_quality = _text(selection.get("data_quality_status"))
    recommended = _text(decision.get("recommended_research_method"))
    recommended_row = _find_method(rows, recommended)
    reasons: list[dict[str, Any]] = []
    if data_quality == "PASS_WITH_WARNINGS":
        reasons.append(
            {
                "reason": "data_quality_pass_with_warnings",
                "severity": "WARNING",
                "blocking": False,
            }
        )
    elif data_quality == "FAIL":
        reasons.append(
            {
                "reason": "data_quality_failed",
                "severity": "BLOCKER",
                "blocking": True,
            }
        )
    if decision_status == "REVIEW_REQUIRED":
        reasons.append(
            {
                "reason": "forward_confirmation_missing",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if recommended_row.get("review_required_reasons"):
        reasons.append(
            {
                "reason": "recommended_method_requires_owner_review",
                "severity": "REVIEW_REQUIRED",
                "blocking": True,
            }
        )
    if recommended_row and _float(recommended_row.get("rank")) > 1:
        reasons.append(
            {
                "reason": "recommended_method_not_top_overall_score",
                "severity": "WARNING",
                "blocking": False,
            }
        )
    if not reasons:
        reasons.append(
            {
                "reason": "no_blocking_review_required_reason_detected",
                "severity": "INFO",
                "blocking": False,
            }
        )
    can_harden = not any(row.get("blocking") is True for row in reasons)
    return {
        "decision_status": decision_status,
        "review_required_reasons": reasons,
        "can_harden_research_method": can_harden,
        "can_trigger_official_target_weights": False,
        "can_trigger_production": False,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_long_window_risk_return(
    backfill: Mapping[str, Any],
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited = _method_path_metrics(states, "limited_adjustment")
    static = _method_path_metrics(states, "static_baseline")
    no_trade = _method_path_metrics(states, "no_trade_baseline")
    metrics = {
        "total_return": limited["total_return"],
        "annualized_return": limited["annualized_return"],
        "max_drawdown": limited["max_drawdown"],
        "realized_volatility": limited["realized_volatility"],
        "turnover": limited["turnover"],
        "relative_to_static_baseline": round(
            _float(limited.get("total_return")) - _float(static.get("total_return")),
            10,
        ),
        "relative_to_no_trade_baseline": round(
            _float(limited.get("total_return")) - _float(no_trade.get("total_return")),
            10,
        ),
    }
    return {
        "target_method": "limited_adjustment",
        "date_start": backfill.get("date_start"),
        "date_end": backfill.get("date_end"),
        "metrics": metrics,
        "risk_return_status": _risk_return_status(limited, static),
        "confidence": _long_window_confidence(
            observation_count=int(_float(limited.get("observation_count"))),
            data_quality_status=_text(backfill.get("data_quality_status")),
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _method_path_metrics(
    states: Sequence[Mapping[str, Any]],
    method: str,
) -> dict[str, Any]:
    rows = [row for row in states if row.get("target_method") == method]
    metrics = _state_path_metrics(rows, min_observations=2)
    metrics["observation_count"] = len(rows)
    return metrics


def _risk_return_status(
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
) -> str:
    if not limited.get("observation_count") or not baseline.get("observation_count"):
        return "INSUFFICIENT_DATA"
    return_improves = _float(limited.get("total_return")) > _float(baseline.get("total_return"))
    risk_improves = _float(limited.get("max_drawdown")) >= _float(baseline.get("max_drawdown"))
    if return_improves and risk_improves:
        return "RETURN_IMPROVES_RISK_IMPROVES"
    if return_improves and not risk_improves:
        return "RETURN_IMPROVES_RISK_WORSENS"
    if not return_improves and risk_improves:
        return "RETURN_WORSE_RISK_IMPROVES"
    return "RETURN_WORSE_RISK_WORSE"


def _long_window_confidence(*, observation_count: int, data_quality_status: str) -> str:
    if (
        observation_count >= LONG_WINDOW_HIGH_CONFIDENCE_OBSERVATIONS
        and data_quality_status == "PASS"
    ):
        return "HIGH"
    if observation_count >= LONG_WINDOW_MEDIUM_CONFIDENCE_OBSERVATIONS and data_quality_status in {
        "PASS",
        "PASS_WITH_WARNINGS",
    }:
        return "MEDIUM"
    return "LOW"


def _limited_vs_baseline_breakdown(
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited = _method_path_metrics(states, "limited_adjustment")
    comparisons = []
    for baseline_method in ("static_baseline", "no_trade_baseline"):
        baseline = _method_path_metrics(states, baseline_method)
        comparisons.append(_baseline_comparison(limited, baseline, baseline_method))
    return {"comparisons": comparisons, **SYSTEM_TARGET_SAFETY}


def _baseline_comparison(
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
    baseline_method: str,
) -> dict[str, Any]:
    return_delta = round(
        _float(limited.get("total_return")) - _float(baseline.get("total_return")),
        10,
    )
    drawdown_delta = round(
        _float(limited.get("max_drawdown")) - _float(baseline.get("max_drawdown")),
        10,
    )
    volatility_delta = round(
        _float(limited.get("realized_volatility")) - _float(baseline.get("realized_volatility")),
        10,
    )
    turnover_delta = round(_float(limited.get("turnover")) - _float(baseline.get("turnover")), 10)
    return {
        "baseline": baseline_method,
        "return_delta": return_delta,
        "drawdown_delta": drawdown_delta,
        "volatility_delta": volatility_delta,
        "turnover_delta": turnover_delta,
        "conclusion": _comparison_conclusion(
            limited=limited,
            baseline=baseline,
            return_delta=return_delta,
            drawdown_delta=drawdown_delta,
            volatility_delta=volatility_delta,
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _comparison_conclusion(
    *,
    limited: Mapping[str, Any],
    baseline: Mapping[str, Any],
    return_delta: float,
    drawdown_delta: float,
    volatility_delta: float,
) -> str:
    if not limited.get("observation_count") or not baseline.get("observation_count"):
        return "insufficient_data"
    risk_better = drawdown_delta >= 0.0 and volatility_delta <= 0.0
    risk_worse = drawdown_delta < 0.0 and volatility_delta > 0.0
    if return_delta > 0.0 and risk_better:
        return "limited_better"
    if return_delta <= 0.0 and risk_worse:
        return "baseline_better"
    return "mixed"


def _limited_exposure_path_analysis(
    states: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    limited_rows = [row for row in states if row.get("target_method") == "limited_adjustment"]
    static_rows = [row for row in states if row.get("target_method") == "static_baseline"]
    limited_exposure = _exposure_summary(limited_rows)
    static_exposure = _exposure_summary(static_rows)
    avg_risk = _float(limited_exposure.get("avg_risk_asset_weight"))
    static_avg_risk = _float(static_exposure.get("avg_risk_asset_weight"))
    if not limited_rows:
        interpretation = "mixed"
    elif avg_risk > static_avg_risk + EXPOSURE_SIMILARITY_TOLERANCE:
        interpretation = "higher_risk_exposure"
    elif avg_risk < static_avg_risk - EXPOSURE_SIMILARITY_TOLERANCE:
        interpretation = "lower_risk_exposure"
    else:
        interpretation = "similar_risk_exposure"
    warnings: list[str] = []
    if interpretation == "higher_risk_exposure":
        warnings.append("limited_adjustment_higher_risk_asset_exposure")
    return {
        "target_method": "limited_adjustment",
        **limited_exposure,
        "risk_exposure_interpretation": interpretation,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _exposure_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    risk_weights: list[float] = []
    semiconductor_weights: list[float] = []
    cash_weights: list[float] = []
    for row in rows:
        weights = _normalize_weights(_mapping(row.get("weights")))
        risk_weights.append(sum(value for symbol, value in weights.items() if symbol != "CASH"))
        semiconductor_weights.append(
            sum(_float(weights.get(symbol)) for symbol in ("SMH", "SOXX"))
        )
        cash_weights.append(_float(weights.get("CASH")))
    return {
        "avg_risk_asset_weight": round(_mean_float(risk_weights), 10),
        "max_risk_asset_weight": round(max(risk_weights or [0.0]), 10),
        "avg_semiconductor_weight": round(_mean_float(semiconductor_weights), 10),
        "max_semiconductor_weight": round(max(semiconductor_weights or [0.0]), 10),
        "avg_cash_weight": round(_mean_float(cash_weights), 10),
        "min_cash_weight": round(min(cash_weights or [0.0]), 10),
    }


def _latest_or_run_rolling_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    rolling_eval_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        rolling_eval_dir,
        "rolling_eval_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_rolling_eval_report_payload(
            rolling_eval_id=existing.name,
            output_dir=rolling_eval_dir,
        )
    run = run_paper_shadow_rolling_eval(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=rolling_eval_dir,
    )
    return paper_shadow_rolling_eval_report_payload(
        rolling_eval_id=run["rolling_eval_id"],
        output_dir=rolling_eval_dir,
    )


def _latest_or_run_regime_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    regime_review_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        regime_review_dir,
        "paper_shadow_regime_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_regime_review_report_payload(
            regime_review_id=existing.name,
            output_dir=regime_review_dir,
        )
    run = run_paper_shadow_regime_review(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=regime_review_dir,
    )
    return paper_shadow_regime_review_report_payload(
        regime_review_id=run["regime_review_id"],
        output_dir=regime_review_dir,
    )


def _latest_or_run_stability_for_backfill(
    backfill_id: str,
    *,
    backfill_dir: Path,
    stability_dir: Path,
) -> dict[str, Any]:
    existing = _matching_child_artifact_dir(
        stability_dir,
        "paper_shadow_stability_manifest.json",
        backfill_id,
    )
    if existing is not None:
        return paper_shadow_stability_report_payload(
            stability_id=existing.name,
            output_dir=stability_dir,
        )
    run = run_paper_shadow_stability(
        backfill_id=backfill_id,
        backfill_dir=backfill_dir,
        output_dir=stability_dir,
    )
    return paper_shadow_stability_report_payload(
        stability_id=run["stability_id"],
        output_dir=stability_dir,
    )


def _matching_child_artifact_dir(root: Path, manifest_name: str, backfill_id: str) -> Path | None:
    if not root.exists():
        return None
    candidates = sorted(
        [path for path in root.glob(f"*/{manifest_name}") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in candidates:
        payload = _read_optional_json(path) or {}
        if payload.get("backfill_id") == backfill_id:
            return path.parent
    return None


def _limited_rolling_consistency_summary(rolling: Mapping[str, Any]) -> dict[str, Any]:
    rank_rows = _records(_mapping(rolling.get("rolling_rank_stability")).get("methods"))
    metrics = _records(rolling.get("rolling_method_metrics"))
    limited_rank = _find_method(rank_rows, "limited_adjustment")
    limited_metrics = [
        row
        for row in metrics
        if row.get("target_method") == "limited_adjustment"
        and _float(row.get("rank_by_return")) > 0
    ]
    total = len({str(row.get("window_id")) for row in limited_metrics})
    top_risk = (
        sum(1 for row in limited_metrics if _float(row.get("rank_by_risk_adjusted")) <= 3) / total
        if total
        else 0.0
    )
    top_return = _float(limited_rank.get("top_3_frequency"))
    bottom = _float(limited_rank.get("bottom_3_frequency"))
    if not total:
        status = "INSUFFICIENT_DATA"
    elif top_return >= 0.6 and top_risk >= 0.6 and bottom <= 0.2:
        status = "STABLE"
    elif bottom >= 0.5:
        status = "UNSTABLE"
    else:
        status = "MIXED"
    return {
        "target_method": "limited_adjustment",
        "rolling_windows_total": total,
        "top_3_frequency_by_return": round(top_return, 6),
        "top_3_frequency_by_risk_adjusted": round(top_risk, 6),
        "bottom_3_frequency": round(bottom, 6),
        "avg_rank_return": round(_float(limited_rank.get("avg_rank_return")), 6),
        "avg_rank_risk_adjusted": round(_float(limited_rank.get("avg_rank_risk_adjusted")), 6),
        "rolling_consistency_status": status,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_regime_consistency_summary(regime: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _records(regime.get("method_regime_metrics"))
    rows: list[dict[str, Any]] = []
    for regime_name in _configured_regimes():
        regime_rows = [
            row
            for row in metrics
            if row.get("regime") == regime_name and row.get("status") != "INSUFFICIENT_DATA"
        ]
        limited = _find_method(
            [row for row in metrics if row.get("regime") == regime_name],
            "limited_adjustment",
        )
        rank = 0
        if regime_rows:
            ordered = sorted(
                regime_rows,
                key=lambda row: _float(row.get("total_return")),
                reverse=True,
            )
            for index, item in enumerate(ordered, start=1):
                if item.get("target_method") == "limited_adjustment":
                    rank = index
                    break
        status = _limited_regime_status(limited)
        rows.append(
            {
                "regime": regime_name,
                "relative_to_static_baseline": round(
                    _float(limited.get("relative_to_static_baseline")),
                    10,
                ),
                "relative_to_no_trade_baseline": round(
                    _float(limited.get("relative_to_no_trade_baseline")),
                    10,
                ),
                "rank": rank,
                "status": status,
                **SYSTEM_TARGET_SAFETY,
            }
        )
    statuses = {str(row.get("status")) for row in rows}
    pressure_fail = any(
        row.get("status") == "FAIL"
        and row.get("regime") in {"tech_drawdown", "semiconductor_pullback", "risk_off"}
        for row in rows
    )
    if statuses == {"INSUFFICIENT_DATA"}:
        overall = "INSUFFICIENT_DATA"
    elif pressure_fail:
        overall = "WEAK_IN_PRESSURE"
    elif "FAIL" in statuses:
        overall = "REGIME_DEPENDENT"
    else:
        overall = "BROADLY_CONSISTENT"
    return {
        "target_method": "limited_adjustment",
        "regimes": rows,
        "regime_consistency_status": overall,
        **SYSTEM_TARGET_SAFETY,
    }


def _limited_regime_status(limited: Mapping[str, Any]) -> str:
    if not limited or limited.get("status") == "INSUFFICIENT_DATA":
        return "INSUFFICIENT_DATA"
    rel_static = _float(limited.get("relative_to_static_baseline"))
    rel_no_trade = _float(limited.get("relative_to_no_trade_baseline"))
    if rel_static >= 0.0 and rel_no_trade >= 0.0:
        return "PASS"
    if rel_static >= 0.0 or rel_no_trade >= 0.0:
        return "PASS_WITH_WARNINGS"
    return "FAIL"


def _limited_stability_consistency_summary(stability: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _find_method(
        _records(stability.get("method_stability_metrics")),
        "limited_adjustment",
    )
    turnover = _find_method(
        _records(_mapping(stability.get("turnover_diagnostics")).get("methods")),
        "limited_adjustment",
    )
    return {
        "target_method": "limited_adjustment",
        "avg_rebalance_turnover": round(_float(metrics.get("avg_rebalance_turnover")), 10),
        "max_rebalance_turnover": round(_float(metrics.get("max_rebalance_turnover")), 10),
        "large_jump_count": int(_float(metrics.get("large_jump_count"))),
        "stability_status": _text(metrics.get("stability_status"), "INSUFFICIENT_DATA"),
        "turnover_status": _text(turnover.get("turnover_status"), "INSUFFICIENT_DATA"),
        **SYSTEM_TARGET_SAFETY,
    }


def _data_warning_inventory(backfill: Mapping[str, Any]) -> dict[str, Any]:
    data_quality = _text(backfill.get("data_quality_status"), _text(backfill.get("data_quality")))
    quality_payload = _mapping(backfill.get("backfill_data_quality"))
    warnings: list[dict[str, Any]] = []
    for item in [
        *_records(quality_payload.get("warnings")),
        *_records(quality_payload.get("issues")),
    ]:
        severity = _text(item.get("severity"), "WARNING")
        if severity not in {"WARNING", "INFO"}:
            continue
        warnings.append(
            {
                "warning_id": _text(
                    item.get("warning_id"),
                    _text(item.get("code"), "data_quality_warning"),
                ),
                "severity": severity,
                "affected_symbols": _texts(item.get("affected_symbols")),
                "affected_dates": _texts(item.get("affected_dates")),
                "potential_metric_impact": _text(item.get("potential_metric_impact"), "UNKNOWN"),
            }
        )
    missing_symbols = _texts(quality_payload.get("missing_symbols"))
    missing_dates = _texts(quality_payload.get("missing_price_dates"))
    if missing_symbols:
        warnings.append(
            {
                "warning_id": "missing_symbols_present",
                "severity": "WARNING",
                "affected_symbols": missing_symbols,
                "affected_dates": [],
                "potential_metric_impact": "HIGH",
            }
        )
    if missing_dates:
        warnings.append(
            {
                "warning_id": "missing_price_dates_present",
                "severity": "WARNING",
                "affected_symbols": [],
                "affected_dates": missing_dates,
                "potential_metric_impact": "MEDIUM",
            }
        )
    if data_quality == "PASS_WITH_WARNINGS" and not warnings:
        warnings.append(
            {
                "warning_id": "pass_with_warnings_detail_unavailable",
                "severity": "WARNING",
                "affected_symbols": [],
                "affected_dates": [],
                "potential_metric_impact": "UNKNOWN",
            }
        )
    return {
        "backfill_id": backfill.get("backfill_id"),
        "data_quality": data_quality,
        "warnings": warnings,
        **SYSTEM_TARGET_SAFETY,
    }


def _affected_metrics_from_warnings(inventory: Mapping[str, Any]) -> dict[str, Any]:
    warnings = _records(inventory.get("warnings"))
    impact_levels = {_text(row.get("potential_metric_impact"), "UNKNOWN") for row in warnings}
    if not warnings:
        level = "LOW"
        affected: bool | None = False
        reason = "data_quality_pass_without_recorded_warning"
    elif "UNKNOWN" in impact_levels:
        level = "UNKNOWN"
        affected = None
        reason = "warning_detail_missing_or_unquantified"
    elif "HIGH" in impact_levels:
        level = "HIGH"
        affected = True
        reason = "high_potential_data_warning_impact"
    elif "MEDIUM" in impact_levels:
        level = "MEDIUM"
        affected = True
        reason = "medium_potential_data_warning_impact"
    else:
        level = "LOW"
        affected = False
        reason = "warnings_not_expected_to_move_core_metrics"
    return {
        "metrics": [
            {
                "metric": metric,
                "affected": affected,
                "impact_level": level,
                "reason": reason,
            }
            for metric in ("total_return", "max_drawdown", "realized_volatility", "turnover")
        ],
        **SYSTEM_TARGET_SAFETY,
    }


def _recommendation_sensitivity_to_warnings(
    selection: Mapping[str, Any],
    inventory: Mapping[str, Any],
    affected_metrics: Mapping[str, Any],
) -> dict[str, Any]:
    data_quality = _text(inventory.get("data_quality"))
    metrics = _records(affected_metrics.get("metrics"))
    impact_levels = {_text(row.get("impact_level")) for row in metrics}
    warning_ids = _texts([row.get("warning_id") for row in _records(inventory.get("warnings"))])
    if data_quality == "FAIL":
        stability = "UNSTABLE"
        would_change: bool | None = True
        decision = "BLOCKED"
        blocking = ["data_quality_failed"]
    elif "UNKNOWN" in impact_levels:
        stability = "REVIEW_REQUIRED"
        would_change = None
        decision = "REVIEW_REQUIRED"
        blocking = ["warning_metric_impact_unknown"]
    elif data_quality == "PASS_WITH_WARNINGS" and {"HIGH", "MEDIUM"} & impact_levels:
        stability = "REVIEW_REQUIRED"
        would_change = None
        decision = "REVIEW_REQUIRED"
        blocking = ["warning_metric_impact_potentially_material"]
    else:
        stability = "STABLE"
        would_change = False
        decision = "ACCEPT_FOR_RESEARCH"
        blocking = []
    return {
        "recommended_research_method": _text(
            _mapping(selection.get("selection_decision")).get("recommended_research_method"),
            "limited_adjustment",
        ),
        "recommendation_stability": stability,
        "would_change_if_warnings_excluded": would_change,
        "warning_blocking_reasons": blocking,
        "warning_ids": warning_ids,
        "data_quality_decision": decision,
        **SYSTEM_TARGET_SAFETY,
    }


def _research_method_hardening_decision(
    attribution: Mapping[str, Any],
    risk: Mapping[str, Any],
    consistency: Mapping[str, Any],
    data_warning: Mapping[str, Any],
) -> dict[str, Any]:
    recommendation = _mapping(attribution.get("recommendation_reason_breakdown"))
    review = _mapping(attribution.get("review_required_reason_breakdown"))
    risk_return = _mapping(risk.get("long_window_risk_return"))
    rolling = _mapping(consistency.get("rolling_consistency_summary"))
    regime = _mapping(consistency.get("regime_consistency_summary"))
    stability = _mapping(consistency.get("stability_consistency_summary"))
    warning_sensitivity = _mapping(data_warning.get("recommendation_sensitivity_to_warnings"))
    recommended = _text(recommendation.get("recommended_research_method"), "MISSING")
    candidate = "limited_adjustment"
    limited_row = _find_method(
        _records(attribution.get("method_score_attribution")),
        candidate,
    )
    blocking_issues = [
        _text(row.get("reason"))
        for row in _records(review.get("review_required_reasons"))
        if row.get("blocking") is True
    ]
    warnings: list[str] = []
    if warning_sensitivity.get("data_quality_decision") != "ACCEPT_FOR_RESEARCH":
        blocking_issues.append(
            "data_quality_warning_impact_"
            + _text(warning_sensitivity.get("data_quality_decision"), "review_required").lower()
        )
    risk_status = _text(risk_return.get("risk_return_status"))
    if risk_status == "RETURN_WORSE_RISK_WORSE":
        blocking_issues.append("long_window_return_worse_and_risk_worse")
    elif risk_status == "RETURN_IMPROVES_RISK_WORSENS":
        warnings.append("long_window_return_improves_but_risk_worsens")
    rolling_status = _text(rolling.get("rolling_consistency_status"))
    regime_status = _text(regime.get("regime_consistency_status"))
    stability_status = _text(stability.get("stability_status"))
    if rolling_status == "UNSTABLE":
        blocking_issues.append("rolling_consistency_unstable")
    if regime_status == "WEAK_IN_PRESSURE":
        blocking_issues.append("weak_in_pressure_regimes")
    if stability_status == "UNSTABLE":
        blocking_issues.append("weight_path_unstable")
    if regime_status == "REGIME_DEPENDENT":
        warnings.append("regime_dependent_performance")
    if rolling_status == "MIXED":
        warnings.append("rolling_consistency_mixed")
    if recommended != candidate:
        blocking_issues.append("limited_adjustment_not_recommended_by_selection_review")
    blocking_issues = sorted(set(item for item in blocking_issues if item))
    warnings = sorted(set(item for item in warnings if item))
    if (
        "long_window_return_worse_and_risk_worse" in blocking_issues
        and "weight_path_unstable" in blocking_issues
    ):
        hardening_decision = "REJECT"
    elif blocking_issues:
        hardening_decision = "REVIEW_REQUIRED"
    elif warnings:
        hardening_decision = "CONTINUE_OBSERVATION"
    else:
        hardening_decision = "HARDEN_AS_PRIMARY_RESEARCH"
    confidence = (
        "LOW"
        if blocking_issues
        else "MEDIUM"
        if warnings or warning_sensitivity.get("data_quality_decision") != "ACCEPT_FOR_RESEARCH"
        else "HIGH"
    )
    reasons = [
        f"candidate_method={candidate}",
        f"selection_recommended_method={recommended}",
        f"risk_return_status={risk_status}",
        f"rolling_consistency_status={rolling_status}",
        f"regime_consistency_status={regime_status}",
        f"stability_status={stability_status}",
        f"data_quality_decision={warning_sensitivity.get('data_quality_decision')}",
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "hardening_id": "",
        "candidate_method": candidate,
        "current_status": _text(limited_row.get("selection_status"), "observed_method"),
        "hardening_decision": hardening_decision,
        "decision_confidence": confidence,
        "reasons": reasons,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "research_target_only": True,
        "not_official_target_weights": True,
        "broker_action_allowed": False,
        "production_effect": "none",
        "requires_forward_confirmation": True,
        "next_action": (
            "owner_review_required"
            if hardening_decision == "REVIEW_REQUIRED"
            else "continue_paper_shadow_observation"
        ),
        **SYSTEM_TARGET_SAFETY,
    }


def _mean_float(values: Sequence[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _rank_score(avg_rank: float, method_count: int) -> float:
    if avg_rank <= 0 or method_count <= 1:
        return 0.0
    return max(0.0, min(1.0, 1.0 - (avg_rank - 1.0) / (method_count - 1.0)))


def _regime_score(regime_summary: Sequence[Mapping[str, Any]], method: str) -> float:
    if not regime_summary:
        return 0.0
    points = 0.0
    total = 0.0
    for row in regime_summary:
        if row.get("sample_count", 0) == 0:
            continue
        total += 3.0
        points += 1.0 if row.get("best_return_method") == method else 0.0
        points += 1.0 if row.get("best_drawdown_method") == method else 0.0
        points += 1.0 if row.get("best_risk_adjusted_method") == method else 0.0
    return points / total if total > 0 else 0.0


def _stability_status_score(status: str) -> float:
    return {
        "STABLE": 1.0,
        "MODERATE": 0.65,
        "MIXED": 0.5,
        "UNSTABLE": 0.15,
        "INSUFFICIENT_DATA": 0.0,
    }.get(status, 0.0)


def _annualized_return(total_return: float, periods: int) -> float:
    if periods <= 0:
        return 0.0
    if total_return <= -1.0:
        return -1.0
    return float((1.0 + total_return) ** (252.0 / periods) - 1.0)


def _stddev(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))


def _best_metric_method(
    rows: Sequence[Mapping[str, Any]],
    field: str,
    *,
    high: bool,
) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    selected = (
        max(rows, key=lambda row: _float(row.get(field)))
        if high
        else min(
            rows,
            key=lambda row: _float(row.get(field)),
        )
    )
    return _text(selected.get("target_method"), "INSUFFICIENT_DATA")


def _best_rank_stability_method(rows: Sequence[Mapping[str, Any]]) -> str:
    valid = [row for row in rows if row.get("rank_stability_status") != "INSUFFICIENT_DATA"]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        min(
            valid,
            key=lambda row: (
                (
                    _float(row.get("avg_rank_return"))
                    + _float(row.get("avg_rank_drawdown"))
                    + _float(row.get("avg_rank_risk_adjusted"))
                )
                / 3.0
            ),
        ).get("target_method")
    )


def _best_status_method(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    order = {"STABLE": 0, "LOW": 0, "MODERATE": 1, "MIXED": 2, "HIGH": 2, "UNSTABLE": 3}
    valid = [row for row in rows if row.get(field) not in {None, "INSUFFICIENT_DATA"}]
    if not valid:
        return "INSUFFICIENT_DATA"
    return _text(
        min(valid, key=lambda row: order.get(_text(row.get(field)), 9)).get("target_method")
    )


def _max_field_method(rows: Sequence[Mapping[str, Any]], field: str) -> str:
    if not rows:
        return "INSUFFICIENT_DATA"
    return _text(max(rows, key=lambda row: _float(row.get(field))).get("target_method"))


def _config_int(config: Mapping[str, Any], path: Sequence[str], default: int) -> int:
    return int(_config_float(config, path, float(default)))


def _config_float(config: Mapping[str, Any], path: Sequence[str], default: float) -> float:
    node: Any = config
    for key in path:
        node = _mapping(node).get(key)
    return _float(node, default)


def _load_backfill_config_from_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    path_text = _text(manifest.get("config_path"))
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        return {}
    try:
        return load_paper_shadow_backfill_config(path)
    except DynamicV3SystemTargetError:
        return {}


def _resolve_project_path(value: object, default: Path) -> Path:
    if value in {None, ""}:
        return default
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_ROOT / path


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


def _assert_paper_shadow_backfill_config_safe(payload: Mapping[str, Any]) -> None:
    backfill = _mapping(payload.get("backfill"))
    date_range = _mapping(payload.get("date_range"))
    source = _mapping(payload.get("source"))
    if backfill.get("mode") != "BACKTEST_SIMULATION":
        raise DynamicV3SystemTargetError("paper shadow backfill must use BACKTEST_SIMULATION")
    if backfill.get("not_pit_safe") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must disclose not_pit_safe=true")
    if backfill.get("research_target_only") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must be research_target_only")
    if backfill.get("paper_shadow_only") is not True:
        raise DynamicV3SystemTargetError("paper shadow backfill must be paper_shadow_only")
    if _coerce_date(date_range.get("start"), date(1970, 1, 1)) < AI_AFTER_CHATGPT_START:
        raise DynamicV3SystemTargetError("paper shadow backfill start cannot predate 2022-12-01")
    if not source.get("model_target_config") or not source.get("paper_shadow_config"):
        raise DynamicV3SystemTargetError("paper shadow backfill source configs are required")
    unknown = set(_enabled_methods(payload)) - set(TARGET_METHODS)
    if unknown:
        raise DynamicV3SystemTargetError(f"unknown target methods: {sorted(unknown)}")
    if not _safety_config_locked(_mapping(payload.get("safety"))):
        raise DynamicV3SystemTargetError("paper shadow backfill safety fields are unsafe")


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
