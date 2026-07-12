from __future__ import annotations

import json
import math
import tempfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any

import pandas as pd

from ai_trading_system.config import (
    configured_price_tickers,
    configured_rate_series,
    load_data_quality,
    load_universe,
)
from ai_trading_system.data.quality import validate_data_cache, write_data_quality_report
from ai_trading_system.etf_portfolio.data import load_standard_prices
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_RATES_CACHE_PATH,
    _apply_weight_deltas,
    _artifact_dir_from_latest,
    _available_price_dates,
    _avg,
    _check,
    _download_manifest_path,
    _float,
    _int,
    _mapping,
    _marketstack_prices_path,
    _nth_trading_date_after,
    _portfolio_metrics,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _requires_marketstack_prices,
    _stable_id,
    _text,
    _texts,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
    _weight_deltas,
    _write_json,
    _write_jsonl,
    _write_text,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)
from ai_trading_system.etf_portfolio.models import load_etf_config_bundle
from ai_trading_system.yaml_loader import safe_load_yaml_path

DEFAULT_BACKTEST_SIM_CONFIG_PATH = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.parents[2]
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "backtest_simulation_advisory_v1.yaml"
)
DEFAULT_BACKTEST_SIM_EVENT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_events"
DEFAULT_BACKTEST_SIM_VARIANT_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_variants"
DEFAULT_BACKTEST_SIM_OUTCOME_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_outcome"
DEFAULT_BACKTEST_SIM_PAPER_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_paper"
DEFAULT_BACKTEST_SIM_REGIME_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_regime"
DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_sensitivity"
DEFAULT_BACKTEST_SIM_CALIBRATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_calibration"
DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backtest_sim_forward_bridge"
)
DEFAULT_SIM_INTERPRETATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sim_interpretation"
DEFAULT_SIM_RISK_RETURN_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sim_risk_return"
DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "sim_defensive_validation"
DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "advisory_proposal_review"
DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "forward_confirmation_plan"
)

OUTCOME_MODE_BACKTEST_SIMULATION = "BACKTEST_SIMULATION"
PIT_SAFETY_SIMULATION = "SIMULATION_NOT_PIT"
REPORT_LABEL_BACKTEST_SIMULATION = "BACKTEST_SIMULATION_NOT_PIT"
BACKTEST_SIM_EVENT_SNAPSHOT_SCHEMA_VERSION = "backtest_sim_event_input_snapshot.v2"
BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION = "backtest_sim_variant_input_snapshot.v2"
BACKTEST_SIM_OUTCOME_SNAPSHOT_SCHEMA_VERSION = "backtest_sim_outcome_input_snapshot.v2"
BACKTEST_SIM_VARIANTS = (
    "no_trade",
    "consensus_target",
    "limited_adjustment",
    "defensive_limited_adjustment",
    "equal_weight_shadow_candidates",
)
OUTCOME_STATUSES = {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"}
VARIANT_STATUSES = {"READY", "SKIPPED", "INSUFFICIENT_DATA"}
EVENT_STATUSES = {"READY", "SKIPPED", "INSUFFICIENT_DATA"}
REGIME_BUCKETS = {
    "ai_trend",
    "tech_drawdown",
    "semiconductor_pullback",
    "sideways_choppy",
    "risk_off",
    "strong_recovery",
    "unknown",
}
ACTIVE_SIM_VARIANTS = (
    "limited_adjustment",
    "consensus_target",
    "defensive_limited_adjustment",
)
RISK_RETURN_STATUSES = {
    "RETURN_IMPROVES_RISK_IMPROVES",
    "RETURN_IMPROVES_RISK_WORSENS",
    "RETURN_WORSE_RISK_IMPROVES",
    "RETURN_WORSE_RISK_WORSE",
    "INSUFFICIENT_DATA",
}
DEFENSIVE_PRESSURE_REGIMES = {"tech_drawdown", "risk_off", "semiconductor_pullback"}
DEFENSIVE_VALIDATION_STATUSES = {
    "PROVEN_DEFENSIVE",
    "PARTIALLY_DEFENSIVE",
    "NOT_PROVEN_DEFENSIVE",
    "FAILS_DEFENSIVE_EXPECTATION",
    "INSUFFICIENT_DATA",
    "INSUFFICIENT_SAMPLE",
}
PROPOSAL_REVIEW_DECISIONS = {
    "ACCEPT",
    "ACCEPT_FOR_OBSERVATION",
    "REJECT",
    "DEFER",
    "REQUIRES_MORE_DATA",
    "OWNER_REVIEW_REQUIRED",
}


class DynamicV3BacktestSimulationError(ValueError):
    """Raised when backtest simulation artifacts fail closed."""


def load_backtest_simulation_config(
    path: Path = DEFAULT_BACKTEST_SIM_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(path)
    if not isinstance(payload, dict):
        raise DynamicV3BacktestSimulationError(f"invalid backtest simulation config: {path}")
    return payload


def _finite_number(value: Any, *, minimum: float | None = None) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    number = float(value)
    return math.isfinite(number) and (minimum is None or number >= minimum)


def _mapping_values_finite(payload: Mapping[str, Any]) -> bool:
    if not payload:
        return False
    for value in payload.values():
        if isinstance(value, Mapping):
            if not _mapping_values_finite(value):
                return False
        elif isinstance(value, list):
            if not value or not all(
                _finite_number(item) or (isinstance(item, str) and bool(item.strip()))
                for item in value
            ):
                return False
        elif not _finite_number(value) and not (isinstance(value, str) and bool(value.strip())):
            return False
    return True


def validate_backtest_simulation_config(
    *, config_path: Path = DEFAULT_BACKTEST_SIM_CONFIG_PATH
) -> dict[str, Any]:
    config = load_backtest_simulation_config(config_path)
    simulation = _mapping(config.get("simulation"))
    source = _mapping(config.get("source"))
    portfolio = _mapping(config.get("portfolio"))
    variants = _texts(_mapping(config.get("variants")).get("enabled"))
    safety = _mapping(config.get("safety"))
    date_range = _mapping(config.get("date_range"))
    limits = _mapping(config.get("limits"))
    windows = _records_as_ints(_mapping(config.get("outcome_windows")).get("trading_days"))
    baseline = _normalize_weights(_mapping(portfolio.get("baseline_snapshot")))
    raw_baseline = _mapping(portfolio.get("baseline_snapshot"))
    position_path_text = _text(source.get("position_advisory_config"))
    price_path_text = _text(source.get("price_cache_path"))
    rates_path_text = _text(source.get("rates_cache_path"))
    policy_metadata = _mapping(config.get("policy_metadata"))
    sensitivity = _mapping(config.get("sensitivity_policy"))
    adjustment_grid = (
        sensitivity.get("adjustment_limit_grid")
        if isinstance(sensitivity.get("adjustment_limit_grid"), list)
        else []
    )
    shortlist_grid = (
        sensitivity.get("shortlist_top_n_grid")
        if isinstance(sensitivity.get("shortlist_top_n_grid"), list)
        else []
    )
    event_frequency = _text(date_range.get("event_frequency"))
    event_day = _text(date_range.get("event_day"))
    shortlist_path = _shadow_shortlist_path(config)
    shortlist_rows = _read_jsonl(shortlist_path) if shortlist_path.is_file() else []
    shortlist_candidate_ids = [_text(row.get("candidate_id")) for row in shortlist_rows]
    checks = [
        _check("schema_version", config.get("schema_version") == SCHEMA_VERSION, "schema=1"),
        _check(
            "policy_governance_complete",
            all(
                _text(policy_metadata.get(key))
                for key in (
                    "policy_id",
                    "owner",
                    "version",
                    "status",
                    "rationale",
                    "intended_effect",
                    "validation_evidence",
                    "review_condition",
                )
            ),
            "policy metadata",
        ),
        _check(
            "outcome_mode_backtest_simulation",
            simulation.get("outcome_mode") == OUTCOME_MODE_BACKTEST_SIMULATION,
            _text(simulation.get("outcome_mode")),
        ),
        _check(
            "pit_safety_simulation_not_pit",
            simulation.get("pit_safety_status") == PIT_SAFETY_SIMULATION,
            _text(simulation.get("pit_safety_status")),
        ),
        _check(
            "not_for_production",
            simulation.get("not_for_production") is True,
            "not_for_production=true",
        ),
        _check(
            "ai_after_chatgpt_start",
            (_date_from_any(date_range.get("start")) or date.min) >= date(2022, 12, 1),
            _text(date_range.get("start")),
        ),
        _check(
            "date_order",
            (_date_from_any(date_range.get("start")) or date.max)
            <= (_date_from_any(date_range.get("end")) or date.min),
            "start <= end",
        ),
        _check(
            "event_schedule_supported",
            event_frequency in {"weekly", "biweekly", "monthly"}
            and event_day in {"MON", "TUE", "WED", "THU", "FRI"}
            and _int(date_range.get("min_history_days_before_event")) >= 1,
            f"{event_frequency}/{event_day}",
        ),
        _check(
            "baseline_weights_sum",
            bool(raw_baseline)
            and len(baseline) == len(raw_baseline)
            and all(math.isfinite(value) and value >= 0 for value in baseline.values())
            and abs(sum(baseline.values()) - 1.0) <= 0.000001,
            json.dumps(baseline, sort_keys=True),
        ),
        _check(
            "variants_supported",
            bool(variants)
            and len(variants) == len(set(variants))
            and set(variants) <= set(BACKTEST_SIM_VARIANTS),
            ",".join(variants),
        ),
        _check(
            "outcome_windows_present",
            bool(windows)
            and len(windows) == len(set(windows))
            and all(item > 0 for item in windows),
            "",
        ),
        _check(
            "adjustment_limits_governed",
            _finite_number(limits.get("max_single_event_total_adjustment"), minimum=0.0)
            and _finite_number(limits.get("max_single_symbol_adjustment"), minimum=0.0)
            and _finite_number(limits.get("min_trade_threshold"), minimum=0.0)
            and 0 < _float(limits.get("max_single_event_total_adjustment")) <= 1
            and 0 < _float(limits.get("max_single_symbol_adjustment")) <= 1
            and _float(limits.get("min_trade_threshold"))
            <= _float(limits.get("max_single_symbol_adjustment")),
            "limits",
        ),
        _check(
            "investment_thresholds_finite",
            all(
                _mapping_values_finite(_mapping(config.get(section)))
                for section in (
                    "consensus_policy",
                    "regime_policy",
                    "forward_confirmation",
                )
            ),
            "policy thresholds",
        ),
        _check(
            "sensitivity_policy_valid",
            bool(_texts(sensitivity.get("event_frequency_profiles")))
            and len(_texts(sensitivity.get("event_frequency_profiles")))
            == len(set(_texts(sensitivity.get("event_frequency_profiles"))))
            and set(_texts(sensitivity.get("event_frequency_profiles"))).issubset(
                {"weekly", "biweekly", "monthly"}
            )
            and bool(adjustment_grid)
            and all(_finite_number(item, minimum=0.0) for item in adjustment_grid)
            and bool(shortlist_grid)
            and all(
                isinstance(item, int) and not isinstance(item, bool) and item >= 1
                for item in shortlist_grid
            )
            and _mapping_values_finite(_mapping(sensitivity.get("consensus_dispersion_thresholds")))
            and all(
                _finite_number(sensitivity.get(field), minimum=0.0)
                for field in (
                    "min_available_windows_for_low_risk",
                    "max_regime_return_concentration_low_risk",
                    "max_parameter_result_spread_low_risk",
                    "high_risk_regime_return_concentration",
                    "high_risk_parameter_result_spread",
                )
            ),
            "sensitivity policy",
        ),
        _check(
            "shadow_shortlist_source_valid",
            bool(_text(source.get("shadow_shortlist_id")))
            and shortlist_path.is_file()
            and bool(shortlist_rows)
            and all(shortlist_candidate_ids)
            and len(shortlist_candidate_ids) == len(set(shortlist_candidate_ids))
            and all(_text(row.get("real_evaluation_artifact_path")) for row in shortlist_rows),
            str(shortlist_path),
        ),
        _check(
            "position_advisory_config_exists",
            bool(position_path_text) and _resolve_project_path(Path(position_path_text)).is_file(),
            position_path_text,
        ),
        _check(
            "price_cache_exists",
            bool(price_path_text) and _resolve_project_path(Path(price_path_text)).is_file(),
            price_path_text,
        ),
        _check(
            "rates_cache_exists",
            bool(rates_path_text) and _resolve_project_path(Path(rates_path_text)).is_file(),
            rates_path_text,
        ),
        _check(
            "safety_no_broker",
            safety.get("broker_action_allowed") is False
            and safety.get("broker_action_taken") is False,
            "broker flags false",
        ),
        _check(
            "safety_no_auto_policy",
            safety.get("auto_policy_apply") is False and safety.get("production_effect") == "none",
            "policy flags safe",
        ),
        _check(
            "report_label",
            safety.get("require_report_label") == REPORT_LABEL_BACKTEST_SIMULATION,
            _text(safety.get("require_report_label")),
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_config_validation",
        artifact_id_key="config_path",
        artifact_id=str(config_path),
        checks=checks,
    )


def generate_backtest_sim_events(
    *,
    config_path: Path = DEFAULT_BACKTEST_SIM_CONFIG_PATH,
    output_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3BacktestSimulationError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    config = load_backtest_simulation_config(config_path)
    validation = validate_backtest_simulation_config(config_path=config_path)
    if validation["status"] != "PASS":
        raise DynamicV3BacktestSimulationError("backtest simulation config validation failed")
    source = _mapping(config.get("source"))
    date_range = _mapping(config.get("date_range"))
    start = _date_from_any(date_range.get("start")) or date(2022, 12, 1)
    end = _date_from_any(date_range.get("end")) or generated.date()
    if end > generated.date():
        raise DynamicV3BacktestSimulationError("simulation end exceeds generated cutoff")
    prices_path = _resolve_project_path(Path(_text(source.get("price_cache_path"))))
    rates_text = _text(source.get("rates_cache_path"))
    rates_path = _resolve_project_path(Path(rates_text)) if rates_text else DEFAULT_RATES_CACHE_PATH
    event_set_id = _stable_id(
        "backtest-sim-events",
        config_path,
        start.isoformat(),
        end.isoformat(),
        generated.isoformat(),
    )
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    quality_report_text = ""
    if enforce_data_quality_gate:
        with tempfile.TemporaryDirectory(prefix="backtest-sim-event-dq-") as temp_dir:
            quality_report = Path(temp_dir) / "validate_data_quality_report.md"
            quality = _run_cached_quality_gate(
                as_of=end,
                prices_path=prices_path,
                rates_path=rates_path,
                report_path=quality_report,
            )
            if quality_report.exists():
                quality_report_text = quality_report.read_text(encoding="utf-8")
        quality_status = quality.status
        if not quality.passed:
            raise DynamicV3BacktestSimulationError(
                f"backtest simulation event data quality gate failed: {quality.status}"
            )
    prices = _load_prices(prices_path, extra_symbols=_portfolio_symbols(config))
    price_dates = _available_price_dates(prices)
    event_dates = _scheduled_event_dates(price_dates, start=start, end=end, config=config)
    candidates = _load_shadow_candidate_sources(config)
    shortlist_path = _shadow_shortlist_path(config)
    position_path = _resolve_project_path(Path(_text(source.get("position_advisory_config"))))
    baseline = _normalize_weights(
        _mapping(_mapping(config.get("portfolio")).get("baseline_snapshot"))
    )
    min_history = _int(date_range.get("min_history_days_before_event"), 0)
    events = []
    for event_date in event_dates:
        events.append(
            _sim_event(
                event_date=event_date,
                config=config,
                candidates=candidates,
                baseline=baseline,
                prices=prices,
                price_dates=price_dates,
                min_history=min_history,
            )
        )
    counts = Counter(_text(row.get("event_status")) for row in events)
    input_snapshot = {
        "schema_version": BACKTEST_SIM_EVENT_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_input_snapshot",
        "config_path": str(config_path),
        "config_checksum": sha256(config_path.read_bytes()).hexdigest(),
        "config": config,
        "generated_at": generated.isoformat(),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "event_dates": [item.isoformat() for item in event_dates],
        "candidate_count": len(candidates),
        "shortlist_source": _text_source_snapshot(shortlist_path),
        "position_advisory_source": _text_source_snapshot(position_path),
        "candidate_sources": _candidate_source_snapshot(candidates),
        "price_source": _price_source_snapshot(prices_path),
        "rate_source": _price_source_snapshot(rates_path),
        "price_cutoff_snapshot": _csv_cutoff_snapshot(prices_path, end=end),
        "rate_cutoff_snapshot": _csv_cutoff_snapshot(rates_path, end=end),
        "data_quality_status": quality_status,
        "data_quality_enforced": enforce_data_quality_gate,
        "data_quality_report_content": quality_report_text,
        "data_quality_report_checksum": sha256(quality_report_text.encode("utf-8")).hexdigest()
        if quality_report_text
        else "",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
    }
    status = "PASS" if counts.get("READY", 0) else "INSUFFICIENT_DATA"
    if counts.get("INSUFFICIENT_DATA", 0) and counts.get("READY", 0):
        status = "PASS_WITH_WARNINGS"
    event_dir = _unique_dir(output_dir / event_set_id)
    event_dir.mkdir(parents=True, exist_ok=False)
    event_set_dir = event_dir
    if enforce_data_quality_gate:
        quality_report = event_dir / "validate_data_quality_report.md"
        _write_text(quality_report, quality_report_text)
        quality_report_path = str(quality_report)
    manifest = _backtest_sim_event_manifest(
        event_set_dir=event_set_dir,
        generated=generated,
        status=status,
        config=config,
        config_path=config_path,
        start=start,
        end=end,
        events=events,
        quality_status=quality_status,
        quality_report_path=quality_report_path,
    )
    _write_json(event_set_dir / "backtest_sim_event_manifest.json", manifest)
    _write_jsonl(event_set_dir / "simulated_advisory_events.jsonl", events)
    _write_json(event_set_dir / "simulation_input_snapshot.json", input_snapshot)
    _write_text(
        event_set_dir / "event_generation_report.md",
        render_event_generation_report(manifest, events),
    )
    _update_latest_pointer(
        "latest_backtest_sim_events",
        event_set_dir.name,
        event_set_dir / "backtest_sim_event_manifest.json",
    )
    return {
        "event_set_id": event_set_dir.name,
        "event_set_dir": event_set_dir,
        "manifest": manifest,
        "events": events,
        "input_snapshot": input_snapshot,
    }


def _backtest_sim_event_manifest(
    *,
    event_set_dir: Path,
    generated: datetime,
    status: str,
    config: Mapping[str, Any],
    config_path: Path,
    start: date,
    end: date,
    events: Sequence[Mapping[str, Any]],
    quality_status: str,
    quality_report_path: str,
) -> dict[str, Any]:
    source = _mapping(config.get("source"))
    counts = Counter(_text(row.get("event_status")) for row in events)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_event_manifest",
        "event_set_id": event_set_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "market_regime": _text(_mapping(config.get("simulation")).get("market_regime")),
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "event_count": len(events),
        "ready_count": counts.get("READY", 0),
        "skipped_count": counts.get("SKIPPED", 0),
        "insufficient_data_count": counts.get("INSUFFICIENT_DATA", 0),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "source_shadow_shortlist_id": _text(source.get("shadow_shortlist_id")),
        "config_path": str(config_path),
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "backtest_sim_event_manifest_path": str(event_set_dir / "backtest_sim_event_manifest.json"),
        "simulated_advisory_events_path": str(event_set_dir / "simulated_advisory_events.jsonl"),
        "simulation_input_snapshot_path": str(event_set_dir / "simulation_input_snapshot.json"),
        "event_generation_report_path": str(event_set_dir / "event_generation_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def backtest_sim_event_report_payload(
    *,
    event_set_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
) -> dict[str, Any]:
    event_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=event_set_id if not latest else None,
        pointer_name="latest_backtest_sim_events",
    )
    return {
        **_read_json(event_dir / "backtest_sim_event_manifest.json"),
        "simulated_advisory_events": _read_jsonl(event_dir / "simulated_advisory_events.jsonl"),
        "simulation_input_snapshot": _read_json(event_dir / "simulation_input_snapshot.json"),
        "event_set_dir": str(event_dir),
    }


def validate_backtest_sim_events_artifact(
    *, event_set_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR
) -> dict[str, Any]:
    event_dir = output_dir / event_set_id
    manifest = _read_optional_json(event_dir / "backtest_sim_event_manifest.json") or {}
    rows = _read_jsonl(event_dir / "simulated_advisory_events.jsonl")
    snapshot = _read_optional_json(event_dir / "simulation_input_snapshot.json") or {}
    recompute_error = ""
    source_errors: list[str] = []
    try:
        config_path = Path(_text(snapshot.get("config_path")))
        config = _mapping(snapshot.get("config"))
        generated = _datetime_from_any(snapshot.get("generated_at"))
        start = _date_from_any(snapshot.get("requested_start"))
        end = _date_from_any(snapshot.get("requested_end"))
        if generated is None or start is None or end is None:
            raise DynamicV3BacktestSimulationError("event snapshot time invalid")
        if snapshot.get("schema_version") != BACKTEST_SIM_EVENT_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("snapshot_schema_invalid")
        if (
            not config_path.is_file()
            or sha256(config_path.read_bytes()).hexdigest() != snapshot.get("config_checksum")
            or load_backtest_simulation_config(config_path) != config
            or validate_backtest_simulation_config(config_path=config_path).get("status") != "PASS"
        ):
            source_errors.append("config_changed")
        source = _mapping(config.get("source"))
        shortlist_path = _shadow_shortlist_path(config)
        position_path = _resolve_project_path(Path(_text(source.get("position_advisory_config"))))
        if _text_source_snapshot(shortlist_path) != snapshot.get("shortlist_source"):
            source_errors.append("shortlist_source_changed")
        if _text_source_snapshot(position_path) != snapshot.get("position_advisory_source"):
            source_errors.append("position_advisory_source_changed")
        prices_path = _resolve_project_path(Path(_text(source.get("price_cache_path"))))
        rates_path = _resolve_project_path(Path(_text(source.get("rates_cache_path"))))
        if _price_source_snapshot(prices_path) != snapshot.get("price_source"):
            source_errors.append("price_source_changed")
        if _price_source_snapshot(rates_path) != snapshot.get("rate_source"):
            source_errors.append("rate_source_changed")
        if _csv_cutoff_snapshot(prices_path, end=end) != snapshot.get("price_cutoff_snapshot"):
            source_errors.append("price_cutoff_rows_changed")
        if _csv_cutoff_snapshot(rates_path, end=end) != snapshot.get("rate_cutoff_snapshot"):
            source_errors.append("rate_cutoff_rows_changed")
        candidates = _load_shadow_candidate_sources(config)
        if _candidate_source_snapshot(candidates) != snapshot.get("candidate_sources"):
            source_errors.append("candidate_sources_changed")
        quality_status = _text(snapshot.get("data_quality_status"))
        frozen_quality_content = _text(snapshot.get("data_quality_report_content"))
        frozen_quality_checksum = _text(snapshot.get("data_quality_report_checksum"))
        if bool(frozen_quality_content) != bool(snapshot.get("data_quality_enforced")):
            source_errors.append("data_quality_evidence_missing")
        if (
            frozen_quality_content
            and sha256(frozen_quality_content.encode("utf-8")).hexdigest()
            != frozen_quality_checksum
        ):
            source_errors.append("data_quality_evidence_checksum_invalid")
        live_quality_path = Path(_text(manifest.get("data_quality_report_path")))
        if frozen_quality_content and (
            not live_quality_path.is_file()
            or live_quality_path.read_text(encoding="utf-8") != frozen_quality_content
        ):
            source_errors.append("data_quality_report_changed")
        if snapshot.get("data_quality_enforced") is True:
            with tempfile.TemporaryDirectory(prefix="backtest-sim-event-validate-dq-") as temp_dir:
                quality = _run_cached_quality_gate(
                    as_of=end,
                    prices_path=prices_path,
                    rates_path=rates_path,
                    report_path=Path(temp_dir) / "validate_data_quality_report.md",
                )
            if not quality.passed or quality.status != quality_status:
                source_errors.append("data_quality_changed")
        prices = _load_prices(prices_path, extra_symbols=_portfolio_symbols(config))
        price_dates = _available_price_dates(prices)
        event_dates = _scheduled_event_dates(price_dates, start=start, end=end, config=config)
        if [item.isoformat() for item in event_dates] != snapshot.get("event_dates"):
            source_errors.append("event_schedule_changed")
        baseline = _normalize_weights(
            _mapping(_mapping(config.get("portfolio")).get("baseline_snapshot"))
        )
        min_history = _int(_mapping(config.get("date_range")).get("min_history_days_before_event"))
        expected_rows = [
            _sim_event(
                event_date=event_date,
                config=config,
                candidates=candidates,
                baseline=baseline,
                prices=prices,
                price_dates=price_dates,
                min_history=min_history,
            )
            for event_date in event_dates
        ]
        counts = Counter(_text(row.get("event_status")) for row in expected_rows)
        expected_status = "PASS" if counts.get("READY", 0) else "INSUFFICIENT_DATA"
        if counts.get("INSUFFICIENT_DATA", 0) and counts.get("READY", 0):
            expected_status = "PASS_WITH_WARNINGS"
        expected_manifest = _backtest_sim_event_manifest(
            event_set_dir=event_dir,
            generated=generated,
            status=expected_status,
            config=config,
            config_path=config_path,
            start=start,
            end=end,
            events=expected_rows,
            quality_status=quality_status,
            quality_report_path=_text(manifest.get("data_quality_report_path")),
        )
        expected_report = render_event_generation_report(expected_manifest, expected_rows)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_rows = []
        expected_manifest = {}
        expected_report = ""
    report_path = event_dir / "event_generation_report.md"
    checks = [
        _check("manifest_exists", (event_dir / "backtest_sim_event_manifest.json").exists(), ""),
        _check("events_exists", (event_dir / "simulated_advisory_events.jsonl").exists(), ""),
        _check(
            "input_snapshot_exists", (event_dir / "simulation_input_snapshot.json").exists(), ""
        ),
        _check("report_exists", (event_dir / "event_generation_report.md").exists(), ""),
        _check("event_set_id_matches", manifest.get("event_set_id") == event_set_id, event_set_id),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("events_recomputed", rows == expected_rows, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "report_recomputed",
            report_path.exists() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown",
        ),
        _check(
            "event_status_valid",
            all(row.get("event_status") in EVENT_STATUSES for row in rows),
            "event statuses",
        ),
        _check(
            "all_backtest_simulation",
            all(row.get("outcome_mode") == OUTCOME_MODE_BACKTEST_SIMULATION for row in rows),
            OUTCOME_MODE_BACKTEST_SIMULATION,
        ),
        _check(
            "all_simulation_not_pit",
            all(row.get("pit_safety_status") == PIT_SAFETY_SIMULATION for row in rows),
            PIT_SAFETY_SIMULATION,
        ),
        _check(
            "not_for_production",
            manifest.get("not_for_production") is True
            and all(row.get("not_for_production") is True for row in rows),
            "not for production",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False
            and all(row.get("broker_action_allowed") is False for row in rows)
            and all(row.get("broker_action_taken") is False for row in rows),
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_events_validation",
        artifact_id_key="event_set_id",
        artifact_id=event_set_id,
        checks=checks,
    )


def generate_backtest_sim_variants(
    *,
    event_set_id: str,
    event_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3BacktestSimulationError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    source_dir = event_dir / event_set_id
    event_validation = validate_backtest_sim_events_artifact(
        event_set_id=event_set_id, output_dir=event_dir
    )
    if event_validation.get("status") != "PASS":
        raise DynamicV3BacktestSimulationError(
            "backtest simulation event artifact validation failed"
        )
    event_bundle = _backtest_sim_event_bundle(source_dir)
    event_manifest = _mapping(event_bundle.get("manifest"))
    event_generated = _datetime_from_any(event_manifest.get("generated_at"))
    if event_generated is None or event_generated > generated:
        raise DynamicV3BacktestSimulationError("event source generated after variant cutoff")
    events = _records(event_bundle.get("events"))
    event_snapshot = _mapping(event_bundle.get("input_snapshot"))
    config = _mapping(event_snapshot.get("config"))
    enabled = _texts(_mapping(config.get("variants")).get("enabled"))
    rows, ledger = _variant_rows(events=events, config=config, enabled=enabled)
    counts = Counter(_text(row.get("variant_status")) for row in rows)
    status = "PASS" if counts.get("READY") else "INSUFFICIENT_DATA"
    if counts.get("INSUFFICIENT_DATA") and counts.get("READY"):
        status = "PASS_WITH_WARNINGS"
    variant_set_id = _stable_id("backtest-sim-variants", event_set_id, generated.isoformat())
    variant_dir = _unique_dir(output_dir / variant_set_id)
    input_snapshot = {
        "schema_version": BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_variant_input_snapshot",
        "generated_at": generated.isoformat(),
        "event_set_id": event_set_id,
        "event_set_dir": str(source_dir),
        "event_bundle": event_bundle,
        "event_validation": event_validation,
        "config": config,
        "enabled_variants": enabled,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
    }
    manifest = _backtest_sim_variant_manifest(
        variant_dir=variant_dir,
        generated=generated,
        status=status,
        event_set_id=event_set_id,
        event_manifest=event_manifest,
        rows=rows,
    )
    variant_dir.mkdir(parents=True, exist_ok=False)
    _write_json(variant_dir / "variant_set_manifest.json", manifest)
    _write_jsonl(variant_dir / "simulated_variant_weights.jsonl", rows)
    _write_jsonl(variant_dir / "variant_action_ledger.jsonl", ledger)
    _write_json(variant_dir / "variant_input_snapshot.json", input_snapshot)
    _write_text(variant_dir / "variant_generation_report.md", render_variant_report(manifest, rows))
    _update_latest_pointer(
        "latest_backtest_sim_variants",
        variant_dir.name,
        variant_dir / "variant_set_manifest.json",
    )
    return {
        "variant_set_id": variant_dir.name,
        "variant_set_dir": variant_dir,
        "manifest": manifest,
        "variant_rows": rows,
        "variant_action_ledger": ledger,
        "input_snapshot": input_snapshot,
    }


def _backtest_sim_variant_manifest(
    *,
    variant_dir: Path,
    generated: datetime,
    status: str,
    event_set_id: str,
    event_manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    counts = Counter(_text(row.get("variant_status")) for row in rows)
    variants_generated = sorted({_text(row.get("variant")) for row in rows})
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_variant_set_manifest",
        "variant_set_id": variant_dir.name,
        "event_set_id": event_set_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "variants_generated": variants_generated,
        "ready_count": counts.get("READY", 0),
        "insufficient_data_count": counts.get("INSUFFICIENT_DATA", 0),
        "skipped_count": counts.get("SKIPPED", 0),
        "source_event_manifest_path": event_manifest.get("backtest_sim_event_manifest_path"),
        "variant_set_manifest_path": str(variant_dir / "variant_set_manifest.json"),
        "simulated_variant_weights_path": str(variant_dir / "simulated_variant_weights.jsonl"),
        "variant_action_ledger_path": str(variant_dir / "variant_action_ledger.jsonl"),
        "variant_input_snapshot_path": str(variant_dir / "variant_input_snapshot.json"),
        "variant_generation_report_path": str(variant_dir / "variant_generation_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def backtest_sim_variant_report_payload(
    *,
    variant_set_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
) -> dict[str, Any]:
    variant_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=variant_set_id if not latest else None,
        pointer_name="latest_backtest_sim_variants",
    )
    return {
        **_read_json(variant_dir / "variant_set_manifest.json"),
        "simulated_variant_weights": _read_jsonl(variant_dir / "simulated_variant_weights.jsonl"),
        "variant_action_ledger": _read_jsonl(variant_dir / "variant_action_ledger.jsonl"),
        "variant_input_snapshot": _read_json(variant_dir / "variant_input_snapshot.json"),
        "variant_set_dir": str(variant_dir),
    }


def validate_backtest_sim_variants_artifact(
    *, variant_set_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR
) -> dict[str, Any]:
    variant_dir = output_dir / variant_set_id
    manifest = _read_optional_json(variant_dir / "variant_set_manifest.json") or {}
    rows = _read_jsonl(variant_dir / "simulated_variant_weights.jsonl")
    ledger = _read_jsonl(variant_dir / "variant_action_ledger.jsonl")
    snapshot = _read_optional_json(variant_dir / "variant_input_snapshot.json") or {}
    source_errors: list[str] = []
    recompute_error = ""
    try:
        if snapshot.get("schema_version") != BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("snapshot_schema_invalid")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        event_set_id = _text(snapshot.get("event_set_id"))
        event_set_dir = Path(_text(snapshot.get("event_set_dir")))
        if generated is None or not event_set_id or event_set_dir.name != event_set_id:
            raise DynamicV3BacktestSimulationError("variant source identity/time invalid")
        live_validation = validate_backtest_sim_events_artifact(
            event_set_id=event_set_id,
            output_dir=event_set_dir.parent,
        )
        if live_validation.get("status") != "PASS":
            source_errors.append("event_validation_failed")
        if live_validation != snapshot.get("event_validation"):
            source_errors.append("event_validation_changed")
        live_bundle = _backtest_sim_event_bundle(event_set_dir)
        if live_bundle != snapshot.get("event_bundle"):
            source_errors.append("event_bundle_changed")
        frozen_bundle = _mapping(snapshot.get("event_bundle"))
        event_manifest = _mapping(frozen_bundle.get("manifest"))
        events = _records(frozen_bundle.get("events"))
        event_snapshot = _mapping(frozen_bundle.get("input_snapshot"))
        config = _mapping(snapshot.get("config"))
        if config != _mapping(event_snapshot.get("config")):
            source_errors.append("config_binding_changed")
        enabled = _texts(snapshot.get("enabled_variants"))
        if enabled != _texts(_mapping(config.get("variants")).get("enabled")):
            source_errors.append("enabled_variants_changed")
        expected_snapshot = {
            "schema_version": BACKTEST_SIM_VARIANT_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_backtest_sim_variant_input_snapshot",
            "generated_at": generated.isoformat(),
            "event_set_id": event_set_id,
            "event_set_dir": str(event_set_dir),
            "event_bundle": live_bundle,
            "event_validation": live_validation,
            "config": config,
            "enabled_variants": enabled,
            "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
            "pit_safety_status": PIT_SAFETY_SIMULATION,
            "not_for_production": True,
        }
        expected_rows, expected_ledger = _variant_rows(
            events=events, config=config, enabled=enabled
        )
        counts = Counter(_text(row.get("variant_status")) for row in expected_rows)
        expected_status = "PASS" if counts.get("READY") else "INSUFFICIENT_DATA"
        if counts.get("INSUFFICIENT_DATA") and counts.get("READY"):
            expected_status = "PASS_WITH_WARNINGS"
        expected_manifest = _backtest_sim_variant_manifest(
            variant_dir=variant_dir,
            generated=generated,
            status=expected_status,
            event_set_id=event_set_id,
            event_manifest=event_manifest,
            rows=expected_rows,
        )
        expected_report = render_variant_report(expected_manifest, expected_rows)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_rows = []
        expected_ledger = []
        expected_manifest = {}
        expected_snapshot = {}
        expected_report = ""
    report_path = variant_dir / "variant_generation_report.md"
    checks = [
        _check("manifest_exists", (variant_dir / "variant_set_manifest.json").exists(), ""),
        _check("weights_exists", (variant_dir / "simulated_variant_weights.jsonl").exists(), ""),
        _check("ledger_exists", (variant_dir / "variant_action_ledger.jsonl").exists(), ""),
        _check("snapshot_exists", (variant_dir / "variant_input_snapshot.json").exists(), ""),
        _check("report_exists", (variant_dir / "variant_generation_report.md").exists(), ""),
        _check("variant_set_id_matches", manifest.get("variant_set_id") == variant_set_id, ""),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "live source"),
        _check("rows_recomputed", rows == expected_rows, "snapshot"),
        _check("ledger_recomputed", ledger == expected_ledger, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "snapshot_bytes_recomputed",
            (variant_dir / "variant_input_snapshot.json").read_text(encoding="utf-8")
            == _canonical_json_text(expected_snapshot),
            "canonical JSON",
        ),
        _check(
            "rows_bytes_recomputed",
            (variant_dir / "simulated_variant_weights.jsonl").read_text(encoding="utf-8")
            == _canonical_jsonl_text(expected_rows),
            "canonical JSONL",
        ),
        _check(
            "ledger_bytes_recomputed",
            (variant_dir / "variant_action_ledger.jsonl").read_text(encoding="utf-8")
            == _canonical_jsonl_text(expected_ledger),
            "canonical JSONL",
        ),
        _check(
            "manifest_bytes_recomputed",
            (variant_dir / "variant_set_manifest.json").read_text(encoding="utf-8")
            == _canonical_json_text(expected_manifest),
            "canonical JSON",
        ),
        _check(
            "report_recomputed",
            report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown",
        ),
        _check(
            "variant_status_valid",
            all(row.get("variant_status") in VARIANT_STATUSES for row in rows),
            "variant statuses",
        ),
        _check("variant_contract_valid", _variant_rows_contract_valid(rows), "rows"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_taken") is False
            and all(row.get("broker_action_taken") is False for row in rows),
            "broker action forbidden",
        ),
        _check(
            "not_for_production",
            manifest.get("not_for_production") is True
            and all(row.get("not_for_production") is True for row in rows),
            "not for production",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_variants_validation",
        artifact_id_key="variant_set_id",
        artifact_id=variant_set_id,
        checks=checks,
    )


def _backtest_sim_event_bundle(event_dir: Path) -> dict[str, Any]:
    quality_path = event_dir / "validate_data_quality_report.md"
    manifest_path = event_dir / "backtest_sim_event_manifest.json"
    events_path = event_dir / "simulated_advisory_events.jsonl"
    snapshot_path = event_dir / "simulation_input_snapshot.json"
    report_path = event_dir / "event_generation_report.md"
    return {
        "event_set_dir": str(event_dir),
        "manifest": _read_json(manifest_path),
        "events": _read_jsonl(events_path),
        "input_snapshot": _read_json(snapshot_path),
        "event_generation_report": _read_text(report_path),
        "data_quality_report": _read_text(quality_path) if quality_path.is_file() else "",
        "file_contents": {
            "backtest_sim_event_manifest.json": _read_text(manifest_path),
            "simulated_advisory_events.jsonl": _read_text(events_path),
            "simulation_input_snapshot.json": _read_text(snapshot_path),
            "event_generation_report.md": _read_text(report_path),
            "validate_data_quality_report.md": (
                _read_text(quality_path) if quality_path.is_file() else ""
            ),
        },
    }


def _canonical_json_text(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def _canonical_jsonl_text(rows: Sequence[Mapping[str, Any]]) -> str:
    return "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)


def _variant_rows_contract_valid(rows: Sequence[Mapping[str, Any]]) -> bool:
    identities: set[tuple[str, str]] = set()
    previous_by_variant: dict[str, Mapping[str, Any]] = {}
    for row in sorted(
        rows, key=lambda item: (_text(item.get("as_of")), _text(item.get("variant")))
    ):
        identity = (_text(row.get("sim_event_id")), _text(row.get("variant")))
        if not all(identity) or identity in identities:
            return False
        identities.add(identity)
        before = _mapping(row.get("before_weights"))
        after = _mapping(row.get("after_weights"))
        deltas = _mapping(row.get("deltas"))
        if not _normalized_finite_weights(before) or not _normalized_finite_weights(after):
            return False
        if _weight_deltas(before, after) != deltas:
            return False
        turnover = sum(abs(_float(value)) for value in deltas.values())
        if not math.isclose(turnover, _float(row.get("turnover")), abs_tol=0.000001):
            return False
        variant = identity[1]
        if variant in previous_by_variant and before != previous_by_variant[variant]:
            return False
        previous_by_variant[variant] = after
    return True


def _normalized_finite_weights(weights: Mapping[str, Any]) -> bool:
    return (
        bool(weights)
        and all(_finite_number(value, minimum=0.0) for value in weights.values())
        and math.isclose(sum(float(value) for value in weights.values()), 1.0, abs_tol=0.000001)
    )


def run_backtest_sim_outcome(
    *,
    variant_set_id: str,
    variant_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    if generated.tzinfo is None or generated.utcoffset() is None:
        raise DynamicV3BacktestSimulationError("generated_at must be timezone-aware")
    generated = generated.astimezone(UTC)
    source_dir = variant_dir / variant_set_id
    variant_validation = validate_backtest_sim_variants_artifact(
        variant_set_id=variant_set_id, output_dir=variant_dir
    )
    if variant_validation.get("status") != "PASS":
        raise DynamicV3BacktestSimulationError(
            "backtest simulation variant artifact validation failed"
        )
    variant_bundle = _backtest_sim_variant_bundle(source_dir)
    variant_manifest = _mapping(variant_bundle.get("manifest"))
    variant_generated = _datetime_from_any(variant_manifest.get("generated_at"))
    if variant_generated is None or variant_generated > generated:
        raise DynamicV3BacktestSimulationError("variant source generated after outcome cutoff")
    event_set_id = _text(variant_manifest.get("event_set_id"))
    variant_snapshot = _mapping(variant_bundle.get("input_snapshot"))
    event_bundle = _mapping(variant_snapshot.get("event_bundle"))
    event_snapshot = _mapping(event_bundle.get("input_snapshot"))
    frozen_event_dir = Path(_text(event_bundle.get("event_set_dir")))
    if frozen_event_dir.name != event_set_id or frozen_event_dir.parent != event_dir:
        raise DynamicV3BacktestSimulationError("variant event source binding invalid")
    config = _mapping(variant_snapshot.get("config"))
    if config != _mapping(event_snapshot.get("config")):
        raise DynamicV3BacktestSimulationError("variant config binding invalid")
    source = _mapping(config.get("source"))
    requested_end = (
        _date_from_any(_mapping(config.get("date_range")).get("end")) or generated.date()
    )
    if requested_end > generated.date():
        raise DynamicV3BacktestSimulationError("requested end exceeds outcome cutoff")
    prices_path = _resolve_project_path(Path(_text(source.get("price_cache_path"))))
    rates_path = _resolve_project_path(Path(_text(source.get("rates_cache_path"))))
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_content = ""
    if enforce_data_quality_gate:
        with tempfile.TemporaryDirectory(prefix="backtest-sim-outcome-dq-") as temp_dir:
            temp_quality_report = Path(temp_dir) / "validate_data_quality_report.md"
            quality = _run_cached_quality_gate(
                as_of=requested_end,
                prices_path=prices_path,
                rates_path=rates_path,
                report_path=temp_quality_report,
            )
            quality_report_content = (
                temp_quality_report.read_text(encoding="utf-8")
                if temp_quality_report.is_file()
                else ""
            )
        quality_status = quality.status
        if not quality.passed:
            raise DynamicV3BacktestSimulationError(
                f"backtest simulation outcome data quality gate failed: {quality.status}"
            )
    rows_in = _records(variant_bundle.get("weights"))
    prices = _load_prices(prices_path, extra_symbols=_weights_symbols(rows_in))
    price_dates = _available_price_dates(prices)
    windows = _records_as_ints(_mapping(config.get("outcome_windows")).get("trading_days"))
    outcome_rows = _outcome_rows(
        variant_rows=rows_in,
        windows=windows,
        prices=prices,
        price_dates=price_dates,
        generated_date=generated.date(),
    )
    summary = _variant_summary(outcome_rows)
    rollup = Counter(row["outcome_status"] for row in outcome_rows)
    status = "AVAILABLE" if rollup.get("AVAILABLE") else "INSUFFICIENT_DATA"
    if rollup.get("PENDING") and rollup.get("AVAILABLE"):
        status = "PASS_WITH_WARNINGS"
    elif rollup.get("PENDING"):
        status = "PENDING"
    sim_outcome_id = _stable_id("backtest-sim-outcome", variant_set_id, generated.isoformat())
    outcome_dir = _unique_dir(output_dir / sim_outcome_id)
    quality_report_path = (
        str(outcome_dir / "validate_data_quality_report.md") if enforce_data_quality_gate else ""
    )
    input_snapshot = {
        "schema_version": BACKTEST_SIM_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_outcome_input_snapshot",
        "generated_at": generated.isoformat(),
        "variant_set_id": variant_set_id,
        "variant_set_dir": str(source_dir),
        "variant_bundle": variant_bundle,
        "variant_validation": variant_validation,
        "event_set_id": event_set_id,
        "event_set_dir": str(frozen_event_dir),
        "config": config,
        "requested_end": requested_end.isoformat(),
        "price_source": _price_source_snapshot(prices_path),
        "rate_source": _price_source_snapshot(rates_path),
        "price_cutoff_snapshot": _csv_cutoff_snapshot(prices_path, end=requested_end),
        "rate_cutoff_snapshot": _csv_cutoff_snapshot(rates_path, end=requested_end),
        "data_quality_enforced": enforce_data_quality_gate,
        "data_quality_status": quality_status,
        "data_quality_report_content": quality_report_content,
        "data_quality_report_checksum": (
            sha256(quality_report_content.encode("utf-8")).hexdigest()
            if quality_report_content
            else ""
        ),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
    }
    manifest = _backtest_sim_outcome_manifest(
        outcome_dir=outcome_dir,
        generated=generated,
        status=status,
        variant_set_id=variant_set_id,
        event_set_id=event_set_id,
        windows=windows,
        rollup=rollup,
        summary=summary,
        config=config,
        quality_status=quality_status,
        quality_report_path=quality_report_path,
    )
    outcome_dir.mkdir(parents=True, exist_ok=False)
    _write_json(outcome_dir / "sim_outcome_manifest.json", manifest)
    _write_jsonl(outcome_dir / "simulated_outcome_windows.jsonl", outcome_rows)
    _write_json(outcome_dir / "simulated_variant_summary.json", summary)
    _write_json(outcome_dir / "outcome_input_snapshot.json", input_snapshot)
    _write_text(
        outcome_dir / "backtest_sim_outcome_report.md",
        render_outcome_report(manifest, summary),
    )
    if quality_report_content:
        _write_text(outcome_dir / "validate_data_quality_report.md", quality_report_content)
    _update_latest_pointer(
        "latest_backtest_sim_outcome",
        outcome_dir.name,
        outcome_dir / "sim_outcome_manifest.json",
    )
    return {
        "sim_outcome_id": outcome_dir.name,
        "sim_outcome_dir": outcome_dir,
        "manifest": manifest,
        "outcome_rows": outcome_rows,
        "variant_summary": summary,
        "input_snapshot": input_snapshot,
    }


def _backtest_sim_outcome_manifest(
    *,
    outcome_dir: Path,
    generated: datetime,
    status: str,
    variant_set_id: str,
    event_set_id: str,
    windows: Sequence[int],
    rollup: Mapping[str, int],
    summary: Mapping[str, Any],
    config: Mapping[str, Any],
    quality_status: str,
    quality_report_path: str,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_outcome_manifest",
        "sim_outcome_id": outcome_dir.name,
        "variant_set_id": variant_set_id,
        "event_set_id": event_set_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "tracked_windows": windows,
        "available_count": rollup.get("AVAILABLE", 0),
        "pending_count": rollup.get("PENDING", 0),
        "insufficient_data_count": rollup.get("INSUFFICIENT_DATA", 0),
        "best_variant": summary.get("best_variant", "MISSING"),
        "forward_confirmation_policy": dict(_mapping(config.get("forward_confirmation"))),
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "sim_outcome_manifest_path": str(outcome_dir / "sim_outcome_manifest.json"),
        "simulated_outcome_windows_path": str(outcome_dir / "simulated_outcome_windows.jsonl"),
        "simulated_variant_summary_path": str(outcome_dir / "simulated_variant_summary.json"),
        "outcome_input_snapshot_path": str(outcome_dir / "outcome_input_snapshot.json"),
        "backtest_sim_outcome_report_path": str(outcome_dir / "backtest_sim_outcome_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def backtest_sim_outcome_report_payload(
    *,
    sim_outcome_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
) -> dict[str, Any]:
    outcome_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=sim_outcome_id if not latest else None,
        pointer_name="latest_backtest_sim_outcome",
    )
    return {
        **_read_json(outcome_dir / "sim_outcome_manifest.json"),
        "simulated_outcome_windows": _read_jsonl(outcome_dir / "simulated_outcome_windows.jsonl"),
        "simulated_variant_summary": _read_json(outcome_dir / "simulated_variant_summary.json"),
        "outcome_input_snapshot": _read_json(outcome_dir / "outcome_input_snapshot.json"),
        "sim_outcome_dir": str(outcome_dir),
    }


def validate_backtest_sim_outcome_artifact(
    *, sim_outcome_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR
) -> dict[str, Any]:
    outcome_dir = output_dir / sim_outcome_id
    manifest = _read_optional_json(outcome_dir / "sim_outcome_manifest.json") or {}
    rows = _read_jsonl(outcome_dir / "simulated_outcome_windows.jsonl")
    summary = _read_optional_json(outcome_dir / "simulated_variant_summary.json") or {}
    snapshot = _read_optional_json(outcome_dir / "outcome_input_snapshot.json") or {}
    source_errors: list[str] = []
    recompute_error = ""
    try:
        if snapshot.get("schema_version") != BACKTEST_SIM_OUTCOME_SNAPSHOT_SCHEMA_VERSION:
            source_errors.append("snapshot_schema_invalid")
        generated = _datetime_from_any(snapshot.get("generated_at"))
        variant_set_id = _text(snapshot.get("variant_set_id"))
        variant_set_dir = Path(_text(snapshot.get("variant_set_dir")))
        event_set_id = _text(snapshot.get("event_set_id"))
        event_set_dir = Path(_text(snapshot.get("event_set_dir")))
        requested_end = _date_from_any(snapshot.get("requested_end"))
        if (
            generated is None
            or requested_end is None
            or requested_end > generated.date()
            or variant_set_dir.name != variant_set_id
            or event_set_dir.name != event_set_id
        ):
            raise DynamicV3BacktestSimulationError("outcome source identity/time invalid")
        live_validation = validate_backtest_sim_variants_artifact(
            variant_set_id=variant_set_id, output_dir=variant_set_dir.parent
        )
        if live_validation.get("status") != "PASS":
            source_errors.append("variant_validation_failed")
        if live_validation != snapshot.get("variant_validation"):
            source_errors.append("variant_validation_changed")
        live_bundle = _backtest_sim_variant_bundle(variant_set_dir)
        if live_bundle != snapshot.get("variant_bundle"):
            source_errors.append("variant_bundle_changed")
        variant_snapshot = _mapping(live_bundle.get("input_snapshot"))
        frozen_event_bundle = _mapping(variant_snapshot.get("event_bundle"))
        if Path(_text(frozen_event_bundle.get("event_set_dir"))) != event_set_dir:
            source_errors.append("event_binding_changed")
        config = _mapping(snapshot.get("config"))
        if config != _mapping(variant_snapshot.get("config")):
            source_errors.append("config_binding_changed")
        source = _mapping(config.get("source"))
        prices_path = _resolve_project_path(Path(_text(source.get("price_cache_path"))))
        rates_path = _resolve_project_path(Path(_text(source.get("rates_cache_path"))))
        if _price_source_snapshot(prices_path) != snapshot.get("price_source"):
            source_errors.append("price_source_changed")
        if _price_source_snapshot(rates_path) != snapshot.get("rate_source"):
            source_errors.append("rate_source_changed")
        if _csv_cutoff_snapshot(prices_path, end=requested_end) != snapshot.get(
            "price_cutoff_snapshot"
        ):
            source_errors.append("price_cutoff_rows_changed")
        if _csv_cutoff_snapshot(rates_path, end=requested_end) != snapshot.get(
            "rate_cutoff_snapshot"
        ):
            source_errors.append("rate_cutoff_rows_changed")
        quality_status = _text(snapshot.get("data_quality_status"))
        quality_content = _text(snapshot.get("data_quality_report_content"))
        quality_checksum = _text(snapshot.get("data_quality_report_checksum"))
        quality_enforced = snapshot.get("data_quality_enforced") is True
        if bool(quality_content) != quality_enforced:
            source_errors.append("data_quality_evidence_missing")
        if (
            quality_content
            and sha256(quality_content.encode("utf-8")).hexdigest() != quality_checksum
        ):
            source_errors.append("data_quality_evidence_checksum_invalid")
        quality_path = outcome_dir / "validate_data_quality_report.md"
        if quality_content and (
            not quality_path.is_file()
            or quality_path.read_text(encoding="utf-8") != quality_content
        ):
            source_errors.append("data_quality_report_changed")
        if quality_enforced:
            with tempfile.TemporaryDirectory(
                prefix="backtest-sim-outcome-validate-dq-"
            ) as temp_dir:
                quality = _run_cached_quality_gate(
                    as_of=requested_end,
                    prices_path=prices_path,
                    rates_path=rates_path,
                    report_path=Path(temp_dir) / "validate_data_quality_report.md",
                )
            if not quality.passed or quality.status != quality_status:
                source_errors.append("data_quality_changed")
        variant_rows = _records(live_bundle.get("weights"))
        prices = _load_prices(prices_path, extra_symbols=_weights_symbols(variant_rows))
        price_dates = _available_price_dates(prices)
        windows = _records_as_ints(_mapping(config.get("outcome_windows")).get("trading_days"))
        expected_rows = _outcome_rows(
            variant_rows=variant_rows,
            windows=windows,
            prices=prices,
            price_dates=price_dates,
            generated_date=generated.date(),
        )
        expected_summary = _variant_summary(expected_rows)
        rollup = Counter(_text(row.get("outcome_status")) for row in expected_rows)
        expected_status = "AVAILABLE" if rollup.get("AVAILABLE") else "INSUFFICIENT_DATA"
        if rollup.get("PENDING") and rollup.get("AVAILABLE"):
            expected_status = "PASS_WITH_WARNINGS"
        elif rollup.get("PENDING"):
            expected_status = "PENDING"
        expected_snapshot = {
            "schema_version": BACKTEST_SIM_OUTCOME_SNAPSHOT_SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_backtest_sim_outcome_input_snapshot",
            "generated_at": generated.isoformat(),
            "variant_set_id": variant_set_id,
            "variant_set_dir": str(variant_set_dir),
            "variant_bundle": live_bundle,
            "variant_validation": live_validation,
            "event_set_id": event_set_id,
            "event_set_dir": str(event_set_dir),
            "config": config,
            "requested_end": requested_end.isoformat(),
            "price_source": _price_source_snapshot(prices_path),
            "rate_source": _price_source_snapshot(rates_path),
            "price_cutoff_snapshot": _csv_cutoff_snapshot(prices_path, end=requested_end),
            "rate_cutoff_snapshot": _csv_cutoff_snapshot(rates_path, end=requested_end),
            "data_quality_enforced": quality_enforced,
            "data_quality_status": quality_status,
            "data_quality_report_content": quality_content,
            "data_quality_report_checksum": quality_checksum,
            "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
            "pit_safety_status": PIT_SAFETY_SIMULATION,
            "not_for_production": True,
        }
        expected_manifest = _backtest_sim_outcome_manifest(
            outcome_dir=outcome_dir,
            generated=generated,
            status=expected_status,
            variant_set_id=variant_set_id,
            event_set_id=event_set_id,
            windows=windows,
            rollup=rollup,
            summary=expected_summary,
            config=config,
            quality_status=quality_status,
            quality_report_path=str(quality_path) if quality_enforced else "",
        )
        expected_report = render_outcome_report(expected_manifest, expected_summary)
    except Exception as exc:  # noqa: BLE001
        recompute_error = str(exc)
        expected_rows = []
        expected_summary = {}
        expected_snapshot = {}
        expected_manifest = {}
        expected_report = ""
    report_path = outcome_dir / "backtest_sim_outcome_report.md"
    checks = [
        _check("manifest_exists", (outcome_dir / "sim_outcome_manifest.json").exists(), ""),
        _check("windows_exists", (outcome_dir / "simulated_outcome_windows.jsonl").exists(), ""),
        _check("summary_exists", (outcome_dir / "simulated_variant_summary.json").exists(), ""),
        _check("snapshot_exists", (outcome_dir / "outcome_input_snapshot.json").exists(), ""),
        _check("report_exists", (outcome_dir / "backtest_sim_outcome_report.md").exists(), ""),
        _check("sim_outcome_id_matches", manifest.get("sim_outcome_id") == sim_outcome_id, ""),
        _check("source_snapshot_valid", not source_errors, ",".join(source_errors)),
        _check("views_recomputed", not recompute_error, recompute_error),
        _check("snapshot_recomputed", snapshot == expected_snapshot, "live source"),
        _check("windows_recomputed", rows == expected_rows, "snapshot"),
        _check("summary_recomputed", summary == expected_summary, "snapshot"),
        _check("manifest_recomputed", manifest == expected_manifest, "snapshot"),
        _check(
            "snapshot_bytes_recomputed",
            (outcome_dir / "outcome_input_snapshot.json").is_file()
            and (outcome_dir / "outcome_input_snapshot.json").read_text(encoding="utf-8")
            == _canonical_json_text(expected_snapshot),
            "canonical JSON",
        ),
        _check(
            "windows_bytes_recomputed",
            (outcome_dir / "simulated_outcome_windows.jsonl").is_file()
            and (outcome_dir / "simulated_outcome_windows.jsonl").read_text(encoding="utf-8")
            == _canonical_jsonl_text(expected_rows),
            "canonical JSONL",
        ),
        _check(
            "summary_bytes_recomputed",
            (outcome_dir / "simulated_variant_summary.json").is_file()
            and (outcome_dir / "simulated_variant_summary.json").read_text(encoding="utf-8")
            == _canonical_json_text(expected_summary),
            "canonical JSON",
        ),
        _check(
            "manifest_bytes_recomputed",
            (outcome_dir / "sim_outcome_manifest.json").is_file()
            and (outcome_dir / "sim_outcome_manifest.json").read_text(encoding="utf-8")
            == _canonical_json_text(expected_manifest),
            "canonical JSON",
        ),
        _check(
            "report_recomputed",
            report_path.is_file() and report_path.read_text(encoding="utf-8") == expected_report,
            "Markdown",
        ),
        _check(
            "outcome_status_valid",
            all(row.get("outcome_status") in OUTCOME_STATUSES for row in rows),
            "statuses",
        ),
        _check(
            "all_backtest_simulation",
            all(row.get("outcome_mode") == OUTCOME_MODE_BACKTEST_SIMULATION for row in rows),
            OUTCOME_MODE_BACKTEST_SIMULATION,
        ),
        _check("outcome_contract_valid", _outcome_rows_contract_valid(rows), "rows"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_taken") is False
            and all(row.get("broker_action_taken") is False for row in rows),
            "broker action forbidden",
        ),
        _check(
            "not_for_production",
            manifest.get("not_for_production") is True
            and all(row.get("not_for_production") is True for row in rows),
            "not for production",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_outcome_validation",
        artifact_id_key="sim_outcome_id",
        artifact_id=sim_outcome_id,
        checks=checks,
    )


def _backtest_sim_variant_bundle(variant_dir: Path) -> dict[str, Any]:
    manifest_path = variant_dir / "variant_set_manifest.json"
    weights_path = variant_dir / "simulated_variant_weights.jsonl"
    ledger_path = variant_dir / "variant_action_ledger.jsonl"
    snapshot_path = variant_dir / "variant_input_snapshot.json"
    report_path = variant_dir / "variant_generation_report.md"
    return {
        "variant_set_dir": str(variant_dir),
        "manifest": _read_json(manifest_path),
        "weights": _read_jsonl(weights_path),
        "ledger": _read_jsonl(ledger_path),
        "input_snapshot": _read_json(snapshot_path),
        "variant_generation_report": _read_text(report_path),
        "file_contents": {
            "variant_set_manifest.json": _read_text(manifest_path),
            "simulated_variant_weights.jsonl": _read_text(weights_path),
            "variant_action_ledger.jsonl": _read_text(ledger_path),
            "variant_input_snapshot.json": _read_text(snapshot_path),
            "variant_generation_report.md": _read_text(report_path),
        },
    }


def run_backtest_sim_paper(
    *,
    variant_set_id: str,
    variant: str = "limited_adjustment",
    variant_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_PAPER_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = variant_dir / variant_set_id
    variant_manifest = _read_json(source_dir / "variant_set_manifest.json")
    event_set_id = _text(variant_manifest.get("event_set_id"))
    event_snapshot = _read_json(event_dir / event_set_id / "simulation_input_snapshot.json")
    config = _mapping(event_snapshot.get("config"))
    prices_path = _resolve_project_path(
        Path(_text(_mapping(config.get("source")).get("price_cache_path")))
    )
    rows = _read_jsonl(source_dir / "simulated_variant_weights.jsonl")
    selected = [row for row in rows if row.get("variant") == variant]
    if not selected:
        raise DynamicV3BacktestSimulationError(f"unsupported simulation variant: {variant}")
    prices = _load_prices(prices_path, extra_symbols=_weights_symbols(selected))
    state_history, ledger, summary = _paper_history(selected, variant=variant, prices=prices)
    baseline_history, _, baseline_summary = _paper_history(
        [row for row in rows if row.get("variant") == "no_trade"],
        variant="no_trade",
        prices=prices,
    )
    summary["relative_to_no_trade"] = round(
        _float(summary.get("total_return")) - _float(baseline_summary.get("total_return")),
        6,
    )
    summary["relative_to_baseline"] = summary["relative_to_no_trade"]
    sim_paper_id = _stable_id("backtest-sim-paper", variant_set_id, variant, generated.isoformat())
    paper_dir = _unique_dir(output_dir / sim_paper_id)
    paper_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_paper_manifest",
        "sim_paper_id": paper_dir.name,
        "variant_set_id": variant_set_id,
        "event_set_id": event_set_id,
        "generated_at": generated.isoformat(),
        "status": summary.get("simulation_status", "INSUFFICIENT_DATA"),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "variant": variant,
        "state_row_count": len(state_history),
        "trade_count": len(ledger),
        "baseline_state_row_count": len(baseline_history),
        "sim_paper_manifest_path": str(paper_dir / "sim_paper_manifest.json"),
        "sim_paper_state_history_path": str(paper_dir / "sim_paper_state_history.jsonl"),
        "sim_trade_ledger_path": str(paper_dir / "sim_trade_ledger.jsonl"),
        "sim_paper_performance_summary_path": str(paper_dir / "sim_paper_performance_summary.json"),
        "backtest_sim_paper_report_path": str(paper_dir / "backtest_sim_paper_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(paper_dir / "sim_paper_manifest.json", manifest)
    _write_jsonl(paper_dir / "sim_paper_state_history.jsonl", state_history)
    _write_jsonl(paper_dir / "sim_trade_ledger.jsonl", ledger)
    _write_json(paper_dir / "sim_paper_performance_summary.json", summary)
    _write_text(paper_dir / "backtest_sim_paper_report.md", render_paper_report(manifest, summary))
    _update_latest_pointer(
        "latest_backtest_sim_paper",
        paper_dir.name,
        paper_dir / "sim_paper_manifest.json",
    )
    return {
        "sim_paper_id": paper_dir.name,
        "sim_paper_dir": paper_dir,
        "manifest": manifest,
        "state_history": state_history,
        "trade_ledger": ledger,
        "performance_summary": summary,
    }


def backtest_sim_paper_report_payload(
    *,
    sim_paper_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_PAPER_DIR,
) -> dict[str, Any]:
    paper_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=sim_paper_id if not latest else None,
        pointer_name="latest_backtest_sim_paper",
    )
    return {
        **_read_json(paper_dir / "sim_paper_manifest.json"),
        "sim_paper_state_history": _read_jsonl(paper_dir / "sim_paper_state_history.jsonl"),
        "sim_trade_ledger": _read_jsonl(paper_dir / "sim_trade_ledger.jsonl"),
        "sim_paper_performance_summary": _read_json(
            paper_dir / "sim_paper_performance_summary.json"
        ),
        "sim_paper_dir": str(paper_dir),
    }


def validate_backtest_sim_paper_artifact(
    *, sim_paper_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_PAPER_DIR
) -> dict[str, Any]:
    paper_dir = output_dir / sim_paper_id
    manifest = _read_optional_json(paper_dir / "sim_paper_manifest.json") or {}
    history = _read_jsonl(paper_dir / "sim_paper_state_history.jsonl")
    ledger = _read_jsonl(paper_dir / "sim_trade_ledger.jsonl")
    checks = [
        _check("manifest_exists", (paper_dir / "sim_paper_manifest.json").exists(), ""),
        _check("history_exists", (paper_dir / "sim_paper_state_history.jsonl").exists(), ""),
        _check("ledger_exists", (paper_dir / "sim_trade_ledger.jsonl").exists(), ""),
        _check("summary_exists", (paper_dir / "sim_paper_performance_summary.json").exists(), ""),
        _check("report_exists", (paper_dir / "backtest_sim_paper_report.md").exists(), ""),
        _check("sim_paper_id_matches", manifest.get("sim_paper_id") == sim_paper_id, ""),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_taken") is False
            and all(row.get("broker_action_taken") is False for row in history)
            and all(row.get("broker_action_taken") is False for row in ledger),
            "broker action forbidden",
        ),
        _check(
            "outcome_mode_backtest_simulation",
            manifest.get("outcome_mode") == OUTCOME_MODE_BACKTEST_SIMULATION,
            OUTCOME_MODE_BACKTEST_SIMULATION,
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_paper_validation",
        artifact_id_key="sim_paper_id",
        artifact_id=sim_paper_id,
        checks=checks,
    )


def run_backtest_sim_regime_review(
    *,
    sim_outcome_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_REGIME_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = outcome_dir / sim_outcome_id
    manifest_in = _read_json(source_dir / "sim_outcome_manifest.json")
    rows = _read_jsonl(source_dir / "simulated_outcome_windows.jsonl")
    metrics = _regime_metrics(rows)
    inventory = _regime_inventory(rows)
    summary = _regime_summary(metrics)
    review_id = _stable_id("backtest-sim-regime", sim_outcome_id, generated.isoformat())
    regime_dir = _unique_dir(output_dir / review_id)
    regime_dir.mkdir(parents=True, exist_ok=False)
    available = sum(row["event_count"] for row in metrics if row["status"] != "INSUFFICIENT_DATA")
    status = "PASS" if available else "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_regime_manifest",
        "regime_review_id": regime_dir.name,
        "sim_outcome_id": sim_outcome_id,
        "variant_set_id": manifest_in.get("variant_set_id"),
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "sim_regime_manifest_path": str(regime_dir / "sim_regime_manifest.json"),
        "regime_window_inventory_path": str(regime_dir / "regime_window_inventory.json"),
        "variant_regime_metrics_path": str(regime_dir / "variant_regime_metrics.jsonl"),
        "regime_review_summary_path": str(regime_dir / "regime_review_summary.json"),
        "backtest_sim_regime_report_path": str(regime_dir / "backtest_sim_regime_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(regime_dir / "sim_regime_manifest.json", manifest)
    _write_json(regime_dir / "regime_window_inventory.json", inventory)
    _write_jsonl(regime_dir / "variant_regime_metrics.jsonl", metrics)
    _write_json(regime_dir / "regime_review_summary.json", summary)
    _write_text(
        regime_dir / "backtest_sim_regime_report.md", render_regime_report(manifest, summary)
    )
    _update_latest_pointer(
        "latest_backtest_sim_regime",
        regime_dir.name,
        regime_dir / "sim_regime_manifest.json",
    )
    return {
        "regime_review_id": regime_dir.name,
        "regime_review_dir": regime_dir,
        "manifest": manifest,
        "regime_window_inventory": inventory,
        "variant_regime_metrics": metrics,
        "regime_review_summary": summary,
    }


def backtest_sim_regime_report_payload(
    *,
    regime_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_REGIME_DIR,
) -> dict[str, Any]:
    regime_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=regime_review_id if not latest else None,
        pointer_name="latest_backtest_sim_regime",
    )
    return {
        **_read_json(regime_dir / "sim_regime_manifest.json"),
        "regime_window_inventory": _read_json(regime_dir / "regime_window_inventory.json"),
        "variant_regime_metrics": _read_jsonl(regime_dir / "variant_regime_metrics.jsonl"),
        "regime_review_summary": _read_json(regime_dir / "regime_review_summary.json"),
        "regime_review_dir": str(regime_dir),
    }


def validate_backtest_sim_regime_artifact(
    *, regime_review_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_REGIME_DIR
) -> dict[str, Any]:
    regime_dir = output_dir / regime_review_id
    manifest = _read_optional_json(regime_dir / "sim_regime_manifest.json") or {}
    metrics = _read_jsonl(regime_dir / "variant_regime_metrics.jsonl")
    checks = [
        _check("manifest_exists", (regime_dir / "sim_regime_manifest.json").exists(), ""),
        _check("inventory_exists", (regime_dir / "regime_window_inventory.json").exists(), ""),
        _check("metrics_exists", (regime_dir / "variant_regime_metrics.jsonl").exists(), ""),
        _check("summary_exists", (regime_dir / "regime_review_summary.json").exists(), ""),
        _check("report_exists", (regime_dir / "backtest_sim_regime_report.md").exists(), ""),
        _check(
            "regime_review_id_matches",
            manifest.get("regime_review_id") == regime_review_id,
            "",
        ),
        _check(
            "regime_names_valid",
            all(row.get("regime") in REGIME_BUCKETS for row in metrics),
            "regime buckets",
        ),
        _check("broker_action_forbidden", manifest.get("broker_action_taken") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_regime_validation",
        artifact_id_key="regime_review_id",
        artifact_id=regime_review_id,
        checks=checks,
    )


def run_backtest_sim_sensitivity(
    *,
    sim_outcome_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    variant_dir: Path = DEFAULT_BACKTEST_SIM_VARIANT_DIR,
    event_dir: Path = DEFAULT_BACKTEST_SIM_EVENT_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_source_dir = outcome_dir / sim_outcome_id
    outcome_manifest = _read_json(outcome_source_dir / "sim_outcome_manifest.json")
    variant_set_id = _text(outcome_manifest.get("variant_set_id"))
    event_set_id = _text(outcome_manifest.get("event_set_id"))
    events = _read_jsonl(event_dir / event_set_id / "simulated_advisory_events.jsonl")
    config = _mapping(
        _read_json(event_dir / event_set_id / "simulation_input_snapshot.json").get("config")
    )
    rows = _read_jsonl(outcome_source_dir / "simulated_outcome_windows.jsonl")
    source = _mapping(config.get("source"))
    prices = _load_prices(
        _resolve_project_path(Path(_text(source.get("price_cache_path")))),
        extra_symbols=_event_symbols(events),
    )
    policy = _mapping(config.get("sensitivity_policy"))
    frequency = _frequency_sensitivity(rows, policy)
    adjustment = _adjustment_limit_sensitivity(events, config, prices, policy)
    shortlist = _shortlist_sensitivity(events, prices, policy)
    threshold = _threshold_sensitivity(rows, policy)
    warnings = _overfit_warnings(rows, frequency, adjustment, shortlist, threshold, policy)
    sensitivity_id = _stable_id("backtest-sim-sensitivity", sim_outcome_id, generated.isoformat())
    sensitivity_dir = _unique_dir(output_dir / sensitivity_id)
    sensitivity_dir.mkdir(parents=True, exist_ok=False)
    status = warnings["simulation_overfit_status"]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_sensitivity_manifest",
        "sensitivity_id": sensitivity_dir.name,
        "sim_outcome_id": sim_outcome_id,
        "variant_set_id": variant_set_id,
        "event_set_id": event_set_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "simulation_overfit_status": status,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "sim_sensitivity_manifest_path": str(sensitivity_dir / "sim_sensitivity_manifest.json"),
        "threshold_sensitivity_path": str(sensitivity_dir / "threshold_sensitivity.json"),
        "shortlist_sensitivity_path": str(sensitivity_dir / "shortlist_sensitivity.json"),
        "adjustment_limit_sensitivity_path": str(
            sensitivity_dir / "adjustment_limit_sensitivity.json"
        ),
        "event_frequency_sensitivity_path": str(
            sensitivity_dir / "event_frequency_sensitivity.json"
        ),
        "overfit_warning_summary_path": str(sensitivity_dir / "overfit_warning_summary.json"),
        "backtest_sim_sensitivity_report_path": str(
            sensitivity_dir / "backtest_sim_sensitivity_report.md"
        ),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(sensitivity_dir / "sim_sensitivity_manifest.json", manifest)
    _write_json(sensitivity_dir / "threshold_sensitivity.json", threshold)
    _write_json(sensitivity_dir / "shortlist_sensitivity.json", shortlist)
    _write_json(sensitivity_dir / "adjustment_limit_sensitivity.json", adjustment)
    _write_json(sensitivity_dir / "event_frequency_sensitivity.json", frequency)
    _write_json(sensitivity_dir / "overfit_warning_summary.json", warnings)
    _write_text(
        sensitivity_dir / "backtest_sim_sensitivity_report.md",
        render_sensitivity_report(manifest, warnings),
    )
    _update_latest_pointer(
        "latest_backtest_sim_sensitivity",
        sensitivity_dir.name,
        sensitivity_dir / "sim_sensitivity_manifest.json",
    )
    return {
        "sensitivity_id": sensitivity_dir.name,
        "sensitivity_dir": sensitivity_dir,
        "manifest": manifest,
        "threshold_sensitivity": threshold,
        "shortlist_sensitivity": shortlist,
        "adjustment_limit_sensitivity": adjustment,
        "event_frequency_sensitivity": frequency,
        "overfit_warning_summary": warnings,
    }


def backtest_sim_sensitivity_report_payload(
    *,
    sensitivity_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
) -> dict[str, Any]:
    sensitivity_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=sensitivity_id if not latest else None,
        pointer_name="latest_backtest_sim_sensitivity",
    )
    return {
        **_read_json(sensitivity_dir / "sim_sensitivity_manifest.json"),
        "threshold_sensitivity": _read_json(sensitivity_dir / "threshold_sensitivity.json"),
        "shortlist_sensitivity": _read_json(sensitivity_dir / "shortlist_sensitivity.json"),
        "adjustment_limit_sensitivity": _read_json(
            sensitivity_dir / "adjustment_limit_sensitivity.json"
        ),
        "event_frequency_sensitivity": _read_json(
            sensitivity_dir / "event_frequency_sensitivity.json"
        ),
        "overfit_warning_summary": _read_json(sensitivity_dir / "overfit_warning_summary.json"),
        "sensitivity_dir": str(sensitivity_dir),
    }


def validate_backtest_sim_sensitivity_artifact(
    *, sensitivity_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR
) -> dict[str, Any]:
    sensitivity_dir = output_dir / sensitivity_id
    manifest = _read_optional_json(sensitivity_dir / "sim_sensitivity_manifest.json") or {}
    warnings = _read_optional_json(sensitivity_dir / "overfit_warning_summary.json") or {}
    checks = [
        _check("manifest_exists", (sensitivity_dir / "sim_sensitivity_manifest.json").exists(), ""),
        _check("threshold_exists", (sensitivity_dir / "threshold_sensitivity.json").exists(), ""),
        _check("shortlist_exists", (sensitivity_dir / "shortlist_sensitivity.json").exists(), ""),
        _check(
            "adjustment_limit_exists",
            (sensitivity_dir / "adjustment_limit_sensitivity.json").exists(),
            "",
        ),
        _check(
            "frequency_exists",
            (sensitivity_dir / "event_frequency_sensitivity.json").exists(),
            "",
        ),
        _check("warnings_exists", (sensitivity_dir / "overfit_warning_summary.json").exists(), ""),
        _check(
            "report_exists", (sensitivity_dir / "backtest_sim_sensitivity_report.md").exists(), ""
        ),
        _check("sensitivity_id_matches", manifest.get("sensitivity_id") == sensitivity_id, ""),
        _check(
            "overfit_status_valid",
            warnings.get("simulation_overfit_status")
            in {"LOW_RISK", "REVIEW_REQUIRED", "HIGH_RISK", "INSUFFICIENT_DATA"},
            _text(warnings.get("simulation_overfit_status")),
        ),
        _check(
            "high_risk_no_strong_calibration",
            warnings.get("simulation_overfit_status") != "HIGH_RISK"
            or warnings.get("strong_calibration_allowed") is False,
            "HIGH_RISK must not allow strong calibration",
        ),
        _check("broker_action_forbidden", manifest.get("broker_action_taken") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_sensitivity_validation",
        artifact_id_key="sensitivity_id",
        artifact_id=sensitivity_id,
        checks=checks,
    )


def run_backtest_sim_calibration_pack(
    *,
    sim_outcome_id: str,
    sim_paper_id: str,
    regime_review_id: str,
    sensitivity_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    paper_dir: Path = DEFAULT_BACKTEST_SIM_PAPER_DIR,
    regime_dir: Path = DEFAULT_BACKTEST_SIM_REGIME_DIR,
    sensitivity_dir: Path = DEFAULT_BACKTEST_SIM_SENSITIVITY_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_manifest = _read_json(outcome_dir / sim_outcome_id / "sim_outcome_manifest.json")
    outcome_summary = _read_json(outcome_dir / sim_outcome_id / "simulated_variant_summary.json")
    paper_summary = _read_json(paper_dir / sim_paper_id / "sim_paper_performance_summary.json")
    regime_summary = _read_json(regime_dir / regime_review_id / "regime_review_summary.json")
    sensitivity_summary = _read_json(
        sensitivity_dir / sensitivity_id / "overfit_warning_summary.json"
    )
    evidence = _calibration_evidence(
        outcome_summary, paper_summary, regime_summary, sensitivity_summary
    )
    evidence["forward_confirmation_policy"] = _mapping(
        outcome_manifest.get("forward_confirmation_policy")
    )
    proposals = _calibration_proposals(evidence, sensitivity_summary)
    limitations = _simulation_limitations()
    calibration_id = _stable_id(
        "backtest-sim-calibration",
        sim_outcome_id,
        sim_paper_id,
        regime_review_id,
        sensitivity_id,
        generated.isoformat(),
    )
    calibration_dir = _unique_dir(output_dir / calibration_id)
    calibration_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_calibration_manifest",
        "calibration_pack_id": calibration_dir.name,
        "sim_outcome_id": sim_outcome_id,
        "sim_paper_id": sim_paper_id,
        "regime_review_id": regime_review_id,
        "sensitivity_id": sensitivity_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "sim_calibration_manifest_path": str(calibration_dir / "sim_calibration_manifest.json"),
        "simulation_evidence_summary_path": str(
            calibration_dir / "simulation_evidence_summary.json"
        ),
        "proposed_advisory_rule_changes_path": str(
            calibration_dir / "proposed_advisory_rule_changes.json"
        ),
        "simulation_limitations_path": str(calibration_dir / "simulation_limitations.json"),
        "backtest_sim_calibration_report_path": str(
            calibration_dir / "backtest_sim_calibration_report.md"
        ),
        "reader_brief_section_path": str(calibration_dir / "reader_brief_section.md"),
        "auto_apply": False,
        "can_trigger_production": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(calibration_dir / "sim_calibration_manifest.json", manifest)
    _write_json(calibration_dir / "simulation_evidence_summary.json", evidence)
    _write_json(calibration_dir / "proposed_advisory_rule_changes.json", proposals)
    _write_json(calibration_dir / "simulation_limitations.json", limitations)
    _write_text(
        calibration_dir / "backtest_sim_calibration_report.md",
        render_calibration_report(manifest, evidence, proposals),
    )
    _write_text(
        calibration_dir / "reader_brief_section.md",
        render_calibration_reader_brief(evidence, proposals),
    )
    _update_latest_pointer(
        "latest_backtest_sim_calibration",
        calibration_dir.name,
        calibration_dir / "sim_calibration_manifest.json",
    )
    return {
        "calibration_pack_id": calibration_dir.name,
        "calibration_pack_dir": calibration_dir,
        "manifest": manifest,
        "simulation_evidence_summary": evidence,
        "proposed_advisory_rule_changes": proposals,
        "simulation_limitations": limitations,
    }


def backtest_sim_calibration_report_payload(
    *,
    calibration_pack_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
) -> dict[str, Any]:
    calibration_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=calibration_pack_id if not latest else None,
        pointer_name="latest_backtest_sim_calibration",
    )
    return {
        **_read_json(calibration_dir / "sim_calibration_manifest.json"),
        "simulation_evidence_summary": _read_json(
            calibration_dir / "simulation_evidence_summary.json"
        ),
        "proposed_advisory_rule_changes": _read_json(
            calibration_dir / "proposed_advisory_rule_changes.json"
        ),
        "simulation_limitations": _read_json(calibration_dir / "simulation_limitations.json"),
        "reader_brief_section": _read_text(calibration_dir / "reader_brief_section.md"),
        "calibration_pack_dir": str(calibration_dir),
    }


def validate_backtest_sim_calibration_artifact(
    *, calibration_pack_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR
) -> dict[str, Any]:
    calibration_dir = output_dir / calibration_pack_id
    manifest = _read_optional_json(calibration_dir / "sim_calibration_manifest.json") or {}
    proposals = _read_optional_json(calibration_dir / "proposed_advisory_rule_changes.json") or {}
    limitations = _read_optional_json(calibration_dir / "simulation_limitations.json") or {}
    checks = [
        _check("manifest_exists", (calibration_dir / "sim_calibration_manifest.json").exists(), ""),
        _check(
            "evidence_exists",
            (calibration_dir / "simulation_evidence_summary.json").exists(),
            "",
        ),
        _check(
            "proposals_exists",
            (calibration_dir / "proposed_advisory_rule_changes.json").exists(),
            "",
        ),
        _check(
            "limitations_exists", (calibration_dir / "simulation_limitations.json").exists(), ""
        ),
        _check(
            "report_exists", (calibration_dir / "backtest_sim_calibration_report.md").exists(), ""
        ),
        _check("reader_brief_exists", (calibration_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "calibration_pack_id_matches",
            manifest.get("calibration_pack_id") == calibration_pack_id,
            "",
        ),
        _check(
            "auto_apply_false",
            manifest.get("auto_apply") is False
            and all(row.get("auto_apply") is False for row in _records(proposals.get("proposals"))),
            "auto apply false",
        ),
        _check(
            "cannot_trigger_production",
            limitations.get("can_trigger_production") is False
            and manifest.get("can_trigger_production") is False,
            "cannot trigger production",
        ),
        _check("broker_action_forbidden", manifest.get("broker_action_taken") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_calibration_validation",
        artifact_id_key="calibration_pack_id",
        artifact_id=calibration_pack_id,
        checks=checks,
    )


def run_backtest_sim_forward_bridge(
    *,
    calibration_pack_id: str,
    calibration_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    output_dir: Path = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = calibration_dir / calibration_pack_id
    calibration_manifest = _read_json(source_dir / "sim_calibration_manifest.json")
    evidence = _read_json(source_dir / "simulation_evidence_summary.json")
    proposals = _read_json(source_dir / "proposed_advisory_rule_changes.json")
    targets = _forward_confirmation_targets(evidence, proposals)
    questions = _weekly_review_questions()
    bridge_id = _stable_id(
        "backtest-sim-forward-bridge", calibration_pack_id, generated.isoformat()
    )
    bridge_dir = _unique_dir(output_dir / bridge_id)
    bridge_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_forward_bridge_manifest",
        "bridge_id": bridge_dir.name,
        "calibration_pack_id": calibration_pack_id,
        "sim_outcome_id": calibration_manifest.get("sim_outcome_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "next_action": "continue_forward_tracking",
        "sim_forward_bridge_manifest_path": str(bridge_dir / "sim_forward_bridge_manifest.json"),
        "forward_confirmation_targets_path": str(bridge_dir / "forward_confirmation_targets.json"),
        "weekly_review_questions_path": str(bridge_dir / "weekly_review_questions.json"),
        "sim_forward_bridge_report_path": str(bridge_dir / "sim_forward_bridge_report.md"),
        "reader_brief_section_path": str(bridge_dir / "reader_brief_section.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(bridge_dir / "sim_forward_bridge_manifest.json", manifest)
    _write_json(bridge_dir / "forward_confirmation_targets.json", targets)
    _write_json(bridge_dir / "weekly_review_questions.json", questions)
    _write_text(
        bridge_dir / "sim_forward_bridge_report.md", render_forward_bridge_report(manifest, targets)
    )
    _write_text(bridge_dir / "reader_brief_section.md", render_forward_bridge_reader_brief(targets))
    _update_latest_pointer(
        "latest_backtest_sim_forward_bridge",
        bridge_dir.name,
        bridge_dir / "sim_forward_bridge_manifest.json",
    )
    return {
        "bridge_id": bridge_dir.name,
        "bridge_dir": bridge_dir,
        "manifest": manifest,
        "forward_confirmation_targets": targets,
        "weekly_review_questions": questions,
    }


def backtest_sim_forward_bridge_report_payload(
    *,
    bridge_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
) -> dict[str, Any]:
    bridge_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=bridge_id if not latest else None,
        pointer_name="latest_backtest_sim_forward_bridge",
    )
    return {
        **_read_json(bridge_dir / "sim_forward_bridge_manifest.json"),
        "forward_confirmation_targets": _read_json(
            bridge_dir / "forward_confirmation_targets.json"
        ),
        "weekly_review_questions": _read_json(bridge_dir / "weekly_review_questions.json"),
        "reader_brief_section": _read_text(bridge_dir / "reader_brief_section.md"),
        "bridge_dir": str(bridge_dir),
    }


def validate_backtest_sim_forward_bridge_artifact(
    *, bridge_id: str, output_dir: Path = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR
) -> dict[str, Any]:
    bridge_dir = output_dir / bridge_id
    manifest = _read_optional_json(bridge_dir / "sim_forward_bridge_manifest.json") or {}
    targets = _read_optional_json(bridge_dir / "forward_confirmation_targets.json") or {}
    checks = [
        _check("manifest_exists", (bridge_dir / "sim_forward_bridge_manifest.json").exists(), ""),
        _check("targets_exists", (bridge_dir / "forward_confirmation_targets.json").exists(), ""),
        _check("questions_exists", (bridge_dir / "weekly_review_questions.json").exists(), ""),
        _check("report_exists", (bridge_dir / "sim_forward_bridge_report.md").exists(), ""),
        _check("reader_brief_exists", (bridge_dir / "reader_brief_section.md").exists(), ""),
        _check("bridge_id_matches", manifest.get("bridge_id") == bridge_id, ""),
        _check("targets_present", bool(_records(targets.get("targets"))), "targets"),
        _check(
            "no_auto_production",
            manifest.get("production_effect") == "none"
            and manifest.get("broker_action_allowed") is False,
            "no production",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backtest_sim_forward_bridge_validation",
        artifact_id_key="bridge_id",
        artifact_id=bridge_id,
        checks=checks,
    )


def run_sim_interpretation(
    *,
    outcome_id: str,
    calibration_id: str,
    bridge_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    calibration_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    bridge_dir: Path = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    output_dir: Path = DEFAULT_SIM_INTERPRETATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_manifest = _read_json(outcome_dir / outcome_id / "sim_outcome_manifest.json")
    outcome_summary = _read_json(outcome_dir / outcome_id / "simulated_variant_summary.json")
    calibration_evidence = _read_json(
        calibration_dir / calibration_id / "simulation_evidence_summary.json"
    )
    bridge_manifest = _read_json(bridge_dir / bridge_id / "sim_forward_bridge_manifest.json")
    bridge_targets = _read_json(bridge_dir / bridge_id / "forward_confirmation_targets.json")
    matrix = _variant_interpretation_matrix(outcome_summary)
    findings = _sim_key_findings(
        outcome_summary=outcome_summary,
        calibration_evidence=calibration_evidence,
        bridge_targets=bridge_targets,
    )
    interpretation_id = _stable_id(
        "sim-interpretation",
        outcome_id,
        calibration_id,
        bridge_id,
        generated.isoformat(),
    )
    interpretation_dir = _unique_dir(output_dir / interpretation_id)
    interpretation_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sim_interpretation_manifest",
        "interpretation_id": interpretation_dir.name,
        "outcome_id": outcome_id,
        "calibration_id": calibration_id,
        "bridge_id": bridge_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "not_for_production": True,
        "source_best_variant": outcome_manifest.get("best_variant"),
        "source_calibration_readiness": calibration_evidence.get("calibration_readiness"),
        "source_bridge_next_action": bridge_manifest.get("next_action"),
        "sim_interpretation_manifest_path": str(
            interpretation_dir / "sim_interpretation_manifest.json"
        ),
        "variant_interpretation_matrix_path": str(
            interpretation_dir / "variant_interpretation_matrix.json"
        ),
        "key_findings_path": str(interpretation_dir / "key_findings.json"),
        "sim_interpretation_report_path": str(interpretation_dir / "sim_interpretation_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(interpretation_dir / "sim_interpretation_manifest.json", manifest)
    _write_json(interpretation_dir / "variant_interpretation_matrix.json", matrix)
    _write_json(interpretation_dir / "key_findings.json", findings)
    _write_text(
        interpretation_dir / "sim_interpretation_report.md",
        render_sim_interpretation_report(manifest, matrix, findings),
    )
    _update_latest_pointer(
        "latest_sim_interpretation",
        interpretation_dir.name,
        interpretation_dir / "sim_interpretation_manifest.json",
    )
    return {
        "interpretation_id": interpretation_dir.name,
        "interpretation_dir": interpretation_dir,
        "manifest": manifest,
        "variant_interpretation_matrix": matrix,
        "key_findings": findings,
    }


def sim_interpretation_report_payload(
    *,
    interpretation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIM_INTERPRETATION_DIR,
) -> dict[str, Any]:
    interpretation_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=interpretation_id if not latest else None,
        pointer_name="latest_sim_interpretation",
    )
    return {
        **_read_json(interpretation_dir / "sim_interpretation_manifest.json"),
        "variant_interpretation_matrix": _read_json(
            interpretation_dir / "variant_interpretation_matrix.json"
        ),
        "key_findings": _read_json(interpretation_dir / "key_findings.json"),
        "interpretation_dir": str(interpretation_dir),
    }


def validate_sim_interpretation_artifact(
    *, interpretation_id: str, output_dir: Path = DEFAULT_SIM_INTERPRETATION_DIR
) -> dict[str, Any]:
    interpretation_dir = output_dir / interpretation_id
    manifest = _read_optional_json(interpretation_dir / "sim_interpretation_manifest.json") or {}
    matrix = _read_optional_json(interpretation_dir / "variant_interpretation_matrix.json") or {}
    findings = _read_optional_json(interpretation_dir / "key_findings.json") or {}
    variants = _records(matrix.get("variants"))
    checks = [
        _check(
            "manifest_exists",
            (interpretation_dir / "sim_interpretation_manifest.json").exists(),
            "",
        ),
        _check(
            "matrix_exists",
            (interpretation_dir / "variant_interpretation_matrix.json").exists(),
            "",
        ),
        _check("findings_exists", (interpretation_dir / "key_findings.json").exists(), ""),
        _check(
            "report_exists",
            (interpretation_dir / "sim_interpretation_report.md").exists(),
            "",
        ),
        _check(
            "interpretation_id_matches",
            manifest.get("interpretation_id") == interpretation_id,
            "",
        ),
        _check(
            "all_variants_interpreted",
            {row.get("variant") for row in variants} >= set(BACKTEST_SIM_VARIANTS),
            "variants",
        ),
        _check(
            "variant_fields_present",
            all(
                row.get("role") and row.get("risk_profile") and row.get("recommended_usage")
                for row in variants
            ),
            "role/risk/recommended usage",
        ),
        _check(
            "simulation_not_pit_marked",
            manifest.get("report_label") == REPORT_LABEL_BACKTEST_SIMULATION
            and any(
                REPORT_LABEL_BACKTEST_SIMULATION in _texts(row.get("limitations"))
                or "BACKTEST_SIMULATION_NOT_PIT" in _text(row.get("summary"))
                for row in _records(findings.get("findings"))
            ),
            REPORT_LABEL_BACKTEST_SIMULATION,
        ),
        _check(
            "no_auto_policy",
            manifest.get("auto_policy_apply") is False
            and manifest.get("production_effect") == "none",
            "no policy apply",
        ),
        _check("broker_action_forbidden", manifest.get("broker_action_taken") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_sim_interpretation_validation",
        artifact_id_key="interpretation_id",
        artifact_id=interpretation_id,
        checks=checks,
    )


def run_sim_risk_return(
    *,
    outcome_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    output_dir: Path = DEFAULT_SIM_RISK_RETURN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_manifest = _read_json(outcome_dir / outcome_id / "sim_outcome_manifest.json")
    outcome_summary = _read_json(outcome_dir / outcome_id / "simulated_variant_summary.json")
    table = _risk_return_tradeoff_table(outcome_summary)
    summary = _risk_adjusted_summary(table)
    risk_return_id = _stable_id("sim-risk-return", outcome_id, generated.isoformat())
    risk_return_dir = _unique_dir(output_dir / risk_return_id)
    risk_return_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sim_risk_return_manifest",
        "risk_return_id": risk_return_dir.name,
        "outcome_id": outcome_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if table else "INSUFFICIENT_DATA",
        "market_regime": "ai_after_chatgpt",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "not_for_production": True,
        "source_best_variant": outcome_manifest.get("best_variant"),
        "risk_return_manifest_path": str(risk_return_dir / "risk_return_manifest.json"),
        "active_variant_tradeoff_table_path": str(
            risk_return_dir / "active_variant_tradeoff_table.csv"
        ),
        "risk_adjusted_summary_path": str(risk_return_dir / "risk_adjusted_summary.json"),
        "risk_return_report_path": str(risk_return_dir / "risk_return_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(risk_return_dir / "risk_return_manifest.json", manifest)
    pd.DataFrame(table).to_csv(risk_return_dir / "active_variant_tradeoff_table.csv", index=False)
    _write_json(risk_return_dir / "risk_adjusted_summary.json", summary)
    _write_text(
        risk_return_dir / "risk_return_report.md",
        render_risk_return_report(manifest, table, summary),
    )
    _update_latest_pointer(
        "latest_sim_risk_return",
        risk_return_dir.name,
        risk_return_dir / "risk_return_manifest.json",
    )
    return {
        "risk_return_id": risk_return_dir.name,
        "risk_return_dir": risk_return_dir,
        "manifest": manifest,
        "active_variant_tradeoff_table": table,
        "risk_adjusted_summary": summary,
    }


def sim_risk_return_report_payload(
    *,
    risk_return_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIM_RISK_RETURN_DIR,
) -> dict[str, Any]:
    risk_return_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=risk_return_id if not latest else None,
        pointer_name="latest_sim_risk_return",
    )
    return {
        **_read_json(risk_return_dir / "risk_return_manifest.json"),
        "risk_adjusted_summary": _read_json(risk_return_dir / "risk_adjusted_summary.json"),
        "risk_return_dir": str(risk_return_dir),
    }


def validate_sim_risk_return_artifact(
    *, risk_return_id: str, output_dir: Path = DEFAULT_SIM_RISK_RETURN_DIR
) -> dict[str, Any]:
    risk_return_dir = output_dir / risk_return_id
    manifest = _read_optional_json(risk_return_dir / "risk_return_manifest.json") or {}
    summary = _read_optional_json(risk_return_dir / "risk_adjusted_summary.json") or {}
    rows = _records(summary.get("summary"))
    checks = [
        _check("manifest_exists", (risk_return_dir / "risk_return_manifest.json").exists(), ""),
        _check(
            "tradeoff_csv_exists",
            (risk_return_dir / "active_variant_tradeoff_table.csv").exists(),
            "",
        ),
        _check("summary_exists", (risk_return_dir / "risk_adjusted_summary.json").exists(), ""),
        _check("report_exists", (risk_return_dir / "risk_return_report.md").exists(), ""),
        _check("risk_return_id_matches", manifest.get("risk_return_id") == risk_return_id, ""),
        _check(
            "active_variants_present",
            {row.get("variant") for row in rows} >= set(ACTIVE_SIM_VARIANTS),
            "active variants",
        ),
        _check(
            "status_values_valid",
            all(row.get("risk_return_status") in RISK_RETURN_STATUSES for row in rows),
            "risk-return statuses",
        ),
        _check(
            "return_and_risk_separated",
            all(
                "return_improvement_20d_pp" in row and "drawdown_worsening_20d_pp" in row
                for row in rows
            ),
            "separate return/risk fields",
        ),
        _check(
            "no_auto_policy",
            manifest.get("auto_policy_apply") is False
            and manifest.get("production_effect") == "none",
            "no policy apply",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_sim_risk_return_validation",
        artifact_id_key="risk_return_id",
        artifact_id=risk_return_id,
        checks=checks,
    )


def run_sim_defensive_validation(
    *,
    outcome_id: str,
    outcome_dir: Path = DEFAULT_BACKTEST_SIM_OUTCOME_DIR,
    output_dir: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    outcome_manifest = _read_json(outcome_dir / outcome_id / "sim_outcome_manifest.json")
    outcome_rows = _read_jsonl(outcome_dir / outcome_id / "simulated_outcome_windows.jsonl")
    matrix_rows = _defensive_regime_matrix(outcome_rows)
    failure_cases = [
        _defensive_failure_case(row)
        for row in matrix_rows
        if _mapping(row.get("defensive_limited_adjustment")).get("status")
        == "FAILS_DEFENSIVE_EXPECTATION"
    ]
    summary = _defensive_validation_summary(matrix_rows)
    defensive_validation_id = _stable_id(
        "sim-defensive-validation",
        outcome_id,
        generated.isoformat(),
    )
    defensive_dir = _unique_dir(output_dir / defensive_validation_id)
    defensive_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sim_defensive_validation_manifest",
        "defensive_validation_id": defensive_dir.name,
        "outcome_id": outcome_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "not_for_production": True,
        "source_best_variant": outcome_manifest.get("best_variant"),
        "defensive_validation_manifest_path": str(
            defensive_dir / "defensive_validation_manifest.json"
        ),
        "defensive_regime_matrix_path": str(defensive_dir / "defensive_regime_matrix.jsonl"),
        "defensive_failure_cases_path": str(defensive_dir / "defensive_failure_cases.jsonl"),
        "defensive_validation_summary_path": str(
            defensive_dir / "defensive_validation_summary.json"
        ),
        "defensive_validation_report_path": str(defensive_dir / "defensive_validation_report.md"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(defensive_dir / "defensive_validation_manifest.json", manifest)
    _write_jsonl(defensive_dir / "defensive_regime_matrix.jsonl", matrix_rows)
    _write_jsonl(defensive_dir / "defensive_failure_cases.jsonl", failure_cases)
    _write_json(defensive_dir / "defensive_validation_summary.json", summary)
    _write_text(
        defensive_dir / "defensive_validation_report.md",
        render_defensive_validation_report(manifest, summary, matrix_rows),
    )
    _update_latest_pointer(
        "latest_sim_defensive_validation",
        defensive_dir.name,
        defensive_dir / "defensive_validation_manifest.json",
    )
    return {
        "defensive_validation_id": defensive_dir.name,
        "defensive_validation_dir": defensive_dir,
        "manifest": manifest,
        "defensive_regime_matrix": matrix_rows,
        "defensive_failure_cases": failure_cases,
        "defensive_validation_summary": summary,
    }


def sim_defensive_validation_report_payload(
    *,
    defensive_validation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
) -> dict[str, Any]:
    defensive_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=defensive_validation_id if not latest else None,
        pointer_name="latest_sim_defensive_validation",
    )
    return {
        **_read_json(defensive_dir / "defensive_validation_manifest.json"),
        "defensive_regime_matrix": _read_jsonl(defensive_dir / "defensive_regime_matrix.jsonl"),
        "defensive_validation_summary": _read_json(
            defensive_dir / "defensive_validation_summary.json"
        ),
        "defensive_validation_dir": str(defensive_dir),
    }


def validate_sim_defensive_validation_artifact(
    *,
    defensive_validation_id: str,
    output_dir: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
) -> dict[str, Any]:
    defensive_dir = output_dir / defensive_validation_id
    manifest = _read_optional_json(defensive_dir / "defensive_validation_manifest.json") or {}
    rows = _read_jsonl(defensive_dir / "defensive_regime_matrix.jsonl")
    summary = _read_optional_json(defensive_dir / "defensive_validation_summary.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (defensive_dir / "defensive_validation_manifest.json").exists(),
            "",
        ),
        _check("matrix_exists", (defensive_dir / "defensive_regime_matrix.jsonl").exists(), ""),
        _check(
            "failure_cases_exists",
            (defensive_dir / "defensive_failure_cases.jsonl").exists(),
            "",
        ),
        _check(
            "summary_exists",
            (defensive_dir / "defensive_validation_summary.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (defensive_dir / "defensive_validation_report.md").exists(),
            "",
        ),
        _check(
            "defensive_validation_id_matches",
            manifest.get("defensive_validation_id") == defensive_validation_id,
            "",
        ),
        _check(
            "pressure_regimes_present",
            {row.get("regime") for row in rows} >= DEFENSIVE_PRESSURE_REGIMES,
            "pressure regimes",
        ),
        _check(
            "defensive_not_auto_proven",
            summary.get("defensive_limited_adjustment_status")
            in {"NOT_PROVEN_DEFENSIVE", "PARTIALLY_DEFENSIVE", "PROVEN_DEFENSIVE"},
            _text(summary.get("defensive_limited_adjustment_status")),
        ),
        _check(
            "regime_status_valid",
            all(
                _mapping(row.get("defensive_limited_adjustment")).get("status")
                in DEFENSIVE_VALIDATION_STATUSES
                for row in rows
            ),
            "defensive statuses",
        ),
        _check("broker_action_forbidden", manifest.get("broker_action_taken") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_sim_defensive_validation_validation",
        artifact_id_key="defensive_validation_id",
        artifact_id=defensive_validation_id,
        checks=checks,
    )


def run_advisory_proposal_review(
    *,
    interpretation_id: str,
    risk_return_id: str,
    defensive_validation_id: str,
    calibration_id: str,
    interpretation_dir: Path = DEFAULT_SIM_INTERPRETATION_DIR,
    risk_return_dir: Path = DEFAULT_SIM_RISK_RETURN_DIR,
    defensive_validation_dir: Path = DEFAULT_SIM_DEFENSIVE_VALIDATION_DIR,
    calibration_dir: Path = DEFAULT_BACKTEST_SIM_CALIBRATION_DIR,
    output_dir: Path = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    interpretation_manifest = _read_json(
        interpretation_dir / interpretation_id / "sim_interpretation_manifest.json"
    )
    key_findings = _read_json(interpretation_dir / interpretation_id / "key_findings.json")
    risk_summary = _read_json(risk_return_dir / risk_return_id / "risk_adjusted_summary.json")
    defensive_summary = _read_json(
        defensive_validation_dir / defensive_validation_id / "defensive_validation_summary.json"
    )
    calibration_manifest = _read_json(
        calibration_dir / calibration_id / "sim_calibration_manifest.json"
    )
    proposals = _read_json(calibration_dir / calibration_id / "proposed_advisory_rule_changes.json")
    decision_matrix = _proposal_decision_matrix(
        proposals=proposals,
        risk_summary=risk_summary,
        defensive_summary=defensive_summary,
        key_findings=key_findings,
    )
    proposal_review_id = _stable_id(
        "advisory-proposal-review",
        interpretation_id,
        risk_return_id,
        defensive_validation_id,
        calibration_id,
        generated.isoformat(),
    )
    review_dir = _unique_dir(output_dir / proposal_review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    reader_brief = render_proposal_review_reader_brief(decision_matrix, defensive_summary)
    checklist = render_owner_approval_checklist(decision_matrix, defensive_summary)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_proposal_review_manifest",
        "proposal_review_id": review_dir.name,
        "interpretation_id": interpretation_id,
        "risk_return_id": risk_return_id,
        "defensive_validation_id": defensive_validation_id,
        "calibration_id": calibration_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "not_for_production": True,
        "source_best_variant": interpretation_manifest.get("source_best_variant"),
        "source_calibration_pack_id": calibration_manifest.get("calibration_pack_id"),
        "proposal_review_manifest_path": str(review_dir / "proposal_review_manifest.json"),
        "proposal_decision_matrix_path": str(review_dir / "proposal_decision_matrix.json"),
        "owner_approval_checklist_path": str(review_dir / "owner_approval_checklist.md"),
        "advisory_proposal_review_report_path": str(
            review_dir / "advisory_proposal_review_report.md"
        ),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "auto_apply": False,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(review_dir / "proposal_review_manifest.json", manifest)
    _write_json(review_dir / "proposal_decision_matrix.json", decision_matrix)
    _write_text(review_dir / "owner_approval_checklist.md", checklist)
    _write_text(
        review_dir / "advisory_proposal_review_report.md",
        render_advisory_proposal_review_report(manifest, decision_matrix, defensive_summary),
    )
    _write_text(review_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_advisory_proposal_review",
        review_dir.name,
        review_dir / "proposal_review_manifest.json",
    )
    return {
        "proposal_review_id": review_dir.name,
        "proposal_review_dir": review_dir,
        "manifest": manifest,
        "proposal_decision_matrix": decision_matrix,
        "owner_approval_checklist": checklist,
        "reader_brief_section": reader_brief,
    }


def advisory_proposal_review_report_payload(
    *,
    proposal_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=proposal_review_id if not latest else None,
        pointer_name="latest_advisory_proposal_review",
    )
    return {
        **_read_json(review_dir / "proposal_review_manifest.json"),
        "proposal_decision_matrix": _read_json(review_dir / "proposal_decision_matrix.json"),
        "reader_brief_section": _read_text(review_dir / "reader_brief_section.md"),
        "proposal_review_dir": str(review_dir),
    }


def validate_advisory_proposal_review_artifact(
    *,
    proposal_review_id: str,
    output_dir: Path = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = output_dir / proposal_review_id
    manifest = _read_optional_json(review_dir / "proposal_review_manifest.json") or {}
    matrix = _read_optional_json(review_dir / "proposal_decision_matrix.json") or {}
    proposals = _records(matrix.get("proposals"))
    checks = [
        _check("manifest_exists", (review_dir / "proposal_review_manifest.json").exists(), ""),
        _check("matrix_exists", (review_dir / "proposal_decision_matrix.json").exists(), ""),
        _check(
            "checklist_exists",
            (review_dir / "owner_approval_checklist.md").exists(),
            "",
        ),
        _check(
            "report_exists",
            (review_dir / "advisory_proposal_review_report.md").exists(),
            "",
        ),
        _check("reader_brief_exists", (review_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "proposal_review_id_matches",
            manifest.get("proposal_review_id") == proposal_review_id,
            "",
        ),
        _check(
            "decision_values_valid",
            all(row.get("decision") in PROPOSAL_REVIEW_DECISIONS for row in proposals),
            "decisions",
        ),
        _check(
            "auto_apply_false",
            manifest.get("auto_apply") is False
            and all(row.get("auto_apply") is False for row in proposals),
            "auto apply false",
        ),
        _check(
            "owner_approval_required",
            manifest.get("owner_approval_required") is True
            and any(row.get("owner_approval_required") is True for row in proposals),
            "owner approval required",
        ),
        _check(
            "position_policy_not_modified",
            matrix.get("position_advisory_config_mutated") is False,
            "no position_advisory_v1 mutation",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_advisory_proposal_review_validation",
        artifact_id_key="proposal_review_id",
        artifact_id=proposal_review_id,
        checks=checks,
    )


def run_forward_confirmation_plan(
    *,
    proposal_review_id: str,
    bridge_id: str,
    proposal_review_dir: Path = DEFAULT_ADVISORY_PROPOSAL_REVIEW_DIR,
    bridge_dir: Path = DEFAULT_BACKTEST_SIM_FORWARD_BRIDGE_DIR,
    output_dir: Path = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    review_manifest = _read_json(
        proposal_review_dir / proposal_review_id / "proposal_review_manifest.json"
    )
    decision_matrix = _read_json(
        proposal_review_dir / proposal_review_id / "proposal_decision_matrix.json"
    )
    bridge_manifest = _read_json(bridge_dir / bridge_id / "sim_forward_bridge_manifest.json")
    bridge_targets = _read_json(bridge_dir / bridge_id / "forward_confirmation_targets.json")
    targets = _confirmation_targets(decision_matrix, bridge_targets)
    trigger_conditions = _confirmation_trigger_conditions(targets)
    failure_conditions = _confirmation_failure_conditions()
    confirmation_plan_id = _stable_id(
        "forward-confirmation-plan",
        proposal_review_id,
        bridge_id,
        generated.isoformat(),
    )
    plan_dir = _unique_dir(output_dir / confirmation_plan_id)
    plan_dir.mkdir(parents=True, exist_ok=False)
    reader_brief = render_forward_confirmation_plan_reader_brief(targets, trigger_conditions)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_confirmation_plan_manifest",
        "confirmation_plan_id": plan_dir.name,
        "proposal_review_id": proposal_review_id,
        "bridge_id": bridge_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "market_regime": "ai_after_chatgpt",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "not_for_production": True,
        "source_bridge_next_action": bridge_manifest.get("next_action"),
        "source_review_status": review_manifest.get("status"),
        "confirmation_plan_manifest_path": str(plan_dir / "confirmation_plan_manifest.json"),
        "confirmation_targets_path": str(plan_dir / "confirmation_targets.json"),
        "trigger_conditions_path": str(plan_dir / "trigger_conditions.json"),
        "failure_conditions_path": str(plan_dir / "failure_conditions.json"),
        "forward_confirmation_plan_report_path": str(
            plan_dir / "forward_confirmation_plan_report.md"
        ),
        "reader_brief_section_path": str(plan_dir / "reader_brief_section.md"),
        "auto_apply": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(plan_dir / "confirmation_plan_manifest.json", manifest)
    _write_json(plan_dir / "confirmation_targets.json", targets)
    _write_json(plan_dir / "trigger_conditions.json", trigger_conditions)
    _write_json(plan_dir / "failure_conditions.json", failure_conditions)
    _write_text(
        plan_dir / "forward_confirmation_plan_report.md",
        render_forward_confirmation_plan_report(
            manifest,
            targets,
            trigger_conditions,
            failure_conditions,
        ),
    )
    _write_text(plan_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_forward_confirmation_plan",
        plan_dir.name,
        plan_dir / "confirmation_plan_manifest.json",
    )
    return {
        "confirmation_plan_id": plan_dir.name,
        "confirmation_plan_dir": plan_dir,
        "manifest": manifest,
        "confirmation_targets": targets,
        "trigger_conditions": trigger_conditions,
        "failure_conditions": failure_conditions,
        "reader_brief_section": reader_brief,
    }


def forward_confirmation_plan_report_payload(
    *,
    confirmation_plan_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
) -> dict[str, Any]:
    plan_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=confirmation_plan_id if not latest else None,
        pointer_name="latest_forward_confirmation_plan",
    )
    return {
        **_read_json(plan_dir / "confirmation_plan_manifest.json"),
        "confirmation_targets": _read_json(plan_dir / "confirmation_targets.json"),
        "trigger_conditions": _read_json(plan_dir / "trigger_conditions.json"),
        "failure_conditions": _read_json(plan_dir / "failure_conditions.json"),
        "reader_brief_section": _read_text(plan_dir / "reader_brief_section.md"),
        "confirmation_plan_dir": str(plan_dir),
    }


def validate_forward_confirmation_plan_artifact(
    *,
    confirmation_plan_id: str,
    output_dir: Path = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
) -> dict[str, Any]:
    plan_dir = output_dir / confirmation_plan_id
    manifest = _read_optional_json(plan_dir / "confirmation_plan_manifest.json") or {}
    targets = _read_optional_json(plan_dir / "confirmation_targets.json") or {}
    trigger_conditions = _read_optional_json(plan_dir / "trigger_conditions.json") or {}
    failure_conditions = _read_optional_json(plan_dir / "failure_conditions.json") or {}
    target_rows = _records(targets.get("targets"))
    checks = [
        _check("manifest_exists", (plan_dir / "confirmation_plan_manifest.json").exists(), ""),
        _check("targets_exists", (plan_dir / "confirmation_targets.json").exists(), ""),
        _check("triggers_exists", (plan_dir / "trigger_conditions.json").exists(), ""),
        _check("failures_exists", (plan_dir / "failure_conditions.json").exists(), ""),
        _check(
            "report_exists",
            (plan_dir / "forward_confirmation_plan_report.md").exists(),
            "",
        ),
        _check("reader_brief_exists", (plan_dir / "reader_brief_section.md").exists(), ""),
        _check(
            "confirmation_plan_id_matches",
            manifest.get("confirmation_plan_id") == confirmation_plan_id,
            "",
        ),
        _check(
            "required_targets_present",
            {row.get("target_id") for row in target_rows}
            >= {
                "limited_adjustment_vs_no_trade",
                "defensive_limited_adjustment_drawdown",
                "consensus_target_risk",
            },
            "targets",
        ),
        _check(
            "trigger_conditions_readable",
            bool(_records(trigger_conditions.get("calibration_ready_conditions")))
            and bool(_records(trigger_conditions.get("calibration_not_ready_conditions"))),
            "trigger conditions",
        ),
        _check(
            "failure_conditions_readable",
            bool(_records(failure_conditions.get("failure_conditions"))),
            "failure conditions",
        ),
        _check("no_auto_policy", manifest.get("auto_policy_apply") is False, ""),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_forward_confirmation_plan_validation",
        artifact_id_key="confirmation_plan_id",
        artifact_id=confirmation_plan_id,
        checks=checks,
    )


def render_event_generation_report(
    manifest: Mapping[str, Any], events: Sequence[Mapping[str, Any]]
) -> str:
    regimes = Counter(_text(row.get("regime_label")) for row in events)
    requested_range = f"{manifest.get('requested_start')} 至 {manifest.get('requested_end')}"
    status_counts = (
        f"{manifest.get('ready_count')} / {manifest.get('skipped_count')} / "
        f"{manifest.get('insufficient_data_count')}"
    )
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation event generation",
            "",
            f"- 标签：{REPORT_LABEL_BACKTEST_SIMULATION}",
            f"- event_set_id：{manifest.get('event_set_id')}",
            f"- market_regime：{manifest.get('market_regime')}",
            f"- 请求区间：{requested_range}",
            f"- events：{manifest.get('event_count')}",
            f"- READY / SKIPPED / INSUFFICIENT_DATA：{status_counts}",
            f"- regime 分布：{dict(sorted(regimes.items()))}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 结论：所有事件均为 BACKTEST_SIMULATION，pit_safety_status=SIMULATION_NOT_PIT。",
            "- broker action：false；production_effect=none；auto_policy_apply=false。",
        ]
    )


def render_variant_report(manifest: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> str:
    variants = Counter(_text(row.get("variant")) for row in rows)
    return "\n".join(
        [
            "# Dynamic v3 simulated advisory variants",
            "",
            f"- variant_set_id：{manifest.get('variant_set_id')}",
            f"- event_set_id：{manifest.get('event_set_id')}",
            f"- variants：{dict(sorted(variants.items()))}",
            f"- READY：{manifest.get('ready_count')}",
            "- broker_action_taken=false",
            "- 所有 variants 仅供 BACKTEST_SIMULATION 研究，不进入生产。",
        ]
    )


def render_outcome_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    limited = _variant_summary_row(summary, "limited_adjustment")
    defensive = _variant_summary_row(summary, "defensive_limited_adjustment")
    consensus = _variant_summary_row(summary, "consensus_target")
    outcome_counts = (
        f"{manifest.get('available_count')} / {manifest.get('pending_count')} / "
        f"{manifest.get('insufficient_data_count')}"
    )
    limited_relative = limited.get("avg_relative_to_no_trade_5d")
    defensive_drawdown = defensive.get("avg_max_drawdown_20d")
    consensus_drawdown = consensus.get("avg_max_drawdown_20d")
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation outcome",
            "",
            f"- sim_outcome_id：{manifest.get('sim_outcome_id')}",
            f"- status：{manifest.get('status')}",
            f"- available / pending / insufficient：{outcome_counts}",
            f"- best_variant：{manifest.get('best_variant')}",
            f"- limited_adjustment vs no_trade 5d：{limited_relative}",
            f"- defensive_limited_adjustment 20d drawdown：{defensive_drawdown}",
            f"- consensus_target 20d drawdown：{consensus_drawdown}",
            "- 该结果是 BACKTEST_SIMULATION，不是 PIT replay，也不是 production evidence。",
        ]
    )


def render_paper_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation paper portfolio",
            "",
            f"- sim_paper_id：{manifest.get('sim_paper_id')}",
            f"- variant：{summary.get('variant')}",
            f"- total_return：{summary.get('total_return')}",
            f"- max_drawdown：{summary.get('max_drawdown')}",
            f"- turnover：{summary.get('turnover')}",
            f"- relative_to_no_trade：{summary.get('relative_to_no_trade')}",
            "- broker_action_taken=false；production_effect=none。",
        ]
    )


def render_regime_report(manifest: Mapping[str, Any], summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation regime review",
            "",
            f"- regime_review_id：{manifest.get('regime_review_id')}",
            f"- status：{manifest.get('status')}",
            f"- best_variant_by_regime：{summary.get('best_variant_by_regime')}",
            f"- tech_drawdown：{summary.get('tech_drawdown')}",
            f"- semiconductor_pullback：{summary.get('semiconductor_pullback')}",
            "- regime-specific 结论需要 forward tracking 继续确认。",
        ]
    )


def render_sensitivity_report(manifest: Mapping[str, Any], warnings: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation sensitivity diagnostics",
            "",
            f"- sensitivity_id：{manifest.get('sensitivity_id')}",
            f"- simulation_overfit_status：{warnings.get('simulation_overfit_status')}",
            f"- sensitive_parameters：{warnings.get('sensitive_parameters')}",
            f"- regime_return_concentration：{warnings.get('regime_return_concentration')}",
            f"- strong_calibration_allowed：{warnings.get('strong_calibration_allowed')}",
            "- HIGH_RISK 时不允许产生 strong calibration proposal。",
        ]
    )


def render_calibration_report(
    manifest: Mapping[str, Any],
    evidence: Mapping[str, Any],
    proposals: Mapping[str, Any],
) -> str:
    proposal_ids = [row.get("proposal_id") for row in _records(proposals.get("proposals"))]
    limited_relative = evidence.get("limited_adjustment_vs_no_trade_5d")
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation calibration pack",
            "",
            f"- calibration_pack_id：{manifest.get('calibration_pack_id')}",
            f"- limited_adjustment_vs_no_trade_5d：{limited_relative}",
            f"- overfit_status：{evidence.get('simulation_overfit_status')}",
            f"- proposals：{proposal_ids}",
            "- auto_apply=false；can_trigger_production=false。",
            "- 配置是否调整仍需 owner approval 与 forward confirmation。",
        ]
    )


def render_calibration_reader_brief(
    evidence: Mapping[str, Any], proposals: Mapping[str, Any]
) -> str:
    proposal_ids = ", ".join(
        row.get("proposal_id", "") for row in _records(proposals.get("proposals"))
    )
    return "\n".join(
        [
            "## Dynamic v3 Backtest Simulation Calibration",
            "",
            f"- status：{evidence.get('calibration_readiness')}",
            f"- best_variant：{evidence.get('best_variant')}",
            f"- overfit_status：{evidence.get('simulation_overfit_status')}",
            f"- proposals：{proposal_ids}",
            "- production_effect=none；auto_apply=false；requires_forward_confirmation=true。",
        ]
    )


def render_forward_bridge_report(manifest: Mapping[str, Any], targets: Mapping[str, Any]) -> str:
    target_ids = [row.get("target") for row in _records(targets.get("targets"))]
    return "\n".join(
        [
            "# Dynamic v3 backtest simulation forward bridge",
            "",
            f"- bridge_id：{manifest.get('bridge_id')}",
            f"- next_action：{manifest.get('next_action')}",
            f"- targets：{target_ids}",
            "- 下周 / 下月 review 继续比较 limited、defensive 与 no_trade。",
            "- no broker / no production：true。",
        ]
    )


def render_forward_bridge_reader_brief(targets: Mapping[str, Any]) -> str:
    target_ids = ", ".join(row.get("target", "") for row in _records(targets.get("targets")))
    return "\n".join(
        [
            "## Dynamic v3 Simulation-to-Forward Bridge",
            "",
            f"- forward_confirmation_targets：{target_ids}",
            "- next_action：continue_forward_tracking",
            "- broker_action_allowed=false；production_effect=none。",
        ]
    )


def render_sim_interpretation_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    findings: Mapping[str, Any],
) -> str:
    variants = _records(matrix.get("variants"))
    finding_rows = _records(findings.get("findings"))
    return "\n".join(
        [
            "# Dynamic v3 Simulation Interpretation",
            "",
            f"- interpretation_id: {manifest.get('interpretation_id')}",
            f"- outcome_id: {manifest.get('outcome_id')}",
            f"- calibration_id: {manifest.get('calibration_id')}",
            f"- bridge_id: {manifest.get('bridge_id')}",
            f"- market_regime: {manifest.get('market_regime')}",
            f"- evidence_label: {REPORT_LABEL_BACKTEST_SIMULATION}",
            (
                "- 结论边界：本报告解释 BACKTEST_SIMULATION，"
                "不是 PIT / forward / production evidence。"
            ),
            "",
            "## Variant Roles",
            "",
            *[
                (
                    f"- {row.get('variant')}: role={row.get('role')}; "
                    f"return={_mapping(row.get('return_profile')).get('five_to_twenty_days')}; "
                    f"risk={_mapping(row.get('risk_profile')).get('drawdown')}; "
                    f"usage={row.get('recommended_usage')}"
                )
                for row in variants
            ],
            "",
            "## Key Findings",
            "",
            *[
                (
                    f"- {row.get('finding_id')}: {row.get('summary')} "
                    f"confidence={row.get('confidence')}; limitations="
                    f"{', '.join(_texts(row.get('limitations')))}"
                )
                for row in finding_rows
            ],
            "",
            "## Review Answers",
            "",
            f"- overall best variant: {manifest.get('source_best_variant')}",
            (
                "- lowest-risk variant: no_trade, because it has zero turnover "
                "and avoids simulated active exposure."
            ),
            "- limited_adjustment: medium-horizon active tilt, not a defensive rule.",
            "- consensus_target: upper-bound reference, not default execution.",
            "- defensive_limited_adjustment: defensive behavior is not proven by overall ranking.",
            (
                "- all conclusions remain BACKTEST_SIMULATION_NOT_PIT "
                "and require forward confirmation."
            ),
        ]
    )


def render_risk_return_report(
    manifest: Mapping[str, Any],
    table: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    rows = _records(summary.get("summary"))
    return "\n".join(
        [
            "# Dynamic v3 Simulation Risk-Return Review",
            "",
            f"- risk_return_id: {manifest.get('risk_return_id')}",
            f"- outcome_id: {manifest.get('outcome_id')}",
            f"- evidence_label: {REPORT_LABEL_BACKTEST_SIMULATION}",
            "- 解释边界：收益改善和风险改善分开判断；收益改善不会自动触发规则修改。",
            "",
            "## Active Variant Tradeoff",
            "",
            *[
                (
                    f"- {row.get('variant')}: status={row.get('risk_return_status')}; "
                    f"20d_return_improvement_pp={row.get('return_improvement_20d_pp')}; "
                    f"drawdown_worsening_pp={row.get('drawdown_worsening_20d_pp')}; "
                    f"return_per_drawdown_worsening="
                    f"{row.get('return_per_drawdown_worsening')}"
                )
                for row in rows
            ],
            "",
            "## Review Answers",
            "",
            "- active variants 的收益提升尚不能单独补偿 drawdown 加深；需要 forward confirmation。",
            "- limited_adjustment 的补偿关系见 return_per_drawdown_worsening。",
            "- consensus_target 更像更高风险暴露的 upper-bound reference。",
            (
                "- defensive_limited_adjustment 是否改善 risk-adjusted profile "
                "由本表和 regime validation 共同判断。"
            ),
            "- 若没有 RETURN_IMPROVES_RISK_IMPROVES，不能把 active variant 改成默认执行。",
            f"- table_rows: {len(table)}",
        ]
    )


def render_defensive_validation_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    matrix_rows: Sequence[Mapping[str, Any]],
) -> str:
    return "\n".join(
        [
            "# Dynamic v3 Defensive Variant Validation",
            "",
            f"- defensive_validation_id: {manifest.get('defensive_validation_id')}",
            f"- outcome_id: {manifest.get('outcome_id')}",
            f"- evidence_label: {REPORT_LABEL_BACKTEST_SIMULATION}",
            f"- defensive_limited_adjustment_status: "
            f"{summary.get('defensive_limited_adjustment_status')}",
            f"- recommendation: {summary.get('recommendation')}",
            "",
            "## Regime Results",
            "",
            *[
                (
                    f"- {row.get('regime')}: best_variant={row.get('best_variant')}; "
                    f"sample_count={row.get('sample_count')}; "
                    f"defensive_status="
                    f"{_mapping(row.get('defensive_limited_adjustment')).get('status')}; "
                    f"avg_relative_to_no_trade="
                    f"{_mapping(row.get('defensive_limited_adjustment')).get('avg_relative_to_no_trade')}"
                )
                for row in matrix_rows
            ],
            "",
            "## Review Answers",
            "",
            "- defensive_limited_adjustment 不能因 overall best 自动判定为 defensive。",
            "- tech_drawdown 下若 no_trade 更好，说明主动暴露没有降低压力窗口损失。",
            "- risk_off 样本不足时只能保持观察，不得给强结论。",
            "- semiconductor_pullback 样本不足或表现不稳时不支持默认防守规则。",
            "- 若继续失败，应考虑改名或重新分类为 active variant。",
        ]
    )


def render_owner_approval_checklist(
    decision_matrix: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Owner Approval Checklist",
            "",
            "- [ ] 是否接受 limited_adjustment 继续作为默认观察规则？",
            "- [ ] 是否同意不自动使用 consensus_target？",
            "- [ ] 是否同意 defensive_limited_adjustment 暂不认定为 defensive？",
            "- [ ] 是否要求 forward confirmation 后再改规则？",
            "- [ ] 是否继续保持 no broker / no production？",
            "- [ ] 是否需要重命名 defensive_limited_adjustment？",
            "",
            f"auto_apply: {decision_matrix.get('auto_apply')}",
            f"owner_approval_required: {decision_matrix.get('owner_approval_required')}",
            "defensive_limited_adjustment_status: "
            f"{defensive_summary.get('defensive_limited_adjustment_status')}",
        ]
    )


def render_proposal_review_reader_brief(
    decision_matrix: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    decisions = ", ".join(
        f"{row.get('proposal_id')}={row.get('decision')}"
        for row in _records(decision_matrix.get("proposals"))
    )
    return "\n".join(
        [
            "## Dynamic Rescue Advisory Proposal Review",
            "",
            f"- proposal_decisions: {decisions}",
            f"- defensive_status: {defensive_summary.get('defensive_limited_adjustment_status')}",
            "- auto_apply=false；owner_approval_required=true。",
            "- no broker / no production / no position_advisory_v1.yaml mutation。",
        ]
    )


def render_advisory_proposal_review_report(
    manifest: Mapping[str, Any],
    decision_matrix: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    proposals = _records(decision_matrix.get("proposals"))
    return "\n".join(
        [
            "# Dynamic v3 Advisory Proposal Review",
            "",
            f"- proposal_review_id: {manifest.get('proposal_review_id')}",
            f"- calibration_id: {manifest.get('calibration_id')}",
            f"- evidence_label: {REPORT_LABEL_BACKTEST_SIMULATION}",
            f"- auto_apply: {decision_matrix.get('auto_apply')}",
            f"- owner_approval_required: {decision_matrix.get('owner_approval_required')}",
            "",
            "## Proposal Decisions",
            "",
            *[
                (
                    f"- {row.get('proposal_id')}: decision={row.get('decision')}; "
                    f"reason={row.get('reason')}"
                )
                for row in proposals
            ],
            "",
            "## Review Answers",
            "",
            "- 当前 proposals 只接受为 observation / forward confirmation，不自动修改配置。",
            (
                "- 不自动修改配置的原因：simulation 不是 PIT-safe，"
                "active variants 仍有 drawdown worsening。"
            ),
            (
                "- 下一轮 calibration 需要 forward win rate、relative return "
                "和 drawdown 条件同时达标。"
            ),
            (
                "- owner 需要确认 observation、consensus target、defensive naming "
                "和 no production 边界。"
            ),
            "- Reader Brief 展示 proposal decisions、defensive status 和 safety boundary。",
            f"- defensive_limited_adjustment_status: "
            f"{defensive_summary.get('defensive_limited_adjustment_status')}",
        ]
    )


def render_forward_confirmation_plan_reader_brief(
    targets: Mapping[str, Any],
    trigger_conditions: Mapping[str, Any],
) -> str:
    target_ids = ", ".join(row.get("target_id", "") for row in _records(targets.get("targets")))
    ready = ", ".join(
        row.get("condition", "")
        for row in _records(trigger_conditions.get("calibration_ready_conditions"))
    )
    return "\n".join(
        [
            "## Dynamic Rescue Forward Confirmation Plan",
            "",
            f"- targets: {target_ids}",
            f"- calibration_readiness: {ready}",
            (
                "- limited_adjustment confirmation、defensive drawdown confirmation "
                "和 consensus risk watch 均需 forward evidence。"
            ),
            "- auto_apply=false；production_effect=none；broker_action_allowed=false。",
        ]
    )


def render_forward_confirmation_plan_report(
    manifest: Mapping[str, Any],
    targets: Mapping[str, Any],
    trigger_conditions: Mapping[str, Any],
    failure_conditions: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            "# Dynamic v3 Forward Confirmation Plan",
            "",
            f"- confirmation_plan_id: {manifest.get('confirmation_plan_id')}",
            f"- proposal_review_id: {manifest.get('proposal_review_id')}",
            f"- bridge_id: {manifest.get('bridge_id')}",
            f"- evidence_label: {REPORT_LABEL_BACKTEST_SIMULATION}",
            "",
            "## Confirmation Targets",
            "",
            *[
                (
                    f"- {row.get('target_id')}: priority={row.get('priority')}; "
                    f"windows={row.get('windows')}; status={row.get('current_status')}; "
                    f"reason={row.get('reason')}"
                )
                for row in _records(targets.get("targets"))
            ],
            "",
            "## Trigger Conditions",
            "",
            *[
                f"- ready: {row.get('condition')} requires={row.get('requires')}"
                for row in _records(trigger_conditions.get("calibration_ready_conditions"))
            ],
            *[
                f"- not_ready: {row.get('condition')} action={row.get('action')}"
                for row in _records(trigger_conditions.get("calibration_not_ready_conditions"))
            ],
            "",
            "## Failure Conditions",
            "",
            *[
                (
                    f"- {row.get('target')}: condition={row.get('condition')}; "
                    f"action={row.get('action')}"
                )
                for row in _records(failure_conditions.get("failure_conditions"))
            ],
            "",
            "## Review Answers",
            "",
            (
                "- forward tracking 需要验证 limited_adjustment、"
                "defensive drawdown 和 consensus risk。"
            ),
            "- 每个信号的 sample floor 写在 confirmation_targets.json。",
            "- 成功标准和失败标准分别写入 trigger_conditions.json 与 failure_conditions.json。",
            "- 只有 ready conditions 满足后才允许重新讨论 rule calibration。",
            "- drawdown worsening 持续时必须继续保持当前规则。",
        ]
    )


def _variant_interpretation_matrix(outcome_summary: Mapping[str, Any]) -> dict[str, Any]:
    no_trade = _variant_summary_row(outcome_summary, "no_trade")
    variants = []
    for variant in BACKTEST_SIM_VARIANTS:
        row = _variant_summary_row(outcome_summary, variant)
        variants.append(
            {
                "variant": variant,
                "role": _variant_role(variant),
                "return_profile": _variant_return_profile(row, no_trade),
                "risk_profile": _variant_risk_profile(row, no_trade),
                "interpretation": _variant_interpretation_text(variant, row, no_trade),
                "recommended_usage": _variant_recommended_usage(variant),
                "not_recommended_usage": _variant_not_recommended_usage(variant),
                "evidence_label": REPORT_LABEL_BACKTEST_SIMULATION,
                "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_interpretation_matrix",
        "variants": variants,
        "report_label": REPORT_LABEL_BACKTEST_SIMULATION,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _variant_role(variant: str) -> str:
    return {
        "no_trade": "passive_reference",
        "limited_adjustment": "risk_aware_active_tilt",
        "consensus_target": "upper_bound_reference",
        "defensive_limited_adjustment": "unproven_defensive_active_tilt",
        "equal_weight_shadow_candidates": "diversification_shadow_reference",
    }.get(variant, "simulation_reference")


def _variant_return_profile(
    row: Mapping[str, Any],
    no_trade: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "one_day": _return_compare(row, no_trade, "avg_1d_return"),
        "five_to_twenty_days": _medium_horizon_compare(row, no_trade),
        "avg_1d_return": row.get("avg_1d_return", 0.0),
        "avg_5d_return": row.get("avg_5d_return", 0.0),
        "avg_10d_return": row.get("avg_10d_return", 0.0),
        "avg_20d_return": row.get("avg_20d_return", 0.0),
    }


def _variant_risk_profile(
    row: Mapping[str, Any],
    no_trade: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "drawdown": _drawdown_compare(row, no_trade),
        "turnover": _turnover_compare(row, no_trade),
        "avg_max_drawdown_20d": row.get("avg_max_drawdown_20d", 0.0),
        "avg_turnover": row.get("avg_turnover", 0.0),
    }


def _variant_interpretation_text(
    variant: str,
    row: Mapping[str, Any],
    no_trade: Mapping[str, Any],
) -> str:
    if variant == "no_trade":
        return "no_trade is the lowest-intervention reference for comparing active variants."
    if variant == "limited_adjustment":
        return (
            "limited_adjustment appears to add medium-horizon active exposure rather than "
            "reduce risk."
        )
    if variant == "consensus_target":
        return "consensus_target is an upper-bound reference for full simulated consensus exposure."
    if variant == "defensive_limited_adjustment":
        return (
            "defensive_limited_adjustment requires regime-specific proof before it can be "
            "called defensive."
        )
    delta = _float(row.get("avg_20d_return")) - _float(no_trade.get("avg_20d_return"))
    return f"simulation reference variant; 20d delta vs no_trade={round(delta, 6)}."


def _variant_recommended_usage(variant: str) -> str:
    return {
        "no_trade": "risk_reference",
        "limited_adjustment": "continue_observation",
        "consensus_target": "upper_bound_reference_only",
        "defensive_limited_adjustment": "regime_specific_validation_only",
        "equal_weight_shadow_candidates": "shadow_reference_only",
    }.get(variant, "review_only")


def _variant_not_recommended_usage(variant: str) -> str:
    return {
        "no_trade": "do_not_treat_as_active_alpha",
        "limited_adjustment": "do_not_treat_as_defensive_rule",
        "consensus_target": "do_not_use_as_default_execution",
        "defensive_limited_adjustment": "do_not_label_as_proven_defensive",
        "equal_weight_shadow_candidates": "do_not_promote_without_candidate_review",
    }.get(variant, "do_not_use_for_production")


def _return_compare(row: Mapping[str, Any], baseline: Mapping[str, Any], key: str) -> str:
    delta = _float(row.get(key)) - _float(baseline.get(key))
    if abs(delta) <= 0.000001:
        return "similar_to_no_trade"
    return "stronger_than_no_trade" if delta > 0 else "weaker_than_no_trade"


def _medium_horizon_compare(row: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    comparisons = [
        _float(row.get(key)) - _float(baseline.get(key))
        for key in ("avg_5d_return", "avg_10d_return", "avg_20d_return")
    ]
    positive = sum(1 for value in comparisons if value > 0.000001)
    negative = sum(1 for value in comparisons if value < -0.000001)
    if positive == len(comparisons):
        return "stronger_than_no_trade"
    if negative == len(comparisons):
        return "weaker_than_no_trade"
    if positive:
        return "mixed_but_positive_medium_horizon"
    return "similar_to_no_trade"


def _drawdown_compare(row: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    delta = _float(row.get("avg_max_drawdown_20d")) - _float(baseline.get("avg_max_drawdown_20d"))
    if abs(delta) <= 0.000001:
        return "similar_to_no_trade"
    return "better_than_no_trade" if delta > 0 else "worse_than_no_trade"


def _turnover_compare(row: Mapping[str, Any], baseline: Mapping[str, Any]) -> str:
    delta = _float(row.get("avg_turnover")) - _float(baseline.get("avg_turnover"))
    if abs(delta) <= 0.000001:
        return "similar_to_no_trade"
    return "higher_than_no_trade" if delta > 0 else "lower_than_no_trade"


def _sim_key_findings(
    *,
    outcome_summary: Mapping[str, Any],
    calibration_evidence: Mapping[str, Any],
    bridge_targets: Mapping[str, Any],
) -> dict[str, Any]:
    no_trade = _variant_summary_row(outcome_summary, "no_trade")
    active_rows = [
        _variant_summary_row(outcome_summary, variant) for variant in ACTIVE_SIM_VARIANTS
    ]
    medium_positive = [
        row
        for row in active_rows
        if _float(row.get("avg_20d_return")) > _float(no_trade.get("avg_20d_return"))
    ]
    drawdown_worse = [
        row
        for row in active_rows
        if _float(row.get("avg_max_drawdown_20d")) < _float(no_trade.get("avg_max_drawdown_20d"))
    ]
    findings = [
        {
            "finding_id": "active_variants_improve_medium_horizon_return",
            "summary": (
                "Active variants improve 5/10/20d average return versus no_trade "
                "when the 20d delta is positive."
            ),
            "evidence": [
                {
                    "variant": row.get("variant"),
                    "avg_20d_return": row.get("avg_20d_return"),
                    "no_trade_avg_20d_return": no_trade.get("avg_20d_return"),
                }
                for row in medium_positive
            ],
            "confidence": "MEDIUM" if medium_positive else "LOW",
            "limitations": [REPORT_LABEL_BACKTEST_SIMULATION, "drawdown may worsen"],
        },
        {
            "finding_id": "active_variants_worsen_drawdown",
            "summary": "Active variants can increase average 20d max drawdown versus no_trade.",
            "evidence": [
                {
                    "variant": row.get("variant"),
                    "avg_max_drawdown_20d": row.get("avg_max_drawdown_20d"),
                    "no_trade_avg_max_drawdown_20d": no_trade.get("avg_max_drawdown_20d"),
                }
                for row in drawdown_worse
            ],
            "confidence": "HIGH" if drawdown_worse else "MEDIUM",
            "limitations": [REPORT_LABEL_BACKTEST_SIMULATION],
        },
        {
            "finding_id": "consensus_target_is_upper_bound_reference",
            "summary": (
                "consensus_target should be read as an upper-bound reference "
                "rather than default execution."
            ),
            "evidence": [_variant_summary_row(outcome_summary, "consensus_target")],
            "confidence": "MEDIUM",
            "limitations": [REPORT_LABEL_BACKTEST_SIMULATION, "turnover and drawdown risk"],
        },
        {
            "finding_id": "defensive_variant_requires_regime_validation",
            "summary": (
                "defensive_limited_adjustment is not proven defensive by overall ranking alone."
            ),
            "evidence": [
                {
                    "best_variant": outcome_summary.get("best_variant"),
                    "defensive_drawdown_20d": calibration_evidence.get(
                        "defensive_limited_avg_drawdown_20d"
                    ),
                }
            ],
            "confidence": "MEDIUM",
            "limitations": [REPORT_LABEL_BACKTEST_SIMULATION, "pressure regime sample required"],
        },
        {
            "finding_id": "requires_forward_confirmation",
            "summary": (
                "BACKTEST_SIMULATION_NOT_PIT requires forward confirmation before rule calibration."
            ),
            "evidence": _records(bridge_targets.get("targets")),
            "confidence": "HIGH",
            "limitations": [REPORT_LABEL_BACKTEST_SIMULATION, "not PIT evidence"],
        },
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sim_interpretation_key_findings",
        "findings": findings,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _risk_return_tradeoff_table(outcome_summary: Mapping[str, Any]) -> list[dict[str, Any]]:
    no_trade = _variant_summary_row(outcome_summary, "no_trade")
    if not no_trade:
        return []
    rows = []
    for variant in ACTIVE_SIM_VARIANTS:
        row = _variant_summary_row(outcome_summary, variant)
        if not row:
            rows.append(_empty_risk_return_row(variant))
            continue
        return_delta_pp = _pp(
            _float(row.get("avg_20d_return")) - _float(no_trade.get("avg_20d_return"))
        )
        drawdown_delta_pp = _pp(
            _float(row.get("avg_max_drawdown_20d")) - _float(no_trade.get("avg_max_drawdown_20d"))
        )
        drawdown_worsening_pp = round(
            max(
                0.0,
                abs(_float(row.get("avg_max_drawdown_20d")))
                - abs(_float(no_trade.get("avg_max_drawdown_20d"))),
            )
            * 100,
            4,
        )
        turnover_delta_pp = _pp(
            _float(row.get("avg_turnover")) - _float(no_trade.get("avg_turnover"))
        )
        status = _risk_return_status(return_delta_pp, drawdown_delta_pp, row)
        rows.append(
            {
                "variant": variant,
                "avg_1d_return": row.get("avg_1d_return", 0.0),
                "avg_5d_return": row.get("avg_5d_return", 0.0),
                "avg_10d_return": row.get("avg_10d_return", 0.0),
                "avg_20d_return": row.get("avg_20d_return", 0.0),
                "delta_20d_return_vs_no_trade": return_delta_pp,
                "avg_max_drawdown_20d": row.get("avg_max_drawdown_20d", 0.0),
                "drawdown_delta_vs_no_trade": drawdown_delta_pp,
                "avg_turnover": row.get("avg_turnover", 0.0),
                "turnover_delta_vs_no_trade": turnover_delta_pp,
                "return_per_drawdown_worsening": _ratio(
                    return_delta_pp,
                    drawdown_worsening_pp,
                ),
                "return_per_turnover": _ratio(return_delta_pp, turnover_delta_pp),
                "risk_return_status": status,
                "drawdown_worsening_20d_pp": drawdown_worsening_pp,
            }
        )
    return rows


def _empty_risk_return_row(variant: str) -> dict[str, Any]:
    return {
        "variant": variant,
        "avg_1d_return": 0.0,
        "avg_5d_return": 0.0,
        "avg_10d_return": 0.0,
        "avg_20d_return": 0.0,
        "delta_20d_return_vs_no_trade": 0.0,
        "avg_max_drawdown_20d": 0.0,
        "drawdown_delta_vs_no_trade": 0.0,
        "avg_turnover": 0.0,
        "turnover_delta_vs_no_trade": 0.0,
        "return_per_drawdown_worsening": 0.0,
        "return_per_turnover": 0.0,
        "risk_return_status": "INSUFFICIENT_DATA",
        "drawdown_worsening_20d_pp": 0.0,
    }


def _risk_adjusted_summary(table: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = []
    for row in table:
        status = _text(row.get("risk_return_status"), "INSUFFICIENT_DATA")
        rows.append(
            {
                "variant": row.get("variant"),
                "return_improvement_20d_pp": row.get("delta_20d_return_vs_no_trade", 0.0),
                "drawdown_worsening_20d_pp": row.get("drawdown_worsening_20d_pp", 0.0),
                "turnover_increase_pp": max(0.0, _float(row.get("turnover_delta_vs_no_trade"))),
                "return_per_drawdown_worsening": row.get(
                    "return_per_drawdown_worsening",
                    0.0,
                ),
                "return_per_turnover": row.get("return_per_turnover", 0.0),
                "risk_return_status": status,
                "recommendation": (
                    "observe_not_execute"
                    if status == "RETURN_IMPROVES_RISK_WORSENS"
                    else "continue_review"
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_sim_risk_adjusted_summary",
        "summary": rows,
        "has_return_improves_risk_improves": any(
            row.get("risk_return_status") == "RETURN_IMPROVES_RISK_IMPROVES" for row in rows
        ),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _risk_return_status(
    return_delta_pp: float,
    drawdown_delta_pp: float,
    row: Mapping[str, Any],
) -> str:
    if _int(row.get("available_count")) <= 0:
        return "INSUFFICIENT_DATA"
    return_improves = return_delta_pp > 0
    risk_improves = drawdown_delta_pp >= 0
    if return_improves and risk_improves:
        return "RETURN_IMPROVES_RISK_IMPROVES"
    if return_improves and not risk_improves:
        return "RETURN_IMPROVES_RISK_WORSENS"
    if not return_improves and risk_improves:
        return "RETURN_WORSE_RISK_IMPROVES"
    return "RETURN_WORSE_RISK_WORSE"


def _defensive_regime_matrix(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    available = [
        row
        for row in rows
        if row.get("outcome_status") == "AVAILABLE" and _int(row.get("window_days")) in {5, 10, 20}
    ]
    by_variant_regime: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in available:
        key = (_text(row.get("regime_label"), "unknown"), _text(row.get("variant")))
        by_variant_regime[key].append(row)
    ordered_regimes = [
        "tech_drawdown",
        "risk_off",
        "semiconductor_pullback",
        "sideways_choppy",
        "strong_recovery",
        "ai_trend",
        "unknown",
    ]
    matrix = []
    for regime in ordered_regimes:
        defensive_rows = by_variant_regime.get((regime, "defensive_limited_adjustment"), [])
        no_trade_rows = by_variant_regime.get((regime, "no_trade"), [])
        rel = [_float(row.get("relative_to_no_trade")) for row in defensive_rows]
        drawdown_deltas = _aligned_drawdown_deltas(defensive_rows, no_trade_rows)
        sample_count = len({_text(row.get("sim_event_id")) for row in defensive_rows})
        avg_relative = round(_avg(rel), 6)
        avg_drawdown_delta = round(_avg(drawdown_deltas), 6)
        status = _defensive_regime_status(
            regime=regime,
            sample_count=sample_count,
            avg_relative=avg_relative,
            avg_drawdown_delta=avg_drawdown_delta,
        )
        best_variant = _best_regime_variant(by_variant_regime, regime)
        matrix.append(
            {
                "schema_version": SCHEMA_VERSION,
                "regime": regime,
                "sample_count": sample_count,
                "best_variant": best_variant,
                "defensive_limited_adjustment": {
                    "avg_return": round(
                        _avg([_float(row.get("return")) for row in defensive_rows]),
                        6,
                    ),
                    "avg_relative_to_no_trade": avg_relative,
                    "win_rate_vs_no_trade": (
                        round(sum(1 for value in rel if value > 0) / len(rel), 6) if rel else 0.0
                    ),
                    "avg_drawdown_delta_vs_no_trade": avg_drawdown_delta,
                    "status": status,
                },
                "conclusion": _defensive_regime_conclusion(regime, best_variant, status),
                "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
                "broker_action_taken": False,
            }
        )
    return matrix


def _aligned_drawdown_deltas(
    defensive_rows: Sequence[Mapping[str, Any]],
    no_trade_rows: Sequence[Mapping[str, Any]],
) -> list[float]:
    no_trade_by_key = {
        (_text(row.get("sim_event_id")), _int(row.get("window_days"))): row for row in no_trade_rows
    }
    deltas = []
    for row in defensive_rows:
        key = (_text(row.get("sim_event_id")), _int(row.get("window_days")))
        reference = no_trade_by_key.get(key)
        if reference is None:
            continue
        deltas.append(_float(row.get("max_drawdown")) - _float(reference.get("max_drawdown")))
    return deltas


def _defensive_regime_status(
    *,
    regime: str,
    sample_count: int,
    avg_relative: float,
    avg_drawdown_delta: float,
) -> str:
    if sample_count <= 0:
        return "INSUFFICIENT_DATA"
    if regime in DEFENSIVE_PRESSURE_REGIMES and sample_count < 5:
        return "INSUFFICIENT_SAMPLE"
    return_ok = avg_relative >= 0
    drawdown_ok = avg_drawdown_delta >= 0
    if return_ok and drawdown_ok:
        return "PROVEN_DEFENSIVE"
    if not return_ok and not drawdown_ok:
        return "FAILS_DEFENSIVE_EXPECTATION"
    return "PARTIALLY_DEFENSIVE"


def _best_regime_variant(
    by_variant_regime: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    regime: str,
) -> str:
    candidates = []
    for variant in BACKTEST_SIM_VARIANTS:
        rows = by_variant_regime.get((regime, variant), [])
        if rows:
            candidates.append(
                (
                    variant,
                    _avg([_float(row.get("return")) for row in rows]),
                )
            )
    if not candidates:
        return "INSUFFICIENT_DATA"
    return max(candidates, key=lambda item: item[1])[0]


def _defensive_regime_conclusion(regime: str, best_variant: str, status: str) -> str:
    if status == "INSUFFICIENT_DATA":
        return f"{regime} has no available defensive validation sample."
    if status == "INSUFFICIENT_SAMPLE":
        return f"{regime} sample is too small for defensive confirmation."
    if best_variant == "no_trade":
        return f"no_trade outperformed defensive_limited_adjustment in {regime}."
    if status == "PROVEN_DEFENSIVE":
        return f"defensive_limited_adjustment met return and drawdown checks in {regime}."
    if status == "PARTIALLY_DEFENSIVE":
        return f"defensive_limited_adjustment was mixed in {regime}."
    return f"defensive_limited_adjustment failed defensive expectations in {regime}."


def _defensive_failure_case(row: Mapping[str, Any]) -> dict[str, Any]:
    defensive = _mapping(row.get("defensive_limited_adjustment"))
    return {
        "schema_version": SCHEMA_VERSION,
        "regime": row.get("regime"),
        "sample_count": row.get("sample_count"),
        "best_variant": row.get("best_variant"),
        "avg_relative_to_no_trade": defensive.get("avg_relative_to_no_trade"),
        "avg_drawdown_delta_vs_no_trade": defensive.get("avg_drawdown_delta_vs_no_trade"),
        "reason": "defensive_limited_adjustment did not beat no_trade in pressure validation",
        "broker_action_taken": False,
    }


def _defensive_validation_summary(matrix_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regime_results = {
        _text(row.get("regime")): _mapping(row.get("defensive_limited_adjustment")).get("status")
        for row in matrix_rows
    }
    pressure_statuses = [
        _text(regime_results.get(regime), "INSUFFICIENT_DATA")
        for regime in sorted(DEFENSIVE_PRESSURE_REGIMES)
    ]
    if pressure_statuses and all(status == "PROVEN_DEFENSIVE" for status in pressure_statuses):
        overall = "PROVEN_DEFENSIVE"
        recommendation = "continue_defensive_label_under_forward_confirmation"
    elif any(status == "PARTIALLY_DEFENSIVE" for status in pressure_statuses):
        overall = "PARTIALLY_DEFENSIVE"
        recommendation = "continue_observation_without_default_defensive_claim"
    else:
        overall = "NOT_PROVEN_DEFENSIVE"
        recommendation = "rename_or_reclassify_as_active_variant"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_defensive_validation_summary",
        "defensive_limited_adjustment_status": overall,
        "regime_results": regime_results,
        "recommendation": recommendation,
        "requires_forward_confirmation": True,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _proposal_decision_matrix(
    *,
    proposals: Mapping[str, Any],
    risk_summary: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
    key_findings: Mapping[str, Any],
) -> dict[str, Any]:
    proposal_rows = []
    source_proposals = _records(proposals.get("proposals"))
    if not source_proposals:
        source_proposals = [
            {
                "proposal_id": "require_forward_confirmation",
                "proposal_type": "require_forward_confirmation",
            }
        ]
    for proposal in source_proposals:
        proposal_id = _text(proposal.get("proposal_id") or proposal.get("proposal_type"))
        proposal_rows.append(
            {
                "proposal_id": proposal_id,
                "decision": _proposal_decision(proposal_id),
                "auto_apply": False,
                "owner_approval_required": True,
                "reason": _proposal_review_reason(
                    proposal_id,
                    risk_summary=risk_summary,
                    defensive_summary=defensive_summary,
                ),
                "conditions": _proposal_review_conditions(proposal_id),
                "source_confidence": proposal.get("confidence", "MEDIUM"),
                "source_evidence_mode": proposal.get(
                    "evidence_mode",
                    OUTCOME_MODE_BACKTEST_SIMULATION,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_proposal_decision_matrix",
        "proposals": proposal_rows,
        "key_findings": [row.get("finding_id") for row in _records(key_findings.get("findings"))],
        "auto_apply": False,
        "owner_approval_required": True,
        "position_advisory_config_mutated": False,
        "production_candidate_generated": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _proposal_decision(proposal_id: str) -> str:
    if proposal_id == "keep_limited_adjustment_default":
        return "ACCEPT_FOR_OBSERVATION"
    if proposal_id == "require_forward_confirmation":
        return "ACCEPT"
    return "OWNER_REVIEW_REQUIRED"


def _proposal_review_reason(
    proposal_id: str,
    *,
    risk_summary: Mapping[str, Any],
    defensive_summary: Mapping[str, Any],
) -> str:
    if proposal_id == "keep_limited_adjustment_default":
        return (
            "limited_adjustment improves medium-horizon simulated return in some windows but "
            "risk-return review still requires observation before execution."
        )
    if proposal_id == "require_forward_confirmation":
        return "simulation is not PIT-safe and active variants can worsen drawdown."
    if proposal_id == "rename_defensive_limited_adjustment":
        return (
            "defensive_limited_adjustment_status="
            f"{defensive_summary.get('defensive_limited_adjustment_status')}."
        )
    statuses = [row.get("risk_return_status") for row in _records(risk_summary.get("summary"))]
    return f"owner review required; risk_return_statuses={statuses}."


def _proposal_review_conditions(proposal_id: str) -> list[str]:
    if proposal_id in {"keep_limited_adjustment_default", "require_forward_confirmation"}:
        return [
            "forward win_rate_vs_no_trade >= 0.55",
            "forward avg_relative_return >= 0",
            "forward drawdown_delta <= 0",
        ]
    return ["owner review required before any policy mutation"]


def _confirmation_targets(
    decision_matrix: Mapping[str, Any],
    bridge_targets: Mapping[str, Any],
) -> dict[str, Any]:
    bridge_by_id = {
        _text(row.get("target") or row.get("target_id")): row
        for row in _records(bridge_targets.get("targets"))
    }
    limited = bridge_by_id.get("limited_adjustment_vs_no_trade", {})
    defensive = bridge_by_id.get("defensive_limited_adjustment_drawdown", {})
    limited_criteria = _mapping(limited.get("success_criteria"))
    defensive_criteria = _mapping(defensive.get("success_criteria"))
    required_events = _int(limited.get("required_forward_events"), 10)
    win_rate_min = _float(limited_criteria.get("win_rate_vs_no_trade_min"), 0.55)
    avg_relative_min = _float(limited_criteria.get("avg_relative_return_min"), 0.0)
    drawdown_max = _float(
        limited_criteria.get("drawdown_delta_max")
        or limited_criteria.get("avg_drawdown_delta_max"),
        0.0,
    )
    targets = [
        {
            "target_id": "limited_adjustment_vs_no_trade",
            "priority": "HIGH",
            "windows": [1, 5, 10, 20],
            "required_forward_events": required_events,
            "success_criteria": {
                "win_rate_vs_no_trade_min": win_rate_min,
                "avg_relative_return_min": avg_relative_min,
                "drawdown_delta_max": drawdown_max,
            },
            "current_status": "IN_PROGRESS",
            "reason": (
                "Backtest simulation suggests medium-horizon return benefit but worse drawdown."
            ),
        },
        {
            "target_id": "defensive_limited_adjustment_drawdown",
            "priority": "HIGH",
            "windows": [5, 10, 20],
            "required_pressure_regime_events": max(
                5,
                _int(defensive.get("required_forward_events"), 5),
            ),
            "success_criteria": {
                "drawdown_delta_vs_no_trade_max": _float(
                    defensive_criteria.get("drawdown_delta_vs_no_trade_max")
                    or defensive_criteria.get("avg_drawdown_delta_max"),
                    0.0,
                ),
                "win_rate_vs_no_trade_min": 0.50,
            },
            "current_status": "IN_PROGRESS",
            "reason": "Defensive behavior not proven in simulation.",
        },
        {
            "target_id": "consensus_target_risk",
            "priority": "MEDIUM",
            "windows": [5, 10, 20],
            "required_forward_events": required_events,
            "success_criteria": {
                "drawdown_delta_vs_limited_adjustment_max": 0.0,
                "turnover_delta_max": 0.0,
            },
            "current_status": "WATCH_ONLY",
            "reason": "Consensus target acts as upper-bound reference, not default action.",
        },
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_targets",
        "targets": targets,
        "source_proposals": [
            row.get("proposal_id") for row in _records(decision_matrix.get("proposals"))
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _confirmation_trigger_conditions(targets: Mapping[str, Any]) -> dict[str, Any]:
    limited = next(
        (
            row
            for row in _records(targets.get("targets"))
            if row.get("target_id") == "limited_adjustment_vs_no_trade"
        ),
        {},
    )
    criteria = _mapping(limited.get("success_criteria"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_trigger_conditions",
        "calibration_ready_conditions": [
            {
                "condition": "limited_adjustment_forward_success",
                "requires": [
                    f"forward_events >= {limited.get('required_forward_events', 10)}",
                    f"win_rate_vs_no_trade >= {criteria.get('win_rate_vs_no_trade_min', 0.55)}",
                    f"avg_relative_return >= {criteria.get('avg_relative_return_min', 0.0)}",
                    f"drawdown_delta <= {criteria.get('drawdown_delta_max', 0.0)}",
                ],
            }
        ],
        "calibration_not_ready_conditions": [
            {
                "condition": "drawdown_worsening_persists",
                "requires": ["drawdown_delta > 0"],
                "action": "do_not_loosen_rules",
            },
            {
                "condition": "defensive_status_not_proven",
                "requires": ["pressure_regime_drawdown_delta > 0"],
                "action": "do_not_label_defensive_variant_as_defensive",
            },
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _confirmation_failure_conditions() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_failure_conditions",
        "failure_conditions": [
            {
                "target": "limited_adjustment_vs_no_trade",
                "condition": "underperforms_no_trade",
                "action": "tighten_or_disable_limited_adjustment_proposal",
            },
            {
                "target": "defensive_limited_adjustment_drawdown",
                "condition": "fails_to_reduce_drawdown_in_pressure_regime",
                "action": "rename_or_remove_defensive_label",
            },
            {
                "target": "consensus_target_risk",
                "condition": "excess_drawdown_persists",
                "action": "keep_consensus_target_as_reference_only",
            },
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _pp(value: float) -> float:
    return round(value * 100, 4)


def _ratio(numerator: float, denominator: float) -> float:
    return round(numerator / denominator, 4) if denominator > 0 else 0.0


def _records_as_ints(value: Any) -> list[int]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_int(item) for item in value]


def _records_as_floats(value: Any) -> list[float]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [_float(item) for item in value]


def _sim_event(
    *,
    event_date: date,
    config: Mapping[str, Any],
    candidates: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
    prices: pd.DataFrame,
    price_dates: Sequence[date],
    min_history: int,
) -> dict[str, Any]:
    history_count = sum(1 for item in price_dates if item < event_date)
    available = []
    candidate_weights = {}
    for candidate in candidates:
        weights = _mapping(_mapping(candidate.get("weights_by_date")).get(event_date.isoformat()))
        if weights:
            available.append(_text(candidate.get("candidate_id")))
            candidate_weights[_text(candidate.get("candidate_id"))] = weights
    skip_reasons = []
    if history_count < min_history:
        skip_reasons.append("INSUFFICIENT_HISTORY_BEFORE_EVENT")
    if not candidate_weights:
        skip_reasons.append("MISSING_CANDIDATE_WEIGHTS_FOR_EVENT_DATE")
    target = _average_weights(candidate_weights.values())
    dispersion = _weight_dispersion(candidate_weights.values())
    regime = _classify_regime(prices, event_date, config)
    status = "READY" if target and not skip_reasons else "INSUFFICIENT_DATA"
    source = _mapping(config.get("source"))
    return {
        "schema_version": SCHEMA_VERSION,
        "sim_event_id": _stable_id(
            "backtest-sim-event",
            source.get("shadow_shortlist_id"),
            event_date.isoformat(),
            target,
        ),
        "as_of": event_date.isoformat(),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "source_shadow_shortlist_id": _text(source.get("shadow_shortlist_id")),
        "source_candidate_ids": [_text(row.get("candidate_id")) for row in candidates],
        "available_candidates": available,
        "simulated_target_weights": target,
        "simulated_consensus_weights": target,
        "equal_weight_shadow_candidate_weights": target,
        "candidate_weights": candidate_weights,
        "baseline_weights": dict(baseline),
        "regime_label": regime,
        "consensus_max_symbol_dispersion": round(dispersion["max_symbol_dispersion"], 6),
        "consensus_average_dispersion": round(dispersion["average_dispersion"], 6),
        "high_consensus": _is_high_consensus(dispersion, config),
        "event_status": status,
        "skip_reasons": skip_reasons,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "auto_policy_apply": False,
        "production_effect": "none",
    }


def _variant_rows(
    *,
    events: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    enabled: Sequence[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows = []
    ledger = []
    state = (
        {
            variant: _normalize_weights(_mapping(events[0].get("baseline_weights")))
            for variant in enabled
        }
        if events
        else {}
    )
    for event in sorted(events, key=lambda row: _text(row.get("as_of"))):
        for variant in enabled:
            before = state.get(variant, _normalize_weights(_mapping(event.get("baseline_weights"))))
            target = _variant_target(event, variant)
            if event.get("event_status") != "READY" or not target:
                after = before
                status = "INSUFFICIENT_DATA"
                skip_reasons = _texts(event.get("skip_reasons")) or ["EVENT_NOT_READY"]
            else:
                after = _apply_variant(
                    before=before, target=target, event=event, variant=variant, config=config
                )
                status = "READY"
                skip_reasons = []
            deltas = _weight_deltas(before, after)
            turnover = round(sum(abs(value) for value in deltas.values()), 6)
            row = {
                "schema_version": SCHEMA_VERSION,
                "sim_event_id": event.get("sim_event_id"),
                "as_of": event.get("as_of"),
                "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
                "pit_safety_status": PIT_SAFETY_SIMULATION,
                "not_for_production": True,
                "regime_label": event.get("regime_label", "unknown"),
                "variant": variant,
                "before_weights": before,
                "target_weights": target,
                "after_weights": after,
                "deltas": deltas,
                "turnover": turnover,
                "risk_asset_exposure": round(_risk_exposure(after, config), 6),
                "semiconductor_exposure": round(_semiconductor_exposure(after, config), 6),
                "cash_exposure": round(_float(after.get(_cash_symbol(config))), 6),
                "variant_status": status,
                "event_status": event.get("event_status"),
                "skip_reasons": skip_reasons,
                "broker_action_allowed": False,
                "broker_action_taken": False,
                "auto_policy_apply": False,
                "production_effect": "none",
            }
            rows.append(row)
            ledger.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "sim_event_id": event.get("sim_event_id"),
                    "as_of": event.get("as_of"),
                    "variant": variant,
                    "action_type": _variant_action_type(variant),
                    "total_abs_adjustment": turnover,
                    "largest_symbol_delta": _largest_delta(deltas),
                    "reason": "simulation_variant_generation",
                    "limits_applied": _limits_applied(variant, turnover, config),
                    "not_for_production": True,
                    "broker_action_taken": False,
                    "production_effect": "none",
                }
            )
            state[variant] = after
    return rows, ledger


def _outcome_rows(
    *,
    variant_rows: Sequence[Mapping[str, Any]],
    windows: Sequence[int],
    prices: pd.DataFrame,
    price_dates: Sequence[date],
    generated_date: date,
) -> list[dict[str, Any]]:
    by_event: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in variant_rows:
        by_event[_text(row.get("sim_event_id"))].append(row)
    rows = []
    for sim_event_id, event_rows in sorted(by_event.items()):
        start = _date_from_any(event_rows[0].get("as_of"))
        if start is None:
            continue
        for window in windows:
            end = _nth_trading_date_after(price_dates, start, window)
            if end is None or end > generated_date:
                for variant_row in event_rows:
                    rows.append(
                        _pending_sim_outcome(variant_row, window=window, start=start, end=end)
                    )
                continue
            returns = {
                _text(row.get("variant")): (
                    _portfolio_metrics(prices, _mapping(row.get("after_weights")), start, end)
                    if row.get("variant_status") == "READY"
                    else _missing_metrics()
                )
                for row in event_rows
            }
            for variant_row in event_rows:
                variant = _text(variant_row.get("variant"))
                metrics = returns.get(variant, _missing_metrics())
                status = _text(metrics.get("status"), "INSUFFICIENT_DATA")
                rows.append(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "sim_event_id": sim_event_id,
                        "as_of": start.isoformat(),
                        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
                        "pit_safety_status": PIT_SAFETY_SIMULATION,
                        "not_for_production": True,
                        "regime_label": variant_row.get("regime_label", "unknown"),
                        "variant": variant,
                        "window_days": window,
                        "start_date": start.isoformat(),
                        "end_date": end.isoformat(),
                        "return": metrics.get("return") if status == "AVAILABLE" else None,
                        "relative_to_no_trade": _relative(metrics, returns, "no_trade", status),
                        "relative_to_consensus_target": _relative(
                            metrics, returns, "consensus_target", status
                        ),
                        "relative_to_limited_adjustment": _relative(
                            metrics, returns, "limited_adjustment", status
                        ),
                        "max_drawdown": (
                            metrics.get("max_drawdown") if status == "AVAILABLE" else None
                        ),
                        "realized_volatility": (
                            metrics.get("realized_volatility") if status == "AVAILABLE" else None
                        ),
                        "turnover": _float(variant_row.get("turnover")),
                        "outcome_status": status,
                        "broker_action_taken": False,
                        "production_effect": "none",
                    }
                )
    return rows


def _paper_history(
    rows: Sequence[Mapping[str, Any]], *, variant: str, prices: pd.DataFrame
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not rows:
        return [], [], _empty_paper_summary(variant)
    selected = sorted(rows, key=lambda row: _text(row.get("as_of")))
    current = _normalize_weights(_mapping(selected[0].get("before_weights")))
    previous_date = _date_from_any(selected[0].get("as_of"))
    if previous_date is None:
        return [], [], _empty_paper_summary(variant)
    start_date = previous_date
    value = 1.0
    peak = 1.0
    daily_returns = []
    history = []
    ledger = []
    for row in selected:
        event_date = _date_from_any(row.get("as_of"))
        if event_date is None:
            continue
        period_return = 0.0
        if event_date > previous_date:
            metrics = _portfolio_metrics(prices, current, previous_date, event_date)
            period_return = metrics["return"] if metrics["status"] == "AVAILABLE" else 0.0
            value = round(value * (1.0 + period_return), 8)
            daily_returns.append(period_return)
            peak = max(peak, value)
        after = (
            _normalize_weights(_mapping(row.get("after_weights")))
            if row.get("variant_status") == "READY"
            else current
        )
        deltas = _weight_deltas(current, after)
        turnover = round(sum(abs(delta) for delta in deltas.values()), 6)
        if turnover:
            ledger.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "date": event_date.isoformat(),
                    "variant": variant,
                    "before_weights": current,
                    "after_weights": after,
                    "deltas": deltas,
                    "turnover": turnover,
                    "reason": "backtest_sim_variant",
                    "source_sim_event_id": row.get("sim_event_id"),
                    "broker_action_taken": False,
                }
            )
        current = after
        drawdown = round(value / peak - 1.0, 6) if peak else 0.0
        history.append(
            {
                "schema_version": SCHEMA_VERSION,
                "date": event_date.isoformat(),
                "variant": variant,
                "weights": current,
                "portfolio_value": value,
                "daily_return": period_return,
                "drawdown": drawdown,
                "turnover": turnover,
                "source_sim_event_id": row.get("sim_event_id"),
                "broker_action_taken": False,
            }
        )
        previous_date = event_date
    end_date = _date_from_any(history[-1]["date"]) if history else start_date
    total_return = round(value - 1.0, 6)
    years = max((end_date - start_date).days / 365.25, 0.0)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "variant": variant,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_return": total_return,
        "annualized_return": (
            round(value ** (1 / years) - 1.0, 6) if years > 0 and value > 0 else 0.0
        ),
        "max_drawdown": round(min([item["drawdown"] for item in history] or [0.0]), 6),
        "realized_volatility": round(_realized_volatility(daily_returns), 6),
        "turnover": round(sum(_float(item.get("turnover")) for item in ledger), 6),
        "trade_count": len(ledger),
        "relative_to_no_trade": 0.0,
        "relative_to_baseline": 0.0,
        "simulation_status": "PASS" if history else "INSUFFICIENT_DATA",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }
    return history, ledger, summary


def _variant_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    summary = []
    by_variant: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_variant[_text(row.get("variant"))].append(row)
    for variant, variant_rows in sorted(by_variant.items()):
        available = [row for row in variant_rows if row.get("outcome_status") == "AVAILABLE"]
        rel_5 = _window_values(available, 5, "relative_to_no_trade")
        summary.append(
            {
                "variant": variant,
                "event_count": len({_text(row.get("sim_event_id")) for row in variant_rows}),
                "available_count": len(available),
                "avg_1d_return": _nullable_avg(_window_values(available, 1, "return")),
                "avg_5d_return": _nullable_avg(_window_values(available, 5, "return")),
                "avg_10d_return": _nullable_avg(_window_values(available, 10, "return")),
                "avg_20d_return": _nullable_avg(_window_values(available, 20, "return")),
                "avg_relative_to_no_trade_5d": _nullable_avg(rel_5),
                "win_rate_vs_no_trade_5d": (
                    round(sum(1 for value in rel_5 if value > 0) / len(rel_5), 6) if rel_5 else None
                ),
                "avg_max_drawdown_20d": _nullable_avg(
                    _window_values(available, 20, "max_drawdown")
                ),
                "avg_turnover": _nullable_avg([_float(row.get("turnover")) for row in available]),
            }
        )
    ranked = [
        row
        for row in summary
        if row["variant"] != "no_trade" and _finite_number(row.get("avg_relative_to_no_trade_5d"))
    ]
    best = (
        max(ranked, key=lambda row: row["avg_relative_to_no_trade_5d"])["variant"]
        if ranked
        else "MISSING"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_variant_summary",
        "summary": summary,
        "available_count": sum(row["available_count"] for row in summary),
        "best_variant": best,
        "limited_adjustment_vs_no_trade_5d": _variant_summary_row(
            {"summary": summary}, "limited_adjustment"
        ).get("avg_relative_to_no_trade_5d"),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _regime_metrics(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    result = []
    for regime in sorted(REGIME_BUCKETS):
        for variant in sorted({_text(row.get("variant")) for row in rows}):
            subset = [
                row
                for row in available
                if row.get("regime_label") == regime and row.get("variant") == variant
            ]
            rel = [_float(row.get("relative_to_no_trade")) for row in subset]
            result.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "regime": regime,
                    "variant": variant,
                    "event_count": len({_text(row.get("sim_event_id")) for row in subset}),
                    "avg_return": round(_avg([_float(row.get("return")) for row in subset]), 6),
                    "avg_relative_to_no_trade": round(_avg(rel), 6),
                    "win_rate_vs_no_trade": (
                        round(sum(1 for value in rel if value > 0) / len(rel), 6) if rel else 0.0
                    ),
                    "avg_drawdown": round(
                        _avg([_float(row.get("max_drawdown")) for row in subset]), 6
                    ),
                    "avg_turnover": round(_avg([_float(row.get("turnover")) for row in subset]), 6),
                    "status": "PASS" if subset else "INSUFFICIENT_DATA",
                    "broker_action_taken": False,
                }
            )
    return result


def _regime_inventory(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(_text(row.get("regime_label")) for row in rows)
    statuses = Counter(_text(row.get("outcome_status")) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_regime_window_inventory",
        "regime_counts": dict(sorted(counts.items())),
        "outcome_status_counts": dict(sorted(statuses.items())),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _regime_summary(metrics: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    best = {}
    for regime in sorted(REGIME_BUCKETS):
        subset = [
            row
            for row in metrics
            if row.get("regime") == regime and row.get("status") != "INSUFFICIENT_DATA"
        ]
        if subset:
            best[regime] = max(subset, key=lambda row: _float(row.get("avg_relative_to_no_trade")))[
                "variant"
            ]
        else:
            best[regime] = "INSUFFICIENT_DATA"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_regime_summary",
        "best_variant_by_regime": best,
        "tech_drawdown": best.get("tech_drawdown"),
        "semiconductor_pullback": best.get("semiconductor_pullback"),
        "risk_off": best.get("risk_off"),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _frequency_sensitivity(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    profiles = _texts(policy.get("event_frequency_profiles")) or ["weekly", "biweekly", "monthly"]
    limited_5d = [
        row
        for row in rows
        if row.get("variant") == "limited_adjustment"
        and row.get("window_days") == 5
        and row.get("outcome_status") == "AVAILABLE"
    ]
    by_date = defaultdict(list)
    for row in limited_5d:
        by_date[_text(row.get("as_of"))].append(row)
    dates = sorted(by_date)
    results = []
    for profile in profiles:
        selected_dates = _profile_dates(dates, profile)
        selected = [row for day in selected_dates for row in by_date[day]]
        results.append(
            {
                "profile": profile,
                "sample_count": len(selected),
                "avg_relative_to_no_trade_5d": round(
                    _avg([_float(row.get("relative_to_no_trade")) for row in selected]),
                    6,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_event_frequency_sensitivity",
        "results": results,
        "broker_action_taken": False,
    }


def _adjustment_limit_sensitivity(
    events: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
    prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    grid = _records_as_floats(policy.get("adjustment_limit_grid"))
    if not grid:
        grid = [0.05, 0.10, 0.15]
    price_dates = _available_price_dates(prices)
    results = []
    for limit in grid:
        cfg = _with_limit(config, limit)
        variant_rows, _ = _variant_rows(
            events=events,
            config=cfg,
            enabled=["no_trade", "limited_adjustment"],
        )
        outcome = _outcome_rows(
            variant_rows=variant_rows,
            windows=[5],
            prices=prices,
            price_dates=price_dates,
            generated_date=date.max,
        )
        selected = [
            row
            for row in outcome
            if row.get("variant") == "limited_adjustment"
            and row.get("outcome_status") == "AVAILABLE"
        ]
        results.append(
            {
                "max_single_event_total_adjustment": limit,
                "sample_count": len(selected),
                "avg_relative_to_no_trade_5d": round(
                    _avg([_float(row.get("relative_to_no_trade")) for row in selected]),
                    6,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_adjustment_limit_sensitivity",
        "results": results,
        "broker_action_taken": False,
    }


def _shortlist_sensitivity(
    events: Sequence[Mapping[str, Any]],
    prices: pd.DataFrame,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    grid = _records_as_ints(policy.get("shortlist_top_n_grid")) or [1, 2, 5]
    price_dates = _available_price_dates(prices)
    results = []
    for top_n in grid:
        rows = []
        for event in events:
            candidate_items = sorted(
                _mapping(event.get("candidate_weights")).items(),
                key=lambda item: item[0],
            )[:top_n]
            target = _average_weights([_mapping(weights) for _, weights in candidate_items])
            baseline = _normalize_weights(_mapping(event.get("baseline_weights")))
            if not target:
                continue
            rows.extend(
                [
                    {
                        "sim_event_id": event.get("sim_event_id"),
                        "as_of": event.get("as_of"),
                        "variant": "no_trade",
                        "after_weights": baseline,
                        "turnover": 0.0,
                        "variant_status": "READY",
                        "regime_label": event.get("regime_label"),
                    },
                    {
                        "sim_event_id": event.get("sim_event_id"),
                        "as_of": event.get("as_of"),
                        "variant": f"top_{top_n}_consensus",
                        "after_weights": target,
                        "turnover": sum(abs(v) for v in _weight_deltas(baseline, target).values()),
                        "variant_status": "READY",
                        "regime_label": event.get("regime_label"),
                    },
                ]
            )
        outcome = _outcome_rows(
            variant_rows=rows,
            windows=[5],
            prices=prices,
            price_dates=price_dates,
            generated_date=date.max,
        )
        selected = [
            row
            for row in outcome
            if row.get("variant") == f"top_{top_n}_consensus"
            and row.get("outcome_status") == "AVAILABLE"
        ]
        results.append(
            {
                "shortlist_top_n": top_n,
                "sample_count": len(selected),
                "avg_relative_to_no_trade_5d": round(
                    _avg([_float(row.get("relative_to_no_trade")) for row in selected]),
                    6,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_shortlist_sensitivity",
        "results": results,
        "broker_action_taken": False,
    }


def _threshold_sensitivity(
    rows: Sequence[Mapping[str, Any]], policy: Mapping[str, Any]
) -> dict[str, Any]:
    thresholds = _mapping(policy.get("consensus_dispersion_thresholds"))
    base = [
        row
        for row in rows
        if row.get("variant") == "limited_adjustment"
        and row.get("window_days") == 5
        and row.get("outcome_status") == "AVAILABLE"
    ]
    results = []
    for name, threshold in sorted(thresholds.items()):
        selected = [
            row
            for row in base
            if _float(row.get("consensus_max_symbol_dispersion"), default=0.0) <= _float(threshold)
            or "consensus_max_symbol_dispersion" not in row
        ]
        results.append(
            {
                "threshold_profile": name,
                "max_symbol_dispersion": _float(threshold),
                "sample_count": len(selected),
                "avg_relative_to_no_trade_5d": round(
                    _avg([_float(row.get("relative_to_no_trade")) for row in selected]),
                    6,
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_threshold_sensitivity",
        "results": results,
        "broker_action_taken": False,
    }


def _overfit_warnings(
    rows: Sequence[Mapping[str, Any]],
    frequency: Mapping[str, Any],
    adjustment: Mapping[str, Any],
    shortlist: Mapping[str, Any],
    threshold: Mapping[str, Any],
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    min_available = _float(policy.get("min_available_windows_for_low_risk"), 20)
    concentration = _regime_return_concentration(available)
    spreads = {
        "event_frequency": _result_spread(_records(frequency.get("results"))),
        "adjustment_limit": _result_spread(_records(adjustment.get("results"))),
        "shortlist": _result_spread(_records(shortlist.get("results"))),
        "consensus_threshold": _result_spread(_records(threshold.get("results"))),
    }
    sensitive = [
        name
        for name, spread in spreads.items()
        if spread > _float(policy.get("max_parameter_result_spread_low_risk"), 0.02)
    ]
    if not available:
        status = "INSUFFICIENT_DATA"
    elif concentration >= _float(policy.get("high_risk_regime_return_concentration"), 0.85) or any(
        spread >= _float(policy.get("high_risk_parameter_result_spread"), 0.05)
        for spread in spreads.values()
    ):
        status = "HIGH_RISK"
    elif (
        len(available) < min_available
        or sensitive
        or concentration > _float(policy.get("max_regime_return_concentration_low_risk"), 0.70)
    ):
        status = "REVIEW_REQUIRED"
    else:
        status = "LOW_RISK"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_overfit_warning_summary",
        "simulation_overfit_status": status,
        "available_window_count": len(available),
        "regime_return_concentration": round(concentration, 6),
        "parameter_result_spreads": spreads,
        "sensitive_parameters": sensitive,
        "strong_calibration_allowed": status not in {"HIGH_RISK", "INSUFFICIENT_DATA"},
        "requires_forward_confirmation": True,
        "broker_action_taken": False,
    }


def _calibration_evidence(
    outcome_summary: Mapping[str, Any],
    paper_summary: Mapping[str, Any],
    regime_summary: Mapping[str, Any],
    sensitivity_summary: Mapping[str, Any],
) -> dict[str, Any]:
    limited = _variant_summary_row(outcome_summary, "limited_adjustment")
    consensus = _variant_summary_row(outcome_summary, "consensus_target")
    defensive = _variant_summary_row(outcome_summary, "defensive_limited_adjustment")
    overfit = _text(sensitivity_summary.get("simulation_overfit_status"), "INSUFFICIENT_DATA")
    readiness = "REVIEW_ONLY"
    if overfit in {"HIGH_RISK", "INSUFFICIENT_DATA"}:
        readiness = "FORWARD_CONFIRMATION_REQUIRED"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_evidence_summary",
        "best_variant": outcome_summary.get("best_variant", "MISSING"),
        "limited_adjustment_vs_no_trade_5d": limited.get("avg_relative_to_no_trade_5d", 0.0),
        "limited_adjustment_win_rate_5d": limited.get("win_rate_vs_no_trade_5d", 0.0),
        "consensus_target_avg_drawdown_20d": consensus.get("avg_max_drawdown_20d", 0.0),
        "defensive_limited_avg_drawdown_20d": defensive.get("avg_max_drawdown_20d", 0.0),
        "paper_total_return": paper_summary.get("total_return", 0.0),
        "paper_max_drawdown": paper_summary.get("max_drawdown", 0.0),
        "paper_turnover": paper_summary.get("turnover", 0.0),
        "best_variant_by_regime": regime_summary.get("best_variant_by_regime", {}),
        "simulation_overfit_status": overfit,
        "calibration_readiness": readiness,
        "requires_forward_confirmation": True,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _calibration_proposals(
    evidence: Mapping[str, Any], sensitivity_summary: Mapping[str, Any]
) -> dict[str, Any]:
    overfit = _text(sensitivity_summary.get("simulation_overfit_status"), "INSUFFICIENT_DATA")
    confidence = "LOW" if overfit in {"HIGH_RISK", "INSUFFICIENT_DATA"} else "MEDIUM"
    proposals = [
        {
            "proposal_id": "require_forward_confirmation",
            "proposal_type": "require_forward_confirmation",
            "affected_config": "position_advisory_v1.yaml",
            "auto_apply": False,
            "owner_approval_required": True,
            "evidence_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
            "confidence": confidence,
            "reason": "BACKTEST_SIMULATION can inform research but cannot trigger production.",
            "risks": ["not_pit_safe", "overfit_possible"],
        }
    ]
    if _float(evidence.get("limited_adjustment_vs_no_trade_5d")) > 0 and overfit != "HIGH_RISK":
        proposals.insert(
            0,
            {
                "proposal_id": "keep_limited_adjustment_default",
                "proposal_type": "keep_current_rules",
                "affected_config": "position_advisory_v1.yaml",
                "auto_apply": False,
                "owner_approval_required": True,
                "evidence_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
                "confidence": confidence,
                "reason": "limited_adjustment has positive simulated relative 5d evidence.",
                "risks": ["requires_forward_confirmation", "simulation_not_pit"],
            },
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_proposed_rule_changes",
        "proposals": proposals,
        "auto_apply": False,
        "can_trigger_production": False,
        "broker_action_taken": False,
    }


def _simulation_limitations() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_limitations",
        "not_pit_safe": True,
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "can_inform_research": True,
        "can_trigger_production": False,
        "requires_forward_confirmation": True,
        "known_limitations": [
            "current rules and current shortlist are projected over historical dates",
            "candidate selection is not point-in-time",
            "results can be overfit to the selected AI-after-ChatGPT period",
        ],
        "broker_action_taken": False,
    }


def _forward_confirmation_targets(
    evidence: Mapping[str, Any], proposals: Mapping[str, Any]
) -> dict[str, Any]:
    policy = _mapping(evidence.get("forward_confirmation_policy"))
    min_events = _int(
        policy.get("min_new_forward_events") or policy.get("required_forward_events"),
        10,
    )
    min_win_rate = _float(
        policy.get("min_limited_vs_notrade_win_rate") or policy.get("win_rate_vs_no_trade_min"),
        0.55,
    )
    min_relative_return = _float(policy.get("min_relative_return"), 0.0)
    max_drawdown_delta = _float(
        policy.get("max_drawdown_delta") or policy.get("avg_drawdown_delta_max"),
        0.0,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_forward_confirmation_targets",
        "targets": [
            {
                "target": "limited_adjustment_vs_no_trade",
                "priority": "HIGH",
                "reason": "simulation suggests potential benefit but requires forward confirmation",
                "required_forward_events": min_events,
                "windows": [1, 5, 10, 20],
                "success_criteria": {
                    "win_rate_vs_no_trade_min": min_win_rate,
                    "avg_relative_return_min": min_relative_return,
                    "avg_drawdown_delta_max": max_drawdown_delta,
                },
                "source_best_variant": evidence.get("best_variant"),
                "source_proposals": [
                    row.get("proposal_id") for row in _records(proposals.get("proposals"))
                ],
            },
            {
                "target": "defensive_limited_adjustment_drawdown",
                "priority": "MEDIUM",
                "reason": "simulation should be checked in future risk-off windows",
                "required_forward_events": min_events,
                "windows": [5, 10, 20],
                "success_criteria": {"avg_drawdown_delta_max": max_drawdown_delta},
            },
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _weekly_review_questions() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backtest_sim_weekly_review_questions",
        "questions": [
            "Did limited_adjustment outperform no_trade in new forward outcomes?",
            "Did defensive_limited_adjustment reduce drawdown in risk-off windows?",
            "Did consensus_target produce excess drawdown?",
            "Did high consensus remain predictive?",
        ],
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _load_shadow_candidate_sources(config: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _read_jsonl(_shadow_shortlist_path(config))
    result = []
    candidate_ids: set[str] = set()
    for row in rows:
        candidate_id = _text(row.get("candidate_id"))
        if not candidate_id or candidate_id in candidate_ids:
            raise DynamicV3BacktestSimulationError(
                "shadow shortlist candidate identity missing or duplicate"
            )
        candidate_ids.add(candidate_id)
        artifact_path = _resolve_project_path(Path(_text(row.get("real_evaluation_artifact_path"))))
        daily_weights_path = artifact_path.parent / "daily_weights.csv"
        manifest = _read_optional_json(artifact_path) or {}
        if manifest.get("candidate_id") != candidate_id or not daily_weights_path.is_file():
            raise DynamicV3BacktestSimulationError(
                f"candidate source identity/files invalid: {candidate_id}"
            )
        weights_by_date, regime_by_date = _read_daily_weights(daily_weights_path)
        if not weights_by_date or any(
            not weights or abs(sum(weights.values()) - 1.0) > 0.000001
            for weights in weights_by_date.values()
        ):
            raise DynamicV3BacktestSimulationError(
                f"candidate daily weights invalid: {candidate_id}"
            )
        result.append(
            {
                "candidate_id": candidate_id,
                "shortlist_rank": row.get("shortlist_rank"),
                "real_evaluation_manifest_path": artifact_path,
                "real_evaluation_manifest_checksum": sha256(artifact_path.read_bytes()).hexdigest(),
                "daily_weights_path": daily_weights_path,
                "daily_weights_checksum": sha256(daily_weights_path.read_bytes()).hexdigest(),
                "weights_by_date": weights_by_date,
                "regime_by_date": regime_by_date,
            }
        )
    return result


def _shadow_shortlist_path(config: Mapping[str, Any]) -> Path:
    source = _mapping(config.get("source"))
    shortlist_id = _text(source.get("shadow_shortlist_id"))
    return (
        DEFAULT_DYNAMIC_V3_RESEARCH_ROOT
        / "shadow_shortlist"
        / shortlist_id
        / "shadow_shortlist_candidates.jsonl"
    )


def _candidate_source_snapshot(
    candidates: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": row.get("candidate_id"),
            "shortlist_rank": row.get("shortlist_rank"),
            "real_evaluation_manifest_path": str(row.get("real_evaluation_manifest_path")),
            "real_evaluation_manifest_checksum": row.get("real_evaluation_manifest_checksum"),
            "daily_weights_path": str(row.get("daily_weights_path")),
            "daily_weights_checksum": row.get("daily_weights_checksum"),
            "weights_by_date": row.get("weights_by_date"),
            "regime_by_date": row.get("regime_by_date"),
        }
        for row in candidates
    ]


def _read_daily_weights(path: Path) -> tuple[dict[str, dict[str, float]], dict[str, str]]:
    if not path.exists():
        return {}, {}
    frame = pd.read_csv(path)
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    weights_by_date: dict[str, dict[str, float]] = {}
    regime_by_date: dict[str, str] = {}
    for date_value, group in frame.dropna(subset=["_date"]).groupby("_date"):
        weights = {
            _text(row["symbol"]): _float(row.get("target_weight", row.get("weight")))
            for row in group.to_dict("records")
        }
        weights_by_date[date_value.isoformat()] = _normalize_weights(weights)
        regime_by_date[date_value.isoformat()] = _text(group.iloc[0].get("regime"), "unknown")
    return weights_by_date, regime_by_date


def _scheduled_event_dates(
    price_dates: Sequence[date], *, start: date, end: date, config: Mapping[str, Any]
) -> list[date]:
    date_range = _mapping(config.get("date_range"))
    frequency = _text(date_range.get("event_frequency"), "weekly")
    event_day = _weekday_number(_text(date_range.get("event_day"), "MON"))
    candidates = [item for item in price_dates if start <= item <= end]
    if frequency == "daily":
        return candidates
    selected = []
    current = start - timedelta(days=start.weekday())
    step_weeks = 2 if frequency == "biweekly" else 1
    if frequency == "monthly":
        months = sorted({(item.year, item.month) for item in candidates})
        for year, month in months:
            month_dates = [item for item in candidates if item.year == year and item.month == month]
            selected.append(month_dates[0])
        return selected
    while current <= end:
        target = current + timedelta(days=event_day)
        window_end = current + timedelta(days=7)
        in_window = [item for item in candidates if target <= item < window_end]
        if in_window:
            selected.append(in_window[0])
        current += timedelta(days=7 * step_weeks)
    return selected


def _weekday_number(value: str) -> int:
    return {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4}.get(value.upper(), 0)


def _classify_regime(prices: pd.DataFrame, event_date: date, config: Mapping[str, Any]) -> str:
    policy = _mapping(config.get("regime_policy"))
    qqq_20 = _asset_return(prices, "QQQ", event_date, 20)
    qqq_60 = _asset_return(prices, "QQQ", event_date, 60)
    smh_20 = _asset_return(prices, "SMH", event_date, 20)
    smh_60 = _asset_return(prices, "SMH", event_date, 60)
    spy_20 = _asset_return(prices, "SPY", event_date, 20)
    if any(math.isnan(value) for value in (qqq_20, qqq_60, smh_20, smh_60, spy_20)):
        return "unknown"
    if qqq_60 >= _float(policy.get("ai_trend_qqq_60d_return_min")) and smh_60 - qqq_60 >= _float(
        policy.get("ai_trend_smh_minus_qqq_60d_min")
    ):
        return "ai_trend"
    if smh_20 <= _float(
        policy.get("semiconductor_pullback_smh_20d_return_max")
    ) and smh_20 - qqq_20 <= _float(policy.get("semiconductor_pullback_smh_minus_qqq_20d_max")):
        return "semiconductor_pullback"
    if qqq_20 <= _float(policy.get("tech_drawdown_qqq_20d_return_max")) or qqq_60 <= _float(
        policy.get("tech_drawdown_qqq_60d_return_max")
    ):
        return "tech_drawdown"
    if qqq_20 <= _float(policy.get("risk_off_qqq_20d_return_max")) and spy_20 <= _float(
        policy.get("risk_off_spy_20d_return_max")
    ):
        return "risk_off"
    if qqq_20 >= _float(policy.get("strong_recovery_qqq_20d_return_min")) and smh_20 >= _float(
        policy.get("strong_recovery_smh_20d_return_min")
    ):
        return "strong_recovery"
    if abs(qqq_60) <= _float(policy.get("sideways_choppy_qqq_60d_abs_return_max")):
        return "sideways_choppy"
    return "unknown"


def _asset_return(prices: pd.DataFrame, symbol: str, event_date: date, window: int) -> float:
    frame = prices.loc[prices["symbol"] == symbol].copy()
    if frame.empty:
        return float("nan")
    frame = frame.sort_values("_date")
    dates = [item for item in frame["_date"].tolist() if item <= event_date]
    if len(dates) <= window:
        return float("nan")
    end = dates[-1]
    start = dates[-window - 1]
    start_price = _float(frame.loc[frame["_date"] == start, "_adj_close"].iloc[-1])
    end_price = _float(frame.loc[frame["_date"] == end, "_adj_close"].iloc[-1])
    if start_price <= 0:
        return float("nan")
    return end_price / start_price - 1.0


def _load_prices(prices_path: Path, *, extra_symbols: set[str] | None = None) -> pd.DataFrame:
    config = load_etf_config_bundle()
    prices, quality = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=extra_symbols or set(),
    )
    if not quality.passed:
        raise DynamicV3BacktestSimulationError(f"ETF price validation failed: {quality.status}")
    prices = prices.copy()
    prices["_date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date
    prices["_adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
    return prices.dropna(subset=["_date", "_adj_close"])


def _run_cached_quality_gate(
    *, as_of: date, prices_path: Path, rates_path: Path, report_path: Path
) -> Any:
    universe = load_universe()
    quality = validate_data_cache(
        prices_path=prices_path,
        rates_path=rates_path,
        expected_price_tickers=configured_price_tickers(universe),
        expected_rate_series=configured_rate_series(universe),
        quality_config=load_data_quality(),
        as_of=as_of,
        manifest_path=_download_manifest_path(prices_path),
        secondary_prices_path=_marketstack_prices_path(prices_path),
        require_secondary_prices=_requires_marketstack_prices(prices_path),
    )
    write_data_quality_report(quality, report_path)
    return quality


def _variant_target(event: Mapping[str, Any], variant: str) -> dict[str, float]:
    if variant == "no_trade":
        return _normalize_weights(_mapping(event.get("baseline_weights")))
    if variant == "equal_weight_shadow_candidates":
        return _normalize_weights(_mapping(event.get("equal_weight_shadow_candidate_weights")))
    return _normalize_weights(_mapping(event.get("simulated_consensus_weights")))


def _apply_variant(
    *,
    before: Mapping[str, Any],
    target: Mapping[str, Any],
    event: Mapping[str, Any],
    variant: str,
    config: Mapping[str, Any],
) -> dict[str, float]:
    if variant == "no_trade":
        return _normalize_weights(before)
    if variant in {"consensus_target", "equal_weight_shadow_candidates"}:
        return _normalize_weights(target)
    if variant == "defensive_limited_adjustment" and not event.get("high_consensus"):
        target = _defensive_target(before=before, target=target, config=config)
    return _limited_weights(before, target, config)


def _limited_weights(
    before: Mapping[str, Any],
    target: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, float]:
    current = _normalize_weights(before)
    desired = _normalize_weights(target)
    limits = _mapping(config.get("limits"))
    min_trade = _float(limits.get("min_trade_threshold"))
    raw_delta = _weight_deltas(current, desired)
    if all(abs(value) < min_trade for value in raw_delta.values()):
        return current
    max_total = _float(limits.get("max_single_event_total_adjustment"), 1.0)
    max_symbol = _float(limits.get("max_single_symbol_adjustment"), 1.0)
    total_abs = sum(abs(value) for value in raw_delta.values())
    max_abs = max([abs(value) for value in raw_delta.values()] or [0.0])
    scale = 1.0
    if max_total > 0 and total_abs > max_total:
        scale = min(scale, max_total / total_abs)
    if max_symbol > 0 and max_abs > max_symbol:
        scale = min(scale, max_symbol / max_abs)
    limited = {symbol: round(value * scale, 6) for symbol, value in raw_delta.items()}
    drift = round(sum(limited.values()), 6)
    if drift:
        limited[_cash_symbol(config)] = round(_float(limited.get(_cash_symbol(config))) - drift, 6)
    return _apply_weight_deltas(current, limited)


def _defensive_target(
    *, before: Mapping[str, Any], target: Mapping[str, Any], config: Mapping[str, Any]
) -> dict[str, float]:
    current = _normalize_weights(before)
    desired = _normalize_weights(target)
    defensive = set(_texts(_mapping(config.get("portfolio")).get("defensive_symbols"))) | {"CASH"}
    adjusted = dict(current)
    for symbol, delta in _weight_deltas(current, desired).items():
        if symbol in defensive and delta > 0:
            adjusted[symbol] = _float(adjusted.get(symbol)) + delta
        elif symbol not in defensive and delta < 0:
            adjusted[symbol] = _float(adjusted.get(symbol)) + delta
    drift = round(sum(adjusted.values()) - 1.0, 6)
    if drift:
        adjusted[_cash_symbol(config)] = _float(adjusted.get(_cash_symbol(config))) - drift
    return _normalize_weights(adjusted)


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    clean = {_text(symbol): _float(value) for symbol, value in weights.items() if _text(symbol)}
    total = sum(clean.values())
    if not clean or total <= 0:
        return {}
    normalized = {symbol: round(max(value, 0.0) / total, 6) for symbol, value in clean.items()}
    drift = round(1.0 - sum(normalized.values()), 6)
    if drift:
        cash = "CASH" if "CASH" in normalized else sorted(normalized)[-1]
        normalized[cash] = round(normalized[cash] + drift, 6)
    return {symbol: value for symbol, value in sorted(normalized.items()) if abs(value) >= 0.000001}


def _average_weights(items: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    rows = [_normalize_weights(row) for row in items if row]
    if not rows:
        return {}
    symbols = sorted({symbol for row in rows for symbol in row})
    averaged = {
        symbol: round(_avg([_float(row.get(symbol)) for row in rows]), 6) for symbol in symbols
    }
    return _normalize_weights(averaged)


def _weight_dispersion(items: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    rows = [_normalize_weights(row) for row in items if row]
    if len(rows) <= 1:
        return {"max_symbol_dispersion": 0.0, "average_dispersion": 0.0}
    symbols = sorted({symbol for row in rows for symbol in row})
    spreads = []
    for symbol in symbols:
        values = [_float(row.get(symbol)) for row in rows]
        spreads.append(max(values) - min(values))
    return {
        "max_symbol_dispersion": max(spreads or [0.0]),
        "average_dispersion": _avg(spreads),
    }


def _is_high_consensus(dispersion: Mapping[str, Any], config: Mapping[str, Any]) -> bool:
    policy = _mapping(config.get("consensus_policy"))
    return _float(dispersion.get("max_symbol_dispersion")) <= _float(
        policy.get("high_consensus_max_symbol_dispersion")
    ) and _float(dispersion.get("average_dispersion")) <= _float(
        policy.get("low_disagreement_max_average_dispersion")
    )


def _cash_symbol(config: Mapping[str, Any]) -> str:
    return _text(_mapping(config.get("portfolio")).get("cash_symbol"), "CASH")


def _risk_exposure(weights: Mapping[str, Any], config: Mapping[str, Any]) -> float:
    defensive = set(_texts(_mapping(config.get("portfolio")).get("defensive_symbols"))) | {"CASH"}
    return sum(_float(value) for symbol, value in weights.items() if symbol not in defensive)


def _semiconductor_exposure(weights: Mapping[str, Any], config: Mapping[str, Any]) -> float:
    symbols = set(_texts(_mapping(config.get("portfolio")).get("semiconductor_symbols")))
    return sum(_float(weights.get(symbol)) for symbol in symbols)


def _largest_delta(deltas: Mapping[str, Any]) -> dict[str, float]:
    if not deltas:
        return {}
    symbol = max(deltas, key=lambda key: abs(_float(deltas.get(key))))
    return {symbol: round(_float(deltas.get(symbol)), 6)}


def _limits_applied(variant: str, turnover: float, config: Mapping[str, Any]) -> list[str]:
    if variant not in {"limited_adjustment", "defensive_limited_adjustment"} or turnover == 0:
        return []
    limits = _mapping(config.get("limits"))
    return [
        f"max_single_event_total_adjustment={limits.get('max_single_event_total_adjustment')}",
        f"max_single_symbol_adjustment={limits.get('max_single_symbol_adjustment')}",
    ]


def _variant_action_type(variant: str) -> str:
    return {
        "no_trade": "no_trade",
        "consensus_target": "rebalance",
        "limited_adjustment": "limited_adjustment",
        "defensive_limited_adjustment": "defensive_adjustment",
        "equal_weight_shadow_candidates": "rebalance",
    }.get(variant, "rebalance")


def _pending_sim_outcome(
    variant_row: Mapping[str, Any], *, window: int, start: date, end: date | None
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "sim_event_id": variant_row.get("sim_event_id"),
        "as_of": start.isoformat(),
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "pit_safety_status": PIT_SAFETY_SIMULATION,
        "not_for_production": True,
        "regime_label": variant_row.get("regime_label", "unknown"),
        "variant": variant_row.get("variant"),
        "window_days": window,
        "start_date": start.isoformat(),
        "end_date": "" if end is None else end.isoformat(),
        "return": None,
        "relative_to_no_trade": None,
        "relative_to_consensus_target": None,
        "relative_to_limited_adjustment": None,
        "max_drawdown": None,
        "realized_volatility": None,
        "turnover": _float(variant_row.get("turnover")),
        "outcome_status": "PENDING",
        "broker_action_taken": False,
        "production_effect": "none",
    }


def _relative(
    metrics: Mapping[str, Any],
    returns: Mapping[str, Mapping[str, Any]],
    reference: str,
    status: str,
) -> float | None:
    ref = returns.get(reference, _missing_metrics())
    if status != "AVAILABLE" or ref.get("status") != "AVAILABLE":
        return None
    return round(_float(metrics.get("return")) - _float(ref.get("return")), 6)


def _missing_metrics() -> dict[str, Any]:
    return {
        "return": None,
        "max_drawdown": None,
        "realized_volatility": None,
        "status": "INSUFFICIENT_DATA",
    }


def _empty_paper_summary(variant: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "variant": variant,
        "start_date": "",
        "end_date": "",
        "total_return": 0.0,
        "annualized_return": 0.0,
        "max_drawdown": 0.0,
        "realized_volatility": 0.0,
        "turnover": 0.0,
        "trade_count": 0,
        "relative_to_no_trade": 0.0,
        "relative_to_baseline": 0.0,
        "simulation_status": "INSUFFICIENT_DATA",
        "outcome_mode": OUTCOME_MODE_BACKTEST_SIMULATION,
        "broker_action_taken": False,
    }


def _realized_volatility(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    series = pd.Series(values, dtype="float64")
    return float(series.std(ddof=1) * math.sqrt(252))


def _window_values(rows: Sequence[Mapping[str, Any]], window: int, key: str) -> list[float]:
    return [
        float(row[key])
        for row in rows
        if _int(row.get("window_days")) == window
        and row.get("outcome_status") == "AVAILABLE"
        and _finite_number(row.get(key))
    ]


def _nullable_avg(values: Sequence[float]) -> float | None:
    return round(_avg(values), 6) if values else None


def _outcome_rows_contract_valid(rows: Sequence[Mapping[str, Any]]) -> bool:
    identities: set[tuple[str, str, int]] = set()
    metric_keys = (
        "return",
        "relative_to_no_trade",
        "relative_to_consensus_target",
        "relative_to_limited_adjustment",
        "max_drawdown",
        "realized_volatility",
    )
    for row in rows:
        identity = (
            _text(row.get("sim_event_id")),
            _text(row.get("variant")),
            _int(row.get("window_days")),
        )
        if not identity[0] or not identity[1] or identity[2] <= 0 or identity in identities:
            return False
        identities.add(identity)
        status = _text(row.get("outcome_status"))
        if not _finite_number(row.get("turnover"), minimum=0.0):
            return False
        if status == "AVAILABLE":
            if not all(_finite_number(row.get(key)) for key in metric_keys):
                return False
        elif status in {"PENDING", "INSUFFICIENT_DATA"}:
            if any(row.get(key) is not None for key in metric_keys):
                return False
        else:
            return False
    return True


def _variant_summary_row(summary: Mapping[str, Any], variant: str) -> dict[str, Any]:
    return next(
        (dict(row) for row in _records(summary.get("summary")) if row.get("variant") == variant),
        {},
    )


def _profile_dates(dates: Sequence[str], profile: str) -> list[str]:
    if profile == "weekly":
        return list(dates)
    if profile == "biweekly":
        return [item for index, item in enumerate(dates) if index % 2 == 0]
    if profile == "monthly":
        seen = set()
        selected = []
        for item in dates:
            key = item[:7]
            if key not in seen:
                selected.append(item)
                seen.add(key)
        return selected
    return list(dates)


def _with_limit(config: Mapping[str, Any], limit: float) -> dict[str, Any]:
    result = json.loads(json.dumps(config))
    result.setdefault("limits", {})["max_single_event_total_adjustment"] = limit
    return result


def _regime_return_concentration(rows: Sequence[Mapping[str, Any]]) -> float:
    by_regime: dict[str, float] = defaultdict(float)
    total = 0.0
    for row in rows:
        value = abs(_float(row.get("relative_to_no_trade")))
        by_regime[_text(row.get("regime_label"), "unknown")] += value
        total += value
    return max(by_regime.values() or [0.0]) / total if total > 0 else 0.0


def _result_spread(rows: Sequence[Mapping[str, Any]]) -> float:
    values = [
        _float(row.get("avg_relative_to_no_trade_5d")) for row in rows if row.get("sample_count")
    ]
    return round(max(values) - min(values), 6) if values else 0.0


def _price_source_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "provider": "existing_price_cache",
            "path": str(path),
            "download_timestamp": "",
            "row_count": 0,
            "checksum": "",
        }
    checksum = sha256(path.read_bytes()).hexdigest()
    try:
        row_count = max(sum(1 for _ in path.open("r", encoding="utf-8")) - 1, 0)
    except OSError:
        row_count = 0
    return {
        "provider": "existing_price_cache",
        "path": str(path),
        "download_timestamp": datetime.fromtimestamp(path.stat().st_mtime, UTC).isoformat(),
        "row_count": row_count,
        "checksum": checksum,
    }


def _text_source_snapshot(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "checksum": "", "content": ""}
    content = path.read_text(encoding="utf-8")
    return {
        "path": str(path),
        "checksum": sha256(content.encode("utf-8")).hexdigest(),
        "content": content,
    }


def _csv_cutoff_snapshot(path: Path, *, end: date) -> dict[str, Any]:
    if not path.is_file():
        return {"row_count": 0, "max_date": "", "rows_checksum": ""}
    frame = pd.read_csv(path)
    date_column = next(
        (name for name in ("date", "Date", "timestamp") if name in frame.columns),
        "",
    )
    if not date_column:
        raise DynamicV3BacktestSimulationError(f"cache date column missing: {path}")
    parsed = pd.to_datetime(frame[date_column], errors="coerce").dt.date
    selected = frame.loc[parsed.notna() & (parsed <= end)].copy()
    selected[date_column] = parsed.loc[selected.index].map(date.isoformat)
    max_date = parsed.loc[selected.index].max() if not selected.empty else None
    selected = selected.sort_values(list(selected.columns), kind="stable").reset_index(drop=True)
    material = selected.to_csv(index=False, lineterminator="\n").encode("utf-8")
    return {
        "row_count": len(selected),
        "max_date": "" if max_date is None else max_date.isoformat(),
        "rows_checksum": sha256(material).hexdigest(),
    }


def _portfolio_symbols(config: Mapping[str, Any]) -> set[str]:
    baseline = _mapping(_mapping(config.get("portfolio")).get("baseline_snapshot"))
    symbols = set(baseline)
    symbols.update(_texts(_mapping(config.get("portfolio")).get("defensive_symbols")))
    symbols.update(_texts(_mapping(config.get("portfolio")).get("semiconductor_symbols")))
    return {symbol for symbol in symbols if symbol != "CASH"}


def _event_symbols(events: Sequence[Mapping[str, Any]]) -> set[str]:
    symbols = set()
    for event in events:
        symbols.update(_mapping(event.get("baseline_weights")))
        symbols.update(_mapping(event.get("simulated_consensus_weights")))
        for weights in _mapping(event.get("candidate_weights")).values():
            symbols.update(_mapping(weights))
    return {symbol for symbol in symbols if symbol != "CASH"}


def _weights_symbols(rows: Sequence[Mapping[str, Any]]) -> set[str]:
    symbols = set()
    for row in rows:
        symbols.update(_mapping(row.get("before_weights")))
        symbols.update(_mapping(row.get("target_weights")))
        symbols.update(_mapping(row.get("after_weights")))
    return {symbol for symbol in symbols if symbol != "CASH"}


def _resolve_project_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.parents[2] / path


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return pd.to_datetime(value, errors="raise").date()
        except (ValueError, TypeError):
            return None


def _datetime_from_any(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")
