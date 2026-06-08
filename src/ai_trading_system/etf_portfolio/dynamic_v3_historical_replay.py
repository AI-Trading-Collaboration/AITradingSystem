from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
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
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    DEFAULT_RATES_CACHE_PATH,
    load_paper_portfolio_config,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_CONSENSUS_DRIFT_DIR,
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    DEFAULT_SHADOW_MONITOR_RUN_DIR,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
    STRATEGY_FAMILY,
)
from ai_trading_system.etf_portfolio.models import DEFAULT_ETF_PRICE_PATH, load_etf_config_bundle

DEFAULT_REPLAY_INVENTORY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_inventory"
DEFAULT_HISTORICAL_REPLAY_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "historical_replay"
DEFAULT_BACKFILLED_OUTCOME_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "backfilled_outcome"
DEFAULT_HISTORICAL_PAPER_SIM_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "historical_paper_sim"
DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_performance_review"
)
DEFAULT_PAPER_PORTFOLIO_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_portfolio"

OUTCOME_MODE_HISTORICAL_REPLAY = "HISTORICAL_REPLAY"
PIT_SAFE_STATUSES = {"PIT_SAFE", "PIT_WARNING", "PIT_UNSAFE"}
REPLAY_VARIANTS = (
    "no_trade",
    "consensus_target",
    "limited_adjustment",
    "owner_decision",
    "paper_action",
)
SIM_VARIANTS = ("no_trade_baseline", *REPLAY_VARIANTS[1:])
OUTCOME_WINDOWS = (1, 5, 10, 20)
OUTCOME_STATUSES = {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"}


class DynamicV3HistoricalReplayError(ValueError):
    """Raised when historical replay artifacts fail closed."""


def build_replay_inventory(
    *,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    paper_portfolio_dir: Path = DEFAULT_PAPER_PORTFOLIO_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_paper_portfolio_config(config_path)
    owner_reviews = _read_jsonl(owner_review_dir / "owner_review_journal.jsonl")
    price_index = _price_availability_index(prices_path)
    rows = []
    for advisory_manifest_path in sorted(daily_advisory_dir.glob("*/daily_advisory_manifest.json")):
        advisory_dir = advisory_manifest_path.parent
        manifest = _read_optional_json(advisory_manifest_path) or {}
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            continue
        rows.append(
            _inventory_row(
                advisory_dir=advisory_dir,
                manifest=manifest,
                as_of=as_of,
                config=config,
                owner_reviews=owner_reviews,
                shadow_monitor_run_dir=shadow_monitor_run_dir,
                consensus_drift_dir=consensus_drift_dir,
                paper_portfolio_dir=paper_portfolio_dir,
                price_index=price_index,
            )
        )
    rows = sorted(rows, key=lambda row: (row.get("as_of", ""), row.get("daily_advisory_id", "")))
    inventory_id = _stable_id("replay-inventory", start.isoformat(), end.isoformat(), generated)
    inventory_dir = _unique_dir(output_dir / inventory_id)
    inventory_dir.mkdir(parents=True, exist_ok=False)
    audit = _pit_safety_audit(rows)
    coverage = _replay_coverage_summary(rows, start=start, end=end)
    status = "PASS"
    if audit["pit_unsafe_count"]:
        status = "PASS_WITH_WARNINGS"
    if not rows:
        status = "INSUFFICIENT_DATA"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_inventory_manifest",
        "inventory_id": inventory_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total_replay_events": len(rows),
        "pit_safe_count": audit["pit_safe_count"],
        "pit_warning_count": audit["pit_warning_count"],
        "pit_unsafe_count": audit["pit_unsafe_count"],
        "eligible_count": coverage["eligible_count"],
        "partial_count": coverage["partial_count"],
        "ineligible_count": coverage["ineligible_count"],
        "config_path": str(config_path),
        "prices_path": str(prices_path),
        "replay_artifact_inventory_path": str(
            inventory_dir / "replay_artifact_inventory.jsonl"
        ),
        "pit_safety_audit_path": str(inventory_dir / "pit_safety_audit.json"),
        "replay_coverage_summary_path": str(inventory_dir / "replay_coverage_summary.json"),
        "replay_inventory_report_path": str(inventory_dir / "replay_inventory_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(inventory_dir / "replay_inventory_manifest.json", manifest)
    _write_jsonl(inventory_dir / "replay_artifact_inventory.jsonl", rows)
    _write_json(inventory_dir / "pit_safety_audit.json", audit)
    _write_json(inventory_dir / "replay_coverage_summary.json", coverage)
    _write_text(
        inventory_dir / "replay_inventory_report.md",
        render_replay_inventory_report(manifest, audit, coverage, rows),
    )
    _update_latest_pointer(
        "latest_replay_inventory",
        inventory_dir.name,
        inventory_dir / "replay_inventory_manifest.json",
    )
    return {
        "inventory_id": inventory_dir.name,
        "inventory_dir": inventory_dir,
        "manifest": manifest,
        "rows": rows,
        "pit_safety_audit": audit,
        "coverage_summary": coverage,
    }


def replay_inventory_report_payload(
    *,
    inventory_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
) -> dict[str, Any]:
    inventory_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=inventory_id if not latest else None,
        pointer_name="latest_replay_inventory",
    )
    return {
        **_read_json(inventory_dir / "replay_inventory_manifest.json"),
        "replay_artifact_inventory": _read_jsonl(
            inventory_dir / "replay_artifact_inventory.jsonl"
        ),
        "pit_safety_audit": _read_json(inventory_dir / "pit_safety_audit.json"),
        "replay_coverage_summary": _read_json(inventory_dir / "replay_coverage_summary.json"),
        "inventory_dir": str(inventory_dir),
    }


def validate_replay_inventory_artifact(
    *,
    inventory_id: str,
    output_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
) -> dict[str, Any]:
    inventory_dir = output_dir / inventory_id
    manifest = _read_optional_json(inventory_dir / "replay_inventory_manifest.json") or {}
    rows = _read_jsonl(inventory_dir / "replay_artifact_inventory.jsonl")
    checks = [
        _check(
            "manifest_exists",
            (inventory_dir / "replay_inventory_manifest.json").exists(),
            inventory_id,
        ),
        _check(
            "inventory_rows_exist",
            (inventory_dir / "replay_artifact_inventory.jsonl").exists(),
            inventory_id,
        ),
        _check("pit_safety_audit_exists", (inventory_dir / "pit_safety_audit.json").exists(), ""),
        _check(
            "coverage_summary_exists",
            (inventory_dir / "replay_coverage_summary.json").exists(),
            "",
        ),
        _check("report_exists", (inventory_dir / "replay_inventory_report.md").exists(), ""),
        _check("inventory_id_matches", manifest.get("inventory_id") == inventory_id, inventory_id),
        _check(
            "pit_safety_status_valid",
            all(row.get("pit_safety_status") in PIT_SAFE_STATUSES for row in rows),
            "pit safety",
        ),
        _check(
            "pit_unsafe_not_eligible",
            all(
                row.get("replay_eligibility") == "INELIGIBLE"
                for row in rows
                if row.get("pit_safety_status") == "PIT_UNSAFE"
            ),
            "PIT_UNSAFE rows must be ineligible",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_replay_inventory_validation",
        artifact_id_key="inventory_id",
        artifact_id=inventory_id,
        checks=checks,
    )


def run_historical_replay(
    *,
    inventory_id: str,
    include_pit_warning: bool = False,
    inventory_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
    output_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_dir = inventory_dir / inventory_id
    inventory_manifest = _read_json(source_dir / "replay_inventory_manifest.json")
    inventory_rows = _read_jsonl(source_dir / "replay_artifact_inventory.jsonl")
    events = []
    decision_inputs = []
    skipped = []
    for row in inventory_rows:
        pit_status = _text(row.get("pit_safety_status"))
        if pit_status == "PIT_UNSAFE":
            skipped.append(_skip_row(row, "PIT_UNSAFE_EXCLUDED"))
            continue
        if pit_status == "PIT_WARNING" and not include_pit_warning:
            skipped.append(_skip_row(row, "PIT_WARNING_EXCLUDED"))
            continue
        event = _historical_replay_event(row)
        events.append(event)
        decision_inputs.append(_historical_decision_input(row, event))
    replay_id = _stable_id(
        "historical-replay",
        inventory_id,
        include_pit_warning,
        generated.isoformat(),
    )
    replay_dir = _unique_dir(output_dir / replay_id)
    replay_dir.mkdir(parents=True, exist_ok=False)
    action_summary = _replay_action_summary(events, skipped)
    status = "PASS" if events else "INSUFFICIENT_DATA"
    if skipped and events:
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_historical_replay_manifest",
        "replay_id": replay_dir.name,
        "inventory_id": inventory_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "include_pit_warning": include_pit_warning,
        "replay_event_count": len(events),
        "skipped_count": len(skipped),
        "generated_variants": list(REPLAY_VARIANTS),
        "source_inventory_path": str(source_dir / "replay_inventory_manifest.json"),
        "historical_replay_manifest_path": str(replay_dir / "historical_replay_manifest.json"),
        "replay_events_path": str(replay_dir / "replay_events.jsonl"),
        "replay_decision_inputs_path": str(replay_dir / "replay_decision_inputs.jsonl"),
        "replay_action_summary_path": str(replay_dir / "replay_action_summary.json"),
        "historical_replay_report_path": str(replay_dir / "historical_replay_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(replay_dir / "historical_replay_manifest.json", manifest)
    _write_jsonl(replay_dir / "replay_events.jsonl", events)
    _write_jsonl(replay_dir / "replay_decision_inputs.jsonl", decision_inputs)
    _write_json(replay_dir / "replay_action_summary.json", action_summary)
    _write_text(
        replay_dir / "historical_replay_report.md",
        render_historical_replay_report(manifest, action_summary, events, skipped),
    )
    _update_latest_pointer(
        "latest_historical_replay",
        replay_dir.name,
        replay_dir / "historical_replay_manifest.json",
    )
    return {
        "replay_id": replay_dir.name,
        "replay_dir": replay_dir,
        "manifest": manifest,
        "events": events,
        "action_summary": action_summary,
        "inventory_manifest": inventory_manifest,
    }


def historical_replay_report_payload(
    *,
    replay_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
) -> dict[str, Any]:
    replay_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=replay_id if not latest else None,
        pointer_name="latest_historical_replay",
    )
    return {
        **_read_json(replay_dir / "historical_replay_manifest.json"),
        "replay_events": _read_jsonl(replay_dir / "replay_events.jsonl"),
        "replay_decision_inputs": _read_jsonl(replay_dir / "replay_decision_inputs.jsonl"),
        "replay_action_summary": _read_json(replay_dir / "replay_action_summary.json"),
        "replay_dir": str(replay_dir),
    }


def validate_historical_replay_artifact(
    *,
    replay_id: str,
    output_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
) -> dict[str, Any]:
    replay_dir = output_dir / replay_id
    manifest = _read_optional_json(replay_dir / "historical_replay_manifest.json") or {}
    events = _read_jsonl(replay_dir / "replay_events.jsonl")
    checks = [
        _check(
            "manifest_exists",
            (replay_dir / "historical_replay_manifest.json").exists(),
            replay_id,
        ),
        _check("replay_events_exists", (replay_dir / "replay_events.jsonl").exists(), ""),
        _check(
            "decision_inputs_exists",
            (replay_dir / "replay_decision_inputs.jsonl").exists(),
            "",
        ),
        _check("action_summary_exists", (replay_dir / "replay_action_summary.json").exists(), ""),
        _check("report_exists", (replay_dir / "historical_replay_report.md").exists(), ""),
        _check("replay_id_matches", manifest.get("replay_id") == replay_id, replay_id),
        _check(
            "outcome_mode_is_historical_replay",
            all(row.get("outcome_mode") == OUTCOME_MODE_HISTORICAL_REPLAY for row in events),
            OUTCOME_MODE_HISTORICAL_REPLAY,
        ),
        _check(
            "pit_unsafe_skipped",
            all(row.get("pit_safety_status") != "PIT_UNSAFE" for row in events),
            "PIT_UNSAFE must not enter replay",
        ),
        _check(
            "variants_generated",
            all(
                {variant["variant"] for variant in _records(row.get("variants"))}
                >= set(REPLAY_VARIANTS)
                for row in events
            ),
            "required variants",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False
            and all(row.get("broker_action_taken") is False for row in events),
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_historical_replay_validation",
        artifact_id_key="replay_id",
        artifact_id=replay_id,
        checks=checks,
    )


def run_backfill_outcome(
    *,
    replay_id: str,
    replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    config_path: Path = DEFAULT_PAPER_PORTFOLIO_CONFIG_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    config = load_paper_portfolio_config(config_path)
    windows = _configured_outcome_windows(config)
    source_dir = replay_dir / replay_id
    replay_manifest = _read_json(source_dir / "historical_replay_manifest.json")
    replay_events = _read_jsonl(source_dir / "replay_events.jsonl")
    backfill_id = _stable_id("backfill-outcome", replay_id, generated.isoformat())
    backfill_dir = _unique_dir(output_dir / backfill_id)
    backfill_dir.mkdir(parents=True, exist_ok=False)
    quality_status = "SKIPPED_EXPLICIT_TEST_FIXTURE"
    quality_report_path = ""
    if enforce_data_quality_gate:
        quality_report = backfill_dir / "validate_data_quality_report.md"
        quality = _run_cached_data_quality_gate(
            as_of=generated.date(),
            prices_path=prices_path,
            rates_path=rates_path,
            report_path=quality_report,
        )
        quality_status = quality.status
        quality_report_path = str(quality_report)
        if not quality.passed:
            raise DynamicV3HistoricalReplayError(
                f"backfill outcome data quality gate failed: {quality.status}"
            )
    prices = _load_prices_for_replay(prices_path, replay_events)
    price_dates = _available_price_dates(prices)
    rows = []
    for event in replay_events:
        rows.extend(
            _backfilled_outcome_rows(
                event=event,
                windows=windows,
                prices=prices,
                price_dates=price_dates,
                generated_date=generated.date(),
            )
        )
    summary = _variant_performance_summary(rows)
    rollup = Counter(row["outcome_status"] for row in rows)
    status = "AVAILABLE" if rollup.get("AVAILABLE") else "INSUFFICIENT_DATA"
    if rollup.get("PENDING") and not rollup.get("AVAILABLE"):
        status = "PENDING"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_backfill_outcome_manifest",
        "backfill_id": backfill_dir.name,
        "replay_id": replay_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "tracked_windows": windows,
        "replay_event_count": replay_manifest.get("replay_event_count", len(replay_events)),
        "available_count": rollup.get("AVAILABLE", 0),
        "pending_count": rollup.get("PENDING", 0),
        "insufficient_data_count": rollup.get("INSUFFICIENT_DATA", 0),
        "best_variant": summary.get("best_variant", "MISSING"),
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "prices_path": str(prices_path),
        "source_replay_path": str(source_dir / "historical_replay_manifest.json"),
        "backfill_manifest_path": str(backfill_dir / "backfill_manifest.json"),
        "replay_outcome_windows_path": str(backfill_dir / "replay_outcome_windows.jsonl"),
        "variant_performance_summary_path": str(
            backfill_dir / "variant_performance_summary.json"
        ),
        "backfill_outcome_report_path": str(backfill_dir / "backfill_outcome_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(backfill_dir / "backfill_manifest.json", manifest)
    _write_jsonl(backfill_dir / "replay_outcome_windows.jsonl", rows)
    _write_json(backfill_dir / "variant_performance_summary.json", summary)
    _write_text(
        backfill_dir / "backfill_outcome_report.md",
        render_backfill_outcome_report(manifest, summary),
    )
    _update_latest_pointer(
        "latest_backfilled_outcome",
        backfill_dir.name,
        backfill_dir / "backfill_manifest.json",
    )
    return {
        "backfill_id": backfill_dir.name,
        "backfill_dir": backfill_dir,
        "manifest": manifest,
        "outcome_rows": rows,
        "variant_performance_summary": summary,
        "replay_manifest": replay_manifest,
    }


def backfill_outcome_report_payload(
    *,
    backfill_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
) -> dict[str, Any]:
    backfill_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=backfill_id if not latest else None,
        pointer_name="latest_backfilled_outcome",
    )
    return {
        **_read_json(backfill_dir / "backfill_manifest.json"),
        "replay_outcome_windows": _read_jsonl(backfill_dir / "replay_outcome_windows.jsonl"),
        "variant_performance_summary": _read_json(
            backfill_dir / "variant_performance_summary.json"
        ),
        "backfill_dir": str(backfill_dir),
    }


def validate_backfill_outcome_artifact(
    *,
    backfill_id: str,
    output_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
) -> dict[str, Any]:
    backfill_dir = output_dir / backfill_id
    manifest = _read_optional_json(backfill_dir / "backfill_manifest.json") or {}
    rows = _read_jsonl(backfill_dir / "replay_outcome_windows.jsonl")
    checks = [
        _check("manifest_exists", (backfill_dir / "backfill_manifest.json").exists(), backfill_id),
        _check("outcome_rows_exists", (backfill_dir / "replay_outcome_windows.jsonl").exists(), ""),
        _check(
            "summary_exists",
            (backfill_dir / "variant_performance_summary.json").exists(),
            "",
        ),
        _check("report_exists", (backfill_dir / "backfill_outcome_report.md").exists(), ""),
        _check("backfill_id_matches", manifest.get("backfill_id") == backfill_id, backfill_id),
        _check(
            "window_status_valid",
            all(row.get("outcome_status") in OUTCOME_STATUSES for row in rows),
            "outcome status",
        ),
        _check(
            "outcome_mode_is_historical_replay",
            all(row.get("outcome_mode") == OUTCOME_MODE_HISTORICAL_REPLAY for row in rows),
            OUTCOME_MODE_HISTORICAL_REPLAY,
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_backfill_outcome_validation",
        artifact_id_key="backfill_id",
        artifact_id=backfill_id,
        checks=checks,
    )


def run_historical_paper_sim(
    *,
    replay_id: str,
    variant: str = "limited_adjustment",
    replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    output_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if variant not in SIM_VARIANTS:
        raise DynamicV3HistoricalReplayError(f"unsupported simulation variant: {variant}")
    generated = generated_at or datetime.now(UTC)
    source_dir = replay_dir / replay_id
    replay_manifest = _read_json(source_dir / "historical_replay_manifest.json")
    events = sorted(_read_jsonl(source_dir / "replay_events.jsonl"), key=lambda row: row["as_of"])
    sim_id = _stable_id("historical-paper-sim", replay_id, variant, generated.isoformat())
    sim_dir = _unique_dir(output_dir / sim_id)
    sim_dir.mkdir(parents=True, exist_ok=False)
    prices = _load_prices_for_replay(prices_path, events)
    history, ledger, summary = _simulate_paper_history(events, variant=variant, prices=prices)
    status = summary["simulation_status"]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_historical_paper_sim_manifest",
        "sim_id": sim_dir.name,
        "replay_id": replay_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "variant": variant,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "source_replay_path": str(source_dir / "historical_replay_manifest.json"),
        "historical_paper_sim_manifest_path": str(
            sim_dir / "historical_paper_sim_manifest.json"
        ),
        "simulated_paper_state_history_path": str(
            sim_dir / "simulated_paper_state_history.jsonl"
        ),
        "simulated_trade_ledger_path": str(sim_dir / "simulated_trade_ledger.jsonl"),
        "simulated_performance_summary_path": str(
            sim_dir / "simulated_performance_summary.json"
        ),
        "historical_paper_sim_report_path": str(sim_dir / "historical_paper_sim_report.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(sim_dir / "historical_paper_sim_manifest.json", manifest)
    _write_jsonl(sim_dir / "simulated_paper_state_history.jsonl", history)
    _write_jsonl(sim_dir / "simulated_trade_ledger.jsonl", ledger)
    _write_json(sim_dir / "simulated_performance_summary.json", summary)
    _write_text(
        sim_dir / "historical_paper_sim_report.md",
        render_historical_paper_sim_report(manifest, summary),
    )
    _update_latest_pointer(
        "latest_historical_paper_sim",
        sim_dir.name,
        sim_dir / "historical_paper_sim_manifest.json",
    )
    return {
        "sim_id": sim_dir.name,
        "sim_dir": sim_dir,
        "manifest": manifest,
        "state_history": history,
        "trade_ledger": ledger,
        "performance_summary": summary,
        "replay_manifest": replay_manifest,
    }


def historical_paper_sim_report_payload(
    *,
    sim_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
) -> dict[str, Any]:
    sim_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=sim_id if not latest else None,
        pointer_name="latest_historical_paper_sim",
    )
    return {
        **_read_json(sim_dir / "historical_paper_sim_manifest.json"),
        "simulated_paper_state_history": _read_jsonl(
            sim_dir / "simulated_paper_state_history.jsonl"
        ),
        "simulated_trade_ledger": _read_jsonl(sim_dir / "simulated_trade_ledger.jsonl"),
        "simulated_performance_summary": _read_json(
            sim_dir / "simulated_performance_summary.json"
        ),
        "sim_dir": str(sim_dir),
    }


def validate_historical_paper_sim_artifact(
    *,
    sim_id: str,
    output_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
) -> dict[str, Any]:
    sim_dir = output_dir / sim_id
    manifest = _read_optional_json(sim_dir / "historical_paper_sim_manifest.json") or {}
    history = _read_jsonl(sim_dir / "simulated_paper_state_history.jsonl")
    ledger = _read_jsonl(sim_dir / "simulated_trade_ledger.jsonl")
    checks = [
        _check(
            "manifest_exists",
            (sim_dir / "historical_paper_sim_manifest.json").exists(),
            sim_id,
        ),
        _check(
            "state_history_exists",
            (sim_dir / "simulated_paper_state_history.jsonl").exists(),
            "",
        ),
        _check("trade_ledger_exists", (sim_dir / "simulated_trade_ledger.jsonl").exists(), ""),
        _check(
            "summary_exists",
            (sim_dir / "simulated_performance_summary.json").exists(),
            "",
        ),
        _check("report_exists", (sim_dir / "historical_paper_sim_report.md").exists(), ""),
        _check("sim_id_matches", manifest.get("sim_id") == sim_id, sim_id),
        _check("state_history_present", bool(history), "state history"),
        _check(
            "ledger_broker_action_not_taken",
            all(row.get("broker_action_taken") is False for row in ledger),
            "broker action not taken",
        ),
        _check(
            "manifest_broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_historical_paper_sim_validation",
        artifact_id_key="sim_id",
        artifact_id=sim_id,
        checks=checks,
    )


def run_replay_performance_review(
    *,
    backfill_id: str,
    sim_id: str,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    sim_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    output_dir: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    source_backfill_dir = backfill_dir / backfill_id
    source_sim_dir = sim_dir / sim_id
    backfill_manifest = _read_json(source_backfill_dir / "backfill_manifest.json")
    outcome_rows = _read_jsonl(source_backfill_dir / "replay_outcome_windows.jsonl")
    variant_summary = _read_json(source_backfill_dir / "variant_performance_summary.json")
    sim_summary = _read_json(source_sim_dir / "simulated_performance_summary.json")
    review_id = _stable_id("replay-performance-review", backfill_id, sim_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    effectiveness = _advisory_rule_effectiveness(outcome_rows, variant_summary, sim_summary)
    recommendations = _calibration_recommendations(variant_summary, sim_summary)
    available_outcome_count = _int(backfill_manifest.get("available_count"))
    status = (
        "AVAILABLE"
        if available_outcome_count
        else _text(backfill_manifest.get("status"), "INSUFFICIENT_DATA")
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_performance_manifest",
        "review_id": review_dir.name,
        "backfill_id": backfill_id,
        "sim_id": sim_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "replay_event_count": backfill_manifest.get("replay_event_count", 0),
        "available_outcome_count": available_outcome_count,
        "best_variant": variant_summary.get("best_variant", "MISSING"),
        "limited_adjustment_vs_no_trade": variant_summary.get(
            "limited_adjustment_vs_no_trade_5d",
            0.0,
        ),
        "calibration_recommendation": recommendations["recommendations"][0]["type"],
        "next_action": recommendations["recommendations"][0]["type"],
        "source_backfill_path": str(source_backfill_dir / "backfill_manifest.json"),
        "source_sim_path": str(source_sim_dir / "historical_paper_sim_manifest.json"),
        "replay_performance_manifest_path": str(review_dir / "replay_performance_manifest.json"),
        "advisory_rule_effectiveness_path": str(
            review_dir / "advisory_rule_effectiveness.json"
        ),
        "calibration_recommendations_path": str(
            review_dir / "calibration_recommendations.json"
        ),
        "replay_performance_review_path": str(review_dir / "replay_performance_review.md"),
        "reader_brief_section_path": str(review_dir / "reader_brief_section.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "automatic_candidate_promotion": False,
        "official_target_weights_mutated": False,
        "baseline_config_mutated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(review_dir / "replay_performance_manifest.json", manifest)
    _write_json(review_dir / "advisory_rule_effectiveness.json", effectiveness)
    _write_json(review_dir / "calibration_recommendations.json", recommendations)
    _write_text(
        review_dir / "replay_performance_review.md",
        render_replay_performance_review(manifest, effectiveness, recommendations, sim_summary),
    )
    _write_text(
        review_dir / "reader_brief_section.md",
        render_replay_performance_reader_brief(manifest, recommendations),
    )
    _update_latest_pointer(
        "latest_replay_performance_review",
        review_dir.name,
        review_dir / "replay_performance_manifest.json",
    )
    return {
        "review_id": review_dir.name,
        "review_dir": review_dir,
        "manifest": manifest,
        "advisory_rule_effectiveness": effectiveness,
        "calibration_recommendations": recommendations,
        "simulated_performance_summary": sim_summary,
    }


def replay_performance_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=review_id if not latest else None,
        pointer_name="latest_replay_performance_review",
    )
    return {
        **_read_json(review_dir / "replay_performance_manifest.json"),
        "advisory_rule_effectiveness": _read_json(
            review_dir / "advisory_rule_effectiveness.json"
        ),
        "calibration_recommendations": _read_json(
            review_dir / "calibration_recommendations.json"
        ),
        "review_dir": str(review_dir),
    }


def validate_replay_performance_review_artifact(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_REPLAY_PERFORMANCE_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "replay_performance_manifest.json") or {}
    recommendations = _read_optional_json(review_dir / "calibration_recommendations.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (review_dir / "replay_performance_manifest.json").exists(),
            review_id,
        ),
        _check(
            "effectiveness_exists",
            (review_dir / "advisory_rule_effectiveness.json").exists(),
            "",
        ),
        _check(
            "recommendations_exists",
            (review_dir / "calibration_recommendations.json").exists(),
            "",
        ),
        _check("report_exists", (review_dir / "replay_performance_review.md").exists(), ""),
        _check("reader_brief_exists", (review_dir / "reader_brief_section.md").exists(), ""),
        _check("review_id_matches", manifest.get("review_id") == review_id, review_id),
        _check(
            "recommendations_do_not_auto_modify_config",
            all(
                row.get("requires_owner_approval") is True
                for row in _records(recommendations.get("recommendations"))
            ),
            "owner approval required",
        ),
        _check(
            "production_mutation_forbidden",
            manifest.get("production_candidate_generated") is False
            and manifest.get("automatic_candidate_promotion") is False
            and manifest.get("official_target_weights_mutated") is False
            and manifest.get("baseline_config_mutated") is False,
            "no production mutation",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_replay_performance_review_validation",
        artifact_id_key="review_id",
        artifact_id=review_id,
        checks=checks,
    )


def render_replay_inventory_report(
    manifest: Mapping[str, Any],
    audit: Mapping[str, Any],
    coverage: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> str:
    dates = ", ".join(sorted({_text(row.get("as_of")) for row in rows if row.get("as_of")}))
    missing_targets = ", ".join(
        _text(row.get("as_of"))
        for row in rows
        if "MISSING_TARGET_WEIGHTS" in _texts(row.get("replay_limitations"))
    )
    missing_prices = ", ".join(
        _text(row.get("as_of"))
        for row in rows
        if "MISSING_PRICE_DATA" in _texts(row.get("replay_limitations"))
    )
    return "\n".join(
        [
            "# Dynamic v3 historical replay inventory",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- inventory_id：`{manifest.get('inventory_id')}`",
            f"- 日期范围：{manifest.get('start')} to {manifest.get('end')}",
            f"- historical advisory events：{manifest.get('total_replay_events')}",
            f"- PIT_SAFE：{audit.get('pit_safe_count')}",
            f"- PIT_WARNING：{audit.get('pit_warning_count')}",
            f"- PIT_UNSAFE：{audit.get('pit_unsafe_count')}",
            f"- 可重放日期：{dates or 'none'}",
            f"- 缺少 target weights 日期：{missing_targets or 'none'}",
            f"- 缺少价格数据日期：{missing_prices or 'none'}",
            f"- eligible / partial / ineligible：{coverage.get('eligible_count')} / "
            f"{coverage.get('partial_count')} / {coverage.get('ineligible_count')}",
            "- 默认 replay 不允许 PIT_UNSAFE 进入 outcome 链路。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_historical_replay_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    skipped: Sequence[Mapping[str, Any]],
) -> str:
    variants = ", ".join(_texts(manifest.get("generated_variants")))
    pit_counts = Counter(_text(row.get("pit_safety_status")) for row in events)
    skipped_reasons = Counter(_text(row.get("skip_reason")) for row in skipped)
    return "\n".join(
        [
            "# Dynamic v3 historical advisory replay",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- replay_id：`{manifest.get('replay_id')}`",
            f"- replay events：{manifest.get('replay_event_count')}",
            f"- skipped events：{manifest.get('skipped_count')}",
            f"- generated variants：{variants}",
            f"- PIT_SAFE / PIT_WARNING：{pit_counts.get('PIT_SAFE', 0)} / "
            f"{pit_counts.get('PIT_WARNING', 0)}",
            f"- skipped reasons：{dict(skipped_reasons) or 'none'}",
            f"- broker action present：{summary.get('broker_action_present')}",
            "- outcome_mode=HISTORICAL_REPLAY。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_backfill_outcome_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Dynamic v3 backfilled replay outcome",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- backfill_id：`{manifest.get('backfill_id')}`",
            f"- replay_id：`{manifest.get('replay_id')}`",
            f"- AVAILABLE：{manifest.get('available_count')}",
            f"- PENDING：{manifest.get('pending_count')}",
            f"- INSUFFICIENT_DATA：{manifest.get('insufficient_data_count')}",
            f"- best_variant：{summary.get('best_variant')}",
            f"- limited_adjustment_vs_no_trade_5d："
            f"{summary.get('limited_adjustment_vs_no_trade_5d')}",
            f"- data_quality_status：{manifest.get('data_quality_status')}",
            "- 价格只用于 outcome calculation，不进入 replay decision input。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_historical_paper_sim_report(
    manifest: Mapping[str, Any], summary: Mapping[str, Any]
) -> str:
    return "\n".join(
        [
            "# Dynamic v3 historical paper portfolio simulation",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- sim_id：`{manifest.get('sim_id')}`",
            f"- variant：{summary.get('variant')}",
            f"- 日期范围：{summary.get('start_date')} to {summary.get('end_date')}",
            f"- total_return：{summary.get('total_return')}",
            f"- max_drawdown：{summary.get('max_drawdown')}",
            f"- turnover：{summary.get('turnover')}",
            f"- trade_count：{summary.get('trade_count')}",
            f"- relative_to_no_trade：{summary.get('relative_to_no_trade')}",
            "- outcome_mode=HISTORICAL_REPLAY；不是真实仓位历史。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_replay_performance_review(
    manifest: Mapping[str, Any],
    effectiveness: Mapping[str, Any],
    recommendations: Mapping[str, Any],
    sim_summary: Mapping[str, Any],
) -> str:
    top = _records(recommendations.get("recommendations"))[0]
    return "\n".join(
        [
            "# Dynamic v3 replay performance review",
            "",
            f"- 状态：{manifest.get('status')}",
            f"- review_id：`{manifest.get('review_id')}`",
            f"- replay_event_count：{manifest.get('replay_event_count')}",
            f"- available_outcome_count：{manifest.get('available_outcome_count')}",
            f"- best_variant：{manifest.get('best_variant')}",
            f"- limited_adjustment_vs_no_trade："
            f"{manifest.get('limited_adjustment_vs_no_trade')}",
            f"- simulation_variant：{sim_summary.get('variant')}",
            f"- simulation_total_return：{sim_summary.get('total_return')}",
            f"- primary_recommendation：{top.get('type')}",
            f"- reason：{top.get('reason')}",
            f"- requires_owner_approval：{top.get('requires_owner_approval')}",
            f"- recommendation_effectiveness_count："
            f"{len(_records(effectiveness.get('recommendation_effectiveness')))}",
            "- 不自动修改 position_advisory_v1.yaml 或 production config。",
            "- production_effect=none；broker_action_taken=false。",
            "",
        ]
    )


def render_replay_performance_reader_brief(
    manifest: Mapping[str, Any], recommendations: Mapping[str, Any]
) -> str:
    top = _records(recommendations.get("recommendations"))[0]
    return "\n".join(
        [
            "## Dynamic Rescue Historical Replay Performance",
            "",
            f"- replay_event_count: {manifest.get('replay_event_count')}",
            f"- available_outcome_count: {manifest.get('available_outcome_count')}",
            f"- best_variant: {manifest.get('best_variant')}",
            f"- limited_adjustment_vs_no_trade: "
            f"{manifest.get('limited_adjustment_vs_no_trade')}",
            f"- calibration_recommendation: {top.get('type')}",
            f"- next_action: {manifest.get('next_action')}",
            "- production_effect: none",
            "- broker_action_taken: false",
            "",
        ]
    )


def _inventory_row(
    *,
    advisory_dir: Path,
    manifest: Mapping[str, Any],
    as_of: date,
    config: Mapping[str, Any],
    owner_reviews: Sequence[Mapping[str, Any]],
    shadow_monitor_run_dir: Path,
    consensus_drift_dir: Path,
    paper_portfolio_dir: Path,
    price_index: Mapping[str, set[date]],
) -> dict[str, Any]:
    daily_advisory_id = _text(manifest.get("daily_advisory_id"), advisory_dir.name)
    actions = _read_optional_json(advisory_dir / "daily_advisory_actions.json") or {}
    target_rows = _read_jsonl(advisory_dir / "daily_candidate_targets.jsonl")
    delta_rows = _read_jsonl(advisory_dir / "daily_position_deltas.jsonl")
    consensus_weights, consensus_limitations = _consensus_weights(advisory_dir, target_rows)
    current_weights, current_limitations = _current_weights(
        advisory_dir=advisory_dir,
        as_of=as_of,
        paper_portfolio_dir=paper_portfolio_dir,
    )
    owner_review = _owner_review_for_daily(daily_advisory_id, owner_reviews)
    paper_action_weights = _paper_action_weights_for_daily(
        daily_advisory_id=daily_advisory_id,
        paper_portfolio_dir=paper_portfolio_dir,
    )
    limited_weights = _limited_adjustment_weights(current_weights, consensus_weights, config)
    source_shadow_monitor_id = _text(manifest.get("source_shadow_monitor_run_id"))
    consensus_drift = _consensus_drift_for_monitor(
        source_shadow_monitor_id,
        consensus_drift_dir=consensus_drift_dir,
    )
    limitations = [*consensus_limitations, *current_limitations]
    if not owner_review:
        limitations.append("OWNER_DECISION_MISSING")
    generated_date = _date_from_any(manifest.get("generated_at"))
    if generated_date is not None and generated_date > as_of:
        limitations.append("ADVISORY_GENERATED_AFTER_AS_OF_DATE")
    if not consensus_weights:
        limitations.append("MISSING_TARGET_WEIGHTS")
    price_symbols = (
        set(current_weights)
        | set(consensus_weights)
        | set(_mapping(paper_action_weights.get("weights")))
    )
    has_future_price = _has_price_after(price_symbols, as_of, price_index)
    if not has_future_price:
        limitations.append("MISSING_PRICE_DATA")
    pit_status, eligibility = _pit_status_and_eligibility(limitations)
    available_inputs = {
        "target_weights": bool(consensus_weights),
        "candidate_weights": bool(target_rows),
        "consensus_weights": bool(consensus_weights),
        "current_portfolio_snapshot": "PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_BASELINE"
        not in limitations,
        "owner_decision": bool(owner_review),
        "price_data_after_as_of": has_future_price,
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "replay_event_id": _stable_id("replay-event", daily_advisory_id, as_of.isoformat()),
        "as_of": as_of.isoformat(),
        "daily_advisory_id": daily_advisory_id,
        "recommended_action": _text(
            manifest.get("recommended_action") or actions.get("recommended_action"),
            "MISSING",
        ),
        "source_artifacts": {
            "shadow_monitor_run_id": source_shadow_monitor_id,
            "shadow_monitor_manifest_path": str(
                shadow_monitor_run_dir / source_shadow_monitor_id / "shadow_monitor_manifest.json"
            )
            if source_shadow_monitor_id
            else "",
            "daily_advisory_id": daily_advisory_id,
            "daily_advisory_manifest_path": str(advisory_dir / "daily_advisory_manifest.json"),
            "consensus_drift_id": _text(consensus_drift.get("drift_id")),
            "consensus_drift_manifest_path": _text(consensus_drift.get("manifest_path")),
            "owner_review_id": _text(owner_review.get("review_id")),
            "paper_portfolio_id": _text(paper_action_weights.get("paper_portfolio_id")),
        },
        "available_inputs": available_inputs,
        "decision_inputs": {
            "current_weights": current_weights,
            "target_weights": consensus_weights,
            "consensus_weights": consensus_weights,
            "limited_adjustment_weights": limited_weights,
            "owner_decision": _owner_decision(owner_review),
            "owner_decision_weights": _owner_decision_weights(
                current_weights=current_weights,
                limited_weights=limited_weights,
                owner_review=owner_review,
            ),
            "paper_action_weights": _mapping(paper_action_weights.get("weights")),
            "candidate_target_count": len(target_rows),
            "delta_row_count": len(delta_rows),
        },
        "pit_safety_status": pit_status,
        "replay_eligibility": eligibility,
        "replay_limitations": sorted(set(limitations)),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _historical_replay_event(row: Mapping[str, Any]) -> dict[str, Any]:
    inputs = _mapping(row.get("decision_inputs"))
    current = _normalize_weights(_mapping(inputs.get("current_weights")))
    consensus = _normalize_weights(_mapping(inputs.get("consensus_weights")))
    limited = _normalize_weights(_mapping(inputs.get("limited_adjustment_weights")))
    owner_weights = _normalize_weights(_mapping(inputs.get("owner_decision_weights"))) or current
    paper_weights = _normalize_weights(_mapping(inputs.get("paper_action_weights"))) or current
    variants = [
        _variant("no_trade", current, "Keep current weights unchanged"),
        _variant("consensus_target", consensus, "Move fully to consensus target weights"),
        _variant("limited_adjustment", limited, "Apply capped advisory deltas"),
        _variant("owner_decision", owner_weights, "Apply recorded owner decision if available"),
        _variant("paper_action", paper_weights, "Apply recorded paper action if available"),
    ]
    for variant in variants:
        weights = _mapping(variant.get("weights"))
        variant["turnover"] = (
            round(sum(abs(value) for value in _weight_deltas(current, weights).values()), 6)
            if weights
            else 0.0
        )
    missing = [
        variant["variant"] for variant in variants if not _mapping(variant.get("weights"))
    ]
    replay_status = "READY_FOR_OUTCOME" if not missing else "PARTIAL"
    return {
        "schema_version": SCHEMA_VERSION,
        "replay_event_id": row.get("replay_event_id"),
        "as_of": row.get("as_of"),
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "pit_safety_status": row.get("pit_safety_status"),
        "daily_advisory_id": row.get("daily_advisory_id"),
        "recommended_action": row.get("recommended_action"),
        "owner_decision": _text(inputs.get("owner_decision"), "missing"),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "target_weights": _mapping(inputs.get("target_weights")),
        "current_weights": current,
        "consensus_weights": consensus,
        "limited_adjustment_weights": limited,
        "variants": variants,
        "replay_status": replay_status,
        "skip_reasons": [],
        "limitations": _texts(row.get("replay_limitations")),
        "source_artifacts": _mapping(row.get("source_artifacts")),
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _backfilled_outcome_rows(
    *,
    event: Mapping[str, Any],
    windows: Sequence[int],
    prices: pd.DataFrame,
    price_dates: Sequence[date],
    generated_date: date,
) -> list[dict[str, Any]]:
    start = _date_from_any(event.get("as_of"))
    if start is None:
        return []
    variants = _records(event.get("variants"))
    variant_weights = {row["variant"]: _mapping(row.get("weights")) for row in variants}
    rows = []
    for window in windows:
        end = _nth_trading_date_after(price_dates, start, window)
        if end is None or end > generated_date:
            for variant in variants:
                rows.append(
                    _pending_outcome_row(
                        event=event,
                        variant=_text(variant.get("variant")),
                        window=window,
                        start=start,
                        end=end,
                        turnover=_float(_mapping(variant).get("turnover")),
                    )
                )
            continue
        returns = {
            name: _portfolio_metrics(prices, weights, start, end)
            for name, weights in variant_weights.items()
        }
        for variant in variants:
            name = _text(variant.get("variant"))
            metrics = returns.get(name, _missing_metrics())
            status = _text(metrics.get("status"), "INSUFFICIENT_DATA")
            rows.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "replay_event_id": event.get("replay_event_id"),
                    "daily_advisory_id": event.get("daily_advisory_id"),
                    "recommended_action": event.get("recommended_action"),
                    "as_of": start.isoformat(),
                    "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
                    "pit_safety_status": event.get("pit_safety_status"),
                    "variant": name,
                    "window_days": window,
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "return": metrics["return"],
                    "relative_to_no_trade": round(
                        metrics["return"] - returns.get("no_trade", _missing_metrics())["return"],
                        6,
                    )
                    if status == "AVAILABLE"
                    else 0.0,
                    "relative_to_consensus_target": round(
                        metrics["return"]
                        - returns.get("consensus_target", _missing_metrics())["return"],
                        6,
                    )
                    if status == "AVAILABLE"
                    else 0.0,
                    "relative_to_limited_adjustment": round(
                        metrics["return"]
                        - returns.get("limited_adjustment", _missing_metrics())["return"],
                        6,
                    )
                    if status == "AVAILABLE"
                    else 0.0,
                    "max_drawdown": metrics["max_drawdown"],
                    "realized_volatility": metrics["realized_volatility"],
                    "turnover": _float(_mapping(variant).get("turnover")),
                    "outcome_status": status,
                    "broker_action_taken": False,
                }
            )
    return rows


def _simulate_paper_history(
    events: Sequence[Mapping[str, Any]], *, variant: str, prices: pd.DataFrame
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    if not events:
        return [], [], _empty_sim_summary(variant)
    replay_variant = "no_trade" if variant == "no_trade_baseline" else variant
    history = []
    ledger = []
    current_weights = _normalize_weights(_mapping(events[0].get("current_weights")))
    value = 1.0
    peak = 1.0
    daily_returns = []
    previous_date = _date_from_any(events[0].get("as_of"))
    if previous_date is None:
        return [], [], _empty_sim_summary(variant)
    start_date = previous_date
    for event in events:
        event_date = _date_from_any(event.get("as_of"))
        if event_date is None:
            continue
        if event_date > previous_date:
            metrics = _portfolio_metrics(prices, current_weights, previous_date, event_date)
            day_return = metrics["return"] if metrics["status"] == "AVAILABLE" else 0.0
            value = round(value * (1.0 + day_return), 8)
            daily_returns.append(day_return)
            peak = max(peak, value)
        after = _variant_weights(event, replay_variant) or current_weights
        deltas = _weight_deltas(current_weights, after)
        turnover = round(sum(abs(value) for value in deltas.values()), 6)
        if turnover:
            ledger.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "date": event_date.isoformat(),
                    "variant": variant,
                    "before_weights": current_weights,
                    "after_weights": after,
                    "deltas": deltas,
                    "turnover": turnover,
                    "reason": "historical_replay_advisory",
                    "source_replay_event_id": event.get("replay_event_id"),
                    "broker_action_taken": False,
                }
            )
        current_weights = after
        drawdown = round(value / peak - 1.0, 6) if peak else 0.0
        history.append(
            {
                "schema_version": SCHEMA_VERSION,
                "date": event_date.isoformat(),
                "variant": variant,
                "weights": current_weights,
                "portfolio_value": value,
                "daily_return": daily_returns[-1] if daily_returns else 0.0,
                "drawdown": drawdown,
                "turnover": turnover,
                "source_replay_event_id": event.get("replay_event_id"),
                "broker_action_taken": False,
            }
        )
        previous_date = event_date
    end_date = _date_from_any(history[-1]["date"]) if history else start_date
    baseline_return = _portfolio_metrics(
        prices,
        _normalize_weights(_mapping(events[0].get("current_weights"))),
        start_date,
        end_date,
    )["return"]
    total_return = round(value - 1.0, 6)
    years = max((end_date - start_date).days / 365.25, 0.0)
    annualized = round((value ** (1 / years) - 1.0), 6) if years > 0 and value > 0 else 0.0
    summary = {
        "schema_version": SCHEMA_VERSION,
        "variant": variant,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "total_return": total_return,
        "annualized_return": annualized,
        "max_drawdown": round(min([row["drawdown"] for row in history] or [0.0]), 6),
        "realized_volatility": round(_realized_volatility(daily_returns), 6),
        "turnover": round(sum(_float(row.get("turnover")) for row in ledger), 6),
        "trade_count": len(ledger),
        "relative_to_no_trade": round(total_return - baseline_return, 6),
        "relative_to_baseline": round(total_return - baseline_return, 6),
        "simulation_status": "AVAILABLE" if history else "INSUFFICIENT_DATA",
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "broker_action_taken": False,
    }
    return history, ledger, summary


def _advisory_rule_effectiveness(
    rows: Sequence[Mapping[str, Any]],
    variant_summary: Mapping[str, Any],
    sim_summary: Mapping[str, Any],
) -> dict[str, Any]:
    available = [row for row in rows if row.get("outcome_status") == "AVAILABLE"]
    by_action: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in available:
        by_action[_text(row.get("recommended_action"), "MISSING")].append(row)
    recommendation_effectiveness = []
    for action, action_rows in sorted(by_action.items()):
        rel_5 = _window_values(action_rows, 5, "relative_to_no_trade")
        rel_20 = _window_values(action_rows, 20, "relative_to_no_trade")
        recommendation_effectiveness.append(
            {
                "recommended_action": action,
                "event_count": len({_text(row.get("replay_event_id")) for row in action_rows}),
                "avg_relative_to_no_trade_5d": round(_avg(rel_5), 6),
                "avg_relative_to_no_trade_20d": round(_avg(rel_20), 6),
                "false_alarm_rate": round(
                    sum(1 for value in rel_5 if value <= 0) / len(rel_5), 6
                )
                if rel_5
                else 0.0,
                "missed_opportunity_rate": round(
                    sum(1 for value in rel_5 if value > 0) / len(rel_5), 6
                )
                if rel_5
                else 0.0,
            }
        )
    variant_effectiveness = [
        {
            "variant": row.get("variant"),
            "win_rate_vs_no_trade": row.get("win_rate_vs_no_trade_5d", 0.0),
            "avg_return_delta": row.get("avg_relative_to_no_trade_5d", 0.0),
            "drawdown_delta": row.get("avg_max_drawdown_20d", 0.0),
            "turnover": sim_summary.get("turnover", 0.0)
            if row.get("variant") == sim_summary.get("variant")
            else row.get("avg_turnover", 0.0),
        }
        for row in _records(variant_summary.get("summary"))
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_advisory_rule_effectiveness",
        "recommendation_effectiveness": recommendation_effectiveness,
        "variant_effectiveness": variant_effectiveness,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _calibration_recommendations(
    variant_summary: Mapping[str, Any], sim_summary: Mapping[str, Any]
) -> dict[str, Any]:
    available = _int(variant_summary.get("available_count"))
    best = _text(variant_summary.get("best_variant"), "MISSING")
    limited_delta = _float(variant_summary.get("limited_adjustment_vs_no_trade_5d"))
    recommendations = []
    if available == 0:
        recommendations.append(
            {
                "type": "continue_forward_tracking",
                "priority": "HIGH",
                "reason": "historical replay outcome has no AVAILABLE windows yet",
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
            }
        )
    elif best == "limited_adjustment" and limited_delta > 0:
        recommendations.append(
            {
                "type": "keep_current_rules",
                "priority": "MEDIUM",
                "reason": "limited_adjustment ranks best and is positive versus no_trade",
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
            }
        )
    elif best == "no_trade":
        recommendations.append(
            {
                "type": "tighten_adjustment",
                "priority": "MEDIUM",
                "reason": "no_trade ranks best in available historical replay windows",
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
            }
        )
    else:
        recommendations.append(
            {
                "type": "continue_forward_tracking",
                "priority": "MEDIUM",
                "reason": f"best historical replay variant is {best}; owner review required",
                "affected_config": "position_advisory_v1.yaml",
                "requires_owner_approval": True,
            }
        )
    recommendations.append(
        {
            "type": "continue_forward_tracking",
            "priority": "LOW",
            "reason": "FORWARD_OUTCOME remains higher confidence than backfilled replay",
            "affected_config": "paper_portfolio_v1.yaml",
            "requires_owner_approval": True,
        }
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_calibration_recommendations",
        "recommendations": recommendations,
        "simulation_status": sim_summary.get("simulation_status", "MISSING"),
        "production_effect": "none",
        "broker_action_taken": False,
        "automatic_config_update": False,
    }


def _variant_performance_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
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
                "event_count": len({_text(row.get("replay_event_id")) for row in variant_rows}),
                "available_count": len(available),
                "avg_1d_return": round(_avg(_window_values(available, 1, "return")), 6),
                "avg_5d_return": round(_avg(_window_values(available, 5, "return")), 6),
                "avg_10d_return": round(_avg(_window_values(available, 10, "return")), 6),
                "avg_20d_return": round(_avg(_window_values(available, 20, "return")), 6),
                "avg_relative_to_no_trade_5d": round(_avg(rel_5), 6),
                "win_rate_vs_no_trade_5d": round(
                    sum(1 for value in rel_5 if value > 0) / len(rel_5), 6
                )
                if rel_5
                else 0.0,
                "avg_max_drawdown_20d": round(
                    _avg(_window_values(available, 20, "max_drawdown")),
                    6,
                ),
                "avg_turnover": round(_avg([_float(row.get("turnover")) for row in available]), 6),
            }
        )
    best = ""
    ranked = [
        row
        for row in summary
        if row["available_count"] and row["variant"] not in {"owner_decision", "paper_action"}
    ]
    if ranked:
        best = max(ranked, key=lambda row: row["avg_relative_to_no_trade_5d"])["variant"]
    limited = next((row for row in summary if row["variant"] == "limited_adjustment"), {})
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_variant_performance_summary",
        "summary": summary,
        "available_count": sum(row["available_count"] for row in summary),
        "best_variant": best or "MISSING",
        "limited_adjustment_vs_no_trade_5d": limited.get("avg_relative_to_no_trade_5d", 0.0),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _current_weights(
    *, advisory_dir: Path, as_of: date, paper_portfolio_dir: Path
) -> tuple[dict[str, float], list[str]]:
    limitations = []
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        current = _mapping(row.get("current_weights"))
        if current:
            return _normalize_weights(current), limitations
    paper = _paper_weights_at_or_before(as_of=as_of, paper_portfolio_dir=paper_portfolio_dir)
    if paper:
        limitations.append("PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_PAPER_STATE")
        return paper, limitations
    limitations.append("PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_BASELINE")
    return _baseline_weights(), limitations


def _consensus_weights(
    advisory_dir: Path, target_rows: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, float], list[str]]:
    path = advisory_dir / "daily_consensus_weights.csv"
    if path.exists():
        rows = _read_csv(path)
        weights = {}
        for row in rows:
            symbol = _text(row.get("symbol"))
            if not symbol:
                continue
            weights[symbol] = _float(row.get("median_target_weight", row.get("mean_target_weight")))
        if weights:
            return _normalize_weights(weights), []
    collected: dict[str, list[float]] = defaultdict(list)
    for row in target_rows:
        for symbol, value in _mapping(row.get("target_weights")).items():
            collected[_text(symbol)].append(_float(value))
    if not collected:
        return {}, ["MISSING_TARGET_WEIGHTS"]
    rebuilt = {symbol: _avg(values) for symbol, values in collected.items()}
    return _normalize_weights(rebuilt), ["CONSENSUS_WEIGHTS_RECONSTRUCTED_FROM_TARGETS"]


def _limited_adjustment_weights(
    current_weights: Mapping[str, Any],
    target_weights: Mapping[str, Any],
    config: Mapping[str, Any],
) -> dict[str, float]:
    current = _normalize_weights(current_weights)
    target = _normalize_weights(target_weights)
    if not current or not target:
        return current
    symbols = sorted(set(current) | set(target))
    raw_delta = {
        symbol: round(_float(target.get(symbol)) - _float(current.get(symbol)), 6)
        for symbol in symbols
    }
    limits = _mapping(config.get("simulation"))
    max_total = _float(limits.get("max_single_day_total_adjustment"), 1.0)
    max_symbol = _float(limits.get("max_single_symbol_adjustment"), 1.0)
    min_trade = _float(limits.get("min_trade_threshold"), 0.0)
    if all(abs(value) < min_trade for value in raw_delta.values()):
        return current
    total_abs = sum(abs(value) for value in raw_delta.values())
    max_abs = max([abs(value) for value in raw_delta.values()] or [0.0])
    scale = 1.0
    if total_abs > max_total > 0:
        scale = min(scale, max_total / total_abs)
    if max_abs > max_symbol > 0:
        scale = min(scale, max_symbol / max_abs)
    limited = {symbol: round(value * scale, 6) for symbol, value in raw_delta.items()}
    drift = round(sum(limited.values()), 6)
    if drift:
        limited["CASH"] = round(_float(limited.get("CASH")) - drift, 6)
    return _apply_weight_deltas(current, limited)


def _paper_weights_at_or_before(*, as_of: date, paper_portfolio_dir: Path) -> dict[str, float]:
    candidates: list[tuple[date, dict[str, Any]]] = []
    for path in paper_portfolio_dir.glob("*/paper_position_history.jsonl"):
        for row in _read_jsonl(path):
            row_date = _date_from_any(row.get("as_of"))
            if row_date is not None and row_date <= as_of:
                candidates.append((row_date, row))
    if not candidates:
        return {}
    latest = sorted(candidates, key=lambda item: item[0])[-1][1]
    return _normalize_weights(_mapping(latest.get("positions")))


def _paper_action_weights_for_daily(
    *, daily_advisory_id: str, paper_portfolio_dir: Path
) -> dict[str, Any]:
    candidates = []
    for path in paper_portfolio_dir.glob("*/paper_action_ledger.jsonl"):
        for row in _read_jsonl(path):
            if _text(row.get("daily_advisory_id")) == daily_advisory_id:
                candidates.append((_date_from_any(row.get("created_at")) or date.min, row, path))
    if not candidates:
        return {}
    _, row, path = sorted(candidates, key=lambda item: item[0])[-1]
    return {
        "paper_portfolio_id": _text(row.get("paper_portfolio_id"), path.parent.name),
        "weights": _normalize_weights(_mapping(row.get("after_weights"))),
    }


def _owner_review_for_daily(
    daily_advisory_id: str, owner_reviews: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in owner_reviews
        if _text(row.get("daily_advisory_id")) == daily_advisory_id
    ]
    return matches[-1] if matches else {}


def _owner_decision(owner_review: Mapping[str, Any]) -> str:
    decision = _text(owner_review.get("owner_decision"))
    return decision if decision and decision != "pending" else "missing"


def _owner_decision_weights(
    *,
    current_weights: Mapping[str, Any],
    limited_weights: Mapping[str, Any],
    owner_review: Mapping[str, Any],
) -> dict[str, float]:
    decision = _owner_decision(owner_review)
    if decision in {"paper_adjustment", "manual_adjustment"}:
        return _normalize_weights(limited_weights)
    return _normalize_weights(current_weights)


def _consensus_drift_for_monitor(
    shadow_monitor_run_id: str, *, consensus_drift_dir: Path
) -> dict[str, Any]:
    if not shadow_monitor_run_id:
        return {}
    candidates = []
    for child in consensus_drift_dir.glob("*"):
        if not child.is_dir():
            continue
        manifest = _read_optional_json(child / "consensus_drift_manifest.json") or {}
        if _text(manifest.get("source_shadow_monitor_run_id")) == shadow_monitor_run_id:
            candidates.append(child)
    if not candidates:
        return {}
    latest = max(candidates, key=lambda path: path.stat().st_mtime)
    manifest = _read_optional_json(latest / "consensus_drift_manifest.json") or {}
    summary = _read_optional_json(latest / "consensus_drift_summary.json") or {}
    return {
        **summary,
        "drift_id": manifest.get("drift_id", latest.name),
        "manifest_path": str(latest / "consensus_drift_manifest.json"),
    }


def _pit_status_and_eligibility(limitations: Sequence[str]) -> tuple[str, str]:
    if "MISSING_TARGET_WEIGHTS" in limitations:
        return "PIT_UNSAFE", "INELIGIBLE"
    warning_prefixes = {
        "OWNER_DECISION_MISSING",
        "PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_BASELINE",
        "PORTFOLIO_SNAPSHOT_APPROXIMATED_FROM_PAPER_STATE",
        "CONSENSUS_WEIGHTS_RECONSTRUCTED_FROM_TARGETS",
        "MISSING_PRICE_DATA",
        "ADVISORY_GENERATED_AFTER_AS_OF_DATE",
    }
    if any(item in warning_prefixes for item in limitations):
        return "PIT_WARNING", "PARTIAL"
    return "PIT_SAFE", "ELIGIBLE"


def _pit_safety_audit(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counter = Counter(_text(row.get("pit_safety_status")) for row in rows)
    limitations = Counter(
        limitation
        for row in rows
        for limitation in _texts(row.get("replay_limitations"))
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_pit_safety_audit",
        "status": "PASS" if not counter.get("PIT_UNSAFE") else "PASS_WITH_WARNINGS",
        "total_replay_events": len(rows),
        "pit_safe_count": counter.get("PIT_SAFE", 0),
        "pit_warning_count": counter.get("PIT_WARNING", 0),
        "pit_unsafe_count": counter.get("PIT_UNSAFE", 0),
        "top_limitations": [
            {"limitation": limitation, "count": count}
            for limitation, count in limitations.most_common()
        ],
        "pit_unsafe_allowed_in_default_replay": False,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _replay_coverage_summary(
    rows: Sequence[Mapping[str, Any]], *, start: date, end: date
) -> dict[str, Any]:
    eligibility = Counter(_text(row.get("replay_eligibility")) for row in rows)
    by_date = Counter(_text(row.get("as_of")) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_coverage_summary",
        "status": "AVAILABLE" if rows else "INSUFFICIENT_DATA",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "total_replay_events": len(rows),
        "eligible_count": eligibility.get("ELIGIBLE", 0),
        "partial_count": eligibility.get("PARTIAL", 0),
        "ineligible_count": eligibility.get("INELIGIBLE", 0),
        "dates_with_replay_events": sorted(by_date),
        "events_by_date": dict(sorted(by_date.items())),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _replay_action_summary(
    events: Sequence[Mapping[str, Any]], skipped: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    action_counter = Counter(_text(row.get("recommended_action")) for row in events)
    variant_counter = Counter(
        _text(variant.get("variant"))
        for row in events
        for variant in _records(row.get("variants"))
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_action_summary",
        "replay_event_count": len(events),
        "skipped_count": len(skipped),
        "recommended_action_counts": dict(sorted(action_counter.items())),
        "variant_counts": dict(sorted(variant_counter.items())),
        "broker_action_present": any(row.get("broker_action_taken") is True for row in events),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _historical_decision_input(row: Mapping[str, Any], event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "replay_event_id": row.get("replay_event_id"),
        "daily_advisory_id": row.get("daily_advisory_id"),
        "as_of": row.get("as_of"),
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "pit_safety_status": row.get("pit_safety_status"),
        "source_artifacts": _mapping(row.get("source_artifacts")),
        "decision_inputs": _mapping(row.get("decision_inputs")),
        "generated_variants": [
            {"variant": variant.get("variant"), "turnover": variant.get("turnover")}
            for variant in _records(event.get("variants"))
        ],
        "future_prices_used_for_decision_input": False,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _skip_row(row: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {
        "replay_event_id": row.get("replay_event_id"),
        "daily_advisory_id": row.get("daily_advisory_id"),
        "as_of": row.get("as_of"),
        "pit_safety_status": row.get("pit_safety_status"),
        "skip_reason": reason,
    }


def _variant(name: str, weights: Mapping[str, Any], description: str) -> dict[str, Any]:
    clean = _normalize_weights(weights)
    return {
        "variant": name,
        "description": description,
        "weights": clean,
        "turnover": 0.0,
        "broker_action_taken": False,
    }


def _variant_weights(event: Mapping[str, Any], variant: str) -> dict[str, float]:
    for row in _records(event.get("variants")):
        if _text(row.get("variant")) == variant:
            return _normalize_weights(_mapping(row.get("weights")))
    return {}


def _pending_outcome_row(
    *,
    event: Mapping[str, Any],
    variant: str,
    window: int,
    start: date,
    end: date | None,
    turnover: float,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "replay_event_id": event.get("replay_event_id"),
        "daily_advisory_id": event.get("daily_advisory_id"),
        "recommended_action": event.get("recommended_action"),
        "as_of": start.isoformat(),
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "pit_safety_status": event.get("pit_safety_status"),
        "variant": variant,
        "window_days": window,
        "start_date": start.isoformat(),
        "end_date": "" if end is None else end.isoformat(),
        "return": 0.0,
        "relative_to_no_trade": 0.0,
        "relative_to_consensus_target": 0.0,
        "relative_to_limited_adjustment": 0.0,
        "max_drawdown": 0.0,
        "realized_volatility": 0.0,
        "turnover": turnover,
        "outcome_status": "PENDING",
        "broker_action_taken": False,
    }


def _portfolio_metrics(
    prices: pd.DataFrame, weights: Mapping[str, Any], start: date, end: date
) -> dict[str, Any]:
    try:
        result, daily_returns = _portfolio_return_and_path(prices, weights, start, end)
    except DynamicV3HistoricalReplayError:
        return _missing_metrics()
    return {
        "return": round(result, 6),
        "max_drawdown": round(_max_drawdown(daily_returns), 6),
        "realized_volatility": round(_realized_volatility(daily_returns), 6),
        "status": "AVAILABLE",
    }


def _missing_metrics() -> dict[str, Any]:
    return {
        "return": 0.0,
        "max_drawdown": 0.0,
        "realized_volatility": 0.0,
        "status": "INSUFFICIENT_DATA",
    }


def _run_cached_data_quality_gate(
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


def _load_prices_for_replay(prices_path: Path, events: Sequence[Mapping[str, Any]]) -> pd.DataFrame:
    symbols = set()
    for event in events:
        symbols.update(_mapping(event.get("current_weights")))
        for variant in _records(event.get("variants")):
            symbols.update(_mapping(variant.get("weights")))
    config = load_etf_config_bundle()
    prices, quality = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=symbols,
    )
    if not quality.passed:
        raise DynamicV3HistoricalReplayError(f"ETF price validation failed: {quality.status}")
    prices = prices.copy()
    prices["_date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date
    prices["_adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
    return prices.dropna(subset=["_date", "_adj_close"])


def _available_price_dates(prices: pd.DataFrame) -> list[date]:
    return sorted(date_value for date_value in prices["_date"].dropna().unique())


def _nth_trading_date_after(dates: Sequence[date], start: date, n: int) -> date | None:
    after = [item for item in dates if item > start]
    if len(after) < n:
        return None
    return after[n - 1]


def _portfolio_return_and_path(
    prices: pd.DataFrame,
    weights: Mapping[str, Any],
    start: date,
    end: date,
) -> tuple[float, list[float]]:
    clean = _normalize_weights(weights)
    if not clean:
        raise DynamicV3HistoricalReplayError("empty weights")
    pivot = prices.pivot_table(index="_date", columns="symbol", values="_adj_close", aggfunc="last")
    available_dates = sorted(pivot.index)
    prior = [item for item in available_dates if item <= start]
    if not prior or end not in pivot.index:
        raise DynamicV3HistoricalReplayError("missing start or end price")
    start_idx = prior[-1]
    path_dates = [item for item in available_dates if start_idx <= item <= end]
    total_return = 0.0
    for symbol, weight in clean.items():
        if symbol == "CASH":
            continue
        if symbol not in pivot.columns:
            raise DynamicV3HistoricalReplayError(f"missing symbol price: {symbol}")
        start_price = _float(pivot.loc[start_idx, symbol])
        end_price = _float(pivot.loc[end, symbol])
        if start_price <= 0 or end_price <= 0:
            raise DynamicV3HistoricalReplayError(f"invalid price for {symbol}")
        total_return += _float(weight) * (end_price / start_price - 1.0)
    daily_returns = []
    for left, right in zip(path_dates, path_dates[1:], strict=False):
        day_return = 0.0
        for symbol, weight in clean.items():
            if symbol == "CASH" or symbol not in pivot.columns:
                continue
            left_price = _float(pivot.loc[left, symbol])
            right_price = _float(pivot.loc[right, symbol])
            if left_price <= 0 or right_price <= 0:
                continue
            day_return += _float(weight) * (right_price / left_price - 1.0)
        daily_returns.append(day_return)
    return total_return, daily_returns


def _price_availability_index(prices_path: Path) -> dict[str, set[date]]:
    if not prices_path.exists():
        return {}
    try:
        frame = pd.read_csv(prices_path)
    except (OSError, pd.errors.ParserError):
        return {}
    if "date" not in frame.columns or "symbol" not in frame.columns:
        return {}
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    result: dict[str, set[date]] = defaultdict(set)
    for row in frame.dropna(subset=["_date"]).to_dict("records"):
        result[_text(row.get("symbol"))].add(row["_date"])
    return result


def _has_price_after(symbols: set[str], as_of: date, price_index: Mapping[str, set[date]]) -> bool:
    symbols = {symbol for symbol in symbols if symbol and symbol != "CASH"}
    if not symbols:
        return True
    return all(any(day > as_of for day in price_index.get(symbol, set())) for symbol in symbols)


def _download_manifest_path(prices_path: Path) -> Path | None:
    candidate = prices_path.parent / "download_manifests" / "prices_daily_download_manifest.json"
    return candidate if candidate.exists() else None


def _marketstack_prices_path(prices_path: Path) -> Path | None:
    candidate = prices_path.with_name("prices_marketstack_daily.csv")
    return candidate if candidate.exists() else None


def _requires_marketstack_prices(prices_path: Path) -> bool:
    try:
        return prices_path.resolve() == DEFAULT_ETF_PRICE_PATH.resolve()
    except FileNotFoundError:
        return prices_path == DEFAULT_ETF_PRICE_PATH


def _baseline_weights() -> dict[str, float]:
    config = load_etf_config_bundle()
    return _normalize_weights(
        {
            symbol: asset.default_weight
            for symbol, asset in config.assets.assets.items()
            if asset.default_weight > 0
        }
    )


def _configured_outcome_windows(config: Mapping[str, Any]) -> list[int]:
    values = _mapping(config.get("outcome_tracking")).get("windows_trading_days", OUTCOME_WINDOWS)
    windows = [int(value) for value in values if int(value) > 0]
    return windows or list(OUTCOME_WINDOWS)


def _max_drawdown(daily_returns: Sequence[float]) -> float:
    wealth = 1.0
    peak = 1.0
    max_dd = 0.0
    for value in daily_returns:
        wealth *= 1.0 + value
        peak = max(peak, wealth)
        max_dd = min(max_dd, wealth / peak - 1.0)
    return max_dd


def _realized_volatility(daily_returns: Sequence[float]) -> float:
    if len(daily_returns) < 2:
        return 0.0
    series = pd.Series(list(daily_returns), dtype=float)
    return float(series.std(ddof=1) * (252**0.5))


def _empty_sim_summary(variant: str) -> dict[str, Any]:
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
        "outcome_mode": OUTCOME_MODE_HISTORICAL_REPLAY,
        "broker_action_taken": False,
    }


def _weight_deltas(left: Mapping[str, Any], right: Mapping[str, Any]) -> dict[str, float]:
    symbols = sorted(set(left) | set(right))
    return {
        symbol: round(_float(right.get(symbol)) - _float(left.get(symbol)), 6)
        for symbol in symbols
        if round(_float(right.get(symbol)) - _float(left.get(symbol)), 6) != 0
    }


def _apply_weight_deltas(weights: Mapping[str, Any], deltas: Mapping[str, Any]) -> dict[str, float]:
    symbols = sorted(set(weights) | set(deltas))
    result = {
        symbol: round(max(0.0, _float(weights.get(symbol)) + _float(deltas.get(symbol))), 6)
        for symbol in symbols
    }
    return _normalize_weights(result)


def _normalize_weights(weights: Mapping[str, Any]) -> dict[str, float]:
    clean = {_text(symbol): _float(value) for symbol, value in weights.items() if _text(symbol)}
    clean = {symbol: value for symbol, value in clean.items() if value > 0}
    total = sum(clean.values())
    if total <= 0:
        return {}
    normalized = {symbol: round(value / total, 6) for symbol, value in sorted(clean.items())}
    drift = round(1.0 - sum(normalized.values()), 6)
    if drift and "CASH" in normalized:
        normalized["CASH"] = round(normalized["CASH"] + drift, 6)
    elif drift and normalized:
        first = sorted(normalized)[0]
        normalized[first] = round(normalized[first] + drift, 6)
    return {symbol: value for symbol, value in normalized.items() if value > 0}


def _window_values(rows: Sequence[Mapping[str, Any]], window: int, key: str) -> list[float]:
    return [_float(row.get(key)) for row in rows if _int(row.get("window_days")) == window]


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if pd.notna(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _validation_payload(
    *, report_type: str, artifact_id_key: str, artifact_id: str, checks: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    status = "PASS" if all(check["passed"] for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        artifact_id_key: artifact_id,
        "status": status,
        "checks": list(checks),
        "failed_check_count": sum(1 for check in checks if check.get("passed") is not True),
        "family": STRATEGY_FAMILY,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}


def _artifact_dir_from_latest(
    *, output_dir: Path, artifact_id: str | None, pointer_name: str
) -> Path:
    if artifact_id:
        path = output_dir / artifact_id
        if not path.exists():
            raise DynamicV3HistoricalReplayError(f"artifact not found: {artifact_id}")
        return path
    pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{pointer_name}.json") or {}
    pointer_id = _text(pointer.get("artifact_id"))
    if pointer_id and (output_dir / pointer_id).exists():
        return output_dir / pointer_id
    latest = _latest_child_dir(output_dir)
    if latest is None:
        raise DynamicV3HistoricalReplayError(f"latest artifact not found for {pointer_name}")
    return latest


def _update_latest_pointer(name: str, artifact_id: str, path: Path) -> None:
    if not _is_dynamic_v3_artifact(path):
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


def _is_dynamic_v3_artifact(path: Path) -> bool:
    try:
        return path.resolve(strict=False).is_relative_to(
            DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve(strict=False)
        )
    except (OSError, RuntimeError, ValueError):
        return False


def _latest_child_dir(path: Path) -> Path | None:
    if not path.exists():
        return None
    dirs = [child for child in path.iterdir() if child.is_dir()]
    return max(dirs, key=lambda child: child.stat().st_mtime) if dirs else None


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3HistoricalReplayError(f"could not allocate unique artifact dir: {path}")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3HistoricalReplayError(f"required JSON artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n")


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = _text(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _stable_id(*parts: Any) -> str:
    return sha256(_canonical_json(parts).encode("utf-8")).hexdigest()[:16]


def _canonical_json(payload: Any) -> str:
    return json.dumps(_jsonable(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if isinstance(value, set):
        return sorted(_jsonable(item) for item in value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, Sequence):
        return [_text(item) for item in value if _text(item)]
    return []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
