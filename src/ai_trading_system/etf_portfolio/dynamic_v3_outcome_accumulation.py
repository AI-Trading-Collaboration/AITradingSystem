from __future__ import annotations

import csv
import json
import math
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
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    DEFAULT_BACKFILL_REPAIR_DIR,
    DEFAULT_BACKFILLED_OUTCOME_DIR,
    DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    DEFAULT_HISTORICAL_REPLAY_DIR,
    DEFAULT_REPLAY_DIAGNOSIS_DIR,
    DEFAULT_REPLAY_INVENTORY_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_paper_tracking import (
    DEFAULT_ADVISORY_OUTCOME_DIR,
    DEFAULT_RATES_CACHE_PATH,
    update_advisory_outcome,
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

DEFAULT_OUTCOME_DUE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_due"
DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "replay_sample_expansion"
DEFAULT_OUTCOME_DASHBOARD_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "outcome_dashboard"
DEFAULT_LIMITED_VS_NOTRADE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "limited_vs_notrade"
DEFAULT_CONSENSUS_RISK_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "consensus_risk"

OUTCOME_WINDOWS = (1, 5, 10, 20)
OUTCOME_WINDOW_STATUSES = {"AVAILABLE", "PENDING", "INSUFFICIENT_DATA"}
OUTCOME_DUE_STATUSES = {
    "DUE",
    "NOT_DUE",
    "PRICE_MISSING",
    "ALREADY_AVAILABLE",
    "INSUFFICIENT_DATA",
}
PIT_SAFETY_STATUSES = {"PIT_SAFE", "PIT_WARNING", "PIT_UNSAFE"}
REPLAY_ELIGIBILITY_STATUSES = {"ELIGIBLE", "PARTIAL", "INELIGIBLE"}
OUTCOME_MODES = {"FORWARD_OUTCOME", "HISTORICAL_REPLAY", "BACKTEST_SIMULATION"}
REPLAY_VARIANTS = {
    "no_trade",
    "consensus_target",
    "limited_adjustment",
    "owner_decision",
    "paper_action",
}

# Pilot confidence floors documented in
# docs/requirements/TRADING-151_to_155_Forward_Outcome_Accumulation_and_Replay_Sample_Expansion.md.
# They only label review confidence and never authorize policy mutation,
# production, or broker action.
FOCUSED_MEDIUM_CONFIDENCE_SAMPLE_FLOOR = 5
FOCUSED_HIGH_CONFIDENCE_SAMPLE_FLOOR = 20
CONSENSUS_RISK_REVIEW_SAMPLE_FLOOR = 5
CONSENSUS_SEMICONDUCTOR_EXPOSURE_REVIEW_LEVEL = 0.50
CONSENSUS_RISK_ASSET_EXPOSURE_REVIEW_LEVEL = 0.95
CONSENSUS_DRAWDOWN_REVIEW_DELTA = -0.02
CONSENSUS_TURNOVER_REVIEW_LEVEL = 0.50

SEMICONDUCTOR_SYMBOLS = {
    "AMD",
    "AMAT",
    "ASML",
    "AVGO",
    "INTC",
    "KLAC",
    "LRCX",
    "MU",
    "NVDA",
    "SMH",
    "SOXX",
    "TSM",
}

PENDING_REASON_ACTIONS = {
    "future_window_not_reached": "wait_for_due_window_or_use_older_replay_events",
    "missing_price_data": "refresh_price_cache",
    "no_available_outcome_windows": "expand_replay_samples_or_wait_for_forward_outcomes",
    "review_waiting_for_backfill": "run_backfill_repair_or_outcome_due_update",
    "insufficient_replay_events": "run_replay_sample_expansion",
    "unknown": "manual_investigation_required",
}


class DynamicV3OutcomeAccumulationError(ValueError):
    """Raised when outcome accumulation artifacts fail closed."""


def run_outcome_due_scan(
    *,
    as_of: date,
    output_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    enforce_data_quality_gate: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    due_id = _stable_id("outcome-due", as_of.isoformat(), generated.isoformat())
    due_dir = _unique_dir(output_dir / due_id)
    due_dir.mkdir(parents=True, exist_ok=False)
    quality_status, quality_report_path = _quality_gate_for_cached_data(
        as_of=as_of,
        generated=generated,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=due_dir / "validate_data_quality_report.md",
        enforce=enforce_data_quality_gate,
    )
    price_index = _price_availability_index(prices_path)
    latest_price_date = _latest_price_date_on_or_before(price_index, as_of)
    price_dates = _all_price_dates(price_index)
    inventory: list[dict[str, Any]] = []
    for outcome_dir in _artifact_children(advisory_outcome_dir):
        manifest = _read_optional_json(outcome_dir / "advisory_outcome_manifest.json") or {}
        event = _read_optional_json(outcome_dir / "advisory_event.json") or {}
        if not manifest:
            continue
        for row in _read_jsonl(outcome_dir / "outcome_windows.jsonl"):
            inventory.append(
                _due_inventory_row(
                    outcome_id=_text(manifest.get("outcome_id"), outcome_dir.name),
                    manifest=manifest,
                    event=event,
                    window=row,
                    price_index=price_index,
                    price_dates=price_dates,
                    latest_price_date=latest_price_date,
                )
            )
    inventory = sorted(
        inventory,
        key=lambda row: (
            _text(row.get("as_of")),
            _text(row.get("daily_advisory_id")),
            _int(row.get("window_days")),
            _text(row.get("outcome_id")),
        ),
    )
    update_ready = [row for row in inventory if row.get("can_update") is True]
    summary = _pending_window_summary(
        inventory,
        as_of=as_of,
        latest_price_date=latest_price_date,
    )
    status = "PASS"
    if not inventory:
        status = "INSUFFICIENT_DATA"
    elif summary["price_missing_windows"]:
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_due_manifest",
        "due_id": due_dir.name,
        "generated_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "status": status,
        "latest_price_date": "" if latest_price_date is None else latest_price_date.isoformat(),
        "data_quality_status": quality_status,
        "data_quality_report_path": quality_report_path,
        "advisory_outcome_dir": str(advisory_outcome_dir),
        "prices_path": str(prices_path),
        "outcome_due_manifest_path": str(due_dir / "outcome_due_manifest.json"),
        "due_window_inventory_path": str(due_dir / "due_window_inventory.jsonl"),
        "pending_window_summary_path": str(due_dir / "pending_window_summary.json"),
        "update_ready_list_path": str(due_dir / "update_ready_list.json"),
        "outcome_due_report_path": str(due_dir / "outcome_due_report.md"),
        "update_ready_count": summary["update_ready_count"],
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(due_dir / "outcome_due_manifest.json", manifest)
    _write_jsonl(due_dir / "due_window_inventory.jsonl", inventory)
    _write_json(due_dir / "pending_window_summary.json", summary)
    _write_json(due_dir / "update_ready_list.json", {"update_ready": update_ready})
    _write_text(
        due_dir / "outcome_due_report.md",
        render_outcome_due_report(manifest, summary, update_ready),
    )
    _update_latest_pointer(
        "latest_outcome_due", due_dir.name, due_dir / "outcome_due_manifest.json"
    )
    return {
        "due_id": due_dir.name,
        "due_dir": due_dir,
        "manifest": manifest,
        "due_window_inventory": inventory,
        "pending_window_summary": summary,
        "update_ready_list": update_ready,
    }


def outcome_due_update_ready(
    *,
    due_id: str,
    output_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    paper_portfolio_dir: Path | None = None,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    rates_path: Path = DEFAULT_RATES_CACHE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    due_dir = output_dir / due_id
    manifest = _read_json(due_dir / "outcome_due_manifest.json")
    as_of = _date_from_any(manifest.get("as_of"))
    if as_of is None:
        raise DynamicV3OutcomeAccumulationError("outcome due manifest missing as_of")
    ready_payload = _read_json(due_dir / "update_ready_list.json")
    ready_rows = [
        row for row in _records(ready_payload.get("update_ready")) if row.get("can_update")
    ]
    updated = []
    seen: set[str] = set()
    for row in ready_rows:
        outcome_id = _text(row.get("outcome_id"))
        if not outcome_id or outcome_id in seen:
            continue
        seen.add(outcome_id)
        result = update_advisory_outcome(
            as_of=as_of,
            outcome_id=outcome_id,
            output_dir=advisory_outcome_dir,
            paper_portfolio_dir=(
                DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "paper_portfolio"
                if paper_portfolio_dir is None
                else paper_portfolio_dir
            ),
            prices_path=prices_path,
            rates_path=rates_path,
            generated_at=generated,
        )
        rows = result["outcome_windows"]
        updated.append(
            {
                "outcome_id": outcome_id,
                "status": result["manifest"].get("status", "MISSING"),
                "available_windows": sum(
                    1 for item in rows if item.get("outcome_status") == "AVAILABLE"
                ),
                "pending_windows": sum(
                    1 for item in rows if item.get("outcome_status") == "PENDING"
                ),
                "insufficient_data_windows": sum(
                    1 for item in rows if item.get("outcome_status") == "INSUFFICIENT_DATA"
                ),
                "updated_at": generated.isoformat(),
            }
        )
    execution = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_due_update_ready_execution",
        "due_id": due_id,
        "executed_at": generated.isoformat(),
        "as_of": as_of.isoformat(),
        "ready_window_count": len(ready_rows),
        "updated_outcome_count": len(updated),
        "updated_outcomes": updated,
        "not_due_windows_updated": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
    }
    _write_json(due_dir / "update_ready_execution.json", execution)
    return {"due_id": due_id, "execution": execution}


def outcome_due_report_payload(
    *,
    due_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
) -> dict[str, Any]:
    due_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=due_id if not latest else None,
        pointer_name="latest_outcome_due",
    )
    return {
        **_read_json(due_dir / "outcome_due_manifest.json"),
        "due_window_inventory": _read_jsonl(due_dir / "due_window_inventory.jsonl"),
        "pending_window_summary": _read_json(due_dir / "pending_window_summary.json"),
        "update_ready_list": _records(
            _read_json(due_dir / "update_ready_list.json").get("update_ready")
        ),
        "due_dir": str(due_dir),
    }


def validate_outcome_due_artifact(
    *, due_id: str, output_dir: Path = DEFAULT_OUTCOME_DUE_DIR
) -> dict[str, Any]:
    due_dir = output_dir / due_id
    manifest = _read_optional_json(due_dir / "outcome_due_manifest.json") or {}
    rows = _read_jsonl(due_dir / "due_window_inventory.jsonl")
    ready = _records(
        (_read_optional_json(due_dir / "update_ready_list.json") or {}).get("update_ready")
    )
    checks = [
        _check("manifest_exists", (due_dir / "outcome_due_manifest.json").exists(), due_id),
        _check("inventory_exists", (due_dir / "due_window_inventory.jsonl").exists(), due_id),
        _check("summary_exists", (due_dir / "pending_window_summary.json").exists(), due_id),
        _check("update_ready_exists", (due_dir / "update_ready_list.json").exists(), due_id),
        _check("report_exists", (due_dir / "outcome_due_report.md").exists(), due_id),
        _check("due_id_matches", manifest.get("due_id") == due_id, due_id),
        _check(
            "due_status_valid",
            all(row.get("due_status") in OUTCOME_DUE_STATUSES for row in rows),
            "due status",
        ),
        _check(
            "update_ready_is_due_only",
            all(row.get("can_update") is True and row.get("due_status") == "DUE" for row in ready),
            "update-ready rows must be DUE",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_outcome_due_validation",
        artifact_id_key="due_id",
        artifact_id=due_id,
        checks=checks,
    )


def run_replay_sample_expansion(
    *,
    start: date,
    end: date,
    output_dir: Path = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    shadow_monitor_run_dir: Path = DEFAULT_SHADOW_MONITOR_RUN_DIR,
    consensus_drift_dir: Path = DEFAULT_CONSENSUS_DRIFT_DIR,
    owner_review_dir: Path = DEFAULT_OWNER_REVIEW_JOURNAL_DIR,
    replay_inventory_dir: Path = DEFAULT_REPLAY_INVENTORY_DIR,
    prices_path: Path = DEFAULT_ETF_PRICE_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    owner_reviews = _read_jsonl(owner_review_dir / "owner_review_journal.jsonl")
    price_index = _price_availability_index(prices_path)
    rows: list[dict[str, Any]] = []
    source_inventory: list[dict[str, Any]] = []
    daily_rows = _expanded_events_from_daily_advisory(
        daily_advisory_dir=daily_advisory_dir,
        owner_reviews=owner_reviews,
        price_index=price_index,
        start=start,
        end=end,
    )
    rows.extend(daily_rows)
    source_inventory.append(
        _source_inventory_row(
            source_type="daily_advisory",
            source_root=daily_advisory_dir,
            scanned_count=len(list(daily_advisory_dir.glob("*/daily_advisory_manifest.json"))),
            discovered_count=len(daily_rows),
        )
    )
    replay_rows = _expanded_events_from_replay_inventory(
        replay_inventory_dir=replay_inventory_dir,
        start=start,
        end=end,
    )
    rows.extend(replay_rows)
    source_inventory.append(
        _source_inventory_row(
            source_type="replay_inventory",
            source_root=replay_inventory_dir,
            scanned_count=len(list(replay_inventory_dir.glob("*/replay_artifact_inventory.jsonl"))),
            discovered_count=len(replay_rows),
        )
    )
    shadow_count = len(list(shadow_monitor_run_dir.glob("*/shadow_monitor_manifest.json")))
    source_inventory.append(
        _source_inventory_row(
            source_type="shadow_monitor",
            source_root=shadow_monitor_run_dir,
            scanned_count=shadow_count,
            discovered_count=0,
        )
    )
    source_inventory.append(
        _source_inventory_row(
            source_type="consensus_drift",
            source_root=consensus_drift_dir,
            scanned_count=len(list(consensus_drift_dir.glob("*/consensus_drift_manifest.json"))),
            discovered_count=0,
        )
    )
    rows = _dedupe_expanded_events(rows)
    summary = _pit_classification_summary(rows)
    expansion_id = _stable_id(
        "replay-sample-expansion",
        start.isoformat(),
        end.isoformat(),
        generated.isoformat(),
    )
    expansion_dir = _unique_dir(output_dir / expansion_id)
    expansion_dir.mkdir(parents=True, exist_ok=False)
    status = "PASS"
    if not rows:
        status = "INSUFFICIENT_DATA"
    elif summary["pit_unsafe_count"] or summary["pit_warning_count"]:
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_sample_expansion_manifest",
        "expansion_id": expansion_dir.name,
        "generated_at": generated.isoformat(),
        "start": start.isoformat(),
        "end": end.isoformat(),
        "status": status,
        "new_replay_event_count": len(rows),
        "pit_safe_count": summary["pit_safe_count"],
        "pit_warning_count": summary["pit_warning_count"],
        "pit_unsafe_count": summary["pit_unsafe_count"],
        "eligible_count": summary["eligible_count"],
        "partial_count": summary["partial_count"],
        "ineligible_count": summary["ineligible_count"],
        "candidate_source_inventory_path": str(expansion_dir / "candidate_source_inventory.jsonl"),
        "expanded_replay_events_path": str(expansion_dir / "expanded_replay_events.jsonl"),
        "pit_classification_summary_path": str(expansion_dir / "pit_classification_summary.json"),
        "replay_sample_expansion_report_path": str(
            expansion_dir / "replay_sample_expansion_report.md"
        ),
        "pit_unsafe_allowed_in_default_replay": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(expansion_dir / "expansion_manifest.json", manifest)
    _write_jsonl(expansion_dir / "candidate_source_inventory.jsonl", source_inventory)
    _write_jsonl(expansion_dir / "expanded_replay_events.jsonl", rows)
    _write_json(expansion_dir / "pit_classification_summary.json", summary)
    _write_text(
        expansion_dir / "replay_sample_expansion_report.md",
        render_replay_sample_expansion_report(manifest, summary, source_inventory),
    )
    _update_latest_pointer(
        "latest_replay_sample_expansion",
        expansion_dir.name,
        expansion_dir / "expansion_manifest.json",
    )
    return {
        "expansion_id": expansion_dir.name,
        "expansion_dir": expansion_dir,
        "manifest": manifest,
        "candidate_source_inventory": source_inventory,
        "expanded_replay_events": rows,
        "pit_classification_summary": summary,
    }


def replay_sample_expansion_report_payload(
    *,
    expansion_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
) -> dict[str, Any]:
    expansion_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=expansion_id if not latest else None,
        pointer_name="latest_replay_sample_expansion",
    )
    return {
        **_read_json(expansion_dir / "expansion_manifest.json"),
        "candidate_source_inventory": _read_jsonl(
            expansion_dir / "candidate_source_inventory.jsonl"
        ),
        "expanded_replay_events": _read_jsonl(expansion_dir / "expanded_replay_events.jsonl"),
        "pit_classification_summary": _read_json(expansion_dir / "pit_classification_summary.json"),
        "expansion_dir": str(expansion_dir),
    }


def validate_replay_sample_expansion_artifact(
    *,
    expansion_id: str,
    output_dir: Path = DEFAULT_REPLAY_SAMPLE_EXPANSION_DIR,
) -> dict[str, Any]:
    expansion_dir = output_dir / expansion_id
    manifest = _read_optional_json(expansion_dir / "expansion_manifest.json") or {}
    rows = _read_jsonl(expansion_dir / "expanded_replay_events.jsonl")
    checks = [
        _check(
            "manifest_exists", (expansion_dir / "expansion_manifest.json").exists(), expansion_id
        ),
        _check(
            "source_inventory_exists",
            (expansion_dir / "candidate_source_inventory.jsonl").exists(),
            expansion_id,
        ),
        _check(
            "expanded_events_exists",
            (expansion_dir / "expanded_replay_events.jsonl").exists(),
            expansion_id,
        ),
        _check(
            "pit_summary_exists",
            (expansion_dir / "pit_classification_summary.json").exists(),
            expansion_id,
        ),
        _check(
            "report_exists",
            (expansion_dir / "replay_sample_expansion_report.md").exists(),
            expansion_id,
        ),
        _check("expansion_id_matches", manifest.get("expansion_id") == expansion_id, expansion_id),
        _check(
            "pit_status_valid",
            all(row.get("pit_safety_status") in PIT_SAFETY_STATUSES for row in rows),
            "pit status",
        ),
        _check(
            "pit_unsafe_default_excluded",
            all(
                row.get("replay_eligibility") == "INELIGIBLE"
                for row in rows
                if row.get("pit_safety_status") == "PIT_UNSAFE"
            ),
            "PIT_UNSAFE must not be replay eligible by default",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_replay_sample_expansion_validation",
        artifact_id_key="expansion_id",
        artifact_id=expansion_id,
        checks=checks,
    )


def build_outcome_dashboard(
    *,
    output_dir: Path = DEFAULT_OUTCOME_DASHBOARD_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    paper_sim_dir: Path = DEFAULT_HISTORICAL_PAPER_SIM_DIR,
    diagnosis_dir: Path = DEFAULT_REPLAY_DIAGNOSIS_DIR,
    outcome_due_dir: Path = DEFAULT_OUTCOME_DUE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    forward_rows = _forward_outcome_rows(advisory_outcome_dir)
    historical_rows = _historical_outcome_rows(backfill_dir=backfill_dir, repair_dir=repair_dir)
    simulation_rows = _simulation_outcome_rows(paper_sim_dir)
    rows_by_mode = {
        "FORWARD_OUTCOME": forward_rows,
        "HISTORICAL_REPLAY": historical_rows,
        "BACKTEST_SIMULATION": simulation_rows,
    }
    matrix = _outcome_availability_matrix(rows_by_mode)
    mode_summary = _outcome_mode_summary(rows_by_mode)
    pending_dashboard = _pending_reason_dashboard(
        rows_by_mode=rows_by_mode,
        diagnosis_dir=diagnosis_dir,
        outcome_due_dir=outcome_due_dir,
    )
    reader_brief = _outcome_dashboard_reader_brief(matrix, pending_dashboard)
    dashboard_id = _stable_id("outcome-dashboard", generated.isoformat())
    dashboard_dir = _unique_dir(output_dir / dashboard_id)
    dashboard_dir.mkdir(parents=True, exist_ok=False)
    total_available = sum(item["available"] for item in matrix["summary"].values())
    total_pending = sum(item["pending"] for item in matrix["summary"].values())
    status = "PASS" if total_available else "INSUFFICIENT_DATA"
    if total_pending:
        status = "PASS_WITH_WARNINGS" if total_available else "PENDING"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_dashboard_manifest",
        "dashboard_id": dashboard_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "available_count": total_available,
        "pending_count": total_pending,
        "insufficient_data_count": sum(
            item["insufficient_data"] for item in matrix["summary"].values()
        ),
        "outcome_dashboard_manifest_path": str(dashboard_dir / "outcome_dashboard_manifest.json"),
        "outcome_availability_matrix_path": str(dashboard_dir / "outcome_availability_matrix.json"),
        "outcome_mode_summary_path": str(dashboard_dir / "outcome_mode_summary.json"),
        "pending_reason_dashboard_path": str(dashboard_dir / "pending_reason_dashboard.json"),
        "outcome_dashboard_report_path": str(dashboard_dir / "outcome_dashboard_report.md"),
        "reader_brief_section_path": str(dashboard_dir / "reader_brief_section.md"),
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(dashboard_dir / "outcome_dashboard_manifest.json", manifest)
    _write_json(dashboard_dir / "outcome_availability_matrix.json", matrix)
    _write_json(dashboard_dir / "outcome_mode_summary.json", mode_summary)
    _write_json(dashboard_dir / "pending_reason_dashboard.json", pending_dashboard)
    _write_text(
        dashboard_dir / "outcome_dashboard_report.md",
        render_outcome_dashboard_report(manifest, matrix, pending_dashboard),
    )
    _write_text(
        dashboard_dir / "reader_brief_section.md", render_reader_brief_section(reader_brief)
    )
    _update_latest_pointer(
        "latest_outcome_dashboard",
        dashboard_dir.name,
        dashboard_dir / "outcome_dashboard_manifest.json",
    )
    return {
        "dashboard_id": dashboard_dir.name,
        "dashboard_dir": dashboard_dir,
        "manifest": manifest,
        "outcome_availability_matrix": matrix,
        "outcome_mode_summary": mode_summary,
        "pending_reason_dashboard": pending_dashboard,
        "reader_brief": reader_brief,
    }


def outcome_dashboard_report_payload(
    *,
    dashboard_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_OUTCOME_DASHBOARD_DIR,
) -> dict[str, Any]:
    dashboard_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=dashboard_id if not latest else None,
        pointer_name="latest_outcome_dashboard",
    )
    return {
        **_read_json(dashboard_dir / "outcome_dashboard_manifest.json"),
        "outcome_availability_matrix": _read_json(
            dashboard_dir / "outcome_availability_matrix.json"
        ),
        "outcome_mode_summary": _read_json(dashboard_dir / "outcome_mode_summary.json"),
        "pending_reason_dashboard": _read_json(dashboard_dir / "pending_reason_dashboard.json"),
        "reader_brief_section": _read_text(dashboard_dir / "reader_brief_section.md"),
        "dashboard_dir": str(dashboard_dir),
    }


def validate_outcome_dashboard_artifact(
    *,
    dashboard_id: str,
    output_dir: Path = DEFAULT_OUTCOME_DASHBOARD_DIR,
) -> dict[str, Any]:
    dashboard_dir = output_dir / dashboard_id
    manifest = _read_optional_json(dashboard_dir / "outcome_dashboard_manifest.json") or {}
    matrix = _read_optional_json(dashboard_dir / "outcome_availability_matrix.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (dashboard_dir / "outcome_dashboard_manifest.json").exists(),
            dashboard_id,
        ),
        _check(
            "matrix_exists",
            (dashboard_dir / "outcome_availability_matrix.json").exists(),
            dashboard_id,
        ),
        _check(
            "mode_summary_exists",
            (dashboard_dir / "outcome_mode_summary.json").exists(),
            dashboard_id,
        ),
        _check(
            "pending_dashboard_exists",
            (dashboard_dir / "pending_reason_dashboard.json").exists(),
            dashboard_id,
        ),
        _check(
            "report_exists", (dashboard_dir / "outcome_dashboard_report.md").exists(), dashboard_id
        ),
        _check(
            "reader_brief_exists",
            (dashboard_dir / "reader_brief_section.md").exists(),
            dashboard_id,
        ),
        _check("dashboard_id_matches", manifest.get("dashboard_id") == dashboard_id, dashboard_id),
        _check(
            "required_modes_present",
            set(_mapping(matrix.get("summary")))
            == {
                "forward_outcome",
                "historical_replay",
                "backtest_simulation",
            },
            "outcome modes",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_outcome_dashboard_validation",
        artifact_id_key="dashboard_id",
        artifact_id=dashboard_id,
        checks=checks,
    )


def run_limited_vs_notrade_evaluation(
    *,
    output_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    advisory_outcome_dir: Path = DEFAULT_ADVISORY_OUTCOME_DIR,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    samples = _limited_vs_notrade_samples(
        advisory_outcome_dir=advisory_outcome_dir,
        backfill_dir=backfill_dir,
        repair_dir=repair_dir,
    )
    metrics = _limited_window_metrics(samples)
    regime_breakdown = _limited_regime_breakdown(samples)
    focus_id = _stable_id("limited-vs-notrade", generated.isoformat())
    focus_dir = _unique_dir(output_dir / focus_id)
    focus_dir.mkdir(parents=True, exist_ok=False)
    available_count = sum(1 for row in samples if row.get("sample_status") == "AVAILABLE")
    recommendation = _limited_overall_recommendation(metrics)
    status = "PASS" if available_count else "INSUFFICIENT_DATA"
    if recommendation == "continue_tracking":
        status = "PASS_WITH_WARNINGS"
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_vs_notrade_manifest",
        "focus_id": focus_dir.name,
        "generated_at": generated.isoformat(),
        "status": status,
        "available_count": available_count,
        "overall_recommendation": recommendation,
        "limited_vs_notrade_manifest_path": str(focus_dir / "limited_vs_notrade_manifest.json"),
        "sample_inventory_path": str(focus_dir / "sample_inventory.jsonl"),
        "window_comparison_metrics_path": str(focus_dir / "window_comparison_metrics.json"),
        "regime_breakdown_path": str(focus_dir / "regime_breakdown.json"),
        "limited_vs_notrade_report_path": str(focus_dir / "limited_vs_notrade_report.md"),
        "auto_policy_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    comparison = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_vs_notrade_window_comparison_metrics",
        "by_window": metrics,
        "overall_recommendation": recommendation,
        "policy_mutated": False,
        "production_effect": "none",
        "broker_action_taken": False,
    }
    _write_json(focus_dir / "limited_vs_notrade_manifest.json", manifest)
    _write_jsonl(focus_dir / "sample_inventory.jsonl", samples)
    _write_json(focus_dir / "window_comparison_metrics.json", comparison)
    _write_json(focus_dir / "regime_breakdown.json", regime_breakdown)
    _write_text(
        focus_dir / "limited_vs_notrade_report.md",
        render_limited_vs_notrade_report(manifest, comparison, regime_breakdown),
    )
    _update_latest_pointer(
        "latest_limited_vs_notrade",
        focus_dir.name,
        focus_dir / "limited_vs_notrade_manifest.json",
    )
    return {
        "focus_id": focus_dir.name,
        "focus_dir": focus_dir,
        "manifest": manifest,
        "sample_inventory": samples,
        "window_comparison_metrics": comparison,
        "regime_breakdown": regime_breakdown,
    }


def limited_vs_notrade_report_payload(
    *,
    focus_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
) -> dict[str, Any]:
    focus_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=focus_id if not latest else None,
        pointer_name="latest_limited_vs_notrade",
    )
    return {
        **_read_json(focus_dir / "limited_vs_notrade_manifest.json"),
        "sample_inventory": _read_jsonl(focus_dir / "sample_inventory.jsonl"),
        "window_comparison_metrics": _read_json(focus_dir / "window_comparison_metrics.json"),
        "regime_breakdown": _read_json(focus_dir / "regime_breakdown.json"),
        "focus_dir": str(focus_dir),
    }


def validate_limited_vs_notrade_artifact(
    *,
    focus_id: str,
    output_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
) -> dict[str, Any]:
    focus_dir = output_dir / focus_id
    manifest = _read_optional_json(focus_dir / "limited_vs_notrade_manifest.json") or {}
    samples = _read_jsonl(focus_dir / "sample_inventory.jsonl")
    metrics = _read_optional_json(focus_dir / "window_comparison_metrics.json") or {}
    checks = [
        _check(
            "manifest_exists", (focus_dir / "limited_vs_notrade_manifest.json").exists(), focus_id
        ),
        _check("samples_exists", (focus_dir / "sample_inventory.jsonl").exists(), focus_id),
        _check("metrics_exists", (focus_dir / "window_comparison_metrics.json").exists(), focus_id),
        _check("regime_exists", (focus_dir / "regime_breakdown.json").exists(), focus_id),
        _check("report_exists", (focus_dir / "limited_vs_notrade_report.md").exists(), focus_id),
        _check("focus_id_matches", manifest.get("focus_id") == focus_id, focus_id),
        _check(
            "sample_status_valid",
            all(row.get("sample_status") in OUTCOME_WINDOW_STATUSES for row in samples),
            "sample status",
        ),
        _check(
            "insufficient_data_explicit",
            bool(samples)
            or all(
                row.get("confidence") == "INSUFFICIENT_DATA"
                for row in _records(metrics.get("by_window"))
            ),
            "empty samples must be insufficient",
        ),
        _check(
            "no_auto_policy_apply",
            manifest.get("auto_policy_apply") is False
            and manifest.get("broker_action_allowed") is False,
            "policy must not auto apply",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_limited_vs_notrade_validation",
        artifact_id_key="focus_id",
        artifact_id=focus_id,
        checks=checks,
    )


def run_consensus_risk_review(
    *,
    output_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    daily_advisory_dir: Path = DEFAULT_POSITION_ADVISORY_DAILY_DIR,
    historical_replay_dir: Path = DEFAULT_HISTORICAL_REPLAY_DIR,
    backfill_dir: Path = DEFAULT_BACKFILLED_OUTCOME_DIR,
    repair_dir: Path = DEFAULT_BACKFILL_REPAIR_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    exposure_samples = _consensus_exposure_samples(
        daily_advisory_dir=daily_advisory_dir,
        historical_replay_dir=historical_replay_dir,
    )
    exposure = _consensus_exposure_summary(exposure_samples)
    outcome_rows = _historical_outcome_rows(backfill_dir=backfill_dir, repair_dir=repair_dir)
    drawdown = _consensus_drawdown_risk(outcome_rows)
    turnover = _consensus_turnover_risk(exposure_samples, outcome_rows)
    risk_status = _consensus_overall_risk_status(exposure, drawdown, turnover)
    risk_id = _stable_id("consensus-risk", generated.isoformat())
    risk_dir = _unique_dir(output_dir / risk_id)
    risk_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_risk_manifest",
        "risk_id": risk_dir.name,
        "generated_at": generated.isoformat(),
        "status": risk_status,
        "consensus_target_risk": risk_status,
        "sample_count": exposure["sample_count"],
        "consensus_risk_manifest_path": str(risk_dir / "consensus_risk_manifest.json"),
        "consensus_exposure_summary_path": str(risk_dir / "consensus_exposure_summary.json"),
        "consensus_drawdown_risk_path": str(risk_dir / "consensus_drawdown_risk.json"),
        "consensus_turnover_risk_path": str(risk_dir / "consensus_turnover_risk.json"),
        "consensus_risk_report_path": str(risk_dir / "consensus_risk_report.md"),
        "consensus_target_default_execution_recommended": False,
        "auto_policy_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(risk_dir / "consensus_risk_manifest.json", manifest)
    _write_json(risk_dir / "consensus_exposure_summary.json", exposure)
    _write_json(risk_dir / "consensus_drawdown_risk.json", drawdown)
    _write_json(risk_dir / "consensus_turnover_risk.json", turnover)
    _write_text(
        risk_dir / "consensus_risk_report.md",
        render_consensus_risk_report(manifest, exposure, drawdown, turnover),
    )
    _update_latest_pointer(
        "latest_consensus_risk",
        risk_dir.name,
        risk_dir / "consensus_risk_manifest.json",
    )
    return {
        "risk_id": risk_dir.name,
        "risk_dir": risk_dir,
        "manifest": manifest,
        "consensus_exposure_summary": exposure,
        "consensus_drawdown_risk": drawdown,
        "consensus_turnover_risk": turnover,
    }


def consensus_risk_report_payload(
    *,
    risk_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
) -> dict[str, Any]:
    risk_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=risk_id if not latest else None,
        pointer_name="latest_consensus_risk",
    )
    return {
        **_read_json(risk_dir / "consensus_risk_manifest.json"),
        "consensus_exposure_summary": _read_json(risk_dir / "consensus_exposure_summary.json"),
        "consensus_drawdown_risk": _read_json(risk_dir / "consensus_drawdown_risk.json"),
        "consensus_turnover_risk": _read_json(risk_dir / "consensus_turnover_risk.json"),
        "risk_dir": str(risk_dir),
    }


def validate_consensus_risk_artifact(
    *, risk_id: str, output_dir: Path = DEFAULT_CONSENSUS_RISK_DIR
) -> dict[str, Any]:
    risk_dir = output_dir / risk_id
    manifest = _read_optional_json(risk_dir / "consensus_risk_manifest.json") or {}
    exposure = _read_optional_json(risk_dir / "consensus_exposure_summary.json") or {}
    checks = [
        _check("manifest_exists", (risk_dir / "consensus_risk_manifest.json").exists(), risk_id),
        _check(
            "exposure_exists",
            (risk_dir / "consensus_exposure_summary.json").exists(),
            risk_id,
        ),
        _check("drawdown_exists", (risk_dir / "consensus_drawdown_risk.json").exists(), risk_id),
        _check("turnover_exists", (risk_dir / "consensus_turnover_risk.json").exists(), risk_id),
        _check("report_exists", (risk_dir / "consensus_risk_report.md").exists(), risk_id),
        _check("risk_id_matches", manifest.get("risk_id") == risk_id, risk_id),
        _check(
            "insufficient_sample_not_pass",
            not (
                _int(exposure.get("sample_count")) < CONSENSUS_RISK_REVIEW_SAMPLE_FLOOR
                and manifest.get("consensus_target_risk") == "PASS"
            ),
            "sample floor must block PASS",
        ),
        _check(
            "no_default_consensus_execution",
            manifest.get("consensus_target_default_execution_recommended") is False
            and manifest.get("auto_policy_apply") is False,
            "consensus target remains observation-only",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action forbidden",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_consensus_risk_validation",
        artifact_id_key="risk_id",
        artifact_id=risk_id,
        checks=checks,
    )


def render_outcome_due_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    update_ready: Sequence[Mapping[str, Any]],
) -> str:
    ready_ids = ", ".join(sorted({_text(row.get("outcome_id")) for row in update_ready})) or "NONE"
    return (
        "\n".join(
            [
                "# Dynamic v3 Outcome Due Report",
                "",
                f"- due_id: `{manifest.get('due_id')}`",
                f"- as_of: {manifest.get('as_of')}",
                f"- status: {manifest.get('status')}",
                f"- data_quality_status: {manifest.get('data_quality_status')}",
                f"- total_pending_windows: {summary.get('total_pending_windows')}",
                f"- due_windows: {summary.get('due_windows')}",
                f"- not_due_windows: {summary.get('not_due_windows')}",
                f"- price_missing_windows: {summary.get('price_missing_windows')}",
                f"- already_available_windows: {summary.get('already_available_windows')}",
                f"- update_ready_count: {summary.get('update_ready_count')}",
                f"- update_ready_outcome_ids: {ready_ids}",
                f"- advisory_outcome_update_recommended: {bool(update_ready)}",
                f"- backfill_repair_recommended: {summary.get('price_missing_windows', 0) > 0}",
                "",
                (
                    "本报告只判断 forward outcome window 是否到期，"
                    "不修改 advisory policy，不触发 broker。"
                ),
            ]
        )
        + "\n"
    )


def render_replay_sample_expansion_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    source_inventory: Sequence[Mapping[str, Any]],
) -> str:
    useful = ", ".join(
        f"{row.get('source_type')}={row.get('discovered_count')}" for row in source_inventory
    )
    return (
        "\n".join(
            [
                "# Dynamic v3 Replay Sample Expansion Report",
                "",
                f"- expansion_id: `{manifest.get('expansion_id')}`",
                f"- status: {manifest.get('status')}",
                f"- new_replay_events: {manifest.get('new_replay_event_count')}",
                (
                    "- PIT_SAFE / PIT_WARNING / PIT_UNSAFE: "
                    f"{summary.get('pit_safe_count')} / "
                    f"{summary.get('pit_warning_count')} / "
                    f"{summary.get('pit_unsafe_count')}"
                ),
                f"- replay_eligible_count: {summary.get('eligible_count')}",
                f"- partial_count: {summary.get('partial_count')}",
                f"- ineligible_count: {summary.get('ineligible_count')}",
                f"- source_type_discovery: {useful}",
                "- PIT_UNSAFE 默认不进入 historical replay。",
                (
                    "- 样本是否足以改善 variant comparison 取决于后续 "
                    "outcome availability dashboard 和 focused evaluation。"
                ),
            ]
        )
        + "\n"
    )


def render_outcome_dashboard_report(
    manifest: Mapping[str, Any],
    matrix: Mapping[str, Any],
    pending_dashboard: Mapping[str, Any],
) -> str:
    top = (_records(pending_dashboard.get("top_pending_reasons")) or [{}])[0]
    summary = _mapping(matrix.get("summary"))
    return (
        "\n".join(
            [
                "# Dynamic Rescue Outcome Availability",
                "",
                f"- dashboard_id: `{manifest.get('dashboard_id')}`",
                f"- status: {manifest.get('status')}",
                f"- FORWARD_OUTCOME: {summary.get('forward_outcome')}",
                f"- HISTORICAL_REPLAY: {summary.get('historical_replay')}",
                f"- BACKTEST_SIMULATION: {summary.get('backtest_simulation')}",
                f"- top_pending_reason: {top.get('reason', 'MISSING')}",
                f"- next_action: {pending_dashboard.get('next_action')}",
                "",
                "该 dashboard 只聚合 outcome availability，不补跑上游、不修改 policy。",
            ]
        )
        + "\n"
    )


def render_reader_brief_section(payload: Mapping[str, Any]) -> str:
    return (
        "\n".join(
            [
                "## Dynamic Rescue Outcome Availability",
                "",
                f"- available_count: {payload.get('available_count')}",
                f"- pending_count: {payload.get('pending_count')}",
                f"- insufficient_count: {payload.get('insufficient_count')}",
                f"- top_pending_reason: {payload.get('top_pending_reason')}",
                f"- next_action: {payload.get('next_action')}",
            ]
        )
        + "\n"
    )


def render_limited_vs_notrade_report(
    manifest: Mapping[str, Any],
    comparison: Mapping[str, Any],
    regime_breakdown: Mapping[str, Any],
) -> str:
    rows = _records(comparison.get("by_window"))
    row_lines = [
            (
                f"- {row.get('window_days')}d: available={row.get('available_count')}, "
                f"win_rate={row.get('win_rate')}, "
                f"avg_relative_return={row.get('avg_relative_return')}, "
                f"confidence={row.get('confidence')}"
            )
        for row in rows
    ]
    return (
        "\n".join(
            [
                "# Limited Adjustment vs No Trade Focused Evaluation",
                "",
                f"- focus_id: `{manifest.get('focus_id')}`",
                f"- status: {manifest.get('status')}",
                f"- available_count: {manifest.get('available_count')}",
                f"- overall_recommendation: {comparison.get('overall_recommendation')}",
                *row_lines,
                f"- regime_status: {regime_breakdown.get('status')}",
                "",
                "本报告只输出 focused evaluation，不自动修改 advisory policy。",
            ]
        )
        + "\n"
    )


def render_consensus_risk_report(
    manifest: Mapping[str, Any],
    exposure: Mapping[str, Any],
    drawdown: Mapping[str, Any],
    turnover: Mapping[str, Any],
) -> str:
    return (
        "\n".join(
            [
                "# Consensus Target Risk Review",
                "",
                f"- risk_id: `{manifest.get('risk_id')}`",
                f"- consensus_target_risk: {manifest.get('consensus_target_risk')}",
                f"- sample_count: {exposure.get('sample_count')}",
                f"- risk_asset_exposure: {exposure.get('risk_asset_exposure')}",
                f"- semiconductor_exposure: {exposure.get('semiconductor_exposure')}",
                f"- concentration_warnings: {exposure.get('concentration_warnings')}",
                f"- drawdown_window_results: {drawdown.get('window_results')}",
                f"- turnover_status: {turnover.get('turnover_status')}",
                "",
                (
                    "consensus_target 仍是观察对象；本报告不建议默认执行、"
                    "不自动改 policy、不触发 broker。"
                ),
            ]
        )
        + "\n"
    )


def _due_inventory_row(
    *,
    outcome_id: str,
    manifest: Mapping[str, Any],
    event: Mapping[str, Any],
    window: Mapping[str, Any],
    price_index: Mapping[str, set[date]],
    price_dates: Sequence[date],
    latest_price_date: date | None,
) -> dict[str, Any]:
    start = _date_from_any(window.get("start_date") or event.get("as_of") or manifest.get("as_of"))
    window_days = _int(window.get("window_days"))
    expected_end = _date_from_any(window.get("end_date"))
    if expected_end is None and start is not None and window_days > 0:
        expected_end = _nth_trading_date_after(price_dates, start, window_days)
    current_status = _text(window.get("outcome_status"), "INSUFFICIENT_DATA")
    symbols = _required_event_symbols(event)
    due_status = "INSUFFICIENT_DATA"
    reason = "missing_window_config"
    can_update = False
    if current_status == "AVAILABLE":
        due_status = "ALREADY_AVAILABLE"
        reason = "outcome_window_already_available"
    elif start is None or window_days <= 0 or expected_end is None or latest_price_date is None:
        due_status = "INSUFFICIENT_DATA"
        reason = "missing_advisory_id_as_of_window_or_price_calendar"
    elif expected_end > latest_price_date:
        due_status = "NOT_DUE"
        reason = "future_window_not_reached"
    elif not _has_required_prices(symbols, start, expected_end, price_index):
        due_status = "PRICE_MISSING"
        reason = "missing_price_data"
    elif current_status == "PENDING":
        due_status = "DUE"
        reason = "window_end_reached"
        can_update = True
    else:
        due_status = "INSUFFICIENT_DATA"
        reason = "outcome_window_not_pending"
    return {
        "daily_advisory_id": _text(
            window.get("daily_advisory_id")
            or event.get("daily_advisory_id")
            or manifest.get("daily_advisory_id")
        ),
        "outcome_id": outcome_id,
        "as_of": "" if start is None else start.isoformat(),
        "window_days": window_days,
        "window_start": "" if start is None else start.isoformat(),
        "expected_window_end": "" if expected_end is None else expected_end.isoformat(),
        "latest_price_date": "" if latest_price_date is None else latest_price_date.isoformat(),
        "current_outcome_status": current_status,
        "due_status": due_status,
        "can_update": can_update,
        "reason": reason,
        "required_symbols": sorted(symbols),
    }


def _pending_window_summary(
    rows: Sequence[Mapping[str, Any]], *, as_of: date, latest_price_date: date | None
) -> dict[str, Any]:
    counter = Counter(_text(row.get("due_status")) for row in rows)
    pending_count = sum(1 for row in rows if _text(row.get("current_outcome_status")) == "PENDING")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pending_window_summary",
        "as_of": as_of.isoformat(),
        "latest_price_date": "" if latest_price_date is None else latest_price_date.isoformat(),
        "total_pending_windows": pending_count,
        "due_windows": counter.get("DUE", 0),
        "not_due_windows": counter.get("NOT_DUE", 0),
        "price_missing_windows": counter.get("PRICE_MISSING", 0),
        "already_available_windows": counter.get("ALREADY_AVAILABLE", 0),
        "insufficient_data_windows": counter.get("INSUFFICIENT_DATA", 0),
        "update_ready_count": sum(1 for row in rows if row.get("can_update") is True),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _expanded_events_from_daily_advisory(
    *,
    daily_advisory_dir: Path,
    owner_reviews: Sequence[Mapping[str, Any]],
    price_index: Mapping[str, set[date]],
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(daily_advisory_dir.glob("*/daily_advisory_manifest.json")):
        manifest = _read_optional_json(path) or {}
        as_of = _date_from_any(manifest.get("as_of"))
        if as_of is None or not (start <= as_of <= end):
            continue
        advisory_dir = path.parent
        daily_advisory_id = _text(manifest.get("daily_advisory_id"), advisory_dir.name)
        target = _daily_target_weights(advisory_dir)
        current = _daily_current_weights(advisory_dir)
        consensus = _daily_consensus_weights(advisory_dir) or target
        owner_review = _owner_review_for_daily(daily_advisory_id, owner_reviews)
        symbols = set(target) | set(current) | set(consensus)
        limitations = []
        if not target:
            limitations.append("MISSING_TARGET_WEIGHTS")
        if not current:
            limitations.append("MISSING_CURRENT_WEIGHTS")
        if not consensus:
            limitations.append("CONSENSUS_WEIGHTS_MISSING")
        if not owner_review:
            limitations.append("OWNER_DECISION_MISSING")
        generated = _date_from_any(manifest.get("generated_at"))
        if generated is not None and generated > as_of:
            limitations.append("SOURCE_GENERATED_AFTER_AS_OF_DATE")
        price_after = _has_price_after(symbols, as_of, price_index)
        pit_status, eligibility = _pit_status_from_limitations(limitations)
        rows.append(
            {
                "expanded_event_id": _stable_id(
                    "expanded-event", "daily_advisory", daily_advisory_id, as_of.isoformat()
                ),
                "as_of": as_of.isoformat(),
                "daily_advisory_id": daily_advisory_id,
                "source_type": "daily_advisory",
                "source_artifact_path": str(path),
                "candidate_id": _daily_candidate_id(advisory_dir),
                "target_weights_available": bool(target),
                "current_weights_available": bool(current),
                "consensus_weights_available": bool(consensus),
                "owner_decision_available": bool(owner_review),
                "price_data_after_as_of_available": price_after,
                "pit_safety_status": pit_status,
                "replay_eligibility": eligibility,
                "limitations": limitations,
                "production_effect": "none",
                "broker_action_taken": False,
            }
        )
    return rows


def _expanded_events_from_replay_inventory(
    *, replay_inventory_dir: Path, start: date, end: date
) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(replay_inventory_dir.glob("*/replay_artifact_inventory.jsonl")):
        for source in _read_jsonl(path):
            as_of = _date_from_any(source.get("as_of"))
            if as_of is None or not (start <= as_of <= end):
                continue
            pit = _text(source.get("pit_safety_status"), "PIT_UNSAFE")
            eligibility = _text(source.get("replay_eligibility"), "INELIGIBLE")
            if pit == "PIT_UNSAFE":
                eligibility = "INELIGIBLE"
            inputs = _mapping(source.get("decision_inputs"))
            rows.append(
                {
                    "expanded_event_id": _stable_id(
                        "expanded-event",
                        "replay_inventory",
                        _text(source.get("daily_advisory_id")),
                        as_of.isoformat(),
                    ),
                    "as_of": as_of.isoformat(),
                    "daily_advisory_id": _text(source.get("daily_advisory_id")),
                    "source_type": "replay_inventory",
                    "source_artifact_path": str(path),
                    "candidate_id": _text(source.get("candidate_id")),
                    "target_weights_available": bool(_mapping(inputs.get("target_weights"))),
                    "current_weights_available": bool(_mapping(inputs.get("current_weights"))),
                    "consensus_weights_available": bool(_mapping(inputs.get("consensus_weights"))),
                    "owner_decision_available": _text(inputs.get("owner_decision"))
                    not in {
                        "",
                        "missing",
                    },
                    "price_data_after_as_of_available": "MISSING_PRICE_DATA"
                    not in _texts(source.get("replay_limitations")),
                    "pit_safety_status": pit if pit in PIT_SAFETY_STATUSES else "PIT_UNSAFE",
                    "replay_eligibility": (
                        eligibility if eligibility in REPLAY_ELIGIBILITY_STATUSES else "INELIGIBLE"
                    ),
                    "limitations": _texts(source.get("replay_limitations")),
                    "production_effect": "none",
                    "broker_action_taken": False,
                }
            )
    return rows


def _source_inventory_row(
    *, source_type: str, source_root: Path, scanned_count: int, discovered_count: int
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "source_type": source_type,
        "source_root": str(source_root),
        "scanned_count": scanned_count,
        "discovered_count": discovered_count,
        "useful_for_replay_expansion": discovered_count > 0,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _pit_status_from_limitations(limitations: Sequence[str]) -> tuple[str, str]:
    hard = {"MISSING_TARGET_WEIGHTS", "MISSING_CURRENT_WEIGHTS"}
    if hard & set(limitations):
        return "PIT_UNSAFE", "INELIGIBLE"
    if limitations:
        return "PIT_WARNING", "PARTIAL"
    return "PIT_SAFE", "ELIGIBLE"


def _pit_classification_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pit = Counter(_text(row.get("pit_safety_status")) for row in rows)
    eligibility = Counter(_text(row.get("replay_eligibility")) for row in rows)
    source_types = Counter(_text(row.get("source_type")) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_replay_sample_expansion_pit_classification_summary",
        "total_expanded_events": len(rows),
        "pit_safe_count": pit.get("PIT_SAFE", 0),
        "pit_warning_count": pit.get("PIT_WARNING", 0),
        "pit_unsafe_count": pit.get("PIT_UNSAFE", 0),
        "eligible_count": eligibility.get("ELIGIBLE", 0),
        "partial_count": eligibility.get("PARTIAL", 0),
        "ineligible_count": eligibility.get("INELIGIBLE", 0),
        "source_type_counts": dict(sorted(source_types.items())),
        "pit_unsafe_allowed_in_default_replay": False,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _dedupe_expanded_events(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _text(row.get("expanded_event_id"))
        if key and key not in result:
            result[key] = dict(row)
    return sorted(
        result.values(), key=lambda row: (_text(row.get("as_of")), _text(row.get("source_type")))
    )


def _forward_outcome_rows(advisory_outcome_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for child in _artifact_children(advisory_outcome_dir):
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json") or {}
        for row in _read_jsonl(child / "outcome_windows.jsonl"):
            rows.append(
                {
                    **row,
                    "source_mode": "FORWARD_OUTCOME",
                    "outcome_mode": "FORWARD_OUTCOME",
                    "outcome_id": manifest.get("outcome_id", child.name),
                    "source_artifact_path": str(child / "advisory_outcome_manifest.json"),
                }
            )
    return rows


def _historical_outcome_rows(*, backfill_dir: Path, repair_dir: Path) -> list[dict[str, Any]]:
    repair_children = _artifact_children(repair_dir)
    rows = []
    if repair_children:
        for child in repair_children:
            for row in _read_jsonl(child / "repaired_outcome_windows.jsonl"):
                rows.append(
                    {
                        **row,
                        "source_mode": "HISTORICAL_REPLAY",
                        "outcome_mode": "HISTORICAL_REPLAY",
                        "source_artifact_path": str(child / "backfill_repair_manifest.json"),
                    }
                )
        return rows
    for child in _artifact_children(backfill_dir):
        for row in _read_jsonl(child / "replay_outcome_windows.jsonl"):
            rows.append(
                {
                    **row,
                    "source_mode": "HISTORICAL_REPLAY",
                    "outcome_mode": "HISTORICAL_REPLAY",
                    "source_artifact_path": str(child / "backfill_manifest.json"),
                }
            )
    return rows


def _simulation_outcome_rows(paper_sim_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for child in _artifact_children(paper_sim_dir):
        manifest = _read_optional_json(child / "historical_paper_sim_manifest.json") or {}
        summary = _read_optional_json(child / "simulated_performance_summary.json") or {}
        status = _text(summary.get("simulation_status") or manifest.get("status"))
        outcome_status = "INSUFFICIENT_DATA"
        if status in {"PASS", "AVAILABLE"}:
            outcome_status = "AVAILABLE"
        elif status == "PENDING":
            outcome_status = "PENDING"
        rows.append(
            {
                "sample_id": child.name,
                "source_mode": "BACKTEST_SIMULATION",
                "outcome_mode": "BACKTEST_SIMULATION",
                "window_days": 0,
                "outcome_status": outcome_status,
                "source_artifact_path": str(child / "historical_paper_sim_manifest.json"),
            }
        )
    return rows


def _outcome_availability_matrix(
    rows_by_mode: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    summary = {}
    for mode in OUTCOME_MODES:
        summary[_mode_key(mode)] = _status_counts(rows_by_mode.get(mode, []))
    by_window = {}
    for window in OUTCOME_WINDOWS:
        window_rows = [
            row
            for rows in rows_by_mode.values()
            for row in rows
            if _int(row.get("window_days")) == window
        ]
        by_window[str(window)] = _status_counts(window_rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_availability_matrix",
        "summary": summary,
        "by_window_days": by_window,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _outcome_mode_summary(
    rows_by_mode: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_outcome_mode_summary",
        "modes": {
            mode: {
                **_status_counts(rows),
                "sample_count": len(rows),
                "source_artifact_count": len(
                    {
                        _text(row.get("source_artifact_path"))
                        for row in rows
                        if row.get("source_artifact_path")
                    }
                ),
            }
            for mode, rows in sorted(rows_by_mode.items())
        },
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _pending_reason_dashboard(
    *,
    rows_by_mode: Mapping[str, Sequence[Mapping[str, Any]]],
    diagnosis_dir: Path,
    outcome_due_dir: Path,
) -> dict[str, Any]:
    reasons = Counter()
    for row in _records_from_latest_json(diagnosis_dir, "replay_pending_reason_summary.json").get(
        "pending_reasons", []
    ):
        reasons[_text(row.get("reason"), "unknown")] += _int(row.get("count"), 0)
    for child in _artifact_children(outcome_due_dir):
        for row in _read_jsonl(child / "due_window_inventory.jsonl"):
            if _text(row.get("current_outcome_status")) == "PENDING":
                reasons[_text(row.get("reason"), "unknown")] += 1
    for rows in rows_by_mode.values():
        for row in rows:
            if _text(row.get("outcome_status")) == "PENDING":
                reasons[_text(row.get("pending_reason"), "future_window_not_reached")] += 1
    if not reasons:
        reasons["none"] = 0
    top = [
        {
            "reason": reason,
            "count": count,
            "action": PENDING_REASON_ACTIONS.get(reason, "manual_investigation_required"),
        }
        for reason, count in reasons.most_common()
    ]
    actionable = [
        row for row in top if row["reason"] in {"missing_price_data", "review_waiting_for_backfill"}
    ]
    next_action = top[0]["action"] if top else "continue_forward_tracking"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_pending_reason_dashboard",
        "top_pending_reasons": top,
        "actionable_pending_reasons": actionable,
        "next_action": next_action,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _outcome_dashboard_reader_brief(
    matrix: Mapping[str, Any], pending_dashboard: Mapping[str, Any]
) -> dict[str, Any]:
    summary = _mapping(matrix.get("summary"))
    available = sum(_int(item.get("available")) for item in summary.values())
    pending = sum(_int(item.get("pending")) for item in summary.values())
    insufficient = sum(_int(item.get("insufficient_data")) for item in summary.values())
    top = (_records(pending_dashboard.get("top_pending_reasons")) or [{}])[0]
    return {
        "available_count": available,
        "pending_count": pending,
        "insufficient_count": insufficient,
        "top_pending_reason": _text(top.get("reason"), "MISSING"),
        "next_action": _text(pending_dashboard.get("next_action"), "continue_forward_tracking"),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _limited_vs_notrade_samples(
    *, advisory_outcome_dir: Path, backfill_dir: Path, repair_dir: Path
) -> list[dict[str, Any]]:
    samples = []
    for child in _artifact_children(advisory_outcome_dir):
        manifest = _read_optional_json(child / "advisory_outcome_manifest.json") or {}
        for row in _read_jsonl(child / "outcome_windows.jsonl"):
            samples.append(
                {
                    "sample_id": _stable_id(
                        "limited-forward", child.name, _text(row.get("window_days"))
                    ),
                    "source_mode": "FORWARD_OUTCOME",
                    "as_of": _text(row.get("start_date") or manifest.get("as_of")),
                    "window_days": _int(row.get("window_days")),
                    "limited_adjustment_return": _float(row.get("paper_portfolio_return")),
                    "no_trade_return": _float(row.get("no_trade_return")),
                    "relative_return": _float(row.get("relative_to_no_trade")),
                    "limited_drawdown": _float(row.get("max_drawdown")),
                    "no_trade_drawdown": _float(row.get("no_trade_max_drawdown")),
                    "relative_drawdown": round(
                        _float(row.get("max_drawdown")) - _float(row.get("no_trade_max_drawdown")),
                        6,
                    ),
                    "turnover": _float(row.get("turnover")),
                    "sample_status": _text(row.get("outcome_status"), "INSUFFICIENT_DATA"),
                }
            )
    rows = _historical_outcome_rows(backfill_dir=backfill_dir, repair_dir=repair_dir)
    grouped: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        key = (
            _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
            _int(row.get("window_days")),
        )
        grouped[key][_text(row.get("variant"))] = row
    for (event_id, window), variants in sorted(grouped.items()):
        limited = variants.get("limited_adjustment")
        no_trade = variants.get("no_trade")
        if not limited or not no_trade:
            continue
        status = _paired_status(limited, no_trade)
        samples.append(
            {
                "sample_id": _stable_id("limited-replay", event_id, str(window)),
                "source_mode": "HISTORICAL_REPLAY",
                "as_of": _text(limited.get("as_of")),
                "window_days": window,
                "limited_adjustment_return": _float(limited.get("return")),
                "no_trade_return": _float(no_trade.get("return")),
                "relative_return": round(
                    _float(limited.get("return")) - _float(no_trade.get("return")),
                    6,
                ),
                "limited_drawdown": _float(limited.get("max_drawdown")),
                "no_trade_drawdown": _float(no_trade.get("max_drawdown")),
                "relative_drawdown": round(
                    _float(limited.get("max_drawdown")) - _float(no_trade.get("max_drawdown")),
                    6,
                ),
                "turnover": _float(limited.get("turnover")),
                "sample_status": status,
            }
        )
    return sorted(samples, key=lambda row: (_text(row.get("as_of")), _int(row.get("window_days"))))


def _limited_window_metrics(samples: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for window in OUTCOME_WINDOWS:
        rows = [
            row
            for row in samples
            if _int(row.get("window_days")) == window and row.get("sample_status") == "AVAILABLE"
        ]
        rel = [_float(row.get("relative_return")) for row in rows]
        drawdowns = [_float(row.get("relative_drawdown")) for row in rows]
        turnovers = [_float(row.get("turnover")) for row in rows]
        count = len(rows)
        confidence = "INSUFFICIENT_DATA"
        if count >= FOCUSED_HIGH_CONFIDENCE_SAMPLE_FLOOR:
            confidence = "HIGH"
        elif count >= FOCUSED_MEDIUM_CONFIDENCE_SAMPLE_FLOOR:
            confidence = "MEDIUM"
        elif count > 0:
            confidence = "LOW"
        result.append(
            {
                "window_days": window,
                "available_count": count,
                "avg_relative_return": round(_avg(rel), 6),
                "median_relative_return": round(_median(rel), 6),
                "win_rate": round(sum(1 for value in rel if value > 0) / count, 6)
                if count
                else 0.0,
                "avg_drawdown_delta": round(_avg(drawdowns), 6),
                "avg_turnover": round(_avg(turnovers), 6),
                "confidence": confidence,
            }
        )
    return result


def _limited_overall_recommendation(metrics: Sequence[Mapping[str, Any]]) -> str:
    available = [row for row in metrics if _int(row.get("available_count")) > 0]
    if not available:
        return "insufficient_data"
    confident = [row for row in available if _text(row.get("confidence")) in {"MEDIUM", "HIGH"}]
    if not confident:
        return "continue_tracking"
    avg = _avg([_float(row.get("avg_relative_return")) for row in confident])
    return "support_limited_adjustment" if avg > 0 else "weaken_limited_adjustment"


def _limited_regime_breakdown(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    available = [row for row in samples if row.get("sample_status") == "AVAILABLE"]
    if not available:
        status = "INSUFFICIENT_DATA"
    else:
        status = "PARTIAL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_limited_vs_notrade_regime_breakdown",
        "status": status,
        "by_regime": {
            regime: {
                "available_count": 0,
                "avg_relative_return": 0.0,
                "status": "INSUFFICIENT_DATA",
            }
            for regime in (
                "ai_trend",
                "tech_drawdown",
                "semiconductor_pullback",
                "sideways",
                "risk_off",
                "unknown",
            )
        },
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_exposure_samples(
    *, daily_advisory_dir: Path, historical_replay_dir: Path
) -> list[dict[str, Any]]:
    samples = []
    for path in sorted(daily_advisory_dir.glob("*/daily_advisory_manifest.json")):
        advisory_dir = path.parent
        manifest = _read_optional_json(path) or {}
        weights = _daily_consensus_weights(advisory_dir) or _daily_target_weights(advisory_dir)
        if weights:
            samples.append(
                {
                    "source": "daily_advisory",
                    "as_of": _text(manifest.get("as_of")),
                    "weights": _normalize_weights(weights),
                    "turnover": _daily_turnover(advisory_dir),
                }
            )
    for child in _artifact_children(historical_replay_dir):
        for event in _read_jsonl(child / "replay_events.jsonl"):
            for variant in _records(event.get("variants")):
                if _text(variant.get("variant")) == "consensus_target":
                    samples.append(
                        {
                            "source": "historical_replay",
                            "as_of": _text(event.get("as_of")),
                            "weights": _normalize_weights(_mapping(variant.get("weights"))),
                            "turnover": _float(variant.get("turnover")),
                        }
                    )
    return samples


def _consensus_exposure_summary(samples: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    risk_asset = []
    cash = []
    semiconductor = []
    warnings = []
    for sample in samples:
        weights = _mapping(sample.get("weights"))
        cash_weight = _float(weights.get("CASH"))
        risk_asset_weight = round(max(0.0, 1.0 - cash_weight), 6)
        semi_weight = round(sum(_float(weights.get(symbol)) for symbol in SEMICONDUCTOR_SYMBOLS), 6)
        cash.append(cash_weight)
        risk_asset.append(risk_asset_weight)
        semiconductor.append(semi_weight)
        if semi_weight > CONSENSUS_SEMICONDUCTOR_EXPOSURE_REVIEW_LEVEL:
            warnings.append("semiconductor_exposure_review_required")
        if risk_asset_weight > CONSENSUS_RISK_ASSET_EXPOSURE_REVIEW_LEVEL:
            warnings.append("risk_asset_exposure_review_required")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_exposure_summary",
        "sample_count": len(samples),
        "risk_asset_exposure": _min_mean_max(risk_asset),
        "cash_exposure": _min_mean_max(cash),
        "semiconductor_exposure": _min_mean_max(semiconductor),
        "concentration_warnings": sorted(set(warnings)),
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_drawdown_risk(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    grouped: dict[tuple[str, int], dict[str, Mapping[str, Any]]] = defaultdict(dict)
    for row in rows:
        key = (
            _text(row.get("replay_event_id") or row.get("daily_advisory_id")),
            _int(row.get("window_days")),
        )
        grouped[key][_text(row.get("variant"))] = row
    results = []
    for window in OUTCOME_WINDOWS:
        drawdowns = []
        deltas = []
        for (event_id, row_window), variants in grouped.items():
            _ = event_id
            if row_window != window:
                continue
            consensus = variants.get("consensus_target")
            no_trade = variants.get("no_trade")
            if not consensus or not no_trade:
                continue
            if _paired_status(consensus, no_trade) != "AVAILABLE":
                continue
            drawdowns.append(_float(consensus.get("max_drawdown")))
            deltas.append(
                _float(consensus.get("max_drawdown")) - _float(no_trade.get("max_drawdown"))
            )
        count = len(drawdowns)
        delta = round(_avg(deltas), 6)
        status = "INSUFFICIENT_DATA"
        if count >= CONSENSUS_RISK_REVIEW_SAMPLE_FLOOR:
            status = "REVIEW_REQUIRED" if delta < CONSENSUS_DRAWDOWN_REVIEW_DELTA else "PASS"
        elif count:
            status = "INSUFFICIENT_DATA"
        results.append(
            {
                "window_days": window,
                "available_count": count,
                "avg_drawdown": round(_avg(drawdowns), 6),
                "max_drawdown": round(min(drawdowns), 6) if drawdowns else 0.0,
                "drawdown_delta_vs_no_trade": delta,
                "risk_status": status,
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_drawdown_risk",
        "window_results": results,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_turnover_risk(
    exposure_samples: Sequence[Mapping[str, Any]], outcome_rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    values = [
        _float(row.get("turnover")) for row in exposure_samples if row.get("turnover") is not None
    ]
    values.extend(
        _float(row.get("turnover"))
        for row in outcome_rows
        if _text(row.get("variant")) == "consensus_target"
        and row.get("outcome_status") == "AVAILABLE"
    )
    values = [value for value in values if math.isfinite(value)]
    avg_turnover = round(_avg(values), 6)
    max_turnover = round(max(values), 6) if values else 0.0
    warnings = []
    status = "INSUFFICIENT_DATA"
    if len(values) >= CONSENSUS_RISK_REVIEW_SAMPLE_FLOOR:
        if avg_turnover > CONSENSUS_TURNOVER_REVIEW_LEVEL or max_turnover > 1.0:
            status = "REVIEW_REQUIRED"
            warnings.append("turnover_review_required")
        elif max_turnover > CONSENSUS_TURNOVER_REVIEW_LEVEL:
            status = "PASS_WITH_WARNINGS"
            warnings.append("high_single_sample_turnover")
        else:
            status = "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_consensus_turnover_risk",
        "avg_turnover": avg_turnover,
        "max_turnover": max_turnover,
        "turnover_status": status,
        "warnings": warnings,
        "production_effect": "none",
        "broker_action_taken": False,
    }


def _consensus_overall_risk_status(
    exposure: Mapping[str, Any],
    drawdown: Mapping[str, Any],
    turnover: Mapping[str, Any],
) -> str:
    if _int(exposure.get("sample_count")) < CONSENSUS_RISK_REVIEW_SAMPLE_FLOOR:
        return "INSUFFICIENT_DATA"
    statuses = [_text(row.get("risk_status")) for row in _records(drawdown.get("window_results"))]
    statuses.append(_text(turnover.get("turnover_status")))
    if "REVIEW_REQUIRED" in statuses:
        return "REVIEW_REQUIRED"
    if _texts(exposure.get("concentration_warnings")) or "PASS_WITH_WARNINGS" in statuses:
        return "PASS_WITH_WARNINGS"
    if all(status == "PASS" for status in statuses if status):
        return "PASS"
    return "INSUFFICIENT_DATA"


def _paired_status(left: Mapping[str, Any], right: Mapping[str, Any]) -> str:
    statuses = {_text(left.get("outcome_status")), _text(right.get("outcome_status"))}
    if statuses == {"AVAILABLE"}:
        return "AVAILABLE"
    if "PENDING" in statuses:
        return "PENDING"
    return "INSUFFICIENT_DATA"


def _status_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counter = Counter(_text(row.get("outcome_status") or row.get("sample_status")) for row in rows)
    return {
        "available": counter.get("AVAILABLE", 0),
        "pending": counter.get("PENDING", 0),
        "insufficient_data": counter.get("INSUFFICIENT_DATA", 0),
    }


def _mode_key(mode: str) -> str:
    return mode.lower()


def _quality_gate_for_cached_data(
    *,
    as_of: date,
    generated: datetime,
    prices_path: Path,
    rates_path: Path,
    report_path: Path,
    enforce: bool,
) -> tuple[str, str]:
    if not enforce:
        return "SKIPPED_EXPLICIT_TEST_FIXTURE", ""
    quality_as_of = min(as_of, generated.date())
    quality = _run_cached_data_quality_gate(
        as_of=quality_as_of,
        prices_path=prices_path,
        rates_path=rates_path,
        report_path=report_path,
    )
    if not quality.passed:
        raise DynamicV3OutcomeAccumulationError(
            f"cached data quality gate failed: {quality.status}"
        )
    return quality.status, str(report_path)


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


def _price_availability_index(prices_path: Path) -> dict[str, set[date]]:
    if not prices_path.exists():
        return {}
    try:
        frame = pd.read_csv(prices_path)
    except (OSError, pd.errors.ParserError):
        return {}
    symbol_column = "symbol" if "symbol" in frame.columns else "ticker"
    if "date" not in frame.columns or symbol_column not in frame.columns:
        return {}
    frame["_date"] = pd.to_datetime(frame["date"], errors="coerce").dt.date
    result: dict[str, set[date]] = defaultdict(set)
    for row in frame.dropna(subset=["_date"]).to_dict("records"):
        symbol = _text(row.get(symbol_column))
        if symbol:
            result[symbol].add(row["_date"])
    return result


def _latest_price_date_on_or_before(
    price_index: Mapping[str, set[date]], as_of: date
) -> date | None:
    dates = [item for values in price_index.values() for item in values if item <= as_of]
    return max(dates) if dates else None


def _all_price_dates(price_index: Mapping[str, set[date]]) -> list[date]:
    return sorted({item for values in price_index.values() for item in values})


def _has_required_prices(
    symbols: set[str], start: date, end: date, price_index: Mapping[str, set[date]]
) -> bool:
    for symbol in symbols:
        if symbol == "CASH":
            continue
        dates = price_index.get(symbol, set())
        if not any(item <= start for item in dates):
            return False
        if end not in dates:
            return False
    return True


def _has_price_after(symbols: set[str], as_of: date, price_index: Mapping[str, set[date]]) -> bool:
    return all(
        symbol == "CASH" or any(item > as_of for item in price_index.get(symbol, set()))
        for symbol in symbols
    )


def _required_event_symbols(event: Mapping[str, Any]) -> set[str]:
    symbols = set()
    for key in (
        "no_trade_weights",
        "paper_action_weights",
        "baseline_weights",
        "target_weights",
    ):
        symbols.update(_mapping(event.get(key)))
    return {_text(symbol) for symbol in symbols if _text(symbol)}


def _daily_target_weights(advisory_dir: Path) -> dict[str, float]:
    for row in _read_jsonl(advisory_dir / "daily_candidate_targets.jsonl"):
        weights = _mapping(row.get("target_weights"))
        if weights:
            return _normalize_weights(weights)
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        weights = _mapping(row.get("target_weights"))
        if weights:
            return _normalize_weights(weights)
    return _daily_consensus_weights(advisory_dir)


def _daily_current_weights(advisory_dir: Path) -> dict[str, float]:
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        weights = _mapping(row.get("current_weights"))
        if weights:
            return _normalize_weights(weights)
    return {}


def _daily_consensus_weights(advisory_dir: Path) -> dict[str, float]:
    path = advisory_dir / "daily_consensus_weights.csv"
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return {}
    weights = {}
    for row in rows:
        symbol = _text(row.get("symbol"))
        value = _float(row.get("median_target_weight"), default=float("nan"))
        if symbol and math.isfinite(value):
            weights[symbol] = value
    return _normalize_weights(weights)


def _daily_candidate_id(advisory_dir: Path) -> str:
    for path in (
        advisory_dir / "daily_candidate_targets.jsonl",
        advisory_dir / "daily_position_deltas.jsonl",
    ):
        for row in _read_jsonl(path):
            candidate_id = _text(row.get("candidate_id"))
            if candidate_id:
                return candidate_id
    return ""


def _daily_turnover(advisory_dir: Path) -> float:
    values = []
    for row in _read_jsonl(advisory_dir / "daily_position_deltas.jsonl"):
        deltas = _mapping(row.get("deltas"))
        if deltas:
            values.append(sum(abs(_float(value)) for value in deltas.values()))
    return round(max(values), 6) if values else 0.0


def _owner_review_for_daily(
    daily_advisory_id: str, owner_reviews: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in owner_reviews
        if _text(row.get("daily_advisory_id")) == daily_advisory_id
    ]
    return matches[-1] if matches else {}


def _records_from_latest_json(root: Path, filename: str) -> dict[str, Any]:
    children = _artifact_children(root)
    if not children:
        return {}
    return _read_optional_json(children[-1] / filename) or {}


def _load_outcome_prices(prices_path: Path, symbols: set[str]) -> pd.DataFrame:
    config = load_etf_config_bundle()
    prices, quality = load_standard_prices(
        prices_path,
        config.assets,
        config.strategy,
        extra_symbols=symbols,
    )
    if not quality.passed:
        raise DynamicV3OutcomeAccumulationError(f"ETF price validation failed: {quality.status}")
    prices = prices.copy()
    prices["_date"] = pd.to_datetime(prices["date"], errors="coerce").dt.date
    prices["_adj_close"] = pd.to_numeric(prices["adj_close"], errors="coerce")
    return prices.dropna(subset=["_date", "_adj_close"])


def _nth_trading_date_after(dates: Sequence[date], start: date, n: int) -> date | None:
    after = [item for item in dates if item > start]
    if len(after) < n:
        return None
    return after[n - 1]


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


def _artifact_dir_from_latest(
    *, output_dir: Path, artifact_id: str | None, pointer_name: str
) -> Path:
    if artifact_id:
        path = output_dir / artifact_id
        if not path.exists():
            raise DynamicV3OutcomeAccumulationError(f"artifact not found: {artifact_id}")
        return path
    pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{pointer_name}.json") or {}
    pointer_id = _text(pointer.get("artifact_id"))
    if pointer_id and (output_dir / pointer_id).exists():
        return output_dir / pointer_id
    latest = _latest_child_dir(output_dir)
    if latest is None:
        raise DynamicV3OutcomeAccumulationError(f"latest artifact not found for {pointer_name}")
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


def _artifact_children(path: Path) -> list[Path]:
    if not path.exists():
        return []
    return sorted(
        [child for child in path.iterdir() if child.is_dir()],
        key=lambda child: child.stat().st_mtime,
    )


def _latest_child_dir(path: Path) -> Path | None:
    children = _artifact_children(path)
    return children[-1] if children else None


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for idx in range(1, 1000):
        candidate = path.with_name(f"{path.name}_{idx:03d}")
        if not candidate.exists():
            return candidate
    raise DynamicV3OutcomeAccumulationError(f"could not allocate unique artifact dir: {path}")


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


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3OutcomeAccumulationError(f"required JSON artifact not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(_jsonable(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(_jsonable(row), ensure_ascii=False, sort_keys=True) + "\n" for row in rows
        ),
        encoding="utf-8",
    )


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(inner) for inner in value]
    return value


def _date_from_any(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = _text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _stable_id(*parts: object) -> str:
    raw = "|".join(str(part) for part in parts)
    return sha256(raw.encode("utf-8")).hexdigest()[:16]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _records(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value] if isinstance(value, list) else []


def _texts(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple | set):
        return [_text(item) for item in value if _text(item)]
    return []


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _avg(values: Sequence[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    return sum(clean) / len(clean) if clean else 0.0


def _median(values: Sequence[float]) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return 0.0
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2


def _min_mean_max(values: Sequence[float]) -> dict[str, float]:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return {"mean": 0.0, "max": 0.0, "min": 0.0}
    return {
        "mean": round(_avg(clean), 6),
        "max": round(max(clean), 6),
        "min": round(min(clean), 6),
    }


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
